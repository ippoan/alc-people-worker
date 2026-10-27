//! 端末の測定・打刻の取り込みと一覧を実 DB で通す (rust-alc-api の alc-devices の hub_measurements.rs の要点と、
//! 打刻の凍結・dev の軸)。接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。

mod support;

use serde_json::{json, Value};
use support::{admin, broken_client, build, call, call_with, employee, tenant};
use uuid::Uuid;

const URI: &str = "/api/hub/measurements";

fn m(device: &str, kind: &str, seq: i64, payload: Value) -> Value {
    json!({ "device_id": device, "kind": kind, "seq": seq, "payload": payload })
}

async fn post(app: &support::App, t: Uuid, body: Value, dev: bool) -> support::Got {
    let headers: &[(&str, &str)] = if dev { &[("X-Device-Dev", "1")] } else { &[] };
    call_with(app, "POST", URI, Some(t), headers, Some(body)).await
}

async fn get(app: &support::App, t: Uuid, query: &str, dev: bool) -> support::Got {
    let headers: &[(&str, &str)] = if dev { &[("X-Device-Dev", "1")] } else { &[] };
    call_with(app, "GET", &format!("{URI}{query}"), Some(t), headers, None).await
}

/// 行の (seq, payload) を新しい順に (superuser で。軸を問わない)
async fn stored(t: Uuid) -> Vec<(i64, Value, bool)> {
    admin()
        .await
        .query(
            "SELECT seq, payload, is_dev FROM alc_api.hub_measurements WHERE tenant_id = $1 ORDER BY seq, is_dev",
            &[&t],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

#[tokio::test]
async fn tenant_header_is_required() {
    let app = support::app().await;
    assert_eq!(call(&app, "GET", URI, None, None).await.status, 401);
    assert_eq!(
        call(
            &app,
            "POST",
            URI,
            None,
            Some(m("d", "alcohol", 1, json!({})))
        )
        .await
        .status,
        401
    );
}

/// 取り込み: 配列でも 1 件でも 201。同じ軸の再送は数えるだけ。端末の時計は TIMESTAMPTZ に (範囲外は NULL)
#[tokio::test]
async fn ingest_counts_duplicates_and_rejects_bad_input() {
    let app = support::app().await;
    let t = tenant().await;
    let batch = json!([
        { "device_id": "dev-1", "kind": "alcohol", "seq": 1, "recorded_at_ms": 1752300000000_i64,
          "session_id": "s1", "payload": { "level": 0.0 } },
        { "device_id": "dev-1", "kind": "temperature", "seq": 2, "recorded_at_ms": i64::MAX,
          "payload": { "value": 36.5 } },
    ]);
    let got = post(&app, t, batch.clone(), false).await;
    assert_eq!(got.status, 201);
    assert_eq!(got.json(), json!({ "inserted": 2, "duplicates": 0 }));
    let got = post(&app, t, batch, false).await;
    assert_eq!(got.json(), json!({ "inserted": 0, "duplicates": 2 }));
    let got = post(&app, t, m("dev-1", "license", 3, json!({ "n": 1 })), false).await;
    assert_eq!(
        (got.status, got.json()),
        (201, json!({ "inserted": 1, "duplicates": 0 }))
    );

    let row = admin()
        .await
        .query(
            "SELECT seq, recorded_at IS NULL, session_id FROM alc_api.hub_measurements WHERE tenant_id = $1 ORDER BY seq",
            &[&t],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, bool>(1), r.get::<_, Option<String>>(2)))
        .collect::<Vec<_>>();
    assert_eq!(
        row,
        vec![
            (1, false, Some("s1".into())),
            (2, true, None),
            (3, true, None)
        ]
    );

    // 400: 空・501 件・知らない kind・連番が負・本番の軸の試験記録
    let many: Vec<Value> = (0..501).map(|i| m("d", "alcohol", i, json!({}))).collect();
    for body in [
        json!([]),
        Value::Array(many),
        m("d", "unknown", 1, json!({})),
        m("d", "alcohol", -1, json!({})),
        m("d", "test", 1, json!({})),
    ] {
        assert_eq!(post(&app, t, body, false).await.status, 400);
    }
    assert_eq!(stored(t).await.len(), 3);
}

/// dev の軸: 試験記録は dev だけ。同じ端末・同じ連番でも軸ごとに 1 行。一覧は軸ごとに分かれる
#[tokio::test]
async fn dev_axis_is_separate() {
    let app = support::app().await;
    let t = tenant().await;
    assert_eq!(
        post(&app, t, m("d", "alcohol", 1, json!({})), false)
            .await
            .status,
        201
    );
    let got = post(
        &app,
        t,
        json!([
            m("d", "alcohol", 1, json!({})),
            m("d", "test", 2, json!({}))
        ]),
        true,
    )
    .await;
    assert_eq!(
        (got.status, got.json()),
        (201, json!({ "inserted": 2, "duplicates": 0 }))
    );
    assert_eq!(
        stored(t)
            .await
            .iter()
            .map(|(s, _, d)| (*s, *d))
            .collect::<Vec<_>>(),
        vec![(1, false), (1, true), (2, true)]
    );
    let prod = get(&app, t, "", false).await.json();
    assert_eq!(prod["items"].as_array().unwrap().len(), 1);
    let dev = get(&app, t, "?kind=test", true).await.json();
    assert_eq!(dev["items"].as_array().unwrap().len(), 1);
    assert_eq!(dev["items"][0]["kind"], "test");
}

/// 打刻: カードを社員に解決して payload に凍結する (台帳 → 免許証番号。表記の揺れは正規化)。
/// 未登録・カード無しは凍結せずに入れる。端末の申告の employee_id は kind を問わず捨てる
#[tokio::test]
async fn timecard_freezes_employee() {
    let app = support::app().await;
    let t = tenant().await;
    let by_card = employee(t, "打刻 一郎").await;
    let by_nfc = employee(t, "打刻 二郎").await;
    let forged = Uuid::new_v4();
    let got = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": by_card, "card_id": "0123456789abcdef" })),
    )
    .await;
    assert_eq!(got.status, 201);
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET nfc_id = '2023040120280331' WHERE id = $1",
            &[&by_nfc],
        )
        .await
        .unwrap();

    let body = json!([
        m(
            "tc",
            "timecard",
            1,
            json!({ "card_id": "01:23:45:67:89:AB:CD:EF", "employee_id": forged })
        ),
        m(
            "tc",
            "timecard",
            2,
            json!({ "card_id": "2023040120280331", "card_kind": "license" })
        ),
        m(
            "tc",
            "timecard",
            3,
            json!({ "card_id": "ffffffffffffffff" })
        ),
        m("tc", "timecard", 4, json!({ "employee_id": forged })),
        m(
            "tc",
            "alcohol",
            5,
            json!({ "level": 0.0, "employee_id": forged })
        ),
    ]);
    let got = post(&app, t, body, false).await;
    assert_eq!(got.json(), json!({ "inserted": 5, "duplicates": 0 }));
    let payloads: Vec<Value> = stored(t).await.into_iter().map(|(_, p, _)| p).collect();
    assert_eq!(
        payloads,
        vec![
            json!({ "card_id": "01:23:45:67:89:AB:CD:EF", "employee_id": by_card.to_string() }),
            json!({ "card_id": "2023040120280331", "card_kind": "license", "employee_id": by_nfc.to_string() }),
            json!({ "card_id": "ffffffffffffffff" }),
            json!({}),
            json!({ "level": 0.0 }),
        ]
    );
    assert_eq!(
        app.logs(),
        vec![
            "people: hub timecard has no card_id".to_owned(),
            "people: hub timecard card not registered".to_owned(),
        ]
    );
    support::assert_logs_clean(&app, &["ffffffffffffffff".into(), t.to_string()]);
}

/// 一覧: 受信の新しい順・絞り込み・1 件多く引いて has_more。鍵の ID で絞ると結び付いた hub の行も返し、
/// hub の行の machine_id は鍵の ID に畳む
#[tokio::test]
async fn list_filters_and_links_keys() {
    let app = support::app().await;
    let t = tenant().await;
    let other = tenant().await;
    let key = "alarm:0123456789abcdef";
    for body in [
        m("hub-1", "temperature", 1, json!({})),
        json!({ "device_id": key, "kind": "alcohol", "seq": 1, "session_id": "s9", "via_device_id": "hub-1", "payload": {} }),
        m("dev-2", "alcohol", 1, json!({})),
    ] {
        assert_eq!(post(&app, t, body, false).await.status, 201);
    }
    assert_eq!(
        post(&app, other, m("hub-1", "alcohol", 1, json!({})), false)
            .await
            .status,
        201
    );

    let all = get(&app, t, "", false).await.json();
    assert_eq!(
        (
            all["limit"].clone(),
            all["offset"].clone(),
            all["has_more"].clone()
        ),
        (json!(50), json!(0), json!(false))
    );
    let rows = all["items"].as_array().unwrap();
    let seen: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| {
            (
                r["device_id"].as_str().unwrap(),
                r["machine_id"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(seen, vec![("dev-2", "dev-2"), (key, key), ("hub-1", key)]);
    assert_eq!(rows[1]["via_device_id"], "hub-1");
    assert_eq!(rows[1]["session_id"], "s9");
    assert_eq!(rows[1]["tenant_id"], json!(t));

    let devices = |v: &Value| -> Vec<String> {
        v["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["device_id"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(
        devices(
            &get(&app, t, &format!("?device_id={key}"), false)
                .await
                .json()
        ),
        vec![key, "hub-1"]
    );
    assert_eq!(
        devices(&get(&app, t, "?device_id=dev-2", false).await.json()),
        vec!["dev-2"]
    );
    assert_eq!(
        devices(&get(&app, t, "?kind=temperature", false).await.json()),
        vec!["hub-1"]
    );
    assert_eq!(
        devices(&get(&app, t, "?session_id=s9", false).await.json()),
        vec![key]
    );
    assert!(devices(
        &get(&app, t, "?from=2999-01-01T00:00:00Z", false)
            .await
            .json()
    )
    .is_empty());
    assert_eq!(
        devices(
            &get(
                &app,
                t,
                "?from=2000-01-01T00:00:00Z&to=2999-01-01T00:00:00Z",
                false
            )
            .await
            .json()
        )
        .len(),
        3
    );
    let page = get(&app, t, "?limit=1&offset=1", false).await.json();
    assert_eq!(
        (devices(&page), page["has_more"].clone()),
        (vec![key.to_owned()], json!(true))
    );
    let clamped = get(&app, t, "?limit=0&offset=-3", false).await.json();
    assert_eq!(
        (clamped["limit"].clone(), clamped["offset"].clone()),
        (json!(1), json!(0))
    );

    // 400: 知らない kind・session_id の形・from > to
    for q in [
        "?kind=unknown",
        "?session_id=a%20b",
        "?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z",
    ] {
        assert_eq!(get(&app, t, q, false).await.status, 400, "{q}");
    }
}

#[tokio::test]
async fn db_failures_are_500_with_fixed_logs() {
    let app = build(broken_client().await);
    let t = Uuid::new_v4();
    let got = post(
        &app,
        t,
        m("d", "timecard", 1, json!({ "card_id": "01020304" })),
        false,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(get(&app, t, "", false).await.status, 500);
    let logs = app.logs();
    for stage in ["hub card resolve", "hub ingest", "hub list"] {
        assert!(
            logs.iter()
                .any(|l| l.starts_with(&format!("people: {stage} failed: db ("))),
            "{stage} のログ"
        );
    }
    support::assert_logs_clean(&app, &["01020304".into(), t.to_string()]);
}
