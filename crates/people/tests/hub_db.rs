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

/// 点呼のセッションの中の記録にする (CoreS3 が点呼ごとに発番する session_id)
fn in_session(mut v: Value) -> Value {
    v["session_id"] = json!("boot1-1");
    v
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

async fn measurement(t: Uuid, employee: Uuid, is_dev: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.measurements (tenant_id, employee_id, is_dev) VALUES ($1, $2, $3) RETURNING id",
            &[&t, &employee, &is_dev],
        )
        .await
        .unwrap()
        .get(0)
}

/// 打刻が載せた測定の id は、同じ tenant・軸・凍結した社員の測定が在るときだけ列 `measurement_id` に入る。
/// 点呼の中の体温・血圧・アルコールは社員を照合せずに入る。合わない id・無い id・uuid でない値・セッションの外の記録・
/// ほかの kind は NULL で、行は入る (外部キーの違反でまとめて落ちない)
#[tokio::test]
async fn timecard_links_measurement() {
    let app = support::app().await;
    let t = tenant().await;
    let other_t = tenant().await;
    let emp = employee(t, "打刻 三郎").await;
    let other = employee(t, "打刻 四郎").await;
    let got = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "0123456789abcdef" })),
    )
    .await;
    assert_eq!(got.status, 201);
    let m_ok = measurement(t, emp, false).await;
    let m_other = measurement(t, other, false).await;
    let m_dev = measurement(t, emp, true).await;
    let m_other_tenant = measurement(other_t, employee(other_t, "別 太郎").await, false).await;
    let card = |mid: Value| json!({ "card_id": "0123456789abcdef", "measurement_id": mid });

    let body = json!([
        m("tc", "timecard", 1, card(json!(m_ok))),
        m("tc", "timecard", 2, card(json!(m_other))),
        m("tc", "timecard", 3, card(json!(Uuid::new_v4()))),
        m("tc", "timecard", 4, card(json!("not-a-uuid"))),
        m("tc", "timecard", 5, card(json!(m_dev))),
        m("tc", "timecard", 6, card(json!(m_other_tenant))),
        m(
            "tc",
            "timecard",
            7,
            json!({ "card_id": "ffffffffffffffff", "measurement_id": m_ok })
        ),
        m("tc", "alcohol", 8, json!({ "measurement_id": m_ok })),
        // 点呼の中の体温・血圧・アルコール (CoreS3 が載せる) は社員を照合せずに結ぶ。別の tenant の測定は結ばない
        in_session(m("tc", "alcohol", 9, json!({ "measurement_id": m_other }))),
        in_session(m(
            "tc",
            "temperature",
            10,
            json!({ "measurement_id": m_ok })
        )),
        in_session(m(
            "tc",
            "blood_pressure",
            11,
            json!({ "measurement_id": m_other_tenant })
        )),
        in_session(m("tc", "license", 12, json!({ "measurement_id": m_ok }))),
    ]);
    let got = post(&app, t, body, false).await;
    assert_eq!(got.json(), json!({ "inserted": 12, "duplicates": 0 }));
    let got = post(
        &app,
        t,
        json!([m("tc", "timecard", 1, card(json!(m_dev)))]),
        true,
    )
    .await;
    assert_eq!(got.json(), json!({ "inserted": 1, "duplicates": 0 }));

    let linked: Vec<(i64, bool, Option<Uuid>)> = admin()
        .await
        .query(
            "SELECT seq, is_dev, measurement_id FROM alc_api.hub_measurements WHERE tenant_id = $1 ORDER BY is_dev, seq",
            &[&t],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    assert_eq!(
        linked,
        vec![
            (1, false, Some(m_ok)),
            (2, false, None),
            (3, false, None),
            (4, false, None),
            (5, false, None),
            (6, false, None),
            (7, false, None),
            (8, false, None),
            (9, false, Some(m_other)),
            (10, false, Some(m_ok)),
            (11, false, None),
            (12, false, None),
            (1, true, Some(m_dev)),
        ]
    );
}

/// 一覧: 受信の新しい順・絞り込み・1 件多く引いて has_more。鍵の ID で絞ると結び付いた hub の行も返し、
/// hub の行の machine_id は鍵の ID に畳む
/// 端末の時計が合う前 (1970 起点) の記録は端末の時刻を捨てて受信の時刻で読み、どちらかを返す
#[tokio::test]
async fn device_clock_before_2020_uses_received_time() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "時刻 太郎").await;
    let got = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "00000000000000aa" })),
    )
    .await;
    assert_eq!(got.status, 201);
    let at = |mut v: Value, ms: i64| {
        v["recorded_at_ms"] = json!(ms);
        v
    };
    let body = json!([
        at(
            m(
                "tc",
                "timecard",
                1,
                json!({ "card_id": "00000000000000aa" })
            ),
            323_354
        ),
        at(
            m(
                "tc",
                "timecard",
                2,
                json!({ "card_id": "00000000000000aa" })
            ),
            1_761_980_037_735
        ),
        at(m("tc", "temperature", 3, json!({})), 1_577_836_799_999),
    ]);
    assert_eq!(post(&app, t, body, false).await.status, 201);

    let rows = get(&app, t, "?device_id=tc", false).await.json();
    let seen: Vec<(i64, Value, Value)> = rows["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["seq"].as_i64().unwrap(),
                r["recorded_at"].clone(),
                r["recorded_at_source"].clone(),
            )
        })
        .collect();
    let mut seen = seen;
    seen.sort_by_key(|r| r.0);
    assert_eq!(
        seen,
        vec![
            (1, Value::Null, json!("received")),
            (2, json!("2025-11-01T06:53:57.735Z"), json!("device")),
            (3, Value::Null, json!("received")),
        ]
    );

    let punches = call(&app, "GET", "/api/timecard/punches", Some(t), None)
        .await
        .json();
    let mut punched: Vec<(String, Value)> = punches["punches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            (
                p["punched_at"].as_str().unwrap().to_owned(),
                p["punched_at_source"].clone(),
            )
        })
        .collect();
    punched.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(punched.len(), 2);
    assert_eq!(
        punched[0],
        ("2025-11-01T06:53:57.735Z".to_owned(), json!("device"))
    );
    // 受信の時刻 (= 今日) で読むので 1970 にならない
    assert!(punched[1].0.as_str() > "2026-01-01", "{punched:?}");
    assert_eq!(punched[1].1, json!("received"));
}

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

/// 一覧: 列 `measurement_id` で結ばれた点呼の区分・確定の方法・場所を返す (同じ測定に複数あれば新しい方)。
/// 結ばれていない行と、測定が在っても点呼が無い行は null
#[tokio::test]
async fn list_carries_tenko_method_and_location() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee(t, "点呼 五郎").await;
    let m_it = measurement(t, emp, false).await;
    let m_none = measurement(t, emp, false).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.measurements SET status = 'started' WHERE id = $1",
            &[&m_none],
        )
        .await
        .unwrap();
    for (method, judged, location, at) in [
        ("遠隔点呼", None, Some("旧営業所"), 0_i64),
        ("IT点呼", Some("in_person"), Some("本社営業所"), 60),
    ] {
        admin()
            .await
            .execute(
                "INSERT INTO alc_api.tenko_sessions \
                   (tenant_id, employee_id, tenko_type, status, measurement_id, is_dev, tenko_method, \
                    manager_judgment_method, location, created_at) \
                 VALUES ($1, $2, 'pre_operation', 'completed', $3, false, $4, $5, $6, \
                         to_timestamp(1767225600 + $7::bigint))",
                &[&t, &emp, &m_it, &method, &judged, &location, &at],
            )
            .await
            .unwrap();
    }
    let body = json!([
        in_session(m("hub-1", "alcohol", 1, json!({ "measurement_id": m_it }))),
        in_session(m(
            "hub-1",
            "temperature",
            2,
            json!({ "measurement_id": m_none })
        )),
        in_session(m("hub-1", "license", 3, json!({}))),
    ]);
    assert_eq!(post(&app, t, body, false).await.status, 201);

    let all = get(&app, t, "", false).await.json();
    // 1 回の POST は受信の時刻が同じなので、並びは seq で揃えて比べる
    let mut seen: Vec<(i64, Value, Value, Value, Value)> = all["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["seq"].as_i64().unwrap(),
                r["tenko_method"].clone(),
                r["manager_judgment_method"].clone(),
                r["tenko_location"].clone(),
                r["measurement_status"].clone(),
            )
        })
        .collect();
    seen.sort_by_key(|r| r.0);
    assert_eq!(
        seen,
        vec![
            (
                1,
                json!("IT点呼"),
                json!("in_person"),
                json!("本社営業所"),
                json!("completed")
            ),
            // 測定は在るが点呼まで進まなかった (started のまま) = 画面の「未完了」
            (2, Value::Null, Value::Null, Value::Null, json!("started")),
            (3, Value::Null, Value::Null, Value::Null, Value::Null),
        ]
    );
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
