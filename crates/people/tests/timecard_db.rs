//! タイムカードの口を実 DB で通す (rust-alc-api の tests/timecard_cards_bulk_test.rs・timecard_punches_test.rs・
//! mock_timecard_test.rs の要点を写したもの)。接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。

mod support;

use serde_json::{json, Value};
use support::{
    admin, broken_client, build, call, call_dev, call_with, client_as, tenant, READ_ONLY_ROLE,
};
use uuid::Uuid;

/// 社員を 1 人作る (社員番号・免許証番号つき)
async fn employee_with(tenant: Uuid, name: &str, code: Option<&str>, nfc: Option<&str>) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, name, code, nfc_id) VALUES ($1, $2, $3, $4, $5)",
            &[&id, &tenant, &name, &code, &nfc],
        )
        .await
        .unwrap();
    id
}

async fn source_of(tenant: Uuid, card_id: &str) -> Option<String> {
    admin()
        .await
        .query_one(
            "SELECT source FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2",
            &[&tenant, &card_id],
        )
        .await
        .unwrap()
        .get(0)
}

async fn card_count(tenant: Uuid) -> i64 {
    admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.timecard_cards WHERE tenant_id = $1",
            &[&tenant],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn tenant_header_is_required() {
    let app = support::app().await;
    assert_eq!(
        call(&app, "GET", "/api/timecard/cards", None, None)
            .await
            .status,
        401
    );
    assert_eq!(
        call(&app, "GET", "/api/timecard/punches", None, None)
            .await
            .status,
        401
    );
}

#[tokio::test]
async fn card_crud() {
    let app = support::app().await;
    let t = tenant().await;
    let other = tenant().await;
    let emp = employee_with(t, "台帳 太郎", Some("C001"), None).await;

    // 登録は正規化形で入る (source は付かない = 直接登録)
    let created = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "  AA:BB:CC:DD  ", "label": "社員証" })),
    )
    .await;
    assert_eq!(created.status, 201);
    let c = created.json();
    assert_eq!(c["card_id"], "aabbccdd");
    assert_eq!(c["employee_id"], emp.to_string());
    assert_eq!(c["label"], "社員証");
    assert!(c["source"].is_null());
    assert!(c["pending_employee_code"].is_null());
    let id = c["id"].as_str().unwrap().to_owned();

    // 同じ card_id (大文字で送っても) は 409
    let dup = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "AABBCCDD" })),
    )
    .await;
    assert_eq!(dup.status, 409);

    // 一覧 (社員で絞る・絞らない)。別 tenant からは見えない
    let all = call(&app, "GET", "/api/timecard/cards", Some(t), None).await;
    assert_eq!(all.json().as_array().unwrap().len(), 1);
    let by_emp = call(
        &app,
        "GET",
        &format!("/api/timecard/cards?employee_id={}", Uuid::new_v4()),
        Some(t),
        None,
    )
    .await;
    assert_eq!(by_emp.json(), json!([]));
    let mine = call(
        &app,
        "GET",
        &format!("/api/timecard/cards?employee_id={emp}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(mine.json().as_array().unwrap().len(), 1);
    assert_eq!(
        call(&app, "GET", "/api/timecard/cards", Some(other), None)
            .await
            .json(),
        json!([])
    );

    // 取得・削除
    let got = call(
        &app,
        "GET",
        &format!("/api/timecard/cards/{id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.json(), c);
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/timecard/cards/{id}"),
            Some(other),
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/timecard/cards/{id}"),
            Some(other),
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/timecard/cards/{id}"),
            Some(t),
            None
        )
        .await
        .status,
        204
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/timecard/cards/{id}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/timecard/cards/{id}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );

    // 存在しない社員の登録は 409 ではなく 500 (rust と同じ。一意の制約以外の失敗)
    let bad = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": Uuid::new_v4(), "card_id": "01020304" })),
    )
    .await;
    assert_eq!(bad.status, 500);
    support::assert_logs_clean(
        &app,
        &["aabbccdd".into(), "01020304".into(), emp.to_string()],
    );
}

#[tokio::test]
async fn bulk_upsert_by_code() {
    let app = support::app().await;
    let t = tenant().await;
    let a = employee_with(t, "一括 一郎", Some("E001"), None).await;
    let b = employee_with(t, "一括 二郎", Some("E002"), None).await;

    // 0 件・上限超えは 400 (本文は文字列)
    let empty = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": [] })),
    )
    .await;
    assert_eq!(empty.status, 400);
    assert_eq!(String::from_utf8(empty.body).unwrap(), "items が不正です");
    let many: Vec<Value> = (0..501)
        .map(|i| json!({ "code": "E001", "card_id": format!("{i:08x}") }))
        .collect();
    assert_eq!(
        call(
            &app,
            "PUT",
            "/api/timecard/cards/bulk-by-code",
            Some(t),
            Some(json!({ "items": many }))
        )
        .await
        .status,
        400
    );

    let items = json!([
        { "code": "E001", "card_id": "0123456789ABCDEF", "label": "社員証" },
        { "code": "E002", "card_id": "未登録" },
        { "code": "E999", "card_id": "04A1B2C3D4E5F6" },
        { "code": "E002", "card_id": "0123456789abcdef" },
    ]);

    // dry_run は判定だけ返して 1 行も書かない
    let dry = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "dry_run": true, "items": items })),
    )
    .await;
    assert_eq!(dry.status, 200);
    assert_eq!(card_count(t).await, 0);

    let first = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": items })),
    )
    .await;
    assert_eq!(first.status, 200);
    assert_eq!(first.json(), dry.json(), "dry_run と本番で判定が同じ");
    assert_eq!(
        first.json(),
        json!({
            "created": 2, "updated": 0, "unchanged": 0, "pending": 1,
            "skipped": [
                { "index": 1, "code": "E002", "reason": "invalid_card_id" },
                { "index": 3, "code": "E002", "reason": "duplicate_in_batch" },
            ]
        })
    );
    assert_eq!(
        source_of(t, "0123456789abcdef").await.as_deref(),
        Some("timecard-ic-ledger-import")
    );
    let pending = admin()
        .await
        .query_one(
            "SELECT employee_id, pending_employee_code FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2",
            &[&t, &"04a1b2c3d4e5f6"],
        )
        .await
        .unwrap();
    assert_eq!(pending.get::<_, Option<Uuid>>(0), None);
    assert_eq!(pending.get::<_, Option<String>>(1).as_deref(), Some("E999"));

    // 2 回目: 同じ持ち主は unchanged、保留行は社員が居ないまま unchanged + pending
    let again = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": [
            { "code": "E001", "card_id": "0123456789abcdef" },
            { "code": "E999", "card_id": "04a1b2c3d4e5f6" },
        ] })),
    )
    .await;
    assert_eq!(
        again.json(),
        json!({ "created": 0, "updated": 0, "unchanged": 2, "pending": 1, "skipped": [] })
    );

    // 別の社員に付いているカード: 既定は見送り、reassign で付け替え。保留行は reassign を待たずに引き取る
    let conflict = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": [
            { "code": "E002", "card_id": "0123456789abcdef" },
            { "code": "E002", "card_id": "04a1b2c3d4e5f6" },
        ] })),
    )
    .await;
    assert_eq!(
        conflict.json(),
        json!({
            "created": 0, "updated": 1, "unchanged": 0, "pending": 0,
            "skipped": [{ "index": 0, "code": "E002", "reason": "card_owner_conflict" }]
        })
    );
    let reassign = call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "on_conflict": "reassign", "items": [{ "code": "E002", "card_id": "0123456789abcdef" }] })),
    )
    .await;
    assert_eq!(
        reassign.json(),
        json!({ "created": 0, "updated": 1, "unchanged": 0, "pending": 0, "skipped": [] })
    );
    let owners = admin()
        .await
        .query(
            "SELECT employee_id, pending_employee_code FROM alc_api.timecard_cards WHERE tenant_id = $1 ORDER BY card_id",
            &[&t],
        )
        .await
        .unwrap();
    for r in &owners {
        assert_eq!(r.get::<_, Option<Uuid>>(0), Some(b));
        assert_eq!(r.get::<_, Option<String>>(1), None);
    }
    assert_ne!(a, b);
    support::assert_logs_clean(&app, &["0123456789abcdef".into(), "04a1b2c3d4e5f6".into()]);
}

#[tokio::test]
async fn delete_by_card_from_sync() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee_with(t, "削除 太郎", Some("D001"), None).await;
    // 同期の行 2 枚 (片方は保留) と、直接登録の行 1 枚
    call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": [
            { "code": "D001", "card_id": "11111111" },
            { "code": "D404", "card_id": "22222222" },
        ] })),
    )
    .await;
    call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "33333333" })),
    )
    .await;

    let del = |card: &'static str, dry: bool| {
        let app = &app;
        async move {
            call(
                app,
                "POST",
                "/api/timecard/cards/delete-by-card",
                Some(t),
                Some(json!({ "card_id": card, "dry_run": dry })),
            )
            .await
            .json()
        }
    };
    assert_eq!(
        del("未登録", false).await,
        json!({ "deleted": 0, "reason": "invalid_card_id", "code": null })
    );
    // dry_run は消さない (判定は同じ)
    assert_eq!(
        del("11111111", true).await,
        json!({ "deleted": 1, "reason": "deleted", "code": "D001" })
    );
    assert_eq!(card_count(t).await, 3);
    assert_eq!(
        del("11:11:11:11", false).await,
        json!({ "deleted": 1, "reason": "deleted", "code": "D001" })
    );
    // 保留行は台帳の社員番号を返す
    assert_eq!(
        del("22222222", false).await,
        json!({ "deleted": 1, "reason": "deleted", "code": "D404" })
    );
    // 直接登録の行は射程外
    assert_eq!(
        del("33333333", false).await,
        json!({ "deleted": 0, "reason": "out_of_scope", "code": null })
    );
    assert_eq!(
        del("44444444", false).await,
        json!({ "deleted": 0, "reason": "not_found", "code": null })
    );
    assert_eq!(card_count(t).await, 1);

    // POST だけ (他 method は 405)
    assert_eq!(
        call(
            &app,
            "GET",
            "/api/timecard/cards/delete-by-card",
            Some(t),
            None
        )
        .await
        .status,
        405
    );
}

#[tokio::test]
async fn lookup_employee_by_card() {
    let app = support::app().await;
    let t = tenant().await;
    let emp = employee_with(t, "照会 太郎", Some("L001"), Some("2023040120280331")).await;
    let retired = employee_with(t, "退職 次郎", Some("L002"), None).await;
    admin()
        .await
        .execute(
            "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
            &[&retired],
        )
        .await
        .unwrap();
    for (emp_id, card) in [(emp, "AABBCCDD"), (retired, "EEFF0011")] {
        call(
            &app,
            "POST",
            "/api/timecard/cards",
            Some(t),
            Some(json!({ "employee_id": emp_id, "card_id": card })),
        )
        .await;
    }
    // 保留行 (社員番号の社員が居ない) は免許証番号へ落ちる
    call(
        &app,
        "PUT",
        "/api/timecard/cards/bulk-by-code",
        Some(t),
        Some(json!({ "items": [{ "code": "L999", "card_id": "99999999" }] })),
    )
    .await;

    let look = |card: Value| {
        let app = &app;
        async move {
            call(
                app,
                "POST",
                "/api/timecard/cards/lookup",
                Some(t),
                Some(json!({ "card_id": card })),
            )
            .await
        }
    };
    // 空・欠落・区切りだけは 400
    assert_eq!(look(json!("   ")).await.status, 400);
    assert_eq!(look(json!(":::")).await.status, 400);
    assert_eq!(
        call(
            &app,
            "POST",
            "/api/timecard/cards/lookup",
            Some(t),
            Some(json!({}))
        )
        .await
        .status,
        400
    );
    // カード (生値の大文字・区切り付き) → 在籍の社員
    let hit = look(json!("aa:bb:cc:dd")).await;
    assert_eq!(hit.status, 200);
    let e = hit.json();
    assert_eq!(e["id"], emp.to_string());
    assert_eq!(e["name"], "照会 太郎");
    assert_eq!(e["code"], "L001");
    assert_eq!(e["role"], json!(["driver"]));
    assert!(e.get("face_embedding").is_none());
    // 免許証番号で引ける
    assert_eq!(
        look(json!("2023040120280331")).await.json()["id"],
        emp.to_string()
    );
    // 退職・未登録・保留 (免許証にも無い) は 404
    assert_eq!(look(json!("EEFF0011")).await.status, 404);
    assert_eq!(look(json!("12345678")).await.status, 404);
    assert_eq!(look(json!("99999999")).await.status, 404);
    // 照会は書かない (台帳は 3 枚のまま)
    assert_eq!(card_count(t).await, 3);
    assert_eq!(
        call(&app, "GET", "/api/timecard/cards/lookup", Some(t), None)
            .await
            .status,
        405
    );
}

/// 打刻の行を入れる (`at` は基準からの秒)
async fn hub_row(
    tenant: Uuid,
    seq: i64,
    device: &str,
    kind: &str,
    payload: Value,
    at: i64,
    is_dev: bool,
) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.hub_measurements (tenant_id, device_id, kind, payload, seq, recorded_at, is_dev) \
             VALUES ($1, $2, $3, $4, $5, to_timestamp(1767225600 + $6::bigint), $7) RETURNING id",
            &[&tenant, &device, &kind, &payload, &seq, &at, &is_dev],
        )
        .await
        .unwrap()
        .get(0)
}

async fn measurement(tenant: Uuid, employee: Uuid, level: f64, result: &str, is_dev: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.measurements (tenant_id, employee_id, alcohol_level, result, is_dev) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
            &[&tenant, &employee, &level, &result, &is_dev],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn punches_list_and_csv() {
    let app = support::app().await;
    let t = tenant().await;
    let other = tenant().await;
    let emp = employee_with(t, "打刻 太郎", Some("P001"), Some("2023040120280331")).await;
    let emp2 = employee_with(t, "打刻 次郎", Some("P002"), None).await;
    call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp2, "card_id": "0123456789abcdef" })),
    )
    .await;
    let m_fail = measurement(t, emp, 0.15, "fail", false).await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenko_sessions (tenant_id, employee_id, tenko_type, status, measurement_id, is_dev) \
             VALUES ($1, $2, 'normal', 'completed', $3, false)",
            &[&t, &emp, &m_fail],
        )
        .await
        .unwrap();

    // ① カード (大文字・区切りで送ったタップ) → 台帳の社員。② 未登録カード。
    // ③ 免許証のタッチ + 5 秒後の点呼 (license の行は一覧から外れる)。④ 対の無い点呼 (別の端末)。
    // ⑤ payload の employee_id と測定。⑥ dev端末の打刻。⑦ 別 tenant
    let r1 = hub_row(
        t,
        1,
        "cores3-1",
        "timecard",
        json!({ "card_id": "01:23:45:67:89:AB:CD:EF", "card_kind": "felica_idm" }),
        10,
        false,
    )
    .await;
    let r2 = hub_row(
        t,
        2,
        "cores3-1",
        "timecard",
        json!({ "card_id": "DEADBEEF", "card_kind": "nfca_uid" }),
        20,
        false,
    )
    .await;
    let r3 = hub_row(
        t,
        3,
        "cores3-1",
        "timecard",
        json!({ "card_id": "2023040120280331", "card_kind": "license" }),
        30,
        false,
    )
    .await;
    let r3l = hub_row(
        t,
        4,
        "cores3-1",
        "license",
        json!({ "nfc_id": "2023040120280331" }),
        35,
        false,
    )
    .await;
    let r4 = hub_row(
        t,
        5,
        "cores3-2",
        "license",
        json!({ "nfc_id": "2023040120280331" }),
        40,
        false,
    )
    .await;
    let r5 = hub_row(
        t,
        6,
        "browser",
        "timecard",
        json!({ "employee_id": emp.to_string(), "measurement_id": m_fail.to_string() }),
        50,
        false,
    )
    .await;
    let r6 = hub_row(
        t,
        7,
        "cores3-1",
        "timecard",
        json!({ "card_id": "0123456789abcdef" }),
        60,
        true,
    )
    .await;
    hub_row(
        other,
        1,
        "cores3-1",
        "timecard",
        json!({ "card_id": "0123456789abcdef" }),
        70,
        false,
    )
    .await;

    let list = call(&app, "GET", "/api/timecard/punches", Some(t), None).await;
    assert_eq!(list.status, 200);
    let body = list.json();
    assert_eq!(body["total"], 5);
    assert_eq!(body["page"], 1);
    assert_eq!(body["per_page"], 50);
    let punches = body["punches"].as_array().unwrap().clone();
    let ids: Vec<&str> = punches.iter().map(|p| p["id"].as_str().unwrap()).collect();
    // 新しい順。license の対の行 (r3l) と dev の行 (r6) は出ない
    assert_eq!(ids, [r5, r4, r3, r2, r1].map(|u| u.to_string()));
    assert!(!ids.contains(&r3l.to_string().as_str()));
    let by = |id: Uuid| {
        punches
            .iter()
            .find(|p| p["id"] == id.to_string())
            .unwrap()
            .clone()
    };
    assert_eq!(by(r1)["employee_id"], emp2.to_string());
    assert_eq!(by(r1)["employee_name"], "打刻 次郎");
    assert_eq!(by(r1)["card_kind"], "felica_idm");
    assert_eq!(by(r1)["device_name"], "cores3-1");
    assert!(by(r1)["device_id"].is_null());
    assert!(by(r2)["employee_id"].is_null());
    assert_eq!(by(r2)["card_id"], "DEADBEEF");
    assert_eq!(by(r3)["employee_id"], emp.to_string());
    assert_eq!(by(r4)["kind"], "license");
    assert_eq!(by(r4)["card_kind"], "license");
    assert_eq!(by(r5)["measurement_id"], m_fail.to_string());
    assert_eq!(by(r5)["alcohol_level"], 0.15);
    assert_eq!(by(r5)["result"], "fail");
    assert_eq!(by(r5)["tenko_recorded"], true);
    assert!(by(r1)["tenko_recorded"].is_null());
    assert_eq!(by(r1)["punched_at"], "2026-01-01T00:00:10Z");

    // 頁と絞り込み
    let page2 = call(
        &app,
        "GET",
        "/api/timecard/punches?page=2&per_page=2",
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(page2["total"], 5);
    assert_eq!(page2["page"], 2);
    assert_eq!(page2["per_page"], 2);
    assert_eq!(
        page2["punches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(r3.to_string()), json!(r2.to_string())]
    );
    let mine = call(
        &app,
        "GET",
        &format!("/api/timecard/punches?employee_id={emp}"),
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(mine["total"], 3);
    let window = call(
        &app,
        "GET",
        "/api/timecard/punches?date_from=2026-01-01T00:00:20Z&date_to=2026-01-01T00:00:40Z",
        Some(t),
        None,
    )
    .await
    .json();
    assert_eq!(window["total"], 3);

    // dev端末の要求は dev の行だけ
    let dev = call_dev(&app, "GET", "/api/timecard/punches", t)
        .await
        .json();
    assert_eq!(dev["total"], 1);
    assert_eq!(dev["punches"][0]["id"], r6.to_string());
    assert_eq!(dev["punches"][0]["employee_name"], "打刻 次郎");

    // CSV: BOM・見出し・JST・区分とカードの列。未登録のタップも空欄で出る
    let csv = call(&app, "GET", "/api/timecard/punches/csv", Some(t), None).await;
    assert_eq!(csv.status, 200);
    assert_eq!(csv.headers["content-type"], "text/csv; charset=utf-8");
    assert_eq!(
        csv.headers["content-disposition"],
        "attachment; filename=\"time_punches.csv\""
    );
    assert_eq!(&csv.body[..3], &[0xEF, 0xBB, 0xBF]);
    let text = String::from_utf8(csv.body[3..].to_vec()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], people::timecard::CSV_HEADER.join(","));
    assert_eq!(lines.len(), 6);
    assert_eq!(
        lines[1],
        format!("{r5},打刻,,P001,打刻 太郎,2026-01-01 09:00:50,browser,{m_fail},0.15,fail,済")
    );
    assert_eq!(
        lines[4],
        format!("{r2},打刻,ICカード,,,2026-01-01 09:00:20,cores3-1,,,,")
    );
    assert_eq!(lines[3].split(',').nth(1), Some("打刻"));
    assert_eq!(lines[2].split(',').nth(1), Some("点呼"));
    let dev_csv = call_dev(&app, "GET", "/api/timecard/punches/csv", t).await;
    assert_eq!(
        String::from_utf8(dev_csv.body[3..].to_vec())
            .unwrap()
            .lines()
            .count(),
        2
    );
    let filtered = call_with(
        &app,
        "GET",
        &format!("/api/timecard/punches/csv?employee_id={emp2}"),
        Some(t),
        &[],
        None,
    )
    .await;
    assert_eq!(
        String::from_utf8(filtered.body[3..].to_vec())
            .unwrap()
            .lines()
            .count(),
        2
    );
    support::assert_logs_clean(&app, &[]);
}

#[tokio::test]
async fn db_failures_are_500_with_fixed_logs() {
    let app = build(broken_client().await);
    let t = Uuid::new_v4();
    let id = Uuid::new_v4();
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        (
            "POST",
            "/api/timecard/cards".into(),
            Some(json!({ "employee_id": id, "card_id": "01020304" })),
            "card create",
        ),
        ("GET", "/api/timecard/cards".into(), None, "card list"),
        ("GET", format!("/api/timecard/cards/{id}"), None, "card get"),
        (
            "DELETE",
            format!("/api/timecard/cards/{id}"),
            None,
            "card delete",
        ),
        (
            "PUT",
            "/api/timecard/cards/bulk-by-code".into(),
            Some(json!({ "items": [{ "code": "X", "card_id": "01020304" }] })),
            "card bulk",
        ),
        (
            "POST",
            "/api/timecard/cards/delete-by-card".into(),
            Some(json!({ "card_id": "01020304" })),
            "card delete-by-card",
        ),
        (
            "POST",
            "/api/timecard/cards/lookup".into(),
            Some(json!({ "card_id": "01020304" })),
            "card lookup",
        ),
        ("GET", "/api/timecard/punches".into(), None, "punch list"),
        ("GET", "/api/timecard/punches/csv".into(), None, "punch csv"),
    ];
    for (method, uri, body, stage) in cases {
        let got = call(&app, method, &uri, Some(t), body).await;
        assert_eq!(got.status, 500, "{stage}");
        assert!(
            app.logs()
                .iter()
                .any(|l| l.starts_with(&format!("people: {stage} failed: db ("))),
            "{stage} のログ"
        );
    }
    support::assert_logs_clean(&app, &["01020304".into(), id.to_string(), t.to_string()]);
}

#[tokio::test]
async fn read_only_role_cannot_write() {
    // 書けないロールでの作成は 500 (一意の制約の 409 には化けない)
    let app = build(client_as(READ_ONLY_ROLE).await);
    let t = tenant().await;
    let emp = employee_with(t, "読取 太郎", None, None).await;
    let got = call(
        &app,
        "POST",
        "/api/timecard/cards",
        Some(t),
        Some(json!({ "employee_id": emp, "card_id": "01020304" })),
    )
    .await;
    assert_eq!(got.status, 500);
}
