//! 社員の口を実 DB で通す (rust-alc-api の mock_employees_test・employees_bulk の DB テスト・
//! employees_pending_cards_test の要点を写したもの)。接続は本番の worker と同じ実行用ロール `alc_api_rt` (RLS が効く)。

mod support;

use serde_json::{json, Value};
use support::{admin, broken_client, build, call, tenant};
use uuid::Uuid;

const NFC_A: &str = "2023040120280331";
const NFC_B: &str = "2022010120270101";

async fn employee_with(t: Uuid, name: &str, code: Option<&str>, nfc: Option<&str>) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, name, code, nfc_id) VALUES ($1, $2, $3, $4, $5)",
            &[&id, &t, &name, &code, &nfc],
        )
        .await
        .unwrap();
    id
}

async fn exec(sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) {
    admin().await.execute(sql, params).await.unwrap();
}

/// 1 人の (社員番号, 免許証番号, driver_cd, 削除済みか)
async fn row(id: Uuid) -> (Option<String>, Option<String>, Option<String>, bool) {
    let r = admin()
        .await
        .query_one(
            "SELECT code, nfc_id, driver_cd, deleted_at IS NOT NULL FROM alc_api.employees WHERE id = $1",
            &[&id],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1), r.get(2), r.get(3))
}

/// 保留カードを 1 枚入れる
async fn pending_card(t: Uuid, card_id: &str, code: &str) {
    exec(
        "INSERT INTO alc_api.timecard_cards (tenant_id, employee_id, card_id, source, pending_employee_code) \
         VALUES ($1, NULL, $2, 'timecard-ic-ledger-import', $3)",
        &[&t, &card_id, &code],
    )
    .await;
}

async fn card_owner(t: Uuid, card_id: &str) -> (Option<Uuid>, Option<String>) {
    let r = admin()
        .await
        .query_one(
            "SELECT employee_id, pending_employee_code FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2",
            &[&t, &card_id],
        )
        .await
        .unwrap();
    (r.get(0), r.get(1))
}

#[tokio::test]
async fn crud() {
    let app = support::app().await;
    let t = tenant().await;
    let other = tenant().await;

    let created = call(
        &app,
        "POST",
        "/api/employees",
        Some(t),
        Some(json!({ "name": "乙 次郎", "code": "C002" })),
    )
    .await;
    assert_eq!(created.status, 201);
    let e = created.json();
    assert_eq!(e["role"], json!(["driver"]));
    assert_eq!(e["face_approval_status"], "none");
    assert!(e.get("face_embedding").is_none());
    let id = e["id"].as_str().unwrap().to_owned();
    let admin_role = call(
        &app,
        "POST",
        "/api/employees",
        Some(t),
        Some(json!({ "name": "甲 太郎", "code": "C001", "role": ["admin", "driver"] })),
    )
    .await
    .json();
    assert_eq!(admin_role["role"], json!(["admin", "driver"]));
    let gone = employee_with(t, "丙 退職", Some("C003"), None).await;
    exec(
        "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
        &[&gone],
    )
    .await;

    // 一覧は名前の順・在籍だけ。別 tenant からは見えない
    let list = call(&app, "GET", "/api/employees", Some(t), None)
        .await
        .json();
    let names: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["乙 次郎", "甲 太郎"]);
    assert_eq!(
        call(&app, "GET", "/api/employees", Some(other), None)
            .await
            .json(),
        json!([])
    );

    assert_eq!(
        call(&app, "GET", &format!("/api/employees/{id}"), Some(t), None)
            .await
            .json(),
        e
    );
    assert_eq!(
        call(
            &app,
            "GET",
            &format!("/api/employees/{id}"),
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
            "GET",
            &format!("/api/employees/{gone}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );

    // 更新 (役割は省略で据え置き)。同じ社員番号は 409、無ければ 404
    let up = call(
        &app,
        "PUT",
        &format!("/api/employees/{id}"),
        Some(t),
        Some(json!({ "name": "乙 改名", "code": "C002B" })),
    )
    .await;
    assert_eq!(up.status, 200);
    assert_eq!(up.json()["name"], "乙 改名");
    assert_eq!(up.json()["role"], json!(["driver"]));
    let dup = call(
        &app,
        "PUT",
        &format!("/api/employees/{id}"),
        Some(t),
        Some(json!({ "name": "乙", "code": "C001" })),
    )
    .await;
    assert_eq!(dup.status, 409);
    assert_eq!(
        call(
            &app,
            "PUT",
            &format!("/api/employees/{gone}"),
            Some(t),
            Some(json!({ "name": "x" }))
        )
        .await
        .status,
        404
    );

    // 社員番号で引く
    assert_eq!(
        call(&app, "GET", "/api/employees/by-code/C002B", Some(t), None)
            .await
            .json()["id"],
        id
    );
    assert_eq!(
        call(&app, "GET", "/api/employees/by-code/C003", Some(t), None)
            .await
            .status,
        404
    );

    // 削除は論理削除
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/employees/{id}"),
            Some(t),
            None
        )
        .await
        .status,
        204
    );
    assert!(row(id.parse().unwrap()).await.3);
    assert_eq!(
        call(
            &app,
            "DELETE",
            &format!("/api/employees/{id}"),
            Some(t),
            None
        )
        .await
        .status,
        404
    );
    support::assert_logs_clean(&app, &[]);
}

#[tokio::test]
async fn face_license_and_lookup() {
    let app = support::app().await;
    let t = tenant().await;
    let id = employee_with(t, "顔 太郎", Some("F001"), None).await;
    let u = |p: &str| format!("/api/employees/{id}{p}");

    // embedding の次元が違えば 400
    let bad = call(
        &app,
        "PUT",
        &u("/face"),
        Some(t),
        Some(json!({ "face_embedding": [0.1, 0.2] })),
    )
    .await;
    assert_eq!(bad.status, 400);
    // 写真だけなら承認の状態は変わらない
    let photo = call(
        &app,
        "PUT",
        &u("/face"),
        Some(t),
        Some(json!({ "face_photo_url": "r2://alc-media/x.jpg" })),
    )
    .await
    .json();
    assert_eq!(photo["face_photo_url"], "r2://alc-media/x.jpg");
    assert_eq!(photo["face_approval_status"], "none");
    // 承認待ちでなければ承認・却下は 404
    assert_eq!(
        call(&app, "PUT", &u("/face/approve"), Some(t), None)
            .await
            .status,
        404
    );
    // embedding を入れると承認待ち。顔の一覧は承認済みだけ
    let emb: Vec<f64> = (0..1024).map(|i| i as f64 / 1024.0).collect();
    let face = call(
        &app,
        "PUT",
        &u("/face"),
        Some(t),
        Some(json!({ "face_embedding": emb, "face_model_version": "v2" })),
    )
    .await
    .json();
    assert_eq!(face["face_approval_status"], "pending");
    assert_eq!(face["face_model_version"], "v2");
    assert!(face["face_embedding_at"].is_string());
    assert_eq!(
        call(&app, "GET", "/api/employees/face-data", Some(t), None)
            .await
            .json(),
        json!([])
    );
    let approved = call(&app, "PUT", &u("/face/approve"), Some(t), None)
        .await
        .json();
    assert_eq!(approved["face_approval_status"], "approved");
    assert!(approved["face_approved_at"].is_string());
    let data = call(&app, "GET", "/api/employees/face-data", Some(t), None)
        .await
        .json();
    assert_eq!(data[0]["id"], id.to_string());
    assert_eq!(data[0]["face_embedding"].as_array().unwrap().len(), 1024);
    assert_eq!(data[0]["face_embedding"][512], 0.5);
    // 却下は承認待ちのときだけ
    call(
        &app,
        "PUT",
        &u("/face"),
        Some(t),
        Some(json!({ "face_embedding": emb })),
    )
    .await;
    assert_eq!(
        call(&app, "PUT", &u("/face/reject"), Some(t), None)
            .await
            .json()["face_approval_status"],
        "rejected"
    );
    assert_eq!(
        call(&app, "PUT", &u("/face/reject"), Some(t), None)
            .await
            .status,
        404
    );

    // 免許証: PUT の null は「変更なし」、DELETE で 3 列とも消える
    let lic = call(
        &app,
        "PUT",
        &u("/license"),
        Some(t),
        Some(json!({ "license_issue_date": "2023-04-01", "license_expiry_date": "2028-03-31" })),
    )
    .await
    .json();
    assert_eq!(lic["license_issue_date"], "2023-04-01");
    let kept = call(
        &app,
        "PUT",
        &u("/license"),
        Some(t),
        Some(json!({ "license_expiry_date": null })),
    )
    .await
    .json();
    assert_eq!(kept["license_expiry_date"], "2028-03-31");
    let nfc = call(
        &app,
        "PUT",
        &u("/nfc"),
        Some(t),
        Some(json!({ "nfc_id": NFC_A })),
    )
    .await
    .json();
    assert_eq!(nfc["nfc_id"], NFC_A);

    // 免許証番号で照会 (POST だけ)。無い・退職は 404
    let look = |n: &str| {
        let app = &app;
        let n = n.to_owned();
        async move {
            call(
                app,
                "POST",
                "/api/employees/lookup",
                Some(t),
                Some(json!({ "nfc_id": n })),
            )
            .await
        }
    };
    assert_eq!(look(NFC_A).await.json()["id"], id.to_string());
    assert_eq!(look(NFC_B).await.status, 404);
    assert_eq!(
        call(&app, "GET", "/api/employees/lookup", Some(t), None)
            .await
            .status,
        405
    );

    let cleared = call(&app, "DELETE", &u("/license"), Some(t), None)
        .await
        .json();
    assert!(cleared["license_issue_date"].is_null());
    assert!(cleared["license_expiry_date"].is_null());
    assert!(cleared["nfc_id"].is_null());
    assert_eq!(look(NFC_A).await.status, 404);

    // 退職者には効かない (NFC の更新だけは rust と同じく退職者にも効く)
    exec(
        "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
        &[&id],
    )
    .await;
    assert_eq!(
        call(&app, "PUT", &u("/license"), Some(t), Some(json!({})))
            .await
            .status,
        404
    );
    assert_eq!(
        call(&app, "DELETE", &u("/license"), Some(t), None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(&app, "PUT", &u("/face"), Some(t), Some(json!({})))
            .await
            .status,
        404
    );
    assert_eq!(
        call(
            &app,
            "PUT",
            &u("/nfc"),
            Some(t),
            Some(json!({ "nfc_id": NFC_B }))
        )
        .await
        .status,
        200
    );
    support::assert_logs_clean(&app, &[NFC_A.into(), NFC_B.into()]);
}

async fn bulk(app: &support::App, t: Uuid, items: Value) -> support::Got {
    call(
        app,
        "PUT",
        "/api/employees/bulk-by-code",
        Some(t),
        Some(json!({ "items": items })),
    )
    .await
}

#[tokio::test]
async fn bulk_validation() {
    let app = support::app().await;
    let t = tenant().await;
    let empty = bulk(&app, t, json!([])).await;
    assert_eq!(empty.status, 400);
    assert_eq!(String::from_utf8(empty.body).unwrap(), "items が不正です");
    let many: Vec<Value> = (0..501)
        .map(|i| json!({ "code": format!("E{i}"), "name": "x" }))
        .collect();
    assert_eq!(bulk(&app, t, json!(many)).await.status, 400);
    let bad = bulk(
        &app,
        t,
        json!([{ "code": "E1", "name": "x" }, { "code": "E2", "name": "y", "nfc_id": "12" }]),
    )
    .await;
    assert_eq!(bad.status, 400);
    assert_eq!(
        String::from_utf8(bad.body).unwrap(),
        "items[1].nfc_id が不正です"
    );
}

#[tokio::test]
async fn bulk_paths_and_pending_cards() {
    let app = support::app().await;
    let t = tenant().await;

    // (a) 既存の更新 / 削除済みの復活 (driver_cd が別の在籍の社員と衝突 → 落として skip) / nfc_id の衝突 (据え置いて skip)
    let a1 = employee_with(t, "既存 一郎", Some("A001"), None).await;
    let revived = employee_with(t, "復活 二郎", Some("A002"), None).await;
    exec(
        "UPDATE alc_api.employees SET deleted_at = now(), driver_cd = 'D9' WHERE id = $1",
        &[&revived],
    )
    .await;
    employee_with(t, "D9 の持ち主", Some("A900"), None).await;
    exec(
        "UPDATE alc_api.employees SET driver_cd = 'D9' WHERE tenant_id = $1 AND code = 'A900'",
        &[&t],
    )
    .await;
    let holder = employee_with(t, "免許の持ち主", Some("A901"), Some(NFC_A)).await;
    let a3 = employee_with(t, "衝突 三郎", Some("A003"), None).await;
    // (b) 社員番号の無い社員を免許証番号で引いて社員番号を付ける / 別の社員番号を持っていたら skip
    let b1 = employee_with(t, "番号なし 四郎", None, Some(NFC_B)).await;
    employee_with(t, "別番号 五郎", Some("B999"), Some("2021010120260101")).await;
    // (c) 退職者の免許証番号で新規 → 一意の違反で skip
    let ex = employee_with(t, "退職 六郎", Some("X001"), Some("2020010120250101")).await;
    exec(
        "UPDATE alc_api.employees SET deleted_at = now() WHERE id = $1",
        &[&ex],
    )
    .await;
    // 保留カード (どの経路でも結ばれる)
    for (card, code) in [
        ("aaaa0001", "A001"),
        ("aaaa0002", "A002"),
        ("aaaa0003", "A003"),
        ("bbbb0001", "B001"),
        ("cccc0001", "C001"),
        ("zzzz0001", "Z999"),
    ] {
        pending_card(t, card, code).await;
    }

    let res = bulk(
        &app,
        t,
        json!([
            { "code": "A001", "name": "既存 一郎 改", "license_issue_date": "2024-01-01" },
            { "code": "A002", "name": "復活 二郎" },
            { "code": "A003", "name": "衝突 三郎", "nfc_id": NFC_A },
            { "code": "B001", "name": "番号なし 四郎", "nfc_id": NFC_B },
            { "code": "B002", "name": "別番号 五郎", "nfc_id": "2021010120260101" },
            { "code": "C001", "name": "新規 七郎" },
            { "code": "C002", "name": "新規 八郎", "nfc_id": "2020010120250101" },
        ]),
    )
    .await;
    assert_eq!(res.status, 200);
    assert_eq!(
        res.json(),
        json!({
            "created": 1, "updated": 4,
            "skipped": [
                { "code": "A002", "reason": "driver_cd_conflict" },
                { "code": "A003", "reason": "nfc_id_conflict" },
                { "code": "B002", "reason": "nfc_id_conflict" },
                { "code": "C002", "reason": "unique_violation" },
            ]
        })
    );
    assert_eq!(
        row(revived).await,
        (Some("A002".into()), None, None, false),
        "復活し driver_cd は落ちる"
    );
    assert_eq!(row(a3).await.1, None, "衝突した nfc_id は据え置き");
    assert_eq!(row(holder).await.1.as_deref(), Some(NFC_A));
    assert_eq!(
        row(b1).await.0.as_deref(),
        Some("B001"),
        "免許証番号で引いた社員に社員番号が付く"
    );
    let new: Uuid = admin()
        .await
        .query_one(
            "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND code = 'C001'",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);
    for (card, owner) in [
        ("aaaa0001", a1),
        ("aaaa0002", revived),
        ("aaaa0003", a3),
        ("bbbb0001", b1),
        ("cccc0001", new),
    ] {
        assert_eq!(card_owner(t, card).await, (Some(owner), None), "{card}");
    }
    assert_eq!(
        card_owner(t, "zzzz0001").await,
        (None, Some("Z999".into())),
        "別の社員番号の保留は触らない"
    );
    let got = call(&app, "GET", &format!("/api/employees/{a1}"), Some(t), None)
        .await
        .json();
    assert_eq!(got["name"], "既存 一郎 改");
    assert_eq!(got["license_issue_date"], "2024-01-01");
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
            "/api/employees".into(),
            Some(json!({ "name": "x" })),
            "employee create",
        ),
        ("GET", "/api/employees".into(), None, "employee list"),
        ("GET", format!("/api/employees/{id}"), None, "employee get"),
        (
            "PUT",
            format!("/api/employees/{id}"),
            Some(json!({ "name": "x" })),
            "employee update",
        ),
        (
            "DELETE",
            format!("/api/employees/{id}"),
            None,
            "employee delete",
        ),
        (
            "PUT",
            format!("/api/employees/{id}/face"),
            Some(json!({})),
            "employee face",
        ),
        (
            "GET",
            "/api/employees/face-data".into(),
            None,
            "employee face-data",
        ),
        (
            "PUT",
            format!("/api/employees/{id}/license"),
            Some(json!({})),
            "employee license",
        ),
        (
            "PUT",
            format!("/api/employees/{id}/nfc"),
            Some(json!({ "nfc_id": NFC_A })),
            "employee nfc",
        ),
        (
            "DELETE",
            format!("/api/employees/{id}/license"),
            None,
            "employee license clear",
        ),
        (
            "PUT",
            format!("/api/employees/{id}/face/approve"),
            None,
            "employee face approve",
        ),
        (
            "PUT",
            format!("/api/employees/{id}/face/reject"),
            None,
            "employee face reject",
        ),
        (
            "POST",
            "/api/employees/lookup".into(),
            Some(json!({ "nfc_id": NFC_A })),
            "employee lookup",
        ),
        (
            "GET",
            "/api/employees/by-code/X".into(),
            None,
            "employee by-code",
        ),
        (
            "PUT",
            "/api/employees/bulk-by-code".into(),
            Some(json!({ "items": [{ "code": "X", "name": "x" }] })),
            "employee bulk",
        ),
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
    support::assert_logs_clean(&app, &[NFC_A.into(), id.to_string()]);
}
