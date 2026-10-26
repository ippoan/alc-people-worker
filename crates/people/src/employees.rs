//! 社員 (rust の `alc-misc` の employees.rs と repo/employees.rs の写し。alc-app の管理画面・キオスク・測定台・
//! 運行管理者席、乗務員マスタの relay (`bulk-by-code`) が呼ぶ)。
//!
//! - `GET /employees` (名前の順、在籍だけ)・`POST /employees` (201)
//! - `GET/PUT/DELETE /employees/{id}` — 更新は名前・社員番号・役割 (同じ社員番号は 409)。削除は論理削除 (204)
//! - `PUT /employees/{id}/face` — 顔写真・embedding (1024 次元。それ以外は 400)。embedding を変えたら承認待ちに戻す
//! - `PUT /employees/{id}/nfc`・`PUT/DELETE /employees/{id}/license` — 免許証 (PUT は null を「変更なし」、DELETE で消す)
//! - `GET /employees/face-data` — 承認済みの顔の embedding (顔照合用)
//! - `PUT /employees/{id}/face/approve`・`/reject` — 承認待ちのものだけ
//! - `POST /employees/lookup` — 免許証番号から在籍の社員 (読み取り専用。番号を URL に載せない)
//! - `GET /employees/by-code/{code}` — 社員番号から在籍の社員
//! - `PUT /employees/bulk-by-code` — 乗務員マスタの一括取り込み (1〜500 件。衝突は 1 件ずつ skip して 200)
//!
//! rust との違い: SQL に tenant_id を明示した (rust は RLS 任せの文もあった)。
//! ★ **`nfc_id` は応答の外 (ログ) に出さない** (照会の口の呼び手は端末の鍵で、応答がログに写る)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::timecard::MAX_BULK_UPSERT_ITEMS;
use crate::AppState;

/// 社員番号・名前の長さの上限 (rust と同じ)
const MAX_CODE_LEN: usize = 64;
const MAX_NAME_LEN: usize = 200;
/// 顔の embedding の次元 (Human.js の faceres モデル)
const FACE_EMBEDDING_DIM: usize = 1024;
/// 一意の制約 (同じ tenant の同じ社員番号)。更新でこれに当たったら 409
const CODE_UNIQUE_CONSTRAINT: &str = "idx_employees_code";

/// rust の `Employee` (`face_embedding` は rust と同じく出さない)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Employee {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub code: Option<String>,
    pub nfc_id: Option<String>,
    pub name: String,
    pub face_photo_url: Option<String>,
    pub face_embedding_at: Option<DateTime<Utc>>,
    pub face_model_version: Option<String>,
    pub face_approval_status: String,
    pub face_approved_by: Option<Uuid>,
    pub face_approved_at: Option<DateTime<Utc>>,
    pub license_issue_date: Option<NaiveDate>,
    pub license_expiry_date: Option<NaiveDate>,
    pub role: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for Employee {}

fn default_driver() -> Vec<String> {
    vec!["driver".to_owned()]
}

/// rust の `CreateEmployee`
#[derive(Debug, Deserialize)]
pub struct CreateEmployee {
    pub code: Option<String>,
    pub nfc_id: Option<String>,
    pub name: String,
    #[serde(default = "default_driver")]
    pub role: Vec<String>,
}

/// rust の `UpdateEmployee`
#[derive(Debug, Deserialize)]
pub struct UpdateEmployee {
    pub name: String,
    pub code: Option<String>,
    pub role: Option<Vec<String>>,
}

/// rust の `UpdateFace`
#[derive(Debug, Deserialize)]
pub struct UpdateFace {
    pub face_photo_url: Option<String>,
    pub face_embedding: Option<Vec<f64>>,
    pub face_model_version: Option<String>,
}

/// rust の `FaceDataEntry`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FaceDataEntry {
    pub id: Uuid,
    pub face_embedding: Option<Vec<f64>>,
    pub face_embedding_at: Option<DateTime<Utc>>,
    pub face_model_version: Option<String>,
    pub face_approval_status: String,
}

impl TxOutput for FaceDataEntry {}

/// rust の `UpdateLicense`
#[derive(Debug, Deserialize)]
pub struct UpdateLicense {
    pub license_issue_date: Option<NaiveDate>,
    pub license_expiry_date: Option<NaiveDate>,
}

/// rust の `UpdateNfcId` / `EmployeeLookupByNfc`
#[derive(Debug, Deserialize)]
pub struct NfcId {
    pub nfc_id: String,
}

/// rust の `EmployeeUpsertItem`
#[derive(Debug, Clone, Deserialize)]
pub struct UpsertItem {
    pub code: String,
    pub name: String,
    pub nfc_id: Option<String>,
    pub license_issue_date: Option<NaiveDate>,
    pub license_expiry_date: Option<NaiveDate>,
}

/// rust の `EmployeeBulkUpsert`
#[derive(Debug, Deserialize)]
pub struct BulkUpsert {
    pub items: Vec<UpsertItem>,
}

/// rust の `EmployeeUpsertSkipped` (reason は `nfc_id_conflict` / `driver_cd_conflict` / `unique_violation`)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Skipped {
    pub code: String,
    pub reason: String,
}

/// rust の `EmployeeUpsertSummary`
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct UpsertSummary {
    pub created: usize,
    pub updated: usize,
    pub skipped: Vec<Skipped>,
}

impl TxOutput for UpsertSummary {}

/// 免許証番号は「交付日 8 桁 + 有効期限 8 桁」= 数字 16 桁 (一括取り込みの検証)
pub fn valid_bulk_nfc_id(nfc_id: &str) -> bool {
    nfc_id.len() == 16 && nfc_id.chars().all(|c| c.is_ascii_digit())
}

/// 一括の 1 件の検証。不正なら理由 (index 込み。rust と同じ文言) を返す
pub fn validate_bulk_item(idx: usize, item: &UpsertItem) -> Result<(), String> {
    if item.code.is_empty() || item.code.chars().count() > MAX_CODE_LEN {
        return Err(format!("items[{idx}].code が不正です"));
    }
    if item.name.is_empty() || item.name.chars().count() > MAX_NAME_LEN {
        return Err(format!("items[{idx}].name が不正です"));
    }
    if item
        .nfc_id
        .as_deref()
        .is_some_and(|n| !valid_bulk_nfc_id(n))
    {
        return Err(format!("items[{idx}].nfc_id が不正です"));
    }
    Ok(())
}

pub mod sql {
    /// 応答の列 (`face_embedding` は出さないので読まない)
    pub const COLUMNS: &str = "id, tenant_id, code, nfc_id, name, face_photo_url, face_embedding_at, \
         face_model_version, face_approval_status, face_approved_by, face_approved_at, license_issue_date, \
         license_expiry_date, role, created_at, updated_at, deleted_at";

    pub const INSERT: &str = const_format::concatcp!(
        "INSERT INTO alc_api.employees (tenant_id, code, nfc_id, name, role) \
         VALUES ($1, $2, $3, $4, $5) RETURNING ",
        COLUMNS
    );

    pub const LIST: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.employees WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY name"
    );

    /// 在籍の社員 1 人 (カードの照会もこれを使う)
    pub const GET: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.employees WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL"
    );

    pub const GET_BY_NFC: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.employees WHERE tenant_id = $1 AND nfc_id = $2 AND deleted_at IS NULL"
    );

    pub const GET_BY_CODE: &str = const_format::concatcp!(
        "SELECT ",
        COLUMNS,
        " FROM alc_api.employees WHERE tenant_id = $1 AND code = $2 AND deleted_at IS NULL"
    );

    pub const UPDATE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET name = $3, code = $4, role = COALESCE($5, role), updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL RETURNING ",
        COLUMNS
    );

    pub const DELETE: &str = "UPDATE alc_api.employees SET deleted_at = NOW(), updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL";

    /// embedding を変えたら時刻・版を付け直し、承認待ちに戻す (rust と同じ)
    pub const UPDATE_FACE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET \
             face_photo_url = COALESCE($3, face_photo_url), \
             face_embedding = COALESCE($4, face_embedding), \
             face_embedding_at = CASE WHEN $4 IS NOT NULL THEN NOW() ELSE face_embedding_at END, \
             face_model_version = CASE WHEN $4 IS NOT NULL THEN $5 ELSE face_model_version END, \
             face_approval_status = CASE WHEN $4 IS NOT NULL THEN 'pending' ELSE face_approval_status END, \
             updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL RETURNING ",
        COLUMNS
    );

    pub const FACE_DATA: &str =
        "SELECT id, face_embedding, face_embedding_at, face_model_version, face_approval_status \
         FROM alc_api.employees \
         WHERE tenant_id = $1 AND deleted_at IS NULL AND face_embedding IS NOT NULL \
           AND face_approval_status = 'approved'";

    /// null は「変更なし」(消すのは CLEAR_LICENSE)
    pub const UPDATE_LICENSE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET \
             license_issue_date = COALESCE($3, license_issue_date), \
             license_expiry_date = COALESCE($4, license_expiry_date), \
             updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL RETURNING ",
        COLUMNS
    );

    /// rust と同じく deleted_at は見ない
    pub const UPDATE_NFC: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET nfc_id = $3, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 RETURNING ",
        COLUMNS
    );

    /// 交付日・有効期限・nfc_id を一緒に消す (nfc_id を残すと免許証の照合で名前が出続ける)
    pub const CLEAR_LICENSE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET \
             license_issue_date = NULL, license_expiry_date = NULL, nfc_id = NULL, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL RETURNING ",
        COLUMNS
    );

    pub const APPROVE_FACE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET \
             face_approval_status = 'approved', face_approved_at = NOW(), updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND face_approval_status = 'pending' AND deleted_at IS NULL \
         RETURNING ",
        COLUMNS
    );

    pub const REJECT_FACE: &str = const_format::concatcp!(
        "UPDATE alc_api.employees SET face_approval_status = 'rejected', updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2 AND face_approval_status = 'pending' AND deleted_at IS NULL \
         RETURNING ",
        COLUMNS
    );

    // --- 一括取り込み (rust の upsert_by_code と同じ文と順) ---

    /// (a) 社員番号の一致。**deleted_at は問わない** (`idx_employees_code` が deleted_at を見ない一意制約なので、
    /// 削除済みを無視すると INSERT が衝突する。見つかれば復活させる)
    pub const BULK_BY_CODE: &str =
        "SELECT id, driver_cd FROM alc_api.employees WHERE tenant_id = $1 AND code = $2";

    /// 復活させる行の driver_cd を、別の在籍の社員が持っていないか
    pub const BULK_DRIVER_CD_TAKEN: &str = "SELECT 1 FROM alc_api.employees \
         WHERE tenant_id = $1 AND driver_cd = $2 AND deleted_at IS NULL AND id <> $3";

    /// 送られた nfc_id を別の社員 (退職者も含む。`UNIQUE (tenant_id, nfc_id)`) が持っていないか
    pub const BULK_NFC_TAKEN: &str =
        "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND nfc_id = $2 AND id <> $3";

    /// (a) nfc_id が衝突しているとき: nfc_id だけ据え置いて残りを更新・復活
    pub const BULK_UPDATE_KEEP_NFC: &str = "UPDATE alc_api.employees SET \
             name = $3, license_issue_date = $4, license_expiry_date = $5, \
             driver_cd = CASE WHEN $6::BOOLEAN THEN NULL ELSE driver_cd END, \
             deleted_at = NULL, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2";

    /// (a) 通常の更新・復活
    pub const BULK_UPDATE: &str = "UPDATE alc_api.employees SET \
             name = $3, nfc_id = $4, license_issue_date = $5, license_expiry_date = $6, \
             driver_cd = CASE WHEN $7::BOOLEAN THEN NULL ELSE driver_cd END, \
             deleted_at = NULL, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2";

    /// (b) 社員番号が未登録なら、在籍の社員を nfc_id で引く
    pub const BULK_BY_NFC: &str = "SELECT id, code FROM alc_api.employees \
         WHERE tenant_id = $1 AND nfc_id = $2 AND deleted_at IS NULL";

    /// (b) nfc_id で 1 人に決まり、社員番号が無い / 同じなら社員番号を付ける
    pub const BULK_ASSIGN_CODE: &str = "UPDATE alc_api.employees SET \
             code = $3, name = $4, license_issue_date = $5, license_expiry_date = $6, updated_at = NOW() \
         WHERE tenant_id = $1 AND id = $2";

    /// (c) 新規 (残る一意の違反は ON CONFLICT DO NOTHING で拾って skip)
    pub const BULK_INSERT: &str = "INSERT INTO alc_api.employees \
         (tenant_id, code, nfc_id, name, role, license_issue_date, license_expiry_date) \
         VALUES ($1, $2, $3, $4, ARRAY['driver'], $5, $6) \
         ON CONFLICT DO NOTHING RETURNING id";

    /// 社員が現れたら、その社員番号で保留になっているカード (ippoan/alc-people-worker の timecard の一括取り込みが
    /// 入れた `employee_id IS NULL` の行) を結ぶ。**社員マスタ同期と同じ transaction で走らせる**
    /// (別にすると「社員は入ったがカードは保留のまま」が残る)。既に結び付いている行は触らない
    pub const LINK_PENDING_CARDS: &str = "UPDATE alc_api.timecard_cards \
         SET employee_id = $3, pending_employee_code = NULL \
         WHERE tenant_id = $1 AND pending_employee_code = $2 AND employee_id IS NULL";
}

/// 応答の 1 行 ([`sql::COLUMNS`] の順)
pub fn employee(r: &Row) -> Employee {
    Employee {
        id: r.get(0),
        tenant_id: r.get(1),
        code: r.get(2),
        nfc_id: r.get(3),
        name: r.get(4),
        face_photo_url: r.get(5),
        face_embedding_at: r.get(6),
        face_model_version: r.get(7),
        face_approval_status: r.get(8),
        face_approved_by: r.get(9),
        face_approved_at: r.get(10),
        license_issue_date: r.get(11),
        license_expiry_date: r.get(12),
        role: r.get(13),
        created_at: r.get(14),
        updated_at: r.get(15),
        deleted_at: r.get(16),
    }
}

fn face_entry(r: &Row) -> FaceDataEntry {
    FaceDataEntry {
        id: r.get(0),
        face_embedding: r.get(1),
        face_embedding_at: r.get(2),
        face_model_version: r.get(3),
        face_approval_status: r.get(4),
    }
}

/// 更新の失敗が社員番号の一意の制約か
pub fn is_code_conflict(e: &Error) -> bool {
    e.as_db_error()
        .and_then(|db| db.constraint())
        .is_some_and(|c| c == CODE_UNIQUE_CONSTRAINT)
}

// --- DB (どれも 1 つの tenant_tx) ---

/// 社員 1 人を返す文を 1 回流す (取得・更新の多くがこの形)
async fn one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    sql: &'static str,
    id: Uuid,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql, &[(&tenant_id, Type::UUID), (&id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

/// 社員 1 人を文字列の鍵 (免許証番号・社員番号) で引く
async fn one_by(
    pg: &mut PgClient,
    tenant_id: Uuid,
    sql: &'static str,
    key: String,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql, &[(&tenant_id, Type::UUID), (&key, Type::TEXT)])
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    b: CreateEmployee,
) -> Result<Employee, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&b.code, Type::TEXT),
                        (&b.nfc_id, Type::TEXT),
                        (&b.name, Type::TEXT),
                        (&b.role, Type::TEXT_ARRAY),
                    ],
                )
                .await?;
            Ok(employee(&row))
        })
    })
    .await
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(employee).collect())
        })
    })
    .await
}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    b: UpdateEmployee,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&b.name, Type::TEXT),
                        (&b.code, Type::TEXT),
                        (&b.role, Type::TEXT_ARRAY),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql::DELETE, &[(&tenant_id, Type::UUID), (&id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn update_face(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    b: UpdateFace,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE_FACE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&b.face_photo_url, Type::TEXT),
                        (&b.face_embedding, Type::FLOAT8_ARRAY),
                        (&b.face_model_version, Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

pub async fn face_data(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<FaceDataEntry>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::FACE_DATA, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(face_entry).collect())
        })
    })
    .await
}

pub async fn update_license(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    b: UpdateLicense,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE_LICENSE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&b.license_issue_date, Type::DATE),
                        (&b.license_expiry_date, Type::DATE),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

pub async fn update_nfc(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    nfc_id: String,
) -> Result<Option<Employee>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE_NFC,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&nfc_id, Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(employee))
        })
    })
    .await
}

/// 保留カードを結ぶ ([`sql::LINK_PENDING_CARDS`])
async fn link_pending_cards(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    code: &str,
    employee_id: Uuid,
) -> Result<(), Error> {
    tx.execute_typed(
        sql::LINK_PENDING_CARDS,
        &[
            (&tenant_id, Type::UUID),
            (&code, Type::TEXT),
            (&employee_id, Type::UUID),
        ],
    )
    .await?;
    Ok(())
}

fn skip(summary: &mut UpsertSummary, code: &str, reason: &str) {
    summary.skipped.push(Skipped {
        code: code.to_owned(),
        reason: reason.to_owned(),
    });
}

/// (a) 社員番号が一致した行 (削除済みも) を更新・復活させる。nfc_id・driver_cd が別の社員と衝突するときは、
/// その列だけ据え置き / 落として skip に名指しで残す (1 人のせいで transaction ごと 500 にしない)
async fn bulk_update_existing(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    item: &UpsertItem,
    id: Uuid,
    driver_cd: Option<String>,
    summary: &mut UpsertSummary,
) -> Result<(), Error> {
    let driver_cd_conflict = match &driver_cd {
        Some(d) => tx
            .query_typed_opt(
                sql::BULK_DRIVER_CD_TAKEN,
                &[(&tenant_id, Type::UUID), (d, Type::TEXT), (&id, Type::UUID)],
            )
            .await?
            .is_some(),
        None => false,
    };
    let nfc_conflict = match &item.nfc_id {
        Some(n) => tx
            .query_typed_opt(
                sql::BULK_NFC_TAKEN,
                &[(&tenant_id, Type::UUID), (n, Type::TEXT), (&id, Type::UUID)],
            )
            .await?
            .is_some(),
        None => false,
    };
    if nfc_conflict {
        tx.execute_typed(
            sql::BULK_UPDATE_KEEP_NFC,
            &[
                (&tenant_id, Type::UUID),
                (&id, Type::UUID),
                (&item.name, Type::TEXT),
                (&item.license_issue_date, Type::DATE),
                (&item.license_expiry_date, Type::DATE),
                (&driver_cd_conflict, Type::BOOL),
            ],
        )
        .await?;
    } else {
        tx.execute_typed(
            sql::BULK_UPDATE,
            &[
                (&tenant_id, Type::UUID),
                (&id, Type::UUID),
                (&item.name, Type::TEXT),
                (&item.nfc_id, Type::TEXT),
                (&item.license_issue_date, Type::DATE),
                (&item.license_expiry_date, Type::DATE),
                (&driver_cd_conflict, Type::BOOL),
            ],
        )
        .await?;
    }
    summary.updated += 1;
    // 社員が居る経路すべてで結ぶ (nfc_id の衝突の経路も)
    link_pending_cards(tx, tenant_id, &item.code, id).await?;
    if nfc_conflict {
        skip(summary, &item.code, "nfc_id_conflict");
    }
    if driver_cd_conflict {
        skip(summary, &item.code, "driver_cd_conflict");
    }
    Ok(())
}

/// 一括の 1 件 (rust の upsert_by_code のループの 1 回)
async fn bulk_one(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    item: &UpsertItem,
    summary: &mut UpsertSummary,
) -> Result<(), Error> {
    // (a) 社員番号の一致 (削除済みも)
    if let Some(r) = tx
        .query_typed_opt(
            sql::BULK_BY_CODE,
            &[(&tenant_id, Type::UUID), (&item.code, Type::TEXT)],
        )
        .await?
    {
        return bulk_update_existing(tx, tenant_id, item, r.get(0), r.get(1), summary).await;
    }
    // (b) 社員番号が未登録なら在籍の社員を nfc_id で引く。1 人に決まり社員番号が無い / 同じなら社員番号を付ける
    if let Some(nfc_id) = &item.nfc_id {
        let found = tx
            .query_typed(
                sql::BULK_BY_NFC,
                &[(&tenant_id, Type::UUID), (nfc_id, Type::TEXT)],
            )
            .await?;
        if let [only] = found.as_slice() {
            let id: Uuid = only.get(0);
            let code: Option<String> = only.get(1);
            if code.as_deref().is_none_or(|c| c == item.code) {
                tx.execute_typed(
                    sql::BULK_ASSIGN_CODE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&id, Type::UUID),
                        (&item.code, Type::TEXT),
                        (&item.name, Type::TEXT),
                        (&item.license_issue_date, Type::DATE),
                        (&item.license_expiry_date, Type::DATE),
                    ],
                )
                .await?;
                summary.updated += 1;
                // ここで初めて社員番号がこの社員に付く (保留カードは社員番号で待っている)
                link_pending_cards(tx, tenant_id, &item.code, id).await?;
                return Ok(());
            }
        }
        if !found.is_empty() {
            skip(summary, &item.code, "nfc_id_conflict");
            return Ok(());
        }
    }
    // (c) 新規
    let inserted = tx
        .query_typed_opt(
            sql::BULK_INSERT,
            &[
                (&tenant_id, Type::UUID),
                (&item.code, Type::TEXT),
                (&item.nfc_id, Type::TEXT),
                (&item.name, Type::TEXT),
                (&item.license_issue_date, Type::DATE),
                (&item.license_expiry_date, Type::DATE),
            ],
        )
        .await?;
    match inserted {
        Some(r) => {
            summary.created += 1;
            link_pending_cards(tx, tenant_id, &item.code, r.get(0)).await?;
        }
        None => skip(summary, &item.code, "unique_violation"),
    }
    Ok(())
}

/// 乗務員マスタの一括取り込み (1 つの transaction で順に。渡すのは検証を通した items だけ)
pub async fn bulk_upsert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    items: Vec<UpsertItem>,
) -> Result<UpsertSummary, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut summary = UpsertSummary::default();
            for item in &items {
                bulk_one(tx, tenant_id, item, &mut summary).await?;
            }
            Ok(summary)
        })
    })
    .await
}

// --- 口 ---

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/employees", post(create_handler).get(list_handler))
        .route(
            "/employees/{id}",
            get(get_handler).put(update_handler).delete(delete_handler),
        )
        .route("/employees/{id}/face", put(update_face_handler))
        .route("/employees/{id}/nfc", put(update_nfc_handler))
        .route(
            "/employees/{id}/license",
            put(update_license_handler).delete(clear_license_handler),
        )
        .route("/employees/face-data", get(face_data_handler))
        .route("/employees/{id}/face/approve", put(approve_face_handler))
        .route("/employees/{id}/face/reject", put(reject_face_handler))
        // POST だけを登録する (呼び出し元の allowlist は method を見ないので、他 method は axum の 405 で閉じる)
        .route("/employees/lookup", post(lookup_handler))
        .route("/employees/by-code/{code}", get(by_code_handler))
        .route("/employees/bulk-by-code", put(bulk_handler))
}

type One = Result<Json<Employee>, StatusCode>;

fn found(state: &AppState, stage: &str, r: Result<Option<Employee>, Error>) -> One {
    r.map_err(|e| state.db_fail(stage, &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(b): Json<CreateEmployee>,
) -> Result<(StatusCode, Json<Employee>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let e = create(&mut pg, tenant_id, b)
        .await
        .map_err(|e| state.db_fail("employee create", &e))?;
    Ok((StatusCode::CREATED, Json(e)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<Employee>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("employee list", &e))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one(&mut pg, tenant_id, sql::GET, id).await;
    found(&state, "employee get", r)
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(b): Json<UpdateEmployee>,
) -> One {
    let mut pg = state.pg.lock().await;
    match update(&mut pg, tenant_id, id, b).await {
        Err(e) if is_code_conflict(&e) => Err(StatusCode::CONFLICT),
        r => found(&state, "employee update", r),
    }
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    if delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("employee delete", &e))?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn update_face_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(b): Json<UpdateFace>,
) -> One {
    if b.face_embedding
        .as_ref()
        .is_some_and(|v| v.len() != FACE_EMBEDDING_DIM)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let r = update_face(&mut pg, tenant_id, id, b).await;
    found(&state, "employee face", r)
}

async fn face_data_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<FaceDataEntry>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    face_data(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("employee face-data", &e))
}

async fn update_license_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(b): Json<UpdateLicense>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = update_license(&mut pg, tenant_id, id, b).await;
    found(&state, "employee license", r)
}

async fn update_nfc_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(b): Json<NfcId>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = update_nfc(&mut pg, tenant_id, id, b.nfc_id).await;
    found(&state, "employee nfc", r)
}

async fn clear_license_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one(&mut pg, tenant_id, sql::CLEAR_LICENSE, id).await;
    found(&state, "employee license clear", r)
}

async fn approve_face_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one(&mut pg, tenant_id, sql::APPROVE_FACE, id).await;
    found(&state, "employee face approve", r)
}

async fn reject_face_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one(&mut pg, tenant_id, sql::REJECT_FACE, id).await;
    found(&state, "employee face reject", r)
}

/// ★ この口に書き込みを足さない (端末の鍵に開ける根拠が「読み取り専用」であること)
async fn lookup_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(b): Json<NfcId>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one_by(&mut pg, tenant_id, sql::GET_BY_NFC, b.nfc_id).await;
    found(&state, "employee lookup", r)
}

async fn by_code_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(code): Path<String>,
) -> One {
    let mut pg = state.pg.lock().await;
    let r = one_by(&mut pg, tenant_id, sql::GET_BY_CODE, code).await;
    found(&state, "employee by-code", r)
}

/// items が 0 件・上限超え・1 件でも形が不正なら 400 (rust と同じく本文は文字列)。衝突は skip して 200
async fn bulk_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(b): Json<BulkUpsert>,
) -> Result<Json<UpsertSummary>, (StatusCode, String)> {
    if b.items.is_empty() || b.items.len() > MAX_BULK_UPSERT_ITEMS {
        return Err((StatusCode::BAD_REQUEST, "items が不正です".to_owned()));
    }
    for (idx, item) in b.items.iter().enumerate() {
        validate_bulk_item(idx, item).map_err(|msg| (StatusCode::BAD_REQUEST, msg))?;
    }
    let mut pg = state.pg.lock().await;
    bulk_upsert(&mut pg, tenant_id, b.items)
        .await
        .map(Json)
        .map_err(|e| {
            (
                state.db_fail("employee bulk", &e),
                "internal error".to_owned(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(code: &str, name: &str, nfc: Option<&str>) -> UpsertItem {
        UpsertItem {
            code: code.to_owned(),
            name: name.to_owned(),
            nfc_id: nfc.map(str::to_owned),
            license_issue_date: None,
            license_expiry_date: None,
        }
    }

    #[test]
    fn bulk_nfc_id_is_16_digits() {
        assert!(valid_bulk_nfc_id("2023040120280331"));
        assert!(!valid_bulk_nfc_id("202304012028033"));
        assert!(!valid_bulk_nfc_id("202304012028033a"));
    }

    #[test]
    fn bulk_item_validation_is_the_rust_rule() {
        assert_eq!(validate_bulk_item(0, &item("E1", "名前", None)), Ok(()));
        assert_eq!(
            validate_bulk_item(0, &item("E1", "名前", Some("2023040120280331"))),
            Ok(())
        );
        assert_eq!(
            validate_bulk_item(1, &item("", "名前", None)),
            Err("items[1].code が不正です".to_owned())
        );
        assert_eq!(
            validate_bulk_item(2, &item(&"c".repeat(65), "名前", None)),
            Err("items[2].code が不正です".to_owned())
        );
        assert_eq!(
            validate_bulk_item(3, &item("E1", "", None)),
            Err("items[3].name が不正です".to_owned())
        );
        assert_eq!(
            validate_bulk_item(4, &item("E1", &"名".repeat(201), None)),
            Err("items[4].name が不正です".to_owned())
        );
        assert_eq!(
            validate_bulk_item(5, &item("E1", "名前", Some("123"))),
            Err("items[5].nfc_id が不正です".to_owned())
        );
    }
}
