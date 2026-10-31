//! タイムカード (rust の `alc-misc` の timecard.rs・repo/timecard.rs と `alc-core` の repository/timecard.rs の写し。
//! alc-app の管理画面・キオスク、運行管理者席の IT点呼、tc-ohishi のカード同期が呼ぶ)。
//!
//! - `GET /timecard/cards` (`?employee_id=`) — 台帳 (作成の順)
//! - `POST /timecard/cards` (201) — 1 枚登録。card_id は照合と同じ正規化形で入れる。同じ card_id は 409
//! - `GET /timecard/cards/{id}`・`DELETE /timecard/cards/{id}` (204) — 無ければ 404
//! - `PUT /timecard/cards/bulk-by-code` — 社員番号キーの一括取り込み (1〜500 件。skip があっても 200。`dry_run` は書かない)
//! - `POST /timecard/cards/delete-by-card` — 同期で入れたカード 1 枚を外す (範囲外・不在・形不正も 200 + `reason`)
//! - `POST /timecard/cards/lookup` — カードから社員を返す読み取り専用の口 (未登録・保留・退職は 404)
//! - `GET /timecard/punches` (`?employee_id=&date_from=&date_to=&page=&per_page=`) — 打刻の一覧 (`hub_measurements` から導出)
//! - `GET /timecard/punches/csv` — 同じ打刻の CSV (BOM 付き)
//!
//! rust との違い:
//! - SQL に tenant_id を明示した (rust は RLS 任せの文もあった)。打刻の絞り込みは文を組み立てず、
//!   `($n IS NULL OR ...)` の 1 文にした (bind の順がずれる余地を無くす)
//! - `dry_run` は transaction の中の SAVEPOINT へ戻すことで書かない (rust は transaction を rollback。判定は同じコードを通る)
//! - カードの照会は「カードの照合」と「社員の取得」を 1 つの transaction にした
//!
//! ★ **`card_id` は応答にもログにも出さない** (呼び出し元が public repo の Worker で、応答がそのままログや issue に写るため)。

use std::collections::HashSet;

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::employees::Employee;
use crate::{set_device_dev, AppState};

/// 一括の上限 (rust の `alc_core::models::MAX_BULK_UPSERT_ITEMS`。社員の一括取り込みと共有する値)
pub const MAX_BULK_UPSERT_ITEMS: usize = 500;

/// 一括取り込みが入れた行に刻む出所 (rust の `CARD_SOURCE_LEDGER_SYNC`)。
/// **`delete-by-card` が消してよい範囲はこの値ちょうど** (alc 側で直接登録したカード `source IS NULL` を巻き込まない)。
/// body から受け取らない (送り手が書けると直接登録したカードまで消せる)
pub const CARD_SOURCE_LEDGER_SYNC: &str = "timecard-ic-ledger-import";

/// 一意の制約 (同じ tenant の同じ card_id)。作成でこれに当たったら 409
const CARD_UNIQUE_CONSTRAINT: &str = "idx_timecard_cards_unique";

// --- 行と本文 (rust の alc_core::models の写し。JSON の形は同じ) ---

/// rust の `TimecardCard`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimecardCard {
    pub id: Uuid,
    pub tenant_id: Uuid,
    /// 持ち主。社員がまだ居ないカードは None (社員番号は `pending_employee_code`)
    pub employee_id: Option<Uuid>,
    pub card_id: String,
    pub label: Option<String>,
    /// 出所。一括取り込みが入れた行だけ [`CARD_SOURCE_LEDGER_SYNC`]
    pub source: Option<String>,
    pub pending_employee_code: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for TimecardCard {}

/// rust の `CreateTimecardCard`
#[derive(Debug, Deserialize)]
pub struct CreateCard {
    pub employee_id: Uuid,
    pub card_id: String,
    pub label: Option<String>,
}

/// rust の `TimecardCardUpsertItem`
#[derive(Debug, Clone, Deserialize)]
pub struct UpsertItem {
    pub code: String,
    pub card_id: String,
    pub label: Option<String>,
}

/// rust の `TimecardCardConflictPolicy` (別の社員に付いているカードを付け替えるか)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    #[default]
    Skip,
    Reassign,
}

/// rust の `TimecardCardBulkUpsert`
#[derive(Debug, Deserialize)]
pub struct BulkUpsert {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub on_conflict: ConflictPolicy,
    pub items: Vec<UpsertItem>,
}

/// rust の `TimecardCardUpsertSkipped` (reason は `invalid_card_id` / `card_owner_conflict` / `duplicate_in_batch`)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Skipped {
    pub index: usize,
    pub code: String,
    pub reason: String,
}

impl TxOutput for Skipped {}

/// rust の `TimecardCardUpsertSummary`。`pending` は created / updated / unchanged と重なる別軸の数え方
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct UpsertSummary {
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub pending: usize,
    pub skipped: Vec<Skipped>,
}

impl TxOutput for UpsertSummary {}

/// rust の `TimecardCardDeleteByCard`
#[derive(Debug, Deserialize)]
pub struct DeleteByCard {
    pub card_id: String,
    #[serde(default)]
    pub dry_run: bool,
}

/// rust の `TimecardCardDeleteResult` (reason は `deleted` / `not_found` / `out_of_scope` / `invalid_card_id`)
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct DeleteResult {
    pub deleted: usize,
    pub reason: String,
    /// 誰のカードを外したか (`employees.code`、保留行なら `pending_employee_code`)。消さなかったときは None
    pub code: Option<String>,
}

impl TxOutput for DeleteResult {}

/// rust の `TimePunchFilter`
#[derive(Debug, Default, Deserialize)]
pub struct PunchFilter {
    pub employee_id: Option<Uuid>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `TimePunchWithDevice` (打刻 1 件。`id` は `hub_measurements.id`)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Punch {
    pub id: Uuid,
    pub tenant_id: Uuid,
    /// 解決できた社員。未登録カードのタップでは None
    pub employee_id: Option<Uuid>,
    /// 常に None (rust と同じ。どの端末かは `device_name`)
    pub device_id: Option<Uuid>,
    pub device_name: Option<String>,
    pub card_id: Option<String>,
    /// `timecard` (打刻機のタップ) か `license` (点呼開始時の免許証)
    pub kind: String,
    /// `license` / `felica_idm` / `nfca_uid`。表示専用
    pub card_kind: Option<String>,
    pub employee_name: Option<String>,
    pub punched_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub measurement_id: Option<Uuid>,
    pub alcohol_level: Option<f64>,
    pub result: Option<String>,
    pub tenko_recorded: Option<bool>,
    /// 結ばれた測定の点呼の方法 (`通常点呼` / `IT点呼` / `遠隔点呼` / `自動点呼`)。点呼になっていなければ None
    pub tenko_method: Option<String>,
    /// IT点呼 を運行管理者が通話 (`it`) と対面 (`in_person`) のどちらで確定したか。未確定・IT点呼 でなければ None
    pub manager_judgment_method: Option<String>,
}

impl TxOutput for Punch {}

/// rust の `TimePunchesResponse`
#[derive(Debug, Serialize)]
pub struct PunchesResponse {
    pub punches: Vec<Punch>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

/// rust の `TimePunchCsvRow`
#[derive(Debug, Clone, PartialEq)]
pub struct CsvRow {
    pub id: Uuid,
    pub punched_at: DateTime<Utc>,
    pub employee_name: Option<String>,
    pub employee_code: Option<String>,
    pub device_name: Option<String>,
    pub kind: String,
    pub card_kind: Option<String>,
    pub measurement_id: Option<Uuid>,
    pub alcohol_level: Option<f64>,
    pub result: Option<String>,
    pub tenko_recorded: Option<bool>,
}

impl TxOutput for CsvRow {}

// --- 正規化と形の検査 (rust の alc_core::repository::timecard の写し。純粋) ---

/// card_id の正規化 (trim + 小文字 + `':'` 除去)。**照合・登録・削除の唯一の正規化点**。
/// 打刻の一覧の SQL (`lower(replace(btrim(..), ':', ''))`) と同じ規則 (片方だけ変えると照合が静かに外れる)
pub fn normalize_card_id(card_id: &str) -> String {
    card_id.trim().to_lowercase().replace(':', "")
}

/// 一括取り込みが受ける形: `[0-9a-f]` で長さ 8 / 14 / 16 (Mifare UID 4・7 バイトと FeliCa IDm 8 バイト)。
/// **正規化の後に掛ける**
pub fn is_valid_bulk_card_id(normalized: &str) -> bool {
    matches!(normalized.len(), 8 | 14 | 16)
        && normalized
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// 正規化・形の検査・バッチ内の重複の除去を通した 1 件
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedCard {
    pub index: usize,
    pub code: String,
    pub card_id: String,
    pub label: Option<String>,
}

/// DB を引かずに決まる分 (正規化・形・バッチ内の重複) を先に分ける。重複は後勝ちにせず skip
pub fn prepare_bulk_cards(items: &[UpsertItem]) -> (Vec<PreparedCard>, Vec<Skipped>) {
    let mut prepared = Vec::new();
    let mut skipped = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in items.iter().enumerate() {
        let card_id = normalize_card_id(&item.card_id);
        let reason = if !is_valid_bulk_card_id(&card_id) {
            "invalid_card_id"
        } else if !seen.insert(card_id.clone()) {
            "duplicate_in_batch"
        } else {
            prepared.push(PreparedCard {
                index,
                code: item.code.clone(),
                card_id,
                label: item.label.clone(),
            });
            continue;
        };
        skipped.push(Skipped {
            index,
            code: item.code.clone(),
            reason: reason.to_owned(),
        });
    }
    (prepared, skipped)
}

/// 打刻の一覧の頁 (rust と同じ: per_page は既定 50・上限 200、page は 1 以上)。(page, per_page, offset)
pub fn punch_paging(page: Option<i64>, per_page: Option<i64>) -> (i64, i64, i64) {
    let per_page = per_page.unwrap_or(50).min(200);
    let page = page.unwrap_or(1).max(1);
    (page, per_page, (page - 1) * per_page)
}

/// CSV の「区分」列。未知の値はそのまま出す (隠すと診断できない)
pub fn csv_kind_label(kind: &str) -> String {
    match kind {
        "timecard" => "打刻".to_owned(),
        "license" => "点呼".to_owned(),
        other => other.to_owned(),
    }
}

/// CSV の「カード」列。未知の値はそのまま、None (ブラウザ打刻・旧行) は空欄
pub fn csv_card_kind_label(card_kind: Option<&str>) -> String {
    match card_kind {
        Some("license") => "免許証".to_owned(),
        Some("felica_idm") | Some("nfca_uid") => "ICカード".to_owned(),
        Some(other) => other.to_owned(),
        None => String::new(),
    }
}

/// CSV の「点呼記録」列。結ばれた測定が無い打刻は空欄
pub fn csv_tenko_recorded_label(tenko_recorded: Option<bool>) -> &'static str {
    match tenko_recorded {
        Some(true) => "済",
        Some(false) => "未",
        None => "",
    }
}

/// CSV の見出し (rust と同じ 11 列。区分列は必須 — 無いと点呼が打刻として集計される)
pub const CSV_HEADER: [&str; 11] = [
    "ID",
    "区分",
    "カード",
    "社員コード",
    "社員名",
    "打刻日時",
    "デバイス",
    "測定ID",
    "アルコール値",
    "判定",
    "点呼記録",
];

/// CSV の本体 (UTF-8 の BOM + 見出し + 行。打刻日時は JST)
pub fn render_csv(rows: &[CsvRow]) -> Vec<u8> {
    let jst = chrono::FixedOffset::east_opt(9 * 3600).expect("JST");
    let mut wtr = csv::Writer::from_writer(vec![0xEF, 0xBB, 0xBF]);
    // Vec への書き込みは失敗しない (csv の失敗は io の失敗だけ)
    wtr.write_record(CSV_HEADER).expect("csv header");
    for r in rows {
        wtr.write_record([
            r.id.to_string(),
            csv_kind_label(&r.kind),
            csv_card_kind_label(r.card_kind.as_deref()),
            r.employee_code.clone().unwrap_or_default(),
            r.employee_name.clone().unwrap_or_default(),
            r.punched_at
                .with_timezone(&jst)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            r.device_name.clone().unwrap_or_default(),
            r.measurement_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            r.alcohol_level.map(|v| v.to_string()).unwrap_or_default(),
            r.result.clone().unwrap_or_default(),
            csv_tenko_recorded_label(r.tenko_recorded).to_owned(),
        ])
        .expect("csv row");
    }
    wtr.into_inner().expect("csv flush")
}

pub mod sql {
    const CARD_COLUMNS: &str =
        "id, tenant_id, employee_id, card_id, label, source, pending_employee_code, created_at";

    pub const INSERT_CARD: &str = const_format::concatcp!(
        "INSERT INTO alc_api.timecard_cards (tenant_id, employee_id, card_id, label) \
         VALUES ($1, $2, $3, $4) RETURNING ",
        CARD_COLUMNS
    );

    pub const LIST_CARDS: &str = const_format::concatcp!(
        "SELECT ",
        CARD_COLUMNS,
        " FROM alc_api.timecard_cards \
         WHERE tenant_id = $1 AND ($2::uuid IS NULL OR employee_id = $2) ORDER BY created_at"
    );

    pub const GET_CARD: &str = const_format::concatcp!(
        "SELECT ",
        CARD_COLUMNS,
        " FROM alc_api.timecard_cards WHERE tenant_id = $1 AND id = $2"
    );

    pub const DELETE_CARD: &str =
        "DELETE FROM alc_api.timecard_cards WHERE tenant_id = $1 AND id = $2";

    /// 社員番号から社員。**`deleted_at` は問わない** (`idx_employees_code` が deleted_at を見ない一意制約なので、
    /// 削除済みを無視すると「居ないのに code が使われている」状態になる。rust と同じ)
    pub const EMPLOYEE_BY_CODE: &str =
        "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND code = $2";

    /// 一括の 1 件 (rust と同じ 1 文)。`xmax = 0` で INSERT と UPDATE を見分け、DO UPDATE の WHERE が外れた
    /// (同じ持ち主 / 付け替えない指定) ときは 1 行も返らない。
    /// - 比較は `IS DISTINCT FROM` (`<>` は片側 NULL で付け替えが黙って外れる)
    /// - 持ち主の居ない行 (保留行) は reassign を待たずに引き取る
    /// - SET に `pending_employee_code` を必ず含める (落とすと付け替え後の行に古い code が残る)
    /// - SET に `source` を足さない (同じ持ち主の再確認が 1 行も書かない冪等性を保つ)
    pub const UPSERT_CARD: &str = "INSERT INTO alc_api.timecard_cards \
         (tenant_id, employee_id, card_id, label, source, pending_employee_code) \
         VALUES ($1, $2, $3, $4, $5, $7) \
         ON CONFLICT (tenant_id, card_id) DO UPDATE \
             SET employee_id = EXCLUDED.employee_id, \
                 pending_employee_code = EXCLUDED.pending_employee_code \
             WHERE ($6::boolean OR timecard_cards.employee_id IS NULL) \
               AND (timecard_cards.employee_id IS DISTINCT FROM EXCLUDED.employee_id \
                    OR timecard_cards.pending_employee_code IS DISTINCT FROM EXCLUDED.pending_employee_code) \
         RETURNING (xmax = 0) AS inserted";

    /// 書かなかった行の今の持ち主 (保留行は NULL)
    pub const CARD_OWNER: &str =
        "SELECT employee_id FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2";

    /// 同期で入れたカード 1 枚を外す (rust と同じ 1 文)。CTE の中では `gone` の DELETE は `present` から見えないので
    /// 「消えた / 出所が違う / そもそも無い」が 1 往復で分かれる。保留行は employees に当たらないので LEFT JOIN + COALESCE
    pub const DELETE_BY_CARD: &str = "WITH gone AS ( \
             DELETE FROM alc_api.timecard_cards \
             WHERE tenant_id = $1 AND card_id = $2 AND source = $3 \
             RETURNING employee_id, pending_employee_code \
         ), present AS ( \
             SELECT 1 FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2 \
         ) \
         SELECT EXISTS (SELECT 1 FROM gone) AS deleted, \
                EXISTS (SELECT 1 FROM present) AS present, \
                (SELECT COALESCE(e.code, gone.pending_employee_code) \
                   FROM gone LEFT JOIN alc_api.employees e ON e.id = gone.employee_id) AS code";

    /// card_id (正規化済み) から台帳の持ち主 (行が無ければ 0 行、保留行なら NULL)
    pub const CARD_EMPLOYEE: &str =
        "SELECT employee_id FROM alc_api.timecard_cards WHERE tenant_id = $1 AND card_id = $2";

    /// 免許証番号 (`employees.nfc_id`) から社員 (rust と同じく deleted_at は見ない。退職は `employees::sql::GET` で弾く)
    pub const EMPLOYEE_BY_NFC: &str =
        "SELECT id FROM alc_api.employees WHERE tenant_id = $1 AND nfc_id = $2";

    /// 打刻の一覧の共通 CTE (rust の `PUNCHES_CTE` の写し。$1 = tenant)。
    /// - 打刻の一次表は `hub_measurements` だけ
    /// - 社員の解決は 1 本の COALESCE: `payload.employee_id` (ingest で凍結。形を見てから uuid に) → カード → 免許証番号
    /// - 端末の列は、hub の ID の行を結び付いた機体の鍵の ID に畳む
    /// - card_id の正規化は [`super::normalize_card_id`] と同じ規則
    /// - 免許証 1 回のタッチは 1 行にまとめる (対の打刻が 30 秒以内に在る license の行を外す。tenant_id・is_dev も書く)
    const PUNCHES_CTE: &str = r#"
WITH p AS (
    SELECT
        hm.id,
        hm.tenant_id,
        COALESCE(
            CASE WHEN hm.payload->>'employee_id'
                      ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                 THEN (hm.payload->>'employee_id')::uuid END,
            tc.employee_id,
            e_nfc.id
        ) AS employee_id,
        COALESCE(
            (SELECT l.device_id FROM alc_api.hub_measurements l
              WHERE l.tenant_id = hm.tenant_id AND l.is_dev = hm.is_dev
                AND l.via_device_id = hm.device_id
              ORDER BY l.created_at DESC LIMIT 1),
            hm.device_id
        ) AS hub_device_id,
        COALESCE(hm.payload->>'card_id', hm.payload->>'nfc_id') AS card_id,
        hm.kind,
        COALESCE(hm.payload->>'card_kind', CASE WHEN hm.kind = 'license' THEN 'license' END) AS card_kind,
        COALESCE(hm.recorded_at, hm.created_at) AS punched_at,
        hm.created_at,
        hm.is_dev,
        hm.payload->>'measurement_id' AS payload_measurement_id
    FROM alc_api.hub_measurements hm
    LEFT JOIN alc_api.timecard_cards tc
           ON tc.tenant_id = hm.tenant_id
          AND tc.card_id = lower(replace(btrim(hm.payload->>'card_id'), ':', ''))
    LEFT JOIN alc_api.employees e_nfc
           ON e_nfc.tenant_id = hm.tenant_id
          AND e_nfc.nfc_id = COALESCE(hm.payload->>'card_id', hm.payload->>'nfc_id')
    WHERE hm.tenant_id = $1 AND hm.kind IN ('timecard', 'license')
      AND NOT EXISTS (
          SELECT 1
          FROM alc_api.hub_measurements pair
          WHERE hm.kind = 'license'
            AND pair.tenant_id = hm.tenant_id
            AND pair.is_dev = hm.is_dev
            AND pair.device_id = hm.device_id
            AND pair.kind = 'timecard'
            AND pair.payload->>'card_id' = hm.payload->>'nfc_id'
            AND COALESCE(pair.recorded_at, pair.created_at)
                BETWEEN COALESCE(hm.recorded_at, hm.created_at) - interval '30 seconds'
                    AND COALESCE(hm.recorded_at, hm.created_at)
      )
)
"#;

    /// 打刻に結ばれた測定 (tenant・is_dev・社員・id が全部そろったときだけ。id は形を見てから uuid に)
    const PUNCH_MEASUREMENT_JOIN: &str = r#"
LEFT JOIN alc_api.measurements m
       ON m.tenant_id = p.tenant_id
      AND m.is_dev = p.is_dev
      AND m.employee_id = p.employee_id
      AND m.id = CASE WHEN p.payload_measurement_id
                           ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                      THEN p.payload_measurement_id::uuid END
"#;

    const PUNCH_MEASUREMENT_COLUMNS: &str = r#"
m.id AS measurement_id, m.alcohol_level, m.result,
CASE WHEN m.id IS NOT NULL THEN EXISTS (
    SELECT 1 FROM alc_api.tenko_sessions ts
    WHERE ts.tenant_id = m.tenant_id
      AND ts.is_dev = m.is_dev
      AND ts.measurement_id = m.id
) END AS tenko_recorded
"#;

    /// 一覧だけに足す点呼の区分 (CSV は `tenko_recorded` までで、列を変えない)。
    /// 1 つの測定に点呼が複数あるときは新しい方
    const PUNCH_TENKO_METHOD_COLUMNS: &str = r#"
, ts_m.tenko_method, ts_m.manager_judgment_method
"#;

    const PUNCH_TENKO_METHOD_JOIN: &str = r#"
LEFT JOIN LATERAL (
    SELECT ts.tenko_method, ts.manager_judgment_method
    FROM alc_api.tenko_sessions ts
    WHERE ts.tenant_id = m.tenant_id
      AND ts.is_dev = m.is_dev
      AND ts.measurement_id = m.id
    ORDER BY ts.created_at DESC
    LIMIT 1
) ts_m ON TRUE
"#;

    /// 絞り込み ($2 = 社員、$3 = 以降、$4 = 以前。どれも NULL なら絞らない)
    const PUNCH_WHERE: &str = " WHERE p.tenant_id = $1 \
         AND ($2::uuid IS NULL OR p.employee_id = $2) \
         AND ($3::timestamptz IS NULL OR p.punched_at >= $3) \
         AND ($4::timestamptz IS NULL OR p.punched_at <= $4)";

    pub const COUNT_PUNCHES: &str =
        const_format::concatcp!(PUNCHES_CTE, " SELECT COUNT(*) FROM p", PUNCH_WHERE);

    /// device_id は常に NULL (rust と同じ)。$5 = limit、$6 = offset
    pub const LIST_PUNCHES: &str = const_format::concatcp!(
        PUNCHES_CTE,
        " SELECT p.id, p.tenant_id, p.employee_id, NULL::uuid AS device_id, \
           p.hub_device_id AS device_name, p.card_id, p.kind, p.card_kind, \
           e.name AS employee_name, p.punched_at, p.created_at, ",
        PUNCH_MEASUREMENT_COLUMNS,
        PUNCH_TENKO_METHOD_COLUMNS,
        " FROM p LEFT JOIN alc_api.employees e ON e.id = p.employee_id ",
        PUNCH_MEASUREMENT_JOIN,
        PUNCH_TENKO_METHOD_JOIN,
        PUNCH_WHERE,
        " ORDER BY p.punched_at DESC LIMIT $5 OFFSET $6"
    );

    /// employees は LEFT JOIN (未登録カードのタップも空欄で出す)
    pub const CSV_PUNCHES: &str = const_format::concatcp!(
        PUNCHES_CTE,
        " SELECT p.id, p.punched_at, e.name AS employee_name, e.code AS employee_code, \
           p.hub_device_id AS device_name, p.kind, p.card_kind, ",
        PUNCH_MEASUREMENT_COLUMNS,
        " FROM p LEFT JOIN alc_api.employees e ON e.id = p.employee_id ",
        PUNCH_MEASUREMENT_JOIN,
        PUNCH_WHERE,
        " ORDER BY p.punched_at DESC"
    );

    /// dry_run の戻り先 (transaction の中。tenant_tx は最後に commit するので、書いた分をここへ戻してから終える)
    pub const SAVEPOINT: &str = "SAVEPOINT people_dry_run";
    pub const ROLLBACK_TO_SAVEPOINT: &str = "ROLLBACK TO SAVEPOINT people_dry_run";
}

fn card(r: &Row) -> TimecardCard {
    TimecardCard {
        id: r.get(0),
        tenant_id: r.get(1),
        employee_id: r.get(2),
        card_id: r.get(3),
        label: r.get(4),
        source: r.get(5),
        pending_employee_code: r.get(6),
        created_at: r.get(7),
    }
}

fn punch(r: &Row) -> Punch {
    Punch {
        id: r.get(0),
        tenant_id: r.get(1),
        employee_id: r.get(2),
        device_id: r.get(3),
        device_name: r.get(4),
        card_id: r.get(5),
        kind: r.get(6),
        card_kind: r.get(7),
        employee_name: r.get(8),
        punched_at: r.get(9),
        created_at: r.get(10),
        measurement_id: r.get(11),
        alcohol_level: r.get(12),
        result: r.get(13),
        tenko_recorded: r.get(14),
        tenko_method: r.get(15),
        manager_judgment_method: r.get(16),
    }
}

fn csv_row(r: &Row) -> CsvRow {
    CsvRow {
        id: r.get(0),
        punched_at: r.get(1),
        employee_name: r.get(2),
        employee_code: r.get(3),
        device_name: r.get(4),
        kind: r.get(5),
        card_kind: r.get(6),
        measurement_id: r.get(7),
        alcohol_level: r.get(8),
        result: r.get(9),
        tenko_recorded: r.get(10),
    }
}

/// 作成の失敗が一意の制約 (同じ card_id) か
pub fn is_card_conflict(e: &Error) -> bool {
    e.as_db_error()
        .and_then(|db| db.constraint())
        .is_some_and(|c| c == CARD_UNIQUE_CONSTRAINT)
}

// --- DB (どれも 1 つの tenant_tx) ---

pub async fn create_card(
    pg: &mut PgClient,
    tenant_id: Uuid,
    employee_id: Uuid,
    card_id: String,
    label: Option<String>,
) -> Result<TimecardCard, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT_CARD,
                    &[
                        (&tenant_id, Type::UUID),
                        (&employee_id, Type::UUID),
                        (&card_id, Type::TEXT),
                        (&label, Type::TEXT),
                    ],
                )
                .await?;
            Ok(card(&row))
        })
    })
    .await
}

pub async fn list_cards(
    pg: &mut PgClient,
    tenant_id: Uuid,
    employee_id: Option<Uuid>,
) -> Result<Vec<TimecardCard>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_CARDS,
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(card).collect())
        })
    })
    .await
}

pub async fn get_card(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<TimecardCard>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_CARD,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(card))
        })
    })
    .await
}

pub async fn delete_card(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(
                    sql::DELETE_CARD,
                    &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                )
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

/// 一括の 1 件を書く。受け入れたら true、別の社員に付いていて見送ったら false
async fn upsert_one(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    item: &PreparedCard,
    reassign: bool,
    summary: &mut UpsertSummary,
) -> Result<(), Error> {
    let employee_id: Option<Uuid> = tx
        .query_typed_opt(
            sql::EMPLOYEE_BY_CODE,
            &[(&tenant_id, Type::UUID), (&item.code, Type::TEXT)],
        )
        .await?
        .map(|r| r.get(0));
    // 社員が引けなくても落とさない (後で社員マスタ同期が結ぶよう社員番号を残す)
    let pending_code: Option<&str> = employee_id.is_none().then_some(item.code.as_str());
    let written = tx
        .query_typed_opt(
            sql::UPSERT_CARD,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&item.card_id, Type::TEXT),
                (&item.label, Type::TEXT),
                (&CARD_SOURCE_LEDGER_SYNC, Type::TEXT),
                (&reassign, Type::BOOL),
                (&pending_code, Type::TEXT),
            ],
        )
        .await?
        .map(|r| r.get::<_, bool>(0));
    let accepted = match written {
        Some(true) => {
            summary.created += 1;
            true
        }
        Some(false) => {
            summary.updated += 1;
            true
        }
        None => {
            // 書かなかった = 同じ card_id の行が既にある。持ち主が同じなら unchanged、別人なら見送り。
            // 保留行の持ち主は NULL なので Option で受ける
            let owner: Option<Option<Uuid>> = tx
                .query_typed_opt(
                    sql::CARD_OWNER,
                    &[(&tenant_id, Type::UUID), (&item.card_id, Type::TEXT)],
                )
                .await?
                .map(|r| r.get(0));
            if owner == Some(employee_id) {
                summary.unchanged += 1;
                true
            } else {
                summary.skipped.push(Skipped {
                    index: item.index,
                    code: item.code.clone(),
                    reason: "card_owner_conflict".to_owned(),
                });
                false
            }
        }
    };
    if accepted && employee_id.is_none() {
        summary.pending += 1;
    }
    Ok(())
}

/// 社員番号キーの一括取り込み (渡すのは [`prepare_bulk_cards`] を通した後の items だけ)。
/// 1 件の不備で落とさない (解決は SELECT、書き込みは ON CONFLICT)。`dry_run` は判定を最後まで同じコードで通し、
/// SAVEPOINT へ戻して書かない
pub async fn bulk_upsert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    items: Vec<PreparedCard>,
    on_conflict: ConflictPolicy,
    dry_run: bool,
) -> Result<UpsertSummary, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            if dry_run {
                tx.execute_typed(sql::SAVEPOINT, &[]).await?;
            }
            let reassign = on_conflict == ConflictPolicy::Reassign;
            let mut summary = UpsertSummary::default();
            for item in &items {
                upsert_one(tx, tenant_id, item, reassign, &mut summary).await?;
            }
            if dry_run {
                tx.execute_typed(sql::ROLLBACK_TO_SAVEPOINT, &[]).await?;
            }
            Ok(summary)
        })
    })
    .await
}

/// 同期で入れたカード 1 枚を外す (`card_id` は正規化・形の検査を通した後の値)
pub async fn delete_by_card(
    pg: &mut PgClient,
    tenant_id: Uuid,
    card_id: String,
    dry_run: bool,
) -> Result<DeleteResult, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            if dry_run {
                tx.execute_typed(sql::SAVEPOINT, &[]).await?;
            }
            let row = tx
                .query_typed_one(
                    sql::DELETE_BY_CARD,
                    &[
                        (&tenant_id, Type::UUID),
                        (&card_id, Type::TEXT),
                        (&CARD_SOURCE_LEDGER_SYNC, Type::TEXT),
                    ],
                )
                .await?;
            if dry_run {
                tx.execute_typed(sql::ROLLBACK_TO_SAVEPOINT, &[]).await?;
            }
            let (deleted, present): (bool, bool) = (row.get(0), row.get(1));
            Ok(match (deleted, present) {
                (true, _) => DeleteResult {
                    deleted: 1,
                    reason: "deleted".to_owned(),
                    code: row.get(2),
                },
                (false, true) => DeleteResult {
                    deleted: 0,
                    reason: "out_of_scope".to_owned(),
                    code: None,
                },
                (false, false) => DeleteResult {
                    deleted: 0,
                    reason: "not_found".to_owned(),
                    code: None,
                },
            })
        })
    })
    .await
}

/// カードから在籍の社員 (rust の `resolve_employee_by_card` + `employees.get`)。
/// 台帳 → 持ち主が居なければ (保留行) 免許証番号へ。照合へは生値を渡し、正規化はここの 1 回だけ。
/// 未登録・保留で免許証にも無い・退職はどれも None
pub async fn lookup_employee(
    pg: &mut PgClient,
    tenant_id: Uuid,
    raw_card_id: String,
) -> Result<Option<Employee>, Error> {
    let card_id = normalize_card_id(&raw_card_id);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let args: [(&(dyn ToSql + Sync), Type); 2] =
                [(&tenant_id, Type::UUID), (&card_id, Type::TEXT)];
            let from_card: Option<Uuid> = tx
                .query_typed_opt(sql::CARD_EMPLOYEE, &args)
                .await?
                .and_then(|r| r.get(0));
            let employee_id = match from_card {
                Some(id) => Some(id),
                None => tx
                    .query_typed_opt(sql::EMPLOYEE_BY_NFC, &args)
                    .await?
                    .map(|r| r.get::<_, Uuid>(0)),
            };
            let Some(employee_id) = employee_id else {
                return Ok(None);
            };
            let row = tx
                .query_typed_opt(
                    crate::employees::sql::GET,
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok(row.as_ref().map(crate::employees::employee))
        })
    })
    .await
}

/// 打刻の一覧 (件数と頁)。dev端末の要求では `app.device_dev` を立てる (RLS が dev の行を出す)
pub async fn list_punches(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: PunchFilter,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<Punch>), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let total: i64 = tx
                .query_typed_one(
                    sql::COUNT_PUNCHES,
                    &[
                        (&tenant_id, Type::UUID),
                        (&filter.employee_id, Type::UUID),
                        (&filter.date_from, Type::TIMESTAMPTZ),
                        (&filter.date_to, Type::TIMESTAMPTZ),
                    ],
                )
                .await?
                .get(0);
            let rows = tx
                .query_typed(
                    sql::LIST_PUNCHES,
                    &[
                        (&tenant_id, Type::UUID),
                        (&filter.employee_id, Type::UUID),
                        (&filter.date_from, Type::TIMESTAMPTZ),
                        (&filter.date_to, Type::TIMESTAMPTZ),
                        (&limit, Type::INT8),
                        (&offset, Type::INT8),
                    ],
                )
                .await?;
            Ok((total, rows.iter().map(punch).collect()))
        })
    })
    .await
}

/// 打刻の CSV の行 (頁なし)
pub async fn csv_punches(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: PunchFilter,
) -> Result<Vec<CsvRow>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(
                    sql::CSV_PUNCHES,
                    &[
                        (&tenant_id, Type::UUID),
                        (&filter.employee_id, Type::UUID),
                        (&filter.date_from, Type::TIMESTAMPTZ),
                        (&filter.date_to, Type::TIMESTAMPTZ),
                    ],
                )
                .await?;
            Ok(rows.iter().map(csv_row).collect())
        })
    })
    .await
}

// --- 口 ---

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/timecard/cards",
            post(create_card_handler).get(list_cards_handler),
        )
        .route(
            "/timecard/cards/{id}",
            get(get_card_handler).delete(delete_card_handler),
        )
        .route("/timecard/cards/bulk-by-code", put(bulk_upsert_handler))
        // POST だけを登録する (呼び出し元の allowlist は method を見ないので、他 method は axum の 405 で閉じる)
        .route(
            "/timecard/cards/delete-by-card",
            post(delete_by_card_handler),
        )
        .route("/timecard/cards/lookup", post(lookup_handler))
        .route("/timecard/punches", get(list_punches_handler))
        .route("/timecard/punches/csv", get(csv_handler))
}

async fn create_card_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateCard>,
) -> Result<(StatusCode, Json<TimecardCard>), StatusCode> {
    let mut pg = state.pg.lock().await;
    // 登録も照合と同じ正規化形で入れる (読み側だけ正規化すると `ABC` と `abc` が同時に一致し得る)
    let created = create_card(
        &mut pg,
        tenant_id,
        body.employee_id,
        normalize_card_id(&body.card_id),
        body.label,
    )
    .await;
    match created {
        Ok(c) => Ok((StatusCode::CREATED, Json(c))),
        Err(e) if is_card_conflict(&e) => Err(StatusCode::CONFLICT),
        Err(e) => Err(state.db_fail("card create", &e)),
    }
}

#[derive(Debug, Deserialize)]
struct CardFilter {
    employee_id: Option<Uuid>,
}

async fn list_cards_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<CardFilter>,
) -> Result<Json<Vec<TimecardCard>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_cards(&mut pg, tenant_id, filter.employee_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("card list", &e))
}

async fn get_card_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<TimecardCard>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_card(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("card get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_card_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    if delete_card(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("card delete", &e))?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

/// skip があっても 200。items が 0 件・上限超えは 400 (rust と同じく本文は文字列)
async fn bulk_upsert_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<BulkUpsert>,
) -> Result<Json<UpsertSummary>, (StatusCode, String)> {
    if body.items.is_empty() || body.items.len() > MAX_BULK_UPSERT_ITEMS {
        return Err((StatusCode::BAD_REQUEST, "items が不正です".to_owned()));
    }
    let (prepared, invalid) = prepare_bulk_cards(&body.items);
    let mut pg = state.pg.lock().await;
    let mut summary = bulk_upsert(&mut pg, tenant_id, prepared, body.on_conflict, body.dry_run)
        .await
        .map_err(|e| (state.db_fail("card bulk", &e), "internal error".to_owned()))?;
    // 送り手が items を走査しながら突き合わせられるよう index 順に戻す
    summary.skipped.extend(invalid);
    summary.skipped.sort_by_key(|s| s.index);
    Ok(Json(summary))
}

/// 範囲外・不在・形不正もどれも 200 + reason
async fn delete_by_card_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<DeleteByCard>,
) -> Result<Json<DeleteResult>, (StatusCode, String)> {
    let card_id = normalize_card_id(&body.card_id);
    if !is_valid_bulk_card_id(&card_id) {
        return Ok(Json(DeleteResult {
            deleted: 0,
            reason: "invalid_card_id".to_owned(),
            code: None,
        }));
    }
    let mut pg = state.pg.lock().await;
    delete_by_card(&mut pg, tenant_id, card_id, body.dry_run)
        .await
        .map(Json)
        .map_err(|e| {
            (
                state.db_fail("card delete-by-card", &e),
                "internal error".to_owned(),
            )
        })
}

#[derive(Debug, Deserialize)]
struct CardLookup {
    /// 欠落も空と同じ 400 にするため default で受ける (必須にすると axum が 422 を返す)
    #[serde(default)]
    card_id: String,
}

/// ★ この口に書き込みを足さない (打刻しない・台帳を更新しないことが存在理由。auth-worker が席の鍵に開ける根拠)
async fn lookup_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CardLookup>,
) -> Result<Json<Employee>, StatusCode> {
    // 空かどうかは照合と同じ正規化形で見る (空白だけ・区切りだけも空)
    if normalize_card_id(&body.card_id).is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    lookup_employee(&mut pg, tenant_id, body.card_id)
        .await
        .map_err(|e| state.db_fail("card lookup", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn list_punches_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<PunchFilter>,
) -> Result<Json<PunchesResponse>, StatusCode> {
    let (page, per_page, offset) = punch_paging(filter.page, filter.per_page);
    let mut pg = state.pg.lock().await;
    let (total, punches) = list_punches(&mut pg, tenant_id, dev.get(), filter, per_page, offset)
        .await
        .map_err(|e| state.db_fail("punch list", &e))?;
    Ok(Json(PunchesResponse {
        punches,
        total,
        page,
        per_page,
    }))
}

async fn csv_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<PunchFilter>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut pg = state.pg.lock().await;
    let rows = csv_punches(&mut pg, tenant_id, dev.get(), filter)
        .await
        .map_err(|e| state.db_fail("punch csv", &e))?;
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"time_punches.csv\"",
            ),
        ],
        render_csv(&rows),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(code: &str, card_id: &str) -> UpsertItem {
        UpsertItem {
            code: code.to_owned(),
            card_id: card_id.to_owned(),
            label: None,
        }
    }

    #[test]
    fn normalize_is_the_rust_rule() {
        // 端末は大文字の IDm、区切り・前後の空白を落とす。免許証番号 (数字 16 桁) は変わらない
        assert_eq!(normalize_card_id("0123456789ABCDEF"), "0123456789abcdef");
        assert_eq!(normalize_card_id("  AA:BB:CC:DD  "), "aabbccdd");
        assert_eq!(normalize_card_id("2023040120280331"), "2023040120280331");
        assert_eq!(normalize_card_id("   "), "");
    }

    #[test]
    fn bulk_card_id_shape() {
        assert!(is_valid_bulk_card_id("01ab23cd"));
        assert!(is_valid_bulk_card_id("04a1b2c3d4e5f6"));
        assert!(is_valid_bulk_card_id("01401d0b1d37b660"));
        assert!(!is_valid_bulk_card_id(""));
        assert!(!is_valid_bulk_card_id("01ab23cde"));
        assert!(!is_valid_bulk_card_id("01401D0B1D37B660"));
        assert!(!is_valid_bulk_card_id("01401g0b1d37b660"));
    }

    #[test]
    fn prepare_keeps_index_and_skips_invalid_and_duplicates() {
        let (prepared, skipped) = prepare_bulk_cards(&[
            item("E001", "  01401D0B:1D37:B660  "),
            item("E002", "未登録"),
            item("E003", "01401d0b1d37b660"),
            item("E004", "04A1B2C3D4E5F6"),
        ]);
        assert_eq!(
            prepared
                .iter()
                .map(|p| (p.index, p.card_id.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, "01401d0b1d37b660"), (3, "04a1b2c3d4e5f6")]
        );
        assert_eq!(
            skipped
                .iter()
                .map(|s| (s.index, s.reason.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "invalid_card_id"), (2, "duplicate_in_batch")]
        );
    }

    #[test]
    fn punch_paging_is_the_rust_rule() {
        assert_eq!(punch_paging(None, None), (1, 50, 0));
        assert_eq!(punch_paging(Some(0), Some(500)), (1, 200, 0));
        assert_eq!(punch_paging(Some(3), Some(10)), (3, 10, 20));
    }

    #[test]
    fn csv_labels() {
        assert_eq!(csv_kind_label("timecard"), "打刻");
        assert_eq!(csv_kind_label("license"), "点呼");
        assert_eq!(csv_kind_label("other"), "other");
        assert_eq!(csv_card_kind_label(Some("license")), "免許証");
        assert_eq!(csv_card_kind_label(Some("felica_idm")), "ICカード");
        assert_eq!(csv_card_kind_label(Some("nfca_uid")), "ICカード");
        assert_eq!(csv_card_kind_label(Some("x")), "x");
        assert_eq!(csv_card_kind_label(None), "");
        assert_eq!(csv_tenko_recorded_label(Some(true)), "済");
        assert_eq!(csv_tenko_recorded_label(Some(false)), "未");
        assert_eq!(csv_tenko_recorded_label(None), "");
    }

    #[test]
    fn csv_has_bom_header_and_jst() {
        let id = Uuid::nil();
        let bytes = render_csv(&[CsvRow {
            id,
            punched_at: "2026-01-15T00:30:00Z".parse().unwrap(),
            employee_name: None,
            employee_code: Some("E1".into()),
            device_name: None,
            kind: "timecard".into(),
            card_kind: Some("felica_idm".into()),
            measurement_id: None,
            alcohol_level: Some(0.05),
            result: None,
            tenko_recorded: None,
        }]);
        assert_eq!(&bytes[..3], &[0xEF, 0xBB, 0xBF]);
        let text = std::str::from_utf8(&bytes[3..]).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next().unwrap(), CSV_HEADER.join(","));
        assert_eq!(
            lines.next().unwrap(),
            format!("{id},打刻,ICカード,E1,,2026-01-15 09:30:00,,,0.05,,")
        );
    }
}
