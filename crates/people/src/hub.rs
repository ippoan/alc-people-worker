//! 端末の測定・打刻の取り込みと一覧 (W6a、Refs ippoan/rust-alc-api#680。rust の `alc-devices` の hub_measurements.rs・
//! repo/hub_measurements.rs と `alc-core` の models の写し)。
//!
//! - `POST /hub/measurements` (201) — 測定の取り込み (配列でも 1 件でも。1〜500 件)。cf-alc-recorder が auth-worker の
//!   内部用の proxy から呼ぶ。同じ (tenant, 端末, 連番, dev の軸) の再送は数えるだけで入れない (`duplicates`)
//! - `GET /hub/measurements` (`?device_id=&kind=&session_id=&from=&to=&limit=&offset=`) — 一覧 (受信の新しい順。
//!   総数は返さず、1 件多く引いて `has_more` だけ返す)。alc-app の画面と auth-worker の MCP (試験記録) が呼ぶ
//!
//! 打刻 (`kind = "timecard"`) は、取り込みの時点でカードを社員に解決し、payload の `employee_id` に凍結する
//! (打刻の一覧 [`crate::timecard`] はこの値を読む。後からカードを付け替えても過去の打刻は動かない)。
//! 照合はカードの照会と同じ規則 ([`crate::timecard::normalize_card_id`] → 台帳 → 免許証番号)。端末が名乗った
//! `employee_id` は kind を問わず捨てる。解決できなくても (未登録・DB の失敗) 行は入れる。
//!
//! dev端末の要求 (`X-Device-Dev: 1`) では `app.device_dev` を立てる (列 `is_dev` の既定値と RLS が dev の行と本番の行を分ける)。
//! 試験記録 (`kind = "test"`) は dev の軸のときだけ受ける。
//!
//! rust との違い:
//! - 一覧の絞り込みは文を組み立てず、`($n IS NULL OR ...)` の 1 文にした。SQL に tenant_id を明示する (rust と同じ)
//! - ログに card_id・端末の ID・連番を出さない (固定の語だけ)
//! - staging の DB でテナントを作り直す救済 (rust の `ensure_tenant_for_staging`) は持たない
//!   (worker の振り分けは本番だけ。staging の auth-worker は Cloud Run の staging に回す)

use alc_core_wasm::{DeviceDevSlot, TenantId};
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::timecard::normalize_card_id;
use crate::{set_device_dev, AppState};

/// 打刻の kind
pub const KIND_TIMECARD: &str = "timecard";

/// 開発用の試験記録の kind。dev の軸のときだけ受ける
pub const KIND_TEST: &str = "test";

/// 受ける kind (rust の `HUB_MEASUREMENT_KINDS`)。**ここに無い kind を端末が送ると 400 で ack されず、端末は再送し続ける。
/// 新しい kind は端末より先にここへ足す**
pub const HUB_MEASUREMENT_KINDS: &[&str] = &[
    "temperature",
    "blood_pressure",
    "alcohol",
    "fc1200_raw",
    "license",
    KIND_TIMECARD,
    KIND_TEST,
];

/// 1 回で受ける件数の上限 (再送の山を防ぐ)
pub const MAX_BATCH_ITEMS: usize = 500;
/// 端末の ID の長さの上限
pub const MAX_DEVICE_ID_LEN: usize = 128;
/// session_id の長さの上限
pub const MAX_SESSION_ID_LEN: usize = 64;
/// 一覧の既定の件数と上限
pub const DEFAULT_LIST_LIMIT: i64 = 50;
pub const MAX_LIST_LIMIT: i64 = 200;

/// 打刻の payload のカードの生値のキー
const FIELD_CARD_ID: &str = "card_id";
/// 取り込みの時点で凍結する「解決した社員」のキー (**サーバが書く。端末の申告は必ず捨てる**)
const FIELD_EMPLOYEE_ID: &str = "employee_id";

pub mod sql {
    /// 1 行入れる (同じ軸の再送は入れない)。is_dev は列の既定値 (`app.device_dev`) で入る。
    /// $1 tenant_id / $2 device_id / $3 kind / $4 payload / $5 seq / $6 recorded_at / $7 session_id / $8 via_device_id
    pub const INSERT: &str = "INSERT INTO alc_api.hub_measurements \
         (tenant_id, device_id, kind, payload, seq, recorded_at, session_id, via_device_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id, device_id, seq, is_dev) DO NOTHING";

    /// 一覧 (受信の新しい順、1 件多く引く)。$1 tenant_id / $2 device_id / $3 kind / $4 session_id / $5 from / $6 to
    /// (どれも NULL なら絞らない) / $7 件数 / $8 飛ばす件数。
    /// 機体の鍵の ID (`alarm:<fingerprint>`) で絞ると、その鍵に結び付いた hub の ID の行も返す。
    /// `machine_id` は hub の ID の行を、結び付いた鍵の ID (最後に見えたもの) に畳む。dev の軸は跨がない
    pub const LIST: &str = "SELECT h.id, h.tenant_id, h.device_id, h.kind, h.payload, h.seq, h.session_id, \
                h.via_device_id, COALESCE(link.key_id, h.device_id) AS machine_id, h.recorded_at, h.created_at \
           FROM alc_api.hub_measurements h \
           LEFT JOIN LATERAL ( \
                SELECT l.device_id AS key_id FROM alc_api.hub_measurements l \
                 WHERE l.tenant_id = h.tenant_id AND l.is_dev = h.is_dev AND l.via_device_id = h.device_id \
                 ORDER BY l.created_at DESC LIMIT 1 \
           ) link ON true \
          WHERE h.tenant_id = $1 \
            AND ($2::text IS NULL OR h.device_id = $2 OR h.device_id IN ( \
                 SELECT l.via_device_id FROM alc_api.hub_measurements l \
                  WHERE l.tenant_id = h.tenant_id AND l.is_dev = h.is_dev \
                    AND l.device_id = $2 AND l.via_device_id IS NOT NULL)) \
            AND ($3::text IS NULL OR h.kind = $3) \
            AND ($4::text IS NULL OR h.session_id = $4) \
            AND ($5::timestamptz IS NULL OR h.created_at >= $5) \
            AND ($6::timestamptz IS NULL OR h.created_at <= $6) \
          ORDER BY h.created_at DESC, h.id DESC \
          LIMIT $7 OFFSET $8";
}

// --- 本文と行 (rust の alc_core::models の写し。JSON の形は同じ) ---

/// rust の `HubMeasurementCreate`
#[derive(Debug, Clone, Deserialize)]
pub struct HubMeasurementCreate {
    pub device_id: String,
    pub kind: String,
    pub seq: i64,
    /// 端末の時計 (unix ms)。範囲外は NULL にする
    pub recorded_at_ms: Option<i64>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub via_device_id: Option<String>,
    pub payload: Value,
}

/// 配列 (まとめて) と 1 件の両方を受ける
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum IngestBody {
    Batch(Vec<HubMeasurementCreate>),
    Single(HubMeasurementCreate),
}

impl IngestBody {
    pub fn into_items(self) -> Vec<HubMeasurementCreate> {
        match self {
            IngestBody::Batch(items) => items,
            IngestBody::Single(item) => vec![item],
        }
    }
}

/// rust の `HubMeasurementsIngestResponse`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IngestResponse {
    pub inserted: i64,
    pub duplicates: i64,
}

/// rust の `HubMeasurement`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HubMeasurement {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    pub kind: String,
    pub payload: Value,
    pub seq: i64,
    pub session_id: Option<String>,
    pub via_device_id: Option<String>,
    pub machine_id: String,
    pub recorded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for HubMeasurement {}

/// rust の `HubMeasurementFilter`
#[derive(Debug, Clone, Default, Deserialize)]
pub struct HubMeasurementFilter {
    pub device_id: Option<String>,
    pub kind: Option<String>,
    pub session_id: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// rust の `HubMeasurementsListResponse`
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub items: Vec<HubMeasurement>,
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
}

// --- 検査 (rust と同じ) ---

/// session_id は端末が付ける値なので、長さと字種を絞る (無いのは正常)
pub fn valid_session_id(session_id: Option<&String>) -> bool {
    session_id.is_none_or(|v| {
        !v.is_empty()
            && v.len() <= MAX_SESSION_ID_LEN
            && v.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    })
}

/// 端末の ID の形 (空白だけでない・長さの上限)。`device_id` と `via_device_id` で使う
pub fn valid_device_id(device_id: &str) -> bool {
    !device_id.trim().is_empty() && device_id.len() <= MAX_DEVICE_ID_LEN
}

/// 1 件の検査 (理由は返さず 400 にまとめる。呼び手は内部の Worker だけ)
pub fn validate(item: &HubMeasurementCreate) -> bool {
    valid_device_id(&item.device_id)
        && item.via_device_id.as_deref().is_none_or(valid_device_id)
        && HUB_MEASUREMENT_KINDS.contains(&item.kind.as_str())
        && item.seq >= 0
        && valid_session_id(item.session_id.as_ref())
}

/// 試験記録は dev の軸のときだけ受ける (本番の軸に届くのは recorder の取りこぼし。本番の行にしない)
pub fn kind_allowed_in_axis(item: &HubMeasurementCreate, dev: bool) -> bool {
    dev || item.kind != KIND_TEST
}

/// 一覧の件数と飛ばす件数を整える (0・負・上限超えは安全側へ)
pub fn clamp_paging(filter: &HubMeasurementFilter) -> (i64, i64) {
    let limit = filter
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let offset = filter.offset.unwrap_or(0).max(0);
    (limit, offset)
}

/// 打刻ならカードの生値 (文字列のときだけ)。打刻でない・カードが無いなら None
fn timecard_card_id(item: &HubMeasurementCreate) -> Option<String> {
    (item.kind == KIND_TIMECARD)
        .then(|| item.payload.get(FIELD_CARD_ID)?.as_str().map(str::to_owned))
        .flatten()
}

// --- DB ---

/// カードを社員に解決する (カードの照会と同じ規則。退職は見ない = rust の `resolve_employee_by_card` と同じ)。
/// 引数の順に結果を返す
pub async fn resolve_cards(
    pg: &mut PgClient,
    tenant_id: Uuid,
    card_ids: Vec<String>,
) -> Result<Vec<Option<Uuid>>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let mut out = Vec::with_capacity(card_ids.len());
            for raw in &card_ids {
                let card_id = normalize_card_id(raw);
                let args: [(&(dyn ToSql + Sync), Type); 2] =
                    [(&tenant_id, Type::UUID), (&card_id, Type::TEXT)];
                let from_card: Option<Uuid> = tx
                    .query_typed_opt(crate::timecard::sql::CARD_EMPLOYEE, &args)
                    .await?
                    .and_then(|r| r.get(0));
                let employee_id = match from_card {
                    Some(id) => Some(id),
                    None => tx
                        .query_typed_opt(crate::timecard::sql::EMPLOYEE_BY_NFC, &args)
                        .await?
                        .map(|r| r.get::<_, Uuid>(0)),
                };
                out.push(employee_id);
            }
            Ok(out)
        })
    })
    .await
}

/// まとめて入れる (1 つの transaction)。返すのは新しく入った件数
pub async fn insert_batch(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    items: Vec<HubMeasurementCreate>,
) -> Result<i64, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let mut inserted: i64 = 0;
            for item in &items {
                let recorded_at = item
                    .recorded_at_ms
                    .and_then(DateTime::from_timestamp_millis);
                let args: [(&(dyn ToSql + Sync), Type); 8] = [
                    (&tenant_id, Type::UUID),
                    (&item.device_id, Type::TEXT),
                    (&item.kind, Type::TEXT),
                    (&item.payload, Type::JSONB),
                    (&item.seq, Type::INT8),
                    (&recorded_at, Type::TIMESTAMPTZ),
                    (&item.session_id, Type::TEXT),
                    (&item.via_device_id, Type::TEXT),
                ];
                // ON CONFLICT DO NOTHING なので 0 か 1 (再送は 0)
                inserted += tx.execute_typed(sql::INSERT, &args).await? as i64;
            }
            Ok(inserted)
        })
    })
    .await
}

fn measurement(r: &Row) -> HubMeasurement {
    HubMeasurement {
        id: r.get(0),
        tenant_id: r.get(1),
        device_id: r.get(2),
        kind: r.get(3),
        payload: r.get(4),
        seq: r.get(5),
        session_id: r.get(6),
        via_device_id: r.get(7),
        machine_id: r.get(8),
        recorded_at: r.get(9),
        created_at: r.get(10),
    }
}

/// 一覧 ([`sql::LIST`]。`limit + 1` 件まで返す)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    dev: bool,
    filter: HubMeasurementFilter,
    limit: i64,
    offset: i64,
) -> Result<Vec<HubMeasurement>, Error> {
    let fetch = limit + 1;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            set_device_dev(tx, dev).await?;
            let rows = tx
                .query_typed(
                    sql::LIST,
                    &[
                        (&tenant_id, Type::UUID),
                        (&filter.device_id, Type::TEXT),
                        (&filter.kind, Type::TEXT),
                        (&filter.session_id, Type::TEXT),
                        (&filter.from, Type::TIMESTAMPTZ),
                        (&filter.to, Type::TIMESTAMPTZ),
                        (&fetch, Type::INT8),
                        (&offset, Type::INT8),
                    ],
                )
                .await?;
            Ok(rows.iter().map(measurement).collect())
        })
    })
    .await
}

// --- 口 ---

pub fn router() -> Router<AppState> {
    Router::new().route("/hub/measurements", get(list_handler).post(ingest_handler))
}

/// 端末の申告の `employee_id` を全部捨て、打刻はカードから社員を解決して凍結する (**入れる前に**。再送は入らないので、
/// 最初に入った payload が凍結の実体)。解決できなくても行は入れる
async fn freeze_employee_ids(
    state: &AppState,
    tenant_id: Uuid,
    items: &mut [HubMeasurementCreate],
) {
    for item in items.iter_mut() {
        if let Some(obj) = item.payload.as_object_mut() {
            obj.remove(FIELD_EMPLOYEE_ID);
        }
    }
    let mut targets = Vec::new();
    for (i, item) in items.iter().enumerate() {
        if item.kind != KIND_TIMECARD {
            continue;
        }
        match timecard_card_id(item) {
            Some(card_id) => targets.push((i, card_id)),
            None => (state.log)("people: hub timecard has no card_id"),
        }
    }
    if targets.is_empty() {
        return;
    }
    let card_ids = targets.iter().map(|(_, c)| c.clone()).collect();
    let resolved = {
        let mut pg = state.pg.lock().await;
        resolve_cards(&mut pg, tenant_id, card_ids).await
    };
    let resolved = match resolved {
        Ok(r) => r,
        Err(e) => {
            // 失敗しても行は入れる (employee_id の無い行は後から埋め直せる)
            let _ = state.db_fail("hub card resolve", &e);
            return;
        }
    };
    for ((i, _), employee_id) in targets.into_iter().zip(resolved) {
        let Some(employee_id) = employee_id else {
            // 未登録のカードは運用の事象 (登録漏れ)。行は入れる
            (state.log)("people: hub timecard card not registered");
            continue;
        };
        if let Some(obj) = items[i].payload.as_object_mut() {
            obj.insert(
                FIELD_EMPLOYEE_ID.to_owned(),
                Value::String(employee_id.to_string()),
            );
        }
    }
}

async fn ingest_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Json(body): Json<IngestBody>,
) -> Result<(StatusCode, Json<IngestResponse>), StatusCode> {
    let mut items = body.into_items();
    let dev = dev.get();
    if items.is_empty()
        || items.len() > MAX_BATCH_ITEMS
        || !items.iter().all(validate)
        || !items.iter().all(|item| kind_allowed_in_axis(item, dev))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    freeze_employee_ids(&state, tenant_id, &mut items).await;
    let total = items.len() as i64;
    let mut pg = state.pg.lock().await;
    let inserted = insert_batch(&mut pg, tenant_id, dev, items)
        .await
        .map_err(|e| state.db_fail("hub ingest", &e))?;
    Ok((
        StatusCode::CREATED,
        Json(IngestResponse {
            inserted,
            duplicates: total - inserted,
        }),
    ))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Extension(dev): Extension<DeviceDevSlot>,
    Query(filter): Query<HubMeasurementFilter>,
) -> Result<Json<ListResponse>, StatusCode> {
    let kind_ok = filter
        .kind
        .as_deref()
        .is_none_or(|k| HUB_MEASUREMENT_KINDS.contains(&k));
    let range_ok = match (filter.from, filter.to) {
        (Some(from), Some(to)) => from <= to,
        _ => true,
    };
    if !kind_ok || !valid_session_id(filter.session_id.as_ref()) || !range_ok {
        return Err(StatusCode::BAD_REQUEST);
    }
    let (limit, offset) = clamp_paging(&filter);
    let mut pg = state.pg.lock().await;
    let mut items = list(&mut pg, tenant_id, dev.get(), filter, limit, offset)
        .await
        .map_err(|e| state.db_fail("hub list", &e))?;
    let has_more = items.len() as i64 > limit;
    items.truncate(limit as usize);
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
        has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(kind: &str, seq: i64, device_id: &str) -> HubMeasurementCreate {
        HubMeasurementCreate {
            device_id: device_id.to_owned(),
            kind: kind.to_owned(),
            seq,
            recorded_at_ms: None,
            session_id: None,
            via_device_id: None,
            payload: json!({}),
        }
    }

    #[test]
    fn validate_matches_rust() {
        for kind in HUB_MEASUREMENT_KINDS {
            assert!(validate(&item(kind, 0, "dev-1")), "kind={kind}");
        }
        assert!(!validate(&item("unknown", 0, "dev-1")));
        assert!(!validate(&item("alcohol", 0, "  ")));
        assert!(!validate(&item("alcohol", -1, "dev-1")));
        assert!(!validate(&item("alcohol", 0, &"x".repeat(129))));
        let session = |s: Option<&str>| HubMeasurementCreate {
            session_id: s.map(str::to_owned),
            ..item("alcohol", 1, "dev-1")
        };
        assert!(validate(&session(None)));
        assert!(validate(&session(Some("boot-1234_7"))));
        for bad in ["", "s 42", "s/42"] {
            assert!(!validate(&session(Some(bad))), "{bad:?}");
        }
        assert!(!validate(&session(Some(&"x".repeat(65)))));
        let via = |v: Option<&str>| HubMeasurementCreate {
            via_device_id: v.map(str::to_owned),
            ..item("alcohol", 1, "alarm:0123456789abcdef")
        };
        assert!(validate(&via(None)));
        assert!(validate(&via(Some("fxPqzwAbCd"))));
        assert!(!validate(&via(Some("  "))));
        assert!(!validate(&via(Some(&"x".repeat(129)))));
    }

    #[test]
    fn test_kind_only_in_dev_axis() {
        assert!(kind_allowed_in_axis(&item(KIND_TEST, 0, "d"), true));
        assert!(!kind_allowed_in_axis(&item(KIND_TEST, 0, "d"), false));
        for kind in HUB_MEASUREMENT_KINDS.iter().filter(|k| **k != KIND_TEST) {
            assert!(kind_allowed_in_axis(&item(kind, 0, "d"), false), "{kind}");
        }
    }

    #[test]
    fn clamp_paging_bounds() {
        let f = |limit, offset| HubMeasurementFilter {
            limit,
            offset,
            ..Default::default()
        };
        assert_eq!(clamp_paging(&f(None, None)), (DEFAULT_LIST_LIMIT, 0));
        assert_eq!(clamp_paging(&f(Some(10), Some(20))), (10, 20));
        assert_eq!(clamp_paging(&f(Some(0), Some(-5))), (1, 0));
        assert_eq!(
            clamp_paging(&f(Some(MAX_LIST_LIMIT + 1), None)),
            (MAX_LIST_LIMIT, 0)
        );
    }

    #[test]
    fn body_accepts_single_and_batch_and_reads_card_id() {
        let single: IngestBody =
            serde_json::from_str(r#"{"device_id":"d","kind":"alcohol","seq":1,"payload":{}}"#)
                .unwrap();
        assert_eq!(single.into_items().len(), 1);
        let batch: IngestBody = serde_json::from_str(
            r#"[{"device_id":"d","kind":"alcohol","seq":1,"payload":{}},
                {"device_id":"d","kind":"temperature","seq":2,"recorded_at_ms":1752300000000,"payload":{"value":36.5}}]"#,
        )
        .unwrap();
        assert_eq!(batch.into_items().len(), 2);
        let card = |kind: &str, payload| HubMeasurementCreate {
            payload,
            ..item(kind, 0, "d")
        };
        assert_eq!(
            timecard_card_id(&card(KIND_TIMECARD, json!({ "card_id": "AA" }))),
            Some("AA".to_owned())
        );
        assert_eq!(
            timecard_card_id(&card(KIND_TIMECARD, json!({ "card_id": 1 }))),
            None
        );
        assert_eq!(timecard_card_id(&card(KIND_TIMECARD, json!({}))), None);
        assert_eq!(
            timecard_card_id(&card("alcohol", json!({ "card_id": "AA" }))),
            None
        );
    }
}
