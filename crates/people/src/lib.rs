//! 社員・タイムカードの口 (Refs ippoan/rust-alc-api#680)。rust-alc-api の `crates/alc-misc` の timecard.rs と
//! repo/timecard.rs・`alc-core` の repository/timecard.rs の写しで、path・method・JSON の形・status は rust と同じ
//! (alc-app・tc-ohishi のカード同期・運行管理者席がそのまま使う)。
//!
//! - [`timecard`] — カード台帳 (登録・一覧・取得・削除・社員番号キーの一括取り込み・同期の削除・カードから社員の照会) と
//!   打刻の一覧・CSV (打刻の一次表は `hub_measurements`。書き込みは alc-devices の `POST /api/hub/measurements` に残る)
//!
//! 全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。`alc_core_wasm::require_tenant_header`)。
//! rust と同じく role は見ない。dev端末の要求 (`X-Device-Dev: 1`) では打刻の一覧で `app.device_dev` を立てる
//! (`hub_measurements`・`measurements`・`tenko_sessions` の RLS が dev の行と本番の行を分ける)。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。WHERE に tenant_id も書く
//! (rust は RLS 任せだった)。ログ・エラー応答は固定の語だけ (エラーの文・識別子・card_id を載せない)。

pub mod timecard;

use std::sync::Arc;

use alc_core_wasm::{require_tenant_header, DeviceDevSlot};
use alc_worker_db::{kind, PgClient, TenantTx};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
}

impl AppState {
    /// DB の失敗を固定の語でログに出し、500 にする
    fn db_fail(&self, stage: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("people: {stage} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// dev端末の要求で、transaction の中だけ `app.device_dev` を `'1'` にする文 (固定。値を埋め込まない)。
/// rust の `alc_core::tenant::set_current_tenant` と同じ働き (rust は接続ごと、ここは transaction-local)。
/// 列 `is_dev` の RLS がこれを見て、dev の行と本番の行を分ける (alc-migrations 153。ippoan/alc-measurements-worker と同じ)
pub const SET_DEVICE_DEV: &str = "SELECT set_config('app.device_dev', '1', true)";

/// `dev` なら [`SET_DEVICE_DEV`] を流す (tenant_tx の最初の文。dev でなければ何もしない = 設定は空のまま)
pub async fn set_device_dev(tx: &TenantTx<'_>, dev: bool) -> Result<(), tokio_postgres::Error> {
    if dev {
        tx.execute_typed(SET_DEVICE_DEV, &[]).await?;
    }
    Ok(())
}

/// 要求ごとに空の `DeviceDevSlot` を置く (kit の `require_tenant_header` は、在る slot にだけ dev の印を書く。
/// 共有の 1 つを使い回さない)
pub async fn with_device_dev_slot(mut req: Request, next: Next) -> Response {
    req.extensions_mut().insert(DeviceDevSlot::default());
    next.run(req).await
}

/// 全部の口。rust と同じく `/api` の下 (auth-worker は `/api/...` のまま Service Binding に渡す)
pub fn router() -> Router<AppState> {
    // 後に足した route_layer が外側: slot を置く → tenant と dev の印を読む → 口
    let api = timecard::router()
        .route_layer(middleware::from_fn(require_tenant_header))
        .route_layer(middleware::from_fn(with_device_dev_slot));
    Router::new().nest("/api", api)
}
