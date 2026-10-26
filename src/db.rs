//! postgres への接続 (ippoan/alc-records-worker の `src/db.rs` の写し、元は ippoan/alc-dtako-worker。Refs ippoan/rust-alc-api#680)。
//! **DB に繋ぐのはこの 1 か所だけ**で、env の binding で経路を出し分ける
//! (上から順に見て、最初にあったものを使う)。1 とローカルは間に transaction mode のプーラー (PgBouncer) が入る。
//! 2 は Hyperdrive が間に入る (Refs ippoan/rust-alc-api#723)。3 は接続文字列の host:port へ繋ぐだけで、宛先の形
//! (プーラーか直接か) は接続文字列しだい (RLS はトランザクション単位で、SQL は名前なしの文だけにするので、どれでも動く作り):
//!
//! | 順 | env | 読むもの | 経路 |
//! |---|---|---|---|
//! | 1 | staging | Workers VPC の binding `PEOPLE_DB_VPC` (VPC Service 型、宛先の host:port は Service 側で固定。vein・lineworks・trouble・carins・records の staging と同じ Service) | TCP → 既存の Tunnel → staging の DB の PgBouncer (6432) |
//! | 2 | 本番 | Hyperdrive の binding `PEOPLE_HYPERDRIVE` (設定は実行用ロールのもの 1 つを複数の worker で共有する) | Worker → Hyperdrive → DB。接続・TLS・接続の使い回しは Hyperdrive が受け持つ。接続の部品は [`alc_worker_db::hyperdrive::connect`] |
//! | 3 | ローカル | 文字列 `DATABASE_URL` (worker 自身の secret / `wrangler dev --var`) | 接続文字列の host:port へ STARTTLS。`sslmode=disable` + var `ALLOW_INSECURE_DB = "1"` のときだけ手元の PgBouncer へ平文 |
//!
//! **2 は「binding が無い (undefined)」ときだけ 3 へ落ちる。** binding が在るのに使えない (読めない /
//! Hyperdrive の binding ではない / 接続文字列が parse できない / socket・handshake の失敗) ときは
//! [`ConnectError::Other`] (fetch が 500 にする) で、3 へは落とさない — Hyperdrive を使う環境が、
//! worker secret へ黙って戻らないようにするため (判定は `alc_worker_db::hyperdrive::connect` が持つ)。
//! どの段の binding も secret も無ければ [`ConnectError::NotConfigured`] (fetch が 503 にする)。
//! binding は接続文字列より先に見るので、平文に落ちる binding (`PEOPLE_DB_VPC`) を本番
//! (トップレベル) に置かないことを scripts/check-exposure.sh が検査する。
//!
//! `Socket` は `!Send` 相当 (JS の値) なので、この関数は handler (axum が Send を要求する側)
//! の外 = fetch から呼ぶ。

use alc_worker_db::PgClient;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Config, NoTls};
use worker::postgres_tls::PassthroughTls;
use worker::{console_error, Env, SecureTransport, Socket};

use crate::tcp::TcpPort;

/// staging の Workers VPC binding (VPC Service 型)。宛先 (staging の DB の PgBouncer) は Service 側で固定
const PEOPLE_DB_VPC_BINDING: &str = "PEOPLE_DB_VPC";
const PEOPLE_DB_NAME: &str = "people-db";
/// staging の DB の PgBouncer のポート
const PGBOUNCER_PORT: u16 = 6432;
/// 本番の Hyperdrive の binding (`[[hyperdrive]]`、wrangler.toml のトップレベルにだけ置く)
const PEOPLE_HYPERDRIVE_BINDING: &str = "PEOPLE_HYPERDRIVE";
/// 文字列の接続文字列 (worker 自身の secret / `wrangler dev --var`)。ローカルの経路
const DATABASE_URL_SECRET: &str = "DATABASE_URL";
/// 平文 (`sslmode=disable`) を許すローカル専用のフラグ。`wrangler dev --var ALLOW_INSECURE_DB:1`
/// か `.dev.vars` にだけ置く (wrangler.toml に書かないことを scripts/check-exposure.sh が検査する)
const ALLOW_INSECURE_DB_VAR: &str = "ALLOW_INSECURE_DB";

#[derive(Debug)]
pub enum ConnectError {
    /// どの経路の binding も接続文字列も無い
    NotConfigured,
    Other(String),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(
                f,
                "no {PEOPLE_DB_VPC_BINDING} / {PEOPLE_HYPERDRIVE_BINDING} binding and no {DATABASE_URL_SECRET} secret"
            ),
            Self::Other(e) => f.write_str(e),
        }
    }
}

fn other(what: &str) -> impl Fn(worker::Error) -> ConnectError + '_ {
    move |e| ConnectError::Other(format!("{what}: {e}"))
}

pub async fn connect(env: &Env) -> Result<PgClient, ConnectError> {
    if let Ok(vpc) = env.get_binding::<TcpPort>(PEOPLE_DB_VPC_BINDING) {
        return connect_vpc(&vpc).await;
    }
    // Hyperdrive の段 (ALLOW_INSECURE_DB は下の文字列の段でしか読まない)。
    // エラーの文は kit の Display = binding 名・段の label・kind だけ (文・宛先を含まない)
    match alc_worker_db::hyperdrive::connect(env, PEOPLE_HYPERDRIVE_BINDING).await {
        Ok(Some(client)) => return Ok(client),
        // binding が無い → 次の段 (ローカルの文字列) へ
        Ok(None) => {}
        // 在るのに使えない → 500 (次の段へ落とさない)
        Err(e) => return Err(ConnectError::Other(e.to_string())),
    }
    if let Ok(url) = env.secret(DATABASE_URL_SECRET) {
        let allow_insecure = env
            .var(ALLOW_INSECURE_DB_VAR)
            .is_ok_and(|v| v.to_string() == "1");
        return connect_url(&url.to_string(), allow_insecure, DATABASE_URL_SECRET).await;
    }
    Err(ConnectError::NotConfigured)
}

/// staging: Workers VPC の binding へ TCP を開き、そのまま postgres のソケットとして使う。
/// VPC Service 型は宛先の host:port を Service 側で固定するので、`connect()` に渡すアドレスは
/// 名目の値 (宛先は binding 側で固定され、この文字列は使われない。IP はコードに書かない)。Tunnel の区間は Cloudflare が暗号化し、ホスト内の
/// PgBouncer までは平文・trust 認証。
async fn connect_vpc(vpc: &TcpPort) -> Result<PgClient, ConnectError> {
    let raw = vpc
        .connect(&format!("{PEOPLE_DB_NAME}:{PGBOUNCER_PORT}"))
        .map_err(|e| ConnectError::Other(format!("people-db vpc connect: {e:?}")))?;
    handshake(pgbouncer_config(), Socket::from(raw), NoTls).await
}

/// staging の DB の PgBouncer (trust 認証) へ、RLS が効く `alc_api_app` で繋ぐ設定
fn pgbouncer_config() -> Config {
    let mut config = Config::new();
    config
        .user("alc_api_app")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    config
}

/// ローカルの文字列の段: 接続文字列の host:port へ STARTTLS で繋ぐ (postgres の SSLRequest の後に
/// `PassthroughTls` が Workers の `startTls()` を呼ぶ)。`source` は読み先の名前 (エラーの文言に出すだけ)。
///
/// 平文で繋ぐのは、接続文字列が `sslmode=disable` で**かつ** ローカル専用フラグ
/// `ALLOW_INSECURE_DB = "1"` があるときだけ (ローカルの `wrangler dev` から手元の PgBouncer へ繋ぐ用)。
/// フラグが無ければ `sslmode=disable` でも TLS を強制する — 本番の接続文字列に
/// `sslmode=disable` が紛れ込んでも平文には落ちない。
async fn connect_url(
    url: &str,
    allow_insecure: bool,
    source: &str,
) -> Result<PgClient, ConnectError> {
    let mut config: Config = url
        .parse()
        .map_err(|e| ConnectError::Other(format!("parse {source}: {e}")))?;
    let host = match config.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        _ => return Err(ConnectError::Other(format!("{source} has no tcp host"))),
    };
    let port = config.get_ports().first().copied().unwrap_or(5432);
    if allow_insecure && config.get_ssl_mode() == SslMode::Disable {
        let socket = Socket::builder()
            .connect(host, port)
            .map_err(other("socket"))?;
        return handshake(config, socket, NoTls).await;
    }
    config.ssl_mode(SslMode::Require);
    let socket = Socket::builder()
        .secure_transport(SecureTransport::StartTls)
        .connect(host, port)
        .map_err(other("socket"))?;
    handshake(config, socket, PassthroughTls).await
}

/// 原因の文の長さの上限 (ランタイムが返す文をそのまま載せるので、際限なく出さない)
const CAUSE_MAX_CHARS: usize = 300;

/// エラーの文に、原因 (`source()`) の文を `: ` でつないで足す。source が無ければエラーの文だけ。
///
/// 通信エラーの原因は Workers のランタイムが返す文 (`Socket` が `io::Error` に包む) で、中身はランタイムしだい。
/// 宛先が混ざっても出さないよう、接続設定の host と同じ文字列は `<host>` に置き換える
/// (接続文字列・ユーザー名・パスワードはここでは組み立てない)。
fn with_causes(e: &(dyn std::error::Error + 'static), config: &Config) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        let mut text = cause.to_string();
        // wasm の Host は Tcp だけ (unix socket の variant は cfg(unix))
        for tokio_postgres::config::Host::Tcp(h) in config.get_hosts() {
            if !h.is_empty() {
                text = text.replace(h.as_str(), "<host>");
            }
        }
        out.push_str(": ");
        out.extend(text.chars().take(CAUSE_MAX_CHARS));
        source = cause.source();
    }
    out
}

async fn handshake<T>(config: Config, socket: Socket, tls: T) -> Result<PgClient, ConnectError>
where
    T: tokio_postgres::tls::TlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let (client, connection) = config.connect_raw(socket, tls).await.map_err(|e| {
        // tokio_postgres::Error の Display は種別の文だけ ("db error" / "error communicating with the server")
        // なので、DB エラーなら message と code を、それ以外なら原因 (source) の文を載せる
        let detail = match e.as_db_error() {
            Some(db) => format!("{} ({})", db.message(), db.code().code()),
            None => with_causes(&e, &config),
        };
        ConnectError::Other(format!("postgres handshake: {detail}"))
    })?;
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = connection.await {
            console_error!("postgres connection: {e}");
        }
    });
    // 生の Client はこのファイルの外へ出さない (名前付き prepared statement を流せる口を渡さない)
    Ok(PgClient::new(client))
}
