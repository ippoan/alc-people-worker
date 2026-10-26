//! 実 DB のテストの準備・接続 (ippoan/alc-records-worker の tests/support の型)。
//!
//! - **`PEOPLE_TEST_ADMIN_DATABASE_URL` が未設定なら失敗する** (skip して緑にしない)。値は準備用の superuser の
//!   接続文字列で、**使い捨ての DB (database 名 `postgres`) に向けること** (ロール・schema・全 migration を作る)。表示はしない
//! - 準備の SQL は ippoan/alc-migrations から版を固定して取る (`scripts/ALC_MIGRATIONS_REV`、
//!   `bash scripts/fetch-migrations.sh` が repo 直下の `.alc-migrations/` に取り出す)。流す順は init_local_db.sql →
//!   空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) → local_app_grants.sql
//! - 本体のテストは、本番の worker と同じ実行用ロール `alc_api_rt` (migration 158。表の所有者ではない・`NOSUPERUSER`・
//!   `NOBYPASSRLS`) で繋ぐ ([`client_as`] が毎回確かめる)。パスワードは使い捨ての DB 専用の固定の文字列で、秘密ではない

#![allow(dead_code)]

use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::Router;
use futures_util::lock::Mutex;
use people::{router, AppState};
use tokio::sync::OnceCell;
use tokio_postgres::{Client, Config, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

pub const ADMIN_URL_ENV: &str = "PEOPLE_TEST_ADMIN_DATABASE_URL";
pub const RT_ROLE: &str = "alc_api_rt";
/// 口の表を読めるが書けないロール (行の作成が失敗する経路を通すため)
pub const READ_ONLY_ROLE: &str = "people_test_read_only";
/// 使い捨ての DB 専用 (秘密ではない)
const PASSWORD: &str = "people-test-throwaway";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";
/// 準備を直列にする advisory lock の鍵 (並列に走るテストの binary どうし)
const PREPARE_LOCK: i64 = 1_012_682;

/// 口の表 (読み取り専用のロールに SELECT だけ付ける)
pub const TABLES: &[&str] = &[
    "timecard_cards",
    "employees",
    "hub_measurements",
    "measurements",
    "tenko_sessions",
];

fn migrations_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        ".alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

pub fn admin_config() -> Config {
    let url = std::env::var(ADMIN_URL_ENV).unwrap_or_else(|_| {
        panic!("{ADMIN_URL_ENV} が未設定。実 DB のテストは接続先が無ければ失敗させる (skip しない)")
    });
    url.parse()
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} が接続文字列として読めない"))
}

async fn raw_connect(config: &Config) -> Client {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} の DB に繋げない"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// superuser の接続 (準備と、テストの行を作る・見るのだけに使う)
pub async fn admin() -> Client {
    let mut config = admin_config();
    config.options(SEARCH_PATH);
    raw_connect(&config).await
}

/// 準備をプロセスの中で 1 回だけ流す。既に読み取り専用のロールが在れば (別の binary が流した) 何もしない
pub async fn prepare() {
    static PREPARED: OnceCell<()> = OnceCell::const_new();
    PREPARED
        .get_or_init(|| async {
            let su = admin().await;
            su.batch_execute(&format!("SELECT pg_advisory_lock({PREPARE_LOCK})"))
                .await
                .unwrap();
            let done = su
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[&READ_ONLY_ROLE],
                )
                .await
                .unwrap()
                .get::<_, bool>(0);
            if !done {
                migrate(&su).await;
            }
            su.batch_execute(&format!("SELECT pg_advisory_unlock({PREPARE_LOCK})"))
                .await
                .unwrap();
            eprintln!("[people-db] 実 DB で準備済み (migrations は {})", rev());
        })
        .await;
}

fn rev() -> String {
    std::fs::read_to_string(migrations_dir().join(".rev"))
        .unwrap()
        .trim()
        .to_owned()
}

async fn migrate(su: &Client) {
    let dir = migrations_dir();
    let read = |p: std::path::PathBuf| std::fs::read_to_string(p).unwrap();
    su.batch_execute(&read(dir.join("scripts/init_local_db.sql")))
        .await
        .unwrap();
    su.batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("migrations"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    assert!(
        files.iter().any(|f| f
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("_runtime_role.sql")),
        "取り出した migrations に 158 (alc_api_rt) が無い (scripts/ALC_MIGRATIONS_REV が古い)"
    );
    for file in files {
        let name = file.file_name().unwrap().to_owned();
        if let Err(e) = su
            .batch_execute(&format!("BEGIN;\n{}\n;COMMIT;", read(file)))
            .await
        {
            panic!("migration {name:?}: {:?}", e.code());
        }
    }
    su.batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
        .await
        .unwrap();
    // 実行用ロール (158 が NOLOGIN で作る) と、口の表を読むだけのロール
    let tables = TABLES
        .iter()
        .map(|t| format!("alc_api.{t}"))
        .collect::<Vec<_>>()
        .join(", ");
    su.batch_execute(&format!(
        "ALTER ROLE {RT_ROLE} LOGIN PASSWORD '{PASSWORD}';
         CREATE ROLE {READ_ONLY_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{PASSWORD}';
         GRANT USAGE ON SCHEMA alc_api TO {READ_ONLY_ROLE};
         GRANT SELECT ON {tables} TO {READ_ONLY_ROLE};"
    ))
    .await
    .unwrap();
}

/// `role` の接続 (worker と同じく `PgClient` に包む)。**superuser / BYPASSRLS / 触る表の所有者なら panic**
pub async fn client_as(role: &str) -> PgClient {
    prepare().await;
    let mut config = admin_config();
    config.user(role).password(PASSWORD);
    let client = raw_connect(&config).await;
    let row = client
        .query_one(
            "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
             (SELECT coalesce(bool_or(tableowner = current_user), false) FROM pg_tables \
              WHERE schemaname = 'alc_api' AND tablename = ANY($1))",
            &[&TABLES],
        )
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0), "RLS を素通りするロールで繋いでいる");
    assert!(!row.get::<_, bool>(1), "表の所有者で繋いでいる");
    PgClient::new(client)
}

pub async fn rt_client() -> PgClient {
    client_as(RT_ROLE).await
}

/// 接続が切れた `alc_api_rt` の接続 (backend を落とした接続)
pub async fn broken_client() -> PgClient {
    let mut pg = rt_client().await;
    let pid: i32 = pg
        .tenant_tx(Uuid::nil(), |tx| {
            Box::pin(async move {
                let row = tx.query_typed_one("SELECT pg_backend_pid()", &[]).await?;
                Ok(row.get::<_, i32>(0))
            })
        })
        .await
        .unwrap();
    let terminated: bool = admin()
        .await
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
    pg
}

/// tenant を 1 つ作る (superuser で。名前は乱数)。
pub async fn tenant() -> Uuid {
    prepare().await;
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenants (id, name) VALUES ($1, $2)",
            &[&id, &format!("people-test-{id}")],
        )
        .await
        .unwrap();
    id
}

pub struct App {
    pub router: Router,
    pub logs: Arc<StdMutex<Vec<String>>>,
}

impl App {
    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }
}

/// worker の src/lib.rs と同じ組み立て
pub fn build(pg: PgClient) -> App {
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
    };
    App {
        router: router().with_state(state),
        logs,
    }
}

/// 社員を 1 人作る (superuser で)
pub async fn employee(tenant: Uuid, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, name) VALUES ($1, $2, $3)",
            &[&id, &tenant, &name],
        )
        .await
        .unwrap();
    id
}

pub async fn app() -> App {
    build(rt_client().await)
}

/// ログが固定の頭で始まり、識別子・名前・番号を含まないこと
pub fn assert_logs_clean(app: &App, secrets: &[String]) {
    for line in app.logs() {
        assert!(line.starts_with("people: "), "固定の頭で始まらないログ");
        for s in secrets {
            assert!(!line.contains(s.as_str()), "ログに識別子・名前が出た");
        }
    }
}

pub struct Got {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Got {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// 1 リクエストを打つ。`tenant` が在れば `X-Tenant-ID` を付ける。`body` が在れば JSON で送る
pub async fn call(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    body: Option<serde_json::Value>,
) -> Got {
    call_with(app, method, uri, tenant, &[], body).await
}

/// dev端末の要求 (auth-worker が付ける `X-Device-Dev: 1`) として打つ
pub async fn call_dev(app: &App, method: &str, uri: &str, tenant: Uuid) -> Got {
    call_with(
        app,
        method,
        uri,
        Some(tenant),
        &[("X-Device-Dev", "1")],
        None,
    )
    .await
}

/// 欄を足して打つ
pub async fn call_with(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    headers: &[(&str, &str)],
    body: Option<serde_json::Value>,
) -> Got {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        b = b.header("X-Tenant-ID", t.to_string());
    }
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let bytes = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            serde_json::to_vec(&v).unwrap()
        }
        None => Vec::new(),
    };
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(bytes)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got {
        status,
        headers,
        body,
    }
}
