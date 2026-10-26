# alc-people-worker

社員・タイムカードの Worker `alc-people` (Refs ippoan/rust-alc-api#680)。rust-alc-api の `crates/alc-misc` の
timecard (W4d-a) と、後で employees (W4d-b) を移す先で、path・method・JSON の形・status は rust と同じ
(alc-app・運行管理者席・tc-ohishi のカード同期がそのまま使う)。口は `/api/timecard/...` (`crates/people/src/timecard.rs`)。
直下 = Worker (wasm 専用)、`crates/people/` = 口・SQL。

**呼び手は auth-worker の Service Binding だけ** (auth-worker が JWT / 端末の鍵 / RPC を検証し `X-Tenant-ID`・`X-Device-Dev` を
付け直す。この Worker は認証しない)。auth-worker の振り分け (`ALC_BINDING_ROUTES`) は別の PR。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
PEOPLE_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ。URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`PEOPLE_HYPERDRIVE`) は本番だけ。`wrangler.toml` の表の順を変えない (新しい表は placement より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id・名前・card_id を出さない。
  DB の失敗は `people: <段> failed: db (<kind>)`。応答の本文は rust と同じ (多くは空、一括の 2 口は文字列)。
- **card_id の正規化は `timecard::normalize_card_id` の 1 か所** (打刻の一覧の SQL の `lower(replace(btrim(..), ':', ''))` と
  同じ規則。片方だけ変えると照合が静かに外れる)。カードの照会 (`/timecard/cards/lookup`) に書き込みを足さない。
- dev端末の要求 (`X-Device-Dev: 1`) は打刻の一覧・CSV で `app.device_dev` を立てる (`hub_measurements` 等の RLS)。
- 打刻の書き込みはこの Worker に無い (alc-devices の `POST /api/hub/measurements`)。
- role は見ない (rust も `X-Tenant-ID` だけ)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
  WHERE に tenant_id も明示する。SQL の意味は rust の repo と同じにする。`dry_run` は SAVEPOINT へ戻して書かない。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 6・timecard_db 8) はテストを足したら上げる。
- public repo: ホスト名・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-records-worker と同じ rev)。
