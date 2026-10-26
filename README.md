# alc-people-worker

alc の社員・タイムカードの Cloudflare Worker `alc-people`。rust-alc-api (Cloud Run) の `crates/alc-misc` の
timecard と employees を移したもの (Refs ippoan/rust-alc-api#680)。型は ippoan/alc-records-worker と同じ。

呼び手は auth-worker の Service Binding だけ (この Worker は認証しない。`X-Tenant-ID` を信頼する)。

## 口 (`/api` の下。rust と同じ path・JSON・status)

| path | method | 中身 |
|---|---|---|
| `/api/timecard/cards` | GET / POST | 台帳の一覧 (`?employee_id=`) と 1 枚の登録 (card_id は正規化して入れる。同じ card_id は 409) |
| `/api/timecard/cards/{id}` | GET / DELETE | 取得・削除 (無ければ 404) |
| `/api/timecard/cards/bulk-by-code` | PUT | 社員番号キーの一括取り込み (1〜500 件。skip があっても 200。社員の居ないカードは保留で受け入れる。`dry_run`) |
| `/api/timecard/cards/delete-by-card` | POST | 一括取り込みで入れたカード 1 枚を外す (範囲外・不在・形不正も 200 + `reason`) |
| `/api/timecard/cards/lookup` | POST | カードから在籍の社員 (台帳 → 免許証番号。未登録・保留・退職は 404。書き込まない) |
| `/api/timecard/punches` | GET | 打刻の一覧 (`hub_measurements` から導出。社員・期間・頁。dev端末は dev の行だけ) |
| `/api/timecard/punches/csv` | GET | 同じ打刻の CSV (BOM 付き、打刻日時は JST) |
| `/api/employees` | GET / POST | 社員の一覧 (名前の順、在籍だけ) と作成 |
| `/api/employees/{id}` | GET / PUT / DELETE | 取得・更新 (同じ社員番号は 409)・論理削除 |
| `/api/employees/{id}/face`・`/face/approve`・`/face/reject` | PUT | 顔写真・embedding (1024 次元。変えたら承認待ち)・承認・却下 |
| `/api/employees/face-data` | GET | 承認済みの顔の embedding |
| `/api/employees/{id}/nfc`・`/license` | PUT (license は DELETE も) | 免許証番号・交付日と有効期限 (PUT の null は変更なし、DELETE で 3 列とも消す) |
| `/api/employees/lookup` | POST | 免許証番号から在籍の社員 (書き込まない) |
| `/api/employees/by-code/{code}` | GET | 社員番号から在籍の社員 |
| `/api/employees/bulk-by-code` | PUT | 乗務員マスタの一括取り込み (1〜500 件。衝突は skip して 200。保留中のカードを同じ transaction で結ぶ) |

## DB

本番は Hyperdrive (`PEOPLE_HYPERDRIVE`、設定はほかの worker と共有)、staging は Workers VPC (`PEOPLE_DB_VPC`) の先の PgBouncer。
どちらもトランザクション単位の RLS (`alc-worker-db` の `tenant_tx`)。

## デプロイ

PR → `wrangler deploy --dry-run` / main → staging / タグ `v*` (Tag Release を手動) → 本番。
