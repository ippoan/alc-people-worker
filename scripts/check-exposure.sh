#!/usr/bin/env bash
# alc-people が外から素で届かないことを wrangler.toml で検査する (ippoan/alc-lineworks-worker の同名の script の写し、
# 元は ippoan/alc-vein-worker。people は ippoan/alc-records-worker から写した。Refs ippoan/rust-alc-api#680)。
#
# この Worker は呼び手を認証せず (X-Tenant-ID を信頼する)、その tenant の社員・タイムカードを読み書きするので、
# 到達経路は auth-worker からの Service Binding だけでなければならない (ippoan/rust-alc-api#556 と同じ穴を開けない)。
#   - トップレベル (本番): workers_dev = false と preview_urls = false が明示されている
#   - どこにも (トップレベル・env 配下とも) route / routes が無い (custom_domain も routes の中に書く)
#   - どの env (staging を含む) にも workers_dev = true / preview_urls = true が無い
#     (入口は Service Binding だけ。トップレベルの true を env が継承するのも不可)
#   - どの vars にも ALLOW_INSECURE_DB (DB への平文接続を許すローカル専用フラグ) が無い
#   - トップレベル (本番) に平文 (NoTls + trust) で繋ぐ DB の binding が無い: vpc_services /
#     vpc_networks (Workers VPC)。src/db.rs はこの binding を Hyperdrive の binding PEOPLE_HYPERDRIVE →
#     文字列 DATABASE_URL より先に見るので、本番に紛れると TLS を強制する経路を飛ばして平文に落ちる。
#     env.staging にだけ置く
#   - どの階層にも (トップレベル・env 配下とも) durable_objects と containers が無い
#     (この worker は Durable Object も Container も持たない)
#   - env 配下 (env.staging を含む) に hyperdrive が無い (ippoan/rust-alc-api#723)。Hyperdrive の binding は本番の DB へ
#     届くので、トップレベル (本番) にだけ置く (staging に置かない)
#   - env 配下 (env.staging を含む) の r2_buckets が本番の bucket (トップレベルの r2_buckets の bucket_name) を指さない
#     (本番のファイルを読み書きする)
#   - env 配下 (env.staging を含む) の services が本番の Worker (トップレベルの services の service) を指さない
#     (いまは Service Binding を持たない。足したときに staging から本番の Worker へ届かないように)
# 違えば exit 1。CI で毎回走らせる。陰性対照は scripts/check-exposure-test.sh。
#
#   bash scripts/check-exposure.sh [wrangler.toml]
set -euo pipefail

TOML="${1:-$(dirname "$0")/../wrangler.toml}"

python3 - "$TOML" <<'PY'
import sys
import tomllib

path = sys.argv[1]
with open(path, "rb") as f:
    cfg = tomllib.load(f)

errors = []

def err(msg):
    errors.append(msg)
    print(f"::error file={path}::{msg}")

# 本番 (トップレベル): 明示的に false
for key in ("workers_dev", "preview_urls"):
    if cfg.get(key) is not False:
        err(f"トップレベルに {key} = false がない (本番が外から直接届く)")

envs = cfg.get("env", {})

for scope, table in [("トップレベル", cfg)] + [(f"env.{n}", e) for n, e in envs.items()]:
    for key in ("route", "routes"):
        if key in table:
            err(f"{scope} に {key} がある (外から直接届く)")

# どの env も公開しない (workers_dev / preview_urls は env へ継承される)
for name, e in envs.items():
    for key in ("workers_dev", "preview_urls"):
        if e.get(key, cfg.get(key)) is True:
            err(f"env.{name} の {key} が true (どの env も workers.dev / preview URL を開けない)")

# 平文の DB 接続を許すフラグはローカル (wrangler dev --var / .dev.vars) にだけ置く
for scope, table in [("トップレベル", cfg)] + [(f"env.{n}", e) for n, e in envs.items()]:
    if "ALLOW_INSECURE_DB" in table.get("vars", {}):
        err(f"{scope} の vars に ALLOW_INSECURE_DB がある (DB 接続が平文に落ちうる。ローカル専用)")

# 平文の DB 経路の binding は staging 専用 (db.rs が secret より先に見るので、本番にあると TLS を飛ばす)
for key in ("vpc_services", "vpc_networks"):
    if cfg.get(key):
        err(f"トップレベルに {key} がある (本番の DB 接続が平文の VPC 経路に落ちる。env.staging にだけ置く)")

# この worker は Durable Object も Container も持たない (どの階層にも置かない)
for scope, table in [("トップレベル", cfg)] + [(f"env.{n}", e) for n, e in envs.items()]:
    for key in ("durable_objects", "containers"):
        if key in table:
            err(f"{scope} に {key} がある (この worker は Durable Object も Container も持たない)")

# Hyperdrive の binding は本番の DB へ届く。staging を含め、env 配下には置かない
for name, e in envs.items():
    if e.get("hyperdrive"):
        err(f"env.{name} に hyperdrive がある (本番の DB へ届く binding。トップレベルにだけ置く)")

# 本番の R2 の bucket を env 配下 (staging を含む) から指さない (本番のファイルを読み書きする)
prod_buckets = {b.get("bucket_name") for b in cfg.get("r2_buckets", [])}
for name, e in envs.items():
    for b in e.get("r2_buckets", []):
        if b.get("bucket_name") in prod_buckets:
            err(f"env.{name} の r2_buckets が本番の bucket {b['bucket_name']} を指している (env 用の bucket を指すこと)")

# 本番の Worker への Service Binding を env 配下 (staging を含む) から張らない
prod_services = {b.get("service") for b in cfg.get("services", [])}
for name, e in envs.items():
    for b in e.get("services", []):
        if b.get("service") in prod_services:
            err(f"env.{name} の services が本番の Worker {b['service']} を指している (env 用の Worker を指すこと)")

if errors:
    sys.exit(1)
print(f"OK: {path} は本番が workers_dev / preview_urls = false・route 無しで、どの env も workers.dev を開けない")
PY
