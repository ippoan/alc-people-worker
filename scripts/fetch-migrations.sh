#!/usr/bin/env bash
# 実 DB のテスト (crates/people/tests/*_db.rs) の準備が流す SQL を、正本の ippoan/alc-migrations (public) から
# 版を固定して取り出す (ippoan/alc-lineworks-worker の同名の script の写し、元は ippoan/alc-vein-worker。
# Refs ippoan/rust-alc-api#680)。
# 写しをこの repo に commit しない。
#
#   - 版は scripts/ALC_MIGRATIONS_REV の 1 行 (commit の SHA)。**rev を書くのはそのファイルだけ**
#   - 取り出す先は repo 直下の .alc-migrations/ (.gitignore 済み)。持ち込むのは
#     scripts/init_local_db.sql・scripts/local_app_grants.sql・migrations/ だけ
#   - 既に同じ rev が在れば何もしない
# テスト・coverage の計測の前に必ず通す (通していないとテストが落ちる)。
#
#   bash scripts/fetch-migrations.sh
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_URL="https://github.com/ippoan/alc-migrations"
dest=".alc-migrations"
rev="$(tr -d '[:space:]' <scripts/ALC_MIGRATIONS_REV)"
if ! [[ "$rev" =~ ^[0-9a-f]{40}$ ]]; then
  echo "scripts/ALC_MIGRATIONS_REV が 40 桁の commit SHA ではない" >&2
  exit 1
fi

if [ -f "$dest/.rev" ] && [ "$(cat "$dest/.rev")" = "$rev" ]; then
  echo "alc-migrations ${rev}: 取得済み (${dest})"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git init -q "$tmp/src"
git -C "$tmp/src" fetch -q --depth 1 "$REPO_URL" "$rev"
git -C "$tmp/src" checkout -q FETCH_HEAD
got="$(git -C "$tmp/src" rev-parse HEAD)"
if [ "$got" != "$rev" ]; then
  echo "取り出した commit (${got}) が scripts/ALC_MIGRATIONS_REV (${rev}) と違う" >&2
  exit 1
fi

mkdir -p "$tmp/out/scripts"
cp "$tmp/src/scripts/init_local_db.sql" "$tmp/src/scripts/local_app_grants.sql" "$tmp/out/scripts/"
cp -R "$tmp/src/migrations" "$tmp/out/migrations"
# .rev は最後に書く (途中で落ちたら次回は取り直す)
echo "$rev" >"$tmp/out/.rev"
rm -rf "$dest"
mv "$tmp/out" "$dest"
echo "alc-migrations ${rev}: $(ls "$dest/migrations"/*.sql | wc -l) 個の migration を ${dest} に取り出した"
