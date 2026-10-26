#!/bin/bash
# ★ ohishi-exp/rust-ichibanboshi の scripts/check_coverage_100.sh (2293d66) をそのまま写したもの
#   (ippoan/alc-dtako-worker・ohishi-exp/rust-leave-worker・ippoan/alc-lineworks-worker 経由。Refs ippoan/rust-alc-api#747)。
#   この枠より下は写しのまま変えていない — 直すときは元と揃える。
#   この repo での使い方は 1 つだけ (ci.yml の「Coverage 100% gate」と同じ。登録簿は直下の coverage_100.toml):
#
#     cargo llvm-cov --locked -p people --text > cov.txt
#     bash scripts/check_coverage_100.sh --use-cache cov.txt
#
#   crate を足したら、crate ごとに別の回で計測した結果を `--use-cache` を繰り返して 1 回で渡す
#   (いまの crate は crates/people の 1 つだけ)。
#
#   `--use-cache` 無し (script が自分で `cargo llvm-cov --text` を打つ形) は使わない — 直下の package は
#   wasm 専用の worker で、計測の対象は crates/people だけ。
#   この枠の中の説明と、`--use-cache` を繰り返せること (EXTERNAL_CACHES) だけが元からの変更。
#
# coverage_100.toml に登録されたファイルが 100% 行カバレッジを維持しているか検証する。
#
# Usage:
#   bash scripts/check_coverage_100.sh
#   bash scripts/check_coverage_100.sh --use-cache /path/to/llvm-cov-text.txt
#   bash scripts/check_coverage_100.sh --config workers/kyuyo/coverage_100.toml --use-cache <text>
#     (workers/kyuyo は別 workspace。計測は worker-kyuyo.yml が --manifest-path で取って渡す)
#
# 前提: cargo-llvm-cov がインストール済み。DB / 外部サービスは不要
#       (本 repo のテストは全て MockRepo / wiremock / 純粋関数で完結する)。
#
# 判定は `cargo llvm-cov --text` の行注釈ベース。--json は閉じ括弧等を余分に
# 数えて結果が変わるので使わない (rust-alc-api での確認事項)。
#
# ## 先例 (rust-alc-api) から意図的に変えた点
#
# 1. **登録済みなのに計測データに無いファイルは FAIL** (先例は WARN + continue)。
#    握り潰すと「登録簿にあるのに一度も検証されていないファイル」が生まれる。
#    先例では実際に crates/alc-csv-parser/src/kudgivt.rs が `--lib` が root
#    package しか計測しないせいで黙ってスキップされ続けていた。gate が守って
#    いる範囲を利用者が誤認するのが最悪なので、ここは必ず落とす。
#    実行行 0 のファイル (mod.rs 等) も同じ理由で FAIL — 検証できないものを
#    登録簿に置かない。
# 2. **パス照合は「/」区切りの suffix 完全一致** (先例は素の grep = 部分一致)。
#    部分一致は別ファイルに当たり得るし、複数行ヒット時に数値比較が壊れる。
# 3. **ヒット数の k / M サフィックスを解釈する** (先例の awk は `1.19k` を
#    covered とも uncovered とも数えず総行数から静かに落としていた。本 repo の
#    実測では 87 行が該当した)。
# 4. `--unit-only` / `--mock-only` は無い (登録簿に `type` が無いため。理由は
#    coverage_100.toml の頭を参照)。未知のオプションは黙殺せず usage error。

set -euo pipefail

usage() {
  echo "usage: $0 [--config <registry toml>] [--use-cache <llvm-cov --text output>]" >&2
}

EXTERNAL_CACHES=()
CONFIG="coverage_100.toml"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --use-cache)
      [[ $# -ge 2 ]] || { echo "ERROR: --use-cache requires a file argument" >&2; usage; exit 2; }
      EXTERNAL_CACHES+=("$2"); shift 2 ;;
    --config)
      [[ $# -ge 2 ]] || { echo "ERROR: --config requires a file argument" >&2; usage; exit 2; }
      CONFIG="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: $CONFIG not found (repo root から実行すること)" >&2
  exit 1
fi

# --- カバレッジデータの用意 ---
# ci.yml の "Upload coverage text (artifact)" が /tmp/llvm-cov-cache/text-*.txt を
# 拾うので、mktemp ではなく安定パスに置く (mktemp だと artifact が常に空になる)。
if [[ ${#EXTERNAL_CACHES[@]} -gt 0 ]]; then
  for f in "${EXTERNAL_CACHES[@]}"; do
    if [[ ! -f "$f" ]]; then
      echo "ERROR: --use-cache file not found: $f" >&2
      exit 1
    fi
    echo "Using pre-built coverage data: $f"
  done
  CACHE_FILES=("${EXTERNAL_CACHES[@]}")
else
  CACHE_DIR="/tmp/llvm-cov-cache"
  mkdir -p "$CACHE_DIR"
  PROJECT_HASH=$(printf '%s' "$PWD" | md5sum | cut -c1-8)
  CACHE_FILE="$CACHE_DIR/text-$PROJECT_HASH.txt"
  echo "Running cargo llvm-cov --text ..."
  if ! cargo llvm-cov --text > "$CACHE_FILE" 2>"$CACHE_FILE.stderr"; then
    echo "cargo llvm-cov failed:" >&2
    tail -50 "$CACHE_FILE.stderr" >&2
    exit 101
  fi
  CACHE_FILES=("$CACHE_FILE")
fi

python3 - "$CONFIG" "${CACHE_FILES[@]}" <<'PYEOF'
import re
import sys
import tomllib

config_path, cov_paths = sys.argv[1], sys.argv[2:]
cov_path = " ".join(cov_paths)

with open(config_path, "rb") as fh:
    registered = [f["path"] for f in tomllib.load(fh).get("files", [])]

if not registered:
    print("ERROR: coverage_100.toml に登録ファイルがありません")
    sys.exit(1)

HEADER = re.compile(r"^(/.*\.rs):$")
# "  123|   4.51k| source"  — ヒット数は 12 / 1.19k / 2.50M いずれもあり得る
COUNTED = re.compile(r"^\s*(\d+)\|\s*([0-9][0-9.]*)([kKmMgG]?)\s*\|")
# "  123|       | source"   — 非実行行 (コメント・宣言等)
UNCOUNTED = re.compile(r"^\s*\d+\|\s*\|")

SCALE = {"": 1, "k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6, "g": 1e9, "G": 1e9}

# path -> {line_no: hit}
files = {}
for one_path in cov_paths:
    cur = None
    with open(one_path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            m = HEADER.match(line)
            if m:
                cur = files.setdefault(m.group(1), {})
                continue
            if cur is None or UNCOUNTED.match(line):
                continue
            m = COUNTED.match(line)
            if m:
                ln = int(m.group(1))
                hit = float(m.group(2)) * SCALE[m.group(3)]
                # 同一行が複数リージョンで出ることがある。最大ヒットを採る
                cur[ln] = max(cur.get(ln, 0.0), hit)


def lookup(path):
    """登録パスに対応する計測レコードを返す。'/' 区切りの suffix 完全一致。"""
    needle = "/" + path.lstrip("/")
    hits = [(p, d) for p, d in files.items() if p == path or p.endswith(needle)]
    if len(hits) > 1:
        return "ambiguous", [p for p, _ in hits]
    if not hits:
        return "missing", None
    return "ok", hits[0]


print("=== Coverage 100% Check ===")
print(f"Registered files: {len(registered)}")
print(f"Files in coverage data: {len(files)}")
print()

failed = 0
checked = 0
missing = 0

for path in sorted(registered):
    status, payload = lookup(path)

    if status == "missing":
        print(f"FAIL: {path} — カバレッジデータに存在しない")
        print("      登録簿にあるのに検証されていない状態。以下のいずれか:")
        print("        - ファイルが移動/削除された → 登録簿を直す")
        print("        - 実行行が 0 (mod.rs 等の re-export のみ) → 登録簿から外す")
        print("        - cfg で当該プラットフォームのコンパイル対象外 → 登録簿から外す")
        failed += 1
        missing += 1
        continue

    if status == "ambiguous":
        print(f"FAIL: {path} — 複数のカバレッジレコードに一致: {payload}")
        failed += 1
        continue

    full_path, lines = payload
    total = len(lines)

    if total == 0:
        print(f"FAIL: {path} — 実行行が 0 (検証できないものは登録しない)")
        failed += 1
        continue

    checked += 1
    missed = sorted(ln for ln, hit in lines.items() if hit == 0)

    if missed:
        covered = total - len(missed)
        pct = covered * 100.0 / total
        print(f"FAIL: {path} — {covered}/{total} lines ({pct:.1f}%, {len(missed)} lines missing)")
        shown = ", ".join(str(n) for n in missed[:20])
        if len(missed) > 20:
            shown += f", … (+{len(missed) - 20})"
        print(f"      uncovered: {shown}")
        failed += 1
    else:
        print(f"  OK: {path} — {total}/{total} lines (100%)")

print()
print(f"Checked: {checked} / Registered: {len(registered)}")

if failed:
    print()
    if registered and missing == len(registered):
        print("ERROR: 登録簿の全ファイルがカバレッジデータで見つかりませんでした。")
        print("       これは「カバレッジ不足」ではなく、十中八九スクリプトが")
        print("       llvm-cov の出力を読めていない (ヘッダ行のパス判定ミス /")
        print(f"       cargo llvm-cov の失敗など)。{cov_path} の中身を確認すること。")
    print(f"FAILED: {failed} 件。カバレッジ回帰、または登録簿と実体のズレ。")
    sys.exit(1)

print("All registered files maintain 100% coverage.")
PYEOF
