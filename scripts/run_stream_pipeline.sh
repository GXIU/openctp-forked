#!/usr/bin/env bash
# Run the full live pipeline together: production Rust stream → 3m graph bars
# → signal (alpha = bars mean) → sinks, as one command.
#
# Usage:
#   scripts/run_stream_pipeline.sh [DATE] [STOCKS] [GRAPH_YAML] [OUT_JSONL]
#
# Defaults: 20220104, 20 stocks, alfa cqr v260823, bars/live_stream.jsonl.
# Requires: cn_rst3 binary (v260827+ for --graph), local place/trade parquet,
#           the .venv-live python.
set -euo pipefail

DATE="${1:-20220104}"
STOCKS="${2:-20}"
GRAPH="${3:-/home/george/code/alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml}"
OUT="${4:-bars/live_stream.jsonl}"
DATA_DIR="${DATA_DIR:-/mnt/disks/data/cn/raw/parquet}"
CN_RST3="${CN_RST3:-/mnt/disks/data/cargo-target/release/cn_rst3}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -x "$CN_RST3" ]]; then
  echo "cn_rst3 binary not found at $CN_RST3 (set CN_RST3=/path/to/cn_rst3)" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"
"$CN_RST3" stream-real \
  --date "$DATE" --data-dir "$DATA_DIR" --stocks "$STOCKS" \
  --graph "$GRAPH" --freq 3m \
  --bars-stdout-ipc 2>/dev/null \
  | PYTHONPATH="$REPO_ROOT" "$REPO_ROOT/.venv-live/bin/python" -m live.ipc_consumer \
      --stdin --jsonl "$OUT"
