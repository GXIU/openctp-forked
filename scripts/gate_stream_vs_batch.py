#!/usr/bin/env python3
"""Fidelity gate: cn_rst3 stream (graph eval) vs batch bars, cellwise.

Runs the two production paths on the same date/universe and compares every
leaf cell of the 3m graph bars:
  1. `cn_rst3 bars --graph G --freq F --max-stocks N` → parquet (batch)
  2. `cn_rst3 stream-real --graph G --freq F --stocks N --bars-ipc` → IPC
Comparison is restricted to the sids present in the stream output, so the
two runs need not pick identical universes.

The standing gate (pending the obgx replay-state port): **0 mismatches**,
i.e. `--expect-exact` must exit 0. Without it the script reports the
percentage and exits 0. This is the rerunnable form of the 2026-08-27
experiment E4d (93.33% on 100 stocks — the design doc's pre-flagged
streaming-vs-batch fidelity risk).

Usage:
  scripts/gate_stream_vs_batch.py                       # 3 stocks, report only
  scripts/gate_stream_vs_batch.py --stocks 100 --expect-exact
"""

import argparse
import json
import math
import subprocess
import sys
import tempfile
from pathlib import Path

CN_RST3 = "/mnt/disks/data/cargo-target/release/cn_rst3"
DATA_DIR = "/mnt/disks/data/cn/raw/parquet"
GRAPH = "/home/george/code/alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml"
CONFIG = "/home/george/code/obgx/study/alpha/run_config_cqr_test.yaml"

KEY_COLS = ("sid", "date", "endtime")


def _run(cmd: list[str]) -> None:
    print(f"+ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def _load_stream(path: Path):
    import pyarrow as pa

    with pa.ipc.open_stream(pa.memory_map(str(path), "r")) as reader:
        batches = list(reader)
    return pa.Table.from_batches(batches)


def _load_batch(path: Path):
    import pyarrow.parquet as pq

    return pq.read_table(path)


def _fmt_endtime(t: int) -> str:
    h, rem = divmod(t, 3_600_000_000_000)
    m, rem = divmod(rem, 60_000_000_000)
    s, _ = divmod(rem, 1_000_000_000)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _cell_exact(a, b) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    return a == b


def compare(stream_rows: list[dict], batch_rows: list[dict], leaves: list[str]) -> dict:
    """Cellwise comparison on rows sharing (sid, endtime). Returns the report."""
    by_key = {(r["sid"], r["endtime"]): r for r in batch_rows}
    mismatched_cells = 0
    cells = 0
    rows_compared = 0
    rows_with_mismatch = 0
    per_endtime = {}  # endtime → [rows compared, rows with ≥1 mismatch]
    per_column = {}  # leaf → mismatched cells
    for r in stream_rows:
        other = by_key.get((r["sid"], r["endtime"]))
        if other is None:
            continue
        rows_compared += 1
        row_mismatches = 0
        for leaf in leaves:
            cells += 1
            if not _cell_exact(r[leaf], other[leaf]):
                mismatched_cells += 1
                row_mismatches += 1
                per_column[leaf] = per_column.get(leaf, 0) + 1
        n, k = per_endtime.get(r["endtime"], (0, 0))
        per_endtime[r["endtime"]] = (n + 1, k + (1 if row_mismatches else 0))
        if row_mismatches:
            rows_with_mismatch += 1
    pct = 100.0 * (cells - mismatched_cells) / cells if cells else float("nan")
    return {
        "rows_compared": rows_compared,
        "leaves": len(leaves),
        "cells": cells,
        "mismatched_cells": mismatched_cells,
        "pct_exact": round(pct, 4),
        "rows_with_mismatch": rows_with_mismatch,
        "per_endtime": {t: round(100.0 * k / n, 2) for t, (n, k) in per_endtime.items()},
        "top_columns": sorted(per_column.items(), key=lambda kv: -kv[1])[:20],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="gate_stream_vs_batch")
    p.add_argument("--date", default="20220104")
    p.add_argument("--stocks", type=int, default=3, help="stream universe size")
    p.add_argument("--freq", default="3m")
    p.add_argument("--bar-dtype", default="float32", choices=["float32", "float64"])
    p.add_argument("--cn-rst3", default=CN_RST3)
    p.add_argument("--data-dir", default=DATA_DIR)
    p.add_argument("--graph", default=GRAPH)
    p.add_argument("--config", default=CONFIG, help="batch `bars` runner YAML")
    p.add_argument("--output", default=None, help="work dir (default: fresh tmp)")
    p.add_argument("--keep", action="store_true", help="do not delete the work dir")
    p.add_argument("--expect-exact", action="store_true", help="exit 1 unless 100.0000%")
    p.add_argument("--json", default=None, help="write the report dict to this path")
    args = p.parse_args(argv)

    tmp = Path(args.output) if args.output else Path(tempfile.mkdtemp(prefix="gate_vs_batch_"))
    try:
        _run(
            [
                args.cn_rst3,
                "bars",
                "--config",
                args.config,
                "--date",
                args.date,
                "--graph",
                args.graph,
                "--freq",
                args.freq,
                "--bar-dtype",
                args.bar_dtype,
                "--max-stocks",
                str(args.stocks),
                "--output",
                str(tmp / "bars"),
                "--version",
                "gate",
            ]
        )
        batch_path = tmp / "bars" / "gate" / args.freq / f"{args.date}.parquet"
        _run(
            [
                args.cn_rst3,
                "stream-real",
                "--date",
                args.date,
                "--data-dir",
                args.data_dir,
                "--stocks",
                str(args.stocks),
                "--graph",
                args.graph,
                "--freq",
                args.freq,
                "--bar-dtype",
                args.bar_dtype,
                "--bars-ipc",
                str(tmp / "stream.ipc"),
            ]
        )

        stream_table = _load_stream(tmp / "stream.ipc")
        batch_table = _load_batch(batch_path)
        leaves = [c for c in stream_table.column_names if c not in KEY_COLS]
        stream_rows = stream_table.to_pylist()
        stream_sids = {r["sid"] for r in stream_rows}
        batch_rows = [r for r in batch_table.to_pylist() if r["sid"] in stream_sids]

        report = compare(stream_rows, batch_rows, leaves)
        report.update(
            {
                "date": args.date,
                "freq": args.freq,
                "bar_dtype": args.bar_dtype,
                "stream_rows": len(stream_rows),
                "batch_rows_in_universe": len(batch_rows),
                "universe": sorted(str(s) for s in stream_sids),
            }
        )
        if args.json:
            Path(args.json).write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(f"gate stream-vs-batch: date={args.date} freq={args.freq} "
              f"bar_dtype={args.bar_dtype} stocks={args.stocks}")
        print(f"  stream rows: {report['stream_rows']}  batch rows in universe: "
              f"{report['batch_rows_in_universe']}  compared: {report['rows_compared']}")
        print(f"  cells: {report['cells']}  mismatched: {report['mismatched_cells']}  "
              f"exact: {report['pct_exact']}%  rows with any mismatch: "
              f"{report['rows_with_mismatch']}")
        worst = sorted(report["per_endtime"].items(), key=lambda kv: -kv[1])[:5]
        print("  worst windows (endtime → % rows mismatched): "
              + ", ".join(f"{_fmt_endtime(t)} {v}%" for t, v in worst))
        print("  top mismatched leaves: "
              + ", ".join(f"{col} {n}" for col, n in report["top_columns"][:10]))
        if args.expect_exact and report["pct_exact"] < 100.0:
            print(f"GATE FAIL: {report['pct_exact']}% < 100%")
            return 1
        return 0
    finally:
        if not args.keep and args.output is None:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
