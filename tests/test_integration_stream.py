"""Integration test: the full live pipeline runs TOGETHER end to end.

Production Rust stream (cn_rst3 stream-real --graph --freq 3m) piped into
live.ipc_consumer with the bars-mean alpha — one process chain, real data.
Skipped when the binary / data / graph are not available on the host.
"""

import json
import subprocess
from pathlib import Path

import pytest

CN_RST3 = "/mnt/disks/data/cargo-target/release/cn_rst3"
DATA_DIR = "/mnt/disks/data/cn/raw/parquet"
GRAPH = "/home/george/code/alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml"
REPO = Path(__file__).resolve().parents[1]

needs_chain = pytest.mark.skipif(
    not (Path(CN_RST3).is_file() and Path(DATA_DIR).is_dir() and Path(GRAPH).is_file()),
    reason="cn_rst3 binary / place-trade parquet / graph not present",
)


@needs_chain
def test_full_pipeline_runs_together(tmp_path):
    out = tmp_path / "signals.jsonl"
    cmd = (
        f"{CN_RST3} stream-real --date 20220104 --data-dir {DATA_DIR} --stocks 3 "
        f"--graph {GRAPH} --freq 3m --bars-stdout-ipc 2>/dev/null "
        f"| PYTHONPATH={REPO} {REPO}/.venv-live/bin/python -m live.ipc_consumer "
        f"--stdin --jsonl {out}"
    )
    subprocess.run(cmd, shell=True, check=True, timeout=600)

    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert len(rows) == 3 * 81  # 3 stocks × 81 windows
    leaves = [k for k in rows[0] if k not in ("sid", "date", "endtime", "signal")]
    assert len(leaves) == 252
    assert all(r["signal"]["meta"] == "bars-mean" for r in rows)
    assert all("score" in r["signal"] for r in rows)
    # one stock's bars are ordered by endtime and unique
    by_sid = {}
    for r in rows:
        by_sid.setdefault(r["sid"], []).append(r["endtime"])
    for ends in by_sid.values():
        assert ends == sorted(ends) and len(set(ends)) == 81
