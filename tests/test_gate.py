"""Gate test: scripts/gate_stream_vs_batch.py runs and reports (skipif-guarded).

The gate compares the cn_rst3 streaming path against the batch `bars` path
cellwise. The test verifies the gate machinery end to end; it deliberately
does NOT assert 100.0% — that is the standing gate itself, rerun manually
with --expect-exact (pending the obgx replay-state port).
"""

import json
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PY = REPO / ".venv-live" / "bin" / "python"
SCRIPT = REPO / "scripts" / "gate_stream_vs_batch.py"
CN_RST3 = "/mnt/disks/data/cargo-target/release/cn_rst3"
DATA_DIR = "/mnt/disks/data/cn/raw/parquet"
GRAPH = "/home/george/code/alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml"
CONFIG = "/home/george/code/obgx/study/alpha/run_config_cqr_test.yaml"

needs_gate = pytest.mark.skipif(
    not (
        Path(CN_RST3).is_file()
        and Path(DATA_DIR).is_dir()
        and Path(GRAPH).is_file()
        and Path(CONFIG).is_file()
    ),
    reason="cn_rst3 binary / data / graph / runner config not present",
)


@needs_gate
def test_gate_runs_and_reports(tmp_path):
    report_path = tmp_path / "gate_report.json"
    proc = subprocess.run(
        [str(PY), str(SCRIPT), "--stocks", "3", "--json", str(report_path)],
        capture_output=True,
        text=True,
        timeout=600,
        check=True,
    )
    report = json.loads(report_path.read_text())
    # at least one full stock × 81 windows compared; 252 leaves each
    assert report["rows_compared"] >= 81
    assert report["cells"] == report["rows_compared"] * 252
    assert report["leaves"] == 252
    assert report["per_endtime"]  # per-window rates present
    assert "exact:" in proc.stdout
