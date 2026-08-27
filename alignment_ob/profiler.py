"""M3 alignment profiler: TORA-sourced tape vs the research tape.

Same idea as scripts/gate_stream_vs_batch.py, one level lower: before any
bars are computed, compare the two tick_table-level tapes cellwise so every
field-level delta (resend dedupe, aggressor sign, book stamps, missing
columns) is attributed to its column instead of being smeared across leaves.

Cell-exactness matches the gate: NaN (and None) equals NaN; everything else
must compare equal bitwise.
"""

import math
from dataclasses import dataclass, field
from pathlib import Path


def _cell_exact(a, b) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    return a == b


@dataclass
class TapeReport:
    rows_a: int = 0
    rows_b: int = 0
    compared: int = 0
    missing_a: int = 0  # keys in b but not a
    missing_b: int = 0  # keys in a but not b
    cells: int = 0
    mismatched_cells: int = 0
    rows_with_mismatch: int = 0
    per_column: dict = field(default_factory=dict)
    missing_columns: list = field(default_factory=list)

    @property
    def pct_exact(self) -> float:
        return 100.0 * (self.cells - self.mismatched_cells) / self.cells if self.cells else 0.0

    def summary(self) -> str:
        lines = [
            (
                f"tape profile: rows a={self.rows_a} b={self.rows_b} "
                f"compared={self.compared} (missing a={self.missing_a} b={self.missing_b})"
            ),
            (
                f"  cells={self.cells} mismatched={self.mismatched_cells} "
                f"exact={self.pct_exact:.4f}% rows_with_mismatch={self.rows_with_mismatch}"
            ),
        ]
        if self.missing_columns:
            lines.append(f"  columns in b missing from a: {self.missing_columns}")
        if self.per_column:
            top = sorted(self.per_column.items(), key=lambda kv: -kv[1])[:10]
            lines.append("  top mismatched columns: " + ", ".join(f"{c} {n}" for c, n in top))
        return "\n".join(lines)


def compare_tapes(
    rows_a: list[dict], rows_b: list[dict], keys: tuple[str, ...], value_cols: list[str]
) -> TapeReport:
    """Cellwise comparison aligned on `keys` (inner join).

    rows_a = the tape under test (e.g. TORA-derived), rows_b = the reference
    (research tick_table). Value columns missing on EITHER side are reported
    as missing and skipped (a converted table would have them all-zero — the
    converter must fill them for a clean profile).
    """
    report = TapeReport(rows_a=len(rows_a), rows_b=len(rows_b))
    by_key = {tuple(r[k] for k in keys): r for r in rows_b}
    report.missing_columns = sorted(
        c
        for c in value_cols
        if any(c not in r for r in rows_a) or any(c not in r for r in rows_b)
    )
    cols = [c for c in value_cols if c not in report.missing_columns]
    seen = set()
    for r in rows_a:
        key = tuple(r[k] for k in keys)
        if key in seen:
            continue  # duplicate key on side a — ambiguous, skip
        seen.add(key)
        other = by_key.get(key)
        if other is None:
            report.missing_b += 1
            continue
        report.compared += 1
        row_mismatches = 0
        for col in cols:
            report.cells += 1
            if not _cell_exact(r[col], other[col]):
                report.mismatched_cells += 1
                row_mismatches += 1
                report.per_column[col] = report.per_column.get(col, 0) + 1
        if row_mismatches:
            report.rows_with_mismatch += 1
    report.missing_a = len(by_key) - report.compared
    return report


def profile_tape_dirs(
    dir_a: Path, dir_b: Path, date: str, sids: list[str], keys: tuple[str, ...]
) -> TapeReport:
    """Compare two tick_table trees ({dir}/{date}/{sid}.parquet) per sid.

    Accumulates one TapeReport across sids (rows_b counts the reference rows
    actually compared)."""
    import pyarrow.parquet as pq

    total = TapeReport()
    for sid in sids:
        pa_, pb = dir_a / date / f"{sid}.parquet", dir_b / date / f"{sid}.parquet"
        if not pa_.exists() or not pb.exists():
            continue
        ta, tb = pq.read_table(pa_).to_pylist(), pq.read_table(pb).to_pylist()
        value_cols = [c for c in tb[0] if c not in keys]
        rep = compare_tapes(ta, tb, keys, value_cols)
        for f in ("rows_a", "rows_b", "compared", "missing_a", "missing_b",
                  "cells", "mismatched_cells", "rows_with_mismatch"):
            setattr(total, f, getattr(total, f) + getattr(rep, f))
        for col, n in rep.per_column.items():
            total.per_column[col] = total.per_column.get(col, 0) + n
        total.missing_columns.extend(rep.missing_columns)
    total.missing_columns = sorted(set(total.missing_columns))
    return total
