"""Pluggable per-bar signal.

Interface: `compute_signal(bars, sym)` — `bars` maps md_symbol → the latest
closed bar per instrument (cross-section), `sym` is the instrument whose bar
just closed. Returns a SignalRow or None. A real model later implements the
same signature (config `signal.module` / `signal.function`).

Placement in the pipeline (per user direction, 2026-08-27): cn_rst3
`stream-real --graph --freq 3m` PROVIDES the 3m bar stream; the bars then
pass downstream to lp (bars → features) and cc (features → yhat). Those
stages are not wired here yet — the standing simplification is:

    **the bar's mean IS the alpha** — alpha = mean of the bar's leaf values
    (NaN excluded). Replace this function with the lp/cc chain when it lands.
"""

import math

from .engine import Bar

SignalRow = dict  # {"score": float, "direction": int, "meta": str}


def compute_signal(bars: dict[str, Bar], sym: str) -> SignalRow | None:
    """Alpha = mean of the just-closed bar's leaf values (placeholder for
    the lp → cc feature/model chain; NaN leaves are excluded)."""
    bar = bars.get(sym)
    if bar is None:
        return None
    vals = [v for v in bar.leaves.values() if not math.isnan(v)]
    if not vals:
        return None
    alpha = float(sum(vals) / len(vals))
    direction = 1 if alpha > 0 else -1 if alpha < 0 else 0
    return {"score": alpha, "direction": direction, "meta": "bars-mean"}
