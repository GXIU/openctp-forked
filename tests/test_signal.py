import math

from live.engine import Bar
from live.signal import compute_signal


def _bar(sym="sh600000", date=20220104, endtime=1, **leaves):
    return Bar(sid=2_600_000, date=date, endtime=endtime, leaves=leaves)


def test_alpha_is_bar_leaves_mean():
    leaves = {"n_trade": 10.0, "qty_sum": 2.0, "ratio": 0.0, "nan_leaf": math.nan}
    s = compute_signal({"x": _bar(sym="x", **leaves)}, "x")
    # NaN excluded: (10 + 2 + 0) / 3
    assert s["score"] == 4.0
    assert s["meta"] == "bars-mean"
    assert s["direction"] == 1


def test_deterministic():
    leaves = {"a": 1.0, "b": 2.0}
    a = compute_signal({"x": _bar(sym="x", **leaves)}, "x")
    b = compute_signal({"x": _bar(sym="x", **leaves)}, "x")
    assert a == b


def test_negative_mean_direction():
    s = compute_signal({"x": _bar(sym="x", a=-5.0, b=1.0)}, "x")
    assert s["score"] == -2.0
    assert s["direction"] == -1


def test_all_nan_bar_returns_none():
    assert compute_signal({"x": _bar(sym="x", a=math.nan)}, "x") is None


def test_unknown_symbol_returns_none():
    assert compute_signal({"sh600000": _bar()}, "sz000001") is None
