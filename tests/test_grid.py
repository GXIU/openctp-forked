"""Grid tests — the 81-endtime CN 3m grid must match cn_rst3 exactly."""

from itertools import pairwise

import pytest

from live.grid import endtimes_3m_cn, make_grid, ns_of


def test_endtimes_exact_81():
    et = endtimes_3m_cn()
    assert len(et) == 81
    assert et[0] == ns_of(9, 30)
    assert et[40] == ns_of(11, 30)
    assert et[41] == ns_of(13, 3)
    assert et[79] == ns_of(14, 57)
    assert et[80] == ns_of(15, 0)
    # 13:00 must be skipped, morning pins at 09:30
    assert ns_of(13, 0) not in et
    assert et == sorted(et)
    morning = et[:41]
    assert all(b - a == 3 * 60 * 1_000_000_000 for a, b in pairwise(morning))


def test_window_assignment_partition_point():
    """streaming_bars.rs BarGrid::window_index: [start, end) windows, clamp."""
    g = make_grid(3)
    # tick at exactly an endtime → the NEXT window
    assert g.window_index(ns_of(9, 30)) == 1
    # one nanosecond before → still the 09:30 window
    assert g.window_index(ns_of(9, 30) - 1) == 0
    # auction fold-in: (09:15, 09:30]
    assert g.window_index(ns_of(9, 15) + 1) == 0
    assert g.window_index(ns_of(9, 25)) == 0


def test_lunch_gap_attaches_to_first_afternoon_bar():
    """Lunch-gap timestamps attach to the first afternoon bar (13:03)."""
    g = make_grid(3)
    assert g.window_index(ns_of(11, 30) + 1) == 41
    assert g.window_index(ns_of(12, 0)) == 41
    assert g.window_index(ns_of(13, 0)) == 41  # 13:00 <= 13:00 → skipped → 13:03


def test_close_auction_clamp_unconditional():
    """Ticks at/after 15:00 clamp into the final bar (late stragglers)."""
    g = make_grid(3)
    assert g.window_index(ns_of(15, 0)) == 80
    assert g.window_index(ns_of(15, 0, ms=100)) == 80
    assert g.window_index(ns_of(15, 2)) == 80  # unconditional clamp


def test_other_freqs_rejected():
    with pytest.raises(ValueError):
        make_grid(5)
