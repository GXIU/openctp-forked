"""Engine tests: windowing, bit-faithful graph evaluation, rollover."""

import math
from pathlib import Path

import numpy as np
import pytest

from live.engine import BarEngine
from live.events import Event
from live.grid import make_grid, ns_of
from live.instruments import normalize_instrument

from .helpers import ev

ALFA = Path("/home/george/code/alfa")
TICK_TABLE = ALFA / "cn/tick-table/20220104/sh603019.parquet"
BAR_YAML = ALFA / "cn/yaml/spd/cqr/v260823/cqr_bar.yaml"

FIELDS = [
    "ask_price_0",
    "bid_price_0",
    "event",
    "last_price",
    "level_placed",
    "price",
    "sign",
    "vol_placed",
    "vol_traded",
]

needs_alfa = pytest.mark.skipif(
    not TICK_TABLE.exists() or not BAR_YAML.exists(),
    reason="alfa tick_table / bar YAML not present",
)


@pytest.fixture(scope="session")
def engine():
    return BarEngine(
        [normalize_instrument("sh603019")],
        make_grid(3),
        str(BAR_YAML),
    )


@needs_alfa
def test_full_day_replay_81_bars_bit_faithful(engine):
    """Full-day tick_table through the engine: 81 bars, and window 0's leaves
    match a direct graph.run on the same window slice exactly."""
    import pyarrow.parquet as pq

    df = pq.read_table(str(TICK_TABLE)).to_pandas()
    bars: list = []
    engine.set_trading_day(20220104)
    for i in range(len(df)):
        r = df.iloc[i]
        bars += engine.on_event(
            Event(
                instrument="sh603019",
                t_ns=int(r["local_ts"]),
                trading_day=20220104,
                event=int(r["event"]),
                sign=int(r["sign"]),
                price=float(r["price"]),
                last_price=float(r["last_price"]),
                vol_placed=float(r["vol_placed"]),
                vol_traded=float(r["vol_traded"]),
                level_placed=float(r["level_placed"]),
                bid_price_0=float(r["bid_price_0"]),
                ask_price_0=float(r["ask_price_0"]),
                has_book=True,
            )
        )
    bars += engine.flush()

    assert len(bars) == 81
    assert [b.endtime for b in bars] == sorted(b.endtime for b in bars)
    assert bars[0].endtime == ns_of(9, 30)
    assert bars[-1].endtime == ns_of(15, 0)

    # reference: graph.run on the same window slice (09:15, 09:30]
    w = df[(df.local_ts > 33_300_000_000_000) & (df.local_ts <= 34_200_000_000_000)][
        FIELDS
    ].astype("float64")
    ref = engine.graph.run(w, min_samples=0)
    assert len(ref) == len(engine.leaf_names) == 252
    for name in engine.leaf_names:
        a = np.asarray(ref[name]).ravel()[0]
        b = bars[0].leaves[name]
        if math.isnan(a):
            assert math.isnan(b), name
        else:
            assert a == pytest.approx(b, rel=1e-12), name


@needs_alfa
def test_empty_windows_emitted_and_skipped():
    eng_on = BarEngine(
        [normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML), emit_empty_windows=True
    )
    eng_off = BarEngine(
        [normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML), emit_empty_windows=False
    )
    eng_on.set_trading_day(20220104)
    eng_off.set_trading_day(20220104)
    # tick in the auction window, then jump to 09:40 (window 4 = 09:42)
    total_on, total_off = [], []
    for eng, total in ((eng_on, total_on), (eng_off, total_off)):
        total += eng.on_event(ev((9, 15, 30)))
        total += eng.on_event(ev((9, 40, 0)))
        total += eng.flush()
    # windows 1..3 (09:33/09:36/09:39) close empty in between:
    # emit_empty_windows=True → 81 bars; False → only the two non-empty bars
    assert len(total_on) == 81
    assert len(total_off) == 2
    empty = [b for b in total_on if b.endtime == ns_of(9, 33)]
    assert empty and all(math.isnan(v) for v in empty[0].leaves.values())


@needs_alfa
def test_book_stamping_non_book_events():
    eng = BarEngine([normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML))
    eng.set_trading_day(20220104)
    # book event first: establishes last_book
    eng.on_event(ev((9, 16, 0), has_book=True, bid=990.0, ask=1010.0))
    # non-book trade event: book stamped from last_book
    bars = eng.on_event(ev((9, 16, 1), has_book=False, event=1, sign=1, vol_traded=100.0))
    assert bars == []  # same window, no close
    bars = eng.flush()
    assert len(bars) == 81
    # the 09:30 bar's window rows used stamped book — n_trade must see the trade
    first = bars[0]
    assert first.leaves["n_trade"] == 1.0


@needs_alfa
def test_rollover_trading_day_change():
    eng = BarEngine([normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML))
    eng.set_trading_day(20220104)
    eng.on_event(ev((9, 16, 0), day=20220104))
    bars = eng.on_event(ev((9, 16, 0), day=20220105))
    # rollover closed the whole 20220104 day (81 bars), then the new tick opens 20220105
    assert len(bars) == 81
    assert all(b.date == 20220104 for b in bars)
    bars2 = eng.flush()
    assert all(b.date == 20220105 for b in bars2)
    assert len(bars2) == 81


@needs_alfa
def test_rollover_cum_volume_reset():
    eng = BarEngine([normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML))
    eng.set_trading_day(20220104)
    eng.on_event(ev((9, 16, 0), event=1, vol_traded=100.0, cum_volume=1000.0))
    bars = eng.on_event(ev((9, 17, 0), event=1, vol_traded=100.0, cum_volume=50.0))
    assert len(bars) == 81  # volume reset → new day (same date, loop restart)
    assert bars[0].date == 20220104


@needs_alfa
def test_rollover_time_wrap():
    eng = BarEngine([normalize_instrument("sh600000")], make_grid(3), str(BAR_YAML))
    eng.set_trading_day(20220104)
    eng.on_event(ev((14, 59, 0)))  # lands in the 15:00 window (index 80)
    bars = eng.on_event(ev((9, 15, 1)))  # 7x24 loop restart: time wraps backwards
    # the interrupted day closes only its reached windows (80..81 → 1 bar);
    # windows before the first tick of a day are never emitted (mid-day attach)
    assert len(bars) == 1
    assert bars[0].endtime == ns_of(15, 0)
    # the new loop's day flushes through all 81 windows
    assert len(eng.flush()) == 81
