"""alignment_ob tests: M2 converters + M3 profiler, offline against real data.

The headline: the research tick_table round-trips through the capture → event
→ converter loop **bit-faithfully** — which is exactly the property the TORA
live capture must reproduce on a China host for the M3 gate to pass.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from alignment_ob.converters import (
    events_to_place_rows,
    events_to_tick_table,
    events_to_trade_rows,
)
from alignment_ob.profiler import compare_tapes
from live.events import Event

TICK_TABLE_DIR = "/mnt/disks/data/cn/tick_table"
DATE, SID = "20220104", "sh600000"

needs_data = pytest.mark.skipif(
    not Path(f"{TICK_TABLE_DIR}/{DATE}/{SID}.parquet").exists(),
    reason="research tick_table not present",
)


def _replay_events(date=DATE, sid=SID):
    from live.source import ReplaySource

    src = ReplaySource(engine=None, outbox=None, replay_dir=TICK_TABLE_DIR, dates=[date])
    return list(src._parquet_events(SimpleNamespace(md_symbol=sid), date))


def _ev(**kw):
    base = {
        "instrument": SID,
        "t_ns": 34200000000000,
        "trading_day": 20220104,
        "event": 0,
        "sign": 1,
        "price": 830,
        "vol_placed": 100,
    }
    return Event(**(base | kw))


@needs_data
def test_tick_table_roundtrip_bit_faithful():
    import pyarrow.parquet as pq

    events = _replay_events()
    assert len(events) == 30_598  # full research day for sh600000
    table = events_to_tick_table(events, sid=2_600_000)
    original = pq.read_table(f"{TICK_TABLE_DIR}/{DATE}/{SID}.parquet")

    assert table.column_names == original.column_names  # canonical 83-column order
    report = compare_tapes(
        table.to_pylist(),
        original.to_pylist(),
        keys=("local_ts", "order_id", "order_seq"),
        value_cols=[c for c in original.column_names if c not in ("local_ts", "order_id", "order_seq")],
    )
    assert report.compared == len(events)
    assert report.mismatched_cells == 0
    assert report.pct_exact == 100.0
    assert report.missing_columns == []


@needs_data
def test_capture_dump_roundtrip(tmp_path):
    from alignment_ob.capture import replay_capture

    n = replay_capture(TICK_TABLE_DIR, DATE, [SID], out_dir=tmp_path)
    assert n == 30_598
    dumped = (tmp_path / DATE / f"{SID}.events.jsonl").read_text().splitlines()
    assert len(dumped) == n
    assert "extra" in dumped[0] and '"event"' in dumped[0]


def test_place_rows_add_and_cancel():
    add = _ev(event=0, price=830, vol_placed=100, sign=1, extra={"order_id": 54})
    cancel = _ev(event=2, price=830, vol_placed=100, sign=-1, extra={"order_id": 54})
    rows = events_to_place_rows([add, cancel])
    assert len(rows) == 2
    assert rows[0]["SecurityID"] == 2_600_000
    assert rows[0]["Price"] == 8.3  # cents → yuan
    assert rows[0]["Qty"] == 100.0
    assert rows[0]["OrdType"] == 0 and rows[1]["OrdType"] == -1  # add / cancel
    # OrderTime = CST-midnight epoch + t_ns (09:30:00)
    assert rows[0]["OrderTime"] == 1641225600000000000 + 34200000000000
    assert rows[0]["TradeDate"] == 20220104
    assert rows[1]["OrderID"] == 54


def test_trade_rows_only_trade_events():
    trade = _ev(event=4, price=854, vol_traded=100, sign=1)
    rows = events_to_trade_rows([trade, _ev(event=0), _ev(event=2)])
    assert len(rows) == 1
    assert rows[0]["Price"] == 8.54 and rows[0]["Qty"] == 100.0
    assert rows[0]["TradeTime"] == 1641225600000000000 + 34200000000000


def test_events_to_tick_table_zeros_missing_extra():
    table = events_to_tick_table([_ev()], sid=2_600_000)
    row = table.to_pylist()[0]
    assert row["event"] == 0 and row["price"] == 830 and row["sid"] == 2_600_000
    assert row["qty"] == 0  # not provided by the synthetic event → 0, profiler flags it


def test_profiler_self_compare_and_mutation():
    base = [{"t": 1, "a": 1.0, "b": float("nan"), "c": 5}, {"t": 2, "a": 2.0, "b": 3.0, "c": 6}]
    same = compare_tapes(base, [dict(r) for r in base], ("t",), ["a", "b", "c"])
    assert same.pct_exact == 100.0 and same.mismatched_cells == 0
    assert same.missing_a == 0 and same.missing_b == 0

    mutated = [dict(base[0], a=1.5), dict(base[1])]
    rep = compare_tapes(mutated, base, ("t",), ["a", "b", "c"])
    assert rep.mismatched_cells == 1 and rep.per_column == {"a": 1}
    assert rep.rows_with_mismatch == 1
    # missing row on side b / missing column on side a
    rep2 = compare_tapes(base, [base[0]], ("t",), ["a", "b", "c"])
    assert rep2.missing_b == 1
    rep3 = compare_tapes(
        base, [{k: v for k, v in r.items() if k != "a"} for r in base], ("t",), ["a", "b", "c"]
    )
    assert rep3.missing_columns == ["a"]
    # None on side a vs value on side b → mismatch (NaN==None only when both)
    rep4 = compare_tapes([dict(r, a=None) for r in base], base, ("t",), ["a", "b", "c"])
    assert rep4.missing_columns == [] and rep4.per_column == {"a": 2}
