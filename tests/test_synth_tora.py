"""The full-fidelity dry run on real past data: research place/trade +
tick_table → TORA-shaped capture → normalize → convert → profile vs the
research tick_table.

This is the answer to "can we test the API against past data aligned with
place and trade": yes — the research tape re-dressed as a TORA capture
reproduces the research tick_table at **98.4% cell-exact** across the
statelessly-representable rows (adds/cancels/auction fills, 21,719 rows),
with event codes 99.9% exact, sign 99.9%, volumes 99.9%. Residual deltas
are the documented structural gaps (tick_row_spec.md): level_placed beyond
the visible 10 levels, book-stamp timing at fan-out instants, and
continuous trades (Rust mutates the aggressor's Add row — not statelessly
representable; needs the per-order row-mutation port).

Assertions are sanity thresholds, not the gate: the gate itself is the
printed profile.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from alignment_ob.converters import TICK_TABLE_COLUMNS, events_to_tick_table
from alignment_ob.normalize import normalize_capture
from alignment_ob.profiler import compare_tapes
from alignment_ob.synth_tora import _hhmmssmmm, _stat_of

DATA = Path("/mnt/disks/data/cn")
EPOCH = 1641225600000000000

needs_data = pytest.mark.skipif(
    not (
        (DATA / "raw/parquet/place/20220104/sh600000.parquet").exists()
        and (DATA / "raw/parquet/trade/20220104/sh600000.parquet").exists()
        and (DATA / "tick_table/20220104/sh600000.parquet").exists()
    ),
    reason="research place/trade/tick_table not present",
)


def _synth_capture(path: Path) -> Path:
    place = pq.read_table(DATA / "raw/parquet/place/20220104/sh600000.parquet").to_pylist()
    trade = pq.read_table(DATA / "raw/parquet/trade/20220104/sh600000.parquet").to_pylist()
    ticks = pq.read_table(DATA / "tick_table/20220104/sh600000.parquet").to_pylist()
    sec, exch = "600000", "1"
    lines = []
    for r in place:
        t = r["OrderTime"] - EPOCH
        lines.append((t, "od", {
            "type": "od", "SecurityID": sec, "ExchangeID": exch,
            "OrderTime": _hhmmssmmm(t), "Price": r["Price"], "Volume": r["Qty"],
            "Side": "1" if r["BSFlag"] == 1 else "2", "OrderType": "2",
            "MainSeq": r["OrderIndex"], "SubSeq": 0, "OrderNO": r["OrderID"],
            "OrderStatus": "A" if r["OrdType"] == 0 else "D", "BizIndex": r["BizIndex"],
        }))
    for r in trade:
        t = r["TradeTime"] - EPOCH
        if _stat_of(t) == "2":
            continue  # continuous fills: mutation-only, not stateless
        lines.append((t, "tr", {
            "type": "tr", "SecurityID": sec, "ExchangeID": exch,
            "TradeTime": _hhmmssmmm(t), "TradePrice": r["Price"], "TradeVolume": r["Qty"],
            "ExecType": "1", "MainSeq": r["BizIndex"], "SubSeq": 0,
            "BuyNo": r["BuyOrderID"], "SellNo": r["SellOrderID"],
            "TradeBSFlag": "N" if r["BSFlag"] == 0 else "B" if r["BSFlag"] == 1 else "S",
            "BizIndex": r["BizIndex"],
        }))
    for r in ticks:
        t = r["exch_ts"]  # the vendor book exists at exchange time
        md = {
            "type": "md", "SecurityID": sec, "ExchangeID": exch,
            "DataTimeStamp": _hhmmssmmm(t), "LastPrice": r["last_price"] / 100.0,
            "TotalVolumeTrade": 0, "NumTrades": 0, "MDSecurityStat": _stat_of(t),
        }
        for i in range(1, 11):
            md[f"BidPrice{i}"] = r[f"bid_price_{i-1}"] / 100.0
            md[f"BidVolume{i}"] = r[f"bid_qty_{i-1}"]
            md[f"Bid{i}NumOrders"] = r[f"bid_orders_{i-1}"]
            md[f"AskPrice{i}"] = r[f"ask_price_{i-1}"] / 100.0
            md[f"AskVolume{i}"] = r[f"ask_qty_{i-1}"]
            md[f"Ask{i}NumOrders"] = r[f"ask_orders_{i-1}"]
        lines.append((t, "md", md))
    lines.sort(key=lambda e: (e[0] // 10**6, 0 if e[1] == "md" else 1))
    path.write_text("".join(json.dumps(l) + "\n" for _, _, l in lines))
    return path


@needs_data
def test_synth_tora_dry_run_profiles_high(tmp_path):
    cap = _synth_capture(tmp_path / "cap.jsonl")
    events = normalize_capture(cap, 20220104)
    table = events_to_tick_table(events, sid=2_600_000)
    rows_a = table.to_pylist()
    for r in rows_a:
        r["_xms"] = r["exch_ts"] // 10**6
    ticks = pq.read_table(DATA / "tick_table/20220104/sh600000.parquet").to_pylist()
    rows_b = [dict(r, _xms=r["exch_ts"] // 10**6) for r in ticks]
    keys = ("_xms", "price", "qty")
    skip = {
        "local_ts", "order_seq", "order_id", "order_lifetime", "tvr",
        "depth_traded", "count_traded", "volume_ahead", "orders_ahead",
        "volume_better", "orders_better", "volume_behind", "vwap_better", "exch_ts",
    }
    rep = compare_tapes(
        rows_a, rows_b, keys, [c for c in TICK_TABLE_COLUMNS if c not in keys and c not in skip]
    )
    # the printed profile is the gate; these are sanity thresholds:
    assert rep.compared >= 21_000
    assert rep.pct_exact >= 98.0
    assert rep.per_column.get("event", 0) <= 100  # 25 on 20220104
    assert rep.per_column.get("sign", 0) <= 50  # 15
    assert rep.per_column.get("vol_placed", 0) + rep.per_column.get("vol_traded", 0) <= 400
