"""M2 converters: normalized events → research storage formats.

Two outputs (cross-checking paths, see docs/tora_onramp.md):

1. **tick_table** — the research 逐笔 stream (83 int64 columns, cents/shares).
   The round-trip `tick_table → events → tick_table` is bit-faithful by
   construction (Event carries every column: core fields + `extra`) and is
   pinned by tests/test_alignment.py against the real research file.

2. **place/ + trade/ parquet** — the obgx raw format (yuan prices, ns-since-
   epoch times). Grounded on the real 20220104 files:
   - place OrdType: 0 = add, -1 = cancel (cross-validated: the -1 count
     equals the tick_table event-2 cancel count exactly);
   - prices/volumes in YUAN/SHARES here (tick_table is cents);
   - OrderTime/TradeTime = CST-midnight epoch ns + t_ns.

Field mappings not yet verifiable offline (BSFlag side codes, ChannelNo,
BizIndex, BuyOrderID/SellOrderID attribution) default to documented guesses
and are the M1/M3 alignment checklist — the profiler flags every delta.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from live.events import CANCEL_EVENT, TRADE_EVENTS, Event

if TYPE_CHECKING:
    import pyarrow as pa

CST = ZoneInfo("Asia/Shanghai")

#: canonical tick_table column order (the research stream schema)
TICK_TABLE_COLUMNS = [
    "price", "qty", "sign", "vol_placed", "vol_traded", "level_placed",
    "event", "order_id", "order_seq", "sid", "order_lifetime", "exch_ts",
    "local_ts", "tvr", "last_price", "depth_traded", "count_traded",
    "volume_ahead", "orders_ahead", "volume_better", "orders_better",
    "volume_behind", "vwap_better",
    # book columns are level-major: price/qty/orders per level 0..9
    *[c for i in range(10) for c in (f"ask_price_{i}", f"ask_qty_{i}", f"ask_orders_{i}")],
    *[c for i in range(10) for c in (f"bid_price_{i}", f"bid_qty_{i}", f"bid_orders_{i}")],
]

#: the single float column in the research schema (all others int64)
TICK_TABLE_FLOATS = {"vwap_better"}

#: Event fields stored on the dataclass itself (the rest ride in `extra`)
_EVENT_CORE = {
    "event", "sign", "price", "last_price", "vol_placed", "vol_traded",
    "level_placed", "bid_price_0", "ask_price_0", "bid_qty_0", "ask_qty_0",
}


@dataclass
class ConvertOptions:
    """Documented-default mappings for fields not yet verifiable offline."""

    sid: int = 2_600_000  # SecurityID / tick_table sid (2_000_000 + code)
    side_map: dict = field(default_factory=lambda: {1: 1, -1: 2})  # sign → BSFlag
    channel_no: int = 5  # real 20220104 files are all channel 5


def sid_of(instrument: str) -> int:
    """md_symbol "sh600000" → 2_600_000 (SecurityID / tick_table sid)."""
    return 2_000_000 + int("".join(ch for ch in instrument if ch.isdigit()))


def epoch_ns(trading_day: int) -> int:
    """CST-midnight of YYYYMMDD in ns since epoch (OrderTime anchor)."""
    y, m, d = trading_day // 10_000, trading_day // 100 % 100, trading_day % 100
    dt = datetime(y, m, d, tzinfo=CST)
    return int(dt.timestamp()) * 1_000_000_000


def events_to_tick_table(events: list[Event], *, sid: int) -> pa.Table:
    """Events → tick_table rows in the canonical 83-column int64 schema.

    Core Event fields map back to their tick_table columns; every other
    column comes from `Event.extra` (ReplaySource fills all of them — the
    round-trip is bit-faithful). Missing extra columns → 0; the M3 profiler
    reports which columns a capture failed to provide.
    """
    import pyarrow as pa

    columns: dict[str, list] = {name: [] for name in TICK_TABLE_COLUMNS}
    for ev in events:
        values = {
            "price": ev.price,
            "sign": ev.sign,
            "vol_placed": ev.vol_placed,
            "vol_traded": ev.vol_traded,
            "level_placed": ev.level_placed,
            "event": ev.event,
            "sid": sid,
            "local_ts": ev.t_ns,
            "last_price": ev.last_price,
            "bid_price_0": ev.bid_price_0,
            "ask_price_0": ev.ask_price_0,
            "bid_qty_0": ev.bid_qty_0,
            "ask_qty_0": ev.ask_qty_0,
            **ev.extra,
        }
        for name in TICK_TABLE_COLUMNS:
            v = values.get(name, 0)
            columns[name].append(v if name in TICK_TABLE_FLOATS else int(v))
    schema = pa.schema(
        [
            (name, pa.float64() if name in TICK_TABLE_FLOATS else pa.int64())
            for name in TICK_TABLE_COLUMNS
        ]
    )
    return pa.Table.from_pydict({name: columns[name] for name in TICK_TABLE_COLUMNS}, schema=schema)


def _order_row(ev: Event, opts: ConvertOptions, *, ord_type: int) -> dict:
    return {
        "SecurityID": opts.sid,
        "TradeDate": ev.trading_day,
        "OrderTime": epoch_ns(ev.trading_day) + ev.t_ns,
        "OrderID": int(ev.extra.get("order_id", 0)),
        "Price": ev.price / 100.0,  # cents → yuan
        "Qty": ev.vol_placed,
        "BSFlag": opts.side_map.get(ev.sign, ev.sign),
        "OrdType": ord_type,
        "OrderIndex": int(ev.extra.get("order_index", 0)),
        "ChannelNo": opts.channel_no,
        "BizIndex": int(ev.extra.get("biz_index", 0)),
    }


def events_to_place_rows(events: list[Event], opts: ConvertOptions | None = None) -> list[dict]:
    """Place (逐笔委托) rows: add (event 0, OrdType 0) and cancel (event 2,
    OrdType -1) — grounded on the real 20220104 place file."""
    opts = opts or ConvertOptions(sid=sid_of(events[0].instrument) if events else 2_600_000)
    rows = []
    for ev in events:
        if ev.event == 0:
            rows.append(_order_row(ev, opts, ord_type=0))
        elif ev.event == CANCEL_EVENT:
            rows.append(_order_row(ev, opts, ord_type=-1))
    return rows


def events_to_trade_rows(events: list[Event], opts: ConvertOptions | None = None) -> list[dict]:
    """Trade (逐笔成交) rows for event 1/4/5. Buyer/seller attribution: the
    event stream carries one order_id per row — the counterpart side is filled
    from extra (buy_order_id/sell_order_id) when a source provides it, else 0."""
    opts = opts or ConvertOptions(sid=sid_of(events[0].instrument) if events else 2_600_000)
    rows = []
    for ev in events:
        if ev.event not in TRADE_EVENTS:
            continue
        rows.append(
            {
                "SecurityID": opts.sid,
                "TradeDate": ev.trading_day,
                "ChannelNo": opts.channel_no,
                "TradeTime": epoch_ns(ev.trading_day) + ev.t_ns,
                "BuyOrderID": int(ev.extra.get("buy_order_id", 0)),
                "SellOrderID": int(ev.extra.get("sell_order_id", 0)),
                "Price": ev.price / 100.0,  # cents → yuan
                "Qty": ev.vol_traded,
                "BSFlag": opts.side_map.get(ev.sign, ev.sign),
                "BizIndex": int(ev.extra.get("biz_index", 0)),
            }
        )
    return rows


PLACE_SCHEMA = {
    "SecurityID": "int32", "TradeDate": "int32", "OrderTime": "int64",
    "OrderID": "int64", "Price": "float64", "Qty": "float64",
    "BSFlag": "int8", "OrdType": "int8", "OrderIndex": "int64",
    "ChannelNo": "int64", "BizIndex": "int64",
}
TRADE_SCHEMA = {
    "SecurityID": "int32", "TradeDate": "int32", "ChannelNo": "int64",
    "TradeTime": "int64", "BuyOrderID": "int64", "SellOrderID": "int64",
    "Price": "float64", "Qty": "float64", "BSFlag": "int8", "BizIndex": "int64",
}


def write_parquet(path: Path, rows: list[dict], schema_cols: dict) -> None:
    """Write rows as parquet in the exact obgx raw schema."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        raise ValueError("no rows to write")
    schema = pa.schema(
        [(name, getattr(pa, t)()) for name, t in schema_cols.items()]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
