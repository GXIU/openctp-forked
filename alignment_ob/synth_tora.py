"""Synthesize a TORA-shaped capture from the RESEARCH tape (place/trade +
tick_table) — the full-fidelity dry run of the live path on real past data.

The live chain is:  TORA L2 (md/tr/od JSONL) → normalize.py → converters.py
→ tick_table.  The research tape IS the same underlying data in another
format: place (逐笔委托) ≈ TORA od, trade (逐笔成交) ≈ TORA tr, and the
reconstructed book on every tick_table row ≈ TORA md snapshots.  So we can
re-dress the research data as a TORA capture and measure exactly how well
the live pipeline would reproduce the research tick_table — before any
network is involved.

Known structural deltas to expect in the profile (quantified by
tests/test_synth_tora.py):
- research tick_table emits one row per executable event (no event-0 place
  rows; trades may fan out per fill) while the TORA od stream carries adds
  too → row-count differences;
- event 5 (SH "continuous trade, secondary code", 71 rows on 20220104) has
  no TORA equivalent → emitted as 1;
- the book-internal columns (order_lifetime, volume_ahead/behind/better,
  orders_*, vwap_better, depth_traded) are computed by cn_rst3's book
  reconstruction and are NOT on any TORA wire field — the normalizer leaves
  them 0; the cqr graph does not read them (its inputs are the 9 core
  fields), so their mismatch does not affect live cqr leaves.
"""

import json
from pathlib import Path

from alignment_ob.normalize import EXCH_SSE, EXCH_SZSE

# Empirical auction windows (sh600000/20220104 tick_table):
# adds 09:15:00→09:24:59.67 and 14:57:00.21→14:59:56.54; the auction fill
# print is a single instant (09:25:00.980 open / 15:00:00.000 close).
AUCTION_OPEN_ADD = (9 * 3600 + 15 * 60, 9 * 3600 + 25 * 60)
AUCTION_OPEN_FILL = (9 * 3600 + 25 * 60, 9 * 3600 + 25 * 60 + 1)
AUCTION_CLOSE_ADD = (14 * 3600 + 57 * 60, 15 * 3600)
AUCTION_CLOSE_FILL = (15 * 3600, 15 * 3600 + 1)


def _hhmmssmmm(t_ns: int) -> int:
    """ns past midnight → packed HHMMSSmmm int (TORA timestamp form).

    h*10^7 + m*10^5 + s*10^3 + mmm — the seconds slot must be ×1000 so the
    3 millisecond digits never carry into it."""
    s, ns = divmod(t_ns, 10**9)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return h * 10**7 + m * 10**5 + sec * 10**3 + ns // 10**6


def _stat_of(t_ns: int) -> str:
    s = t_ns // 10**9
    if AUCTION_OPEN_ADD[0] <= s < AUCTION_OPEN_ADD[1] or AUCTION_OPEN_FILL[0] <= s < AUCTION_OPEN_FILL[1]:
        return "1"  # 集合竞价 (adds + the fill print instant)
    if AUCTION_CLOSE_ADD[0] <= s < AUCTION_CLOSE_ADD[1] or AUCTION_CLOSE_FILL[0] <= s < AUCTION_CLOSE_FILL[1]:
        return "b"  # 收盘集合竞价
    return "2"  # 连续交易


def _exchange_of(sid: int) -> str:
    return EXCH_SSE if 2_600_000 <= sid < 2_700_000 else EXCH_SZSE


def _epoch_ns(trading_day: int) -> int:
    from alignment_ob.converters import epoch_ns

    return epoch_ns(trading_day)


def synth_capture(
    place_path: Path,
    trade_path: Path,
    tick_table_path: Path,
    out_path: Path,
) -> Path:
    """Research tape → TORA-shaped JSONL (md/tr/od lines, time-interleaved).

    Book snapshots (md) come from the tick_table book columns at each event
    time; the snapshot is emitted BEFORE its event so the normalizer's book
    stamping reproduces the research row exactly.
    """
    import pyarrow.parquet as pq

    place = pq.read_table(place_path).to_pylist()
    trade = pq.read_table(trade_path).to_pylist()
    ticks = pq.read_table(tick_table_path).to_pylist()

    sid = place[0]["SecurityID"]
    sec_id = str(sid - 2_000_000)
    exch = _exchange_of(sid)
    epoch = _epoch_ns(place[0]["TradeDate"])

    # book snapshots indexed by t_ns (a tick_table row's book is the book at
    # that moment; stamping order md-before-event reproduces research rows)
    snaps: dict[int, dict] = {}
    for r in ticks:
        t = r["local_ts"]
        snaps.setdefault(t, r)  # first row wins if several share a ts

    events: list[tuple[int, str, dict]] = []  # (t_ns, kind, line)

    for r in place:
        t_ns = r["OrderTime"] - epoch  # CST epoch → ns of day
        line = {
            "type": "od",
            "SecurityID": sec_id,
            "ExchangeID": exch,
            "OrderTime": _hhmmssmmm(t_ns),
            "Price": r["Price"],
            "Volume": r["Qty"],
            "Side": "1" if r["BSFlag"] == 1 else "2",
            "OrderType": "2",
            "MainSeq": r["OrderIndex"],
            "SubSeq": 0,
            "OrderNO": r["OrderID"],
            "OrderStatus": "A" if r["OrdType"] == 0 else "D",
            "BizIndex": r["BizIndex"],
        }
        events.append((t_ns, "od", line))

    for r in trade:
        t_ns = r["TradeTime"] - epoch
        # only auction fills are statelessly representable (Rust mutates the
        # aggressor's Add row for continuous fills — tick_row_spec.md)
        if _stat_of(t_ns) == "2":
            continue
        line = {
            "type": "tr",
            "SecurityID": sec_id,
            "ExchangeID": exch,
            "TradeTime": _hhmmssmmm(t_ns),
            "TradePrice": r["Price"],
            "TradeVolume": r["Qty"],
            "ExecType": "1",
            "MainSeq": r["BizIndex"],
            "SubSeq": 0,
            "BuyNo": r["BuyOrderID"],
            "SellNo": r["SellOrderID"],
            "TradeBSFlag": "N" if r["BSFlag"] == 0 else "B" if r["BSFlag"] == 1 else "S",
            "BizIndex": r["BizIndex"],
        }
        events.append((t_ns, "tr", line))

    # md snapshot lines: one per tick_table row, stamped just before its time
    for r in ticks:
        t_ns = r["local_ts"]
        md = {
            "type": "md",
            "SecurityID": sec_id,
            "ExchangeID": exch,
            "DataTimeStamp": _hhmmssmmm(t_ns),
            "LastPrice": r["last_price"] / 100.0,
            "TotalVolumeTrade": 0,
            "NumTrades": 0,
            "MDSecurityStat": _stat_of(t_ns),
        }
        for i in range(1, 11):
            md[f"BidPrice{i}"] = r[f"bid_price_{i-1}"] / 100.0
            md[f"BidVolume{i}"] = r[f"bid_qty_{i-1}"]
            md[f"Bid{i}NumOrders"] = r[f"bid_orders_{i-1}"]
            md[f"AskPrice{i}"] = r[f"ask_price_{i-1}"] / 100.0
            md[f"AskVolume{i}"] = r[f"ask_qty_{i-1}"]
            md[f"Ask{i}NumOrders"] = r[f"ask_orders_{i-1}"]
        events.append((t_ns, "md", md))

    # md snapshots must precede their event at equal timestamps (book-first)
    events.sort(key=lambda e: (e[0], 0 if e[1] == "md" else 1))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(line) + "\n" for _, _, line in events)
    return out_path
