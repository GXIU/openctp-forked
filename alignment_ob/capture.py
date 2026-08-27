"""M0/M1 capture harness: dump feed data → normalized Event stream.

The full offline loop works TODAY with ReplayCapture (research tick_table →
normalized Event JSONL), proving capture → convert → profile end to end on
real data. ToraCapture is the same loop against the live L2 SDK — it is
scaffolded with the confirmed struct names (tora-rs docs) and activates
when the SDK lands in alignment_ob/sdk/ (see README "SDK acquisition").

Capture layout (gitignored):
    captures/{date}/{sid}.events.jsonl   — normalized events (M1)
    captures/{date}/raw/{sid}.jsonl      — raw feed fields, pre-normalization (M0)
"""

import json
import logging
from pathlib import Path

from live.events import Event

LOG = logging.getLogger("alignment_ob.capture")

_CAPTURE_DIR = Path(__file__).parent / "captures"


def _dump(events: list[Event], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(
                    {
                        "instrument": ev.instrument,
                        "t_ns": ev.t_ns,
                        "trading_day": ev.trading_day,
                        "event": ev.event,
                        "sign": ev.sign,
                        "price": ev.price,
                        "last_price": ev.last_price,
                        "vol_placed": ev.vol_placed,
                        "vol_traded": ev.vol_traded,
                        "level_placed": ev.level_placed,
                        "bid_price_0": ev.bid_price_0,
                        "ask_price_0": ev.ask_price_0,
                        "bid_qty_0": ev.bid_qty_0,
                        "ask_qty_0": ev.ask_qty_0,
                        "cum_volume": ev.cum_volume,
                        "has_book": ev.has_book,
                        "extra": ev.extra,
                    }
                )
                + "\n" for ev in events)


def replay_capture(replay_dir: str, date: str, sids: list[str], out_dir: Path | None = None) -> int:
    """Research tick_table → normalized Event JSONL (the offline capture loop).

    Uses the same normalization ReplaySource does (raw pass-through, cents/
    shares, has_book=True), so the captured stream round-trips bit-faithfully
    through the converters — pinned by tests/test_alignment.py.
    """
    from types import SimpleNamespace

    from live.source import ReplaySource

    out_dir = out_dir or _CAPTURE_DIR
    n = 0
    for sid in sids:
        src = ReplaySource(
            engine=None, outbox=None, replay_dir=replay_dir, dates=[date]
        )
        inst = SimpleNamespace(md_symbol=sid)
        events = list(src._parquet_events(inst, date))
        _dump(events, out_dir / date / f"{sid}.events.jsonl")
        n += len(events)
        LOG.info("captured %s %s: %d events", date, sid, len(events))
    return n


class ToraCapture:
    """Live TORA L2 capture (M0 raw + M1 normalized) — SDK-conditional.

    SDK surface (struct names confirmed via tora-rs, NOT via the SDK itself):
      Lev2MdApi — Create/RegisterFront/Init/SubscribeLev2MarketData
      OnRtnLev2MarketData     — 10-level book (MarketDataField)
      OnRtnLev2OrderDetail    — 逐笔委托 (OrderDetailField: order no, price,
                                qty, side, order status)
      OnRtnLev2Transaction    — 逐笔成交 (TransactionField: price, qty,
                                BSFlag aggressor)
    Normalization rules to implement once live fields are seen (M1 checklist
    in the README): yuan→cents ×100, BSFlag→sign, resend dedupe by seq,
    level_placed derivation, qty/`cum_volume` for rollover.
    """

    def __init__(self, secrets: dict) -> None:
        self.secrets = secrets  # user/password/appid/authcode/fronts

    def sdk_available(self) -> bool:
        return (Path(__file__).parent / "sdk").is_dir()

    def run(self) -> None:
        if not self.sdk_available():
            raise RuntimeError(
                "TORA SDK not present — see alignment_ob/README.md "
                "'SDK acquisition' for the browser download steps, then put "
                "the SDK under alignment_ob/sdk/ and run from a China host."
            )
        raise NotImplementedError(
            "SDK callbacks land here once the SDK is unpacked: Lev2MdApi + "
            "OnRtnLev2MarketData/OrderDetail/Transaction → Event stream (M1). "
            "Struct names are pre-mapped in live/source.py ToraSource."
        )
