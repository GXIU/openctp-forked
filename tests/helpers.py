"""Shared test helpers: synthetic events."""

from live.events import Event
from live.grid import ns_of


def ev(
    t,
    *,
    sym="sh600000",
    event=0,
    sign=0,
    price=1000.0,
    last_price=1000.0,
    vol_placed=0.0,
    vol_traded=0.0,
    level_placed=0.0,
    bid=990.0,
    ask=1010.0,
    bidq=1000.0,
    askq=1000.0,
    has_book=True,
    day=20220104,
    cum_volume=0.0,
    extra=None,
) -> Event:
    """Synthetic event; prices in cents (research units)."""
    return Event(
        instrument=sym,
        t_ns=ns_of(*t) if isinstance(t, tuple) else t,
        trading_day=day,
        event=event,
        sign=sign,
        price=price,
        last_price=last_price,
        vol_placed=vol_placed,
        vol_traded=vol_traded,
        level_placed=level_placed,
        bid_price_0=bid,
        ask_price_0=ask,
        bid_qty_0=bidq,
        ask_qty_0=askq,
        has_book=has_book,
        cum_volume=cum_volume,
        extra=extra or {},
    )
