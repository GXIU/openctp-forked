"""Normalized tick events — the research field schema.

Every event row carries the fields the alfa bar graphs read (get_field
const_args of cqr_bar.yaml): ask_price_0, bid_price_0, event, last_price,
level_placed, price, sign, vol_placed, vol_traded. UNITS ARE THE RESEARCH
UNITS — prices in CENTS, volumes in SHARES (cqr_cn.py feeds raw tick_table
columns to the graph; sources convert: tick_table passes through, TORA yuan
→ ×100). Leaf values are then directly comparable to research runs.

Event codes follow the research conventions (obgx cqr_cn.py):
    0  place (委托挂单, incl. 1/5/0 variants)     1  continuous trade
    2  cancel                                     4  auction match
    5  continuous trade (secondary code)
sign: +1 aggressive buy, -1 aggressive sell, 0 neutral (research BSFlag).
"""

from dataclasses import dataclass, field

TRADE_EVENTS = (1, 4, 5)
CANCEL_EVENT = 2


@dataclass(slots=True)
class Event:
    instrument: str  # "sh600000" style md_symbol
    t_ns: int  # nanos past midnight
    trading_day: int  # YYYYMMDD; 0 = unknown (engine keeps its current day)
    event: int  # research event code (0 place, 2 cancel, 4 auction, 1/5 trade)
    sign: int = 0
    price: float = 0.0  # order/execution price, cents
    last_price: float = 0.0  # last trade price, cents (== price for trade rows)
    vol_placed: float = 0.0
    vol_traded: float = 0.0
    level_placed: float = 0.0  # book level of a placed order; may be -1/-2 specials
    bid_price_0: float = 0.0  # book L0 stamped on the row (0 = unknown)
    ask_price_0: float = 0.0
    bid_qty_0: float = 0.0
    ask_qty_0: float = 0.0
    cum_volume: float = 0.0  # cumulative day volume from the feed; 0 = not provided
    # (used for 7x24 loop-restart rollover detection)
    has_book: bool = False  # True when the event carries its own book snapshot
    # (research tick_table rows do; TORA order/trade events do not — the
    # engine stamps its last-known book onto those instead. Verbatim is
    # required for bit-fidelity with research: genuine 0.0 book values exist.)
    extra: dict = field(default_factory=dict)  # additional graph input columns
    # (e.g. order_lifetime, volume_behind from tick_table — graphs beyond cqr
    # read these; sources fill them, engine merges into each row)

    #: graph input columns, in the order they appear in the bar YAML
    GRAPH_FIELDS = (
        "ask_price_0",
        "bid_price_0",
        "event",
        "last_price",
        "level_placed",
        "price",
        "sign",
        "vol_placed",
        "vol_traded",
    )

    def row(self, bid_price_0: float = 0.0, ask_price_0: float = 0.0) -> dict[str, float]:
        """One graph-input row, with the current book L0 stamped on it.

        Events carrying their own book snapshot (`has_book=True`, e.g.
        research tick_table rows) keep it verbatim — genuine 0.0 book values
        are meaningful there. Other events get the engine's last-known book
        stamped on, matching research semantics (every event row carries the
        book at event time)."""
        if self.has_book:
            b0, a0 = self.bid_price_0, self.ask_price_0
        else:
            b0 = self.bid_price_0 if self.bid_price_0 else bid_price_0
            a0 = self.ask_price_0 if self.ask_price_0 else ask_price_0
        row = {
            "ask_price_0": a0,
            "bid_price_0": b0,
            "event": float(self.event),
            "last_price": self.last_price,
            "level_placed": self.level_placed,
            "price": self.price,
            "sign": float(self.sign),
            "vol_placed": self.vol_placed,
            "vol_traded": self.vol_traded,
        }
        row.update(self.extra)
        return row
