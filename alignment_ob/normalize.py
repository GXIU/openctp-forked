"""M1 normalizer: raw lev2_dump JSONL → research Event stream.

Every mapping here is grounded on the official headers (SDK v4.0.8):
- prices: double YUAN (TTORATstpPriceType) → int CENTS (×100, round)
- volumes: long long shares → verbatim
- timestamps: int (TTORATstpTimeStampType) — packed HHMMSSmmm (9 digits) or
  HHMMSS (6 digits); M0 dumps will confirm the exact form
- OrderStatus 'A'=add → event 0, 'D'=delete → event 2
- TradeBSFlag 'B'=主动买 → sign +1, 'S'=主动卖 → sign -1 (SH only; for SZ the
  sign is derived from price vs book mid)
- ExecType '2' (SZ 撤销) → cancel event; MDSecurityStat '1'/'b' (集合竞价) →
  event 4 auction match, '2' continuous → event 1

The three TORA streams (book snapshots / 逐笔成交 / 逐笔委托) are merged into
one event stream: the normalizer keeps the last 10-level book and stamps
every transaction/order event with it (has_book=True) so the M2 converter
reproduces the research tick_table row shape exactly. Resend streams are
deduped by (MainSeq, SubSeq) against the live streams.

Open items for M3 (profiler-verified against the research tape, listed in
alignment_ob/README.md): level_placed derivation, order_id attribution on
trade rows (BuyNo/SellNo → aggressor side), SZ sign derivation, event-5
semantics, DepartmentID requirement at login.
"""

from dataclasses import dataclass, field

from live.events import Event

# --- header constants (TORATstpLev2ApiDataType.h) ---
EXCH_SSE, EXCH_SZSE = "1", "2"
TBSF_TO_SIGN = {"B": 1, "S": -1, "N": 0}
SIDE_TO_SIGN = {"1": 1, "2": -1, "3": 1, "4": -1}  # 借入/借出 fold to ±1
LOS_ADD, LOS_DELETE = "A", "D"
EXEC_CANCEL = "2"  # SZ 撤销 (ExecType)
STAT_AUCTION = ("1", "b")  # 集合竞价 / 收盘集合竞价

MAX_LEVELS = 10


def decode_ts(v: int) -> int:
    """Packed int timestamp → ns past midnight.

    Accepts HHMMSSmmm (8-9 digits, e.g. 93001123 = 09:30:01.123) or HHMMSS
    (5-6 digits, e.g. 93001 = 09:30:01) — left-padded to 9/6 digits so the
    leading zero of hours before 10:00 survives the int round-trip. Anything
    ≥ 1e12 is assumed to already be ns-since-midnight. The exact form is
    confirmed from the first M0 dump.
    """
    if v >= 10**12:
        return v
    if v <= 999999:  # HHMMSS
        s = f"{v:06d}"
        frac = 0
    else:  # HHMMSSmmm
        s = f"{v:09d}"
        frac = int(s[6:9])
    h, m, sec = int(s[:2]), int(s[2:4]), int(s[4:6])
    return ((h * 3600 + m * 60 + sec) * 10**9) + frac * 10**6


@dataclass
class BookState:
    """Last-known 10-level book for one instrument (research int cents)."""

    bid_price: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    bid_qty: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    bid_orders: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    ask_price: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    ask_qty: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    ask_orders: list = field(default_factory=lambda: [0] * MAX_LEVELS)
    total_volume: int = 0  # cum day volume (rollover detection)
    num_trades: int = 0
    md_stat: str = ""  # last MDSecurityStat
    last_price: int = 0

    def update_md(self, line: dict) -> None:
        for i in range(1, MAX_LEVELS + 1):
            self.bid_price[i - 1] = round(line[f"BidPrice{i}"] * 100)
            self.bid_qty[i - 1] = line[f"BidVolume{i}"]
            self.bid_orders[i - 1] = line[f"Bid{i}NumOrders"]
            self.ask_price[i - 1] = round(line[f"AskPrice{i}"] * 100)
            self.ask_qty[i - 1] = line[f"AskVolume{i}"]
            self.ask_orders[i - 1] = line[f"Ask{i}NumOrders"]
        self.total_volume = line.get("TotalVolumeTrade", self.total_volume)
        self.num_trades = line.get("NumTrades", self.num_trades)
        self.md_stat = line.get("MDSecurityStat", self.md_stat)
        if line.get("LastPrice"):
            self.last_price = round(line["LastPrice"] * 100)

    def level_placed(self, price_cents: int, side_sign: int) -> float:
        """Order price vs book → level on the order's side (1 = best).

        Approximation of the research level_placed; M3 verifies against the
        tape. 0 when the price does not touch the book / no book yet."""
        levels = self.bid_price if side_sign > 0 else self.ask_price
        for i, book_price in enumerate(levels, start=1):
            if book_price and (
                (side_sign > 0 and price_cents >= book_price)
                or (side_sign < 0 and price_cents <= book_price)
            ):
                return float(i)
        return 0.0

    def extra_cols(self) -> dict:
        """Book columns in tick_table naming (all 10 levels)."""
        out = {}
        for i in range(MAX_LEVELS):
            out[f"bid_price_{i}"] = self.bid_price[i]
            out[f"bid_qty_{i}"] = self.bid_qty[i]
            out[f"bid_orders_{i}"] = self.bid_orders[i]
            out[f"ask_price_{i}"] = self.ask_price[i]
            out[f"ask_qty_{i}"] = self.ask_qty[i]
            out[f"ask_orders_{i}"] = self.ask_orders[i]
        return out


def md_symbol_of(security_id: str, exchange_id: str) -> str:
    return f"{'sh' if exchange_id == EXCH_SSE else 'sz'}{security_id}"


class Normalizer:
    """Merges raw capture lines into a research Event stream per instrument."""

    def __init__(self, trading_day: int) -> None:
        self.trading_day = trading_day
        self.books: dict[str, BookState] = {}
        self.seen: set[tuple] = set()  # (type, MainSeq, SubSeq) dedupe

    def book(self, sid: str) -> BookState:
        return self.books.setdefault(sid, BookState())

    def _stamp(self, sym: str, **fields) -> dict:
        book = self.book(sym)
        extra = book.extra_cols()
        extra.update(fields)
        return extra

    def _tr_event(self, sym: str, line: dict, is_resend: bool) -> Event | None:
        # resend = re-send of the same missed sequence: same key space as live
        key = ("tr", line["MainSeq"], line["SubSeq"])
        if key in self.seen:
            return None
        self.seen.add(key)
        book = self.book(sym)
        price = round(line["TradePrice"] * 100)
        vol = line["TradeVolume"]
        exec_type = line.get("ExecType", "")
        if exec_type == EXEC_CANCEL:  # SZ 撤销
            event = 2
            sign = 0
            vol_placed = vol
            vol_traded = 0.0
        else:
            event = 4 if book.md_stat in STAT_AUCTION else 1
            sign = TBSF_TO_SIGN.get(line.get("TradeBSFlag", "N"), 0)
            if line.get("ExchangeID") == EXCH_SZSE and sign == 0:
                sign = self._derive_sign(book, price)
            vol_placed = 0.0
            vol_traded = float(vol)
        order_id = line["BuyNo"] if sign >= 0 else line["SellNo"]
        return Event(
            instrument=sym,
            t_ns=decode_ts(line["TradeTime"]),
            trading_day=self.trading_day,
            event=event,
            sign=sign,
            price=float(price),
            last_price=float(price),
            vol_placed=vol_placed,
            vol_traded=vol_traded,
            level_placed=0.0,
            cum_volume=float(book.total_volume),
            has_book=True,
            extra=self._stamp(
                sym,
                qty=float(vol),
                order_id=order_id,
                order_seq=line["SubSeq"],
                exch_ts=decode_ts(line["TradeTime"]),
                tvr=float(book.total_volume),
            ),
        )

    def _derive_sign(self, book: BookState, price: int) -> int:
        """SZ sign from price vs book midpoint (aggressor side)."""
        if book.bid_price[0] and book.ask_price[0]:
            mid = (book.bid_price[0] + book.ask_price[0]) / 2
            return 1 if price >= mid else -1
        return 0

    def _od_event(self, sym: str, line: dict, is_resend: bool) -> Event | None:
        key = ("od", line["MainSeq"], line["SubSeq"])
        if key in self.seen:
            return None
        self.seen.add(key)
        book = self.book(sym)
        status = line.get("OrderStatus", "")
        event = 0 if status == LOS_ADD else 2 if status == LOS_DELETE else -1
        sign = SIDE_TO_SIGN.get(line.get("Side", ""), 0)
        price = round(line["Price"] * 100)
        return Event(
            instrument=sym,
            t_ns=decode_ts(line["OrderTime"]),
            trading_day=self.trading_day,
            event=event,
            sign=sign,
            price=float(price),
            last_price=float(book.last_price),
            vol_placed=float(line["Volume"]),
            vol_traded=0.0,
            level_placed=book.level_placed(price, sign) if status == LOS_ADD else 0.0,
            cum_volume=float(book.total_volume),
            has_book=True,
            extra=self._stamp(
                sym,
                order_id=line["OrderNO"],
                order_seq=line["SubSeq"],
                exch_ts=decode_ts(line["OrderTime"]),
            ),
        )

    def normalize_line(self, line: dict, trading_day: int | None = None) -> list[Event]:
        """One raw capture line → 0..1 events (md lines only update state)."""
        typ = line["type"]
        if typ in ("front", "login", "rsp_error", "sub_md", "sub_tr", "sub_od"):
            return []  # control/response lines — no event
        sym = md_symbol_of(line["SecurityID"], line["ExchangeID"])
        if typ == "md":
            self.book(sym).update_md(line)
            return []
        if typ == "tr":
            ev = self._tr_event(sym, line, is_resend=False)
        elif typ == "od":
            ev = self._od_event(sym, line, is_resend=False)
        elif typ == "rtr":
            ev = self._tr_event(sym, line, is_resend=True)
        elif typ == "rod":
            ev = self._od_event(sym, line, is_resend=True)
        else:
            return []  # unknown line — no event
        return [ev] if ev is not None else []


def normalize_capture(path, trading_day: int) -> list[Event]:
    """lev2_dump JSONL file → merged, book-stamped, deduped Event stream."""
    import json

    norm = Normalizer(trading_day)
    events: list[Event] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw.startswith("{"):
                continue  # .so chatter like CONNECT_TIMER
            try:
                line = json.loads(raw)
            except json.JSONDecodeError:
                continue
            events.extend(norm.normalize_line(line))
    return events
