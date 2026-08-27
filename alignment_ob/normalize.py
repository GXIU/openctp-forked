"""M1 normalizer: raw lev2_dump JSONL → research Event stream.

The mapping is grounded on BOTH sources of truth:
- the official TORA headers (SDK v4.0.8): field names/types/enums
  (prices double yuan, volumes shares, int HHMMSSmmm timestamps, 'B'/'S'
  TradeBSFlag, 'A'/'D' OrderStatus, '1'/'2' SSE/SZSE);
- the cn_rst3 tick_row semantics (alignment_ob/tick_row_spec.md):
  event 1 = Add (4 when in auction), event 2 = Cancel with
  **vol_placed = -(qty)**, local_ts = exch_ts + 100ms,
  order_seq = per-symbol row counter, level_placed = price-level distance
  (0 at best, -1 crossed), auction fills = event 4 with vol_placed=0,
  vol_traded=qty, level_placed=-2, tvr=price×qty/100.

The three TORA streams are merged into one event stream: md snapshots
update the 10-level book state (pre-event stamping, matching the Rust
default); every od/tr event is stamped with the current book (has_book=True)
so the M2 converter reproduces research rows. Resend streams dedupe by
(MainSeq, SubSeq) in the same sequence space as live.

**Continuous trades are dropped here by design**: per the Rust semantics a
continuous fill MUTATES the aggressor's Add row and re-emits it — a
stateless mapper cannot reproduce that; it needs the per-order row-mutation
port (tick_row_spec.md §"Trades do not emit rows"). Auction fills ARE
emitted (statelessly representable).
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
        """Rust `level_placed()` on the visible 10-level book: 0 at best,
        -1 crossed (better than best), else the count of distinct price
        levels ABOVE the order price (bid) / BELOW it (ask) — inclusive of
        the best level. Rust counts the full book; the visible 10 levels
        undercount deeper orders (documented — cqr's `_lle1` leaves need
        only the top-2 and stay exact)."""
        prices = self.bid_price if side_sign > 0 else self.ask_price
        best = next((p for p in prices if p), 0)
        if not best:
            return 0.0
        if price_cents == best:
            return 0.0
        if (side_sign > 0 and price_cents > best) or (
            side_sign < 0 and price_cents < best
        ):
            return -1.0
        above = [p for p in prices if p and (price_cents < p)] if side_sign > 0 else [
            p for p in prices if p and (p < price_cents)
        ]
        return float(len(above))

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
        self._row_counter: dict[str, int] = {}  # order_seq = row_index + 1

    def book(self, sym: str) -> BookState:
        return self.books.setdefault(sym, BookState())

    def _next_seq(self, sym: str) -> int:
        self._row_counter[sym] = self._row_counter.get(sym, 0) + 1
        return self._row_counter[sym]

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
        exch_ts = decode_ts(line["TradeTime"])
        if exec_type == EXEC_CANCEL:  # SZ 撤销
            event, sign, vol_placed, vol_traded = 2, 0, -vol, 0.0
        elif book.md_stat in STAT_AUCTION:
            # Rust make_auction_row: event 4 fill, vol_placed=0,
            # level_placed=-2, order_id=0, tvr=price×qty/100
            event, sign = 4, TBSF_TO_SIGN.get(line.get("TradeBSFlag", "N"), 0)
            vol_placed, vol_traded = 0.0, float(vol)
        else:
            # Rust semantics: a continuous fill MUTATES the aggressor's Add
            # row — not statelessly representable. Dropped until the
            # per-order row-mutation port lands (tick_row_spec.md).
            return None
        if event == 4:
            order_id, level = 0, -2.0
            tvr = price * vol / 100.0
            last_price = float(price)
        else:
            order_id = line["BuyNo"] if sign >= 0 else line["SellNo"]
            level, tvr = 0.0, 0.0
            last_price = float(price)
        return Event(
            instrument=sym,
            t_ns=exch_ts + 100_000_000,  # local_ts = exch_ts + 100ms (Rust)
            trading_day=self.trading_day,
            event=event,
            sign=sign,
            price=float(price),
            last_price=last_price,
            vol_placed=vol_placed,
            vol_traded=vol_traded,
            level_placed=level,
            cum_volume=float(book.total_volume),
            has_book=True,
            extra=self._stamp(
                sym,
                qty=float(vol),
                order_id=order_id,
                order_seq=self._next_seq(sym),
                exch_ts=exch_ts,
                tvr=float(tvr),
            ),
        )

    def _od_event(self, sym: str, line: dict, is_resend: bool) -> Event | None:
        key = ("od", line["MainSeq"], line["SubSeq"])
        if key in self.seen:
            return None
        self.seen.add(key)
        book = self.book(sym)
        status = line.get("OrderStatus", "")
        sign = SIDE_TO_SIGN.get(line.get("Side", ""), 0)
        price = round(line["Price"] * 100)
        vol = line["Volume"]
        exch_ts = decode_ts(line["OrderTime"])
        if status == LOS_ADD:
            event = 4 if book.md_stat in STAT_AUCTION else 1  # Rust
            vol_placed = float(vol)
            level = book.level_placed(price, sign)
        elif status == LOS_DELETE:
            event = 2
            vol_placed = -float(vol)  # Rust: cancel carries -(qty)
            level = 0.0
        else:
            return None
        return Event(
            instrument=sym,
            t_ns=exch_ts + 100_000_000,  # local_ts = exch_ts + 100ms (Rust)
            trading_day=self.trading_day,
            event=event,
            sign=sign,
            price=float(price),
            last_price=float(book.last_price),
            vol_placed=vol_placed,
            vol_traded=0.0,
            level_placed=level,
            cum_volume=float(book.total_volume),
            has_book=True,
            extra=self._stamp(
                sym,
                qty=float(vol),
                order_id=line["OrderNO"],
                order_seq=self._next_seq(sym),
                exch_ts=exch_ts,
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
