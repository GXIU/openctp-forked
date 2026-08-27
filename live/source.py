"""Tick sources: replay (offline, primary dev path) and live adapters.

All sources produce normalized `Event`s and feed them to a `BarEngine` via a
callback queue; the engine returns closed bars which the runner funnels to
signal + sinks. Sources must never import openctp/TORA SDKs at module import
time — imports are lazy inside the source class so the replay path (and all
tests) run without any broker SDK installed.
"""

import csv
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar

from .engine import BarEngine
from .events import Event
from .instruments import Instrument

LOG = logging.getLogger("live.source")

_CORE_FIELDS = {
    "local_ts",
    "event",
    "sign",
    "price",
    "last_price",
    "vol_placed",
    "vol_traded",
    "level_placed",
    "bid_price_0",
    "ask_price_0",
    "bid_qty_0",
    "ask_qty_0",
}


class MdSource(ABC):
    """Produces events → engine; closed bars land in `outbox`."""

    def __init__(self, engine: BarEngine, outbox: queue.Queue) -> None:
        self.engine = engine
        self.outbox = outbox

    @abstractmethod
    def run(self) -> None:
        """Blocking: pump events until stopped (or EOF)."""

    def stop(self) -> None:
        pass


class ReplaySource(MdSource):
    """Replay research tick_table parquet (or a CSV of events) through the engine.

    tick_table parquet = the actual 逐笔 research data (obgx/alfa format):
    one file per sid per date, columns `event, sign, price, vol_placed,
    vol_traded, level_placed, last_price, {ask,bid}_price_0..9, ...`,
    prices in CENTS, `local_ts` in ns past midnight. Values pass through
    RAW (research units) so live leaves are directly comparable to research
    runs; every row keeps its own book snapshot verbatim (has_book=True).
    """

    def __init__(
        self,
        engine: BarEngine,
        outbox: queue.Queue,
        *,
        replay_dir: str,
        dates: list[str],
        fmt: str = "parquet",
        loop: bool = False,
    ) -> None:
        super().__init__(engine, outbox)
        self.replay_dir = Path(replay_dir)
        self.dates = dates
        self.fmt = fmt
        self.loop = loop

    def run(self) -> None:
        while True:
            for date in self.dates:
                self.engine.set_trading_day(int(date))
                n_bars = 0
                for inst in self.engine.instruments():
                    for ev in self._events_for(inst, date):
                        bars = self.engine.on_event(ev)
                        self._emit(bars)
                        n_bars += len(bars)
                bars = self.engine.flush()
                self._emit(bars)
                n_bars += len(bars)
                LOG.info("replay done date=%s bars=%d", date, n_bars)
            if not self.loop:
                break

    def _emit(self, bars) -> None:
        for bar in bars:
            self.outbox.put(bar)

    def _events_for(self, inst: Instrument, date: str):
        if self.fmt == "parquet":
            yield from self._parquet_events(inst, date)
        elif self.fmt == "csv":
            yield from self._csv_events(inst, date)
        else:
            raise ValueError(f"bad replay fmt {self.fmt!r}")

    def _parquet_events(self, inst: Instrument, date: str):
        import pyarrow.parquet as pq

        path = self.replay_dir / date / f"{inst.md_symbol}.parquet"
        if not path.exists():
            LOG.warning("no replay file %s — skipping", path)
            return
        import pyarrow.types as pat

        table = pq.read_table(path)
        names = table.column_names
        cols = {n: table.column(n).to_numpy() for n in names}
        n = len(table)
        trading_day = int(date)
        numeric = {
            n
            for n in names
            if pat.is_integer(table.schema.field(n).type)
            or pat.is_floating(table.schema.field(n).type)
        }
        for i in range(n):
            # Raw research units: prices stay in cents, volumes in shares.
            extra = {
                name: float(cols[name][i])
                for name in names
                if name not in _CORE_FIELDS and name in numeric
            }
            yield Event(
                instrument=inst.md_symbol,
                t_ns=int(cols["local_ts"][i]),
                trading_day=trading_day,
                event=int(cols["event"][i]),
                sign=int(cols["sign"][i]),
                price=float(cols["price"][i]),
                last_price=float(cols["last_price"][i]),
                vol_placed=float(cols["vol_placed"][i]),
                vol_traded=float(cols["vol_traded"][i]),
                level_placed=float(cols["level_placed"][i]),
                bid_price_0=float(cols["bid_price_0"][i]),
                ask_price_0=float(cols["ask_price_0"][i]),
                bid_qty_0=float(cols["bid_qty_0"][i]),
                ask_qty_0=float(cols["ask_qty_0"][i]),
                has_book=True,
                extra=extra,
            )

    def _csv_events(self, inst: Instrument, date: str):
        path = self.replay_dir / date / f"{inst.md_symbol}.csv"
        if not path.exists():
            LOG.warning("no replay file %s — skipping", path)
            return
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                yield Event(
                    instrument=row["instrument"],
                    t_ns=int(row["t_ns"]),
                    trading_day=int(row.get("trading_day") or date),
                    event=int(row["event"]),
                    sign=int(row.get("sign") or 0),
                    price=float(row.get("price") or 0),
                    last_price=float(row.get("last_price") or 0),
                    vol_placed=float(row.get("vol_placed") or 0),
                    vol_traded=float(row.get("vol_traded") or 0),
                    level_placed=float(row.get("level_placed") or 0),
                    bid_price_0=float(row.get("bid_price_0") or 0),
                    ask_price_0=float(row.get("ask_price_0") or 0),
                    bid_qty_0=float(row.get("bid_qty_0") or 0),
                    ask_qty_0=float(row.get("ask_qty_0") or 0),
                )


class ToraSource(MdSource):
    """TORA (华鑫奇点) native L2 market data adapter — tick on quote AND trade.

    The official 奇点 SDK L2 module (`TORALEV2API`, v4.0.8) provides exactly
    the research event stream (struct names confirmed via the tora-rs bindings
    and vnpy_tora generators):

      - Lev2OrderDetailField   — 逐笔委托 (LOS_Add/LOS_Delete → place/cancel)
      - Lev2TransactionField   — 逐笔成交 (TBSF_Buy/Sell → aggressor sign)
      - Lev2MarketDataField    — 10-level book snapshot (stamped on event rows)
      - Lev2Resend{OrderDetail,Transaction}Field — retransmission (dedupe by seq)

    7x24 行情前置: tcp://210.14.72.16:9402   仿真行情前置: tcp://210.14.72.21:4402
    L2 行情 needs a 华鑫 account (n-sight.com.cn); free in A类量化机房,
    purchased for B类 (自备机器).

    BLOCKED ON SDK: whether the official Python SDK ships the L2 逐笔 module
    must be confirmed from the SDK download (needs the account). If Python L2
    is not shipped, bridge via a small C++ shim or the Rust tora-rs crate
    (logged decision — see CLAUDE.md decisions log). The mapping below is
    written against the confirmed C++ surface so only the SDK plumbing needs
    filling in.
    """

    #: event-code mappings into the research conventions (Event docstring)
    TICK_TYPE_TO_EVENT: ClassVar = {"Add": 0, "Delete": 2, "Trade": 1}
    ORDER_STATUS_TO_EVENT: ClassVar = {"Add": 0, "Delete": 2}
    TBSF_TO_SIGN: ClassVar = {"Buy": 1, "Sell": -1}

    def __init__(
        self,
        engine: BarEngine,
        outbox: queue.Queue,
        *,
        front: str,
        account_user: str,
        account_password: str,
        instruments: list[Instrument],
        env: str = "sim",
    ) -> None:
        super().__init__(engine, outbox)
        self.front = front
        self.user = account_user
        self.password = account_password
        self.instruments = instruments
        self.env = env
        self._sdk = None  # lazily bound in run()

    def run(self) -> None:
        # Lazy import: the official SDK module name (e.g. `lev2mdapi`) must be
        # confirmed from the 华鑫奇点 SDK download. Replace the import below.
        try:
            import lev2mdapi
        except ImportError as exc:
            raise RuntimeError(
                "TORA L2 Python SDK not installed (or module name differs). "
                "Obtain the official 奇点 SDK from n-sight.com.cn; see "
                "live/source.py ToraSource for the mapping scaffold."
            ) from exc
        self._sdk = lev2mdapi
        # TODO(blocked-on-sdk): bind SPI callbacks → self._on_* handlers,
        # CreateLev2MdApi, RegisterFront(self.front), Init, login (user/password
        # per 华鑫 rules), SubscribeMarketData for self.instruments, Join loop.
        raise NotImplementedError("ToraSource awaits the official SDK (see docstring)")

    # -- callback → Event mappings (written against the confirmed C++ surface)

    def _on_lev2_order_detail(self, d) -> Event:
        """Lev2OrderDetailField → place/cancel Event."""
        return Event(
            instrument=f"{_exchange_prefix(d.ExchangeID)}{d.SecurityID}",
            t_ns=_data_timestamp_ns(d.DataTimeStamp),
            trading_day=0,
            event=self.ORDER_STATUS_TO_EVENT[d.OrderStatus],
            price=float(d.Price),
            vol_placed=float(d.OrderQty) if hasattr(d, "OrderQty") else 0.0,
        )

    def _on_lev2_transaction(self, d) -> Event:
        """Lev2TransactionField → trade Event with BSFlag aggressor sign."""
        return Event(
            instrument=f"{_exchange_prefix(d.ExchangeID)}{d.SecurityID}",
            t_ns=_data_timestamp_ns(d.DataTimeStamp),
            trading_day=0,
            event=1,
            sign=self.TBSF_TO_SIGN.get(d.TradeBSFlag, 0),
            price=float(d.Price),
            last_price=float(d.Price),
            vol_traded=float(d.TradeQty) if hasattr(d, "TradeQty") else 0.0,
            cum_volume=float(d.TotalVolumeTrade) if hasattr(d, "TotalVolumeTrade") else 0.0,
        )

    def _on_lev2_market_data(self, d) -> None:
        """Lev2MarketDataField → book L0 stamp (no event row by itself)."""
        # TODO(blocked-on-sdk): store into a per-instrument book cache the
        # engine stamps onto subsequent event rows (bid_price_0/ask_price_0).


def _exchange_prefix(exchange_id: str) -> str:
    e = exchange_id.upper()
    if e in ("SSE", "SH", "1"):
        return "sh"
    if e in ("SZSE", "SZ", "2"):
        return "sz"
    return "bj"


def _data_timestamp_ns(ts: int) -> int:
    """TORA DataTimeStamp (int) → ns past midnight.

    Exact unit TBD from the SDK header; 奇点 timestamps are typically
    milliseconds past midnight in the L2 feed — confirm when the SDK lands."""
    return int(ts) * 1_000_000


class MdapiSource(MdSource):
    """Degraded TTS fallback: CTP-compatible mdapi snapshots (5-level).

    NOT the production path — snapshots only, no 逐笔, so event-count leaves
    (n_placeetc, n_bid, ...) evaluate to 0/NaN. Kept because it works today
    without any broker account (TTS 7x24) and exercises the whole pipeline.
    Runs from the 3.12 venv (openctp-ctp wheel segfaults on 3.13).
    """

    def __init__(
        self,
        engine: BarEngine,
        outbox: queue.Queue,
        *,
        front: str,
        instruments: list[Instrument],
    ) -> None:
        super().__init__(engine, outbox)
        self.front = front
        self.instruments = instruments
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        try:
            from openctp_ctp import thostmduserapi as mdapi
        except ImportError as exc:
            raise RuntimeError(
                "mdapi mode needs openctp-ctp (3.12 venv) — this mode is a "
                "degraded fallback; use TORA L2 for tick-on-quote-and-trade."
            ) from exc

        symbols = [i.md_symbol.encode() for i in self.instruments]
        engine, outbox = self.engine, self.outbox

        class _Spi(mdapi.CThostFtdcMdSpi):
            def __init__(self):
                super().__init__()
                self.api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi()

            def OnFrontConnected(self):
                LOG.info("mdapi connected; logging in")
                self.api.ReqUserLogin(mdapi.CThostFtdcReqUserLoginField(), 0)

            def OnRspUserLogin(self, rsp, info, req_id, is_last):
                LOG.info("mdapi login: %s %s", info.ErrorID, info.ErrorMsg)
                self.api.SubscribeMarketData(symbols, len(symbols))

            def OnRtnDepthMarketData(self, d):
                t_ns = _hhmmss_ns(d.UpdateTime, d.UpdateMillisec)
                # Snapshot → book-stamped pseudo event (event=-1: research
                # cqr treats it as "no modify" — degraded by construction).
                bars = engine.on_event(
                    Event(
                        instrument=f"{_exchange_prefix(d.ExchangeID)}{d.InstrumentID}",
                        t_ns=t_ns,
                        trading_day=0,
                        event=-1,
                        last_price=float(d.LastPrice),
                        price=float(d.LastPrice),
                        bid_price_0=float(d.BidPrice1),
                        ask_price_0=float(d.AskPrice1),
                        bid_qty_0=float(d.BidVolume1),
                        ask_qty_0=float(d.AskVolume1),
                        cum_volume=float(d.Volume),
                    )
                )
                for bar in bars:
                    outbox.put(bar)

        spi = _Spi()
        spi.api.RegisterSpi(spi)
        spi.api.RegisterFront(self.front)
        spi.api.Init()
        while not self._stop.is_set():
            time.sleep(0.2)


def _hhmmss_ns(update_time: str, update_ms: int) -> int:
    h, m, s = update_time.split(":")
    return ((int(h) * 60 + int(m)) * 60 + int(s)) * 1_000_000_000 + int(update_ms) * 1_000_000


def make_source(cfg, engine: BarEngine, outbox: queue.Queue) -> MdSource:
    if cfg.mode == "replay":
        return ReplaySource(
            engine,
            outbox,
            replay_dir=cfg.replay_dir,
            dates=list(cfg.replay_dates),
            fmt=cfg.replay_fmt,
        )
    if cfg.mode == "tora":
        return ToraSource(
            engine,
            outbox,
            front=cfg.front,
            account_user=cfg.account_user,
            account_password=cfg.account_password,
            instruments=list(engine.instruments()),
            env=cfg.env,
        )
    if cfg.mode == "mdapi":
        return MdapiSource(
            engine,
            outbox,
            front=cfg.front,
            instruments=list(engine.instruments()),
        )
    raise ValueError(f"bad mode {cfg.mode!r}")


def make_source_events_csv(rows: list[dict], path: str) -> None:
    """Write events as the CSV replay format (test helper)."""
    with open(path, "w", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
