"""BarEngine: per-window event buffers → alfa bar-graph evaluation.

For each instrument, events are buffered per session-aware 3m window; when a
tick crosses a window endtime the engine closes the window(s) and evaluates
the configured alfa bar YAML (obgx `ASTGraph.load_flat` + `run`) over the
window's rows — leaves match research semantics by construction.

Tick-driven only (no wall clock): replay and live behave identically.
Empty windows are emitted with all-NaN leaves (matching cn_rst3 production
behavior); skipped windows (lunch gap) emit nothing.
"""

import threading
from dataclasses import dataclass, field

import numpy as np

from .events import TRADE_EVENTS, Event
from .grid import Grid, make_grid
from .instruments import Instrument

_EMPTY_ROWS = ()


@dataclass(slots=True)
class Bar:
    sid: int
    date: int
    endtime: int
    leaves: dict[str, float] = field(default_factory=dict)

    def to_row(self) -> dict[str, int | float]:
        return {"sid": self.sid, "date": self.date, "endtime": self.endtime, **self.leaves}


@dataclass(slots=True)
class _InstrumentState:
    inst: Instrument
    trading_day: int = 0
    cur_idx: int | None = None
    rows: list[dict] = field(default_factory=list)
    last_book: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)
    last_t_ns: int = -1
    last_cum_volume: float = 0.0


class BarEngine:
    def __init__(
        self,
        instruments: list[Instrument],
        grid: Grid,
        graph_path: str,
        *,
        min_samples: int = 0,
        emit_empty_windows: bool = True,
    ) -> None:
        # obgx import is deliberate here: this module only runs from .venv-live.
        from obg.astg import ASTGraph

        self.grid = grid
        self.min_samples = min_samples
        self.emit_empty_windows = emit_empty_windows
        self.graph = ASTGraph.load_flat(graph_path)
        self.leaf_names = sorted(v.name for v in self.graph.get_leaves())
        self.input_fields = self._discover_input_fields()
        self._states = {i.md_symbol: _InstrumentState(inst=i) for i in instruments}
        self._lock = threading.Lock()

    def _discover_input_fields(self) -> tuple[str, ...]:
        """Field names the graph reads via get_field, in first-seen order."""
        fields: list[str] = []
        seen: set[str] = set()
        for node in self.graph.variables.values():
            op = getattr(node, "producer", None)
            op_func = getattr(op, "op_func", None)
            if op_func is not None and op_func.__name__ == "get_field":
                name = op.const_args[0]
                if name not in seen:
                    seen.add(name)
                    fields.append(name)
        return tuple(fields)

    def instruments(self):
        """Configured instruments (iteration order = config order)."""
        for st in self._states.values():
            yield st.inst

    def set_trading_day(self, date: int) -> None:
        with self._lock:
            for st in self._states.values():
                if st.trading_day == 0:
                    st.trading_day = date

    def on_event(self, ev: Event) -> list[Bar]:
        with self._lock:
            st = self._states[ev.instrument]
            bars: list[Bar] = []

            if self._is_rollover(ev, st):
                bars += self._close_day(st)
                st.trading_day = ev.trading_day or st.trading_day

            idx = self.grid.window_index(ev.t_ns)
            if idx is None:
                # Post-close straggler / lunch gap: update tracking, drop row.
                self._track(ev, st)
                return bars

            if st.cur_idx is None:
                st.cur_idx = idx
            elif idx < st.cur_idx:
                # Out-of-order duplicate (ms-precision): update book only.
                self._track(ev, st)
                return bars
            elif idx > st.cur_idx:
                bars += self._close_windows(st, st.cur_idx, idx)
                st.cur_idx = idx

            bid, ask, _, _ = st.last_book
            st.rows.append(ev.row(bid_price_0=bid, ask_price_0=ask))
            self._track(ev, st)
            return bars

    def flush(self) -> list[Bar]:
        """Close out the current day (e.g. at EOF / shutdown)."""
        with self._lock:
            bars: list[Bar] = []
            for st in self._states.values():
                if st.cur_idx is not None:
                    bars += self._close_day(st)
            return bars

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _is_rollover(ev: Event, st: _InstrumentState) -> bool:
        # new trading day (live feeds) / time wrap or volume reset (7x24 loop restart)
        return (
            bool(ev.trading_day and st.trading_day and ev.trading_day != st.trading_day)
            or bool(st.last_t_ns >= 0 and ev.t_ns < st.last_t_ns)
            or bool(
                ev.cum_volume
                and st.last_cum_volume
                and ev.cum_volume < st.last_cum_volume
            )
        )

    @staticmethod
    def _track(ev: Event, st: _InstrumentState) -> None:
        st.last_t_ns = ev.t_ns
        if ev.event in TRADE_EVENTS and ev.cum_volume:
            st.last_cum_volume = ev.cum_volume
        st.last_book = (
            ev.bid_price_0 or st.last_book[0],
            ev.ask_price_0 or st.last_book[1],
            ev.bid_qty_0 or st.last_book[2],
            ev.ask_qty_0 or st.last_book[3],
        )

    def _close_windows(self, st: _InstrumentState, start: int, stop: int) -> list[Bar]:
        bars: list[Bar] = []
        for wi in range(start, stop):
            rows = st.rows if wi == st.cur_idx else _EMPTY_ROWS
            bar = self._evaluate_window(st, wi, rows)
            if bar is not None:
                bars.append(bar)
        st.rows.clear()
        return bars

    def _close_day(self, st: _InstrumentState) -> list[Bar]:
        start = st.cur_idx if st.cur_idx is not None else 0
        bars = self._close_windows(st, start, len(self.grid.endtimes))
        st.cur_idx = None
        st.rows.clear()
        return bars

    def _evaluate_window(
        self, st: _InstrumentState, wi: int, rows: list[dict]
    ) -> Bar | None:
        if not rows:
            if not self.emit_empty_windows:
                return None
            leaves = {name: np.nan for name in self.leaf_names}
        else:
            cols = self._build_columns(rows)
            result = self.graph.run(cols, min_samples=self.min_samples)
            leaves = {name: float(result[name]) for name in self.leaf_names}
        return Bar(
            sid=st.inst.sid,
            date=st.trading_day,
            endtime=self.grid.endtimes[wi],
            leaves=leaves,
        )

    def _build_columns(self, rows: list[dict]) -> dict[str, np.ndarray]:
        missing = [f for f in self.input_fields if not any(f in r for r in rows)]
        if missing:
            raise KeyError(
                f"feed does not provide graph input field(s) {missing} — "
                f"check the source adapter / graph compatibility"
            )
        cols: dict[str, np.ndarray] = {}
        for f in self.input_fields:
            arr = np.empty(len(rows), dtype=np.float64)
            for i, r in enumerate(rows):
                v = r[f]
                arr[i] = v if v is not None else np.nan
            cols[f] = arr
        return cols


def make_engine(
    instruments: list[Instrument],
    *,
    freq_min: int = 3,
    emit_empty_windows: bool = True,
    graph_path: str = "",
    min_samples: int = 0,
) -> BarEngine:
    return BarEngine(
        instruments,
        make_grid(freq_min),
        graph_path,
        min_samples=min_samples,
        emit_empty_windows=emit_empty_windows,
    )
