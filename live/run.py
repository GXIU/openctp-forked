"""CLI: run the live tick → bar graph features → signal service.

    .venv-live/bin/python -m live.run --config configs/live_replay.yaml
    .venv-live/bin/python -m live.run --config configs/live_tora.yaml
    .venv-live/bin/python -m live.run --check-grid
"""

import argparse
import importlib
import logging
import queue
import signal as os_signal
import sys
import threading
from pathlib import Path

from .config import load_config
from .engine import Bar, make_engine
from .grid import make_grid
from .sinks import Sink, make_sinks
from .source import make_source

LOG = logging.getLogger("live.run")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="live.run")
    p.add_argument("--config", default="configs/live_replay.yaml")
    p.add_argument("--check-grid", action="store_true", help="print the 81 endtimes and exit")
    p.add_argument("--dry-run", action="store_true", help="validate config/imports, exit")
    return p


def load_signal_fn(module: str, function: str):
    mod = importlib.import_module(module)
    return getattr(mod, function)


class Runner:
    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.engine = make_engine(
            list(cfg.instruments),
            freq_min=cfg.bars.freq_min,
            emit_empty_windows=cfg.bars.emit_empty_windows,
            graph_path=cfg.graph.path,
            min_samples=cfg.graph.min_samples,
        )
        self.sinks: list[Sink] = make_sinks(cfg.sinks)
        self.signal_fn = load_signal_fn(cfg.signal.module, cfg.signal.function)
        self.outbox: queue.Queue = queue.Queue(maxsize=100_000)
        self.latest: dict[str, Bar] = {}
        self._sym_by_sid = {i.sid: i.md_symbol for i in cfg.instruments}
        self._stop = threading.Event()
        LOG.info(
            "runner ready: %d instruments, graph=%s (%d leaves)",
            len(cfg.instruments),
            cfg.graph.path,
            len(self.engine.leaf_names),
        )

    def emit(self, bar: Bar) -> None:
        sym = self._sym_by_sid[bar.sid]
        self.latest[sym] = bar
        signal = None
        try:
            signal = self.signal_fn(self.latest, sym)
        except Exception:  # signal must never kill the pipeline
            LOG.exception("signal fn failed for %s", sym)
        for sink in self.sinks:
            try:
                sink.write_bar(bar, signal)
            except Exception:
                LOG.exception("sink %s failed", type(sink).__name__)

    def pump(self) -> None:
        """Drain the outbox until stop is set (main thread)."""
        while not self._stop.is_set():
            try:
                bar = self.outbox.get(timeout=0.5)
            except queue.Empty:
                continue
            self.emit(bar)

    def run(self) -> None:
        source = make_source(self.cfg.source, self.engine, self.outbox)
        if self.cfg.source.mode == "replay":
            source.run()  # blocking, finite — bars land in outbox
            self._stop.set()
            self.pump_final()
        else:
            t = threading.Thread(target=source.run, daemon=True, name="source")
            t.start()
            try:
                self.pump()
            except KeyboardInterrupt:
                LOG.info("interrupt — flushing")
            source.stop()
            self._stop.set()
            self._flush_remaining()
        for sink in self.sinks:
            sink.close()

    def pump_final(self) -> None:
        while not self.outbox.empty():
            self.emit(self.outbox.get_nowait())

    def _flush_remaining(self) -> None:
        self.pump_final()
        for bar in self.engine.flush():
            self.emit(bar)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    if args.check_grid:
        grid = make_grid(3)
        for i, e in enumerate(grid.endtimes):
            print(i, e)
        print("count:", len(grid.endtimes))
        return 0

    cfg = load_config(args.config)
    cfg_path = Path(args.config)
    LOG.info("config loaded from %s (mode=%s)", cfg_path, cfg.source.mode)
    if args.dry_run:
        Runner(cfg)  # validates graph load + imports without starting
        LOG.info("dry-run OK")
        return 0

    os_signal.signal(os_signal.SIGTERM, lambda *_: sys.exit(0))
    Runner(cfg).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
