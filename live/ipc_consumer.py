"""Live consumer: read cn_rst3's continuous Arrow IPC bar stream → signal.

The production Rust streaming engine (`cn_rst3 stream-real` / `stream`)
writes per-symbol bars as a continuous Arrow IPC stream (`--bars-ipc FILE`
or `--bars-stdout-ipc`) — "a downstream consumer can read it live". This
module is that downstream consumer: it maps each closed bar into a
`live.Bar` and feeds it through the pluggable `live.signal` + the shared
`sinks` stack (JSONL / parquet / Kafka — same output path as `live.run`).

Inputs (pick one):
- `--stdin` / `--file` — Arrow IPC stream on a pipe or file (direct link)
- `--kafka-consume` — subscribe the bars topic instead of the pipe (obgx
  `bridge_stdout_to_kafka` → topic → consumer here); each message value is
  one Arrow IPC batch in obgx wire format, key `{date}:{sid}`

Two producer schemas are accepted (auto-detected per row):
- graph mode (v260827+): `sid, date, endtime` + sorted leaf columns
  (`stream-real --graph <yaml> --freq 3m`)
- legacy 239 mode: `sid, date, window_start, window_end, row_count,
  close, high, low, open, volume, vwap, vwap_gt1`

Usage (true live — stdout pipe):
    cn_rst3 stream-real --date 20220104 --data-dir ... \
      --graph alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml --freq 3m \
      --bars-stdout-ipc 2>/dev/null \
      | PYTHONPATH=<repo> .venv-live/bin/python -m live.ipc_consumer --stdin

Usage (Kafka bus):
    python -m live.ipc_consumer --kafka-consume \
      --kafka-consume-brokers localhost:9092 --kafka-consume-topic cn.bars.3m \
      --jsonl bars/kafka.jsonl
"""

import argparse
import importlib
import json
import logging
import sys

from .engine import Bar
from .sinks import JsonlSink, KafkaSink, ParquetSink, Sink

LOG = logging.getLogger("live.ipc_consumer")

_KEY_COLS = ("sid", "date", "endtime", "window_start", "window_end", "row_count")


def bar_from_row(row: dict) -> Bar:
    """cn_rst3 bar row (graph or legacy 239 schema) → live Bar."""
    sid = row["sid"]
    if isinstance(sid, str):
        code = "".join(ch for ch in sid if ch.isdigit())
        sid = 2_000_000 + int(code)
    if "endtime" in row:  # graph mode (v260827+)
        endtime = int(row["endtime"])
    else:  # legacy 239 MinuteBar
        endtime = int(row["window_end"])
    leaves = {k: float(v) for k, v in row.items() if k not in _KEY_COLS}
    return Bar(sid=sid, date=int(row["date"]), endtime=endtime, leaves=leaves)


def consume_batches(batches, signal_fn, sinks: list[Sink]) -> tuple[int, int]:
    """Run the signal over every batch; return (bars_seen, signals_emitted)."""
    latest: dict[str, Bar] = {}
    n_bars = n_signals = 0
    for batch in batches:
        for row in batch.to_pylist():
            bar = bar_from_row(row)
            sym = row["sid"]
            latest[sym] = bar
            n_bars += 1
            signal = None
            try:
                signal = signal_fn(latest, sym)
            except Exception:  # signal must never kill the consumer
                LOG.exception("signal fn failed for %s", sym)
            if signal is not None:
                n_signals += 1
            for sink in sinks:
                sink.write_bar(bar, signal)
    return n_bars, n_signals


def decode_batches(payload: bytes):
    """Decode one Kafka message value (Arrow IPC stream) → record batches.

    obgx `kafka_sink` / `bridge_stdout_to_kafka` puts exactly one batch per
    message, but we iterate the stream anyway — a multi-batch message is a
    valid IPC stream and consumes identically.
    """
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(payload)) as reader:
        return list(reader)


def _default_consumer_factory(config: dict):
    """Build a confluent_kafka.Consumer — lazy import, mirrors KafkaSink."""
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise ImportError(
            "Kafka consume mode requires confluent-kafka: uv pip install "
            "--python .venv-live/bin/python confluent-kafka"
        ) from exc
    return Consumer(config)


def consume_kafka(
    consumer_factory,
    brokers: str,
    topic: str,
    group: str,
    consumer_config: dict | None,
    signal_fn,
    sinks: list[Sink],
    *,
    max_messages: int | None = None,
) -> tuple[int, int]:
    """Subscribe the bars topic and run the signal over every message.

    Each message value is one Arrow IPC batch (obgx wire format); its key is
    `{date}:{sid}` (informational — the bar row carries both). Polls forever
    in production; `max_messages` caps messages for tests/debug runs.
    """
    config = {
        "bootstrap.servers": brokers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        **(consumer_config or {}),
    }
    consumer = consumer_factory(config)
    n_msgs = n_bars = n_signals = 0
    try:
        consumer.subscribe([topic])
        while max_messages is None or n_msgs < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"kafka consumer error: {msg.error()}")
            n_msgs += 1
            bars, signals = consume_batches(decode_batches(msg.value()), signal_fn, sinks)
            n_bars += bars
            n_signals += signals
    finally:
        consumer.close()
    return n_bars, n_signals


def make_consumer_sinks(args) -> list[Sink]:
    sinks: list[Sink] = []
    if args.jsonl:
        sinks.append(JsonlSink(args.jsonl, dedupe=not args.no_dedupe))
    if args.parquet_dir:
        sinks.append(ParquetSink(args.parquet_dir, freq=args.freq))
    if args.kafka_brokers and args.kafka_topic:
        sinks.append(
            KafkaSink(
                args.kafka_brokers,
                args.kafka_topic,
                key_by=args.kafka_key_by,
                producer_config=args.kafka_producer_config or {},
            )
        )
    return sinks


def main(argv: list[str] | None = None) -> int:
    import pyarrow as pa

    p = argparse.ArgumentParser(prog="live.ipc_consumer")
    p.add_argument("--stdin", action="store_true", help="read Arrow IPC from stdin (live pipe)")
    p.add_argument("--file", help="read Arrow IPC from a file")
    p.add_argument("--kafka-consume", action="store_true", help="subscribe a Kafka topic instead")
    p.add_argument("--kafka-consume-brokers", default="", help="bootstrap servers (with --kafka-consume)")
    p.add_argument("--kafka-consume-topic", default="", help="topic to subscribe (with --kafka-consume)")
    p.add_argument("--kafka-consumer-group", default="live.ipc_consumer")
    p.add_argument("--kafka-consumer-config", default=None, help="JSON dict of extra consumer config")
    p.add_argument("--kafka-consume-max", type=int, default=None, help="debug: stop after N messages")
    p.add_argument("--jsonl", default="bars/live_stream.jsonl", help="JSONL output path (default)")
    p.add_argument("--parquet-dir", default="", help="optional parquet sink dir")
    p.add_argument("--freq", default="3m", help="freq label for parquet sink")
    p.add_argument("--no-dedupe", action="store_true")
    p.add_argument("--kafka-brokers", default="", help="Kafka bootstrap servers (enables Kafka sink)")
    p.add_argument("--kafka-topic", default="", help="Kafka topic (required with --kafka-brokers)")
    p.add_argument("--kafka-key-by", default="sid")
    p.add_argument("--kafka-producer-config", default=None, help="JSON dict of extra producer config")
    p.add_argument("--signal-module", default="live.signal")
    p.add_argument("--signal-function", default="compute_signal")
    args = p.parse_args(argv)
    if args.kafka_brokers and not args.kafka_topic:
        p.error("--kafka-topic is required with --kafka-brokers")
    if args.kafka_producer_config:
        args.kafka_producer_config = json.loads(args.kafka_producer_config)
    if args.kafka_consumer_config:
        args.kafka_consumer_config = json.loads(args.kafka_consumer_config)
    if args.kafka_consume and not (args.kafka_consume_brokers and args.kafka_consume_topic):
        p.error("--kafka-consume requires --kafka-consume-brokers and --kafka-consume-topic")

    signal_fn = getattr(importlib.import_module(args.signal_module), args.signal_function)
    sinks = make_consumer_sinks(args)
    if not sinks:
        p.error("no sinks configured (--jsonl '' disables it; enable one)")

    if args.stdin:
        with pa.ipc.open_stream(sys.stdin.buffer) as reader:
            n_bars, n_signals = consume_batches(reader, signal_fn, sinks)
    elif args.file:
        with pa.ipc.open_stream(pa.memory_map(args.file, "r")) as reader:
            n_bars, n_signals = consume_batches(reader, signal_fn, sinks)
    elif args.kafka_consume:
        n_bars, n_signals = consume_kafka(
            _default_consumer_factory,
            args.kafka_consume_brokers,
            args.kafka_consume_topic,
            args.kafka_consumer_group,
            args.kafka_consumer_config,
            signal_fn,
            sinks,
            max_messages=args.kafka_consume_max,
        )
    else:
        p.error("one of --stdin / --file / --kafka-consume is required")
    for sink in sinks:
        sink.close()
    LOG.info("consumed %d bars, %d signals", n_bars, n_signals)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
