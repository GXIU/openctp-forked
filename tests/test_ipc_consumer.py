"""ipc_consumer tests: cn_rst3 bar IPC rows (graph + legacy 239 schemas) → Bar + signal."""

import json

import pyarrow as pa
import pytest

from live.ipc_consumer import bar_from_row, consume_batches, consume_kafka
from live.signal import compute_signal
from live.sinks import JsonlSink, serialize_row_batch

MINUTEBAR_SCHEMA = pa.schema(
    [
        pa.field("sid", pa.string()),
        pa.field("date", pa.int64()),
        pa.field("window_start", pa.int64()),
        pa.field("window_end", pa.int64()),
        pa.field("row_count", pa.int64()),
        pa.field("close", pa.int64()),
        pa.field("high", pa.int64()),
        pa.field("low", pa.int64()),
        pa.field("open", pa.int64()),
        pa.field("volume", pa.float64()),
        pa.field("vwap", pa.float64()),
        pa.field("vwap_gt1", pa.float64()),
    ]
)

GRAPH_SCHEMA = pa.schema(
    [
        pa.field("sid", pa.string()),
        pa.field("date", pa.int64()),
        pa.field("endtime", pa.int64()),
        pa.field("n_trade", pa.float64()),
        pa.field("n_g_in_up_eq_trend", pa.float64()),
        pa.field("n_g_in_down_eq_trend", pa.float64()),
        pa.field("qty_xyz", pa.float64()),
    ]
)

MINUTEBAR_ROWS = [
    {
        "sid": "sh600007",
        "date": 20220104,
        "window_start": 33300000000000,
        "window_end": 34200000000000,
        "row_count": 234,
        "close": 1440,
        "high": 1440,
        "low": 0,
        "open": 0,
        "volume": 532900.0,
        "vwap": 23.5,
        "vwap_gt1": 21.2,
    },
    {
        "sid": "sz000001",
        "date": 20220104,
        "window_start": 34200000000000,
        "window_end": 34260000000000,
        "row_count": 423,
        "close": 1439,
        "high": 1455,
        "low": 1436,
        "open": 1440,
        "volume": 211900.0,
        "vwap": 1440.9,
        "vwap_gt1": 1440.9,
    },
]

GRAPH_ROWS = [
    {
        "sid": "sh600000",
        "date": 20220104,
        "endtime": 34200000000000,
        "n_trade": 12.0,
        "n_g_in_up_eq_trend": 99.0,
        "n_g_in_down_eq_trend": 0.0,
        "qty_xyz": float("nan"),
    }
]


def test_bar_from_row_legacy_239_schema():
    bar = bar_from_row(MINUTEBAR_ROWS[0])
    assert bar.sid == 2_600_007  # "sh600007" → 2_000_000 + 600007
    assert bar.date == 20220104
    assert bar.endtime == 34200000000000
    assert bar.leaves == {
        "close": 1440.0,
        "high": 1440.0,
        "low": 0.0,
        "open": 0.0,
        "volume": 532900.0,
        "vwap": 23.5,
        "vwap_gt1": 21.2,
    }
    assert bar_from_row(MINUTEBAR_ROWS[1]).sid == 2_000_001  # "sz000001"


def test_bar_from_row_graph_schema_v260827():
    bar = bar_from_row(GRAPH_ROWS[0])
    assert bar.sid == 2_600_000
    assert bar.endtime == 34200000000000
    # endtime must NOT leak into leaves
    assert set(bar.leaves) == {"n_trade", "n_g_in_up_eq_trend", "n_g_in_down_eq_trend", "qty_xyz"}
    assert bar.leaves["n_g_in_up_eq_trend"] == 99.0


def test_consume_batches_emits_signals(tmp_path):
    batch = pa.Table.from_pylist(MINUTEBAR_ROWS, schema=MINUTEBAR_SCHEMA).to_batches()[0]
    sink = JsonlSink(str(tmp_path / "signals.jsonl"))

    def fake_signal(bars, sym):
        return {"score": 0.5, "direction": 1, "meta": "test"}

    n_bars, n_signals = consume_batches([batch], fake_signal, [sink])
    sink.close()
    assert n_bars == 2 and n_signals == 2
    lines = [json.loads(l) for l in (tmp_path / "signals.jsonl").read_text().splitlines()]
    assert [l["sid"] for l in lines] == [2_600_007, 2_000_001]
    assert all(l["signal"]["meta"] == "test" for l in lines)


def _wire_payload(rows, schema):
    """One Arrow IPC batch as bytes — the shape obgx puts on the Kafka bus."""
    batch = pa.Table.from_pylist(rows, schema=schema).to_batches()[0]
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


class _FakeKafkaMessage:
    def __init__(self, value, error=None):
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error


class _FakeKafkaConsumer:
    """confluent_kafka.Consumer stand-in: yields queued messages, then None."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.polls = 0
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        self.polls += 1
        return self._messages.pop(0) if self._messages else None

    def close(self):
        self.closed = True


def test_consume_kafka_decodes_obgx_wire_and_emits_signals(tmp_path):
    # obgx wire format: one Arrow IPC batch per message, int64 sid as emitted.
    # The legacy 239 shape can also appear on the topic (bridged pre-v260827
    # stream) — the consumer must tolerate both.
    legacy_wire_schema = pa.schema(
        [
            pa.field("sid", pa.int64()),
            *[MINUTEBAR_SCHEMA.field(n) for n in MINUTEBAR_SCHEMA.names if n != "sid"],
        ]
    )
    payloads = [
        serialize_row_batch({**GRAPH_ROWS[0], "sid": 2_600_000}),
        _wire_payload([{**MINUTEBAR_ROWS[1], "sid": 2_000_001}], legacy_wire_schema),
    ]
    consumer = _FakeKafkaConsumer([_FakeKafkaMessage(p) for p in payloads])
    sink = JsonlSink(str(tmp_path / "signals.jsonl"))

    n_bars, n_signals = consume_kafka(
        lambda cfg: consumer,
        brokers="localhost:9092",
        topic="cn.bars.3m",
        group="test-group",
        consumer_config=None,
        signal_fn=compute_signal,
        sinks=[sink],
        max_messages=2,
    )
    sink.close()

    assert consumer.subscribed == ["cn.bars.3m"]
    assert consumer.polls == 2  # max_messages caps the loop
    assert consumer.closed
    assert n_bars == 2 and n_signals == 2  # bars-mean alpha on both schemas
    rows = [json.loads(l) for l in (tmp_path / "signals.jsonl").read_text().splitlines()]
    assert [r["sid"] for r in rows] == [2_600_000, 2_000_001]
    assert all(r["signal"]["meta"] == "bars-mean" for r in rows)


def test_consume_kafka_error_message_raises(tmp_path):
    consumer = _FakeKafkaConsumer([_FakeKafkaMessage(b"", error="transport down")])

    with pytest.raises(RuntimeError, match="transport down"):
        consume_kafka(
            lambda cfg: consumer,
            brokers="x",
            topic="t",
            group="g",
            consumer_config=None,
            signal_fn=compute_signal,
            sinks=[JsonlSink(str(tmp_path / "signals.jsonl"))],
            max_messages=1,
        )
    assert consumer.closed  # consumer is released even on error


def test_consume_batches_signal_failure_does_not_kill(tmp_path):
    batch = pa.Table.from_pylist(GRAPH_ROWS, schema=GRAPH_SCHEMA).to_batches()[0]
    sink = JsonlSink(str(tmp_path / "signals.jsonl"))

    def bad_signal(bars, sym):
        raise RuntimeError("boom")

    n_bars, n_signals = consume_batches([batch], bad_signal, [sink])
    sink.close()
    assert n_bars == 1 and n_signals == 0
    # bar still lands (signal is optional metadata)
    rows = [json.loads(l) for l in (tmp_path / "signals.jsonl").read_text().splitlines()]
    assert rows[0]["n_trade"] == 12.0
