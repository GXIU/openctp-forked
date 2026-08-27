"""Emit backends: JSONL (default), parquet per date, Kafka (obgx-compatible).

Kafka wire format mirrors obgx `kafka_sink.py` byte-for-byte: each message
value is a self-contained Arrow IPC stream holding exactly one record batch;
each message key is "{date}:{key_value}" (key_by column, `sid` by default).
obgx's kafka_consumers (ParquetArchiver / OrderGateway) can consume this
without modification.
"""

import json
import logging
import math
from abc import ABC, abstractmethod
from pathlib import Path

from .engine import Bar

LOG = logging.getLogger("live.sinks")


class Sink(ABC):
    @abstractmethod
    def write_bar(self, bar: Bar, signal: dict | None = None) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


def bar_row(bar: Bar, signal: dict | None = None) -> dict:
    """sid/date/endtime + leaves; `signal` merged in for JSONL only."""
    row = bar.to_row()
    if signal is not None:
        row["signal"] = signal
    return row


def _nan_to_null(obj):
    """Recursively map float('nan') → None so JSONL emits null, not NaN."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_null(v) for k, v in obj.items()}
    return obj


class JsonlSink(Sink):
    """Append-mode JSONL; dedupes repeated (date, sid, endtime) rows."""

    def __init__(self, path: str, *, dedupe: bool = True) -> None:
        self.path = Path(path)
        self.dedupe = dedupe
        self._seen: set[tuple[int, int, int]] = set()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")  # noqa: SIM115

    def write_bar(self, bar: Bar, signal: dict | None = None) -> None:
        row = bar_row(bar, signal)
        if self.dedupe:
            key = (row["date"], row["sid"], row["endtime"])
            if key in self._seen:
                return
            self._seen.add(key)
        # NaN → null; -0.0 stays -0.0 (matches cn_rst3 volume/amount sentinel).
        self._fh.write(json.dumps(_nan_to_null(row)) + "\n")
        self._fh.flush()

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()


class ParquetSink(Sink):
    """Buffer rows per date; write {dir}/{freq}/{date}.parquet at day close.

    Schema matches cn_rst3 on-disk order: sid/date/endtime int64, then the
    graph leaves (sorted leaf-name order) as float64. `signal` is NOT written
    (keeps files cn_rst3-compatible).
    """

    def __init__(self, out_dir: str, *, freq: str = "3m", overwrite: bool = False) -> None:
        self.out_dir = Path(out_dir)
        self.freq = freq
        self.overwrite = overwrite
        self._buffer: dict[int, list[dict]] = {}

    def write_bar(self, bar: Bar, signal: dict | None = None) -> None:
        self._buffer.setdefault(bar.date, []).append(bar.to_row())

    def write_day(self, date: int) -> None:
        rows = self._buffer.pop(date, None)
        if not rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = self.out_dir / self.freq / f"{date}.parquet"
        if path.exists() and not self.overwrite:
            raise FileExistsError(f"{path} exists and overwrite=False")
        path.parent.mkdir(parents=True, exist_ok=True)
        leaf_names = [k for k in rows[0] if k not in ("sid", "date", "endtime")]
        schema = pa.schema(
            [
                pa.field("sid", pa.int64()),
                pa.field("date", pa.int64()),
                pa.field("endtime", pa.int64()),
                *[pa.field(name, pa.float64()) for name in leaf_names],
            ]
        )
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, path)
        LOG.info("wrote %s (%d rows)", path, len(rows))

    def close(self) -> None:
        for date in list(self._buffer):
            self.write_day(date)


class KafkaSink(Sink):
    """One message per closed bar, obgx wire format. Lazy confluent-kafka."""

    def __init__(
        self,
        brokers: str,
        topic: str,
        *,
        key_by: str = "sid",
        producer_config: dict | None = None,
        producer_factory=None,
    ) -> None:
        self.brokers = brokers
        self.topic = topic
        self.key_by = key_by
        self.producer_config = producer_config or {}
        self._factory = producer_factory or _default_producer_factory
        self._producer = None
        self._n = 0

    def write_bar(self, bar: Bar, signal: dict | None = None) -> None:
        if self._producer is None:
            self._producer = self._factory(
                {"bootstrap.servers": self.brokers, **self.producer_config}
            )
        row = bar.to_row()
        key = f"{row['date']}:{row[self.key_by]}".encode()
        payload = serialize_row_batch(row)
        self._producer.produce(self.topic, key=key, value=payload)
        self._n += 1
        if self._n % 64 == 0:
            self._producer.poll(0)

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()


def serialize_row_batch(row: dict) -> bytes:
    """One record batch (single row) as an Arrow IPC stream — obgx format."""
    import pyarrow as pa

    leaf_names = [k for k in row if k not in ("sid", "date", "endtime")]
    arrays = [
        pa.array([row["sid"]], type=pa.int64()),
        pa.array([row["date"]], type=pa.int64()),
        pa.array([row["endtime"]], type=pa.int64()),
        *[pa.array([row[name]], type=pa.float64()) for name in leaf_names],
    ]
    batch = pa.record_batch(arrays, names=["sid", "date", "endtime", *leaf_names])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def deserialize_row_batch(payload: bytes):
    """Decode a payload produced by :func:`serialize_row_batch` (test helper)."""
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(payload)) as reader:
        batches = list(reader)
    if len(batches) != 1:
        raise ValueError(f"expected exactly one record batch, got {len(batches)}")
    return batches[0]


def _default_producer_factory(config: dict):
    """Build a confluent_kafka.Producer — lazy import like obg kafka_sink."""
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise ImportError(
            "KafkaSink requires confluent-kafka: uv pip install --python "
            ".venv-live/bin/python confluent-kafka"
        ) from exc
    return Producer(config)


def make_sinks(cfg) -> list[Sink]:
    sinks: list[Sink] = []
    if cfg.jsonl.enabled:
        sinks.append(JsonlSink(cfg.jsonl.path, dedupe=cfg.jsonl.dedupe))
    if cfg.parquet.enabled:
        sinks.append(
            ParquetSink(cfg.parquet.dir, freq=cfg.parquet.freq, overwrite=cfg.parquet.overwrite)
        )
    if cfg.kafka.enabled:
        sinks.append(
            KafkaSink(
                cfg.kafka.brokers,
                cfg.kafka.topic,
                key_by=cfg.kafka.key_by,
                producer_config=cfg.kafka.producer_config,
            )
        )
    return sinks
