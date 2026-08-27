import json
import math

import pytest

from live.engine import Bar
from live.sinks import (
    JsonlSink,
    KafkaSink,
    ParquetSink,
    deserialize_row_batch,
    serialize_row_batch,
)


def _bar(date=20220104, endtime=34200000000000, **leaves):
    return Bar(sid=2_600_000, date=date, endtime=endtime, leaves=leaves)


LEAVES = {"n_trade": 5.0, "qty_xyz": math.nan, "vol_neg": -0.0}


def test_jsonl_row_and_dedupe(tmp_path):
    p = tmp_path / "out.jsonl"
    sink = JsonlSink(str(p))
    bar = _bar(**LEAVES)
    sink.write_bar(bar, {"score": 0.5, "direction": 1, "meta": "demo"})
    sink.write_bar(bar, {"score": 0.5, "direction": 1, "meta": "demo"})  # deduped
    sink.close()
    lines = p.read_text().strip().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["sid"] == 2_600_000 and row["date"] == 20220104
    assert row["endtime"] == 34200000000000
    assert row["n_trade"] == 5.0
    assert row["qty_xyz"] is None  # NaN → null
    assert row["vol_neg"] == -0.0
    assert row["signal"] == {"score": 0.5, "direction": 1, "meta": "demo"}


def test_parquet_roundtrip_and_overwrite_guard(tmp_path):
    sink = ParquetSink(str(tmp_path), freq="3m")
    sink.write_bar(_bar(**LEAVES))
    sink.close()  # writes day file
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "3m" / "20220104.parquet"
    t = pq.read_table(path)
    assert t.num_rows == 1
    assert t.schema.names[:3] == ["sid", "date", "endtime"]
    assert t.schema.field("sid").type == pa.int64()
    assert t.column("n_trade")[0].as_py() == 5.0

    sink2 = ParquetSink(str(tmp_path), freq="3m")
    sink2.write_bar(_bar(**LEAVES))
    with pytest.raises(FileExistsError):
        sink2.close()


def test_kafka_wire_format_and_fake_producer():
    produced = []

    class FakeProducer:
        def __init__(self, config):
            produced.append(("config", config))

        def produce(self, topic, key, value):
            produced.append((topic, key, value))

        def poll(self, timeout):
            pass

        def flush(self):
            pass

    sink = KafkaSink(
        "localhost:9092",
        "cn.bars.3m.live",
        key_by="sid",
        producer_factory=FakeProducer,
    )
    sink.write_bar(_bar(**LEAVES))
    sink.close()
    assert produced[0][0] == "config"
    assert produced[0][1] == {"bootstrap.servers": "localhost:9092"}
    topic, key, payload = produced[1]
    assert topic == "cn.bars.3m.live"
    assert key == b"20220104:2600000"
    batch = deserialize_row_batch(payload)
    assert batch.num_rows == 1
    assert batch.schema.names[:3] == ["sid", "date", "endtime"]
    assert batch.column("n_trade")[0].as_py() == 5.0


def test_serialize_matches_obgx():
    """Byte-identical wire format with obgx kafka_sink.serialize_record_batch."""
    obg = pytest.importorskip("obg.kafka_sink")
    row = _bar(**LEAVES).to_row()
    import pyarrow as pa

    batch = pa.Table.from_pylist([row]).to_batches()[0]
    ours = serialize_row_batch(row)
    theirs = obg.serialize_record_batch(batch)
    assert ours == theirs
