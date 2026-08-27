"""YAML config → frozen dataclasses for the live service."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .instruments import Instrument, normalize_instrument


@dataclass(frozen=True)
class SourceConfig:
    mode: str  # "tora" | "replay" | "mdapi"
    front: str = ""
    account_user: str = ""
    account_password: str = ""
    env: str = "sim"  # sim | live
    replay_dir: str = ""
    replay_dates: tuple[str, ...] = ()
    replay_fmt: str = "parquet"  # parquet | csv


@dataclass(frozen=True)
class BarsConfig:
    freq_min: int = 3
    emit_empty_windows: bool = True


@dataclass(frozen=True)
class GraphConfig:
    path: str = ""
    min_samples: int = 0  # rows below this → all-NaN leaves (research guard)


@dataclass(frozen=True)
class SignalConfig:
    module: str = "live.signal"
    function: str = "compute_signal"


@dataclass(frozen=True)
class JsonlSinkConfig:
    enabled: bool = True
    path: str = "bars/live.jsonl"
    dedupe: bool = True


@dataclass(frozen=True)
class ParquetSinkConfig:
    enabled: bool = False
    dir: str = "bars/parquet"
    freq: str = "3m"
    overwrite: bool = False


@dataclass(frozen=True)
class KafkaSinkConfig:
    enabled: bool = False
    brokers: str = "localhost:9092"
    topic: str = "cn.bars.3m.live"
    key_by: str = "sid"
    producer_config: dict = field(default_factory=dict)


@dataclass(frozen=True)
class SinksConfig:
    jsonl: JsonlSinkConfig = field(default_factory=JsonlSinkConfig)
    parquet: ParquetSinkConfig = field(default_factory=ParquetSinkConfig)
    kafka: KafkaSinkConfig = field(default_factory=KafkaSinkConfig)


@dataclass(frozen=True)
class LiveConfig:
    instruments: tuple[Instrument, ...]
    source: SourceConfig
    bars: BarsConfig = field(default_factory=BarsConfig)
    graph: GraphConfig = field(default_factory=GraphConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    sinks: SinksConfig = field(default_factory=SinksConfig)
    log_level: str = "INFO"


_SOURCE_KEYS = {"mode", "front", "account_user", "account_password", "env", "replay"}
_BARS_KEYS = {"freq_min", "emit_empty_windows"}
_GRAPH_KEYS = {"path", "min_samples"}
_SIGNAL_KEYS = {"module", "function"}
_JSONL_KEYS = {"enabled", "path", "dedupe"}
_PARQUET_KEYS = {"enabled", "dir", "freq", "overwrite"}
_KAFKA_KEYS = {"enabled", "brokers", "topic", "key_by", "producer_config"}


def _check_keys(d: dict, allowed: set, where: str) -> None:
    unknown = set(d) - allowed
    if unknown:
        raise ValueError(f"unknown keys in {where}: {sorted(unknown)}")


def load_config(path: str | Path) -> LiveConfig:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    raw_src = raw.get("source") or {}
    _check_keys(raw_src, _SOURCE_KEYS, "source")
    src = SourceConfig(
        mode=raw_src.get("mode", "replay"),
        front=raw_src.get("front", ""),
        account_user=raw_src.get("account", {}).get("user", ""),
        account_password=raw_src.get("account", {}).get("password", ""),
        env=raw_src.get("env", "sim"),
        replay_dir=raw_src.get("replay", {}).get("dir", ""),
        replay_dates=tuple(str(d) for d in raw_src.get("replay", {}).get("dates", [])),
        replay_fmt=raw_src.get("replay", {}).get("fmt", "parquet"),
    )
    if src.mode not in ("tora", "replay", "mdapi"):
        raise ValueError(f"bad source.mode {src.mode!r}")

    raw_bars = raw.get("bars") or {}
    _check_keys(raw_bars, _BARS_KEYS, "bars")
    bars = BarsConfig(**raw_bars)

    raw_graph = raw.get("graph") or {}
    _check_keys(raw_graph, _GRAPH_KEYS, "graph")
    graph = GraphConfig(**raw_graph)
    if not graph.path:
        raise ValueError("graph.path is required (bar YAML with feature_generator key)")

    raw_signal = raw.get("signal") or {}
    _check_keys(raw_signal, _SIGNAL_KEYS, "signal")
    signal = SignalConfig(**raw_signal)

    raw_sinks = raw.get("sinks") or {}
    _check_keys(raw_sinks, {"jsonl", "parquet", "kafka"}, "sinks")
    raw_jsonl = raw_sinks.get("jsonl") or {}
    _check_keys(raw_jsonl, _JSONL_KEYS, "sinks.jsonl")
    raw_parquet = raw_sinks.get("parquet") or {}
    _check_keys(raw_parquet, _PARQUET_KEYS, "sinks.parquet")
    raw_kafka = raw_sinks.get("kafka") or {}
    _check_keys(raw_kafka, _KAFKA_KEYS, "sinks.kafka")
    sinks = SinksConfig(
        jsonl=JsonlSinkConfig(**raw_jsonl),
        parquet=ParquetSinkConfig(**raw_parquet),
        kafka=KafkaSinkConfig(**raw_kafka),
    )

    raw_instruments = raw.get("instruments") or []
    if not raw_instruments:
        raise ValueError("instruments list is required")
    instruments = tuple(normalize_instrument(i) for i in raw_instruments)

    return LiveConfig(
        instruments=instruments,
        source=src,
        bars=bars,
        graph=graph,
        signal=signal,
        sinks=sinks,
        log_level=raw.get("log_level", "INFO"),
    )
