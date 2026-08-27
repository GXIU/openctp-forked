# Experiments log — 2026-08-27 (live tick → 3m graph bars → signal)

All experiments from the live-service session. Commands are runnable as
written (paths assume this box). `/tmp` artifacts are ephemeral; the
permanent code paths are `live/`, `scripts/`, `tests/`.

## 0. Environment constraints (discovered the hard way)

| # | Experiment | Result |
|---|-----------|--------|
| E0a | `openctp-ctp` wheel under CPython 3.13 | **Segfaults** (struct construction / API creation). Works under 3.12 → two venvs: `.venv` (3.12, bindings) + `.venv-live` (3.13, obgx) |
| E0b | Import `openctp_ctp` without `zh_CN.GB18030` locale | Aborts: `locale::facet::_S_create_c_locale name not valid` (the .so hard-codes `codecvt_byname("zh_CN.GB18030")`). Fix: `sudo locale-gen zh_CN.GB18030` |
| E0c | TCP to TTS 7×24 front `180.168.146.187:10131` from this GCP box | Unreachable (openctp.cn HTTP fine) → live broker tests must run on a China host |

## 1. Graph evaluation feasibility

| # | Experiment | Result |
|---|-----------|--------|
| E1a | `ASTGraph.load_flat(obgx study/alpha/cqr_bar.yaml)` + run on tick_table window | Works — 126 leaves (obgx graph is **stale**; user pointed to alfa v260823) |
| E1b | Same with `alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml` | 252 leaves, input fields = the 9 research fields (ask/bid_price_0, event, last_price, level_placed, price, sign, vol_placed, vol_traded) |
| E1c | Units check: cqr_cn.py feeds the graph **raw** tick_table columns | Prices in cents, volumes in shares — no ÷100 anywhere. Live engine therefore carries research units raw |

## 2. Live Python engine (`live/`) vs research / production

| # | Experiment | Result |
|---|-----------|--------|
| E2a | Full-day replay sh603019/20220104 (tick_table) through live engine, graph = alfa v260823 | **81 bars**; window 0's 252 leaves **bit-faithful** to direct `ASTGraph.run` on the same slice (0 mismatches) — `tests/test_engine.py` pins this |
| E2b | Live engine (Python graph eval) vs `cn_rst3 bars` (Rust), sh600000/20220104, 81×252 cells | **95.2% exact**; deltas are small count/qty (n_bid 635 vs 636) = documented Python-vs-Rust operator divergence |
| E2c | Local production backfill: `cn_rst3 bars --config study/alpha/run_config_cqr_test.yaml --date 20220104 --graph alfa v260823 --output /tmp/bars_cqr_v260823` | 8,100 bars / 100 stocks / 71s from place/trade parquet (canonical static-data path) |

## 3. Rust streaming path

| # | Experiment | Result |
|---|-----------|--------|
| E3a | `cn_rst3 stream-real --stocks 20 --bars-ipc` (pre-v260827, 239 MinuteBar product) | 1.87M msgs → 4,780 bars / 20 symbols / 6.8s on the Arrow IPC channel |
| E3b | Pipe it into `live.ipc_consumer --stdin` | 4,780 bars → 4,780 signals — the consumer chain works on the live stream |
| E3c | `synth --date 20220104 --data-dir .../raw/parquet --stocks 10` | Selected **0 stocks** — the synth feed generator reads `.csv` only (parquet era gap); stream-real (parquet-first reader) is the path |

## 4. v260827: stream graph eval (the clarified request, landed by user)

| # | Experiment | Result |
|---|-----------|--------|
| E4a | obgx `git fetch`: commits `16ad38c` (feat: --graph/--freq/--bar-dtype on stream/stream-real), `519f13c` (design doc incl. pre-flagged fidelity risk), `a0f5ad4` (release bump); tag `v260827`; registry image `tick-to-bar:v260827` exists | Landed |
| E4b | Build cn_rst3 clean at `2ca25f2` from a git worktree (user's local WIP untouched); `stream-real --help` | `--graph`, `--freq`, `--bar-dtype` present |
| E4c | `stream-real --stocks 100 --graph alfa v260823 --freq 3m --bars-ipc` | 7.46M msgs → **8,100 graph bars / 252 leaves / 17.7s** |
| E4d | **Acceptance gate**: stream vs `cn_rst3 bars` (same binary/graph/date), float32 matched | **93.33% cells exact** (135,993 / 2,041,200 mismatched) |
| E4e | Mismatch localization | first-100 universe all-SH; ~70% of rows mismatch in every continuous window (auction 09:30 only 37%); trend/grouping leaves (`n_*_tks_*`, `qtysum_*_in_*_eq_trend`) dominate |
| E4f | Interpretation | Design doc's pre-flagged fidelity risk realized: streaming emits rows once at creation; batch `ShardReplay` mutates/re-emits across window boundaries (sweep-confirm re-stamps). Gate says the per-symbol replay-state port **is needed** — user's WIP (+446 lines) is presumably it |

## 5. Linked pipeline (current end state)

| # | Experiment | Result |
|---|-----------|--------|
| E5a | `stream-real --stocks 20 --graph cqr --freq 3m --bars-stdout-ipc \| ipc_consumer --stdin` | 1,620 bars (20×81) / 252 leaves → 1,620 signals (demo-trend at the time) |
| E5b | Signal swap: **bars' mean = alpha** (user direction; lp/cc placeholder) | `compute_signal` = mean of non-NaN leaf values; `meta: bars-mean` |
| E5c | Kafka link inventory | obgx ships both ends: `bridge_stdout_to_kafka` (IPC stdout → topic, key `{date}:{sid}`) + `ParquetArchiver`/`OrderGateway`; our `KafkaSink` is **byte-identical** to `obg.kafka_sink.serialize_record_batch` (tested) |
| E5d | **Together-test**: `tests/test_integration_stream.py` — one process chain, real data | 3 stocks → 243 bars / 252 leaves → bars-mean signals, **5.1s**, ordered+unique per sid |
| E5e | Wrapper: `scripts/run_stream_pipeline.sh [DATE] [STOCKS] [GRAPH] [OUT]` | Runs the whole chain as one command (CN_RST3/DATA_DIR env-overridable) |

## 6. Later in the day — Kafka consume mode + gate as tools

| # | Experiment | Result |
|---|-----------|--------|
| E6a | Kafka consume mode in `live/ipc_consumer.py` (`--kafka-consume`): subscribe topic, decode one Arrow IPC batch per message, route through signal/sinks | Landed; offline-tested with fake consumer (graph + legacy 239 payloads, error close, poll cap) — 6 tests |
| E6b | Gate as a tool: `scripts/gate_stream_vs_batch.py` (E4d rerunnable, `--expect-exact`/`--json`) | **GREEN**: 3 stocks → 243 rows / 61,236 cells → 100.0% exact; 100 stocks → 8,100 rows / **2,041,200 cells → 100.0% exact, 0 mismatched** — with the release binary the user rebuilt 2026-08-27 15:07 (their WIP). The 93.33% was specific to the v260827 build @2ca25f2; the replay-state port closed it |
| E6c | `tests/test_gate.py` — gate machinery end to end (13s at 3 stocks), percentage deliberately not asserted | Passed |

## 7. Open items

1. **Gate re-confirmation after obgx commits** — the green was measured on an uncommitted-WIP binary (15:07 rebuild); re-run `--stocks 100 --expect-exact` after every rebuild/commit.
2. **TORA live feed** — ToraSource scaffold awaits the official 华鑫奇点 SDK (n-sight account); China-host run.
3. **lp/cc substitution** — bars-mean alpha is the placeholder; the real chain is bars → lp (features) → cc (yhat).
4. **Live broker run** — the whole chain needs a China host once a broker/live feed exists (Kafka consume mode means the signal stage can live anywhere that reaches the brokers).
