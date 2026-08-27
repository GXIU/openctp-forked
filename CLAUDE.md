# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A fork of [openctp/openctp](https://github.com/openctp/openctp) — a broker-connectivity/execution layer for Chinese markets. The core trick: every broker's native SDK is wrapped behind the **CTPAPI** interface (上期技术's futures API, the de-facto standard), so one CTP-style codebase reaches any broker by swapping the shared library. For CN equities, orders go through a broker counter (XTP/TORA/EMT/OST), never directly to SSE/SZSE.

This fork is used as the execution layer for the obgx tick-research stack: signals computed from obgx bars / alfa graphs / cc models flow out through these bindings. The no-account testing ground is the TTS 7×24 sim (`TTS-CTPAPI`), which speaks CTPAPI natively and supports stocks via the extended `ProductClass 'E'`.

## Live bars service (`live/`) — tick → bar graph features → signal

The repo also hosts a live service: ingest tick-level quote+trade data, aggregate into the same session-aware 3m bar windows as obgx `cn_rst3`, **evaluate the actual alfa bar graph** (e.g. `alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml`) per window via obgx's Python evaluator, emit leaf rows, run a pluggable signal.

**Two venvs — interpreter conflict is real, not a preference:**
- `.venv` (Python **3.12**) — openctp-ctp SWIG wheel segfaults on 3.13. For bindings work only (`smoke_md.py`, ctpcommand).
- `.venv-live` (Python **3.13**) — obgx requires >=3.13. The live service runs here: deps in `live-requirements.txt`; created by `uv venv .venv-live --python 3.13 && uv pip install --python .venv-live/bin/python -r live-requirements.txt`.

**Run:**
```bash
.venv-live/bin/python -m pytest tests/ -q                    # all offline (25 tests, ~30s)
.venv-live/bin/python -m live.run --check-grid              # print the 81 endtimes
.venv-live/bin/python -m live.run --config configs/live_replay.yaml   # offline replay
.venv-live/bin/python -m live.run --config configs/live_tora.yaml     # live (China host)
.venv-live/bin/python -m ruff check live/ tests/ scripts/
scripts/run_stream_pipeline.sh [DATE] [STOCKS] [GRAPH] [OUT_JSONL]    # Rust stream → signal, one command
.venv-live/bin/python scripts/gate_stream_vs_batch.py [--stocks N] [--expect-exact]  # stream-vs-batch gate
```

**Bar conventions replicated from cn_rst3** (`live/grid.py`): 81 endtimes for 3m (09:30 pinned … 11:30; 13:03 … 14:57; 15:00 — no 13:00 bar), bar labeled by endtime in ns-past-midnight, `partition_point` window assignment (tick at exactly HH:MM:00 → next window; lunch-gap ticks attach to the first afternoon bar; ticks ≥15:00 clamp into the close bar — verbatim `BarGrid::window_index`). Empty windows emit all-NaN leaves. Pinned by `tests/test_grid.py`.

**Research-unit invariant:** `Event` carries research units — prices in **cents**, volumes in **shares** (cqr_cn.py feeds raw tick_table columns to the graph). `ReplaySource` passes tick_table values through raw and marks `has_book=True` so genuine 0.0 book values survive (bit-fidelity). The engine's full-day replay is verified **bit-faithful**: window 0's 252 leaves equal a direct `ASTGraph.run` on the same slice exactly (`test_full_day_replay_81_bars_bit_faithful`).

**place/ + trade/ can NOT feed cqr_bar.yaml directly.** The graph reads reconstructed book columns (`bid_price_0/ask_price_0`, `level_placed`) that place/trade reorg files don't contain — cn_rst3's ShardReplay reconstructs the book first. The tick_table on disk (`/mnt/disks/data/cn/tick_table`) IS that reconstructed stream (cn_rst3 tick export). Chain: place/+trade/ → book reconstruction (cn_rst3) → tick_table → cqr_bar.yaml → bars. Live TORA L2 provides 逐笔+book natively, so no reconstruction is needed in the live path.

**Canonical obgx commands for static bar generation** (from the obgx repo, its README + k8s yamls — use these, don't reimplement):
```bash
# Production K8s backfill (one pod per date, GCS direct I/O, no PVCs):
#   uv run obgx-backfill-k8s k8s/cqr_bar_backfill.yaml [start-date] [end-date]
#   SKIP_BUILD=1 / IMAGE_ONLY=1 / PARALLELISM knobs; runs scripts/deploy_backfill.sh
# Local production equivalent (Rust cn_rst3, place/trade parquet in, bars out):
cd /home/george/code/obgx
/mnt/disks/data/cargo-target/release/cn_rst3 bars \
  --config study/alpha/run_config_cqr_test.yaml --date 20220104 \
  --graph /home/george/code/alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml \
  --output /tmp/bars_cqr_v260823 --version v260823test
# → {output}/{version}/3m/{date}.parquet   (verified 2026-08-27: 8100 bars / 100 stocks / 71s)
```
**Fidelity of the live engine vs production:** compared cellwise on sh600000/20220104 (81 windows × 252 leaves): **95.2% exact match** vs the Rust bar output; the rest are small count/qty deltas (e.g. `n_bid` 635 vs 636) — the documented Python-operators vs Rust-operators divergence ("Python and Rust have since diverged — ground truth is the raw tape", obgx README). The live engine matches *research-Python* semantics exactly (bit-faithful to `ASTGraph.run`, see above); production bar numbers come from Rust.

**Rerunnable fidelity gate (stream vs batch, cellwise):**
```bash
.venv-live/bin/python scripts/gate_stream_vs_batch.py            # 3 stocks, report
.venv-live/bin/python scripts/gate_stream_vs_batch.py --stocks 100 --expect-exact
# runs `cn_rst3 bars` + `stream-real --graph` on the same date/universe with the
# SAME binary, compares every leaf cell, reports % exact + worst windows + top
# mismatched leaves (--expect-exact exits 1 below 100.0000%).
# Status: 100.0% exact at 3 AND 100 stocks (2,041,200 cells) with the release
# binary the user rebuilt 2026-08-27 15:07 (their WIP) — the replay-state port
# closed the 93.33% seen with the v260827 build @2ca25f2. Re-run the gate after
# every obgx rebuild; the WIP is uncommitted, so the number needs re-confirmation
# once it lands.
```

**TORA live iteration (`alignment_ob/`)** — the M0→M5 on-ramp workspace (plan: `docs/tora_onramp.md`, log: `alignment_ob/README.md`). Offline-verified: the research tick_table **round-trips bit-faithfully** through capture → events → `alignment_ob.converters.events_to_tick_table` (30,598 rows × 83 cols, 100.0% exact — the property the live capture must reproduce); place/trade converters grounded on the real obgx parquet (OrdType 0 add / -1 cancel, yuan, ns-epoch); `alignment_ob.profiler.compare_tapes` = the M3 tape-level comparator. Blocked pieces: TORA SDK (n-sight browser download, steps in the README — account 00974579, SDK pins `API_C++_行情_v1.0.8_20241220` + `API_C++_交易_v4.1.3_20250415`, secrets in gitignored `alignment_ob/secrets.yaml`) and a China-reachable host for the fronts.

**Rust live-stream path (verified working 2026-08-27):** cn_rst3 itself streams — real data in, continuous Arrow IPC bars out, this repo's consumer reads them live:

```bash
# Production engine streams 20 stocks: 1.87M msgs → 4,780 minute bars → IPC in ~7s
cd /home/george/code/obgx
/mnt/disks/data/cargo-target/release/cn_rst3 stream-real \
  --date 20220104 --data-dir /mnt/disks/data/cn/raw/parquet --stocks 20 \
  --bars-ipc /tmp/stream_bars.ipc        # or --bars-stdout-ipc for a live pipe

# True live chain: Rust stream → Arrow IPC on stdout → signal consumer → JSONL
/mnt/disks/data/cargo-target/release/cn_rst3 stream-real --date 20220104 \
  --data-dir /mnt/disks/data/cn/raw/parquet --stocks 20 --bars-stdout-ipc 2>/dev/null \
  | PYTHONPATH=$(pwd) .venv-live/bin/python -m live.ipc_consumer --stdin --out /tmp/signals.jsonl
# → 4,780 bars → 4,780 signals (verified)
```
`live/ipc_consumer.py` maps the streamed bar IPC rows into `live.Bar`s and runs the pluggable `live.signal` per closed bar — the same signal interface the `live.run` engine uses. It auto-detects both producer schemas: graph mode (v260827+: `sid, date, endtime` + leaves) and legacy 239 MinuteBar (`window_start/window_end/row_count` + OHLCV). Sinks: `--jsonl` (default), `--parquet-dir`, `--kafka-brokers/--kafka-topic` (KafkaSink, obgx-wire byte-compatible).

**Kafka is the intended bus** (user-confirmed direction): obgx ships both ends — `obg.kafka_consumers.bridge_stdout_to_kafka(proc_stdout, topic, ...)` forwards the cn_rst3 stdout IPC stream into Kafka (one Arrow IPC batch per message, key `{date}:{sid}`), and `ParquetArchiver`/`OrderGateway` consume it. Topologies: (a) stream `--bars-stdout-ipc` → obgx bridge → topic → [ParquetArchiver, OrderGateway, live consumer]; (b) stream → `live.ipc_consumer --kafka-*` → same topic (consumer doubles as signal stage + producer); (c) direct pipe, no Kafka (`--stdin`). All three are wired: `ipc_consumer` now also has a **consume mode** —

```bash
# Kafka bus: bridge the stream (obgx) → topic → consume HERE → signal → sinks
.venv-live/bin/python -m live.ipc_consumer --kafka-consume \
  --kafka-consume-brokers localhost:9092 --kafka-consume-topic cn.bars.3m \
  --kafka-consumer-group live.ipc_consumer --jsonl bars/kafka_signals.jsonl
# (--kafka-consume-max N caps messages for debug; --kafka-consumer-config JSON
#  adds confluent-kafka options; producer/sink flags unchanged)
```
`consume_kafka` decodes one Arrow IPC batch per message (any producer schema — graph or legacy 239, e.g. a bridged pre-v260827 stream), polls forever in production, and is tested offline with an injected fake consumer (same pattern as `KafkaSink.producer_factory`).

**Verified linked chain (2026-08-27) — run it as one command:**
```bash
scripts/run_stream_pipeline.sh [DATE] [STOCKS] [GRAPH_YAML] [OUT_JSONL]
# defaults: 20220104, 20 stocks, alfa cqr v260823, bars/live_stream.jsonl
# = cn_rst3 stream-real --graph --freq 3m --bars-stdout-ipc | live.ipc_consumer --stdin --jsonl
# (CN_RST3 / DATA_DIR env-overridable)
```
The signal is the **bars-mean alpha** (user direction: "you can assume that the bars' mean IS the alpha"): `compute_signal` = mean of the bar's non-NaN leaf values, `{"score", "direction", "meta": "bars-mean"}` — a placeholder for the real bars → lp (features) → cc (yhat) chain. The chain runs together: `tests/test_integration_stream.py` (skipif-guarded on binary/data/graph) — 3 stocks → 243 bars/252 leaves → signals, 5.1s; full suite is 30 tests. All session experiments: `docs/experiments.md`.
Notes: `stream-real` emits the **CN-239 minute-bar product** (10-column MinuteBar) when `--graph` is absent, and the **graph leaves** product when present; the `synth` feed generator (CSV-based, `--feed`) is the other stream input but reads `.csv` place/trade, not parquet. The TORA live adapter (ToraSource) remains the China-host path for real 逐笔 live feeds.

### Decisions log (recorded 2026-08-27; open items decide when unblocked)

**Made:**
1. Live feed = **TORA native L2** (`TORALEV2API`, official 华鑫奇点 SDK), not the CTP wrapper — the wrapper's mdapi only pushes 5-level `OnRtnDepthMarketData` snapshots; the user's requirement is tick-on-quote AND tick-on-trade (逐笔委托/成交), which only the native L2 module has (`Lev2OrderDetailField`, `Lev2TransactionField` with BSFlag, 10-level `Lev2MarketDataField`, resend streams — struct names confirmed via tora-rs bindings).
2. Live features = run the **alfa bar graph per window** (`ASTGraph.load_flat` + `run`) — leaves match research by construction.
3. Graph = `alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml` (252 leaves) — the production graph; obgx's `study/alpha/cqr_bar.yaml` (126 leaves) is stale.
4. Tick-driven bar closes; streaming per-window event buffers; rollover on trading-day change / time wrap / cumulative-volume reset (7×24 loop restarts).
5. Replay mode = the offline verification path (tick_table parquet); tests never import openctp/TORA SDKs.
6. Signal stage = **bars-mean alpha** (user direction): the 3m stream *provides* bars; downstream would be lp → cc, but for now alpha = mean of the bar's leaf values (NaN excluded). Swappable via `live/signal.py`; `meta: "bars-mean"` rows the placeholder.
7. Streams link by **Kafka** when brokers exist, plain pipe otherwise — both paths wired and byte-compatible.

**Open:**
- TORA live feed — full on-ramp plan in `docs/tora_onramp.md`: what to ask 华鑫奇点 for (n-sight account, L2 module form factor, 行情 tier, recording permission, historical 逐笔 dump, trading account), L2 → place/trade + tick_table converters, gate-style alignment profiling vs research tape, milestones M0 capture → M5 trade. **cqr_bar on live TORA data is correctly off the table until M3** (no SDK; and field fidelity must be profiled before trusting live leaves).
- `level_placed` unconfirmed in TORA structs → derive from order price vs book, or NaN (`_lle1` leaf variants only).
- The cc pipeline signal (full-universe cross-sectional features + fitted beta) **cannot run on a live single feed** — documented gap, not attempted.
- TTS `MdapiSource` kept as degraded fallback (snapshots only; event-count leaves 0/NaN) — works today with no account.
- Kafka sink is obgx-wire-compatible (Arrow IPC batch per message, key `{date}:{sid}`) but disabled by default.

**Environment constraints (hard-won):** Python 3.12-only wheel / 3.13-only obgx (hence two venvs); `zh_CN.GB18030` locale must exist on the host (binding .so hard-codes `codecvt_byname("zh_CN.GB18030")`, else import aborts); TTS/TORA fronts are China-reachable only — live smoke tests must run on a China host.

## Architecture: three kinds of directories

**Adapter dirs (XTP/TORA/EMT/OST/TTS/CTPOPT/QDP/FEMAS/TAP/IB/YD-CTPAPI)** — each is a *readme only*; the wrapper `.dll`/`.so` binaries are distributed via downloads (openctp.cn), not vendored here. The readme is the interface contract: front addresses, auth quirks, version pins. XTP/EMT also carry `dict.csv` — required in the runtime working directory because their native APIs have no instrument-query; the wrapper answers `ReqQryInstrument` from this file.

**Source adapters (QQ-CTPAPI, SINA-CTPAPI)** — quote-only mdapi implementations, built in-tree: CMake compiles `FtdcMdApiImpl.cpp` into `thostmduserapi_se` (boost include/lib paths are hard-coded: `/usr/local/include` + `/usr/local/lib` on Linux, `E:/boost_1_73_0` on Windows). Both exist to feed market data into CTP-style code without a broker.

**Language bindings** — `ctpapi-python` and `ctpopt-python` are built in-tree (SWIG); `ctpapi-c/java/go/rust/csharp` are readmes pointing at separate repos/pip packages.

## Python bindings (the actively used path)

`ctpapi-python` (CTPAPI 6.3.15) and `ctpopt-python` (CTPOPT 3.7.0) share the same shape:

```
<dir>/<API>/
  CMakeLists.txt        # runs swig, compiles wrap, links the vendor .so
  linux/ win32/ win64/  # prebuilt thosttraderapi_se.so / thostmduserapi_se.so + C++ headers
<dir>/thosttraderapi.i / thostmduserapi.i   # SWIG interface (trader = orders, md = quotes)
<dir>/demo/td_demo.py / md_demo.py          # interactive walk-throughs
```

Build flow (Linux): `swig -threads -c++ -python` on the `.i` files → `add_library(... SHARED thosttraderapi_wrap.cxx)` linked with `-Wl,-Bsymbolic` against `linux/thosttraderapi_se.so` → importable `thosttraderapi.py` / `_thosttraderapi.so`. Requires `swig`, `cmake`, `g++`, `python3-dev`, `boost` (`boost_locale`/`boost_system` — the locale lib does the charset conversion). All swig output, `*.so`, `*.pyd` are gitignored — fresh clones build clean.

Consumers normally don't build: `pip install openctp-ctp==6.3.15.*` (or `openctp-ctpopt==3.7.0.*`), typically via `-i https://pypi.tuna.tsinghua.edu.cn/simple`. Demos import `openctp_ctp.thosttraderapi` in pip mode, `thosttraderapi` in local mode.

### uv environment (this fork, already set up)

```bash
uv sync                  # .venv: Python 3.12, openctp-ctp (bindings work)
uv run python smoke_md.py [instrument]   # live tick smoke test vs TTS 7x24
uv run python ctpapi-python/demo/md_demo.py
# Live service (obgx graph eval) → the separate 3.13 venv — see the live/
# section at the top of this file for commands.
```

Hard constraints discovered the hard way (2026-08-27):

- **Python 3.12 only.** The SWIG wheel segfaults under CPython 3.13 (crashes at struct construction / API creation). `pyproject.toml` pins `>=3.12,<3.13` and **excludes this repo from the parent workspace** at `/home/george/code` (which requires >=3.13). Do not let `uv sync` at the workspace root re-absorb it.
- **`zh_CN.GB18030` locale must exist on the host.** The binding `.so` hard-codes `codecvt_byname("zh_CN.GB18030")` for GBK↔UTF-8 conversion of instrument/error strings; without it, *import itself* aborts with `std::runtime_error: locale::facet::_S_create_c_locale name not valid`. Fix: `echo 'zh_CN.GB18030 GB18030' | sudo tee -a /etc/locale.gen && sudo locale-gen zh_CN.GB18030`.
- **TTS front is not reachable from this GCP box** (`tcp://180.168.146.187:10131` times out; openctp.cn HTTP works). Live smoke tests must run from a China-reachable host.

### Build commands

```bash
# trader+md bindings for CTP futures/options
cmake -S ctpapi-python/CTPAPI -B ctpapi-python/CTPAPI/build && cmake --build ctpapi-python/CTPAPI/build
# stock-options (CTPOPT) bindings — same pattern
cmake -S ctpopt-python/CTPOPT -B ctpopt-python/CTPOPT/build && cmake --build ctpopt-python/CTPOPT/build
# QQ/SINA quote adapters
cmake -S QQ-CTPAPI -B QQ-CTPAPI/build && cmake --build QQ-CTPAPI/build
```

There is no top-level build system, no test suite, and no CI in this repo. Verification = running the demos against a reachable front.

### Runtime / interactive tools

```bash
# telnet-style console against any CTP-compatible counter (query instruments, funds, positions, orders)
python tools/ctpcommand/ctpcommand.py <front> <broker> <user> <password> <appid> <authcode>
# full auth→login→query→order-insert→cancel flow, step by step
python ctpapi-python/demo/td_demo.py      # and ctpopt-python/demo/td_demo.py for CTPOPT
```

The demo order flow is the canonical minimal sequence for any execution service built on this repo: `CreateFtdcTraderApi → RegisterSpi → RegisterFront → SubscribePrivate/PublicTopic(QUICK) → Init → ReqAuthenticate → ReqUserLogin → ReqOrderInsert/ReqOrderAction`. SPI callbacks are async — demos use a semaphore per callback to serialize.

**Tick updates (mdapi):** `CThostFtdcMdApi.RegisterFront(md_front) → Init → OnFrontConnected → ReqUserLogin → SubscribeMarketData([...])`, then ticks arrive asynchronously in the SPI callback `OnRtnDepthMarketData` (per-instrument snapshot: last, volume, bid/ask 1–5, update time). This is the broker quote feed (L1/L5 snapshots), **not** the raw 逐笔 order/event stream that obgx backtests consume — live full-L2 events for CN equities require a separate exchange/vendor data license, no broker API exposes them. See `smoke_md.py`.

## Adapter-specific gotchas (read the readme before touching)

These live in each adapter readme and have bitten real integrations:

- **XTP (中泰证券)**: assign `AppID` **before** `AuthCode` — XTP's AuthCode overflows CTP's field, and assigning AppID second clobbers the overflow. Wrapper uses fixed ClientID 80 (don't use 80 in other terminals). `dict.csv` must sit beside the .so. Linux wrappers target libc 2.17, XTP 2.2.36.1. Sim/live front addresses differ (7×24: `tcp://119.3.103.38:6002` md / `tcp://122.112.139.0:6101` td).
- **TTS (openctp sim)**: 7×24 sim supports stocks/bonds/funds/futures/options; the CTPAPI-extension enums (see `CTP接口扩展说明.md`) add `ProductClass 'E'/'B'/'D'/'I'` and ExchangeIDs `SSE/SZSE/BSE` for non-futures instruments.
- **EMT (东方财富)**: also needs its `dict.csv`.
- **Stock trading (any broker)**: requires a broker account + API authorization. TTS/N视界 are the sim-only paths.

## Key facts

- `.gitignore` excludes all build artifacts (`*.so`, `*.pyd`, swig `_wrap.*`, generated `thosttraderapi.py` etc.) — do not try to `git add` built bindings; the vendor `.so` files in `linux/`/`win32/`/`win64/` are committed and are the source of truth for linkage.
- `demo/ctpprint`, `demo/prices`, `widgets/ViTrader` are Windows-centric C++ examples/tools with `.vcxproj`/`Makefile`; ViTrader is the reference CLI trading client implementation.
- Master tracks upstream openctp; recent local commit `6731585` fixed a CMakeLists compile issue (SOL-001) — if CI/build breaks appear after merging upstream, the binding CMakeLists are the usual suspect.
- README.md (root, Chinese) is the canonical index: adapter list, sim environments, pip installs, ecosystem links.
