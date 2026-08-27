# Dev Log

## 2026-08-27 — live/ service: tick → alfa bar-graph features → signal; Rust stream chain verified

### What changed

- **uv setup with two venvs, by necessity not preference:** openctp-ctp's SWIG wheel segfaults under CPython 3.13 (verified empirically), obgx requires >=3.13. `.venv` (3.12) = bindings only; `.venv-live` (3.13, `live-requirements.txt`) = the live service. Repo excluded from the parent `/home/george/code` workspace (which pins >=3.13). Host needs `zh_CN.GB18030` locale generated — the binding .so hard-codes `codecvt_byname("zh_CN.GB18030")` and aborts at import without it.
- **`live/` package** — the live pipeline:
  - `grid.py` — CN 3m bar grid replicated from cn_rst3 (`BarGrid::window_index` partition_point semantics, 81 endtimes, no 13:00 bar, unconditional 15:00 clamp; pinned by tests).
  - `events.py` — normalized events in **research units** (prices cents, volumes shares — cqr_cn.py feeds raw tick_table columns); `has_book` flag so genuine 0.0 book values survive verbatim.
  - `engine.py` — per-instrument per-window event buffers → `ASTGraph.load_flat(graph.yaml)` + `run()` at bar close; rollover on trading-day change / time wrap / cumulative-volume reset (7×24 loop restarts); empty windows emit NaN leaves.
  - `source.py` — ReplaySource (tick_table parquet, raw passthrough), ToraSource scaffold (TORA L2 逐笔 structs mapped; blocked on official SDK), degraded MdapiSource (TTS snapshots, 3.12 venv).
  - `signal.py` (pluggable demo), `sinks.py` (JSONL with NaN→null, parquet per date, Kafka byte-compatible with obgx `kafka_sink`), `ipc_consumer.py` (reads cn_rst3's live Arrow IPC bar stream → signal), `run.py` CLI.
- **Graph:** production alfa graph `alfa/cn/yaml/spd/cqr/v260823/cqr_bar.yaml` (252 leaves). obgx's `study/alpha/cqr_bar.yaml` (126 leaves) is stale — user correction, recorded.

### Verification numbers

- Live engine replay (sh603019/20220104, tick_table): **81 bars**, window-0 leaves **bit-faithful** to direct `ASTGraph.run` (252/252 leaves, 0 mismatches).
- Live engine vs production Rust (`cn_rst3 bars`, same stock/date, 81 windows × 252 leaves): **95.2% cells exact**; rest are small count/qty deltas (documented Python-vs-Rust operator divergence — Rust is tape ground truth).
- **Rust live-stream chain** (the production live path): `cn_rst3 stream-real --date 20220104 --data-dir /mnt/disks/data/cn/raw/parquet --stocks 20 --bars-stdout-ipc | live.ipc_consumer --stdin` → 1,870,735 msgs → 4,780 minute bars (239 × 20) → 4,780 signals in ~7s.
- Local production backfill equivalent: `cn_rst3 bars --config study/alpha/run_config_cqr_test.yaml --date 20220104 --graph <alfa v260823> --output /tmp/...` → 8,100 bars / 100 stocks / 71s.
- 28 tests green; ruff clean.

### Lessons

- **Read obgx's README before reimplementing anything bar-related.** `obgx-backfill-k8s k8s/cqr_bar_backfill.yaml [start] [end]` is the canonical deploy; `cn_rst3 bars` its local equivalent; cn_rst3's `stream-real`/`synth` are the streaming engines. The place/trade → book reconstruction → tick_table → graph chain is all inside cn_rst3.
- place/+trade/ reorg parquet cannot feed cqr_bar.yaml directly — the graph needs reconstructed book columns (bid/ask L0, level_placed), which only the book engine produces.
- mdapi (CTP wrapper, any broker) is snapshots-only. Tick-on-quote AND tick-on-trade requires the native L2 SDK (TORA `lev2md`: OrderDetail/Transaction/MarketData + resend streams, confirmed via tora-rs docs).
- Sources must never import openctp/TORA SDKs at module import time (tests run without them; 3.13 segfaults on the openctp wheel).
- 7×24 sim loop restarts are handled by rollover (trading-day change / time wrap / cum-volume reset) + JSONL dedupe.

### Open items (decisions log — see CLAUDE.md for full rationale)

1. ToraSource SDK plumbing — blocked on 华鑫奇点 SDK download (n-sight account). If Python L2 module isn't shipped: C++ shim or Rust tora-rs.
2. Rust-exact live cqr leaves — needs cn_rst3 streaming to evaluate a graph per window (see clarified request below); Python `live.run` covers research-semantics meanwhile.
3. `level_placed` from TORA structs unconfirmed (derive from price vs book, or NaN → `_lle1` variants only).
4. TORA L2 行情 tier (A类机房 free / B类 purchased) — host placement decision.
5. cc pipeline signal cannot run on a live single feed (needs full universe + beta) — documented gap.

### Clarified request (for obgx side)

**Add alfa-graph evaluation to cn_rst3's streaming path** — `cn_rst3 stream-real --graph <cqr_bar.yaml> --freq 3m --bars-ipc` emitting `sid, date, endtime, leaves...` over the same live IPC channel, reusing the batch `bars` path's graph loading + `evaluate_bar_graph` + 3m `BarGrid` so streaming leaves are bit-identical to batch. Rationale: the current stream emits only the fixed CN-239 MinuteBar product; cqr leaves cannot be recomputed downstream (the event detail the graph needs is already aggregated away). Verification: stream vs `cn_rst3 bars` cellwise on one stock/date → expect 0 mismatches. Default stays MinuteBar when `--graph` is absent. Owner: obgx (Rust); this repo's `ipc_consumer.py` needs no change beyond schema tolerance.

**Checked 2026-08-27 — no existing engine has it:** cn_rst (v1, frozen) has no graph layer at all — `Replay`/`Batch`/`Stream` produce tick parquet only (book reconstruction; no `ticks_to_bars`, no graph refs in `streaming.rs`/`dual_stream.rs`). cn_rst2's `stream`/`stream-real`/`stream-dual` all call `ticks_to_bars` (fixed MinuteBar product, no `--graph` flag anywhere in `cn_rst2/src/main.rs`); its `run --bars-ipc` also streams the 10-column MinuteBar schema, not graph leaves. The gap exists in all three engines; cn_rst3 is the right target (astg_rst evaluator already linked, `evaluate_bar_graph` machinery exists in its `bars` path).

## 2026-08-27 (13:52 check) — v260827 landed: stream graph eval verified, fidelity gate partially open

### What landed (obgx)

- Tag `v260827` + registry image `tick-to-bar:v260827`; commits `16ad38c feat: stream graph eval — --graph/--freq/--bar-dtype on stream/stream-real` (new `graph_stream.rs`: GraphBarCollector + GraphIpcWriter, routes through the batch path's `evaluate_bar_graph` per `cn_bar_endtimes(freq)` window, schema identical to bars parquet via shared `build_bar_batch`), `519f13c docs(stream): graph-eval-in-streaming design` (documents the exact clarified request + a pre-identified fidelity risk), `a0f5ad4 release bump v260826→v260827`.
- CLI verified: `stream-real --graph <yaml> --freq <1m|3m> --bar-dtype <f32|f64> --bars-ipc/--bars-stdout-ipc`; no `--graph` → 239 MinuteBar path unchanged.

### Verification run (built clean at 2ca25f2 from a git worktree; user's local WIP untouched)

- Stream: `stream-real --date 20220104 --stocks 100 --graph alfa/cqr v260823 --freq 3m --bars-ipc` → 7.46M msgs → **8,100 graph bars / 252 leaves in 17.7s** on the IPC channel. The live path works end to end.
- **Acceptance gate (stream vs `cn_rst3 bars`, same binary/graph/date): 93.33% cells exact** (135,993 of 2,041,200 mismatched; float32 matched to batch config — not a dtype artifact).
- Localization: first-100-stocks universe is all-SH (SZ not exercised); ~70% of rows mismatched in every continuous window from 09:33 on (auction 09:30 window only 37%); mismatches concentrate in trend/grouping leaves (`n_*_tks_*`, `qtysum_*_in_*_eq_trend` family).

### Interpretation

This is the design doc's own pre-flagged "fidelity risk" realized: the streaming workers emit each row once at creation, while the batch `ShardReplay` mutates rows after their timestamp (SZ sweep-confirm re-stamps, resting-row rewrites) and re-emits across window boundaries (`emission_start`/`KeepAndReemit`). The doc states the 0-mismatch gate "decides whether a per-symbol replay-state port is needed" — the gate says **yes**. The user's local WIP (+446 lines: `streaming.rs`, `time_replay.rs`, `engine/{sh,sz}_continuous.rs`, `engine/output.rs`) is presumably that port; do not touch it. Re-run the gate when the WIP lands.

### Linked (2026-08-27): stream → consumer → signal, Kafka as the bus

- `live/ipc_consumer.py` now auto-detects both producer schemas (graph `endtime`+leaves and legacy 239 `window_end` OHLCV) and routes through the shared sinks stack (`--jsonl`, `--parquet-dir`, `--kafka-brokers/--kafka-topic` via the byte-compatible KafkaSink).
- **Verified chain:** v260827 `stream-real --graph cqr v260823 --freq 3m --bars-stdout-ipc | ipc_consumer --stdin` → 1,620 bars (20×81) with 252 cqr leaves each → 1,620 **demo-trend** signals (consumer picks up `n_g_in_up/down_eq_trend`). Production Rust leaves → live signal, end to end.
- **Kafka topology:** obgx already ships `bridge_stdout_to_kafka` (IPC stdout → topic, one batch per message, key `{date}:{sid}`) and the consumers (`ParquetArchiver`, `OrderGateway`). My consumer can either produce to the same topic or read the pipe directly. Open item: a Kafka consume mode in `ipc_consumer` to subscribe the topic instead of the pipe.

## 2026-08-27 (final) — bars-mean alpha, one-command pipeline, everything runs together

### Signal swap (user direction: "the bars' mean IS the alpha")

- `live/signal.py` rewritten: `compute_signal(bars, sym)` → `{"score": alpha, "direction": ±1/0, "meta": "bars-mean"}` where alpha = mean of the just-closed bar's non-NaN leaf values. All-NaN bar / unknown symbol → None.
- Frame recorded in the docstring + CLAUDE.md decisions log: the 3m stream *provides* bars; downstream would be bars → lp (features) → cc (yhat). bars-mean is the standing placeholder until lp/cc are wired.
- `tests/test_signal.py` rewritten for the new semantics (mean-with-NaN-excluded, determinism, negative mean, all-NaN, unknown sym).

### One-command pipeline + together-test

- `scripts/run_stream_pipeline.sh [DATE] [STOCKS] [GRAPH] [OUT]` — `cn_rst3 stream-real --graph --freq 3m --bars-stdout-ipc | live.ipc_consumer --stdin --jsonl` as one command; `CN_RST3`/`DATA_DIR` env-overridable, defaults 20220104 / 20 / alfa cqr v260823.
- `tests/test_integration_stream.py` — the full chain in one subprocess (skipif-guarded on binary/data/graph): 3 stocks → 243 bars × 252 leaves → bars-mean signals in 5.1s, per-sid endtimes ordered+unique. Passed.
- **Full suite together: 30 passed (29 unit + 1 integration) in 38.95s; ruff clean.** All previous green + the new chain test in a single run.

### Experiments documented

- `docs/experiments.md` — every experiment from this session with commands/results: E0 env constraints (3.13 segfault → two venvs, zh_CN.GB18030 locale, GCP→TTS unreachable), E1 graph feasibility (alfa v260823 252 leaves, raw cent/share units), E2 Python engine vs research/production (bit-faithful window 0; 95.2% vs Rust; 8,100-bar local backfill), E3 Rust streaming (239 MinuteBar chain), E4 v260827 gate (landed, 8,100 graph bars/17.7s, **93.33% stream-vs-batch**, localization, replay-state-port conclusion), E5 linked pipeline (bars-mean swap, Kafka byte-compat, integration test), E6 open items.
- CLAUDE.md updated to match: bars-mean alpha in the linked-chain section, `run_stream_pipeline.sh` as the canonical command, decisions 6 (bars-mean) + 7 (Kafka bus), experiments doc pointer.

## 2026-08-27 (late) — Kafka consume mode + rerunnable fidelity gate landed

### Kafka consume mode in live/ipc_consumer.py

- New input mode closes the last in-repo open item: `--kafka-consume` subscribes the bars topic instead of reading the pipe — obgx `bridge_stdout_to_kafka` → topic → consumer here → signal → sinks. Usage: `--kafka-consume-brokers/--kafka-consume-topic/--kafka-consumer-group/--kafka-consumer-config` (JSON) + `--kafka-consume-max N` debug cap.
- `consume_kafka` decodes one Arrow IPC batch per message (any producer schema — graph or legacy 239, e.g. a bridged pre-v260827 stream), polls forever in production, releases the consumer on error. Lazy confluent-kafka import, injectable `consumer_factory` (mirrors KafkaSink.producer_factory).
- Offline tests with a fake consumer: obgx-wire decode + bars-mean signals on both schemas, poll cap, error → RuntimeError + close. `tests/test_ipc_consumer.py` now 6 tests.

### Fidelity gate as a permanent tool (scripts/gate_stream_vs_batch.py)

- The E4d experiment is now rerunnable: runs `cn_rst3 bars --max-stocks N` and `stream-real --graph --stocks N` on the same date/universe, compares every leaf cell, reports % exact, worst windows, top mismatched leaves; `--expect-exact` exits 1 below 100.0000%; `--json` writes the report; `--keep` retains workdir.
- **First result: 3 stocks → 243 rows / 61,236 cells → 100.0% exact.** The stream and batch paths are bit-identical at small universe. The 93.33% seen at 100 stocks is therefore universe/parallelism-dependent, not a wholesale stream bug — the 100-stock re-run with the script is validating this as this entry is written.
- `tests/test_gate.py` (skipif-guarded, 13s at 3 stocks) pins the machinery, not the percentage — the percentage IS the standing gate, rerun manually with `--expect-exact` once the obgx replay-state port lands.

### GATE GREEN — 100-stock rerun: 0 mismatches on the rebuilt binary

- `scripts/gate_stream_vs_batch.py --stocks 100` (same binary both sides): 8,100 rows / **2,041,200 cells → 100.0% exact, 0 mismatched**.
- Provenance: `/mnt/disks/data/cargo-target/release/cn_rst3` was rebuilt **2026-08-27 15:07** by the user — their WIP (continuous.rs, sz_continuous.rs, orderbook.rs, time_replay.rs + new `k8s/cqr_stable_gate.yaml`, `cqr_evalport_*` manifests) is in the binary. The 93.33% measured at E4d was against the v260827 build @2ca25f2; the user's replay-state/compaction work closed the gap. **The standing gate is green on the current binary.**
- Caveat: the WIP is uncommitted — re-run the gate after every obgx rebuild/commit to keep the green honest.

## 2026-08-27 (final-final) — TORA on-ramp documented; repo committed

- `docs/tora_onramp.md`: what to ask 华鑫奇点 for (n-sight account, L2 module form factor, 行情 tier, recording permission, same-date historical 逐笔 dump, trading account), the L2 → place/trade + tick_table conversion plan (two cross-checking paths), gate-style alignment profiling vs the research tape, milestones M0 capture → M5 trade, and why cqr_bar on live TORA is off the table until M3 (SDK blocked + field fidelity unprofiled).
- CLAUDE.md open-items rewritten around the on-ramp; decisions log unchanged (made decisions still hold).
- Full working tree committed to master (not pushed) — live service, scripts, tests, docs, configs, venv pins; `bars/` outputs + pycache gitignored.

## 2026-08-27 (night) — alignment_ob/ iteration workspace, TORA credentials in hand

### What was built

- `alignment_ob/` package (user direction: "create a folder alignment_ob/ to iterate on this", n-sight account 00974579 provided):
  - `converters.py` — M2: events → tick_table (canonical 83-col schema: int64 + the one float col `vwap_better` — found by the round-trip test catching 8,128 fractional mismatches); events → place/trade (yuan prices, ns-epoch OrderTime/TradeTime, OrdType 0 add / -1 cancel cross-validated: the -1 count 5,894 == tick_table event-2 cancel count exactly).
  - `profiler.py` — M3: tape-level comparator (NaN==None exact, missing rows/columns on either side, per-column delta attribution) — the gate idea one level below bars.
  - `capture.py` — M0/M1: `replay_capture` (offline loop on real data) + `ToraCapture` scaffold (SDK-conditional, struct names from tora-rs pre-mapped).
  - `README.md` iteration log + SDK acquisition steps; `secrets.example.yaml` template; real `secrets.yaml` (600, gitignored).
- `tests/test_alignment.py` — 6 tests: **bit-faithful tick_table round-trip (30,598 × 83 = 100.0%)**, capture dump, place/trade converter unit tests, profiler self-compare + mutation detection.

### SDK status

- n-sight.com.cn reachable from this GCP box (HTTP 200, JSESSIONID) but the login is an element-ui SPA with no curl-friendly endpoint (RSA/captcha risk with real creds → did not brute-force). **Manual browser download needed** (README steps; SDK pins from the openctp TORA readme: API_C++_行情_v1.0.8_20241220 + API_C++_交易_v4.1.3_20250415; check the 行情 package for the lev2 L2 module).
- Fronts remain China-reachable only — capture must run on a China host.

### Numbers

- Full suite with alignment_ob: 39 tests (33 + 6), all green; ruff clean.
