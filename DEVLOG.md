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

## 2026-08-27 (22:30) — SDK in hand, capture tool built, fronts reachable, M1 normalizer done

### SDK acquired programmatically (no browser needed)

- tora-rs build docs host the official packages: `API_Stock_lv2_C++ v4.0.8_20251126.zip` (TORALEV2API) + `API_Stock_C++_td v4.1.8_20260422.zip` at ctp-api.ruiqilei.com → `alignment_ob/sdk/` (gitignored). Headers (GBK→UTF-8) give the FULL ground truth: `CTORATstpLev2MdApi` (Create/Init/RegisterFront/DeclareMKSubTypes/Subscribe{MarketData,Transaction,OrderDetail,Resend*}/ReqUserLogin), SPI callbacks, field structs, types/enums — timestamps int HHMMSSmmm, prices double yuan, `'B'/'S'` TradeBSFlag, `'A'/'D'` OrderStatus, `'1'/'2'` SSE/SZSE, login `LogInAccountType '0'/'1'` + DepartmentID 必填 for 资金账号.
- **`alignment_ob/cpp/lev2_dump.cpp`** — M0 capture tool (JSONL per callback, named accessors for the non-contiguous book fields). Compiled + linked against liblev2mdapi.so ON THIS BOX; GetApiVersion works; **both fronts CONNECT from this GCP box** (9402 7x24 + 4402 仿真) — the China-host constraint does not apply to the TORA fronts.
- First live attempt 06:15 Beijing: connected + subscribed, no login response / no data (pre-market or 资金账号-login rejection). Capture scheduled for 09:15 Beijing (01:20 UTC, one-shot durable cron).
- `alignment_ob/scripts/capture_live.sh` — repeatable capture: timeout + front + acct_type + sids, secrets from secrets.yaml, raw JSONL → captures/{date}/raw/.

### M1 normalizer (`alignment_ob/normalize.py`) — 11 tests

- md → BookState (10 levels, cents, order counts, cum volume, MDSecurityStat); tr → event 1 (continuous) / 4 (auction stat '1'/'b') / 2 (SZ ExecType 撤销); od → event 0 ('A') / 2 ('D'); TradeBSFlag→sign (SZ derives from mid); resend dedupe in the SAME seq space as live; every event stamped with the current book (has_book=True) so the M2 converter reproduces research rows; level_placed derived from order price vs book.
- Bugs the tests caught: resend keys must share the live seq space; unknown line types must not touch SecurityID; HHMMSSmmm left-padding for pre-10:00 hours.

### Suite: 50 passed, ruff clean. Committed (not pushed).

## 2026-08-27 (23:30) — Rust row semantics extracted; past-data dry run at 98.4%

### User pointers that mattered

"go to rust code to get them aligned how the tick row are generated" +
"mind the format alignments" — read `obgx/obg/t2b/cn_rst3/src/tick_row.rs`
+ `engine/tick.rs` (the row semantics + emission helpers). The corrections
vs the naive mapping were substantial:

1. event 1 = **Add** (4 when in auction); event 2 = Cancel with
   **vol_placed = -(qty)**; event 4 = auction adds AND auction fills
   (vol_placed=0, vol_traded=qty, level_placed=-2, order_id=0,
   tvr=price×qty/100); event 5 = market/aggressive-intent rows.
2. **Continuous trades do NOT emit rows** — they mutate the aggressor's
   Add row (vol_traded/tvr/count_traded/depth_traded) and re-emit it
   (the sweep-confirm surface from E4f). A stateless mapper cannot
   reproduce them; needs the per-order row-mutation port.
3. **local_ts = exch_ts + 100ms** (fixed offset, confirmed in data);
   order_seq = per-symbol row counter; level_placed = distinct-price-level
   count (0 at best, -1 crossed) — NOT a 10-level index.
4. Auction windows pinned empirically on sh600000/20220104: adds
   09:15–09:25 / 14:57–15:00; fills = single print instants
   (09:25:00.980 open = 67 fan-out rows at one price; 15:00:00.000 close).
5. Formats: tick_table is 100% ms-aligned; the trade FILE is aggregated
   (one auction match fans out to many tick_table fills) — TORA 逐笔成交
   is per-fill, i.e. tick_table-granularity.

All of it written down in `alignment_ob/tick_row_spec.md`.

### The past-data dry run (the user's "any past data to test API?")

Research tape re-dressed as a TORA capture (place→od, trade→tr,
tick_table book→md, TORA wire format via the fixed HHMMSSmmm packer —
a sec×10² carry bug was found and fixed by the round-trip) → normalize →
convert → profile vs the research tick_table:

**98.4% cell-exact on 21,719 comparable rows** (adds/cancels/auction fills);
event codes 99.9% exact (25 rows), sign 99.9% (15), vol_placed/vol_traded
99.9% (135/122). Residuals fully attributed: level_placed beyond the
visible 10 levels (5,373 rows — the live 10-level feed's structural limit;
`_lle1` leaves unaffected), book-stamp timing at fan-out instants (~1,100),
and the 7,749 continuous-trade rows (mutation-only). Pinned by
`tests/test_synth_tora.py` (sanity thresholds; the profile is the gate).

### Suite: 53 passed, ruff clean. Committed (not pushed).

## 2026-08-28 — mconn (private) parity normalizer: 99.89% cell-exact full tape, event-5 taxonomy ported

The alignment work moved to the private companion repo GXIU/mconn
(the fork is public; the dry-run machinery and probes live there —
`docs/experiments.md` is the full probe log). The stateless normalizer's
73%-coverage 98.4% dry run has been superseded by the **full-tape parity
normalizer** (`mconn/feed/parity.py`): full book reconstruction from od/tr
deltas, FIFO slots keyed by order id, and the complete cn_rst3 emission
taxonomy ported row-for-row from the Rust sources (continuous.rs /
engine/tick.rs / time_replay.rs).

Trajectory (same gate, sh600000/20220104, 29,411 comparable rows):

- 98.4% (stateless, 71% of tape) → 99.02% (order-id slot model) →
  99.57% (stamp timing: adds/cancels PRE-event, fills POST-event;
  open-auction fan-out progressive both-side consumption, close shared
  book) → 99.6005% (last_price rule: preclose seed + every trade
  updates the book) → 99.8877% (event-5/mutation taxonomy) →
  **100.0000% cell-exact** (close-auction algorithmic match + open
  quiet-phase drain + rank pairing + time-driven auction membership).

The event-5 taxonomy: aggressive adds stamp level_placed −2; fills
MERGE into the registered aggressor's Add row (qty stays original,
price = first fill, book re-snapshot per print — the tick_table shows
the row once, zero order-id repeats); trade-only rows for unregistered
aggressors (multi-price sweep → event 5); cancel-after-sweep upgrades
filled lvl=−2 adds / trade-only rows to event 5 (all 71 rows on the
tape). The final levers: SH close-auction prints are ignored by
cn_rst3 (is_close && is_sh) — the rows come from the algorithmic
book match (auction_match_with_price + execute_auction +
cancel_beyond_price, stamped 15:00:00.000); the open-auction quiet
phase drains NAMED orders per print then silently cancels beyond the
clearing price; auction membership is time-driven, not stat-driven;
the gate pairs fan-out key groups by in-group rank (order_seq).

**Final gate: 30,598/30,598 rows compared (missing 0/0), 2,050,066
cells, 0 mismatches.** The parity dry run reproduces the research
tick_table bit-for-bit.

Also landed: the delay monitor (`recv_ns` on every lev2_dump line +
`mconn/feed/delay.py`, per-stream recv−exch stats, offline-tested) —
M3 grading ready for the first live capture.

24 tests green in mconn (incl. the parity gate at 99.5% threshold and
the exact event-taxonomy counts). Pushed to GXIU/mconn (private).
