# TORA live on-ramp — from L2 feed to aligned bars, then trading

Status 2026-08-27: the whole live path is blocked on the 华鑫奇点 (TORA) SDK
+ account. This doc is the plan for unblocking it: what to ask TORA for, how
to convert their L2 stream into the research formats (place/trade, tick_table),
how to profile alignment against local backtests, and the milestones to
actually trading.

## What to ask 华鑫奇点 for (concrete list)

1. **n-sight account** (n-sight.com.cn) — gates the SDK download and API
   authorization (user/password, appid/authcode). 仿真账号 first.
   *2026-08-28 update:* the account 00974579 connects to both fronts
   (`OnFrontConnected` fires) but `ReqUserLogin` is NEVER answered on
   acct types 0/1/2/3/9 × both fronts, during market hours, with a
   header-verified well-formed login struct. → **ask for the 行情账号**
   (L2 逐笔 typically ships under a separate market-data account, not the
   trading account), **the L2 行情 front address** (the 9402/4402 fronts
   may be L1-only), and the **一级机构代码 (DepartmentID)**.
2. **SDK form factor** — confirm whether the official SDK ships the **L2 逐笔
   module** (`lev2md`: `Lev2OrderDetail` 逐笔委托, `Lev2Transaction` 逐笔成交
   with BSFlag aggressor sign, 10-level `Lev2MarketData`, resend streams) as a
   Python module or C++ only. C++ only → thin shim or the Rust `tora-rs` crate.
   (`ToraSource` in `live/source.py` is already scaffolded with the confirmed
   struct mappings; only plumbing is missing. The C++ capture tool
   `alignment_ob/cpp/lev2_dump.cpp` is built and working — SDK v4.0.8 C++.)
3. **L2 行情 tier** — free in A类量化机房, purchased for B类 (自备机器). This
   decides host placement: the capture host must sit China-side anyway.
4. **Recording permission** — confirm the 行情授权 covers recording to disk for
   internal research (needed for the alignment profiling below).
5. **Historical 逐笔 dump for a date we already backtest** (e.g. 20220104,
   where tick_table + cn_rst3 bars exist) — or replay of that date on the 仿真
   feed. Alignment profiling needs the SAME date in both worlds, so the
   TORA-sourced events can be compared cellwise against the research tape.
6. **Trading side** (later, for execution): 华鑫 broker account with API
   authorization (XTP or TORA tdapi) + order-front addresses.

## Converting live L2 → research formats

```
TORA L2 (live)                         research formats
─────────────────                      ────────────────────────────────
Lev2OrderDetail (逐笔委托) ──┐
Lev2Transaction (逐笔成交) ──┼──→ live/events.py Event stream
Lev2MarketData (10-level book)┘      (instrument, t_ns, event, price, sign,
                                      vol_placed/traded, level_placed?,
                                      bid/ask_price_0/qty_0, ...)
```

From the normalized event stream there are two output paths, which should
cross-check each other:

1. **place/ + trade/ parquet** (obgx raw layout `place/YYYYMMDD/`,
   `trade/YYYYMMDD/`) — order/exec events only. Then run cn_rst3's book
   reconstruction → tick_table → bars: **the exact research chain**, giving
   by-construction-comparable bars.
2. **tick_table directly** — TORA natively provides the book, so
   `bid_price_0/ask_price_0` come from `Lev2MarketData` and place/trade events
   fill the order/exec columns; this skips reconstruction. Simpler, and it's
   what the live engine already consumes (`ReplaySource` format).

Open field questions to resolve against real captures (M1/M2):
- `level_placed` — not confirmed in TORA structs; derive from order price vs
  book, or NaN (`_lle1` leaf variants only).
- resend/retransmission dedupe — resend streams must be deduped by seq or
  volumes double-count.
- BSFlag aggressor semantics — must map 1:1 to the research `sign` convention.
- Trading-day rollover on the 7×24 loop (already handled by the engine).

## Profiling live-vs-backtest alignment

The gate script pattern already exists (`scripts/gate_stream_vs_batch.py`) —
alignment profiling is the same idea one level up:

1. Capture TORA L2 for a backtested date (or get the historical dump) → M2
   converters → TORA-sourced place/trade + tick_table.
2. Compare TORA tick_table vs research tick_table cellwise (volumes, prices,
   book, signs per window) — the tape-level alignment report.
3. Run both through the bar path (`cn_rst3 bars` / `stream-real --graph`) and
   compare the cqr leaves cellwise — reuse the gate comparator. Acceptance:
   100.0% on the key leaves (n_trade, qty sums, price stats); document
   deltas for the known-unknown fields (level_placed, resend, aggressor).
4. Only then is the live feed trustworthy enough for live cqr_bar evaluation.

## Milestones

- **M0 — capture harness** (needs SDK): ToraSource dump mode — subscribe a few
  instruments, write raw `Lev2*` fields to files before any normalization.
  Verifies field semantics against reality.
- **M1 — normalize**: raw → `Event` stream; validate against TORA's own
  aggregates (cumulative volume, snapshot books).
- **M2 — converters**: Event → place/trade parquet + direct tick_table;
  cross-check the two paths.
- **M3 — alignment**: same-date TORA data vs research tape + gate-style bar
  comparison (above).
- **M4 — live cqr_bar**: run `live.run --config configs/live_tora.yaml` on the
  China host; 3m graph bars + signal live; compare to M3 expectations.
- **M5 — trade**: Kafka bus already wires the signal onward; add the execution
  side (obgx OrderGateway consumes the topic; broker tdapi XTP/TORA; risk
  checks) and replace bars-mean with the real lp/cc signal.

## Why cqr_bar can't run on TORA live right now

Two reasons, both real:
1. **No SDK/account** — the feed itself is unavailable (M0 blocked).
2. **Fidelity is unverified** — even with the SDK, field-level semantics
   (level_placed, resend dedupe, BSFlag) are unknown; running the graph before
   M1–M3 profiling would produce leaves we can't verify against research.
   The only graph-verified input today is the cn_rst3 replay stream
   (`scripts/run_stream_pipeline.sh`), which is bit-exact to batch on the
   current binary.
