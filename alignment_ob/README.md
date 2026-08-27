# alignment_ob — iterate the TORA live feed toward research-aligned signals

Iteration workspace for the M0→M5 on-ramp (full plan: `docs/tora_onramp.md`).
Everything that does not need the TORA SDK is built and **offline-verified
against the real research data**; what needs the SDK is scaffolded with the
exact manual steps below.

## Iteration log

| # | Date | What | Result |
|---|------|------|--------|
| 1 | 2026-08-27 | M2 converters: events → tick_table (canonical 83-col schema, int64 + float `vwap_better`), events → place/trade (yuan, ns-epoch; OrdType 0 add / -1 cancel cross-validated against the real file) | Round-trip `tick_table → events → tick_table` on sh600000/20220104 is **bit-faithful: 30,598 rows × 83 cols, 100.0% exact** (pinned by `tests/test_alignment.py`) |
| 2 | 2026-08-27 | M3 profiler: tape-level cellwise comparator (NaN==None exact, missing columns/rows attribution, per-column deltas) | 6 tests incl. self-compare + single-cell mutation detection |
| 3 | 2026-08-27 | M0/M1 capture harness: `replay_capture` proves capture → convert → profile end to end on real data; `ToraCapture` scaffolded (SDK-conditional) | Replay capture dumps 30,598 normalized events and round-trips bit-faithfully |
| 4 | 2026-08-27 | n-sight.com.cn probed from this box: reachable (HTTP 200, JSESSIONID) but the SPA login endpoint is not curl-friendly (element-ui SPA; RSA/captcha risk) | SDK download must be done manually in a browser — steps below |

## SDK acquisition (manual, one-time)

The openctp TORA wrapper pins the official SDK versions:
`API_C++_行情_v1.0.8_20241220` and `API_C++_交易_v4.1.3_20250415`.

1. In a browser: https://n-sight.com.cn → log in with the 00974579 account
   (N视界 platform login — the SPA; if it asks for a phone number, use the
   one registered to the account).
2. 下载中心 → download the two SDK packages above.
3. Unzip into `alignment_ob/sdk/` (gitignored).
4. **Check the 行情 SDK for the L2 逐笔 module**: `lev2md` dir / `TORALEV2API`
   headers — `Lev2MdApi`, `OnRtnLev2OrderDetail`, `OnRtnLev2Transaction`,
   `OnRtnLev2MarketData`. C++ headers → small shim (or the Rust `tora-rs`
   crate); a Python `lev2` module → wire `ToraCapture` directly.
5. Fill `alignment_ob/secrets.yaml` (copy of `secrets.example.yaml` —
   gitignored) with user/password/appid/authcode.
6. Run on a China-reachable host (the fronts `tcp://210.14.72.*` are not
   reachable from this GCP box):
   ```bash
   python -c "from alignment_ob.capture import ToraCapture; ..."
   ```
   → M0 raw dumps land in `captures/{date}/raw/{sid}.jsonl` first; normalize
   only after eyeballing the raw fields (M1).

## Offline loop (works today, no SDK)

```bash
.venv-live/bin/python -m pytest tests/test_alignment.py -q
# → 6 passed: bit-faithful tick_table round-trip + capture dump + converters + profiler
```

The capture → convert → profile chain:
```python
from alignment_ob.capture import replay_capture
from alignment_ob.converters import events_to_tick_table
from alignment_ob.profiler import profile_tape_dirs
```
M3 on real TORA data will be: capture a backtested date → convert →
`profile_tape_dirs(captures_dir, "/mnt/disks/data/cn/tick_table", date, sids,
keys=("local_ts", "order_id", "order_seq"))` → per-column report → fix the
M1 mapping until 100.0% → then bars via `scripts/gate_stream_vs_batch.py`.

## M1 normalization checklist (when live fields are seen)

- [ ] yuan→cents ×100, shares verbatim
- [ ] BSFlag aggressor → research `sign` (+1/-1/0) — verify against
      `tick_table` sign distribution for the same window
- [ ] resend/retransmission dedupe by seq (double-count → vol leaves wrong)
- [ ] `level_placed` — derive from order price vs book, or NaN
- [ ] `qty` (last-trade size) from the L2 transaction field
- [ ] `cum_volume` for 7×24 loop-restart rollover
- [ ] `exch_ts` vs `local_ts` (feed has exchange timestamps; tick_table keeps both)
- [ ] cancel attribution: OrdType -1 (place) cross-checked against event 2 count
- [ ] BuyOrderID/SellOrderID attribution (event stream has one order_id per row)
