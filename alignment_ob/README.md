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

## SDK (acquired — DONE 2026-08-27, no browser needed)

The tora-rs build docs host the official packages at `ctp-api.ruiqilei.com`;
both are downloaded into `alignment_ob/sdk/` (gitignored):

- `API_Stock_lv2_C++ v4.0.8_20251126.zip` → `C++_API_v4.0.8_20251126/`:
  `TORATstpLev2MdApi.h`, `TORATstpLev2ApiStruct.h`, `TORATstpLev2ApiDataType.h`,
  Linux `liblev2mdapi.so`, Windows dll/lib. **The L2 逐笔 module is C++** —
  hence the C++ capture tool `cpp/lev2_dump.cpp` (built, `sdk/bin/lev2_dump`).
- `API_Stock_C++_td v4.1.8_20260422.zip` → `traderapi.zip` (execution side, M5).

Login form: `ReqUserLoginField` — `LogInAccount` (00974579), `LogInAccountType`
('0' UserID / '1' 资金账号), `Password`, `AuthMode='0'`; **`DepartmentID` is
required when logging in as 资金账号** (unknown value — try UserID first).

**The fronts are reachable from this GCP box** — `OnFrontConnected` fires on
both `tcp://210.14.72.16:9402` (7x24) and `tcp://210.14.72.21:4402` (仿真).
First live run blocked on: pre-market hours (06:15 Beijing) and/or login
rejection. `alignment_ob/scripts/capture_live.sh` runs the capture; the first
market-open capture is scheduled for 09:15 Beijing.

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

## 2026-08-27 night — SDK acquired + capture tool built + M1 normalizer

| # | What | Result |
|---|------|--------|
| 5 | **SDK acquired programmatically** — tora-rs build docs host direct URLs at `ctp-api.ruiqilei.com`: `API_Stock_lv2_C++ v4.0.8_20251126.zip` (TORALEV2API) + `API_Stock_C++_td v4.1.8_20260422.zip` — downloaded to `alignment_ob/sdk/` (gitignored) | C++ headers + Linux `liblev2mdapi.so` + Win dlls |
| 6 | Headers read (GBK→UTF-8): `CTORATstpLev2MdApi` (Create/Init/RegisterFront/SubscribeMarketData/Transaction/OrderDetail/Resend*), SPI callbacks, all three field structs, types/enums (`'B'/'S'` aggressor, `'A'/'D'` add/delete, `'1'/'2'` SSE/SZSE, int HHMMSSmmm timestamps, yuan prices, login `LogInAccountType '0'/'1'` + `DepartmentID` 必填 for 资金账号) | M1 mapping grounded on the official headers, not guesses |
| 7 | **`cpp/lev2_dump.cpp` built and linked** against `liblev2mdapi.so` (g++ on this box) — M0 raw capture tool, JSONL per callback, all book fields via named accessors | Builds clean; `GetApiVersion()` works offline; **the fronts CONNECT from this GCP box** (`tcp://210.14.72.16:9402` 7x24 and `tcp://210.14.72.21:4402` 仿真 both fire `OnFrontConnected`) |
| 8 | First live attempts (acct_type 0/1, both fronts, 06:15 Beijing): front connects, subscribes, **no login response / no data** — pre-market or login-rejection (DepartmentID missing for 资金账号 login) | Scheduled capture at 09:15 Beijing to distinguish |
| 9 | `normalize.py` — M1 normalizer: md→book state, tr→event 1/4/2 (stat/ExecType), od→event 0/2, resend dedupe in the same seq space, book stamped on every event, `level_placed` derived, SZ sign from mid | 11 tests green |

## Live capture (works from this box — the fronts are reachable)

```bash
# M0: dump 2 hours of raw L2 (open auction + morning session)
alignment_ob/scripts/capture_live.sh 7200 tcp://210.14.72.16:9402 0 600000,000001 1
# → alignment_ob/captures/{date}/raw/capture_tcp210.14.72.169402.jsonl

# M1: normalize → events → tick_table
.venv-live/bin/python - << 'PYEOF'
from alignment_ob.normalize import normalize_capture
from alignment_ob.converters import events_to_tick_table
import glob
path = sorted(glob.glob("alignment_ob/captures/*/raw/capture_*.jsonl"))[-1]
day = int(path.split("/")[2])
events = normalize_capture(path, day)
t = events_to_tick_table(events, sid=2_600_000)
print(f"{len(events)} events → {len(t)} tick_table rows")
PYEOF

# M3: profile against the research tape (same date), then bars via the gate
```
