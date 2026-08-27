# tick_table row-generation spec — extracted from cn_rst3 (obgx)

Source of truth: `obg/t2b/cn_rst3/src/tick_row.rs` + `engine/tick.rs`
(+ the per-engine emission code in `engine/sh_continuous.rs` /
`engine/sz_continuous.rs` — user WIP, read-only). This is the format
contract the TORA live path must reproduce. **Mind these alignments** —
every one is pinned by data or code:

## Schema (83 columns)

24 core + 60 book (10 levels × 3 fields × 2 sides), prices int cents,
timestamps ns past midnight. `sid` filled at array conversion;
**`order_seq = row_index + 1`** (per-symbol emission counter);
**`local_ts = exch_ts + 100_000_000`** (fixed +100ms offset — confirmed in
data: every research row has `local_ts - exch_ts == 100ms`).
`vwap_better` is the single float column.

## Event codes (the row you emit per feed event)

| code | meaning | vol_placed | vol_traded | notes |
|------|---------|-----------|-----------|-------|
| 1 | **Add** (limit order to book; also trade-only rows when the aggressor has no book row and the target side has no qty) | +qty | 0 | `make_order_row`; in-auction adds → **event 4** |
| 2 | **Cancel** | **−(qty)** | 0 | `make_cancel_row`; `order_lifetime = exch_ts − order.exch_ts`; `vol_behind = level_total − vol_ahead − qty` (FIFO walk, `position_ahead_at_level`) |
| 4 | **Auction** — both auction adds (`make_order_row(in_auction)`) AND auction fills (`make_auction_row`: vol_placed=0, vol_traded=qty, sign, `level_placed = −2`, order_id=0, `tvr = price×qty/100`, depth_traded=count_traded=1, last_price=price) | | | two different row shapes share code 4 |
| 5 | **Market / aggressive intent** (`make_market_row`: vol_placed=qty, vol_traded=0; `level_placed=−1` in lightweight mode) | +qty | 0 | also trade-only rows when target side HAS qty |
| 3 | trade (standalone) — defined, **never emitted**; Python-only legacy | | | |
| 6/7 | PU (SZ, ~0.05%) / FOK — FOK dead in the parquet pipeline | | | |

**Trades do not emit rows.** A continuous fill MUTATES the aggressor's
existing Add row (`vol_traded`, `tvr`, `count_traded`, `depth_traded` are
updated) and the updated row is RE-EMITTED (the sweep-confirm
re-emission — this is the stream-vs-batch fidelity surface, E4f). A
stateless mapper therefore cannot reproduce the trade portion of the
tick_table; reproducing it needs the per-order row-mutation port
(`orderbook.rs` slots + cages + sweeps). The add/cancel subset
(22,451/30,598 rows on sh600000/20220104 = 73%) maps 1:1 to TORA od
records.

## level_placed (Rust `level_placed()`)

- 0 if the order price equals best on its side (at best)
- −1 if better than best (crossed/spread)
- else: the NUMBER OF DISTINCT PRICE LEVELS between best and the order's
  price (full book, not capped at 10 — that's why the data shows 50)
- auction fills: −2; lightweight market rows: −1

Live 10-level feed caveat: exact level_placed is computable only while
the order's price is within the visible 10 levels; deeper → the live
engine must cap or NaN (cqr only uses `_lle1` variants — `==1` needs only
the top 2 levels, so `_lle1` leaves are live-computable).

## Book snapshot timing

Default = **pre-event** (`make_base_row` snapshots before the event is
applied, unless `ob_snapshot_timing == Post`): a row's 60 book columns are
the book AS IT STOOD BEFORE its own event. (Confirmed empirically: stamp
md-before-event reproduces research rows.)

## Position fields (per-row book queue stats)

- add/market: `compute_position` — vol/orders better (strictly better
  prices, cage-aware), ahead = total level qty/len at own price, behind=0,
  `vwap_better` = better-price vwap or own price.
- cancel: `position_better` + `position_ahead_at_level` (FIFO walk to own
  slot) + `vol_behind = level_total − vol_ahead − qty`.
- `tvr` on auction fills = price×qty/100; `zero_sweep_row` (SZ voided
  sweeps) zeroes vol_traded/tvr/count_traded/depth_traded and sets
  `qty = vol_placed`.

## TORA → primitives mapping (for the live engine)

| TORA | primitive | row |
|------|-----------|-----|
| md (10-level) | book snapshot (10 levels only!) | none |
| od 'A' | add(price, qty, side, exch_id=OrderNO) | event 1 (4 in auction stat) |
| od 'D' | cancel(OrderNO) | event 2 |
| tr (连续) | fill(BuyNo/SellNo, price, qty) | mutates aggressor row → re-emit |
| tr (auction stat) | auction trade | event 4 fill row |
| TradeBSFlag | aggressor sign | 'B' +1 / 'S' −1 / 'N' derive |

The live engine therefore needs: (1) a full OrderBook with FIFO slots
(10 levels visible; internal book for positions), (2) per-order row state
+ mutation, (3) the emission rules above. That port = the next milestone
(`alignment_ob/rust_parity.py`), verified bit-faithfully against
place/trade → tick_table on sh600000/20220104.
