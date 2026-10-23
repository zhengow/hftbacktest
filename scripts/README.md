# Bitget V3 multi-channel replay

`merge_bitget_books.py` is an exchange-specific data adapter. It does not change
hftbacktest matching rules, PostOnly behavior, or fill reason definitions.

```sh
python scripts/merge_bitget_books.py \
  --input /absolute/path/to/sealed/journal \
  --output /absolute/path/to/output \
  --symbol ZECUSDT --tick-size 0.01 --build-npz
```

Existing output is protected; pass `-f` to regenerate. Progress is printed while
reading and merging. Compressed input requires `zstandard`; NPZ generation also
requires the normal hftbacktest, NumPy and Polars dependencies.

## Input

One healthy connection, one symbol, and a full `books` snapshot followed by its
continuous incrementals. `books1` and `books5` messages must be snapshots. Input
may be a sealed directory of `events_*.jsonl.zst`, or one JSONL/JSONL.ZST file.
Capture journal rows have this envelope (other private/nonmarket rows are ignored):

```json
{"event_id":1,"utc_us":1700000000010000,"kind":"market_raw","data":{"source":"sampler_public","connection_id":"connection-1","source_clock_us":1700000000010000,"raw":"{\"arg\":{\"topic\":\"books1\",\"symbol\":\"ZECUSDT\"},\"action\":\"snapshot\",\"data\":[{\"ts\":\"1700000000000\",\"seq\":1,\"pseq\":0,\"b\":[[\"100\",\"1\"]],\"a\":[[\"101\",\"1\"]]}]}"}}
```

Alternatively each row can contain `receive_us`, `connection_id`, and a parsed
Bitget WebSocket envelope under `message`. Exchange time is `data[].ts`, not the
outer push timestamp. Public trades retain `T`, side, price, amount, ID and the
original receive time; duplicate IDs with different content are an error.

## Fusion

- Full-depth sequence continuity is checked in receive order, including the first
  snapshot-to-incremental bridge. Resets, gaps and multiple connections require
  explicit segmentation. No silent gap repair or connection mixing.
- `books1` and `books5` replace only their known top-price range. Better prices
  absent from the snapshot are removed. A price falling outside the newly
  covered range is retained: leaving the top five is not cancellation evidence.
- Per-price clocks and range tombstones stop older messages resurrecting stale
  quotes, while allowing older full-depth updates outside newer top coverage.
- Cross-channel precedence is row exchange time; at equal milliseconds the
  explicit policy is `books1 > books5 > books`. Sequence values are compared
  only within a channel. Cross-channel sequence comparability is not assumed.
- A crossed/empty mixed view is withheld until consistent; pending differences
  are then emitted. Counts are exposed in `summary.json`. This is a data-fusion
  safeguard, not proof that omitted intermediate states were invalid at venue.
- Deltas remove obsolete quotes before adding replacements. This reduces, but
  does not eliminate, the core engine's level-by-level processing limitation.

## Outputs and causality

- `depth.csv`: exchange-time fusion, Tardis-compatible.
- `depth_local.csv`: **independent receive-order fusion**, with stale state
  rejected. It is not a delayed copy of the exchange-time fused book.
- `trades.csv`: public trades with original exchange/receive timestamps.
- `market.npz`: standard EXCH-only and LOCAL-only events, independently ordered.
  No custom event kinds or engine rebuild are needed.
- `summary.json`: counts, skipped states and output hashes. Written only after
  successful completion; an interrupted directory is not a completed export.

The two views may have different payloads and numbers of events. This prevents
exchange-time fusion from incorporating a later-received message into an earlier
local observation. No clock shifting is performed; negative feed latency requires
explicit upstream clock alignment. Same-millisecond Trade/depth ties use Trade
first; millisecond timestamps cannot establish an exact physical matching order.

Own-liquidity correction is a separate **subsequent** research step. Replacing
only the EXCH payload with an own-corrected book preserves the recorded causal
LOCAL observations; do not accidentally reconstruct LOCAL data from that corrected
exchange book. This adapter never needs private order/fill records.

## PostOnly replay setting

The engine's optional `HFTBACKTEST_POST_ONLY_REJECT_NS` environment variable is
read when each exchange model is constructed; its native default is **0**.
Zero disables only the extra rule that treats an early book/trade crossing as
an arrival-time rejection. Normal GTX/PostOnly rejection against the book on
order arrival remains active. Changing the environment after construction does
not reconfigure an existing backtest.

Do not automatically carry a coarse-data 20ms compensation window into this
high-frequency fused feed. It can reject genuine fast maker fills. Measure actual
order rejection timing and check admission-time book state first. Private cTime
to terminal time is not the same quantity as time to the next public depth push.

Tests:

```sh
python -m unittest discover -s scripts/tests -p 'test_*.py'
```

Protocol references: [Depth channel](https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel),
[Best practices](https://www.bitget.com/docs/uta/best-practices-guide).
