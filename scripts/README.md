# Bitget multi-channel replay

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

## Performance backends

Historical CSV mode now defaults to the Rust backend when Cargo or a prebuilt
native binary is available. Raw journal mode keeps the Python implementation.
The Python command and output event semantics remain compatible. Use
`--backend python` for the reference implementation, or `--backend rust` to
require native execution. `summary.json` identifies the selected backend.

The first Rust run builds `scripts/bitget_merge_native` automatically with at
most two compile jobs. Later runs reuse the binary; source changes trigger a
rebuild. Cargo may download build dependencies on first use; the data conversion
itself does not download market data or access exchange accounts. Build files
live inside that standalone crate, separate from the main workspace.

For historical backtests, the fastest output mode is:

```sh
python scripts/merge_bitget_books.py \
  --input mm_strategy/data_buy/zecusdt --date 20260916 \
  --output mm_strategy/model_v1/merged_books/ZECUSDT/20260916 \
  --symbol ZECUSDT --tick-size 0.01 --npz-only
```

`--npz-only` implies `--build-npz` and writes `market.npz` plus metadata without
the large depth/trade CSV outputs. It requires the Rust historical backend.
For diagnostic CSVs and NPZ together, use `--build-npz` instead. Both produce the
same numeric event stream; textual floating-point formatting may differ between
Python and Rust CSVs.

Rust uses compact frames/levels instead of Python objects per level, independently
sorts the two causal views, and streams a four-way merge directly into a standard
NumPy NPZ/NPY container. It does not create or globally sort a full event array.
Intermediate event files are removed on success or normal error return. ZIP64
supports large daily outputs. No hftbacktest engine modifications are required.

`--jobs 2` (default) uses one worker for each time view; `--jobs 1` reduces CPU
and index memory. Parsing and final NPZ writing are sequential. Frames and levels
remain in compact memory for the selected day; this is not an unbounded-memory
multi-month importer. Process days sequentially, and start with one symbol per
process. Logs include phase progress and fusion-stage ETA. The summary records
phase timings, worker count and native binary SHA256.

Output protection and `-f` remain supported. Forced regeneration removes the
adapter's prior generated outputs to avoid stale CSV/NPZ combinations. Historical
input files cannot overlap generated output paths.

## Historical CSV input

The same script also accepts downloaded Bitget historical CSV files. Input mode
is detected automatically, or selected with `--input-format raw|csv`.

```sh
python scripts/merge_bitget_books.py \
  --input mm_strategy/data_buy/zecusdt --date 20260916 \
  --output mm_strategy/model_v1/merged_books/ZECUSDT/20260916 \
  --symbol ZECUSDT --tick-size 0.01 --build-npz
```

The directory must contain one file for each of these types for the selected day:

- `bitget-futures_incremental_book_L2_ZECUSDT_20260916.csv.zst`
- `bitget-futures_book_ticker_ZECUSDT_20260916.csv.zst`
- `bitget-futures_trades_ZECUSDT_20260916.csv.zst`

Plain `.csv` and `.csv.gz` are supported too. If there are several dates, select
one with `--date YYYYMMDD` or `YYYY-MM-DD`. Process days sequentially into separate
output directories; the current adapter retains frames in memory for independent
exchange-time and receive-time sorting. No network access or downloads occur.

For custom filenames, provide all three explicit paths instead of `--input`:

```sh
python scripts/merge_bitget_books.py --input-format csv \
  --depth-file /data/incremental_book_L2.csv.zst \
  --book-ticker-file /data/book_ticker.csv.zst \
  --trades-file /data/trades.csv.zst \
  --output /data/merged --symbol ZECUSDT --tick-size 0.01 --build-npz
```

Historical mode fuses **L2 + native-feed book_ticker (books1)**; it does not require
an independent `books5`. `book_snapshot_5` is a derived L2 snapshot dataset and is
not treated as the native 10ms `books5` feed. Raw capture mode continues to use
all three actual channels, including `books5`.

CSV timestamps are Unix microseconds. CSV omits original venue `seq/pseq` and
connection identity, so historical mode does not claim to verify their continuity.
It uses source CSV order only as an internal tiebreaker, not a venue sequence.
Adjacent L2 rows sharing exchange/receive timestamps and snapshot flag are grouped
as a frame; original message boundaries cannot be recovered exactly from CSV.
L2 must start with a full snapshot. Later snapshots reset the visible book and
are counted in `historical_full_snapshots`; they do not prove gap-free coverage.
Wrong symbols, malformed quotes and conflicting duplicate trades are errors.

Outputs have the same names and independent EXCH/LOCAL semantics as raw mode.
`summary.json` records `input_format`, the three historical paths and the missing
sequence/connection validation. Neither mode applies own-liquidity correction.

## Raw capture input

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
