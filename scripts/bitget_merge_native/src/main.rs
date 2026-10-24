//! Historical adapter. Numeric event semantics match merge_bitget_books.py.
//! Compact column storage replaces Python per-level objects; NPZ is streamed.
use anyhow::{Context, Result, bail, ensure};
use csv::{Reader, StringRecord};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use zip::{ZipWriter, write::SimpleFileOptions};

type Clock = (i64, u8, u64);
const EMPTY: Clock = (-1, 0, 0);
const EXCH: u64 = 1 << 31;
const LOCAL: u64 = 1 << 30;
const BUY: u64 = 1 << 29;
const SELL: u64 = 1 << 28;

#[derive(Clone, Copy)]
struct Level {
    price: i64,
    qty: f64,
}
#[derive(Clone, Copy)]
struct Frame {
    ts: i64,
    local: i64,
    ordinal: u64,
    start: usize,
    bids: u32,
    asks: u32,
    topic: u8,
    snapshot: bool,
}
impl Frame {
    fn clock(&self) -> Clock {
        (self.ts, self.topic, self.ordinal)
    }
}
struct Trade {
    ts: i64,
    local: i64,
    id: String,
    side: u64,
    price: f64,
    qty: f64,
    raw_price: String,
    raw_qty: String,
}

fn reader(path: &Path) -> Result<Reader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let name = path.to_string_lossy();
    let input: Box<dyn Read> = if name.ends_with(".zst") {
        Box::new(zstd::stream::read::Decoder::new(f)?)
    } else if name.ends_with(".gz") {
        Box::new(flate2::read::MultiGzDecoder::new(f))
    } else {
        Box::new(BufReader::with_capacity(1 << 20, f))
    };
    Ok(csv::ReaderBuilder::new()
        .buffer_capacity(1 << 20)
        .from_reader(input))
}
struct Columns {
    names: HashMap<String, usize>,
}
impl Columns {
    fn new(r: &mut Reader<Box<dyn Read>>, required: &[&str]) -> Result<Self> {
        let names: HashMap<_, _> = r
            .headers()?
            .iter()
            .enumerate()
            .map(|(i, s)| (s.to_owned(), i))
            .collect();
        for name in required {
            ensure!(names.contains_key(*name), "missing CSV column {name}");
        }
        Ok(Self { names })
    }
    fn get<'a>(&self, r: &'a StringRecord, key: &str) -> &'a str {
        &r[self.names[key]]
    }
    fn times(&self, r: &StringRecord, symbol: &str) -> Result<(i64, i64)> {
        ensure!(
            self.get(r, "exchange") == "bitget-futures" && self.get(r, "symbol") == symbol,
            "wrong exchange or symbol"
        );
        let ts = self.get(r, "timestamp").parse::<i64>()?;
        let local = self.get(r, "local_timestamp").parse::<i64>()?;
        ensure!(
            ts > 0 && local > 0,
            "timestamps must be positive Unix microseconds"
        );
        Ok((ts, local))
    }
}
fn level(p: &str, q: &str, tick: f64) -> Result<Level> {
    let price = p.parse::<f64>()?;
    let qty = q.parse::<f64>()?;
    ensure!(
        price.is_finite() && qty.is_finite() && price > 0. && qty >= 0.,
        "invalid historical price/quantity"
    );
    let n = (price / tick).round();
    ensure!(
        n < (i64::MAX as f64) && (price - n * tick).abs() <= tick * 1e-5,
        "price is not aligned to tick size"
    );
    Ok(Level {
        price: n as i64,
        qty,
    })
}
fn progress(start: Instant, message: impl AsRef<str>) {
    eprintln!(
        "[Rust +{:.1}s] {}",
        start.elapsed().as_secs_f64(),
        message.as_ref()
    );
}

fn load(
    depth: &Path,
    ticker: &Path,
    trades_path: &Path,
    symbol: &str,
    tick: f64,
    start: Instant,
) -> Result<(Vec<Frame>, Vec<Level>, Vec<Trade>, Value)> {
    let mut frames = Vec::new();
    let mut levels = Vec::new();
    let mut snapshots = 0u64;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut group = None;
    let mut book_count = 0u64;
    let flush = |group: Option<(i64, i64, bool)>,
                 bids: &mut Vec<Level>,
                 asks: &mut Vec<Level>,
                 frames: &mut Vec<Frame>,
                 levels: &mut Vec<Level>,
                 book_count: &mut u64| {
        if let Some((ts, local, snapshot)) = group {
            *book_count += 1;
            frames.push(Frame {
                ts,
                local,
                ordinal: frames.len() as u64,
                start: levels.len(),
                bids: bids.len() as u32,
                asks: asks.len() as u32,
                topic: 0,
                snapshot,
            });
            levels.append(bids);
            levels.append(asks);
        }
    };
    let mut r = reader(depth)?;
    let c = Columns::new(
        &mut r,
        &[
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "is_snapshot",
            "side",
            "price",
            "amount",
        ],
    )?;
    let mut record = StringRecord::new();
    let mut rows = 0u64;
    while r.read_record(&mut record)? {
        let (ts, local) = c.times(&record, symbol)?;
        let flag = c.get(&record, "is_snapshot");
        let snapshot = match flag {
            "true" | "True" | "1" => true,
            "false" | "False" | "0" => false,
            _ => bail!("invalid is_snapshot"),
        };
        let key = (ts, local, snapshot);
        if Some(key) != group {
            flush(
                group,
                &mut bids,
                &mut asks,
                &mut frames,
                &mut levels,
                &mut book_count,
            );
            group = Some(key);
            snapshots += snapshot as u64;
        }
        let l = level(c.get(&record, "price"), c.get(&record, "amount"), tick)?;
        match c.get(&record, "side") {
            "bid" | "buy" => bids.push(l),
            "ask" | "sell" => asks.push(l),
            _ => bail!("invalid depth side"),
        }
        rows += 1;
        if rows % 1_000_000 == 0 {
            progress(start, format!("L2 {rows} rows, {} frames", frames.len()));
        }
    }
    flush(
        group,
        &mut bids,
        &mut asks,
        &mut frames,
        &mut levels,
        &mut book_count,
    );
    ensure!(
        frames.first().is_some_and(|f| f.snapshot),
        "historical L2 must begin with a complete snapshot"
    );
    let mut r = reader(ticker)?;
    let c = Columns::new(
        &mut r,
        &[
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "bid_price",
            "bid_amount",
            "ask_price",
            "ask_amount",
        ],
    )?;
    let mut ticker_count = 0u64;
    while r.read_record(&mut record)? {
        let (ts, local) = c.times(&record, symbol)?;
        let bid = level(
            c.get(&record, "bid_price"),
            c.get(&record, "bid_amount"),
            tick,
        )?;
        let ask = level(
            c.get(&record, "ask_price"),
            c.get(&record, "ask_amount"),
            tick,
        )?;
        ensure!(
            bid.qty > 0. && ask.qty > 0. && bid.price < ask.price,
            "book_ticker requires positive sizes and bid < ask"
        );
        frames.push(Frame {
            ts,
            local,
            ordinal: frames.len() as u64,
            start: levels.len(),
            bids: 1,
            asks: 1,
            topic: 2,
            snapshot: true,
        });
        levels.push(bid);
        levels.push(ask);
        ticker_count += 1;
        if ticker_count % 1_000_000 == 0 {
            progress(start, format!("book_ticker {ticker_count} rows"));
        }
    }
    ensure!(ticker_count > 0, "empty historical book_ticker");
    let mut trades = Vec::new();
    let mut seen = HashMap::new();
    let mut duplicates = 0;
    let mut r = reader(trades_path)?;
    let c = Columns::new(
        &mut r,
        &[
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "id",
            "side",
            "price",
            "amount",
        ],
    )?;
    while r.read_record(&mut record)? {
        let (ts, local) = c.times(&record, symbol)?;
        let id = c.get(&record, "id");
        ensure!(!id.is_empty(), "missing Trade ID");
        let side = match c.get(&record, "side") {
            "buy" => BUY,
            "sell" => SELL,
            _ => bail!("invalid Trade side"),
        };
        let l = level(c.get(&record, "price"), c.get(&record, "amount"), tick)?;
        ensure!(l.qty > 0., "Trade quantity must be positive");
        let value = (ts, side, l.price, l.qty.to_bits());
        if let Some(previous) = seen.get(id) {
            ensure!(
                previous == &value,
                "conflicting duplicate historical Trade ID"
            );
            duplicates += 1;
            continue;
        }
        seen.insert(id.to_owned(), value);
        trades.push(Trade {
            ts,
            local,
            id: id.to_owned(),
            side,
            price: c.get(&record, "price").parse()?,
            qty: l.qty,
            raw_price: c.get(&record, "price").to_owned(),
            raw_qty: c.get(&record, "amount").to_owned(),
        });
    }
    progress(
        start,
        format!(
            "loaded {} frames, {} levels, {} trades",
            frames.len(),
            levels.len(),
            trades.len()
        ),
    );
    Ok((
        frames,
        levels,
        trades,
        json!({"books":book_count,"books1":ticker_count,"historical_full_snapshots":snapshots,"duplicate_trades":duplicates}),
    ))
}

#[derive(Default)]
struct Side {
    qty: BTreeMap<i64, f64>,
    clocks: HashMap<i64, Clock>,
    coverage: BTreeMap<i64, Clock>,
    stale: u64,
}
impl Side {
    fn at(&self, key: i64) -> Clock {
        self.coverage
            .range(..=key)
            .next_back()
            .map_or(EMPTY, |(_, v)| *v)
    }
    fn set(&mut self, key: i64, qty: f64, clock: Clock) -> bool {
        if clock
            < self
                .clocks
                .get(&key)
                .copied()
                .unwrap_or(EMPTY)
                .max(self.at(key))
        {
            self.stale += 1;
            return false;
        }
        let old = self.qty.get(&key).copied().unwrap_or(0.);
        self.clocks.insert(key, clock);
        if qty > 0. {
            self.qty.insert(key, qty);
        } else {
            self.qty.remove(&key);
        }
        (old - qty).abs() > 1e-12
    }
    fn protect(&mut self, boundary: i64, clock: Clock) {
        if self.at(boundary) >= clock {
            return;
        }
        // Stable/improving BBO is the common case: update in place rather
        // than allocate a removal vector and rebuild the same tree entry.
        if self
            .coverage
            .last_key_value()
            .is_none_or(|(key, _)| *key <= boundary)
        {
            self.coverage.insert(boundary, clock);
            return;
        }
        let remove: Vec<_> = self
            .coverage
            .range(boundary..)
            .take_while(|(_, v)| **v <= clock)
            .map(|(key, _)| *key)
            .collect();
        for key in remove {
            self.coverage.remove(&key);
        }
        self.coverage.insert(boundary, clock);
    }
    fn snapshot(
        &mut self,
        levels: &[Level],
        clock: Clock,
        sign: i64,
        full: bool,
        pending: &mut BTreeSet<i64>,
    ) -> Result<()> {
        if !full && levels.len() == 1 && levels[0].qty > 0. {
            let key = sign * levels[0].price;
            if self
                .qty
                .last_key_value()
                .is_none_or(|(best, _)| *best <= key)
            {
                if self.set(key, levels[0].qty, clock) {
                    pending.insert(key);
                }
                self.protect(key, clock);
                return Ok(());
            }
        }
        let shown: BTreeMap<_, _> = levels
            .iter()
            .filter(|l| l.qty > 0.)
            .map(|l| (sign * l.price, l.qty))
            .collect();
        ensure!(
            full || !shown.is_empty(),
            "empty partial snapshot side: ambiguous coverage"
        );
        let boundary = if full {
            i64::MIN
        } else {
            *shown.first_key_value().unwrap().0
        };
        let keys: BTreeSet<_> = self
            .qty
            .range(boundary..)
            .map(|(k, _)| *k)
            .chain(shown.keys().copied())
            .collect();
        for key in keys {
            if self.set(key, shown.get(&key).copied().unwrap_or(0.), clock) {
                pending.insert(key);
            }
        }
        self.protect(boundary, clock);
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct Update {
    side: usize,
    price: i64,
    qty: f64,
}
struct Merger {
    sides: [Side; 2],
    published: [HashMap<i64, f64>; 2],
    pending: [BTreeSet<i64>; 2],
    initialized: bool,
    crossed: u64,
    emitted: u64,
}
impl Merger {
    fn new() -> Self {
        Self {
            sides: Default::default(),
            published: Default::default(),
            pending: Default::default(),
            initialized: false,
            crossed: 0,
            emitted: 0,
        }
    }
    fn apply(&mut self, f: &Frame, levels: &[Level]) -> Result<(Vec<Update>, bool)> {
        let split = f.bids as usize;
        let all = &levels[f.start..f.start + split + f.asks as usize];
        for (i, ls) in [all[..split].as_ref(), all[split..].as_ref()]
            .into_iter()
            .enumerate()
        {
            let sign = if i == 0 { 1 } else { -1 };
            if f.snapshot {
                self.sides[i].snapshot(ls, f.clock(), sign, f.topic == 0, &mut self.pending[i])?;
            } else {
                for l in ls {
                    let key = sign * l.price;
                    if self.sides[i].set(key, l.qty, f.clock()) {
                        self.pending[i].insert(key);
                    }
                }
            }
        }
        if f.topic == 0 && f.snapshot {
            self.initialized = true;
        }
        if !self.initialized {
            return Ok((Vec::new(), false));
        }
        let bid = self.sides[0].qty.last_key_value();
        let ask = self.sides[1].qty.last_key_value();
        if bid.is_none() || ask.is_none() || *bid.unwrap().0 >= -*ask.unwrap().0 {
            self.crossed += 1;
            return Ok((Vec::new(), false));
        }
        let bootstrap = self.published.iter().all(|s| s.is_empty());
        let mut updates = Vec::new();
        for i in 0..2 {
            let keys: Vec<_> = if bootstrap {
                self.sides[i].qty.keys().rev().copied().collect()
            } else {
                self.pending[i].iter().rev().copied().collect()
            };
            for key in keys {
                let qty = self.sides[i].qty.get(&key).copied().unwrap_or(0.);
                if bootstrap
                    || (qty - self.published[i].get(&key).copied().unwrap_or(0.)).abs() > 1e-12
                {
                    updates.push(Update {
                        side: i,
                        price: if i == 0 { key } else { -key },
                        qty,
                    });
                    if qty > 0. {
                        self.published[i].insert(key, qty);
                    } else {
                        self.published[i].remove(&key);
                    }
                }
            }
            self.pending[i].clear();
        }
        if !bootstrap {
            updates.sort_unstable_by_key(|u| {
                (
                    u.qty > 0.,
                    u.side,
                    if u.side == 0 { -u.price } else { u.price },
                )
            });
        }
        self.emitted += (!updates.is_empty()) as u64;
        Ok((updates, bootstrap))
    }
}

#[derive(Clone, Copy)]
struct Event {
    ev: u64,
    ts: i64,
    local: i64,
    price: f64,
    qty: f64,
}
impl Event {
    fn bytes(self) -> [u8; 64] {
        let mut b = [0; 64];
        b[0..8].copy_from_slice(&self.ev.to_le_bytes());
        b[8..16].copy_from_slice(&self.ts.to_le_bytes());
        b[16..24].copy_from_slice(&self.local.to_le_bytes());
        b[24..32].copy_from_slice(&self.price.to_le_bytes());
        b[32..40].copy_from_slice(&self.qty.to_le_bytes());
        b
    }
    fn from(b: [u8; 64]) -> Self {
        Self {
            ev: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            ts: i64::from_le_bytes(b[8..16].try_into().unwrap()),
            local: i64::from_le_bytes(b[16..24].try_into().unwrap()),
            price: f64::from_le_bytes(b[24..32].try_into().unwrap()),
            qty: f64::from_le_bytes(b[32..40].try_into().unwrap()),
        }
    }
}
struct EventSink {
    file: BufWriter<File>,
    count: u64,
    pending: Vec<Event>,
    last: Option<i64>,
    flag: u64,
}
impl EventSink {
    fn new(path: &Path, flag: u64) -> Result<Self> {
        Ok(Self {
            file: BufWriter::with_capacity(1 << 20, File::create(path)?),
            count: 0,
            pending: Vec::new(),
            last: None,
            flag,
        })
    }
    fn put(&mut self, e: Event) -> Result<()> {
        let clock = if self.flag == EXCH { e.ts } else { e.local };
        ensure!(
            self.last.is_none_or(|p| clock >= p),
            "depth view processing clock moved backwards"
        );
        ensure!(
            e.local >= e.ts,
            "negative feed latency: align clocks explicitly"
        );
        self.last = Some(clock);
        self.file.write_all(&e.bytes())?;
        self.count += 1;
        Ok(())
    }
    fn frame(&mut self, updates: &[Update], f: &Frame, snapshot: bool, tick: f64) -> Result<()> {
        if snapshot {
            for side in 0..2 {
                let side_rows: Vec<_> = updates.iter().filter(|u| u.side == side).collect();
                if let Some(last) = side_rows.last() {
                    let bit = if side == 0 { BUY } else { SELL };
                    self.pending.push(Event {
                        ev: self.flag | bit | 3,
                        ts: f.ts * 1000,
                        local: f.local * 1000,
                        price: last.price as f64 * tick,
                        qty: 0.,
                    });
                    for u in side_rows {
                        self.pending.push(Event {
                            ev: self.flag | bit | 4,
                            ts: f.ts * 1000,
                            local: f.local * 1000,
                            price: u.price as f64 * tick,
                            qty: u.qty,
                        });
                    }
                }
            }
        } else if !updates.is_empty() {
            // Match _convert_depth: a bootstrap snapshot is flushed at the
            // first non-snapshot delta; a snapshot-only input emits no events.
            for e in std::mem::take(&mut self.pending) {
                self.put(e)?;
            }
            for u in updates {
                self.put(Event {
                    ev: self.flag | if u.side == 0 { BUY } else { SELL } | 1,
                    ts: f.ts * 1000,
                    local: f.local * 1000,
                    price: u.price as f64 * tick,
                    qty: u.qty,
                })?;
            }
        }
        Ok(())
    }
}
fn merge(
    frames: &[Frame],
    levels: &[Level],
    out: &Path,
    symbol: &str,
    tick: f64,
    local: bool,
    build: bool,
    csv_output: bool,
    start: Instant,
) -> Result<Value> {
    let phase_start = Instant::now();
    let name = if local { "local" } else { "exchange" };
    progress(start, format!("sorting {name}: {} frames", frames.len()));
    let mut order: Vec<_> = (0..frames.len()).collect();
    order.sort_unstable_by_key(|i| {
        let f = &frames[*i];
        if local {
            (f.local, 0, f.ordinal)
        } else {
            (f.ts, f.topic, f.ordinal)
        }
    });
    let path = out.join(if local {
        "depth_local.csv"
    } else {
        "depth.csv"
    });
    let mut csv = if csv_output {
        Some(
            csv::WriterBuilder::new()
                .terminator(csv::Terminator::CRLF)
                .buffer_capacity(1 << 20)
                .from_path(path)?,
        )
    } else {
        None
    };
    if let Some(w) = csv.as_mut() {
        w.write_record([
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "is_snapshot",
            "side",
            "price",
            "amount",
        ])?;
    }
    let mut sink = if build {
        Some(EventSink::new(
            &out.join(format!(".{name}.events")),
            if local { LOCAL } else { EXCH },
        )?)
    } else {
        None
    };
    let mut state = Merger::new();
    let mut rows = 0u64;
    let mut ts_text = itoa::Buffer::new();
    let mut local_text = itoa::Buffer::new();
    let mut price_text = ryu::Buffer::new();
    let mut qty_text = ryu::Buffer::new();
    for (n, i) in order.into_iter().enumerate() {
        let f = &frames[i];
        let (updates, snapshot) = state.apply(f, levels)?;
        if let Some(w) = csv.as_mut() {
            let ts = ts_text.format(f.ts);
            let received = local_text.format(f.local);
            for u in &updates {
                w.write_record([
                    "bitget-futures",
                    symbol,
                    ts,
                    received,
                    if snapshot { "true" } else { "false" },
                    if u.side == 0 { "bid" } else { "ask" },
                    price_text.format(u.price as f64 * tick),
                    qty_text.format(u.qty),
                ])?;
            }
        }
        rows += updates.len() as u64;
        if let Some(s) = sink.as_mut() {
            s.frame(&updates, f, snapshot, tick)?;
        }
        if (n + 1) % 500_000 == 0 {
            progress(
                start,
                format!(
                    "{name}: {}/{} frames ({:.1}%); ETA {:.1}s; {rows} output rows",
                    n + 1,
                    frames.len(),
                    (n + 1) as f64 / frames.len() as f64 * 100.,
                    phase_start.elapsed().as_secs_f64() * (frames.len() - n - 1) as f64
                        / (n + 1) as f64
                ),
            );
        }
    }
    if let Some(w) = csv.as_mut() {
        w.flush()?;
    }
    let events = if let Some(mut s) = sink {
        s.file.flush()?;
        s.count
    } else {
        0
    };
    Ok(
        json!({"frames":frames.len(),"depth_rows":rows,"published_frames":state.emitted,"withheld_crossed_or_empty_frames":state.crossed,"stale_level_updates":state.sides.iter().map(|s|s.stale).sum::<u64>(),"events":events,"elapsed_seconds":phase_start.elapsed().as_secs_f64()}),
    )
}
fn next_event(r: &mut BufReader<File>) -> Result<Option<Event>> {
    let mut b = [0; 64];
    let n = r.read(&mut b[..1])?;
    if n == 0 {
        return Ok(None);
    }
    r.read_exact(&mut b[1..])?;
    Ok(Some(Event::from(b)))
}
fn npz(
    out: &Path,
    trades: &[Trade],
    exch_count: u64,
    local_count: u64,
    start: Instant,
) -> Result<Value> {
    progress(
        start,
        "streaming NPZ (no full event array or global event sort)",
    );
    let mut exch = BufReader::with_capacity(1 << 20, File::open(out.join(".exchange.events"))?);
    let mut local = BufReader::with_capacity(1 << 20, File::open(out.join(".local.events"))?);
    let mut et: Vec<_> = (0..trades.len()).collect();
    et.sort_by_key(|i| trades[*i].ts);
    let mut lt: Vec<_> = (0..trades.len()).collect();
    lt.sort_by_key(|i| trades[*i].local);
    let mut ei = 0;
    let mut li = 0;
    let mut de = next_event(&mut exch)?;
    let mut dl = next_event(&mut local)?;
    let count = exch_count + local_count + 2 * trades.len() as u64;
    let part = out.join("market.npz.part");
    let writer = BufWriter::with_capacity(1 << 20, File::create(&part)?);
    let mut zip = ZipWriter::new(writer);
    zip.start_file(
        "data.npy",
        SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .large_file(true),
    )?;
    let mut header = format!(
        "{{'descr': [('ev', '<u8'), ('exch_ts', '<i8'), ('local_ts', '<i8'), ('px', '<f8'), ('qty', '<f8'), ('order_id', '<u8'), ('ival', '<i8'), ('fval', '<f8')], 'fortran_order': False, 'shape': ({count},), }}"
    );
    while (10 + header.len() + 1) % 64 != 0 {
        header.push(' ');
    }
    header.push('\n');
    zip.write_all(b"\x93NUMPY\x01\x00")?;
    zip.write_all(&(header.len() as u16).to_le_bytes())?;
    zip.write_all(header.as_bytes())?;
    let trade_event = |t: &Trade, flag| Event {
        ev: flag | t.side | 2,
        ts: t.ts * 1000,
        local: t.local * 1000,
        price: t.price,
        qty: t.qty,
    };
    let mut written = 0u64;
    loop {
        // Stable concatenate order in the Python reference is LOCAL Trade,
        // EXCH Trade, EXCH depth, LOCAL depth, then processing-clock sort.
        let candidates = [
            lt.get(li).map(|i| (trades[*i].local * 1000, 0)),
            et.get(ei).map(|i| (trades[*i].ts * 1000, 1)),
            de.map(|e| (e.ts, 2)),
            dl.map(|e| (e.local, 3)),
        ];
        let Some((_, kind)) = candidates.into_iter().flatten().min() else {
            break;
        };
        let e = match kind {
            0 => {
                let e = trade_event(&trades[lt[li]], LOCAL);
                li += 1;
                e
            }
            1 => {
                let e = trade_event(&trades[et[ei]], EXCH);
                ei += 1;
                e
            }
            2 => {
                let e = de.unwrap();
                de = next_event(&mut exch)?;
                e
            }
            _ => {
                let e = dl.unwrap();
                dl = next_event(&mut local)?;
                e
            }
        };
        ensure!(
            e.local >= e.ts,
            "negative feed latency: align clocks explicitly"
        );
        zip.write_all(&e.bytes())?;
        written += 1;
        if written % 5_000_000 == 0 {
            progress(start, format!("NPZ {written}/{count} events"));
        }
    }
    ensure!(written == count, "NPZ event count mismatch");
    zip.finish()?.flush()?;
    std::fs::rename(part, out.join("market.npz"))?;
    for name in [".exchange.events", ".local.events"] {
        std::fs::remove_file(out.join(name))?;
    }
    Ok(
        json!({"events":count,"exchange_events":exch_count+trades.len() as u64,"local_events":local_count+trades.len() as u64,"local_view":"independent receive-order fusion; payload may differ from exchange view","writer":"streamed four-way merge; no full event array"}),
    )
}

struct TemporaryFiles(Vec<PathBuf>);
impl Drop for TemporaryFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() >= 7,
        "usage: depth ticker trades output symbol tick [--build-npz] [--npz-only] [--jobs 1|2]"
    );
    let start = Instant::now();
    let out = PathBuf::from(&args[4]);
    let _cleanup = TemporaryFiles(
        [".exchange.events", ".local.events", "market.npz.part"]
            .map(|name| out.join(name))
            .to_vec(),
    );
    let symbol = &args[5];
    let tick = args[6].parse::<f64>()?;
    ensure!(tick.is_finite() && tick > 0., "invalid tick size");
    let mut build = false;
    let mut csv_output = true;
    let mut jobs = 2;
    let mut i = 7;
    while i < args.len() {
        match args[i].as_str() {
            "--build-npz" => build = true,
            "--npz-only" => {
                csv_output = false;
                build = true;
            }
            "--jobs" => {
                i += 1;
                jobs = args.get(i).context("missing jobs")?.parse::<usize>()?;
                ensure!((1..=2).contains(&jobs), "jobs must be 1 or 2");
            }
            _ => bail!("unknown option {}", args[i]),
        };
        i += 1;
    }
    let (frames, levels, trades, channels) = load(
        Path::new(&args[1]),
        Path::new(&args[2]),
        Path::new(&args[3]),
        symbol,
        tick,
        start,
    )?;
    let loaded = start.elapsed().as_secs_f64();
    let merge_start = Instant::now();
    let (exchange, local) = if jobs == 2 {
        std::thread::scope(|scope| -> Result<_> {
            let e = scope.spawn(|| {
                merge(
                    &frames, &levels, &out, symbol, tick, false, build, csv_output, start,
                )
            });
            let l = scope.spawn(|| {
                merge(
                    &frames, &levels, &out, symbol, tick, true, build, csv_output, start,
                )
            });
            Ok((
                e.join()
                    .map_err(|_| anyhow::anyhow!("exchange worker panicked"))??,
                l.join()
                    .map_err(|_| anyhow::anyhow!("local worker panicked"))??,
            ))
        })?
    } else {
        (
            merge(
                &frames, &levels, &out, symbol, tick, false, build, csv_output, start,
            )?,
            merge(
                &frames, &levels, &out, symbol, tick, true, build, csv_output, start,
            )?,
        )
    };
    let merge_wall = merge_start.elapsed().as_secs_f64();
    drop(frames);
    drop(levels);
    if csv_output {
        let mut writer = csv::WriterBuilder::new()
            .terminator(csv::Terminator::CRLF)
            .from_path(out.join("trades.csv"))?;
        writer.write_record([
            "exchange",
            "symbol",
            "timestamp",
            "local_timestamp",
            "id",
            "side",
            "price",
            "amount",
        ])?;
        for t in &trades {
            writer.write_record([
                "bitget-futures",
                symbol,
                &t.ts.to_string(),
                &t.local.to_string(),
                &t.id,
                if t.side == BUY { "buy" } else { "sell" },
                &t.raw_price,
                &t.raw_qty,
            ])?;
        }
        writer.flush()?;
    }
    let phase = json!({"parse":loaded,"exchange":exchange["elapsed_seconds"],"local":local["elapsed_seconds"],"merge_wall":merge_wall});
    let mut summary = json!({"channels":channels,"trade_rows":trades.len(),"exchange":exchange,"local":local,"backend":"rust","jobs":jobs,"csv_written":csv_output,"phase_seconds":phase});
    if build {
        summary["npz"] = npz(
            &out,
            &trades,
            summary["exchange"]["events"].as_u64().unwrap(),
            summary["local"]["events"].as_u64().unwrap(),
            start,
        )?;
    }
    summary["elapsed_seconds"] = json!(start.elapsed().as_secs_f64());
    std::fs::write(
        out.join("native_summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    progress(start, "complete");
    Ok(())
}
