#!/usr/bin/env python3
"""Merge a Bitget capture or historical L2 + book_ticker CSV into causal views.

No exchange API or private-account access. Core matching code is unchanged.
See scripts/README.md for format, ordering and limitations.
"""
from __future__ import annotations

import argparse
import bisect
import csv
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

PRIORITY = {'books': 0, 'books5': 1, 'books1': 2}
DEPTH_FIELDS = ['exchange','symbol','timestamp','local_timestamp','is_snapshot','side','price','amount']
TRADE_FIELDS = ['exchange','symbol','timestamp','local_timestamp','id','side','price','amount']
START = time.monotonic()
NATIVE = Path(__file__).resolve().parent/'bitget_merge_native'


def log(message):
    print(f'[{datetime.now().astimezone().isoformat(timespec="seconds")}] '
          f'+{time.monotonic()-START:.1f}s {message}', flush=True)


@dataclass(slots=True)
class Frame:
    ts: int
    local: int
    topic: str
    seq: int
    action: str
    bids: list
    asks: list
    event_id: int

    @property
    def clock(self):
        # seq is only compared within a channel (priority is before seq).
        # Historical CSV uses source row order here, never a venue sequence.
        return self.ts, PRIORITY[self.topic], self.seq


class Coverage:
    """Monotone version floor for all keys >= a snapshot's worst covered key.

    A tombstone for only existing prices is insufficient: an older full-depth
    message could otherwise resurrect a previously unknown, now-impossible BBO.
    This compact step function also remembers negative information over ranges.
    """
    def __init__(self):
        self.bounds, self.versions = [], []

    def at(self, key):
        i = bisect.bisect_right(self.bounds, key)-1
        return self.versions[i] if i >= 0 else (-1,-1,-1)

    def protect(self, boundary, clock):
        if self.at(boundary) >= clock:
            return
        start = bisect.bisect_left(self.bounds, boundary)
        end = start
        while end < len(self.bounds) and self.versions[end] <= clock:
            end += 1
        self.bounds[start:end] = [boundary]
        self.versions[start:end] = [clock]


class SideBook:
    def __init__(self, sign):
        self.sign = sign  # bigger key = better price, on either side
        self.keys, self.qty, self.clocks = [], {}, {}
        self.coverage = Coverage()
        self.stale = 0

    def set(self, key, qty, clock):
        if clock < max(self.clocks.get(key, (-1,-1,-1)), self.coverage.at(key)):
            self.stale += 1
            return False
        old = self.qty.get(key, 0.)
        self.clocks[key] = clock
        if qty > 0:
            if key not in self.qty:
                bisect.insort(self.keys, key)
            self.qty[key] = qty
        elif key in self.qty:
            del self.qty[key]
            self.keys.pop(bisect.bisect_left(self.keys, key))
        return abs(old-qty) > 1e-12

    def snapshot(self, levels, clock, full=False):
        shown = {self.sign*p:q for p,q in levels if q > 0}
        # Empty side is only accepted for a full snapshot. A partial snapshot
        # with no levels has no unambiguous coverage boundary: fail explicitly.
        if not shown and not full:
            raise ValueError('Empty books1/books5 side: ambiguous coverage')
        boundary = -(1<<63) if full else min(shown)
        covered = self.keys[bisect.bisect_left(self.keys, boundary):]
        changed = set()
        for key in set(covered) | shown.keys():
            if self.set(key, shown.get(key, 0.), clock):
                changed.add(key)
        self.coverage.protect(boundary, clock)
        return changed


class Merger:
    def __init__(self):
        self.sides = [SideBook(1), SideBook(-1)]
        self.initialized = False
        self.published = [{}, {}]
        self.pending = [set(), set()]
        self.crossed, self.frames, self.emitted = 0, 0, 0

    def apply(self, f):
        self.frames += 1
        first = f.topic == 'books' and f.action == 'snapshot' and not self.initialized
        for i, levels in enumerate((f.bids, f.asks)):
            side = self.sides[i]
            if f.action == 'snapshot':
                self.pending[i].update(side.snapshot(levels, f.clock, full=f.topic=='books'))
            else:
                for price, qty in levels:
                    key = side.sign*price
                    if side.set(key, qty, f.clock):
                        self.pending[i].add(key)
        if first:
            self.initialized = True
        if not self.initialized:
            return [], False
        bid, ask = self.sides
        if not bid.keys or not ask.keys or bid.keys[-1] >= -ask.keys[-1]:
            # Retain internal changes, but don't publish a crossed/incomplete
            # mixed-clock view. Publish accumulated diffs once it is consistent.
            self.crossed += 1
            return [], False
        rows = []
        bootstrap = not any(self.published)
        for i, side in enumerate(self.sides):
            keys = set(side.keys) if bootstrap else self.pending[i]
            for key in sorted(keys, reverse=True):
                qty = side.qty.get(key, 0.)
                if bootstrap or abs(qty-self.published[i].get(key, 0.)) > 1e-12:
                    rows.append((i, side.sign*key, qty))
                    if qty:
                        self.published[i][key] = qty
                    else:
                        self.published[i].pop(key, None)
            self.pending[i].clear()
        # Remove obsolete quotes before adding replacements. Snapshot markers
        # only bootstrap once; a partial snapshot must never clear the whole L2.
        if not bootstrap:
            rows.sort(key=lambda r: (r[2] > 0, r[0], -r[1] if r[0]==0 else r[1]))
        self.emitted += bool(rows)
        return rows, bootstrap


def json_events(path):
    paths = sorted(path.glob('events_*.jsonl.zst')) if path.is_dir() else [path]
    if not paths:
        raise ValueError('No finalized JSONL journal files')
    if path.is_dir():
        if list(path.glob('*.part')):
            raise ValueError('Use a frozen/sealed capture, not an active .part file')
        health = path/'health.json'
        if health.exists() and json.loads(health.read_text()).get('dropped', 0):
            raise ValueError('Capture reports dropped events')
    for p in paths:
        with p.open('rb') as f:
            if '.zst' in p.name:
                import zstandard
                with zstandard.ZstdDecompressor().stream_reader(f) as reader:
                    for line in io.BufferedReader(reader):
                        yield json.loads(line)
            else:
                for line in f:
                    yield json.loads(line)


def load(path, symbol, tick):
    frames, trades, seen_trades = [], [], {}
    counts, last, bridge = Counter(), {}, False
    connection = None
    run_id, previous_id = None, -1
    def levels(raw):
        result=[]
        for price, quantity, *_ in raw:
            price, qty = float(price), float(quantity)
            if not math.isfinite(price) or not math.isfinite(qty) or price<=0 or qty<0:
                raise ValueError('Invalid market price/quantity')
            p = round(price/tick)
            if p >= (1<<63) or abs(price-p*tick)>tick*1e-5:
                raise ValueError('Price is not aligned to supplied tick size')
            result.append((p,qty))
        return result
    for number, event in enumerate(json_events(path),1):
        if number % 100000 == 0:
            log(f'读取 {number:,} 条原始记录；盘口 {len(frames):,} 帧')
        if 'kind' in event:
            if event.get('run_id'):
                run_id=run_id or event['run_id']
                if event['run_id']!=run_id or event['event_id']<=previous_id:
                    raise ValueError('Mixed runs or out-of-order journal IDs')
                previous_id=event['event_id']
            if event['kind']=='connection_gap':
                raise ValueError('Connection gap: split capture into healthy sessions')
            if event['kind']!='market_raw':
                continue
            d=event['data']
            if d.get('source')!='sampler_public':
                continue
            msg=json.loads(d['raw']) if isinstance(d['raw'],str) else d['raw']
            local=int(d.get('source_clock_us',event['utc_us']))
            conn=d.get('connection_id')
            event_id=event['event_id']
        else:
            msg=event['message'];local=int(event['receive_us'])
            conn=event['connection_id'];event_id=number
        if msg.get('arg',{}).get('symbol')!=symbol or not msg.get('data'):
            continue
        if conn is None:
            raise ValueError('Missing public connection ID')
        connection=connection or conn
        if conn!=connection:
            raise ValueError('Multiple public connections: segment explicitly')
        topic=msg['arg']['topic']
        if topic=='publicTrade':
            # Initial publicTrade snapshots contain recent history, not new
            # executions at subscription time. Match the existing replay export.
            if msg.get('action')!='update':
                counts['ignored_trade_history_rows']+=len(msg['data'])
                continue
            for t in msg['data']:
                tid=str(t['i']);value=(int(t['T'])*1000,t['S'],str(t['p']),str(t['v']))
                if tid in seen_trades:
                    if seen_trades[tid]!=value:raise ValueError('Conflicting duplicate Trade ID')
                    counts['duplicate_trades']+=1;continue
                seen_trades[tid]=value
                if t['S'] not in ('buy','sell'):raise ValueError('Unknown trade side')
                levels([[t['p'],t['v']]])
                trades.append((value[0],local,tid,t['S'],str(t['p']),str(t['v'])))
            continue
        if topic not in PRIORITY:
            continue
        for row in msg['data']:
            ts=int(row.get('ts',0))*1000;seq=int(row['seq']);action=msg['action']
            if not ts:raise ValueError('Missing row exchange timestamp')
            if topic=='books':
                if action=='snapshot':
                    if topic in last:raise ValueError('Repeated full snapshot/reset: segment capture')
                    bridge=True
                elif action=='update':
                    prev=int(row['pseq'])
                    prior=last.get(topic)
                    if prior is None or not prev or not (prev<=prior<=seq if bridge else prev==prior and seq>prior):
                        raise ValueError(f'Broken books chain: prior={prior}, pseq={prev}, seq={seq}')
                    bridge=False
                else:raise ValueError('Unknown books action')
            else:
                if action!='snapshot':raise ValueError('books1/books5 must be snapshots')
                if seq<=last.get(topic,-1):
                    counts[topic+'_stale_sequence']+=1;continue
            last[topic]=seq
            frames.append(Frame(ts,local,topic,seq,action,levels(row.get('b',[])),levels(row.get('a',[])),event_id))
            counts[topic]+=1
    if not all(counts[t] for t in PRIORITY):
        raise ValueError('All three channels are required')
    return frames,trades,dict(counts),connection


@contextmanager
def csv_input(path):
    """Read plain, gzip or zstd CSV without extracting whole files to disk."""
    if path.name.endswith('.zst'):
        import zstandard
        with path.open('rb') as f, zstandard.ZstdDecompressor().stream_reader(f) as reader:
            with io.TextIOWrapper(reader, newline='') as text:
                yield text
    elif path.name.endswith('.gz'):
        with gzip.open(path, 'rt', newline='') as f:
            yield f
    else:
        with path.open(newline='') as f:
            yield f


def historical_paths(args):
    explicit=[args.depth_file,args.book_ticker_file,args.trades_file]
    if any(explicit):
        if not all(explicit):
            raise ValueError('Historical mode requires --depth-file, --book-ticker-file and --trades-file together')
        if not all(p.is_file() for p in explicit):
            raise ValueError('A historical input file does not exist')
        return tuple(explicit)
    if args.input is None or not args.input.is_dir():
        raise ValueError('Historical input must be a directory, or provide all three explicit CSV file paths')
    date=args.date
    if date:
        date=datetime.strptime(date.replace('-',''),'%Y%m%d').strftime('%Y%m%d')
    types=('incremental_book_L2','book_ticker','trades')
    found={}
    for kind in types:
        pattern=re.compile(rf'^bitget-futures_{kind}_{re.escape(args.symbol)}_(\d{{8}})\.csv(?:\.zst|\.gz)?$')
        for path in args.input.iterdir():
            match=pattern.match(path.name)
            if match and (not date or match[1]==date):
                if (kind,match[1]) in found:
                    raise ValueError(f'Duplicate CSV copies for {kind} {match[1]}; use explicit paths')
                found[kind,match[1]]=path
    dates={d for _,d in found}
    if len(dates)!=1:
        raise ValueError('Select exactly one historical day with --date YYYYMMDD; use a separate output per day')
    day=next(iter(dates))
    missing=[kind for kind in types if (kind,day) not in found]
    if missing:
        raise ValueError('Missing historical CSV types: '+', '.join(missing))
    return tuple(found[kind,day] for kind in types)


def load_historical(depth_file, ticker_file, trades_file, symbol, tick):
    """CSV has no venue sequence or connection ID: do not fabricate either.

    Frame.seq is an internal CSV ordinal only. Raw protocol continuity cannot
    be verified. Adjacent depth rows with identical timestamps/snapshot flag
    form one frame; CSV does not retain original WebSocket message boundaries.
    """
    frames,trades,counts=[],[],Counter()
    def rows(path, fields):
        with csv_input(path) as f:
            reader=csv.DictReader(f)
            if not fields.issubset(reader.fieldnames or []):
                raise ValueError(f'{path.name}: missing columns {sorted(fields-set(reader.fieldnames or []))}')
            for number,r in enumerate(reader,1):
                if r['exchange']!='bitget-futures' or r['symbol']!=symbol:
                    raise ValueError(f'{path.name}:{number}: wrong exchange or symbol')
                ts,local=int(r['timestamp']),int(r['local_timestamp'])
                if ts<=0 or local<=0:
                    raise ValueError(f'{path.name}:{number}: timestamps must be positive Unix microseconds')
                if number%1_000_000==0:
                    log(f'历史 CSV {path.name} 已读取 {number:,} 行')
                yield r,ts,local
    def level(price,qty):
        price,qty=float(price),float(qty)
        if not math.isfinite(price) or not math.isfinite(qty) or price<=0 or qty<0:
            raise ValueError('Invalid historical price/quantity')
        p=round(price/tick)
        if p >= (1<<63) or abs(price-p*tick)>tick*1e-5:
            raise ValueError('Historical price is not aligned to supplied tick size')
        return p,qty
    common={'exchange','symbol','timestamp','local_timestamp'}
    group=None;bids=[];asks=[]
    def flush():
        if group is not None:
            ts,local,snapshot=group
            ordinal=counts['books']+1
            frames.append(Frame(ts,local,'books',ordinal,'snapshot' if snapshot else 'update',
                                bids,asks,len(frames)+1))
            counts['books']+=1
            counts['historical_full_snapshots']+=int(snapshot)
    for r,ts,local in rows(depth_file,common|{'is_snapshot','side','price','amount'}):
        flag=r['is_snapshot'].lower()
        if flag not in ('true','false','1','0'):
            raise ValueError('Invalid is_snapshot in historical L2')
        key=(ts,local,flag in ('true','1'))
        if key!=group:
            flush();group=key;bids=[];asks=[]
        if r['side'] not in ('bid','ask','buy','sell'):
            raise ValueError('Invalid historical depth side')
        (bids if r['side'] in ('bid','buy') else asks).append(level(r['price'],r['amount']))
    flush()
    if not frames or frames[0].action!='snapshot':
        raise ValueError('Historical L2 must begin with a complete snapshot')
    for r,ts,local in rows(ticker_file,common|{'bid_price','bid_amount','ask_price','ask_amount'}):
        bids=[level(r['bid_price'],r['bid_amount'])]
        asks=[level(r['ask_price'],r['ask_amount'])]
        if bids[0][1]<=0 or asks[0][1]<=0 or bids[0][0]>=asks[0][0]:
            raise ValueError('Historical book_ticker requires positive sizes and bid < ask')
        ordinal=counts['books1']+1
        frames.append(Frame(ts,local,'books1',ordinal,'snapshot',bids,asks,len(frames)+1))
        counts['books1']+=1
    if not counts['books1']:
        raise ValueError('Empty historical book_ticker')
    seen={}
    for r,ts,local in rows(trades_file,common|{'id','side','price','amount'}):
        if r['side'] not in ('buy','sell') or not r['id']:
            raise ValueError('Invalid historical Trade side or ID')
        p,q=level(r['price'],r['amount'])
        if q<=0:raise ValueError('Historical Trade quantity must be positive')
        value=(ts,r['side'],p,q)
        if r['id'] in seen:
            if seen[r['id']]!=value:raise ValueError('Conflicting duplicate historical Trade ID')
            counts['duplicate_trades']+=1;continue
        seen[r['id']]=value
        trades.append((ts,local,r['id'],r['side'],r['price'],r['amount']))
    # Already-grouped frame lists may not be ordered by exchange time. Each
    # causal view is sorted independently by merge_view, just as for raw input.
    return frames,trades,dict(counts),None


def merge_view(frames, output, symbol, tick, view):
    state=Merger();rows=0
    order=sorted(range(len(frames)),key=lambda i:
                 (frames[i].ts,PRIORITY[frames[i].topic],frames[i].seq,frames[i].event_id) if view=='exchange'
                 else (frames[i].local,frames[i].event_id))
    with output.open('w',newline='') as f:
        w=csv.writer(f);w.writerow(DEPTH_FIELDS)
        for n,i in enumerate(order,1):
            frame=frames[i]
            changes,snapshot=state.apply(frame)
            for side,price,qty in changes:
                w.writerow(['bitget-futures',symbol,frame.ts,frame.local,str(snapshot).lower(),
                            'bid' if side==0 else 'ask',format(price*tick,'.17g'),format(qty,'.17g')])
            rows+=len(changes)
            if n%100000==0:
                elapsed=time.monotonic()-START
                log(f'{view} {n:,}/{len(order):,} 帧 ({n/len(order):.0%})；输出 {rows:,} 行；累计 {elapsed:.1f}s')
    return dict(frames=state.frames,depth_rows=rows,published_frames=state.emitted,
                withheld_crossed_or_empty_frames=state.crossed,
                stale_level_updates=sum(s.stale for s in state.sides))


def build_npz(output, filename):
    import numpy as np
    import polars as pl
    from hftbacktest.types import event_dtype,EXCH_EVENT,LOCAL_EVENT,TRADE_EVENT,BUY_EVENT,SELL_EVENT
    from hftbacktest.data.utils.tardis import _convert_depth,depth_schema,trade_schema
    from hftbacktest.data.validation import validate_event_order
    arrays=[]
    for name,flag in [('depth.csv',EXCH_EVENT),('depth_local.csv',LOCAL_EVENT)]:
        df=pl.read_csv(output/name,schema=depth_schema)
        a=df.select((pl.col('timestamp')*1000).alias('exch_ts'),
                    (pl.col('local_timestamp')*1000).alias('local_ts'),pl.col('price').alias('px'),
                    pl.col('amount').alias('qty'),
                    pl.when(pl.col('side')=='bid').then(1).otherwise(-1).cast(pl.Int8).alias('side'),
                    pl.col('is_snapshot').cast(pl.Int8)).to_numpy(structured=True)
        snapshot_rows=int(df['is_snapshot'].sum());del df
        result=np.zeros(len(a)+snapshot_rows+4,event_dtype)
        ss_bid=np.zeros(max(1,snapshot_rows+1),event_dtype);ss_ask=np.zeros_like(ss_bid)
        n=_convert_depth(result,a,0,ss_bid,ss_ask,0)
        result=result[:n].copy();result['ev']|=flag;arrays.append(result)
    trades=pl.read_csv(output/'trades.csv',schema=trade_schema)
    for flag in [EXCH_EVENT,LOCAL_EVENT]:
        t=np.zeros(len(trades),event_dtype)
        t['exch_ts']=trades['timestamp'].to_numpy()*1000;t['local_ts']=trades['local_timestamp'].to_numpy()*1000
        t['px']=trades['price'].to_numpy();t['qty']=trades['amount'].to_numpy()
        t['ev']=np.where(trades['side'].to_numpy()=='buy',BUY_EVENT,SELL_EVENT)|TRADE_EVENT|flag
        arrays.insert(0,t)
    data=np.concatenate(arrays);del arrays
    # Each view has its own payload. Merging by processing clock preserves both
    # causal orders without copying exchange-fused state into the LOCAL view.
    clock=np.where((data['ev']&EXCH_EVENT)!=0,data['exch_ts'],data['local_ts'])
    data=data[np.argsort(clock,kind='stable')]
    validate_event_order(data)
    if np.any(data['local_ts']<data['exch_ts']):
        raise ValueError('Negative feed latency: align capture clocks explicitly; raw times are not silently changed')
    np.savez(filename,data=data)
    return dict(events=len(data),exchange_events=int(((data['ev']&EXCH_EVENT)!=0).sum()),
                local_events=int(((data['ev']&LOCAL_EVENT)!=0).sum()),
                local_view='independent receive-order fusion; payload may differ from exchange view')


def native_binary():
    binary=NATIVE/'target/release/bitget-merge-native'
    sources=[NATIVE/'Cargo.toml',NATIVE/'Cargo.lock',*NATIVE.glob('src/**/*.rs')]
    modified=max(p.stat().st_mtime_ns for p in sources if p.exists())
    if not binary.exists() or binary.stat().st_mtime_ns<modified:
        if not shutil.which('cargo'):
            raise ValueError('Rust backend needs cargo for the first build; install Rust or use --backend python')
        log('首次构建或更新 Rust 历史融合后端（编译最多使用 2 个任务）')
        env=dict(os.environ,CARGO_TARGET_DIR=str(NATIVE/'target'))
        subprocess.run(['cargo','build','--locked','--release','--manifest-path',str(NATIVE/'Cargo.toml'),'-j','2'],env=env,check=True)
    return binary


def run_native(paths,args):
    binary=native_binary()
    with binary.open('rb') as f:
        binary_hash=hashlib.file_digest(f,'sha256').hexdigest()
    command=[str(binary),*(str(p.resolve()) for p in paths),str(args.output.resolve()),args.symbol,str(args.tick_size)]
    command+=['--jobs',str(args.jobs)]
    if args.build_npz:command.append('--build-npz')
    if args.npz_only:command.append('--npz-only')
    log('Rust：解压/解析历史数据、融合独立时间视角、流式输出')
    subprocess.run(command,check=True)
    result=json.loads((args.output/'native_summary.json').read_text())
    result['native_binary_sha256']=binary_hash
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,help='Sealed raw journal/file, or historical CSV directory')
    p.add_argument('--input-format',choices=['auto','raw','csv'],default='auto')
    p.add_argument('--backend',choices=['auto','python','rust'],default='auto',
                   help='auto: prefer Rust for historical CSV; raw capture uses Python')
    p.add_argument('--jobs',type=int,choices=[1,2],default=2,
                   help='Rust fusion workers: 2 independent time views by default; use 1 to reduce CPU/memory')
    p.add_argument('--date',help='Historical CSV day, YYYYMMDD or YYYY-MM-DD')
    p.add_argument('--depth-file',type=Path,help='Explicit incremental_book_L2 CSV[.gz/.zst]')
    p.add_argument('--book-ticker-file',type=Path,help='Explicit book_ticker CSV[.gz/.zst]')
    p.add_argument('--trades-file',type=Path,help='Explicit trades CSV[.gz/.zst]')
    p.add_argument('--output',required=True,type=Path)
    p.add_argument('--symbol',required=True)
    p.add_argument('--tick-size',required=True,type=float)
    p.add_argument('--build-npz',action='store_true')
    p.add_argument('--npz-only',action='store_true',help='Rust historical mode: write market.npz and metadata without large CSV outputs; implies --build-npz')
    p.add_argument('-f','--force',action='store_true')
    args=p.parse_args()
    if not math.isfinite(args.tick_size) or args.tick_size<=0:p.error('tick-size must be positive')
    if args.input is None and not any([args.depth_file,args.book_ticker_file,args.trades_file]):
        p.error('Provide --input, or all three historical CSV paths')
    if args.input is not None and not args.input.exists():p.error('Input does not exist')
    mode=args.input_format
    if mode=='auto':
        raw=args.input is not None and (
            args.input.is_dir() and any(args.input.glob('events_*.jsonl.zst'))
            or args.input.is_file() and '.jsonl' in args.input.name)
        mode='raw' if raw and not any([args.depth_file,args.book_ticker_file,args.trades_file]) else 'csv'
    if mode=='raw' and (args.input is None or args.date or any([args.depth_file,args.book_ticker_file,args.trades_file])):
        p.error('Raw mode requires --input and does not accept historical CSV/date options')
    if mode=='raw' and args.backend=='rust':p.error('Rust backend currently supports historical CSV; use --backend python or auto for raw capture')
    backend=args.backend
    if backend=='auto':
        available=(NATIVE/'target/release/bitget-merge-native').exists() or shutil.which('cargo')
        backend='rust' if mode=='csv' and available else 'python'
        if mode=='csv' and backend=='python':log('未找到 Rust 后端/编译器，使用 Python；可安装 Rust 以加速历史融合')
    if args.npz_only:
        if backend!='rust':p.error('--npz-only requires Rust historical backend')
        args.build_npz=True
    try:
        paths=historical_paths(args) if mode=='csv' else None
    except ValueError as error:
        p.error(str(error))
    generated=['depth.csv','depth_local.csv','trades.csv','market.npz','native_summary.json','summary.json']
    if paths and any(path.resolve() in {(args.output/name).resolve() for name in generated} for path in paths):
        p.error('Output files would overwrite historical input; use a separate output directory')
    if args.output.exists() and any(args.output.iterdir()) and not args.force:
        p.error('Output exists; choose a new directory or pass -f')
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'summary.json').unlink(missing_ok=True)
    if args.force:
        for name in ['depth.csv','depth_local.csv','trades.csv','market.npz','native_summary.json']:
            (args.output/name).unlink(missing_ok=True)
    native=None
    if backend=='rust':
        native=run_native(paths,args)
        counts=native['channels'];connection=None;trade_rows=native['trade_rows']
    elif mode=='raw':
        log('读取三个盘口频道及公开 Trade；只处理一个健康连接')
        frames,trades,counts,connection=load(args.input,args.symbol,args.tick_size)
    else:
        log('读取历史 L2 + book_ticker + trades CSV；不伪造序列号或独立 books5')
        frames,trades,counts,connection=load_historical(*paths,args.symbol,args.tick_size)
    if native is None:
        log(f'读取完成：{len(frames):,} 帧，{len(trades):,} 笔公开成交')
        trade_rows=len(trades)
    summary=dict(symbol=args.symbol,tick_size=args.tick_size,input_format=mode,backend=backend,connection_id=connection,channels=counts,
                 trade_rows=trade_rows,tie_policy=('row timestamp, then books < books5 < books1; seq only within channel'
                     if mode=='raw' else 'row timestamp, then L2 < book_ticker; source CSV order within each stream; equal receive-time ties use L2 before book_ticker'),
                 sources=['https://www.bitget.com/docs/uta/websocket/public/Order-Book-Channel',
                          'https://www.bitget.com/docs/uta/best-practices-guide'])
    if mode=='csv':
        summary['historical']=dict(files={kind:str(path.resolve()) for kind,path in
            zip(['incremental_book_L2','book_ticker','trades'],paths)},
            venue_sequence_available=False,sequence_continuity_verified=False,
            internal_order='CSV ordinal is an internal tiebreaker, not a reconstructed exchange sequence',
            frame_boundaries='Adjacent L2 rows sharing exchange/receive timestamps and snapshot flag; original message boundaries unavailable',
            independent_books5=False,
            limitations=['No seq/pseq or connection ID: cannot verify original packet continuity or connection identity.',
                         'book_snapshot_5 is not used as a native high-frequency books5 feed.',
                         'CSV row order breaks same-channel timestamp ties; cross-channel physical order remains uncertain.',
                         'Repeated L2 snapshots reset the visible book; retained in historical_full_snapshots count.'])
    if native is not None:
        summary.update(native)
    else:
        for view,name in [('exchange','depth.csv'),('local','depth_local.csv')]:
            summary[view]=merge_view(frames,args.output/name,args.symbol,args.tick_size,view)
        with (args.output/'trades.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow(TRADE_FIELDS)
            for t in trades:w.writerow(['bitget-futures',args.symbol,*t])
        if args.build_npz:
            log('构造 hftbacktest EXCH/LOCAL 独立事件流')
            summary['npz']=build_npz(args.output,args.output/'market.npz')
    for name in (['market.npz'] if args.npz_only else ['depth.csv','depth_local.csv','trades.csv']):
        with (args.output/name).open('rb') as f:
            summary.setdefault('sha256',{})[name]=hashlib.file_digest(f,'sha256').hexdigest()
    summary['elapsed_seconds']=time.monotonic()-START
    (args.output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    log(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':
    main()
