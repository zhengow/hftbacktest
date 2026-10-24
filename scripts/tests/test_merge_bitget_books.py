import csv
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import gzip
import subprocess
import random

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import merge_bitget_books as m


def frame(ts,topic='books1',bids=None,asks=None,local=None,seq=None,action='snapshot'):
    return m.Frame(ts,local or ts+10,topic,seq or ts,action,
                   bids if bids is not None else [(100,1.)],
                   asks if asks is not None else [(102,1.)],ts)


def initial():
    return frame(1,'books',[(100,1.),(99,2.),(98,3.)],[(102,1.),(103,2.),(104,3.)])


class MergeTests(unittest.TestCase):
    def test_retreat_removes_old_best_but_preserves_deeper_levels(self):
        book=m.Merger();book.apply(initial())
        rows,_=book.apply(frame(10,bids=[(99,.5)]))
        self.assertIn((0,100,0.),rows)
        self.assertEqual(book.sides[0].qty,{99:.5,98:3.})

    def test_improved_best_does_not_delete_old_best_now_second(self):
        book=m.Merger();book.apply(initial());book.apply(frame(10,bids=[(101,.5)]))
        self.assertEqual(book.sides[0].qty[100],1.)
        self.assertEqual(book.sides[0].keys[-1],101)

    def test_older_full_update_cannot_resurrect_even_unknown_better_price(self):
        book=m.Merger();book.apply(initial());book.apply(frame(10,bids=[(99,.5)]))
        book.apply(frame(5,'books',[(101,8.),(98,4.)],[],action='update'))
        self.assertNotIn(101,book.sides[0].qty)
        self.assertEqual(book.sides[0].qty[98],4.)
        self.assertEqual(book.sides[0].qty[99],.5)

    def test_equal_ms_books1_wins_even_if_other_channel_sequence_is_larger(self):
        book=m.Merger();book.apply(initial())
        book.apply(frame(10,seq=5,bids=[(100,.2)]))
        book.apply(frame(10,'books5',seq=500,bids=[(100,9.),(99,1.)],asks=[(102,1.),(103,1.)]))
        self.assertEqual(book.sides[0].qty[100],.2)
        self.assertEqual(book.sides[0].qty[99],1.)

    def test_coverage_remembers_older_wider_snapshot(self):
        book=m.Merger();book.apply(initial())
        book.apply(frame(10,bids=[(99,.5)]))
        book.apply(frame(20,bids=[(101,.2)]))
        book.apply(frame(5,'books',[(100,8.)],[],action='update'))
        self.assertNotIn(100,book.sides[0].qty)

    def test_crossed_state_is_not_published(self):
        book=m.Merger();book.apply(initial())
        rows,_=book.apply(frame(10,'books',[(103,1.)],[],action='update'))
        self.assertEqual(rows,[]);self.assertEqual(book.crossed,1)
        rows,_=book.apply(frame(20,bids=[(103,1.)],asks=[(105,1.)]))
        self.assertIn((0,103,1.),rows)
        self.assertNotIn(-102,book.sides[1].qty)  # ask keys use negative prices

    def test_receive_view_does_not_publish_unreceived_full_depth(self):
        frames=[frame(100,'books',local=500),frame(200,local=300,bids=[(100,.2)])]
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'local.csv'
            m.merge_view(frames,p,'TEST',1.,'local')
            with p.open() as f:rows=list(csv.DictReader(f))
            self.assertTrue(rows)
            self.assertTrue(all(int(r['local_timestamp'])>=500 for r in rows))
            self.assertEqual(next(float(r['amount']) for r in rows if r['side']=='bid'),.2)

    def test_independent_views_survive_standard_engine_event_validation(self):
        import numpy as np
        from hftbacktest.types import EXCH_EVENT,LOCAL_EVENT
        from hftbacktest.data.validation import validate_event_order
        frames=[frame(100,'books',local=200),frame(110,local=400,bids=[(100,.2)]),
                frame(120,local=300,bids=[(100,.7)])]
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)
            for view,name in [('exchange','depth.csv'),('local','depth_local.csv')]:
                m.merge_view(frames,p/name,'TEST',1.,view)
            with (p/'trades.csv').open('w',newline='') as f:
                w=csv.writer(f);w.writerow(m.TRADE_FIELDS)
                w.writerow(['bitget-futures','TEST',115,250,'1','sell','100','.1'])
            m.build_npz(p,p/'market.npz')
            data=np.load(p/'market.npz')['data'];validate_event_order(data)
            local=data[(data['ev']&LOCAL_EVENT)!=0]
            self.assertFalse(np.any((local['local_ts']==400000)&(local['qty']==.2)))
            exch=data[(data['ev']&EXCH_EVENT)!=0]
            self.assertTrue(np.any((exch['exch_ts']==110000)&(exch['qty']==.2)))

    def test_broken_full_depth_chain_stops_export(self):
        def event(topic,ts,seq,pseq=0,action='snapshot'):
            return dict(receive_us=ts*1000+10,connection_id='one',message=dict(action=action,
                arg=dict(topic=topic,symbol='TEST'),data=[dict(ts=str(ts),seq=seq,pseq=pseq,
                b=[['100','1']],a=[['102','1']])]))
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw.jsonl'
            items=[event('books',1,1),event('books1',2,2),event('books5',2,2),
                   event('books',3,5,4,'update')]
            p.write_text(''.join(json.dumps(e)+'\n' for e in items))
            with self.assertRaisesRegex(ValueError,'Broken books chain'):m.load(p,'TEST',1.)

    def test_trade_history_is_not_replayed_and_live_trades_are_deduplicated(self):
        def envelope(topic,action,data):
            return dict(receive_us=10000,connection_id='one',message=dict(action=action,
                arg=dict(topic=topic,symbol='TEST'),data=data))
        book=dict(ts='1',seq=1,pseq=0,b=[['100','1']],a=[['102','1']])
        trade=dict(T='2',i='one',S='sell',p='100',v='.005')
        items=[envelope(t,'snapshot',[book]) for t in ['books','books1','books5']]
        items += [envelope('publicTrade','snapshot',[dict(trade,i='history')]),
                  envelope('publicTrade','update',[trade,trade])]
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw.jsonl';p.write_text(''.join(json.dumps(e)+'\n' for e in items))
            frames,trades,counts,connection=m.load(p,'TEST',1.)
            self.assertEqual(len(trades),1)
            self.assertEqual(trades[0][2],'one')
            self.assertEqual(counts['ignored_trade_history_rows'],1)
            self.assertEqual(counts['duplicate_trades'],1)


class HistoricalTests(unittest.TestCase):
    def fixture(self, root):
        """Stale L2 must not resurrect a better quote after faster BBO retreat."""
        names=['bitget-futures_incremental_book_L2_TEST_20260916.csv',
               'bitget-futures_book_ticker_TEST_20260916.csv.gz',
               'bitget-futures_trades_TEST_20260916.csv.zst']
        depth='exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n'
        depth+='bitget-futures,TEST,1000,1100,true,bid,100,1\n'
        depth+='bitget-futures,TEST,1000,1100,true,bid,99,2\n'
        depth+='bitget-futures,TEST,1000,1100,true,ask,102,1\n'
        depth+='bitget-futures,TEST,1000,1100,true,ask,103,2\n'
        depth+='bitget-futures,TEST,1500,2100,false,bid,100,8\n'
        ticker='exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount\n'
        ticker+='bitget-futures,TEST,2000,2050,1,102,99,.5\n'
        trades='exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n'
        trades+='bitget-futures,TEST,2200,2250,t1,sell,99,.5\n'
        import zstandard
        (root/names[0]).write_text(depth)
        with gzip.open(root/names[1],'wt') as f:f.write(ticker)
        (root/names[2]).write_bytes(zstandard.ZstdCompressor().compress(trades.encode()))
        return [root/name for name in names]

    def test_csv_fusion_removes_stale_best_without_fabricated_sequence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.fixture(root)
            frames,trades,counts,connection=m.load_historical(*paths,'TEST',1.)
            self.assertIsNone(connection)
            self.assertEqual(counts['books'],2)
            self.assertEqual(counts['books1'],1)
            self.assertNotIn('books5',counts)
            self.assertEqual(trades[0][:3],(2200,2250,'t1'))
            for view in ['exchange','local']:
                m.merge_view(frames,root/(view+'.csv'),'TEST',1.,view)
                with (root/(view+'.csv')).open() as f:rows=list(csv.DictReader(f))
                book={}
                for r in rows:
                    if r['side']=='bid':book[float(r['price'])]=float(r['amount'])
                self.assertEqual(book[100.],0.)
                self.assertEqual(book[99.],.5)

    def test_historical_cli_auto_discovery_builds_valid_npz(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            command=[sys.executable,str(Path(m.__file__)), '--input',str(root),
                     '--output',str(root/'out'),'--symbol','TEST','--tick-size','1',
                     '--date','2026-09-16','--build-npz']
            result=subprocess.run(command,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            summary=json.loads((root/'out/summary.json').read_text())
            self.assertEqual(summary['input_format'],'csv')
            self.assertFalse(summary['historical']['sequence_continuity_verified'])
            self.assertTrue(summary['npz']['events']>0)

    def test_missing_initial_snapshot_and_wrong_symbol_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.fixture(root)
            with self.assertRaisesRegex(ValueError,'wrong exchange or symbol'):
                m.load_historical(*paths,'OTHER',1.)
            text=paths[0].read_text();paths[0].write_text(text.replace(',true,',',false,'))
            with self.assertRaisesRegex(ValueError,'begin with a complete snapshot'):
                m.load_historical(*paths,'TEST',1.)

    def test_ambiguous_day_requires_selection(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=self.fixture(root)
            (root/paths[0].name.replace('20260916','20260917')).write_text(paths[0].read_text())
            args=argparse.Namespace(input=root,date=None,symbol='TEST',depth_file=None,
                                    book_ticker_file=None,trades_file=None)
            with self.assertRaisesRegex(ValueError,'exactly one historical day'):
                m.historical_paths(args)

    def test_explicit_paths_are_supported_and_partial_set_is_rejected(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            paths=self.fixture(Path(tmp))
            args=argparse.Namespace(input=None,date=None,symbol='TEST',depth_file=paths[0],
                                    book_ticker_file=paths[1],trades_file=paths[2])
            self.assertEqual(m.historical_paths(args),tuple(paths))
            args.trades_file=None
            with self.assertRaisesRegex(ValueError,'together'):m.historical_paths(args)


@unittest.skipUnless((m.NATIVE/'target/release/bitget-merge-native').exists(), 'Build Rust backend before native parity tests')
class NativeParityTests(unittest.TestCase):
    def compare_backends(self,root,paths):
        import numpy as np
        outputs=[]
        for backend in ['python','rust']:
            out=root/backend
            cmd=[sys.executable,str(Path(m.__file__)),'--depth-file',str(paths[0]),
                 '--book-ticker-file',str(paths[1]),'--trades-file',str(paths[2]),
                 '--output',str(out),'--symbol','TEST','--tick-size','1',
                 '--build-npz','--backend',backend]
            result=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            outputs.append(out)
        a=np.load(outputs[0]/'market.npz')['data'];b=np.load(outputs[1]/'market.npz')['data']
        self.assertEqual(a.dtype,b.dtype)
        np.testing.assert_array_equal(a,b)
        sa,sb=[json.loads((out/'summary.json').read_text()) for out in outputs]
        for view in ['exchange','local']:
            for key,value in sa[view].items():self.assertEqual(value,sb[view][key],(view,key))
        self.assertEqual(sa['trade_rows'],sb['trade_rows'])
        for name in ['depth.csv','depth_local.csv','trades.csv']:
            # Text float formatting differs; semantic rows must be identical.
            with (outputs[0]/name).open() as f:ra=list(csv.DictReader(f))
            with (outputs[1]/name).open() as f:rb=list(csv.DictReader(f))
            self.assertEqual(len(ra),len(rb))
            for x,y in zip(ra,rb):
                for k in x:
                    self.assertEqual(float(x[k]),float(y[k])) if k in ['price','amount'] else self.assertEqual(x[k],y[k])

    def test_native_matches_reference_with_compression_and_late_l2(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=HistoricalTests.fixture(self,root)
            self.compare_backends(root,paths)

    def test_npz_only_and_single_worker_match_reference_without_csv_files(self):
        import numpy as np
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=HistoricalTests.fixture(self,root)
            self.compare_backends(root,paths)
            out=root/'fast'
            cmd=[sys.executable,str(Path(m.__file__)),'--depth-file',str(paths[0]),
                 '--book-ticker-file',str(paths[1]),'--trades-file',str(paths[2]),
                 '--output',str(out),'--symbol','TEST','--tick-size','1',
                 '--npz-only','--backend','rust','--jobs','1']
            result=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            np.testing.assert_array_equal(np.load(root/'python/market.npz')['data'],np.load(out/'market.npz')['data'])
            for name in ['depth.csv','depth_local.csv','trades.csv','.exchange.events','.local.events','market.npz.part']:
                self.assertFalse((out/name).exists(),name)
            summary=json.loads((out/'summary.json').read_text())
            self.assertEqual(summary['jobs'],1)
            self.assertFalse(summary['csv_written'])
            self.assertIn('market.npz',summary['sha256'])

    def test_native_error_cleans_temporary_events_and_does_not_mark_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=HistoricalTests.fixture(self,root)
            # Invalid latency is discovered while emitting native events.
            text=paths[0].read_text().replace(',1000,1100,',',1000,999,')
            paths[0].write_text(text)
            out=root/'bad'
            cmd=[sys.executable,str(Path(m.__file__)),'--depth-file',str(paths[0]),
                 '--book-ticker-file',str(paths[1]),'--trades-file',str(paths[2]),
                 '--output',str(out),'--symbol','TEST','--tick-size','1','--npz-only','--backend','rust']
            result=subprocess.run(cmd,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('negative feed latency',result.stdout+result.stderr)
            for name in ['summary.json','.exchange.events','.local.events','market.npz.part']:
                self.assertFalse((out/name).exists(),name)

    def test_native_matches_randomized_resets_ties_and_crossed_views(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);paths=[root/name for name in ['depth.csv','ticker.csv','trades.csv']]
            rng=random.Random(20261024)
            def save(path,fields,rows):
                with path.open('w',newline='') as f:
                    w=csv.writer(f);w.writerow(fields);w.writerows(rows)
            depth=[];ticker=[];trades=[];base=1_700_000_000_000_000
            for i in range(300):
                ts=base+i*1000;local=ts+rng.randint(1,2500)
                snapshot=i in (0,80,160)
                for side,price in [('bid',100+rng.randrange(-3,2)),('ask',105+rng.randrange(-2,3))]:
                    depth.append(['bitget-futures','TEST',ts,local,str(snapshot).lower(),side,price,rng.choice([.005,.01,.1,1.,0.])])
                bt=ts+rng.choice([0,1,500]);bl=bt+rng.randint(1,2500)
                ticker.append(['bitget-futures','TEST',bt,bl,rng.choice([.005,1.,2.]),rng.randrange(102,108),rng.randrange(96,102),rng.choice([.005,.1,1.])])
                trades.append(['bitget-futures','TEST',ts+600,ts+4000,str(i),rng.choice(['buy','sell']),rng.randrange(99,107),.005])
            save(paths[0],m.DEPTH_FIELDS,depth)
            save(paths[1],['exchange','symbol','timestamp','local_timestamp','ask_amount','ask_price','bid_price','bid_amount'],ticker)
            save(paths[2],m.TRADE_FIELDS,trades)
            self.compare_backends(root,paths)


if __name__=='__main__':unittest.main()
