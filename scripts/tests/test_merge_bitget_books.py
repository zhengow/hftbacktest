import csv
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

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


if __name__=='__main__':unittest.main()
