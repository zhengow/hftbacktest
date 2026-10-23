"""Disabling the coarse-feed heuristic must preserve normal GTX admission checks."""
import os
import unittest
from unittest.mock import patch

import numpy as np
from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest, GTX, LIMIT
from hftbacktest.order import EXPIRED, FILLED
from hftbacktest.types import event_dtype, EXCH_EVENT, LOCAL_EVENT, DEPTH_EVENT, BUY_EVENT, SELL_EVENT


class PostOnlyWindowTests(unittest.TestCase):
    def replay(self, window_ns):
        data=np.zeros(4,event_dtype)
        data['ev']=[DEPTH_EVENT|BUY_EVENT,DEPTH_EVENT|SELL_EVENT,
                    DEPTH_EVENT|SELL_EVENT,DEPTH_EVENT|BUY_EVENT]
        data['ev']|=EXCH_EVENT|LOCAL_EVENT
        data['exch_ts']=[1_000_000_000,1_000_000_000,1_015_000_000,2_000_000_000]
        data['local_ts']=data['exch_ts']
        data['px']=[99.,101.,100.,99.];data['qty']=10.
        with patch.dict(os.environ,{'HFTBACKTEST_POST_ONLY_REJECT_NS':str(window_ns)}):
            asset=(BacktestAsset().data([data]).linear_asset(1.)
                   .constant_order_latency(0,0).partial_fill_exchange()
                   .risk_adverse_queue_model().book_fill_mode(0)
                   .tick_size(1.).lot_size(1.).trading_value_fee_model(0.,0.))
            hbt=HashMapMarketDepthBacktest([asset])
        try:
            hbt.elapse(1)
            hbt.elapse(1_010_000_000-hbt.current_timestamp)
            # 100 is passive on arrival; the opposite quote reaches it 5ms later.
            hbt.submit_buy_order(0,1,100.,1.,GTX,LIMIT,False)
            # 101 crosses immediately and must always be rejected.
            hbt.submit_buy_order(0,2,101.,1.,GTX,LIMIT,False)
            hbt.elapse(30_000_000)
            return hbt.orders(0).get(1).status,hbt.orders(0).get(2).status,hbt.position(0)
        finally:
            hbt.close()

    def test_zero_window_preserves_admission_rejection_and_fast_maker_fill(self):
        passive,crossing,position=self.replay(0)
        self.assertEqual(passive,FILLED)
        self.assertEqual(crossing,EXPIRED)
        self.assertEqual(position,1.)

    def test_20ms_heuristic_rejects_the_same_legitimate_5ms_fill(self):
        passive,crossing,position=self.replay(20_000_000)
        self.assertEqual(passive,EXPIRED)
        self.assertEqual(crossing,EXPIRED)
        self.assertEqual(position,0.)


if __name__=='__main__':unittest.main()
