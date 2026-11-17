"""Real read-only preparation flow with synthetic broker/chart methods only."""
from pathlib import Path
import sys
import unittest

APP = Path(__file__).resolve().parents[1]
SHARED_TESTS = APP.parents[1] / 'packages/trading_runtime/tests'
for path in (APP/'scripts', SHARED_TESTS):
    if str(path) not in sys.path: sys.path.insert(0, str(path))
from test_stock_indicator_basis import TossPairClient, toss_spec
import test_toss_cash_operation as base
from trading_runtime.provider_backfill_toss import TossClosedBarFeed
from trading_runtime.stock_indicator_basis import CODE


class TossIndicatorBasisPreparationTests(unittest.TestCase):
    def harness(self, market='KR', neutral=True):
        case = base.OperationTests(); case.setUp(); self.addCleanup(case.doCleanups)
        original = TossPairClient(market=market, neutral=neutral)
        case.clock.now = original.now.timestamp()
        case.specs[:] = [toss_spec(original)]
        api = case.api
        api.client_id = original.client_id
        api._stock_session_source = original._stock_session_source
        def candles(*args, **kwargs):
            value = original.candles(*args, **kwargs)
            api.last_observation = original.last_observation
            return value
        api.candles = candles
        old_stocks = api.stocks
        api.stocks = lambda symbols:[{**row, 'koreanMarketDetail':{'nxtSupported':True,'nxtTradingSuspended':False}}
                                    for row in old_stocks(symbols)]
        case.op.feed_factory = lambda specs, api:TossClosedBarFeed.from_specs(specs, client=api,
            cache=object(), now=case.clock)
        return case, original

    def test_neutral_current_pair_prepares_kr_us_without_writable_connection_or_orders(self):
        for market in ('KR','US'):
            with self.subTest(market=market):
                case, original = self.harness(market)
                result = case.prepare()
                self.assertTrue(result['readOnly']); self.assertEqual(result['ordersSubmitted'],0)
                self.assertEqual([False,True],[row['adjusted'] for row in original.calls])
                self.assertEqual(case.api.calls,[]); self.assertFalse(any(case.writable))
                self.assertFalse(case.op.ledger.path.exists())

    def test_nonneutral_current_pair_refuses_preparation_without_side_effects(self):
        for market in ('KR','US'):
            with self.subTest(market=market):
                case, original = self.harness(market,False)
                with self.assertRaisesRegex(ValueError,CODE):case.prepare()
                self.assertIsNone(case.op.prepared); self.assertEqual(case.api.calls,[])
                self.assertFalse(any(case.writable)); self.assertFalse(case.op.ledger.path.exists())


if __name__ == '__main__':unittest.main()
