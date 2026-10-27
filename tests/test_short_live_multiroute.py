"""Synthetic cross-exchange routing; never uses actual broker adapters."""
import unittest
from datetime import timedelta
from dataclasses import replace
import test_short_live_session as fixture


class ShortLiveMultiRouteTests(unittest.TestCase):
    def setUp(self):
        self.h = fixture.ShortLiveSessionTests()
        self.h.setUp()

    def tearDown(self):
        self.h.tearDown()

    def service(self, specs):
        service, brokers, feeds = self.h.service(specs)
        # KRX 1h history must skip overnight and preserve the 15:30 shortened
        # final session bar. A continuous 24h crypto fixture is invalid here.
        def krx_warmup(spec, limit):
            assert limit == 5
            previous = fixture.NOW - timedelta(days=1)
            ends = [previous + timedelta(hours=2), previous + timedelta(hours=3),
                    previous + timedelta(hours=3, minutes=30),
                    fixture.NOW - timedelta(hours=2), fixture.NOW - timedelta(hours=1)]
            bars = [fixture.candle(specs[1], end, 5-index) for index, end in enumerate(ends)]
            bars[2] = replace(bars[2], start_time=fixture.stamp(ends[2]-timedelta(minutes=30)))
            return bars
        feeds["KIS_KR_CASH"].warmup = krx_warmup
        return service, brokers, feeds

    def test_btc_and_samsung_same_start_dispatch_only_to_exact_bound_broker(self):
        btc = fixture.runtime_spec("BINANCE_SPOT", "btc", "1h")
        samsung = fixture.runtime_spec("KIS_KR_CASH", "samsung", "1h")
        service, brokers, feeds = self.service([btc, samsung])
        snapshot = self.h.start(service)
        self.assertEqual(snapshot["runtime"]["status"], "RUNNING")
        self.assertEqual(len(service._engines), 2)
        self.assertTrue(all(feed.connected for feed in feeds.values()))
        self.h.first_buy(service, btc)
        self.assertEqual(len(brokers["BINANCE_SPOT"].submits), 1)
        self.assertEqual(brokers["KIS_KR_CASH"].submits, [])
        self.h.first_buy(service, samsung)
        self.assertEqual(len(brokers["KIS_KR_CASH"].submits), 1)
        for spec in (btc, samsung):
            order = brokers[spec["route"]].submits[0]
            self.assertEqual(order["route"], spec["route"])
            self.assertEqual(order["symbol"], spec["symbol"])
            self.assertEqual(order["sessionId"], snapshot["runtime"]["sessionId"])
        rows = service._public_runtime()["strategyRows"]
        self.assertEqual({row["route"] for row in rows}, {"BINANCE_SPOT", "KIS_KR_CASH"})
        self.assertTrue(all(row["positionQuantity"] > 0 for row in rows))
        self.assertTrue(all(row["lastBarAt"] == fixture.stamp(fixture.NOW) for row in rows))

    def test_short_test_joint_stop_is_explicit_not_partial_success(self):
        specs = [fixture.runtime_spec("BINANCE_SPOT", "btc", "1h"),
                 fixture.runtime_spec("KIS_KR_CASH", "samsung", "1h")]
        service, brokers, _ = self.service(specs)
        self.h.start(service)
        brokers["KIS_KR_CASH"].reconcile_known = False
        service.tick()
        snapshot = service.snapshot(include_catalog=False)["runtime"]
        self.assertIn("KIS_KR_CASH:RECONCILIATION_UNKNOWN", str(snapshot["firstFailure"]))
        self.assertTrue(snapshot["newEntriesBlocked"])
        self.assertNotEqual(snapshot["status"], "RUNNING")
        self.assertFalse(any(broker.submits for broker in brokers.values()))

if __name__ == "__main__":
    unittest.main()
