"""Offline sequencing regressions; injected feeds never touch a broker."""
import unittest
from datetime import timedelta
import test_short_live_session as fixture


class AutomatedPreparationTests(unittest.TestCase):
    def setUp(self):
        self.h = fixture.ShortLiveSessionTests()
        self.h.setUp()

    def tearDown(self):
        self.h.tearDown()

    def test_all_three_connections_precede_warmup_with_one_cutoff(self):
        specs = [fixture.runtime_spec(route, route, '1h') for route in
                 ('BINANCE_SPOT', 'BINANCE_USDM_FUTURES', 'UPBIT_KRW_SPOT')]
        service, brokers, feeds = self.h.service(specs)
        cutoffs = []
        for feed in feeds.values():
            feed.set_history_window = cutoffs.append
            original = feed.warmup
            def warmup(spec, limit, original=original):
                self.assertTrue(all(feed.connected for feed in feeds.values()))
                self.h.now += timedelta(seconds=2)
                return original(spec, limit)
            feed.warmup = warmup
        self.h.start(service)
        self.assertEqual(cutoffs, [int(fixture.NOW.timestamp())] * 3)
        self.assertFalse(any(broker.submits for broker in brokers.values()))

    def test_incomplete_history_blocks_start_without_connecting_or_ordering(self):
        service, brokers, feeds = self.h.service()
        service.history_preparer = lambda specs, through: {
            'schemaVersion': 'history-preparation-v1', 'ready': False, 'streams': []}
        prepared = self.h.prepare(service)['prepared']
        self.assertIn('HISTORY_PREPARATION_INCOMPLETE', prepared['blockers'])
        self.assertFalse(prepared['readyToStart'])
        self.assertFalse(any(feed.connected for feed in feeds.values()))
        self.assertFalse(any(broker.submits for broker in brokers.values()))

    def test_profile_survives_service_restart_without_preparation_or_start(self):
        service, brokers, feeds = self.h.service()
        self.h.prepare(service)
        reloaded = fixture.ShortLiveTestService(service.catalog_provider, service.broker_provider,
            service.feed_provider, service.store_root, background=False)
        snapshot = reloaded.snapshot(include_catalog=False)
        self.assertIsNotNone(snapshot['savedProfile'])
        self.assertIsNone(snapshot['prepared'])
        self.assertIsNone(snapshot['runtime'])
        self.assertFalse(any(feed.connected for feed in feeds.values()))
        self.assertFalse(any(broker.submits for broker in brokers.values()))

    def test_terminal_report_persists_without_claiming_no_signal_success(self):
        service, brokers, _ = self.h.service()
        self.h.start(service)
        result = service.stop({'reason': 'OPERATOR'})['runtime']
        self.assertTrue(result['terminalVerified'])
        report = result['completionReport']
        self.assertEqual(report['outcome'], 'NO_EVALUATIONS')
        self.assertFalse(report['promotionEligible'])
        self.assertTrue((service.store_root/f"session-report-{report['sessionId']}.json").is_file())


if __name__ == '__main__':
    unittest.main()
