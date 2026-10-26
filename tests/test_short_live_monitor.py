"""Synthetic display projection checks. No real .env, credentials or network."""
import unittest
from test_short_live_brokers import Fixture
import test_short_live_session as session_fixture

class ShortLiveMonitorTests(unittest.TestCase):
    def test_signed_native_equity_change_does_not_relax_loss_guard(self):
        f = Fixture()
        self.assertTrue(f.adapter.readiness(f.manifest)['ready'])
        f.truth['equityNative'] = '105000'
        positive = f.adapter.reconcile(f.manifest)
        self.assertEqual(positive['realizedLossKrw'], '0')
        self.assertEqual(positive['monitorEquity']['baselineNative'], '100000')
        self.assertEqual(positive['monitorEquity']['currentNative'], '105000')
        f.truth['equityNative'] = '95000'
        negative = f.adapter.reconcile(f.manifest)
        self.assertEqual(negative['realizedLossKrw'], '5000')
        self.assertEqual(negative['monitorEquity']['currentNative'], '95000')
        self.assertFalse(negative['monitorEquity']['cashFlowAdjusted'])
        self.assertEqual(f.calls, [])

    def test_public_activity_is_bounded_whitelisted_and_read_only(self):
        h = session_fixture.ShortLiveSessionTests(); h.setUp()
        try:
            service, brokers, _ = h.service()
            h.start(service)
            for index in range(75):
                service._event('BAR_EVALUATED', route='BINANCE_SPOT', accountId='SYNTHETIC_PRIVATE', rawReply={'apiKey':'SYNTHETIC_PRIVATE'})
            count_before = len(list(service.store_root.glob('*')))
            first = service.snapshot(include_catalog=False)['runtime']
            second = service.snapshot(include_catalog=False)['runtime']
            self.assertEqual(len(first['recentActivity']), 60)
            self.assertEqual(first['recentActivity'], second['recentActivity'])
            self.assertNotIn('SYNTHETIC_PRIVATE', str(first['recentActivity']))
            self.assertEqual(len(list(service.store_root.glob('*'))), count_before)
            self.assertEqual(brokers['BINANCE_SPOT'].submits, [])
            service._monitor_events.append({'sessionId':'other-session','kind':'STARTED','observedAt':'2020-01-01T00:00:00Z'})
            self.assertTrue(all(row['sessionId'] == service._session['sessionId'] for row in service._public_runtime()['recentActivity']))
        finally:
            h.tearDown()



class PassiveFeedProjectionTests(unittest.TestCase):
    def test_cached_socket_receipt_only_and_cycles_are_bounded(self):
        from types import SimpleNamespace
        from live_trader.short_live_monitor import public_feed_observation
        leaf=SimpleNamespace(socket=object(),last_wire_received_at=1793000000,api_key='PRIVATE')
        parent=SimpleNamespace(children=[leaf])
        parent.children.append(parent)
        data=public_feed_observation(parent)
        self.assertEqual(len(data['streams']),1)
        self.assertTrue(data['streams'][0]['socketPresent'])
        self.assertNotIn('PRIVATE',str(data))
        self.assertTrue(data['streams'][0]['lastFrameAt'])
        self.assertIsNone(public_feed_observation(SimpleNamespace())['streams'][0]['socketPresent'])
        self.assertIsNone(public_feed_observation(SimpleNamespace(last_wire_received_at=float('nan')))['streams'][0]['lastFrameAt'])

if __name__ == '__main__': unittest.main()
