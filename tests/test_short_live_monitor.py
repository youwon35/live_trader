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


class EquityHistoryProjectionTests(unittest.TestCase):
    def sample(self, at=1792652400.0, currency="USDT", current="101"):
        return {"accountObservedAt": at, "accountId": "PRIVATE",
                "monitorEquity": {"basis": "OBSERVED_NATIVE_EQUITY_CHANGE",
                    "baselineNative": "100", "currentNative": current, "quoteCurrency": currency,
                    "krwPerQuote": "1400" if currency == "USDT" else "1",
                    "baselineObservedAt": 1792652400.0,
                    "fxObservedAt": at, "cashFlowAdjusted": False,
                    "preservedSpotExcluded": True, "privateReply": "PRIVATE"}}

    def test_real_receipt_per_route_and_poll_cannot_generate_samples(self):
        from live_trader.short_live_monitor import append_equity_observation
        history = []
        self.assertTrue(append_equity_observation(history, "BINANCE_SPOT", self.sample()))
        self.assertFalse(append_equity_observation(history, "BINANCE_SPOT", self.sample()))
        self.assertFalse(append_equity_observation(history, "BINANCE_SPOT", self.sample(1792652415)))
        self.assertTrue(append_equity_observation(history, "UPBIT_KRW_SPOT", self.sample(currency="KRW")))
        self.assertNotIn("PRIVATE", str(history))
        self.assertEqual(history[0]["currentNative"], "101")
        self.assertFalse(history[0]["cashFlowAdjusted"])
        self.assertEqual(len(history), 2)

    def test_invalid_missing_or_regressing_observation_does_not_invent_point(self):
        from live_trader.short_live_monitor import append_equity_observation
        from copy import deepcopy
        history = []
        valid = self.sample()
        self.assertTrue(append_equity_observation(history, "BINANCE_SPOT", valid))
        for timestamp in (None, "not-a-time", float("nan"), 1792652300, "2026-10-22T00:00:00"):
            value = deepcopy(valid); value["accountObservedAt"] = timestamp
            self.assertFalse(append_equity_observation(history, "BINANCE_SPOT", value))
        for field, value in (("baselineNative", None), ("currentNative", "NaN"),
                             ("krwPerQuote", "0"), ("fxObservedAt", None),
                             ("quoteCurrency", "PRIVATE"), ("basis", "ARBITRARY_PRIVATE")):
            row = self.sample(1792652460); row["monitorEquity"][field] = value
            self.assertFalse(append_equity_observation(history, "BINANCE_SPOT", row))
        self.assertEqual(len(history), 1)

    def test_bounded_per_route_retains_real_native_amounts_and_fx_times(self):
        from live_trader.short_live_monitor import append_equity_observation
        history = []
        for index in range(725):
            append_equity_observation(history, "BINANCE_SPOT", self.sample(1792652400+60*index, current=str(100+index)))
        append_equity_observation(history, "UPBIT_KRW_SPOT", self.sample(currency="KRW"))
        self.assertEqual(sum(row["route"] == "BINANCE_SPOT" for row in history), 720)
        self.assertEqual(len(history), 721)
        self.assertEqual(history[-1]["currentNative"], "824")
        self.assertEqual(history[-1]["observedAt"], history[-1]["fxObservedAt"])

    def test_adapter_receipt_timestamp_comes_from_reader_not_ui_poll(self):
        f = Fixture()
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        value = f.adapter.reconcile(f.manifest)
        from live_trader.short_live_monitor import public_observed_at
        self.assertEqual(value["accountObservedAt"], public_observed_at(f.truth["observedAt"]))
        self.assertEqual(value["monitorEquity"]["baselineObservedAt"], value["accountObservedAt"])
        self.assertEqual(value["monitorEquity"]["fxBasis"], "IDENTITY_KRW")
        self.assertFalse(f.calls)

    def test_session_only_records_reconcile_samples_and_whitelists_strategy(self):
        from copy import deepcopy
        h = session_fixture.ShortLiveSessionTests(); h.setUp()
        try:
            service, brokers, _ = h.service()
            h.start(service)
            broker = brokers["BINANCE_SPOT"]; original = broker.reconcile
            broker.reconcile = lambda session: {**original(session), **self.sample()}
            service._reconcile()
            first = service.snapshot(include_catalog=False)["runtime"]
            self.assertEqual(len(first["equityHistory"]), 1)
            for _ in range(4):
                self.assertEqual(service.snapshot(include_catalog=False)["runtime"]["equityHistory"], first["equityHistory"])
            first["equityHistory"][0]["currentNative"] = "9999"
            self.assertEqual(service._public_runtime()["equityHistory"][0]["currentNative"], "101")
            sleeve = service._session["sleeves"]["instance-1"]
            sleeve.update(lastPrice=42.5, lastBarAt="2026-10-22T03:00:00Z", lastDecisionAt="2026-10-22T03:00:01Z")
            service._session["orders"]["synthetic-pending"] = {
                "status": "ACCEPTED", "order": {"route": "BINANCE_SPOT"},
                "allocationWeights": {"instance-1": 1}}
            row = service._public_runtime()["strategyRows"][0]
            self.assertEqual(row["lastPrice"], 42.5)
            self.assertEqual(row["pendingOrderCount"], 1)
            self.assertNotIn("PRIVATE", str(service._public_runtime()))
            self.assertFalse(broker.submits)
        finally:
            h.tearDown()


if __name__ == '__main__': unittest.main()
