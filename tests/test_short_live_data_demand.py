"""Call-count regressions using injected readers; no credentials or network."""
import copy
import unittest

from test_short_live_brokers import Fixture, NOW, mod
import test_short_live_broker_readers as reader_tests


class FlatReconciliationDemandTests(unittest.TestCase):
    def prepare(self, route):
        fixture = Fixture(route)
        self.assertTrue(fixture.adapter.readiness(fixture.manifest)["ready"])
        calls = []

        def account(*_):
            calls.append("account")
            return copy.deepcopy(fixture.truth)

        def market(*_):
            calls.append("market")
            return copy.deepcopy(fixture.quote)

        def conversion(actual_route):
            calls.append("fx")
            self.assertEqual(route, actual_route)
            return {"known": True, "quoteCurrency": fixture.currency,
                    "krwPerQuote": fixture.quote["krwPerQuote"],
                    "fxObservedAt": fixture.quote["fxObservedAt"]}

        fixture.adapter.snapshot_reader = account
        fixture.adapter.market_reader = market
        fixture.adapter.conversion_reader = conversion
        return fixture, calls

    def test_flat_reconciliation_reads_fresh_account_and_fx_without_order_metadata(self):
        for route in ("BINANCE_SPOT", "BINANCE_USDM_FUTURES"):
            with self.subTest(route=route):
                fixture, calls = self.prepare(route)
                for _ in range(3):
                    result = fixture.adapter.reconcile(fixture.manifest)
                    self.assertTrue(result["known"], result)
                    self.assertEqual("0", result["exposureKrw"])
                self.assertEqual(["account", "fx"] * 3, calls)

    def test_flat_cash_still_measures_native_loss_with_current_fx(self):
        fixture, calls = self.prepare("BINANCE_SPOT")
        fixture.truth["equityNative"] = "99960"
        fixture.quote["krwPerQuote"] = "1400"
        result = fixture.adapter.reconcile(fixture.manifest)
        self.assertTrue(result["known"], result)
        self.assertEqual("56000", result["realizedLossKrw"])
        self.assertEqual(["account", "fx"], calls)

    def test_stale_future_wrong_currency_and_unknown_fx_do_not_pass(self):
        for value, code in (
            ({"known": True, "quoteCurrency": "USDT", "krwPerQuote": "1400", "fxObservedAt": NOW - 16}, "FX_OBSERVATION_STALE"),
            ({"known": True, "quoteCurrency": "USDT", "krwPerQuote": "1400", "fxObservedAt": NOW + 3}, "FX_OBSERVATION_STALE"),
            ({"known": True, "quoteCurrency": "USD", "krwPerQuote": "1400", "fxObservedAt": NOW}, "FRESH_FX_REQUIRED"),
            ({"known": False, "blockers": ["NATIVE_FX_OBSERVATION_STALE_OR_FUTURE"]}, "NATIVE_FX_OBSERVATION_STALE_OR_FUTURE"),
        ):
            with self.subTest(value=value):
                fixture, _ = self.prepare("BINANCE_SPOT")
                fixture.adapter.conversion_reader = lambda _: value
                result = fixture.adapter.reconcile(fixture.manifest)
                self.assertFalse(result["known"])
                self.assertIn(code, result["blockers"])

    def test_pending_intent_and_held_position_still_read_full_market(self):
        fixture, calls = self.prepare("BINANCE_SPOT")
        fixture.adapter._claims["unsubmitted-intent"] = {"status": "RESERVED"}
        self.assertTrue(fixture.adapter.reconcile(fixture.manifest)["known"])
        self.assertEqual(["account", "market"], calls)

        for route in ("BINANCE_SPOT", "BINANCE_USDM_FUTURES"):
            with self.subTest(route=route):
                fixture, calls = self.prepare(route)
                sized = fixture.adapter.size_order(fixture.proposal())
                self.assertTrue(sized["ready"], sized)
                fixture.adapter.submit(sized)  # Injected in-memory transport only.
                calls.clear()
                result = fixture.adapter.reconcile(fixture.manifest)
                self.assertTrue(result["known"], result)
                self.assertEqual(["account", "market"], calls)

    def test_flat_optimization_cannot_replace_fresh_order_settings(self):
        fixture, calls = self.prepare("BINANCE_USDM_FUTURES")
        self.assertTrue(fixture.adapter.reconcile(fixture.manifest)["known"])
        calls.clear()
        fixture.quote["leverage"] = "10"
        result = fixture.adapter.size_order(fixture.proposal())
        self.assertFalse(result["ready"])
        self.assertIn("FUTURES_SETTINGS_DIFFER_FROM_PREPARED_POLICY", result["blockers"])
        self.assertIn("market", calls)
        self.assertNotIn("fx", calls)
        self.assertEqual([], fixture.calls)

    def test_flat_krw_reconciliation_needs_neither_fx_nor_market(self):
        fixture, calls = self.prepare("UPBIT_KRW_SPOT")
        self.assertTrue(fixture.adapter.reconcile(fixture.manifest)["known"])
        self.assertEqual(["account"], calls)

    def test_missing_account_truth_is_not_treated_as_flat(self):
        for change in ({"positions": None}, {"openOrders": None}, {"positionsComplete": False},
                       {"ordersComplete": False}, {"known": False}):
            with self.subTest(change=change):
                fixture, calls = self.prepare("BINANCE_SPOT")
                fixture.truth.update(change)
                self.assertFalse(fixture.adapter.reconcile(fixture.manifest)["known"])
                self.assertEqual(["account"], calls)


class ConversionReaderDemandTests(unittest.TestCase):
    def test_idle_conversion_reduces_market_endpoint_calls_without_cache(self):
        import test_short_live_reader_diagnostics as diagnostics_tests
        for route, futures, before_count in (("BINANCE_SPOT", False, 4),
                                              ("BINANCE_USDM_FUTURES", True, 7)):
            with self.subTest(route=route):
                reader, calls, _ = diagnostics_tests.QuoteReadOrderingTests().build(futures)
                self.assertTrue(reader.market(route, "SOLUSDT")["known"])
                self.assertEqual(before_count, len(calls))
                calls.clear()
                self.assertTrue(reader.conversion(route)["known"])
                self.assertEqual(["FX"], calls)
                self.assertTrue(reader.conversion(route)["known"])
                self.assertEqual(["FX", "FX"], calls)  # No hidden TTL fallback.

    def test_only_public_fx_is_read_and_original_time_is_kept(self):
        reader = reader_tests.ReaderTests().build()
        calls = []
        def fx(currency):
            calls.append(currency)
            return {"known": True, "krwPerQuote": "1400", "observedAt": NOW - 4}
        reader.fx_reader = fx
        reader._binance = reader._upbit = lambda *_a, **_k: self.fail("No account or market request")
        for route in ("BINANCE_SPOT", "BINANCE_USDM_FUTURES"):
            result = reader.conversion(route)
            self.assertTrue(result["known"], result)
            self.assertEqual(NOW - 4, result["fxObservedAt"])
            self.assertEqual("USDT", result["quoteCurrency"])
        self.assertEqual(["USDT", "USDT"], calls)
        result = reader.conversion("UPBIT_KRW_SPOT")
        self.assertEqual("1", result["krwPerQuote"])
        self.assertEqual(["USDT", "USDT"], calls)

    def test_failed_public_read_keeps_first_reason_and_diagnostic(self):
        reader = reader_tests.ReaderTests().build()
        def fx(_):
            raise mod.ShortLiveBrokerBlocked("NATIVE_FX_OBSERVATION_STALE_OR_FUTURE")
        reader.fx_reader = fx
        result = reader.conversion("BINANCE_SPOT")
        self.assertFalse(result["known"])
        self.assertEqual(["NATIVE_FX_OBSERVATION_STALE_OR_FUTURE"], result["blockers"])
        self.assertEqual(1, len(result["readDiagnostics"]))


if __name__ == "__main__":
    unittest.main()
