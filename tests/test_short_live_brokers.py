from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import sys
import types
import unittest

# Never execute live_trader.__init__ or read/migrate the real .env in tests.
ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_short_live_broker_tests"
if PKG not in sys.modules:
    package = types.ModuleType(PKG); package.__path__ = [str(ROOT)]
    sys.modules[PKG] = package
spec = importlib.util.spec_from_file_location(PKG + ".short_live_brokers", ROOT / "short_live_brokers.py")
mod = importlib.util.module_from_spec(spec); sys.modules[spec.name] = mod; spec.loader.exec_module(mod)

NOW = 1792652400.0


class Fixture:
    def __init__(self, route="KIS_KR_CASH", *, budget=None):
        self.route = route
        self.symbol = "069500" if route == "KIS_KR_CASH" else "KRW-BTC" if route == "UPBIT_KRW_SPOT" else "BTCUSDT"
        self.currency = mod.ROUTES[route][2]
        self.account = "live-account:test-exact"
        self.manifest = {"route": route, "sessionId": "session-exact", "manifestHash": "a" * 64,
                         "accountId": self.account, "symbols": [self.symbol], "brokerBudgetKrw": 100000,
                         "routeBudgetKrw": 100000, "dailyLossStopKrw": 50000}
        self.truth = {"known": True, "ordersComplete": True, "positionsComplete": True,
                      "accountId": self.account, "currency": self.currency, "observedAt": NOW,
                      "availableNative": "100000", "equityNative": "100000", "positions": [], "openOrders": []}
        self.truth["maximumOrderQuantities"] = {self.symbol: "100000"}
        self.quote = {"known": True, "symbol": self.symbol, "nativePrice": "1000", "quoteCurrency": self.currency,
                      "krwPerQuote": "1", "fxObservedAt": NOW, "feeRate": "0.001", "lotStep": "1",
                      "minQuantity": "1", "minNotional": "0", "sessionOpen": True, "sellTaxRate": "0",
                      "sessionRemainingSeconds": 24000,
                      "observedAt": NOW, "marginMode": "ISOLATED", "leverage": "1",
                      "positionMode": "ONE_WAY", "fundingRate": "0.0001"}
        self.calls = []; self.rows = {}; self.allow = True; self.unknown = False
        owner = self
        class Transport:
            def enabled(self): return owner.allow
            def submit(self, route, order):
                owner.calls.append(copy.deepcopy(order))
                if owner.unknown: raise TimeoutError("private body must never escape")
                oid = str(len(owner.calls))
                qty = mod.number(order["quantity"])
                previous = sum((mod.number(p["quantity"]) for p in owner.truth["positions"]), mod.Decimal(0))
                delta = qty if order["side"] == "BUY" else -qty
                if previous + delta:
                    owner.truth["positions"] = [{"symbol": owner.symbol, "quantity": str(previous + delta), "markPrice": "1000"}]
                else: owner.truth["positions"] = []
                owner.rows[oid] = {"known": True, "brokerOrderId": oid, "clientOrderId": order["identifier"],
                    "symbol": owner.symbol, "status": "FILLED", "filledQuantity": order["quantity"],
                    "baseCommissionQuantity": "0", "baseCommissionKnown": True,
                    "fillPrice": "1000", "feeNative": "10", "observedAt": NOW}
                return {"json": {"orderId": oid, "status": "FILLED", "executedQty": order["quantity"],
                                  "avgPrice": "1000", "clientOrderId": order["identifier"]}}
            def read_order(self, route, row): return copy.deepcopy(owner.rows[row["brokerOrderId"]])
            def cancel(self, route, broker_id, row): owner.calls.append({"cancel": broker_id})
        self.adapter = mod.ShortLiveBrokerAdapter(route, snapshot_reader=lambda *_: copy.deepcopy(self.truth),
            market_reader=lambda *_: copy.deepcopy(self.quote), transport=Transport(),
            authority_guard=lambda _: self.allow, budget=budget, clock=lambda: NOW)
        self.adapter.reserve_cleanup_order = lambda _: True
        self.adapter.reserve_cleanup_cancel = lambda _: True
        self.adapter.observe_cleanup_order = lambda *_: True

    def proposal(self, amount="10000", *, side="BUY", reducing=False, cid="short-client-0001", direction="long"):
        return {"route": self.route, "sessionId": self.manifest["sessionId"], "manifestHash": "a" * 64,
                "symbol": self.symbol, "side": side, "notionalKrw": amount, "riskReducing": reducing,
                "positionDirection": direction, "clientOrderId": cid, "accountId": self.account}


class ShortLiveBrokerTests(unittest.TestCase):
    @staticmethod
    def native_cash_fixture(route="BINANCE_SPOT"):
        f = Fixture(route)
        f.truth.update(availableNative="10000", equityNative="10000")
        f.quote.update(nativePrice="1", krwPerQuote="1336", lotStep="0.001", minQuantity="0.001", minNotional="5")
        return f

    def test_idle_cash_fx_changes_cannot_mask_fee_inclusive_native_loss(self):
        for current_fx in ("1342", "1330"):
            for route in ("BINANCE_SPOT", "BINANCE_USDM_FUTURES"):
                with self.subTest(fx=current_fx, route=route):
                    f = self.native_cash_fixture(route)
                    self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
                    # Actual equity declined40USDT, including fees/funding;
                    # a rising FX rate on unallocated10000USDT cannot hide it.
                    f.truth["equityNative"] = "9960"
                    f.quote["krwPerQuote"] = current_fx
                    result = f.adapter.reconcile(f.manifest)
                    self.assertTrue(result["known"], result)
                    self.assertEqual(mod.Decimal(40)*mod.number(current_fx), mod.number(result["realizedLossKrw"]))
                    self.assertGreaterEqual(mod.number(result["realizedLossKrw"]), mod.MAX_LOSS_KRW)
                    self.assertEqual("ACCOUNT_NATIVE_EQUITY_DECLINE_AT_FRESH_FX", result["lossBasis"])
                    sized = f.adapter.size_order(f.proposal("10000"))
                    self.assertEqual(current_fx, sized["krwPerQuote"])  # budget sizing still uses fresh FX

    def test_idle_cash_fx_alone_does_not_manufacture_a_trading_loss(self):
        for current_fx in ("1342", "1300"):
            with self.subTest(fx=current_fx):
                f = self.native_cash_fixture()
                self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
                f.quote["krwPerQuote"] = current_fx
                result = f.adapter.reconcile(f.manifest)
                self.assertEqual("0", result["realizedLossKrw"])
                self.assertEqual(mod.Decimal(10000)*mod.number(current_fx), mod.number(result["equityKrw"]))

    def test_native_loss_baseline_survives_kst_midnight_and_repeated_readiness(self):
        f = self.native_cash_fixture()
        before = (NOW//86400)*86400+15*3600-1  #23:59:59KST
        clock = [before]
        f.adapter.clock = lambda: clock[0]
        f.truth["observedAt"] = before
        f.quote.update(observedAt=before, fxObservedAt=before)
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        clock[0] += 2  #00:00:01KST; no date-based budget/loss reset
        f.truth.update(equityNative="9960", observedAt=clock[0])
        f.quote.update(krwPerQuote="1342", observedAt=clock[0], fxObservedAt=clock[0])
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        self.assertEqual(mod.Decimal(10000), f.adapter._baseline_equity_native)
        self.assertEqual("53680", f.adapter.reconcile(f.manifest)["realizedLossKrw"])

    def test_source_five_percent_size_does_not_expand_to_full_budget(self):
        f = Fixture("BINANCE_SPOT")
        f.manifest["specs"] = [{"symbol": f.symbol, "targetWeight": 1,
                                "parameters": {"positionSize": 5}}]
        f.quote["minNotional"] = "6000"
        result = f.adapter.readiness(f.manifest)
        self.assertIn("ALLOCATION_BELOW_NATIVE_MINIMUM", result["blockers"])
        self.assertIsNone(f.adapter._manifest)
        f.manifest["specs"][0]["parameters"]["positionSize"] = 100
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])

    def test_invalid_source_position_sizes_are_blocked(self):
        for invalid in (True, False, None, "NaN", "Infinity", "bad", -1, 101):
            with self.subTest(positionSize=invalid):
                f = Fixture("BINANCE_SPOT")
                f.manifest["specs"] = [{"symbol": f.symbol, "targetWeight": 1,
                                        "parameters": {"positionSize": invalid}}]
                self.assertIn("POSITION_SIZE_INVALID", f.adapter.readiness(f.manifest)["blockers"])

    def test_each_symbol_maximum_allocation_must_afford_native_minimum(self):
        f = Fixture("BINANCE_SPOT")
        f.manifest.update(routeBudgetKrw=10000, symbols=["BTCUSDT", "ETHUSDT"],
                          specs=[{"symbol": "BTCUSDT", "targetWeight": .5}, {"symbol": "ETHUSDT", "targetWeight": .5}])
        f.quote["minNotional"] = "6000"
        f.adapter.market_reader = lambda _route, symbol: {**f.quote, "symbol": symbol}
        result = f.adapter.readiness(f.manifest)
        self.assertIn("ALLOCATION_BELOW_NATIVE_MINIMUM", result["blockers"])
        self.assertEqual("6006.000", result["minimumOrderKrwBySymbol"]["BTCUSDT"])
        f.manifest.update(symbols=["BTCUSDT"], specs=[{"symbol": "BTCUSDT", "targetWeight": .5}, {"symbol": "BTCUSDT", "targetWeight": .5}])
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])

    def test_distinct_actual_fresh_fx_rates_do_not_require_fabricated_equality(self):
        f = Fixture("BINANCE_SPOT"); f.manifest["symbols"] = ["BTCUSDT", "ETHUSDT"]
        f.adapter.market_reader = lambda _route, symbol: {**f.quote, "symbol": symbol, "krwPerQuote": "1" if symbol == "BTCUSDT" else "1.001"}
        result = f.adapter.readiness(f.manifest)
        self.assertTrue(result["ready"], result)
        self.assertEqual("100000", result["availableKrw"])
    def test_normal_transport_lost_reply_remains_unknown_not_rejected(self):
        f = Fixture("BINANCE_SPOT"); order = f.proposal(); order.update(quantity="1", referencePrice="1000")
        for response in ({"ok": False, "statusCode": 0, "json": {}},
                         {"ok": False, "statusCode": 504, "json": {}},
                         {"ok": False, "statusCode": 302, "redirectBlocked": True, "json": {}},
                         {"ok": False, "statusCode": 400, "json": {"code": -1006}},
                         {"ok": False, "statusCode": 400, "json": {"code": -1007}}):
            with self.subTest(response=response):
                self.assertEqual("UNKNOWN", f.adapter._normalize_reply(response, order, mod.Decimal(1))["status"])
        self.assertEqual("REJECTED", f.adapter._normalize_reply({"ok": False, "statusCode": 400, "json": {"code": -1021}}, order, mod.Decimal(1))["status"])
    def test_minimum_affordability_and_fee_inclusive_full_ceiling(self):
        f = Fixture(); f.truth["availableNative"] = "999"
        self.assertIn("AVAILABLE_BELOW_NATIVE_MINIMUM", f.adapter.readiness(f.manifest)["blockers"])
        f = Fixture(); self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        sized = f.adapter.size_order(f.proposal("100000"))
        self.assertTrue(sized["ready"], sized)
        self.assertLessEqual(mod.number(sized["projectedGrossExposureKrw"]), mod.MAX_EXCHANGE_KRW)

    def test_kis_duration_must_fit_today_and_flat_can_retire_after_close(self):
        f = Fixture(); f.quote["sessionRemainingSeconds"] = 6*3600+119
        self.assertIn("KRX_TEST_DURATION_EXCEEDS_SESSION", f.adapter.readiness(f.manifest)["blockers"])
        f = Fixture(); self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        f.quote["sessionOpen"] = False
        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
        self.assertFalse(f.adapter.size_order(f.proposal())["ready"])

    def test_pending_order_observation_is_cumulative_without_second_post(self):
        f = Fixture(); self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        order = f.adapter.size_order(f.proposal())
        result = f.adapter.submit(order)
        oid = result["brokerOrderId"]
        f.adapter._fill_rows.clear()
        f.rows[oid].update(status="PARTIALLY_FILLED", filledQuantity="5", feeNative="2")
        observed = f.adapter.observe_order(order["clientOrderId"])
        self.assertEqual("PARTIAL", observed["status"])
        self.assertEqual("5", observed["filledQuantity"])
        f.rows[oid].update(status="FILLED", filledQuantity=order["quantity"])
        self.assertEqual("FILLED", f.adapter.observe_order(order["clientOrderId"])["status"])
        self.assertEqual(1, len(f.calls))

    def test_budget_recycles_only_after_actual_flat_and_terminal_orders(self):
        f = Fixture(); self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        f.adapter.submit(f.adapter.size_order(f.proposal()))
        self.assertTrue(f.adapter.budget.reservations)
        self.assertTrue(f.adapter.cleanup(f.manifest)["complete"])
        self.assertEqual({}, f.adapter.budget.reservations)

    def test_spot_base_asset_fee_matches_actual_owned_units_and_exact_close(self):
        f = Fixture("BINANCE_SPOT"); f.quote.update(lotStep="0.000001", minQuantity="0.000001")
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        order = f.adapter.size_order(f.proposal())
        result = f.adapter.submit(order)
        f.rows[result["brokerOrderId"]]["baseCommissionQuantity"] = ".01"
        f.truth["positions"][0]["quantity"] = "9.99"
        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
        final = f.adapter.cleanup(f.manifest)
        self.assertTrue(final["complete"], final)
        self.assertEqual(mod.Decimal("9.99"), mod.number(f.calls[-1]["quantity"]))

    def test_fresh_recheck_preserves_reserved_native_limit_and_quantity(self):
        f = Fixture("BINANCE_SPOT")
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        order = f.adapter.size_order(f.proposal())
        f.quote.update(nativePrice="1001", krwPerQuote="1.001")
        result = f.adapter.submit(order)
        self.assertEqual("FILLED", result["status"])
        self.assertEqual(mod.number(order["quantity"]), mod.number(f.calls[0]["quantity"]))
        self.assertEqual(mod.number(order["referencePrice"]), mod.number(f.calls[0]["price"]))

    def test_all_four_routes_prepare_with_exact_complete_mock_truth(self):
        for route in mod.ROUTES:
            with self.subTest(route=route):
                f = Fixture(route)
                self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
                self.assertEqual([], f.calls)

    def test_unknown_cash_incomplete_paging_foreign_positions_and_stale_fx_block(self):
        mutations = [lambda f: f.truth.update(availableNative=None),
                     lambda f: f.truth.update(ordersComplete=False),
                     lambda f: f.truth.update(positions=[{"symbol": f.symbol, "quantity": "1", "markPrice": "1000"}]),
                     lambda f: f.quote.update(fxObservedAt=NOW - 61),
                     lambda f: f.truth.update(openOrders=[{"brokerOrderId": "foreign"}]),
                     lambda f: f.truth.update(accountId="wrong-account")]
        for mutate in mutations:
            f = Fixture(); mutate(f)
            self.assertFalse(f.adapter.readiness(f.manifest)["ready"])
            self.assertEqual([], f.calls)

    def test_quantity_rounding_fee_budget_and_actual_fill_then_owned_cleanup(self):
        f = Fixture(); self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        sized = f.adapter.size_order(f.proposal())
        self.assertTrue(sized["ready"]); self.assertEqual("10", sized["quantity"])
        result = f.adapter.submit(sized)
        self.assertEqual("FILLED", result["status"]); self.assertTrue(result["actualFeeKnown"])
        truth = f.adapter.reconcile(f.manifest)
        self.assertTrue(truth["known"])
        self.assertEqual("10", truth["positionsBySymbol"][f.symbol]["quantity"])
        final = f.adapter.cleanup(f.manifest)
        self.assertTrue(final["complete"], final)
        self.assertTrue(f.calls[-1]["reduce_only"])

    def test_unknown_submit_is_one_attempt_and_never_retried_or_claimed_filled(self):
        f = Fixture(); f.adapter.readiness(f.manifest); f.unknown = True
        order = f.adapter.size_order(f.proposal())
        first = f.adapter.submit(order); second = f.adapter.submit(order)
        self.assertEqual("UNKNOWN", first["status"]); self.assertEqual(first, second)
        self.assertEqual(1, len(f.calls)); self.assertFalse(f.adapter.reconcile(f.manifest)["known"])

    def test_fresh_manual_authority_and_real_order_switch_required(self):
        f = Fixture(); f.adapter.readiness(f.manifest)
        order = f.adapter.size_order(f.proposal()); f.allow = False
        self.assertEqual("REJECTED", f.adapter.submit(order)["status"])
        self.assertEqual([], f.calls)

    def test_futures_mismatch_does_not_change_account_settings(self):
        for change in ({"leverage": "2"}, {"marginMode": "CROSSED"}, {"positionMode": "UNKNOWN"}):
            f = Fixture("BINANCE_USDM_FUTURES"); f.quote.update(change)
            self.assertFalse(f.adapter.readiness(f.manifest)["ready"])
            self.assertEqual([], f.calls)

    def test_futures_short_entry_and_owned_cover_keep_direction(self):
        f = Fixture("BINANCE_USDM_FUTURES"); f.adapter.readiness(f.manifest)
        order = f.adapter.size_order(f.proposal(side="SELL", direction="short"))
        self.assertEqual("FILLED", f.adapter.submit(order)["status"])
        final = f.adapter.cleanup(f.manifest)
        self.assertTrue(final["complete"], final)
        self.assertEqual("SELL", f.calls[0]["side"]); self.assertEqual("BUY", f.calls[-1]["side"])
        self.assertEqual("short", f.calls[-1]["position_direction"])

    def test_binance_spot_and_futures_share_one_exchange_ceiling(self):
        budget = mod.SharedExchangeBudget()
        a = Fixture("BINANCE_SPOT", budget=budget); b = Fixture("BINANCE_USDM_FUTURES", budget=budget)
        a.adapter.readiness(a.manifest); b.adapter.readiness(b.manifest)
        self.assertEqual("FILLED", a.adapter.submit(a.adapter.size_order(a.proposal("60000")))["status"])
        self.assertEqual("REJECTED", b.adapter.submit(b.adapter.size_order(b.proposal("60000", cid="short-client-0002")))["status"])
        self.assertEqual([], b.calls)

    def test_official_transport_uses_normal_router_final_boundaries(self):
        class Router:
            calls = []
            def place_order(self, intent): self.calls.append(("place", intent)); return {"json": {"orderId": 1}}
            def cancel_order(self, broker, oid, **context): self.calls.append(("cancel", broker, oid)); return {"ok": True}
        router = Router(); edge = mod.OfficialBrokerTransport(router, enabled_reader=lambda: True)
        edge.submit("BINANCE_USDM_FUTURES", {"broker_id": "binance-futures"})
        edge.cancel("BINANCE_SPOT", "exact-owned-id", {"symbol": "BTCUSDT"})
        self.assertEqual(["place", "cancel"], [r[0] for r in router.calls])


if __name__ == "__main__": unittest.main()
