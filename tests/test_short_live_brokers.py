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
    def preserved_spot_fixture(route="BINANCE_SPOT", baseline="2"):
        f = Fixture(route)
        f.quote_equity = mod.Decimal("100000")
        f.inventory = {"BNB": "1", "OLD_DELISTED": ".00001"}
        f.truth["positions"] = [{"symbol": f.symbol, "quantity": baseline, "markPrice": "1000"}]

        def snapshot(*_):
            result = copy.deepcopy(f.truth)
            asset = f.symbol.removesuffix("USDT") if route == "BINANCE_SPOT" else f.symbol.removeprefix("KRW-")
            quantity = sum((mod.number(p["quantity"]) for p in result["positions"]), mod.Decimal(0))
            inventory = {**f.inventory, asset: str(quantity)}
            result.update(spotInventoryComplete=True, spotInventory=inventory, equityScope="QUOTE_AND_SELECTED_SPOT")
            result["equityNative"] = str(f.quote_equity + sum(
                (mod.number(p["quantity"]) * mod.number(p["markPrice"]) for p in result["positions"]), mod.Decimal(0)))
            return result

        f.adapter.snapshot_reader = snapshot
        return f

    def test_existing_selected_and_unselected_spot_units_are_preserved_through_cleanup(self):
        for route in ("BINANCE_SPOT", "UPBIT_KRW_SPOT"):
            with self.subTest(route=route):
                f = self.preserved_spot_fixture(route)
                ready = f.adapter.readiness(f.manifest)
                self.assertTrue(ready["ready"], ready)
                self.assertEqual("AVAILABLE_QUOTE_ONLY", ready["fundingBasis"])
                self.assertEqual("100000", ready["availableQuoteNative"])
                self.assertEqual(["BNB", "BTC", "OLD_DELISTED"], ready["preservedAssetSymbols"])
                self.assertEqual({}, f.adapter.reconcile(f.manifest)["positionsBySymbol"])
                self.assertEqual("FILLED", f.adapter.submit(f.adapter.size_order(f.proposal()))["status"])
                self.assertEqual("10", f.adapter.reconcile(f.manifest)["positionsBySymbol"][f.symbol]["quantity"])
                final = f.adapter.cleanup(f.manifest)
                self.assertTrue(final["complete"], final)
                self.assertEqual("10", f.calls[-1]["quantity"])
                self.assertEqual(mod.Decimal(2), mod.number(f.truth["positions"][0]["quantity"]))
                self.assertEqual("1", f.inventory["BNB"])

    def test_official_unicode_zero_balance_assets_do_not_block_or_enter_baseline(self):
        f = self.preserved_spot_fixture()
        original = f.adapter.readiness(f.manifest)
        fresh = self.preserved_spot_fixture()
        fresh.inventory.update({"币安人生": "0", "牛来": "0.00000000"})
        ready = fresh.adapter.readiness(fresh.manifest)
        self.assertTrue(ready["ready"], ready)
        self.assertEqual(original["preservedAssetsBaselineHash"], ready["preservedAssetsBaselineHash"])
        self.assertNotIn("币安人生", ready["preservedAssetSymbols"])
        self.assertNotIn("牛来", ready["preservedAssetSymbols"])
        self.assertEqual([], fresh.calls)

    def test_nonzero_unicode_inventory_is_preserved_and_quantity_changes_still_block(self):
        f = self.preserved_spot_fixture()
        f.inventory.update({"币安人生": "2.5", "牛来": ".00001"})
        ready = f.adapter.readiness(f.manifest)
        self.assertTrue(ready["ready"], ready)
        self.assertIn("币安人生", ready["preservedAssetSymbols"])
        self.assertIn("牛来", ready["preservedAssetSymbols"])
        observed = f.adapter.reconcile(f.manifest)
        self.assertTrue(observed["known"], observed)
        self.assertEqual({}, observed["positionsBySymbol"])
        self.assertEqual("100000", ready["availableQuoteNative"])
        self.assertTrue(f.adapter.cleanup(f.manifest)["complete"])
        self.assertEqual([], f.calls)
        f.inventory["牛来"] = ".00002"
        self.assertIn("PRESERVED_SPOT_INVENTORY_CHANGED", f.adapter.reconcile(f.manifest)["blockers"])

    def test_asset_identifiers_are_not_case_folded_or_unicode_normalized(self):
        f = self.preserved_spot_fixture()
        f.inventory.update({"é": "1", "e\u0301": "2", "coin": "3", "COIN": "4"})
        ready = f.adapter.readiness(f.manifest)
        self.assertTrue(ready["ready"], ready)
        for asset in ("é", "e\u0301", "coin", "COIN"):
            self.assertIn(asset, ready["preservedAssetSymbols"])
        f.inventory["é"], f.inventory["e\u0301"] = "2", "1"
        self.assertIn("PRESERVED_SPOT_INVENTORY_CHANGED", f.adapter.reconcile(f.manifest)["blockers"])
        fresh = self.preserved_spot_fixture()
        fresh.inventory.update(f.inventory)
        self.assertNotEqual(ready["preservedAssetsBaselineHash"], fresh.adapter.readiness(fresh.manifest)["preservedAssetsBaselineHash"])

    def test_unicode_support_does_not_accept_invalid_quantities_or_control_names(self):
        for name in ("", "A B", "A\nB", "A\x00B", "A\u202eB", "A" * 257):
            f = self.preserved_spot_fixture()
            f.inventory[name] = "0"
            self.assertIn("SPOT_INVENTORY_INVALID", f.adapter.readiness(f.manifest)["blockers"])
        for quantity in ("-1", "NaN", "Infinity", None):
            f = self.preserved_spot_fixture()
            f.inventory["牛来"] = quantity
            self.assertFalse(f.adapter.readiness(f.manifest)["ready"])

    def test_external_fee_identifier_uses_the_same_opaque_unicode_identity(self):
        f = self.preserved_spot_fixture()
        f.inventory["牛来"] = "1"
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        reply = f.adapter.submit(f.adapter.size_order(f.proposal()))
        f.quote_equity -= 10000
        row = f.rows[reply["brokerOrderId"]]
        row.update(externalCommissionKnown=True, externalCommissionQuantities={"牛来": ".01"},
                   externalCommissionNative={"牛来": ".1"})
        f.inventory["牛来"] = ".99"
        result = f.adapter.reconcile(f.manifest)
        self.assertTrue(result["known"], result)
        self.assertEqual(mod.Decimal(".1"), mod.number(result["realizedLossKrw"]))
        row["externalCommissionQuantities"] = {"牛来\u202e": ".01"}
        row["externalCommissionNative"] = {"牛来\u202e": ".1"}
        self.assertIn("EXTERNAL_COMMISSION_INVALID", f.adapter.reconcile(f.manifest)["blockers"])

    def test_existing_coin_price_changes_do_not_manufacture_or_hide_session_loss(self):
        for price in ("10", "10000"):
            f = self.preserved_spot_fixture()
            self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
            f.truth["positions"][0]["markPrice"] = price
            f.quote["nativePrice"] = price
            self.assertEqual("0", f.adapter.reconcile(f.manifest)["realizedLossKrw"])
            f.quote_equity -= 123
            self.assertEqual("123", f.adapter.reconcile(f.manifest)["realizedLossKrw"])

    def test_external_spot_addition_sale_withdrawal_or_missing_inventory_blocks(self):
        for change in (lambda f: f.inventory.update(ETH="1"),
                       lambda f: f.inventory.update(BNB=".999"),
                       lambda f: f.truth["positions"][0].update(quantity="3"),
                       lambda f: f.truth["positions"][0].update(quantity="1")):
            f = self.preserved_spot_fixture()
            self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
            change(f)
            result = f.adapter.reconcile(f.manifest)
            self.assertIn("PRESERVED_SPOT_INVENTORY_CHANGED", result["blockers"])
            self.assertFalse(f.adapter.cleanup(f.manifest)["complete"])
            self.assertEqual([], f.calls)
        f = self.preserved_spot_fixture()
        f.adapter.readiness(f.manifest)
        f.adapter.snapshot_reader = lambda *_: copy.deepcopy(f.truth)
        self.assertIn("COMPLETE_SPOT_INVENTORY_REQUIRED", f.adapter.reconcile(f.manifest)["blockers"])

    def test_existing_coins_never_substitute_for_missing_or_insufficient_quote(self):
        for quote, minimum, blocker in (("0", "0", "NO_AVAILABLE_BALANCE"),
                                        ("2", "1000", "AVAILABLE_BELOW_NATIVE_MINIMUM")):
            f = self.preserved_spot_fixture()
            f.truth["availableNative"] = quote
            f.quote["minNotional"] = minimum
            ready = f.adapter.readiness(f.manifest)
            self.assertFalse(ready["ready"])
            self.assertIn(blocker, ready["blockers"])
            self.assertEqual(quote, ready["availableQuoteNative"])
            self.assertNotIn("accountId", ready)
            self.assertEqual([], f.calls)

    def test_spot_baseline_is_frozen_per_adapter_and_hash_matches_exact_fresh_inventory(self):
        f = self.preserved_spot_fixture()
        ready = f.adapter.readiness(f.manifest)
        duplicate = self.preserved_spot_fixture(baseline="2.00000")
        self.assertEqual(ready["preservedAssetsBaselineHash"], duplicate.adapter.readiness(duplicate.manifest)["preservedAssetsBaselineHash"])
        changed = self.preserved_spot_fixture(baseline="2.1")
        self.assertNotEqual(ready["preservedAssetsBaselineHash"], changed.adapter.readiness(changed.manifest)["preservedAssetsBaselineHash"])
        f.truth["positions"][0]["quantity"] = "3"
        self.assertFalse(f.adapter.readiness(f.manifest)["ready"])
        self.assertIn("ADAPTER_ALREADY_BOUND", f.adapter.readiness({**f.manifest, "sessionId": "another"})["blockers"])

    def test_exact_third_asset_commission_debit_is_charged_without_becoming_owned(self):
        f = self.preserved_spot_fixture()
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        order = f.adapter.size_order(f.proposal())
        reply = f.adapter.submit(order)
        f.quote_equity -= 10000
        row = f.rows[reply["brokerOrderId"]]
        row.update(externalCommissionKnown=True, externalCommissionQuantities={"BNB": ".001"},
                   externalCommissionNative={"BNB": ".6"})
        f.inventory["BNB"] = ".999"
        result = f.adapter.reconcile(f.manifest)
        self.assertTrue(result["known"], result)
        self.assertEqual(mod.Decimal(".6"), mod.number(result["realizedLossKrw"]))
        self.assertEqual({f.symbol}, set(result["positionsBySymbol"]))
        row["externalCommissionNative"]["BNB"] = ".5"
        self.assertEqual(mod.Decimal(".6"), mod.number(f.adapter.reconcile(f.manifest)["realizedLossKrw"]))
        row["externalCommissionQuantities"]["BNB"] = ".002"
        row["externalCommissionNative"]["BNB"] = ".8"
        f.inventory["BNB"] = ".998"
        # A further .001BNB costs .4USDT now; earlier .6USDT is not revalued away.
        self.assertEqual(mod.Decimal("1.0"), mod.number(f.adapter.reconcile(f.manifest)["realizedLossKrw"]))
        final = f.adapter.cleanup(f.manifest)
        self.assertTrue(final["complete"], final)
        self.assertEqual(mod.Decimal(2), mod.number(f.truth["positions"][0]["quantity"]))
        self.assertEqual(".998", f.inventory["BNB"])
        f.inventory["BNB"] = ".997"
        self.assertIn("PRESERVED_SPOT_INVENTORY_CHANGED", f.adapter.reconcile(f.manifest)["blockers"])

    def test_existing_spot_inventory_does_not_authorize_foreign_open_orders(self):
        f = self.preserved_spot_fixture()
        f.truth["openOrders"] = [{"brokerOrderId": "foreign", "symbol": f.symbol}]
        ready = f.adapter.readiness(f.manifest)
        self.assertIn("UNOWNED_ACCOUNT_ORDER", ready["blockers"])
        self.assertIsNone(f.adapter._spot_baseline)
        self.assertEqual([], f.calls)

    def test_unknown_or_regressing_external_fee_never_justifies_inventory_change(self):
        f = self.preserved_spot_fixture()
        f.adapter.readiness(f.manifest)
        reply = f.adapter.submit(f.adapter.size_order(f.proposal()))
        row = f.rows[reply["brokerOrderId"]]
        row.update(externalCommissionQuantities={"BNB": ".001"}, externalCommissionNative={"BNB": ".6"})
        f.inventory["BNB"] = ".999"
        self.assertIn("EXTERNAL_COMMISSION_UNKNOWN", f.adapter.reconcile(f.manifest)["blockers"])
        row["externalCommissionKnown"] = True
        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
        row["externalCommissionQuantities"] = {}
        row["externalCommissionNative"] = {}
        self.assertIn("EXTERNAL_COMMISSION_CUMULATIVE_CHANGED", f.adapter.reconcile(f.manifest)["blockers"])

    def test_cross_symbol_discount_fee_is_blocked_before_any_order(self):
        f = self.preserved_spot_fixture()
        f.manifest["symbols"].append("BNBUSDT")
        f.adapter.market_reader = lambda _r, symbol: {**f.quote, "symbol": symbol,
            "commissionDiscountEnabled": True, "commissionDiscountAsset": "BNB"}
        f.truth["positions"].append({"symbol": "BNBUSDT", "quantity": "1", "markPrice": "1000"})
        # Supply exact multi-symbol inventory independently of the one-symbol fixture.
        f.adapter.snapshot_reader = lambda *_: {**copy.deepcopy(f.truth), "spotInventoryComplete": True,
            "spotInventory": {"BTC": "2", "BNB": "1"}, "equityScope": "QUOTE_AND_SELECTED_SPOT", "equityNative": "103000"}
        self.assertIn("CROSS_SYMBOL_COMMISSION_ALLOCATION_UNSUPPORTED", f.adapter.readiness(f.manifest)["blockers"])
        self.assertEqual([], f.calls)

    def test_futures_settings_failure_retains_sanitized_cash_and_exact_settings(self):
        f = Fixture("BINANCE_USDM_FUTURES")
        f.quote.update(marginMode="CROSSED", leverage="20")
        result = f.adapter.readiness(f.manifest)
        self.assertFalse(result["ready"])
        self.assertEqual("100000", result["availableQuoteNative"])
        self.assertEqual({"marginMode": "CROSSED", "leverage": "20", "positionMode": "ONE_WAY"}, result["observedFuturesSettings"])
        self.assertEqual({"marginMode": "ISOLATED", "leverage": "1"}, result["expectedFuturesSettings"])
        self.assertNotIn("accountId", result)

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
