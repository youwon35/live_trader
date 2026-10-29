from __future__ import annotations
import copy
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
from test_short_live_brokers import mod, PKG, ROOT, NOW

spec = importlib.util.spec_from_file_location(PKG + ".short_live_broker_readers", ROOT / "short_live_broker_readers.py")
readers = importlib.util.module_from_spec(spec); sys.modules[spec.name] = readers; spec.loader.exec_module(readers)


class ReaderTests(unittest.TestCase):
    def build(self):
        class Dummy:
            pass
        r = readers.ProductionShortLiveReaders(settings={}, fx_reader=lambda _: {"known": True, "krwPerQuote": "1400", "observedAt": NOW},
                                               reader=Dummy(), clock=lambda: NOW)
        r._identity = lambda _: "live-account:exact"
        return r

    def test_spot_available_excludes_locked_and_account_wide_orders_are_read(self):
        r = self.build(); calls = []
        def get(provider, path, params=None, signed=True):
            calls.append((provider, path, params))
            if path.endswith("/account"): return {"canTrade": True, "balances": [{"asset": "USDT", "free": "10", "locked": "90"}]}
            if path.endswith("/openOrders"): return []
            raise AssertionError(path)
        r._binance = get
        result = r.snapshot("BINANCE_SPOT", ("BTCUSDT",))
        self.assertTrue(result["known"]); self.assertEqual("10", result["availableNative"])
        self.assertEqual("100", result["equityNative"])
        self.assertIn(("binance", "/api/v3/openOrders", None), calls)

    def test_unscoped_spot_holdings_are_quantity_only_without_dust_market_queries(self):
        r = self.build()
        def get(_provider, path, *_a, **_k):
            if path.endswith("/account"):
                return {"canTrade": True, "balances": [{"asset": "USDT", "free": "10", "locked": "0"},
                    {"asset": "ETH", "free": "1", "locked": "0"}, {"asset": "OLD_DELISTED", "free": ".00001", "locked": "0"}]}
            if path.endswith("/openOrders"): return []
            raise AssertionError(path)
        r._binance = get
        r.market = lambda *_: self.fail("Unselected holdings never need tradable markets")
        result = r.snapshot("BINANCE_SPOT", ("BTCUSDT",))
        self.assertTrue(result["known"], result)
        self.assertEqual([], result["positions"])
        self.assertEqual("10", result["equityNative"])
        self.assertEqual({"ETH": "1", "OLD_DELISTED": "0.00001"}, result["spotInventory"])

    def test_selected_spot_inventory_is_explicit_and_outside_coins_are_not_equity(self):
        r = self.build()
        r.market = lambda *_: self.fail("Inventory valuation must not repeat full trading-rule reads")
        def get(path, params=None, signed=True):
            if path == "/v1/accounts": return [{"currency": "KRW", "balance": "10000", "locked": "20"},
                {"currency": "BTC", "balance": "2", "locked": ".5"},
                {"currency": "ETH", "balance": "100", "locked": "0"}]
            if path == "/v1/orders/open": return []
            if path == "/v1/ticker":
                self.assertFalse(signed)
                self.assertEqual({"markets": "KRW-BTC"}, params)
                return [{"market": "KRW-BTC", "trade_price": "1000", "trade_timestamp": int(NOW*1000), "timestamp": int(NOW*1000)}]
            raise AssertionError(path)
        r._upbit = get
        result = r.snapshot("UPBIT_KRW_SPOT", ("KRW-BTC",))
        self.assertTrue(result["known"], result)
        self.assertEqual("10000", result["availableNative"])
        self.assertEqual("12520.0", result["equityNative"])
        self.assertEqual({"BTC": "2.5", "ETH": "100"}, result["spotInventory"])
        self.assertEqual(["KRW-BTC"], [row["symbol"] for row in result["positions"]])

    def test_inventory_native_price_needs_no_fee_filter_or_fx_but_keeps_source_freshness(self):
        for route, symbol in (("BINANCE_SPOT", "BTCUSDT"), ("UPBIT_KRW_SPOT", "KRW-BTC")):
            with self.subTest(route=route):
                r = self.build(); calls = []
                r.market = lambda *_: self.fail("No full market read for inventory valuation")
                r.fx_reader = lambda *_: self.fail("Native valuation needs no FX")
                def get(*args, **kwargs):
                    calls.append((args, kwargs))
                    if route == "BINANCE_SPOT":
                        self.assertEqual(("binance", "/api/v3/ticker/price", {"symbol": symbol}), args)
                        return {"symbol": symbol, "price": "1000"}
                    self.assertEqual(("/v1/ticker", {"markets": symbol}), args)
                    return [{"market": symbol, "trade_price": "1000", "trade_timestamp": int(NOW*1000), "timestamp": int(NOW*1000)}]
                r._binance = r._upbit = get
                self.assertEqual(mod.Decimal("1000"), r._inventory_price(route, symbol))
                self.assertEqual(1, len(calls))
                self.assertFalse(calls[0][1]["signed"])
        r = self.build()
        # A fresh ticker snapshot need not contain a trade in the last ten
        # seconds. The official information timestamp, not trade frequency,
        # is the unchanged source-freshness boundary.
        r._upbit = lambda *_a, **_k: [{"market": "KRW-BTC", "trade_price": "1000", "trade_timestamp": int((NOW-11)*1000), "timestamp": int(NOW*1000)}]
        self.assertEqual(mod.Decimal("1000"), r._inventory_price("UPBIT_KRW_SPOT", "KRW-BTC"))
        for trade_age, response_age in ((11, 11), (-3, -3)):
            r._upbit = lambda *_a, **_k: [{"market": "KRW-BTC", "trade_price": "1000", "trade_timestamp": int((NOW-trade_age)*1000), "timestamp": int((NOW-response_age)*1000)}]
            with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "SOURCE_QUOTE_OBSERVATION_STALE"):
                r._inventory_price("UPBIT_KRW_SPOT", "KRW-BTC")

    def test_inventory_price_rejects_wrong_symbol_or_slow_read(self):
        r = self.build(); now = [NOW]; r.clock = lambda: now[0]
        r._binance = lambda *_a, **_k: {"symbol": "OTHERUSDT", "price": "1000"}
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "EXACT_MARKET_TRUTH_REQUIRED"):
            r._inventory_price("BINANCE_SPOT", "BTCUSDT")
        def slow(*_a, **_k):
            now[0] += 11
            return {"symbol": "BTCUSDT", "price": "1000"}
        r._binance = slow
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "MARKET_OBSERVATION_STALE"):
            r._inventory_price("BINANCE_SPOT", "BTCUSDT")

    def test_duplicate_negative_or_unknown_spot_balances_block_complete_truth(self):
        for holdings in ([{"currency": "ETH", "balance": "1", "locked": "0"}] * 2,
                         [{"currency": "ETH", "balance": "-1", "locked": "2"}],
                         [{"currency": "ETH", "balance": "1", "locked": None}]):
            r = self.build()
            r._upbit = lambda *_a, **_k: [{"currency": "KRW", "balance": "10000", "locked": "0"}] + holdings
            self.assertFalse(r.snapshot("UPBIT_KRW_SPOT", ("KRW-BTC",))["known"])

    def test_negative_quote_component_cannot_hide_inside_positive_equity(self):
        for free, locked in (("100", "-1"), ("-1", "100")):
            r = self.build()
            r._upbit = lambda *_a, **_k: [{"currency": "KRW", "balance": free, "locked": locked}]
            self.assertFalse(r.snapshot("UPBIT_KRW_SPOT", ("KRW-BTC",))["known"])
            r._binance = lambda *_a, **_k: {"canTrade": True, "balances": [{"asset": "USDT", "free": free, "locked": locked}]}
            self.assertFalse(r.snapshot("BINANCE_SPOT", ("BTCUSDT",))["known"])

    def test_unselected_holdings_do_not_skip_foreign_open_order_observation(self):
        r = self.build(); calls = []
        def get(path, params=None, signed=True):
            calls.append(path)
            if path == "/v1/accounts": return [{"currency": "KRW", "balance": "10000", "locked": "0"},
                                               {"currency": "ETH", "balance": "1", "locked": "0"}]
            if path == "/v1/orders/open": return [{"uuid": "foreign", "market": "KRW-ETH", "state": "wait"}] if params["state"] == "wait" else []
            raise AssertionError(path)
        r._upbit = get
        result = r.snapshot("UPBIT_KRW_SPOT", ("KRW-BTC",))
        self.assertTrue(result["known"])
        self.assertEqual(["foreign"], [row["brokerOrderId"] for row in result["openOrders"]])
        self.assertEqual(2, calls.count("/v1/orders/open"))

    def test_upbit_pagination_consumes_wait_and_watch_before_complete(self):
        r = self.build(); calls = []
        def get(path, params=None, signed=True):
            calls.append((path, params))
            if path == "/v1/accounts": return [{"currency": "KRW", "balance": "10000", "locked": "20"}]
            if path == "/v1/orders/open": return []
            raise AssertionError(path)
        r._upbit = get
        result = r.snapshot("UPBIT_KRW_SPOT", ("KRW-BTC",))
        self.assertTrue(result["known"])
        self.assertEqual({"wait", "watch"}, {p["state"] for path, p in calls if path == "/v1/orders/open"})

    def test_future_conditional_orders_unknown_blocks_whole_snapshot(self):
        r = self.build()
        def get(provider, path, params=None, signed=True):
            values = {"/fapi/v3/account": {"assets": [{"asset": "USDT", "availableBalance": "100", "marginBalance": "100"}]},
                      "/fapi/v1/accountConfig": {"canTrade": True, "multiAssetsMargin": False}, "/fapi/v3/positionRisk": [],
                      "/fapi/v1/openOrders": [], "/fapi/v1/openAlgoOrders": {"unknown": True}}
            return values[path]
        r._binance = get
        self.assertFalse(r.snapshot("BINANCE_USDM_FUTURES", ("BTCUSDT",))["known"])

    def test_upbit_market_uses_both_actual_fees_and_minimums(self):
        r = self.build()
        def get(path, params=None, signed=True):
            if path == "/v1/orders/chance": return {"market": {"state": "active", "bid": {"min_total": "5000"}, "ask": {"min_total": "5000"}}, "bid_fee": "0.0005", "ask_fee": "0.001"}
            if path == "/v1/ticker": return [{"market": "KRW-BTC", "trade_price": 100000000, "trade_timestamp": int(NOW*1000), "timestamp": int(NOW*1000)}]
            if path == "/v1/orderbook/instruments": return [{"market": "KRW-BTC", "tick_size": "1000"}]
            raise AssertionError(path)
        r._upbit = get
        result = r.market("UPBIT_KRW_SPOT", "KRW-BTC")
        self.assertTrue(result["known"]); self.assertEqual("0.001", result["feeRate"])
        self.assertEqual("5000", result["minNotional"])
        self.assertEqual("1000", result["priceTick"])

    def test_kis_costs_require_fresh_exact_symbol_and_personal_confirmation(self):
        settings = {"SHORT_LIVE_KIS_COST_CONFIRMED": True, "SHORT_LIVE_KIS_COST_SOURCE": "user tariff evidence",
                    "SHORT_LIVE_KIS_COST_OBSERVED_AT": NOW, "SHORT_LIVE_KIS_FEE_RATE": "0.001",
                    "SHORT_LIVE_KIS_SELL_TAX_RATE": "0", "SHORT_LIVE_KIS_COST_SYMBOLS": "069500"}
        costs = readers.production_cost_reader(settings, clock=lambda: NOW)
        self.assertTrue(costs("KIS_KR_CASH", "069500")["known"])
        self.assertFalse(costs("KIS_KR_CASH", "005930")["known"])
        settings["SHORT_LIVE_KIS_COST_CONFIRMED"] = False
        self.assertFalse(readers.production_cost_reader(settings, clock=lambda: NOW)("KIS_KR_CASH", "069500")["known"])
        settings["SHORT_LIVE_KIS_COST_CONFIRMED"] = True
        settings["SHORT_LIVE_KIS_COST_OBSERVED_AT"] = NOW - 86401
        self.assertFalse(readers.production_cost_reader(settings, clock=lambda: NOW)("KIS_KR_CASH", "069500")["known"])

    def test_fx_factory_does_not_freeze_real_request_observation_clock(self):
        fx = types.ModuleType(PKG + ".account_currency_limits")
        calls = []
        receipt = {"krwPerNativeCurrency": "1399.8", "rateAsOf": NOW, "contentHash": "a" * 64}
        def observe(reader, **kwargs):
            calls.append(kwargs)
            return receipt
        fx.observe_upbit_usdt_krw = observe
        fx.validate_conversion_observation = lambda row, **kwargs: row
        fx.conversion_observation_diagnostic = lambda row, **kwargs: {}
        with patch.dict(sys.modules, {fx.__name__: fx}):
            value = readers.production_fx_reader(object(), clock=lambda: NOW)("USDT")
        self.assertEqual([{}], calls)
        self.assertEqual("1399.8", value["krwPerQuote"])

    def test_final_quote_preserves_stale_provider_time_after_slow_metadata(self):
        r = self.build(); current = [NOW]; r.clock = lambda: current[0]
        def get(path, params=None, signed=True):
            if path == "/v1/orders/chance": return {"market": {"state": "active", "bid": {"min_total": "5000"}, "ask": {"min_total": "5000"}}, "bid_fee": "0.0005", "ask_fee": "0.0005"}
            if path == "/v1/ticker": return [{"market": "KRW-BTC", "trade_price": 100000000, "trade_timestamp": int(NOW*1000), "timestamp": int(NOW*1000)}]
            if path == "/v1/orderbook/instruments":
                current[0] += 11
                return [{"market": "KRW-BTC", "tick_size": "1000"}]
            raise AssertionError(path)
        # Exercise the actual instrumented reader: invalid source time now
        # fails at parsing and still carries the original safe timing values.
        r.reader._get = lambda provider, path, **kwargs: (get(path, kwargs.get("query")), {})
        r._upbit = lambda path, params=None, signed=True: r._read("upbit", path, query=params)[0]
        result = r.market("UPBIT_KRW_SPOT", "KRW-BTC")
        self.assertFalse(result["known"])
        self.assertEqual(["SOURCE_QUOTE_OBSERVATION_STALE"], result["blockers"])
        observation = result["readDiagnostics"][-1]["quoteObservation"]
        self.assertEqual(NOW + 11, mod.epoch(observation["receivedAt"]))
        self.assertEqual(NOW, mod.epoch(observation["sourceObservedAt"]))
        self.assertEqual(11, observation["sourceAgeSeconds"])
        adapter = mod.ShortLiveBrokerAdapter("UPBIT_KRW_SPOT", snapshot_reader=lambda *_: {}, market_reader=lambda *_: result,
                                             transport=None, authority_guard=lambda _: False, clock=lambda: current[0])
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "OBSERVATION_STALE"):
            adapter._market("KRW-BTC")

    def test_spot_actual_base_asset_fee_is_retained_from_exact_trades(self):
        r = self.build()
        def get(provider, path, params=None, signed=True):
            if path == "/api/v3/order": return {"symbol": "BTCUSDT", "orderId": 1, "clientOrderId": "short-client-001",
                "side": "BUY", "status": "FILLED", "executedQty": ".01", "price": "1000"}
            if path == "/api/v3/myTrades": return [{"id": 100, "symbol": "BTCUSDT", "orderId": 1, "qty": ".01", "price": "1000",
                                                     "commission": ".00001", "commissionAsset": "BTC"}]
            if path == "/api/v3/ticker/price": return {"symbol": "BTCUSDT", "price": "1000"}
            raise AssertionError(path)
        r._binance = get
        result = r.get_order("BINANCE_SPOT", {"brokerOrderId": "1", "symbol": "BTCUSDT"})
        self.assertTrue(result["known"])
        self.assertTrue(result["baseCommissionKnown"])
        self.assertEqual("0.00001", result["baseCommissionQuantity"])
        self.assertEqual("0.01000", result["feeNative"])

    def test_exact_bnb_commission_quantity_and_actual_quote_cost_are_retained(self):
        r = self.build()
        def get(provider, path, params=None, signed=True):
            if path == "/api/v3/order": return {"symbol": "BTCUSDT", "orderId": 1, "clientOrderId": "short-client-001",
                "side": "BUY", "status": "FILLED", "executedQty": ".01", "price": "1000"}
            if path == "/api/v3/myTrades": return [{"id": 100, "symbol": "BTCUSDT", "orderId": 1, "qty": ".01", "price": "1000",
                                                     "commission": ".001", "commissionAsset": "BNB"}]
            if path == "/api/v3/ticker/price": return {"symbol": "BNBUSDT", "price": "600"}
            raise AssertionError(path)
        r._binance = get
        result = r.get_order("BINANCE_SPOT", {"brokerOrderId": "1", "symbol": "BTCUSDT"})
        self.assertTrue(result["known"], result)
        self.assertEqual({"BNB": "0.001"}, result["externalCommissionQuantities"])
        self.assertEqual({"BNB": "0.600"}, result["externalCommissionNative"])
        self.assertTrue(result["externalCommissionKnown"])
        self.assertEqual("0", result["baseCommissionQuantity"])

    def test_duplicate_or_missing_trade_ids_cannot_create_external_fee_evidence(self):
        for identity in (100, None):
            r = self.build()
            def get(provider, path, params=None, signed=True):
                if path == "/api/v3/order": return {"symbol": "BTCUSDT", "orderId": 1, "clientOrderId": "short-client-001",
                    "side": "BUY", "status": "FILLED", "executedQty": ".01", "price": "1000"}
                if path == "/api/v3/myTrades": return [{"id": identity, "symbol": "BTCUSDT", "orderId": 1,
                    "qty": ".005", "price": "1000", "commission": ".001", "commissionAsset": "BNB"}] * 2
                raise AssertionError(path)
            r._binance = get
            result = r.get_order("BINANCE_SPOT", {"brokerOrderId": "1", "symbol": "BTCUSDT"})
            self.assertFalse(result["known"])


if __name__ == "__main__": unittest.main()
