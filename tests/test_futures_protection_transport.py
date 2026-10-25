"""Offline request/authority tests; no app startup, credentials, or sockets."""
from __future__ import annotations

import ast
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal, ROUND_DOWN
from decimal import InvalidOperation
import importlib
from pathlib import Path
import re
import sys
import threading
import time
import types
import unittest
import urllib.parse
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_futures_protection_transport_tests"
package = types.ModuleType(PKG); package.__path__ = [str(ROOT)]; sys.modules[PKG] = package
emergency = types.ModuleType(PKG + ".emergency_stop")
@contextmanager
def emergency_boundary():
    yield {"active": False}
emergency.emergency_stop_dispatch_boundary = emergency_boundary
sys.modules[emergency.__name__] = emergency
auth = importlib.import_module(PKG + ".short_live_order_authority")
transport = importlib.import_module(PKG + ".futures_protection_transport")
binance = importlib.import_module(PKG + ".binance_order_authority")


def signed_request(method, endpoint, query, **_):
    wire = {**query, "timestamp": 1000, "signature": "fixture-only"}
    return types.SimpleNamespace(provider="binance-futures", method=method, endpoint=endpoint, body=None,
        url="https://fapi.binance.com" + endpoint + "?" + urllib.parse.urlencode(wire), query=wire)


# Compile the real pure builders while replacing only signing and network I/O.
# Importing the product package would otherwise load the user's configuration.
adapters = types.ModuleType(PKG + ".live_adapters")
adapters.__dict__.update(__package__=PKG, re=re, PreparedRequest=object,
    Decimal=Decimal, ROUND_DOWN=ROUND_DOWN, urllib=urllib, time=time,
    _BINANCE_SYMBOL_RULE_LOCK=threading.Lock(), _BINANCE_SYMBOL_RULE_CACHE={},
    env_value=lambda _: "", BINANCE_FUTURES_BASE_URL="https://fapi.binance.com",
    BINANCE_BASE_URL="https://api.binance.com", BINANCE_FUTURES_EXCHANGE_INFO_ENDPOINT="/fapi/v1/exchangeInfo",
    BINANCE_EXCHANGE_INFO_ENDPOINT="/api/v3/exchangeInfo",
    BINANCE_FUTURES_ALGO_ORDER_ENDPOINT="/fapi/v1/algoOrder", BINANCE_FUTURES_ORDER_ENDPOINT="/fapi/v1/order",
    BINANCE_FUTURES_TEST_ORDER_ENDPOINT="/fapi/v1/order/test", _build_binance_futures_signed_request=signed_request,
    _decimal_or_zero=lambda value: Decimal(str(value or 0)),
    normalize_decimal_text=lambda value: format(Decimal(str(value)), "f"),
    normalize_side=lambda value: str(value).upper())
builder_names = {"build_binance_futures_protection_request", "build_binance_futures_protection_lookup_request",
                 "build_binance_futures_order_request", "binance_symbol_rules", "normalize_binance_futures_intent",
                 "normalize_binance_spot_intent"}
tree = ast.parse((ROOT / "live_adapters.py").read_text(encoding="utf-8-sig"))
exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in builder_names],
                        type_ignores=[]), str(ROOT / "live_adapters.py"), "exec"), adapters.__dict__)
sys.modules[adapters.__name__] = adapters

broker_tree = ast.parse((ROOT / "brokers.py").read_text(encoding="utf-8-sig"))
broker_class = next(n for n in broker_tree.body if isinstance(n, ast.ClassDef) and n.name == "LiveBrokerRouter")
router_methods = {"place_futures_protection", "get_futures_protection", "cancel_futures_protection"}
router_ast = ast.ClassDef(name="FixtureRouter", bases=[], keywords=[], decorator_list=[],
    body=[n for n in broker_class.body if isinstance(n, ast.FunctionDef) and n.name in router_methods])
broker_globals = {"__package__": PKG, "re": re, "Decimal": Decimal, "InvalidOperation": InvalidOperation,
    "BrokerNotReadyError": type("BrokerNotReadyError", (RuntimeError,), {}),
    "BinanceOrderAuthorityError": binance.BinanceOrderAuthorityError,
    "ordinary_binance_final_mutation_boundary": binance.ordinary_binance_final_mutation_boundary,
    "real_orders_enabled": lambda: True, "ensure_response_ok": lambda _, value: value.get("json", value),
    "build_binance_futures_position_mode_request": lambda: types.SimpleNamespace(method="GET", endpoint="/fapi/v1/positionSide/dual"),
    "compare_futures_policy_to_broker": lambda *_: {"compliant": True},
    "numeric_value": lambda value, default=0: float(value if value is not None else default)}
policy_function = next(n for n in broker_tree.body if isinstance(n, ast.FunctionDef) and n.name == "validate_binance_futures_execution_policy")
exec(compile(ast.fix_missing_locations(ast.Module(body=[router_ast, policy_function], type_ignores=[])),
             str(ROOT / "brokers.py"), "exec"), broker_globals)
FixtureRouter = broker_globals["FixtureRouter"]


def claim(**updates):
    return {"route": "BINANCE_USDM_FUTURES", "operation": "PROTECT", "sessionId": "session-fixture",
            "manifestHash": "a" * 64, "accountId": "account-fixture", "symbol": "SOLUSDT", "side": "SELL",
            "positionDirection": "long", "positionMode": "ONE_WAY", "clientAlgoId": "slt-stop-fixture-001",
            "triggerPrice": "95.00", "riskReducing": True, **updates}


def observation(**updates):
    return {"algoId": 123, "clientAlgoId": "slt-stop-fixture-001", "symbol": "SOLUSDT", "side": "SELL",
            "positionSide": "BOTH", "orderType": "STOP_MARKET", "algoType": "CONDITIONAL",
            "closePosition": True, "workingType": "MARK_PRICE", "priceProtect": False,
            "triggerPrice": "95", "algoStatus": "NEW", "actualOrderId": "", **updates}


class ProtectionTests(unittest.TestCase):
    def setUp(self):
        auth._reset_for_tests()

    def tearDown(self):
        auth._reset_for_tests()

    def test_exchange_rules_select_exact_symbol_and_order_type_without_cache_mix(self):
        adapters._BINANCE_SYMBOL_RULE_CACHE.clear()
        calls = []
        def row(symbol, lot, market, notional):
            return {"symbol": symbol, "filters": [
                {"filterType": "LOT_SIZE", "minQty": lot, "maxQty": "1000", "stepSize": lot},
                {"filterType": "MARKET_LOT_SIZE", "minQty": market, "maxQty": "100", "stepSize": market},
                {"filterType": "MIN_NOTIONAL", "notional": notional}]}
        def read(method, url, **kwargs):
            calls.append((method, url))
            return {"ok": True, "json": {"symbols": [row("BTCUSDT", ".001", ".002", "100"),
                                                          row("SOLUSDT", ".1", ".2", "5")]}}
        with patch.object(adapters, "http_json", read, create=True):
            limit = adapters.binance_symbol_rules("SOLUSDT", futures=True, order_type="LIMIT")
            market = adapters.binance_symbol_rules("SOLUSDT", futures=True, order_type="MARKET")
            cached = adapters.binance_symbol_rules("SOLUSDT", futures=True, order_type="LIMIT")
        self.assertEqual(Decimal(".1"), limit["stepSize"])
        self.assertEqual(Decimal(".2"), market["stepSize"])
        self.assertEqual(Decimal("5"), limit["minNotional"])
        self.assertEqual(limit, cached)
        self.assertEqual(2, len(calls))
        self.assertEqual({("GET", "https://fapi.binance.com/fapi/v1/exchangeInfo")}, set(calls))

    def test_rules_reject_missing_or_duplicate_symbol_and_preserve_market_max(self):
        target = {"symbol": "SOLUSDT", "filters": [
            {"filterType": "LOT_SIZE", "minQty": ".1", "maxQty": "1000", "stepSize": ".1"},
            {"filterType": "MARKET_LOT_SIZE", "minQty": ".2", "maxQty": "10", "stepSize": "0"},
            {"filterType": "MIN_NOTIONAL", "notional": "5"}]}
        for rows in ([], [{**target, "symbol": "BTCUSDT"}], [target, target]):
            adapters._BINANCE_SYMBOL_RULE_CACHE.clear()
            with self.subTest(rows=len(rows)), patch.object(adapters, "http_json", return_value={"ok": True, "json": {"symbols": rows}}, create=True):
                with self.assertRaises(RuntimeError):
                    adapters.binance_symbol_rules("SOLUSDT", futures=True)
        adapters._BINANCE_SYMBOL_RULE_CACHE.clear()
        with patch.object(adapters, "http_json", return_value={"ok": True, "json": {"symbols": [target]}}, create=True):
            rules = adapters.binance_symbol_rules("SOLUSDT", futures=True)
        self.assertEqual(Decimal(".1"), rules["stepSize"])
        self.assertEqual(Decimal("10"), rules["maxQty"])

    def test_normalizers_request_type_and_reducing_min_notional_exemption_is_futures_only(self):
        rules = {"minQty": Decimal(".1"), "maxQty": Decimal("100"), "stepSize": Decimal(".1"), "minNotional": Decimal("5")}
        row = {"symbol": "SOLUSDT", "side": "SELL", "quantity": ".1", "price": "10", "order_type": "LIMIT",
               "risk_reducing": True, "reduce_only": True, "position_direction": "long"}
        with patch.object(adapters, "binance_symbol_rules", return_value=rules) as lookup:
            self.assertEqual("0.1", adapters.normalize_binance_futures_intent(row)["quantity"])
            lookup.assert_called_with("SOLUSDT", futures=True, order_type="LIMIT")
            with self.assertRaises(RuntimeError):
                adapters.normalize_binance_futures_intent({**row, "risk_reducing": False, "reduce_only": False})
            with self.assertRaises(RuntimeError):
                adapters.normalize_binance_spot_intent(row)
            lookup.assert_called_with("SOLUSDT", order_type="LIMIT")

    def test_futures_quantity_grid_is_anchored_at_minimum(self):
        rules = {"minQty": Decimal(".3"), "maxQty": Decimal("100"), "stepSize": Decimal(".2"), "minNotional": Decimal("0")}
        for kind in ("LIMIT", "MARKET"):
            with self.subTest(kind=kind), patch.object(adapters, "binance_symbol_rules", return_value=rules):
                intent = {"symbol": "SOLUSDT", "side": "SELL", "price": "10", "order_type": kind,
                          "risk_reducing": True, "position_direction": "long"}
                self.assertEqual("0.5", adapters.normalize_binance_futures_intent({**intent, "quantity": ".5"})["quantity"])
                self.assertEqual("0.5", adapters.normalize_binance_futures_intent({**intent, "quantity": ".6"})["quantity"])
                with self.assertRaises(RuntimeError):
                    adapters.normalize_binance_futures_intent({**intent, "quantity": ".2"})

    def test_verified_reducing_intent_is_not_blocked_by_margin_or_leverage_drift(self):
        validate = broker_globals["validate_binance_futures_execution_policy"]
        for direction, side in (("long", "SELL"), ("short", "BUY")):
            intent = {"risk_reducing": True, "reduce_only": True, "expected_leverage": "5",
                      "position_direction": direction, "side": side, "position_mode": "HEDGE",
                      "required_margin_type": "ISOLATED"}
            with patch.dict(broker_globals, {"compare_futures_policy_to_broker": lambda *_: {"compliant": False, "blockers": ["DRIFT"]}}):
                validate(intent, {"leverage": "20", "margin_type": "CROSSED"})

    def test_close_all_long_and_short_builder_uses_exact_algo_contract(self):
        for mode in ("ONE_WAY", "HEDGE"):
            for direction, side in (("long", "SELL"), ("short", "BUY")):
                row = claim(positionMode=mode, positionDirection=direction, side=side)
                native = transport.protection_native(row)
                req = adapters.build_binance_futures_protection_request(native)
                self.assertEqual("/fapi/v1/algoOrder", req.endpoint)
                self.assertEqual("true", req.query["closePosition"])
                self.assertEqual("MARK_PRICE", req.query["workingType"])
                self.assertEqual("false", req.query["priceProtect"])
                self.assertEqual(direction.upper() if mode == "HEDGE" else "BOTH", req.query["positionSide"])
                self.assertNotIn("quantity", req.query)
                self.assertNotIn("reduceOnly", req.query)
                self.assertNotIn("price", req.query)

    def test_reverse_direction_invalid_numbers_and_quantity_are_rejected(self):
        for updates in ({"side": "BUY"}, {"triggerPrice": "NaN"}, {"triggerPrice": True},
                        {"triggerPrice": "0"}, {"positionMode": "unknown"}, {"clientAlgoId": "x" * 37}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                transport.protection_native(claim(**updates))
        native = transport.protection_native(claim())
        for key in ("quantity", "reduce_only", "price"):
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                adapters.build_binance_futures_protection_request({**native, key: "1"})

    def test_exact_stop_wire_consumed_once_and_tampering_rejected(self):
        row = claim(); native = transport.protection_native(row)
        auth.activate_short_live_routes([row])
        with auth.short_live_dispatch(row, native, lambda _: True):
            auth.validate_short_live_native_payload(native)
            req = adapters.build_binance_futures_protection_request(native)
            auth.validate_short_live_prepared_request(req, consume=True)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "ALREADY_ATTEMPTED"):
                auth.validate_short_live_prepared_request(req, consume=True)
        for extra in ({"quantity": "5"}, {"reduceOnly": "true"}, {"triggerPrice": "94"},
                      {"workingType": "CONTRACT_PRICE"}, {"clientAlgoId": "foreign"}, {"positionSide": "SHORT"}):
            with self.subTest(extra=extra):
                req = adapters.build_binance_futures_protection_request(native)
                bad = {**req.query, **extra}
                req.url = "https://fapi.binance.com/fapi/v1/algoOrder?" + urllib.parse.urlencode(bad)
                with auth.short_live_dispatch(row, native, lambda _: True):
                    auth.validate_short_live_native_payload(native)
                    with self.assertRaises(auth.ShortLiveAuthorityError):
                        auth.validate_short_live_prepared_request(req, consume=True)

    def test_stop_authority_rechecks_guard_and_operation(self):
        row = claim(); native = transport.protection_native(row)
        auth.activate_short_live_routes([row])
        allowed = [True]
        with auth.short_live_dispatch(row, native, lambda _: allowed[0]):
            auth.validate_short_live_native_payload(native)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "OPERATION_MISMATCH"):
                auth.check_short_live_authority("BINANCE", operation="FUTURES_PLACE_ORDER")
            allowed[0] = False
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "RESERVATION_CHANGED"):
                auth.validate_short_live_prepared_request(adapters.build_binance_futures_protection_request(native), consume=True)

    def test_lost_post_reply_never_reposts_same_client_and_lookup_does_not_mutate(self):
        class Router:
            calls = 0
            def place_futures_protection(self, native):
                self.calls += 1
                raise TimeoutError("not displayed")
            def get_futures_protection(self, symbol, **ids):
                return observation()
        router = Router(); edge = transport.FuturesProtectionTransport(router)
        row = claim(); auth.activate_short_live_routes([row])
        with self.assertRaises(TimeoutError): edge.protect(row, lambda _: True)
        with self.assertRaisesRegex(transport.FuturesProtectionError, "ALREADY_ATTEMPTED"):
            edge.protect(row, lambda _: True)
        self.assertEqual("NEW", edge.lookup(row)["status"])
        self.assertEqual(1, router.calls)

    def test_lookup_requires_both_identity_and_original_terms(self):
        self.assertTrue(transport.normalize_protection_observation(observation(), claim(), observed_at=1)["known"])
        for updates in ({"algoId": 99}, {"symbol": "BTCUSDT"}, {"clientAlgoId": "foreign"},
                        {"workingType": "CONTRACT_PRICE"}, {"triggerPrice": "94"}, {"closePosition": False},
                        {"priceProtect": True}, {"algoStatus": "UNRECOGNIZED"}, {"actualOrderId": "900"}):
            with self.subTest(updates=updates), self.assertRaises(transport.FuturesProtectionError):
                transport.normalize_protection_observation(observation(**updates), claim(algoId="123"), observed_at=1)
        result = transport.normalize_protection_observation(observation(algoStatus="FINISHED", actualOrderId="900"),
                                                          claim(algoId="123"), observed_at=1)
        self.assertEqual("900", result["actualOrderId"])
        self.assertNotIn("filledQuantity", result)  # child order/trades must supply real fills

    def test_cancel_is_exact_known_algo_and_no_symbol_or_all_orders_wire(self):
        row = claim(operation="CANCEL_PROTECTION", algoId="123")
        native = transport.protection_native(row); auth.activate_short_live_routes([row])
        req = adapters.build_binance_futures_protection_lookup_request(algo_id="123", cancel=True)
        self.assertEqual({"algoId", "timestamp", "signature"}, set(req.query))
        with auth.short_live_dispatch(row, native, lambda _: True):
            auth.validate_short_live_native_payload(native)
            auth.validate_short_live_prepared_request(req, consume=True)
        with self.assertRaises(RuntimeError):
            adapters.build_binance_futures_protection_lookup_request(client_algo_id=row["clientAlgoId"], cancel=True)

    def test_emergency_close_market_never_creates_reverse_position_side(self):
        for mode in ("ONE_WAY", "HEDGE"):
            row = claim(operation="EMERGENCY_CLOSE", positionMode=mode, clientOrderId="slt-close-001",
                        quantity="0.1", referencePrice="100")
            native = {"broker_id": "binance-futures", "symbol": row["symbol"], "side": "SELL", "quantity": "0.1",
                      "price": "100", "identifier": row["clientOrderId"], "new_client_order_id": row["clientOrderId"],
                      "session_id": row["sessionId"], "manifest_hash": row["manifestHash"], "execution_purpose": "FUNCTIONAL_TEST_ONLY",
                      "order_type": "MARKET", "risk_reducing": True, "reduce_only": True,
                      "position_direction": "long", "position_mode": mode}
            auth.activate_short_live_routes([row])
            req = adapters.build_binance_futures_order_request(native, hedge_mode=mode == "HEDGE")
            with auth.short_live_dispatch(row, native, lambda _: True):
                auth.validate_short_live_native_payload(native)
                auth.validate_short_live_prepared_request(req, consume=True)
            self.assertNotIn("price", req.query)
            self.assertEqual("MARKET", req.query["type"])
            if mode == "ONE_WAY": self.assertEqual("true", req.query["reduceOnly"])
            else: self.assertNotIn("reduceOnly", req.query)
            with self.assertRaises(auth.ShortLiveAuthorityError):
                with auth.short_live_dispatch({**row, "side": "BUY"}, {**native, "side": "BUY"}, lambda _: True): pass

    def test_real_router_uses_current_mode_final_boundary_and_single_physical_send(self):
        row = claim(); auth.activate_short_live_routes([row])
        calls = []
        def send(builder, **_):
            req = builder(); calls.append((req.method, req.endpoint))
            if req.endpoint == "/fapi/v1/positionSide/dual": return {"dualSidePosition": False}
            auth.validate_short_live_prepared_request(req, consume=True)
            return {"json": observation()}
        edge = transport.FuturesProtectionTransport(FixtureRouter())
        with patch.dict(broker_globals, {"send_binance_signed_request": send}), patch.object(binance, "_snapshot", return_value={
                "applicationInstanceLeaseHeld": True, "functionalAuthorityOpen": False}):
            result = edge.protect(row, lambda _: True)
        self.assertEqual(123, result["json"]["algoId"])
        self.assertEqual([("GET", "/fapi/v1/positionSide/dual"), ("POST", "/fapi/v1/algoOrder")], calls)

    def test_real_router_refuses_unscoped_disabled_or_changed_position_mode(self):
        router = FixtureRouter(); row = claim(); native = transport.protection_native(row)
        with self.assertRaisesRegex(RuntimeError, "SCOPED_PROTECTION"):
            router.place_futures_protection(native)
        auth.activate_short_live_routes([row])
        for enabled, dual in ((False, False), (True, True)):
            calls = []
            def send(builder, **_):
                req = builder(); calls.append(req.method)
                return {"dualSidePosition": dual}
            with patch.dict(broker_globals, {"real_orders_enabled": lambda: enabled, "send_binance_signed_request": send}):
                with auth.short_live_dispatch(row, native, lambda _: True), self.assertRaises(RuntimeError):
                    router.place_futures_protection(native)
            self.assertNotIn("POST", calls)

    def test_final_entry_policy_binds_actual_leverage_in_both_directions_of_drift(self):
        validate = broker_globals["validate_binance_futures_execution_policy"]
        intent = {"expected_leverage": 5, "max_leverage": 5}
        validate(intent, {"leverage": "5", "margin_type": "ISOLATED"})
        for actual in (1, 3, 10, None, "NaN"):
            with self.subTest(actual=actual), self.assertRaisesRegex(RuntimeError, "EXACT_LEVERAGE_CHANGED"):
                validate(intent, {"leverage": actual})
        validate({**intent, "risk_reducing": True}, {"leverage": 3})


if __name__ == "__main__":
    unittest.main()
