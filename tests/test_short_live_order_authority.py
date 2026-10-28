"""Only in-memory boundary tests: no live package startup, files or I/O."""
from contextlib import contextmanager
import ast
import importlib
from pathlib import Path
import sys
import threading
import types
import urllib.parse
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_short_live_authority_tests"
package = types.ModuleType(PKG); package.__path__ = [str(ROOT)]; sys.modules[PKG] = package
emergency = types.ModuleType(PKG + ".emergency_stop")
@contextmanager
def boundary():
    yield {"active": False, "revision": "unit-1"}
emergency.emergency_stop_dispatch_boundary = boundary
sys.modules[emergency.__name__] = emergency
auth = importlib.import_module(PKG + ".short_live_order_authority")
binance = importlib.import_module(PKG + ".binance_order_authority")
upbit = importlib.import_module(PKG + ".upbit_order_authority")
kis = importlib.import_module(PKG + ".kis_order_authority")


class AuthorityTests(unittest.TestCase):
    def setUp(self):
        auth._reset_for_tests()
        self.order = {"route": "BINANCE_SPOT", "sessionId": "exact-session", "manifestHash": "a" * 64,
                      "accountId": "exact-account", "symbol": "BTCUSDT", "clientOrderId": "short-client-001",
                      "side": "BUY", "quantity": "0.01", "referencePrice": "1000", "riskReducing": False}
        self.native = {"broker_id": "binance", "symbol": "BTCUSDT", "quantity": "0.01", "side": "BUY", "price": "1000",
                       "identifier": "short-client-001", "new_client_order_id": "short-client-001", "order_type": "LIMIT",
                       "session_id": "exact-session", "manifest_hash": "a" * 64, "execution_purpose": "FUNCTIONAL_TEST_ONLY",
                       "risk_reducing": False, "reduce_only": False, "position_direction": "long"}
        self.allowed = True

    def tearDown(self):
        auth._reset_for_tests()

    def activate(self, route=None):
        if route:
            self.order["route"] = route
            self.native["broker_id"] = auth._PROVIDERS[route]
            if route == "KIS_KR_CASH":
                self.order.update(symbol="069500", quantity="10")
                self.native.update(asset="KR_STOCK", order_type="00", symbol="069500", quantity="10")
        auth.activate_short_live_routes([self.order])

    def test_active_exchange_blocks_ordinary_and_old_functional(self):
        self.activate()
        with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "EXCLUSIVE_ROUTE_FENCE"):
            with binance.ordinary_binance_final_mutation_boundary(operation="SPOT_PLACE_ORDER"): pass
        with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "EXCLUSIVE_ROUTE_FENCE"):
            with binance.functional_binance_final_mutation_boundary(session_id="old", cleanup_only=False): pass

    def test_same_exchange_cannot_be_owned_by_second_session(self):
        self.activate()
        with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "EXCHANGE_ALREADY_OWNED"):
            auth.activate_short_live_routes([{**self.order, "route": "BINANCE_USDM_FUTURES", "sessionId": "foreign"}])

    def test_exact_native_single_entry_and_fresh_service_reservation(self):
        self.activate()
        with auth.short_live_dispatch(self.order, self.native, lambda _: self.allowed):
            auth.validate_short_live_native_payload(self.native)
            self.assertTrue(auth.check_short_live_authority("BINANCE", operation="SPOT_PLACE_ORDER"))
            self.allowed = False
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "RESERVATION_CHANGED"):
                auth.check_short_live_authority("BINANCE", operation="SPOT_PLACE_ORDER")
        self.allowed = True
        with auth.short_live_dispatch(self.order, self.native, lambda _: True):
            auth.validate_short_live_native_payload(self.native)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "CHANGED_OR_REUSED"):
                auth.validate_short_live_native_payload(self.native)

    def test_payload_change_cancel_mismatch_and_foreign_thread_are_blocked(self):
        self.activate()
        with auth.short_live_dispatch(self.order, self.native, lambda _: True):
            with self.assertRaises(auth.ShortLiveAuthorityError):
                auth.validate_short_live_native_payload({**self.native, "quantity": "1"})
            auth.validate_short_live_native_payload(self.native)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "OPERATION_MISMATCH"):
                auth.check_short_live_authority("BINANCE", operation="SPOT_CANCEL_ORDER")
            failures = []
            def other():
                try: auth.check_short_live_authority("BINANCE")
                except auth.ShortLiveAuthorityError: failures.append(True)
            thread = threading.Thread(target=other); thread.start(); thread.join()
            self.assertEqual([True], failures)

    def test_binance_exception_preserves_real_app_lease(self):
        self.activate()
        snapshot = {"applicationInstanceLeaseHeld": True, "functionalAuthorityOpen": True}
        with patch.object(binance, "_snapshot", return_value=snapshot):
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with binance.ordinary_binance_final_mutation_boundary(operation="SPOT_PLACE_ORDER"):
                    pass
            snapshot["applicationInstanceLeaseHeld"] = False
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with self.assertRaises(binance.BinanceOrderAuthorityError):
                    with binance.ordinary_binance_final_mutation_boundary(operation="SPOT_PLACE_ORDER"): pass

    def test_upbit_exception_preserves_real_order_control(self):
        self.activate("UPBIT_KRW_SPOT")
        snapshot = {"applicationInstanceLeaseHeld": True, "functionalAuthorityOpen": True,
                    "ordinaryRoutesClosed": True, "ordinaryMutationEnabled": True}
        with patch.object(upbit, "_snapshot", return_value=snapshot):
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with upbit.ordinary_upbit_final_mutation_boundary(operation="PLACE_ORDER"): pass
            snapshot["ordinaryMutationEnabled"] = False
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with upbit.ordinary_upbit_final_mutation_boundary(operation="PLACE_ORDER"): pass
            snapshot["applicationInstanceLeaseHeld"] = False
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with self.assertRaises(upbit.UpbitOrderAuthorityError):
                    with upbit.ordinary_upbit_final_mutation_boundary(operation="PLACE_ORDER"): pass

    def test_kis_exact_scoped_exception_keeps_original_account_and_lease(self):
        self.activate("KIS_KR_CASH")
        snapshot = {"applicationInstanceLeaseHeld": True, "controlReservation": {},
                    "functionalAuthorityOpen": True, "functionalPhase": "RECONCILIATION_REQUIRED",
                    "ordinaryRoutesClosed": True, "stateRevision": 1, "functionalRevision": 1,
                    "ownerEpochId": "exact-owner", "ownerEpochHash": "b" * 64,
                    "functionalAccountFingerprint": "c" * 64, "credentialConfigurationHash": "d" * 64}
        intent = {"endpoint": "/uapi/domestic-stock/v1/trading/order-cash", "payloadHash": "e" * 64,
                  "accountFingerprint": "c" * 64, "credentialConfigurationHash": "d" * 64,
                  "ownedOrderKey": {}}
        with patch.object(kis, "_snapshot", return_value=snapshot), patch.object(kis, "_normalize_intent", side_effect=lambda x, **_: dict(x)):
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                auth.validate_short_live_native_payload(self.native)
                with kis.kis_authenticated_mutation_preflight(mode="ORDINARY"):
                    with kis.ordinary_kis_final_mutation_boundary(operation="PLACE_ORDER", intent=intent) as read:
                        self.assertEqual("PLACE_ORDER", read(endpoint=intent["endpoint"], payload_hash=intent["payloadHash"])["operation"])
            with self.assertRaises(auth.ShortLiveAuthorityError):
                with kis.kis_authenticated_mutation_preflight(mode="ORDINARY"): pass

    def test_release_exact_scope_only(self):
        self.activate()
        auth.release_short_live_routes("foreign", "a" * 64)
        with self.assertRaises(auth.ShortLiveAuthorityError): auth.check_short_live_authority("BINANCE")
        auth.release_short_live_routes(self.order["sessionId"], self.order["manifestHash"])
        self.assertFalse(auth.check_short_live_authority("BINANCE"))

    def test_context_must_map_reserved_order_to_actual_native_payload(self):
        self.activate()
        for field, value in (("quantity", "NaN"), ("quantity", True), ("side", "SELL"),
                             ("symbol", "ETHUSDT"), ("price", "1001"), ("identifier", "foreign-client"),
                             ("broker_id", "upbit"), ("reduce_only", True)):
            with self.subTest(field=field, value=value):
                with self.assertRaises(auth.ShortLiveAuthorityError):
                    with auth.short_live_dispatch(self.order, {**self.native, field: value}, lambda _: True):
                        self.fail("changed native payload reached authority")

    def prepared(self, **changes):
        query = {"symbol": self.order["symbol"], "side": "BUY", "type": "LIMIT", "quantity": ".01",
                 "price": "1000", "newClientOrderId": self.order["clientOrderId"]}
        values = {"provider": "binance", "method": "POST", "endpoint": "/api/v3/order", "body": None,
                  "url": "https://api.binance.com/api/v3/order?" + urllib.parse.urlencode(query), "headers": {}}
        values.update(changes)
        return types.SimpleNamespace(**values)

    def test_actual_wire_payload_and_one_physical_attempt_are_bound(self):
        self.activate()
        with auth.short_live_dispatch(self.order, self.native, lambda _: True):
            auth.validate_short_live_native_payload(self.native)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "RESERVED_WIRE_CHANGED"):
                auth.validate_short_live_prepared_request(self.prepared(url=self.prepared().url.replace("quantity=.01", "quantity=1")))
            auth.validate_short_live_prepared_request(self.prepared(), consume=True)
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "ALREADY_ATTEMPTED"):
                auth.validate_short_live_prepared_request(self.prepared(), consume=True)

    def test_real_signed_helper_never_retries_short_mutation_on_minus1021(self):
        # Compile only this pure helper definition from the real source. Its
        # fake sender records calls; the live package/env is never imported.
        tree = ast.parse((ROOT / "brokers.py").read_text(encoding="utf-8"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "send_binance_signed_request")
        calls = []
        namespace = {"__package__": PKG, "Callable": __import__("typing").Callable, "Any": object,
                     "send_prepared_request": lambda p: calls.append(p) or {"json": {"code": -1021}},
                     "refresh_binance_time_offset": lambda **_: self.fail("mutation retried")}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "broker-helper-test", "exec"), namespace)
        self.activate()
        with auth.short_live_dispatch(self.order, self.native, lambda _: True):
            auth.validate_short_live_native_payload(self.native)
            result = namespace["send_binance_signed_request"](self.prepared)
        self.assertEqual(-1021, result["json"]["code"])
        self.assertEqual(1, len(calls))

    def test_actual_spot_builder_preserves_exact_client_id(self):
        tree = ast.parse((ROOT / "live_adapters.py").read_text(encoding="utf-8"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "build_binance_spot_order_request")
        namespace = {"PreparedRequest": lambda **kwargs: types.SimpleNamespace(**kwargs),
                     "missing_env": lambda *_: [], "env_value": lambda key: "" if key == "BINANCE_BASE_URL" else "unit-test-placeholder",
                     "BINANCE_BASE_URL": "https://api.binance.com", "BINANCE_ORDER_ENDPOINT": "/api/v3/order",
                     "BINANCE_TEST_ORDER_ENDPOINT": "/api/v3/order/test", "normalize_side": lambda s: s.upper(),
                     "normalize_decimal_text": lambda s: str(s), "binance_timestamp_ms": lambda: 12345,
                     "sign_binance_query": lambda q, _: urllib.parse.urlencode(q), "urllib": __import__("urllib"), "re": __import__("re")}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "spot-builder-test", "exec"), namespace)
        request = namespace["build_binance_spot_order_request"](self.native)
        parsed = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(request.url).query))
        self.assertEqual(self.order["clientOrderId"], parsed["newClientOrderId"])

    def test_real_router_accepts_only_exact_short_reservation_with_global_switch_off(self):
        tree = ast.parse((ROOT / "brokers.py").read_text(encoding="utf-8"))
        helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_reserved_short_order_authorized")
        router = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "LiveBrokerRouter")
        place = next(n for n in router.body if isinstance(n, ast.FunctionDef) and n.name == "place_order")
        sent = []
        self.activate("UPBIT_KRW_SPOT")
        request = self.prepared(provider="upbit", endpoint="/v1/orders", url="https://api.upbit.com/v1/orders",
            body={"market": self.order["symbol"], "side": "bid", "ord_type": "limit", "identifier": self.order["clientOrderId"],
                  "volume": self.order["quantity"], "price": self.order["referencePrice"]})
        namespace = {"__package__": PKG, "real_orders_enabled": lambda: False, "BrokerNotReadyError": RuntimeError,
            "build_upbit_order_request": lambda _: request, "ordinary_upbit_final_mutation_boundary": upbit.ordinary_upbit_final_mutation_boundary,
            "UpbitOrderAuthorityError": upbit.UpbitOrderAuthorityError, "send_prepared_request": lambda value: sent.append(value) or {"ok": True}}
        exec(compile(ast.Module(body=[helper, place], type_ignores=[]), "real-router-offline", "exec"), namespace)
        snapshot = {"applicationInstanceLeaseHeld": True, "functionalAuthorityOpen": False,
                    "ordinaryRoutesClosed": True, "ordinaryMutationEnabled": False}
        with patch.object(upbit, "_snapshot", return_value=snapshot):
            with self.assertRaises(auth.ShortLiveAuthorityError):
                namespace["place_order"](None, self.native)
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                self.assertTrue(namespace["place_order"](None, self.native)["ok"])
            with auth.short_live_dispatch(self.order, self.native, lambda _: True):
                with self.assertRaises(auth.ShortLiveAuthorityError):
                    namespace["place_order"](None, {**self.native, "quantity": "99"})
        self.assertEqual([request], sent)

    def test_domestic_wire_endpoint_and_native_code_are_required(self):
        self.activate("KIS_KR_CASH")
        body = {"PDNO": "069500", "ORD_DVSN": "00", "ORD_QTY": "10", "ORD_UNPR": "1000"}
        request = self.prepared(provider="kis", endpoint="/uapi/domestic-stock/v1/trading/order-cash",
                                url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/order-cash",
                                body=body, headers={"tr_id": "TTTC0012U"})
        with auth.short_live_dispatch(self.order, self.native, lambda _: True):
            auth.validate_short_live_native_payload(self.native)
            auth.validate_short_live_prepared_request(request)
            request.endpoint = "/uapi/overseas-stock/v1/trading/order"
            request.url = "https://openapi.koreainvestment.com:9443" + request.endpoint
            with self.assertRaisesRegex(auth.ShortLiveAuthorityError, "DOMESTIC_LIMIT_WIRE_REQUIRED"):
                auth.validate_short_live_prepared_request(request)


if __name__ == "__main__": unittest.main()
