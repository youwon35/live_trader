"""Offline shared-request wiring; no product startup, keys, sockets or orders."""
from __future__ import annotations

import ast
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import importlib
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.parse
import urllib.request


ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_shared_public_request_live_fixture"
package = types.ModuleType(PKG)
package.__path__ = [str(ROOT)]
sys.modules[PKG] = package
adapter_stub = types.ModuleType(PKG + ".live_adapters")
def no_signing(*_args, **_kwargs):
    raise AssertionError("synthetic transport checks never sign requests")
for name in ("build_upbit_authorization", "sign_binance_query", "split_kis_account"):
    setattr(adapter_stub, name, no_signing)
sys.modules[adapter_stub.__name__] = adapter_stub
reads = importlib.import_module(PKG + ".read_only_account")
short = importlib.import_module(PKG + ".short_live_broker_readers")
from trading_runtime import public_request_manager as shared


class Response(io.BytesIO):
    def __init__(self, payload, url, *, status=200):
        super().__init__(json.dumps(payload).encode())
        self.url, self.status = url, status
        self.headers = {"Date": "Sun, 01 Nov 2026 00:00:00 GMT", "Authorization": "must-not-share"}
    def geturl(self):
        return self.url


class ReadSharingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        manager = shared.PublicRequestCoordinator(Path(temporary.name) / "requests.sqlite3")
        scope = patch.object(shared, "_DEFAULT", manager)
        scope.start(); self.addCleanup(scope.stop)
        self.transport = reads.ReadOnlyTransport()

    def test_same_public_get_reuses_original_receipt_and_filters_headers(self):
        def open_(req, **kwargs):
            return Response({"symbol": "BTCUSDT", "price": "100"}, req.full_url)
        self.transport.opener = types.SimpleNamespace(open=Mock(side_effect=open_))
        first, first_headers = self.transport.request("binance", "GET", "/api/v3/ticker/price", query={"symbol": "BTCUSDT"})
        observation = self.transport.last_public_observation
        first["price"] = "changed"
        second, headers = self.transport.request("binance", "GET", "/api/v3/ticker/price", query={"symbol": "BTCUSDT"})
        self.assertEqual(self.transport.opener.open.call_count, 1)
        self.assertEqual(second["price"], "100")
        self.assertEqual(self.transport.last_public_observation.received_at, observation.received_at)
        self.assertTrue(self.transport.last_public_observation.cache_hit)
        self.assertEqual(first_headers, headers)
        self.assertNotIn("authorization", headers)

    def test_private_and_sealed_fx_reads_stay_physical_and_clear_public_receipt(self):
        scopes = []
        @contextmanager
        def critical(provider, **kwargs):
            scopes.append((provider, kwargs["purpose"]))
            yield
        self.transport.opener = types.SimpleNamespace(open=Mock(side_effect=lambda req, **kwargs: Response({}, req.full_url)))
        with patch.object(reads, "critical_request_scope", side_effect=critical), \
             patch.object(reads, "shared_public_json", side_effect=AssertionError("private/FX may not share")):
            for provider, path, query, headers in (
                ("binance", "/api/v3/account", "signature=synthetic&timestamp=1", {"X-MBX-APIKEY": "fixture"}),
                ("upbit", "/v1/orderbook", {"markets": "KRW-USDT"}, {}),
                ("binance", "/api/v3/time", {}, {}),
            ):
                self.transport.last_public_observation = object()
                for _ in range(2):
                    self.transport.request(provider, "GET", path, query=query, headers=headers)
                    self.assertIsNone(self.transport.last_public_observation)
        self.assertEqual(self.transport.opener.open.call_count, 6)
        self.assertEqual(scopes, [("binance", "PROTECTION")]*2 + [("upbit", "PROTECTION")]*2 + [("binance", "PROTECTION")]*2)

    def test_read_boundary_blocks_mutation_before_manager_or_opener(self):
        self.transport.opener = Mock()
        with patch.object(reads, "shared_public_json") as request, patch.object(reads, "critical_request_scope") as critical:
            for method, path, body in (("POST", "/api/v3/order", {}), ("GET", "/api/v3/ticker/price", {}), ("GET", "/api/v3/unknown", None)):
                self.transport.last_public_observation = object()
                with self.assertRaises(reads.ReadBoundaryError):
                    self.transport.request("binance", method, path, body=body)
                self.assertIsNone(self.transport.last_public_observation)
        self.transport.opener.open.assert_not_called()
        request.assert_not_called(); critical.assert_not_called()

    def test_failed_public_transport_has_one_attempt_and_no_previous_receipt(self):
        self.transport.last_public_observation = object()
        self.transport.opener = types.SimpleNamespace(open=Mock(side_effect=TimeoutError("fixture timeout")))
        with self.assertRaises(reads.ReadFailure):
            self.transport.request("upbit", "GET", "/v1/ticker", query={"markets": "KRW-BTC"})
        self.assertEqual(self.transport.opener.open.call_count, 1)
        self.assertIsNone(self.transport.last_public_observation)
        self.assertEqual(self.transport.events[-1]["status"], "UNAVAILABLE")

    def test_failed_shared_follower_does_not_fallback_or_expose_remote_failure(self):
        self.transport.opener = Mock()
        self.transport.last_public_observation = object()
        with patch.object(reads, "shared_public_json", side_effect=shared.PublicRequestUnavailable("fixture-private-message")):
            with self.assertRaisesRegex(reads.ReadFailure, "^READ_UNAVAILABLE$"):
                self.transport.request("upbit", "GET", "/v1/ticker", query={"markets": "KRW-BTC"})
        self.transport.opener.open.assert_not_called()
        self.assertIsNone(self.transport.last_public_observation)
        self.assertEqual(self.transport.events[-1]["status"], "UNAVAILABLE")
        self.assertEqual(short._safe_read_failure(shared.PublicRequestUnavailable("fixture-private-message")),
            {"status": "UNAVAILABLE", "failureKind": "READ_UNAVAILABLE"})

    def test_extended_futures_public_and_private_paths_do_not_mix(self):
        reader = short.ProductionShortLiveReaders(settings={}, fx_reader=lambda _: {})
        transport = reader.reader.transport
        transport.opener = types.SimpleNamespace(open=Mock(side_effect=lambda req, **kwargs: Response({"symbol": "SOLUSDT"}, req.full_url)))
        for _ in range(2):
            transport.request("binance-futures", "GET", "/fapi/v1/premiumIndex", query={"symbol": "SOLUSDT"})
        self.assertEqual(transport.opener.open.call_count, 1)
        self.assertTrue(transport.last_public_observation.cache_hit)
        for _ in range(2):
            transport.request("binance-futures", "GET", "/fapi/v3/positionRisk", query="signature=fixture&timestamp=1", headers={"X-MBX-APIKEY": "fixture"})
            self.assertIsNone(transport.last_public_observation)
        self.assertEqual(transport.opener.open.call_count, 3)

    def test_same_transport_concurrent_threads_keep_their_own_receipts(self):
        arrived = threading.Barrier(2)
        readings, errors = {}, []
        def open_(req, **kwargs):
            symbol = urllib.parse.parse_qs(urllib.parse.urlparse(req.full_url).query)["symbol"][0]
            return Response({"symbol": symbol, "price": "100"}, req.full_url)
        self.transport.opener = types.SimpleNamespace(open=Mock(side_effect=open_))
        def read(symbol):
            try:
                payload, _ = self.transport.request("binance", "GET", "/api/v3/ticker/price", query={"symbol": symbol})
                arrived.wait(timeout=3)
                readings[symbol] = (payload["symbol"], self.transport.last_public_observation.payload["symbol"])
            except BaseException as exc:
                errors.append(exc)
        workers = [threading.Thread(target=read, args=(symbol,)) for symbol in ("BTCUSDT", "ETHUSDT")]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)
        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(readings, {"BTCUSDT": ("BTCUSDT", "BTCUSDT"), "ETHUSDT": ("ETHUSDT", "ETHUSDT")})
        self.assertIsNone(self.transport.last_public_observation)
        self.assertEqual(self.transport.opener.open.call_count, 2)

    def test_upbit_validation_retains_cache_receipt_not_current_clock(self):
        now = time.time()
        original = shared.PublicJsonObservation([], {}, now-.4, now-.3, .1, True)
        dummy = types.SimpleNamespace(transport=types.SimpleNamespace(last_public_observation=original))
        reader = short.ProductionShortLiveReaders(settings={}, reader=dummy, clock=lambda: now)
        quote = {"market": "KRW-BTC", "timestamp": int(now*1000), "trade_timestamp": int(now*1000)}
        observation = reader._upbit_quote_observation(quote, "KRW-BTC", purpose="ORDER_MARKET", started=now, received=now, previous_read=None)
        self.assertAlmostEqual(datetime.fromisoformat(observation["receivedAt"].replace("Z", "+00:00")).timestamp(), now-.3, delta=0.000001)
        self.assertAlmostEqual(observation["receiveAgeSeconds"], .3, places=5)


# Compile the actual transport function while leaving configuration/authority
# imports untouched. Only the injected wire opener can run in these fixtures.
tree = ast.parse((ROOT / "live_adapters.py").read_text(encoding="utf-8-sig"))
transport_globals = {
    "copy": copy, "json": json, "urllib": urllib,
    "parse_json": json.loads, "KisOrderAuthorityError": type("KisFixtureError", (ValueError,), {}),
    "_KisTradingNoRedirectHandler": urllib.request.HTTPRedirectHandler,
    "_guard_kis_owned_http_edge": lambda **_: "NON_KIS",
}
exec(compile(ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)
    and node.name == "http_json"], type_ignores=[]), str(ROOT / "live_adapters.py"), "exec"), transport_globals)


class WirePriorityTests(unittest.TestCase):
    def test_each_exact_provider_gets_priority_without_caching_or_retries(self):
        scopes = []
        @contextmanager
        def critical(provider, **kwargs):
            scopes.append((provider, kwargs["purpose"]))
            yield
        opener = Mock(side_effect=lambda req, **kwargs: Response({"fixture": True}, req.full_url))
        with patch.object(shared, "critical_request_scope", side_effect=critical), \
             patch.object(urllib.request, "build_opener", return_value=types.SimpleNamespace(open=opener)):
            for host in ("api.binance.com", "fapi.binance.com", "api.upbit.com"):
                for _ in range(2):
                    result = transport_globals["http_json"]("POST", "https://"+host+"/fixture", body={"fixture": True}, headers={}, timeout_seconds=8)
                    self.assertTrue(result["ok"])
        self.assertEqual(opener.call_count, 6)
        self.assertEqual(scopes, [("binance", "ORDER")]*2 + [("binance-futures", "ORDER")]*2 + [("upbit", "ORDER")]*2)

    def test_authority_failure_precedes_any_priority_lease_or_physical_attempt(self):
        with patch.dict(transport_globals, {"_guard_kis_owned_http_edge": Mock(side_effect=ValueError("fixture guard"))}), \
             patch.object(shared, "critical_request_scope") as critical, patch.object(urllib.request, "build_opener") as opener:
            with self.assertRaisesRegex(ValueError, "fixture guard"):
                transport_globals["http_json"]("POST", "https://api.upbit.com/fixture", body={}, headers={}, timeout_seconds=8)
        critical.assert_not_called(); opener.assert_not_called()

    def test_timeout_does_not_trigger_order_resubmission(self):
        @contextmanager
        def critical(*_args, **_kwargs):
            yield
        opener = Mock(side_effect=TimeoutError("fixture timeout"))
        with patch.object(shared, "critical_request_scope", side_effect=critical), \
             patch.object(urllib.request, "build_opener", return_value=types.SimpleNamespace(open=opener)):
            result = transport_globals["http_json"]("POST", "https://fapi.binance.com/fixture", body={}, headers={}, timeout_seconds=8)
        self.assertFalse(result["ok"])
        self.assertEqual(opener.call_count, 1)


if __name__ == "__main__":
    unittest.main()
