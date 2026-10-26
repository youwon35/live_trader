"""Synthetic reads only: no production app import, credentials or network."""
from __future__ import annotations

import io
import json
import sys
import types
import unittest
import urllib.error
from unittest.mock import patch

from test_short_live_broker_readers import readers
from test_short_live_brokers import mod, PKG, NOW


def build_readers(get=None):
    reader = types.SimpleNamespace(_get=get or (lambda *_a, **_k: ({}, {})))
    result = readers.ProductionShortLiveReaders(settings={}, reader=reader, clock=lambda: NOW,
        fx_reader=lambda _: {"known": True, "krwPerQuote": "1400", "observedAt": NOW})
    result._identity = lambda _: "live-account:synthetic"
    return result


class SafeReadDiagnosticTests(unittest.TestCase):
    def test_http_failure_retains_only_fixed_path_status_and_elapsed(self):
        secret = "SYNTHETIC_SECRET_MUST_NOT_ESCAPE"
        def fail(*_a, **_k):
            raise urllib.error.HTTPError("https://example.invalid/?signature=" + secret,
                429, secret, {"Authorization": secret}, io.BytesIO(secret.encode()))
        r = build_readers(fail)
        with patch.object(readers.time, "monotonic", side_effect=[10, 10.125]):
            with self.assertRaises(urllib.error.HTTPError) as raised:
                r._read("binance-futures", "/fapi/v1/commissionRate",
                    query={"signature": secret}, headers={"X-MBX-APIKEY": secret})
        raised.exception.close()
        self.assertEqual([{"provider": "binance-futures", "path": "/fapi/v1/commissionRate",
            "elapsedMs": 125.0, "status": "REJECTED", "failureKind": "HTTP_ERROR", "httpStatus": 429}],
            r._failure_reads())
        self.assertNotIn(secret, json.dumps(r._failure_reads()))

    def test_sanitized_exception_keeps_timeout_from_hidden_context(self):
        def fail(*_a, **_k):
            try:
                raise urllib.error.URLError(TimeoutError("synthetic raw url and key"))
            except urllib.error.URLError:
                raise RuntimeError("READ_UNAVAILABLE") from None
        r = build_readers(fail)
        with self.assertRaises(RuntimeError):
            r._read("binance", "/api/v3/time")
        self.assertEqual("TIMEOUT", r._failure_reads()[0]["failureKind"])
        self.assertNotIn("synthetic raw", json.dumps(r._failure_reads()))

    def test_connection_json_and_boundary_have_fixed_failure_kinds(self):
        for exc, kind in ((ConnectionError("private"), "CONNECTION_ERROR"),
                          (json.JSONDecodeError("private", "private", 0), "INVALID_RESPONSE"),
                          (ValueError("READ_ONLY_ENDPOINT_BLOCKED"), "READ_BOUNDARY"),
                          (mod.ShortLiveBrokerBlocked("OFFICIAL_READ_REJECTED"), "READ_REJECTED"),
                          (mod.ShortLiveBrokerBlocked("OFFICIAL_PRIVATE_READ_REJECTED"), "READ_REJECTED"),
                          (RuntimeError("private arbitrary message"), "UNEXPECTED_ERROR")):
            with self.subTest(kind=kind):
                self.assertEqual(kind, readers._safe_read_failure(exc)["failureKind"])
                self.assertNotIn("private", json.dumps(readers._safe_read_failure(exc)))

    def test_sanitizer_discards_raw_fields_and_invalid_identifiers(self):
        row = {"provider": "upbit", "path": "/v1/ticker", "elapsedMs": 8.5,
            "status": "AVAILABLE", "url": "private", "query": {"private": "private"},
            "headers": {"Authorization": "private"}, "body": "private", "message": "private"}
        self.assertEqual([{"provider": "upbit", "path": "/v1/ticker", "elapsedMs": 8.5,
            "status": "AVAILABLE"}], readers.sanitize_read_diagnostics([row]))
        for field, value in (("provider", "secret"), ("path", "/v1/ticker?signature=secret"),
                             ("path", "/v1/unknown"), ("status", "private"),
                             ("provider", []), ("path", {}), ("status", [])):
            with self.subTest(field=field, value=value):
                self.assertEqual([], readers.sanitize_read_diagnostics([{**row, field: value}]))

    def test_sanitizer_requires_bounded_finite_numeric_types(self):
        base = {"provider": "binance", "path": "/api/v3/time", "elapsedMs": 1, "status": "BLOCKED"}
        for elapsed in (True, -1, float("nan"), float("inf"), "1", 10 ** 1000, None):
            self.assertEqual([], readers.sanitize_read_diagnostics([{**base, "elapsedMs": elapsed}]))
        for invalid in (True, "-1021", 0, -2147483649, 1.0):
            row = readers.sanitize_read_diagnostics([{**base, "brokerCode": invalid}])[0]
            self.assertNotIn("brokerCode", row)
        for invalid in (True, "429", 99, 600, 429.0):
            row = readers.sanitize_read_diagnostics([{**base, "httpStatus": invalid}])[0]
            self.assertNotIn("httpStatus", row)
        accepted = readers.sanitize_read_diagnostics([{**base, "brokerCode": -1021,
            "httpStatus": 400, "failureKind": "READ_REJECTED"}])[0]
        self.assertEqual(-1021, accepted["brokerCode"])
        self.assertEqual(400, accepted["httpStatus"])
        self.assertEqual([], readers.sanitize_read_diagnostics(base))
        self.assertEqual([], readers.sanitize_read_diagnostics([None, "secret", 42]))

    def test_only_latest_32_reads_are_exposed_and_next_failure_resets(self):
        r = build_readers()
        for _ in range(40):
            r._read("binance", "/api/v3/time")
        self.assertEqual(32, len(r._failure_reads()))
        self.assertEqual([], r.snapshot("UNKNOWN", ("BTCUSDT",))["readDiagnostics"])
        self.assertEqual([], r.market("UNKNOWN", "BTCUSDT")["readDiagnostics"])

    def test_market_failure_exposes_fixed_endpoint_and_preserves_generic_blocker(self):
        def fail(*_a, **_k): raise TimeoutError("private secret")
        r = build_readers(fail)
        r._binance = lambda provider, path, *a, **kw: r._read(provider, path)[0]
        result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
        self.assertFalse(result["known"])
        self.assertEqual(["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"], result["blockers"])
        self.assertEqual("/fapi/v1/exchangeInfo", result["readDiagnostics"][0]["path"])
        self.assertEqual("TIMEOUT", result["readDiagnostics"][0]["failureKind"])
        self.assertNotIn("private", json.dumps(result))

    def test_inventory_market_failure_keeps_original_blocker_instead_of_keyerror(self):
        r = build_readers()
        def get(provider, path, *args, **kwargs):
            if path == "/api/v3/account":
                return {"canTrade": True, "balances": [{"asset": "USDT", "free": "10", "locked": "0"},
                    {"asset": "SOL", "free": "1", "locked": "0"}]}
            return r._read(provider, path)[0]
        def fail(*_a, **_k): raise TimeoutError("private")
        r.reader._get = fail
        r._binance = get
        result = r.snapshot("BINANCE_SPOT", ("SOLUSDT",))
        self.assertFalse(result["known"])
        self.assertFalse(result["ordersComplete"])
        self.assertFalse(result["positionsComplete"])
        self.assertEqual(["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"], result["blockers"])
        self.assertEqual("TIMEOUT", result["readDiagnostics"][0]["failureKind"])

    def test_inventory_preserves_specific_validation_blocker(self):
        r = build_readers()
        r.market = lambda *_: {"known": False, "blockers": ["FUTURES_QUOTE_SOURCE_TIME_UNKNOWN"]}
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "^FUTURES_QUOTE_SOURCE_TIME_UNKNOWN$"):
            r._inventory_price("BINANCE_SPOT", "SOLUSDT")

    def test_transport_keeps_numeric_code_and_clears_it_before_later_timeout(self):
        fake = types.ModuleType(PKG + ".read_only_account")
        class Transport:
            def request(self, *_a, **_k): return {"code": -1021, "msg": "private payload"}, {}
        class Reader:
            def __init__(self, settings, transport): self.transport = transport
            def _get(self, provider, path, **kwargs):
                value, headers = self.transport.request(provider, "GET", path, **kwargs)
                if value.get("code", 0) < 0: raise RuntimeError("BROKER_READ_REJECTED")
                return value, headers
        fake.ReadOnlyTransport, fake.ReadOnlyBrokerReader = Transport, Reader
        fake.ORIGINS, fake.NoRedirect = {}, object
        with patch.dict(sys.modules, {fake.__name__: fake}):
            r = readers.ProductionShortLiveReaders(settings={}, fx_reader=lambda _: {})
        with self.assertRaises(RuntimeError): r._read("binance", "/api/v3/account")
        self.assertEqual(-1021, r._failure_reads()[0]["brokerCode"])
        def timeout(*_a, **_k): raise TimeoutError("private")
        r.reader._get = timeout
        with self.assertRaises(TimeoutError): r._read("binance", "/api/v3/time")
        self.assertNotIn("brokerCode", r._failure_reads()[-1])
        self.assertEqual("TIMEOUT", r._failure_reads()[-1]["failureKind"])

    def test_extra_transport_captures_code_without_retaining_signed_url(self):
        fake = types.ModuleType(PKG + ".read_only_account")
        calls = []
        class Response:
            status = 200
            headers = {"private-header": "private"}
            def __init__(self, url): self.url = url
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def geturl(self): return self.url
            def read(self, _size): return b'{"code": -2015, "msg": "private response"}'
        class Opener:
            def open(self, req, timeout):
                calls.append(req.method)
                return Response(req.full_url)
        class Transport:
            def __init__(self): self.opener, self.timeout = Opener(), 8
        class Reader:
            def __init__(self, settings, transport): self.transport = transport
            def _get(self, provider, path, **kwargs):
                value, headers = self.transport.request(provider, "GET", path, **kwargs)
                if value.get("code", 0) < 0: raise RuntimeError("BROKER_READ_REJECTED")
                return value, headers
        fake.ReadOnlyTransport, fake.ReadOnlyBrokerReader = Transport, Reader
        fake.ORIGINS, fake.NoRedirect = {"binance-futures": "https://example.invalid"}, object
        with patch.dict(sys.modules, {fake.__name__: fake}):
            r = readers.ProductionShortLiveReaders(settings={}, fx_reader=lambda _: {})
        with self.assertRaises(RuntimeError):
            r._read("binance-futures", "/fapi/v1/commissionRate", query={"signature": "private"})
        self.assertEqual(["GET"], calls)
        self.assertEqual(-2015, r._failure_reads()[0]["brokerCode"])
        self.assertNotIn("private", json.dumps(r._failure_reads()))


class QuoteReadOrderingTests(unittest.TestCase):
    def build(self, futures=False):
        r = build_readers(); calls = []; current = [NOW]
        r.clock = lambda: current[0]
        filters = [{"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01", "maxQty": "1000"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.01", "minQty": "0.01", "maxQty": "1000"}]
        def get(provider, path, params=None, signed=True):
            calls.append(path)
            if path.endswith("exchangeInfo"):
                return {"symbols": [{"symbol": "SOLUSDT", "status": "TRADING", "quoteAsset": "USDT", "filters": filters}]}
            if path.endswith("commissionRate"):
                current[0] += 11
                return {"takerCommissionRate": "0.001"}
            if path.endswith("symbolConfig"):
                return [{"symbol": "SOLUSDT", "leverage": 1, "marginType": "ISOLATED", "isAutoAddMargin": False}]
            if path.endswith("positionSide/dual"): return {"dualSidePosition": False}
            if path.endswith("leverageBracket"):
                return [{"symbol": "SOLUSDT", "brackets": [{"notionalFloor": "0", "notionalCap": "1000000",
                    "maintMarginRatio": "0.005", "cum": "0", "initialLeverage": 20}]}]
            if path.endswith("premiumIndex"):
                return {"markPrice": "100", "time": int(current[0] * 1000), "lastFundingRate": "0.0001"}
            if path.endswith("account/commission"):
                current[0] += 11
                return {k: {"taker": "0.001", "buyer": "0", "seller": "0"}
                    for k in ("standardCommission", "specialCommission", "taxCommission")}
            if path.endswith("ticker/price"): return {"symbol": "SOLUSDT", "price": "100"}
            raise AssertionError(path)
        def fx(currency):
            calls.append("FX")
            return {"known": True, "krwPerQuote": "1400", "observedAt": current[0]}
        r._binance, r.fx_reader = get, fx
        return r, calls, current

    def test_spot_metadata_completes_before_fresh_quote_and_fx(self):
        r, calls, current = self.build()
        result = r.market("BINANCE_SPOT", "SOLUSDT")
        self.assertTrue(result["known"], result)
        self.assertEqual(["/api/v3/exchangeInfo", "/api/v3/account/commission", "/api/v3/ticker/price", "FX"], calls)
        self.assertEqual(NOW + 11, mod.epoch(result["observedAt"]))
        self.assertEqual(NOW + 11, mod.epoch(result["fxObservedAt"]))

    def test_futures_metadata_completes_before_quote_and_fx_without_retry(self):
        r, calls, current = self.build(True)
        result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
        self.assertTrue(result["known"], result)
        self.assertEqual(["/fapi/v1/exchangeInfo", "/fapi/v1/commissionRate", "/fapi/v1/symbolConfig",
            "/fapi/v1/positionSide/dual", "/fapi/v1/leverageBracket", "/fapi/v1/premiumIndex", "FX"], calls)
        self.assertEqual(NOW + 11, mod.epoch(result["observedAt"]))
        self.assertEqual(NOW + 11, mod.epoch(result["sourceQuoteObservedAt"]))
        self.assertEqual(NOW + 11, mod.epoch(result["fxObservedAt"]))

    def test_final_fx_timeout_reports_its_endpoint_without_retry(self):
        r, calls, current = self.build(True)
        def fx(currency):
            calls.append("FX")
            raise TimeoutError("private body")
        r.fx_reader = fx
        result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
        self.assertFalse(result["known"])
        self.assertEqual(1, calls.count("FX"))
        self.assertEqual("upbit", result["readDiagnostics"][-1]["provider"])
        self.assertEqual("/v1/ticker", result["readDiagnostics"][-1]["path"])
        self.assertEqual("TIMEOUT", result["readDiagnostics"][-1]["failureKind"])

    def test_exact_fx_validation_codes_remain_blocked_with_the_original_reason(self):
        for code in readers._FX_VALIDATION_CODES:
            with self.subTest(code=code):
                r, calls, current = self.build(True)
                def fx(currency): raise ValueError(code)
                r.fx_reader = fx
                result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
                self.assertFalse(result["known"])
                self.assertEqual([code], result["blockers"])
                self.assertEqual("INVALID_RESPONSE", result["readDiagnostics"][-1]["failureKind"])

    def test_arbitrary_fx_valueerror_cannot_escape_as_a_blocker(self):
        r, calls, current = self.build(True)
        def fx(currency): raise ValueError("NATIVE_FX_PRIVATE_SECRET_VALUE")
        r.fx_reader = fx
        result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
        self.assertEqual(["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"], result["blockers"])
        self.assertNotIn("PRIVATE_SECRET", json.dumps(result))

    def test_unknown_fx_is_recorded_as_blocked_without_promoting_it(self):
        r, calls, current = self.build(True)
        r.fx_reader = lambda _: {"known": False}
        result = r.market("BINANCE_USDM_FUTURES", "SOLUSDT")
        self.assertEqual(["FRESH_FX_REQUIRED"], result["blockers"])
        self.assertEqual("BLOCKED", result["readDiagnostics"][-1]["status"])


if __name__ == "__main__":
    unittest.main()
