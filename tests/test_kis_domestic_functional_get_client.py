from __future__ import annotations

import inspect
import io
import json
import os
import urllib.error
import unittest
from copy import deepcopy
from unittest.mock import patch

from live_trader.kis_domestic_functional_get_client import (
    ALLOWED_KIS_DOMESTIC_FUNCTIONAL_GET_PAIRS,
    KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
    KisDomesticFunctionalBoundAccessToken,
    KisDomesticFunctionalGetBlocked,
    KisDomesticFunctionalGetClient,
    kis_domestic_functional_account_fingerprint,
)


CANO = "12345678"
PRODUCT = "91"
APP_KEY = "app-key-secret-marker-4b12"
APP_SECRET = "app-secret-marker-9c73"
TOKEN = "access-token-marker-e810"
SERVER_KEY = b"server-authority-key-marker-32bytes-minimum"
FINGERPRINT = kis_domestic_functional_account_fingerprint(CANO, PRODUCT)

BALANCE = (
    "/uapi/domestic-stock/v1/trading/inquire-balance",
    "TTTC8434R",
)
WORKING = (
    "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
    "TTTC0084R",
)
HOLIDAY = (
    "/uapi/domestic-stock/v1/quotations/chk-holiday",
    "CTCA0903R",
)
QUOTE = (
    "/uapi/domestic-stock/v1/quotations/inquire-price",
    "FHKST01010100",
)


def _query(endpoint: str) -> dict[str, str]:
    if endpoint == HOLIDAY[0]:
        return {"BASS_DT": "20260813", "CTX_AREA_FK": "", "CTX_AREA_NK": ""}
    if endpoint == QUOTE[0]:
        return {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "010140"}
    base = {"CANO": CANO, "ACNT_PRDT_CD": PRODUCT}
    if endpoint == BALANCE[0]:
        return {
            **base,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
    if endpoint == WORKING[0]:
        return {
            **base,
            "INQR_DVSN_1": "1",
            "INQR_DVSN_2": "0",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
    if endpoint.endswith("inquire-daily-ccld"):
        return {
            **base,
            "INQR_STRT_DT": "20260813",
            "INQR_END_DT": "20260813",
            "SLL_BUY_DVSN_CD": "00",
            "PDNO": "",
            "CCLD_DVSN": "00",
            "INQR_DVSN": "00",
            "INQR_DVSN_3": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_1": "",
            "EXCG_ID_DVSN_CD": "ALL",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
    common = {
        **base,
        "INQR_STRT_DT": "20260813",
        "INQR_END_DT": "20260813",
        "SORT_DVSN": "01",
        "CBLC_DVSN": "00",
        "PDNO": "010140",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    if endpoint.endswith("inquire-period-profit"):
        common["INQR_DVSN"] = "00"
    return common


def _headers(tr_id: str, continuation: str = "") -> dict[str, str]:
    return {"custtype": "P", "tr_id": tr_id, "tr_cont": continuation}


class _Transport:
    def __init__(self) -> None:
        self.token_calls = 0
        self.requests = []
        self.next_tr_cont = "D"
        self.next_body = {"rt_cd": "0", "output": []}

    def token(self) -> str:
        self.token_calls += 1
        return TOKEN

    def send(self, request):
        self.requests.append(request)
        return {
            "statusCode": 200,
            "trCont": self.next_tr_cont,
            "json": deepcopy(self.next_body),
        }


class _HttpResponse:
    def __init__(
        self,
        *,
        url: str,
        status: int,
        body: dict,
        tr_cont: str = "",
    ) -> None:
        self._url = url
        self.status = status
        self._body = json.dumps(body).encode("utf-8")
        self.headers = {"tr_cont": tr_cont}

    def geturl(self):
        return self._url

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Opener:
    def __init__(self, callback) -> None:
        self.callback = callback
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        return self.callback(request)


class KisDomesticFunctionalGetClientTest(unittest.TestCase):
    def _client(self, transport: _Transport | None = None, **overrides):
        transport = transport or _Transport()
        values = {
            "app_key": APP_KEY,
            "app_secret": APP_SECRET,
            "cano": CANO,
            "account_product_code": PRODUCT,
            "account_fingerprint": FINGERPRINT,
            "server_authority_key": SERVER_KEY,
            "token_reader": transport.token,
            "sender": transport.send,
            "allow_mock_transport": True,
            "min_request_interval_seconds": 0,
        }
        values.update(overrides)
        return KisDomesticFunctionalGetClient(**values), transport

    def _get(self, client, pair=BALANCE, *, continuation="", query=None, headers=None):
        endpoint, tr_id = pair
        return client.get(
            origin=KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            endpoint=endpoint,
            tr_id=tr_id,
            query=_query(endpoint) if query is None else query,
            continuation=continuation,
            public_headers=_headers(tr_id, continuation) if headers is None else headers,
        )

    def _preflight(self, client, pair=BALANCE, **overrides):
        endpoint, tr_id = pair
        values = {
            "method": "GET",
            "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "endpoint": endpoint,
            "tr_id": tr_id,
            "query": _query(endpoint),
            "continuation": "",
            "public_headers": _headers(tr_id),
            "body": None,
        }
        values.update(overrides)
        return client.preflight(**values)

    def test_exact_seven_get_pairs_dispatch_bodyless_and_preserve_pagination_header(self) -> None:
        expected = {
            (
                "/uapi/domestic-stock/v1/trading/inquire-balance",
                "TTTC8434R",
            ),
            (
                "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
                "TTTC0084R",
            ),
            (
                "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
                "TTTC0081R",
            ),
            (
                "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
                "TTTC8715R",
            ),
            (
                "/uapi/domestic-stock/v1/trading/inquire-period-profit",
                "TTTC8708R",
            ),
            (
                "/uapi/domestic-stock/v1/quotations/chk-holiday",
                "CTCA0903R",
            ),
            (
                "/uapi/domestic-stock/v1/quotations/inquire-price",
                "FHKST01010100",
            ),
        }
        self.assertEqual(expected, set(ALLOWED_KIS_DOMESTIC_FUNCTIONAL_GET_PAIRS))
        client, transport = self._client()
        transport.next_tr_cont = "M"
        for pair in sorted(expected):
            result = self._get(client, pair)
            self.assertEqual(
                {"statusCode": 200, "trCont": "M", "body": transport.next_body},
                result,
            )
        self.assertEqual(7, transport.token_calls)
        self.assertEqual(7, len(transport.requests))
        for request in transport.requests:
            self.assertEqual("GET", request.method)
            self.assertIsNone(request.body)
            self.assertEqual(KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN, request.origin)
            self.assertEqual("P", request.headers["custtype"])
            self.assertEqual(request.tr_id, request.headers["tr_id"])
            self.assertEqual("Bearer " + TOKEN, request.headers["authorization"])

    def test_truth_protocol_signature_is_exact_and_raw_body_is_preserved(self) -> None:
        client, transport = self._client()
        signature = inspect.signature(client.get)
        self.assertEqual(
            [
                "origin",
                "endpoint",
                "tr_id",
                "query",
                "continuation",
                "public_headers",
            ],
            list(signature.parameters),
        )
        raw_body = {
            "rt_cd": "0",
            "CANO": CANO,
            "ACNT_PRDT_CD": PRODUCT,
            "output1": [],
        }
        transport.next_body = raw_body
        result = self._get(client)
        self.assertEqual(raw_body, result["body"])
        self.assertIsNot(raw_body, result["body"])
        # The truth reader owns raw-body HMAC/redaction. Client-owned evidence is safe.
        safe = transport.requests[0].safe_snapshot()
        self.assertNotIn(CANO, json.dumps(safe, sort_keys=True))
        self.assertNotIn(CANO, repr(transport.requests[0]))

    def test_method_body_origin_and_route_are_rejected_before_token_or_sender(self) -> None:
        attempts = (
            {"method": "POST"},
            {"method": "DELETE"},
            {"body": {}},
            {"origin": "http://openapi.koreainvestment.com:9443"},
            {"origin": "https://openapivts.koreainvestment.com:29443"},
            {"origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN + "/"},
            {
                "endpoint": "/uapi/domestic-stock/v1/trading/order-cash",
                "tr_id": "TTTC0802U",
                "query": _query(BALANCE[0]),
                "public_headers": _headers("TTTC0802U"),
            },
            {"endpoint": BALANCE[0], "tr_id": "TTTC0084R"},
        )
        client, transport = self._client()
        for attempt in attempts:
            with self.subTest(attempt=attempt):
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._preflight(client, **attempt)
        with self.assertRaises(KisDomesticFunctionalGetBlocked):
            client.get(
                origin="https://attacker.invalid",
                endpoint=BALANCE[0],
                tr_id=BALANCE[1],
                query=_query(BALANCE[0]),
                continuation="",
                public_headers=_headers(BALANCE[1]),
            )
        self.assertEqual(0, transport.token_calls)
        self.assertEqual([], transport.requests)

    def test_account_fingerprint_and_every_account_query_are_exactly_bound(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticFunctionalGetBlocked,
            "kis-functional-account-fingerprint-mismatch",
        ):
            self._client(account_fingerprint="a" * 64)
        client, transport = self._client()
        bad_queries = (
            {"CANO": "87654321", "ACNT_PRDT_CD": PRODUCT},
            {"CANO": CANO, "ACNT_PRDT_CD": "01"},
            {"CANO": CANO},
            {"ACNT_PRDT_CD": PRODUCT},
        )
        for query in bad_queries:
            with self.subTest(query_keys=sorted(query)):
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._get(client, query=query)
        with self.assertRaises(KisDomesticFunctionalGetBlocked):
            self._get(
                client,
                HOLIDAY,
                query={"BASS_DT": "20260813", "CANO": CANO},
            )
        self.assertEqual(0, transport.token_calls)
        self.assertEqual([], transport.requests)

    def test_public_headers_query_grammar_and_continuation_are_exact(self) -> None:
        client, transport = self._client()
        failures = (
            {"headers": {"custtype": "B", "tr_id": BALANCE[1], "tr_cont": ""}},
            {"headers": {"custtype": "P", "tr_id": BALANCE[1], "tr_cont": "", "x": "1"}},
            {"continuation": "M", "headers": _headers(BALANCE[1], "M")},
            {"query": {**_query(BALANCE[0]), "authorization": "secret"}},
            {"query": {**_query(BALANCE[0]), "APPSECRET": "secret"}},
            {"query": {**_query(BALANCE[0]), "PDNO": 10140}},
            {"query": {**_query(BALANCE[0]), "EXTRA": "1"}},
            {"query": {**_query(BALANCE[0]), "INQR_DVSN": "01"}},
        )
        for failure in failures:
            with self.subTest(failure=failure):
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._get(client, **failure)
        self.assertEqual(0, transport.token_calls)

    def test_every_tr_query_schema_and_fixed_value_is_enforced(self) -> None:
        client, transport = self._client()
        for endpoint, tr_id in sorted(ALLOWED_KIS_DOMESTIC_FUNCTIONAL_GET_PAIRS):
            query = _query(endpoint)
            with self.subTest(tr_id=tr_id, mutation="extra"):
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._get(client, (endpoint, tr_id), query={**query, "EXTRA": "1"})
            fixed_candidates = {
                "INQR_DVSN_1": "9",
                "INQR_DVSN_2": "9",
                "PDNO": "005930",
                "FID_COND_MRKT_DIV_CODE": "Q",
                "FID_INPUT_ISCD": "005930",
                "BASS_DT": "2026-08-13",
            }
            changed = next((key for key in fixed_candidates if key in query), None)
            if changed is not None:
                with self.subTest(tr_id=tr_id, mutation=changed):
                    bad = dict(query)
                    bad[changed] = fixed_candidates[changed]
                    with self.assertRaises(KisDomesticFunctionalGetBlocked):
                        self._get(client, (endpoint, tr_id), query=bad)
        self.assertEqual(0, transport.token_calls)

    def test_working_order_query_is_exact_all_side_grouping_without_pdno(self) -> None:
        client, _ = self._client()
        query = _query(WORKING[0])
        self.assertEqual(
            {
                "CANO",
                "ACNT_PRDT_CD",
                "INQR_DVSN_1",
                "INQR_DVSN_2",
                "CTX_AREA_FK100",
                "CTX_AREA_NK100",
            },
            set(query),
        )
        self.assertEqual("1", query["INQR_DVSN_1"])
        self.assertEqual("0", query["INQR_DVSN_2"])
        self.assertNotIn("PDNO", query)
        self._get(client, WORKING)

    def test_next_page_request_and_response_continuations_are_kept_distinct(self) -> None:
        client, transport = self._client()
        transport.next_tr_cont = "D"
        next_query = _query(BALANCE[0])
        next_query["CTX_AREA_FK100"] = "FK-NEXT"
        next_query["CTX_AREA_NK100"] = "NK-NEXT"
        result = self._get(client, continuation="N", query=next_query)
        self.assertEqual("D", result["trCont"])
        request = transport.requests[0]
        self.assertEqual("N", request.continuation)
        self.assertEqual("N", request.headers["tr_cont"])
        self.assertEqual("N", request.safe_snapshot()["continuation"])
        for invalid in ("N", "X", None):
            with self.subTest(invalid=invalid):
                transport.next_tr_cont = invalid
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._get(client)

    def test_credentials_account_and_transport_exception_text_never_leak(self) -> None:
        client, transport = self._client()
        for safe_value in (
            repr(client),
            json.dumps(client.authenticated_attestation(), sort_keys=True),
            json.dumps(self._preflight(client), sort_keys=True),
        ):
            self.assertNotIn(CANO, safe_value)
            self.assertNotIn(APP_KEY, safe_value)
            self.assertNotIn(APP_SECRET, safe_value)
            self.assertNotIn(TOKEN, safe_value)

        def broken_token():
            raise RuntimeError(f"{CANO}:{APP_KEY}:{APP_SECRET}:{TOKEN}")

        broken_client, _ = self._client(token_reader=broken_token)
        with self.assertRaises(KisDomesticFunctionalGetBlocked) as raised:
            self._get(broken_client)
        error = str(raised.exception)
        for secret in (CANO, APP_KEY, APP_SECRET, TOKEN):
            self.assertNotIn(secret, error)
        self.assertEqual([], transport.requests)

        def broken_sender(_request):
            raise ValueError(f"{CANO}:{APP_KEY}:{APP_SECRET}:{TOKEN}")

        broken_client, _ = self._client(sender=broken_sender)
        with self.assertRaises(KisDomesticFunctionalGetBlocked) as raised:
            self._get(broken_client)
        error = str(raised.exception)
        for secret in (CANO, APP_KEY, APP_SECRET, TOKEN):
            self.assertNotIn(secret, error)

    def test_attestation_matches_truth_contract_without_account_or_credentials(self) -> None:
        client, _ = self._client()
        attestation = client.authenticated_attestation()
        self.assertEqual(
            {
                "schemaVersion",
                "environment",
                "origin",
                "custtype",
                "accountFingerprint",
                "credentialConfigurationHash",
                "authenticated",
                "allowedMethods",
                "signatureHash",
            },
            set(attestation),
        )
        self.assertEqual("KIS_LIVE", attestation["environment"])
        self.assertEqual(FINGERPRINT, attestation["accountFingerprint"])
        self.assertEqual(
            client.credential_configuration_hash,
            attestation["credentialConfigurationHash"],
        )
        self.assertEqual(["GET"], attestation["allowedMethods"])
        self.assertRegex(attestation["signatureHash"], r"^[0-9a-f]{64}$")
        self.assertTrue(client.verify_authenticated_attestation(attestation))
        for key, value in (
            ("authenticated", False),
            ("accountFingerprint", "a" * 64),
            ("credentialConfigurationHash", "b" * 64),
            ("signatureHash", "c" * 64),
        ):
            forged = dict(attestation)
            forged[key] = value
            self.assertFalse(client.verify_authenticated_attestation(forged), key)
        other_client, _ = self._client(server_authority_key=b"x" * 32)
        self.assertFalse(other_client.verify_authenticated_attestation(attestation))

    def test_capture_envelope_signature_is_domain_separated_and_tamper_evident(self) -> None:
        client, _ = self._client()
        envelope = {
            "endpoint": BALANCE[0],
            "queryHash": "a" * 64,
            "pageHashes": ["b" * 64],
            "cutoff": "2026-08-13T15:00:00+09:00",
            "accountFingerprint": FINGERPRINT,
            "tradingDate": "20260813",
        }
        signature = client.sign_capture_envelope(envelope)
        self.assertTrue(client.verify_capture_envelope(envelope, signature))
        forged = deepcopy(envelope)
        forged["cutoff"] = "2026-08-13T14:59:59+09:00"
        self.assertFalse(client.verify_capture_envelope(forged, signature))
        self.assertNotEqual(
            signature,
            client.authenticated_attestation()["signatureHash"],
        )

    def test_custom_token_or_sender_is_mock_only_and_bound_token_is_checked(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticFunctionalGetBlocked,
            "kis-functional-custom-transport-production-forbidden",
        ):
            KisDomesticFunctionalGetClient(
                app_key=APP_KEY,
                app_secret=APP_SECRET,
                cano=CANO,
                account_product_code=PRODUCT,
                account_fingerprint=FINGERPRINT,
                server_authority_key=SERVER_KEY,
                token_reader=lambda: TOKEN,
            )
        transport = _Transport()
        client, _ = self._client(
            transport,
            token_reader=lambda: KisDomesticFunctionalBoundAccessToken(
                access_token=TOKEN,
                credential_configuration_hash="f" * 64,
            ),
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalGetBlocked,
            "kis-functional-token-credential-binding-mismatch",
        ):
            self._get(client)
        self.assertEqual([], transport.requests)

    def test_pacing_is_at_least_two_point_one_seconds_and_audit_counts_auth_separately(self) -> None:
        class FakeTime:
            def __init__(self):
                self.value = 100.0
                self.sleeps = []

            def now(self):
                return self.value

            def sleep(self, seconds):
                self.sleeps.append(seconds)
                self.value += seconds

        fake = FakeTime()
        transport = _Transport()
        client, _ = self._client(
            transport,
            monotonic_clock=fake.now,
            sleeper=fake.sleep,
            min_request_interval_seconds=2.1,
        )
        self._get(client, QUOTE)
        self._get(client, QUOTE)
        self.assertEqual(1, len(fake.sleeps))
        self.assertAlmostEqual(2.1, fake.sleeps[0])
        audit = client.audit_snapshot()
        signature = audit.pop("signatureHash")
        self.assertTrue(client.verify_capture_envelope(audit, signature))
        self.assertEqual(2, audit["authenticationTokenReadCount"])
        self.assertTrue(audit["oauthTokenIssuanceMayUsePost"])
        self.assertEqual(0, audit["authenticationOauthPostDispatchCount"])
        self.assertFalse(audit["authenticationOauthPostCountComplete"])
        self.assertTrue(audit["authenticationOauthPostAuthOnly"])
        self.assertEqual(2, audit["officialGetDispatchCount"])
        self.assertEqual(0, audit["physicalOfficialGetAttemptCount"])
        self.assertFalse(audit["physicalOfficialGetAttemptCountComplete"])
        self.assertEqual(0, audit["hiddenGetRetryCount"])
        self.assertEqual(0, audit["redirectFollowCount"])
        self.assertEqual(0, audit["tradingPostDeleteDispatchCount"])
        self.assertEqual(2.1, audit["minimumRequestIntervalSeconds"])
        self.assertAlmostEqual(2.1, audit["pacingWaitSeconds"])
        self.assertEqual([100.0, 102.1], [row["monotonicStartedAt"] for row in audit["dispatches"]])

    def test_nonfinite_pacing_and_clocks_fail_before_sender(self) -> None:
        for invalid in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(interval=invalid):
                with self.assertRaisesRegex(
                    KisDomesticFunctionalGetBlocked,
                    "kis-functional-request-interval-invalid",
                ):
                    self._client(min_request_interval_seconds=invalid)
        for invalid in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(clock=invalid):
                client, transport = self._client(monotonic_clock=lambda value=invalid: value)
                with self.assertRaisesRegex(
                    KisDomesticFunctionalGetBlocked,
                    "kis-functional-monotonic-clock-invalid",
                ):
                    self._get(client, QUOTE)
                self.assertEqual([], transport.requests)

    def test_authority_status_marks_ephemeral_key_not_restart_verifiable(self) -> None:
        client, _ = self._client()
        audit = client.audit_snapshot()
        self.assertEqual("", audit["serverAuthorityKeyIdHash"])
        self.assertFalse(audit["serverAuthorityRestartVerifiable"])
        durable, _ = self._client(
            server_authority_key_id="windows-dpapi:kis-functional-v1",
            server_authority_restart_verifiable=True,
        )
        durable_audit = durable.audit_snapshot()
        self.assertRegex(durable_audit["serverAuthorityKeyIdHash"], r"^[0-9a-f]{64}$")
        self.assertTrue(durable_audit["serverAuthorityRestartVerifiable"])

    def test_no_order_cancel_withdraw_or_general_request_surface_exists(self) -> None:
        client, _ = self._client()
        for name in (
            "post",
            "put",
            "patch",
            "delete",
            "request",
            "order",
            "cancel",
            "withdraw",
        ):
            self.assertFalse(hasattr(client, name), name)

    def test_environment_origin_override_is_rejected_without_token_or_network(self) -> None:
        transport = _Transport()
        environment = {
            "KIS_BASE_URL": "https://openapivts.koreainvestment.com:29443",
            "KIS_APP_KEY": APP_KEY,
            "KIS_APP_SECRET": APP_SECRET,
            "KIS_ACCOUNT_NO": CANO,
            "KIS_ACCOUNT_PRODUCT_CODE": PRODUCT,
        }
        with patch.dict(os.environ, environment, clear=False):
            with self.assertRaises(KisDomesticFunctionalGetBlocked):
                KisDomesticFunctionalGetClient.from_environment(
                    expected_account_fingerprint=FINGERPRINT,
                    server_authority_key=SERVER_KEY,
                )
        self.assertEqual(0, transport.token_calls)
        self.assertEqual([], transport.requests)

    def test_production_factory_uses_one_environment_snapshot_and_mocked_auth_only_token(self) -> None:
        environment = {
            "KIS_BASE_URL": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "KIS_APP_KEY": APP_KEY,
            "KIS_APP_SECRET": APP_SECRET,
            "KIS_ACCOUNT_NO": CANO,
            "KIS_ACCOUNT_PRODUCT_CODE": PRODUCT,
        }
        sent = []

        def fake_http(method, url, *, body, headers, timeout_seconds):
            sent.append((method, url, body, dict(headers), timeout_seconds))
            if method == "POST":
                return {
                    "statusCode": 200,
                    "json": {"access_token": TOKEN, "expires_in": 86400},
                    "effectiveUrlExact": True,
                    "redirectFollowed": False,
                    "physicalAttemptCount": 1,
                }
            return {
                "statusCode": 200,
                "trCont": "",
                "json": {"rt_cd": "0", "output": {}},
                "effectiveUrlExact": True,
                "redirectFollowed": False,
                "physicalAttemptCount": 1,
            }

        with patch.dict(os.environ, environment, clear=False), patch(
            "live_trader.kis_domestic_functional_get_client._owned_no_redirect_json_request",
            side_effect=fake_http,
        ):
            client = KisDomesticFunctionalGetClient.from_environment(
                expected_account_fingerprint=FINGERPRINT,
                server_authority_key=SERVER_KEY,
                server_authority_key_id="dpapi:test",
                server_authority_restart_verifiable=True,
            )
            result = self._get(client, QUOTE)
        self.assertEqual(200, result["statusCode"])
        self.assertEqual(2, len(sent))
        self.assertEqual("POST", sent[0][0])
        self.assertEqual(
            KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN + "/oauth2/tokenP",
            sent[0][1],
        )
        self.assertEqual("client_credentials", sent[0][2]["grant_type"])
        self.assertEqual("GET", sent[1][0])
        self.assertIsNone(sent[1][2])
        self.assertNotIn(CANO, repr(client))
        audit = client.audit_snapshot()
        self.assertEqual(1, audit["authenticationTokenReadCount"])
        self.assertEqual(1, audit["authenticationOauthPostDispatchCount"])
        self.assertTrue(audit["authenticationOauthPostCountComplete"])
        self.assertTrue(audit["authenticationOauthPostAuthOnly"])
        self.assertEqual(1, audit["officialGetDispatchCount"])
        self.assertEqual(1, audit["physicalOfficialGetAttemptCount"])
        self.assertTrue(audit["physicalOfficialGetAttemptCountComplete"])
        self.assertEqual(0, audit["hiddenGetRetryCount"])
        self.assertEqual(0, audit["redirectFollowCount"])
        self.assertEqual(0, audit["tradingPostDeleteDispatchCount"])

    def test_production_auth_post_failure_is_counted_and_never_leaks(self) -> None:
        environment = {
            "KIS_BASE_URL": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "KIS_APP_KEY": APP_KEY,
            "KIS_APP_SECRET": APP_SECRET,
            "KIS_ACCOUNT_NO": CANO,
            "KIS_ACCOUNT_PRODUCT_CODE": PRODUCT,
        }

        def broken_http(method, url, *, body, headers, timeout_seconds):
            self.assertEqual("POST", method)
            self.assertEqual(
                KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN + "/oauth2/tokenP",
                url,
            )
            raise RuntimeError(
                f"{url}?cano={CANO}&appkey={APP_KEY}&secret={APP_SECRET}&token={TOKEN}"
            )

        with patch.dict(os.environ, environment, clear=False), patch(
            "live_trader.kis_domestic_functional_get_client._owned_no_redirect_json_request",
            side_effect=broken_http,
        ):
            client = KisDomesticFunctionalGetClient.from_environment(
                expected_account_fingerprint=FINGERPRINT,
                server_authority_key=SERVER_KEY,
                server_authority_key_id="dpapi:test",
                server_authority_restart_verifiable=True,
            )
            with self.assertRaises(KisDomesticFunctionalGetBlocked) as raised:
                self._get(client, QUOTE)
        error = str(raised.exception)
        for secret in (CANO, APP_KEY, APP_SECRET, TOKEN):
            self.assertNotIn(secret, error)
        self.assertNotIn("?", error)
        audit = client.audit_snapshot()
        self.assertEqual(1, audit["authenticationOauthPostDispatchCount"])
        self.assertTrue(audit["authenticationOauthPostCountComplete"])
        self.assertEqual(0, audit["officialGetDispatchCount"])
        self.assertEqual(0, audit["tradingPostDeleteDispatchCount"])

    def test_production_auth_post_budget_blocks_short_ttl_refresh_before_second_socket(self) -> None:
        environment = {
            "KIS_BASE_URL": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "KIS_APP_KEY": APP_KEY,
            "KIS_APP_SECRET": APP_SECRET,
            "KIS_ACCOUNT_NO": CANO,
            "KIS_ACCOUNT_PRODUCT_CODE": PRODUCT,
        }
        now = [0.0]
        calls = []

        def fake_http(method, url, *, body, headers, timeout_seconds):
            calls.append((method, url))
            if method == "POST":
                return {
                    "statusCode": 200,
                    "json": {"access_token": TOKEN, "expires_in": 61},
                    "effectiveUrlExact": True,
                    "redirectFollowed": False,
                    "physicalAttemptCount": 1,
                }
            return {
                "statusCode": 200,
                "trCont": "",
                "json": {"rt_cd": "0", "output": {}},
                "effectiveUrlExact": True,
                "redirectFollowed": False,
                "physicalAttemptCount": 1,
            }

        with patch.dict(os.environ, environment, clear=False), patch(
            "live_trader.kis_domestic_functional_get_client.time.monotonic",
            side_effect=lambda: now[0],
        ), patch(
            "live_trader.kis_domestic_functional_get_client._owned_no_redirect_json_request",
            side_effect=fake_http,
        ):
            client = KisDomesticFunctionalGetClient.from_environment(
                expected_account_fingerprint=FINGERPRINT,
                server_authority_key=SERVER_KEY,
                server_authority_key_id="dpapi:test",
                server_authority_restart_verifiable=True,
            )
            self._get(client, QUOTE)
            now[0] = 2.0
            with self.assertRaisesRegex(
                KisDomesticFunctionalGetBlocked,
                "oauth-post-one-shot-exhausted",
            ):
                self._get(client, QUOTE)

        self.assertEqual(1, sum(method == "POST" for method, _url in calls))
        self.assertEqual(1, sum(method == "GET" for method, _url in calls))
        audit = client.audit_snapshot()
        self.assertEqual(1, audit["authenticationOauthPostDispatchCount"])
        self.assertEqual(1, audit["officialGetDispatchCount"])
        self.assertEqual(0, audit["tradingPostDeleteDispatchCount"])

    def test_owned_transport_forbids_redirects_and_never_hides_get_retries(self) -> None:
        environment = {
            "KIS_BASE_URL": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "KIS_APP_KEY": APP_KEY,
            "KIS_APP_SECRET": APP_SECRET,
            "KIS_ACCOUNT_NO": CANO,
            "KIS_ACCOUNT_PRODUCT_CODE": PRODUCT,
        }

        def run(get_callback):
            def callback(request):
                if request.get_method() == "POST":
                    return _HttpResponse(
                        url=request.full_url,
                        status=200,
                        body={"access_token": TOKEN, "expires_in": 86400},
                    )
                return get_callback(request)

            opener = _Opener(callback)
            with patch.dict(os.environ, environment, clear=False), patch(
                "urllib.request.build_opener",
                return_value=opener,
            ) as build_opener:
                client = KisDomesticFunctionalGetClient.from_environment(
                    expected_account_fingerprint=FINGERPRINT,
                    server_authority_key=SERVER_KEY,
                    server_authority_key_id="dpapi:test",
                    server_authority_restart_verifiable=True,
                )
                result = None
                error = None
                try:
                    result = self._get(client, QUOTE)
                except KisDomesticFunctionalGetBlocked as exc:
                    error = exc
            self.assertEqual(2, len(opener.requests))
            self.assertEqual(2, build_opener.call_count)
            for request, _timeout in opener.requests:
                self.assertTrue(
                    request.full_url.startswith(
                        KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN + "/"
                    )
                )
            return client, result, error

        def redirect(request):
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://attacker.invalid/steal"},
                io.BytesIO(b""),
            )

        redirected, _result, error = run(redirect)
        self.assertIsNotNone(error)
        self.assertIn("redirect-forbidden", str(error))
        redirect_audit = redirected.audit_snapshot()
        self.assertEqual(1, redirect_audit["physicalOfficialGetAttemptCount"])
        self.assertEqual(0, redirect_audit["redirectFollowCount"])
        self.assertEqual(0, redirect_audit["hiddenGetRetryCount"])

        mismatched, _result, error = run(
            lambda _request: _HttpResponse(
                url="https://attacker.invalid/effective",
                status=200,
                body={"rt_cd": "0", "output": {}},
            )
        )
        self.assertIsNotNone(error)
        self.assertIn("effective-url-mismatch", str(error))
        self.assertEqual(
            1,
            mismatched.audit_snapshot()["physicalOfficialGetAttemptCount"],
        )

        limited, result, error = run(
            lambda request: _HttpResponse(
                url=request.full_url,
                status=429,
                body={"rt_cd": "1", "msg_cd": "EGW00201"},
            )
        )
        self.assertIsNone(error)
        self.assertEqual(429, result["statusCode"])
        limited_audit = limited.audit_snapshot()
        self.assertEqual(1, limited_audit["officialGetDispatchCount"])
        self.assertEqual(1, limited_audit["physicalOfficialGetAttemptCount"])
        self.assertEqual(0, limited_audit["hiddenGetRetryCount"])
        self.assertEqual("RESPONSE", limited_audit["dispatches"][0]["transportOutcome"])
        self.assertEqual(429, limited_audit["dispatches"][0]["statusCode"])

    def test_bad_response_shape_is_fail_closed_without_echoing_payload(self) -> None:
        client, transport = self._client()
        failures = (
            {"statusCode": True, "trCont": "", "json": {}},
            {"statusCode": 200, "trCont": "X", "json": {}},
            {"statusCode": 200, "trCont": "", "json": []},
            {"statusCode": 200, "json": {}},
        )
        for response in failures:
            with self.subTest(response=response):
                transport.send = lambda _request, value=response: value
                client, _ = self._client(transport, sender=transport.send)
                with self.assertRaises(KisDomesticFunctionalGetBlocked):
                    self._get(client)


if __name__ == "__main__":
    unittest.main()
