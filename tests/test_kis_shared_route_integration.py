from __future__ import annotations

import ast
import copy
import os
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from io import BytesIO
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from live_trader.brokers import (
    BrokerNotReadyError,
    LiveBrokerRouter,
    build_kis_mutation_authority_intent,
)
from live_trader.emergency_stop import (
    _reset_emergency_stop_sticky_for_tests,
    clear_emergency_stop,
    engage_emergency_stop,
)
from live_trader.kis_order_authority import (
    KisOrderAuthorityError,
    _reset_kis_order_authority_reader_for_tests,
    functional_kis_final_mutation_boundary,
    kill_ordinary_kis_cancel_boundary,
    kis_read_diagnostic_boundary,
    ordinary_kis_final_mutation_boundary,
    register_kis_order_authority_reader,
)
from live_trader.live_adapters import (
    KIS_LIVE_BASE_URL,
    _KIS_OWNED_ENDPOINTS,
    _clear_kis_access_token_cache,
    _send_kis_http_json,
    build_kis_cancel_order_request,
    build_kis_domestic_balance_request,
    build_kis_live_order_request,
    http_json,
    issue_kis_access_token,
    send_prepared_request,
)
from live_trader import state
from live_trader import kis_order_authority as kis_order_authority_module
from live_trader.kis_domestic_functional_get_client import (
    _credential_configuration_hash,
    kis_domestic_functional_account_fingerprint,
)
from live_trader.order_management import OrderIntent


class _SuccessfulKisResponse:
    def __init__(self, url: str) -> None:
        self.status = 200
        self.headers: dict[str, str] = {}
        self._url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def read(self) -> bytes:
        return (
            b'{"rt_cd":"0","output":{"ODNO":"0000012345",'
            b'"KRX_FWDG_ORD_ORGNO":"00123"}}'
        )


class _CountingKisOpener:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests: list[urllib.request.Request] = []

    def open(self, request, timeout=None):
        with self._lock:
            self.requests.append(request)
        return _SuccessfulKisResponse(request.full_url)


class KisSharedRouteIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.old_env = dict(os.environ)
        self.original_state = copy.deepcopy(state.STATE)
        self.original_kis_authority_reader = (
            kis_order_authority_module._AUTHORITY_READER
        )
        self.original_kis_kill_cancel_journal_path = (
            kis_order_authority_module._KILL_CANCEL_JOURNAL_PATH
        )
        # addCleanup runs even when setUp or the test raises.  This module
        # deliberately exercises sticky Kill/STOP state and resets the shared
        # KIS provider, so both process-wide contracts must be restored for the
        # next test module.
        self.addCleanup(self._restore_process_globals)
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-shared-route-app-key",
                "KIS_APP_SECRET": "kis-shared-route-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://openapi.koreainvestment.com:9443",
                "KIS_ENV": "real",
                "LIVE_TRADER_EMERGENCY_STOP_PATH": str(
                    Path(self.temp.name) / "emergency.json"
                ),
            }
        )
        _reset_emergency_stop_sticky_for_tests()
        _reset_kis_order_authority_reader_for_tests()
        _clear_kis_access_token_cache()
        account = kis_domestic_functional_account_fingerprint("12345678", "01")
        credential = _credential_configuration_hash(
            app_key="kis-shared-route-app-key",
            app_secret="kis-shared-route-app-secret",
            account_fingerprint=account,
        )
        self.snapshot = {
            "durableAuthorityReadable": True,
            "functionalAuthorityOpen": False,
            "functionalPhase": "IDLE",
            "functionalRevision": 0,
            "stateRevision": 1,
            "functionalSessionId": "",
            "functionalAccountFingerprint": account,
            "credentialConfigurationHash": credential,
            "functionalMutationIntent": {},
            "killOrdinaryCancelAllowed": False,
            "killOrdinaryCancelRevision": 0,
            "killOrdinaryCancelIntent": {},
            "applicationInstanceLeaseHeld": True,
            "ordinaryRoutesClosed": False,
            "ownerEpochId": "kis-router-owner-epoch-1",
            "ownerEpochHash": "e" * 64,
            "controlReservation": {},
        }
        register_kis_order_authority_reader(
            lambda: dict(self.snapshot),
            kill_cancel_journal_path=(
                Path(self.temp.name) / "kis-kill-cancel.sqlite3"
            ),
        )

    def _restore_process_globals(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        _reset_emergency_stop_sticky_for_tests()
        _clear_kis_access_token_cache()
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        os.environ.clear()
        os.environ.update(self.old_env)
        if self.original_kis_authority_reader is not None:
            register_kis_order_authority_reader(
                self.original_kis_authority_reader,
                kill_cancel_journal_path=(
                    self.original_kis_kill_cancel_journal_path
                ),
            )

    def tearDown(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        _reset_emergency_stop_sticky_for_tests()
        _clear_kis_access_token_cache()
        os.environ.clear()
        os.environ.update(self.old_env)

    @staticmethod
    def order() -> dict[str, object]:
        return {
            "broker_id": "kis",
            "symbol": "010140.KS",
            "asset": "KR-STOCK",
            "side": "BUY",
            "quantity": 1,
            "price": 80000,
            "order_type": "00",
        }

    @staticmethod
    def cancel_context() -> dict[str, object]:
        return {
            "symbol": "010140.KS",
            "asset": "KR-STOCK",
            "quantity": 1,
            "organization_no": "00123",
            "order_date": "20260814",
            "exchange": "KRX",
        }

    @staticmethod
    def state_cancel_fixture(
        suffix: str,
    ) -> tuple[dict[str, object], dict[str, object]]:
        order_date = datetime.now(
            timezone(timedelta(hours=9))
        ).strftime("%Y%m%d")
        binding = state._kis_environment_order_account_binding()
        order = {
            "order_id": "state-kis-cancel-" + suffix,
            "state": "sent",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "kis",
            "broker_order_id": "0000012345",
            "broker_order_key": f"{order_date}:00123:0000012345",
            "order_date": order_date,
            "organization_no": "00123",
            "symbol": "010140.KS",
            "asset": "KR-STOCK",
            "qty": 1,
            "idempotency_key": "",
            "cancel_request_id": "",
            "kis_order_account_binding": dict(binding),
            "broker_request": {
                "broker_id": "kis",
                "symbol": "010140.KS",
                "asset": "KR-STOCK",
                "quantity": 1,
                "exchange": "KRX",
            },
            "broker_response": {
                "json": {
                    "output": {"KRX_FWDG_ORD_ORGNO": "00123"}
                }
            },
        }
        truth = {
            "complete": True,
            "fresh": True,
            "absenceIsAuthoritative": True,
            "lastError": "",
            "ambiguousBrokerOrderIds": [],
            "accountBinding": dict(binding),
            "workingOrders": [
                {
                    "broker_order_id": "0000012345",
                    "broker_order_key": (
                        f"{order_date}:00123:0000012345"
                    ),
                    "order_date": order_date,
                    "organization_no": "00123",
                }
            ],
        }
        return order, truth

    def install_state_ordinary_reader(self) -> None:
        baseline = dict(self.snapshot)

        def read() -> dict[str, object]:
            account, credential = (
                state._kis_environment_order_authority_binding()
            )
            return {
                **baseline,
                "stateRevision": state._KIS_ROUTE_STATE_REVISION,
                "functionalRevision": state._KIS_ROUTE_STATE_REVISION,
                "functionalAccountFingerprint": account,
                "credentialConfigurationHash": credential,
                "controlReservation": dict(
                    state._KIS_CONTROL_RESERVATION
                ),
            }

        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(
            read,
            kill_cancel_journal_path=(
                Path(self.temp.name) / "state-ordinary-cancel.sqlite3"
            ),
        )

    @staticmethod
    def mock_env_settings_snapshot() -> dict[str, object]:
        from live_trader import env_settings

        fields = []
        for field in env_settings.ENV_SETTING_FIELDS:
            raw = os.getenv(field.key, "") or field.default
            fields.append(
                {
                    "key": field.key,
                    "kind": field.kind,
                    "value": "" if field.kind == "secret" else raw,
                    "configured": bool(raw),
                    "default": field.default,
                }
            )
        return {"fields": fields}

    def test_live_adapter_direct_domestic_mutation_bypass_is_rejected(self) -> None:
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        with patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "inherited final mutation lease"
            ):
                send_prepared_request(prepared)
        socket.assert_not_called()

        cancel = build_kis_cancel_order_request(
            {**self.cancel_context(), "broker_order_id": "0000012345"},
            access_token="token-exact",
        )
        with patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "inherited final mutation lease"
            ):
                send_prepared_request(cancel)
        socket.assert_not_called()

    def test_live_adapter_direct_overseas_mutations_are_also_rejected(self) -> None:
        overseas = build_kis_live_order_request(
            {
                "broker_id": "kis",
                "symbol": "AAPL",
                "asset": "US-STOCK",
                "side": "BUY",
                "quantity": 1,
                "price": 200,
            },
            access_token="token-exact",
        )
        with patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "inherited final mutation lease"
            ):
                send_prepared_request(overseas)
        socket.assert_not_called()

    def test_actual_url_path_and_provider_metadata_cannot_bypass_gate(self) -> None:
        requests = [
            build_kis_live_order_request(
                self.order(), access_token="token-exact"
            ),
            build_kis_cancel_order_request(
                {**self.cancel_context(), "broker_order_id": "0000012345"},
                access_token="token-exact",
            ),
            build_kis_live_order_request(
                {
                    "symbol": "AAPL",
                    "asset": "US-STOCK",
                    "side": "BUY",
                    "quantity": 1,
                    "price": 200,
                },
                access_token="token-exact",
            ),
            build_kis_cancel_order_request(
                {
                    "broker_order_id": "0000012345",
                    "symbol": "AAPL",
                    "asset": "US-STOCK",
                    "quantity": 1,
                    "exchange": "NASD",
                },
                access_token="token-exact",
            ),
        ]
        for original in requests:
            for changed in (
                replace(original, endpoint="/uapi/metadata-other"),
                replace(original, provider="other"),
                replace(original, url=original.url + "?redirect=true"),
                replace(
                    original,
                    url=original.url.replace(
                        "https://", "https://user:password@"
                    ),
                ),
                replace(
                    original,
                    url=original.url.replace(":9443", ":443"),
                ),
                replace(
                    original,
                    url=original.url.replace(
                        "openapi.koreainvestment.com",
                        "OPENAPI.KOREAINVESTMENT.COM",
                    ),
                ),
            ):
                with self.subTest(endpoint=original.endpoint, changed=changed):
                    with patch(
                        "live_trader.live_adapters._send_kis_http_json"
                    ) as socket:
                        with self.assertRaises(KisOrderAuthorityError):
                            send_prepared_request(changed)
                    socket.assert_not_called()

    def test_tr_id_substitution_is_rejected_for_every_trading_endpoint(self) -> None:
        requests = [
            build_kis_live_order_request(
                self.order(), access_token="token-exact"
            ),
            build_kis_cancel_order_request(
                {**self.cancel_context(), "broker_order_id": "0000012345"},
                access_token="token-exact",
            ),
            build_kis_live_order_request(
                {
                    "symbol": "AAPL", "asset": "US-STOCK", "side": "BUY",
                    "quantity": 1, "price": 200,
                },
                access_token="token-exact",
            ),
            build_kis_cancel_order_request(
                {
                    "broker_order_id": "0000012345", "symbol": "AAPL",
                    "asset": "US-STOCK", "quantity": 1, "exchange": "NASD",
                },
                access_token="token-exact",
            ),
        ]
        substitutions = ["TTTC0011U", "TTTC0012U", "TTTT1006U", "BAD"]
        for original, tr_id in zip(requests, substitutions):
            changed = replace(
                original,
                headers={**original.headers, "tr_id": tr_id},
            )
            with self.subTest(endpoint=original.endpoint, tr_id=tr_id):
                with patch(
                    "live_trader.live_adapters._send_kis_http_json"
                ) as socket:
                    with self.assertRaises(KisOrderAuthorityError):
                        send_prepared_request(changed)
                socket.assert_not_called()

    def test_low_level_and_generic_kis_trading_helpers_require_lease(self) -> None:
        requests = [
            build_kis_live_order_request(
                self.order(), access_token="token-exact"
            ),
            build_kis_cancel_order_request(
                {**self.cancel_context(), "broker_order_id": "0000012345"},
                access_token="token-exact",
            ),
            build_kis_live_order_request(
                {
                    "symbol": "AAPL", "asset": "US-STOCK", "side": "BUY",
                    "quantity": 1, "price": 200,
                },
                access_token="token-exact",
            ),
            build_kis_cancel_order_request(
                {
                    "broker_order_id": "0000012345", "symbol": "AAPL",
                    "asset": "US-STOCK", "quantity": 1, "exchange": "NASD",
                },
                access_token="token-exact",
            ),
        ]
        for prepared in requests:
            for sender in (_send_kis_http_json, http_json):
                with self.subTest(endpoint=prepared.endpoint, sender=sender.__name__):
                    with patch("live_trader.live_adapters.urllib.request.urlopen") as socket:
                        with self.assertRaisesRegex(
                            KisOrderAuthorityError, "inherited final mutation lease"
                        ):
                            sender(
                                prepared.method,
                                prepared.url,
                                body=prepared.body,
                                headers=prepared.headers,
                                timeout_seconds=1,
                            )
                    socket.assert_not_called()

        overseas_cancel = build_kis_cancel_order_request(
            {
                "broker_order_id": "0000012345",
                "symbol": "AAPL",
                "asset": "US-STOCK",
                "quantity": 1,
                "exchange": "NASD",
            },
            access_token="token-exact",
        )
        with patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "inherited final mutation lease"
            ):
                send_prepared_request(overseas_cancel)
        socket.assert_not_called()

    def test_kis_wire_method_must_be_exact_uppercase(self) -> None:
        trading = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        read = build_kis_domestic_balance_request(
            access_token="cached-live-token"
        )
        token_body = {
            "grant_type": "client_credentials",
            "appkey": os.environ["KIS_APP_KEY"],
            "appsecret": os.environ["KIS_APP_SECRET"],
        }
        cases = [
            (
                changed,
                trading.url,
                trading.body,
                trading.headers,
            )
            for changed in ("post", " POST", "POST ")
        ] + [
            (changed, read.url, read.body, read.headers)
            for changed in ("get", " GET", "GET ")
        ] + [
            (
                changed,
                KIS_LIVE_BASE_URL + "/oauth2/tokenP",
                token_body,
                {"content-type": "application/json; charset=utf-8"},
            )
            for changed in ("post", " POST", "POST ")
        ]
        with patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as kis_socket:
            for method, url, body, headers in cases:
                with self.subTest(method=method, path=url):
                    with self.assertRaisesRegex(
                        KisOrderAuthorityError, "exact uppercase wire token"
                    ):
                        http_json(
                            method,
                            url,
                            body=body,
                            headers=headers,
                            timeout_seconds=1,
                        )
        generic_socket.assert_not_called()
        kis_socket.assert_not_called()

    def test_kis_shaped_request_to_foreign_origin_is_socket_zero(self) -> None:
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        evil = replace(
            prepared,
            url="https://evil.example.invalid" + prepared.endpoint,
        )
        with patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as kis_socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "origin is not configured"
            ):
                http_json(
                    evil.method,
                    evil.url,
                    body=evil.body,
                    headers=evil.headers,
                    timeout_seconds=1,
                )
        generic_socket.assert_not_called()
        kis_socket.assert_not_called()

    def test_malformed_kis_config_blocks_kis_shape_but_not_crypto(self) -> None:
        os.environ["KIS_BASE_URL"] = "https://[malformed"
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        evil = replace(
            prepared,
            url="https://evil.example.invalid" + prepared.endpoint,
        )
        with patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as kis_socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "configured origin is invalid"
            ):
                http_json(
                    evil.method,
                    evil.url,
                    body=evil.body,
                    headers=evil.headers,
                    timeout_seconds=1,
                )
        generic_socket.assert_not_called()
        kis_socket.assert_not_called()

        with patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as kis_socket:
            for path in sorted(_KIS_OWNED_ENDPOINTS):
                with self.subTest(owned_path=path):
                    with self.assertRaisesRegex(
                        KisOrderAuthorityError,
                        "configured origin is invalid",
                    ):
                        http_json(
                            "GET",
                            "https://evil.example.invalid" + path,
                            body=None,
                            headers={
                                "authorization": "Bearer cached-live-token"
                            },
                            timeout_seconds=1,
                        )
        generic_socket.assert_not_called()
        kis_socket.assert_not_called()

        def response(request, timeout=None):
            return _SuccessfulKisResponse(request.full_url)

        crypto_open = MagicMock(side_effect=response)
        crypto_opener = MagicMock()
        crypto_opener.open = crypto_open
        with patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=crypto_opener,
        ) as opener_factory:
            upbit = http_json(
                "POST",
                "https://api.upbit.com/v1/orders",
                body={"market": "KRW-BTC", "side": "bid"},
                headers={
                    "Authorization": "Bearer upbit-token",
                    "Content-Type": "application/json",
                },
                timeout_seconds=1,
            )
            binance = http_json(
                "DELETE",
                "https://api.binance.com/api/v3/order?symbol=BTCUSDT",
                body=None,
                headers={"X-MBX-APIKEY": "binance-key"},
                timeout_seconds=1,
            )
        self.assertTrue(upbit["ok"])
        self.assertTrue(binance["ok"])
        self.assertEqual(2, opener_factory.call_count)
        self.assertEqual(2, crypto_open.call_count)

    def test_token_requires_official_live_origin_and_route_authority(self) -> None:
        os.environ["KIS_BASE_URL"] = "https://evil.example.invalid"
        with kis_read_diagnostic_boundary(), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot"
        ) as limiter, patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as token_socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "token wire tuple"
            ):
                issue_kis_access_token()
        limiter.assert_not_called()
        generic_socket.assert_not_called()
        token_socket.assert_not_called()

        os.environ["KIS_BASE_URL"] = KIS_LIVE_BASE_URL
        with patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot"
        ) as limiter:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "authenticated route authority"
            ):
                issue_kis_access_token()
        limiter.assert_not_called()

    def test_cached_token_cannot_follow_env_flip_to_foreign_get(self) -> None:
        os.environ["KIS_BASE_URL"] = "https://evil.example.invalid"
        prepared = build_kis_domestic_balance_request(
            access_token="cached-live-token"
        )
        with patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as wrapper_socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "credentialed read wire tuple"
            ):
                send_prepared_request(prepared)
        wrapper_socket.assert_not_called()

        with patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as kis_socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "credentialed read wire tuple"
            ):
                http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                )
        generic_socket.assert_not_called()
        kis_socket.assert_not_called()

    def test_credentialed_kis_get_redirect_never_follows(self) -> None:
        prepared = build_kis_domestic_balance_request(
            access_token="cached-live-token"
        )

        class RedirectingReadOpener:
            calls = 0
            errors: list[urllib.error.HTTPError] = []

            def open(self, request, timeout=None):
                type(self).calls += 1
                error = urllib.error.HTTPError(
                    request.full_url,
                    read_status,
                    "redirect",
                    {"Location": "https://evil.example.invalid/collect"},
                    BytesIO(b""),
                )
                type(self).errors.append(error)
                raise error

        for read_status in (302, 307):
            RedirectingReadOpener.calls = 0
            RedirectingReadOpener.errors = []
            with self.subTest(status=read_status), kis_read_diagnostic_boundary(), patch(
                "live_trader.live_adapters._acquire_shared_kis_rest_slot",
                return_value=0.0,
            ), patch(
                "live_trader.live_adapters.kis_request_min_interval_seconds",
                return_value=0.0,
            ), patch(
                "live_trader.live_adapters.urllib.request.build_opener",
                return_value=RedirectingReadOpener(),
            ), patch(
                "live_trader.live_adapters.urllib.request.urlopen"
            ) as default_socket:
                result = send_prepared_request(prepared)
                for error in RedirectingReadOpener.errors:
                    error.close()
            self.assertTrue(result["redirectBlocked"])
            self.assertTrue(result["outcomeAmbiguous"])
            self.assertFalse(result["retryAllowed"])
            self.assertEqual(1, RedirectingReadOpener.calls)
            default_socket.assert_not_called()

    def test_direct_kis_read_helpers_require_exact_read_only_scope(self) -> None:
        prepared = build_kis_domestic_balance_request(
            access_token="cached-live-token"
        )
        with patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as opener, patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as generic_socket, patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot"
        ) as limiter:
            for direct in (
                lambda: send_prepared_request(prepared),
                lambda: _send_kis_http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                ),
                lambda: http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                ),
            ):
                with self.subTest(direct=direct):
                    with self.assertRaisesRegex(
                        KisOrderAuthorityError, "READ_ONLY authority"
                    ):
                        direct()
        opener.assert_not_called()
        generic_socket.assert_not_called()
        limiter.assert_not_called()

    def test_one_read_scope_allows_one_exact_physical_get(self) -> None:
        prepared = build_kis_domestic_balance_request(
            access_token="cached-live-token"
        )
        opener = _CountingKisOpener()
        with kis_read_diagnostic_boundary(), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=opener,
        ):
            self.assertTrue(send_prepared_request(prepared)["ok"])
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "already consumed"
            ):
                send_prepared_request(prepared)
        self.assertEqual(1, len(opener.requests))

    def test_state_treats_redirect_and_server_error_post_as_unknown(self) -> None:
        intent = OrderIntent(
            strategy_id="kis-ambiguity-test",
            asset="KR_STOCK",
            symbol="010140.KS",
            side="BUY",
            quantity=1.0,
            reference_price=80_000.0,
            mode="SMALL_LIVE",
            reason="KIS ambiguity classification regression",
            metadata={"broker_id": "kis"},
        )
        for status_code in (307, 400, 408, 409, 425, 429, 503):
            order_id = f"kis-ambiguous-{status_code}"
            order = {
                "order_id": order_id,
                "idempotency_key": f"kis-ambiguous-key-{status_code}",
                "dry_run": False,
                "canary_scope": {},
            }
            managed = SimpleNamespace(order_id=order_id)
            oms_order = SimpleNamespace(status="ACCEPTED")
            oms = MagicMock()
            oms.orders = {order_id: oms_order}

            def transition(_order_id, target, *_args, **_kwargs):
                oms_order.status = target
                return oms_order

            def mark_unknown(_order_id, *_args, **_kwargs):
                oms_order.status = "UNKNOWN"
                return oms_order

            oms.transition.side_effect = transition
            oms.mark_unknown.side_effect = mark_unknown
            response = {
                "ok": False,
                "statusCode": status_code,
                "text": "redirect" if status_code == 307 else "server error",
                "json": {},
                "outcomeAmbiguous": True,
                "physicalAttemptCount": 1,
                "retryAllowed": False,
            }
            router = MagicMock()
            router.place_order.return_value = response
            with self.subTest(status=status_code), patch.object(
                state,
                "live_broker_dispatch_allowed",
                return_value=(True, "allowed"),
            ), patch.object(
                state,
                "functional_test_dispatch_assessment",
                return_value=(True, "allowed", {}),
            ), patch.object(
                state,
                "exact_live_canary_scope_dispatch_allowed",
                return_value=(True, "allowed", {}),
            ), patch.object(
                state,
                "operational_runtime_dispatch_allowed",
                return_value=(True, "allowed", {}),
            ), patch.object(
                state,
                "live_broker_payload",
                return_value={
                    "broker_id": "kis",
                    "symbol": "010140.KS",
                    "side": "BUY",
                    "quantity": 1,
                    "price": 80_000,
                },
            ), patch.object(
                state.PROGRAM_LEDGER,
                "checkpoint_order_dispatch",
                return_value={"created": True, "order": {}},
            ), patch.object(
                state.PROGRAM_LEDGER,
                "update_order_dispatch",
                return_value=True,
            ), patch.object(
                state,
                "recovery_state_payload",
                return_value={},
            ), patch.object(
                state.RECOVERY_JOURNAL,
                "save",
            ), patch.object(
                state,
                "kis_cross_process_dispatch_lease",
                return_value={"acquired": True},
            ), patch.object(
                state,
                "LIVE_OMS",
                oms,
            ), patch.object(
                state,
                "LiveBrokerRouter",
                return_value=router,
            ), patch.object(
                state.DECISION_TRACE_STORE,
                "append",
            ):
                ok, reason = state.dispatch_live_order_with_checkpoint(
                    order,
                    intent,
                    managed,
                    trace_id=f"trace-{status_code}",
                )
            self.assertFalse(ok)
            self.assertEqual("broker-dispatch-outcome-ambiguous", reason)
            self.assertEqual("unknown", order["state"])
            self.assertEqual("reconcile_required", order["queue_state"])
            self.assertEqual(1, order["physical_attempt_count"])
            self.assertTrue(order["broker_outcome_ambiguous"])
            self.assertEqual("UNKNOWN", order["oms_status"])
            router.place_order.assert_called_once()

    def test_router_full_edge_covers_all_four_kis_mutation_routes(self) -> None:
        opener = _CountingKisOpener()
        router = LiveBrokerRouter()
        overseas = {
            "broker_id": "kis",
            "symbol": "AAPL",
            "asset": "US-STOCK",
            "side": "BUY",
            "quantity": 1,
            "price": 200,
            "exchange": "NASD",
        }
        with patch(
            "live_trader.brokers.real_orders_enabled", return_value=True
        ), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=opener,
        ), patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as default_socket:
            self.assertTrue(router.place_order(self.order())["ok"])
            self.assertTrue(
                router.cancel_order(
                    "kis", "0000012345", **self.cancel_context()
                )["ok"]
            )
            self.assertTrue(router.place_order(overseas)["ok"])
            self.assertTrue(
                router.cancel_order(
                    "kis",
                    "0000012345",
                    symbol="AAPL",
                    asset="US-STOCK",
                    quantity=1,
                    exchange="NASD",
                )["ok"]
            )
        self.assertEqual(4, len(opener.requests))
        self.assertEqual(
            {
                "/uapi/domestic-stock/v1/trading/order-cash",
                "/uapi/domestic-stock/v1/trading/order-rvsecncl",
                "/uapi/overseas-stock/v1/trading/order",
                "/uapi/overseas-stock/v1/trading/order-rvsecncl",
            },
            {
                urllib.parse.urlsplit(request.full_url).path
                for request in opener.requests
            },
        )
        default_socket.assert_not_called()

    def test_router_place_returns_exact_nonsecret_wire_account_binding(self) -> None:
        router = LiveBrokerRouter()
        with patch(
            "live_trader.brokers.real_orders_enabled", return_value=True
        ), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value={
                "ok": True,
                "statusCode": 200,
                "json": {
                    "rt_cd": "0",
                    "output": {
                        "ODNO": "0000012345",
                        "KRX_FWDG_ORD_ORGNO": "00123",
                    },
                },
            },
        ):
            response = router.place_order(self.order())
        current = state._kis_environment_order_account_binding()
        self.assertEqual(
            {
                key: current[key]
                for key in (
                    "schemaVersion",
                    "accountCanoHash",
                    "accountProductCode",
                    "accountFingerprint",
                    "credentialConfigurationHash",
                )
            },
            response["kisOrderAccountBinding"],
        )
        self.assertNotIn("12345678", str(response))

    def test_state_ack_persists_exact_kis_binding_and_missing_or_tampered_is_unknown(self) -> None:
        intent = OrderIntent(
            strategy_id="kis-ack-binding",
            asset="KR_STOCK",
            symbol="010140.KS",
            side="BUY",
            quantity=1.0,
            reference_price=80_000.0,
            mode="SMALL_LIVE",
            reason="persist exact KIS account binding",
            metadata={"broker_id": "kis"},
        )
        current = state._kis_environment_order_account_binding()
        wire_binding = {
            key: current[key]
            for key in (
                "schemaVersion",
                "accountCanoHash",
                "accountProductCode",
                "accountFingerprint",
                "credentialConfigurationHash",
            )
        }

        for case, binding, expected_ok in (
            ("exact", wire_binding, True),
            ("missing", None, False),
            (
                "tampered",
                {**wire_binding, "accountFingerprint": "f" * 64},
                False,
            ),
        ):
            with self.subTest(case=case):
                order_id = "kis-ack-binding-" + case
                order = {
                    "order_id": order_id,
                    "idempotency_key": order_id + "-key",
                    "dry_run": False,
                    "canary_scope": {},
                }
                managed = SimpleNamespace(order_id=order_id)
                oms_order = SimpleNamespace(status="ACCEPTED")
                oms = MagicMock()
                oms.orders = {order_id: oms_order}

                def transition(_order_id, target, *_args, **_kwargs):
                    oms_order.status = target
                    return oms_order

                def acknowledge(_order_id, _broker_order_id):
                    oms_order.status = "ACKNOWLEDGED"
                    return oms_order

                def mark_unknown(_order_id, *_args, **_kwargs):
                    oms_order.status = "UNKNOWN"
                    return oms_order

                oms.transition.side_effect = transition
                oms.acknowledge.side_effect = acknowledge
                oms.mark_unknown.side_effect = mark_unknown
                response = {
                    "ok": True,
                    "statusCode": 200,
                    "json": {
                        "rt_cd": "0",
                        "output": {
                            "ODNO": "0000012345",
                            "KRX_FWDG_ORD_ORGNO": "00123",
                        },
                    },
                    "physicalAttemptCount": 1,
                }
                if binding is not None:
                    response["kisOrderAccountBinding"] = dict(binding)
                router = MagicMock()
                router.place_order.return_value = response
                with patch.object(
                    state,
                    "live_broker_dispatch_allowed",
                    return_value=(True, "allowed"),
                ), patch.object(
                    state,
                    "functional_test_dispatch_assessment",
                    return_value=(True, "allowed", {}),
                ), patch.object(
                    state,
                    "exact_live_canary_scope_dispatch_allowed",
                    return_value=(True, "allowed", {}),
                ), patch.object(
                    state,
                    "operational_runtime_dispatch_allowed",
                    return_value=(True, "allowed", {}),
                ), patch.object(
                    state,
                    "live_broker_payload",
                    return_value={
                        "broker_id": "kis",
                        "symbol": "010140.KS",
                        "side": "BUY",
                        "quantity": 1,
                        "price": 80_000,
                    },
                ), patch.object(
                    state.PROGRAM_LEDGER,
                    "checkpoint_order_dispatch",
                    return_value={"created": True, "order": {}},
                ), patch.object(
                    state.PROGRAM_LEDGER,
                    "update_order_dispatch",
                    return_value=True,
                ) as ledger_update, patch.object(
                    state, "recovery_state_payload", return_value={}
                ), patch.object(
                    state.RECOVERY_JOURNAL, "save"
                ), patch.object(
                    state,
                    "kis_cross_process_dispatch_lease",
                    return_value={"acquired": True},
                ), patch.object(
                    state, "LIVE_OMS", oms
                ), patch.object(
                    state, "LiveBrokerRouter", return_value=router
                ), patch.object(
                    state.DECISION_TRACE_STORE, "append"
                ):
                    ok, reason = state.dispatch_live_order_with_checkpoint(
                        order,
                        intent,
                        managed,
                        trace_id="trace-" + case,
                    )
                self.assertEqual(expected_ok, ok)
                if expected_ok:
                    self.assertEqual("broker-acknowledged", reason)
                    self.assertEqual(
                        current, order["kis_order_account_binding"]
                    )
                    self.assertEqual("acknowledged", order["state"])
                    self.assertEqual(
                        current,
                        ledger_update.call_args.args[0][
                            "kis_order_account_binding"
                        ],
                    )
                    oms.acknowledge.assert_called_once()
                else:
                    self.assertEqual(
                        "broker-dispatch-outcome-ambiguous", reason
                    )
                    self.assertEqual("unknown", order["state"])
                    self.assertNotIn("kis_order_account_binding", order)
                    oms.acknowledge.assert_not_called()

    def test_blocked_routes_never_issue_token_or_trade(self) -> None:
        baseline = dict(self.snapshot)
        router = LiveBrokerRouter()
        blocked_states = [
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
                "functionalRevision": 2,
                "stateRevision": 2,
                "functionalSessionId": "kis-functional-block-token-1",
                "ordinaryRoutesClosed": True,
            },
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ARMED_WAIT_PUBLIC",
                "functionalRevision": 3,
                "stateRevision": 3,
                "ordinaryRoutesClosed": True,
                "controlReservation": {
                    "reservationId": "kis-control-settings-token-1",
                    "reservationKind": "SETTINGS",
                    "reservationRevision": 3,
                    "stateRevision": 3,
                    "phase": "ARMED_WAIT_PUBLIC",
                    "reservationBindingHash": "c" * 64,
                },
            },
            {"applicationInstanceLeaseHeld": False},
        ]
        for index, changes in enumerate(blocked_states):
            self.snapshot.clear()
            self.snapshot.update(baseline)
            self.snapshot.update(changes)
            with self.subTest(index=index), patch(
                "live_trader.brokers.real_orders_enabled",
                return_value=True,
            ), patch(
                "live_trader.brokers.issue_kis_access_token"
            ) as token, patch(
                "live_trader.live_adapters._send_kis_http_json"
            ) as trade:
                with self.assertRaises(BrokerNotReadyError):
                    router.place_order(self.order())
            token.assert_not_called()
            trade.assert_not_called()

        self.snapshot.clear()
        self.snapshot.update(baseline)
        engage_emergency_stop("block ordinary token", source="unit-test")
        with patch(
            "live_trader.brokers.real_orders_enabled", return_value=True
        ), patch(
            "live_trader.brokers.issue_kis_access_token"
        ) as token, patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as trade:
            with self.assertRaises(BrokerNotReadyError):
                router.place_order(self.order())
        token.assert_not_called()
        trade.assert_not_called()

        prepared = build_kis_cancel_order_request(
            {
                **self.cancel_context(),
                "broker_order_id": "0000012345",
            },
            access_token="token-exact",
        )
        absent_kill_intent = build_kis_mutation_authority_intent(
            prepared,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-absent-kill-token-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        with patch(
            "live_trader.brokers.real_orders_enabled", return_value=True
        ), patch(
            "live_trader.brokers.issue_kis_access_token"
        ) as token, patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as trade:
            with self.assertRaises(BrokerNotReadyError):
                router.cancel_order(
                    "kis",
                    "0000012345",
                    **self.cancel_context(),
                    _kis_kill_authority_intent=absent_kill_intent,
                    _kis_kill_cancel_expected_revision=99,
                )
        token.assert_not_called()
        trade.assert_not_called()

    def test_one_inherited_ordinary_lease_allows_one_socket(self) -> None:
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="PLACE_ORDER",
            claim_id="kis-one-shot-ordinary-1",
        )
        opener = _CountingKisOpener()
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=intent
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=opener,
        ):
            self.assertTrue(
                _send_kis_http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                )["ok"]
            )
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "already consumed"
            ):
                _send_kis_http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                )
        self.assertEqual(1, len(opener.requests))

    def test_concurrent_same_intent_has_one_physical_socket(self) -> None:
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="PLACE_ORDER",
            claim_id="kis-one-shot-concurrent-1",
        )
        opener = _CountingKisOpener()
        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def call() -> None:
            barrier.wait(timeout=2)
            try:
                with ordinary_kis_final_mutation_boundary(
                    operation="PLACE_ORDER", intent=intent
                ):
                    _send_kis_http_json(
                        prepared.method,
                        prepared.url,
                        body=prepared.body,
                        headers=prepared.headers,
                        timeout_seconds=1,
                    )
                outcomes.append("sent")
            except KisOrderAuthorityError:
                outcomes.append("blocked")

        with patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=opener,
        ):
            threads = [threading.Thread(target=call) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive())
        self.assertCountEqual(["sent", "blocked"], outcomes)
        self.assertEqual(1, len(opener.requests))

    def test_kill_and_functional_leases_are_each_one_socket(self) -> None:
        cancel = build_kis_cancel_order_request(
            {
                **self.cancel_context(),
                "broker_order_id": "0000012345",
            },
            access_token="token-exact",
        )
        kill_intent = build_kis_mutation_authority_intent(
            cancel,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-one-shot-kill-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 12,
                "killOrdinaryCancelIntent": dict(kill_intent),
            }
        )
        engage_emergency_stop("KIS one-shot Kill", source="unit-test")
        kill_opener = _CountingKisOpener()
        with kill_ordinary_kis_cancel_boundary(
            intent=kill_intent, expected_revision=12
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=kill_opener,
        ):
            self.assertTrue(
                _send_kis_http_json(
                    cancel.method,
                    cancel.url,
                    body=cancel.body,
                    headers=cancel.headers,
                    timeout_seconds=1,
                )["ok"]
            )
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "already consumed"
            ):
                _send_kis_http_json(
                    cancel.method,
                    cancel.url,
                    body=cancel.body,
                    headers=cancel.headers,
                    timeout_seconds=1,
                )
        self.assertEqual(1, len(kill_opener.requests))

        self.assertTrue(
            clear_emergency_stop(
                confirmed=True,
                reason="unit-test functional one-shot",
                source="unit-test",
            )["ok"]
        )
        entry = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        functional_intent = build_kis_mutation_authority_intent(
            entry,
            operation="NATURAL_BUY",
            claim_id="kis-one-shot-functional-1",
        )
        self.snapshot.update(
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
                "functionalRevision": 13,
                "stateRevision": 13,
                "functionalSessionId": "kis-functional-one-shot-1",
                "functionalMutationIntent": dict(functional_intent),
                "ordinaryRoutesClosed": True,
                "killOrdinaryCancelAllowed": False,
                "killOrdinaryCancelRevision": 0,
                "killOrdinaryCancelIntent": {},
            }
        )
        functional_opener = _CountingKisOpener()
        with functional_kis_final_mutation_boundary(
            operation="NATURAL_BUY",
            session_id="kis-functional-one-shot-1",
            cleanup_only=False,
            expected_revision=13,
            intent=functional_intent,
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.kis_request_min_interval_seconds",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=functional_opener,
        ):
            self.assertTrue(
                _send_kis_http_json(
                    entry.method,
                    entry.url,
                    body=entry.body,
                    headers=entry.headers,
                    timeout_seconds=1,
                )["ok"]
            )
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "already consumed"
            ):
                _send_kis_http_json(
                    entry.method,
                    entry.url,
                    body=entry.body,
                    headers=entry.headers,
                    timeout_seconds=1,
                )
        self.assertEqual(1, len(functional_opener.requests))

    def test_router_place_and_cancel_hold_inherited_exact_lease_to_socket(self) -> None:
        response = {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}
        router = LiveBrokerRouter()
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value=response,
        ) as socket:
            place = router.place_order(self.order())
            self.assertTrue(place["ok"])
            self.assertEqual(response["json"], place["json"])
            binding = place["kisOrderAccountBinding"]
            current = state._kis_environment_order_account_binding()
            self.assertEqual(
                {
                    key: current[key]
                    for key in (
                        "schemaVersion",
                        "accountCanoHash",
                        "accountProductCode",
                        "accountFingerprint",
                        "credentialConfigurationHash",
                    )
                },
                binding,
            )
            self.assertNotIn("12345678", str(binding))
            self.assertEqual(
                response,
                router.cancel_order(
                    "kis", "0000012345", **self.cancel_context()
                ),
            )
        self.assertEqual(2, socket.call_count)
        self.assertEqual("POST", socket.call_args_list[0].args[0])
        self.assertEqual("POST", socket.call_args_list[1].args[0])

    def test_router_overseas_place_and_cancel_use_ordinary_shared_route(self) -> None:
        response = {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}
        order = {
            "broker_id": "kis",
            "symbol": "AAPL",
            "asset": "US-STOCK",
            "side": "BUY",
            "quantity": 1,
            "price": 200,
            "exchange": "NASD",
        }
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value=response,
        ) as socket:
            place = LiveBrokerRouter().place_order(order)
            self.assertTrue(place["ok"])
            self.assertIn("kisOrderAccountBinding", place)
            self.assertEqual(
                response,
                LiveBrokerRouter().cancel_order(
                    "kis",
                    "0000012345",
                    symbol="AAPL",
                    asset="US-STOCK",
                    quantity=1,
                    exchange="NASD",
                ),
            )
        self.assertEqual(2, socket.call_count)

    def test_router_payload_hash_tamper_is_rejected_before_socket(self) -> None:
        router = LiveBrokerRouter()
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.brokers.kis_prepared_payload_hash",
            return_value="f" * 64,
        ), patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                BrokerNotReadyError, "endpoint/payload"
            ):
                router.place_order(self.order())
        socket.assert_not_called()

    def test_router_rejects_prepared_account_or_credential_mismatch(self) -> None:
        router = LiveBrokerRouter()
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch("live_trader.live_adapters._send_kis_http_json") as socket:
            os.environ["KIS_ACCOUNT_NO"] = "87654321-01"
            with self.assertRaisesRegex(
                BrokerNotReadyError, "account/credential"
            ):
                router.place_order(self.order())
        socket.assert_not_called()

    def test_final_adapter_recomputes_mutable_headers_before_socket(self) -> None:
        prepared = build_kis_live_order_request(
            self.order(), access_token="token-exact"
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="PLACE_ORDER",
            claim_id="kis-final-header-binding-1",
        )
        with ordinary_kis_final_mutation_boundary(
            operation="PLACE_ORDER", intent=intent
        ):
            prepared.headers["appkey"] = "changed-after-seal"
            with patch("live_trader.live_adapters._send_kis_http_json") as socket:
                with self.assertRaisesRegex(
                    KisOrderAuthorityError, "credential/origin binding"
                ):
                    send_prepared_request(prepared)
        socket.assert_not_called()

    def test_authorized_trading_redirect_is_one_ambiguous_attempt_never_followed(self) -> None:
        class RedirectingOpener:
            calls = 0
            last_error = None

            def open(self, request, timeout=None):
                type(self).calls += 1
                error = urllib.error.HTTPError(
                    request.full_url,
                    status,
                    "redirect",
                    {"Location": "https://attacker.invalid/order"},
                    BytesIO(b""),
                )
                type(self).last_error = error
                raise error

        for status in (301, 302, 303, 307, 308):
            prepared = build_kis_live_order_request(
                self.order(), access_token="token-exact"
            )
            intent = build_kis_mutation_authority_intent(
                prepared,
                operation="PLACE_ORDER",
                claim_id=f"kis-no-redirect-{status}",
            )
            RedirectingOpener.calls = 0
            RedirectingOpener.last_error = None
            with self.subTest(status=status), ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=intent
            ), patch(
                "live_trader.live_adapters.urllib.request.build_opener",
                return_value=RedirectingOpener(),
            ), patch(
                "live_trader.live_adapters.urllib.request.urlopen"
            ) as default_socket, patch(
                "live_trader.live_adapters._acquire_shared_kis_rest_slot",
                return_value=0.0,
            ), patch(
                "live_trader.live_adapters.kis_request_min_interval_seconds",
                return_value=0.0,
            ):
                result = _send_kis_http_json(
                    prepared.method,
                    prepared.url,
                    body=prepared.body,
                    headers=prepared.headers,
                    timeout_seconds=1,
                )
                if RedirectingOpener.last_error is not None:
                    RedirectingOpener.last_error.close()
            self.assertTrue(result["redirectBlocked"])
            self.assertTrue(result["outcomeAmbiguous"])
            self.assertEqual(1, result["physicalAttemptCount"])
            self.assertEqual(1, RedirectingOpener.calls)
            default_socket.assert_not_called()

    def test_nested_body_or_header_mutation_changes_wire_hash_before_socket(self) -> None:
        for mutation in ("nested-body", "header"):
            prepared = build_kis_live_order_request(
                self.order(), access_token="token-exact"
            )
            intent = build_kis_mutation_authority_intent(
                prepared,
                operation="PLACE_ORDER",
                claim_id="kis-late-wire-" + mutation,
            )
            with ordinary_kis_final_mutation_boundary(
                operation="PLACE_ORDER", intent=intent
            ):
                if mutation == "nested-body":
                    prepared.body["LATE"] = {"nested": ["changed"]}
                else:
                    prepared.headers["tr_id"] = "TTTC0011U"
                with patch("live_trader.live_adapters.urllib.request.urlopen") as socket:
                    with self.assertRaises(KisOrderAuthorityError):
                        http_json(
                            prepared.method,
                            prepared.url,
                            body=prepared.body,
                            headers=prepared.headers,
                            timeout_seconds=1,
                        )
                socket.assert_not_called()

    def test_operation_endpoint_and_functional_side_substitution_is_socket_zero(self) -> None:
        cancel = build_kis_cancel_order_request(
            {
                **self.cancel_context(),
                "broker_order_id": "0000012345",
            },
            access_token="token-exact",
        )
        wrong_operation = build_kis_mutation_authority_intent(
            cancel,
            operation="PLACE_ORDER",
            claim_id="kis-wrong-operation-endpoint-1",
        )
        with patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "operation/endpoint"
            ):
                with ordinary_kis_final_mutation_boundary(
                    operation="PLACE_ORDER", intent=wrong_operation
                ):
                    self.fail("wrong operation must not acquire a lease")
        socket.assert_not_called()

        sell = build_kis_live_order_request(
            {**self.order(), "side": "SELL"},
            access_token="token-exact",
        )
        natural_buy = build_kis_mutation_authority_intent(
            sell,
            operation="NATURAL_BUY",
            claim_id="kis-functional-side-substitution-1",
        )
        self.snapshot.update(
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
                "functionalRevision": 31,
                "stateRevision": 31,
                "functionalSessionId": "kis-side-session-1",
                "functionalMutationIntent": dict(natural_buy),
                "ordinaryRoutesClosed": True,
            }
        )
        with functional_kis_final_mutation_boundary(
            operation="NATURAL_BUY",
            session_id="kis-side-session-1",
            cleanup_only=False,
            expected_revision=31,
            intent=natural_buy,
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "operation/endpoint/TR/side"
            ):
                _send_kis_http_json(
                    sell.method,
                    sell.url,
                    body=sell.body,
                    headers=sell.headers,
                    timeout_seconds=1,
                )
        socket.assert_not_called()

    def test_exact_owned_cancel_tuple_is_joined_to_final_body(self) -> None:
        cancel = build_kis_cancel_order_request(
            {
                **self.cancel_context(),
                "broker_order_id": "0000012345",
            },
            access_token="token-exact",
        )
        mismatched = build_kis_mutation_authority_intent(
            cancel,
            operation="CANCEL_ORDER",
            claim_id="kis-owned-body-mismatch-ordinary-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00999",
                "orderNo": "0000099999",
            },
        )
        with ordinary_kis_final_mutation_boundary(
            operation="CANCEL_ORDER", intent=mismatched
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "exact owned order"
            ):
                _send_kis_http_json(
                    cancel.method,
                    cancel.url,
                    body=cancel.body,
                    headers=cancel.headers,
                    timeout_seconds=1,
                )
        socket.assert_not_called()

        kill_intent = build_kis_mutation_authority_intent(
            cancel,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-owned-body-mismatch-kill-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00999",
                "orderNo": "0000099999",
            },
        )
        self.snapshot.update(
            {
                "functionalAuthorityOpen": False,
                "functionalPhase": "IDLE",
                "functionalRevision": 0,
                "stateRevision": 32,
                "functionalSessionId": "",
                "functionalMutationIntent": {},
                "ordinaryRoutesClosed": False,
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 32,
                "killOrdinaryCancelIntent": dict(kill_intent),
            }
        )
        engage_emergency_stop("KIS owned body mismatch", source="unit-test")
        with kill_ordinary_kis_cancel_boundary(
            intent=kill_intent, expected_revision=32
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "exact owned order"
            ):
                _send_kis_http_json(
                    cancel.method,
                    cancel.url,
                    body=cancel.body,
                    headers=cancel.headers,
                    timeout_seconds=1,
                )
        socket.assert_not_called()

    def test_overseas_cancel_cannot_be_substituted_with_revision(self) -> None:
        cancel = build_kis_cancel_order_request(
            {
                "broker_order_id": "0000012345",
                "symbol": "AAPL",
                "asset": "US-STOCK",
                "quantity": 1,
                "exchange": "NASD",
            },
            access_token="token-exact",
        )
        revised = replace(
            cancel,
            body={
                **cancel.body,
                "RVSE_CNCL_DVSN_CD": "01",
                "OVRS_ORD_UNPR": "199.99",
            },
        )
        intent = build_kis_mutation_authority_intent(
            revised,
            operation="OVERSEAS_CANCEL_ORDER",
            claim_id="kis-overseas-revise-substitution-1",
        )
        with ordinary_kis_final_mutation_boundary(
            operation="OVERSEAS_CANCEL_ORDER", intent=intent
        ), patch(
            "live_trader.live_adapters._acquire_shared_kis_rest_slot",
            return_value=0.0,
        ), patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as socket:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "cancel-only"
            ):
                _send_kis_http_json(
                    revised.method,
                    revised.url,
                    body=revised.body,
                    headers=revised.headers,
                    timeout_seconds=1,
                )
        socket.assert_not_called()

    def test_owner_epoch_or_state_change_during_router_boundary_is_socket_zero(self) -> None:
        router = LiveBrokerRouter()
        original = build_kis_live_order_request

        def change_state(intent, *, access_token=""):
            prepared = original(intent, access_token=access_token)
            self.snapshot["stateRevision"] = 2
            self.snapshot["ownerEpochHash"] = "f" * 64
            return prepared

        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.brokers.build_kis_live_order_request",
            side_effect=change_state,
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value={
                "ok": True,
                "statusCode": 200,
                "json": {"rt_cd": "0"},
            },
        ) as socket:
            # The final boundary samples the changed revision/epoch and still
            # reaches the socket only under a lease bound to that fresh state.
            self.assertTrue(router.place_order(self.order())["ok"])
        socket.assert_called_once()

        # Change owner epoch after the boundary's initial read but before the
        # generic adapter's inherited read: the socket must remain untouched.
        from live_trader import kis_order_authority as authority

        original_require = authority.require_inherited_kis_transport_authority

        def stale_require(
            *,
            endpoint: str,
            payload_hash: str,
            account_fingerprint: str,
            credential_configuration_hash: str,
        ):
            self.snapshot["stateRevision"] = 3
            self.snapshot["ownerEpochHash"] = "d" * 64
            return original_require(
                endpoint=endpoint,
                payload_hash=payload_hash,
                account_fingerprint=account_fingerprint,
                credential_configuration_hash=(
                    credential_configuration_hash
                ),
            )

        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters.require_inherited_kis_transport_authority",
            side_effect=stale_require,
        ), patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaises(BrokerNotReadyError):
                router.place_order(self.order())
        socket.assert_not_called()

    def test_kill_cancel_requires_exact_owned_durable_intent(self) -> None:
        context = self.cancel_context()
        prepared = build_kis_cancel_order_request(
            {**context, "broker_order_id": "0000012345"},
            access_token="token-exact",
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-kill-owned-router-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 4,
                "killOrdinaryCancelIntent": dict(intent),
            }
        )
        engage_emergency_stop("KIS exact Kill cancel", source="unit-test")
        response = {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}
        router = LiveBrokerRouter()
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value=response,
        ) as socket:
            self.assertEqual(
                response,
                router.cancel_order(
                    "kis",
                    "0000012345",
                    **context,
                    _kis_kill_authority_intent=intent,
                    _kis_kill_cancel_expected_revision=4,
                ),
            )
        socket.assert_called_once()

        changed = {
            **intent,
            "ownedOrderKey": {
                **intent["ownedOrderKey"],
                "orderNo": "0000099999",
            },
        }
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                BrokerNotReadyError, "reservation changed"
            ):
                router.cancel_order(
                    "kis",
                    "0000012345",
                    **context,
                    _kis_kill_authority_intent=changed,
                    _kis_kill_cancel_expected_revision=4,
                )
        socket.assert_not_called()

    def test_same_kill_cancel_lease_is_one_use_under_two_threads(self) -> None:
        context = self.cancel_context()
        prepared = build_kis_cancel_order_request(
            {**context, "broker_order_id": "0000012345"},
            access_token="token-exact",
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-kill-concurrent-router-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 5,
                "killOrdinaryCancelIntent": dict(intent),
            }
        )
        engage_emergency_stop("KIS concurrent Kill cancel", source="unit-test")
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        response = {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}

        def call() -> None:
            barrier.wait(timeout=2)
            try:
                LiveBrokerRouter().cancel_order(
                    "kis",
                    "0000012345",
                    **context,
                    _kis_kill_authority_intent=intent,
                    _kis_kill_cancel_expected_revision=5,
                )
                outcomes.append("sent")
            except BrokerNotReadyError:
                outcomes.append("blocked")

        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value=response,
        ) as socket:
            threads = [threading.Thread(target=call) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive())
        self.assertCountEqual(["sent", "blocked"], outcomes)
        socket.assert_called_once()

    def test_kill_cancel_burn_survives_reader_process_reset(self) -> None:
        context = self.cancel_context()
        prepared = build_kis_cancel_order_request(
            {**context, "broker_order_id": "0000012345"},
            access_token="token-exact",
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="KILL_ORDINARY_CANCEL",
            claim_id="kis-kill-restart-router-1",
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": 8,
                "killOrdinaryCancelIntent": dict(intent),
            }
        )
        engage_emergency_stop("KIS restart burn", source="unit-test")
        response = {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}
        journal = Path(self.temp.name) / "kis-kill-cancel.sqlite3"
        kwargs = {
            **context,
            "_kis_kill_authority_intent": intent,
            "_kis_kill_cancel_expected_revision": 8,
        }
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json",
            return_value=response,
        ) as socket:
            self.assertEqual(
                response,
                LiveBrokerRouter().cancel_order(
                    "kis", "0000012345", **kwargs
                ),
            )
        socket.assert_called_once()

        # Simulate a fresh process: memory cache and registered reader vanish,
        # but the exact FULL-synchronous SQLite burn remains on disk.
        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(
            lambda: dict(self.snapshot),
            kill_cancel_journal_path=journal,
        )
        with patch("live_trader.brokers.real_orders_enabled", return_value=True), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch("live_trader.live_adapters._send_kis_http_json") as socket:
            with self.assertRaisesRegex(
                BrokerNotReadyError, "already consumed"
            ):
                LiveBrokerRouter().cancel_order(
                    "kis", "0000012345", **kwargs
                )
        socket.assert_not_called()
        conn = sqlite3.connect(journal)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*), MIN(LENGTH(grant_burn_key)),
                       MIN(LENGTH(entry_hash)), MIN(LENGTH(intent_hash))
                FROM kis_kill_cancel_burns
                """
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual((1, 64, 64, 64), row)

    def test_kill_journal_same_columns_without_constraints_is_socket_zero(self) -> None:
        journal = Path(self.temp.name) / "kis-kill-weak-schema.sqlite3"
        conn = sqlite3.connect(journal)
        try:
            conn.executescript(
                """
                CREATE TABLE kis_kill_cancel_burns (
                    sequence_no INTEGER,
                    grant_burn_key TEXT,
                    owned_order_burn_key TEXT,
                    owner_epoch_id TEXT,
                    owner_epoch_hash TEXT,
                    state_revision INTEGER,
                    kill_revision INTEGER,
                    intent_hash TEXT,
                    intent_json TEXT,
                    previous_entry_hash TEXT,
                    entry_hash TEXT,
                    burned_at TEXT
                );
                PRAGMA user_version = 1;
                """
            )
            conn.commit()
        finally:
            conn.close()
        self._assert_kill_schema_blocks_socket(
            journal,
            claim_id="kis-kill-weak-schema-1",
            revision=21,
            reason="schema",
        )

    def test_kill_journal_unique_replace_policy_is_socket_zero(self) -> None:
        journal = Path(self.temp.name) / "kis-kill-replace-schema.sqlite3"
        conn = sqlite3.connect(journal)
        try:
            conn.executescript(
                """
                CREATE TABLE kis_kill_cancel_burns (
                    sequence_no INTEGER PRIMARY KEY,
                    grant_burn_key TEXT NOT NULL UNIQUE ON CONFLICT REPLACE,
                    owned_order_burn_key TEXT NOT NULL UNIQUE ON CONFLICT REPLACE,
                    owner_epoch_id TEXT NOT NULL,
                    owner_epoch_hash TEXT NOT NULL,
                    state_revision INTEGER NOT NULL,
                    kill_revision INTEGER NOT NULL,
                    intent_hash TEXT NOT NULL,
                    intent_json TEXT NOT NULL,
                    previous_entry_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE ON CONFLICT REPLACE,
                    burned_at TEXT NOT NULL
                );
                PRAGMA user_version = 1;
                """
            )
            conn.commit()
        finally:
            conn.close()
        self._assert_kill_schema_blocks_socket(
            journal,
            claim_id="kis-kill-replace-schema-1",
            revision=22,
            reason="DDL",
        )

    def _assert_kill_schema_blocks_socket(
        self,
        journal: Path,
        *,
        claim_id: str,
        revision: int,
        reason: str,
    ) -> None:
        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(
            lambda: dict(self.snapshot),
            kill_cancel_journal_path=journal,
        )
        context = self.cancel_context()
        prepared = build_kis_cancel_order_request(
            {**context, "broker_order_id": "0000012345"},
            access_token="token-exact",
        )
        intent = build_kis_mutation_authority_intent(
            prepared,
            operation="KILL_ORDINARY_CANCEL",
            claim_id=claim_id,
            owned_order_key={
                "orderDate": "20260814",
                "organizationNo": "00123",
                "orderNo": "0000012345",
            },
        )
        self.snapshot.update(
            {
                "killOrdinaryCancelAllowed": True,
                "killOrdinaryCancelRevision": revision,
                "killOrdinaryCancelIntent": dict(intent),
            }
        )
        engage_emergency_stop("KIS invalid journal", source="unit-test")
        with patch(
            "live_trader.brokers.real_orders_enabled", return_value=True
        ), patch(
            "live_trader.brokers.issue_kis_access_token",
            return_value="token-exact",
        ), patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as socket:
            with self.assertRaisesRegex(BrokerNotReadyError, reason):
                LiveBrokerRouter().cancel_order(
                    "kis",
                    "0000012345",
                    **context,
                    _kis_kill_authority_intent=intent,
                    _kis_kill_cancel_expected_revision=revision,
                )
        socket.assert_not_called()
        conn = sqlite3.connect(journal)
        try:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM kis_kill_cancel_burns"
                ).fetchone()[0],
            )
        finally:
            conn.close()

    def test_state_kill_cancel_holds_route_through_socket_and_revoke(self) -> None:
        journal = Path(self.temp.name) / "kis-kill-cancel.sqlite3"
        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(
            state._kis_order_authority_snapshot,
            kill_cancel_journal_path=journal,
        )
        old_orders = state.STATE.get("orders")
        old_route_values = (
            state._KIS_ROUTE_STATE_REVISION,
            state._KIS_KILL_CANCEL_REVISION,
            state._KIS_KILL_CANCEL_ALLOWED,
            dict(state._KIS_KILL_CANCEL_INTENT),
        )
        order_date = datetime.now(
            timezone(timedelta(hours=9))
        ).strftime("%Y%m%d")
        account_binding = state._kis_environment_order_account_binding()
        order = {
            "order_id": "state-kis-kill-cancel-1",
            "state": "sent",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "kis",
            "broker_order_id": "0000012345",
            "broker_order_key": f"{order_date}:00123:0000012345",
            "order_date": order_date,
            "organization_no": "00123",
            "symbol": "010140.KS",
            "asset": "KR-STOCK",
            "qty": 1,
            "idempotency_key": "",
            "cancel_request_id": "",
            "kis_order_account_binding": dict(account_binding),
            "broker_request": {
                "broker_id": "kis",
                "symbol": "010140.KS",
                "asset": "KR-STOCK",
                "quantity": 1,
                "exchange": "KRX",
            },
            "broker_response": {
                "json": {"output": {"KRX_FWDG_ORD_ORGNO": "00123"}}
            },
        }
        truth = {
            "complete": True,
            "fresh": True,
            "absenceIsAuthoritative": True,
            "lastError": "",
            "ambiguousBrokerOrderIds": [],
            "accountBinding": dict(account_binding),
            "workingOrders": [
                {
                    "broker_order_id": "0000012345",
                    "broker_order_key": f"{order_date}:00123:0000012345",
                    "order_date": order_date,
                    "organization_no": "00123",
                }
            ],
        }
        socket_entered = threading.Event()
        release_socket = threading.Event()
        contender_acquired = threading.Event()
        allowed_at_contender: list[bool] = []
        result: list[dict[str, object]] = []

        def socket(*_args, **_kwargs):
            socket_entered.set()
            if not release_socket.wait(2):
                raise TimeoutError("test socket release timed out")
            return {"ok": True, "statusCode": 200, "json": {"rt_cd": "0"}}

        def contend() -> None:
            from live_trader.kis_order_authority import (
                kis_route_authority_serialization,
            )

            with kis_route_authority_serialization():
                allowed_at_contender.append(
                    bool(state._KIS_KILL_CANCEL_ALLOWED)
                )
                contender_acquired.set()

        try:
            state._KIS_ROUTE_STATE_REVISION = 1
            state._KIS_KILL_CANCEL_REVISION = 0
            state._KIS_KILL_CANCEL_ALLOWED = False
            state._KIS_KILL_CANCEL_INTENT = {}
            state.STATE["orders"] = [order]
            engage_emergency_stop("state KIS Kill cancel", source="unit-test")
            with patch.object(
                state,
                "_kis_application_owner_epoch",
                return_value=("kis-state-owner-epoch-1", "e" * 64, True),
            ), patch.object(
                state, "issue_kis_access_token", return_value="token-exact"
            ), patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token-exact",
            ), patch(
                "live_trader.brokers.real_orders_enabled", return_value=True
            ), patch(
                "live_trader.live_adapters._send_kis_http_json",
                side_effect=socket,
            ) as network, patch.object(
                state, "append_audit"
            ), patch.object(
                state, "snapshot", return_value={}
            ):
                worker = threading.Thread(
                    target=lambda: result.append(
                        state.cancel_order(
                            "state-kis-kill-cancel-1",
                            _official_kis_truth=truth,
                            _kill_cleanup=True,
                        )
                    )
                )
                worker.start()
                self.assertTrue(socket_entered.wait(1))
                contender = threading.Thread(target=contend)
                contender.start()
                self.assertFalse(
                    contender_acquired.wait(0.2),
                    "KIS route escaped while the exact cancel socket was open",
                )
                release_socket.set()
                worker.join(2)
                contender.join(2)
                self.assertFalse(worker.is_alive())
                self.assertFalse(contender.is_alive())
                self.assertEqual(1, network.call_count)
            self.assertTrue(result and result[0].get("ok") is True)
            self.assertEqual([False], allowed_at_contender)
            self.assertFalse(state._KIS_KILL_CANCEL_ALLOWED)
        finally:
            state.STATE["orders"] = old_orders
            (
                state._KIS_ROUTE_STATE_REVISION,
                state._KIS_KILL_CANCEL_REVISION,
                state._KIS_KILL_CANCEL_ALLOWED,
                state._KIS_KILL_CANCEL_INTENT,
            ) = old_route_values

    def test_same_manual_kis_cancel_two_threads_has_one_physical_socket(self) -> None:
        old_orders = state.STATE.get("orders")
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        order, truth = self.state_cancel_fixture("same-two-threads")
        socket_entered = threading.Event()
        release_socket = threading.Event()
        results: list[dict[str, object]] = []

        def socket(*_args, **_kwargs):
            socket_entered.set()
            if not release_socket.wait(2):
                raise TimeoutError("manual cancel socket release timed out")
            return {
                "ok": True,
                "statusCode": 200,
                "json": {"rt_cd": "0"},
            }

        try:
            state._KIS_ROUTE_STATE_REVISION = 20
            state._KIS_CONTROL_RESERVATION = {}
            state.STATE["orders"] = [order]
            self.install_state_ordinary_reader()
            with patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token-exact",
            ), patch(
                "live_trader.brokers.real_orders_enabled", return_value=True
            ), patch(
                "live_trader.live_adapters._send_kis_http_json",
                side_effect=socket,
            ) as network, patch.object(
                state, "append_audit"
            ), patch.object(
                state, "snapshot", return_value={}
            ), patch.object(
                state, "queue_live_order_lifecycle_notification"
            ):
                workers = [
                    threading.Thread(
                        target=lambda: results.append(
                            state.cancel_order(
                                str(order["order_id"]),
                                _official_kis_truth=truth,
                            )
                        )
                    )
                    for _ in range(2)
                ]
                workers[0].start()
                self.assertTrue(socket_entered.wait(1))
                workers[1].start()
                time.sleep(0.1)
                self.assertEqual(1, network.call_count)
                release_socket.set()
                for worker in workers:
                    worker.join(2)
                    self.assertFalse(worker.is_alive())
            self.assertEqual(1, network.call_count)
            self.assertEqual(2, len(results))
            self.assertTrue(all(item.get("ok") is True for item in results))
            self.assertTrue(
                all(
                    item.get("reconciliation_required") is True
                    for item in results
                )
            )
        finally:
            state.STATE["orders"] = old_orders
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation

    def test_manual_kis_cancel_legacy_binding_is_socket_zero(self) -> None:
        old_orders = state.STATE.get("orders")
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        order, truth = self.state_cancel_fixture("legacy-account")
        order.pop("kis_order_account_binding")
        try:
            state._KIS_ROUTE_STATE_REVISION = 30
            state._KIS_CONTROL_RESERVATION = {}
            state.STATE["orders"] = [order]
            self.install_state_ordinary_reader()
            with patch(
                "live_trader.brokers.issue_kis_access_token"
            ) as token, patch(
                "live_trader.live_adapters._send_kis_http_json"
            ) as network, patch.object(
                state, "append_audit"
            ), patch.object(
                state, "snapshot", return_value={}
            ):
                result = state.cancel_order(
                    str(order["order_id"]),
                    _official_kis_truth=truth,
                )
            self.assertFalse(result["ok"])
            self.assertEqual(
                "kis-cancel-order-account-binding-required",
                result["reason"],
            )
            token.assert_not_called()
            network.assert_not_called()
        finally:
            state.STATE["orders"] = old_orders
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation

    def test_settings_first_changed_account_blocks_old_kis_cancel_socket(self) -> None:
        from live_trader import env_settings

        old_orders = state.STATE.get("orders")
        old_config_revision = state.STATE.get("config_revision")
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        old_account = os.environ["KIS_ACCOUNT_NO"]
        order, truth = self.state_cancel_fixture("settings-first")

        def save_settings(values):
            os.environ["KIS_ACCOUNT_NO"] = str(values["KIS_ACCOUNT_NO"])
            return self.mock_env_settings_snapshot()

        try:
            state._KIS_ROUTE_STATE_REVISION = 40
            state._KIS_CONTROL_RESERVATION = {}
            state.STATE["orders"] = [order]
            self.install_state_ordinary_reader()
            with patch.object(
                env_settings,
                "env_settings_snapshot",
                side_effect=self.mock_env_settings_snapshot,
            ), patch.object(
                env_settings, "save_env_settings", side_effect=save_settings
            ), patch.object(
                state, "snapshot", return_value={}
            ), patch.object(
                state, "append_audit"
            ), patch.object(
                state, "real_orders_enabled", return_value=False
            ), patch.object(
                state, "live_exposure_active", return_value=False
            ), patch.object(
                state,
                "_upbit_functional_durable_authority_open",
                return_value=False,
            ), patch.object(
                state,
                "_binance_functional_durable_authority_open",
                return_value=False,
            ):
                settings = state.save_environment_settings(
                    {"KIS_ACCOUNT_NO": "87654321-01"}
                )
                self.assertTrue(settings["ok"])
                with patch(
                    "live_trader.brokers.issue_kis_access_token"
                ) as token, patch(
                    "live_trader.live_adapters._send_kis_http_json"
                ) as network:
                    result = state.cancel_order(
                        str(order["order_id"]),
                        _official_kis_truth=truth,
                    )
            self.assertFalse(result["ok"])
            self.assertEqual(
                "kis-cancel-current-account-binding-changed",
                result["reason"],
            )
            token.assert_not_called()
            network.assert_not_called()
        finally:
            os.environ["KIS_ACCOUNT_NO"] = old_account
            state.STATE["orders"] = old_orders
            state.STATE["config_revision"] = old_config_revision
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation

    def test_kis_order_environment_revision_ignores_unrelated_config_revision(self) -> None:
        old_revision = state.STATE.get("config_revision")
        try:
            before = state._kis_environment_order_account_binding()
            state.STATE["config_revision"] = int(old_revision or 1) + 100
            after = state._kis_environment_order_account_binding()
        finally:
            state.STATE["config_revision"] = old_revision
        self.assertEqual(before, after)
        self.assertRegex(
            str(before["environmentRevision"]), r"^[0-9a-f]{64}$"
        )

    def test_cancel_first_holds_settings_until_old_account_socket_finishes(self) -> None:
        from live_trader import env_settings

        old_orders = state.STATE.get("orders")
        old_config_revision = state.STATE.get("config_revision")
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        old_account = os.environ["KIS_ACCOUNT_NO"]
        order, truth = self.state_cancel_fixture("cancel-first")
        socket_entered = threading.Event()
        release_socket = threading.Event()
        settings_write_entered = threading.Event()
        cancel_results: list[dict[str, object]] = []
        settings_results: list[dict[str, object]] = []

        def socket(*_args, **_kwargs):
            socket_entered.set()
            if not release_socket.wait(2):
                raise TimeoutError("cancel-first socket release timed out")
            return {
                "ok": True,
                "statusCode": 200,
                "json": {"rt_cd": "0"},
            }

        def save_settings(values):
            settings_write_entered.set()
            os.environ["KIS_ACCOUNT_NO"] = str(values["KIS_ACCOUNT_NO"])
            return self.mock_env_settings_snapshot()

        try:
            state._KIS_ROUTE_STATE_REVISION = 50
            state._KIS_CONTROL_RESERVATION = {}
            state.STATE["orders"] = [order]
            self.install_state_ordinary_reader()
            with patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token-exact",
            ), patch(
                "live_trader.brokers.real_orders_enabled", return_value=True
            ), patch(
                "live_trader.live_adapters._send_kis_http_json",
                side_effect=socket,
            ) as network, patch.object(
                env_settings,
                "env_settings_snapshot",
                side_effect=self.mock_env_settings_snapshot,
            ), patch.object(
                env_settings, "save_env_settings", side_effect=save_settings
            ), patch.object(
                state, "snapshot", return_value={}
            ), patch.object(
                state, "append_audit"
            ), patch.object(
                state, "queue_live_order_lifecycle_notification"
            ), patch.object(
                state, "real_orders_enabled", return_value=False
            ), patch.object(
                state, "live_exposure_active", return_value=False
            ), patch.object(
                state,
                "_upbit_functional_durable_authority_open",
                return_value=False,
            ), patch.object(
                state,
                "_binance_functional_durable_authority_open",
                return_value=False,
            ):
                cancel = threading.Thread(
                    target=lambda: cancel_results.append(
                        state.cancel_order(
                            str(order["order_id"]),
                            _official_kis_truth=truth,
                        )
                    )
                )
                settings = threading.Thread(
                    target=lambda: settings_results.append(
                        state.save_environment_settings(
                            {"KIS_ACCOUNT_NO": "87654321-01"}
                        )
                    )
                )
                cancel.start()
                self.assertTrue(socket_entered.wait(1))
                settings.start()
                self.assertFalse(
                    settings_write_entered.wait(0.2),
                    "settings crossed the cancel truth-to-socket fence",
                )
                request_body = network.call_args.kwargs["body"]
                self.assertEqual("12345678", request_body["CANO"])
                self.assertEqual("0000012345", request_body["ORGN_ODNO"])
                release_socket.set()
                cancel.join(2)
                settings.join(2)
                self.assertFalse(cancel.is_alive())
                self.assertFalse(settings.is_alive())
            self.assertEqual(1, network.call_count)
            self.assertTrue(cancel_results and cancel_results[0].get("ok"))
            self.assertTrue(
                settings_results and settings_results[0].get("ok")
            )
            self.assertTrue(settings_write_entered.is_set())
        finally:
            os.environ["KIS_ACCOUNT_NO"] = old_account
            state.STATE["orders"] = old_orders
            state.STATE["config_revision"] = old_config_revision
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation

    def test_state_controls_share_route_and_two_phase_controller_waits_outside(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        route_acquired = threading.Event()

        with patch.object(state, "_set_mode_serialized") as controller:
            controller.side_effect = lambda _mode: (
                entered.set()
                or release.wait(2)
                or {"ok": True, "reason": "mock"}
            )
            worker = threading.Thread(target=lambda: state.set_mode("MONITOR"))
            worker.start()
            self.assertTrue(entered.wait(1))

            def take_route() -> None:
                from live_trader.kis_order_authority import (
                    kis_route_authority_serialization,
                )

                with kis_route_authority_serialization():
                    route_acquired.set()

            contender = threading.Thread(target=take_route)
            contender.start()
            self.assertTrue(
                route_acquired.wait(1),
                "controller waited while KIS route remained held",
            )
            release.set()
            worker.join(2)
            contender.join(2)
            self.assertFalse(worker.is_alive())

        with patch.object(
            state, "persist_risk_setting_values"
        ), patch.object(state, "snapshot", return_value={}):
            key = "strategy_capital_limit_krw"
            old = state.STATE["risk_settings"][key]
            try:
                result = state.set_risk_setting(key, float(old))
                self.assertIn("ok", result)
            finally:
                state.STATE["risk_settings"][key] = old

        old_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        observed: list[tuple[int, str]] = []

        @state._two_phase_kis_route_control("SETTINGS")
        def nested_control() -> None:
            observed.append(
                (
                    state._KIS_ROUTE_STATE_REVISION,
                    str(
                        state._KIS_CONTROL_RESERVATION.get(
                            "reservationId"
                        )
                        or ""
                    ),
                )
            )

        @state._two_phase_kis_route_control("SETTINGS")
        def outer_control() -> None:
            observed.append(
                (
                    state._KIS_ROUTE_STATE_REVISION,
                    str(
                        state._KIS_CONTROL_RESERVATION.get(
                            "reservationId"
                        )
                        or ""
                    ),
                )
            )
            nested_control()

        try:
            state._KIS_CONTROL_RESERVATION = {}
            outer_control()
            self.assertEqual(2, len(observed))
            self.assertTrue(observed[0][1])
            self.assertEqual(observed[0], observed[1])
            self.assertEqual(old_revision + 2, state._KIS_ROUTE_STATE_REVISION)
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)

            quiet_revision = state._KIS_ROUTE_STATE_REVISION
            with patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
            ) as controller:
                self.assertFalse(
                    state.apply_watchdog_fail_closed(
                        {"critical_count": 0, "next_actions": []}
                    )
                )
            controller.assert_not_called()
            self.assertEqual(quiet_revision, state._KIS_ROUTE_STATE_REVISION)
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)
        finally:
            state._KIS_ROUTE_STATE_REVISION = old_revision
            state._KIS_CONTROL_RESERVATION = old_reservation

    def test_completed_stop_supersedes_late_start_finalizer_without_clearing_or_error(self) -> None:
        old_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        old_tombstones = set(state._KIS_CONTROL_SUPERSEDED_IDS)
        stop_results: list[str] = []

        @state._two_phase_kis_route_control("STOP")
        def stop_control() -> str:
            self.assertEqual(
                "STOP",
                state._KIS_CONTROL_RESERVATION.get("reservationKind"),
            )
            return "stopped"

        @state._two_phase_kis_route_control("START")
        def start_control() -> str:
            stopper = threading.Thread(
                target=lambda: stop_results.append(stop_control())
            )
            stopper.start()
            stopper.join(2)
            self.assertFalse(stopper.is_alive())
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)
            return "started"

        try:
            state._KIS_CONTROL_RESERVATION = {}
            state._KIS_CONTROL_SUPERSEDED_IDS.clear()
            self.assertEqual("started", start_control())
            self.assertEqual(["stopped"], stop_results)
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)
            self.assertEqual(set(), state._KIS_CONTROL_SUPERSEDED_IDS)
        finally:
            state._KIS_ROUTE_STATE_REVISION = old_revision
            state._KIS_CONTROL_RESERVATION = old_reservation
            state._KIS_CONTROL_SUPERSEDED_IDS.clear()
            state._KIS_CONTROL_SUPERSEDED_IDS.update(old_tombstones)

    def test_watchdog_control_and_paused_sender_have_one_exact_winner(self) -> None:
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        old_runtime_state = {
            "new_entries_blocked": state.STATE["new_entries_blocked"],
            "mode": state.STATE["mode"],
            "automation": copy.deepcopy(state.STATE["automation"]),
            "watchdog": copy.deepcopy(state.STATE["watchdog"]),
        }
        report = {
            "critical_count": 1,
            "next_actions": ["unit-test-critical"],
        }

        def install_state_reader() -> None:
            _reset_kis_order_authority_reader_for_tests()

            def read():
                return {
                    **self.snapshot,
                    "stateRevision": state._KIS_ROUTE_STATE_REVISION,
                    "functionalRevision": state._KIS_ROUTE_STATE_REVISION,
                    "controlReservation": dict(
                        state._KIS_CONTROL_RESERVATION
                    ),
                }

            register_kis_order_authority_reader(
                read,
                kill_cancel_journal_path=(
                    Path(self.temp.name) / "kis-control-winner.sqlite3"
                ),
            )

        try:
            state._KIS_ROUTE_STATE_REVISION = 100
            state._KIS_CONTROL_RESERVATION = {}
            install_state_reader()

            controller_entered = threading.Event()
            release_controller = threading.Event()

            def blocked_transition(_mode):
                controller_entered.set()
                if not release_controller.wait(2):
                    raise TimeoutError("watchdog controller release timed out")
                return {"ok": True, "results": {"stock": {"ok": True}}}

            watchdog_results: list[bool] = []
            with patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
                side_effect=blocked_transition,
            ), patch.object(state, "append_audit"):
                watchdog = threading.Thread(
                    target=lambda: watchdog_results.append(
                        state.apply_watchdog_fail_closed(report)
                    )
                )
                watchdog.start()
                self.assertTrue(controller_entered.wait(1))
                self.assertEqual(
                    "STOP",
                    state._KIS_CONTROL_RESERVATION.get(
                        "reservationKind"
                    ),
                )
                with patch(
                    "live_trader.brokers.real_orders_enabled",
                    return_value=True,
                ), patch(
                    "live_trader.brokers.issue_kis_access_token"
                ) as token, patch(
                    "live_trader.live_adapters._send_kis_http_json"
                ) as socket:
                    with self.assertRaises(BrokerNotReadyError):
                        LiveBrokerRouter().place_order(self.order())
                token.assert_not_called()
                socket.assert_not_called()
                release_controller.set()
                watchdog.join(2)
                self.assertFalse(watchdog.is_alive())
            self.assertEqual([True], watchdog_results)
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)

            state.STATE["new_entries_blocked"] = False
            state.STATE["mode"] = "SMALL_LIVE"
            state._KIS_ROUTE_STATE_REVISION = 200
            state._KIS_CONTROL_RESERVATION = {}
            install_state_reader()
            socket_entered = threading.Event()
            release_socket = threading.Event()
            second_controller_entered = threading.Event()
            release_second_controller = threading.Event()
            sender_results: list[dict[str, object]] = []

            def blocked_socket(*_args, **_kwargs):
                socket_entered.set()
                if not release_socket.wait(2):
                    raise TimeoutError("KIS socket release timed out")
                return {
                    "ok": True,
                    "statusCode": 200,
                    "json": {
                        "rt_cd": "0",
                        "output": {"ODNO": "0000012345"},
                    },
                }

            def second_transition(_mode):
                second_controller_entered.set()
                if not release_second_controller.wait(2):
                    raise TimeoutError("second controller release timed out")
                return {"ok": True, "results": {"stock": {"ok": True}}}

            with patch(
                "live_trader.brokers.real_orders_enabled",
                return_value=True,
            ), patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token-exact",
            ), patch(
                "live_trader.live_adapters._send_kis_http_json",
                side_effect=blocked_socket,
            ) as socket, patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
                side_effect=second_transition,
            ), patch.object(state, "append_audit"):
                sender = threading.Thread(
                    target=lambda: sender_results.append(
                        LiveBrokerRouter().place_order(self.order())
                    )
                )
                watchdog = threading.Thread(
                    target=lambda: state.apply_watchdog_fail_closed(report)
                )
                sender.start()
                self.assertTrue(socket_entered.wait(1))
                watchdog.start()
                self.assertFalse(
                    second_controller_entered.wait(0.2),
                    "watchdog crossed an already-held final KIS route",
                )
                release_socket.set()
                sender.join(2)
                self.assertFalse(sender.is_alive())
                self.assertTrue(second_controller_entered.wait(1))
                release_second_controller.set()
                watchdog.join(2)
                self.assertFalse(watchdog.is_alive())
            self.assertEqual(1, socket.call_count)
            self.assertTrue(sender_results and sender_results[0].get("ok"))
        finally:
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation
            state.STATE["new_entries_blocked"] = old_runtime_state[
                "new_entries_blocked"
            ]
            state.STATE["mode"] = old_runtime_state["mode"]
            state.STATE["automation"] = old_runtime_state["automation"]
            state.STATE["watchdog"] = old_runtime_state["watchdog"]

    def test_kill_recovery_and_set_mode_follow_safety_then_runtime_order(self) -> None:
        old_flags = {
            key: state.STATE.get(key)
            for key in (
                "kill_switch",
                "kill_switch_rearm_required",
                "new_entries_blocked",
                "mode",
            )
        }

        def restore_flags() -> None:
            for key, value in old_flags.items():
                state.STATE[key] = value

        self.addCleanup(restore_flags)
        transition_entered = threading.Event()
        release_transition = threading.Event()
        mode_entered = threading.Event()
        lock_observations: list[tuple[bool, bool]] = []
        transition_lock_observations: list[tuple[bool, bool]] = []
        recovery_results: list[dict[str, object]] = []
        mode_results: list[dict[str, object]] = []

        def transition(_mode):
            transition_lock_observations.append(
                (
                    bool(
                        getattr(
                            state.SAFETY_CONFIRMATION_MUTATION_LOCK,
                            "_is_owned",
                            lambda: False,
                        )()
                    ),
                    bool(
                        getattr(
                            state.RUNTIME_CONTROL_LOCK,
                            "_is_owned",
                            lambda: False,
                        )()
                    ),
                )
            )
            transition_entered.set()
            if not release_transition.wait(2):
                raise TimeoutError("runtime transition release timed out")
            return {"ok": True, "results": {}}

        def cancel(*_args, **_kwargs):
            runtime_owned = bool(
                getattr(state.RUNTIME_CONTROL_LOCK, "_is_owned", lambda: False)()
            )
            safety_owned = bool(
                getattr(
                    state.SAFETY_CONFIRMATION_MUTATION_LOCK,
                    "_is_owned",
                    lambda: False,
                )()
            )
            lock_observations.append((safety_owned, runtime_owned))
            return {
                "working_count": 0,
                "unresolved_count": 0,
                "cleanup_complete": True,
            }

        def set_mode_body(_mode):
            mode_entered.set()
            return {"ok": True, "reason": "mock", "snapshot": {}}

        with patch.object(
            state,
            "emergency_stop_status",
            return_value={"active": True, "revision": "kill-lock-order"},
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "transition_running",
            side_effect=transition,
        ), patch.object(
            state,
            "refresh_kis_order_truth_for_kill_switch",
            return_value={"ok": True, "truth": {}},
        ), patch.object(
            state,
            "cancel_working_orders_for_kill_switch",
            side_effect=cancel,
        ), patch.object(
            state,
            "stop_operational_runtime_sessions_for_kill",
            return_value=[],
        ), patch.object(
            state,
            "_run_reconciliation_without_public_fence",
            return_value={"ok": True},
        ), patch.object(
            state,
            "_revoke_crypto_first_live_entry_before_cleanup",
            return_value={"ok": True, "entryAuthorityRevoked": True},
        ) as global_revoke, patch.object(
            state,
            "_upbit_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ), patch.object(
            state,
            "_binance_spot_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ), patch.object(
            state,
            "_set_mode_serialized",
            side_effect=set_mode_body,
        ), patch.object(
            state,
            "sync_runtime_profile_mode",
        ):
            recovery = threading.Thread(
                target=lambda: recovery_results.append(
                    state._apply_durable_emergency_stop_recovery()
                )
            )
            mode = threading.Thread(
                target=lambda: mode_results.append(state.set_mode("MONITOR"))
            )
            recovery.start()
            self.assertTrue(transition_entered.wait(1))
            mode.start()
            self.assertFalse(
                mode_entered.wait(0.2),
                "set_mode crossed the recovery SAFETY boundary",
            )
            release_transition.set()
            recovery.join(2)
            mode.join(2)
            self.assertFalse(recovery.is_alive())
            self.assertFalse(mode.is_alive())
        self.assertEqual([(False, False)], lock_observations)
        self.assertEqual([(False, True)], transition_lock_observations)
        global_revoke.assert_called_once_with("durable-emergency-stop")
        self.assertTrue(recovery_results and recovery_results[0].get("ok"))
        self.assertTrue(mode_results and mode_results[0].get("ok"))

    def test_kill_runtime_transition_does_not_hold_safety_while_waiting_for_cycle(self) -> None:
        runtime_held = threading.Event()
        transition_waiting = threading.Event()
        let_cycle_take_safety = threading.Event()
        cycle_took_safety = threading.Event()
        release_cycle = threading.Event()
        results: list[dict[str, object]] = []

        def due_cycle() -> None:
            with state.RUNTIME_MODE_LOCK:
                runtime_held.set()
                if not let_cycle_take_safety.wait(2):
                    raise TimeoutError("cycle safety turn timed out")
                with state.SAFETY_CONFIRMATION_MUTATION_LOCK:
                    cycle_took_safety.set()
                if not release_cycle.wait(2):
                    raise TimeoutError("cycle release timed out")

        def transition(_mode):
            transition_waiting.set()
            with state.RUNTIME_MODE_LOCK:
                return {"ok": True, "results": {}}

        with patch.object(
            state,
            "emergency_stop_status",
            return_value={"active": True, "revision": "kill-cycle-order"},
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "transition_running",
            side_effect=transition,
        ), patch.object(
            state, "sync_runtime_profile_mode"
        ), patch.object(
            state,
            "refresh_kis_order_truth_for_kill_switch",
            return_value={"ok": True, "truth": {}},
        ), patch.object(
            state,
            "cancel_working_orders_for_kill_switch",
            return_value={
                "working_count": 0,
                "unresolved_count": 0,
                "cleanup_complete": True,
            },
        ), patch.object(
            state,
            "stop_operational_runtime_sessions_for_kill",
            return_value=[],
        ), patch.object(
            state,
            "_run_reconciliation_without_public_fence",
            return_value={"ok": True},
        ), patch.object(
            state,
            "_revoke_crypto_first_live_entry_before_cleanup",
            return_value={"ok": True, "entryAuthorityRevoked": True},
        ) as global_revoke, patch.object(
            state,
            "_upbit_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ), patch.object(
            state,
            "_binance_spot_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ):
            cycle = threading.Thread(target=due_cycle)
            recovery = threading.Thread(
                target=lambda: results.append(
                    state._apply_durable_emergency_stop_recovery()
                )
            )
            cycle.start()
            self.assertTrue(runtime_held.wait(1))
            recovery.start()
            self.assertTrue(transition_waiting.wait(1))
            let_cycle_take_safety.set()
            self.assertTrue(
                cycle_took_safety.wait(1),
                "Kill retained SAFETY while waiting for RUNTIME_MODE_LOCK",
            )
            release_cycle.set()
            cycle.join(2)
            recovery.join(2)
            self.assertFalse(cycle.is_alive())
            self.assertFalse(recovery.is_alive())
        global_revoke.assert_called_once_with("durable-emergency-stop")
        self.assertTrue(results and results[0].get("ok"))

    def test_kill_generation_change_during_phase_b_skips_every_cleanup_edge(self) -> None:
        transition_entered = threading.Event()
        release_transition = threading.Event()
        generation_changed = threading.Event()
        results: list[dict[str, object]] = []

        def emergency_status():
            if generation_changed.is_set():
                return {"active": False, "revision": "kill-generation-b"}
            return {"active": True, "revision": "kill-generation-a"}

        def transition(_mode):
            transition_entered.set()
            if not release_transition.wait(2):
                raise TimeoutError("Kill transition release timed out")
            return {"ok": True, "results": {}}

        with patch.object(
            state, "emergency_stop_status", side_effect=emergency_status
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "transition_running",
            side_effect=transition,
        ), patch.object(
            state, "sync_runtime_profile_mode"
        ), patch.object(
            state, "refresh_kis_order_truth_for_kill_switch"
        ) as truth, patch.object(
            state, "cancel_working_orders_for_kill_switch"
        ) as cancel, patch.object(
            state, "stop_operational_runtime_sessions_for_kill"
        ) as sessions, patch.object(
            state, "_run_reconciliation_without_public_fence"
        ) as reconcile, patch.object(
            state, "_upbit_functional_emergency_cleanup_after_latch"
        ) as upbit_cleanup, patch.object(
            state, "_binance_spot_functional_emergency_cleanup_after_latch"
        ) as binance_cleanup, patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as socket:
            worker = threading.Thread(
                target=lambda: results.append(
                    state._apply_durable_emergency_stop_recovery()
                )
            )
            worker.start()
            self.assertTrue(transition_entered.wait(1))
            generation_changed.set()
            release_transition.set()
            worker.join(2)
            self.assertFalse(worker.is_alive())
        self.assertEqual(1, len(results))
        self.assertFalse(results[0]["ok"])
        self.assertEqual(
            "emergency-stop-generation-changed", results[0]["reason"]
        )
        self.assertTrue(state.STATE["kill_switch"])
        self.assertTrue(state.STATE["kill_switch_rearm_required"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        truth.assert_not_called()
        cancel.assert_not_called()
        sessions.assert_not_called()
        reconcile.assert_not_called()
        upbit_cleanup.assert_not_called()
        binance_cleanup.assert_not_called()
        socket.assert_not_called()
        for lock in (
            state.SAFETY_CONFIRMATION_MUTATION_LOCK,
            state.RUNTIME_CONTROL_LOCK,
            state.RUNTIME_MODE_LOCK,
        ):
            self.assertTrue(lock.acquire(timeout=0.2))
            lock.release()

    def test_kill_runtime_transition_exception_is_fail_closed_data_and_cleanup_continues(self) -> None:
        cancellation = {
            "working_count": 0,
            "unresolved_count": 0,
            "cleanup_complete": True,
        }
        with patch.object(
            state,
            "emergency_stop_status",
            return_value={"active": True, "revision": "kill-runtime-error"},
        ), patch.object(
            state.LIVE_CONTINUOUS_CONTROLLER,
            "transition_running",
            side_effect=RuntimeError("controller unavailable"),
        ), patch.object(
            state, "sync_runtime_profile_mode"
        ) as sync, patch.object(
            state,
            "refresh_kis_order_truth_for_kill_switch",
            return_value={"ok": True, "truth": {}},
        ) as truth, patch.object(
            state,
            "cancel_working_orders_for_kill_switch",
            return_value=cancellation,
        ) as cancel, patch.object(
            state,
            "stop_operational_runtime_sessions_for_kill",
            return_value=[],
        ) as sessions, patch.object(
            state,
            "_run_reconciliation_without_public_fence",
            return_value={"ok": True},
        ) as reconcile, patch.object(
            state,
            "_upbit_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ) as upbit_cleanup, patch.object(
            state,
            "_binance_spot_functional_emergency_cleanup_after_latch",
            return_value={"ok": True},
        ) as binance_cleanup, patch(
            "live_trader.live_adapters._send_kis_http_json"
        ) as socket:
            result = state._apply_durable_emergency_stop_recovery()
        self.assertFalse(result["ok"])
        self.assertEqual(
            "kill-runtime-transition-error:RuntimeError",
            result["runtime"]["reason"],
        )
        self.assertTrue(state.STATE["kill_switch"])
        self.assertTrue(state.STATE["kill_switch_rearm_required"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        sync.assert_not_called()
        truth.assert_called_once_with()
        cancel.assert_called_once_with(kis_truth={})
        sessions.assert_called_once()
        reconcile.assert_called_once()
        upbit_cleanup.assert_called_once_with()
        binance_cleanup.assert_called_once_with()
        socket.assert_not_called()

    def test_kis_poll_read_publication_reservation_blocks_final_send(self) -> None:
        old_route_revision = state._KIS_ROUTE_STATE_REVISION
        old_reservation = dict(state._KIS_CONTROL_RESERVATION)
        old_poll_snapshot = copy.deepcopy(state.STATE["execution_events"])
        baseline = dict(self.snapshot)
        try:
            state._KIS_ROUTE_STATE_REVISION = 300
            state._KIS_CONTROL_RESERVATION = {}
            _reset_kis_order_authority_reader_for_tests()

            def read():
                return {
                    **baseline,
                    "stateRevision": state._KIS_ROUTE_STATE_REVISION,
                    "functionalRevision": state._KIS_ROUTE_STATE_REVISION,
                    "controlReservation": dict(
                        state._KIS_CONTROL_RESERVATION
                    ),
                }

            register_kis_order_authority_reader(
                read,
                kill_cancel_journal_path=(
                    Path(self.temp.name) / "kis-poll-fence.sqlite3"
                ),
            )
            read_entered = threading.Event()
            release_read = threading.Event()
            publication_seen = threading.Event()
            poll_results: list[dict[str, object]] = []

            def broker_read(*_args, **_kwargs):
                read_entered.set()
                if not release_read.wait(2):
                    raise TimeoutError("KIS poll read release timed out")
                return {
                    "events": [],
                    "accounts": [],
                    "positions": [],
                    "execution_truth": {
                        "complete": True,
                        "rows": [],
                        "pages": 1,
                    },
                }

            def observe_publication(*_args, **_kwargs):
                self.assertTrue(bool(state._KIS_CONTROL_RESERVATION))
                publication_seen.set()

            with patch.object(
                state.LIVE_EXECUTION_STREAMS,
                "drain",
                return_value=[],
            ), patch.object(
                state,
                "broker_snapshot_is_due",
                return_value=True,
            ), patch.object(
                state,
                "fetch_broker_snapshots",
                side_effect=broker_read,
            ), patch.object(
                state,
                "record_complete_kis_order_truth",
            ), patch.object(
                state,
                "merge_broker_reconciliation_cache",
                side_effect=observe_publication,
            ), patch.object(
                state,
                "record_connectivity_state",
            ), patch.object(
                state,
                "mark_broker_snapshot_success",
                return_value=False,
            ), patch.object(
                state.PROGRAM_LEDGER,
                "existing_execution_event_ids",
                return_value=set(),
            ), patch.object(
                state.PROGRAM_LEDGER,
                "record_execution_events",
                return_value=0,
            ), patch.object(
                state,
                "program_ledger_snapshot",
                return_value={},
            ), patch.object(
                state,
                "execution_event_snapshot",
                return_value={},
            ), patch.object(
                state,
                "kis_order_truth_snapshot",
                return_value={},
            ), patch.object(
                state,
                "notify_new_live_fills",
                return_value=0,
            ), patch.object(
                state,
                "append_audit",
            ):
                poller = threading.Thread(
                    target=lambda: poll_results.append(
                        state.poll_execution_events(
                            "kis",
                            force_snapshot=True,
                            include_snapshot=False,
                        )
                    )
                )
                poller.start()
                self.assertTrue(read_entered.wait(1))
                self.assertEqual(
                    "SETTINGS",
                    state._KIS_CONTROL_RESERVATION.get(
                        "reservationKind"
                    ),
                )
                with patch(
                    "live_trader.brokers.real_orders_enabled",
                    return_value=True,
                ), patch(
                    "live_trader.brokers.issue_kis_access_token"
                ) as token, patch(
                    "live_trader.live_adapters._send_kis_http_json"
                ) as socket:
                    with self.assertRaises(BrokerNotReadyError):
                        LiveBrokerRouter().place_order(self.order())
                token.assert_not_called()
                socket.assert_not_called()
                release_read.set()
                poller.join(2)
                self.assertFalse(poller.is_alive())
            self.assertTrue(publication_seen.is_set())
            self.assertTrue(poll_results)
            self.assertIn("ok", poll_results[0])
            self.assertEqual({}, state._KIS_CONTROL_RESERVATION)

            sender_entered = threading.Event()
            release_sender = threading.Event()
            second_read_entered = threading.Event()
            release_second_read = threading.Event()

            def blocked_socket(*_args, **_kwargs):
                sender_entered.set()
                if not release_sender.wait(2):
                    raise TimeoutError("KIS sender release timed out")
                return {
                    "ok": True,
                    "statusCode": 200,
                    "json": {
                        "rt_cd": "0",
                        "output": {"ODNO": "0000012345"},
                    },
                }

            def second_read(*_args, **_kwargs):
                second_read_entered.set()
                if not release_second_read.wait(2):
                    raise TimeoutError("second KIS read release timed out")
                return {
                    "events": [],
                    "accounts": [],
                    "positions": [],
                    "execution_truth": {
                        "complete": True,
                        "rows": [],
                        "pages": 1,
                    },
                }

            sender_results: list[dict[str, object]] = []
            with patch(
                "live_trader.brokers.real_orders_enabled",
                return_value=True,
            ), patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token-exact",
            ), patch(
                "live_trader.live_adapters._send_kis_http_json",
                side_effect=blocked_socket,
            ) as socket, patch.object(
                state.LIVE_EXECUTION_STREAMS,
                "drain",
                return_value=[],
            ), patch.object(
                state,
                "broker_snapshot_is_due",
                return_value=True,
            ), patch.object(
                state,
                "fetch_broker_snapshots",
                side_effect=second_read,
            ), patch.object(
                state,
                "record_complete_kis_order_truth",
            ), patch.object(
                state,
                "merge_broker_reconciliation_cache",
            ), patch.object(
                state,
                "record_connectivity_state",
            ), patch.object(
                state,
                "mark_broker_snapshot_success",
                return_value=False,
            ), patch.object(
                state.PROGRAM_LEDGER,
                "existing_execution_event_ids",
                return_value=set(),
            ), patch.object(
                state.PROGRAM_LEDGER,
                "record_execution_events",
                return_value=0,
            ), patch.object(
                state,
                "program_ledger_snapshot",
                return_value={},
            ), patch.object(
                state,
                "execution_event_snapshot",
                return_value={},
            ), patch.object(
                state,
                "kis_order_truth_snapshot",
                return_value={},
            ), patch.object(
                state,
                "notify_new_live_fills",
                return_value=0,
            ), patch.object(state, "append_audit"):
                sender = threading.Thread(
                    target=lambda: sender_results.append(
                        LiveBrokerRouter().place_order(self.order())
                    )
                )
                poller = threading.Thread(
                    target=lambda: state.poll_execution_events(
                        "kis",
                        force_snapshot=True,
                        include_snapshot=False,
                    )
                )
                sender.start()
                self.assertTrue(sender_entered.wait(1))
                poller.start()
                self.assertFalse(
                    second_read_entered.wait(0.2),
                    "KIS poll read crossed an already-held final route",
                )
                release_sender.set()
                sender.join(2)
                self.assertFalse(sender.is_alive())
                self.assertTrue(second_read_entered.wait(1))
                release_second_read.set()
                poller.join(2)
                self.assertFalse(poller.is_alive())
            self.assertEqual(1, socket.call_count)
            self.assertTrue(sender_results and sender_results[0].get("ok"))
        finally:
            state._KIS_ROUTE_STATE_REVISION = old_route_revision
            state._KIS_CONTROL_RESERVATION = old_reservation
            state.STATE["execution_events"] = old_poll_snapshot

    def test_server_post_state_calls_have_exhaustive_kis_authority_classification(self) -> None:
        server_path = Path(state.__file__).with_name("server.py")
        tree = ast.parse(server_path.read_text(encoding="utf-8"))
        handler = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "LiveTraderHandler"
        )
        post = next(
            node
            for node in handler.body
            if isinstance(node, ast.FunctionDef) and node.name == "do_POST"
        )
        public_calls = {
            node.func.attr
            for node in ast.walk(post)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "state"
        }
        kis_authority_controls = {
            "promote_strategy_to_live",
            "poll_execution_events",
            "run_final_preflight",
            "run_policy_replay",
            "run_reconciliation",
            "run_recovery_drill",
            "run_shadow_live",
            "save_environment_settings",
            "set_automation_profile",
            "set_checklist_item",
            "set_flag",
            "set_mode",
            "set_retry_policy",
            "set_risk_setting",
            "set_strategy_lifecycle_status",
            "start_continuous_runtime",
            "stop_continuous_runtime",
        }
        delegated_kis_authority_controls = {"run_watchdog"}
        exchange_owned_or_final_mutations = {
            "apply_binance_futures_settings",
            "cancel_order",
            "recover_binance_spot_functional_backend_state",
            "recover_upbit_functional_backend_state",
            "retry_order",
            "run_strategy_cycle",
            "start_binance_futures_fill_soak",
            "start_binance_spot_functional_backend_state",
            "start_upbit_functional_backend_state",
            "stop_binance_futures_fill_soak",
            "stop_binance_spot_functional_backend_state",
            "stop_upbit_functional_backend_state",
            "submit_binance_smoke_order",
            "submit_test_intent",
            "submit_upbit_smoke_order",
            "test_binance_futures_canary_order",
        }
        observation_or_offline_mutations = {
            "export_audit",
            "issue_safety_confirmation",
            "preview_binance_futures_canary",
            "preview_binance_futures_fill_soak",
            "preview_binance_futures_order_risk",
            "preview_binance_futures_settings",
            "preview_binance_smoke_order",
            "preview_upbit_smoke_order",
            "refresh_upbit_smoke_order",
            "reprepare_crypto_first_live_functional_state",
            "run_broker_check",
            "run_validation_small_live_once",
            "snapshot",
            "start_execution_streams",
            "stop_execution_streams",
            "transition_operational_incident",
        }
        classified = (
            kis_authority_controls
            | delegated_kis_authority_controls
            | exchange_owned_or_final_mutations
            | observation_or_offline_mutations
        )
        self.assertEqual(public_calls, classified)
        self.assertIn(
            "apply_watchdog_fail_closed(report)",
            Path(state.__file__).read_text(encoding="utf-8"),
        )

    def test_all_kis_affecting_public_controls_have_canonical_fence_order(self) -> None:
        def fences(function):
            values: list[str] = []
            current = function
            while current is not None:
                marker = getattr(current, "_authority_fence", "")
                if marker:
                    values.append(marker)
                current = getattr(current, "__wrapped__", None)
            return values

        expected = {
            state.set_flag: ["SAFETY", "UPBIT", "BINANCE", "KIS_DIRECT"],
            state.save_environment_settings: [
                "SAFETY",
                "UPBIT",
                "BINANCE",
                "KIS_DIRECT",
            ],
            state.set_risk_setting: [
                "SAFETY",
                "UPBIT",
                "BINANCE",
                "KIS_DIRECT",
            ],
            state.set_checklist_item: [
                "SAFETY",
                "UPBIT",
                "BINANCE",
                "KIS_DIRECT",
            ],
            state.set_retry_policy: [
                "SAFETY",
                "UPBIT",
                "BINANCE",
                "KIS_DIRECT",
            ],
            state.set_mode: ["SAFETY", "KIS_TWO_PHASE_SETTINGS"],
            state.set_automation_profile: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.promote_strategy_to_live: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.set_strategy_lifecycle_status: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.run_final_preflight: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.run_reconciliation: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.run_shadow_live: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.run_policy_replay: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.run_recovery_drill: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.seed_program_ledger_from_broker_snapshot: [
                "SAFETY",
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.poll_execution_events: [
                "KIS_TWO_PHASE_SETTINGS",
            ],
            state.apply_watchdog_fail_closed: [
                "SAFETY",
                "KIS_TWO_PHASE_STOP",
            ],
            state.start_continuous_runtime: [
                "SAFETY",
                "UPBIT",
                "KIS_TWO_PHASE_START",
            ],
            state.stop_continuous_runtime: ["KIS_TWO_PHASE_STOP"],
        }
        for function, exact_order in expected.items():
            with self.subTest(function=function.__name__):
                self.assertEqual(exact_order, fences(function))


if __name__ == "__main__":
    unittest.main()
