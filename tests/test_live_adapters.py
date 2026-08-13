import os
import unittest
import urllib.error
from decimal import Decimal
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch

from live_trader.kis_order_authority import KisOrderAuthorityError

from live_trader.live_adapters import (
    BINANCE_ACCOUNT_ENDPOINT,
    BINANCE_ORDER_ENDPOINT,
    BINANCE_TEST_ORDER_ENDPOINT,
    KIS_DOMESTIC_BALANCE_ENDPOINT,
    KIS_DOMESTIC_CANCEL_ENDPOINT,
    KIS_DOMESTIC_ORDER_ENDPOINT,
    KIS_LIVE_BASE_URL,
    KIS_OVERSEAS_BALANCE_ENDPOINT,
    KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT,
    KIS_OVERSEAS_ORDER_ENDPOINT,
    KisRestRateLimitError,
    PreparedRequest,
    UPBIT_ACCOUNTS_ENDPOINT,
    UPBIT_ORDER_ENDPOINT,
    UPBIT_ORDER_DETAIL_ENDPOINT,
    _KisTradingNoRedirectHandler,
    _clear_binance_time_offset_cache,
    _clear_kis_access_token_cache,
    _reset_kis_request_pacer,
    build_binance_account_request,
    build_binance_cancel_order_request,
    build_binance_spot_order_request,
    build_kis_domestic_balance_request,
    build_kis_cancel_order_request,
    build_kis_live_order_request,
    build_kis_overseas_balance_request,
    build_kis_overseas_working_orders_request,
    build_upbit_accounts_request,
    build_upbit_cancel_order_request,
    build_upbit_order_request,
    http_json,
    issue_kis_access_token,
    normalize_binance_spot_intent,
    refresh_binance_time_offset,
    send_prepared_request,
    sign_binance_query,
)


ADAPTER_ENV_KEYS = (
    "KIS_APP_KEY",
    "KIS_APP_SECRET",
    "KIS_ACCOUNT_NO",
    "KIS_ACCOUNT_PRODUCT_CODE",
    "KIS_BASE_URL",
    "KIS_ENV",
    "KIS_REQUEST_MIN_INTERVAL_SECONDS",
    "KIS_READ_RATE_LIMIT_RETRIES",
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_BASE_URL",
    "UPBIT_ACCESS_KEY",
    "UPBIT_SECRET_KEY",
    "UPBIT_BASE_URL",
)


class EnvRestoreMixin:
    def setUp(self) -> None:
        self.previous_env = {key: os.environ.get(key) for key in ADAPTER_ENV_KEYS}
        _clear_binance_time_offset_cache()
        _clear_kis_access_token_cache()
        _reset_kis_request_pacer()
        for key in ADAPTER_ENV_KEYS:
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        _clear_binance_time_offset_cache()
        _clear_kis_access_token_cache()
        _reset_kis_request_pacer()
        for key, value in self.previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class LiveAdapterRequestBuilderTest(EnvRestoreMixin, unittest.TestCase):
    def test_response_read_timeout_returns_structured_failure(self) -> None:
        with patch(
            "live_trader.live_adapters.urllib.request.build_opener"
        ) as build_opener, patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as default_urlopen:
            response = (
                build_opener.return_value.open.return_value
                .__enter__.return_value
            )
            response.status = 200
            response.geturl.return_value = (
                "https://broker.example.test/account"
            )
            response.read.side_effect = TimeoutError("The read operation timed out")

            result = http_json(
                "GET",
                "https://broker.example.test/account",
                body=None,
                headers={},
                timeout_seconds=10,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(0, result["statusCode"])
        self.assertEqual("The read operation timed out", result["text"])
        default_urlopen.assert_not_called()

    def test_crypto_credentials_never_follow_redirects(self) -> None:
        routes = (
            (
                "upbit-get",
                "GET",
                "https://api.upbit.com/v1/accounts",
                None,
                {"Authorization": "Bearer upbit-private-token"},
            ),
            (
                "upbit-post",
                "POST",
                "https://api.upbit.com/v1/orders",
                {"market": "KRW-BTC", "side": "bid"},
                {
                    "Authorization": "Bearer upbit-private-token",
                    "Content-Type": "application/json",
                },
            ),
            (
                "binance-get",
                "GET",
                "https://api.binance.com/api/v3/account?timestamp=1",
                None,
                {"X-MBX-APIKEY": "binance-private-key"},
            ),
            (
                "binance-post",
                "POST",
                "https://api.binance.com/api/v3/order?timestamp=1",
                None,
                {"X-MBX-APIKEY": "binance-private-key"},
            ),
        )
        location = "https://attacker.invalid/collect"
        for label, method, url, body, headers in routes:
            for status in (301, 302, 307, 308):
                opened_urls: list[str] = []
                errors: list[urllib.error.HTTPError] = []

                def open_once(request, timeout=None):
                    opened_urls.append(request.full_url)
                    error = urllib.error.HTTPError(
                        request.full_url,
                        status,
                        "redirect",
                        {"Location": location},
                        BytesIO(b""),
                    )
                    errors.append(error)
                    raise error

                owned_opener = Mock()
                owned_opener.open.side_effect = open_once
                with self.subTest(route=label, status=status), patch(
                    "live_trader.live_adapters.urllib.request.build_opener",
                    return_value=owned_opener,
                ) as build_opener, patch(
                    "live_trader.live_adapters.urllib.request.urlopen"
                ) as default_urlopen:
                    result = http_json(
                        method,
                        url,
                        body=body,
                        headers=headers,
                        timeout_seconds=1,
                    )
                for error in errors:
                    error.close()
                self.assertFalse(result["ok"])
                self.assertEqual(status, result["statusCode"])
                self.assertTrue(result["redirectBlocked"])
                self.assertFalse(result["retryAllowed"])
                self.assertEqual(1, result["physicalAttemptCount"])
                self.assertEqual(method == "POST", result["outcomeAmbiguous"])
                self.assertEqual([url], opened_urls)
                self.assertNotIn(location, opened_urls)
                self.assertEqual(1, owned_opener.open.call_count)
                build_opener.assert_called_once()
                self.assertIsInstance(
                    build_opener.call_args.args[0],
                    _KisTradingNoRedirectHandler,
                )
                default_urlopen.assert_not_called()

    def test_crypto_no_redirect_opener_preserves_ordinary_success(self) -> None:
        routes = (
            (
                "GET",
                "https://api.upbit.com/v1/accounts",
                None,
                {"Authorization": "Bearer upbit-private-token"},
            ),
            (
                "POST",
                "https://api.upbit.com/v1/orders",
                {"market": "KRW-BTC", "side": "bid"},
                {"Authorization": "Bearer upbit-private-token"},
            ),
            (
                "GET",
                "https://api.binance.com/api/v3/account?timestamp=1",
                None,
                {"X-MBX-APIKEY": "binance-private-key"},
            ),
            (
                "POST",
                "https://api.binance.com/api/v3/order?timestamp=1",
                None,
                {"X-MBX-APIKEY": "binance-private-key"},
            ),
        )
        for method, url, body, headers in routes:
            owned_opener = MagicMock()
            response = owned_opener.open.return_value.__enter__.return_value
            response.status = 200
            response.geturl.return_value = url
            response.read.return_value = b'{"accepted":true}'
            response.headers = {}
            with self.subTest(method=method, url=url), patch(
                "live_trader.live_adapters.urllib.request.build_opener",
                return_value=owned_opener,
            ) as build_opener, patch(
                "live_trader.live_adapters.urllib.request.urlopen"
            ) as default_urlopen:
                result = http_json(
                    method,
                    url,
                    body=body,
                    headers=headers,
                    timeout_seconds=1,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(200, result["statusCode"])
            self.assertEqual({"accepted": True}, result["json"])
            self.assertEqual(1, owned_opener.open.call_count)
            self.assertIsInstance(
                build_opener.call_args.args[0],
                _KisTradingNoRedirectHandler,
            )
            default_urlopen.assert_not_called()

    def test_crypto_effective_url_change_is_terminal_without_retry(self) -> None:
        url = "https://api.upbit.com/v1/accounts"
        location = "https://attacker.invalid/collect"
        owned_opener = MagicMock()
        response = owned_opener.open.return_value.__enter__.return_value
        response.status = 200
        response.geturl.return_value = location
        response.headers = {}
        with patch(
            "live_trader.live_adapters.urllib.request.build_opener",
            return_value=owned_opener,
        ), patch(
            "live_trader.live_adapters.urllib.request.urlopen"
        ) as default_urlopen:
            result = http_json(
                "GET",
                url,
                body=None,
                headers={"Authorization": "Bearer upbit-private-token"},
                timeout_seconds=1,
            )
        self.assertFalse(result["ok"])
        self.assertTrue(result["redirectBlocked"])
        self.assertFalse(result["retryAllowed"])
        self.assertFalse(result["outcomeAmbiguous"])
        self.assertEqual(1, owned_opener.open.call_count)
        response.read.assert_not_called()
        default_urlopen.assert_not_called()

    def test_cancel_request_builders_use_official_endpoints_and_identifiers(self) -> None:
        os.environ.update({
            "KIS_APP_KEY": "kis-app-key",
            "KIS_APP_SECRET": "kis-app-secret",
            "KIS_ACCOUNT_NO": "12345678-01",
            "KIS_ACCOUNT_PRODUCT_CODE": "01",
            "BINANCE_API_KEY": "binance-key",
            "BINANCE_API_SECRET": "binance-secret",
            "UPBIT_ACCESS_KEY": "upbit-access",
            "UPBIT_SECRET_KEY": "upbit-secret",
        })
        kis = build_kis_cancel_order_request({
            "symbol": "005930.KS",
            "asset": "KR-STOCK",
            "broker_order_id": "00012345",
            "organization_no": "91252",
            "quantity": 3,
        }, access_token="kis-token")
        with patch("live_trader.live_adapters.time.time", return_value=1700000000.123):
            binance = build_binance_cancel_order_request("BTCUSDT", "98765")
        upbit = build_upbit_cancel_order_request("upbit-order-uuid")

        self.assertTrue(kis.can_send)
        self.assertEqual(KIS_DOMESTIC_CANCEL_ENDPOINT, kis.endpoint)
        self.assertEqual("02", kis.body["RVSE_CNCL_DVSN_CD"])
        self.assertEqual("Y", kis.body["QTY_ALL_ORD_YN"])
        self.assertEqual("DELETE", binance.method)
        self.assertEqual(BINANCE_ORDER_ENDPOINT, binance.endpoint)
        self.assertEqual("98765", binance.query["orderId"])
        self.assertEqual("DELETE", upbit.method)
        self.assertEqual(UPBIT_ORDER_DETAIL_ENDPOINT, upbit.endpoint)
        self.assertEqual("upbit-order-uuid", upbit.query["uuid"])

    def test_kis_domestic_order_request_uses_safe_headers_and_cash_order_body(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
            }
        )

        prepared = build_kis_live_order_request(
            {
                "symbol": "005930.KS",
                "asset": "KR-STOCK",
                "side": "BUY",
                "quantity": 3,
                "price": 71000,
                "order_type": "00",
            },
            access_token="token-123",
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual(prepared.provider, "kis")
        self.assertEqual(prepared.endpoint, KIS_DOMESTIC_ORDER_ENDPOINT)
        self.assertEqual(prepared.url, "https://kis.example.test" + KIS_DOMESTIC_ORDER_ENDPOINT)
        self.assertEqual(prepared.headers["tr_id"], "TTTC0012U")
        self.assertEqual(prepared.headers["authorization"], "Bearer token-123")
        self.assertEqual(prepared.safe_headers["appkey_configured"], True)
        self.assertEqual(prepared.safe_headers["appsecret_configured"], True)
        self.assertEqual(prepared.safe_headers["authorization_configured"], True)
        self.assertEqual(prepared.body["CANO"], "12345678")
        self.assertEqual(prepared.body["ACNT_PRDT_CD"], "01")
        self.assertEqual(prepared.body["PDNO"], "005930")
        self.assertEqual(prepared.body["ORD_QTY"], "3")
        self.assertEqual(prepared.body["ORD_UNPR"], "71000")
        self.assertEqual(prepared.body["ORD_DVSN"], "00")

    def test_kis_overseas_sell_order_request_uses_overseas_endpoint_and_tr_id(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "87654321",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
            }
        )

        prepared = build_kis_live_order_request(
            {
                "symbol": "AAPL",
                "asset": "US-STOCK",
                "side": "SELL",
                "quantity": 2,
                "price": 199.125,
                "order_type": "00",
            }
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual(prepared.endpoint, KIS_OVERSEAS_ORDER_ENDPOINT)
        self.assertEqual(prepared.headers["tr_id"], "TTTT1006U")
        self.assertEqual(prepared.body["OVRS_EXCG_CD"], "NASD")
        self.assertEqual(prepared.body["PDNO"], "AAPL")
        self.assertEqual(prepared.body["ORD_QTY"], "2")
        self.assertEqual(prepared.body["OVRS_ORD_UNPR"], "199.125")
        self.assertEqual(prepared.body["ORD_DVSN"], "00")

    def test_binance_test_order_request_signs_query_without_leaking_secret_in_preview(self) -> None:
        os.environ.update(
            {
                "BINANCE_API_KEY": "binance-key",
                "BINANCE_API_SECRET": "binance-secret",
                "BINANCE_BASE_URL": "https://binance.example.test",
            }
        )

        with patch("live_trader.live_adapters.time.time", return_value=1700000000.123):
            prepared = build_binance_spot_order_request(
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "quantity": "0.125",
                    "order_type": "LIMIT",
                    "price": "43000.5",
                    "time_in_force": "GTC",
                },
                test=True,
            )

        expected_query = {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "quantity": "0.125",
            "timestamp": 1700000000123,
            "timeInForce": "GTC",
            "price": "43000.5",
        }
        expected_signed_query = sign_binance_query(expected_query, "binance-secret")

        self.assertTrue(prepared.can_send)
        self.assertEqual(prepared.provider, "binance")
        self.assertEqual(prepared.endpoint, BINANCE_TEST_ORDER_ENDPOINT)
        self.assertEqual(prepared.url, f"https://binance.example.test{BINANCE_TEST_ORDER_ENDPOINT}?{expected_signed_query}")
        self.assertEqual(prepared.headers["X-MBX-APIKEY"], "binance-key")
        self.assertEqual(prepared.safe_headers["X-MBX-APIKEY_configured"], True)
        self.assertEqual(prepared.preview()["query"]["signature"], "***")

    def test_binance_market_buy_prefers_quote_notional(self) -> None:
        os.environ.update(
            {
                "BINANCE_API_KEY": "binance-key",
                "BINANCE_API_SECRET": "binance-secret",
                "BINANCE_BASE_URL": "https://binance.example.test",
            }
        )

        with patch("live_trader.live_adapters.time.time", return_value=1700000000.123):
            prepared = build_binance_spot_order_request(
                {
                    "symbol": "ETHUSDT",
                    "side": "BUY",
                    "quantity": "0.00123456789",
                    "notional": "5.5",
                    "order_type": "MARKET",
                }
            )

        self.assertTrue(prepared.can_send)
        self.assertEqual("5.5", prepared.query["quoteOrderQty"])
        self.assertNotIn("quantity", prepared.query)

    def test_binance_sell_quantity_is_rounded_down_to_exchange_step(self) -> None:
        rules = {
            "minQty": Decimal("0.001"),
            "maxQty": Decimal("1000"),
            "stepSize": Decimal("0.001"),
            "minNotional": Decimal("5"),
        }
        with patch("live_trader.live_adapters.binance_symbol_rules", return_value=rules):
            normalized = normalize_binance_spot_intent({
                "symbol": "ETHUSDT",
                "side": "SELL",
                "quantity": "0.012987",
                "price": "4000",
                "order_type": "MARKET",
            })

        self.assertEqual("0.012", normalized["quantity"])

    def test_upbit_order_request_builds_signed_limit_order_preview(self) -> None:
        os.environ.update(
            {
                "UPBIT_ACCESS_KEY": "upbit-access",
                "UPBIT_SECRET_KEY": "upbit-secret",
                "UPBIT_BASE_URL": "https://api.upbit.com",
            }
        )

        prepared = build_upbit_order_request(
            {
                "market": "KRW-BTC",
                "side": "BUY",
                "order_type": "limit",
                "quantity": "0.01",
                "price": "50000000",
            }
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual(prepared.provider, "upbit")
        self.assertEqual(prepared.endpoint, UPBIT_ORDER_ENDPOINT)
        self.assertEqual(prepared.url, "https://api.upbit.com" + UPBIT_ORDER_ENDPOINT)
        self.assertEqual(prepared.headers["Content-Type"], "application/json")
        self.assertTrue(prepared.headers["Authorization"].startswith("Bearer "))
        self.assertEqual(prepared.safe_headers["authorization_configured"], True)
        self.assertEqual(prepared.body["market"], "KRW-BTC")
        self.assertEqual(prepared.body["side"], "bid")
        self.assertEqual(prepared.body["ord_type"], "limit")
        self.assertEqual(prepared.body["volume"], "0.01")
        self.assertEqual(prepared.body["price"], "50000000")

    def test_upbit_mutation_rejects_nonofficial_base_before_jwt_or_socket(self) -> None:
        invalid_bases = (
            "https://attacker.invalid",
            "http://api.upbit.com",
            "https://api.upbit.com:443",
            "https://api.upbit.com/proxy",
            "https://user@api.upbit.com",
        )
        builders = (
            lambda: build_upbit_order_request(
                {
                    "market": "KRW-BTC",
                    "side": "BUY",
                    "order_type": "price",
                    "notional": "5000",
                }
            ),
            lambda: build_upbit_cancel_order_request("order-uuid"),
        )

        for configured in invalid_bases:
            for builder in builders:
                with self.subTest(configured=configured, builder=builder):
                    os.environ.update(
                        {
                            "UPBIT_ACCESS_KEY": "upbit-access",
                            "UPBIT_SECRET_KEY": "upbit-secret",
                            "UPBIT_BASE_URL": configured,
                        }
                    )
                    with (
                        patch(
                            "live_trader.live_adapters.build_upbit_authorization"
                        ) as jwt,
                        patch(
                            "live_trader.live_adapters.urllib.request.build_opener"
                        ) as build_opener,
                        patch(
                            "live_trader.live_adapters.urllib.request.urlopen"
                        ) as default_urlopen,
                    ):
                        with self.assertRaisesRegex(
                            RuntimeError,
                            "exact official production URL",
                        ):
                            builder()
                    jwt.assert_not_called()
                    build_opener.assert_not_called()
                    default_urlopen.assert_not_called()

    def test_upbit_mutation_edge_rejects_forged_foreign_url_socket_zero(self) -> None:
        for method, endpoint, url, body in (
            (
                "POST",
                UPBIT_ORDER_ENDPOINT,
                "https://attacker.invalid/v1/orders",
                {"market": "KRW-BTC", "side": "bid", "ord_type": "price"},
            ),
            (
                "DELETE",
                UPBIT_ORDER_DETAIL_ENDPOINT,
                "https://user@api.upbit.com/v1/order?uuid=order-uuid",
                None,
            ),
        ):
            prepared = PreparedRequest(
                provider="upbit",
                method=method,
                url=url,
                endpoint=endpoint,
                headers={"Authorization": "Bearer secret-jwt"},
                safe_headers={"authorization_configured": True},
                body=body,
                query=None,
                blocked_reasons=[],
            )
            with (
                self.subTest(method=method, url=url),
                patch("live_trader.live_adapters.http_json") as http_socket,
                patch(
                    "live_trader.live_adapters.urllib.request.build_opener"
                ) as build_opener,
                patch(
                    "live_trader.live_adapters.urllib.request.urlopen"
                ) as default_urlopen,
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "exact official endpoint",
                ):
                    send_prepared_request(prepared)
            http_socket.assert_not_called()
            build_opener.assert_not_called()
            default_urlopen.assert_not_called()

    def test_account_snapshot_requests_are_read_only_and_signed(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
                "BINANCE_API_KEY": "binance-key",
                "BINANCE_API_SECRET": "binance-secret",
                "BINANCE_BASE_URL": "https://binance.example.test",
                "UPBIT_ACCESS_KEY": "upbit-access",
                "UPBIT_SECRET_KEY": "upbit-secret",
                "UPBIT_BASE_URL": "https://upbit.example.test",
            }
        )

        with patch("live_trader.live_adapters.time.time", return_value=1700000000.123):
            kis = build_kis_domestic_balance_request(access_token="token-123")
            kis_overseas = build_kis_overseas_balance_request(access_token="token-123")
            binance = build_binance_account_request()
            upbit = build_upbit_accounts_request()

        self.assertTrue(kis.can_send)
        self.assertEqual(kis.method, "GET")
        self.assertEqual(kis.endpoint, KIS_DOMESTIC_BALANCE_ENDPOINT)
        self.assertIn("CANO=12345678", kis.url)
        self.assertEqual(kis.safe_headers["authorization_configured"], True)

        self.assertTrue(kis_overseas.can_send)
        self.assertEqual("GET", kis_overseas.method)
        self.assertEqual(KIS_OVERSEAS_BALANCE_ENDPOINT, kis_overseas.endpoint)
        self.assertEqual("TTTS3012R", kis_overseas.headers["tr_id"])
        self.assertEqual("NASD", kis_overseas.query["OVRS_EXCG_CD"])
        self.assertEqual("USD", kis_overseas.query["TR_CRCY_CD"])
        self.assertEqual("", kis_overseas.query["CTX_AREA_FK200"])
        self.assertEqual("", kis_overseas.query["CTX_AREA_NK200"])

        self.assertTrue(binance.can_send)
        self.assertEqual(binance.method, "GET")
        self.assertEqual(binance.endpoint, BINANCE_ACCOUNT_ENDPOINT)
        self.assertIn("timestamp=1700000000123", binance.url)
        self.assertEqual(binance.preview()["query"]["signature"], "***")

        self.assertTrue(upbit.can_send)
        self.assertEqual(upbit.method, "GET")
        self.assertEqual(upbit.endpoint, UPBIT_ACCOUNTS_ENDPOINT)
        self.assertTrue(upbit.headers["Authorization"].startswith("Bearer "))
        self.assertEqual(upbit.safe_headers["authorization_configured"], True)

    def test_kis_overseas_working_orders_request_is_account_wide_and_read_only(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
                "KIS_ENV": "real",
            }
        )

        prepared = build_kis_overseas_working_orders_request(
            access_token="token-123",
            context_fk200="FK-2",
            context_nk200="NK-2",
            continuation="N",
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual("GET", prepared.method)
        self.assertEqual(KIS_OVERSEAS_WORKING_ORDERS_ENDPOINT, prepared.endpoint)
        self.assertEqual("TTTS3018R", prepared.headers["tr_id"])
        self.assertEqual("N", prepared.headers["tr_cont"])
        self.assertEqual("NASD", prepared.query["OVRS_EXCG_CD"])
        self.assertEqual("DS", prepared.query["SORT_SQN"])
        self.assertEqual("FK-2", prepared.query["CTX_AREA_FK200"])
        self.assertEqual("NK-2", prepared.query["CTX_AREA_NK200"])
        self.assertEqual(
            {
                "CANO",
                "ACNT_PRDT_CD",
                "OVRS_EXCG_CD",
                "SORT_SQN",
                "CTX_AREA_FK200",
                "CTX_AREA_NK200",
            },
            set(prepared.query),
        )
        self.assertNotIn("authorization", prepared.safe_headers)

        os.environ["KIS_ENV"] = "demo"
        demo = build_kis_overseas_working_orders_request(access_token="token-123")
        self.assertFalse(demo.can_send)
        self.assertIn("kis_live_environment", demo.blocked_reasons)

    def test_kis_access_token_is_reused_until_near_expiry(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_BASE_URL": KIS_LIVE_BASE_URL,
                "KIS_ENV": "real",
            }
        )
        response = {"json": {"access_token": "token-123", "expires_in": 3600}}

        with patch("live_trader.live_adapters.require_kis_token_authority", return_value={}), patch("live_trader.live_adapters._acquire_shared_kis_rest_slot", return_value=0.0), patch("live_trader.live_adapters.http_json", return_value=response) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[100.0, 100.0, 100.0, 101.0],
        ):
            first = issue_kis_access_token()
            second = issue_kis_access_token()

        self.assertEqual(first, "token-123")
        self.assertEqual(second, "token-123")
        request.assert_called_once()

    def test_kis_requests_share_one_serial_pacer(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_ENV": "real",
                "KIS_REQUEST_MIN_INTERVAL_SECONDS": "2.1",
            }
        )
        prepared = build_kis_domestic_balance_request(
            access_token="token-123"
        )
        response = {"ok": True, "json": {"rt_cd": "0"}}

        with patch(
            "live_trader.live_adapters.require_kis_read_transport_authority",
            return_value={},
        ), patch("live_trader.live_adapters._acquire_shared_kis_rest_slot", return_value=0.0), patch(
            "live_trader.live_adapters.http_json",
            return_value=response,
        ) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[100.0, 100.0, 100.2, 102.1],
        ), patch("live_trader.live_adapters.time.sleep") as sleep:
            first = send_prepared_request(prepared)
            second = send_prepared_request(prepared)

        self.assertEqual(response, first)
        self.assertEqual(response, second)
        self.assertEqual(2, request.call_count)
        sleep.assert_called_once()
        self.assertAlmostEqual(1.9, sleep.call_args.args[0], places=9)

    def test_kis_shared_slot_is_reserved_immediately_before_network(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_ENV": "real",
            }
        )
        prepared = build_kis_domestic_balance_request(access_token="token-123")
        events = []
        with patch(
            "live_trader.live_adapters.require_kis_read_transport_authority",
            return_value={},
        ), patch(
            "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get"
        ) as get_limiter, patch(
            "live_trader.live_adapters.http_json",
            side_effect=lambda *args, **kwargs: (
                events.append("network")
                or {"ok": True, "json": {"rt_cd": "0"}}
            ),
        ) as request:
            get_limiter.return_value.acquire.side_effect = lambda: (
                events.append("limit") or 0.0
            )
            result = send_prepared_request(prepared)

        self.assertTrue(result["ok"])
        self.assertEqual(["limit", "network"], events)
        account_id, app_key_id, mode = get_limiter.call_args.args
        self.assertTrue(account_id.startswith("sha256:"))
        self.assertTrue(app_key_id.startswith("sha256:"))
        self.assertEqual("PROD", mode)
        request.assert_called_once()

        with patch(
            "live_trader.live_adapters.require_kis_read_transport_authority",
            return_value={},
        ), patch(
            "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get"
        ) as blocked_limiter, patch(
            "live_trader.live_adapters.http_json"
        ) as blocked_request:
            blocked_limiter.return_value.acquire.side_effect = (
                KisRestRateLimitError("limiter unavailable")
            )
            with self.assertRaises(KisRestRateLimitError):
                send_prepared_request(prepared)
        blocked_request.assert_not_called()

    def test_kis_token_uses_separate_app_key_scoped_one_second_limiter(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_BASE_URL": KIS_LIVE_BASE_URL,
                "KIS_ENV": "real",
            }
        )
        events = []
        response = {"json": {"access_token": "token-123", "expires_in": 3600}}

        with (
            patch(
                "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get_token"
            ) as get_token_limiter,
            patch(
                "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get"
            ) as get_rest_limiter,
            patch(
                "live_trader.live_adapters.require_kis_token_authority",
                return_value={},
            ),
            patch(
                "live_trader.live_adapters.http_json",
                side_effect=lambda *args, **kwargs: (
                    events.append("network") or response
                ),
            ) as request,
        ):
            get_token_limiter.return_value.acquire.side_effect = lambda: (
                events.append("token-limit") or 0.0
            )
            token = issue_kis_access_token()

        self.assertEqual("token-123", token)
        self.assertEqual(["token-limit", "network"], events)
        (app_key_id,) = get_token_limiter.call_args.args
        self.assertTrue(app_key_id.startswith("sha256:"))
        self.assertNotIn("kis-app-key", app_key_id)
        get_rest_limiter.assert_not_called()
        request.assert_called_once()

        _clear_kis_access_token_cache()
        with (
            patch(
                "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get_token"
            ) as blocked_limiter,
            patch(
                "live_trader.live_adapters.require_kis_token_authority",
                return_value={},
            ),
            patch("live_trader.live_adapters.http_json") as blocked_request,
        ):
            blocked_limiter.return_value.acquire.side_effect = (
                KisRestRateLimitError("token limiter unavailable")
            )
            with self.assertRaises(KisRestRateLimitError):
                issue_kis_access_token()
        blocked_request.assert_not_called()

    def test_kis_read_rate_limit_is_terminal_one_shot_and_post_is_not_retried(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_ENV": "real",
                "KIS_REQUEST_MIN_INTERVAL_SECONDS": "2.1",
            }
        )
        read_request = build_kis_domestic_balance_request(
            access_token="token-123"
        )
        rate_limited = {
            "ok": False,
            "statusCode": 500,
            "json": {
                "rt_cd": "1",
                "msg_cd": "EGW00215",
                "msg1": "원장에서 허용 가능한 초당 거래건수를 초과하였습니다.",
            },
        }
        with patch(
            "live_trader.live_adapters.require_kis_read_transport_authority",
            return_value={},
        ), patch("live_trader.live_adapters._acquire_shared_kis_rest_slot", return_value=0.0), patch(
            "live_trader.live_adapters.http_json", return_value=rate_limited,
        ) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[100.0, 100.0],
        ), patch("live_trader.live_adapters.time.sleep") as sleep:
            result = send_prepared_request(read_request)

        self.assertFalse(result["ok"])
        self.assertEqual(1, result["physicalAttemptCount"])
        self.assertFalse(result["retryAllowed"])
        self.assertTrue(result["terminalReadLease"])
        request.assert_called_once()
        sleep.assert_not_called()

        order_request = build_kis_live_order_request(
            {
                "symbol": "005930.KS",
                "side": "BUY",
                "quantity": 1,
                "price": 70000,
            },
            access_token="token-123",
        )
        _reset_kis_request_pacer()
        with patch("live_trader.live_adapters._acquire_shared_kis_rest_slot", return_value=0.0), patch(
            "live_trader.live_adapters.http_json",
            return_value=rate_limited,
        ) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[200.0, 200.0],
        ), patch("live_trader.live_adapters.time.sleep") as sleep:
            with self.assertRaisesRegex(
                KisOrderAuthorityError, "inherited final mutation lease"
            ):
                send_prepared_request(order_request)

        request.assert_not_called()
        sleep.assert_not_called()

    def test_binance_server_time_offset_is_applied_to_signed_requests(self) -> None:
        os.environ.update(
            {
                "BINANCE_API_KEY": "binance-key",
                "BINANCE_API_SECRET": "binance-secret",
                "BINANCE_BASE_URL": "https://binance.example.test",
            }
        )
        response = {"ok": True, "json": {"serverTime": 1700000005123}}

        with patch("live_trader.live_adapters.http_json", return_value=response), patch(
            "live_trader.live_adapters.time.time",
            side_effect=[1700000000.123, 1700000000.123],
        ):
            offset = refresh_binance_time_offset()
        with patch("live_trader.live_adapters.time.time", return_value=1700000000.123):
            prepared = build_binance_account_request()

        self.assertEqual(5000, offset)
        self.assertEqual(1700000005123, prepared.query["timestamp"])

    def test_missing_settings_and_invalid_intent_are_reported_as_blocked_reasons(self) -> None:
        kis = build_kis_live_order_request({"symbol": "", "side": "BUY", "quantity": 0, "price": 0})
        binance = build_binance_spot_order_request({"symbol": "", "side": "BUY", "quantity": 0})
        upbit = build_upbit_order_request({"market": "", "side": "SELL", "quantity": 0})
        kis_balance = build_kis_domestic_balance_request()
        kis_overseas_balance = build_kis_overseas_balance_request()
        binance_account = build_binance_account_request()
        upbit_accounts = build_upbit_accounts_request()

        self.assertFalse(kis.can_send)
        self.assertIn("KIS_APP_KEY", kis.blocked_reasons)
        self.assertIn("KIS_APP_SECRET", kis.blocked_reasons)
        self.assertIn("KIS_ACCOUNT_NO", kis.blocked_reasons)
        self.assertIn("KIS_ACCOUNT_PRODUCT_CODE", kis.blocked_reasons)
        self.assertIn("symbol", kis.blocked_reasons)
        self.assertIn("quantity", kis.blocked_reasons)

        self.assertFalse(binance.can_send)
        self.assertIn("BINANCE_API_KEY", binance.blocked_reasons)
        self.assertIn("BINANCE_API_SECRET", binance.blocked_reasons)
        self.assertIn("symbol", binance.blocked_reasons)
        self.assertIn("quantity", binance.blocked_reasons)

        self.assertFalse(upbit.can_send)
        self.assertIn("UPBIT_ACCESS_KEY", upbit.blocked_reasons)
        self.assertIn("UPBIT_SECRET_KEY", upbit.blocked_reasons)
        self.assertIn("market", upbit.blocked_reasons)

        self.assertFalse(kis_balance.can_send)
        self.assertIn("access_token", kis_balance.blocked_reasons)
        self.assertFalse(kis_overseas_balance.can_send)
        self.assertIn("access_token", kis_overseas_balance.blocked_reasons)
        self.assertFalse(binance_account.can_send)
        self.assertIn("BINANCE_API_KEY", binance_account.blocked_reasons)
        self.assertFalse(upbit_accounts.can_send)
        self.assertIn("UPBIT_ACCESS_KEY", upbit_accounts.blocked_reasons)


if __name__ == "__main__":
    unittest.main()
