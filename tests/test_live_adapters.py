import os
import unittest
from decimal import Decimal
from unittest.mock import patch

from live_trader.live_adapters import (
    BINANCE_ACCOUNT_ENDPOINT,
    BINANCE_ORDER_ENDPOINT,
    BINANCE_TEST_ORDER_ENDPOINT,
    KIS_DOMESTIC_BALANCE_ENDPOINT,
    KIS_DOMESTIC_CANCEL_ENDPOINT,
    KIS_DOMESTIC_ORDER_ENDPOINT,
    KIS_OVERSEAS_BALANCE_ENDPOINT,
    KIS_OVERSEAS_ORDER_ENDPOINT,
    UPBIT_ACCOUNTS_ENDPOINT,
    UPBIT_ORDER_ENDPOINT,
    UPBIT_ORDER_DETAIL_ENDPOINT,
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
        with patch("live_trader.live_adapters.urllib.request.urlopen") as urlopen:
            response = urlopen.return_value.__enter__.return_value
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
                "UPBIT_BASE_URL": "https://upbit.example.test",
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
        self.assertEqual(prepared.url, "https://upbit.example.test" + UPBIT_ORDER_ENDPOINT)
        self.assertEqual(prepared.headers["Content-Type"], "application/json")
        self.assertTrue(prepared.headers["Authorization"].startswith("Bearer "))
        self.assertEqual(prepared.safe_headers["authorization_configured"], True)
        self.assertEqual(prepared.body["market"], "KRW-BTC")
        self.assertEqual(prepared.body["side"], "bid")
        self.assertEqual(prepared.body["ord_type"], "limit")
        self.assertEqual(prepared.body["volume"], "0.01")
        self.assertEqual(prepared.body["price"], "50000000")

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

    def test_kis_access_token_is_reused_until_near_expiry(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_BASE_URL": "https://kis.example.test",
            }
        )
        response = {"json": {"access_token": "token-123", "expires_in": 3600}}

        with patch("live_trader.live_adapters.http_json", return_value=response) as request, patch(
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
                "KIS_REQUEST_MIN_INTERVAL_SECONDS": "2.1",
            }
        )
        prepared = build_kis_domestic_balance_request(
            access_token="token-123"
        )
        response = {"ok": True, "json": {"rt_cd": "0"}}

        with patch(
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

    def test_kis_read_rate_limit_is_retried_but_post_is_not(self) -> None:
        os.environ.update(
            {
                "KIS_APP_KEY": "kis-app-key",
                "KIS_APP_SECRET": "kis-app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
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
        recovered = {"ok": True, "json": {"rt_cd": "0"}}

        with patch(
            "live_trader.live_adapters.http_json",
            side_effect=[rate_limited, recovered],
        ) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[100.0, 100.0, 102.1],
        ), patch("live_trader.live_adapters.time.sleep") as sleep:
            result = send_prepared_request(read_request)

        self.assertEqual(recovered, result)
        self.assertEqual(2, request.call_count)
        sleep.assert_called_once_with(2.1)

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
        with patch(
            "live_trader.live_adapters.http_json",
            return_value=rate_limited,
        ) as request, patch(
            "live_trader.live_adapters.time.monotonic",
            side_effect=[200.0, 200.0],
        ), patch("live_trader.live_adapters.time.sleep") as sleep:
            result = send_prepared_request(order_request)

        self.assertEqual(rate_limited, result)
        request.assert_called_once()
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
