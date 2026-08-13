from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from live_trader.brokers import (
    BrokerNotReadyError,
    fetch_kis_us_live_quote,
)
from live_trader.live_adapters import (
    KIS_OVERSEAS_PRICE_ENDPOINT,
    KIS_OVERSEAS_PRICE_TR_ID,
    build_kis_us_live_quote_request,
)


class KisUsLiveQuoteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_environment = dict(os.environ)
        self.addCleanup(self._restore_environment)
        os.environ.update(
            {
                "KIS_APP_KEY": "app-key",
                "KIS_APP_SECRET": "app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
                "KIS_ENV": "real",
            }
        )

    def _restore_environment(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_environment)

    @staticmethod
    def _response(
        *,
        identity: str = "DNYSF",
        price: object = "11.25",
        **output: object,
    ) -> dict[str, object]:
        return {
            "ok": True,
            "statusCode": 200,
            "json": {
                "rt_cd": "0",
                "msg_cd": "MCA00000",
                "output": {
                    "rsym": identity,
                    "last": price,
                    **output,
                },
            },
        }

    def test_builder_is_get_only_and_maps_exact_nyse_to_nys(self) -> None:
        request = build_kis_us_live_quote_request(
            access_token="token",
            symbol="f",
            exchange="nyse",
        )

        self.assertTrue(request.can_send)
        self.assertEqual("GET", request.method)
        self.assertEqual(KIS_OVERSEAS_PRICE_ENDPOINT, request.endpoint)
        self.assertEqual(KIS_OVERSEAS_PRICE_TR_ID, request.headers["tr_id"])
        self.assertEqual(
            {"AUTH": "", "EXCD": "NYS", "SYMB": "F"},
            request.query,
        )
        self.assertIsNone(request.body)
        self.assertIn("EXCD=NYS", request.url)
        self.assertIn("SYMB=F", request.url)
        self.assertNotIn("CANO", request.query)

    def test_builder_fails_closed_outside_exact_live_scope(self) -> None:
        wrong_symbol = build_kis_us_live_quote_request(
            access_token="token",
            symbol="AAPL",
            exchange="NYSE",
        )
        wrong_exchange = build_kis_us_live_quote_request(
            access_token="token",
            symbol="F",
            exchange="NASD",
        )
        os.environ["KIS_ENV"] = "demo"
        demo = build_kis_us_live_quote_request(
            access_token="token",
            symbol="F",
            exchange="NYSE",
        )

        self.assertFalse(wrong_symbol.can_send)
        self.assertIn("exact_symbol_f", wrong_symbol.blocked_reasons)
        self.assertFalse(wrong_exchange.can_send)
        self.assertIn("exact_exchange_nyse", wrong_exchange.blocked_reasons)
        self.assertFalse(demo.can_send)
        self.assertIn("kis_live_environment", demo.blocked_reasons)
        for request in (wrong_symbol, wrong_exchange, demo):
            self.assertEqual("GET", request.method)
            self.assertIsNone(request.body)

    def test_fetch_verifies_identity_price_and_local_freshness_metadata(self) -> None:
        ticks = iter(
            (
                datetime(2026, 8, 12, 13, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 8, 12, 13, 30, 1, tzinfo=timezone.utc),
            )
        )
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=self._response(symb="F", excd="NYS"),
        ) as send:
            quote = fetch_kis_us_live_quote(
                "token",
                clock=lambda: next(ticks),
            )

        prepared = send.call_args.args[0]
        self.assertEqual("GET", prepared.method)
        self.assertIsNone(prepared.body)
        self.assertEqual(5.0, send.call_args.kwargs["timeout_seconds"])
        self.assertEqual("F", quote["symbol"])
        self.assertEqual("NYSE", quote["exchange"])
        self.assertEqual("NYS", quote["quote_exchange"])
        self.assertEqual(11.25, quote["price"])
        self.assertEqual("11.25", quote["price_text"])
        self.assertEqual("DNYSF", quote["response_identity"])
        self.assertTrue(quote["identity_verified"])
        self.assertEqual("delayed-indicator", quote["provider_feed_mode"])
        self.assertEqual("2026-08-12T13:30:01Z", quote["observed_at"])
        self.assertEqual(
            "2026-08-12T09:30:01-04:00",
            quote["observed_at_new_york"],
        )
        self.assertEqual(1.0, quote["round_trip_seconds"])
        self.assertTrue(quote["locally_fresh"])
        self.assertEqual("2026-08-12T13:30:06Z", quote["fresh_until"])
        self.assertEqual(
            "local-http-response-observation",
            quote["freshness_basis"],
        )
        self.assertFalse(quote["provider_trade_timestamp_available"])

    def test_fetch_rejects_wrong_or_ambiguous_response_identity(self) -> None:
        wrong_rows = (
            self._response(identity="DNASF"),
            self._response(identity="DNYSEF"),
            self._response(identity="DNYSMSFT"),
            self._response(identity="XNYSF"),
            self._response(identity="DNYSF", symb="AAPL"),
            self._response(identity="DNYSF", excd="NAS"),
        )
        for response in wrong_rows:
            with self.subTest(response=response):
                with patch(
                    "live_trader.brokers.send_prepared_request",
                    return_value=response,
                ):
                    with self.assertRaisesRegex(
                        BrokerNotReadyError,
                        "identity|종목코드|거래소코드",
                    ):
                        fetch_kis_us_live_quote("token")

    def test_fetch_rejects_nonpositive_nonfinite_or_missing_price(self) -> None:
        for price in (0, "-1", "NaN", "Infinity", None, "bad"):
            with self.subTest(price=price):
                with patch(
                    "live_trader.brokers.send_prepared_request",
                    return_value=self._response(price=price),
                ):
                    with self.assertRaisesRegex(
                        BrokerNotReadyError,
                        "현재가",
                    ):
                        fetch_kis_us_live_quote("token")

    def test_fetch_requires_explicit_success_and_nonempty_output(self) -> None:
        failures = (
            {"ok": True, "json": {"output": {"rsym": "DNYSF", "last": "11"}}},
            {"ok": True, "json": {"rt_cd": "0", "output": {}}},
            {"ok": True, "json": {"rt_cd": "0", "output": []}},
        )
        for response in failures:
            with self.subTest(response=response):
                with patch(
                    "live_trader.brokers.send_prepared_request",
                    return_value=response,
                ):
                    with self.assertRaises(BrokerNotReadyError):
                        fetch_kis_us_live_quote("token")

    def test_fetch_rejects_stale_or_invalid_local_clock(self) -> None:
        slow_ticks = iter(
            (
                datetime(2026, 8, 12, 13, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 8, 12, 13, 30, 6, tzinfo=timezone.utc),
            )
        )
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=self._response(),
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "freshness"):
                fetch_kis_us_live_quote(
                    "token",
                    clock=lambda: next(slow_ticks),
                )

        with self.assertRaisesRegex(BrokerNotReadyError, "시간대"):
            fetch_kis_us_live_quote(
                "token",
                clock=lambda: datetime(2026, 8, 12, 13, 30),
            )
        for invalid in (True, 0, -1, 5.1, float("nan"), float("inf")):
            with self.subTest(freshness=invalid):
                with self.assertRaisesRegex(BrokerNotReadyError, "freshness"):
                    fetch_kis_us_live_quote(
                        "token",
                        freshness_seconds=invalid,
                    )

    def test_fetch_rejects_caller_scope_before_transport(self) -> None:
        with patch("live_trader.brokers.send_prepared_request") as send:
            with self.assertRaisesRegex(BrokerNotReadyError, "exact F/NYSE"):
                fetch_kis_us_live_quote(
                    "token",
                    symbol="AAPL",
                    exchange="NASDAQ",
                )

        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
