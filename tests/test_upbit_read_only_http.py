from __future__ import annotations

from io import BytesIO
import unittest
from unittest.mock import patch
import urllib.error

from live_trader.upbit_read_only_http import (
    PreparedRequest,
    _protected_upbit_read_only_http_network_capability,
    send_prepared_request,
)


class _Response:
    status = 200
    headers = {}

    def __init__(self, url: str, body: bytes = b"[]") -> None:
        self._url = url
        self._body = body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def read(self) -> bytes:
        return self._body


def _request(**changes: object) -> PreparedRequest:
    values: dict[str, object] = {
        "provider": "upbit-functional-read",
        "method": "GET",
        "url": "https://api.upbit.com/v1/api_keys",
        "endpoint": "/v1/api_keys",
        "headers": {"Authorization": "Bearer must-not-leak"},
        "safe_headers": {"authorization_configured": True},
        "body": None,
        "query": {},
        "blocked_reasons": [],
    }
    values.update(changes)
    return PreparedRequest(**values)  # type: ignore[arg-type]


class UpbitReadOnlyHttpTest(unittest.TestCase):
    def test_blocked_request_has_zero_physical_attempt_and_redacted_preview(
        self,
    ) -> None:
        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_read_only_http_network_capability()
            result = send_prepared_request(
                _request(blocked_reasons=["gate-disabled"]),
                network_capability=capability,
            )
        self.assertEqual(0, result["physicalAttemptCount"])
        self.assertFalse(result["retryAllowed"])
        serialized = str(result)
        self.assertNotIn("must-not-leak", serialized)
        self.assertNotIn("Authorization", serialized)

    def test_rejects_post_body_and_nonofficial_origin_before_transport(
        self,
    ) -> None:
        hostile = (
            _request(method="POST"),
            _request(body={"market": "KRW-BTC"}),
            _request(url="https://api.upbit.com.evil.invalid/v1/api_keys"),
            _request(url="https://api.upbit.com:443/v1/api_keys"),
            _request(
                url="https://api.upbit.com/v1/withdraws",
                endpoint="/v1/withdraws",
            ),
        )
        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_read_only_http_network_capability()
            with patch(
                "live_trader.upbit_read_only_http.urllib.request.build_opener"
            ) as opener:
                for request in hostile:
                    with self.subTest(url=request.url, method=request.method):
                        with self.assertRaisesRegex(
                            RuntimeError, "read-only-request-shape"
                        ):
                            send_prepared_request(
                                request,
                                network_capability=capability,
                            )
        opener.assert_not_called()

    def test_public_direct_call_is_closed_before_opener(self) -> None:
        with patch(
            "live_trader.upbit_read_only_http.urllib.request.build_opener"
        ) as opener, self.assertRaisesRegex(
            RuntimeError, "network-capability-closed"
        ):
            send_prepared_request(_request())
        opener.assert_not_called()

        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ), patch(
            "live_trader.upbit_read_only_http.urllib.request.build_opener"
        ) as forged_opener, self.assertRaisesRegex(
            RuntimeError, "network-capability-closed"
        ):
            send_prepared_request(
                _request(), network_capability=object()
            )
        forged_opener.assert_not_called()

    def test_exact_get_has_one_attempt_no_body_and_no_retry(self) -> None:
        class Opener:
            def __init__(self) -> None:
                self.calls = 0

            def open(self, request: object, *, timeout: float) -> _Response:
                self.calls += 1
                self.request = request
                self.timeout = timeout
                return _Response("https://api.upbit.com/v1/api_keys")

        opener = Opener()
        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_read_only_http_network_capability()
            with patch(
                "live_trader.upbit_read_only_http.urllib.request.build_opener",
                return_value=opener,
            ):
                result = send_prepared_request(
                    _request(),
                    timeout_seconds=3.0,
                    network_capability=capability,
                )
        self.assertTrue(result["ok"])
        self.assertEqual(1, result["physicalAttemptCount"])
        self.assertFalse(result["retryAllowed"])
        self.assertEqual(1, opener.calls)
        self.assertEqual(3.0, opener.timeout)
        self.assertEqual("GET", opener.request.get_method())
        self.assertIsNone(opener.request.data)

    def test_redirect_is_not_followed_and_network_error_is_not_retried(self) -> None:
        class RedirectOpener:
            def open(self, request: object, *, timeout: float) -> object:
                raise urllib.error.HTTPError(
                    request.full_url,
                    302,
                    "Found",
                    {"Location": "https://evil.invalid/"},
                    BytesIO(b""),
                )

        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_read_only_http_network_capability()
            with patch(
                "live_trader.upbit_read_only_http.urllib.request.build_opener",
                return_value=RedirectOpener(),
            ):
                redirect = send_prepared_request(
                    _request(), network_capability=capability
                )
        self.assertTrue(redirect["redirectBlocked"])
        self.assertEqual(1, redirect["physicalAttemptCount"])
        self.assertFalse(redirect["retryAllowed"])

        class FailingOpener:
            def __init__(self) -> None:
                self.calls = 0

            def open(self, _request: object, *, timeout: float) -> object:
                self.calls += 1
                raise urllib.error.URLError("offline")

        failing = FailingOpener()
        with patch(
            "live_trader.upbit_read_only_http."
            "UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED",
            True,
        ):
            capability = _protected_upbit_read_only_http_network_capability()
            with patch(
                "live_trader.upbit_read_only_http.urllib.request.build_opener",
                return_value=failing,
            ):
                failure = send_prepared_request(
                    _request(), network_capability=capability
                )
        self.assertEqual(1, failing.calls)
        self.assertEqual(1, failure["physicalAttemptCount"])
        self.assertFalse(failure["retryAllowed"])


if __name__ == "__main__":
    unittest.main()
