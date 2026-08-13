from __future__ import annotations

from email.message import Message
import http.cookiejar
from io import BytesIO
import json
from types import SimpleNamespace
import threading
from urllib.parse import urlsplit
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

from live_trader.emergency_stop import DesktopEmergencyStopBridge
from live_trader.functional_http_session import (
    APP_SESSION_COOKIE,
    CSRF_HEADER,
    FunctionalHttpSessionAuthority,
    FunctionalHttpSessionError,
)
from live_trader.server import LiveTraderHandler, bind_server


def request_headers(
    authority: FunctionalHttpSessionAuthority,
    *,
    host: str | None = None,
    origin: str | None = None,
    csrf: str | None = None,
    cookie: str | None = None,
) -> Message:
    headers = Message()
    headers["Host"] = host or authority.expected_host_header
    if origin is not None:
        headers["Origin"] = origin
    if csrf is not None:
        headers[CSRF_HEADER] = csrf
    headers["Cookie"] = cookie or (
        f"{APP_SESSION_COOKIE}={authority.app_session_token}"
    )
    return headers


class FunctionalHttpSessionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.authority = FunctionalHttpSessionAuthority.mint(
            host="127.0.0.1", port=18795
        )

    def test_native_bootstrap_exposes_csrf_but_never_app_session_secret(self) -> None:
        bootstrap = self.authority.trusted_native_bootstrap()
        self.assertGreaterEqual(len(self.authority.app_session_token), 40)
        self.assertGreaterEqual(len(self.authority.csrf_token), 40)
        self.assertNotEqual(
            self.authority.app_session_token, self.authority.csrf_token
        )
        self.assertNotIn("appSessionToken", bootstrap)
        self.assertEqual(self.authority.csrf_token, bootstrap["csrfToken"])
        self.assertIn("HttpOnly", self.authority.set_cookie_header)
        self.assertIn("SameSite=Strict", self.authority.set_cookie_header)
        self.assertIn("Path=/api/", self.authority.set_cookie_header)
        bridge = DesktopEmergencyStopBridge(
            functional_http_bootstrap=self.authority.trusted_native_bootstrap
        )
        value = bridge.functional_http_session()
        self.assertTrue(value["ok"])
        self.assertNotIn(self.authority.app_session_token, repr(value))

    def test_exact_native_bootstrap_redirects_once_without_body_or_secret(self) -> None:
        parsed = urlsplit(self.authority.native_bootstrap_url)
        handler = object.__new__(LiveTraderHandler)
        handler.server = SimpleNamespace(
            functional_http_session_authority=self.authority
        )
        handler.client_address = ("127.0.0.1", 50000)
        handler.headers = Message()
        handler.headers["Host"] = self.authority.expected_host_header
        handler.send_response = unittest.mock.Mock()
        handler.send_header = unittest.mock.Mock()
        handler.end_headers = unittest.mock.Mock()
        handler.wfile = BytesIO()
        handler._handle_native_bootstrap(parsed)
        handler.send_response.assert_called_once_with(303)
        sent = dict(call.args for call in handler.send_header.call_args_list)
        self.assertEqual("/", sent["Location"])
        self.assertEqual("0", sent["Content-Length"])
        self.assertEqual("no-store", sent["Cache-Control"])
        self.assertEqual("no-cache", sent["Pragma"])
        self.assertEqual("no-referrer", sent["Referrer-Policy"])
        self.assertEqual("nosniff", sent["X-Content-Type-Options"])
        self.assertIn("HttpOnly", sent["Set-Cookie"])
        self.assertNotIn(self.authority.bootstrap_nonce, repr(sent))
        self.assertEqual(b"", handler.wfile.getvalue())

        handler._send_functional_http_denial = unittest.mock.Mock()
        handler._handle_native_bootstrap(parsed)
        handler._send_functional_http_denial.assert_called_once()

    def test_native_bootstrap_rejects_encoded_duplicate_wrong_peer_and_expiry(self) -> None:
        valid = urlsplit(self.authority.native_bootstrap_url)
        invalid_queries = (
            valid.query + "&nonce=" + self.authority.bootstrap_nonce,
            "nonce=%" + self.authority.bootstrap_nonce,
            "nonce=wrong",
            "other=" + self.authority.bootstrap_nonce,
        )
        for query in invalid_queries:
            fresh = FunctionalHttpSessionAuthority.mint(
                host="127.0.0.1", port=18795
            )
            handler = object.__new__(LiveTraderHandler)
            handler.server = SimpleNamespace(
                functional_http_session_authority=fresh
            )
            handler.client_address = ("127.0.0.1", 50000)
            handler.headers = Message()
            handler.headers["Host"] = fresh.expected_host_header
            handler._send_functional_http_denial = unittest.mock.Mock()
            handler._handle_native_bootstrap(
                urlsplit(f"{fresh.expected_origin}/__lt_native_bootstrap?{query}")
            )
            handler._send_functional_http_denial.assert_called_once()

        fresh = FunctionalHttpSessionAuthority.mint(
            host="127.0.0.1", port=18795
        )
        self.assertFalse(
            fresh.consume_native_bootstrap(
                nonce=fresh.bootstrap_nonce,
                host_header=fresh.expected_host_header,
                peer_host="192.0.2.1",
            )
        )
        self.assertFalse(
            fresh.consume_native_bootstrap(
                nonce=fresh.bootstrap_nonce,
                host_header=fresh.expected_host_header,
                peer_host="127.0.0.1",
                now_epoch=fresh.bootstrap_expires_epoch + 1,
            )
        )

        fresh = FunctionalHttpSessionAuthority.mint(
            host="127.0.0.1", port=18795
        )
        with patch(
            "live_trader.functional_http_session.time.time",
            side_effect=[
                fresh.bootstrap_expires_epoch - 0.1,
                fresh.bootstrap_expires_epoch + 0.1,
            ],
        ):
            self.assertFalse(
                fresh.consume_native_bootstrap(
                    nonce=fresh.bootstrap_nonce,
                    host_header=fresh.expected_host_header,
                    peer_host="127.0.0.1",
                )
            )

    def test_malformed_cookie_is_cleanly_rejected(self) -> None:
        headers = request_headers(
            self.authority,
            cookie=(
                f"{APP_SESSION_COOKIE}={self.authority.app_session_token}; "
                "$Bogus=value"
            ),
        )
        with self.assertRaisesRegex(FunctionalHttpSessionError, "malformed"):
            self.authority.assert_request(
                headers=headers,
                peer_host="127.0.0.1",
                require_origin=False,
            )

    def test_absolute_form_native_bootstrap_is_rejected_without_consuming_nonce(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = self.authority.native_bootstrap_url
        handler.server = SimpleNamespace(
            functional_http_session_authority=self.authority
        )
        handler.client_address = ("127.0.0.1", 50000)
        handler.headers = Message()
        handler.headers["Host"] = self.authority.expected_host_header
        handler._send_functional_http_denial = unittest.mock.Mock()
        handler.do_GET()
        handler._send_functional_http_denial.assert_called_once()
        self.assertTrue(
            self.authority.consume_native_bootstrap(
                nonce=self.authority.bootstrap_nonce,
                host_header=self.authority.expected_host_header,
                peer_host="127.0.0.1",
            )
        )

    def test_status_requires_exact_host_cookie_and_loopback_peer(self) -> None:
        self.authority.assert_request(
            headers=request_headers(self.authority),
            peer_host="127.0.0.1",
            require_origin=False,
        )
        invalid = (
            (request_headers(self.authority, host="localhost:18795"), "127.0.0.1"),
            (request_headers(self.authority, host="127.0.0.1:1"), "127.0.0.1"),
            (request_headers(self.authority), "192.0.2.1"),
            (
                request_headers(
                    self.authority,
                    cookie=f"{APP_SESSION_COOKIE}=wrong",
                ),
                "127.0.0.1",
            ),
        )
        for headers, peer in invalid:
            with self.subTest(headers=headers, peer=peer), self.assertRaises(
                FunctionalHttpSessionError
            ):
                self.authority.assert_request(
                    headers=headers,
                    peer_host=peer,
                    require_origin=False,
                )

    def test_mutation_requires_same_origin_and_exact_csrf(self) -> None:
        self.authority.assert_request(
            headers=request_headers(
                self.authority,
                origin=self.authority.expected_origin,
                csrf=self.authority.csrf_token,
            ),
            peer_host="127.0.0.1",
            require_origin=True,
        )
        for origin, csrf in (
            (None, self.authority.csrf_token),
            ("null", self.authority.csrf_token),
            ("https://127.0.0.1:18795", self.authority.csrf_token),
            ("http://evil.example", self.authority.csrf_token),
            (self.authority.expected_origin, "wrong"),
            (self.authority.expected_origin, None),
        ):
            with self.subTest(origin=origin, csrf=csrf), self.assertRaises(
                FunctionalHttpSessionError
            ):
                self.authority.assert_request(
                    headers=request_headers(
                        self.authority, origin=origin, csrf=csrf
                    ),
                    peer_host="127.0.0.1",
                    require_origin=True,
                )

    def test_duplicate_security_headers_are_rejected(self) -> None:
        headers = request_headers(
            self.authority,
            origin=self.authority.expected_origin,
            csrf=self.authority.csrf_token,
        )
        headers["Host"] = self.authority.expected_host_header
        with self.assertRaisesRegex(FunctionalHttpSessionError, "duplicated"):
            self.authority.assert_request(
                headers=headers,
                peer_host="127.0.0.1",
                require_origin=True,
            )
        headers = request_headers(
            self.authority,
            origin=self.authority.expected_origin,
            csrf=self.authority.csrf_token,
            cookie=(
                f"{APP_SESSION_COOKIE}=wrong; "
                f"{APP_SESSION_COOKIE}={self.authority.app_session_token}"
            ),
        )
        with self.assertRaisesRegex(FunctionalHttpSessionError, "duplicated"):
            self.authority.assert_request(
                headers=headers,
                peer_host="127.0.0.1",
                require_origin=True,
            )
        headers = request_headers(
            self.authority,
            origin=self.authority.expected_origin,
            csrf=self.authority.csrf_token,
        )
        headers["Origin"] = self.authority.expected_origin
        with self.assertRaisesRegex(FunctionalHttpSessionError, "duplicated"):
            self.authority.assert_request(
                headers=headers,
                peer_host="127.0.0.1",
                require_origin=True,
            )

    def test_nonloopback_or_hostname_bind_is_rejected_before_socket(self) -> None:
        for host in (
            "0.0.0.0",
            "::",
            "localhost",
            "192.0.2.1",
            "",
            " 127.0.0.1",
            "127.0.0.1 ",
        ):
            with (
                self.subTest(host=host),
                patch(
                    "live_trader.server._assert_application_instance_lease_held"
                ) as lease,
                patch("live_trader.server.ThreadingHTTPServer") as server,
                self.assertRaises(FunctionalHttpSessionError),
            ):
                bind_server(host, 18795)
            lease.assert_not_called()
            server.assert_not_called()

    def test_bound_loopback_bootstrap_sets_cookie_then_protects_status_and_post(
        self,
    ) -> None:
        with patch(
            "live_trader.server._assert_application_instance_lease_held"
        ):
            server = bind_server("127.0.0.1", 0)
        authority = server.functional_http_session_authority  # type: ignore[attr-defined]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar)
        )
        try:
            with opener.open(authority.native_bootstrap_url, timeout=3) as response:
                self.assertEqual(authority.expected_origin + "/", response.url)
            self.assertEqual(1, len(list(jar)))
            self.assertNotIn(authority.bootstrap_nonce, repr(list(jar)))

            expected = {
                "prepared": True,
                "available": False,
                "networkOrderPostAllowed": False,
            }
            with patch(
                "live_trader.server.state."
                "binance_spot_functional_backend_state_status",
                return_value=expected,
            ) as status:
                with opener.open(
                    authority.expected_origin
                    + "/api/binance-spot-functional/status",
                    timeout=3,
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(expected, payload)
            status.assert_called_once_with()

            hostile = urllib.request.Request(
                authority.expected_origin
                + "/api/safety-confirmation/challenge",
                data=b'{"action":"BINANCE_SPOT_FUNCTIONAL_START"}',
                headers={
                    "Content-Type": "application/json",
                    "Origin": "https://evil.example",
                },
                method="POST",
            )
            with patch(
                "live_trader.server.state.issue_safety_confirmation"
            ) as issue, self.assertRaises(urllib.error.HTTPError) as blocked:
                opener.open(hostile, timeout=3)
            self.assertEqual(403, blocked.exception.code)
            issue.assert_not_called()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
