from __future__ import annotations

import json
import math
import unittest
from unittest.mock import Mock, call, patch

from live_trader.server import (
    create_desktop_server,
    is_recoverable_bind_error,
    json_safe_value,
    requests_real_order_enable,
    start_in_thread,
)


class ServerSafetyTests(unittest.TestCase):
    def test_non_finite_metrics_are_serialized_as_null(self) -> None:
        payload = {
            "finite": 12.5,
            "nested": [math.inf, -math.inf, math.nan, {"value": 3}],
        }

        safe = json_safe_value(payload)
        encoded = json.dumps(safe, allow_nan=False)

        self.assertEqual(safe["finite"], 12.5)
        self.assertEqual(safe["nested"][:3], [None, None, None])
        self.assertIn('"value": 3', encoded)

    def test_real_order_enable_request_is_detected_before_env_write(self) -> None:
        self.assertTrue(requests_real_order_enable({"values": {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}}))
        self.assertFalse(requests_real_order_enable({"values": {"LIVE_TRADER_ENABLE_REAL_ORDERS": "false"}}))
        self.assertFalse(requests_real_order_enable({"values": {"KIS_ACCOUNT_NO": "12345678"}}))

    def test_windows_access_denied_bind_error_is_recoverable(self) -> None:
        error = PermissionError(13, "socket access denied")

        self.assertTrue(is_recoverable_bind_error(error))

    @patch("live_trader.server.prepare_server_state")
    @patch("live_trader.server.bind_server")
    def test_desktop_server_falls_back_to_os_assigned_port(
        self,
        bind_server: Mock,
        prepare_server_state: Mock,
    ) -> None:
        fallback_server = Mock(server_port=54321)
        bind_server.side_effect = [PermissionError(13, "socket access denied"), fallback_server]

        server = create_desktop_server("127.0.0.1", 18795)

        self.assertIs(server, fallback_server)
        prepare_server_state.assert_called_once_with()
        self.assertEqual(
            bind_server.call_args_list,
            [call("127.0.0.1", 18795), call("127.0.0.1", 0)],
        )

    @patch("live_trader.server.prepare_server_state")
    @patch("live_trader.server.bind_server")
    def test_desktop_server_does_not_hide_unrelated_socket_errors(
        self,
        bind_server: Mock,
        _prepare_server_state: Mock,
    ) -> None:
        bind_server.side_effect = OSError(22, "invalid argument")

        with self.assertRaises(OSError):
            create_desktop_server("127.0.0.1", 18795)

        bind_server.assert_called_once_with("127.0.0.1", 18795)

    @patch("live_trader.server.start_watchdog_thread")
    @patch("live_trader.server.threading.Thread")
    @patch("live_trader.server.create_desktop_server")
    def test_threaded_server_url_uses_the_actual_bound_port(
        self,
        create_desktop_server_mock: Mock,
        thread_class: Mock,
        _start_watchdog_thread: Mock,
    ) -> None:
        server = Mock(server_port=54321)
        create_desktop_server_mock.return_value = server

        returned_server, url = start_in_thread("127.0.0.1", 18795)

        self.assertIs(returned_server, server)
        self.assertEqual(url, "http://127.0.0.1:54321")
        thread_class.assert_called_once_with(target=server.serve_forever, daemon=True)
        thread_class.return_value.start.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
