"""In-process HTTP boundary tests. No listener, account or real order calls."""
from email.message import Message
from io import BytesIO
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader.functional_http_session import APP_SESSION_COOKIE, CSRF_HEADER, FunctionalHttpSessionAuthority
from live_trader.server import LiveTraderHandler
from live_trader.short_live_runtime import FreshClock, NativeFeed


class ShortLiveHttpTests(unittest.TestCase):
    def handler(self, path, *, authorized=True):
        authority = FunctionalHttpSessionAuthority.mint(host="127.0.0.1", port=18795)
        authority.trusted_native_bootstrap()
        handler = object.__new__(LiveTraderHandler)
        handler.path = path
        handler.client_address = ("127.0.0.1", 40000)
        handler.server = SimpleNamespace(functional_http_session_authority=authority)
        handler.headers = Message()
        handler.headers["Host"] = authority.expected_host_header
        handler.headers["Origin"] = authority.expected_origin
        handler.headers["Content-Type"] = "application/json"
        if authorized:
            handler.headers["Cookie"] = f"{APP_SESSION_COOKIE}={authority.app_session_token}"
            handler.headers[CSRF_HEADER] = authority.csrf_token
        handler.read_json = Mock(return_value={"confirmed": True})
        handler.send_json = Mock()
        handler._send_functional_http_denial = Mock()
        return handler

    def test_each_mutation_is_authenticated_before_reading_body(self):
        for action in ("prepare", "start", "stop"):
            h = self.handler("/api/short-live-tests/"+action, authorized=False)
            with patch("live_trader.server.state.short_live_test_request") as call:
                h.do_POST()
            h._send_functional_http_denial.assert_called_once()
            h.read_json.assert_not_called()
            call.assert_not_called()

    def test_exact_manual_request_reaches_only_requested_action(self):
        for action in ("prepare", "start", "stop"):
            h = self.handler("/api/short-live-tests/"+action)
            with patch("live_trader.server.state.short_live_test_request", return_value={"ok": True}) as call:
                h.do_POST()
            h._send_functional_http_denial.assert_not_called()
            call.assert_called_once_with(action, {"confirmed": True})

    def test_cross_origin_query_absolute_and_preflight_rejected(self):
        h = self.handler("/api/short-live-tests/start")
        h.headers.replace_header("Origin", "https://outside.example")
        h.do_POST()
        h.read_json.assert_not_called()
        h._send_functional_http_denial.assert_called_once()
        h = self.handler("/api/short-live-tests/start?retry=1")
        with patch("live_trader.server.state.short_live_test_request") as call:
            h.do_POST()
        call.assert_not_called()
        self.assertFalse(h.send_json.call_args.args[0]["ok"])
        h = self.handler("http://127.0.0.1:18795/api/short-live-tests/start")
        h.do_POST()
        h.read_json.assert_not_called()
        h = self.handler("/api/short-live-tests/start")
        h.do_OPTIONS()
        h._send_functional_http_denial.assert_called_once()

    def test_status_requires_native_session_and_never_prepares(self):
        h = self.handler("/api/short-live-tests/snapshot", authorized=False)
        with patch("live_trader.server.state.short_live_test_request") as call:
            h.do_GET()
        call.assert_not_called()
        h = self.handler("/api/short-live-tests/snapshot")
        with patch("live_trader.server.state.short_live_test_request", return_value={"ok": True}) as call:
            h.do_GET()
        call.assert_called_once_with("snapshot")

    def test_clock_failure_is_sticky_without_os_mutation(self):
        reader = SimpleNamespace(_get=Mock(side_effect=ValueError("unavailable")))
        guard = FreshClock(reader, lambda: [])
        self.assertFalse(guard({})["ready"])
        self.assertFalse(guard({})["ready"])
        self.assertEqual(1, reader._get.call_count)

    def test_feed_retains_open_boundary_and_proves_disconnect(self):
        feed = object.__new__(NativeFeed)
        event = object()
        child = SimpleNamespace(poll=Mock(return_value=[event]), disconnect=Mock())
        feed.children = [child]
        self.assertEqual([event], feed.poll())
        feed.close()
        child.disconnect.assert_called_once()
        child.disconnect.side_effect = OSError("held socket")
        with self.assertRaises(RuntimeError):
            feed.close()

    def test_each_preparation_uses_new_adapter_and_current_settings(self):
        from live_trader import short_live_runtime as runtime
        with patch.object(runtime, "_SERVICE", None), \
             patch.object(runtime, "_settings", side_effect=[{"key": "initial"}, {"key": "first"}, {"key": "second"}]), \
             patch.object(runtime, "ShortLiveTestService", side_effect=lambda *a, **k: SimpleNamespace(provider=a[1])), \
             patch("live_trader.short_live_brokers.create_production_short_live_broker", side_effect=[object(), object()]) as factory, \
             patch("live_trader.read_only_account.ReadOnlyBrokerReader"), \
             patch("live_trader.short_live_broker_readers.production_fx_reader"), \
             patch("live_trader.short_live_broker_readers.production_cost_reader"):
            service = runtime.get_service()
            first = service.provider("KIS_KR_CASH")
            second = service.provider("KIS_KR_CASH")
            self.assertIsNot(first, second)
            self.assertEqual([{"key": "first"}, {"key": "second"}],
                             [c.kwargs["settings"] for c in factory.call_args_list])

    def test_personal_kis_cost_settings_have_no_invented_default(self):
        from live_trader.short_live_runtime import KIS_COST_KEYS, _settings
        with patch.dict(os.environ, {}, clear=True):
            result = _settings()
        self.assertTrue(all(result[key] == "" for key in KIS_COST_KEYS))


if __name__ == "__main__":
    unittest.main()
