from __future__ import annotations

import json
import math
import unittest
from unittest.mock import Mock, call, patch

from live_trader.server import (
    LiveTraderHandler,
    create_desktop_server,
    is_recoverable_bind_error,
    json_safe_value,
    prepare_server_state,
    requests_real_order_enable,
    start_in_thread,
)


class ServerSafetyTests(unittest.TestCase):
    def test_snapshot_reconnect_is_native_kill_recovery_safe_point(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/snapshot"
        handler.send_json = Mock()
        with (
            patch(
                "live_trader.server.state.recover_durable_emergency_stop"
            ) as recover,
            patch(
                "live_trader.server.state.snapshot",
                return_value={"kill_switch": True},
            ),
            patch(
                "live_trader.soak_monitor.latest_live_soak_report",
                return_value={},
            ),
        ):
            handler.do_GET()

        recover.assert_called_once_with()
        handler.send_json.assert_called_once_with(
            {"kill_switch": True, "soak_report": {}}
        )

    def test_generic_runtime_route_cannot_bypass_functional_challenge(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/runtime/start"
        handler.read_json = Mock(
            return_value={
                "profile_id": "stock",
                "mode": "SMALL_LIVE",
                "execution_purpose": "FUNCTIONAL_TEST",
                "functional_test_context": {"callerControlled": True},
            }
        )
        handler.send_json = Mock()

        with patch(
            "live_trader.server.state.start_continuous_runtime"
        ) as start:
            handler.do_POST()

        start.assert_not_called()
        response = handler.send_json.call_args.args[0]
        self.assertFalse(response["ok"])
        self.assertFalse(response["runtimeStarted"])
        self.assertFalse(response["brokerSubmissionPerformed"])

    def test_server_startup_recovers_independent_emergency_latch(self) -> None:
        with (
            patch(
                "live_trader.server.state.disarm_real_orders_for_process_start"
            ) as disarm,
            patch("live_trader.daemon.read_daemon_status") as daemon_status,
            patch("live_trader.server.state.restore_runtime_from_checkpoint") as restore,
            patch("live_trader.server.state.recover_durable_emergency_stop") as recover,
        ):
            prepare_server_state()

        disarm.assert_called_once_with(persist=True)
        daemon_status.assert_called_once_with(persist=True)
        restore.assert_called_once_with()
        recover.assert_called_once_with()

    def test_preflight_route_forwards_deployment_and_strategy_scope(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/preflight"
        handler.read_json = Mock(
            return_value={
                "deployment_id": "DEPLOY-20260801-01",
                "strategy_id": "STRATEGY-BTC-1H",
            }
        )
        handler.send_json = Mock()
        expected = {
            "ok": True,
            "preflight_snapshot": {
                "deployment_id": "DEPLOY-20260801-01",
                "strategy_id": "STRATEGY-BTC-1H",
            },
        }

        with patch(
            "live_trader.server.state.run_final_preflight",
            return_value=expected,
        ) as run_final_preflight:
            handler.do_POST()

        run_final_preflight.assert_called_once_with(
            "DEPLOY-20260801-01",
            "STRATEGY-BTC-1H",
        )
        handler.send_json.assert_called_once_with(expected)

    def test_runtime_start_route_forwards_exact_deployment_context(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/runtime/start"
        handler.read_json = Mock(
            return_value={
                "profile_id": "crypto",
                "mode": "SMALL_LIVE",
                "portfolio_id": "PORTFOLIO-01",
                "deployment_id": "DEPLOYMENT-01",
                "strategy_id": "STRATEGY-01",
            }
        )
        handler.send_json = Mock()
        expected = {"ok": True, "runtime_session_id": "SESSION-01"}

        with patch(
            "live_trader.server.state.start_continuous_runtime",
            return_value=expected,
        ) as start_runtime:
            handler.do_POST()

        start_runtime.assert_called_once_with(
            "crypto",
            "SMALL_LIVE",
            "PORTFOLIO-01",
            "DEPLOYMENT-01",
            "STRATEGY-01",
            "",
            None,
        )
        handler.send_json.assert_called_once_with(expected)

    def test_incident_transition_route_forwards_only_operator_contract(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/incidents/transition"
        handler.read_json = Mock(
            return_value={
                "incident_id": "incident-01",
                "action": "acknowledge",
                "note": "원인 확인 중",
                "state": "CLIENT-MUST-NOT-CONTROL",
            }
        )
        handler.send_json = Mock()
        expected = {"ok": True, "incident": {"state": "ACKNOWLEDGED"}}

        with patch(
            "live_trader.server.state.transition_operational_incident",
            return_value=expected,
        ) as transition_incident:
            handler.do_POST()

        transition_incident.assert_called_once_with(
            "incident-01",
            "acknowledge",
            "원인 확인 중",
        )
        handler.send_json.assert_called_once_with(expected)

    def test_futures_test_route_accepts_only_confirmation_contract(
        self,
    ) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/binance-futures-canary/test"
        handler.read_json = Mock(
            return_value={
                "confirmation_token": "one-time-token",
                "confirmed": True,
                "symbol": "CLIENT-MUST-NOT-CONTROL",
                "quantity": "999",
            }
        )
        handler.send_json = Mock()
        expected = {"ok": True, "test": {"status": "validated"}}

        with patch(
            "live_trader.server.state.test_binance_futures_canary_order",
            return_value=expected,
        ) as test_order:
            handler.do_POST()

        test_order.assert_called_once_with(
            "one-time-token",
            confirmed=True,
        )
        handler.send_json.assert_called_once_with(expected)

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
