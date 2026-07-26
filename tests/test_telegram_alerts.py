from __future__ import annotations

import copy
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from live_trader import state
from trading_runtime.telegram_notifications import TelegramDispatcher


class LiveTelegramAuditAlertTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    def test_danger_audit_queues_critical_alert(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("danger", "모드 전환 차단", "readiness blocker 2개")

        send.assert_called_once()
        self.assertIn("모드 전환 차단", send.call_args.args[0])
        self.assertEqual("critical", send.call_args.kwargs["severity"])
        self.assertEqual(600, send.call_args.kwargs["dedupe_seconds"])

    def test_recovery_and_safety_events_queue_warning_alerts(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("info", "Recovery Drill", "복구 훈련 통과")
            state.append_audit("info", "Watchdog", "정상: critical 0 / warning 0")

        self.assertEqual(2, send.call_count)
        self.assertTrue(all(call.kwargs["severity"] == "warning" for call in send.call_args_list))

    def test_identical_alert_is_deduplicated_without_network(self) -> None:
        sent: list[str] = []
        environment = {
            "TELEGRAM_ENABLED": "true",
            "TELEGRAM_BOT_TOKEN": "test-token",
            "TELEGRAM_CHAT_ID": "test-chat",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            dispatcher = TelegramDispatcher(
                "live_trader",
                sender=lambda _settings, text: sent.append(text),
            )
            with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
                state,
                "TELEGRAM_DISPATCHER",
                dispatcher,
            ):
                state.append_audit("danger", "Watchdog Fail Closed", "동일한 차단")
                state.append_audit("danger", "Watchdog Fail Closed", "동일한 차단")
                dispatcher.queue.join()

        self.assertEqual(1, len(sent))

    def test_routine_audit_does_not_queue_telegram(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("info", "체결 이벤트 동기화", "신규 체결 이벤트 1건")
            state.append_audit("info", "모드 전환", "운용 모드가 MONITOR로 변경")

        send.assert_not_called()

    def test_telegram_queue_failure_does_not_break_audit(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            side_effect=RuntimeError("telegram unavailable"),
        ):
            state.append_audit("error", "주문 취소 실패", "broker timeout")

        self.assertEqual("주문 취소 실패", state.STATE["audit"][-1]["event"])

    def test_missing_checkpoint_fails_closed_and_queues_startup_warning_once(self) -> None:
        state.STATE.update({"mode": "FULL_LIVE", "dry_run": False, "new_entries_blocked": False})
        with tempfile.TemporaryDirectory() as temporary:
            journal = state.RecoveryJournal(Path(temporary) / "recovery")
            with mock.patch.object(state, "RECOVERY_JOURNAL", journal), mock.patch.object(
                state,
                "persist_audit_event",
            ), mock.patch.object(
                state.TELEGRAM_DISPATCHER,
                "send_async",
                return_value=True,
            ) as send:
                restored = state.restore_runtime_from_checkpoint()

        self.assertFalse(restored["verified"])
        self.assertTrue(restored["safeMode"])
        self.assertEqual(0, restored["generation"])
        self.assertEqual("MONITOR", state.STATE["mode"])
        self.assertTrue(state.STATE["dry_run"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertEqual("Startup Recovery", state.STATE["audit"][-1]["event"])
        self.assertEqual("warn", state.STATE["audit"][-1]["level"])
        send.assert_called_once()
        self.assertEqual("warning", send.call_args.kwargs["severity"])
        self.assertEqual(600, send.call_args.kwargs["dedupe_seconds"])

    def test_invalid_checkpoint_fails_closed_and_queues_startup_warning_once(self) -> None:
        state.STATE.update({"mode": "SMALL_LIVE", "dry_run": False, "new_entries_blocked": False})
        with tempfile.TemporaryDirectory() as temporary:
            recovery_dir = Path(temporary) / "recovery"
            recovery_dir.mkdir()
            (recovery_dir / "checkpoint-00000001.json").write_text(
                '{"schemaVersion":"runtime-recovery-v2","generation":1',
                encoding="utf-8",
            )
            journal = state.RecoveryJournal(recovery_dir)
            with mock.patch.object(state, "RECOVERY_JOURNAL", journal), mock.patch.object(
                state,
                "persist_audit_event",
            ), mock.patch.object(
                state.TELEGRAM_DISPATCHER,
                "send_async",
                return_value=True,
            ) as send:
                restored = state.restore_runtime_from_checkpoint()

        self.assertFalse(restored["verified"])
        self.assertTrue(restored["safeMode"])
        self.assertEqual(0, restored["generation"])
        self.assertTrue(any("checkpoint-00000001.json" in item for item in restored["corruptCheckpoints"]))
        self.assertEqual("MONITOR", state.STATE["mode"])
        self.assertTrue(state.STATE["dry_run"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertEqual("Startup Recovery", state.STATE["audit"][-1]["event"])
        send.assert_called_once()

    def test_execution_stream_down_to_recovered_alerts_once(self) -> None:
        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        down = {
            "running": True,
            "brokers": {
                "binance": {
                    "running": True,
                    "connected": False,
                    "lastError": "RuntimeError: disconnected",
                }
            },
        }
        healthy = {
            "running": True,
            "brokers": {
                "binance": {
                    "running": True,
                    "connected": True,
                    "lastError": "",
                }
            },
        }
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.LIVE_EXECUTION_STREAMS,
            "snapshot",
            side_effect=[down, healthy, healthy],
        ), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            self.assertEqual(0, state.observe_execution_stream_connectivity(("binance",)))
            self.assertEqual(1, state.observe_execution_stream_connectivity(("binance",)))
            self.assertEqual(0, state.observe_execution_stream_connectivity(("binance",)))

        send.assert_called_once()
        self.assertIn("연결 복구", send.call_args.args[0])
        self.assertIn("상태 전이 1회", send.call_args.args[0])

    def test_steady_healthy_connectivity_does_not_alert(self) -> None:
        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            self.assertFalse(state.record_connectivity_state("broker_api", "upbit", healthy=True))
            self.assertFalse(state.record_connectivity_state("broker_api", "upbit", healthy=True))

        send.assert_not_called()

    def test_broker_poll_failure_then_recovery_alerts_once(self) -> None:
        class FakeRouter:
            def __init__(self) -> None:
                self.calls = 0

            def poll_execution_events(self, broker_id: str) -> dict[str, object]:
                self.calls += 1
                if self.calls == 1:
                    raise state.BrokerNotReadyError(f"{broker_id} temporary outage")
                return {
                    "broker_id": broker_id,
                    "events": [],
                    "accounts": [],
                    "positions": [],
                }

        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        router = FakeRouter()
        original_ledger = state.PROGRAM_LEDGER
        with tempfile.TemporaryDirectory() as temporary:
            state.PROGRAM_LEDGER = state.ProgramLedger(Path(temporary) / "program-ledger.sqlite3")
            try:
                with mock.patch("live_trader.state.LiveBrokerRouter", return_value=router), mock.patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "drain",
                    return_value=[],
                ), mock.patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "snapshot",
                    return_value={"running": False, "brokers": {}},
                ), mock.patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "snapshot",
                    return_value={"running": False, "profiles": {}},
                ), mock.patch.object(
                    state,
                    "snapshot",
                    return_value={},
                ), mock.patch.object(
                    state,
                    "persist_audit_event",
                ), mock.patch.object(
                    state.TELEGRAM_DISPATCHER,
                    "send_async",
                    return_value=True,
                ) as send:
                    failed = state.poll_execution_events("binance", force_snapshot=True)
                    recovered = state.poll_execution_events("binance", force_snapshot=True)
                    steady = state.poll_execution_events("binance", force_snapshot=True)
            finally:
                state.PROGRAM_LEDGER = original_ledger

        self.assertFalse(failed["ok"])
        self.assertTrue(recovered["ok"])
        self.assertTrue(steady["ok"])
        recovery_calls = [call for call in send.call_args_list if "연결 복구" in call.args[0]]
        self.assertEqual(1, len(recovery_calls))
        self.assertIn("BINANCE 체결/계좌 API", recovery_calls[0].args[0])

    def test_connectivity_recovery_telegram_failure_is_isolated(self) -> None:
        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        self.assertFalse(state.record_connectivity_state("broker_api", "kis", healthy=False))
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            side_effect=RuntimeError("telegram unavailable"),
        ) as send:
            self.assertTrue(state.record_connectivity_state("broker_api", "kis", healthy=True))
            self.assertFalse(state.record_connectivity_state("broker_api", "kis", healthy=True))

        send.assert_called_once()
        self.assertEqual("체결 이벤트 동기화", state.STATE["audit"][-1]["event"])
        self.assertIn("연결 복구", state.STATE["audit"][-1]["detail"])


if __name__ == "__main__":
    unittest.main()
