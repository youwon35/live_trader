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

    def test_marked_safety_audit_queues_critical_alert(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("danger", "Watchdog Fail Closed", "실거래 중지", telegram_alert=True)

        send.assert_called_once()
        self.assertIn("Watchdog Fail Closed", send.call_args.args[0])
        self.assertEqual("critical", send.call_args.kwargs["severity"])
        self.assertEqual(3600, send.call_args.kwargs["dedupe_seconds"])

    def test_recovery_drill_and_normal_watchdog_stay_quiet(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("info", "Recovery Drill", "복구 훈련 통과")
            state.append_audit("info", "Watchdog", "정상: critical 0 / warning 0")

        send.assert_not_called()

    def test_warning_in_audit_alone_does_not_trigger_external_alert(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("warn", "Watchdog", "체결 이벤트 동기화 지연")

        send.assert_not_called()

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
                state.append_audit("danger", "Watchdog Fail Closed", "동일한 차단", telegram_alert=True)
                state.append_audit("danger", "Watchdog Fail Closed", "동일한 차단", telegram_alert=True)
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
            state.append_audit("danger", "계좌 조회", "API 읽기 응답 없음")
            state.append_audit("danger", "모드 전환 차단", "readiness blocker 2개")
            state.append_audit("info", "체결 스트림", "binance 연결 복구")

        send.assert_not_called()
        self.assertTrue(any(x["event"] == "계좌 조회" for x in state.STATE["audit"]))

    def test_telegram_queue_failure_does_not_break_audit(self) -> None:
        with mock.patch.object(state, "persist_audit_event"), mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            side_effect=RuntimeError("telegram unavailable"),
        ):
            state.append_audit("error", "주문 취소 실패", "broker timeout", telegram_alert=True)

        self.assertEqual("주문 취소 실패", state.STATE["audit"][-1]["event"])

    def test_audit_and_telegram_mask_credentials_but_keep_order_ids(self) -> None:
        secrets = {
            "BINANCE_API_KEY": "binance-secret-value",
            "KIS_ACCOUNT_NO": "12345678",
            "TELEGRAM_BOT_TOKEN": "123456:telegram-secret",
        }
        detail = (
            "api_secret=inline-secret Authorization: Bearer bearer-secret "
            "KIS_ACCOUNT_NO=12345678 "
            "https://api.telegram.org/bot123456:telegram-secret/sendMessage "
            "order_id=ord-live-1 client_order_id=client-123"
        )
        with mock.patch.dict(os.environ, secrets, clear=False), mock.patch.object(
            state,
            "persist_audit_event",
        ) as persist, mock.patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=True,
        ) as send:
            state.append_audit("error", "주문 취소 실패", detail, telegram_alert=True)

        rendered = "\n".join(
            (
                state.STATE["audit"][-1]["detail"],
                persist.call_args.args[0].message,
                send.call_args.args[0],
                send.call_args.kwargs["dedupe_key"],
            )
        )
        for secret in (
            "inline-secret",
            "bearer-secret",
            "12345678",
            "123456:telegram-secret",
            "binance-secret-value",
        ):
            self.assertNotIn(secret, rendered)
        self.assertIn("order_id=ord-live-1", rendered)
        self.assertIn("client_order_id=client-123", rendered)

    def test_missing_first_checkpoint_fails_closed_without_alert(self) -> None:
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
        send.assert_not_called()

    def test_invalid_checkpoint_with_pending_order_queues_startup_warning_once(self) -> None:
        state.STATE.update({"mode": "SMALL_LIVE", "dry_run": False, "new_entries_blocked": False})
        with tempfile.TemporaryDirectory() as temporary:
            recovery_dir = Path(temporary) / "recovery"
            recovery_dir.mkdir()
            (recovery_dir / "checkpoint-00000001.json").write_text(
                '{"schemaVersion":"runtime-recovery-v2","generation":1',
                encoding="utf-8",
            )
            journal = state.RecoveryJournal(recovery_dir)
            pending_real_order = {"order_id": "recorded-broker-order", "state": "unknown", "dry_run": False}
            with mock.patch.object(state, "RECOVERY_JOURNAL", journal), mock.patch.object(
                state, "durable_dispatch_orders_for_restore", return_value=[pending_real_order],
            ), mock.patch.object(state, "persist_audit_event"), mock.patch.object(
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

    def test_execution_stream_down_to_recovered_is_logged_without_telegram(self) -> None:
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

        send.assert_not_called()
        self.assertIn("연결 복구", state.STATE["audit"][-1]["detail"])
        self.assertIn("상태 전이 1회", state.STATE["audit"][-1]["detail"])

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

    def test_broker_poll_failure_then_recovery_stays_local(self) -> None:
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
        send.assert_not_called()
        recovery = [row for row in state.STATE["audit"] if "연결 복구" in row["detail"]]
        self.assertEqual(1, len(recovery))
        self.assertIn("BINANCE 체결/계좌 API", recovery[0]["detail"])

    def test_connectivity_recovery_does_not_touch_dispatcher(self) -> None:
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

        send.assert_not_called()
        self.assertEqual("체결 이벤트 동기화", state.STATE["audit"][-1]["event"])
        self.assertIn("연결 복구", state.STATE["audit"][-1]["detail"])


    def test_only_real_unknown_or_rejected_order_is_urgent(self) -> None:
        order = {"order_id": "ord-urgent", "broker_id": "binance", "dry_run": False}
        with mock.patch.object(state.TELEGRAM_DISPATCHER, "send_async", return_value=True) as send:
            for normal in ("acknowledged", "adapter_blocked", "risk_blocked", "cancel_pending"):
                self.assertFalse(state.queue_live_order_failure_notification({**order, "state": normal}, reason="test"))
            self.assertFalse(state.queue_live_order_failure_notification(
                {**order, "state": "unknown", "dry_run": True}, reason="simulated"
            ))
            self.assertTrue(state.queue_live_order_failure_notification({**order, "state": "unknown"}, reason="broker timed out"))
            self.assertTrue(state.queue_live_order_failure_notification({**order, "state": "broker_rejected"}, reason="broker reject"))

        self.assertEqual(2, send.call_count)
        unknown, rejected = send.call_args_list
        self.assertEqual("critical", unknown.kwargs["severity"])
        self.assertFalse(unknown.kwargs["message_final"])
        self.assertEqual("warning", rejected.kwargs["severity"])
        self.assertTrue(rejected.kwargs["message_final"])

    def test_unfilled_and_seen_events_do_not_read_the_ledger_for_notifications(self) -> None:
        with mock.patch.object(state.PROGRAM_LEDGER, "position_rows") as positions, mock.patch.object(
            state.PROGRAM_LEDGER, "cash_rows"
        ) as cash, mock.patch.object(state.TELEGRAM_DISPATCHER, "send_async") as send:
            count = state.notify_new_live_fills([
                {"event_id": "old-fill", "state": "filled", "quantity": 5},
                {"event_id": "ack", "state": "acknowledged", "quantity": 5},
                {"event_id": "zero-fill", "state": "filled", "quantity": 0},
            ], {"old-fill"})

        self.assertEqual(0, count)
        positions.assert_not_called()
        cash.assert_not_called()
        send.assert_not_called()

    def test_native_toss_terminal_fill_notifies_once_after_durable_reconciliation(self) -> None:
        from live_trader.toss_cash_ledger import TossCashLedger
        from live_trader.toss_cash_operation import TossCashOperation

        with tempfile.TemporaryDirectory() as temporary:
            worker = TossCashOperation.__new__(TossCashOperation)
            worker.owner = "live-runtime:toss-test"
            worker.ledger = TossCashLedger(Path(temporary) / "toss.sqlite3")
            worker.ledger.initialize(worker.owner, {}, {"KRW": "10000"})
            request = {
                "symbol": "005930", "side": "BUY", "quantity": "2", "price": "101",
                "currency": "KRW", "orderType": "LIMIT", "timeInForce": "DAY",
            }
            worker.ledger.register(worker.owner, "order-client", request, "instance-1")
            worker.ledger.acknowledge(worker.owner, "order-client", {"orderId": "toss-1", "clientOrderId": "order-client"})
            worker.api = mock.Mock()
            worker.api.get_order.return_value = {
                **request, "orderId": "toss-1", "status": "CANCELED",
                "execution": {
                    "filledQuantity": "1", "filledAmount": "101", "commission": "0",
                    "tax": "0", "filledAt": "2026-11-26T09:00:00+09:00",
                },
            }
            def send_after_commit(*_args, **_kwargs):
                self.assertEqual(1, worker.ledger.project(worker.owner)["fillCount"])
                return True

            with mock.patch.object(state.TELEGRAM_DISPATCHER, "send_async", side_effect=send_after_commit) as send:
                worker.reconcile()
                worker.reconcile()

            send.assert_called_once()
            self.assertIn("toss-1", send.call_args.args[0])
            self.assertIn("005930", send.call_args.args[0])
            self.assertEqual("trade", send.call_args.kwargs["event_type"])
            worker.api.get_order.assert_called_once_with("toss-1")


if __name__ == "__main__":
    unittest.main()
