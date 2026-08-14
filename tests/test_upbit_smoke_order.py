from __future__ import annotations

import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.audit_store import SQLiteAuditEventStore


class UpbitSmokeOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.original_real_orders_process_armed = state._REAL_ORDERS_PROCESS_ARMED
        self.original_audit_store = state.AUDIT_STORE
        self.original_audit_path = Path(state.AUDIT_STORE.path)
        self.original_flight_path = self._flight_recorder_path()
        self.temporary = tempfile.TemporaryDirectory()
        self.temporary_root = Path(self.temporary.name)
        state.AUDIT_STORE = SQLiteAuditEventStore(
            self.temporary_root / "live_trader_audit.sqlite3"
        )
        # These smoke-order tests exercise the post-confirmation broker gates.
        # Production starts disarmed; the fixture explicitly models a process
        # that has already completed the REAL_ORDERS_ENABLE confirmation.
        state._REAL_ORDERS_PROCESS_ARMED = True
        self.env = patch.dict(
            os.environ,
            {
                "UPBIT_ACCESS_KEY": "test-access",
                "UPBIT_SECRET_KEY": "test-secret",
                "UPBIT_BASE_URL": "https://api.upbit.com",
                "LIVE_TRADER_ENABLE_REAL_ORDERS": "false",
                "TRADING_FLIGHT_RECORDER_DIR": str(
                    self.temporary_root / "flight-recorder"
                ),
            },
        )
        self.env.start()
        self.telegram_send_patcher = patch.object(
            state.TELEGRAM_DISPATCHER,
            "send_async",
            return_value=False,
        )
        self.telegram_send = self.telegram_send_patcher.start()
        self.strategy_rows = patch.object(state, "strategy_rows", return_value=[self.strategy()])
        self.strategy_rows.start()
        self.snapshot = patch.object(state, "snapshot", return_value={})
        self.snapshot.start()

    def tearDown(self) -> None:
        self.snapshot.stop()
        self.strategy_rows.stop()
        self.telegram_send_patcher.stop()
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        state._REAL_ORDERS_PROCESS_ARMED = self.original_real_orders_process_armed
        state.AUDIT_STORE = self.original_audit_store
        self.env.stop()
        self.temporary.cleanup()

    @staticmethod
    def _flight_recorder_path() -> Path:
        configured = str(
            os.environ.get("TRADING_FLIGHT_RECORDER_DIR") or ""
        ).strip()
        if configured:
            root = Path(configured)
        else:
            user_root = str(
                os.environ.get("APPDATA")
                or os.environ.get("LOCALAPPDATA")
                or ""
            ).strip()
            root = (
                Path(user_root) if user_root else Path(tempfile.gettempdir())
            ) / "trading_programs" / "flight-recorder"
        return root / "live_trader.jsonl"

    @staticmethod
    def _file_fingerprint(path: Path) -> tuple[bool, int, int, str]:
        if not path.is_file():
            return False, 0, 0, ""
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        stat = path.stat()
        return True, stat.st_size, stat.st_mtime_ns, digest.hexdigest()

    @staticmethod
    def account_snapshot(balance: float = 50_001.0) -> dict[str, object]:
        return {
            "broker_id": "upbit",
            "accounts": [{"broker_id": "upbit", "broker_cash": balance}],
        }

    @staticmethod
    def order_chance(balance: float = 50_001.0) -> dict[str, object]:
        return {
            "bid_fee": "0.0005",
            "market": {"bid": {"min_total": "5000", "max_total": "1000000000"}},
            "bid_account": {"balance": str(balance), "currency": "KRW"},
        }

    @staticmethod
    def strategy() -> dict[str, object]:
        return {
            "strategy_id": "upbit-btc-qualified",
            "symbol": "KRW-BTC",
            "asset": "CRYPTO",
            "dataset_provider": "upbit",
            "broker_id": "upbit",
            "lifecycle_status": "before-live-small",
            "live_small_eligible": True,
            "strategy_instance_id": "upbit-btc-qualified-instance",
            "deployment_id": "upbit-btc-live-deployment",
        }

    def preview(self) -> dict[str, object]:
        with patch.object(state.LiveBrokerRouter, "get_account_snapshot", return_value=self.account_snapshot()), patch.object(
            state.LiveBrokerRouter,
            "get_upbit_order_chance",
            return_value=self.order_chance(),
        ):
            return state.preview_upbit_smoke_order("upbit-btc-qualified", 5000)

    def test_fixture_keeps_original_audit_and_flight_files_unchanged(self) -> None:
        original_audit = self._file_fingerprint(self.original_audit_path)
        original_flight = self._file_fingerprint(self.original_flight_path)

        result = self.preview()
        state.append_audit(
            "danger",
            "Upbit fixture isolation",
            "synthetic test event",
        )

        self.assertTrue(result["ok"])
        self.assertEqual(2, state.AUDIT_STORE.count())
        temporary_flight = (
            self.temporary_root / "flight-recorder" / "live_trader.jsonl"
        )
        self.assertTrue(temporary_flight.is_file())
        self.assertEqual(2, len(temporary_flight.read_text(encoding="utf-8").splitlines()))
        self.telegram_send.assert_called_once()
        self.assertEqual(
            original_audit,
            self._file_fingerprint(self.original_audit_path),
        )
        self.assertEqual(
            original_flight,
            self._file_fingerprint(self.original_flight_path),
        )

    def test_preview_is_read_only_and_builds_exact_market_buy(self) -> None:
        result = self.preview()

        self.assertTrue(result["ok"])
        preview = result["preview"]
        self.assertEqual(preview["market"], "KRW-BTC")
        self.assertEqual(preview["strategy_id"], "upbit-btc-qualified")
        self.assertEqual(preview["notional_krw"], 5000)
        self.assertEqual(preview["status"], "ready")
        self.assertTrue(preview["confirmation_token"])
        self.assertFalse(preview["used"])
        request = preview["request_preview"]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["body"]["side"], "bid")
        self.assertEqual(request["body"]["ord_type"], "price")
        self.assertEqual(request["body"]["price"], "5000")
        self.assertNotIn("volume", request["body"])
        self.assertTrue(request["body"]["identifier"].startswith("lt-smoke-"))

    def test_preview_hard_blocks_more_than_user_cap(self) -> None:
        result = state.preview_upbit_smoke_order("upbit-btc-qualified", 10_001)

        self.assertFalse(result["ok"])
        self.assertIn("10,000", result["reason"])
        self.assertFalse(state.STATE["upbit_smoke_order"]["confirmation_token"])

    def test_preview_requires_approved_upbit_krw_btc_strategy(self) -> None:
        with patch.object(state, "strategy_rows", return_value=[]), patch.object(state, "snapshot", return_value={}):
            result = state.preview_upbit_smoke_order("missing", 5000)

        self.assertFalse(result["ok"])
        self.assertIn("before-live-small", result["reason"])

    def test_submit_requires_action_time_confirmation_and_all_live_gates(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]

        without_confirmation = state.submit_upbit_smoke_order(token, confirmed=False)
        wrong_token = state.submit_upbit_smoke_order("wrong", confirmed=True)

        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = True
        entry_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["new_entries_blocked"] = False
        state.STATE["dry_run"] = True
        dry_run_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["dry_run"] = False
        state.STATE["mode"] = "MONITOR"
        mode_blocked = state.submit_upbit_smoke_order(token, confirmed=True)

        state.STATE["mode"] = "SMALL_LIVE"
        flag_off = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(without_confirmation["ok"])
        self.assertFalse(wrong_token["ok"])
        self.assertFalse(entry_blocked["ok"])
        self.assertIn("신규 진입", entry_blocked["reason"])
        self.assertFalse(dry_run_blocked["ok"])
        self.assertIn("Dry Run", dry_run_blocked["reason"])
        self.assertFalse(mode_blocked["ok"])
        self.assertIn("SMALL_LIVE", mode_blocked["reason"])
        self.assertFalse(flag_off["ok"])
        self.assertIn("LIVE_TRADER_ENABLE_REAL_ORDERS", flag_off["reason"])
        self.assertFalse(state.STATE["upbit_smoke_order"]["used"])

    def test_submit_revalidates_strategy_approval(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False

        with patch.dict(os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}), patch.object(
            state,
            "strategy_rows",
            return_value=[],
        ), patch.object(
            state.LiveBrokerRouter,
            "place_order",
        ) as place_order, patch.object(
            state,
            "snapshot",
            return_value={},
        ):
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(result["ok"])
        self.assertIn("승인이 더 이상 유효하지 않습니다", result["reason"])
        place_order.assert_not_called()
        self.assertFalse(state.STATE["upbit_smoke_order"]["used"])

    def test_successful_submit_consumes_token_once_and_records_fill(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        state.STATE["operator_confirmed"] = True
        order_payload = {
            "uuid": "upbit-order-1",
            "state": "done",
            "paid_fee": "2.5",
            "trades": [{"volume": "0.00004", "funds": "5000"}],
        }
        with patch.dict(os.environ, {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account_snapshot(),
        ), patch.object(
            state,
            "submit_order_intent",
            return_value={
                "ok": True,
                "reason": "broker-acknowledged",
                "order": {
                    "order_id": "LIVE-1",
                    "state": "acknowledged",
                    "broker_order_id": "upbit-order-1",
                    "broker_response": {
                        "ok": True,
                        "statusCode": 201,
                        "json": {
                            "uuid": "upbit-order-1",
                            "state": "wait",
                        },
                    },
                },
                "snapshot": {},
            },
        ) as submit_intent, patch.object(
            state.LiveBrokerRouter,
            "get_upbit_order",
            return_value=order_payload,
        ), patch.object(state, "poll_execution_events", return_value={"ok": True}), patch.object(
            state,
            "run_reconciliation",
            return_value={"ok": True},
        ):
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["order"]["status"], "filled")
        self.assertEqual(result["order"]["broker_order_id"], "upbit-order-1")
        self.assertEqual(result["order"]["executed_funds"], 5000.0)
        self.assertTrue(result["order"]["used"])
        self.assertEqual(result["order"]["confirmation_token"], "")
        submit_intent.assert_called_once()
        sent = submit_intent.call_args.args[1]
        self.assertEqual(sent.symbol, "KRW-BTC")
        self.assertEqual(sent.strategy_id, "upbit-btc-qualified")
        self.assertEqual(sent.metadata["order_type"], "price")
        self.assertEqual(sent.notional, 5000)
        self.assertEqual(submit_intent.call_args.kwargs["dry_run"], False)

        duplicate = state.submit_upbit_smoke_order(token, confirmed=True)
        self.assertFalse(duplicate["ok"])

    def test_submit_blocks_before_place_order_when_operational_binding_is_stale(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        state.STATE["operator_confirmed"] = True
        with patch.dict(
            os.environ,
            {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"},
        ), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account_snapshot(),
        ), patch.object(
            state,
            "submit_order_intent",
            return_value={
                "ok": False,
                "reason": "operational-paper-final-binding-changed",
                "order": {"state": "risk_blocked"},
                "snapshot": {},
            },
        ) as submit_intent, patch.object(
            state.LiveBrokerRouter,
            "place_order",
        ) as place_order:
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(result["ok"])
        self.assertIn("operational-paper-final-binding-changed", result["reason"])
        submit_intent.assert_called_once()
        place_order.assert_not_called()

    def test_submit_requires_operator_confirmation_before_common_dispatch(self) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        state.STATE["operator_confirmed"] = False

        with patch.dict(
            os.environ,
            {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"},
        ), patch.object(
            state,
            "submit_order_intent",
        ) as submit_intent, patch.object(
            state.LiveBrokerRouter,
            "place_order",
        ) as place_order:
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(result["ok"])
        self.assertIn("운용자", result["reason"])
        submit_intent.assert_not_called()
        place_order.assert_not_called()

    def test_common_dispatch_unknown_outcome_is_exposed_for_reconciliation(
        self,
    ) -> None:
        preview = self.preview()["preview"]
        token = preview["confirmation_token"]
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False
        state.STATE["operator_confirmed"] = True

        with patch.dict(
            os.environ,
            {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"},
        ), patch.object(
            state.LiveBrokerRouter,
            "get_account_snapshot",
            return_value=self.account_snapshot(),
        ), patch.object(
            state,
            "submit_order_intent",
            return_value={
                "ok": False,
                "reason": "network-outcome-unknown",
                "order": {
                    "order_id": "LIVE-UNKNOWN-1",
                    "state": "unknown",
                    "queue_state": "reconcile_required",
                },
                "snapshot": {},
            },
        ) as submit_intent, patch.object(
            state.LiveBrokerRouter,
            "place_order",
        ) as direct_place:
            result = state.submit_upbit_smoke_order(token, confirmed=True)

        self.assertFalse(result["ok"])
        self.assertEqual("unknown", result["order"]["status"])
        self.assertIn("조정", result["order"]["status_label"])
        self.assertTrue(result["order"]["used"])
        submit_intent.assert_called_once()
        direct_place.assert_not_called()

    def test_market_buy_cancel_with_trades_is_reported_as_filled_remainder_cancelled(self) -> None:
        state.STATE["upbit_smoke_order"] = {
            "status": "acknowledged",
            "broker_order_id": "upbit-order-cancelled-remainder",
        }
        order_payload = {
            "uuid": "upbit-order-cancelled-remainder",
            "state": "cancel",
            "paid_fee": "2.5",
            "trades": [{"volume": "0.00004", "funds": "4999.104"}],
        }
        with patch.object(state.LiveBrokerRouter, "get_upbit_order", return_value=order_payload), patch.object(
            state,
            "poll_execution_events",
            return_value={"ok": True},
        ), patch.object(state, "run_reconciliation", return_value={"ok": True}):
            result = state.refresh_upbit_smoke_order()

        self.assertTrue(result["ok"])
        self.assertEqual(result["order"]["status"], "filled")
        self.assertEqual(result["order"]["status_label"], "체결·잔여취소")
        self.assertEqual(result["order"]["executed_funds"], 4999.104)


if __name__ == "__main__":
    unittest.main()
