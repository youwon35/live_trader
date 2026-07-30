from __future__ import annotations

import copy
import re
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.order_management import OrderIntent
from live_trader.program_ledger import ProgramLedger
from live_trader.risk_engine import PreTradeRiskReport, RiskCheck


class OrderDispatchSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.original_ledger = state.PROGRAM_LEDGER
        self.original_recovery = state.RECOVERY_JOURNAL
        self.original_trace_store = state.DECISION_TRACE_STORE
        self.original_oms = state.LIVE_OMS
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.ledger_path = root / "program-ledger.sqlite3"
        self.ledger = ProgramLedger(self.ledger_path)
        state.PROGRAM_LEDGER = self.ledger
        state.RECOVERY_JOURNAL = state.RecoveryJournal(
            root / "recovery-journal"
        )
        state.DECISION_TRACE_STORE = state.DecisionTraceStore(
            root / "decision-trace.jsonl"
        )
        state.LIVE_OMS = state.OrderManagementSystem()
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["persisted_idempotency_keys"] = []
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False

    def tearDown(self) -> None:
        state.PROGRAM_LEDGER = self.original_ledger
        state.RECOVERY_JOURNAL = self.original_recovery
        state.DECISION_TRACE_STORE = self.original_trace_store
        state.LIVE_OMS = self.original_oms
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        self.temporary.cleanup()

    @staticmethod
    def passing_report() -> PreTradeRiskReport:
        return PreTradeRiskReport(
            datetime.now(),
            (RiskCheck("base", "pass", "ok"),),
        )

    @staticmethod
    def intent(
        mode: str = "SMALL_LIVE",
        *,
        target_revision: int = 1,
    ) -> OrderIntent:
        return OrderIntent(
            strategy_id="dispatch-safety",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=70_000,
            mode=mode,
            reason="dispatch safety regression",
            metadata={
                "broker_id": "kis",
                "portfolio_id": "dispatch-safety-portfolio",
                "strategy_instance_id": "dispatch-safety",
                "instrument_id": "KRX:005930",
                "target_revision": target_revision,
                "confirmed_bar_end": (
                    f"2026-07-31T00:00:{target_revision:02d}+00:00"
                ),
                "order_type": "01",
            },
        )

    def submit_with_passing_gate(
        self,
        intent: OrderIntent,
    ) -> dict:
        passing = self.passing_report()
        with (
            patch.object(
                state,
                "evaluate_order_gate_with_report",
                return_value=(
                    True,
                    "approved",
                    "ready",
                    "passed",
                    passing,
                ),
            ),
            patch.object(
                state,
                "snapshot",
                return_value={"summary": {}},
            ),
        ):
            return state.submit_order_intent(
                {"summary": {}},
                intent,
                dry_run=False,
                audit_event="dispatch safety",
            )

    def test_non_live_intents_never_reach_broker_dispatch(self) -> None:
        for index, mode in enumerate(
            ("PAPER", "SHADOW", "MONITOR", "OFF"),
            start=1,
        ):
            with self.subTest(mode=mode):
                router = Mock()
                with patch.object(
                    state,
                    "LiveBrokerRouter",
                    return_value=router,
                ):
                    result = self.submit_with_passing_gate(
                        self.intent(mode, target_revision=100 + index)
                    )
                self.assertFalse(result["ok"])
                self.assertEqual(
                    "non-live-intent-broker-dispatch-forbidden",
                    result["reason"],
                )
                self.assertEqual(
                    "risk_blocked",
                    result["order"]["state"],
                )
                router.place_order.assert_not_called()
                self.assertEqual(
                    [],
                    self.ledger.order_dispatch_rows(),
                )

    def test_checkpoint_exists_before_kis_dispatch_and_terminal_is_saved(
        self,
    ) -> None:
        router = Mock()

        def place_order(payload: dict) -> dict:
            pending = self.ledger.order_dispatch_for_idempotency_key(
                str(payload["identifier"])
            )
            self.assertIsNotNone(pending)
            self.assertEqual("dispatch_pending", pending["state"])
            self.assertEqual(
                "reconcile_required",
                pending["queue_state"],
            )
            return {
                "ok": True,
                "statusCode": 200,
                "json": {"output": {"ODNO": "KIS-ACK-1"}},
            }

        router.place_order.side_effect = place_order
        with patch.object(
            state,
            "LiveBrokerRouter",
            return_value=router,
        ):
            result = self.submit_with_passing_gate(
                self.intent(target_revision=201)
            )

        self.assertTrue(result["ok"])
        self.assertEqual("acknowledged", result["order"]["state"])
        durable = self.ledger.order_dispatch_for_idempotency_key(
            result["order"]["idempotency_key"]
        )
        self.assertEqual("acknowledged", durable["state"])
        self.assertEqual("KIS-ACK-1", durable["broker_order_id"])
        self.assertEqual(
            result["order"]["idempotency_key"],
            durable["broker_request"]["identifier"],
        )

    def test_dispatch_exception_stays_unknown_and_restart_blocks_duplicate(
        self,
    ) -> None:
        first_router = Mock()
        first_router.place_order.side_effect = RuntimeError(
            "connection dropped after send"
        )
        intent = self.intent(target_revision=202)
        with patch.object(
            state,
            "LiveBrokerRouter",
            return_value=first_router,
        ):
            first = self.submit_with_passing_gate(intent)

        self.assertFalse(first["ok"])
        self.assertEqual("unknown", first["order"]["state"])
        self.assertEqual(
            "reconcile_required",
            first["order"]["queue_state"],
        )
        durable = self.ledger.order_dispatch_for_idempotency_key(
            first["order"]["idempotency_key"]
        )
        self.assertEqual("unknown", durable["state"])

        # Simulate a new process: no in-memory order, OMS, or recovered key.
        state.STATE["orders"] = []
        state.STATE["persisted_idempotency_keys"] = []
        state.LIVE_OMS = state.OrderManagementSystem()
        state.PROGRAM_LEDGER = ProgramLedger(self.ledger_path)
        second_router = Mock()
        with patch.object(
            state,
            "LiveBrokerRouter",
            return_value=second_router,
        ):
            second = self.submit_with_passing_gate(intent)
        self.assertFalse(second["ok"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(
            "durable-dispatch-idempotency-key-exists",
            second["reason"],
        )
        second_router.place_order.assert_not_called()

    def test_gate_evidence_is_append_only(self) -> None:
        order = {
            "order_id": "LIVE-ORDER-LEGACY-0001",
            "strategy_id": "strategy",
            "broker_id": "kis",
            "mode": "SMALL_LIVE",
            "dry_run": False,
            "state": "dispatch_pending",
            "created_at": "2026-07-31 00:00:00",
        }
        self.ledger.record_order_gate_event(order)
        self.ledger.record_order_gate_event(
            {**order, "state": "acknowledged"}
        )
        rows = self.ledger.order_gate_event_rows()
        self.assertEqual(2, len(rows))
        self.assertEqual(
            {"dispatch_pending", "acknowledged"},
            {row["state"] for row in rows},
        )
        self.assertEqual(2, len({row["event_id"] for row in rows}))

    def test_order_ids_keep_legacy_prefix_but_never_reuse_local_length(
        self,
    ) -> None:
        first = state.next_order_id("approved", False)
        state.STATE["orders"] = []
        second = state.next_order_id("approved", False)
        pattern = re.compile(r"^LIVE-ORDER-[0-9A-F]{32}$")
        self.assertRegex(first, pattern)
        self.assertRegex(second, pattern)
        self.assertNotEqual(first, second)

    def test_pending_dispatch_restores_without_recovery_checkpoint(
        self,
    ) -> None:
        pending = {
            "order_id": "LIVE-ORDER-LEGACY-0002",
            "idempotency_key": "persisted-pending-key",
            "broker_id": "kis",
            "broker_order_id": "-",
            "state": "dispatch_pending",
            "queue_state": "reconcile_required",
            "created_at": "2026-07-31 00:00:00",
            "updated_at": "2026-07-31 00:00:00",
        }
        self.ledger.checkpoint_order_dispatch(pending)
        restored = state.restore_runtime_from_checkpoint()
        self.assertTrue(restored["safeMode"])
        self.assertEqual("MONITOR", state.STATE["mode"])
        self.assertEqual(
            "LIVE-ORDER-LEGACY-0002",
            state.STATE["orders"][0]["order_id"],
        )
        self.assertIn(
            "persisted-pending-key",
            state.STATE["persisted_idempotency_keys"],
        )

    def test_non_finite_futures_soak_duration_is_fail_closed(self) -> None:
        scope = {
            field: f"value-{field}"
            for field in state.CANARY_SCOPE_FIELDS
        }
        base = {
            "runId": "non-finite",
            "status": "STOPPED",
            "verdict": "PASS",
            "durationSeconds": 18_000,
            "targetDurationSeconds": 18_000,
            "endedAt": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "profiles": ["crypto"],
                "strategyScopes": [dict(scope)],
            },
        }
        for field, value, blocker in (
            ("durationSeconds", float("nan"), "soak-duration-invalid"),
            ("durationSeconds", float("inf"), "soak-duration-invalid"),
            (
                "targetDurationSeconds",
                float("nan"),
                "soak-target-duration-invalid",
            ),
            (
                "targetDurationSeconds",
                float("inf"),
                "soak-target-duration-invalid",
            ),
        ):
            with self.subTest(field=field, value=value):
                assessment = state._accepted_futures_soak_report(
                    {**base, field: value},
                    scope,
                )
                self.assertFalse(assessment["accepted"])
                self.assertIn(blocker, assessment["blockers"])


if __name__ == "__main__":
    unittest.main()
