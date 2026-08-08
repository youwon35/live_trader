from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader import state
from live_trader.program_ledger import ProgramLedger


class FunctionalTestPromotionExclusionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    @staticmethod
    def scope() -> dict:
        scope = {
            "schemaVersion": state.CANARY_SCOPE_SCHEMA_VERSION,
            "eligible": True,
        }
        scope.update(
            {field: f"scope-{field}" for field in state.CANARY_SCOPE_FIELDS}
        )
        scope["beforeLiveSmallAt"] = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        ).isoformat()
        return scope

    def test_functional_orders_and_fills_never_enter_canary_summary(self) -> None:
        scope = self.scope()
        occurred_at = datetime.now(timezone.utc).isoformat()
        state.STATE["orders"] = [
            {
                "order_id": "functional-block",
                "strategy_id": "strategy-one",
                "broker_id": "kis",
                "broker_order_id": "F-BLOCK",
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "state": "risk_blocked",
                "created_at": occurred_at,
                "canary_scope": scope,
                "execution_purpose": "FUNCTIONAL_TEST",
                "promotion_eligible": False,
            },
            {
                "order_id": "standard-block",
                "strategy_id": "strategy-one",
                "broker_id": "kis",
                "broker_order_id": "S-BLOCK",
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "state": "risk_blocked",
                "created_at": occurred_at,
                "canary_scope": scope,
            },
        ]
        gate_events = [
            {
                "order_id": "functional-fill",
                "strategy_id": "strategy-one",
                "broker_id": "kis",
                "broker_order_id": "F-FILL",
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "state": "acknowledged",
                "occurred_at": occurred_at,
                "canary_scope": scope,
                "execution_purpose": "FUNCTIONAL_TEST",
                "promotion_eligible": False,
            },
            {
                "order_id": "standard-fill",
                "strategy_id": "strategy-one",
                "broker_id": "kis",
                "broker_order_id": "S-FILL",
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "state": "acknowledged",
                "occurred_at": occurred_at,
                "canary_scope": scope,
                "execution_purpose": "",
                "promotion_eligible": True,
            },
        ]
        execution_events = [
            {
                "event_id": "event-functional",
                "broker_id": "kis",
                "broker_order_id": "F-FILL",
                "state": "filled",
                "quantity": 1,
                "occurred_at": occurred_at,
            },
            {
                "event_id": "event-standard",
                "broker_id": "kis",
                "broker_order_id": "S-FILL",
                "state": "filled",
                "quantity": 1,
                "occurred_at": occurred_at,
            },
        ]

        with patch.object(
            state,
            "current_live_canary_scope",
            return_value=scope,
        ):
            summary = state.live_small_execution_summary(
                "strategy-one",
                order_gate_events=gate_events,
                execution_events=execution_events,
            )

        self.assertEqual(1, summary["blocked"])
        self.assertEqual(1, summary["fills"])
        self.assertEqual(1, summary["successful"])

    def test_order_gate_schema_migrates_and_persists_non_promotion_flags(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "legacy.sqlite3"
            conn = sqlite3.connect(path)
            try:
                conn.execute(
                    """
                    CREATE TABLE order_gate_events (
                        event_id TEXT PRIMARY KEY,
                        order_id TEXT NOT NULL,
                        strategy_id TEXT NOT NULL,
                        broker_id TEXT NOT NULL,
                        broker_order_id TEXT NOT NULL DEFAULT '',
                        mode TEXT NOT NULL DEFAULT '',
                        dry_run INTEGER NOT NULL DEFAULT 0,
                        state TEXT NOT NULL,
                        occurred_at TEXT NOT NULL,
                        canary_scope_json TEXT NOT NULL
                    )
                    """
                )
                conn.commit()
            finally:
                conn.close()
            ledger = ProgramLedger(path)
            ledger.record_order_gate_event(
                {
                    "order_id": "functional-order",
                    "strategy_id": "strategy-one",
                    "broker_id": "kis",
                    "broker_order_id": "KIS-1",
                    "mode": "SMALL_LIVE",
                    "dry_run": False,
                    "state": "acknowledged",
                    "execution_purpose": "FUNCTIONAL_TEST",
                    "promotion_eligible": False,
                    "canary_scope": {},
                }
            )

            row = ledger.order_gate_event_rows()[0]
            self.assertEqual("FUNCTIONAL_TEST", row["execution_purpose"])
            self.assertFalse(row["promotion_eligible"])

    def test_atomic_permit_counter_allows_only_one_concurrent_last_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = ProgramLedger(Path(temporary) / "atomic.sqlite3")

            def reserve(index: int) -> dict:
                return ledger.reserve_functional_test_order(
                    permit_id="permit-concurrent",
                    idempotency_key=f"key-{index}",
                    order_id=f"order-{index}",
                    maximum_orders=1,
                )

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(reserve, range(8)))

            self.assertEqual(
                1,
                sum(result["allowed"] is True for result in results),
            )
            self.assertEqual(
                1,
                ledger.functional_test_reservation_count("permit-concurrent"),
            )

    def test_durable_counter_keeps_more_than_fifty_terminal_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ledger = ProgramLedger(Path(temporary) / "history.sqlite3")
            terminal_states = ("REJECTED", "CANCELED", "UNKNOWN")
            for index in range(51):
                result = ledger.reserve_functional_test_order(
                    permit_id="permit-history",
                    idempotency_key=f"history-key-{index}",
                    order_id=f"history-order-{index}",
                    maximum_orders=100,
                )
                self.assertTrue(result["allowed"])
                ledger.update_functional_test_reservation(
                    f"history-key-{index}",
                    terminal_states[index % len(terminal_states)],
                )

            self.assertEqual(
                51,
                ledger.functional_test_reservation_count("permit-history"),
            )
            rows = ledger.functional_test_reservation_rows("permit-history")
            self.assertEqual(51, len(rows))
            self.assertEqual(
                {"REJECTED", "CANCELED", "UNKNOWN"},
                {row["state"] for row in rows},
            )
            blocked = ledger.reserve_functional_test_order(
                permit_id="permit-history",
                idempotency_key="history-key-51",
                order_id="history-order-51",
                maximum_orders=51,
            )
            self.assertFalse(blocked["allowed"])
            self.assertEqual(
                "functional-test-order-count-reservation-exhausted",
                blocked["reason"],
            )


if __name__ == "__main__":
    unittest.main()
