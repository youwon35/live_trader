from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_trader import state


def orphan_session(lifecycle: str = "RUNNING") -> SimpleNamespace:
    return SimpleNamespace(
        session_id="functional-session-orphan",
        deployment_id="functional-deployment-one",
        lifecycle=lifecycle,
    )


def unbound_assessment() -> dict[str, object]:
    return {
        "allowed": True,
        "blockers": [],
        "runtime": {"active": False, "phase": "STOPPED"},
        "session": {"sessionId": "", "lifecycle": ""},
        "workingOrderCount": 0,
        "kisReconciled": True,
    }


def fresh_reconciliation_state() -> dict[str, object]:
    return {
        "summary": {"fresh": True, "three_way_verified": True},
        "errors": [],
    }


class FunctionalTestRuntimeRecoveryTests(unittest.TestCase):
    def runtime_state(self) -> dict[str, object]:
        return {
            "active_runtime_session_ids": {},
            "new_entries_blocked": False,
            "manual_new_entries_blocked": False,
            "broker_truth_blocked": False,
            "daily_loss_entries_blocked": False,
            "kill_switch": False,
            "kill_switch_rearm_required": False,
            "broker_reconciliation": fresh_reconciliation_state(),
        }

    def test_reconciled_orphan_transitions_failed_before_replacement(self) -> None:
        prior = orphan_session("RUNNING")
        recovered = SimpleNamespace(
            session_id=prior.session_id,
            deployment_id=prior.deployment_id,
            lifecycle="FAILED",
        )
        calls: list[str] = []

        def poll(*_args: object, **_kwargs: object) -> dict[str, object]:
            calls.append("poll")
            return {"ok": True, "coalesced": False, "errors": []}

        assessments = [unbound_assessment(), unbound_assessment()]

        def assess(**kwargs: object) -> dict[str, object]:
            calls.append(
                "fresh-assessment"
                if kwargs.get("require_kis_reconciliation") is True
                else "binding-assessment"
            )
            return assessments.pop(0)

        def transition(*args: object, **kwargs: object) -> SimpleNamespace:
            calls.append("terminal-transition")
            self.assertEqual(
                (prior.session_id, "FAILED"),
                (args[0], args[1]),
            )
            self.assertEqual(
                "FUNCTIONAL_TEST_CRASH_RECOVERY_FAILED_CLOSED",
                kwargs["event_type"],
            )
            self.assertTrue(kwargs["payload"]["threeWayVerified"])
            self.assertEqual(
                0,
                kwargs["payload"]["unresolvedWorkingOrderCount"],
            )
            return recovered

        with (
            patch.dict(state.STATE, self.runtime_state(), clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                side_effect=assess,
            ),
            patch.object(state, "poll_execution_events", side_effect=poll),
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(
                    True,
                    "functional-test-kis-execution-poll-fresh",
                    {"lastPoll": "2026-08-08T01:00:00"},
                ),
            ),
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
                side_effect=transition,
            ),
            patch.object(
                state,
                "durable_control_snapshot",
                return_value={"halted": False},
            ),
            patch.object(state, "append_audit") as append_audit,
        ):
            result = state._recover_orphaned_functional_test_runtime_session(
                prior
            )
            self.assertFalse(state.STATE["new_entries_blocked"])

        self.assertIs(recovered, result)
        self.assertEqual(
            [
                "binding-assessment",
                "poll",
                "fresh-assessment",
                "terminal-transition",
            ],
            calls,
        )
        append_audit.assert_called_once()

    def test_bound_prior_session_is_not_treated_as_orphan(self) -> None:
        prior = orphan_session("STARTING")
        bound = unbound_assessment()
        bound["session"] = {
            "sessionId": prior.session_id,
            "lifecycle": "STARTING",
        }

        with (
            patch.dict(state.STATE, self.runtime_state(), clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                return_value=bound,
            ),
            patch.object(state, "poll_execution_events") as poll,
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
            ) as transition,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "functional-test-prior-session-still-bound",
            ):
                state._recover_orphaned_functional_test_runtime_session(prior)

        poll.assert_not_called()
        transition.assert_not_called()

    def test_incomplete_poll_keeps_orphan_and_entry_block(self) -> None:
        prior = orphan_session("DRAINING")

        with (
            patch.dict(state.STATE, self.runtime_state(), clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                return_value=unbound_assessment(),
            ),
            patch.object(
                state,
                "poll_execution_events",
                return_value={"ok": True, "coalesced": True, "errors": []},
            ),
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
            ) as transition,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "functional-test-orphan-recovery-poll-incomplete",
            ):
                state._recover_orphaned_functional_test_runtime_session(prior)
            self.assertTrue(state.STATE["new_entries_blocked"])

        transition.assert_not_called()

    def test_unresolved_working_order_keeps_orphan_nonterminal(self) -> None:
        prior = orphan_session("STOPPING")
        unresolved = unbound_assessment()
        unresolved.update(
            {
                "allowed": False,
                "blockers": ["functional-test-working-orders-unresolved"],
                "workingOrderCount": 1,
            }
        )

        with (
            patch.dict(state.STATE, self.runtime_state(), clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                side_effect=[unbound_assessment(), unresolved],
            ),
            patch.object(
                state,
                "poll_execution_events",
                return_value={"ok": True, "coalesced": False, "errors": []},
            ),
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(
                    True,
                    "functional-test-kis-execution-poll-fresh",
                    {"lastPoll": "2026-08-08T01:00:00"},
                ),
            ),
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
            ) as transition,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "functional-test-working-orders-unresolved",
            ):
                state._recover_orphaned_functional_test_runtime_session(prior)
            self.assertTrue(state.STATE["new_entries_blocked"])

        transition.assert_not_called()

    def test_non_three_way_snapshot_keeps_orphan_nonterminal(self) -> None:
        prior = orphan_session("PREFLIGHT")
        runtime_state = self.runtime_state()
        runtime_state["broker_reconciliation"] = {
            "summary": {"fresh": True, "three_way_verified": False},
            "errors": [],
        }

        with (
            patch.dict(state.STATE, runtime_state, clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                side_effect=[unbound_assessment(), unbound_assessment()],
            ),
            patch.object(
                state,
                "poll_execution_events",
                return_value={"ok": True, "coalesced": False, "errors": []},
            ),
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(
                    True,
                    "functional-test-kis-execution-poll-fresh",
                    {"lastPoll": "2026-08-08T01:00:00"},
                ),
            ),
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
            ) as transition,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "functional-test-kis-three-way-unverified",
            ):
                state._recover_orphaned_functional_test_runtime_session(prior)
            self.assertTrue(state.STATE["new_entries_blocked"])

        transition.assert_not_called()

    def test_stale_execution_poll_keeps_orphan_nonterminal(self) -> None:
        prior = orphan_session("DEGRADED")

        with (
            patch.dict(state.STATE, self.runtime_state(), clear=False),
            patch.object(
                state,
                "functional_test_authority_mutation_assessment",
                return_value=unbound_assessment(),
            ),
            patch.object(
                state,
                "poll_execution_events",
                return_value={"ok": True, "coalesced": False, "errors": []},
            ),
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(
                    False,
                    "functional-test-kis-execution-poll-stale",
                    {"lastPoll": "", "errors": []},
                ),
            ),
            patch.object(
                state.OPERATIONAL_GOVERNANCE,
                "transition_runtime_session",
            ) as transition,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "functional-test-orphan-recovery-execution-poll-stale",
            ):
                state._recover_orphaned_functional_test_runtime_session(prior)
            self.assertTrue(state.STATE["new_entries_blocked"])

        transition.assert_not_called()


if __name__ == "__main__":
    unittest.main()
