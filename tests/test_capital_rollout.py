from __future__ import annotations

import unittest
from copy import deepcopy

from live_trader.capital_rollout import build_capital_rollout, capital_cap_for_mode


class CapitalRolloutTest(unittest.TestCase):
    def test_rollout_never_exceeds_policy_or_available_cash(self) -> None:
        rollout = build_capital_rollout(
            account_equity=100,
            available_cash=8,
            max_notional_percent=100,
            canary_fills=20,
            blocked_orders=0,
            observation_hours=168,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS",
        )
        self.assertEqual(8, capital_cap_for_mode(rollout, "SMALL_LIVE")["maxNotional"])
        self.assertEqual(8, capital_cap_for_mode(rollout, "FULL_LIVE")["maxNotional"])

    def test_large_finite_equity_cannot_overflow_the_percentage_cap(self) -> None:
        rollout = build_capital_rollout(
            account_equity=1e308,
            available_cash=1e308,
            max_notional_percent=50,
            canary_fills=20,
            blocked_orders=0,
            observation_hours=168,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS",
        )

        self.assertEqual(5e307, rollout["policyCap"])
        decision = capital_cap_for_mode(rollout, "FULL_LIVE")
        self.assertTrue(decision["ready"])
        self.assertEqual(5e307, decision["maxNotional"])

    def test_small_live_accepts_recovered_warning_but_full_live_does_not(self) -> None:
        rollout = build_capital_rollout(
            account_equity=100,
            available_cash=100,
            max_notional_percent=25,
            canary_fills=20,
            blocked_orders=0,
            observation_hours=168,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS_WITH_WARNING",
        )
        self.assertTrue(capital_cap_for_mode(rollout, "SMALL_LIVE")["ready"])
        full = capital_cap_for_mode(rollout, "FULL_LIVE")
        self.assertFalse(full["ready"])
        self.assertIn("full-live-requires-clean-soak-pass", full["blockers"])

    def test_missing_evidence_blocks_every_live_stage(self) -> None:
        rollout = build_capital_rollout(
            account_equity=100,
            available_cash=100,
            max_notional_percent=10,
        )
        self.assertFalse(capital_cap_for_mode(rollout, "SMALL_LIVE")["ready"])
        self.assertIn(
            "futures-policy-invalid",
            capital_cap_for_mode(rollout, "SMALL_LIVE")["blockers"],
        )

    def test_non_finite_evidence_fails_closed_without_raising(self) -> None:
        rollout = build_capital_rollout(
            account_equity=float("nan"),
            available_cash=float("inf"),
            max_notional_percent=float("-inf"),
            canary_fills=float("nan"),
            blocked_orders=float("inf"),
            observation_hours=float("nan"),
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS",
        )

        canary = capital_cap_for_mode(rollout, "SMALL_LIVE")
        self.assertFalse(canary["ready"])
        self.assertEqual(0.0, canary["maxNotional"])
        self.assertIn("account-equity-invalid", canary["blockers"])
        self.assertIn("available-cash-invalid", canary["blockers"])

    def test_invalid_notional_percent_never_receives_default_exposure(self) -> None:
        invalid_values = (
            float("nan"),
            float("inf"),
            float("-inf"),
            "not-a-number",
            None,
            -1,
            101,
        )
        for value in invalid_values:
            with self.subTest(value=value):
                rollout = build_capital_rollout(
                    account_equity=100,
                    available_cash=100,
                    max_notional_percent=value,
                    canary_fills=20,
                    blocked_orders=0,
                    observation_hours=168,
                    reconciliation_fresh=True,
                    lineage_valid=True,
                    policy_valid=True,
                    soak_status="PASS",
                )

                self.assertFalse(
                    rollout["policyMaxNotionalPercentValid"]
                )
                self.assertEqual(0.0, rollout["policyCap"])
                self.assertTrue(
                    all(stage["ready"] is False for stage in rollout["stages"])
                )
                self.assertTrue(
                    all(
                        "max-notional-percent-invalid"
                        in stage["blockers"]
                        for stage in rollout["stages"]
                    )
                )
                decision = capital_cap_for_mode(rollout, "FULL_LIVE")
                self.assertFalse(decision["ready"])
                self.assertEqual(0.0, decision["maxNotional"])

    def test_evidence_booleans_require_literal_true(self) -> None:
        fields_and_blockers = {
            "reconciliation_fresh": "reconciliation-not-fresh",
            "lineage_valid": "lineage-invalid",
            "policy_valid": "futures-policy-invalid",
        }
        for field, blocker in fields_and_blockers.items():
            with self.subTest(field=field):
                evidence = {
                    "reconciliation_fresh": True,
                    "lineage_valid": True,
                    "policy_valid": True,
                }
                evidence[field] = "false"
                rollout = build_capital_rollout(
                    account_equity=100,
                    available_cash=100,
                    max_notional_percent=25,
                    canary_fills=20,
                    blocked_orders=0,
                    observation_hours=168,
                    soak_status="PASS",
                    **evidence,
                )

                decision = capital_cap_for_mode(rollout, "FULL_LIVE")
                self.assertFalse(decision["ready"])
                self.assertIn(blocker, decision["blockers"])

    def test_invalid_numeric_evidence_zeroes_every_stage_cap(self) -> None:
        cases = (
            (
                "canary_fills",
                (
                    0.5,
                    -1,
                    float("nan"),
                    float("inf"),
                    float("-inf"),
                    "invalid",
                ),
                "canaryFillsValid",
                "canary-fills-invalid",
            ),
            (
                "blocked_orders",
                (
                    0.5,
                    -1,
                    float("nan"),
                    float("inf"),
                    float("-inf"),
                    "invalid",
                ),
                "blockedOrdersValid",
                "blocked-orders-invalid",
            ),
            (
                "observation_hours",
                (
                    -0.5,
                    float("nan"),
                    float("inf"),
                    float("-inf"),
                    "invalid",
                ),
                "observationHoursValid",
                "observation-hours-invalid",
            ),
        )
        for field, values, validity_field, blocker in cases:
            for value in values:
                with self.subTest(field=field, value=value):
                    evidence = {
                        "canary_fills": 20,
                        "blocked_orders": 0,
                        "observation_hours": 168,
                    }
                    evidence[field] = value
                    rollout = build_capital_rollout(
                        account_equity=100,
                        available_cash=100,
                        max_notional_percent=25,
                        reconciliation_fresh=True,
                        lineage_valid=True,
                        policy_valid=True,
                        soak_status="PASS",
                        **evidence,
                    )

                    self.assertFalse(rollout[validity_field])
                    self.assertTrue(
                        all(
                            stage["maxNotional"] == 0.0
                            and stage["ready"] is False
                            and blocker in stage["blockers"]
                            for stage in rollout["stages"]
                        )
                    )
                    decision = capital_cap_for_mode(
                        rollout,
                        "FULL_LIVE",
                    )
                    self.assertFalse(decision["ready"])
                    self.assertEqual(0.0, decision["maxNotional"])

    def test_zero_numeric_evidence_remains_valid(self) -> None:
        rollout = build_capital_rollout(
            account_equity=100,
            available_cash=100,
            max_notional_percent=25,
            canary_fills=0,
            blocked_orders=0,
            observation_hours=0,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="NOT_RUN",
        )

        self.assertTrue(rollout["canaryFillsValid"])
        self.assertTrue(rollout["blockedOrdersValid"])
        self.assertTrue(rollout["observationHoursValid"])
        self.assertEqual(10.0, rollout["stages"][0]["maxNotional"])

    def test_cap_consumer_rejects_forged_or_stale_rollout_structure(self) -> None:
        valid = build_capital_rollout(
            account_equity=100,
            available_cash=100,
            max_notional_percent=25,
            canary_fills=20,
            blocked_orders=0,
            observation_hours=168,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS",
        )
        forged_stage = deepcopy(valid)
        forged_stage["stages"][2]["maxNotional"] = 100
        forged_non_target_stage = deepcopy(valid)
        forged_non_target_stage["stages"][0]["maxNotional"] = 20
        stale_cash = deepcopy(valid)
        stale_cash["availableCash"] = 5
        stale_policy_cap = deepcopy(valid)
        stale_policy_cap["policyCap"] = 100
        wrong_schema = deepcopy(valid)
        wrong_schema["schemaVersion"] = "capital-rollout-v0"
        duplicate_stage = deepcopy(valid)
        duplicate_stage["stages"][2] = deepcopy(
            duplicate_stage["stages"][1]
        )

        cases = {
            "forged-stage": forged_stage,
            "forged-non-target-stage": forged_non_target_stage,
            "stale-cash": stale_cash,
            "stale-policy-cap": stale_policy_cap,
            "wrong-schema": wrong_schema,
            "duplicate-stage": duplicate_stage,
        }
        for name, rollout in cases.items():
            with self.subTest(name=name):
                decision = capital_cap_for_mode(rollout, "FULL_LIVE")
                self.assertFalse(decision["ready"])
                self.assertEqual(0.0, decision["maxNotional"])
                self.assertTrue(
                    any(
                        blocker.startswith("capital-rollout-")
                        for blocker in decision["blockers"]
                    )
                )

    def test_cap_consumer_rejects_unknown_mode(self) -> None:
        rollout = build_capital_rollout(
            account_equity=100,
            available_cash=100,
            max_notional_percent=25,
            canary_fills=20,
            blocked_orders=0,
            observation_hours=168,
            reconciliation_fresh=True,
            lineage_valid=True,
            policy_valid=True,
            soak_status="PASS",
        )

        decision = capital_cap_for_mode(rollout, "FULL_LlVE")

        self.assertFalse(decision["ready"])
        self.assertEqual(0.0, decision["maxNotional"])
        self.assertIn("capital-rollout-mode-invalid", decision["blockers"])


if __name__ == "__main__":
    unittest.main()
