from __future__ import annotations

import copy
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from live_trader import contracts, state
from live_trader.order_management import OrderIntent
from live_trader.risk_engine import PreTradeRiskReport, RiskCheck
from trading_runtime import seal_strategy_artifact


def futures_strategy() -> dict:
    return {
        "strategy_id": "sealed-short",
        "symbol": "ETHUSDT",
        "broker_id": "binance-futures",
        "market_type": "futures",
        "position_direction": "short",
        "artifact_integrity": {"valid": True},
        "futures_execution_policy": {
            "schemaVersion": "futures-execution-policy-v1",
            "marginMode": "ISOLATED",
            "maxLeverageMultiplier": 2,
            "perTradeRiskPercent": 0.5,
            "maxNotionalPercent": 10,
            "valid": True,
            "blockers": [],
        },
        "parameters": {
            "customStrategyDefinition": {
                "riskRules": {"stopLossPct": 2},
            }
        },
    }


def futures_intent(
    *,
    risk_reducing: bool = False,
    side: str = "SELL",
) -> OrderIntent:
    return OrderIntent(
        strategy_id="sealed-short",
        asset="CRYPTO",
        symbol="ETHUSDT",
        side=side,
        quantity=0.1,
        reference_price=100,
        mode="SMALL_LIVE",
        reason="test",
        metadata={
            "broker_id": "binance-futures",
            "market_type": "futures",
            "position_direction": "short",
            "risk_reducing": risk_reducing,
        },
    )


class LiveFuturesSafetyTests(unittest.TestCase):
    def test_tampered_sealed_policy_is_visible_but_not_live_eligible(self) -> None:
        sealed = seal_strategy_artifact(
            {
                "id": "sealed-short",
                "strategy_id": "sealed-short",
                "symbol": "ETHUSDT",
                "broker_id": "binance-futures",
                "marketType": "futures",
                "lifecycle": {"status": "before-live-small"},
                "permissions": {
                    "live_small_eligible": True,
                    "live_allowed": True,
                },
                "capabilities": {
                    "liveSmallEligible": True,
                    "liveEligible": True,
                },
                "futuresExecutionPolicy": {
                    "marginMode": "ISOLATED",
                    "maxLeverageMultiplier": 1,
                },
            }
        )
        sealed["futuresExecutionPolicy"]["maxLeverageMultiplier"] = 5
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "strategy.json"
            path.write_text(json.dumps(sealed), encoding="utf-8")
            with patch.object(
                contracts,
                "strategy_artifact_dirs",
                return_value=[Path(temp_dir)],
            ):
                loaded = contracts.load_strategy_artifacts()

        self.assertEqual(1, len(loaded))
        self.assertFalse(loaded[0]["artifact_integrity"]["valid"])
        self.assertFalse(loaded[0]["live_small_eligible"])
        self.assertFalse(loaded[0]["live_eligible"])
        self.assertTrue(
            any(
                "canonical-lock-content-hash-mismatch" in item
                for item in loaded[0]["permissions"]["fail_reasons"]
            )
        )

    def test_live_risk_uses_current_mark_and_blocks_without_native_stop(self) -> None:
        class Router:
            def get_binance_futures_canary_observation(self, _symbol):
                return {
                    "account": {"available_usdt": 1000},
                    "symbol_config": {
                        "margin_type": "ISOLATED",
                        "leverage": 1,
                    },
                    "position_count": 0,
                    "open_order_count": 0,
                }

            def get_binance_futures_risk_inputs(
                self,
                _symbol,
                *,
                notional_usdt,
            ):
                self.requested_notional = notional_usdt
                return {
                    "mark_price": 120,
                    "maintenance_margin_rate": 0.005,
                    "taker_fee_rate": 0.0005,
                    "funding_rate": 0.0001,
                }

            def get_account_snapshot(self, _broker):
                return {
                    "accounts": [
                        {"broker_equity": 1000, "broker_cash": 1000}
                    ]
                }

        with patch.object(
            state,
            "BINANCE_FUTURES_SETTINGS_ROUTER_FACTORY",
            return_value=Router(),
        ):
            result = state._binance_futures_live_order_risk(
                {"strategies": [futures_strategy()]},
                futures_intent(),
            )

        self.assertIn(
            "protective-stop-order-not-implemented",
            result["blockers"],
        )
        self.assertEqual(12, result["inputs"]["notional_usdt"])
        self.assertAlmostEqual(122.4, result["inputs"]["stop_price"])

    def test_live_gate_calls_futures_guard_only_for_risk_increasing_order(
        self,
    ) -> None:
        passing = PreTradeRiskReport(
            datetime.now(),
            (RiskCheck("base", "pass", "ok"),),
        )
        checks = {"strategies": [futures_strategy()], "watchdog": {}}
        previous_reconciliation = copy.deepcopy(
            state.STATE.get("broker_reconciliation", {})
        )
        state.STATE["broker_reconciliation"] = {
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "accounts": [],
            "positions": [
                {
                    "broker_id": "binance-futures",
                    "symbol": "ETHUSDT",
                    "quantity": -0.2,
                    "position_side": "SHORT",
                }
            ],
            "errors": [],
            "successful_account_brokers": [],
            "successful_position_brokers": ["binance-futures"],
        }
        try:
            with (
                patch.object(
                    state.PreTradeRiskGate,
                    "evaluate",
                    return_value=passing,
                ),
                patch.object(state, "pre_trade_context", return_value=None),
                patch.object(
                    state,
                    "portfolio_gate_for_intent",
                    return_value={"active": False},
                ),
                patch.object(
                    state,
                    "_binance_futures_live_order_risk",
                    return_value={
                        "blockers": [
                            "protective-stop-order-not-implemented"
                        ],
                        "warnings": [],
                    },
                ) as guard,
            ):
                blocked = state.evaluate_order_gate_with_report(
                    checks,
                    "SELL",
                    False,
                    futures_intent(),
                )
                reducing = state.evaluate_order_gate_with_report(
                    checks,
                    "BUY",
                    False,
                    futures_intent(risk_reducing=True, side="BUY"),
                )
                forged_reducing = state.evaluate_order_gate_with_report(
                    checks,
                    "SELL",
                    False,
                    futures_intent(risk_reducing=True, side="SELL"),
                )
        finally:
            state.STATE["broker_reconciliation"] = previous_reconciliation

        self.assertFalse(blocked[0])
        self.assertEqual("risk_blocked", blocked[1])
        self.assertTrue(reducing[0])
        self.assertFalse(forged_reducing[0])
        self.assertEqual(2, guard.call_count)

    def test_soak_acceptance_requires_scope_duration_and_freshness(self) -> None:
        scope = {
            field: f"value-{field}"
            for field in state.CANARY_SCOPE_FIELDS
        }
        ended_at = datetime.now(timezone.utc).isoformat()
        report = {
            "runId": "run-1",
            "status": "STOPPED",
            "verdict": "PASS",
            "durationSeconds": 18_000,
            "targetDurationSeconds": 18_000,
            "endedAt": ended_at,
            "metadata": {
                "profiles": ["crypto"],
                "strategyScopes": [dict(scope)],
            },
        }
        accepted = state._accepted_futures_soak_report(report, scope)
        stale = state._accepted_futures_soak_report(
            {
                **report,
                "endedAt": (
                    datetime.now(timezone.utc) - timedelta(days=2)
                ).isoformat(),
            },
            scope,
        )
        wrong_scope = state._accepted_futures_soak_report(
            {
                **report,
                "metadata": {
                    "profiles": ["crypto"],
                    "strategyScopes": [],
                },
            },
            scope,
        )

        self.assertTrue(accepted["accepted"])
        self.assertIn("soak-report-stale", stale["blockers"])
        self.assertIn(
            "soak-strategy-deployment-scope-mismatch",
            wrong_scope["blockers"],
        )

    def test_full_live_observation_uses_clean_scope_matched_soak_duration(
        self,
    ) -> None:
        scope = {
            field: f"value-{field}"
            for field in state.CANARY_SCOPE_FIELDS
        }
        # A very old lifecycle timestamp must not count as unattended
        # observation time.
        scope["beforeLiveSmallAt"] = "2020-01-01T00:00:00Z"
        execution = {
            "fills": 20,
            "blocked": 0,
            "scope": scope,
        }
        strategy = {
            **futures_strategy(),
            "lineage": {
                "dataset": {"valid": True},
                "backtest": {"valid": True},
                "paper": {"valid": True},
                "blockingIssues": [],
            },
        }
        account_risk = {
            "current_equity": 100,
            "available_cash": 100,
        }

        def report(duration_seconds: int, verdict: str = "PASS") -> dict:
            return {
                "runId": f"run-{duration_seconds}-{verdict}",
                "status": "STOPPED",
                "verdict": verdict,
                "durationSeconds": duration_seconds,
                "targetDurationSeconds": 18_000,
                "endedAt": datetime.now(timezone.utc).isoformat(),
                "metadata": {
                    "profiles": ["crypto"],
                    "strategyScopes": [dict(scope)],
                },
            }

        def rollout_for(soak_report: dict) -> dict:
            with patch.object(
                state,
                "live_small_execution_summary",
                return_value=execution,
            ):
                return state._futures_capital_rollout_for_strategy(
                    strategy,
                    account_risk=account_risk,
                    reconciliation_fresh=True,
                    evidence_context={
                        "soak_report": soak_report,
                    },
                )

        five_hour = rollout_for(report(18_000))
        full_five_hour = next(
            stage
            for stage in five_hour["stages"]
            if stage["id"] == "FULL_LIVE"
        )
        self.assertEqual(5.0, five_hour["observationHours"])
        self.assertFalse(full_five_hour["ready"])
        self.assertIn(
            "minimum-full-live-observation-not-met",
            full_five_hour["blockers"],
        )

        seven_day = rollout_for(report(168 * 60 * 60))
        full_seven_day = next(
            stage
            for stage in seven_day["stages"]
            if stage["id"] == "FULL_LIVE"
        )
        self.assertEqual(168.0, seven_day["observationHours"])
        self.assertTrue(full_seven_day["ready"])

        recovered_warning = rollout_for(
            report(168 * 60 * 60, "PASS_WITH_WARNING")
        )
        small_warning = next(
            stage
            for stage in recovered_warning["stages"]
            if stage["id"] == "SMALL_LIVE"
        )
        full_warning = next(
            stage
            for stage in recovered_warning["stages"]
            if stage["id"] == "FULL_LIVE"
        )
        self.assertEqual(0.0, recovered_warning["observationHours"])
        self.assertTrue(small_warning["ready"])
        self.assertFalse(full_warning["ready"])
        self.assertIn(
            "minimum-full-live-observation-not-met",
            full_warning["blockers"],
        )
        self.assertIn(
            "full-live-requires-clean-soak-pass",
            full_warning["blockers"],
        )

    def test_multi_strategy_futures_entry_is_explicitly_blocked(self) -> None:
        passing = PreTradeRiskReport(
            datetime.now(),
            (RiskCheck("base", "pass", "ok"),),
        )
        base_intent = futures_intent()
        intent = replace(
            base_intent,
            metadata={
                **base_intent.metadata,
                "multi_strategy": True,
                "strategy_instance_ids": ["sleeve-a", "sleeve-b"],
            },
        )
        checks = {"strategies": [futures_strategy()], "watchdog": {}}
        with (
            patch.object(
                state.PreTradeRiskGate,
                "evaluate",
                return_value=passing,
            ),
            patch.object(state, "pre_trade_context", return_value=None),
            patch.object(
                state,
                "portfolio_gate_for_intent",
                return_value={"active": False},
            ),
            patch.object(
                state,
                "futures_risk_reducing_verified",
                return_value=False,
            ),
            patch.object(
                state,
                "_binance_futures_live_order_risk",
            ) as risk_guard,
        ):
            allowed, reason, _, _, report = (
                state.evaluate_order_gate_with_report(
                    checks,
                    "SELL",
                    False,
                    intent,
                )
            )

        self.assertFalse(allowed)
        self.assertEqual("risk_blocked", reason)
        self.assertTrue(
            any(
                check.label == "Multi-Strategy Futures 증거"
                and "rollout-evidence-not-implemented" in check.detail
                for check in report.blockers
            )
        )
        risk_guard.assert_not_called()

    def test_rollout_context_reads_ledgers_once_for_many_strategies(
        self,
    ) -> None:
        strategies = [
            {
                "strategy_id": f"s-{index}",
                "artifact_source_path": str(
                    Path("C:/shared/strategy-core") / f"s-{index}.json"
                ),
                "deployment_id": f"d-{index}",
            }
            for index in range(5)
        ]
        with (
            patch.object(
                state.DeploymentStore,
                "list",
                return_value=[],
            ) as deployments,
            patch.object(
                state,
                "_deployment_event_rows",
                return_value=[],
            ) as events,
            patch.object(
                state.PROGRAM_LEDGER,
                "execution_event_rows",
                return_value=[],
            ) as executions,
            patch.object(
                state.PROGRAM_LEDGER,
                "order_gate_event_rows",
                return_value=[],
            ) as gates,
        ):
            state._build_futures_rollout_evidence_context(
                strategies,
                soak_report={},
            )

        self.assertEqual(1, deployments.call_count)
        self.assertEqual(1, events.call_count)
        self.assertEqual(1, executions.call_count)
        self.assertEqual(1, gates.call_count)

    def test_lineage_flow_exposes_dataset_revision_metadata(self) -> None:
        flow = state.lineage_flow_snapshot(
            [
                {
                    "strategy_id": "lineage-strategy",
                    "lifecycle_status": "backtested",
                    "lineage": {
                        "dataset": {
                            "valid": True,
                            "schemaVersion": "dataset-lineage-v1",
                            "tracked": True,
                            "datasetId": "ds-1",
                            "lineageRunId": "run-1",
                            "sourceStage": "adjusted",
                            "stageRevisionId": "rev-adjusted",
                            "parentStageRevisionId": "rev-processed",
                            "dependencyStageRevisionIds": ["rev-daily"],
                            "rawContentSha256": "a" * 64,
                            "rawMetadataSha256": "b" * 64,
                            "transformationId": "scraper/adjusted/v1",
                            "parent": {"stage": "dataset"},
                        },
                        "backtest": {"valid": True},
                        "paper": {"valid": False},
                        "live": {"valid": False},
                    },
                }
            ]
        )

        metadata = flow["flows"][0]["stages"][0]["metadata"]
        self.assertEqual("dataset-lineage-v1", metadata["schemaVersion"])
        self.assertEqual(["rev-daily"], metadata["dependencyStageRevisionIds"])
        self.assertEqual("a" * 64, metadata["rawContentSha256"])
        self.assertEqual({"stage": "dataset"}, metadata["parent"])

    def test_lineage_flow_keeps_paper_and_live_required_after_pause(self) -> None:
        flow = state.lineage_flow_snapshot(
            [
                {
                    "strategy_id": "paused-live-strategy",
                    "lifecycle_status": "paused",
                    "lifecycle": {
                        "status": "paused",
                        "pausedFrom": "live",
                        "history": [{"from": "before-live-small", "to": "live"}],
                    },
                    "lineage": {
                        "dataset": {"valid": True},
                        "backtest": {"valid": True},
                        "paper": {"valid": False},
                        "live": {"valid": False},
                    },
                }
            ]
        )

        stages = {
            stage["id"]: stage
            for stage in flow["flows"][0]["stages"]
        }
        self.assertTrue(stages["paper"]["required"])
        self.assertTrue(stages["live"]["required"])
        self.assertEqual("BLOCK", stages["paper"]["status"])
        self.assertEqual("BLOCK", stages["live"]["status"])


if __name__ == "__main__":
    unittest.main()
