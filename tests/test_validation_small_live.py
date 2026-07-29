import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader.validation_small_live import (
    build_validation_plan,
    evaluate_validation_candidate_once,
    load_and_validate_plan,
    validate_monitor_only_plan,
    validation_plan_snapshot,
    write_validation_plan,
)
from trading_runtime import ClosedBar, RuntimeStrategySpec
from trading_runtime.artifact_governance import (
    seal_portfolio_artifact,
    seal_strategy_artifact,
)


def strategy_payload(
    strategy_id: str = "VALIDATION-BTC-1H",
    *,
    final_status: str = "pass",
) -> dict:
    return {
        "schemaVersion": "market-strategy-v1",
        "artifactType": "strategy",
        "id": strategy_id,
        "name": "Validation BTC 1h",
        "plugin": "moving_average_cross",
        "dataset": {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "provider": "binance",
        },
        "lifecycle": {"status": "backtested"},
        "finalTest": {"status": final_status},
        "permissions": {
            "trader_export_allowed": True,
            "paper_trader_verified": False,
            "live_small_eligible": False,
            "live_eligible": False,
            "live_allowed": False,
            "fail_reasons": [],
        },
        "portfolioCandidate": {
            "candidateId": f"candidate-{strategy_id}",
            "approved": True,
            "blockers": [],
        },
    }


def portfolio_payload(
    strategy_id: str = "VALIDATION-BTC-1H",
    *,
    target_weight: float = 0.1,
) -> dict:
    instance_id = f"si-{strategy_id}"
    return {
        "schemaVersion": "portfolio-artifact-v1",
        "artifactType": "portfolio",
        "id": f"portfolio-{strategy_id}",
        "name": "Validation Portfolio",
        "lifecycle": {"status": "backtested"},
        "permissions": {
            "paper_export_allowed": True,
            "live_small_allowed": False,
            "live_allowed": False,
            "fail_reasons": [],
        },
        "strategyInstances": [
            {
                "instanceId": instance_id,
                "strategyId": strategy_id,
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "pluginId": "moving_average_cross",
                "brokerId": "binance",
                "marketDataProvider": "binance",
                "allocation": {
                    "normalizedWeight": target_weight,
                    "scoreTargetWeight": target_weight,
                },
            }
        ],
        "framework": {
            "targetPortfolio": [
                {
                    "strategyInstanceId": instance_id,
                    "strategyId": strategy_id,
                    "symbol": "BTCUSDT",
                    "targetWeight": target_weight,
                    "status": "pass",
                }
            ],
            "riskChecks": [
                {"id": "weights", "label": "Weights", "status": "pass"}
            ],
        },
    }


def write_fixture(
    root: Path,
    strategy: dict,
    portfolio: dict,
    *,
    strategy_name: str = "strategy.json",
    portfolio_name: str = "portfolio.json",
) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    portfolio_dir = root / "portfolios"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    strategy_path = root / strategy_name
    portfolio_path = portfolio_dir / portfolio_name
    strategy_path.write_text(
        json.dumps(strategy, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    portfolio_path.write_text(
        json.dumps(portfolio, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return strategy_path, portfolio_path


class ValidationSmallLivePlanTest(unittest.TestCase):
    def test_passed_strategy_and_portfolio_create_monitor_only_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                seal_strategy_artifact(strategy_payload()),
                seal_portfolio_artifact(portfolio_payload()),
            )

            plan = build_validation_plan(root)
            verification = validate_monitor_only_plan(plan)

        self.assertTrue(verification["ok"])
        self.assertEqual(1, plan["candidateCount"])
        candidate = plan["candidates"][0]
        self.assertEqual("integration-monitor-smoke", candidate["validationStage"])
        self.assertEqual(
            "general-integration-smoke",
            candidate["candidateClass"],
        )
        self.assertEqual("MONITOR", candidate["runtimeMode"])
        self.assertTrue(candidate["dryRunRequired"])
        self.assertFalse(candidate["brokerSubmitAllowed"])
        self.assertFalse(candidate["productionPermissionGranted"])
        self.assertEqual(0.1, candidate["targetWeight"])
        self.assertTrue(
            candidate["validationStrategyInstanceId"].startswith("vsi:")
        )
        self.assertTrue(
            candidate["validationPortfolioInstanceId"].startswith("vpi:")
        )
        self.assertEqual(1, plan["validationStrategyInstanceCount"])
        self.assertEqual(1, plan["validationPortfolioInstanceCount"])
        self.assertTrue(candidate["strategyIntegrity"]["productionIntegrityReady"])
        self.assertTrue(candidate["portfolioIntegrity"]["productionIntegrityReady"])
        self.assertFalse(plan["guardrails"]["productionLifecycleMutation"])
        self.assertFalse(plan["guardrails"]["liveSmallPermissionGranted"])
        self.assertFalse(plan["guardrails"]["fullLivePermissionGranted"])
        self.assertEqual(1, plan["generalSmokeCandidateCount"])
        self.assertEqual(0, plan["futuresShortCandidateCount"])

    def test_short_candidate_is_explicitly_futures_routed(self) -> None:
        strategy = strategy_payload()
        strategy["dataset"] = {
            **strategy["dataset"],
            "provider": "binance-futures",
            "marketType": "futures",
            "sourcePath": (
                "processed/crypto/binance/futures/BTCUSDT/1h.parquet"
            ),
        }
        strategy["parameters"] = {
            "customStrategyDefinition": {
                "pluginId": "strategy_builder_custom",
                "positionDirection": "short",
                "entryRules": [],
                "exitRules": [],
            }
        }
        strategy["plugin"] = "strategy_builder_custom"
        portfolio = portfolio_payload()
        portfolio["strategyInstances"][0].update(
            {
                "pluginId": "strategy_builder_custom",
                "brokerId": "binance-futures",
                "marketDataProvider": "binance-futures",
                "marketType": "futures",
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                seal_strategy_artifact(strategy),
                seal_portfolio_artifact(portfolio),
            )
            plan = build_validation_plan(root)

        self.assertEqual(1, plan["futuresShortCandidateCount"])
        self.assertEqual(0, plan["generalSmokeCandidateCount"])
        candidate = plan["candidates"][0]
        self.assertEqual(
            "futures-short-monitor-smoke",
            candidate["candidateClass"],
        )
        self.assertEqual("futures", candidate["marketType"])
        self.assertEqual("short", candidate["positionDirection"])
        self.assertTrue(candidate["allowShort"])
        self.assertEqual("binance-futures", candidate["brokerHint"])

    def test_short_spot_route_is_rejected_instead_of_false_routing(self) -> None:
        strategy = strategy_payload()
        strategy["parameters"] = {
            "customStrategyDefinition": {
                "pluginId": "strategy_builder_custom",
                "positionDirection": "short",
                "entryRules": [],
                "exitRules": [],
            }
        }
        strategy["plugin"] = "strategy_builder_custom"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                seal_strategy_artifact(strategy),
                seal_portfolio_artifact(portfolio_payload()),
            )
            plan = build_validation_plan(root)

        self.assertEqual(0, plan["candidateCount"])
        self.assertTrue(
            any(
                "short-market-must-be-futures:spot" in item["issues"]
                for item in plan["blocked"]
            )
        )

    def test_failed_final_or_zero_target_never_becomes_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            failed_root = Path(tmp) / "failed"
            write_fixture(
                failed_root,
                seal_strategy_artifact(
                    strategy_payload(final_status="fail")
                ),
                seal_portfolio_artifact(portfolio_payload()),
            )
            failed_plan = build_validation_plan(failed_root)

            zero_root = Path(tmp) / "zero"
            write_fixture(
                zero_root,
                seal_strategy_artifact(strategy_payload()),
                seal_portfolio_artifact(
                    portfolio_payload(target_weight=0.0)
                ),
            )
            zero_plan = build_validation_plan(zero_root)

        self.assertEqual(0, failed_plan["candidateCount"])
        self.assertTrue(
            any(
                "final-test-not-passed" in item["issues"]
                for item in failed_plan["blocked"]
            )
        )
        self.assertEqual(0, zero_plan["candidateCount"])
        self.assertTrue(
            any(
                "positive-target-weight-required" in item["issues"]
                for item in zero_plan["blocked"]
            )
        )

    def test_legacy_lock_is_validation_only_and_never_production_ready(self) -> None:
        strategy = strategy_payload()
        strategy["artifactLock"] = {
            "schemaVersion": "strategy-artifact-lock-v1",
            "artifactHash": "a" * 64,
        }
        portfolio = portfolio_payload()
        portfolio["artifactLock"] = {
            "schemaVersion": "portfolio-artifact-lock-v1",
            "artifactHash": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)

            plan = build_validation_plan(root)
            verification = validate_monitor_only_plan(plan)

        self.assertTrue(verification["ok"])
        self.assertEqual(1, plan["candidateCount"])
        candidate = plan["candidates"][0]
        self.assertEqual(
            "legacy-lock-validation-only",
            candidate["strategyIntegrity"]["class"],
        )
        self.assertFalse(
            candidate["strategyIntegrity"]["productionIntegrityReady"]
        )
        self.assertFalse(candidate["productionPermissionGranted"])

    def test_plan_and_source_hash_tampering_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            strategy_path, _portfolio_path = write_fixture(
                root,
                seal_strategy_artifact(strategy_payload()),
                seal_portfolio_artifact(portfolio_payload()),
            )
            plan = build_validation_plan(root)
            output = Path(tmp) / "validation-plan.json"
            write_validation_plan(output, plan)

            loaded = load_and_validate_plan(output)
            self.assertTrue(loaded["verification"]["ok"])

            unsafe = copy.deepcopy(plan)
            unsafe["guardrails"]["brokerSubmitAllowed"] = True
            unsafe_verification = validate_monitor_only_plan(
                unsafe,
                verify_files=False,
            )
            self.assertFalse(unsafe_verification["ok"])
            self.assertIn(
                "guardrail-invalid:brokerSubmitAllowed",
                unsafe_verification["issues"],
            )

            strategy_path.write_text(
                strategy_path.read_text(encoding="utf-8") + "\n",
                encoding="utf-8",
            )
            stale_verification = validate_monitor_only_plan(plan)
            self.assertFalse(stale_verification["ok"])
            self.assertTrue(
                any(
                    issue.endswith("strategy-file-hash-mismatch")
                    for issue in stale_verification["issues"]
                )
            )

    def test_one_shot_evaluation_runs_real_evaluator_without_order_path(self) -> None:
        class FakeFeed:
            provider_id = "binance"

            def warmup(self, _subscription, limit):
                rows = []
                for index in range(limit):
                    start_hour = index
                    rows.append(
                        ClosedBar(
                            instrument_id="BTCUSDT",
                            symbol="BTCUSDT",
                            provider="binance",
                            timeframe="1h",
                            start_time=(
                                f"2026-01-{1 + start_hour // 24:02d}T"
                                f"{start_hour % 24:02d}:00:00Z"
                            ),
                            end_time=(
                                f"2026-01-{1 + (start_hour + 1) // 24:02d}T"
                                f"{(start_hour + 1) % 24:02d}:00:00Z"
                            ),
                            open=100 + index,
                            high=101 + index,
                            low=99 + index,
                            close=100 + index,
                            volume=1,
                            received_time="",
                        )
                    )
                return rows

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                seal_strategy_artifact(strategy_payload()),
                seal_portfolio_artifact(portfolio_payload()),
            )
            plan = build_validation_plan(root)
            output = Path(tmp) / "validation-plan.json"
            write_validation_plan(output, plan)
            candidate = plan["candidates"][0]
            runtime_spec = RuntimeStrategySpec(
                portfolio_id=candidate["portfolioId"],
                portfolio_hash="f" * 64,
                strategy_instance_id=candidate["strategyInstanceId"],
                strategy_id=candidate["strategyId"],
                artifact_hash="e" * 64,
                plugin_id="moving_average_cross",
                instrument_id="BTCUSDT",
                symbol="BTCUSDT",
                timeframe="1h",
                provider="binance",
                broker_id="binance",
                target_weight=0.1,
                parameters={"shortMa": 2, "longMa": 3},
                artifact={},
            )

            with patch(
                "live_trader.validation_small_live._candidate_runtime_spec",
                return_value=runtime_spec,
            ):
                result = evaluate_validation_candidate_once(
                    candidate["validationStrategyInstanceId"],
                    path=output,
                    feed_factory=lambda _specs: [FakeFeed()],
                )

        self.assertTrue(result["ok"])
        self.assertEqual("MONITOR", result["runtimeMode"])
        self.assertFalse(result["brokerSubmitAllowed"])
        self.assertEqual(0, result["maximumOrderNotional"])
        self.assertIn(result["decision"]["signal"], {"BUY", "SELL", "HOLD"})
        self.assertEqual("MONITOR", result["monitorHandler"]["action"])

    def test_packaged_snapshot_uses_safe_research_bundle_plan_fallback(self) -> None:
        embedded = {
            "available": True,
            "researchOnly": True,
            "artifactPromotionAllowed": False,
            "productionPermissionGranted": False,
            "brokerSubmitAllowed": False,
            "strategyCount": 4,
            "functionalPass": True,
            "strategies": [],
            "issues": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                seal_strategy_artifact(strategy_payload()),
                seal_portfolio_artifact(portfolio_payload()),
            )
            plan = build_validation_plan(
                root,
                research_short_bundle=embedded,
            )
            output = Path(tmp) / "validation-plan.json"
            write_validation_plan(output, plan)
            with patch(
                "live_trader.validation_small_live.research_short_bundle_snapshot",
                return_value={
                    **embedded,
                    "available": False,
                    "strategyCount": 0,
                    "functionalPass": False,
                },
            ):
                snapshot = validation_plan_snapshot(output)

        self.assertTrue(snapshot["ok"])
        self.assertTrue(
            snapshot["researchShortBundle"]["portablePlanFallback"]
        )
        self.assertEqual(
            4,
            snapshot["researchShortBundle"]["strategyCount"],
        )
        self.assertTrue(
            snapshot["researchShortBundle"]["functionalPass"]
        )


if __name__ == "__main__":
    unittest.main()
