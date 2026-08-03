import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from live_trader.validation_small_live import (
    _candidate_runtime_spec,
    _stable_hash,
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
from scripts.prepare_validation_small_live import main as prepare_plan_main


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


def build_bound_plan(
    root: Path,
    strategy: dict,
    portfolio: dict | None = None,
    **kwargs,
) -> dict:
    strategy_lock = strategy.get("artifactLock") or {}
    binding = {
        "strategy_id": strategy["id"],
        "strategy_artifact_hash": strategy_lock["artifactHash"],
    }
    if portfolio is None:
        binding["strategy_only"] = True
    else:
        portfolio_lock = portfolio.get("artifactLock") or {}
        binding.update(
            {
                "portfolio_id": portfolio["id"],
                "portfolio_artifact_hash": portfolio_lock[
                    "artifactHash"
                ],
            }
        )
    return build_validation_plan(root, **binding, **kwargs)


class ValidationSmallLivePlanTest(unittest.TestCase):
    def test_tampered_canonical_lock_is_blocked_before_plan_selection(self) -> None:
        strategy = seal_strategy_artifact(strategy_payload())
        strategy["name"] = "Tampered after sealing"
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(
                root,
                strategy,
                portfolio,
            )

            plan = build_bound_plan(root, strategy, portfolio)

        self.assertEqual(0, plan["candidateCount"])
        strategy_block = next(
            item
            for item in plan["blocked"]
            if item["kind"] == "strategy"
        )
        self.assertIn(
            "canonical-lock:canonical-lock-content-hash-mismatch",
            strategy_block["issues"],
        )

    def test_runtime_spec_requires_exact_strategy_and_portfolio_hashes(self) -> None:
        portfolio_hash = "f" * 64
        strategy_hash = "e" * 64
        spec = RuntimeStrategySpec(
            portfolio_id="portfolio-1",
            portfolio_hash=portfolio_hash,
            strategy_instance_id="instance-1",
            strategy_id="strategy-1",
            artifact_hash=strategy_hash,
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
        candidate = {
            "portfolioPath": "portfolio.json",
            "portfolioArtifactHash": portfolio_hash,
            "strategyInstanceId": "instance-1",
            "strategyId": "strategy-1",
            "strategyArtifactHash": "d" * 64,
            "symbol": "BTCUSDT",
            "timeframe": "1h",
        }
        loaded = SimpleNamespace(
            portfolio_hash=portfolio_hash,
            specs=(spec,),
        )

        with patch(
            "live_trader.validation_small_live.load_portfolio_runtime_path",
            return_value=loaded,
        ):
            with self.assertRaisesRegex(ValueError, "Strategy ID·artifact hash"):
                _candidate_runtime_spec(candidate)

        candidate["strategyArtifactHash"] = strategy_hash
        candidate["portfolioArtifactHash"] = "c" * 64
        with patch(
            "live_trader.validation_small_live.load_portfolio_runtime_path",
            return_value=loaded,
        ):
            with self.assertRaisesRegex(ValueError, "Portfolio artifact hash"):
                _candidate_runtime_spec(candidate)

    def test_exact_artifact_binding_selects_only_requested_hashes(self) -> None:
        strategy = seal_strategy_artifact(strategy_payload())
        portfolio = seal_portfolio_artifact(portfolio_payload())
        strategy_hash = strategy["artifactLock"]["artifactHash"]
        portfolio_hash = portfolio["artifactLock"]["artifactHash"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)

            plan = build_validation_plan(
                root,
                strategy_id=strategy["id"],
                strategy_artifact_hash=strategy_hash,
                portfolio_id=portfolio["id"],
                portfolio_artifact_hash=portfolio_hash,
            )
            wrong = build_validation_plan(
                root,
                strategy_id=strategy["id"],
                strategy_artifact_hash="0" * 64,
                portfolio_id=portfolio["id"],
                portfolio_artifact_hash=portfolio_hash,
            )

        self.assertEqual(1, plan["candidateCount"])
        self.assertTrue(plan["artifactBinding"]["requested"])
        self.assertEqual(1, plan["artifactBinding"]["matchedCandidateCount"])
        self.assertTrue(validate_monitor_only_plan(plan, verify_files=False)["ok"])
        self.assertEqual(0, wrong["candidateCount"])
        self.assertTrue(
            any(
                item["kind"] == "exact-artifact-binding"
                for item in wrong["blocked"]
            )
        )

    def test_partial_or_missing_exact_binding_is_rejected_immediately(self) -> None:
        root = Path("unused")
        cases = [
            {},
            {"strategy_id": "strategy-1"},
            {"strategy_artifact_hash": "a" * 64},
            {
                "strategy_id": "strategy-1",
                "strategy_artifact_hash": "not-a-sha256",
            },
            {
                "strategy_id": "strategy-1",
                "strategy_artifact_hash": "a" * 64,
            },
            {
                "strategy_id": "strategy-1",
                "strategy_artifact_hash": "a" * 64,
                "portfolio_id": "portfolio-1",
            },
            {
                "strategy_id": "strategy-1",
                "strategy_artifact_hash": "a" * 64,
                "portfolio_artifact_hash": "b" * 64,
            },
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(
                    ValueError,
                    "exact-artifact-binding-invalid",
                ):
                    build_validation_plan(root, **kwargs)

        with self.assertRaisesRegex(
            ValueError,
            "portfolio-binding-forbidden-for-strategy-only",
        ):
            build_validation_plan(
                root,
                strategy_id="strategy-1",
                strategy_artifact_hash="a" * 64,
                portfolio_id="portfolio-1",
                portfolio_artifact_hash="b" * 64,
                strategy_only=True,
            )

    def test_cli_returns_structured_block_for_partial_binding(self) -> None:
        with patch("builtins.print") as output:
            exit_code = prepare_plan_main(
                [
                    "--artifact-root",
                    "unused",
                    "--strategy-id",
                    "strategy-1",
                    "--preview",
                ]
            )

        payload = json.loads(output.call_args.args[0])
        self.assertEqual(3, exit_code)
        self.assertFalse(payload["ok"])
        self.assertFalse(payload["written"])
        self.assertIn("exact-artifact-binding-invalid", payload["reason"])

    def test_same_strategy_id_never_selects_another_revision_hash(self) -> None:
        first = seal_strategy_artifact(strategy_payload())
        second_payload = strategy_payload()
        second_payload["name"] = "Validation BTC 1h revision 2"
        second = seal_strategy_artifact(second_payload)
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            first_path, _ = write_fixture(
                root,
                first,
                portfolio,
                strategy_name="strategy-v1.json",
            )
            second_path = root / "strategy-v2.json"
            second_path.write_text(
                json.dumps(second, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            first_plan = build_bound_plan(root, first, portfolio)
            second_plan = build_bound_plan(root, second, portfolio)

        self.assertEqual(
            first["artifactLock"]["artifactHash"],
            first_plan["candidates"][0]["strategyArtifactHash"],
        )
        self.assertEqual(str(first_path), first_plan["candidates"][0]["strategyPath"])
        self.assertEqual(
            second["artifactLock"]["artifactHash"],
            second_plan["candidates"][0]["strategyArtifactHash"],
        )
        self.assertEqual(str(second_path), second_plan["candidates"][0]["strategyPath"])

    def test_explicit_strategy_only_plan_never_synthesizes_portfolio(self) -> None:
        strategy = seal_strategy_artifact(strategy_payload())
        strategy_hash = strategy["artifactLock"]["artifactHash"]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            root.mkdir(parents=True)
            (root / "strategy.json").write_text(
                json.dumps(strategy, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "portfolio-binding-complete-pair-required",
            ):
                build_validation_plan(
                    root,
                    strategy_id=strategy["id"],
                    strategy_artifact_hash=strategy_hash,
                )
            plan = build_validation_plan(
                root,
                strategy_id=strategy["id"],
                strategy_artifact_hash=strategy_hash,
                strategy_only=True,
            )
            verification = validate_monitor_only_plan(plan)
            candidate = plan["candidates"][0]
            runtime_spec = _candidate_runtime_spec(candidate)
            wrong_file_sha = copy.deepcopy(candidate)
            wrong_file_sha["strategyFileSha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "file SHA-256"):
                _candidate_runtime_spec(wrong_file_sha)

            wrong_identity_plan = copy.deepcopy(plan)
            wrong_identity_plan["candidates"][0][
                "strategyInstanceId"
            ] = "standalone:other"
            wrong_identity = validate_monitor_only_plan(
                wrong_identity_plan,
                verify_files=False,
            )

        self.assertEqual(1, plan["candidateCount"])
        self.assertTrue(verification["ok"])
        self.assertTrue(candidate["standaloneStrategy"])
        self.assertEqual("", candidate["portfolioId"])
        self.assertEqual("", candidate["portfolioArtifactHash"])
        self.assertEqual(strategy["id"], runtime_spec.strategy_id)
        self.assertEqual(strategy_hash, runtime_spec.artifact_hash)
        self.assertEqual(f"standalone:{strategy['id']}", runtime_spec.portfolio_id)
        self.assertIn(
            "candidate-0:strategy-instance-identity-mismatch",
            wrong_identity["issues"],
        )
        self.assertIn(
            "candidate-0:standalone-runtime-identity-mismatch",
            wrong_identity["issues"],
        )

    def test_passed_strategy_and_portfolio_create_monitor_only_candidate(self) -> None:
        strategy = seal_strategy_artifact(strategy_payload())
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)

            plan = build_bound_plan(root, strategy, portfolio)
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
        strategy = seal_strategy_artifact(strategy)
        portfolio = seal_portfolio_artifact(portfolio)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)
            plan = build_bound_plan(root, strategy, portfolio)

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
        strategy = seal_strategy_artifact(strategy)
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)
            plan = build_bound_plan(root, strategy, portfolio)

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
            failed_strategy = seal_strategy_artifact(
                strategy_payload(final_status="fail")
            )
            failed_portfolio = seal_portfolio_artifact(portfolio_payload())
            write_fixture(failed_root, failed_strategy, failed_portfolio)
            failed_plan = build_bound_plan(
                failed_root,
                failed_strategy,
                failed_portfolio,
            )

            zero_root = Path(tmp) / "zero"
            zero_strategy = seal_strategy_artifact(strategy_payload())
            zero_portfolio = seal_portfolio_artifact(
                portfolio_payload(target_weight=0.0)
            )
            write_fixture(zero_root, zero_strategy, zero_portfolio)
            zero_plan = build_bound_plan(
                zero_root,
                zero_strategy,
                zero_portfolio,
            )

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

            plan = build_bound_plan(root, strategy, portfolio)
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
        strategy = seal_strategy_artifact(strategy_payload())
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            strategy_path, _portfolio_path = write_fixture(
                root,
                strategy,
                portfolio,
            )
            plan = build_bound_plan(root, strategy, portfolio)
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

            partial_binding = copy.deepcopy(plan)
            partial_binding["artifactBinding"][
                "strategyArtifactHash"
            ] = ""
            partial_binding["contentHash"] = _stable_hash(
                {
                    key: value
                    for key, value in partial_binding.items()
                    if key != "contentHash"
                }
            )
            partial_verification = validate_monitor_only_plan(
                partial_binding,
                verify_files=False,
            )
            self.assertFalse(partial_verification["ok"])
            self.assertIn(
                "artifact-binding:strategy-binding-complete-pair-required",
                partial_verification["issues"],
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

        strategy = seal_strategy_artifact(strategy_payload())
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)
            plan = build_bound_plan(root, strategy, portfolio)
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
        strategy = seal_strategy_artifact(strategy_payload())
        portfolio = seal_portfolio_artifact(portfolio_payload())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifacts"
            write_fixture(root, strategy, portfolio)
            plan = build_bound_plan(
                root,
                strategy,
                portfolio,
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
