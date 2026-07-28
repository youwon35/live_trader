import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader.contracts import (
    can_live_use_artifact,
    enrich_strategy_artifact_runtime,
    load_portfolio_artifacts,
    load_strategy_artifacts,
    normalize_portfolio_artifact,
    normalize_strategy_artifact,
    resolve_trading_system_root,
    strategy_artifact_dirs,
    strategy_plugin_dirs,
    strategy_plugin_status,
)
from trading_runtime.artifact_governance import DeploymentStore, EvidenceStore, build_paper_portfolio_evidence
from trading_runtime.professional_flow import build_lineage_manifest


class StrategyContractTest(unittest.TestCase):
    def test_frozen_executable_resolves_workspace_strategy_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            trading_system = Path(tmp) / "trading-system"
            (trading_system / "packages" / "strategy-core").mkdir(parents=True)
            executable = trading_system / "apps" / "live_trader" / "release" / "LiveTrader.exe"
            executable.parent.mkdir(parents=True)
            with patch.object(sys, "frozen", True, create=True), patch.object(sys, "executable", str(executable)):
                resolved = resolve_trading_system_root()

        self.assertEqual(resolved, trading_system)

    def test_tampered_professional_lineage_blocks_live_capability(self) -> None:
        lineage = build_lineage_manifest(
            stage="backtest",
            producer="backtester",
            inputs={"datasetHash": "data", "strategyCodeHash": "code", "parameterHash": "params", "costModelHash": "cost"},
            created_at="2026-07-13T00:00:00+00:00",
        )
        lineage["inputs"]["parameterHash"] = "tampered"
        artifact = normalize_strategy_artifact({
            "id": "LINEAGE-TAMPERED",
            "lifecycle": {"status": "live"},
            "finalTest": {"status": "pass"},
            "permissions": {"live_allowed": True, "live_small_eligible": True, "live_eligible": True},
            "lineageManifest": lineage,
        })

        self.assertFalse(artifact["capabilities"]["canSubmitOrder"])
        self.assertTrue(artifact["lineage"]["blockingIssues"])
        self.assertTrue(any("lineage-hash-mismatch" in reason for reason in artifact["permissions"]["fail_reasons"]))

    def test_unapproved_portfolio_candidate_cannot_be_overridden_by_live_permissions(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "CANDIDATE-BLOCKED",
                "dataset": {"symbol": "BTCUSDT", "assetClass": "CRYPTO", "interval": "1h"},
                "lifecycle": {"status": "live"},
                "finalTest": {"status": "pass"},
                "permissions": {"live_allowed": True, "live_small_eligible": True, "live_eligible": True},
                "portfolioCandidate": {"candidateId": "candidate-1", "approved": False, "blockers": ["failed-stage:walk_forward"]},
            }
        )

        self.assertFalse(artifact["portfolio_candidate"]["approved"])
        self.assertFalse(artifact["live_small_eligible"])
        self.assertFalse(artifact["live_eligible"])
        self.assertFalse(artifact["capabilities"]["canSubmitOrder"])
        self.assertIn("portfolio-candidate-not-approved", artifact["permissions"]["fail_reasons"])

    def test_legacy_candidate_is_explicitly_grandfathered_and_portfolio_policy_is_normalized(self) -> None:
        strategy = normalize_strategy_artifact({"id": "LEGACY", "permissions": {}})
        portfolio = normalize_portfolio_artifact(
            {
                "id": "P1",
                "portfolioPolicy": {"policyHash": "policy-hash", "allocations": []},
                "advancedOperations": {"contentHash": "advanced-hash", "automaticDeRisk": {"capitalMultiplier": 0.5}},
            }
        )

        self.assertTrue(strategy["portfolio_candidate"]["legacyGrandfathered"])
        self.assertEqual(portfolio["portfolio_policy_hash"], "policy-hash")
        self.assertEqual(portfolio["advanced_operations_hash"], "advanced-hash")

    def test_backtester_custom_strategy_artifact_keeps_contract_fields(self) -> None:
        custom_definition = {
            "id": "custom-draft",
            "pluginId": "strategy_builder_custom",
            "definitionVersion": "custom-strategy-definition-v1",
            "entryRules": [{"left": "close", "operator": "above", "right": "value", "rightValue": 100}],
            "exitRules": [{"left": "close", "operator": "below", "right": "value", "rightValue": 95}],
        }

        artifact = normalize_strategy_artifact(
            {
                "id": "BT-CUSTOM-LIVE-001",
                "strategy_id": "BT-CUSTOM-LIVE-001",
                "name": "Custom Live Candidate",
                "dataset": {"symbol": "BTCUSDT", "assetClass": "CRYPTO", "interval": "1m"},
                "plugin": "custom-draft",
                "plugin_version": "1.0.0",
                "strategy_engine_version": "strategy-core-js-0.2.0",
                "strategy_plugin_contract_version": "strategy-plugin-contract-v1",
                "strategy_contract": {"contractVersion": "strategy-plugin-contract-v1", "customStrategyDefinition": custom_definition},
                "traderContract": {"contract_version": "trader-strategy-contract-v2"},
                "lifecycle": {"status": "paper"},
                "finalTest": {"status": "pass"},
                "permissions": {"trader_export_allowed": True, "live_allowed": True, "fail_reasons": []},
            }
        )

        self.assertEqual(artifact["plugin"], "strategy_builder_custom")
        self.assertEqual(artifact["plugin_label"], "Backtester Strategy Builder Custom")
        self.assertEqual(artifact["strategy_contract_version"], "strategy-plugin-contract-v1")
        self.assertEqual(artifact["strategy_engine_version"], "strategy-core-js-0.2.0")
        self.assertEqual(artifact["contract_version"], "trader-strategy-contract-v2")
        self.assertEqual(artifact["lifecycle_status"], "papered")
        self.assertEqual(artifact["final_test_status"], "pass")
        self.assertTrue(can_live_use_artifact(artifact))
        self.assertEqual(artifact["verification"]["backtester"]["status"], "pass")
        self.assertEqual(artifact["verification"]["paper_trader"]["status"], "pass")
        self.assertEqual(artifact["verification"]["live"]["status"], "fail")

    def test_custom_short_strategy_requires_a_shortable_market_contract(self) -> None:
        definition = {
            "id": "custom-short",
            "pluginId": "strategy_builder_custom",
            "positionDirection": "short",
            "entryRules": [
                {
                    "left": "close",
                    "operator": "below",
                    "right": "sma",
                    "rightPeriod": 20,
                }
            ],
            "exitRules": [
                {
                    "left": "close",
                    "operator": "above",
                    "right": "sma",
                    "rightPeriod": 20,
                }
            ],
        }
        base = {
            "id": "CUSTOM-SHORT",
            "dataset": {
                "symbol": "BTCUSDT",
                "assetClass": "CRYPTO",
                "interval": "1h",
            },
            "plugin": "strategy_builder_custom",
            "strategyContract": {
                "customStrategyDefinition": definition,
            },
        }

        spot = normalize_strategy_artifact({**base, "marketType": "spot"})
        futures = normalize_strategy_artifact(
            {**base, "id": "CUSTOM-SHORT-FUTURES", "marketType": "futures"}
        )

        self.assertEqual("short", spot["position_direction"])
        self.assertTrue(spot["allow_short_requested"])
        self.assertFalse(spot["allow_short"])
        self.assertEqual("short", futures["position_direction"])
        self.assertTrue(futures["allow_short_requested"])
        self.assertTrue(futures["allow_short"])

    def test_inverse_etf_uses_long_order_for_bearish_exposure(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "INVERSE-LONG",
                "symbol": "251340.KS",
                "name": "KODEX 코스닥150선물인버스",
                "marketType": "etf",
                "positionDirection": "long",
                "brokerId": "kis",
                "permissions": {
                    "live_allowed": True,
                    "live_small_eligible": True,
                    "live_eligible": True,
                },
            }
        )

        self.assertEqual("long", artifact["position_direction"])
        self.assertEqual("bearish", artifact["economic_exposure"])
        self.assertEqual("BUY", artifact["exposure_contract"]["entrySide"])
        self.assertEqual("KOSDAQ150_FUTURES", artifact["exposure_contract"]["underlyingBenchmark"])
        self.assertFalse(
            any(reason.startswith("exposure-contract-invalid") for reason in artifact["permissions"]["fail_reasons"])
        )

    def test_inverse_etf_short_contract_is_fail_closed(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "INVERSE-SHORT",
                "symbol": "251340.KS",
                "name": "KODEX 코스닥150선물인버스",
                "marketType": "etf",
                "positionDirection": "short",
                "permissions": {
                    "live_allowed": True,
                    "live_small_eligible": True,
                    "live_eligible": True,
                },
            }
        )

        self.assertFalse(artifact["live_small_eligible"])
        self.assertFalse(artifact["live_eligible"])
        self.assertIn(
            "exposure-contract-invalid:inverse-etf-requires-long-position",
            artifact["permissions"]["fail_reasons"],
        )

    def test_cross_provider_route_requires_reconciliation_before_live(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "YAHOO-KIS-PENDING",
                "symbol": "251340.KS",
                "name": "KODEX 코스닥150선물인버스",
                "marketType": "etf",
                "marketDataProvider": "yfinance",
                "brokerId": "kis",
                "positionDirection": "long",
                "permissions": {
                    "live_allowed": True,
                    "live_small_eligible": True,
                    "live_eligible": True,
                },
            }
        )

        self.assertEqual("pending", artifact["provider_reconciliation"]["status"])
        self.assertFalse(artifact["live_small_eligible"])
        self.assertIn(
            "provider-reconciliation-invalid:provider-reconciliation-pending",
            artifact["permissions"]["fail_reasons"],
        )

    def test_passed_cross_provider_reconciliation_is_accepted(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "YAHOO-KIS-PASS",
                "symbol": "251340.KS",
                "name": "KODEX 코스닥150선물인버스",
                "marketType": "etf",
                "marketDataProvider": "yfinance",
                "brokerId": "kis",
                "positionDirection": "long",
                "providerReconciliation": {
                    "schemaVersion": "provider-reconciliation-v1",
                    "required": True,
                    "status": "pass",
                    "passed": True,
                    "sourceProvider": "yahoo",
                    "executionProvider": "kis",
                    "blockers": [],
                },
                "permissions": {
                    "live_allowed": True,
                    "live_small_eligible": True,
                    "live_eligible": True,
                },
            }
        )

        self.assertEqual("pass", artifact["provider_reconciliation"]["status"])
        self.assertFalse(
            any(reason.startswith("provider-reconciliation-invalid") for reason in artifact["permissions"]["fail_reasons"])
        )

    def test_strategy_normalization_preserves_broker_routing_contract(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "UPBIT-ROUTE",
                "dataset": {
                    "symbol": "KRW-BTC",
                    "assetClass": "CRYPTO",
                    "interval": "5m",
                    "provider": "upbit",
                },
                "marketDataProvider": "upbit",
                "brokerId": "upbit",
                "traderContract": {
                    "contract_version": "trader-strategy-contract-v2",
                    "scope": {"allowed_brokers": ["upbit"]},
                },
            }
        )

        self.assertEqual("upbit", artifact["dataset_provider"])
        self.assertEqual("upbit", artifact["market_data_provider"])
        self.assertEqual("upbit", artifact["broker_id"])
        self.assertEqual(["upbit"], artifact["allowed_brokers"])

    def test_paused_lifecycle_exposes_deployment_resume_origin(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "PAUSED-ORIGIN",
                "lifecycle": {"status": "backtested"},
                "_deployment": {
                    "lifecycle": "paused",
                    "permissions": {
                        "pausedFrom": "before-live-small",
                        "live_small_eligible": False,
                        "live_eligible": False,
                        "live_allowed": False,
                    },
                },
            }
        )

        self.assertEqual("paused", artifact["lifecycle_status"])
        self.assertEqual("before-live-small", artifact["lifecycle"]["pausedFrom"])

    def test_unverified_backtester_artifact_stays_visible_with_verification_badges(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "BT-LIVE-WATCH-001",
                "name": "Watch Only Candidate",
                "dataset": {"symbol": "005930.KS", "assetClass": "KR-STOCK", "interval": "1d"},
                "plugin": "threshold_momentum",
                "lifecycle": {"status": "draft"},
                "permissions": {
                    "trader_export_allowed": False,
                    "live_allowed": False,
                    "fail_reasons": ["최종 검증 미통과"],
                },
            }
        )

        self.assertEqual(artifact["strategy_id"], "BT-LIVE-WATCH-001")
        self.assertFalse(can_live_use_artifact(artifact))
        self.assertEqual(artifact["verification"]["backtester"]["status"], "watch")
        self.assertEqual(artifact["verification"]["paper_trader"]["status"], "wait")
        self.assertEqual(artifact["verification"]["live"]["status"], "fail")
        self.assertIn("최종 검증 미통과", artifact["verification"]["backtester"]["detail"])

    def test_strategy_artifact_exposes_paper_portfolio_evidence(self) -> None:
        artifact = normalize_strategy_artifact(
            {
                "id": "PORT-EVIDENCE-1",
                "strategy_id": "STRAT-1",
                "dataset": {"symbol": "069500.KS", "assetClass": "KR-STOCK", "interval": "1d"},
                "plugin": "moving_average_cross",
                "lifecycle": {"status": "before-live-small"},
                "finalTest": {"status": "pass"},
                "permissions": {
                    "paper_trader_verified": True,
                    "live_small_eligible": True,
                    "fail_reasons": [],
                },
                "paperPortfolioEvidence": {
                    "required": True,
                    "ready": True,
                    "portfolioId": "portfolio-1",
                    "portfolioName": "Portfolio 1",
                    "status": "submitted",
                    "filledCount": 1,
                    "rejectedCount": 0,
                    "targetWeight": 0.25,
                },
            }
        )

        evidence = artifact["paper_portfolio_evidence"]
        self.assertTrue(evidence["required"])
        self.assertTrue(evidence["ready"])
        self.assertEqual(evidence["portfolioId"], "portfolio-1")
        self.assertEqual(evidence["filledCount"], 1)
        self.assertEqual(evidence["source"], "legacy-embedded")

    def test_external_evidence_and_deployment_override_legacy_artifact_lifecycle(self) -> None:
        previous_artifact = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp)
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
                strategy_payload = {
                    "id": "STRAT-EXT-1",
                    "strategy_id": "STRAT-EXT-1",
                    "name": "Immutable Strategy",
                    "dataset": {"symbol": "BTCUSDT", "assetClass": "CRYPTO", "interval": "5m"},
                    "plugin": "moving_average_cross",
                    "lifecycle": {"status": "backtested"},
                    "finalTest": {"status": "pass"},
                    "permissions": {"trader_export_allowed": True, "fail_reasons": []},
                    "lineageManifest": build_lineage_manifest(
                        stage="backtest",
                        producer="backtester",
                        inputs={"datasetHash": "dataset", "strategyCodeHash": "code", "parameterHash": "params", "costModelHash": "cost"},
                    ),
                }
                portfolio_payload = {
                    "artifactType": "portfolio",
                    "schemaVersion": "portfolio-artifact-v1",
                    "id": "PORT-EXT-1",
                    "name": "External Portfolio",
                    "strategyInstances": [{"strategyId": "STRAT-EXT-1", "symbol": "BTCUSDT"}],
                }
                (artifact_dir / "strategy.json").write_text(json.dumps(strategy_payload), encoding="utf-8")
                portfolio_dir = artifact_dir / "portfolios"
                portfolio_dir.mkdir()
                (portfolio_dir / "portfolio.json").write_text(json.dumps(portfolio_payload), encoding="utf-8")

                deployments = DeploymentStore(artifact_dir)
                definition = deployments.create_definition(
                    deployment_id="dep:STRAT-EXT-1:PORT-EXT-1:paper",
                    strategy_artifact=strategy_payload,
                    portfolio_artifact=portfolio_payload,
                    account_id="paper-main",
                    environment="PAPER",
                    symbol="BTCUSDT",
                    route="crypto",
                )
                deployments.transition(
                    definition["deploymentId"],
                    lifecycle="before-live-small",
                    mode="MONITOR",
                    actor="unit-test",
                    reason="paper pass",
                    permissions={
                        "trader_export_allowed": True,
                        "paper_trader_verified": True,
                        "live_small_eligible": True,
                        "live_allowed": False,
                        "fail_reasons": [],
                    },
                )
                evidence = build_paper_portfolio_evidence(
                    evidence_id="paper-ext-1",
                    strategy_artifact=strategy_payload,
                    portfolio_artifact=portfolio_payload,
                    deployment_id=definition["deploymentId"],
                    filled_count=4,
                    rejected_count=0,
                    order_count=4,
                    target_weight=0.2,
                    details={
                        "lineageManifest": build_lineage_manifest(
                            stage="paper",
                            producer="paper_trader",
                            inputs={"strategyArtifactHash": "strategy", "runtimeVersion": "paper-v1", "startedAt": "start", "endedAt": "end"},
                            parent={"stage": "backtest", "contentHash": strategy_payload["lineageManifest"]["contentHash"]},
                        )
                    },
                )
                EvidenceStore(artifact_dir).save_paper(evidence)

                enriched = enrich_strategy_artifact_runtime(artifact_dir, artifact_dir / "strategy.json", strategy_payload)
                self.assertTrue(normalize_strategy_artifact(enriched)["lineage"]["paper"]["valid"])
                strategies = load_strategy_artifacts()

            loaded = next(item for item in strategies if item["strategy_id"] == "STRAT-EXT-1")
            self.assertEqual("before-live-small", loaded["lifecycle_status"])
            self.assertEqual(definition["deploymentId"], loaded["deployment_id"])
            self.assertEqual("deployment-registry", loaded["deployment_source"])
            self.assertTrue(loaded["permissions"]["paper_trader_verified"])
            self.assertTrue(loaded["live_small_eligible"])
            self.assertTrue(loaded["paper_portfolio_evidence"]["ready"])
            self.assertEqual("external", loaded["paper_portfolio_evidence"]["source"])
            self.assertEqual("paper-ext-1", loaded["paper_portfolio_evidence"]["evidenceId"])
            self.assertTrue(loaded["lineage"]["backtest"]["valid"])
            self.assertTrue(loaded["lineage"]["paper"]["valid"])
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact

    def test_strategy_paths_can_be_configured_from_shared_environment(self) -> None:
        previous_artifact = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        previous_plugin = os.environ.get("LIVE_TRADER_STRATEGY_PLUGIN_DIR")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp) / "shared-strategies"
                plugin_dir = artifact_dir / "plugins"
                plugin_dir.mkdir(parents=True)
                (plugin_dir / "unit_strategy.py").write_text("PLUGIN_ID = 'unit'\n", encoding="utf-8")
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
                os.environ["LIVE_TRADER_STRATEGY_PLUGIN_DIR"] = str(plugin_dir)

                self.assertEqual(strategy_artifact_dirs()[0], artifact_dir)
                self.assertEqual(strategy_plugin_dirs()[0], plugin_dir)
                self.assertEqual(strategy_plugin_status()[0]["count"], 1)
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact
            if previous_plugin is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_PLUGIN_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_PLUGIN_DIR"] = previous_plugin

    def test_portfolio_artifacts_load_from_shared_strategy_directory(self) -> None:
        previous_artifact = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp)
                portfolio_dir = artifact_dir / "portfolios"
                portfolio_dir.mkdir()
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
                (portfolio_dir / "portfolio.json").write_text(
                    json.dumps(
                        {
                            "artifactType": "portfolio",
                            "schemaVersion": "portfolio-artifact-v1",
                            "id": "portfolio-live-1",
                            "name": "Portfolio Live 1",
                            "lifecycle": {"status": "before-live-small"},
                            "permissions": {"live_small_allowed": True, "fail_reasons": []},
                            "strategyInstances": [
                                {
                                    "strategyId": "STRAT-1",
                                    "symbol": "069500.KS",
                                    "allocation": {"normalizedWeight": 0.2},
                                }
                            ],
                            "framework": {
                                "targetPortfolio": [
                                    {"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.2}
                                ],
                                "riskChecks": [{"label": "unit", "status": "pass"}],
                            },
                            "riskPolicy": {"maxSingleSymbolWeight": 0.25, "maxStrategyWeight": 0.5},
                        },
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )

                portfolios = load_portfolio_artifacts()

            self.assertEqual(len(portfolios), 1)
            self.assertEqual(portfolios[0]["id"], "portfolio-live-1")
            self.assertEqual(portfolios[0]["lifecycle_status"], "before-live-small")
            self.assertTrue(portfolios[0]["permissions"]["live_small_allowed"])
            self.assertEqual(portfolios[0]["target_portfolio"][0]["targetWeight"], 0.2)
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact


if __name__ == "__main__":
    unittest.main()
