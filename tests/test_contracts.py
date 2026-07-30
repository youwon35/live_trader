import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import live_trader.contracts as contracts
from live_trader.contracts import (
    can_live_small_use_artifact,
    can_live_use_artifact,
    enrich_strategy_artifact_runtime,
    load_portfolio_artifacts,
    load_strategy_artifacts,
    normalize_portfolio_artifact,
    normalize_strategy_artifact,
    resolve_trading_system_root,
    sample_strategy_artifacts,
    strategy_artifact_dirs,
    strategy_plugin_dirs,
    strategy_plugin_status,
)
from trading_runtime.artifact_governance import (
    DeploymentStore,
    EvidenceStore,
    build_paper_portfolio_evidence,
    seal_portfolio_artifact,
    seal_strategy_artifact,
)
from trading_runtime.professional_flow import build_lineage_manifest


class StrategyContractTest(unittest.TestCase):
    def test_ui_demo_sample_strategies_are_never_live_candidates(self) -> None:
        samples = sample_strategy_artifacts()

        self.assertTrue(samples)
        for sample in samples:
            self.assertTrue(sample["ui_demo_only"])
            self.assertEqual(
                contracts.UI_DEMO_ARTIFACT_ORIGIN,
                sample["artifact_origin"],
            )
            self.assertFalse(can_live_small_use_artifact(sample))
            self.assertFalse(can_live_use_artifact(sample))

            forged_permissions = {
                **sample.get("permissions", {}),
                "live_small_eligible": True,
                "live_eligible": True,
                "live_allowed": True,
            }
            forged_sample = {
                **sample,
                "permissions": forged_permissions,
                "live_small_eligible": True,
                "live_eligible": True,
                "live_allowed": True,
            }
            self.assertFalse(can_live_small_use_artifact(forged_sample))
            self.assertFalse(can_live_use_artifact(forged_sample))

            legacy_sample = {
                key: value
                for key, value in forged_sample.items()
                if key not in {"artifact_origin", "ui_demo_only"}
            }
            legacy_sample["source_path"] = "sample"
            self.assertFalse(can_live_small_use_artifact(legacy_sample))
            self.assertFalse(can_live_use_artifact(legacy_sample))

    def test_dataset_lineage_requires_tracked_revision_and_preserves_metadata(
        self,
    ) -> None:
        parent = {
            "stage": "dataset",
            "contentHash": "rev-adjusted-123",
            "schemaVersion": "dataset-lineage-v1",
            "tracked": True,
            "datasetId": "ds-123",
            "lineageRunId": "run-123",
            "sourceStage": "adjusted",
            "stageRevisionId": "rev-adjusted-123",
            "parentStageRevisionId": "rev-processed-123",
            "dependencyStageRevisionIds": ["rev-daily-123"],
            "rawContentSha256": "a" * 64,
            "rawMetadataSha256": "b" * 64,
            "transformationId": "stock-data-scraper/adjusted/v1",
        }
        lineage = build_lineage_manifest(
            stage="backtest",
            producer="backtester",
            inputs={
                "datasetHash": "rev-adjusted-123",
                "strategyCodeHash": "code",
                "parameterHash": "params",
                "costModelHash": "cost",
            },
            parent=parent,
        )
        normalized = normalize_strategy_artifact(
            {
                "id": "LINEAGE-STRICT",
                "lineageManifest": lineage,
            }
        )

        dataset = normalized["lineage"]["dataset"]
        self.assertTrue(dataset["valid"], dataset["issues"])
        self.assertEqual("dataset-lineage-v1", dataset["schemaVersion"])
        self.assertTrue(dataset["tracked"])
        self.assertEqual(
            ["rev-daily-123"],
            dataset["dependencyStageRevisionIds"],
        )
        self.assertEqual("a" * 64, dataset["rawContentSha256"])
        self.assertEqual(parent, dataset["parent"])

        mismatched = dict(parent)
        mismatched["contentHash"] = "some-other-hash"
        blocked = normalize_strategy_artifact(
            {
                "id": "LINEAGE-MISMATCH",
                "lifecycle": {"status": "live"},
                "finalTest": {"status": "pass"},
                "permissions": {
                    "paper_trader_verified": True,
                    "live_small_eligible": True,
                    "live_eligible": True,
                    "live_allowed": True,
                },
                "lineageManifest": build_lineage_manifest(
                    stage="backtest",
                    producer="backtester",
                    inputs={
                        "datasetHash": "some-other-hash",
                        "strategyCodeHash": "code",
                        "parameterHash": "params",
                        "costModelHash": "cost",
                    },
                    parent=mismatched,
                ),
            }
        )
        self.assertFalse(blocked["lineage"]["dataset"]["valid"])
        self.assertFalse(blocked["live_small_eligible"])
        self.assertTrue(
            any(
                "content-hash-stage-revision-mismatch" in issue
                for issue in blocked["lineage"]["blockingIssues"]
            )
        )

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
            seal_portfolio_artifact({
                "artifactType": "portfolio",
                "schemaVersion": "portfolio-artifact-v1",
                "id": "P1",
                "portfolioPolicy": {"policyHash": "policy-hash", "allocations": []},
                "advancedOperations": {"contentHash": "advanced-hash", "automaticDeRisk": {"capitalMultiplier": 0.5}},
            })
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
                strategy_payload = seal_strategy_artifact({
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
                        parent={
                            "stage": "dataset",
                            "contentHash": "dataset",
                            "schemaVersion": "dataset-lineage-v1",
                            "tracked": True,
                            "datasetId": "dataset-ext-1",
                            "lineageRunId": "run-ext-1",
                            "sourceStage": "adjusted",
                            "stageRevisionId": "dataset",
                            "parentStageRevisionId": "processed-ext-1",
                            "dependencyStageRevisionIds": [],
                            "rawContentSha256": "a" * 64,
                            "rawMetadataSha256": "b" * 64,
                            "transformationId": "stock-data-scraper/adjusted/v1",
                        },
                    ),
                })
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

    def test_default_artifact_migration_runs_once_per_process_path(self) -> None:
        previous_artifact = os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
        previous_trader_artifact = os.environ.pop("TRADER_STRATEGY_ARTIFACT_DIR", None)
        previous_key = contracts._ARTIFACT_MIGRATION_KEY
        try:
            with tempfile.TemporaryDirectory() as tmp:
                source = Path(tmp) / "legacy"
                target = Path(tmp) / "primary"
                source.mkdir()
                contracts._ARTIFACT_MIGRATION_KEY = ""
                with patch.object(contracts, "LEGACY_STRATEGY_ARTIFACT_DIR", source), patch.object(
                    contracts,
                    "PRIMARY_STRATEGY_ARTIFACT_DIR",
                    target,
                ), patch.object(contracts, "migrate_artifact_tree") as migrate:
                    strategy_artifact_dirs()
                    strategy_artifact_dirs()
                    strategy_plugin_dirs()

                migrate.assert_called_once_with(source, target)
        finally:
            contracts._ARTIFACT_MIGRATION_KEY = previous_key
            if previous_artifact is not None:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact
            if previous_trader_artifact is not None:
                os.environ["TRADER_STRATEGY_ARTIFACT_DIR"] = previous_trader_artifact

    def test_strategy_artifacts_load_beyond_legacy_limit_and_dedupe_mirrors(self) -> None:
        previous_artifact = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                primary = Path(tmp) / "primary"
                mirror = Path(tmp) / "mirror"
                primary.mkdir(parents=True)
                mirror.mkdir(parents=True)
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = os.pathsep.join(
                    (str(primary), str(mirror))
                )

                for index in range(25):
                    payload = {
                        "artifactType": "strategy",
                        "schemaVersion": "market-strategy-v1",
                        "id": f"strategy-{index:02d}",
                        "name": f"Strategy {index:02d}",
                        "dataset": {
                            "symbol": f"TEST{index:02d}",
                            "interval": "1h",
                        },
                    }
                    encoded = json.dumps(payload, ensure_ascii=False)
                    (primary / f"strategy-{index:02d}.json").write_text(
                        encoded,
                        encoding="utf-8",
                    )
                    if index == 0:
                        (mirror / "strategy-00.json").write_text(
                            encoded,
                            encoding="utf-8",
                        )

                strategies = load_strategy_artifacts()
                limited = load_strategy_artifacts(limit=10)
                empty = load_strategy_artifacts(limit=0)

            self.assertEqual(len(strategies), 25)
            self.assertEqual(
                len({strategy["strategy_id"] for strategy in strategies}),
                25,
            )
            self.assertEqual(len(limited), 10)
            self.assertEqual(empty, [])
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact

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
                        seal_portfolio_artifact({
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
                        }),
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
            self.assertTrue(portfolios[0]["artifact_integrity"]["valid"])
            self.assertTrue(portfolios[0]["live_usable"])
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact

    def test_unsealed_portfolio_remains_visible_but_is_live_blocked(self) -> None:
        previous_artifact = os.environ.get(
            "LIVE_TRADER_STRATEGY_ARTIFACT_DIR"
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp)
                portfolio_dir = artifact_dir / "portfolios"
                portfolio_dir.mkdir()
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(
                    artifact_dir
                )
                payload = {
                    "artifactType": "portfolio",
                    "schemaVersion": "portfolio-artifact-v1",
                    "id": "legacy-unsealed",
                    "name": "Legacy Unsealed",
                    "lifecycle": {"status": "live"},
                    "permissions": {
                        "live_small_allowed": True,
                        "live_allowed": True,
                    },
                    "strategyInstances": [
                        {
                            "strategyId": "STRAT-1",
                            "symbol": "BTCUSDT",
                            "allocation": {"normalizedWeight": 1.0},
                        }
                    ],
                    "framework": {
                        "targetPortfolio": [
                            {
                                "strategyId": "STRAT-1",
                                "symbol": "BTCUSDT",
                                "targetWeight": 1.0,
                            }
                        ],
                        "riskChecks": [
                            {"label": "forged-pass", "status": "pass"}
                        ],
                    },
                    "riskPolicy": {"maxSingleSymbolWeight": 1.0},
                    "portfolioPolicy": {
                        "policyHash": "untrusted",
                        "allocations": [],
                    },
                    "advancedOperations": {
                        "contentHash": "untrusted",
                        "automaticDeRisk": {"capitalMultiplier": 1.0},
                    },
                }
                (portfolio_dir / "legacy-unsealed.json").write_text(
                    json.dumps(payload, ensure_ascii=False),
                    encoding="utf-8",
                )

                portfolios = load_portfolio_artifacts()

            self.assertEqual(1, len(portfolios))
            portfolio = portfolios[0]
            self.assertEqual("legacy-unsealed", portfolio["id"])
            self.assertFalse(portfolio["artifact_integrity"]["valid"])
            self.assertTrue(
                portfolio["artifact_integrity"]["requiresRepublication"]
            )
            self.assertIn(
                "canonical-lock-missing",
                portfolio["artifact_integrity"]["issues"],
            )
            self.assertFalse(
                portfolio["permissions"]["live_small_allowed"]
            )
            self.assertFalse(portfolio["permissions"]["live_allowed"])
            self.assertFalse(portfolio["live_usable"])
            self.assertEqual(
                [{"strategyId": "STRAT-1", "symbol": "BTCUSDT"}],
                portfolio["strategy_instances"],
            )
            self.assertEqual([], portfolio["target_portfolio"])
            self.assertEqual({}, portfolio["risk_policy"])
            self.assertEqual({}, portfolio["portfolio_policy"])
            self.assertEqual({}, portfolio["advanced_operations"])
            self.assertEqual(
                "fail",
                portfolio["risk_checks"][0]["status"],
            )
            self.assertTrue(
                any(
                    "artifact-integrity:canonical-lock-missing" in reason
                    for reason in portfolio["permissions"]["fail_reasons"]
                )
            )
        finally:
            if previous_artifact is None:
                os.environ.pop(
                    "LIVE_TRADER_STRATEGY_ARTIFACT_DIR",
                    None,
                )
            else:
                os.environ[
                    "LIVE_TRADER_STRATEGY_ARTIFACT_DIR"
                ] = previous_artifact

    def test_tampered_sealed_portfolio_cannot_supply_live_controls(
        self,
    ) -> None:
        previous_artifact = os.environ.get(
            "LIVE_TRADER_STRATEGY_ARTIFACT_DIR"
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                artifact_dir = Path(tmp)
                portfolio_dir = artifact_dir / "portfolios"
                portfolio_dir.mkdir()
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(
                    artifact_dir
                )
                payload = seal_portfolio_artifact(
                    {
                        "artifactType": "portfolio",
                        "schemaVersion": "portfolio-artifact-v1",
                        "id": "portfolio-tampered",
                        "lifecycle": {"status": "live"},
                        "permissions": {
                            "live_small_allowed": True,
                            "live_allowed": True,
                        },
                        "strategyInstances": [
                            {
                                "strategyId": "STRAT-1",
                                "symbol": "BTCUSDT",
                                "allocation": {"normalizedWeight": 0.1},
                            }
                        ],
                        "framework": {
                            "targetPortfolio": [
                                {
                                    "strategyId": "STRAT-1",
                                    "symbol": "BTCUSDT",
                                    "targetWeight": 0.1,
                                }
                            ]
                        },
                        "riskPolicy": {
                            "maxSingleSymbolWeight": 0.1
                        },
                    }
                )
                payload["framework"]["targetPortfolio"][0][
                    "targetWeight"
                ] = 1.0
                (portfolio_dir / "portfolio-tampered.json").write_text(
                    json.dumps(payload, ensure_ascii=False),
                    encoding="utf-8",
                )

                portfolios = load_portfolio_artifacts()

            self.assertEqual(1, len(portfolios))
            portfolio = portfolios[0]
            self.assertFalse(portfolio["artifact_integrity"]["valid"])
            self.assertIn(
                "canonical-lock-content-hash-mismatch",
                portfolio["artifact_integrity"]["issues"],
            )
            self.assertFalse(portfolio["permissions"]["live_allowed"])
            self.assertFalse(
                portfolio["permissions"]["live_small_allowed"]
            )
            self.assertEqual([], portfolio["target_portfolio"])
            self.assertEqual({}, portfolio["risk_policy"])
        finally:
            if previous_artifact is None:
                os.environ.pop(
                    "LIVE_TRADER_STRATEGY_ARTIFACT_DIR",
                    None,
                )
            else:
                os.environ[
                    "LIVE_TRADER_STRATEGY_ARTIFACT_DIR"
                ] = previous_artifact

    def test_legacy_v1_portfolio_lock_is_display_only(self) -> None:
        payload = seal_portfolio_artifact(
            {
                "artifactType": "portfolio",
                "schemaVersion": "portfolio-artifact-v1",
                "id": "portfolio-legacy-lock",
                "lifecycle": {"status": "live"},
                "permissions": {
                    "live_small_allowed": True,
                    "live_allowed": True,
                },
                "strategyInstances": [
                    {
                        "strategyId": "STRAT-LEGACY",
                        "symbol": "BTCUSDT",
                        "allocation": {"normalizedWeight": 0.25},
                    }
                ],
                "riskPolicy": {"maxSingleSymbolWeight": 0.25},
            }
        )
        payload["artifactLock"][
            "schemaVersion"
        ] = "portfolio-artifact-lock-v1"

        portfolio = normalize_portfolio_artifact(payload)

        self.assertEqual("portfolio-legacy-lock", portfolio["id"])
        self.assertFalse(portfolio["artifact_integrity"]["valid"])
        self.assertTrue(
            portfolio["artifact_integrity"]["requiresRepublication"]
        )
        self.assertIn(
            "canonical-lock-schema-mismatch:"
            "portfolio-artifact-lock-v1->portfolio-artifact-lock-v2",
            portfolio["artifact_integrity"]["issues"],
        )
        self.assertFalse(portfolio["live_usable"])
        self.assertFalse(portfolio["permissions"]["live_allowed"])
        self.assertEqual(
            [
                {
                    "strategyId": "STRAT-LEGACY",
                    "symbol": "BTCUSDT",
                }
            ],
            portfolio["strategy_instances"],
        )
        self.assertEqual({}, portfolio["risk_policy"])

    def test_portfolio_artifacts_load_beyond_legacy_limit_and_dedupe_mirrors(self) -> None:
        previous_artifact = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                primary = Path(tmp) / "primary"
                mirror = Path(tmp) / "mirror"
                primary_portfolios = primary / "portfolios"
                mirror_portfolios = mirror / "portfolios"
                primary_portfolios.mkdir(parents=True)
                mirror_portfolios.mkdir(parents=True)
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = os.pathsep.join((str(primary), str(mirror)))

                for index in range(25):
                    payload = seal_portfolio_artifact({
                        "artifactType": "portfolio",
                        "schemaVersion": "portfolio-artifact-v1",
                        "id": f"portfolio-{index:02d}",
                        "name": f"Portfolio {index:02d}",
                    })
                    encoded = json.dumps(payload, ensure_ascii=False)
                    (primary_portfolios / f"portfolio-{index:02d}.json").write_text(encoded, encoding="utf-8")
                    if index == 0:
                        (mirror_portfolios / "portfolio-00.json").write_text(encoded, encoding="utf-8")

                portfolios = load_portfolio_artifacts()
                limited = load_portfolio_artifacts(limit=10)
                empty = load_portfolio_artifacts(limit=0)

            self.assertEqual(len(portfolios), 25)
            self.assertEqual(len({portfolio["id"] for portfolio in portfolios}), 25)
            self.assertEqual(len(limited), 10)
            self.assertEqual(empty, [])
        finally:
            if previous_artifact is None:
                os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
            else:
                os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact


if __name__ == "__main__":
    unittest.main()
