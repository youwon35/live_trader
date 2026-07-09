import json
import os
import tempfile
import unittest
from pathlib import Path

from live_trader.contracts import (
    can_live_use_artifact,
    load_portfolio_artifacts,
    normalize_strategy_artifact,
    strategy_artifact_dirs,
    strategy_plugin_dirs,
    strategy_plugin_status,
)


class StrategyContractTest(unittest.TestCase):
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
