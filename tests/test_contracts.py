import os
import tempfile
import unittest
from pathlib import Path

from live_trader.contracts import (
    can_live_use_artifact,
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
        self.assertEqual(artifact["lifecycle_status"], "paper")
        self.assertEqual(artifact["final_test_status"], "pass")
        self.assertTrue(can_live_use_artifact(artifact))

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


if __name__ == "__main__":
    unittest.main()
