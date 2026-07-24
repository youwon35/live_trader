import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader import state  # noqa: F401 - initializes the shared runtime path
from live_trader.continuous_live import LiveContinuousController


class LiveContinuousControllerTest(unittest.TestCase):
    def test_standalone_strategy_preserves_binance_runtime_contract(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "instance_id": "si-btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "provider": "binance",
            "broker_id": "binance",
            "plugin": "ma-cross",
            "artifact_hash": "abc123",
            "parameters": {
                "shortMa": 36,
                "longMa": 240,
                "positionSize": 20,
                "paperOrderQuantity": 0.0001,
            },
        }

        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertEqual("standalone:btc-live-small", spec.portfolio_id)
        self.assertEqual("si-btc-live-small", spec.strategy_instance_id)
        self.assertEqual("BTCUSDT", spec.instrument_id)
        self.assertEqual("1h", spec.timeframe)
        self.assertEqual("binance", spec.provider)
        self.assertEqual("binance", spec.broker_id)
        self.assertEqual(0.2, spec.target_weight)
        self.assertEqual(0.0001, spec.parameters["paperOrderQuantity"])

    def test_mode_permission_accepts_live_small_but_not_full_live(self) -> None:
        strategy = {
            "strategy_id": "btc-live-small",
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "plugin": "ma-cross",
            "parameters": {},
            "permissions": {
                "live_small_eligible": True,
                "live_eligible": False,
            },
        }
        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertTrue(LiveContinuousController._spec_mode_allowed(spec, "MONITOR"))
        self.assertTrue(LiveContinuousController._spec_mode_allowed(spec, "SMALL_LIVE"))
        self.assertFalse(LiveContinuousController._spec_mode_allowed(spec, "FULL_LIVE"))

    def test_live_notional_does_not_reuse_large_paper_quantity(self) -> None:
        strategy = {
            "strategy_id": "eth-live-small",
            "symbol": "ETHUSDT",
            "timeframe": "5m",
            "plugin": "ma-cross",
            "parameters": {
                "paperOrderQuantity": 100,
                "liveOrderNotionalUsdt": 5.5,
            },
        }
        spec = LiveContinuousController._standalone_spec(strategy)

        self.assertAlmostEqual(
            5.5 / 4_000,
            LiveContinuousController._order_quantity(spec, 4_000),
        )

    @staticmethod
    def _portfolio() -> dict:
        return {
            "id": "portfolio-btc",
            "lifecycle_status": "backtested",
            "source_path": "portfolio.json",
            "strategy_instances": [
                {
                    "sourceStrategyId": "btc-strategy",
                    "sourceArtifactHash": "artifact-hash",
                    "symbol": "BTCUSDT",
                }
            ],
        }

    @staticmethod
    def _strategy(lifecycle: str = "backtested") -> dict:
        return {
            "strategy_id": "btc-strategy",
            "artifact_hash": "artifact-hash",
            "lifecycle_status": lifecycle,
            "backtester_verified": True,
            "live_small_eligible": lifecycle == "before-live-small",
            "live_eligible": lifecycle == "live",
        }

    def test_monitor_ignores_portfolio_with_retired_component(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("retired")]),
        ):
            selected = controller._select_portfolio("crypto", "", "MONITOR")

        self.assertIsNone(selected)

    def test_monitor_accepts_backtested_portfolio_components(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("backtested")]),
        ):
            selected = controller._select_portfolio("crypto", "", "MONITOR")

        self.assertEqual("portfolio-btc", selected["id"])

    def test_small_live_requires_component_live_small_eligibility(self) -> None:
        controller = LiveContinuousController(Path("."))
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("backtested")]),
        ):
            blocked = controller._select_portfolio("crypto", "", "SMALL_LIVE")
        with (
            patch.object(state, "portfolio_rows", return_value=[self._portfolio()]),
            patch.object(state, "strategy_rows", return_value=[self._strategy("before-live-small")]),
        ):
            allowed = controller._select_portfolio("crypto", "", "SMALL_LIVE")

        self.assertIsNone(blocked)
        self.assertEqual("portfolio-btc", allowed["id"])


if __name__ == "__main__":
    unittest.main()
