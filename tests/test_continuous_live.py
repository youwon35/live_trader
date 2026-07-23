import unittest

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


if __name__ == "__main__":
    unittest.main()
