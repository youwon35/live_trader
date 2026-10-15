from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.market_routes import market_asset, market_broker_id, market_profile


class MarketRouteClassificationTest(unittest.TestCase):
    def test_us_stock_and_etf_tickers_are_not_crypto_substrings(self):
        for symbol in ("ETHA", "ETHU", "QETH", "BTCETF", "METHA"):
            with self.subTest(symbol=symbol):
                self.assertEqual("stock", market_profile(symbol))
                self.assertEqual("미국주식", state.asset_from_symbol(symbol))
                self.assertEqual("kis", state.broker_id_from_symbol(symbol, "미국주식"))
                self.assertTrue(LiveContinuousController._symbol_matches_profile(symbol, "stock"))
                self.assertFalse(LiveContinuousController._symbol_matches_profile(symbol, "crypto"))

    def test_canonical_crypto_pairs_preserve_broker_and_futures_routes(self):
        for symbol, broker in (("ETHUSDT", "binance"), ("BTC-USDT", "binance"), ("ETHBUSD", "binance"), ("ETHUSDT.PERP", "binance-futures"), ("KRW-ETH", "upbit")):
            with self.subTest(symbol=symbol):
                self.assertEqual("crypto", market_profile(symbol))
                self.assertEqual(broker, market_broker_id(symbol))
                self.assertEqual("코인", state.asset_from_symbol(symbol))
                self.assertEqual(broker, state.broker_id_from_symbol(symbol, "CRYPTO"))
        self.assertEqual("binance-futures", market_broker_id("ETHUSDT", market_type="futures"))

    def test_ambiguous_bare_base_requires_explicit_market_identity(self):
        for symbol in ("BTC", "ETH", ""):
            with self.subTest(symbol=symbol):
                self.assertEqual("", market_profile(symbol))
                self.assertEqual("", state.broker_id_from_symbol(symbol, "CRYPTO"))
                self.assertEqual("미확인", state.asset_from_symbol(symbol))
                self.assertFalse(LiveContinuousController._symbol_matches_profile(symbol, "stock"))
                self.assertFalse(LiveContinuousController._symbol_matches_profile(symbol, "crypto"))
        self.assertEqual("kis", state.broker_id_from_symbol("BTC", "미국주식"))
        self.assertEqual("미국주식", state.asset_from_symbol("BTC", broker_id="kis"))

    def test_authoritative_broker_precedes_provider_and_symbol_heuristics(self):
        self.assertEqual("stock", market_profile("ETHUSDT", broker_id="kis", provider="binance"))
        self.assertEqual("미국주식", market_asset("ETHUSDT", broker_id="kis", provider="binance"))
        self.assertEqual("crypto", market_profile("ETHA", broker_id="upbit", provider="yahoo"))
        self.assertEqual("stock", market_profile("BTC", provider="yahoo"))
        self.assertEqual("crypto", market_profile("ETH", provider="binance"))

    def test_unknown_explicit_route_is_not_reinterpreted(self):
        self.assertEqual("", market_profile("ETHUSDT", broker_id="unknown"))
        self.assertEqual("", market_profile("KRW-ETH", provider="unknown"))
        self.assertEqual("", market_profile("ETHA", asset="CRYPTO"))
        self.assertEqual("", market_profile("ETHA", market_type="futures"))

    def test_domestic_stock_suffixes_preserve_stock_asset(self):
        for symbol in ("005930", "005930.KS", "251340.KQ", "KRX:005930"):
            with self.subTest(symbol=symbol):
                self.assertEqual("한국주식", state.asset_from_symbol(symbol, broker_id="kis"))
                self.assertEqual("stock", market_profile(symbol, broker_id="kis"))

    def test_loaded_portfolio_specs_use_broker_before_ticker(self):
        controller = LiveContinuousController(Path("."))
        specs = (
            SimpleNamespace(symbol="BTC", broker_id="kis", provider="yahoo"),
            SimpleNamespace(symbol="ETHA", broker_id="kis", provider="yahoo"),
            SimpleNamespace(symbol="ETHUSDT", broker_id="binance", provider="binance"),
            SimpleNamespace(symbol="KRW-ETH", broker_id="upbit", provider="upbit"),
        )
        self.assertEqual(["BTC", "ETHA"], [s.symbol for s in specs if controller._matches_profile(s, "stock")])
        self.assertEqual(["ETHUSDT", "KRW-ETH"], [s.symbol for s in specs if controller._matches_profile(s, "crypto")])

    def test_standalone_candidate_uses_its_explicit_kis_route(self):
        controller = LiveContinuousController(Path("."))
        strategy = {"strategy_id": "btc-stock", "symbol": "BTC", "broker_id": "kis", "provider": "yahoo", "lifecycle_status": "backtested", "backtester_verified": True}
        with patch.object(state, "strategy_rows", return_value=[strategy]):
            self.assertIs(strategy, controller._select_standalone_strategy("stock", "MONITOR"))
            self.assertIsNone(controller._select_standalone_strategy("crypto", "MONITOR"))

    def test_portfolio_candidate_inherits_exact_source_route_and_member_override(self):
        controller = LiveContinuousController(Path("."))
        source = {"strategy_id": "btc-source", "artifact_hash": "hash", "broker_id": "kis", "provider": "yahoo"}
        member = {"sourceStrategyId": "btc-source", "sourceArtifactHash": "hash", "symbol": "BTC"}
        portfolio = {"id": "portfolio", "strategy_instances": [member]}
        with patch.object(state, "strategy_rows", return_value=[source]), patch.object(state, "portfolio_rows", return_value=[portfolio]), patch.object(controller, "_portfolio_components_eligible", return_value=True):
            self.assertEqual([portfolio], controller._portfolio_candidates("stock", ""))
            self.assertEqual([], controller._portfolio_candidates("crypto", ""))
            member["brokerId"] = "binance"
            self.assertEqual([], controller._portfolio_candidates("stock", ""))
            self.assertEqual([portfolio], controller._portfolio_candidates("crypto", ""))

    def test_kis_stock_signal_keeps_stock_asset_and_broker_without_dispatch(self):
        controller = LiveContinuousController(Path("."))
        spec = controller._standalone_spec({"strategy_id": "btc-stock", "symbol": "BTC", "broker_id": "kis", "provider": "yahoo", "parameters": {}})
        controller.profile_id = "stock"
        controller.mode = "SMALL_LIVE"
        controller.requested_strategy_id = spec.strategy_id
        controller.strategy_ids = (spec.strategy_id,)
        controller.allowed_symbols = (spec.symbol,)
        controller.supervisor = SimpleNamespace(engine=SimpleNamespace(specs=(spec,)))
        decision = SimpleNamespace(strategy_id=spec.strategy_id, strategy_instance_id=spec.strategy_instance_id, signal="BUY", reason="fixture", evaluation_key="fixture-evaluation", bar=SimpleNamespace(close=100, end_time="2026-10-16T00:00:00Z"))
        captured = []
        with patch.object(state, "broker_position_quantity", return_value=0), patch.object(state, "append_audit"), patch.object(controller, "_submit_cycle_intent", side_effect=lambda intent, **kw: captured.append(intent) or {"ok": True, "reason": "FIXTURE_ONLY"}), patch.dict(state.STATE, {"strategy_runner": {}}):
            controller._handle_cycle_locked(SimpleNamespace(decisions=(decision,)))
        self.assertEqual(1, len(captured))
        self.assertEqual("미국주식", captured[0].asset)
        self.assertEqual("kis", captured[0].metadata["broker_id"])


if __name__ == "__main__":
    unittest.main()
