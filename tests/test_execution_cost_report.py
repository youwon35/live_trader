from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from live_trader.execution_cost_report import read_execution_cost_report
from live_trader.program_ledger import ProgramLedger
from live_trader.verified_terminal_cache import VerifiedTerminalCache, terminal_identity, terminal_proof
from trading_runtime.observed_execution_costs import research_cost_settings


ARTIFACT = "a" * 64
NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
SCOPE = {
    "deploymentId": "demo-fixture",
    "accountProfileId": "fixture",
    "accountFingerprint": "b" * 64,
    "credentialConfigurationHash": "c" * 64,
    "scopeHash": "d" * 64,
    "quoteCurrency": "USDT",
}
SPEC = SimpleNamespace(
    broker_id="binance", artifact_hash=ARTIFACT, portfolio_hash="e" * 64,
    symbol="BTCUSDT", strategy_instance_id="fixture-strategy",
)


class ExecutionCostReportTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.ledger = ProgramLedger(self.root / "ledger.sqlite3")
        self.cache = VerifiedTerminalCache(self.root / "cache.sqlite3")

    def record(self, index, *, attested=True):
        side, price = ("BUY", "101") if index % 2 == 0 else ("SELL", "99")
        observed = NOW + timedelta(minutes=5 * index)
        dispatch = {
            "order_id": f"local-{index}", "idempotency_key": f"key-{index}", "created_at": observed.isoformat(),
            "broker_id": "binance", "broker_order_id": str(index), "symbol": SPEC.symbol,
            "side": side, "quantity": 3 if index == 2 else 2, "reference_price": 110, "state": "submitted",
            "metadata": {
                "deployment_id": SCOPE["deploymentId"], "strategy_instance_id": SPEC.strategy_instance_id,
                "source_artifact_hash": ARTIFACT, "portfolio_hash": SPEC.portfolio_hash,
                "native_account_fingerprint": SCOPE["accountFingerprint"],
                "order_purpose": "SIGNAL", "market_type": "spot",
                "native_open_boundary": {
                    "provider": "binance", "sourceProvider": "binance", "timeframe": "5m",
                    "symbol": SPEC.symbol, "sourceArtifactHash": ARTIFACT, "attestation": "BINANCE_WEBSOCKET",
                    "open": 100, "observedTime": observed.isoformat(), "promotionEligible": attested,
                    "recoveryOnly": False,
                },
            },
        }
        self.ledger.checkpoint_order_dispatch(dispatch)
        identity = terminal_identity(SCOPE, SPEC, dispatch)
        status = "CANCELED" if index == 2 else "FILLED"
        fills = [{
            "eventId": f"trade-{index}", "brokerOrderId": str(index), "price": price, "quantity": "2",
            "feeCurrency": "USDT", "fee": "0.2", "accountFingerprint": SCOPE["accountFingerprint"],
            "strategyInstanceId": SPEC.strategy_instance_id, "symbol": SPEC.symbol, "side": side,
            "occurredAt": (observed + timedelta(seconds=1)).isoformat(),
        }]
        proof = terminal_proof(identity, fills, broker_response={"status": status, "executedQty": "2"},
                               all_trades=fills, executed_quantity="2")
        self.cache.put(identity, fills, proof)
        return dispatch

    def report(self):
        return read_execution_cost_report(
            self.cache.path, self.root / "ledger.sqlite3", deployment_id=SCOPE["deploymentId"],
            artifact_hash=ARTIFACT, provider="binance", symbol=SPEC.symbol, interval="5m",
        )

    def test_reconciled_orders_seed_read_only_costs_and_reject_future_fills(self):
        for index in range(1, 21):
            self.record(index)
        self.record(21, attested=False)
        report = self.report()
        self.assertEqual(report["orders"], 20)
        self.assertEqual(report["ordersBySide"], {"BUY": 10, "SELL": 10})
        self.assertEqual(report["partialOrdersWithFills"], 1)
        self.assertEqual(report["medianLastFillLatencyMs"], 1000)
        self.assertEqual(report["observedFeeBps"], 10)
        self.assertEqual(report["observedSignedSlippageBps"], 100)
        self.assertFalse(report["executionAuthority"])
        scopes = {"artifact_hash": ARTIFACT, "provider": "binance", "symbol": SPEC.symbol, "interval": "5m"}
        costs = research_cost_settings(report, **scopes, replay_start=NOW + timedelta(days=1))
        self.assertEqual(costs["feeRate"], 0.001)
        self.assertEqual(costs["slippagePercent"], 1)
        with self.assertRaisesRegex(ValueError, "precede"):
            research_cost_settings(report, **scopes, replay_start=NOW)

    def test_missing_samples_are_not_rates_and_mutated_dispatch_cannot_export(self):
        dispatch = self.record(1)
        self.assertEqual(self.report()["status"], "INSUFFICIENT")
        self.ledger.update_order_dispatch({**dispatch, "reference_price": 120})
        with self.assertRaisesRegex(ValueError, "exact local submission"):
            self.report()
