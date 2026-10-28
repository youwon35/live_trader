"""Offline contract tests: no account, API client, or order transport."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import tempfile
import unittest

from live_trader.short_live_contract import (
    SCHEMA, ShortLiveContractError, build_contract, canonical_hash, leg_manifest,
    normalize_futures_policy, verify_contract,
)


POLICY = {"leverage": 5, "marginMode": "ISOLATED", "stopLossPercent": 2}


class FuturesContractTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent)
        self.addCleanup(self.directory.cleanup)
        source = Path(self.directory.name) / "strategy.json"
        source.write_text('{"fixture":true}', encoding="utf-8")
        self.resolved = {
            "targets": [{"selectionKey": "fixture-exact"}],
            "sourcePins": [{"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}],
            "specs": [{"portfolioId": "portfolio-fixture", "portfolioHash": "a" * 64,
                       "strategyInstanceId": "instance-fixture", "strategyId": "strategy-fixture",
                       "artifactHash": "b" * 64, "pluginId": "moving_average_cross",
                       "instrumentId": "SOLUSDT", "symbol": "SOLUSDT", "timeframe": "1h",
                       "provider": "binance-futures", "brokerId": "binance-futures",
                       "route": "BINANCE_USDM_FUTURES", "targetWeight": 1,
                       "parameters": {"positionSize": 5}, "executionTiming": "next-open"}],
        }

    def contract(self, policy=POLICY):
        return build_contract(self.resolved, {"futuresPolicy": policy}, now=datetime.now(timezone.utc))

    def test_freezes_operator_policy_and_keeps_capital_and_original_position_percentage(self):
        contract = self.contract()
        self.assertEqual(SCHEMA, "short-live-functional-contract-v2")
        self.assertEqual(contract["capitalKrwPerBroker"], 100_000)
        self.assertEqual(contract["specs"][0]["positionSizeFraction"], 0.05)
        policy = contract["futuresPolicy"]
        self.assertEqual(policy["leverage"], 5)
        self.assertEqual(policy["stopLossPercent"], 2)
        self.assertEqual(policy["workingType"], "MARK_PRICE")
        self.assertEqual(policy["orderType"], "STOP_MARKET")
        self.assertIs(policy["closePosition"], True)
        self.assertEqual(policy["triggerBasis"], "ENTRY_LIMIT_PRICE")
        self.assertEqual(leg_manifest(contract, "BINANCE_USDM_FUTURES")["futuresPolicy"], policy)
        verify_contract(contract)

    def test_missing_futures_policy_fails_even_at_one_x(self):
        with self.assertRaisesRegex(ShortLiveContractError, "FUTURES_PROTECTION_POLICY_REQUIRED"):
            self.contract(None)

    def test_futures_policy_rejects_invalid_and_conflicting_values(self):
        for changes in ({"leverage": True}, {"leverage": 0}, {"leverage": 9007199254740992}, {"leverage": 5.5},
                        {"leverage": "NaN"}, {"stopLossPercent": 0}, {"stopLossPercent": 20},
                        {"stopLossPercent": float("nan")}, {"stopLossPercent": True},
                        {"marginMode": "CROSSED"}, {"closePosition": False},
                        {"workingType": "CONTRACT_PRICE"}, {"applySettings": True}):
            with self.subTest(changes=changes), self.assertRaises(ShortLiveContractError):
                normalize_futures_policy({**POLICY, **changes})

    def test_policy_changes_change_scope_hash(self):
        contract = self.contract()
        changed = deepcopy(contract)
        changed["futuresPolicy"]["leverage"] = 3
        with self.assertRaisesRegex(ShortLiveContractError, "IMMUTABLE_FUNCTIONAL_CONTRACT_MISMATCH"):
            verify_contract(changed)

    def test_rehashed_conflicting_leg_policy_is_rejected(self):
        contract = self.contract()
        contract["legs"][0]["futuresPolicy"]["leverage"] = 3
        body = {key: value for key, value in contract.items() if key not in {"scopeHash", "manifestHash"}}
        contract["scopeHash"] = contract["manifestHash"] = canonical_hash(body)
        with self.assertRaisesRegex(ShortLiveContractError, "IMMUTABLE_FUTURES_POLICY_MISMATCH"):
            verify_contract(contract)

    def test_old_schema_cannot_be_promoted_by_rehash(self):
        contract = self.contract()
        contract["schemaVersion"] = "short-live-functional-contract-v1"
        body = {key: value for key, value in contract.items() if key not in {"scopeHash", "manifestHash"}}
        contract["scopeHash"] = contract["manifestHash"] = canonical_hash(body)
        with self.assertRaisesRegex(ShortLiveContractError, "IMMUTABLE_FUNCTIONAL_CONTRACT_MISMATCH"):
            verify_contract(contract)

    def test_spot_does_not_gain_a_futures_policy_or_leveraged_cash(self):
        spec = self.resolved["specs"][0]
        spec.update(route="BINANCE_SPOT", brokerId="binance", provider="binance")
        contract = self.contract(None)
        self.assertIsNone(contract["futuresPolicy"])
        self.assertNotIn("futuresPolicy", contract["legs"][0])
        self.assertEqual(contract["capitalKrwPerBroker"], 100_000)
        verify_contract(contract)


if __name__ == "__main__":
    unittest.main()
