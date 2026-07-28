from __future__ import annotations

import json
import unittest
from pathlib import Path

from live_trader.contracts import normalize_strategy_artifact


FIXTURES = (
    Path(__file__).resolve().parents[3]
    / "packages"
    / "trading_runtime"
    / "tests"
    / "fixtures"
    / "golden_strategy_contracts.json"
)


class LiveGoldenStrategyContractTests(unittest.TestCase):
    def test_live_uses_the_shared_golden_order_and_route_semantics(self) -> None:
        for case in json.loads(FIXTURES.read_text(encoding="utf-8")):
            with self.subTest(case=case["caseId"]):
                normalized = normalize_strategy_artifact(case["artifact"])
                expected = case["expected"]
                exposure = normalized["exposure_contract"]

                self.assertEqual(expected["positionDirection"], normalized["position_direction"])
                self.assertEqual(expected["entrySide"], exposure["entrySide"])
                self.assertEqual(expected["exitSide"], exposure["exitSide"])
                self.assertEqual(expected["economicExposure"], normalized["economic_exposure"])
                self.assertEqual(expected["productType"], exposure["productType"])
                self.assertEqual(expected["allowShort"], normalized["allow_short"])
                self.assertEqual(expected["providerStatus"], normalized["provider_reconciliation"]["status"])
                provider_reasons = [
                    reason
                    for reason in normalized["capabilities"]["blockingFailReasons"]
                    if str(reason).startswith("provider-reconciliation-invalid:")
                ]
                self.assertEqual([], provider_reasons)


if __name__ == "__main__":
    unittest.main()
