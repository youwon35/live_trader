from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_trader import state


def portfolio(artifact_hash, *, execution_passed=True, awarded=15):
    return {
        "id": "shared-portfolio-id",
        "artifact_reference": {
            "artifactId": "shared-portfolio-id",
            "artifactHash": artifact_hash,
            "contentHash": "c" * 64,
        },
        "artifact_integrity": {
            "valid": True,
            "declaredHash": artifact_hash,
            "computedHash": artifact_hash,
        },
        "operational_readiness_bundle": {
            "contentHash": "sealed-bundle",
            "operationalReadiness": {"components": [
                {"id": "dataIntegrity", "passed": True},
                {"id": "executionQuality", "passed": execution_passed, "awarded": awarded},
            ]},
        },
    }


def qualification(artifact_hash):
    return {
        "ready": True,
        "portfolioArtifactId": "shared-portfolio-id",
        "portfolioArtifactHash": artifact_hash,
    }


class PortfolioOperationalReadinessTests(unittest.TestCase):
    def setUp(self):
        self.first = portfolio("a" * 64)
        self.selected = portfolio("b" * 64)
        self.strategy = {
            "strategy_id": "selected-strategy",
            "paper_live_qualification": qualification("b" * 64),
        }
        self.checks = {
            "strategies": [self.strategy],
            "portfolios": [self.first, self.selected],
            "operational_readiness": {"liveEligible": True},
        }
        self.intent = SimpleNamespace(
            strategy_id="selected-strategy",
            symbol="BTCUSDT",
            mode="FULL_LIVE",
            notional=10.0,
            metadata={
                "portfolio_id": "shared-portfolio-id",
                "portfolio_artifact_hash": "b" * 64,
                "current_weight": 0.0,
                "portfolio_equity": 100.0,
                "expected_alpha_bps": 10.0,
                "expected_cost_bps": 2.0,
            },
        )
        runtime = {
            "recovery_status": {"verified": True, "safeMode": False},
            "policy_replays": [],
            "preflight_last_run": None,
            "shadow_evidence": [{"decision": {"decision_price": 100.0, "virtual_fill_price": 100.0}}],
        }
        context = patch.dict(state.STATE, runtime)
        context.start()
        self.addCleanup(context.stop)

    @staticmethod
    def match(item, *_args, **_kwargs):
        return {
            "active": True,
            "allowed": True,
            "targetWeight": 0.1,
            "rebalancePolicy": {"minimumNotional": 1.0},
            "operationalReadinessBundle": item.get("operational_readiness_bundle", {}),
            "portfolioArtifactReference": state.verified_portfolio_artifact_reference(item),
        }

    def evaluate(self):
        with patch.object(state, "portfolio_match_for_strategy", side_effect=self.match) as match:
            result = state.portfolio_gate_for_intent(self.checks, self.intent)
        self.assertIs(match.call_args.args[0], self.selected)
        return result

    def test_other_bundle_shadow_and_partial_points_do_not_pass_execution(self):
        self.selected = portfolio("b" * 64, execution_passed=False, awarded=8)
        self.checks["portfolios"][1] = self.selected
        report = self.evaluate()
        self.assertFalse(report["allowed"])
        self.assertEqual(report["advancedOperationBlockers"], ["operational-readiness=70/85"])

    def test_paper_evidence_from_other_version_cannot_fill_selected_readiness(self):
        self.strategy["paper_live_qualification"] = qualification("a" * 64)
        self.assertFalse(self.evaluate()["allowed"])
        self.strategy["paper_live_qualification"] = qualification("b" * 64)
        self.assertTrue(self.evaluate()["allowed"])
        overview = state.runtime_operational_readiness(self.checks["strategies"], self.checks["portfolios"])
        self.assertEqual(overview["status"], "SELECTION_REQUIRED")

    def test_reduction_and_no_bundle_do_not_require_readiness(self):
        self.selected = portfolio("b" * 64, execution_passed=False, awarded=0)
        self.checks["portfolios"][1] = self.selected
        self.intent.metadata["current_weight"] = 0.2
        self.assertTrue(self.evaluate()["allowed"])
        self.intent.metadata["current_weight"] = 0.0
        del self.selected["operational_readiness_bundle"]
        self.assertTrue(self.evaluate()["allowed"])


if __name__ == "__main__":
    unittest.main()
