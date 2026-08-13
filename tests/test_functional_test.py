from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

from live_trader.functional_test import (
    FUNCTIONAL_TEST_EVIDENCE_CLASS,
    FunctionalTestRiskSnapshot,
    build_functional_test_end_plan,
    evaluate_functional_test_order,
)
from live_trader.order_management import OrderIntent
from trading_runtime.functional_test import (
    FunctionalTestBinding,
    FunctionalTestCaps,
    issue_functional_test_permit,
    issue_live_activation_token,
)


NOW = datetime(2026, 8, 5, 0, 0, tzinfo=timezone.utc)
STRATEGY_HASH = "a" * 64
PORTFOLIO_HASH = "b" * 64


def binding(*, account_id: str = "kis-account-1") -> FunctionalTestBinding:
    return FunctionalTestBinding(
        strategy_artifact_id="strategy-ma-1",
        strategy_artifact_hash=STRATEGY_HASH,
        strategy_instance_id="strategy-instance-1",
        portfolio_required=True,
        portfolio_artifact_id="portfolio-1",
        portfolio_artifact_hash=PORTFOLIO_HASH,
        portfolio_instance_id="portfolio-instance-1",
        account_id=account_id,
        symbols=("005930",),
    )


def caps(**changes: float | int) -> FunctionalTestCaps:
    values: dict[str, float | int] = {
        "max_order_quantity": 1,
        "max_order_notional": 100_000,
        "max_gross_exposure": 100_000,
        "max_orders": 3,
        "max_open_positions": 1,
        "max_loss": 10_000,
    }
    values.update(changes)
    return FunctionalTestCaps(**values)


def permit(
    *,
    environment: str = "KIS_DEMO",
    permit_caps: FunctionalTestCaps | None = None,
):
    return issue_functional_test_permit(
        binding=binding(),
        environment=environment,
        duration_value=6,
        duration_unit="HOURS",
        caps=permit_caps or caps(),
        now=NOW,
    )


def intent(
    *,
    environment: str = "KIS_DEMO",
    mode: str | None = None,
    quantity: float = 1,
    reference_price: float = 40_000,
    metadata_changes: dict | None = None,
) -> OrderIntent:
    metadata = {
        "functional_test_strategy_artifact_id": "strategy-ma-1",
        "functional_test_strategy_artifact_hash": STRATEGY_HASH,
        "functional_test_strategy_instance_id": "strategy-instance-1",
        "strategy_instance_id": "strategy-instance-1",
        "portfolio_artifact_id": "portfolio-1",
        "portfolio_artifact_hash": PORTFOLIO_HASH,
        "portfolio_instance_id": "portfolio-instance-1",
        "account_id": "kis-account-1",
        "environment": environment,
    }
    metadata.update(metadata_changes or {})
    return OrderIntent(
        strategy_id="strategy-ma-1",
        asset="한국주식",
        symbol="005930",
        side="BUY",
        quantity=quantity,
        reference_price=reference_price,
        mode=mode or ("SMALL_LIVE" if environment == "KIS_LIVE" else "PAPER"),
        reason="functional-test",
        metadata=metadata,
    )


def risk(**changes) -> FunctionalTestRiskSnapshot:
    values = {
        "gross_exposure": 20_000,
        "submitted_order_count": 0,
        "open_position_count": 0,
        "loss": 0,
        "opens_new_position": True,
        "working_order_count": 0,
        "reconciled": True,
        "observed_at": "2026-08-05T00:00:30Z",
    }
    values.update(changes)
    return FunctionalTestRiskSnapshot(**values)


class FunctionalTestSafetyAdapterTest(unittest.TestCase):
    def test_us_live_requires_exact_route_hash_and_symbol_exchange_at_action_edge(self) -> None:
        us_binding = FunctionalTestBinding(
            strategy_artifact_id="strategy-f",
            strategy_artifact_hash="d" * 64,
            strategy_instance_id="standalone:strategy-f",
            portfolio_required=False,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_id="kis-account-1",
            symbols=("F",),
            market_group="US_STOCK",
            execution_route="KIS_US_LIVE_CONTINUOUS",
            settlement_currency="USD",
            exchanges=("NYSE",),
            symbol_routes=(("F", "NYSE"),),
        )
        us_caps = FunctionalTestCaps(
            max_order_quantity=1,
            max_order_notional=50,
            max_gross_exposure=50,
            max_orders=2,
            max_open_positions=1,
            max_loss=2.5,
        )
        us_permit = issue_functional_test_permit(
            binding=us_binding,
            environment="KIS_LIVE",
            duration_value=2,
            duration_unit="HOURS",
            caps=us_caps,
            now=NOW,
        )
        activation = issue_live_activation_token(
            permit=us_permit,
            market_day_close=NOW + timedelta(hours=4),
            authorized_by="operator-us",
            now=NOW,
        )
        route_hash = us_permit.binding.snapshot()["routeScopeHash"]
        metadata = {
            "functional_test_strategy_artifact_id": "strategy-f",
            "functional_test_strategy_artifact_hash": "d" * 64,
            "functional_test_strategy_instance_id": "standalone:strategy-f",
            "account_id": "kis-account-1",
            "environment": "KIS_LIVE",
            "marketGroup": "US_STOCK",
            "executionRoute": "KIS_US_LIVE_CONTINUOUS",
            "settlementCurrency": "USD",
            "routeScopeHash": route_hash,
            "exchange": "NYSE",
        }
        us_intent = OrderIntent(
            strategy_id="strategy-f",
            asset="미국주식",
            symbol="F",
            side="BUY",
            quantity=1,
            reference_price=12.5,
            mode="SMALL_LIVE",
            reason="functional-test-us",
            metadata=metadata,
        )
        us_risk = FunctionalTestRiskSnapshot(
            gross_exposure=0,
            submitted_order_count=0,
            open_position_count=0,
            loss=0,
            opens_new_position=True,
            working_order_count=0,
            reconciled=True,
            observed_at="2026-08-05T00:00:30Z",
        )

        allowed = evaluate_functional_test_order(
            us_permit,
            current_binding=us_binding,
            environment="KIS_LIVE",
            intent=us_intent,
            global_caps=us_caps,
            risk=us_risk,
            operator_confirmed=True,
            live_activation_payload=activation,
            now=NOW + timedelta(minutes=1),
        )
        self.assertTrue(allowed.allowed, allowed.to_dict())

        wrong_exchange = replace(
            us_intent,
            metadata={**metadata, "exchange": "NASD"},
        )
        blocked = evaluate_functional_test_order(
            us_permit,
            current_binding=us_binding,
            environment="KIS_LIVE",
            intent=wrong_exchange,
            global_caps=us_caps,
            risk=us_risk,
            operator_confirmed=True,
            live_activation_payload=activation,
            now=NOW + timedelta(minutes=1),
        )
        self.assertFalse(blocked.allowed)
        self.assertIn(
            "functional-test-intent-exchange-mismatch",
            blocked.blocker_codes,
        )
        self.assertIn(
            "functional-test-exchange-binding-mismatch",
            blocked.blocker_codes,
        )

    def test_demo_order_uses_stricter_of_global_and_permit_caps(self) -> None:
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(),
            global_caps=caps(
                max_order_notional=50_000,
                max_gross_exposure=80_000,
                max_orders=2,
                max_loss=5_000,
            ),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )

        self.assertTrue(report.allowed, report.to_dict())
        self.assertEqual(50_000, report.effective_caps["max_order_notional"])
        self.assertEqual(80_000, report.effective_caps["max_gross_exposure"])
        self.assertEqual(2, report.effective_caps["max_orders"])
        self.assertEqual(5_000, report.effective_caps["max_loss"])
        payload = report.to_dict()
        self.assertFalse(payload["promotionEligible"])
        self.assertFalse(payload["fullLiveAllowed"])
        self.assertEqual(FUNCTIONAL_TEST_EVIDENCE_CLASS, payload["evidenceClass"])
        self.assertEqual(64, len(payload["scopeHash"]))
        self.assertEqual(64, len(payload["intentHash"]))

    def test_exact_current_and_intent_binding_are_required(self) -> None:
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(account_id="different-account"),
            environment="KIS_DEMO",
            intent=intent(
                metadata_changes={
                    "functional_test_strategy_artifact_hash": "c" * 64,
                    "account_id": "different-account",
                }
            ),
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )

        self.assertFalse(report.allowed)
        self.assertIn(
            "functional-test-binding-account-id-mismatch",
            report.blocker_codes,
        )
        self.assertIn(
            "functional-test-intent-strategy-artifact-hash-mismatch",
            report.blocker_codes,
        )

    def test_missing_dispatch_binding_is_fail_closed(self) -> None:
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(
                metadata_changes={
                    "functional_test_strategy_instance_id": "",
                    "portfolio_artifact_hash": "",
                }
            ),
            global_caps=caps(),
            risk=risk(),
            phase="DISPATCH",
            now=NOW + timedelta(minutes=1),
        )

        self.assertFalse(report.allowed)
        self.assertEqual("DISPATCH", report.phase)
        self.assertIn(
            "functional-test-intent-strategy-instance-id-missing",
            report.blocker_codes,
        )
        self.assertIn(
            "functional-test-intent-portfolio-artifact-hash-missing",
            report.blocker_codes,
        )

    def test_top_level_strategy_and_order_side_are_part_of_intent_scope(self) -> None:
        malformed_intent = replace(
            intent(),
            strategy_id="another-strategy",
            side="HOLD",  # type: ignore[arg-type]
        )
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=malformed_intent,
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )

        self.assertFalse(report.allowed)
        self.assertIn(
            "functional-test-intent-strategy-id-mismatch",
            report.blocker_codes,
        )
        self.assertIn(
            "functional-test-intent-side-invalid",
            report.blocker_codes,
        )

    def test_each_effective_risk_cap_blocks_the_projected_order(self) -> None:
        cases = (
            (
                "quantity",
                intent(quantity=2, reference_price=10_000),
                risk(),
                "functional-test-order-quantity-cap-exceeded",
            ),
            (
                "notional",
                intent(reference_price=100_001),
                risk(gross_exposure=0),
                "functional-test-order-notional-cap-exceeded",
            ),
            (
                "gross exposure",
                intent(reference_price=10_000),
                risk(gross_exposure=95_000),
                "functional-test-gross-exposure-cap-exceeded",
            ),
            (
                "order count",
                intent(reference_price=10_000),
                risk(submitted_order_count=3),
                "functional-test-order-count-cap-exceeded",
            ),
            (
                "open positions",
                intent(reference_price=10_000),
                risk(open_position_count=1),
                "functional-test-open-position-cap-exceeded",
            ),
            (
                "loss",
                intent(reference_price=10_000),
                risk(loss=10_001),
                "functional-test-loss-cap-exceeded",
            ),
        )
        for name, order_intent, risk_snapshot, expected in cases:
            with self.subTest(name=name):
                report = evaluate_functional_test_order(
                    permit(),
                    current_binding=binding(),
                    environment="KIS_DEMO",
                    intent=order_intent,
                    global_caps=caps(),
                    risk=risk_snapshot,
                    now=NOW + timedelta(minutes=1),
                )
                self.assertFalse(report.allowed)
                self.assertIn(expected, report.blocker_codes)

    def test_invalid_or_unreconciled_risk_truth_is_fail_closed(self) -> None:
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(reference_price=10_000),
            global_caps=caps(),
            risk=risk(gross_exposure=float("nan"), reconciled=False),
            now=NOW + timedelta(minutes=1),
        )

        self.assertFalse(report.allowed)
        self.assertIn(
            "functional-test-risk-gross-exposure-invalid",
            report.blocker_codes,
        )
        self.assertIn("functional-test-risk-not-reconciled", report.blocker_codes)

    def test_kis_live_requires_operator_and_current_daily_activation(self) -> None:
        live_permit = permit(environment="KIS_LIVE")
        without_authorization = evaluate_functional_test_order(
            live_permit,
            current_binding=binding(),
            environment="KIS_LIVE",
            intent=intent(environment="KIS_LIVE"),
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )
        self.assertIn(
            "functional-test-operator-confirmation-required",
            without_authorization.blocker_codes,
        )
        self.assertIn(
            "functional-test-live-activation-required",
            without_authorization.blocker_codes,
        )

        activation = issue_live_activation_token(
            permit=live_permit,
            market_day_close=NOW + timedelta(hours=8),
            authorized_by="operator-youwo",
            now=NOW,
        )
        allowed = evaluate_functional_test_order(
            live_permit,
            current_binding=binding(),
            environment="KIS_LIVE",
            intent=intent(environment="KIS_LIVE"),
            global_caps=caps(),
            risk=risk(),
            operator_confirmed=True,
            live_activation_payload=activation,
            now=NOW + timedelta(minutes=1),
        )
        self.assertTrue(allowed.allowed, allowed.to_dict())

    def test_full_live_and_promotion_evidence_are_never_allowed(self) -> None:
        live_permit = permit(environment="KIS_LIVE")
        activation = issue_live_activation_token(
            permit=live_permit,
            market_day_close=NOW + timedelta(hours=8),
            authorized_by="operator-youwo",
            now=NOW,
        )
        report = evaluate_functional_test_order(
            live_permit,
            current_binding=binding(),
            environment="KIS_LIVE",
            intent=intent(
                environment="KIS_LIVE",
                mode="FULL_LIVE",
                metadata_changes={"use_as_promotion_evidence": True},
            ),
            global_caps=caps(),
            risk=risk(),
            operator_confirmed=True,
            live_activation_payload=activation,
            now=NOW + timedelta(minutes=1),
        )

        self.assertFalse(report.allowed)
        self.assertIn(
            "functional-test-full-live-forbidden",
            report.blocker_codes,
        )
        self.assertIn(
            "functional-test-promotion-evidence-forbidden",
            report.blocker_codes,
        )
        self.assertFalse(report.promotion_eligible)
        self.assertFalse(report.full_live_allowed)

    def test_dispatch_must_recheck_fresh_risk_not_reuse_pretrade_result(self) -> None:
        pretrade = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(reference_price=10_000),
            global_caps=caps(),
            risk=risk(gross_exposure=0),
            phase="PRETRADE",
            now=NOW + timedelta(minutes=1),
        )
        dispatch = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(reference_price=10_000),
            global_caps=caps(),
            risk=risk(gross_exposure=100_000),
            phase="DISPATCH",
            now=NOW + timedelta(minutes=2),
        )

        self.assertTrue(pretrade.allowed, pretrade.to_dict())
        self.assertFalse(dispatch.allowed)
        self.assertIn(
            "functional-test-gross-exposure-cap-exceeded",
            dispatch.blocker_codes,
        )

    def test_reconciled_exit_can_supply_exact_projected_exposure(self) -> None:
        exit_intent = replace(
            intent(reference_price=40_000),
            side="SELL",
        )
        report = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=exit_intent,
            global_caps=caps(),
            risk=risk(
                gross_exposure=80_000,
                open_position_count=1,
                opens_new_position=False,
                gross_exposure_after=40_000,
                open_position_count_after=0,
            ),
            phase="DISPATCH",
            now=NOW + timedelta(minutes=1),
        )

        self.assertTrue(report.allowed, report.to_dict())
        self.assertEqual(40_000, report.observed["grossExposureAfter"])
        self.assertEqual(0, report.observed["openPositionCountAfter"])

    def test_portfolio_only_binding_does_not_accept_an_unbound_strategy(self) -> None:
        portfolio_binding = FunctionalTestBinding(
            strategy_artifact_id="",
            strategy_artifact_hash="",
            strategy_instance_id="",
            portfolio_required=True,
            portfolio_artifact_id="portfolio-1",
            portfolio_artifact_hash=PORTFOLIO_HASH,
            portfolio_instance_id="portfolio-instance-1",
            account_id="kis-account-1",
            symbols=("005930",),
        )
        portfolio_permit = issue_functional_test_permit(
            binding=portfolio_binding,
            environment="KIS_DEMO",
            caps=caps(),
            now=NOW,
        )
        unbound = evaluate_functional_test_order(
            portfolio_permit,
            current_binding=portfolio_binding,
            environment="KIS_DEMO",
            intent=intent(),
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )
        composite_metadata = dict(intent().metadata)
        for field_name in (
            "functional_test_strategy_artifact_id",
            "functional_test_strategy_artifact_hash",
            "functional_test_strategy_instance_id",
        ):
            composite_metadata.pop(field_name)
        composite_intent = replace(
            intent(),
            strategy_id="portfolio-1",
            metadata=composite_metadata,
        )
        allowed = evaluate_functional_test_order(
            portfolio_permit,
            current_binding=portfolio_binding,
            environment="KIS_DEMO",
            intent=composite_intent,
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )

        self.assertIn(
            "functional-test-intent-unbound-strategy-scope",
            unbound.blocker_codes,
        )
        self.assertTrue(allowed.allowed, allowed.to_dict())

    def test_expired_or_tampered_permit_is_reported_as_shared_blocker(self) -> None:
        expired = evaluate_functional_test_order(
            permit(),
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(),
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(hours=6),
        )
        tampered_payload = permit().to_dict()
        tampered_payload["caps"]["maxOrders"] = 999
        tampered = evaluate_functional_test_order(
            tampered_payload,
            current_binding=binding(),
            environment="KIS_DEMO",
            intent=intent(),
            global_caps=caps(),
            risk=risk(),
            now=NOW + timedelta(minutes=1),
        )

        self.assertIn("functional-test-permit-expired", expired.blocker_codes)
        self.assertIn(
            "functional-test-content-hash-mismatch",
            tampered.blocker_codes,
        )

    def test_end_plan_blocks_entries_cancels_reconciles_and_flattens(self) -> None:
        plan = build_functional_test_end_plan(
            permit(),
            environment="KIS_DEMO",
            working_order_count=2,
            open_position_count=1,
            flatten_policy="FLATTEN",
        )

        self.assertTrue(plan.new_entries_blocked)
        self.assertTrue(plan.cancel_working_orders)
        self.assertTrue(plan.reconciliation_required)
        self.assertTrue(plan.flatten_required)
        self.assertFalse(plan.promotion_eligible)
        self.assertEqual(
            [
                "BLOCK_NEW_ENTRIES",
                "CANCEL_WORKING_ORDERS",
                "FLATTEN_POSITIONS",
                "FINAL_BROKER_RECONCILIATION",
            ],
            [item.action for item in plan.actions],
        )
        self.assertEqual(
            FUNCTIONAL_TEST_EVIDENCE_CLASS,
            plan.to_dict()["evidenceClass"],
        )

    def test_keep_policy_requires_explicit_handoff(self) -> None:
        plan = build_functional_test_end_plan(
            permit(),
            environment="KIS_LIVE",
            working_order_count=0,
            open_position_count=1,
            flatten_policy="KEEP_WITH_EXPLICIT_HANDOFF",
        )

        self.assertFalse(plan.flatten_required)
        self.assertTrue(plan.explicit_handoff_required)
        self.assertIn(
            "RECORD_EXPLICIT_POSITION_HANDOFF",
            [item.action for item in plan.actions],
        )


if __name__ == "__main__":
    unittest.main()
