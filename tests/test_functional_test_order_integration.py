from __future__ import annotations

import copy
import os
from dataclasses import replace
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.emergency_stop import _reset_emergency_stop_sticky_for_tests
from live_trader.process_safety import release_held_leases_for_tests
from live_trader.continuous_live import LiveContinuousController
from live_trader.functional_test import FunctionalTestRiskSnapshot
from live_trader.order_management import OrderIntent
from live_trader.program_ledger import ProgramLedger
from live_trader.risk_engine import PreTradeRiskReport, RiskCheck
from live_trader.operational_governance import OperationalGovernanceStore
from trading_runtime.functional_test import (
    FunctionalTestBinding,
    FunctionalTestCaps,
    issue_functional_test_permit,
    issue_live_activation_token,
    write_functional_test_document,
)


class FunctionalTestOrderIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        _reset_emergency_stop_sticky_for_tests()
        release_held_leases_for_tests()
        self.original_state = copy.deepcopy(state.STATE)
        self.original_ledger = state.PROGRAM_LEDGER
        self.original_recovery = state.RECOVERY_JOURNAL
        self.original_trace_store = state.DECISION_TRACE_STORE
        self.original_oms = state.LIVE_OMS
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.root = root
        self.previous_emergency_stop_path = os.environ.get(
            "LIVE_TRADER_EMERGENCY_STOP_PATH"
        )
        self.previous_process_lock_dir = os.environ.get(
            "LIVE_TRADER_PROCESS_LOCK_DIR"
        )
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            root / "emergency-stop.json"
        )
        os.environ["LIVE_TRADER_PROCESS_LOCK_DIR"] = str(root / "locks")
        self.ledger = ProgramLedger(root / "program-ledger.sqlite3")
        state.PROGRAM_LEDGER = self.ledger
        state.RECOVERY_JOURNAL = state.RecoveryJournal(
            root / "recovery-journal"
        )
        state.DECISION_TRACE_STORE = state.DecisionTraceStore(
            root / "decision-trace.jsonl"
        )
        state.LIVE_OMS = state.OrderManagementSystem()
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["persisted_idempotency_keys"] = []
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["dry_run"] = False
        state.STATE["operator_confirmed"] = True
        state.STATE["new_entries_blocked"] = False
        state.STATE["kill_switch"] = False

    def tearDown(self) -> None:
        state.PROGRAM_LEDGER = self.original_ledger
        state.RECOVERY_JOURNAL = self.original_recovery
        state.DECISION_TRACE_STORE = self.original_trace_store
        state.LIVE_OMS = self.original_oms
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        release_held_leases_for_tests()
        _reset_emergency_stop_sticky_for_tests()
        if self.previous_emergency_stop_path is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = (
                self.previous_emergency_stop_path
            )
        if self.previous_process_lock_dir is None:
            os.environ.pop("LIVE_TRADER_PROCESS_LOCK_DIR", None)
        else:
            os.environ["LIVE_TRADER_PROCESS_LOCK_DIR"] = (
                self.previous_process_lock_dir
            )
        self.temporary.cleanup()

    @staticmethod
    def binding(*, account_id: str = "kis-live-account-hash") -> FunctionalTestBinding:
        return FunctionalTestBinding(
            strategy_artifact_id="functional-strategy-artifact",
            strategy_artifact_hash="a" * 64,
            strategy_instance_id="functional-strategy-instance",
            portfolio_required=True,
            portfolio_artifact_id="functional-portfolio-artifact",
            portfolio_artifact_hash="b" * 64,
            portfolio_instance_id="functional-portfolio-instance",
            account_id=account_id,
            symbols=("005930",),
        )

    @staticmethod
    def portfolio_binding() -> FunctionalTestBinding:
        return FunctionalTestBinding(
            strategy_artifact_id="",
            strategy_artifact_hash="",
            strategy_instance_id="",
            portfolio_required=True,
            portfolio_artifact_id="functional-portfolio-artifact",
            portfolio_artifact_hash="b" * 64,
            portfolio_instance_id="functional-portfolio-instance",
            account_id="kis-live-account-hash",
            symbols=("005930",),
        )

    @staticmethod
    def caps() -> FunctionalTestCaps:
        return FunctionalTestCaps(
            max_order_quantity=1,
            max_order_notional=100_000,
            max_gross_exposure=100_000,
            max_orders=3,
            max_open_positions=1,
            max_loss=10_000,
        )

    @staticmethod
    def safe_risk() -> FunctionalTestRiskSnapshot:
        return FunctionalTestRiskSnapshot(
            gross_exposure=0,
            submitted_order_count=0,
            open_position_count=0,
            loss=0,
            opens_new_position=True,
            working_order_count=0,
            reconciled=True,
            observed_at=datetime.now(timezone.utc).isoformat(),
            gross_exposure_after=40_000,
            open_position_count_after=1,
        )

    @staticmethod
    def unsafe_risk() -> FunctionalTestRiskSnapshot:
        return FunctionalTestRiskSnapshot(
            gross_exposure=100_000,
            submitted_order_count=0,
            open_position_count=1,
            loss=0,
            opens_new_position=False,
            working_order_count=0,
            reconciled=True,
            observed_at=datetime.now(timezone.utc).isoformat(),
            gross_exposure_after=140_000,
            open_position_count_after=1,
        )

    def test_risk_uses_durable_multiday_permit_drawdown(self) -> None:
        permit = SimpleNamespace(
            permit_id="permit-multiday-risk",
            binding=SimpleNamespace(account_id="kis-live-account-hash"),
        )
        fingerprint = state.governance_sha256(
            {"functionalTestAccount": "kis-live-account-hash"}
        )
        self.ledger.observe_functional_test_equity(
            permit_id=permit.permit_id,
            account_fingerprint=fingerprint,
            current_equity=1_000_000,
            observed_at="2026-08-04T01:00:00+00:00",
            allow_create=True,
        )
        self.ledger.observe_functional_test_equity(
            permit_id=permit.permit_id,
            account_fingerprint=fingerprint,
            current_equity=1_100_000,
            observed_at="2026-08-04T02:00:00+00:00",
        )
        state.STATE["broker_reconciliation"] = {
            "successful_position_brokers": ["kis"],
            "errors": [],
            "positions": [],
            "fetched_at": "2026-08-05T01:00:00+00:00",
        }
        intent = OrderIntent(
            strategy_id="functional-strategy-artifact",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=40_000,
            mode="SMALL_LIVE",
            reason="multiday-risk",
            metadata={"broker_id": "kis"},
        )
        with (
            patch.object(
                state,
                "functional_test_active_authority",
                return_value=(permit, SimpleNamespace()),
            ),
            patch.object(
                state,
                "account_risk_for_intent",
                return_value={
                    "known": True,
                    "fresh": True,
                    "current_equity": 1_020_000,
                    "daily_pnl": 20_000,
                    "starting_equity": 1_000_000,
                    "minimum_daily_pnl_pct": 0,
                    "observed_at": "2026-08-05T01:00:00+00:00",
                },
            ),
            patch.object(state, "seconds_since", return_value=0.0),
            patch.object(
                state,
                "kis_order_truth_snapshot",
                return_value={
                    "complete": True,
                    "fresh": True,
                    "absenceIsAuthoritative": True,
                    "orderCount": 0,
                    "workingOrders": [],
                },
            ),
        ):
            risk = state.functional_test_risk_snapshot(
                intent,
                permit_id=permit.permit_id,
            )

        self.assertTrue(risk.reconciled)
        self.assertEqual(80_000, risk.loss)

    def test_missing_durable_permit_equity_scope_blocks_risk(self) -> None:
        permit = SimpleNamespace(
            permit_id="permit-missing-risk",
            binding=SimpleNamespace(account_id="kis-live-account-hash"),
        )
        state.STATE["broker_reconciliation"] = {
            "successful_position_brokers": ["kis"],
            "errors": [],
            "positions": [],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        intent = OrderIntent(
            strategy_id="functional-strategy-artifact",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=40_000,
            mode="SMALL_LIVE",
            reason="missing-risk-scope",
            metadata={"broker_id": "kis"},
        )
        with (
            patch.object(
                state,
                "functional_test_active_authority",
                return_value=(permit, SimpleNamespace()),
            ),
            patch.object(
                state,
                "account_risk_for_intent",
                return_value={
                    "known": True,
                    "fresh": True,
                    "current_equity": 1_000_000,
                    "daily_pnl": 0,
                    "starting_equity": 1_000_000,
                    "minimum_daily_pnl_pct": 0,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            ),
            patch.object(state, "seconds_since", return_value=0.0),
        ):
            risk = state.functional_test_risk_snapshot(
                intent,
                permit_id=permit.permit_id,
            )

        self.assertFalse(risk.reconciled)

    def test_effective_cap_preview_keeps_stricter_global_three_order_one_position_limits(self) -> None:
        with patch.object(
            state,
            "account_risk_for_intent",
            return_value={
                "known": True,
                "fresh": True,
                "current_equity": 10_000_000,
                "observed_at": datetime.now(timezone.utc).isoformat(),
            },
        ):
            preview = state.functional_test_effective_caps_snapshot()

        self.assertTrue(preview["available"])
        self.assertEqual(20, preview["permitCaps"]["maxOrders"])
        self.assertEqual(3, preview["permitCaps"]["maxOpenPositions"])
        self.assertEqual(3, preview["values"]["maxOrders"])
        self.assertEqual(1, preview["values"]["maxOpenPositions"])

    def write_documents(self, *, expired_activation: bool = False) -> tuple:
        now = datetime.now(timezone.utc)
        issued_at = now - (
            timedelta(hours=3) if expired_activation else timedelta(minutes=1)
        )
        permit = issue_functional_test_permit(
            binding=self.binding(),
            environment="KIS_LIVE",
            duration_value=6,
            duration_unit="HOURS",
            caps=self.caps(),
            now=issued_at,
        )
        activation_issued_at = issued_at + timedelta(minutes=1)
        market_close = (
            now - timedelta(minutes=1)
            if expired_activation
            else now + timedelta(hours=2)
        )
        activation = issue_live_activation_token(
            permit=permit,
            market_day_close=market_close,
            authorized_by="functional-test-operator",
            now=activation_issued_at,
        )
        live_root = self.root / "live"
        write_functional_test_document(
            live_root / "current-permit.json",
            permit,
        )
        write_functional_test_document(
            live_root / "current-activation.json",
            activation,
        )
        return permit, activation

    def write_portfolio_documents(self) -> None:
        now = datetime.now(timezone.utc) - timedelta(minutes=1)
        permit = issue_functional_test_permit(
            binding=self.portfolio_binding(),
            environment="KIS_LIVE",
            duration_value=6,
            duration_unit="HOURS",
            caps=self.caps(),
            now=now,
        )
        activation = issue_live_activation_token(
            permit=permit,
            market_day_close=now + timedelta(hours=2),
            authorized_by="functional-test-operator",
            now=now + timedelta(seconds=1),
        )
        live_root = self.root / "live"
        write_functional_test_document(
            live_root / "current-permit.json",
            permit,
        )
        write_functional_test_document(
            live_root / "current-activation.json",
            activation,
        )

    def intent(
        self,
        *,
        target_revision: int,
        expired_activation: bool = False,
        order_type: str = "00",
        authority: tuple | None = None,
    ) -> OrderIntent:
        permit, activation = authority or self.write_documents(
            expired_activation=expired_activation
        )
        return OrderIntent(
            strategy_id="functional-strategy-artifact",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=40_000,
            mode="SMALL_LIVE",
            reason="functional-test-integration",
            metadata={
                "execution_purpose": state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
                "environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
                "functional_test_environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
                "functional_test_permit_id": permit.permit_id,
                "functional_test_permit_hash": permit.content_hash,
                "functional_test_activation_token_id": activation.token_id,
                "functional_test_activation_hash": activation.content_hash,
                "functional_test_account_fingerprint": (
                    state.functional_test_account_fingerprint(
                        permit.binding.account_id
                    )
                ),
                "functional_test_strategy_artifact_id": "functional-strategy-artifact",
                "functional_test_strategy_artifact_hash": "a" * 64,
                "functional_test_strategy_instance_id": "functional-strategy-instance",
                "strategy_instance_id": "functional-strategy-instance",
                "portfolio_id": "functional-portfolio-artifact",
                "portfolio_artifact_id": "functional-portfolio-artifact",
                "portfolio_artifact_hash": "b" * 64,
                "portfolio_instance_id": "functional-portfolio-instance",
                "strategy_instance_id": "operational-sleeve-instance",
                "account_id": "kis-live-account-hash",
                "instrument_id": "KRX:005930",
                "broker_id": "kis",
                "order_type": order_type,
                "target_revision": target_revision,
                "confirmed_bar_end": datetime.now(timezone.utc).isoformat(),
            },
        )

    @staticmethod
    def passing_report() -> PreTradeRiskReport:
        return PreTradeRiskReport(
            datetime.now(timezone.utc),
            (RiskCheck("base", "pass", "ok"),),
        )

    @staticmethod
    def canary_scope(intent: OrderIntent) -> dict:
        return {
            "schemaVersion": state.CANARY_SCOPE_SCHEMA_VERSION,
            "strategyId": intent.strategy_id,
            "strategyArtifactId": "functional-strategy-artifact",
            "strategyArtifactHash": "a" * 64,
            "strategyContentHash": "c" * 64,
            "deploymentId": "functional-live-deployment",
            "deploymentRevision": 1,
            "currentDeploymentRevision": 1,
            "beforeLiveSmallAt": "2026-08-01T00:00:00+00:00",
            "paperEvidenceId": "functional-paper-evidence",
            "paperEvidenceHash": "d" * 64,
            "paperEvidenceBundleHash": "e" * 64,
            "paperFinalBindingHash": "f" * 64,
            "paperGovernanceDeploymentId": "functional-paper-deployment",
            "paperStrategyInstanceId": "functional-strategy-instance",
            "paperPortfolioRequired": True,
            "paperPortfolioArtifactId": "functional-portfolio-artifact",
            "paperPortfolioArtifactHash": "b" * 64,
            "paperPortfolioInstanceId": "functional-portfolio-instance",
            "eligible": True,
            "issues": [],
            "scopeId": "1" * 64,
        }

    def submit(
        self,
        intent: OrderIntent,
        *,
        risks: list[FunctionalTestRiskSnapshot],
        bindings: list[FunctionalTestBinding] | None = None,
        router: Mock | None = None,
    ) -> dict:
        broker = router or Mock()
        broker.place_order.return_value = {
            "ok": True,
            "statusCode": 200,
            "json": {"output": {"ODNO": "FUNCTIONAL-KIS-ACK"}},
        }
        binding_values = bindings or [self.binding()] * len(risks)
        with (
            patch.object(
                state,
                "evaluate_order_gate_with_report",
                return_value=(
                    True,
                    "approved",
                    "ready",
                    "passed",
                    self.passing_report(),
                ),
            ),
            patch.object(state, "snapshot", return_value={"summary": {}}),
            patch.object(state, "real_orders_enabled", return_value=True),
            patch.object(
                state,
                "durable_control_halt_active",
                return_value=False,
            ),
            patch.object(
                state,
                "functional_test_runtime_dispatch_allowed",
                return_value=(True, "functional-test-runtime-authorized", {}),
            ),
            patch.object(
                state,
                "current_live_canary_scope",
                return_value=self.canary_scope(intent),
            ),
            patch.object(
                state,
                "functional_test_current_binding",
                side_effect=binding_values,
            ),
            patch.object(
                state,
                "functional_test_global_caps",
                return_value=self.caps(),
            ),
            patch.object(
                state,
                "functional_test_risk_snapshot",
                side_effect=risks,
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_DOCUMENT_ROOT",
                self.root,
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_CURRENT_PERMIT_DOCUMENT",
                self.root / "live" / "current-permit.json",
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_CURRENT_ACTIVATION_DOCUMENT",
                self.root / "live" / "current-activation.json",
            ),
            patch.object(state, "LiveBrokerRouter", return_value=broker),
        ):
            return state.submit_order_intent(
                {"summary": {}},
                intent,
                dry_run=False,
                audit_event="functional test integration",
            )

    def test_live_dispatch_invariant_rejects_unsafe_functional_profiles(self) -> None:
        market_intent = self.intent(target_revision=1, order_type="01")
        allowed, reason = state.live_broker_dispatch_allowed(
            market_intent,
            dry_run=False,
        )
        self.assertFalse(allowed)
        self.assertEqual("functional-test-market-order-forbidden", reason)

        full_live = copy.deepcopy(market_intent)
        full_live = OrderIntent(
            strategy_id=full_live.strategy_id,
            asset=full_live.asset,
            symbol=full_live.symbol,
            side=full_live.side,
            quantity=full_live.quantity,
            reference_price=full_live.reference_price,
            mode="FULL_LIVE",
            reason=full_live.reason,
            metadata={**full_live.metadata, "order_type": "00"},
        )
        state.STATE["mode"] = "FULL_LIVE"
        allowed, reason = state.live_broker_dispatch_allowed(
            full_live,
            dry_run=False,
        )
        self.assertFalse(allowed)
        self.assertEqual("functional-test-small-live-mode-required", reason)

    def test_continuous_functional_orders_are_priced_kis_limits_only(self) -> None:
        self.assertEqual(
            "00",
            LiveContinuousController._order_type_for_broker(
                "kis",
                "BUY",
                "005930",
                functional_test=True,
            ),
        )
        self.assertEqual(
            "",
            LiveContinuousController._functional_test_spec_blocker(
                (SimpleNamespace(broker_id="kis", symbol="005930"),)
            ),
        )
        self.assertIn(
            "KIS broker route",
            LiveContinuousController._functional_test_spec_blocker(
                (SimpleNamespace(broker_id="binance", symbol="BTCUSDT"),)
            ),
        )

    def test_functional_gate_skips_promotion_paper_checks_but_normal_live_blocks(self) -> None:
        functional_intent = self.intent(target_revision=90)
        normal_intent = replace(
            functional_intent,
            metadata={
                key: value
                for key, value in functional_intent.metadata.items()
                if key
                not in {
                    "execution_purpose",
                    "functional_test_environment",
                    "functional_test_strategy_artifact_id",
                    "functional_test_strategy_artifact_hash",
                    "functional_test_strategy_instance_id",
                }
            },
        )
        promotion_blocked_strategy = {
            "lifecycle_status": "papered",
            "paper_portfolio_evidence_gate": {
                "required": True,
                "ready": False,
                "detail": "paper elapsed observation missing",
            },
            "paper_live_qualification_gate": {
                "required": True,
                "ready": False,
                "detail": "strict Paper Final binding missing",
            },
        }
        with (
            patch.object(state, "pre_trade_context", return_value=Mock()),
            patch.object(
                state,
                "portfolio_gate_for_intent",
                return_value={"active": False, "allowed": True},
            ),
            patch.object(
                state.PreTradeRiskGate,
                "evaluate",
                return_value=self.passing_report(),
            ),
            patch.object(
                state,
                "strategy_for_order_intent",
                return_value=promotion_blocked_strategy,
            ),
            patch.object(
                state,
                "strategy_revalidation_status",
                return_value={"expired": True, "detail": "elapsed revalidation"},
            ),
            patch.object(state, "kis_overseas_next_open_quote_error", return_value=""),
            patch.object(state, "order_intent_market_session", return_value=None),
        ):
            functional_result = state.evaluate_order_gate_with_report(
                {"watchdog": {}},
                "BUY",
                False,
                functional_intent,
            )
            normal_result = state.evaluate_order_gate_with_report(
                {"watchdog": {}},
                "BUY",
                False,
                normal_intent,
            )

        self.assertTrue(functional_result[0], functional_result[4].to_dict())
        self.assertFalse(normal_result[0])
        normal_labels = {check.label for check in normal_result[4].blockers}
        self.assertIn("Portfolio Paper Evidence", normal_labels)
        self.assertIn("Exact Paper Final Binding", normal_labels)
        self.assertIn("전략 재검증", normal_labels)

    def test_explicit_start_bridge_never_submits_an_order(self) -> None:
        workspace = {
            "environment": "KIS_LIVE",
            "status": "ACTIVE",
            "current": {
                "ready": True,
                "selectedTargetKey": "portfolio:one",
                "blockers": [],
            },
            "candidates": [
                {
                    "key": "portfolio:one",
                    "kind": "PORTFOLIO",
                    "available": True,
                    "portfolioId": "portfolio-one",
                    "runtimeStrategyId": "lead-strategy",
                }
            ],
        }
        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "snapshot",
                return_value={
                    "profiles": {
                        "stock": {"phase": "STOPPED", "running": False}
                    }
                },
            ),
            patch.object(
                state,
                "start_continuous_runtime",
                return_value={"ok": True, "reason": "started"},
            ) as start,
            patch.object(
                state,
                "safety_confirmation_authoritative_context",
                return_value=(
                    {
                        "action": "FUNCTIONAL_TEST_START",
                        "request": {"targetKey": "portfolio:one"},
                    },
                    {},
                    "LIVE 4321",
                ),
            ),
        ):
            issued = state.issue_safety_confirmation(
                "FUNCTIONAL_TEST_START",
                {"targetKey": "portfolio:one"},
            )
            result = state.start_functional_test_runtime(
                workspace,
                confirmed=True,
                target_key="portfolio:one",
                safety_confirmation={
                    "challengeId": issued["challengeId"],
                    "token": issued["token"],
                    "typedPhrase": issued["expectedPhrase"],
                },
            )

        self.assertTrue(result["runtimeStarted"])
        self.assertFalse(result["brokerSubmissionPerformed"])
        self.assertFalse(result["promotionEligible"])
        start.assert_called_once_with(
            "stock",
            "SMALL_LIVE",
            "portfolio-one",
            "",
            "lead-strategy",
            state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
            {
                "functional_test_target_key": "portfolio:one",
                "functional_test_portfolio_only": True,
                "promotion_eligible": False,
                "use_as_promotion_evidence": False,
                "full_live_requested": False,
            },
            _functional_test_capability=state._FUNCTIONAL_TEST_START_CAPABILITY,
        )

    def test_running_session_renews_kis_preflight_beyond_five_minutes(self) -> None:
        base = datetime(2026, 8, 5, 0, 0, tzinfo=timezone.utc)
        permit = issue_functional_test_permit(
            binding=self.portfolio_binding(),
            environment="KIS_LIVE",
            duration_value=2,
            duration_unit="HOURS",
            caps=self.caps(),
            now=base - timedelta(minutes=1),
        )
        activation = issue_live_activation_token(
            permit=permit,
            market_day_close=base + timedelta(hours=2),
            authorized_by="functional-test-operator",
            now=base - timedelta(seconds=30),
        )
        scope = {
            "leadStrategy": {
                "strategy_id": "functional-strategy-artifact",
            },
            "portfolio": {},
            "portfolioId": "functional-portfolio-artifact",
            "portfolioArtifact": {
                "artifactId": "functional-portfolio-artifact",
                "artifactHash": "b" * 64,
            },
            "strategyMembers": [
                {
                    "strategyId": "functional-strategy-artifact",
                    "strategyInstanceId": "functional-strategy-instance",
                    "symbol": "005930",
                    "brokerId": "kis",
                    "artifactHash": "a" * 64,
                    "minimumVerificationHash": "c" * 64,
                }
            ],
            "strategyMemberHash": state.governance_sha256(
                [
                    {
                        "strategyId": "functional-strategy-artifact",
                        "strategyInstanceId": "functional-strategy-instance",
                        "symbol": "005930",
                        "brokerId": "kis",
                        "artifactHash": "a" * 64,
                        "minimumVerificationHash": "c" * 64,
                    }
                ]
            ),
            "allowedSymbols": ["005930"],
            "bindingHash": state.governance_sha256(
                self.portfolio_binding().snapshot()
            ),
            "accountFingerprint": state.governance_sha256(
                {"functionalTestAccount": "kis-live-account-hash"}
            ),
            "runtimeDeploymentId": "paper-functional-deployment",
        }
        store = OperationalGovernanceStore(
            self.root / "functional-governance.sqlite3",
            clock=lambda: base,
        )
        values = state._functional_test_manifest_values(
            scope,
            permit,
            activation,
            preflight_ttl_seconds=300,
        )
        manifest = store.create_deployment_manifest(
            **values,
            created_at=base,
        )
        initial = store.create_preflight_snapshot(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            checks=(
                {
                    "checkId": "fresh-kis-reconciliation",
                    "status": "PASS",
                    "evidenceHash": "d" * 64,
                },
            ),
            reconciliation_hash="e" * 64,
            broker_snapshot_hash="f" * 64,
            ttl_seconds=300,
            issued_at=base,
        )
        session_metadata = {
            **values["metadata"],
            "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
            "accountFingerprint": scope["accountFingerprint"],
        }
        session = store.create_runtime_session(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            profile="stock",
            mode="SMALL_LIVE",
            runtime_instance_id="functional-soak-test",
            preflight_snapshot_id=initial.snapshot_id,
            metadata=session_metadata,
            occurred_at=base,
        )
        session = store.transition_runtime_session(
            session.session_id,
            "STARTING",
            actor="test",
            occurred_at=base + timedelta(seconds=1),
        )
        session = store.transition_runtime_session(
            session.session_id,
            "RUNNING",
            actor="test",
            occurred_at=base + timedelta(seconds=2),
        )
        state.STATE.setdefault("active_runtime_session_ids", {})["stock"] = (
            session.session_id
        )
        state.STATE["broker_reconciliation"] = {
            "summary": {"fresh": True, "three_way_verified": True},
            "fetched_at": base.isoformat(),
            "errors": [],
        }
        intent = OrderIntent(
            strategy_id="functional-strategy-artifact",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=40_000,
            mode="SMALL_LIVE",
            reason="simulated-long-functional-runtime",
            metadata={
                "execution_purpose": "FUNCTIONAL_TEST",
                "functional_test_environment": "KIS_LIVE",
                "broker_id": "kis",
                "portfolio_id": "functional-portfolio-artifact",
                "strategy_instance_id": "functional-strategy-instance",
                "functional_test_session_id": session.session_id,
            },
        )
        evidence = {
            "safetyBlockers": [],
            "checks": [
                {
                    "checkId": "fresh-kis-reconciliation",
                    "status": "PASS",
                    "evidenceHash": "1" * 64,
                }
            ],
            "reconciliationHash": "2" * 64,
            "brokerSnapshotHash": "3" * 64,
        }
        with (
            patch.object(state, "OPERATIONAL_GOVERNANCE", store),
            patch.object(
                state,
                "functional_test_active_authority",
                return_value=(permit, activation),
            ),
            patch.object(
                state,
                "_functional_test_runtime_scope",
                return_value=scope,
            ),
            patch.object(
                state,
                "_functional_test_preflight_evidence",
                return_value=evidence,
            ) as collect_kis_truth,
            patch.object(
                state,
                "broker_account_risk",
                return_value={
                    "known": True,
                    "fresh": True,
                    "observed_at": base.isoformat(),
                },
            ),
            patch.object(
                state,
                "durable_control_snapshot",
                return_value={"halted": False},
            ),
        ):
            first = state.refresh_functional_test_runtime_preflight(
                intent,
                now=base + timedelta(minutes=4),
            )
            second = state.refresh_functional_test_runtime_preflight(
                intent,
                now=base + timedelta(minutes=8),
            )
            authorization = store.runtime_authorization(
                session.session_id,
                at=base + timedelta(minutes=10),
            )

        self.assertTrue(first[0], first)
        self.assertTrue(second[0], second)
        self.assertEqual("REFRESHED", first[2]["action"])
        self.assertEqual("REFRESHED", second[2]["action"])
        self.assertTrue(authorization["allowed"], authorization["reasons"])
        self.assertEqual(2, collect_kis_truth.call_count)
        rebound_events = [
            event
            for event in store.runtime_events(session.session_id)
            if event.event_type == "PREFLIGHT_REBOUND"
        ]
        self.assertEqual(2, len(rebound_events))

    def test_execution_observation_reserves_single_mux_socket_and_accepts_fresh_poll(self) -> None:
        streams = Mock()
        streams.snapshot.return_value = {
            "running": False,
            "brokers": {},
        }
        with (
            patch.object(state, "LIVE_EXECUTION_STREAMS", streams),
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(
                    True,
                    "functional-test-kis-execution-poll-fresh",
                    {"ageSeconds": 5, "errors": []},
                ),
            ),
        ):
            allowed, reason, details = (
                state.ensure_functional_test_execution_observation()
            )

        self.assertTrue(allowed, details)
        self.assertEqual(
            "functional-test-kis-execution-poll-fresh",
            reason,
        )
        self.assertFalse(details["owned"])
        self.assertFalse(details["reused"])
        self.assertTrue(details["privateThreadDisabled"])
        self.assertEqual(
            "KIS_MARKET_PRIVATE_SINGLE_SOCKET_MUX",
            details["observationMode"],
        )
        streams.start.assert_not_called()
        streams.stop_brokers.assert_not_called()

    def test_guard_runs_three_checks_across_simulated_sixty_seconds(self) -> None:
        class SimulatedStop:
            def __init__(self):
                self.waits: list[float] = []

            def wait(self, seconds):
                self.waits.append(float(seconds))
                return len(self.waits) > 3

        simulated_stop = SimulatedStop()
        with (
            patch.object(
                state,
                "FUNCTIONAL_TEST_RUNTIME_GUARD_SESSION_ID",
                "functional-session-60s",
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_RUNTIME_GUARD_STREAM_OWNED",
                False,
            ),
            patch.dict(
                state.FUNCTIONAL_TEST_RUNTIME_GUARD_STATUS,
                {
                    "running": True,
                    "sessionId": "functional-session-60s",
                    "lastCheckAt": "",
                    "lastSuccessAt": "",
                    "lastReason": "",
                    "failureCount": 0,
                    "kisExecutionStreamOwned": False,
                },
                clear=True,
            ),
            patch.object(
                state,
                "run_functional_test_runtime_guard_cycle",
                return_value=(
                    True,
                    "functional-test-preflight-current",
                    {},
                ),
            ) as cycle,
            patch.object(state, "append_audit"),
        ):
            state._functional_test_runtime_guard_worker(
                "functional-session-60s",
                simulated_stop,  # type: ignore[arg-type]
                20.0,
            )

        self.assertEqual(3, cycle.call_count)
        self.assertEqual([20.0, 20.0, 20.0, 20.0], simulated_stop.waits)

    def test_guard_cycle_polls_kis_before_preflight_revalidation(self) -> None:
        session_id = "functional-guard-cycle"
        state.STATE.setdefault("active_runtime_session_ids", {})["stock"] = (
            session_id
        )
        intent = OrderIntent(
            strategy_id="functional-strategy",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=1,
            mode="SMALL_LIVE",
            reason="guard",
            metadata={
                "execution_purpose": "FUNCTIONAL_TEST",
                "functional_test_session_id": session_id,
                "broker_id": "kis",
            },
        )
        with (
            patch.object(
                state,
                "poll_execution_events",
                return_value={"ok": True, "coalesced": False},
            ) as poll,
            patch.object(
                state,
                "_functional_test_execution_poll_fresh",
                return_value=(True, "fresh", {"ageSeconds": 1}),
            ),
            patch.object(
                state,
                "_functional_test_runtime_guard_intent",
                return_value=intent,
            ),
            patch.object(
                state,
                "refresh_functional_test_runtime_preflight",
                return_value=(True, "renewed", {"action": "REFRESHED"}),
            ) as renew,
        ):
            allowed, reason, details = (
                state.run_functional_test_runtime_guard_cycle(session_id)
            )

        self.assertTrue(allowed, details)
        self.assertEqual("renewed", reason)
        poll.assert_called_once_with(
            "kis",
            force_snapshot=True,
            include_snapshot=False,
        )
        renew.assert_called_once_with(intent)

    def test_explicit_start_blocks_while_stock_websocket_owner_is_active(self) -> None:
        workspace = {
            "environment": "KIS_LIVE",
            "status": "ACTIVE",
            "current": {
                "ready": True,
                "selectedTargetKey": "strategy:one",
                "blockers": [],
            },
            "candidates": [
                {
                    "key": "strategy:one",
                    "kind": "STRATEGY",
                    "available": True,
                    "strategyId": "strategy-one",
                    "runtimeStrategyId": "strategy-one",
                }
            ],
        }
        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "snapshot",
                return_value={
                    "profiles": {
                        "stock": {
                            "phase": "STOPPING",
                            "running": True,
                            "executionPurpose": "FUNCTIONAL_TEST",
                        }
                    }
                },
            ),
            patch.object(state, "start_continuous_runtime") as start,
        ):
            result = state.start_functional_test_runtime(
                workspace,
                confirmed=True,
                target_key="strategy:one",
            )

        self.assertFalse(result["ok"])
        self.assertFalse(result["runtimeStarted"])
        self.assertIn("KIS WebSocket", result["reason"])
        self.assertIn("STOPPING", result["reason"])
        start.assert_not_called()

    def test_portfolio_only_permit_omits_strategy_triple_without_weakening_binding(self) -> None:
        self.write_portfolio_documents()
        intent = OrderIntent(
            strategy_id="logical-sleeve-strategy",
            asset="KR_STOCK",
            symbol="005930",
            side="BUY",
            quantity=1,
            reference_price=40_000,
            mode="SMALL_LIVE",
            reason="portfolio-functional-test",
            metadata={
                "execution_purpose": "FUNCTIONAL_TEST",
                "environment": "KIS_LIVE",
                "portfolio_id": "functional-portfolio-artifact",
                "portfolio_artifact_id": "functional-portfolio-artifact",
                "portfolio_artifact_hash": "b" * 64,
                "portfolio_instance_id": "functional-portfolio-instance",
                "account_id": "kis-live-account-hash",
                "broker_id": "kis",
                "order_type": "00",
                "confirmed_bar_end": datetime.now(timezone.utc).isoformat(),
            },
        )

        def current_binding(_checks, _intent, *, portfolio_only=False):
            self.assertTrue(portfolio_only)
            return self.portfolio_binding()

        with (
            patch.object(
                state,
                "FUNCTIONAL_TEST_DOCUMENT_ROOT",
                self.root,
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_CURRENT_PERMIT_DOCUMENT",
                self.root / "live" / "current-permit.json",
            ),
            patch.object(
                state,
                "FUNCTIONAL_TEST_CURRENT_ACTIVATION_DOCUMENT",
                self.root / "live" / "current-activation.json",
            ),
            patch.object(
                state,
                "functional_test_current_binding",
                side_effect=current_binding,
            ),
            patch.object(
                state,
                "functional_test_global_caps",
                return_value=self.caps(),
            ),
            patch.object(
                state,
                "functional_test_risk_snapshot",
                return_value=self.safe_risk(),
            ),
        ):
            report, errors = state.functional_test_safety_report_for_intent(
                {"summary": {}},
                intent,
                phase="PRETRADE",
            )

        self.assertEqual([], errors)
        self.assertTrue(report.allowed, report.to_dict())
        self.assertEqual("", report.binding["strategy_artifact_id"])
        self.assertEqual(
            "functional-portfolio-artifact",
            report.binding["portfolio_artifact_id"],
        )

    def test_start_bridge_is_operator_confirmed_fail_closed(self) -> None:
        state.STATE["operator_confirmed"] = False
        with patch.object(state, "start_continuous_runtime") as start:
            result = state.start_functional_test_runtime(
                {},
                confirmed=True,
            )

        self.assertFalse(result["ok"])
        self.assertFalse(result["runtimeStarted"])
        self.assertFalse(result["brokerSubmissionPerformed"])
        start.assert_not_called()

    def test_expired_daily_activation_blocks_before_checkpoint(self) -> None:
        router = Mock()
        result = self.submit(
            self.intent(target_revision=2, expired_activation=True),
            risks=[self.safe_risk()],
            router=router,
        )

        self.assertFalse(result["ok"])
        self.assertIn("functional-test-live-activation-expired", result["reason"])
        self.assertEqual("risk_blocked", result["order"]["state"])
        self.assertFalse(result["order"]["promotion_eligible"])
        router.place_order.assert_not_called()
        self.assertEqual([], self.ledger.order_dispatch_rows())

    def test_caller_supplied_permit_override_is_never_authority(self) -> None:
        router = Mock()
        intent = self.intent(target_revision=21)
        intent = replace(
            intent,
            metadata={**intent.metadata, "functionalTestPermit": {}},
        )
        result = self.submit(
            intent,
            risks=[self.safe_risk()],
            router=router,
        )

        self.assertFalse(result["ok"])
        self.assertIn(
            "functional-test-caller-document-override-forbidden",
            result["reason"],
        )
        router.place_order.assert_not_called()
        self.assertEqual([], self.ledger.order_dispatch_rows())

    def test_dispatch_risk_drift_blocks_before_checkpoint(self) -> None:
        router = Mock()
        result = self.submit(
            self.intent(target_revision=3),
            risks=[self.safe_risk(), self.unsafe_risk()],
            router=router,
        )

        self.assertFalse(result["ok"])
        self.assertIn("functional-test-gross-exposure-cap-exceeded", result["reason"])
        self.assertEqual("DISPATCH", result["order"]["functional_test_dispatch"]["phase"])
        router.place_order.assert_not_called()
        self.assertEqual([], self.ledger.order_dispatch_rows())

    def test_final_binding_drift_blocks_after_checkpoint_before_kis_post(self) -> None:
        router = Mock()
        result = self.submit(
            self.intent(target_revision=4),
            risks=[self.safe_risk(), self.safe_risk(), self.safe_risk()],
            bindings=[
                self.binding(),
                self.binding(),
                self.binding(account_id="changed-account-hash"),
            ],
            router=router,
        )

        self.assertFalse(result["ok"])
        self.assertIn("functional-test-binding-account-id-mismatch", result["reason"])
        self.assertEqual("risk_blocked", result["order"]["state"])
        self.assertEqual(
            "DISPATCH",
            result["order"]["functional_test_dispatch_final"]["phase"],
        )
        router.place_order.assert_not_called()
        durable = self.ledger.order_dispatch_for_idempotency_key(
            result["order"]["idempotency_key"]
        )
        self.assertEqual("risk_blocked", durable["state"])

    def test_durable_stop_close_wins_late_cycle_before_kis_post(self) -> None:
        router = Mock()
        intent = self.intent(target_revision=41)
        original_status = self.ledger.functional_test_authority_status
        read_count = 0

        def close_only_at_final_post_boundary(**scope):
            nonlocal read_count
            read_count += 1
            status = original_status(**scope)
            if read_count >= 4:
                return {**status, "closed": True, "reason": "concurrent-stop"}
            return status

        with patch.object(
            self.ledger,
            "functional_test_authority_status",
            side_effect=close_only_at_final_post_boundary,
        ):
            result = self.submit(
                intent,
                risks=[self.safe_risk(), self.safe_risk(), self.safe_risk()],
                router=router,
            )

        self.assertFalse(result["ok"])
        self.assertIn("functional-test-authority-closed", result["reason"])
        router.place_order.assert_not_called()
        durable = self.ledger.order_dispatch_for_idempotency_key(
            result["order"]["idempotency_key"]
        )
        self.assertEqual("risk_blocked", durable["state"])

    def test_authorized_functional_order_reaches_only_mocked_kis_post(self) -> None:
        router = Mock()
        router.place_order.return_value = {
            "ok": True,
            "statusCode": 200,
            "json": {"output": {"ODNO": "FUNCTIONAL-KIS-ACK"}},
        }
        result = self.submit(
            self.intent(target_revision=5),
            risks=[self.safe_risk(), self.safe_risk(), self.safe_risk()],
            router=router,
        )

        self.assertTrue(result["ok"], result)
        self.assertEqual("acknowledged", result["order"]["state"])
        self.assertEqual(state.FUNCTIONAL_TEST_EXECUTION_PURPOSE, result["order"]["execution_purpose"])
        self.assertFalse(result["order"]["promotion_eligible"])
        self.assertFalse(result["order"]["functional_test"]["promotionEligible"])
        self.assertFalse(result["order"]["functional_test_dispatch_final"]["promotionEligible"])
        router.place_order.assert_called_once()
        payload = router.place_order.call_args.args[0]
        self.assertEqual("kis", payload["broker_id"])
        self.assertEqual("00", payload["order_type"])
        self.assertEqual("005930", payload["symbol"])

    def test_final_authority_token_rotation_blocks_before_mocked_kis_post(self) -> None:
        permit, activation_a = self.write_documents()
        intent = self.intent(
            target_revision=6,
            authority=(permit, activation_a),
        )
        now = datetime.now(timezone.utc) + timedelta(seconds=1)
        activation_b = issue_live_activation_token(
            permit=permit,
            market_day_close=now + timedelta(hours=2),
            authorized_by="functional-test-operator-rotation",
            now=now,
        )
        router = Mock()

        with patch.object(
            state,
            "functional_test_active_authority",
            return_value=(permit, activation_b),
        ):
            result = self.submit(
                intent,
                risks=[self.safe_risk(), self.safe_risk(), self.safe_risk()],
                router=router,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(
            "functional-test-final-authority-changed:"
            "activationTokenId,activationHash",
            result["reason"],
        )
        router.place_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
