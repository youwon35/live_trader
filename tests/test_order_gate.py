import copy
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.brokers import (
    BrokerNotReadyError,
    LiveBrokerRouter,
    fetch_kis_overseas_balance,
    parse_kis_overseas_positions,
)
from live_trader.audit_store import SQLiteAuditEventStore
from live_trader.execution_streams import parse_upbit_my_order
from live_trader.emergency_stop import _reset_emergency_stop_sticky_for_tests
from live_trader.program_ledger import ProgramLedger
from trading_runtime import (
    DeploymentStore,
    seal_strategy_artifact,
    stable_sha256,
)
from test_exact_paper_live_binding import exact_evidence_for_artifact


def empty_complete_kis_order_truth() -> dict[str, object]:
    return {
        "complete": True,
        "pagination_complete": True,
        "query_start_date": "20260801",
        "query_end_date": "20260809",
        "page_count": 1,
        "orders": [],
        "events": [],
        "correlation_policy": "official-broker-order-id-only",
        "absence_is_authoritative": True,
    }


class OrderGateTest(unittest.TestCase):
    def setUp(self) -> None:
        _reset_emergency_stop_sticky_for_tests()
        self.original_state = copy.deepcopy(state.STATE)
        # Durable real-account snapshots must never make order-gate unit tests
        # depend on the developer's current broker balance or daily PnL.
        state.STATE["account_risk"] = {}
        state.STATE["daily_loss_gate_tripped"] = False
        state.STATE["daily_loss_entries_blocked"] = False
        self.original_recovery_journal = state.RECOVERY_JOURNAL
        self.original_decision_trace_store = state.DECISION_TRACE_STORE
        self.recovery_temp_dir = tempfile.TemporaryDirectory()
        self.previous_emergency_stop_path = os.environ.get(
            "LIVE_TRADER_EMERGENCY_STOP_PATH"
        )
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            Path(self.recovery_temp_dir.name) / "emergency-stop.json"
        )
        state.RECOVERY_JOURNAL = state.RecoveryJournal(Path(self.recovery_temp_dir.name) / "recovery-journal")
        state.DECISION_TRACE_STORE = state.DecisionTraceStore(Path(self.recovery_temp_dir.name) / "decision-trace.jsonl")

    def tearDown(self) -> None:
        self.restore_temp_program_ledger()
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        state.RECOVERY_JOURNAL = self.original_recovery_journal
        state.DECISION_TRACE_STORE = self.original_decision_trace_store
        if self.previous_emergency_stop_path is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = (
                self.previous_emergency_stop_path
            )
        self.recovery_temp_dir.cleanup()
        _reset_emergency_stop_sticky_for_tests()

    def use_temp_program_ledger(self, temp_dir: str) -> ProgramLedger:
        self.original_program_ledger = state.PROGRAM_LEDGER
        ledger = ProgramLedger(Path(temp_dir) / "program_ledger.sqlite3")
        state.PROGRAM_LEDGER = ledger
        return ledger

    def restore_temp_program_ledger(self) -> None:
        if hasattr(self, "original_program_ledger"):
            state.PROGRAM_LEDGER = self.original_program_ledger

    @staticmethod
    def resume_artifact(
        strategy_id: str,
        *,
        lifecycle: str = "before-live-small",
        artifact_hash: str = "current-artifact-hash",
        validated_until: str = "2099-01-01T00:00:00+00:00",
    ) -> dict:
        normalized_artifact_hash = (
            artifact_hash
            if len(artifact_hash) == 64
            else stable_sha256(artifact_hash)
        )
        return {
            "id": strategy_id,
            "strategy_id": strategy_id,
            "strategyId": "moving_average_cross",
            "name": f"{strategy_id} Resume Test",
            "symbol": "BTCUSDT",
            "asset": "CRYPTO",
            "timeframe": "1h",
            "plugin": "moving_average_cross",
            "parameters": {"shortMa": 20, "longMa": 60},
            "status": lifecycle,
            "lifecycleStatus": lifecycle,
            "promotionStage": lifecycle,
            "lifecycle": {"status": lifecycle, "history": []},
            "promotion": {"stage": lifecycle, "history": []},
            "artifactLock": {
                "schemaVersion": "strategy-artifact-lock-v1",
                "lockId": f"lock-{normalized_artifact_hash}",
                "artifactHash": normalized_artifact_hash,
            },
            "finalTest": {"status": "pass", "passed": True},
            "portfolioCandidate": {"approved": True, "blockers": []},
            "revalidation": {
                "required": True,
                "status": "valid",
                "lastRevalidatedAt": "2026-07-25T00:00:00+00:00",
                "validatedUntil": validated_until,
            },
            "permissions": {
                "trader_export_allowed": True,
                "paper_trader_verified": True,
                "live_small_eligible": lifecycle in {"before-live-small", "live"},
                "live_eligible": lifecycle == "live",
                "live_allowed": lifecycle == "live",
                "fail_reasons": [],
            },
        }

    @staticmethod
    def save_resume_evidence(
        artifact_dir: Path,
        artifact_reference_payload: dict,
        *,
        evidence_id: str = "paper-resume-current",
        observed_days: int = 30,
        observed_seconds: int = 30 * 24 * 60 * 60,
        regime_count: int = 2,
        recovery_verified: bool = True,
        reconciliation_mismatches: int = 0,
        ended_at: str = "2026-07-25T00:00:00+00:00",
        promotion_source: str = "continuous-live-forward-next-open-v3",
    ) -> dict:
        evidence, pins = exact_evidence_for_artifact(
            artifact_reference_payload,
            evidence_id=evidence_id,
            observed_days=observed_days,
            observed_seconds=observed_seconds,
            regime_count=regime_count,
            recovery_verified=recovery_verified,
            reconciliation_mismatches=reconciliation_mismatches,
            ended_at=ended_at,
            promotion_source=promotion_source,
        )
        state.EvidenceStore(artifact_dir).save_paper(evidence)
        return pins

    def run_resume_scenario(
        self,
        artifact_payload: dict,
        *,
        evidence_artifact: dict | None = None,
        evidence_options: dict | None = None,
        persist_live_deployment_permissions: bool = True,
    ) -> tuple[dict, dict, dict]:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            artifact_payload = copy.deepcopy(artifact_payload)
            if evidence_artifact is not None:
                pins = self.save_resume_evidence(
                    artifact_dir,
                    evidence_artifact,
                    **(evidence_options or {}),
                )
                artifact_payload.setdefault("permissions", {}).update(pins)
            artifact_path = artifact_dir / "strategy.json"
            artifact_path.write_text(
                json.dumps(artifact_payload, ensure_ascii=False),
                encoding="utf-8",
            )
            try:
                # Paper evidence pins become Live authority only after an
                # explicit Live Deployment transition.  The production
                # migration path intentionally refuses to inherit Artifact
                # permissions, so this fixture models the governance step
                # instead of relying on the legacy Artifact fallback.
                (
                    strategy_dir,
                    _artifact_path,
                    raw_payload,
                    normalized,
                ) = state.find_strategy_artifact_payload(
                    artifact_payload["strategy_id"]
                )
                assert strategy_dir is not None
                assert raw_payload is not None
                assert normalized is not None
                deployment_store, live_deployment, _portfolio = (
                    state.ensure_live_deployment(
                        strategy_dir,
                        raw_payload,
                        normalized,
                    )
                )
                if persist_live_deployment_permissions:
                    deployment_store.transition(
                        live_deployment["deploymentId"],
                        lifecycle=state.normalize_lifecycle_status(
                            normalized.get("lifecycle_status") or "draft"
                        ),
                        mode="MONITOR",
                        permissions=dict(
                            artifact_payload.get("permissions") or {}
                        ),
                        actor="test-live-deployment-governance",
                        reason=(
                            "persist exact Paper pins on the Live Deployment"
                        ),
                    )
                with patch("live_trader.state.snapshot", return_value={}), patch(
                    "live_trader.state.append_audit",
                ):
                    pause = state.set_strategy_lifecycle_status(
                        artifact_payload["strategy_id"],
                        "pause",
                    )
                    resume = state.set_strategy_lifecycle_status(
                        artifact_payload["strategy_id"],
                        "resume",
                    )
                registry_path = artifact_dir / "deployments" / "deployment-registry.json"
                deployment = next(
                    iter(
                        json.loads(
                            registry_path.read_text(encoding="utf-8")
                        )["entries"].values()
                    )
                )
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact_dir
        return pause, resume, deployment

    def test_order_and_risk_classes_come_from_shared_runtime(self) -> None:
        self.assertEqual(state.OrderIntent.__module__, "trading_runtime.order_management")
        self.assertEqual(state.PreTradeRiskGate.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.PreTradeContext.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.StrategyExecutionRunner.__module__, "trading_runtime.strategy_runner")

    def test_strategy_broker_routing_prefers_explicit_artifact_contract(self) -> None:
        self.assertEqual(
            "upbit",
            state.strategy_broker_id(
                {
                    "dataset": {"provider": "upbit"},
                    "marketDataProvider": "binance",
                    "brokerId": "binance",
                    "symbol": "BTCUSDT",
                    "asset": "CRYPTO",
                }
            ),
        )
        self.assertEqual(
            "upbit",
            state.strategy_broker_id(
                {
                    "dataset": {"provider": "yfinance"},
                    "marketDataProvider": "upbit",
                    "brokerId": "binance",
                    "symbol": "BTCUSDT",
                    "asset": "CRYPTO",
                }
            ),
        )
        self.assertEqual(
            "upbit",
            state.strategy_broker_id(
                {
                    "marketDataProvider": "yfinance",
                    "brokerId": "upbit",
                    "symbol": "BTCUSDT",
                    "asset": "CRYPTO",
                }
            ),
        )
        self.assertEqual(
            "upbit",
            state.strategy_broker_id(
                {
                    "traderContract": {"scope": {"allowed_brokers": ["upbit"]}},
                    "symbol": "BTCUSDT",
                    "asset": "CRYPTO",
                }
            ),
        )
        self.assertEqual("upbit", state.strategy_broker_id({"symbol": "KRW-BTC", "asset": "CRYPTO"}))
        self.assertEqual("binance", state.strategy_broker_id({"symbol": "BTCUSDT", "asset": "CRYPTO"}))
        self.assertEqual("kis", state.strategy_broker_id({"symbol": "069500.KS", "asset": "KR-STOCK"}))

    def test_submitted_order_cancel_calls_broker_before_local_transition(self) -> None:
        order = {
            "order_id": "ord-live-1",
            "state": "acknowledged",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "binance",
            "broker_order_id": "778899",
            "symbol": "BTCUSDT",
            "asset": "crypto",
            "qty": "0.01",
            "broker_request": {"broker_id": "binance", "symbol": "BTCUSDT"},
        }
        state.STATE["orders"] = [order]

        class FakeRouter:
            def __init__(self) -> None:
                self.calls = 0

            def cancel_order(self, broker_id, broker_order_id, **context):
                self.calls += 1
                self.called = (broker_id, broker_order_id, context)
                return {"ok": True, "json": {"status": "CANCELED"}}

        fake_router = FakeRouter()
        with patch("live_trader.state.LiveBrokerRouter", return_value=fake_router):
            result = state.cancel_order("ord-live-1")

        self.assertTrue(result["ok"])
        self.assertEqual(("binance", "778899"), fake_router.called[:2])
        self.assertEqual("BTCUSDT", fake_router.called[2]["symbol"])
        self.assertEqual("cancel_pending", order["state"])
        self.assertEqual("reconcile_required", order["queue_state"])
        self.assertTrue(result["reconciliation_required"])
        self.assertTrue(order["cancel_request_id"].startswith("cancel-"))
        self.assertIn("broker_cancel_response", order)

        duplicate = state.cancel_order("ord-live-1")
        self.assertTrue(duplicate["ok"])
        self.assertEqual(result["cancel_request_id"], duplicate["cancel_request_id"])
        self.assertEqual(1, fake_router.calls)

    def test_ambiguous_cancel_result_requires_reconciliation_without_resubmit(self) -> None:
        order = {
            "order_id": "ord-live-timeout",
            "state": "acknowledged",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "binance",
            "broker_order_id": "778900",
            "symbol": "BTCUSDT",
            "asset": "crypto",
            "qty": "0.01",
            "broker_request": {"broker_id": "binance", "symbol": "BTCUSDT"},
        }
        state.STATE["orders"] = [order]

        class TimeoutRouter:
            def __init__(self) -> None:
                self.calls = 0

            def cancel_order(self, *_args, **_kwargs):
                self.calls += 1
                raise TimeoutError("response lost")

        router = TimeoutRouter()
        with patch("live_trader.state.LiveBrokerRouter", return_value=router):
            first = state.cancel_order("ord-live-timeout")
            second = state.cancel_order("ord-live-timeout")

        self.assertFalse(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(1, router.calls)
        self.assertEqual("unknown_cancel_result", order["state"])
        self.assertEqual("reconcile_required", order["queue_state"])
        self.assertTrue(second["reconciliation_required"])

    def test_kis_cancel_requires_unambiguous_composite_identity(self) -> None:
        today = datetime.now().strftime("%Y%m%d")
        cases = [
            (
                "legacy",
                {
                    "broker_order_id": "700001",
                    "organization_no": "001",
                },
                set(),
                "kis-cancel-composite-identity-required",
            ),
            (
                "ambiguous",
                {
                    "broker_order_id": "700002",
                    "order_date": today,
                    "organization_no": "001",
                    "broker_order_key": f"{today}:001:700002",
                },
                {"700002"},
                "kis-cancel-odno-ambiguous",
            ),
        ]
        for label, identity, ambiguous, expected_reason in cases:
            with self.subTest(label=label):
                order = {
                    "order_id": f"kis-{label}",
                    "state": "acknowledged",
                    "queue_state": "submitted",
                    "dry_run": False,
                    "broker_id": "kis",
                    "symbol": "005930",
                    "asset": "KR_STOCK",
                    "qty": 1,
                    **identity,
                }
                state.STATE["orders"] = [order]
                router = Mock()
                truth = {
                    "complete": True,
                    "fresh": True,
                    "absenceIsAuthoritative": True,
                    "lastError": "",
                    "ambiguousBrokerOrderIds": sorted(ambiguous),
                    "workingOrders": [dict(order)],
                }
                with (
                    patch.object(
                        state,
                        "refresh_kis_order_truth_for_kill_switch",
                        return_value={"ok": True, "truth": truth},
                    ),
                    patch.object(state, "LiveBrokerRouter", return_value=router),
                    patch.object(state, "append_audit"),
                    patch.object(state, "snapshot", return_value={}),
                ):
                    result = state.cancel_order(order["order_id"])

                self.assertFalse(result["ok"])
                self.assertTrue(result["reconciliation_required"])
                self.assertEqual(
                    expected_reason,
                    result["reason"],
                )
                router.cancel_order.assert_not_called()

    def test_kis_cancel_refreshes_and_requires_exact_current_working_row(self) -> None:
        today = datetime.now().strftime("%Y%m%d")
        order = {
            "order_id": "kis-current-working",
            "state": "acknowledged",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "kis",
            "broker_order_id": "700010",
            "broker_order_key": f"{today}:001:700010",
            "order_date": today,
            "organization_no": "001",
            "symbol": "005930",
            "asset": "KR_STOCK",
            "qty": 1,
        }
        truth = {
            "complete": True,
            "fresh": True,
            "absenceIsAuthoritative": True,
            "lastError": "",
            "ambiguousBrokerOrderIds": [],
            "workingOrders": [dict(order)],
        }
        state.STATE["orders"] = [order]
        router = Mock()
        router.cancel_order.return_value = {"ok": True, "json": {}}
        with (
            patch.object(
                state,
                "refresh_kis_order_truth_for_kill_switch",
                return_value={"ok": True, "truth": truth},
            ) as refresh,
            patch.object(state, "LiveBrokerRouter", return_value=router),
            patch.object(state, "append_audit"),
            patch.object(state, "queue_live_order_lifecycle_notification"),
            patch.object(state, "snapshot", return_value={}),
        ):
            result = state.cancel_order(order["order_id"])

        self.assertTrue(result["ok"])
        refresh.assert_called_once_with()
        router.cancel_order.assert_called_once()
        self.assertEqual("kis", router.cancel_order.call_args.args[0])
        self.assertEqual("700010", router.cancel_order.call_args.args[1])
        self.assertEqual(
            "001", router.cancel_order.call_args.kwargs["organization_no"]
        )

    def test_kis_cancel_truth_refresh_failure_posts_zero(self) -> None:
        today = datetime.now().strftime("%Y%m%d")
        order = {
            "order_id": "kis-refresh-failed",
            "state": "acknowledged",
            "queue_state": "submitted",
            "dry_run": False,
            "broker_id": "kis",
            "broker_order_id": "700011",
            "broker_order_key": f"{today}:001:700011",
            "order_date": today,
            "organization_no": "001",
            "symbol": "005930",
            "asset": "KR_STOCK",
            "qty": 1,
        }
        state.STATE["orders"] = [order]
        router = Mock()
        with (
            patch.object(
                state,
                "refresh_kis_order_truth_for_kill_switch",
                return_value={
                    "ok": False,
                    "truth": {
                        "complete": True,
                        "fresh": False,
                        "absenceIsAuthoritative": True,
                        "lastError": "timeout",
                        "workingOrders": [dict(order)],
                    },
                },
            ),
            patch.object(state, "LiveBrokerRouter", return_value=router),
            patch.object(state, "append_audit"),
            patch.object(state, "snapshot", return_value={}),
        ):
            result = state.cancel_order(order["order_id"])

        self.assertFalse(result["ok"])
        self.assertTrue(result["reconciliation_required"])
        self.assertEqual(
            "kis-cancel-official-truth-unavailable", result["reason"]
        )
        router.cancel_order.assert_not_called()

    def test_fill_truth_wins_late_cancel_event(self) -> None:
        order = {
            "order_id": "ord-live-race",
            "state": "cancel_pending",
            "queue_state": "reconcile_required",
            "cancel_request_id": "cancel-race",
            "broker_id": "binance",
            "broker_order_id": "778901",
            "symbol": "BTCUSDT",
            "asset": "crypto",
            "qty": "0.01",
            "filled_quantity": 0.0,
            "average_fill_price": 0.0,
            "fee": 0.0,
        }
        state.STATE["orders"] = [order]

        filled = state.apply_execution_events_to_local_orders(
            [
                {
                    "event_id": "fill-race-1",
                    "broker_id": "binance",
                    "broker_order_id": "778901",
                    "state": "filled",
                    "quantity": 0.01,
                    "price": 65_000.0,
                    "fee": 0.1,
                    "raw": {},
                }
            ]
        )
        canceled = state.apply_execution_events_to_local_orders(
            [
                {
                    "event_id": "cancel-race-2",
                    "broker_id": "binance",
                    "broker_order_id": "778901",
                    "state": "canceled",
                    "quantity": 0.0,
                    "price": 0.0,
                    "fee": 0.0,
                    "raw": {},
                }
            ]
        )

        self.assertEqual(1, filled)
        self.assertEqual(1, canceled)
        self.assertEqual("filled", order["state"])
        self.assertEqual("completed", order["queue_state"])
        self.assertEqual("filled-truth-prevailed", order["cancel_reconciliation"])

    def test_strategy_market_data_uses_shared_canonical_event(self) -> None:
        strategy = {
            "symbol": "BTCUSDT",
            "instrument_id": "crypto:binance:spot:BTCUSDT",
            "broker_id": "binance",
            "market": "BINANCE",
            "market_type": "SPOT",
            "reference_price": 60000,
            "price_time": "2026-07-01T00:00:00Z",
        }

        event = state.strategy_market_event(strategy)
        market_data = state.strategy_market_data(strategy)

        self.assertEqual(event.__class__.__module__, "trading_runtime.market_data")
        self.assertEqual(market_data.instrument_id, event.instrument_id)
        self.assertEqual(market_data.provider, "binance")
        self.assertEqual(market_data.occurred_at, "2026-07-01T00:00:00Z")
        self.assertEqual(market_data.reference_price, 60000)

    def test_live_order_intent_is_registered_in_shared_oms_with_idempotency(self) -> None:
        checks = state.snapshot()
        intent = state.default_order_intent(checks, "BUY")
        intent = state.OrderIntent(
            **{**intent.__dict__, "metadata": {**intent.metadata, "portfolio_id": "test", "instrument_id": "btc", "target_revision": 999991}}
        )
        result = state.submit_order_intent(checks, intent, dry_run=True, audit_event="OMS test")
        order = result["order"]
        self.assertIn("idempotency_key", order)
        self.assertIn("oms_status", order)
        self.assertTrue(order["trace_id"].startswith("trace_"))
        trace_stages = [item["eventType"] for item in state.DECISION_TRACE_STORE.trace(order["trace_id"])]
        self.assertEqual(trace_stages[:4], ["BAR_CLOSED", "SIGNAL_DECIDED", "TARGET_ALLOCATED", "RISK_DECIDED"])
        self.assertEqual(trace_stages[-1], "ORDER_CREATED" if result["ok"] else "BLOCKED")
        self.assertTrue(state.LIVE_OMS.verify_event_chain(order["oms_order_id"]))
        self.assertFalse(order["idempotency_duplicate"])

    def test_small_live_supported_brokers_record_exact_canary_scope_before_gate(self) -> None:
        state.STATE["orders"] = []
        state.STATE["persisted_idempotency_keys"] = []
        checks = {"summary": {"blocker_count": 0}}
        blocked_report = state.PreTradeRiskReport(
            datetime(2026, 8, 4),
            (state.RiskCheck("unit", "fail", "deliberate block"),),
        )

        def exact_scope(strategy_id: str, *, materialize: bool) -> dict:
            self.assertTrue(materialize)
            return {
                "schemaVersion": state.CANARY_SCOPE_SCHEMA_VERSION,
                "eligible": True,
                "strategyId": strategy_id,
                "strategyInstanceId": f"{strategy_id}-instance",
            }

        with patch.object(
            state,
            "current_live_canary_scope",
            side_effect=exact_scope,
        ) as current_scope, patch.object(
            state,
            "evaluate_order_gate_with_report",
            return_value=(
                False,
                "risk_blocked",
                "blocked",
                "deliberate-block",
                blocked_report,
            ),
        ), patch.object(
            state,
            "dispatch_live_order_with_checkpoint",
        ) as dispatch, patch(
            "live_trader.state.snapshot",
            return_value=checks,
        ):
            for revision, (broker_id, symbol, asset) in enumerate(
                (
                    ("kis", "069500.KS", "kr-stock"),
                    ("binance", "BTCUSDT", "CRYPTO"),
                    ("upbit", "KRW-BTC", "CRYPTO"),
                ),
                start=9_876_543,
            ):
                strategy_id = f"CANARY-{broker_id.upper()}"
                intent = state.OrderIntent(
                    strategy_id=strategy_id,
                    asset=asset,
                    symbol=symbol,
                    side="BUY",
                    quantity=0.001,
                    reference_price=60_000,
                    mode="SMALL_LIVE",
                    reason="unit canary scope",
                    metadata={
                        "broker_id": broker_id,
                        "strategy_instance_id": f"{strategy_id}-instance",
                        "portfolio_id": "",
                        "instrument_id": f"{broker_id}:{symbol}",
                        "target_revision": revision,
                    },
                )
                result = state.submit_order_intent(
                    checks,
                    intent,
                    dry_run=False,
                    audit_event=f"{broker_id} canary scope test",
                )

                self.assertFalse(result["ok"])
                self.assertEqual(
                    strategy_id,
                    result["order"]["canary_scope"]["strategyId"],
                )
                current_scope.assert_any_call(
                    strategy_id,
                    materialize=True,
                )

        self.assertEqual(3, current_scope.call_count)
        dispatch.assert_not_called()

    def test_small_live_supported_brokers_block_missing_or_invalid_canary_scope(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["persisted_idempotency_keys"] = []
        checks = {"summary": {"blocker_count": 0}}
        passing_report = state.PreTradeRiskReport(
            datetime(2026, 8, 4),
            (state.RiskCheck("unit", "pass", "base gate passed"),),
        )

        def unavailable_scope(strategy_id: str, *, materialize: bool) -> dict:
            self.assertTrue(materialize)
            if strategy_id.endswith("-MISSING"):
                return {}
            return {
                "schemaVersion": state.CANARY_SCOPE_SCHEMA_VERSION,
                "eligible": False,
                "issues": ["paper-live-qualification:pin-mismatch"],
            }

        brokers = (
            ("kis", "069500.KS", "kr-stock"),
            ("binance", "BTCUSDT", "CRYPTO"),
            ("upbit", "KRW-BTC", "CRYPTO"),
            ("binance-futures", "ETHUSDT", "CRYPTO"),
        )
        with patch.object(
            state,
            "current_live_canary_scope",
            side_effect=unavailable_scope,
        ) as current_scope, patch.object(
            state,
            "evaluate_order_gate_with_report",
            return_value=(
                True,
                "approved",
                "queued",
                "risk pass",
                passing_report,
            ),
        ), patch.object(
            state,
            "dispatch_live_order_with_checkpoint",
        ) as dispatch, patch(
            "live_trader.state.snapshot",
            return_value=checks,
        ):
            revision = 10_000_000
            for broker_id, symbol, asset in brokers:
                for scope_case in ("MISSING", "INVALID"):
                    revision += 1
                    strategy_id = (
                        f"CANARY-{broker_id.upper()}-{scope_case}"
                    )
                    intent = state.OrderIntent(
                        strategy_id=strategy_id,
                        asset=asset,
                        symbol=symbol,
                        side="BUY",
                        quantity=0.001,
                        reference_price=60_000,
                        mode="SMALL_LIVE",
                        reason="unit missing canary scope",
                        metadata={
                            "broker_id": broker_id,
                            "strategy_instance_id": (
                                f"{strategy_id}-instance"
                            ),
                            "portfolio_id": "",
                            "instrument_id": f"{broker_id}:{symbol}",
                            "target_revision": revision,
                        },
                    )

                    result = state.submit_order_intent(
                        checks,
                        intent,
                        dry_run=False,
                        audit_event=f"{broker_id} invalid scope test",
                    )

                    self.assertFalse(result["ok"])
                    order = result["order"]
                    expected_reason = (
                        "live-canary-scope-missing"
                        if scope_case == "MISSING"
                        else "live-canary-scope-invalid:"
                        "paper-live-qualification:pin-mismatch"
                    )
                    self.assertEqual("risk_blocked", order["state"])
                    self.assertEqual("blocked", order["queue_state"])
                    self.assertEqual(expected_reason, order["reason"])
                    self.assertEqual(
                        expected_reason,
                        order["canary_scope_error"],
                    )
                    self.assertTrue(
                        any(
                            check["label"] == "Live Canary Scope"
                            and check["status"] == "fail"
                            and check["detail"] == expected_reason
                            for check in order["risk_report"]["checks"]
                        )
                    )
                    self.assertIn(
                        expected_reason,
                        state.STATE["audit"][-1]["detail"],
                    )
                    self.assertIn(
                        "Live Canary Scope",
                        state.STATE["audit"][-1]["detail"],
                    )
                    current_scope.assert_any_call(
                        strategy_id,
                        materialize=True,
                    )

        self.assertEqual(8, current_scope.call_count)
        dispatch.assert_not_called()

    def test_durable_global_kill_blocks_order_even_if_local_switch_is_off(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            control_path = Path(temp_dir) / "control.json"
            state.DurableControlState(control_path).set_global_kill(True, "hub incident")
            with patch.dict(os.environ, {"TRADING_CONTROL_STATE_PATH": str(control_path)}):
                context = state.pre_trade_context(state.snapshot(), state.default_order_intent(state.snapshot(), "BUY"), True)
        self.assertTrue(context.halted)

    def test_order_gate_blocks_buy_when_new_entries_are_blocked(self) -> None:
        state.STATE["new_entries_blocked"] = True

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("신규 진입 차단", reason)

    def test_order_gate_blocks_when_readiness_has_blockers(self) -> None:
        state.STATE["new_entries_blocked"] = False

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 2}},
            "SELL",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("readiness blocker 2개", reason)

    def test_exchange_holiday_blocks_equity_order(self) -> None:
        state.STATE["new_entries_blocked"] = False
        intent = state.OrderIntent(
            strategy_id="krx-calendar-test",
            asset="KR_STOCK",
            symbol="069500.KS",
            side="BUY",
            quantity=1,
            reference_price=1000,
            mode=state.current_mode(),
            reason="calendar test",
            metadata={"broker_id": "kis"},
        )
        with patch.object(state, "market_session_state", return_value={"orderable": False, "detail": "거래소 휴장일"}):
            ok, order_state, _queue_state, reason, report = state.evaluate_order_gate_with_report(
                {"summary": {"blocker_count": 0}},
                "BUY",
                dry_run=False,
                intent=intent,
            )

        self.assertFalse(ok)
        self.assertEqual("adapter_blocked", order_state)
        self.assertTrue(reason)
        self.assertTrue(any(check.label == "거래소 세션" for check in report.checks))

    def test_kis_us_symbol_uses_nyse_session_not_krx(self) -> None:
        intent = state.OrderIntent(
            strategy_id="us-session-test",
            asset="US_STOCK",
            symbol="AAPL",
            side="BUY",
            quantity=1,
            reference_price=200,
            mode=state.current_mode(),
            reason="calendar route",
            metadata={"broker_id": "kis"},
        )
        with patch.object(
            state,
            "market_session_state",
            return_value={"orderable": True, "detail": "open"},
        ) as market:
            result = state.order_intent_market_session(intent)

        self.assertTrue(result["orderable"])
        market.assert_called_once_with(
            "XNYS",
            regular_open="09:30",
            regular_close="16:00",
        )

    def test_kis_us_next_open_requires_fresh_quote_lifecycle(self) -> None:
        intent = state.OrderIntent(
            strategy_id="us-next-open-test",
            asset="US_STOCK",
            symbol="AAPL",
            side="BUY",
            quantity=1,
            reference_price=200,
            mode=state.current_mode(),
            reason="closed bar",
            metadata={
                "broker_id": "kis",
                "order_type": "00",
                "execution_timing": "next-open-boundary",
            },
        )

        error = state.kis_overseas_next_open_quote_error(intent)

        self.assertIn("5초 이내 실시간 호가", error)

    def test_native_market_payload_does_not_reuse_decision_close(self) -> None:
        upbit_buy = state.OrderIntent(
            strategy_id="upbit-buy",
            asset="코인",
            symbol="KRW-BTC",
            side="BUY",
            quantity=0.00005,
            reference_price=100_000_000,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "upbit", "order_type": "price"},
        )
        upbit_sell = state.OrderIntent(
            strategy_id="upbit-sell",
            asset="코인",
            symbol="KRW-BTC",
            side="SELL",
            quantity=0.00005,
            reference_price=100_000_000,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "upbit", "order_type": "market"},
        )
        kis_domestic = state.OrderIntent(
            strategy_id="kis-market",
            asset="한국주식",
            symbol="069500.KS",
            side="BUY",
            quantity=1,
            reference_price=35_000,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis", "order_type": "01"},
        )
        kis_overseas = state.OrderIntent(
            strategy_id="kis-us-limit",
            asset="미국주식",
            symbol="AAPL",
            side="BUY",
            quantity=1,
            reference_price=190,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={
                "broker_id": "kis",
                "order_type": "00",
                "fresh_quote_verified": True,
                "fresh_quote_price": 201.25,
            },
        )

        buy_payload = state.live_broker_payload(
            upbit_buy,
            idempotency_key="buy-key",
        )
        sell_payload = state.live_broker_payload(
            upbit_sell,
            idempotency_key="sell-key",
        )
        kis_payload = state.live_broker_payload(
            kis_domestic,
            idempotency_key="kis-key",
        )
        kis_us_payload = state.live_broker_payload(
            kis_overseas,
            idempotency_key="kis-us-key",
        )

        self.assertEqual(upbit_buy.notional, buy_payload["price"])
        self.assertEqual(0.0, sell_payload["price"])
        self.assertEqual(0.0, kis_payload["price"])
        self.assertEqual("01", kis_payload["order_type"])
        self.assertEqual(201.25, kis_us_payload["price"])

    def test_order_gate_records_dry_run_without_broker_transmission(self) -> None:
        state.STATE["new_entries_blocked"] = False

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertTrue(ok)
        self.assertEqual(order_state, "dry_run")
        self.assertEqual(queue_state, "simulated")
        self.assertIn("브로커 전송 없이", reason)

    def test_explicit_gate_snapshot_does_not_reload_portfolios_from_disk(self) -> None:
        state.STATE["new_entries_blocked"] = False
        checks = {"summary": {"blocker_count": 0}, "strategies": []}

        with patch.object(state, "portfolio_rows", side_effect=AssertionError("disk reload")):
            ok, order_state, queue_state, _reason = state.evaluate_order_gate(checks, "BUY", dry_run=True)

        self.assertTrue(ok)
        self.assertEqual(order_state, "dry_run")
        self.assertEqual(queue_state, "simulated")

    def test_order_gate_blocks_new_buy_when_strategy_revalidation_expired(self) -> None:
        state.STATE["new_entries_blocked"] = False
        checks = {
            "summary": {"blocker_count": 0},
            "strategies": [
                {
                    "strategy_id": "STALE-1",
                    "symbol": "BTCUSDT",
                    "asset": "crypto",
                    "revalidation": {
                        "required": True,
                        "validatedUntil": "2000-01-01T00:00:00+00:00",
                    },
                }
            ],
        }
        buy_intent = state.OrderIntent(
            strategy_id="STALE-1",
            asset="crypto",
            symbol="BTCUSDT",
            side="BUY",
            quantity=1,
            reference_price=100,
            mode="MONITOR",
            reason="unit",
            metadata={"broker_id": "binance"},
        )
        sell_intent = state.OrderIntent(
            strategy_id="STALE-1",
            asset="crypto",
            symbol="BTCUSDT",
            side="SELL",
            quantity=1,
            reference_price=100,
            mode="MONITOR",
            reason="unit",
            metadata={"broker_id": "binance"},
        )
        state.STATE["broker_reconciliation"]["positions"] = [{
            "broker_id": "binance",
            "symbol": "BTCUSDT",
            "quantity": 1,
        }]

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(checks, "BUY", True, buy_intent)
        sell_ok, sell_order_state, sell_queue_state, sell_reason, _ = state.evaluate_order_gate_with_report(checks, "SELL", True, sell_intent)

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("전략 재검증", reason)
        self.assertFalse(report.can_submit)
        self.assertTrue(sell_ok)
        self.assertEqual(sell_order_state, "dry_run")
        self.assertEqual(sell_queue_state, "simulated")

    def test_portfolio_artifact_blocks_live_order_outside_universe(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [{"strategy_id": "STRAT-1", "symbol": "069500.KS", "asset": "kr-stock"}],
            "portfolios": [
                {
                    "id": "portfolio-1",
                    "name": "Portfolio 1",
                    "lifecycle_status": "before-live-small",
                    "permissions": {"live_small_allowed": True, "live_allowed": False},
                    "strategy_instances": [{"strategyId": "OTHER", "symbol": "005930.KS"}],
                    "target_portfolio": [{"strategyId": "OTHER", "symbol": "005930.KS", "targetWeight": 0.5}],
                    "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
                    "risk_checks": [],
                }
            ],
        }
        intent = state.OrderIntent(
            strategy_id="STRAT-1",
            asset="kr-stock",
            symbol="069500.KS",
            side="BUY",
            quantity=10,
            reference_price=38900,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis", "portfolio_id": "portfolio-1"},
        )

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(checks, "BUY", True, intent)

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("Portfolio", reason)
        self.assertTrue(any(check.label == "Portfolio Artifact" and check.status == "fail" for check in report.checks))

    def test_portfolio_artifact_target_weight_limits_live_order(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["risk_settings"]["strategy_capital_limit_krw"] = 20_000_000
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [{"strategy_id": "STRAT-1", "symbol": "069500.KS", "asset": "kr-stock"}],
            "portfolios": [
                {
                    "id": "portfolio-1",
                    "name": "Portfolio 1",
                    "lifecycle_status": "before-live-small",
                    "permissions": {"live_small_allowed": True, "live_allowed": False},
                    "strategy_instances": [
                        {
                            "strategyId": "STRAT-1",
                            "symbol": "069500.KS",
                            "allocation": {"normalizedWeight": 0.001},
                        }
                    ],
                    "target_portfolio": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.001}],
                    "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
                    "risk_checks": [],
                }
            ],
        }
        intent = state.OrderIntent(
            strategy_id="STRAT-1",
            asset="kr-stock",
            symbol="069500.KS",
            side="BUY",
            quantity=10,
            reference_price=38900,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis"},
        )

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(checks, "BUY", True, intent)

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertTrue(any(check.label == "Portfolio Artifact" and check.status == "pass" for check in report.checks))
        self.assertTrue(
            any(check.label in {"전략별 자본 한도", "종목별 최대 비중"} and check.status == "fail" for check in report.checks)
        )
        self.assertTrue("전략별 자본 한도" in reason or "종목별 최대 비중" in reason)

    def test_portfolio_gate_applies_strategy_instance_position_size_fraction(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        portfolio = {
            "id": "sized-portfolio",
            "name": "Sized Portfolio",
            "lifecycle_status": "before-live-small",
            "permissions": {"live_small_allowed": True},
            "strategy_instances": [
                {
                    "strategyId": "STRAT-1",
                    "symbol": "069500.KS",
                    "instanceId": "instance-1",
                    "positionSizeFraction": 0.2,
                }
            ],
            "target_portfolio": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.6}],
            "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
            "risk_checks": [],
            "portfolio_policy": {
                "allocations": [
                    {
                        "strategyInstanceId": "instance-1",
                        "targetWeight": 0.6,
                        "positionSizeFraction": 0.2,
                    }
                ]
            },
        }

        gate = state.portfolio_gate_for_strategy(
            {"strategy_id": "STRAT-1", "symbol": "069500.KS"},
            [portfolio],
            mode="SMALL_LIVE",
        )

        self.assertTrue(gate["allowed"])
        self.assertAlmostEqual(gate["configuredTargetWeight"], 0.6)
        self.assertAlmostEqual(gate["positionSizeFraction"], 0.2)
        self.assertAlmostEqual(gate["policyTargetWeight"], 0.12)
        self.assertAlmostEqual(gate["targetWeight"], 0.12)
        self.assertTrue(gate["fxFreshness"]["fresh"])
        self.assertEqual(gate["fxFreshness"]["source"], "same-currency")

    def test_unmatched_portfolios_do_not_block_standalone_strategy(self) -> None:
        portfolio = {
            "id": "other-portfolio",
            "lifecycle_status": "before-live-small",
            "permissions": {"live_small_allowed": True},
            "strategy_instances": [
                {"strategyId": "OTHER", "symbol": "ETHUSDT", "instanceId": "other-1"}
            ],
            "target_portfolio": [
                {"strategyId": "OTHER", "symbol": "ETHUSDT", "targetWeight": 0.1}
            ],
        }

        gate = state.portfolio_gate_for_strategy(
            {"strategy_id": "BTC-1", "symbol": "BTCUSDT"},
            [portfolio],
            mode="SMALL_LIVE",
        )

        self.assertTrue(gate["allowed"])
        self.assertFalse(gate["active"])
        self.assertIn("단일 전략", gate["detail"])

    def test_reconciliation_summary_is_scoped_to_selected_broker(self) -> None:
        mixed = {
            "positions": [
                {"broker_id": "binance", "status": "pass"},
                {"broker_id": "kis", "status": "api_required"},
            ],
            "accounts": [{"broker_id": "binance", "status": "pass"}],
        }
        with patch("live_trader.state.reconciliation_snapshot", return_value=mixed), patch(
            "live_trader.state.broker_reconciliation_errors",
            return_value={"kis": "overseas balance unavailable"},
        ):
            binance = state.reconciliation_summary_for_broker("binance")
            kis = state.reconciliation_summary_for_broker("kis")

        self.assertEqual("pass", binance["status"])
        self.assertEqual(2, binance["pass_count"])
        self.assertEqual("warn", kis["status"])
        self.assertGreater(kis["api_required_count"], 0)

    def test_empty_durable_ledger_and_broker_snapshot_do_not_create_phantom_positions(self) -> None:
        with patch("live_trader.state.live_position_rows", return_value={}), patch(
            "live_trader.state.program_position_rows",
            return_value={},
        ), patch(
            "live_trader.state.successful_position_brokers",
            return_value={"kis"},
        ), patch(
            "live_trader.state.broker_reconciliation_errors",
            return_value={},
        ):
            rows = state.positions()

        self.assertEqual([], rows)
        self.assertFalse(any(row.get("program_source") == "sample" for row in rows))

        scoped = {
            "positions": [],
            "accounts": [{"broker_id": "kis", "status": "pass"}],
        }
        with patch("live_trader.state.reconciliation_snapshot", return_value=scoped), patch(
            "live_trader.state.broker_reconciliation_errors",
            return_value={},
        ):
            summary = state.reconciliation_summary_for_broker("kis")

        self.assertEqual("pass", summary["status"])
        self.assertEqual(0, summary["api_required_count"])
        self.assertEqual(0, summary["capability_gap_count"])
        self.assertEqual(0, summary["blocking_count"])

    def test_kis_capability_gap_blocks_only_overseas_intent(self) -> None:
        state.STATE["kill_switch"] = False
        summary = {
            "status": "warn",
            "api_required_count": 0,
            "capability_gap_count": 1,
            "mismatch_count": 0,
            "blocking_count": 0,
        }
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 1},
            "strategies": [
                {
                    "strategy_id": "KIS-LIVE-SMALL",
                    "symbol": "069500.KS",
                    "asset": "kr-stock",
                    "live_small_eligible": True,
                }
            ],
            "brokers": [{"broker_id": "kis", "order_ready": True}],
        }
        domestic = state.OrderIntent(
            strategy_id="KIS-LIVE-SMALL",
            asset="kr-stock",
            symbol="069500.KS",
            side="BUY",
            quantity=1,
            reference_price=40000,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis"},
        )
        overseas = state.OrderIntent(
            strategy_id="KIS-US-LIVE-SMALL",
            asset="us-stock",
            symbol="SPY",
            side="BUY",
            quantity=1,
            reference_price=600,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis"},
        )

        with patch("live_trader.state.real_orders_enabled", return_value=True), patch(
            "live_trader.state.checklist_rows",
            return_value=[],
        ), patch(
            "live_trader.state.durable_control_snapshot",
            return_value={"halted": False},
        ), patch(
            "live_trader.state.reconciliation_summary_for_broker",
            return_value=summary,
        ):
            domestic_blockers = state.intent_readiness_blocker_count(checks, domestic, summary)
            overseas_blockers = state.intent_readiness_blocker_count(checks, overseas, summary)
            domestic_context = state.pre_trade_context(checks, domestic, dry_run=True)
            overseas_context = state.pre_trade_context(checks, overseas, dry_run=True)

        self.assertEqual(0, domestic_blockers)
        self.assertEqual(1, overseas_blockers)
        self.assertTrue(domestic_context.positions_matched)
        self.assertEqual(0, domestic_context.readiness_blockers)
        self.assertTrue(overseas_context.positions_matched)
        self.assertEqual(1, overseas_context.readiness_blockers)

    def test_empty_broker_reconciliation_fails_closed(self) -> None:
        with patch(
            "live_trader.state.reconciliation_snapshot",
            return_value={"positions": [], "accounts": []},
        ), patch("live_trader.state.broker_reconciliation_errors", return_value={}):
            summary = state.reconciliation_summary_for_broker("binance")

        self.assertEqual("warn", summary["status"])
        self.assertEqual(1, summary["api_required_count"])

    def test_portfolio_gate_blocks_foreign_asset_when_fx_is_stale(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        portfolio = {
            "id": "stale-fx-portfolio",
            "lifecycle_status": "before-live-small",
            "permissions": {"live_small_allowed": True},
            "strategy_instances": [{"strategyId": "BTC-1", "symbol": "BTCUSDT", "instanceId": "btc-1"}],
            "target_portfolio": [{"strategyId": "BTC-1", "symbol": "BTCUSDT", "targetWeight": 0.1}],
            "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
            "risk_checks": [],
            "portfolio_policy": {"allocations": [{"strategyInstanceId": "btc-1", "targetWeight": 0.1}]},
            "portfolio": {"baseCurrency": "KRW", "fxPolicy": {"conversions": [{"currency": "USDT", "baseCurrency": "KRW", "rate": 1400, "sourceDate": "2020-01-01"}]}},
        }

        gate = state.portfolio_gate_for_strategy({"strategy_id": "BTC-1", "symbol": "BTCUSDT"}, [portfolio], mode="SMALL_LIVE")

        self.assertFalse(gate["allowed"])
        self.assertFalse(gate["fxFreshness"]["fresh"])
        self.assertIn("fx-stale", gate["detail"])

    def test_portfolio_policy_requires_complete_economic_rebalance_inputs(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        portfolio = {
            "id": "policy-portfolio",
            "name": "Policy Portfolio",
            "lifecycle_status": "before-live-small",
            "permissions": {"live_small_allowed": True},
            "strategy_instances": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "instanceId": "instance-1"}],
            "target_portfolio": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.4}],
            "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
            "risk_checks": [],
            "portfolio_policy_hash": "policy-hash",
            "portfolio_policy": {
                "policyHash": "policy-hash",
                "profiles": [{"strategyInstanceId": "instance-1", "assetClass": "KR_STOCK", "returnSource": "TREND", "instrumentId": "KRX:069500"}],
                "allocations": [{"strategyInstanceId": "instance-1", "targetWeight": 0.25, "returnSource": "TREND"}],
                "limits": [{"level": "risk_cluster", "key": "TREND", "maximumWeight": 0.4}],
                "rebalancePolicy": {"deadbandWeight": 0.01, "minimumNotional": 10000},
            },
        }
        base = dict(strategy_id="STRAT-1", asset="kr-stock", symbol="069500.KS", side="BUY", quantity=1, reference_price=38900, mode="SMALL_LIVE", reason="unit")
        missing = state.OrderIntent(**base, metadata={"broker_id": "kis"})
        uneconomic = state.OrderIntent(**base, metadata={"broker_id": "kis", "current_weight": 0.1, "portfolio_equity": 10_000_000, "expected_alpha_bps": 5, "expected_cost_bps": 8})
        economic = state.OrderIntent(**base, metadata={"broker_id": "kis", "current_weight": 0.1, "portfolio_equity": 10_000_000, "expected_alpha_bps": 12, "expected_cost_bps": 8})
        checks = {"strategies": [], "portfolios": [portfolio]}

        missing_gate = state.portfolio_gate_for_intent(checks, missing)
        uneconomic_gate = state.portfolio_gate_for_intent(checks, uneconomic)
        economic_gate = state.portfolio_gate_for_intent(checks, economic)

        self.assertFalse(missing_gate["allowed"])
        self.assertEqual(missing_gate["rebalanceDecision"]["reason"], "rebalance-inputs-missing")
        self.assertFalse(uneconomic_gate["allowed"])
        self.assertEqual(uneconomic_gate["rebalanceDecision"]["reason"], "expected-alpha-does-not-cover-cost")
        self.assertTrue(economic_gate["allowed"])
        self.assertEqual(economic_gate["targetWeight"], 0.25)
        self.assertEqual(economic_gate["rebalanceDecision"]["action"], "TRADE")

    def test_advanced_portfolio_policy_blocks_new_risk_but_allows_reduction_and_replay(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        portfolio = {
            "id": "advanced-portfolio", "name": "Advanced", "lifecycle_status": "before-live-small",
            "permissions": {"live_small_allowed": True},
            "strategy_instances": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "instanceId": "instance-1"}],
            "target_portfolio": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.4}],
            "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0}, "risk_checks": [],
            "portfolio_policy_hash": "policy-v1",
            "portfolio_policy": {
                "policyHash": "policy-v1",
                "profiles": [{"strategyInstanceId": "instance-1", "assetClass": "KR_STOCK", "returnSource": "TREND", "instrumentId": "KRX:069500"}],
                "allocations": [{"strategyInstanceId": "instance-1", "targetWeight": 0.4, "returnSource": "TREND"}],
                "limits": [], "rebalancePolicy": {"deadbandWeight": 0.001, "minimumNotional": 0},
            },
            "advanced_operations_hash": "advanced-v1",
            "advanced_operations": {
                "mandate": {"compliant": False, "breaches": ["target-volatility-exceeded"]},
                "capacity": [{"strategyInstanceId": "instance-1", "maximumOrderNotional": 50_000, "allowed": False}],
                "automaticDeRisk": {"action": "REDUCE", "capitalMultiplier": 0.5},
                "stressLibrary": {"passed": False},
                "decisionQuality": {"score": 0.8},
            },
        }
        checks = {"strategies": [], "portfolios": [portfolio]}
        common = {"broker_id": "kis", "broker_available": True, "portfolio_equity": 1_000_000, "expected_alpha_bps": 20, "expected_cost_bps": 5}
        increase = state.OrderIntent(strategy_id="STRAT-1", asset="kr-stock", symbol="069500.KS", side="BUY", quantity=2, reference_price=40_000, mode="SMALL_LIVE", reason="unit", metadata={**common, "current_weight": 0.0})
        reduce = state.OrderIntent(strategy_id="STRAT-1", asset="kr-stock", symbol="069500.KS", side="SELL", quantity=1, reference_price=40_000, mode="SMALL_LIVE", reason="unit", metadata={**common, "current_weight": 0.4})

        increase_gate = state.portfolio_gate_for_intent(checks, increase)
        reduce_gate = state.portfolio_gate_for_intent(checks, reduce)
        replay = state.policy_replay_for_intent(checks, reduce, {"policyVersion": "alt", "deadbandWeight": 0.5})

        self.assertFalse(increase_gate["allowed"])
        self.assertIn("order-capacity-exceeded", increase_gate["advancedOperationBlockers"])
        self.assertTrue(reduce_gate["allowed"])
        self.assertEqual(reduce_gate["targetWeight"], 0.2)
        self.assertTrue(replay["sourceEventsImmutable"])

    def test_portfolio_paper_evidence_blocks_live_order_when_missing(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "STRAT-1",
                    "symbol": "069500.KS",
                    "asset": "kr-stock",
                    "paper_portfolio_evidence_gate": {
                        "required": True,
                        "ready": False,
                        "detail": "Portfolio paper evidence가 없습니다.",
                    },
                }
            ],
            "portfolios": [
                {
                    "id": "portfolio-1",
                    "name": "Portfolio 1",
                    "lifecycle_status": "before-live-small",
                    "permissions": {"live_small_allowed": True, "live_allowed": False},
                    "strategy_instances": [{"strategyId": "STRAT-1", "symbol": "069500.KS"}],
                    "target_portfolio": [{"strategyId": "STRAT-1", "symbol": "069500.KS", "targetWeight": 0.25}],
                    "risk_policy": {"maxSingleSymbolWeight": 1.0, "maxStrategyWeight": 1.0},
                    "risk_checks": [],
                }
            ],
        }
        intent = state.OrderIntent(
            strategy_id="STRAT-1",
            asset="kr-stock",
            symbol="069500.KS",
            side="BUY",
            quantity=1,
            reference_price=38900,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={"broker_id": "kis"},
        )

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(checks, "BUY", True, intent)

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("Portfolio Paper Evidence", reason)
        self.assertTrue(any(check.label == "Portfolio Paper Evidence" and check.status == "fail" for check in report.checks))

    def test_order_gate_holds_non_dry_run_until_adapter_is_verified(self) -> None:
        state.STATE["new_entries_blocked"] = False
        checks = {"summary": {"blocker_count": 0}}
        sell_intent = state.default_order_intent(checks, "SELL")
        state.STATE["broker_reconciliation"]["positions"] = [{
            "broker_id": str(sell_intent.metadata.get("broker_id") or ""),
            "symbol": sell_intent.symbol,
            "quantity": sell_intent.quantity,
        }]

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            checks,
            "SELL",
            dry_run=False,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "adapter_blocked")
        self.assertEqual(queue_state, "held")
        self.assertIn("실제 주문 어댑터 안전 검증 전", reason)

    def test_order_gate_uses_pre_trade_risk_engine_for_daily_loss(self) -> None:
        state.STATE["new_entries_blocked"] = False
        state.STATE["risk_settings"]["daily_loss_limit_pct"] = -2.0

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertTrue(ok)
        self.assertEqual(order_state, "dry_run")
        self.assertTrue(report.can_submit)

        state.STATE["risk_settings"]["daily_loss_limit_pct"] = 0.5
        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            {"summary": {"blocker_count": 0}},
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("일일 손실 한도", reason)
        self.assertFalse(report.can_submit)

    def test_kill_switch_forces_monitor_mode_and_blocks_new_entries(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False

        with (
            patch.object(
                state,
                "refresh_kis_order_truth_for_kill_switch",
                return_value={
                    "ok": True,
                    "truth": {
                        "complete": True,
                        "fresh": True,
                        "absenceIsAuthoritative": True,
                        "lastError": "",
                        "workingOrders": [],
                        "ambiguousBrokerOrderIds": [],
                    },
                },
            ),
            patch.object(
                state,
                "run_reconciliation",
                return_value={"ok": True},
            ),
        ):
            result = state.set_flag("kill_switch", True)

        self.assertTrue(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertTrue(state.STATE["kill_switch"])

    def test_risky_safety_release_requires_explicit_confirmation(self) -> None:
        scenarios = [
            ("kill_switch", True),
            ("new_entries_blocked", True),
            ("dry_run", True),
        ]
        for name, initial in scenarios:
            with self.subTest(name=name):
                state.STATE[name] = initial
                rejected = state.set_flag(name, False)
                self.assertFalse(rejected["ok"])
                self.assertTrue(state.STATE[name])

                action = {
                    "kill_switch": "KILL_SWITCH_OFF",
                    "new_entries_blocked": "NEW_ENTRIES_BLOCKED_OFF",
                    "dry_run": "DRY_RUN_OFF",
                }[name]
                issued = state.issue_safety_confirmation(
                    action,
                    {"name": name, "value": False},
                )
                accepted = state.set_flag(
                    name,
                    False,
                    confirmed=True,
                    safety_confirmation={
                        "challengeId": issued["challengeId"],
                        "token": issued["token"],
                        "typedPhrase": issued["expectedPhrase"],
                    },
                )
                self.assertTrue(accepted["ok"])
                self.assertFalse(state.STATE[name])

    def test_full_live_mode_is_blocked_when_warnings_remain(self) -> None:
        state.STATE["mode"] = "MONITOR"
        snapshot = {"summary": {"blocker_count": 0, "warning_count": 1}}

        with patch("live_trader.state.snapshot", return_value=snapshot):
            result = state.set_mode("FULL_LIVE")

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertIn("경고 0개", result["reason"])

    def test_automation_provider_validation_keeps_stock_route_on_kis(self) -> None:
        result = state.set_automation_profile("stock", True, provider="binance", mode="MONITOR")

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["automation"]["stock"]["provider"], "kis")
        self.assertIn("kis만 허용", result["reason"])

    def test_strategy_lifecycle_control_resumes_non_live_stage_without_live_permission(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            artifact_path = artifact_dir / "strategy.json"
            artifact_payload = seal_strategy_artifact({
                "strategy_id": "LIFE-1",
                "name": "Lifecycle Test",
                "symbol": "BTCUSDT",
                "asset": "crypto",
                "timeframe": "1h",
                "plugin": "moving_average_cross",
                "parameters": {"shortMa": 20, "longMa": 60},
                "status": "shadowed",
                "lifecycleStatus": "shadowed",
                "promotionStage": "shadowed",
                "lifecycle": {"status": "shadowed", "history": []},
                "promotion": {"stage": "shadowed", "history": []},
                "permissions": {
                    "paper_trader_verified": True,
                    "live_small_eligible": False,
                    "live_eligible": False,
                    "live_allowed": False,
                    "fail_reasons": [],
                },
                "capabilities": {
                    "liveSmallEligible": True,
                    "liveEligible": False,
                    "canSubmitOrder": False,
                    "failReasons": [],
                    "blockingFailReasons": [],
                },
            })
            artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")
            immutable_source = artifact_path.read_bytes()
            try:
                pause = state.set_strategy_lifecycle_status("LIFE-1", "pause")
                registry_path = artifact_dir / "deployments" / "deployment-registry.json"
                paused_payload = next(iter(json.loads(registry_path.read_text(encoding="utf-8"))["entries"].values()))
                resume = state.set_strategy_lifecycle_status("LIFE-1", "resume")
                resumed_payload = next(iter(json.loads(registry_path.read_text(encoding="utf-8"))["entries"].values()))
                retire = state.set_strategy_lifecycle_status("LIFE-1", "retire")
                retired_payload = next(iter(json.loads(registry_path.read_text(encoding="utf-8"))["entries"].values()))
                immutable_after = artifact_path.read_bytes()
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact_dir

        self.assertTrue(pause["ok"])
        self.assertEqual(immutable_source, immutable_after)
        self.assertEqual(paused_payload["lifecycle"], "paused")
        self.assertFalse(paused_payload["permissions"]["live_small_eligible"])
        self.assertEqual(paused_payload["permissions"]["pausedFrom"], "shadowed")
        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual(resumed_payload["lifecycle"], "shadowed")
        self.assertFalse(resumed_payload["permissions"]["live_small_eligible"])
        self.assertTrue(retire["ok"])
        self.assertEqual(retired_payload["lifecycle"], "retired")
        self.assertFalse(retired_payload["permissions"]["live_small_eligible"])

    def test_legacy_before_live_small_resume_without_forward_evidence_downgrades_to_papered(self) -> None:
        artifact = self.resume_artifact("RESUME-MISSING")

        pause, resume, deployment = self.run_resume_scenario(artifact)

        self.assertTrue(pause["ok"])
        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["paper_trader_verified"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertFalse(deployment["permissions"]["live_eligible"])
        self.assertFalse(deployment["permissions"]["live_allowed"])
        self.assertIn("Paper Trader", resume["reason"])
        self.assertIn(
            "paper-live-deployment-pin-missing",
            deployment["permissions"]["resumeEvidence"]["blockers"][0],
        )

    def test_artifact_pins_cannot_be_laundered_through_pause_and_resume(self) -> None:
        artifact = self.resume_artifact("RESUME-ARTIFACT-PINS")

        pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
            persist_live_deployment_permissions=False,
        )

        self.assertTrue(pause["ok"])
        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "paper-live-deployment-pin-missing",
            deployment["permissions"]["resumeEvidence"]["blockers"][0],
        )
        for key in (
            "paperEvidenceId",
            "paperEvidenceHash",
            "paperEvidenceBundleHash",
            "paperFinalBindingHash",
            "paperGovernanceDeploymentId",
            "paperStrategyInstanceId",
        ):
            self.assertNotIn(key, deployment["permissions"])

    def test_before_live_small_resume_rejects_stale_revalidation(self) -> None:
        artifact = self.resume_artifact(
            "RESUME-STALE",
            validated_until="2020-01-01T00:00:00+00:00",
        )

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "current-revalidation-expired",
            deployment["permissions"]["resumeEvidence"]["blockers"],
        )

    def test_before_live_small_resume_rejects_evidence_older_than_current_revalidation(self) -> None:
        artifact = self.resume_artifact("RESUME-OLD-EVIDENCE")

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
            evidence_options={"ended_at": "2026-07-24T23:59:00+00:00"},
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "paper-evidence-stale-before-current-revalidation",
            deployment["permissions"]["resumeEvidence"]["blockers"],
        )

    def test_before_live_small_resume_rejects_artifact_hash_mismatch(self) -> None:
        artifact = self.resume_artifact(
            "RESUME-HASH",
            artifact_hash="current-artifact-hash",
        )
        stale_artifact = self.resume_artifact(
            "RESUME-HASH",
            artifact_hash="stale-artifact-hash",
        )

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=stale_artifact,
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "paper-live-expected-strategy-artifact-hash-mismatch",
            deployment["permissions"]["resumeEvidence"]["blockers"],
        )

    def test_before_live_small_resume_requires_thirty_full_elapsed_days(self) -> None:
        artifact = self.resume_artifact("RESUME-SHORT-DURATION")

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
            evidence_options={
                "observed_days": 30,
                "observed_seconds": 29 * 24 * 60 * 60,
            },
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "paper-artifact-observation-window-too-short",
            deployment["permissions"]["resumeEvidence"]["blockers"],
        )

    def test_before_live_small_resume_restores_only_live_small_after_current_evidence(self) -> None:
        artifact = self.resume_artifact("RESUME-CURRENT")

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("before-live-small", deployment["lifecycle"], deployment)
        self.assertTrue(deployment["permissions"]["resumeEvidence"]["ready"])
        self.assertTrue(deployment["permissions"]["paper_trader_verified"])
        self.assertTrue(deployment["permissions"]["live_small_eligible"])
        self.assertFalse(deployment["permissions"]["live_eligible"])
        self.assertFalse(deployment["permissions"]["live_allowed"])

    def test_before_live_small_resume_rejects_legacy_v1_forward_policy(self) -> None:
        artifact = self.resume_artifact("RESUME-LEGACY-POLICY")

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
            evidence_options={
                "promotion_source": "continuous-live-forward-closed-bar-v1",
            },
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("papered", deployment["lifecycle"])
        self.assertFalse(deployment["permissions"]["live_small_eligible"])
        self.assertIn(
            "paper-live-forward-source-missing",
            deployment["permissions"]["resumeEvidence"]["blockers"],
        )

    def test_automatic_sweep_never_promotes_full_live_without_operator(self) -> None:
        decision = state.AutomaticPromotionDecision(
            action="PROMOTE",
            current_stage="before-live-small",
            target_stage="live",
            passed=True,
            blockers=(),
            checks=(),
            evidence={"contentHash": "promotion-evidence-hash"},
        )
        strategy = {
            "strategy_id": "AUTO-PROMOTE-BLOCKED",
            "lifecycle_status": "before-live-small",
            "broker_id": "binance",
            "asset": "CRYPTO",
            "symbol": "BTCUSDT",
        }
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(
            state,
            "APP_DATA_ROOT",
            Path(temp_dir),
        ), patch.object(
            state,
            "strategy_rows",
            return_value=[strategy],
        ), patch.object(
            state,
            "reconciliation_snapshot",
            return_value={},
        ), patch.object(
            state,
            "broker_position_truth_snapshot",
            return_value={"mismatchCount": 0},
        ), patch.object(
            state,
            "live_small_execution_summary",
            return_value={"successful": 3, "blocked": 0, "fills": 3},
        ), patch.object(
            state,
            "evaluate_automatic_promotion",
            return_value=decision,
        ), patch.object(
            state,
            "promote_strategy_to_live",
        ) as promote:
            results = state.automatic_live_promotion_sweep(
                fresh_broker_ids={"binance"},
            )

        promote.assert_not_called()
        self.assertEqual(1, len(results))
        self.assertFalse(results[0]["promoted"])
        self.assertEqual(
            "operator-confirmed-manual-promotion-required",
            results[0]["reason"],
        )

    def test_live_resume_never_restores_full_live_permission_directly(self) -> None:
        artifact = self.resume_artifact("RESUME-LIVE", lifecycle="live")

        _pause, resume, deployment = self.run_resume_scenario(
            artifact,
            evidence_artifact=artifact,
        )

        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual("before-live-small", deployment["lifecycle"])
        self.assertTrue(deployment["permissions"]["live_small_eligible"])
        self.assertFalse(deployment["permissions"]["live_eligible"])
        self.assertFalse(deployment["permissions"]["live_allowed"])
        self.assertIn(
            "resume-live-canary-repromotion-required",
            deployment["permissions"]["fail_reasons"],
        )
        self.assertIn("소액 실거래 승급을 다시 요구", resume["reason"])

    def test_live_promotion_requires_three_canary_fills_and_writes_live_evidence(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            artifact_path = artifact_dir / "strategy.json"
            artifact_payload = seal_strategy_artifact({
                "id": "LIVE-PROMOTE-1",
                "strategy_id": "LIVE-PROMOTE-1",
                "name": "Immutable Live Candidate",
                "symbol": "BTCUSDT",
                "asset": "CRYPTO",
                "timeframe": "5m",
                "plugin": "moving_average_cross",
                "finalTest": {"status": "pass"},
                "lifecycle": {"status": "before-live-small"},
                "permissions": {
                    "trader_export_allowed": True,
                    "paper_trader_verified": True,
                    "live_small_eligible": True,
                    "live_eligible": False,
                    "live_allowed": False,
                    "fail_reasons": [],
                },
            })
            artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")
            paper_evidence, paper_pins = exact_evidence_for_artifact(
                artifact_payload,
                evidence_id="paper-live-promote-1",
            )
            state.EvidenceStore(artifact_dir).save_paper(paper_evidence)
            deployment_store = DeploymentStore(artifact_dir)
            definition = deployment_store.create_definition(
                deployment_id="dep:LIVE-PROMOTE-1:standalone:live",
                strategy_artifact=artifact_payload,
                portfolio_artifact=None,
                account_id="live-account-unresolved",
                environment="SMALL_LIVE",
                symbol="BTCUSDT",
                route="crypto",
            )
            deployment_store.transition(
                definition["deploymentId"],
                lifecycle="before-live-small",
                mode="MONITOR",
                actor="unit-test",
                reason="exact Paper final binding pinned",
                permissions={
                    **artifact_payload["permissions"],
                    **paper_pins,
                },
            )
            immutable_source = artifact_path.read_bytes()
            ledger = self.use_temp_program_ledger(temp_dir)
            scope = state.current_live_canary_scope(
                "LIVE-PROMOTE-1",
                materialize=True,
            )
            event_time = (
                datetime.fromisoformat(
                    scope["beforeLiveSmallAt"].replace("Z", "+00:00")
                )
                + timedelta(seconds=1)
            ).isoformat()

            def canary_order(index: int) -> dict:
                return {
                    "strategy_id": "LIVE-PROMOTE-1",
                    "state": "acknowledged",
                    "queue_state": "submitted",
                    "dry_run": False,
                    "mode": "SMALL_LIVE",
                    "broker_id": "binance",
                    "order_id": f"LOCAL-{index}",
                    "broker_order_id": f"BROKER-{index}",
                    "created_at": event_time,
                    "canary_scope": dict(scope),
                }

            def canary_event(index: int) -> dict:
                return {
                    "event_id": f"FILL-{index}",
                    "broker_id": "binance",
                    "order_id": f"LOCAL-{index}",
                    "broker_order_id": f"BROKER-{index}",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "quantity": 0.001,
                    "price": 100_000,
                    "state": "filled",
                    "occurred_at": event_time,
                }

            state.STATE["orders"] = [
                canary_order(1),
                canary_order(2),
            ]
            ledger.record_execution_events(
                [canary_event(1), canary_event(2)]
            )
            readiness = {"operator_confirmed": True, "summary": {"blocker_count": 0}}
            try:
                with patch("live_trader.state.snapshot", return_value=readiness):
                    blocked = state.promote_strategy_to_live("LIVE-PROMOTE-1")
                    state.STATE["orders"].append(canary_order(3))
                    ledger.record_execution_events([canary_event(3)])
                    result = state.promote_strategy_to_live("LIVE-PROMOTE-1")
                self.assertTrue(result["ok"], result["reason"])
                post_promotion_scope = state.current_live_canary_scope(
                    "LIVE-PROMOTE-1"
                )
                post_promotion_summary = state.live_small_execution_summary(
                    "LIVE-PROMOTE-1"
                )
                deployment = next(
                    iter(
                        json.loads(
                            (artifact_dir / "deployments" / "deployment-registry.json").read_text(encoding="utf-8")
                        )["entries"].values()
                    )
                )
                live_files = list((artifact_dir / "evidence" / "live").glob("*.json"))
                evidence = json.loads(live_files[0].read_text(encoding="utf-8"))
                immutable_after = artifact_path.read_bytes()
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact_dir

        self.assertFalse(blocked["ok"])
        self.assertIn("2건", blocked["reason"])
        self.assertIn("3건", blocked["reason"])
        self.assertTrue(result["ok"], result["reason"])
        self.assertEqual(immutable_source, immutable_after)
        self.assertEqual("live", deployment["lifecycle"])
        self.assertTrue(deployment["permissions"]["live_allowed"])
        self.assertEqual("live-execution", evidence["evidenceType"])
        self.assertEqual("PASS", evidence["result"])
        self.assertEqual(3, evidence["successfulOrders"])
        self.assertEqual(
            scope["scopeId"],
            evidence["details"]["canaryScope"]["scopeId"],
        )
        self.assertNotEqual(scope["scopeId"], post_promotion_scope["scopeId"])
        self.assertEqual(0, post_promotion_summary["fills"])
        self.assertTrue(evidence["integrity"]["contentHash"])

    def test_local_filled_rows_without_broker_events_never_count_as_canary(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            payload = {
                "id": "LOCAL-FILL-NOT-CANARY",
                "strategy_id": "LOCAL-FILL-NOT-CANARY",
                "name": "Local Fill",
                "symbol": "BTCUSDT",
                "asset": "CRYPTO",
                "timeframe": "5m",
                "plugin": "moving_average_cross",
                "lifecycle": {"status": "before-live-small"},
                "permissions": {"live_small_eligible": True},
            }
            (artifact_dir / "strategy.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            self.use_temp_program_ledger(temp_dir)
            scope = state.current_live_canary_scope(
                "LOCAL-FILL-NOT-CANARY",
                materialize=True,
            )
            state.STATE["orders"] = [
                {
                    "strategy_id": "LOCAL-FILL-NOT-CANARY",
                    "state": "filled",
                    "queue_state": "filled",
                    "dry_run": False,
                    "mode": "SMALL_LIVE",
                    "broker_id": "binance",
                    "order_id": f"LOCAL-{index}",
                    "broker_order_id": f"BROKER-{index}",
                    "created_at": scope["beforeLiveSmallAt"],
                    "canary_scope": dict(scope),
                }
                for index in range(3)
            ]
            try:
                summary = state.live_small_execution_summary(
                    "LOCAL-FILL-NOT-CANARY"
                )
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact_dir

        self.assertEqual(0, summary["fills"])

    def test_pause_resume_revision_invalidates_prior_canary_fills(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            payload = seal_strategy_artifact({
                "id": "CANARY-RESUME",
                "strategy_id": "CANARY-RESUME",
                "name": "Canary Resume",
                "symbol": "BTCUSDT",
                "asset": "CRYPTO",
                "timeframe": "5m",
                "plugin": "moving_average_cross",
                "lifecycle": {"status": "before-live-small"},
                "permissions": {"live_small_eligible": True},
            })
            (artifact_dir / "strategy.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            paper_evidence, paper_pins = exact_evidence_for_artifact(
                payload,
                evidence_id="paper-canary-resume",
            )
            state.EvidenceStore(artifact_dir).save_paper(paper_evidence)
            store = DeploymentStore(artifact_dir)
            definition = store.create_definition(
                deployment_id="dep:CANARY-RESUME:standalone:live",
                strategy_artifact=payload,
                portfolio_artifact=None,
                account_id="live-account-unresolved",
                environment="SMALL_LIVE",
                symbol="BTCUSDT",
                route="crypto",
            )
            store.transition(
                definition["deploymentId"],
                lifecycle="before-live-small",
                mode="MONITOR",
                permissions={
                    **payload["permissions"],
                    **paper_pins,
                },
                actor="unit",
                reason="exact canary binding",
            )
            ledger = self.use_temp_program_ledger(temp_dir)
            first_scope = state.current_live_canary_scope(
                "CANARY-RESUME",
            )
            event_time = (
                datetime.fromisoformat(
                    first_scope["beforeLiveSmallAt"].replace("Z", "+00:00")
                )
                + timedelta(seconds=1)
            ).isoformat()
            state.STATE["orders"] = []
            events = []
            for index in range(3):
                state.STATE["orders"].append(
                    {
                        "strategy_id": "CANARY-RESUME",
                        "state": "acknowledged",
                        "queue_state": "submitted",
                        "dry_run": False,
                        "mode": "SMALL_LIVE",
                        "broker_id": "binance",
                        "order_id": f"LOCAL-{index}",
                        "broker_order_id": f"BROKER-{index}",
                        "created_at": event_time,
                        "canary_scope": dict(first_scope),
                    }
                )
                events.append(
                    {
                        "event_id": f"FILL-{index}",
                        "broker_id": "binance",
                        "order_id": f"LOCAL-{index}",
                        "broker_order_id": f"BROKER-{index}",
                        "symbol": "BTCUSDT",
                        "quantity": 0.001,
                        "price": 100_000,
                        "state": "filled",
                        "occurred_at": event_time,
                    }
                )
            ledger.record_execution_events(events)
            before = state.live_small_execution_summary("CANARY-RESUME")
            current = store.list()[0]
            permissions = dict(current.get("permissions") or {})
            store.transition(
                current["deploymentId"],
                lifecycle="paused",
                mode="MONITOR",
                permissions=permissions,
                actor="unit",
                reason="pause",
            )
            resumed = store.transition(
                current["deploymentId"],
                lifecycle="before-live-small",
                mode="MONITOR",
                permissions=permissions,
                actor="unit",
                reason="resume",
            )
            after = state.live_small_execution_summary("CANARY-RESUME")
            try:
                self.assertEqual(3, before["fills"])
                self.assertEqual(0, after["fills"])
                self.assertGreater(
                    resumed["revision"],
                    first_scope["deploymentRevision"],
                )
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop("LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None)
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = previous_artifact_dir

    def test_exact_paper_pin_transition_invalidates_prior_canary_fills(self) -> None:
        previous_artifact_dir = os.environ.get(
            "LIVE_TRADER_STRATEGY_ARTIFACT_DIR"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(
                artifact_dir
            )
            payload = seal_strategy_artifact(
                {
                    "id": "CANARY-PIN-CHANGE",
                    "strategy_id": "CANARY-PIN-CHANGE",
                    "name": "Canary Pin Change",
                    "symbol": "BTCUSDT",
                    "asset": "CRYPTO",
                    "timeframe": "5m",
                    "plugin": "moving_average_cross",
                    "lifecycle": {"status": "before-live-small"},
                    "permissions": {"live_small_eligible": True},
                }
            )
            (artifact_dir / "strategy.json").write_text(
                json.dumps(payload),
                encoding="utf-8",
            )
            first_evidence, first_pins = exact_evidence_for_artifact(
                payload,
                evidence_id="paper-canary-first",
            )
            second_evidence, second_pins = exact_evidence_for_artifact(
                payload,
                evidence_id="paper-canary-second",
            )
            evidence_store = state.EvidenceStore(artifact_dir)
            evidence_store.save_paper(first_evidence)
            evidence_store.save_paper(second_evidence)
            store = DeploymentStore(artifact_dir)
            definition = store.create_definition(
                deployment_id="dep:CANARY-PIN-CHANGE:standalone:live",
                strategy_artifact=payload,
                portfolio_artifact=None,
                account_id="live-account-unresolved",
                environment="SMALL_LIVE",
                symbol="BTCUSDT",
                route="crypto",
            )
            first_deployment = store.transition(
                definition["deploymentId"],
                lifecycle="before-live-small",
                mode="MONITOR",
                permissions={**payload["permissions"], **first_pins},
                actor="unit",
                reason="first exact Paper binding",
            )
            ledger = self.use_temp_program_ledger(temp_dir)
            first_scope = state.current_live_canary_scope(
                "CANARY-PIN-CHANGE"
            )
            event_time = (
                datetime.fromisoformat(
                    first_scope["beforeLiveSmallAt"].replace("Z", "+00:00")
                )
                + timedelta(seconds=1)
            ).isoformat()
            state.STATE["orders"] = [
                {
                    "strategy_id": "CANARY-PIN-CHANGE",
                    "state": "acknowledged",
                    "queue_state": "submitted",
                    "dry_run": False,
                    "mode": "SMALL_LIVE",
                    "broker_id": "binance",
                    "order_id": f"LOCAL-PIN-{index}",
                    "broker_order_id": f"BROKER-PIN-{index}",
                    "created_at": event_time,
                    "canary_scope": dict(first_scope),
                }
                for index in range(3)
            ]
            ledger.record_execution_events(
                [
                    {
                        "event_id": f"FILL-PIN-{index}",
                        "broker_id": "binance",
                        "order_id": f"LOCAL-PIN-{index}",
                        "broker_order_id": f"BROKER-PIN-{index}",
                        "symbol": "BTCUSDT",
                        "quantity": 0.001,
                        "price": 100_000,
                        "state": "filled",
                        "occurred_at": event_time,
                    }
                    for index in range(3)
                ]
            )
            before = state.live_small_execution_summary(
                "CANARY-PIN-CHANGE"
            )
            second_deployment = store.transition(
                definition["deploymentId"],
                lifecycle="before-live-small",
                mode="MONITOR",
                permissions={**payload["permissions"], **second_pins},
                actor="unit",
                reason="replace exact Paper binding",
            )
            after = state.live_small_execution_summary(
                "CANARY-PIN-CHANGE"
            )
            try:
                self.assertEqual(3, before["fills"])
                self.assertEqual(0, after["fills"])
                self.assertNotEqual(
                    first_scope["scopeId"],
                    after["scope"]["scopeId"],
                )
                self.assertEqual(
                    second_pins["paperFinalBindingHash"],
                    after["scope"]["paperFinalBindingHash"],
                )
                self.assertGreater(
                    second_deployment["revision"],
                    first_deployment["revision"],
                )
            finally:
                if previous_artifact_dir is None:
                    os.environ.pop(
                        "LIVE_TRADER_STRATEGY_ARTIFACT_DIR", None
                    )
                else:
                    os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = (
                        previous_artifact_dir
                    )

    def test_submit_test_intent_creates_dry_run_order_when_gate_passes(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-OK", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertRegex(
            order["order_id"],
            r"^LIVE-DRY-[0-9A-F]{32}$",
        )
        self.assertEqual(order["state"], "dry_run")
        self.assertEqual(order["queue_state"], "simulated")
        self.assertTrue(order["dry_run"])
        self.assertEqual(order["strategy_id"], "LIVE-OK")
        self.assertIn("risk_report", order)
        self.assertTrue(order["risk_report"]["can_submit"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn(order["order_id"], audit_detail)
        self.assertIn("BTCUSDT BUY dry_run/simulated", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("제출 허용", audit_detail)

    def test_submit_test_intent_persists_common_audit_event(self) -> None:
        original_store = state.AUDIT_STORE
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-AUDIT", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLiteAuditEventStore(Path(temp_dir) / "live_trader_audit.sqlite3")
            state.AUDIT_STORE = store
            try:
                with patch("live_trader.state.snapshot", return_value=fake_snapshot):
                    result = state.submit_test_intent()
            finally:
                state.AUDIT_STORE = original_store

            self.assertTrue(result["ok"])
            rows = store.list_events(newest_first=False)

        self.assertEqual(1, len(rows))
        event = rows[0]
        self.assertEqual("live_trader", event["app"])
        self.assertEqual("ORDER", event["category"])
        self.assertEqual("주문 게이트", event["source"])
        self.assertEqual("allow", event["decision"])
        self.assertEqual("dry_run", event["state"])
        self.assertRegex(
            event["order_id"],
            r"^LIVE-DRY-[0-9A-F]{32}$",
        )
        self.assertEqual("LIVE-AUDIT", event["strategy_id"])
        self.assertEqual("BTCUSDT", event["symbol"])
        self.assertTrue(event["payload"]["risk_report"]["can_submit"])
        self.assertEqual("simulated", event["payload"]["queue_state"])

    def test_submit_test_intent_audit_records_blocking_risk_reason(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = True
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-BLOCKED", "symbol": "BTCUSDT", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertFalse(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["state"], "risk_blocked")
        self.assertFalse(order["risk_report"]["can_submit"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("BTCUSDT BUY risk_blocked/blocked", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("fail", audit_detail)
        self.assertIn("제출 차단", audit_detail)
        self.assertIn("신규 진입 차단", audit_detail)

    def test_submit_test_intent_holds_non_dry_run_order_even_without_blockers(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = False
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0},
            "strategies": [{"strategy_id": "LIVE-OK", "symbol": "069500.KS", "live_allowed": True}],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.submit_test_intent()

        self.assertFalse(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertRegex(
            order["order_id"],
            r"^LIVE-BLOCK-[0-9A-F]{32}$",
        )
        self.assertEqual(order["state"], "adapter_blocked")
        self.assertEqual(order["queue_state"], "held")
        self.assertFalse(order["dry_run"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn(order["order_id"], audit_detail)
        self.assertIn("adapter_blocked/held", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("제출 차단", audit_detail)

    def test_existing_order_retry_requires_new_confirmed_bar_intent(
        self,
    ) -> None:
        original_order = {
            "order_id": "LIVE-BLOCK-RETRY-1",
            "strategy_id": "STRAT-1",
            "asset": "CRYPTO",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "qty": 0.01,
            "reference_price": 100,
            "state": "adapter_blocked",
            "queue_state": "held",
            "attempts": 0,
            "dry_run": False,
        }
        state.STATE["orders"] = [copy.deepcopy(original_order)]
        state.STATE["audit"] = []

        with (
            patch.object(state, "snapshot", return_value={}),
            patch.object(
                state,
                "evaluate_order_gate_with_report",
            ) as evaluate_gate,
            patch.object(
                state,
                "submit_order_intent",
            ) as submit,
            patch.object(
                state.LiveBrokerRouter,
                "place_order",
            ) as place_order,
        ):
            result = state.retry_order(original_order["order_id"])

        self.assertFalse(result["ok"])
        self.assertTrue(
            result["requires_new_confirmed_bar_intent"]
        )
        self.assertIn("다음 확정봉", result["reason"])
        self.assertEqual(
            original_order,
            state.STATE["orders"][0],
        )
        self.assertFalse(
            state.order_rows()[0]["retryable"]
        )
        self.assertEqual(
            "주문 재시도 차단",
            state.STATE["audit"][-1]["event"],
        )
        evaluate_gate.assert_not_called()
        submit.assert_not_called()
        place_order.assert_not_called()

    def test_strategy_cycle_without_signal_records_no_order(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "LIVE-NO-SIGNAL",
                    "name": "No Signal",
                    "symbol": "BTCUSDT",
                    "asset": "코인",
                    "live_allowed": True,
                }
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.run_strategy_cycle("crypto")

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 0)
        self.assertEqual(state.STATE["strategy_runner"]["last_strategy"], "LIVE-NO-SIGNAL")
        self.assertEqual(state.STATE["strategy_runner"]["last_signal"], "-")
        self.assertIn("전략 신호 없음", state.STATE["audit"][-1]["detail"])

    def test_strategy_cycle_signal_routes_through_pre_trade_gate(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "LIVE-RUNNER-BUY",
                    "strategy_instance_id": "standalone:LIVE-RUNNER-BUY",
                    "name": "Runner Buy",
                    "symbol": "BTCUSDT",
                    "asset": "코인",
                    "plugin": "breakout",
                    "live_allowed": True,
                    "signal": "BUY",
                    "reference_price": 65000,
                    "quantity": 0.01,
                }
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.run_strategy_cycle("crypto")

        self.assertTrue(result["ok"])
        self.assertEqual(len(state.STATE["orders"]), 1)
        order = state.STATE["orders"][0]
        self.assertEqual(order["strategy_id"], "LIVE-RUNNER-BUY")
        self.assertEqual(order["state"], "dry_run")
        self.assertEqual(order["runner_report"]["signal"], "BUY")
        self.assertTrue(order["risk_report"]["can_submit"])
        self.assertEqual(result["runner_report"]["intent"]["metadata"]["runner"], "StrategyExecutionRunner")
        self.assertEqual(
            "standalone:LIVE-RUNNER-BUY",
            result["runner_report"]["intent"]["metadata"][
                "strategy_instance_id"
            ],
        )
        self.assertEqual(
            "",
            result["runner_report"]["intent"]["metadata"]["portfolio_id"],
        )
        self.assertEqual(state.STATE["audit"][-1]["event"], "전략 Runner")
        self.assertIn("risk pass", state.STATE["audit"][-1]["detail"])

    def test_portfolio_strategy_cycle_preserves_exact_runtime_context(self) -> None:
        state.STATE["orders"] = []
        state.STATE["audit"] = []
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "strategies": [
                {
                    "strategy_id": "LIVE-PORTFOLIO-BUY",
                    "name": "Portfolio Runner Buy",
                    "symbol": "BTCUSDT",
                    "asset": "코인",
                    "plugin": "breakout",
                    "live_allowed": True,
                    "signal": "BUY",
                    "reference_price": 65_000,
                    "quantity": 0.01,
                    "portfolio_gate": {
                        "active": True,
                        "allowed": True,
                        "portfolioId": "portfolio-live-exact",
                        "targetWeight": 0.2,
                        "policyTargetWeight": 0.2,
                        "maxSymbolWeightPct": 100,
                        "instance": {
                            "instanceId": "portfolio-live-instance"
                        },
                    },
                }
            ],
        }

        executions = state.strategy_executions_for_profile(
            fake_snapshot,
            "crypto",
        )

        self.assertEqual(1, len(executions))
        self.assertIsNotNone(executions[0][1].intent)
        metadata = executions[0][1].intent.metadata
        self.assertEqual(
            "portfolio-live-instance",
            metadata["strategy_instance_id"],
        )
        self.assertEqual("portfolio-live-exact", metadata["portfolio_id"])

    def test_default_order_intent_preserves_exact_runtime_context(self) -> None:
        checks = {
            "strategies": [
                {
                    "strategy_id": "LIVE-MANUAL-PORTFOLIO",
                    "strategy_instance_id": "manual-exact-instance",
                    "symbol": "069500.KS",
                    "asset": "kr-stock",
                    "live_allowed": True,
                    "portfolio_gate": {
                        "active": True,
                        "portfolioId": "manual-exact-portfolio",
                    },
                }
            ]
        }

        intent = state.default_order_intent(checks, "BUY")

        self.assertEqual(
            "manual-exact-instance",
            intent.metadata["strategy_instance_id"],
        )
        self.assertEqual(
            "manual-exact-portfolio",
            intent.metadata["portfolio_id"],
        )

    def test_order_gate_adds_watchdog_blocker_to_risk_report(self) -> None:
        state.STATE["new_entries_blocked"] = False
        checks = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "watchdog": {"critical_count": 1, "next_actions": ["과도 주문 감시"]},
        }

        ok, order_state, queue_state, reason, report = state.evaluate_order_gate_with_report(
            checks,
            "BUY",
            dry_run=True,
        )

        self.assertFalse(ok)
        self.assertEqual(order_state, "risk_blocked")
        self.assertEqual(queue_state, "blocked")
        self.assertIn("Watchdog", reason)
        self.assertTrue(any(check.label == "Watchdog" and check.status == "fail" for check in report.blockers))

    def test_watchdog_fail_closed_blocks_entries_and_monitor_mode(self) -> None:
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["automation"]["crypto"]["enabled"] = True
        state.STATE["automation"]["crypto"]["mode"] = "SMALL_LIVE"
        state.STATE["watchdog"]["settings"]["max_recent_orders_per_min"] = 1.0
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.STATE["orders"] = [
            {
                "order_id": f"LIVE-DRY-{index:04d}",
                "created_at": now,
                "time": now,
                "state": "dry_run",
                "queue_state": "simulated",
                "attempts": 1,
            }
            for index in range(1, 4)
        ]
        state.STATE["audit"] = []

        result = state.run_watchdog(include_snapshot=False)

        self.assertFalse(result["ok"])
        self.assertEqual(state.STATE["mode"], "MONITOR")
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertFalse(state.STATE["automation"]["crypto"]["enabled"])
        self.assertEqual(state.STATE["automation"]["crypto"]["mode"], "MONITOR")
        self.assertEqual(state.STATE["watchdog"]["trip_count"], 1)
        self.assertEqual(state.STATE["audit"][-1]["event"], "Watchdog Fail Closed")

    def test_watchdog_accepts_fresh_continuous_runtime_and_private_stream(self) -> None:
        now = datetime.now()
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["automation"]["crypto"].update(
            {"enabled": True, "provider": "binance", "mode": "SMALL_LIVE"}
        )
        state.STATE["automation"]["stock"].update({"enabled": False, "mode": "MONITOR"})
        state.STATE["watchdog"]["last_run"] = now.strftime("%Y-%m-%d %H:%M:%S")
        state.STATE["strategy_runner"]["last_run"] = ""
        state.STATE["execution_events"]["last_poll"] = ""
        brokers = [{"broker_id": "binance", "order_ready": True}]
        reconciliation = {
            "status": "pass",
            "status_label": "정상",
            "last_run": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        queue = {"retryable": 0, "blocked": 0}
        continuous = {
            "profiles": {
                "crypto": {
                    "running": True,
                    "phase": "RUNNING",
                    "lastHeartbeat": now.isoformat(),
                    "lastDataAt": now.isoformat(),
                    "engine": {
                        "latestBars": {
                            "strategy-1": {
                                "endTime": now.isoformat(),
                                "receivedTime": now.isoformat(),
                                "timeframe": "1h",
                            }
                        }
                    },
                }
            }
        }
        streams = {
            "brokers": {
                "binance": {
                    "running": True,
                    "connected": True,
                }
            }
        }

        with patch.object(state.LIVE_CONTINUOUS_CONTROLLER, "snapshot", return_value=continuous), patch.object(
            state.LIVE_EXECUTION_STREAMS,
            "snapshot",
            return_value=streams,
        ):
            report = state.watchdog_snapshot(brokers, reconciliation, queue, now=now)

        self.assertEqual(0, report["critical_count"], report["checks"])
        checks = {item["label"]: item for item in report["checks"]}
        self.assertEqual("pass", checks["시장 데이터 신선도"]["status"])
        self.assertEqual("pass", checks["체결 이벤트 동기화"]["status"])
        self.assertEqual(["binance"], report["active_brokers"])

    def test_functional_daily_runtime_guard_does_not_self_stop_over_sixty_seconds(self) -> None:
        base = datetime(2026, 8, 5, 9, 0, 0)
        state.STATE["mode"] = "SMALL_LIVE"
        state.STATE["new_entries_blocked"] = False
        state.STATE["orders"] = []
        state.STATE["automation"]["stock"].update(
            {"enabled": True, "provider": "kis", "mode": "SMALL_LIVE"}
        )
        state.STATE["automation"]["crypto"].update(
            {"enabled": False, "mode": "MONITOR"}
        )
        continuous = {
            "profiles": {
                "stock": {
                    "running": True,
                    "phase": "RUNNING",
                    "lastHeartbeat": base.isoformat(),
                    "lastDataAt": "",
                    "engine": {
                        "latestBars": {
                            "daily-strategy": {
                                "endTime": (
                                    base - timedelta(days=1)
                                ).isoformat(),
                                "receivedTime": base.isoformat(),
                                "timeframe": "1d",
                            }
                        }
                    },
                }
            }
        }
        streams = {
            "brokers": {
                "kis": {
                    "running": True,
                    "connected": False,
                    "lastError": "",
                }
            }
        }
        brokers = [{"broker_id": "kis", "order_ready": True}]
        reconciliation = {
            "status": "pass",
            "status_label": "정상",
            "last_run": base.strftime("%Y-%m-%d %H:%M:%S"),
        }
        queue = {"retryable": 0, "blocked": 0}
        reports = []
        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "snapshot",
                return_value=continuous,
            ),
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
            ) as transition,
            patch.object(
                state.LIVE_EXECUTION_STREAMS,
                "snapshot",
                return_value=streams,
            ),
        ):
            for elapsed in (20, 40, 60):
                now = base + timedelta(seconds=elapsed)
                state.STATE["watchdog"]["last_run"] = now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                # The functional runtime guard completes this read-only KIS
                # poll every 20 seconds even when no private fill arrives.
                state.STATE["execution_events"]["last_poll"] = now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                state.STATE["execution_events"]["errors"] = []
                report = state.watchdog_snapshot(
                    brokers,
                    reconciliation,
                    queue,
                    now=now,
                )
                reports.append(report)
                self.assertFalse(state.apply_watchdog_fail_closed(report))

        self.assertTrue(
            all(item["critical_count"] == 0 for item in reports),
            reports,
        )
        self.assertEqual("SMALL_LIVE", state.STATE["mode"])
        self.assertFalse(state.STATE["new_entries_blocked"])
        transition.assert_not_called()

    def test_automation_start_is_blocked_by_watchdog_critical(self) -> None:
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0},
            "watchdog": {"critical_count": 1},
            "automation_profiles": [
                {"id": "crypto", "title": "코인 자동화", "live_strategy_count": 1}
            ],
        }

        with patch("live_trader.state.snapshot", return_value=fake_snapshot):
            result = state.set_automation_profile("crypto", True, provider="binance", mode="SMALL_LIVE")

        self.assertFalse(result["ok"])
        self.assertIn("Watchdog critical", result["reason"])
        self.assertFalse(state.STATE["automation"]["crypto"]["enabled"])

    def test_reconciliation_refreshes_read_only_broker_snapshots(self) -> None:
        self.use_temp_program_ledger(self.recovery_temp_dir.name)

        class FakeRouter:
            def get_account_snapshot(self, broker_id):
                rows = {
                    "kis": [
                        {
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "account": "KIS 실계좌",
                            "currency": "KRW",
                            "broker_cash": 100000.0,
                            "detail": "fake kis account",
                        }
                    ],
                    "binance": [
                        {
                            "broker_id": "binance",
                            "broker_name": "Binance API",
                            "account": "Binance Spot",
                            "currency": "USDT",
                            "broker_cash": 25.0,
                            "detail": "fake binance account",
                        }
                    ],
                    "binance-futures": [
                        {
                            "broker_id": "binance-futures",
                            "broker_name": "Binance USD-M Futures",
                            "account": "Binance Futures",
                            "currency": "USDT",
                            "broker_cash": 15.0,
                            "detail": "fake binance futures account",
                        }
                    ],
                    "upbit": [
                        {
                            "broker_id": "upbit",
                            "broker_name": "Upbit API",
                            "account": "Upbit KRW",
                            "currency": "KRW",
                            "broker_cash": 50000.0,
                            "detail": "fake upbit account",
                        }
                    ],
                }
                return {"broker_id": broker_id, "accounts": rows[broker_id]}

            def list_positions(self, broker_id):
                rows = {
                    "kis": [
                        {
                            "symbol": "069500.KS",
                            "asset": "한국주식",
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "currency": "KRW",
                            "broker_qty": 0.0,
                            "broker_value": 0.0,
                            "detail": "fake kis position",
                        }
                    ],
                    "binance": [
                        {
                            "symbol": "BTC",
                            "asset": "코인",
                            "broker_id": "binance",
                            "broker_name": "Binance API",
                            "currency": "BTC",
                            "broker_qty": 0.1,
                            "broker_value": 0.0,
                            "detail": "fake binance position",
                        }
                    ],
                    "binance-futures": [],
                    "upbit": [],
                }
                return rows[broker_id]

            def poll_execution_events(self, broker_id):
                return {
                    **self.get_account_snapshot(broker_id),
                    "positions": self.list_positions(broker_id),
                    "events": [],
                    **(
                        {"execution_truth": empty_complete_kis_order_truth()}
                        if broker_id == "kis"
                        else {}
                    ),
                }

        with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()), patch(
            "live_trader.state.automatic_live_promotion_sweep",
            return_value=[],
        ):
            result = state.run_reconciliation()

        self.assertTrue(result["ok"])
        broker_data = state.STATE["broker_reconciliation"]
        self.assertEqual(len(broker_data["accounts"]), 4)
        self.assertEqual(len(broker_data["positions"]), 2)
        self.assertEqual(len(broker_data["errors"]), 0)
        self.assertEqual(
            broker_data["successful_position_brokers"],
            ["kis", "binance", "binance-futures", "upbit"],
        )
        self.assertEqual(result["reconciliation"]["summary"]["error_count"], 0)
        self.assertGreaterEqual(result["reconciliation"]["summary"]["mismatch_count"], 1)
        self.assertTrue(
            any(
                row["broker_id"] == "kis"
                and row["currency"] == "KRW"
                and row["broker_cash"].startswith("100,000")
                and row["broker_cash_value"] == 100000.0
                and row["broker_equity_value"] == 100000.0
                and row["status"] == "api_required"
                for row in result["reconciliation"]["accounts"]
            )
        )
        self.assertTrue(
            any(
                row["broker_id"] == "binance"
                and row["symbol"] == "BTC"
                and row["broker_qty_value"] == 0.1
                and row["broker_value"] == 0.0
                and row["status"] == "mismatch"
                for row in result["reconciliation"]["positions"]
            )
        )

    def test_cached_reconciliation_skips_duplicate_broker_io_and_snapshot(self) -> None:
        with patch(
            "live_trader.state.refresh_broker_reconciliation",
        ) as refresh, patch(
            "live_trader.state.automatic_live_promotion_sweep",
            return_value=[],
        ), patch(
            "live_trader.state.snapshot",
        ) as snapshot_mock:
            result = state.run_reconciliation(
                refresh_brokers=False,
                include_snapshot=False,
            )

        refresh.assert_not_called()
        snapshot_mock.assert_not_called()
        self.assertTrue(result["ok"])
        self.assertNotIn("snapshot", result)

    def test_final_preflight_returns_a_fresh_snapshot(self) -> None:
        preflight_snapshot = {
            "launch_report": {
                "hard_stop_count": 0,
                "warning_count": 0,
                "lock_reason": "ready",
            }
        }
        final_snapshot = {"generated_at": "after-preflight"}
        with patch(
            "live_trader.state.snapshot",
            side_effect=[preflight_snapshot, final_snapshot],
        ), patch(
            "live_trader.state.persist_doctor_diagnostic_snapshot",
            return_value={"latest": {}},
        ), patch(
            "live_trader.state.append_audit",
        ):
            result = state.run_final_preflight()

        self.assertEqual(final_snapshot, result["snapshot"])

    def test_program_ledger_baseline_turns_broker_cash_into_reconciled_cash(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                state.STATE["broker_reconciliation"] = {
                    "fetched_at": "2026-07-04 10:00:00",
                    "accounts": [
                        {
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "account": "KIS 실계좌",
                            "currency": "KRW",
                            "broker_cash": 100000.0,
                            "detail": "fake kis account",
                        }
                    ],
                    "positions": [],
                    "errors": [],
                }

                result = state.seed_program_ledger_from_broker_snapshot(refresh_if_empty=False)
            finally:
                self.restore_temp_program_ledger()

        self.assertTrue(result["ok"])
        kis_account = next(row for row in result["reconciliation"]["accounts"] if row["broker_id"] == "kis")
        self.assertEqual(kis_account["status"], "pass")
        self.assertEqual(kis_account["status_label"], "일치")
        self.assertEqual(kis_account["program_source"], "broker_snapshot")

    def test_execution_event_poll_records_events_in_program_ledger(self) -> None:
        trace_id = "trace_test_live_fill"
        state.STATE["orders"] = [
            {
                "order_id": "LIVE-ORDER-1",
                "broker_order_id": "BRK-1",
                "trace_id": trace_id,
                "strategy_id": "strategy-live-1",
                "state": "acknowledged",
                "queue_state": "submitted",
                "dry_run": False,
            }
        ]
        class FakeRouter:
            def poll_execution_events(self, broker_id):
                return {
                    "broker_id": broker_id,
                    "events": [
                        {
                            "event_id": f"{broker_id}-fill-1",
                            "order_id": "LIVE-ORDER-1",
                            "broker_order_id": "BRK-1",
                            "symbol": "BTCUSDT",
                            "side": "BUY",
                            "quantity": 0.01,
                            "price": 65000.0,
                            "state": "filled",
                            "occurred_at": "2026-07-04 10:01:00",
                        }
                    ],
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
                    result = state.poll_execution_events("binance")
            finally:
                self.restore_temp_program_ledger()

        self.assertTrue(result["ok"])
        self.assertEqual(result["execution_events"]["event_count"], 1)
        self.assertEqual(result["program_ledger"]["execution_event_count"], 1)
        self.assertEqual(result["program_ledger"]["execution_events"][0]["symbol"], "BTCUSDT")
        self.assertEqual(result["program_ledger"]["execution_events"][0]["trace_id"], trace_id)
        self.assertEqual(result["local_order_update_count"], 1)
        self.assertEqual(state.STATE["orders"][0]["state"], "filled")
        self.assertEqual(state.STATE["orders"][0]["queue_state"], "completed")
        self.assertEqual(state.STATE["orders"][0]["filled_quantity"], 0.01)
        self.assertEqual(state.STATE["orders"][0]["average_fill_price"], 65000.0)
        self.assertEqual(state.open_order_count(), 0)
        self.assertEqual(
            [item["eventType"] for item in state.DECISION_TRACE_STORE.trace(trace_id)],
            ["FILLED", "POSITION_APPLIED"],
        )

    def test_upbit_partial_fills_and_duplicate_event_apply_only_the_delta(self) -> None:
        state.STATE["orders"] = [
            {
                "order_id": "UPBIT-CLIENT-1",
                "broker_order_id": "UPBIT-BROKER-1",
                "state": "acknowledged",
                "queue_state": "submitted",
                "dry_run": False,
            }
        ]
        first = parse_upbit_my_order({
            "type": "myOrder",
            "uuid": "UPBIT-BROKER-1",
            "identifier": "UPBIT-CLIENT-1",
            "code": "KRW-BTC",
            "ask_bid": "BID",
            "state": "trade",
            "trade_uuid": "UPBIT-TRADE-1",
            "volume": "0.1",
            "remaining_volume": "0.2",
            "executed_volume": "0.1",
            "price": "100",
            "avg_price": "100",
            "trade_fee": "0.01",
            "paid_fee": "0.01",
        })
        second = parse_upbit_my_order({
            "type": "myOrder",
            "uuid": "UPBIT-BROKER-1",
            "identifier": "UPBIT-CLIENT-1",
            "code": "KRW-BTC",
            "ask_bid": "BID",
            "state": "trade",
            "trade_uuid": "UPBIT-TRADE-2",
            "volume": "0.1",
            "remaining_volume": "0.1",
            "executed_volume": "0.2",
            "price": "200",
            "avg_price": "150",
            "trade_fee": "0.02",
            "paid_fee": "0.03",
        })
        terminal = parse_upbit_my_order({
            "type": "myOrder",
            "uuid": "UPBIT-BROKER-1",
            "identifier": "UPBIT-CLIENT-1",
            "code": "KRW-BTC",
            "ask_bid": "BID",
            "state": "done",
            "executed_volume": "0.2",
            "avg_price": "150",
            "paid_fee": "0.03",
            "trades_count": 2,
        })
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertIsNotNone(terminal)

        class FakeRouter:
            def poll_execution_events(self, broker_id):
                return {
                    "broker_id": broker_id,
                    "accounts": [],
                    "positions": [],
                    "events": [],
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch(
                    "live_trader.state.LiveBrokerRouter",
                    return_value=FakeRouter(),
                ), patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "drain",
                    return_value=[
                        first,
                        second,
                        dict(second),
                        terminal,
                    ],
                ), patch(
                    "live_trader.state.notify_new_live_fills",
                    return_value=0,
                ), patch(
                    "live_trader.state.automatic_live_promotion_sweep",
                    return_value=[],
                ):
                    result = state.poll_execution_events(
                        "upbit",
                        force_snapshot=True,
                    )
            finally:
                self.restore_temp_program_ledger()

        order = state.STATE["orders"][0]
        self.assertTrue(result["ok"])
        self.assertEqual(3, result["execution_events"]["event_count"])
        self.assertEqual(3, result["program_ledger"]["execution_event_count"])
        self.assertEqual(3, result["local_order_update_count"])
        self.assertAlmostEqual(0.2, order["filled_quantity"])
        self.assertAlmostEqual(150.0, order["average_fill_price"])
        self.assertAlmostEqual(0.03, order["fee"])
        self.assertEqual("filled", order["state"])
        self.assertEqual("completed", order["queue_state"])
        ledger_events = result["program_ledger"]["execution_events"]
        self.assertAlmostEqual(
            0.2,
            sum(float(item["quantity"]) for item in ledger_events),
        )
        self.assertAlmostEqual(
            0.03,
            sum(float(item["raw"]["fee"]) for item in ledger_events),
        )
        terminal_event = next(
            item
            for item in ledger_events
            if str(item["state"]).lower() == "filled"
        )
        self.assertEqual(0.0, terminal_event["quantity"])
        self.assertEqual(
            0.2,
            terminal_event["raw"]["reported_cumulative_quantity"],
        )

    def test_binance_execution_contract_remains_incremental(self) -> None:
        quantity, price, fee = state.execution_event_increment(
            {
                "broker_id": "binance",
                "quantity": 0.02,
                "price": 65_000.0,
                "fee": 0.5,
                "raw": {
                    "quantity_mode": "cumulative",
                    "cumulative_quantity": 0.03,
                },
            },
            {
                "filled_quantity": 0.01,
                "average_fill_price": 64_000.0,
                "fee": 0.25,
            },
            None,
        )

        self.assertEqual(0.02, quantity)
        self.assertEqual(65_000.0, price)
        self.assertEqual(0.5, fee)

    def test_upbit_cumulative_snapshot_recovers_only_missing_fill(self) -> None:
        quantity, price, fee = state.execution_event_increment(
            {
                "broker_id": "upbit",
                "quantity": 0.2,
                "price": 150.0,
                "fee": 0.03,
                "raw": {
                    "quantity_mode": "cumulative",
                    "fee_mode": "cumulative",
                    "cumulative_quantity": 0.2,
                    "cumulative_fee": 0.03,
                    "cumulative_average_price": 150.0,
                },
            },
            {
                "filled_quantity": 0.1,
                "average_fill_price": 100.0,
                "fee": 0.01,
            },
            None,
        )

        self.assertAlmostEqual(0.1, quantity)
        self.assertAlmostEqual(200.0, price)
        self.assertAlmostEqual(0.02, fee)

    def test_kis_cumulative_partial_then_final_applies_only_watermark_delta(self) -> None:
        state.STATE["broker_order_truth"] = {"kis": {}}
        state.STATE["orders"] = [
            {
                "order_id": "LIVE-KIS-1",
                "oms_order_id": "",
                "broker_id": "kis",
                "broker_order_id": "KIS-ORDER-1",
                "broker_order_key": "20260809:001:KIS-ORDER-1",
                "order_date": "20260809",
                "organization_no": "001",
                "state": "acknowledged",
                "queue_state": "submitted",
                "filled_quantity": 0.0,
                "average_fill_price": 0.0,
                "fee": 0.0,
            }
        ]
        partial = {
            "event_id": "kis-partial-1",
            "broker_id": "kis",
            "broker_order_id": "KIS-ORDER-1",
            "state": "partially_filled",
            "quantity": 1.0,
            "price": 70_000.0,
            "fee": 0.0,
            "raw": {
                "broker_order_key": "20260809:001:KIS-ORDER-1",
                "order_date": "20260809",
                "organization_no": "001",
                "quantity_mode": "cumulative",
                "cumulative_quantity": 1.0,
                "cumulative_average_price": 70_000.0,
            },
        }
        final = {
            "event_id": "kis-filled-1",
            "broker_id": "kis",
            "broker_order_id": "KIS-ORDER-1",
            "state": "filled",
            "quantity": 3.0,
            "price": 71_000.0,
            "fee": 0.0,
            "raw": {
                "broker_order_key": "20260809:001:KIS-ORDER-1",
                "order_date": "20260809",
                "organization_no": "001",
                "quantity_mode": "cumulative",
                "cumulative_quantity": 3.0,
                "cumulative_average_price": 71_000.0,
            },
        }

        self.assertEqual(1, state.apply_execution_events_to_local_orders([partial]))
        self.assertEqual(1, state.apply_execution_events_to_local_orders([final]))

        order = state.STATE["orders"][0]
        self.assertAlmostEqual(3.0, order["filled_quantity"])
        self.assertAlmostEqual(71_000.0, order["average_fill_price"])
        self.assertAlmostEqual(2.0, final["quantity"])
        self.assertAlmostEqual(71_500.0, final["price"])
        self.assertEqual("incremental", final["applied_quantity_mode"])
        self.assertEqual("filled", order["state"])

    def test_reused_kis_odno_never_matches_legacy_or_different_date_order(self) -> None:
        event = {
            "event_id": "kis-rest:20260809:001:REUSED:fill:1",
            "broker_id": "kis",
            "broker_order_id": "REUSED",
            "state": "filled",
            "quantity": 1.0,
            "price": 70_000.0,
            "fee": 0.0,
            "raw": {
                "broker_order_key": "20260809:001:REUSED",
                "order_date": "20260809",
                "organization_no": "001",
                "quantity_mode": "cumulative",
                "cumulative_quantity": 1.0,
                "cumulative_average_price": 70_000.0,
            },
        }
        legacy = {
            "order_id": "LEGACY-LOCAL",
            "broker_id": "kis",
            "broker_order_id": "REUSED",
            "state": "acknowledged",
            "queue_state": "submitted",
            "filled_quantity": 0.0,
        }
        prior_date = {
            **legacy,
            "order_id": "PRIOR-DATE-LOCAL",
            "broker_order_key": "20260701:001:REUSED",
            "order_date": "20260701",
            "organization_no": "001",
        }
        state.STATE["broker_order_truth"] = {"kis": {}}

        for local_order in (legacy, prior_date):
            with self.subTest(order_id=local_order["order_id"]):
                state.STATE["orders"] = [dict(local_order)]
                candidate = copy.deepcopy(event)
                self.assertEqual(
                    0,
                    state.apply_execution_events_to_local_orders([candidate]),
                )
                self.assertEqual(0.0, state.STATE["orders"][0]["filled_quantity"])
                trace_id, matched = state.execution_event_trace_context(candidate)
                self.assertEqual("", trace_id)
                self.assertIsNone(matched)

    def test_kis_poll_keeps_complete_working_order_truth_and_incomplete_poll_stales_it(self) -> None:
        official_order = {
            "broker_order_id": "KIS-WORKING-1",
            "broker_order_key": "20260809:001:KIS-WORKING-1",
            "order_date": "20260809",
            "order_time": "090001",
            "organization_no": "001",
            "symbol": "005930",
            "state": "accepted",
            "order_quantity": 1.0,
            "filled_quantity": 0.0,
            "remaining_quantity": 1.0,
        }
        complete = {
            "complete": True,
            "pagination_complete": True,
            "query_start_date": "20260809",
            "query_end_date": "20260809",
            "page_count": 1,
            "orders": [official_order],
            "events": [],
            "correlation_policy": "official-broker-order-id-only",
            "absence_is_authoritative": True,
        }

        class CompleteRouter:
            def poll_execution_events(self, _broker_id):
                return {
                    "broker_id": "kis",
                    "accounts": [],
                    "positions": [],
                    "events": [],
                    "orders": [official_order],
                    "execution_truth": complete,
                }

        class FailedRouter:
            def poll_execution_events(self, _broker_id):
                raise RuntimeError("pagination interrupted")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with (
                    patch("live_trader.state.LiveBrokerRouter", return_value=CompleteRouter()),
                    patch.object(state.LIVE_EXECUTION_STREAMS, "drain", return_value=[]),
                    patch("live_trader.state.notify_new_live_fills", return_value=0),
                    patch("live_trader.state.automatic_live_promotion_sweep", return_value=[]),
                ):
                    first = state.poll_execution_events(
                        "kis",
                        force_snapshot=True,
                        include_snapshot=False,
                    )
                self.assertTrue(first["ok"])
                truth = state.kis_order_truth_snapshot()
                self.assertTrue(truth["complete"])
                self.assertTrue(truth["fresh"])
                self.assertEqual(1, truth["workingOrderCount"])
                self.assertEqual(1, truth["unmatchedWorkingOrderCount"])

                with (
                    patch("live_trader.state.LiveBrokerRouter", return_value=FailedRouter()),
                    patch.object(state.LIVE_EXECUTION_STREAMS, "drain", return_value=[]),
                    patch("live_trader.state.notify_new_live_fills", return_value=0),
                ):
                    failed = state.poll_execution_events(
                        "kis",
                        force_snapshot=True,
                        include_snapshot=False,
                    )
            finally:
                self.restore_temp_program_ledger()

        self.assertFalse(failed["ok"])
        stale_truth = state.kis_order_truth_snapshot()
        self.assertFalse(stale_truth["fresh"])
        self.assertEqual(1, stale_truth["workingOrderCount"])
        self.assertIn("pagination interrupted", stale_truth["lastError"])

    def test_official_unmatched_kis_working_order_blocks_functional_mutation(self) -> None:
        state.record_complete_kis_order_truth(
            {
                "complete": True,
                "pagination_complete": True,
                "orders": [
                    {
                        "broker_order_id": "KIS-UNMATCHED-1",
                        "broker_order_key": "20260809:001:KIS-UNMATCHED-1",
                        "order_date": "20260809",
                        "organization_no": "001",
                        "state": "partially_filled",
                        "remaining_quantity": 2.0,
                    }
                ],
                "absence_is_authoritative": True,
            }
        )
        state.STATE["orders"] = []
        state.STATE["active_runtime_session_ids"] = {}
        controller = Mock()
        controller.snapshot.return_value = {"profiles": {"stock": {"running": False, "phase": "STOPPED"}}}
        with (
            patch.object(state, "LIVE_CONTINUOUS_CONTROLLER", controller),
            patch.object(state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]),
        ):
            assessment = state.functional_test_authority_mutation_assessment()

        self.assertFalse(assessment["allowed"])
        self.assertEqual(1, assessment["workingOrderCount"])
        self.assertIn(
            "functional-test-working-orders-unresolved",
            assessment["blockers"],
        )

    def test_incomplete_paginated_kis_truth_is_fail_closed_and_preserves_last_rows(self) -> None:
        prior = {
            "broker_order_id": "KIS-PRIOR-WORKING",
            "broker_order_key": "20260808:001:KIS-PRIOR-WORKING",
            "order_date": "20260808",
            "organization_no": "001",
            "state": "accepted",
            "remaining_quantity": 1.0,
        }
        state.record_complete_kis_order_truth(
            {
                "complete": True,
                "pagination_complete": True,
                "orders": [prior],
                "absence_is_authoritative": True,
            }
        )

        class IncompleteRouter:
            def poll_execution_events(self, _broker_id):
                return {
                    "broker_id": "kis",
                    "accounts": [],
                    "positions": [],
                    "events": [],
                    "execution_truth": {
                        "complete": True,
                        "pagination_complete": False,
                        "orders": [],
                        "absence_is_authoritative": False,
                    },
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with (
                    patch("live_trader.state.LiveBrokerRouter", return_value=IncompleteRouter()),
                    patch.object(state.LIVE_EXECUTION_STREAMS, "drain", return_value=[]),
                    patch("live_trader.state.notify_new_live_fills", return_value=0),
                ):
                    result = state.poll_execution_events(
                        "kis",
                        force_snapshot=True,
                        include_snapshot=False,
                    )
            finally:
                self.restore_temp_program_ledger()

        self.assertFalse(result["ok"])
        truth = state.kis_order_truth_snapshot()
        self.assertFalse(truth["fresh"])
        self.assertEqual(1, truth["workingOrderCount"])
        self.assertEqual("KIS-PRIOR-WORKING", truth["workingOrders"][0]["broker_order_id"])
        self.assertIn("kis-order-truth-incomplete", truth["lastError"])

    def test_official_working_order_wins_over_conflicting_local_completed_state(self) -> None:
        state.STATE["orders"] = [
            {
                "order_id": "LOCAL-COMPLETED",
                "broker_order_id": "KIS-STILL-WORKING",
                "broker_order_key": "20260809:001:KIS-STILL-WORKING",
                "order_date": "20260809",
                "organization_no": "001",
                "execution_purpose": state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
                "state": "filled",
                "queue_state": "completed",
            }
        ]
        state.record_complete_kis_order_truth(
            {
                "complete": True,
                "pagination_complete": True,
                "orders": [
                    {
                        "broker_order_id": "KIS-STILL-WORKING",
                        "broker_order_key": "20260809:001:KIS-STILL-WORKING",
                        "order_date": "20260809",
                        "organization_no": "001",
                        "state": "partially_filled",
                        "remaining_quantity": 1.0,
                    }
                ],
                "absence_is_authoritative": True,
            }
        )
        controller = Mock()
        controller.snapshot.return_value = {
            "profiles": {"stock": {"running": False, "phase": "STOPPED"}}
        }
        with patch.object(
            state,
            "LIVE_CONTINUOUS_CONTROLLER",
            controller,
        ), patch.object(
            state.PROGRAM_LEDGER,
            "order_dispatch_rows",
            return_value=[],
        ):
            assessment = state.functional_test_authority_mutation_assessment()

        self.assertFalse(assessment["allowed"])
        self.assertEqual(1, assessment["workingOrderCount"])
        self.assertEqual(
            1,
            assessment["kisOrderTruth"]["matchedWorkingOrderCount"],
        )

    def test_ambiguous_reused_odno_is_always_unmatched_and_blocks_mutation(self) -> None:
        state.STATE["orders"] = [
            {
                "order_id": "LOCAL-AMBIGUOUS",
                "broker_id": "kis",
                "broker_order_id": "KIS-REUSED",
                "broker_order_key": "20260809:001:KIS-REUSED",
                "order_date": "20260809",
                "organization_no": "001",
                "execution_purpose": state.FUNCTIONAL_TEST_EXECUTION_PURPOSE,
                "state": "acknowledged",
                "queue_state": "submitted",
            }
        ]
        orders = [
            {
                "broker_order_id": "KIS-REUSED",
                "broker_order_key": f"{order_date}:001:KIS-REUSED",
                "order_date": order_date,
                "organization_no": "001",
                "state": "accepted",
                "remaining_quantity": 1.0,
            }
            for order_date in ("20260808", "20260809")
        ]
        state.record_complete_kis_order_truth(
            {
                "complete": True,
                "pagination_complete": True,
                "orders": orders,
                "ambiguous_broker_order_ids": ["KIS-REUSED"],
                "absence_is_authoritative": True,
            }
        )
        truth = state.kis_order_truth_snapshot()
        self.assertEqual(0, truth["matchedWorkingOrderCount"])
        self.assertEqual(2, truth["unmatchedWorkingOrderCount"])
        self.assertEqual(
            0,
            state.apply_execution_events_to_local_orders(
                [
                    {
                        "event_id": "ambiguous-kis-fill",
                        "broker_id": "kis",
                        "broker_order_id": "KIS-REUSED",
                        "state": "filled",
                        "quantity": 1.0,
                        "price": 70_000.0,
                        "raw": {
                            "broker_order_key": "20260809:001:KIS-REUSED",
                            "order_date": "20260809",
                            "organization_no": "001",
                        },
                    }
                ]
            ),
        )

        controller = Mock()
        controller.snapshot.return_value = {
            "profiles": {"stock": {"running": False, "phase": "STOPPED"}}
        }
        with patch.object(
            state,
            "LIVE_CONTINUOUS_CONTROLLER",
            controller,
        ), patch.object(
            state.PROGRAM_LEDGER,
            "order_dispatch_rows",
            return_value=[],
        ):
            assessment = state.functional_test_authority_mutation_assessment()

        self.assertFalse(assessment["allowed"])
        self.assertEqual(2, assessment["workingOrderCount"])
        self.assertIn(
            "functional-test-working-orders-unresolved",
            assessment["blockers"],
        )

    def test_open_order_count_excludes_blocked_rejected_and_completed_orders(self) -> None:
        state.STATE["orders"] = [
            {"state": "acknowledged", "queue_state": "submitted"},
            {"state": "unknown", "queue_state": "reconcile_required"},
            {"state": "partially_filled", "queue_state": "submitted"},
            {"state": "risk_blocked", "queue_state": "blocked"},
            {"state": "adapter_blocked", "queue_state": "held"},
            {"state": "broker_rejected", "queue_state": "failed"},
            {"state": "expired", "queue_state": "failed"},
            {"state": "filled", "queue_state": "completed"},
            {"state": "canceled", "queue_state": "canceled"},
        ]

        self.assertEqual(state.open_order_count(), 3)

    def test_execution_event_poll_observes_snapshot_without_overwriting_program_ledger(self) -> None:
        class FakeRouter:
            def poll_execution_events(self, broker_id):
                return {
                    "broker_id": broker_id,
                    "accounts": [
                        {
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "account": "KIS 실계좌",
                            "currency": "KRW",
                            "broker_cash": 123456.0,
                            "detail": "fake account snapshot",
                        }
                    ],
                    "positions": [
                        {
                            "symbol": "005930.KS",
                            "asset": "한국주식",
                            "broker_id": "kis",
                            "broker_name": "한국투자증권 Open API",
                            "currency": "KRW",
                            "broker_qty": 3.0,
                            "broker_value": 210000.0,
                            "detail": "fake position snapshot",
                        }
                    ],
                    "events": [
                        {
                            "event_id": "kis-account-event-1",
                            "state": "account_snapshot",
                            "occurred_at": "2026-07-04 11:00:00",
                        }
                    ],
                    "execution_truth": empty_complete_kis_order_truth(),
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = self.use_temp_program_ledger(temp_dir)
            try:
                ledger.seed_from_broker_snapshot(
                    [
                        {
                            "broker_id": "kis",
                            "account": "KIS 실계좌",
                            "currency": "KRW",
                            "broker_cash": 100000.0,
                        }
                    ],
                    [
                        {
                            "broker_id": "kis",
                            "symbol": "005930.KS",
                            "asset": "한국주식",
                            "currency": "KRW",
                            "broker_qty": 1.0,
                            "broker_value": 70000.0,
                        }
                    ],
                )
                with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
                    result = state.poll_execution_events("kis")
                ledger_cash = ledger.cash_rows()
                ledger_positions = ledger.position_rows()
                reconciliation = state.reconciliation_snapshot()
            finally:
                self.restore_temp_program_ledger()

        self.assertTrue(result["ok"])
        self.assertEqual(result["program_ledger"]["cash_count"], 1)
        self.assertEqual(result["program_ledger"]["position_count"], 1)
        self.assertEqual(result["execution_events"]["synced_cash_count"], 0)
        self.assertEqual(result["execution_events"]["synced_position_count"], 0)
        self.assertEqual(result["execution_events"]["observed_cash_count"], 1)
        self.assertEqual(result["execution_events"]["observed_position_count"], 1)
        self.assertEqual(result["program_ledger"]["execution_event_count"], 0)
        self.assertEqual(ledger_cash[0]["cash"], 100000.0)
        self.assertEqual(ledger_positions[0]["quantity"], 1.0)
        self.assertEqual(
            state.STATE["broker_reconciliation"]["accounts"][0]["broker_cash"],
            123456.0,
        )
        self.assertEqual(
            state.STATE["broker_reconciliation"]["positions"][0]["broker_qty"],
            3.0,
        )
        self.assertTrue(
            any(
                row["broker_id"] == "kis"
                and row["status"] == "mismatch"
                and row["program_cash"].startswith("100,000")
                and row["broker_cash"].startswith("123,456")
                for row in reconciliation["accounts"]
            )
        )
        self.assertTrue(
            any(
                row["broker_id"] == "kis"
                and row["symbol"] == "005930.KS"
                and row["status"] == "mismatch"
                and row["program_qty"] == "1"
                and row["broker_qty"] == "3"
                for row in reconciliation["positions"]
            )
        )

    def test_idle_snapshot_poll_is_throttled_and_does_not_append_execution_events(self) -> None:
        class FakeRouter:
            def __init__(self):
                self.calls = []

            def poll_execution_events(self, broker_id):
                self.calls.append(broker_id)
                currency = "KRW" if broker_id in {"kis", "upbit"} else "USDT"
                return {
                    "broker_id": broker_id,
                    "accounts": [
                        {
                            "broker_id": broker_id,
                            "account": f"{broker_id}-account",
                            "currency": currency,
                            "broker_cash": 100.0,
                        }
                    ],
                    "positions": [],
                    "events": [
                        {
                            "event_id": f"{broker_id}:account:volatile",
                            "state": "account_snapshot",
                            "occurred_at": "2026-07-25 10:00:00",
                        }
                    ],
                    **(
                        {"execution_truth": empty_complete_kis_order_truth()}
                        if broker_id == "kis"
                        else {}
                    ),
                }

        fake_router = FakeRouter()
        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch("live_trader.state.LiveBrokerRouter", return_value=fake_router), patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "drain",
                    return_value=[],
                ), patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "snapshot",
                    return_value={"running": False, "brokers": {}},
                ), patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "snapshot",
                    return_value={"running": False, "profiles": {}},
                ), patch(
                    "live_trader.state.time.monotonic",
                    side_effect=[100.0, 110.0],
                ), patch(
                    "live_trader.state.append_audit",
                ), patch(
                    "live_trader.state.snapshot",
                    return_value={},
                ):
                    first = state.poll_execution_events("all", force_snapshot=False)
                    second = state.poll_execution_events("all", force_snapshot=False)
            finally:
                self.restore_temp_program_ledger()

        self.assertEqual(
            ["kis", "binance", "binance-futures", "upbit"],
            fake_router.calls,
        )
        self.assertEqual(0, first["program_ledger"]["execution_event_count"])
        self.assertEqual(0, first["program_ledger"]["cash_count"])
        self.assertEqual(4, first["execution_events"]["observed_cash_count"])
        self.assertEqual(0, second["execution_events"]["synced_cash_count"])
        self.assertEqual(
            ["binance", "binance-futures", "kis", "upbit"],
            second["execution_events"]["snapshot_skipped_brokers"],
        )

    def test_aggregate_broker_poll_runs_provider_reads_concurrently(self) -> None:
        class ConcurrentRouter:
            def __init__(self):
                self.lock = threading.Lock()
                self.release = threading.Event()
                self.active = 0
                self.max_active = 0
                self.calls = []

            def poll_execution_events(self, broker_id):
                with self.lock:
                    self.calls.append(broker_id)
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    if self.active == 4:
                        self.release.set()
                self.release.wait(0.5)
                with self.lock:
                    self.active -= 1
                currency = "KRW" if broker_id in {"kis", "upbit"} else "USDT"
                return {
                    "broker_id": broker_id,
                    "accounts": [
                        {
                            "broker_id": broker_id,
                            "account": f"{broker_id}-account",
                            "currency": currency,
                            "broker_cash": 100.0,
                        }
                    ],
                    "positions": [],
                    "events": [],
                    **(
                        {"execution_truth": empty_complete_kis_order_truth()}
                        if broker_id == "kis"
                        else {}
                    ),
                }

        fake_router = ConcurrentRouter()
        state.STATE["broker_snapshot_poll"] = {
            "brokers": {},
            "last_summary_audit_monotonic": 0.0,
            "last_summary_signature": "",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch("live_trader.state.LiveBrokerRouter", return_value=fake_router), patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "drain",
                    return_value=[],
                ), patch.object(
                    state.LIVE_EXECUTION_STREAMS,
                    "snapshot",
                    return_value={"running": False, "brokers": {}},
                ), patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "snapshot",
                    return_value={"running": False, "profiles": {}},
                ), patch(
                    "live_trader.state.append_audit",
                ):
                    result = state.poll_execution_events(
                        "all",
                        force_snapshot=True,
                        include_snapshot=False,
                    )
            finally:
                self.restore_temp_program_ledger()

        self.assertEqual(
            {"kis", "binance", "binance-futures", "upbit"},
            set(fake_router.calls),
        )
        self.assertGreaterEqual(fake_router.max_active, 2)
        self.assertNotIn("snapshot", result)
        self.assertEqual(4, result["execution_events"]["observed_cash_count"])

    def test_overlapping_broker_poll_returns_cached_state_without_waiting(self) -> None:
        acquired = state.BROKER_POLL_LOCK.acquire(blocking=False)
        self.assertTrue(acquired)
        try:
            result = state.poll_execution_events(
                "all",
                force_snapshot=True,
                include_snapshot=False,
            )
        finally:
            state.BROKER_POLL_LOCK.release()

        self.assertTrue(result["ok"])
        self.assertTrue(result["coalesced"])
        self.assertNotIn("snapshot", result)

    def test_snapshot_redacts_account_and_signed_request_material(self) -> None:
        secret = "must-not-leak"
        profile = {
            "id": "stock",
            "sample_request": {
                "url": f"https://example.test/order?symbol=BTCUSDT&signature={secret}",
                "headers": {
                    "authorization": f"Bearer {secret}",
                    "X-MBX-APIKEY": secret,
                },
                "body": {
                    "CANO": "12345678",
                    "symbol": "BTCUSDT",
                },
                "query": {"signature": secret, "timestamp": 1},
            },
        }
        with patch("live_trader.state.automation_profiles", return_value=[profile]):
            payload = state.snapshot()

        serialized = json.dumps(payload, ensure_ascii=False)
        sample = payload["automation_profiles"][0]["sample_request"]
        self.assertNotIn(secret, serialized)
        self.assertNotIn("12345678", serialized)
        self.assertEqual("***", sample["body"]["CANO"])
        self.assertEqual("***", sample["headers"]["authorization"])
        self.assertEqual("***", sample["headers"]["X-MBX-APIKEY"])
        self.assertEqual("***", sample["query"]["signature"])
        self.assertIn("signature=%2A%2A%2A", sample["url"])

    def test_kis_poll_execution_events_returns_balance_snapshot_events(self) -> None:
        payload = {
            "output1": [
                {
                    "pdno": "005930",
                    "prdt_name": "삼성전자",
                    "hldg_qty": "5",
                    "evlu_amt": "350000",
                }
            ],
            "output2": [
                {
                    "dnca_tot_amt": "1000000",
                }
            ],
        }

        with patch("live_trader.brokers.issue_kis_access_token", return_value="token"), patch(
            "live_trader.brokers.fetch_kis_domestic_order_truth",
            return_value={
                "rt_cd": "0",
                "output1": [],
                "query_start_date": "20260801",
                "query_end_date": "20260807",
                "page_count": 1,
                "pagination_complete": True,
            },
        ), patch(
            "live_trader.brokers.fetch_kis_domestic_balance",
            return_value=payload,
        ), patch(
            "live_trader.brokers.fetch_kis_overseas_balance",
            return_value={"rt_cd": "0", "output1": [], "output2": []},
        ):
            result = LiveBrokerRouter().poll_execution_events("kis")

        self.assertEqual(result["broker_id"], "kis")
        self.assertEqual(len(result["accounts"]), 1)
        self.assertEqual(len(result["positions"]), 1)
        self.assertTrue(any(event["state"] == "account_snapshot" for event in result["events"]))
        self.assertTrue(
            any(
                event["state"] == "position_snapshot" and event["symbol"] == "005930.KS"
                for event in result["events"]
            )
        )

    def test_kis_position_snapshot_includes_read_only_overseas_balance(self) -> None:
        domestic = {
            "rt_cd": "0",
            "output1": [{"pdno": "005930", "hldg_qty": "1"}],
            "output2": [{"dnca_tot_amt": "100000"}],
        }
        overseas = {
            "rt_cd": "0",
            "output1": [
                {
                    "ovrs_pdno": "SPY",
                    "ovrs_item_name": "SPDR S&P 500 ETF",
                    "ovrs_cblc_qty": "2",
                    "pchs_avg_pric": "500.25",
                    "now_pric2": "510.50",
                    "ovrs_stck_evlu_amt": "1021.00",
                    "tr_crcy_cd": "USD",
                    "ovrs_excg_cd": "NASD",
                }
            ],
            "output2": [],
        }
        with patch("live_trader.brokers.issue_kis_access_token", return_value="token"), patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[
                {"ok": True, "json": domestic},
                {"ok": True, "json": overseas, "trCont": ""},
            ],
        ) as send:
            rows = LiveBrokerRouter().list_positions("kis")

        self.assertEqual(["005930.KS", "SPY"], [row["symbol"] for row in rows])
        spy = rows[1]
        self.assertEqual("미국주식", spy["asset"])
        self.assertEqual("USD", spy["currency"])
        self.assertEqual(2.0, spy["broker_qty"])
        self.assertEqual(1021.0, spy["broker_value"])
        self.assertEqual(
            "/uapi/overseas-stock/v1/trading/inquire-balance",
            send.call_args_list[1].args[0].endpoint,
        )
        self.assertEqual("TTTS3012R", send.call_args_list[1].args[0].headers["tr_id"])

    def test_kis_overseas_balance_consumes_every_continuation_page(self) -> None:
        first = {
            "ok": True,
            "json": {
                "rt_cd": "0",
                "output1": [{"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "1"}],
                "output2": [],
                "ctx_area_fk200": "fk-next",
                "ctx_area_nk200": "nk-next",
            },
            "trCont": "M",
        }
        second = {
            "ok": True,
            "json": {
                "rt_cd": "0",
                "output1": [{"ovrs_pdno": "SPY", "ovrs_cblc_qty": "2"}],
                "output2": [],
            },
            "trCont": "",
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[first, second],
        ) as send:
            payload = fetch_kis_overseas_balance("token")

        rows = parse_kis_overseas_positions(payload)
        self.assertEqual(["AAPL", "SPY"], [row["symbol"] for row in rows])
        second_request = send.call_args_list[1].args[0]
        self.assertEqual("N", second_request.headers["tr_cont"])
        self.assertEqual("fk-next", second_request.query["CTX_AREA_FK200"])
        self.assertEqual("nk-next", second_request.query["CTX_AREA_NK200"])

    def test_kis_overseas_balance_auth_and_api_errors_remain_fail_closed(self) -> None:
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={"ok": False, "statusCode": 401, "text": "unauthorized"},
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "인증 실패"):
                fetch_kis_overseas_balance("expired-token")

        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={
                "ok": True,
                "json": {
                    "rt_cd": "1",
                    "msg_cd": "APBK0919",
                    "msg1": "조회 조건 오류",
                },
            },
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "API 조회 실패"):
                fetch_kis_overseas_balance("token")

        domestic = {
            "rt_cd": "0",
            "output1": [{"pdno": "005930", "hldg_qty": "1"}],
            "output2": [],
        }
        with patch("live_trader.brokers.issue_kis_access_token", return_value="token"), patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[
                {"ok": True, "json": domestic},
                {"ok": False, "statusCode": 403, "text": "forbidden"},
            ],
        ):
            # A valid domestic page must never be published as a complete KIS
            # snapshot when the overseas half failed.
            with self.assertRaisesRegex(BrokerNotReadyError, "인증 실패"):
                LiveBrokerRouter().list_positions("kis")

    def test_binance_poll_execution_events_returns_balance_snapshot_events(self) -> None:
        payload = {
            "balances": [
                {"asset": "USDT", "free": "10.5", "locked": "1.5"},
                {"asset": "BTC", "free": "0.25", "locked": "0.05"},
            ]
        }

        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={"ok": True, "json": payload},
        ):
            result = LiveBrokerRouter().poll_execution_events("binance")

        self.assertEqual(result["broker_id"], "binance")
        self.assertEqual(result["accounts"][0]["broker_cash"], 12.0)
        self.assertEqual(result["positions"][0]["symbol"], "BTC")
        self.assertTrue(any(event["state"] == "account_snapshot" for event in result["events"]))
        self.assertTrue(
            any(
                event["state"] == "position_snapshot" and event["symbol"] == "BTC"
                for event in result["events"]
            )
        )

    def test_binance_pair_position_lookup_maps_base_asset_balance(self) -> None:
        state.STATE["broker_reconciliation"]["positions"] = [
            {"broker_id": "binance", "symbol": "BTC", "broker_qty": 0.00010441},
            {"broker_id": "binance", "symbol": "XRP", "broker_qty": 0.09257},
        ]

        self.assertEqual(0.00010441, state.broker_position_quantity("BTCUSDT"))
        self.assertEqual(0.09257, state.broker_position_quantity("XRPUSDT"))

    def test_position_lookup_is_scoped_to_broker(self) -> None:
        state.STATE["broker_reconciliation"]["positions"] = [
            {"broker_id": "binance", "symbol": "BTC", "broker_qty": 0.01},
            {"broker_id": "upbit", "symbol": "BTC", "broker_qty": 0.02},
        ]

        self.assertEqual(
            0.01,
            state.broker_position_quantity("BTCUSDT", "binance"),
        )
        self.assertEqual(
            0.02,
            state.broker_position_quantity("BTCUSDT", "upbit"),
        )

    def test_upbit_poll_execution_events_returns_balance_snapshot_events(self) -> None:
        payload = [
            {
                "currency": "KRW",
                "balance": "100000",
                "locked": "5000",
            },
            {
                "currency": "BTC",
                "unit_currency": "KRW",
                "balance": "0.01",
                "locked": "0.002",
                "avg_buy_price": "80000000",
            },
        ]

        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={"ok": True, "json": payload},
        ):
            result = LiveBrokerRouter().poll_execution_events("upbit")

        self.assertEqual(result["broker_id"], "upbit")
        self.assertEqual(result["accounts"][0]["broker_cash"], 105000.0)
        self.assertEqual(result["positions"][0]["symbol"], "KRW-BTC")
        self.assertEqual(result["positions"][0]["currency"], "KRW")
        self.assertEqual(result["positions"][0]["broker_value"], 960000.0)
        self.assertTrue(any(event["state"] == "account_snapshot" for event in result["events"]))
        self.assertTrue(
            any(
                event["state"] == "position_snapshot" and event["symbol"] == "KRW-BTC"
                for event in result["events"]
            )
        )

    def test_execution_event_poll_reports_adapter_stub_errors(self) -> None:
        class FakeRouter:
            def poll_execution_events(self, broker_id):
                raise state.BrokerNotReadyError(f"{broker_id} event adapter required")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
                    result = state.poll_execution_events("kis")
            finally:
                self.restore_temp_program_ledger()

        self.assertFalse(result["ok"])
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("event adapter required", result["errors"][0]["detail"])

    def test_recovery_drill_verifies_atomic_checkpoint(self) -> None:
        snapshot_reconciliation = state.reconciliation_snapshot()
        passing_reconciliation = copy.deepcopy(snapshot_reconciliation)
        passing_reconciliation["summary"]["status"] = "pass"
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = state.RecoveryJournal(Path(temp_dir) / "recovery")
            with patch.object(state, "RECOVERY_JOURNAL", journal), patch.object(
                state,
                "reconciliation_snapshot",
                side_effect=[passing_reconciliation, snapshot_reconciliation],
            ):
                result = state.run_recovery_drill()
        self.assertTrue(result["ok"])
        self.assertTrue(result["recovery"]["verified"])
        self.assertFalse(result["recovery"]["safeMode"])

    def test_recovery_drill_stays_blocked_in_monitor_without_broker_reconciliation(self) -> None:
        state.STATE["mode"] = "MONITOR"
        snapshot_reconciliation = state.reconciliation_snapshot()
        warning_reconciliation = copy.deepcopy(snapshot_reconciliation)
        warning_reconciliation["summary"]["status"] = "warn"
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = state.RecoveryJournal(Path(temp_dir) / "recovery")
            with patch.object(state, "RECOVERY_JOURNAL", journal), patch.object(
                state,
                "reconciliation_snapshot",
                side_effect=[warning_reconciliation, snapshot_reconciliation],
            ):
                result = state.run_recovery_drill()

        self.assertFalse(result["ok"])
        self.assertFalse(result["recovery"]["verified"])
        self.assertTrue(result["recovery"]["safeMode"])
        self.assertFalse(result["recovery"]["assurance"]["checks"]["brokerReconciled"])
        self.assertFalse(result["recovery"]["assurance"]["newEntriesAllowed"])

    def test_shadow_live_records_virtual_fill_without_order_or_broker_submission(self) -> None:
        state.STATE["orders"] = []
        before = len(state.STATE["orders"])
        result = state.run_shadow_live({"decision_price": 1000, "virtual_fill_price": 1001, "paper_fill_price": 1000.5})
        self.assertTrue(result["ok"])
        self.assertTrue(result["evidence"]["brokerSubmissionBlocked"])
        self.assertEqual(before, len(state.STATE["orders"]))
        self.assertEqual(1, len(state.STATE["shadow_evidence"]))

    def test_startup_restore_keeps_idempotency_keys_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = state.RecoveryJournal(Path(temp_dir) / "recovery")
            journal.save(
                {
                    "mode": "SMALL_LIVE",
                    "dry_run": False,
                    "orders": [],
                    "order_trace_index": {"BRK-RESTORE-1": {"trace_id": "trace_restore", "strategy_id": "strategy-restore"}},
                    "strategy_runner": {},
                },
                reason="before-restart",
                idempotency_keys=["persisted-key"],
            )
            with patch.object(state, "RECOVERY_JOURNAL", journal):
                restored = state.restore_runtime_from_checkpoint()
        self.assertTrue(restored["verified"])
        self.assertTrue(restored["safeMode"])
        self.assertEqual("MONITOR", state.STATE["mode"])
        self.assertTrue(state.STATE["dry_run"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertIn("persisted-key", state.STATE["persisted_idempotency_keys"])
        self.assertEqual(state.STATE["order_trace_index"]["BRK-RESTORE-1"]["trace_id"], "trace_restore")

    def test_broker_position_truth_fails_closed_on_quantity_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = self.use_temp_program_ledger(temp_dir)
            try:
                ledger.replace_position_rows(
                    [{"broker_id": "binance", "symbol": "BTCUSDT", "asset": "CRYPTO", "currency": "USDT", "broker_qty": 0.02}],
                    "test",
                )
                state.STATE["broker_reconciliation"] = {
                    "fetched_at": "2026-07-04 10:00:00",
                    "positions": [{"broker_id": "binance", "symbol": "BTCUSDT", "broker_qty": 0.01}],
                    "accounts": [],
                    "errors": [],
                }
                report = state.broker_position_truth_snapshot(
                    {"summary": {"api_required_count": 0, "mismatch_count": 0}}
                )
            finally:
                self.restore_temp_program_ledger()

        self.assertFalse(report["matched"])
        self.assertTrue(report["newEntriesBlocked"])
        self.assertEqual(report["mismatchCount"], 1)

    def test_multi_strategy_cycle_nets_opposite_spot_signals_to_one_order(self) -> None:
        state.STATE["dry_run"] = True
        state.STATE["new_entries_blocked"] = False
        state.STATE["orders"] = []
        state.STATE["strategy_sleeves"] = {}
        state.STATE["risk_settings"]["max_symbol_exposure_pct"] = 100.0
        strategies = [
            {
                "strategy_id": "trend", "name": "Trend", "symbol": "BTCUSDT", "asset": "CRYPTO", "plugin": "moving_average_cross",
                "test_signal": "BUY", "reference_price": 1000, "order_quantity": 1, "live_allowed": True,
                "instrument_id": "CRYPTO:BINANCE:BTCUSDT", "market_type": "spot", "allow_short": False,
                "portfolio_gate": {"active": True, "allowed": True, "portfolioId": "portfolio-net", "targetWeight": 0.4, "policyTargetWeight": 0.4, "maxSymbolWeightPct": 100, "instance": {"instanceId": "trend-1"}},
            },
            {
                "strategy_id": "revert", "name": "Revert", "symbol": "BTCUSDT", "asset": "CRYPTO", "plugin": "rsi_reversion",
                "test_signal": "SELL", "reference_price": 1000, "order_quantity": 1, "live_allowed": True,
                "instrument_id": "CRYPTO:BINANCE:BTCUSDT", "market_type": "spot", "allow_short": False,
                "portfolio_gate": {"active": True, "allowed": True, "portfolioId": "portfolio-net", "targetWeight": 0.3, "policyTargetWeight": 0.3, "maxSymbolWeightPct": 100, "instance": {"instanceId": "revert-1"}},
            },
        ]
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0}, "strategies": strategies,
            "portfolios": [{"strategy_instances": [{"strategyId": "trend"}, {"strategyId": "revert"}]}],
            "brokers": [], "reconciliation": {"summary": {"status": "pass"}}, "operational_readiness": {},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(state, "RECOVERY_JOURNAL", state.RecoveryJournal(Path(temp_dir) / "recovery")):
                with patch("live_trader.state.snapshot", return_value=fake_snapshot), patch.object(
                    state,
                    "submit_order_intent",
                    wraps=state.submit_order_intent,
                ) as submit, patch.object(
                    state,
                    "portfolio_gate_for_intent",
                    return_value={"active": False, "allowed": True},
                ):
                    result = state.run_strategy_cycle("crypto")
        self.assertTrue(result["ok"])
        self.assertEqual(2, len(result["runner_reports"]))
        self.assertEqual(1, len(result["plans"]))
        self.assertEqual(1, len(result["orders"]))
        self.assertEqual("BUY", result["plans"][0]["side"])
        self.assertEqual({"trend-1": 0.4, "revert-1": 0.0}, result["plans"][0]["sleeve_targets"])
        self.assertEqual(1, len(state.STATE["orders"]))
        net_intent = submit.call_args.args[1]
        self.assertEqual("portfolio-net", net_intent.metadata["portfolio_id"])
        self.assertEqual("trend-1", net_intent.metadata["strategy_instance_id"])
        self.assertEqual(
            {"trend-1", "revert-1"},
            set(net_intent.metadata["strategy_instance_ids"]),
        )


if __name__ == "__main__":
    unittest.main()
