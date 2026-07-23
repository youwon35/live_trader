import copy
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from live_trader import state
from live_trader.brokers import LiveBrokerRouter
from live_trader.audit_store import SQLiteAuditEventStore
from live_trader.program_ledger import ProgramLedger


class OrderGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.original_recovery_journal = state.RECOVERY_JOURNAL
        self.recovery_temp_dir = tempfile.TemporaryDirectory()
        state.RECOVERY_JOURNAL = state.RecoveryJournal(Path(self.recovery_temp_dir.name) / "recovery-journal")

    def tearDown(self) -> None:
        self.restore_temp_program_ledger()
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))
        state.RECOVERY_JOURNAL = self.original_recovery_journal
        self.recovery_temp_dir.cleanup()

    def use_temp_program_ledger(self, temp_dir: str) -> ProgramLedger:
        self.original_program_ledger = state.PROGRAM_LEDGER
        ledger = ProgramLedger(Path(temp_dir) / "program_ledger.sqlite3")
        state.PROGRAM_LEDGER = ledger
        return ledger

    def restore_temp_program_ledger(self) -> None:
        if hasattr(self, "original_program_ledger"):
            state.PROGRAM_LEDGER = self.original_program_ledger

    def test_order_and_risk_classes_come_from_shared_runtime(self) -> None:
        self.assertEqual(state.OrderIntent.__module__, "trading_runtime.order_management")
        self.assertEqual(state.PreTradeRiskGate.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.PreTradeContext.__module__, "trading_runtime.risk_engine")
        self.assertEqual(state.StrategyExecutionRunner.__module__, "trading_runtime.strategy_runner")

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
            def cancel_order(self, broker_id, broker_order_id, **context):
                self.called = (broker_id, broker_order_id, context)
                return {"ok": True, "json": {"status": "CANCELED"}}

        fake_router = FakeRouter()
        with patch("live_trader.state.LiveBrokerRouter", return_value=fake_router):
            result = state.cancel_order("ord-live-1")

        self.assertTrue(result["ok"])
        self.assertEqual(("binance", "778899"), fake_router.called[:2])
        self.assertEqual("BTCUSDT", fake_router.called[2]["symbol"])
        self.assertEqual("canceled", order["state"])
        self.assertIn("broker_cancel_response", order)

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
        self.assertTrue(state.LIVE_OMS.verify_event_chain(order["oms_order_id"]))
        self.assertFalse(order["idempotency_duplicate"])

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

        ok, order_state, queue_state, reason = state.evaluate_order_gate(
            {"summary": {"blocker_count": 0}},
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

                accepted = state.set_flag(name, False, confirmed=True)
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

    def test_strategy_lifecycle_control_pauses_resumes_and_retires_artifact(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            artifact_path = artifact_dir / "strategy.json"
            artifact_payload = {
                "strategy_id": "LIFE-1",
                "name": "Lifecycle Test",
                "symbol": "BTCUSDT",
                "asset": "crypto",
                "timeframe": "1h",
                "plugin": "moving_average_cross",
                "parameters": {"shortMa": 20, "longMa": 60},
                "status": "before-live-small",
                "lifecycleStatus": "before-live-small",
                "promotionStage": "before-live-small",
                "lifecycle": {"status": "before-live-small", "history": []},
                "promotion": {"stage": "before-live-small", "history": []},
                "permissions": {
                    "paper_trader_verified": True,
                    "live_small_eligible": True,
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
            }
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
        self.assertEqual(paused_payload["permissions"]["pausedFrom"], "before-live-small")
        self.assertTrue(resume["ok"], resume["reason"])
        self.assertEqual(resumed_payload["lifecycle"], "before-live-small")
        self.assertTrue(resumed_payload["permissions"]["live_small_eligible"])
        self.assertTrue(retire["ok"])
        self.assertEqual(retired_payload["lifecycle"], "retired")
        self.assertFalse(retired_payload["permissions"]["live_small_eligible"])

    def test_live_promotion_keeps_strategy_immutable_and_writes_live_evidence(self) -> None:
        previous_artifact_dir = os.environ.get("LIVE_TRADER_STRATEGY_ARTIFACT_DIR")
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact_dir = Path(temp_dir)
            os.environ["LIVE_TRADER_STRATEGY_ARTIFACT_DIR"] = str(artifact_dir)
            artifact_path = artifact_dir / "strategy.json"
            artifact_payload = {
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
            }
            artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")
            immutable_source = artifact_path.read_bytes()
            state.STATE["orders"] = [
                {
                    "strategy_id": "LIVE-PROMOTE-1",
                    "state": "filled",
                    "queue_state": "filled",
                    "dry_run": False,
                }
            ]
            readiness = {"operator_confirmed": True, "summary": {"blocker_count": 0}}
            try:
                with patch("live_trader.state.snapshot", return_value=readiness):
                    result = state.promote_strategy_to_live("LIVE-PROMOTE-1")
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

        self.assertTrue(result["ok"], result["reason"])
        self.assertEqual(immutable_source, immutable_after)
        self.assertEqual("live", deployment["lifecycle"])
        self.assertTrue(deployment["permissions"]["live_allowed"])
        self.assertEqual("live-execution", evidence["evidenceType"])
        self.assertEqual("PASS", evidence["result"])
        self.assertTrue(evidence["integrity"]["contentHash"])

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
        self.assertEqual(order["order_id"], "LIVE-DRY-0001")
        self.assertEqual(order["state"], "dry_run")
        self.assertEqual(order["queue_state"], "simulated")
        self.assertTrue(order["dry_run"])
        self.assertEqual(order["strategy_id"], "LIVE-OK")
        self.assertIn("risk_report", order)
        self.assertTrue(order["risk_report"]["can_submit"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("LIVE-DRY-0001", audit_detail)
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
        self.assertEqual("LIVE-DRY-0001", event["order_id"])
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
        self.assertEqual(order["order_id"], "LIVE-BLOCK-0001")
        self.assertEqual(order["state"], "adapter_blocked")
        self.assertEqual(order["queue_state"], "held")
        self.assertFalse(order["dry_run"])
        audit_detail = state.STATE["audit"][-1]["detail"]
        self.assertIn("LIVE-BLOCK-0001", audit_detail)
        self.assertIn("adapter_blocked/held", audit_detail)
        self.assertIn("risk pass", audit_detail)
        self.assertIn("제출 차단", audit_detail)

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
        self.assertEqual(state.STATE["audit"][-1]["event"], "전략 Runner")
        self.assertIn("risk pass", state.STATE["audit"][-1]["detail"])

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
                    "upbit": [],
                }
                return rows[broker_id]

        with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
            result = state.run_reconciliation()

        self.assertTrue(result["ok"])
        broker_data = state.STATE["broker_reconciliation"]
        self.assertEqual(len(broker_data["accounts"]), 3)
        self.assertEqual(len(broker_data["positions"]), 2)
        self.assertEqual(len(broker_data["errors"]), 0)
        self.assertEqual(broker_data["successful_position_brokers"], ["kis", "binance", "upbit"])
        self.assertEqual(result["reconciliation"]["summary"]["error_count"], 0)
        self.assertGreaterEqual(result["reconciliation"]["summary"]["mismatch_count"], 1)
        self.assertTrue(
            any(
                row["broker_id"] == "kis"
                and row["currency"] == "KRW"
                and row["broker_cash"].startswith("100,000")
                and row["status"] == "api_required"
                for row in result["reconciliation"]["accounts"]
            )
        )
        self.assertTrue(
            any(
                row["broker_id"] == "binance" and row["symbol"] == "BTC" and row["status"] == "mismatch"
                for row in result["reconciliation"]["positions"]
            )
        )

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

    def test_execution_event_poll_syncs_broker_snapshot_to_program_ledger(self) -> None:
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
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            self.use_temp_program_ledger(temp_dir)
            try:
                with patch("live_trader.state.LiveBrokerRouter", return_value=FakeRouter()):
                    result = state.poll_execution_events("kis")
            finally:
                self.restore_temp_program_ledger()

        self.assertTrue(result["ok"])
        self.assertEqual(result["program_ledger"]["cash_count"], 1)
        self.assertEqual(result["program_ledger"]["position_count"], 1)
        self.assertEqual(result["execution_events"]["synced_cash_count"], 1)
        self.assertEqual(result["execution_events"]["synced_position_count"], 1)

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
            "live_trader.brokers.send_prepared_request",
            return_value={"ok": True, "json": payload},
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
            journal.save({"mode": "SMALL_LIVE", "dry_run": False, "orders": [], "strategy_runner": {}}, reason="before-restart", idempotency_keys=["persisted-key"])
            with patch.object(state, "RECOVERY_JOURNAL", journal):
                restored = state.restore_runtime_from_checkpoint()
        self.assertTrue(restored["verified"])
        self.assertTrue(restored["safeMode"])
        self.assertEqual("MONITOR", state.STATE["mode"])
        self.assertTrue(state.STATE["dry_run"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        self.assertIn("persisted-key", state.STATE["persisted_idempotency_keys"])

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
                "portfolio_gate": {"active": True, "allowed": True, "targetWeight": 0.4, "policyTargetWeight": 0.4, "maxSymbolWeightPct": 100, "instance": {"instanceId": "trend-1"}},
            },
            {
                "strategy_id": "revert", "name": "Revert", "symbol": "BTCUSDT", "asset": "CRYPTO", "plugin": "rsi_reversion",
                "test_signal": "SELL", "reference_price": 1000, "order_quantity": 1, "live_allowed": True,
                "instrument_id": "CRYPTO:BINANCE:BTCUSDT", "market_type": "spot", "allow_short": False,
                "portfolio_gate": {"active": True, "allowed": True, "targetWeight": 0.3, "policyTargetWeight": 0.3, "maxSymbolWeightPct": 100, "instance": {"instanceId": "revert-1"}},
            },
        ]
        fake_snapshot = {
            "summary": {"blocker_count": 0, "warning_count": 0}, "strategies": strategies,
            "portfolios": [{"strategy_instances": [{"strategyId": "trend"}, {"strategyId": "revert"}]}],
            "brokers": [], "reconciliation": {"summary": {"status": "pass"}}, "operational_readiness": {},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(state, "RECOVERY_JOURNAL", state.RecoveryJournal(Path(temp_dir) / "recovery")):
                with patch("live_trader.state.snapshot", return_value=fake_snapshot):
                    result = state.run_strategy_cycle("crypto")
        self.assertTrue(result["ok"])
        self.assertEqual(2, len(result["runner_reports"]))
        self.assertEqual(1, len(result["plans"]))
        self.assertEqual(1, len(result["orders"]))
        self.assertEqual("BUY", result["plans"][0]["side"])
        self.assertEqual({"trend-1": 0.4, "revert-1": 0.0}, result["plans"][0]["sleeve_targets"])
        self.assertEqual(1, len(state.STATE["orders"]))


if __name__ == "__main__":
    unittest.main()
