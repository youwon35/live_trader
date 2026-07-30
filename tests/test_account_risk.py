from __future__ import annotations

import json
import tempfile
import unittest
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from live_trader.account_risk import (
    broker_account_risk,
    load_account_risk_budget,
    update_account_risk_budget,
)
from live_trader import state
from live_trader.order_management import OrderIntent


class AccountRiskBudgetTest(unittest.TestCase):
    def test_budget_is_restart_safe_and_computes_drawdown(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "risk-budget.json"
            start = datetime(2026, 7, 30, 9, 0, tzinfo=timezone.utc)
            first = update_account_risk_budget(
                path,
                [
                    {
                        "broker_id": "binance-futures",
                        "currency": "USDT",
                        "broker_cash": 90,
                        "broker_equity": 100,
                    }
                ],
                now=start,
            )
            second = update_account_risk_budget(
                path,
                [
                    {
                        "broker_id": "binance-futures",
                        "currency": "USDT",
                        "broker_cash": 84,
                        "broker_equity": 90,
                    }
                ],
                now=start + timedelta(hours=1),
            )

            self.assertEqual(100, first["budgets"]["binance-futures:USDT"]["starting_equity"])
            self.assertEqual(-10, second["budgets"]["binance-futures:USDT"]["daily_pnl"])
            self.assertEqual(-10, second["budgets"]["binance-futures:USDT"]["daily_pnl_pct"])
            self.assertEqual(
                -10,
                second["budgets"]["binance-futures:USDT"][
                    "minimum_daily_pnl_pct"
                ],
            )
            reloaded = load_account_risk_budget(path)
            self.assertEqual(
                100,
                reloaded["budgets"]["binance-futures:USDT"]["starting_equity"],
            )

    def test_new_local_day_rolls_the_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "risk-budget.json"
            first_day = datetime(2026, 7, 30, 23, 59).astimezone()
            update_account_risk_budget(
                path,
                [
                    {
                        "broker_id": "binance-futures",
                        "currency": "USDT",
                        "broker_cash": 95,
                        "broker_equity": 100,
                    }
                ],
                now=first_day,
            )
            next_day = first_day + timedelta(minutes=2)
            result = update_account_risk_budget(
                path,
                [
                    {
                        "broker_id": "binance-futures",
                        "currency": "USDT",
                        "broker_cash": 85,
                        "broker_equity": 90,
                    }
                ],
                now=next_day,
            )

            budget = result["budgets"]["binance-futures:USDT"]
            self.assertEqual(90, budget["starting_equity"])
            self.assertEqual(0, budget["daily_pnl_pct"])

    def test_lookup_reports_freshness_without_exposing_raw_account(self) -> None:
        observed = datetime(2026, 7, 30, 9, 0, tzinfo=timezone.utc)
        snapshot = {
            "budgets": {
                "binance-futures:USDT": {
                    "broker_id": "binance-futures",
                    "currency": "USDT",
                    "starting_equity": 100,
                    "current_equity": 99,
                    "available_cash": 91,
                    "daily_pnl_pct": -1,
                    "observed_epoch": observed.timestamp(),
                }
            }
        }
        fresh = broker_account_risk(
            snapshot,
            "binance-futures",
            currency="USDT",
            now_epoch=(observed + timedelta(seconds=30)).timestamp(),
        )
        stale = broker_account_risk(
            snapshot,
            "binance-futures",
            currency="USDT",
            now_epoch=(observed + timedelta(seconds=121)).timestamp(),
        )

        self.assertTrue(fresh["fresh"])
        self.assertFalse(stale["fresh"])
        self.assertNotIn("api_key", json.dumps(fresh))


class LiveRiskStateIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(self.original_state)

    def test_risk_settings_are_persisted_and_reloaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            original_path = state.RISK_SETTINGS_PATH
            state.RISK_SETTINGS_PATH = Path(directory) / "risk-settings.json"
            try:
                values = dict(state.DEFAULT_RISK_SETTINGS)
                values["daily_loss_limit_pct"] = -10
                state.persist_risk_setting_values(values)
                loaded = state.load_risk_setting_values()
            finally:
                state.RISK_SETTINGS_PATH = original_path

        self.assertEqual(-10, loaded["daily_loss_limit_pct"])

    def test_futures_live_context_uses_available_usdt_and_real_drawdown(self) -> None:
        now = datetime.now().astimezone()
        state.STATE["account_risk"] = {
            "budgets": {
                "binance-futures:USDT": {
                    "broker_id": "binance-futures",
                    "currency": "USDT",
                    "starting_equity": 100,
                    "current_equity": 90,
                    "available_cash": 84,
                    "daily_pnl_pct": -10,
                    "observed_epoch": now.timestamp(),
                }
            }
        }
        state.STATE["risk_settings"]["daily_loss_limit_pct"] = -10
        state.STATE["broker_reconciliation"]["positions"] = []
        intent = OrderIntent(
            strategy_id="short-live",
            asset="코인 USD-M 선물",
            symbol="BTCUSDT",
            side="SELL",
            quantity=0.001,
            reference_price=60_000,
            mode="SMALL_LIVE",
            reason="test",
            metadata={
                "broker_id": "binance-futures",
                "market_type": "futures",
                "position_direction": "short",
                "short_entries_requested": True,
                "broker_short_adapter_verified": True,
            },
        )

        context = state.pre_trade_context(
            {"summary": {"blocker_count": 0}},
            intent,
            dry_run=False,
        )

        self.assertEqual(84, context.max_order_value)
        self.assertEqual(84, context.available_cash)
        self.assertEqual(90, context.portfolio_equity)
        self.assertEqual(-10, context.daily_pnl_pct)
        self.assertTrue(context.api_healthy)

    def test_daily_loss_gate_latches_to_minimum_observed_equity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            original_path = state.ACCOUNT_RISK_BUDGET_PATH
            state.ACCOUNT_RISK_BUDGET_PATH = (
                Path(directory) / "account-risk.json"
            )
            state.STATE["risk_settings"]["daily_loss_limit_pct"] = -10
            state.STATE["new_entries_blocked"] = False
            try:
                with patch.object(state, "append_audit"):
                    state.refresh_account_risk_budget(
                        [
                            {
                                "broker_id": "binance-futures",
                                "currency": "USDT",
                                "broker_cash": 100,
                                "broker_equity": 100,
                            }
                        ]
                    )
                    state.refresh_account_risk_budget(
                        [
                            {
                                "broker_id": "binance-futures",
                                "currency": "USDT",
                                "broker_cash": 90,
                                "broker_equity": 90,
                            }
                        ]
                    )
                    state.refresh_account_risk_budget(
                        [
                            {
                                "broker_id": "binance-futures",
                                "currency": "USDT",
                                "broker_cash": 95,
                                "broker_equity": 95,
                            }
                        ]
                    )
            finally:
                state.ACCOUNT_RISK_BUDGET_PATH = original_path

        self.assertTrue(state.STATE["daily_loss_gate_tripped"])
        self.assertTrue(state.STATE["new_entries_blocked"])
        risk = state.account_risk_for_intent(
            OrderIntent(
                strategy_id="risk-test",
                asset="코인",
                symbol="BTCUSDT",
                side="SELL",
                quantity=0.001,
                reference_price=60_000,
                mode="SMALL_LIVE",
                reason="risk latch test",
                metadata={"broker_id": "binance-futures"},
            )
        )
        self.assertEqual(-10, risk["minimum_daily_pnl_pct"])


if __name__ == "__main__":
    unittest.main()
