from __future__ import annotations

import copy
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.order_management import OrderIntent


class ReduceOnlyVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        state.STATE.update(
            {
                "mode": "SMALL_LIVE",
                "dry_run": False,
                "operator_confirmed": True,
                "kill_switch": False,
                "new_entries_blocked": True,
                "broker_reconciliation": {
                    "fetched_at": None,
                    "accounts": [],
                    "positions": [],
                    "errors": [],
                    "successful_account_brokers": [],
                    "successful_position_brokers": [],
                },
                "broker_snapshot_poll": {"brokers": {}},
            }
        )

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    @staticmethod
    def _intent(
        broker_id: str,
        symbol: str,
        side: str,
        quantity: float,
        *,
        strategy_id: str = "reduce-test",
    ) -> OrderIntent:
        return OrderIntent(
            strategy_id=strategy_id,
            asset="CRYPTO" if broker_id != "kis" else "한국주식",
            symbol=symbol,
            side=side,
            quantity=quantity,
            reference_price=60_000,
            mode="SMALL_LIVE",
            reason="reduce-only unit test",
            metadata={
                "broker_id": broker_id,
                "risk_reducing": True,
                "confirmed_bar_end": datetime.now(timezone.utc).isoformat(),
            },
        )

    @staticmethod
    def _futures_strategy() -> dict[str, object]:
        return {
            "strategy_id": "short-futures",
            "symbol": "BTCUSDT.PERP",
            "broker_id": "binance-futures",
            "market_type": "futures",
            "position_direction": "short",
        }

    def _install_snapshot(
        self,
        broker_id: str,
        position: dict[str, object],
        *,
        fetched_at: datetime | None = None,
    ) -> None:
        state.STATE["broker_reconciliation"] = {
            "fetched_at": (fetched_at or datetime.now()).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "accounts": [],
            "positions": [{"broker_id": broker_id, **position}],
            "errors": [],
            "successful_account_brokers": [],
            "successful_position_brokers": [broker_id],
        }

    def _dispatch(self, intent: OrderIntent) -> tuple[bool, str]:
        with patch.object(
            state,
            "real_orders_enabled",
            return_value=True,
        ), patch.object(
            state,
            "durable_control_halt_active",
            return_value=False,
        ):
            return state.live_broker_dispatch_allowed(intent, dry_run=False)

    def test_kis_forged_claim_is_blocked_and_valid_sell_is_allowed(self) -> None:
        self._install_snapshot(
            "kis",
            {"symbol": "005930.KS", "broker_qty": 10.0},
        )

        valid, _ = self._dispatch(self._intent("kis", "005930.KS", "SELL", 3))
        forged, reason = self._dispatch(
            self._intent("kis", "005930.KS", "BUY", 1)
        )

        self.assertTrue(valid)
        self.assertFalse(forged)
        self.assertEqual("risk-increasing-order-blocked", reason)

    def test_upbit_forged_claim_is_blocked_and_valid_sell_is_allowed(self) -> None:
        self._install_snapshot(
            "upbit",
            {"symbol": "KRW-BTC", "broker_qty": 1.0},
        )

        valid, _ = self._dispatch(
            self._intent("upbit", "KRW-BTC", "SELL", 0.2)
        )
        forged, reason = self._dispatch(
            self._intent("upbit", "KRW-BTC", "BUY", 0.1)
        )

        self.assertTrue(valid)
        self.assertFalse(forged)
        self.assertEqual("risk-increasing-order-blocked", reason)

    def test_binance_spot_alias_requires_a_real_reducing_position(self) -> None:
        self._install_snapshot(
            "binance",
            {"symbol": "BTC", "broker_qty": 1.0},
        )

        valid, _ = self._dispatch(
            self._intent("binance", "BTCUSDT", "SELL", 0.2)
        )
        forged, reason = self._dispatch(
            self._intent("binance", "BTCUSDT", "BUY", 0.1)
        )

        self.assertTrue(valid)
        self.assertFalse(forged)
        self.assertEqual("risk-increasing-order-blocked", reason)

    def test_binance_futures_short_only_allows_verified_cover(self) -> None:
        self._install_snapshot(
            "binance-futures",
            {
                "symbol": "BTCUSDT",
                "broker_qty": -1.0,
                "position_side": "SHORT",
            },
        )
        strategy = self._futures_strategy()
        with patch.object(state, "portfolio_rows", return_value=[]), patch.object(
            state,
            "strategy_rows",
            return_value=[strategy],
        ):
            valid, _ = self._dispatch(
                self._intent(
                    "binance-futures",
                    "BTCUSDT.PERP",
                    "BUY",
                    0.2,
                    strategy_id="short-futures",
                )
            )
            forged, reason = self._dispatch(
                self._intent(
                    "binance-futures",
                    "BTCUSDT.PERP",
                    "SELL",
                    0.1,
                    strategy_id="short-futures",
                )
            )

        self.assertTrue(valid)
        self.assertFalse(forged)
        self.assertEqual("risk-increasing-order-blocked", reason)

    def test_stale_missing_or_wrong_symbol_snapshot_fails_closed(self) -> None:
        valid_intent = self._intent("binance", "BTCUSDT", "SELL", 0.2)
        stale = datetime.now() - timedelta(
            seconds=state.REDUCE_ONLY_POSITION_MAX_AGE_SECONDS + 5
        )
        self._install_snapshot(
            "binance",
            {"symbol": "BTC", "broker_qty": 1.0},
            fetched_at=stale,
        )
        self.assertFalse(state.futures_risk_reducing_verified({}, valid_intent))

        state.STATE["broker_reconciliation"][
            "fetched_at"
        ] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        state.STATE["broker_reconciliation"]["position_observations"] = {
            "binance": {
                "observedAt": stale.strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        self.assertFalse(state.futures_risk_reducing_verified({}, valid_intent))

        state.STATE["broker_reconciliation"]["position_observations"] = {}
        state.STATE["broker_reconciliation"][
            "successful_position_brokers"
        ] = []
        self.assertFalse(state.futures_risk_reducing_verified({}, valid_intent))

        state.STATE["broker_reconciliation"][
            "successful_position_brokers"
        ] = ["binance"]
        state.STATE["broker_reconciliation"]["positions"] = [
            {
                "broker_id": "binance",
                "symbol": "ETH",
                "broker_qty": 1.0,
            }
        ]
        self.assertFalse(state.futures_risk_reducing_verified({}, valid_intent))

    def test_order_cannot_cross_flat_even_with_true_claim(self) -> None:
        self._install_snapshot(
            "upbit",
            {"symbol": "KRW-BTC", "broker_qty": 1.0},
        )

        allowed, reason = self._dispatch(
            self._intent("upbit", "KRW-BTC", "SELL", 1.1)
        )

        self.assertFalse(allowed)
        self.assertEqual("risk-increasing-order-blocked", reason)

    def test_final_dispatch_rechecks_claim_when_entries_are_allowed(self) -> None:
        self._install_snapshot(
            "binance",
            {"symbol": "BTC", "broker_qty": 1.0},
        )
        intent = self._intent("binance", "BTCUSDT", "SELL", 0.2)
        state.STATE["new_entries_blocked"] = False
        state.STATE["broker_reconciliation"]["fetched_at"] = (
            datetime.now()
            - timedelta(
                seconds=state.REDUCE_ONLY_POSITION_MAX_AGE_SECONDS + 5
            )
        ).strftime("%Y-%m-%d %H:%M:%S")

        allowed, reason = self._dispatch(intent)

        self.assertFalse(allowed)
        self.assertEqual("reduce-only-position-verification-failed", reason)

    def test_submit_rewrites_unverified_claim_for_every_broker(self) -> None:
        captured: list[OrderIntent] = []

        def capture_gate(*args: object) -> tuple[bool, str, str, str, Mock]:
            captured.append(args[3])
            return False, "risk_blocked", "blocked", "unit", Mock()

        state.STATE["orders"] = [{"idempotency_key": "existing"}]
        state.STATE["persisted_idempotency_keys"] = ["existing"]
        brokers = (
            ("kis", "005930.KS"),
            ("upbit", "KRW-BTC"),
            ("binance", "BTCUSDT"),
            ("binance-futures", "BTCUSDT.PERP"),
        )
        with patch.object(state.RECOVERY_JOURNAL, "save"), patch.object(
            state,
            "evaluate_order_gate_with_report",
            side_effect=capture_gate,
        ), patch.object(
            state,
            "portfolio_gate_for_intent",
            return_value={"active": False},
        ), patch.object(
            state,
            "build_idempotency_key",
            return_value="existing",
        ), patch.object(
            state.DECISION_TRACE_STORE,
            "append",
        ), patch.object(state, "append_audit"), patch.object(
            state,
            "snapshot",
            return_value={},
        ):
            for broker_id, symbol in brokers:
                result = state.submit_order_intent(
                    {"strategies": []},
                    self._intent(broker_id, symbol, "SELL", 0.1),
                    dry_run=True,
                    audit_event="unit",
                )
                self.assertTrue(result["duplicate"])

        self.assertEqual(4, len(captured))
        for intent in captured:
            self.assertFalse(intent.metadata["risk_reducing"])
            self.assertTrue(intent.metadata["risk_reducing_claim_rejected"])


if __name__ == "__main__":
    unittest.main()
