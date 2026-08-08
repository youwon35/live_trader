import copy
from decimal import Decimal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from trading_runtime import RuntimeStrategySpec

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent
from live_trader.portfolio_execution import (
    LIVE_PORTFOLIO_PLAN_SCHEMA,
    LivePortfolioLedger,
    SleeveTarget,
    build_symbol_net_plan,
)


class LivePortfolioLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.ledger = LivePortfolioLedger(
            Path(self.temporary.name) / "portfolio.sqlite3"
        )
        self.scope_id = "live:portfolio-one:hash-one"
        self.ledger.register_scope(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            account_id="kis-account:test",
        )

    def _two_sleeve_buy(self):
        return build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("intent-a", "sleeve-a", "005930.KS", 1),
                SleeveTarget("intent-b", "sleeve-b", "005930", 1),
            ),
            current_positions={},
            broker_quantity=0,
        )

    def test_same_symbol_targets_are_netted_to_one_broker_order(self) -> None:
        plan = build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("buy-a", "sleeve-a", "005930", 2),
                SleeveTarget("sell-b", "sleeve-b", "005930.KS", 0),
            ),
            current_positions={
                ("sleeve-a", "005930"): 0,
                ("sleeve-b", "005930"): 1,
            },
            broker_quantity=1,
            reference_price=100,
        )

        self.assertEqual("BUY", plan.side)
        self.assertEqual(Decimal("1"), plan.quantity)
        self.assertEqual(
            {"sleeve-a": Decimal("1")},
            {item.sleeve_id: item.signed_quantity for item in plan.allocations},
        )
        self.assertEqual(
            {"sleeve-a": Decimal("1"), "sleeve-b": Decimal("-1")},
            {
                item.sleeve_id: item.signed_quantity
                for item in plan.internal_allocations
            },
        )
        self.assertEqual(
            Decimal("1"),
            sum((item.signed_quantity for item in plan.allocations), Decimal("0")),
        )

    def test_partial_fills_and_late_exact_fee_are_append_only(self) -> None:
        plan = self._two_sleeve_buy()
        self.ledger.record_accepted_order(
            plan,
            broker_order_id="broker-1",
            local_order_id="local-1",
            occurred_at="2026-08-05T00:00:00Z",
        )
        first = {
            "event_id": "fill-1",
            "broker_id": "kis",
            "broker_order_id": "broker-1",
            "symbol": "005930.KS",
            "side": "BUY",
            "quantity": 1,
            "price": 100,
            "fee": 0,
            "state": "FILLED",
            "occurred_at": "2026-08-05T00:01:00Z",
        }
        report = self.ledger.apply_execution_events(self.scope_id, [first])
        self.assertEqual(1, report.applied_fills)
        self.assertEqual(("broker-1",), self.ledger.pending_orders(self.scope_id))

        replay = self.ledger.apply_execution_events(self.scope_id, [first])
        self.assertEqual(0, replay.applied_fills)

        exact_cost = {**first, "fee": 1}
        revised = self.ledger.apply_execution_events(
            self.scope_id, [exact_cost]
        )
        self.assertEqual(1, revised.applied_fee_adjustments)
        self.assertEqual(Decimal("1"), self.ledger.balances(self.scope_id)["portfolio"]["fees"])

        # A stale fee-less replay cannot reverse the exact cost.
        stale = self.ledger.apply_execution_events(self.scope_id, [first])
        self.assertEqual(0, stale.applied_fee_adjustments)
        self.assertEqual(Decimal("1"), self.ledger.balances(self.scope_id)["portfolio"]["fees"])

        with self.assertRaisesRegex(ValueError, "quantity/price"):
            self.ledger.apply_execution_events(
                self.scope_id,
                [{**exact_cost, "quantity": 2}],
            )

        second = {
            **first,
            "event_id": "fill-2",
            "quantity": 1,
            "price": 110,
            "fee": 1,
            "occurred_at": "2026-08-05T00:02:00Z",
        }
        self.ledger.apply_execution_events(self.scope_id, [second])
        self.assertEqual((), self.ledger.pending_orders(self.scope_id))
        self.assertEqual(
            {
                "sleeve-a": {"005930": Decimal("1")},
                "sleeve-b": {"005930": Decimal("1")},
            },
            self.ledger.sleeve_holdings(self.scope_id),
        )
        self.assertEqual(Decimal("2"), self.ledger.balances(self.scope_id)["portfolio"]["fees"])
        count, head = self.ledger.verify_hash_chain(self.scope_id)
        self.assertGreater(count, 0)
        self.assertEqual(64, len(head))

    def test_opposing_sleeves_cross_zero_cost_and_only_residual_pays_fee(self) -> None:
        seed = build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("seed-b", "sleeve-b", "005930", 3),),
            current_positions={},
            broker_quantity=0,
        )
        self.ledger.record_accepted_order(seed, broker_order_id="seed-order")
        self.ledger.apply_execution_events(
            self.scope_id,
            [
                {
                    "event_id": "seed-fill",
                    "broker_id": "kis",
                    "broker_order_id": "seed-order",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 3,
                    "price": 90,
                    "fee": 0,
                    "state": "FILLED",
                }
            ],
        )
        plan = build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("buy-five", "sleeve-a", "005930", 5),
                SleeveTarget("sell-three", "sleeve-b", "005930", 0),
            ),
            current_positions={
                ("sleeve-a", "005930"): 0,
                ("sleeve-b", "005930"): 3,
            },
            broker_quantity=3,
            reference_price=100,
        )
        self.assertEqual("BUY", plan.side)
        self.assertEqual(Decimal("2"), plan.quantity)
        self.assertEqual(
            {"sleeve-a": Decimal("2")},
            {item.sleeve_id: item.signed_quantity for item in plan.allocations},
        )
        self.assertEqual(
            {"sleeve-a": Decimal("3"), "sleeve-b": Decimal("-3")},
            {
                item.sleeve_id: item.signed_quantity
                for item in plan.internal_allocations
            },
        )

        # ACK recovery atomically reconstructs the zero-cost internal cross.
        recovered = self.ledger.recover_accepted_orders(
            self.scope_id,
            [
                {
                    "order_id": "local-net",
                    "broker_order_id": "broker-net",
                    "updated_at": "2026-08-05T00:00:00Z",
                    "portfolio_execution": plan.metadata(),
                }
            ],
        )
        self.assertEqual(1, recovered)
        self.assertEqual(
            0,
            self.ledger.recover_accepted_orders(
                self.scope_id,
                [
                    {
                        "order_id": "local-net",
                        "broker_order_id": "broker-net",
                        "updated_at": "2026-08-05T00:00:01Z",
                        "portfolio_execution": plan.metadata(),
                    }
                ],
            ),
        )
        self.assertEqual(
            {
                "sleeve-a": {"005930": Decimal("3")},
                "sleeve-b": {"005930": Decimal("0")},
            },
            self.ledger.sleeve_holdings(self.scope_id),
        )
        first = {
            "event_id": "net-fill-1",
            "broker_id": "kis",
            "broker_order_id": "broker-net",
            "symbol": "005930",
            "side": "BUY",
            "quantity": 1,
            "price": 101,
            "fee": 0,
            "state": "PARTIALLY_FILLED",
        }
        report = self.ledger.apply_execution_events(self.scope_id, [first])
        self.assertEqual(1, report.applied_fills)
        self.assertEqual(
            Decimal("4"),
            self.ledger.sleeve_quantity(self.scope_id, "sleeve-a", "005930"),
        )
        self.assertEqual(
            Decimal("0"),
            self.ledger.sleeve_quantity(self.scope_id, "sleeve-b", "005930"),
        )

        # Simulate restart/reconnect.  Exact late cost evidence revises only
        # the external residual buyer and a stale fee-less replay is inert.
        restarted = LivePortfolioLedger(self.ledger.path)
        late_fee = restarted.apply_execution_events(
            self.scope_id,
            [{**first, "fee": 1}],
        )
        self.assertEqual(1, late_fee.applied_fee_adjustments)
        stale = restarted.apply_execution_events(self.scope_id, [first])
        self.assertEqual(0, stale.applied_fee_adjustments)
        replay = restarted.apply_execution_events(
            self.scope_id,
            [{**first, "fee": 1}],
        )
        self.assertEqual(0, replay.applied_fee_adjustments)

        second = {
            **first,
            "event_id": "net-fill-2",
            "price": 102,
            "fee": 1,
            "state": "FILLED",
        }
        restarted.apply_execution_events(self.scope_id, [second])
        self.assertEqual((), restarted.pending_orders(self.scope_id))
        self.assertEqual(
            {
                "sleeve-a": {"005930": Decimal("5")},
                "sleeve-b": {"005930": Decimal("0")},
            },
            restarted.sleeve_holdings(self.scope_id),
        )
        balances = restarted.balances(self.scope_id)["sleeves"]
        self.assertEqual(Decimal("2"), balances["sleeve-a"]["fees"])
        self.assertEqual(Decimal("0"), balances["sleeve-b"]["fees"])

    def test_relevant_execution_time_requires_timezone_and_is_canonical_utc(self) -> None:
        plan = self._two_sleeve_buy()
        self.ledger.record_accepted_order(plan, broker_order_id="broker-time")
        event = {
            "event_id": "time-fill",
            "broker_id": "kis",
            "broker_order_id": "broker-time",
            "symbol": "005930",
            "side": "BUY",
            "quantity": 2,
            "price": 100,
            "fee": 0,
            "state": "FILLED",
        }
        with self.assertRaisesRegex(ValueError, "explicit timezone"):
            self.ledger.apply_execution_events(
                self.scope_id,
                [{**event, "occurred_at": "2026-08-05 09:30:00"}],
            )
        self.ledger.apply_execution_events(
            self.scope_id,
            [{**event, "occurred_at": "2026-08-05T09:30:00+09:00"}],
        )
        fill_rows = [
            row
            for row in self.ledger._events(self.scope_id)
            if row["event_type"] == "BROKER_FILL"
        ]
        self.assertEqual(1, len(fill_rows))
        self.assertEqual(
            "2026-08-05T00:30:00.000000Z",
            fill_rows[0]["occurred_at"],
        )
        self.ledger.verify_hash_chain(self.scope_id)

    def test_restart_reconciliation_enforces_dedicated_account(self) -> None:
        plan = self._two_sleeve_buy()
        self.ledger.record_accepted_order(plan, broker_order_id="broker-1")
        self.ledger.apply_execution_events(
            self.scope_id,
            [
                {
                    "event_id": "fill-all",
                    "broker_id": "kis",
                    "broker_order_id": "broker-1",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 2,
                    "price": 100,
                    "fee": 1,
                    "state": "FILLED",
                }
            ],
        )
        matched = self.ledger.reconcile_restart(
            scope_id=self.scope_id,
            broker_holdings={"005930": 2},
            managed_symbols=("005930",),
            persist=False,
        )
        self.assertTrue(matched.ready)

        external = self.ledger.reconcile_restart(
            scope_id=self.scope_id,
            broker_holdings={"005930": 2, "000660": 1},
            managed_symbols=("005930",),
            persist=False,
        )
        self.assertTrue(external.matched)
        self.assertFalse(external.ready)
        self.assertEqual({"000660": Decimal("1")}, external.external_holdings)

        mismatch = self.ledger.reconcile_restart(
            scope_id=self.scope_id,
            broker_holdings={"005930": 1},
            managed_symbols=("005930",),
            persist=False,
        )
        self.assertFalse(mismatch.matched)
        self.assertFalse(mismatch.ready)

    def test_accepted_order_is_recovered_from_dispatch_journal_metadata(self) -> None:
        plan = self._two_sleeve_buy()
        recovered = self.ledger.recover_accepted_orders(
            self.scope_id,
            [
                {
                    "order_id": "local-1",
                    "broker_order_id": "broker-1",
                    "updated_at": "2026-08-05T00:00:00Z",
                    "portfolio_execution": plan.metadata(),
                }
            ],
        )
        self.assertEqual(1, recovered)
        self.assertEqual(("broker-1",), self.ledger.pending_orders(self.scope_id))
        self.assertEqual(
            0,
            self.ledger.recover_accepted_orders(
                self.scope_id,
                [
                    {
                        "order_id": "local-1",
                        "broker_order_id": "broker-1",
                        "updated_at": "2026-08-05T00:00:01Z",
                        "portfolio_execution": plan.metadata(),
                    }
                ],
            ),
        )

    def test_zero_net_internal_cross_moves_only_sleeve_ownership(self) -> None:
        seed = build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("seed", "sleeve-b", "005930", 1),),
            current_positions={},
            broker_quantity=0,
        )
        self.ledger.record_accepted_order(seed, broker_order_id="seed-order")
        self.ledger.apply_execution_events(
            self.scope_id,
            [
                {
                    "event_id": "seed-fill",
                    "broker_id": "kis",
                    "broker_order_id": "seed-order",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 1,
                    "price": 100,
                    "fee": 0,
                    "state": "FILLED",
                }
            ],
        )
        cross = build_symbol_net_plan(
            scope_id=self.scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("take", "sleeve-a", "005930", 1),
                SleeveTarget("give", "sleeve-b", "005930", 0),
            ),
            current_positions={
                ("sleeve-a", "005930"): 0,
                ("sleeve-b", "005930"): 1,
            },
            broker_quantity=1,
            reference_price=105,
        )
        self.assertTrue(cross.internal_only)
        self.ledger.record_internal_cross(cross, price=105)
        self.assertEqual(
            {
                "sleeve-a": {"005930": Decimal("1")},
                "sleeve-b": {"005930": Decimal("0")},
            },
            self.ledger.sleeve_holdings(self.scope_id),
        )


class LiveContinuousPortfolioExecutionTest(unittest.TestCase):
    @staticmethod
    def _spec(instance_id: str, strategy_id: str) -> RuntimeStrategySpec:
        return RuntimeStrategySpec(
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            strategy_instance_id=instance_id,
            strategy_id=strategy_id,
            artifact_hash=f"hash-{strategy_id}",
            plugin_id="moving_average_cross",
            instrument_id="005930",
            symbol="005930",
            timeframe="1h",
            provider="kis",
            broker_id="kis",
            target_weight=0.5,
            parameters={"liveOrderQuantity": 1},
            artifact={
                "instanceId": instance_id,
                "sourceStrategyId": strategy_id,
                "sourceArtifactHash": f"hash-{strategy_id}",
                "sourceInstanceHash": f"instance-hash-{instance_id}",
            },
        )

    @staticmethod
    def _decision(spec: RuntimeStrategySpec, key: str):
        return SimpleNamespace(
            strategy_id=spec.strategy_id,
            strategy_instance_id=spec.strategy_instance_id,
            signal="BUY",
            reason="entry",
            evaluation_key=key,
            bar=SimpleNamespace(
                close=70_000.0,
                end_time="2026-08-05T00:00:00Z",
            ),
        )

    def _configured_controller(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        controller = LiveContinuousController(Path(temporary.name))
        specs = (
            self._spec("sleeve-a", "strategy-a"),
            self._spec("sleeve-b", "strategy-b"),
        )
        controller.mode = "SMALL_LIVE"
        controller.profile_id = "stock"
        controller.portfolio_id = "portfolio-one"
        controller.strategy_ids = ("strategy-a", "strategy-b")
        controller.allowed_symbols = ("005930",)
        controller.portfolio_instance_id = "portfolio-instance-one"
        controller.portfolio_execution_scope_id = "live:portfolio-one:hash-one"
        controller.portfolio_execution_account_id = "kis-account:test"
        controller.portfolio_execution_symbols = ("005930",)
        controller.portfolio_ledger = LivePortfolioLedger(
            Path(temporary.name) / "logs" / "sleeves.sqlite3"
        )
        controller.portfolio_ledger.register_scope(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            account_id="kis-account:test",
        )
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = specs
        return controller, specs

    @staticmethod
    def _portfolio_intent(plan) -> OrderIntent:
        return OrderIntent(
            strategy_id="portfolio-one",
            asset="KR_STOCK",
            symbol=plan.symbol,
            side=plan.side,
            quantity=float(plan.quantity),
            reference_price=70_000,
            mode="SMALL_LIVE",
            reason="portfolio pre-POST regression",
            metadata={
                "broker_id": "kis",
                "portfolio_id": "portfolio-one",
                "strategy_instance_id": "sleeve-a",
                "instrument_id": "KRX:005930",
                "target_revision": 1,
                "confirmed_bar_end": "2026-08-05T00:00:00Z",
                "order_type": "00",
                "portfolio_execution": plan.metadata(),
            },
        )

    def test_two_same_symbol_sleeves_submit_exactly_one_broker_order(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        controller = LiveContinuousController(Path(temporary.name))
        specs = (self._spec("sleeve-a", "strategy-a"), self._spec("sleeve-b", "strategy-b"))
        controller.mode = "SMALL_LIVE"
        controller.profile_id = "stock"
        controller.portfolio_id = "portfolio-one"
        controller.strategy_ids = ("strategy-a", "strategy-b")
        controller.allowed_symbols = ("005930",)
        controller.portfolio_instance_id = "portfolio-instance-one"
        controller.portfolio_execution_scope_id = "live:portfolio-one:hash-one"
        controller.portfolio_execution_account_id = "kis-account:test"
        controller.portfolio_execution_symbols = ("005930",)
        controller.portfolio_ledger = LivePortfolioLedger(
            Path(temporary.name) / "logs" / "sleeves.sqlite3"
        )
        controller.portfolio_ledger.register_scope(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            account_id="kis-account:test",
        )
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = specs
        decisions = tuple(
            self._decision(spec, f"evaluation-{index}")
            for index, spec in enumerate(specs, start=1)
        )
        original_state = copy.deepcopy(state.STATE)
        try:
            state.STATE["dry_run"] = False
            state.STATE["broker_reconciliation"] = {
                "positions": [],
                "successful_position_brokers": ["kis"],
            }
            with (
                patch.object(
                    state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]
                ),
                patch.object(
                    state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]
                ),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(
                    state,
                    "submit_order_intent",
                    return_value={
                        "ok": True,
                        "reason": "broker-acknowledged",
                        "order": {
                            "order_id": "local-1",
                            "broker_order_id": "broker-1",
                            "created_at": "2026-08-05T00:00:01Z",
                            "updated_at": "2026-08-05T00:00:01Z",
                        },
                    },
                ) as submit,
            ):
                result = controller._handle_cycle(
                    SimpleNamespace(decisions=decisions)
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        submit.assert_called_once()
        intent = submit.call_args.args[1]
        self.assertEqual("BUY", intent.side)
        self.assertEqual(2.0, intent.quantity)
        self.assertTrue(intent.metadata["multi_strategy"])
        self.assertEqual(
            ["sleeve-a", "sleeve-b"],
            intent.metadata["strategy_instance_ids"],
        )
        self.assertEqual(
            LIVE_PORTFOLIO_PLAN_SCHEMA,
            intent.metadata["portfolio_execution"]["schemaVersion"],
        )
        self.assertEqual(2, len(result["results"]))
        self.assertEqual({"broker-1"}, {item["brokerOrderId"] for item in result["results"]})

    def test_controller_nets_opposing_sleeves_and_checkpoints_internal_cross(self) -> None:
        controller, specs = self._configured_controller()
        object.__setattr__(specs[0], "parameters", {"liveOrderQuantity": 5})
        seed = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("seed-b", "sleeve-b", "005930", 3),),
            current_positions={},
            broker_quantity=0,
        )
        controller.portfolio_ledger.record_accepted_order(
            seed,
            broker_order_id="seed-order",
        )
        controller.portfolio_ledger.apply_execution_events(
            controller.portfolio_execution_scope_id,
            [
                {
                    "event_id": "seed-fill",
                    "broker_id": "kis",
                    "broker_order_id": "seed-order",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 3,
                    "price": 90,
                    "fee": 0,
                    "state": "FILLED",
                }
            ],
        )
        buy = self._decision(specs[0], "evaluation-buy-five")
        sell = SimpleNamespace(
            strategy_id=specs[1].strategy_id,
            strategy_instance_id=specs[1].strategy_instance_id,
            signal="SELL",
            reason="exit",
            evaluation_key="evaluation-sell-three",
            bar=SimpleNamespace(
                close=70_000.0,
                end_time="2026-08-05T00:00:00Z",
            ),
        )
        original_state = copy.deepcopy(state.STATE)
        try:
            state.STATE["dry_run"] = False
            state.STATE["broker_reconciliation"] = {
                "positions": [
                    {"broker_id": "kis", "symbol": "005930", "broker_qty": 3}
                ],
                "successful_position_brokers": ["kis"],
            }
            with (
                patch.object(
                    state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]
                ),
                patch.object(
                    state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]
                ),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(
                    state,
                    "submit_order_intent",
                    return_value={
                        "ok": True,
                        "reason": "broker-acknowledged",
                        "order": {
                            "order_id": "local-net",
                            "broker_order_id": "broker-net",
                            "created_at": "2026-08-05T00:00:01Z",
                            "updated_at": "2026-08-05T00:00:01Z",
                        },
                    },
                ) as submit,
            ):
                controller._handle_cycle(
                    SimpleNamespace(decisions=(buy, sell))
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        submit.assert_called_once()
        intent = submit.call_args.args[1]
        self.assertEqual("BUY", intent.side)
        self.assertEqual(2.0, intent.quantity)
        plan_metadata = intent.metadata["portfolio_execution"]
        self.assertEqual(
            [{"intentId": "evaluation-buy-five", "sleeveId": "sleeve-a", "signedQuantity": "2"}],
            plan_metadata["allocations"],
        )
        self.assertEqual(
            {
                "sleeve-a": Decimal("3"),
                "sleeve-b": Decimal("-3"),
            },
            {
                item["sleeveId"]: Decimal(item["signedQuantity"])
                for item in plan_metadata["internalAllocations"]
            },
        )
        self.assertEqual(
            {
                "sleeve-a": {"005930": Decimal("3")},
                "sleeve-b": {"005930": Decimal("0")},
            },
            controller.portfolio_ledger.sleeve_holdings(
                controller.portfolio_execution_scope_id
            ),
        )

    def test_portfolio_lineage_requires_every_sleeve_not_only_lead(self) -> None:
        specs = (self._spec("sleeve-a", "strategy-a"), self._spec("sleeve-b", "strategy-b"))
        loaded = SimpleNamespace(
            payload={
                "strategyInstances": [
                    dict(spec.artifact) for spec in specs
                ]
            }
        )
        self.assertEqual(
            "",
            LiveContinuousController._loaded_portfolio_lineage_blocker(
                loaded, specs, require_complete=True
            ),
        )
        lead_only = LiveContinuousController._loaded_portfolio_lineage_blocker(
            loaded, specs[:1], require_complete=True
        )
        self.assertIn("missing=sleeve-b", lead_only)

        tampered = copy.copy(specs[1])
        object.__setattr__(tampered, "artifact_hash", "wrong-hash")
        hash_blocker = LiveContinuousController._loaded_portfolio_lineage_blocker(
            loaded, (specs[0], tampered), require_complete=True
        )
        self.assertIn("artifact hash", hash_blocker)

    def test_kis_snapshot_never_silently_drops_unsupported_or_corrupt_rows(self) -> None:
        original = copy.deepcopy(state.STATE.get("broker_reconciliation", {}))
        try:
            state.STATE["broker_reconciliation"] = {
                "positions": [
                    {
                        "broker_id": "kis",
                        "symbol": "AAPL",
                        "broker_qty": 1,
                    }
                ]
            }
            with self.assertRaisesRegex(ValueError, "해외/미지원"):
                LiveContinuousController._kis_broker_holdings()

            state.STATE["broker_reconciliation"] = {
                "positions": [
                    {
                        "broker_id": "kis",
                        "symbol": "005930.KS",
                        "broker_qty": "broken",
                    }
                ]
            }
            with self.assertRaisesRegex(ValueError, "수량을 검증"):
                LiveContinuousController._kis_broker_holdings()

            state.STATE["broker_reconciliation"] = {
                "positions": [
                    {
                        "broker_id": "kis",
                        "symbol": "AAPL",
                        "broker_qty": 0,
                    }
                ]
            }
            self.assertEqual({}, LiveContinuousController._kis_broker_holdings())
        finally:
            state.STATE["broker_reconciliation"] = original

    def test_pre_post_validator_rechecks_fresh_account_and_sleeve_base(self) -> None:
        controller, _specs = self._configured_controller()
        plan = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("intent-a", "sleeve-a", "005930", 1),
                SleeveTarget("intent-b", "sleeve-b", "005930", 1),
            ),
            current_positions={},
            broker_quantity=0,
        )
        intent = self._portfolio_intent(plan)
        router = MagicMock()
        router.list_positions.return_value = []
        with (
            patch.object(state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]),
            patch.object(state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]),
            patch.object(state, "LiveBrokerRouter", return_value=router),
        ):
            allowed, reason, report = (
                controller.validate_portfolio_execution_dispatch(intent)
            )
        self.assertTrue(allowed)
        self.assertEqual("portfolio-pre-post-validation-passed", reason)
        self.assertTrue(report["allowed"])
        router.list_positions.assert_called_once_with("kis")

        # The originally sealed plan said both sleeve bases were flat.  A
        # completed broker fill changes sleeve-a and the fresh KIS holding in
        # lockstep, so reconciliation succeeds but this stale plan must fail.
        seed = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("seed", "sleeve-a", "005930", 1),),
            current_positions={},
            broker_quantity=0,
        )
        controller.portfolio_ledger.record_accepted_order(
            seed,
            broker_order_id="seed-order",
        )
        controller.portfolio_ledger.apply_execution_events(
            controller.portfolio_execution_scope_id,
            [
                {
                    "event_id": "seed-fill",
                    "broker_id": "kis",
                    "broker_order_id": "seed-order",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 1,
                    "price": 70_000,
                    "fee": 0,
                    "state": "FILLED",
                }
            ],
        )
        router.list_positions.return_value = [
            {"broker_id": "kis", "symbol": "005930", "broker_qty": 1}
        ]
        with (
            patch.object(state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]),
            patch.object(state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]),
            patch.object(state, "LiveBrokerRouter", return_value=router),
        ):
            allowed, reason, report = (
                controller.validate_portfolio_execution_dispatch(intent)
            )
        self.assertFalse(allowed)
        self.assertIn("base sleeve position changed", reason)
        self.assertFalse(report["allowed"])

    def test_pre_post_validator_verifies_v2_internal_and_external_split(self) -> None:
        controller, _specs = self._configured_controller()
        seed = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("seed-b", "sleeve-b", "005930", 3),),
            current_positions={},
            broker_quantity=0,
        )
        controller.portfolio_ledger.record_accepted_order(
            seed,
            broker_order_id="seed-order",
        )
        controller.portfolio_ledger.apply_execution_events(
            controller.portfolio_execution_scope_id,
            [
                {
                    "event_id": "seed-fill",
                    "broker_id": "kis",
                    "broker_order_id": "seed-order",
                    "symbol": "005930",
                    "side": "BUY",
                    "quantity": 3,
                    "price": 90,
                    "fee": 0,
                    "state": "FILLED",
                    "occurred_at": "2026-08-05T00:00:00Z",
                }
            ],
        )
        plan = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("buy-five", "sleeve-a", "005930", 5),
                SleeveTarget("sell-three", "sleeve-b", "005930", 0),
            ),
            current_positions={
                ("sleeve-a", "005930"): 0,
                ("sleeve-b", "005930"): 3,
            },
            broker_quantity=3,
            reference_price=100,
        )
        intent = self._portfolio_intent(plan)
        router = MagicMock()
        router.list_positions.return_value = [
            {"broker_id": "kis", "symbol": "005930", "broker_qty": 3}
        ]
        with (
            patch.object(state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]),
            patch.object(state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]),
            patch.object(state, "LiveBrokerRouter", return_value=router),
        ):
            allowed, reason, _report = (
                controller.validate_portfolio_execution_dispatch(intent)
            )
        self.assertTrue(allowed, reason)

        tampered = self._portfolio_intent(plan)
        tampered.metadata["portfolio_execution"]["internalAllocations"][0][
            "signedQuantity"
        ] = "2"
        with patch.object(state, "LiveBrokerRouter", return_value=router):
            allowed, reason, _report = (
                controller.validate_portfolio_execution_dispatch(tampered)
            )
        self.assertFalse(allowed)
        self.assertIn("internal allocations", reason)

    def test_pre_post_validator_rejects_tampered_plan_and_malformed_holdings(self) -> None:
        controller, _specs = self._configured_controller()
        plan = build_symbol_net_plan(
            scope_id=controller.portfolio_execution_scope_id,
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(
                SleeveTarget("intent-a", "sleeve-a", "005930", 1),
                SleeveTarget("intent-b", "sleeve-b", "005930", 1),
            ),
            current_positions={},
            broker_quantity=0,
        )
        tampered = self._portfolio_intent(plan)
        tampered.metadata["portfolio_execution"]["sleeveDeltas"][0][
            "targetQuantity"
        ] = "2"
        router = MagicMock()
        router.list_positions.return_value = []
        with patch.object(state, "LiveBrokerRouter", return_value=router):
            allowed, reason, _report = (
                controller.validate_portfolio_execution_dispatch(tampered)
            )
        self.assertFalse(allowed)
        self.assertIn("does not conserve target", reason)
        router.list_positions.assert_not_called()

        valid = self._portfolio_intent(plan)
        router.list_positions.return_value = ["malformed-row"]
        with (
            patch.object(state.PROGRAM_LEDGER, "order_dispatch_rows", return_value=[]),
            patch.object(state.PROGRAM_LEDGER, "execution_event_rows", return_value=[]),
            patch.object(state, "LiveBrokerRouter", return_value=router),
        ):
            allowed, reason, _report = (
                controller.validate_portfolio_execution_dispatch(valid)
            )
        self.assertFalse(allowed)
        self.assertIn("잘못된 행", reason)

    def test_state_post_edge_block_never_calls_router(self) -> None:
        plan = build_symbol_net_plan(
            scope_id="live:portfolio-one:hash-one",
            portfolio_id="portfolio-one",
            portfolio_hash="hash-one",
            targets=(SleeveTarget("intent-a", "sleeve-a", "005930", 1),),
            current_positions={},
            broker_quantity=0,
        )
        intent = self._portfolio_intent(plan)
        order = {
            "order_id": "local-order",
            "idempotency_key": "idempotency-one",
            "dry_run": False,
            "canary_scope": {},
        }
        managed = SimpleNamespace(order_id="local-order")
        router = MagicMock()
        oms = SimpleNamespace(
            orders={"local-order": SimpleNamespace(status="REJECTED")}
        )
        with (
            patch.object(
                state,
                "live_broker_dispatch_allowed",
                return_value=(True, "allowed"),
            ),
            patch.object(
                state,
                "functional_test_dispatch_assessment",
                return_value=(True, "allowed", {}),
            ),
            patch.object(
                state,
                "exact_live_canary_scope_dispatch_allowed",
                return_value=(True, "allowed", {}),
            ),
            patch.object(
                state,
                "operational_runtime_dispatch_allowed",
                return_value=(True, "allowed", {}),
            ),
            patch.object(
                state,
                "live_broker_payload",
                return_value={"broker_id": "kis", "identifier": "idempotency-one"},
            ),
            patch.object(
                state.PROGRAM_LEDGER,
                "checkpoint_order_dispatch",
                return_value={"created": True, "order": {}},
            ),
            patch.object(
                state.PROGRAM_LEDGER,
                "update_order_dispatch",
                return_value=True,
            ),
            patch.object(state, "LIVE_OMS", oms),
            patch.object(state, "recovery_state_payload", return_value={}),
            patch.object(state.RECOVERY_JOURNAL, "save"),
            patch.object(
                state,
                "_block_managed_order_before_dispatch",
                return_value=(False, "stale-portfolio-plan"),
            ) as blocked,
            patch(
                "live_trader.continuous_live.validate_portfolio_execution_dispatch",
                return_value=(
                    False,
                    "stale-portfolio-plan",
                    {"allowed": False},
                ),
            ) as validate,
            patch.object(state, "LiveBrokerRouter", return_value=router),
        ):
            result = state.dispatch_live_order_with_checkpoint(
                order,
                intent,
                managed,
                trace_id="trace-one",
            )
        self.assertEqual((False, "stale-portfolio-plan"), result)
        validate.assert_called_once_with(intent)
        blocked.assert_called_once()
        router.place_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
