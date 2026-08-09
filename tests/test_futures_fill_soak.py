from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from live_trader.futures_fill_soak import (
    FAIL,
    PASS,
    BinanceFuturesFillSoakSession,
    FillSoakConfig,
    ImmutableJsonReportWriter,
    LiveOrderAuthorization,
)


class FakeClock:
    def __init__(self) -> None:
        self.value = 1_000.0
        self.epoch = 2_000_000_000.0

    def monotonic(self) -> float:
        return self.value

    def time(self) -> float:
        return self.epoch + self.value

    def sleep(self, seconds: float) -> None:
        self.value += max(0.0, seconds)

    def utcnow(self) -> datetime:
        return datetime(2033, 5, 18, tzinfo=timezone.utc) + timedelta(
            seconds=self.value
        )


class MemoryReportWriter:
    def __init__(self) -> None:
        self.documents: list[dict[str, object]] = []
        self.session_ids: set[str] = set()

    def ensure_available(self, session_id: str) -> None:
        if session_id in self.session_ids:
            raise FileExistsError(session_id)

    def write(
        self,
        report: dict[str, object],
    ) -> tuple[dict[str, object], str]:
        session_id = str(report["session_id"])
        self.ensure_available(session_id)
        canonical = json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        document = {
            **report,
            "report_sha256": hashlib.sha256(canonical).hexdigest(),
        }
        self.session_ids.add(session_id)
        self.documents.append(document)
        return document, f"memory://{session_id}.json"


class ExplodingRouter:
    def __init__(self) -> None:
        self.calls = 0

    def __getattr__(self, name: str):
        def explode(*_args, **_kwargs):
            self.calls += 1
            raise AssertionError(f"unexpected live boundary call: {name}")

        return explode


class FakeRouter:
    def __init__(
        self,
        *,
        hanging_entry: bool = False,
        ambiguous_entry_fill: bool = False,
        drawdown_after_round_trips: int | None = None,
    ) -> None:
        self.available = Decimal("25")
        self.equity = Decimal("25")
        self.positions: list[dict[str, object]] = []
        self.orders: dict[str, dict[str, object]] = {}
        self.place_calls: list[dict[str, object]] = []
        self.cancel_calls: list[str] = []
        self.read_calls = 0
        self.round_trips = 0
        self.hanging_entry = hanging_entry
        self.ambiguous_entry_fill = ambiguous_entry_fill
        self.drawdown_after_round_trips = drawdown_after_round_trips
        self._ambiguous_raised = False
        self._next_order_id = 100

    def _effective_equity(self) -> Decimal:
        if (
            self.drawdown_after_round_trips is not None
            and self.round_trips >= self.drawdown_after_round_trips
        ):
            return Decimal("22.49")
        return self.equity

    def get_binance_futures_canary_observation(
        self,
        _symbol: str,
    ) -> dict[str, object]:
        self.read_calls += 1
        return {
            "account": {
                "can_trade": True,
                "available_usdt": float(self.available),
                "available_usdt_known": True,
            },
            "position_mode": {"dual_side_position": True},
            "symbol_config": {
                "symbol": "BTCUSDT",
                "margin_type": "ISOLATED",
                "leverage": 1,
            },
            "position_count": len(self.positions),
            "open_order_count": len(self.list_open_orders("binance-futures")),
        }

    def get_account_snapshot(
        self,
        _broker_id: str,
    ) -> dict[str, object]:
        self.read_calls += 1
        equity = self._effective_equity()
        return {
            "broker_id": "binance-futures",
            "accounts": [
                {
                    "currency": "USDT",
                    "broker_cash": float(self.available),
                    "wallet_balance": float(equity),
                    "margin_balance": float(equity),
                    "broker_equity": float(equity),
                }
            ],
        }

    def list_positions(
        self,
        _broker_id: str,
    ) -> list[dict[str, object]]:
        self.read_calls += 1
        return [dict(item) for item in self.positions]

    def list_open_orders(
        self,
        _broker_id: str,
        *,
        symbol: str = "",
    ) -> list[dict[str, object]]:
        self.read_calls += 1
        return [
            dict(item)
            for item in self.orders.values()
            if item["status"] in {"NEW", "PARTIALLY_FILLED"}
            and (not symbol or item["symbol"] == symbol)
        ]

    def place_order(
        self,
        intent: dict[str, object],
    ) -> dict[str, object]:
        self.place_calls.append(dict(intent))
        self._next_order_id += 1
        client_id = str(intent["identifier"])
        side = str(intent["side"])
        quantity = Decimal(str(intent["quantity"]))
        should_hang = self.hanging_entry and side == "SELL"
        status = "NEW" if should_hang else "FILLED"
        row: dict[str, object] = {
            "symbol": "BTCUSDT",
            "orderId": self._next_order_id,
            "clientOrderId": client_id,
            "status": status,
            "executedQty": "0" if should_hang else str(quantity),
            "avgPrice": "65000",
        }
        self.orders[client_id] = row
        if status == "FILLED":
            if side == "SELL":
                if self.positions:
                    raise AssertionError("entry submitted while position open")
                self.positions = [
                    {
                        "symbol": "BTCUSDT",
                        "position_side": "SHORT",
                        "broker_qty": -float(quantity),
                    }
                ]
            else:
                if not self.positions:
                    raise AssertionError("cover submitted while flat")
                self.positions = []
                self.round_trips += 1
        if (
            self.ambiguous_entry_fill
            and side == "SELL"
            and not self._ambiguous_raised
        ):
            self._ambiguous_raised = True
            raise TimeoutError("simulated lost acknowledgement")
        return {"ok": True, "status": 200, "json": dict(row), "text": ""}

    def get_order_status(
        self,
        _broker_id: str,
        *,
        symbol: str,
        broker_order_id: str,
        client_order_id: bool = False,
    ) -> dict[str, object]:
        del symbol
        if client_order_id:
            return dict(self.orders[broker_order_id])
        for row in self.orders.values():
            if str(row["orderId"]) == str(broker_order_id):
                return dict(row)
        raise LookupError(broker_order_id)

    def cancel_order(
        self,
        _broker_id: str,
        broker_order_id: str,
        **context: object,
    ) -> dict[str, object]:
        client_order_id = bool(context.get("client_order_id"))
        if client_order_id:
            row = self.orders[broker_order_id]
        else:
            row = next(
                item
                for item in self.orders.values()
                if str(item["orderId"]) == str(broker_order_id)
            )
        row["status"] = "CANCELED"
        self.cancel_calls.append(str(row["clientOrderId"]))
        return {"ok": True, "json": dict(row)}


def config(**overrides: object) -> FillSoakConfig:
    values: dict[str, object] = {
        "session_id": "bfsoak-unit-test-0001",
        "duration_seconds": 30,
        "fill_timeout_seconds": 10,
        "poll_interval_seconds": 1,
        "monitor_interval_seconds": 10,
    }
    values.update(overrides)
    return FillSoakConfig(**values)


def authorization(clock: FakeClock) -> LiveOrderAuthorization:
    return LiveOrderAuthorization(
        confirmed=True,
        token_fingerprint="0123456789abcdef",
        issued_at_epoch=clock.time() - 5,
        expires_at_epoch=clock.time() + 60,
    )


def rules(_symbol: str) -> dict[str, Decimal]:
    return {
        "minQty": Decimal("0.0001"),
        "maxQty": Decimal("1000"),
        "stepSize": Decimal("0.0001"),
        "minNotional": Decimal("5"),
    }


class FuturesFillSoakTests(unittest.TestCase):
    def build_session(
        self,
        router: object,
        clock: FakeClock,
        *,
        session_config: FillSoakConfig | None = None,
    ) -> BinanceFuturesFillSoakSession:
        return BinanceFuturesFillSoakSession(
            session_config or config(),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("50000"),
            rules_provider=rules,
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
            dispatch_authorizer=lambda _intent: (True, "test-authorized", {}),
        )

    def test_preview_is_read_only_and_exposes_exact_start_facts(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = self.build_session(router, clock)

        preview = session.preview()

        self.assertTrue(preview["ready"])
        self.assertEqual("25", preview["available_usdt"])
        self.assertEqual("25", preview["equity_usdt"])
        self.assertTrue(preview["hedge_mode"])
        self.assertEqual("ISOLATED", preview["margin_type"])
        self.assertEqual("1", preview["leverage"])
        self.assertEqual([], preview["positions"])
        self.assertEqual([], preview["open_orders"])
        self.assertEqual(
            "0.0001",
            preview["minimum_order"]["quantity"],
        )
        self.assertEqual(
            "5",
            preview["minimum_order"]["estimated_notional_usdt"],
        )
        self.assertEqual("25", preview["initial_available_cap_usdt"])
        self.assertEqual([], router.place_calls)
        self.assertEqual([], router.cancel_calls)

    def test_preview_uses_exchange_minimum_quantity_when_still_under_cap(
        self,
    ) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = BinanceFuturesFillSoakSession(
            config(session_id="bfsoak-min-quantity-01"),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("30000"),
            rules_provider=lambda _symbol: {
                "minQty": Decimal("0.0002"),
                "maxQty": Decimal("1000"),
                "stepSize": Decimal("0.0001"),
                "minNotional": Decimal("5"),
            },
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
            dispatch_authorizer=lambda _intent: (True, "test-authorized", {}),
        )

        preview = session.preview()

        self.assertTrue(preview["ready"])
        self.assertEqual(
            "0.0002",
            preview["minimum_order"]["quantity"],
        )
        self.assertEqual(
            "6",
            preview["minimum_order"]["estimated_notional_usdt"],
        )

    def test_missing_operational_authorizer_blocks_before_place_order(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = BinanceFuturesFillSoakSession(
            config(session_id="bfsoak-no-operational-gate"),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("50000"),
            rules_provider=rules,
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
        )

        report = session.run(authorization(clock))

        self.assertEqual("FAIL", report["status"])
        self.assertIn(
            "operational-dispatch-authorizer-missing",
            report["reason_ids"],
        )
        self.assertEqual([], router.place_calls)

    def test_stale_final_binding_authorization_blocks_before_place_order(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = BinanceFuturesFillSoakSession(
            config(session_id="bfsoak-stale-final-binding"),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("50000"),
            rules_provider=rules,
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
            dispatch_authorizer=lambda _intent: (
                False,
                "operational-paper-final-binding-changed",
                {},
            ),
        )

        report = session.run(authorization(clock))

        self.assertEqual("FAIL", report["status"])
        self.assertIn(
            "operational-dispatch-authorization-blocked",
            report["reason_ids"],
        )
        self.assertEqual([], router.place_calls)

    def test_active_kill_boundary_blocks_entry_before_router_post(self) -> None:
        clock = FakeClock()
        router = FakeRouter()

        @contextmanager
        def active_kill():
            yield {"active": True, "revision": "kill-1"}

        session = BinanceFuturesFillSoakSession(
            config(session_id="bfsoak-active-kill-entry"),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("50000"),
            rules_provider=rules,
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
            dispatch_authorizer=lambda _intent: (True, "test-authorized", {}),
            dispatch_boundary=active_kill,
        )

        report = session.run(authorization(clock))

        self.assertEqual(FAIL, report["status"])
        self.assertIn(
            "emergency-stop-latch-broker-dispatch-forbidden",
            report["reason_ids"],
        )
        self.assertEqual([], router.place_calls)

    def test_active_kill_boundary_preserves_verified_reduce_only_cover(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        router.positions = [
            {
                "symbol": "BTCUSDT",
                "position_side": "SHORT",
                "broker_qty": -0.0001,
            }
        ]

        @contextmanager
        def active_kill():
            yield {"active": True, "revision": "kill-2"}

        authorized: list[dict[str, object]] = []

        def authorize(intent: dict[str, object]):
            authorized.append(dict(intent))
            return True, "verified-current-position-reduction", {}

        session = BinanceFuturesFillSoakSession(
            config(session_id="bfsoak-active-kill-cover"),
            router=router,
            clock=clock,
            price_provider=lambda _symbol: Decimal("50000"),
            rules_provider=rules,
            live_orders_enabled=lambda: True,
            report_writer=MemoryReportWriter(),
            dispatch_authorizer=authorize,
            dispatch_boundary=active_kill,
        )
        intent = {
            "broker_id": "binance-futures",
            "strategy_id": "strategy-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": "0.0001",
            "qty": "0.0001",
            "position_direction": "short",
            "risk_reducing": True,
            "reduce_only": True,
            "soak_leg": "recovery-cover",
            "identifier": "ltfs-recovery-cover-test",
        }

        response = session._place_order_at_dispatch_boundary(intent)

        self.assertTrue(response["ok"])
        self.assertEqual(1, len(authorized))
        self.assertEqual(1, len(router.place_calls))
        self.assertEqual([], router.positions)

    def test_run_revalidates_preview_and_blocks_new_position(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = self.build_session(router, clock)
        self.assertTrue(session.preview()["ready"])
        router.positions = [
            {
                "symbol": "ETHUSDT",
                "position_side": "LONG",
                "broker_qty": 0.01,
            }
        ]

        report = session.run(authorization(clock))

        self.assertEqual(FAIL, report["status"])
        self.assertIn(
            "preflight-position-not-flat",
            report["reason_ids"],
        )
        self.assertEqual([], router.place_calls)

    def test_three_round_trips_complete_then_flat_soak_passes(self) -> None:
        clock = FakeClock()
        router = FakeRouter()
        session = self.build_session(router, clock)

        report = session.run(authorization(clock))

        self.assertEqual(PASS, report["status"])
        self.assertEqual(3, report["progress"]["round_trips_completed"])
        self.assertEqual(6, report["progress"]["fill_count"])
        self.assertTrue(report["final_checks"]["duration_complete"])
        self.assertTrue(report["final_checks"]["flat"])
        self.assertTrue(report["final_checks"]["open_orders_clear"])
        self.assertFalse(report["strategy_promotion_authorized"])
        self.assertEqual(
            ["SELL", "BUY", "SELL", "BUY", "SELL", "BUY"],
            [str(item["side"]) for item in router.place_calls],
        )
        identifiers = [
            str(item["identifier"]) for item in router.place_calls
        ]
        self.assertEqual(6, len(set(identifiers)))
        self.assertTrue(
            all(item["max_leverage"] == 1 for item in router.place_calls)
        )
        self.assertTrue(
            all(
                item["required_margin_type"] == "ISOLATED"
                for item in router.place_calls
            )
        )
        self.assertTrue(
            all(
                Decimal(str(item["quantity"])) * Decimal("50000")
                <= Decimal("10")
                for item in router.place_calls
            )
        )

    def test_invalid_authorization_makes_no_broker_or_network_call(self) -> None:
        clock = FakeClock()
        router = ExplodingRouter()
        session = self.build_session(router, clock)
        denied = LiveOrderAuthorization(
            confirmed=False,
            token_fingerprint="0123456789abcdef",
            issued_at_epoch=clock.time() - 5,
            expires_at_epoch=clock.time() + 60,
        )

        report = session.run(denied)

        self.assertEqual(FAIL, report["status"])
        self.assertIn(
            "operator-confirmation-required",
            report["reason_ids"],
        )
        self.assertEqual(0, router.calls)

    def test_ambiguous_filled_entry_fails_closed_and_recovers_flat(self) -> None:
        clock = FakeClock()
        router = FakeRouter(ambiguous_entry_fill=True)
        session = self.build_session(router, clock)

        report = session.run(authorization(clock))

        self.assertEqual(FAIL, report["status"])
        self.assertIn("order-submit-ambiguous", report["reason_ids"])
        self.assertEqual(["SELL", "BUY"], [
            str(item["side"]) for item in router.place_calls
        ])
        self.assertEqual([], router.positions)
        self.assertTrue(report["final_checks"]["flat"])
        self.assertEqual(
            "recovery-cover",
            report["orders"][-1]["leg"],
        )

    def test_deadline_cancels_hanging_entry_and_finishes_flat(self) -> None:
        clock = FakeClock()
        router = FakeRouter(hanging_entry=True)
        session = self.build_session(
            router,
            clock,
            session_config=config(
                duration_seconds=3,
                fill_timeout_seconds=20,
                monitor_interval_seconds=2,
            ),
        )

        report = session.run(authorization(clock))

        self.assertEqual(FAIL, report["status"])
        self.assertIn("session-deadline-reached", report["reason_ids"])
        self.assertEqual(1, len(router.cancel_calls))
        self.assertEqual([], router.positions)
        self.assertTrue(report["final_checks"]["flat"])
        self.assertTrue(report["final_checks"]["open_orders_clear"])

    def test_ten_percent_drawdown_halts_and_never_leaves_position(self) -> None:
        clock = FakeClock()
        router = FakeRouter(drawdown_after_round_trips=1)
        session = self.build_session(router, clock)

        report = session.run(authorization(clock))

        self.assertEqual(FAIL, report["status"])
        self.assertIn(
            "daily-drawdown-limit-reached",
            report["reason_ids"],
        )
        self.assertGreaterEqual(
            Decimal(report["risk"]["max_drawdown_pct"]),
            Decimal("10"),
        )
        self.assertEqual([], router.positions)
        self.assertEqual(2, len(router.place_calls))

    def test_immutable_writer_hashes_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            writer = ImmutableJsonReportWriter(directory)
            report = {
                "schema_version": "test",
                "session_id": "bfsoak-immutable-001",
                "status": FAIL,
            }

            document, path = writer.write(report)
            stored = json.loads(Path(path).read_text(encoding="utf-8"))
            canonical = json.dumps(
                report,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")

            self.assertEqual(
                hashlib.sha256(canonical).hexdigest(),
                document["report_sha256"],
            )
            self.assertEqual(document, stored)
            with self.assertRaises(FileExistsError):
                writer.write(report)


if __name__ == "__main__":
    unittest.main()
