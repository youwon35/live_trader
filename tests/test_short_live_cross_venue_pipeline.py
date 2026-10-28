"""One manually confirmed scope across four routes; fake edges, no network/orders.

These are functional-path checks, never Paper qualification or actual broker
validation. The genuine service/strategy engine/authority/journal are used.
"""
from dataclasses import replace
from datetime import timedelta
import unittest
from unittest.mock import patch

import test_short_live_session as fixture


class CrossVenuePipelineTests(unittest.TestCase):
    def setUp(self):
        self.h = fixture.ShortLiveSessionTests()
        self.h.setUp()
        self.addCleanup(self.h.tearDown)
        for target in ("socket.create_connection", "socket.socket.connect"):
            guard = patch(target, side_effect=AssertionError("external network prohibited"))
            guard.start()
            self.addCleanup(guard.stop)
        self.specs = []
        # Keep the same 1h algorithm and two independent portfolio sleeves at
        # every route, including identical BTC exposure on Binance spot/futures.
        for route in fixture.ROUTE_INFO:
            for index in range(2):
                spec = fixture.runtime_spec(route, f"{route}-instance-{index}", "1h")
                spec["portfolioId"] = f"portfolio-{index}"
                self.specs.append(spec)
        self.service, self.brokers, self.feeds = self.h.service(self.specs)

        def krx_history(spec, limit):
            self.assertEqual(limit, 5)
            row = next(raw for raw in self.specs if raw["strategyInstanceId"] == spec.strategy_instance_id)
            prior = fixture.NOW - timedelta(days=1)
            ends = [prior + timedelta(hours=2), prior + timedelta(hours=3),
                    prior + timedelta(hours=3, minutes=30),
                    fixture.NOW - timedelta(hours=2), fixture.NOW - timedelta(hours=1)]
            result = [fixture.candle(row, end, 5-index) for index, end in enumerate(ends)]
            result[2] = replace(result[2], start_time=fixture.stamp(ends[2]-timedelta(minutes=30)))
            return result
        self.feeds["KIS_KR_CASH"].warmup = krx_history

    def start(self):
        return self.h.start(self.service)["runtime"]

    def enter(self, route):
        self.h.first_buy(self.service, next(row for row in self.specs if row["route"] == route))

    def test_four_routes_and_eight_sleeves_share_one_start_and_keep_exact_ownership(self):
        prepared = self.h.prepare(self.service)["prepared"]
        self.assertTrue(prepared["readyToStart"], prepared)
        self.assertFalse(any(b.submits for b in self.brokers.values()))
        snapshot = self.service.start({"preparationId": prepared["preparationId"],
            "scopeHash": prepared["scopeHash"], "confirmed": True, "executionModelOverrideHash":(prepared.get("executionModelBinding") or {}).get("bindingHash"),
            "confirmationText": fixture.CONFIRMATION_TEXT})["runtime"]
        self.assertEqual(snapshot["status"], "RUNNING")
        self.assertEqual(len(self.service._engines), 8)
        self.assertFalse(any(b.submits for b in self.brokers.values()))
        self.assertTrue(all(f.connected for f in self.feeds.values()))
        for route in fixture.ROUTE_INFO:
            before = {r: len(b.submits) for r, b in self.brokers.items()}
            self.enter(route)
            self.assertEqual(len(self.brokers[route].submits), 1)
            self.assertTrue(all(len(b.submits) == before[r] for r, b in self.brokers.items() if r != route))
            order = self.brokers[route].submits[0]
            self.assertEqual(order["route"], route)
            self.assertEqual(order["sessionId"], snapshot["sessionId"])
            self.assertEqual(order["symbol"], self.brokers[route].spec["symbol"])
            attribution = self.service._session["orders"][order["clientOrderId"]]["allocationWeights"]
            self.assertEqual(set(attribution), {s["strategyInstanceId"] for s in self.specs if s["route"] == route})
            self.assertTrue(all(abs(weight - 0.5) < 1e-9 for weight in attribution.values()))
        rows = self.service.snapshot(include_catalog=False)["runtime"]["strategyRows"]
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(row["positionQuantity"] > 0 and row["fillObservationCount"] == 1 for row in rows))
        self.assertEqual({row["timeframe"] for row in rows}, {"1h"})
        terminal = self.service.stop({})["runtime"]
        self.assertTrue(terminal["terminalVerified"], terminal)
        self.assertTrue(all(b.cleanups == 1 for b in self.brokers.values()))
        self.assertTrue(all(f.closed for f in self.feeds.values()))
        self.assertTrue(all(b.quantity == 0 for b in self.brokers.values()))

    def test_binance_spot_futures_share_cap_without_borrowing_kis_or_upbit_cash(self):
        self.start()
        contract = self.service._session["contract"]
        budgets = {leg["route"]: leg["routeBudgetKrw"] for leg in contract["legs"]}
        self.assertEqual(budgets, {"KIS_KR_CASH": 100000, "BINANCE_SPOT": 50000,
            "BINANCE_USDM_FUTURES": 50000, "UPBIT_KRW_SPOT": 100000})
        for route in fixture.ROUTE_INFO:
            self.enter(route)
        self.assertEqual(self.brokers["BINANCE_SPOT"].exposure + self.brokers["BINANCE_USDM_FUTURES"].exposure, 100000)
        self.assertEqual(self.brokers["KIS_KR_CASH"].exposure, 100000)
        self.assertEqual(self.brokers["UPBIT_KRW_SPOT"].exposure, 100000)

    def test_empty_one_venue_funds_blocks_entire_preparation_without_submissions(self):
        self.brokers["UPBIT_KRW_SPOT"].available = 0
        result = self.h.prepare(self.service)["prepared"]
        self.assertFalse(result["readyToStart"])
        self.assertTrue(any("UPBIT_KRW_SPOT:FUNDING" in reason for reason in result["blockers"]))
        self.assertFalse(any(b.submits for b in self.brokers.values()))
        self.assertFalse(any(f.connected for f in self.feeds.values()))

    def test_partial_order_holds_all_other_routes_and_is_not_resubmitted(self):
        self.start()
        self.brokers["BINANCE_SPOT"].status = "PARTIAL"
        self.enter("BINANCE_SPOT")
        self.assertEqual(self.service._session["status"], "PENDING_FILL")
        for route in ("KIS_KR_CASH", "UPBIT_KRW_SPOT", "BINANCE_USDM_FUTURES"):
            self.enter(route)
            self.assertEqual(self.brokers[route].submits, [])
        self.service.tick()
        self.assertEqual(len(self.brokers["BINANCE_SPOT"].submits), 1)
        self.assertTrue(self.service._session["newEntriesBlocked"])

    def test_lost_reply_stops_scope_without_ordering_other_venues_or_retrying(self):
        self.start()
        self.brokers["BINANCE_SPOT"].status = "RAISE"
        self.enter("BINANCE_SPOT")
        first = self.service.snapshot(include_catalog=False)["runtime"]
        self.assertEqual(first["firstFailure"], "ORDER_RESULT_UNKNOWN_NO_RETRY")
        self.assertTrue(first["newEntriesBlocked"])
        self.service.tick()
        self.service.stop({})
        self.assertEqual(len(self.brokers["BINANCE_SPOT"].submits), 1)
        self.assertTrue(all(not b.submits for r, b in self.brokers.items() if r != "BINANCE_SPOT"))
        self.assertFalse(self.service.snapshot(include_catalog=False)["runtime"]["terminalVerified"])

    def test_binance_loss_is_aggregated_across_spot_and_futures(self):
        self.start()
        self.brokers["BINANCE_SPOT"].loss = 30000
        self.brokers["BINANCE_USDM_FUTURES"].loss = 20000
        runtime = self.service.tick()["runtime"]
        self.assertEqual(runtime["firstFailure"], "BINANCE:DAILY_LOSS_STOP_REACHED")
        self.assertTrue(runtime["newEntriesBlocked"])
        self.assertFalse(any(b.submits for b in self.brokers.values()))
        self.assertTrue(all(f.closed for f in self.feeds.values()))

    def test_failure_of_one_route_cannot_publish_flat_terminal_for_other_routes(self):
        self.start()
        self.enter("UPBIT_KRW_SPOT")
        self.brokers["UPBIT_KRW_SPOT"].cleanup_complete = False
        runtime = self.service.stop({})["runtime"]
        self.assertFalse(runtime["terminalVerified"])
        self.assertTrue(runtime["newEntriesBlocked"])
        self.assertEqual(self.brokers["UPBIT_KRW_SPOT"].cleanups, 1)
        self.assertTrue(all(f.closed for f in self.feeds.values()))
        self.assertFalse(list(self.service.store_root.glob("session-terminal-*.json")))

    def test_account_binding_change_on_any_route_rejects_whole_start(self):
        prepared = self.h.prepare(self.service)["prepared"]
        adapter = self.brokers["UPBIT_KRW_SPOT"]
        original = adapter.readiness
        adapter.readiness = lambda manifest: {**original(manifest), "accountId": "different-fixture-account"}
        with self.assertRaisesRegex(fixture.ShortLiveContractError, "ACCOUNT_CHANGED"):
            self.service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                "confirmed": True, "executionModelOverrideHash":(prepared.get("executionModelBinding") or {}).get("bindingHash"), "confirmationText": fixture.CONFIRMATION_TEXT})
        self.assertFalse(any(b.submits for b in self.brokers.values()))
        self.assertFalse(any(f.connected for f in self.feeds.values()))


if __name__ == "__main__":
    unittest.main()
