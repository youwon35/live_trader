"""Synthetic-only regression: no broker credentials, network or real orders."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from trading_runtime.continuous_runtime import ClosedBar, OpenBoundary, PortfolioRuntimeEngine, RuntimeStrategySpec
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator
from live_trader.short_live_contract import (
    CONFIRMATION_TEXT, ShortLiveContractError, build_contract, canonical_hash,
    verify_contract,
)
from live_trader.short_live_session import ShortLiveTestService
from live_trader.short_live_order_authority import _reset_for_tests

NOW = datetime(2026, 10, 22, 3, 0, tzinfo=timezone.utc)
ROUTE_INFO = {
    "KIS_KR_CASH": ("kis", "kis", "005930"),
    "BINANCE_SPOT": ("binance", "binance", "BTCUSDT"),
    "BINANCE_USDM_FUTURES": ("binance-futures", "binance-futures", "BTCUSDT.PERP"),
    "UPBIT_KRW_SPOT": ("upbit", "upbit", "KRW-BTC"),
}


def stamp(value):
    return value.isoformat().replace("+00:00", "Z")


def runtime_spec(route="BINANCE_SPOT", instance="instance-1", timeframe="5m", timing="next-open"):
    broker, provider, symbol = ROUTE_INFO[route]
    return {
        "portfolioId": "portfolio-1", "portfolioHash": "a" * 64,
        "strategyInstanceId": instance, "strategyId": "source-" + instance,
        "artifactHash": "b" * 64, "pluginId": "moving_average_cross",
        "instrumentId": symbol, "symbol": symbol, "timeframe": timeframe,
        "provider": provider, "brokerId": broker, "route": route,
        "targetWeight": 1.0, "parameters": {"shortMa": 1, "longMa": 3},
        "executionTiming": timing,
    }


def candle(spec, end, close):
    seconds = {"5m": 300, "1h": 3600, "1d": 86400}[spec["timeframe"]]
    return ClosedBar(
        instrument_id=spec["instrumentId"], symbol=spec["symbol"],
        provider=spec["provider"], timeframe=spec["timeframe"],
        start_time=stamp(end - timedelta(seconds=seconds)), end_time=stamp(end),
        open=close, high=close, low=close, close=close, volume=1,
        received_time=stamp(end + timedelta(milliseconds=100)),
    )


def opening(spec, at, price, **changes):
    provider = spec["provider"]
    names = {"binance": "BINANCE_WEBSOCKET", "kis": "KIS_WEBSOCKET",
             "upbit": "UPBIT_WEBSOCKET", "binance-futures": "BINANCE_WEBSOCKET"}
    values = {
        "instrument_id": spec["instrumentId"], "symbol": spec["symbol"],
        "provider": provider, "timeframe": spec["timeframe"],
        "start_time": stamp(at), "open": price,
        "observed_time": stamp(at + timedelta(milliseconds=100)),
        "raw_hash": "c" * 64, "attestation": names[provider],
        "source_sequence": "fixture-exact-frame",
        "promotion_eligible": False, "recovery_only": False,
    }
    values.update(changes)
    return OpenBoundary(**values)


class Catalog:
    def __init__(self, path, specs):
        self.path = path
        self.specs = specs

    def snapshot(self):
        return {"candidates": [{"selectionKey": "exact", "artifactFileSha256": "d" * 64}], "coverage": []}

    def resolve(self, selections):
        if selections != [{"selectionKey": "exact", "artifactFileSha256": "d" * 64}]:
            raise ValueError("exact-selection-required")
        return {"targets": [{"selectionKey": "exact", "kind": "STRATEGY_INSTANCE"}],
                "specs": deepcopy(self.specs),
                "sourcePins": [{"path": str(self.path),
                                "sha256": hashlib.sha256(self.path.read_bytes()).hexdigest()}]}


class Broker:
    def __init__(self, route, spec):
        self.route = route
        self.spec = spec
        self.quantity = 0.0
        self.exposure = 0.0
        self.submits = []
        self.cleanups = 0
        self.available = 200_000.0
        self.loss = 0.0
        self.status = "FILLED"
        self.reconcile_known = True
        self.cleanup_complete = True
        self.over_size = False
        self.observations = []
        self.fee_known = True
        self.last_order = None

    def readiness(self, manifest):
        return {"ready": True, "accountId": "fixture-binance" if "BINANCE" in self.route else "fixture-" + self.route,
                "observedFuturesSettingsBySymbol": {self.spec["symbol"]: {"marginMode": "ISOLATED", "leverage": "1", "positionMode": "ONE_WAY"}},
                "scopeOwnershipVerified": True, "availableKrw": self.available,
                "existingExposureKrw": 0, "blockers": [],
                "boundedCostAllowanceVerified": True, "actualEquityLossTracked": True}

    def size_order(self, proposal):
        notional = proposal["notionalKrw"]
        quantity = proposal.get("closeQuantity", notional / proposal["referencePrice"])
        if proposal["riskReducing"]:
            notional = quantity*proposal["referencePrice"]
        projected = self.exposure + (-notional if proposal["riskReducing"] else notional)
        return {"ready": True, "quantity": quantity,
                "referencePrice": proposal["referencePrice"], "feeKrw": 0,
                "estimatedNotionalKrw": notional,
                "projectedGrossExposureKrw": 200_001 if self.over_size else max(0, projected)}

    def submit(self, order):
        self.submits.append(deepcopy(order))
        self.last_order = deepcopy(order)
        if self.status == "RAISE":
            raise TimeoutError("fixture lost reply")
        if self.status == "FILLED":
            sign = -1 if order["positionDirection"] == "short" else 1
            self.quantity += sign * order["quantity"] * (-1 if order["riskReducing"] else 1)
            self.exposure = max(0, self.exposure + (-order["estimatedNotionalKrw"] if order["riskReducing"] else order["estimatedNotionalKrw"]))
        return {"status": self.status, "brokerOrderId": "fixture-order-" + str(len(self.submits)),
                "clientOrderId": order["clientOrderId"], "filledQuantity": order["quantity"] if self.status == "FILLED" else 0,
                "fillPrice": order["referencePrice"], "feeKrw": 0 if self.fee_known else None,
                "actualFeeKnown": self.fee_known, "baseCommissionQuantity": 0, "baseCommissionKnown": True}

    def observe_order(self, client_id):
        if self.status in {"UNKNOWN", "RAISE"} and not self.observations:
            return {"status": "UNKNOWN", "clientOrderId": client_id}
        if self.observations:
            fraction, status = self.observations.pop(0)
            order = self.last_order
            self.quantity = order["quantity"] * fraction
            self.exposure = order["estimatedNotionalKrw"] * fraction
            return {"status": status, "brokerOrderId": "fixture-order-1",
                    "clientOrderId": client_id, "filledQuantity": self.quantity,
                    "fillPrice": order["referencePrice"], "feeKrw": 0,
                    "actualFeeKnown": True, "baseCommissionQuantity": 0, "baseCommissionKnown": True}
        return {"status": "ACCEPTED", "brokerOrderId": "fixture-order-1",
                "clientOrderId": client_id, "filledQuantity": self.quantity,
                "fillPrice": self.last_order["referencePrice"], "feeKrw": 0,
                "baseCommissionQuantity": 0, "baseCommissionKnown": True}

    def reconcile(self, session):
        return {"ready": self.reconcile_known, "known": self.reconcile_known,
                "protectionRetirementVerified": not self.quantity,
                "exposureKrw": self.exposure, "realizedLossKrw": self.loss,
                "unrealizedLossKrw": 0, "equityKrw": 100_000,
                "ownedOrders": [], "ownedPositions": [{"quantity": self.quantity}] if self.exposure else [],
                "positionsBySymbol": {self.spec["symbol"]: {"quantity": self.quantity, "exposureKrw": self.exposure}}}

    def cleanup(self, session):
        self.cleanups += 1
        if self.cleanup_complete:
            self.quantity = self.exposure = 0.0
        return {"complete": self.cleanup_complete, "ownedOrdersRemaining": 0,
                "ownedPositionsRemaining": 0 if self.cleanup_complete else 1, "blockers": []}


class Feed:
    def __init__(self, spec):
        self.spec = spec
        self.connected = False
        self.closed = False
        self.short = False
        self.rows = []

    def connect(self):
        self.connected = True

    def warmup(self, spec, limit):
        row = self.spec
        seconds = {"5m": 300, "1h": 3600, "1d": 86400}[row["timeframe"]]
        count = limit - 1 if self.short else limit
        return [candle(row, NOW - timedelta(seconds=seconds * (count - i)), float(count - i))
                for i in range(count)]

    def poll(self, timeout):
        rows, self.rows = self.rows, []
        return rows

    def close(self):
        self.closed = True
        return True


class ShortLiveSessionTests(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "artifact.json"
        self.source.write_text('{"fixture":true}', encoding="utf-8")
        self.now = NOW
        self.mono = 10.0
        self.owner = True
        self.guard = True
        self.services = []

    def tearDown(self):
        for service in self.services:
            service.close()
        self.temp.cleanup()
        _reset_for_tests()

    def service(self, specs=None):
        specs = specs or [runtime_spec()]
        catalog = Catalog(self.source, specs)
        brokers = {row["route"]: Broker(row["route"], row) for row in specs}
        feeds = {row["route"]: Feed(row) for row in specs}
        service = ShortLiveTestService(
            catalog, brokers.__getitem__, lambda manifest: feeds[manifest["route"]],
            self.root / ("store-" + str(len(self.services))), clock=lambda: self.now,
            monotonic=lambda: self.mono, owner_is_alive=lambda: self.owner,
            clock_guard=lambda _: {"ready": self.guard, "blockers": [] if self.guard else ["CLOCK_FAILED"]},
            background=False,
        )
        self.services.append(service)
        return service, brokers, feeds

    def prepare(self, service, **changes):
        return service.prepare({"selections": [{"selectionKey": "exact", "artifactFileSha256": "d" * 64}],
                                "futuresPolicy": {"leverage": 1, "marginMode": "ISOLATED", "stopLossPercent": 2},
                                "durationHours": 6, **changes})

    def start(self, service):
        prepared = self.prepare(service)["prepared"]
        return service.start({"preparationId": prepared["preparationId"],
                              "scopeHash": prepared["scopeHash"], "confirmed": True,
                              "confirmationText": CONFIRMATION_TEXT})

    def first_buy(self, service, spec):
        self.now = NOW + timedelta(milliseconds=200)
        service.step(spec["route"], candle(spec, NOW, 6))
        service.step(spec["route"], opening(spec, NOW, 7))

    def test_contract_all_routes_and_timeframes_are_explicit_nonpromotion(self):
        for route in ROUTE_INFO:
            for timeframe in ("5m", "1h", "1d"):
                for count in (1, 2):
                    with self.subTest(route=route, timeframe=timeframe, count=count):
                        specs = [runtime_spec(route, f"i-{i}", timeframe) for i in range(count)]
                        service, _, _ = self.service(specs)
                        snapshot = self.prepare(service)
                        contract = service._prepared["contract"]
                        verify_contract(contract)
                        self.assertEqual(False, contract["promotionEligible"])
                        self.assertEqual(False, contract["automaticRestartAllowed"])
                        self.assertEqual(6, contract["durationHours"])
                        self.assertTrue(snapshot["prepared"]["readyToStart"])

    def test_duration_budget_and_loss_limits_cannot_be_widened(self):
        for change in ({"durationHours": 4}, {"durationHours": 8},
                       {"capitalKrwPerBroker": 100001}, {"dailyLossStopKrwPerBroker": 50001},
                       {"durationHours": float("nan")}):
            with self.subTest(change=change):
                service, _, _ = self.service()
                with self.assertRaises(ShortLiveContractError):
                    self.prepare(service, **change)

    def test_manual_confirmation_scope_and_fresh_prepare_required(self):
        service, brokers, _ = self.service()
        prepared = self.prepare(service)["prepared"]
        with self.assertRaisesRegex(ShortLiveContractError, "CONFIRMATION"):
            service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"]})
        with self.assertRaisesRegex(ShortLiveContractError, "SCOPE_MISMATCH"):
            service.start({"preparationId": prepared["preparationId"], "scopeHash": "wrong",
                           "confirmed": True, "confirmationText": CONFIRMATION_TEXT})
        self.now += timedelta(minutes=3)
        with self.assertRaisesRegex(ShortLiveContractError, "EXPIRED"):
            service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                           "confirmed": True, "confirmationText": CONFIRMATION_TEXT})
        self.assertFalse(any(b.submits for b in brokers.values()))

    def test_funding_guard_owner_and_source_drift_block(self):
        for problem in ("funding", "clock", "owner", "source"):
            with self.subTest(problem=problem):
                service, brokers, _ = self.service()
                if problem == "funding":
                    brokers["BINANCE_SPOT"].available = 0
                if problem == "clock":
                    self.guard = False
                if problem == "owner":
                    self.owner = False
                prepared = self.prepare(service)["prepared"]
                if problem == "source":
                    self.source.write_text("changed", encoding="utf-8")
                with self.assertRaises(ShortLiveContractError):
                    service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                                   "confirmed": True, "confirmationText": CONFIRMATION_TEXT})
                self.assertEqual([], brokers["BINANCE_SPOT"].submits)
                self.guard = self.owner = True

    def test_next_open_never_submits_at_signal_close_and_duplicate_is_once(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        self.now += timedelta(milliseconds=200)
        bar = candle(spec, NOW, 6)
        service.step("BINANCE_SPOT", bar)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)
        boundary = opening(spec, NOW, 7)
        service.step("BINANCE_SPOT", boundary)
        service.step("BINANCE_SPOT", boundary)
        service.step("BINANCE_SPOT", bar)
        self.assertEqual(1, len(brokers["BINANCE_SPOT"].submits))
        self.assertEqual(7, brokers["BINANCE_SPOT"].submits[0]["referencePrice"])
        self.assertEqual("BUY", brokers["BINANCE_SPOT"].submits[0]["side"])

    def test_same_symbol_two_instances_net_to_one_real_submission(self):
        specs = [runtime_spec(instance="fast"), runtime_spec(instance="slow")]
        service, brokers, _ = self.service(specs)
        self.start(service)
        self.first_buy(service, specs[0])
        self.assertEqual(1, len(brokers["BINANCE_SPOT"].submits))
        rows = service.snapshot()["runtime"]["strategyRows"]
        self.assertEqual({"fast", "slow"}, {r["instanceId"] for r in rows})
        self.assertTrue(all(r["positionQuantity"] > 0 for r in rows))

    def test_unknown_reply_is_not_resubmitted_and_first_failure_is_retained(self):
        spec = runtime_spec()
        service, brokers, feeds = self.service([spec])
        self.start(service)
        brokers["BINANCE_SPOT"].status = "RAISE"
        self.first_buy(service, spec)
        service.stop()
        self.assertEqual(1, len(brokers["BINANCE_SPOT"].submits))
        self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
        runtime = service.snapshot()["runtime"]
        self.assertEqual("ORDER_RESULT_UNKNOWN_NO_RETRY", runtime["firstFailure"])
        self.assertTrue(runtime["newEntriesBlocked"])
        self.assertTrue(feeds["BINANCE_SPOT"].closed)

    def test_binance_spot_and_futures_share_one_frozen_budget(self):
        service, _, _ = self.service([runtime_spec("BINANCE_SPOT", "spot"),
                                     runtime_spec("BINANCE_USDM_FUTURES", "future")])
        self.prepare(service)
        contract = service._prepared["contract"]
        self.assertEqual([50000, 50000], [leg["routeBudgetKrw"] for leg in contract["legs"]])
        self.assertEqual(100000, sum(leg["routeBudgetKrw"] for leg in contract["legs"]))

    def test_broker_sizing_cannot_bypass_shared_cap(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        brokers["BINANCE_SPOT"].over_size = True
        self.first_buy(service, spec)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)
        self.assertIn("SHARED_BROKER_BUDGET", service.snapshot()["runtime"]["firstFailure"])

    def test_original_warmup_shortage_fails_without_order(self):
        service, brokers, feeds = self.service()
        feeds["BINANCE_SPOT"].short = True
        with self.assertRaisesRegex(ShortLiveContractError, "WARMUP_INSUFFICIENT"):
            self.start(service)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)

    def test_background_startup_failure_retires_only_verified_resources_without_fake_worker_exit(self):
        service, brokers, feeds = self.service()
        service.background = True
        feeds["BINANCE_SPOT"].short = True
        with self.assertRaisesRegex(ShortLiveContractError, "WARMUP_INSUFFICIENT"):
            self.start(service)
        runtime = service.snapshot()["runtime"]
        self.assertEqual("TERMINATED", runtime["status"])
        self.assertTrue(runtime["terminalVerified"])
        self.assertFalse(runtime["naturalExitObserved"])
        self.assertFalse(runtime["workerRetirementRequired"])
        self.assertEqual("NOT_ATTEMPTED", runtime["workerLaunchState"])
        self.assertIsNone(service._thread)
        self.assertTrue(feeds["BINANCE_SPOT"].closed)
        self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)
        terminal = json.loads(next(service.store_root.glob("session-terminal-*.json")).read_text(encoding="utf-8"))
        self.assertFalse(terminal["naturalExitObserved"])
        self.assertFalse(terminal["functionalPass"])
        self.assertIn("WARMUP_INSUFFICIENT", terminal["firstFailure"])
        # An explicit new preparation is possible; no automatic restart occurs.
        self.assertTrue(self.prepare(service)["prepared"]["readyToStart"])
        self.assertEqual(0, runtime["orderCount"])

    def test_startup_without_worker_still_requires_actual_feed_retirement_and_flat_reconciliation(self):
        for unknown in ("feed", "account"):
            with self.subTest(unknown=unknown):
                service, brokers, feeds = self.service()
                service.background = True
                feeds["BINANCE_SPOT"].short = True
                if unknown == "feed":
                    feeds["BINANCE_SPOT"].close = lambda: False
                else:
                    brokers["BINANCE_SPOT"].reconcile_known = False
                with self.assertRaisesRegex(ShortLiveContractError, "WARMUP_INSUFFICIENT"):
                    self.start(service)
                self.assertEqual("BLOCKED", service.snapshot()["runtime"]["status"])
                self.assertFalse(service._session["terminalVerified"])
                self.assertFalse(service._session["naturalExitObserved"])
                self.assertEqual([], list(service.store_root.glob("session-terminal-*.json")))
                self.assertEqual([], brokers["BINANCE_SPOT"].submits)
                _reset_for_tests()

    def test_failed_thread_start_blocks_entries_closes_feed_and_never_claims_exit(self):
        service, brokers, feeds = self.service()
        service.background = True
        with patch("threading.Thread.start", side_effect=RuntimeError("fixture unavailable")):
            with self.assertRaisesRegex(RuntimeError, "fixture unavailable"):
                self.start(service)
        runtime = service.snapshot()["runtime"]
        self.assertEqual("BLOCKED", runtime["status"])
        self.assertTrue(runtime["newEntriesBlocked"])
        self.assertFalse(runtime["terminalVerified"])
        self.assertFalse(runtime["naturalExitObserved"])
        self.assertTrue(runtime["workerRetirementRequired"])
        self.assertEqual("START_FAILED_UNVERIFIED", runtime["workerLaunchState"])
        self.assertTrue(feeds["BINANCE_SPOT"].closed)
        self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)
        self.assertIn("STARTUP_FAILED", runtime["firstFailure"])
        self.assertEqual("BLOCKED", service.close()["runtime"]["status"])
        self.assertEqual([], list(service.store_root.glob("session-terminal-*.json")))

    def test_explicit_new_session_does_not_poll_old_retired_route_feed(self):
        service, brokers, feeds = self.service()
        self.start(service)
        service.stop()
        self.assertTrue(feeds["BINANCE_SPOT"].closed)
        old_poll_calls = []
        feeds["BINANCE_SPOT"].poll = lambda _: old_poll_calls.append(True) or []
        new_spec = runtime_spec("UPBIT_KRW_SPOT", "new-scope", "1h")
        service.catalog_provider.specs = [new_spec]
        brokers[new_spec["route"]] = Broker(new_spec["route"], new_spec)
        feeds[new_spec["route"]] = Feed(new_spec)
        self.start(service)
        service.tick()
        self.assertEqual({"UPBIT_KRW_SPOT"}, set(service._feeds))
        self.assertEqual([], old_poll_calls)
        self.assertIsNone(service._session["firstFailure"])

    def test_loss_expiry_and_owner_exit_stop_entries_and_cleanup_once(self):
        for reason in ("loss", "expiry", "owner"):
            with self.subTest(reason=reason):
                service, brokers, feeds = self.service()
                self.start(service)
                if reason == "loss":
                    brokers["BINANCE_SPOT"].loss = 50000
                elif reason == "expiry":
                    self.mono += 6 * 3600
                else:
                    self.owner = False
                snapshot = service.tick()
                self.assertTrue(snapshot["runtime"]["newEntriesBlocked"])
                self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
                self.assertTrue(feeds["BINANCE_SPOT"].closed)
                self.assertTrue(snapshot["runtime"]["terminalVerified"])
                self.owner = True
                self.mono = 10

    def test_no_terminal_from_stale_previous_flat_reconciliation(self):
        service, brokers, _ = self.service()
        self.start(service)
        service.tick()
        brokers["BINANCE_SPOT"].reconcile_known = False
        snapshot = service.stop()
        self.assertFalse(snapshot["runtime"]["terminalVerified"])
        self.assertEqual("BLOCKED", snapshot["runtime"]["status"])

    def test_normal_stop_is_idempotent_and_does_not_create_promotion(self):
        service, brokers, _ = self.service()
        self.start(service)
        first = service.stop()
        second = service.stop()
        self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
        self.assertTrue(first["runtime"]["terminalVerified"])
        self.assertTrue(second["runtime"]["naturalExitObserved"])
        paths = list(service.store_root.glob("session-terminal-*.json"))
        self.assertEqual(1, len(paths))
        terminal = json.loads(paths[0].read_text(encoding="utf-8"))
        self.assertFalse(terminal["promotionEligible"])
        self.assertFalse(terminal["functionalPass"])

    def test_cold_start_never_restarts_unresolved_session(self):
        service, _, _ = self.service()
        self.start(service)
        other = ShortLiveTestService(service.catalog_provider, service.broker_provider,
                                    service.feed_provider, service.store_root,
                                    clock=lambda: self.now, background=False)
        self.assertIn("PREVIOUS_SESSION_REQUIRES_MANUAL_RECONCILIATION", other.snapshot()["blockers"])
        self.assertIsNone(other.snapshot()["runtime"])

    def test_lower_available_funds_set_effective_budget_without_deposit(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        brokers["BINANCE_SPOT"].available = 20000
        self.start(service)
        self.first_buy(service, spec)
        self.assertEqual(20000, brokers["BINANCE_SPOT"].submits[0]["notionalKrw"])
        self.assertEqual(20000, service.snapshot()["runtime"]["brokerRows"][0]["effectiveRouteBudgetKrw"])

    def test_accepted_partial_then_full_fill_is_observed_without_resubmit(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        broker = brokers["BINANCE_SPOT"]
        broker.status = "ACCEPTED"
        self.first_buy(service, spec)
        self.assertEqual("PENDING_FILL", service.snapshot()["runtime"]["status"])
        self.assertIsNone(service.snapshot()["runtime"]["firstFailure"])
        broker.observations = [(0.4, "PARTIAL"), (0.4, "PARTIAL"), (1, "FILLED")]
        service.tick()
        partial = service.snapshot()["runtime"]["strategyRows"][0]["positionQuantity"]
        self.assertGreater(partial, 0)
        service.tick()
        self.assertEqual(partial, service.snapshot()["runtime"]["strategyRows"][0]["positionQuantity"])
        service.tick()
        runtime = service.snapshot()["runtime"]
        self.assertEqual("RUNNING", runtime["status"])
        self.assertFalse(runtime["newEntriesBlocked"])
        self.assertAlmostEqual(broker.last_order["quantity"], runtime["strategyRows"][0]["positionQuantity"])
        self.assertEqual(1, len(broker.submits))
        self.assertEqual(2, runtime["fillCount"])

    def test_pending_timeout_cleans_once_without_second_submit(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        brokers["BINANCE_SPOT"].status = "ACCEPTED"
        self.first_buy(service, spec)
        self.mono += 61
        service.tick()
        self.assertEqual(1, len(brokers["BINANCE_SPOT"].submits))
        self.assertEqual(1, brokers["BINANCE_SPOT"].cleanups)
        self.assertEqual("EXACT_PENDING_ORDER_TIMEOUT", service.snapshot()["runtime"]["firstFailure"])

    def test_unknown_fee_is_explicit_and_never_claims_fee_pass(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        brokers["BINANCE_SPOT"].fee_known = False
        self.first_buy(service, spec)
        self.assertEqual("RUNNING", service.snapshot()["runtime"]["status"])
        service.stop()
        terminal = json.loads(next(service.store_root.glob("session-terminal-*.json")).read_text(encoding="utf-8"))
        self.assertFalse(terminal["actualFeeKnown"])
        self.assertEqual("UNVERIFIED_ACTUAL_FEE", terminal["feeReconciliationStatus"])
        self.assertFalse(terminal["functionalPass"])

    def test_actual_api_snapshot_excludes_private_broker_payloads(self):
        service, _, _ = self.service()
        self.start(service)
        service.tick()
        row = service.snapshot()["runtime"]["brokerRows"][0]
        for key in ("ownedOrders", "ownedPositions", "positionsBySymbol", "accountId"):
            self.assertNotIn(key, row)

    def test_display_catalog_is_not_rebuilt_on_every_worker_tick(self):
        service, _, _ = self.service()
        self.start(service)
        service.catalog_provider.snapshot = lambda: (_ for _ in ()).throw(AssertionError("worker loaded catalog"))
        service._catalog_cache = None
        service.tick()
        self.assertIsNone(service._session["firstFailure"])

    def test_slow_catalog_discovery_does_not_block_fresh_status_or_worker(self):
        service, _, _ = self.service()
        self.start(service)
        entered, release = threading.Event(), threading.Event()
        result = []
        def slow_catalog():
            entered.set()
            if not release.wait(5):
                raise AssertionError("synthetic catalog did not release")
            return {"candidates": [], "coverage": []}
        service.catalog_provider.snapshot = slow_catalog
        service._catalog_cache = None
        thread = threading.Thread(target=lambda: result.append(service.catalog()))
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            # If discovery held the state lock, neither could return before
            # the blocking provider failed. No wall-clock performance claim.
            service.tick()
            snapshot = service.snapshot(include_catalog=False)
            self.assertFalse(release.is_set())
            self.assertEqual("RUNNING", snapshot["runtime"]["status"])
            self.assertFalse(snapshot["catalogIncluded"])
            self.assertEqual([], snapshot["catalog"])
        finally:
            release.set(); thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result[0]["readOnly"])
        self.assertFalse(result[0]["authorityGranted"])
        self.assertNotIn("prepared", result[0])
        self.assertNotIn("runtime", result[0])

    def test_prepare_response_and_status_skip_catalog_and_expiry_remains_current(self):
        service, _, _ = self.service()
        service.catalog_provider.snapshot = lambda: (_ for _ in ()).throw(AssertionError("discovery in status"))
        prepared = self.prepare(service)
        self.assertFalse(prepared["catalogIncluded"])
        self.assertTrue(prepared["prepared"]["readyToStart"])
        self.now += timedelta(seconds=121)
        current = service.snapshot(include_catalog=False)
        self.assertFalse(current["prepared"]["readyToStart"])
        self.assertIn("PREPARATION_EXPIRED", current["prepared"]["blockers"])

    def test_cached_display_never_authorizes_changed_source_at_start(self):
        service, brokers, _ = self.service()
        service.catalog()
        prepared = self.prepare(service)["prepared"]
        self.source.write_text('{"fixture":"changed"}', encoding="utf-8")
        with self.assertRaisesRegex(ShortLiveContractError, "SOURCE_RAW_HASH_CHANGED"):
            service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                           "confirmed": True, "confirmationText": CONFIRMATION_TEXT})
        self.assertIsNone(service._session)
        self.assertEqual([], brokers["BINANCE_SPOT"].submits)

    def test_existing_asset_baseline_is_freshly_bound_before_start(self):
        for changed in ("b" * 64, None):
            with self.subTest(changed=changed):
                service, brokers, _ = self.service()
                broker = brokers["BINANCE_SPOT"]
                original = broker.readiness
                digest = ["a" * 64]
                broker.readiness = lambda manifest: {**original(manifest),
                    "fundingBasis": "AVAILABLE_QUOTE_ONLY", "preservedAssetsBaselineHash": digest[0],
                    "quoteCurrency": "USDT", "availableQuoteNative": "200", "preservedAssetSymbols": ["ETH"]}
                prepared = self.prepare(service)["prepared"]
                public = prepared["brokerReadiness"][0]
                self.assertEqual(["ETH"], public["preservedAssetSymbols"])
                self.assertNotIn("accountId", public)
                self.assertNotIn("preservedAssetsBaselineHash", public)
                digest[0] = changed
                with self.assertRaisesRegex(ShortLiveContractError, "PRESERVED_ASSET_BASELINE_CHANGED"):
                    service.start({"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                                   "confirmed": True, "confirmationText": CONFIRMATION_TEXT})
                self.assertIsNone(service._session)
                self.assertEqual([], broker.submits)

    def test_failed_readiness_only_exposes_allowlisted_observations(self):
        service, brokers, _ = self.service()
        brokers["BINANCE_SPOT"].readiness = lambda _: {"ready": False, "blockers": ["NO_AVAILABLE_BALANCE"],
            "fundingBasis": "AVAILABLE_QUOTE_ONLY", "availableQuoteNative": "0", "quoteCurrency": "USDT",
            "accountId": "fixture-private", "rawAccount": {"secret": "fixture-only"}}
        prepared = self.prepare(service)["prepared"]
        self.assertFalse(prepared["readyToStart"])
        self.assertEqual("0", prepared["brokerReadiness"][0]["availableQuoteNative"])
        self.assertNotIn("fixture-private", json.dumps(prepared))
        self.assertNotIn("fixture-only", json.dumps(prepared))

    def test_public_account_check_does_not_promote_adapter_ready_claim(self):
        for changes, reason in (({"scopeOwnershipVerified": False}, "ACCOUNT_SCOPE_OWNERSHIP_UNVERIFIED"),
                                ({"availableKrw": "unknown"}, "FUNDING_OBSERVATION_UNKNOWN")):
            with self.subTest(reason=reason):
                service, brokers, _ = self.service()
                original = brokers["BINANCE_SPOT"].readiness
                brokers["BINANCE_SPOT"].readiness = lambda manifest: {**original(manifest), **changes}
                prepared = self.prepare(service)["prepared"]
                self.assertFalse(prepared["readyToStart"])
                self.assertFalse(prepared["brokerReadiness"][0]["ready"])
                self.assertIn(reason, prepared["brokerReadiness"][0]["blockers"])

    def test_worker_exit_intent_does_not_claim_actual_thread_death(self):
        service, _, _ = self.service()
        service.background = True
        at_finally = threading.Event()
        release = threading.Event()
        original_event = service._event
        def event(kind, **values):
            original_event(kind, **values)
            if kind == "WORKER_EXIT_INTENT":
                at_finally.set()
                release.wait(2)
        service._event = event
        self.start(service)
        try:
            service.stop()
            self.assertTrue(at_finally.wait(2))
            self.assertTrue(service._thread.is_alive())
            self.assertFalse(service._session["terminalVerified"])
            self.assertFalse(service._session["naturalExitObserved"])
            self.assertEqual([], list(service.store_root.glob("session-terminal-*.json")))
        finally:
            release.set()
            service._thread.join(2)
        snapshot = service.snapshot()
        self.assertFalse(snapshot["runtime"]["workerAlive"])
        self.assertTrue(snapshot["runtime"]["naturalExitObserved"])
        self.assertTrue(snapshot["runtime"]["terminalVerified"])

    def test_only_exact_reserved_order_receives_dispatch_authority(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        broker = brokers["BINANCE_SPOT"]
        original_submit = broker.submit
        checks = []
        def submit(order):
            checks.append(service.dispatch_authorized(order))
            changed = {**order, "quantity": order["quantity"] * 2}
            checks.append(service.dispatch_authorized(changed))
            return original_submit(order)
        broker.submit = submit
        self.first_buy(service, spec)
        self.assertEqual([True, False], checks)
        self.assertFalse(service.dispatch_authorized(broker.last_order))

    def test_futures_custom_short_preserves_actual_sell_entry_and_buy_cover(self):
        spec = runtime_spec("BINANCE_USDM_FUTURES")
        spec["pluginId"] = "strategy_builder_custom"
        spec["parameters"] = {"customStrategyDefinition": {
            "positionDirection": "short",
            "entryRules": [{"left": "close", "operator": "below", "right": "value", "rightValue": 10}],
            "exitRules": [{"left": "close", "operator": "above", "right": "value", "rightValue": 10}],
        }}
        service, brokers, _ = self.service([spec])
        self.start(service)
        self.now += timedelta(milliseconds=200)
        service.step(spec["route"], candle(spec, NOW, 6))
        service.step(spec["route"], opening(spec, NOW, 7))
        broker = brokers[spec["route"]]
        self.assertEqual("SELL", broker.submits[0]["side"])
        self.assertLess(service.snapshot()["runtime"]["strategyRows"][0]["positionQuantity"], 0)
        self.now = NOW + timedelta(minutes=5, milliseconds=200)
        service.step(spec["route"], candle(spec, NOW + timedelta(minutes=5), 12))
        service.step(spec["route"], opening(spec, NOW + timedelta(minutes=5), 11))
        self.assertEqual(["SELL", "BUY"], [o["side"] for o in broker.submits])
        self.assertTrue(broker.submits[1]["riskReducing"])

    def test_missing_original_execution_timing_is_not_assumed(self):
        spec = runtime_spec()
        del spec["executionTiming"]
        service, _, _ = self.service([spec])
        with self.assertRaisesRegex(ShortLiveContractError, "EXACT_EXECUTION_TIMING_REQUIRED"):
            self.prepare(service)

    def test_native_futures_provider_uses_exact_continuous_gap_grid(self):
        for timeframe in ("5m", "1h", "1d"):
            with self.subTest(timeframe=timeframe):
                row = runtime_spec("BINANCE_USDM_FUTURES", timeframe=timeframe)
                spec = RuntimeStrategySpec(
                    portfolio_id=row["portfolioId"], portfolio_hash=row["portfolioHash"],
                    strategy_instance_id=row["strategyInstanceId"], strategy_id=row["strategyId"],
                    artifact_hash=row["artifactHash"], plugin_id=row["pluginId"],
                    instrument_id=row["instrumentId"], symbol=row["symbol"], timeframe=timeframe,
                    provider="binance-futures", broker_id="binance-futures", parameters=row["parameters"])
                engine = PortfolioRuntimeEngine([spec], mode="SMALL_LIVE",
                    evaluator=BuiltinBarSignalEvaluator(), cycle_handler=lambda _: None,
                    nonpromotion_gap_continuity=True)
                seconds = {"5m": 300, "1h": 3600, "1d": 86400}[timeframe]
                engine.ingest_closed_bar(candle(row, NOW, 10))
                cycle = engine.ingest_closed_bar(candle(row, NOW + timedelta(seconds=seconds * 2), 11))
                self.assertEqual(1, engine.gap_count)
                self.assertEqual("binance-futures", cycle.trigger_bar.provider)
                self.assertEqual("BTCUSDT.PERP", cycle.trigger_bar.instrument_id)

    def test_delayed_demo_open_does_not_authorize_domestic_live(self):
        spec = runtime_spec("KIS_KR_CASH")
        service, brokers, _ = self.service([spec])
        self.start(service)
        self.now += timedelta(milliseconds=200)
        service.step(spec["route"], candle(spec, NOW, 6))
        service.step(spec["route"], opening(spec, NOW, 7, attestation="KIS_DEMO_DELAYED_WEBSOCKET"))
        self.assertEqual([], brokers[spec["route"]].submits)
        self.assertIn("NATIVE_NEXT_OPEN", service.snapshot()["runtime"]["firstFailure"])

    def test_exact_base_asset_fee_reduces_actual_spot_sleeve_once(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        broker = brokers[spec["route"]]
        original = broker.submit
        def submit(order):
            reply = original(order)
            fee = order["quantity"]*.001
            broker.quantity -= fee
            broker.exposure *= .999
            return {**reply, "baseCommissionQuantity": fee, "baseCommissionKnown": True}
        broker.submit = submit
        self.start(service)
        self.first_buy(service, spec)
        record = next(iter(service._session["orders"].values()))
        self.assertEqual(record["appliedFilledQuantity"], broker.last_order["quantity"])
        self.assertAlmostEqual(record["appliedBaseCommissionQuantity"], broker.last_order["quantity"]*.001)
        self.assertAlmostEqual(service._session["sleeves"][spec["strategyInstanceId"]]["quantity"], broker.quantity)
        self.assertIsNone(service._session["firstFailure"])

    def test_missing_base_asset_fee_is_not_assumed_zero_or_resubmitted(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        broker = brokers[spec["route"]]
        original = broker.submit
        def submit(order):
            reply = original(order)
            del reply["baseCommissionQuantity"]
            return reply
        broker.submit = submit
        self.start(service)
        self.first_buy(service, spec)
        service.tick()
        self.assertIn("BASE_ASSET_COMMISSION_REQUIRED", service._session["firstFailure"])
        self.assertEqual(len(broker.submits), 1)

    def test_cleanup_actual_receipt_and_unknown_fee_are_retained(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        broker = brokers[spec["route"]]
        self.start(service)
        self.first_buy(service, spec)
        def cleanup(scope):
            order = {**broker.last_order, "clientOrderId": "cleanup-fixture-once", "side": "SELL",
                     "riskReducing": True, "quantity": broker.quantity}
            self.assertTrue(service.reserve_cleanup_order(order))
            self.assertTrue(service.dispatch_authorized(order))
            self.assertTrue(service.observe_cleanup_order(order["clientOrderId"], {
                "status": "FILLED", "brokerOrderId": "cleanup-native-order", "clientOrderId": order["clientOrderId"],
                "filledQuantity": order["quantity"], "fillPrice": order["referencePrice"], "feeKrw": None,
                "actualFeeKnown": False, "baseCommissionQuantity": 0, "baseCommissionKnown": True}))
            broker.quantity = broker.exposure = 0
            return {"complete": True}
        broker.cleanup = cleanup
        snapshot = service.stop()
        self.assertTrue(snapshot["runtime"]["terminalVerified"])
        self.assertFalse(service._session["feesKnown"])
        self.assertEqual(service._session["orders"]["cleanup-fixture-once"]["status"], "FILLED")

    def test_cleanup_submitted_without_exact_receipt_is_not_terminal(self):
        spec = runtime_spec()
        service, brokers, _ = self.service([spec])
        broker = brokers[spec["route"]]
        self.start(service)
        self.first_buy(service, spec)
        def cleanup(scope):
            order = {**broker.last_order, "clientOrderId": "cleanup-fixture-unobserved", "side": "SELL",
                     "riskReducing": True, "quantity": broker.quantity}
            self.assertTrue(service.reserve_cleanup_order(order))
            broker.quantity = broker.exposure = 0
            return {"complete": True}
        broker.cleanup = cleanup
        snapshot = service.stop()
        self.assertFalse(snapshot["runtime"]["terminalVerified"])
        self.assertEqual(service._session["orders"]["cleanup-fixture-unobserved"]["status"], "SUBMITTING")

    def test_original_five_percent_is_not_renormalized_to_full_capital(self):
        for percent in (5, 100):
            with self.subTest(percent=percent):
                self.now = NOW
                spec = runtime_spec(instance="percent-"+str(percent))
                spec["parameters"]["positionSize"] = percent
                service, brokers, _ = self.service([spec])
                self.start(service)
                self.first_buy(service, spec)
                broker = brokers[spec["route"]]
                self.assertEqual(1, len(broker.submits))
                self.assertEqual(1000*percent, broker.submits[0]["estimatedNotionalKrw"])
                sealed = service._session["contract"]["specs"][0]
                self.assertEqual(percent, sealed["sourcePositionSizePercent"])
                self.assertEqual(percent/100, sealed["positionSizeFraction"])
                self.assertFalse(sealed["sourcePositionSizeWasDefaulted"])
                service.stop()

    def test_mixed_sleeves_keep_original_allocation_denominator_and_size_fraction(self):
        specs = [runtime_spec(instance="five-percent"), runtime_spec(instance="full-sleeve")]
        specs[0]["targetWeight"], specs[1]["targetWeight"] = .2, .3
        specs[0]["parameters"]["positionSize"], specs[1]["parameters"]["positionSize"] = 5, 100
        service, brokers, _ = self.service(specs)
        self.start(service)
        self.first_buy(service, specs[0])
        order = brokers[specs[0]["route"]].last_order
        self.assertAlmostEqual(62000, order["estimatedNotionalKrw"])
        self.assertEqual(.5, order["originalAllocationWeightSum"])
        self.assertAlmostEqual(.01, order["effectiveSleeveTargets"]["five-percent"])
        self.assertEqual(.3, order["effectiveSleeveTargets"]["full-sleeve"])
        self.assertAlmostEqual(2000/7, service._session["sleeves"]["five-percent"]["quantity"])
        self.assertAlmostEqual(60000/7, service._session["sleeves"]["full-sleeve"]["quantity"])

    def test_original_size_default_and_zero_do_not_invent_new_position(self):
        service, _, _ = self.service()
        self.prepare(service)
        sealed = service._prepared["contract"]["specs"][0]
        self.assertEqual(100, sealed["sourcePositionSizePercent"])
        self.assertTrue(sealed["sourcePositionSizeWasDefaulted"])
        zero = runtime_spec(instance="disabled-zero")
        zero["parameters"]["positionSize"] = 0
        other, brokers, _ = self.service([zero])
        self.start(other)
        self.first_buy(other, zero)
        self.assertEqual([], brokers[zero["route"]].submits)

    def test_invalid_or_forged_source_position_size_is_blocked(self):
        for value in (True, False, None, float("nan"), float("inf"), -1, 101, "invalid"):
            with self.subTest(value=value):
                spec = runtime_spec()
                spec["parameters"]["positionSize"] = value
                service, _, _ = self.service([spec])
                with self.assertRaises(ShortLiveContractError): self.prepare(service)
        spec = runtime_spec()
        spec["parameters"]["positionSize"] = 5
        spec["positionSizeFraction"] = 1
        service, _, _ = self.service([spec])
        with self.assertRaisesRegex(ShortLiveContractError, "DERIVED_VALUE_MISMATCH"):
            self.prepare(service)

    def test_unimplemented_custom_nested_position_size_is_explicitly_blocked(self):
        spec = runtime_spec("BINANCE_USDM_FUTURES")
        spec["pluginId"] = "strategy_builder_custom"
        spec["parameters"]["customStrategyDefinition"] = {"riskRules": {"positionSize": 5}}
        service, _, _ = self.service([spec])
        with self.assertRaisesRegex(ShortLiveContractError, "CUSTOM_POSITION_SIZING_UNSUPPORTED"):
            self.prepare(service)

    def five_percent_pair(self):
        specs = [runtime_spec(instance="exit-sleeve"), runtime_spec(instance="retained-sleeve")]
        for spec in specs: spec["parameters"]["positionSize"] = 5
        service, brokers, _ = self.service(specs)
        self.start(service)
        return specs, service, brokers[specs[0]["route"]]

    def test_appreciated_partial_exit_preserves_other_sleeves_exact_units(self):
        specs, service, broker = self.five_percent_pair()
        self.first_buy(service, specs[0])
        owned_before = service._session["sleeves"]["retained-sleeve"]["quantity"]
        exit_before = service._session["sleeves"]["exit-sleeve"]["quantity"]
        broker.exposure = 6000  # Real units appreciated20%; remains below total cap.
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = 0
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 8.4, "fixture-appreciated-exit", ["exit-sleeve"])
        order = broker.last_order
        self.assertEqual("SELL", order["side"])
        self.assertAlmostEqual(exit_before, order["closeQuantity"])
        self.assertAlmostEqual(exit_before, order["quantity"])
        self.assertEqual(0, service._session["sleeves"]["exit-sleeve"]["quantity"])
        self.assertEqual(owned_before, service._session["sleeves"]["retained-sleeve"]["quantity"])
        self.assertAlmostEqual(owned_before, broker.quantity)

    def test_new_entry_does_not_rebalance_or_sell_an_appreciated_retained_sleeve(self):
        specs, service, broker = self.five_percent_pair()
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-first-entry", ["exit-sleeve"])
        retained_before = service._session["sleeves"]["exit-sleeve"]["quantity"]
        broker.exposure = 3000
        service._session["sleeves"]["retained-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 8.4, "fixture-second-entry", ["retained-sleeve"])
        self.assertEqual(["BUY", "BUY"], [order["side"] for order in broker.submits])
        self.assertEqual(2500, broker.last_order["notionalKrw"])
        self.assertEqual(retained_before, service._session["sleeves"]["exit-sleeve"]["quantity"])
        self.assertAlmostEqual(2500/8.4, service._session["sleeves"]["retained-sleeve"]["quantity"])

    def test_simultaneous_same_symbol_exit_entry_net_zero_is_internal_transfer_not_fake_fill(self):
        specs, service, broker = self.five_percent_pair()
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-original-entry", ["exit-sleeve"])
        held = service._session["sleeves"]["exit-sleeve"]["quantity"]
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = 0
        service._session["sleeves"]["retained-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-cross-net-zero", ["exit-sleeve", "retained-sleeve"])
        self.assertEqual(1, len(broker.submits))
        self.assertEqual(0, service._session["sleeves"]["exit-sleeve"]["quantity"])
        self.assertEqual(held, service._session["sleeves"]["retained-sleeve"]["quantity"])
        self.assertEqual(held, broker.quantity)
        self.assertEqual(1, service._session["fillCount"])
        self.assertEqual(1, service._session["internalTransferCount"])
        receipt = next(iter(service._session["internalTransfers"].values()))
        self.assertEqual((0, 0, 0), (receipt["externalOrders"], receipt["externalFills"], receipt["feeKrw"]))
        self.assertEqual([], receipt["externalAllocations"])
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-cross-net-zero", ["exit-sleeve", "retained-sleeve"])
        self.assertEqual(1, service._session["internalTransferCount"])
        self.assertEqual(1, len(broker.submits))

    def test_internal_transfer_then_appreciated_external_residual_closes_exact_remainder(self):
        specs, service, broker = self.five_percent_pair()
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-internal-before-exit", ["exit-sleeve"])
        before = broker.quantity
        broker.exposure = 3000
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = 0
        service._session["sleeves"]["retained-sleeve"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 8.4, "fixture-internal-exit-residual", ["exit-sleeve", "retained-sleeve"])
        desired = 2500/8.4
        self.assertAlmostEqual(before-desired, broker.last_order["quantity"])
        self.assertEqual("SELL", broker.last_order["side"])
        self.assertEqual(0, service._session["sleeves"]["exit-sleeve"]["quantity"])
        self.assertAlmostEqual(desired, service._session["sleeves"]["retained-sleeve"]["quantity"])
        self.assertAlmostEqual(desired, broker.quantity)
        self.assertEqual(1, service._session["internalTransferCount"])

    def test_internal_transfer_survives_unknown_external_residual_without_replay(self):
        specs, service, broker = self.five_percent_pair()
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = .025
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-internal-before-unknown", ["exit-sleeve"])
        held = broker.quantity
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = 0
        service._session["sleeves"]["retained-sleeve"]["targetWeight"] = .05
        broker.status, broker.cleanup_complete = "UNKNOWN", False
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-internal-unknown-residual", ["exit-sleeve", "retained-sleeve"])
        self.assertEqual(0, service._session["sleeves"]["exit-sleeve"]["quantity"])
        self.assertEqual(held, service._session["sleeves"]["retained-sleeve"]["quantity"])
        self.assertEqual(1, service._session["internalTransferCount"])
        self.assertEqual(1, service._session["fillCount"])
        self.assertEqual(2, len(broker.submits))
        self.assertIn("UNKNOWN", service._session["firstFailure"])
        service.tick()
        self.assertEqual(2, len(broker.submits))
        self.assertEqual(1, service._session["internalTransferCount"])

    def test_unknown_reply_read_recovery_preserves_first_failure_and_does_not_restart(self):
        specs, service, broker = self.five_percent_pair()
        broker.status, broker.cleanup_complete = "UNKNOWN", False
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = .025
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7,
                                     "fixture-original-unknown", ["exit-sleeve"])
        first_failure = service._session["firstFailure"]
        self.assertIn("UNKNOWN", first_failure)
        self.assertEqual(1, len(broker.submits))
        broker.observations = [(1, "FILLED")]
        service.tick()
        self.assertEqual(first_failure, service._session["firstFailure"])
        self.assertTrue(service._session["newEntriesBlocked"])
        self.assertNotEqual("RUNNING", service._session["status"])
        self.assertEqual(1, len(broker.submits))
        self.assertEqual(1, service._session["fillCount"])

    def test_simultaneous_same_side_exits_net_once_without_negative_holdings(self):
        specs, service, broker = self.five_percent_pair()
        self.first_buy(service, specs[0])
        owned = broker.quantity
        broker.exposure = 6000
        for sleeve in service._session["sleeves"].values(): sleeve["targetWeight"] = 0
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 8.4, "fixture-both-exit", [s["strategyInstanceId"] for s in specs])
        self.assertEqual(2, len(broker.submits))
        self.assertAlmostEqual(owned, broker.last_order["quantity"])
        self.assertTrue(all(row["quantity"] == 0 for row in service._session["sleeves"].values()))

    def test_signed_account_and_sleeve_disagreement_blocks_instead_of_guessing(self):
        specs, service, broker = self.five_percent_pair()
        self.first_buy(service, specs[0])
        broker.quantity *= -1
        service._session["sleeves"]["exit-sleeve"]["targetWeight"] = 0
        with service._lock, self.assertRaisesRegex(ShortLiveContractError, "OWNERSHIP_RECONCILIATION"):
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-wrong-owner-position", ["exit-sleeve"])
        self.assertEqual(1, len(broker.submits))

    def test_spot_and_futures_native_losses_share_one_binance_stop(self):
        specs = [runtime_spec("BINANCE_SPOT", "spot-loss"), runtime_spec("BINANCE_USDM_FUTURES", "future-loss")]
        service, brokers, _ = self.service(specs)
        self.start(service)
        brokers["BINANCE_SPOT"].loss, brokers["BINANCE_USDM_FUTURES"].loss = 30000, 20000
        self.mono += 6
        result = service.tick()
        self.assertTrue(result["runtime"]["newEntriesBlocked"])
        self.assertEqual(50000, service._session["dailyLossByBroker"]["BINANCE"])
        self.assertEqual([1, 1], [brokers[route].cleanups for route in ("BINANCE_SPOT", "BINANCE_USDM_FUTURES")])
        self.assertTrue(result["runtime"]["terminalVerified"])
        self.assertFalse(any(broker.submits for broker in brokers.values()))

    def test_internal_short_ownership_transfer_keeps_actual_signed_position(self):
        specs = [runtime_spec("BINANCE_USDM_FUTURES", "short-exit"), runtime_spec("BINANCE_USDM_FUTURES", "short-entry")]
        for spec in specs:
            spec["pluginId"] = "strategy_builder_custom"
            spec["parameters"].update(positionSize=5, customStrategyDefinition={"positionDirection": "short"})
        service, brokers, _ = self.service(specs)
        self.start(service)
        broker = brokers[specs[0]["route"]]
        service._session["sleeves"]["short-exit"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-short-first", ["short-exit"])
        held = broker.quantity
        self.assertLess(held, 0)
        service._session["sleeves"]["short-exit"]["targetWeight"] = 0
        service._session["sleeves"]["short-entry"]["targetWeight"] = .05
        with service._lock:
            service._dispatch_symbol(specs[0]["route"], specs[0]["symbol"], 7, "fixture-short-cross", ["short-exit", "short-entry"])
        self.assertEqual(held, broker.quantity)
        self.assertEqual(0, service._session["sleeves"]["short-exit"]["quantity"])
        self.assertEqual(held, service._session["sleeves"]["short-entry"]["quantity"])
        self.assertEqual(1, len(broker.submits))
        self.assertEqual(1, service._session["internalTransferCount"])


if __name__ == "__main__":
    unittest.main()
