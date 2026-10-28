"""Offline futures protection lifecycle tests; never import app startup or secrets."""
from copy import deepcopy
import ast
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
import importlib
import importlib.util
import hashlib
import json
from pathlib import Path
import sys
import types
import threading
from typing import Any
import unittest

ROOT = Path(__file__).resolve().parents[1] / "live_trader"
PKG = "_short_live_futures_protection_tests"
package = types.ModuleType(PKG); package.__path__ = [str(ROOT)]; sys.modules[PKG] = package
brokers_stub = types.ModuleType(PKG + ".short_live_brokers")
brokers_stub.epoch = lambda value: float(value) if isinstance(value, (int, float)) else datetime.fromisoformat(value).timestamp()
sys.modules[brokers_stub.__name__] = brokers_stub
mod = importlib.import_module(PKG + ".short_live_futures")
NOW = 1792652400.0

# Exercise the real service authority methods without constructing the app,
# starting its worker, or opening the user's configuration and journal.
service_tree = ast.parse((ROOT / "short_live_session.py").read_text(encoding="utf-8-sig"))
service_class = next(node for node in service_tree.body if isinstance(node, ast.ClassDef) and node.name == "ShortLiveTestService")
authority_ast = ast.ClassDef(name="AuthorityService", bases=[], keywords=[], decorator_list=[], body=[
    node for node in service_class.body if isinstance(node, ast.FunctionDef) and node.name in {"reserve_protection", "dispatch_authorized", "_reconcile_and_finish"}])
authority_globals = {"Mapping": Mapping, "Any": Any, "Decimal": Decimal, "deepcopy": deepcopy,
    "canonical_hash": lambda value: hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()}
exec(compile(ast.fix_missing_locations(ast.Module(body=[authority_ast], type_ignores=[])), str(ROOT / "short_live_session.py"), "exec"), authority_globals)


def authority_fixture():
    service = authority_globals["AuthorityService"]()
    entry = ProtectionFixture().order
    entry["leverage"] = "5"
    service._lock = threading.RLock()
    service._owner = lambda: True
    service._brokers = {"BINANCE_USDM_FUTURES": object(), "BINANCE_SPOT": object()}
    service.events = []
    service._event = lambda name, **values: service.events.append((name, values))
    service._route_session = lambda route: {"symbols": ["SOLUSDT"], "accountId": "fixture-account"}
    service._session = {"sessionId": "test-session", "scopeHash": "a" * 64, "status": "RUNNING", "newEntriesBlocked": False,
        "orders": {entry["clientOrderId"]: {"status": "SUBMITTING", "order": entry}},
        "protectionReservations": {}, "protectionObservations": {}, "cancelReservations": {}}
    claim = {key: entry[key] for key in ("route", "sessionId", "manifestHash", "accountId", "symbol", "positionDirection", "positionMode")}
    claim.update(operation="PROTECT", riskReducing=True, parentClientOrderId=entry["clientOrderId"],
                 clientAlgoId="stop-authority-fixture", triggerPrice="98", side="SELL")
    return service, entry, claim


class ProtectionFixture:
    def __init__(self, direction="long", mode="ONE_WAY"):
        self.rows, self.calls = {}, []
        self.create_visible = True
        self.post_timeout = self.cancel_timeout = self.cancel_stays_new = False
        self.reply_changes = {}
        self.order = {"route": "BINANCE_USDM_FUTURES", "sessionId": "test-session", "manifestHash": "a" * 64,
            "accountId": "fixture-account", "symbol": "SOLUSDT", "quantity": "1", "referencePrice": "100",
            "clientOrderId": "entry-fixture-001", "positionDirection": direction, "positionMode": mode,
            "side": "BUY" if direction == "long" else "SELL", "riskReducing": False,
            "estimatedNotionalKrw": "100", "projectedGrossExposureKrw": "100"}
        self.market = {"markPrice": "100", "nativePrice": "100", "priceTick": ".01", "krwPerQuote": "1",
            "feeRate": ".001", "fundingRate": ".0001", "leverageBrackets": [
                {"notionalFloor": "0", "notionalCap": "10000", "initialLeverage": "125", "maintMarginRatio": ".005", "cum": "0"}]}
        self.policy = {"leverage": 5, "marginMode": "ISOLATED", "stopLossPercent": 2}
        owner = self
        class Transport:
            def protect(self, claim, guard):
                self_guard = guard(claim)
                if not self_guard: raise ValueError("GUARD_REQUIRED")
                owner.calls.append(("POST", deepcopy(claim)))
                if owner.create_visible:
                    owner.rows[claim["clientAlgoId"]] = {"known": True, "algoId": str(100 + len(owner.rows)),
                        "clientAlgoId": claim["clientAlgoId"], "symbol": claim["symbol"], "side": claim["side"],
                        "positionSide": direction.upper() if mode == "HEDGE" else "BOTH", "closePosition": True,
                        "workingType": "MARK_PRICE", "type": "STOP_MARKET", "priceProtect": False,
                        "triggerPrice": claim["triggerPrice"], "status": "NEW", "actualOrderId": "",
                        "observedAt": NOW, **owner.reply_changes}
                if owner.post_timeout: raise TimeoutError("fixture lost response")
                return {"accepted": True}
            def cancel(self, claim, guard):
                if not guard(claim): raise ValueError("GUARD_REQUIRED")
                owner.calls.append(("DELETE", deepcopy(claim)))
                if not owner.cancel_stays_new:
                    owner.rows[claim["clientAlgoId"]]["status"] = "CANCELED"
                if owner.cancel_timeout: raise TimeoutError("fixture lost cancellation response")
                return {"accepted": True}
        def read(route, claim):
            self.calls.append(("GET", deepcopy(claim)))
            return deepcopy(self.rows.get(claim["clientAlgoId"], {"known": False}))
        def reserve(claim):
            self.calls.append(("RESERVE", deepcopy(claim)))
            return True
        self.manager = mod.FuturesProtectionManager(clock=lambda: NOW, reader=read, transport=Transport(),
            guard=lambda _: True, reserve=reserve, record=lambda kind, value: self.calls.append((kind, deepcopy(value))))

    def ensure(self, **changes):
        return self.manager.ensure({**self.order, **changes}, self.market, self.policy)


class FuturesProtectionLifecycleTests(unittest.TestCase):
    @staticmethod
    def adapter_fixture():
        spec = importlib.util.spec_from_file_location("_futures_adapter_fixture", Path(__file__).with_name("test_short_live_brokers.py"))
        fixture_module = importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture_module)
        return fixture_module.Fixture("BINANCE_USDM_FUTURES")

    def test_new_stop_is_observed_with_exact_direction_and_hedge_side(self):
        for direction in ("long", "short"):
            for mode in ("ONE_WAY", "HEDGE"):
                with self.subTest(direction=direction, mode=mode):
                    f = ProtectionFixture(direction, mode)
                    row = f.ensure()
                    self.assertEqual("NEW", row["status"])
                    self.assertEqual("98.00" if direction == "long" else "102.00", row["triggerPrice"])
                    self.assertEqual(direction.upper() if mode == "HEDGE" else "BOTH", row["positionSide"])
                    self.assertEqual(1, sum(kind == "POST" for kind, _ in f.calls))
                    self.assertTrue(f.manager.owns(row))

    def test_ack_type_price_protect_and_identity_are_fail_closed(self):
        for change in ({"type": "TAKE_PROFIT_MARKET"}, {"priceProtect": True}, {"priceProtect": None},
                       {"workingType": "CONTRACT_PRICE"}, {"closePosition": False},
                       {"symbol": "BTCUSDT"}, {"positionSide": "SHORT"}, {"clientAlgoId": "foreign"}):
            with self.subTest(change=change):
                f = ProtectionFixture(); f.reply_changes = change
                with self.assertRaisesRegex(mod.FuturesProtectionError, "ACK_UNKNOWN"):
                    f.ensure()
                self.assertEqual({}, f.manager.active_by_symbol)
        f = ProtectionFixture(); original = f.ensure()
        for change in ({"type": "TAKE_PROFIT_MARKET"}, {"priceProtect": True}, {"priceProtect": None}, {"algoId": "999"}):
            self.assertFalse(f.manager.owns({**original, **change}))

    def test_post_timeout_resolves_only_original_client_and_never_reposts(self):
        for visible in (False, True):
            with self.subTest(visible=visible):
                f = ProtectionFixture(); f.post_timeout = True; f.create_visible = visible
                for _ in range(2):
                    if visible:
                        self.assertEqual("NEW", f.ensure()["status"])
                    else:
                        with self.assertRaises(mod.FuturesProtectionError): f.ensure()
                posts = [row for kind, row in f.calls if kind == "POST"]
                reads = [row for kind, row in f.calls if kind == "GET"]
                self.assertEqual(1, len(posts))
                self.assertEqual({posts[0]["clientAlgoId"]}, {row["clientAlgoId"] for row in reads})

    def test_only_new_stop_allows_entry_to_continue(self):
        for status in ("TRIGGERING", "TRIGGERED", "FINISHED", "CANCELED", "REJECTED", "UNKNOWN"):
            with self.subTest(status=status):
                f = ProtectionFixture(); f.reply_changes = {"status": status}
                with self.assertRaisesRegex(mod.FuturesProtectionError, "NOT_ACTIVE"):
                    f.ensure()
                self.assertEqual({}, f.manager.active_by_symbol)

    def test_existing_tighter_stop_is_rechecked_against_current_mark(self):
        for direction in ("long", "short"):
            with self.subTest(direction=direction):
                f = ProtectionFixture(direction); f.ensure()
                f.market["markPrice"] = "97.5" if direction == "long" else "102.5"
                price = "99" if direction == "long" else "101"
                with self.assertRaisesRegex(mod.FuturesProtectionError, "EXISTING_STOP_ALREADY_TRIGGERED"):
                    f.ensure(referencePrice=price, clientOrderId="entry-fixture-002")
                self.assertEqual(1, sum(kind == "POST" for kind, _ in f.calls))

    def test_replacement_ack_precedes_old_cancel_and_failed_new_ack_keeps_old(self):
        f = ProtectionFixture(); old = f.ensure(); old_client = old["clientAlgoId"]
        f.market["markPrice"] = "102"
        new = f.ensure(referencePrice="102", clientOrderId="entry-fixture-002")
        new_client = new["clientAlgoId"]
        self.assertNotEqual(old_client, new_client)
        new_get = next(i for i, (kind, row) in enumerate(f.calls) if kind == "GET" and row["clientAlgoId"] == new_client)
        old_delete = next(i for i, (kind, row) in enumerate(f.calls) if kind == "DELETE" and row["clientAlgoId"] == old_client)
        self.assertLess(new_get, old_delete)
        self.assertEqual("CANCELED", f.rows[old_client]["status"])
        f = ProtectionFixture(); old = f.ensure(); f.market["markPrice"] = "102"; f.reply_changes = {"status": "REJECTED"}
        with self.assertRaises(mod.FuturesProtectionError):
            f.ensure(referencePrice="102", clientOrderId="entry-fixture-002")
        self.assertEqual("NEW", f.rows[old["clientAlgoId"]]["status"])
        self.assertFalse(any(kind == "DELETE" for kind, _ in f.calls))

    def test_partial_actual_execution_requires_separate_exact_execution_truth(self):
        f = ProtectionFixture(); ack = f.ensure(); row = f.rows[ack["clientAlgoId"]]
        row.update(status="TRIGGERED", actualOrderId="777", executionKnown=False)
        with self.assertRaisesRegex(mod.FuturesProtectionError, "EXECUTION_UNKNOWN"):
            f.manager.poll()
        self.assertEqual([], f.manager.incidents)
        row.update(executionKnown=True, actualOrder={"known": True, "brokerOrderId": "777", "symbol": "SOLUSDT",
            "side": "SELL", "positionSide": "BOTH", "status": "PARTIALLY_FILLED", "filledQuantity": ".25", "feeNative": ".01"})
        executions = f.manager.poll()
        self.assertEqual(".25", executions[0]["order"]["filledQuantity"])
        self.assertEqual("FUTURES_PROTECTIVE_STOP_TRIGGERED", f.manager.incidents[0]["code"])
        row["actualOrder"]["brokerOrderId"] = "foreign"
        with self.assertRaisesRegex(mod.FuturesProtectionError, "IDENTITY_CHANGED"):
            f.manager.poll()

    def test_short_stop_is_inside_maintenance_liquidation_buffer(self):
        f = ProtectionFixture("short")
        f.market.update(feeRate="0", fundingRate="0")
        f.market["leverageBrackets"][0].update(maintMarginRatio=".05", initialLeverage="5")
        f.policy["stopLossPercent"] = "14.9"
        self.assertLess(Decimal("120") / Decimal("1.05"), Decimal("114.9"))
        with self.assertRaisesRegex(mod.FuturesProtectionError, "MAINTENANCE_BUFFER"):
            f.ensure()
        self.assertFalse(any(kind == "POST" for kind, _ in f.calls))

    def test_projected_total_exposure_selects_maintenance_tier(self):
        f = ProtectionFixture()
        f.market["leverageBrackets"] = [
            {"notionalFloor": "0", "notionalCap": "1000", "initialLeverage": "125", "maintMarginRatio": ".005"},
            {"notionalFloor": "1000", "notionalCap": "10000", "initialLeverage": "2", "maintMarginRatio": ".05"}]
        with self.assertRaisesRegex(mod.FuturesProtectionError, "LEVERAGE_BRACKET_EXCEEDED"):
            f.ensure(projectedGrossExposureKrw="1500")
        self.assertFalse(any(kind == "POST" for kind, _ in f.calls))

    def test_actual_liquidation_price_breach_and_missing_stop_are_incidents(self):
        f = ProtectionFixture(); f.ensure()
        f.manager.check_positions([{"symbol": "SOLUSDT", "quantity": "1", "positionSide":"BOTH", "markPrice": "100", "liquidationPrice": "99"}])
        self.assertEqual("FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED", f.manager.incidents[0]["code"])
        f.manager.active_by_symbol.clear()
        f.manager.check_positions([{"symbol": "SOLUSDT", "quantity": "1"}])
        self.assertEqual("FUTURES_POSITION_UNPROTECTED", f.manager.incidents[-1]["code"])

    def test_finish_flat_waits_for_exact_terminal_and_never_repeats_delete(self):
        f = ProtectionFixture(); ack = f.ensure(); f.cancel_stays_new = True; f.cancel_timeout = True
        for _ in range(2):
            with self.assertRaisesRegex(mod.FuturesProtectionError, "CANCEL_UNKNOWN"):
                f.manager.finish_flat()
        self.assertEqual(1, sum(kind == "DELETE" for kind, _ in f.calls))
        self.assertEqual("NEW", f.manager.public_rows()[0]["status"])
        f.rows[ack["clientAlgoId"]]["status"] = "CANCELED"
        f.manager.finish_flat()
        self.assertEqual(1, sum(kind == "DELETE" for kind, _ in f.calls))
        self.assertEqual("CANCELED", f.manager.public_rows()[0]["status"])

    def test_terminal_algo_with_working_or_unknown_child_is_not_retired(self):
        f = ProtectionFixture(); ack = f.ensure(); row = f.rows[ack["clientAlgoId"]]
        row.update(status="FINISHED", actualOrderId="777", executionKnown=True,
            actualOrder={"known": True, "brokerOrderId": "777", "symbol": "SOLUSDT", "side": "SELL",
                         "positionSide": "BOTH", "status": "PARTIALLY_FILLED", "filledQuantity": ".25"})
        with self.assertRaisesRegex(mod.FuturesProtectionError, "RETIREMENT_UNKNOWN"):
            f.manager.finish_flat()
        self.assertFalse(f.manager.retirement_verified())
        self.assertTrue(f.manager.active_by_symbol)
        row["actualOrder"]["status"] = "FILLED"
        row["actualOrder"]["filledQuantity"] = "1"
        f.manager.finish_flat()
        self.assertTrue(f.manager.retirement_verified())
        self.assertEqual({}, f.manager.active_by_symbol)
        self.assertFalse(any(kind == "DELETE" for kind, _ in f.calls))

    def test_exact_rejected_protection_without_child_retires_without_delete(self):
        f = ProtectionFixture(); ack = f.ensure()
        f.rows[ack["clientAlgoId"]].update(status="REJECTED", actualOrderId="")
        f.manager.finish_flat()
        self.assertTrue(f.manager.retirement_verified())
        self.assertEqual({}, f.manager.active_by_symbol)
        self.assertFalse(any(kind == "DELETE" for kind, _ in f.calls))

    def test_finished_without_exact_child_id_is_not_retirement_proof(self):
        f = ProtectionFixture(); ack = f.ensure()
        f.rows[ack["clientAlgoId"]].update(status="FINISHED", actualOrderId="")
        with self.assertRaisesRegex(mod.FuturesProtectionError, "RETIREMENT_UNKNOWN"):
            f.manager.finish_flat()
        self.assertFalse(f.manager.retirement_verified())
        self.assertTrue(f.manager.active_by_symbol)
        self.assertFalse(any(kind == "DELETE" for kind, _ in f.calls))

    def test_protection_parent_must_match_exact_route_account_mode_and_scope(self):
        for field, bad in (("route", "BINANCE_SPOT"), ("accountId", "foreign-account"),
                           ("positionMode", "HEDGE"), ("sessionId", "foreign-session"),
                           ("manifestHash", "b" * 64), ("positionDirection", "short"), ("side", "SELL")):
            with self.subTest(field=field):
                service, entry, claim = authority_fixture()
                entry[field] = bad
                self.assertFalse(service.reserve_protection(claim))
                self.assertEqual([], service.events)
        service, entry, claim = authority_fixture()
        self.assertTrue(service.reserve_protection(claim))
        self.assertTrue(service.dispatch_authorized(claim))
        self.assertFalse(service.reserve_protection(claim))
        entry["positionMode"] = "HEDGE"
        self.assertFalse(service.dispatch_authorized(claim))

    def test_final_protection_send_rechecks_parent_lifecycle_and_user_owner(self):
        for mutation in ("FILLED", "STOPPING", "OWNER_LOST", "ENTRY_BLOCKED"):
            with self.subTest(mutation=mutation):
                service, entry, claim = authority_fixture()
                self.assertTrue(service.reserve_protection(claim))
                if mutation == "FILLED": service._session["orders"][entry["clientOrderId"]]["status"] = "FILLED"
                elif mutation == "STOPPING": service._session["status"] = "STOPPING"
                elif mutation == "OWNER_LOST": service._owner = lambda: False
                else: service._session["newEntriesBlocked"] = True
                self.assertFalse(service.dispatch_authorized(claim))

    def test_cancel_protection_retains_original_reservation_and_survives_stopping(self):
        service, entry, claim = authority_fixture()
        self.assertTrue(service.reserve_protection(claim))
        service._session["status"] = "STOPPING"; service._owner = lambda: False
        service._session["protectionObservations"][claim["clientAlgoId"]] = {"known": True, "algoId": "123", "symbol": "SOLUSDT", "clientAlgoId": claim["clientAlgoId"]}
        cancel = {**claim, "operation": "CANCEL_PROTECTION", "algoId": "123"}
        for change in ({"algoId": "foreign"}, {"triggerPrice": "90"}, {"parentClientOrderId": "foreign"}):
            self.assertFalse(service.reserve_protection({**cancel, **change}))
        self.assertTrue(service.reserve_protection(cancel))
        self.assertTrue(service.dispatch_authorized(cancel))
        self.assertFalse(service.dispatch_authorized({**cancel, "algoId": "456"}))
        self.assertFalse(service.reserve_protection(cancel))

    def test_dispatch_binds_reserved_operation_price_mode_leverage_and_quantity(self):
        service, entry, _ = authority_fixture()
        self.assertTrue(service.dispatch_authorized(deepcopy(entry)))
        for change in ({"operation": "EMERGENCY_CLOSE"}, {"referencePrice": "100.000000000000000001"},
                       {"positionMode": "HEDGE"}, {"leverage": "1"}, {"quantity": True}, {"referencePrice": "NaN"}):
            with self.subTest(change=change):
                self.assertFalse(service.dispatch_authorized({**entry, **change}))
        self.assertTrue(service.dispatch_authorized({**entry, "referencePrice": "100.00", "quantity": "1.0"}))

    def test_service_empty_open_algos_do_not_replace_exact_retirement_proof(self):
        service = authority_globals["AuthorityService"]()
        route = "BINANCE_USDM_FUTURES"
        row = {"ready": True, "ownedOrders": [], "ownedPositions": [], "ownedProtectionOrders": [],
               "exposureKrw": 0, "protectionRetirementVerified": False}
        service._session = {"orders": {}, "cleanupAttempted": True, "status": "STOPPING",
                            "feedsClosed": True, "brokerRows": {route: row}}
        service._route_session = lambda _: {"route": route}
        service._reconcile = lambda **_: True
        service.background = False
        retire_calls, finals = [], []
        service._finalize_after_worker_exit = lambda: finals.append("final")
        class Broker:
            confirmed = False
            def retire_flat_protection(self, _):
                retire_calls.append("exact lookup")
                row["protectionRetirementVerified"] = self.confirmed
                return self.confirmed
        broker = Broker(); service._brokers = {route: broker}
        service._reconcile_and_finish()
        self.assertEqual("BLOCKED", service._session["status"])
        self.assertEqual(["exact lookup"], retire_calls)
        self.assertEqual([], finals)
        broker.confirmed = True
        service._reconcile_and_finish()
        self.assertEqual("DRAINED", service._session["status"])
        self.assertEqual(["exact lookup", "exact lookup"], retire_calls)
        self.assertEqual(["final"], finals)

    def test_actual_adapter_cannot_submit_entry_without_confirmed_stop(self):
        # This fixture imports the production adapter into a shadow package;
        # all broker/account/protection I/O remains in-memory.
        for response in ({"known": False}, {"known": True, "status": "TRIGGERING"}):
            with self.subTest(response=response):
                f = self.adapter_fixture()
                self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
                f.adapter._protection.reader = lambda *_: deepcopy(response)
                sized = f.adapter.size_order(f.proposal())
                self.assertTrue(sized["ready"], sized)
                reply = f.adapter.submit(sized)
                self.assertIn(reply["status"], {"REJECTED", "UNKNOWN"})
                self.assertEqual([], f.calls)
                self.assertEqual(1, sum(kind == "PROTECT" for kind, _ in f.protection_calls))

    def test_delayed_cleanup_fill_retires_remaining_algo_without_another_close(self):
        f = self.adapter_fixture()
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        entry = f.adapter.submit(f.adapter.size_order(f.proposal()))
        self.assertEqual("FILLED", entry["status"])
        def accepted_close(route, order):
            self.assertEqual("MARKET", order["order_type"])
            self.assertTrue(order["risk_reducing"])
            f.calls.append(deepcopy(order))
            f.rows["cleanup-2"] = {"known": True, "brokerOrderId": "cleanup-2", "clientOrderId": order["identifier"],
                "symbol": f.symbol, "status": "NEW", "filledQuantity": "0", "fillPrice": "0", "feeNative": "0",
                "side": order["side"], "positionSide": "BOTH", "baseCommissionQuantity": "0", "baseCommissionKnown": True,
                "quantity": order["quantity"], "observedAt": NOW}
            f.truth["openOrders"] = [deepcopy(f.rows["cleanup-2"])]
            return {"json": {"orderId": "cleanup-2", "clientOrderId": order["identifier"], "status": "NEW", "executedQty": "0"}}
        f.adapter.transport.submit = accepted_close
        first = f.adapter.cleanup(f.manifest)
        self.assertFalse(first["complete"], first)
        self.assertFalse(f.adapter.retire_flat_protection(f.manifest))
        self.assertTrue(f.truth["openAlgoOrders"])
        self.assertEqual(2, len(f.calls))
        f.rows["cleanup-2"].update(status="FILLED", filledQuantity=entry["filledQuantity"], fillPrice="1000", feeNative="10")
        f.truth.update(positions=[], openOrders=[])
        self.assertTrue(f.adapter.retire_flat_protection(f.manifest))
        self.assertEqual([], f.truth["openAlgoOrders"])
        self.assertEqual(2, len(f.calls))
        self.assertEqual(1, sum(kind == "CANCEL_PROTECTION" for kind, _ in f.protection_calls))


if __name__ == "__main__":
    unittest.main()
