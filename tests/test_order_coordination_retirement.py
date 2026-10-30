from copy import deepcopy
from pathlib import Path
import tempfile
import time
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from trading_runtime.order_coordination import OrderCoordinator, CoordinationError
from live_trader.order_coordination import LiveOrderCoordination
from live_trader.order_coordination_retirement import OrdinaryRetirementReader, capture_ordinary_worker, ordinary_worker_stopped


class FakeTruth:
    def __init__(self):
        self.orders = {}
        self.account = {"known": True, "ordersComplete": True, "positionsComplete": True,
                        "openOrders": [], "positions": [{"symbol": "BTCUSDT", "quantity": "7"}],
                        "algoOrdersComplete": True, "openAlgoOrders": []}
        self.calls = []

    def order(self, route, proof, intent):
        self.calls.append(("order", route, proof["clientOrderId"]))
        return {"observedAt": time.time(), **deepcopy(self.orders.get(proof["clientOrderId"], {}))}

    def snapshot(self, route, symbols):
        self.calls.append(("snapshot", route, tuple(symbols)))
        return {"observedAt": time.time(), **deepcopy(self.account)}


class OrdinaryRetirementTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "coordinator.sqlite3"
        self.env = patch.dict("os.environ", {"LOCALAPPDATA": temp.name, "BINANCE_API_KEY": "synthetic",
            "UPBIT_ACCESS_KEY": "synthetic", "KIS_ACCOUNT_NO": "12345678", "KIS_ACCOUNT_PRODUCT_CODE": "01"})
        self.env.start(); self.addCleanup(self.env.stop)
        no_network = patch("urllib.request.OpenerDirector.open", side_effect=AssertionError("offline test"))
        no_network.start(); self.addCleanup(no_network.stop)
        self.core = OrderCoordinator(self.path)
        self.live = LiveOrderCoordination(self.core)
        self.owner = "live-runtime:durable-session"
        self.reader = FakeTruth()
        self.sender = Mock()

    def order(self, client, side="BUY", quantity="1", fee="0", route="binance", symbol="BTCUSDT", state="FILLED", protection=False):
        intent = {"broker_id": route, "symbol": symbol, "side": side, "quantity": quantity,
                  "price": "10", "notional": "10", "client_order_id": client}
        if route == "binance-futures":
            intent["position_side"] = "LONG"
        if protection:
            intent.update(client_algo_id=client, position_side="LONG", close_position=True)
        response = {"orderId": client + "-broker", "status": state}
        self.sender.return_value = response
        self.live.dispatch(owner_id=self.owner, route=route, intent=intent, submit=self.sender,
            budget_evidence={"currency": "USDT", "available": "1000", "required": "10",
                             "observedAt": time.time(), "unitScale": 8}, reducing_only=protection)
        self.reader.orders[client] = {"known": True, "symbol": symbol, "clientOrderId": client,
            "brokerOrderId": response["orderId"], "side": side, "status": state,
            "filledQuantity": quantity if state == "FILLED" else "0", "baseCommissionKnown": True,
            "baseCommissionQuantity": fee}
        if route == "binance-futures":
            self.reader.orders[client]["positionSide"] = "LONG"

    def retire(self):
        return self.live.reconcile_stopped(self.owner, worker_stopped=True, reader=self.reader)

    def test_natural_stop_roundtrip_releases_exact_scope_and_preserves_external_inventory(self):
        self.order("buy", fee="0.001")
        self.order("sell", side="SELL", quantity="0.999")
        result = self.retire()
        self.assertFalse(result["retained"])
        self.assertEqual(1, result["releasedScopes"])
        self.assertEqual("7", self.reader.account["positions"][0]["quantity"])
        self.assertEqual(2, self.sender.call_count)
        self.live.claim("gpt-next", "binance", ["BTCUSDT"])

    def test_not_stopped_cannot_release_or_read_accounts(self):
        self.order("buy")
        with self.assertRaisesRegex(CoordinationError, "WORKER_STOP_NOT_VERIFIED"):
            self.live.reconcile_stopped(self.owner, reader=self.reader)
        self.assertFalse(self.reader.calls)

    def test_unknown_not_found_preserved_even_account_zero(self):
        self.order("buy", state="UNKNOWN")
        self.reader.orders["buy"] = {"known": False, "status": "UNKNOWN"}
        self.reader.account["positions"] = []
        result = self.retire()
        self.assertTrue(result["retained"])
        self.assertEqual("COORDINATION_OWNED_ORDER_UNKNOWN", result["blockers"][0]["reason"])

    def test_unknown_resolves_by_existing_id_without_resubmit_then_releases(self):
        self.order("buy", state="UNKNOWN")
        self.reader.orders["buy"].update(status="CANCELED", filledQuantity="0")
        result = self.retire()
        self.assertFalse(result["retained"])
        self.assertEqual(1, self.sender.call_count)

    def test_different_client_or_scope_cannot_be_used(self):
        self.order("buy", state="CANCELED")
        self.reader.orders["buy"]["clientOrderId"] = "foreign"
        self.assertTrue(self.retire()["retained"])

    def test_upbit_same_client_wrong_side_cannot_release(self):
        self.order("buy", route="upbit", symbol="KRW-BTC", state="CANCELED")
        self.reader.orders["buy"]["side"] = "SELL"
        self.assertEqual("COORDINATION_OWNED_ORDER_SIDE_MISMATCH", self.retire()["blockers"][0]["reason"])

    def test_current_pending_order_keeps_ownership(self):
        self.order("buy", state="CANCELED")
        self.reader.account["openOrders"] = [{"symbol": "BTCUSDT", "clientOrderId": "foreign"}]
        self.assertEqual("COORDINATION_PENDING_ORDERS_REMAIN", self.retire()["blockers"][0]["reason"])

    def test_incomplete_order_identity_does_not_disappear_during_scope_filter(self):
        self.order("buy", state="CANCELED")
        self.reader.account["openOrders"] = [{"brokerOrderId": "unidentified"}]
        self.assertEqual("COORDINATION_ACCOUNT_TRUTH_UNKNOWN", self.retire()["blockers"][0]["reason"])

    def test_owned_spot_fee_dust_is_not_flat(self):
        self.order("buy", fee="0.001")
        self.order("sell", side="SELL", quantity="1")
        self.reader.account["positions"] = []
        self.assertEqual("COORDINATION_OWNED_SPOT_QUANTITY_NOT_FLAT", self.retire()["blockers"][0]["reason"])

    def test_external_withdrawal_does_not_erase_owned_fill(self):
        self.order("buy")
        self.reader.account["positions"] = []
        self.assertTrue(self.retire()["retained"])

    def test_missing_base_fee_or_stale_evidence_never_releases(self):
        self.order("buy", state="CANCELED")
        self.reader.orders["buy"]["baseCommissionKnown"] = False
        self.assertTrue(self.retire()["retained"])
        self.reader.orders["buy"].update(baseCommissionKnown=True, observedAt=time.time() - 40)
        self.assertEqual("COORDINATION_RECONCILIATION_STALE", self.retire()["blockers"][0]["reason"])

    def test_stop_with_unknown_then_explicit_refresh_after_restart_can_recover(self):
        self.order("buy", state="UNKNOWN")
        self.reader.orders["buy"] = {"known": False}
        self.assertTrue(self.retire()["retained"])
        resumed = LiveOrderCoordination(OrderCoordinator(self.path))
        self.reader.orders["buy"] = {"known": True, "symbol": "BTCUSDT", "clientOrderId": "buy", "status": "CANCELED",
            "side": "BUY", "filledQuantity": "0", "baseCommissionKnown": True, "baseCommissionQuantity": "0"}
        self.assertFalse(resumed.reconcile_retained_stopped(reader=self.reader)[self.owner]["retained"])
        self.assertEqual(1, self.sender.call_count)

    def test_stopped_owner_cannot_send_a_new_order(self):
        self.order("buy", state="CANCELED")
        self.retire()
        before = self.core.list_owned(owner_id=self.owner)
        with self.assertRaisesRegex(CoordinationError, "STOPPED_OWNER_CANNOT_SUBMIT"):
            self.order("late", symbol="ETHUSDT")
        self.assertEqual(before, self.core.list_owned(owner_id=self.owner))
        self.assertEqual(1, self.sender.call_count)

    def test_future_hedge_sum_zero_is_not_flat(self):
        self.order("buy", route="binance-futures", symbol="SOLUSDT")
        self.reader.account["positions"] = [{"symbol": "SOLUSDT", "positionSide": "LONG", "quantity": "1"},
                                           {"symbol": "SOLUSDT", "positionSide": "SHORT", "quantity": "-1"}]
        self.assertEqual("COORDINATION_FUTURES_POSITION_NOT_FLAT", self.retire()["blockers"][0]["reason"])

    def test_future_flat_keeps_pending_protection_then_releases_after_actual_terminal(self):
        self.order("buy", route="binance-futures", symbol="SOLUSDT")
        self.order("protection", "SELL", route="binance-futures", symbol="SOLUSDT", state="ACKNOWLEDGED", protection=True)
        self.order("owned-exit", "SELL", route="binance-futures", symbol="SOLUSDT")
        self.reader.account["positions"] = []
        self.assertTrue(self.retire()["retained"])
        self.reader.orders["protection"].update(status="CANCELED", filledQuantity="0")
        # The protected position must have an actual owned close fill, not
        # merely disappear from the broker snapshot after a manual trade.
        self.reader.account["openAlgoOrders"] = [{"symbol": "SOLUSDT", "algoId": "unseen"}]
        self.assertTrue(self.live.reconcile_retained_stopped(reader=self.reader)[self.owner]["retained"])
        self.reader.account["openAlgoOrders"] = []
        self.assertFalse(self.live.reconcile_retained_stopped(reader=self.reader)[self.owner]["retained"])

    def test_future_external_flat_without_owned_close_stays_retained(self):
        self.order("buy", route="binance-futures", symbol="SOLUSDT")
        self.reader.account["positions"] = []
        self.assertEqual("COORDINATION_OWNED_FUTURES_FILL_NOT_FLAT", self.retire()["blockers"][0]["reason"])

    def test_actual_protective_fill_closes_owned_futures_quantity(self):
        self.order("buy", route="binance-futures", symbol="SOLUSDT")
        self.order("protection", "SELL", route="binance-futures", symbol="SOLUSDT", state="ACKNOWLEDGED", protection=True)
        self.reader.orders["protection"].update(status="FILLED", filledQuantity="1")
        self.reader.account["positions"] = []
        self.assertFalse(self.retire()["retained"])

    def test_protection_reader_keeps_child_terminal_status_and_actual_quantity(self):
        self.order("buy", route="binance-futures", symbol="SOLUSDT")
        self.order("protection", "SELL", route="binance-futures", symbol="SOLUSDT", state="ACKNOWLEDGED", protection=True)
        item = self.core.find_intent(owner_id=self.owner, intent_key="protection")
        proof = self.live.retirement_journal.proof(self.owner, item["intentId"])
        official = Mock()
        official.protection_order.return_value = {"known": True, "symbol": "SOLUSDT", "clientAlgoId": "protection",
            "status": "FINISHED", "side": "SELL", "positionSide": "LONG", "executionKnown": True,
            "actualOrder": {"status": "CANCELED", "filledQuantity": "0.4"}}
        actual = OrdinaryRetirementReader(readers=official).order("BINANCE_USDM_FUTURES", proof, item)
        self.assertEqual("CANCELED", actual["status"])
        self.assertEqual("0.4", actual["filledQuantity"])

    def test_worker_identity_requires_actual_held_generation_and_exact_stop_result(self):
        worker = threading.Thread(target=lambda: None)
        worker.start(); worker.join()
        supervisor = SimpleNamespace(_thread=worker)
        controller = SimpleNamespace(supervisor=supervisor)
        manager = SimpleNamespace(runtimes={"runtime-one": controller}, controllers={})
        captured = capture_ordinary_worker(manager, "runtime-one")
        good = {"ok": True, "runtime": {"phase": "STOPPED", "running": False}}
        self.assertTrue(ordinary_worker_stopped(captured, good))
        self.assertFalse(ordinary_worker_stopped(captured, {"ok": True, "results": {}}))
        self.assertIsNone(capture_ordinary_worker(manager, "missing"))
        controller.supervisor = SimpleNamespace(_thread=worker)
        self.assertFalse(ordinary_worker_stopped(captured, good))

    def test_controller_without_started_worker_is_not_retirement_evidence(self):
        manager = SimpleNamespace(runtimes={"one": SimpleNamespace(supervisor=None)}, controllers={})
        self.assertIsNone(capture_ordinary_worker(manager, "one"))
        manager.runtimes["one"].supervisor = SimpleNamespace(_thread=threading.Thread(target=lambda: None))
        self.assertIsNone(capture_ordinary_worker(manager, "one"))

    def test_missing_journal_retains_scope_instead_of_guessing(self):
        self.order("buy", state="CANCELED")
        self.live.retirement_journal.path.unlink()
        result = self.retire()
        self.assertTrue(result["retained"])
        self.assertEqual("COORDINATION_ORDER_PROOF_MISSING", result["blockers"][0]["reason"])

    def test_official_reader_queries_client_id_then_validates_exact_broker_id(self):
        self.order("buy", state="CANCELED")
        item = self.core.find_intent(owner_id=self.owner, intent_key="buy")
        proof = self.live.retirement_journal.proof(self.owner, item["intentId"])
        official = Mock()
        official._binance.return_value = {"symbol": "BTCUSDT", "clientOrderId": "buy", "orderId": "buy-broker"}
        official.get_order.return_value = self.reader.orders["buy"]
        adapter = OrdinaryRetirementReader(readers=official)
        adapter.order("BINANCE_SPOT", proof, item)
        official._binance.assert_called_once_with("binance", "/api/v3/order", {"symbol": "BTCUSDT", "origClientOrderId": "buy"})
        official._binance.return_value["orderId"] = "unrelated"
        with self.assertRaisesRegex(CoordinationError, "BROKER_ORDER_IDENTITY_MISMATCH"):
            adapter.order("BINANCE_SPOT", proof, item)

    def test_upbit_rejected_no_order_can_retire_without_inventing_broker_id(self):
        intent = {"symbol": "KRW-BTC", "side": "BUY", "quantity": "0.01", "identifier": "upbit-one"}
        self.live.dispatch(owner_id=self.owner, route="upbit", intent=intent,
            submit=lambda: {"ok": False, "statusCode": 400, "json": {"error": {"name": "insufficient_funds_bid"}}},
            budget_evidence={"currency": "KRW", "available": "10000", "required": "5000", "unitScale": 8, "observedAt": time.time()})
        official = OrdinaryRetirementReader(readers=Mock())
        official.snapshot = Mock(return_value={**self.reader.account, "observedAt": time.time()})
        result = self.live.reconcile_stopped(self.owner, worker_stopped=True, reader=official)
        self.assertFalse(result["retained"])
        official.readers.assert_not_called()


if __name__ == "__main__":
    unittest.main()
