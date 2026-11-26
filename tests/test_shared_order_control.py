"""Offline tests for the shared order stop and read-only restart reconciliation."""

from email.message import Message
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_trader.functional_http_session import APP_SESSION_COOKIE, CSRF_HEADER, FunctionalHttpSessionAuthority
from live_trader.order_coordination import LiveOrderCoordination
from live_trader.server import LiveTraderHandler
from trading_runtime.order_coordination import CoordinationError, OrderCoordinator


HALT = "/api/shared-order-halt"
RECONCILIATION = "/api/shared-order-reconciliation"


class SharedOrderControlTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.coordinator = OrderCoordinator(Path(temp.name) / "orders.sqlite3")
        self.live = LiveOrderCoordination(self.coordinator)
        self.owner = "live-runtime:fake-interrupted"
        env = patch.dict("os.environ", {
            "LOCALAPPDATA": temp.name, "BINANCE_API_KEY": "synthetic-key",
            "KIS_ACCOUNT_NO": "12345678", "KIS_ACCOUNT_PRODUCT_CODE": "01",
        })
        env.start()
        self.addCleanup(env.stop)
        network = patch("urllib.request.OpenerDirector.open", side_effect=AssertionError("offline test"))
        network.start()
        self.addCleanup(network.stop)
        coordinator_factory = patch("live_trader.server.OrderCoordinator", return_value=self.coordinator)
        coordinator_factory.start()
        self.addCleanup(coordinator_factory.stop)

    def dispatch(self, send, *, route="binance", symbol="BTCUSDT"):
        return self.live.dispatch(
            owner_id=self.owner, route=route, submit=send,
            intent={"broker_id": route, "symbol": symbol, "side": "BUY",
                    "quantity": "1", "price": "10", "identifier": "exact-client-id"},
            budget_evidence={"currency": "USDT" if route == "binance" else "KRW",
                             "required": "10", "available": "20", "observedAt": time.time()},
        )

    def handler(self, path, body, *, authorize=True):
        authority = FunctionalHttpSessionAuthority.mint(host="127.0.0.1", port=18795)
        authority.trusted_native_bootstrap()
        handler = object.__new__(LiveTraderHandler)
        handler.path = path
        handler.client_address = ("127.0.0.1", 40000)
        handler.server = SimpleNamespace(functional_http_session_authority=authority)
        handler.headers = Message()
        handler.headers["Host"] = authority.expected_host_header
        handler.headers["Origin"] = authority.expected_origin
        handler.headers["Content-Type"] = "application/json"
        handler.headers["Content-Length"] = "100"
        if authorize:
            handler.headers["Cookie"] = f"{APP_SESSION_COOKIE}={authority.app_session_token}"
            handler.headers[CSRF_HEADER] = authority.csrf_token
        handler.read_json = Mock(return_value=body)
        handler.send_json = Mock()
        handler._send_functional_http_denial = Mock()
        return handler

    def post(self, path, body):
        handler = self.handler(path, body)
        handler.do_POST()
        handler._send_functional_http_denial.assert_not_called()
        return handler.send_json.call_args.args[0]

    def test_http_protects_both_stop_and_broker_lookup_before_reading_request(self):
        for path in (HALT, RECONCILIATION):
            request = self.handler(path, {"halted": True}, authorize=False)
            request.do_POST()
            request._send_functional_http_denial.assert_called_once()
            request.read_json.assert_not_called()
        request = self.handler(HALT, None, authorize=False)
        request.do_GET()
        request._send_functional_http_denial.assert_called_once()
        self.assertFalse(self.coordinator.path.exists())

    def test_shared_stop_blocks_fresh_and_pre_stop_reserved_order_without_transport(self):
        sent = Mock()
        self.coordinator.set_order_halt(halted=True, actor="gpt_trader")
        with self.assertRaisesRegex(CoordinationError, "NEW_ORDERS_HALTED"):
            self.dispatch(sent)
        sent.assert_not_called()
        self.assertFalse(self.coordinator.list_owned(owner_id=self.owner)["scopes"])
        before = self.coordinator.get_order_halt()
        self.assertFalse(self.post(HALT, {"halted": False})["ok"])
        self.assertFalse(self.post(HALT, {
            "halted": False, "expectedRevision": before["revision"] - 1, "bothEnginesPaused": True,
        })["ok"])
        self.assertTrue(self.coordinator.get_order_halt()["halted"])
        cleared = self.post(HALT, {
            "halted": False, "expectedRevision": before["revision"], "bothEnginesPaused": True,
        })
        self.assertEqual("live_trader", cleared["status"]["updatedBy"])
        self.assertFalse(cleared["status"]["halted"])
        sent.assert_not_called()
        register = self.live.retirement_journal.register

        def stop_after_reservation(*args):
            register(*args)
            self.coordinator.set_order_halt(halted=True, actor="gpt_trader")

        with patch.object(self.live.retirement_journal, "register", side_effect=stop_after_reservation):
            with self.assertRaisesRegex(CoordinationError, "NEW_ORDERS_HALTED"):
                self.dispatch(sent)
        sent.assert_not_called()
        intent = self.coordinator.find_intent(owner_id=self.owner, intent_key="exact-client-id")
        self.assertEqual("CANCELLED_BEFORE_SUBMIT", intent["state"])
        self.assertTrue(intent["reconciled"])

    def test_restart_matches_the_existing_order_and_keeps_ownership(self):
        send = Mock(side_effect=TimeoutError("synthetic"))
        with self.assertRaises(TimeoutError):
            self.dispatch(send)
        restarted = LiveOrderCoordination(OrderCoordinator(self.coordinator.path))
        reader = Mock()
        reader.order.return_value = {"known": False}
        failed = restarted.observe_interrupted_orders(reader=reader)
        self.assertEqual(1, failed["unresolved"])
        self.assertEqual("UNKNOWN", self.coordinator.find_intent(owner_id=self.owner, intent_key="exact-client-id")["state"])
        reader.order.return_value = {"known": True, "symbol": "BTCUSDT", "clientOrderId": "exact-client-id",
                                     "side": "BUY", "status": "FILLED", "brokerOrderId": "501", "observedAt": time.time()}
        checked = restarted.observe_interrupted_orders(reader=reader)
        self.assertEqual({"checked": 1, "resolved": 1, "unresolved": 0, "blockers": []}, checked)
        observed = self.coordinator.find_intent(owner_id=self.owner, intent_key="exact-client-id")
        self.assertEqual("FILLED", observed["state"])
        self.assertFalse(observed["reconciled"])
        self.assertTrue(restarted.retirement_status(self.owner)["retained"])
        send.assert_called_once()

    def test_restart_refuses_a_different_kis_account_before_any_read(self):
        send = Mock(side_effect=TimeoutError("synthetic"))
        with self.assertRaises(TimeoutError):
            self.dispatch(send, route="kis", symbol="005930")
        reader = Mock()
        with patch.dict("os.environ", {"KIS_ACCOUNT_NO": "87654321"}):
            checked = self.live.observe_interrupted_orders(reader=reader)
        self.assertEqual(1, checked["unresolved"])
        self.assertEqual("COORDINATION_ACCOUNT_CHANGED", checked["blockers"][0]["reason"])
        reader.order.assert_not_called()
    def test_native_toss_restart_excludes_gpt_until_owned_flat_at_broker(self):
        from live_trader.toss_cash_operation import TossCashOperation
        from trading_runtime.order_coordination import NATIVE_TOSS_OWNER_PREFIX, credential_account_identity

        owner = NATIVE_TOSS_OWNER_PREFIX + "previous-run"
        identity = credential_account_identity(
            broker="toss", api_key="synthetic", account_number="1234567890",
            account_product="VERIFIED_ACCOUNT_NUMBER",
        )
        initial = self.coordinator.claim_scope(
            owner_id=owner, broker="toss", account_identity=identity, market="CASH", symbol="005930",
        )
        monitor = TossCashOperation.__new__(TossCashOperation)
        monitor.coordinator = OrderCoordinator(self.coordinator.path)
        monitor.owner = owner
        monitor.scopes = [initial]
        monitor.claims = []
        monitor.prepared = {"specs": [SimpleNamespace(symbol="005930")], "baseline": {"005930": "5"}}
        monitor.api = Mock()
        monitor.clock = time.time
        monitor.ledger = Mock()
        monitor.view = {}
        monitor.reconcile = Mock(return_value={"unresolvedOrders": [], "positions": {"owned": "1"}})

        def gpt_claim():
            return self.coordinator.claim_scope(
                owner_id="gpt:new-run", broker="toss", account_identity=identity, market="CASH", symbol="SPY",
            )

        monitor._assert_account_dispatch("005930")
        self.coordinator.set_order_halt(halted=True, actor="gpt_trader")
        with self.assertRaisesRegex(CoordinationError, "NEW_ORDERS_HALTED"):
            monitor._assert_account_dispatch("005930")
        monitor._finish()
        self.assertEqual("RECONCILIATION_REQUIRED", monitor.view["state"])
        with self.assertRaisesRegex(CoordinationError, "TOSS_ACCOUNT_EXCLUSIVE_OWNER"):
            gpt_claim()

        monitor.reconcile.return_value = {"unresolvedOrders": [], "positions": {"owned": "0"}}
        with patch("live_trader.toss_cash_operation.open_orders", return_value=[]), patch(
            "live_trader.toss_cash_operation.holdings", return_value={"005930": "6"},
        ):
            with self.assertRaisesRegex(ValueError, "TOSS_TERMINAL_ACCOUNT_RECONCILE"):
                monitor._finish()
        with self.assertRaisesRegex(CoordinationError, "TOSS_ACCOUNT_EXCLUSIVE_OWNER"):
            gpt_claim()

        with patch("live_trader.toss_cash_operation.open_orders", return_value=[]), patch(
            "live_trader.toss_cash_operation.holdings", return_value={"005930": "5"},
        ):
            monitor._finish()
        monitor.ledger.append.assert_called_once()
        self.assertEqual("STOPPED", monitor.view["state"])
        self.assertEqual("gpt:new-run", gpt_claim()["ownerId"])
