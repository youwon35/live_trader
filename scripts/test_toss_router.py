"""Offline router fixtures. No app startup, broker I/O or real account state."""
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from live_trader.brokers import LiveBrokerRouter, BrokerNotReadyError
from trading_runtime.order_coordination import OrderCoordinator
from test_toss_adapter import FakeToss, intent, order


class BrokerFixture(FakeToss):
    def __init__(self):
        super().__init__()
        self.writes = []
        self.wrong_ack = False

    def place_order(self, body):
        self.writes.append(("PLACE", body))
        return {"orderId": "exact-order", "clientOrderId": "wrong" if self.wrong_ack else body["clientOrderId"]}

    def get_order(self, order_id):
        self.calls.append(("order", order_id))
        return order(quantity="1", status="PENDING", execution={"filledQuantity": "0"})

    def cancel_order(self, order_id):
        self.writes.append(("CANCEL", order_id))
        return {"orderId": order_id}


@patch.dict(os.environ, {"TOSS_CLIENT_ID": "fixture-key", "TOSS_CLIENT_SECRET": "fixture-secret", "TOSS_ACCOUNT_SEQ": "1"})
class TossRouterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="toss-router-fixture-")
        self.api = BrokerFixture()
        self.coordinator = OrderCoordinator(Path(self.temp.name) / "coordination.sqlite3")
        self.router = LiveBrokerRouter(coordination_owner_id="live-runtime:fixture",
            order_coordinator=self.coordinator,
            coordination_funds_reader=lambda _: {"currency":"KRW", "available":"100000", "required":"70700", "observedAt":time.time(), "unitScale":8})
        self.client = patch("live_trader.toss_adapter.client", lambda **_: self.api)
        self.client.start()

    def tearDown(self):
        self.client.stop()
        self.temp.cleanup()

    def test_without_normal_start_consent_no_mutation_is_reached(self):
        with patch("live_trader.brokers.real_orders_enabled", return_value=False):
            with self.assertRaises(BrokerNotReadyError):
                self.router.place_order(intent(broker_id="toss"))
        self.assertEqual(self.api.writes, [])

    @patch("live_trader.brokers.real_orders_enabled", return_value=True)
    def test_exact_ack_is_journaled_and_repeat_never_resubmits(self, _):
        self.assertTrue(self.router.place_order(intent(broker_id="toss"))["ok"])
        with self.assertRaises(BrokerNotReadyError):
            self.router.place_order(intent(broker_id="toss"))
        self.assertEqual(len(self.api.writes), 1)
        owned = self.coordinator.list_owned(owner_id="live-runtime:fixture")
        self.assertEqual(owned["intents"][0]["state"], "ACKNOWLEDGED")

    @patch("live_trader.brokers.real_orders_enabled", return_value=True)
    def test_wrong_ack_is_unknown_and_never_retried(self, _):
        self.api.wrong_ack = True
        with self.assertRaisesRegex(BrokerNotReadyError, "ACK_IDENTITY_UNKNOWN"):
            self.router.place_order(intent(broker_id="toss"))
        owned = self.coordinator.list_owned(owner_id="live-runtime:fixture")
        self.assertEqual(owned["intents"][0]["state"], "UNKNOWN")
        with self.assertRaises(BrokerNotReadyError):
            self.router.place_order(intent(broker_id="toss"))
        self.assertEqual(len(self.api.writes), 1)

    @patch("live_trader.brokers.real_orders_enabled", return_value=True)
    def test_cancel_requires_owned_durable_order_not_account_order(self, _):
        self.router.place_order(intent(broker_id="toss"))
        with self.assertRaisesRegex(BrokerNotReadyError, "NOT_OWNED"):
            self.router.cancel_order("toss", "foreign-order", symbol="005930")
        result = self.router.cancel_order("toss", "exact-order", symbol="005930")
        self.assertTrue(result["ok"])
        self.assertEqual(self.api.writes[-1], ("CANCEL", "exact-order"))

    @patch("live_trader.brokers.real_orders_enabled", return_value=True)
    def test_existing_holdings_cannot_be_sold_without_owned_fills(self, _):
        self.router._order_coordination.claim("live-runtime:fixture", "toss", ["005930"])
        with self.assertRaisesRegex(BrokerNotReadyError, "EXCEEDS_OWNED"):
            self.router.place_order(intent(broker_id="toss", side="SELL", risk_reducing=True))
        self.assertEqual(self.api.writes, [])


if __name__ == "__main__":
    unittest.main()
