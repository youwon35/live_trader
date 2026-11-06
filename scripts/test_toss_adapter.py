"""Offline boundary tests; never loads app state or contacts a broker."""
import os
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from live_trader.toss_adapter import (TossAdapterError, account_snapshot, dispatch_funds,
    market_identity, normalized_order, order_status, prepare_read_only, exact_owned_order,
    retirement_snapshot, owned_sell_quantity)
from live_trader.account_profiles import saved_account_profiles
from live_trader.market_routes import market_broker_id, market_profile
from live_trader.runtime_scope import RuntimeScope


class FakeToss:
    def __init__(self):
        self.calls = []
        self.market = "KR"

    def accounts(self):
        self.calls.append("accounts")
        return [{"accountSeq": 1, "accountNo": "12345678", "accountType": "BROKERAGE"}]

    def holdings(self):
        return {"items": [{"symbol": "005930", "name": "삼성전자", "marketCountry": "KR", "currency": "KRW",
            "quantity": "2", "marketValue": {"amount": "140000"}, "averagePurchasePrice": "65000", "lastPrice": "70000"}]}

    def buying_power(self, currency="KRW"):
        self.calls.append(("buying_power", currency))
        return {"currency": currency, "cashBuyingPower": "200000" if currency == "KRW" else "500"}

    def sellable_quantity(self, symbol):
        return {"sellableQuantity": "2"}

    def stocks(self, symbols):
        return [{"symbol": symbols[0], "name": "종목", "currency": "KRW" if self.market == "KR" else "USD",
                 "market": "KOSPI" if self.market == "KR" else "NASDAQ", "status": "ACTIVE", "securityType": "STOCK"}]

    def orders(self, **kwargs):
        return {"orders": [], "hasNext": False, "nextCursor": None}

    def commissions(self):
        return [{"marketCountry": "KR", "commissionRate": "0.00015"}]

    def conditional_orders(self):
        return {"conditionalOrders": [], "hasNext": False}

    def place_order(self, *args, **kwargs):
        raise AssertionError("read-only preparation must not mutate")


def intent(**overrides):
    return dict({"symbol": "005930", "side": "BUY", "quantity": "1", "price": "70000", "order_type": "LIMIT",
                 "identifier": "exact-client-1", "market_country": "KR", "currency": "KRW"}, **overrides)


def order(**overrides):
    return dict({"orderId": "exact-order", "symbol": "005930", "side": "BUY", "quantity": "2",
        "orderType": "LIMIT", "timeInForce": "DAY", "currency": "KRW", "status": "FILLED",
        "execution": {"filledQuantity": "2"}}, **overrides)


def proof(**overrides):
    return dict({"symbol": "005930", "side": "BUY", "quantity": "2", "currency": "KRW",
        "clientOrderId": "original-client", "response": {"brokerOrderId": "exact-order", "responseHash": "a" * 64}}, **overrides)


class TossBoundaryTests(unittest.TestCase):
    def test_limit_wire_uses_exact_id_and_quantity(self):
        normalized, wire = normalized_order(intent())
        self.assertEqual(wire, {"clientOrderId": "exact-client-1", "symbol": "005930", "side": "BUY", "orderType": "LIMIT", "quantity": "1", "price": "70000"})
        self.assertEqual(normalized["notional"], "70000")

    def test_currency_and_market_cannot_disagree(self):
        with self.assertRaises(TossAdapterError): normalized_order(intent(currency="USD"))
        with self.assertRaises(TossAdapterError): market_identity({})

    def test_cash_adapter_rejects_fractional_short_and_market(self):
        for overrides in ({"quantity": "0.2"}, {"position_direction": "short"}, {"order_type": "MARKET"}, {"identifier": "x" * 37}):
            with self.subTest(overrides=overrides), self.assertRaises(TossAdapterError): normalized_order(intent(**overrides))

    def test_korean_alphanumeric_symbol_is_supported(self):
        self.assertEqual(normalized_order(intent(symbol="0101N0"))[1]["symbol"], "0101N0")

    def test_status_unknown_does_not_become_fill(self):
        with self.assertRaises(TossAdapterError): order_status({"orderId": "x", "status": "UNRECOGNIZED"})
        row = order_status({"orderId": "x", "status": "PARTIAL_FILLED", "execution": {"filledQuantity": "1"}})
        self.assertEqual(row["state"], "partially_filled")
        self.assertEqual(row["filled_quantity"], "1")

    @patch.dict(os.environ, {"TOSS_ACCOUNT_SEQ": "1"})
    def test_readonly_prepare_preserves_native_currency_and_cash_unknown(self):
        api = FakeToss()
        result = prepare_read_only(symbol="005930", market_country="KR", api=api)
        self.assertTrue(result["ok"])
        self.assertFalse(result["executionAuthorized"])
        self.assertEqual(result["ordersSubmitted"], 0)
        self.assertEqual(len(result["accounts"]), 1)
        self.assertIsNone(result["accounts"][0]["broker_cash"])
        self.assertIsNone(result["accounts"][0]["broker_equity"])
        self.assertEqual(result["accounts"][0]["available_cash"], 200000)
        self.assertNotIn("12345678", str(result))

    @patch.dict(os.environ, {"TOSS_ACCOUNT_SEQ": "2"})
    def test_configured_account_never_falls_back_to_first(self):
        with self.assertRaisesRegex(TossAdapterError, "CONFIGURED_ACCOUNT"):
            account_snapshot(FakeToss())

    @patch.dict(os.environ, {"TOSS_ACCOUNT_SEQ": "1"})
    def test_fresh_funds_use_order_currency_and_sellable_quantity(self):
        api = FakeToss()
        buy = dispatch_funds(intent(), api)
        self.assertEqual(buy["required"], "70700.00")
        sell = dispatch_funds(intent(side="SELL"), api)
        self.assertEqual(sell["currency"], "TOSS005930")
        self.assertEqual(sell["available"], "2")

    @patch.dict(os.environ, {"TOSS_ACCOUNT_SEQ": "1"})
    def test_funds_reject_market_metadata_mismatch(self):
        api = FakeToss(); api.market = "US"
        with self.assertRaisesRegex(TossAdapterError, "METADATA_MISMATCH"): dispatch_funds(intent(), api)

    def test_explicit_toss_does_not_rewrite_legacy_kis(self):
        self.assertEqual(market_broker_id("005930"), "kis")
        self.assertEqual(market_broker_id("005930", broker_id="toss"), "toss")
        self.assertEqual(market_profile("AAPL", broker_id="toss"), "stock")

    def test_saved_profile_uses_config_and_never_reads_accounts(self):
        rows = saved_account_profiles({"TOSS_CLIENT_ID": "id", "TOSS_CLIENT_SECRET": "secret", "TOSS_ACCOUNT_SEQ": "1"})
        self.assertEqual(rows[0]["brokerId"], "toss")
        self.assertFalse(rows[0]["balanceQueried"])
        self.assertNotIn("secret", str(rows))

    def test_toss_runtime_scope_requires_stock_profile(self):
        self.assertEqual(RuntimeScope("toss", "a" * 64, "deployment", "stock", capital_currency="USD").capital_currency, "USD")
        with self.assertRaises(ValueError): RuntimeScope("toss", "a" * 64, "deployment", "crypto")

    def test_missing_execution_is_unknown_not_zero(self):
        with self.assertRaisesRegex(TossAdapterError, "EXECUTION_UNCONFIRMED"):
            order_status(order(execution={}))

    def test_retirement_requires_original_ack_and_exact_order_identity(self):
        self.assertEqual(exact_owned_order(order(), proof())["filled_quantity"], "2")
        for changed in ({"orderId": "other"}, {"symbol": "000660"}, {"side": "SELL"}, {"quantity": "3"}, {"currency": "USD"}, {"timeInForce": "OPG"}):
            with self.subTest(changed=changed), self.assertRaises(TossAdapterError):
                exact_owned_order(order(**changed), proof())
        with self.assertRaisesRegex(TossAdapterError, "ACK_REQUIRED"):
            exact_owned_order(order(), proof(response={}))

    @patch.dict(os.environ, {"TOSS_ACCOUNT_SEQ": "1"})
    def test_retirement_counts_armed_conditional_orders_as_pending(self):
        api = FakeToss()
        api.conditional_orders = lambda: {"conditionalOrders": [{"conditionalOrderId": "condition", "symbol": "005930", "status": "PAUSED"}], "hasNext": False}
        result = retirement_snapshot(["005930"], api)
        self.assertTrue(result["known"])
        self.assertEqual(len(result["openOrders"]), 1)
        api.conditional_orders = lambda: {"conditionalOrders": [], "hasNext": True}
        with self.assertRaisesRegex(TossAdapterError, "COVERAGE_INCOMPLETE"):
            retirement_snapshot(["005930"], api)

    def test_sell_limit_uses_owned_fills_not_existing_account_assets(self):
        api = FakeToss()
        api.get_order = lambda order_id: order()
        helper = SimpleNamespace(_owned_scope=lambda *args: {"scopeKey": "scope"},
            coordinator=SimpleNamespace(list_owned=lambda **kwargs: {"intents": [
                {"scopeKey": "scope", "intentId": "intent", "clientOrderId": "original-client", "state": "FILLED"}]}),
            retirement_journal=SimpleNamespace(proof=lambda *args: proof()))
        self.assertEqual(str(owned_sell_quantity(helper, "live-runtime:fixture", "005930", api)), "2")
        api.get_order = lambda order_id: order(status="PARTIAL_FILLED")
        with self.assertRaisesRegex(TossAdapterError, "STILL_OPEN"):
            owned_sell_quantity(helper, "live-runtime:fixture", "005930", api)
        helper.retirement_journal.proof = lambda *args: proof(response={})
        with self.assertRaisesRegex(TossAdapterError, "EXECUTION_UNKNOWN"):
            owned_sell_quantity(helper, "live-runtime:fixture", "005930", api)


if __name__ == "__main__":
    unittest.main()
