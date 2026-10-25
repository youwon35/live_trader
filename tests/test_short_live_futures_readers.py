"""Offline official-response fixtures. No credential loading or network calls."""
from __future__ import annotations

import copy
import unittest

import test_short_live_broker_readers as reader_tests

readers, NOW = reader_tests.readers, reader_tests.NOW


ROUTE = "BINANCE_USDM_FUTURES"
SYMBOL = "SOLUSDT"
MS = int(NOW * 1000)


def algo(**changes):
    row = {"algoId": 123, "clientAlgoId": "slt-protect-exact", "symbol": SYMBOL,
           "algoType": "CONDITIONAL", "orderType": "STOP_MARKET", "side": "SELL", "positionSide": "LONG",
           "algoStatus": "NEW", "closePosition": True, "reduceOnly": False,
           "triggerPrice": "98", "workingType": "MARK_PRICE", "actualOrderId": "",
           "createTime": MS - 1000, "updateTime": MS - 1000}
    row.update(changes)
    return row


def position(**changes):
    row = {"symbol": SYMBOL, "positionSide": "LONG", "positionAmt": "1", "markPrice": "101",
           "entryPrice": "100", "liquidationPrice": "80.5", "isolatedMargin": "21", "isolatedWallet": "20",
           "initialMargin": "20.2", "positionInitialMargin": "20.2", "openOrderInitialMargin": "0",
           "maintMargin": ".505", "notional": "101", "unRealizedProfit": "1", "updateTime": MS - 1000,
           "marginAsset": "USDT"}
    row.update(changes)
    return row


def forced(order_id=444, **changes):
    row = {"symbol": SYMBOL, "orderId": order_id, "clientOrderId": "autoclose-test",
           "positionSide": "LONG", "side": "SELL", "status": "FILLED", "origQty": "1",
           "executedQty": "1", "avgPrice": "80", "time": MS - 1000, "updateTime": MS - 500}
    row.update(changes)
    return row


class FuturesReaderTests(unittest.TestCase):
    def reader(self):
        return reader_tests.ReaderTests().build()

    def account_fixture(self, positions=None, algos=None):
        r = self.reader()
        calls = []
        values = {
            "/fapi/v3/account": {"assets": [{"asset": "USDT", "availableBalance": "10", "marginBalance": "31"}]},
            "/fapi/v1/accountConfig": {"canTrade": True, "multiAssetsMargin": False},
            "/fapi/v3/positionRisk": [position()] if positions is None else positions,
            "/fapi/v1/symbolConfig": [{"symbol": SYMBOL, "leverage": 5, "marginType": "ISOLATED"}],
            "/fapi/v1/openOrders": [],
            "/fapi/v1/openAlgoOrders": [algo()] if algos is None else algos,
        }
        def get(_provider, path, params=None, signed=True):
            calls.append((path, params))
            return copy.deepcopy(values[path])
        r._binance = get
        return r, values, calls

    def test_snapshot_preserves_actual_margin_liquidation_and_distinct_algo_identity(self):
        r, _, calls = self.account_fixture()
        result = r.snapshot(ROUTE, (SYMBOL,))
        self.assertTrue(result["known"], result)
        row = result["positions"][0]
        self.assertEqual("5", row["leverage"])
        self.assertEqual("80.5", row["liquidationPrice"])
        self.assertEqual("20", row["isolatedWallet"])
        self.assertEqual("0.505", row["maintMargin"])
        self.assertEqual("LONG", row["positionSide"])
        self.assertEqual([], result["openOrders"])
        self.assertTrue(result["algoOrdersComplete"])
        self.assertEqual("123", result["openAlgoOrders"][0]["algoId"])
        self.assertIsNone(result["openAlgoOrders"][0]["quantity"])
        self.assertNotIn("sessionId", result["openAlgoOrders"][0])
        self.assertNotIn("/fapi/v1/forceOrders", [p for p, _ in calls])

    def test_duplicate_position_identity_or_unknown_margin_is_not_complete_truth(self):
        for rows in ([position(), position()], [position(maintMargin=None)],
                     [position(positionSide="SHORT", positionAmt="1")]):
            r, _, _ = self.account_fixture(positions=rows)
            result = r.snapshot(ROUTE, (SYMBOL,))
            self.assertFalse(result["known"], result)
            self.assertFalse(result["positionsComplete"])

    def test_short_position_retains_negative_quantity_and_actual_liquidation_price(self):
        r, _, _ = self.account_fixture(positions=[position(positionSide="SHORT", positionAmt="-1",
            notional="-101", liquidationPrice="119.5", unRealizedProfit="-1")], algos=[])
        result = r.snapshot(ROUTE, (SYMBOL,))
        self.assertTrue(result["known"], result)
        self.assertEqual("-1", result["positions"][0]["quantity"])
        self.assertEqual("short", result["positions"][0]["positionDirection"])
        self.assertEqual("119.5", result["positions"][0]["liquidationPrice"])

    def test_algo_unknown_flags_duplicate_identity_or_unknown_shape_blocks_snapshot(self):
        for rows in ([algo(reduceOnly=None)], [algo(), algo()], [algo(quantity=None, closePosition=False)]):
            r, _, _ = self.account_fixture(algos=rows)
            self.assertFalse(r.snapshot(ROUTE, (SYMBOL,))["known"])

    def test_close_position_without_quantity_is_readable_without_inventing_execution(self):
        r = self.reader(); calls = []
        def get(_provider, path, params=None, signed=True):
            calls.append((path, params))
            return algo()
        r._binance = get
        result = r.protection_order(ROUTE, {"algoId": "123", "clientAlgoId": "slt-protect-exact", "symbol": SYMBOL})
        self.assertTrue(result["known"], result)
        self.assertFalse(result["triggered"])
        self.assertFalse(result["executionKnown"])
        self.assertIsNone(result["actualOrder"])
        self.assertEqual([("/fapi/v1/algoOrder", {"algoId": "123"})], calls)

    def test_triggered_algo_joins_actual_order_fills_and_fee_without_using_claimed_algo_price(self):
        r = self.reader()
        def get(_provider, path, params=None, signed=True):
            if path == "/fapi/v1/algoOrder":
                return algo(algoStatus="FINISHED", actualOrderId="789", actualPrice="9999")
            if path == "/fapi/v1/order":
                return {"orderId": 789, "clientOrderId": "exchange-child", "symbol": SYMBOL,
                        "side": "SELL", "positionSide": "LONG", "status": "FILLED", "executedQty": "1", "avgPrice": "98"}
            if path == "/fapi/v1/userTrades":
                return [{"id": 9, "orderId": 789, "symbol": SYMBOL, "qty": "1", "price": "97.5",
                         "commission": ".05", "commissionAsset": "USDT"}]
            raise AssertionError(path)
        r._binance = get
        result = r.protection_order(ROUTE, {"algoId": 123, "symbol": SYMBOL, "positionSide": "LONG"})
        self.assertTrue(result["known"], result)
        self.assertTrue(result["executionKnown"])
        self.assertEqual("789", result["actualOrder"]["brokerOrderId"])
        self.assertEqual("97.5", result["actualOrder"]["fillPrice"])
        self.assertEqual("0.05", result["actualOrder"]["feeNative"])

    def test_new_unfilled_stop_child_is_known_without_fabricated_fee_or_trade_reads(self):
        r = self.reader(); calls = []
        def get(_provider, path, params=None, signed=True):
            calls.append(path)
            if path == "/fapi/v1/algoOrder":
                return algo(algoStatus="TRIGGERED", actualOrderId="789")
            if path == "/fapi/v1/order":
                return {"orderId": 789, "clientOrderId": "exchange-child", "symbol": SYMBOL,
                        "side": "SELL", "positionSide": "LONG", "status": "NEW",
                        "origQty": "1", "executedQty": "0", "avgPrice": "0"}
            self.fail("Zero fills must not query or manufacture trade fees")
        r._binance = get
        result = r.protection_order(ROUTE, {"algoId": 123, "symbol": SYMBOL})
        self.assertTrue(result["known"], result)
        self.assertTrue(result["triggered"])
        self.assertTrue(result["executionKnown"])
        actual = result["actualOrder"]
        self.assertEqual("NEW", actual["status"])
        self.assertEqual("0", actual["filledQuantity"])
        self.assertEqual("1", actual["remainingQuantity"])
        self.assertIsNone(actual["feeNative"])
        self.assertEqual(["/fapi/v1/algoOrder", "/fapi/v1/order"], calls)

    def test_positive_protective_child_fill_requires_actual_fee(self):
        r = self.reader(); r._binance = lambda *_a, **_k: algo(algoStatus="FINISHED", actualOrderId="789")
        for fee in (None, "NaN", "-1"):
            r.get_order = lambda *_a, **_k: {"known": True, "brokerOrderId": "789", "symbol": SYMBOL,
                "side": "SELL", "positionSide": "LONG", "status": "FILLED", "filledQuantity": "1",
                "fillPrice": "98", "feeNative": fee}
            self.assertFalse(r.protection_order(ROUTE, {"algoId": 123, "symbol": SYMBOL})["known"])

    def test_algo_identity_drift_or_missing_execution_never_passes(self):
        for row in (algo(symbol="BTCUSDT"), algo(clientAlgoId="someone-else"),
                    algo(algoStatus="FINISHED"), algo(algoId=124)):
            r = self.reader(); r._binance = lambda *_a, **_k: row
            result = r.protection_order(ROUTE, {"algoId": 123, "clientAlgoId": "slt-protect-exact", "symbol": SYMBOL})
            self.assertFalse(result["known"], result)

    def test_protection_execution_must_match_position_side_and_exact_order(self):
        r = self.reader(); r._binance = lambda *_a, **_k: algo(actualOrderId="789", algoStatus="FINISHED")
        for changes in ({"brokerOrderId": "790"}, {"side": "BUY"}, {"positionSide": "SHORT"}, {"known": False}):
            r.get_order = lambda *_a, **_k: {"known": True, "brokerOrderId": "789", "symbol": SYMBOL,
                                           "side": "SELL", "positionSide": "LONG", **changes}
            result = r.protection_order(ROUTE, {"algoId": 123, "symbol": SYMBOL})
            self.assertFalse(result["known"], result)

    def test_actual_order_query_checks_expected_client_and_position_side_before_trades(self):
        r = self.reader()
        def get(_provider, path, params=None, signed=True):
            if path == "/fapi/v1/order":
                return {"orderId": 789, "clientOrderId": "autoclose-exact", "symbol": SYMBOL,
                        "side": "SELL", "positionSide": "LONG", "status": "NEW", "executedQty": "0", "avgPrice": "0"}
            self.fail("Invalid identity must not query fills")
        r._binance = get
        claim = {"brokerOrderId": "789", "symbol": SYMBOL, "side": "SELL", "positionSide": "LONG", "clientOrderId": "autoclose-exact"}
        self.assertTrue(r.get_order(ROUTE, claim)["known"])
        for changes in ({"side": "BUY"}, {"positionSide": "SHORT"}, {"clientOrderId": "foreign"}):
            self.assertFalse(r.get_order(ROUTE, {**claim, **changes})["known"])
        self.assertTrue(r.get_order(ROUTE, {**claim, "externalExecution": True,
            "clientOrderId": "external-local-reservation", "actualClientOrderId": "autoclose-exact"})["known"])

    def test_unknown_futures_submit_is_recovered_by_original_client_id_without_post(self):
        r = self.reader(); calls = []
        def get(_provider, path, params=None, signed=True):
            calls.append((path, params))
            if path == "/fapi/v1/order":
                return {"orderId": 901, "clientOrderId": "short-original-exact", "symbol": SYMBOL,
                        "side": "BUY", "positionSide": "LONG", "status": "PARTIALLY_FILLED",
                        "origQty": "2", "executedQty": "1", "avgPrice": "100"}
            if path == "/fapi/v1/userTrades":
                return [{"id": 1, "orderId": 901, "symbol": SYMBOL, "qty": "1", "price": "100",
                         "commission": ".05", "commissionAsset": "USDT"}]
            self.fail(path)
        r._binance = get
        result = r.get_order(ROUTE, {"brokerOrderId": "", "status": "UNKNOWN", "symbol": SYMBOL,
            "clientOrderId": "short-original-exact", "quantity": "2", "side": "BUY", "positionSide": "LONG"})
        self.assertTrue(result["known"], result)
        self.assertEqual("901", result["brokerOrderId"])
        self.assertEqual("2", result["quantity"])
        self.assertEqual("1", result["remainingQuantity"])
        self.assertEqual("0.05", result["feeNative"])
        self.assertEqual(("/fapi/v1/order", {"symbol": SYMBOL, "origClientOrderId": "short-original-exact"}), calls[0])
        self.assertEqual("901", calls[1][1]["orderId"])

    def test_unknown_reservation_must_contain_original_identity_before_any_read(self):
        r = self.reader(); r._binance = lambda *_a, **_k: self.fail("Incomplete reservation must not query")
        claim = {"status": "UNKNOWN", "symbol": SYMBOL, "clientOrderId": "short-original-exact",
                 "quantity": "2", "side": "BUY", "positionSide": "LONG"}
        for changes in ({"status": "ACCEPTED"}, {"clientOrderId": ""}, {"quantity": None},
                        {"positionSide": None}, {"side": None}):
            self.assertFalse(r.get_order(ROUTE, {**claim, **changes})["known"])

    def test_unknown_client_query_does_not_accept_changed_identity_or_original_quantity(self):
        claim = {"status": "UNKNOWN", "symbol": SYMBOL, "clientOrderId": "short-original-exact",
                 "quantity": "2", "side": "BUY", "positionSide": "LONG"}
        original = {"orderId": 901, "clientOrderId": "short-original-exact", "symbol": SYMBOL,
                    "side": "BUY", "positionSide": "LONG", "status": "NEW",
                    "origQty": "2", "executedQty": "0", "avgPrice": "0"}
        for changes in ({"clientOrderId": "other"}, {"symbol": "BTCUSDT"}, {"side": "SELL"},
                        {"positionSide": "SHORT"}, {"origQty": "3"}, {"origQty": None}, {"orderId": 0}):
            r = self.reader(); calls = []
            def get(_provider, path, params=None, signed=True):
                calls.append(path)
                return {**original, **changes}
            r._binance = get
            self.assertFalse(r.get_order(ROUTE, claim)["known"])
            self.assertEqual(["/fapi/v1/order"], calls)

    def test_force_history_distinguishes_liquidation_and_adl_by_query_source(self):
        r = self.reader(); calls = []
        def get(_provider, path, params=None, signed=True):
            calls.append((path, params))
            return [forced()] if params["autoCloseType"] == "LIQUIDATION" else [forced(445, clientOrderId="adl_autoclose")]
        r._binance = get
        result = r.futures_force_orders(SYMBOL, MS - 2000)
        self.assertTrue(result["known"], result)
        self.assertEqual(["LIQUIDATION", "ADL"], [r["incidentType"] for r in result["events"]])
        self.assertEqual(2, len(calls))
        self.assertTrue(all(p["symbol"] == SYMBOL and p["startTime"] == MS - 2000 and p["endTime"] == MS
                            for _, p in calls))

    def test_force_history_saturation_retains_evidence_but_never_claims_complete(self):
        r = self.reader(); r._binance = lambda *_a, **_k: [forced(400 + i) for i in range(100)]
        result = r.futures_force_orders(SYMBOL, MS - 2000)
        self.assertFalse(result["known"])
        self.assertEqual(100, len(result["events"]))
        self.assertIn("FORCE_ORDER_HISTORY_INCOMPLETE", result["blockers"])

    def test_force_history_scope_time_side_duplicates_and_unreadable_result_fail_closed(self):
        cases = ([forced(symbol="BTCUSDT")], [forced(time=MS - 3000)],
                 [forced(updateTime=MS + 1)], [forced(positionSide="LONG", side="BUY")],
                 [forced(), forced()], {"unknown": True})
        for rows in cases:
            r = self.reader(); r._binance = lambda *_a, **_k: rows
            result = r.futures_force_orders(SYMBOL, MS - 2000)
            self.assertFalse(result["known"], result)

    def test_force_history_invalid_window_never_reaches_transport(self):
        r = self.reader(); r._binance = lambda *_a, **_k: self.fail("No call for invalid window")
        for value in (True, -1, MS + 1, MS - 8 * 24 * 3600 * 1000, float(MS)):
            self.assertFalse(r.futures_force_orders(SYMBOL, value)["known"])

    def market_fixture(self, bracket_override=None):
        r = self.reader()
        rows = [{"notionalFloor": "0", "notionalCap": "10000", "maintMarginRatio": ".005", "cum": "0", "initialLeverage": 20},
                {"notionalFloor": "10000", "notionalCap": "50000", "maintMarginRatio": ".01", "cum": "50", "initialLeverage": 10}]
        values = {
            "/fapi/v1/exchangeInfo": {"symbols": [{"symbol": SYMBOL, "status": "TRADING", "quoteAsset": "USDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": ".01"},
                {"filterType": "LOT_SIZE", "stepSize": ".1", "minQty": ".1", "maxQty": "10000"},
                {"filterType": "MARKET_LOT_SIZE", "stepSize": ".2", "minQty": ".3", "maxQty": "1000"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"}]}]},
            "/fapi/v1/premiumIndex": {"markPrice": "100", "time": MS, "lastFundingRate": ".0001"},
            "/fapi/v1/commissionRate": {"takerCommissionRate": ".0005"},
            "/fapi/v1/symbolConfig": [{"symbol": SYMBOL, "marginType": "ISOLATED", "leverage": 5}],
            "/fapi/v1/positionSide/dual": {"dualSidePosition": True},
            "/fapi/v1/leverageBracket": [{"symbol": SYMBOL, "notionalCoef": "2", "brackets": rows if bracket_override is None else bracket_override}],
        }
        r._binance = lambda _provider, path, *_a, **_k: copy.deepcopy(values[path])
        return r, values

    def test_market_keeps_actual_leverage_mark_and_account_adjusted_brackets(self):
        r, _ = self.market_fixture()
        result = r.market(ROUTE, SYMBOL)
        self.assertTrue(result["known"], result)
        self.assertEqual("5", result["leverage"])
        self.assertEqual("100", result["markPrice"])
        self.assertTrue(result["leverageBracketsAccountAdjusted"])
        self.assertEqual("20000", result["leverageBrackets"][0]["notionalCap"])
        self.assertEqual("100", result["leverageBrackets"][1]["cum"])

    def test_market_quantity_rules_are_separate_from_limit_lot_size(self):
        r, _ = self.market_fixture()
        result = r.market(ROUTE, SYMBOL)
        self.assertTrue(result["known"], result)
        self.assertEqual((".1", ".1", "10000"),
                         (result["lotStep"], result["minQuantity"], result["maxQuantity"]))
        self.assertEqual(("0.2", "0.3", "1000"),
                         (result["marketQuantityStep"], result["marketMinQuantity"], result["marketMaxQuantity"]))

    def test_missing_duplicate_or_invalid_market_lot_rules_never_fall_back_to_limit(self):
        mutations = (lambda rows: rows.remove(next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE")),
            lambda rows: rows.append(copy.deepcopy(next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE"))),
            lambda rows: next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE").update(stepSize="-1"),
            lambda rows: next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE").update(minQty=None),
            lambda rows: next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE").update(maxQty="NaN"),
            lambda rows: next(r for r in rows if r["filterType"] == "MARKET_LOT_SIZE").update(maxQty=".1"))
        for mutate in mutations:
            r, values = self.market_fixture()
            mutate(values["/fapi/v1/exchangeInfo"]["symbols"][0]["filters"])
            self.assertFalse(r.market(ROUTE, SYMBOL)["known"])

    def test_zero_market_step_uses_verified_limit_step_but_preserves_market_minimum_and_cap(self):
        r, values = self.market_fixture()
        rows = values["/fapi/v1/exchangeInfo"]["symbols"][0]["filters"]
        next(row for row in rows if row["filterType"] == "MARKET_LOT_SIZE").update(stepSize="0", minQty="0")
        result = r.market(ROUTE, SYMBOL)
        self.assertTrue(result["known"], result)
        self.assertEqual(("0.1", "0", "1000"),
                         (result["marketQuantityStep"], result["marketMinQuantity"], result["marketMaxQuantity"]))
        self.assertEqual("0", result["marketQuantityStepRaw"])
        self.assertEqual("LOT_SIZE_FOR_ZERO_MARKET_STEP", result["marketQuantityStepSource"])
        next(row for row in rows if row["filterType"] == "LOT_SIZE").update(stepSize="0")
        self.assertFalse(r.market(ROUTE, SYMBOL)["known"])

    def test_auto_add_margin_is_preserved_only_from_explicit_exchange_boolean(self):
        for source, expected in ((False, False), (True, True), ("false", False), ("true", True)):
            r, values = self.market_fixture()
            values["/fapi/v1/symbolConfig"][0]["isAutoAddMargin"] = source
            result = r.market(ROUTE, SYMBOL)
            self.assertTrue(result["known"], result)
            self.assertIs(expected, result["isAutoAddMargin"])
        r, _ = self.market_fixture()
        self.assertNotIn("isAutoAddMargin", r.market(ROUTE, SYMBOL))
        for source in ("yes", "False", 0, 1):
            r, values = self.market_fixture()
            values["/fapi/v1/symbolConfig"][0]["isAutoAddMargin"] = source
            self.assertFalse(r.market(ROUTE, SYMBOL)["known"])

    def test_bad_maintenance_tiers_never_authorize_missing_or_discontinuous_risk(self):
        for tiers in ([], [{"notionalFloor": "1", "notionalCap": "10", "maintMarginRatio": ".005", "cum": "0", "initialLeverage": 5}],
                      [{"notionalFloor": "0", "notionalCap": "10", "maintMarginRatio": "NaN", "cum": "0", "initialLeverage": 5}]):
            r, _ = self.market_fixture(tiers)
            self.assertFalse(r.market(ROUTE, SYMBOL)["known"])


if __name__ == "__main__":
    unittest.main()
