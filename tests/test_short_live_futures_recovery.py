"""Offline lost-reply recovery and protective-order retirement regressions."""
import copy
import unittest

import test_short_live_brokers as base


class FuturesRecoveryTests(unittest.TestCase):
    def fixture(self):
        f = base.Fixture("BINANCE_USDM_FUTURES")
        f.manifest["futuresPolicy"]["leverage"] = 5
        f.quote["leverage"] = "5"
        f.quote["isAutoAddMargin"] = False
        f.read_claims = []
        f.recovery_changes = {}
        f.lose_next_reply = False
        normal_submit = f.adapter.transport.submit

        def submit(route, order):
            # Model an accepted real order whose HTTP reply is lost. Exchange
            # state is updated, then the caller observes only a timeout.
            response = normal_submit(route, order)
            oid = str(response["json"]["orderId"])
            f.rows[oid].update(quantity=str(order["quantity"]), remainingQuantity="0",
                               positionSide=(order["position_direction"].upper()
                                             if order["position_mode"] == "HEDGE" else "BOTH"))
            if f.lose_next_reply:
                f.lose_next_reply = False
                raise TimeoutError("fixture: reply lost after exchange acceptance")
            return response

        def read_order(route, claim):
            f.read_claims.append(copy.deepcopy(claim))
            oid = claim.get("brokerOrderId")
            if oid:
                return copy.deepcopy(f.rows.get(str(oid), {"known": False, "status": "UNKNOWN"}))
            matches = [row for row in f.rows.values() if row["clientOrderId"] == claim["clientOrderId"]]
            if len(matches) != 1:
                return {"known": False, "status": "UNKNOWN"}
            return {**copy.deepcopy(matches[0]), **copy.deepcopy(f.recovery_changes)}

        f.adapter.transport.submit = submit
        f.adapter.transport.read_order = read_order
        ready = f.adapter.readiness(f.manifest)
        self.assertTrue(ready["ready"], ready)
        return f

    def enter(self, f, *, cid="recover-entry-0001", lost=False):
        sized = f.adapter.size_order(f.proposal(cid=cid))
        self.assertTrue(sized["ready"], sized)
        f.lose_next_reply = lost
        reply = f.adapter.submit(sized)
        self.assertEqual("UNKNOWN" if lost else "FILLED", reply["status"], reply)
        return sized, reply

    def round_trip(self, f):
        self.enter(f)
        exit_order = f.adapter.size_order({**f.proposal(side="SELL", reducing=True, cid="roundtrip-exit-0002"),
                                          "closeQuantity": "10"})
        self.assertTrue(exit_order["ready"], exit_order)
        result = f.adapter.submit(exit_order)
        self.assertEqual("FILLED", result["status"], result)
        flat = f.adapter.reconcile(f.manifest)
        self.assertTrue(flat["known"], flat)
        self.assertEqual({}, flat["positionsBySymbol"])
        return flat

    def test_lost_post_response_is_queried_by_original_reservation_and_never_reposted(self):
        f = self.fixture()
        sized, first = self.enter(f, lost=True)
        self.assertEqual("", first["brokerOrderId"])
        self.assertEqual(1, len(f.calls))
        recovered = f.adapter.observe_order(sized["clientOrderId"])
        self.assertEqual("FILLED", recovered["status"], recovered)
        self.assertEqual("1", recovered["brokerOrderId"])
        original_query = f.read_claims[0]
        self.assertEqual("", original_query["brokerOrderId"])
        self.assertEqual(sized["clientOrderId"], original_query["clientOrderId"])
        self.assertEqual(sized["quantity"], original_query["quantity"])
        self.assertEqual("BUY", original_query["side"])
        self.assertEqual("BOTH", original_query["positionSide"])
        self.assertEqual("UNKNOWN", first["status"])
        again = f.adapter.submit(sized)
        self.assertEqual(recovered["brokerOrderId"], again["brokerOrderId"])
        self.assertEqual(1, len(f.calls))
        self.assertEqual(1, sum(kind == "PROTECT" for kind, _ in f.protection_calls))

    def test_reconciliation_can_recover_fill_without_resubmitting_unknown_post(self):
        f = self.fixture()
        sized, first = self.enter(f, lost=True)
        state = f.adapter.reconcile(f.manifest)
        self.assertTrue(state["known"], state)
        self.assertEqual("10", state["positionsBySymbol"][f.symbol]["quantity"])
        self.assertEqual("UNKNOWN", first["status"])
        self.assertEqual("1", f.adapter._claims[sized["clientOrderId"]]["brokerOrderId"])
        self.assertEqual(1, len(f.calls))

    def test_recovery_with_wrong_client_side_quantity_or_position_side_is_denied(self):
        for changed in ({"clientOrderId": "foreign-order"}, {"side": "SELL"},
                        {"quantity": "11"}, {"positionSide": "SHORT"}, {"symbol": "SOLUSDT"}):
            with self.subTest(changed=changed):
                f = self.fixture()
                sized, _ = self.enter(f, lost=True)
                f.recovery_changes = changed
                result = f.adapter.observe_order(sized["clientOrderId"])
                self.assertEqual("UNKNOWN", result["status"], result)
                self.assertEqual("", f.adapter._claims[sized["clientOrderId"]]["brokerOrderId"])
                self.assertNotIn(sized["clientOrderId"], f.adapter._fill_rows)
                self.assertFalse(f.adapter.reconcile(f.manifest)["known"])
                f.adapter.submit(sized)
                self.assertEqual(1, len(f.calls))

    def test_missing_exchange_order_stays_unknown_and_retains_same_reserved_id(self):
        f = self.fixture()
        sized, _ = self.enter(f, lost=True)
        f.rows.clear()
        for _ in range(2):
            self.assertEqual("UNKNOWN", f.adapter.observe_order(sized["clientOrderId"])["status"])
            self.assertEqual("UNKNOWN", f.adapter.submit(sized)["status"])
        self.assertEqual(1, len(f.calls))
        self.assertEqual({sized["clientOrderId"]}, set(f.adapter._claims))

    def test_recovered_lost_entry_can_be_cleaned_up_with_one_exact_reducing_order(self):
        f = self.fixture()
        sized, first = self.enter(f, lost=True)
        result = f.adapter.cleanup(f.manifest)
        self.assertTrue(result["complete"], result)
        self.assertEqual("UNKNOWN", first["status"])
        self.assertEqual(2, len(f.calls))
        self.assertTrue(f.calls[1]["risk_reducing"])
        self.assertEqual("SELL", f.calls[1]["side"])
        self.assertEqual("10", f.calls[1]["quantity"])
        self.assertEqual("MARKET", f.calls[1]["order_type"])
        self.assertTrue(result["protectionRetirementVerified"])
        repeated = f.adapter.cleanup(f.manifest)
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(2, len(f.calls))
        self.assertEqual(1, sum(call["identifier"] == sized["clientOrderId"] for call in f.calls))

    def test_lost_cleanup_reply_reuses_same_close_identity_and_keeps_observed_unknown(self):
        f = self.fixture()
        self.enter(f)
        cleanup_observations = []
        def observe(client, result):
            cleanup_observations.append((client, copy.deepcopy(result)))
            return True
        f.adapter.observe_cleanup_order = observe
        f.lose_next_reply = True
        result = f.adapter.cleanup(f.manifest)
        self.assertTrue(result["complete"], result)
        self.assertEqual("UNKNOWN", cleanup_observations[0][1]["status"])
        original_cleanup_id = cleanup_observations[0][0]
        self.assertEqual(original_cleanup_id, f.calls[1]["identifier"])
        self.assertTrue(any(claim["clientOrderId"] == original_cleanup_id and not claim.get("brokerOrderId")
                            for claim in f.read_claims))
        self.assertTrue(f.adapter.continue_cleanup(f.manifest)["complete"])
        self.assertEqual(2, len(f.calls))
        self.assertEqual("UNKNOWN", cleanup_observations[0][1]["status"])

    def test_partial_cleanup_continues_same_order_until_terminal_without_new_post(self):
        f = self.fixture()
        self.enter(f)
        normal_submit = f.adapter.transport.submit
        def partial_close(route, order):
            response = normal_submit(route, order)
            if order["risk_reducing"]:
                f.rows["2"].update(status="PARTIALLY_FILLED", filledQuantity="4", remainingQuantity="6")
                f.truth["positions"] = [{"symbol": f.symbol, "quantity": "6", "markPrice": "1000",
                    "entryPrice": "1000", "isolatedWallet": "1200", "liquidationPrice": "0", "positionSide": "BOTH"}]
                response["json"].update(status="PARTIALLY_FILLED", executedQty="4")
            return response
        f.adapter.transport.submit = partial_close
        first = f.adapter.cleanup(f.manifest)
        self.assertFalse(first["complete"], first)
        self.assertEqual("6", first["positionsBySymbol"][f.symbol]["quantity"])
        cleanup_id = f.calls[1]["identifier"]
        self.assertFalse(f.adapter.continue_cleanup(f.manifest)["complete"])
        self.assertEqual(2, len(f.calls))
        f.rows["2"].update(status="FILLED", filledQuantity="10", remainingQuantity="0")
        f.truth["positions"] = []
        final = f.adapter.continue_cleanup(f.manifest)
        self.assertTrue(final["complete"], final)
        self.assertEqual(2, len(f.calls))
        self.assertEqual(cleanup_id, f.calls[1]["identifier"])

    def test_flat_roundtrip_retires_prior_stop_before_new_entry_new_algo_id(self):
        f = self.fixture()
        flat = self.round_trip(f)
        self.assertFalse(flat["protectionRetirementVerified"])
        old_client, old = next(iter(f.protections.items()))
        old_algo = old["algoId"]
        self.assertEqual("NEW", old["status"])
        sized, reply = self.enter(f, cid="next-entry-0003")
        self.assertEqual("CANCELED", f.protections[old_client]["status"])
        new_client = f.adapter._protection.active_by_symbol[f.symbol]
        self.assertNotEqual(old_client, new_client)
        self.assertNotEqual(old_algo, f.protections[new_client]["algoId"])
        self.assertEqual(["PROTECT", "CANCEL_PROTECTION", "PROTECT"], [kind for kind, _ in f.protection_calls])
        f.adapter.submit(sized)
        self.assertEqual(3, len(f.calls))
        self.assertEqual(2, sum(kind == "PROTECT" for kind, _ in f.protection_calls))

    def test_flat_unknown_algo_cannot_be_marked_complete_or_reenter(self):
        f = self.fixture()
        self.round_trip(f)
        next(iter(f.protections.values())).update(known=False, status="UNKNOWN")
        result = f.adapter.cleanup(f.manifest)
        self.assertFalse(result["complete"], result)
        self.assertFalse(f.adapter._protection.retirement_verified())
        self.assertFalse(f.adapter.size_order(f.proposal(cid="blocked-next-0003"))["ready"])
        self.assertEqual(2, len(f.calls))

    def test_previous_positive_retirement_cannot_hide_a_later_unknown_algo_read(self):
        f = self.fixture()
        self.round_trip(f)
        terminal = f.adapter.cleanup(f.manifest)
        self.assertTrue(terminal["complete"], terminal)
        self.assertTrue(f.adapter._protection.retirement_verified())
        next(iter(f.protections.values())).update(known=False, status="UNKNOWN")
        fresh = f.adapter.cleanup(f.manifest)
        self.assertFalse(fresh["complete"], fresh)
        self.assertFalse(f.adapter._protection.retirement_verified())
        self.assertEqual(2, len(f.calls))

    def test_flat_finished_algo_with_unknown_child_cannot_be_marked_complete(self):
        f = self.fixture()
        self.round_trip(f)
        row = next(iter(f.protections.values()))
        row.update(status="FINISHED", actualOrderId="99", executionKnown=False, actualOrder=None)
        f.truth["openAlgoOrders"] = []
        result = f.adapter.cleanup(f.manifest)
        self.assertFalse(result["complete"], result)
        self.assertFalse(f.adapter._protection.retirement_verified())
        self.assertEqual(2, len(f.calls))

    def test_flat_finished_algo_with_live_unfilled_child_cannot_be_marked_complete(self):
        f = self.fixture()
        self.round_trip(f)
        row = next(iter(f.protections.values()))
        row.update(status="FINISHED", actualOrderId="99", executionKnown=True,
                   actualOrder={"known": True, "brokerOrderId": "99", "symbol": f.symbol,
                                "side": "SELL", "positionSide": "BOTH", "status": "NEW",
                                "filledQuantity": "0", "feeNative": None, "observedAt": base.NOW})
        f.truth["openAlgoOrders"] = []
        result = f.adapter.cleanup(f.manifest)
        self.assertFalse(result["complete"], result)
        self.assertFalse(f.adapter._protection.retirement_verified())
        self.assertEqual(2, len(f.calls))


if __name__ == "__main__":
    unittest.main()
