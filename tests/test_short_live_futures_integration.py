"""Offline adapter integration: exchange truth is injected; no live package boot."""
import copy
import unittest

import test_short_live_brokers as base


mod = base.mod
NOW = base.NOW


class FuturesIntegrationTests(unittest.TestCase):
    def fixture(self, leverage=5, *, entry=True, direction="long", mode="ONE_WAY"):
        f = base.Fixture("BINANCE_USDM_FUTURES")
        f.manifest["futuresPolicy"]["leverage"] = leverage
        f.quote.update(leverage=str(leverage), positionMode=mode)
        f.manifest["specs"] = [{"symbol": f.symbol, "positionDirection": direction, "targetWeight": 1}]
        ready = f.adapter.readiness(f.manifest)
        self.assertTrue(ready["ready"], ready)
        if entry:
            side = "BUY" if direction == "long" else "SELL"
            proposal = f.proposal("10000", side=side, direction=direction)
            reply = f.adapter.submit(f.adapter.size_order(proposal))
            self.assertEqual("FILLED", reply["status"], reply)
        return f

    @staticmethod
    def actual(symbol="BTCUSDT", qty="4", *, oid="91", side="SELL", position="BOTH"):
        return {"known": True, "brokerOrderId": oid, "clientOrderId": "exchange-" + oid,
                "symbol": symbol, "side": side, "positionSide": position, "status": "FILLED",
                "filledQuantity": qty, "fillPrice": "900", "feeNative": "1",
                "baseCommissionKnown": True, "baseCommissionQuantity": "0", "observedAt": NOW}

    def force(self, f, qty="4", *, kind="LIQUIDATION", oid="91", side="SELL", position="BOTH", time_ms=None):
        row = {"incidentType": kind, "autoCloseType": kind, "brokerOrderId": oid,
               "symbol": f.symbol, "side": side, "positionSide": position,
               "filledQuantity": qty, "time": int(NOW * 1000) if time_ms is None else time_ms,
               "updateTime": int(NOW * 1000)}
        f.rows[oid] = self.actual(f.symbol, qty, oid=oid, side=side, position=position)
        calls = []
        def read(symbol, since_ms):
            calls.append((symbol, since_ms))
            return {"known": True, "observedAt": NOW, "sinceMs": since_ms,
                    "untilMs": int(NOW * 1000), "events": [copy.deepcopy(row)]}
        f.adapter._force_order_reader = read
        return row, calls

    def test_one_and_five_times_size_use_actual_capital_and_fee_reserve(self):
        quantities = {}
        for leverage in (1, 5):
            f = self.fixture(leverage, entry=False)
            sized = f.adapter.size_order(f.proposal("1000000"))
            self.assertTrue(sized["ready"], sized)
            qty = mod.number(sized["quantity"])
            quantities[leverage] = qty
            required = qty * 1000 * (mod.Decimal(1) / leverage + mod.Decimal("0.0022"))
            self.assertEqual(required, mod.number(sized["requiredCapitalKrw"]))
            self.assertLessEqual(required, mod.Decimal(100000))
            self.assertGreater((qty + 1) * 1000 * (mod.Decimal(1) / leverage + mod.Decimal("0.0022")), 100000)
        self.assertEqual(mod.Decimal(99), quantities[1])
        self.assertEqual(mod.Decimal(494), quantities[5])

    def test_available_cash_is_not_multiplied_into_spendable_collateral(self):
        f = self.fixture(entry=False)
        f.truth["availableNative"] = "1000"
        sized = f.adapter.size_order(f.proposal("1000000"))
        self.assertTrue(sized["ready"], sized)
        self.assertEqual("4", sized["quantity"])
        self.assertLessEqual(mod.number(sized["requiredCapitalKrw"]), 1000)

    def test_mark_gain_changes_notional_but_not_entry_capital(self):
        f = self.fixture()
        before = f.adapter.reconcile(f.manifest)
        self.assertTrue(before["known"], before)
        f.truth["positions"][0]["markPrice"] = "2000"
        f.quote.update(nativePrice="2000", markPrice="2000")
        after = f.adapter.reconcile(f.manifest)
        self.assertTrue(after["known"], after)
        self.assertEqual("2000", before["committedCapitalKrw"])
        self.assertEqual(before["committedCapitalKrw"], after["committedCapitalKrw"])
        self.assertEqual("20000", after["notionalExposureKrw"])

    def test_added_isolated_margin_is_counted_as_committed_capital(self):
        f = self.fixture()
        f.truth["positions"][0]["isolatedWallet"] = "3000"
        state = f.adapter.reconcile(f.manifest)
        self.assertTrue(state["known"], state)
        self.assertEqual("3000", state["committedCapitalKrw"])

    def test_setting_drift_blocks_new_entry_but_keeps_exact_reduction_available(self):
        for changed in ({"leverage": "10"}, {"marginMode": "CROSSED"}):
            with self.subTest(changed=changed):
                f = self.fixture()
                f.quote.update(changed)
                blocked = f.adapter.size_order(f.proposal(cid="new-client-0002"))
                self.assertFalse(blocked["ready"], blocked)
                reducing = f.proposal("10000", side="SELL", reducing=True, cid="exit-client-0003")
                reducing["closeQuantity"] = "10"
                sized = f.adapter.size_order(reducing)
                self.assertTrue(sized["ready"], sized)
                self.assertEqual("10", sized["quantity"])
                self.assertEqual(1, len(f.calls))

    def test_auto_add_margin_requires_observed_false_before_readiness(self):
        for value in ("missing", None, True, "false", 0, 1):
            with self.subTest(value=value):
                f = base.Fixture("BINANCE_USDM_FUTURES")
                if value == "missing":
                    f.quote.pop("isAutoAddMargin")
                else:
                    f.quote["isAutoAddMargin"] = value
                ready = f.adapter.readiness(f.manifest)
                self.assertFalse(ready["ready"], ready)
                expected = "FUTURES_AUTO_ADD_MARGIN_MUST_BE_DISABLED" if value is True else "FUTURES_AUTO_ADD_MARGIN_UNKNOWN"
                self.assertIn(expected, ready["blockers"])
                self.assertEqual([], f.calls)
                self.assertEqual([], f.protection_calls)

    def test_auto_add_margin_drift_blocks_entry_but_preserves_exact_exit(self):
        for value in ("missing", None, True):
            with self.subTest(value=value):
                f = self.fixture()
                if value == "missing":
                    f.quote.pop("isAutoAddMargin")
                else:
                    f.quote["isAutoAddMargin"] = value
                blocked = f.adapter.size_order(f.proposal(cid="new-client-0002"))
                self.assertFalse(blocked["ready"], blocked)
                self.assertEqual(1, len(f.calls))
                reducing = f.proposal("10000", side="SELL", reducing=True, cid="exit-client-0003")
                reducing["closeQuantity"] = "10"
                sized = f.adapter.size_order(reducing)
                self.assertTrue(sized["ready"], sized)
                self.assertEqual("10", sized["quantity"])
                expected = "FUTURES_AUTO_ADD_MARGIN_ENABLED" if value is True else "FUTURES_AUTO_ADD_MARGIN_UNKNOWN"
                self.assertIn(expected, [row["code"] for row in f.adapter._protection.incidents])
                reply = f.adapter.submit(sized)
                self.assertEqual("FILLED", reply["status"], reply)
                self.assertEqual(2, len(f.calls))
                self.assertEqual([], f.truth["positions"])
                f.quote["isAutoAddMargin"] = False
                self.assertFalse(f.adapter.size_order(f.proposal(cid="later-client-0004"))["ready"])

    def test_liquidation_and_adl_require_official_event_and_exact_execution(self):
        for kind in ("LIQUIDATION", "ADL"):
            with self.subTest(kind=kind):
                f = self.fixture()
                _, calls = self.force(f, kind=kind)
                self.assertTrue(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
                self.assertEqual([(f.symbol, int(NOW * 1000))], calls)
                incident = f.adapter._futures_incidents[-1]
                self.assertEqual(kind, incident["type"])
                self.assertEqual("4", incident["quantity"])
                self.assertTrue(incident["verified"])

    def test_manual_reduction_is_not_promoted_to_liquidation(self):
        f = self.fixture()
        f.adapter._force_order_reader = lambda *_: {"known": True, "observedAt": NOW, "events": []}
        self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
        self.assertEqual([], f.adapter._futures_incidents)

    def test_increase_and_sign_flip_do_not_even_query_force_history(self):
        for actual in ("11", "-1", "-20"):
            f = self.fixture()
            _, calls = self.force(f)
            self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(actual)))
            self.assertEqual([], calls)
            self.assertEqual([], f.adapter._futures_incidents)

    def test_wrong_hedge_side_is_not_force_evidence(self):
        f = self.fixture(mode="HEDGE")
        self.force(f, position="SHORT")
        self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
        self.assertEqual([], f.adapter._futures_incidents)

    def test_short_reduction_uses_buy_and_short_side(self):
        f = self.fixture(direction="short", mode="HEDGE")
        self.force(f, side="BUY", position="SHORT")
        self.assertTrue(f.adapter._detect_forced_close(f.symbol, mod.Decimal(-10), mod.Decimal(-6)))
        self.assertEqual("SHORT", f.adapter._futures_incidents[-1]["positionSide"])

    def test_force_before_this_session_entry_is_rejected(self):
        f = self.fixture()
        self.force(f, time_ms=int(NOW * 1000) - 1)
        self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
        self.assertEqual([], f.adapter._futures_incidents)

    def test_partial_force_execution_updates_cumulative_fill_once(self):
        f = self.fixture()
        event, _ = self.force(f)
        self.assertTrue(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
        event["filledQuantity"] = "8"
        f.rows["91"]["filledQuantity"] = "8"
        self.assertTrue(f.adapter._detect_forced_close(f.symbol, mod.Decimal(6), mod.Decimal(2)))
        rows = [row for cid, row in f.adapter._fill_rows.items() if cid.startswith("external-")]
        self.assertEqual(1, len(rows))
        self.assertEqual("8", rows[0]["filledQuantity"])
        self.assertEqual(1, len(f.adapter._futures_incidents))
        self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(2), mod.Decimal(1)))

    def test_force_cumulative_regression_does_not_modify_ledger(self):
        f = self.fixture()
        event, _ = self.force(f)
        self.assertTrue(f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6)))
        event["filledQuantity"] = "3"
        f.rows["91"]["filledQuantity"] = "3"
        self.assertFalse(f.adapter._detect_forced_close(f.symbol, mod.Decimal(6), mod.Decimal(5)))
        self.assertEqual("4", f.adapter._futures_incidents[-1]["quantity"])

    def test_external_order_ids_are_scoped_to_symbol_and_position_side(self):
        f = self.fixture(entry=False)
        for symbol, position, qty in (("BTCUSDT", "LONG", "1"), ("SOLUSDT", "LONG", "2"), ("BTCUSDT", "SHORT", "3")):
            actual = self.actual(symbol, qty, position=position)
            f.adapter._add_external_fill(actual, incident_type="ADL", position_side=position, source_id="91")
        self.assertEqual(3, len(f.adapter._claims))
        self.assertEqual(3, len(f.adapter._fill_rows))
        self.assertEqual(3, len(f.adapter._futures_incidents))

    def test_protective_stop_fill_is_joined_to_owned_position_ledger(self):
        f = self.fixture()
        protected = next(iter(f.protections.values()))
        protected.update(status="FINISHED", actualOrderId="92", executionKnown=True,
                         actualOrder=self.actual(f.symbol, "10", oid="92"))
        f.truth.update(positions=[], openAlgoOrders=[])
        state = f.adapter.reconcile(f.manifest)
        self.assertTrue(state["known"], state)
        self.assertEqual({}, state["positionsBySymbol"])
        self.assertEqual("0", state["committedCapitalKrw"])
        self.assertEqual("PROTECTIVE_STOP", state["futuresIncidents"][0]["type"])
        self.assertTrue(f.adapter.reconcile(f.manifest)["known"])
        self.assertEqual(2, len(f.adapter._fill_rows))
        self.assertFalse(f.adapter.size_order(f.proposal(cid="new-client-0002"))["ready"])

    def test_unknown_force_history_preserves_first_unknown_and_adds_no_fill(self):
        f = self.fixture()
        f.adapter._force_order_reader = lambda *_: {"known": False, "events": []}
        with self.assertRaisesRegex(mod.ShortLiveBrokerBlocked, "FUTURES_FORCE_ORDER_HISTORY_UNKNOWN"):
            f.adapter._detect_forced_close(f.symbol, mod.Decimal(10), mod.Decimal(6))
        self.assertEqual([], f.adapter._futures_incidents)
        self.assertEqual(1, len(f.adapter._fill_rows))


if __name__ == "__main__":
    unittest.main()
