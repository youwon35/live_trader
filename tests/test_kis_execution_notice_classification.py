from __future__ import annotations

from copy import deepcopy
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.execution_streams import parse_kis_domestic_execution, parse_kis_overseas_execution


def notice(*, domestic=False, kind="2", rejection="0", quantity="2", price=None):
    fields = [""] * (26 if domestic else 25)
    fields[2] = "123456"
    fields[4] = "02"
    fields[8 if domestic else 7] = "005930" if domestic else "AAPL"
    fields[9 if domestic else 8] = quantity
    fields[10 if domestic else 9] = ("12345" if domestic else "001234500") if price is None else price
    fields[11 if domestic else 10] = "093001"
    fields[12 if domestic else 11] = rejection
    fields[13 if domestic else 12] = kind
    if domestic:fields[19] = "1"
    else:fields[18] = "6"
    return fields


class KisExecutionNoticeClassificationTest(unittest.TestCase):
    def test_only_official_execution_class_is_fill_delta_and_never_terminal(self):
        for domestic in (False, True):
            with self.subTest(domestic=domestic):
                parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
                event = parse(notice(domestic=domestic))
                self.assertIsNotNone(event)
                self.assertEqual("partially_filled", event["state"])
                self.assertEqual(2, event["quantity"])
                self.assertEqual("delta", event["quantity_mode"])
                self.assertIs(event["terminal"], False)
                self.assertIsNone(event["fee"])
                self.assertIs(event["actual_fee_observed"], False)
                self.assertIs(event["financial_projection_proven"], False)
                self.assertIsNone(event["provider_time_zone"])

    def test_positive_order_amend_cancel_intake_quantity_is_never_fill(self):
        for domestic in (False, True):
            parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            for correction in ("0", "1", "2"):
                with self.subTest(domestic=domestic, correction=correction):
                    fields = notice(domestic=domestic, kind="1", quantity="17")
                    fields[5] = correction
                    event = parse(fields)
                    self.assertEqual("order_intake", event["state"])
                    self.assertEqual((0, 0), (event["quantity"], event["price"]))
                    self.assertEqual("17", event["reported_notice_quantity"])

    def test_rejected_intake_positive_order_quantity_is_not_an_execution(self):
        for domestic in (False, True):
            parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            for flag in ("1",):
                event = parse(notice(domestic=domestic, kind="1", rejection=flag))
                self.assertEqual("intake_rejected", event["state"])
                self.assertEqual(0, event["quantity"])
                self.assertIs(event["terminal"], False)
                self.assertIsNone(parse(notice(domestic=domestic, kind="2", rejection=flag)))

    def test_unknown_execution_or_rejection_flags_fail_closed(self):
        for domestic in (False, True):
            parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            for kind in ("", "0", "Y", "3", "2 "):
                self.assertIsNone(parse(notice(domestic=domestic, kind=kind)))
            for flag in ("", "?", "2", "False", "N", "Y"):
                self.assertIsNone(parse(notice(domestic=domestic, rejection=flag)))

    def test_malformed_nonfinite_negative_and_zero_execution_numbers_fail_closed(self):
        for domestic in (False, True):
            parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            for quantity in ("", "NaN", "Inf", "-1", "0", "1.5", "1,000", "oops", "1e2"):
                self.assertIsNone(parse(notice(domestic=domestic, quantity=quantity)))
            for price in ("", "NaN", "Infinity", "-1", "0", "not-a-price"):
                self.assertIsNone(parse(notice(domestic=domestic, price=price)))

    def test_official_integer_width_prevents_share_rounding_and_unknown_price_scale(self):
        for domestic in (False,True):
            parse=parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            self.assertIsNone(parse(notice(domestic=domestic,quantity="9007199254740993")))
            self.assertIsNone(parse(notice(domestic=domestic,price="1234567890")))
            self.assertIsNone(parse(notice(domestic=domestic,price="123.45")))

    def test_missing_identity_unknown_side_bad_time_and_shape_fail_closed(self):
        for domestic in (False, True):
            parse = parse_kis_domestic_execution if domestic else parse_kis_overseas_execution
            for index, bad in ((2, ""), (4, "unknown"), (8 if domestic else 7, ""), (11 if domestic else 10, "246060")):
                fields = notice(domestic=domestic)
                fields[index] = bad
                self.assertIsNone(parse(fields))
            self.assertIsNone(parse(["short"]))
            self.assertIsNone(parse(None))
            for side in ("S","B","1","2","03","04"):
                fields=notice(domestic=domestic);fields[4]=side
                self.assertIsNone(parse(fields))

    def test_official_us_primary_implied_four_decimal_price_alternate_scale_never_guessed(self):
        fields = notice(price="001480100")
        fields[24] = "000001480100"
        event = parse_kis_overseas_execution(fields)
        self.assertEqual(148.01, event["price"])
        self.assertEqual(fields[24], event["raw_alternate_price"])
        self.assertEqual("NASD", event["exchange"])
        fields[24] = "NaN"
        self.assertIsNone(parse_kis_overseas_execution(fields))

    def test_missing_overseas_provider_time_is_opaque_and_venue_is_required(self):
        fields=notice();fields[10]=""
        event=parse_kis_overseas_execution(fields)
        self.assertIsNotNone(event)
        self.assertEqual("",event["provider_time"])
        self.assertIsNone(event["provider_time_zone"])
        fields[18]=""
        self.assertIsNone(parse_kis_overseas_execution(fields))

    def test_domestic_nxt_sor_unknown_or_legacy_truncation_never_labeled_krx(self):
        for route in ("2","3","4","", "unknown"):
            fields=notice(domestic=True);fields[19]=route
            self.assertIsNone(parse_kis_domestic_execution(fields))
        for length in (14,23,25,27):
            fields=notice(domestic=True)
            fields=(fields+[""]*length)[:length]
            self.assertIsNone(parse_kis_domestic_execution(fields))
        self.assertEqual("KRX",parse_kis_domestic_execution(notice(domestic=True))["exchange"])

    def test_same_second_ack_and_execution_are_distinct_and_exact_replay_stable(self):
        ack = parse_kis_overseas_execution(notice(kind="1"))
        fill = parse_kis_overseas_execution(notice(kind="2"))
        replay = parse_kis_overseas_execution(notice(kind="2"))
        self.assertNotEqual(ack["event_id"], fill["event_id"])
        self.assertEqual(fill["event_id"], replay["event_id"])

    def test_normal_state_increment_never_substitutes_order_quantity_for_intake_zero(self):
        event = parse_kis_overseas_execution(notice(kind="1", quantity="17"))
        order = {"broker_id": "kis", "broker_order_id": "123456", "symbol": "AAPL", "qty": 17,
                 "filled_quantity": 0, "average_fill_price": 0, "fee": 0, "state": "submitted"}
        before = deepcopy(order)
        self.assertEqual((0, 0, 0), state.execution_event_increment(event, order, None))
        with patch.object(state, "STATE", {"orders": [order]}), patch.object(state, "execution_event_matches_local_order", return_value=True):
            self.assertEqual(0, state.apply_execution_events_to_local_orders([event]))
        self.assertEqual(before["filled_quantity"], order["filled_quantity"])
        self.assertEqual(before["average_fill_price"], order["average_fill_price"])
        self.assertEqual(before["fee"], order["fee"])
        self.assertNotEqual("filled", order["state"])


if __name__ == "__main__":
    unittest.main()
