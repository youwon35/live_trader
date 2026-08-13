from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from live_trader.brokers import (
    BrokerNotReadyError,
    fetch_kis_overseas_working_order_truth,
    normalize_kis_overseas_working_order_truth,
)


QUERY_NOW = datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc)
QUERY_DATE = "20260811"


def _row(
    order_id: str,
    *,
    requested: int = 1,
    filled: int = 0,
    remaining: int | None = None,
    side: str = "02",
    symbol: str = "F",
    exchange: str = "NYSE",
    branch: str = "02111",
    order_date: str = QUERY_DATE,
    order_time: str = "093001",
) -> dict[str, str]:
    unresolved = requested - filled if remaining is None else remaining
    return {
        "ord_dt": order_date,
        "ord_tmd": order_time,
        "ord_gno_brno": branch,
        "odno": order_id,
        "orgn_odno": "",
        "pdno": symbol,
        "sll_buy_dvsn_cd": side,
        "sll_buy_dvsn_cd_name": "매수" if side == "02" else "매도",
        "ft_ord_qty": str(requested),
        "ft_ord_unpr3": "11.25",
        "ft_ccld_qty": str(filled),
        "ft_ccld_unpr3": "11.20" if filled else "0",
        "ft_ccld_amt3": str(filled * 11.20),
        "nccs_qty": str(unresolved),
        "prcs_stat_name": "접수",
        "rjct_rson": "",
        "tr_crcy_cd": "USD",
        "ovrs_excg_cd": exchange,
    }


def _complete_payload(*rows: dict[str, str]) -> dict[str, object]:
    return {
        "rt_cd": "0",
        "output": list(rows),
        "page_count": 1,
        "pagination_complete": True,
        "fetched_at": "2026-08-12T02:00:00Z",
    }


class KisOverseasWorkingOrderTruthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_environment = dict(os.environ)
        self.addCleanup(self._restore_environment)
        os.environ.update(
            {
                "KIS_APP_KEY": "app-key",
                "KIS_APP_SECRET": "app-secret",
                "KIS_ACCOUNT_NO": "12345678-01",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
                "KIS_ENV": "real",
            }
        )

    def _restore_environment(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_environment)

    def test_fetch_consumes_all_pages_and_normalizes_working_orders(self) -> None:
        first = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output": [_row("ORDER-1")],
                "ctx_area_fk200": "FK-2",
                "ctx_area_nk200": "NK-2",
            },
        }
        second = {
            "ok": True,
            "trCont": "",
            "json": {
                "rt_cd": "0",
                "output": [
                    _row(
                        "ORDER-2",
                        requested=2,
                        filled=1,
                        side="01",
                        symbol="MSFT",
                        exchange="NASD",
                        order_time="093502",
                    )
                ],
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[first, second],
        ) as send:
            fetched = fetch_kis_overseas_working_order_truth(
                "token",
                now=QUERY_NOW,
            )

        self.assertTrue(fetched["pagination_complete"])
        self.assertEqual(2, fetched["page_count"])
        self.assertEqual(2, len(fetched["output"]))
        first_request = send.call_args_list[0].args[0]
        self.assertEqual("GET", first_request.method)
        self.assertEqual("TTTS3018R", first_request.headers["tr_id"])
        self.assertEqual("NASD", first_request.query["OVRS_EXCG_CD"])
        self.assertEqual("DS", first_request.query["SORT_SQN"])
        self.assertNotIn("ORD_STRT_DT", first_request.query)
        self.assertNotIn("ORD_END_DT", first_request.query)
        second_request = send.call_args_list[1].args[0]
        self.assertEqual("N", second_request.headers["tr_cont"])
        self.assertEqual("FK-2", second_request.query["CTX_AREA_FK200"])
        self.assertEqual("NK-2", second_request.query["CTX_AREA_NK200"])

        truth = normalize_kis_overseas_working_order_truth(fetched)
        self.assertTrue(truth["complete"])
        self.assertFalse(truth["absence_is_authoritative"])
        self.assertFalse(truth["accountWideAbsenceAuthoritative"])
        self.assertTrue(truth["accountWideWorkingOrdersAuthoritative"])
        self.assertEqual(
            ["KIS_US_WORKING_ORDERS_PRESENT"],
            truth["absenceAuthorityBlockers"],
        )
        self.assertEqual("US", truth["market"])
        self.assertEqual(2, truth["working_order_count"])
        self.assertEqual([], truth["events"])
        self.assertEqual("F", truth["working_orders"][0]["symbol"])
        self.assertEqual("accepted", truth["working_orders"][0]["state"])
        self.assertEqual(
            "partially_filled",
            truth["working_orders"][1]["state"],
        )
        self.assertEqual(1.0, truth["working_orders"][1]["remaining_quantity"])
        self.assertEqual("2026-08-11T13:30:01Z", truth["working_orders"][0]["occurred_at"])

    def test_completed_empty_output_is_authoritative_absence(self) -> None:
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={
                "ok": True,
                "trCont": "D",
                "json": {"rt_cd": "0", "output": []},
            },
        ):
            fetched = fetch_kis_overseas_working_order_truth(
                "token",
                now=QUERY_NOW,
            )

        truth = normalize_kis_overseas_working_order_truth(fetched)
        self.assertTrue(truth["absence_is_authoritative"])
        self.assertTrue(truth["accountWideAbsenceAuthoritative"])
        self.assertEqual([], truth["absenceAuthorityBlockers"])
        self.assertEqual(0, truth["working_order_count"])
        self.assertEqual([], truth["working_orders"])

    def test_fetch_fails_closed_without_output_or_complete_pagination(self) -> None:
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={"ok": True, "trCont": "", "json": {"rt_cd": "0"}},
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "output"):
                fetch_kis_overseas_working_order_truth("token", now=QUERY_NOW)

        repeated = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output": [],
                "ctx_area_fk200": "FK-SAME",
                "ctx_area_nk200": "NK-SAME",
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[repeated, repeated],
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "반복"):
                fetch_kis_overseas_working_order_truth("token", now=QUERY_NOW)

        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value={
                "ok": True,
                "trCont": "UNKNOWN",
                "json": {"rt_cd": "0", "output": []},
            },
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "알 수 없는"):
                fetch_kis_overseas_working_order_truth("token", now=QUERY_NOW)

    def test_normalization_rejects_ambiguous_or_nonworking_rows(self) -> None:
        older_gtc = normalize_kis_overseas_working_order_truth(
            _complete_payload(
                _row("OLD-GTC", order_date="20250501", order_time="120000")
            )
        )
        self.assertEqual("20250501", older_gtc["working_orders"][0]["order_date"])

        future_date = _row("ORDER-1", order_date="20260812")
        with self.assertRaisesRegex(BrokerNotReadyError, "신뢰"):
            normalize_kis_overseas_working_order_truth(
                _complete_payload(future_date)
            )

        zero_remaining = _row("ORDER-1", remaining=0)
        with self.assertRaisesRegex(BrokerNotReadyError, "양수 미체결수량"):
            normalize_kis_overseas_working_order_truth(
                _complete_payload(zero_remaining)
            )

        missing_identity = _row("ORDER-1")
        missing_identity["ord_gno_brno"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "주문조직번호"):
            normalize_kis_overseas_working_order_truth(
                _complete_payload(missing_identity)
            )

        missing_symbol = _row("ORDER-1")
        missing_symbol["pdno"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "종목코드"):
            normalize_kis_overseas_working_order_truth(
                _complete_payload(missing_symbol)
            )

        wrong_exchange = _row("ORDER-1", exchange="SEHK")
        with self.assertRaisesRegex(BrokerNotReadyError, "미국 거래소"):
            normalize_kis_overseas_working_order_truth(
                _complete_payload(wrong_exchange)
            )

        incomplete = _complete_payload()
        incomplete["pagination_complete"] = False
        with self.assertRaisesRegex(BrokerNotReadyError, "전체 페이지"):
            normalize_kis_overseas_working_order_truth(incomplete)


if __name__ == "__main__":
    unittest.main()
