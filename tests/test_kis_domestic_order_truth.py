from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from live_trader.brokers import (
    BrokerNotReadyError,
    LiveBrokerRouter,
    fetch_kis_domestic_order_truth,
    lookup_kis_domestic_order_truth,
    normalize_kis_domestic_order_truth,
)
from trading_runtime import normalize_broker_execution
from live_trader.live_adapters import (
    KIS_DOMESTIC_EXECUTION_ENDPOINT,
    _acquire_shared_kis_rest_slot,
    build_kis_domestic_execution_request,
)


def _row(
    order_id: str,
    *,
    requested: int = 3,
    filled: int = 0,
    remaining: int | None = None,
    canceled: int = 0,
    rejected: int = 0,
    cancel_flag: str = "N",
    price: int = 70_000,
    order_date: str = "20260807",
    branch: str = "001",
) -> dict[str, str]:
    unresolved = requested - filled if remaining is None else remaining
    return {
        "ord_dt": order_date,
        "ord_tmd": "101530",
        "ord_gno_brno": branch,
        "odno": order_id,
        "pdno": "005930",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": str(requested),
        "tot_ccld_qty": str(filled),
        "rmn_qty": str(unresolved),
        "cncl_cfrm_qty": str(canceled),
        "rjct_qty": str(rejected),
        "cncl_yn": cancel_flag,
        "avg_prvs": str(price if filled else 0),
        "tot_ccld_amt": str(price * filled),
    }


def _complete_payload(*rows: dict[str, str]) -> dict[str, object]:
    return {
        "rt_cd": "0",
        "output1": list(rows),
        "query_start_date": "20260801",
        "query_end_date": "20260807",
        "page_count": 1,
        "pagination_complete": True,
    }


class KisDomesticOrderTruthTest(unittest.TestCase):
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

    def test_builder_requests_all_domestic_orders_and_supports_continuation(self) -> None:
        prepared = build_kis_domestic_execution_request(
            access_token="token",
            start_date="20260801",
            end_date="20260807",
            context_fk100="FK-2",
            context_nk100="NK-2",
            continuation="N",
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual("GET", prepared.method)
        self.assertEqual(KIS_DOMESTIC_EXECUTION_ENDPOINT, prepared.endpoint)
        self.assertEqual("TTTC0081R", prepared.headers["tr_id"])
        self.assertEqual("N", prepared.headers["tr_cont"])
        self.assertEqual("00", prepared.query["CCLD_DVSN"])
        self.assertEqual("ALL", prepared.query["EXCG_ID_DVSN_CD"])
        self.assertEqual("", prepared.query["ODNO"])
        self.assertEqual("", prepared.query["PDNO"])
        self.assertEqual("FK-2", prepared.query["CTX_AREA_FK100"])
        self.assertEqual("NK-2", prepared.query["CTX_AREA_NK100"])

        os.environ["KIS_ENV"] = "demo"
        os.environ.pop("KIS_BASE_URL")
        demo = build_kis_domestic_execution_request(
            access_token="token",
            start_date="20260807",
            end_date="20260807",
        )
        self.assertEqual("VTTC0081R", demo.headers["tr_id"])
        self.assertTrue(
            demo.url.startswith(
                "https://openapivts.koreainvestment.com:29443"
            )
        )
        with patch(
            "live_trader.live_adapters.GLOBAL_KIS_REST_LIMITERS.get"
        ) as get_limiter:
            get_limiter.return_value.acquire.return_value = 0.0
            _acquire_shared_kis_rest_slot(demo.url)
        self.assertEqual("VPS", get_limiter.call_args.args[2])

    def test_fetch_consumes_every_page_and_fails_closed_on_broken_keys(self) -> None:
        first = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [_row("ORDER-1")],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "NK-2",
            },
        }
        second = {
            "ok": True,
            "trCont": "",
            "json": {"rt_cd": "0", "output1": [_row("ORDER-2")]},
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[first, second],
        ) as send:
            result = fetch_kis_domestic_order_truth(
                "token",
                start_date="20260801",
                end_date="20260807",
            )

        self.assertTrue(result["pagination_complete"])
        self.assertEqual(2, result["page_count"])
        self.assertEqual(2, len(result["output1"]))
        next_request = send.call_args_list[1].args[0]
        self.assertEqual("N", next_request.headers["tr_cont"])
        self.assertEqual("FK-2", next_request.query["CTX_AREA_FK100"])
        self.assertEqual("NK-2", next_request.query["CTX_AREA_NK100"])

        repeated = {
            **first,
            "json": {
                **first["json"],
                "output1": [],
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[repeated, repeated],
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "반복"):
                fetch_kis_domestic_order_truth(
                    "token",
                    start_date="20260801",
                    end_date="20260807",
                )

        missing_output = {"ok": True, "trCont": "", "json": {"rt_cd": "0"}}
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=missing_output,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "output1"):
                fetch_kis_domestic_order_truth(
                    "token",
                    start_date="20260801",
                    end_date="20260807",
                )

        missing_result_code = {
            "ok": True,
            "trCont": "",
            "json": {"output1": []},
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=missing_result_code,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "rt_cd"):
                fetch_kis_domestic_order_truth(
                    "token",
                    start_date="20260801",
                    end_date="20260807",
                )

        blank_result_code = {
            "ok": True,
            "trCont": "",
            "json": {"rt_cd": "", "output1": []},
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=blank_result_code,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "rt_cd=0"):
                fetch_kis_domestic_order_truth(
                    "token",
                    start_date="20260801",
                    end_date="20260807",
                )

        unknown_continuation = {
            "ok": True,
            "trCont": "UNKNOWN",
            "json": {"rt_cd": "0", "output1": []},
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=unknown_continuation,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "알 수 없는"):
                fetch_kis_domestic_order_truth(
                    "token",
                    start_date="20260801",
                    end_date="20260807",
                )

    def test_all_domestic_order_states_and_official_ids_are_normalized(self) -> None:
        truth = normalize_kis_domestic_order_truth(
            _complete_payload(
                _row("PENDING"),
                _row("PARTIAL", filled=1, remaining=2),
                _row("FILLED", filled=3, remaining=0),
                _row(
                    "CANCELED",
                    filled=1,
                    remaining=0,
                    canceled=2,
                    cancel_flag="Y",
                ),
                _row(
                    "REJECTED",
                    filled=0,
                    remaining=0,
                    rejected=3,
                ),
            )
        )

        self.assertTrue(truth["complete"])
        self.assertTrue(truth["absence_is_authoritative"])
        self.assertEqual("official-broker-order-id-only", truth["correlation_policy"])
        states = {
            row["broker_order_id"]: row["state"] for row in truth["orders"]
        }
        self.assertEqual(
            {
                "PENDING": "accepted",
                "PARTIAL": "partially_filled",
                "FILLED": "filled",
                "CANCELED": "canceled",
                "REJECTED": "rejected",
            },
            states,
        )
        self.assertTrue(
            all(row["local_order_id"] == "" for row in truth["orders"])
        )
        partial_event = next(
            event
            for event in truth["events"]
            if event["broker_order_id"] == "PARTIAL"
        )
        self.assertEqual("partially_filled", partial_event["state"])
        self.assertEqual("cumulative", partial_event["quantity_mode"])
        self.assertEqual(1.0, partial_event["cumulative_quantity"])
        pending_event = next(
            event
            for event in truth["events"]
            if event["broker_order_id"] == "PENDING"
        )
        normalized_pending = normalize_broker_execution("kis", pending_event)
        self.assertEqual("ACKNOWLEDGED", normalized_pending.status)
        self.assertEqual(
            pending_event["occurred_at"],
            normalized_pending.occurred_at,
        )
        canceled_events = [
            event
            for event in truth["events"]
            if event["broker_order_id"] == "CANCELED"
        ]
        self.assertEqual(
            ["partially_filled", "canceled"],
            [event["state"] for event in canceled_events],
        )
        self.assertEqual(0.0, canceled_events[-1]["quantity"])

    def test_inconsistent_or_incomplete_truth_never_publishes_events(self) -> None:
        inconsistent = _row("BROKEN", filled=1, remaining=1)
        with self.assertRaisesRegex(BrokerNotReadyError, "합계"):
            normalize_kis_domestic_order_truth(
                _complete_payload(inconsistent)
            )

        missing_price = _row("NO-PRICE", filled=1, remaining=2)
        missing_price["avg_prvs"] = ""
        missing_price["tot_ccld_amt"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "체결가"):
            normalize_kis_domestic_order_truth(
                _complete_payload(missing_price)
            )

        with self.assertRaisesRegex(BrokerNotReadyError, "전체 페이지"):
            normalize_kis_domestic_order_truth(
                {"output1": [_row("PARTIAL", filled=1)]}
            )

        blank_success = _complete_payload()
        blank_success["rt_cd"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "rt_cd=0"):
            normalize_kis_domestic_order_truth(blank_success)

        no_page = _complete_payload()
        no_page["page_count"] = 0
        with self.assertRaisesRegex(BrokerNotReadyError, "완료 페이지"):
            normalize_kis_domestic_order_truth(no_page)

        outside_window = _complete_payload(
            _row("OUTSIDE", order_date="20260731")
        )
        with self.assertRaisesRegex(BrokerNotReadyError, "조회 범위 밖"):
            normalize_kis_domestic_order_truth(outside_window)

        missing_branch = _row("NO-BRANCH")
        missing_branch["ord_gno_brno"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "주문조직번호"):
            normalize_kis_domestic_order_truth(
                _complete_payload(missing_branch)
            )

        missing_order_time = _row("NO-TIME")
        missing_order_time["ord_tmd"] = ""
        with self.assertRaisesRegex(BrokerNotReadyError, "주문시각"):
            normalize_kis_domestic_order_truth(
                _complete_payload(missing_order_time)
            )

    def test_conflicting_terminal_quantities_fail_closed(self) -> None:
        filled_with_cancel = _row(
            "FILLED-CANCELED",
            filled=3,
            remaining=0,
            canceled=1,
            cancel_flag="Y",
        )
        with self.assertRaisesRegex(BrokerNotReadyError, "취소·거부"):
            normalize_kis_domestic_order_truth(
                _complete_payload(filled_with_cancel)
            )

        rejected_with_remaining = _row(
            "REJECTED-REMAINING",
            filled=0,
            remaining=3,
            rejected=3,
        )
        with self.assertRaisesRegex(BrokerNotReadyError, "일관되지"):
            normalize_kis_domestic_order_truth(
                _complete_payload(rejected_with_remaining)
            )

        working_with_reject_overlap = _row(
            "WORKING-REJECTED",
            filled=1,
            remaining=2,
            rejected=1,
        )
        with self.assertRaisesRegex(BrokerNotReadyError, "일관되지"):
            normalize_kis_domestic_order_truth(
                _complete_payload(working_with_reject_overlap)
            )

    def test_restart_lookup_uses_official_identity_and_post_loss_stays_unresolved(self) -> None:
        truth = normalize_kis_domestic_order_truth(
            _complete_payload(_row("ORDER-1", filled=1, remaining=2))
        )

        matched = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="ORDER-1",
            order_date="20260807",
            organization_no="001",
        )
        lost_response = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="",
        )
        absent = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="ORDER-NOT-THERE",
            order_date="20260807",
        )
        outside_window = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="ORDER-1",
            order_date="20260731",
        )

        self.assertEqual("MATCHED", matched["status"])
        self.assertEqual("ORDER-1", matched["order"]["broker_order_id"])
        self.assertEqual("UNRESOLVED", lost_response["status"])
        self.assertFalse(lost_response["absence_authoritative"])
        self.assertIn("자동 귀속하지 않습니다", lost_response["reason"])
        self.assertEqual("ABSENT", absent["status"])
        self.assertTrue(absent["absence_authoritative"])
        self.assertEqual("UNRESOLVED", outside_window["status"])
        self.assertFalse(outside_window["absence_authoritative"])
        self.assertIn("조회 범위 밖", outside_window["reason"])

    def test_reused_odno_is_kept_as_truth_but_not_emitted_without_date_scope(self) -> None:
        truth = normalize_kis_domestic_order_truth(
            _complete_payload(
                _row("REUSED", order_date="20260806", filled=3, remaining=0),
                _row("REUSED", order_date="20260807", filled=1, remaining=2),
            )
        )

        self.assertEqual(2, truth["order_count"])
        self.assertEqual(["REUSED"], truth["ambiguous_broker_order_ids"])
        self.assertEqual([], truth["events"])
        ambiguous = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="REUSED",
        )
        exact = lookup_kis_domestic_order_truth(
            truth,
            broker_order_id="REUSED",
            order_date="20260807",
            organization_no="001",
        )
        self.assertEqual("AMBIGUOUS", ambiguous["status"])
        self.assertEqual("MATCHED", exact["status"])
        self.assertEqual("20260807", exact["order"]["order_date"])

    def test_router_returns_balance_plus_complete_order_truth(self) -> None:
        domestic = {
            "rt_cd": "0",
            "output1": [],
            "output2": [{"dnca_tot_amt": "100000"}],
        }
        overseas = {"rt_cd": "0", "output1": [], "output2": []}

        def execution_truth(_token, *, start_date, end_date):
            return {
                **_complete_payload(_row("ORDER-1", filled=3, remaining=0)),
                "query_start_date": start_date,
                "query_end_date": end_date,
            }

        with (
            patch(
                "live_trader.brokers.issue_kis_access_token",
                return_value="token",
            ),
            patch(
                "live_trader.brokers.fetch_kis_domestic_order_truth",
                side_effect=execution_truth,
            ) as order_poll,
            patch(
                "live_trader.brokers.fetch_kis_domestic_balance",
                return_value=domestic,
            ),
            patch(
                "live_trader.brokers.fetch_kis_overseas_balance",
                return_value=overseas,
            ),
        ):
            result = LiveBrokerRouter().poll_execution_events("kis")

        order_poll.assert_called_once()
        self.assertEqual(
            "kis_balance_and_domestic_order_truth_poll",
            result["source"],
        )
        self.assertTrue(result["execution_truth"]["complete"])
        self.assertEqual("ORDER-1", result["orders"][0]["broker_order_id"])
        self.assertTrue(
            any(
                event["state"] == "filled"
                and event["broker_order_id"] == "ORDER-1"
                for event in result["events"]
            )
        )
        router_fill = next(
            event
            for event in result["events"]
            if event.get("broker_order_id") == "ORDER-1"
        )
        normalized_fill = normalize_broker_execution("kis", router_fill)
        self.assertEqual("FILLED", normalized_fill.status)
        self.assertTrue(normalized_fill.occurred_at.endswith("Z"))
        self.assertTrue(
            any(event["state"] == "account_snapshot" for event in result["events"])
        )


if __name__ == "__main__":
    unittest.main()
