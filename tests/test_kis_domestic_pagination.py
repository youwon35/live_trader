import os
import unittest
from unittest.mock import patch

from live_trader import kis_order_authority as kis_order_authority_module
from live_trader.brokers import (
    BrokerNotReadyError,
    LiveBrokerRouter,
    fetch_kis_domestic_balance,
    fetch_kis_overseas_balance,
    parse_kis_positions,
    parse_kis_overseas_positions,
)
from live_trader.live_adapters import build_kis_domestic_balance_request
from live_trader.kis_order_authority import (
    _reset_kis_order_authority_reader_for_tests,
    register_kis_order_authority_reader,
)


def _read_authority_snapshot() -> dict[str, object]:
    return {
        "durableAuthorityReadable": True,
        "functionalAuthorityOpen": False,
        "functionalPhase": "IDLE",
        "functionalRevision": 0,
        "stateRevision": 1,
        "functionalSessionId": "",
        "functionalAccountFingerprint": "a" * 64,
        "credentialConfigurationHash": "b" * 64,
        "functionalMutationIntent": {},
        "killOrdinaryCancelAllowed": False,
        "killOrdinaryCancelRevision": 0,
        "killOrdinaryCancelIntent": {},
        "applicationInstanceLeaseHeld": True,
        "ordinaryRoutesClosed": False,
        "ownerEpochId": "kis-read-pagination-owner-1",
        "ownerEpochHash": "e" * 64,
        "controlReservation": {},
    }


def _holding(index: int) -> dict[str, str]:
    return {
        "pdno": f"{index:06d}",
        "hldg_qty": "1",
        "evlu_amt": "1000",
        "pchs_avg_pric": "900",
        "prpr": "1000",
    }


class KisDomesticPaginationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_environment = dict(os.environ)
        self.addCleanup(self._restore_environment)
        self.original_kis_authority_reader = (
            kis_order_authority_module._AUTHORITY_READER
        )
        self.original_kis_kill_cancel_journal_path = (
            kis_order_authority_module._KILL_CANCEL_JOURNAL_PATH
        )
        _reset_kis_order_authority_reader_for_tests()
        register_kis_order_authority_reader(_read_authority_snapshot)
        self.addCleanup(self._restore_kis_authority_provider)
        os.environ.update(
            {
                "KIS_APP_KEY": "app-key",
                "KIS_APP_SECRET": "app-secret",
                "KIS_ACCOUNT_NO": "12345678",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
                "KIS_BASE_URL": "https://kis.example.test",
            }
        )

    def _restore_environment(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_environment)

    def _restore_kis_authority_provider(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        if self.original_kis_authority_reader is not None:
            register_kis_order_authority_reader(
                self.original_kis_authority_reader,
                kill_cancel_journal_path=(
                    self.original_kis_kill_cancel_journal_path
                ),
            )

    def test_builder_carries_both_context_keys_and_continuation_header(self) -> None:
        prepared = build_kis_domestic_balance_request(
            access_token="token",
            context_fk100="FK-NEXT",
            context_nk100="NK-NEXT",
            continuation="N",
        )

        self.assertTrue(prepared.can_send)
        self.assertEqual("FK-NEXT", prepared.query["CTX_AREA_FK100"])
        self.assertEqual("NK-NEXT", prepared.query["CTX_AREA_NK100"])
        self.assertEqual("N", prepared.headers["tr_cont"])
        self.assertEqual("N", prepared.safe_headers["tr_cont"])

    def test_more_than_twenty_holdings_are_fetched_before_zero_is_inferred(self) -> None:
        first_page = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [_holding(index) for index in range(1, 21)],
                "output2": [{"dnca_tot_amt": "100000"}],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "NK-2",
            },
        }
        second_page = {
            "ok": True,
            "trCont": "",
            "json": {
                "rt_cd": "0",
                "output1": [_holding(index) for index in range(21, 26)],
                "output2": [],
            },
        }
        requests = []

        def send(prepared):
            requests.append(prepared)
            return (first_page, second_page)[len(requests) - 1]

        with patch("live_trader.brokers.send_prepared_request", side_effect=send):
            payload = fetch_kis_domestic_balance("token")

        self.assertEqual(25, len(payload["output1"]))
        self.assertEqual(25, len(parse_kis_positions(payload)))
        self.assertEqual(2, len(requests))
        self.assertEqual("FK-2", requests[1].query["CTX_AREA_FK100"])
        self.assertEqual("NK-2", requests[1].query["CTX_AREA_NK100"])
        self.assertEqual("N", requests[1].headers["tr_cont"])

    def test_one_context_key_is_valid_but_missing_or_repeated_pair_fails(self) -> None:
        one_key = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "",
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[
                one_key,
                {"ok": True, "trCont": "", "json": {"rt_cd": "0", "output1": []}},
            ],
        ) as send:
            payload = fetch_kis_domestic_balance("token")
        self.assertEqual([], payload["output1"])
        self.assertEqual("FK-2", send.call_args_list[1].args[0].query["CTX_AREA_FK100"])
        self.assertEqual("", send.call_args_list[1].args[0].query["CTX_AREA_NK100"])

        nk_only = {
            "ok": True,
            "trCont": "F",
            "json": {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "",
                "ctx_area_nk100": "NK-ONLY",
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[
                nk_only,
                {"ok": True, "trCont": "", "json": {"rt_cd": "0", "output1": []}},
            ],
        ) as send:
            fetch_kis_domestic_balance("token")
        self.assertEqual("", send.call_args_list[1].args[0].query["CTX_AREA_FK100"])
        self.assertEqual(
            "NK-ONLY",
            send.call_args_list[1].args[0].query["CTX_AREA_NK100"],
        )

        missing = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "",
                "ctx_area_nk100": "",
            },
        }
        with patch("live_trader.brokers.send_prepared_request", return_value=missing):
            with self.assertRaisesRegex(BrokerNotReadyError, "누락"):
                fetch_kis_domestic_balance("token")

        repeated = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "NK-2",
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            side_effect=[repeated, repeated],
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "반복"):
                fetch_kis_domestic_balance("token")

    def test_page_limit_fails_closed_and_router_uses_complete_helper(self) -> None:
        continued = {
            "ok": True,
            "trCont": "M",
            "json": {
                "rt_cd": "0",
                "output1": [],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "NK-2",
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request", return_value=continued
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "1페이지"):
                fetch_kis_domestic_balance("token", max_pages=1)

        complete = {
            "rt_cd": "0",
            "output1": [_holding(index) for index in range(1, 26)],
            "output2": [],
        }
        with (
            patch("live_trader.brokers.issue_kis_access_token", return_value="token"),
            patch(
                "live_trader.brokers.fetch_kis_domestic_balance",
                return_value=complete,
            ) as domestic,
            patch(
                "live_trader.brokers.fetch_kis_overseas_balance",
                return_value={"rt_cd": "0", "output1": [], "output2": []},
            ),
        ):
            positions = LiveBrokerRouter().list_positions("kis")

        domestic.assert_called_once_with("token")
        self.assertEqual(25, len(positions))

    def test_malformed_domestic_or_overseas_holding_never_becomes_zero(self) -> None:
        malformed_domestic = {
            "rt_cd": "0",
            "output1": [{"pdno": "005930", "hldg_qty": "not-a-number"}],
        }
        with self.assertRaisesRegex(BrokerNotReadyError, "보유수량"):
            parse_kis_positions(malformed_domestic)

        missing_domestic_symbol = {
            "rt_cd": "0",
            "output1": [{"hldg_qty": "1"}],
        }
        with self.assertRaisesRegex(BrokerNotReadyError, "종목코드"):
            parse_kis_positions(missing_domestic_symbol)

        malformed_overseas = {
            "rt_cd": "0",
            "output1": [{"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "NaN"}],
        }
        with self.assertRaisesRegex(BrokerNotReadyError, "보유수량"):
            parse_kis_overseas_positions(malformed_overseas)

        malformed_page = {
            "ok": True,
            "trCont": "",
            "json": {
                "rt_cd": "0",
                "output1": [{"pdno": "005930", "hldg_qty": "broken"}],
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=malformed_page,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "보유수량"):
                fetch_kis_domestic_balance("token")

        malformed_overseas_page = {
            "ok": True,
            "trCont": "",
            "json": {
                "rt_cd": "0",
                "output1": [
                    {"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "broken"}
                ],
            },
        }
        with patch(
            "live_trader.brokers.send_prepared_request",
            return_value=malformed_overseas_page,
        ):
            with self.assertRaisesRegex(BrokerNotReadyError, "보유수량"):
                fetch_kis_overseas_balance("token")


if __name__ == "__main__":
    unittest.main()
