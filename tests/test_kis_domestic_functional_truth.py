from __future__ import annotations

import copy
import json
import unittest
from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from live_trader.kis_domestic_functional_contract import LIVE_ORIGIN
from live_trader.kis_domestic_functional_get_client import (
    KisDomesticFunctionalGetClient,
    kis_domestic_functional_account_fingerprint,
)
from live_trader.kis_domestic_functional_truth import (
    HOLIDAY_TARGET_DATE_FIRST_PAGE_SCOPE,
    KisDomesticFunctionalTruthBlocked,
    KisDomesticFunctionalTruthReader,
    production_entrypoint_status,
    seal_owned_action_record,
)


KST = ZoneInfo("Asia/Seoul")
YMD = "20260813"
ACCOUNT_FINGERPRINT = kis_domestic_functional_account_fingerprint("12345678", "01")
SERVER_AUTHORITY_KEY = b"q" * 32
BUY_KEY = f"{YMD}:001:1000001"
SELL_KEY = f"{YMD}:001:1000002"


def _response(body: Mapping[str, Any], *, tr_cont: str = "") -> dict[str, Any]:
    return {"statusCode": 200, "trCont": tr_cont, "body": dict(body)}


def _summary(*, buy_qty_key: str) -> dict[str, str]:
    value = {
        "sll_qty_smtl": "1",
        "sll_tr_amt_smtl": "81000",
        "sll_fee_smtl": "10",
        "sll_tltx_smtl": "100",
        "sll_excc_amt_smtl": "80890",
        buy_qty_key: "1",
        "buy_tr_amt_smtl": "80000",
        "buy_fee_smtl": "10",
        "buy_tax_smtl": "0",
        "buy_excc_amt_smtl": "80010",
        "tot_qty": "2",
        "tot_tr_amt": "161000",
        "tot_fee": "20",
        "tot_tltx": "100",
        "tot_excc_amt": "160900",
        "tot_rlzt_pfls": "880",
    }
    if buy_qty_key == "buy_qty_smtl":
        value["loan_int"] = "0"
    return value


def _order(odno: str, side: str, price: str, *, pdno: str = "010140") -> dict[str, str]:
    return {
        "ord_dt": YMD,
        "ord_gno_brno": "001",
        "odno": odno,
        "pdno": pdno,
        "sll_buy_dvsn_cd": side,
        "ord_qty": "1",
        "ord_unpr": price,
        "tot_ccld_qty": "1",
        "tot_ccld_amt": price,
        "avg_prvs": price,
        "rmn_qty": "0",
        "cncl_cfrm_qty": "0",
        "rjct_qty": "0",
    }


def _owned(*, baseline_quantity: str = "0", baseline_cash: str = "500000") -> dict[str, Any]:
    return seal_owned_action_record(
        {
            "schemaVersion": "kis-domestic-owned-actions/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "origin": LIVE_ORIGIN,
            "pdno": "010140",
            "accountFingerprint": ACCOUNT_FINGERPRINT,
            "sessionId": "kis-session-1",
            "permitId": "kis-permit-1",
            "permitHash": "b" * 64,
            "phase": "TERMINAL",
            "baselineQuantity": baseline_quantity,
            "baselineCashKrw": baseline_cash,
            "actions": [
                {
                    "actionKind": "NATURAL_BUY",
                    "orderDate": YMD,
                    "orgNo": "001",
                    "odno": "1000001",
                    "side": "02",
                    "quantity": "1",
                    "submittedGrossKrw": "80000",
                },
                {
                    "actionKind": "CLEANUP_SELL",
                    "orderDate": YMD,
                    "orgNo": "001",
                    "odno": "1000002",
                    "side": "01",
                    "quantity": "1",
                    "submittedGrossKrw": "81000",
                },
            ],
        }
    )


def _fixture() -> dict[str, list[dict[str, Any]]]:
    balance_summary = {
        "dnca_tot_amt": "500880",
        "thdt_buy_amt": "80000",
        "thdt_sll_amt": "81000",
        "thdt_tlex_amt": "120",
        "tot_evlu_amt": "500880",
        "nass_amt": "500880",
    }
    trade_row = {
        "trad_dt": YMD,
        "pdno": "010140",
        "buy_qty": "1",
        "buy_amt": "80000",
        "sll_qty": "1",
        "sll_amt": "81000",
        "fee": "20",
        "tl_tax": "100",
        "loan_int": "0",
        "rlzt_pfls": "880",
    }
    daily_row = {
        "trad_dt": YMD,
        "buy_qty1": "1",
        "buy_amt": "80000",
        "sll_qty1": "1",
        "sll_amt": "81000",
        "fee": "20",
        "tl_tax": "100",
        "loan_int": "0",
        "rlzt_pfls": "880",
    }
    return {
        "TTTC8434R": [
            _response(
                {
                    "rt_cd": "0",
                    "output1": [
                        {
                            "pdno": "005930",
                            "hldg_qty": "2",
                            "ord_psbl_qty": "2",
                            "pchs_avg_pric": "70000",
                            "pchs_amt": "140000",
                            "prpr": "71000",
                            "evlu_amt": "142000",
                        }
                    ],
                    "output2": [balance_summary],
                    "ctx_area_fk100": "FK-2",
                    "ctx_area_nk100": "NK-2",
                },
                tr_cont="M",
            ),
            _response({"rt_cd": "0", "output1": [], "output2": []}, tr_cont="D"),
        ],
        "TTTC0081R": [
            _response(
                {
                    "rt_cd": "0",
                    "output1": [_order("1000001", "02", "80000"), _order("1000002", "01", "81000")],
                    "output2": {},
                },
                tr_cont="E",
            )
        ],
        "TTTC0084R": [_response({"rt_cd": "0", "output": []}, tr_cont="D")],
        "TTTC8715R": [
            _response({"rt_cd": "0", "output1": [trade_row], "output2": _summary(buy_qty_key="buyqty_smtl")})
        ],
        "TTTC8708R": [
            _response({"rt_cd": "0", "output1": [daily_row], "output2": _summary(buy_qty_key="buy_qty_smtl")})
        ],
        "CTCA0903R": [
            _response({"rt_cd": "0", "output": [{"bass_dt": YMD, "opnd_yn": "Y"}]})
        ],
    }


def _baseline_fixture(
    *,
    quantity: str = "0",
    cash: str = "500000",
) -> dict[str, list[dict[str, Any]]]:
    value = _fixture()
    value["TTTC0081R"][0]["body"]["output1"] = []
    balance = value["TTTC8434R"][0]["body"]
    if quantity != "0":
        balance["output1"].append(
            {
                "pdno": "010140",
                "hldg_qty": quantity,
                "ord_psbl_qty": quantity,
                "pchs_avg_pric": "70000",
                "pchs_amt": str(int(quantity) * 70000),
                "prpr": "80000",
                "evlu_amt": str(int(quantity) * 80000),
            }
        )
    summary = balance["output2"][0]
    summary["dnca_tot_amt"] = cash
    for key in ("thdt_buy_amt", "thdt_sll_amt", "thdt_tlex_amt"):
        summary[key] = "0"
    for tr_id in ("TTTC8715R", "TTTC8708R"):
        row = value[tr_id][0]["body"]["output1"][0]
        for key in tuple(row):
            if key not in {"trad_dt", "pdno"}:
                row[key] = "0"
        profit_summary = value[tr_id][0]["body"]["output2"]
        for key in tuple(profit_summary):
            profit_summary[key] = "0"
    return value


def _set_cumulative_profit(
    fixture: dict[str, list[dict[str, Any]]],
    *,
    buy_qty: int,
    buy_amt: int,
    sell_qty: int,
    sell_amt: int,
    buy_fee: int,
    sell_fee: int,
    sell_tax: int,
    realized: int,
) -> None:
    fee = buy_fee + sell_fee
    tax = sell_tax
    for tr_id in ("TTTC8715R", "TTTC8708R"):
        row = fixture[tr_id][0]["body"]["output1"][0]
        row.update(
            {
                "buy_qty" if tr_id == "TTTC8715R" else "buy_qty1": str(buy_qty),
                "buy_amt": str(buy_amt),
                "sll_qty" if tr_id == "TTTC8715R" else "sll_qty1": str(sell_qty),
                "sll_amt": str(sell_amt),
                "fee": str(fee),
                "tl_tax": str(tax),
                "loan_int": "0",
                "rlzt_pfls": str(realized),
            }
        )
        summary = fixture[tr_id][0]["body"]["output2"]
        summary.update(
            {
                "sll_qty_smtl": str(sell_qty),
                "sll_tr_amt_smtl": str(sell_amt),
                "sll_fee_smtl": str(sell_fee),
                "sll_tltx_smtl": str(sell_tax),
                "sll_excc_amt_smtl": str(sell_amt - sell_fee - sell_tax),
                "buyqty_smtl" if tr_id == "TTTC8715R" else "buy_qty_smtl": str(buy_qty),
                "buy_tr_amt_smtl": str(buy_amt),
                "buy_fee_smtl": str(buy_fee),
                "buy_tax_smtl": "0",
                "buy_excc_amt_smtl": str(buy_amt + buy_fee),
                "tot_qty": str(buy_qty + sell_qty),
                "tot_tr_amt": str(buy_amt + sell_amt),
                "tot_fee": str(fee),
                "tot_tltx": str(tax),
                "tot_excc_amt": str(
                    sell_amt - sell_fee - sell_tax + buy_amt + buy_fee
                ),
                "tot_rlzt_pfls": str(realized),
            }
        )
        if tr_id == "TTTC8708R":
            summary["loan_int"] = "0"


class _Client:
    def __init__(self, fixture: dict[str, list[dict[str, Any]]]) -> None:
        self.fixture = fixture
        self.calls: list[dict[str, Any]] = []
        self.post_calls = 0

    def get(self, **kwargs: Any) -> Mapping[str, Any]:
        self.calls.append(copy.deepcopy(kwargs))
        pages = self.fixture[kwargs["tr_id"]]
        query = kwargs["query"]
        fk = query.get("CTX_AREA_FK100", query.get("CTX_AREA_FK", ""))
        return copy.deepcopy(pages[1 if fk else 0])

    def token(self) -> str:
        return "offline-token"

    def send(self, request) -> Mapping[str, Any]:
        return self.get(
            origin=request.origin,
            endpoint=request.endpoint,
            tr_id=request.tr_id,
            query=request.query,
            continuation=request.continuation,
            public_headers={
                "custtype": "P",
                "tr_id": request.tr_id,
                "tr_cont": request.continuation,
            },
        )

    def post(self, **kwargs: Any) -> None:
        self.post_calls += 1
        raise AssertionError("POST must never be called")


class _SecondCaptureClient(_Client):
    def __init__(self, fixture, mutator) -> None:
        super().__init__(fixture)
        self.mutator = mutator

    def get(self, **kwargs: Any) -> Mapping[str, Any]:
        second_capture = any(call["tr_id"] == "CTCA0903R" for call in self.calls)
        result = copy.deepcopy(super().get(**kwargs))
        if second_capture:
            self.mutator(kwargs["tr_id"], result)
        return result


class KisDomesticFunctionalTruthTest(unittest.TestCase):
    def _reader(
        self,
        client: _Client,
        *,
        clock=None,
        max_pages: int = 20,
        max_stable_read_seconds: float = 120.0,
    ):
        return KisDomesticFunctionalTruthReader(
            client=KisDomesticFunctionalGetClient(
                app_key="offline-app-key",
                app_secret="offline-app-secret",
                cano="12345678",
                account_product_code="01",
                account_fingerprint=ACCOUNT_FINGERPRINT,
                server_authority_key=SERVER_AUTHORITY_KEY,
                token_reader=client.token,
                sender=client.send,
                allow_mock_transport=True,
                min_request_interval_seconds=0,
            ),
            cano="12345678",
            account_product_code="01",
            trading_date=date(2026, 8, 13),
            clock=clock or (lambda: datetime(2026, 8, 13, 14, 0, tzinfo=KST)),
            max_pages=max_pages,
            max_stable_read_seconds=max_stable_read_seconds,
        )

    def _read(
        self,
        client: _Client,
        *,
        owned=None,
        baseline_fixture=None,
        **reader_kwargs,
    ):
        owned_record = owned or _owned()
        baseline_body = owned_record["body"]
        baseline_reader = self._reader(
            _Client(baseline_fixture or _baseline_fixture())
        )
        baseline = baseline_reader.read_preactivation_baseline()
        return self._reader(client, **reader_kwargs).read(
            owned_action_record=owned_record,
            preactivation_baseline=baseline,
        )

    def test_six_get_trs_all_pages_are_stable_sanitized_hashed_and_reconciled(self) -> None:
        client = _Client(_fixture())
        truth = self._read(client)
        self.assertTrue(truth["stableRepeatedReads"])
        self.assertEqual(2, truth["readCount"])
        self.assertEqual(14, len(client.calls))
        self.assertEqual(0, client.post_calls)
        self.assertEqual(
            {"TTTC8434R", "TTTC0081R", "TTTC0084R", "TTTC8715R", "TTTC8708R", "CTCA0903R"},
            {call["tr_id"] for call in client.calls},
        )
        self.assertTrue(all(call["origin"] == LIVE_ORIGIN for call in client.calls))
        self.assertTrue(all(call["public_headers"]["custtype"] == "P" for call in client.calls))
        daily_call = next(call for call in client.calls if call["tr_id"] == "TTTC0081R")
        self.assertEqual("", daily_call["query"]["PDNO"])
        self.assertEqual("00", daily_call["query"]["INQR_DVSN"])
        self.assertEqual("ALL", daily_call["query"]["EXCG_ID_DVSN_CD"])
        working_call = next(call for call in client.calls if call["tr_id"] == "TTTC0084R")
        self.assertEqual("1", working_call["query"]["INQR_DVSN_1"])
        self.assertEqual("0", working_call["query"]["INQR_DVSN_2"])
        next_balance = client.calls[1]
        self.assertEqual("N", next_balance["continuation"])
        self.assertEqual("FK-2", next_balance["query"]["CTX_AREA_FK100"])
        raw_page = truth["rawCaptures"][0]["endpoints"]["balance"]["pages"][0]
        self.assertEqual("GET", raw_page["method"])
        self.assertEqual({"tr_cont": "M"}, raw_page["responseHeaders"])
        self.assertEqual(64, len(raw_page["redactedQueryHash"]))
        self.assertEqual(64, len(raw_page["queryAuthoritySignature"]))
        self.assertEqual(64, len(raw_page["bodyAuthoritySignature"]))
        self.assertEqual(64, len(raw_page["serverAuthoritySignature"]))
        serialized = json.dumps(truth, ensure_ascii=False)
        self.assertNotIn("12345678", serialized)
        self.assertNotIn('"01"', json.dumps(raw_page["query"].get("ACNT_PRDT_CD")))
        self.assertEqual([BUY_KEY, SELL_KEY], truth["normalized"]["officialOrderKeys"])
        self.assertEqual("20", truth["normalized"]["totalFeeKrw"])
        self.assertEqual("100", truth["normalized"]["totalTaxKrw"])
        self.assertEqual("0", truth["normalized"]["finalQuantity"])
        self.assertEqual("0", truth["normalized"]["ownerLossKrw"])
        self.assertEqual("26", truth["minimumGetPacingFloorSeconds"])
        self.assertEqual(14, truth["officialGetRequestCount"])
        endpoints = truth["rawCaptures"][0]["endpoints"]
        self.assertEqual(
            HOLIDAY_TARGET_DATE_FIRST_PAGE_SCOPE,
            endpoints["holiday"]["paginationScope"],
        )
        self.assertFalse(endpoints["holiday"]["allAvailablePagesClaimed"])
        self.assertFalse(endpoints["holiday"]["continuationFollowAllowed"])
        self.assertEqual(1, endpoints["holiday"]["maximumPhysicalPages"])

    def test_holiday_is_exact_target_date_first_page_proof_and_never_follows_continuation(self) -> None:
        fixture = _fixture()
        first = fixture["CTCA0903R"][0]
        first["trCont"] = "F"
        first["body"]["ctx_area_fk"] = "DO-NOT-FOLLOW"
        first["body"]["ctx_area_nk"] = "DO-NOT-FOLLOW"
        fixture["CTCA0903R"].append(
            _response(
                {
                    "rt_cd": "0",
                    "output": [{"bass_dt": "20260814", "opnd_yn": "Y"}],
                },
                tr_cont="D",
            )
        )
        client = _Client(fixture)

        truth = self._read(client)

        holiday_calls = [
            call for call in client.calls if call["tr_id"] == "CTCA0903R"
        ]
        self.assertEqual(2, len(holiday_calls))
        for call in holiday_calls:
            self.assertEqual("", call["continuation"])
            self.assertEqual(
                {"BASS_DT": YMD, "CTX_AREA_FK": "", "CTX_AREA_NK": ""},
                call["query"],
            )
        for capture in truth["rawCaptures"]:
            holiday = capture["endpoints"]["holiday"]
            self.assertEqual(1, len(holiday["pages"]))
            self.assertEqual("F", holiday["pages"][0]["continuationReceived"])
            self.assertEqual(
                HOLIDAY_TARGET_DATE_FIRST_PAGE_SCOPE,
                holiday["paginationScope"],
            )
            self.assertFalse(holiday["allAvailablePagesClaimed"])
            self.assertFalse(holiday["continuationFollowAllowed"])
            self.assertEqual(YMD, holiday["targetDate"])
        self.assertTrue(truth["normalized"]["holidayTargetDateFirstPageProofComplete"])
        self.assertFalse(truth["normalized"]["holidayAllAvailablePagesClaimed"])

    def test_holiday_first_page_missing_duplicate_or_closed_target_fails_without_follow(self) -> None:
        cases = (
            ("missing", [{"bass_dt": "20260814", "opnd_yn": "Y"}]),
            (
                "duplicate",
                [
                    {"bass_dt": YMD, "opnd_yn": "Y"},
                    {"bass_dt": YMD, "opnd_yn": "Y"},
                ],
            ),
            ("closed", [{"bass_dt": YMD, "opnd_yn": "N"}]),
        )
        for label, rows in cases:
            with self.subTest(label=label):
                fixture = _fixture()
                holiday = fixture["CTCA0903R"][0]
                holiday["trCont"] = "F"
                holiday["body"]["output"] = rows
                holiday["body"]["ctx_area_fk"] = "DO-NOT-FOLLOW"
                holiday["body"]["ctx_area_nk"] = "DO-NOT-FOLLOW"
                fixture["CTCA0903R"].append(
                    _response(
                        {
                            "rt_cd": "0",
                            "output": [{"bass_dt": YMD, "opnd_yn": "Y"}],
                        },
                        tr_cont="D",
                    )
                )
                client = _Client(fixture)
                with self.assertRaisesRegex(
                    KisDomesticFunctionalTruthBlocked,
                    "holiday first page",
                ):
                    self._read(client)
                holiday_calls = [
                    call for call in client.calls if call["tr_id"] == "CTCA0903R"
                ]
                self.assertEqual(1, len(holiday_calls))
                self.assertEqual("", holiday_calls[0]["continuation"])
                self.assertEqual("", holiday_calls[0]["query"]["CTX_AREA_FK"])
                self.assertEqual("", holiday_calls[0]["query"]["CTX_AREA_NK"])

    def test_account_wide_other_symbol_or_working_order_blocks(self) -> None:
        other = _fixture()
        other["TTTC0081R"][0]["body"]["output1"].append(
            _order("9999999", "02", "70000", pdno="005930")
        )
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "new nonowned PDNO"):
            self._read(_Client(other))

        working = _fixture()
        working["TTTC0084R"][0]["body"]["output"] = [
            {"ord_dt": YMD, "ord_gno_brno": "999", "odno": "9999998", "pdno": "005930", "psbl_qty": "1"}
        ]
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "working orders are not zero"):
            self._read(_Client(working))

    def test_owned_action_seal_binds_account_session_permit_qty_and_exact_ids(self) -> None:
        tampered = _owned()
        tampered["body"]["actions"][0]["quantity"] = "2"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "seal hash mismatch"):
            self._read(_Client(_fixture()), owned=tampered)

        resealed_body = copy.deepcopy(_owned()["body"])
        resealed_body["actions"][0]["quantity"] = "2"
        resealed = seal_owned_action_record(resealed_body)
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "side/quantity"):
            self._read(_Client(_fixture()), owned=resealed)

        missing_sell_body = copy.deepcopy(_owned()["body"])
        missing_sell_body["actions"] = missing_sell_body["actions"][:1]
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "nonowned order"):
            self._read(_Client(_fixture()), owned=seal_owned_action_record(missing_sell_body))

        over_cap_body = copy.deepcopy(_owned()["body"])
        over_cap_body["actions"][0]["submittedGrossKrw"] = "100001"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "gross cap"):
            self._read(_Client(_fixture()), owned=seal_owned_action_record(over_cap_body))

        invalid_ids = ("BUY-1", "1:2", "１２３", "", "1" * 17)
        for invalid in invalid_ids:
            with self.subTest(invalid=invalid):
                body = copy.deepcopy(_owned()["body"])
                body["actions"][0]["odno"] = invalid
                with self.assertRaisesRegex(
                    KisDomesticFunctionalTruthBlocked,
                    "exact ASCII digits 1..16",
                ):
                    self._read(
                        _Client(_fixture()),
                        owned=seal_owned_action_record(body),
                    )

        leading = copy.deepcopy(_owned()["body"])
        leading["actions"][0]["odno"] = "0000001"
        leading["actions"][1]["odno"] = "0000002"
        terminal = _fixture()
        terminal["TTTC0081R"][0]["body"]["output1"][0]["odno"] = "0000001"
        terminal["TTTC0081R"][0]["body"]["output1"][1]["odno"] = "0000002"
        truth = self._read(
            _Client(terminal), owned=seal_owned_action_record(leading)
        )
        self.assertEqual(
            [f"{YMD}:001:0000001", f"{YMD}:001:0000002"],
            truth["normalized"]["officialOrderKeys"],
        )

    def test_continuation_terminal_variants_and_cursor_failures(self) -> None:
        truth = self._read(_Client(_fixture()))
        balance_pages = truth["rawCaptures"][0]["endpoints"]["balance"]["pages"]
        self.assertEqual(["M", "D"], [page["continuationReceived"] for page in balance_pages])
        daily_pages = truth["rawCaptures"][0]["endpoints"]["dailyCcld"]["pages"]
        self.assertEqual("E", daily_pages[-1]["continuationReceived"])

        missing = _fixture()
        missing["TTTC8434R"][0]["body"]["ctx_area_fk100"] = ""
        missing["TTTC8434R"][0]["body"]["ctx_area_nk100"] = ""
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "cursor is missing"):
            self._read(_Client(missing))
        unknown = _fixture()
        unknown["TTTC8434R"][0]["trCont"] = "UNKNOWN"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "trusted GET client rejected"):
            self._read(_Client(unknown))
        repeated = _fixture()
        repeated["TTTC8434R"][1] = _response(
            {
                "rt_cd": "0",
                "output1": [],
                "output2": [],
                "ctx_area_fk100": "FK-2",
                "ctx_area_nk100": "NK-2",
            },
            tr_cont="F",
        )
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "cursor repeated"):
            self._read(_Client(repeated))
        limited = _Client(_fixture())
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "truncated"):
            self._read(limited, max_pages=1)
        self.assertFalse(
            any(call["tr_id"] == "CTCA0903R" for call in limited.calls)
        )

    def test_unstable_stale_closed_day_and_malformed_envelope_block(self) -> None:
        def close_day(tr_id, result):
            if tr_id == "CTCA0903R":
                result["body"]["output"][0]["opnd_yn"] = "N"

        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "not authoritatively open"):
            self._read(_SecondCaptureClient(_fixture(), close_day))
        times = iter(
            [
                datetime(2026, 8, 13, 14, 0, tzinfo=KST),
                datetime(2026, 8, 13, 14, 2, 1, tzinfo=KST),
            ]
        )
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "stale"):
            self._read(_Client(_fixture()), clock=lambda: next(times))
        closed = _fixture()
        closed["CTCA0903R"][0]["body"]["output"][0]["opnd_yn"] = "N"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "not authoritatively open"):
            self._read(_Client(closed))

    def test_realistic_pacing_and_dynamic_marks_do_not_fake_instability(self) -> None:
        paced = iter(
            [
                datetime(2026, 8, 13, 14, 0, tzinfo=KST),
                datetime(2026, 8, 13, 14, 0, 29, tzinfo=KST),
            ]
        )
        truth = self._read(_Client(_fixture()), clock=lambda: next(paced))
        self.assertEqual("29", truth["stableReadElapsedSeconds"])

        moving = _fixture()
        moving["TTTC8434R"][0]["body"]["output1"].append(
            {
                "pdno": "010140",
                "hldg_qty": "0",
                "ord_psbl_qty": "0",
                "pchs_avg_pric": "0",
                "pchs_amt": "0",
                "prpr": "80000",
                "evlu_amt": "0",
            }
        )

        def move_marks(tr_id, result):
            if tr_id != "TTTC8434R" or not result["body"].get("output1"):
                return
            for row in result["body"]["output1"]:
                row["prpr"] = str(int(row["prpr"]) + 100)
                row["evlu_amt"] = str(int(row["evlu_amt"]) + 200)
            result["body"]["output2"][0]["tot_evlu_amt"] = "501080"
            result["body"]["output2"][0]["nass_amt"] = "501080"

        dynamic = self._read(_SecondCaptureClient(moving, move_marks))
        self.assertFalse(dynamic["rawCapturesByteEqual"])
        self.assertEqual("PARSED_CAUSAL_PROJECTION", dynamic["stableComparison"])

    def test_second_capture_causal_changes_and_pagination_reorder_block(self) -> None:
        mutators = {
            "quantity": lambda tr, response: (
                response["body"]["output1"].append(
                    {
                        "pdno": "010140",
                        "hldg_qty": "1",
                        "ord_psbl_qty": "1",
                        "pchs_avg_pric": "80000",
                        "pchs_amt": "80000",
                        "prpr": "80000",
                        "evlu_amt": "80000",
                    }
                )
                if tr == "TTTC8434R" and response["body"].get("output1")
                else None
            ),
            "cash": lambda tr, response: (
                response["body"]["output2"][0].__setitem__("dnca_tot_amt", "500879")
                if tr == "TTTC8434R" and response["body"].get("output2")
                else None
            ),
            "working": lambda tr, response: (
                response["body"]["output"].append(
                    {
                        "ord_dt": YMD,
                        "ord_gno_brno": "001",
                        "odno": "9999997",
                        "pdno": "010140",
                        "psbl_qty": "1",
                    }
                )
                if tr == "TTTC0084R"
                else None
            ),
            "daily-order-reorder": lambda tr, response: (
                response["body"]["output1"].reverse() if tr == "TTTC0081R" else None
            ),
            "profit": lambda tr, response: (
                response["body"]["output1"][0].__setitem__("fee", "21")
                if tr == "TTTC8715R"
                else None
            ),
        }
        for label, mutator in mutators.items():
            with self.subTest(label=label):
                with self.assertRaises(KisDomesticFunctionalTruthBlocked):
                    self._read(_SecondCaptureClient(_fixture(), mutator))

    def test_cost_aggregates_order_amount_balance_and_conservative_loss_are_exact(self) -> None:
        cost = _fixture()
        cost["TTTC8708R"][0]["body"]["output2"]["tot_fee"] = "19"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "total fee mismatch"):
            self._read(_Client(cost))

        independent = _fixture()
        independent["TTTC8708R"][0]["body"]["output1"][0]["fee"] = "19"
        independent["TTTC8708R"][0]["body"]["output2"]["buy_fee_smtl"] = "9"
        independent["TTTC8708R"][0]["body"]["output2"]["buy_excc_amt_smtl"] = "80009"
        independent["TTTC8708R"][0]["body"]["output2"]["tot_fee"] = "19"
        independent["TTTC8708R"][0]["body"]["output2"]["tot_excc_amt"] = "160899"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "summaries disagree"):
            self._read(_Client(independent))

        loan = _fixture()
        loan["TTTC8708R"][0]["body"]["output2"]["loan_int"] = "1"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "loan_int aggregate"):
            self._read(_Client(loan))

        amount = _fixture()
        amount["TTTC0081R"][0]["body"]["output1"][0]["tot_ccld_amt"] = "79999"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "average"):
            self._read(_Client(amount))

        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "balance delta"):
            self._read(
                _Client(_fixture()),
                owned=_owned(baseline_quantity="1"),
                baseline_fixture=_baseline_fixture(quantity="1"),
            )

        adverse = _fixture()
        adverse["TTTC8434R"][0]["body"]["output2"][0]["dnca_tot_amt"] = "495880"
        truth = self._read(_Client(adverse))
        self.assertEqual("5000", truth["normalized"]["adverseCashDeltaKrw"])
        self.assertEqual("5000", truth["normalized"]["ownerLossKrw"])
        self.assertFalse(truth["normalized"]["ownerLossLimitSatisfied"])

        favorable = _fixture()
        favorable["TTTC8434R"][0]["body"]["output2"][0]["dnca_tot_amt"] = "500881"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "favorable account cash"):
            self._read(_Client(favorable))

    def test_official_padded_numeric_strings_are_accepted_and_normalized(self) -> None:
        padded = _fixture()
        for tr_id in ("TTTC8715R", "TTTC8708R"):
            padded[tr_id][0]["body"]["output1"][0]["buy_amt"] = " 00080000.00 "
            padded[tr_id][0]["body"]["output2"]["buy_tr_amt_smtl"] = "+00080000.00"
        truth = self._read(_Client(padded))
        self.assertEqual("80000", truth["normalized"]["buyFilledAmountKrw"])

    def test_signed_preactivation_baseline_subtracts_legitimate_same_day_history(self) -> None:
        baseline = _baseline_fixture()
        prior_buy = _order("9000001", "02", "100")
        prior_sell = _order("9000002", "01", "110")
        baseline["TTTC0081R"][0]["body"]["output1"] = [prior_buy, prior_sell]
        baseline_summary = baseline["TTTC8434R"][0]["body"]["output2"][0]
        baseline_summary.update(
            {"thdt_buy_amt": "100", "thdt_sll_amt": "110", "thdt_tlex_amt": "3"}
        )
        _set_cumulative_profit(
            baseline,
            buy_qty=1,
            buy_amt=100,
            sell_qty=1,
            sell_amt=110,
            buy_fee=1,
            sell_fee=1,
            sell_tax=1,
            realized=7,
        )

        terminal = _fixture()
        terminal["TTTC0081R"][0]["body"]["output1"] = [
            prior_buy,
            prior_sell,
            *terminal["TTTC0081R"][0]["body"]["output1"],
        ]
        terminal_summary = terminal["TTTC8434R"][0]["body"]["output2"][0]
        terminal_summary.update(
            {
                "thdt_buy_amt": "80100",
                "thdt_sll_amt": "81110",
                "thdt_tlex_amt": "123",
            }
        )
        _set_cumulative_profit(
            terminal,
            buy_qty=2,
            buy_amt=80100,
            sell_qty=2,
            sell_amt=81110,
            buy_fee=11,
            sell_fee=11,
            sell_tax=101,
            realized=887,
        )
        truth = self._read(_Client(terminal), baseline_fixture=baseline)
        self.assertEqual([BUY_KEY, SELL_KEY], truth["normalized"]["officialOrderKeys"])
        self.assertEqual("80000", truth["normalized"]["buyFilledAmountKrw"])
        self.assertEqual("81000", truth["normalized"]["sellFilledAmountKrw"])
        self.assertTrue(truth["normalized"]["preactivationCumulativeCostsSubtracted"])
        self.assertFalse(truth["preactivationBaselineDurableCasPersisted"])

        forged = copy.deepcopy(
            self._reader(_Client(_baseline_fixture())).read_preactivation_baseline()
        )
        forged["body"]["normalized"]["cashKrw"] = "999999"
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "baseline hash"):
            self._reader(_Client(terminal)).read(
                owned_action_record=_owned(),
                preactivation_baseline=forged,
            )

    def test_fresh_quote_is_signed_bounded_and_never_classified_durable(self) -> None:
        fixture = {
            "FHKST01010100": [
                _response({"rt_cd": "0", "output": {"stck_prpr": "80000"}})
            ]
        }
        times = iter(
            [
                datetime(2026, 8, 13, 13, 0, tzinfo=KST),
                datetime(2026, 8, 13, 13, 0, 4, tzinfo=KST),
            ]
        )
        reader = self._reader(_Client(fixture), clock=lambda: next(times))
        quote = reader.read_fresh_quote_preflight()
        self.assertEqual("FHKST01010100", quote["trId"])
        self.assertEqual("80000", quote["notionalKrw"])
        self.assertFalse(quote["durableCasPersisted"])
        signed_body = dict(quote)
        signature = signed_body.pop("serverAuthoritySignature")
        self.assertTrue(reader.client.verify_capture_envelope(signed_body, signature))

        too_large = {
            "FHKST01010100": [
                _response({"rt_cd": "0", "output": {"stck_prpr": "100001"}})
            ]
        }
        with self.assertRaisesRegex(KisDomesticFunctionalTruthBlocked, "max order"):
            self._reader(_Client(too_large)).read_fresh_quote_preflight()

    def test_release_gate_is_false_and_no_network_adapter_exists(self) -> None:
        status = production_entrypoint_status()
        self.assertFalse(status["available"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(status["accountWideWorkingZeroSemanticsObserved"])
        self.assertEqual("GET_ONLY", status["method"])


if __name__ == "__main__":
    unittest.main()
