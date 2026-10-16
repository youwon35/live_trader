"""Sanitized USD read preparation only; never a cash/cost/settlement producer."""
from copy import deepcopy
import re
from .kis_margin_observation import SCHEMA as MARGIN_SCHEMA, ROLE_FIELDS
from .kis_usd_observations import FALSE_CLAIMS

SCHEMA = "kis-usd-readonly-preparation-v1"
SUMMARY_KEYS = {"schemaVersion", "status", "trId", "pageCount", "paginationStatus", "rowCount", "usdRowCount", "usdPresence", "blankCurrencyRowCount", "recognizedFieldNames", "unreportedOtherFieldCount", "reportedRoleNames", "cashAndCostArithmeticPerformed", "hashMeaning", "contentHash"} | set(FALSE_CLAIMS)
FLAGS = {**FALSE_CLAIMS, "authorityGranted": False, "authorizationGranted": False, "executable": False}
MESSAGES = {
 "KIS_USD_OBSERVATION_UNAVAILABLE": "USD 통화별 조회를 확인하지 못했습니다. 금액을 추정하지 않습니다.",
 "KIS_USD_ROW_ABSENT_NOT_ZERO": "명시된 USD 행이 없습니다. USD 잔액 0으로 판단하지 않습니다.",
 "KIS_USD_CURRENCY_UNKNOWN": "통화가 표시되지 않은 행은 USD 금액으로 해석하지 않습니다.",
 "KIS_USD_MULTIPLE_ROWS_UNMERGED": "USD가 여러 행에 표시됩니다. 국가별 범위와 중복 여부를 확인하기 전에는 합산하지 않습니다.",
 "KIS_USD_PAGINATION_UNCONFIRMED": "조회 종료가 확인되지 않아 USD 전체 내역으로 판단하지 않습니다.",
 "KIS_USD_DEPOSIT_SCOPE_UNPROVEN": "표시된 예수금과 미결제·증거금·주문가능금액의 범위 확인이 필요합니다.",
 "KIS_USD_ACTUAL_COST_UNPROVEN": "계좌에 실제 부과된 수수료·세금은 아직 확인되지 않았습니다.",
 "KIS_USD_FINAL_POSTING_UNPROVEN": "USD 현금과 비용의 최종 정산 반영이 아직 확인되지 않았습니다.",
}
BASE_REASONS = ("KIS_USD_DEPOSIT_SCOPE_UNPROVEN", "KIS_USD_ACTUAL_COST_UNPROVEN", "KIS_USD_FINAL_POSTING_UNPROVEN")


def _invalid():
    raise ValueError("KIS_USD_PREPARATION_DIAGNOSTIC_INVALID")


def validate_sanitized_margin_summary(value):
    if type(value) is not dict or set(value) != SUMMARY_KEYS:
        _invalid()
    if (value["schemaVersion"] != MARGIN_SCHEMA or value["status"] != "OBSERVED_ONLY_NOT_FINANCIALLY_QUALIFIED"
        or value["trId"] != "TTTC2101R" or type(value["pageCount"]) is not int or value["pageCount"] != 1
        or value["paginationStatus"] not in {"COMPLETE", "PARTIAL", "UNKNOWN"}
        or value["hashMeaning"] != "CANONICAL_PARSED_JSON_NOT_HTTP_WIRE_BYTES"
        or value["cashAndCostArithmeticPerformed"] is not False
        or any(value[k] is not False for k in FALSE_CLAIMS)):
        _invalid()
    for key in ("rowCount", "usdRowCount", "blankCurrencyRowCount", "unreportedOtherFieldCount"):
        if type(value[key]) is not int or not 0 <= value[key] <= 256:
            _invalid()
    if value["usdRowCount"] + value["blankCurrencyRowCount"] > value["rowCount"]:
        _invalid()
    expected = ("EXPLICIT_USD_ROWS_PRESENT_UNMERGED" if value["usdRowCount"] else
                "UNKNOWN_CURRENCY_ROWS" if value["blankCurrencyRowCount"] else "ABSENT_NOT_ZERO")
    if value["usdPresence"] != expected or value["reportedRoleNames"] != ROLE_FIELDS:
        _invalid()
    names = value["recognizedFieldNames"]
    if type(names) is not list or any(type(x) is not str for x in names) or names != sorted(set(names)) or not set(names) <= {"natn_name", "crcy_cd", *ROLE_FIELDS}:
        _invalid()
    if type(value["contentHash"]) is not str or not re.fullmatch(r"[a-f0-9]{64}", value["contentHash"]):
        _invalid()
    return deepcopy(value)


def build_usd_preparation_diagnostic(summary=None):
    summary = validate_sanitized_margin_summary(summary) if summary is not None else None
    reasons = []
    if summary is None:
        reasons.append("KIS_USD_OBSERVATION_UNAVAILABLE")
    else:
        if summary["usdRowCount"] == 0:
            reasons.append("KIS_USD_ROW_ABSENT_NOT_ZERO")
        if summary["blankCurrencyRowCount"]:
            reasons.append("KIS_USD_CURRENCY_UNKNOWN")
        if summary["usdRowCount"] > 1:
            reasons.append("KIS_USD_MULTIPLE_ROWS_UNMERGED")
        if summary["paginationStatus"] != "COMPLETE":
            reasons.append("KIS_USD_PAGINATION_UNCONFIRMED")
    reasons.extend(BASE_REASONS)
    return {"schemaVersion": SCHEMA, "status": "OBSERVED_REQUIRES_OFFICIAL_CONTRACT" if summary is not None else "UNAVAILABLE", "observation": summary,
            "reasonCodes": reasons, "userMessages": [MESSAGES[x] for x in reasons], **FLAGS}


def validate_usd_preparation_diagnostic(value):
    if type(value) is not dict or set(value) != {"schemaVersion", "status", "observation", "reasonCodes", "userMessages", *FLAGS}:
        _invalid()
    if any(value[key] is not False for key in FLAGS):
        _invalid()
    expected = build_usd_preparation_diagnostic(value["observation"])
    if value != expected:
        _invalid()
    return expected


def usd_preparation_checks(value):
    diagnostic = validate_usd_preparation_diagnostic(value)
    return [{"code": code, "status": "BLOCKED", "detail": MESSAGES[code]} for code in diagnostic["reasonCodes"]]
