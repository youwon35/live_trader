"""Truthful USD gate for START, renewal and final CLEAN; no transport or writes.

Current v2 PSAMOUNT proves *order capacity*, and the owned book represents an
allocated portfolio with estimated cost reserves. Neither is an account-wide
USD cash reconciliation. In particular a generic broker_id='kis' KRW PASS is
not evidence for USD. There is currently no reviewed account/settlement/cost
producer for the missing positive proof, so this gate deliberately cannot PASS.

Do not add a caller-controlled 'verified' flag to open this gate. A future
positive implementation needs a separately reviewed, exact account/currency
baseline plus official cash-role, positions, terminal and actual-cost evidence.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import re

from trading_runtime.artifact_governance import stable_sha256
from .kis_equity_execution import OBSERVATION, checked

SCHEMA = "kis-native-usd-reconciliation/v1"
PRODUCER_UNAVAILABLE = "KIS_USD_ACCOUNT_RECONCILIATION_PRODUCER_UNAVAILABLE"
ACTUAL_COST_UNPROVEN = "KIS_USD_ACTUAL_COST_UNPROVEN"
_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_DECIMAL = re.compile(r"[0-9]{1,24}(?:\.[0-9]{1,16})?\Z", re.ASCII)
_OPERATIONS = frozenset({"START", "RENEWAL", "CLEAN"})

REFERENCES = (
    "https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/overseas_stock/inquire_psamount/chk_inquire_psamount.py",
    "https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/overseas_stock/inquire_present_balance/chk_inquire_present_balance.py",
    "https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/overseas_stock/inquire_balance/chk_inquire_balance.py",
    "https://apiportal.koreainvestment.com/api/apis/public/detail?accessUrl=%2Fuapi%2Foverseas-stock%2Fv1%2Ftrading%2Finquire-psamount",
    "https://apiportal.koreainvestment.com/api/apis/public/detail?accessUrl=%2Fuapi%2Foverseas-stock%2Fv1%2Ftrading%2Finquire-present-balance",
)


def _decimal(value, *, whole=False):
    if type(value) is not str or not _DECIMAL.fullmatch(value):
        raise ValueError("DECIMAL_TEXT_REQUIRED")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("FINITE_DECIMAL_REQUIRED") from error
    if not result.is_finite() or result < 0 or (whole and result != result.to_integral_value()):
        raise ValueError("NONNEGATIVE_NATIVE_UNIT_REQUIRED")
    return result


def _time(value):
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("AWARE_CLOCK_REQUIRED")
    return value.astimezone(timezone.utc)


def _scope(scope):
    if (type(scope) is not dict or scope.get("version") != "kis-owned-capital-v2"
        or scope.get("quoteCurrency") != "USD"
        or scope.get("scopeHash") != stable_sha256({k:v for k,v in scope.items() if k != "scopeHash"})
        or any(type(scope.get(k)) is not str or not _HASH.fullmatch(scope[k])
               for k in ("scopeHash", "accountFingerprint", "credentialConfigurationHash", "reviewBindingHash"))
        or not isinstance(scope.get("deploymentId"),str) or not scope["deploymentId"]
        or not isinstance(scope.get("accountProfileId"),str) or not scope["accountProfileId"]):
        raise ValueError("EXACT_USD_NATIVE_SCOPE_REQUIRED")
    percent = _decimal(scope.get("capitalPercent"))
    if not 0 < percent <= 100:
        raise ValueError("REVIEWED_CAPITAL_PERCENT_REQUIRED")
    members = scope.get("members")
    if type(members) is not list or not 1 <= len(members) <= 32:
        raise ValueError("BOUNDED_CANONICAL_MEMBERS_REQUIRED")
    ids = []
    for member in members:
        if (type(member) is not dict or set(member) != {"strategyInstanceId","sourceArtifactHash","portfolioHash","symbol","quoteCurrency"}
            or member["quoteCurrency"] != "USD" or type(member["symbol"]) is not str
            or not re.fullmatch(r"[A-Z][A-Z.\-]{0,9}", member["symbol"], re.ASCII)
            or type(member["strategyInstanceId"]) is not str or not member["strategyInstanceId"]
            or any(type(member[k]) is not str or not _HASH.fullmatch(member[k]) for k in ("sourceArtifactHash","portfolioHash"))):
            raise ValueError("EXACT_USD_CANONICAL_MEMBER_REQUIRED")
        ids.append(member["strategyInstanceId"])
    if len(set(ids)) != len(ids):
        raise ValueError("DUPLICATE_NATIVE_MEMBER")
    return scope


def native_usd_reconciliation_status(*, operation, scope=None,
        native_observation=None, owned_projection=None, terminal_projection=None,
        generic_reconciliation=None, now=None):
    """Inspect existing proof roles, returning BLOCKED without account calls.

    ``native_observation`` is the existing hash-sealed operational v2 capacity
    receipt. ``owned_projection`` is KisOwnedCapitalStore.project's existing
    projection, not a manufactured account baseline. A terminal projection is
    optional provenance only and never substitutes for actual account cash.
    The generic reconciliation is intentionally ignored for USD authority.

    Positive capacity/book consistency indicators are diagnostic only. They
    cannot authorize START, renewal, or a final CLEAN declaration.
    """
    reasons = [PRODUCER_UNAVAILABLE]
    checks = {"exactNativeUsdScope":False,"currentUsdCapacityObserved":False,
        "ownedProjectionConsistent":False,"officialUsdAccountCashReconciled":False,
        "accountWideUsdPositionsReconciled":False,"actualCostsReconciled":False}
    current = None
    if type(operation) is not str or operation not in _OPERATIONS:
        reasons.append("KIS_USD_RECONCILIATION_OPERATION_INVALID")
    try:
        current = _time(now if now is not None else datetime.now(timezone.utc))
        validated_scope = _scope(scope)
        checks["exactNativeUsdScope"] = True
    except (ValueError,TypeError,KeyError,ArithmeticError):
        validated_scope = None
        reasons.append("KIS_USD_EXACT_NATIVE_SCOPE_UNVERIFIED")
    if native_observation is None:
        reasons.append("KIS_USD_CURRENT_ORDER_CAPACITY_UNAVAILABLE")
    else:
        try:
            receipt = checked(native_observation,OBSERVATION)
            if (validated_scope is None or current is None or receipt.get("brokerId") != "kis"
                or receipt.get("currency") != "USD" or receipt.get("exchange") not in {"NASD","NYSE","AMEX"}
                or receipt.get("symbol") not in {m["symbol"] for m in validated_scope["members"]}
                or receipt.get("profileId") != validated_scope["accountProfileId"]
                or any(receipt.get(k) != validated_scope[k] for k in ("accountFingerprint","credentialConfigurationHash"))
                or receipt.get("complete") is not True or receipt.get("orderAuthorityGranted") is not False
                or receipt.get("side") not in {"BUY","SELL"} or receipt.get("orderable") is not True
                or receipt.get("openOrders") != "NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE"
                or receipt.get("actualPersonalFeeObserved") is not False
                or not 0 <= (current-_time(receipt.get("asOf"))).total_seconds() <= 15
                or not 0 <= (_time(receipt.get("asOf"))-_time(receipt.get("startedAt"))).total_seconds() <= 15
                or not 0 <= (current-_time(receipt.get("startedAt"))).total_seconds() <= 15):
                raise ValueError("EXACT_FRESH_CAPACITY_REQUIRED")
            _decimal(receipt.get("availableCash"))
            _decimal(receipt.get("maximumBuyQuantity"),whole=True)
            checks["currentUsdCapacityObserved"] = True
            reasons.append("KIS_USD_ORDER_CAPACITY_IS_NOT_ACCOUNT_CASH_RECONCILIATION")
        except (ValueError,TypeError,KeyError,ArithmeticError):
            reasons.append("KIS_USD_CURRENT_ORDER_CAPACITY_UNVERIFIED")
    if owned_projection is None:
        reasons.append("KIS_USD_OWNED_PROJECTION_UNAVAILABLE")
    else:
        try:
            if (validated_scope is None or type(owned_projection) is not dict
                or owned_projection.get("capitalScopeHash") != validated_scope["scopeHash"]
                or owned_projection.get("equityScope") != "OWNED_DEPLOYMENT_ONLY"
                or owned_projection.get("capitalAccounting") != "OWNED_CONSERVATIVE_COST_RESERVE"
                or owned_projection.get("actualFeesReconciled") is not False
                or owned_projection.get("orderAuthorityGranted") is not False
                or owned_projection.get("netPerformanceStatus") != "ESTIMATED_UNTIL_ACTUAL_COST_RECONCILIATION"
                or type(owned_projection.get("fillCount")) is not int or owned_projection["fillCount"] < 0):
                raise ValueError("EXACT_ESTIMATED_OWNED_BOOK_REQUIRED")
            cash = _decimal(owned_projection.get("ownedCash"))
            gross = _decimal(owned_projection.get("grossCash"))
            reserve = _decimal(owned_projection.get("costReserveOutstanding"))
            with localcontext() as precision:
                precision.prec = 128
                if gross-cash != reserve:
                    raise ValueError("OWNED_RESERVE_IDENTITY_MISMATCH")
            positions = owned_projection.get("ownedPositions")
            if type(positions) is not dict or set(positions) != {m["strategyInstanceId"] for m in validated_scope["members"]}:
                raise ValueError("EXACT_OWNED_POSITION_SET_REQUIRED")
            for quantity in positions.values():
                _decimal(quantity,whole=True)
            if owned_projection["fillCount"] == 0 and (reserve != 0 or any(_decimal(q) != 0 for q in positions.values())):
                raise ValueError("EMPTY_OWNED_BOOK_INCONSISTENT")
            checks["ownedProjectionConsistent"] = True
            reasons.append("KIS_USD_ALLOCATED_OWNED_BOOK_IS_NOT_ACCOUNT_WIDE_BASELINE")
            if owned_projection["fillCount"] > 0:
                reasons.append(ACTUAL_COST_UNPROVEN)
        except (ValueError,TypeError,KeyError,ArithmeticError):
            reasons.append("KIS_USD_OWNED_PROJECTION_UNVERIFIED")
    if terminal_projection is not None:
        # A gross terminal notional projection cannot prove a fee, settlement
        # finality, or cash scope. No field in this unreviewed input is trusted.
        reasons.append("KIS_USD_TERMINAL_GROSS_PROJECTION_IS_NOT_ACCOUNT_CASH_PROOF")
    body = {"schemaVersion":SCHEMA,"operation":operation if type(operation) is str else "INVALID",
        "status":"BLOCKED","currency":"USD","brokerId":"kis",
        "reasons":list(dict.fromkeys(reasons)),"checks":checks,
        "threeWayVerified":False,"usdCashReconciled":False,
        "actualFeesReconciled":False,"actualPersonalFeeObserved":False,
        "genericBrokerReconciliationIgnored":generic_reconciliation is not None,
        "requiresSeparateUsdReconciliation":True,"positiveProofProducerAvailable":False,
        "cashRoles":{"psamount":"ORDER_CAPACITY_NOT_DEPOSIT_OR_SETTLED_CASH",
            "ownedGrossCash":"ALLOCATED_PRINCIPAL_PLUS_GROSS_TERMINAL_NOTIONAL",
            "ownedCash":"OWNED_GROSS_CASH_MINUS_ESTIMATED_COST_RESERVE",
            "costReserve":"ESTIMATED_HOLDBACK_NOT_ACTUAL_FEE",
            "presentBalanceDeposit":"CURRENCY_FOREIGN_FUNDS_AVAILABLE_WITH_DOCUMENTED_SETTLEMENT_AND_POSTING_DELAY",
            "presentBalanceWithdrawable":"WITHDRAWABLE_CASH_NOT_ORDER_CAPACITY_OR_OWNED_PRINCIPAL",
            "presentBalanceFinality":"FRESH_HTTP_RESPONSE_DOES_NOT_PROVE_REALTIME_OR_FINALIZED_CASH"},
        "missingPositiveProof":["REVIEWED_OFFICIAL_USD_CASH_ROLE_AND_SETTLEMENT_MAPPING",
            "EXACT_ACCOUNT_CURRENCY_BASELINE_AND_EXTERNAL_FLOW_RECONCILIATION",
            "COMPLETE_USD_POSITIONS_AND_WORKING_TERMINAL_ORDER_SCOPE",
            "OFFICIAL_ACTUAL_COST_SCOPE_AND_FINALITY_WHEN_FILLS_EXIST"],
        "references":list(REFERENCES),"orderAuthorityGranted":False}
    body["contentHash"] = stable_sha256(body)
    return body


def require_native_usd_reconciliation(**kwargs):
    """Mandatory USD gate. Current absence of a positive producer always blocks."""
    result = native_usd_reconciliation_status(**kwargs)
    raise ValueError("KIS_USD_NATIVE_RECONCILIATION_REQUIRED:" + ",".join(result["reasons"]))
