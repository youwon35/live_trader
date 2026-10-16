"""Pure exact US REST receipt planning; no broker calls or ledger writes.

The broker's terminal CCNL row proves cumulative gross fills. It does not
report actual commission/tax. Plans remain non-writable while costs are
unproven; a cost reserve is never converted into an actual paid fee. This
module deliberately does not create a synthetic private-WebSocket event.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import re

from trading_runtime.artifact_governance import stable_sha256
from .kis_equity_execution import RESULT, checked
from .kis_owned_capital import terminal_receipt
from .native_spot_quantity import decimal_value
from .verified_terminal_cache import terminal_identity, terminal_proof

SCHEMA = "kis-us-terminal-ledger-projection/v1"
CCNL_PATH = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
ACTUAL_COST_BLOCKER = "KIS_US_ACTUAL_COST_EXACT_ATTRIBUTION_UNPROVEN"


def _fail(code):
    raise ValueError("KIS_US_LEDGER_" + code)


def _seal(body):
    return {**deepcopy(body), "contentHash": stable_sha256(body)}


def _official_number(value, label, *, positive=False):
    if type(value) is not str or not re.fullmatch(r"[0-9]{1,24}(?:\.[0-9]{1,16})?", value):
        _fail("OFFICIAL_DECIMAL_STRING_REQUIRED")
    return decimal_value(value, label, positive=positive)


def _checked_plan(value):
    if (type(value) is not dict or value.get("schemaVersion") != SCHEMA
        or value.get("contentHash") != stable_sha256({k:v for k,v in value.items() if k != "contentHash"})
        or value.get("genericLedgerWriteAllowed") is not False
        or value.get("actualFeeObserved") is not False):
        _fail("PREVIOUS_PLAN_INVALID")
    return value


def terminal_gross_projection(scope, spec, dispatch, official, *, previous=None):
    """Validate exact terminal facts and return an append-only gross plan.

    Inputs are the original state-owned dispatch and official REST row. A
    caller must establish the authenticated complete query provenance before
    calling this pure function. A content hash protects local consistency; it
    is not authentication and never grants write/trading authority.
    """
    if (not isinstance(scope, dict) or scope.get("version") != "kis-owned-capital-v2"
        or scope.get("scopeHash") != stable_sha256({k:v for k,v in scope.items() if k != "scopeHash"})
        or scope.get("quoteCurrency") != "USD"):
        _fail("EXACT_NATIVE_USD_SCOPE_REQUIRED")
    if not isinstance(dispatch, dict) or not isinstance(official, dict):
        _fail("EXACT_DISPATCH_AND_OFFICIAL_ROW_REQUIRED")
    metadata = dispatch.get("metadata") or {}
    ticket = checked(metadata.get("native_quantity_model"), RESULT)
    member = {"strategyInstanceId":spec.strategy_instance_id, "sourceArtifactHash":spec.artifact_hash,
        "portfolioHash":spec.portfolio_hash, "symbol":spec.symbol, "quoteCurrency":"USD"}
    if (getattr(spec,"provider",None) != "kis" or getattr(spec,"broker_id",None) != "kis"
        or dispatch.get("symbol") != spec.symbol or type(scope.get("members")) is not list
        or scope["members"].count(member) != 1):
        _fail("EXACT_CANONICAL_US_MEMBER_REQUIRED")
    if (ticket.get("exchange") not in {"NASD", "NYSE", "AMEX"}
        or ticket.get("currency") != "USD" or ticket.get("capitalScopeHash") != scope["scopeHash"]
        or ticket.get("credentialConfigurationHash") != scope.get("credentialConfigurationHash")
        or ticket.get("deploymentId") != scope.get("deploymentId")
        or ticket.get("sourceArtifactHash") != getattr(spec, "artifact_hash", None)
        or ticket.get("symbol") != getattr(spec, "symbol", None)
        or ticket.get("side") != dispatch.get("side")
        or dispatch.get("broker_order_identity_verified") is not True
        or dispatch.get("order_date_source") != "OFFICIAL_ORDER_QUERY_EXACT_COMPOSITE_IDENTITY"):
        _fail("DURABLE_ACCOUNT_SOURCE_IDENTITY_REQUIRED")
    day = dispatch.get("order_date")
    branch, number = dispatch.get("organization_no"), dispatch.get("broker_order_id")
    if (not isinstance(day, str) or not re.fullmatch(r"[0-9]{8}", day)
        or any(not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,16}", value) for value in (branch, number))
        or dispatch.get("broker_order_key") != ":".join((day, branch, number))):
        _fail("OFFICIAL_COMPOSITE_ORDER_KEY_REQUIRED")
    try:
        datetime.strptime(day,"%Y%m%d")
    except ValueError:
        _fail("OFFICIAL_COMPOSITE_ORDER_KEY_REQUIRED")
    quantity = _official_number(official.get("ft_ccld_qty"), "official cumulative shares")
    ordered = _official_number(official.get("ft_ord_qty"), "official ordered shares", positive=True)
    _official_number(official.get("nccs_qty"), "official remaining shares")
    official_notional = _official_number(official.get("ft_ccld_amt3"), "official gross filled notional")
    if (quantity == 0) != (official_notional == 0):
        _fail("CUMULATIVE_QUANTITY_NOTIONAL_CONTRADICTION")
    if any(value != value.to_integral_value() for value in (quantity, ordered)):
        _fail("WHOLE_SHARES_REQUIRED")
    status = official.get("prcs_stat_name")
    allowed = {"체결", "체결완료"} if quantity == ordered else {"취소", "취소완료", "거부", "주문거부"}
    if type(status) is not str or status.strip() not in allowed:
        _fail("EXPLICIT_TERMINAL_STATUS_REQUIRED")
    receipt = terminal_receipt(scope, spec, dispatch, official)
    identity = terminal_identity(scope, spec, dispatch)
    proof = terminal_proof(identity, [receipt] if receipt is not None else [],
        broker_response=official, executed_quantity=str(quantity))
    # Account + exact official identity is global across deployments. Changing
    # a deployment/session must not make the same broker order bookable twice.
    order_key = {"accountFingerprint":scope["accountFingerprint"], "currency":"USD",
        "venue":ticket["exchange"], "orderDate":day, "organizationNo":branch,
        "orderNo":number, "symbol":spec.symbol}
    notional = Decimal(receipt["filledNotional"]) if receipt else Decimal(0)
    body = {"schemaVersion":SCHEMA, "projectionKey":stable_sha256(order_key),
        "officialOrderKey":order_key, "immutableIdentityHash":stable_sha256(identity),
        "sourceTransport":"KIS_AUTHENTICATED_REST_CCNL", "officialEndpoint":CCNL_PATH,
        "officialResponseHash":stable_sha256(official), "terminalProofHash":stable_sha256(proof),
        "terminalVerified":True, "side":dispatch["side"],
        "cumulativeFilledQuantity":str(quantity), "cumulativeGrossNotional":str(notional),
        "proposedPositionDelta":str(quantity if dispatch["side"] == "BUY" else quantity.copy_negate()),
        "proposedGrossCashDelta":str(notional.copy_negate() if dispatch["side"] == "BUY" and notional else notional),
        "actualFee":None, "actualTax":None, "actualSettlementCashDelta":None,
        "actualFeeObserved":False, "genericLedgerWriteAllowed":False,
        "accountingStatus":"ACTUAL_COST_UNPROVEN" if quantity else "NO_FILL_NO_FINANCIAL_PROJECTION",
        "blockers":[ACTUAL_COST_BLOCKER] if quantity else [], "orderAuthorityGranted":False}
    current = _seal(body)
    if previous is not None:
        old = _checked_plan(previous)
        if old != current:
            _fail("TERMINAL_PROOF_OR_IMMUTABLE_SCOPE_CHANGED")
        return {"action":"NOOP_EXACT_REPLAY", "plan":current}
    return {"action":"BLOCKED_COST_EVIDENCE" if quantity else "NOOP_ZERO_FILL", "plan":current}
