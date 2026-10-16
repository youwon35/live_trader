"""Bounded GET-only STOP evidence, never an order/cancel authority.

Official NCCS/PSBL and exact-day CCNL are separate observations. Absence from
NCCS/PSBL is never terminal evidence. Raw broker order time is not interpreted
as a fill timestamp or used to manufacture receipt freshness. Call outside all
authority locks; revalidate the state-owned dispatch set before any later wire.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import format_datetime, parsedate_to_datetime
import re
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from trading_runtime.artifact_governance import stable_sha256
from .kis_equity_execution import RESULT, VENUES, checked
from .kis_owned_capital import terminal_receipt
from .kis_scoped_cleanup_authority import dispatch_business_hash

SCHEMA = "kis-scoped-cleanup-truth/v1"
MAX_ORDERS = 64
MAX_ROWS = 128
MAX_PAGES = 20
MAX_SECONDS = 15
HTTP_DATE_ROLE = "HTTP_SERVER_CALENDAR_ONLY_NOT_BROKER_EVENT_OR_SETTLEMENT_CLOCK"
_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_NUM = re.compile(r"[0-9]{1,16}\Z", re.ASCII)
_QUANTITY = re.compile(r"[0-9]{1,24}(?:\.[0-9]{1,16})?\Z", re.ASCII)
_BASE = frozenset({"dispatchKey", "runtimeScopeId", "sessionId",
    "accountFingerprint", "credentialConfigurationHash", "deploymentId",
    "market", "currency", "symbol", "side", "orderDate", "organizationNo",
    "orderNo", "venue", "originalQuantity", "businessHash", "dispatchHash", "dispatch"})


def _fail(reason):
    raise ValueError("KIS_CLEANUP_" + reason)


def _hash(value):
    return stable_sha256(value)


def _now(value=None):
    result = value() if callable(value) else value
    if result is None:
        result = datetime.now(timezone.utc)
    if not isinstance(result, datetime) or result.tzinfo is None:
        _fail("AWARE_CLOCK_REQUIRED")
    return result.astimezone(timezone.utc)


def _timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError):
        _fail("INVALID_TIMESTAMP")
    return _now(result)


def _whole(value, *, positive=False):
    # Broker quantities stay decimal strings. Floats, bools and exponents must
    # not be rounded/truncated by a later int() cancel builder.
    if type(value) is not str or not _QUANTITY.fullmatch(value):
        _fail("DECIMAL_WHOLE_QUANTITY_REQUIRED")
    try:
        result = Decimal(value)
    except InvalidOperation:
        _fail("DECIMAL_WHOLE_QUANTITY_REQUIRED")
    if not result.is_finite() or result != result.to_integral_value() or (positive and result <= 0):
        _fail("DECIMAL_WHOLE_QUANTITY_REQUIRED")
    return str(int(result))


def _date(value):
    if type(value) is not str or not re.fullmatch(r"[0-9]{8}", value, re.ASCII):
        _fail("OFFICIAL_DATE_REQUIRED")
    try:
        if datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d") != value:
            _fail("OFFICIAL_DATE_REQUIRED")
    except ValueError:
        _fail("OFFICIAL_DATE_REQUIRED")
    return value


def _market_date(current, venue):
    return current.astimezone(ZoneInfo("Asia/Seoul" if venue == "KRX" else "America/New_York")).strftime("%Y%m%d")


def _spec(row):
    metadata = row["dispatch"]["metadata"]
    return SimpleNamespace(symbol=row["symbol"],
        strategy_instance_id=metadata["strategy_instance_id"],
        artifact_hash=metadata["source_artifact_hash"],
        portfolio_hash=metadata["portfolio_hash"])


def _owned(owned_orders, scope):
    if type(owned_orders) is not list or not 1 <= len(owned_orders) <= MAX_ORDERS:
        _fail("BOUNDED_NONEMPTY_OWNED_SET_REQUIRED")
    if (not isinstance(scope, dict) or scope.get("version") != "kis-owned-capital-v2"
        or scope.get("scopeHash") != _hash({k:v for k,v in scope.items() if k != "scopeHash"})):
        _fail("NATIVE_SCOPE_HASH_INVALID")
    result = deepcopy(owned_orders)
    seen = set()
    members = scope.get("members")
    if not isinstance(members, list):
        _fail("NATIVE_MEMBERS_REQUIRED")
    for row in result:
        if type(row) is not dict or set(row) != _BASE:
            _fail("EXACT_OWNERSHIP_FIELDS_REQUIRED")
        if any(type(row[key]) is not str or not row[key] or len(row[key]) > 512 for key in _BASE - {"dispatch"}):
            _fail("OWNERSHIP_VALUE_INVALID")
        if any(not _HASH.fullmatch(row[key]) for key in ("accountFingerprint", "credentialConfigurationHash", "businessHash", "dispatchHash")):
            _fail("OWNERSHIP_HASH_INVALID")
        if row["dispatchKey"] in seen:
            _fail("DUPLICATE_OWNED_DISPATCH")
        seen.add(row["dispatchKey"])
        if (row["venue"] not in VENUES or row["currency"] != VENUES[row["venue"]][0]
            or row["market"] != ("KR" if row["venue"] == "KRX" else "US")
            or row["side"] not in {"BUY", "SELL"}):
            _fail("EXACT_MARKET_CURRENCY_VENUE_REQUIRED")
        if not re.fullmatch(r"[0-9]{6}" if row["market"] == "KR" else r"[A-Z][A-Z.\-]{0,9}",row["symbol"],re.ASCII):
            _fail("EXACT_SYMBOL_REQUIRED")
        _date(row["orderDate"])
        if not all(_NUM.fullmatch(row[key]) for key in ("organizationNo", "orderNo")):
            _fail("EXACT_OFFICIAL_ORDER_KEY_REQUIRED")
        if row["originalQuantity"] != _whole(row["originalQuantity"], positive=True):
            _fail("CANONICAL_ORIGINAL_QUANTITY_REQUIRED")
        dispatch = row["dispatch"]
        if type(dispatch) is not dict or _hash(dispatch) != row["dispatchHash"]:
            _fail("IMMUTABLE_DISPATCH_HASH_CHANGED")
        if dispatch_business_hash(dispatch) != row["businessHash"]:
            _fail("IMMUTABLE_DISPATCH_BUSINESS_HASH_CHANGED")
        if (dispatch.get("broker_id") != "kis" or dispatch.get("idempotency_key") != row["dispatchKey"]
            or dispatch.get("symbol") != row["symbol"] or dispatch.get("side") != row["side"]
            or dispatch.get("broker_order_identity_verified") is not True
            or dispatch.get("order_date_source") != "OFFICIAL_ORDER_QUERY_EXACT_COMPOSITE_IDENTITY"
            or dispatch.get("order_date") != row["orderDate"]
            or dispatch.get("organization_no") != row["organizationNo"]
            or dispatch.get("broker_order_id") != row["orderNo"]
            or dispatch.get("broker_order_key") != ":".join((row["orderDate"], row["organizationNo"], row["orderNo"]))):
            _fail("DURABLE_VERIFIED_IDENTITY_REQUIRED")
        metadata = dispatch.get("metadata")
        if not isinstance(metadata, dict):
            _fail("DISPATCH_METADATA_REQUIRED")
        ticket = checked(metadata.get("native_quantity_model"), RESULT)
        if (metadata.get("deployment_id") != row["deploymentId"]
            or metadata.get("runtime_scope_id") != row["runtimeScopeId"]
            or metadata.get("native_account_fingerprint") != row["accountFingerprint"]
            or ticket.get("accountFingerprint") != row["accountFingerprint"]
            or ticket.get("credentialConfigurationHash") != row["credentialConfigurationHash"]
            or ticket.get("exchange") != row["venue"] or ticket.get("currency") != row["currency"]
            or ticket.get("deploymentId") != row["deploymentId"]
            or ticket.get("sourceArtifactHash") != metadata.get("source_artifact_hash")
            or ticket.get("symbol") != row["symbol"] or ticket.get("side") != row["side"]
            or ticket.get("capitalScopeHash") != scope["scopeHash"]
            or ticket.get("quantity") != row["originalQuantity"]
            or scope.get("deploymentId") != row["deploymentId"]
            or scope.get("accountFingerprint") != row["accountFingerprint"]
            or scope.get("credentialConfigurationHash") != row["credentialConfigurationHash"]
            or scope.get("quoteCurrency") != row["currency"]):
            _fail("DISPATCH_ACCOUNT_SOURCE_SCOPE_CHANGED")
        member = {"strategyInstanceId":metadata.get("strategy_instance_id"),
            "sourceArtifactHash":metadata.get("source_artifact_hash"),
            "portfolioHash":metadata.get("portfolio_hash"), "symbol":row["symbol"],
            "quoteCurrency":row["currency"]}
        if any(not member[key] for key in member) or sum(item == member for item in members) != 1:
            _fail("DISPATCH_CANONICAL_MEMBER_CHANGED")
    # Different local dispatches must never claim the same broker order.
    identities = {(r["orderDate"],r["organizationNo"],r["orderNo"],r["venue"],r["symbol"]) for r in result}
    if len(identities) != len(result):
        _fail("DUPLICATE_OFFICIAL_OWNERSHIP")
    if len({(r["runtimeScopeId"],r["sessionId"],r["deploymentId"]) for r in result}) != 1:
        _fail("MIXED_RUNTIME_SESSION")
    return result


def _official_identity(raw, owned, *, history):
    if not isinstance(raw, dict):
        _fail("INVALID_OFFICIAL_ROW")
    if any(str(raw.get(field) or "") != owned[key] for field,key in (
        ("ord_gno_brno", "organizationNo"), ("odno", "orderNo"), ("pdno", "symbol"))):
        _fail("OFFICIAL_COMPOSITE_IDENTITY_MISMATCH")
    if (history or owned["market"] == "US") and str(raw.get("ord_dt") or "") != owned["orderDate"]:
        _fail("OFFICIAL_DATE_MISMATCH")
    if raw.get("sll_buy_dvsn_cd") != ("02" if owned["side"] == "BUY" else "01"):
        _fail("OFFICIAL_SIDE_MISMATCH")
    if owned["market"] == "US":
        if raw.get("ovrs_excg_cd") != owned["venue"] or raw.get("tr_crcy_cd") != "USD":
            _fail("OFFICIAL_VENUE_CURRENCY_MISMATCH")
    else:
        # New KRX-only cleanup never accepts NXT/SOR rows or guesses exchange.
        if raw.get("excg_id_dvsn_cd") != "KRX":
            _fail("OFFICIAL_KRX_EXCHANGE_REQUIRED")
        if str(raw.get("loan_dt") or "") not in {"", "00000000"}:
            _fail("CASH_EQUITY_ONLY")


def _quantities(raw, owned, *, history):
    domestic = owned["market"] == "KR"
    ordered = _whole(raw.get("ord_qty" if domestic else "ft_ord_qty"), positive=True)
    filled = _whole(raw.get("tot_ccld_qty" if domestic else "ft_ccld_qty"))
    remaining = _whole(raw.get(("rmn_qty" if history else "psbl_qty") if domestic else "nccs_qty"))
    if ordered != owned["originalQuantity"] or int(filled) > int(ordered):
        _fail("OFFICIAL_ORDER_QUANTITY_MISMATCH")
    if int(remaining) > int(ordered):
        _fail("OFFICIAL_REMAINING_QUANTITY_INVALID")
    return ordered, filled, remaining


def _gross_notional(raw, owned, filled):
    value=raw.get("tot_ccld_amt" if owned["market"] == "KR" else "ft_ccld_amt3")
    if type(value) is not str or not _QUANTITY.fullmatch(value):
        _fail("OFFICIAL_GROSS_NOTIONAL_REQUIRED")
    try:
        amount=Decimal(value)
    except InvalidOperation:
        _fail("OFFICIAL_GROSS_NOTIONAL_REQUIRED")
    if not amount.is_finite() or amount < 0 or ((filled == "0") != (amount == 0)):
        _fail("OFFICIAL_FILL_NOTIONAL_CONTRADICTION")
    return amount


def _matching(rows, owned):
    # A same branch+ODNO row with a conflicting day/symbol/venue is a failure,
    # not a row we silently filter out before establishing exact ownership.
    selected = [r for r in rows if str(r.get("ord_gno_brno") or "") == owned["organizationNo"]
        and str(r.get("odno") or "") == owned["orderNo"]]
    if len(selected) > 1:
        _fail("AMBIGUOUS_OFFICIAL_COMPOSITE_IDENTITY")
    return selected[0] if selected else None


def _project(owned, scope, working_rows, history_rows, *, observed_at):
    history = _matching(history_rows, owned)
    if history is None:
        _fail("EXACT_HISTORY_ROW_REQUIRED")
    _official_identity(history, owned, history=True)
    ordered, filled, remaining = _quantities(history, owned, history=True)
    history_notional=_gross_notional(history,owned,filled)
    historical = owned["orderDate"] < _market_date(observed_at,owned["venue"])
    # KR PSBL has no ord_dt. An identical branch/ODNO in today's list may be a
    # newly reused identifier and must not be attached to an old terminal row.
    # Original-day official CCNL alone proves this old order's terminal state.
    if historical and remaining == "0" and owned["market"] == "KR":
        working = None
    elif historical and remaining == "0":
        # US NCCS does have ord_dt: retain every same-original-day row so an
        # actual old WORKING versus old terminal CCNL contradiction blocks.
        # Only a different, explicitly echoed day can disambiguate ODNO reuse.
        same_identity = [r for r in working_rows
            if str(r.get("ord_gno_brno") or "") == owned["organizationNo"]
            and str(r.get("odno") or "") == owned["orderNo"]]
        original_day = [r for r in same_identity if _date(r.get("ord_dt")) == owned["orderDate"]]
        working = _matching(original_day,owned)
    else:
        working = _matching(working_rows, owned)
    result = {k:v for k,v in owned.items() if k != "dispatch"}
    if working is not None:
        _official_identity(working, owned, history=False)
        quantities = _quantities(working, owned, history=False)
        if quantities != (ordered,filled,remaining) or int(remaining) <= 0 or int(filled) + int(remaining) != int(ordered):
            _fail("WORKING_HISTORY_QUANTITY_RACE_OR_MISMATCH")
        if _gross_notional(working,owned,filled) != history_notional:
            _fail("WORKING_HISTORY_NOTIONAL_RACE_OR_MISMATCH")
        if owned["market"] == "US":
            try:
                average = Decimal(working["ft_ccld_unpr3"])
            except (KeyError, TypeError, InvalidOperation):
                _fail("WORKING_FILL_PRICE_INVALID")
            if not average.is_finite() or average < 0 or (int(filled) > 0 and average <= 0) or (int(filled) == 0 and average != 0):
                _fail("WORKING_FILL_PRICE_INVALID")
        state, receipt = "WORKING", None
    else:
        if remaining != "0":
            _fail("MISSING_WORKING_ROW_IS_NOT_TERMINAL")
        if owned["market"] == "US":
            status = history.get("prcs_stat_name")
            allowed = {"체결", "체결완료"} if filled == ordered else {"취소", "취소완료", "거부", "주문거부"}
            if type(status) is not str or status.strip() not in allowed:
                _fail("US_TERMINAL_STATUS_UNPROVEN")
        else:
            canceled = _whole(history.get("cnc_cfrm_qty"))
            rejected = _whole(history.get("rjct_qty"))
            if int(filled) + int(canceled) + int(rejected) != int(ordered):
                _fail("KR_TERMINAL_ACCOUNTING_INCOMPLETE")
        # This validates exact persisted source/account/ticket and the official
        # terminal accounting, including explicit zero-fill rejection/cancel.
        receipt = terminal_receipt(scope, _spec(owned), owned["dispatch"], history)
        state = "TERMINAL"
    evidence = {"officialWorkingRow":working, "officialHistoryRow":history}
    return {**result, "verifiedDispatchHash":_hash(owned["dispatch"]), "historicalTerminalOnly":historical and state == "TERMINAL",
        "state":state, "filledQuantity":filled,
        "remainingQuantity":remaining, **evidence, "responseHash":_hash(evidence),
        "terminalVerified":state == "TERMINAL", "terminalReceipt":receipt}


def _http_date_evidence(headers, request_elapsed, response_elapsed):
    """Conservative calendar interval, never a broker trading/event timestamp.

    IMF-fixdate has one-second resolution (RFC 9110 6.6.1). Expand by measured
    whole-request RTT on both sides; keep the original header and monotonic
    offsets. Age>0 (RFC 9111) or an unknown/missing Date cannot support a cancel.
    """
    date = headers.get("date", ""); age = headers.get("age", "")
    body = {"schemaVersion":"kis-cleanup-http-date-page/v1","dateHeader":date,
        "ageHeader":age,"requestElapsedSeconds":request_elapsed,
        "responseElapsedSeconds":response_elapsed,"clockRole":HTTP_DATE_ROLE}
    try:
        if (type(date) is not str or len(date) > 80 or type(age) is not str or age not in {"", "0"}
            or type(request_elapsed) not in {int,float} or type(response_elapsed) not in {int,float}
            or not 0 <= request_elapsed <= response_elapsed <= MAX_SECONDS):
            _fail("HTTP_DATE_OR_AGE_UNVERIFIED")
        parsed = parsedate_to_datetime(date)
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or format_datetime(parsed,usegmt=True) != date:
            _fail("CANONICAL_HTTP_DATE_REQUIRED")
        rtt = response_elapsed-request_elapsed
        return {**body,"status":"VALID","roundTripSeconds":rtt,
            "minimumServerUtc":(parsed-timedelta(seconds=rtt)).isoformat(),
            "maximumServerUtc":(parsed+timedelta(seconds=1+rtt)).isoformat()}
    except (ValueError,TypeError,OverflowError,AttributeError):
        return {**body,"status":"UNVERIFIED","reason":"HTTP_DATE_OR_AGE_UNVERIFIED"}


def _check_order_days(orders, owned, queries, *, started, current):
    by_dispatch = {r["dispatchKey"]:r for r in owned}
    for row in orders:
        original = by_dispatch[row["dispatchKey"]]; day = original["orderDate"];venue=original["venue"]
        if day > _market_date(started,venue) or day > _market_date(current,venue):
            _fail("FUTURE_OFFICIAL_ORDER_DATE")
        if row["state"] == "WORKING" and (day != _market_date(started,venue) or day != _market_date(current,venue)):
            _fail("CURRENT_MARKET_DAY_REQUIRED")
        selected = [q for q in queries if q["market"] == original["market"]
            and (q["kind"] == "WORKING" or (q["venue"] == venue and q["orderDate"] == day))]
        for query in selected:
            for evidence in query["httpDateEvidence"]:
                if evidence["status"] == "VALID":
                    lower=_timestamp(evidence["minimumServerUtc"]);upper=_timestamp(evidence["maximumServerUtc"])
                    if day > _market_date(lower,venue):
                        _fail("FUTURE_OFFICIAL_ORDER_DATE_VS_HTTP_SERVER")
                if row["state"] != "WORKING":
                    continue
                if evidence["status"] != "VALID":
                    _fail("CURRENT_HTTP_SERVER_DAY_UNVERIFIED")
                # Offset is bounded from Date's quantization interval and the
                # complete local request/response interval, not receipt alone.
                date=parsedate_to_datetime(evidence["dateHeader"])
                local_request=started+timedelta(seconds=evidence["requestElapsedSeconds"])
                local_response=started+timedelta(seconds=evidence["responseElapsedSeconds"])
                if ((date-local_response).total_seconds() < -MAX_SECONDS
                    or (date+timedelta(seconds=1)-local_request).total_seconds() > MAX_SECONDS):
                    _fail("HTTP_SERVER_LOCAL_CLOCK_SKEW_UNVERIFIED")
                if day != _market_date(lower,venue) or day != _market_date(upper,venue):
                    _fail("CURRENT_MARKET_DAY_REQUIRED")


def _pages(observer, path, extra, *, context, output, budget):
    from .live_adapters import split_kis_account
    cano, product = split_kis_account(observer.reader.settings["KIS_ACCOUNT_NO"], observer.reader.settings["KIS_ACCOUNT_PRODUCT_CODE"])
    fk = nk = ""
    rows, hashes, date_evidence, seen = [], [], [], set()
    for index in range(MAX_PAGES):
        request_elapsed = budget()
        payload, headers = observer._get(path, {"CANO":cano, "ACNT_PRDT_CD":product,
            **extra, "CTX_AREA_FK"+context:fk, "CTX_AREA_NK"+context:nk}, continuation="N" if index else "")
        response_elapsed = budget()
        headers = {str(k).lower():str(v).strip() for k,v in headers.items()}
        if headers.get("age", "") not in {"", "0"}:
            # This is a new purportedly fresh GET, not reuse of a previously
            # verified immutable terminal cache. Explicit cache age is stale
            # evidence for both working and historical terminal observations.
            _fail("FRESH_GET_CACHE_AGE_UNVERIFIED")
        if type(payload) is not dict or str(payload.get("rt_cd")) != "0":
            _fail("OFFICIAL_GET_REJECTED")
        page = payload.get(output)
        if type(page) is not list or any(type(row) is not dict for row in page):
            _fail("INVALID_OFFICIAL_PAGE")
        rows.extend(deepcopy(page)); hashes.append(_hash({"payload":payload,"continuation":headers.get("tr_cont",""),
            "date":headers.get("date",""),"age":headers.get("age","")}))
        date_evidence.append(_http_date_evidence(headers,request_elapsed,response_elapsed))
        if len(rows) > MAX_ROWS:
            _fail("OFFICIAL_ROW_BOUND_EXCEEDED")
        continuation = headers.get("tr_cont", "").upper()
        if continuation not in {"", "D", "E", "F", "M"}:
            _fail("UNKNOWN_CONTINUATION")
        if continuation not in {"F", "M"}:
            return {"rows":rows,"rawPageHashes":hashes,"pageCount":len(hashes),
                "rowCount":len(rows),"paginationComplete":True,"httpDateEvidence":date_evidence}
        fk,nk = str(payload.get("ctx_area_fk"+context) or ""), str(payload.get("ctx_area_nk"+context) or "")
        if not (fk or nk) or (fk,nk) in seen:
            _fail("INCOMPLETE_OR_REPEATED_PAGINATION")
        seen.add((fk,nk))
    _fail("PAGE_BOUND_EXCEEDED")


def collect_cleanup_truth(native_runtime, owned_orders, *, now=None, monotonic_clock=time.monotonic):
    """GET only. No recovery/CAS writes: first recover and recapture provisionals.

    ``now`` may be an aware datetime or an injected clock callable. Production
    uses the real UTC clock before and after reads, plus elapsed monotonic time.
    Returned raw rows are in-memory proof material; callers must not log them.
    """
    started = _now(now); start_mono = monotonic_clock()
    def budget():
        elapsed = monotonic_clock() - start_mono
        if type(elapsed) not in {int,float} or not 0 <= elapsed <= MAX_SECONDS:
            _fail("READ_STALE_OR_CLOCK_CHANGED")
        return elapsed
    scope = deepcopy(native_runtime.scope())
    owned = _owned(owned_orders, scope)
    observer = native_runtime.observer
    account = observer.account()
    if any(account.get(k) != scope.get(k) for k in ("accountFingerprint", "credentialConfigurationHash")):
        _fail("CURRENT_ACCOUNT_SCOPE_CHANGED")
    for row in owned:
        if row["orderDate"] > _market_date(started,row["venue"]):
            _fail("FUTURE_OFFICIAL_ORDER_DATE")
    queries = []
    for market in sorted({r["market"] for r in owned}):
        domestic = market == "KR"
        prefix = "/uapi/" + ("domestic" if domestic else "overseas") + "-stock/v1/trading/"
        path = prefix + ("inquire-psbl-rvsecncl" if domestic else "inquire-nccs")
        query = _pages(observer,path,{"INQR_DVSN_1":"0","INQR_DVSN_2":"0"} if domestic else {"OVRS_EXCG_CD":"NASD","SORT_SQN":"DS"},
            context="100" if domestic else "200",output="output",budget=budget)
        queries.append({"kind":"WORKING","market":market,"venue":"KRX" if domestic else "ALL_US","orderDate":"", "path":path,**query})
    for venue,day in sorted({(r["venue"],r["orderDate"]) for r in owned}):
        domestic = venue == "KRX"
        path = "/uapi/" + ("domestic" if domestic else "overseas") + "-stock/v1/trading/" + ("inquire-daily-ccld" if domestic else "inquire-ccnl")
        params = ({"INQR_STRT_DT":day,"INQR_END_DT":day,"SLL_BUY_DVSN_CD":"00","INQR_DVSN":"00","PDNO":"",
            "CCLD_DVSN":"00","ORD_GNO_BRNO":"","ODNO":"","INQR_DVSN_3":"00","INQR_DVSN_1":"","INQR_DVSN_2":"","EXCG_ID_DVSN_CD":"KRX"} if domestic else
            {"PDNO":"%","ORD_STRT_DT":day,"ORD_END_DT":day,"SLL_BUY_DVSN":"00","CCLD_NCCS_DVSN":"00",
             "OVRS_EXCG_CD":venue,"SORT_SQN":"DS","ORD_DT":"","ORD_GNO_BRNO":"","ODNO":""})
        query = _pages(observer,path,params,context="100" if domestic else "200",output="output1" if domestic else "output",budget=budget)
        queries.append({"kind":"HISTORY","market":"KR" if domestic else "US","venue":venue,"orderDate":day,"path":path,**query})
    finished = _now(now); elapsed = monotonic_clock() - start_mono
    if native_runtime.scope() != scope or observer.account() != account:
        _fail("SCOPE_CHANGED_DURING_GET")
    body = {"schemaVersion":SCHEMA,"observedAt":started.isoformat(),"completedAt":finished.isoformat(),
        "durationSeconds":elapsed,"accountFingerprint":scope["accountFingerprint"],
        "credentialConfigurationHash":scope["credentialConfigurationHash"],"nativeScope":scope,
        "nativeScopeHash":scope["scopeHash"],"ownedOrdersHash":_hash(owned),"paginationComplete":True,
        "queries":queries,"rawResponseHash":_hash(queries),"orderAuthorityGranted":False}
    body["orders"] = _project_all(owned,scope,queries,observed_at=started)
    body["proofHash"] = _hash(body)
    validate_cleanup_truth(body,owned,now=finished)
    return body


def _project_all(owned, scope, queries, *, observed_at):
    def query_rows(kind, row):
        selected = [q for q in queries if q["kind"] == kind and q["market"] == row["market"]
            and (kind == "WORKING" or (q["venue"] == row["venue"] and q["orderDate"] == row["orderDate"]))]
        if len(selected) != 1:
            _fail("EXACT_COMPLETE_QUERY_SCOPE_REQUIRED")
        return selected[0]["rows"]
    return [_project(row,scope,query_rows("WORKING",row),query_rows("HISTORY",row),observed_at=observed_at) for row in owned]


def validate_cleanup_truth(truth, owned_orders, *, now=None, max_age_seconds=MAX_SECONDS):
    """Pure recheck against the current state-owned immutable dispatch set."""
    if type(max_age_seconds) not in {int,float} or not 0 < max_age_seconds <= MAX_SECONDS:
        _fail("FRESHNESS_BOUND_INVALID")
    required = {"schemaVersion","observedAt","completedAt","durationSeconds","accountFingerprint",
        "credentialConfigurationHash","nativeScope","nativeScopeHash","ownedOrdersHash",
        "paginationComplete","queries","rawResponseHash","orderAuthorityGranted","orders","proofHash"}
    if (type(truth) is not dict or set(truth) != required or truth.get("schemaVersion") != SCHEMA
        or truth.get("proofHash") != _hash({k:v for k,v in truth.items() if k != "proofHash"})
        or truth.get("orderAuthorityGranted") is not False or truth.get("paginationComplete") is not True):
        _fail("TRUTH_SCHEMA_OR_HASH_INVALID")
    current = _now(now); started = _timestamp(truth["observedAt"]); finished = _timestamp(truth["completedAt"])
    elapsed = truth["durationSeconds"]
    if (type(elapsed) not in {int,float} or not 0 <= elapsed <= max_age_seconds
        or not 0 <= (finished-started).total_seconds() <= max_age_seconds
        or not 0 <= (current-started).total_seconds() <= max_age_seconds
        or finished > current):
        _fail("READ_STALE_OR_CLOCK_CHANGED")
    scope = truth["nativeScope"]; owned = _owned(owned_orders,scope)
    if (truth["ownedOrdersHash"] != _hash(owned) or truth["nativeScopeHash"] != scope["scopeHash"]
        or any(truth[k] != scope.get(k) for k in ("accountFingerprint","credentialConfigurationHash"))):
        _fail("CURRENT_OWNED_SET_OR_SCOPE_CHANGED")
    queries = truth["queries"]
    if type(queries) is not list or not 2 <= len(queries) <= MAX_ORDERS+2 or truth["rawResponseHash"] != _hash(queries):
        _fail("RAW_RESPONSE_HASH_INVALID")
    expected_scopes = {("WORKING",r["market"],"KRX" if r["market"] == "KR" else "ALL_US","") for r in owned}
    expected_scopes |= {("HISTORY",r["market"],r["venue"],r["orderDate"]) for r in owned}
    actual_scopes = []
    last_response_elapsed = 0
    for query in queries:
        fields = {"kind","market","venue","orderDate","path","rows","rawPageHashes","pageCount","rowCount","paginationComplete","httpDateEvidence"}
        if type(query) is not dict or set(query) != fields:
            _fail("QUERY_PROOF_FIELDS_INVALID")
        identity = tuple(query[k] for k in ("kind","market","venue","orderDate"))
        actual_scopes.append(identity)
        domestic = query["market"] == "KR"
        path = "/uapi/" + ("domestic" if domestic else "overseas") + "-stock/v1/trading/" + (
            ("inquire-psbl-rvsecncl" if domestic else "inquire-nccs") if query["kind"] == "WORKING" else
            ("inquire-daily-ccld" if domestic else "inquire-ccnl"))
        if (query["path"] != path or query["paginationComplete"] is not True
            or type(query["rows"]) is not list or len(query["rows"]) > MAX_ROWS
            or any(type(row) is not dict for row in query["rows"])
            or type(query["rawPageHashes"]) is not list or not 1 <= len(query["rawPageHashes"]) <= MAX_PAGES
            or any(type(h) is not str or not _HASH.fullmatch(h) for h in query["rawPageHashes"])
            or type(query["pageCount"]) is not int or query["pageCount"] != len(query["rawPageHashes"])
            or type(query["httpDateEvidence"]) is not list or len(query["httpDateEvidence"]) != query["pageCount"]
            or type(query["rowCount"]) is not int or query["rowCount"] != len(query["rows"])):
            _fail("COMPLETE_BOUNDED_QUERY_REQUIRED")
        for evidence in query["httpDateEvidence"]:
            if type(evidence) is not dict:
                _fail("HTTP_DATE_PAGE_PROOF_INVALID")
            if type(evidence.get("ageHeader")) is not str or evidence["ageHeader"] not in {"", "0"}:
                _fail("FRESH_GET_CACHE_AGE_UNVERIFIED")
            request_elapsed=evidence.get("requestElapsedSeconds");response_elapsed=evidence.get("responseElapsedSeconds")
            if (type(request_elapsed) not in {int,float} or type(response_elapsed) not in {int,float}
                or not last_response_elapsed <= request_elapsed <= response_elapsed <= elapsed):
                _fail("HTTP_DATE_PAGE_MONOTONIC_ORDER_INVALID")
            last_response_elapsed=response_elapsed
            expected = _http_date_evidence({"date":evidence.get("dateHeader"),"age":evidence.get("ageHeader")},
                evidence.get("requestElapsedSeconds"),evidence.get("responseElapsedSeconds"))
            if evidence != expected:
                _fail("HTTP_DATE_PAGE_PROOF_INVALID")
    if len(set(actual_scopes)) != len(actual_scopes) or set(actual_scopes) != expected_scopes:
        _fail("EXACT_QUERY_SET_REQUIRED")
    if truth["orders"] != _project_all(owned,scope,queries,observed_at=started):
        _fail("OFFICIAL_ORDER_PROJECTION_CHANGED")
    _check_order_days(truth["orders"],owned,queries,started=started,current=current)
    return deepcopy(truth)
