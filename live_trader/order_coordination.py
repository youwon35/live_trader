"""Live adapter for the shared coordinator. Does not start a runtime or order.

Normal dispatch reads funds only immediately before a real order. Short tests
may inject their already observed exact native-currency budget. Existing order
authority, protection, owner quantity and emergency checks remain authoritative.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from decimal import Decimal
import os
import time

from trading_runtime.order_coordination import (
    CoordinationError, OrderCoordinator, canonical_request_hash,
    credential_account_identity, currency_units,
)

_CONTEXT = ContextVar("live_order_coordination_context", default=None)
_UPBIT_ORDER_REJECTIONS = {"create_ask_error", "create_bid_error", "insufficient_funds_ask",
    "insufficient_funds_bid", "under_min_total_ask", "under_min_total_bid", "validation_error",
    "invalid_parameter", "invalid_post_only"}


def explicit_upbit_order_rejection(body):
    # Official create-order errors only. duplicated_identifier/nonce/timeouts
    # cannot prove the original client order did not already get accepted.
    # https://docs.upbit.com/kr/reference/rest-api-guide
    return isinstance(body, dict) and isinstance(body.get("error"), dict) and body["error"].get("name") in _UPBIT_ORDER_REJECTIONS


ROUTES = {
    "BINANCE_SPOT": ("binance", "SPOT"), "binance": ("binance", "SPOT"),
    "BINANCE_USDM_FUTURES": ("binance", "USDM_FUTURES"), "binance-futures": ("binance", "USDM_FUTURES"),
    "UPBIT_KRW_SPOT": ("upbit", "SPOT"), "upbit": ("upbit", "SPOT"),
    "KIS_KRX_CASH": ("kis", "CASH"), "KIS_KRX": ("kis", "CASH"), "KIS_KR_CASH": ("kis", "CASH"),
    "kis": ("kis", "CASH"),
}


@contextmanager
def live_order_coordination_context(owner_id, *, budget_evidence=None):
    token = _CONTEXT.set({"ownerId": owner_id, "budgetEvidence": budget_evidence})
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def current_coordination_context():
    return _CONTEXT.get() or {}


def _positive(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0:
            raise ValueError
        return number
    except (ValueError, ArithmeticError):
        raise CoordinationError("COORDINATION_ORDER_AMOUNT_INVALID") from None


class LiveOrderCoordination:
    def __init__(self, coordinator=None):
        self.coordinator = coordinator if coordinator is not None else OrderCoordinator()
        from .order_coordination_retirement import LiveRetirementJournal
        self.retirement_journal = LiveRetirementJournal(self.coordinator.path.with_name("live-retirement.sqlite3"))

    def reconcile_stopped(self, owner_id, *, worker_stopped=False, reader=None):
        from .order_coordination_retirement import reconcile_stopped_owner
        if not self.coordinator.path.exists():
            return self.retirement_status(owner_id)
        if worker_stopped:
            self.retirement_journal.mark_stopped(owner_id)
        return reconcile_stopped_owner(self, owner_id, reader)

    def reconcile_retained_stopped(self, *, reader=None):
        # Called only by an explicit account refresh. No timer or startup read.
        return {owner: self.reconcile_stopped(owner, reader=reader)
                for owner in self.retirement_journal.stopped_owners()}

    @staticmethod
    def _binding(route, settings=None):
        if route not in ROUTES:
            raise CoordinationError("COORDINATION_ROUTE_UNSUPPORTED")
        broker, market = ROUTES[route]
        settings = os.environ if settings is None else settings
        identity = credential_account_identity(broker=broker,
            api_key=settings.get("BINANCE_API_KEY" if broker == "binance" else "UPBIT_ACCESS_KEY", ""),
            account_number=settings.get("KIS_ACCOUNT_NO", ""),
            account_product=settings.get("KIS_ACCOUNT_PRODUCT_CODE", ""))
        return broker, market, identity

    def claim(self, owner_id, route, symbols, settings=None):
        broker, market, identity = self._binding(route, settings)
        symbols = list(dict.fromkeys(symbols))
        if not symbols:
            raise CoordinationError("COORDINATION_SYMBOLS_REQUIRED")
        prior = {row["scope_key"] for row in self.coordinator.list_owned(owner_id=owner_id)["scopes"] if row["status"] == "CLAIMED"}
        claimed = []
        try:
            for symbol in symbols:
                claimed.append(self.coordinator.claim_scope(owner_id=owner_id, broker=broker,
                    account_identity=identity, market=market, symbol=symbol))
        except BaseException:
            for scope in claimed:
                if scope["scopeKey"] not in prior:
                    try:
                        self.coordinator.release_unsubmitted_scope(scope_key=scope["scopeKey"], owner_id=owner_id)
                    except CoordinationError:
                        pass  # Never turn a concurrently reserved order into an empty claim.
            raise
        return claimed

    def rollback_unsubmitted(self, owner_id, claims):
        """Only empty pre-dispatch claims; any registered intent requires reconciliation."""
        for scope in claims:
            self.coordinator.release_unsubmitted_scope(scope_key=scope["scopeKey"], owner_id=owner_id)

    def retirement_status(self, owner_id):
        """Stopping a monitor is not closing its positions. Never release on stop alone."""
        if not self.coordinator.path.exists():
            return {"retained": False, "scopeCount": 0, "unreconciledIntentCount": 0,
                    "reconciliationRequired": False}
        owned = self.coordinator.list_owned(owner_id=owner_id)
        scopes = [s for s in owned["scopes"] if s["status"] == "CLAIMED"]
        unresolved = [i for i in owned["intents"] if not i["reconciled"]]
        return {"retained": bool(scopes), "scopeCount": len(scopes),
                "unreconciledIntentCount": len(unresolved),
                "reconciliationRequired": bool(scopes),
                "reason": "OWNED_POSITION_AND_ORDER_RECONCILIATION_REQUIRED" if scopes else ""}

    @contextmanager
    def dispatch_context(self, owner_id, *, budget_evidence=None):
        with live_order_coordination_context(owner_id, budget_evidence=budget_evidence):
            yield

    def _owned_scope(self, owner_id, route, symbol):
        broker, market, identity = self._binding(route)
        # Looking up existing scopes never claims a foreign/current position.
        matches = [s for s in self.coordinator.list_owned(owner_id=owner_id)["scopes"]
                   if s["broker"] == broker and s["market"] == market and s["symbol"] == symbol
                   and s["namespace"] == "LIVE" and s["status"] == "CLAIMED"]
        if len(matches) != 1:
            raise CoordinationError("COORDINATION_REDUCE_SCOPE_NOT_OWNED")
        # Recheck identity binding, e.g. KIS account changed after this run started.
        expected = self.coordinator.claim_scope(owner_id=owner_id, broker=broker,
            account_identity=identity, market=market, symbol=symbol)
        if expected["scopeKey"] != matches[0]["scope_key"]:
            self.coordinator.release_unsubmitted_scope(scope_key=expected["scopeKey"], owner_id=owner_id)
            raise CoordinationError("COORDINATION_ACCOUNT_CHANGED")
        return expected

    def dispatch(self, *, owner_id, route, intent, submit, budget_evidence=None,
                 funds_reader=None, reducing_only=False):
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise CoordinationError("COORDINATION_RUNTIME_OWNER_REQUIRED")
        if owner_id.startswith("live-runtime:") and owner_id in self.retirement_journal.stopped_owners():
            raise CoordinationError("COORDINATION_STOPPED_OWNER_CANNOT_SUBMIT")
        symbol = str(intent.get("symbol") or "").upper()
        prior_scopes = {s["scope_key"] for s in self.coordinator.list_owned(owner_id=owner_id)["scopes"]
                        if s["status"] == "CLAIMED"}
        if reducing_only:
            scope = self._owned_scope(owner_id, route, symbol)
        else:
            scope = self.claim(owner_id, route, [symbol])[0]
        try:
            client_id = str(intent.get("identifier") or intent.get("client_order_id") or intent.get("clientOrderId")
                            or intent.get("client_algo_id") or "").strip()
            if not client_id:
                raise CoordinationError("COORDINATION_CLIENT_ORDER_ID_REQUIRED")
            if reducing_only:
                funds = {"currency": "USDT" if route in {"binance", "binance-futures", "BINANCE_SPOT", "BINANCE_USDM_FUTURES"} else "KRW",
                         "required": "0", "available": "0", "observedAt": 0, "unitScale": 8}
            else:
                funds = budget_evidence if budget_evidence is not None else funds_reader(intent) if funds_reader is not None else None
                if not isinstance(funds, dict) or funds.get("unitScale", 8) != 8:
                    raise CoordinationError("COORDINATION_FRESH_FUNDS_REQUIRED")
            receipt = self.coordinator.reserve_intent(scope_key=scope["scopeKey"], owner_id=owner_id,
                intent_key=client_id, request_hash=canonical_request_hash(dict(intent)),
                currency=funds.get("currency", ""), amount_units=currency_units(funds.get("required"), unit_scale=8),
                available_units=currency_units(funds.get("available"), unit_scale=8, reserve=False), unit_scale=8,
                observed_at=funds.get("observedAt"), external_client_order_id=client_id, reducing_only=reducing_only)
        except BaseException:
            if scope["scopeKey"] not in prior_scopes:
                try:
                    self.coordinator.release_unsubmitted_scope(scope_key=scope["scopeKey"], owner_id=owner_id)
                except CoordinationError:
                    pass  # A durable or ambiguous registration must survive.
            raise
        self.retirement_journal.register(owner_id, receipt, intent, route, self.coordinator.clock())
        if not self.coordinator.begin_submission(intent_id=receipt["intentId"], owner_id=owner_id):
            raise CoordinationError("COORDINATION_ALREADY_SUBMITTED_RECONCILE_ONLY")
        try:
            response = submit()
        except BaseException:
            self.coordinator.record_result(intent_id=receipt["intentId"], owner_id=owner_id, status="UNKNOWN")
            raise
        status, broker_order_id = self._result(response)
        self.coordinator.record_result(intent_id=receipt["intentId"], owner_id=owner_id,
                                       status=status, broker_order_id=broker_order_id)
        self.retirement_journal.response(owner_id, receipt, response, status, broker_order_id)
        return response

    @staticmethod
    def _result(response):
        if not isinstance(response, dict):
            return "UNKNOWN", ""
        if response.get("outcomeAmbiguous") is True or response.get("redirectBlocked") is True:
            return "UNKNOWN", ""
        if response.get("statusCode") is not None:
            try:
                code = int(response["statusCode"])
            except (TypeError, ValueError):
                return "UNKNOWN", ""
            if code == 0 or code >= 500:
                return "UNKNOWN", ""
        if "ok" in response:
            payload = response.get("json")
            if not isinstance(payload, dict):
                return "UNKNOWN", ""
            response = payload
        output = response.get("output") if isinstance(response.get("output"), dict) else {}
        order_id = str(response.get("orderId") or response.get("uuid") or response.get("algoId") or output.get("ODNO") or "")
        state = str(response.get("status") or response.get("state") or "").upper()
        mapped = {"NEW": "ACKNOWLEDGED", "WAIT": "ACKNOWLEDGED", "WATCH": "ACKNOWLEDGED",
                  "PARTIALLY_FILLED": "PARTIALLY_FILLED", "FILLED": "FILLED", "DONE": "FILLED",
                  "CANCELED": "CANCELED", "CANCEL": "CANCELED", "REJECTED": "REJECTED", "EXPIRED": "EXPIRED"}
        if order_id:
            return mapped.get(state, "ACKNOWLEDGED"), order_id
        explicit_rejection = (response.get("code") in {-1013, -1021, -1100, -1101, -1102, -1111, -2010, -2015}
                              or explicit_upbit_order_rejection(response)
                              or "rt_cd" in response and str(response["rt_cd"]) != "0")
        return ("REJECTED" if explicit_rejection else "UNKNOWN"), ""

    def reconcile_release(self, owner_id, route, symbols, actual_truth, *, release_scope=True):
        """Caller supplies actual owned-flat + regular/conditional-pending-zero truth.

        actual_truth has observedAt/evidenceHash/ownedPositionFlat/pendingOrderCount
        and terminalOrders {clientOrderId: terminalStatus}. Missing facts retain
        ownership and funds; do not infer flat from balances or elapsed time.
        """
        evidence = dict(owned_position_flat=actual_truth.get("ownedPositionFlat"),
            pending_order_count=actual_truth.get("pendingOrderCount"),
            evidence_hash=actual_truth.get("evidenceHash"), observed_at=actual_truth.get("observedAt"))
        terminal = actual_truth.get("terminalOrders") or {}
        for symbol in symbols:
            scope = self._owned_scope(owner_id, route, symbol)
            for item in self.coordinator.list_owned(owner_id=owner_id)["intents"]:
                if item["scopeKey"] != scope["scopeKey"] or item["reconciled"]:
                    continue
                status = terminal.get(item["clientOrderId"])
                if not isinstance(status, str):
                    raise CoordinationError("COORDINATION_TERMINAL_ORDER_EVIDENCE_MISSING")
                self.coordinator.reconcile_terminal(intent_id=item["intentId"], owner_id=owner_id,
                                                     terminal_status=status, **evidence)
            # Reconciliation only changes local state. Keep the actual observation
            # time; never make broker evidence fresh by replacing it with now.
            if release_scope:
                self.coordinator.release_scope(scope_key=scope["scopeKey"], owner_id=owner_id, **evidence)
        return True


def read_dispatch_funds(intent):
    """Explicit per-order balance read; never used by page rendering or startup."""
    from . import brokers as b
    broker = str(intent.get("broker_id") or intent.get("broker") or "")
    symbol, side = str(intent.get("symbol") or "").upper(), str(intent.get("side") or "").upper()
    quantity = _positive(intent.get("quantity") or intent.get("qty"))
    notional = _positive(intent.get("notional") or quantity * _positive(intent.get("price")))
    currency = "KRW" if broker in {"upbit", "kis"} else "USDT"
    required = notional * Decimal("1.01")
    if broker == "binance":
        if not symbol.endswith("USDT"):
            raise CoordinationError("COORDINATION_QUOTE_SCOPE_UNSUPPORTED")
        currency = "USDT" if side == "BUY" else symbol[:-4]
        raw = b.ensure_response_ok(broker, b.send_binance_signed_request(b.build_binance_account_request))
        rows = [r for r in raw.get("balances", []) if r.get("asset") == currency]
        if len(rows) != 1 or "free" not in rows[0]:
            raise CoordinationError("COORDINATION_AVAILABLE_BALANCE_UNKNOWN")
        available = rows[0]["free"]
        required = required if side == "BUY" else quantity
    elif broker == "binance-futures":
        if not symbol.endswith("USDT"):
            raise CoordinationError("COORDINATION_QUOTE_SCOPE_UNSUPPORTED")
        raw = b.ensure_response_ok(broker, b.send_binance_signed_request(b.build_binance_futures_account_request, futures=True))
        rows = [r for r in raw.get("assets", []) if r.get("asset") == "USDT"]
        if len(rows) != 1 or "availableBalance" not in rows[0]:
            raise CoordinationError("COORDINATION_AVAILABLE_BALANCE_UNKNOWN")
        available = rows[0]["availableBalance"]
        policy = intent.get("futures_execution_policy") or {}
        leverage = _positive(policy.get("leverage") or intent.get("max_leverage") or 1)
        required = notional / leverage + notional * Decimal("0.01")
    elif broker == "upbit":
        if not symbol.startswith("KRW-"):
            raise CoordinationError("COORDINATION_QUOTE_SCOPE_UNSUPPORTED")
        currency = "KRW" if side == "BUY" else symbol.split("-", 1)[1]
        raw = b.ensure_response_ok(broker, b.send_prepared_request(b.build_upbit_accounts_request()))
        rows = [r for r in raw if r.get("currency") == currency]
        if len(rows) != 1 or "balance" not in rows[0]:
            raise CoordinationError("COORDINATION_AVAILABLE_BALANCE_UNKNOWN")
        available = rows[0]["balance"]
        required = required if side == "BUY" else quantity
    elif broker == "kis":
        from .read_only_account import ReadOnlyBrokerReader, runtime_settings
        if not symbol.isdigit() or len(symbol) != 6:
            raise CoordinationError("COORDINATION_KIS_DOMESTIC_SCOPE_REQUIRED")
        reader = ReadOnlyBrokerReader(runtime_settings())
        if side == "BUY":
            facts = reader._kis_cash("kis-kr", symbol, notional / quantity)
            available = facts.get("_quote")
        else:
            facts = reader._kis("kis-kr", symbol)
            available = facts.get("_base")
            required, currency = quantity, "KIS" + symbol
        if available is None:
            raise CoordinationError("COORDINATION_AVAILABLE_BALANCE_UNKNOWN")
    else:
        raise CoordinationError("COORDINATION_ROUTE_UNSUPPORTED")
    return {"currency": currency, "available": str(available), "required": str(required),
            "observedAt": time.time(), "unitScale": 8}
