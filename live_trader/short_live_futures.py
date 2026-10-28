"""Exchange-confirmed close-all stops for an exclusively owned futures leg.

No network or mutation runs at import. A service-owned durable reservation is
required before each write. A lost reply is queried by its original client ID;
it never creates a second POST. Entry and stop are not an atomic transaction.
"""
from .futures_leverage_limits import MAX_SAFE_LEVERAGE_INTEGER
from copy import deepcopy
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
import hashlib
import json


class FuturesProtectionError(ValueError):
    pass


def numeric(value, *, positive=False):
    try:
        if value is None or isinstance(value, bool):
            raise ValueError
        result = Decimal(str(value))
        if not result.is_finite() or (positive and result <= 0):
            raise ValueError
        return result
    except (ValueError, InvalidOperation):
        raise FuturesProtectionError("FUTURES_PROTECTION_NUMBER_UNKNOWN") from None


def policy(value):
    if not isinstance(value, dict):
        raise FuturesProtectionError("FUTURES_PROTECTION_POLICY_REQUIRED")
    leverage = numeric(value.get("leverage"), positive=True)
    stop = numeric(value.get("stopLossPercent"), positive=True)
    if leverage != leverage.to_integral_value() or not 1 <= leverage <= MAX_SAFE_LEVERAGE_INTEGER:
        raise FuturesProtectionError("FUTURES_LEVERAGE_INVALID")
    if value.get("marginMode") != "ISOLATED" or stop >= 100 / leverage:
        raise FuturesProtectionError("FUTURES_PROTECTION_POLICY_INVALID")
    return leverage, stop


def stop_price(order, market, frozen_policy):
    """Round toward the entry; reject stops at/beyond maintenance liquidation.

    This pre-entry calculation is conservative, not an authoritative liquidation
    price. Once a position exists its actual exchange liquidationPrice is used.
    """
    leverage, percent = policy(frozen_policy)
    price = numeric(order.get("referencePrice"), positive=True)
    mark = numeric(market.get("markPrice"), positive=True)
    tick = numeric(market.get("priceTick"), positive=True)
    long = order.get("positionDirection") == "long"
    raw = price * (1 - percent / 100 if long else 1 + percent / 100)
    trigger = (raw / tick).to_integral_value(rounding=ROUND_UP if long else ROUND_DOWN) * tick
    if trigger <= 0 or (long and not trigger < min(price, mark)) or (not long and not trigger > max(price, mark)):
        raise FuturesProtectionError("FUTURES_STOP_ALREADY_TRIGGERED_OR_INVALID")
    notional = max(numeric(order.get("quantity"), positive=True) * price,
                   numeric(order.get("projectedGrossExposureKrw", order.get("estimatedNotionalKrw", 0))) /
                   numeric(market.get("krwPerQuote"), positive=True))
    if market.get("maxNotionalValue") is not None and notional > numeric(market["maxNotionalValue"], positive=True):
        raise FuturesProtectionError("FUTURES_SYMBOL_NOTIONAL_LIMIT_EXCEEDED")
    brackets = market.get("leverageBrackets")
    if not isinstance(brackets, list):
        raise FuturesProtectionError("FUTURES_MAINTENANCE_BRACKETS_UNKNOWN")
    candidates = [row for row in brackets if numeric(row.get("notionalFloor")) <= notional < numeric(row.get("notionalCap"), positive=True)]
    if len(candidates) != 1:
        raise FuturesProtectionError("FUTURES_NOTIONAL_BRACKET_UNKNOWN")
    bracket = candidates[0]
    if leverage > numeric(bracket.get("initialLeverage"), positive=True):
        raise FuturesProtectionError("FUTURES_LEVERAGE_BRACKET_EXCEEDED")
    # Price changes the position's notional before the stop can trigger.
    # Cover every tier on that adverse path; a short may enter a higher MMR
    # tier even though its entry still fits the lower one. Ignore deductions.
    low, high = sorted((notional, notional * trigger / price))
    path_tiers = []
    for row in brackets:
        floor = numeric(row.get("notionalFloor"))
        cap = numeric(row.get("notionalCap"), positive=True)
        rate = numeric(row.get("maintMarginRatio"))
        if floor < 0 or cap <= floor or not 0 <= rate < 1:
            raise FuturesProtectionError("FUTURES_MAINTENANCE_BRACKETS_UNKNOWN")
        if cap > low and floor <= high:
            path_tiers.append((floor, cap, rate))
    path_tiers.sort(key=lambda row: row[0])
    if not path_tiers or not path_tiers[0][0] <= low < path_tiers[0][1]:
        raise FuturesProtectionError("FUTURES_STOP_NOTIONAL_PATH_UNKNOWN")
    covered_until = path_tiers[0][1]
    for floor, cap, _ in path_tiers[1:]:
        if floor != covered_until:
            raise FuturesProtectionError("FUTURES_STOP_NOTIONAL_PATH_UNKNOWN")
        covered_until = cap
    if high >= covered_until:
        raise FuturesProtectionError("FUTURES_STOP_NOTIONAL_PATH_UNKNOWN")
    maintenance = max(row[2] for row in path_tiers)
    fee = numeric(market.get("feeRate"))
    funding = abs(numeric(market.get("fundingRate")))
    # Do not spend maintenance, two commissions and one funding allowance on
    # the stop distance. Ignore maintenance deductions rather than over-credit.
    cost = 2 * fee + funding
    # A short's maintenance requirement rises with the adverse mark price.
    # Use that stricter denominator for both sides, ignoring any tier deduction.
    room = (1 / leverage - maintenance - cost) / (1 + maintenance + cost)
    distance = abs(trigger - price) / price
    if maintenance < 0 or fee < 0 or room <= 0 or distance >= room:
        raise FuturesProtectionError("FUTURES_STOP_AT_OR_BEYOND_MAINTENANCE_BUFFER")
    return format(trigger, "f")


class FuturesProtectionManager:
    def __init__(self, *, clock, reader, transport, guard, reserve, record):
        self.clock, self.reader, self.transport = clock, reader, transport
        self.guard, self.reserve, self.record = guard, reserve, record
        self.claims = {}
        self.active_by_symbol = {}
        self.incidents = []

    def _incident(self, code, **detail):
        item = {"code": code, **detail}
        if item not in self.incidents:
            self.incidents.append(item)
            self.record("FUTURES_PROTECTION_INCIDENT", deepcopy(item))

    def observe(self, claim):
        claim["lastReadVerified"] = False
        observed = dict(self.reader("BINANCE_USDM_FUTURES", deepcopy(claim)))
        expected_side = claim["positionDirection"].upper() if claim["positionMode"] == "HEDGE" else "BOTH"
        if (observed.get("known") is not True or observed.get("clientAlgoId") != claim["clientAlgoId"]
                or observed.get("symbol") != claim["symbol"] or observed.get("side") != claim["side"]
                or observed.get("positionSide") != expected_side or observed.get("closePosition") is not True
                or observed.get("workingType") != "MARK_PRICE"
                or observed.get("type", observed.get("orderType")) != "STOP_MARKET"
                or observed.get("priceProtect") is not False
                or numeric(observed.get("triggerPrice"), positive=True) != numeric(claim["triggerPrice"], positive=True)
                or (claim.get("algoId") and str(observed.get("algoId")) != str(claim["algoId"]))):
            raise FuturesProtectionError("FUTURES_PROTECTION_ACK_UNKNOWN")
        algo_id = str(observed.get("algoId", ""))
        if not algo_id.isdigit() or int(algo_id) <= 0:
            raise FuturesProtectionError("FUTURES_PROTECTION_ID_UNKNOWN")
        from .short_live_brokers import epoch
        if not -2 <= self.clock() - epoch(observed.get("observedAt")) <= 30:
            raise FuturesProtectionError("FUTURES_PROTECTION_OBSERVATION_STALE")
        claim.update(algoId=algo_id, status=observed.get("status"), observation=deepcopy(observed), lastReadVerified=True)
        self.record("FUTURES_PROTECTION_OBSERVED", {"clientAlgoId": claim["clientAlgoId"], "observation": observed})
        return observed

    def ensure(self, order, market, frozen_policy):
        trigger = stop_price(order, market, frozen_policy)
        symbol = order["symbol"]
        previous = self.active_by_symbol.get(symbol)
        if previous:
            current = self.observe(self.claims[previous])
            if current.get("status") != "NEW":
                raise FuturesProtectionError("FUTURES_PROTECTION_NOT_ACTIVE")
            old = numeric(current["triggerPrice"])
            new = numeric(trigger)
            mark = numeric(market.get("markPrice"), positive=True)
            if (order["positionDirection"] == "long" and old >= mark) or (order["positionDirection"] == "short" and old <= mark):
                raise FuturesProtectionError("FUTURES_EXISTING_STOP_ALREADY_TRIGGERED")
            # Never loosen a confirmed stop when another sleeve enters.
            if (order["positionDirection"] == "long" and old >= new) or (order["positionDirection"] == "short" and old <= new):
                return current
        material = {"session": order["sessionId"], "manifest": order["manifestHash"],
                    "symbol": symbol, "trigger": trigger, "entry": order["clientOrderId"]}
        client = "slp-" + hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()[:28]
        claim = self.claims.get(client)
        if claim is None:
            claim = {key: order[key] for key in ("route", "sessionId", "manifestHash", "accountId", "symbol", "positionDirection", "positionMode")}
            claim.update(operation="PROTECT", riskReducing=True, clientAlgoId=client,
                         parentClientOrderId=order["clientOrderId"], triggerPrice=trigger,
                         side="SELL" if order["positionDirection"] == "long" else "BUY")
            if not self.reserve(deepcopy(claim)):
                raise FuturesProtectionError("FUTURES_PROTECTION_RESERVATION_REQUIRED")
            self.claims[client] = claim
            self.record("FUTURES_PROTECTION_BEFORE_SUBMIT", deepcopy(claim))
            try:
                self.transport.protect(deepcopy(claim), self.guard)
            except Exception:
                # Exchange receipt, including a definite-looking error, is
                # resolved only by this exact ID. No retry and no new UUID.
                pass
        current = self.observe(claim)
        if current.get("status") != "NEW":
            raise FuturesProtectionError("FUTURES_PROTECTION_NOT_ACTIVE")
        self.active_by_symbol[symbol] = client
        if previous and previous != client:
            self.cancel(self.claims[previous])  # replacement already confirmed
        return current

    def cancel(self, claim):
        observed = self.observe(claim)
        if observed.get("status") in {"CANCELED", "CANCELLED", "EXPIRED", "FINISHED", "REJECTED"}:
            return observed
        action = {key: value for key, value in claim.items() if key not in {"status", "observation", "cancelAttempted", "lastReadVerified"}}
        action.update(operation="CANCEL_PROTECTION", riskReducing=True)
        if not claim.get("cancelAttempted"):
            if not self.reserve(deepcopy(action)):
                raise FuturesProtectionError("FUTURES_PROTECTION_CANCEL_RESERVATION_REQUIRED")
            claim["cancelAttempted"] = True
            try:
                self.transport.cancel(action, self.guard)
            except Exception:
                pass
        current = self.observe(claim)
        if current.get("status") not in {"CANCELED", "CANCELLED", "EXPIRED", "FINISHED", "REJECTED"}:
            raise FuturesProtectionError("FUTURES_PROTECTION_CANCEL_UNKNOWN")
        return current

    def poll(self):
        executions = []
        for claim in list(self.claims.values()):
            observed = self.observe(claim)
            if observed.get("actualOrderId"):
                actual = observed.get("actualOrder")
                if observed.get("executionKnown") is not True or not isinstance(actual, dict):
                    raise FuturesProtectionError("FUTURES_PROTECTIVE_EXECUTION_UNKNOWN")
                if (str(actual.get("brokerOrderId")) != str(observed["actualOrderId"])
                        or actual.get("symbol") != claim["symbol"]
                        or actual.get("side") != claim["side"]):
                    raise FuturesProtectionError("FUTURES_PROTECTIVE_EXECUTION_IDENTITY_CHANGED")
                executions.append({"claim": deepcopy(claim), "order": deepcopy(actual)})
                self._incident("FUTURES_PROTECTIVE_STOP_TRIGGERED", symbol=claim["symbol"],
                               algoId=claim["algoId"], brokerOrderId=str(observed["actualOrderId"]))
            if (self.active_by_symbol.get(claim["symbol"]) == claim["clientAlgoId"]
                    and observed.get("status") != "NEW" and not observed.get("actualOrderId")
                    and not claim.get("cancelAttempted")):
                self._incident("FUTURES_PROTECTION_NOT_ACTIVE", symbol=claim["symbol"], algoId=claim["algoId"])
        return executions

    def owns(self, row):
        claim = self.claims.get(str(row.get("clientAlgoId", "")))
        if not claim or not claim.get("algoId"):
            return False
        expected = claim["positionDirection"].upper() if claim["positionMode"] == "HEDGE" else "BOTH"
        return (str(row.get("algoId")) == claim["algoId"] and row.get("symbol") == claim["symbol"]
                and row.get("side") == claim["side"] and row.get("positionSide") == expected
                and row.get("closePosition") is True and row.get("workingType") == "MARK_PRICE"
                and row.get("type", row.get("orderType")) == "STOP_MARKET" and row.get("priceProtect") is False
                and numeric(row.get("triggerPrice"), positive=True) == numeric(claim["triggerPrice"], positive=True))

    def check_positions(self, positions):
        for position in positions:
            if numeric(position.get("quantity")) == 0:
                continue
            symbol = position["symbol"]
            client = self.active_by_symbol.get(symbol)
            claim = self.claims.get(client, {})
            current = claim.get("observation", {})
            if current.get("status") != "NEW":
                self._incident("FUTURES_POSITION_UNPROTECTED", symbol=symbol)
                continue
            mark = numeric(position.get("markPrice"), positive=True)
            liquidation = numeric(position.get("liquidationPrice"))
            trigger = numeric(claim["triggerPrice"], positive=True)
            long = numeric(position["quantity"]) > 0
            if liquidation < 0:
                raise FuturesProtectionError("FUTURES_LIQUIDATION_PRICE_UNKNOWN")
            if (liquidation > 0 and (not liquidation < trigger < mark if long else not mark < trigger < liquidation)):
                self._incident("FUTURES_ACTUAL_LIQUIDATION_BUFFER_BREACHED", symbol=symbol,
                               liquidationPrice=str(liquidation), markPrice=str(mark), triggerPrice=str(trigger))
            elif (long and mark <= trigger) or (not long and mark >= trigger):
                self._incident("FUTURES_STOP_TRIGGER_PENDING", symbol=symbol)

    def finish_flat(self):
        for claim in list(self.claims.values()):
            self.cancel(claim)
        if not self.retirement_verified():
            raise FuturesProtectionError("FUTURES_PROTECTION_RETIREMENT_UNKNOWN")
        self.active_by_symbol.clear()

    def retirement_verified(self):
        for claim in self.claims.values():
            observed = claim.get("observation", {})
            if (claim.get("lastReadVerified") is not True or observed.get("known") is not True or observed.get("status") not in
                    {"CANCELED", "CANCELLED", "EXPIRED", "FINISHED", "REJECTED"}):
                return False
            if observed.get("status") == "FINISHED" and not observed.get("actualOrderId"):
                return False
            if observed.get("actualOrderId"):
                actual = observed.get("actualOrder", {})
                if (observed.get("executionKnown") is not True or actual.get("status") not in
                        {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}):
                    return False
        return True

    def public_rows(self):
        return [{**{key: claim.get(key) for key in ("symbol", "clientAlgoId", "algoId", "triggerPrice", "status", "positionDirection")},
                 "positionSide": claim["positionDirection"].upper() if claim["positionMode"] == "HEDGE" else "BOTH",
                 "verified": claim.get("lastReadVerified") is True,
                 "observedAt": claim.get("observation", {}).get("observedAt")}
                for claim in self.claims.values()]
