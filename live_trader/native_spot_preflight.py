"""Fresh official spot observations. GET only; no permits, orders or persisted secrets."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from collections.abc import Mapping
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.execution_quantity import seal_quantity_contract, validate_quantity_contract
from .native_spot_quantity import decimal_value, RULE_FIELDS
from .read_only_account import ReadFailure
from .live_adapters import sign_binance_query

SCHEMA = "live-native-spot-observation-v1"
UPBIT_PRECISION_REFERENCE = "https://docs.upbit.com/kr/docs/faq-order"
MATH_FIELDS = tuple(k for k in RULE_FIELDS if k != "capabilityStatus")


def timestamp(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("NATIVE_OBSERVATION_TIME_INVALID")
    return result


def seal(body):
    body = dict(body)
    body.pop("contentHash", None)
    return {**body, "contentHash": stable_sha256(body)}


def valid_observation(observation, source, *, side, now=None):
    """Validate typed producer receipt; raw caller prices never attest an average."""
    source = validate_quantity_contract(source)
    if not isinstance(observation, Mapping):
        raise ValueError("CURRENT_NATIVE_OBSERVATION_MISSING")
    body = {key:value for key,value in observation.items() if key != "contentHash"}
    if observation.get("contentHash") != stable_sha256(body) or body.get("schemaVersion") != SCHEMA:
        raise ValueError("CURRENT_NATIVE_OBSERVATION_SEAL_INVALID")
    if body.get("sourceContractHash") != source["contractHash"] or body.get("provider") != source["provider"] or body.get("symbol") != source["symbol"] or body.get("side") != side:
        raise ValueError("CURRENT_NATIVE_OBSERVATION_SCOPE_CHANGED")
    age = ((now or datetime.now(timezone.utc)) - timestamp(body.get("asOf"))).total_seconds()
    if not 0 <= age <= 15 or body.get("credentialsUnchanged") is not True:
        raise ValueError("CURRENT_NATIVE_OBSERVATION_STALE")
    current = validate_quantity_contract(body.get("currentQuantityContract"))
    source_rules_fit_current(source, current)
    if source["schemaVersion"] != "native-spot-quantity-v1" or body.get("marketCapabilityVerified") is not True:
        raise ValueError("CURRENT_SPOT_CAPABILITY_NOT_VERIFIED")
    if body.get("accountBinding") != "SAVED_CREDENTIAL_CONFIGURATION_OBSERVED" or body.get("orderability") != "MARKET_AND_ACCOUNT_ENABLED" or body.get("openOrders") != "SYMBOL_NONE_OBSERVED":
        raise ValueError("CURRENT_ACCOUNT_OR_OPEN_ORDERS_NOT_VERIFIED")
    fee = decimal_value(body.get("feeRate"), "actual fee")
    if fee >= 1 or body.get("feeScope") not in {"CURRENT_SYMBOL_SIDE_TAKER_NO_DISCOUNT_UPPER_BOUND", "CURRENT_UPBIT_SIDE_FEE"}:
        raise ValueError("CURRENT_ACTUAL_SIDE_FEE_NOT_VERIFIED")
    reference = body.get("marketReference")
    if source["notionalReferenceMinutes"] > 0:
        if not isinstance(reference, Mapping) or reference.get("kind") != "BINANCE_AVG_PRICE" or reference.get("symbol") != source["symbol"] or type(reference.get("minutes")) is not int or reference.get("minutes") != source["notionalReferenceMinutes"]:
            raise ValueError("CURRENT_AVERAGE_INTERVAL_OR_SYMBOL_MISMATCH")
        if reference.get("responseHash") is None or reference.get("endpoint") != "/api/v3/avgPrice":
            raise ValueError("CURRENT_AVERAGE_SOURCE_NOT_VERIFIED")
        from .binance_reference_clock import validate_average_reference
        validate_average_reference(reference, as_of=timestamp(body["asOf"]), now=now or datetime.now(timezone.utc))
        decimal_value(reference.get("price"), "average", positive=True)
    return dict(observation)


def _binance_rules(source, market):
    if market.get("symbol") != source["symbol"] or market.get("baseAsset") != source["baseCurrency"] or market.get("quoteAsset") != source["quoteCurrency"]:
        raise ValueError("CURRENT_SYMBOL_CURRENCIES_DIFFER")
    if market.get("status") != "TRADING" or market.get("isSpotTradingAllowed") is not True or "MARKET" not in market.get("orderTypes", []):
        raise ValueError("CURRENT_MARKET_CAPABILITY_BLOCKED")
    filters = {}
    for row in market.get("filters", []):
        kind = row.get("filterType")
        if kind in filters:
            raise ValueError("CURRENT_FILTER_DUPLICATE")
        filters[kind] = row
    lot, market_lot = filters["LOT_SIZE"], filters["MARKET_LOT_SIZE"]
    steps = [decimal_value(row["stepSize"], "step") for row in (lot,market_lot) if decimal_value(row["stepSize"],"step") > 0]
    if not steps:
        raise ValueError("CURRENT_LOT_STEP_MISSING")
    step = max(steps)
    if any(step % candidate for candidate in steps):
        raise ValueError("CURRENT_LOT_INTERSECTION_UNSUPPORTED")
    maxima = [decimal_value(row["maxQty"],"max") for row in (lot,market_lot) if decimal_value(row["maxQty"],"max") > 0]
    if not maxima:
        raise ValueError("CURRENT_MAX_QUANTITY_MISSING")
    n = filters.get("NOTIONAL")
    if n is not None:
        if type(n.get("applyMinToMarket")) is not bool or type(n.get("applyMaxToMarket")) is not bool:
            raise ValueError("CURRENT_NOTIONAL_MARKET_FLAGS_MISSING")
        min_notional = n["minNotional"] if n["applyMinToMarket"] else "0"
        max_notional = n["maxNotional"] if n["applyMaxToMarket"] else "0"
    else:
        n = filters["MIN_NOTIONAL"]
        if type(n.get("applyToMarket")) is not bool:
            raise ValueError("CURRENT_NOTIONAL_MARKET_FLAGS_MISSING")
        min_notional = n["minNotional"] if n["applyToMarket"] else "0"
        max_notional = "0"
    return seal_quantity_contract({**{key:value for key,value in source.items() if key != "contractHash"},
        "stepSize": str(step), "minQuantity": str(max(decimal_value(lot["minQty"],"min"),decimal_value(market_lot["minQty"],"min"))),
        "maxQuantity": str(min(maxima)), "minNotional": min_notional, "maxNotional": max_notional,
        "notionalReferenceMinutes": n["avgPriceMins"], "capabilityStatus":"VERIFIED",
        "provenance":{"references":["https://developers.binance.com/en/docs/products/spot/filters"],"symbolRulesHash":stable_sha256(market)}})


def observe_native_spot(reader, source, *, side, now=None):
    """Calls bounded official GETs through ReadOnlyTransport; returns memory-only truth."""
    source = validate_quantity_contract(source)
    if source["schemaVersion"] != "native-spot-quantity-v1" or side not in {"BUY","SELL"}:
        raise ValueError("NATIVE_SPOT_ONLY_AND_EXACT_SIDE_REQUIRED")
    begin = now or datetime.now(timezone.utc)
    settings_hash = stable_sha256(reader.settings)
    provider, symbol = source["provider"], source["symbol"]
    observation = reader.observe_preparation(provider, symbol, side=side)
    if observation.get("account") != "AVAILABLE":
        raise ValueError("CURRENT_NATIVE_ACCOUNT_UNAVAILABLE")
    reference = None
    if provider == "binance":
        reader._credentials(provider)
        info,_ = reader._get(provider,"/api/v3/exchangeInfo",query={"symbol":symbol})
        matches = [row for row in info.get("symbols",[]) if row.get("symbol")==symbol]
        if len(matches)!=1:
            raise ValueError("CURRENT_SYMBOL_RULES_AMBIGUOUS")
        current = _binance_rules(source,matches[0])
        from .binance_reference_clock import observe_clock
        clock = observe_clock(reader, now=now)
        milliseconds = clock["serverTimeMilliseconds"]
        query = sign_binance_query({"symbol":symbol,"timestamp":milliseconds,"recvWindow":5000},reader.settings["BINANCE_API_SECRET"])
        commission,_ = reader._get(provider,"/api/v3/account/commission",query=query,headers={"X-MBX-APIKEY":reader.settings["BINANCE_API_KEY"]})
        if commission.get("symbol") != symbol:
            raise ValueError("CURRENT_COMMISSION_SYMBOL_MISMATCH")
        with localcontext() as precision:
            precision.prec = 2048
            fee = sum((decimal_value(commission[kind]["taker"],"taker")+decimal_value(commission[kind]["buyer" if side=="BUY" else "seller"],"side fee") for kind in ("standardCommission","specialCommission","taxCommission")),Decimal(0))
        fee_scope = "CURRENT_SYMBOL_SIDE_TAKER_NO_DISCOUNT_UPPER_BOUND"
        if source["notionalReferenceMinutes"] > 0:
            average,_ = reader._get(provider,"/api/v3/avgPrice",query={"symbol":symbol})
            if type(average.get("closeTime")) is not int or type(average.get("mins")) is not int:
                raise ValueError("CURRENT_AVERAGE_RESPONSE_INVALID")
            average_received = now or datetime.now(timezone.utc)
            # The official avgPrice closeTime is the last trade time, not the
            # response time. Observe server time after that response, then bind
            # its age to a bounded local request interval.
            reference = {"kind":"BINANCE_AVG_PRICE","symbol":symbol,"minutes":average["mins"],"price":average["price"],"closeTime":datetime.fromtimestamp(average["closeTime"]/1000,timezone.utc).isoformat(),"endpoint":"/api/v3/avgPrice","responseHash":stable_sha256(average),
                         "responseReceivedAt":average_received.isoformat(),"closeTimeRole":"LAST_TRADE_TIME",
                         "freshnessClock":"BINANCE_SERVER_TIME_WITH_BOUNDED_LOCAL_RTT","clockObservation":observe_clock(reader, now=now)}
    else:
        reader._credentials("upbit")
        from .live_adapters import build_upbit_authorization
        query={"market":symbol}
        chance,_=reader._get("upbit","/v1/orders/chance",query=query,headers={"Authorization":build_upbit_authorization(reader.settings["UPBIT_ACCESS_KEY"],reader.settings["UPBIT_SECRET_KEY"],query)})
        market=chance.get("market",{})
        if market.get("id") != symbol or market.get("state")!="active" or source["quoteCurrency"]!="KRW":
            raise ValueError("CURRENT_UPBIT_MARKET_MISMATCH")
        supported=market.get("bid_types" if side=="BUY" else "ask_types",[])
        if ("price" if side=="BUY" else "market") not in supported:
            raise ValueError("CURRENT_UPBIT_MARKET_SIDE_UNSUPPORTED")
        # The official 8-place BUY-formed quantity precision applies only to owned positions.
        if source["stepSize"]!="0.00000001" or source["minQuantity"]!="0.00000001" or source["notionalReferenceMinutes"]!=0 or UPBIT_PRECISION_REFERENCE not in source["provenance"]["references"]:
            raise ValueError("CURRENT_UPBIT_PRECISION_SCOPE_UNPROVEN")
        current=seal_quantity_contract({**{k:v for k,v in source.items() if k!="contractHash"},"minNotional":str(decimal_value((market.get("bid") or {})["min_total"],"minimum")),"capabilityStatus":"VERIFIED","provenance":{"references":[UPBIT_PRECISION_REFERENCE],"chanceHash":stable_sha256(chance),"scope":"OWNED_BUY_FORMED_QUANTITY_ONLY"}})
        # Current max_total is also enforced even though the Source sentinel is only local.
        observation["currentMaximumQuoteAmount"]=str(decimal_value(market["max_total"],"maximum",positive=True))
        fee=decimal_value(chance["bid_fee" if side=="BUY" else "ask_fee"],"side fee")
        fee_scope="CURRENT_UPBIT_SIDE_FEE"
    end=now or datetime.now(timezone.utc)
    if not 0 <= (end-begin).total_seconds() <= 15:
        raise ValueError("CURRENT_NATIVE_MULTIPLE_READS_STALE")
    body={**observation,"schemaVersion":SCHEMA,"provider":provider,"symbol":symbol,"side":side,"asOf":end.isoformat(),"sourceContractHash":source["contractHash"],"currentQuantityContract":current,"marketCapabilityVerified":True,"sourceCurrentRuleDifferences":source_rules_fit_current(source,current),"feeRate":str(fee),"feeScope":fee_scope,"marketReference":reference,"credentialsUnchanged":stable_sha256(reader.settings)==settings_hash,"priceRole":"READ_ONLY_REFERENCE_NOT_NEXT_OPEN","orderAuthorityGranted":False}
    receipt=seal(body)
    return valid_observation(receipt,source,side=side,now=end)

def verify_calculation_inputs(context, receipt, source, *, side, fee_field="actualFeeRate", now=None):
    valid_observation(receipt, source, side=side, now=now)
    current = validate_quantity_contract(context.get("currentQuantityContract"))
    if context.get("asOf") != receipt.get("asOf") or current["contractHash"] != receipt["currentQuantityContract"]["contractHash"]:
        raise ValueError("NATIVE_QUANTITY_TYPED_OBSERVATION_CONTEXT_CHANGED")
    if decimal_value(context.get(fee_field), "context fee") != decimal_value(receipt.get("feeRate"), "observed fee"):
        raise ValueError("NATIVE_QUANTITY_TYPED_FEE_CHANGED")
    expected = (receipt.get("marketReference") or {}).get("price")
    actual = context.get("notionalReferencePrice")
    if (expected is None) != (actual is None) or (expected is not None and decimal_value(actual,"context average",positive=True) != decimal_value(expected,"observed average",positive=True)):
        raise ValueError("NATIVE_QUANTITY_TYPED_AVERAGE_CHANGED")
    if decimal_value(context.get("availableCash"),"context cash") > decimal_value(receipt.get("availableCash"),"observed cash"):
        raise ValueError("NATIVE_QUANTITY_TYPED_AVAILABLE_CASH_EXCEEDED")
    return current

def source_rules_fit_current(source, current):
    """Keep sealed rules; Binance maximum drift requires a LIVE-only intersection."""
    source=validate_quantity_contract(source);current=validate_quantity_contract(current)
    differences=[]
    for key in MATH_FIELDS:
        if current.get(key)==source.get(key):
            continue
        if key=="maxQuantity" and (source["provider"] == "binance" or decimal_value(current[key],"current maximum",positive=True)>=decimal_value(source[key],"Source maximum",positive=True)):
            effect = "LIVE_OPERATIONAL_MAXIMUM_INTERSECTION" if decimal_value(current[key],"current maximum",positive=True) < decimal_value(source[key],"Source maximum",positive=True) else "FROZEN_SOURCE_LOWER_CAP_RETAINED"
            differences.append({"field":key,"source":source[key],"current":current[key],"effect":effect,"sourceArtifactUnchanged":True})
        else:
            raise ValueError("CURRENT_NATIVE_RULES_DIFFER_FROM_SOURCE")
    return differences
