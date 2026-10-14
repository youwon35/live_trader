"""Pure native spot quantity consumer; arithmetic never grants order authority."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from collections.abc import Mapping
from trading_runtime.execution_quantity import execution_sizing_from_settings, validate_quantity_contract, size_spot_buy, size_spot_sell

CAPITAL_BASIS = "PORTFOLIO_EQUITY_WITH_CASH_CAP"
RULE_FIELDS = ("schemaVersion", "provider", "symbol", "baseCurrency", "quoteCurrency", "orderType", "stepSize", "minQuantity", "maxQuantity", "minNotional", "maxNotional", "notionalReferenceMinutes", "capabilityStatus")


def decimal_value(value, label, *, positive=False):
    if value is None or isinstance(value, bool):
        raise ValueError(label + ": finite decimal required")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(label + ": finite decimal required") from None
    if parsed.is_finite() and (len(parsed.as_tuple().digits) > 256 or abs(parsed.as_tuple().exponent) > 256):
        raise ValueError(label + ": decimal exceeds explicit 256-digit/exponent bound")
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        raise ValueError(label + ": finite nonnegative decimal required")
    return parsed


def sizing_from_artifact(artifact):
    if not isinstance(artifact, Mapping):
        return None
    execution = artifact.get("execution") or {}
    trader = artifact.get("traderContract") or artifact.get("trader_contract") or {}
    aliases = [artifact.get("executionSizing"), artifact.get("execution_sizing"), execution.get("paperExecutionSizing"), execution.get("paper_execution_sizing"), trader.get("executionSizing")]
    declared = [value for value in aliases if isinstance(value, Mapping) and "quantityContract" in value]
    if not declared:
        return None
    if any(dict(value) != dict(declared[0]) for value in declared[1:]):
        raise ValueError("native spot execution sizing aliases conflict")
    sizing = execution_sizing_from_settings({"executionSizing": dict(declared[0])})
    if sizing.get("capitalBasis") != CAPITAL_BASIS:
        raise ValueError("native spot sizing capital basis is not sealed")
    return sizing


def source_cost_rates(artifact, parameters):
    with localcontext() as precision:
        precision.prec = 2048
        return _source_cost_rates(artifact, parameters)


def _source_cost_rates(artifact, parameters):
    policy = artifact.get("portfolioExecutionPolicy") or artifact.get("executionPolicy") or {}
    def rate(policy_key, parameter_key, direct_key, divisor):
        values = []
        if policy.get(policy_key) is not None:
            values.append(decimal_value(policy[policy_key], policy_key) / 100)
        elif parameters.get(parameter_key) is not None:
            values.append(decimal_value(parameters[parameter_key], parameter_key) / 100)
        if artifact.get(direct_key) is not None and not artifact.get("portfolioExecutionPolicy"):
            values.append(decimal_value(artifact[direct_key], direct_key) / divisor)
        if not values or any(value != values[0] for value in values[1:]):
            raise ValueError("native spot source cost assumptions missing/conflicting")
        return values[0]
    return rate("feePercent", "fee", "fee_rate", 1), rate("slippagePercent", "slippage", "slippage_bps", 10000)


def calculate_model_quantity(sizing, *, side, price, deployment_equity, available_cash, allocation_weight, position_percent, source_fee_rate, source_slippage_rate, actual_fee_rate=None, position_quantity=0, notional_reference_price=None, current_maximum_quote=None):
    """Same numeric inputs as Backtester/Paper; funds are a separate cash cap."""
    sizing = execution_sizing_from_settings({"executionSizing": sizing})
    if sizing.get("capitalBasis") != CAPITAL_BASIS:
        raise ValueError("native spot sizing capital basis is not sealed")
    contract = sizing["quantityContract"]
    fee = decimal_value(source_fee_rate, "source_fee_rate")
    actual_fee = decimal_value(actual_fee_rate, "actual_fee_rate") if actual_fee_rate is not None else None
    conservative_fee = max(fee, actual_fee) if actual_fee is not None else fee
    slippage = decimal_value(source_slippage_rate, "source_slippage_rate")
    with localcontext() as precision:
        precision.prec = 2048
        weight = decimal_value(allocation_weight, "allocation_weight", positive=True)
        percent = decimal_value(position_percent, "position_percent", positive=True)
        if weight > 1 or percent > 100:
            raise ValueError("native spot allocation/position percent outside range")
        if side == "BUY":
            equity = decimal_value(deployment_equity, "deployment_equity", positive=True)
            cash = decimal_value(available_cash, "available_cash")
            capital = min(equity * weight * percent / 100, cash)
            if current_maximum_quote is not None:
                capital = min(capital, decimal_value(current_maximum_quote, "current max quote", positive=True))
            result = size_spot_buy(contract, price=price, spend_capital=capital, fee_rate=conservative_fee, slippage_rate=slippage, notional_reference_price=notional_reference_price)
        elif side == "SELL":
            result = size_spot_sell(contract, price=price, position_quantity=position_quantity, fee_rate=conservative_fee, slippage_rate=slippage, notional_reference_price=notional_reference_price)
        else:
            raise ValueError("native spot quantity requires exact BUY/SELL side")
    return {**result, "capitalBasis": CAPITAL_BASIS, "deploymentEquity":str(decimal_value(deployment_equity,"deployment equity",positive=True)), "availableCashCap":str(decimal_value(available_cash,"cash cap")), "allocationWeight": str(weight), "sourcePositionPercent": str(percent), "sourceFeeRate": str(fee), "feeRateUsed": str(conservative_fee), "feeAssumptionAdjusted": actual_fee is not None and actual_fee > fee, "actualFeeObserved": actual_fee is not None}


def live_quantity(spec, *, side, price, allocation_weight, identity, context=None, position_quantity=0, now=None):
    """Require exact, recent, owned deployment equity and current account/rules.

    The context is an internal preflight observation, never a submitted order
    ticket or account-wide equity. Missing truth stays BLOCKED; cash is not a
    substitute for deployment equity. This function performs no I/O.
    """
    blocked = {"status": "BLOCKED", "quantityForSubmission": None, "nativeEligibility": "UNKNOWN", "modelResult": None, "reasons": [], "orderAuthorityGranted": False}
    try:
        sizing = sizing_from_artifact(spec.artifact)
        if sizing is None:
            return None
        contract = sizing["quantityContract"]
        if spec.broker_id != contract["provider"] or spec.provider != contract["provider"] or str(spec.symbol).upper() != contract["symbol"]:
            raise ValueError("NATIVE_QUANTITY_SOURCE_ROUTE_MISMATCH")
        if not isinstance(context, Mapping):
            raise ValueError("OWNED_DEPLOYMENT_EQUITY_AND_CURRENT_RULES_NOT_OBSERVED")
        if not isinstance(identity.get("deploymentId"), str) or not identity["deploymentId"].strip():
            raise ValueError("NATIVE_QUANTITY_DEPLOYMENT_BINDING_MISSING")
        expected = {**identity, "sourceArtifactHash": spec.artifact_hash, "strategyInstanceId": spec.strategy_instance_id, "portfolioHash": spec.portfolio_hash, "quoteCurrency": contract["quoteCurrency"]}
        if any(context.get(key) != value for key, value in expected.items()):
            raise ValueError("NATIVE_QUANTITY_PREFLIGHT_SCOPE_CHANGED")
        if context.get("equityScope") != "OWNED_DEPLOYMENT_ONLY" or context.get("accountKnown") is not True or context.get("accountFresh") is not True:
            raise ValueError("OWNED_DEPLOYMENT_EQUITY_OR_ACCOUNT_TRUTH_NOT_VERIFIED")
        observed = datetime.fromisoformat(str(context.get("asOf") or "").replace("Z", "+00:00"))
        if observed.tzinfo is None:
            raise ValueError("NATIVE_QUANTITY_PREFLIGHT_TIME_INVALID")
        age = ((now or datetime.now(timezone.utc)) - observed).total_seconds()
        if not 0 <= age <= 120:
            raise ValueError("NATIVE_QUANTITY_PREFLIGHT_STALE")
        current = validate_quantity_contract(context.get("currentQuantityContract"))
        market_observation = context.get("nativeMarketObservation")
        market_verified = False
        if market_observation is not None:
            from .native_spot_preflight import verify_calculation_inputs
            verify_calculation_inputs(context, market_observation, contract, side=side, now=now)
            market_verified = True
        compare_fields = tuple(key for key in RULE_FIELDS if key != "capabilityStatus") if market_verified else RULE_FIELDS
        if market_verified:
            from .native_spot_preflight import source_rules_fit_current
            source_rules_fit_current(contract, current)
        elif any(current.get(key) != contract.get(key) for key in compare_fields):
            raise ValueError("NATIVE_QUANTITY_CURRENT_RULES_DIFFER_FROM_SOURCE")
        source_fee, source_slippage = source_cost_rates(spec.artifact, spec.parameters)
        model = calculate_model_quantity(sizing, side=side, price=price, deployment_equity=context.get("deploymentEquity"), available_cash=context.get("availableCash"), allocation_weight=allocation_weight, position_percent=spec.parameters.get("positionSize"), source_fee_rate=source_fee, source_slippage_rate=source_slippage, actual_fee_rate=context.get("actualFeeRate"), position_quantity=position_quantity, notional_reference_price=context.get("notionalReferencePrice"), current_maximum_quote=(market_observation or {}).get("currentMaximumQuoteAmount"))
        blocked["modelResult"] = model
        blocked["nativeEligibility"] = "VERIFIED" if market_verified else model["nativeEligibility"]
        if model["actualFeeObserved"] is not True:
            raise ValueError("NATIVE_QUANTITY_ACTUAL_ACCOUNT_FEE_NOT_OBSERVED")
        if model["status"] != "READY":
            raise ValueError(model["reason"])
        if blocked["nativeEligibility"] != "VERIFIED":
            raise ValueError("NATIVE_QUANTITY_MARKET_CAPABILITY_OR_REFERENCE_UNPROVEN")
        quantity = decimal_value(model["quantity"], "quantity", positive=True)
        # OrderIntent currently carries float. Refuse lossy decimal transport.
        transported = float(quantity)
        if not Decimal(str(transported)) == quantity:
            raise ValueError("NATIVE_QUANTITY_FLOAT_TRANSPORT_LOSES_EXACT_STEP")
        return {**blocked, "status": "READY", "quantityForSubmission": transported, "reasons": [], "preflightBinding": {"asOf":context.get("asOf"),"accountFingerprint":context.get("accountFingerprint"),"capitalScopeHash":context.get("capitalScopeHash"),"deploymentId":identity["deploymentId"],"sourceContractHash":contract["contractHash"],"marketObservationHash":(market_observation or {}).get("contentHash"),"actualFeeRate":context.get("actualFeeRate"),"sourceSlippageRate":str(source_slippage),"modelFeeRate":model["feeRateUsed"]}}
    except (ValueError, TypeError, KeyError, ArithmeticError) as error:
        blocked["reasons"].append(str(error))
        return blocked
