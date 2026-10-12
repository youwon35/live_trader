"""Exact decimal cash sizing for information-only previews; never a trading permit."""
from decimal import Decimal, InvalidOperation, localcontext


def number(value):
    try:
        value = Decimal(str(value))
        return value if value.is_finite() and value >= 0 else None
    except (ValueError, InvalidOperation, TypeError):
        return None


def cash_preview(instrument, observation, draft):
    result = {"capitalBasis": "ACCOUNT_AVAILABLE_CASH", "capitalPercent": 100,
              "sourceMaxOrderPercent": instrument.get("sourceMaxOrderPercent"),
              "allocationWeight": instrument.get("allocationWeight"), "status": "BLOCKED",
              "executable": False, "orderSubmissionEnabled": False, "armingEnabled": False,
              "quantityStatus": "BLOCKED", "maximumQuantity": None, "calculationScope": "BUY_CASH_CAP_ONLY", "reasons": []}
    if draft.get("side") != "BUY":
        result["status"] = "NOT_APPLICABLE" if draft.get("side") == "SELL" else "BLOCKED"
        result["reasons"].append("매수 방향을 직접 선택한 경우에만 현금 기반 매수 수량을 계산합니다." if draft.get("side") != "SELL" else "매도 수량은 현금 기반 매수 한도로 계산하지 않습니다. 실제 보유·주문가능 수량과 reduce-only/노출 검사를 별도로 확인해야 합니다.")
        return result
    if (instrument.get("executionSizing") or {}).get("quantityContract") is not None:
        return native_contract_preview(instrument, observation, draft, result)
    cash = number(observation.get("availableCash"))
    weight = number(instrument.get("allocationWeight"))
    percent = number(instrument.get("sourceMaxOrderPercent"))
    policy = instrument.get("executionPolicy") or {}
    fee = number(policy.get("feePercent"))
    slippage = number(policy.get("slippagePercent"))
    price = number(draft.get("limitPrice"))
    if cash is None or weight is None or weight > 1 or percent is None or percent > 100 or fee is None or slippage is None:
        result["reasons"].append("주문가능현금·원본 allocation·원본 수수료/슬리피지 결합이 미확인입니다.")
        return result
    broker_fee = number(observation.get("feeRate"))
    actual_fee = max(fee / 100, broker_fee) if broker_fee is not None else fee / 100
    with localcontext() as context:
        context.prec = 80
        budget = cash * weight * percent / 100
        usable = budget / (1 + actual_fee + slippage / 100)
        result.update(availableCash=format(cash, "f"), allocatedCash=format(cash * weight, "f"),
                      sourceOrderBudget=format(budget, "f"), notionalAfterCostAllowance=format(usable, "f"),
                      feeRateUsed=format(actual_fee, "f"), slippagePercent=format(slippage, "f"),
                      actualBrokerFeeObserved=broker_fee is not None)
        if price is None or price <= 0:
            result["reasons"].append("지정가를 직접 입력해야 수량·호가 단위를 검사할 수 있습니다.")
            return result
        constraints = observation.get("constraints") or {}
        lot = constraints.get("LOT_SIZE") or {}
        grid = constraints.get("PRICE_FILTER") or {}
        step = number(lot.get("stepSize", constraints.get("quantityStep")))
        minimum = number(lot.get("minQty", constraints.get("minQuantity")))
        tick = number(grid.get("tickSize", constraints.get("priceTick")))
        if step is None or step <= 0 or minimum is None or tick is None or tick <= 0:
            result["reasons"].append("거래소의 정확한 수량·호가 단위를 확인하지 못해 실행 수량을 확정하지 않습니다.")
            return result
        amount = (usable / price // step) * step
        upper = number(lot.get("maxQty", constraints.get("maximumBuyQuantity")))
        if upper is not None:
            amount = min(amount, (upper // step) * step)
        notional = amount * price
        minimum_notional = number((constraints.get("NOTIONAL") or constraints.get("MIN_NOTIONAL") or {}).get("minNotional", constraints.get("minNotional")))
        maximum_notional = number((constraints.get("NOTIONAL") or {}).get("maxNotional", constraints.get("maxNotional")))
        low_price = number(grid.get("minPrice")); high_price = number(grid.get("maxPrice"))
        if price % tick or (low_price and price < low_price) or (high_price and price > high_price):
            result["reasons"].append("입력 지정가가 현재 거래소 PRICE_FILTER와 다릅니다.")
        if amount < minimum or amount <= 0 or minimum_notional is None or notional < minimum_notional or (maximum_notional and notional > maximum_notional):
            result["reasons"].append("원본 allocation·5% 한도에서 최소 주문 단위/금액을 충족하지 못합니다.")
        if broker_fee is None:
            result["reasons"].append("실제 계좌 적용 수수료가 미확인입니다. 원본 가정만으로 실행 수량을 확정하지 않습니다.")
        if observation.get("orderability") != "MARKET_AND_ACCOUNT_ENABLED" or observation.get("accountBinding") != "SAVED_CREDENTIAL_CONFIGURATION_OBSERVED":
            result["reasons"].append("현재 저장 계좌 또는 시장 주문 가능 표시가 미확인입니다.")
        if not result["reasons"]:
            result.update(status="CASH_AND_STATIC_GRID_OBSERVED", quantityStatus="STATIC_RULES_OBSERVED",
                          maximumQuantity=format(amount, "f"), quantityStep=format(step, "f"), priceTick=format(tick, "f"))
        result["reasons"].append("봉인 Paper 근거·Live 계좌 심사·실행 직전 재조회와 동적 위험검사는 별도이며 이 결과는 주문 권한이 없습니다.")
    return result


def native_contract_preview(instrument, observation, draft, result):
    """A native Source uses owned deployment equity, with cash as a separate cap."""
    from .native_spot_quantity import CAPITAL_BASIS, RULE_FIELDS, calculate_model_quantity
    from trading_runtime.execution_quantity import validate_quantity_contract
    result.update(capitalBasis=CAPITAL_BASIS, initialBudgetBasis="ACCOUNT_AVAILABLE_CASH", calculationScope="SOURCE_MARKET_QUANTITY_MODEL_AT_ENTERED_REFERENCE_PRICE", modelQuantity=None, nativeEligibility="UNKNOWN", availableCash=observation.get("availableCash"), deploymentEquity=observation.get("deploymentEquity"), actualBrokerFeeObserved=observation.get("feeRate") is not None)
    if observation.get("equityScope") != "OWNED_DEPLOYMENT_ONLY" or observation.get("deploymentEquity") is None:
        result["reasons"].append("배포에 배정된 현재 자본·보유 가치가 미확인입니다. 주문가능현금을 현재 배포 자본으로 대신하지 않습니다.")
        return result
    try:
        policy = instrument.get("executionPolicy") or {}
        fee = number(policy.get("feePercent")); slippage = number(policy.get("slippagePercent"))
        if fee is None or slippage is None:
            raise ValueError("봉인 원본의 수수료·슬리피지가 미확인입니다.")
        model = calculate_model_quantity(instrument["executionSizing"], side="BUY", price=draft.get("limitPrice"), deployment_equity=observation["deploymentEquity"], available_cash=observation.get("availableCash"), allocation_weight=instrument.get("allocationWeight"), position_percent=instrument.get("sourceMaxOrderPercent"), source_fee_rate=fee/100, source_slippage_rate=slippage/100, actual_fee_rate=observation.get("feeRate"), notional_reference_price=observation.get("notionalReferencePrice"))
        result.update(modelQuantity=model["quantity"], quantityContractHash=model["contractHash"], nativeEligibility=model["nativeEligibility"], feeRateUsed=model["feeRateUsed"], feeAssumptionAdjusted=model["feeAssumptionAdjusted"], actualBrokerFeeObserved=model["actualFeeObserved"], modelResult=model)
        current = validate_quantity_contract(observation.get("currentQuantityContract"))
        source = instrument["executionSizing"]["quantityContract"]
        if any(current.get(key) != source.get(key) for key in RULE_FIELDS):
            raise ValueError("현재 거래소 수량 규칙이 봉인 원본과 다릅니다.")
        if model["status"] != "READY":
            raise ValueError("현재 자본 배정에서 최소 수량·금액을 충족하지 못했습니다: " + model["reason"])
        if model["nativeEligibility"] != "VERIFIED":
            raise ValueError("거래소 정밀도 또는 실제 시장 평균가격 기준이 미확인입니다. 수량 모델의 READY는 실주문 가능을 뜻하지 않습니다.")
        if model["actualFeeObserved"] is not True or observation.get("accountBinding") != "SAVED_CREDENTIAL_CONFIGURATION_OBSERVED" or observation.get("orderability") != "MARKET_AND_ACCOUNT_ENABLED":
            raise ValueError("현재 저장 계좌·적용 수수료·시장 주문 가능 표시가 미확인입니다.")
        result.update(status="NATIVE_SOURCE_MODEL_OBSERVED", quantityStatus="SOURCE_QUANTITY_MODEL_OBSERVED", maximumQuantity=model["quantity"])
    except (ValueError, TypeError, KeyError, ArithmeticError) as error:
        result["reasons"].append(str(error))
    result["reasons"].append("계산은 입력 기준가격의 모델이며 Paper 봉인 근거·Live 계좌 심사·현재 배포 자본 및 실행 직전 재조회와 위험검사는 별도입니다. 주문 권한이 없습니다.")
    return result
