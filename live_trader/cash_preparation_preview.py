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
              "quantityStatus": "BLOCKED", "maximumQuantity": None, "reasons": []}
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
