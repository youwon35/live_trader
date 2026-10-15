"""Live-only intersection of immutable Source sizing and a current Binance cap.

This produces an operational risk bound, never a corrected Source artifact or a
claim that a research fill was reproduced. No network, clock or ledger is read.
"""
from __future__ import annotations

from decimal import ROUND_DOWN, localcontext

from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.execution_quantity import seal_quantity_contract, validate_quantity_contract
from .native_spot_quantity import calculate_model_quantity, decimal_value

VERSION = "live-spot-maximum-intersection-v1"
ROLE = "LIVE_OPERATIONAL_BOUND_NOT_SOURCE_CORRECTION"


def decimal_text(value):
    raw = format(value, "f")
    return raw.rstrip("0").rstrip(".") if "." in raw else raw


def intersection_contract(source, current):
    from .native_spot_preflight import source_rules_fit_current

    source, current = validate_quantity_contract(source), validate_quantity_contract(current)
    source_rules_fit_current(source, current)
    if source["provider"] != "binance":
        raise ValueError("OPERATIONAL_MAXIMUM_BINANCE_ONLY")
    with localcontext() as precision:
        precision.prec = 2048
        step = decimal_value(source["stepSize"], "sealed step", positive=True)
        cap = min(decimal_value(source["maxQuantity"], "Source maximum", positive=True), decimal_value(current["maxQuantity"], "current maximum", positive=True))
        cap = (cap / step).to_integral_value(rounding=ROUND_DOWN) * step
        if cap <= 0 or cap < decimal_value(source["minQuantity"], "sealed minimum"):
            raise ValueError("OPERATIONAL_MAXIMUM_BELOW_MIN_QUANTITY")
    effective = seal_quantity_contract({**source, "maxQuantity": decimal_text(cap),
        "provenance": {"references": source["provenance"]["references"], "scope": ROLE,
                       "sourceContractHash": source["contractHash"], "currentContractHash": current["contractHash"]}})
    return effective


def calculate_live_spot_model(sizing, *, market_observation, now=None, **inputs):
    """Recompute all amounts under the intersection; preserve the original model."""
    from .native_spot_preflight import valid_observation

    source = validate_quantity_contract(sizing["quantityContract"])
    receipt = valid_observation(market_observation, source, side=inputs["side"], now=now)
    original = calculate_model_quantity(sizing, **inputs)
    if source["provider"] != "binance":
        return original
    current = validate_quantity_contract(receipt["currentQuantityContract"])
    effective = intersection_contract(source, current)
    adjusted = dict(inputs)
    held = decimal_value(inputs.get("position_quantity", 0), "owned held quantity")
    if inputs["side"] == "SELL":
        adjusted["position_quantity"] = min(held, decimal_value(effective["maxQuantity"], "operational maximum", positive=True))
    result = calculate_model_quantity({**sizing, "quantityContract": effective}, **adjusted)
    if inputs["side"] == "SELL":
        with localcontext() as precision:
            precision.prec = 2048
            result["residualQuantity"] = decimal_text(held - decimal_value(result["quantity"], "operational quantity"))
    proof = {"version": VERSION, "role": ROLE, "sourceArtifactUnchanged": True,
             "sourceContractHash": source["contractHash"], "currentContractHash": current["contractHash"],
             "marketObservationHash": receipt["contentHash"], "sourceModelHash": stable_sha256(original),
             "operationalContractHash": effective["contractHash"], "sourceMaximumQuantity": source["maxQuantity"],
             "currentMaximumQuantity": current["maxQuantity"], "effectiveMaximumQuantity": effective["maxQuantity"],
             "stepSize": source["stepSize"], "side": inputs["side"], "ownedPositionQuantity": decimal_text(held)}
    proof["contentHash"] = stable_sha256(proof)
    return {**result, "contractHash": source["contractHash"], "sourceModel": original,
            "operationalQuantityPolicy": proof, "operationalContractHash": effective["contractHash"]}


def validate_operational_ticket(model, binding, *, side, now=None):
    """Revalidate the exact current receipt and reduced quantity at dispatch."""
    from .native_spot_preflight import valid_observation

    source = validate_quantity_contract(binding.get("sourceQuantityContract"))
    receipt = valid_observation(binding.get("nativeMarketObservation"), source, side=side, now=now)
    current = validate_quantity_contract(receipt["currentQuantityContract"])
    effective = intersection_contract(source, current)
    proof = model.get("operationalQuantityPolicy")
    if not isinstance(proof, dict) or proof.get("contentHash") != stable_sha256({k: v for k, v in proof.items() if k != "contentHash"}):
        raise ValueError("NATIVE_TICKET_OPERATIONAL_POLICY_INVALID")
    expected = {"version": VERSION, "role": ROLE, "sourceArtifactUnchanged": True,
                "sourceContractHash": source["contractHash"], "currentContractHash": current["contractHash"],
                "marketObservationHash": receipt["contentHash"], "operationalContractHash": effective["contractHash"],
                "sourceMaximumQuantity": source["maxQuantity"], "currentMaximumQuantity": current["maxQuantity"],
                "effectiveMaximumQuantity": effective["maxQuantity"], "stepSize": source["stepSize"], "side": side}
    if any(proof.get(key) != value for key, value in expected.items()):
        raise ValueError("NATIVE_TICKET_OPERATIONAL_POLICY_SCOPE_CHANGED")
    if (binding.get("marketObservationHash") != receipt["contentHash"]
        or binding.get("currentQuantityContractHash") != current["contractHash"]
        or binding.get("operationalQuantityPolicyHash") != proof["contentHash"]
        or binding.get("sourceContractHash") != source["contractHash"]
        or model.get("contractHash") != source["contractHash"]
        or model.get("operationalContractHash") != effective["contractHash"]):
        raise ValueError("NATIVE_TICKET_CURRENT_OPERATIONAL_RECEIPT_CHANGED")
    original = model.get("sourceModel")
    if not isinstance(original, dict) or original.get("contractHash") != source["contractHash"] or proof.get("sourceModelHash") != stable_sha256(original):
        raise ValueError("NATIVE_TICKET_SOURCE_MODEL_PROVENANCE_CHANGED")
    for key in ("fillPrice", "capitalBasis", "deploymentEquity", "availableCashCap", "allocationWeight", "sourcePositionPercent", "sourceFeeRate", "feeRateUsed", "actualFeeObserved", "feeAssumptionAdjusted"):
        if model.get(key) != original.get(key):
            raise ValueError("NATIVE_TICKET_OPERATIONAL_INPUTS_CHANGED")
    with localcontext() as precision:
        precision.prec = 2048
        quantity = decimal_value(model.get("quantity"), "operational quantity", positive=True)
        step = decimal_value(source["stepSize"], "sealed step", positive=True)
        cap = decimal_value(effective["maxQuantity"], "operational maximum", positive=True)
        if side == "BUY":
            if original.get("status") != "READY":
                raise ValueError("NATIVE_TICKET_SOURCE_BUY_MODEL_BLOCKED")
            before = decimal_value(original.get("quantity"), "source model quantity", positive=True)
        else:
            before = decimal_value(proof.get("ownedPositionQuantity"), "owned position quantity", positive=True)
        expected_quantity = (min(before, cap) / step).to_integral_value(rounding=ROUND_DOWN) * step
        reference = (receipt.get("marketReference") or {}).get("price") if source["notionalReferenceMinutes"] else model.get("fillPrice")
        notional = quantity * decimal_value(reference, "notional reference", positive=True)
        if (quantity != expected_quantity or quantity > cap or quantity % step
            or quantity < decimal_value(source["minQuantity"], "sealed minimum")
            or notional < decimal_value(source["minNotional"], "sealed minimum notional")
            or (decimal_value(source["maxNotional"], "sealed maximum notional") and notional > decimal_value(source["maxNotional"], "sealed maximum notional"))):
            raise ValueError("NATIVE_TICKET_OPERATIONAL_QUANTITY_OUTSIDE_INTERSECTION")
        fill = decimal_value(model["fillPrice"], "operational fill", positive=True)
        fee = quantity * fill * decimal_value(model["feeRateUsed"], "operational fee")
        if side == "BUY":
            if decimal_value(model["totalSpend"], "total spend") != quantity * fill + fee:
                raise ValueError("NATIVE_TICKET_OPERATIONAL_TOTAL_SPEND_CHANGED")
        elif (decimal_value(model["residualQuantity"], "residual quantity") != before - quantity
              or decimal_value(model["netProceeds"], "net proceeds") != quantity * fill - fee):
            raise ValueError("NATIVE_TICKET_OPERATIONAL_SELL_ACCOUNTING_CHANGED")
    return {"effectiveMaximumQuantity": effective["maxQuantity"], "sourceArtifactUnchanged": True, "orderAuthorityGranted": False}
