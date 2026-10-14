"""Final native ticket integrity. Valid math remains separate from order authority."""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import re
from .native_spot_quantity import decimal_value
from .native_spot_preflight import timestamp

def validate_native_ticket(intent,*,now=None):
    metadata=intent.metadata or {}
    boundary=metadata.get("native_open_boundary")
    model=metadata.get("native_quantity_model")
    binding=metadata.get("native_preflight")
    if not isinstance(boundary,dict) or not isinstance(model,dict) or not isinstance(binding,dict) or model.get("status")!="READY" or metadata.get("native_quantity_status")!="READY":
        raise ValueError("NATIVE_TICKET_EXACT_MODEL_AND_BOUNDARY_REQUIRED")
    provider=metadata.get("broker_id")
    if provider not in {"binance","upbit"} or boundary.get("provider")!=provider or boundary.get("symbol")!=intent.symbol or boundary.get("attestation")!={"binance":"BINANCE_WEBSOCKET","upbit":"UPBIT_WEBSOCKET"}[provider] or boundary.get("recoveryOnly") is not False or boundary.get("promotionEligible") is not True:
        raise ValueError("NATIVE_TICKET_BOUNDARY_ROUTE_UNPROVEN")
    if boundary.get("evaluationKey")!=metadata.get("runtime_evaluation_key") or boundary.get("sourceArtifactHash")!=metadata.get("source_artifact_hash") or binding.get("deploymentId")!=metadata.get("deployment_id") or binding.get("sourceContractHash")!=model.get("contractHash"):
        raise ValueError("NATIVE_TICKET_BOUNDARY_SCOPE_CHANGED")
    for value in (boundary.get("rawHash"),boundary.get("closedBarHash"),binding.get("marketObservationHash"),binding.get("capitalScopeHash"),binding.get("accountFingerprint")):
        if not isinstance(value,str) or not re.fullmatch("[a-f0-9]{64}",value):
            raise ValueError("NATIVE_TICKET_PROOF_BINDINGS_MISSING")
    current=now or datetime.now(timezone.utc)
    if not 0<=(current-timestamp(boundary["startTime"])).total_seconds()<=30 or not 0<=(timestamp(boundary["observedTime"])-timestamp(boundary["startTime"])).total_seconds()<=30 or not 0<=(current-timestamp(binding["asOf"])).total_seconds()<=15:
        raise ValueError("NATIVE_TICKET_PRICE_OR_PREFLIGHT_EXPIRED")
    if intent.side not in {"BUY","SELL"} or model.get("actualFeeObserved") is not True:
        raise ValueError("NATIVE_TICKET_ACTUAL_FEE_OR_SIDE_MISSING")
    with localcontext() as precision:
        precision.prec=2048
        raw=decimal_value(boundary["open"],"exact open",positive=True)
        slip=decimal_value(binding.get("sourceSlippageRate"),"Source slippage")
        fill=raw*(1+slip if intent.side=="BUY" else 1-slip)
        quantity=decimal_value(model.get("quantity"),"quantity",positive=True)
        notional=quantity*fill
        fee=notional*decimal_value(model["feeRateUsed"],"fee")
        if Decimal(str(intent.quantity))!=quantity or Decimal(str(intent.reference_price))!=fill or decimal_value(model["fillPrice"],"modeled fill")!=fill or decimal_value(model["notional"],"quote notional")!=notional or decimal_value(model["fee"],"quote fee")!=fee:
            raise ValueError("NATIVE_TICKET_DECIMAL_MODEL_TRANSPORT_CHANGED")
        if decimal_value(binding["actualFeeRate"],"actual fee")>decimal_value(model["feeRateUsed"],"conservative fee"):
            raise ValueError("NATIVE_TICKET_FEE_ALLOWANCE_NOT_CONSERVATIVE")
    text=format(notional,"f")
    text=text.rstrip("0").rstrip(".") if "." in text else text
    return {"quoteAmount":text,"quantity":model["quantity"],"orderAuthorityGranted":False,"actualFillQuantityGuaranteed":False}

def native_upbit_quote_amount(intent,*,now=None):
    if intent.side!="BUY" or intent.metadata.get("broker_id")!="upbit":
        raise ValueError("NATIVE_UPBIT_BUY_ONLY")
    return validate_native_ticket(intent,now=now)["quoteAmount"]
