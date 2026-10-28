"""Versioned, non-promotional authority for an operator-started short Live test.

This contract is separate from STANDARD Paper qualification.  Constructing it
does not issue a broker permission, change an account, or submit an order.
"""
from __future__ import annotations

from .leverage_accounting import model_binding
from .futures_leverage_limits import MAX_SAFE_LEVERAGE_INTEGER, leverage_integer

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping
from uuid import uuid4

SCHEMA = "short-live-functional-contract-v2"
PURPOSE = "FUNCTIONAL_TEST_ONLY"
CONFIRMATION_TEXT = "실제 주문 시험 시작"
ROUTES = {
    "KIS_KR_CASH": "KIS",
    "BINANCE_SPOT": "BINANCE",
    "BINANCE_USDM_FUTURES": "BINANCE",
    "UPBIT_KRW_SPOT": "UPBIT",
}
TIMEFRAMES = frozenset({"5m", "1h", "1d"})
MAX_CAPITAL_KRW = 100_000.0
MAX_DAILY_LOSS_KRW = 50_000.0
_HASH = re.compile(r"^[0-9a-f]{64}$")


class ShortLiveContractError(ValueError):
    pass


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ShortLiveContractError("AWARE_UTC_CLOCK_REQUIRED")
    return value.astimezone(timezone.utc)


def utc_text(value: datetime) -> str:
    return utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def finite(value: Any, name: str, *, minimum: float = 0,
           maximum: float | None = None, positive: bool = False) -> float:
    # Official adapters preserve Decimal arithmetic as canonical JSON strings.
    # Only decimal digits are accepted: no NaN, infinity or implicit fallback.
    if isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
        value = float(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShortLiveContractError(f"{name}_NUMBER_REQUIRED")
    result = float(value)
    if not math.isfinite(result) or result < minimum or (positive and result <= 0):
        raise ShortLiveContractError(f"{name}_INVALID")
    if maximum is not None and result > maximum:
        raise ShortLiveContractError(f"{name}_EXCEEDS_LIMIT")
    return result


def text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ShortLiveContractError(f"{name}_REQUIRED")
    if len(value) > 512 or any(ord(ch) < 32 for ch in value):
        raise ShortLiveContractError(f"{name}_INVALID")
    return value


def hash_text(value: Any, name: str) -> str:
    result = text(value, name)
    if not _HASH.fullmatch(result):
        raise ShortLiveContractError(f"{name}_SHA256_REQUIRED")
    return result


def normalize_futures_policy(value: Any) -> dict[str, Any]:
    """Freeze the operator's expected configuration; never change the account.

    The percentage is only the entry-price trigger distance. Actual exchange
    margin tiers and liquidation distance must still pass the broker guard.
    """
    constants = {"orderType": "STOP_MARKET", "workingType": "MARK_PRICE",
                 "closePosition": True, "triggerBasis": "ENTRY_LIMIT_PRICE"}
    if not isinstance(value, Mapping):
        raise ShortLiveContractError("FUTURES_PROTECTION_POLICY_REQUIRED")
    if set(value) - ({"leverage", "marginMode", "stopLossPercent"} | constants.keys()):
        raise ShortLiveContractError("FUTURES_PROTECTION_POLICY_INVALID")
    try:
        leverage = leverage_integer(value.get("leverage"))
    except ValueError as error:
        raise ShortLiveContractError("FUTURES_LEVERAGE_INTEGER_REQUIRED") from error
    if value.get("marginMode") != "ISOLATED":
        raise ShortLiveContractError("FUTURES_ISOLATED_MARGIN_REQUIRED")
    stop = finite(value.get("stopLossPercent"), "FUTURES_STOP_LOSS_PERCENT", positive=True)
    if stop >= 100 / leverage:
        raise ShortLiveContractError("FUTURES_STOP_DISTANCE_EXCEEDS_MARGIN")
    for key, expected in constants.items():
        if key in value and (value[key] != expected or
                             (key == "closePosition" and value[key] is not True)):
            raise ShortLiveContractError("FUTURES_PROTECTION_POLICY_INVALID")
    return {"leverage": int(leverage), "marginMode": "ISOLATED",
            "stopLossPercent": stop, **constants}


def validate_source_pins(pins: list[dict[str, Any]]) -> None:
    if not isinstance(pins, list) or not pins:
        raise ShortLiveContractError("EXACT_SOURCE_PINS_REQUIRED")
    seen: dict[str, str] = {}
    for pin in pins:
        if not isinstance(pin, Mapping):
            raise ShortLiveContractError("SOURCE_PIN_INVALID")
        path = Path(text(pin.get("path"), "SOURCE_PATH"))
        digest = hash_text(pin.get("sha256"), "SOURCE_HASH")
        key = str(path.resolve())
        if key in seen and seen[key] != digest:
            raise ShortLiveContractError("CONFLICTING_SOURCE_PIN")
        seen[key] = digest
        if not path.is_file() or path.is_symlink():
            raise ShortLiveContractError("SOURCE_FILE_UNAVAILABLE")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ShortLiveContractError("SOURCE_RAW_HASH_CHANGED")


def source_position_size(parameters: Mapping[str, Any], plugin_id: str, artifact=None) -> tuple[float, float, bool]:
    """Preserve the original fixed capital percentage, independently of allocation.

    A portfolio weight chooses its sleeve; positionSize chooses how much of
    that sleeve an entry uses. Normalizing their product would erase 5% sizing.
    Unimplemented alternate custom sizing never silently becomes 100%.
    """
    if not isinstance(parameters, Mapping):
        raise ShortLiveContractError("EXACT_PARAMETERS_REQUIRED")
    sizing_keys = {"positionSizing", "fixedQuantity", "orderQuantity", "riskPerTrade", "riskPerTradePercent"}
    if any(key in parameters and parameters[key] is not None for key in sizing_keys):
        raise ShortLiveContractError("ORIGINAL_ALTERNATE_POSITION_SIZING_UNSUPPORTED")
    artifact = artifact if isinstance(artifact, Mapping) else {}
    if plugin_id == "strategy_builder_custom":
        definitions = [parameters.get("customStrategyDefinition"), parameters.get("custom_strategy_definition"),
                       artifact.get("customStrategyDefinition"), artifact.get("custom_strategy_definition"),
                       (artifact.get("settings") or {}).get("customStrategyDefinition")
                       if isinstance(artifact.get("settings"), Mapping) else None]
        for definition in definitions:
            if not isinstance(definition, Mapping):
                continue
            for source in (definition, definition.get("riskRules")):
                if isinstance(source, Mapping) and any(key in source and source[key] is not None
                                                      for key in sizing_keys | {"positionSize"}):
                    raise ShortLiveContractError("ORIGINAL_CUSTOM_POSITION_SIZING_UNSUPPORTED")
    defaulted = "positionSize" not in parameters
    percent = finite(parameters.get("positionSize", 100), "SOURCE_POSITION_SIZE_PERCENT", maximum=100)
    return percent, percent/100.0, defaulted


def normalize_spec(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ShortLiveContractError("RUNTIME_SPEC_OBJECT_REQUIRED")
    result = deepcopy(dict(value))
    names = ("portfolioId", "portfolioHash", "strategyInstanceId", "strategyId",
             "artifactHash", "pluginId", "instrumentId", "symbol", "timeframe",
             "provider", "brokerId", "route")
    for name in names:
        result[name] = text(result.get(name), name.upper())
    for name in ("portfolioHash", "artifactHash"):
        hash_text(result[name], name.upper())
    if result["route"] not in ROUTES or result["timeframe"] not in TIMEFRAMES:
        raise ShortLiveContractError("UNSUPPORTED_ROUTE_OR_TIMEFRAME")
    if result["pluginId"] not in {
        "moving_average_cross", "rsi_revert", "breakout",
        "threshold_momentum", "strategy_builder_custom",
    }:
        raise ShortLiveContractError("ORIGINAL_SHARED_EVALUATOR_PLUGIN_UNSUPPORTED")
    route_brokers = {
        "KIS_KR_CASH": {"kis"}, "BINANCE_SPOT": {"binance"},
        "BINANCE_USDM_FUTURES": {"binance", "binance-futures", "binance_futures", "binance_usdm", "binance-usdm"},
        "UPBIT_KRW_SPOT": {"upbit"},
    }
    if result["brokerId"].lower() not in route_brokers[result["route"]]:
        raise ShortLiveContractError("BROKER_ROUTE_BINDING_MISMATCH")
    if result["route"] == "KIS_KR_CASH" and not re.fullmatch(
            r"\d{6}(?:\.KS|\.KQ)?", result["symbol"].upper()):
        raise ShortLiveContractError("DOMESTIC_SYMBOL_REQUIRED")
    result["targetWeight"] = finite(result.get("targetWeight", 1), "TARGET_WEIGHT",
                                    positive=True, maximum=1)
    parameters = result.get("parameters", {})
    if not isinstance(parameters, Mapping):
        raise ShortLiveContractError("EXACT_PARAMETERS_REQUIRED")
    result["parameters"] = deepcopy(dict(parameters))
    artifact = result.get("artifact") or {}
    percent, fraction, defaulted = source_position_size(parameters, result["pluginId"], artifact)
    canonical_hash(result["parameters"])
    for key, expected in (("sourcePositionSizePercent", percent), ("positionSizeFraction", fraction)):
        if key in result and finite(result[key], key.upper()) != expected:
            raise ShortLiveContractError("SOURCE_POSITION_SIZE_DERIVED_VALUE_MISMATCH")
    result.update(sourcePositionSizePercent=percent, positionSizeFraction=fraction,
                  sourcePositionSizeWasDefaulted=defaulted)
    strategy_contract = artifact.get("strategyContract") or {}
    declared_timings = {str(value) for value in (
        result.get("executionTiming"), parameters.get("executionTiming"),
        artifact.get("executionTiming"), strategy_contract.get("executionTiming"),
    ) if value is not None and value != ""}
    if len(declared_timings) != 1 or next(iter(declared_timings), "") not in {"next-open", "signal-close"}:
        raise ShortLiveContractError("EXACT_EXECUTION_TIMING_REQUIRED")
    result["executionTiming"] = next(iter(declared_timings))
    direction = str(parameters.get("positionDirection") or
                    result.get("positionDirection") or "long").lower()
    definition = parameters.get("customStrategyDefinition")
    if isinstance(definition, Mapping):
        direction = str(definition.get("positionDirection", direction)).lower()
    if direction not in {"long", "short"}:
        raise ShortLiveContractError("POSITION_DIRECTION_INVALID")
    if direction == "short" and result["route"] != "BINANCE_USDM_FUTURES":
        raise ShortLiveContractError("SPOT_SHORT_FORBIDDEN")
    if direction == "short" and result["pluginId"] != "strategy_builder_custom":
        raise ShortLiveContractError("ORIGINAL_PLUGIN_HAS_NO_SIGNED_SHORT_EVALUATOR")
    result["positionDirection"] = direction
    return result


def build_contract(resolved: Mapping[str, Any], payload: Mapping[str, Any],
                   *, now: datetime) -> dict[str, Any]:
    """Freeze only server-resolved identities; client parameters never replace them."""
    duration = finite(payload.get("durationHours", 6), "DURATION_HOURS",
                      minimum=5, maximum=7)
    capital = finite(payload.get("capitalKrwPerBroker", 100_000), "CAPITAL_KRW",
                     positive=True, maximum=MAX_CAPITAL_KRW)
    loss = finite(payload.get("dailyLossStopKrwPerBroker", 50_000), "LOSS_STOP_KRW",
                  positive=True, maximum=MAX_DAILY_LOSS_KRW)
    if loss > capital:
        raise ShortLiveContractError("LOSS_STOP_EXCEEDS_CAPITAL")
    if not isinstance(resolved, Mapping):
        raise ShortLiveContractError("CATALOG_RESOLUTION_REQUIRED")
    targets = deepcopy(resolved.get("targets", []))
    if not isinstance(targets, list) or not targets:
        raise ShortLiveContractError("EXACT_TARGETS_REQUIRED")
    specs = [normalize_spec(item) for item in resolved.get("specs", [])]
    if not specs:
        raise ShortLiveContractError("EXACT_RUNTIME_SPECS_REQUIRED")
    ids = [item["strategyInstanceId"] for item in specs]
    if len(ids) != len(set(ids)):
        raise ShortLiveContractError("DUPLICATE_EXECUTION_INSTANCE")
    # Netting opposed futures sleeves would need an explicit hedge ledger.  Do
    # not invent virtual positions and present those as actual broker holdings.
    directions: dict[tuple[str, str], set[str]] = {}
    timings: dict[tuple[str, str], set[str]] = {}
    for spec in specs:
        directions.setdefault((spec["route"], spec["symbol"]), set()).add(
            spec["positionDirection"])
        timings.setdefault((spec["route"], spec["symbol"]), set()).add(spec["executionTiming"])
    if any(len(values) != 1 for values in directions.values()):
        raise ShortLiveContractError("OPPOSED_SAME_SYMBOL_SLEEVES_REQUIRE_HEDGE_LEDGER")
    if any(len(values) != 1 for values in timings.values()):
        raise ShortLiveContractError("MIXED_SAME_SYMBOL_EXECUTION_TIMING_UNSUPPORTED")
    pins = deepcopy(resolved.get("sourcePins", []))
    validate_source_pins(pins)
    routes = sorted({spec["route"] for spec in specs})
    futures_policy = (normalize_futures_policy(payload.get("futuresPolicy"))
                      if "BINANCE_USDM_FUTURES" in routes else None)
    execution_binding = model_binding(specs, futures_policy["leverage"]) if futures_policy else None
    broker_counts = {broker: sum(ROUTES[route] == broker for route in routes)
                     for broker in set(ROUTES[route] for route in routes)}
    session_id = f"short-live-{uuid4()}"
    legs = []
    for route in routes:
        leg_specs = [s for s in specs if s["route"] == route]
        legs.append({
            "route": route, "brokerGroup": ROUTES[route],
            "brokerBudgetKrw": capital,
            "routeBudgetKrw": capital / broker_counts[ROUTES[route]],
            "dailyLossStopKrw": loss, "durationHours": duration,
            "sessionId": session_id,
            "accountId": str((resolved.get("accounts") or {}).get(route, "")),
            "symbols": sorted({s["symbol"] for s in leg_specs}),
            "specs": leg_specs,
            "ownershipScope": {"sessionId": session_id,
                               "instanceIds": [s["strategyInstanceId"] for s in leg_specs]},
            "sameSymbolCrossSleeveTransferPolicy": "EXPLICIT_INTERNAL_OWNERSHIP_TRANSFER_THEN_EXTERNAL_NET_RESIDUAL",
        })
        if route == "BINANCE_USDM_FUTURES":
            legs[-1]["futuresPolicy"] = deepcopy(futures_policy)
            legs[-1]["executionModelBinding"] = deepcopy(execution_binding)
            selection_keys={spec.get("selectionKey") for spec in leg_specs}
            legs[-1]["marginBudgetAllocationDenominator"]=(len(selection_keys) if None not in selection_keys
                else max(1.0,sum(spec["targetWeight"] for spec in leg_specs)))
    body = {
        "schemaVersion": SCHEMA, "purpose": PURPOSE,
        "promotionEligible": False, "useAsPromotionEvidence": False,
        "standardEvidenceModified": False, "preparationId": f"prep-{uuid4()}",
        "sessionId": session_id, "preparedAt": utc_text(now),
        "durationHours": duration, "durationSeconds": int(duration * 3600),
        "capitalKrwPerBroker": capital, "dailyLossStopKrwPerBroker": loss,
        "binanceSpotAndFuturesShareBudget": True,
        "capitalBudgetBasis": "CASH_AND_FUTURES_MARGIN_WITH_COST_RESERVES",
        "futuresPolicy": deepcopy(futures_policy),
        "executionModelBinding": deepcopy(execution_binding),
        "targets": targets, "specs": specs, "sourcePins": pins,
        "legs": legs, "automaticRestartAllowed": False,
        "realOrderStartRequiresOperatorConfirmation": True,
    }
    digest = canonical_hash(body)
    return {**body, "scopeHash": digest, "manifestHash": digest}


def verify_contract(value: Mapping[str, Any], *, check_sources: bool = True) -> None:
    body = deepcopy(dict(value))
    declared = body.pop("scopeHash", None)
    alias = body.pop("manifestHash", None)
    if (body.get("schemaVersion") != SCHEMA or body.get("purpose") != PURPOSE
            or body.get("promotionEligible") is not False
            or body.get("useAsPromotionEvidence") is not False
            or body.get("automaticRestartAllowed") is not False
            or alias != declared or canonical_hash(body) != declared):
        raise ShortLiveContractError("IMMUTABLE_FUNCTIONAL_CONTRACT_MISMATCH")
    futures_legs = [leg for leg in body.get("legs", [])
                    if leg.get("route") == "BINANCE_USDM_FUTURES"]
    if futures_legs:
        policy = normalize_futures_policy(body.get("futuresPolicy"))
        if body.get("futuresPolicy") != policy or any(
                leg.get("futuresPolicy") != policy for leg in futures_legs):
            raise ShortLiveContractError("IMMUTABLE_FUTURES_POLICY_MISMATCH")
    elif body.get("futuresPolicy") is not None:
        raise ShortLiveContractError("IMMUTABLE_FUTURES_POLICY_MISMATCH")
    if check_sources:
        validate_source_pins(body["sourcePins"])


def leg_manifest(contract: Mapping[str, Any], route: str) -> dict[str, Any]:
    verify_contract(contract, check_sources=False)
    leg = next(item for item in contract["legs"] if item["route"] == route)
    result = deepcopy(leg)
    result.update({"manifestHash": contract["manifestHash"],
                   "scopeHash": contract["scopeHash"], "purpose": PURPOSE,
                   "promotionEligible": False})
    result["ownershipScope"]["manifestHash"] = contract["manifestHash"]
    return result
