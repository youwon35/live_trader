from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


MAX_FINITE_FLOAT = float.fromhex("0x1.fffffffffffffp+1023")


def _number(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        parsed = float(default)
    if math.isfinite(parsed):
        return parsed
    fallback = float(default)
    return fallback if math.isfinite(fallback) else 0.0


def _finite_number(value: object) -> tuple[float, bool]:
    try:
        parsed = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0, False
    return (parsed, True) if math.isfinite(parsed) else (0.0, False)


def _bounded(value: object, *, minimum: float, maximum: float, default: float) -> float:
    return min(max(_number(value, default), minimum), maximum)


def _first_present(payload: Mapping[str, Any], aliases: tuple[str, ...]) -> object:
    """Return the first explicitly supplied value, preserving numeric zero."""

    for alias in aliases:
        if alias in payload and payload.get(alias) not in (None, ""):
            return payload.get(alias)
    return None


def _non_finite_fields(
    payload: Mapping[str, Any],
    fields: Mapping[str, tuple[str, ...]],
) -> list[str]:
    """Return numeric fields explicitly supplied as NaN or infinity.

    Normalization maps non-finite values to conservative defaults so no NaN
    can leak into JSON or arithmetic.  The explicit field list remains a
    fail-closed audit trail instead of silently accepting that fallback.
    """

    invalid: list[str] = []
    for canonical, aliases in fields.items():
        for alias in aliases:
            if alias not in payload:
                continue
            value = payload.get(alias)
            if value in (None, ""):
                continue
            try:
                parsed = float(str(value).replace(",", "").strip())
            except (TypeError, ValueError):
                continue
            if not math.isfinite(parsed):
                invalid.append(canonical)
                break
    return invalid


def _non_numeric_fields(
    payload: Mapping[str, Any],
    fields: Mapping[str, tuple[str, ...]],
) -> list[str]:
    """Return explicitly supplied, non-empty values that cannot be parsed."""

    invalid: list[str] = []
    for canonical, aliases in fields.items():
        for alias in aliases:
            if alias not in payload:
                continue
            value = payload.get(alias)
            if value in (None, ""):
                continue
            try:
                float(str(value).replace(",", "").strip())
            except (TypeError, ValueError):
                invalid.append(canonical)
                break
    return invalid


@dataclass(frozen=True)
class FuturesRiskInputs:
    symbol: str
    direction: str
    margin_type: str
    account_equity_usdt: float
    available_usdt: float
    entry_price: float
    notional_usdt: float
    leverage: float
    maintenance_margin_rate: float
    taker_fee_rate: float
    funding_rate: float
    funding_intervals: int
    stop_price: float


def normalize_futures_risk_inputs(payload: Mapping[str, Any]) -> FuturesRiskInputs:
    symbol = str(payload.get("symbol") or "").strip().upper().replace("-", "")
    raw_direction = _first_present(payload, ("direction",))
    raw_margin_type = _first_present(
        payload,
        ("margin_type", "marginType"),
    )
    direction = str(
        "LONG" if raw_direction is None else raw_direction
    ).strip().upper()
    margin_type = str(
        "ISOLATED" if raw_margin_type is None else raw_margin_type
    ).strip().upper()
    return FuturesRiskInputs(
        symbol=symbol,
        direction=direction if direction in {"LONG", "SHORT"} else "LONG",
        margin_type=margin_type if margin_type in {"ISOLATED", "CROSSED"} else "ISOLATED",
        account_equity_usdt=max(
            0.0,
            _number(
                _first_present(
                    payload,
                    ("account_equity_usdt", "accountEquityUsdt"),
                )
            ),
        ),
        available_usdt=max(
            0.0,
            _number(
                _first_present(payload, ("available_usdt", "availableUsdt"))
            ),
        ),
        entry_price=max(
            0.0,
            _number(_first_present(payload, ("entry_price", "entryPrice"))),
        ),
        notional_usdt=max(
            0.0,
            _number(
                _first_present(payload, ("notional_usdt", "notionalUsdt"))
            ),
        ),
        leverage=_bounded(payload.get("leverage"), minimum=1.0, maximum=125.0, default=1.0),
        maintenance_margin_rate=_bounded(
            _first_present(
                payload,
                ("maintenance_margin_rate", "maintenanceMarginRate"),
            ),
            minimum=0.0,
            maximum=0.5,
            default=0.005,
        ),
        taker_fee_rate=_bounded(
            _first_present(payload, ("taker_fee_rate", "takerFeeRate")),
            minimum=0.0,
            maximum=0.1,
            default=0.0005,
        ),
        funding_rate=_bounded(
            _first_present(payload, ("funding_rate", "fundingRate")),
            minimum=-0.1,
            maximum=0.1,
            default=0.0001,
        ),
        funding_intervals=max(
            0,
            min(
                90,
                int(
                    _number(
                        _first_present(
                            payload,
                            ("funding_intervals", "fundingIntervals"),
                        ),
                        1,
                    )
                ),
            ),
        ),
        stop_price=max(
            0.0,
            _number(_first_present(payload, ("stop_price", "stopPrice"))),
        ),
    )


def simulate_futures_order_risk(
    payload: Mapping[str, Any],
    *,
    policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a conservative, read-only USD-M order risk estimate.

    The liquidation estimate intentionally uses a simplified isolated-margin
    model. Binance's actual liquidation price also depends on the current
    maintenance bracket, accumulated funding/fees and other account state.
    The result must therefore be treated as a preflight guard, never as the
    broker's authoritative liquidation price.
    """

    contract = dict(policy or {})
    numeric_input_fields = {
        "account_equity_usdt": (
            "account_equity_usdt",
            "accountEquityUsdt",
        ),
        "available_usdt": ("available_usdt", "availableUsdt"),
        "entry_price": ("entry_price", "entryPrice"),
        "notional_usdt": ("notional_usdt", "notionalUsdt"),
        "leverage": ("leverage",),
        "maintenance_margin_rate": (
            "maintenance_margin_rate",
            "maintenanceMarginRate",
        ),
        "taker_fee_rate": ("taker_fee_rate", "takerFeeRate"),
        "funding_rate": ("funding_rate", "fundingRate"),
        "funding_intervals": (
            "funding_intervals",
            "fundingIntervals",
        ),
        "stop_price": ("stop_price", "stopPrice"),
    }
    numeric_policy_fields = {
        "max_leverage": (
            "max_leverage",
            "maxLeverage",
            "maxLeverageMultiplier",
        ),
        "max_notional_pct": (
            "max_notional_pct",
            "maxNotionalPct",
            "maxNotionalPercent",
        ),
        "per_trade_risk_pct": (
            "per_trade_risk_pct",
            "perTradeRiskPct",
            "perTradeRiskPercent",
        ),
    }
    non_finite_inputs = _non_finite_fields(
        payload,
        numeric_input_fields,
    )
    non_finite_policy = _non_finite_fields(
        contract,
        numeric_policy_fields,
    )
    non_numeric_inputs = _non_numeric_fields(
        payload,
        numeric_input_fields,
    )
    non_numeric_policy = _non_numeric_fields(
        contract,
        numeric_policy_fields,
    )
    inputs = normalize_futures_risk_inputs(payload)
    raw_direction_value = _first_present(payload, ("direction",))
    raw_direction = str(
        "LONG" if raw_direction_value is None else raw_direction_value
    ).strip().upper()
    raw_margin_type_value = _first_present(
        payload,
        ("margin_type", "marginType"),
    )
    raw_margin_type = str(
        "ISOLATED"
        if raw_margin_type_value is None
        else raw_margin_type_value
    ).strip().upper()
    allowed_margin_type_value = _first_present(
        contract,
        (
            "allowed_margin_type",
            "allowedMarginType",
            "marginMode",
        ),
    )
    allowed_margin_type = str(
        "ISOLATED"
        if allowed_margin_type_value is None
        else allowed_margin_type_value
    ).strip().upper()
    raw_leverage_value = _first_present(payload, ("leverage",))
    parsed_leverage, parsed_leverage_valid = _finite_number(
        raw_leverage_value
    )
    raw_max_leverage_value = _first_present(
        contract,
        (
            "max_leverage",
            "maxLeverage",
            "maxLeverageMultiplier",
        ),
    )
    parsed_max_leverage, parsed_max_leverage_valid = _finite_number(
        raw_max_leverage_value
    )
    max_leverage = max(
        1.0,
        _number(
            raw_max_leverage_value,
            1.0,
        ),
    )
    max_notional_pct = _bounded(
        _first_present(
            contract,
            (
                "max_notional_pct",
                "maxNotionalPct",
                "maxNotionalPercent",
            ),
        ),
        minimum=0.0,
        maximum=100.0,
        default=10.0,
    )
    max_risk_pct = _bounded(
        _first_present(
            contract,
            (
                "per_trade_risk_pct",
                "perTradeRiskPct",
                "perTradeRiskPercent",
            ),
        ),
        minimum=0.0,
        maximum=100.0,
        default=1.0,
    )

    arithmetic_non_finite: list[str] = []

    def finite_arithmetic(
        field: str,
        value: float,
        *,
        fallback: float,
    ) -> float:
        if math.isfinite(value):
            return value
        arithmetic_non_finite.append(field)
        return fallback

    quantity = finite_arithmetic(
        "quantity",
        (
            inputs.notional_usdt / inputs.entry_price
            if inputs.entry_price > 0
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )
    initial_margin = finite_arithmetic(
        "initial_margin_usdt",
        (
            inputs.notional_usdt / inputs.leverage
            if inputs.leverage > 0
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )
    maintenance_margin = finite_arithmetic(
        "maintenance_margin_usdt",
        inputs.notional_usdt * inputs.maintenance_margin_rate,
        fallback=MAX_FINITE_FLOAT,
    )
    entry_fee = finite_arithmetic(
        "entry_fee_usdt",
        inputs.notional_usdt * inputs.taker_fee_rate,
        fallback=MAX_FINITE_FLOAT,
    )
    exit_fee = entry_fee
    round_trip_fee = finite_arithmetic(
        "round_trip_fee_usdt",
        entry_fee + exit_fee,
        fallback=MAX_FINITE_FLOAT,
    )
    signed_funding_cashflow = finite_arithmetic(
        "signed_funding_cashflow_usdt",
        (
            inputs.notional_usdt
            * inputs.funding_rate
            * inputs.funding_intervals
            * (-1.0 if inputs.direction == "LONG" else 1.0)
        ),
        # Never turn overflow into unbounded expected income.
        fallback=-MAX_FINITE_FLOAT,
    )
    estimated_funding_cost = finite_arithmetic(
        "estimated_funding_cost_usdt",
        max(0.0, -signed_funding_cashflow),
        fallback=MAX_FINITE_FLOAT,
    )

    loss_budget_to_liquidation = max(
        0.0,
        finite_arithmetic(
            "loss_budget_to_liquidation_usdt",
            initial_margin - maintenance_margin - exit_fee,
            fallback=0.0,
        ),
    )
    liquidation_move_fraction = finite_arithmetic(
        "liquidation_move_fraction",
        (
            loss_budget_to_liquidation / inputs.notional_usdt
            if inputs.notional_usdt > 0
            else 0.0
        ),
        fallback=0.0,
    )
    if inputs.margin_type != "ISOLATED" or inputs.entry_price <= 0:
        liquidation_price: float | None = None
        liquidation_buffer_pct: float | None = None
    elif inputs.direction == "LONG":
        liquidation_price = max(
            0.0,
            finite_arithmetic(
                "liquidation_price",
                inputs.entry_price * (1.0 - liquidation_move_fraction),
                fallback=inputs.entry_price,
            ),
        )
        liquidation_buffer_pct = finite_arithmetic(
            "liquidation_buffer_pct",
            (
                (inputs.entry_price - liquidation_price)
                / inputs.entry_price
                * 100.0
            ),
            fallback=0.0,
        )
    else:
        liquidation_price = finite_arithmetic(
            "liquidation_price",
            inputs.entry_price * (1.0 + liquidation_move_fraction),
            fallback=inputs.entry_price,
        )
        liquidation_buffer_pct = finite_arithmetic(
            "liquidation_buffer_pct",
            (
                (liquidation_price - inputs.entry_price)
                / inputs.entry_price
                * 100.0
            ),
            fallback=0.0,
        )

    adverse_stop_loss = 0.0
    stop_is_protective = False
    stop_precedes_liquidation: bool | None = None
    if inputs.stop_price > 0 and quantity > 0:
        stop_is_protective = (
            inputs.stop_price < inputs.entry_price
            if inputs.direction == "LONG"
            else inputs.stop_price > inputs.entry_price
        )
        if stop_is_protective:
            adverse_stop_loss = finite_arithmetic(
                "adverse_stop_loss_usdt",
                abs(inputs.entry_price - inputs.stop_price) * quantity,
                fallback=MAX_FINITE_FLOAT,
            )
            if liquidation_price is not None:
                stop_precedes_liquidation = (
                    inputs.stop_price > liquidation_price
                    if inputs.direction == "LONG"
                    else inputs.stop_price < liquidation_price
                )
    estimated_loss_at_stop = finite_arithmetic(
        "estimated_loss_at_stop_usdt",
        (
            adverse_stop_loss
            + round_trip_fee
            + estimated_funding_cost
            if stop_is_protective
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )
    risk_pct_of_equity = finite_arithmetic(
        "risk_pct_of_equity",
        (
            estimated_loss_at_stop / inputs.account_equity_usdt * 100.0
            if inputs.account_equity_usdt > 0
            and estimated_loss_at_stop > 0
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )
    notional_pct_of_equity = finite_arithmetic(
        "notional_pct_of_equity",
        (
            inputs.notional_usdt / inputs.account_equity_usdt * 100.0
            if inputs.account_equity_usdt > 0
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )
    required_cash = finite_arithmetic(
        "required_cash_usdt",
        initial_margin + entry_fee,
        fallback=MAX_FINITE_FLOAT,
    )
    margin_utilization_pct = finite_arithmetic(
        "margin_utilization_pct",
        (
            required_cash / inputs.available_usdt * 100.0
            if inputs.available_usdt > 0
            else 0.0
        ),
        fallback=MAX_FINITE_FLOAT,
    )

    blockers: list[str] = [
        *[
            f"numeric-input-non-finite:{field}"
            for field in non_finite_inputs
        ],
        *[
            f"numeric-policy-non-finite:{field}"
            for field in non_finite_policy
        ],
        *[
            f"numeric-input-invalid:{field}"
            for field in non_numeric_inputs
        ],
        *[
            f"numeric-policy-invalid:{field}"
            for field in non_numeric_policy
        ],
        *[
            f"risk-arithmetic-non-finite:{field}"
            for field in dict.fromkeys(arithmetic_non_finite)
        ],
    ]
    warnings: list[str] = []
    if "valid" in contract and contract.get("valid") is not True:
        raw_inherited = contract.get("blockers")
        inherited = [
            str(item)
            for item in (
                raw_inherited
                if isinstance(raw_inherited, (list, tuple))
                else []
            )
            if str(item)
        ]
        blockers.extend(inherited or ["futures-policy-invalid"])
    if raw_direction not in {"LONG", "SHORT"}:
        blockers.append("direction-invalid")
    if raw_margin_type not in {"ISOLATED", "CROSSED"}:
        blockers.append("margin-type-invalid")
    if not inputs.symbol:
        blockers.append("symbol-missing")
    if inputs.entry_price <= 0:
        blockers.append("entry-price-invalid")
    if inputs.notional_usdt <= 0:
        blockers.append("notional-invalid")
    if inputs.account_equity_usdt <= 0:
        blockers.append("account-equity-invalid")
    if inputs.available_usdt <= 0:
        blockers.append("available-usdt-invalid")
    if raw_leverage_value is not None and (
        not parsed_leverage_valid or parsed_leverage <= 0
    ):
        blockers.append("leverage-invalid")
    if raw_max_leverage_value is not None and (
        not parsed_max_leverage_valid or parsed_max_leverage <= 0
    ):
        blockers.append("max-leverage-policy-invalid")
    if required_cash > inputs.available_usdt + 1e-9:
        blockers.append("required-margin-exceeds-available")
    if allowed_margin_type not in {"ANY", inputs.margin_type}:
        blockers.append("margin-type-policy-drift")
    if inputs.leverage > max_leverage + 1e-9:
        blockers.append("leverage-policy-drift")
    if notional_pct_of_equity > max_notional_pct + 1e-9:
        blockers.append("max-notional-policy-exceeded")
    if inputs.stop_price <= 0:
        blockers.append("protective-stop-missing")
    elif not stop_is_protective:
        blockers.append("protective-stop-direction-invalid")
    elif stop_precedes_liquidation is False:
        blockers.append("protective-stop-at-or-beyond-liquidation")
    if stop_is_protective and risk_pct_of_equity > max_risk_pct + 1e-9:
        blockers.append("per-trade-risk-policy-exceeded")
    if inputs.margin_type == "CROSSED":
        warnings.append("cross-margin-liquidation-account-dependent")
    if liquidation_buffer_pct is not None and liquidation_buffer_pct < 5.0:
        warnings.append("liquidation-buffer-below-5pct")
    if inputs.funding_intervals == 0:
        warnings.append("funding-not-included")

    return {
        "schema_version": 1,
        "status": "BLOCKED" if blockers else ("WARNING" if warnings else "READY"),
        "inputs": {
            "symbol": inputs.symbol,
            "direction": inputs.direction,
            "margin_type": inputs.margin_type,
            "account_equity_usdt": inputs.account_equity_usdt,
            "available_usdt": inputs.available_usdt,
            "entry_price": inputs.entry_price,
            "notional_usdt": inputs.notional_usdt,
            "quantity": quantity,
            "leverage": inputs.leverage,
            "maintenance_margin_rate": inputs.maintenance_margin_rate,
            "taker_fee_rate": inputs.taker_fee_rate,
            "funding_rate": inputs.funding_rate,
            "funding_intervals": inputs.funding_intervals,
            "stop_price": inputs.stop_price,
        },
        "policy": {
            "allowed_margin_type": allowed_margin_type,
            "max_leverage": max_leverage,
            "per_trade_risk_pct": max_risk_pct,
            "max_notional_pct": max_notional_pct,
        },
        "estimate": {
            "initial_margin_usdt": initial_margin,
            "maintenance_margin_usdt": maintenance_margin,
            "round_trip_fee_usdt": round_trip_fee,
            "signed_funding_cashflow_usdt": signed_funding_cashflow,
            "estimated_funding_cost_usdt": estimated_funding_cost,
            "liquidation_price": liquidation_price,
            "liquidation_buffer_pct": liquidation_buffer_pct,
            "protective_stop_precedes_liquidation": (
                stop_precedes_liquidation
            ),
            "estimated_loss_at_stop_usdt": estimated_loss_at_stop,
            "risk_pct_of_equity": risk_pct_of_equity,
            "notional_pct_of_equity": notional_pct_of_equity,
            "required_cash_usdt": required_cash,
            "margin_utilization_pct": margin_utilization_pct,
        },
        "blockers": blockers,
        "warnings": warnings,
        "disclaimer": (
            "청산가는 격리 증거금의 단순 추정치입니다. 실제 값은 Binance의 "
            "maintenance bracket, mark price, 수수료·펀딩 및 계좌 상태를 따릅니다."
        ),
    }
