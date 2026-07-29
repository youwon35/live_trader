from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any


SCHEMA_VERSION = "binance-usdm-futures-canary-v1"
HARD_MIN_NOTIONAL_USDT = Decimal("5")
HARD_MAX_NOTIONAL_USDT = Decimal("10")


def _decimal(value: object) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return number if number.is_finite() else None


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def normalize_usdm_symbol(value: object) -> str:
    return (
        str(value or "")
        .strip()
        .upper()
        .removesuffix(".PERP")
        .replace("-", "")
    )


def derive_canary_quantity(
    *,
    target_notional_usdt: object,
    price: object,
    min_qty: object,
    max_qty: object,
    step_size: object,
    exchange_min_notional: object,
) -> dict[str, object]:
    target = _decimal(target_notional_usdt)
    mark = _decimal(price)
    minimum_quantity = _decimal(min_qty)
    maximum_quantity = _decimal(max_qty)
    step = _decimal(step_size)
    exchange_minimum = _decimal(exchange_min_notional)
    blockers: list[str] = []
    if target is None or target <= 0:
        blockers.append("target-notional-invalid")
    elif target < HARD_MIN_NOTIONAL_USDT:
        blockers.append("notional-below-hard-min")
    elif target > HARD_MAX_NOTIONAL_USDT:
        blockers.append("notional-above-hard-max")
    if mark is None or mark <= 0:
        blockers.append("ticker-observation-failed")
    if step is None or step <= 0:
        blockers.append("quantity-step-invalid")
    if exchange_minimum is None or exchange_minimum < 0:
        blockers.append("exchange-rules-observation-failed")
    elif exchange_minimum > HARD_MAX_NOTIONAL_USDT:
        blockers.append("exchange-min-notional-above-hard-max")
    if blockers:
        return {
            "ok": False,
            "quantity": "",
            "estimated_notional_usdt": "",
            "blockers": list(dict.fromkeys(blockers)),
        }

    assert target is not None and mark is not None
    assert step is not None and exchange_minimum is not None
    requested = max(target, exchange_minimum)
    raw_quantity = requested / mark
    quantity = (raw_quantity / step).to_integral_value(
        rounding=ROUND_CEILING
    ) * step
    estimated = quantity * mark
    if minimum_quantity is None or minimum_quantity < 0:
        blockers.append("exchange-rules-observation-failed")
    elif quantity < minimum_quantity:
        blockers.append("quantity-below-min")
    if maximum_quantity is None or maximum_quantity <= 0:
        blockers.append("exchange-rules-observation-failed")
    elif quantity > maximum_quantity:
        blockers.append("quantity-above-max")
    if estimated < HARD_MIN_NOTIONAL_USDT:
        blockers.append("estimated-notional-below-hard-min")
    if estimated > HARD_MAX_NOTIONAL_USDT:
        blockers.append("estimated-notional-above-hard-max")
    return {
        "ok": not blockers,
        "quantity": _decimal_text(quantity),
        "estimated_notional_usdt": _decimal_text(estimated),
        "blockers": list(dict.fromkeys(blockers)),
    }


def build_futures_canary_test_intents(
    *,
    strategy_id: str,
    symbol: str,
    quantity: str,
    token_fingerprint: str,
) -> tuple[dict[str, object], dict[str, object]]:
    normalized_symbol = normalize_usdm_symbol(symbol)
    base = {
        "broker_id": "binance-futures",
        "strategy_id": str(strategy_id),
        "symbol": normalized_symbol,
        "quantity": str(quantity),
        "qty": str(quantity),
        "order_type": "MARKET",
        "position_direction": "short",
        "market_type": "futures",
        "max_leverage": 1,
        "required_margin_type": "ISOLATED",
    }
    fingerprint = "".join(
        character
        for character in str(token_fingerprint).lower()
        if character in "0123456789abcdef"
    )[:12].ljust(12, "0")
    return (
        {
            **base,
            "side": "SELL",
            "risk_reducing": False,
            "reduce_only": False,
            "identifier": f"lt-fc-{fingerprint}-e",
        },
        {
            **base,
            "side": "BUY",
            "risk_reducing": True,
            "reduce_only": True,
            "identifier": f"lt-fc-{fingerprint}-c",
        },
    )


def evaluate_futures_canary_preflight(
    *,
    strategy_gate: dict[str, object],
    account: dict[str, object],
    position_mode: dict[str, object],
    symbol_config: dict[str, object],
    position_count: int | None,
    open_order_count: int | None,
    requested_notional_usdt: object,
    quantity_result: dict[str, object],
    real_orders_enabled: bool,
    observation_errors: tuple[str, ...] = (),
) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    def check(
        check_id: str,
        passed: bool,
        reason_id: str,
        *,
        actual: object,
        scope: str = "test",
    ) -> None:
        checks.append(
            {
                "id": check_id,
                "status": "pass" if passed else "fail",
                "scope": scope,
                "required": True,
                "actual": actual,
                "reason_id": "" if passed else reason_id,
            }
        )

    check(
        "strategy-artifact",
        strategy_gate.get("found") is True,
        "strategy-artifact-missing",
        actual=strategy_gate.get("found"),
    )
    check(
        "strategy-broker",
        str(strategy_gate.get("broker_id") or "") == "binance-futures",
        "strategy-broker-mismatch",
        actual=strategy_gate.get("broker_id"),
    )
    check(
        "strategy-symbol",
        strategy_gate.get("symbol_matches") is True,
        "strategy-symbol-mismatch",
        actual=strategy_gate.get("symbol"),
    )
    check(
        "strategy-market",
        str(strategy_gate.get("market_type") or "").lower()
        in {"future", "futures", "perpetual", "swap"},
        "strategy-market-not-futures",
        actual=strategy_gate.get("market_type"),
    )
    check(
        "strategy-short",
        strategy_gate.get("short_authorized") is True,
        "strategy-short-not-authorized",
        actual=strategy_gate.get("position_direction"),
    )
    check(
        "strategy-lifecycle",
        str(strategy_gate.get("lifecycle_status") or "")
        == "before-live-small",
        "strategy-lifecycle-not-before-live-small",
        actual=strategy_gate.get("lifecycle_status"),
    )
    check(
        "strategy-live-small",
        strategy_gate.get("live_small_eligible") is True,
        "strategy-live-small-ineligible",
        actual=strategy_gate.get("live_small_eligible"),
    )
    check(
        "strategy-deployment-provenance",
        strategy_gate.get("deployment_provenance_ok") is True,
        "strategy-deployment-provenance-missing",
        actual=strategy_gate.get("deployment_provenance_ok"),
    )
    check(
        "strategy-canary-scope",
        strategy_gate.get("canary_scope_ok") is True,
        "strategy-canary-scope-invalid",
        actual=strategy_gate.get("canary_scope_ok"),
    )

    can_trade = account.get("can_trade")
    check(
        "account-can-trade-known",
        isinstance(can_trade, bool),
        "account-can-trade-unknown",
        actual=can_trade,
    )
    if isinstance(can_trade, bool):
        check(
            "account-can-trade",
            can_trade,
            "account-can-trade-false",
            actual=can_trade,
        )
    available = _decimal(account.get("available_usdt"))
    available_known = (
        account.get("available_usdt_known") is True
        and available is not None
    )
    check(
        "available-usdt-known",
        available_known,
        "available-usdt-unknown",
        actual=account.get("available_usdt"),
    )
    required_notional = _decimal(
        quantity_result.get("estimated_notional_usdt")
    )
    if available_known and required_notional is not None:
        check(
            "available-usdt",
            available >= required_notional,
            "available-usdt-insufficient",
            actual=_decimal_text(available),
        )

    dual_side = position_mode.get("dual_side_position")
    check(
        "position-mode-known",
        isinstance(dual_side, bool),
        "position-mode-unknown",
        actual=dual_side,
    )
    if isinstance(dual_side, bool):
        check(
            "position-mode-hedge",
            dual_side,
            "position-mode-not-hedge",
            actual="HEDGE" if dual_side else "ONE_WAY",
        )

    margin_type = str(symbol_config.get("margin_type") or "").upper()
    leverage = _decimal(symbol_config.get("leverage"))
    check(
        "symbol-config",
        bool(normalize_usdm_symbol(symbol_config.get("symbol"))),
        "symbol-config-missing",
        actual=symbol_config.get("symbol"),
    )
    check(
        "margin-type-known",
        bool(margin_type),
        "margin-type-unknown",
        actual=margin_type,
    )
    if margin_type:
        check(
            "margin-type-isolated",
            margin_type == "ISOLATED",
            "margin-type-not-isolated",
            actual=margin_type,
        )
    check(
        "leverage-known",
        leverage is not None and leverage > 0,
        "leverage-unknown",
        actual=symbol_config.get("leverage"),
    )
    if leverage is not None and leverage > 0:
        check(
            "leverage-1x",
            leverage == Decimal("1"),
            "leverage-not-1x",
            actual=_decimal_text(leverage),
        )

    check(
        "positions-observed",
        position_count is not None,
        "positions-observation-failed",
        actual=position_count,
    )
    if position_count is not None:
        check(
            "positions-flat",
            position_count == 0,
            "existing-futures-position",
            actual=position_count,
        )
    check(
        "open-orders-observed",
        open_order_count is not None,
        "open-orders-observation-failed",
        actual=open_order_count,
    )
    if open_order_count is not None:
        check(
            "open-orders-clear",
            open_order_count == 0,
            "existing-futures-open-order",
            actual=open_order_count,
        )
    for reason in quantity_result.get("blockers") or []:
        check(
            f"quantity:{reason}",
            False,
            str(reason),
            actual=requested_notional_usdt,
        )
    for reason in observation_errors:
        check(
            f"observation:{reason}",
            False,
            str(reason),
            actual="failed",
        )

    check(
        "real-orders-enabled",
        bool(real_orders_enabled),
        "real-orders-disabled",
        actual=bool(real_orders_enabled),
        scope="start",
    )
    check(
        "live-start-implementation",
        False,
        "live-start-not-implemented",
        actual=False,
        scope="start",
    )
    test_blockers = [
        str(item["reason_id"])
        for item in checks
        if item["status"] == "fail" and item["scope"] == "test"
    ]
    start_blockers = [
        *test_blockers,
        *[
            str(item["reason_id"])
            for item in checks
            if item["status"] == "fail" and item["scope"] == "start"
        ],
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "evaluated": True,
        "status": "test_ready" if not test_blockers else "blocked",
        "ready_for_test": not test_blockers,
        "preflight_ready_for_live_start": False,
        "live_start_available": False,
        "test_blockers": list(dict.fromkeys(test_blockers)),
        "start_blockers": list(dict.fromkeys(start_blockers)),
        "checks": checks,
        "account": {
            "can_trade": can_trade if isinstance(can_trade, bool) else None,
            "available_usdt": (
                _decimal_text(available)
                if available_known and available is not None
                else None
            ),
        },
        "position_mode": {
            "dual_side_position": (
                dual_side if isinstance(dual_side, bool) else None
            ),
            "label": (
                "HEDGE"
                if dual_side is True
                else "ONE_WAY"
                if dual_side is False
                else "UNKNOWN"
            ),
        },
        "symbol_config": {
            "symbol": normalize_usdm_symbol(symbol_config.get("symbol")),
            "margin_type": margin_type,
            "leverage": (
                _decimal_text(leverage)
                if leverage is not None and leverage > 0
                else None
            ),
        },
        "positions": {
            "observed": position_count is not None,
            "flat": position_count == 0 if position_count is not None else False,
            "count": position_count,
        },
        "open_orders": {
            "observed": open_order_count is not None,
            "clear": (
                open_order_count == 0
                if open_order_count is not None
                else False
            ),
            "count": open_order_count,
        },
        "order_plan": {
            "requested_notional_usdt": str(requested_notional_usdt),
            "estimated_notional_usdt": quantity_result.get(
                "estimated_notional_usdt"
            ),
            "quantity": quantity_result.get("quantity"),
            "round_trips": 3,
            "sequential": True,
        },
    }
