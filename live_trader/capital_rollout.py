from __future__ import annotations

import math
from typing import Any, Mapping


CANARY_MAX_NOTIONAL_USDT = 10.0
SMALL_LIVE_MAX_NOTIONAL_USDT = 25.0
MINIMUM_CANARY_FILLS = 3
MINIMUM_FULL_LIVE_FILLS = 20
MINIMUM_FULL_LIVE_OBSERVATION_HOURS = 168.0
ROLLOUT_SCHEMA_VERSION = "capital-rollout-v1"
ROLLOUT_STAGE_IDS = ("CANARY", "SMALL_LIVE", "FULL_LIVE")


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


def _valid_notional_percent(value: object) -> tuple[float, bool]:
    parsed, valid = _finite_number(value)
    if not valid or parsed < 0.0 or parsed > 100.0:
        return 0.0, False
    return parsed, True


def _valid_nonnegative_integer(value: object) -> tuple[int, bool]:
    parsed, valid = _finite_number(value)
    if not valid or parsed < 0.0 or not parsed.is_integer():
        return 0, False
    return int(parsed), True


def _valid_nonnegative_number(value: object) -> tuple[float, bool]:
    parsed, valid = _finite_number(value)
    if not valid or parsed < 0.0:
        return 0.0, False
    return parsed, True


def build_capital_rollout(
    *,
    account_equity: object,
    available_cash: object,
    max_notional_percent: object,
    canary_fills: object = 0,
    blocked_orders: object = 0,
    observation_hours: object = 0,
    reconciliation_fresh: bool = False,
    lineage_valid: bool = False,
    policy_valid: bool = False,
    soak_status: object = "",
) -> dict[str, Any]:
    equity = max(0.0, _number(account_equity))
    available = max(0.0, _number(available_cash))
    notional_percent, notional_percent_valid = _valid_notional_percent(
        max_notional_percent
    )
    fills, fills_valid = _valid_nonnegative_integer(canary_fills)
    blocked, blocked_valid = _valid_nonnegative_integer(blocked_orders)
    hours, hours_valid = _valid_nonnegative_number(observation_hours)
    rollout_evidence_valid = (
        fills_valid and blocked_valid and hours_valid
    )
    normalized_soak = str(soak_status or "").strip().upper()
    reconciliation_valid = reconciliation_fresh is True
    lineage_is_valid = lineage_valid is True
    policy_is_valid = policy_valid is True
    policy_cap = (
        min(available, equity * (notional_percent / 100.0))
        if notional_percent_valid
        else 0.0
    )
    evidence_bounded_cap = policy_cap if rollout_evidence_valid else 0.0
    canary_cap = min(evidence_bounded_cap, CANARY_MAX_NOTIONAL_USDT)
    small_cap = min(evidence_bounded_cap, SMALL_LIVE_MAX_NOTIONAL_USDT)
    full_cap = evidence_bounded_cap

    common_blockers: list[str] = []
    if equity <= 0:
        common_blockers.append("account-equity-invalid")
    if available <= 0:
        common_blockers.append("available-cash-invalid")
    if not notional_percent_valid:
        common_blockers.append("max-notional-percent-invalid")
    if not policy_is_valid:
        common_blockers.append("futures-policy-invalid")
    if not lineage_is_valid:
        common_blockers.append("lineage-invalid")
    if not reconciliation_valid:
        common_blockers.append("reconciliation-not-fresh")
    if not fills_valid:
        common_blockers.append("canary-fills-invalid")
    if not blocked_valid:
        common_blockers.append("blocked-orders-invalid")
    if not hours_valid:
        common_blockers.append("observation-hours-invalid")
    if blocked > 0:
        common_blockers.append("blocked-orders-present")

    canary_blockers = list(common_blockers)
    if canary_cap <= 0:
        canary_blockers.append("canary-cap-unavailable")

    small_blockers = list(common_blockers)
    if fills < MINIMUM_CANARY_FILLS:
        small_blockers.append("minimum-canary-fills-not-met")
    if normalized_soak not in {"PASS", "PASS_WITH_WARNING"}:
        small_blockers.append("soak-not-accepted")
    if small_cap <= 0:
        small_blockers.append("small-live-cap-unavailable")

    full_blockers = list(small_blockers)
    if fills < MINIMUM_FULL_LIVE_FILLS:
        full_blockers.append("minimum-full-live-fills-not-met")
    if hours < MINIMUM_FULL_LIVE_OBSERVATION_HOURS:
        full_blockers.append("minimum-full-live-observation-not-met")
    if normalized_soak != "PASS":
        full_blockers.append("full-live-requires-clean-soak-pass")
    if full_cap <= 0:
        full_blockers.append("full-live-cap-unavailable")

    stages = [
        {
            "id": "CANARY",
            "label": "최소 Canary",
            "maxNotional": canary_cap,
            "ready": not canary_blockers,
            "blockers": list(dict.fromkeys(canary_blockers)),
            "requirements": [
                "유효한 전략 계보·Futures 정책",
                "최신 계좌·포지션 대조",
                f"주문당 최대 {CANARY_MAX_NOTIONAL_USDT:g} USDT와 정책 상한 중 작은 값",
            ],
        },
        {
            "id": "SMALL_LIVE",
            "label": "Small Live",
            "maxNotional": small_cap,
            "ready": not small_blockers,
            "blockers": list(dict.fromkeys(small_blockers)),
            "requirements": [
                f"동일 artifact·deployment 실제 체결 최소 {MINIMUM_CANARY_FILLS}건",
                "차단 주문 0건",
                "PASS 또는 복구 완료 PASS_WITH_WARNING Soak",
                f"주문당 최대 {SMALL_LIVE_MAX_NOTIONAL_USDT:g} USDT와 정책 상한 중 작은 값",
            ],
        },
        {
            "id": "FULL_LIVE",
            "label": "Full Live",
            "maxNotional": full_cap,
            "ready": not full_blockers,
            "blockers": list(dict.fromkeys(full_blockers)),
            "requirements": [
                f"실제 체결 최소 {MINIMUM_FULL_LIVE_FILLS}건",
                f"무인 관찰 최소 {int(MINIMUM_FULL_LIVE_OBSERVATION_HOURS / 24)}일",
                "경고 없는 clean PASS Soak",
                "Artifact 최대 명목금액 정책 안에서만 확대",
            ],
        },
    ]
    return {
        "schemaVersion": ROLLOUT_SCHEMA_VERSION,
        "accountEquity": equity,
        "availableCash": available,
        "policyMaxNotionalPercent": notional_percent,
        "policyMaxNotionalPercentValid": notional_percent_valid,
        "policyCap": policy_cap,
        "reconciliationFresh": reconciliation_valid,
        "lineageValid": lineage_is_valid,
        "policyValid": policy_is_valid,
        "canaryFills": fills,
        "canaryFillsValid": fills_valid,
        "blockedOrders": blocked,
        "blockedOrdersValid": blocked_valid,
        "observationHours": hours,
        "observationHoursValid": hours_valid,
        "soakStatus": normalized_soak or "NOT_RUN",
        "stages": stages,
    }


def capital_cap_for_mode(
    rollout: Mapping[str, Any],
    mode: object,
) -> dict[str, Any]:
    normalized_mode = str(mode or "MONITOR").strip().upper()
    integrity_blockers: list[str] = []
    if not isinstance(rollout, Mapping):
        rollout = {}
        integrity_blockers.append("capital-rollout-structure-invalid")
    if rollout.get("schemaVersion") != ROLLOUT_SCHEMA_VERSION:
        integrity_blockers.append("capital-rollout-schema-invalid")

    if normalized_mode not in {"SMALL_LIVE", "FULL_LIVE"}:
        integrity_blockers.append("capital-rollout-mode-invalid")

    equity, equity_valid = _finite_number(rollout.get("accountEquity"))
    available, available_valid = _finite_number(rollout.get("availableCash"))
    declared_policy_cap, policy_cap_valid = _finite_number(
        rollout.get("policyCap")
    )
    notional_percent, notional_percent_valid = _valid_notional_percent(
        rollout.get("policyMaxNotionalPercent")
    )
    notional_percent_valid_flag = rollout.get(
        "policyMaxNotionalPercentValid"
    )
    if not equity_valid or equity < 0:
        integrity_blockers.append("capital-rollout-account-equity-invalid")
    if not available_valid or available < 0:
        integrity_blockers.append("capital-rollout-available-cash-invalid")
    if not policy_cap_valid or declared_policy_cap < 0:
        integrity_blockers.append("capital-rollout-policy-cap-invalid")
    if type(notional_percent_valid_flag) is not bool:
        integrity_blockers.append(
            "capital-rollout-max-notional-percent-invalid"
        )
    elif notional_percent_valid_flag is True and not notional_percent_valid:
        integrity_blockers.append(
            "capital-rollout-max-notional-percent-invalid"
        )
    elif notional_percent_valid_flag is False and (
        not notional_percent_valid or notional_percent != 0.0
    ):
        integrity_blockers.append(
            "capital-rollout-max-notional-percent-invalid"
        )

    expected_policy_cap = (
        min(available, equity * (notional_percent / 100.0))
        if (
            equity_valid
            and equity >= 0
            and available_valid
            and available >= 0
            and notional_percent_valid
            and notional_percent_valid_flag is True
        )
        else 0.0
    )
    if (
        policy_cap_valid
        and declared_policy_cap >= 0
        and not math.isclose(
            declared_policy_cap,
            expected_policy_cap,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )
    ):
        integrity_blockers.append("capital-rollout-policy-cap-stale")

    evidence_values = {
        "reconciliation_fresh": rollout.get("reconciliationFresh"),
        "lineage_valid": rollout.get("lineageValid"),
        "policy_valid": rollout.get("policyValid"),
    }
    if any(type(value) is not bool for value in evidence_values.values()):
        integrity_blockers.append("capital-rollout-evidence-invalid")

    fills, fills_valid = _finite_number(rollout.get("canaryFills"))
    blocked, blocked_valid = _finite_number(rollout.get("blockedOrders"))
    hours, hours_valid = _finite_number(rollout.get("observationHours"))
    fills_valid_flag = rollout.get("canaryFillsValid")
    blocked_valid_flag = rollout.get("blockedOrdersValid")
    hours_valid_flag = rollout.get("observationHoursValid")
    fills_value_valid = (
        fills_valid and fills >= 0 and fills.is_integer()
    )
    blocked_value_valid = (
        blocked_valid and blocked >= 0 and blocked.is_integer()
    )
    hours_value_valid = hours_valid and hours >= 0
    if any(
        type(flag) is not bool
        for flag in (
            fills_valid_flag,
            blocked_valid_flag,
            hours_valid_flag,
        )
    ):
        integrity_blockers.append("capital-rollout-evidence-invalid")
    elif (
        (fills_valid_flag is True and not fills_value_valid)
        or (
            fills_valid_flag is False
            and (not fills_value_valid or fills != 0.0)
        )
        or (blocked_valid_flag is True and not blocked_value_valid)
        or (
            blocked_valid_flag is False
            and (not blocked_value_valid or blocked != 0.0)
        )
        or (hours_valid_flag is True and not hours_value_valid)
        or (
            hours_valid_flag is False
            and (not hours_value_valid or hours != 0.0)
        )
    ):
        integrity_blockers.append("capital-rollout-evidence-invalid")
    if not isinstance(rollout.get("soakStatus"), str):
        integrity_blockers.append("capital-rollout-evidence-invalid")

    stages = rollout.get("stages")
    if not isinstance(stages, list):
        stages = []
        integrity_blockers.append("capital-rollout-stages-invalid")
    stage_rows = [item for item in stages if isinstance(item, Mapping)]
    stage_ids = [str(item.get("id") or "") for item in stage_rows]
    if (
        len(stage_rows) != len(stages)
        or len(stage_rows) != len(ROLLOUT_STAGE_IDS)
        or set(stage_ids) != set(ROLLOUT_STAGE_IDS)
        or len(stage_ids) != len(set(stage_ids))
    ):
        integrity_blockers.append("capital-rollout-stages-invalid")
    by_id = {
        str(item.get("id") or ""): dict(item)
        for item in stage_rows
    }

    canonical_by_id: dict[str, dict[str, Any]] = {}
    if not integrity_blockers:
        canonical = build_capital_rollout(
            account_equity=equity,
            available_cash=available,
            max_notional_percent=(
                notional_percent
                if notional_percent_valid_flag is True
                else float("nan")
            ),
            canary_fills=(
                fills if fills_valid_flag is True else float("nan")
            ),
            blocked_orders=(
                blocked if blocked_valid_flag is True else float("nan")
            ),
            observation_hours=(
                hours if hours_valid_flag is True else float("nan")
            ),
            reconciliation_fresh=evidence_values[
                "reconciliation_fresh"
            ],
            lineage_valid=evidence_values["lineage_valid"],
            policy_valid=evidence_values["policy_valid"],
            soak_status=rollout.get("soakStatus"),
        )
        canonical_by_id = {
            str(item.get("id") or ""): dict(item)
            for item in canonical["stages"]
        }
        base_ceiling = max(
            0.0,
            min(
                equity,
                available,
                declared_policy_cap,
                expected_policy_cap,
            ),
        )
        for stage_id in ROLLOUT_STAGE_IDS:
            candidate = by_id.get(stage_id, {})
            expected = canonical_by_id[stage_id]
            candidate_cap, candidate_cap_valid = _finite_number(
                candidate.get("maxNotional")
            )
            stage_ceiling = min(
                base_ceiling,
                CANARY_MAX_NOTIONAL_USDT
                if stage_id == "CANARY"
                else SMALL_LIVE_MAX_NOTIONAL_USDT
                if stage_id == "SMALL_LIVE"
                else base_ceiling,
            )
            candidate_blockers = candidate.get("blockers")
            if (
                not candidate_cap_valid
                or candidate_cap < 0
                or candidate_cap > stage_ceiling + 1e-9
            ):
                integrity_blockers.append(
                    "capital-rollout-stage-cap-invalid"
                )
            if (
                type(candidate.get("ready")) is not bool
                or not isinstance(candidate_blockers, list)
                or any(
                    not isinstance(item, str)
                    for item in candidate_blockers
                )
            ):
                integrity_blockers.append("capital-rollout-stage-invalid")
            if (
                candidate.get("ready") is not expected.get("ready")
                or not math.isclose(
                    candidate_cap if candidate_cap_valid else 0.0,
                    float(expected.get("maxNotional") or 0.0),
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                )
                or candidate_blockers != expected.get("blockers")
            ):
                integrity_blockers.append("capital-rollout-stage-stale")

    if normalized_mode == "FULL_LIVE":
        target = "FULL_LIVE"
    elif normalized_mode == "SMALL_LIVE":
        # SMALL_LIVE begins under the canary cap. Once the canary evidence
        # passes, the same runtime mode receives the larger Small cap.
        target = (
            "SMALL_LIVE"
            if canonical_by_id.get("SMALL_LIVE", {}).get("ready") is True
            else "CANARY"
        )
    else:
        target = "CANARY"

    stage = by_id.get(target, {})
    canonical_stage = canonical_by_id.get(target, {})
    stage_cap, stage_cap_valid = _finite_number(stage.get("maxNotional"))
    static_cap = (
        CANARY_MAX_NOTIONAL_USDT
        if target == "CANARY"
        else SMALL_LIVE_MAX_NOTIONAL_USDT
        if target == "SMALL_LIVE"
        else expected_policy_cap
    )
    effective_ceiling = max(
        0.0,
        min(
            equity if equity_valid else 0.0,
            available if available_valid else 0.0,
            declared_policy_cap if policy_cap_valid else 0.0,
            expected_policy_cap,
            static_cap,
        ),
    )
    if (
        not stage_cap_valid
        or stage_cap < 0
        or stage_cap > effective_ceiling + 1e-9
    ):
        integrity_blockers.append("capital-rollout-stage-cap-invalid")

    stage_blockers = stage.get("blockers")
    if (
        type(stage.get("ready")) is not bool
        or not isinstance(stage_blockers, list)
        or any(not isinstance(item, str) for item in stage_blockers)
    ):
        integrity_blockers.append("capital-rollout-stage-invalid")
    if canonical_stage and (
        stage.get("ready") is not canonical_stage.get("ready")
        or not math.isclose(
            stage_cap if stage_cap_valid else 0.0,
            float(canonical_stage.get("maxNotional") or 0.0),
            rel_tol=1e-9,
            abs_tol=1e-9,
        )
        or stage_blockers != canonical_stage.get("blockers")
    ):
        integrity_blockers.append("capital-rollout-stage-stale")

    blockers = list(stage_blockers) if isinstance(stage_blockers, list) else []
    blockers.extend(integrity_blockers)
    blockers = list(dict.fromkeys(str(item) for item in blockers if str(item)))
    structurally_valid = not integrity_blockers
    return {
        "stage": target,
        "ready": structurally_valid and stage.get("ready") is True,
        "maxNotional": (
            min(max(0.0, stage_cap), effective_ceiling)
            if structurally_valid
            else 0.0
        ),
        "blockers": blockers,
    }


__all__ = [
    "CANARY_MAX_NOTIONAL_USDT",
    "MINIMUM_CANARY_FILLS",
    "MINIMUM_FULL_LIVE_FILLS",
    "MINIMUM_FULL_LIVE_OBSERVATION_HOURS",
    "ROLLOUT_SCHEMA_VERSION",
    "SMALL_LIVE_MAX_NOTIONAL_USDT",
    "build_capital_rollout",
    "capital_cap_for_mode",
]
