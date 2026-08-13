from __future__ import annotations

"""Fail-closed Live Trader adapter for time-boxed functional tests.

The shared :mod:`trading_runtime.functional_test` module owns permit issuance,
immutable hashes, expiry, and KIS live activation tokens.  This module adds the
piece that must be evaluated against *current* Live Trader truth immediately
before both pre-trade admission and broker dispatch.

Functional-test results are deliberately not promotion evidence.  The adapter
also never treats ``FULL_LIVE`` as a functional-test execution mode.
"""

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any, Literal, Mapping

try:  # The shared contract can be delivered independently of this app.
    from trading_runtime.functional_test import (
        FUNCTIONAL_TEST_PERMIT_SCHEMA_VERSION,
        FunctionalTestBinding,
        FunctionalTestCaps,
        FunctionalTestContractError,
        FunctionalTestLiveActivation,
        FunctionalTestPermit,
        assert_functional_test_action_allowed,
        parse_functional_test_permit,
        parse_live_activation_token,
    )

    _FUNCTIONAL_TEST_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - exercised only in partial installs.
    FUNCTIONAL_TEST_PERMIT_SCHEMA_VERSION = "functional-test-permit-unavailable"
    FunctionalTestBinding = Any  # type: ignore[misc,assignment]
    FunctionalTestCaps = Any  # type: ignore[misc,assignment]
    FunctionalTestContractError = ValueError  # type: ignore[misc,assignment]
    FunctionalTestLiveActivation = Any  # type: ignore[misc,assignment]
    FunctionalTestPermit = Any  # type: ignore[misc,assignment]
    assert_functional_test_action_allowed = None  # type: ignore[assignment]
    parse_functional_test_permit = None  # type: ignore[assignment]
    parse_live_activation_token = None  # type: ignore[assignment]
    _FUNCTIONAL_TEST_IMPORT_ERROR = f"{type(exc).__name__}:{exc}"


FUNCTIONAL_TEST_SAFETY_REPORT_SCHEMA_VERSION = (
    "live-functional-test-safety-report-v1"
)
FUNCTIONAL_TEST_END_PLAN_SCHEMA_VERSION = "functional-test-end-plan-v1"
FUNCTIONAL_TEST_EVIDENCE_CLASS = "FUNCTIONAL_TEST_NON_PROMOTION"

FunctionalTestPhase = Literal["PRETRADE", "DISPATCH"]
FunctionalTestEnvironment = Literal["KIS_DEMO", "KIS_LIVE"]
FlattenPolicy = Literal[
    "FLATTEN",
    "RECONCILE_ONLY",
    "KEEP_WITH_EXPLICIT_HANDOFF",
]

_CAP_NAMES = (
    "max_order_quantity",
    "max_order_notional",
    "max_gross_exposure",
    "max_orders",
    "max_open_positions",
    "max_loss",
)
_CAP_ALIASES = {
    "max_order_quantity": ("max_order_quantity", "maxOrderQuantity"),
    "max_order_notional": ("max_order_notional", "maxOrderNotional"),
    "max_gross_exposure": ("max_gross_exposure", "maxGrossExposure"),
    "max_orders": ("max_orders", "maxOrders"),
    "max_open_positions": ("max_open_positions", "maxOpenPositions"),
    "max_loss": ("max_loss", "maxLoss"),
}
_BINDING_ALIASES = {
    "strategy_artifact_id": (
        "strategy_artifact_id",
        "strategyArtifactId",
        "strategy_id",
        "strategyId",
    ),
    "strategy_artifact_hash": (
        "strategy_artifact_hash",
        "strategyArtifactHash",
    ),
    "strategy_instance_id": (
        "strategy_instance_id",
        "strategyInstanceId",
    ),
    "portfolio_required": ("portfolio_required", "portfolioRequired"),
    "portfolio_artifact_id": (
        "portfolio_artifact_id",
        "portfolioArtifactId",
        "portfolio_id",
        "portfolioId",
    ),
    "portfolio_artifact_hash": (
        "portfolio_artifact_hash",
        "portfolioArtifactHash",
    ),
    "portfolio_instance_id": (
        "portfolio_instance_id",
        "portfolioInstanceId",
    ),
    "account_id": ("account_id", "accountId"),
    "symbols": ("symbols", "allowedSymbols"),
    "market_group": ("market_group", "marketGroup"),
    "execution_route": ("execution_route", "executionRoute"),
    "settlement_currency": (
        "settlement_currency",
        "settlementCurrency",
    ),
    "exchanges": ("exchanges",),
    "symbol_routes": ("symbol_routes", "symbolRoutes"),
    "route_scope_hash": ("route_scope_hash", "routeScopeHash"),
}
_INTENT_BINDING_ALIASES = {
    **_BINDING_ALIASES,
    # Operational sleeve identity remains in strategy_instance_id. Permit
    # claims use an isolated namespace so a portfolio-only permit can keep
    # exact fill attribution without accidentally claiming a strategy scope.
    "strategy_artifact_id": (
        "functional_test_strategy_artifact_id",
        "functionalTestStrategyArtifactId",
        "permit_scope_strategy_artifact_id",
        "permitScopeStrategyArtifactId",
    ),
    "strategy_artifact_hash": (
        "functional_test_strategy_artifact_hash",
        "functionalTestStrategyArtifactHash",
        "permit_scope_strategy_artifact_hash",
        "permitScopeStrategyArtifactHash",
    ),
    "strategy_instance_id": (
        "functional_test_strategy_instance_id",
        "functionalTestStrategyInstanceId",
        "permit_scope_strategy_instance_id",
        "permitScopeStrategyInstanceId",
    ),
}


@dataclass(frozen=True)
class FunctionalTestRiskSnapshot:
    """Fresh risk truth used to project the order after admission.

    Loss is an absolute, non-negative currency amount.  ``opens_new_position``
    must be derived from reconciled broker positions, not from an untrusted
    strategy claim.
    """

    gross_exposure: float
    submitted_order_count: int
    open_position_count: int
    loss: float
    opens_new_position: bool
    working_order_count: int = 0
    reconciled: bool = True
    observed_at: str = ""
    gross_exposure_after: float | None = None
    open_position_count_after: int | None = None


@dataclass(frozen=True)
class FunctionalTestBlocker:
    code: str
    detail: str
    source: str = "live-functional-test"

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "detail": self.detail,
            "source": self.source,
        }


@dataclass(frozen=True)
class FunctionalTestSafetyReport:
    phase: FunctionalTestPhase
    allowed: bool
    environment: str
    permit_id: str
    permit_expires_at: str
    blockers: tuple[FunctionalTestBlocker, ...]
    effective_caps: dict[str, float | int]
    observed: dict[str, Any]
    binding: dict[str, Any]
    scope_hash: str
    intent_hash: str
    runtime_permit_schema_version: str = FUNCTIONAL_TEST_PERMIT_SCHEMA_VERSION
    evidence_class: str = FUNCTIONAL_TEST_EVIDENCE_CLASS
    promotion_eligible: bool = False
    full_live_allowed: bool = False
    schema_version: str = FUNCTIONAL_TEST_SAFETY_REPORT_SCHEMA_VERSION

    @property
    def blocker_codes(self) -> tuple[str, ...]:
        return tuple(item.code for item in self.blockers)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "phase": self.phase,
            "allowed": self.allowed,
            "environment": self.environment,
            "permitId": self.permit_id,
            "permitExpiresAt": self.permit_expires_at,
            "blockers": [item.to_dict() for item in self.blockers],
            "blockerCodes": list(self.blocker_codes),
            "effectiveCaps": dict(self.effective_caps),
            "observed": dict(self.observed),
            "binding": dict(self.binding),
            "scopeHash": self.scope_hash,
            "intentHash": self.intent_hash,
            "runtimePermitSchemaVersion": self.runtime_permit_schema_version,
            "evidenceClass": self.evidence_class,
            "promotionEligible": False,
            "fullLiveAllowed": False,
        }


@dataclass(frozen=True)
class FunctionalTestEndAction:
    sequence: int
    action: str
    required: bool
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "action": self.action,
            "required": self.required,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class FunctionalTestEndPlan:
    permit_id: str
    environment: str
    flatten_policy: FlattenPolicy
    new_entries_blocked: bool
    cancel_working_orders: bool
    reconciliation_required: bool
    flatten_required: bool
    explicit_handoff_required: bool
    actions: tuple[FunctionalTestEndAction, ...]
    blockers: tuple[FunctionalTestBlocker, ...] = field(default_factory=tuple)
    evidence_class: str = FUNCTIONAL_TEST_EVIDENCE_CLASS
    promotion_eligible: bool = False
    schema_version: str = FUNCTIONAL_TEST_END_PLAN_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "permitId": self.permit_id,
            "environment": self.environment,
            "flattenPolicy": self.flatten_policy,
            "newEntriesBlocked": True,
            "cancelWorkingOrders": self.cancel_working_orders,
            "reconciliationRequired": True,
            "flattenRequired": self.flatten_required,
            "explicitHandoffRequired": self.explicit_handoff_required,
            "actions": [item.to_dict() for item in self.actions],
            "blockers": [item.to_dict() for item in self.blockers],
            "evidenceClass": self.evidence_class,
            "promotionEligible": False,
        }


def evaluate_functional_test_order(
    permit_payload: Any,
    *,
    current_binding: Any,
    environment: str,
    intent: Any,
    global_caps: Any,
    risk: FunctionalTestRiskSnapshot,
    operator_confirmed: bool = False,
    live_activation_payload: Any | None = None,
    phase: FunctionalTestPhase = "PRETRADE",
    now: datetime | None = None,
) -> FunctionalTestSafetyReport:
    """Evaluate one functional-test intent against fresh, exact scope.

    Callers must run this function twice: once before local pre-trade admission
    and once immediately before dispatch, using newly read binding/risk state.
    A PRETRADE report is not a dispatch token.
    """

    normalized_phase = str(phase or "").strip().upper()
    blockers: list[FunctionalTestBlocker] = []
    if normalized_phase not in {"PRETRADE", "DISPATCH"}:
        blockers.append(
            _blocker(
                "functional-test-phase-invalid",
                "phase must be PRETRADE or DISPATCH",
            )
        )
        normalized_phase = "PRETRADE"

    normalized_environment = _enum_text(environment).strip().upper()
    if normalized_environment not in {"KIS_DEMO", "KIS_LIVE"}:
        blockers.append(
            _blocker(
                "functional-test-environment-invalid",
                "functional tests support only KIS_DEMO or KIS_LIVE",
            )
        )

    permit: Any | None = None
    if parse_functional_test_permit is None:
        blockers.append(
            _blocker(
                "functional-test-runtime-unavailable",
                _FUNCTIONAL_TEST_IMPORT_ERROR
                or "trading_runtime.functional_test is unavailable",
                source="shared-contract",
            )
        )
    else:
        try:
            permit = parse_functional_test_permit(permit_payload)
        except (FunctionalTestContractError, TypeError, ValueError) as exc:
            blockers.append(_contract_blocker(exc, "permit"))

    current_binding_payload = _binding_payload(current_binding)
    permit_binding_payload: dict[str, Any] = {}
    permit_caps_payload: dict[str, Any] = {}
    permit_environment = ""
    permit_id = ""
    permit_expires_at = ""
    if permit is not None:
        permit_binding_payload = _binding_payload(_value(permit, "binding"))
        permit_caps_payload = _object_payload(_value(permit, "caps"))
        permit_environment = _enum_text(
            _value(permit, "environment", "")
        ).strip().upper()
        permit_id = str(
            _value(permit, "permit_id", _value(permit, "permitId", "")) or ""
        )
        permit_expires_at = _iso_value(
            _value(
                permit,
                "ends_at",
                _value(
                    permit,
                    "endsAt",
                    _value(permit, "expires_at", _value(permit, "expiresAt", "")),
                ),
            )
        )
        if permit_environment != normalized_environment:
            blockers.append(
                _blocker(
                    "functional-test-environment-mismatch",
                    f"permit={permit_environment or '<missing>'}, "
                    f"current={normalized_environment or '<missing>'}",
                )
            )
        blockers.extend(
            _binding_blockers(
                permit_binding_payload,
                current_binding_payload,
            )
        )

    intent_payload = _intent_payload(intent)
    blockers.extend(
        _intent_binding_blockers(
            current_binding_payload,
            intent_payload,
            normalized_environment,
        )
    )

    mode = str(intent_payload.get("mode") or "").strip().upper()
    if mode == "FULL_LIVE" or normalized_environment == "FULL_LIVE":
        blockers.append(
            _blocker(
                "functional-test-full-live-forbidden",
                "FULL_LIVE is never a functional-test execution mode",
            )
        )
    elif normalized_environment == "KIS_LIVE" and mode != "SMALL_LIVE":
        blockers.append(
            _blocker(
                "functional-test-live-mode-invalid",
                "KIS_LIVE functional tests require SMALL_LIVE dispatch mode",
            )
        )
    elif normalized_environment == "KIS_DEMO" and mode not in {
        "PAPER",
        "SMALL_LIVE",
    }:
        blockers.append(
            _blocker(
                "functional-test-demo-mode-invalid",
                "KIS_DEMO functional tests require PAPER or SMALL_LIVE mode",
            )
        )

    metadata = intent_payload.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    if any(
        metadata.get(key) is True
        for key in (
            "promotion_eligible",
            "promotionEligible",
            "use_as_promotion_evidence",
            "useAsPromotionEvidence",
            "full_live_requested",
            "fullLiveRequested",
        )
    ):
        blockers.append(
            _blocker(
                "functional-test-promotion-evidence-forbidden",
                "functional-test orders cannot request promotion/FULL_LIVE evidence",
            )
        )

    if normalized_environment == "KIS_LIVE":
        if operator_confirmed is not True:
            blockers.append(
                _blocker(
                    "functional-test-operator-confirmation-required",
                    "KIS_LIVE requires an explicit operator confirmation",
                )
            )
        if permit is not None:
            if live_activation_payload is None:
                blockers.append(
                    _blocker(
                        "functional-test-live-activation-required",
                        "KIS_LIVE requires a current daily activation token",
                    )
                )
            elif parse_live_activation_token is None:
                blockers.append(
                    _blocker(
                        "functional-test-live-activation-runtime-unavailable",
                        "shared activation-token validator is unavailable",
                        source="shared-contract",
                    )
                )
            else:
                try:
                    parse_live_activation_token(
                        live_activation_payload,
                        permit=permit,
                        now=now,
                    )
                except (FunctionalTestContractError, TypeError, ValueError) as exc:
                    blockers.append(_contract_blocker(exc, "live-activation"))

    effective_caps, cap_blockers = _effective_caps(
        global_caps,
        permit_caps_payload,
    )
    blockers.extend(cap_blockers)

    observation, risk_blockers = _risk_assessment(
        effective_caps,
        intent_payload,
        risk,
    )
    blockers.extend(risk_blockers)

    # The shared contract is the canonical action-scope validator.  Its exact
    # error codes are preserved in the structured report.
    if permit is not None and callable(assert_functional_test_action_allowed):
        try:
            _assert_shared_action_allowed(
                permit=permit,
                current_binding=current_binding,
                environment=normalized_environment,
                intent=intent,
                live_activation_payload=live_activation_payload,
                effective_observation=observation,
                now=now or datetime.now(timezone.utc),
            )
        except (FunctionalTestContractError, TypeError, ValueError) as exc:
            blockers.append(_contract_blocker(exc, "action"))

    blockers = _dedupe_blockers(blockers)
    scope_hash = _stable_hash(
        {
            "permitBinding": permit_binding_payload,
            "currentBinding": current_binding_payload,
            "environment": normalized_environment,
        }
    )
    intent_hash = _stable_hash(
        {
            "strategyId": intent_payload.get("strategy_id"),
            "symbol": intent_payload.get("symbol"),
            "side": intent_payload.get("side"),
            "quantity": intent_payload.get("quantity"),
            "referencePrice": intent_payload.get("reference_price"),
            "mode": mode,
            "binding": {
                key: _first(metadata, *_INTENT_BINDING_ALIASES[key])
                for key in _INTENT_BINDING_ALIASES
                if key != "symbols"
            },
        }
    )
    return FunctionalTestSafetyReport(
        phase=normalized_phase,  # type: ignore[arg-type]
        allowed=not blockers,
        environment=normalized_environment,
        permit_id=permit_id,
        permit_expires_at=permit_expires_at,
        blockers=tuple(blockers),
        effective_caps=effective_caps,
        observed=observation,
        binding=current_binding_payload,
        scope_hash=scope_hash,
        intent_hash=intent_hash,
    )


def build_functional_test_end_plan(
    permit_payload: Any,
    *,
    environment: str,
    working_order_count: int,
    open_position_count: int,
    flatten_policy: FlattenPolicy = "FLATTEN",
) -> FunctionalTestEndPlan:
    """Return the mandatory, non-promotional end-of-session action plan.

    Permit expiry must never prevent cleanup, so this function intentionally
    reads only non-authorizing identifiers and does not reject an expired
    permit.  Cleanup actions remain required even for an invalid payload.
    """

    blockers: list[FunctionalTestBlocker] = []
    permit_payload_dict = _object_payload(permit_payload)
    permit_id = str(
        _first(permit_payload_dict, "permit_id", "permitId") or ""
    )
    normalized_environment = _enum_text(environment).strip().upper()
    normalized_policy = str(flatten_policy or "").strip().upper()
    if normalized_policy not in {
        "FLATTEN",
        "RECONCILE_ONLY",
        "KEEP_WITH_EXPLICIT_HANDOFF",
    }:
        blockers.append(
            _blocker(
                "functional-test-flatten-policy-invalid",
                "flatten policy must be FLATTEN, RECONCILE_ONLY, or "
                "KEEP_WITH_EXPLICIT_HANDOFF",
            )
        )
        normalized_policy = "FLATTEN"

    working_count = _non_negative_int(working_order_count)
    position_count = _non_negative_int(open_position_count)
    if _finite_integer(working_order_count) is None or int(working_order_count) < 0:
        blockers.append(
            _blocker(
                "functional-test-end-working-order-count-invalid",
                "working-order count is unknown; cancel-all remains mandatory",
                source="end-session",
            )
        )
    if _finite_integer(open_position_count) is None or int(open_position_count) < 0:
        blockers.append(
            _blocker(
                "functional-test-end-open-position-count-invalid",
                "open-position count is unknown; flatten/reconciliation remains mandatory",
                source="end-session",
            )
        )
    flatten_required = normalized_policy == "FLATTEN" and position_count > 0
    handoff_required = (
        normalized_policy == "KEEP_WITH_EXPLICIT_HANDOFF"
        and position_count > 0
    )
    actions = [
        FunctionalTestEndAction(
            1,
            "BLOCK_NEW_ENTRIES",
            True,
            "기능시험 종료 시각부터 신규 진입을 즉시 차단합니다.",
        ),
        FunctionalTestEndAction(
            2,
            "CANCEL_WORKING_ORDERS",
            True,
            f"현재 작업 주문 {working_count}건을 취소하고 terminal 상태를 확인합니다.",
        ),
    ]
    if normalized_policy == "FLATTEN":
        actions.append(
            FunctionalTestEndAction(
                3,
                "FLATTEN_POSITIONS",
                position_count > 0,
                f"현재 포지션 {position_count}건을 축소/청산합니다.",
            )
        )
    elif normalized_policy == "KEEP_WITH_EXPLICIT_HANDOFF":
        actions.append(
            FunctionalTestEndAction(
                3,
                "RECORD_EXPLICIT_POSITION_HANDOFF",
                position_count > 0,
                "잔존 포지션별 담당자·손절·만료 시각을 기록해야 합니다.",
            )
        )
    else:
        actions.append(
            FunctionalTestEndAction(
                3,
                "RECONCILE_ONLY_POSITION_POLICY",
                position_count > 0,
                "잔존 포지션을 변경하지 않고 정확한 브로커 원장으로 대조합니다.",
            )
        )
    actions.append(
        FunctionalTestEndAction(
            4,
            "FINAL_BROKER_RECONCILIATION",
            True,
            "취소·청산/인계 후 주문, 체결, 현금, 포지션을 최종 대조합니다.",
        )
    )
    return FunctionalTestEndPlan(
        permit_id=permit_id,
        environment=normalized_environment,
        flatten_policy=normalized_policy,  # type: ignore[arg-type]
        new_entries_blocked=True,
        cancel_working_orders=True,
        reconciliation_required=True,
        flatten_required=flatten_required,
        explicit_handoff_required=handoff_required,
        actions=tuple(actions),
        blockers=tuple(blockers),
    )


def _assert_shared_action_allowed(
    *,
    permit: Any,
    current_binding: Any,
    environment: str,
    intent: Any,
    live_activation_payload: Any | None,
    effective_observation: Mapping[str, Any],
    now: datetime,
) -> None:
    """Call the canonical validator using its public keyword contract."""

    binding_payload = _binding_payload(current_binding)
    assert_functional_test_action_allowed(
        permit=permit,
        now=now,
        environment=environment,
        account_id=str(binding_payload.get("account_id") or ""),
        strategy_artifact_id=str(
            binding_payload.get("strategy_artifact_id") or ""
        ),
        strategy_artifact_hash=str(
            binding_payload.get("strategy_artifact_hash") or ""
        ),
        strategy_instance_id=str(
            binding_payload.get("strategy_instance_id") or ""
        ),
        portfolio_artifact_id=str(
            binding_payload.get("portfolio_artifact_id") or ""
        ),
        portfolio_artifact_hash=str(
            binding_payload.get("portfolio_artifact_hash") or ""
        ),
        portfolio_instance_id=str(
            binding_payload.get("portfolio_instance_id") or ""
        ),
        symbol=str(_value(intent, "symbol", "")),
        order_quantity=int(
            float(effective_observation.get("quantity") or 0)
        ),
        order_notional=float(
            effective_observation.get("orderNotional") or 0.0
        ),
        projected_gross_exposure=float(
            effective_observation.get("grossExposureAfter") or 0.0
        ),
        projected_order_count=int(
            effective_observation.get("submittedOrderCountAfter") or 0
        ),
        projected_open_positions=int(
            effective_observation.get("openPositionCountAfter") or 0
        ),
        cumulative_loss=float(effective_observation.get("loss") or 0.0),
        exchange=str(
            _first(
                _object_payload(_value(intent, "metadata", {})),
                "exchange",
                "exchangeCode",
            )
            or ""
        ),
        live_activation=live_activation_payload,
    )


def _effective_caps(
    global_caps: Any,
    permit_caps: Mapping[str, Any],
) -> tuple[dict[str, float | int], list[FunctionalTestBlocker]]:
    global_payload = _object_payload(global_caps)
    effective: dict[str, float | int] = {}
    blockers: list[FunctionalTestBlocker] = []
    for name in _CAP_NAMES:
        global_value = _numeric_cap(global_payload, name)
        permit_value = _numeric_cap(permit_caps, name)
        if global_value is None:
            blockers.append(
                _blocker(
                    f"functional-test-global-{name.replace('_', '-')}-invalid",
                    f"global risk cap {name} must be finite and positive",
                    source="global-risk",
                )
            )
            continue
        if permit_value is None:
            blockers.append(
                _blocker(
                    f"functional-test-permit-{name.replace('_', '-')}-invalid",
                    f"permit cap {name} must be finite and positive",
                    source="permit",
                )
            )
            continue
        selected = min(global_value, permit_value)
        effective[name] = int(selected) if name in {
            "max_order_quantity",
            "max_orders",
            "max_open_positions",
        } else selected
    return effective, blockers


def _risk_assessment(
    effective_caps: Mapping[str, float | int],
    intent: Mapping[str, Any],
    risk: FunctionalTestRiskSnapshot,
) -> tuple[dict[str, Any], list[FunctionalTestBlocker]]:
    blockers: list[FunctionalTestBlocker] = []
    quantity = _finite_number(intent.get("quantity"))
    price = _finite_number(intent.get("reference_price"))
    if quantity is None or quantity <= 0:
        blockers.append(
            _blocker(
                "functional-test-order-quantity-invalid",
                "order quantity must be finite and positive",
                source="order-intent",
            )
        )
        quantity = 0.0
    elif not quantity.is_integer():
        blockers.append(
            _blocker(
                "functional-test-order-quantity-must-be-integer",
                "KIS functional-test order quantity must be a whole share",
                source="order-intent",
            )
        )
    if price is None or price <= 0:
        blockers.append(
            _blocker(
                "functional-test-reference-price-invalid",
                "reference price must be finite and positive",
                source="order-intent",
            )
        )
        price = 0.0
    notional = quantity * price

    risk_values = {
        "gross_exposure": _finite_number(risk.gross_exposure),
        "submitted_order_count": _finite_integer(risk.submitted_order_count),
        "open_position_count": _finite_integer(risk.open_position_count),
        "loss": _finite_number(risk.loss),
        "working_order_count": _finite_integer(risk.working_order_count),
    }
    for name, value in risk_values.items():
        if value is None or value < 0:
            blockers.append(
                _blocker(
                    f"functional-test-risk-{name.replace('_', '-')}-invalid",
                    f"fresh risk field {name} must be finite and non-negative",
                    source="risk-snapshot",
                )
            )
            risk_values[name] = 0
    if risk.reconciled is not True:
        blockers.append(
            _blocker(
                "functional-test-risk-not-reconciled",
                "broker orders and positions must be reconciled before admission",
                source="risk-snapshot",
            )
        )

    gross = float(risk_values["gross_exposure"] or 0.0)
    order_count = int(risk_values["submitted_order_count"] or 0)
    position_count = int(risk_values["open_position_count"] or 0)
    loss = float(risk_values["loss"] or 0.0)
    explicit_projected_gross = _finite_number(risk.gross_exposure_after)
    if risk.gross_exposure_after is not None and (
        explicit_projected_gross is None or explicit_projected_gross < 0
    ):
        blockers.append(
            _blocker(
                "functional-test-risk-projected-gross-exposure-invalid",
                "projected gross exposure must be finite and non-negative",
                source="risk-snapshot",
            )
        )
        explicit_projected_gross = None
    if not isinstance(risk.opens_new_position, bool):
        blockers.append(
            _blocker(
                "functional-test-risk-opens-new-position-invalid",
                "opens_new_position must be broker-derived boolean truth",
                source="risk-snapshot",
            )
        )
    opens_new_position = (
        risk.opens_new_position
        if isinstance(risk.opens_new_position, bool)
        else True
    )
    projected_gross = (
        explicit_projected_gross
        if explicit_projected_gross is not None
        else gross + notional
    )
    projected_orders = order_count + 1
    explicit_projected_positions = _finite_integer(
        risk.open_position_count_after
    )
    if risk.open_position_count_after is not None and (
        explicit_projected_positions is None
        or explicit_projected_positions < 0
    ):
        blockers.append(
            _blocker(
                "functional-test-risk-projected-open-position-count-invalid",
                "projected open-position count must be an integer >= 0",
                source="risk-snapshot",
            )
        )
        explicit_projected_positions = None
    projected_positions = (
        explicit_projected_positions
        if explicit_projected_positions is not None
        else position_count + (1 if opens_new_position else 0)
    )

    checks = (
        (
            "max_order_quantity",
            quantity,
            "functional-test-order-quantity-cap-exceeded",
        ),
        (
            "max_order_notional",
            notional,
            "functional-test-order-notional-cap-exceeded",
        ),
        (
            "max_gross_exposure",
            projected_gross,
            "functional-test-gross-exposure-cap-exceeded",
        ),
        (
            "max_orders",
            projected_orders,
            "functional-test-order-count-cap-exceeded",
        ),
        (
            "max_open_positions",
            projected_positions,
            "functional-test-open-position-cap-exceeded",
        ),
        (
            "max_loss",
            loss,
            "functional-test-loss-cap-exceeded",
        ),
    )
    for cap_name, observed, code in checks:
        cap = effective_caps.get(cap_name)
        exceeded = (
            observed >= cap
            if cap_name == "max_loss" and cap is not None
            else observed > cap
            if cap is not None
            else False
        )
        if exceeded:
            blockers.append(
                _blocker(
                    code,
                    f"observed={observed:g}, effectiveCap={float(cap):g}",
                    source="effective-risk-cap",
                )
            )

    return {
        "quantity": quantity,
        "referencePrice": price,
        "orderNotional": notional,
        "grossExposureBefore": gross,
        "grossExposureAfter": projected_gross,
        "submittedOrderCountBefore": order_count,
        "submittedOrderCountAfter": projected_orders,
        "openPositionCountBefore": position_count,
        "openPositionCountAfter": projected_positions,
        "workingOrderCount": int(risk_values["working_order_count"] or 0),
        "loss": loss,
        "opensNewPosition": opens_new_position,
        "reconciled": risk.reconciled,
        "observedAt": risk.observed_at,
    }, blockers


def _binding_blockers(
    permit_binding: Mapping[str, Any],
    current_binding: Mapping[str, Any],
) -> list[FunctionalTestBlocker]:
    blockers: list[FunctionalTestBlocker] = []
    route_fields = {
        "market_group",
        "execution_route",
        "settlement_currency",
        "exchanges",
        "symbol_routes",
        "route_scope_hash",
    }
    us_route = "US_STOCK" in {
        str(permit_binding.get("market_group") or "").upper(),
        str(current_binding.get("market_group") or "").upper(),
    }
    for field_name in _BINDING_ALIASES:
        if field_name in route_fields and not us_route:
            continue
        permit_value = permit_binding.get(field_name)
        current_value = current_binding.get(field_name)
        if field_name in {"symbols", "exchanges", "symbol_routes"}:
            permit_symbols = _symbols(permit_value)
            current_symbols = _symbols(current_value)
            if field_name == "symbol_routes":
                permit_symbols = _symbol_routes(permit_value)
                current_symbols = _symbol_routes(current_value)
            if permit_symbols != current_symbols:
                blockers.append(
                    _blocker(
                        "functional-test-binding-symbols-mismatch",
                        f"permit={permit_symbols}, current={current_symbols}",
                    )
                )
            continue
        if field_name == "portfolio_required":
            permit_value = bool(permit_value)
            current_value = bool(current_value)
        else:
            permit_value = str(permit_value or "")
            current_value = str(current_value or "")
        if permit_value != current_value:
            blockers.append(
                _blocker(
                    f"functional-test-binding-{field_name.replace('_', '-')}-mismatch",
                    f"permit={permit_value!r}, current={current_value!r}",
                )
            )
    return blockers


def _intent_binding_blockers(
    binding: Mapping[str, Any],
    intent: Mapping[str, Any],
    environment: str,
) -> list[FunctionalTestBlocker]:
    blockers: list[FunctionalTestBlocker] = []
    metadata = intent.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    strategy_fields = (
        "strategy_artifact_id",
        "strategy_artifact_hash",
        "strategy_instance_id",
    )
    expected_fields: tuple[str, ...] = ("account_id",)
    if any(str(binding.get(field_name) or "") for field_name in strategy_fields):
        expected_fields = (*strategy_fields, *expected_fields)
        # ``OrderIntent.strategy_id`` is the operational strategy identity.
        # Validate it independently from the isolated permit-claim metadata
        # below; otherwise a valid metadata triple could mask a mutated
        # top-level strategy and send the order to the wrong sleeve/session.
        strategy_id = str(intent.get("strategy_id") or "")
        expected_strategy_id = str(binding.get("strategy_artifact_id") or "")
        if strategy_id != expected_strategy_id:
            blockers.append(
                _blocker(
                    "functional-test-intent-strategy-id-mismatch",
                    f"expected={expected_strategy_id!r}, observed={strategy_id!r}",
                    source="order-intent",
                )
            )
    elif any(
        str(_first(metadata, *_INTENT_BINDING_ALIASES[field_name]) or "")
        for field_name in strategy_fields
    ):
        blockers.append(
            _blocker(
                "functional-test-intent-unbound-strategy-scope",
                "portfolio-only permit cannot accept an intent-bound strategy triple",
                source="order-intent",
            )
        )
    if bool(binding.get("portfolio_required")):
        expected_fields += (
            "portfolio_artifact_id",
            "portfolio_artifact_hash",
            "portfolio_instance_id",
        )
    for field_name in expected_fields:
        expected = str(binding.get(field_name) or "")
        observed = str(
            _first(metadata, *_INTENT_BINDING_ALIASES[field_name]) or ""
        )
        if field_name == "strategy_artifact_id" and not observed:
            observed = str(intent.get("strategy_id") or "")
        if not observed:
            blockers.append(
                _blocker(
                    f"functional-test-intent-{field_name.replace('_', '-')}-missing",
                    f"order intent must bind {field_name}",
                    source="order-intent",
                )
            )
        elif observed != expected:
            blockers.append(
                _blocker(
                    f"functional-test-intent-{field_name.replace('_', '-')}-mismatch",
                    f"expected={expected!r}, observed={observed!r}",
                    source="order-intent",
                )
            )
    symbol = str(intent.get("symbol") or "").strip().upper()
    if symbol not in _symbols(binding.get("symbols")):
        blockers.append(
            _blocker(
                "functional-test-intent-symbol-not-permitted",
                f"symbol {symbol or '<missing>'} is outside the exact permit",
                source="order-intent",
            )
        )
    if str(binding.get("market_group") or "") == "US_STOCK":
        route_fields = {
            "market_group": "functional-test-market-group",
            "execution_route": "functional-test-execution-route",
            "settlement_currency": "functional-test-settlement-currency",
            "route_scope_hash": "functional-test-route-scope-hash",
        }
        for field_name, code_prefix in route_fields.items():
            expected = str(binding.get(field_name) or "")
            observed = str(
                _first(metadata, *_BINDING_ALIASES[field_name]) or ""
            ).strip().upper() if field_name != "route_scope_hash" else str(
                _first(metadata, *_BINDING_ALIASES[field_name]) or ""
            ).strip().lower()
            compared_expected = (
                expected.lower()
                if field_name == "route_scope_hash"
                else expected.upper()
            )
            if not observed:
                blockers.append(
                    _blocker(
                        code_prefix + "-missing",
                        f"US order intent must bind {field_name}",
                        source="order-intent",
                    )
                )
            elif observed != compared_expected:
                blockers.append(
                    _blocker(
                        code_prefix + "-mismatch",
                        f"expected={compared_expected!r}, observed={observed!r}",
                        source="order-intent",
                    )
                )
        expected_exchange = dict(
            _symbol_routes(binding.get("symbol_routes"))
        ).get(symbol, "")
        observed_exchange = str(
            _first(metadata, "exchange", "exchangeCode") or ""
        ).strip().upper()
        if not expected_exchange or observed_exchange != expected_exchange:
            blockers.append(
                _blocker(
                    "functional-test-intent-exchange-mismatch",
                    f"expected={expected_exchange!r}, observed={observed_exchange!r}",
                    source="order-intent",
                )
            )
    side = str(intent.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        blockers.append(
            _blocker(
                "functional-test-intent-side-invalid",
                "KIS functional-test order side must be BUY or SELL",
                source="order-intent",
            )
        )
    intent_environment = str(
        _first(metadata, "environment", "functional_test_environment", "functionalTestEnvironment")
        or ""
    ).strip().upper()
    if not intent_environment:
        blockers.append(
            _blocker(
                "functional-test-intent-environment-missing",
                "order intent must bind its KIS functional-test environment",
                source="order-intent",
            )
        )
    elif intent_environment != environment:
        blockers.append(
            _blocker(
                "functional-test-intent-environment-mismatch",
                f"expected={environment}, observed={intent_environment}",
                source="order-intent",
            )
        )
    return blockers


def _binding_payload(value: Any) -> dict[str, Any]:
    payload = _object_payload(value)
    snapshot = getattr(value, "snapshot", None)
    if callable(snapshot):
        snap = snapshot()
        if isinstance(snap, Mapping):
            # FunctionalTestBinding computes routeScopeHash in snapshot(); it
            # is deliberately not a caller-supplied dataclass field.
            payload = {**payload, **dict(snap)}
    result: dict[str, Any] = {}
    for field_name, aliases in _BINDING_ALIASES.items():
        raw = _first(payload, *aliases)
        if field_name in {"symbols", "exchanges"}:
            result[field_name] = _symbols(raw)
        elif field_name == "symbol_routes":
            result[field_name] = _symbol_routes(raw)
        elif field_name == "portfolio_required":
            result[field_name] = bool(raw)
        else:
            result[field_name] = str(raw or "")
    return result


def _intent_payload(intent: Any) -> dict[str, Any]:
    payload = _object_payload(intent)
    return {
        "strategy_id": _first(payload, "strategy_id", "strategyId"),
        "symbol": _first(payload, "symbol", "instrument_id", "instrumentId"),
        "side": _first(payload, "side"),
        "quantity": _first(payload, "quantity"),
        "reference_price": _first(payload, "reference_price", "referencePrice"),
        "mode": _first(payload, "mode"),
        "metadata": _object_payload(_first(payload, "metadata") or {}),
    }


def _object_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        return dict(payload) if isinstance(payload, Mapping) else {}
    if is_dataclass(value):
        payload = asdict(value)
        return dict(payload) if isinstance(payload, dict) else {}
    values = getattr(value, "__dict__", None)
    return dict(values) if isinstance(values, dict) else {}


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _enum_text(value: Any) -> str:
    member_value = getattr(value, "value", None)
    return str(member_value if member_value is not None else value or "")


def _first(payload: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
    return None


def _symbols(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = list(value)
    else:
        values = []
    return tuple(
        sorted(
            {
                str(item).strip().upper()
                for item in values
                if str(item).strip()
            }
        )
    )


def _symbol_routes(value: Any) -> tuple[tuple[str, str], ...]:
    values = list(value) if isinstance(value, (list, tuple)) else []
    routes: list[tuple[str, str]] = []
    for item in values:
        if isinstance(item, Mapping):
            symbol = str(item.get("symbol") or "").strip().upper()
            exchange = str(item.get("exchange") or "").strip().upper()
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            symbol = str(item[0] or "").strip().upper()
            exchange = str(item[1] or "").strip().upper()
        else:
            continue
        if symbol and exchange:
            routes.append((symbol, exchange))
    return tuple(sorted(set(routes)))


def _numeric_cap(payload: Mapping[str, Any], name: str) -> float | None:
    value = _first(payload, *_CAP_ALIASES[name])
    number = _finite_number(value)
    if number is None or number <= 0:
        return None
    if name in {
        "max_order_quantity",
        "max_orders",
        "max_open_positions",
    } and not number.is_integer():
        return None
    return number


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _finite_integer(value: Any) -> int | None:
    number = _finite_number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _non_negative_int(value: Any) -> int:
    parsed = _finite_integer(value)
    return max(0, parsed) if parsed is not None else 0


def _iso_value(value: Any) -> str:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value or "")


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _blocker(
    code: str,
    detail: str,
    *,
    source: str = "live-functional-test",
) -> FunctionalTestBlocker:
    return FunctionalTestBlocker(code=code, detail=detail, source=source)


def _contract_blocker(exc: Exception, context: str) -> FunctionalTestBlocker:
    code = str(getattr(exc, "code", "") or "").strip()
    detail = str(getattr(exc, "detail", "") or str(exc)).strip()
    return _blocker(
        code or f"functional-test-{context}-invalid",
        detail or f"{context} failed shared contract validation",
        source="shared-contract",
    )


def _dedupe_blockers(
    blockers: list[FunctionalTestBlocker],
) -> list[FunctionalTestBlocker]:
    seen: set[str] = set()
    result: list[FunctionalTestBlocker] = []
    for blocker in blockers:
        if blocker.code in seen:
            continue
        seen.add(blocker.code)
        result.append(blocker)
    return result


__all__ = [
    "FUNCTIONAL_TEST_EVIDENCE_CLASS",
    "FUNCTIONAL_TEST_END_PLAN_SCHEMA_VERSION",
    "FUNCTIONAL_TEST_SAFETY_REPORT_SCHEMA_VERSION",
    "FunctionalTestBlocker",
    "FunctionalTestEndAction",
    "FunctionalTestEndPlan",
    "FunctionalTestRiskSnapshot",
    "FunctionalTestSafetyReport",
    "build_functional_test_end_plan",
    "evaluate_functional_test_order",
]
