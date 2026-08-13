from __future__ import annotations

import hashlib
import hmac
import json
import math
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


def _ensure_shared_runtime_path() -> None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "packages" / "trading_runtime"
        if candidate.exists():
            path = str(candidate)
            if path not in sys.path:
                sys.path.insert(0, path)
            return


_ensure_shared_runtime_path()

from trading_runtime.artifact_governance import (  # noqa: E402
    compute_strategy_artifact_hash,
    compute_strategy_instance_hash,
    verify_strategy_artifact,
    verify_strategy_instance,
)
from trading_runtime.market_calendar import session_bounds_utc  # noqa: E402


KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_REAL_E2E_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
PDNO = "010140"
BAR_INTERVAL_MINUTES = 5
ORDER_QUANTITY = 1
MAX_ORDER_KRW = Decimal("100000")
MAX_GROSS_KRW = Decimal("100000")
OWNER_LOSS_LIMIT_KRW = Decimal("5000")
ACTIVE_SECONDS = 7200
KST = ZoneInfo("Asia/Seoul")
REGULAR_OPEN = time(9, 0)
ARMED_LATEST = time(13, 15)
ACTIVE_END_LATEST = time(15, 15)
CLEANUP_END_LATEST = time(15, 30)

APPROVED_ARTIFACT_PATH = Path(
    r"C:\Users\youwo\AppData\Local\trading-system\artifacts\strategy-core\ft-probe-kr-stock-010140-5m-20260810-multiasset-functional-v4-98a1d9d901.json"
)
APPROVED_INSTANCE_PATH = Path(
    r"C:\Users\youwo\AppData\Local\trading-system\artifacts\strategy-core\strategy-instances\si-ft-probe-kr-stock-010140-5m-20260810-multiasset-functional-v4-dfc9a22bfe.json"
)
APPROVED_ARTIFACT_ID = "ft-probe-kr-stock-010140-5m-20260810-multiasset-functional-v4"
APPROVED_INSTANCE_ID = "si-ft-probe-kr-stock-010140-5m-20260810-multiasset-functional-v4"
APPROVED_ARTIFACT_CONTENT_HASH = "d62f00dc2a3f0a53682f0d7d0bd0550dd7f9fcacc3daed713ee42c3ff45c9e78"
APPROVED_ARTIFACT_FILE_SHA256 = "c8c3b5271dad40b8f7caa00f5d088723128ef46bc9a8b60a5da0e87ff0d25536"
APPROVED_INSTANCE_CONTENT_HASH = "a1d39dac2d0f2984daa4e2e8e72cd9388da85b7eb1eac6aa37bfd207b6dd1004"
APPROVED_INSTANCE_FILE_SHA256 = "60c2e6a41103600c0dc61b7fc71f952961ce4b329b88587403fca762efebc095"


class KisDomesticFunctionalContractBlocked(RuntimeError):
    pass


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_content_hash(value: Any) -> str:
    return sha256_hex(canonical_json_bytes(value))


def _exact(value: Any, expected: Any, label: str) -> None:
    if type(value) is not type(expected) or value != expected:
        raise KisDomesticFunctionalContractBlocked(f"approved publication {label} changed")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalContractBlocked(f"{label} must be an object")
    return value


def _parse_json(raw: bytes, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KisDomesticFunctionalContractBlocked(f"{label} JSON is invalid") from exc
    return _object(value, label)


def _validate_approved_semantics(
    artifact: Mapping[str, Any],
    instance: Mapping[str, Any],
) -> None:
    for key in ("id", "strategy_id"):
        _exact(artifact.get(key), APPROVED_ARTIFACT_ID, f"artifact.{key}")
    exact_artifact = {
        "artifact_schema_version": "strategy-artifact-v2",
        "asset": "KR_STOCK",
        "assetGroup": "kr-stock",
        "instrumentType": "KR_STOCK",
        "symbol": PDNO,
        "timeframe": "5m",
        "plugin": "breakout",
        "strategyId": "breakout",
        "order_quantity": 1,
        "evidenceClass": "FUNCTIONAL_TEST_NON_PROMOTION",
        "executionPurpose": "FUNCTIONAL_TEST",
        "promotionEligible": False,
        "useAsPromotionEvidence": False,
        "fullLiveAllowed": False,
    }
    for key, expected in exact_artifact.items():
        _exact(artifact.get(key), expected, f"artifact.{key}")
    parameters = _object(artifact.get("parameters"), "artifact.parameters")
    _exact(parameters.get("breakoutWindow"), 10, "artifact.parameters.breakoutWindow")
    _exact(parameters.get("breakoutK"), 0.3, "artifact.parameters.breakoutK")
    _exact(parameters.get("paperOrderQuantity"), 1, "artifact.parameters.paperOrderQuantity")
    settings = _object(artifact.get("settings"), "artifact.settings")
    _exact(settings.get("executionTiming"), "next-open", "artifact.settings.executionTiming")
    execution = _object(artifact.get("execution"), "artifact.execution")
    _exact(execution.get("timing"), "next-open", "artifact.execution.timing")
    trader = _object(artifact.get("traderContract"), "artifact.traderContract")
    for key, expected in {
        "realtimeMarketDataProvider": "kis",
        "transportProvider": "kis",
        "brokerId": "kis",
        "canGenerateSignals": True,
        "canPlaceOrders": False,
    }.items():
        _exact(trader.get(key), expected, f"artifact.traderContract.{key}")
    runtime_market = _object(trader.get("runtimeMarketDataContract"), "runtimeMarketDataContract")
    _exact(runtime_market.get("closedBarRequired"), True, "closedBarRequired")
    _exact(
        runtime_market.get("openBoundaryAttestationRequired"),
        "KIS_WEBSOCKET",
        "openBoundaryAttestationRequired",
    )
    sizing = _object(trader.get("executionSizing"), "executionSizing")
    _exact(sizing.get("mode"), "fixed_quantity", "executionSizing.mode")
    _exact(sizing.get("paperOrderQuantity"), 1, "executionSizing.paperOrderQuantity")
    _exact(sizing.get("fractionalAllowed"), False, "executionSizing.fractionalAllowed")
    permissions = _object(artifact.get("permissions"), "artifact.permissions")
    for key in ("live_small_eligible", "live_eligible", "live_allowed"):
        _exact(permissions.get(key), False, f"artifact.permissions.{key}")
    _exact(
        permissions.get("fail_reasons"),
        ["functional-test-non-promotion"],
        "artifact.permissions.fail_reasons",
    )
    scope = _object(artifact.get("scope"), "artifact.scope")
    for key, expected in {
        "allowed_symbols": [PDNO],
        "allowed_asset_classes": ["KR_STOCK"],
        "allowed_timeframes": ["5m"],
        "allowed_brokers": ["kis"],
    }.items():
        _exact(scope.get(key), expected, f"artifact.scope.{key}")

    exact_instance = {
        "instanceId": APPROVED_INSTANCE_ID,
        "pluginId": "breakout",
        "sourceStrategyId": APPROVED_ARTIFACT_ID,
        "sourceArtifactHash": APPROVED_ARTIFACT_CONTENT_HASH,
        "qualifiedSymbol": PDNO,
        "qualifiedTimeframe": "5m",
        "asset": "KR_STOCK",
        "assetGroup": "kr-stock",
        "instrumentType": "KR_STOCK",
        "realtimeMarketDataProvider": "kis",
        "brokerId": "kis",
        "schemaVersion": "strategy-instance-template-v1",
        "immutable": True,
        "artifactHash": APPROVED_INSTANCE_CONTENT_HASH,
    }
    for key, expected in exact_instance.items():
        _exact(instance.get(key), expected, f"instance.{key}")
    instance_parameters = _object(instance.get("parameters"), "instance.parameters")
    _exact(instance_parameters.get("breakoutWindow"), 10, "instance.parameters.breakoutWindow")
    _exact(instance_parameters.get("breakoutK"), 0.3, "instance.parameters.breakoutK")
    _exact(instance_parameters.get("paperOrderQuantity"), 1, "instance.parameters.paperOrderQuantity")
    instance_runtime_market = _object(
        instance.get("runtimeMarketDataContract"),
        "instance.runtimeMarketDataContract",
    )
    _exact(instance_runtime_market.get("closedBarRequired"), True, "instance.closedBarRequired")
    _exact(
        instance_runtime_market.get("openBoundaryAttestationRequired"),
        "KIS_WEBSOCKET",
        "instance.openBoundaryAttestationRequired",
    )
    qualification = _object(instance.get("qualification"), "instance.qualification")
    _exact(qualification.get("promotionEligible"), False, "instance.qualification.promotionEligible")
    _exact(
        qualification.get("evidenceClass"),
        "FUNCTIONAL_TEST_NON_PROMOTION",
        "instance.qualification.evidenceClass",
    )


@dataclass(frozen=True)
class KisDomesticPublicationSeal:
    artifact: Mapping[str, Any]
    instance: Mapping[str, Any]
    artifact_content_hash: str
    artifact_file_sha256: str
    instance_content_hash: str
    instance_file_sha256: str
    contract_envelope: Mapping[str, Any]
    contract_envelope_hash: str


def verify_kis_domestic_functional_publication() -> KisDomesticPublicationSeal:
    try:
        artifact_raw = APPROVED_ARTIFACT_PATH.read_bytes()
        instance_raw = APPROVED_INSTANCE_PATH.read_bytes()
    except OSError as exc:
        raise KisDomesticFunctionalContractBlocked("approved KIS publication files are unavailable") from exc
    if not hmac.compare_digest(sha256_hex(artifact_raw), APPROVED_ARTIFACT_FILE_SHA256):
        raise KisDomesticFunctionalContractBlocked("approved artifact raw file SHA changed")
    if not hmac.compare_digest(sha256_hex(instance_raw), APPROVED_INSTANCE_FILE_SHA256):
        raise KisDomesticFunctionalContractBlocked("approved instance raw file SHA changed")
    artifact = _parse_json(artifact_raw, "approved artifact")
    instance = _parse_json(instance_raw, "approved instance")
    artifact_verification = verify_strategy_artifact(dict(artifact))
    instance_verification = verify_strategy_instance(dict(instance))
    if not artifact_verification.valid or not instance_verification.valid:
        raise KisDomesticFunctionalContractBlocked("trading_runtime canonical lock verification failed")
    artifact_hash = compute_strategy_artifact_hash(dict(artifact))
    instance_hash = compute_strategy_instance_hash(dict(instance))
    if not hmac.compare_digest(artifact_hash, APPROVED_ARTIFACT_CONTENT_HASH):
        raise KisDomesticFunctionalContractBlocked("approved artifact canonical hash changed")
    if not hmac.compare_digest(instance_hash, APPROVED_INSTANCE_CONTENT_HASH):
        raise KisDomesticFunctionalContractBlocked("approved instance canonical hash changed")
    _validate_approved_semantics(artifact, instance)
    envelope = {
        "schemaVersion": "kis-domestic-functional-contract/v1",
        "route": ROUTE,
        "origin": LIVE_ORIGIN,
        "pdno": PDNO,
        "barIntervalMinutes": BAR_INTERVAL_MINUTES,
        "orderQuantity": ORDER_QUANTITY,
        "maxOrderKrw": "100000",
        "maxGrossKrw": "100000",
        "ownerLossMustRemainBelowKrw": "5000",
        "armedPublicDataOnly": True,
        "armedLatestKst": "13:15:00",
        "activeSeconds": ACTIVE_SECONDS,
        "activeEndLatestKst": "15:15:00",
        "cleanupEndLatestKst": "15:30:00",
        "naturalSellSupported": False,
        "naturalSellTerminalOutcome": "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT",
        "productionPromotionEligible": False,
        "approvedArtifactId": APPROVED_ARTIFACT_ID,
        "approvedArtifactContentHash": artifact_hash,
        "approvedArtifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
        "approvedInstanceId": APPROVED_INSTANCE_ID,
        "approvedInstanceContentHash": instance_hash,
        "approvedInstanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
        "naturalBreakoutEvaluationRequired": True,
        "exchangeCalendarId": "XKRX",
        "officialSessionCalendarRequired": True,
        "freshSignedHolidayOpenPreflightRequired": True,
        "executionTiming": "next-open",
        "openBoundaryAttestationRequired": "KIS_WEBSOCKET",
        "freshSignedQuotePreflightRequired": True,
        "terminalVerifierAvailable": False,
    }
    return KisDomesticPublicationSeal(
        artifact=dict(artifact),
        instance=dict(instance),
        artifact_content_hash=artifact_hash,
        artifact_file_sha256=APPROVED_ARTIFACT_FILE_SHA256,
        instance_content_hash=instance_hash,
        instance_file_sha256=APPROVED_INSTANCE_FILE_SHA256,
        contract_envelope=envelope,
        contract_envelope_hash=canonical_content_hash(envelope),
    )


def _aware_kst(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalContractBlocked(f"{label} must be timezone-aware")
    converted = value.astimezone(KST)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalContractBlocked(f"{label} must be finite")
    return converted


def _parse_exact_datetime(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise KisDomesticFunctionalContractBlocked(f"{label} is missing")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise KisDomesticFunctionalContractBlocked(f"{label} is invalid") from exc
    parsed = _aware_kst(parsed, label)
    if value != parsed.isoformat():
        raise KisDomesticFunctionalContractBlocked(f"{label} is not canonical KST ISO")
    return parsed


_EVALUATION_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "artifactContentHash",
    "artifactFileSha256",
    "instanceContentHash",
    "instanceFileSha256",
    "plugin",
    "breakoutWindow",
    "breakoutK",
    "barIntervalMinutes",
    "signal",
    "barCloseAt",
    "evaluatedAt",
    "evaluationBodyHash",
}


def verify_natural_breakout_evaluation(
    value: Mapping[str, Any],
    *,
    publication: KisDomesticPublicationSeal,
) -> datetime:
    if set(value) != _EVALUATION_KEYS:
        raise KisDomesticFunctionalContractBlocked("natural evaluation fields are not exact")
    exact = {
        "schemaVersion": "kis-domestic-natural-breakout-evaluation/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "artifactContentHash": publication.artifact_content_hash,
        "artifactFileSha256": publication.artifact_file_sha256,
        "instanceContentHash": publication.instance_content_hash,
        "instanceFileSha256": publication.instance_file_sha256,
        "plugin": "breakout",
        "breakoutWindow": 10,
        "breakoutK": "0.3",
        "barIntervalMinutes": 5,
        "signal": "BUY",
    }
    for key, expected in exact.items():
        _exact(value.get(key), expected, f"evaluation.{key}")
    body = dict(value)
    claimed_hash = body.pop("evaluationBodyHash")
    if not isinstance(claimed_hash, str) or not hmac.compare_digest(
        claimed_hash, canonical_content_hash(body)
    ):
        raise KisDomesticFunctionalContractBlocked("natural evaluation hash mismatch")
    bar_close = _parse_exact_datetime(value["barCloseAt"], "barCloseAt")
    evaluated = _parse_exact_datetime(value["evaluatedAt"], "evaluatedAt")
    if bar_close != evaluated:
        raise KisDomesticFunctionalContractBlocked("evaluation is not causally sealed to exact closed bar")
    return evaluated


@dataclass(frozen=True)
class KisDomesticActivationWindow:
    trading_date: date
    armed_at: datetime
    activated_at: datetime
    active_ends_at: datetime
    cleanup_ends_at: datetime
    evaluation_hash: str
    active_seconds: int = ACTIVE_SECONDS

    def as_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": "kis-domestic-functional-activation/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "tradingDate": self.trading_date.isoformat(),
            "armedAt": self.armed_at.isoformat(),
            "activatedAt": self.activated_at.isoformat(),
            "activeEndsAt": self.active_ends_at.isoformat(),
            "cleanupEndsAt": self.cleanup_ends_at.isoformat(),
            "activeSeconds": self.active_seconds,
            "evaluationHash": self.evaluation_hash,
            "armedPublicDataOnly": True,
            "freshSignedQuotePreflightSatisfied": False,
            "productionAvailable": False,
        }


def seal_kis_domestic_activation_window(
    *,
    publication: KisDomesticPublicationSeal,
    natural_evaluation: Mapping[str, Any],
    trading_date: date,
    armed_at: datetime,
    activated_at: datetime,
) -> KisDomesticActivationWindow:
    armed = _aware_kst(armed_at, "armed_at")
    activated = _aware_kst(activated_at, "activated_at")
    evaluated = verify_natural_breakout_evaluation(natural_evaluation, publication=publication)
    try:
        session_open_utc, session_close_utc = session_bounds_utc("XKRX", trading_date)
    except ValueError as exc:
        raise KisDomesticFunctionalContractBlocked(
            "trading_date is not an official XKRX regular session"
        ) from exc
    session_open = session_open_utc.astimezone(KST)
    session_close = session_close_utc.astimezone(KST)
    if (
        session_open.date() != trading_date
        or session_open.time().replace(tzinfo=None) != REGULAR_OPEN
        or session_close.date() != trading_date
        or session_close.time().replace(tzinfo=None) != CLEANUP_END_LATEST
    ):
        raise KisDomesticFunctionalContractBlocked(
            "official XKRX session bounds are not the approved 09:00-15:30 contract"
        )
    if armed.date() != trading_date or activated.date() != trading_date:
        raise KisDomesticFunctionalContractBlocked("schedule must stay on the exact KST trading date")
    if not (session_open <= armed < session_close) or not (
        session_open <= activated < session_close
    ):
        raise KisDomesticFunctionalContractBlocked(
            "schedule is outside official XKRX regular hours"
        )
    if armed.time().replace(tzinfo=None) < REGULAR_OPEN or armed.time().replace(tzinfo=None) > ARMED_LATEST:
        raise KisDomesticFunctionalContractBlocked("ARMED must be during XKRX regular hours and no later than 13:15 KST")
    if activated < armed or activated != evaluated:
        raise KisDomesticFunctionalContractBlocked("activation is not the exact natural evaluation boundary")
    if activated.second != 0 or activated.microsecond != 0 or activated.minute % 5:
        raise KisDomesticFunctionalContractBlocked("activation must be an exact closed 5-minute boundary")
    active_ends = activated + timedelta(seconds=ACTIVE_SECONDS)
    latest_active_end = datetime.combine(trading_date, ACTIVE_END_LATEST, KST)
    cleanup_ends = datetime.combine(trading_date, CLEANUP_END_LATEST, KST)
    if active_ends > latest_active_end:
        raise KisDomesticFunctionalContractBlocked("activation+7200 exceeds 15:15 KST")
    return KisDomesticActivationWindow(
        trading_date=trading_date,
        armed_at=armed,
        activated_at=activated,
        active_ends_at=active_ends,
        cleanup_ends_at=cleanup_ends,
        evaluation_hash=str(natural_evaluation["evaluationBodyHash"]),
    )


def terminal_taxonomy_contract() -> dict[str, Any]:
    value = {
        "schemaVersion": "kis-domestic-functional-terminal-taxonomy/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "naturalSellSupported": False,
        "terminalOutcomeIfNaturalSellAbsent": "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT",
        "fullNaturalRoundTripPossible": False,
        "productionPromotionEligible": False,
        "terminalVerifierAvailable": False,
    }
    return {**value, "taxonomyHash": canonical_content_hash(value)}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False,
        "productionAvailable": KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_AVAILABLE,
        "realE2EAvailable": KIS_DOMESTIC_FUNCTIONAL_REAL_E2E_AVAILABLE,
        "networkEnabled": False,
        "mutationEnabled": False,
        "terminalVerifierAvailable": False,
        "naturalEvaluationReaderAvailable": False,
        "officialSessionCalendarRequired": True,
        "freshSignedHolidayOpenPreflightAvailable": False,
        "nextOpenWebsocketAttestationAvailable": False,
        "freshSignedQuotePreflightAvailable": False,
        "route": ROUTE,
        "pdno": PDNO,
        "reason": "GET_ONLY_CONTRACT_AND_TRUTH_OFFLINE_VALIDATION_ONLY",
    }
