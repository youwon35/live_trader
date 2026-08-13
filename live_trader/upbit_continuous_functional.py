from __future__ import annotations

"""Fail-closed Upbit KRW-BTC continuous functional-test safety core.

This module is intentionally isolated from the ordinary SMALL_LIVE dispatcher.
It accepts only an exact two-hour, non-promotional shared permit and exposes no
production activation while :data:`UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE` is
false.  Network readers and POST functions are injected so tests can exercise
the final boundary without touching a broker.

The lifecycle is BUY once -> SELL once, with no re-entry.  After stop, expiry,
or owner-loss breach it permits only bounded, risk-reducing cleanup generations
against the exact remaining session-owned delta until an authoritative final
snapshot proves the account returned to its pre-test BTC baseline.  Existing
BTC is never part of the owned delta.
"""

from contextlib import AbstractContextManager, closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any, Callable, Mapping, Protocol

from trading_runtime.functional_test import (
    FUNCTIONAL_TEST_CRYPTO_CLEANUP_MAX_HOURS,
    FUNCTIONAL_TEST_CRYPTO_DURATION_HOURS,
    FUNCTIONAL_TEST_UPBIT_MAX_GROSS_EXPOSURE_KRW,
    FUNCTIONAL_TEST_UPBIT_MAX_LOSS_KRW,
    FUNCTIONAL_TEST_UPBIT_MAX_ORDER_NOTIONAL_KRW,
    FunctionalTestEnvironment,
    FunctionalTestPermit,
    parse_functional_test_permit,
)


UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE = False
UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED = False
UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED = False
UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_STATUS = "HOLD"
SCHEMA_VERSION = "upbit-continuous-functional-v1"
EVIDENCE_CLASS = "FUNCTIONAL_TEST_NON_PROMOTION"
EXECUTION_PURPOSE = "FUNCTIONAL_TEST"
EXECUTION_ROUTE = "UPBIT_KRW_SPOT_CONTINUOUS"
MARKET_GROUP = "CRYPTO_SPOT"
EXCHANGE = "UPBIT_SPOT"
SYMBOL = "KRW-BTC"
SETTLEMENT_CURRENCY = "KRW"
MAX_TRUTH_AGE_SECONDS = 15
MAX_CLEANUP_ACTION_GENERATIONS = 12

ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION = (
    "upbit-functional-account-exclusivity-proof/v1"
)
ACCOUNT_EXCLUSIVITY_VERIFIER_PIN_SCHEMA_VERSION = (
    "upbit-account-exclusivity-verifier-pin/v1"
)
ACCOUNT_API_KEY_INVENTORY_SOURCE = (
    "UPBIT_AUTHENTICATED_ACCOUNT_API_KEY_INVENTORY_V1"
)
ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE = (
    "UPBIT_ACCOUNT_ALL_MARKETS_MANUAL_ORDER_AUDIT_V1"
)
ACCOUNT_BOT_REGISTRY_SOURCE = (
    "SERVER_OWNED_UPBIT_ACCOUNT_BOT_REGISTRY_V1"
)

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_ACTIVATION_CAPABILITY = object()
_TEST_CAPABILITY = object()


class UpbitFunctionalError(RuntimeError):
    pass


class UpbitFunctionalBlocked(UpbitFunctionalError):
    pass


class UpbitFunctionalAmbiguous(UpbitFunctionalError):
    pass


class UpbitBrokerPostNotSent(UpbitFunctionalError):
    """A broker adapter proved no HTTP order/cancel POST left the process."""

    pass


class AccountExclusivityProofVerifier(Protocol):
    """Pinned verifier for an authoritative detached exclusivity proof.

    ``identity`` is deliberately separate from callback implementation
    introspection.  A function/code hash cannot identify a closure, key, or
    loaded configuration.  Production must pin all four identities below and
    the live verifier must report the exact same identity at verification
    time.  With no verifier or no exact pin, proof status stays HOLD/false.
    """

    def identity(self) -> Mapping[str, Any]: ...

    def __call__(
        self,
        *,
        payload: Mapping[str, Any],
        signature: str,
        verifier_pin: Mapping[str, Any],
    ) -> bool: ...


def _text(value: object) -> str:
    return str(value or "").strip()


def _upper(value: object) -> str:
    return _text(value).upper()


def _decimal(value: object, label: str, *, minimum: Decimal | None = None) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UpbitFunctionalBlocked(f"{label}-invalid") from exc
    if not parsed.is_finite() or (minimum is not None and parsed < minimum):
        raise UpbitFunctionalBlocked(f"{label}-invalid")
    return parsed


def _decimal_text(value: Decimal) -> str:
    result = format(value.normalize(), "f")
    return "0" if result in {"", "-0"} else result


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = _text(value)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitFunctionalBlocked(f"{label}-invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitFunctionalBlocked(f"{label}-timezone-missing")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _stable_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _strict_stable_hash(value: object) -> str:
    """Hash canonical JSON only; proof verification must never stringify objects."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _exact_json_string(value: object) -> bool:
    return type(value) is str


def _exact_lower_hash(value: object) -> bool:
    return _exact_json_string(value) and _HASH_RE.fullmatch(value) is not None


def _safe_input_hash(value: object) -> str:
    """Hash hostile evidence without letting malformed JSON block cleanup."""

    try:
        return _stable_hash(value)
    except (TypeError, ValueError, RecursionError):
        return _stable_hash(
            {
                "schemaVersion": "unhashable-hostile-input/v1",
                "pythonType": (
                    f"{type(value).__module__}.{type(value).__qualname__}"
                ),
            }
        )


def _require_hash(value: object, label: str) -> str:
    if not _exact_lower_hash(value):
        raise UpbitFunctionalBlocked(f"{label}-invalid")
    return value


def _invalid_account_exclusivity_proof(
    *,
    raw_value: object,
    reason: str,
) -> tuple[dict[str, Any], str, bool]:
    """Return an immutable, primitive-shaped fail-closed terminal record."""

    component_defaults = {
        "apiKeyInventory": {
            "schemaVersion": "upbit-account-api-key-inventory-evidence/v1",
            "source": "",
            "accountFingerprint": "",
            "coverageStartedAt": "",
            "coverageEndedAt": "",
            "complete": False,
            "independentlyVerified": False,
            "continuousCoverage": False,
            "activeApiKeyCount": -1,
            "authorizedFunctionalApiKeyCount": -1,
            "otherActiveApiKeyCount": -1,
            "authorityArtifactHash": "",
            "evidenceHash": "",
        },
        "manualTradeAudit": {
            "schemaVersion": "upbit-account-manual-trade-audit-evidence/v1",
            "source": "",
            "accountFingerprint": "",
            "coverageStartedAt": "",
            "coverageEndedAt": "",
            "complete": False,
            "independentlyVerified": False,
            "continuousCoverage": False,
            "manualOrderCount": -1,
            "authorityArtifactHash": "",
            "evidenceHash": "",
        },
        "botRegistry": {
            "schemaVersion": "upbit-account-bot-registry-evidence/v1",
            "source": "",
            "accountFingerprint": "",
            "coverageStartedAt": "",
            "coverageEndedAt": "",
            "complete": False,
            "independentlyVerified": False,
            "continuousCoverage": False,
            "activeBotCount": -1,
            "authorizedFunctionalBotCount": -1,
            "otherActiveBotCount": -1,
            "authorityArtifactHash": "",
            "evidenceHash": "",
        },
    }
    normalized = {
        "schemaVersion": ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION,
        "verificationState": "SAFE_INCOMPLETE",
        "verificationReason": _text(reason) or "UNVERIFIABLE",
        "rawProofHash": _safe_input_hash(raw_value),
        "authorityPinned": False,
        **component_defaults,
        "payloadHash": "",
        "signature": "",
    }
    return normalized, _stable_hash(normalized), False


def _normalized_account_exclusivity_verifier_pin(
    value: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    expected_keys = {
        "schemaVersion",
        "verifierId",
        "keyId",
        "algorithm",
        "verifierType",
        "verifierCodeSha256",
        "verifierConfigSha256",
        "keyFingerprintSha256",
        "authorityPinned",
    }
    if set(value) != expected_keys:
        return None
    result = dict(value)
    if (
        any(
            not _exact_json_string(result.get(field))
            for field in expected_keys - {"authorityPinned"}
        )
        or type(result.get("authorityPinned")) is not bool
        or result.get("schemaVersion")
        != ACCOUNT_EXCLUSIVITY_VERIFIER_PIN_SCHEMA_VERSION
        or result.get("authorityPinned") is not True
        or _SAFE_ID_RE.fullmatch(_text(result.get("verifierId"))) is None
        or _SAFE_ID_RE.fullmatch(_text(result.get("keyId"))) is None
        or _SAFE_ID_RE.fullmatch(_text(result.get("algorithm"))) is None
        or _SAFE_ID_RE.fullmatch(_text(result.get("verifierType"))) is None
        or any(
            not _exact_lower_hash(result.get(field))
            for field in (
                "verifierCodeSha256",
                "verifierConfigSha256",
                "keyFingerprintSha256",
            )
        )
    ):
        return None
    for field in (
        "verifierCodeSha256",
        "verifierConfigSha256",
        "keyFingerprintSha256",
    ):
        result[field] = result[field]
    return result


def _canonical_exclusivity_component(
    value: object,
    *,
    name: str,
    schema_version: str,
    source: str,
    account_fingerprint: str,
    coverage_started_at: str,
    coverage_ended_at: str,
) -> dict[str, Any] | None:
    count_fields = {
        "apiKeyInventory": (
            "activeApiKeyCount",
            "authorizedFunctionalApiKeyCount",
            "otherActiveApiKeyCount",
        ),
        "manualTradeAudit": ("manualOrderCount",),
        "botRegistry": (
            "activeBotCount",
            "authorizedFunctionalBotCount",
            "otherActiveBotCount",
        ),
    }[name]
    common_keys = {
        "schemaVersion",
        "source",
        "accountFingerprint",
        "coverageStartedAt",
        "coverageEndedAt",
        "complete",
        "independentlyVerified",
        "continuousCoverage",
        "authorityArtifactHash",
        "evidenceHash",
    }
    if not isinstance(value, Mapping) or set(value) != common_keys | set(count_fields):
        return None
    row = dict(value)
    if (
        any(
            not _exact_json_string(row.get(field))
            for field in (
                "schemaVersion",
                "source",
                "accountFingerprint",
                "coverageStartedAt",
                "coverageEndedAt",
                "authorityArtifactHash",
                "evidenceHash",
            )
        )
        or any(
            type(row.get(field)) is not bool
            for field in (
                "complete",
                "independentlyVerified",
                "continuousCoverage",
            )
        )
        or row.get("schemaVersion") != schema_version
        or row.get("source") != source
        or not _exact_lower_hash(row.get("accountFingerprint"))
        or not hmac.compare_digest(row["accountFingerprint"], account_fingerprint)
        or row.get("coverageStartedAt") != coverage_started_at
        or row.get("coverageEndedAt") != coverage_ended_at
        or row.get("complete") is not True
        or row.get("independentlyVerified") is not True
        or row.get("continuousCoverage") is not True
        or not _exact_lower_hash(row.get("authorityArtifactHash"))
        or any(
            isinstance(row.get(field), bool)
            or not isinstance(row.get(field), int)
            or int(row[field]) < 0
            for field in count_fields
        )
    ):
        return None
    if name == "apiKeyInventory" and (
        row["activeApiKeyCount"] != 1
        or row["authorizedFunctionalApiKeyCount"] != 1
        or row["otherActiveApiKeyCount"] != 0
    ):
        return None
    if name == "manualTradeAudit" and row["manualOrderCount"] != 0:
        return None
    if name == "botRegistry" and (
        row["activeBotCount"] != 1
        or row["authorizedFunctionalBotCount"] != 1
        or row["otherActiveBotCount"] != 0
    ):
        return None
    evidence_hash = row.get("evidenceHash")
    projection = {
        key: item for key, item in row.items() if key != "evidenceHash"
    }
    if (
        not _exact_lower_hash(evidence_hash)
        or not hmac.compare_digest(evidence_hash, _strict_stable_hash(projection))
    ):
        return None
    row["evidenceHash"] = evidence_hash
    return row


def _verify_account_exclusivity_proof(
    value: object,
    *,
    session_id: str,
    account_fingerprint: str,
    session_started_at: datetime,
    observation_started_at: datetime,
    observed_at: datetime,
    verifier: AccountExclusivityProofVerifier | None,
    verifier_pin: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], str, bool]:
    """Normalize and independently verify exact signed primitive evidence."""

    if not isinstance(value, Mapping):
        return _invalid_account_exclusivity_proof(
            raw_value=value,
            reason="PROOF_MISSING",
        )
    raw = dict(value)
    top_keys = {
        "schemaVersion",
        "sessionId",
        "accountFingerprint",
        "sessionStartedAt",
        "observationStartedAt",
        "observedAt",
        "authority",
        "apiKeyInventory",
        "manualTradeAudit",
        "botRegistry",
        "payloadHash",
        "signature",
    }
    normalized_pin = _normalized_account_exclusivity_verifier_pin(verifier_pin)
    started_text = _utc_text(session_started_at)
    observation_started_text = _utc_text(observation_started_at)
    observed_text = _utc_text(observed_at)
    if (
        set(raw) != top_keys
        or any(
            not _exact_json_string(raw.get(field))
            for field in (
                "schemaVersion",
                "sessionId",
                "accountFingerprint",
                "sessionStartedAt",
                "observationStartedAt",
                "observedAt",
                "payloadHash",
                "signature",
            )
        )
        or raw.get("schemaVersion") != ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION
        or raw.get("sessionId") != session_id
        or not _exact_lower_hash(raw.get("accountFingerprint"))
        or not hmac.compare_digest(raw["accountFingerprint"], account_fingerprint)
        or raw.get("sessionStartedAt") != started_text
        or raw.get("observationStartedAt") != observation_started_text
        or raw.get("observedAt") != observed_text
        or normalized_pin is None
        or not isinstance(raw.get("authority"), Mapping)
        or dict(raw["authority"]) != normalized_pin
        or verifier is None
    ):
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="PROOF_BINDING_OR_AUTHORITY_UNVERIFIABLE",
        )
    try:
        runtime_identity = verifier.identity()
    except Exception:
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="VERIFIER_IDENTITY_UNAVAILABLE",
        )
    if (
        not isinstance(runtime_identity, Mapping)
        or _normalized_account_exclusivity_verifier_pin(runtime_identity)
        != normalized_pin
    ):
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="VERIFIER_IDENTITY_PIN_MISMATCH",
        )
    components: dict[str, dict[str, Any]] = {}
    for name, schema_version, source in (
        (
            "apiKeyInventory",
            "upbit-account-api-key-inventory-evidence/v1",
            ACCOUNT_API_KEY_INVENTORY_SOURCE,
        ),
        (
            "manualTradeAudit",
            "upbit-account-manual-trade-audit-evidence/v1",
            ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE,
        ),
        (
            "botRegistry",
            "upbit-account-bot-registry-evidence/v1",
            ACCOUNT_BOT_REGISTRY_SOURCE,
        ),
    ):
        component = _canonical_exclusivity_component(
            raw.get(name),
            name=name,
            schema_version=schema_version,
            source=source,
            account_fingerprint=account_fingerprint,
            coverage_started_at=started_text,
            coverage_ended_at=observed_text,
        )
        if component is None:
            return _invalid_account_exclusivity_proof(
                raw_value=raw,
                reason=f"{name.upper()}_PRIMITIVE_INVALID",
            )
        components[name] = component
    signature = raw.get("signature")
    payload_hash = raw.get("payloadHash")
    signed_payload = {
        key: item
        for key, item in raw.items()
        if key not in {"payloadHash", "signature"}
    }
    if (
        not _exact_lower_hash(payload_hash)
        or not hmac.compare_digest(payload_hash, _strict_stable_hash(signed_payload))
        or not signature
    ):
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="SIGNED_PAYLOAD_INVALID",
        )
    try:
        signature_valid = verifier(
            payload=signed_payload,
            signature=signature,
            verifier_pin=normalized_pin,
        )
    except Exception:
        signature_valid = False
    if signature_valid is not True:
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="SIGNATURE_INVALID",
        )
    normalized = {
        **signed_payload,
        "payloadHash": payload_hash,
        "signature": signature,
    }
    normalized["authority"] = normalized_pin
    normalized.update(components)
    if normalized != raw:
        return _invalid_account_exclusivity_proof(
            raw_value=raw,
            reason="PROOF_NOT_CANONICAL",
        )
    return normalized, _strict_stable_hash(normalized), True


def _account_exclusivity_evidence_complete(
    value: Mapping[str, Any],
    *,
    verifier: AccountExclusivityProofVerifier | None,
    verifier_pin: Mapping[str, Any] | None,
) -> bool:
    """Re-verify an immutable terminal proof; summary booleans are ignored."""

    try:
        proof = value.get("accountExclusivityProof")
        if not isinstance(proof, Mapping):
            return False
        normalized, proof_hash, verified = _verify_account_exclusivity_proof(
            proof,
            session_id=_text(value.get("sessionId")),
            account_fingerprint=_require_hash(
                value.get("accountFingerprint"),
                "upbit-terminal-account-fingerprint",
            ),
            session_started_at=_utc(
                value.get("activatedAt"),
                "upbit-terminal-activation",
            ),
            observation_started_at=_utc(
                value.get("terminalObservationStartedAt"),
                "upbit-terminal-observation-start",
            ),
            observed_at=_utc(
                value.get("finalObservedAt"),
                "upbit-terminal-observed-at",
            ),
            verifier=verifier,
            verifier_pin=verifier_pin,
        )
        return bool(
            verified
            and normalized == dict(proof)
            and _exact_lower_hash(
                value.get("accountExclusivityProofHash")
            )
            and hmac.compare_digest(
                proof_hash,
                value["accountExclusivityProofHash"],
            )
        )
    except (TypeError, ValueError, UpbitFunctionalBlocked):
        return False


@dataclass(frozen=True, slots=True)
class UpbitPermitScope:
    permit_id: str
    permit_hash: str
    strategy_artifact_id: str
    strategy_artifact_hash: str
    strategy_artifact_file_sha256: str
    strategy_instance_id: str
    strategy_instance_hash: str
    strategy_instance_file_sha256: str
    publication_proof_hash: str
    publication_proof_file_sha256: str
    portfolio_artifact_id: str
    portfolio_artifact_hash: str
    portfolio_instance_id: str
    account_fingerprint: str
    route_scope_hash: str
    starts_at: datetime
    ends_at: datetime
    cleanup_deadline: datetime
    max_order_notional: Decimal
    max_gross_exposure: Decimal
    max_loss: Decimal

    @classmethod
    def parse(
        cls,
        value: FunctionalTestPermit | Mapping[str, Any],
        *,
        immutable_selection: Mapping[str, Any],
    ) -> "UpbitPermitScope":
        permit = parse_functional_test_permit(value)
        binding = permit.binding
        caps = permit.caps
        if permit.environment is not FunctionalTestEnvironment.UPBIT_LIVE:
            raise UpbitFunctionalBlocked("upbit-permit-environment-mismatch")
        if (
            permit.duration_unit.value != "HOURS"
            or permit.duration_value != FUNCTIONAL_TEST_CRYPTO_DURATION_HOURS
        ):
            raise UpbitFunctionalBlocked("upbit-permit-duration-mismatch")
        if (
            binding.market_group != MARKET_GROUP
            or binding.execution_route != EXECUTION_ROUTE
            or binding.settlement_currency != SETTLEMENT_CURRENCY
            or binding.symbols != (SYMBOL,)
            or binding.exchanges != (EXCHANGE,)
            or binding.symbol_routes != ((SYMBOL, EXCHANGE),)
        ):
            raise UpbitFunctionalBlocked("upbit-permit-route-scope-mismatch")
        if binding.portfolio_required or any(
            (
                binding.portfolio_artifact_id,
                binding.portfolio_artifact_hash,
                binding.portfolio_instance_id,
            )
        ):
            raise UpbitFunctionalBlocked("upbit-permit-standalone-strategy-required")
        identity_fields = (
            binding.strategy_artifact_id,
            binding.strategy_artifact_hash,
            binding.strategy_instance_id,
        )
        if not all(identity_fields):
            raise UpbitFunctionalBlocked("upbit-permit-artifact-instance-incomplete")
        account_fingerprint = _require_hash(
            binding.account_id,
            "upbit-account-fingerprint",
        )
        selection = dict(immutable_selection)
        selection_exact = {
            "strategyArtifactId": binding.strategy_artifact_id,
            "strategyArtifactHash": binding.strategy_artifact_hash,
            "strategyInstanceId": binding.strategy_instance_id,
            "strategyInstanceArtifactHash": binding.strategy_artifact_hash,
            "accountFingerprint": account_fingerprint,
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
            "interval": "5m",
        }
        for field, expected in selection_exact.items():
            actual = _text(selection.get(field))
            if not hmac.compare_digest(actual, expected):
                raise UpbitFunctionalBlocked(
                    f"upbit-immutable-selection-{field}-mismatch"
                )
        if selection.get("verified") is not True:
            raise UpbitFunctionalBlocked("upbit-immutable-selection-not-verified")
        strategy_instance_hash = _require_hash(
            selection.get("strategyInstanceHash"),
            "upbit-strategy-instance-hash",
        )
        strategy_artifact_file_sha256 = _require_hash(
            selection.get("strategyArtifactFileSha256"),
            "upbit-strategy-artifact-file-sha256",
        )
        strategy_instance_file_sha256 = _require_hash(
            selection.get("strategyInstanceFileSha256"),
            "upbit-strategy-instance-file-sha256",
        )
        publication_proof_hash = _require_hash(
            selection.get("publicationProofHash"),
            "upbit-publication-proof-hash",
        )
        publication_proof_file_sha256 = _require_hash(
            selection.get("publicationProofFileSha256"),
            "upbit-publication-proof-file-sha256",
        )
        if selection.get("publicationProofVerified") is not True:
            raise UpbitFunctionalBlocked(
                "upbit-publication-proof-not-verified"
            )
        if (
            selection.get("publishedActiveCatalogVisible") is not True
            or selection.get("publishedNaturalSignalsOnly") is not True
            or selection.get("publishedPromotionEligible") is not False
        ):
            raise UpbitFunctionalBlocked(
                "upbit-publication-proof-policy-invalid"
            )
        proof_exact = {
            "publishedProvider": "upbit",
            "publishedGroup": "crypto-upbit",
            "publishedSymbol": SYMBOL,
            "publishedStrategyArtifactHash": binding.strategy_artifact_hash,
            "publishedStrategyArtifactFileSha256": strategy_artifact_file_sha256,
            "publishedStrategyInstanceHash": strategy_instance_hash,
            "publishedStrategyInstanceFileSha256": strategy_instance_file_sha256,
        }
        for field, expected in proof_exact.items():
            if not hmac.compare_digest(_text(selection.get(field)), expected):
                raise UpbitFunctionalBlocked(
                    f"upbit-publication-proof-{field}-mismatch"
                )
        expected_caps = (
            (Decimal(str(caps.max_order_notional)), Decimal(str(FUNCTIONAL_TEST_UPBIT_MAX_ORDER_NOTIONAL_KRW))),
            (Decimal(str(caps.max_gross_exposure)), Decimal(str(FUNCTIONAL_TEST_UPBIT_MAX_GROSS_EXPOSURE_KRW))),
            (Decimal(str(caps.max_loss)), Decimal(str(FUNCTIONAL_TEST_UPBIT_MAX_LOSS_KRW))),
        )
        if (
            caps.max_order_quantity != 1
            or caps.max_orders != 2
            or caps.max_open_positions != 1
            or any(actual > maximum for actual, maximum in expected_caps)
        ):
            raise UpbitFunctionalBlocked("upbit-permit-cap-mismatch")
        return cls(
            permit_id=permit.permit_id,
            permit_hash=permit.content_hash,
            strategy_artifact_id=binding.strategy_artifact_id,
            strategy_artifact_hash=binding.strategy_artifact_hash,
            strategy_artifact_file_sha256=strategy_artifact_file_sha256,
            strategy_instance_id=binding.strategy_instance_id,
            strategy_instance_hash=strategy_instance_hash,
            strategy_instance_file_sha256=strategy_instance_file_sha256,
            publication_proof_hash=publication_proof_hash,
            publication_proof_file_sha256=publication_proof_file_sha256,
            portfolio_artifact_id="",
            portfolio_artifact_hash="",
            portfolio_instance_id="",
            account_fingerprint=account_fingerprint,
            route_scope_hash=binding.snapshot()["routeScopeHash"],
            starts_at=permit.starts_at,
            ends_at=permit.ends_at,
            cleanup_deadline=permit.starts_at
            + timedelta(hours=FUNCTIONAL_TEST_CRYPTO_CLEANUP_MAX_HOURS),
            max_order_notional=Decimal(str(caps.max_order_notional)),
            max_gross_exposure=Decimal(str(caps.max_gross_exposure)),
            max_loss=Decimal(str(caps.max_loss)),
        )

    def snapshot(self) -> dict[str, str]:
        return {
            "permitId": self.permit_id,
            "permitHash": self.permit_hash,
            "strategyArtifactId": self.strategy_artifact_id,
            "strategyArtifactHash": self.strategy_artifact_hash,
            "strategyArtifactFileSha256": self.strategy_artifact_file_sha256,
            "strategyInstanceId": self.strategy_instance_id,
            "strategyInstanceHash": self.strategy_instance_hash,
            "strategyInstanceFileSha256": self.strategy_instance_file_sha256,
            "publicationProofHash": self.publication_proof_hash,
            "publicationProofFileSha256": self.publication_proof_file_sha256,
            "portfolioArtifactId": self.portfolio_artifact_id,
            "portfolioArtifactHash": self.portfolio_artifact_hash,
            "portfolioInstanceId": self.portfolio_instance_id,
            "accountFingerprint": self.account_fingerprint,
            "routeScopeHash": self.route_scope_hash,
            "startsAt": _utc_text(self.starts_at),
            "endsAt": _utc_text(self.ends_at),
            "cleanupDeadline": _utc_text(self.cleanup_deadline),
            "maxOrderNotional": _decimal_text(self.max_order_notional),
            "maxGrossExposure": _decimal_text(self.max_gross_exposure),
            "maxLoss": _decimal_text(self.max_loss),
        }


@dataclass(frozen=True, slots=True)
class FinalizedFiveMinuteBar:
    bar_id: str
    bar_hash: str
    closed_at: datetime
    signal: str
    evaluation_id: str
    evaluation_observed_at: datetime
    evaluation_json: str
    evaluation_hash: str

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        now: datetime,
        strategy_artifact_id: str,
        strategy_artifact_hash: str,
        strategy_artifact_file_sha256: str,
        strategy_instance_id: str,
        strategy_instance_hash: str,
        strategy_instance_file_sha256: str,
        publication_proof_hash: str,
        publication_proof_file_sha256: str,
    ) -> "FinalizedFiveMinuteBar":
        if _text(value.get("schemaVersion")) != "upbit-natural-ma-evaluation/v1":
            raise UpbitFunctionalBlocked("upbit-strategy-evaluation-schema-invalid")
        if _upper(value.get("symbol")) != SYMBOL:
            raise UpbitFunctionalBlocked("upbit-bar-symbol-mismatch")
        if _text(value.get("interval")).lower() not in {"5m", "5min", "minute5"}:
            raise UpbitFunctionalBlocked("upbit-bar-interval-mismatch")
        if value.get("finalized") is not True or value.get("closed") is not True:
            raise UpbitFunctionalBlocked("upbit-bar-not-finalized")
        if _upper(value.get("source")) not in {"UPBIT_WEBSOCKET", "UPBIT_REST"}:
            raise UpbitFunctionalBlocked("upbit-bar-source-not-official")
        bar_id = _text(value.get("barId"))
        if not _SAFE_ID_RE.fullmatch(bar_id):
            raise UpbitFunctionalBlocked("upbit-bar-id-invalid")
        bar_hash = _require_hash(value.get("barHash"), "upbit-bar-hash")
        closed_at = _utc(value.get("closedAt"), "upbit-bar-closed-at")
        current = _utc(now, "upbit-current-time")
        if closed_at > current or current - closed_at > timedelta(minutes=10):
            raise UpbitFunctionalBlocked("upbit-bar-stale-or-future")
        if (
            closed_at.second != 0
            or closed_at.microsecond != 0
            or closed_at.minute % 5 != 0
        ):
            raise UpbitFunctionalBlocked("upbit-bar-not-exact-five-minute-boundary")
        signal = _upper(value.get("signal"))
        if signal not in {"BUY", "SELL", "HOLD"}:
            raise UpbitFunctionalBlocked("upbit-bar-signal-invalid")
        evaluation_id = _text(value.get("evaluationId"))
        if not _SAFE_ID_RE.fullmatch(evaluation_id):
            raise UpbitFunctionalBlocked("upbit-strategy-evaluation-id-invalid")
        if (
            value.get("strategyEvaluationComplete") is not True
            or value.get("naturalSignal") is not True
            or value.get("forcedSignal") is not False
            or value.get("signalOverrideUsed") is not False
            or value.get("manualSignal") is not False
            or _text(value.get("strategyArtifactId"))
            != strategy_artifact_id
            or _text(value.get("strategyArtifactHash")).lower()
            != strategy_artifact_hash
            or _text(value.get("strategyArtifactFileSha256")).lower()
            != strategy_artifact_file_sha256
            or _text(value.get("strategyInstanceId"))
            != strategy_instance_id
            or _text(value.get("strategyInstanceHash")).lower()
            != strategy_instance_hash
            or _text(value.get("strategyInstanceFileSha256")).lower()
            != strategy_instance_file_sha256
            or _text(value.get("publicationProofHash")).lower()
            != publication_proof_hash
            or _text(value.get("publicationProofFileSha256")).lower()
            != publication_proof_file_sha256
            or _text(value.get("strategyPluginId"))
            != "moving_average_cross"
            or int(value.get("strategyShortMa") or 0) != 3
            or int(value.get("strategyLongMa") or 0) != 10
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-provenance-invalid"
            )
        raw_window = value.get("rawFinalizedWindow")
        if not isinstance(raw_window, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-raw-window-missing"
            )
        raw_fields = {
            "schemaVersion",
            "symbol",
            "interval",
            "source",
            "finalized",
            "closed",
            "barId",
            "closedAt",
            "bars",
            "officialCandleEvidence",
        }
        rows = raw_window.get("bars")
        if (
            set(raw_window) != raw_fields
            or _text(raw_window.get("schemaVersion"))
            != "upbit-official-finalized-5m-window-v1"
            or _upper(raw_window.get("symbol")) != SYMBOL
            or _text(raw_window.get("interval")).lower() != "5m"
            or _upper(raw_window.get("source")) != _upper(value.get("source"))
            or raw_window.get("finalized") is not True
            or raw_window.get("closed") is not True
            or not isinstance(rows, list)
            or len(rows) != 11
            or any(not isinstance(row, Mapping) for row in rows)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-raw-window-invalid"
            )
        parsed_rows: list[tuple[str, datetime, Decimal]] = []
        for row in rows:
            if set(row) != {
                "barId",
                "closedAt",
                "close",
                "finalized",
                "closed",
            }:
                raise UpbitFunctionalBlocked(
                    "upbit-strategy-natural-signal-raw-bar-invalid"
                )
            row_id = _text(row.get("barId"))
            row_closed_at = _utc(
                row.get("closedAt"), "upbit-natural-raw-bar-closed-at"
            )
            if (
                _SAFE_ID_RE.fullmatch(row_id) is None
                or row.get("finalized") is not True
                or row.get("closed") is not True
                or row_closed_at.second != 0
                or row_closed_at.microsecond != 0
                or row_closed_at.minute % 5 != 0
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-strategy-natural-signal-raw-bar-invalid"
                )
            close = _decimal(
                row.get("close"), "upbit-natural-raw-close", minimum=Decimal("0.00000001")
            )
            parsed_rows.append((row_id, row_closed_at, close))
        official = raw_window.get("officialCandleEvidence")
        raw_response = (
            official.get("rawResponse")
            if isinstance(official, Mapping)
            else None
        )
        if (
            not isinstance(official, Mapping)
            or set(official)
            != {
                "schemaVersion",
                "origin",
                "endpoint",
                "orderedQuery",
                "observedAt",
                "maxResponseTimestampMs",
                "rawResponse",
                "rawResponseHash",
            }
            or _text(official.get("schemaVersion"))
            != "upbit-official-candle-rest-evidence/v1"
            or _text(official.get("origin")) != "https://api.upbit.com"
            or _text(official.get("endpoint")) != "/v1/candles/minutes/5"
            or official.get("orderedQuery")
            != [["market", SYMBOL], ["count", "20"]]
            or not isinstance(raw_response, list)
            or not 11 <= len(raw_response) <= 20
            or any(not isinstance(row, Mapping) for row in raw_response)
            or _require_hash(
                official.get("rawResponseHash"),
                "upbit-natural-raw-response-hash",
            )
            != _stable_hash(raw_response)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-official-candle-evidence-invalid"
            )
        observed_at = _utc(
            official.get("observedAt"), "upbit-natural-candle-observed-at"
        )
        if observed_at > current + timedelta(seconds=15) or current - observed_at > timedelta(seconds=15):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-official-candle-observation-stale"
            )
        raw_by_start: dict[datetime, tuple[int, dict[str, Any]]] = {}
        response_timestamps: list[int] = []
        for raw_row in raw_response:
            if _upper(raw_row.get("market")) != SYMBOL:
                raise UpbitFunctionalBlocked(
                    "upbit-strategy-natural-signal-raw-market-mismatch"
                )
            try:
                opened_at = datetime.fromisoformat(
                    _text(raw_row.get("candle_date_time_utc"))
                ).replace(tzinfo=timezone.utc)
                response_timestamp = int(raw_row.get("timestamp"))
            except (TypeError, ValueError) as exc:
                raise UpbitFunctionalBlocked(
                    "upbit-strategy-natural-signal-raw-candle-invalid"
                ) from exc
            close = _decimal(
                raw_row.get("trade_price"),
                "upbit-natural-raw-trade-price",
                minimum=Decimal("0.00000001"),
            )
            closed = opened_at + timedelta(minutes=5)
            if (
                opened_at in raw_by_start
                or opened_at.second != 0
                or opened_at.microsecond != 0
                or opened_at.minute % 5 != 0
                or response_timestamp <= 0
                or datetime.fromtimestamp(
                    response_timestamp / 1000, tz=timezone.utc
                )
                > observed_at + timedelta(seconds=15)
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-strategy-natural-signal-raw-candle-invalid"
                )
            response_timestamps.append(response_timestamp)
            if closed <= observed_at:
                raw_by_start[opened_at] = (
                    response_timestamp,
                    {
                        "barId": "upbit-rest-five-minute-"
                        + opened_at.strftime("%Y%m%dT%H%M%SZ"),
                        "closedAt": _utc_text(closed),
                        "close": _decimal_text(close),
                        "finalized": True,
                        "closed": True,
                    },
                )
        independently_normalized = [
            raw_by_start[key][1] for key in sorted(raw_by_start)
        ][-11:]
        if (
            len(independently_normalized) != 11
            or independently_normalized != [dict(row) for row in rows]
            or int(official.get("maxResponseTimestampMs") or 0)
            != max(response_timestamps)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-raw-candle-reduction-mismatch"
            )
        if any(
            current[1] - previous[1] != timedelta(minutes=5)
            for previous, current in zip(parsed_rows, parsed_rows[1:])
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-window-not-contiguous"
            )
        final_id, final_closed_at, _ = parsed_rows[-1]
        if (
            _text(raw_window.get("barId")) != final_id
            or _utc(
                raw_window.get("closedAt"),
                "upbit-natural-raw-window-closed-at",
            )
            != final_closed_at
            or final_id != bar_id
            or final_closed_at != closed_at
            or not secrets.compare_digest(_stable_hash(dict(raw_window)), bar_hash)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-window-identity-mismatch"
            )
        closes = [row[2] for row in parsed_rows]
        previous_short = sum(closes[-4:-1], Decimal("0")) / Decimal("3")
        previous_long = sum(closes[-11:-1], Decimal("0")) / Decimal("10")
        current_short = sum(closes[-3:], Decimal("0")) / Decimal("3")
        current_long = sum(closes[-10:], Decimal("0")) / Decimal("10")
        derived_signal = (
            "BUY"
            if previous_short <= previous_long and current_short > current_long
            else "SELL"
            if previous_short >= previous_long and current_short < current_long
            else "HOLD"
        )
        expected_evaluation_id = "upbit-ma-eval-" + _stable_hash(
            {
                "windowHash": bar_hash,
                "strategyArtifactHash": strategy_artifact_hash,
                "strategyInstanceHash": strategy_instance_hash,
            }
        )[:32]
        if derived_signal != signal or not secrets.compare_digest(
            expected_evaluation_id, evaluation_id
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-recompute-mismatch"
            )
        try:
            evaluation_json = json.dumps(
                dict(value),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            canonical_value = json.loads(evaluation_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-strategy-natural-signal-not-canonical"
            ) from exc
        evaluation_hash = _stable_hash(canonical_value)
        return cls(
            bar_id=bar_id,
            bar_hash=bar_hash,
            closed_at=closed_at,
            signal=signal,
            evaluation_id=evaluation_id,
            evaluation_observed_at=observed_at,
            evaluation_json=evaluation_json,
            evaluation_hash=evaluation_hash,
        )


@dataclass(frozen=True, slots=True)
class UpbitOrderRules:
    bid_min_total: Decimal
    ask_min_total: Decimal
    quantity_step: Decimal
    quantity_scale: int
    bid_fee_rate: Decimal
    ask_fee_rate: Decimal


@dataclass(frozen=True, slots=True)
class UpbitTruth:
    observed_at: datetime
    observation_started_at: datetime
    quote_available: Decimal
    base_available: Decimal
    base_total: Decimal
    rules: UpbitOrderRules
    open_orders: tuple[dict[str, Any], ...]
    closed_orders: tuple[dict[str, Any], ...]
    fills: tuple[dict[str, Any], ...]
    total_fees: Decimal
    mark_price: Decimal
    identifier_truth: dict[str, dict[str, Any] | None]
    private_stream_events: tuple[dict[str, Any], ...]
    private_stream_recovery: bool
    private_stream_writer_generation: int
    private_stream_revision: int
    private_stream_event_cursor: int
    private_stream_last_event_id: str
    private_stream_event_head_hash: str
    account_rows: tuple[dict[str, str], ...]
    account_rows_hash: str
    account_external_activity_absent: bool
    account_exclusivity_proof: dict[str, Any]
    account_exclusivity_proof_hash: str
    account_exclusivity_proof_verified: bool
    official_rest_raw_snapshot: dict[str, Any]
    official_rest_raw_snapshot_hash: str

    @classmethod
    def parse(
        cls,
        value: Mapping[str, Any],
        *,
        account_fingerprint: str,
        now: datetime,
        session_id: str = "",
        session_started_at: datetime | None = None,
        account_exclusivity_verifier: (
            AccountExclusivityProofVerifier | None
        ) = None,
        account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
    ) -> "UpbitTruth":
        if _upper(value.get("broker")) != "UPBIT":
            raise UpbitFunctionalBlocked("upbit-truth-broker-mismatch")
        if _upper(value.get("market")) != SYMBOL:
            raise UpbitFunctionalBlocked("upbit-truth-market-mismatch")
        if not hmac.compare_digest(
            _require_hash(value.get("accountFingerprint"), "upbit-truth-account-fingerprint"),
            account_fingerprint,
        ):
            raise UpbitFunctionalBlocked("upbit-truth-account-mismatch")
        observed_at = _utc(value.get("observedAt"), "upbit-truth-observed-at")
        observation_started_at = _utc(
            value.get("observationStartedAt"),
            "upbit-truth-observation-started-at",
        )
        current = _utc(now, "upbit-current-time")
        age = (current - observed_at).total_seconds()
        if age < 0 or age > MAX_TRUTH_AGE_SECONDS:
            raise UpbitFunctionalBlocked("upbit-truth-stale-or-future")
        read_duration = _decimal(
            value.get("truthReadDurationSeconds"),
            "upbit-truth-read-duration",
            minimum=Decimal("0"),
        )
        if (
            observed_at < observation_started_at
            or observed_at - observation_started_at > timedelta(seconds=15)
            or read_duration
            != Decimal(str((observed_at - observation_started_at).total_seconds()))
        ):
            raise UpbitFunctionalBlocked(
                "upbit-truth-read-duration-invalid"
            )
        required_true = (
            "accountComplete",
            "openOrdersComplete",
            "closedOrdersComplete",
            "fillsComplete",
            "feesComplete",
            "orderChanceComplete",
            "tickerComplete",
            "identifierTruthComplete",
            "accountExternalActivityAbsent",
        )
        for field in required_true:
            if value.get(field) is not True:
                raise UpbitFunctionalBlocked(f"upbit-truth-{field}-required")
        if _text(value.get("accountSource")) != "GET /v1/accounts":
            raise UpbitFunctionalBlocked("upbit-account-source-invalid")
        if _text(value.get("orderChanceSource")) != "GET /v1/orders/chance":
            raise UpbitFunctionalBlocked("upbit-order-chance-source-invalid")
        if _text(value.get("tickerSource")) != "GET /v1/ticker":
            raise UpbitFunctionalBlocked("upbit-ticker-source-invalid")
        if (
            _text(value.get("quantityRuleSource"))
            != "UPBIT OFFICIAL MARKET ORDER 8-DECIMAL POLICY"
        ):
            raise UpbitFunctionalBlocked("upbit-quantity-rule-source-invalid")
        if _upper(value.get("openOrdersScope")) != "ACCOUNT_ALL_OPEN_ORDERS":
            raise UpbitFunctionalBlocked("upbit-open-orders-scope-incomplete")
        if _upper(value.get("closedOrdersScope")) != "ACCOUNT_SESSION_INTERVAL":
            raise UpbitFunctionalBlocked("upbit-closed-orders-scope-incomplete")
        if _upper(value.get("fillsScope")) != "ACCOUNT_SESSION_INTERVAL":
            raise UpbitFunctionalBlocked("upbit-fills-scope-incomplete")
        if _upper(value.get("identifierTruthScope")) != "ALL_OWNED_IDENTIFIERS":
            raise UpbitFunctionalBlocked("upbit-identifier-truth-scope-incomplete")
        normal_private = (
            value.get("privateStreamConnected") is True
            and value.get("privateStreamAuthenticated") is True
            and
            value.get("privateStreamComplete") is True
            and value.get("privateStreamGapDetected") is False
            and value.get("privateStreamRecoveryAttested") is not True
        )
        recovery_private = (
            value.get("privateStreamConnected") is True
            and value.get("privateStreamAuthenticated") is True
            and value.get("privateStreamComplete") is False
            and value.get("privateStreamGapDetected") is True
            and value.get("privateStreamRecoveryAttested") is True
        )
        if (
            not (normal_private or recovery_private)
            or value.get("privateStreamExternalActivityAbsent") is not True
            or _upper(value.get("privateStreamSource"))
            != "UPBIT_WEBSOCKET_MYORDER"
            or _upper(value.get("privateStreamScope"))
            != "ACCOUNT_MYORDER_SESSION"
            or _upper(value.get("externalActivityScope"))
            != "UPBIT_ACCOUNT_ALL_MARKETS"
        ):
            raise UpbitFunctionalBlocked("upbit-private-stream-attestation-invalid")
        raw_private_events = value.get("privateStreamEvents")
        if not isinstance(raw_private_events, list) or any(
            not isinstance(row, Mapping) for row in raw_private_events
        ):
            raise UpbitFunctionalBlocked("upbit-private-stream-events-invalid")
        private_stream_events = tuple(dict(row) for row in raw_private_events)
        private_event_ids: list[str] = []
        for row in private_stream_events:
            event_id = _text(row.get("eventId"))
            if (
                not event_id
                or not _text(row.get("orderUuid"))
                or not _upper(row.get("market"))
            ):
                raise UpbitFunctionalBlocked("upbit-private-stream-event-identity-incomplete")
            private_event_ids.append(event_id)
        if len(private_event_ids) != len(set(private_event_ids)):
            raise UpbitFunctionalBlocked("upbit-private-stream-event-duplicate")
        try:
            private_writer_generation = int(
                value.get("privateStreamWriterGeneration")
            )
            private_revision = int(value.get("privateStreamRevision"))
            private_event_cursor = int(value.get("privateStreamEventCursor"))
        except (TypeError, ValueError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-private-stream-cursor-invalid"
            ) from exc
        private_last_event_id = _text(value.get("privateStreamLastEventId"))
        private_event_head_hash = _require_hash(
            value.get("privateStreamEventHeadHash"),
            "upbit-private-stream-event-head-hash",
        )
        if (
            private_writer_generation < 1
            or private_revision < 1
            or private_event_cursor != len(private_stream_events)
            or private_last_event_id
            != (private_event_ids[-1] if private_event_ids else "")
        ):
            raise UpbitFunctionalBlocked(
                "upbit-private-stream-cursor-invalid"
            )
        raw_identifier_truth = value.get("identifierTruth")
        if not isinstance(raw_identifier_truth, Mapping):
            raise UpbitFunctionalBlocked("upbit-identifier-truth-invalid")
        identifier_truth: dict[str, dict[str, Any] | None] = {}
        for raw_identifier, raw_order in raw_identifier_truth.items():
            identifier = _text(raw_identifier)
            if not _SAFE_ID_RE.fullmatch(identifier):
                raise UpbitFunctionalBlocked("upbit-identifier-truth-key-invalid")
            if raw_order is None:
                identifier_truth[identifier] = None
                continue
            if not isinstance(raw_order, Mapping):
                raise UpbitFunctionalBlocked("upbit-identifier-truth-row-invalid")
            row = dict(raw_order)
            if (
                _text(row.get("identifier")) != identifier
                or not _text(row.get("uuid"))
                or _upper(row.get("market")) != SYMBOL
            ):
                raise UpbitFunctionalBlocked("upbit-identifier-truth-row-mismatch")
            identifier_truth[identifier] = row
        rules_payload = value.get("orderRules")
        if not isinstance(rules_payload, Mapping):
            raise UpbitFunctionalBlocked("upbit-order-rules-invalid")
        rules = UpbitOrderRules(
            bid_min_total=_decimal(rules_payload.get("bidMinTotal"), "upbit-bid-min-total", minimum=Decimal("0.00000001")),
            ask_min_total=_decimal(rules_payload.get("askMinTotal"), "upbit-ask-min-total", minimum=Decimal("0.00000001")),
            quantity_step=_decimal(rules_payload.get("quantityStep"), "upbit-quantity-step", minimum=Decimal("0.00000001")),
            quantity_scale=int(_decimal(rules_payload.get("quantityScale"), "upbit-quantity-scale", minimum=Decimal("1"))),
            bid_fee_rate=_decimal(
                rules_payload.get("bidFeeRate"),
                "upbit-bid-fee-rate",
                minimum=Decimal("0"),
            ),
            ask_fee_rate=_decimal(
                rules_payload.get("askFeeRate"),
                "upbit-ask-fee-rate",
                minimum=Decimal("0"),
            ),
        )
        if rules.quantity_scale > 16 or rules.quantity_step.as_tuple().exponent < -rules.quantity_scale:
            raise UpbitFunctionalBlocked("upbit-order-quantity-precision-invalid")
        lists: list[tuple[dict[str, Any], ...]] = []
        for field in ("openOrders", "closedOrders", "fills"):
            rows = value.get(field)
            if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
                raise UpbitFunctionalBlocked(f"upbit-{field}-invalid")
            lists.append(tuple(dict(row) for row in rows))
        total_fees = _decimal(value.get("totalFees"), "upbit-total-fees", minimum=Decimal("0"))
        fill_fee_sum = sum(
            (_decimal(row.get("fee"), "upbit-fill-fee", minimum=Decimal("0")) for row in lists[2]),
            Decimal("0"),
        )
        if total_fees != fill_fee_sum:
            raise UpbitFunctionalBlocked("upbit-fee-truth-mismatch")
        for row in (*lists[0], *lists[1]):
            if _upper(row.get("market")) != SYMBOL:
                continue
            if not _text(row.get("uuid")):
                raise UpbitFunctionalBlocked("upbit-order-identity-incomplete")
            if _upper(row.get("side")) not in {"BID", "ASK"}:
                raise UpbitFunctionalBlocked("upbit-order-side-invalid")
            if _text(row.get("state")).lower() not in {"wait", "watch", "done", "cancel", "reject"}:
                raise UpbitFunctionalBlocked("upbit-order-state-invalid")
        scoped_orders = [
            row
            for row in (*lists[0], *lists[1])
            if _upper(row.get("market")) == SYMBOL
        ]
        order_uuids = [_text(row.get("uuid")) for row in scoped_orders]
        order_identifiers = [
            _text(row.get("identifier"))
            for row in scoped_orders
            if _text(row.get("identifier"))
        ]
        if (
            len(order_uuids) != len(set(order_uuids))
            or len(order_identifiers) != len(set(order_identifiers))
        ):
            raise UpbitFunctionalBlocked("upbit-order-identity-duplicate")
        for row in lists[2]:
            if (
                _upper(row.get("market")) != SYMBOL
                or not _text(row.get("tradeUuid"))
                or not _text(row.get("orderUuid"))
            ):
                raise UpbitFunctionalBlocked("upbit-fill-identity-incomplete")
            _decimal(row.get("volume"), "upbit-fill-volume", minimum=Decimal("0.00000001"))
            _decimal(row.get("funds"), "upbit-fill-funds", minimum=Decimal("0.00000001"))
        trade_ids = [_text(row.get("tradeUuid")) for row in lists[2]]
        if len(trade_ids) != len(set(trade_ids)):
            raise UpbitFunctionalBlocked("upbit-fill-trade-identity-duplicate")
        raw_rest_value = value.get("officialRestRawSnapshot")
        raw_rest_hash_value = value.get("officialRestRawSnapshotHash")
        if raw_rest_value is None and not raw_rest_hash_value:
            raw_rest_snapshot: dict[str, Any] = {}
            raw_rest_hash = ""
        elif (
            not isinstance(raw_rest_value, Mapping)
            or not _exact_lower_hash(raw_rest_hash_value)
            or not hmac.compare_digest(
                raw_rest_hash_value, _stable_hash(dict(raw_rest_value))
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-official-rest-raw-snapshot-invalid"
            )
        else:
            raw_rest_snapshot = dict(raw_rest_value)
            raw_rest_hash = raw_rest_hash_value
        raw_account_rows = value.get("accountRows")
        if not isinstance(raw_account_rows, list) or any(
            not isinstance(row, Mapping) for row in raw_account_rows
        ):
            raise UpbitFunctionalBlocked("upbit-account-rows-invalid")
        account_rows: list[dict[str, str]] = []
        currencies: list[str] = []
        for raw_row in raw_account_rows:
            currency = _upper(raw_row.get("currency"))
            if not currency:
                raise UpbitFunctionalBlocked("upbit-account-currency-invalid")
            row = {
                "currency": currency,
                "available": _decimal_text(
                    _decimal(
                        raw_row.get("available"),
                        "upbit-account-available",
                        minimum=Decimal("0"),
                    )
                ),
                "locked": _decimal_text(
                    _decimal(
                        raw_row.get("locked"),
                        "upbit-account-locked",
                        minimum=Decimal("0"),
                    )
                ),
            }
            account_rows.append(row)
            currencies.append(currency)
        account_rows.sort(key=lambda row: row["currency"])
        if (
            len(currencies) != len(set(currencies))
            or currencies != sorted(currencies)
            or not {"KRW", "BTC"}.issubset(currencies)
        ):
            raise UpbitFunctionalBlocked("upbit-account-rows-incomplete")
        account_rows_hash = _require_hash(
            value.get("accountRowsHash"), "upbit-account-rows-hash"
        )
        if not hmac.compare_digest(
            account_rows_hash, _stable_hash(account_rows)
        ):
            raise UpbitFunctionalBlocked("upbit-account-rows-hash-mismatch")
        account_by_currency = {
            row["currency"]: row for row in account_rows
        }
        if (
            _decimal(
                account_by_currency["KRW"]["available"],
                "upbit-account-krw-available",
            )
            != _decimal(value.get("quoteAvailable"), "upbit-quote-available")
            or _decimal(
                account_by_currency["BTC"]["available"],
                "upbit-account-btc-available",
            )
            != _decimal(value.get("baseAvailable"), "upbit-base-available")
            or (
                _decimal(
                    account_by_currency["BTC"]["available"],
                    "upbit-account-btc-available",
                )
                + _decimal(
                    account_by_currency["BTC"]["locked"],
                    "upbit-account-btc-locked",
                )
                != _decimal(value.get("baseTotal"), "upbit-base-total")
            )
        ):
            raise UpbitFunctionalBlocked("upbit-account-row-total-mismatch")
        if _text(session_id) and session_started_at is not None:
            (
                account_exclusivity_proof,
                account_exclusivity_proof_hash,
                account_exclusivity_proof_verified,
            ) = _verify_account_exclusivity_proof(
                value.get("accountExclusivityProof"),
                session_id=_text(session_id),
                account_fingerprint=account_fingerprint,
                session_started_at=session_started_at,
                observation_started_at=observation_started_at,
                observed_at=observed_at,
                verifier=account_exclusivity_verifier,
                verifier_pin=account_exclusivity_verifier_pin,
            )
        else:
            (
                account_exclusivity_proof,
                account_exclusivity_proof_hash,
                account_exclusivity_proof_verified,
            ) = _invalid_account_exclusivity_proof(
                raw_value=value.get("accountExclusivityProof"),
                reason="SESSION_BINDING_UNAVAILABLE",
            )
        return cls(
            observed_at=observed_at,
            observation_started_at=observation_started_at,
            quote_available=_decimal(value.get("quoteAvailable"), "upbit-quote-available", minimum=Decimal("0")),
            base_available=_decimal(value.get("baseAvailable"), "upbit-base-available", minimum=Decimal("0")),
            base_total=_decimal(value.get("baseTotal"), "upbit-base-total", minimum=Decimal("0")),
            rules=rules,
            open_orders=lists[0],
            closed_orders=lists[1],
            fills=lists[2],
            total_fees=total_fees,
            mark_price=_decimal(
                value.get("markPrice"),
                "upbit-mark-price",
                minimum=Decimal("0.00000001"),
            ),
            identifier_truth=identifier_truth,
            private_stream_events=private_stream_events,
            private_stream_recovery=recovery_private,
            private_stream_writer_generation=private_writer_generation,
            private_stream_revision=private_revision,
            private_stream_event_cursor=private_event_cursor,
            private_stream_last_event_id=private_last_event_id,
            private_stream_event_head_hash=private_event_head_hash,
            account_rows=tuple(account_rows),
            account_rows_hash=account_rows_hash,
            account_external_activity_absent=True,
            account_exclusivity_proof=account_exclusivity_proof,
            account_exclusivity_proof_hash=account_exclusivity_proof_hash,
            account_exclusivity_proof_verified=(
                account_exclusivity_proof_verified
            ),
            official_rest_raw_snapshot=raw_rest_snapshot,
            official_rest_raw_snapshot_hash=raw_rest_hash,
        )


@dataclass(frozen=True, slots=True)
class UpbitLeg:
    slot: str
    side: str
    order_type: str
    notional: Decimal = Decimal("0")
    quantity: Decimal = Decimal("0")
    target_order_identifier: str = ""

    @classmethod
    def buy(cls, notional: object) -> "UpbitLeg":
        return cls("STRATEGY_BUY", "BID", "PRICE", notional=_decimal(notional, "upbit-buy-notional", minimum=Decimal("0.00000001")))

    @classmethod
    def sell(cls, quantity: object, *, cleanup: bool = False) -> "UpbitLeg":
        return cls("CLEANUP_SELL" if cleanup else "STRATEGY_SELL", "ASK", "MARKET", quantity=_decimal(quantity, "upbit-sell-quantity", minimum=Decimal("0.00000001")))

    @classmethod
    def cancel(cls, identifier: str) -> "UpbitLeg":
        return cls("CLEANUP_CANCEL", "CANCEL", "CANCEL", target_order_identifier=_text(identifier))


class TruthReader(Protocol):
    def __call__(
        self,
        *,
        session_id: str,
        phase: str,
        identifiers: tuple[str, ...],
    ) -> Mapping[str, Any]: ...


class PostOrder(Protocol):
    def __call__(
        self,
        payload: Mapping[str, str],
        *,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> Mapping[str, Any]: ...


class CancelOrder(Protocol):
    def __call__(
        self,
        *,
        identifier: str,
        functional_capability: str,
        functional_action: str,
        claim_id: str,
        request_hash: str,
    ) -> Mapping[str, Any]: ...


class LeaseFactory(Protocol):
    def __call__(self, *, session_id: str, claim_id: str) -> AbstractContextManager[Callable[[], Mapping[str, Any]]]: ...


class TerminalStreamPrepare(Protocol):
    def __call__(
        self, *, session_id: str, identifiers: tuple[str, ...]
    ) -> Mapping[str, Any]: ...


class TerminalStreamCommit(Protocol):
    def __call__(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
        expected: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class TerminalStreamBarrier(Protocol):
    def __call__(self, *, session_id: str) -> Mapping[str, Any]: ...


class UpbitFunctionalLedger:
    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS upbit_functional_sessions (
                    session_id TEXT PRIMARY KEY,
                    permit_id TEXT NOT NULL UNIQUE,
                    permit_hash TEXT NOT NULL,
                    scope_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    starts_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    cleanup_deadline TEXT NOT NULL,
                    baseline_base TEXT NOT NULL,
                    baseline_quote TEXT NOT NULL,
                    baseline_account_rows_json TEXT NOT NULL DEFAULT '',
                    baseline_account_rows_hash TEXT NOT NULL DEFAULT '',
                    owner_loss TEXT NOT NULL DEFAULT '0',
                    max_owner_gross TEXT NOT NULL DEFAULT '0',
                    last_bar_id TEXT NOT NULL DEFAULT '',
                    last_bar_closed_at TEXT NOT NULL DEFAULT '',
                    new_entries_blocked INTEGER NOT NULL DEFAULT 1,
                    real_orders_enabled INTEGER NOT NULL DEFAULT 0,
                    capability_hash TEXT NOT NULL,
                    capability_seal_hash TEXT NOT NULL DEFAULT '',
                    account_exclusivity_breach INTEGER NOT NULL DEFAULT 0,
                    final_evidence_json TEXT NOT NULL DEFAULT '',
                    final_evidence_hash TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_claims (
                    claim_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    claim_key TEXT NOT NULL,
                    slot TEXT NOT NULL,
                    side TEXT NOT NULL,
                    identifier TEXT NOT NULL UNIQUE,
                    target_identifier TEXT NOT NULL DEFAULT '',
                    request_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    response_hash TEXT NOT NULL DEFAULT '',
                    proven_not_sent_retries INTEGER NOT NULL DEFAULT 0,
                    absence_observations INTEGER NOT NULL DEFAULT 0,
                    absence_first_at TEXT NOT NULL DEFAULT '',
                    absence_last_at TEXT NOT NULL DEFAULT '',
                    absence_base TEXT NOT NULL DEFAULT '',
                    absence_quote TEXT NOT NULL DEFAULT '',
                    bar_id TEXT NOT NULL DEFAULT '',
                    evaluation_id TEXT NOT NULL DEFAULT '',
                    evaluation_closed_at TEXT NOT NULL DEFAULT '',
                    evaluation_hash TEXT NOT NULL DEFAULT '',
                    claimed_at TEXT NOT NULL DEFAULT '',
                    post_boundary_at TEXT NOT NULL DEFAULT '',
                    resolved_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(session_id, claim_key)
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_bars (
                    session_id TEXT NOT NULL,
                    bar_id TEXT NOT NULL,
                    closed_at TEXT NOT NULL,
                    bar_hash TEXT NOT NULL DEFAULT '',
                    signal TEXT NOT NULL DEFAULT '',
                    evaluation_id TEXT NOT NULL DEFAULT '',
                    evaluation_json TEXT NOT NULL DEFAULT '',
                    evaluation_hash TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(session_id, bar_id)
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_recovery_claims (
                    recovery_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    recovery_hash TEXT NOT NULL UNIQUE,
                    claimed_at TEXT NOT NULL,
                    UNIQUE(session_id, recovery_id)
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_terminal_truth (
                    session_id TEXT PRIMARY KEY,
                    truth_json TEXT NOT NULL,
                    truth_hash TEXT NOT NULL UNIQUE,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS upbit_functional_terminal_raw_truth (
                    session_id TEXT PRIMARY KEY,
                    account_fingerprint TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    route_scope_hash TEXT NOT NULL,
                    cutoff TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    raw_hash TEXT NOT NULL UNIQUE,
                    observed_at TEXT NOT NULL
                );
                """
            )
            claim_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(upbit_functional_claims)"
                )
            }
            if "target_identifier" not in claim_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_claims
                    ADD COLUMN target_identifier TEXT NOT NULL DEFAULT ''"""
                )
            if "proven_not_sent_retries" not in claim_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_claims ADD COLUMN
                    proven_not_sent_retries INTEGER NOT NULL DEFAULT 0"""
                )
            if "claim_key" not in claim_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_claims ADD COLUMN
                    claim_key TEXT NOT NULL DEFAULT ''"""
                )
                connection.execute(
                    """UPDATE upbit_functional_claims SET claim_key=slot
                    WHERE claim_key=''"""
                )
            for name, definition in (
                ("absence_observations", "INTEGER NOT NULL DEFAULT 0"),
                ("absence_first_at", "TEXT NOT NULL DEFAULT ''"),
                ("absence_last_at", "TEXT NOT NULL DEFAULT ''"),
                ("absence_base", "TEXT NOT NULL DEFAULT ''"),
                ("absence_quote", "TEXT NOT NULL DEFAULT ''"),
                ("bar_id", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_id", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_closed_at", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_hash", "TEXT NOT NULL DEFAULT ''"),
                ("claimed_at", "TEXT NOT NULL DEFAULT ''"),
                ("post_boundary_at", "TEXT NOT NULL DEFAULT ''"),
                ("resolved_at", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in claim_columns:
                    connection.execute(
                        f"ALTER TABLE upbit_functional_claims ADD COLUMN {name} {definition}"
                    )
            bar_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(upbit_functional_bars)"
                )
            }
            for name, definition in (
                ("bar_hash", "TEXT NOT NULL DEFAULT ''"),
                ("signal", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_id", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_json", "TEXT NOT NULL DEFAULT ''"),
                ("evaluation_hash", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in bar_columns:
                    connection.execute(
                        f"ALTER TABLE upbit_functional_bars ADD COLUMN {name} {definition}"
                    )
            connection.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS
                idx_upbit_functional_bar_evaluation
                ON upbit_functional_bars(session_id,evaluation_id)
                WHERE evaluation_id<>''"""
            )
            session_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(upbit_functional_sessions)"
                )
            }
            if "capability_seal_hash" not in session_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_sessions ADD COLUMN
                    capability_seal_hash TEXT NOT NULL DEFAULT ''"""
                )
            if "final_evidence_json" not in session_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_sessions ADD COLUMN
                    final_evidence_json TEXT NOT NULL DEFAULT ''"""
                )
            if "account_exclusivity_breach" not in session_columns:
                connection.execute(
                    """ALTER TABLE upbit_functional_sessions ADD COLUMN
                    account_exclusivity_breach INTEGER NOT NULL DEFAULT 0"""
                )
            for name, definition in (
                ("baseline_account_rows_json", "TEXT NOT NULL DEFAULT ''"),
                ("baseline_account_rows_hash", "TEXT NOT NULL DEFAULT ''"),
                ("max_owner_gross", "TEXT NOT NULL DEFAULT '0'"),
            ):
                if name not in session_columns:
                    connection.execute(
                        f"ALTER TABLE upbit_functional_sessions "
                        f"ADD COLUMN {name} {definition}"
                    )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def activate(
        self,
        scope: UpbitPermitScope,
        truth: UpbitTruth,
        *,
        session_id: str,
        capability_hash: str,
    ) -> dict[str, Any]:
        if not _SAFE_ID_RE.fullmatch(session_id):
            raise UpbitFunctionalBlocked("upbit-session-id-invalid")
        if truth.open_orders:
            raise UpbitFunctionalBlocked("upbit-baseline-working-order-present")
        if any(
            _decimal(row.get("locked"), "upbit-baseline-account-locked") != 0
            for row in truth.account_rows
        ):
            raise UpbitFunctionalBlocked("upbit-baseline-account-lock-present")
        scope_hash = _stable_hash(scope.snapshot())
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """INSERT INTO upbit_functional_sessions
                    (session_id,permit_id,permit_hash,scope_hash,state,starts_at,expires_at,
                     cleanup_deadline,baseline_base,baseline_quote,
                     baseline_account_rows_json,baseline_account_rows_hash,
                     new_entries_blocked,real_orders_enabled,capability_hash,
                     capability_seal_hash)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?)""",
                    (
                        session_id,
                        scope.permit_id,
                        scope.permit_hash,
                        scope_hash,
                        "ACTIVE",
                        _utc_text(scope.starts_at),
                        _utc_text(scope.ends_at),
                        _utc_text(scope.cleanup_deadline),
                        _decimal_text(truth.base_total),
                        _decimal_text(truth.quote_available),
                        json.dumps(
                            list(truth.account_rows),
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        truth.account_rows_hash,
                        _require_hash(
                            capability_hash,
                            "upbit-functional-capability-hash",
                        ),
                        _require_hash(
                            capability_hash,
                            "upbit-functional-capability-seal-hash",
                        ),
                    ),
                )
                self._event(connection, session_id, "ACTIVATED", {"scopeHash": scope_hash, "evidenceClass": EVIDENCE_CLASS})
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-or-permit-already-activated") from exc
        return self.session(session_id)

    def rotate_cleanup_capability(
        self,
        session_id: str,
        *,
        capability_hash: str,
    ) -> dict[str, Any]:
        normalized_hash = _require_hash(
            capability_hash,
            "upbit-functional-capability-hash",
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM upbit_functional_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if row is None or row["state"] != "CLEANUP":
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-recovery-cleanup-state-required"
                )
            connection.execute(
                """UPDATE upbit_functional_sessions
                SET capability_hash=?,capability_seal_hash=?,new_entries_blocked=1
                WHERE session_id=?""",
                (normalized_hash, normalized_hash, session_id),
            )
            self._event(
                connection,
                session_id,
                "CLEANUP_CAPABILITY_ROTATED",
                {"capabilityHash": normalized_hash},
            )
            connection.commit()
        return self.session(session_id)

    def revoke_cleanup_capability(
        self,
        session_id: str,
        *,
        reason: str,
    ) -> dict[str, Any]:
        """Durably revoke mutation authority while preserving its audit hash.

        The durable hash is cleared before the runtime pointer.  A crash in
        between therefore leaves a stale runtime value that cannot satisfy a
        service/ledger capability comparison and can be safely cleared during
        cleanup recovery.
        """

        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM upbit_functional_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if row is None or row["state"] not in {
                "CLEANUP",
                "FINAL_RESET_PENDING",
                "FINALIZED",
            }:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-capability-revoke-cleanup-state-required"
                )
            connection.execute(
                """UPDATE upbit_functional_sessions
                SET capability_hash='',new_entries_blocked=1,
                    real_orders_enabled=0
                WHERE session_id=?""",
                (session_id,),
            )
            self._event(
                connection,
                session_id,
                "CAPABILITY_REVOKED",
                {"reason": reason},
            )
            connection.commit()
        return self.session(session_id)

    @staticmethod
    def _event(connection: sqlite3.Connection, session_id: str, event_type: str, payload: Mapping[str, Any]) -> None:
        connection.execute(
            "INSERT INTO upbit_functional_events(session_id,event_type,payload) VALUES (?,?,?)",
            (session_id, event_type, json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))),
        )

    def session(self, session_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM upbit_functional_sessions WHERE session_id=?", (session_id,)).fetchone()
        if row is None:
            raise UpbitFunctionalBlocked("upbit-session-missing")
        return dict(row)

    def nonterminal_sessions(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM upbit_functional_sessions
                WHERE state!='FINALIZED' ORDER BY starts_at,session_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def sessions(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM upbit_functional_sessions
                ORDER BY starts_at,session_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_recovery_approval(
        self,
        *,
        session_id: str,
        recovery_id: str,
        recovery_hash: str,
        claimed_at: datetime,
    ) -> dict[str, str]:
        """Consume one server-approved cleanup-recovery pointer exactly once."""

        if not _SAFE_ID_RE.fullmatch(_text(recovery_id)):
            raise UpbitFunctionalBlocked("upbit-recovery-id-invalid")
        normalized_hash = _require_hash(
            recovery_hash, "upbit-recovery-content-hash"
        )
        observed = _utc(claimed_at, "upbit-recovery-claimed-at")
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """SELECT recovery_hash FROM upbit_functional_recovery_claims
                    WHERE recovery_id=? OR recovery_hash=?""",
                    (_text(recovery_id), normalized_hash),
                ).fetchone()
                if existing is not None:
                    raise UpbitFunctionalBlocked(
                        "upbit-recovery-approval-already-consumed"
                    )
                session = connection.execute(
                    "SELECT state FROM upbit_functional_sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if session is None or session["state"] not in {"ACTIVE", "CLEANUP"}:
                    raise UpbitFunctionalBlocked(
                        "upbit-recovery-session-not-recoverable"
                    )
                connection.execute(
                    """INSERT INTO upbit_functional_recovery_claims
                    (recovery_id,session_id,recovery_hash,claimed_at)
                    VALUES (?,?,?,?)""",
                    (
                        _text(recovery_id),
                        session_id,
                        normalized_hash,
                        _utc_text(observed),
                    ),
                )
                self._event(
                    connection,
                    session_id,
                    "RECOVERY_APPROVAL_CONSUMED",
                    {
                        "recoveryId": _text(recovery_id),
                        "recoveryHash": normalized_hash,
                    },
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-recovery-approval-already-consumed"
                ) from exc
            except Exception:
                connection.rollback()
                raise
        return {
            "recoveryId": _text(recovery_id),
            "recoveryHash": normalized_hash,
        }

    def claims(self, session_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM upbit_functional_claims WHERE session_id=? ORDER BY rowid", (session_id,)).fetchall()
        return [dict(row) for row in rows]

    def mutation_authority(
        self,
        session_id: str,
        claim_id: str,
    ) -> dict[str, Any]:
        """Read the exact durable session/claim binding for the final edge."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT
                    s.session_id,s.permit_id,s.permit_hash,s.scope_hash,
                    s.state AS session_state,s.capability_hash,
                    c.claim_id,c.slot,c.side,c.identifier,c.target_identifier,
                    c.request_hash,c.state AS claim_state
                FROM upbit_functional_sessions s
                JOIN upbit_functional_claims c ON c.session_id=s.session_id
                WHERE s.session_id=? AND c.claim_id=?""",
                (session_id, claim_id),
            ).fetchone()
        if row is None:
            raise UpbitFunctionalBlocked(
                "upbit-mutation-durable-authority-missing"
            )
        return dict(row)

    def enter_cleanup(self, session_id: str, *, reason: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT state FROM upbit_functional_sessions WHERE session_id=?", (session_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-missing")
            if row["state"] not in {"ACTIVE", "CLEANUP"}:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-not-cleanable")
            connection.execute("UPDATE upbit_functional_sessions SET state='CLEANUP',new_entries_blocked=1 WHERE session_id=?", (session_id,))
            self._event(connection, session_id, "CLEANUP_ENTERED", {"reason": reason})
            connection.commit()
        return self.session(session_id)

    def latch_account_exclusivity_breach(
        self, session_id: str, *, phase: str
    ) -> dict[str, Any]:
        """Durably forbid entry while retaining bounded cleanup authority."""

        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM upbit_functional_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if row is None or row["state"] not in {"ACTIVE", "CLEANUP"}:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-account-exclusivity-breach-session-invalid"
                )
            connection.execute(
                """UPDATE upbit_functional_sessions
                SET state='CLEANUP',new_entries_blocked=1,
                    account_exclusivity_breach=1 WHERE session_id=?""",
                (session_id,),
            )
            self._event(
                connection,
                session_id,
                "ACCOUNT_EXCLUSIVITY_BREACH",
                {"phase": phase},
            )
            connection.commit()
        return self.session(session_id)

    def claim(
        self,
        session_id: str,
        leg: UpbitLeg,
        payload: Mapping[str, Any],
        *,
        functional_capability_verified: bool,
        natural_evaluation: FinalizedFiveMinuteBar | None = None,
    ) -> dict[str, str]:
        request_hash = _stable_hash(payload)
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                session = connection.execute("SELECT * FROM upbit_functional_sessions WHERE session_id=?", (session_id,)).fetchone()
                if session is None:
                    raise UpbitFunctionalBlocked("upbit-session-missing")
                strategy_leg = leg.slot in {"STRATEGY_BUY", "STRATEGY_SELL"}
                evaluation_row = None
                if strategy_leg:
                    if natural_evaluation is None:
                        raise UpbitFunctionalBlocked(
                            "upbit-strategy-natural-evaluation-required"
                        )
                    evaluation_row = connection.execute(
                        """SELECT * FROM upbit_functional_bars
                        WHERE session_id=? AND bar_id=? AND evaluation_id=?""",
                        (
                            session_id,
                            natural_evaluation.bar_id,
                            natural_evaluation.evaluation_id,
                        ),
                    ).fetchone()
                    expected_signal = (
                        "BUY" if leg.slot == "STRATEGY_BUY" else "SELL"
                    )
                    if (
                        evaluation_row is None
                        or natural_evaluation.signal != expected_signal
                        or evaluation_row["signal"] != expected_signal
                        or evaluation_row["bar_hash"]
                        != natural_evaluation.bar_hash
                        or evaluation_row["closed_at"]
                        != _utc_text(natural_evaluation.closed_at)
                        or evaluation_row["evaluation_json"]
                        != natural_evaluation.evaluation_json
                        or evaluation_row["evaluation_hash"]
                        != natural_evaluation.evaluation_hash
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-strategy-natural-evaluation-not-sealed"
                        )
                elif natural_evaluation is not None:
                    raise UpbitFunctionalBlocked(
                        "upbit-cleanup-natural-evaluation-forbidden"
                    )
                existing_rows = connection.execute(
                    "SELECT * FROM upbit_functional_claims WHERE session_id=?",
                    (session_id,),
                ).fetchall()
                slots = {row["slot"] for row in existing_rows}
                retry = next(
                    (
                        row
                        for row in reversed(existing_rows)
                        if row["slot"] == leg.slot
                        and row["state"] == "BLOCKED_BEFORE_POST"
                        and int(row["proven_not_sent_retries"]) < 1
                        and row["request_hash"] == request_hash
                        and row["side"] == leg.side
                        and row["target_identifier"]
                        == leg.target_order_identifier
                    ),
                    None,
                )
                if leg.slot == "STRATEGY_BUY":
                    if (
                        session["state"] != "ACTIVE"
                        or functional_capability_verified is not True
                    ):
                        raise UpbitFunctionalBlocked("upbit-new-entry-blocked")
                    if "STRATEGY_SELL" in slots or "CLEANUP_SELL" in slots:
                        raise UpbitFunctionalBlocked("upbit-reentry-forbidden")
                    claim_key = leg.slot
                elif leg.slot == "STRATEGY_SELL":
                    if session["state"] != "ACTIVE" or "STRATEGY_BUY" not in slots:
                        raise UpbitFunctionalBlocked("upbit-strategy-sell-without-owned-buy")
                    claim_key = leg.slot
                elif leg.slot not in {"CLEANUP_CANCEL", "CLEANUP_SELL"} or session["state"] != "CLEANUP":
                    raise UpbitFunctionalBlocked("upbit-cleanup-authority-required")
                elif leg.slot == "CLEANUP_CANCEL":
                    cleanup_count = sum(
                        row["slot"] in {"CLEANUP_CANCEL", "CLEANUP_SELL"}
                        for row in existing_rows
                    )
                    if retry is None and cleanup_count >= MAX_CLEANUP_ACTION_GENERATIONS:
                        raise UpbitFunctionalBlocked(
                            "upbit-cleanup-action-generation-cap-reached"
                        )
                    claim_key = (
                        "CLEANUP_CANCEL:" + leg.target_order_identifier
                    )
                else:
                    sell_count = sum(
                        row["slot"] == "CLEANUP_SELL"
                        for row in existing_rows
                    )
                    cleanup_count = sum(
                        row["slot"] in {"CLEANUP_CANCEL", "CLEANUP_SELL"}
                        for row in existing_rows
                    )
                    # A newly submitted cleanup SELL can itself remain
                    # working.  Never spend the final action generation on a
                    # SELL: retain one exact-identifier CANCEL generation.
                    if (
                        retry is None
                        and cleanup_count >= MAX_CLEANUP_ACTION_GENERATIONS - 1
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-cleanup-sell-cancel-reserve-required"
                        )
                    claim_key = (
                        retry["claim_key"]
                        if retry is not None
                        else f"CLEANUP_SELL:{sell_count + 1}"
                    )
                existing = retry or next(
                    (
                        row
                        for row in existing_rows
                        if row["claim_key"] == claim_key
                    ),
                    None,
                )
                if existing is not None:
                    if (
                        existing["state"] != "BLOCKED_BEFORE_POST"
                        or int(existing["proven_not_sent_retries"]) >= 1
                        or existing["request_hash"] != request_hash
                        or existing["side"] != leg.side
                        or existing["target_identifier"]
                        != leg.target_order_identifier
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-leg-already-claimed-no-retry"
                        )
                    connection.execute(
                        """UPDATE upbit_functional_claims
                        SET state='CLAIMED_PRE_POST',broker_order_id='',
                            response_hash='',
                            proven_not_sent_retries=proven_not_sent_retries+1,
                            bar_id=?,evaluation_id=?,evaluation_closed_at=?,
                            evaluation_hash=?,claimed_at=?,post_boundary_at='',
                            resolved_at=''
                        WHERE claim_id=?""",
                        (
                            natural_evaluation.bar_id
                            if natural_evaluation is not None
                            else "",
                            natural_evaluation.evaluation_id
                            if natural_evaluation is not None
                            else "",
                            _utc_text(natural_evaluation.closed_at)
                            if natural_evaluation is not None
                            else "",
                            natural_evaluation.evaluation_hash
                            if natural_evaluation is not None
                            else "",
                            _utc_text(_utc(self.clock(), "upbit-claim-time")),
                            existing["claim_id"],
                        ),
                    )
                    self._event(
                        connection,
                        session_id,
                        "PROVEN_NOT_SENT_CLAIM_REUSED",
                        {
                            "claimId": existing["claim_id"],
                            "slot": leg.slot,
                            "requestHash": request_hash,
                        },
                    )
                    connection.commit()
                    return {
                        "claimId": existing["claim_id"],
                        "identifier": existing["identifier"],
                        "requestHash": request_hash,
                    }
                claim_material = {
                    "sessionId": session_id,
                    "claimKey": claim_key,
                    "requestHash": request_hash,
                    "evaluationId": (
                        natural_evaluation.evaluation_id
                        if natural_evaluation is not None
                        else ""
                    ),
                    "evaluationHash": (
                        natural_evaluation.evaluation_hash
                        if natural_evaluation is not None
                        else ""
                    ),
                }
                claim_digest = _stable_hash(claim_material)
                claim_id = f"upbit-claim-{claim_digest[:32]}"
                identifier = f"uft-{claim_digest[:28]}"
                connection.execute(
                    """INSERT INTO upbit_functional_claims
                    (claim_id,session_id,claim_key,slot,side,identifier,
                     target_identifier,request_hash,state,bar_id,evaluation_id,
                     evaluation_closed_at,evaluation_hash,claimed_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        claim_id,
                        session_id,
                        claim_key,
                        leg.slot,
                        leg.side,
                        identifier,
                        leg.target_order_identifier,
                        request_hash,
                        "CLAIMED_PRE_POST",
                        natural_evaluation.bar_id
                        if natural_evaluation is not None
                        else "",
                        natural_evaluation.evaluation_id
                        if natural_evaluation is not None
                        else "",
                        _utc_text(natural_evaluation.closed_at)
                        if natural_evaluation is not None
                        else "",
                        natural_evaluation.evaluation_hash
                        if natural_evaluation is not None
                        else "",
                        _utc_text(_utc(self.clock(), "upbit-claim-time")),
                    ),
                )
                self._event(
                    connection,
                    session_id,
                    "PRE_POST_CLAIM",
                    {
                        "claimId": claim_id,
                        "slot": leg.slot,
                        "requestHash": request_hash,
                        "barId": natural_evaluation.bar_id
                        if natural_evaluation is not None
                        else "",
                        "evaluationId": natural_evaluation.evaluation_id
                        if natural_evaluation is not None
                        else "",
                        "evaluationHash": natural_evaluation.evaluation_hash
                        if natural_evaluation is not None
                        else "",
                    },
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return {"claimId": claim_id, "identifier": identifier, "requestHash": request_hash}

    def resolve_claim(self, claim_id: str, *, state: str, response: Mapping[str, Any] | None = None) -> None:
        if state not in {"ACKNOWLEDGED", "RECONCILED", "AMBIGUOUS", "BLOCKED_BEFORE_POST"}:
            raise UpbitFunctionalBlocked("upbit-claim-state-invalid")
        response_payload = dict(response or {})
        broker_order_id = _text(response_payload.get("uuid"))
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            source_states = (
                ("CLAIMED_PRE_POST",)
                if state == "BLOCKED_BEFORE_POST"
                else ("CLAIMED_PRE_POST", "POST_MAY_HAVE_CROSSED")
            )
            placeholders = ",".join("?" for _ in source_states)
            cursor = connection.execute(
                f"""UPDATE upbit_functional_claims
                SET state=?,broker_order_id=?,response_hash=?,resolved_at=?
                WHERE claim_id=? AND state IN ({placeholders})""",
                (
                    state,
                    broker_order_id,
                    _stable_hash(response_payload) if response_payload else "",
                    _utc_text(_utc(self.clock(), "upbit-claim-resolved-time")),
                    claim_id,
                    *source_states,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-claim-transition-invalid")
            row = connection.execute("SELECT session_id FROM upbit_functional_claims WHERE claim_id=?", (claim_id,)).fetchone()
            self._event(connection, row[0], "CLAIM_RESOLVED", {"claimId": claim_id, "state": state})
            connection.commit()

    def mark_post_may_have_crossed(
        self,
        claim_id: str,
        request_hash: str,
    ) -> dict[str, Any]:
        """Durably mark the irreversible transport boundary before URL open."""

        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE upbit_functional_claims
                SET state='POST_MAY_HAVE_CROSSED',post_boundary_at=?
                WHERE claim_id=? AND request_hash=?
                AND state='CLAIMED_PRE_POST'""",
                (
                    _utc_text(_utc(self.clock(), "upbit-post-boundary-time")),
                    claim_id,
                    request_hash,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-post-boundary-marker-cas-failed"
                )
            row = connection.execute(
                """SELECT session_id,state FROM upbit_functional_claims
                WHERE claim_id=?""",
                (claim_id,),
            ).fetchone()
            self._event(
                connection,
                row["session_id"],
                "POST_MAY_HAVE_CROSSED",
                {"claimId": claim_id, "requestHash": request_hash},
            )
            connection.commit()
        return {"claimId": claim_id, "state": row["state"]}

    def reconcile_ambiguous_claim(
        self,
        claim_id: str,
        *,
        state: str,
        response: Mapping[str, Any] | None = None,
    ) -> None:
        if state not in {"RECONCILED", "AMBIGUOUS_PROVEN_NOT_ACCEPTED"}:
            raise UpbitFunctionalBlocked(
                "upbit-ambiguous-reconciliation-state-invalid"
            )
        response_payload = dict(response or {})
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """UPDATE upbit_functional_claims
                SET state=?,broker_order_id=?,response_hash=?,resolved_at=?
                WHERE claim_id=? AND state IN
                ('AMBIGUOUS','POST_MAY_HAVE_CROSSED')""",
                (
                    state,
                    _text(response_payload.get("uuid")),
                    _stable_hash(response_payload) if response_payload else "",
                    _utc_text(_utc(self.clock(), "upbit-ambiguous-resolved-time")),
                    claim_id,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-claim-transition-invalid"
                )
            row = connection.execute(
                "SELECT session_id FROM upbit_functional_claims WHERE claim_id=?",
                (claim_id,),
            ).fetchone()
            self._event(
                connection,
                row[0],
                "AMBIGUOUS_CLAIM_RECONCILED",
                {"claimId": claim_id, "state": state},
            )
            connection.commit()

    def observe_ambiguous_absence(
        self,
        claim_id: str,
        *,
        observed_at: datetime,
        base_total: Decimal,
        quote_available: Decimal,
    ) -> dict[str, Any]:
        """Build a conservative, durable non-acceptance visibility proof."""

        observed = _utc(observed_at, "upbit-absence-observed-at")
        base = _decimal_text(base_total)
        quote = _decimal_text(quote_available)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM upbit_functional_claims WHERE claim_id=?",
                (claim_id,),
            ).fetchone()
            if row is None or row["state"] not in {
                "AMBIGUOUS",
                "POST_MAY_HAVE_CROSSED",
            }:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-absence-claim-state-invalid"
                )
            count = int(row["absence_observations"])
            if count == 0:
                first = observed
            else:
                first = _utc(
                    row["absence_first_at"],
                    "upbit-absence-first-at",
                )
                last = _utc(
                    row["absence_last_at"],
                    "upbit-absence-last-at",
                )
                if observed <= last:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-ambiguous-absence-observation-not-new"
                    )
                if row["absence_base"] != base or row["absence_quote"] != quote:
                    connection.execute(
                        """UPDATE upbit_functional_claims SET
                        absence_observations=1,absence_first_at=?,
                        absence_last_at=?,absence_base=?,absence_quote=?
                        WHERE claim_id=?""",
                        (
                            _utc_text(observed),
                            _utc_text(observed),
                            base,
                            quote,
                            claim_id,
                        ),
                    )
                    connection.commit()
                    return {
                        "state": row["state"],
                        "observations": 1,
                        "visibilityHorizonSatisfied": False,
                    }
            count += 1
            horizon_satisfied = (
                count >= 3
                and observed - first >= timedelta(seconds=30)
            )
            state = (
                "AMBIGUOUS_PROVEN_NOT_ACCEPTED"
                if horizon_satisfied
                else row["state"]
            )
            connection.execute(
                """UPDATE upbit_functional_claims SET state=?,
                absence_observations=?,absence_first_at=?,absence_last_at=?,
                absence_base=?,absence_quote=? WHERE claim_id=?""",
                (
                    state,
                    count,
                    _utc_text(first),
                    _utc_text(observed),
                    base,
                    quote,
                    claim_id,
                ),
            )
            self._event(
                connection,
                row["session_id"],
                "AMBIGUOUS_ABSENCE_OBSERVED",
                {
                    "claimId": claim_id,
                    "observations": count,
                    "visibilityHorizonSatisfied": horizon_satisfied,
                },
            )
            connection.commit()
        return {
            "state": state,
            "observations": count,
            "visibilityHorizonSatisfied": horizon_satisfied,
        }

    def note_bar(
        self,
        session_id: str,
        bar: FinalizedFiveMinuteBar,
    ) -> None:
        closed_at_text = _utc_text(bar.closed_at)
        with closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                session = connection.execute(
                    "SELECT last_bar_closed_at FROM upbit_functional_sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if session is None:
                    raise UpbitFunctionalBlocked("upbit-session-missing")
                if session["last_bar_closed_at"] and closed_at_text <= session["last_bar_closed_at"]:
                    raise UpbitFunctionalBlocked("upbit-bar-not-strictly-newer")
                connection.execute(
                    """INSERT INTO upbit_functional_bars
                    (session_id,bar_id,closed_at,bar_hash,signal,
                     evaluation_id,evaluation_json,evaluation_hash)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        session_id,
                        bar.bar_id,
                        closed_at_text,
                        bar.bar_hash,
                        bar.signal,
                        bar.evaluation_id,
                        bar.evaluation_json,
                        bar.evaluation_hash,
                    ),
                )
                connection.execute(
                    "UPDATE upbit_functional_sessions SET last_bar_id=?,last_bar_closed_at=? WHERE session_id=?",
                    (bar.bar_id, closed_at_text, session_id),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-bar-already-consumed") from exc
            except Exception:
                connection.rollback()
                raise

    def set_owner_risk(
        self, session_id: str, *, loss: Decimal, gross: Decimal
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT max_owner_gross FROM upbit_functional_sessions "
                "WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-missing")
            previous = _decimal(
                row["max_owner_gross"], "upbit-max-owner-gross"
            )
            connection.execute(
                "UPDATE upbit_functional_sessions SET owner_loss=?,"
                "max_owner_gross=? WHERE session_id=?",
                (
                    _decimal_text(loss),
                    _decimal_text(max(previous, gross)),
                    session_id,
                ),
            )
            connection.commit()

    def set_owner_loss(self, session_id: str, loss: Decimal) -> None:
        session = self.session(session_id)
        self.set_owner_risk(
            session_id,
            loss=loss,
            gross=_decimal(
                session.get("max_owner_gross"), "upbit-max-owner-gross"
            ),
        )

    def begin_final_reset(
        self,
        session_id: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically seal evidence and revoke the durable capability.

        Runtime capability clearing happens after this transaction.  The
        intermediate state is deliberately restartable, so a crash cannot
        leave a live durable capability or force an unsafe re-finalization.
        """

        evidence_payload = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
        evidence_hash = _stable_hash(evidence)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                """SELECT state,final_evidence_hash,final_evidence_json
                FROM upbit_functional_sessions WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if session is None:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-missing")
            if session["state"] in {"FINAL_RESET_PENDING", "FINALIZED"}:
                if (
                    session["final_evidence_hash"] != evidence_hash
                    or session["final_evidence_json"] != evidence_payload
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-final-reset-evidence-mismatch"
                    )
                connection.commit()
                return self.session(session_id)
            if session["state"] != "CLEANUP":
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-finalize-cleanup-state-required"
                )
            unresolved = connection.execute(
                """SELECT COUNT(*) FROM upbit_functional_claims
                WHERE session_id=? AND state NOT IN
                ('RECONCILED','BLOCKED_BEFORE_POST',
                 'AMBIGUOUS_PROVEN_NOT_ACCEPTED')""",
                (session_id,),
            ).fetchone()[0]
            if unresolved:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-finalize-unresolved-claims")
            connection.execute(
                """UPDATE upbit_functional_sessions
                SET state='FINAL_RESET_PENDING',new_entries_blocked=1,
                    real_orders_enabled=0,capability_hash='',
                    final_evidence_json=?,final_evidence_hash=?
                WHERE session_id=?""",
                (evidence_payload, evidence_hash, session_id),
            )
            self._event(
                connection,
                session_id,
                "FINAL_RESET_PENDING",
                {
                    "evidenceHash": evidence_hash,
                    "newEntriesBlocked": True,
                    "realOrdersEnabled": False,
                    "durableCapabilityCleared": True,
                },
            )
            connection.commit()
        return self.session(session_id)

    def complete_final_reset(
        self,
        session_id: str,
        *,
        evidence_hash: str,
    ) -> dict[str, Any]:
        normalized_hash = _require_hash(
            evidence_hash, "upbit-final-evidence-hash"
        )
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                """SELECT state,capability_hash,final_evidence_hash
                FROM upbit_functional_sessions WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if session is None:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-session-missing")
            if session["state"] == "FINALIZED":
                if session["final_evidence_hash"] != normalized_hash:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-final-reset-evidence-mismatch"
                    )
                connection.commit()
                return self.session(session_id)
            if (
                session["state"] != "FINAL_RESET_PENDING"
                or session["capability_hash"]
                or session["final_evidence_hash"] != normalized_hash
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-final-reset-pending-state-invalid"
                )
            connection.execute(
                """UPDATE upbit_functional_sessions
                SET state='FINALIZED',new_entries_blocked=1,
                    real_orders_enabled=0
                WHERE session_id=?""",
                (session_id,),
            )
            self._event(
                connection,
                session_id,
                "FINALIZED",
                {
                    "evidenceHash": normalized_hash,
                    "newEntriesBlocked": True,
                    "realOrdersEnabled": False,
                    "runtimeCapabilityCleared": True,
                },
            )
            connection.commit()
        return self.session(session_id)

    def final_evidence(self, session_id: str) -> dict[str, Any]:
        session = self.session(session_id)
        raw = _text(session.get("final_evidence_json"))
        if not raw:
            raise UpbitFunctionalBlocked("upbit-final-evidence-missing")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-final-evidence-invalid"
            ) from exc
        if not isinstance(value, Mapping) or _stable_hash(value) != session.get(
            "final_evidence_hash"
        ):
            raise UpbitFunctionalBlocked("upbit-final-evidence-hash-mismatch")
        return dict(value)

    def seal_terminal_truth(
        self,
        session_id: str,
        truth: Mapping[str, Any],
    ) -> str:
        """Persist the normalized official terminal rows apart from evidence.

        The approval boundary later joins this immutable snapshot with durable
        claims and the private journal instead of trusting producer booleans.
        """

        value = dict(truth)
        if value.get("sessionId") != session_id:
            raise UpbitFunctionalBlocked(
                "upbit-terminal-truth-session-mismatch"
            )
        observed_at = _utc(
            value.get("observedAt"), "upbit-terminal-truth-observed-at"
        )
        raw_snapshot = value.pop("officialRestRawSnapshot", None)
        raw_snapshot_hash = value.get("officialRestRawSnapshotHash")
        if (
            not isinstance(raw_snapshot, Mapping)
            or not _exact_lower_hash(raw_snapshot_hash)
            or not hmac.compare_digest(
                raw_snapshot_hash, _stable_hash(raw_snapshot)
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-terminal-raw-truth-invalid"
            )
        raw_snapshot_json = json.dumps(
            dict(raw_snapshot),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        raw = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        truth_hash = _stable_hash(value)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT * FROM upbit_functional_sessions WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if session is None or session["state"] != "CLEANUP":
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-terminal-truth-session-not-cleanup"
                )
            existing = connection.execute(
                """SELECT truth_json,truth_hash FROM
                upbit_functional_terminal_truth WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["truth_json"] != raw
                    or not hmac.compare_digest(
                        existing["truth_hash"], truth_hash
                    )
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-terminal-truth-seal-mismatch"
                    )
                connection.commit()
                return truth_hash
            raw_existing = connection.execute(
                """SELECT * FROM upbit_functional_terminal_raw_truth
                WHERE session_id=?""",
                (session_id,),
            ).fetchone()
            if raw_existing is not None:
                if (
                    raw_existing["raw_json"] != raw_snapshot_json
                    or not hmac.compare_digest(
                        raw_existing["raw_hash"], raw_snapshot_hash
                    )
                ):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-terminal-raw-truth-seal-mismatch"
                    )
            else:
                connection.execute(
                    """INSERT INTO upbit_functional_terminal_raw_truth
                    (session_id,account_fingerprint,permit_hash,
                     route_scope_hash,cutoff,raw_json,raw_hash,observed_at)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (
                        session_id,
                        _text(value.get("accountFingerprint")).lower(),
                        session["permit_hash"],
                        session["scope_hash"],
                        _text(raw_snapshot.get("observationCutoff")),
                        raw_snapshot_json,
                        raw_snapshot_hash,
                        _utc_text(observed_at),
                    ),
                )
            connection.execute(
                """INSERT INTO upbit_functional_terminal_truth
                (session_id,truth_json,truth_hash,observed_at)
                VALUES (?,?,?,?)""",
                (session_id, raw, truth_hash, _utc_text(observed_at)),
            )
            connection.commit()
        return truth_hash


class UpbitContinuousFunctionalService:
    def __init__(
        self,
        *,
        ledger: UpbitFunctionalLedger,
        scope: UpbitPermitScope,
        session_id: str,
        truth_reader: TruthReader,
        post_order: PostOrder,
        cancel_order: CancelOrder,
        lease_factory: LeaseFactory,
        runtime_reader: Callable[[], Mapping[str, Any]],
        immutable_selection_reader: Callable[[], Mapping[str, Any]],
        runtime_capability_registrar: Callable[[str], None],
        real_orders_reader: Callable[[], bool],
        terminal_stream_prepare: TerminalStreamPrepare,
        terminal_stream_commit: TerminalStreamCommit,
        terminal_stream_barrier: TerminalStreamBarrier,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
        activation_monotonic: float | None,
        monotonic_continuity: bool,
        account_exclusivity_verifier: (
            AccountExclusivityProofVerifier | None
        ),
        account_exclusivity_verifier_pin: Mapping[str, Any] | None,
        capability: object,
        runtime_capability_secret: str,
    ) -> None:
        if capability not in {_ACTIVATION_CAPABILITY, _TEST_CAPABILITY}:
            raise UpbitFunctionalBlocked("upbit-functional-capability-required")
        self.ledger = ledger
        self.scope = scope
        self.session_id = session_id
        self.truth_reader = truth_reader
        self.post_order = post_order
        self.cancel_order = cancel_order
        self.lease_factory = lease_factory
        self.runtime_reader = runtime_reader
        self.immutable_selection_reader = immutable_selection_reader
        self.runtime_capability_registrar = runtime_capability_registrar
        self.real_orders_reader = real_orders_reader
        self.terminal_stream_prepare = terminal_stream_prepare
        self.terminal_stream_commit = terminal_stream_commit
        self.terminal_stream_barrier = terminal_stream_barrier
        self.clock = clock
        self.monotonic_clock = monotonic_clock
        self._activation_monotonic = activation_monotonic
        self._monotonic_continuity = monotonic_continuity
        self.account_exclusivity_verifier = account_exclusivity_verifier
        self.account_exclusivity_verifier_pin = (
            dict(account_exclusivity_verifier_pin)
            if isinstance(account_exclusivity_verifier_pin, Mapping)
            else None
        )
        self._capability = capability
        self.__runtime_capability_secret = runtime_capability_secret

    @classmethod
    def activate(
        cls,
        *,
        permit: FunctionalTestPermit | Mapping[str, Any],
        ledger: UpbitFunctionalLedger,
        session_id: str,
        truth_reader: TruthReader,
        post_order: PostOrder,
        cancel_order: CancelOrder,
        lease_factory: LeaseFactory,
        runtime_reader: Callable[[], Mapping[str, Any]],
        immutable_selection_reader: Callable[[], Mapping[str, Any]],
        runtime_capability_registrar: Callable[[str], None],
        real_orders_reader: Callable[[], bool],
        terminal_stream_prepare: TerminalStreamPrepare | None = None,
        terminal_stream_commit: TerminalStreamCommit | None = None,
        terminal_stream_barrier: TerminalStreamBarrier | None = None,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float] | None = None,
        account_exclusivity_verifier: (
            AccountExclusivityProofVerifier | None
        ) = None,
        account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
        _capability: object | None = None,
    ) -> "UpbitContinuousFunctionalService":
        capability = _capability
        if capability is not _TEST_CAPABILITY:
            if not UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE:
                raise UpbitFunctionalBlocked("upbit-continuous-functional-availability-false")
            if not UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED:
                raise UpbitFunctionalBlocked(
                    "upbit-account-exclusivity-authority-unpinned"
                )
            capability = _ACTIVATION_CAPABILITY
        if (
            terminal_stream_prepare is None
            or terminal_stream_commit is None
            or terminal_stream_barrier is None
        ):
            if capability is not _TEST_CAPABILITY:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-terminal-stream-sealer-required"
                )

            def terminal_stream_prepare(
                *, session_id: str, identifiers: tuple[str, ...]
            ) -> Mapping[str, Any]:
                raw = truth_reader(
                    session_id=session_id,
                    phase="FINAL_STREAM_CURSOR",
                    identifiers=identifiers,
                )
                body = {
                    "schemaVersion": "upbit-functional-private-terminal-seal/v1",
                    "sessionId": session_id,
                    "accountFingerprint": _text(raw.get("accountFingerprint")),
                    "channel": "myOrder",
                    "writerGeneration": int(raw.get("privateStreamWriterGeneration") or 0),
                    "journalRevision": int(raw.get("privateStreamRevision") or 0),
                    "eventCursor": int(raw.get("privateStreamEventCursor") or 0),
                    "lastEventId": _text(raw.get("privateStreamLastEventId")),
                    "eventHeadHash": _text(raw.get("privateStreamEventHeadHash")),
                    "ownedIdentifiers": sorted(set(identifiers)),
                    "ownedIdentifiersHash": _stable_hash(sorted(set(identifiers))),
                    "externalActivityAbsent": True,
                    "streamContinuous": raw.get("privateStreamComplete") is True,
                    "cleanupOnlyRecovery": raw.get("privateStreamRecoveryAttested") is True,
                    "gapDetected": raw.get("privateStreamGapDetected") is True,
                    "observedAt": _utc_text(clock()),
                }
                return {**body, "sealHash": _stable_hash(body)}

            def terminal_stream_commit(
                *,
                session_id: str,
                identifiers: tuple[str, ...],
                expected: Mapping[str, Any],
            ) -> Mapping[str, Any]:
                return dict(expected)

            def terminal_stream_barrier(
                *, session_id: str
            ) -> Mapping[str, Any]:
                return {"cutoffEstablished": True, "sessionId": session_id}
        monotonic_reader = monotonic_clock or time.monotonic
        activation_monotonic = float(monotonic_reader())
        immutable_selection = immutable_selection_reader()
        scope = UpbitPermitScope.parse(
            permit,
            immutable_selection=immutable_selection,
        )
        now = _utc(clock(), "upbit-current-time")
        if not (scope.starts_at <= now < scope.ends_at):
            raise UpbitFunctionalBlocked("upbit-permit-not-active")
        runtime = dict(runtime_reader())
        cls._assert_runtime(
            runtime,
            scope,
            session_id,
            cleanup=False,
            activation=True,
            capability_hash="",
        )
        if real_orders_reader() is not False:
            raise UpbitFunctionalBlocked(
                "upbit-activation-real-orders-must-start-off"
            )
        truth = UpbitTruth.parse(
            truth_reader(session_id=session_id, phase="BASELINE", identifiers=()),
            account_fingerprint=scope.account_fingerprint,
            now=now,
            session_id=session_id,
            session_started_at=scope.starts_at,
            account_exclusivity_verifier=account_exclusivity_verifier,
            account_exclusivity_verifier_pin=(
                account_exclusivity_verifier_pin
            ),
        )
        if truth.account_exclusivity_proof_verified is not True:
            raise UpbitFunctionalBlocked(
                "upbit-activation-account-exclusivity-proof-required"
            )
        runtime_capability_secret = secrets.token_urlsafe(48)
        runtime_capability_hash = hashlib.sha256(
            runtime_capability_secret.encode("utf-8")
        ).hexdigest()
        runtime_capability_registrar(runtime_capability_hash)
        try:
            cls._assert_runtime(
                runtime_reader(),
                scope,
                session_id,
                cleanup=False,
                activation=True,
                capability_hash=runtime_capability_hash,
            )
            ledger.activate(
                scope,
                truth,
                session_id=session_id,
                capability_hash=runtime_capability_hash,
            )
        except Exception:
            runtime_capability_registrar("")
            raise
        return cls(
            ledger=ledger,
            scope=scope,
            session_id=session_id,
            truth_reader=truth_reader,
            post_order=post_order,
            cancel_order=cancel_order,
            lease_factory=lease_factory,
            runtime_reader=runtime_reader,
            immutable_selection_reader=immutable_selection_reader,
            runtime_capability_registrar=runtime_capability_registrar,
            real_orders_reader=real_orders_reader,
            terminal_stream_prepare=terminal_stream_prepare,
            terminal_stream_commit=terminal_stream_commit,
            terminal_stream_barrier=terminal_stream_barrier,
            clock=clock,
            monotonic_clock=monotonic_reader,
            activation_monotonic=activation_monotonic,
            monotonic_continuity=True,
            account_exclusivity_verifier=account_exclusivity_verifier,
            account_exclusivity_verifier_pin=(
                account_exclusivity_verifier_pin
            ),
            capability=capability,
            runtime_capability_secret=runtime_capability_secret,
        )

    @classmethod
    def reattach_cleanup_after_owner_loss(
        cls,
        *,
        permit: FunctionalTestPermit | Mapping[str, Any],
        ledger: UpbitFunctionalLedger,
        session_id: str,
        owner_recovery_attestation: Mapping[str, Any],
        truth_reader: TruthReader,
        post_order: PostOrder,
        cancel_order: CancelOrder,
        lease_factory: LeaseFactory,
        runtime_reader: Callable[[], Mapping[str, Any]],
        immutable_selection_reader: Callable[[], Mapping[str, Any]],
        runtime_capability_registrar: Callable[[str], None],
        real_orders_reader: Callable[[], bool],
        terminal_stream_prepare: TerminalStreamPrepare | None = None,
        terminal_stream_commit: TerminalStreamCommit | None = None,
        terminal_stream_barrier: TerminalStreamBarrier | None = None,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float] | None = None,
        account_exclusivity_verifier: (
            AccountExclusivityProofVerifier | None
        ) = None,
        account_exclusivity_verifier_pin: Mapping[str, Any] | None = None,
        _capability: object | None = None,
    ) -> "UpbitContinuousFunctionalService":
        capability = _capability
        if capability is not _TEST_CAPABILITY:
            if not UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE:
                raise UpbitFunctionalBlocked(
                    "upbit-continuous-functional-availability-false"
                )
            capability = _ACTIVATION_CAPABILITY
        if (
            terminal_stream_prepare is None
            or terminal_stream_commit is None
            or terminal_stream_barrier is None
        ):
            if capability is not _TEST_CAPABILITY:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-terminal-stream-sealer-required"
                )

            def terminal_stream_prepare(
                *, session_id: str, identifiers: tuple[str, ...]
            ) -> Mapping[str, Any]:
                raw = truth_reader(
                    session_id=session_id,
                    phase="FINAL_STREAM_CURSOR",
                    identifiers=identifiers,
                )
                body = {
                    "schemaVersion": "upbit-functional-private-terminal-seal/v1",
                    "sessionId": session_id,
                    "accountFingerprint": _text(raw.get("accountFingerprint")),
                    "channel": "myOrder",
                    "writerGeneration": int(raw.get("privateStreamWriterGeneration") or 0),
                    "journalRevision": int(raw.get("privateStreamRevision") or 0),
                    "eventCursor": int(raw.get("privateStreamEventCursor") or 0),
                    "lastEventId": _text(raw.get("privateStreamLastEventId")),
                    "eventHeadHash": _text(raw.get("privateStreamEventHeadHash")),
                    "ownedIdentifiers": sorted(set(identifiers)),
                    "ownedIdentifiersHash": _stable_hash(sorted(set(identifiers))),
                    "externalActivityAbsent": True,
                    "streamContinuous": raw.get("privateStreamComplete") is True,
                    "cleanupOnlyRecovery": raw.get("privateStreamRecoveryAttested") is True,
                    "gapDetected": raw.get("privateStreamGapDetected") is True,
                    "observedAt": _utc_text(clock()),
                }
                return {**body, "sealHash": _stable_hash(body)}

            def terminal_stream_commit(
                *,
                session_id: str,
                identifiers: tuple[str, ...],
                expected: Mapping[str, Any],
            ) -> Mapping[str, Any]:
                return dict(expected)

            def terminal_stream_barrier(
                *, session_id: str
            ) -> Mapping[str, Any]:
                return {"cutoffEstablished": True, "sessionId": session_id}
        scope = UpbitPermitScope.parse(
            permit,
            immutable_selection=immutable_selection_reader(),
        )
        now = _utc(clock(), "upbit-current-time")
        attestation = dict(owner_recovery_attestation)
        exact = {
            "schemaVersion": "upbit-functional-recovery-approval/v1",
            "mode": "CLEANUP_ONLY",
            "sessionId": session_id,
            "permitId": scope.permit_id,
            "permitHash": scope.permit_hash,
            "accountFingerprint": scope.account_fingerprint,
            "approvalState": "ACTIVE",
        }
        for field, expected in exact.items():
            if not hmac.compare_digest(_text(attestation.get(field)), expected):
                raise UpbitFunctionalBlocked(
                    f"upbit-owner-recovery-{field}-mismatch"
                )
        if (
            attestation.get("serverManaged") is not True
            or attestation.get("operatorAuthenticated") is not True
            or attestation.get("operatorApproved") is not True
            or attestation.get("singleUse") is not True
            or attestation.get("previousOwnerLost") is not True
            or attestation.get("previousOwnerLeaseExpired") is not True
            or attestation.get("officialRestReconciled") is not True
        ):
            raise UpbitFunctionalBlocked(
                "upbit-owner-recovery-attestation-incomplete"
            )
        observed_at = _utc(
            attestation.get("observedAt"),
            "upbit-owner-recovery-observed-at",
        )
        age = (now - observed_at).total_seconds()
        if age < 0 or age > MAX_TRUTH_AGE_SECONDS:
            raise UpbitFunctionalBlocked(
                "upbit-owner-recovery-attestation-stale"
            )
        recovery_id = _text(attestation.get("recoveryId"))
        recovery_hash = _require_hash(
            attestation.get("contentHash"),
            "upbit-owner-recovery-content-hash",
        )
        expected_recovery_hash = _stable_hash(
            {
                key: value
                for key, value in attestation.items()
                if key != "contentHash"
            }
        )
        if not hmac.compare_digest(recovery_hash, expected_recovery_hash):
            raise UpbitFunctionalBlocked(
                "upbit-owner-recovery-content-hash-mismatch"
            )
        durable = ledger.session(session_id)
        if (
            durable["permit_id"] != scope.permit_id
            or durable["permit_hash"] != scope.permit_hash
            or durable["scope_hash"] != _stable_hash(scope.snapshot())
        ):
            raise UpbitFunctionalBlocked(
                "upbit-owner-recovery-durable-scope-mismatch"
            )
        if now >= scope.cleanup_deadline:
            raise UpbitFunctionalBlocked(
                "upbit-cleanup-deadline-expired-manual-intervention-required"
            )
        ledger.claim_recovery_approval(
            session_id=session_id,
            recovery_id=recovery_id,
            recovery_hash=recovery_hash,
            claimed_at=now,
        )
        if durable["state"] == "ACTIVE":
            durable = ledger.enter_cleanup(
                session_id,
                reason="owner-loss-restart-recovery",
            )
        if durable["state"] != "CLEANUP":
            raise UpbitFunctionalBlocked(
                "upbit-owner-recovery-cleanup-state-required"
            )
        runtime_capability_secret = secrets.token_urlsafe(48)
        runtime_capability_hash = hashlib.sha256(
            runtime_capability_secret.encode("utf-8")
        ).hexdigest()
        ledger.rotate_cleanup_capability(
            session_id,
            capability_hash=runtime_capability_hash,
        )
        try:
            runtime_capability_registrar(runtime_capability_hash)
            cls._assert_runtime(
                runtime_reader(),
                scope,
                session_id,
                cleanup=True,
                capability_hash=runtime_capability_hash,
            )
        except Exception:
            # The raw secret has not escaped this method.  Revoke the durable
            # hash first, then best-effort clear any partially registered
            # runtime pointer; a subsequent cleanup recovery may rotate again.
            ledger.revoke_cleanup_capability(
                session_id,
                reason="cleanup-capability-registration-failed",
            )
            try:
                runtime_capability_registrar("")
            except Exception:
                pass
            raise
        monotonic_reader = monotonic_clock or time.monotonic
        return cls(
            ledger=ledger,
            scope=scope,
            session_id=session_id,
            truth_reader=truth_reader,
            post_order=post_order,
            cancel_order=cancel_order,
            lease_factory=lease_factory,
            runtime_reader=runtime_reader,
            immutable_selection_reader=immutable_selection_reader,
            runtime_capability_registrar=runtime_capability_registrar,
            real_orders_reader=real_orders_reader,
            terminal_stream_prepare=terminal_stream_prepare,
            terminal_stream_commit=terminal_stream_commit,
            terminal_stream_barrier=terminal_stream_barrier,
            clock=clock,
            monotonic_clock=monotonic_reader,
            activation_monotonic=None,
            monotonic_continuity=False,
            account_exclusivity_verifier=account_exclusivity_verifier,
            account_exclusivity_verifier_pin=(
                account_exclusivity_verifier_pin
            ),
            capability=capability,
            runtime_capability_secret=runtime_capability_secret,
        )

    @staticmethod
    def _assert_runtime(
        runtime: Mapping[str, Any],
        scope: UpbitPermitScope,
        session_id: str,
        *,
        cleanup: bool,
        activation: bool = False,
        capability_hash: str = "",
    ) -> None:
        exact = {
            "executionPurpose": EXECUTION_PURPOSE,
            "executionRoute": EXECUTION_ROUTE,
            "functionalTestSessionId": session_id,
            "functionalTestPermitId": scope.permit_id,
            "functionalTestPermitHash": scope.permit_hash,
            "functionalTestRouteScopeHash": scope.route_scope_hash,
            "functionalTestAccountFingerprint": scope.account_fingerprint,
            "functionalTestSessionScopeHash": _stable_hash(scope.snapshot()),
        }
        if capability_hash:
            exact["functionalCapabilityHash"] = capability_hash
        for field, expected in exact.items():
            actual = _text(runtime.get(field))
            if not hmac.compare_digest(actual, expected):
                raise UpbitFunctionalBlocked(f"upbit-runtime-{field}-mismatch")
        if runtime.get("killSwitch") is True and not cleanup:
            raise UpbitFunctionalBlocked("upbit-runtime-kill-switch-active")
        if runtime.get("dryRun") is not False or runtime.get("operatorConfirmed") is not True:
            raise UpbitFunctionalBlocked("upbit-runtime-live-authorization-incomplete")
        if runtime.get("functionalOnlyRouting") is not True:
            raise UpbitFunctionalBlocked("upbit-runtime-functional-only-routing-required")
        if runtime.get("ordinaryRoutesClosed") is not True:
            raise UpbitFunctionalBlocked("upbit-runtime-ordinary-routes-must-be-closed")
        if runtime.get("upbitSmokeRouteClosed") is not True:
            raise UpbitFunctionalBlocked("upbit-runtime-smoke-route-must-be-closed")
        if runtime.get("newEntriesBlocked") is not True:
            raise UpbitFunctionalBlocked("upbit-runtime-new-entries-state-mismatch")
        if activation and runtime.get("realOrdersEnabled") is not False:
            raise UpbitFunctionalBlocked("upbit-runtime-real-orders-must-start-off")

    def _assert_current_selection(self) -> None:
        selection = dict(self.immutable_selection_reader())
        expected = {
            "strategyArtifactId": self.scope.strategy_artifact_id,
            "strategyArtifactHash": self.scope.strategy_artifact_hash,
            "strategyArtifactFileSha256": self.scope.strategy_artifact_file_sha256,
            "strategyInstanceId": self.scope.strategy_instance_id,
            "strategyInstanceHash": self.scope.strategy_instance_hash,
            "strategyInstanceFileSha256": self.scope.strategy_instance_file_sha256,
            "publicationProofHash": self.scope.publication_proof_hash,
            "publicationProofFileSha256": self.scope.publication_proof_file_sha256,
            "strategyInstanceArtifactHash": self.scope.strategy_artifact_hash,
            "accountFingerprint": self.scope.account_fingerprint,
            "executionRoute": EXECUTION_ROUTE,
            "symbol": SYMBOL,
            "interval": "5m",
            "publishedProvider": "upbit",
            "publishedGroup": "crypto-upbit",
            "publishedSymbol": SYMBOL,
            "publishedStrategyArtifactHash": self.scope.strategy_artifact_hash,
            "publishedStrategyArtifactFileSha256": self.scope.strategy_artifact_file_sha256,
            "publishedStrategyInstanceHash": self.scope.strategy_instance_hash,
            "publishedStrategyInstanceFileSha256": self.scope.strategy_instance_file_sha256,
        }
        for field, expected_value in expected.items():
            if not hmac.compare_digest(
                _text(selection.get(field)),
                expected_value,
            ):
                raise UpbitFunctionalBlocked(
                    f"upbit-current-selection-{field}-mismatch"
                )
        if selection.get("verified") is not True:
            raise UpbitFunctionalBlocked("upbit-current-selection-not-verified")
        if (
            selection.get("publicationProofVerified") is not True
            or selection.get("publishedActiveCatalogVisible") is not True
            or selection.get("publishedNaturalSignalsOnly") is not True
            or selection.get("publishedPromotionEligible") is not False
        ):
            raise UpbitFunctionalBlocked(
                "upbit-current-selection-publication-policy-mismatch"
            )

    def _assert_dispatch_capability(self) -> str:
        session = self.ledger.session(self.session_id)
        durable_hash = _require_hash(
            session.get("capability_hash"),
            "upbit-durable-capability-hash",
        )
        actual_hash = hashlib.sha256(
            self.__runtime_capability_secret.encode("utf-8")
        ).hexdigest()
        if not hmac.compare_digest(actual_hash, durable_hash):
            raise UpbitFunctionalBlocked(
                "upbit-functional-runtime-capability-invalid"
            )
        return durable_hash

    def _read_truth(self, phase: str) -> UpbitTruth:
        claims = self.ledger.claims(self.session_id)
        identifiers = tuple(
            dict.fromkeys(
                identifier
                for claim in claims
                for identifier in (
                    _text(claim["identifier"]),
                    _text(claim.get("target_identifier")),
                )
                if identifier
            )
        )
        return UpbitTruth.parse(
            self.truth_reader(
                session_id=self.session_id,
                phase=phase,
                identifiers=identifiers,
            ),
            account_fingerprint=self.scope.account_fingerprint,
            now=self.clock(),
            session_id=self.session_id,
            session_started_at=self.scope.starts_at,
            account_exclusivity_verifier=(
                self.account_exclusivity_verifier
            ),
            account_exclusivity_verifier_pin=(
                self.account_exclusivity_verifier_pin
            ),
        )

    def _require_natural_buy_exclusivity(
        self, truth: UpbitTruth, *, phase: str
    ) -> None:
        if truth.account_exclusivity_proof_verified is True:
            return
        self.ledger.latch_account_exclusivity_breach(
            self.session_id,
            phase=phase,
        )
        raise UpbitFunctionalBlocked(
            "upbit-natural-buy-account-exclusivity-proof-required"
        )

    def _owned_fills(self, truth: UpbitTruth) -> tuple[Decimal, Decimal, Decimal]:
        identifiers = {
            _text(identifier)
            for claim in self.ledger.claims(self.session_id)
            for identifier in (
                claim["identifier"],
                claim.get("target_identifier"),
            )
            if _text(identifier)
        }
        bought = sold = funds_delta = Decimal("0")
        fees = Decimal("0")
        for fill in truth.fills:
            if _text(fill.get("identifier")) not in identifiers:
                continue
            volume = _decimal(fill.get("volume"), "upbit-fill-volume", minimum=Decimal("0"))
            funds = _decimal(fill.get("funds"), "upbit-fill-funds", minimum=Decimal("0"))
            fee = _decimal(fill.get("fee"), "upbit-fill-fee", minimum=Decimal("0"))
            fees += fee
            if _upper(fill.get("side")) == "BID":
                bought += volume
                funds_delta -= funds
            elif _upper(fill.get("side")) == "ASK":
                sold += volume
                funds_delta += funds
            else:
                raise UpbitFunctionalBlocked("upbit-fill-side-invalid")
        owned = bought - sold
        if owned < 0:
            raise UpbitFunctionalBlocked("upbit-owned-position-negative")
        loss = max(Decimal("0"), -funds_delta + fees - owned * truth.mark_price)
        return owned, loss, fees

    @staticmethod
    def _assert_quantity_step(quantity: Decimal, rules: UpbitOrderRules) -> None:
        if quantity <= 0 or quantity % rules.quantity_step != 0:
            raise UpbitFunctionalBlocked("upbit-sell-quantity-step-mismatch")
        if max(0, -quantity.as_tuple().exponent) > rules.quantity_scale:
            raise UpbitFunctionalBlocked("upbit-sell-quantity-precision-exceeded")

    def _validate_leg(self, leg: UpbitLeg, truth: UpbitTruth, *, cleanup: bool) -> dict[str, str]:
        session = self.ledger.session(self.session_id)
        owned, loss, _fees = self._owned_fills(truth)
        self.ledger.set_owner_risk(
            self.session_id, loss=loss, gross=owned * truth.mark_price
        )
        if loss >= self.scope.max_loss and session["state"] == "ACTIVE":
            self.ledger.enter_cleanup(self.session_id, reason="owner-loss-limit-reached")
            raise UpbitFunctionalBlocked("upbit-owner-loss-limit-reached-cleanup-only")
        if leg.side == "BID":
            if cleanup:
                raise UpbitFunctionalBlocked("upbit-cleanup-buy-forbidden")
            if leg.notional < truth.rules.bid_min_total:
                raise UpbitFunctionalBlocked("upbit-buy-below-official-minimum")
            if leg.notional > self.scope.max_order_notional:
                raise UpbitFunctionalBlocked("upbit-buy-notional-cap-exceeded")
            required_quote = leg.notional * (
                Decimal("1") + truth.rules.bid_fee_rate
            )
            if truth.quote_available < required_quote:
                raise UpbitFunctionalBlocked("upbit-buy-insufficient-quote")
            return {"market": SYMBOL, "side": "bid", "ord_type": "price", "price": _decimal_text(leg.notional)}
        if leg.side == "ASK":
            self._assert_quantity_step(leg.quantity, truth.rules)
            if leg.quantity > owned:
                raise UpbitFunctionalBlocked("upbit-sell-exceeds-owned-delta")
            if leg.quantity > truth.base_available:
                raise UpbitFunctionalBlocked(
                    "upbit-sell-exceeds-official-base-available"
                )
            if leg.quantity <= 0:
                raise UpbitFunctionalBlocked("upbit-sell-owned-delta-empty")
            if leg.quantity * truth.mark_price < truth.rules.ask_min_total:
                raise UpbitFunctionalBlocked("upbit-sell-below-official-minimum")
            return {"market": SYMBOL, "side": "ask", "ord_type": "market", "volume": _decimal_text(leg.quantity)}
        if leg.side == "CANCEL":
            if not cleanup:
                raise UpbitFunctionalBlocked("upbit-cancel-cleanup-only")
            matches = [row for row in truth.open_orders if _text(row.get("identifier")) == leg.target_order_identifier]
            owned_ids = {
                _text(identifier)
                for claim in self.ledger.claims(self.session_id)
                for identifier in (
                    claim["identifier"],
                    claim.get("target_identifier"),
                )
                if _text(identifier)
            }
            if len(matches) != 1 or leg.target_order_identifier not in owned_ids:
                raise UpbitFunctionalBlocked("upbit-cancel-owned-working-order-required")
            return {"identifier": leg.target_order_identifier}
        raise UpbitFunctionalBlocked("upbit-leg-side-invalid")

    def assess_risk(self) -> dict[str, Any]:
        """Refresh owner-only risk even when the strategy emits no order.

        This read-only check is intentionally independent of strategy signals so
        a HOLD-only stretch cannot defer the loss, gross-exposure, or permit
        expiry latch until another order happens to be requested.
        """

        now = _utc(self.clock(), "upbit-current-time")
        session = self.ledger.session(self.session_id)
        if session["state"] == "FINALIZED":
            raise UpbitFunctionalBlocked("upbit-risk-session-finalized")
        if session["state"] == "ACTIVE" and now >= self.scope.ends_at:
            self.ledger.enter_cleanup(self.session_id, reason="permit-expired")
            return {
                "ok": True,
                "action": "CLEANUP",
                "reason": "permit-expired",
            }

        truth = self._read_truth("RISK_MONITOR")
        owned, loss, fees = self._owned_fills(truth)
        gross = owned * truth.mark_price
        self.ledger.set_owner_risk(
            self.session_id, loss=loss, gross=gross
        )
        reason = ""
        if session["state"] == "ACTIVE":
            if loss >= self.scope.max_loss:
                reason = "owner-loss-limit-reached"
            elif gross > self.scope.max_gross_exposure:
                reason = "owner-gross-exposure-limit-reached"
            if reason:
                self.ledger.enter_cleanup(self.session_id, reason=reason)
        durable = self.ledger.session(self.session_id)
        return {
            "ok": True,
            "action": "CLEANUP" if durable["state"] == "CLEANUP" else "MONITOR",
            "reason": reason,
            "ownedQuantity": _decimal_text(owned),
            "ownerGrossExposure": _decimal_text(gross),
            "ownerLoss": _decimal_text(loss),
            "fees": _decimal_text(fees),
        }

    def on_bar(self, value: Mapping[str, Any], *, buy_notional: object = 10_000) -> dict[str, Any]:
        now = _utc(self.clock(), "upbit-current-time")
        if now >= self.scope.ends_at:
            self.ledger.enter_cleanup(self.session_id, reason="permit-expired")
            return {"ok": True, "action": "CLEANUP", "reason": "permit-expired"}
        bar = FinalizedFiveMinuteBar.parse(
            value,
            now=now,
            strategy_artifact_id=self.scope.strategy_artifact_id,
            strategy_artifact_hash=self.scope.strategy_artifact_hash,
            strategy_artifact_file_sha256=self.scope.strategy_artifact_file_sha256,
            strategy_instance_id=self.scope.strategy_instance_id,
            strategy_instance_hash=self.scope.strategy_instance_hash,
            strategy_instance_file_sha256=self.scope.strategy_instance_file_sha256,
            publication_proof_hash=self.scope.publication_proof_hash,
            publication_proof_file_sha256=self.scope.publication_proof_file_sha256,
        )
        session = self.ledger.session(self.session_id)
        if (
            bar.closed_at < self.scope.starts_at
            or bar.closed_at >= self.scope.ends_at
            or bar.evaluation_observed_at < self.scope.starts_at
            or bar.evaluation_observed_at >= self.scope.ends_at
        ):
            raise UpbitFunctionalBlocked(
                "upbit-strategy-bar-outside-active-permit-window"
            )
        if session["last_bar_id"] == bar.bar_id:
            raise UpbitFunctionalBlocked("upbit-bar-already-consumed")
        self.ledger.note_bar(
            self.session_id,
            bar,
        )
        risk = self.assess_risk()
        if risk["action"] == "CLEANUP":
            return risk
        consumed_strategy_slots = {
            _text(claim.get("slot"))
            for claim in self.ledger.claims(self.session_id)
            if _text(claim.get("slot")) in {"STRATEGY_BUY", "STRATEGY_SELL"}
            and (
                _text(claim.get("state")) != "BLOCKED_BEFORE_POST"
                or int(claim.get("proven_not_sent_retries") or 0) >= 1
            )
        }
        if (
            bar.signal == "BUY"
            and "STRATEGY_BUY" in consumed_strategy_slots
        ) or (
            bar.signal == "SELL"
            and "STRATEGY_SELL" in consumed_strategy_slots
        ):
            # The strategy lane is deliberately one natural BUY plus one
            # natural SELL.  Later crossovers are still provenance-checked
            # and durably consume their finalized bar, but never reach the
            # claim/mutation edge or turn a normal 2h run into an exception.
            return {
                "ok": True,
                "action": "HOLD",
                "reason": "NO_REENTRY_STRATEGY_SLOT_CONSUMED",
                "barId": bar.bar_id,
            }
        if bar.signal == "HOLD":
            return {"ok": True, "action": "HOLD", "barId": bar.bar_id}
        if bar.signal == "BUY":
            leg = UpbitLeg.buy(buy_notional)
        else:
            leg = self._natural_sell_leg()
            if leg is None:
                return {
                    "ok": True,
                    "action": "HOLD",
                    "reason": "sell-signal-without-owned-position",
                    "barId": bar.bar_id,
                }
        return self.dispatch(leg, natural_evaluation=bar)

    def _natural_sell_leg(self) -> UpbitLeg | None:
        truth = self._read_truth("SELL_SIGNAL_PREVIEW")
        owned, _loss, _fees = self._owned_fills(truth)
        if owned == 0:
            return None
        return UpbitLeg.sell(owned)

    def dispatch(
        self,
        leg: UpbitLeg,
        *,
        natural_evaluation: FinalizedFiveMinuteBar | None = None,
    ) -> dict[str, Any]:
        now = _utc(self.clock(), "upbit-current-time")
        session = self.ledger.session(self.session_id)
        cleanup = session["state"] == "CLEANUP"
        if now >= self.scope.cleanup_deadline:
            raise UpbitFunctionalBlocked("upbit-cleanup-deadline-expired-manual-intervention-required")
        if not cleanup and now >= self.scope.ends_at:
            self.ledger.enter_cleanup(
                self.session_id, reason="permit-expired-before-strategy-claim"
            )
            raise UpbitFunctionalBlocked(
                "upbit-strategy-claim-after-permit-expiry"
            )
        self._assert_current_selection()
        capability_hash = self._assert_dispatch_capability()
        self._assert_runtime(
            self.runtime_reader(),
            self.scope,
            self.session_id,
            cleanup=cleanup,
            capability_hash=capability_hash,
        )
        if self.real_orders_reader() is not True:
            raise UpbitFunctionalBlocked("upbit-real-orders-global-flag-off")
        truth = self._read_truth("PRE_DISPATCH")
        if not cleanup and leg.slot == "STRATEGY_BUY":
            self._require_natural_buy_exclusivity(
                truth,
                phase="PRE_DISPATCH",
            )
        sealed_payload = self._validate_leg(leg, truth, cleanup=cleanup)
        claim = self.ledger.claim(
            self.session_id,
            leg,
            sealed_payload,
            functional_capability_verified=True,
            natural_evaluation=natural_evaluation,
        )
        payload = (
            dict(sealed_payload)
            if leg.side == "CANCEL"
            else {**sealed_payload, "identifier": claim["identifier"]}
        )
        post_boundary_entered = False
        try:
            with self.lease_factory(session_id=self.session_id, claim_id=claim["claimId"]) as lease_reader:
                self._assert_current_selection()
                capability_hash = self._assert_dispatch_capability()
                self._assert_runtime(
                    self.runtime_reader(),
                    self.scope,
                    self.session_id,
                    cleanup=cleanup,
                    capability_hash=capability_hash,
                )
                dispatch_truth = self._read_truth("FINAL_PRE_POST")
                if not cleanup and leg.slot == "STRATEGY_BUY":
                    self._require_natural_buy_exclusivity(
                        dispatch_truth,
                        phase="FINAL_PRE_POST",
                    )
                dispatch_payload = self._validate_leg(
                    leg,
                    dispatch_truth,
                    cleanup=cleanup,
                )
                if dispatch_payload != sealed_payload:
                    raise UpbitFunctionalBlocked(
                        "upbit-final-pre-post-truth-changed"
                    )
                if claim["identifier"] not in dispatch_truth.identifier_truth:
                    raise UpbitFunctionalBlocked(
                        "upbit-final-pre-post-identifier-truth-missing"
                    )
                if dispatch_truth.identifier_truth[claim["identifier"]] is not None:
                    raise UpbitFunctionalBlocked(
                        "upbit-final-pre-post-identifier-already-used"
                    )
                lease = dict(lease_reader())
                if (
                    lease.get("active") is not True
                    or _text(lease.get("sessionId")) != self.session_id
                    or _text(lease.get("claimId")) != claim["claimId"]
                    or _text(lease.get("permitHash")) != self.scope.permit_hash
                ):
                    raise UpbitFunctionalBlocked("upbit-final-dispatch-lease-invalid")
                if self.real_orders_reader() is not True:
                    raise UpbitFunctionalBlocked("upbit-real-orders-disabled-at-post-boundary")
                if not cleanup and _utc(
                    self.clock(), "upbit-final-post-time"
                ) >= self.scope.ends_at:
                    self.ledger.enter_cleanup(
                        self.session_id,
                        reason="permit-expired-at-final-post-boundary",
                    )
                    raise UpbitFunctionalBlocked(
                        "upbit-strategy-post-after-permit-expiry"
                    )
                # From this assignment until exact REST/private-stream
                # reconciliation, any untyped exception is an unknown broker
                # outcome and can never be retried blindly.
                post_boundary_entered = True
                response = (
                    self.cancel_order(
                        identifier=leg.target_order_identifier,
                        functional_capability=self.__runtime_capability_secret,
                        functional_action=leg.slot,
                        claim_id=claim["claimId"],
                        request_hash=claim["requestHash"],
                    )
                    if leg.side == "CANCEL"
                    else self.post_order(
                        payload,
                        functional_capability=self.__runtime_capability_secret,
                        functional_action=leg.slot,
                        claim_id=claim["claimId"],
                        request_hash=claim["requestHash"],
                    )
                )
        except UpbitBrokerPostNotSent as exc:
            self.ledger.resolve_claim(
                claim["claimId"],
                state="BLOCKED_BEFORE_POST",
            )
            raise UpbitFunctionalBlocked(
                "upbit-broker-adapter-proved-post-not-sent"
            ) from exc
        except Exception as exc:
            if not post_boundary_entered:
                self.ledger.resolve_claim(
                    claim["claimId"],
                    state="BLOCKED_BEFORE_POST",
                )
                if isinstance(exc, UpbitFunctionalBlocked):
                    raise
                raise UpbitFunctionalBlocked(
                    "upbit-pre-post-boundary-failed-not-sent"
                ) from exc
            self.ledger.resolve_claim(claim["claimId"], state="AMBIGUOUS")
            self.ledger.enter_cleanup(self.session_id, reason="ambiguous-broker-outcome")
            raise UpbitFunctionalAmbiguous("upbit-broker-outcome-ambiguous-no-blind-retry") from exc
        if not isinstance(response, Mapping) or not _text(response.get("uuid")):
            self.ledger.resolve_claim(claim["claimId"], state="AMBIGUOUS", response=dict(response or {}))
            self.ledger.enter_cleanup(self.session_id, reason="broker-receipt-incomplete")
            raise UpbitFunctionalAmbiguous("upbit-broker-receipt-incomplete-no-blind-retry")
        try:
            post_truth = self._read_truth("POST_SUBMIT")
            lookup_identifier = (
                leg.target_order_identifier
                if leg.side == "CANCEL"
                else claim["identifier"]
            )
            exact_order = post_truth.identifier_truth.get(lookup_identifier)
            if not isinstance(exact_order, Mapping):
                raise UpbitFunctionalAmbiguous(
                    "upbit-post-submit-exact-order-missing"
                )
            if (
                _text(exact_order.get("uuid")) != _text(response.get("uuid"))
                or _text(exact_order.get("identifier")) != lookup_identifier
                or _upper(exact_order.get("market")) != SYMBOL
            ):
                raise UpbitFunctionalAmbiguous(
                    "upbit-post-submit-exact-order-mismatch"
                )
            if leg.side == "CANCEL":
                if _text(exact_order.get("state")).lower() not in {"cancel", "done"}:
                    raise UpbitFunctionalAmbiguous(
                        "upbit-post-cancel-order-not-terminal"
                    )
            elif _upper(exact_order.get("side")) != leg.side:
                raise UpbitFunctionalAmbiguous(
                    "upbit-post-submit-side-mismatch"
                )
        except UpbitFunctionalAmbiguous:
            self.ledger.resolve_claim(
                claim["claimId"],
                state="AMBIGUOUS",
                response=response,
            )
            self.ledger.enter_cleanup(
                self.session_id,
                reason="post-submit-truth-ambiguous",
            )
            raise
        except Exception as exc:
            self.ledger.resolve_claim(
                claim["claimId"],
                state="AMBIGUOUS",
                response=response,
            )
            self.ledger.enter_cleanup(
                self.session_id,
                reason="post-submit-truth-error",
            )
            raise UpbitFunctionalAmbiguous(
                "upbit-post-submit-truth-error-no-blind-retry"
            ) from exc
        post_owned, post_loss, _post_fees = self._owned_fills(post_truth)
        post_gross = post_owned * post_truth.mark_price
        self.ledger.set_owner_risk(
            self.session_id, loss=post_loss, gross=post_gross
        )
        self.ledger.resolve_claim(claim["claimId"], state="RECONCILED", response=response)
        if self.ledger.session(self.session_id)["state"] == "ACTIVE":
            if post_loss >= self.scope.max_loss:
                self.ledger.enter_cleanup(
                    self.session_id, reason="owner-loss-limit-reached"
                )
            elif post_gross > self.scope.max_gross_exposure:
                self.ledger.enter_cleanup(
                    self.session_id,
                    reason="owner-gross-exposure-limit-reached",
                )
        return {"ok": True, "action": leg.slot, "claimId": claim["claimId"], "identifier": claim["identifier"], "brokerOrderId": _text(response.get("uuid")), "evidenceClass": EVIDENCE_CLASS, "promotionEligible": False}

    def recover_or_expire(self, *, reason: str = "restart-recovery") -> dict[str, Any]:
        session = self.ledger.session(self.session_id)
        now = _utc(self.clock(), "upbit-current-time")
        if session["state"] == "ACTIVE" and (now >= self.scope.ends_at or reason != "status-check"):
            session = self.ledger.enter_cleanup(self.session_id, reason=reason)
        return session

    def fail_closed_revoke(self, *, reason: str) -> dict[str, Any]:
        """Leave an activation failure in durable cleanup-only recovery."""

        session = self.ledger.session(self.session_id)
        if session["state"] == "ACTIVE":
            session = self.ledger.enter_cleanup(
                self.session_id,
                reason=reason,
            )
        if session["state"] != "CLEANUP":
            raise UpbitFunctionalBlocked(
                "upbit-fail-closed-cleanup-state-required"
            )
        revoked = self.ledger.revoke_cleanup_capability(
            self.session_id,
            reason=reason,
        )
        self.__runtime_capability_secret = ""
        self.runtime_capability_registrar("")
        return revoked

    def cleanup_plan(self) -> dict[str, Any]:
        session = self.ledger.session(self.session_id)
        if session["state"] != "CLEANUP":
            raise UpbitFunctionalBlocked("upbit-cleanup-state-required")
        truth = self._read_truth("CLEANUP")
        self._reconcile_ambiguous_claims(truth)
        owned_ids = {
            _text(identifier)
            for claim in self.ledger.claims(self.session_id)
            for identifier in (
                claim["identifier"],
                claim.get("target_identifier"),
            )
            if _text(identifier)
        }
        owned_working = [row for row in truth.open_orders if _text(row.get("identifier")) in owned_ids]
        nonowned_working = [
            row
            for row in truth.open_orders
            if _text(row.get("identifier")) not in owned_ids
        ]
        if nonowned_working:
            raise UpbitFunctionalBlocked(
                "upbit-cleanup-nonowned-working-order-present"
            )
        if len(owned_working) > 1:
            raise UpbitFunctionalBlocked("upbit-cleanup-multiple-owned-working-orders")
        owned, loss, fees = self._owned_fills(truth)
        self.ledger.set_owner_loss(self.session_id, loss)
        actions: list[UpbitLeg] = []
        claims = self.ledger.claims(self.session_id)
        cancel_claims = [
            claim for claim in claims if claim["slot"] == "CLEANUP_CANCEL"
        ]
        sell_claims = [
            claim for claim in claims if claim["slot"] == "CLEANUP_SELL"
        ]
        cleanup_claims = [
            claim
            for claim in claims
            if claim["slot"] in {"CLEANUP_CANCEL", "CLEANUP_SELL"}
        ]
        if owned_working:
            target = _text(owned_working[0].get("identifier"))
            prior = next(
                (
                    claim
                    for claim in cancel_claims
                    if claim.get("target_identifier") == target
                ),
                None,
            )
            if (
                prior is None
                and len(cleanup_claims) < MAX_CLEANUP_ACTION_GENERATIONS
            ):
                actions.append(UpbitLeg.cancel(target))
            elif (
                prior is not None
                and prior["state"] == "BLOCKED_BEFORE_POST"
                and int(prior.get("proven_not_sent_retries") or 0) < 1
            ):
                actions.append(UpbitLeg.cancel(target))
        orderable_residual = (
            owned >= truth.rules.quantity_step
            and owned * truth.mark_price >= truth.rules.ask_min_total
        )
        if not owned_working and orderable_residual:
            retryable = next(
                (
                    claim
                    for claim in reversed(sell_claims)
                    if claim["state"] == "BLOCKED_BEFORE_POST"
                    and int(claim.get("proven_not_sent_retries") or 0) < 1
                ),
                None,
            )
            if retryable is not None or len(cleanup_claims) < (
                MAX_CLEANUP_ACTION_GENERATIONS - 1
            ):
                actions.append(UpbitLeg.sell(owned, cleanup=True))
        ready_to_finalize = not owned_working and not orderable_residual
        return {
            "sessionId": self.session_id,
            "newEntriesBlocked": True,
            "actions": actions,
            "ownedQuantity": _decimal_text(owned),
            "orderableResidual": orderable_residual,
            "ownedWorkingOrderCount": len(owned_working),
            "cleanupCancelCount": len(cancel_claims),
            "cleanupSellGenerationCount": len(sell_claims),
            "cleanupActionGenerationCount": len(cleanup_claims),
            "cleanupActionGenerationCap": MAX_CLEANUP_ACTION_GENERATIONS,
            "readyToFinalize": ready_to_finalize,
            "ownerLoss": _decimal_text(loss),
            "fees": _decimal_text(fees),
            "cleanupDeadline": _utc_text(self.scope.cleanup_deadline),
        }

    def _reconcile_ambiguous_claims(self, truth: UpbitTruth) -> None:
        for claim in self.ledger.claims(self.session_id):
            if claim["state"] not in {"AMBIGUOUS", "POST_MAY_HAVE_CROSSED"}:
                continue
            is_cancel = _upper(claim["side"]) == "CANCEL"
            identifier = _text(
                claim.get("target_identifier") if is_cancel else claim["identifier"]
            )
            if identifier not in truth.identifier_truth:
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-identifier-truth-missing"
                )
            exact = truth.identifier_truth[identifier]
            if exact is None:
                if any(
                    _text(row.get("identifier")) == identifier
                    for row in (
                        *truth.open_orders,
                        *truth.closed_orders,
                        *truth.private_stream_events,
                    )
                ):
                    raise UpbitFunctionalBlocked(
                        "upbit-ambiguous-absence-scope-contradiction"
                    )
                absence = self.ledger.observe_ambiguous_absence(
                    claim["claim_id"],
                    observed_at=truth.observed_at,
                    base_total=truth.base_total,
                    quote_available=truth.quote_available,
                )
                if absence["visibilityHorizonSatisfied"] is True:
                    continue
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-absence-not-terminal-proof"
                )
            if _upper(exact.get("market")) != SYMBOL or not _text(
                exact.get("uuid")
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-exact-order-identity-mismatch"
                )
            if is_cancel:
                if _text(exact.get("state")).lower() not in {"cancel", "done"}:
                    raise UpbitFunctionalBlocked(
                        "upbit-ambiguous-cancel-not-terminal"
                    )
            elif _upper(exact.get("side")) != _upper(claim["side"]):
                raise UpbitFunctionalBlocked(
                    "upbit-ambiguous-exact-order-identity-mismatch"
                )
            self.ledger.reconcile_ambiguous_claim(
                claim["claim_id"],
                state="RECONCILED",
                response=exact,
            )

    def finalize_if_flat(self) -> dict[str, Any]:
        session = self.ledger.session(self.session_id)
        if session["state"] in {"FINAL_RESET_PENDING", "FINALIZED"}:
            return self.resume_final_reset()
        if session["state"] != "CLEANUP":
            raise UpbitFunctionalBlocked("upbit-finalize-cleanup-state-required")
        truth = self._read_truth("FINAL")
        owned_ids = {
            _text(identifier)
            for claim in self.ledger.claims(self.session_id)
            for identifier in (
                claim["identifier"],
                claim.get("target_identifier"),
            )
            if _text(identifier)
        }
        if any(_text(row.get("identifier")) in owned_ids for row in truth.open_orders):
            raise UpbitFunctionalBlocked("upbit-final-owned-working-order-present")
        owned, loss, fees = self._owned_fills(truth)
        baseline = _decimal(session["baseline_base"], "upbit-baseline-base")
        if truth.base_total < baseline:
            raise UpbitFunctionalBlocked(
                "upbit-final-preexisting-baseline-not-preserved"
            )
        residual = truth.base_total - baseline
        residual_value = residual * truth.mark_price
        residual_orderable = (
            residual >= truth.rules.quantity_step
            and residual_value >= truth.rules.ask_min_total
        )
        if owned != residual:
            raise UpbitFunctionalBlocked(
                "upbit-final-owned-residual-account-mismatch"
            )
        if residual_orderable:
            raise UpbitFunctionalBlocked(
                "upbit-final-orderable-residual-remains"
            )
        barrier = dict(
            self.terminal_stream_barrier(session_id=self.session_id)
        )
        if (
            barrier.get("cutoffEstablished") is not True
            or not hmac.compare_digest(
                _text(barrier.get("sessionId")), self.session_id
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-terminal-barrier-invalid"
            )
        # Re-read every official surface only after the private socket's
        # deterministic PONG cutoff.  Broker activity later than the cutoff is
        # therefore present in REST truth, while all earlier received frames
        # are already durable in the cursor sealed below.
        truth = self._read_truth("FINAL")
        if any(
            _text(row.get("identifier")) in owned_ids
            for row in truth.open_orders
        ):
            raise UpbitFunctionalBlocked(
                "upbit-final-owned-working-order-present"
            )
        owned, loss, fees = self._owned_fills(truth)
        if truth.base_total < baseline:
            raise UpbitFunctionalBlocked(
                "upbit-final-preexisting-baseline-not-preserved"
            )
        residual = truth.base_total - baseline
        residual_value = residual * truth.mark_price
        residual_orderable = (
            residual >= truth.rules.quantity_step
            and residual_value >= truth.rules.ask_min_total
        )
        if owned != residual:
            raise UpbitFunctionalBlocked(
                "upbit-final-owned-residual-account-mismatch"
            )
        if residual_orderable:
            raise UpbitFunctionalBlocked(
                "upbit-final-orderable-residual-remains"
            )
        self._assert_current_selection()
        capability_hash = self._assert_dispatch_capability()
        runtime = dict(self.runtime_reader())
        self._assert_runtime(
            runtime,
            self.scope,
            self.session_id,
            cleanup=True,
            capability_hash=capability_hash,
        )
        if runtime.get("realOrdersEnabled") is not False:
            raise UpbitFunctionalBlocked(
                "upbit-final-runtime-real-orders-still-enabled"
            )
        if self.real_orders_reader() is not False:
            raise UpbitFunctionalBlocked(
                "upbit-final-global-real-orders-still-enabled"
            )
        claims = self.ledger.claims(self.session_id)
        orders_by_identifier = {
            _text(row.get("identifier")): row
            for row in truth.closed_orders
            if _text(row.get("identifier"))
        }
        fills_by_identifier: dict[str, list[dict[str, Any]]] = {}
        for fill in truth.fills:
            fills_by_identifier.setdefault(
                _text(fill.get("identifier")), []
            ).append(fill)

        def terminal_filled(slot: str, side: str) -> bool:
            matches = [
                claim
                for claim in claims
                if claim["slot"] == slot and claim["state"] == "RECONCILED"
            ]
            if len(matches) != 1:
                return False
            identifier = _text(matches[0]["identifier"])
            order = orders_by_identifier.get(identifier)
            fills = fills_by_identifier.get(identifier, [])
            return bool(
                isinstance(order, Mapping)
                and _text(order.get("state")).lower() == "done"
                and _upper(order.get("side")) == side
                and fills
                and all(_upper(fill.get("side")) == side for fill in fills)
                and sum(
                    (
                        _decimal(
                            fill.get("volume"),
                            "upbit-final-fill-volume",
                            minimum=Decimal("0.00000001"),
                        )
                        for fill in fills
                    ),
                    Decimal("0"),
                )
                > 0
            )

        strategy_buy_reconciled = any(
            claim["slot"] == "STRATEGY_BUY"
            and claim["state"] == "RECONCILED"
            for claim in claims
        )
        strategy_sell_reconciled = any(
            claim["slot"] == "STRATEGY_SELL"
            and claim["state"] == "RECONCILED"
            for claim in claims
        )
        cleanup_flatten_used = any(
            claim["slot"] == "CLEANUP_SELL"
            and claim["state"] == "RECONCILED"
            for claim in claims
        )
        strategy_buy_terminal_filled = terminal_filled(
            "STRATEGY_BUY", "BID"
        )
        strategy_sell_terminal_filled = terminal_filled(
            "STRATEGY_SELL", "ASK"
        )
        strategy_claims = [
            claim
            for claim in claims
            if claim["slot"] in {"STRATEGY_BUY", "STRATEGY_SELL"}
        ]
        strategy_order_count_exact = bool(
            len(strategy_claims) == 2
            and sum(
                claim["slot"] == "STRATEGY_BUY"
                for claim in strategy_claims
            )
            == 1
            and sum(
                claim["slot"] == "STRATEGY_SELL"
                for claim in strategy_claims
            )
            == 1
        )
        strategy_buy_identifiers = {
            _text(claim.get("identifier"))
            for claim in strategy_claims
            if claim["slot"] == "STRATEGY_BUY"
        }
        strategy_buy_executed_notional = sum(
            (
                _decimal(fill.get("funds"), "upbit-final-buy-notional")
                for fill in truth.fills
                if _text(fill.get("identifier"))
                in strategy_buy_identifiers
                and _upper(fill.get("side")) == "BID"
            ),
            Decimal("0"),
        )
        strategy_notional_cap_satisfied = bool(
            strategy_buy_executed_notional > 0
            and strategy_buy_executed_notional
            <= self.scope.max_order_notional
        )
        max_owner_gross = _decimal(
            session.get("max_owner_gross"), "upbit-final-max-owner-gross"
        )
        strategy_gross_exposure_cap_satisfied = bool(
            max_owner_gross <= self.scope.max_gross_exposure
        )
        try:
            baseline_account_rows = json.loads(
                _text(session.get("baseline_account_rows_json"))
            )
        except (TypeError, ValueError):
            baseline_account_rows = []
        baseline_by_currency = {
            _upper(row.get("currency")): dict(row)
            for row in baseline_account_rows
            if isinstance(row, Mapping) and _upper(row.get("currency"))
        }
        final_by_currency = {
            _upper(row.get("currency")): dict(row)
            for row in truth.account_rows
        }
        non_target_balances_unchanged = bool(
            set(baseline_by_currency) == set(final_by_currency)
            and all(
                baseline_by_currency[currency]
                == final_by_currency[currency]
                for currency in set(baseline_by_currency)
                - {"KRW", "BTC"}
            )
        )
        final_account_unlocked = all(
            _decimal(row.get("locked"), "upbit-final-account-locked") == 0
            for row in truth.account_rows
        )
        buy_funds = sum(
            (
                _decimal(fill.get("funds"), "upbit-final-buy-funds")
                for fill in truth.fills
                if _upper(fill.get("side")) == "BID"
            ),
            Decimal("0"),
        )
        buy_fees = sum(
            (
                _decimal(fill.get("fee"), "upbit-final-buy-fee")
                for fill in truth.fills
                if _upper(fill.get("side")) == "BID"
            ),
            Decimal("0"),
        )
        sell_funds = sum(
            (
                _decimal(fill.get("funds"), "upbit-final-sell-funds")
                for fill in truth.fills
                if _upper(fill.get("side")) == "ASK"
            ),
            Decimal("0"),
        )
        sell_fees = sum(
            (
                _decimal(fill.get("fee"), "upbit-final-sell-fee")
                for fill in truth.fills
                if _upper(fill.get("side")) == "ASK"
            ),
            Decimal("0"),
        )
        expected_quote = (
            _decimal(session["baseline_quote"], "upbit-baseline-quote")
            - buy_funds
            - buy_fees
            + sell_funds
            - sell_fees
        )
        quote_balance_causally_reconciled = (
            truth.quote_available == expected_quote
        )
        exclusive_account_causal_proof = bool(
            truth.account_external_activity_absent
            and truth.account_exclusivity_proof_verified
            and int(session.get("account_exclusivity_breach") or 0) == 0
            and truth.base_total == baseline + owned
            and quote_balance_causally_reconciled
            and non_target_balances_unchanged
            and final_account_unlocked
        )
        proof_api_keys = truth.account_exclusivity_proof.get(
            "apiKeyInventory", {}
        )
        proof_manual = truth.account_exclusivity_proof.get(
            "manualTradeAudit", {}
        )
        proof_bots = truth.account_exclusivity_proof.get("botRegistry", {})
        other_api_keys_absent = bool(
            truth.account_exclusivity_proof_verified
            and isinstance(proof_api_keys, Mapping)
            and proof_api_keys.get("otherActiveApiKeyCount") == 0
        )
        manual_trading_absent = bool(
            truth.account_exclusivity_proof_verified
            and isinstance(proof_manual, Mapping)
            and proof_manual.get("manualOrderCount") == 0
        )
        other_bots_absent = bool(
            truth.account_exclusivity_proof_verified
            and isinstance(proof_bots, Mapping)
            and proof_bots.get("otherActiveBotCount") == 0
        )
        account_exclusivity_authority_pinned = bool(
            truth.account_exclusivity_proof_verified
            and isinstance(
                truth.account_exclusivity_proof.get("authority"), Mapping
            )
            and truth.account_exclusivity_proof["authority"].get(
                "authorityPinned"
            )
            is True
        )
        terminal_identifiers = tuple(sorted(owned_ids))
        terminal_stream_seal = dict(
            self.terminal_stream_prepare(
                session_id=self.session_id,
                identifiers=terminal_identifiers,
            )
        )
        truth_cursor_expected = {
            "writerGeneration": truth.private_stream_writer_generation,
            "journalRevision": truth.private_stream_revision,
            "eventCursor": truth.private_stream_event_cursor,
            "lastEventId": truth.private_stream_last_event_id,
            "eventHeadHash": truth.private_stream_event_head_hash,
        }
        for field, expected in truth_cursor_expected.items():
            actual = terminal_stream_seal.get(field)
            if isinstance(expected, str):
                if not hmac.compare_digest(_text(actual), expected):
                    raise UpbitFunctionalBlocked(
                        f"upbit-final-private-stream-truth-{field}-mismatch"
                    )
            elif actual != expected:
                raise UpbitFunctionalBlocked(
                    f"upbit-final-private-stream-truth-{field}-mismatch"
                )
        seal_body = {
            key: value
            for key, value in terminal_stream_seal.items()
            if key != "sealHash"
        }
        expected_terminal = {
            "schemaVersion": "upbit-functional-private-terminal-seal/v1",
            "sessionId": self.session_id,
            "accountFingerprint": self.scope.account_fingerprint,
            "channel": "myOrder",
            "ownedIdentifiers": list(terminal_identifiers),
            "ownedIdentifiersHash": _stable_hash(list(terminal_identifiers)),
            "externalActivityAbsent": True,
        }
        for field, expected in expected_terminal.items():
            actual = terminal_stream_seal.get(field)
            if isinstance(expected, str):
                if not hmac.compare_digest(_text(actual), expected):
                    raise UpbitFunctionalBlocked(
                        f"upbit-final-private-stream-{field}-mismatch"
                    )
            elif actual != expected:
                raise UpbitFunctionalBlocked(
                    f"upbit-final-private-stream-{field}-mismatch"
                )
        terminal_seal_hash = _require_hash(
            terminal_stream_seal.get("sealHash"),
            "upbit-final-private-stream-seal-hash",
        )
        if not hmac.compare_digest(terminal_seal_hash, _stable_hash(seal_body)):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-seal-hash-mismatch"
            )
        if truth.private_stream_recovery:
            if not (
                terminal_stream_seal.get("cleanupOnlyRecovery") is True
                and terminal_stream_seal.get("gapDetected") is True
                and terminal_stream_seal.get("streamContinuous") is False
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-final-private-stream-recovery-seal-invalid"
                )
        elif not (
            terminal_stream_seal.get("cleanupOnlyRecovery") is False
            and terminal_stream_seal.get("gapDetected") is False
            and terminal_stream_seal.get("streamContinuous") is True
        ):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-continuity-seal-invalid"
            )
        activated_at = _utc(session["starts_at"], "upbit-final-activated-at")
        expires_at = _utc(session["expires_at"], "upbit-final-expires-at")
        final_observed_at = _utc(
            truth.observed_at, "upbit-final-observed-at"
        )
        actual_duration_seconds = Decimal(
            str((final_observed_at - activated_at).total_seconds())
        )
        monotonic_now = Decimal(str(self.monotonic_clock()))
        monotonic_elapsed_seconds = (
            monotonic_now - Decimal(str(self._activation_monotonic))
            if self._monotonic_continuity
            and self._activation_monotonic is not None
            else Decimal("-1")
        )
        clock_discontinuity_absent = bool(
            self._monotonic_continuity
            and monotonic_elapsed_seconds >= 0
            and abs(actual_duration_seconds - monotonic_elapsed_seconds)
            <= Decimal("15")
        )
        exact_two_hour_permit = bool(
            expires_at - activated_at == timedelta(seconds=7200)
            and self.scope.starts_at == activated_at
            and self.scope.ends_at == expires_at
        )
        exact_two_hour_runtime_complete = bool(
            exact_two_hour_permit
            and actual_duration_seconds >= Decimal("7200")
            and monotonic_elapsed_seconds >= Decimal("7200")
            and clock_discontinuity_absent
        )
        terminal_truth_snapshot = {
            "schemaVersion": "upbit-functional-terminal-official-truth/v1",
            "sessionId": self.session_id,
            "accountFingerprint": self.scope.account_fingerprint,
            "observedAt": _utc_text(truth.observed_at),
            "observationStartedAt": _utc_text(
                truth.observation_started_at
            ),
            "sessionStartedAt": _utc_text(activated_at),
            "quoteAvailable": _decimal_text(truth.quote_available),
            "baseAvailable": _decimal_text(truth.base_available),
            "baseTotal": _decimal_text(truth.base_total),
            "markPrice": _decimal_text(truth.mark_price),
            "orderRules": {
                "bidMinTotal": _decimal_text(truth.rules.bid_min_total),
                "askMinTotal": _decimal_text(truth.rules.ask_min_total),
                "quantityStep": _decimal_text(truth.rules.quantity_step),
                "quantityScale": truth.rules.quantity_scale,
                "bidFeeRate": _decimal_text(truth.rules.bid_fee_rate),
                "askFeeRate": _decimal_text(truth.rules.ask_fee_rate),
            },
            "openOrders": [dict(row) for row in truth.open_orders],
            "closedOrders": [dict(row) for row in truth.closed_orders],
            "fills": [dict(row) for row in truth.fills],
            "totalFees": _decimal_text(truth.total_fees),
            "identifierTruth": {
                identifier: (
                    dict(order) if isinstance(order, Mapping) else None
                )
                for identifier, order in truth.identifier_truth.items()
            },
            "accountRows": [dict(row) for row in truth.account_rows],
            "accountRowsHash": truth.account_rows_hash,
            "accountExternalActivityAbsent": (
                truth.account_external_activity_absent
            ),
            "accountExclusivityProof": dict(
                truth.account_exclusivity_proof
            ),
            "accountExclusivityProofHash": (
                truth.account_exclusivity_proof_hash
            ),
            "accountExclusivityProofVerified": (
                truth.account_exclusivity_proof_verified
            ),
            "accountExclusivityAuthorityPinned": (
                account_exclusivity_authority_pinned
            ),
            "otherApiKeysAbsent": other_api_keys_absent,
            "manualTradingAbsent": manual_trading_absent,
            "otherBotsAbsent": other_bots_absent,
            "officialRestRawSnapshot": dict(
                truth.official_rest_raw_snapshot
            ),
            "officialRestRawSnapshotHash": (
                truth.official_rest_raw_snapshot_hash
            ),
            "terminalPrivateStreamSeal": dict(terminal_stream_seal),
        }
        terminal_truth_hash = self.ledger.seal_terminal_truth(
            self.session_id,
            terminal_truth_snapshot,
        )
        functional_wiring_passed = bool(
            strategy_buy_terminal_filled
            and strategy_sell_terminal_filled
            and strategy_order_count_exact
            and strategy_buy_reconciled
            and strategy_sell_reconciled
            and strategy_notional_cap_satisfied
            and strategy_gross_exposure_cap_satisfied
            and loss < self.scope.max_loss
            and not residual_orderable
            and truth.base_total >= baseline
            and not truth.private_stream_recovery
            and truth.account_external_activity_absent
            and truth.account_exclusivity_proof_verified
            and int(session.get("account_exclusivity_breach") or 0) == 0
            and exact_two_hour_runtime_complete
            and not cleanup_flatten_used
            and not truth.open_orders
            and bool(truth.official_rest_raw_snapshot)
            and bool(truth.official_rest_raw_snapshot_hash)
        )
        evidence = {
            "schemaVersion": SCHEMA_VERSION,
            "sessionId": self.session_id,
            "accountFingerprint": self.scope.account_fingerprint,
            "permitId": self.scope.permit_id,
            "permitHash": self.scope.permit_hash,
            "routeScopeHash": self.scope.route_scope_hash,
            "baselineBase": _decimal_text(baseline),
            "finalBase": _decimal_text(truth.base_total),
            "ownedQuantity": _decimal_text(residual),
            "residualQuantity": _decimal_text(residual),
            "residualValue": _decimal_text(residual_value),
            "orderableResidual": False,
            "preexistingBaselinePreserved": True,
            "baselineRestoredWithinExchangePrecision": (
                not residual_orderable and loss < self.scope.max_loss
            ),
            "ownerLoss": _decimal_text(loss),
            "maxOwnerLoss": _decimal_text(self.scope.max_loss),
            "ownerLossLimitSatisfied": loss < self.scope.max_loss,
            "fees": _decimal_text(fees),
            "maxOrderNotionalKRW": _decimal_text(
                self.scope.max_order_notional
            ),
            "maxGrossExposureKRW": _decimal_text(
                self.scope.max_gross_exposure
            ),
            "accountOpenOrderCount": len(truth.open_orders),
            "ownedWorkingOrderCount": sum(
                1
                for row in truth.open_orders
                if _text(row.get("identifier")) in set(terminal_identifiers)
            ),
            "newEntriesBlocked": True,
            "realOrdersEnabled": False,
            "functionalCapabilityCleared": True,
            "functionalMutationEnabled": False,
            "privateStreamContinuous": not truth.private_stream_recovery,
            "cleanupRecoveryRestAttested": truth.private_stream_recovery,
            "terminalPrivateStreamSeal": terminal_stream_seal,
            "terminalPrivateStreamSealHash": terminal_seal_hash,
            "terminalOfficialTruthHash": terminal_truth_hash,
            "officialRestRawSnapshotHash": (
                truth.official_rest_raw_snapshot_hash
            ),
            "exclusiveAccountScope": "UPBIT_ACCOUNT_ALL_MARKETS",
            "exclusiveAccountOperatorContractRequired": True,
            "accountRowsHash": truth.account_rows_hash,
            "baselineAccountRowsHash": _text(
                session.get("baseline_account_rows_hash")
            ).lower(),
            "accountExternalActivityAbsent": (
                truth.account_external_activity_absent
            ),
            "accountExclusivityProof": dict(
                truth.account_exclusivity_proof
            ),
            "accountExclusivityProofHash": (
                truth.account_exclusivity_proof_hash
            ),
            "accountExclusivityProofVerified": (
                truth.account_exclusivity_proof_verified
            ),
            "accountExclusivityAuthorityPinned": (
                account_exclusivity_authority_pinned
            ),
            "accountExclusivityContinuouslyVerified": (
                int(session.get("account_exclusivity_breach") or 0) == 0
            ),
            "otherApiKeysAbsent": other_api_keys_absent,
            "manualTradingAbsent": manual_trading_absent,
            "otherBotsAbsent": other_bots_absent,
            "nonTargetBalancesUnchanged": non_target_balances_unchanged,
            "finalAccountUnlocked": final_account_unlocked,
            "quoteBalanceCausallyReconciled": (
                quote_balance_causally_reconciled
            ),
            "exclusiveAccountCausalProofComplete": (
                exclusive_account_causal_proof
            ),
            "claimCount": len(claims),
            "strategyBuyReconciled": strategy_buy_reconciled,
            "strategySellReconciled": strategy_sell_reconciled,
            "strategyBuyTerminalFilled": strategy_buy_terminal_filled,
            "strategySellTerminalFilled": strategy_sell_terminal_filled,
            "strategyOrderCountExact": strategy_order_count_exact,
            "noReentryVerified": strategy_order_count_exact,
            "strategyBuyExecutedNotional": _decimal_text(
                strategy_buy_executed_notional
            ),
            "strategyNotionalCapSatisfied": (
                strategy_notional_cap_satisfied
            ),
            "maxObservedOwnerGrossExposure": _decimal_text(max_owner_gross),
            "strategyGrossExposureCapSatisfied": (
                strategy_gross_exposure_cap_satisfied
            ),
            "fillAndFeeTruthComplete": True,
            "cleanupFlattenUsed": cleanup_flatten_used,
            "activatedAt": _utc_text(activated_at),
            "permitEndsAt": _utc_text(expires_at),
            "finalObservedAt": _utc_text(final_observed_at),
            "terminalObservationStartedAt": _utc_text(
                truth.observation_started_at
            ),
            "actualDurationSeconds": _decimal_text(
                max(actual_duration_seconds, Decimal("0"))
            ),
            "processMonotonicElapsedSeconds": (
                _decimal_text(monotonic_elapsed_seconds)
                if monotonic_elapsed_seconds >= 0
                else "UNAVAILABLE_AFTER_RESTART"
            ),
            "processMonotonicContinuity": self._monotonic_continuity,
            "clockDiscontinuityAbsent": clock_discontinuity_absent,
            "requiredActiveDurationSeconds": "7200",
            "activationRelativePermitExact": exact_two_hour_permit,
            "exactTwoHourRuntimeComplete": exact_two_hour_runtime_complete,
            "functionalWiringPassed": functional_wiring_passed,
            "functionalTestPassed": (
                functional_wiring_passed
                and exclusive_account_causal_proof
            ),
            "evidenceClass": EVIDENCE_CLASS,
            "promotionEligible": False,
        }
        pending = self.ledger.begin_final_reset(self.session_id, evidence)
        if pending["state"] not in {"FINAL_RESET_PENDING", "FINALIZED"}:
            raise UpbitFunctionalBlocked(
                "upbit-final-reset-durable-transition-failed"
            )
        # Durable authority is revoked before the runtime pointer.  Clear the
        # in-memory secret immediately so even an exception in the registrar
        # cannot reuse this service object for a mutation.
        self.__runtime_capability_secret = ""
        self.runtime_capability_registrar("")
        cleared_runtime = dict(self.runtime_reader())
        self._assert_runtime(
            cleared_runtime,
            self.scope,
            self.session_id,
            cleanup=True,
            capability_hash="",
        )
        if _text(cleared_runtime.get("functionalCapabilityHash")):
            raise UpbitFunctionalBlocked(
                "upbit-final-runtime-capability-not-cleared"
            )
        if cleared_runtime.get("realOrdersEnabled") is not False:
            raise UpbitFunctionalBlocked(
                "upbit-final-runtime-real-orders-still-enabled"
            )
        if self.real_orders_reader() is not False:
            raise UpbitFunctionalBlocked(
                "upbit-final-global-real-orders-still-enabled"
            )
        committed_stream_seal = dict(
            self.terminal_stream_commit(
                session_id=self.session_id,
                identifiers=terminal_identifiers,
                expected=terminal_stream_seal,
            )
        )
        if committed_stream_seal != terminal_stream_seal:
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-commit-mismatch"
            )
        finalized = self.ledger.complete_final_reset(
            self.session_id,
            evidence_hash=_stable_hash(evidence),
        )
        return self._final_result(evidence, finalized)

    def resume_final_reset(self) -> dict[str, Any]:
        """Idempotently finish a crash-interrupted capability reset."""

        session = self.ledger.session(self.session_id)
        if session["state"] not in {"FINAL_RESET_PENDING", "FINALIZED"}:
            raise UpbitFunctionalBlocked(
                "upbit-final-reset-resume-state-invalid"
            )
        if _text(session.get("capability_hash")):
            raise UpbitFunctionalBlocked(
                "upbit-final-reset-durable-capability-not-cleared"
            )
        self.__runtime_capability_secret = ""
        self.runtime_capability_registrar("")
        runtime = dict(self.runtime_reader())
        self._assert_runtime(
            runtime,
            self.scope,
            self.session_id,
            cleanup=True,
            capability_hash="",
        )
        if (
            _text(runtime.get("functionalCapabilityHash"))
            or runtime.get("realOrdersEnabled") is not False
            or self.real_orders_reader() is not False
        ):
            raise UpbitFunctionalBlocked(
                "upbit-final-reset-runtime-authority-not-cleared"
            )
        evidence = self.ledger.final_evidence(self.session_id)
        terminal_stream_seal = evidence.get("terminalPrivateStreamSeal")
        if not isinstance(terminal_stream_seal, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-seal-missing"
            )
        identifiers_value = terminal_stream_seal.get("ownedIdentifiers")
        if not isinstance(identifiers_value, list) or any(
            not isinstance(identifier, str) or not identifier
            for identifier in identifiers_value
        ):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-identifiers-invalid"
            )
        terminal_identifiers = tuple(identifiers_value)
        committed_stream_seal = dict(
            self.terminal_stream_commit(
                session_id=self.session_id,
                identifiers=terminal_identifiers,
                expected=terminal_stream_seal,
            )
        )
        if committed_stream_seal != dict(terminal_stream_seal):
            raise UpbitFunctionalBlocked(
                "upbit-final-private-stream-commit-mismatch"
            )
        finalized = self.ledger.complete_final_reset(
            self.session_id,
            evidence_hash=_stable_hash(evidence),
        )
        return self._final_result(evidence, finalized)

    def _final_result(
        self,
        evidence: Mapping[str, Any],
        finalized: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            claim_count = int(evidence.get("claimCount"))
        except (TypeError, ValueError):
            claim_count = -1
        claims_present = claim_count > 0
        try:
            proof_content_hash = _strict_stable_hash(
                evidence.get("accountExclusivityProof")
            )
        except (TypeError, ValueError, RecursionError):
            proof_content_hash = ""
        exclusivity_proof_complete = bool(
            evidence.get("accountExclusivityProofVerified") is True
            and evidence.get("accountExclusivityAuthorityPinned") is True
            and evidence.get("accountExclusivityContinuouslyVerified") is True
            and evidence.get("otherApiKeysAbsent") is True
            and evidence.get("manualTradingAbsent") is True
            and evidence.get("otherBotsAbsent") is True
            and isinstance(evidence.get("accountExclusivityProof"), Mapping)
            and _exact_lower_hash(
                evidence.get("accountExclusivityProofHash")
            )
            and hmac.compare_digest(
                evidence["accountExclusivityProofHash"],
                proof_content_hash,
            )
            and _account_exclusivity_evidence_complete(
                evidence,
                verifier=self.account_exclusivity_verifier,
                verifier_pin=self.account_exclusivity_verifier_pin,
            )
        )
        causal_proof_complete = bool(
            evidence.get("exclusiveAccountCausalProofComplete") is True
            and exclusivity_proof_complete
            and evidence.get("accountExternalActivityAbsent") is True
            and evidence.get("nonTargetBalancesUnchanged") is True
            and evidence.get("finalAccountUnlocked") is True
            and evidence.get("quoteBalanceCausallyReconciled") is True
        )
        wiring_recomputed = bool(
            evidence.get("functionalWiringPassed") is True
            and claim_count == 2
            and evidence.get("strategyBuyTerminalFilled") is True
            and evidence.get("strategySellTerminalFilled") is True
            and evidence.get("strategyBuyReconciled") is True
            and evidence.get("strategySellReconciled") is True
            and evidence.get("strategyOrderCountExact") is True
            and evidence.get("noReentryVerified") is True
            and evidence.get("fillAndFeeTruthComplete") is True
            and evidence.get("cleanupFlattenUsed") is False
            and evidence.get("strategyNotionalCapSatisfied") is True
            and evidence.get("strategyGrossExposureCapSatisfied") is True
            and evidence.get("ownerLossLimitSatisfied") is True
            and evidence.get("orderableResidual") is False
            and type(evidence.get("accountOpenOrderCount")) is int
            and evidence.get("accountOpenOrderCount") == 0
            and type(evidence.get("ownedWorkingOrderCount")) is int
            and evidence.get("ownedWorkingOrderCount") == 0
            and evidence.get("privateStreamContinuous") is True
            and evidence.get("activationRelativePermitExact") is True
            and evidence.get("exactTwoHourRuntimeComplete") is True
            and evidence.get("processMonotonicContinuity") is True
            and evidence.get("clockDiscontinuityAbsent") is True
            and evidence.get("functionalCapabilityCleared") is True
            and evidence.get("functionalMutationEnabled") is False
            and evidence.get("realOrdersEnabled") is False
            and evidence.get("newEntriesBlocked") is True
            and exclusivity_proof_complete
        )
        evidence_hash = _stable_hash(evidence)
        try:
            canonical_evidence_json = json.dumps(
                dict(evidence), sort_keys=True, separators=(",", ":")
            )
        except (TypeError, ValueError, RecursionError):
            canonical_evidence_json = ""
        durable_complete = bool(
            finalized.get("state") == "FINALIZED"
            and int(finalized.get("new_entries_blocked") or 0) == 1
            and int(finalized.get("real_orders_enabled") or 0) == 0
            and not _text(finalized.get("capability_hash"))
            and _exact_lower_hash(finalized.get("final_evidence_hash"))
            and hmac.compare_digest(
                finalized["final_evidence_hash"],
                evidence_hash,
            )
            and _text(finalized.get("final_evidence_json"))
            == canonical_evidence_json
        )
        pass_complete = bool(
            evidence.get("functionalTestPassed") is True
            and wiring_recomputed
            and causal_proof_complete
            and durable_complete
        )
        normalized_evidence = dict(evidence)
        normalized_evidence["functionalWiringPassed"] = wiring_recomputed
        normalized_evidence["functionalTestPassed"] = pass_complete
        normalized_evidence["promotionEligible"] = False
        return {
            "ok": True,
            "state": finalized["state"],
            "testOutcome": (
                "PASS"
                if pass_complete
                else (
                    "INCONCLUSIVE_NO_SIGNAL"
                    if not claims_present and causal_proof_complete
                    else (
                        "SAFE_INCOMPLETE"
                        if not exclusivity_proof_complete
                        else (
                            "SAFE_INCOMPLETE_CAUSAL_UNPROVEN"
                            if evidence.get("functionalWiringPassed") is True
                            and not causal_proof_complete
                            else "SAFE_INCOMPLETE"
                        )
                    )
                )
            ),
            "evidence": normalized_evidence,
            "evidenceHash": finalized["final_evidence_hash"],
        }


def activate_upbit_continuous_functional(**kwargs: Any) -> UpbitContinuousFunctionalService:
    """Production entry point; fail closed until official adapters and E2E pass."""

    return UpbitContinuousFunctionalService.activate(**kwargs)


def _activate_for_test(**kwargs: Any) -> UpbitContinuousFunctionalService:
    return UpbitContinuousFunctionalService.activate(**kwargs, _capability=_TEST_CAPABILITY)


__all__ = [
    "EVIDENCE_CLASS",
    "EXECUTION_ROUTE",
    "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
    "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_STATUS",
    "UPBIT_CONTINUOUS_FUNCTIONAL_AVAILABLE",
    "UPBIT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
    "UPBIT_PRODUCTION_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED",
    "FinalizedFiveMinuteBar",
    "UpbitContinuousFunctionalService",
    "UpbitFunctionalAmbiguous",
    "UpbitFunctionalBlocked",
    "UpbitBrokerPostNotSent",
    "UpbitFunctionalError",
    "UpbitFunctionalLedger",
    "UpbitLeg",
    "UpbitOrderRules",
    "UpbitPermitScope",
    "UpbitTruth",
    "activate_upbit_continuous_functional",
]
