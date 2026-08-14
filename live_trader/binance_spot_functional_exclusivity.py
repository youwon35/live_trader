from __future__ import annotations

"""Detached Binance Spot account-exclusivity and causal-closure evidence.

The trading process is deliberately a verifier, never a signer.  A production
composition must inject an independently owned proof reader plus a verifier
whose runtime identity exactly matches a durable pin.  Missing, stale, swapped,
or self-authored evidence fails closed before a natural order can be sent.
"""

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping, Protocol


BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED = False
BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED = False
BINANCE_SPOT_ACCOUNT_WIDE_CAUSAL_AUTHORITY_AVAILABLE = False
BINANCE_SPOT_GLOBAL_FIRST_LIVE_AUTHORITY_WIRED = False

PROOF_SCHEMA_VERSION = "binance-spot-functional-account-exclusivity-proof/v1"
PROOF_REQUEST_SCHEMA_VERSION = (
    "binance-spot-functional-account-exclusivity-request/v1"
)
VERIFIER_PIN_SCHEMA_VERSION = "binance-account-exclusivity-verifier-pin/v1"
API_INVENTORY_SOURCE = "BINANCE_ACCOUNT_ADMIN_API_CREDENTIAL_INVENTORY_V1"
MANUAL_AUDIT_SOURCE = "BINANCE_ACCOUNT_ALL_SYMBOLS_MANUAL_ORDER_AUDIT_V1"
BOT_REGISTRY_SOURCE = "SERVER_OWNED_BINANCE_ACCOUNT_BOT_REGISTRY_V1"
CAUSAL_AUDIT_SOURCE = "BINANCE_ACCOUNT_ALL_SYMBOLS_CAUSAL_EVENT_AUDIT_V1"
MAX_PROOF_AGE_SECONDS = 15.0
GLOBAL_AUTHORITY_SCHEMA_VERSION = (
    "crypto-first-live-binance-authority-snapshot/v1"
)

_PHASES = frozenset({"BASELINE", "ACTIVATION", "PRE_POST", "TERMINAL"})
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class BinanceSpotExclusivityError(RuntimeError):
    pass


class BinanceSpotExclusivityVerifier(Protocol):
    """Verification-only interface owned outside the trading process."""

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


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _stable_hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _is_hash(value: object) -> bool:
    return type(value) is str and _HASH_RE.fullmatch(value) is not None


def _utc_text(epoch: float) -> str:
    return (
        datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _utc_epoch(value: object, label: str) -> float:
    if type(value) is not str:
        raise BinanceSpotExclusivityError(f"{label} must be an exact JSON string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BinanceSpotExclusivityError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BinanceSpotExclusivityError(f"{label} lacks a timezone")
    canonical = parsed.astimezone(timezone.utc)
    if _utc_text(canonical.timestamp()) != value:
        raise BinanceSpotExclusivityError(f"{label} is not canonical UTC")
    return canonical.timestamp()


def normalize_verifier_pin(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    fields = {
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
    if set(value) != fields:
        return None
    result = dict(value)
    if (
        result.get("schemaVersion") != VERIFIER_PIN_SCHEMA_VERSION
        or result.get("authorityPinned") is not True
        or any(
            type(result.get(field)) is not str
            or _SAFE_ID_RE.fullmatch(result[field]) is None
            for field in ("verifierId", "keyId", "algorithm", "verifierType")
        )
        or any(
            not _is_hash(result.get(field))
            for field in (
                "verifierCodeSha256",
                "verifierConfigSha256",
                "keyFingerprintSha256",
            )
        )
    ):
        return None
    return result


def verifier_wiring_status(
    verifier: BinanceSpotExclusivityVerifier | None,
    verifier_pin: Mapping[str, Any] | None,
    account_identity_fingerprint: object,
) -> dict[str, Any]:
    pin = normalize_verifier_pin(verifier_pin)
    account_identity = _text(account_identity_fingerprint).lower()
    if pin is None:
        return {
            "ready": False,
            "authorityPinned": False,
            "runtimeIdentityMatched": False,
            "accountIdentityPinned": False,
            "reason": "BINANCE_EXCLUSIVITY_VERIFIER_PIN_INVALID",
            "pinHash": "",
        }
    pin_hash = _stable_hash(pin)
    if not _is_hash(account_identity):
        return {
            "ready": False,
            "authorityPinned": True,
            "runtimeIdentityMatched": False,
            "accountIdentityPinned": False,
            "reason": "BINANCE_ACCOUNT_IDENTITY_PIN_MISSING",
            "pinHash": pin_hash,
        }
    if verifier is None:
        return {
            "ready": False,
            "authorityPinned": True,
            "runtimeIdentityMatched": False,
            "accountIdentityPinned": True,
            "reason": "BINANCE_EXCLUSIVITY_VERIFIER_MISSING",
            "pinHash": pin_hash,
        }
    try:
        identity = verifier.identity()
    except Exception:
        identity = None
    normalized_identity = (
        normalize_verifier_pin(identity) if isinstance(identity, Mapping) else None
    )
    matched = normalized_identity == pin
    return {
        "ready": matched,
        "authorityPinned": True,
        "runtimeIdentityMatched": matched,
        "accountIdentityPinned": True,
        "reason": "READY" if matched else "BINANCE_EXCLUSIVITY_VERIFIER_PIN_MISMATCH",
        "pinHash": pin_hash,
    }


def exclusivity_proof_request_payload(
    *,
    phase: str,
    session_id: str,
    permit_id: str,
    permit_hash: str,
    account_identity_fingerprint: str,
    credential_fingerprint: str,
    boundary_id: str,
    boundary_hash: str,
    coverage_started_epoch: float,
    requested_epoch: float,
    require_causal_closure: bool,
) -> dict[str, Any]:
    """Canonical signer request; contains identities/hashes but no secrets."""

    return {
        "schemaVersion": PROOF_REQUEST_SCHEMA_VERSION,
        "phase": _text(phase).upper(),
        "sessionId": _text(session_id),
        "permitId": _text(permit_id),
        "permitHash": _text(permit_hash).lower(),
        "accountIdentityFingerprint": _text(
            account_identity_fingerprint
        ).lower(),
        "credentialFingerprint": _text(credential_fingerprint).lower(),
        "boundaryId": _text(boundary_id),
        "boundaryHash": _text(boundary_hash).lower(),
        "coverageStartedAt": _utc_text(coverage_started_epoch),
        "requestedAt": _utc_text(requested_epoch),
        "requireCausalClosure": bool(require_causal_closure),
    }


def verify_global_first_live_authority(
    value: object,
    *,
    purpose: str,
    session_id: str,
    permit_id: str,
    permit_hash: str,
    account_fingerprint: str,
    cleanup_only: bool,
    now_epoch: float,
) -> dict[str, Any]:
    """Verify the coordinator-owned projection without importing it.

    The adapter owned by the root composition must build this exact projection
    from a freshly verified coordinator row.  An arbitrary coordinator status
    mapping is intentionally rejected so additive/default fields cannot change
    the authority semantics at the broker edge.
    """

    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError("global first-live authority is missing")
    row = dict(value)
    fields = {
        "schemaVersion",
        "scope",
        "lane",
        "phase",
        "runId",
        "sessionId",
        "permitId",
        "permitHash",
        "accountFingerprint",
        "ownerLeaseActive",
        "entryAuthorityOpen",
        "hardStopEpoch",
        "revision",
        "observedEpoch",
        "authorityHash",
    }
    if set(row) != fields:
        raise BinanceSpotExclusivityError(
            "global first-live authority fields are not exact"
        )
    projection = {key: item for key, item in row.items() if key != "authorityHash"}
    expected = {
        "schemaVersion": GLOBAL_AUTHORITY_SCHEMA_VERSION,
        "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
        "lane": "BINANCE_SPOT",
        "sessionId": _text(session_id),
        "permitId": _text(permit_id),
        "permitHash": _text(permit_hash).lower(),
        "accountFingerprint": _text(account_fingerprint).lower(),
    }
    for field, expected_value in expected.items():
        actual = row.get(field)
        if type(actual) is not str or not hmac.compare_digest(actual, expected_value):
            raise BinanceSpotExclusivityError(
                f"global first-live authority {field} changed"
            )
    if (
        type(row.get("runId")) is not str
        or _SAFE_ID_RE.fullmatch(row["runId"]) is None
        or isinstance(row.get("revision"), bool)
        or not isinstance(row.get("revision"), int)
        or row["revision"] < 1
        or isinstance(row.get("hardStopEpoch"), bool)
        or not isinstance(row.get("hardStopEpoch"), (int, float))
        or isinstance(row.get("observedEpoch"), bool)
        or not isinstance(row.get("observedEpoch"), (int, float))
        or not math.isfinite(float(row["hardStopEpoch"]))
        or not math.isfinite(float(row["observedEpoch"]))
        or type(row.get("phase")) is not str
        or type(row.get("entryAuthorityOpen")) is not bool
        or not _is_hash(row.get("authorityHash"))
        or not hmac.compare_digest(row["authorityHash"], _stable_hash(projection))
    ):
        raise BinanceSpotExclusivityError(
            "global first-live authority revision/time/hash is invalid"
        )
    observed = float(row["observedEpoch"])
    age = float(now_epoch) - observed
    if age < -1.0 or age > 5.0:
        raise BinanceSpotExclusivityError(
            "global first-live authority snapshot is stale or future-dated"
        )
    phase = _text(row.get("phase")).upper()
    if row.get("ownerLeaseActive") is not True:
        raise BinanceSpotExclusivityError(
            "global first-live owner lease is not active"
        )
    if cleanup_only:
        if phase not in {"ACTIVE", "CLEANUP_ONLY"}:
            raise BinanceSpotExclusivityError(
                "global first-live authority is not cleanup-capable"
            )
        if phase == "CLEANUP_ONLY" and row.get("entryAuthorityOpen") is not False:
            raise BinanceSpotExclusivityError(
                "global first-live cleanup-only authority exposes entry"
            )
        # Cleanup may be executed while ACTIVE, but this projection is never
        # interpreted as new-entry authority by a cleanup caller.
    elif (
        phase != "ACTIVE"
        or row.get("entryAuthorityOpen") is not True
        or float(row["hardStopEpoch"]) <= float(now_epoch)
    ):
        raise BinanceSpotExclusivityError(
            "global first-live entry authority is closed"
        )
    return {
        **row,
        "purpose": _text(purpose).upper(),
        "verified": True,
    }


def _component(
    value: object,
    *,
    name: str,
    schema: str,
    source: str,
    session_id: str,
    account_identity: str,
    credential_fingerprint: str,
    coverage_started_at: str,
    observed_at: str,
    boundary_id: str,
    boundary_hash: str,
) -> dict[str, Any]:
    common = {
        "schemaVersion",
        "source",
        "sessionId",
        "accountIdentityFingerprint",
        "credentialFingerprint",
        "coverageStartedAt",
        "coverageEndedAt",
        "complete",
        "independentlyVerified",
        "continuousCoverage",
        "authorityArtifactHash",
        "evidenceHash",
    }
    extras = {
        "apiCredentialInventory": {
            "activeApiCredentialCount",
            "authorizedFunctionalCredentialCount",
            "otherActiveApiCredentialCount",
        },
        "manualTradeAudit": {"manualOrderCount"},
        "botRegistry": {
            "activeBotCount",
            "authorizedFunctionalBotCount",
            "otherActiveBotCount",
        },
        "accountWideCausalAudit": {
            "allSymbolsCovered",
            "accountWideOrderEventCount",
            "accountWideTradeEventCount",
            "unownedOrderEventCount",
            "unownedTradeEventCount",
            "boundaryMarkerId",
            "boundaryMarkerHash",
            "causalClosureProven",
        },
    }[name]
    if not isinstance(value, Mapping) or set(value) != common | extras:
        raise BinanceSpotExclusivityError(f"{name} fields are not exact")
    row = dict(value)
    exact = {
        "schemaVersion": schema,
        "source": source,
        "sessionId": session_id,
        "accountIdentityFingerprint": account_identity,
        "credentialFingerprint": credential_fingerprint,
        "coverageStartedAt": coverage_started_at,
        "coverageEndedAt": observed_at,
    }
    for field, expected in exact.items():
        actual = row.get(field)
        if type(actual) is not str or not hmac.compare_digest(actual, expected):
            raise BinanceSpotExclusivityError(f"{name} {field} binding changed")
    if (
        row.get("complete") is not True
        or row.get("independentlyVerified") is not True
        or row.get("continuousCoverage") is not True
        or not _is_hash(row.get("authorityArtifactHash"))
    ):
        raise BinanceSpotExclusivityError(f"{name} is incomplete")
    count_fields = extras - {
        "allSymbolsCovered",
        "boundaryMarkerId",
        "boundaryMarkerHash",
        "causalClosureProven",
    }
    if any(
        isinstance(row.get(field), bool)
        or not isinstance(row.get(field), int)
        or row[field] < 0
        for field in count_fields
    ):
        raise BinanceSpotExclusivityError(f"{name} counts are invalid")
    if name == "apiCredentialInventory" and (
        row["activeApiCredentialCount"] != 1
        or row["authorizedFunctionalCredentialCount"] != 1
        or row["otherActiveApiCredentialCount"] != 0
    ):
        raise BinanceSpotExclusivityError("another Binance API credential is active")
    if name == "manualTradeAudit" and row["manualOrderCount"] != 0:
        raise BinanceSpotExclusivityError("manual Binance order activity was observed")
    if name == "botRegistry" and (
        row["activeBotCount"] != 1
        or row["authorizedFunctionalBotCount"] != 1
        or row["otherActiveBotCount"] != 0
    ):
        raise BinanceSpotExclusivityError("another Binance bot is active")
    if name == "accountWideCausalAudit":
        if (
            row.get("allSymbolsCovered") is not True
            or row["unownedOrderEventCount"] != 0
            or row["unownedTradeEventCount"] != 0
            or type(row.get("boundaryMarkerId")) is not str
            or _SAFE_ID_RE.fullmatch(row["boundaryMarkerId"]) is None
            or not _is_hash(row.get("boundaryMarkerHash"))
            or type(row.get("causalClosureProven")) is not bool
        ):
            raise BinanceSpotExclusivityError(
                "account-wide causal audit is incomplete or contains external activity"
            )
        if (
            not hmac.compare_digest(row["boundaryMarkerId"], boundary_id)
            or not hmac.compare_digest(row["boundaryMarkerHash"], boundary_hash)
        ):
            raise BinanceSpotExclusivityError(
                "account-wide causal audit boundary marker changed"
            )
    evidence_hash = row.get("evidenceHash")
    projection = {key: item for key, item in row.items() if key != "evidenceHash"}
    if not _is_hash(evidence_hash) or not hmac.compare_digest(
        evidence_hash, _stable_hash(projection)
    ):
        raise BinanceSpotExclusivityError(f"{name} evidence hash changed")
    return row


@dataclass(frozen=True)
class VerifiedBinanceSpotExclusivityProof:
    proof: Mapping[str, Any]
    proof_hash: str
    observed_epoch: float
    account_wide_causal_closure_proven: bool

    def summary(self) -> dict[str, Any]:
        api_inventory = dict(self.proof["apiCredentialInventory"])
        manual_audit = dict(self.proof["manualTradeAudit"])
        bot_registry = dict(self.proof["botRegistry"])
        no_other_api_keys = bool(
            api_inventory["activeApiCredentialCount"] == 1
            and api_inventory["authorizedFunctionalCredentialCount"] == 1
            and api_inventory["otherActiveApiCredentialCount"] == 0
        )
        no_manual_trading = bool(manual_audit["manualOrderCount"] == 0)
        no_other_bots = bool(
            bot_registry["activeBotCount"] == 1
            and bot_registry["authorizedFunctionalBotCount"] == 1
            and bot_registry["otherActiveBotCount"] == 0
        )
        return {
            "verified": True,
            "phase": self.proof["phase"],
            "sessionId": self.proof["sessionId"],
            "boundaryId": self.proof["boundaryId"],
            "proofId": self.proof["proofId"],
            "proofHash": self.proof_hash,
            "proofRequestHash": self.proof["proofRequestHash"],
            "authorityJournalId": self.proof["authorityJournalId"],
            "authoritySequence": self.proof["authoritySequence"],
            "previousAuthorityProofHash": self.proof[
                "previousAuthorityProofHash"
            ],
            "serverOwnerIdentitySha256": self.proof[
                "serverOwnerIdentitySha256"
            ],
            "observedEpoch": self.observed_epoch,
            "exclusiveAccountConfirmed": bool(
                no_other_api_keys and no_manual_trading and no_other_bots
            ),
            "noManualTradingConfirmed": no_manual_trading,
            "noBotsConfirmed": no_other_bots,
            "noOtherApiKeysConfirmed": no_other_api_keys,
            "accountWideCausalClosureProven": (
                self.account_wide_causal_closure_proven
            ),
        }


def verify_exclusivity_proof(
    value: object,
    *,
    phase: str,
    session_id: str,
    permit_id: str,
    permit_hash: str,
    account_identity_fingerprint: str,
    credential_fingerprint: str,
    boundary_id: str,
    boundary_hash: str,
    coverage_started_epoch: float,
    requested_epoch: float,
    now_epoch: float,
    verifier: BinanceSpotExclusivityVerifier | None,
    verifier_pin: Mapping[str, Any] | None,
    require_causal_closure: bool,
) -> VerifiedBinanceSpotExclusivityProof:
    normalized_phase = _text(phase).upper()
    pin = normalize_verifier_pin(verifier_pin)
    account_identity = _text(account_identity_fingerprint).lower()
    credential = _text(credential_fingerprint).lower()
    permit_digest = _text(permit_hash).lower()
    boundary_digest = _text(boundary_hash).lower()
    if (
        normalized_phase not in _PHASES
        or not _SAFE_ID_RE.fullmatch(_text(session_id))
        or not _SAFE_ID_RE.fullmatch(_text(permit_id))
        or not _SAFE_ID_RE.fullmatch(_text(boundary_id))
        or any(
            not _is_hash(item)
            for item in (
                permit_digest,
                account_identity,
                credential,
                boundary_digest,
            )
        )
        or hmac.compare_digest(account_identity, credential)
        or pin is None
        or verifier is None
    ):
        raise BinanceSpotExclusivityError("exclusivity verification context is incomplete")
    wiring = verifier_wiring_status(verifier, pin, account_identity)
    if wiring.get("ready") is not True:
        raise BinanceSpotExclusivityError(_text(wiring.get("reason")))
    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError("detached exclusivity proof is missing")
    raw = dict(value)
    fields = {
        "schemaVersion",
        "proofId",
        "phase",
        "sessionId",
        "permitId",
        "permitHash",
        "accountIdentityFingerprint",
        "credentialFingerprint",
        "boundaryId",
        "boundaryHash",
        "coverageStartedAt",
        "requestedAt",
        "requireCausalClosure",
        "observedAt",
        "authorityJournalId",
        "authoritySequence",
        "previousAuthorityProofHash",
        "proofRequestHash",
        "serverOwnerIdentitySha256",
        "authority",
        "apiCredentialInventory",
        "manualTradeAudit",
        "botRegistry",
        "accountWideCausalAudit",
        "payloadHash",
        "signature",
    }
    if set(raw) != fields:
        raise BinanceSpotExclusivityError("detached exclusivity proof fields are not exact")
    coverage_text = _utc_text(coverage_started_epoch)
    request = exclusivity_proof_request_payload(
        phase=normalized_phase,
        session_id=session_id,
        permit_id=permit_id,
        permit_hash=permit_digest,
        account_identity_fingerprint=account_identity,
        credential_fingerprint=credential,
        boundary_id=boundary_id,
        boundary_hash=boundary_digest,
        coverage_started_epoch=coverage_started_epoch,
        requested_epoch=requested_epoch,
        require_causal_closure=require_causal_closure,
    )
    expected = {
        "schemaVersion": PROOF_SCHEMA_VERSION,
        "phase": normalized_phase,
        "sessionId": _text(session_id),
        "permitId": _text(permit_id),
        "permitHash": permit_digest,
        "accountIdentityFingerprint": account_identity,
        "credentialFingerprint": credential,
        "boundaryId": _text(boundary_id),
        "boundaryHash": boundary_digest,
        "coverageStartedAt": coverage_text,
        "requestedAt": request["requestedAt"],
    }
    for field, expected_value in expected.items():
        actual = raw.get(field)
        if type(actual) is not str or not hmac.compare_digest(actual, expected_value):
            raise BinanceSpotExclusivityError(f"detached proof {field} changed")
    proof_id = raw.get("proofId")
    if type(proof_id) is not str or _SAFE_ID_RE.fullmatch(proof_id) is None:
        raise BinanceSpotExclusivityError("detached proof identity is invalid")
    if (
        raw.get("requireCausalClosure") is not bool(require_causal_closure)
        or type(raw.get("authorityJournalId")) is not str
        or _SAFE_ID_RE.fullmatch(raw["authorityJournalId"]) is None
        or isinstance(raw.get("authoritySequence"), bool)
        or not isinstance(raw.get("authoritySequence"), int)
        or raw["authoritySequence"] < 1
        or not _is_hash(raw.get("previousAuthorityProofHash"))
        or not _is_hash(raw.get("proofRequestHash"))
        or not hmac.compare_digest(
            raw["proofRequestHash"], _stable_hash(request)
        )
        or not _is_hash(raw.get("serverOwnerIdentitySha256"))
    ):
        raise BinanceSpotExclusivityError(
            "detached proof authority chain/request binding is invalid"
        )
    observed_epoch = _utc_epoch(raw.get("observedAt"), "proof observedAt")
    age = float(now_epoch) - observed_epoch
    if (
        age < -1.0
        or age > MAX_PROOF_AGE_SECONDS
        or observed_epoch < float(coverage_started_epoch)
        or observed_epoch < float(requested_epoch)
    ):
        raise BinanceSpotExclusivityError("detached exclusivity proof is stale or future-dated")
    if not isinstance(raw.get("authority"), Mapping) or dict(raw["authority"]) != pin:
        raise BinanceSpotExclusivityError("detached proof authority pin changed")
    components: dict[str, dict[str, Any]] = {}
    for name, schema, source in (
        (
            "apiCredentialInventory",
            "binance-account-api-credential-inventory-evidence/v1",
            API_INVENTORY_SOURCE,
        ),
        (
            "manualTradeAudit",
            "binance-account-manual-trade-audit-evidence/v1",
            MANUAL_AUDIT_SOURCE,
        ),
        (
            "botRegistry",
            "binance-account-bot-registry-evidence/v1",
            BOT_REGISTRY_SOURCE,
        ),
        (
            "accountWideCausalAudit",
            "binance-account-wide-causal-audit-evidence/v1",
            CAUSAL_AUDIT_SOURCE,
        ),
    ):
        components[name] = _component(
            raw.get(name),
            name=name,
            schema=schema,
            source=source,
            session_id=_text(session_id),
            account_identity=account_identity,
            credential_fingerprint=credential,
            coverage_started_at=coverage_text,
            observed_at=raw["observedAt"],
            boundary_id=_text(boundary_id),
            boundary_hash=boundary_digest,
        )
    causal = components["accountWideCausalAudit"]
    causal_proven = causal.get("causalClosureProven") is True
    if require_causal_closure and not causal_proven:
        raise BinanceSpotExclusivityError(
            "terminal account-wide causal closure is not independently proven"
        )
    payload_hash = raw.get("payloadHash")
    signature = raw.get("signature")
    payload = {
        key: item for key, item in raw.items() if key not in {"payloadHash", "signature"}
    }
    if (
        not _is_hash(payload_hash)
        or not hmac.compare_digest(payload_hash, _stable_hash(payload))
        or type(signature) is not str
        or len(signature) < 32
    ):
        raise BinanceSpotExclusivityError("detached signed payload changed")
    try:
        signature_valid = verifier(
            payload=payload,
            signature=signature,
            verifier_pin=pin,
        )
    except Exception:
        signature_valid = False
    if signature_valid is not True:
        raise BinanceSpotExclusivityError("detached exclusivity signature is invalid")
    normalized = {**payload, "payloadHash": payload_hash, "signature": signature}
    normalized.update(components)
    normalized["authority"] = pin
    if normalized != raw:
        raise BinanceSpotExclusivityError("detached exclusivity proof is not canonical")
    return VerifiedBinanceSpotExclusivityProof(
        proof=normalized,
        proof_hash=_stable_hash(normalized),
        observed_epoch=observed_epoch,
        account_wide_causal_closure_proven=causal_proven,
    )


class DurableBinanceSpotExclusivityProofStore:
    """Append-only phase evidence; raw API secrets are never persisted."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS binance_spot_functional_exclusivity_proofs (
                    proof_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    boundary_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    account_identity_fingerprint TEXT NOT NULL,
                    credential_fingerprint TEXT NOT NULL,
                    proof_json TEXT NOT NULL,
                    proof_hash TEXT NOT NULL UNIQUE,
                    observed_epoch REAL NOT NULL,
                    causal_closure_proven INTEGER NOT NULL,
                    created_epoch REAL NOT NULL,
                    UNIQUE(session_id, phase, boundary_id)
                )
                """
            )
            connection.commit()

    def record(
        self,
        verified: VerifiedBinanceSpotExclusivityProof,
        *,
        recorded_epoch: float | None = None,
    ) -> dict[str, Any]:
        proof = dict(verified.proof)
        raw = _canonical(proof)
        now = time.time() if recorded_epoch is None else float(recorded_epoch)
        if not math.isfinite(now):
            raise BinanceSpotExclusivityError(
                "durable exclusivity record time is invalid"
            )
        values = (
            proof["proofId"],
            proof["sessionId"],
            proof["phase"],
            proof["boundaryId"],
            proof["permitId"],
            proof["permitHash"],
            proof["accountIdentityFingerprint"],
            proof["credentialFingerprint"],
            raw,
            verified.proof_hash,
            verified.observed_epoch,
            1 if verified.account_wide_causal_closure_proven else 0,
            now,
        )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT * FROM binance_spot_functional_exclusivity_proofs
                WHERE session_id=? AND phase=? AND boundary_id=?""",
                (proof["sessionId"], proof["phase"], proof["boundaryId"]),
            ).fetchone()
            if existing is not None:
                if not hmac.compare_digest(
                    _text(existing["proof_hash"]), verified.proof_hash
                ):
                    connection.rollback()
                    raise BinanceSpotExclusivityError(
                        "durable exclusivity phase proof changed or was replayed"
                    )
                connection.rollback()
                return self._validated_row(existing)
            try:
                connection.execute(
                    """INSERT INTO binance_spot_functional_exclusivity_proofs (
                    proof_id,session_id,phase,boundary_id,permit_id,permit_hash,
                    account_identity_fingerprint,credential_fingerprint,
                    proof_json,proof_hash,observed_epoch,causal_closure_proven,
                    created_epoch) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    values,
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotExclusivityError(
                    "detached exclusivity proof identity/hash is a replay"
                ) from exc
        return self.record_for(
            session_id=proof["sessionId"],
            phase=proof["phase"],
            boundary_id=proof["boundaryId"],
        )

    @staticmethod
    def _validated_row(row: sqlite3.Row) -> dict[str, Any]:
        try:
            proof = json.loads(row["proof_json"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof JSON is malformed"
            ) from exc
        if not isinstance(proof, Mapping):
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof identity/hash changed"
            )
        try:
            observed_epoch = float(row["observed_epoch"])
            created_epoch = float(row["created_epoch"])
            proof_observed_epoch = _utc_epoch(
                proof.get("observedAt"), "durable proof observedAt"
            )
            stored_causal = int(row["causal_closure_proven"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof time/causal metadata is malformed"
            ) from exc
        proof_causal = (
            isinstance(proof.get("accountWideCausalAudit"), Mapping)
            and proof["accountWideCausalAudit"].get("causalClosureProven") is True
        )
        if (
            not _is_hash(row["proof_hash"])
            or not hmac.compare_digest(_stable_hash(proof), row["proof_hash"])
            or proof.get("proofId") != row["proof_id"]
            or proof.get("sessionId") != row["session_id"]
            or proof.get("phase") != row["phase"]
            or proof.get("boundaryId") != row["boundary_id"]
            or proof.get("permitId") != row["permit_id"]
            or proof.get("permitHash") != row["permit_hash"]
            or proof.get("accountIdentityFingerprint")
            != row["account_identity_fingerprint"]
            or proof.get("credentialFingerprint") != row["credential_fingerprint"]
            or not math.isfinite(observed_epoch)
            or not math.isfinite(created_epoch)
            or abs(observed_epoch - proof_observed_epoch) > 0.000001
            or created_epoch < observed_epoch - 1.0
            or created_epoch - observed_epoch > MAX_PROOF_AGE_SECONDS
            or stored_causal not in {0, 1}
            or bool(stored_causal) is not proof_causal
        ):
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof identity/hash changed"
            )
        result = dict(row)
        result["proof"] = dict(proof)
        return result

    def record_for(self, *, session_id: str, phase: str, boundary_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM binance_spot_functional_exclusivity_proofs
                WHERE session_id=? AND phase=? AND boundary_id=?""",
                (_text(session_id), _text(phase).upper(), _text(boundary_id)),
            ).fetchone()
        if row is None:
            raise BinanceSpotExclusivityError("durable exclusivity phase proof is absent")
        return self._validated_row(row)

    def session_records(self, session_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM binance_spot_functional_exclusivity_proofs
                WHERE session_id=? ORDER BY created_epoch,proof_id""",
                (_text(session_id),),
            ).fetchall()
        return [self._validated_row(row) for row in rows]


class BinanceSpotExclusivityGuard:
    """Fetch, verify, and durably seal one exact phase proof."""

    def __init__(
        self,
        *,
        store: DurableBinanceSpotExclusivityProofStore,
        proof_reader: Callable[..., Mapping[str, Any]] | None,
        verifier: BinanceSpotExclusivityVerifier | None,
        verifier_pin: Mapping[str, Any] | None,
        account_identity_fingerprint: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.proof_reader = proof_reader
        self.verifier = verifier
        self.verifier_pin = dict(verifier_pin) if isinstance(verifier_pin, Mapping) else None
        self.account_identity_fingerprint = _text(account_identity_fingerprint).lower()
        self.clock = clock

    def status(self) -> dict[str, Any]:
        wiring = verifier_wiring_status(
            self.verifier, self.verifier_pin, self.account_identity_fingerprint
        )
        return {
            **wiring,
            "proofReaderWired": callable(self.proof_reader),
            "ready": wiring.get("ready") is True and callable(self.proof_reader),
            "releaseFlags": {
                "authorityPinned": BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED,
                "verifierWired": BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED,
                "causalAuthorityAvailable": (
                    BINANCE_SPOT_ACCOUNT_WIDE_CAUSAL_AUTHORITY_AVAILABLE
                ),
            },
        }

    def verify_and_record(
        self,
        *,
        phase: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        credential_fingerprint: str,
        boundary_id: str,
        boundary_hash: str,
        coverage_started_epoch: float,
        require_causal_closure: bool = False,
    ) -> dict[str, Any]:
        now = float(self.clock())
        if self.status().get("ready") is not True or self.proof_reader is None:
            raise BinanceSpotExclusivityError(
                "independent Binance exclusivity verifier/proof reader is not ready"
            )
        request = {
            "phase": _text(phase).upper(),
            "sessionId": _text(session_id),
            "permitId": _text(permit_id),
            "permitHash": _text(permit_hash).lower(),
            "accountIdentityFingerprint": self.account_identity_fingerprint,
            "credentialFingerprint": _text(credential_fingerprint).lower(),
            "boundaryId": _text(boundary_id),
            "boundaryHash": _text(boundary_hash).lower(),
            "coverageStartedAt": _utc_text(coverage_started_epoch),
            "requestedAt": _utc_text(now),
            "requireCausalClosure": bool(require_causal_closure),
        }
        try:
            proof = self.proof_reader(**request)
        except BinanceSpotExclusivityError:
            raise
        except Exception as exc:
            raise BinanceSpotExclusivityError(
                "independent Binance exclusivity proof reader failed closed"
            ) from exc
        verified = verify_exclusivity_proof(
            proof,
            phase=request["phase"],
            session_id=request["sessionId"],
            permit_id=request["permitId"],
            permit_hash=request["permitHash"],
            account_identity_fingerprint=request["accountIdentityFingerprint"],
            credential_fingerprint=request["credentialFingerprint"],
            boundary_id=request["boundaryId"],
            boundary_hash=request["boundaryHash"],
            coverage_started_epoch=coverage_started_epoch,
            requested_epoch=now,
            now_epoch=now,
            verifier=self.verifier,
            verifier_pin=self.verifier_pin,
            require_causal_closure=require_causal_closure,
        )
        durable = self.store.record(verified, recorded_epoch=now)
        self._verify_durable_record(durable)
        return {
            **verified.summary(),
            "proof": dict(verified.proof),
            "durable": True,
            "durableProofHash": durable["proof_hash"],
            "restartVerifiable": True,
        }

    def _verify_durable_record(self, row: Mapping[str, Any]) -> None:
        proof = row.get("proof")
        if not isinstance(proof, Mapping):
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof body is absent"
            )
        observed_epoch = _utc_epoch(
            proof.get("observedAt"), "durable proof observedAt"
        )
        coverage_started_epoch = _utc_epoch(
            proof.get("coverageStartedAt"),
            "durable proof coverageStartedAt",
        )
        requested_epoch = _utc_epoch(
            proof.get("requestedAt"), "durable proof requestedAt"
        )
        verified = verify_exclusivity_proof(
            proof,
            phase=_text(proof.get("phase")),
            session_id=_text(proof.get("sessionId")),
            permit_id=_text(proof.get("permitId")),
            permit_hash=_text(proof.get("permitHash")),
            account_identity_fingerprint=self.account_identity_fingerprint,
            credential_fingerprint=_text(proof.get("credentialFingerprint")),
            boundary_id=_text(proof.get("boundaryId")),
            boundary_hash=_text(proof.get("boundaryHash")),
            coverage_started_epoch=coverage_started_epoch,
            requested_epoch=requested_epoch,
            # Historic rows are checked at their signed observation instant;
            # the durable created/observed delta was validated by the store.
            now_epoch=observed_epoch,
            verifier=self.verifier,
            verifier_pin=self.verifier_pin,
            require_causal_closure=(
                proof.get("requireCausalClosure") is True
            ),
        )
        if not hmac.compare_digest(
            verified.proof_hash, _text(row.get("proof_hash"))
        ):
            raise BinanceSpotExclusivityError(
                "durable exclusivity proof signature/hash changed"
            )

    def session_records(self, session_id: str) -> list[dict[str, Any]]:
        """Reload and cryptographically re-verify every durable phase proof."""

        records = self.store.session_records(session_id)
        for row in records:
            self._verify_durable_record(row)
        return records


__all__ = [
    "API_INVENTORY_SOURCE",
    "BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_AUTHORITY_PINNED",
    "BINANCE_SPOT_ACCOUNT_EXCLUSIVITY_VERIFIER_WIRED",
    "BINANCE_SPOT_ACCOUNT_WIDE_CAUSAL_AUTHORITY_AVAILABLE",
    "BINANCE_SPOT_GLOBAL_FIRST_LIVE_AUTHORITY_WIRED",
    "BOT_REGISTRY_SOURCE",
    "BinanceSpotExclusivityError",
    "BinanceSpotExclusivityGuard",
    "BinanceSpotExclusivityVerifier",
    "CAUSAL_AUDIT_SOURCE",
    "DurableBinanceSpotExclusivityProofStore",
    "MANUAL_AUDIT_SOURCE",
    "MAX_PROOF_AGE_SECONDS",
    "PROOF_SCHEMA_VERSION",
    "PROOF_REQUEST_SCHEMA_VERSION",
    "GLOBAL_AUTHORITY_SCHEMA_VERSION",
    "VERIFIER_PIN_SCHEMA_VERSION",
    "VerifiedBinanceSpotExclusivityProof",
    "normalize_verifier_pin",
    "exclusivity_proof_request_payload",
    "verifier_wiring_status",
    "verify_global_first_live_authority",
    "verify_exclusivity_proof",
]
