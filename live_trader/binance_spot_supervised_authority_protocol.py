from __future__ import annotations

"""Public-key-only protocol for the independent Binance observer daemon."""

import base64
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import threading
from typing import Any, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .binance_spot_functional_exclusivity import BinanceSpotExclusivityError
from .binance_spot_functional_supervised_exclusivity import (
    ASSURANCE_MODE,
    OFFICIAL_EVIDENCE_SCHEMA_VERSION,
)


SNAPSHOT_SCHEMA_VERSION = (
    "binance-spot-supervised-independent-authority-snapshot/v1"
)
PROCESS_AUDIT_SCHEMA_VERSION = (
    "binance-spot-supervised-independent-process-audit/v1"
)
STREAM_AUDIT_SCHEMA_VERSION = (
    "binance-spot-supervised-independent-stream-audit/v1"
)
MAX_SNAPSHOT_AGE_SECONDS = 5.0
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_SIGNATURE_RE = re.compile(r"^[A-Za-z0-9_-]{86}$")
MAX_SNAPSHOT_FILE_BYTES = 2 * 1024 * 1024


def canonical_authority_message(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def authority_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _text(value: object) -> str:
    return str(value or "").strip()


def _is_hash(value: object) -> bool:
    return type(value) is str and _HASH_RE.fullmatch(value) is not None


def _epoch(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BinanceSpotExclusivityError(f"{label} is not an exact epoch")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise BinanceSpotExclusivityError(f"{label} is invalid")
    return result


class PinnedBinanceSpotSupervisedAuthorityVerifier:
    """Pinned Ed25519 verifier.  This class cannot sign authority records."""

    def __init__(
        self,
        *,
        public_key: bytes | str,
        authority_id: str,
        key_id: str,
        expected_credential_fingerprint: str,
    ) -> None:
        self.authority_id = _text(authority_id)
        self.key_id = _text(key_id)
        self.expected_credential_fingerprint = _text(
            expected_credential_fingerprint
        ).lower()
        if (
            _ID_RE.fullmatch(self.authority_id) is None
            or _ID_RE.fullmatch(self.key_id) is None
            or not _is_hash(self.expected_credential_fingerprint)
        ):
            raise BinanceSpotExclusivityError(
                "supervised authority verifier pin is invalid"
            )
        try:
            key = ECC.import_key(public_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise BinanceSpotExclusivityError(
                "supervised authority public key is invalid"
            ) from exc
        if key.has_private() or getattr(key, "curve", None) != "Ed25519":
            raise BinanceSpotExclusivityError(
                "supervised authority requires a public-only Ed25519 key"
            )
        self._key = key
        self.public_key_fingerprint = hashlib.sha256(
            key.export_key(format="DER")
        ).hexdigest()
        self._continuity_lock = threading.Lock()
        self._continuity: dict[
            tuple[str, str, str, str], tuple[int, str, str]
        ] = {}

    def status(self) -> dict[str, Any]:
        return {
            "ready": True,
            "authorityId": self.authority_id,
            "keyId": self.key_id,
            "algorithm": "Ed25519",
            "publicKeyOnly": True,
            "keyFingerprintSha256": self.public_key_fingerprint,
            "promotionEligible": False,
            "realE2EEligible": False,
        }

    def verify_snapshot(
        self,
        value: object,
        *,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        credential_fingerprint: str,
        owner_client_order_prefix: str,
        coverage_started_epoch: float,
        now_epoch: float,
    ) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise BinanceSpotExclusivityError(
                "independent supervised authority snapshot is missing"
            )
        row = dict(value)
        fields = {
            "schemaVersion",
            "assuranceMode",
            "authorityId",
            "keyId",
            "authorityProcessIdentityHash",
            "sessionId",
            "permitId",
            "permitHash",
            "credentialFingerprint",
            "ownerClientOrderPrefix",
            "coverageStartedEpoch",
            "observedEpoch",
            "authoritySequence",
            "previousSnapshotHash",
            "officialBaseline",
            "processAudit",
            "userDataStreamAudit",
            "revoked",
            "cleanupOnlyRequired",
            "revokeReason",
            "otherApiKeyInventoryProven",
            "manualOrderCausalAuditIndependentlyVerified",
            "botRegistryIndependentlyVerified",
            "accountWideCausalClosureProven",
            "promotionEligible",
            "realE2EEligible",
            "productionPromotionAllowed",
            "payloadHash",
            "signature",
        }
        if set(row) != fields:
            raise BinanceSpotExclusivityError(
                "independent authority snapshot fields are not exact"
            )
        payload = {key: item for key, item in row.items() if key != "signature"}
        body = {key: item for key, item in payload.items() if key != "payloadHash"}
        credential = _text(credential_fingerprint).lower()
        if not hmac.compare_digest(
            credential, self.expected_credential_fingerprint
        ):
            raise BinanceSpotExclusivityError(
                "independent authority credential pin changed"
            )
        expected = {
            "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
            "assuranceMode": ASSURANCE_MODE,
            "authorityId": self.authority_id,
            "keyId": self.key_id,
            "sessionId": _text(session_id),
            "permitId": _text(permit_id),
            "permitHash": _text(permit_hash).lower(),
            "credentialFingerprint": self.expected_credential_fingerprint,
            "ownerClientOrderPrefix": _text(owner_client_order_prefix),
        }
        for field, expected_value in expected.items():
            actual = row.get(field)
            if type(actual) is not str or not hmac.compare_digest(
                actual, expected_value
            ):
                raise BinanceSpotExclusivityError(
                    f"independent authority {field} binding changed"
                )
        coverage = _epoch(row.get("coverageStartedEpoch"), "authority coverage")
        observed = _epoch(row.get("observedEpoch"), "authority observation")
        now = float(now_epoch)
        if (
            coverage > float(coverage_started_epoch) + 1.0
            or float(coverage_started_epoch) - coverage > 15.0
            or observed < coverage
            or now - observed < -1.0
            or now - observed > MAX_SNAPSHOT_AGE_SECONDS
            or isinstance(row.get("authoritySequence"), bool)
            or not isinstance(row.get("authoritySequence"), int)
            or row["authoritySequence"] < 1
            or not _is_hash(row.get("previousSnapshotHash"))
            or not _is_hash(row.get("authorityProcessIdentityHash"))
            or not _is_hash(row.get("payloadHash"))
            or not hmac.compare_digest(row["payloadHash"], authority_hash(body))
            or type(row.get("signature")) is not str
            or _SIGNATURE_RE.fullmatch(row["signature"]) is None
        ):
            raise BinanceSpotExclusivityError(
                "independent authority time/sequence/hash is invalid"
            )
        try:
            signature = base64.urlsafe_b64decode(row["signature"] + "==")
            if len(signature) != 64:
                raise ValueError("signature length")
            eddsa.new(self._key, "rfc8032").verify(
                canonical_authority_message(payload), signature
            )
        except (ValueError, TypeError) as exc:
            raise BinanceSpotExclusivityError(
                "independent authority Ed25519 signature is invalid"
            ) from exc
        self._validate_official(row.get("officialBaseline"), coverage=coverage)
        self._validate_process(row.get("processAudit"), observed=observed)
        self._validate_stream(
            row.get("userDataStreamAudit"),
            session_id=session_id,
            permit_id=permit_id,
            permit_hash=permit_hash,
            owner_client_order_prefix=owner_client_order_prefix,
            coverage=coverage,
            observed=observed,
        )
        if (
            row.get("revoked") is not False
            or row.get("cleanupOnlyRequired") is not False
            or row.get("revokeReason") != ""
            or row.get("otherApiKeyInventoryProven") is not False
            or row.get("manualOrderCausalAuditIndependentlyVerified") is not True
            or row.get("botRegistryIndependentlyVerified") is not True
            or row.get("accountWideCausalClosureProven") is not False
            or row.get("promotionEligible") is not False
            or row.get("realE2EEligible") is not False
            or row.get("productionPromotionAllowed") is not False
        ):
            raise BinanceSpotExclusivityError(
                "independent authority is revoked or overclaims assurance"
            )
        self._validate_continuity(row)
        return row

    def _validate_continuity(self, row: Mapping[str, Any]) -> None:
        """Reject rollback/equivocation across repeated health reads.

        Pollers can legitimately skip one-second snapshots, so a gap in the
        observed sequence cannot prove a complete hash chain.  The verifier
        still enforces monotonicity, exact idempotence, stable daemon process
        identity, and the producer's previous-envelope link whenever two
        adjacent snapshots are observed.
        """

        binding = (
            _text(row.get("sessionId")),
            _text(row.get("permitId")),
            _text(row.get("permitHash")).lower(),
            _text(row.get("ownerClientOrderPrefix")),
        )
        sequence = int(row["authoritySequence"])
        envelope_hash = authority_hash(dict(row))
        process_identity = _text(row.get("authorityProcessIdentityHash"))
        with self._continuity_lock:
            prior = self._continuity.get(binding)
            if prior is not None:
                prior_sequence, prior_hash, prior_process = prior
                if process_identity != prior_process or sequence < prior_sequence:
                    raise BinanceSpotExclusivityError(
                        "independent authority continuity rolled back"
                    )
                if sequence == prior_sequence:
                    if not hmac.compare_digest(envelope_hash, prior_hash):
                        raise BinanceSpotExclusivityError(
                            "independent authority sequence equivocated"
                        )
                    return
                if sequence == prior_sequence + 1 and not hmac.compare_digest(
                    _text(row.get("previousSnapshotHash")), prior_hash
                ):
                    raise BinanceSpotExclusivityError(
                        "independent authority adjacent hash link changed"
                    )
            self._continuity[binding] = (
                sequence,
                envelope_hash,
                process_identity,
            )

    @staticmethod
    def _validate_official(value: object, *, coverage: float) -> None:
        if not isinstance(value, Mapping):
            raise BinanceSpotExclusivityError(
                "independent authority official baseline is absent"
            )
        row = dict(value)
        restrictions = row.get("apiRestrictions")
        trading = row.get("apiTradingStatus")
        orders = row.get("accountWideOpenOrders")
        transport = row.get("transport")
        if (
            row.get("schemaVersion") != OFFICIAL_EVIDENCE_SCHEMA_VERSION
            or row.get("origin") != "https://api.binance.com"
            or abs(_epoch(row.get("observedEpoch"), "official baseline") - coverage)
            > 5.0
            or not isinstance(restrictions, Mapping)
            or restrictions.get("enableReading") is not True
            or restrictions.get("enableSpotAndMarginTrading") is not True
            or restrictions.get("ipRestrict") is not True
            or not _is_hash(restrictions.get("responseHash"))
            or not isinstance(trading, Mapping)
            or trading.get("locked") is not False
            or not _is_hash(trading.get("responseHash"))
            or not isinstance(orders, Mapping)
            or orders.get("scope") != "ACCOUNT_WIDE_ALL_SYMBOLS"
            or orders.get("openOrderCount") != 0
            or not _is_hash(orders.get("responseHash"))
            or not isinstance(transport, Mapping)
            or dict(transport)
            != {
                "physicalGetAttemptCount": 3,
                "retryCount": 0,
                "redirectCount": 0,
                "nonGetAttemptCount": 0,
                "mutationAttemptCount": 0,
            }
            or not _is_hash(row.get("evidenceHash"))
        ):
            raise BinanceSpotExclusivityError(
                "independent authority official baseline is incomplete"
            )
        evidence_body = {
            key: item for key, item in row.items() if key != "evidenceHash"
        }
        if not hmac.compare_digest(
            row["evidenceHash"], authority_hash(evidence_body)
        ):
            raise BinanceSpotExclusivityError(
                "independent authority official baseline hash changed"
            )

    @staticmethod
    def _validate_process(value: object, *, observed: float) -> None:
        if not isinstance(value, Mapping):
            raise BinanceSpotExclusivityError(
                "independent authority process audit is absent"
            )
        row = dict(value)
        fields = {
            "schemaVersion",
            "source",
            "observedEpoch",
            "authorizedTraderProcessIdentityHash",
            "authorizedFunctionalBotCount",
            "otherRegisteredBotCount",
            "observerProcessSeparate",
            "independentlyVerified",
            "auditHash",
        }
        body = {key: item for key, item in row.items() if key != "auditHash"}
        if (
            set(row) != fields
            or row.get("schemaVersion") != PROCESS_AUDIT_SCHEMA_VERSION
            or row.get("source") != "WINDOWS_CIM_PROCESS_REGISTRY"
            or abs(_epoch(row.get("observedEpoch"), "process audit") - observed)
            > MAX_SNAPSHOT_AGE_SECONDS
            or not _is_hash(row.get("authorizedTraderProcessIdentityHash"))
            or row.get("authorizedFunctionalBotCount") != 1
            or row.get("otherRegisteredBotCount") != 0
            or row.get("observerProcessSeparate") is not True
            or row.get("independentlyVerified") is not True
            or not _is_hash(row.get("auditHash"))
            or not hmac.compare_digest(row["auditHash"], authority_hash(body))
        ):
            raise BinanceSpotExclusivityError(
                "independent authority process/bot audit is incomplete"
            )

    @staticmethod
    def _validate_stream(
        value: object,
        *,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        owner_client_order_prefix: str,
        coverage: float,
        observed: float,
    ) -> None:
        if not isinstance(value, Mapping):
            raise BinanceSpotExclusivityError(
                "independent authority stream audit is absent"
            )
        row = dict(value)
        fields = {
            "schemaVersion",
            "transportKind",
            "listenKeyRequired",
            "subscriptionAuthenticated",
            "connected",
            "gapDetected",
            "crashDetected",
            "continuousCoverage",
            "sessionId",
            "permitId",
            "permitHash",
            "ownerClientOrderPrefix",
            "subscribedEpoch",
            "lastLivenessEpoch",
            "eventCount",
            "orderEventCount",
            "unownedOrderEventCount",
            "eventChainHash",
            "journalDatabaseIdentityHash",
            "auditHash",
        }
        body = {key: item for key, item in row.items() if key != "auditHash"}
        count_fields = ("eventCount", "orderEventCount", "unownedOrderEventCount")
        if (
            set(row) != fields
            or row.get("schemaVersion") != STREAM_AUDIT_SCHEMA_VERSION
            or row.get("transportKind")
            != "SIGNED_WS_API_USER_DATA_STREAM"
            or row.get("listenKeyRequired") is not False
            or row.get("subscriptionAuthenticated") is not True
            or row.get("connected") is not True
            or row.get("gapDetected") is not False
            or row.get("crashDetected") is not False
            or row.get("continuousCoverage") is not True
            or row.get("sessionId") != _text(session_id)
            or row.get("permitId") != _text(permit_id)
            or row.get("permitHash") != _text(permit_hash).lower()
            or row.get("ownerClientOrderPrefix") != _text(owner_client_order_prefix)
            or abs(_epoch(row.get("subscribedEpoch"), "stream subscription") - coverage)
            > 5.0
            or observed - _epoch(row.get("lastLivenessEpoch"), "stream liveness")
            > MAX_SNAPSHOT_AGE_SECONDS
            or any(
                isinstance(row.get(field), bool)
                or not isinstance(row.get(field), int)
                or row[field] < 0
                for field in count_fields
            )
            or row.get("unownedOrderEventCount") != 0
            or not _is_hash(row.get("eventChainHash"))
            or not _is_hash(row.get("journalDatabaseIdentityHash"))
            or not _is_hash(row.get("auditHash"))
            or not hmac.compare_digest(row["auditHash"], authority_hash(body))
        ):
            raise BinanceSpotExclusivityError(
                "independent authority user-data stream is incomplete"
            )


class BinanceSpotSupervisedAuthoritySnapshotReader:
    """Read one atomically replaced signed snapshot from the observer."""

    def __init__(self, path: str | Path) -> None:
        raw = Path(path)
        if not raw.is_absolute():
            raise BinanceSpotExclusivityError(
                "independent authority snapshot path must be absolute"
            )
        self.path = raw.resolve()

    def __call__(self, **_request: Any) -> dict[str, Any]:
        if (
            not self.path.is_file()
            or self.path.is_symlink()
            or self.path.resolve() != self.path
        ):
            raise BinanceSpotExclusivityError(
                "independent authority snapshot file is unavailable"
            )
        try:
            size = self.path.stat().st_size
            if not 1 <= size <= MAX_SNAPSHOT_FILE_BYTES:
                raise BinanceSpotExclusivityError(
                    "independent authority snapshot size is invalid"
                )
            raw = self.path.read_bytes()
            if len(raw) != size:
                raise BinanceSpotExclusivityError(
                    "independent authority snapshot changed while reading"
                )
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BinanceSpotExclusivityError(
                "independent authority snapshot is unreadable"
            ) from exc
        if not isinstance(value, dict):
            raise BinanceSpotExclusivityError(
                "independent authority snapshot body is malformed"
            )
        return dict(value)

    def status(self) -> dict[str, Any]:
        return {
            "configured": True,
            "absolutePath": True,
            "snapshotPresent": self.path.is_file() and not self.path.is_symlink(),
            "networkMutationCapability": False,
            "privateKeyPresentInTrader": False,
        }


__all__ = [
    "MAX_SNAPSHOT_AGE_SECONDS",
    "MAX_SNAPSHOT_FILE_BYTES",
    "PROCESS_AUDIT_SCHEMA_VERSION",
    "BinanceSpotSupervisedAuthoritySnapshotReader",
    "PinnedBinanceSpotSupervisedAuthorityVerifier",
    "SNAPSHOT_SCHEMA_VERSION",
    "STREAM_AUDIT_SCHEMA_VERSION",
    "authority_hash",
    "canonical_authority_message",
]
