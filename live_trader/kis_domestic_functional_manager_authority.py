from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    from Crypto.PublicKey import ECC
    from Crypto.Signature import eddsa
except ImportError:  # pragma: no cover - reported as an explicit blocker.
    ECC = None
    eddsa = None

from .kis_domestic_functional_contract import PDNO, ROUTE


KIS_DOMESTIC_FUNCTIONAL_MANAGER_AUTHORITY_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MANAGER_AUTHORITY_PROVISIONING_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MANAGER_AUTHORITY_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MANAGER_AUTHORITY_RELEASE_AVAILABLE = False

MANIFEST_SCHEMA = "kis-domestic-functional-manager-authority-manifest/v1"
STATUS_SCHEMA = "kis-domestic-functional-manager-authority-status/v1"
MANAGER_KEY_PURPOSE = "MANAGER_RECEIPT_VERIFY"
KEY_ALGORITHM = "ED25519"
ROOT_SIGNATURE_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_MANAGER_AUTHORITY_ROOT\0"
BINDING_SIGNATURE_DOMAIN = b"KIS_STATE_MANAGER_BINDING\n"
RECEIPT_SIGNATURE_DOMAIN = b"KIS_MANAGER_RECEIPT\n"
DETAILED_RECEIPT_SIGNATURE_DOMAIN = b"KIS_FUNCTIONAL_MANAGER_RECEIPT\n"

_DOMAINS = (
    "KIS_STATE_MANAGER_BINDING",
    "KIS_MANAGER_RECEIPT",
    "KIS_FUNCTIONAL_MANAGER_RECEIPT",
)
_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_ENVELOPE_KEYS = {"body", "manifestHash", "rootKeyIdHash", "rootSignature"}
_MANIFEST_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "registryId",
    "registryEpoch",
    "previousManifestHash",
    "accountFingerprint",
    "credentialConfigurationHash",
    "codeManifestHash",
    "rootKeyIdHash",
    "managerKey",
    "issuedAt",
    "issuedMonotonicNs",
    "productionProvisioned",
}
_MANAGER_KEY_KEYS = {
    "purpose",
    "algorithm",
    "keyIdHash",
    "publicKeyPem",
    "rotationEpoch",
    "notBefore",
    "notAfter",
    "signatureDomains",
}


class KisDomesticFunctionalManagerAuthorityBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            "manager-authority-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    return value


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    return value


def _iso(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            "manager-authority-time-invalid"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            f"{label}-invalid"
        ) from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    parsed = parsed.astimezone(timezone.utc)
    if _iso(parsed) != value:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            f"{label}-not-canonical"
        )
    return parsed


def _now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            "manager-authority-trusted-clock-invalid"
        )
    return value.astimezone(timezone.utc)


def _decode_signature(value: Any, label: str) -> bytes:
    if type(value) is not str:
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    try:
        raw = base64.b64decode(value, validate=True)
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            f"{label}-invalid"
        ) from exc
    if len(raw) != 64:
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    return raw


def _public_key(value: Any, expected_hash: str, label: str):
    if ECC is None or eddsa is None:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            "manager-authority-asymmetric-runtime-unavailable"
        )
    if type(value) is not str or "PRIVATE KEY" in value:
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    try:
        key = ECC.import_key(value)
    except (ValueError, TypeError, IndexError) as exc:
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            f"{label}-invalid"
        ) from exc
    if key.has_private() or getattr(key, "curve", None) != "Ed25519":
        raise KisDomesticFunctionalManagerAuthorityBlocked(f"{label}-invalid")
    exported = key.export_key(format="PEM")
    actual = hashlib.sha256(exported.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(actual, _sha(expected_hash, label + "-id")):
        raise KisDomesticFunctionalManagerAuthorityBlocked(
            f"{label}-id-mismatch"
        )
    return key


def _verify(key: Any, message: bytes, signature: Any) -> bool:
    try:
        eddsa.new(key, mode="rfc8032").verify(
            message,
            _decode_signature(signature, "manager-authority-signature"),
        )
        return True
    except (ValueError, TypeError, KisDomesticFunctionalManagerAuthorityBlocked):
        return False


@dataclass(frozen=True, slots=True)
class ManagerAuthorityPins:
    registry_id: str
    registry_epoch: int
    manifest_file_hash: str
    root_public_key_pem: str
    root_key_id_hash: str
    manager_key_id_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    code_manifest_hash: str

    def validated(self) -> "ManagerAuthorityPins":
        _identifier(self.registry_id, "manager-authority-pin-registry-id")
        _integer(
            self.registry_epoch,
            "manager-authority-pin-registry-epoch",
            minimum=1,
        )
        _sha(self.manifest_file_hash, "manager-authority-pin-manifest-file")
        _public_key(
            self.root_public_key_pem,
            self.root_key_id_hash,
            "manager-authority-pin-root-key",
        )
        _sha(self.manager_key_id_hash, "manager-authority-pin-manager-key")
        _sha(self.account_fingerprint, "manager-authority-pin-account")
        _sha(
            self.credential_configuration_hash,
            "manager-authority-pin-credential",
        )
        _sha(self.code_manifest_hash, "manager-authority-pin-code-manifest")
        return self


class VerifyOnlyKisDomesticFunctionalManagerAuthority:
    """Root-pinned Ed25519 verifier for the manager-only signing purpose.

    The manifest and every receipt are re-read/re-verified at the boundary.
    This object never accepts or retains a private key and has no signing API.
    """

    def __init__(
        self,
        manifest_path: str | Path,
        *,
        pins: ManagerAuthorityPins,
        trusted_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(manifest_path).expanduser().resolve()
        self.pins = pins.validated()
        self._trusted_clock = trusted_clock or (
            lambda: datetime.now(timezone.utc)
        )
        self._root_key = _public_key(
            self.pins.root_public_key_pem,
            self.pins.root_key_id_hash,
            "manager-authority-root-key",
        )
        self._load_verified_manifest()

    def _stable_bytes(self) -> bytes:
        try:
            first = self.path.read_bytes()
            second = self.path.read_bytes()
        except OSError as exc:
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-unreadable"
            ) from exc
        if not first or first != second:
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-unstable"
            )
        if not hmac.compare_digest(
            hashlib.sha256(first).hexdigest(), self.pins.manifest_file_hash
        ):
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-file-hash-mismatch"
            )
        return first

    def _load_verified_manifest(self) -> tuple[dict[str, Any], Any]:
        raw = self._stable_bytes()
        try:
            envelope = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-json-invalid"
            ) from exc
        if (
            not isinstance(envelope, Mapping)
            or set(envelope) != _ENVELOPE_KEYS
            or _canonical(envelope) != raw
        ):
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-envelope-not-exact"
            )
        body = envelope.get("body")
        if not isinstance(body, Mapping) or set(body) != _MANIFEST_KEYS:
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-body-not-exact"
            )
        body = dict(body)
        key_body = body.get("managerKey")
        if not isinstance(key_body, Mapping) or set(key_body) != _MANAGER_KEY_KEYS:
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-key-certificate-not-exact"
            )
        key_body = dict(key_body)
        if any(
            marker in key.lower()
            for key in key_body
            for marker in ("private", "secret", "seed")
        ):
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-private-material-forbidden"
            )
        issued = _time(body.get("issuedAt"), "manager-authority-issued-at")
        not_before = _time(
            key_body.get("notBefore"), "manager-authority-key-not-before"
        )
        not_after = _time(
            key_body.get("notAfter"), "manager-authority-key-not-after"
        )
        now = _now(self._trusted_clock)
        expected = {
            "schemaVersion": MANIFEST_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": self.pins.registry_id,
            "registryEpoch": self.pins.registry_epoch,
            "accountFingerprint": self.pins.account_fingerprint,
            "credentialConfigurationHash": (
                self.pins.credential_configuration_hash
            ),
            "codeManifestHash": self.pins.code_manifest_hash,
            "rootKeyIdHash": self.pins.root_key_id_hash,
            "productionProvisioned": False,
        }
        key_expected = {
            "purpose": MANAGER_KEY_PURPOSE,
            "algorithm": KEY_ALGORITHM,
            "keyIdHash": self.pins.manager_key_id_hash,
            "signatureDomains": list(_DOMAINS),
        }
        if (
            any(
                type(body.get(name)) is not type(value)
                or body.get(name) != value
                for name, value in expected.items()
            )
            or any(
                type(key_body.get(name)) is not type(value)
                or key_body.get(name) != value
                for name, value in key_expected.items()
            )
            or _sha(
                body.get("previousManifestHash"),
                "manager-authority-previous-manifest",
            )
            != body["previousManifestHash"]
            or _integer(
                body.get("issuedMonotonicNs"),
                "manager-authority-issued-monotonic",
            )
            != body["issuedMonotonicNs"]
            or _integer(
                key_body.get("rotationEpoch"),
                "manager-authority-key-rotation-epoch",
                minimum=1,
            )
            != self.pins.registry_epoch
            or not_before > issued
            or issued >= not_after
            or issued > now
            or now < not_before
            or now >= not_after
        ):
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-manifest-binding-or-validity-invalid"
            )
        manager_key = _public_key(
            key_body.get("publicKeyPem"),
            self.pins.manager_key_id_hash,
            "manager-authority-manager-key",
        )
        manifest_hash = _sha(
            envelope.get("manifestHash"), "manager-authority-manifest-hash"
        )
        root_id = _sha(
            envelope.get("rootKeyIdHash"), "manager-authority-root-key-id"
        )
        if (
            not hmac.compare_digest(manifest_hash, _hash(body))
            or not hmac.compare_digest(root_id, self.pins.root_key_id_hash)
            or not _verify(
                self._root_key,
                ROOT_SIGNATURE_DOMAIN + _canonical(body),
                envelope.get("rootSignature"),
            )
        ):
            raise KisDomesticFunctionalManagerAuthorityBlocked(
                "manager-authority-root-signature-invalid"
            )
        return {**body, "managerKey": key_body, "manifestHash": manifest_hash}, manager_key

    def _verify_candidate(
        self,
        candidate: Mapping[str, Any],
        *,
        hash_field: str,
        key_field: str,
        domain: bytes,
        require_pdno: bool,
    ) -> bool:
        try:
            if not isinstance(candidate, Mapping):
                return False
            raw = dict(candidate)
            if set((hash_field, "signature")) - set(raw):
                return False
            signature = raw.pop("signature")
            claimed_hash = raw.pop(hash_field)
            if (
                raw.get("schemaVersion")
                != "kis-domestic-functional-manager-receipt/v2"
                and domain != BINDING_SIGNATURE_DOMAIN
            ):
                return False
            if domain == BINDING_SIGNATURE_DOMAIN and raw.get(
                "schemaVersion"
            ) != "kis-domestic-functional-state-manager-binding/v1":
                return False
            if (
                raw.get("route") != ROUTE
                or (require_pdno and raw.get("pdno") != PDNO)
                or raw.get("productionAvailable") is not False
                or raw.get(key_field) != self.pins.manager_key_id_hash
                or not hmac.compare_digest(
                    _sha(claimed_hash, "manager-authority-candidate-hash"),
                    _hash(raw),
                )
            ):
                return False
            _, manager_key = self._load_verified_manifest()
            return _verify(
                manager_key,
                domain + claimed_hash.encode("ascii"),
                signature,
            )
        except Exception:
            return False

    def verify_binding(self, candidate: Mapping[str, Any]) -> bool:
        return self._verify_candidate(
            candidate,
            hash_field="bindingHash",
            key_field="managerKeyIdHash",
            domain=BINDING_SIGNATURE_DOMAIN,
            require_pdno=True,
        )

    def verify_receipt(self, candidate: Mapping[str, Any]) -> bool:
        return self._verify_candidate(
            candidate,
            hash_field="receiptHash",
            key_field="keyIdHash",
            domain=RECEIPT_SIGNATURE_DOMAIN,
            require_pdno=False,
        )

    def verify_pending_reservation_proof(
        self, candidate: Mapping[str, Any]
    ) -> bool:
        return self.verify_receipt(candidate)

    def verify_detailed_receipt(self, candidate: Mapping[str, Any]) -> bool:
        return self._verify_candidate(
            candidate,
            hash_field="receiptHash",
            key_field="signerKeyIdHash",
            domain=DETAILED_RECEIPT_SIGNATURE_DOMAIN,
            require_pdno=True,
        )

    def status(self) -> dict[str, Any]:
        manifest, _ = self._load_verified_manifest()
        body = {
            "schemaVersion": STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": self.pins.registry_id,
            "registryEpoch": self.pins.registry_epoch,
            "manifestHash": manifest["manifestHash"],
            "manifestFileHash": self.pins.manifest_file_hash,
            "rootKeyIdHash": self.pins.root_key_id_hash,
            "managerKeyIdHash": self.pins.manager_key_id_hash,
            "managerKeyPurpose": MANAGER_KEY_PURPOSE,
            "signatureDomains": list(_DOMAINS),
            "rootSignatureVerified": True,
            "dedicatedManagerPurposeVerified": True,
            "verifyOnlyConsumer": True,
            "privateSignerPresent": False,
            "consumerSigningSurface": False,
            "productionProvisioningAvailable": False,
            "integrationAccepted": False,
            "readinessBlockers": [
                "PRODUCTION_MANAGER_AUTHORITY_MANIFEST_NOT_PROVISIONED",
                "MANAGER_ED25519_SIGNER_NOT_WIRED",
                "STATE_FACTORY_MANAGER_AUTHORITY_NOT_INTEGRATED",
            ],
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "statusHash": _hash(body)}


def production_entrypoint_status() -> dict[str, Any]:
    body = {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "dedicatedManagerPurposeImplemented": True,
        "verifyOnlyConsumerImplemented": True,
        "productionProvisioningAvailable": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}
