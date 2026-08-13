from __future__ import annotations

import base64
import ctypes
import hashlib
import hmac
import json
import math
import os
import re
import tempfile
import threading
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .kis_domestic_functional_contract import PDNO, ROUTE
from .process_safety import CrossProcessLease, acquire_process_lease


KIS_DOMESTIC_FUNCTIONAL_HIGH_WATER_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HIGH_WATER_WRITER_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HIGH_WATER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_HIGH_WATER_RELEASE_AVAILABLE = False

INSTALLATION_SCHEMA = "kis-domestic-functional-external-high-water-installation/v1"
TRANSITION_SCHEMA = "kis-domestic-functional-external-high-water-transition/v1"
MAIN_PROJECTION_SCHEMA = "kis-domestic-functional-external-high-water-main-projection/v1"
STATUS_SCHEMA = "kis-domestic-functional-external-high-water-status/v1"
ROOT_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_EXTERNAL_HIGH_WATER_ROOT\0"
WRITER_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_EXTERNAL_HIGH_WATER_WRITER\0"
ZERO_HASH = "0" * 64

_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_HEADER_KEYS = {
    "schemaVersion", "route", "pdno", "anchorId", "anchorPathHash",
    "epoch", "everIssued", "issuanceBindingHash", "previousHeadHash",
    "ownerEpoch", "ownerRecordHash", "registryId", "registryEpoch",
    "registryAcceptedHeadHash", "rootKeyIdHash", "writerKeyIdHash",
    "writerPublicKeyPem", "writerPurpose", "writerNotBefore",
    "writerNotAfter", "createdAt", "createdMonotonicNs",
    "productionProvisioned",
}
_TRANSITION_KEYS = {
    "schemaVersion", "route", "pdno", "anchorId", "anchorPathHash",
    "epoch", "everIssued", "issuanceBindingHash", "previousHeadHash",
    "ownerEpoch", "ownerRecordHash", "registryId", "registryEpoch",
    "registryAcceptedHeadHash", "writerKeyIdHash", "occurredAt",
    "occurredMonotonicNs",
}
_ENVELOPE_KEYS = {"body", "recordHash", "signature", "keyIdHash"}
_MAIN_KEYS = {
    "schemaVersion", "route", "pdno", "anchorId", "anchorEpoch",
    "anchorHeadHash", "everIssued", "issuanceBindingHash", "ownerEpoch",
    "ownerRecordHash", "registryAcceptedHeadHash",
}
_LOCAL_LEASES_LOCK = threading.RLock()
_LOCAL_LEASE_SCOPES: set[str] = set()


class KisDomesticFunctionalHighWaterBlocked(RuntimeError):
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
        raise KisDomesticFunctionalHighWaterBlocked(
            "high-water-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    return value


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    return value


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid") from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    result = parsed.astimezone(timezone.utc)
    if _iso(result) != value:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-not-canonical")
    return result


def _iso(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalHighWaterBlocked("high-water-time-invalid")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _path_hash(path: Path) -> str:
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-high-water-path/v1",
            "absolutePath": str(path.resolve()),
        }
    )


def _decode_signature(value: Any, label: str) -> bytes:
    if type(value) is not str:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    try:
        result = base64.b64decode(value, validate=True)
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid") from exc
    if len(result) != 64:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    return result


def _public_key(value: Any, expected_key_id_hash: str, label: str) -> ECC.EccKey:
    if type(value) is not str or "PRIVATE KEY" in value:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    try:
        key = ECC.import_key(value)
    except (ValueError, TypeError, IndexError) as exc:
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid") from exc
    if key.has_private() or getattr(key, "curve", None) != "Ed25519":
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-invalid")
    exported = key.export_key(format="PEM")
    actual = hashlib.sha256(exported.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(actual, _sha(expected_key_id_hash, label + "-id")):
        raise KisDomesticFunctionalHighWaterBlocked(f"{label}-id-mismatch")
    return key


def _verify_signature(
    key: ECC.EccKey,
    domain: bytes,
    body: Mapping[str, Any],
    signature: Any,
) -> bool:
    try:
        eddsa.new(key, mode="rfc8032").verify(
            domain + _canonical(body),
            _decode_signature(signature, "high-water-signature"),
        )
        return True
    except (ValueError, TypeError, KisDomesticFunctionalHighWaterBlocked):
        return False


def _fsync_file_descriptor(handle: int) -> None:
    os.fsync(handle)


def _fsync_directory(path: Path) -> str:
    if os.name != "nt":
        handle = os.open(path, os.O_RDONLY)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        return "POSIX_DIRECTORY_FSYNC"
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.FlushFileBuffers.argtypes = [wintypes.HANDLE]
    kernel32.FlushFileBuffers.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.CreateFileW(
        str(path),
        0x40000000,
        0x00000007,
        None,
        3,
        0x02000000,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle in (None, invalid):
        raise OSError(ctypes.get_last_error(), "high-water-directory-open-failed")
    try:
        if not kernel32.FlushFileBuffers(handle):
            raise OSError(
                ctypes.get_last_error(), "high-water-directory-flush-failed"
            )
    finally:
        kernel32.CloseHandle(handle)
    return "WINDOWS_DIRECTORY_FLUSH_FILE_BUFFERS"


@dataclass(frozen=True, slots=True)
class ExternalHighWaterPins:
    anchor_id: str
    owner_epoch: int
    owner_record_hash: str
    registry_id: str
    registry_epoch: int
    registry_accepted_head_hash: str
    root_public_key_pem: str
    root_key_id_hash: str
    writer_key_id_hash: str
    minimum_epoch: int
    minimum_head_hash: str

    def validated(self) -> "ExternalHighWaterPins":
        _identifier(self.anchor_id, "high-water-pin-anchor-id")
        _integer(self.owner_epoch, "high-water-pin-owner-epoch", minimum=1)
        _sha(self.owner_record_hash, "high-water-pin-owner-record")
        _identifier(self.registry_id, "high-water-pin-registry-id")
        _integer(self.registry_epoch, "high-water-pin-registry-epoch", minimum=1)
        _sha(
            self.registry_accepted_head_hash,
            "high-water-pin-registry-accepted-head",
        )
        _public_key(
            self.root_public_key_pem,
            self.root_key_id_hash,
            "high-water-pin-root-key",
        )
        _sha(self.writer_key_id_hash, "high-water-pin-writer-key")
        if self.minimum_epoch not in (0, 1):
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-pin-minimum-epoch-invalid"
            )
        _sha(self.minimum_head_hash, "high-water-pin-minimum-head")
        return self


class AppendOnlyKisBootstrapHighWater:
    """Verify-only, one-way route-burn anchor guarded by a kernel lease.

    The consumer owns no signing key.  Initial and burn records must be signed
    outside this object.  The on-disk log is canonical JSONL: any truncation,
    rewrite, unknown line, rollback below the independently supplied minimum
    pin, or missing file blocks all issuance.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        pins: ExternalHighWaterPins,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.pins = pins.validated()
        self._root_key = _public_key(
            pins.root_public_key_pem,
            pins.root_key_id_hash,
            "high-water-root-key",
        )
        self._failure_injector = failure_injector
        self._thread_lock = threading.RLock()
        self._closed = False
        if not self.path.is_file():
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-anchor-missing-hold"
            )
        lease_scope = (
            "live-trader:kis-domestic-functional-external-high-water:v1:"
            + _path_hash(self.path)
            + ":"
            + self.pins.anchor_id
        )
        self._lease_scope = lease_scope
        with _LOCAL_LEASES_LOCK:
            if lease_scope in _LOCAL_LEASE_SCOPES:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-os-lease-unavailable"
                )
            lease = acquire_process_lease(lease_scope)
            if type(lease) is not CrossProcessLease:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-os-lease-unavailable"
                )
            _LOCAL_LEASE_SCOPES.add(lease_scope)
        self._lease = lease
        try:
            self.read()
        except BaseException:
            self.close()
            raise

    @classmethod
    def provision_disabled(
        cls,
        path: str | Path,
        *,
        pins: ExternalHighWaterPins,
        root_signed_installation: Mapping[str, Any],
        failure_injector: Callable[[str], None] | None = None,
    ) -> "AppendOnlyKisBootstrapHighWater":
        """Create an offline-only anchor without exposing any signer."""

        target = Path(path).expanduser().resolve()
        if target.exists():
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-provision-target-exists"
            )
        validated_pins = pins.validated()
        cls._verify_header_static(
            target,
            validated_pins,
            root_signed_installation,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = _canonical(root_signed_installation) + b"\n"
        temporary: Path | None = None
        try:
            handle, name = tempfile.mkstemp(
                prefix=target.name + ".pending-",
                suffix=".tmp",
                dir=target.parent,
            )
            temporary = Path(name)
            try:
                written = os.write(handle, payload)
                if written != len(payload):
                    raise OSError("high-water-provision-short-write")
                _fsync_file_descriptor(handle)
            finally:
                os.close(handle)
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-provision-race-lost"
                ) from exc
            _fsync_directory(target.parent)
        except KisDomesticFunctionalHighWaterBlocked:
            raise
        except OSError as exc:
            raise KisDomesticFunctionalHighWaterBlocked(
                f"high-water-provision-failed:{type(exc).__name__}"
            ) from exc
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                    if target.parent.exists():
                        _fsync_directory(target.parent)
                except OSError:
                    # The target is already immutable authority.  A stale
                    # temporary file is diagnostic and grants no authority.
                    pass
        return cls(target, pins=validated_pins, failure_injector=failure_injector)

    @staticmethod
    def _verify_header_static(
        path: Path,
        pins: ExternalHighWaterPins,
        envelope: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, ECC.EccKey]:
        if not isinstance(envelope, Mapping) or set(envelope) != _ENVELOPE_KEYS:
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-header-envelope-not-exact"
            )
        body = envelope.get("body")
        if not isinstance(body, Mapping) or set(body) != _HEADER_KEYS:
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-header-body-not-exact"
            )
        body = dict(body)
        created = _time(body.get("createdAt"), "high-water-header-created-at")
        writer_before = _time(
            body.get("writerNotBefore"), "high-water-writer-not-before"
        )
        writer_after = _time(
            body.get("writerNotAfter"), "high-water-writer-not-after"
        )
        expected = {
            "schemaVersion": INSTALLATION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": pins.anchor_id,
            "anchorPathHash": _path_hash(path),
            "epoch": 0,
            "everIssued": False,
            "issuanceBindingHash": None,
            "previousHeadHash": ZERO_HASH,
            "ownerEpoch": pins.owner_epoch,
            "ownerRecordHash": pins.owner_record_hash,
            "registryId": pins.registry_id,
            "registryEpoch": pins.registry_epoch,
            "registryAcceptedHeadHash": pins.registry_accepted_head_hash,
            "rootKeyIdHash": pins.root_key_id_hash,
            "writerKeyIdHash": pins.writer_key_id_hash,
            "writerPurpose": "KIS_BOOTSTRAP_HIGH_WATER_APPEND_ONLY",
            "productionProvisioned": False,
        }
        if (
            any(type(body.get(key)) is not type(value) or body.get(key) != value
                for key, value in expected.items())
            or _integer(
                body.get("createdMonotonicNs"),
                "high-water-header-created-monotonic",
            ) != body["createdMonotonicNs"]
            or writer_before > created
            or created >= writer_after
        ):
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-header-binding-invalid"
            )
        writer_key = _public_key(
            body.get("writerPublicKeyPem"),
            pins.writer_key_id_hash,
            "high-water-writer-key",
        )
        record_hash = _sha(
            envelope.get("recordHash"), "high-water-header-record-hash"
        )
        key_id = _sha(envelope.get("keyIdHash"), "high-water-header-key-id")
        root_key = _public_key(
            pins.root_public_key_pem,
            pins.root_key_id_hash,
            "high-water-header-root-key",
        )
        if (
            record_hash != _hash(body)
            or key_id != pins.root_key_id_hash
            or not _verify_signature(
                root_key, ROOT_DOMAIN, body, envelope.get("signature")
            )
        ):
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-header-root-signature-invalid"
            )
        return body, _hash(dict(envelope)), writer_key

    def close(self) -> None:
        with self._thread_lock:
            if not self._closed:
                try:
                    self._lease.release()
                finally:
                    with _LOCAL_LEASES_LOCK:
                        _LOCAL_LEASE_SCOPES.discard(self._lease_scope)
                    self._closed = True

    def _assert_lease(self) -> None:
        if self._closed:
            raise KisDomesticFunctionalHighWaterBlocked("high-water-closed")
        status = self._lease.status(reused=True)
        if status.get("acquired") is not True or status.get("reused") is not True:
            raise KisDomesticFunctionalHighWaterBlocked("high-water-os-lease-lost")

    def _read_lines(self) -> list[dict[str, Any]]:
        self._assert_lease()
        try:
            raw = self.path.read_bytes()
        except OSError as exc:
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-anchor-unreadable-hold"
            ) from exc
        if not raw or not raw.endswith(b"\n") or b"\r" in raw:
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-anchor-truncated-or-noncanonical"
            )
        raw_lines = raw.splitlines(keepends=True)
        if len(raw_lines) not in (1, 2):
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-anchor-cardinality-invalid"
            )
        result: list[dict[str, Any]] = []
        for line in raw_lines:
            try:
                value = json.loads(line[:-1].decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-anchor-json-invalid"
                ) from exc
            if not isinstance(value, dict) or _canonical(value) + b"\n" != line:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-anchor-noncanonical"
                )
            result.append(value)
        return result

    def _verify_chain(self) -> dict[str, Any]:
        lines = self._read_lines()
        header, header_head, writer_key = self._verify_header_static(
            self.path, self.pins, lines[0]
        )
        records: list[tuple[int, str]] = [(0, header_head)]
        transition: dict[str, Any] | None = None
        current_head = header_head
        if len(lines) == 2:
            envelope = lines[1]
            if set(envelope) != _ENVELOPE_KEYS:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-envelope-not-exact"
                )
            candidate = envelope.get("body")
            if not isinstance(candidate, Mapping) or set(candidate) != _TRANSITION_KEYS:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-body-not-exact"
                )
            transition = dict(candidate)
            occurred = _time(
                transition.get("occurredAt"), "high-water-transition-occurred-at"
            )
            created = _time(header["createdAt"], "high-water-header-created-at")
            writer_before = _time(
                header["writerNotBefore"], "high-water-writer-not-before"
            )
            writer_after = _time(
                header["writerNotAfter"], "high-water-writer-not-after"
            )
            expected = {
                "schemaVersion": TRANSITION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "epoch": 1,
                "everIssued": True,
                "previousHeadHash": header_head,
                "ownerEpoch": self.pins.owner_epoch,
                "ownerRecordHash": self.pins.owner_record_hash,
                "registryId": self.pins.registry_id,
                "registryEpoch": self.pins.registry_epoch,
                "registryAcceptedHeadHash": self.pins.registry_accepted_head_hash,
                "writerKeyIdHash": self.pins.writer_key_id_hash,
            }
            if (
                any(
                    type(transition.get(key)) is not type(value)
                    or transition.get(key) != value
                    for key, value in expected.items()
                )
                or _sha(
                    transition.get("issuanceBindingHash"),
                    "high-water-transition-issuance-binding",
                ) != transition["issuanceBindingHash"]
                or _integer(
                    transition.get("occurredMonotonicNs"),
                    "high-water-transition-monotonic",
                ) < header["createdMonotonicNs"]
                or occurred < created
                or occurred < writer_before
                or occurred >= writer_after
            ):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-binding-invalid"
                )
            if (
                _sha(
                    envelope.get("recordHash"),
                    "high-water-transition-record-hash",
                ) != _hash(transition)
                or _sha(
                    envelope.get("keyIdHash"),
                    "high-water-transition-key-id",
                ) != self.pins.writer_key_id_hash
                or not _verify_signature(
                    writer_key,
                    WRITER_DOMAIN,
                    transition,
                    envelope.get("signature"),
                )
            ):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-signature-invalid"
                )
            current_head = _hash(envelope)
            records.append((1, current_head))
        record_at_minimum = dict(records).get(self.pins.minimum_epoch)
        if (
            record_at_minimum is None
            or not hmac.compare_digest(
                record_at_minimum, self.pins.minimum_head_hash
            )
        ):
            raise KisDomesticFunctionalHighWaterBlocked(
                "high-water-anchor-rollback-or-substitution-hold"
            )
        return {
            "header": header,
            "transition": transition,
            "epoch": len(lines) - 1,
            "headHash": current_head,
            "everIssued": transition is not None,
            "issuanceBindingHash": (
                None if transition is None else transition["issuanceBindingHash"]
            ),
        }

    def read(self) -> dict[str, Any]:
        with self._thread_lock:
            chain = self._verify_chain()
            body = {
                "schemaVersion": STATUS_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "epoch": chain["epoch"],
                "headHash": chain["headHash"],
                "everIssued": chain["everIssued"],
                "issuanceBindingHash": chain["issuanceBindingHash"],
                "ownerEpoch": self.pins.owner_epoch,
                "ownerRecordHash": self.pins.owner_record_hash,
                "registryId": self.pins.registry_id,
                "registryEpoch": self.pins.registry_epoch,
                "registryAcceptedHeadHash": self.pins.registry_accepted_head_hash,
                "rootKeyIdHash": self.pins.root_key_id_hash,
                "writerKeyIdHash": self.pins.writer_key_id_hash,
                "minimumEpochVerified": self.pins.minimum_epoch,
                "minimumHeadHashVerified": self.pins.minimum_head_hash,
                "rootRegistrySignatureVerified": True,
                "writerCertificateRootVerified": True,
                "appendOnlyChainVerified": True,
                "minimumRollbackPinSuppliedAndVerified": True,
                "minimumRollbackPinExternallyPersisted": False,
                "anchorLossFailsClosed": True,
                "powerLossDurabilityIndependentlyProven": False,
                "externalWriterBurnCommitPrecedesLocalAppend": False,
                "osProcessLeaseHeld": True,
                "verifyOnlyConsumer": True,
                "privateSignerPresent": False,
                "readinessBlockers": [
                    "INDEPENDENT_PRODUCTION_WRITER_NOT_PROVISIONED",
                    "PRODUCTION_OS_PROTECTED_PATH_NOT_PROVISIONED",
                    "ROOT_REGISTRY_ACCEPTED_HEAD_READER_NOT_WIRED",
                    "EXTERNAL_MINIMUM_ROLLBACK_PIN_STORE_NOT_WIRED",
                    "EXTERNAL_WRITER_BURN_COMMIT_PRECEDES_LOCAL_APPEND_NOT_WIRED",
                    "MAIN_BOOTSTRAP_RECONCILIATION_WRITER_NOT_WIRED",
                ],
                "productionWriterAvailable": False,
                "productionAvailable": False,
                "networkAvailable": False,
                "releaseAvailable": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }
            if chain["transition"] is not None:
                body["issuedAt"] = chain["transition"]["occurredAt"]
                body["issuedMonotonicNs"] = chain["transition"][
                    "occurredMonotonicNs"
                ]
            else:
                body["issuedAt"] = None
                body["issuedMonotonicNs"] = None
            return {**body, "statusHash": _hash(body)}

    def next_burn_body(
        self,
        *,
        issuance_binding_hash: str,
        occurred_at: datetime,
        occurred_monotonic_ns: int,
    ) -> dict[str, Any]:
        with self._thread_lock:
            chain = self._verify_chain()
            if chain["everIssued"]:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-route-ever-issued-burned"
                )
            occurred_text = _iso(occurred_at)
            occurred = _time(occurred_text, "high-water-next-occurred-at")
            monotonic = _integer(
                occurred_monotonic_ns,
                "high-water-next-occurred-monotonic",
            )
            header = chain["header"]
            if (
                occurred < _time(header["createdAt"], "high-water-header-created-at")
                or occurred < _time(
                    header["writerNotBefore"], "high-water-writer-not-before"
                )
                or occurred >= _time(
                    header["writerNotAfter"], "high-water-writer-not-after"
                )
                or monotonic < header["createdMonotonicNs"]
            ):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-next-clock-outside-certificate"
                )
            return {
                "schemaVersion": TRANSITION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "epoch": 1,
                "everIssued": True,
                "issuanceBindingHash": _sha(
                    issuance_binding_hash, "high-water-next-issuance-binding"
                ),
                "previousHeadHash": chain["headHash"],
                "ownerEpoch": self.pins.owner_epoch,
                "ownerRecordHash": self.pins.owner_record_hash,
                "registryId": self.pins.registry_id,
                "registryEpoch": self.pins.registry_epoch,
                "registryAcceptedHeadHash": self.pins.registry_accepted_head_hash,
                "writerKeyIdHash": self.pins.writer_key_id_hash,
                "occurredAt": occurred_text,
                "occurredMonotonicNs": monotonic,
            }

    def append_signed_burn(
        self,
        envelope: Mapping[str, Any],
    ) -> dict[str, Any]:
        with self._thread_lock:
            before = self._verify_chain()
            if before["everIssued"]:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-route-ever-issued-burned"
                )
            candidate = envelope.get("body") if isinstance(envelope, Mapping) else None
            if not isinstance(candidate, Mapping):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-envelope-not-exact"
                )
            # Reuse the complete independent chain verifier by validating the
            # candidate against an in-memory second line before touching disk.
            expected = self.next_burn_body(
                issuance_binding_hash=candidate.get("issuanceBindingHash"),
                occurred_at=_time(
                    candidate.get("occurredAt"),
                    "high-water-append-occurred-at",
                ),
                occurred_monotonic_ns=candidate.get("occurredMonotonicNs"),
            )
            if dict(candidate) != expected:
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-binding-invalid"
                )
            header = before["header"]
            writer_key = _public_key(
                header["writerPublicKeyPem"],
                self.pins.writer_key_id_hash,
                "high-water-append-writer-key",
            )
            if (
                set(envelope) != _ENVELOPE_KEYS
                or envelope.get("recordHash") != _hash(expected)
                or envelope.get("keyIdHash") != self.pins.writer_key_id_hash
                or not _verify_signature(
                    writer_key,
                    WRITER_DOMAIN,
                    expected,
                    envelope.get("signature"),
                )
            ):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-transition-signature-invalid"
                )
            payload = _canonical(dict(envelope)) + b"\n"
            if self._failure_injector is not None:
                self._failure_injector("BEFORE_APPEND")
            handle = None
            try:
                handle = os.open(
                    self.path,
                    os.O_WRONLY | os.O_APPEND | getattr(os, "O_BINARY", 0),
                )
                written = os.write(handle, payload)
                if written != len(payload):
                    raise OSError("high-water-append-short-write")
                if self._failure_injector is not None:
                    self._failure_injector("AFTER_APPEND_WRITE_BEFORE_FSYNC")
                _fsync_file_descriptor(handle)
                if self._failure_injector is not None:
                    self._failure_injector("AFTER_FILE_FSYNC_BEFORE_PARENT_FSYNC")
            except BaseException:
                raise
            finally:
                if handle is not None:
                    os.close(handle)
            _fsync_directory(self.path.parent)
            after = self._verify_chain()
            if (
                not after["everIssued"]
                or after["issuanceBindingHash"] != expected["issuanceBindingHash"]
            ):
                raise KisDomesticFunctionalHighWaterBlocked(
                    "high-water-post-append-verification-failed"
                )
            self.pins = replace(
                self.pins,
                minimum_epoch=1,
                minimum_head_hash=after["headHash"],
            )
            result = self.read()
            result["restartMinimumEpoch"] = 1
            result["restartMinimumHeadHash"] = after["headHash"]
            result["receiptHash"] = _hash(result)
            return result

    def reconcile_main(
        self,
        main_projection: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            anchor = self._verify_chain()
            main: dict[str, Any] | None = None
            if main_projection is not None:
                if (
                    not isinstance(main_projection, Mapping)
                    or set(main_projection) != _MAIN_KEYS
                ):
                    raise KisDomesticFunctionalHighWaterBlocked(
                        "high-water-main-projection-not-exact"
                    )
                main = dict(main_projection)
                expected = {
                    "schemaVersion": MAIN_PROJECTION_SCHEMA,
                    "route": ROUTE,
                    "pdno": PDNO,
                    "anchorId": self.pins.anchor_id,
                    "ownerEpoch": self.pins.owner_epoch,
                    "ownerRecordHash": self.pins.owner_record_hash,
                    "registryAcceptedHeadHash": self.pins.registry_accepted_head_hash,
                }
                if (
                    any(
                        type(main.get(key)) is not type(value)
                        or main.get(key) != value
                        for key, value in expected.items()
                    )
                    or type(main.get("everIssued")) is not bool
                    or type(main.get("anchorEpoch")) is not int
                    or main["anchorEpoch"] not in (0, 1)
                    or not _SHA.fullmatch(str(main.get("anchorHeadHash") or ""))
                    or (
                        main["everIssued"]
                        and not _SHA.fullmatch(
                            str(main.get("issuanceBindingHash") or "")
                        )
                    )
                    or (
                        not main["everIssued"]
                        and main.get("issuanceBindingHash") is not None
                    )
                ):
                    raise KisDomesticFunctionalHighWaterBlocked(
                        "high-water-main-projection-binding-invalid"
                    )
            if not anchor["everIssued"]:
                if main is not None and (
                    main["everIssued"]
                    or main["anchorEpoch"] != 0
                    or main["anchorHeadHash"] != anchor["headHash"]
                ):
                    raise KisDomesticFunctionalHighWaterBlocked(
                        "high-water-main-ahead-or-substituted"
                    )
                classification = "UNISSUED_EXACT"
                reconciliation_required = False
            elif main is None or not main["everIssued"]:
                if main is not None and (
                    main["anchorEpoch"] != 0
                    or main["anchorHeadHash"] != _hash(
                        self._read_lines()[0]
                    )
                ):
                    raise KisDomesticFunctionalHighWaterBlocked(
                        "high-water-main-stale-projection-invalid"
                    )
                classification = "BURNED_RECONCILIATION_REQUIRED"
                reconciliation_required = True
            else:
                if (
                    main["anchorEpoch"] != anchor["epoch"]
                    or main["anchorHeadHash"] != anchor["headHash"]
                    or main["issuanceBindingHash"]
                    != anchor["issuanceBindingHash"]
                ):
                    raise KisDomesticFunctionalHighWaterBlocked(
                        "high-water-main-burn-mismatch"
                    )
                classification = "BURNED_CONFIRMED"
                reconciliation_required = False
            body = {
                "schemaVersion": "kis-domestic-functional-high-water-reconciliation/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "classification": classification,
                "anchorEpoch": anchor["epoch"],
                "anchorHeadHash": anchor["headHash"],
                "routeEverIssuedBurned": anchor["everIssued"],
                "issuanceBindingHash": anchor["issuanceBindingHash"],
                "mainProjectionPresent": main is not None,
                "mainReconciliationRequired": reconciliation_required,
                "mayIssue": not anchor["everIssued"],
                "productionIssueAuthorityAvailable": False,
            }
            return {**body, "reconciliationHash": _hash(body)}


def high_water_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "verifyOnlyConsumer": True,
        "privateSignerPresent": False,
        "externalAnchorPresent": False,
        "rootRegistrySignatureVerified": False,
        "osProcessLeaseHeld": False,
        "productionWriterAvailable": False,
        "powerLossDurabilityIndependentlyProven": False,
        "externalWriterBurnCommitPrecedesLocalAppend": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
        "readinessBlockers": [
            "EXTERNAL_HIGH_WATER_ANCHOR_NOT_OPEN",
            "INDEPENDENT_PRODUCTION_WRITER_NOT_PROVISIONED",
            "ROOT_REGISTRY_ACCEPTED_HEAD_READER_NOT_WIRED",
            "EXTERNAL_MINIMUM_ROLLBACK_PIN_STORE_NOT_WIRED",
            "EXTERNAL_WRITER_BURN_COMMIT_PRECEDES_LOCAL_APPEND_NOT_WIRED",
            "MAIN_BOOTSTRAP_RECONCILIATION_WRITER_NOT_WIRED",
        ],
    }
    return {**body, "statusHash": _hash(body)}
