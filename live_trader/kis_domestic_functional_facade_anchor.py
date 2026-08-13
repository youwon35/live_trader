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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    from Crypto.PublicKey import ECC
    from Crypto.Signature import eddsa
except ImportError:  # pragma: no cover - explicit status blocker.
    ECC = None
    eddsa = None

from .kis_domestic_functional_contract import PDNO, ROUTE
from .process_safety import CrossProcessLease, acquire_process_lease


KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_PROVISIONING_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_RELEASE_AVAILABLE = False

INSTALLATION_SCHEMA = "kis-domestic-functional-facade-anchor-installation/v1"
TRANSITION_SCHEMA = "kis-domestic-functional-facade-anchor-transition/v1"
PROJECTION_SCHEMA = "kis-domestic-functional-facade-anchor-projection/v1"
STATUS_SCHEMA = "kis-domestic-functional-facade-anchor-status/v1"
WRITER_PURPOSE = "KIS_FACADE_EXTERNAL_MONOTONIC_ANCHOR_APPEND"
ROOT_SIGNATURE_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_ROOT\0"
WRITER_SIGNATURE_DOMAIN = b"KIS_DOMESTIC_FUNCTIONAL_FACADE_ANCHOR_APPEND\0"
ZERO_HASH = "0" * 64

_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_ENVELOPE_KEYS = {"body", "recordHash", "keyIdHash", "signature"}
_HEADER_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "anchorId",
    "anchorPathHash",
    "facadeLedgerIdHash",
    "rootKeyIdHash",
    "writerKeyIdHash",
    "writerPublicKeyPem",
    "writerPurpose",
    "writerNotBefore",
    "writerNotAfter",
    "createdAt",
    "createdMonotonicNs",
    "productionProvisioned",
}
_TRANSITION_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "anchorId",
    "anchorPathHash",
    "facadeLedgerIdHash",
    "anchorEpoch",
    "facadeEpoch",
    "epochSequence",
    "epochHeadHash",
    "snapshotSequence",
    "snapshotHeadHash",
    "burnSequence",
    "burnHeadHash",
    "previousRecordHash",
    "writerKeyIdHash",
    "observedAt",
    "observedMonotonicNs",
    "productionAvailable",
}
_PROJECTION_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "facadeLedgerIdHash",
    "facadeEpoch",
    "epochSequence",
    "epochHeadHash",
    "snapshotSequence",
    "snapshotHeadHash",
    "burnSequence",
    "burnHeadHash",
}
_LOCAL_LEASES_LOCK = threading.RLock()
_LOCAL_LEASE_SCOPES: set[str] = set()


class KisDomesticFunctionalFacadeAnchorBlocked(RuntimeError):
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
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    return value


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    return value


def _iso(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-time-invalid"
        )
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            f"{label}-invalid"
        ) from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    parsed = parsed.astimezone(timezone.utc)
    if _iso(parsed) != value:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            f"{label}-not-canonical"
        )
    return parsed


def _decode_signature(value: Any, label: str) -> bytes:
    if type(value) is not str:
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    try:
        raw = base64.b64decode(value, validate=True)
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            f"{label}-invalid"
        ) from exc
    if len(raw) != 64:
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    return raw


def _public_key(value: Any, expected_hash: str, label: str):
    if ECC is None or eddsa is None:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-asymmetric-runtime-unavailable"
        )
    if type(value) is not str or "PRIVATE KEY" in value:
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    try:
        key = ECC.import_key(value)
    except (ValueError, TypeError, IndexError) as exc:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            f"{label}-invalid"
        ) from exc
    if key.has_private() or getattr(key, "curve", None) != "Ed25519":
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-invalid")
    exported = key.export_key(format="PEM")
    actual = hashlib.sha256(exported.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(actual, _sha(expected_hash, label + "-id")):
        raise KisDomesticFunctionalFacadeAnchorBlocked(f"{label}-id-mismatch")
    return key


def _verify(key: Any, domain: bytes, body: Mapping[str, Any], signature: Any) -> bool:
    try:
        eddsa.new(key, mode="rfc8032").verify(
            domain + _canonical(body),
            _decode_signature(signature, "facade-anchor-signature"),
        )
        return True
    except (ValueError, TypeError, KisDomesticFunctionalFacadeAnchorBlocked):
        return False


def _path_hash(path: Path) -> str:
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-facade-anchor-path/v1",
            "absolutePath": str(path.resolve()),
        }
    )


def _file_identity(path: Path) -> tuple[int, int]:
    try:
        status = path.stat()
    except OSError as exc:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-file-missing"
        ) from exc
    return int(status.st_dev), int(status.st_ino)


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
        raise OSError(ctypes.get_last_error(), "facade-anchor-directory-open-failed")
    try:
        if not kernel32.FlushFileBuffers(handle):
            raise OSError(
                ctypes.get_last_error(), "facade-anchor-directory-flush-failed"
            )
    finally:
        kernel32.CloseHandle(handle)
    return "WINDOWS_DIRECTORY_FLUSH_FILE_BUFFERS"


def _projection(value: Mapping[str, Any], ledger_id: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _PROJECTION_KEYS:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-projection-not-exact"
        )
    result = dict(value)
    expected = {
        "schemaVersion": PROJECTION_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "facadeLedgerIdHash": ledger_id,
    }
    if any(
        type(result.get(key)) is not type(wanted)
        or result.get(key) != wanted
        for key, wanted in expected.items()
    ):
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-projection-binding-invalid"
        )
    _integer(result.get("facadeEpoch"), "facade-anchor-facade-epoch", minimum=1)
    for sequence_key, head_key in (
        ("epochSequence", "epochHeadHash"),
        ("snapshotSequence", "snapshotHeadHash"),
        ("burnSequence", "burnHeadHash"),
    ):
        sequence = _integer(
            result.get(sequence_key), f"facade-anchor-{sequence_key}"
        )
        head = _sha(result.get(head_key), f"facade-anchor-{head_key}")
        if (sequence == 0) is not (head == ZERO_HASH):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-projection-sequence-head-invalid"
            )
    if result["epochSequence"] < result["facadeEpoch"]:
        raise KisDomesticFunctionalFacadeAnchorBlocked(
            "facade-anchor-projection-epoch-lifecycle-invalid"
        )
    return result


@dataclass(frozen=True, slots=True)
class ExternalFacadeAnchorPins:
    anchor_id: str
    facade_ledger_id_hash: str
    root_public_key_pem: str
    root_key_id_hash: str
    writer_key_id_hash: str
    minimum_anchor_epoch: int
    minimum_anchor_head_hash: str

    def validated(self) -> "ExternalFacadeAnchorPins":
        _identifier(self.anchor_id, "facade-anchor-pin-anchor-id")
        _sha(self.facade_ledger_id_hash, "facade-anchor-pin-ledger-id")
        _public_key(
            self.root_public_key_pem,
            self.root_key_id_hash,
            "facade-anchor-pin-root-key",
        )
        _sha(self.writer_key_id_hash, "facade-anchor-pin-writer-key")
        _integer(
            self.minimum_anchor_epoch,
            "facade-anchor-pin-minimum-epoch",
        )
        _sha(
            self.minimum_anchor_head_hash,
            "facade-anchor-pin-minimum-head",
        )
        return self


class AppendOnlyKisDomesticFunctionalFacadeAnchor:
    """Verify-only external monotonic facade projection anchor.

    The file is separate from the facade database/local high-water pair.  A
    kernel lease serializes one process, writes use O_APPEND plus fsync, and
    every transition requires an external Ed25519 signature.  The consumer
    intentionally has no signer.  Hardware/WORM rollback resistance remains a
    production blocker and is reported honestly by ``status``.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        pins: ExternalFacadeAnchorPins,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        self.pins = pins.validated()
        self._root_key = _public_key(
            self.pins.root_public_key_pem,
            self.pins.root_key_id_hash,
            "facade-anchor-root-key",
        )
        self._failure_injector = failure_injector
        self._lock = threading.RLock()
        self._closed = False
        self._observed_anchor_epoch = self.pins.minimum_anchor_epoch
        self._observed_anchor_head_hash = self.pins.minimum_anchor_head_hash
        if not self.path.is_file():
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-missing-hold"
            )
        scope = (
            "live-trader:kis-domestic-functional-facade-anchor:v1:"
            + _path_hash(self.path)
            + ":"
            + self.pins.anchor_id
        )
        self._lease_scope = scope
        with _LOCAL_LEASES_LOCK:
            if scope in _LOCAL_LEASE_SCOPES:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-os-lease-unavailable"
                )
            lease = acquire_process_lease(scope)
            if type(lease) is not CrossProcessLease:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-os-lease-unavailable"
                )
            _LOCAL_LEASE_SCOPES.add(scope)
        self._lease = lease
        self._identity = _file_identity(self.path)
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
        pins: ExternalFacadeAnchorPins,
        root_signed_installation: Mapping[str, Any],
        failure_injector: Callable[[str], None] | None = None,
    ) -> "AppendOnlyKisDomesticFunctionalFacadeAnchor":
        target = Path(path).expanduser().resolve()
        if target.exists():
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-provision-target-exists"
            )
        validated = pins.validated()
        cls._verify_header_static(target, validated, root_signed_installation)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = _canonical(root_signed_installation) + b"\n"
        temporary: Path | None = None
        try:
            descriptor, name = tempfile.mkstemp(
                prefix=target.name + ".pending-",
                suffix=".tmp",
                dir=target.parent,
            )
            temporary = Path(name)
            try:
                written = os.write(descriptor, payload)
                if written != len(payload):
                    raise OSError("facade-anchor-provision-short-write")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            try:
                os.link(temporary, target)
            except FileExistsError as exc:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-provision-race-lost"
                ) from exc
            _fsync_directory(target.parent)
        except KisDomesticFunctionalFacadeAnchorBlocked:
            raise
        except OSError as exc:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                f"facade-anchor-provision-failed:{type(exc).__name__}"
            ) from exc
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
        return cls(target, pins=validated, failure_injector=failure_injector)

    @staticmethod
    def _verify_header_static(
        path: Path,
        pins: ExternalFacadeAnchorPins,
        envelope: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str, Any]:
        if not isinstance(envelope, Mapping) or set(envelope) != _ENVELOPE_KEYS:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-header-envelope-not-exact"
            )
        body = envelope.get("body")
        if not isinstance(body, Mapping) or set(body) != _HEADER_KEYS:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-header-body-not-exact"
            )
        body = dict(body)
        created = _time(body.get("createdAt"), "facade-anchor-created-at")
        not_before = _time(
            body.get("writerNotBefore"), "facade-anchor-writer-not-before"
        )
        not_after = _time(
            body.get("writerNotAfter"), "facade-anchor-writer-not-after"
        )
        expected = {
            "schemaVersion": INSTALLATION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": pins.anchor_id,
            "anchorPathHash": _path_hash(path),
            "facadeLedgerIdHash": pins.facade_ledger_id_hash,
            "rootKeyIdHash": pins.root_key_id_hash,
            "writerKeyIdHash": pins.writer_key_id_hash,
            "writerPurpose": WRITER_PURPOSE,
            "productionProvisioned": False,
        }
        if (
            any(
                type(body.get(name)) is not type(value)
                or body.get(name) != value
                for name, value in expected.items()
            )
            or _integer(
                body.get("createdMonotonicNs"),
                "facade-anchor-created-monotonic",
            )
            != body["createdMonotonicNs"]
            or not_before > created
            or created >= not_after
        ):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-header-binding-invalid"
            )
        writer_key = _public_key(
            body.get("writerPublicKeyPem"),
            pins.writer_key_id_hash,
            "facade-anchor-writer-key",
        )
        if (
            _sha(envelope.get("recordHash"), "facade-anchor-header-hash")
            != _hash(body)
            or _sha(envelope.get("keyIdHash"), "facade-anchor-header-key-id")
            != pins.root_key_id_hash
            or not _verify(
                _public_key(
                    pins.root_public_key_pem,
                    pins.root_key_id_hash,
                    "facade-anchor-header-root-key",
                ),
                ROOT_SIGNATURE_DOMAIN,
                body,
                envelope.get("signature"),
            )
        ):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-header-root-signature-invalid"
            )
        return body, _hash(dict(envelope)), writer_key

    def _assert_lease_and_identity(self) -> None:
        if self._closed:
            raise KisDomesticFunctionalFacadeAnchorBlocked("facade-anchor-closed")
        status = self._lease.status(reused=True)
        if status.get("acquired") is not True or status.get("reused") is not True:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-os-lease-lost"
            )
        if _file_identity(self.path) != self._identity:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-file-identity-replaced"
            )

    def _read_lines(self) -> list[dict[str, Any]]:
        self._assert_lease_and_identity()
        try:
            raw = self.path.read_bytes()
        except OSError as exc:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-unreadable-hold"
            ) from exc
        if not raw or not raw.endswith(b"\n") or b"\r" in raw:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-truncated-or-noncanonical"
            )
        result: list[dict[str, Any]] = []
        for line in raw.splitlines(keepends=True):
            try:
                value = json.loads(line[:-1].decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-json-invalid"
                ) from exc
            if not isinstance(value, dict) or _canonical(value) + b"\n" != line:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-noncanonical"
                )
            result.append(value)
        return result

    @staticmethod
    def _initial_projection(ledger_id: str) -> dict[str, Any]:
        return {
            "schemaVersion": PROJECTION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "facadeLedgerIdHash": ledger_id,
            "facadeEpoch": 0,
            "epochSequence": 0,
            "epochHeadHash": ZERO_HASH,
            "snapshotSequence": 0,
            "snapshotHeadHash": ZERO_HASH,
            "burnSequence": 0,
            "burnHeadHash": ZERO_HASH,
        }

    @staticmethod
    def _assert_transition_progress(
        previous: Mapping[str, Any], candidate: Mapping[str, Any]
    ) -> None:
        changed = candidate["facadeEpoch"] > previous["facadeEpoch"]
        if candidate["facadeEpoch"] < previous["facadeEpoch"]:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-facade-epoch-rollback"
            )
        if (
            candidate["facadeEpoch"] > previous["facadeEpoch"]
            and candidate["epochSequence"] <= previous["epochSequence"]
        ):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-facade-epoch-without-epoch-transition"
            )
        for sequence_key, head_key in (
            ("epochSequence", "epochHeadHash"),
            ("snapshotSequence", "snapshotHeadHash"),
            ("burnSequence", "burnHeadHash"),
        ):
            prior_sequence = previous[sequence_key]
            sequence = candidate[sequence_key]
            if sequence < prior_sequence:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-sequence-rollback"
                )
            if sequence == prior_sequence:
                if candidate[head_key] != previous[head_key]:
                    raise KisDomesticFunctionalFacadeAnchorBlocked(
                        "facade-anchor-equal-sequence-head-substitution"
                    )
            else:
                changed = True
                if candidate[head_key] == ZERO_HASH:
                    raise KisDomesticFunctionalFacadeAnchorBlocked(
                        "facade-anchor-advanced-sequence-zero-head"
                    )
        if not changed:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-transition-does-not-advance"
            )

    def _verify_chain(self) -> dict[str, Any]:
        lines = self._read_lines()
        header, header_head, writer_key = self._verify_header_static(
            self.path, self.pins, lines[0]
        )
        records: list[str] = [header_head]
        projection = self._initial_projection(self.pins.facade_ledger_id_hash)
        prior_wall = _time(header["createdAt"], "facade-anchor-created-at")
        prior_mono = header["createdMonotonicNs"]
        writer_before = _time(
            header["writerNotBefore"], "facade-anchor-writer-not-before"
        )
        writer_after = _time(
            header["writerNotAfter"], "facade-anchor-writer-not-after"
        )
        for expected_epoch, envelope in enumerate(lines[1:], start=1):
            if not isinstance(envelope, Mapping) or set(envelope) != _ENVELOPE_KEYS:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-transition-envelope-not-exact"
                )
            body = envelope.get("body")
            if not isinstance(body, Mapping) or set(body) != _TRANSITION_KEYS:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-transition-body-not-exact"
                )
            body = dict(body)
            candidate = {
                key: body[key]
                for key in _PROJECTION_KEYS
            }
            candidate["schemaVersion"] = PROJECTION_SCHEMA
            _projection(candidate, self.pins.facade_ledger_id_hash)
            observed = _time(body.get("observedAt"), "facade-anchor-observed-at")
            monotonic_ns = _integer(
                body.get("observedMonotonicNs"),
                "facade-anchor-observed-monotonic",
            )
            expected = {
                "schemaVersion": TRANSITION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "facadeLedgerIdHash": self.pins.facade_ledger_id_hash,
                "anchorEpoch": expected_epoch,
                "previousRecordHash": records[-1],
                "writerKeyIdHash": self.pins.writer_key_id_hash,
                "productionAvailable": False,
            }
            if (
                any(
                    type(body.get(name)) is not type(value)
                    or body.get(name) != value
                    for name, value in expected.items()
                )
                or observed < prior_wall
                or observed < writer_before
                or observed >= writer_after
                or monotonic_ns < prior_mono
            ):
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-transition-binding-or-clock-invalid"
                )
            self._assert_transition_progress(projection, candidate)
            if (
                _sha(envelope.get("recordHash"), "facade-anchor-record-hash")
                != _hash(body)
                or _sha(envelope.get("keyIdHash"), "facade-anchor-record-key")
                != self.pins.writer_key_id_hash
                or not _verify(
                    writer_key,
                    WRITER_SIGNATURE_DOMAIN,
                    body,
                    envelope.get("signature"),
                )
            ):
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-transition-signature-invalid"
                )
            records.append(_hash(dict(envelope)))
            projection = candidate
            prior_wall = observed
            prior_mono = monotonic_ns
        minimum = self.pins.minimum_anchor_epoch
        if (
            minimum >= len(records)
            or not hmac.compare_digest(
                records[minimum], self.pins.minimum_anchor_head_hash
            )
        ):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-external-minimum-rollback-pin-failed"
            )
        current_epoch = len(records) - 1
        current_head = records[-1]
        if current_epoch < self._observed_anchor_epoch:
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-process-lifetime-rollback-detected"
            )
        if (
            current_epoch == self._observed_anchor_epoch
            and not hmac.compare_digest(
                current_head, self._observed_anchor_head_hash
            )
        ):
            raise KisDomesticFunctionalFacadeAnchorBlocked(
                "facade-anchor-process-lifetime-head-substitution-detected"
            )
        self._observed_anchor_epoch = current_epoch
        self._observed_anchor_head_hash = current_head
        return {
            "header": header,
            "writerKey": writer_key,
            "anchorEpoch": current_epoch,
            "anchorHeadHash": current_head,
            "records": records,
            "projection": projection,
            "lastObservedAt": prior_wall,
            "lastObservedMonotonicNs": prior_mono,
        }

    def next_transition_body(
        self,
        projection: Mapping[str, Any],
        *,
        observed_at: datetime,
        observed_monotonic_ns: int,
    ) -> dict[str, Any]:
        with self._lock:
            chain = self._verify_chain()
            candidate = _projection(
                projection, self.pins.facade_ledger_id_hash
            )
            self._assert_transition_progress(chain["projection"], candidate)
            observed_text = _iso(observed_at)
            observed = _time(observed_text, "facade-anchor-next-observed-at")
            monotonic_ns = _integer(
                observed_monotonic_ns,
                "facade-anchor-next-observed-monotonic",
            )
            header = chain["header"]
            if (
                observed < chain["lastObservedAt"]
                or observed < _time(
                    header["writerNotBefore"], "facade-anchor-writer-not-before"
                )
                or observed >= _time(
                    header["writerNotAfter"], "facade-anchor-writer-not-after"
                )
                or monotonic_ns < chain["lastObservedMonotonicNs"]
            ):
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-next-clock-invalid"
                )
            return {
                "schemaVersion": TRANSITION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "facadeLedgerIdHash": self.pins.facade_ledger_id_hash,
                "anchorEpoch": chain["anchorEpoch"] + 1,
                "facadeEpoch": candidate["facadeEpoch"],
                "epochSequence": candidate["epochSequence"],
                "epochHeadHash": candidate["epochHeadHash"],
                "snapshotSequence": candidate["snapshotSequence"],
                "snapshotHeadHash": candidate["snapshotHeadHash"],
                "burnSequence": candidate["burnSequence"],
                "burnHeadHash": candidate["burnHeadHash"],
                "previousRecordHash": chain["anchorHeadHash"],
                "writerKeyIdHash": self.pins.writer_key_id_hash,
                "observedAt": observed_text,
                "observedMonotonicNs": monotonic_ns,
                "productionAvailable": False,
            }

    def append_signed_transition(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            chain = self._verify_chain()
            if not isinstance(envelope, Mapping) or set(envelope) != _ENVELOPE_KEYS:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-append-envelope-not-exact"
                )
            body = envelope.get("body")
            if not isinstance(body, Mapping) or set(body) != _TRANSITION_KEYS:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-append-body-not-exact"
                )
            body = dict(body)
            candidate = {key: body[key] for key in _PROJECTION_KEYS}
            candidate["schemaVersion"] = PROJECTION_SCHEMA
            expected_body = self.next_transition_body(
                candidate,
                observed_at=_time(
                    body.get("observedAt"), "facade-anchor-append-observed-at"
                ),
                observed_monotonic_ns=body.get("observedMonotonicNs"),
            )
            if body != expected_body:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-append-body-not-current-next"
                )
            if (
                _sha(envelope.get("recordHash"), "facade-anchor-append-hash")
                != _hash(body)
                or _sha(envelope.get("keyIdHash"), "facade-anchor-append-key")
                != self.pins.writer_key_id_hash
                or not _verify(
                    chain["writerKey"],
                    WRITER_SIGNATURE_DOMAIN,
                    body,
                    envelope.get("signature"),
                )
            ):
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-append-signature-invalid"
                )
            if self._failure_injector is not None:
                self._failure_injector("before-append")
            payload = _canonical(dict(envelope)) + b"\n"
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    self.path,
                    os.O_WRONLY | os.O_APPEND | getattr(os, "O_BINARY", 0),
                )
                opened = os.fstat(descriptor)
                if (int(opened.st_dev), int(opened.st_ino)) != self._identity:
                    raise KisDomesticFunctionalFacadeAnchorBlocked(
                        "facade-anchor-file-identity-replaced-before-append"
                    )
                written = os.write(descriptor, payload)
                if written != len(payload):
                    raise OSError("facade-anchor-append-short-write")
                if self._failure_injector is not None:
                    self._failure_injector("after-write-before-fsync")
                os.fsync(descriptor)
                if self._failure_injector is not None:
                    self._failure_injector("after-fsync")
            except KisDomesticFunctionalFacadeAnchorBlocked:
                raise
            except OSError as exc:
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    f"facade-anchor-append-failed:{type(exc).__name__}"
                ) from exc
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            verified = self._verify_chain()
            wanted_head = _hash(dict(envelope))
            if (
                verified["anchorEpoch"] != chain["anchorEpoch"] + 1
                or not hmac.compare_digest(verified["anchorHeadHash"], wanted_head)
            ):
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-append-not-durable"
                )
            return self.read()

    def current_projection(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._verify_chain()["projection"])

    def assert_current_projection(self, candidate: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            value = _projection(candidate, self.pins.facade_ledger_id_hash)
            current = self._verify_chain()["projection"]
            if value != current:
                for sequence_key in (
                    "facadeEpoch",
                    "epochSequence",
                    "snapshotSequence",
                    "burnSequence",
                ):
                    if value[sequence_key] < current[sequence_key]:
                        raise KisDomesticFunctionalFacadeAnchorBlocked(
                            "facade-anchor-paired-local-rollback-detected"
                        )
                raise KisDomesticFunctionalFacadeAnchorBlocked(
                    "facade-anchor-projection-substitution-or-reconciliation-required"
                )
            return dict(value)

    def read(self) -> dict[str, Any]:
        with self._lock:
            chain = self._verify_chain()
            projection = chain["projection"]
            body = {
                "schemaVersion": STATUS_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.pins.anchor_id,
                "anchorPathHash": _path_hash(self.path),
                "facadeLedgerIdHash": self.pins.facade_ledger_id_hash,
                "anchorEpoch": chain["anchorEpoch"],
                "anchorHeadHash": chain["anchorHeadHash"],
                "facadeEpoch": projection["facadeEpoch"],
                "epochSequence": projection["epochSequence"],
                "epochHeadHash": projection["epochHeadHash"],
                "snapshotSequence": projection["snapshotSequence"],
                "snapshotHeadHash": projection["snapshotHeadHash"],
                "burnSequence": projection["burnSequence"],
                "burnHeadHash": projection["burnHeadHash"],
                "minimumAnchorEpochVerified": self.pins.minimum_anchor_epoch,
                "minimumAnchorHeadHashVerified": (
                    self.pins.minimum_anchor_head_hash
                ),
                "rootInstallationSignatureVerified": True,
                "writerPurposeVerified": True,
                "appendOnlyChainVerified": True,
                "osProcessLeaseHeld": True,
                "pathFileIdentityPinnedForProcessLifetime": True,
                "pairedFacadeAndLocalHighWaterRollbackDetected": True,
                "externalMinimumRollbackPinSuppliedAndVerified": True,
                "externalMinimumRollbackPinStoreWired": False,
                "hardwareOrWormMonotonicityProven": False,
                "verifyOnlyConsumer": True,
                "privateSignerPresent": False,
                "productionProvisioningAvailable": False,
                "facadeIntegrationWired": False,
                "readinessBlockers": [
                    "PRODUCTION_EXTERNAL_ANCHOR_PATH_NOT_PROVISIONED",
                    "EXTERNAL_WRITER_SERVICE_NOT_PROVISIONED",
                    "HARDWARE_OR_WORM_MONOTONIC_COUNTER_NOT_WIRED",
                    "EXTERNAL_MINIMUM_ANCHOR_PIN_STORE_NOT_WIRED",
                    "FACADE_ADVANCE_AND_STARTUP_JOIN_NOT_INTEGRATED",
                ],
                "productionAvailable": False,
                "networkAvailable": False,
                "releaseAvailable": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }
            return {**body, "statusHash": _hash(body)}

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._lease.release()
            finally:
                with _LOCAL_LEASES_LOCK:
                    _LOCAL_LEASE_SCOPES.discard(self._lease_scope)
                self._closed = True

    def __enter__(self) -> "AppendOnlyKisDomesticFunctionalFacadeAnchor":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()


def production_entrypoint_status() -> dict[str, Any]:
    body = {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "externalAppendOnlyAnchorImplemented": True,
        "verifyOnlyConsumerImplemented": True,
        "productionProvisioningAvailable": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "releaseAvailable": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "statusHash": _hash(body)}
