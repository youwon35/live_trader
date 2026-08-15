from __future__ import annotations

"""Independent Upbit account-exclusivity proof authority.

This module is intentionally outside :mod:`live_trader`.  It owns the private
Ed25519 key, the authoritative observation database, authenticated account-wide
GET polling and the all-market ``myOrder`` socket.  The trading process sees
only a public key, a verifier pin and signed proof files.

The daemon has no order/cancel builder and refuses to start when any live-order
release flag is enabled.  A stream gap, observer restart during an active
session, foreign account activity, key-inventory drift, or bot/process drift is
latched durably and permanently prevents further proofs for that session.
"""

import argparse
import base64
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import getpass
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence
from urllib.parse import urlsplit

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.process_safety import (
    _APPLICATION_INSTANCE_SCOPE,
    _owner_metadata,
    _safe_lock_name,
    acquire_process_lease,
    process_lock_root,
)
from live_trader.upbit_read_only_http import (
    PreparedRequest,
    _protected_upbit_read_only_http_network_capability,
    send_prepared_request,
)
from live_trader.upbit_account_exclusivity import (
    PinnedEd25519UpbitAccountExclusivityVerifier,
    canonical_exclusivity_signature_message,
    upbit_spot_credential_binding_sha256,
)
from live_trader.upbit_continuous_functional import (
    ACCOUNT_API_KEY_INVENTORY_SOURCE,
    ACCOUNT_BOT_REGISTRY_SOURCE,
    ACCOUNT_EXCLUSIVITY_PROOF_REQUEST_SCHEMA_VERSION,
    ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION_V2,
    ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE,
    _strict_stable_hash,
    upbit_functional_session_identifier_prefix,
)
from live_trader.upbit_functional_sources import (
    OfficialUpbitFunctionalMyOrderPump,
)
from live_trader.upbit_functional_transport import (
    DurableUpbitMyOrderJournal,
    OfficialUpbitFunctionalGetClient,
    _protected_upbit_functional_get_network_capability,
    upbit_credential_fingerprint,
)
from live_trader.upbit_functional_truth import (
    UPBIT_API_KEYS_ENDPOINT,
    UPBIT_CLOSED_ORDERS_ENDPOINT,
    UPBIT_OPEN_ORDERS_ENDPOINT,
)
from trading_runtime.secret_store import SecretStore, default_secret_store_path


AUTHORITY_CONFIG_SCHEMA = "upbit-independent-exclusivity-authority-config/v1"
PUBLIC_CONFIG_SCHEMA = "upbit-independent-exclusivity-public-config/v1"
OBSERVATION_SCHEMA = "upbit-independent-exclusivity-observation/v1"
LOSS_SCHEMA = "upbit-independent-exclusivity-proof-loss/v1"
OUTBOX_SCHEMA = "upbit-account-exclusivity-proof-request-outbox/v1"
STATUS_SCHEMA = "upbit-independent-exclusivity-authority-status/v1"
DB_APPLICATION_ID = 0x55414558
DB_USER_VERSION = 1
ZERO_HASH = "0" * 64
OFFICIAL_ORIGIN = "https://api.upbit.com"
GET_GATE = "UPBIT_EXCLUSIVITY_AUTHORITY_GET_ENABLED"
MAX_REQUEST_BYTES = 256 * 1024
MAX_OPEN_PAGES = 20
OPEN_PAGE_LIMIT = 100
CLOSED_LIMIT = 100
MAX_REQUEST_AGE_SECONDS = 15
POLL_INTERVAL_SECONDS = 5
TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
BUNDLE_MANIFEST_SCHEMA = (
    "crypto-first-live-supervised-authority-bundle-manifest/v1"
)
_BUNDLE_MANIFEST_FIELDS = {
    "schemaVersion",
    "authorityOsSid",
    "traderOsSid",
    "authorityRoot",
    "sharedRoot",
    "sourcePins",
    "sealedRoots",
    "files",
    "pinnedFiles",
    "externalBinaries",
    "pycryptodomeWheelSha256",
    "githubHostKeyRawSha256",
    "remoteRef",
    "brokerBundleDescriptorSha256",
    "brokerModes",
    "brokerCredentialAuthorityId",
    "machineProtectedCredentials",
    "formalWorm",
    "promotionEligible",
}
_SYSTEM_SID = "S-1-5-18"
_ADMINISTRATORS_SID = "S-1-5-32-544"
_WINDOWS_FULL_CONTROL = 2032127
_WINDOWS_READ_AND_EXECUTE = 131241

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_PHASE_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")
_SID_RE = re.compile(r"^S-1-(?:\d+-){1,14}\d+$", re.IGNORECASE)


class UpbitIndependentAuthorityError(RuntimeError):
    pass


def live_secret_name(key: str) -> str:
    """Match the trader secret namespace without importing app bootstrap."""

    return f"live_trader.{str(key).strip()}"


def _text(value: object) -> str:
    return str(value or "").strip()


def _hash(value: object) -> str:
    return _strict_stable_hash(value)


def _require_hash(value: object, label: str) -> str:
    text = _text(value)
    if type(value) is not str or _HASH_RE.fullmatch(text) is None:
        raise UpbitIndependentAuthorityError(label + "-invalid")
    return text


def _require_id(value: object, label: str) -> str:
    text = _text(value)
    if type(value) is not str or _SAFE_ID_RE.fullmatch(text) is None:
        raise UpbitIndependentAuthorityError(label + "-invalid")
    return text


def _count(value: Mapping[str, Any], field: str) -> int:
    raw = value.get(field)
    if type(raw) is not int or raw < 0:
        raise UpbitIndependentAuthorityError(
            "authority-count-invalid:" + field
        )
    return raw


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitIndependentAuthorityError(label + "-invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitIndependentAuthorityError(label + "-timezone-missing")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: object) -> str:
    # Match the functional verifier's exact, fixed-width UTC contract.
    return _utc(value, "time").strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _strict_json(path: Path, *, maximum: int = MAX_REQUEST_BYTES) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise UpbitIndependentAuthorityError("authority-json-file-missing-or-link")
    size = path.stat().st_size
    if size <= 1 or size > maximum:
        raise UpbitIndependentAuthorityError("authority-json-file-size-invalid")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
        if len(raw) != size or raw.startswith(b"\xef\xbb\xbf"):
            raise ValueError("unstable/BOM file")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")
            ),
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError) as exc:
        raise UpbitIndependentAuthorityError("authority-json-file-invalid") from exc
    if not isinstance(value, dict):
        raise UpbitIndependentAuthorityError("authority-json-object-required")
    return value


def _write_new(path: Path, value: Mapping[str, Any], *, private: bool) -> None:
    encoded = _canonical_bytes(dict(value))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            str(path),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600 if private else 0o644,
        )
    except FileExistsError as exc:
        raise UpbitIndependentAuthorityError("authority-output-already-exists") from exc
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def _replace_exact(path: Path, value: Mapping[str, Any], *, private: bool) -> None:
    encoded = _canonical_bytes(dict(value))
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    try:
        descriptor = os.open(
            str(temporary),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600 if private else 0o644,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _principal() -> str:
    if os.name == "nt":
        try:
            value = subprocess.run(
                ["whoami"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            if value:
                return value
        except (OSError, subprocess.SubprocessError):
            pass
    return getpass.getuser()


def _windows_acl_projection(path: Path) -> dict[str, Any]:
    if os.name != "nt":
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-windows-acl-required"
        )
    variable = "UPBIT_AUTHORITY_ACL_TARGET"
    environment = dict(os.environ)
    environment[variable] = str(path)
    script = (
        f"$a=Get-Acl -LiteralPath $env:{variable};"
        "$o=([Security.Principal.NTAccount]$a.Owner).Translate("
        "[Security.Principal.SecurityIdentifier]).Value;"
        "$r=@($a.Access|ForEach-Object{[ordered]@{"
        "sid=$_.IdentityReference.Translate("
        "[Security.Principal.SecurityIdentifier]).Value;"
        "type=$_.AccessControlType.ToString();"
        "rights=[int64]$_.FileSystemRights;"
        "inherited=[bool]$_.IsInherited}});"
        "[ordered]@{ownerSid=$o;protected=[bool]"
        "$a.AreAccessRulesProtected;rules=$r}|ConvertTo-Json -Compress -Depth 6"
    )
    try:
        completed = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
            env=environment,
        )
        value = json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-acl-read-failed"
        ) from exc
    if not isinstance(value, Mapping):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-acl-invalid"
        )
    return dict(value)


def _validate_protected_acl(value: Mapping[str, Any]) -> None:
    projection = dict(value)
    rules = projection.get("rules")
    if not isinstance(rules, list) or any(
        not isinstance(row, Mapping) for row in rules
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-acl-invalid"
        )
    allowed = {_SYSTEM_SID, _ADMINISTRATORS_SID}
    full: set[str] = set()
    for raw in rules:
        row = dict(raw)
        try:
            rights = int(row.get("rights"))
        except (TypeError, ValueError) as exc:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-acl-invalid"
            ) from exc
        sid = _text(row.get("sid"))
        if (
            sid not in allowed
            or _text(row.get("type")) != "Allow"
            or (
                rights & _WINDOWS_FULL_CONTROL
            ) != _WINDOWS_FULL_CONTROL
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-acl-not-exclusive"
            )
        full.add(sid)
    if (
        projection.get("protected") is not True
        or _text(projection.get("ownerSid")) != _ADMINISTRATORS_SID
        or full != allowed
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-acl-not-exclusive"
        )


def _validate_shared_acl(
    value: Mapping[str, Any], *, trader_os_sid: str
) -> None:
    projection = dict(value)
    rules = projection.get("rules")
    if not isinstance(rules, list) or any(
        not isinstance(row, Mapping) for row in rules
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-shared-acl-invalid"
        )
    expected = {
        _SYSTEM_SID: _WINDOWS_FULL_CONTROL,
        _ADMINISTRATORS_SID: _WINDOWS_FULL_CONTROL,
        trader_os_sid: _WINDOWS_READ_AND_EXECUTE,
    }
    observed: dict[str, int] = {}
    for raw in rules:
        row = dict(raw)
        try:
            rights = int(row.get("rights"))
        except (TypeError, ValueError) as exc:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-shared-acl-invalid"
            ) from exc
        sid = _text(row.get("sid")).upper()
        if (
            sid not in expected
            or _text(row.get("type")) != "Allow"
            or rights != expected[sid]
            or sid in observed
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-shared-acl-not-exclusive"
            )
        observed[sid] = rights
    if (
        projection.get("protected") is not True
        or _text(projection.get("ownerSid")).upper()
        != _ADMINISTRATORS_SID
        or set(observed) != set(expected)
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-shared-acl-not-exclusive"
        )


def verify_protected_authority_bundle(
    *,
    manifest_path: str | Path,
    workspace_root: str | Path,
    canonical_python_executable: str | Path,
    authority_entrypoint: str | Path,
    acl_reader: Callable[[Path], Mapping[str, Any]] = (
        _windows_acl_projection
    ),
) -> dict[str, Any]:
    """Verify the SYSTEM-owned frozen source/venv bundle before any GET."""

    manifest_file = Path(manifest_path).expanduser().resolve()
    workspace = Path(workspace_root).expanduser().resolve()
    python_path = Path(canonical_python_executable).expanduser().resolve()
    entrypoint = Path(authority_entrypoint).expanduser().resolve()
    manifest = _strict_json(manifest_file, maximum=16 * 1024 * 1024)
    sealed_raw = manifest.get("sealedRoots")
    files_raw = manifest.get("files")
    pinned_raw = manifest.get("pinnedFiles")
    external_raw = manifest.get("externalBinaries")
    source_pins = manifest.get("sourcePins")
    broker_modes = manifest.get("brokerModes")
    credentials = manifest.get("machineProtectedCredentials")
    authority_root = manifest_file.parent
    declared_authority_root = Path(
        _text(manifest.get("authorityRoot"))
    ).expanduser().resolve()
    shared_root = Path(_text(manifest.get("sharedRoot"))).expanduser().resolve()
    trader_os_sid = _text(manifest.get("traderOsSid")).upper()
    if (
        manifest.get("schemaVersion") != BUNDLE_MANIFEST_SCHEMA
        or set(manifest) != _BUNDLE_MANIFEST_FIELDS
        or _text(manifest.get("authorityOsSid")).upper() != _SYSTEM_SID
        or _SID_RE.fullmatch(trader_os_sid) is None
        or trader_os_sid in {_SYSTEM_SID, _ADMINISTRATORS_SID}
        or declared_authority_root != authority_root
        or shared_root == authority_root
        or _is_within(shared_root, authority_root)
        or _is_within(authority_root, shared_root)
        or shared_root.is_symlink()
        or not shared_root.is_dir()
        or not isinstance(sealed_raw, list)
        or not isinstance(files_raw, list)
        or not isinstance(pinned_raw, list)
        or not isinstance(external_raw, list)
        or not isinstance(source_pins, Mapping)
        or set(source_pins)
        != {
            "authorityToolSha256",
            "anchorModuleSha256",
            "brokerBundleDescriptorSha256",
            "credentialRewrapToolSha256",
        }
        or not isinstance(broker_modes, list)
        or len(broker_modes) != 2
        or not isinstance(credentials, list)
        or len(credentials) != 2
        or manifest.get("formalWorm") is not False
        or manifest.get("promotionEligible") is not False
        or not sealed_raw
        or not files_raw
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-manifest-invalid"
        )
    for label, raw in (
        ("pycryptodome-wheel", manifest.get("pycryptodomeWheelSha256")),
        ("github-host-key", manifest.get("githubHostKeyRawSha256")),
        ("broker-descriptor", manifest.get("brokerBundleDescriptorSha256")),
        *[("source-pin", value) for value in source_pins.values()],
    ):
        _require_hash(raw, label)
    _require_id(
        manifest.get("brokerCredentialAuthorityId"),
        "broker-credential-authority-id",
    )
    if not _text(manifest.get("remoteRef")).startswith("refs/heads/"):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-manifest-invalid"
        )
    normalized_modes: dict[str, dict[str, Any]] = {}
    mode_fields = {
        "mode",
        "taskName",
        "pipeAddress",
        "entryPoint",
        "importRoot",
        "arguments",
        "environment",
    }
    for raw in broker_modes:
        if not isinstance(raw, Mapping) or set(raw) != mode_fields:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-broker-mode-invalid"
            )
        mode = dict(raw)
        name = _text(mode.get("mode"))
        environment = mode.get("environment")
        if (
            name in normalized_modes
            or name not in {"UPBIT_AUTHORITY", "BINANCE_OBSERVER"}
            or not _text(mode.get("taskName"))
            or not _text(mode.get("pipeAddress")).startswith(
                "\\\\.\\pipe\\"
            )
            or not isinstance(mode.get("arguments"), list)
            or any(
                not isinstance(item, str) for item in mode["arguments"]
            )
            or not isinstance(environment, list)
            or any(
                not isinstance(item, Mapping)
                or set(item) != {"name", "value"}
                or not isinstance(item.get("name"), str)
                or not isinstance(item.get("value"), str)
                for item in environment
            )
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-broker-mode-invalid"
            )
        normalized_modes[name] = mode
    if set(normalized_modes) != {"UPBIT_AUTHORITY", "BINANCE_OBSERVER"}:
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-broker-mode-invalid"
        )
    upbit_mode = normalized_modes["UPBIT_AUTHORITY"]
    mode_workspace = Path(_text(upbit_mode["importRoot"])).resolve()
    mode_entrypoint = Path(_text(upbit_mode["entryPoint"])).resolve()
    sealed: list[Path] = []
    for raw in sealed_raw:
        path = Path(_text(raw)).expanduser().resolve()
        if path.is_symlink() or not path.is_dir() or path in sealed:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-sealed-root-invalid"
            )
        sealed.append(path)
    if (
        workspace != mode_workspace
        or entrypoint != mode_entrypoint
        or not any(_is_within(workspace, root) for root in sealed)
        or not any(
        _is_within(python_path, root) for root in sealed
        )
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-canonical-path-unsealed"
        )
    if (
        not _is_within(entrypoint, workspace)
        or entrypoint.is_symlink()
        or not entrypoint.is_file()
        or python_path.is_symlink()
        or not python_path.is_file()
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-entrypoint-invalid"
        )
    expected: dict[Path, str] = {}
    for raw in files_raw:
        if not isinstance(raw, Mapping) or set(raw) != {"path", "sha256"}:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-file-record-invalid"
            )
        path = Path(_text(raw.get("path"))).expanduser().resolve()
        digest = _require_hash(raw.get("sha256"), "bundle-file-sha256")
        if path in expected or not any(
            _is_within(path, root) for root in sealed
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-file-record-invalid"
            )
        expected[path] = digest
    for mode in normalized_modes.values():
        mode_root = Path(_text(mode["importRoot"])).resolve()
        mode_entry = Path(_text(mode["entryPoint"])).resolve()
        if (
            not any(_is_within(mode_root, root) for root in sealed)
            or not _is_within(mode_entry, mode_root)
            or mode_entry not in expected
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-broker-mode-unsealed"
            )
    if entrypoint not in expected or python_path not in expected:
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-canonical-file-unlisted"
        )
    for path, digest in expected.items():
        if path.is_symlink() or not path.is_file():
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-file-missing-or-link"
            )
        if not hmac.compare_digest(
            hashlib.sha256(path.read_bytes()).hexdigest(), digest
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-file-changed"
            )
    for root in sealed:
        observed: set[Path] = set()
        for candidate in root.rglob("*"):
            if candidate.is_symlink():
                raise UpbitIndependentAuthorityError(
                    "authority-protected-bundle-link-forbidden"
                )
            if candidate.is_file():
                observed.add(candidate.resolve())
        if observed != {
            path for path in expected if _is_within(path, root)
        }:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-extra-or-missing-file"
            )
    pinned: dict[Path, str] = {}
    for raw in pinned_raw:
        if not isinstance(raw, Mapping) or set(raw) != {"path", "sha256"}:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-pinned-record-invalid"
            )
        path = Path(_text(raw.get("path"))).expanduser().resolve()
        digest = _require_hash(raw.get("sha256"), "bundle-pinned-sha256")
        if (
            path in expected
            or path in pinned
            or any(_is_within(path, root) for root in sealed)
            or not (
                _is_within(path, authority_root)
                or _is_within(path, shared_root)
            )
            or path.is_symlink()
            or not path.is_file()
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-pinned-record-invalid"
            )
        if not hmac.compare_digest(
            hashlib.sha256(path.read_bytes()).hexdigest(), digest
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-pinned-file-changed"
            )
        pinned[path] = digest
    credential_lanes: set[str] = set()
    exact_credential_paths = {
        "UPBIT": (authority_root / "secrets" / "upbit-credential.dpapi").resolve(),
        "BINANCE_SPOT": (
            authority_root / "secrets" / "binance-credential.dpapi"
        ).resolve(),
    }
    credential_fields = {
        "lane",
        "path",
        "credentialFingerprint",
        "accountFingerprint",
        "envelopeHash",
        "credentialGenerationId",
    }
    for raw in credentials:
        if not isinstance(raw, Mapping) or set(raw) != credential_fields:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-credential-record-invalid"
            )
        credential = dict(raw)
        lane = _text(credential.get("lane"))
        path = Path(_text(credential.get("path"))).resolve()
        if (
            lane in credential_lanes
            or lane not in {"UPBIT", "BINANCE_SPOT"}
            or path != exact_credential_paths.get(lane)
            or path in pinned
            or path in expected
            or not _is_within(path, authority_root)
            or path.is_symlink()
            or not path.is_file()
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-credential-record-invalid"
            )
        for field in (
            "credentialFingerprint",
            "accountFingerprint",
            "envelopeHash",
        ):
            _require_hash(
                credential.get(field), "bundle-credential-" + field
            )
        _require_id(
            credential.get("credentialGenerationId"),
            "bundle-credential-generation-id",
        )
        _validate_protected_acl(dict(acl_reader(path)))
        credential_lanes.add(lane)
    if credential_lanes != {"UPBIT", "BINANCE_SPOT"}:
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-credential-record-invalid"
        )
    for raw in external_raw:
        if not isinstance(raw, Mapping) or set(raw) != {"path", "sha256"}:
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-external-record-invalid"
            )
        path = Path(_text(raw.get("path"))).expanduser().resolve()
        digest = _require_hash(
            raw.get("sha256"), "bundle-external-sha256"
        )
        if path.is_symlink() or not path.is_file() or not hmac.compare_digest(
            hashlib.sha256(path.read_bytes()).hexdigest(), digest
        ):
            raise UpbitIndependentAuthorityError(
                "authority-protected-bundle-external-binary-changed"
            )
    if any(not _is_within(root, authority_root) for root in sealed):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-root-escaped"
        )
    _validate_protected_acl(dict(acl_reader(authority_root)))
    _validate_shared_acl(
        dict(acl_reader(shared_root)), trader_os_sid=trader_os_sid
    )
    manifest_sha = hashlib.sha256(manifest_file.read_bytes()).hexdigest()
    return {
        "schemaVersion": BUNDLE_MANIFEST_SCHEMA,
        "manifestPath": str(manifest_file),
        "manifestSha256": manifest_sha,
        "workspaceRoot": str(workspace),
        "canonicalPythonExecutable": str(python_path),
        "authorityEntrypointPath": str(entrypoint),
        "authorityEntrypointSha256": expected[entrypoint],
        "aclExclusive": True,
        "sealed": True,
        "restartVerifiable": True,
    }


def _harden_acl(
    private_root: Path,
    public_root: Path,
    proof_directory: Path,
    authority_principal: str,
    trader_principal: str,
) -> bool:
    if os.name != "nt":
        os.chmod(private_root, 0o700)
        os.chmod(public_root, 0o755)
        os.chmod(proof_directory, 0o770)
        return True
    commands = (
        [
            "icacls",
            str(private_root),
            "/inheritance:r",
            "/grant:r",
            f"{authority_principal}:(OI)(CI)F",
            "/T",
            "/C",
        ],
        [
            "icacls",
            str(public_root),
            "/inheritance:r",
            "/grant:r",
            f"{authority_principal}:(OI)(CI)F",
            "/grant:r",
            f"{trader_principal}:(OI)(CI)RX",
            "/T",
            "/C",
        ],
        [
            "icacls",
            str(proof_directory),
            "/inheritance:r",
            "/grant:r",
            f"{authority_principal}:(OI)(CI)M",
            "/grant:r",
            f"{trader_principal}:(OI)(CI)M",
            "/T",
            "/C",
        ],
    )
    try:
        for command in commands:
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=20,
            )
    except (OSError, subprocess.SubprocessError):
        return False
    return True


@dataclass(frozen=True, slots=True)
class AuthorityPaths:
    private_root: Path
    public_root: Path
    proof_directory: Path
    private_key: Path
    public_key: Path
    verifier_pin: Path
    authority_config: Path
    public_config: Path
    database: Path
    myorder_database: Path
    proof_loss: Path
    process_lock_directory: Path


def store_authority_credentials(
    *,
    authority_principal: str,
    trader_principal: str,
    secret_store_path: str | Path | None = None,
    principal_reader: Callable[[], str] = _principal,
    secret_reader: Callable[[], tuple[str, str]] | None = None,
    store_factory: Callable[[Path], Any] = SecretStore,
) -> dict[str, Any]:
    """Interactively persist the Upbit pair under the authority DPAPI user."""

    current = _text(principal_reader())
    authority_user = _text(authority_principal)
    trader_user = _text(trader_principal)
    if (
        not current
        or not authority_user
        or not trader_user
        or current.casefold() != authority_user.casefold()
        or authority_user.casefold() == trader_user.casefold()
    ):
        raise UpbitIndependentAuthorityError(
            "authority-distinct-current-principal-required"
        )
    path = Path(
        secret_store_path or default_secret_store_path()
    ).expanduser().resolve()
    if path.is_symlink():
        raise UpbitIndependentAuthorityError(
            "authority-secret-store-symlink-forbidden"
        )
    if secret_reader is None:
        secret_reader = lambda: (
            getpass.getpass("Upbit access key (hidden): "),
            getpass.getpass("Upbit secret key (hidden): "),
        )
    access_key, secret_key = secret_reader()
    access_key = _text(access_key)
    secret_key = _text(secret_key)
    account_fingerprint = upbit_credential_fingerprint(access_key)
    credential_binding = upbit_spot_credential_binding_sha256(
        access_key, secret_key
    )
    if not account_fingerprint or not credential_binding:
        raise UpbitIndependentAuthorityError(
            "authority-upbit-credentials-invalid"
        )
    store = store_factory(path)
    access_name = live_secret_name("UPBIT_ACCESS_KEY")
    secret_name = live_secret_name("UPBIT_SECRET_KEY")
    old_access = _text(store.get(access_name))
    old_secret = _text(store.get(secret_name))
    try:
        store.set(access_name, access_key)
        store.set(secret_name, secret_key)
        if (
            not hmac.compare_digest(_text(store.get(access_name)), access_key)
            or not hmac.compare_digest(
                _text(store.get(secret_name)), secret_key
            )
        ):
            raise UpbitIndependentAuthorityError(
                "authority-dpapi-credential-readback-failed"
            )
    except Exception:
        for name, prior in (
            (access_name, old_access),
            (secret_name, old_secret),
        ):
            if prior:
                store.set(name, prior)
            else:
                store.delete(name)
        raise
    access_key = ""
    secret_key = ""
    return {
        "schemaVersion": "upbit-independent-authority-credential-store/v1",
        "stored": True,
        "protector": str(getattr(store.protector, "name", "")),
        "accountFingerprint": account_fingerprint,
        "credentialBindingSha256": credential_binding,
        "secretValuesReturned": False,
        "networkRequestCount": 0,
        "orderMutationAllowed": False,
        "liveActivationReleased": False,
    }


def provision_authority(
    *,
    private_root: str | Path,
    public_root: str | Path,
    authority_principal: str,
    trader_principal: str,
    server_owner_identity_sha256: str,
    canonical_python_executable: str | Path,
    process_lock_directory: str | Path,
    workspace_root: str | Path,
    trader_data_root: str | Path,
    bundle_manifest_path: str | Path,
    secret_store_path: str | Path | None = None,
    verifier_id: str = "upbit-independent-ed25519-verifier-v1",
    key_id: str = "upbit-independent-ed25519-key-v1",
    authority_journal_id: str = "upbit-independent-authority-journal-v1",
    principal_reader: Callable[[], str] = _principal,
    credential_reader: Callable[[Path], tuple[str, str]] | None = None,
    acl_hardener: Callable[[Path, Path, Path, str, str], bool] = _harden_acl,
    protected_bundle_verifier: Callable[..., Mapping[str, Any]] = (
        verify_protected_authority_bundle
    ),
) -> dict[str, Any]:
    """Provision a private authority root and public trader-only material.

    Provisioning must be executed as the distinct authority principal.  The
    authority principal must have its own DPAPI secret store containing the
    Upbit credential pair; secrets are used only to derive pinned hashes.
    """

    private = Path(private_root).expanduser().absolute()
    public = Path(public_root).expanduser().absolute()
    workspace = Path(workspace_root).expanduser().absolute()
    trader_data = Path(trader_data_root).expanduser().absolute()
    current = _text(principal_reader())
    authority_user = _text(authority_principal)
    trader_user = _text(trader_principal)
    if (
        not current
        or not authority_user
        or not trader_user
        or current.casefold() != authority_user.casefold()
        or authority_user.casefold() == trader_user.casefold()
    ):
        raise UpbitIndependentAuthorityError(
            "authority-distinct-current-principal-required"
        )
    if any(path.is_symlink() for path in (private, public, workspace, trader_data)):
        raise UpbitIndependentAuthorityError("authority-root-symlink-forbidden")
    if (
        private == public
        or _is_within(private, public)
        or _is_within(public, private)
        or _is_within(private, workspace)
        or _is_within(private, trader_data)
    ):
        raise UpbitIndependentAuthorityError("authority-private-root-not-separated")
    owner_hash = _require_hash(
        server_owner_identity_sha256, "server-owner-identity-sha256"
    )
    for value, label in (
        (verifier_id, "verifier-id"),
        (key_id, "key-id"),
        (authority_journal_id, "authority-journal-id"),
    ):
        _require_id(value, label)
    python_path = Path(canonical_python_executable).expanduser().resolve()
    if not python_path.is_file() or python_path.is_symlink():
        raise UpbitIndependentAuthorityError("canonical-python-executable-invalid")
    bundle = dict(
        protected_bundle_verifier(
            manifest_path=bundle_manifest_path,
            workspace_root=workspace,
            canonical_python_executable=python_path,
            authority_entrypoint=Path(__file__).resolve(),
        )
    )
    expected_bundle_fields = {
        "schemaVersion",
        "manifestPath",
        "manifestSha256",
        "workspaceRoot",
        "canonicalPythonExecutable",
        "authorityEntrypointPath",
        "authorityEntrypointSha256",
        "aclExclusive",
        "sealed",
        "restartVerifiable",
    }
    if (
        set(bundle) != expected_bundle_fields
        or bundle.get("schemaVersion") != BUNDLE_MANIFEST_SCHEMA
        or bundle.get("workspaceRoot") != str(workspace)
        or bundle.get("canonicalPythonExecutable") != str(python_path)
        or bundle.get("authorityEntrypointPath")
        != str(Path(__file__).resolve())
        or any(
            bundle.get(field) is not True
            for field in ("aclExclusive", "sealed", "restartVerifiable")
        )
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-verification-invalid"
        )
    _require_hash(bundle.get("manifestSha256"), "bundle-manifest-sha256")
    _require_hash(
        bundle.get("authorityEntrypointSha256"),
        "authority-entrypoint-sha256",
    )
    shared_locks = Path(process_lock_directory).expanduser().resolve()
    if (
        not shared_locks.is_dir()
        or shared_locks.is_symlink()
        or _is_within(shared_locks, private)
    ):
        raise UpbitIndependentAuthorityError(
            "authority-shared-process-lock-directory-invalid"
        )
    store_path = Path(
        secret_store_path or default_secret_store_path()
    ).expanduser().resolve()
    if credential_reader is None:
        store = SecretStore(store_path)
        credential_reader = lambda _path: (
            _text(store.get(live_secret_name("UPBIT_ACCESS_KEY"))),
            _text(store.get(live_secret_name("UPBIT_SECRET_KEY"))),
        )
    access_key, secret_key = credential_reader(store_path)
    account_fingerprint = upbit_credential_fingerprint(access_key)
    credential_binding = upbit_spot_credential_binding_sha256(
        access_key, secret_key
    )
    if not account_fingerprint or not credential_binding:
        raise UpbitIndependentAuthorityError(
            "authority-dpapi-upbit-credentials-missing"
        )

    proof_directory = public / "proof-outbox"
    paths = AuthorityPaths(
        private_root=private,
        public_root=public,
        proof_directory=proof_directory,
        private_key=private / "authority-private.pem",
        public_key=public / "authority-public.pem",
        verifier_pin=public / "verifier-pin.json",
        authority_config=private / "authority-config.json",
        public_config=public / "trader-public-config.json",
        database=private / "authority.sqlite3",
        myorder_database=private / "myorder.sqlite3",
        proof_loss=private / "proof-loss.json",
        process_lock_directory=shared_locks,
    )
    if private.exists() or public.exists():
        raise UpbitIndependentAuthorityError("authority-provision-root-exists")
    private.mkdir(parents=True, mode=0o700)
    public.mkdir(parents=True, mode=0o755)
    proof_directory.mkdir(mode=0o770)
    private_key = ECC.generate(curve="Ed25519")
    private_pem = private_key.export_key(
        format="PEM", passphrase=None, use_pkcs8=True
    ).encode("ascii")
    public_pem = private_key.public_key().export_key(format="PEM").encode("ascii")
    try:
        descriptor = os.open(
            str(paths.private_key), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(private_pem)
            handle.flush()
            os.fsync(handle.fileno())
        descriptor = os.open(
            str(paths.public_key), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(public_pem)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        private_pem = b""
        access_key = ""
        secret_key = ""
    verifier = PinnedEd25519UpbitAccountExclusivityVerifier(
        public_key=public_pem,
        verifier_id=verifier_id,
        key_id=key_id,
        authority_journal_id=authority_journal_id,
        expected_account_fingerprint=account_fingerprint,
        expected_credential_binding_sha256=credential_binding,
        expected_server_owner_identity_sha256=owner_hash,
    )
    pin = dict(verifier.identity())
    _write_new(paths.verifier_pin, pin, private=False)
    public_body = {
        "schemaVersion": PUBLIC_CONFIG_SCHEMA,
        "proofDirectory": str(paths.proof_directory),
        "cursorDatabase": str(public / "consumer-cursor.sqlite3"),
        "publicKey": str(paths.public_key),
        "verifierPin": str(paths.verifier_pin),
        "verifierId": verifier_id,
        "keyId": key_id,
        "authorityJournalId": authority_journal_id,
        "accountFingerprint": account_fingerprint,
        "credentialBindingSha256": credential_binding,
        "serverOwnerIdentitySha256": owner_hash,
        "processLockDirectory": str(paths.process_lock_directory),
        "traderEnvironment": {
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_PROOF_DIR": str(
                paths.proof_directory
            ),
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_CURSOR_DB": str(
                public / "consumer-cursor.sqlite3"
            ),
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_PUBLIC_KEY": str(
                paths.public_key
            ),
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_VERIFIER_PIN": str(
                paths.verifier_pin
            ),
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_VERIFIER_ID": verifier_id,
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_KEY_ID": key_id,
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_AUTHORITY_JOURNAL_ID": (
                authority_journal_id
            ),
            "LIVE_TRADER_PROCESS_LOCK_DIR": str(
                paths.process_lock_directory
            ),
        },
        "authorityPrincipalHash": hashlib.sha256(
            authority_user.casefold().encode("utf-8")
        ).hexdigest(),
        "traderPrincipalHash": hashlib.sha256(
            trader_user.casefold().encode("utf-8")
        ).hexdigest(),
        "networkCapabilityOpen": False,
        "liveActivationReleased": False,
        "orderMutationAllowed": False,
    }
    public_config = {**public_body, "configHash": _hash(public_body)}
    _write_new(paths.public_config, public_config, private=False)
    authority_body = {
        "schemaVersion": AUTHORITY_CONFIG_SCHEMA,
        "authorityPrincipal": authority_user,
        "traderPrincipal": trader_user,
        "privateRoot": str(private),
        "publicRoot": str(public),
        "proofDirectory": str(paths.proof_directory),
        "privateKeyPath": str(paths.private_key),
        "publicKeyPath": str(paths.public_key),
        "verifierPinPath": str(paths.verifier_pin),
        "databasePath": str(paths.database),
        "myOrderDatabasePath": str(paths.myorder_database),
        "proofLossPath": str(paths.proof_loss),
        "processLockDirectory": str(paths.process_lock_directory),
        "secretStorePath": str(store_path),
        "verifierId": verifier_id,
        "keyId": key_id,
        "authorityJournalId": authority_journal_id,
        "accountFingerprint": account_fingerprint,
        "credentialBindingSha256": credential_binding,
        "serverOwnerIdentitySha256": owner_hash,
        "canonicalPythonExecutable": str(python_path),
        "canonicalApplicationArgv": ["-m", "live_trader"],
        "protectedWorkspaceRoot": str(workspace),
        "bundleManifestPath": bundle["manifestPath"],
        "bundleManifestSha256": bundle["manifestSha256"],
        "authorityEntrypointPath": bundle["authorityEntrypointPath"],
        "authorityEntrypointSha256": bundle[
            "authorityEntrypointSha256"
        ],
        "pollIntervalSeconds": POLL_INTERVAL_SECONDS,
    }
    authority_config = {**authority_body, "configHash": _hash(authority_body)}
    _write_new(paths.authority_config, authority_config, private=True)
    if not acl_hardener(
        private, public, proof_directory, authority_user, trader_user
    ):
        raise UpbitIndependentAuthorityError("authority-acl-hardening-failed")
    return {
        "schemaVersion": "upbit-independent-exclusivity-provision-result/v1",
        "authorityConfigPath": str(paths.authority_config),
        "publicConfigPath": str(paths.public_config),
        "proofDirectory": str(paths.proof_directory),
        "publicKeyFingerprintSha256": pin["keyFingerprintSha256"],
        "verifierPinHash": _hash(pin),
        "accountFingerprint": account_fingerprint,
        "credentialBindingSha256": credential_binding,
        "serverOwnerIdentitySha256": owner_hash,
        "bundleManifestSha256": bundle["manifestSha256"],
        "authorityEntrypointSha256": bundle[
            "authorityEntrypointSha256"
        ],
        "protectedBundleVerified": True,
        "privateKeyReturned": False,
        "privateKeyInTraderConfig": False,
        "distinctPrincipalRequired": True,
        "networkCapabilityOpen": False,
        "liveActivationReleased": False,
        "orderMutationAllowed": False,
    }


def load_authority_config(
    path: str | Path,
    *,
    principal_reader: Callable[[], str] = _principal,
    protected_bundle_verifier: Callable[..., Mapping[str, Any]] = (
        verify_protected_authority_bundle
    ),
) -> dict[str, Any]:
    config_path = Path(path).expanduser().resolve()
    value = _strict_json(config_path)
    expected = {
        "schemaVersion",
        "authorityPrincipal",
        "traderPrincipal",
        "privateRoot",
        "publicRoot",
        "proofDirectory",
        "privateKeyPath",
        "publicKeyPath",
        "verifierPinPath",
        "databasePath",
        "myOrderDatabasePath",
        "proofLossPath",
        "processLockDirectory",
        "secretStorePath",
        "verifierId",
        "keyId",
        "authorityJournalId",
        "accountFingerprint",
        "credentialBindingSha256",
        "serverOwnerIdentitySha256",
        "canonicalPythonExecutable",
        "canonicalApplicationArgv",
        "protectedWorkspaceRoot",
        "bundleManifestPath",
        "bundleManifestSha256",
        "authorityEntrypointPath",
        "authorityEntrypointSha256",
        "pollIntervalSeconds",
        "configHash",
    }
    body = {key: item for key, item in value.items() if key != "configHash"}
    if (
        set(value) != expected
        or value.get("schemaVersion") != AUTHORITY_CONFIG_SCHEMA
        or value.get("configHash") != _hash(body)
        or _text(principal_reader()).casefold()
        != _text(value.get("authorityPrincipal")).casefold()
        or _text(value.get("authorityPrincipal")).casefold()
        == _text(value.get("traderPrincipal")).casefold()
        or value.get("canonicalApplicationArgv") != ["-m", "live_trader"]
        or int(value.get("pollIntervalSeconds") or 0) != POLL_INTERVAL_SECONDS
    ):
        raise UpbitIndependentAuthorityError("authority-config-invalid")
    private_root = Path(value["privateRoot"]).resolve()
    public_root = Path(value["publicRoot"]).resolve()
    for field in (
        "privateKeyPath",
        "databasePath",
        "myOrderDatabasePath",
        "proofLossPath",
        "secretStorePath",
    ):
        candidate = Path(value[field]).resolve()
        if field != "secretStorePath" and not _is_within(candidate, private_root):
            raise UpbitIndependentAuthorityError("authority-private-path-escaped")
    for field in ("publicKeyPath", "verifierPinPath", "proofDirectory"):
        if not _is_within(Path(value[field]).resolve(), public_root):
            raise UpbitIndependentAuthorityError("authority-public-path-escaped")
    shared_locks = Path(value["processLockDirectory"]).resolve()
    if not shared_locks.is_dir() or shared_locks.is_symlink():
        raise UpbitIndependentAuthorityError(
            "authority-shared-process-lock-directory-invalid"
        )
    for field in (
        "accountFingerprint",
        "credentialBindingSha256",
        "serverOwnerIdentitySha256",
    ):
        _require_hash(value.get(field), field)
    for field in ("verifierId", "keyId", "authorityJournalId"):
        _require_id(value.get(field), field)
    bundle = dict(
        protected_bundle_verifier(
            manifest_path=value["bundleManifestPath"],
            workspace_root=value["protectedWorkspaceRoot"],
            canonical_python_executable=value[
                "canonicalPythonExecutable"
            ],
            authority_entrypoint=value["authorityEntrypointPath"],
        )
    )
    if (
        bundle.get("schemaVersion") != BUNDLE_MANIFEST_SCHEMA
        or bundle.get("manifestPath") != value["bundleManifestPath"]
        or bundle.get("manifestSha256") != value["bundleManifestSha256"]
        or bundle.get("workspaceRoot") != value["protectedWorkspaceRoot"]
        or bundle.get("canonicalPythonExecutable")
        != value["canonicalPythonExecutable"]
        or bundle.get("authorityEntrypointPath")
        != value["authorityEntrypointPath"]
        or bundle.get("authorityEntrypointSha256")
        != value["authorityEntrypointSha256"]
        or any(
            bundle.get(field) is not True
            for field in ("aclExclusive", "sealed", "restartVerifiable")
        )
    ):
        raise UpbitIndependentAuthorityError(
            "authority-protected-bundle-changed"
        )
    return dict(value)


_OBSERVATION_TABLE_SQL = """CREATE TABLE IF NOT EXISTS observation (
    sequence INTEGER PRIMARY KEY,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE
)"""
_SESSION_TABLE_SQL = """CREATE TABLE IF NOT EXISTS proof_session (
    session_id TEXT PRIMARY KEY,
    session_started_at TEXT NOT NULL,
    identifier_prefix TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_proof_hash TEXT NOT NULL,
    finalized INTEGER NOT NULL,
    record_hash TEXT NOT NULL
)"""
_PROOF_TABLE_SQL = """CREATE TABLE IF NOT EXISTS signed_proof (
    request_hash TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    authority_sequence INTEGER NOT NULL,
    proof_hash TEXT NOT NULL UNIQUE,
    proof_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(session_id,authority_sequence)
)"""
_META_TABLE_SQL = """CREATE TABLE IF NOT EXISTS authority_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)"""
_LOSS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS proof_loss (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    loss_json TEXT NOT NULL,
    loss_hash TEXT NOT NULL
)"""


class DurableAuthorityJournal:
    def __init__(
        self,
        *,
        database_path: str | Path,
        proof_loss_path: str | Path,
        authority_journal_id: str,
        config_hash: str,
        private_key: ECC.EccKey,
        clock: Callable[[], datetime],
    ) -> None:
        self.database_path = Path(database_path).resolve()
        self.proof_loss_path = Path(proof_loss_path).resolve()
        self.authority_journal_id = _require_id(
            authority_journal_id, "authority-journal-id"
        )
        self.config_hash = _require_hash(config_hash, "config-hash")
        if not private_key.has_private() or getattr(private_key, "curve", None) != "Ed25519":
            raise UpbitIndependentAuthorityError("authority-private-ed25519-required")
        self.private_key = private_key
        self.clock = clock
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.verify_restart()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.database_path), timeout=5, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(_META_TABLE_SQL)
            connection.execute(_OBSERVATION_TABLE_SQL)
            connection.execute(_SESSION_TABLE_SQL)
            connection.execute(_PROOF_TABLE_SQL)
            connection.execute(_LOSS_TABLE_SQL)
            connection.execute(f"PRAGMA application_id={DB_APPLICATION_ID}")
            connection.execute(f"PRAGMA user_version={DB_USER_VERSION}")
            stored = connection.execute(
                "SELECT value FROM authority_meta WHERE key='configHash'"
            ).fetchone()
            if stored is None:
                connection.execute(
                    "INSERT INTO authority_meta(key,value) VALUES('configHash',?)",
                    (self.config_hash,),
                )
                connection.execute(
                    "INSERT INTO authority_meta(key,value) VALUES('observerState','CLEAN')"
                )
                connection.execute(
                    "INSERT INTO authority_meta(key,value) VALUES('coverageStartedAt','')"
                )
            elif not hmac.compare_digest(_text(stored["value"]), self.config_hash):
                raise UpbitIndependentAuthorityError("authority-db-config-rotated")

    @staticmethod
    def _session_record(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schemaVersion": "upbit-independent-proof-session/v1",
            "sessionId": row["session_id"],
            "sessionStartedAt": row["session_started_at"],
            "identifierPrefix": row["identifier_prefix"],
            "lastSequence": int(row["last_sequence"]),
            "lastProofHash": row["last_proof_hash"],
            "finalized": bool(row["finalized"]),
        }

    def verify_restart(self) -> bool:
        with closing(self._connect()) as connection:
            if (
                int(connection.execute("PRAGMA application_id").fetchone()[0])
                != DB_APPLICATION_ID
                or int(connection.execute("PRAGMA user_version").fetchone()[0])
                != DB_USER_VERSION
                or connection.execute("PRAGMA integrity_check").fetchone()[0]
                != "ok"
            ):
                raise UpbitIndependentAuthorityError("authority-db-integrity-invalid")
            previous = ZERO_HASH
            for index, raw in enumerate(
                connection.execute("SELECT * FROM observation ORDER BY sequence"), 1
            ):
                row = dict(raw)
                payload = json.loads(row["payload_json"])
                body = {
                    "sequence": index,
                    "payload": payload,
                    "previousObservationHash": previous,
                }
                if (
                    int(row["sequence"]) != index
                    or row["previous_hash"] != previous
                    or row["event_hash"] != _hash(body)
                ):
                    raise UpbitIndependentAuthorityError(
                        "authority-observation-chain-tampered"
                    )
                previous = row["event_hash"]
            for raw in connection.execute("SELECT * FROM proof_session"):
                row = dict(raw)
                if row["record_hash"] != _hash(self._session_record(row)):
                    raise UpbitIndependentAuthorityError(
                        "authority-proof-session-tampered"
                    )
                proofs = [
                    dict(item)
                    for item in connection.execute(
                        "SELECT * FROM signed_proof WHERE session_id=? ORDER BY authority_sequence",
                        (row["session_id"],),
                    )
                ]
                prior = ZERO_HASH
                for index, proof_row in enumerate(proofs, 1):
                    proof = json.loads(proof_row["proof_json"])
                    if (
                        int(proof_row["authority_sequence"]) != index
                        or int(proof.get("authoritySequence") or 0) != index
                        or proof.get("previousAuthorityProofHash") != prior
                        or proof_row["proof_hash"] != _hash(proof)
                    ):
                        raise UpbitIndependentAuthorityError(
                            "authority-proof-chain-tampered"
                        )
                    prior = proof_row["proof_hash"]
                if (
                    len(proofs) != int(row["last_sequence"])
                    or prior != row["last_proof_hash"]
                ):
                    raise UpbitIndependentAuthorityError(
                        "authority-proof-head-tampered"
                    )
            loss = connection.execute("SELECT * FROM proof_loss").fetchone()
            if loss is not None:
                parsed = json.loads(loss["loss_json"])
                if loss["loss_hash"] != _hash(parsed):
                    raise UpbitIndependentAuthorityError("authority-proof-loss-tampered")
                if not self.proof_loss_path.is_file() or self.proof_loss_path.is_symlink():
                    raise UpbitIndependentAuthorityError("authority-proof-loss-file-missing")
                if _strict_json(self.proof_loss_path) != parsed:
                    raise UpbitIndependentAuthorityError("authority-proof-loss-file-mismatch")
        return True

    def _meta(self, connection: sqlite3.Connection, key: str) -> str:
        row = connection.execute(
            "SELECT value FROM authority_meta WHERE key=?", (key,)
        ).fetchone()
        return _text(row["value"]) if row is not None else ""

    def begin_observer(self, *, coverage_started_at: datetime) -> None:
        started = _utc_text(coverage_started_at)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            if self._meta(connection, "observerState") == "RUNNING":
                active = connection.execute(
                    "SELECT session_id FROM proof_session WHERE finalized=0"
                ).fetchone()
                if active is not None:
                    connection.rollback()
                    self.latch_loss(
                        "OBSERVER_RESTART_DURING_ACTIVE_SESSION",
                        {"activeSessionHash": hashlib.sha256(
                            _text(active["session_id"]).encode("utf-8")
                        ).hexdigest()},
                    )
                    raise UpbitIndependentAuthorityError(
                        "authority-unclean-observer-restart"
                    )
                # No trader session had been admitted, so the lost observer
                # cannot create a false continuous-session claim.  Start a
                # new coverage interval and keep all prior high-water rows.
                connection.execute(
                    "UPDATE authority_meta SET value='CLEAN' WHERE key='observerState'"
                )
            connection.execute(
                "UPDATE authority_meta SET value='RUNNING' WHERE key='observerState'"
            )
            connection.execute(
                "UPDATE authority_meta SET value=? WHERE key='coverageStartedAt'",
                (started,),
            )
            connection.commit()

    def end_observer(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT session_id FROM proof_session WHERE finalized=0"
            ).fetchone()
            if active is not None:
                connection.rollback()
                self.latch_loss(
                    "OBSERVER_STOP_DURING_ACTIVE_SESSION",
                    {"activeSessionHash": hashlib.sha256(
                        _text(active["session_id"]).encode("utf-8")
                    ).hexdigest()},
                )
                return
            connection.execute(
                "UPDATE authority_meta SET value='CLEAN' WHERE key='observerState'"
            )
            connection.commit()

    def loss(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT loss_json FROM proof_loss").fetchone()
        return json.loads(row["loss_json"]) if row is not None else None

    def latch_loss(self, reason_code: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
        reason = _text(reason_code).upper()
        if _PHASE_RE.fullmatch(reason) is None:
            reason = "AUTHORITY_PROOF_LOSS"
        existing = self.loss()
        if existing is not None:
            return existing
        observed_at = _utc_text(self.clock())
        with closing(self._connect()) as connection:
            last = connection.execute(
                "SELECT event_hash FROM observation ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            observation_head = _text(last["event_hash"]) if last else ZERO_HASH
        signed = {
            "schemaVersion": LOSS_SCHEMA,
            "authorityJournalId": self.authority_journal_id,
            "occurredAt": observed_at,
            "reasonCode": reason,
            "evidenceHash": _hash(dict(evidence)),
            "previousObservationHash": observation_head,
        }
        signature = eddsa.new(self.private_key, "rfc8032").sign(
            canonical_exclusivity_signature_message(signed)
        )
        loss = {
            **signed,
            "payloadHash": _hash(signed),
            "signature": base64.urlsafe_b64encode(signature)
            .rstrip(b"=")
            .decode("ascii"),
        }
        loss_hash = _hash(loss)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT OR IGNORE INTO proof_loss(singleton,loss_json,loss_hash) VALUES(1,?,?)",
                (_canonical_bytes(loss).decode("utf-8"), loss_hash),
            )
            connection.commit()
        _replace_exact(self.proof_loss_path, loss, private=True)
        return loss

    def active_session(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM proof_session WHERE finalized=0"
            ).fetchone()
        return dict(row) if row is not None else None

    def record_observation(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(payload)
        required = {
            "schemaVersion",
            "observedAt",
            "apiKeyInventory",
            "orderAudit",
            "streamAudit",
            "botAudit",
            "transport",
        }
        if set(value) != required or value.get("schemaVersion") != OBSERVATION_SCHEMA:
            raise UpbitIndependentAuthorityError("authority-observation-fields-invalid")
        observed_at = _utc(value.get("observedAt"), "observation-time")
        api = value.get("apiKeyInventory")
        order = value.get("orderAudit")
        stream = value.get("streamAudit")
        bot = value.get("botAudit")
        transport = value.get("transport")
        if not all(isinstance(item, Mapping) for item in (api, order, stream, bot, transport)):
            raise UpbitIndependentAuthorityError("authority-observation-component-invalid")
        api = dict(api)
        order = dict(order)
        stream = dict(stream)
        bot = dict(bot)
        transport = dict(transport)
        failure = ""
        if (
            api.get("complete") is not True
            or _count(api, "activeApiKeyCount") != 1
            or _count(api, "authorizedFunctionalApiKeyCount") != 1
            or _count(api, "otherActiveApiKeyCount") != 0
        ):
            failure = "API_KEY_INVENTORY_DRIFT"
        elif (
            stream.get("connected") is not True
            or stream.get("authenticated") is not True
            or stream.get("allMarketsSubscribed") is not True
            or stream.get("continuous") is not True
            or stream.get("gapDetected") is not False
        ):
            failure = "MYORDER_STREAM_GAP"
        elif (
            order.get("complete") is not True
            or _count(order, "foreignOrderCount") != 0
        ):
            failure = "FOREIGN_ACCOUNT_ORDER_ACTIVITY"
        elif any(
            _count(transport, field) != 0
            for field in ("retryCount", "redirectCount", "mutationAttemptCount")
        ):
            failure = "GET_ONLY_TRANSPORT_CONTRACT_LOST"
        active = self.active_session()
        if active is not None and (
            bot.get("complete") is not True
            or _count(bot, "activeBotCount") != 1
            or _count(bot, "authorizedFunctionalBotCount") != 1
            or _count(bot, "otherActiveBotCount") != 0
        ):
            failure = failure or "AUTHORIZED_BOT_PROCESS_DRIFT"
        if failure:
            self.latch_loss(failure, {"observationHash": _hash(value)})
            raise UpbitIndependentAuthorityError("authority-proof-loss:" + failure)
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT sequence,observed_at,event_hash FROM observation ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence = int(prior["sequence"]) + 1 if prior is not None else 1
            previous_hash = _text(prior["event_hash"]) if prior is not None else ZERO_HASH
            if prior is not None and observed_at < _utc(prior["observed_at"], "prior-observation"):
                connection.rollback()
                self.latch_loss("OBSERVATION_TIME_ROLLBACK", {"candidateHash": _hash(value)})
                raise UpbitIndependentAuthorityError("authority-observation-time-rollback")
            body = {
                "sequence": sequence,
                "payload": value,
                "previousObservationHash": previous_hash,
            }
            event_hash = _hash(body)
            connection.execute(
                "INSERT INTO observation(sequence,observed_at,payload_json,previous_hash,event_hash) VALUES(?,?,?,?,?)",
                (
                    sequence,
                    _utc_text(observed_at),
                    _canonical_bytes(value).decode("utf-8"),
                    previous_hash,
                    event_hash,
                ),
            )
            connection.commit()
        return {"sequence": sequence, "eventHash": event_hash, "observedAt": _utc_text(observed_at)}

    def latest_observation(self) -> tuple[dict[str, Any], str]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT payload_json,event_hash FROM observation ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        if row is None:
            raise UpbitIndependentAuthorityError("authority-observation-missing")
        return json.loads(row["payload_json"]), _text(row["event_hash"])

    def coverage_started_at(self) -> datetime:
        with closing(self._connect()) as connection:
            value = self._meta(connection, "coverageStartedAt")
        return _utc(value, "coverage-started-at")

    def stored_proof(self, request_hash: str) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT proof_json FROM signed_proof WHERE request_hash=?",
                (request_hash,),
            ).fetchone()
        return json.loads(row["proof_json"]) if row is not None else None

    def store_proof(self, proof: Mapping[str, Any]) -> None:
        value = dict(proof)
        request_hash = _require_hash(value.get("proofRequestHash"), "proof-request-hash")
        session_id = _require_id(value.get("sessionId"), "proof-session-id")
        sequence = int(value.get("authoritySequence") or 0)
        proof_hash = _hash(value)
        session_started_at = _text(value.get("sessionStartedAt"))
        prefix = upbit_functional_session_identifier_prefix(session_id)
        finalized = value.get("phase") == "FINAL"
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT proof_json FROM signed_proof WHERE request_hash=?",
                (request_hash,),
            ).fetchone()
            if existing is not None:
                if json.loads(existing["proof_json"]) != value:
                    raise UpbitIndependentAuthorityError("authority-proof-request-conflict")
                connection.rollback()
                return
            row = connection.execute(
                "SELECT * FROM proof_session WHERE session_id=?", (session_id,)
            ).fetchone()
            active_other = connection.execute(
                "SELECT session_id FROM proof_session WHERE finalized=0 AND session_id<>?",
                (session_id,),
            ).fetchone()
            if active_other is not None:
                raise UpbitIndependentAuthorityError("authority-concurrent-session-forbidden")
            if row is None:
                if sequence != 1 or value.get("phase") != "BASELINE" or value.get("previousAuthorityProofHash") != ZERO_HASH:
                    raise UpbitIndependentAuthorityError("authority-proof-genesis-invalid")
            else:
                current = dict(row)
                if (
                    bool(current["finalized"])
                    or sequence != int(current["last_sequence"]) + 1
                    or value.get("previousAuthorityProofHash") != current["last_proof_hash"]
                    or session_started_at != current["session_started_at"]
                ):
                    raise UpbitIndependentAuthorityError("authority-proof-chain-invalid")
            connection.execute(
                "INSERT INTO signed_proof(request_hash,session_id,authority_sequence,proof_hash,proof_json,created_at) VALUES(?,?,?,?,?,?)",
                (
                    request_hash,
                    session_id,
                    sequence,
                    proof_hash,
                    _canonical_bytes(value).decode("utf-8"),
                    _utc_text(self.clock()),
                ),
            )
            record = {
                "session_id": session_id,
                "session_started_at": session_started_at,
                "identifier_prefix": prefix,
                "last_sequence": sequence,
                "last_proof_hash": proof_hash,
                "finalized": 1 if finalized else 0,
            }
            record_hash = _hash(self._session_record(record))
            connection.execute(
                """INSERT INTO proof_session(session_id,session_started_at,identifier_prefix,last_sequence,last_proof_hash,finalized,record_hash)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(session_id) DO UPDATE SET
                   last_sequence=excluded.last_sequence,
                   last_proof_hash=excluded.last_proof_hash,
                   finalized=excluded.finalized,
                   record_hash=excluded.record_hash""",
                (
                    session_id,
                    session_started_at,
                    prefix,
                    sequence,
                    proof_hash,
                    1 if finalized else 0,
                    record_hash,
                ),
            )
            connection.commit()


def _component(
    *,
    name: str,
    account_fingerprint: str,
    credential_binding_sha256: str,
    server_owner_identity_sha256: str,
    coverage_started_at: str,
    coverage_ended_at: str,
    observation_head_hash: str,
) -> dict[str, Any]:
    if name == "apiKeyInventory":
        schema = "upbit-account-api-key-inventory-evidence/v1"
        source = ACCOUNT_API_KEY_INVENTORY_SOURCE
        values: dict[str, Any] = {
            "activeApiKeyCount": 1,
            "authorizedFunctionalApiKeyCount": 1,
            "otherActiveApiKeyCount": 0,
            "authorizedCredentialBindingSha256": credential_binding_sha256,
        }
    elif name == "manualTradeAudit":
        schema = "upbit-account-manual-trade-audit-evidence/v1"
        source = ACCOUNT_MANUAL_TRADE_AUDIT_SOURCE
        values = {"manualOrderCount": 0}
    else:
        schema = "upbit-account-bot-registry-evidence/v1"
        source = ACCOUNT_BOT_REGISTRY_SOURCE
        values = {
            "activeBotCount": 1,
            "authorizedFunctionalBotCount": 1,
            "otherActiveBotCount": 0,
            "authorizedServerOwnerIdentitySha256": server_owner_identity_sha256,
        }
    artifact = _hash(
        {
            "schemaVersion": "upbit-independent-observer-high-water-binding/v1",
            "component": name,
            "observationHeadHash": observation_head_hash,
            "coverageStartedAt": coverage_started_at,
            "coverageEndedAt": coverage_ended_at,
        }
    )
    body = {
        "schemaVersion": schema,
        "source": source,
        "accountFingerprint": account_fingerprint,
        "coverageStartedAt": coverage_started_at,
        "coverageEndedAt": coverage_ended_at,
        "complete": True,
        "independentlyVerified": True,
        "continuousCoverage": True,
        **values,
        "authorityArtifactHash": artifact,
    }
    return {**body, "evidenceHash": _hash(body)}


class IndependentProofSigner:
    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        verifier_pin: Mapping[str, Any],
        private_key: ECC.EccKey,
        journal: DurableAuthorityJournal,
        clock: Callable[[], datetime],
    ) -> None:
        self.config = dict(config)
        self.verifier_pin = dict(verifier_pin)
        self.private_key = private_key
        self.journal = journal
        self.clock = clock

    def validate_envelope(
        self, path: Path, *, allow_stale_stored: bool = False
    ) -> dict[str, Any]:
        envelope = _strict_json(path)
        if set(envelope) != {
            "schemaVersion",
            "authorityJournalId",
            "verifierPinHash",
            "request",
            "contentHash",
        }:
            raise UpbitIndependentAuthorityError("authority-outbox-fields-invalid")
        body = {key: item for key, item in envelope.items() if key != "contentHash"}
        if (
            envelope.get("schemaVersion") != OUTBOX_SCHEMA
            or envelope.get("authorityJournalId") != self.config["authorityJournalId"]
            or envelope.get("verifierPinHash") != _hash(self.verifier_pin)
            or envelope.get("contentHash") != _hash(body)
            or not isinstance(envelope.get("request"), Mapping)
        ):
            raise UpbitIndependentAuthorityError("authority-outbox-binding-invalid")
        request = dict(envelope["request"])
        expected = {
            "schemaVersion",
            "sessionId",
            "phase",
            "accountFingerprint",
            "credentialBindingSha256",
            "serverOwnerIdentitySha256",
            "sessionStartedAt",
            "observationStartedAt",
            "observedAt",
            "proofRequestHash",
        }
        unsigned = {key: item for key, item in request.items() if key != "proofRequestHash"}
        phase = _text(request.get("phase"))
        if (
            set(request) != expected
            or request.get("schemaVersion")
            != ACCOUNT_EXCLUSIVITY_PROOF_REQUEST_SCHEMA_VERSION
            or _PHASE_RE.fullmatch(phase) is None
            or request.get("proofRequestHash") != _hash(unsigned)
            or path.name != request["proofRequestHash"] + ".request.json"
        ):
            raise UpbitIndependentAuthorityError("authority-proof-request-invalid")
        exact = {
            "accountFingerprint": self.config["accountFingerprint"],
            "credentialBindingSha256": self.config["credentialBindingSha256"],
            "serverOwnerIdentitySha256": self.config["serverOwnerIdentitySha256"],
        }
        if any(request.get(field) != expected_value for field, expected_value in exact.items()):
            raise UpbitIndependentAuthorityError("authority-proof-request-identity-mismatch")
        session_id = _require_id(request.get("sessionId"), "request-session-id")
        started = _utc(request.get("sessionStartedAt"), "request-session-started")
        observation_started = _utc(
            request.get("observationStartedAt"), "request-observation-started"
        )
        observed = _utc(request.get("observedAt"), "request-observed")
        now = _utc(self.clock(), "authority-current-time")
        request_is_stale = (now - observed).total_seconds() > MAX_REQUEST_AGE_SECONDS
        if (
            started > observation_started
            or observation_started > observed
            or observed > now + timedelta(seconds=1)
            or (
                request_is_stale
                and not (
                    allow_stale_stored
                    and self.journal.stored_proof(request["proofRequestHash"])
                    is not None
                )
            )
            or self.journal.coverage_started_at() > started
            or upbit_functional_session_identifier_prefix(session_id)
            != "uft-" + hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:8] + "-"
        ):
            raise UpbitIndependentAuthorityError("authority-proof-request-time-invalid")
        return request

    def build_proof(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self.journal.loss() is not None:
            raise UpbitIndependentAuthorityError("authority-proof-loss-latched")
        request_hash = _require_hash(request.get("proofRequestHash"), "request-hash")
        existing = self.journal.stored_proof(request_hash)
        if existing is not None:
            return existing
        observation, observation_head = self.journal.latest_observation()
        observed = _utc(request.get("observedAt"), "proof-observed-at")
        if _utc(observation.get("observedAt"), "latest-observation") < observed:
            raise UpbitIndependentAuthorityError("authority-observation-high-water-behind")
        active = self.journal.active_session()
        phase = _text(request.get("phase"))
        if active is None:
            if phase != "BASELINE":
                raise UpbitIndependentAuthorityError("authority-baseline-proof-required")
            sequence = 1
            previous = ZERO_HASH
        else:
            if active["session_id"] != request["sessionId"] or phase == "BASELINE":
                raise UpbitIndependentAuthorityError("authority-proof-session-mismatch")
            sequence = int(active["last_sequence"]) + 1
            previous = _text(active["last_proof_hash"])
        for name in ("apiKeyInventory", "orderAudit", "streamAudit", "botAudit", "transport"):
            if not isinstance(observation.get(name), Mapping):
                raise UpbitIndependentAuthorityError("authority-observation-incomplete")
        api = observation["apiKeyInventory"]
        order = observation["orderAudit"]
        stream = observation["streamAudit"]
        bot = observation["botAudit"]
        transport = observation["transport"]
        if not (
            api.get("complete") is True
            and _count(api, "activeApiKeyCount") == 1
            and _count(api, "authorizedFunctionalApiKeyCount") == 1
            and _count(api, "otherActiveApiKeyCount") == 0
            and order.get("complete") is True
            and _count(order, "foreignOrderCount") == 0
            and stream.get("continuous") is True
            and stream.get("gapDetected") is False
            and stream.get("allMarketsSubscribed") is True
            and bot.get("complete") is True
            and _count(bot, "activeBotCount") == 1
            and _count(bot, "authorizedFunctionalBotCount") == 1
            and _count(bot, "otherActiveBotCount") == 0
            and all(
                _count(transport, field) == 0
                for field in ("retryCount", "redirectCount", "mutationAttemptCount")
            )
        ):
            raise UpbitIndependentAuthorityError("authority-observation-not-provable")
        coverage_started = _utc_text(request["sessionStartedAt"])
        coverage_ended = _utc_text(request["observedAt"])
        signed = {
            "schemaVersion": ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION_V2,
            "sessionId": request["sessionId"],
            "phase": phase,
            "accountFingerprint": self.config["accountFingerprint"],
            "credentialBindingSha256": self.config["credentialBindingSha256"],
            "serverOwnerIdentitySha256": self.config["serverOwnerIdentitySha256"],
            "sessionStartedAt": coverage_started,
            "observationStartedAt": _utc_text(request["observationStartedAt"]),
            "observedAt": coverage_ended,
            "proofRequestHash": request_hash,
            "authorityJournalId": self.config["authorityJournalId"],
            "authoritySequence": sequence,
            "previousAuthorityProofHash": previous,
            "authority": dict(self.verifier_pin),
            "apiKeyInventory": _component(
                name="apiKeyInventory",
                account_fingerprint=self.config["accountFingerprint"],
                credential_binding_sha256=self.config["credentialBindingSha256"],
                server_owner_identity_sha256=self.config["serverOwnerIdentitySha256"],
                coverage_started_at=coverage_started,
                coverage_ended_at=coverage_ended,
                observation_head_hash=observation_head,
            ),
            "manualTradeAudit": _component(
                name="manualTradeAudit",
                account_fingerprint=self.config["accountFingerprint"],
                credential_binding_sha256=self.config["credentialBindingSha256"],
                server_owner_identity_sha256=self.config["serverOwnerIdentitySha256"],
                coverage_started_at=coverage_started,
                coverage_ended_at=coverage_ended,
                observation_head_hash=observation_head,
            ),
            "botRegistry": _component(
                name="botRegistry",
                account_fingerprint=self.config["accountFingerprint"],
                credential_binding_sha256=self.config["credentialBindingSha256"],
                server_owner_identity_sha256=self.config["serverOwnerIdentitySha256"],
                coverage_started_at=coverage_started,
                coverage_ended_at=coverage_ended,
                observation_head_hash=observation_head,
            ),
        }
        signature = eddsa.new(self.private_key, "rfc8032").sign(
            canonical_exclusivity_signature_message(signed)
        )
        proof = {
            **signed,
            "payloadHash": _hash(signed),
            "signature": base64.urlsafe_b64encode(signature)
            .rstrip(b"=")
            .decode("ascii"),
        }
        self.journal.store_proof(proof)
        return proof

    def sign_request_file(self, path: Path) -> Path:
        request = self.validate_envelope(path)
        proof = self.build_proof(request)
        proof_path = path.with_name(request["proofRequestHash"] + ".json")
        encoded = _canonical_bytes(proof)
        if proof_path.exists():
            if proof_path.is_symlink() or proof_path.read_bytes() != encoded:
                raise UpbitIndependentAuthorityError("authority-proof-file-conflict")
            return proof_path
        temporary = proof_path.with_name(proof_path.name + f".{os.getpid()}.tmp")
        try:
            descriptor = os.open(
                str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, proof_path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return proof_path


class ExactGetOnlySender:
    def __init__(self, *, network_capability: object) -> None:
        self.physical_get_count = 0
        self.redirect_count = 0
        self._network_capability = network_capability

    def __call__(self, request: PreparedRequest) -> Mapping[str, Any]:
        parsed = urlsplit(request.url)
        if (
            request.method != "GET"
            or request.body is not None
            or request.provider != "upbit-functional-read"
            or f"{parsed.scheme}://{parsed.netloc}" != OFFICIAL_ORIGIN
            or parsed.path != request.endpoint
            or request.safe_headers.get("authorization_configured") is not True
        ):
            raise UpbitIndependentAuthorityError("authority-get-shape-invalid")
        self.physical_get_count += 1
        response = dict(
            send_prepared_request(
                request,
                timeout_seconds=10.0,
                network_capability=self._network_capability,
            )
        )
        if response.get("redirectBlocked") is True:
            self.redirect_count += 1
            raise UpbitIndependentAuthorityError("authority-get-redirect-blocked")
        return response


def _rows(value: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise UpbitIndependentAuthorityError(label + "-invalid")
    return [dict(row) for row in value]


def _read_open(client: OfficialUpbitFunctionalGetClient) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in range(1, MAX_OPEN_PAGES + 1):
        current = _rows(
            client.get(
                UPBIT_OPEN_ORDERS_ENDPOINT,
                (
                    ("states[]", "wait"),
                    ("states[]", "watch"),
                    ("page", str(page)),
                    ("limit", str(OPEN_PAGE_LIMIT)),
                    ("order_by", "asc"),
                ),
            ),
            "authority-open-orders",
        )
        rows.extend(current)
        if len(current) < OPEN_PAGE_LIMIT:
            return rows
    raise UpbitIndependentAuthorityError("authority-open-orders-pagination-exhausted")


def _read_closed(
    client: OfficialUpbitFunctionalGetClient, *, now: datetime
) -> list[dict[str, Any]]:
    start = now - timedelta(days=7) + timedelta(seconds=1)
    rows = _rows(
        client.get(
            UPBIT_CLOSED_ORDERS_ENDPOINT,
            (
                ("states[]", "done"),
                ("states[]", "cancel"),
                ("start_time", _utc_text(start)),
                ("end_time", _utc_text(now)),
                ("limit", str(CLOSED_LIMIT)),
                ("order_by", "asc"),
            ),
        ),
        "authority-closed-orders",
    )
    if len(rows) >= CLOSED_LIMIT:
        raise UpbitIndependentAuthorityError("authority-closed-orders-truncated")
    return rows


def _api_inventory(
    client: OfficialUpbitFunctionalGetClient,
    *,
    access_key: str,
    now: datetime,
) -> dict[str, Any]:
    rows = _rows(client.get(UPBIT_API_KEYS_ENDPOINT, ()), "authority-api-keys")
    active = authorized = 0
    seen: set[str] = set()
    expiry_hashes: list[str] = []
    for row in rows:
        raw_key = _text(row.get("access_key"))
        expires_text = _text(row.get("expire_at"))
        if not raw_key or not expires_text or raw_key in seen:
            raise UpbitIndependentAuthorityError("authority-api-key-row-invalid")
        seen.add(raw_key)
        expires = _utc(expires_text, "api-key-expiry")
        expiry_hashes.append(hashlib.sha256(expires_text.encode("utf-8")).hexdigest())
        if expires > now:
            active += 1
            if hmac.compare_digest(raw_key, access_key):
                authorized += 1
    return {
        "complete": True,
        "activeApiKeyCount": active,
        "authorizedFunctionalApiKeyCount": authorized,
        "otherActiveApiKeyCount": active - authorized,
        "expirySummaryHash": _hash(sorted(expiry_hashes)),
    }


def _windows_processes() -> list[dict[str, Any]]:
    if os.name != "nt":
        raise UpbitIndependentAuthorityError("authority-process-audit-windows-required")
    command = (
        "Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ExecutablePath,CommandLine | "
        "ConvertTo-Json -Compress"
    )
    try:
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        value = json.loads(completed.stdout or "[]")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise UpbitIndependentAuthorityError("authority-process-enumeration-failed") from exc
    rows = value if isinstance(value, list) else [value]
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _command_line_argv(command_line: str) -> list[str]:
    if os.name != "nt":
        return []
    import ctypes

    shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    shell32.CommandLineToArgvW.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int)]
    shell32.CommandLineToArgvW.restype = ctypes.POINTER(ctypes.c_wchar_p)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    count = ctypes.c_int()
    pointer = shell32.CommandLineToArgvW(command_line, ctypes.byref(count))
    if not pointer:
        raise UpbitIndependentAuthorityError("authority-command-line-parse-failed")
    try:
        return [pointer[index] for index in range(count.value)]
    finally:
        kernel32.LocalFree(pointer)


def audit_live_trader_process(
    *,
    account_fingerprint: str,
    canonical_python_executable: str | Path,
    process_reader: Callable[[], list[dict[str, Any]]] = _windows_processes,
    lease_acquirer: Callable[[str], Any] = acquire_process_lease,
    lock_root_reader: Callable[[], Path] = process_lock_root,
) -> dict[str, Any]:
    scopes = (
        _APPLICATION_INSTANCE_SCOPE,
        f"crypto-first-live-account:UPBIT:{account_fingerprint}",
    )
    owner_pids: list[int] = []
    for scope in scopes:
        lease = lease_acquirer(scope)
        if lease is not None:
            try:
                lease.release()
            finally:
                pass
            return {
                "complete": True,
                "activeBotCount": 0,
                "authorizedFunctionalBotCount": 0,
                "otherActiveBotCount": 0,
                "processAuditHash": _hash({"reason": "required-lease-not-held"}),
            }
        path = lock_root_reader() / _safe_lock_name(scope)
        metadata = _owner_metadata(path)
        pid = int(metadata.get("pid") or 0)
        if pid <= 0:
            raise UpbitIndependentAuthorityError("authority-lease-owner-metadata-missing")
        owner_pids.append(pid)
    if len(set(owner_pids)) != 1:
        raise UpbitIndependentAuthorityError("authority-lease-owner-pid-mismatch")
    expected_python = str(Path(canonical_python_executable).resolve()).casefold()
    matches: list[dict[str, Any]] = []
    for row in process_reader():
        executable = _text(row.get("ExecutablePath"))
        command_line = _text(row.get("CommandLine"))
        if not executable or not command_line:
            continue
        argv = _command_line_argv(command_line)
        if (
            str(Path(executable).resolve()).casefold() == expected_python
            and len(argv) == 3
            and str(Path(argv[0]).resolve()).casefold() == expected_python
            and argv[1:] == ["-m", "live_trader"]
        ):
            matches.append(
                {
                    "pid": int(row.get("ProcessId") or 0),
                    "commandHash": hashlib.sha256(command_line.encode("utf-8")).hexdigest(),
                }
            )
    authorized = sum(1 for row in matches if row["pid"] == owner_pids[0])
    return {
        "complete": True,
        "activeBotCount": len(matches),
        "authorizedFunctionalBotCount": authorized,
        "otherActiveBotCount": len(matches) - authorized,
        "processAuditHash": _hash(
            {"leaseOwnerPid": owner_pids[0], "matches": matches}
        ),
    }


@contextmanager
def _credential_environment(access_key: str, secret_key: str) -> Iterator[None]:
    previous = {
        name: os.environ.get(name)
        for name in ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY", "UPBIT_BASE_URL")
    }
    try:
        os.environ["UPBIT_ACCESS_KEY"] = access_key
        os.environ["UPBIT_SECRET_KEY"] = secret_key
        os.environ["UPBIT_BASE_URL"] = OFFICIAL_ORIGIN
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class UpbitIndependentObserverDaemon:
    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        journal: DurableAuthorityJournal,
        signer: IndependentProofSigner,
        access_key: str,
        secret_key: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        get_client_factory: Callable[..., OfficialUpbitFunctionalGetClient] = OfficialUpbitFunctionalGetClient,
        pump_factory: Callable[..., OfficialUpbitFunctionalMyOrderPump] = OfficialUpbitFunctionalMyOrderPump,
        process_auditor: Callable[..., Mapping[str, Any]] = audit_live_trader_process,
        functional_get_capability_reader: Callable[[], object] = _protected_upbit_functional_get_network_capability,
        raw_http_capability_reader: Callable[[], object] = _protected_upbit_read_only_http_network_capability,
    ) -> None:
        self.config = dict(config)
        self.journal = journal
        self.signer = signer
        self.access_key = access_key
        self.secret_key = secret_key
        self.clock = clock
        self.get_client_factory = get_client_factory
        self.pump_factory = pump_factory
        self.process_auditor = process_auditor
        self.functional_get_capability_reader = (
            functional_get_capability_reader
        )
        self.raw_http_capability_reader = raw_http_capability_reader
        self.observer_session = "upbit-authority-observer-" + secrets.token_hex(12)
        self.myorder = DurableUpbitMyOrderJournal(
            Path(self.config["myOrderDatabasePath"]), clock=self.clock
        )
        self.writer: dict[str, Any] = {}
        self.pump: OfficialUpbitFunctionalMyOrderPump | None = None
        self._terminal_served = False

    def _order_audit(
        self,
        rows: Iterable[Mapping[str, Any]],
        stream_events: Iterable[Mapping[str, Any]],
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        active = self.journal.active_session()
        if active is not None:
            session_id = _text(active.get("session_id"))
            prefix = _text(active.get("identifier_prefix"))
            started = _utc(active["session_started_at"], "active-session-start")
        elif request_context is not None:
            session_id = _require_id(
                request_context.get("sessionId"), "pending-session-id"
            )
            prefix = upbit_functional_session_identifier_prefix(session_id)
            started = _utc(
                request_context.get("sessionStartedAt"),
                "pending-session-start",
            )
        else:
            session_id = ""
            prefix = ""
            started = None
        owned = foreign = 0
        event_hashes: list[str] = []
        for raw in [*rows, *stream_events]:
            row = dict(raw)
            occurred_text = _text(
                row.get("created_at") or row.get("createdAt") or row.get("occurredAt")
            )
            if started is not None and occurred_text:
                try:
                    if _utc(occurred_text, "order-occurred") < started:
                        continue
                except UpbitIndependentAuthorityError:
                    foreign += 1
                    continue
            if not session_id:
                continue
            identifier = _text(row.get("identifier"))
            if identifier.startswith(prefix) and re.fullmatch(
                re.escape(prefix) + r"[0-9a-f]{19}", identifier
            ):
                owned += 1
            else:
                foreign += 1
            event_hashes.append(
                _hash(
                    {
                        "identifierHash": hashlib.sha256(identifier.encode("utf-8")).hexdigest(),
                        "state": _text(row.get("state")),
                        "market": _text(row.get("market") or row.get("code")),
                        "occurredAt": occurred_text,
                    }
                )
            )
        return {
            "complete": True,
            "ownedOrderCount": owned,
            "foreignOrderCount": foreign,
            "orderEventSummaryHash": _hash(sorted(event_hashes)),
        }

    def start(self) -> None:
        if os.environ.get(GET_GATE, "").strip().lower() != "true":
            raise UpbitIndependentAuthorityError("authority-get-gate-disabled")
        if any(
            os.environ.get(name, "").strip().lower() in TRUE_VALUES
            for name in (
                "LIVE_TRADER_ENABLE_REAL_ORDERS",
                "UPBIT_FUNCTIONAL_LIVE_ENABLED",
            )
        ):
            raise UpbitIndependentAuthorityError("authority-live-order-flags-must-be-false")
        configured_lock_root = Path(
            self.config["processLockDirectory"]
        ).resolve()
        if process_lock_root().resolve() != configured_lock_root:
            raise UpbitIndependentAuthorityError(
                "authority-shared-process-lock-directory-required"
            )
        if (
            upbit_credential_fingerprint(self.access_key)
            != self.config["accountFingerprint"]
            or upbit_spot_credential_binding_sha256(self.access_key, self.secret_key)
            != self.config["credentialBindingSha256"]
        ):
            raise UpbitIndependentAuthorityError("authority-credential-binding-rotated")
        started = _utc(self.clock(), "authority-start")
        self.journal.begin_observer(coverage_started_at=started)
        self.writer = self.myorder.begin_authenticated_session(
            session_id=self.observer_session,
            account_fingerprint=self.config["accountFingerprint"],
            started_at=started,
        )
        with _credential_environment(self.access_key, self.secret_key):
            self.pump = self.pump_factory(
                expected_account_fingerprint=self.config["accountFingerprint"],
                clock=self.clock,
                credential_reader=lambda: (self.access_key, self.secret_key),
                all_markets=True,
            )
            handshake = self.pump.handshake(
                session_id=self.observer_session,
                journal=self.myorder,
                writer_authority=self.writer,
            )
            if handshake.get("allMarketsSubscribed") is not True:
                raise UpbitIndependentAuthorityError("authority-all-market-stream-required")
            self.myorder.attest_authenticated_connection(
                self.observer_session,
                writer_token=self.writer["writerToken"],
                writer_generation=int(self.writer["writerGeneration"]),
            )
            self.pump.liveness()
            self.poll_once()

    def poll_once(
        self,
        *,
        terminal: bool = False,
        request_context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.pump is None:
            raise UpbitIndependentAuthorityError("authority-pump-not-started")
        now = _utc(self.clock(), "authority-poll-time")
        with _credential_environment(self.access_key, self.secret_key):
            if terminal:
                self.pump.terminal_barrier(session_id=self.observer_session)
            else:
                self.pump.liveness()
                self.myorder.observe(
                    self.observer_session,
                    writer_token=self.writer["writerToken"],
                    writer_generation=int(self.writer["writerGeneration"]),
                )
            # Both frozen releases and both identity-only capabilities are
            # acquired inside the protected observer process immediately
            # before GET construction. A held release fails before signing or
            # raw socket opener construction.
            functional_get_capability = (
                self.functional_get_capability_reader()
            )
            sender = ExactGetOnlySender(
                network_capability=self.raw_http_capability_reader()
            )
            client = self.get_client_factory(
                expected_account_fingerprint=self.config["accountFingerprint"],
                credential_fingerprint_reader=lambda: upbit_credential_fingerprint(
                    self.access_key
                ),
                sender=sender,
                network_capability=functional_get_capability,
            )
            inventory = _api_inventory(
                client, access_key=self.access_key, now=now
            )
            open_orders = _read_open(client)
            closed_orders = _read_closed(client, now=now)
        snapshot = self.myorder.snapshot(
            session_id=self.observer_session,
            identifiers=(),
        )
        stream = {
            "connected": bool(snapshot.get("connected") or terminal),
            "authenticated": snapshot.get("authenticated") is True,
            "allMarketsSubscribed": True,
            "continuous": bool(
                snapshot.get("eventsComplete") is True
                or (
                    terminal
                    and snapshot.get("gapDetected") is False
                    and snapshot.get("authenticated") is True
                )
            ),
            "gapDetected": snapshot.get("gapDetected") is True,
            "eventCursor": int(snapshot.get("eventCursor") or 0),
            "eventHeadHash": _require_hash(
                snapshot.get("eventHeadHash"), "stream-event-head"
            ),
        }
        bot = dict(
            self.process_auditor(
                account_fingerprint=self.config["accountFingerprint"],
                canonical_python_executable=self.config[
                    "canonicalPythonExecutable"
                ],
                lock_root_reader=lambda: Path(
                    self.config["processLockDirectory"]
                ),
            )
        )
        observation = {
            "schemaVersion": OBSERVATION_SCHEMA,
            "observedAt": _utc_text(now),
            "apiKeyInventory": inventory,
            "orderAudit": self._order_audit(
                [*open_orders, *closed_orders],
                snapshot.get("events") or [],
                request_context=request_context,
            ),
            "streamAudit": stream,
            "botAudit": bot,
            "transport": {
                "physicalGetAttemptCount": sender.physical_get_count,
                "authenticatedGetCount": sender.physical_get_count,
                "retryCount": 0,
                "redirectCount": sender.redirect_count,
                "mutationAttemptCount": 0,
            },
        }
        return self.journal.record_observation(observation)

    def service_requests_once(self) -> int:
        proof_directory = Path(self.config["proofDirectory"]).resolve()
        count = 0
        for path in sorted(proof_directory.glob("*.request.json")):
            request = self.signer.validate_envelope(
                path, allow_stale_stored=True
            )
            if self.journal.stored_proof(request["proofRequestHash"]) is None:
                self.poll_once(
                    terminal=request["phase"] == "FINAL",
                    request_context=request,
                )
            self.signer.sign_request_file(path)
            if request["phase"] == "FINAL":
                self._terminal_served = True
            count += 1
        return count

    def serve(self) -> None:
        self.start()
        try:
            while True:
                self.service_requests_once()
                if self._terminal_served:
                    break
                self.poll_once()
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            self.journal.latch_loss(
                "AUTHORITY_DAEMON_FAILURE",
                {"errorType": type(exc).__name__},
            )
            raise
        finally:
            if self.pump is not None:
                self.pump.close()
            self.journal.end_observer()


def build_daemon(
    config_path: str | Path,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    principal_reader: Callable[[], str] = _principal,
) -> UpbitIndependentObserverDaemon:
    config = load_authority_config(config_path, principal_reader=principal_reader)
    pin = _strict_json(Path(config["verifierPinPath"]))
    try:
        private_key = ECC.import_key(Path(config["privateKeyPath"]).read_bytes())
    except (OSError, ValueError, TypeError, IndexError) as exc:
        raise UpbitIndependentAuthorityError("authority-private-key-invalid") from exc
    store = SecretStore(Path(config["secretStorePath"]))
    access_key = _text(store.get(live_secret_name("UPBIT_ACCESS_KEY")))
    secret_key = _text(store.get(live_secret_name("UPBIT_SECRET_KEY")))
    if not access_key or not secret_key:
        raise UpbitIndependentAuthorityError("authority-dpapi-upbit-credentials-missing")
    journal = DurableAuthorityJournal(
        database_path=config["databasePath"],
        proof_loss_path=config["proofLossPath"],
        authority_journal_id=config["authorityJournalId"],
        config_hash=config["configHash"],
        private_key=private_key,
        clock=clock,
    )
    signer = IndependentProofSigner(
        config=config,
        verifier_pin=pin,
        private_key=private_key,
        journal=journal,
        clock=clock,
    )
    return UpbitIndependentObserverDaemon(
        config=config,
        journal=journal,
        signer=signer,
        access_key=access_key,
        secret_key=secret_key,
        clock=clock,
    )


def authority_status(config_path: str | Path) -> dict[str, Any]:
    config = load_authority_config(config_path)
    private_key = ECC.import_key(Path(config["privateKeyPath"]).read_bytes())
    journal = DurableAuthorityJournal(
        database_path=config["databasePath"],
        proof_loss_path=config["proofLossPath"],
        authority_journal_id=config["authorityJournalId"],
        config_hash=config["configHash"],
        private_key=private_key,
        clock=lambda: datetime.now(timezone.utc),
    )
    active = journal.active_session()
    loss = journal.loss()
    return {
        "schemaVersion": STATUS_SCHEMA,
        "durable": True,
        "restartVerifiable": journal.verify_restart(),
        "authorityJournalId": config["authorityJournalId"],
        "activeSessionHash": (
            hashlib.sha256(active["session_id"].encode("utf-8")).hexdigest()
            if active
            else ""
        ),
        "proofLossLatched": loss is not None,
        "proofLossHash": _hash(loss) if loss else "",
        "protectedBundleVerified": True,
        "bundleManifestSha256": config["bundleManifestSha256"],
        "authorityEntrypointSha256": config[
            "authorityEntrypointSha256"
        ],
        "privateKeyReturned": False,
        "networkCapabilityOpen": False,
        "liveActivationReleased": False,
        "orderMutationAllowed": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Independent GET-only Upbit exclusivity authority"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    credentials = sub.add_parser("store-credentials")
    credentials.add_argument("--authority-principal", required=True)
    credentials.add_argument("--trader-principal", required=True)
    credentials.add_argument("--secret-store-path", default="")
    provision = sub.add_parser("provision")
    provision.add_argument("--private-root", required=True)
    provision.add_argument("--public-root", required=True)
    provision.add_argument("--authority-principal", required=True)
    provision.add_argument("--trader-principal", required=True)
    provision.add_argument("--server-owner-identity-sha256", required=True)
    provision.add_argument("--canonical-python-executable", required=True)
    provision.add_argument("--process-lock-directory", required=True)
    provision.add_argument("--workspace-root", required=True)
    provision.add_argument("--trader-data-root", required=True)
    provision.add_argument("--bundle-manifest", required=True)
    provision.add_argument("--secret-store-path", default="")
    serve = sub.add_parser("serve")
    serve.add_argument("--config", required=True)
    status = sub.add_parser("status")
    status.add_argument("--config", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "store-credentials":
            result = store_authority_credentials(
                authority_principal=args.authority_principal,
                trader_principal=args.trader_principal,
                secret_store_path=args.secret_store_path or None,
            )
            print(_canonical_bytes(result).decode("utf-8"))
            return 0
        if args.command == "provision":
            result = provision_authority(
                private_root=args.private_root,
                public_root=args.public_root,
                authority_principal=args.authority_principal,
                trader_principal=args.trader_principal,
                server_owner_identity_sha256=args.server_owner_identity_sha256,
                canonical_python_executable=args.canonical_python_executable,
                process_lock_directory=args.process_lock_directory,
                workspace_root=args.workspace_root,
                trader_data_root=args.trader_data_root,
                bundle_manifest_path=args.bundle_manifest,
                secret_store_path=args.secret_store_path or None,
            )
            print(_canonical_bytes(result).decode("utf-8"))
            return 0
        if args.command == "status":
            print(_canonical_bytes(authority_status(args.config)).decode("utf-8"))
            return 0
        build_daemon(args.config).serve()
        return 0
    except Exception as exc:
        failure = {
            "schemaVersion": "upbit-independent-exclusivity-authority-failure/v1",
            "ok": False,
            "errorType": type(exc).__name__,
            "errorHash": hashlib.sha256(str(exc).encode("utf-8")).hexdigest(),
            "networkCapabilityOpen": False,
            "liveActivationReleased": False,
            "orderMutationAllowed": False,
        }
        print(_canonical_bytes(failure).decode("utf-8"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
