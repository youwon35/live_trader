from __future__ import annotations

"""Protected SYSTEM pre-listener for one Binance observer launch.

This authority is installed in an ACL-protected frozen bundle.  It accepts
one HMAC-authenticated, peer-SID-checked PREPARED_INERT plan, starts the
observer child behind a filesystem gate, acknowledges zero broker attempts,
and only then opens the gate that permits WS subscription and three GETs.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from live_trader.binance_spot_supervised_authority_protocol import (  # noqa: E402
    authority_hash,
)
from live_trader.binance_spot_supervised_observer_launch import (  # noqa: E402
    LAUNCH_ACK_SCHEMA,
    LAUNCH_COMMAND_SCHEMA,
    LAUNCH_RESPONSE_SCHEMA,
    validate_prearmed_observer_launch_request,
)
from live_trader.crypto_first_live_supervised_anchor import (  # noqa: E402
    WindowsNamedPipeSupervisedAuthorityServer,
    _windows_process_sid,
    decode_authenticated_pipe_request,
    encode_authenticated_pipe_response,
)
from scripts.binance_supervised_observer_daemon import (  # noqa: E402
    CONFIG_SCHEMA as OBSERVER_CONFIG_SCHEMA,
    PREARMED_READY_SCHEMA,
    _process_audit,
)


AUTHORITY_CONFIG_SCHEMA = (
    "binance-supervised-observer-launch-authority-config/v1"
)
BINANCE_OBSERVER_AUTHORITY_NETWORK_RELEASED = False
MAX_CONFIG_BYTES = 64 * 1024
MAX_WIRE_BYTES = 65_536
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_PIPE_RE = re.compile(r"^\\\\\.\\pipe\\[A-Za-z0-9._-]{8,120}$")
_SID_RE = re.compile(r"^S-1-(?:\d+-){1,14}\d+$", re.IGNORECASE)


class BinanceObserverAuthorityError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _strict_json(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        raise BinanceObserverAuthorityError("authority config is unavailable")
    raw = path.read_bytes()
    if not 1 <= len(raw) <= MAX_CONFIG_BYTES:
        raise BinanceObserverAuthorityError("authority config size is invalid")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BinanceObserverAuthorityError(
            "authority config JSON is invalid"
        ) from exc
    if not isinstance(value, dict):
        raise BinanceObserverAuthorityError("authority config must be an object")
    return dict(value)


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _absolute(value: object, label: str) -> Path:
    path = Path(_text(value))
    if not path.is_absolute():
        raise BinanceObserverAuthorityError(label + " must be absolute")
    return path.resolve()


def validate_authority_config(value: Mapping[str, Any]) -> dict[str, Any]:
    config = dict(value)
    fields = {
        "schemaVersion",
        "authorityId",
        "keyId",
        "authorityOsSid",
        "traderOsSid",
        "pipeAddress",
        "pipeAuthKeyPath",
        "authorityRoot",
        "bundleRoot",
        "credentialFilePath",
        "privateKeyPath",
        "snapshotPath",
        "credentialFingerprint",
        "botCommandMarker",
        "maxRuntimeSeconds",
        "pythonExecutableSha256",
        "serverSourceSha256",
        "daemonSourceSha256",
        "bundleManifestPath",
    }
    if set(config) != fields:
        raise BinanceObserverAuthorityError(
            "authority config fields are not exact"
        )
    for field in ("authorityId", "keyId"):
        if _ID_RE.fullmatch(_text(config.get(field))) is None:
            raise BinanceObserverAuthorityError(field + " is invalid")
    for field in (
        "credentialFingerprint",
        "pythonExecutableSha256",
        "serverSourceSha256",
        "daemonSourceSha256",
    ):
        if _HASH_RE.fullmatch(_text(config.get(field))) is None:
            raise BinanceObserverAuthorityError(field + " is invalid")
    authority_sid = _text(config.get("authorityOsSid")).upper()
    trader_sid = _text(config.get("traderOsSid")).upper()
    if (
        config.get("schemaVersion") != AUTHORITY_CONFIG_SCHEMA
        or _SID_RE.fullmatch(authority_sid) is None
        or _SID_RE.fullmatch(trader_sid) is None
        or secrets.compare_digest(authority_sid, trader_sid)
        or _PIPE_RE.fullmatch(_text(config.get("pipeAddress"))) is None
        or config.get("maxRuntimeSeconds") != 10800
        or len(_text(config.get("botCommandMarker"))) < 8
    ):
        raise BinanceObserverAuthorityError("authority config binding is invalid")
    authority_root = _absolute(config["authorityRoot"], "authority root")
    bundle_root = _absolute(config["bundleRoot"], "bundle root")
    if (
        not authority_root.is_dir()
        or authority_root.is_symlink()
        or not bundle_root.is_dir()
        or bundle_root.is_symlink()
        or not _within(bundle_root, authority_root)
    ):
        raise BinanceObserverAuthorityError("protected authority root is invalid")
    protected_paths: dict[str, Path] = {}
    for field in (
        "pipeAuthKeyPath",
        "credentialFilePath",
        "privateKeyPath",
        "bundleManifestPath",
    ):
        path = _absolute(config[field], field)
        if (
            not path.is_file()
            or path.is_symlink()
            or not _within(path, authority_root)
        ):
            raise BinanceObserverAuthorityError(field + " is not protected")
        protected_paths[field] = path
    snapshot = _absolute(config["snapshotPath"], "snapshot path")
    if snapshot.exists() and (not snapshot.is_file() or snapshot.is_symlink()):
        raise BinanceObserverAuthorityError("snapshot path is unsafe")
    config.update(
        {
            "authorityOsSid": authority_sid,
            "traderOsSid": trader_sid,
            "authorityRoot": str(authority_root),
            "bundleRoot": str(bundle_root),
            "snapshotPath": str(snapshot),
            **{key: str(path) for key, path in protected_paths.items()},
        }
    )
    return config


def verify_protected_runtime(config: Mapping[str, Any]) -> tuple[Path, Path]:
    authority_root = Path(str(config["authorityRoot"]))
    bundle_root = Path(str(config["bundleRoot"]))
    server = Path(__file__).resolve()
    daemon = server.with_name("binance_supervised_observer_daemon.py")
    executable = Path(sys.executable).resolve()
    if (
        not _within(server, bundle_root)
        or not _within(daemon, bundle_root)
        or not _within(executable, authority_root)
        or server.is_symlink()
        or daemon.is_symlink()
        or executable.is_symlink()
        or not daemon.is_file()
        or not executable.is_file()
        or _sha256_file(server) != config["serverSourceSha256"]
        or _sha256_file(daemon) != config["daemonSourceSha256"]
        or _sha256_file(executable) != config["pythonExecutableSha256"]
    ):
        raise BinanceObserverAuthorityError(
            "protected observer runtime manifest changed"
        )
    return executable, daemon


def _normalized_path(path: Path) -> str:
    return os.path.normcase(os.path.normpath(str(path.resolve())))


def _is_reparse(path: Path) -> bool:
    try:
        attributes = int(getattr(path.lstat(), "st_file_attributes", 0))
    except OSError as exc:
        raise BinanceObserverAuthorityError(
            "protected bundle path is unreadable"
        ) from exc
    return bool(attributes & 0x400) or path.is_symlink()


def _assert_no_reparse(path: Path, *, stop: Path) -> None:
    current = path.resolve()
    root = stop.resolve()
    if not _within(current, root) and current != root:
        raise BinanceObserverAuthorityError("protected bundle path escaped root")
    while True:
        if _is_reparse(current):
            raise BinanceObserverAuthorityError(
                "protected bundle reparse point is forbidden"
            )
        if current == root:
            return
        current = current.parent


def verify_protected_dacl(
    path: Path, *, authority_os_sid: str, trader_os_sid: str
) -> None:
    """Require a protected SYSTEM/authority/Administrators-only root DACL."""

    if os.name != "nt":
        raise BinanceObserverAuthorityError(
            "protected observer DACL verification requires Windows"
        )
    script = (
        "$ErrorActionPreference='Stop';"
        "$acl=Get-Acl -LiteralPath $args[0];"
        "$rows=@($acl.Access|ForEach-Object{"
        "$sid=$_.IdentityReference.Translate("
        "[System.Security.Principal.SecurityIdentifier]).Value;"
        "[pscustomobject]@{sid=$sid;type=$_.AccessControlType.ToString();"
        "rights=[int64]$_.FileSystemRights}});"
        "[pscustomobject]@{protected=$acl.AreAccessRulesProtected;rows=$rows}"
        "|ConvertTo-Json -Compress -Depth 4"
    )
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
            str(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=8.0,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    try:
        value = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise BinanceObserverAuthorityError(
            "protected observer DACL is unreadable"
        ) from exc
    rows = value.get("rows") if isinstance(value, Mapping) else None
    rows = rows if isinstance(rows, list) else [rows] if isinstance(rows, Mapping) else []
    allowed = {
        _text(authority_os_sid).upper(),
        "S-1-5-18",
        "S-1-5-32-544",
    }
    readable_allow: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise BinanceObserverAuthorityError(
                "protected observer DACL row is invalid"
            )
        try:
            rights = int(row.get("rights"))
        except (TypeError, ValueError) as exc:
            raise BinanceObserverAuthorityError(
                "protected observer DACL rights are invalid"
            ) from exc
        sid = _text(row.get("sid")).upper()
        if _text(row.get("type")).upper() == "ALLOW" and (
            rights & 0x1 or rights & 0x20000
        ):
            readable_allow.add(sid)
    if (
        result.returncode != 0
        or not isinstance(value, Mapping)
        or value.get("protected") is not True
        or not readable_allow
        or not readable_allow.issubset(allowed)
        or _text(trader_os_sid).upper() in readable_allow
        or "S-1-5-18" not in readable_allow
    ):
        raise BinanceObserverAuthorityError(
            "protected observer DACL is too broad or incomplete"
        )


def verify_bundle_manifest(
    config: Mapping[str, Any], *, config_path: Path
) -> dict[str, Any]:
    """Re-hash the exact sealed app/venv and pinned authority inputs."""

    manifest_path = Path(str(config["bundleManifestPath"]))
    manifest = _strict_json(manifest_path)
    top_fields = {
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
        "brokerCredentialAuthorityId",
        "machineProtectedCredentials",
        "brokerModes",
        "formalWorm",
        "promotionEligible",
    }
    if set(manifest) != top_fields:
        raise BinanceObserverAuthorityError(
            "protected bundle manifest fields are not exact"
        )
    authority_root = Path(str(config["authorityRoot"])).resolve()
    expected_roots = [
        authority_root / "app",
        authority_root / "venv",
    ]
    sealed_values = manifest.get("sealedRoots")
    shared_root = _absolute(manifest.get("sharedRoot"), "shared root")
    if (
        manifest.get("schemaVersion")
        != "crypto-first-live-supervised-authority-bundle-manifest/v1"
        or manifest.get("authorityOsSid") != config["authorityOsSid"]
        or manifest.get("traderOsSid") != config["traderOsSid"]
        or _normalized_path(Path(str(manifest.get("authorityRoot"))))
        != _normalized_path(authority_root)
        or not isinstance(sealed_values, list)
        or len(sealed_values) != 2
        or [
            _normalized_path(Path(str(item))) for item in sealed_values
        ]
        != [_normalized_path(item) for item in expected_roots]
        or manifest.get("formalWorm") is not False
        or manifest.get("promotionEligible") is not False
        or not _within(shared_root, authority_root)
        or not shared_root.is_dir()
        or not isinstance(manifest.get("sourcePins"), list)
        or not isinstance(manifest.get("externalBinaries"), list)
        or _HASH_RE.fullmatch(
            _text(manifest.get("pycryptodomeWheelSha256"))
        )
        is None
        or _HASH_RE.fullmatch(
            _text(manifest.get("githubHostKeyRawSha256"))
        )
        is None
        or _HASH_RE.fullmatch(
            _text(manifest.get("brokerBundleDescriptorSha256"))
        )
        is None
        or manifest.get("brokerCredentialAuthorityId")
        != config["authorityId"]
        or not isinstance(manifest.get("machineProtectedCredentials"), list)
        or not _text(manifest.get("remoteRef"))
    ):
        raise BinanceObserverAuthorityError(
            "protected bundle manifest identity changed"
        )
    for root in expected_roots:
        if not root.is_dir():
            raise BinanceObserverAuthorityError(
                "protected sealed root is unavailable"
            )
        _assert_no_reparse(root, stop=authority_root)

    def records(label: str) -> list[dict[str, str]]:
        value = manifest.get(label)
        if not isinstance(value, list):
            raise BinanceObserverAuthorityError(
                "protected bundle " + label + " is invalid"
            )
        result: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, Mapping) or set(item) != {"path", "sha256"}:
                raise BinanceObserverAuthorityError(
                    "protected bundle " + label + " record is invalid"
                )
            path = _absolute(item["path"], label + " path")
            normalized = _normalized_path(path)
            digest = _text(item["sha256"])
            if (
                normalized in seen
                or _HASH_RE.fullmatch(digest) is None
                or not path.is_file()
                or _is_reparse(path)
                or _sha256_file(path) != digest
            ):
                raise BinanceObserverAuthorityError(
                    "protected bundle " + label + " hash/path changed"
                )
            seen.add(normalized)
            result.append({"path": str(path), "sha256": digest})
        return result

    sealed_records = records("files")
    sealed_record_paths = {
        _normalized_path(Path(row["path"])) for row in sealed_records
    }
    actual_sealed_paths: set[str] = set()
    for root in expected_roots:
        for path in root.rglob("*"):
            if path.is_file():
                _assert_no_reparse(path, stop=root)
                actual_sealed_paths.add(_normalized_path(path))
    if sealed_record_paths != actual_sealed_paths:
        raise BinanceObserverAuthorityError(
            "protected sealed roots contain missing or extra files"
        )
    pinned_records = records("pinnedFiles")
    pinned_paths = [_normalized_path(Path(row["path"])) for row in pinned_records]
    normalized_config = _normalized_path(config_path)
    if pinned_paths.count(normalized_config) != 1:
        raise BinanceObserverAuthorityError(
            "authority config is not pinned exactly once"
        )
    for row in pinned_records:
        path = Path(row["path"])
        if not _within(path, authority_root):
            raise BinanceObserverAuthorityError(
                "protected pinned file escaped authority root"
            )
        _assert_no_reparse(path, stop=authority_root)

    credential_records = manifest["machineProtectedCredentials"]
    matching_credentials: list[dict[str, Any]] = []
    for item in credential_records:
        if not isinstance(item, Mapping) or set(item) != {
            "lane",
            "path",
            "credentialFingerprint",
            "accountFingerprint",
            "envelopeHash",
            "credentialGenerationId",
        }:
            raise BinanceObserverAuthorityError(
                "machine protected credential record is invalid"
            )
        path = _absolute(item["path"], "machine credential path")
        if (
            item.get("lane") != "BINANCE_SPOT"
            or _normalized_path(path)
            != _normalized_path(Path(config["credentialFilePath"]))
            or item.get("credentialFingerprint")
            != config["credentialFingerprint"]
            or item.get("accountFingerprint")
            != config["credentialFingerprint"]
            or _HASH_RE.fullmatch(_text(item.get("envelopeHash"))) is None
            or _ID_RE.fullmatch(
                _text(item.get("credentialGenerationId"))
            )
            is None
            or not path.is_file()
            or _is_reparse(path)
            or _normalized_path(path) in pinned_paths
            or _normalized_path(path) in sealed_record_paths
        ):
            raise BinanceObserverAuthorityError(
                "machine protected Binance credential binding changed"
            )
        matching_credentials.append(dict(item))
    if len(matching_credentials) != 1:
        raise BinanceObserverAuthorityError(
            "machine protected Binance credential is not unique"
        )

    modes = manifest.get("brokerModes")
    if not isinstance(modes, list):
        raise BinanceObserverAuthorityError("protected broker modes are invalid")
    matching = []
    for mode in modes:
        if not isinstance(mode, Mapping) or set(mode) != {
            "mode",
            "taskName",
            "pipeAddress",
            "entryPoint",
            "importRoot",
            "arguments",
            "environment",
        }:
            raise BinanceObserverAuthorityError(
                "protected broker mode fields are invalid"
            )
        if mode.get("mode") == "BINANCE_OBSERVER":
            matching.append(dict(mode))
    if len(matching) != 1:
        raise BinanceObserverAuthorityError(
            "protected Binance observer mode is not unique"
        )
    mode = matching[0]
    environment = mode.get("environment")
    environment_map = (
        {str(item["name"]): str(item["value"]) for item in environment}
        if isinstance(environment, list)
        and all(isinstance(item, Mapping) for item in environment)
        else {}
    )
    if (
        mode.get("taskName") != "CryptoFirstLive-BinanceObserver"
        or mode.get("pipeAddress") != config["pipeAddress"]
        or _normalized_path(Path(str(mode.get("entryPoint"))))
        != _normalized_path(Path(__file__))
        or _normalized_path(Path(str(mode.get("importRoot"))))
        != _normalized_path(Path(str(config["bundleRoot"])))
        or not isinstance(mode.get("arguments"), list)
        or any(not isinstance(item, str) for item in mode["arguments"])
        or mode.get("arguments")
        != ["--config", str(config_path.resolve())]
        or not isinstance(environment, list)
        or any(
            not isinstance(item, Mapping)
            or set(item) != {"name", "value"}
            or not isinstance(item.get("name"), str)
            or not isinstance(item.get("value"), str)
            for item in environment
        )
        or environment_map.get("PYTHONNOUSERSITE") != "1"
        or environment_map.get("PYTHONDONTWRITEBYTECODE") != "1"
        or environment_map.get("PYTHONSAFEPATH") != "1"
        or any(
            str(item["name"]).upper()
            in {
                "BINANCE_API_KEY",
                "BINANCE_API_SECRET",
                "PYTHONPATH",
                "PYTHONHOME",
            }
            for item in environment
        )
    ):
        raise BinanceObserverAuthorityError(
            "protected Binance observer mode changed"
        )
    return {
        **manifest,
        "_verifiedManifestSha256": _sha256_file(manifest_path),
        "_verifiedBinanceCredential": matching_credentials[0],
    }


def _load_pipe_authkey(path: Path) -> bytes:
    raw = path.read_bytes().strip()
    if len(raw) == 64:
        try:
            raw = bytes.fromhex(raw.decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            raw = b""
    if len(raw) != 32:
        raise BinanceObserverAuthorityError("authority pipe auth key is invalid")
    return raw


def build_observer_config(
    authority_config: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    verified_manifest_sha256: str = "",
    credential_record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if (
        request.get("authorityId") != authority_config["authorityId"]
        or request.get("keyId") != authority_config["keyId"]
        or request.get("accountFingerprint")
        != authority_config["credentialFingerprint"]
    ):
        raise BinanceObserverAuthorityError(
            "observer launch authority binding changed"
        )
    session_id = _text(request["sessionId"])
    credential = dict(credential_record or {})
    manifest_hash = _text(verified_manifest_sha256)
    if credential:
        if (
            _HASH_RE.fullmatch(manifest_hash) is None
            or credential.get("lane") != "BINANCE_SPOT"
            or credential.get("credentialFingerprint")
            != request["accountFingerprint"]
            or credential.get("accountFingerprint")
            != request["accountFingerprint"]
            or _HASH_RE.fullmatch(_text(credential.get("envelopeHash")))
            is None
            or _ID_RE.fullmatch(
                _text(credential.get("credentialGenerationId"))
            )
            is None
        ):
            raise BinanceObserverAuthorityError(
                "observer credential manifest projection changed"
            )
    elif manifest_hash:
        raise BinanceObserverAuthorityError(
            "observer credential manifest projection is incomplete"
        )
    return {
        "schemaVersion": OBSERVER_CONFIG_SCHEMA,
        "authorityId": authority_config["authorityId"],
        "keyId": authority_config["keyId"],
        "sessionId": session_id,
        "permitId": request["permitId"],
        "permitHash": request["permitHash"],
        "credentialFingerprint": request["accountFingerprint"],
        "credentialAuthorityId": authority_config["authorityId"],
        "credentialGenerationId": _text(
            credential.get("credentialGenerationId")
        ),
        "credentialEnvelopeHash": _text(credential.get("envelopeHash")),
        "bundleManifestSha256": manifest_hash,
        "ownerClientOrderPrefix": (
            "ftb-"
            + hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:12]
            + "-"
        ),
        "authorizedTraderPid": request["authorizedTraderPid"],
        "authorizedTraderCommandSha256": request[
            "authorizedTraderCommandSha256"
        ],
        "botCommandMarker": authority_config["botCommandMarker"],
        "maxRuntimeSeconds": 10800,
    }


def build_released_observer_config(
    authority_config: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    verified_manifest_sha256: str,
    credential_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a launchable observer config only after the compile-time fence."""
    if not BINANCE_OBSERVER_AUTHORITY_NETWORK_RELEASED:
        raise BinanceObserverAuthorityError(
            "observer authority network release is held"
        )
    return build_observer_config(
        authority_config,
        request,
        verified_manifest_sha256=verified_manifest_sha256,
        credential_record=credential_record,
    )


def verify_pipe_request_peer(
    connection: Any,
    request: Mapping[str, Any],
    authority_config: Mapping[str, Any],
) -> None:
    if (
        int(request["authorizedTraderPid"])
        != int(getattr(connection, "peer_process_id", 0))
        or not secrets.compare_digest(
            _text(getattr(connection, "peer_os_sid", "")).upper(),
            _text(authority_config["traderOsSid"]).upper(),
        )
    ):
        raise BinanceObserverAuthorityError(
            "observer launch pipe peer process changed"
        )


def _write_exclusive_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=False)
    encoded = _canonical(value)
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _read_ready(
    path: Path,
    *,
    process: subprocess.Popen[bytes],
    observer_config: Mapping[str, Any],
    deadline_epoch: float,
    clock=time.time,
) -> dict[str, Any]:
    while float(clock()) <= deadline_epoch:
        if process.poll() is not None:
            raise BinanceObserverAuthorityError(
                "observer child exited before prearmed acknowledgement"
            )
        if path.is_file() and not path.is_symlink():
            ready = _strict_json(path)
            body = {key: item for key, item in ready.items() if key != "readyHash"}
            if (
                set(ready)
                != {
                    "schemaVersion",
                    "observerProcessId",
                    "configHash",
                    "signedGetAttemptCount",
                    "mutationAttemptCount",
                    "networkCapabilityOpen",
                    "readyHash",
                }
                or ready.get("schemaVersion") != PREARMED_READY_SCHEMA
                or ready.get("observerProcessId") != process.pid
                or ready.get("configHash")
                != authority_hash(dict(observer_config))
                or ready.get("signedGetAttemptCount") != 0
                or ready.get("mutationAttemptCount") != 0
                or ready.get("networkCapabilityOpen") is not False
                or ready.get("readyHash") != authority_hash(body)
            ):
                raise BinanceObserverAuthorityError(
                    "observer child prearmed acknowledgement is invalid"
                )
            return ready
        time.sleep(0.005)
    raise BinanceObserverAuthorityError(
        "observer child prearmed acknowledgement missed coverage deadline"
    )


def build_launch_ack(
    request: Mapping[str, Any],
    *,
    request_id: str,
    observer_process_id: int,
    accepted_epoch: float,
) -> dict[str, Any]:
    accepted = float(accepted_epoch)
    if not math.isfinite(accepted):
        raise BinanceObserverAuthorityError("observer accepted epoch is invalid")
    body: dict[str, Any] = {
        "schemaVersion": LAUNCH_ACK_SCHEMA,
        "requestId": request_id,
        "authorityId": request["authorityId"],
        "keyId": request["keyId"],
        "lane": "BINANCE_SPOT",
        "sessionId": request["sessionId"],
        "permitId": request["permitId"],
        "permitHash": request["permitHash"],
        "preparedPlanHash": request["preparedPlanHash"],
        "observerProcessId": int(observer_process_id),
        "acceptedEpoch": accepted,
        "coverageDeadlineEpoch": request["coverageDeadlineEpoch"],
        "prearmedBeforeRequest": True,
        "pipePeerVerified": True,
        "signedGetAttemptCountBeforeAck": 0,
        "orderMutationAttemptCountBeforeAck": 0,
        "cancelMutationAttemptCountBeforeAck": 0,
        "transferMutationAttemptCountBeforeAck": 0,
        "withdrawMutationAttemptCountBeforeAck": 0,
        "marginMutationAttemptCountBeforeAck": 0,
        "futuresMutationAttemptCountBeforeAck": 0,
        "networkCapabilityOpen": False,
    }
    return {**body, "ackHash": authority_hash(body)}


def _child_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for key in (
        "BINANCE_API_KEY",
        "BINANCE_API_SECRET",
        "PYTHONHOME",
        "PYTHONPATH",
    ):
        environment.pop(key, None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["PYTHONSAFEPATH"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def _create_zero_gate(path: Path) -> None:
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _terminate(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3.0)


def run_authority(config_path: Path) -> int:
    config = validate_authority_config(_strict_json(config_path.resolve()))
    if not secrets.compare_digest(
        _windows_process_sid(), config["authorityOsSid"]
    ):
        raise BinanceObserverAuthorityError(
            "observer authority SYSTEM identity changed"
        )
    manifest = verify_bundle_manifest(
        config, config_path=config_path.resolve()
    )
    protected_roots = {
        Path(config["authorityRoot"]),
        Path(config["authorityRoot"]) / "app",
        Path(config["authorityRoot"]) / "venv",
        config_path.resolve().parent,
        Path(config["pipeAuthKeyPath"]).parent,
        Path(config["credentialFilePath"]).parent,
        Path(config["privateKeyPath"]).parent,
    }
    for protected_root in protected_roots:
        verify_protected_dacl(
            protected_root,
            authority_os_sid=config["authorityOsSid"],
            trader_os_sid=config["traderOsSid"],
        )
    executable, daemon = verify_protected_runtime(config)
    authkey = _load_pipe_authkey(Path(config["pipeAuthKeyPath"]))
    listener = WindowsNamedPipeSupervisedAuthorityServer(
        pipe_address=config["pipeAddress"],
        trader_os_sid=config["traderOsSid"],
        timeout_seconds=5.0,
    )
    connection = None
    process: subprocess.Popen[bytes] | None = None
    request_id = ""
    nonce = ""
    authenticated = False
    try:
        while connection is None:
            connection = listener.accept(connect_timeout_seconds=1.0)
        raw = connection.recv_bytes(MAX_WIRE_BYTES)
        command, nonce = decode_authenticated_pipe_request(raw, authkey=authkey)
        authenticated = True
        if (
            set(command) != {"schemaVersion", "requestId", "request"}
            or command.get("schemaVersion") != LAUNCH_COMMAND_SCHEMA
            or _ID_RE.fullmatch(_text(command.get("requestId"))) is None
            or not isinstance(command.get("request"), Mapping)
        ):
            raise BinanceObserverAuthorityError(
                "observer launch command is invalid"
            )
        request_id = _text(command["requestId"])
        request = validate_prearmed_observer_launch_request(
            command["request"], clock=time.time
        )
        verify_pipe_request_peer(connection, request, config)
        observer_config = build_released_observer_config(
            config,
            request,
            verified_manifest_sha256=manifest[
                "_verifiedManifestSha256"
            ],
            credential_record=manifest["_verifiedBinanceCredential"],
        )
        process_audit = _process_audit(observer_config, time.time())
        if (
            process_audit.get("exactAuthorizedTraderPresent") is not True
            or process_audit.get("exactAuthorizedTraderProcessCount") != 1
            or process_audit.get("otherMatchingBotProcessCount") != 0
            or process_audit.get("observerProcessSeparate") is not True
        ):
            raise BinanceObserverAuthorityError(
                "observer launch process audit is not exclusive"
            )
        authority_root = Path(config["authorityRoot"])
        session_key = hashlib.sha256(
            (request["sessionId"] + "\x00" + request["permitHash"]).encode(
                "utf-8"
            )
        ).hexdigest()
        session_root = authority_root / "sessions" / session_key
        config_file = session_root / "observer-config.json"
        database_file = session_root / "observer-private.sqlite3"
        ready_file = session_root / "prearmed-ready.json"
        gate_file = session_root / "start.gate"
        _write_exclusive_json(config_file, observer_config)
        command_line = [
            str(executable),
            str(daemon),
            "--config",
            str(config_file),
            "--database",
            str(database_file),
            "--private-key",
            config["privateKeyPath"],
            "--snapshot",
            config["snapshotPath"],
            "--credential-file",
            config["credentialFilePath"],
            "--prearmed-ready-file",
            str(ready_file),
            "--start-gate",
            str(gate_file),
        ]
        process = subprocess.Popen(
            command_line,
            cwd=str(authority_root),
            env=_child_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        _read_ready(
            ready_file,
            process=process,
            observer_config=observer_config,
            deadline_epoch=float(request["coverageDeadlineEpoch"]),
        )
        accepted = time.time()
        ack = build_launch_ack(
            request,
            request_id=request_id,
            observer_process_id=process.pid,
            accepted_epoch=accepted,
        )
        response = {
            "schemaVersion": LAUNCH_RESPONSE_SCHEMA,
            "requestId": request_id,
            "ok": True,
            "ack": ack,
            "error": "",
        }
        connection.send_bytes(
            encode_authenticated_pipe_response(
                response,
                request_nonce=nonce,
                request_id=request_id,
                authkey=authkey,
            )
        )
        connection.close()
        connection = None
        _create_zero_gate(gate_file)
        return int(process.wait(timeout=10830.0))
    except BaseException:
        if authenticated and connection is not None and request_id and nonce:
            try:
                response = {
                    "schemaVersion": LAUNCH_RESPONSE_SCHEMA,
                    "requestId": request_id,
                    "ok": False,
                    "ack": {},
                    "error": "authority-request-rejected",
                }
                connection.send_bytes(
                    encode_authenticated_pipe_response(
                        response,
                        request_nonce=nonce,
                        request_id=request_id,
                        authkey=authkey,
                    )
                )
            except Exception:
                pass
        _terminate(process)
        raise
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    return run_authority(Path(args.config))


if __name__ == "__main__":
    raise SystemExit(main())
