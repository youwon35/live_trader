from __future__ import annotations

"""Provision the private Binance supervised-observer key and exact config.

This utility has no broker/network capability.  It never prints private key
bytes, API keys, API secrets, command lines, signatures, or order identities.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

from Crypto.PublicKey import ECC


CONFIG_SCHEMA = "binance-supervised-observer-config/v1"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


def _absolute(value: str, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise RuntimeError(f"{label} must be an absolute path")
    return path.resolve()


def _write_exclusive(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def _powershell_json(script: str) -> Any:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            script,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=8.0,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    return json.loads(completed.stdout)


def _current_sid() -> str:
    value = _powershell_json(
        "[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value "
        "| ConvertTo-Json -Compress"
    )
    sid = str(value or "").strip()
    if not sid.startswith("S-1-"):
        raise RuntimeError("current Windows SID is unavailable")
    return sid


def _restrict_new_authority_root(path: Path) -> str:
    if os.name != "nt":
        raise RuntimeError("observer provisioning requires Windows ACLs")
    if path.exists():
        raise RuntimeError("authority root already exists; refusing to overwrite")
    path.mkdir(parents=True)
    sid = _current_sid()
    completed = subprocess.run(
        [
            "icacls.exe",
            str(path),
            "/inheritance:r",
            "/grant:r",
            f"*{sid}:(OI)(CI)F",
            "*S-1-5-18:(OI)(CI)F",
            "*S-1-5-32-544:(OI)(CI)F",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=8.0,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if completed.returncode != 0:
        raise RuntimeError("authority root ACL restriction failed")
    return sid


def _command_hash_for_pid(pid: int, marker: str) -> str:
    if os.name != "nt" or pid <= 0:
        raise RuntimeError("authorized trader PID is invalid")
    value = _powershell_json(
        "Get-CimInstance Win32_Process -Filter \"ProcessId="
        + str(pid)
        + "\" | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
    )
    if not isinstance(value, dict) or int(value.get("ProcessId") or 0) != pid:
        raise RuntimeError("authorized trader process is absent")
    command = " ".join(str(value.get("CommandLine") or "").split()).lower()
    normalized_marker = " ".join(str(marker or "").split()).lower()
    if len(normalized_marker) < 8 or normalized_marker not in command:
        raise RuntimeError("bot command marker does not identify the trader")
    return hashlib.sha256(command.encode("utf-8")).hexdigest()


def _keypair(args: argparse.Namespace) -> int:
    root = _absolute(args.authority_root, "authority root")
    sid = _restrict_new_authority_root(root)
    private_path = root / "ed25519-private.pem"
    public_path = _absolute(args.public_key, "public key")
    if public_path == private_path or public_path.is_relative_to(root):
        raise RuntimeError("trader public key must be outside the private root")
    key = ECC.generate(curve="Ed25519")
    _write_exclusive(private_path, key.export_key(format="PEM").encode("ascii"))
    public = key.public_key()
    _write_exclusive(public_path, public.export_key(format="PEM").encode("ascii"))
    result = {
        "ok": True,
        "schemaVersion": "binance-supervised-observer-key-provision/v1",
        "authorityRoot": str(root),
        "privateKeyPath": str(private_path),
        "publicKeyPath": str(public_path),
        "authorityOsSidHash": hashlib.sha256(sid.encode("utf-8")).hexdigest(),
        "publicKeyFingerprintSha256": hashlib.sha256(
            public.export_key(format="DER")
        ).hexdigest(),
        "privateKeyPrinted": False,
        "networkRequestCount": 0,
        "mutationCount": 0,
    }
    print(json.dumps(result, sort_keys=True))
    return 0


def _exact_id(value: str, label: str) -> str:
    result = str(value or "").strip()
    if _ID_RE.fullmatch(result) is None:
        raise RuntimeError(f"{label} is invalid")
    return result


def _exact_hash(value: str, label: str) -> str:
    result = str(value or "").strip().lower()
    if _HASH_RE.fullmatch(result) is None:
        raise RuntimeError(f"{label} is invalid")
    return result


def _config(args: argparse.Namespace) -> int:
    root = _absolute(args.authority_root, "authority root")
    private_path = root / "ed25519-private.pem"
    if not root.is_dir() or root.is_symlink() or not private_path.is_file():
        raise RuntimeError("restricted authority key root is unavailable")
    session_id = _exact_id(args.session_id, "session id")
    permit_id = _exact_id(args.permit_id, "permit id")
    permit_hash = _exact_hash(args.permit_hash, "permit hash")
    credential = _exact_hash(
        args.credential_fingerprint, "credential fingerprint"
    )
    owner_prefix = (
        "ftb-" + hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:12] + "-"
    )
    command_hash = _command_hash_for_pid(
        int(args.authorized_trader_pid), args.bot_command_marker
    )
    body = {
        "schemaVersion": CONFIG_SCHEMA,
        "authorityId": _exact_id(args.authority_id, "authority id"),
        "keyId": _exact_id(args.key_id, "key id"),
        "sessionId": session_id,
        "permitId": permit_id,
        "permitHash": permit_hash,
        "credentialFingerprint": credential,
        "ownerClientOrderPrefix": owner_prefix,
        "authorizedTraderPid": int(args.authorized_trader_pid),
        "authorizedTraderCommandSha256": command_hash,
        "botCommandMarker": str(args.bot_command_marker).strip(),
        "maxRuntimeSeconds": 10800,
    }
    config_path = root / "observer-config.json"
    _write_exclusive(
        config_path,
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    result = {
        "ok": True,
        "schemaVersion": "binance-supervised-observer-config-provision/v1",
        "configPath": str(config_path),
        "authorityId": body["authorityId"],
        "keyId": body["keyId"],
        "sessionId": session_id,
        "permitId": permit_id,
        "permitHash": permit_hash,
        "credentialFingerprintConfigured": True,
        "ownerClientOrderPrefix": owner_prefix,
        "authorizedTraderCommandSha256": command_hash,
        "rawCommandLinePrinted": False,
        "secretPrinted": False,
        "networkRequestCount": 0,
        "mutationCount": 0,
    }
    print(json.dumps(result, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    keypair = commands.add_parser("keypair")
    keypair.add_argument("--authority-root", required=True)
    keypair.add_argument("--public-key", required=True)
    keypair.set_defaults(run=_keypair)

    config = commands.add_parser("config")
    config.add_argument("--authority-root", required=True)
    config.add_argument("--authority-id", required=True)
    config.add_argument("--key-id", required=True)
    config.add_argument("--session-id", required=True)
    config.add_argument("--permit-id", required=True)
    config.add_argument("--permit-hash", required=True)
    config.add_argument("--credential-fingerprint", required=True)
    config.add_argument("--authorized-trader-pid", required=True, type=int)
    config.add_argument("--bot-command-marker", required=True)
    config.set_defaults(run=_config)
    args = parser.parse_args()
    return int(args.run(args))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errorType": type(exc).__name__,
                    "privateKeyPrinted": False,
                    "secretPrinted": False,
                    "networkRequestCount": 0,
                    "mutationCount": 0,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2)
