from __future__ import annotations

"""One-shot CurrentUser -> LocalMachine DPAPI broker credential rewrap.

Only non-secret fingerprints and hashes are emitted.  Raw credentials remain
in this elevated trader process and the destination is an exact protected
authority path.  No broker network operation exists in this module.
"""

import argparse
import base64
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping


ENVELOPE_SCHEMA = "crypto-first-live-machine-protected-broker-credential/v1"
MANIFEST_SCHEMA = "crypto-first-live-supervised-authority-bundle-manifest/v1"
SECRET_STORE_SCHEMA = "trading-system-secret-store-v1"
AUTHORITY_ROOT = Path(r"D:\crypto-first-live-authority")
EXACT_DESTINATIONS = {
    "UPBIT": AUTHORITY_ROOT / "secrets" / "upbit-credential.dpapi",
    "BINANCE_SPOT": AUTHORITY_ROOT / "secrets" / "binance-credential.dpapi",
}
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class CredentialRewrapError(RuntimeError):
    pass


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fingerprint(lane: str, access_key: str) -> str:
    if lane == "UPBIT":
        raw = b"UPBIT_SPOT\0" + access_key.encode("utf-8")
    elif lane == "BINANCE_SPOT":
        raw = (
            b"trading-system:binance-spot-account:v1\x00"
            + access_key.encode("utf-8")
        )
    else:  # pragma: no cover - internal invariant
        raise CredentialRewrapError("credential lane is invalid")
    return hashlib.sha256(raw).hexdigest()


def _origin(lane: str) -> str:
    return (
        "https://api.upbit.com"
        if lane == "UPBIT"
        else "https://api.binance.com"
    )


def _secret_names(lane: str) -> tuple[str, str]:
    return (
        ("UPBIT_ACCESS_KEY", "UPBIT_SECRET_KEY")
        if lane == "UPBIT"
        else ("BINANCE_API_KEY", "BINANCE_API_SECRET")
    )


def _secret_store_path() -> Path:
    configured = str(os.environ.get("TRADING_SYSTEM_SECRET_STORE_PATH") or "").strip()
    if configured:
        return Path(os.path.expandvars(configured)).expanduser()
    home = Path(
        os.environ.get("USERPROFILE")
        or os.environ.get("HOME")
        or os.getcwd()
    )
    base = Path(
        os.environ.get("LOCALAPPDATA")
        or os.environ.get("APPDATA")
        or str(home / "AppData" / "Local")
    )
    return base / "trading-system" / "secrets-v1.json"


def _unprotect_dpapi(ciphertext: bytes, entropy: bytes) -> bytes:
    if os.name != "nt":
        raise CredentialRewrapError("Windows DPAPI is required")
    cipher_buffer = bytearray(ciphertext)
    entropy_buffer = bytearray(entropy)
    cipher_blob, cipher_view = _blob_from_buffer(cipher_buffer)
    entropy_blob, entropy_view = _blob_from_buffer(entropy_buffer)
    output = _DataBlob()
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_DataBlob),
        ctypes.POINTER(wintypes.LPWSTR),
        ctypes.POINTER(_DataBlob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    try:
        if not crypt32.CryptUnprotectData(
            ctypes.byref(cipher_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x1,
            ctypes.byref(output),
        ):
            raise CredentialRewrapError(
                "current-user credential unprotect failed:win32-"
                + str(ctypes.get_last_error())
            )
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        if output.pbData:
            ctypes.memset(output.pbData, 0, output.cbData)
            kernel32.LocalFree(output.pbData)
        for index in range(len(cipher_buffer)):
            cipher_buffer[index] = 0
        for index in range(len(entropy_buffer)):
            entropy_buffer[index] = 0
        del cipher_view, entropy_view


def _read_current_user_secret(name: str) -> str:
    path = _secret_store_path().resolve(strict=True)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CredentialRewrapError("current-user-secret-store-invalid") from exc
    record = (
        payload.get("values", {}).get("live_trader." + name)
        if isinstance(payload, dict)
        and payload.get("schema") == SECRET_STORE_SCHEMA
        and isinstance(payload.get("values"), dict)
        else None
    )
    if (
        not isinstance(record, Mapping)
        or record.get("protector") != "windows-dpapi-current-user"
        or not isinstance(record.get("ciphertext"), str)
    ):
        raise CredentialRewrapError("current-user-broker-credential-missing:" + name)
    entropy_scope = f"{SECRET_STORE_SCHEMA}\0{path}\0live_trader.{name}"
    entropy = hashlib.sha256(entropy_scope.encode("utf-8")).digest()
    try:
        plain = bytearray(
            _unprotect_dpapi(
                base64.b64decode(record["ciphertext"], validate=True),
                entropy,
            )
        )
        value = plain.decode("utf-8").strip()
        if not value:
            raise CredentialRewrapError(
                "current-user-broker-credential-missing:" + name
            )
        return value
    except (ValueError, UnicodeDecodeError) as exc:
        raise CredentialRewrapError("current-user-secret-store-invalid") from exc
    finally:
        if "plain" in locals():
            for index in range(len(plain)):
                plain[index] = 0


def _read_current_user_credentials() -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for lane in ("UPBIT", "BINANCE_SPOT"):
        access_name, secret_name = _secret_names(lane)
        access = _read_current_user_secret(access_name)
        secret = _read_current_user_secret(secret_name)
        if not access or not secret:
            raise CredentialRewrapError(
                "current-user-broker-credential-missing:" + lane
            )
        result[lane] = (access, secret)
    return result


def _envelope(
    *,
    lane: str,
    authority_id: str,
    generation_id: str,
    access_key: str,
    secret_key: str,
) -> dict[str, Any]:
    fingerprint = _fingerprint(lane, access_key)
    body = {
        "schemaVersion": ENVELOPE_SCHEMA,
        "authorityId": authority_id,
        "credentialGenerationId": generation_id,
        "lane": lane,
        "origin": _origin(lane),
        "accessKey": access_key,
        "secretKey": secret_key,
        "credentialFingerprint": fingerprint,
        "accountFingerprint": fingerprint,
    }
    return {**body, "envelopeHash": _digest(body)}


def _metadata(envelope: Mapping[str, Any], destination: Path) -> dict[str, Any]:
    return {
        "lane": envelope["lane"],
        "path": str(destination),
        "credentialFingerprint": envelope["credentialFingerprint"],
        "accountFingerprint": envelope["accountFingerprint"],
        "envelopeHash": envelope["envelopeHash"],
        "credentialGenerationId": envelope["credentialGenerationId"],
    }


def _entropy(
    *,
    authority_id: str,
    manifest_sha256: str,
    account_fingerprint: str,
    generation_id: str,
    lane: str,
) -> bytes:
    values = (
        authority_id,
        manifest_sha256,
        account_fingerprint,
        generation_id,
        lane,
    )
    if (
        _ID_RE.fullmatch(authority_id) is None
        or _HASH_RE.fullmatch(manifest_sha256) is None
        or _HASH_RE.fullmatch(account_fingerprint) is None
        or _ID_RE.fullmatch(generation_id) is None
        or lane not in EXACT_DESTINATIONS
    ):
        raise CredentialRewrapError("credential entropy binding is invalid")
    return b"crypto-first-live-machine-credential:v1\x00" + b"\x00".join(
        item.encode("utf-8") for item in values
    )


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_ubyte))]


def _blob_from_buffer(buffer: bytearray) -> tuple[_DataBlob, Any]:
    if not buffer:
        return _DataBlob(0, None), None
    view = (ctypes.c_ubyte * len(buffer)).from_buffer(buffer)
    return _DataBlob(len(buffer), ctypes.cast(view, ctypes.POINTER(ctypes.c_ubyte))), view


def _protect_local_machine(plain: bytes, entropy: bytes) -> bytes:
    if os.name != "nt":
        raise CredentialRewrapError("Windows DPAPI is required")
    plain_buffer = bytearray(plain)
    entropy_buffer = bytearray(entropy)
    plain_blob, plain_view = _blob_from_buffer(plain_buffer)
    entropy_blob, entropy_view = _blob_from_buffer(entropy_buffer)
    output = _DataBlob()
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    crypt32.CryptProtectData.argtypes = [
        ctypes.POINTER(_DataBlob),
        wintypes.LPCWSTR,
        ctypes.POINTER(_DataBlob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptProtectData.restype = wintypes.BOOL
    try:
        if not crypt32.CryptProtectData(
            ctypes.byref(plain_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x1 | 0x4,  # UI_FORBIDDEN | LOCAL_MACHINE
            ctypes.byref(output),
        ):
            raise CredentialRewrapError(
                "machine credential protect failed:win32-"
                + str(ctypes.get_last_error())
            )
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        if output.pbData:
            ctypes.memset(output.pbData, 0, output.cbData)
            kernel32.LocalFree(output.pbData)
        for index in range(len(plain_buffer)):
            plain_buffer[index] = 0
        for index in range(len(entropy_buffer)):
            entropy_buffer[index] = 0
        del plain_view, entropy_view


def _unprotect_local_machine(ciphertext: bytes, entropy: bytes) -> bytes:
    """Runtime/test helper; wrong entropy and tampering fail closed."""

    if os.name != "nt":
        raise CredentialRewrapError("Windows DPAPI is required")
    cipher_buffer = bytearray(ciphertext)
    entropy_buffer = bytearray(entropy)
    cipher_blob, cipher_view = _blob_from_buffer(cipher_buffer)
    entropy_blob, entropy_view = _blob_from_buffer(entropy_buffer)
    output = _DataBlob()
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_DataBlob),
        ctypes.POINTER(wintypes.LPWSTR),
        ctypes.POINTER(_DataBlob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    try:
        if not crypt32.CryptUnprotectData(
            ctypes.byref(cipher_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x1,
            ctypes.byref(output),
        ):
            raise CredentialRewrapError(
                "machine credential unprotect failed:win32-"
                + str(ctypes.get_last_error())
            )
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        if output.pbData:
            ctypes.memset(output.pbData, 0, output.cbData)
            kernel32.LocalFree(output.pbData)
        for index in range(len(cipher_buffer)):
            cipher_buffer[index] = 0
        for index in range(len(entropy_buffer)):
            entropy_buffer[index] = 0
        del cipher_view, entropy_view


def _manifest(path: Path) -> tuple[dict[str, Any], str]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(value, dict)
        or value.get("schemaVersion") != MANIFEST_SCHEMA
        or not isinstance(value.get("machineProtectedCredentials"), list)
    ):
        raise CredentialRewrapError("credential manifest is invalid")
    return dict(value), _sha256_file(path)


def inspect_metadata(
    *, authority_id: str, generations: Mapping[str, str]
) -> dict[str, Any]:
    if _ID_RE.fullmatch(authority_id) is None:
        raise CredentialRewrapError("credential authority id is invalid")
    credentials = _read_current_user_credentials()
    rows = []
    try:
        for lane in ("UPBIT", "BINANCE_SPOT"):
            access, secret = credentials[lane]
            envelope = _envelope(
                lane=lane,
                authority_id=authority_id,
                generation_id=str(generations[lane]),
                access_key=access,
                secret_key=secret,
            )
            rows.append(_metadata(envelope, EXACT_DESTINATIONS[lane]))
    finally:
        credentials.clear()
    return {
        "schemaVersion": "crypto-first-live-broker-credential-inspection/v1",
        "brokerNetworkRequestCount": 0,
        "orderMutationCount": 0,
        "credentials": rows,
    }


def rewrap_from_manifest(path: Path) -> dict[str, Any]:
    manifest, manifest_hash = _manifest(path.resolve(strict=True))
    authority_id = str(manifest.get("brokerCredentialAuthorityId") or "")
    rows = manifest["machineProtectedCredentials"]
    if len(rows) != 2:
        raise CredentialRewrapError("credential manifest rows are invalid")
    by_lane = {str(row.get("lane")): dict(row) for row in rows if isinstance(row, dict)}
    if set(by_lane) != set(EXACT_DESTINATIONS):
        raise CredentialRewrapError("credential manifest lanes are invalid")
    credentials = _read_current_user_credentials()
    outputs: list[dict[str, Any]] = []
    try:
        for lane in ("UPBIT", "BINANCE_SPOT"):
            row = by_lane[lane]
            if set(row) != {
                "lane",
                "path",
                "credentialFingerprint",
                "accountFingerprint",
                "envelopeHash",
                "credentialGenerationId",
            }:
                raise CredentialRewrapError("credential manifest row fields are not exact")
            destination = Path(str(row["path"]))
            if destination != EXACT_DESTINATIONS[lane] or destination.exists():
                raise CredentialRewrapError("credential destination is not exact or is replayed")
            access, secret = credentials[lane]
            envelope = _envelope(
                lane=lane,
                authority_id=authority_id,
                generation_id=str(row["credentialGenerationId"]),
                access_key=access,
                secret_key=secret,
            )
            metadata = _metadata(envelope, destination)
            if any(metadata[key] != row[key] for key in metadata):
                raise CredentialRewrapError("credential account binding changed")
            entropy = _entropy(
                authority_id=authority_id,
                manifest_sha256=manifest_hash,
                account_fingerprint=str(row["accountFingerprint"]),
                generation_id=str(row["credentialGenerationId"]),
                lane=lane,
            )
            protected = _protect_local_machine(_canonical(envelope), entropy)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as handle:
                handle.write(protected)
                handle.flush()
                os.fsync(handle.fileno())
            outputs.append(
                {
                    "lane": lane,
                    "ciphertextSha256": hashlib.sha256(protected).hexdigest(),
                    "envelopeHash": envelope["envelopeHash"],
                }
            )
    finally:
        credentials.clear()
    return {
        "schemaVersion": "crypto-first-live-broker-credential-rewrap-receipt/v1",
        "rewrapped": True,
        "manifestSha256": manifest_hash,
        "brokerNetworkRequestCount": 0,
        "orderMutationCount": 0,
        "credentials": outputs,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)
    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--authority-id", required=True)
    inspect_parser.add_argument("--upbit-generation-id", required=True)
    inspect_parser.add_argument("--binance-generation-id", required=True)
    rewrap_parser = subparsers.add_parser("rewrap")
    rewrap_parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    if args.mode == "inspect":
        result = inspect_metadata(
            authority_id=args.authority_id,
            generations={
                "UPBIT": args.upbit_generation_id,
                "BINANCE_SPOT": args.binance_generation_id,
            },
        )
    else:
        result = rewrap_from_manifest(Path(args.manifest))
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
