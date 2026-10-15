"""Public account/deployment identity and an OS-owned live writer lease."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping


_SHA = re.compile(r"[a-f0-9]{64}\Z")
_BROKERS = {"kis": "stock", "binance": "crypto", "binance-futures": "crypto", "upbit": "crypto"}


def runtime_scope_id(broker_id: str, account_fingerprint: str, deployment_id: str, capital_currency: str = "") -> str:
    identity = [broker_id, account_fingerprint, deployment_id] + ([capital_currency] if capital_currency else [])
    body = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return "runtime-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RuntimeScope:
    broker_id: str
    account_fingerprint: str
    deployment_id: str
    profile_id: str
    account_binding_id: str = ""
    credential_configuration_hash: str = ""
    capital_currency: str = ""

    def __post_init__(self):
        if self.broker_id not in _BROKERS or self.profile_id != _BROKERS[self.broker_id]:
            raise ValueError("runtime-scope-broker-profile-mismatch")
        if not _SHA.fullmatch(self.account_fingerprint):
            raise ValueError("runtime-scope-account-fingerprint-invalid")
        for name, value in (("deployment", self.deployment_id), ("account-binding", self.account_binding_id)):
            if not isinstance(value, str) or len(value) > 256 or value != value.strip() or any(ord(c) < 32 for c in value):
                raise ValueError("runtime-scope-" + name + "-invalid")
        if not self.deployment_id:
            raise ValueError("runtime-scope-deployment-required")
        if self.credential_configuration_hash and not _SHA.fullmatch(self.credential_configuration_hash):
            raise ValueError("runtime-scope-credential-hash-invalid")

        if self.capital_currency and (not re.fullmatch(r"[A-Z]{2,10}", self.capital_currency) or (self.broker_id == "kis" and self.capital_currency not in {"KRW", "USD"})):
            raise ValueError("runtime-scope-capital-currency-invalid")

    @property
    def runtime_id(self):
        return runtime_scope_id(self.broker_id, self.account_fingerprint, self.deployment_id, self.capital_currency)

    @property
    def account_key(self):
        return hashlib.sha256((self.broker_id + ":" + self.account_fingerprint).encode("ascii")).hexdigest()

    def to_dict(self):
        return {"runtimeId": self.runtime_id, "runtimeScopeId": self.runtime_id,
                "brokerId": self.broker_id, "accountFingerprint": self.account_fingerprint,
                "deploymentId": self.deployment_id, "profileId": self.profile_id,
                "accountBindingId": self.account_binding_id,
                "credentialConfigurationHash": self.credential_configuration_hash,
                "capitalCurrency": self.capital_currency, "quoteCurrency": self.capital_currency}


def scope_from_mapping(value: RuntimeScope | Mapping[str, Any]) -> RuntimeScope:
    if isinstance(value, RuntimeScope):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("runtime-scope-mapping-required")
    scope = RuntimeScope(
        broker_id=value.get("brokerId", ""), account_fingerprint=value.get("accountFingerprint", ""),
        deployment_id=value.get("deploymentId", ""), profile_id=value.get("profileId", ""),
        account_binding_id=value.get("accountBindingId", ""),
        credential_configuration_hash=value.get("credentialConfigurationHash", ""),
        capital_currency=value.get("capitalCurrency") or value.get("quoteCurrency") or "",
    )
    for key in ("runtimeId", "runtimeScopeId"):
        if key in value and value[key] != scope.runtime_id:
            raise ValueError("runtime-scope-id-mismatch")
    return scope


def scope_directory(root: Path, scope: RuntimeScope) -> Path:
    return Path(root) / "logs" / "runtime_scopes" / scope.runtime_id


def save_scope(root: Path, scope: RuntimeScope) -> None:
    path = scope_directory(root, scope) / "runtime_scope.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if scope_from_mapping(json.loads(path.read_text(encoding="utf-8"))) != scope:
            raise ValueError("persisted-runtime-scope-binding-changed")
        return
    temporary = path.with_suffix(".tmp-" + str(os.getpid()))
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(scope.to_dict(), stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_scopes(root: Path) -> tuple[RuntimeScope, ...]:
    parent = Path(root) / "logs" / "runtime_scopes"
    if not parent.exists():
        return ()
    scopes = []
    for path in sorted(parent.glob("runtime-*/runtime_scope.json")):
        scope = scope_from_mapping(json.loads(path.read_text(encoding="utf-8")))
        if path.parent.name != scope.runtime_id:
            raise ValueError("persisted-runtime-scope-path-mismatch")
        scopes.append(scope)
    return tuple(scopes)


def default_account_writer_lease_root() -> Path:
    # Both source and frozen builds use the same per-user OS ownership domain.
    # Application data directories deliberately do not participate in identity.
    return Path(os.getenv("LOCALAPPDATA") or Path.home()) / "trading-system" / "runtime-locks" / "account-writers"


class AccountWriterLease:
    """A crash releases the OS lock; a marker file alone grants no authority."""
    def __init__(self, root: Path, scope: RuntimeScope, *, lease_root: Path | None = None):
        # root remains a compatible caller argument, but never selects ownership.
        # Explicit injection is for isolated tests; no UI/data-root override exists.
        ownership = Path(lease_root) if lease_root is not None else default_account_writer_lease_root()
        self.path = ownership / (scope.account_key + ".lock")
        self._handle = None

    @property
    def held(self):
        return self._handle is not None

    def acquire(self):
        if self.held:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            raise ValueError("runtime-account-live-writer-already-owned") from None
        self._handle = handle

    def release(self):
        handle, self._handle = self._handle, None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
