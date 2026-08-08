from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3
import uuid
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


DEPLOYMENT_MANIFEST_SCHEMA_VERSION = "live-deployment-manifest-v1"
PREFLIGHT_SNAPSHOT_SCHEMA_VERSION = "live-preflight-snapshot-v1"
RUNTIME_SESSION_SCHEMA_VERSION = "live-runtime-session-v1"
RUNTIME_EVENT_SCHEMA_VERSION = "live-runtime-event-v1"
INCIDENT_SCHEMA_VERSION = "live-incident-v1"
INCIDENT_EVENT_SCHEMA_VERSION = "live-incident-event-v1"
STORE_SCHEMA_VERSION = "live-operational-governance-store-v1"

DEFAULT_PREFLIGHT_TTL_SECONDS = 300
MAX_PREFLIGHT_TTL_SECONDS = 3600

LIVE_MODES = frozenset({"SMALL_LIVE", "FULL_LIVE"})
RUNTIME_MODES = frozenset({"MONITOR", *LIVE_MODES})
RUNTIME_LIFECYCLES = frozenset(
    {
        "PREFLIGHT",
        "STARTING",
        "RUNNING",
        "DEGRADED",
        "DRAINING",
        "STOPPING",
        "STOPPED",
        "FAILED",
    }
)
RUNTIME_TERMINAL_LIFECYCLES = frozenset({"STOPPED", "FAILED"})
RUNTIME_TRANSITIONS: dict[str, frozenset[str]] = {
    "PREFLIGHT": frozenset({"STARTING", "FAILED"}),
    "STARTING": frozenset({"RUNNING", "DEGRADED", "STOPPING", "FAILED"}),
    "RUNNING": frozenset({"DEGRADED", "DRAINING", "STOPPING", "FAILED"}),
    "DEGRADED": frozenset({"RUNNING", "DRAINING", "STOPPING", "FAILED"}),
    "DRAINING": frozenset({"STOPPING", "STOPPED", "FAILED"}),
    "STOPPING": frozenset({"STOPPED", "FAILED"}),
}

INCIDENT_STATES = frozenset({"OPEN", "ACKNOWLEDGED", "MITIGATING", "RESOLVED"})
INCIDENT_TRANSITIONS: dict[str, frozenset[str]] = {
    "OPEN": frozenset({"ACKNOWLEDGED", "MITIGATING", "RESOLVED"}),
    "ACKNOWLEDGED": frozenset({"MITIGATING", "RESOLVED"}),
    "MITIGATING": frozenset({"RESOLVED"}),
}
INCIDENT_SEVERITIES = frozenset({"INFO", "WARNING", "ERROR", "CRITICAL"})
INCIDENT_SEVERITY_RANK = {
    "INFO": 0,
    "WARNING": 1,
    "ERROR": 2,
    "CRITICAL": 3,
}
INCIDENT_SCOPE_TYPES = frozenset(
    {"GLOBAL", "DEPLOYMENT", "SESSION", "ROUTE", "STRATEGY", "INSTRUMENT"}
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_ACCOUNT_KEYS = frozenset(
    {
        "account",
        "accountid",
        "accountno",
        "accountnumber",
        "brokeraccount",
        "brokeraccountid",
        "brokeraccountno",
        "brokeraccountnumber",
        "cano",
        "kisaccountno",
        "paperaccountid",
    }
)


def stable_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_text(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    if len(text) > 500:
        raise ValueError(f"{label} is too long")
    return text


def _required_sha256(value: object, label: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{label} must be a SHA-256 hex fingerprint")
    return text


def _optional_sha256(value: object, label: str) -> str:
    text = str(value or "").strip()
    return _required_sha256(text, label) if text else ""


def _positive_revision(value: object, label: str) -> int:
    try:
        revision = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be an integer") from exc
    if revision < 1:
        raise ValueError(f"{label} must be at least 1")
    return revision


def _utc_iso(value: datetime | str, label: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{label} is required")
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: datetime | str, label: str) -> datetime:
    return datetime.fromisoformat(_utc_iso(value, label).replace("Z", "+00:00"))


def _normalized_key(value: object) -> str:
    return "".join(character for character in str(value).lower() if character.isalnum())


def _assert_no_raw_account_material(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = _normalized_key(key)
            if (
                normalized in _FORBIDDEN_ACCOUNT_KEYS
                or (
                    normalized.endswith(("accountid", "accountno", "accountnumber"))
                    and "fingerprint" not in normalized
                )
            ):
                raise ValueError(
                    f"{path}.{key} may not contain a raw account identifier; "
                    "pass accountFingerprint instead"
                )
            _assert_no_raw_account_material(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_no_raw_account_material(item, f"{path}[{index}]")


def _json_object(value: Mapping[str, Any] | None, label: str) -> dict[str, Any]:
    source = dict(value or {})
    _assert_no_raw_account_material(source, label)
    try:
        encoded = json.dumps(
            source,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must contain strict JSON values") from exc
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} must be an object")
    return decoded


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _enum_text(value: object, allowed: Iterable[str], label: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in set(allowed):
        raise ValueError(f"unsupported {label}: {normalized or '-'}")
    return normalized


def _normalized_preflight_checks(
    checks: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(checks):
        item = _json_object(raw, f"checks[{index}]")
        check_id = _required_text(
            item.get("checkId") or item.get("check_id"),
            f"checks[{index}].checkId",
        )
        if check_id in seen:
            raise ValueError(f"duplicate preflight check ID: {check_id}")
        seen.add(check_id)
        status = str(item.get("status") or "").strip().upper()
        status = {"BLOCK": "FAIL", "BLOCKED": "FAIL", "WARNING": "WARN"}.get(
            status,
            status,
        )
        if status not in {"PASS", "WARN", "FAIL"}:
            raise ValueError(f"unsupported preflight check status: {status or '-'}")
        evidence_hash = _optional_sha256(
            item.get("evidenceHash") or item.get("evidence_hash"),
            f"checks[{index}].evidenceHash",
        )
        result.append(
            {
                "checkId": check_id,
                "status": status,
                "detail": str(item.get("detail") or "").strip(),
                "evidenceHash": evidence_hash,
            }
        )
    if not result:
        raise ValueError("at least one preflight check is required")
    return tuple(sorted(result, key=lambda item: item["checkId"]))


def _preflight_status(checks: Iterable[Mapping[str, Any]]) -> str:
    statuses = {str(item.get("status") or "").upper() for item in checks}
    if "FAIL" in statuses:
        return "BLOCKED"
    if "WARN" in statuses:
        return "REVIEW_REQUIRED"
    return "PASS"


@dataclass(frozen=True)
class DeploymentManifest:
    deployment_id: str
    revision: int
    previous_manifest_hash: str
    strategy_artifact_hash: str
    portfolio_artifact_hash: str
    account_fingerprint: str
    broker_route: str
    runtime_version: str
    build_hash: str
    execution_adapter: str
    execution_adapter_version: str
    risk_policy_revision: int
    risk_policy_hash: str
    config_revision: int
    config_hash: str
    preflight_ttl_seconds: int
    created_at: str
    metadata: Mapping[str, Any]
    schema_version: str = DEPLOYMENT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "deployment_id", _required_text(self.deployment_id, "deployment_id"))
        object.__setattr__(self, "revision", _positive_revision(self.revision, "revision"))
        object.__setattr__(
            self,
            "previous_manifest_hash",
            _optional_sha256(self.previous_manifest_hash, "previous_manifest_hash"),
        )
        if self.revision == 1 and self.previous_manifest_hash:
            raise ValueError("the first deployment revision cannot have a previous hash")
        if self.revision > 1 and not self.previous_manifest_hash:
            raise ValueError("a later deployment revision requires a previous hash")
        for name in (
            "strategy_artifact_hash",
            "account_fingerprint",
            "build_hash",
            "risk_policy_hash",
            "config_hash",
        ):
            object.__setattr__(self, name, _required_sha256(getattr(self, name), name))
        object.__setattr__(
            self,
            "portfolio_artifact_hash",
            _optional_sha256(self.portfolio_artifact_hash, "portfolio_artifact_hash"),
        )
        for name in (
            "broker_route",
            "runtime_version",
            "execution_adapter",
            "execution_adapter_version",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "risk_policy_revision",
            _positive_revision(self.risk_policy_revision, "risk_policy_revision"),
        )
        object.__setattr__(
            self,
            "config_revision",
            _positive_revision(self.config_revision, "config_revision"),
        )
        try:
            ttl = int(self.preflight_ttl_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("preflight_ttl_seconds must be an integer") from exc
        if not 1 <= ttl <= MAX_PREFLIGHT_TTL_SECONDS:
            raise ValueError(
                f"preflight_ttl_seconds must be between 1 and {MAX_PREFLIGHT_TTL_SECONDS}"
            )
        object.__setattr__(self, "preflight_ttl_seconds", ttl)
        object.__setattr__(self, "created_at", _utc_iso(self.created_at, "created_at"))
        object.__setattr__(self, "metadata", _json_object(self.metadata, "metadata"))
        if self.schema_version != DEPLOYMENT_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported deployment manifest schema")

    @property
    def manifest_hash(self) -> str:
        return stable_sha256(self.to_dict(include_hash=False))

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schemaVersion": self.schema_version,
            "deploymentId": self.deployment_id,
            "revision": self.revision,
            "previousManifestHash": self.previous_manifest_hash,
            "strategyArtifactHash": self.strategy_artifact_hash,
            "portfolioArtifactHash": self.portfolio_artifact_hash,
            "accountFingerprint": self.account_fingerprint,
            "brokerRoute": self.broker_route,
            "runtimeVersion": self.runtime_version,
            "buildHash": self.build_hash,
            "executionAdapter": self.execution_adapter,
            "executionAdapterVersion": self.execution_adapter_version,
            "riskPolicyRevision": self.risk_policy_revision,
            "riskPolicyHash": self.risk_policy_hash,
            "configRevision": self.config_revision,
            "configHash": self.config_hash,
            "preflightTtlSeconds": self.preflight_ttl_seconds,
            "createdAt": self.created_at,
            "metadata": dict(self.metadata),
        }
        if include_hash:
            payload["manifestHash"] = self.manifest_hash
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "DeploymentManifest":
        manifest = cls(
            deployment_id=str(payload.get("deploymentId") or ""),
            revision=payload.get("revision", 0),
            previous_manifest_hash=str(payload.get("previousManifestHash") or ""),
            strategy_artifact_hash=str(payload.get("strategyArtifactHash") or ""),
            portfolio_artifact_hash=str(payload.get("portfolioArtifactHash") or ""),
            account_fingerprint=str(payload.get("accountFingerprint") or ""),
            broker_route=str(payload.get("brokerRoute") or ""),
            runtime_version=str(payload.get("runtimeVersion") or ""),
            build_hash=str(payload.get("buildHash") or ""),
            execution_adapter=str(payload.get("executionAdapter") or ""),
            execution_adapter_version=str(payload.get("executionAdapterVersion") or ""),
            risk_policy_revision=payload.get("riskPolicyRevision", 0),
            risk_policy_hash=str(payload.get("riskPolicyHash") or ""),
            config_revision=payload.get("configRevision", 0),
            config_hash=str(payload.get("configHash") or ""),
            preflight_ttl_seconds=payload.get("preflightTtlSeconds", 0),
            created_at=str(payload.get("createdAt") or ""),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {},
            schema_version=str(payload.get("schemaVersion") or ""),
        )
        declared_hash = str(payload.get("manifestHash") or "").strip().lower()
        if declared_hash and not hmac.compare_digest(declared_hash, manifest.manifest_hash):
            raise ValueError("deployment manifest hash mismatch")
        return manifest


@dataclass(frozen=True)
class PreflightSnapshot:
    snapshot_id: str
    deployment_id: str
    deployment_revision: int
    deployment_manifest_hash: str
    account_fingerprint: str
    broker_route: str
    config_revision: int
    config_hash: str
    reconciliation_hash: str
    broker_snapshot_hash: str
    status: str
    checks: tuple[Mapping[str, Any], ...]
    issued_at: str
    expires_at: str
    metadata: Mapping[str, Any]
    schema_version: str = PREFLIGHT_SNAPSHOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in ("snapshot_id", "deployment_id", "broker_route"):
            object.__setattr__(self, name, _required_text(getattr(self, name), name))
        object.__setattr__(
            self,
            "deployment_revision",
            _positive_revision(self.deployment_revision, "deployment_revision"),
        )
        object.__setattr__(
            self,
            "config_revision",
            _positive_revision(self.config_revision, "config_revision"),
        )
        for name in (
            "deployment_manifest_hash",
            "account_fingerprint",
            "config_hash",
            "reconciliation_hash",
            "broker_snapshot_hash",
        ):
            object.__setattr__(self, name, _required_sha256(getattr(self, name), name))
        checks = _normalized_preflight_checks(self.checks)
        object.__setattr__(self, "checks", checks)
        status = _enum_text(
            self.status,
            {"PASS", "REVIEW_REQUIRED", "BLOCKED"},
            "preflight status",
        )
        if status != _preflight_status(checks):
            raise ValueError("preflight status does not match check results")
        object.__setattr__(self, "status", status)
        issued = _utc_iso(self.issued_at, "issued_at")
        expires = _utc_iso(self.expires_at, "expires_at")
        if _parse_utc(expires, "expires_at") <= _parse_utc(issued, "issued_at"):
            raise ValueError("preflight expiry must be after issue time")
        object.__setattr__(self, "issued_at", issued)
        object.__setattr__(self, "expires_at", expires)
        object.__setattr__(self, "metadata", _json_object(self.metadata, "metadata"))
        if self.schema_version != PREFLIGHT_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported preflight snapshot schema")

    @property
    def snapshot_hash(self) -> str:
        return stable_sha256(self.to_dict(include_hash=False))

    def is_expired(self, at: datetime | str) -> bool:
        return _parse_utc(at, "at") >= _parse_utc(self.expires_at, "expires_at")

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schemaVersion": self.schema_version,
            "snapshotId": self.snapshot_id,
            "deploymentId": self.deployment_id,
            "deploymentRevision": self.deployment_revision,
            "deploymentManifestHash": self.deployment_manifest_hash,
            "accountFingerprint": self.account_fingerprint,
            "brokerRoute": self.broker_route,
            "configRevision": self.config_revision,
            "configHash": self.config_hash,
            "reconciliationHash": self.reconciliation_hash,
            "brokerSnapshotHash": self.broker_snapshot_hash,
            "status": self.status,
            "checks": [dict(item) for item in self.checks],
            "issuedAt": self.issued_at,
            "expiresAt": self.expires_at,
            "metadata": dict(self.metadata),
        }
        if include_hash:
            payload["snapshotHash"] = self.snapshot_hash
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PreflightSnapshot":
        snapshot = cls(
            snapshot_id=str(payload.get("snapshotId") or ""),
            deployment_id=str(payload.get("deploymentId") or ""),
            deployment_revision=payload.get("deploymentRevision", 0),
            deployment_manifest_hash=str(payload.get("deploymentManifestHash") or ""),
            account_fingerprint=str(payload.get("accountFingerprint") or ""),
            broker_route=str(payload.get("brokerRoute") or ""),
            config_revision=payload.get("configRevision", 0),
            config_hash=str(payload.get("configHash") or ""),
            reconciliation_hash=str(payload.get("reconciliationHash") or ""),
            broker_snapshot_hash=str(payload.get("brokerSnapshotHash") or ""),
            status=str(payload.get("status") or ""),
            checks=tuple(
                item for item in payload.get("checks", ()) if isinstance(item, Mapping)
            ),
            issued_at=str(payload.get("issuedAt") or ""),
            expires_at=str(payload.get("expiresAt") or ""),
            metadata=payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {},
            schema_version=str(payload.get("schemaVersion") or ""),
        )
        declared_hash = str(payload.get("snapshotHash") or "").strip().lower()
        if declared_hash and not hmac.compare_digest(declared_hash, snapshot.snapshot_hash):
            raise ValueError("preflight snapshot hash mismatch")
        return snapshot


@dataclass(frozen=True)
class RuntimeEvent:
    session_id: str
    sequence: int
    event_id: str
    previous_hash: str
    event_hash: str
    lifecycle: str
    event_type: str
    actor: str
    payload: Mapping[str, Any]
    occurred_at: str
    schema_version: str = RUNTIME_EVENT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "sessionId": self.session_id,
            "sequence": self.sequence,
            "eventId": self.event_id,
            "previousHash": self.previous_hash,
            "eventHash": self.event_hash,
            "lifecycle": self.lifecycle,
            "eventType": self.event_type,
            "actor": self.actor,
            "payload": dict(self.payload),
            "occurredAt": self.occurred_at,
        }


@dataclass(frozen=True)
class RuntimeSession:
    session_id: str
    deployment_id: str
    deployment_manifest_hash: str
    preflight_snapshot_id: str
    preflight_snapshot_hash: str
    config_revision: int
    config_hash: str
    profile: str
    mode: str
    runtime_instance_id: str
    lifecycle: str
    created_at: str
    metadata: Mapping[str, Any]
    schema_version: str = RUNTIME_SESSION_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "sessionId": self.session_id,
            "deploymentId": self.deployment_id,
            "deploymentManifestHash": self.deployment_manifest_hash,
            "preflightSnapshotId": self.preflight_snapshot_id,
            "preflightSnapshotHash": self.preflight_snapshot_hash,
            "configRevision": self.config_revision,
            "configHash": self.config_hash,
            "profile": self.profile,
            "mode": self.mode,
            "runtimeInstanceId": self.runtime_instance_id,
            "lifecycle": self.lifecycle,
            "createdAt": self.created_at,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class IncidentEvent:
    incident_id: str
    sequence: int
    event_id: str
    previous_hash: str
    event_hash: str
    state: str
    severity: str
    event_type: str
    actor: str
    evidence_hash: str
    payload: Mapping[str, Any]
    occurred_at: str
    schema_version: str = INCIDENT_EVENT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "incidentId": self.incident_id,
            "sequence": self.sequence,
            "eventId": self.event_id,
            "previousHash": self.previous_hash,
            "eventHash": self.event_hash,
            "state": self.state,
            "severity": self.severity,
            "eventType": self.event_type,
            "actor": self.actor,
            "evidenceHash": self.evidence_hash,
            "payload": dict(self.payload),
            "occurredAt": self.occurred_at,
        }


@dataclass(frozen=True)
class Incident:
    incident_id: str
    dedupe_key: str
    code: str
    scope_type: str
    scope_id: str
    deployment_id: str
    session_id: str
    account_fingerprint: str
    state: str
    severity: str
    opened_at: str
    schema_version: str = INCIDENT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "incidentId": self.incident_id,
            "dedupeKey": self.dedupe_key,
            "code": self.code,
            "scopeType": self.scope_type,
            "scopeId": self.scope_id,
            "deploymentId": self.deployment_id,
            "sessionId": self.session_id,
            "accountFingerprint": self.account_fingerprint,
            "state": self.state,
            "severity": self.severity,
            "openedAt": self.opened_at,
        }


class OperationalGovernanceStore:
    """Append-only operational identity, preflight, session, and incident store.

    The caller must derive account fingerprints before invoking this API.  No
    method accepts or derives a raw broker account number.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def _transaction(self):
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    def _now(self, value: datetime | str | None = None) -> str:
        return _utc_iso(value if value is not None else self._clock(), "occurred_at")

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS operational_governance_meta (
                    schema_version TEXT PRIMARY KEY,
                    initialized_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS live_deployment_manifests (
                    deployment_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision >= 1),
                    manifest_hash TEXT NOT NULL UNIQUE,
                    previous_manifest_hash TEXT NOT NULL DEFAULT '',
                    account_fingerprint TEXT NOT NULL,
                    config_revision INTEGER NOT NULL CHECK(config_revision >= 1),
                    config_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(deployment_id, revision)
                );

                CREATE TABLE IF NOT EXISTS live_preflight_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    snapshot_hash TEXT NOT NULL UNIQUE,
                    deployment_id TEXT NOT NULL,
                    deployment_revision INTEGER NOT NULL,
                    deployment_manifest_hash TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    config_revision INTEGER NOT NULL,
                    config_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('PASS', 'REVIEW_REQUIRED', 'BLOCKED')),
                    issued_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(deployment_manifest_hash)
                        REFERENCES live_deployment_manifests(manifest_hash)
                );

                CREATE INDEX IF NOT EXISTS live_preflight_scope_idx
                    ON live_preflight_snapshots
                    (deployment_id, deployment_revision, issued_at);

                CREATE TABLE IF NOT EXISTS live_runtime_sessions (
                    session_id TEXT PRIMARY KEY,
                    deployment_id TEXT NOT NULL,
                    deployment_manifest_hash TEXT NOT NULL,
                    preflight_snapshot_id TEXT NOT NULL DEFAULT '',
                    preflight_snapshot_hash TEXT NOT NULL DEFAULT '',
                    config_revision INTEGER NOT NULL,
                    config_hash TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('MONITOR', 'SMALL_LIVE', 'FULL_LIVE')),
                    runtime_instance_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    FOREIGN KEY(deployment_manifest_hash)
                        REFERENCES live_deployment_manifests(manifest_hash)
                );

                CREATE TABLE IF NOT EXISTS live_runtime_events (
                    session_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK(sequence >= 1),
                    event_id TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL DEFAULT '',
                    event_hash TEXT NOT NULL UNIQUE,
                    lifecycle TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, sequence),
                    FOREIGN KEY(session_id) REFERENCES live_runtime_sessions(session_id)
                );

                CREATE TABLE IF NOT EXISTS live_incidents (
                    incident_id TEXT PRIMARY KEY,
                    dedupe_key TEXT NOT NULL,
                    code TEXT NOT NULL,
                    scope_type TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    deployment_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    account_fingerprint TEXT NOT NULL DEFAULT '',
                    opened_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS live_incident_dedupe_idx
                    ON live_incidents(dedupe_key, opened_at);

                CREATE TABLE IF NOT EXISTS live_incident_events (
                    incident_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK(sequence >= 1),
                    event_id TEXT NOT NULL UNIQUE,
                    previous_hash TEXT NOT NULL DEFAULT '',
                    event_hash TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    PRIMARY KEY(incident_id, sequence),
                    FOREIGN KEY(incident_id) REFERENCES live_incidents(incident_id)
                );

                CREATE TRIGGER IF NOT EXISTS live_deployment_manifests_no_update
                BEFORE UPDATE ON live_deployment_manifests
                BEGIN SELECT RAISE(ABORT, 'deployment manifests are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_deployment_manifests_no_delete
                BEFORE DELETE ON live_deployment_manifests
                BEGIN SELECT RAISE(ABORT, 'deployment manifests are immutable'); END;

                CREATE TRIGGER IF NOT EXISTS live_preflight_snapshots_no_update
                BEFORE UPDATE ON live_preflight_snapshots
                BEGIN SELECT RAISE(ABORT, 'preflight snapshots are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_preflight_snapshots_no_delete
                BEFORE DELETE ON live_preflight_snapshots
                BEGIN SELECT RAISE(ABORT, 'preflight snapshots are immutable'); END;

                CREATE TRIGGER IF NOT EXISTS live_runtime_sessions_no_update
                BEFORE UPDATE ON live_runtime_sessions
                BEGIN SELECT RAISE(ABORT, 'runtime sessions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_runtime_sessions_no_delete
                BEFORE DELETE ON live_runtime_sessions
                BEGIN SELECT RAISE(ABORT, 'runtime sessions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_runtime_events_no_update
                BEFORE UPDATE ON live_runtime_events
                BEGIN SELECT RAISE(ABORT, 'runtime events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS live_runtime_events_no_delete
                BEFORE DELETE ON live_runtime_events
                BEGIN SELECT RAISE(ABORT, 'runtime events are append-only'); END;

                CREATE TRIGGER IF NOT EXISTS live_incidents_no_update
                BEFORE UPDATE ON live_incidents
                BEGIN SELECT RAISE(ABORT, 'incidents are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_incidents_no_delete
                BEFORE DELETE ON live_incidents
                BEGIN SELECT RAISE(ABORT, 'incidents are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS live_incident_events_no_update
                BEFORE UPDATE ON live_incident_events
                BEGIN SELECT RAISE(ABORT, 'incident events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS live_incident_events_no_delete
                BEFORE DELETE ON live_incident_events
                BEGIN SELECT RAISE(ABORT, 'incident events are append-only'); END;
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO operational_governance_meta "
                "(schema_version, initialized_at) VALUES (?, ?)",
                (STORE_SCHEMA_VERSION, self._now()),
            )
            connection.commit()

    def create_deployment_manifest(
        self,
        *,
        deployment_id: str,
        strategy_artifact_hash: str,
        portfolio_artifact_hash: str = "",
        account_fingerprint: str,
        broker_route: str,
        runtime_version: str,
        build_hash: str,
        execution_adapter: str,
        execution_adapter_version: str,
        risk_policy_revision: int,
        risk_policy_hash: str,
        config_revision: int,
        config_hash: str,
        preflight_ttl_seconds: int = DEFAULT_PREFLIGHT_TTL_SECONDS,
        metadata: Mapping[str, Any] | None = None,
        created_at: datetime | str | None = None,
    ) -> DeploymentManifest:
        normalized_id = _required_text(deployment_id, "deployment_id")
        occurred_at = self._now(created_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT revision, manifest_hash FROM live_deployment_manifests "
                "WHERE deployment_id = ? ORDER BY revision DESC LIMIT 1",
                (normalized_id,),
            ).fetchone()
            revision = int(row["revision"] if row is not None else 0) + 1
            previous_hash = str(row["manifest_hash"] if row is not None else "")
            manifest = DeploymentManifest(
                deployment_id=normalized_id,
                revision=revision,
                previous_manifest_hash=previous_hash,
                strategy_artifact_hash=strategy_artifact_hash,
                portfolio_artifact_hash=portfolio_artifact_hash,
                account_fingerprint=account_fingerprint,
                broker_route=broker_route,
                runtime_version=runtime_version,
                build_hash=build_hash,
                execution_adapter=execution_adapter,
                execution_adapter_version=execution_adapter_version,
                risk_policy_revision=risk_policy_revision,
                risk_policy_hash=risk_policy_hash,
                config_revision=config_revision,
                config_hash=config_hash,
                preflight_ttl_seconds=preflight_ttl_seconds,
                created_at=occurred_at,
                metadata=metadata or {},
            )
            connection.execute(
                """
                INSERT INTO live_deployment_manifests
                (deployment_id, revision, manifest_hash, previous_manifest_hash,
                 account_fingerprint, config_revision, config_hash,
                 payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest.deployment_id,
                    manifest.revision,
                    manifest.manifest_hash,
                    manifest.previous_manifest_hash,
                    manifest.account_fingerprint,
                    manifest.config_revision,
                    manifest.config_hash,
                    json.dumps(manifest.to_dict(), ensure_ascii=False, sort_keys=True),
                    manifest.created_at,
                ),
            )
        return manifest

    def get_deployment_manifest(
        self,
        deployment_id: str,
        revision: int | None = None,
    ) -> DeploymentManifest | None:
        with closing(self._connect()) as connection:
            if revision is None:
                row = connection.execute(
                    "SELECT payload_json FROM live_deployment_manifests "
                    "WHERE deployment_id = ? ORDER BY revision DESC LIMIT 1",
                    (str(deployment_id),),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT payload_json FROM live_deployment_manifests "
                    "WHERE deployment_id = ? AND revision = ?",
                    (str(deployment_id), int(revision)),
                ).fetchone()
        return self._manifest_from_row(row)

    def get_deployment_manifest_by_hash(
        self,
        manifest_hash: str,
    ) -> DeploymentManifest | None:
        normalized_hash = _required_sha256(manifest_hash, "manifest_hash")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM live_deployment_manifests WHERE manifest_hash = ?",
                (normalized_hash,),
            ).fetchone()
        return self._manifest_from_row(row)

    @staticmethod
    def _manifest_from_row(row: sqlite3.Row | None) -> DeploymentManifest | None:
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as exc:
            raise ValueError("deployment manifest JSON is corrupt") from exc
        return DeploymentManifest.from_dict(payload)

    def create_preflight_snapshot(
        self,
        *,
        deployment_id: str,
        deployment_manifest_hash: str,
        checks: Iterable[Mapping[str, Any]],
        reconciliation_hash: str,
        broker_snapshot_hash: str,
        ttl_seconds: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        issued_at: datetime | str | None = None,
        snapshot_id: str = "",
    ) -> PreflightSnapshot:
        normalized_manifest_hash = _required_sha256(
            deployment_manifest_hash,
            "deployment_manifest_hash",
        )
        issued = self._now(issued_at)
        with self._transaction() as connection:
            manifest = self._manifest_by_hash_conn(connection, normalized_manifest_hash)
            if manifest is None or manifest.deployment_id != str(deployment_id).strip():
                raise ValueError("deployment manifest does not match deployment_id")
            latest = self._latest_manifest_conn(connection, manifest.deployment_id)
            if latest is None or latest.manifest_hash != manifest.manifest_hash:
                raise ValueError("deployment manifest has been superseded")
            ttl = manifest.preflight_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
            if not 1 <= ttl <= min(manifest.preflight_ttl_seconds, MAX_PREFLIGHT_TTL_SECONDS):
                raise ValueError("preflight TTL exceeds the deployment policy")
            normalized_checks = _normalized_preflight_checks(checks)
            snapshot = PreflightSnapshot(
                snapshot_id=snapshot_id or _new_id("preflight"),
                deployment_id=manifest.deployment_id,
                deployment_revision=manifest.revision,
                deployment_manifest_hash=manifest.manifest_hash,
                account_fingerprint=manifest.account_fingerprint,
                broker_route=manifest.broker_route,
                config_revision=manifest.config_revision,
                config_hash=manifest.config_hash,
                reconciliation_hash=reconciliation_hash,
                broker_snapshot_hash=broker_snapshot_hash,
                status=_preflight_status(normalized_checks),
                checks=normalized_checks,
                issued_at=issued,
                expires_at=(
                    _parse_utc(issued, "issued_at") + timedelta(seconds=ttl)
                ).isoformat().replace("+00:00", "Z"),
                metadata=metadata or {},
            )
            connection.execute(
                """
                INSERT INTO live_preflight_snapshots
                (snapshot_id, snapshot_hash, deployment_id, deployment_revision,
                 deployment_manifest_hash, account_fingerprint,
                 config_revision, config_hash, status, issued_at, expires_at,
                 payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.snapshot_hash,
                    snapshot.deployment_id,
                    snapshot.deployment_revision,
                    snapshot.deployment_manifest_hash,
                    snapshot.account_fingerprint,
                    snapshot.config_revision,
                    snapshot.config_hash,
                    snapshot.status,
                    snapshot.issued_at,
                    snapshot.expires_at,
                    json.dumps(snapshot.to_dict(), ensure_ascii=False, sort_keys=True),
                ),
            )
        return snapshot

    def get_preflight_snapshot(self, snapshot_id: str) -> PreflightSnapshot | None:
        with closing(self._connect()) as connection:
            return self._preflight_conn(connection, snapshot_id)

    def preflight_validity(
        self,
        snapshot_id: str,
        *,
        at: datetime | str | None = None,
    ) -> dict[str, Any]:
        current = self._now(at)
        with closing(self._connect()) as connection:
            return self._preflight_validity_conn(connection, snapshot_id, current)

    def _preflight_validity_conn(
        self,
        connection: sqlite3.Connection,
        snapshot_id: str,
        current: str,
    ) -> dict[str, Any]:
        snapshot = self._preflight_conn(connection, snapshot_id)
        if snapshot is None:
            return {"valid": False, "reasons": ["preflight-snapshot-missing"], "snapshot": None}
        reasons: list[str] = []
        if snapshot.status != "PASS":
            reasons.append(f"preflight-status-{snapshot.status.lower()}")
        if snapshot.is_expired(current):
            reasons.append("preflight-expired")
        latest = self._latest_manifest_conn(connection, snapshot.deployment_id)
        if latest is None:
            reasons.append("deployment-manifest-missing")
        elif latest.manifest_hash != snapshot.deployment_manifest_hash:
            reasons.append("deployment-manifest-superseded")
        elif (
            latest.config_revision != snapshot.config_revision
            or latest.config_hash != snapshot.config_hash
        ):
            reasons.append("preflight-config-mismatch")
        return {
            "valid": not reasons,
            "reasons": reasons,
            "snapshot": snapshot.to_dict(),
            "checkedAt": current,
        }

    @staticmethod
    def _preflight_conn(
        connection: sqlite3.Connection,
        snapshot_id: str,
    ) -> PreflightSnapshot | None:
        row = connection.execute(
            "SELECT payload_json FROM live_preflight_snapshots WHERE snapshot_id = ?",
            (str(snapshot_id),),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as exc:
            raise ValueError("preflight snapshot JSON is corrupt") from exc
        return PreflightSnapshot.from_dict(payload)

    @staticmethod
    def _manifest_by_hash_conn(
        connection: sqlite3.Connection,
        manifest_hash: str,
    ) -> DeploymentManifest | None:
        row = connection.execute(
            "SELECT payload_json FROM live_deployment_manifests WHERE manifest_hash = ?",
            (manifest_hash,),
        ).fetchone()
        return OperationalGovernanceStore._manifest_from_row(row)

    @staticmethod
    def _latest_manifest_conn(
        connection: sqlite3.Connection,
        deployment_id: str,
    ) -> DeploymentManifest | None:
        row = connection.execute(
            "SELECT payload_json FROM live_deployment_manifests "
            "WHERE deployment_id = ? ORDER BY revision DESC LIMIT 1",
            (deployment_id,),
        ).fetchone()
        return OperationalGovernanceStore._manifest_from_row(row)

    def create_runtime_session(
        self,
        *,
        deployment_id: str,
        deployment_manifest_hash: str,
        profile: str,
        mode: str,
        runtime_instance_id: str,
        preflight_snapshot_id: str = "",
        actor: str = "live_trader",
        metadata: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
        session_id: str = "",
    ) -> RuntimeSession:
        normalized_mode = _enum_text(mode, RUNTIME_MODES, "runtime mode")
        current = self._now(occurred_at)
        normalized_manifest_hash = _required_sha256(
            deployment_manifest_hash,
            "deployment_manifest_hash",
        )
        with self._transaction() as connection:
            manifest = self._manifest_by_hash_conn(connection, normalized_manifest_hash)
            if manifest is None or manifest.deployment_id != str(deployment_id).strip():
                raise ValueError("deployment manifest does not match deployment_id")
            latest = self._latest_manifest_conn(connection, manifest.deployment_id)
            if latest is None or latest.manifest_hash != manifest.manifest_hash:
                raise ValueError("deployment manifest has been superseded")
            preflight: PreflightSnapshot | None = None
            if preflight_snapshot_id:
                validity = self._preflight_validity_conn(
                    connection,
                    preflight_snapshot_id,
                    current,
                )
                if validity["valid"] is not True:
                    raise ValueError(
                        "preflight snapshot is not valid: "
                        + ", ".join(validity["reasons"])
                    )
                preflight = self._preflight_conn(connection, preflight_snapshot_id)
                if preflight is None or preflight.deployment_manifest_hash != manifest.manifest_hash:
                    raise ValueError("preflight snapshot is outside the deployment scope")
            elif normalized_mode in LIVE_MODES:
                raise ValueError("a valid preflight snapshot is required for live mode")

            normalized_session_id = session_id or _new_id("live-session")
            normalized_metadata = _json_object(metadata, "metadata")
            connection.execute(
                """
                INSERT INTO live_runtime_sessions
                (session_id, deployment_id, deployment_manifest_hash,
                 preflight_snapshot_id, preflight_snapshot_hash,
                 config_revision, config_hash, profile, mode,
                 runtime_instance_id, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_session_id,
                    manifest.deployment_id,
                    manifest.manifest_hash,
                    preflight.snapshot_id if preflight else "",
                    preflight.snapshot_hash if preflight else "",
                    manifest.config_revision,
                    manifest.config_hash,
                    _required_text(profile, "profile"),
                    normalized_mode,
                    _required_text(runtime_instance_id, "runtime_instance_id"),
                    current,
                    json.dumps(normalized_metadata, ensure_ascii=False, sort_keys=True),
                ),
            )
            self._append_runtime_event_conn(
                connection,
                normalized_session_id,
                lifecycle="PREFLIGHT",
                event_type="SESSION_CREATED",
                actor=actor,
                payload={
                    "deploymentManifestHash": manifest.manifest_hash,
                    "preflightSnapshotHash": preflight.snapshot_hash if preflight else "",
                },
                occurred_at=current,
            )
        session = self.get_runtime_session(normalized_session_id)
        if session is None:  # pragma: no cover - committed row invariant
            raise RuntimeError("runtime session was not persisted")
        return session

    def get_runtime_session(self, session_id: str) -> RuntimeSession | None:
        with closing(self._connect()) as connection:
            return self._runtime_session_conn(connection, session_id)

    @staticmethod
    def _runtime_session_conn(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> RuntimeSession | None:
        row = connection.execute(
            """
            SELECT s.*, e.lifecycle
            FROM live_runtime_sessions s
            JOIN live_runtime_events e
              ON e.session_id = s.session_id
             AND e.sequence = (
                SELECT MAX(last.sequence) FROM live_runtime_events last
                WHERE last.session_id = s.session_id
             )
            WHERE s.session_id = ?
            """,
            (str(session_id),),
        ).fetchone()
        if row is None:
            return None
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except json.JSONDecodeError as exc:
            raise ValueError("runtime session metadata is corrupt") from exc
        preflight_snapshot_id = str(row["preflight_snapshot_id"])
        preflight_snapshot_hash = str(row["preflight_snapshot_hash"])
        rebound = connection.execute(
            """
            SELECT payload_json
            FROM live_runtime_events
            WHERE session_id = ? AND event_type = 'PREFLIGHT_REBOUND'
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (str(session_id),),
        ).fetchone()
        if rebound is not None:
            try:
                rebound_payload = json.loads(str(rebound["payload_json"] or "{}"))
            except json.JSONDecodeError as exc:
                raise ValueError("runtime preflight binding event is corrupt") from exc
            preflight_snapshot_id = _required_text(
                rebound_payload.get("preflightSnapshotId"),
                "preflightSnapshotId",
            )
            preflight_snapshot_hash = _required_sha256(
                rebound_payload.get("preflightSnapshotHash"),
                "preflightSnapshotHash",
            )
        return RuntimeSession(
            session_id=str(row["session_id"]),
            deployment_id=str(row["deployment_id"]),
            deployment_manifest_hash=str(row["deployment_manifest_hash"]),
            preflight_snapshot_id=preflight_snapshot_id,
            preflight_snapshot_hash=preflight_snapshot_hash,
            config_revision=int(row["config_revision"]),
            config_hash=str(row["config_hash"]),
            profile=str(row["profile"]),
            mode=str(row["mode"]),
            runtime_instance_id=str(row["runtime_instance_id"]),
            lifecycle=str(row["lifecycle"]),
            created_at=str(row["created_at"]),
            metadata=metadata,
        )

    def transition_runtime_session(
        self,
        session_id: str,
        lifecycle: str,
        *,
        actor: str,
        event_type: str = "LIFECYCLE_TRANSITION",
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> RuntimeSession:
        target = _enum_text(lifecycle, RUNTIME_LIFECYCLES, "runtime lifecycle")
        current_time = self._now(occurred_at)
        with self._transaction() as connection:
            session = self._runtime_session_conn(connection, session_id)
            if session is None:
                raise ValueError("runtime session not found")
            if target == session.lifecycle:
                raise ValueError("use append_runtime_event for a non-transition event")
            if target not in RUNTIME_TRANSITIONS.get(session.lifecycle, frozenset()):
                raise ValueError(
                    f"invalid runtime transition: {session.lifecycle} -> {target}"
                )
            self._append_runtime_event_conn(
                connection,
                session.session_id,
                lifecycle=target,
                event_type=event_type,
                actor=actor,
                payload=payload or {},
                occurred_at=current_time,
            )
        updated = self.get_runtime_session(session_id)
        if updated is None:  # pragma: no cover - committed row invariant
            raise RuntimeError("runtime session disappeared")
        return updated

    def rebind_runtime_preflight(
        self,
        session_id: str,
        preflight_snapshot_id: str,
        *,
        actor: str,
        occurred_at: datetime | str | None = None,
    ) -> RuntimeSession:
        """Append-only rebind a running session to a fresh exact-scope preflight."""

        current_time = self._now(occurred_at)
        with self._transaction() as connection:
            session = self._runtime_session_conn(connection, session_id)
            if session is None:
                raise ValueError("runtime session not found")
            if session.lifecycle not in {"RUNNING", "DEGRADED"}:
                raise ValueError(
                    "runtime preflight can only be rebound while RUNNING or DEGRADED"
                )
            validity = self._preflight_validity_conn(
                connection,
                preflight_snapshot_id,
                current_time,
            )
            if validity.get("valid") is not True:
                raise ValueError(
                    "replacement preflight snapshot is not valid: "
                    + ", ".join(str(item) for item in validity.get("reasons", []))
                )
            replacement = self._preflight_conn(connection, preflight_snapshot_id)
            if replacement is None:  # pragma: no cover - validity invariant
                raise ValueError("replacement preflight snapshot is missing")
            if (
                replacement.deployment_id != session.deployment_id
                or replacement.deployment_manifest_hash
                != session.deployment_manifest_hash
                or replacement.config_revision != session.config_revision
                or replacement.config_hash != session.config_hash
            ):
                raise ValueError(
                    "replacement preflight snapshot is outside the runtime session scope"
                )
            manifest = self._manifest_by_hash_conn(
                connection,
                session.deployment_manifest_hash,
            )
            if (
                manifest is None
                or replacement.account_fingerprint != manifest.account_fingerprint
            ):
                raise ValueError(
                    "replacement preflight account fingerprint does not match the session"
                )
            self._append_runtime_event_conn(
                connection,
                session.session_id,
                lifecycle=session.lifecycle,
                event_type="PREFLIGHT_REBOUND",
                actor=actor,
                payload={
                    "previousPreflightSnapshotId": session.preflight_snapshot_id,
                    "previousPreflightSnapshotHash": session.preflight_snapshot_hash,
                    "preflightSnapshotId": replacement.snapshot_id,
                    "preflightSnapshotHash": replacement.snapshot_hash,
                    "deploymentManifestHash": session.deployment_manifest_hash,
                    "configHash": session.config_hash,
                    "accountFingerprint": replacement.account_fingerprint,
                },
                occurred_at=current_time,
            )
        updated = self.get_runtime_session(session_id)
        if updated is None:  # pragma: no cover - committed row invariant
            raise RuntimeError("runtime session disappeared")
        return updated

    def append_runtime_event(
        self,
        session_id: str,
        *,
        event_type: str,
        actor: str,
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> RuntimeEvent:
        current_time = self._now(occurred_at)
        with self._transaction() as connection:
            session = self._runtime_session_conn(connection, session_id)
            if session is None:
                raise ValueError("runtime session not found")
            return self._append_runtime_event_conn(
                connection,
                session.session_id,
                lifecycle=session.lifecycle,
                event_type=event_type,
                actor=actor,
                payload=payload or {},
                occurred_at=current_time,
            )

    def _append_runtime_event_conn(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        *,
        lifecycle: str,
        event_type: str,
        actor: str,
        payload: Mapping[str, Any],
        occurred_at: str,
    ) -> RuntimeEvent:
        lifecycle = _enum_text(lifecycle, RUNTIME_LIFECYCLES, "runtime lifecycle")
        event_type = _required_text(event_type, "event_type")
        actor = _required_text(actor, "actor")
        normalized_payload = _json_object(payload, "payload")
        previous = connection.execute(
            "SELECT sequence, event_hash, occurred_at FROM live_runtime_events "
            "WHERE session_id = ? ORDER BY sequence DESC LIMIT 1",
            (session_id,),
        ).fetchone()
        sequence = int(previous["sequence"] if previous is not None else 0) + 1
        previous_hash = str(previous["event_hash"] if previous is not None else "")
        if previous is not None and _parse_utc(occurred_at, "occurred_at") < _parse_utc(
            str(previous["occurred_at"]),
            "previous_occurred_at",
        ):
            raise ValueError("runtime event time cannot move backwards")
        event_id = _new_id("runtime-event")
        base = {
            "schemaVersion": RUNTIME_EVENT_SCHEMA_VERSION,
            "sessionId": session_id,
            "sequence": sequence,
            "eventId": event_id,
            "previousHash": previous_hash,
            "lifecycle": lifecycle,
            "eventType": event_type,
            "actor": actor,
            "payload": normalized_payload,
            "occurredAt": occurred_at,
        }
        event_hash = stable_sha256(base)
        connection.execute(
            """
            INSERT INTO live_runtime_events
            (session_id, sequence, event_id, previous_hash, event_hash,
             lifecycle, event_type, actor, payload_json, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                sequence,
                event_id,
                previous_hash,
                event_hash,
                lifecycle,
                event_type,
                actor,
                json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True),
                occurred_at,
            ),
        )
        return RuntimeEvent(
            session_id=session_id,
            sequence=sequence,
            event_id=event_id,
            previous_hash=previous_hash,
            event_hash=event_hash,
            lifecycle=lifecycle,
            event_type=event_type,
            actor=actor,
            payload=normalized_payload,
            occurred_at=occurred_at,
        )

    def runtime_events(self, session_id: str) -> list[RuntimeEvent]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM live_runtime_events WHERE session_id = ? ORDER BY sequence",
                (str(session_id),),
            ).fetchall()
        return [self._runtime_event_from_row(row) for row in rows]

    @staticmethod
    def _runtime_event_from_row(row: sqlite3.Row) -> RuntimeEvent:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError as exc:
            raise ValueError("runtime event payload is corrupt") from exc
        return RuntimeEvent(
            session_id=str(row["session_id"]),
            sequence=int(row["sequence"]),
            event_id=str(row["event_id"]),
            previous_hash=str(row["previous_hash"]),
            event_hash=str(row["event_hash"]),
            lifecycle=str(row["lifecycle"]),
            event_type=str(row["event_type"]),
            actor=str(row["actor"]),
            payload=payload,
            occurred_at=str(row["occurred_at"]),
        )

    def open_incident(
        self,
        *,
        code: str,
        scope_type: str,
        scope_id: str,
        severity: str,
        actor: str,
        deployment_id: str = "",
        session_id: str = "",
        account_fingerprint: str = "",
        evidence_hash: str = "",
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
        incident_id: str = "",
    ) -> tuple[Incident, bool]:
        normalized_code = _required_text(code, "code").upper()
        normalized_scope_type = _enum_text(
            scope_type,
            INCIDENT_SCOPE_TYPES,
            "incident scope",
        )
        normalized_scope_id = _required_text(scope_id, "scope_id")
        normalized_severity = _enum_text(
            severity,
            INCIDENT_SEVERITIES,
            "incident severity",
        )
        normalized_account = _optional_sha256(
            account_fingerprint,
            "account_fingerprint",
        )
        normalized_evidence = _optional_sha256(evidence_hash, "evidence_hash")
        current = self._now(occurred_at)
        normalized_payload = _json_object(payload, "payload")
        dedupe_key = stable_sha256(
            {
                "code": normalized_code,
                "scopeType": normalized_scope_type,
                "scopeId": normalized_scope_id,
                "deploymentId": str(deployment_id or "").strip(),
                "sessionId": str(session_id or "").strip(),
            }
        )
        with self._transaction() as connection:
            existing_row = connection.execute(
                """
                SELECT i.incident_id
                FROM live_incidents i
                JOIN live_incident_events e
                  ON e.incident_id = i.incident_id
                 AND e.sequence = (
                    SELECT MAX(last.sequence) FROM live_incident_events last
                    WHERE last.incident_id = i.incident_id
                 )
                WHERE i.dedupe_key = ? AND e.state <> 'RESOLVED'
                ORDER BY i.opened_at DESC LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
            if existing_row is not None:
                existing = self._incident_conn(connection, str(existing_row["incident_id"]))
                if existing is None:  # pragma: no cover - join invariant
                    raise RuntimeError("active incident disappeared")
                self._append_incident_event_conn(
                    connection,
                    existing.incident_id,
                    state=existing.state,
                    severity=normalized_severity,
                    event_type="INCIDENT_OBSERVED",
                    actor=actor,
                    evidence_hash=normalized_evidence,
                    payload=normalized_payload,
                    occurred_at=current,
                )
                updated = self._incident_conn(connection, existing.incident_id)
                if updated is None:  # pragma: no cover - committed row invariant
                    raise RuntimeError("incident disappeared")
                return updated, False

            normalized_incident_id = incident_id or _new_id("incident")
            connection.execute(
                """
                INSERT INTO live_incidents
                (incident_id, dedupe_key, code, scope_type, scope_id,
                 deployment_id, session_id, account_fingerprint, opened_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_incident_id,
                    dedupe_key,
                    normalized_code,
                    normalized_scope_type,
                    normalized_scope_id,
                    str(deployment_id or "").strip(),
                    str(session_id or "").strip(),
                    normalized_account,
                    current,
                ),
            )
            self._append_incident_event_conn(
                connection,
                normalized_incident_id,
                state="OPEN",
                severity=normalized_severity,
                event_type="INCIDENT_OPENED",
                actor=actor,
                evidence_hash=normalized_evidence,
                payload=normalized_payload,
                occurred_at=current,
            )
            created = self._incident_conn(connection, normalized_incident_id)
            if created is None:  # pragma: no cover - inserted row invariant
                raise RuntimeError("incident was not persisted")
            return created, True

    def get_incident(self, incident_id: str) -> Incident | None:
        with closing(self._connect()) as connection:
            return self._incident_conn(connection, incident_id)

    @staticmethod
    def _incident_conn(
        connection: sqlite3.Connection,
        incident_id: str,
    ) -> Incident | None:
        row = connection.execute(
            """
            SELECT i.*, e.state, e.severity
            FROM live_incidents i
            JOIN live_incident_events e
              ON e.incident_id = i.incident_id
             AND e.sequence = (
                SELECT MAX(last.sequence) FROM live_incident_events last
                WHERE last.incident_id = i.incident_id
             )
            WHERE i.incident_id = ?
            """,
            (str(incident_id),),
        ).fetchone()
        if row is None:
            return None
        return Incident(
            incident_id=str(row["incident_id"]),
            dedupe_key=str(row["dedupe_key"]),
            code=str(row["code"]),
            scope_type=str(row["scope_type"]),
            scope_id=str(row["scope_id"]),
            deployment_id=str(row["deployment_id"]),
            session_id=str(row["session_id"]),
            account_fingerprint=str(row["account_fingerprint"]),
            state=str(row["state"]),
            severity=str(row["severity"]),
            opened_at=str(row["opened_at"]),
        )

    def transition_incident(
        self,
        incident_id: str,
        state: str,
        *,
        actor: str,
        severity: str | None = None,
        event_type: str = "INCIDENT_TRANSITION",
        evidence_hash: str = "",
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> Incident:
        target = _enum_text(state, INCIDENT_STATES, "incident state")
        current_time = self._now(occurred_at)
        with self._transaction() as connection:
            incident = self._incident_conn(connection, incident_id)
            if incident is None:
                raise ValueError("incident not found")
            if target == incident.state:
                raise ValueError("use append_incident_event for a non-transition event")
            if target not in INCIDENT_TRANSITIONS.get(incident.state, frozenset()):
                raise ValueError(f"invalid incident transition: {incident.state} -> {target}")
            next_severity = incident.severity if severity is None else severity
            self._append_incident_event_conn(
                connection,
                incident.incident_id,
                state=target,
                severity=next_severity,
                event_type=event_type,
                actor=actor,
                evidence_hash=_optional_sha256(evidence_hash, "evidence_hash"),
                payload=payload or {},
                occurred_at=current_time,
            )
        updated = self.get_incident(incident_id)
        if updated is None:  # pragma: no cover - committed row invariant
            raise RuntimeError("incident disappeared")
        return updated

    def resolve_incident(
        self,
        incident_id: str,
        *,
        actor: str,
        evidence_hash: str = "",
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> Incident:
        return self.transition_incident(
            incident_id,
            "RESOLVED",
            actor=actor,
            event_type="INCIDENT_RESOLVED",
            evidence_hash=evidence_hash,
            payload=payload,
            occurred_at=occurred_at,
        )

    def append_incident_event(
        self,
        incident_id: str,
        *,
        event_type: str,
        actor: str,
        severity: str | None = None,
        evidence_hash: str = "",
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | str | None = None,
    ) -> IncidentEvent:
        current_time = self._now(occurred_at)
        with self._transaction() as connection:
            incident = self._incident_conn(connection, incident_id)
            if incident is None:
                raise ValueError("incident not found")
            if incident.state == "RESOLVED":
                raise ValueError("resolved incidents are terminal")
            return self._append_incident_event_conn(
                connection,
                incident.incident_id,
                state=incident.state,
                severity=severity or incident.severity,
                event_type=event_type,
                actor=actor,
                evidence_hash=_optional_sha256(evidence_hash, "evidence_hash"),
                payload=payload or {},
                occurred_at=current_time,
            )

    def _append_incident_event_conn(
        self,
        connection: sqlite3.Connection,
        incident_id: str,
        *,
        state: str,
        severity: str,
        event_type: str,
        actor: str,
        evidence_hash: str,
        payload: Mapping[str, Any],
        occurred_at: str,
    ) -> IncidentEvent:
        state = _enum_text(state, INCIDENT_STATES, "incident state")
        severity = _enum_text(severity, INCIDENT_SEVERITIES, "incident severity")
        event_type = _required_text(event_type, "event_type")
        actor = _required_text(actor, "actor")
        normalized_payload = _json_object(payload, "payload")
        previous = connection.execute(
            "SELECT sequence, event_hash, occurred_at, severity FROM live_incident_events "
            "WHERE incident_id = ? ORDER BY sequence DESC LIMIT 1",
            (incident_id,),
        ).fetchone()
        sequence = int(previous["sequence"] if previous is not None else 0) + 1
        previous_hash = str(previous["event_hash"] if previous is not None else "")
        if previous is not None:
            if _parse_utc(occurred_at, "occurred_at") < _parse_utc(
                str(previous["occurred_at"]),
                "previous_occurred_at",
            ):
                raise ValueError("incident event time cannot move backwards")
            previous_severity = str(previous["severity"])
            if INCIDENT_SEVERITY_RANK[severity] < INCIDENT_SEVERITY_RANK[previous_severity]:
                raise ValueError("incident severity cannot be downgraded")
        event_id = _new_id("incident-event")
        base = {
            "schemaVersion": INCIDENT_EVENT_SCHEMA_VERSION,
            "incidentId": incident_id,
            "sequence": sequence,
            "eventId": event_id,
            "previousHash": previous_hash,
            "state": state,
            "severity": severity,
            "eventType": event_type,
            "actor": actor,
            "evidenceHash": evidence_hash,
            "payload": normalized_payload,
            "occurredAt": occurred_at,
        }
        event_hash = stable_sha256(base)
        connection.execute(
            """
            INSERT INTO live_incident_events
            (incident_id, sequence, event_id, previous_hash, event_hash,
             state, severity, event_type, actor, evidence_hash,
             payload_json, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident_id,
                sequence,
                event_id,
                previous_hash,
                event_hash,
                state,
                severity,
                event_type,
                actor,
                evidence_hash,
                json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True),
                occurred_at,
            ),
        )
        return IncidentEvent(
            incident_id=incident_id,
            sequence=sequence,
            event_id=event_id,
            previous_hash=previous_hash,
            event_hash=event_hash,
            state=state,
            severity=severity,
            event_type=event_type,
            actor=actor,
            evidence_hash=evidence_hash,
            payload=normalized_payload,
            occurred_at=occurred_at,
        )

    def incident_events(self, incident_id: str) -> list[IncidentEvent]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM live_incident_events WHERE incident_id = ? ORDER BY sequence",
                (str(incident_id),),
            ).fetchall()
        return [self._incident_event_from_row(row) for row in rows]

    @staticmethod
    def _incident_event_from_row(row: sqlite3.Row) -> IncidentEvent:
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError as exc:
            raise ValueError("incident event payload is corrupt") from exc
        return IncidentEvent(
            incident_id=str(row["incident_id"]),
            sequence=int(row["sequence"]),
            event_id=str(row["event_id"]),
            previous_hash=str(row["previous_hash"]),
            event_hash=str(row["event_hash"]),
            state=str(row["state"]),
            severity=str(row["severity"]),
            event_type=str(row["event_type"]),
            actor=str(row["actor"]),
            evidence_hash=str(row["evidence_hash"]),
            payload=payload,
            occurred_at=str(row["occurred_at"]),
        )

    def runtime_authorization(
        self,
        session_id: str,
        *,
        at: datetime | str | None = None,
        require_fresh_preflight: bool = True,
    ) -> dict[str, Any]:
        current = self._now(at)
        with closing(self._connect()) as connection:
            session = self._runtime_session_conn(connection, session_id)
            if session is None:
                return {"allowed": False, "reasons": ["runtime-session-missing"], "session": None}
            reasons: list[str] = []
            if session.mode not in LIVE_MODES:
                reasons.append("runtime-session-not-live")
            if session.lifecycle != "RUNNING":
                reasons.append(f"runtime-session-{session.lifecycle.lower()}")
            latest = self._latest_manifest_conn(connection, session.deployment_id)
            if latest is None or latest.manifest_hash != session.deployment_manifest_hash:
                reasons.append("deployment-manifest-superseded")
            if require_fresh_preflight:
                validity = self._preflight_validity_conn(
                    connection,
                    session.preflight_snapshot_id,
                    current,
                )
                reasons.extend(validity["reasons"])
            critical_count = self._blocking_incident_count_conn(connection, session)
            if critical_count:
                reasons.append(f"critical-incidents:{critical_count}")
            reasons = list(dict.fromkeys(reasons))
            return {
                "allowed": not reasons,
                "reasons": reasons,
                "session": session.to_dict(),
                "checkedAt": current,
            }

    @staticmethod
    def _blocking_incident_count_conn(
        connection: sqlite3.Connection,
        session: RuntimeSession,
    ) -> int:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM live_incidents i
            JOIN live_incident_events e
              ON e.incident_id = i.incident_id
             AND e.sequence = (
                SELECT MAX(last.sequence) FROM live_incident_events last
                WHERE last.incident_id = i.incident_id
             )
            WHERE e.state <> 'RESOLVED'
              AND e.severity = 'CRITICAL'
              AND (
                    i.scope_type = 'GLOBAL'
                 OR i.session_id = ?
                 OR i.deployment_id = ?
                 OR (i.scope_type = 'SESSION' AND i.scope_id = ?)
                 OR (i.scope_type = 'DEPLOYMENT' AND i.scope_id = ?)
                 OR (i.scope_type = 'ROUTE' AND i.scope_id = (
                       SELECT json_extract(payload_json, '$.brokerRoute')
                       FROM live_deployment_manifests
                       WHERE manifest_hash = ?
                    ))
              )
            """,
            (
                session.session_id,
                session.deployment_id,
                session.session_id,
                session.deployment_id,
                session.deployment_manifest_hash,
            ),
        ).fetchone()
        return int(row["count"] if row is not None else 0)

    def latest_preflight_for_deployment(
        self,
        deployment_id: str,
    ) -> PreflightSnapshot | None:
        """Return the newest immutable preflight for one deployment."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM live_preflight_snapshots "
                "WHERE deployment_id = ? ORDER BY issued_at DESC LIMIT 1",
                (str(deployment_id),),
            ).fetchone()
        if row is None:
            return None
        try:
            return PreflightSnapshot.from_dict(json.loads(str(row["payload_json"])))
        except json.JSONDecodeError as exc:
            raise ValueError("preflight snapshot JSON is corrupt") from exc

    def latest_runtime_session(
        self,
        deployment_id: str = "",
    ) -> RuntimeSession | None:
        """Return the newest runtime session, optionally scoped to deployment."""

        with closing(self._connect()) as connection:
            if deployment_id:
                row = connection.execute(
                    "SELECT session_id FROM live_runtime_sessions "
                    "WHERE deployment_id = ? ORDER BY created_at DESC LIMIT 1",
                    (str(deployment_id),),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT session_id FROM live_runtime_sessions "
                    "ORDER BY created_at DESC LIMIT 1"
                ).fetchone()
            if row is None:
                return None
            return self._runtime_session_conn(connection, str(row["session_id"]))

    def list_incidents(
        self,
        *,
        limit: int = 100,
        active_only: bool = False,
    ) -> list[Incident]:
        """Project current incident state without mutating append-only history."""

        with closing(self._connect()) as connection:
            where = "WHERE e.state <> 'RESOLVED'" if active_only else ""
            rows = connection.execute(
                f"""
                SELECT i.incident_id
                FROM live_incidents i
                JOIN live_incident_events e
                  ON e.incident_id = i.incident_id
                 AND e.sequence = (
                    SELECT MAX(last.sequence) FROM live_incident_events last
                    WHERE last.incident_id = i.incident_id
                 )
                {where}
                ORDER BY i.opened_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            result = [
                self._incident_conn(connection, str(row["incident_id"]))
                for row in rows
            ]
        return [incident for incident in result if incident is not None]

    def workspace_snapshot(self, deployment_id: str = "") -> dict[str, Any]:
        """Read-only projection consumed by the desktop operations console."""

        manifest = self.get_deployment_manifest(deployment_id) if deployment_id else None
        preflight = (
            self.latest_preflight_for_deployment(deployment_id)
            if deployment_id
            else None
        )
        session = self.latest_runtime_session(deployment_id)
        validity = (
            self.preflight_validity(preflight.snapshot_id)
            if preflight is not None
            else {"valid": False, "reasons": ["preflight-snapshot-missing"], "snapshot": None}
        )
        return {
            "schemaVersion": "live-operational-governance-workspace-v1",
            "deploymentId": str(deployment_id),
            "manifest": manifest.to_dict() if manifest is not None else None,
            "latestPreflight": preflight.to_dict() if preflight is not None else None,
            "preflightValidity": validity,
            "activeSession": session.to_dict() if session is not None else None,
            "incidents": [item.to_dict() for item in self.list_incidents(limit=100)],
            "integrity": self.verify_integrity(),
        }

    def verify_integrity(self) -> dict[str, Any]:
        issues: list[str] = []
        counts = {
            "deploymentManifests": 0,
            "preflightSnapshots": 0,
            "runtimeSessions": 0,
            "runtimeEvents": 0,
            "incidents": 0,
            "incidentEvents": 0,
        }
        with closing(self._connect()) as connection:
            manifest_rows = connection.execute(
                "SELECT deployment_id, revision, manifest_hash, previous_manifest_hash, "
                "payload_json FROM live_deployment_manifests "
                "ORDER BY deployment_id, revision"
            ).fetchall()
            previous_by_deployment: dict[str, str] = {}
            revision_by_deployment: dict[str, int] = {}
            for row in manifest_rows:
                counts["deploymentManifests"] += 1
                deployment_id = str(row["deployment_id"])
                try:
                    manifest = DeploymentManifest.from_dict(json.loads(str(row["payload_json"])))
                except (ValueError, json.JSONDecodeError) as exc:
                    issues.append(f"deployment:{deployment_id}:{row['revision']}:{exc}")
                    continue
                expected_revision = revision_by_deployment.get(deployment_id, 0) + 1
                expected_previous = previous_by_deployment.get(deployment_id, "")
                if manifest.revision != expected_revision:
                    issues.append(f"deployment:{deployment_id}:revision-gap")
                if manifest.previous_manifest_hash != expected_previous:
                    issues.append(f"deployment:{deployment_id}:previous-hash-mismatch")
                if manifest.manifest_hash != str(row["manifest_hash"]):
                    issues.append(f"deployment:{deployment_id}:row-hash-mismatch")
                revision_by_deployment[deployment_id] = manifest.revision
                previous_by_deployment[deployment_id] = manifest.manifest_hash

            for row in connection.execute(
                "SELECT snapshot_id, snapshot_hash, payload_json FROM live_preflight_snapshots"
            ).fetchall():
                counts["preflightSnapshots"] += 1
                try:
                    snapshot = PreflightSnapshot.from_dict(json.loads(str(row["payload_json"])))
                    if snapshot.snapshot_hash != str(row["snapshot_hash"]):
                        issues.append(f"preflight:{row['snapshot_id']}:row-hash-mismatch")
                except (ValueError, json.JSONDecodeError) as exc:
                    issues.append(f"preflight:{row['snapshot_id']}:{exc}")

            counts["runtimeSessions"] = int(
                connection.execute("SELECT COUNT(*) FROM live_runtime_sessions").fetchone()[0]
            )
            runtime_rows = connection.execute(
                "SELECT * FROM live_runtime_events ORDER BY session_id, sequence"
            ).fetchall()
            counts["runtimeEvents"] = len(runtime_rows)
            self._verify_event_rows(
                runtime_rows,
                entity_key="session_id",
                schema_version=RUNTIME_EVENT_SCHEMA_VERSION,
                state_key="lifecycle",
                issues=issues,
                label="runtime",
            )

            counts["incidents"] = int(
                connection.execute("SELECT COUNT(*) FROM live_incidents").fetchone()[0]
            )
            incident_rows = connection.execute(
                "SELECT * FROM live_incident_events ORDER BY incident_id, sequence"
            ).fetchall()
            counts["incidentEvents"] = len(incident_rows)
            self._verify_event_rows(
                incident_rows,
                entity_key="incident_id",
                schema_version=INCIDENT_EVENT_SCHEMA_VERSION,
                state_key="state",
                issues=issues,
                label="incident",
            )
        return {"ok": not issues, "issues": issues, "counts": counts}

    @staticmethod
    def _verify_event_rows(
        rows: Iterable[sqlite3.Row],
        *,
        entity_key: str,
        schema_version: str,
        state_key: str,
        issues: list[str],
        label: str,
    ) -> None:
        previous_by_entity: dict[str, str] = {}
        sequence_by_entity: dict[str, int] = {}
        for row in rows:
            entity_id = str(row[entity_key])
            sequence = int(row["sequence"])
            expected_sequence = sequence_by_entity.get(entity_id, 0) + 1
            expected_previous = previous_by_entity.get(entity_id, "")
            try:
                payload = json.loads(str(row["payload_json"] or "{}"))
            except json.JSONDecodeError:
                issues.append(f"{label}:{entity_id}:{sequence}:payload-json")
                continue
            base = {
                "schemaVersion": schema_version,
                "sessionId" if entity_key == "session_id" else "incidentId": entity_id,
                "sequence": sequence,
                "eventId": str(row["event_id"]),
                "previousHash": str(row["previous_hash"]),
                state_key if entity_key == "session_id" else "state": str(row[state_key]),
                "eventType": str(row["event_type"]),
                "actor": str(row["actor"]),
                "payload": payload,
                "occurredAt": str(row["occurred_at"]),
            }
            if entity_key == "incident_id":
                base["severity"] = str(row["severity"])
                base["evidenceHash"] = str(row["evidence_hash"])
            if sequence != expected_sequence:
                issues.append(f"{label}:{entity_id}:{sequence}:sequence-gap")
            if str(row["previous_hash"]) != expected_previous:
                issues.append(f"{label}:{entity_id}:{sequence}:previous-hash-mismatch")
            if stable_sha256(base) != str(row["event_hash"]):
                issues.append(f"{label}:{entity_id}:{sequence}:event-hash-mismatch")
            sequence_by_entity[entity_id] = sequence
            previous_by_entity[entity_id] = str(row["event_hash"])


__all__ = [
    "DEFAULT_PREFLIGHT_TTL_SECONDS",
    "MAX_PREFLIGHT_TTL_SECONDS",
    "DeploymentManifest",
    "Incident",
    "IncidentEvent",
    "OperationalGovernanceStore",
    "PreflightSnapshot",
    "RuntimeEvent",
    "RuntimeSession",
    "stable_sha256",
]
