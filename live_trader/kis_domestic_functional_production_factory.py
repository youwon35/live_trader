from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from .kis_domestic_functional_contract import PDNO, ROUTE
from .kis_domestic_functional_key_registry import (
    GRAPH_BINDING_SCHEMA,
    KEY_PURPOSES,
    VerifyOnlyKeyRegistry,
)
from .kis_domestic_functional_owner import (
    OWNER_RECORD_SCHEMA,
    SCHEMA_FINGERPRINT as _OWNER_SCHEMA_FINGERPRINT,
    SCHEMA_VERSION as _OWNER_SCHEMA_VERSION,
    _SCHEMA_SQL as _OWNER_SCHEMA_SQL,
)


KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_FACTORY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_FACTORY_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_FACTORY_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_FACTORY_RELEASE_AVAILABLE = False

FACTORY_SCHEMA = "kis-domestic-functional-production-factory/v1"
VERIFIER_SCHEMA = "kis-domestic-functional-registry-derived-verifier/v1"
OWNER_READER_SCHEMA = "kis-domestic-functional-registry-owner-reader/v1"
STATUS_SCHEMA = "kis-domestic-functional-production-factory-status/v1"

_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_FACTORY_TOKEN = object()
_MAX_CLOCK_DIVERGENCE_SECONDS = 5.0
_MAX_OWNER_HEARTBEAT_AGE_SECONDS = 30.0
_COMPONENT_FILES = {
    "productionFactory": "kis_domestic_functional_production_factory.py",
    "keyRegistry": "kis_domestic_functional_key_registry.py",
    "owner": "kis_domestic_functional_owner.py",
    "graph": "kis_domestic_functional_graph.py",
    "lane": "kis_domestic_functional_lane.py",
    "manager": "kis_domestic_functional_manager.py",
    "state": "kis_domestic_functional_state.py",
    "transport": "kis_domestic_functional_transport.py",
    "productionTransport": "kis_domestic_functional_production_transport.py",
}
_OWNER_COLUMNS = (
    "route", "pdno", "epoch", "state", "owner_id_hash",
    "process_identity_hash", "lease_scope_hash", "lease_factory_hash",
    "acquired_at", "acquired_monotonic_ns", "heartbeat_at",
    "heartbeat_monotonic_ns", "heartbeat_count", "hazardous_authority_open",
    "owned_exposure_present", "orphan_count", "timed_out_call_count",
    "detached_call_count", "hazard_union_hash", "route_observation_id",
    "route_fence_revision", "route_fence_hash", "hazard_observed_at",
    "hazard_observed_monotonic_ns", "session_id", "authority_expires_at",
    "shared_route_fence_wired", "hazard_reader_registry_hash", "reason",
    "revision", "record_json", "record_hash", "signature",
    "authority_key_id_hash",
)
_TRANSITION_COLUMNS = (
    "route", "epoch", "revision", "phase", "occurred_at",
    "occurred_monotonic_ns", "previous_hash", "record_json", "record_hash",
    "signature", "authority_key_id_hash",
)
_OWNER_BODY_KEYS = {
    "schemaVersion", "route", "pdno", "epoch", "state", "ownerIdHash",
    "processIdentityHash", "leaseScopeHash", "leaseFactoryHash", "acquiredAt",
    "acquiredMonotonicNs", "heartbeatAt", "heartbeatMonotonicNs",
    "heartbeatCount", "hazardousAuthorityOpen", "ownedExposurePresent",
    "orphanCount", "timedOutCallCount", "detachedCallCount",
    "hazardUnionHash", "hazardComponents", "routeObservationId",
    "routeFenceRevision", "routeFenceHash", "hazardObservedAt",
    "hazardObservedMonotonicNs", "sessionId", "authorityExpiresAt",
    "sharedRouteFenceWired", "hazardReaderRegistryHash", "reason", "revision",
    "authorityKeyIdHash",
}
_OWNER_COMPONENTS = {
    "lane", "source", "rolling", "heartbeat", "mutation", "capability",
    "quote", "graph", "truth",
}
_OWNER_TABLES = (
    "kis_functional_owner_meta",
    "kis_functional_route_owner",
    "kis_functional_owner_transition",
)
_OWNER_BOOL_FIELDS = (
    "hazardousAuthorityOpen",
    "ownedExposurePresent",
    "sharedRouteFenceWired",
)
_OWNER_INT_FIELDS = (
    "epoch",
    "acquiredMonotonicNs",
    "heartbeatMonotonicNs",
    "heartbeatCount",
    "orphanCount",
    "timedOutCallCount",
    "detachedCallCount",
    "routeFenceRevision",
    "hazardObservedMonotonicNs",
    "revision",
)
_OWNER_SHA_FIELDS = (
    "ownerIdHash",
    "processIdentityHash",
    "leaseScopeHash",
    "leaseFactoryHash",
    "hazardUnionHash",
    "routeObservationId",
    "routeFenceHash",
    "hazardReaderRegistryHash",
    "authorityKeyIdHash",
)
_OWNER_TIME_FIELDS = (
    "acquiredAt",
    "heartbeatAt",
    "hazardObservedAt",
    "authorityExpiresAt",
)


class KisDomesticFunctionalProductionFactoryBlocked(RuntimeError):
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
        raise KisDomesticFunctionalProductionFactoryBlocked(
            "production-factory-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalProductionFactoryBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalProductionFactoryBlocked(f"{label}-invalid")
    return value


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalProductionFactoryBlocked(f"{label}-invalid")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalProductionFactoryBlocked(f"{label}-invalid") from exc
    if result.tzinfo is None or not math.isfinite(result.timestamp()):
        raise KisDomesticFunctionalProductionFactoryBlocked(f"{label}-invalid")
    result = result.astimezone(timezone.utc)
    if result.isoformat().replace("+00:00", "Z") != value:
        raise KisDomesticFunctionalProductionFactoryBlocked(
            f"{label}-not-canonical-utc"
        )
    return result


def _code_hash(value: Any, label: str) -> str:
    target = getattr(value, "__func__", value)
    code = getattr(target, "__code__", None)
    if code is None:
        raise KisDomesticFunctionalProductionFactoryBlocked(
            f"{label}-code-identity-unavailable"
        )
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-factory-code/v1",
            "module": getattr(target, "__module__", ""),
            "qualname": getattr(target, "__qualname__", ""),
            "bytecodeHash": hashlib.sha256(code.co_code).hexdigest(),
            "constantsHash": hashlib.sha256(repr(code.co_consts).encode()).hexdigest(),
            "namesHash": hashlib.sha256(repr(code.co_names).encode()).hexdigest(),
        }
    )


def _stable_file_read(path: Path, label: str) -> tuple[bytes, str]:
    try:
        before = path.read_bytes()
        after = path.read_bytes()
    except OSError as exc:
        raise KisDomesticFunctionalProductionFactoryBlocked(
            f"{label}-file-unreadable:{type(exc).__name__}"
        ) from None
    if before != after:
        raise KisDomesticFunctionalProductionFactoryBlocked(
            f"{label}-file-changed-during-read"
        )
    return before, hashlib.sha256(before).hexdigest()


def _stable_file_hash(path: Path, label: str) -> str:
    return _stable_file_read(path, label)[1]


class _FilePinGuard:
    __slots__ = ("_pins", "_lock", "binding_hash")

    def __init__(
        self,
        *,
        token: object,
        pins: Mapping[str, tuple[Path, str]],
    ) -> None:
        if token is not _FACTORY_TOKEN or not isinstance(pins, Mapping):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-file-guard-construction-forbidden"
            )
        normalized: dict[str, tuple[Path, str]] = {}
        for name, item in pins.items():
            if (
                type(name) is not str
                or not _ID.fullmatch(name)
                or not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], Path)
            ):
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-file-guard-pin-invalid"
                )
            normalized[name] = (
                item[0].resolve(),
                _sha(item[1], f"production-factory-file-guard:{name}"),
            )
        if not normalized:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-file-guard-empty"
            )
        self._pins = normalized
        self._lock = threading.RLock()
        self.binding_hash = _hash(
            {
                name: {"pathName": path.name, "fileHash": expected}
                for name, (path, expected) in sorted(normalized.items())
            }
        )

    def verify(self) -> str:
        with self._lock:
            observed = {
                name: _stable_file_hash(path, f"production-factory-guard:{name}")
                for name, (path, _) in sorted(self._pins.items())
            }
            expected = {
                name: digest for name, (_, digest) in sorted(self._pins.items())
            }
            if observed != expected:
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-file-guard-drift"
                )
            return self.binding_hash


def _system_clock_pair() -> tuple[datetime, int]:
    wall = datetime.now(timezone.utc)
    monotonic_ns = time.monotonic_ns()
    if type(monotonic_ns) is not int or monotonic_ns < 0:
        raise KisDomesticFunctionalProductionFactoryBlocked(
            "production-factory-system-monotonic-invalid"
        )
    return wall, monotonic_ns


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).lower()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _owner_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    placeholders = ",".join("?" for _ in _OWNER_TABLES)
    objects = [
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            _normalize_sql(row[3]),
        )
        for row in conn.execute(
            "SELECT name,type,tbl_name,sql FROM sqlite_master "
            f"WHERE tbl_name IN ({placeholders}) OR name IN ({placeholders}) "
            "ORDER BY type,name",
            _OWNER_TABLES + _OWNER_TABLES,
        ).fetchall()
    ]
    columns = {
        table: [tuple(row) for row in conn.execute(
            f"PRAGMA table_xinfo({_quote_identifier(table)})"
        ).fetchall()]
        for table in _OWNER_TABLES
    }
    index_lists = {
        table: [tuple(row) for row in conn.execute(
            f"PRAGMA index_list({_quote_identifier(table)})"
        ).fetchall()]
        for table in _OWNER_TABLES
    }
    index_names = sorted(
        {
            str(row[1])
            for rows in index_lists.values()
            for row in rows
        }
    )
    index_details = {
        name: [tuple(row) for row in conn.execute(
            f"PRAGMA index_xinfo({_quote_identifier(name)})"
        ).fetchall()]
        for name in index_names
    }
    foreign_keys = {
        table: [tuple(row) for row in conn.execute(
            f"PRAGMA foreign_key_list({_quote_identifier(table)})"
        ).fetchall()]
        for table in _OWNER_TABLES
    }
    return {
        "objects": objects,
        "columns": columns,
        "indexLists": index_lists,
        "indexDetails": index_details,
        "foreignKeys": foreign_keys,
    }


def _expected_owner_schema() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        for statement in _OWNER_SCHEMA_SQL:
            conn.execute(statement)
        return _owner_schema_snapshot(conn)
    finally:
        conn.close()


_EXPECTED_OWNER_SCHEMA = _expected_owner_schema()


def _strict_owner_body(body: Mapping[str, Any]) -> bool:
    if (
        set(body) != _OWNER_BODY_KEYS
        or body.get("schemaVersion") != OWNER_RECORD_SCHEMA
        or body.get("route") != ROUTE
        or body.get("pdno") != PDNO
        or body.get("state") not in {
            "ACTIVE", "RECONCILIATION_REQUIRED", "RELEASED"
        }
        or type(body.get("sessionId")) is not str
        or not _ID.fullmatch(body["sessionId"])
        or type(body.get("reason")) is not str
        or not body["reason"]
        or any(type(body.get(field)) is not bool for field in _OWNER_BOOL_FIELDS)
        or any(
            type(body.get(field)) is not int or body[field] < 0
            for field in _OWNER_INT_FIELDS
        )
        or body["epoch"] < 1
        or body["heartbeatCount"] < 1
        or body["routeFenceRevision"] < 1
        or body["revision"] < 1
        or any(
            type(body.get(field)) is not str or not _SHA.fullmatch(body[field])
            for field in _OWNER_SHA_FIELDS
        )
        or not isinstance(body.get("hazardComponents"), Mapping)
        or set(body["hazardComponents"]) != _OWNER_COMPONENTS
        or any(
            not isinstance(item, Mapping)
            for item in body["hazardComponents"].values()
        )
    ):
        return False
    try:
        for field in _OWNER_TIME_FIELDS:
            parsed = _time(body[field], f"factory-owner-{field}")
            if body[field] != parsed.isoformat().replace("+00:00", "Z"):
                return False
        acquired = _time(body["acquiredAt"], "factory-owner-acquired")
        heartbeat = _time(body["heartbeatAt"], "factory-owner-heartbeat")
        hazard_at = _time(body["hazardObservedAt"], "factory-owner-hazard")
        expires = _time(body["authorityExpiresAt"], "factory-owner-expires")
    except KisDomesticFunctionalProductionFactoryBlocked:
        return False
    return bool(
        acquired <= hazard_at <= heartbeat < expires
        and body["acquiredMonotonicNs"]
        <= body["hazardObservedMonotonicNs"]
        <= body["heartbeatMonotonicNs"]
    )


@dataclass(frozen=True, slots=True)
class ProductionFactoryPins:
    registry_id: str
    registry_epoch: int
    registry_manifest_hash: str
    registry_manifest_file_hash: str
    registry_accepted_head_hash: str
    registry_acceptance_revision: int
    registry_factory_binding_hash: str
    registry_graph_binding_hash: str
    root_key_id_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    code_manifest_hash: str
    owner_epoch: int
    owner_authority_key_id_hash: str
    production_factory_file_hash: str
    key_registry_file_hash: str
    owner_file_hash: str
    graph_file_hash: str
    lane_file_hash: str
    manager_file_hash: str
    state_file_hash: str
    transport_file_hash: str
    production_transport_file_hash: str
    clock_generation: str

    def canonical_body(self) -> dict[str, Any]:
        if type(self.registry_epoch) is not int or self.registry_epoch < 1:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-epoch-invalid"
            )
        if type(self.registry_acceptance_revision) is not int or self.registry_acceptance_revision < 1:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-revision-invalid"
            )
        if type(self.owner_epoch) is not int or self.owner_epoch < 1:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-epoch-invalid"
            )
        body = {
            "schemaVersion": FACTORY_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "registryId": _identifier(self.registry_id, "factory-registry-id"),
            "registryEpoch": self.registry_epoch,
            "registryManifestHash": _sha(self.registry_manifest_hash, "factory-manifest-hash"),
            "registryManifestFileHash": _sha(self.registry_manifest_file_hash, "factory-manifest-file-hash"),
            "registryAcceptedHeadHash": _sha(self.registry_accepted_head_hash, "factory-accepted-head"),
            "registryAcceptanceRevision": self.registry_acceptance_revision,
            "registryFactoryBindingHash": _sha(
                self.registry_factory_binding_hash,
                "factory-registry-factory-binding",
            ),
            "registryGraphBindingHash": _sha(
                self.registry_graph_binding_hash,
                "factory-registry-graph-binding",
            ),
            "rootKeyIdHash": _sha(self.root_key_id_hash, "factory-root-key-id"),
            "accountFingerprint": _sha(self.account_fingerprint, "factory-account"),
            "credentialConfigurationHash": _sha(self.credential_configuration_hash, "factory-credential"),
            "codeManifestHash": _sha(self.code_manifest_hash, "factory-code-manifest"),
            "ownerEpoch": self.owner_epoch,
            "ownerAuthorityKeyIdHash": _sha(self.owner_authority_key_id_hash, "factory-owner-key-id"),
            "componentFileHashes": {
                "productionFactory": _sha(
                    self.production_factory_file_hash,
                    "factory-production-factory-file",
                ),
                "keyRegistry": _sha(
                    self.key_registry_file_hash, "factory-key-registry-file"
                ),
                "owner": _sha(self.owner_file_hash, "factory-owner-file"),
                "graph": _sha(self.graph_file_hash, "factory-graph-file"),
                "lane": _sha(self.lane_file_hash, "factory-lane-file"),
                "manager": _sha(self.manager_file_hash, "factory-manager-file"),
                "state": _sha(self.state_file_hash, "factory-state-file"),
                "transport": _sha(self.transport_file_hash, "factory-transport-file"),
                "productionTransport": _sha(self.production_transport_file_hash, "factory-production-transport-file"),
            },
            "clockGeneration": _identifier(self.clock_generation, "factory-clock-generation"),
            "productionAvailable": False,
        }
        return body


class _TrustedClockLineage:
    __slots__ = (
        "generation", "_previous_wall", "_previous_monotonic", "_lock"
    )

    def __init__(self, generation: str) -> None:
        self.generation = _identifier(generation, "factory-clock-generation")
        self._previous_wall: datetime | None = None
        self._previous_monotonic: int | None = None
        self._lock = threading.RLock()

    def sample(self) -> tuple[datetime, int, str]:
        with self._lock:
            wall, monotonic_ns = _system_clock_pair()
            if self._previous_wall is not None:
                if (
                    wall < self._previous_wall
                    or monotonic_ns < int(self._previous_monotonic)
                ):
                    raise KisDomesticFunctionalProductionFactoryBlocked(
                        "production-factory-clock-rollback"
                    )
                wall_delta = (wall - self._previous_wall).total_seconds()
                mono_delta = (
                    monotonic_ns - int(self._previous_monotonic)
                ) / 1_000_000_000
                if abs(wall_delta - mono_delta) > _MAX_CLOCK_DIVERGENCE_SECONDS:
                    raise KisDomesticFunctionalProductionFactoryBlocked(
                        "production-factory-clock-lineage-diverged"
                    )
            self._previous_wall = wall
            self._previous_monotonic = monotonic_ns
            body = {
                "schemaVersion": "kis-domestic-functional-system-clock-lineage/v1",
                "generation": self.generation,
                "wallAt": wall.isoformat().replace("+00:00", "Z"),
                "monotonicNs": monotonic_ns,
                "wallClockImplementation": "datetime.now(timezone.utc)",
                "monotonicImplementation": "time.monotonic_ns",
                "productionAvailable": False,
            }
            return wall, monotonic_ns, _hash(body)


class RegistryDerivedVerifier:
    __slots__ = (
        "_registry", "purpose", "_binding", "_factory_binding_hash", "_clock",
        "_file_guard",
    )

    def __init__(
        self,
        *,
        token: object,
        registry: VerifyOnlyKeyRegistry,
        purpose: str,
        binding: Mapping[str, Any],
        factory_binding_hash: str,
        clock: _TrustedClockLineage,
        file_guard: _FilePinGuard,
    ) -> None:
        if (
            token is not _FACTORY_TOKEN
            or type(registry) is not VerifyOnlyKeyRegistry
            or type(clock) is not _TrustedClockLineage
            or type(file_guard) is not _FilePinGuard
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "registry-derived-verifier-construction-forbidden"
            )
        self._registry = registry
        self.purpose = purpose
        self._binding = dict(binding)
        self._factory_binding_hash = factory_binding_hash
        self._clock = clock
        self._file_guard = file_guard

    def binding_status(self) -> dict[str, Any]:
        return dict(self._binding)

    def verify(
        self,
        *,
        domain: str,
        body: Mapping[str, Any],
        signature: str,
        key_id_hash: str,
        observed_at: datetime | None = None,
    ) -> bool:
        if not isinstance(body, Mapping) or type(domain) is not str or not domain:
            return False
        try:
            if self._file_guard.verify() != self._binding["fileGuardBindingHash"]:
                return False
            trusted_now, _, _ = self._clock.sample()
            status = self._registry.status()
            exact = self._binding
            manifest = getattr(self._registry, "manifest", None)
            if not isinstance(manifest, Mapping):
                return False
            manifest_not_before = _time(
                manifest.get("notBefore"), "factory-verifier-manifest-not-before"
            )
            manifest_not_after = _time(
                manifest.get("notAfter"), "factory-verifier-manifest-not-after"
            )
            if (
                _hash(manifest) != exact["registryManifestHash"]
                or not manifest_not_before <= trusted_now < manifest_not_after
            ):
                return False
            when = trusted_now
            if observed_at is not None:
                if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
                    return False
                when = observed_at.astimezone(timezone.utc)
                if when > trusted_now + timedelta(seconds=1):
                    return False
            if (
                status.get("registryEpoch") != exact["registryEpoch"]
                or status.get("acceptedManifestHeadHash") != exact["registryAcceptedHeadHash"]
                or status.get("manifestHash") != exact["registryManifestHash"]
                or status.get("accountFingerprint") != exact["accountFingerprint"]
                or status.get("credentialConfigurationHash") != exact["credentialConfigurationHash"]
                or status.get("codeManifestHash") != exact["codeManifestHash"]
                or status.get("productionFactoryBindingHash")
                != exact["registryFactoryBindingHash"]
                or status.get("graphRegistryBindingHash")
                != exact["registryGraphBindingHash"]
            ):
                return False
            return self._registry.verify(
                purpose=self.purpose,
                domain=domain,
                body=dict(body),
                signature=signature,
                key_id_hash=key_id_hash,
                observed_at=when,
            )
        except BaseException:
            return False

    def __call__(
        self,
        component: str,
        domain: str,
        body: Mapping[str, Any],
        signature: str,
        key_id_hash: str,
    ) -> bool:
        if type(component) is not str or not component:
            return False
        return self.verify(
            domain=domain,
            body=body,
            signature=signature,
            key_id_hash=key_id_hash,
        )


class RegistryDerivedStateManagerVerifier:
    """Verify-only adapter for the exact state/manager v2 evidence domains.

    The existing frozen registry has no dedicated manager-receipt purpose, so
    this seam deliberately remains non-production and uses the graph-record
    public key only as an offline integration proof.  Production readiness is
    false until a dedicated purpose is accepted in a new registry epoch.
    """

    __slots__ = ("_verifier", "_factory_binding_hash", "_file_guard")

    def __init__(
        self,
        *,
        token: object,
        verifier: RegistryDerivedVerifier,
        factory_binding_hash: str,
        file_guard: _FilePinGuard,
    ) -> None:
        if (
            token is not _FACTORY_TOKEN
            or type(verifier) is not RegistryDerivedVerifier
            or verifier.purpose != "GRAPH_RECORD_VERIFY"
            or type(file_guard) is not _FilePinGuard
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "state-manager-verifier-construction-forbidden"
            )
        self._verifier = verifier
        self._factory_binding_hash = _sha(
            factory_binding_hash, "state-manager-factory-binding"
        )
        self._file_guard = file_guard

    def _verify(self, candidate: Mapping[str, Any], *, domain: str) -> bool:
        try:
            if not isinstance(candidate, Mapping):
                return False
            value = dict(candidate)
            signature = value.pop("signature")
            key_id_hash = value.get("keyIdHash") or value.get(
                "managerKeyIdHash"
            )
            if (
                type(signature) is not str
                or type(key_id_hash) is not str
                or self._file_guard.verify()
                != self._verifier.binding_status()["fileGuardBindingHash"]
            ):
                return False
            observed_at = None
            observed = value.get("occurredAt") or value.get("reservedAt")
            if type(observed) is str:
                observed_at = _time(observed, "state-manager-observed-at")
            return self._verifier.verify(
                domain=domain,
                body=value,
                signature=signature,
                key_id_hash=key_id_hash,
                observed_at=observed_at,
            )
        except BaseException:
            return False

    def verify_binding(self, candidate: Mapping[str, Any]) -> bool:
        return self._verify(
            candidate, domain="KIS_STATE_MANAGER_BINDING"
        )

    def verify_receipt(self, candidate: Mapping[str, Any]) -> bool:
        domain = (
            "KIS_FUNCTIONAL_MANAGER_RECEIPT"
            if isinstance(candidate, Mapping) and "pdno" in candidate
            else "KIS_MANAGER_RECEIPT"
        )
        return self._verify(candidate, domain=domain)

    def status(self) -> dict[str, Any]:
        body = {
            "schemaVersion":
                "kis-domestic-functional-state-manager-verifier/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "factoryBindingHash": self._factory_binding_hash,
            "registryPurpose": "GRAPH_RECORD_VERIFY",
            "bindingDomain": "KIS_STATE_MANAGER_BINDING",
            "receiptDomains": [
                "KIS_FUNCTIONAL_MANAGER_RECEIPT",
                "KIS_MANAGER_RECEIPT",
            ],
            "verifyOnly": True,
            "dedicatedManagerKeyPurposeAvailable": False,
            "productionAvailable": False,
        }
        return {**body, "statusHash": _hash(body)}


class RegistryBoundStateManagerConstructors:
    __slots__ = (
        "state_constructor", "manager_constructor", "graph_port_constructor",
        "verifier", "manager_implementation_type", "manager_code_hash",
        "manager_protocol_hash", "factory_binding_hash", "_file_guard",
    )

    def __init__(
        self,
        *,
        token: object,
        verifier: RegistryDerivedStateManagerVerifier,
        factory_binding_hash: str,
        manager_code_hash: str,
        file_guard: _FilePinGuard,
    ) -> None:
        if (
            token is not _FACTORY_TOKEN
            or type(verifier) is not RegistryDerivedStateManagerVerifier
            or type(file_guard) is not _FilePinGuard
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "state-manager-constructors-construction-forbidden"
            )
        from .kis_domestic_functional_graph import (
            FrozenKisDomesticGraphLedgerPort,
            V2_FROZEN_PROTOCOL_HASHES,
        )
        from .kis_domestic_functional_manager import (
            DisabledKisDomesticFunctionalManager,
        )
        from .kis_domestic_functional_state import (
            DurableKisDomesticFunctionalState,
        )

        self.state_constructor = DurableKisDomesticFunctionalState
        self.manager_constructor = DisabledKisDomesticFunctionalManager
        self.graph_port_constructor = FrozenKisDomesticGraphLedgerPort
        self.verifier = verifier
        self.manager_implementation_type = (
            "live_trader.kis_domestic_functional_manager."
            "DisabledKisDomesticFunctionalManager"
        )
        self.manager_code_hash = _sha(
            manager_code_hash, "state-manager-manager-code-hash"
        )
        self.manager_protocol_hash = _sha(
            V2_FROZEN_PROTOCOL_HASHES["manager"],
            "state-manager-manager-protocol-hash",
        )
        self.factory_binding_hash = _sha(
            factory_binding_hash, "state-manager-factory-binding"
        )
        self._file_guard = file_guard

    def state_verifier_kwargs(
        self, *, manager_binding_reader: Any, manager_key_id_hash: str
    ) -> dict[str, Any]:
        if not callable(manager_binding_reader):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "state-manager-binding-reader-invalid"
            )
        return {
            "manager_receipt_verifier": self.verifier.verify_receipt,
            "manager_receipt_key_id_hash": _sha(
                manager_key_id_hash, "state-manager-key-id"
            ),
            "manager_binding_reader": manager_binding_reader,
            "manager_binding_verifier": self.verifier.verify_binding,
            "manager_implementation_type": self.manager_implementation_type,
            "manager_code_hash": self.manager_code_hash,
            "manager_protocol_hash": self.manager_protocol_hash,
        }

    def status(self) -> dict[str, Any]:
        self._file_guard.verify()
        body = {
            "schemaVersion":
                "kis-domestic-functional-state-manager-constructors/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "factoryBindingHash": self.factory_binding_hash,
            "stateImplementationType": (
                "live_trader.kis_domestic_functional_state."
                "DurableKisDomesticFunctionalState"
            ),
            "managerImplementationType": self.manager_implementation_type,
            "graphPortImplementationType": (
                "live_trader.kis_domestic_functional_graph."
                "FrozenKisDomesticGraphLedgerPort"
            ),
            "managerCodeHash": self.manager_code_hash,
            "managerProtocolHash": self.manager_protocol_hash,
            "stateReceiptV2IntegrationWired": True,
            "exactConstructorsPinned": True,
            "verifyOnly": True,
            "dedicatedManagerKeyPurposeAvailable": False,
            "sharedRouteFenceWired": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
        }
        return {**body, "statusHash": _hash(body)}


class RegistryDerivedOwnerEpochReader:
    __slots__ = (
        "_path", "_epoch", "_key_id_hash", "_verifier", "_clock",
        "_factory_binding_hash", "_lock", "_owner_identity",
        "_last_revision", "_last_head_hash", "_file_guard",
    )

    def __init__(
        self,
        *,
        token: object,
        path: Path,
        epoch: int,
        key_id_hash: str,
        verifier: RegistryDerivedVerifier,
        clock: _TrustedClockLineage,
        factory_binding_hash: str,
        file_guard: _FilePinGuard,
    ) -> None:
        if (
            token is not _FACTORY_TOKEN
            or type(verifier) is not RegistryDerivedVerifier
            or type(file_guard) is not _FilePinGuard
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "registry-owner-reader-construction-forbidden"
            )
        self._path = path
        self._epoch = epoch
        self._key_id_hash = key_id_hash
        self._verifier = verifier
        self._clock = clock
        self._factory_binding_hash = factory_binding_hash
        self._file_guard = file_guard
        self._lock = threading.RLock()
        self._owner_identity: tuple[Any, ...] | None = None
        self._last_revision = 0
        self._last_head_hash = "0" * 64

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
        return tuple(str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})"))

    def read(self) -> dict[str, Any]:
        with self._lock:
            return self._read_locked()

    def _read_locked(self) -> dict[str, Any]:
        self._file_guard.verify()
        wall, monotonic_ns, clock_hash = self._clock.sample()
        try:
            conn = sqlite3.connect(f"file:{self._path.as_posix()}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            if _owner_schema_snapshot(conn) != _EXPECTED_OWNER_SCHEMA:
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-owner-schema-dirty"
                )
            meta = conn.execute(
                "SELECT singleton,schema_version,schema_fingerprint "
                "FROM kis_functional_owner_meta"
            ).fetchall()
            if len(meta) != 1 or tuple(meta[0]) != (
                1,
                _OWNER_SCHEMA_VERSION,
                _OWNER_SCHEMA_FINGERPRINT,
            ):
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-owner-schema-meta-dirty"
                )
            row = conn.execute(
                "SELECT * FROM kis_functional_route_owner WHERE route=? AND epoch=?",
                (ROUTE, self._epoch),
            ).fetchone()
            transitions = conn.execute(
                "SELECT * FROM kis_functional_owner_transition "
                "WHERE route=? AND epoch=? ORDER BY revision",
                (ROUTE, self._epoch),
            ).fetchall()
            conn.commit()
        except sqlite3.Error as exc:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                f"production-factory-owner-read-failed:{type(exc).__name__}"
            ) from None
        finally:
            if "conn" in locals():
                conn.close()
        if row is None:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-epoch-missing"
            )
        try:
            body = json.loads(str(row["record_json"]))
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-json-invalid"
            ) from None
        projection = {
            "route": row["route"], "pdno": row["pdno"], "epoch": row["epoch"],
            "state": row["state"], "ownerIdHash": row["owner_id_hash"],
            "processIdentityHash": row["process_identity_hash"],
            "leaseScopeHash": row["lease_scope_hash"],
            "leaseFactoryHash": row["lease_factory_hash"],
            "acquiredAt": row["acquired_at"],
            "acquiredMonotonicNs": row["acquired_monotonic_ns"],
            "heartbeatAt": row["heartbeat_at"],
            "heartbeatMonotonicNs": row["heartbeat_monotonic_ns"],
            "heartbeatCount": row["heartbeat_count"],
            "hazardousAuthorityOpen": bool(row["hazardous_authority_open"]),
            "ownedExposurePresent": bool(row["owned_exposure_present"]),
            "orphanCount": row["orphan_count"],
            "timedOutCallCount": row["timed_out_call_count"],
            "detachedCallCount": row["detached_call_count"],
            "hazardUnionHash": row["hazard_union_hash"],
            "routeObservationId": row["route_observation_id"],
            "routeFenceRevision": row["route_fence_revision"],
            "routeFenceHash": row["route_fence_hash"],
            "hazardObservedAt": row["hazard_observed_at"],
            "hazardObservedMonotonicNs": row["hazard_observed_monotonic_ns"],
            "sessionId": row["session_id"],
            "authorityExpiresAt": row["authority_expires_at"],
            "sharedRouteFenceWired": bool(row["shared_route_fence_wired"]),
            "hazardReaderRegistryHash": row["hazard_reader_registry_hash"],
            "reason": row["reason"], "revision": row["revision"],
            "authorityKeyIdHash": row["authority_key_id_hash"],
        }
        if (
            not isinstance(body, Mapping)
            or not _strict_owner_body(body)
            or any(body.get(key) != value for key, value in projection.items())
            or body.get("authorityKeyIdHash") != self._key_id_hash
            or any(
                type(row[column]) is not int or row[column] not in (0, 1)
                for column in (
                    "hazardous_authority_open",
                    "owned_exposure_present",
                    "shared_route_fence_wired",
                )
            )
            or not hmac.compare_digest(str(row["record_hash"]), _hash(body))
            or not self._verifier.verify(
                domain="KIS_FUNCTIONAL_OWNER",
                body=body,
                signature=str(row["signature"]),
                key_id_hash=self._key_id_hash,
                observed_at=_time(body["heartbeatAt"], "factory-owner-heartbeat"),
            )
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-record-unverified"
            )
        previous = "0" * 64
        last_owner: Mapping[str, Any] | None = None
        previous_at: datetime | None = None
        previous_monotonic: int | None = None
        for revision, transition in enumerate(transitions, 1):
            try:
                item = json.loads(str(transition["record_json"]))
            except (TypeError, json.JSONDecodeError):
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-owner-transition-json-invalid"
                ) from None
            owner_item = (
                {key: item[key] for key in _OWNER_BODY_KEYS}
                if isinstance(item, Mapping) and _OWNER_BODY_KEYS <= set(item)
                else {}
            )
            if (
                not isinstance(item, Mapping)
                or set(item) != _OWNER_BODY_KEYS | {
                    "previousHash", "occurredAt", "occurredMonotonicNs"
                }
                or not _strict_owner_body(owner_item)
                or transition["revision"] != revision
                or item.get("revision") != revision
                or item.get("previousHash") != previous
                or transition["previous_hash"] != previous
                or transition["route"] != ROUTE
                or transition["epoch"] != self._epoch
                or transition["phase"] != item.get("state")
                or item.get("occurredAt") != transition["occurred_at"]
                or item.get("occurredMonotonicNs")
                != transition["occurred_monotonic_ns"]
                or item.get("occurredAt") != item.get("heartbeatAt")
                or item.get("occurredMonotonicNs")
                != item.get("heartbeatMonotonicNs")
                or transition["authority_key_id_hash"] != self._key_id_hash
                or not hmac.compare_digest(
                    str(transition["record_hash"]), _hash(item)
                )
                or not self._verifier.verify(
                    domain="KIS_FUNCTIONAL_OWNER_TRANSITION",
                    body=item,
                    signature=str(transition["signature"]),
                    key_id_hash=self._key_id_hash,
                    observed_at=_time(item["occurredAt"], "factory-owner-transition"),
                )
            ):
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-owner-transition-unverified"
                )
            occurred_at = _time(
                item["occurredAt"], "factory-owner-transition-occurred"
            )
            occurred_monotonic = item["occurredMonotonicNs"]
            if (
                previous_at is not None
                and (
                    occurred_at < previous_at
                    or occurred_monotonic < int(previous_monotonic)
                )
            ):
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-owner-transition-time-rollback"
                )
            previous_at = occurred_at
            previous_monotonic = occurred_monotonic
            previous = str(transition["record_hash"])
            last_owner = {
                key: value for key, value in item.items()
                if key not in {"previousHash", "occurredAt", "occurredMonotonicNs"}
            }
        heartbeat = _time(body["heartbeatAt"], "factory-owner-heartbeat")
        expires = _time(body["authorityExpiresAt"], "factory-owner-expires")
        heartbeat_monotonic = body["heartbeatMonotonicNs"]
        wall_age = (wall - heartbeat).total_seconds()
        mono_age = (monotonic_ns - heartbeat_monotonic) / 1_000_000_000
        if (
            len(transitions) != body["revision"]
            or last_owner != body
            or body["state"] != "ACTIVE"
            or body["sharedRouteFenceWired"] is not False
            or wall_age < 0
            or mono_age < 0
            or wall_age > _MAX_OWNER_HEARTBEAT_AGE_SECONDS
            or abs(wall_age - mono_age) > _MAX_CLOCK_DIVERGENCE_SECONDS
            or wall >= expires
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-epoch-not-fresh"
            )
        identity = (
            body["ownerIdHash"],
            body["processIdentityHash"],
            body["leaseScopeHash"],
            body["leaseFactoryHash"],
            body["acquiredAt"],
            body["acquiredMonotonicNs"],
            body["authorityKeyIdHash"],
        )
        if self._owner_identity is not None and identity != self._owner_identity:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-identity-changed"
            )
        if body["revision"] < self._last_revision:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-revision-regressed"
            )
        if self._last_revision and (
            len(transitions) < self._last_revision
            or str(transitions[self._last_revision - 1]["record_hash"])
            != self._last_head_hash
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-history-rewritten"
            )
        self._owner_identity = identity
        self._last_revision = body["revision"]
        self._last_head_hash = previous
        result = {
            "schemaVersion": OWNER_READER_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "ownerEpoch": body["epoch"],
            "ownerRevision": body["revision"],
            "ownerRecordHash": str(row["record_hash"]),
            "ownerTransitionHeadHash": previous,
            "ownerAuthorityKeyIdHash": self._key_id_hash,
            "sessionId": body["sessionId"],
            "authorityExpiresAt": body["authorityExpiresAt"],
            "hazardousAuthorityOpen": body["hazardousAuthorityOpen"],
            "ownedExposurePresent": body["ownedExposurePresent"],
            "sharedRouteFenceWired": body["sharedRouteFenceWired"],
            "trustedClockLineageHash": clock_hash,
            "factoryBindingHash": self._factory_binding_hash,
            "verifyOnly": True,
            "productionAvailable": False,
        }
        return {**result, "readerResultHash": _hash(result)}


class DisabledKisDomesticFunctionalProductionFactory:
    __slots__ = (
        "_registry", "_pins", "_pin_body", "_clock", "_verifiers",
        "_owner_reader", "_binding", "_binding_hash", "_file_hashes",
        "_file_guard", "_state_manager",
    )

    def __init__(
        self,
        *,
        registry: VerifyOnlyKeyRegistry,
        owner_database_path: str | Path,
        pins: ProductionFactoryPins,
    ) -> None:
        if type(registry) is not VerifyOnlyKeyRegistry:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-exact-registry-required"
            )
        if type(pins) is not ProductionFactoryPins:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-exact-pins-required"
            )
        pin_body = pins.canonical_body()
        clock = _TrustedClockLineage(pin_body["clockGeneration"])
        trusted_now, _, initial_clock_lineage_hash = clock.sample()
        status = registry.status()
        status_unsigned = dict(status)
        status_hash = status_unsigned.pop("statusHash", "")
        if not hmac.compare_digest(str(status_hash), _hash(status_unsigned)):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-status-hash-invalid"
            )
        exact_registry = {
            "registryId": pin_body["registryId"],
            "registryEpoch": pin_body["registryEpoch"],
            "manifestHash": pin_body["registryManifestHash"],
            "manifestFileHash": pin_body["registryManifestFileHash"],
            "acceptedManifestHeadHash": pin_body["registryAcceptedHeadHash"],
            "acceptanceRevision": pin_body["registryAcceptanceRevision"],
            "productionFactoryBindingHash": pin_body[
                "registryFactoryBindingHash"
            ],
            "graphRegistryBindingHash": pin_body["registryGraphBindingHash"],
            "rootKeyIdHash": pin_body["rootKeyIdHash"],
            "accountFingerprint": pin_body["accountFingerprint"],
            "credentialConfigurationHash": pin_body["credentialConfigurationHash"],
            "codeManifestHash": pin_body["codeManifestHash"],
            "verifyOnly": True,
            "privateKeyMaterialPresent": False,
            "signingSurfacePresent": False,
            "asymmetricRootVerified": True,
            "durableAcceptanceVerified": True,
            "trustedWallMonotonicLineageVerified": True,
            "productionFactoryPinsBound": True,
        }
        if any(
            type(status.get(key)) is not type(expected) or status.get(key) != expected
            for key, expected in exact_registry.items()
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-binding-mismatch"
            )
        registry_path = getattr(registry, "path", None)
        if not isinstance(registry_path, Path):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-manifest-path-unavailable"
            )
        registry_bytes, registry_file_hash = _stable_file_read(
            registry_path, "production-factory-registry-manifest"
        )
        if not hmac.compare_digest(
            registry_file_hash, pin_body["registryManifestFileHash"]
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-manifest-file-drift"
            )
        try:
            registry_document = json.loads(registry_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-manifest-document-invalid"
            ) from None
        manifest = getattr(registry, "manifest", None)
        if (
            not isinstance(registry_document, Mapping)
            or set(registry_document) != {
                "manifest", "manifestHash", "rootKeyIdHash", "rootSignature"
            }
            or not isinstance(manifest, Mapping)
            or registry_document.get("manifest") != manifest
            or registry_document.get("manifestHash")
            != pin_body["registryManifestHash"]
            or _hash(manifest) != pin_body["registryManifestHash"]
            or registry_document.get("rootKeyIdHash") != pin_body["rootKeyIdHash"]
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-manifest-memory-file-mismatch"
            )
        manifest_not_before = _time(
            manifest.get("notBefore"), "factory-manifest-not-before"
        )
        manifest_not_after = _time(
            manifest.get("notAfter"), "factory-manifest-not-after"
        )
        if not manifest_not_before <= trusted_now < manifest_not_after:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-manifest-system-time-invalid"
            )
        registry_factory_pin_body = getattr(registry, "_factory_pin_body", None)
        if (
            not isinstance(registry_factory_pin_body, Mapping)
            or _hash(registry_factory_pin_body)
            != pin_body["registryFactoryBindingHash"]
            or registry_factory_pin_body.get("graphFileHash")
            != pin_body["componentFileHashes"]["graph"]
            or registry_factory_pin_body.get("accountFingerprint")
            != pin_body["accountFingerprint"]
            or registry_factory_pin_body.get("credentialConfigurationHash")
            != pin_body["credentialConfigurationHash"]
            or registry_factory_pin_body.get("codeManifestHash")
            != pin_body["codeManifestHash"]
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-factory-pin-body-mismatch"
            )
        expected_registry_graph_binding = _hash(
            {
                "schemaVersion": GRAPH_BINDING_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "registryId": manifest["registryId"],
                "registryEpoch": manifest["registryEpoch"],
                "manifestHash": pin_body["registryManifestHash"],
                "rootKeyIdHash": pin_body["rootKeyIdHash"],
                "accountFingerprint": pin_body["accountFingerprint"],
                "credentialConfigurationHash": pin_body[
                    "credentialConfigurationHash"
                ],
                "codeManifestHash": pin_body["codeManifestHash"],
                "graphFileHash": registry_factory_pin_body["graphFileHash"],
                "graphProtocolHash": registry_factory_pin_body[
                    "graphProtocolHash"
                ],
                "graphSchemaFingerprint": registry_factory_pin_body[
                    "graphSchemaFingerprint"
                ],
            }
        )
        if not hmac.compare_digest(
            expected_registry_graph_binding,
            pin_body["registryGraphBindingHash"],
        ):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-graph-binding-mismatch"
            )
        base = Path(__file__).resolve().parent
        observed_files = {
            name: _stable_file_hash(base / filename, f"factory-{name}")
            for name, filename in _COMPONENT_FILES.items()
        }
        if observed_files != pin_body["componentFileHashes"]:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-component-file-pin-mismatch"
            )
        file_guard = _FilePinGuard(
            token=_FACTORY_TOKEN,
            pins={
                "registryManifest": (
                    registry_path.resolve(),
                    pin_body["registryManifestFileHash"],
                ),
                **{
                    name: (base / _COMPONENT_FILES[name], expected)
                    for name, expected in observed_files.items()
                },
            },
        )
        file_guard.verify()
        purpose_keys: dict[str, list[dict[str, Any]]] = {
            purpose: [] for purpose in KEY_PURPOSES
        }
        if not isinstance(manifest, Mapping) or not isinstance(manifest.get("keys"), list):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-registry-public-keys-unavailable"
            )
        for item in manifest["keys"]:
            if not isinstance(item, Mapping) or item.get("purpose") not in purpose_keys:
                raise KisDomesticFunctionalProductionFactoryBlocked(
                    "production-factory-public-key-config-invalid"
                )
            purpose_keys[item["purpose"]].append(
                {
                    "keyIdHash": _sha(item.get("keyIdHash"), "factory-public-key-id"),
                    "rotationEpoch": item.get("rotationEpoch"),
                    "notBefore": item.get("notBefore"),
                    "notAfter": item.get("notAfter"),
                    "publicKeyPemHash": hashlib.sha256(
                        str(item.get("publicKeyPem", "")).encode("utf-8")
                    ).hexdigest(),
                }
            )
        if any(not values for values in purpose_keys.values()):
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-public-key-purpose-incomplete"
            )
        public_key_sets = {
            purpose: sorted(values, key=lambda item: (item["rotationEpoch"], item["keyIdHash"]))
            for purpose, values in purpose_keys.items()
        }
        binding = {
            **pin_body,
            "componentFileSetHash": _hash(observed_files),
            "fileGuardBindingHash": file_guard.binding_hash,
            "registryStatusHash": status_hash,
            "publicKeyPurposeSetHash": _hash(public_key_sets),
            "verifierImplementationType": (
                "live_trader.kis_domestic_functional_production_factory."
                "RegistryDerivedVerifier"
            ),
            "verifierCodeHash": _code_hash(
                RegistryDerivedVerifier.verify, "factory-verifier"
            ),
            "ownerReaderImplementationType": (
                "live_trader.kis_domestic_functional_production_factory."
                "RegistryDerivedOwnerEpochReader"
            ),
            "ownerReaderCodeHash": _code_hash(
                RegistryDerivedOwnerEpochReader.read, "factory-owner-reader"
            ),
            "trustedClockImplementationHash": _code_hash(
                _system_clock_pair, "factory-system-clock"
            ),
            "initialTrustedClockLineageHash": initial_clock_lineage_hash,
            "verifyOnly": True,
            "privateKeyMaterialPresent": False,
            "signingSurfacePresent": False,
            "networkSurfacePresent": False,
        }
        binding_hash = _hash(binding)
        verifiers: dict[str, RegistryDerivedVerifier] = {}
        for purpose in KEY_PURPOSES:
            verifier_body = {
                "schemaVersion": VERIFIER_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "purpose": purpose,
                "registryId": pin_body["registryId"],
                "registryEpoch": pin_body["registryEpoch"],
                "registryManifestHash": pin_body["registryManifestHash"],
                "registryAcceptedHeadHash": pin_body["registryAcceptedHeadHash"],
                "registryFactoryBindingHash": pin_body[
                    "registryFactoryBindingHash"
                ],
                "registryGraphBindingHash": pin_body[
                    "registryGraphBindingHash"
                ],
                "accountFingerprint": pin_body["accountFingerprint"],
                "credentialConfigurationHash": pin_body["credentialConfigurationHash"],
                "codeManifestHash": pin_body["codeManifestHash"],
                "publicKeySetHash": _hash(public_key_sets[purpose]),
                "callbackCodeHash": binding["verifierCodeHash"],
                "closureConfigHash": _hash(
                    {
                        "purpose": purpose,
                        "publicKeys": public_key_sets[purpose],
                        "factoryBindingHash": binding_hash,
                    }
                ),
                "factoryBindingHash": binding_hash,
                "trustedClockGeneration": pin_body["clockGeneration"],
                "trustedClockImplementationHash": binding[
                    "trustedClockImplementationHash"
                ],
                "fileGuardBindingHash": file_guard.binding_hash,
                "verifyOnly": True,
                "productionAvailable": False,
            }
            verifier_status = {
                **verifier_body, "verifierBindingHash": _hash(verifier_body)
            }
            verifiers[purpose] = RegistryDerivedVerifier(
                token=_FACTORY_TOKEN,
                registry=registry,
                purpose=purpose,
                binding=verifier_status,
                factory_binding_hash=binding_hash,
                clock=clock,
                file_guard=file_guard,
            )
        owner_path = Path(owner_database_path).expanduser().resolve()
        if not owner_path.is_file():
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-owner-db-missing"
            )
        owner_reader = RegistryDerivedOwnerEpochReader(
            token=_FACTORY_TOKEN,
            path=owner_path,
            epoch=pin_body["ownerEpoch"],
            key_id_hash=pin_body["ownerAuthorityKeyIdHash"],
            verifier=verifiers["OWNER_STATE_VERIFY"],
            clock=clock,
            factory_binding_hash=binding_hash,
            file_guard=file_guard,
        )
        owner_reader.read()
        state_manager_verifier = RegistryDerivedStateManagerVerifier(
            token=_FACTORY_TOKEN,
            verifier=verifiers["GRAPH_RECORD_VERIFY"],
            factory_binding_hash=binding_hash,
            file_guard=file_guard,
        )
        state_manager = RegistryBoundStateManagerConstructors(
            token=_FACTORY_TOKEN,
            verifier=state_manager_verifier,
            factory_binding_hash=binding_hash,
            manager_code_hash=observed_files["manager"],
            file_guard=file_guard,
        )
        self._registry = registry
        self._pins = pins
        self._pin_body = pin_body
        self._clock = clock
        self._verifiers = verifiers
        self._owner_reader = owner_reader
        self._binding = binding
        self._binding_hash = binding_hash
        self._file_hashes = observed_files
        self._file_guard = file_guard
        self._state_manager = state_manager

    def verifier(self, purpose: str) -> RegistryDerivedVerifier:
        if purpose not in KEY_PURPOSES:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-purpose-invalid"
            )
        return self._verifiers[purpose]

    def owner_epoch_reader(self) -> RegistryDerivedOwnerEpochReader:
        return self._owner_reader

    def state_manager_constructors(self) -> RegistryBoundStateManagerConstructors:
        return self._state_manager

    def status(self) -> dict[str, Any]:
        if self._file_guard.verify() != self._binding["fileGuardBindingHash"]:
            raise KisDomesticFunctionalProductionFactoryBlocked(
                "production-factory-file-guard-binding-mismatch"
            )
        owner = self._owner_reader.read()
        registry_status = self._registry.status()
        blockers = [
            "SHARED_KIS_ROUTE_FENCE_NOT_WIRED",
            "PRODUCTION_GRAPH_COMPONENT_REGISTRY_NOT_WIRED",
            "DEDICATED_MANAGER_KEY_PURPOSE_NOT_ACCEPTED",
            "RUNTIME_STATE_MANAGER_GRAPH_NOT_CONSTRUCTED",
            "OWNER_OS_LEASE_CURRENTNESS_NOT_INDEPENDENTLY_WIRED",
            "OWNER_COMPONENT_HAZARD_SIGNATURE_REPLAY_NOT_WIRED",
        ]
        if registry_status.get("graphRegistryBindingWired") is not True:
            blockers.append("GRAPH_KEY_REGISTRY_BINDING_NOT_WIRED")
        if owner["sharedRouteFenceWired"] is not True:
            blockers.append("OWNER_SHARED_ROUTE_FENCE_NOT_WIRED")
        body = {
            "schemaVersion": STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "factoryBindingHash": self._binding_hash,
            "registryEpoch": self._pin_body["registryEpoch"],
            "registryAcceptedHeadHash": self._pin_body["registryAcceptedHeadHash"],
            "rootKeyIdHash": self._pin_body["rootKeyIdHash"],
            "accountFingerprint": self._pin_body["accountFingerprint"],
            "credentialConfigurationHash": self._pin_body["credentialConfigurationHash"],
            "codeManifestHash": self._pin_body["codeManifestHash"],
            "ownerEpoch": owner["ownerEpoch"],
            "ownerReaderResultHash": owner["readerResultHash"],
            "componentFileSetHash": self._binding["componentFileSetHash"],
            "fileGuardBindingHash": self._file_guard.binding_hash,
            "verifierPurposeCount": len(self._verifiers),
            "allVerifierPurposesRegistryDerived": set(self._verifiers) == set(KEY_PURPOSES),
            "callbackClosureAndPublicKeyIdentityPinned": True,
            "trustedClockLineagePinned": True,
            "ownerEpochReaderPinned": True,
            "stateReceiptV2IntegrationWired": True,
            "stateManagerExactConstructorsPinned": True,
            "dedicatedManagerKeyPurposeAvailable": False,
            "verifyOnly": True,
            "privateKeyMaterialPresent": False,
            "signingSurfacePresent": False,
            "networkSurfacePresent": False,
            "readinessBlockers": sorted(blockers),
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "sharedRouteFenceWired": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "statusHash": _hash(body)}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "schemaVersion": STATUS_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "sharedRouteFenceWired": False,
        "verifyOnlyRegistryFactoryImplemented": True,
        "verifyOnlyRegistryFactoryAvailable": False,
        "stateReceiptV2IntegrationImplemented": True,
        "stateReceiptV2IntegrationWired": False,
        "stateManagerExactConstructorsPinned": False,
        "privateKeyMaterialPresent": False,
        "signingSurfacePresent": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
        "reason": "PRODUCTION_REGISTRY_AND_SHARED_ROUTE_GRAPH_NOT_WIRED",
    }


__all__ = [
    "DisabledKisDomesticFunctionalProductionFactory",
    "KisDomesticFunctionalProductionFactoryBlocked",
    "ProductionFactoryPins",
    "RegistryBoundStateManagerConstructors",
    "RegistryDerivedOwnerEpochReader",
    "RegistryDerivedStateManagerVerifier",
    "RegistryDerivedVerifier",
    "production_entrypoint_status",
]
