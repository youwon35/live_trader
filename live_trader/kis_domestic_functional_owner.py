from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .kis_domestic_functional_contract import PDNO, ROUTE
from .process_safety import CrossProcessLease, acquire_process_lease


KIS_DOMESTIC_FUNCTIONAL_OWNER_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_OWNER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_OWNER_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_OWNER_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_OWNER_STATE_SERVER_WIRED = False

SCHEMA_VERSION = "kis-domestic-functional-owner-schema/v3"
OWNER_RECORD_SCHEMA = "kis-domestic-functional-owner-record/v3"
HAZARD_SCHEMA = "kis-domestic-functional-owner-hazard/v3"
STATUS_SCHEMA = "kis-domestic-functional-owner-status/v3"
HAZARD_READ_REQUEST_SCHEMA = "kis-domestic-functional-owner-hazard-read/v1"
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 30
_SCOPE = "live-trader:kis-domestic-functional-owner:kr-live-continuous:v1"
_SCOPE_HASH = hashlib.sha256(_SCOPE.encode("utf-8")).hexdigest()
_PINNED_PROCESS_SAFETY_FILE_HASH = (
    "175b2074e983c74d67ce4178784ac3fd8a7db5aa1e1e9477dd1f2a8bb66ffa80"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_COMPONENTS = (
    "lane",
    "source",
    "rolling",
    "heartbeat",
    "mutation",
    "capability",
    "quote",
    "graph",
    "truth",
)
_STATES = {"ACTIVE", "RECONCILIATION_REQUIRED", "RELEASED"}

_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS kis_functional_owner_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        schema_version TEXT NOT NULL,
        schema_fingerprint TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kis_functional_route_owner(
        route TEXT PRIMARY KEY,
        pdno TEXT NOT NULL,
        epoch INTEGER NOT NULL CHECK(epoch>=1),
        state TEXT NOT NULL CHECK(state IN ('ACTIVE','RECONCILIATION_REQUIRED','RELEASED')),
        owner_id_hash TEXT NOT NULL,
        process_identity_hash TEXT NOT NULL,
        lease_scope_hash TEXT NOT NULL,
        lease_factory_hash TEXT NOT NULL,
        acquired_at TEXT NOT NULL,
        acquired_monotonic_ns INTEGER NOT NULL CHECK(acquired_monotonic_ns>=0),
        heartbeat_at TEXT NOT NULL,
        heartbeat_monotonic_ns INTEGER NOT NULL CHECK(heartbeat_monotonic_ns>=acquired_monotonic_ns),
        heartbeat_count INTEGER NOT NULL CHECK(heartbeat_count>=1),
        hazardous_authority_open INTEGER NOT NULL CHECK(hazardous_authority_open IN (0,1)),
        owned_exposure_present INTEGER NOT NULL CHECK(owned_exposure_present IN (0,1)),
        orphan_count INTEGER NOT NULL CHECK(orphan_count>=0),
        timed_out_call_count INTEGER NOT NULL CHECK(timed_out_call_count>=0),
        detached_call_count INTEGER NOT NULL CHECK(detached_call_count>=0),
        hazard_union_hash TEXT NOT NULL,
        route_observation_id TEXT NOT NULL,
        route_fence_revision INTEGER NOT NULL CHECK(route_fence_revision>=1),
        route_fence_hash TEXT NOT NULL,
        hazard_observed_at TEXT NOT NULL,
        hazard_observed_monotonic_ns INTEGER NOT NULL CHECK(hazard_observed_monotonic_ns>=0),
        session_id TEXT NOT NULL,
        authority_expires_at TEXT NOT NULL,
        shared_route_fence_wired INTEGER NOT NULL CHECK(shared_route_fence_wired IN (0,1)),
        hazard_reader_registry_hash TEXT NOT NULL,
        reason TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK(revision>=1),
        record_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        authority_key_id_hash TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS kis_functional_owner_transition(
        route TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        revision INTEGER NOT NULL,
        phase TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        occurred_monotonic_ns INTEGER NOT NULL CHECK(occurred_monotonic_ns>=0),
        previous_hash TEXT NOT NULL,
        record_json TEXT NOT NULL,
        record_hash TEXT NOT NULL,
        signature TEXT NOT NULL,
        authority_key_id_hash TEXT NOT NULL,
        PRIMARY KEY(route,epoch,revision)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_kis_functional_owner_transition_route ON kis_functional_owner_transition(route,epoch,revision)",
)

LeaseFactory = Callable[[str], CrossProcessLease | None]
HazardReader = Callable[[Mapping[str, Any]], Mapping[str, Any]]
Signer = Callable[[str, Mapping[str, Any]], str]
Verifier = Callable[[str, Mapping[str, Any], str], bool]
WallClock = Callable[[], datetime]
MonotonicClock = Callable[[], int]

_HAZARD_PIN_KEYS = {
    "componentReaderHash",
    "componentFileHash",
    "componentProtocolHash",
    "authorityKeyIdHash",
}
_HAZARD_UNSIGNED_KEYS = {
    "schemaVersion",
    "component",
    *_HAZARD_PIN_KEYS,
    "routeObservationId",
    "routeFenceRevision",
    "routeFenceHash",
    "observedAt",
    "observedMonotonicNs",
    "componentRevision",
    "componentHeadHash",
    "sessionId",
    "authorityExpiresAt",
    "hazardousAuthorityOpen",
    "ownedWorkingExposurePresent",
    "ownedPositionExposurePresent",
    "nonterminalOrphanCount",
    "timedOutCallCount",
    "detachedCallCount",
}
_HAZARD_RECORD_KEYS = _HAZARD_UNSIGNED_KEYS | {"recordHash", "signature"}
_OWNER_RECORD_KEYS = {
    "schemaVersion", "route", "pdno", "epoch", "state", "ownerIdHash",
    "processIdentityHash", "leaseScopeHash", "leaseFactoryHash",
    "acquiredAt", "acquiredMonotonicNs", "heartbeatAt",
    "heartbeatMonotonicNs", "heartbeatCount", "hazardousAuthorityOpen",
    "ownedExposurePresent", "orphanCount", "timedOutCallCount",
    "detachedCallCount", "hazardUnionHash", "hazardComponents",
    "routeObservationId", "routeFenceRevision", "routeFenceHash",
    "hazardObservedAt", "hazardObservedMonotonicNs", "sessionId",
    "authorityExpiresAt", "sharedRouteFenceWired",
    "hazardReaderRegistryHash", "reason", "revision", "authorityKeyIdHash",
}


class _Lease(Protocol):
    scope: str
    path: Path

    def status(self, *, reused: bool = False) -> Mapping[str, Any]: ...

    def release(self) -> None: ...


class KisDomesticFunctionalOwnerBlocked(RuntimeError):
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
        raise KisDomesticFunctionalOwnerBlocked("owner-json-invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalOwnerBlocked(f"{label}-invalid")
    return value


def _utc(value: Any, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalOwnerBlocked(f"{label}-invalid")
    parsed = value.astimezone(timezone.utc)
    if not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalOwnerBlocked(f"{label}-invalid")
    return parsed


def _time_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalOwnerBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalOwnerBlocked(f"{label}-invalid") from exc
    return _utc(parsed, label)


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).lower()


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): _normalize_sql(row[2])
        for row in conn.execute(
            "SELECT name,type,sql FROM sqlite_master "
            "WHERE name LIKE 'kis_functional_owner_%' ORDER BY name"
        ).fetchall()
    }
    columns = {
        name: [tuple(row) for row in conn.execute(f"PRAGMA table_info({name})")]
        for name in (
            "kis_functional_owner_meta",
            "kis_functional_route_owner",
            "kis_functional_owner_transition",
        )
        if name in objects
    }
    indexes = {
        name: [tuple(row) for row in conn.execute(f"PRAGMA index_xinfo({name})")]
        for name in objects
        if name.startswith("idx_")
    }
    return {"objects": objects, "columns": columns, "indexes": indexes}


def _expected_schema() -> tuple[dict[str, Any], str]:
    conn = sqlite3.connect(":memory:")
    try:
        for sql in _SCHEMA_SQL:
            conn.execute(sql)
        snapshot = _schema_snapshot(conn)
        return snapshot, _hash(snapshot)
    finally:
        conn.close()


_EXPECTED_SCHEMA, SCHEMA_FINGERPRINT = _expected_schema()


def _verify_schema(conn: sqlite3.Connection) -> None:
    if _schema_snapshot(conn) != _EXPECTED_SCHEMA:
        raise KisDomesticFunctionalOwnerBlocked("owner-schema-dirty")
    row = conn.execute(
        "SELECT singleton,schema_version,schema_fingerprint "
        "FROM kis_functional_owner_meta"
    ).fetchone()
    if row is None or tuple(row) != (1, SCHEMA_VERSION, SCHEMA_FINGERPRINT):
        raise KisDomesticFunctionalOwnerBlocked("owner-schema-meta-dirty")


def _lease_held(lease: _Lease) -> bool:
    checker = getattr(lease, "is_held", None)
    if callable(checker):
        try:
            return checker() is True
        except BaseException:
            return False
    handle = getattr(lease, "handle", None)
    if handle is not None and bool(getattr(handle, "closed", True)):
        return False
    try:
        return lease.status(reused=True).get("acquired") is True
    except BaseException:
        return False


class DurableKisDomesticFunctionalOwner:
    def __init__(
        self,
        database_path: str | Path,
        *,
        hazard_readers: Mapping[str, HazardReader],
        hazard_reader_pins: Mapping[str, Mapping[str, str]],
        hazard_verifiers: Mapping[str, Verifier],
        signer: Signer,
        verifier: Verifier,
        server_authority_key_id: str,
        trusted_wall_clock: WallClock,
        trusted_monotonic_clock: MonotonicClock = time.monotonic_ns,
        lease_factory: LeaseFactory = acquire_process_lease,
        allow_mock_lease_factory: bool = False,
        heartbeat_timeout_seconds: int = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(database_path).expanduser().resolve()
        if set(hazard_readers) != set(_COMPONENTS):
            raise KisDomesticFunctionalOwnerBlocked("owner-hazard-readers-not-exact")
        if not all(callable(value) for value in hazard_readers.values()):
            raise KisDomesticFunctionalOwnerBlocked("owner-hazard-reader-invalid")
        if set(hazard_reader_pins) != set(_COMPONENTS):
            raise KisDomesticFunctionalOwnerBlocked("owner-hazard-reader-pins-not-exact")
        if set(hazard_verifiers) != set(_COMPONENTS) or not all(
            callable(value) for value in hazard_verifiers.values()
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-hazard-verifiers-not-exact"
            )
        normalized_pins: dict[str, dict[str, str]] = {}
        for component in _COMPONENTS:
            pin = hazard_reader_pins[component]
            if not isinstance(pin, Mapping) or set(pin) != _HAZARD_PIN_KEYS:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-reader-pin-invalid:{component}"
                )
            normalized_pins[component] = {
                key: _sha(pin.get(key), f"owner-hazard-pin:{component}:{key}")
                for key in sorted(_HAZARD_PIN_KEYS)
            }
        if not all(
            callable(value)
            for value in (
                signer,
                verifier,
                trusted_wall_clock,
                trusted_monotonic_clock,
                lease_factory,
            )
        ):
            raise KisDomesticFunctionalOwnerBlocked("owner-callable-invalid")
        if type(allow_mock_lease_factory) is not bool:
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-mock-lease-factory-flag-invalid"
            )
        production_lease_factory = lease_factory is acquire_process_lease
        if not production_lease_factory and not allow_mock_lease_factory:
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-lease-factory-not-code-pinned"
            )
        if production_lease_factory:
            process_safety_hash = hashlib.sha256(
                Path(__file__).with_name("process_safety.py").read_bytes()
            ).hexdigest()
            if not hmac.compare_digest(
                process_safety_hash, _PINNED_PROCESS_SAFETY_FILE_HASH
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-process-safety-file-drift"
                )
        if type(server_authority_key_id) is not str or not server_authority_key_id:
            raise KisDomesticFunctionalOwnerBlocked("owner-authority-key-id-invalid")
        if type(heartbeat_timeout_seconds) is not int or not 2 <= heartbeat_timeout_seconds <= 300:
            raise KisDomesticFunctionalOwnerBlocked("owner-heartbeat-timeout-invalid")
        self.hazard_readers = dict(hazard_readers)
        self.hazard_reader_pins = normalized_pins
        self.hazard_verifiers = dict(hazard_verifiers)
        self.hazard_reader_registry_hash = _hash(normalized_pins)
        self.signer = signer
        self.verifier = verifier
        self.clock = trusted_wall_clock
        self.monotonic_clock = trusted_monotonic_clock
        self.lease_factory = lease_factory
        self.production_lease_factory_pinned = production_lease_factory
        self.lease_factory_hash = (
            _PINNED_PROCESS_SAFETY_FILE_HASH
            if production_lease_factory
            else "0" * 64
        )
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.failure_injector = failure_injector
        self.authority_key_id_hash = hashlib.sha256(
            server_authority_key_id.encode("utf-8")
        ).hexdigest()
        self.owner_id_hash = hashlib.sha256(uuid.uuid4().bytes).hexdigest()
        self.process_identity_hash = hashlib.sha256(
            f"{os.getpid()}:{uuid.uuid4().hex}".encode()
        ).hexdigest()
        self._lease: _Lease | None = None
        self._epoch = 0
        self._route_observation_revision = 0
        self._closed = False
        self._lock = threading.RLock()
        self._prepare_schema()
        self._acquire()

    @property
    def epoch(self) -> int:
        return self._epoch

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _prepare_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            before = _schema_snapshot(conn)
            if before["objects"] and before != _EXPECTED_SCHEMA:
                raise KisDomesticFunctionalOwnerBlocked("owner-schema-dirty")
            for sql in _SCHEMA_SQL:
                conn.execute(sql)
            conn.execute(
                "INSERT OR IGNORE INTO kis_functional_owner_meta "
                "VALUES(1,?,?)",
                (SCHEMA_VERSION, SCHEMA_FINGERPRINT),
            )
            _verify_schema(conn)
            conn.commit()
        finally:
            conn.close()

    def _now(self) -> datetime:
        return _utc(self.clock(), "owner-trusted-now")

    def _monotonic_now(self) -> int:
        value = self.monotonic_clock()
        if type(value) is not int or value < 0:
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-trusted-monotonic-now-invalid"
            )
        return value

    def _now_pair(self) -> tuple[datetime, int]:
        return self._now(), self._monotonic_now()

    def _inject(self, stage: str) -> None:
        if self.failure_injector is not None:
            self.failure_injector(stage)

    def _sign(self, domain: str, body: Mapping[str, Any]) -> tuple[str, str, str]:
        text = _canonical(body).decode("utf-8")
        digest = _hash(body)
        try:
            signature = self.signer(domain, deepcopy(dict(body)))
            valid = self.verifier(domain, deepcopy(dict(body)), signature)
        except BaseException as exc:
            raise KisDomesticFunctionalOwnerBlocked(
                f"owner-signature-authority-failed:{type(exc).__name__}"
            ) from None
        _sha(signature, "owner-signature")
        if valid is not True:
            raise KisDomesticFunctionalOwnerBlocked("owner-signature-invalid")
        return text, digest, signature

    def _read_hazards(
        self, *, now: datetime, monotonic_ns: int
    ) -> dict[str, Any]:
        self._route_observation_revision += 1
        request_body = {
            "schemaVersion": HAZARD_READ_REQUEST_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "ownerIdHash": self.owner_id_hash,
            "processIdentityHash": self.process_identity_hash,
            "ownerEpoch": self._epoch,
            "routeFenceRevision": self._route_observation_revision,
            "observedAt": _time_text(now),
            "observedMonotonicNs": monotonic_ns,
            "sharedRouteFenceWired": False,
        }
        route_fence_hash = _hash(request_body)
        request = {
            **request_body,
            "routeObservationId": route_fence_hash,
            "routeFenceHash": route_fence_hash,
        }
        records: dict[str, Any] = {}
        for component in _COMPONENTS:
            try:
                value = self.hazard_readers[component](deepcopy(request))
            except BaseException as exc:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-reader-failed:{component}:{type(exc).__name__}"
                ) from None
            if not isinstance(value, Mapping) or set(value) != _HAZARD_RECORD_KEYS:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-contract-invalid:{component}"
                )
            if (
                value.get("schemaVersion") != HAZARD_SCHEMA
                or value.get("component") != component
                or value.get("routeObservationId") != route_fence_hash
                or value.get("routeFenceRevision")
                != self._route_observation_revision
                or value.get("routeFenceHash") != route_fence_hash
                or value.get("observedAt") != request_body["observedAt"]
                or value.get("observedMonotonicNs") != monotonic_ns
                or type(value.get("componentRevision")) is not int
                or value["componentRevision"] < 1
                or type(value.get("sessionId")) is not str
                or not value["sessionId"]
                or type(value.get("hazardousAuthorityOpen")) is not bool
                or type(value.get("ownedWorkingExposurePresent")) is not bool
                or type(value.get("ownedPositionExposurePresent")) is not bool
                or any(
                    type(value.get(field)) is not int or value.get(field) < 0
                    for field in (
                        "nonterminalOrphanCount",
                        "timedOutCallCount",
                        "detachedCallCount",
                    )
                )
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-contract-invalid:{component}"
                )
            _sha(
                value.get("componentHeadHash"),
                f"owner-hazard-component-head:{component}",
            )
            expires_at = _parse_time(
                value.get("authorityExpiresAt"),
                f"owner-hazard-authority-expiry:{component}",
            )
            if expires_at < now or expires_at > now + timedelta(hours=3):
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-authority-expiry-invalid:{component}"
                )
            pins = self.hazard_reader_pins[component]
            if any(
                not hmac.compare_digest(
                    _sha(
                        value.get(key),
                        f"owner-hazard-record:{component}:{key}",
                    ),
                    pins[key],
                )
                for key in _HAZARD_PIN_KEYS
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-reader-pin-mismatch:{component}"
                )
            unsigned = {
                key: deepcopy(value[key])
                for key in sorted(_HAZARD_UNSIGNED_KEYS)
            }
            if not hmac.compare_digest(
                _sha(
                    value.get("recordHash"),
                    f"owner-hazard-record-hash:{component}",
                ),
                _hash(unsigned),
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-record-hash-mismatch:{component}"
                )
            signature = _sha(
                value.get("signature"),
                f"owner-hazard-record-signature:{component}",
            )
            try:
                valid = self.hazard_verifiers[component](
                    f"KIS_FUNCTIONAL_OWNER_HAZARD:{component.upper()}",
                    deepcopy(unsigned),
                    signature,
                )
            except BaseException as exc:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-verifier-failed:{component}:{type(exc).__name__}"
                ) from None
            if valid is not True:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-hazard-record-signature-mismatch:{component}"
                )
            records[component] = deepcopy(dict(value))
        session_ids = {item["sessionId"] for item in records.values()}
        expiries = {item["authorityExpiresAt"] for item in records.values()}
        if len(session_ids) != 1 or len(expiries) != 1:
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-hazard-route-instant-session-or-expiry-mismatch"
            )
        union = {
            "schemaVersion": "kis-domestic-functional-owner-hazard-union/v2",
            "route": ROUTE,
            "pdno": PDNO,
            "routeObservationId": route_fence_hash,
            "routeFenceRevision": self._route_observation_revision,
            "routeFenceHash": route_fence_hash,
            "observedAt": request_body["observedAt"],
            "observedMonotonicNs": monotonic_ns,
            "sessionId": next(iter(session_ids)),
            "authorityExpiresAt": next(iter(expiries)),
            "sharedRouteFenceWired": False,
            "components": records,
            "hazardousAuthorityOpen": any(
                item["hazardousAuthorityOpen"] for item in records.values()
            ),
            "ownedExposurePresent": any(
                item["ownedWorkingExposurePresent"]
                or item["ownedPositionExposurePresent"]
                for item in records.values()
            ),
            "orphanCount": sum(
                item["nonterminalOrphanCount"] for item in records.values()
            ),
            "timedOutCallCount": sum(
                item["timedOutCallCount"] for item in records.values()
            ),
            "detachedCallCount": sum(
                item["detachedCallCount"] for item in records.values()
            ),
        }
        return {**union, "hazardUnionHash": _hash(union)}

    @staticmethod
    def _hazard_open(hazard: Mapping[str, Any]) -> bool:
        return bool(
            hazard["hazardousAuthorityOpen"]
            or hazard["ownedExposurePresent"]
            or hazard["orphanCount"]
            or hazard["timedOutCallCount"]
            or hazard["detachedCallCount"]
        )

    def _record_body(
        self,
        *,
        epoch: int,
        state: str,
        now: datetime,
        monotonic_ns: int,
        acquired_at: str,
        acquired_monotonic_ns: int,
        heartbeat_count: int,
        hazard: Mapping[str, Any],
        reason: str,
        revision: int,
        lease_scope_hash: str,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": OWNER_RECORD_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "epoch": epoch,
            "state": state,
            "ownerIdHash": self.owner_id_hash,
            "processIdentityHash": self.process_identity_hash,
            "leaseScopeHash": lease_scope_hash,
            "leaseFactoryHash": self.lease_factory_hash,
            "acquiredAt": acquired_at,
            "acquiredMonotonicNs": acquired_monotonic_ns,
            "heartbeatAt": _time_text(now),
            "heartbeatMonotonicNs": monotonic_ns,
            "heartbeatCount": heartbeat_count,
            "hazardousAuthorityOpen": bool(hazard["hazardousAuthorityOpen"]),
            "ownedExposurePresent": bool(hazard["ownedExposurePresent"]),
            "orphanCount": int(hazard["orphanCount"]),
            "timedOutCallCount": int(hazard["timedOutCallCount"]),
            "detachedCallCount": int(hazard["detachedCallCount"]),
            "hazardUnionHash": str(hazard["hazardUnionHash"]),
            "hazardComponents": deepcopy(dict(hazard["components"])),
            "routeObservationId": hazard["routeObservationId"],
            "routeFenceRevision": hazard["routeFenceRevision"],
            "routeFenceHash": hazard["routeFenceHash"],
            "hazardObservedAt": hazard["observedAt"],
            "hazardObservedMonotonicNs": hazard["observedMonotonicNs"],
            "sessionId": hazard["sessionId"],
            "authorityExpiresAt": hazard["authorityExpiresAt"],
            "sharedRouteFenceWired": hazard["sharedRouteFenceWired"],
            "hazardReaderRegistryHash": self.hazard_reader_registry_hash,
            "reason": reason,
            "revision": revision,
            "authorityKeyIdHash": self.authority_key_id_hash,
        }

    def _write_owner(
        self,
        conn: sqlite3.Connection,
        *,
        body: Mapping[str, Any],
        previous_hash: str,
        insert: bool,
        expected_epoch: int | None,
        expected_revision: int | None,
    ) -> None:
        text, digest, signature = self._sign("KIS_FUNCTIONAL_OWNER", body)
        values = (
            body["pdno"], body["epoch"], body["state"], body["ownerIdHash"],
            body["processIdentityHash"], body["leaseScopeHash"], body["leaseFactoryHash"],
            body["acquiredAt"], body["acquiredMonotonicNs"],
            body["heartbeatAt"], body["heartbeatMonotonicNs"], body["heartbeatCount"],
            int(body["hazardousAuthorityOpen"]), int(body["ownedExposurePresent"]),
            body["orphanCount"], body["timedOutCallCount"],
            body["detachedCallCount"], body["hazardUnionHash"],
            body["routeObservationId"], body["routeFenceRevision"],
            body["routeFenceHash"], body["hazardObservedAt"],
            body["hazardObservedMonotonicNs"], body["sessionId"],
            body["authorityExpiresAt"], int(body["sharedRouteFenceWired"]),
            body["hazardReaderRegistryHash"], body["reason"],
            body["revision"], text, digest, signature, self.authority_key_id_hash,
        )
        if insert:
            conn.execute(
                "INSERT INTO kis_functional_route_owner VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ROUTE,) + values,
            )
        else:
            changed = conn.execute(
                "UPDATE kis_functional_route_owner SET "
                "pdno=?,epoch=?,state=?,owner_id_hash=?,process_identity_hash=?,"
                "lease_scope_hash=?,lease_factory_hash=?,acquired_at=?,acquired_monotonic_ns=?,"
                "heartbeat_at=?,heartbeat_monotonic_ns=?,heartbeat_count=?,"
                "hazardous_authority_open=?,owned_exposure_present=?,orphan_count=?,"
                "timed_out_call_count=?,detached_call_count=?,hazard_union_hash=?,"
                "route_observation_id=?,route_fence_revision=?,route_fence_hash=?,"
                "hazard_observed_at=?,hazard_observed_monotonic_ns=?,session_id=?,"
                "authority_expires_at=?,shared_route_fence_wired=?,hazard_reader_registry_hash=?,"
                "reason=?,revision=?,record_json=?,record_hash=?,signature=?,"
                "authority_key_id_hash=? WHERE route=? AND epoch=? AND revision=?",
                values + (ROUTE, expected_epoch, expected_revision),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalOwnerBlocked("owner-epoch-or-revision-stale")
        transition = {
            **dict(body),
            "previousHash": previous_hash,
            "occurredAt": body["heartbeatAt"],
            "occurredMonotonicNs": body["heartbeatMonotonicNs"],
        }
        transition_text, transition_hash, transition_signature = self._sign(
            "KIS_FUNCTIONAL_OWNER_TRANSITION", transition
        )
        conn.execute(
            "INSERT INTO kis_functional_owner_transition VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                ROUTE, body["epoch"], body["revision"], body["state"],
                body["heartbeatAt"], body["heartbeatMonotonicNs"], previous_hash, transition_text,
                transition_hash, transition_signature, self.authority_key_id_hash,
            ),
        )

    def _acquire(self) -> None:
        lease = self.lease_factory(_SCOPE)
        if lease is None or not _lease_held(lease):
            raise KisDomesticFunctionalOwnerBlocked("owner-os-lease-unavailable")
        if (
            getattr(lease, "scope", None) != _SCOPE
            or Path(getattr(lease, "path", Path("invalid"))).stem != _SCOPE_HASH
            or (self.production_lease_factory_pinned and type(lease) is not CrossProcessLease)
        ):
            try:
                lease.release()
            finally:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-os-lease-scope-or-factory-mismatch"
                )
        self._lease = lease
        try:
            self._inject("AFTER_OS_LEASE")
            now, monotonic_ns = self._now_pair()
            hazard = self._read_hazards(now=now, monotonic_ns=monotonic_ns)
            lease_status = lease.status(reused=True)
            if set(lease_status) != {
                "acquired",
                "scopeHash",
                "ownerPid",
                "acquiredAt",
                "reused",
            } or (
                lease_status.get("acquired") is not True
                or lease_status.get("reused") is not True
                or type(lease_status.get("ownerPid")) is not int
                or lease_status["ownerPid"] <= 0
                or lease_status.get("scopeHash") != _SCOPE_HASH
                or _parse_time(
                    lease_status.get("acquiredAt"), "owner-os-lease-acquired-at"
                )
                > now
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-os-lease-status-invalid"
                )
            lease_scope_hash = _sha(
                lease_status.get("scopeHash"), "owner-lease-scope-hash"
            )
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                _verify_schema(conn)
                prior = conn.execute(
                    "SELECT * FROM kis_functional_route_owner WHERE route=?",
                    (ROUTE,),
                ).fetchone()
                self._inject("AFTER_OWNER_ROW_READ")
                if prior is None:
                    epoch = 1
                    state = (
                        "RECONCILIATION_REQUIRED"
                        if self._hazard_open(hazard)
                        else "ACTIVE"
                    )
                    reason = "HAZARD_PRESENT_AT_FIRST_ACQUIRE" if state != "ACTIVE" else "FIRST_ACQUIRE"
                    revision = 1
                    previous_hash = "0" * 64
                    insert = True
                    expected_epoch = expected_revision = None
                else:
                    prior, _verified_prior = self._load_verified(
                        conn, int(prior["epoch"])
                    )
                    if now < _parse_time(
                        prior["heartbeat_at"], "owner-prior-heartbeat-at"
                    ):
                        raise KisDomesticFunctionalOwnerBlocked(
                            "owner-acquire-wall-clock-rollback"
                        )
                    epoch = int(prior["epoch"]) + 1
                    prior_state = str(prior["state"])
                    persisted_hazard = bool(
                        prior["hazardous_authority_open"]
                        or prior["owned_exposure_present"]
                        or prior["orphan_count"]
                        or prior["timed_out_call_count"]
                        or prior["detached_call_count"]
                    )
                    normal_reissue = prior_state == "RELEASED" and not persisted_hazard and not self._hazard_open(hazard)
                    state = "ACTIVE" if normal_reissue else "RECONCILIATION_REQUIRED"
                    reason = "CLEAN_REISSUE_AFTER_RELEASE" if normal_reissue else "OLD_PROCESS_ABSENT_NONTERMINAL_OR_HAZARDOUS_EPOCH"
                    revision = 1
                    previous_hash = str(prior["record_hash"])
                    insert = False
                    expected_epoch = int(prior["epoch"])
                    expected_revision = int(prior["revision"])
                body = self._record_body(
                    epoch=epoch,
                    state=state,
                    now=now,
                    monotonic_ns=monotonic_ns,
                    acquired_at=_time_text(now),
                    acquired_monotonic_ns=monotonic_ns,
                    heartbeat_count=1,
                    hazard=hazard,
                    reason=reason,
                    revision=revision,
                    lease_scope_hash=lease_scope_hash,
                )
                self._write_owner(
                    conn,
                    body=body,
                    previous_hash=previous_hash,
                    insert=insert,
                    expected_epoch=expected_epoch,
                    expected_revision=expected_revision,
                )
                self._inject("BEFORE_ACQUIRE_COMMIT")
                conn.commit()
                self._epoch = epoch
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        except BaseException:
            try:
                lease.release()
            finally:
                self._lease = None
                self._closed = True
            raise

    def _assert_live(self, expected_epoch: int) -> None:
        if self._closed or self._lease is None or not _lease_held(self._lease):
            raise KisDomesticFunctionalOwnerBlocked("owner-os-lease-lost")
        if type(expected_epoch) is not int or expected_epoch != self._epoch:
            raise KisDomesticFunctionalOwnerBlocked("owner-epoch-stale")

    def _load_verified(self, conn: sqlite3.Connection, expected_epoch: int) -> tuple[sqlite3.Row, dict[str, Any]]:
        _verify_schema(conn)
        row = conn.execute(
            "SELECT * FROM kis_functional_route_owner WHERE route=? AND epoch=?",
            (ROUTE, expected_epoch),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalOwnerBlocked("owner-durable-epoch-missing")
        try:
            body = json.loads(row["record_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalOwnerBlocked("owner-record-json-invalid") from exc
        if (
            not isinstance(body, Mapping)
            or set(body) != _OWNER_RECORD_KEYS
            or body.get("schemaVersion") != OWNER_RECORD_SCHEMA
            or body.get("route") != ROUTE
            or body.get("pdno") != PDNO
            or body.get("leaseScopeHash") != _SCOPE_HASH
            or body.get("leaseFactoryHash") != self.lease_factory_hash
            or body.get("hazardReaderRegistryHash")
            != self.hazard_reader_registry_hash
            or _hash(body) != str(row["record_hash"])
            or str(row["authority_key_id_hash"]) != self.authority_key_id_hash
        ):
            raise KisDomesticFunctionalOwnerBlocked("owner-record-integrity-invalid")
        try:
            valid = self.verifier("KIS_FUNCTIONAL_OWNER", deepcopy(dict(body)), str(row["signature"]))
        except BaseException as exc:
            raise KisDomesticFunctionalOwnerBlocked(
                f"owner-record-verifier-failed:{type(exc).__name__}"
            ) from None
        if valid is not True:
            raise KisDomesticFunctionalOwnerBlocked("owner-record-signature-invalid")
        hazard_components = body.get("hazardComponents")
        if (
            not isinstance(hazard_components, Mapping)
            or set(hazard_components) != set(_COMPONENTS)
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-record-hazard-components-invalid"
            )
        for component, item in hazard_components.items():
            if (
                not isinstance(item, Mapping)
                or set(item) != _HAZARD_RECORD_KEYS
                or item.get("component") != component
                or item.get("schemaVersion") != HAZARD_SCHEMA
                or item.get("routeObservationId")
                != body.get("routeObservationId")
                or item.get("routeFenceRevision")
                != body.get("routeFenceRevision")
                or item.get("routeFenceHash") != body.get("routeFenceHash")
                or item.get("observedAt") != body.get("hazardObservedAt")
                or item.get("observedMonotonicNs")
                != body.get("hazardObservedMonotonicNs")
                or item.get("sessionId") != body.get("sessionId")
                or item.get("authorityExpiresAt")
                != body.get("authorityExpiresAt")
                or type(item.get("componentRevision")) is not int
                or item["componentRevision"] < 1
                or type(item.get("componentHeadHash")) is not str
                or not _SHA256.fullmatch(item["componentHeadHash"])
                or any(
                    item.get(key) != self.hazard_reader_pins[component][key]
                    for key in _HAZARD_PIN_KEYS
                )
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-record-hazard-component-contract-invalid"
                )
            unsigned = {
                key: deepcopy(item[key]) for key in sorted(_HAZARD_UNSIGNED_KEYS)
            }
            if item.get("recordHash") != _hash(unsigned):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-record-hazard-component-hash-invalid"
                )
            try:
                hazard_valid = self.hazard_verifiers[component](
                    f"KIS_FUNCTIONAL_OWNER_HAZARD:{component.upper()}",
                    deepcopy(unsigned),
                    item.get("signature"),
                )
            except BaseException as exc:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-record-hazard-verifier-failed:{component}:{type(exc).__name__}"
                ) from None
            if hazard_valid is not True:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-record-hazard-component-signature-invalid"
                )
        recomputed_hazard = {
            "schemaVersion": "kis-domestic-functional-owner-hazard-union/v2",
            "route": ROUTE,
            "pdno": PDNO,
            "routeObservationId": body.get("routeObservationId"),
            "routeFenceRevision": body.get("routeFenceRevision"),
            "routeFenceHash": body.get("routeFenceHash"),
            "observedAt": body.get("hazardObservedAt"),
            "observedMonotonicNs": body.get("hazardObservedMonotonicNs"),
            "sessionId": body.get("sessionId"),
            "authorityExpiresAt": body.get("authorityExpiresAt"),
            "sharedRouteFenceWired": body.get("sharedRouteFenceWired"),
            "components": deepcopy(dict(hazard_components)),
            "hazardousAuthorityOpen": any(
                item.get("hazardousAuthorityOpen") is True
                for item in hazard_components.values()
                if isinstance(item, Mapping)
            ),
            "ownedExposurePresent": any(
                item.get("ownedWorkingExposurePresent") is True
                or item.get("ownedPositionExposurePresent") is True
                for item in hazard_components.values()
                if isinstance(item, Mapping)
            ),
            "orphanCount": sum(
                int(item.get("nonterminalOrphanCount", -1))
                for item in hazard_components.values()
                if isinstance(item, Mapping)
            ),
            "timedOutCallCount": sum(
                int(item.get("timedOutCallCount", -1))
                for item in hazard_components.values()
                if isinstance(item, Mapping)
            ),
            "detachedCallCount": sum(
                int(item.get("detachedCallCount", -1))
                for item in hazard_components.values()
                if isinstance(item, Mapping)
            ),
        }
        if (
            len(recomputed_hazard["components"]) != len(_COMPONENTS)
            or _hash(recomputed_hazard) != body.get("hazardUnionHash")
            or recomputed_hazard["hazardousAuthorityOpen"]
            is not body.get("hazardousAuthorityOpen")
            or recomputed_hazard["ownedExposurePresent"]
            is not body.get("ownedExposurePresent")
            or recomputed_hazard["orphanCount"] != body.get("orphanCount")
            or recomputed_hazard["timedOutCallCount"]
            != body.get("timedOutCallCount")
            or recomputed_hazard["detachedCallCount"]
            != body.get("detachedCallCount")
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-record-hazard-union-mismatch"
            )
        hazard_observed_at = _parse_time(
            body.get("hazardObservedAt"), "owner-record-hazard-observed-at"
        )
        authority_expires_at = _parse_time(
            body.get("authorityExpiresAt"),
            "owner-record-authority-expires-at",
        )
        if (
            type(body.get("hazardObservedMonotonicNs")) is not int
            or body["hazardObservedMonotonicNs"] < 0
            or hazard_observed_at
            > _parse_time(body.get("heartbeatAt"), "owner-heartbeat-at")
            or body["hazardObservedMonotonicNs"]
            > body.get("heartbeatMonotonicNs")
            or authority_expires_at < hazard_observed_at
            or body.get("sharedRouteFenceWired") is not False
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-record-hazard-route-instant-invalid"
            )
        transitions = conn.execute(
            "SELECT * FROM kis_functional_owner_transition WHERE route=? "
            "ORDER BY epoch,revision",
            (ROUTE,),
        ).fetchall()
        if not transitions:
            raise KisDomesticFunctionalOwnerBlocked("owner-transition-chain-missing")
        expected_previous = "0" * 64
        prior_epoch = 0
        prior_revision = 0
        last_owner_body: Mapping[str, Any] | None = None
        prior_occurred_at: datetime | None = None
        prior_monotonic_ns: int | None = None
        for transition_row in transitions:
            epoch = int(transition_row["epoch"])
            revision = int(transition_row["revision"])
            if (
                epoch < 1
                or epoch > expected_epoch
                or (epoch == prior_epoch and revision != prior_revision + 1)
                or (epoch != prior_epoch and (epoch != prior_epoch + 1 or revision != 1))
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-chain-sequence-invalid"
                )
            try:
                transition = json.loads(transition_row["record_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-json-invalid"
                ) from exc
            if (
                not isinstance(transition, Mapping)
                or _hash(transition) != str(transition_row["record_hash"])
                or transition.get("previousHash") != expected_previous
                or str(transition_row["previous_hash"]) != expected_previous
                or transition.get("occurredAt") != str(transition_row["occurred_at"])
                or transition.get("occurredMonotonicNs")
                != int(transition_row["occurred_monotonic_ns"])
                or transition.get("epoch") != epoch
                or transition.get("revision") != revision
                or transition.get("state") != str(transition_row["phase"])
                or str(transition_row["authority_key_id_hash"])
                != self.authority_key_id_hash
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-chain-integrity-invalid"
                )
            occurred_at = _parse_time(
                transition["occurredAt"], "owner-transition-occurred-at"
            )
            occurred_monotonic_ns = transition["occurredMonotonicNs"]
            if type(occurred_monotonic_ns) is not int or occurred_monotonic_ns < 0:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-monotonic-invalid"
                )
            if prior_occurred_at is not None and occurred_at < prior_occurred_at:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-wall-lineage-rollback"
                )
            if (
                epoch == prior_epoch
                and prior_monotonic_ns is not None
                and occurred_monotonic_ns < prior_monotonic_ns
            ):
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-monotonic-lineage-rollback"
                )
            try:
                transition_valid = self.verifier(
                    "KIS_FUNCTIONAL_OWNER_TRANSITION",
                    deepcopy(dict(transition)),
                    str(transition_row["signature"]),
                )
            except BaseException as exc:
                raise KisDomesticFunctionalOwnerBlocked(
                    f"owner-transition-verifier-failed:{type(exc).__name__}"
                ) from None
            if transition_valid is not True:
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-transition-signature-invalid"
                )
            owner_body = {
                key: value
                for key, value in transition.items()
                if key not in {
                    "previousHash",
                    "occurredAt",
                    "occurredMonotonicNs",
                }
            }
            expected_previous = _hash(owner_body)
            last_owner_body = owner_body
            prior_epoch = epoch
            prior_revision = revision
            prior_occurred_at = occurred_at
            prior_monotonic_ns = occurred_monotonic_ns
        if (
            prior_epoch != expected_epoch
            or expected_previous != str(row["record_hash"])
            or last_owner_body != body
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-transition-chain-head-mismatch"
            )
        projection = {
            "epoch": int(row["epoch"]), "state": str(row["state"]),
            "ownerIdHash": str(row["owner_id_hash"]),
            "heartbeatAt": str(row["heartbeat_at"]),
            "acquiredAt": str(row["acquired_at"]),
            "acquiredMonotonicNs": int(row["acquired_monotonic_ns"]),
            "heartbeatMonotonicNs": int(row["heartbeat_monotonic_ns"]),
            "heartbeatCount": int(row["heartbeat_count"]),
            "revision": int(row["revision"]),
            "hazardUnionHash": str(row["hazard_union_hash"]),
            "routeObservationId": str(row["route_observation_id"]),
            "routeFenceRevision": int(row["route_fence_revision"]),
            "routeFenceHash": str(row["route_fence_hash"]),
            "hazardObservedAt": str(row["hazard_observed_at"]),
            "hazardObservedMonotonicNs": int(
                row["hazard_observed_monotonic_ns"]
            ),
            "sessionId": str(row["session_id"]),
            "authorityExpiresAt": str(row["authority_expires_at"]),
            "sharedRouteFenceWired": bool(row["shared_route_fence_wired"]),
            "hazardReaderRegistryHash": str(row["hazard_reader_registry_hash"]),
            "leaseFactoryHash": str(row["lease_factory_hash"]),
            "processIdentityHash": str(row["process_identity_hash"]),
            "leaseScopeHash": str(row["lease_scope_hash"]),
            "pdno": str(row["pdno"]),
            "hazardousAuthorityOpen": bool(row["hazardous_authority_open"]),
            "ownedExposurePresent": bool(row["owned_exposure_present"]),
            "orphanCount": int(row["orphan_count"]),
            "timedOutCallCount": int(row["timed_out_call_count"]),
            "detachedCallCount": int(row["detached_call_count"]),
            "reason": str(row["reason"]),
            "authorityKeyIdHash": str(row["authority_key_id_hash"]),
        }
        if any(body.get(key) != value for key, value in projection.items()):
            raise KisDomesticFunctionalOwnerBlocked("owner-record-projection-mismatch")
        return row, dict(body)

    def _freshness_reason(
        self,
        *,
        now: datetime,
        monotonic_ns: int,
        prior_body: Mapping[str, Any],
    ) -> str | None:
        acquired_at = _parse_time(
            prior_body.get("acquiredAt"), "owner-acquired-at"
        )
        heartbeat_at = _parse_time(
            prior_body.get("heartbeatAt"), "owner-heartbeat-at"
        )
        acquired_monotonic_ns = prior_body.get("acquiredMonotonicNs")
        heartbeat_monotonic_ns = prior_body.get("heartbeatMonotonicNs")
        if (
            type(acquired_monotonic_ns) is not int
            or type(heartbeat_monotonic_ns) is not int
            or acquired_monotonic_ns < 0
            or heartbeat_monotonic_ns < acquired_monotonic_ns
        ):
            raise KisDomesticFunctionalOwnerBlocked(
                "owner-monotonic-lineage-invalid"
            )
        if now < acquired_at or now < heartbeat_at:
            return "TRUSTED_WALL_CLOCK_ROLLBACK"
        if (
            monotonic_ns < acquired_monotonic_ns
            or monotonic_ns < heartbeat_monotonic_ns
        ):
            return "TRUSTED_MONOTONIC_CLOCK_ROLLBACK"
        timeout_ns = self.heartbeat_timeout_seconds * 1_000_000_000
        if now - heartbeat_at > timedelta(
            seconds=self.heartbeat_timeout_seconds
        ):
            return "HEARTBEAT_WALL_LEASE_STALE"
        if monotonic_ns - heartbeat_monotonic_ns > timeout_ns:
            return "HEARTBEAT_MONOTONIC_LEASE_STALE"
        return None

    @staticmethod
    def _nondecreasing_observation(
        *,
        now: datetime,
        monotonic_ns: int,
        prior_body: Mapping[str, Any],
    ) -> tuple[datetime, int]:
        return (
            max(now, _parse_time(prior_body["heartbeatAt"], "owner-heartbeat-at")),
            max(monotonic_ns, int(prior_body["heartbeatMonotonicNs"])),
        )

    def _refresh(
        self,
        *,
        expected_epoch: int,
        phase: str,
        require_active: bool,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        self._assert_live(expected_epoch)
        now, monotonic_ns = self._now_pair()
        hazard = self._read_hazards(now=now, monotonic_ns=monotonic_ns)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row, prior_body = self._load_verified(conn, expected_epoch)
            state = str(row["state"])
            freshness_reason = self._freshness_reason(
                now=now,
                monotonic_ns=monotonic_ns,
                prior_body=prior_body,
            )
            unsafe = (
                self._hazard_open(hazard)
                or freshness_reason is not None
                or state != "ACTIVE"
            )
            target_state = "RECONCILIATION_REQUIRED" if unsafe else "ACTIVE"
            reason = (
                freshness_reason if freshness_reason is not None
                else "HAZARD_OR_ORPHAN_UNION_OPEN" if self._hazard_open(hazard)
                else str(row["reason"])
            )
            write_now, write_monotonic_ns = self._nondecreasing_observation(
                now=now,
                monotonic_ns=monotonic_ns,
                prior_body=prior_body,
            )
            revision = int(row["revision"]) + 1
            body = self._record_body(
                epoch=expected_epoch,
                state=target_state,
                now=write_now,
                monotonic_ns=write_monotonic_ns,
                acquired_at=str(row["acquired_at"]),
                acquired_monotonic_ns=int(row["acquired_monotonic_ns"]),
                heartbeat_count=int(row["heartbeat_count"]) + 1,
                hazard=hazard,
                reason=reason,
                revision=revision,
                lease_scope_hash=str(row["lease_scope_hash"]),
            )
            self._write_owner(
                conn,
                body=body,
                previous_hash=str(row["record_hash"]),
                insert=False,
                expected_epoch=expected_epoch,
                expected_revision=int(row["revision"]),
            )
            conn.commit()
            if require_active and target_state != "ACTIVE":
                raise KisDomesticFunctionalOwnerBlocked(
                    "owner-operation-blocked-by-hazard-or-stale-lease"
                )
            return body, hazard
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def heartbeat(self, *, expected_epoch: int) -> dict[str, Any]:
        with self._lock:
            body, _hazard = self._refresh(
                expected_epoch=expected_epoch,
                phase="HEARTBEAT",
                require_active=True,
            )
            return self._status_body(body)

    def guard_operation(self, *, expected_epoch: int) -> dict[str, Any]:
        with self._lock:
            body, _hazard = self._refresh(
                expected_epoch=expected_epoch,
                phase="OPERATION_GUARD",
                require_active=True,
            )
            return {
                "epoch": expected_epoch,
                "revision": body["revision"],
                "authorityFresh": True,
                "hazardousAuthorityOpen": False,
                "ownedExposurePresent": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }

    def status(self, *, expected_epoch: int) -> dict[str, Any]:
        with self._lock:
            self._assert_live(expected_epoch)
            conn = self._connect()
            try:
                conn.execute("BEGIN")
                row, body = self._load_verified(conn, expected_epoch)
                conn.commit()
            finally:
                conn.close()
            return self._status_body(body)

    def _status_body(self, body: Mapping[str, Any]) -> dict[str, Any]:
        now, monotonic_ns = self._now_pair()
        heartbeat_age = now - _parse_time(body["heartbeatAt"], "owner-heartbeat-at")
        monotonic_age_ns = monotonic_ns - int(body["heartbeatMonotonicNs"])
        freshness_reason = self._freshness_reason(
            now=now,
            monotonic_ns=monotonic_ns,
            prior_body=body,
        )
        result = {
            "schemaVersion": STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "epoch": body["epoch"],
            "state": body["state"],
            "revision": body["revision"],
            "ownerIdHash": body["ownerIdHash"],
            "processIdentityHash": body["processIdentityHash"],
            "leaseScopeHash": body["leaseScopeHash"],
            "leaseFactoryHash": body["leaseFactoryHash"],
            "leaseFactoryPinned": self.production_lease_factory_pinned,
            "heartbeatAt": body["heartbeatAt"],
            "heartbeatMonotonicNs": body["heartbeatMonotonicNs"],
            "heartbeatCount": body["heartbeatCount"],
            "heartbeatLeaseFresh": freshness_reason is None,
            "wallHeartbeatAgeSeconds": heartbeat_age.total_seconds(),
            "monotonicHeartbeatAgeSeconds": monotonic_age_ns / 1_000_000_000,
            "clockLineageFailureReason": freshness_reason,
            "hazardousAuthorityOpen": body["hazardousAuthorityOpen"],
            "ownedExposurePresent": body["ownedExposurePresent"],
            "orphanCount": body["orphanCount"],
            "timedOutCallCount": body["timedOutCallCount"],
            "detachedCallCount": body["detachedCallCount"],
            "hazardUnionHash": body["hazardUnionHash"],
            "routeObservationId": body["routeObservationId"],
            "routeFenceRevision": body["routeFenceRevision"],
            "routeFenceHash": body["routeFenceHash"],
            "hazardObservedAt": body["hazardObservedAt"],
            "hazardObservedMonotonicNs": body["hazardObservedMonotonicNs"],
            "sessionId": body["sessionId"],
            "authorityExpiresAt": body["authorityExpiresAt"],
            "sharedRouteFenceWired": body["sharedRouteFenceWired"],
            "hazardReaderRegistryHash": body["hazardReaderRegistryHash"],
            "verifyOnlyProductionAuthorityPinned": False,
            "readinessBlockers": [
                "EXTERNAL_VERIFY_ONLY_HAZARD_AUTHORITY_REGISTRY_NOT_WIRED",
                "SHARED_ROUTE_FENCE_NOT_WIRED",
            ],
            "reason": body["reason"],
            "osLeaseHeld": bool(self._lease is not None and _lease_held(self._lease)),
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
            "stateServerWired": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**result, "statusHash": _hash(result)}

    def release(self, *, expected_epoch: int) -> dict[str, Any]:
        with self._lock:
            self._assert_live(expected_epoch)
            now, monotonic_ns = self._now_pair()
            hazard = self._read_hazards(now=now, monotonic_ns=monotonic_ns)
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row, prior_body = self._load_verified(conn, expected_epoch)
                persisted_hazard = bool(
                    row["hazardous_authority_open"]
                    or row["owned_exposure_present"]
                    or row["orphan_count"]
                    or row["timed_out_call_count"]
                    or row["detached_call_count"]
                )
                freshness_reason = self._freshness_reason(
                    now=now,
                    monotonic_ns=monotonic_ns,
                    prior_body=prior_body,
                )
                release_blocker = (
                    freshness_reason
                    or (
                        "PERSISTED_HAZARD_OR_ORPHAN_UNION_OPEN"
                        if persisted_hazard
                        else None
                    )
                    or (
                        "HAZARD_OR_ORPHAN_UNION_OPEN"
                        if self._hazard_open(hazard)
                        else None
                    )
                    or (
                        "OWNER_STATE_NOT_ACTIVE"
                        if str(row["state"]) != "ACTIVE"
                        else None
                    )
                )
                write_now, write_monotonic_ns = self._nondecreasing_observation(
                    now=now,
                    monotonic_ns=monotonic_ns,
                    prior_body=prior_body,
                )
                body = self._record_body(
                    epoch=expected_epoch,
                    state=(
                        "RECONCILIATION_REQUIRED"
                        if release_blocker is not None
                        else "RELEASED"
                    ),
                    now=write_now,
                    monotonic_ns=write_monotonic_ns,
                    acquired_at=str(row["acquired_at"]),
                    acquired_monotonic_ns=int(row["acquired_monotonic_ns"]),
                    heartbeat_count=int(row["heartbeat_count"]) + 1,
                    hazard=hazard,
                    reason=release_blocker or "CLEAN_RELEASE",
                    revision=int(row["revision"]) + 1,
                    lease_scope_hash=str(row["lease_scope_hash"]),
                )
                self._write_owner(
                    conn,
                    body=body,
                    previous_hash=str(row["record_hash"]),
                    insert=False,
                    expected_epoch=expected_epoch,
                    expected_revision=int(row["revision"]),
                )
                self._inject("BEFORE_RELEASE_COMMIT")
                conn.commit()
                if release_blocker is not None:
                    raise KisDomesticFunctionalOwnerBlocked(
                        "owner-release-forbidden-hazard-stale-rollback-or-nonactive"
                    )
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
            lease = self._lease
            if lease is None:
                raise KisDomesticFunctionalOwnerBlocked("owner-os-lease-lost")
            lease.release()
            self._lease = None
            self._closed = True
            return {
                "epoch": expected_epoch,
                "state": "RELEASED",
                "revision": body["revision"],
                "osLeaseHeld": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }


def owner_component_status() -> dict[str, Any]:
    body = {
        "schemaVersion": "kis-domestic-functional-owner-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "schemaFingerprint": SCHEMA_FINGERPRINT,
        "leaseScopeHash": _SCOPE_HASH,
        "pinnedProcessSafetyFileHash": _PINNED_PROCESS_SAFETY_FILE_HASH,
        "hazardComponents": list(_COMPONENTS),
        "signedHazardReaderContractRequired": True,
        "monotonicHeartbeatRequired": True,
        "verifyOnlyProductionAuthorityPinned": False,
        "readinessBlockers": [
            "EXTERNAL_VERIFY_ONLY_HAZARD_AUTHORITY_REGISTRY_NOT_WIRED",
            "SHARED_ROUTE_FENCE_NOT_WIRED",
        ],
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "stateServerWired": False,
        "networkOrderPostAllowed": False,
        "tradingMutationCount": 0,
    }
    return {**body, "componentHash": _hash(body)}


__all__ = [
    "DEFAULT_HEARTBEAT_TIMEOUT_SECONDS",
    "HAZARD_SCHEMA",
    "KIS_DOMESTIC_FUNCTIONAL_OWNER_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_OWNER_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_OWNER_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_OWNER_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_OWNER_STATE_SERVER_WIRED",
    "KisDomesticFunctionalOwnerBlocked",
    "DurableKisDomesticFunctionalOwner",
    "SCHEMA_FINGERPRINT",
    "SCHEMA_VERSION",
    "owner_component_status",
]
