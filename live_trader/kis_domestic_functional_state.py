from __future__ import annotations

"""Disabled state-owned KIS functional composition and two-phase coordinator."""

from datetime import datetime, timezone
from contextlib import contextmanager
import hashlib
import hmac
import json
import re
import sqlite3
import uuid
from typing import Any, Callable, Iterator, Mapping

from .kis_order_authority import kis_route_authority_serialization
from .program_ledger import ProgramLedger


KIS_DOMESTIC_FUNCTIONAL_STATE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_STATE_BACKEND_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_STATE_NETWORK_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
_SCHEMA_VERSION = "kis-domestic-functional-state-schema/v4"
_COMPONENT_SCHEMA = "kis-domestic-functional-component-status/v1"
_OWNER_EPOCH_SCHEMA = "kis-domestic-functional-owner-epoch/v1"
_MANAGER_RECEIPT_V1_SCHEMA = "kis-domestic-functional-manager-receipt/v1"
_MANAGER_RECEIPT_V2_SCHEMA = "kis-domestic-functional-manager-receipt/v2"
_MANAGER_BINDING_REQUEST_SCHEMA = (
    "kis-domestic-functional-state-manager-binding-request/v1"
)
_MANAGER_BINDING_SCHEMA = "kis-domestic-functional-state-manager-binding/v1"
_MANAGER_RECEIPT_JOURNAL_SCHEMA = (
    "kis-domestic-functional-state-manager-receipt-journal/v1"
)
_FINAL_BOUNDARY_SCHEMA = "kis-domestic-functional-final-reservation/v1"
_COMPONENTS = ("graph", "backend", "capability", "transport")
_OPEN_PHASES = {
    "ARMED_WAIT_PUBLIC", "ACTIVE", "CLEANUP", "RECONCILIATION_REQUIRED"
}
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_HAZARD = re.compile(r"^[A-Z][A-Z0-9_]{2,95}$", flags=re.ASCII)

_DDL = """
CREATE TABLE IF NOT EXISTS kis_functional_state_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    version TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    owner_hash TEXT NOT NULL,
    component_owner_hashes_json TEXT NOT NULL
    ,owner_epoch_key_id_hash TEXT NOT NULL
    ,manager_receipt_key_id_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kis_functional_state_authority (
    route TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1),
    session_id TEXT NOT NULL,
    pending_session_id TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    credential_configuration_hash TEXT NOT NULL,
    ordinary_routes_closed INTEGER NOT NULL CHECK(ordinary_routes_closed IN (0,1)),
    owner_epoch_id TEXT NOT NULL,
    owner_epoch_hash TEXT NOT NULL,
    durable_hazards_json TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    reservation_kind TEXT NOT NULL,
    reservation_revision INTEGER NOT NULL CHECK(reservation_revision>=0),
    reservation_binding_hash TEXT NOT NULL,
    transition_head_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kis_functional_state_transition (
    route TEXT NOT NULL,
    revision INTEGER NOT NULL,
    phase TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    reservation_kind TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    body_json TEXT NOT NULL,
    body_hash TEXT NOT NULL UNIQUE,
    previous_hash TEXT NOT NULL,
    signature_hash TEXT NOT NULL,
    signer_key_id_hash TEXT NOT NULL,
    PRIMARY KEY(route,revision)
);
CREATE TABLE IF NOT EXISTS kis_functional_state_manager_binding (
    route TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    reservation_kind TEXT NOT NULL,
    reservation_revision INTEGER NOT NULL CHECK(reservation_revision>=2),
    state_component_readers_hash TEXT NOT NULL,
    manager_component_readers_hash TEXT NOT NULL,
    manager_binding_json TEXT NOT NULL,
    manager_binding_hash TEXT NOT NULL,
    final_mutation_boundary_handle TEXT NOT NULL,
    reservation_binding_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    PRIMARY KEY(route,reservation_id)
);
CREATE TABLE IF NOT EXISTS kis_functional_state_manager_receipt (
    route TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    reservation_kind TEXT NOT NULL,
    reservation_revision INTEGER NOT NULL CHECK(reservation_revision>=2),
    receipt_kind TEXT NOT NULL CHECK(receipt_kind IN ('FINISH','PENDING')),
    manager_receipt_hash TEXT NOT NULL,
    execution_proof_hash TEXT NOT NULL,
    mutation_plan_hash TEXT NOT NULL,
    owned_projection_hash TEXT NOT NULL,
    owned_projection_head_hash TEXT NOT NULL,
    boundary_entry_proof_hash TEXT NOT NULL,
    attempt_chain_head TEXT NOT NULL,
    transport_receipt_set_hash TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    receipt_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    key_id_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    persisted_at TEXT NOT NULL,
    state_transition_revision INTEGER NOT NULL CHECK(state_transition_revision>=3),
    PRIMARY KEY(route,reservation_id,receipt_kind)
)
""".strip()


class KisDomesticFunctionalStateBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalStateBlocked(f"{label} is invalid")
    return value


def _identity(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalStateBlocked(f"{label} is invalid")
    return value


def _utc(value: datetime, label: str) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        raise KisDomesticFunctionalStateBlocked(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _verified_bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise KisDomesticFunctionalStateBlocked(f"{label} is invalid")
    return value


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = [tuple(row) for row in conn.execute(
        """SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master
           WHERE name LIKE 'kis_functional_state_%' OR tbl_name LIKE 'kis_functional_state_%'
           ORDER BY type,name"""
    )]
    tables = {}
    for kind, name, _, _ in objects:
        if kind != "table":
            continue
        escaped = str(name).replace('"', '""')
        tables[str(name)] = {
            "xinfo": tuple(tuple(row) for row in conn.execute(f'PRAGMA table_xinfo("{escaped}")')),
            "indexes": tuple(tuple(row) for row in conn.execute(f'PRAGMA index_list("{escaped}")')),
            "foreignKeys": tuple(tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{escaped}")')),
        }
    return {"objects": objects, "tables": tables}


class DurableKisDomesticFunctionalState:
    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        owner_id: str,
        component_owner_ids: Mapping[str, str],
        component_readers: Mapping[str, Callable[[], Mapping[str, Any]]],
        account_fingerprint: str,
        credential_configuration_hash: str,
        application_lease_held: bool,
        owner_epoch_reader: Callable[[], Mapping[str, Any]] | None = None,
        owner_epoch_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
        owner_epoch_key_id_hash: str | None = None,
        manager_receipt_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
        manager_receipt_key_id_hash: str | None = None,
        manager_binding_reader: Callable[
            [Mapping[str, Any]], Mapping[str, Any]
        ] | None = None,
        manager_binding_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
        manager_implementation_type: str = "",
        manager_code_hash: str | None = None,
        manager_protocol_hash: str | None = None,
        state_signer_key: bytes | None = None,
        state_signer_key_id: str = "",
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalStateBlocked("exact ProgramLedger is required")
        owner_id = _identity(owner_id, "state owner id")
        if not isinstance(component_owner_ids, Mapping) or set(component_owner_ids) != set(_COMPONENTS):
            raise KisDomesticFunctionalStateBlocked("component owner set is not exact")
        if not isinstance(component_readers, Mapping) or set(component_readers) != set(_COMPONENTS):
            raise KisDomesticFunctionalStateBlocked("component reader set is not exact")
        if any(not callable(component_readers[name]) for name in _COMPONENTS):
            raise KisDomesticFunctionalStateBlocked("component reader is invalid")
        if type(application_lease_held) is not bool:
            raise KisDomesticFunctionalStateBlocked("application lease flag is invalid")
        if state_signer_key is None and state_signer_key_id == "":
            # Compatibility is confined to this disabled composition layer;
            # production readiness remains false until a secret-store key is supplied.
            state_signer_key = hashlib.sha256(
                ("disabled-state-key\x00" + owner_id).encode()
            ).digest()
            state_signer_key_id = "disabled-derived-state-key-v1"
        if type(state_signer_key) is not bytes or len(state_signer_key) < 32:
            raise KisDomesticFunctionalStateBlocked("state signer key is invalid")
        state_signer_key_id = _identity(state_signer_key_id, "state signer key id")
        self.ledger = program_ledger
        self._owner_hash = hashlib.sha256(owner_id.encode()).hexdigest()
        self._key = bytes(state_signer_key)
        self._key_id_hash = hashlib.sha256(state_signer_key_id.encode()).hexdigest()
        self._component_owner_hashes = {
            name: hashlib.sha256(_identity(component_owner_ids[name], f"{name} owner id").encode()).hexdigest()
            for name in _COMPONENTS
        }
        self._disabled_owner_epoch_key = hashlib.sha256(
            b"disabled-kis-state-owner-epoch-reader-v1"
        ).digest()
        self._disabled_manager_key = hashlib.sha256(
            b"disabled-kis-state-manager-receipt-v1"
        ).digest()
        disabled_owner_epoch = (
            owner_epoch_reader is None
            and owner_epoch_verifier is None
            and owner_epoch_key_id_hash is None
        )
        if disabled_owner_epoch:
            owner_epoch_key_id_hash = hashlib.sha256(
                b"disabled-owner-epoch-key-v1"
            ).hexdigest()
            owner_epoch_reader = lambda: dict(self._disabled_owner_epoch_snapshot)
            owner_epoch_verifier = self._verify_disabled_owner_epoch
        if not callable(owner_epoch_reader) or not callable(owner_epoch_verifier):
            raise KisDomesticFunctionalStateBlocked("owner epoch reader/verifier is required")
        disabled_manager_receipts = (
            manager_receipt_verifier is None
            and manager_receipt_key_id_hash is None
        )
        if disabled_manager_receipts:
            manager_receipt_key_id_hash = hashlib.sha256(
                b"disabled-manager-receipt-key-v1"
            ).hexdigest()
            manager_receipt_verifier = self._verify_disabled_manager_receipt
        if not callable(manager_receipt_verifier):
            raise KisDomesticFunctionalStateBlocked("manager receipt verifier is required")
        self._owner_epoch_reader = owner_epoch_reader
        self._owner_epoch_verifier = owner_epoch_verifier
        self._owner_epoch_key_id_hash = _sha(
            owner_epoch_key_id_hash, "owner epoch key id hash"
        )
        self._manager_receipt_verifier = manager_receipt_verifier
        self._disabled_manager_receipts = disabled_manager_receipts
        self._manager_receipt_key_id_hash = _sha(
            manager_receipt_key_id_hash, "manager receipt key id hash"
        )
        manager_v2_requested = any(
            value is not None and value != ""
            for value in (
                manager_binding_reader,
                manager_binding_verifier,
                manager_implementation_type,
                manager_code_hash,
                manager_protocol_hash,
            )
        )
        if manager_v2_requested:
            if (
                disabled_manager_receipts
                or not callable(manager_binding_reader)
                or not callable(manager_binding_verifier)
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "v2 manager binding requires external verify-only readers"
                )
            self._manager_implementation_type = _identity(
                manager_implementation_type, "manager implementation type"
            )
            self._manager_code_hash = _sha(manager_code_hash, "manager code hash")
            self._manager_protocol_hash = _sha(
                manager_protocol_hash, "manager protocol hash"
            )
        else:
            if manager_binding_reader is not None or manager_binding_verifier is not None:
                raise KisDomesticFunctionalStateBlocked(
                    "partial manager binding configuration is forbidden"
                )
            self._manager_implementation_type = ""
            self._manager_code_hash = ""
            self._manager_protocol_hash = ""
        self._manager_binding_reader = manager_binding_reader
        self._manager_binding_verifier = manager_binding_verifier
        self._manager_v2_wired = manager_v2_requested
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if disabled_owner_epoch:
            # An owner epoch is an immutable, signed ownership generation, not
            # a freshly timestamped assertion on every read.  Caching the
            # disabled fixture also exercises the same exact-hash join used by
            # an external state-owned reader.
            self._disabled_owner_epoch_snapshot = self._disabled_owner_epoch(
                application_lease_held
            )
        self._readers = {name: component_readers[name] for name in _COMPONENTS}
        initial_epoch = self._owner_epoch()
        self._ensure_schema(
            _sha(account_fingerprint, "account fingerprint"),
            _sha(credential_configuration_hash, "credential configuration hash"),
            initial_epoch,
        )

    def _now(self) -> str:
        return _utc(self.clock(), "state clock")

    def _disabled_owner_epoch(self, held: bool) -> dict[str, Any]:
        body = {
            "schemaVersion": _OWNER_EPOCH_SCHEMA, "route": ROUTE,
            "ownerHash": self._owner_hash, "ownerEpochId": "disabled-owner-epoch-v1",
            "applicationLeaseHeld": held, "observedAt": self._now(),
            "keyIdHash": self._owner_epoch_key_id_hash,
            "productionAvailable": False,
        }
        body_hash = _hash(body)
        return {
            **body, "ownerEpochHash": body_hash,
            "signature": hmac.new(
                self._disabled_owner_epoch_key,
                ("KIS_OWNER_EPOCH\n" + body_hash).encode(), hashlib.sha256,
            ).hexdigest(),
        }

    def _verify_disabled_owner_epoch(self, value: Mapping[str, Any]) -> bool:
        raw = dict(value); signature = raw.pop("signature", "")
        digest = raw.pop("ownerEpochHash", "")
        return (
            type(signature) is str and type(digest) is str
            and hmac.compare_digest(digest, _hash(raw))
            and hmac.compare_digest(
                signature,
                hmac.new(
                    self._disabled_owner_epoch_key,
                    ("KIS_OWNER_EPOCH\n" + digest).encode(), hashlib.sha256,
                ).hexdigest(),
            )
        )

    def _owner_epoch(self) -> dict[str, Any]:
        try:
            raw = self._owner_epoch_reader()
            if not isinstance(raw, Mapping):
                raise TypeError("non-mapping")
            value = dict(raw)
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked("owner epoch reader is unreadable") from exc
        keys = {
            "schemaVersion", "route", "ownerHash", "ownerEpochId",
            "applicationLeaseHeld", "observedAt", "keyIdHash",
            "productionAvailable", "ownerEpochHash", "signature",
        }
        if set(value) != keys:
            raise KisDomesticFunctionalStateBlocked("owner epoch snapshot is not exact")
        exact = {
            "schemaVersion": _OWNER_EPOCH_SCHEMA, "route": ROUTE,
            "ownerHash": self._owner_hash, "keyIdHash": self._owner_epoch_key_id_hash,
            "productionAvailable": False,
        }
        for key, expected in exact.items():
            if type(value.get(key)) is not type(expected) or value.get(key) != expected:
                raise KisDomesticFunctionalStateBlocked(f"owner epoch {key} mismatch")
        _identity(value["ownerEpochId"], "owner epoch id")
        _verified_bool(value["applicationLeaseHeld"], "owner epoch lease")
        _utc(datetime.fromisoformat(value["observedAt"].replace("Z", "+00:00")), "owner epoch observedAt")
        _sha(value["ownerEpochHash"], "owner epoch hash")
        _sha(value["signature"], "owner epoch signature")
        try:
            verified = self._owner_epoch_verifier(dict(value))
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked("owner epoch verifier failed") from exc
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalStateBlocked("owner epoch signature is unverified")
        return value

    def _matching_owner_epoch(self, row: sqlite3.Row) -> dict[str, Any]:
        owner_epoch = self._owner_epoch()
        if (
            str(row["owner_epoch_id"]) != owner_epoch["ownerEpochId"]
            or str(row["owner_epoch_hash"]) != owner_epoch["ownerEpochHash"]
        ):
            raise KisDomesticFunctionalStateBlocked("owner epoch changed")
        return owner_epoch

    @staticmethod
    def _final_boundary_body(
        *, reservation_id: str, reservation_kind: str, revision: int,
        session_id: str, account: str, credential: str,
        owner_epoch_hash: str, component_readers_hash: str,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": _FINAL_BOUNDARY_SCHEMA,
            "route": ROUTE,
            "reservationId": reservation_id,
            "reservationKind": reservation_kind,
            "reservationRevision": revision,
            "sessionId": session_id,
            "accountFingerprint": account,
            "credentialConfigurationHash": credential,
            "ownerEpochHash": owner_epoch_hash,
            "componentReadersHash": component_readers_hash,
            "productionAvailable": False,
        }

    def sign_disabled_manager_result_for_tests(
        self, *, reservation: Mapping[str, Any], ok: bool,
        mutation_may_have_occurred: bool, components_hash: str,
    ) -> dict[str, Any]:
        body = {
            "schemaVersion": _MANAGER_RECEIPT_V1_SCHEMA, "route": ROUTE,
            "reservationId": reservation["reservationId"],
            "reservationKind": reservation["reservationKind"],
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation["reservedCredentialConfigurationHash"],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "componentReadersHash": components_hash,
            "ok": ok, "mutationMayHaveOccurred": mutation_may_have_occurred,
            "occurredAt": self._now(), "keyIdHash": self._manager_receipt_key_id_hash,
            "productionAvailable": False,
        }
        digest = _hash(body)
        return {
            **body, "receiptHash": digest,
            "signature": hmac.new(
                self._disabled_manager_key,
                ("KIS_MANAGER_RECEIPT\n" + digest).encode(), hashlib.sha256,
            ).hexdigest(),
        }

    def _verify_disabled_manager_receipt(self, value: Mapping[str, Any]) -> bool:
        raw = dict(value); signature = raw.pop("signature", "")
        digest = raw.pop("receiptHash", "")
        return (
            type(signature) is str and type(digest) is str
            and hmac.compare_digest(digest, _hash(raw))
            and hmac.compare_digest(
                signature,
                hmac.new(
                    self._disabled_manager_key,
                    ("KIS_MANAGER_RECEIPT\n" + digest).encode(), hashlib.sha256,
                ).hexdigest(),
            )
        )

    def _ensure_schema(self, account: str, credential: str, owner_epoch: Mapping[str, Any]) -> None:
        statements = [item.strip() for item in _DDL.split(";") if item.strip()]
        expected = sqlite3.connect(":memory:")
        try:
            for statement in statements:
                expected.execute(statement)
            schema_hash = _hash(_schema_snapshot(expected))
        finally:
            expected.close()
        owners_json = _canonical(self._component_owner_hashes)
        with self.ledger.connection() as conn:
            before = _schema_snapshot(conn)
            if before["objects"] and _hash(before) != schema_hash:
                raise KisDomesticFunctionalStateBlocked("state SQLite schema fingerprint mismatch")
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                conn.execute(statement)
            if _hash(_schema_snapshot(conn)) != schema_hash:
                raise KisDomesticFunctionalStateBlocked("state SQLite schema fingerprint mismatch")
            manifest = conn.execute("SELECT * FROM kis_functional_state_schema").fetchall()
            expected_manifest = (
                1, _SCHEMA_VERSION, schema_hash, self._owner_hash, owners_json,
                self._owner_epoch_key_id_hash, self._manager_receipt_key_id_hash,
            )
            if not manifest:
                conn.execute("INSERT INTO kis_functional_state_schema VALUES (?,?,?,?,?,?,?)", expected_manifest)
            elif len(manifest) != 1 or tuple(manifest[0]) != expected_manifest:
                raise KisDomesticFunctionalStateBlocked("state owner/component manifest mismatch")
            row = conn.execute("SELECT * FROM kis_functional_state_authority WHERE route=?", (ROUTE,)).fetchone()
            if row is None:
                now = self._now()
                body = {
                    "schemaVersion": "kis-domestic-functional-state-transition/v1",
                    "route": ROUTE, "revision": 1, "phase": "IDLE",
                    "sessionId": "", "pendingSessionId": "",
                    "accountFingerprint": account,
                    "credentialConfigurationHash": credential,
                    "ordinaryRoutesClosed": False,
                    "ownerEpochId": owner_epoch["ownerEpochId"],
                    "ownerEpochHash": owner_epoch["ownerEpochHash"],
                    "hazards": [], "reservationId": "", "reservationKind": "",
                    "reservationRevision": 0, "reservationBindingHash": "",
                    "occurredAt": now,
                    "previousHash": "0" * 64,
                    "signerKeyIdHash": self._key_id_hash,
                    "productionAvailable": False,
                }
                body_json = _canonical(body)
                body_hash = _hash(body)
                signature = hmac.new(
                    self._key,
                    ("KIS_STATE_TRANSITION\n" + body_hash).encode(),
                    hashlib.sha256,
                ).hexdigest()
                conn.execute(
                    """INSERT INTO kis_functional_state_authority
                       VALUES (?,'IDLE',1,'','',?,?,0,?,?,'[]','','',0,'',?,?)""",
                    (ROUTE, account, credential, owner_epoch["ownerEpochId"],
                     owner_epoch["ownerEpochHash"], body_hash, now),
                )
                conn.execute(
                    """INSERT INTO kis_functional_state_transition
                       VALUES (?,1,'IDLE','','',?,?,?,?,?,?)""",
                    (ROUTE, now, body_json, body_hash, "0" * 64,
                     signature, self._key_id_hash),
                )
            elif row["account_fingerprint"] != account or row["credential_configuration_hash"] != credential:
                raise KisDomesticFunctionalStateBlocked("state durable account/credential changed")

    def _component(
        self,
        name: str,
        row: sqlite3.Row,
        *,
        expected_account: str | None = None,
        expected_credential: str | None = None,
    ) -> dict[str, Any]:
        try:
            raw = self._readers[name]()
            if not isinstance(raw, Mapping):
                raise TypeError("non-mapping")
            value = dict(raw)
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked(f"{name} component reader is unreadable") from exc
        exact_keys = {
            "schemaVersion", "component", "ownerHash", "route", "readable",
            "sessionId", "accountFingerprint", "credentialConfigurationHash",
            "hazards", "functionalMutationIntent", "killOrdinaryCancelAllowed",
            "killOrdinaryCancelRevision", "killOrdinaryCancelIntent", "productionAvailable",
        }
        if set(value) != exact_keys:
            raise KisDomesticFunctionalStateBlocked(f"{name} component snapshot is not exact")
        exact = {
            "schemaVersion": _COMPONENT_SCHEMA, "component": name,
            "ownerHash": self._component_owner_hashes[name], "route": ROUTE,
            "readable": True,
            "accountFingerprint": expected_account or row["account_fingerprint"],
            "credentialConfigurationHash": expected_credential or row["credential_configuration_hash"],
            "productionAvailable": False,
        }
        for key, expected in exact.items():
            if type(value.get(key)) is not type(expected) or value.get(key) != expected:
                raise KisDomesticFunctionalStateBlocked(f"{name} component {key} mismatch")
        expected_sessions = {
            "", str(row["session_id"]), str(row["pending_session_id"])
        }
        if value.get("sessionId") not in expected_sessions:
            raise KisDomesticFunctionalStateBlocked(f"{name} component session mismatch")
        if type(value.get("hazards")) is not list or any(
            type(item) is not str or not _HAZARD.fullmatch(item) for item in value["hazards"]
        ):
            raise KisDomesticFunctionalStateBlocked(f"{name} component hazards are invalid")
        if not isinstance(value.get("functionalMutationIntent"), Mapping):
            raise KisDomesticFunctionalStateBlocked(f"{name} mutation intent is invalid")
        if type(value.get("killOrdinaryCancelAllowed")) is not bool or type(value.get("killOrdinaryCancelRevision")) is not int or not isinstance(value.get("killOrdinaryCancelIntent"), Mapping):
            raise KisDomesticFunctionalStateBlocked(f"{name} Kill-cancel snapshot is invalid")
        return value

    def _verify_row(self, conn: sqlite3.Connection, row: sqlite3.Row) -> None:
        transitions = conn.execute(
            "SELECT * FROM kis_functional_state_transition WHERE route=? ORDER BY revision",
            (ROUTE,),
        ).fetchall()
        if len(transitions) != int(row["revision"]):
            raise KisDomesticFunctionalStateBlocked("state revision history is incomplete")
        previous_hash = "0" * 64
        previous_time = ""
        exact_body_keys = {
            "schemaVersion", "route", "revision", "phase", "sessionId",
            "pendingSessionId", "accountFingerprint",
            "credentialConfigurationHash", "ordinaryRoutesClosed",
            "ownerEpochId", "ownerEpochHash", "hazards", "reservationId",
            "reservationKind", "reservationRevision", "reservationBindingHash",
            "occurredAt",
            "previousHash", "signerKeyIdHash", "productionAvailable",
        }
        for expected_revision, transition in enumerate(transitions, start=1):
            try:
                body = json.loads(transition["body_json"])
            except json.JSONDecodeError:
                raise KisDomesticFunctionalStateBlocked("state transition history is malformed") from None
            if (
                int(transition["revision"]) != expected_revision
                or set(body) != exact_body_keys
                or transition["route"] != ROUTE
                or body.get("schemaVersion") != "kis-domestic-functional-state-transition/v1"
                or body.get("route") != ROUTE
                or body.get("revision") != expected_revision
                or body.get("phase") != transition["phase"]
                or body.get("reservationId") != transition["reservation_id"]
                or body.get("reservationKind") != transition["reservation_kind"]
                or body.get("occurredAt") != transition["occurred_at"]
                or body.get("previousHash") != previous_hash
                or transition["previous_hash"] != previous_hash
                or transition["signer_key_id_hash"] != self._key_id_hash
                or body.get("productionAvailable") is not False
                or _hash(body) != transition["body_hash"]
                or not hmac.compare_digest(
                    transition["signature_hash"],
                    hmac.new(
                        self._key,
                        ("KIS_STATE_TRANSITION\n" + transition["body_hash"]).encode(),
                        hashlib.sha256,
                    ).hexdigest(),
                )
                or previous_time and transition["occurred_at"] < previous_time
            ):
                raise KisDomesticFunctionalStateBlocked("state transition history failed integrity")
            previous_hash = transition["body_hash"]
            previous_time = transition["occurred_at"]
        if not transitions or int(row["revision"]) < 1:
            raise KisDomesticFunctionalStateBlocked("state signed INIT transition is absent")
        if transitions[-1]["phase"] != row["phase"]:
            raise KisDomesticFunctionalStateBlocked("state phase/history projection mismatch")
        if str(row["transition_head_hash"]) != previous_hash:
            raise KisDomesticFunctionalStateBlocked("state transition head mismatch")
        if transitions:
            tail = json.loads(transitions[-1]["body_json"])
            exact_projection = {
                "phase": str(row["phase"]), "revision": int(row["revision"]),
                "sessionId": str(row["session_id"]),
                "pendingSessionId": str(row["pending_session_id"]),
                "accountFingerprint": str(row["account_fingerprint"]),
                "credentialConfigurationHash": str(row["credential_configuration_hash"]),
                "ordinaryRoutesClosed": bool(row["ordinary_routes_closed"]),
                "ownerEpochId": str(row["owner_epoch_id"]),
                "ownerEpochHash": str(row["owner_epoch_hash"]),
                "hazards": json.loads(row["durable_hazards_json"]),
                "reservationId": str(row["reservation_id"]),
                "reservationKind": str(row["reservation_kind"]),
                "reservationRevision": int(row["reservation_revision"]),
                "reservationBindingHash": str(row["reservation_binding_hash"]),
                "occurredAt": str(row["updated_at"]),
                "previousHash": str(transitions[-1]["previous_hash"]),
                "signerKeyIdHash": self._key_id_hash,
            }
            for key, expected in exact_projection.items():
                if type(tail.get(key)) is not type(expected) or tail.get(key) != expected:
                    raise KisDomesticFunctionalStateBlocked(
                        f"state current row {key} projection mismatch"
                    )
        self._verify_durable_manager_evidence(conn, row)

    def _verify_durable_manager_evidence(
        self, conn: sqlite3.Connection, authority: sqlite3.Row
    ) -> None:
        bindings = conn.execute(
            "SELECT * FROM kis_functional_state_manager_binding "
            "WHERE route=? ORDER BY reservation_revision,reservation_id",
            (ROUTE,),
        ).fetchall()
        seen_ids: set[str] = set()
        for row in bindings:
            reservation_id = _identity(
                str(row["reservation_id"]), "durable reservation id"
            )
            if reservation_id in seen_ids:
                raise KisDomesticFunctionalStateBlocked(
                    "duplicate durable manager binding"
                )
            seen_ids.add(reservation_id)
            kind = str(row["reservation_kind"])
            if kind not in {"START", "STOP", "KILL", "SETTINGS"}:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager binding kind is invalid"
                )
            revision = int(row["reservation_revision"])
            if revision < 2:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager binding revision is invalid"
                )
            state_hash = _sha(
                str(row["state_component_readers_hash"]),
                "durable state component hash",
            )
            manager_hash = _sha(
                str(row["manager_component_readers_hash"]),
                "durable manager component hash",
            )
            binding_hash = _sha(
                str(row["manager_binding_hash"]),
                "durable manager binding hash",
            )
            boundary_handle = _sha(
                str(row["final_mutation_boundary_handle"]),
                "durable final boundary handle",
            )
            reservation_hash = _sha(
                str(row["reservation_binding_hash"]),
                "durable reservation binding hash",
            )
            try:
                binding = json.loads(str(row["manager_binding_json"]))
            except json.JSONDecodeError:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager binding JSON is malformed"
                ) from None
            if not isinstance(binding, Mapping):
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager binding JSON is invalid"
                )
            binding = dict(binding)
            if binding:
                unsigned = dict(binding)
                signature = unsigned.pop("signature", "")
                stored_binding_hash = unsigned.pop("bindingHash", "")
                if (
                    binding.get("schemaVersion") != _MANAGER_BINDING_SCHEMA
                    or binding.get("route") != ROUTE
                    or binding.get("pdno") != PDNO
                    or binding.get("reservationId") != reservation_id
                    or binding.get("reservationKind") != kind
                    or binding.get("reservationRevision") != revision
                    or binding.get("componentReadersHash") != manager_hash
                    or binding.get("stateComponentReadersHash") != state_hash
                    or binding.get("managerImplementationType")
                    != self._manager_implementation_type
                    or binding.get("managerCodeHash") != self._manager_code_hash
                    or binding.get("managerProtocolHash")
                    != self._manager_protocol_hash
                    or binding.get("managerKeyIdHash")
                    != self._manager_receipt_key_id_hash
                    or binding.get("receiptSchemaVersion")
                    != _MANAGER_RECEIPT_V2_SCHEMA
                    or binding.get("verifyOnly") is not True
                    or binding.get("productionAvailable") is not False
                    or not hmac.compare_digest(
                        str(stored_binding_hash), binding_hash
                    )
                    or not hmac.compare_digest(binding_hash, _hash(unsigned))
                    or type(signature) is not str
                    or not signature
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "durable manager binding integrity failed"
                    )
                try:
                    verified = self._manager_binding_verifier(dict(binding))
                except Exception as exc:
                    raise KisDomesticFunctionalStateBlocked(
                        "durable manager binding verifier failed"
                    ) from exc
                if type(verified) is not bool or verified is not True:
                    raise KisDomesticFunctionalStateBlocked(
                        "durable manager binding signature is unverified"
                    )
            elif (
                self._manager_v2_wired
                or binding_hash != "0" * 64
                or manager_hash != state_hash
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "legacy manager binding appeared in v2 state"
                )
            expected_reservation_hash = self._reservation_binding_hash(
                reservation_id=reservation_id,
                reservation_kind=kind,
                revision=revision,
                state_component_readers_hash=state_hash,
                manager_component_readers_hash=manager_hash,
                manager_binding_hash=binding_hash,
                final_mutation_boundary_handle=boundary_handle,
            )
            if not hmac.compare_digest(
                reservation_hash, expected_reservation_hash
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "durable reservation binding integrity failed"
                )

        transition_rows = conn.execute(
            "SELECT revision,body_json FROM kis_functional_state_transition "
            "WHERE route=? ORDER BY revision",
            (ROUTE,),
        ).fetchall()
        transition_bodies: list[dict[str, Any]] = []
        lifecycles: dict[str, dict[str, Any]] = {}
        previous_reservation_id = ""
        closed_reservation_ids: set[str] = set()
        for transition_row in transition_rows:
            try:
                transition_body = json.loads(
                    str(transition_row["body_json"])
                )
            except json.JSONDecodeError:
                raise KisDomesticFunctionalStateBlocked(
                    "state reservation lifecycle history is malformed"
                ) from None
            transition_bodies.append(transition_body)
            reservation_id = str(transition_body["reservationId"])
            if not reservation_id:
                if previous_reservation_id:
                    closed_reservation_ids.add(previous_reservation_id)
                previous_reservation_id = ""
                continue
            if (
                previous_reservation_id
                and previous_reservation_id != reservation_id
            ):
                closed_reservation_ids.add(previous_reservation_id)
            if reservation_id in closed_reservation_ids:
                raise KisDomesticFunctionalStateBlocked(
                    "state reservation lifecycle is non-contiguous"
                )
            kind = str(transition_body["reservationKind"])
            reservation_revision = int(
                transition_body["reservationRevision"]
            )
            lifecycle = lifecycles.get(reservation_id)
            if lifecycle is None:
                if reservation_revision != int(transition_row["revision"]):
                    raise KisDomesticFunctionalStateBlocked(
                        "state reservation birth revision mismatch"
                    )
                lifecycle = {
                    "kind": kind,
                    "reservationRevision": reservation_revision,
                    "transitionRevisions": [],
                    "lastTransitionIndex": -1,
                }
                lifecycles[reservation_id] = lifecycle
            elif (
                lifecycle["kind"] != kind
                or lifecycle["reservationRevision"]
                != reservation_revision
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "state reservation lifecycle identity changed"
                )
            lifecycle["transitionRevisions"].append(
                int(transition_row["revision"])
            )
            lifecycle["lastTransitionIndex"] = len(transition_bodies) - 1
            previous_reservation_id = reservation_id
        if set(lifecycles) != seen_ids:
            raise KisDomesticFunctionalStateBlocked(
                "signed reservation/binding cardinality mismatch"
            )
        binding_by_id = {
            str(row["reservation_id"]): row for row in bindings
        }
        for reservation_id, lifecycle in lifecycles.items():
            binding = binding_by_id[reservation_id]
            if (
                str(binding["reservation_kind"]) != lifecycle["kind"]
                or int(binding["reservation_revision"])
                != lifecycle["reservationRevision"]
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "signed reservation/binding lifecycle mismatch"
                )

        current_reservation_id = str(authority["reservation_id"])
        if current_reservation_id:
            if (
                current_reservation_id not in seen_ids
                or str(authority["reservation_binding_hash"])
                not in {
                    str(row["reservation_binding_hash"]) for row in bindings
                    if str(row["reservation_id"]) == current_reservation_id
                }
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "active reservation lacks its immutable manager binding"
                )
        elif str(authority["reservation_binding_hash"]):
            raise KisDomesticFunctionalStateBlocked(
                "cleared reservation retains a manager binding projection"
            )

        receipts = conn.execute(
            "SELECT * FROM kis_functional_state_manager_receipt "
            "WHERE route=? ORDER BY state_transition_revision,reservation_id",
            (ROUTE,),
        ).fetchall()
        receipts_by_id: dict[str, list[sqlite3.Row]] = {
            reservation_id: [] for reservation_id in lifecycles
        }
        for receipt in receipts:
            receipts_by_id.setdefault(
                str(receipt["reservation_id"]), []
            ).append(receipt)
        envelope_keys = {
            "schemaVersion", "route", "reservationId", "reservationKind",
            "reservationRevision", "sessionId", "accountFingerprint",
            "credentialConfigurationHash", "ownerEpochHash",
            "componentReadersHash", "managerReceiptHash",
            "executionProofHash", "mutationPlanHash", "ownedProjectionHash",
            "ownedProjectionHeadHash", "boundaryEntryProofHash",
            "attemptChainHead", "transportReceiptSetHash",
            "detachedBoundaryHazard", "pendingReservation",
            "reservationFinishAllowed", "reconciliationRequired", "ok",
            "mutationMayHaveOccurred", "occurredAt", "keyIdHash",
            "productionAvailable", "receiptHash", "signature",
        }
        for receipt in receipts:
            reservation_id = str(receipt["reservation_id"])
            if reservation_id not in seen_ids:
                raise KisDomesticFunctionalStateBlocked(
                    "manager receipt has no immutable reservation binding"
                )
            try:
                envelope = json.loads(str(receipt["receipt_json"]))
            except json.JSONDecodeError:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager receipt JSON is malformed"
                ) from None
            if not isinstance(envelope, Mapping) or set(envelope) != envelope_keys:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager receipt shape is not exact"
                )
            envelope = dict(envelope)
            unsigned = dict(envelope)
            signature = unsigned.pop("signature")
            receipt_hash = unsigned.pop("receiptHash")
            kind = str(receipt["receipt_kind"])
            expected_columns = {
                "reservationId": reservation_id,
                "reservationKind": str(receipt["reservation_kind"]),
                "reservationRevision": int(receipt["reservation_revision"]),
                "managerReceiptHash": str(receipt["manager_receipt_hash"]),
                "executionProofHash": str(receipt["execution_proof_hash"]),
                "mutationPlanHash": str(receipt["mutation_plan_hash"]),
                "ownedProjectionHash": str(receipt["owned_projection_hash"]),
                "ownedProjectionHeadHash": str(
                    receipt["owned_projection_head_hash"]
                ),
                "boundaryEntryProofHash": str(
                    receipt["boundary_entry_proof_hash"]
                ),
                "attemptChainHead": str(receipt["attempt_chain_head"]),
                "transportReceiptSetHash": str(
                    receipt["transport_receipt_set_hash"]
                ),
                "receiptHash": str(receipt["receipt_hash"]),
                "signature": str(receipt["signature"]),
                "keyIdHash": str(receipt["key_id_hash"]),
                "occurredAt": str(receipt["occurred_at"]),
            }
            if (
                envelope.get("schemaVersion") != _MANAGER_RECEIPT_V2_SCHEMA
                or envelope.get("route") != ROUTE
                or envelope.get("productionAvailable") is not False
                or any(envelope.get(key) != value for key, value in expected_columns.items())
                or not hmac.compare_digest(str(receipt_hash), _hash(unsigned))
                or kind not in {"FINISH", "PENDING"}
                or (
                    kind == "FINISH"
                    and (
                        envelope.get("pendingReservation") is not False
                        or envelope.get("reservationFinishAllowed") is not True
                        or envelope.get("detachedBoundaryHazard") is not False
                    )
                )
                or (
                    kind == "PENDING"
                    and (
                        envelope.get("pendingReservation") is not True
                        or envelope.get("reservationFinishAllowed") is not False
                        or envelope.get("detachedBoundaryHazard") is not True
                        or envelope.get("reconciliationRequired") is not True
                        or envelope.get("ok") is not False
                    )
                )
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager receipt integrity failed"
                )
            transition = conn.execute(
                "SELECT body_json FROM kis_functional_state_transition "
                "WHERE route=? AND revision=?",
                (ROUTE, int(receipt["state_transition_revision"])),
            ).fetchone()
            if transition is None:
                raise KisDomesticFunctionalStateBlocked(
                    "manager receipt state transition is absent"
                )
            transition_body = json.loads(str(transition["body_json"]))
            if kind == "PENDING" and (
                transition_body.get("phase") != "RECONCILIATION_REQUIRED"
                or transition_body.get("reservationId") != reservation_id
                or transition_body.get("ordinaryRoutesClosed") is not True
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "pending manager receipt can reopen the route"
                )
            try:
                verified = self._manager_receipt_verifier(dict(envelope))
            except Exception as exc:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager receipt verifier failed"
                ) from exc
            if type(verified) is not bool or verified is not True:
                raise KisDomesticFunctionalStateBlocked(
                    "durable manager receipt signature is unverified"
                )

        if self._manager_v2_wired:
            for reservation_id, lifecycle in lifecycles.items():
                lifecycle_receipts = receipts_by_id.get(reservation_id, [])
                if len(lifecycle_receipts) > 1:
                    raise KisDomesticFunctionalStateBlocked(
                        "manager reservation has multiple terminal receipts"
                    )
                last_index = int(lifecycle["lastTransitionIndex"])
                next_body = (
                    transition_bodies[last_index + 1]
                    if last_index + 1 < len(transition_bodies)
                    else None
                )
                current = (
                    reservation_id == str(authority["reservation_id"])
                )
                if next_body is None:
                    if not current:
                        raise KisDomesticFunctionalStateBlocked(
                            "closed reservation lacks a terminal transition"
                        )
                    if not lifecycle_receipts:
                        # The only zero-receipt lifecycle is the exact current
                        # crash/in-flight reservation.  It remains route-closed
                        # and cannot be mistaken for completion.
                        if (
                            len(lifecycle["transitionRevisions"]) != 1
                            or
                            not bool(authority["ordinary_routes_closed"])
                            or str(authority["reservation_binding_hash"])
                            != str(
                                binding_by_id[reservation_id][
                                    "reservation_binding_hash"
                                ]
                            )
                        ):
                            raise KisDomesticFunctionalStateBlocked(
                                "zero-receipt reservation is not fail-closed"
                            )
                        continue
                    receipt = lifecycle_receipts[0]
                    if (
                        str(receipt["receipt_kind"]) != "PENDING"
                        or int(receipt["state_transition_revision"])
                        != int(lifecycle["transitionRevisions"][-1])
                    ):
                        raise KisDomesticFunctionalStateBlocked(
                            "current reservation receipt is not exact PENDING"
                        )
                    continue
                next_reservation_id = str(next_body["reservationId"])
                if not lifecycle_receipts:
                    raise KisDomesticFunctionalStateBlocked(
                        "closed reservation terminal receipt is missing"
                    )
                receipt = lifecycle_receipts[0]
                expected_receipt_kind = (
                    "FINISH" if not next_reservation_id else "PENDING"
                )
                if (
                    str(receipt["receipt_kind"])
                    != expected_receipt_kind
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "closed reservation terminal receipt kind mismatch"
                    )
                if expected_receipt_kind == "FINISH":
                    expected_transition_revision = int(
                        next_body["revision"]
                    )
                else:
                    if (
                        str(next_body["reservationKind"]) != "KILL"
                        or int(receipt["state_transition_revision"])
                        not in lifecycle["transitionRevisions"]
                    ):
                        raise KisDomesticFunctionalStateBlocked(
                            "reservation replacement is not pending-to-Kill"
                        )
                    expected_transition_revision = int(
                        receipt["state_transition_revision"]
                    )
                if int(receipt["state_transition_revision"]) != expected_transition_revision:
                    raise KisDomesticFunctionalStateBlocked(
                        "manager receipt/state lifecycle revision mismatch"
                    )

    def authority_snapshot(self) -> dict[str, Any]:
        with kis_route_authority_serialization():
            with self.ledger.connection() as conn:
                row = conn.execute("SELECT * FROM kis_functional_state_authority WHERE route=?", (ROUTE,)).fetchone()
                if row is None:
                    raise KisDomesticFunctionalStateBlocked("durable state authority is absent")
                self._verify_row(conn, row)
                try:
                    durable_hazards = json.loads(row["durable_hazards_json"])
                except json.JSONDecodeError:
                    raise KisDomesticFunctionalStateBlocked("durable hazard journal is invalid") from None
                if type(durable_hazards) is not list or any(type(item) is not str or not _HAZARD.fullmatch(item) for item in durable_hazards):
                    raise KisDomesticFunctionalStateBlocked("durable hazards are invalid")
                owner_epoch = self._matching_owner_epoch(row)
                components = {name: self._component(name, row) for name in _COMPONENTS}
                receipt_row = conn.execute(
                    """SELECT receipt_kind,receipt_hash,manager_receipt_hash,
                              execution_proof_hash,state_transition_revision
                       FROM kis_functional_state_manager_receipt
                       WHERE route=? AND reservation_id=?
                       ORDER BY state_transition_revision DESC LIMIT 1""",
                    (ROUTE, str(row["reservation_id"])),
                ).fetchone()
        hazards = sorted(set(durable_hazards).union(*(value["hazards"] for value in components.values())))
        graph = components["graph"]
        phase = str(row["phase"])
        open_authority = phase in _OPEN_PHASES or bool(row["reservation_id"]) or bool(hazards)
        control_reservation = (
            {
                "reservationId": str(row["reservation_id"]),
                "reservationKind": str(row["reservation_kind"]),
                "reservationRevision": int(row["reservation_revision"]),
                "stateRevision": int(row["revision"]),
                "phase": phase,
                "reservationBindingHash": str(
                    row["reservation_binding_hash"]
                ),
            }
            if str(row["reservation_id"])
            else {}
        )
        return {
            "durableAuthorityReadable": True,
            "functionalAuthorityOpen": open_authority,
            "functionalPhase": phase,
            "functionalRevision": int(row["revision"]),
            "stateRevision": int(row["revision"]),
            "functionalSessionId": str(row["session_id"]),
            "functionalAccountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "functionalMutationIntent": dict(graph["functionalMutationIntent"]),
            "killOrdinaryCancelAllowed": graph["killOrdinaryCancelAllowed"],
            "killOrdinaryCancelRevision": graph["killOrdinaryCancelRevision"],
            "killOrdinaryCancelIntent": dict(graph["killOrdinaryCancelIntent"]),
            "applicationInstanceLeaseHeld": owner_epoch["applicationLeaseHeld"],
            "ownerEpochId": str(row["owner_epoch_id"]),
            "ownerEpochHash": str(row["owner_epoch_hash"]),
            "ordinaryRoutesClosed": bool(row["ordinary_routes_closed"]) or open_authority,
            "hazards": hazards,
            "reservationId": str(row["reservation_id"]),
            "reservationKind": str(row["reservation_kind"]),
            "controlReservation": control_reservation,
            "reservationBindingHash": str(row["reservation_binding_hash"]),
            "managerReceiptKind": (
                "" if receipt_row is None else str(receipt_row["receipt_kind"])
            ),
            "managerReceiptHash": (
                "" if receipt_row is None
                else str(receipt_row["manager_receipt_hash"])
            ),
            "managerExecutionProofHash": (
                "" if receipt_row is None
                else str(receipt_row["execution_proof_hash"])
            ),
            "stateReceiptV2IntegrationWired": self._manager_v2_wired,
            "ownerHash": self._owner_hash,
            "componentOwnerHashes": dict(self._component_owner_hashes),
            "productionAvailable": False,
        }

    def _transition(self, conn: sqlite3.Connection, *, row: sqlite3.Row, phase: str, reservation_id: str, reservation_kind: str, session_id: str, pending_session_id: str, account: str, credential: str, routes_closed: bool, hazards: list[str], reservation_binding_hash: str = "", occurred_at: str | None = None) -> int:
        revision = int(row["revision"]) + 1
        now = occurred_at or self._now()
        _utc(
            datetime.fromisoformat(now.replace("Z", "+00:00")),
            "state transition time",
        )
        if now < str(row["updated_at"]):
            raise KisDomesticFunctionalStateBlocked(
                "state clock moved backwards before transition CAS"
            )
        previous_hash = str(row["transition_head_hash"])
        reservation_revision = (
            int(row["reservation_revision"])
            if (
                reservation_id
                and reservation_id == str(row["reservation_id"])
                and reservation_kind == str(row["reservation_kind"])
            )
            else revision if reservation_id else 0
        )
        if reservation_id:
            _sha(reservation_binding_hash, "reservation binding hash")
        elif reservation_binding_hash:
            raise KisDomesticFunctionalStateBlocked(
                "cleared reservation cannot retain a binding hash"
            )
        body = {
            "schemaVersion": "kis-domestic-functional-state-transition/v1",
            "route": ROUTE, "revision": revision, "phase": phase,
            "sessionId": session_id, "pendingSessionId": pending_session_id,
            "accountFingerprint": account, "credentialConfigurationHash": credential,
            "ordinaryRoutesClosed": routes_closed,
            "ownerEpochId": str(row["owner_epoch_id"]),
            "ownerEpochHash": str(row["owner_epoch_hash"]),
            "hazards": sorted(set(hazards)),
            "reservationId": reservation_id, "reservationKind": reservation_kind,
            "reservationRevision": reservation_revision,
            "reservationBindingHash": reservation_binding_hash,
            "occurredAt": now, "previousHash": previous_hash,
            "signerKeyIdHash": self._key_id_hash, "productionAvailable": False,
        }
        body_hash = _hash(body)
        signature = hmac.new(
            self._key, ("KIS_STATE_TRANSITION\n" + body_hash).encode(), hashlib.sha256
        ).hexdigest()
        changed = conn.execute(
            """UPDATE kis_functional_state_authority SET phase=?,revision=?,session_id=?,
               pending_session_id=?,account_fingerprint=?,credential_configuration_hash=?,
               ordinary_routes_closed=?,owner_epoch_id=?,owner_epoch_hash=?,
               durable_hazards_json=?,reservation_id=?,
               reservation_kind=?,reservation_revision=?,reservation_binding_hash=?,
               transition_head_hash=?,updated_at=?
               WHERE route=? AND revision=?""",
            (phase, revision, session_id, pending_session_id, account, credential,
             int(routes_closed), str(row["owner_epoch_id"]), str(row["owner_epoch_hash"]),
             _canonical(sorted(set(hazards))), reservation_id,
             reservation_kind, reservation_revision, reservation_binding_hash,
             body_hash, now, ROUTE,
             int(row["revision"])),
        ).rowcount
        if changed != 1:
            raise KisDomesticFunctionalStateBlocked("state transition CAS failed")
        conn.execute(
            "INSERT INTO kis_functional_state_transition VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (ROUTE, revision, phase, reservation_id, reservation_kind, now,
             _canonical(body), body_hash, previous_hash, signature, self._key_id_hash),
        )
        return revision

    def _manager_result(
        self, result: Any, *, reservation: Mapping[str, Any],
        component_readers_hash: str,
    ) -> dict[str, Any]:
        expected_keys = {
            "schemaVersion", "route", "reservationId", "reservationKind",
            "reservationRevision", "sessionId", "accountFingerprint",
            "credentialConfigurationHash", "ownerEpochHash",
            "componentReadersHash", "ok", "mutationMayHaveOccurred",
            "occurredAt", "keyIdHash", "productionAvailable", "receiptHash",
            "signature",
        }
        if not isinstance(result, Mapping) or set(result) != expected_keys:
            raise KisDomesticFunctionalStateBlocked("manager result is not exact")
        value = dict(result)
        expected = {
            "schemaVersion": _MANAGER_RECEIPT_V1_SCHEMA, "route": ROUTE,
            "reservationId": reservation["reservationId"],
            "reservationKind": reservation["reservationKind"],
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation["reservedCredentialConfigurationHash"],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "componentReadersHash": component_readers_hash,
            "keyIdHash": self._manager_receipt_key_id_hash,
            "productionAvailable": False,
        }
        if any(type(value.get(key)) is not type(wanted) or value[key] != wanted for key, wanted in expected.items()):
            raise KisDomesticFunctionalStateBlocked("manager result binding mismatch")
        if type(value["ok"]) is not bool or type(value["mutationMayHaveOccurred"]) is not bool or type(value["receiptHash"]) is not str or not _SHA.fullmatch(value["receiptHash"]) or type(value["signature"]) is not str or not _SHA.fullmatch(value["signature"]):
            raise KisDomesticFunctionalStateBlocked("manager result is invalid")
        occurred = _utc(
            datetime.fromisoformat(value["occurredAt"].replace("Z", "+00:00")),
            "manager receipt time",
        )
        if occurred < reservation["reservedAt"] or occurred > self._now():
            raise KisDomesticFunctionalStateBlocked("manager receipt time is out of bounds")
        unsigned = dict(value)
        unsigned.pop("signature")
        receipt_hash = unsigned.pop("receiptHash")
        if not hmac.compare_digest(receipt_hash, _hash(unsigned)):
            raise KisDomesticFunctionalStateBlocked("manager receipt body hash mismatch")
        try:
            verified = self._manager_receipt_verifier(dict(value))
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked("manager receipt verifier failed") from exc
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalStateBlocked("manager receipt signature is unverified")
        return value

    def _manager_v2_result(
        self,
        result: Any,
        *,
        reservation: Mapping[str, Any],
        binding_record: Mapping[str, Any],
    ) -> dict[str, Any]:
        result_keys = {
            "receipt", "stateManagerReceipt", "pendingReservationProof",
            "boundaryEntryProof", "attemptProofs", "transportReceipts",
        }
        if not isinstance(result, Mapping) or set(result) != result_keys:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 execution result is not exact"
            )
        detailed_raw = result["receipt"]
        boundary_raw = result["boundaryEntryProof"]
        attempts_raw = result["attemptProofs"]
        transports_raw = result["transportReceipts"]
        if (
            not isinstance(detailed_raw, Mapping)
            or not isinstance(boundary_raw, Mapping)
            or type(attempts_raw) is not tuple
            or type(transports_raw) is not tuple
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 proof collection is invalid"
            )
        finish_raw = result["stateManagerReceipt"]
        pending_raw = result["pendingReservationProof"]
        if (finish_raw is None) == (pending_raw is None):
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 finish/pending receipt cardinality is invalid"
            )
        receipt_kind = "FINISH" if finish_raw is not None else "PENDING"
        envelope_raw = finish_raw if finish_raw is not None else pending_raw
        if not isinstance(envelope_raw, Mapping):
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 state receipt is invalid"
            )
        envelope_keys = {
            "schemaVersion", "route", "reservationId", "reservationKind",
            "reservationRevision", "sessionId", "accountFingerprint",
            "credentialConfigurationHash", "ownerEpochHash",
            "componentReadersHash", "managerReceiptHash",
            "executionProofHash", "mutationPlanHash", "ownedProjectionHash",
            "ownedProjectionHeadHash", "boundaryEntryProofHash",
            "attemptChainHead", "transportReceiptSetHash",
            "detachedBoundaryHazard", "pendingReservation",
            "reservationFinishAllowed", "reconciliationRequired", "ok",
            "mutationMayHaveOccurred", "occurredAt", "keyIdHash",
            "productionAvailable", "receiptHash", "signature",
        }
        envelope = dict(envelope_raw)
        if set(envelope) != envelope_keys:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 state receipt shape is not exact"
            )
        binding = binding_record.get("managerBinding")
        if not isinstance(binding, Mapping) or not binding:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 durable binding is absent"
            )
        binding = dict(binding)
        expected_envelope = {
            "schemaVersion": _MANAGER_RECEIPT_V2_SCHEMA,
            "route": ROUTE,
            "reservationId": reservation["reservationId"],
            "reservationKind": reservation["reservationKind"],
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation[
                "reservedCredentialConfigurationHash"
            ],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "componentReadersHash": reservation["componentReadersHash"],
            "managerReceiptHash": dict(detailed_raw).get("receiptHash"),
            "mutationPlanHash": binding["mutationPlanHash"],
            "ownedProjectionHash": binding["ownedProjectionHash"],
            "ownedProjectionHeadHash": binding["ownedProjectionHeadHash"],
            "keyIdHash": self._manager_receipt_key_id_hash,
            "productionAvailable": False,
        }
        for key, wanted in expected_envelope.items():
            if type(envelope.get(key)) is not type(wanted) or envelope.get(key) != wanted:
                raise KisDomesticFunctionalStateBlocked(
                    f"manager v2 state receipt {key} mismatch"
                )
        for key in (
            "managerReceiptHash", "executionProofHash", "mutationPlanHash",
            "ownedProjectionHash", "ownedProjectionHeadHash",
            "boundaryEntryProofHash", "attemptChainHead",
            "transportReceiptSetHash", "keyIdHash", "receiptHash",
            "signature",
        ):
            _sha(envelope[key], f"manager v2 receipt {key}")
        for key in (
            "detachedBoundaryHazard", "pendingReservation",
            "reservationFinishAllowed", "reconciliationRequired", "ok",
            "mutationMayHaveOccurred",
        ):
            _verified_bool(envelope[key], f"manager v2 receipt {key}")
        if receipt_kind == "FINISH":
            if (
                envelope["pendingReservation"] is not False
                or envelope["reservationFinishAllowed"] is not True
                or envelope["detachedBoundaryHazard"] is not False
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "finish receipt retains a detached reservation"
                )
        elif (
            envelope["pendingReservation"] is not True
            or envelope["reservationFinishAllowed"] is not False
            or envelope["detachedBoundaryHazard"] is not True
            or envelope["reconciliationRequired"] is not True
            or envelope["ok"] is not False
        ):
            raise KisDomesticFunctionalStateBlocked(
                "pending receipt can finish or reopen the reservation"
            )
        occurred = _utc(
            datetime.fromisoformat(
                envelope["occurredAt"].replace("Z", "+00:00")
            ),
            "manager v2 receipt time",
        )
        if occurred < reservation["reservedAt"] or occurred > self._now():
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 receipt time is out of bounds"
            )
        unsigned_envelope = dict(envelope)
        unsigned_envelope.pop("signature")
        envelope_hash = unsigned_envelope.pop("receiptHash")
        if not hmac.compare_digest(envelope_hash, _hash(unsigned_envelope)):
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 state receipt body hash mismatch"
            )
        try:
            verified = self._manager_receipt_verifier(dict(envelope))
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 receipt verifier failed"
            ) from exc
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 receipt signature is unverified"
            )

        detailed_keys = {
            "schemaVersion", "route", "pdno", "managerIdHash", "command",
            "reservationId", "reservationRevision", "sessionId",
            "ownerEpochId", "ownerEpochHash", "accountFingerprint",
            "credentialConfigurationHash", "componentBindingsHash",
            "componentJoinHash", "mutationPlanHash", "ownedProjectionHash",
            "ownedProjectionHeadHash", "authoritativePlanVerified",
            "boundaryEntryProofHash", "attemptCount", "attemptChainHead",
            "transportReceiptHashes", "transportReceiptSetHash",
            "executionProofHash", "cleanupExactOwned", "ok",
            "mutationMayHaveOccurred", "reconciliationRequired",
            "detachedMutationHazard", "detachedBoundaryHazard",
            "clockDiscontinuityHazard", "boundaryReleaseObserved",
            "operationDeadlineComplete", "pendingReservation",
            "reservationFinishAllowed", "durablePendingReservationRequired",
            "failureCode", "startedAt", "finishedAt",
            "elapsedMonotonicSeconds", "signerKeyIdHash",
            "productionAvailable", "networkAvailable",
            "releaseEvidenceAvailable", "receiptHash", "signature",
        }
        detailed = dict(detailed_raw)
        if set(detailed) != detailed_keys:
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed receipt shape is not exact"
            )
        expected_detailed = {
            "schemaVersion": _MANAGER_RECEIPT_V2_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "managerIdHash": binding["managerIdHash"],
            "command": reservation["reservationKind"],
            "reservationId": reservation["reservationId"],
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "ownerEpochId": reservation["ownerEpochId"],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation[
                "reservedCredentialConfigurationHash"
            ],
            "componentBindingsHash": binding[
                "managerComponentBindingsHash"
            ],
            "componentJoinHash": reservation["componentReadersHash"],
            "mutationPlanHash": envelope["mutationPlanHash"],
            "ownedProjectionHash": envelope["ownedProjectionHash"],
            "ownedProjectionHeadHash": envelope[
                "ownedProjectionHeadHash"
            ],
            "boundaryEntryProofHash": envelope["boundaryEntryProofHash"],
            "attemptChainHead": envelope["attemptChainHead"],
            "transportReceiptSetHash": envelope[
                "transportReceiptSetHash"
            ],
            "executionProofHash": envelope["executionProofHash"],
            "ok": envelope["ok"],
            "mutationMayHaveOccurred": envelope["mutationMayHaveOccurred"],
            "reconciliationRequired": envelope["reconciliationRequired"],
            "detachedBoundaryHazard": envelope[
                "detachedBoundaryHazard"
            ],
            "pendingReservation": envelope["pendingReservation"],
            "reservationFinishAllowed": envelope[
                "reservationFinishAllowed"
            ],
            "finishedAt": envelope["occurredAt"],
            "signerKeyIdHash": self._manager_receipt_key_id_hash,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseEvidenceAvailable": False,
            "receiptHash": envelope["managerReceiptHash"],
        }
        for key, wanted in expected_detailed.items():
            if type(detailed.get(key)) is not type(wanted) or detailed.get(key) != wanted:
                raise KisDomesticFunctionalStateBlocked(
                    f"manager detailed receipt {key} mismatch"
                )
        unsigned_detailed = dict(detailed)
        detailed_signature = unsigned_detailed.pop("signature")
        detailed_hash = unsigned_detailed.pop("receiptHash")
        _sha(detailed_signature, "manager detailed receipt signature")
        if not hmac.compare_digest(detailed_hash, _hash(unsigned_detailed)):
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed receipt body hash mismatch"
            )
        try:
            detailed_verified = self._manager_receipt_verifier(dict(detailed))
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed receipt verifier failed"
            ) from exc
        if type(detailed_verified) is not bool or detailed_verified is not True:
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed receipt signature is unverified"
            )
        if type(detailed["attemptCount"]) is not int or detailed["attemptCount"] < 0:
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed attempt count is invalid"
            )
        for key in (
            "authoritativePlanVerified", "cleanupExactOwned",
            "detachedMutationHazard", "clockDiscontinuityHazard",
            "boundaryReleaseObserved", "operationDeadlineComplete",
            "durablePendingReservationRequired",
        ):
            _verified_bool(detailed[key], f"manager detailed {key}")
        if (
            detailed["durablePendingReservationRequired"]
            is not envelope["pendingReservation"]
            or detailed["operationDeadlineComplete"]
            is envelope["pendingReservation"]
            or type(detailed["failureCode"]) is not str
            or type(detailed["elapsedMonotonicSeconds"]) not in {int, float}
            or detailed["elapsedMonotonicSeconds"] < 0
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager detailed deadline/pending proof is inconsistent"
            )
        _utc(
            datetime.fromisoformat(
                detailed["startedAt"].replace("Z", "+00:00")
            ),
            "manager detailed start time",
        )
        _utc(
            datetime.fromisoformat(
                detailed["finishedAt"].replace("Z", "+00:00")
            ),
            "manager detailed finish time",
        )

        boundary = dict(boundary_raw)
        zero = "0" * 64
        if boundary:
            boundary_keys = {
                "schemaVersion", "route", "pdno", "command",
                "reservationId", "reservationRevision", "sessionId",
                "ownerEpochId", "ownerEpochHash", "componentBindingsHash",
                "componentJoinHash", "mutationPlanHash",
                "ownedProjectionHash", "ownedProjectionHeadHash",
                "finalMutationBoundaryHandle", "routeLockHeld",
                "productionAvailable", "boundaryEntryProofHash",
            }
            if set(boundary) != boundary_keys:
                raise KisDomesticFunctionalStateBlocked(
                    "manager boundary proof is not exact"
                )
            expected_boundary = {
                "schemaVersion":
                    "kis-domestic-functional-manager-boundary-entry/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "command": reservation["reservationKind"],
                "reservationId": reservation["reservationId"],
                "reservationRevision": reservation["revision"],
                "sessionId": reservation["sessionId"],
                "ownerEpochId": reservation["ownerEpochId"],
                "ownerEpochHash": reservation["ownerEpochHash"],
                "componentBindingsHash": binding[
                    "managerComponentBindingsHash"
                ],
                "componentJoinHash": reservation["componentReadersHash"],
                "mutationPlanHash": binding["mutationPlanHash"],
                "ownedProjectionHash": binding["ownedProjectionHash"],
                "ownedProjectionHeadHash": binding[
                    "ownedProjectionHeadHash"
                ],
                "finalMutationBoundaryHandle": reservation[
                    "finalMutationBoundaryHandle"
                ],
                "routeLockHeld": True,
                "productionAvailable": False,
            }
            proof_hash = boundary.pop("boundaryEntryProofHash")
            if boundary != expected_boundary or not hmac.compare_digest(
                proof_hash, _hash(boundary)
            ):
                raise KisDomesticFunctionalStateBlocked(
                    "manager final mutation boundary proof mismatch"
                )
            boundary = {**boundary, "boundaryEntryProofHash": proof_hash}
        elif envelope["boundaryEntryProofHash"] != zero:
            raise KisDomesticFunctionalStateBlocked(
                "manager boundary proof body is absent"
            )
        if (
            str(boundary.get("boundaryEntryProofHash") or zero)
            != envelope["boundaryEntryProofHash"]
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager boundary proof hash mismatch"
            )
        if envelope["ok"] and (
            not boundary or detailed["boundaryReleaseObserved"] is not True
        ):
            raise KisDomesticFunctionalStateBlocked(
                "successful manager receipt lacks released final boundary proof"
            )

        attempts: list[dict[str, Any]] = []
        previous_attempt = zero
        attempt_keys = {
            "schemaVersion", "route", "pdno", "reservationId",
            "reservationRevision", "sessionId", "attemptIndex", "operation",
            "claimId", "requestHash", "boundaryEntryProofHash",
            "previousAttemptProofHash", "productionAvailable",
            "attemptProofHash",
        }
        for index, raw_attempt in enumerate(attempts_raw, 1):
            if not isinstance(raw_attempt, Mapping):
                raise KisDomesticFunctionalStateBlocked(
                    "manager attempt proof is not a mapping"
                )
            attempt = dict(raw_attempt)
            if set(attempt) != attempt_keys:
                raise KisDomesticFunctionalStateBlocked(
                    "manager attempt proof is not exact"
                )
            expected_attempt = {
                "schemaVersion":
                    "kis-domestic-functional-manager-attempt/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "reservationId": reservation["reservationId"],
                "reservationRevision": reservation["revision"],
                "sessionId": reservation["sessionId"],
                "attemptIndex": index,
                "boundaryEntryProofHash": envelope[
                    "boundaryEntryProofHash"
                ],
                "previousAttemptProofHash": previous_attempt,
                "productionAvailable": False,
            }
            if any(attempt.get(key) != wanted for key, wanted in expected_attempt.items()):
                raise KisDomesticFunctionalStateBlocked(
                    "manager attempt chain binding mismatch"
                )
            unsigned_attempt = dict(attempt)
            attempt_hash = unsigned_attempt.pop("attemptProofHash")
            if not hmac.compare_digest(attempt_hash, _hash(unsigned_attempt)):
                raise KisDomesticFunctionalStateBlocked(
                    "manager attempt proof hash mismatch"
                )
            previous_attempt = attempt_hash
            attempts.append(attempt)
        if (
            len(attempts) != detailed["attemptCount"]
            or previous_attempt != envelope["attemptChainHead"]
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager attempt chain head/count mismatch"
            )

        transports: list[dict[str, Any]] = []
        transport_keys = {
            "schemaVersion", "route", "pdno", "operation", "claimId",
            "requestHash", "attemptProofHash", "status",
            "mutationMayHaveOccurred", "occurredAt", "signerKeyIdHash",
            "productionAvailable", "networkAvailable", "receiptHash",
            "signature",
        }
        for index, raw_transport in enumerate(transports_raw):
            if not isinstance(raw_transport, Mapping):
                raise KisDomesticFunctionalStateBlocked(
                    "manager transport receipt is not a mapping"
                )
            transport = dict(raw_transport)
            if set(transport) != transport_keys:
                raise KisDomesticFunctionalStateBlocked(
                    "manager transport receipt is not exact"
                )
            if index >= len(attempts):
                raise KisDomesticFunctionalStateBlocked(
                    "manager transport receipt has no attempt"
                )
            attempt = attempts[index]
            expected_transport = {
                "route": ROUTE,
                "pdno": PDNO,
                "operation": attempt["operation"],
                "claimId": attempt["claimId"],
                "requestHash": attempt["requestHash"],
                "attemptProofHash": attempt["attemptProofHash"],
                "productionAvailable": False,
                "networkAvailable": False,
            }
            if any(transport.get(key) != wanted for key, wanted in expected_transport.items()):
                raise KisDomesticFunctionalStateBlocked(
                    "manager transport/attempt binding mismatch"
                )
            unsigned_transport = dict(transport)
            unsigned_transport.pop("signature")
            transport_hash = unsigned_transport.pop("receiptHash")
            if not hmac.compare_digest(transport_hash, _hash(unsigned_transport)):
                raise KisDomesticFunctionalStateBlocked(
                    "manager transport receipt body hash mismatch"
                )
            transports.append(transport)
        transport_hashes = [item["receiptHash"] for item in transports]
        if (
            detailed["transportReceiptHashes"] != transport_hashes
            or not hmac.compare_digest(
                envelope["transportReceiptSetHash"], _hash(transport_hashes)
            )
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager transport receipt set hash mismatch"
            )
        execution_body = {
            "schemaVersion":
                "kis-domestic-functional-manager-execution-proof/v1",
            "route": ROUTE,
            "reservationId": reservation["reservationId"],
            "reservationRevision": reservation["revision"],
            "mutationPlanHash": envelope["mutationPlanHash"],
            "ownedProjectionHash": envelope["ownedProjectionHash"],
            "ownedProjectionHeadHash": envelope[
                "ownedProjectionHeadHash"
            ],
            "boundaryEntryProofHash": envelope["boundaryEntryProofHash"],
            "attemptCount": detailed["attemptCount"],
            "attemptChainHead": envelope["attemptChainHead"],
            "transportReceiptSetHash": envelope[
                "transportReceiptSetHash"
            ],
            "detachedBoundaryHazard": envelope[
                "detachedBoundaryHazard"
            ],
            "boundaryReleaseObserved": detailed[
                "boundaryReleaseObserved"
            ],
            "productionAvailable": False,
        }
        if not hmac.compare_digest(
            envelope["executionProofHash"], _hash(execution_body)
        ):
            raise KisDomesticFunctionalStateBlocked(
                "manager execution proof hash mismatch"
            )
        return {
            "receiptKind": receipt_kind,
            "envelope": envelope,
            "detailedReceipt": detailed,
            "boundaryEntryProof": boundary,
            "attemptProofs": attempts,
            "transportReceipts": transports,
        }

    def _persist_manager_v2_receipt(
        self,
        conn: sqlite3.Connection,
        *,
        reservation: Mapping[str, Any],
        verified: Mapping[str, Any],
        state_transition_revision: int,
    ) -> None:
        envelope = dict(verified["envelope"])
        persisted_at = self._now()
        if persisted_at < envelope["occurredAt"]:
            raise KisDomesticFunctionalStateBlocked(
                "manager receipt persistence clock moved backwards"
            )
        try:
            conn.execute(
                """INSERT INTO kis_functional_state_manager_receipt
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ROUTE, reservation["reservationId"],
                    reservation["reservationKind"], reservation["revision"],
                    verified["receiptKind"], envelope["managerReceiptHash"],
                    envelope["executionProofHash"],
                    envelope["mutationPlanHash"],
                    envelope["ownedProjectionHash"],
                    envelope["ownedProjectionHeadHash"],
                    envelope["boundaryEntryProofHash"],
                    envelope["attemptChainHead"],
                    envelope["transportReceiptSetHash"],
                    _canonical(envelope), envelope["receiptHash"],
                    envelope["signature"], envelope["keyIdHash"],
                    envelope["occurredAt"], persisted_at,
                    state_transition_revision,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 receipt replay or journal collision"
            ) from exc

    @staticmethod
    def _components_hash(components: Mapping[str, Mapping[str, Any]]) -> str:
        return _hash({name: dict(components[name]) for name in sorted(components)})

    def _manager_binding(
        self, request: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not self._manager_v2_wired:
            raise KisDomesticFunctionalStateBlocked(
                "manager v2 binding is not configured"
            )
        expected_request_keys = {
            "schemaVersion", "route", "pdno", "reservationId",
            "reservationKind", "reservationRevision", "sessionId", "reservedAt",
            "accountFingerprint", "credentialConfigurationHash",
            "ownerEpochId", "ownerEpochHash", "stateComponentReadersHash",
            "productionAvailable",
        }
        if set(request) != expected_request_keys:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding request is not exact"
            )
        try:
            raw = self._manager_binding_reader(dict(request))
            if not isinstance(raw, Mapping):
                raise TypeError("non-mapping")
            value = dict(raw)
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding reader failed"
            ) from exc
        body_keys = expected_request_keys | {
            "componentReadersHash", "managerImplementationType",
            "managerIdHash", "managerCodeHash", "managerProtocolHash",
            "managerKeyIdHash",
            "managerComponentBindingsHash", "mutationPlanHash",
            "ownedProjectionHash", "ownedProjectionHeadHash",
            "finalMutationBoundarySchema", "receiptSchemaVersion",
            "verifyOnly",
        }
        if set(value) != body_keys | {"bindingHash", "signature"}:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding is not exact"
            )
        expected = {
            **dict(request),
            "schemaVersion": _MANAGER_BINDING_SCHEMA,
            "managerImplementationType": self._manager_implementation_type,
            "managerCodeHash": self._manager_code_hash,
            "managerProtocolHash": self._manager_protocol_hash,
            "managerKeyIdHash": self._manager_receipt_key_id_hash,
            "finalMutationBoundarySchema": _FINAL_BOUNDARY_SCHEMA,
            "receiptSchemaVersion": _MANAGER_RECEIPT_V2_SCHEMA,
            "verifyOnly": True,
            "productionAvailable": False,
        }
        for key, wanted in expected.items():
            if type(value.get(key)) is not type(wanted) or value.get(key) != wanted:
                raise KisDomesticFunctionalStateBlocked(
                    f"manager binding {key} mismatch"
                )
        for key in (
            "componentReadersHash", "managerIdHash", "managerCodeHash", "managerProtocolHash",
            "managerKeyIdHash", "managerComponentBindingsHash",
            "mutationPlanHash", "ownedProjectionHash",
            "ownedProjectionHeadHash", "bindingHash",
        ):
            _sha(value[key], f"manager binding {key}")
        if type(value["signature"]) is not str or not value["signature"]:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding signature is invalid"
            )
        unsigned = dict(value)
        unsigned.pop("signature")
        binding_hash = unsigned.pop("bindingHash")
        if not hmac.compare_digest(binding_hash, _hash(unsigned)):
            raise KisDomesticFunctionalStateBlocked(
                "manager binding body hash mismatch"
            )
        try:
            verified = self._manager_binding_verifier(dict(value))
        except Exception as exc:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding verifier failed"
            ) from exc
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalStateBlocked(
                "manager binding signature is unverified"
            )
        return value

    @staticmethod
    def _manager_binding_request(
        *, reservation_id: str, reservation_kind: str, revision: int,
        session_id: str, reserved_at: str, account: str, credential: str,
        owner_epoch_id: str, owner_epoch_hash: str,
        state_component_readers_hash: str,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": _MANAGER_BINDING_REQUEST_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "reservationId": reservation_id,
            "reservationKind": reservation_kind,
            "reservationRevision": revision,
            "sessionId": session_id,
            "reservedAt": reserved_at,
            "accountFingerprint": account,
            "credentialConfigurationHash": credential,
            "ownerEpochId": owner_epoch_id,
            "ownerEpochHash": owner_epoch_hash,
            "stateComponentReadersHash": state_component_readers_hash,
            "productionAvailable": False,
        }

    @staticmethod
    def _reservation_binding_hash(
        *, reservation_id: str, reservation_kind: str, revision: int,
        state_component_readers_hash: str,
        manager_component_readers_hash: str,
        manager_binding_hash: str,
        final_mutation_boundary_handle: str,
    ) -> str:
        return _hash(
            {
                "schemaVersion":
                    "kis-domestic-functional-state-reservation-binding/v1",
                "route": ROUTE,
                "reservationId": reservation_id,
                "reservationKind": reservation_kind,
                "reservationRevision": revision,
                "stateComponentReadersHash": state_component_readers_hash,
                "managerComponentReadersHash": manager_component_readers_hash,
                "managerBindingHash": manager_binding_hash,
                "finalMutationBoundaryHandle": final_mutation_boundary_handle,
                "productionAvailable": False,
            }
        )

    def _binding_record(
        self, conn: sqlite3.Connection, reservation: Mapping[str, Any]
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM kis_functional_state_manager_binding "
            "WHERE route=? AND reservation_id=?",
            (ROUTE, reservation["reservationId"]),
        ).fetchone()
        if row is None:
            raise KisDomesticFunctionalStateBlocked(
                "reservation manager binding is absent"
            )
        try:
            manager_binding = json.loads(str(row["manager_binding_json"]))
        except json.JSONDecodeError:
            raise KisDomesticFunctionalStateBlocked(
                "reservation manager binding is malformed"
            ) from None
        expected = (
            reservation["reservationKind"], reservation["revision"],
            reservation["componentReadersHash"],
            reservation["finalMutationBoundaryHandle"],
        )
        actual = (
            str(row["reservation_kind"]), int(row["reservation_revision"]),
            str(row["manager_component_readers_hash"]),
            str(row["final_mutation_boundary_handle"]),
        )
        if actual != expected:
            raise KisDomesticFunctionalStateBlocked(
                "reservation manager binding mismatch:projection"
            )
        calculated = self._reservation_binding_hash(
            reservation_id=reservation["reservationId"],
            reservation_kind=reservation["reservationKind"],
            revision=reservation["revision"],
            state_component_readers_hash=str(
                row["state_component_readers_hash"]
            ),
            manager_component_readers_hash=str(
                row["manager_component_readers_hash"]
            ),
            manager_binding_hash=str(row["manager_binding_hash"]),
            final_mutation_boundary_handle=str(
                row["final_mutation_boundary_handle"]
            ),
        )
        if not hmac.compare_digest(
            calculated, str(row["reservation_binding_hash"])
        ):
            raise KisDomesticFunctionalStateBlocked(
                "reservation binding hash mismatch"
            )
        return {
            "stateComponentReadersHash": str(
                row["state_component_readers_hash"]
            ),
            "managerComponentReadersHash": str(
                row["manager_component_readers_hash"]
            ),
            "managerBinding": manager_binding,
            "managerBindingHash": str(row["manager_binding_hash"]),
            "reservationBindingHash": str(row["reservation_binding_hash"]),
        }

    def _reserve(self, *, kind: str, allowed_phases: set[str], pending_session: str = "", account: str | None = None, credential: str | None = None, kill_preempt: bool = False) -> dict[str, Any]:
        reservation_id = f"kis-state-{kind.lower()}-{uuid.uuid4().hex}"
        hazard_blocked = False
        with kis_route_authority_serialization():
            with self.ledger.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT * FROM kis_functional_state_authority WHERE route=?", (ROUTE,)).fetchone()
                if row is None or str(row["phase"]) not in allowed_phases:
                    raise KisDomesticFunctionalStateBlocked(f"{kind} phase is not eligible")
                self._verify_row(conn, row)
                components = {
                    name: self._component(name, row) for name in _COMPONENTS
                }
                owner_epoch = self._matching_owner_epoch(row)
                component_hazards = sorted(
                    set().union(*(value["hazards"] for value in components.values()))
                )
                if row["reservation_id"] and not kill_preempt:
                    raise KisDomesticFunctionalStateBlocked("another state reservation is active")
                if (
                    row["reservation_id"]
                    and kill_preempt
                    and self._manager_v2_wired
                ):
                    prior_receipts = conn.execute(
                        "SELECT receipt_kind FROM "
                        "kis_functional_state_manager_receipt "
                        "WHERE route=? AND reservation_id=?",
                        (ROUTE, str(row["reservation_id"])),
                    ).fetchall()
                    if (
                        len(prior_receipts) != 1
                        or str(prior_receipts[0]["receipt_kind"])
                        != "PENDING"
                    ):
                        raise KisDomesticFunctionalStateBlocked(
                            "Kill preemption requires a durable pending manager receipt"
                        )
                if kind == "START" and owner_epoch["applicationLeaseHeld"] is not True:
                    raise KisDomesticFunctionalStateBlocked("start requires application lease")
                session = str(row["session_id"])
                pending = (
                    str(row["pending_session_id"]) or pending_session
                    if kind == "KILL"
                    else pending_session or str(row["pending_session_id"])
                )
                phase = "ARMED_WAIT_PUBLIC" if kind in {"START", "SETTINGS"} else "CLEANUP"
                if kind == "KILL" and not session:
                    session = pending
                hazards = sorted(
                    set(json.loads(row["durable_hazards_json"]))
                    | set(component_hazards)
                )
                if kind in {"START", "SETTINGS"} and hazards:
                    # Persist the observed hazard union before denying any
                    # authority-expanding manager call.
                    self._transition(
                        conn, row=row, phase=str(row["phase"]),
                        reservation_id="", reservation_kind="",
                        session_id=str(row["session_id"]),
                        pending_session_id=str(row["pending_session_id"]),
                        account=str(row["account_fingerprint"]),
                        credential=str(row["credential_configuration_hash"]),
                        routes_closed=True, hazards=hazards,
                    )
                    hazard_blocked = True
                    revision = int(row["revision"]) + 1
                else:
                    hazards = sorted(set(hazards) | {f"{kind}_PENDING"})
                    revision = int(row["revision"]) + 1
                    reserved_account = account or str(row["account_fingerprint"])
                    reserved_credential = credential or str(
                        row["credential_configuration_hash"]
                    )
                    reservation_session = session or pending
                    reserved_at = self._now()
                    state_components_hash = self._components_hash(components)
                    manager_binding: dict[str, Any] = {}
                    manager_binding_hash = "0" * 64
                    manager_components_hash = state_components_hash
                    if self._manager_v2_wired:
                        manager_request = self._manager_binding_request(
                            reservation_id=reservation_id,
                            reservation_kind=kind,
                            revision=revision,
                            session_id=reservation_session,
                            reserved_at=reserved_at,
                            account=reserved_account,
                            credential=reserved_credential,
                            owner_epoch_id=owner_epoch["ownerEpochId"],
                            owner_epoch_hash=owner_epoch["ownerEpochHash"],
                            state_component_readers_hash=state_components_hash,
                        )
                        manager_binding = self._manager_binding(manager_request)
                        manager_binding_hash = manager_binding["bindingHash"]
                        manager_components_hash = manager_binding[
                            "componentReadersHash"
                        ]
                    boundary_body = self._final_boundary_body(
                        reservation_id=reservation_id,
                        reservation_kind=kind,
                        revision=revision,
                        session_id=reservation_session,
                        account=reserved_account,
                        credential=reserved_credential,
                        owner_epoch_hash=owner_epoch["ownerEpochHash"],
                        component_readers_hash=manager_components_hash,
                    )
                    final_boundary_handle = _hash(boundary_body)
                    reservation_binding_hash = self._reservation_binding_hash(
                        reservation_id=reservation_id,
                        reservation_kind=kind,
                        revision=revision,
                        state_component_readers_hash=state_components_hash,
                        manager_component_readers_hash=manager_components_hash,
                        manager_binding_hash=manager_binding_hash,
                        final_mutation_boundary_handle=final_boundary_handle,
                    )
                    revision = self._transition(
                        conn, row=row, phase=phase, reservation_id=reservation_id,
                        reservation_kind=kind, session_id=session,
                        pending_session_id=pending,
                        account=reserved_account,
                        credential=reserved_credential,
                        routes_closed=True, hazards=hazards,
                        reservation_binding_hash=reservation_binding_hash,
                        occurred_at=reserved_at,
                    )
                    reserved_at = str(conn.execute(
                        "SELECT updated_at FROM kis_functional_state_authority WHERE route=?",
                        (ROUTE,),
                    ).fetchone()[0])
                    conn.execute(
                        """INSERT INTO kis_functional_state_manager_binding
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            ROUTE, reservation_id, kind, revision,
                            state_components_hash, manager_components_hash,
                            _canonical(manager_binding), manager_binding_hash,
                            final_boundary_handle, reservation_binding_hash,
                            reserved_at,
                        ),
                    )
        if hazard_blocked:
            raise KisDomesticFunctionalStateBlocked(
                f"{kind} blocked by component hazard union"
            )
        return {
            "reservationId": reservation_id, "reservationKind": kind,
            "revision": revision, "sessionId": session or pending,
            "reservedAt": reserved_at,
            "previousAccountFingerprint": str(row["account_fingerprint"]),
            "previousCredentialConfigurationHash": str(
                row["credential_configuration_hash"]
            ),
            "reservedAccountFingerprint": account or str(row["account_fingerprint"]),
            "reservedCredentialConfigurationHash": credential or str(
                row["credential_configuration_hash"]
            ),
            "ownerEpochId": owner_epoch["ownerEpochId"],
            "ownerEpochHash": owner_epoch["ownerEpochHash"],
            "componentReadersHash": manager_components_hash,
            "finalMutationBoundaryRequired": True,
            "finalMutationBoundaryHandleSchema": _FINAL_BOUNDARY_SCHEMA,
            "finalMutationBoundaryHandle": final_boundary_handle,
            "productionAvailable": False,
        }

    @contextmanager
    def final_mutation_boundary(
        self, *, reservation: Mapping[str, Any]
    ) -> Iterator[dict[str, Any]]:
        """Hold the KIS route while revalidating a two-phase reservation.

        This does not perform a mutation.  A future pinned manager/transport may
        enter this boundary immediately around its final transport edge.  The
        disabled state layer therefore exposes the complete contract without
        making any production path reachable.
        """
        exact_keys = {
            "reservationId", "reservationKind", "revision", "sessionId",
            "reservedAt", "previousAccountFingerprint",
            "previousCredentialConfigurationHash", "reservedAccountFingerprint",
            "reservedCredentialConfigurationHash", "ownerEpochId",
            "ownerEpochHash", "componentReadersHash",
            "finalMutationBoundaryRequired", "finalMutationBoundaryHandleSchema",
            "finalMutationBoundaryHandle", "productionAvailable",
        }
        if not isinstance(reservation, Mapping) or set(reservation) != exact_keys:
            raise KisDomesticFunctionalStateBlocked(
                "final mutation reservation is not exact"
            )
        value = dict(reservation)
        _identity(value["reservationId"], "final reservation id")
        if value["reservationKind"] not in {"START", "STOP", "KILL", "SETTINGS"}:
            raise KisDomesticFunctionalStateBlocked("final reservation kind is invalid")
        if type(value["revision"]) is not int or value["revision"] < 2:
            raise KisDomesticFunctionalStateBlocked("final reservation revision is invalid")
        if value["finalMutationBoundaryRequired"] is not True:
            raise KisDomesticFunctionalStateBlocked("final mutation boundary is not required")
        if value["finalMutationBoundaryHandleSchema"] != _FINAL_BOUNDARY_SCHEMA:
            raise KisDomesticFunctionalStateBlocked("final mutation boundary schema mismatch")
        if value["productionAvailable"] is not False:
            raise KisDomesticFunctionalStateBlocked("final mutation production flag mismatch")
        for key in (
            "reservedAccountFingerprint", "reservedCredentialConfigurationHash",
            "ownerEpochHash", "componentReadersHash",
            "finalMutationBoundaryHandle",
        ):
            _sha(value[key], key)
        _identity(value["ownerEpochId"], "final owner epoch id")
        _utc(
            datetime.fromisoformat(value["reservedAt"].replace("Z", "+00:00")),
            "final reservation time",
        )
        with kis_route_authority_serialization():
            with self.ledger.connection() as conn:
                conn.execute("BEGIN")
                row = conn.execute(
                    "SELECT * FROM kis_functional_state_authority WHERE route=?",
                    (ROUTE,),
                ).fetchone()
                if (
                    row is None
                    or str(row["reservation_id"]) != value["reservationId"]
                    or str(row["reservation_kind"]) != value["reservationKind"]
                    or int(row["revision"]) != value["revision"]
                    or str(row["updated_at"]) != value["reservedAt"]
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "final mutation reservation was superseded"
                    )
                self._verify_row(conn, row)
                binding_record = self._binding_record(conn, value)
                if not hmac.compare_digest(
                    str(row["reservation_binding_hash"]),
                    binding_record["reservationBindingHash"],
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "state reservation binding was superseded"
                    )
                owner_epoch = self._matching_owner_epoch(row)
                components = {
                    name: self._component(
                        name, row,
                        expected_account=value["reservedAccountFingerprint"],
                        expected_credential=value[
                            "reservedCredentialConfigurationHash"
                        ],
                    )
                    for name in _COMPONENTS
                }
                state_components_hash = self._components_hash(components)
                if not hmac.compare_digest(
                    state_components_hash,
                    binding_record["stateComponentReadersHash"],
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "state component projection changed before final boundary"
                    )
                if self._manager_v2_wired:
                    manager_request = self._manager_binding_request(
                        reservation_id=value["reservationId"],
                        reservation_kind=value["reservationKind"],
                        revision=value["revision"],
                        session_id=value["sessionId"],
                        reserved_at=value["reservedAt"],
                        account=value["reservedAccountFingerprint"],
                        credential=value[
                            "reservedCredentialConfigurationHash"
                        ],
                        owner_epoch_id=value["ownerEpochId"],
                        owner_epoch_hash=value["ownerEpochHash"],
                        state_component_readers_hash=state_components_hash,
                    )
                    fresh_manager_binding = self._manager_binding(
                        manager_request
                    )
                    if fresh_manager_binding != binding_record["managerBinding"]:
                        raise KisDomesticFunctionalStateBlocked(
                            "manager plan/projection binding changed before final boundary"
                        )
                manager_components_hash = binding_record[
                    "managerComponentReadersHash"
                ]
                body = self._final_boundary_body(
                    reservation_id=value["reservationId"],
                    reservation_kind=value["reservationKind"],
                    revision=value["revision"], session_id=value["sessionId"],
                    account=value["reservedAccountFingerprint"],
                    credential=value["reservedCredentialConfigurationHash"],
                    owner_epoch_hash=owner_epoch["ownerEpochHash"],
                    component_readers_hash=manager_components_hash,
                )
                if (
                    value["ownerEpochId"] != owner_epoch["ownerEpochId"]
                    or value["ownerEpochHash"] != owner_epoch["ownerEpochHash"]
                    or value["componentReadersHash"] != manager_components_hash
                    or not hmac.compare_digest(
                        value["finalMutationBoundaryHandle"], _hash(body)
                    )
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "final mutation reservation binding mismatch"
                    )
                yield {
                    **body,
                    "finalMutationBoundaryHandle": value[
                        "finalMutationBoundaryHandle"
                    ],
                    "routeLockHeld": True,
                }
                tail = conn.execute(
                    "SELECT reservation_id,reservation_kind,revision FROM kis_functional_state_authority WHERE route=?",
                    (ROUTE,),
                ).fetchone()
                if tail is None or tuple(tail) != (
                    value["reservationId"], value["reservationKind"],
                    value["revision"],
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "final mutation reservation changed inside boundary"
                    )

    def _finish(self, *, reservation: Mapping[str, Any], result: Mapping[str, Any], success_phase: str, clear_session: bool, open_routes: bool) -> dict[str, Any]:
        with kis_route_authority_serialization():
            with self.ledger.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT * FROM kis_functional_state_authority WHERE route=?", (ROUTE,)).fetchone()
                if row is None:
                    raise KisDomesticFunctionalStateBlocked("durable state authority is absent")
                self._verify_row(conn, row)
                if row["reservation_id"] != reservation["reservationId"] or row["reservation_kind"] != reservation["reservationKind"] or int(row["revision"]) != reservation["revision"]:
                    if (
                        reservation.get("reservationKind") == "START"
                        and not str(row["reservation_id"])
                    ):
                        hazards = sorted(
                            set(json.loads(row["durable_hazards_json"]))
                            | {"SUPERSEDED_START_REQUIRES_CLEANUP"}
                        )
                        cleanup_phase = (
                            str(row["phase"])
                            if str(row["phase"]) in {
                                "CLEANUP", "RECONCILIATION_REQUIRED"
                            }
                            else "RECONCILIATION_REQUIRED"
                        )
                        self._transition(
                            conn, row=row, phase=cleanup_phase,
                            reservation_id="", reservation_kind="",
                            session_id=str(row["session_id"]),
                            pending_session_id=str(row["pending_session_id"]),
                            account=str(row["account_fingerprint"]),
                            credential=str(row["credential_configuration_hash"]),
                            routes_closed=True, hazards=hazards,
                        )
                        # The caller must still receive the superseded error,
                        # but the cleanup obligation may not roll back with
                        # that exception.
                        conn.commit()
                    raise KisDomesticFunctionalStateBlocked("state reservation was superseded")
                self._matching_owner_epoch(row)
                binding_record = self._binding_record(conn, reservation)
                if not hmac.compare_digest(
                    str(row["reservation_binding_hash"]),
                    binding_record["reservationBindingHash"],
                ):
                    raise KisDomesticFunctionalStateBlocked(
                        "state reservation binding changed"
                    )
                manager_v2 = self._manager_v2_wired
                early_pending = False
                if manager_v2:
                    verified_v2 = self._manager_v2_result(
                        result,
                        reservation=reservation,
                        binding_record=binding_record,
                    )
                    result = verified_v2["envelope"]
                    early_pending = verified_v2["receiptKind"] == "PENDING"
                else:
                    verified_v2 = None
                component_account = str(row["account_fingerprint"])
                component_credential = str(row["credential_configuration_hash"])
                if reservation["reservationKind"] == "SETTINGS" and not result["ok"]:
                    component_account = reservation["previousAccountFingerprint"]
                    component_credential = reservation["previousCredentialConfigurationHash"]
                components = {
                    name: self._component(
                        name, row, expected_account=component_account,
                        expected_credential=component_credential,
                    )
                    for name in _COMPONENTS
                }
                components_hash = self._components_hash(components)
                if manager_v2:
                    if not hmac.compare_digest(
                        components_hash,
                        binding_record["stateComponentReadersHash"],
                    ):
                        raise KisDomesticFunctionalStateBlocked(
                            "state component projection changed after manager"
                        )
                    fresh_request = self._manager_binding_request(
                        reservation_id=reservation["reservationId"],
                        reservation_kind=reservation["reservationKind"],
                        revision=reservation["revision"],
                        session_id=reservation["sessionId"],
                        reserved_at=reservation["reservedAt"],
                        account=reservation["reservedAccountFingerprint"],
                        credential=reservation[
                            "reservedCredentialConfigurationHash"
                        ],
                        owner_epoch_id=reservation["ownerEpochId"],
                        owner_epoch_hash=reservation["ownerEpochHash"],
                        state_component_readers_hash=components_hash,
                    )
                    if self._manager_binding(fresh_request) != binding_record[
                        "managerBinding"
                    ]:
                        raise KisDomesticFunctionalStateBlocked(
                            "manager immutable plan/projection binding changed"
                        )
                elif (
                    self._disabled_manager_receipts
                    and isinstance(result, Mapping)
                    and set(result) == {
                        "ok", "mutationMayHaveOccurred", "receiptHash"
                    }
                ):
                    # Compatibility exists only for the disabled, no-network
                    # harness.  The durable result consumed below is still an
                    # exact signed receipt bound to the reservation and the
                    # fresh component-reader projection.
                    result = self.sign_disabled_manager_result_for_tests(
                        reservation=reservation,
                        ok=result["ok"],
                        mutation_may_have_occurred=result[
                            "mutationMayHaveOccurred"
                        ],
                        components_hash=components_hash,
                    )
                if not manager_v2:
                    result = self._manager_result(
                        result, reservation=reservation,
                        component_readers_hash=components_hash,
                    )
                hazards = sorted(
                    (
                        set(json.loads(row["durable_hazards_json"]))
                        | set().union(*(value["hazards"] for value in components.values()))
                    )
                    - {f"{reservation['reservationKind']}_PENDING"}
                )
                if early_pending:
                    hazards = sorted(
                        set(hazards)
                        | {
                            "MANAGER_DETACHED_BOUNDARY_PENDING",
                            "MANAGER_OUTCOME_AMBIGUOUS",
                        }
                    )
                    phase = "RECONCILIATION_REQUIRED"
                    revision = self._transition(
                        conn,
                        row=row,
                        phase=phase,
                        reservation_id=reservation["reservationId"],
                        reservation_kind=reservation["reservationKind"],
                        session_id=(
                            str(row["session_id"])
                            or str(row["pending_session_id"])
                        ),
                        pending_session_id=str(row["pending_session_id"]),
                        account=str(row["account_fingerprint"]),
                        credential=str(row[
                            "credential_configuration_hash"
                        ]),
                        routes_closed=True,
                        hazards=hazards,
                        reservation_binding_hash=str(
                            row["reservation_binding_hash"]
                        ),
                    )
                    self._persist_manager_v2_receipt(
                        conn,
                        reservation=reservation,
                        verified=verified_v2,
                        state_transition_revision=revision,
                    )
                    return {
                        "phase": phase,
                        "revision": revision,
                        "sessionId": (
                            str(row["session_id"])
                            or str(row["pending_session_id"])
                        ),
                        "reservationId": reservation["reservationId"],
                        "reservationPending": True,
                        "managerReceiptHash": result[
                            "managerReceiptHash"
                        ],
                        "productionAvailable": False,
                    }
                if result["ok"]:
                    phase = success_phase
                elif result["mutationMayHaveOccurred"]:
                    phase = "RECONCILIATION_REQUIRED"; hazards.append("MANAGER_OUTCOME_AMBIGUOUS")
                    open_routes = False; clear_session = False
                else:
                    phase = "IDLE" if reservation["reservationKind"] in {"START", "SETTINGS"} else "RECONCILIATION_REQUIRED"
                    if phase != "IDLE": hazards.append("MANAGER_OPERATION_FAILED")
                    open_routes = phase == "IDLE"; clear_session = phase == "IDLE"
                if open_routes and hazards:
                    phase = "RECONCILIATION_REQUIRED"
                    hazards.append("ROUTE_REOPEN_BLOCKED_BY_HAZARD")
                    open_routes = False
                    clear_session = False
                session = "" if clear_session else (str(row["session_id"]) or str(row["pending_session_id"]))
                account = str(row["account_fingerprint"])
                credential = str(row["credential_configuration_hash"])
                if reservation["reservationKind"] == "SETTINGS" and not result["ok"]:
                    account = reservation["previousAccountFingerprint"]
                    credential = reservation["previousCredentialConfigurationHash"]
                revision = self._transition(
                    conn, row=row, phase=phase, reservation_id="", reservation_kind="",
                    session_id=session, pending_session_id="",
                    account=account, credential=credential,
                    routes_closed=not open_routes, hazards=hazards,
                )
                if manager_v2:
                    self._persist_manager_v2_receipt(
                        conn,
                        reservation=reservation,
                        verified=verified_v2,
                        state_transition_revision=revision,
                    )
        return {
            "phase": phase,
            "revision": revision,
            "sessionId": session,
            "reservationId": "",
            "reservationPending": False,
            "managerReceiptHash": (
                result["managerReceiptHash"]
                if manager_v2
                else result["receiptHash"]
            ),
            "productionAvailable": False,
        }

    @staticmethod
    def _configure_manager_for_reservation(
        manager: Any,
        *,
        reservation: Mapping[str, Any],
    ) -> None:
        configure = getattr(manager, "configure_reservation", None)
        if configure is None:
            return
        if not callable(configure):
            raise KisDomesticFunctionalStateBlocked(
                "manager reservation configurator is invalid"
            )
        configured = configure(dict(reservation))
        if configured is not None:
            raise KisDomesticFunctionalStateBlocked(
                "manager reservation configurator returned authority"
            )

    def start(self, *, session_id: str, manager: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> dict[str, Any]:
        session_id = _identity(session_id, "start session id")
        if not callable(manager): raise KisDomesticFunctionalStateBlocked("start manager is invalid")
        reservation = self._reserve(kind="START", allowed_phases={"IDLE"}, pending_session=session_id)
        self._configure_manager_for_reservation(manager, reservation=reservation)
        result = manager(dict(reservation))
        return self._finish(reservation=reservation, result=result, success_phase="ACTIVE", clear_session=False, open_routes=False)

    def stop(self, *, manager: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> dict[str, Any]:
        if not callable(manager): raise KisDomesticFunctionalStateBlocked("stop manager is invalid")
        reservation = self._reserve(kind="STOP", allowed_phases={"ACTIVE", "CLEANUP", "RECONCILIATION_REQUIRED"})
        self._configure_manager_for_reservation(manager, reservation=reservation)
        result = manager(dict(reservation))
        return self._finish(reservation=reservation, result=result, success_phase="IDLE", clear_session=True, open_routes=True)

    def kill(self, *, manager: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> dict[str, Any]:
        if not callable(manager): raise KisDomesticFunctionalStateBlocked("Kill manager is invalid")
        reservation = self._reserve(
            kind="KILL",
            allowed_phases={"IDLE", "ARMED_WAIT_PUBLIC", "ACTIVE", "CLEANUP", "RECONCILIATION_REQUIRED"},
            pending_session=f"kis-kill-{uuid.uuid4().hex}",
            kill_preempt=True,
        )
        self._configure_manager_for_reservation(manager, reservation=reservation)
        result = manager(dict(reservation))
        return self._finish(reservation=reservation, result=result, success_phase="CLEANUP", clear_session=False, open_routes=False)

    def apply_settings(self, *, account_fingerprint: str, credential_configuration_hash: str, manager: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> dict[str, Any]:
        account = _sha(account_fingerprint, "settings account fingerprint")
        credential = _sha(credential_configuration_hash, "settings credential configuration hash")
        if not callable(manager): raise KisDomesticFunctionalStateBlocked("settings manager is invalid")
        reservation = self._reserve(kind="SETTINGS", allowed_phases={"IDLE"}, pending_session=f"kis-settings-{uuid.uuid4().hex}", account=account, credential=credential)
        self._configure_manager_for_reservation(manager, reservation=reservation)
        result = manager(dict(reservation))
        return self._finish(reservation=reservation, result=result, success_phase="IDLE", clear_session=True, open_routes=True)

    def status(self) -> dict[str, Any]:
        return {**self.authority_snapshot(), **production_entrypoint_status()}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False, "backendAvailable": False, "networkAvailable": False,
        "managerWiringAvailable": False, "releaseEvidenceAvailable": False,
        "stateReceiptV2IntegrationImplemented": True,
        "stateReceiptV2IntegrationWired": False,
        "durableManagerBindingAndReceiptJournalImplemented": True,
        "route": ROUTE, "pdno": PDNO,
        "reason": "ISOLATED_STATE_COMPOSITION_ONLY_NO_SHARED_OR_BACKEND_WIRING",
    }


__all__ = [
    "DurableKisDomesticFunctionalState", "KisDomesticFunctionalStateBlocked",
    "production_entrypoint_status",
]
