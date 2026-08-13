from __future__ import annotations

"""Disabled external-capability ledger for the KIS functional canary."""

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import re
import sqlite3
from typing import Any, Callable, Mapping

from .kis_domestic_functional_contract import PDNO, ROUTE
from .program_ledger import ProgramLedger


KIS_DOMESTIC_FUNCTIONAL_CAPABILITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CAPABILITY_MINT_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CAPABILITY_REVOKE_AVAILABLE = False

_SCHEMA_VERSION = "kis-domestic-functional-capability-schema/v1"
_ZERO = "0" * 64
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_ENTRY = frozenset({"NATURAL_BUY"})
_CLEANUP = frozenset({"CLEANUP_CANCEL", "CLEANUP_SELL"})
_PHASES = frozenset({"ACTIVE", "CLEANUP", "REVOKED", "RECONCILIATION_REQUIRED"})
_PROVIDER_REVOKE_DOMAIN = b"kis-domestic-functional-external-revoke/v1\x00"
_LEASE_DOMAIN = b"kis-domestic-functional-capability-lease/v1\x00"
_MAX_REVOKE_PROOF_AGE = timedelta(seconds=5)
_GRANT_KEYS = frozenset({
    "schemaVersion", "route", "pdno", "capabilityId", "armId", "sessionId",
    "accountFingerprint", "credentialConfigurationHash", "permitId", "permitHash",
    "codeManifestHash", "baselineHash", "capsHash", "rollingSnapshotHash",
    "heartbeatBindingHash", "phase", "grantedAt",
})
_TRANSITION_KEYS = frozenset({
    "schemaVersion", "route", "pdno", "capabilityId", "revision", "phase",
    "occurredAt", "previousHash",
})
_REVOKE_PROOF_KEYS = frozenset({
    "schemaVersion", "provider", "route", "pdno", "capabilityId", "sessionId",
    "accountFingerprint", "credentialConfigurationHash", "revokedAt",
    "providerReceiptHash", "capabilityAbsentVerified", "providerKeyIdHash",
    "signatureHash",
})


class KisDomesticFunctionalCapabilityBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is invalid")
    return value


def _id(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is invalid")
    return value


def _utc(value: datetime, label: str) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is not canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or _utc(parsed, label) != value:
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is not canonical UTC")
    return parsed


def _revision(value: Any, label: str = "expected revision") -> int:
    if type(value) is not int or value < 1:
        raise KisDomesticFunctionalCapabilityBlocked(f"{label} is invalid")
    return value


def sign_kis_domestic_external_revoke_proof(
    key: bytes, body: Mapping[str, Any]
) -> str:
    if not isinstance(key, bytes) or len(key) < 32 or not isinstance(body, Mapping):
        raise KisDomesticFunctionalCapabilityBlocked("external revoke signer input is invalid")
    return hmac.new(
        key,
        _PROVIDER_REVOKE_DOMAIN + _canonical(dict(body)).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = [tuple(row) for row in conn.execute(
        """SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master
           WHERE name LIKE 'kis_capability_%' OR tbl_name LIKE 'kis_capability_%'
           ORDER BY type,name"""
    )]
    tables: dict[str, Any] = {}
    for typ, name, _, _ in objects:
        if typ != "table":
            continue
        quoted = str(name).replace('"', '""')
        indexes = []
        for row in conn.execute(f'PRAGMA index_list("{quoted}")'):
            idx = str(row[1]).replace('"', '""')
            indexes.append((tuple(row), tuple(tuple(x) for x in conn.execute(f'PRAGMA index_xinfo("{idx}")'))))
        tables[str(name)] = {
            "info": tuple(tuple(row) for row in conn.execute(f'PRAGMA table_info("{quoted}")')),
            "xinfo": tuple(tuple(row) for row in conn.execute(f'PRAGMA table_xinfo("{quoted}")')),
            "foreignKeys": tuple(tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{quoted}")')),
            "indexes": tuple(indexes),
        }
    return {"objects": objects, "tables": tables}


class DurableKisDomesticFunctionalCapabilityLedger:
    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        signer_key: bytes,
        signer_key_id: str,
        revoke_provider_key: bytes,
        revoke_provider_key_id: str,
        owner_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalCapabilityBlocked("exact ProgramLedger is required")
        if not isinstance(signer_key, bytes) or len(signer_key) < 32:
            raise KisDomesticFunctionalCapabilityBlocked("signer key is invalid")
        if not isinstance(revoke_provider_key, bytes) or len(revoke_provider_key) < 32:
            raise KisDomesticFunctionalCapabilityBlocked("revoke provider key is invalid")
        self.ledger = program_ledger
        self._key = bytes(signer_key)
        self._key_id_hash = hashlib.sha256(_id(signer_key_id, "signer key id").encode()).hexdigest()
        self._provider_key = bytes(revoke_provider_key)
        self._provider_key_id_hash = hashlib.sha256(
            _id(revoke_provider_key_id, "revoke provider key id").encode()
        ).hexdigest()
        self._owner_hash = hashlib.sha256(_id(owner_id, "owner id").encode()).hexdigest()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_schema()

    def _now(self) -> str:
        return _utc(self.clock(), "capability clock")

    def _now_datetime(self) -> datetime:
        return _parse_utc(self._now(), "capability clock")

    def _ensure_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS kis_capability_schema (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), version TEXT NOT NULL,
            schema_hash TEXT NOT NULL, owner_hash TEXT NOT NULL, signer_key_id_hash TEXT NOT NULL,
            revoke_provider_key_id_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kis_capability_authority (
            route TEXT PRIMARY KEY, capability_id TEXT NOT NULL UNIQUE,
            capability_hash TEXT NOT NULL UNIQUE, phase TEXT NOT NULL,
            arm_id TEXT NOT NULL, session_id TEXT NOT NULL, account_fingerprint TEXT NOT NULL,
            credential_configuration_hash TEXT NOT NULL,
            permit_id TEXT NOT NULL, permit_hash TEXT NOT NULL, code_manifest_hash TEXT NOT NULL,
            baseline_hash TEXT NOT NULL, caps_hash TEXT NOT NULL, rolling_snapshot_hash TEXT NOT NULL,
            heartbeat_binding_hash TEXT NOT NULL, grant_record_json TEXT NOT NULL,
            grant_record_hash TEXT NOT NULL UNIQUE, grant_signature TEXT NOT NULL,
            revoke_record_json TEXT NOT NULL DEFAULT '', revoke_record_hash TEXT NOT NULL DEFAULT '',
            revoke_signature TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kis_capability_transition (
            capability_id TEXT NOT NULL, revision INTEGER NOT NULL, phase TEXT NOT NULL,
            occurred_at TEXT NOT NULL, previous_hash TEXT NOT NULL, record_json TEXT NOT NULL,
            record_hash TEXT NOT NULL UNIQUE, signature TEXT NOT NULL, signer_key_id_hash TEXT NOT NULL,
            PRIMARY KEY(capability_id,revision)
        );
        """
        statements = [item.strip() for item in ddl.split(";") if item.strip()]
        expected = sqlite3.connect(":memory:")
        try:
            for statement in statements:
                expected.execute(statement)
            expected_hash = _hash(_snapshot(expected))
        finally:
            expected.close()
        with self.ledger.connection() as conn:
            before = _snapshot(conn)
            if before["objects"] and not hmac.compare_digest(_hash(before), expected_hash):
                raise KisDomesticFunctionalCapabilityBlocked("capability SQLite schema fingerprint mismatch")
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                conn.execute(statement)
            if not hmac.compare_digest(_hash(_snapshot(conn)), expected_hash):
                raise KisDomesticFunctionalCapabilityBlocked("capability SQLite schema fingerprint mismatch")
            rows = conn.execute("SELECT * FROM kis_capability_schema").fetchall()
            expected_row = (
                1, _SCHEMA_VERSION, expected_hash, self._owner_hash,
                self._key_id_hash, self._provider_key_id_hash,
            )
            if not rows:
                conn.execute("INSERT INTO kis_capability_schema VALUES (?,?,?,?,?,?)", expected_row)
            elif len(rows) != 1 or tuple(rows[0]) != expected_row:
                raise KisDomesticFunctionalCapabilityBlocked("capability owner/key/schema changed")

    def _signed(self, body: Mapping[str, Any]) -> tuple[str, str, str]:
        text = _canonical(dict(body))
        digest = hashlib.sha256(text.encode()).hexdigest()
        signature = hmac.new(self._key, text.encode(), hashlib.sha256).hexdigest()
        return text, digest, signature

    def _transition(self, conn: sqlite3.Connection, *, capability_id: str, revision: int, phase: str, previous: str, now: str) -> str:
        body = {"schemaVersion": "kis-domestic-functional-capability-transition/v1", "route": ROUTE, "pdno": PDNO, "capabilityId": capability_id, "revision": revision, "phase": phase, "occurredAt": now, "previousHash": previous}
        text, digest, signature = self._signed(body)
        conn.execute("INSERT INTO kis_capability_transition VALUES (?,?,?,?,?,?,?,?,?)", (capability_id, revision, phase, now, previous, text, digest, signature, self._key_id_hash))
        return digest

    def mint(
        self,
        *,
        capability_id: str,
        raw_capability: str,
        arm_id: str,
        session_id: str,
        account_fingerprint: str,
        credential_configuration_hash: str,
        permit_id: str,
        permit_hash: str,
        code_manifest_hash: str,
        baseline_hash: str,
        caps_hash: str,
        rolling_snapshot_hash: str,
        heartbeat_binding_hash: str,
    ) -> dict[str, Any]:
        capability_id = _id(capability_id, "capability id")
        raw_capability = _id(raw_capability, "raw capability")
        values = {
            "schemaVersion": "kis-domestic-functional-capability-grant/v1", "route": ROUTE, "pdno": PDNO,
            "capabilityId": capability_id, "armId": _id(arm_id, "arm id"),
            "sessionId": _id(session_id, "session id"), "accountFingerprint": _sha(account_fingerprint, "account fingerprint"),
            "credentialConfigurationHash": _sha(
                credential_configuration_hash, "credential configuration hash"
            ),
            "permitId": _id(permit_id, "permit id"), "permitHash": _sha(permit_hash, "permit hash"),
            "codeManifestHash": _sha(code_manifest_hash, "code manifest hash"), "baselineHash": _sha(baseline_hash, "baseline hash"),
            "capsHash": _sha(caps_hash, "caps hash"), "rollingSnapshotHash": _sha(rolling_snapshot_hash, "rolling snapshot hash"),
            "heartbeatBindingHash": _sha(heartbeat_binding_hash, "heartbeat binding hash"), "phase": "ACTIVE", "grantedAt": self._now(),
        }
        capability_hash = hashlib.sha256(raw_capability.encode()).hexdigest()
        text, grant_hash, signature = self._signed(values)
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            head = self._transition(conn, capability_id=capability_id, revision=1, phase="ACTIVE", previous=_ZERO, now=values["grantedAt"])
            try:
                conn.execute(
                    """INSERT INTO kis_capability_authority
                       (route,capability_id,capability_hash,phase,arm_id,session_id,account_fingerprint,
                        credential_configuration_hash,
                        permit_id,permit_hash,code_manifest_hash,baseline_hash,caps_hash,
                        rolling_snapshot_hash,heartbeat_binding_hash,grant_record_json,
                        grant_record_hash,grant_signature,revoke_record_json,revoke_record_hash,
                        revoke_signature,revision,created_at,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'','','',1,?,?)""",
                    (
                        ROUTE, capability_id, capability_hash, "ACTIVE", values["armId"],
                        values["sessionId"], values["accountFingerprint"],
                        values["credentialConfigurationHash"], values["permitId"],
                        values["permitHash"], values["codeManifestHash"], values["baselineHash"],
                        values["capsHash"], values["rollingSnapshotHash"],
                        values["heartbeatBindingHash"], text, grant_hash, signature,
                        values["grantedAt"], values["grantedAt"],
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise KisDomesticFunctionalCapabilityBlocked("route capability already has an owner") from exc
        return {"capabilityId": capability_id, "capabilityHash": capability_hash, "phase": "ACTIVE", "revision": 1, "transitionHeadHash": head}

    def _verify_provider_proof(
        self,
        proof: Mapping[str, Any],
        row: sqlite3.Row,
        *,
        trusted_observed_at: datetime,
    ) -> dict[str, Any]:
        if set(proof) != _REVOKE_PROOF_KEYS:
            raise KisDomesticFunctionalCapabilityBlocked("external revoke proof fields are not exact")
        body = dict(proof)
        signature = body.pop("signatureHash")
        if type(signature) is not str or not _SHA.fullmatch(signature):
            raise KisDomesticFunctionalCapabilityBlocked("external revoke signature is invalid")
        expected = {
            "schemaVersion": "kis-domestic-functional-capability-revoke-proof/v1",
            "provider": "STATE_OWNED_KIS_CAPABILITY_PROVIDER",
            "route": ROUTE,
            "pdno": PDNO,
            "capabilityId": str(row["capability_id"]),
            "sessionId": str(row["session_id"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "revokedAt": body.get("revokedAt"),
            "providerReceiptHash": body.get("providerReceiptHash"),
            "capabilityAbsentVerified": True,
            "providerKeyIdHash": self._provider_key_id_hash,
        }
        if set(body) != set(expected):
            raise KisDomesticFunctionalCapabilityBlocked("external revoke proof body is not exact")
        for key, wanted in expected.items():
            if type(body.get(key)) is not type(wanted) or body.get(key) != wanted:
                raise KisDomesticFunctionalCapabilityBlocked(
                    f"external revoke proof {key} mismatch"
                )
        _sha(body["providerReceiptHash"], "provider receipt hash")
        revoked_at = _parse_utc(body["revokedAt"], "external revoke revokedAt")
        age = trusted_observed_at - revoked_at
        if age < timedelta(0) or age > _MAX_REVOKE_PROOF_AGE:
            raise KisDomesticFunctionalCapabilityBlocked(
                "external revoke proof is stale or future-dated"
            )
        wanted_signature = sign_kis_domestic_external_revoke_proof(
            self._provider_key, body
        )
        if not hmac.compare_digest(signature, wanted_signature):
            raise KisDomesticFunctionalCapabilityBlocked(
                "external revoke provider signature mismatch"
            )
        return body

    def _verify(self, conn: sqlite3.Connection, row: sqlite3.Row) -> None:
        if (
            str(row["route"]) != ROUTE
            or str(row["phase"]) not in _PHASES
            or type(row["revision"]) is not int
            or int(row["revision"]) < 1
        ):
            raise KisDomesticFunctionalCapabilityBlocked(
                "capability authority row projection is invalid"
            )
        for column in (
            "capability_hash", "account_fingerprint", "credential_configuration_hash",
            "permit_hash", "code_manifest_hash", "baseline_hash", "caps_hash",
            "rolling_snapshot_hash", "heartbeat_binding_hash", "grant_record_hash",
        ):
            _sha(str(row[column]), f"capability {column}")
        for column in ("capability_id", "arm_id", "session_id", "permit_id"):
            _id(str(row[column]), f"capability {column}")
        created_at = _parse_utc(str(row["created_at"]), "capability createdAt")
        updated_at = _parse_utc(str(row["updated_at"]), "capability updatedAt")
        if updated_at < created_at:
            raise KisDomesticFunctionalCapabilityBlocked("capability time lineage regressed")
        grant = {
            "schemaVersion": "kis-domestic-functional-capability-grant/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "capabilityId": str(row["capability_id"]),
            "armId": str(row["arm_id"]),
            "sessionId": str(row["session_id"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "permitId": str(row["permit_id"]),
            "permitHash": str(row["permit_hash"]),
            "codeManifestHash": str(row["code_manifest_hash"]),
            "baselineHash": str(row["baseline_hash"]),
            "capsHash": str(row["caps_hash"]),
            "rollingSnapshotHash": str(row["rolling_snapshot_hash"]),
            "heartbeatBindingHash": str(row["heartbeat_binding_hash"]),
            "phase": "ACTIVE",
            "grantedAt": str(row["created_at"]),
        }
        if set(grant) != _GRANT_KEYS:
            raise AssertionError("internal grant schema drift")
        text, digest, signature = self._signed(grant)
        if (
            str(row["grant_record_json"]) != text
            or not hmac.compare_digest(digest, str(row["grant_record_hash"]))
            or not hmac.compare_digest(signature, str(row["grant_signature"]))
        ):
            raise KisDomesticFunctionalCapabilityBlocked(
                "capability grant failed exact reconstruction"
            )

        transitions = conn.execute(
            "SELECT * FROM kis_capability_transition WHERE capability_id=? ORDER BY revision",
            (row["capability_id"],),
        ).fetchall()
        previous = _ZERO
        previous_phase: str | None = None
        previous_time: datetime | None = None
        legal = {
            None: {"ACTIVE"},
            "ACTIVE": {"CLEANUP", "RECONCILIATION_REQUIRED"},
            "CLEANUP": {"CLEANUP", "RECONCILIATION_REQUIRED", "REVOKED"},
            "RECONCILIATION_REQUIRED": {
                "CLEANUP", "RECONCILIATION_REQUIRED", "REVOKED"
            },
            "REVOKED": set(),
        }
        for revision, item in enumerate(transitions, start=1):
            occurred = _parse_utc(str(item["occurred_at"]), "transition occurredAt")
            phase = str(item["phase"])
            expected = {
                "schemaVersion": "kis-domestic-functional-capability-transition/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "capabilityId": str(row["capability_id"]),
                "revision": revision,
                "phase": phase,
                "occurredAt": str(item["occurred_at"]),
                "previousHash": previous,
            }
            if set(expected) != _TRANSITION_KEYS:
                raise AssertionError("internal transition schema drift")
            text, digest, signature = self._signed(expected)
            if (
                type(item["revision"]) is not int
                or int(item["revision"]) != revision
                or phase not in legal.get(previous_phase, set())
                or str(item["previous_hash"]) != previous
                or str(item["record_json"]) != text
                or not hmac.compare_digest(digest, str(item["record_hash"]))
                or not hmac.compare_digest(signature, str(item["signature"]))
                or not hmac.compare_digest(
                    self._key_id_hash, str(item["signer_key_id_hash"])
                )
                or (previous_time is not None and occurred < previous_time)
            ):
                raise KisDomesticFunctionalCapabilityBlocked(
                    "capability transition chain failed exact reconstruction"
                )
            if revision == 1 and occurred != created_at:
                raise KisDomesticFunctionalCapabilityBlocked(
                    "capability grant/transition time mismatch"
                )
            previous, previous_phase, previous_time = digest, phase, occurred
        if (
            len(transitions) != int(row["revision"])
            or not transitions
            or previous_phase != str(row["phase"])
            or previous_time != updated_at
        ):
            raise KisDomesticFunctionalCapabilityBlocked(
                "capability transition chain is incomplete"
            )
        revoke_values = tuple(
            str(row[name])
            for name in ("revoke_record_json", "revoke_record_hash", "revoke_signature")
        )
        if str(row["phase"]) == "REVOKED":
            try:
                revoke_body = json.loads(revoke_values[0])
            except (TypeError, json.JSONDecodeError):
                raise KisDomesticFunctionalCapabilityBlocked(
                    "external revoke record JSON is invalid"
                ) from None
            proof = {**revoke_body, "signatureHash": revoke_values[2]}
            verified = self._verify_provider_proof(
                proof, row, trusted_observed_at=updated_at
            )
            if (
                _canonical(verified) != revoke_values[0]
                or not hmac.compare_digest(_hash(verified), revoke_values[1])
            ):
                raise KisDomesticFunctionalCapabilityBlocked(
                    "external revoke record projection mismatch"
                )
        elif revoke_values != ("", "", ""):
            raise KisDomesticFunctionalCapabilityBlocked(
                "non-revoked capability contains revoke evidence"
            )

    def authorize(self, *, raw_capability: str, operation: str, expected_revision: int) -> dict[str, Any]:
        expected_revision = _revision(expected_revision)
        if type(operation) is not str or operation not in (_ENTRY | _CLEANUP):
            raise KisDomesticFunctionalCapabilityBlocked("capability operation is invalid")
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT * FROM kis_capability_authority WHERE route=?", (ROUTE,)).fetchone()
            if row is None:
                raise KisDomesticFunctionalCapabilityBlocked("capability is absent")
            self._verify(conn, row)
            head = conn.execute(
                "SELECT record_hash FROM kis_capability_transition WHERE capability_id=? AND revision=?",
                (row["capability_id"], expected_revision),
            ).fetchone()
        if int(row["revision"]) != expected_revision or not hmac.compare_digest(hashlib.sha256(_id(raw_capability, "raw capability").encode()).hexdigest(), str(row["capability_hash"])):
            raise KisDomesticFunctionalCapabilityBlocked("capability/revision changed")
        phase = str(row["phase"])
        allowed = operation in _ENTRY and phase == "ACTIVE" or operation in _CLEANUP and phase in {"CLEANUP", "RECONCILIATION_REQUIRED"}
        if not allowed:
            raise KisDomesticFunctionalCapabilityBlocked("operation is not authorized in capability phase")
        if head is None:
            raise KisDomesticFunctionalCapabilityBlocked("capability transition head is absent")
        body = {
            "schemaVersion": "kis-domestic-functional-capability-lease/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "active": True,
            "capabilityId": str(row["capability_id"]),
            "capabilityHash": str(row["capability_hash"]),
            "armId": str(row["arm_id"]),
            "sessionId": str(row["session_id"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "permitId": str(row["permit_id"]),
            "permitHash": str(row["permit_hash"]),
            "codeManifestHash": str(row["code_manifest_hash"]),
            "baselineHash": str(row["baseline_hash"]),
            "capsHash": str(row["caps_hash"]),
            "rollingSnapshotHash": str(row["rolling_snapshot_hash"]),
            "heartbeatBindingHash": str(row["heartbeat_binding_hash"]),
            "grantRecordHash": str(row["grant_record_hash"]),
            "transitionHeadHash": str(head["record_hash"]),
            "phase": phase,
            "revision": expected_revision,
            "operation": operation,
            "cleanupOnly": operation in _CLEANUP,
            "authorizedAt": self._now(),
            "authorityKeyIdHash": self._key_id_hash,
            "productionAvailable": False,
        }
        lease_hash = _hash(body)
        signature = hmac.new(
            self._key,
            _LEASE_DOMAIN + _canonical({**body, "leaseHash": lease_hash}).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {**body, "leaseHash": lease_hash, "signatureHash": signature}

    def verify_authorization_lease(self, lease: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(lease, Mapping):
            raise KisDomesticFunctionalCapabilityBlocked("capability lease is not an object")
        candidate = dict(lease)
        if set(candidate) != {
            "schemaVersion", "route", "pdno", "active", "capabilityId",
            "capabilityHash", "armId", "sessionId", "accountFingerprint",
            "credentialConfigurationHash", "permitId", "permitHash",
            "codeManifestHash", "baselineHash", "capsHash", "rollingSnapshotHash",
            "heartbeatBindingHash", "grantRecordHash", "transitionHeadHash", "phase",
            "revision", "operation", "cleanupOnly", "authorizedAt",
            "authorityKeyIdHash", "productionAvailable", "leaseHash", "signatureHash",
        }:
            raise KisDomesticFunctionalCapabilityBlocked("capability lease fields are not exact")
        signature = candidate.pop("signatureHash")
        lease_hash = candidate.pop("leaseHash")
        if (
            type(signature) is not str
            or not _SHA.fullmatch(signature)
            or type(lease_hash) is not str
            or not _SHA.fullmatch(lease_hash)
            or not hmac.compare_digest(lease_hash, _hash(candidate))
            or not hmac.compare_digest(
                signature,
                hmac.new(
                    self._key,
                    _LEASE_DOMAIN
                    + _canonical({**candidate, "leaseHash": lease_hash}).encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest(),
            )
        ):
            raise KisDomesticFunctionalCapabilityBlocked("capability lease signature mismatch")
        revision = _revision(candidate.get("revision"), "lease revision")
        if (
            candidate.get("schemaVersion") != "kis-domestic-functional-capability-lease/v1"
            or candidate.get("route") != ROUTE
            or candidate.get("pdno") != PDNO
            or candidate.get("active") is not True
            or candidate.get("productionAvailable") is not False
            or candidate.get("authorityKeyIdHash") != self._key_id_hash
            or type(candidate.get("cleanupOnly")) is not bool
        ):
            raise KisDomesticFunctionalCapabilityBlocked("capability lease constants mismatch")
        _parse_utc(candidate.get("authorizedAt"), "lease authorizedAt")
        operation = candidate.get("operation")
        if type(operation) is not str or operation not in (_ENTRY | _CLEANUP):
            raise KisDomesticFunctionalCapabilityBlocked("capability lease operation invalid")
        with self.ledger.connection() as conn:
            row = conn.execute(
                "SELECT * FROM kis_capability_authority WHERE route=?", (ROUTE,)
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalCapabilityBlocked("capability is absent")
            self._verify(conn, row)
            head = conn.execute(
                "SELECT record_hash FROM kis_capability_transition WHERE capability_id=? AND revision=?",
                (row["capability_id"], revision),
            ).fetchone()
        expected = {
            "capabilityId": str(row["capability_id"]),
            "capabilityHash": str(row["capability_hash"]),
            "armId": str(row["arm_id"]),
            "sessionId": str(row["session_id"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(row["credential_configuration_hash"]),
            "permitId": str(row["permit_id"]),
            "permitHash": str(row["permit_hash"]),
            "codeManifestHash": str(row["code_manifest_hash"]),
            "baselineHash": str(row["baseline_hash"]),
            "capsHash": str(row["caps_hash"]),
            "rollingSnapshotHash": str(row["rolling_snapshot_hash"]),
            "heartbeatBindingHash": str(row["heartbeat_binding_hash"]),
            "grantRecordHash": str(row["grant_record_hash"]),
            "transitionHeadHash": "" if head is None else str(head["record_hash"]),
            "phase": str(row["phase"]),
            "revision": int(row["revision"]),
            "cleanupOnly": operation in _CLEANUP,
        }
        for key, wanted in expected.items():
            if type(candidate.get(key)) is not type(wanted) or candidate.get(key) != wanted:
                raise KisDomesticFunctionalCapabilityBlocked(
                    f"capability lease {key} is stale or mismatched"
                )
        return {**candidate, "leaseHash": lease_hash, "signatureHash": signature}

    def begin_cleanup(self, *, expected_revision: int, reason_hash: str, reconciliation_required: bool = False) -> dict[str, Any]:
        expected_revision = _revision(expected_revision)
        if type(reconciliation_required) is not bool:
            raise KisDomesticFunctionalCapabilityBlocked("reconciliation flag is invalid")
        target = "RECONCILIATION_REQUIRED" if reconciliation_required else "CLEANUP"
        now = self._now()
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM kis_capability_authority WHERE route=?", (ROUTE,)).fetchone()
            if row is None or int(row["revision"]) != expected_revision or str(row["phase"]) not in {"ACTIVE", "CLEANUP", "RECONCILIATION_REQUIRED"}:
                raise KisDomesticFunctionalCapabilityBlocked("cleanup capability CAS failed")
            self._verify(conn, row); _sha(reason_hash, "cleanup reason hash")
            revision = expected_revision + 1
            previous = str(conn.execute("SELECT record_hash FROM kis_capability_transition WHERE capability_id=? ORDER BY revision DESC LIMIT 1", (row["capability_id"],)).fetchone()[0])
            self._transition(conn, capability_id=row["capability_id"], revision=revision, phase=target, previous=previous, now=now)
            cursor = conn.execute("UPDATE kis_capability_authority SET phase=?,revision=?,updated_at=? WHERE route=? AND revision=?", (target, revision, now, ROUTE, expected_revision))
            if cursor.rowcount != 1:
                raise KisDomesticFunctionalCapabilityBlocked("cleanup capability update CAS failed")
        return {"phase": target, "revision": revision, "entryAuthority": False, "cleanupAuthority": True}

    def revoke(self, *, expected_revision: int, external_revoke_proof: Mapping[str, Any]) -> dict[str, Any]:
        expected_revision = _revision(expected_revision)
        if not isinstance(external_revoke_proof, Mapping):
            raise KisDomesticFunctionalCapabilityBlocked("external revoke proof is incomplete")
        proof = dict(external_revoke_proof)
        now_datetime = self._now_datetime()
        now = _utc(now_datetime, "capability clock")
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM kis_capability_authority WHERE route=?", (ROUTE,)).fetchone()
            if row is None or int(row["revision"]) != expected_revision or str(row["phase"]) not in {"CLEANUP", "RECONCILIATION_REQUIRED"}:
                raise KisDomesticFunctionalCapabilityBlocked("revoke capability CAS failed")
            self._verify(conn, row)
            body = self._verify_provider_proof(
                proof, row, trusted_observed_at=now_datetime
            )
            text = _canonical(body)
            digest = _hash(body)
            signature = str(proof["signatureHash"])
            revision = expected_revision + 1
            previous = str(conn.execute("SELECT record_hash FROM kis_capability_transition WHERE capability_id=? ORDER BY revision DESC LIMIT 1", (row["capability_id"],)).fetchone()[0])
            self._transition(conn, capability_id=row["capability_id"], revision=revision, phase="REVOKED", previous=previous, now=now)
            cursor = conn.execute("UPDATE kis_capability_authority SET phase='REVOKED',revoke_record_json=?,revoke_record_hash=?,revoke_signature=?,revision=?,updated_at=? WHERE route=? AND revision=?", (text, digest, signature, revision, now, ROUTE, expected_revision))
            if cursor.rowcount != 1:
                raise KisDomesticFunctionalCapabilityBlocked("revoke capability update CAS failed")
        return {"phase": "REVOKED", "revision": revision, "externallyRevoked": True}

    def status(self) -> dict[str, Any]:
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT * FROM kis_capability_authority WHERE route=?", (ROUTE,)).fetchone()
            if row is not None:
                self._verify(conn, row)
        return {**production_entrypoint_status(), "phase": None if row is None else row["phase"], "revision": 0 if row is None else row["revision"]}


def production_entrypoint_status() -> dict[str, Any]:
    return {"available": False, "mintAvailable": False, "revokeAvailable": False, "networkAvailable": False, "sharedWiringAvailable": False, "route": ROUTE, "pdno": PDNO, "reason": "ISOLATED_CAPABILITY_LEDGER_ONLY"}


__all__ = [
    "DurableKisDomesticFunctionalCapabilityLedger",
    "KisDomesticFunctionalCapabilityBlocked",
    "production_entrypoint_status",
    "sign_kis_domestic_external_revoke_proof",
]
