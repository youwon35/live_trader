from __future__ import annotations

"""Exact manual-start consent plus fresh short wire grants; import grants nothing.

The store is deliberately transport free. Its caller must hold the KIS route
lock and prove ownership of the same OS account lease as the specialized path.
Serialized permits are evidence only: the final sender always resolves the
current state-owned row and consumes a durable one-use wire claim.
"""

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any, Mapping


class KisScopedAuthorityError(RuntimeError):
    pass


SCHEMA = "kis-scoped-manual-start-context/v1"
_HASH = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_FIELDS = frozenset({"schemaVersion", "scopeHash", "profileId", "mode",
    "portfolioId", "deploymentId", "strategyId", "runtimeScopeId",
    "capitalCurrency", "accountFingerprint", "credentialConfigurationHash",
    "wireAccountFingerprint", "wireCredentialConfigurationHash", "ownerEpochHash"})
_HASH_FIELDS = frozenset({"scopeHash", "accountFingerprint", "credentialConfigurationHash",
    "wireAccountFingerprint", "wireCredentialConfigurationHash", "ownerEpochHash"})


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, allow_nan=False,
        sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def exact_context(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != _FIELDS:
        raise KisScopedAuthorityError("KIS operational context fields are not exact")
    result = dict(value)
    if any(type(item) is not str or len(item) > 512 for item in result.values()):
        raise KisScopedAuthorityError("KIS operational context value is invalid")
    if result["schemaVersion"] != SCHEMA or result["profileId"] != "stock":
        raise KisScopedAuthorityError("KIS operational context schema/profile is invalid")
    if result["mode"] not in {"SMALL_LIVE", "FULL_LIVE"} or result["capitalCurrency"] not in {"KRW", "USD"}:
        raise KisScopedAuthorityError("KIS operational mode/currency is invalid")
    if any(not _HASH.fullmatch(result[key]) for key in _HASH_FIELDS):
        raise KisScopedAuthorityError("KIS operational identity hash is invalid")
    if not all(result[key] for key in ("deploymentId", "runtimeScopeId", "strategyId")):
        raise KisScopedAuthorityError("KIS operational exact deployment is missing")
    return result


@dataclass(frozen=True)
class ScopedPermit:
    permit_id: str
    session_id: str
    reservation_id: str
    context: Mapping[str, str]
    expires_at: float


class KisScopedOperationalAuthority:
    def __init__(self, path: Path | str, *, clock=time.time, monotonic=time.monotonic):
        self.path = Path(path)
        self.clock = clock
        self.monotonic = monotonic

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.path), timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        connection.executescript("""
          CREATE TABLE IF NOT EXISTS kis_operational_exclusions (
            account TEXT PRIMARY KEY, owner TEXT NOT NULL, proof TEXT NOT NULL,
            status TEXT NOT NULL, updated REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS kis_operational_permits (
            permit_id TEXT PRIMARY KEY, session_id TEXT UNIQUE NOT NULL,
            reservation_id TEXT NOT NULL, context_json TEXT NOT NULL,
            context_hash TEXT NOT NULL, status TEXT NOT NULL,
            issued REAL NOT NULL, expires REAL NOT NULL,
            issued_mono REAL NOT NULL, expires_mono REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS kis_operational_events (
            seq INTEGER PRIMARY KEY, permit_id TEXT NOT NULL, event TEXT NOT NULL,
            detail TEXT NOT NULL, occurred REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS kis_operational_wire_claims (
            claim_hash TEXT PRIMARY KEY, permit_id TEXT NOT NULL,
            consumed REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS kis_operational_wire_grants (
            grant_id TEXT PRIMARY KEY, permit_id TEXT NOT NULL,
            evidence_hash TEXT NOT NULL, issued REAL NOT NULL, expires REAL NOT NULL,
            issued_mono REAL NOT NULL, expires_mono REAL NOT NULL,
            UNIQUE(permit_id,evidence_hash));
        """)
        return connection

    def exclusion(self, account: str) -> dict[str, Any]:
        # Missing storage is UNKNOWN, never an inactivity proof.
        if not self.path.is_file():
            return {"status": "UNKNOWN"}
        with closing(sqlite3.connect(f"file:{self.path.as_posix()}?mode=ro", uri=True)) as connection:
            row = connection.execute("SELECT owner,proof,status FROM kis_operational_exclusions WHERE account=?", (account,)).fetchone()
        if row is None:
            return {"status": "UNKNOWN"}
        return {"status": row[2], "ownerEpochHash": row[0], "proofHash": row[1]}

    def establish_exclusion(self, context: Mapping[str, Any], *, proof: Mapping[str, Any]) -> None:
        context = exact_context(context)
        required = {"routeQuiescent", "specializedClosureInstalled", "sameAccountLeaseHeld", "reconciliationClear"}
        if set(proof) != required or any(proof[key] is not True for key in required):
            raise KisScopedAuthorityError("KIS specialized writer exclusion is UNKNOWN")
        with closing(self._connect()) as connection, connection:
            connection.execute("INSERT OR REPLACE INTO kis_operational_exclusions VALUES(?,?,?,?,?)",
                (context["wireAccountFingerprint"], context["ownerEpochHash"], canonical_hash(dict(proof)), "EXCLUDED", self.clock()))
            connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) VALUES('','SPECIALIZED_EXCLUDED',?,?)",
                (canonical_hash(context), self.clock()))

    def reserve(self, context: Mapping[str, Any], *, session_id: str, reservation_id: str,
                expires_at: float) -> ScopedPermit:
        context = exact_context(context)
        now = self.clock()
        if not session_id or not reservation_id or not now < expires_at <= now + 300:
            raise KisScopedAuthorityError("KIS pending permit session/reservation/expiry is invalid")
        exclusion = self.exclusion(context["wireAccountFingerprint"])
        if exclusion.get("status") != "EXCLUDED" or exclusion.get("ownerEpochHash") != context["ownerEpochHash"]:
            raise KisScopedAuthorityError("KIS specialized writer exclusion is UNKNOWN")
        permit = ScopedPermit("kis-operational-" + secrets.token_hex(16), session_id, reservation_id, context, expires_at)
        with closing(self._connect()) as connection, connection:
            connection.execute("UPDATE kis_operational_permits SET status='REVOKED' WHERE status IN ('PENDING','ACTIVE') AND json_extract(context_json,'$.runtimeScopeId')=?", (context["runtimeScopeId"],))
            mono = self.monotonic()
            connection.execute("INSERT INTO kis_operational_permits VALUES(?,?,?,?,?,?,?,?,?,?)",
                (permit.permit_id, session_id, reservation_id, json.dumps(context, sort_keys=True), canonical_hash(context), "PENDING", now, expires_at, mono, mono + expires_at - now))
            connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) VALUES(?,?,?,?)",
                (permit.permit_id, "RESERVED", reservation_id, now))
        return permit

    def activate(self, session_id: str, *, context: Mapping[str, Any], reservation_id: str,
                 reservation_finalized: bool, session_running: bool) -> None:
        if reservation_finalized is not True or session_running is not True:
            self.revoke(session_id=session_id, reason="start-not-finalized")
            raise KisScopedAuthorityError("KIS permit requires finalized reservation and exact RUNNING session")
        self._read(session_id, context, allowed={"PENDING"}, reservation_id=reservation_id)
        with closing(self._connect()) as connection, connection:
            changed = connection.execute("UPDATE kis_operational_permits SET status='ACTIVE' WHERE session_id=? AND status='PENDING' AND issued<=? AND expires>? AND issued_mono<=? AND expires_mono>?", (session_id, self.clock(), self.clock(), self.monotonic(), self.monotonic())).rowcount
            if changed != 1:
                raise KisScopedAuthorityError("KIS permit was revoked during start")
            connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) SELECT permit_id,'ACTIVE',?,? FROM kis_operational_permits WHERE session_id=?", (reservation_id, self.clock(), session_id))

    def record_native_cleanup_policy(self, session_id, *, context, policy):
        """Seal the STOP policy only while the new native start is PENDING.

        An older ACTIVE consent cannot silently acquire cancellation consent
        after an application upgrade. The state caller has just consumed the
        one-use native confirmation for this exact context including policy.
        """
        row = self._read(session_id, context, allowed={"PENDING"})
        with closing(self._connect()) as connection, connection:
            current = connection.execute("SELECT status FROM kis_operational_permits WHERE permit_id=?", (row["permit_id"],)).fetchone()
            if current is None or current[0] != "PENDING":
                raise KisScopedAuthorityError("KIS cleanup consent cannot be added to an old running session")
            connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) VALUES(?,?,?,?)",
                (row["permit_id"], "NATIVE_STOP_CLEANUP_POLICY", canonical_hash(policy), self.clock()))

    def _read(self, session_id: str, context: Mapping[str, Any], *, allowed=frozenset({"ACTIVE"}), reservation_id: str = "") -> dict[str, Any]:
        context = exact_context(context)
        if not self.path.is_file():
            raise KisScopedAuthorityError("KIS operational permit is missing")
        with closing(sqlite3.connect(f"file:{self.path.as_posix()}?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM kis_operational_permits WHERE session_id=?", (session_id,)).fetchone()
        if row is None or row["status"] not in allowed or self.clock() < row["issued"] or self.monotonic() < row["issued_mono"]:
            raise KisScopedAuthorityError("KIS operational permit is missing, revoked or expired")
        if row["status"] == "PENDING" and (self.clock() >= row["expires"] or self.monotonic() >= row["expires_mono"]):
            raise KisScopedAuthorityError("KIS pending native start reservation expired")
        if row["context_hash"] != canonical_hash(context) or row["context_json"] != json.dumps(context, sort_keys=True):
            raise KisScopedAuthorityError("KIS operational account/deployment/policy context changed")
        if reservation_id and row["reservation_id"] != reservation_id:
            raise KisScopedAuthorityError("KIS operational start reservation changed")
        exclusion = self.exclusion(context["wireAccountFingerprint"])
        if exclusion.get("status") != "EXCLUDED" or exclusion.get("ownerEpochHash") != context["ownerEpochHash"]:
            raise KisScopedAuthorityError("KIS specialized writer exclusion is UNKNOWN")
        return dict(row)

    def authorize_consent(self, session_id: str, *, context: Mapping[str, Any]) -> dict[str, Any]:
        row = self._read(session_id, context)
        return {"consentId": row["permit_id"], "scopeHash": context["scopeHash"],
            "consentLifetime": "EXACT_RUNNING_USER_SESSION_UNTIL_REVOKED", "wirePermitMaximumSeconds": 15,
            "freshPreflightRequired": True, "orderAuthorityGranted": False}

    def authorize(self, session_id: str, *, context: Mapping[str, Any], fresh_evidence: Mapping[str, Any]) -> dict[str, Any]:
        """Renew only a short grant under unchanged human consent and NEW truth.

        These state-owned facts are not accepted from HTTP/strategy metadata.
        The final resolver validates the exact current preflight and native
        ticket before constructing this typed evidence.
        """
        row=self._read(session_id,context)
        fields={"schemaVersion","preflightHash","preflightExpiresAt","nativeTicketHash","nativeAsOf","boundaryHash","boundaryStartsAt"}
        if not isinstance(fresh_evidence,Mapping) or set(fresh_evidence)!=fields or fresh_evidence.get("schemaVersion")!="kis-operational-wire-evidence/v1":
            raise KisScopedAuthorityError("KIS fresh current wire evidence is required")
        if any(not _HASH.fullmatch(str(fresh_evidence[key])) for key in ("preflightHash","nativeTicketHash","boundaryHash")):
            raise KisScopedAuthorityError("KIS wire evidence hash is invalid")
        now=self.clock(); mono=self.monotonic()
        for key in ("preflightExpiresAt","nativeAsOf","boundaryStartsAt"):
            if type(fresh_evidence[key]) not in {int,float} or not 0<float(fresh_evidence[key])<float('inf'):
                raise KisScopedAuthorityError("KIS wire evidence timestamp is invalid")
        native=float(fresh_evidence["nativeAsOf"]);boundary=float(fresh_evidence["boundaryStartsAt"])
        expires=min(float(fresh_evidence["preflightExpiresAt"]),native+15,boundary+30,now+15)
        if not 0<=now-native<=15 or not 0<=now-boundary<=30 or expires<=now:
            raise KisScopedAuthorityError("KIS current preflight/native next-open evidence expired")
        evidence_hash=canonical_hash(dict(fresh_evidence)); grant_id="kis-wire-"+canonical_hash({"consentId":row["permit_id"],"evidenceHash":evidence_hash})
        with closing(self._connect()) as connection, connection:
            connection.execute("INSERT OR IGNORE INTO kis_operational_wire_grants VALUES(?,?,?,?,?,?,?)",
                (grant_id,row["permit_id"],evidence_hash,now,expires,mono,mono+expires-now))
            grant=connection.execute("SELECT * FROM kis_operational_wire_grants WHERE grant_id=?",(grant_id,)).fetchone()
        if grant is None or not grant["issued"]<=now<grant["expires"] or not grant["issued_mono"]<=mono<grant["expires_mono"]:
            raise KisScopedAuthorityError("KIS one-use wire grant expired; fresh evidence is required")
        return {"consentId":row["permit_id"],"permitId":grant_id,"expiresAt":grant["expires"],
            "scopeHash":context["scopeHash"],"maximumLifetimeSeconds":15}

    def consume_wire(self, session_id: str, *, context: Mapping[str, Any], intent: Mapping[str, Any], dispatch_key: str, fresh_evidence: Mapping[str, Any]) -> None:
        row = self._read(session_id, context)
        grant=self.authorize(session_id,context=context,fresh_evidence=fresh_evidence)
        if intent.get("operation") != "PLACE_ORDER" or intent.get("accountFingerprint") != context["wireAccountFingerprint"] or intent.get("credentialConfigurationHash") != context["wireCredentialConfigurationHash"]:
            raise KisScopedAuthorityError("KIS operational exact wire account/operation changed")
        if not _HASH.fullmatch(str(intent.get("payloadHash") or "")):
            raise KisScopedAuthorityError("KIS operational wire hash is invalid")
        if type(dispatch_key) is not str or not _HASH.fullmatch(dispatch_key):
            raise KisScopedAuthorityError("KIS operational state-owned dispatch identity is missing")
        # A refreshed access token, different claim nonce or a restarted
        # session must never allow the same native evaluation to submit twice.
        claim = canonical_hash({"runtimeScopeId": context["runtimeScopeId"], "dispatchKey": dispatch_key})
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                current=connection.execute("SELECT * FROM kis_operational_permits WHERE permit_id=?", (row["permit_id"],)).fetchone()
                if current is None or current["status"] != "ACTIVE" or self.clock()<current["issued"] or self.monotonic()<current["issued_mono"] or current["context_hash"] != canonical_hash(context):
                    raise KisScopedAuthorityError("KIS operational permit changed before wire claim")
                wire_grant=connection.execute("SELECT * FROM kis_operational_wire_grants WHERE grant_id=?",(grant["permitId"],)).fetchone()
                if wire_grant is None or self.clock()>=wire_grant["expires"] or self.monotonic()>=wire_grant["expires_mono"]:
                    raise KisScopedAuthorityError("KIS wire grant expired before consumption")
                connection.execute("INSERT INTO kis_operational_wire_claims VALUES(?,?,?)", (claim, row["permit_id"], self.clock()))
        except sqlite3.IntegrityError as exc:
            raise KisScopedAuthorityError("KIS operational exact wire claim was already consumed") from exc

    def revoke(self, *, session_id: str = "", reason: str = "state-mutated") -> None:
        if not self.path.is_file():
            return
        with closing(self._connect()) as connection, connection:
            rows = connection.execute("SELECT permit_id FROM kis_operational_permits WHERE status IN ('PENDING','ACTIVE')" + (" AND session_id=?" if session_id else ""), (session_id,) if session_id else ()).fetchall()
            for row in rows:
                connection.execute("UPDATE kis_operational_permits SET status='REVOKED' WHERE permit_id=?", (row[0],))
                connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) VALUES(?,?,?,?)", (row[0], "REVOKED", reason[:160], self.clock()))

    def status(self, *, session_id: str = "") -> dict[str, Any]:
        if not self.path.is_file():
            return {"status": "MISSING", "permits": [], "orderAuthorityGranted": False}
        with closing(sqlite3.connect(f"file:{self.path.as_posix()}?mode=ro", uri=True)) as connection:
            connection.row_factory=sqlite3.Row
            rows=connection.execute("SELECT * FROM kis_operational_permits" + (" WHERE session_id=?" if session_id else "") + " ORDER BY issued DESC LIMIT 50", (session_id,) if session_id else ()).fetchall()
        permits=[]
        for row in rows:
            context=json.loads(row["context_json"])
            remaining=min(row["expires"]-self.clock(),row["expires_mono"]-self.monotonic())
            status=row["status"]
            if status == "PENDING" and remaining<=0 or status in {"PENDING","ACTIVE"} and (self.clock()<row["issued"] or self.monotonic()<row["issued_mono"]):
                status="EXPIRED"
            permits.append({"permitId":row["permit_id"],"sessionId":row["session_id"],
                "runtimeScopeId":context["runtimeScopeId"],"deploymentId":context["deploymentId"],
                "capitalCurrency":context["capitalCurrency"],"mode":context["mode"],"status":status,
                "expiresAt":row["expires"] if status=="PENDING" else None,"remainingSeconds":max(0,remaining) if status=="PENDING" else None})
        return {"status":permits[0]["status"] if permits else "MISSING", "permits":permits,
            "consentLifetime":"EXACT_RUNNING_USER_SESSION_UNTIL_REVOKED","wirePermitMaximumSeconds":15,
            "automaticRenewalEnabled":True,"freshPreflightRequired":True,"orderAuthorityGranted":False}
