"""Durable, cancel-only continuation of one previously native-approved run.

Nothing in this module performs transport or grants authority at import. The
state-owned final resolver must independently prove current owner/credentials,
the stopped worker, exact ledger receipts and fresh complete broker truth.
"""
from __future__ import annotations

from contextlib import closing
import json
import secrets
import sqlite3
import time
from typing import Any, Mapping

from .kis_scoped_operational_authority import (
    KisScopedAuthorityError, canonical_hash, exact_context,
)

CLEANUP_POLICY = {
    "schemaVersion": "kis-scoped-owned-stop-cleanup/v1",
    "trigger": "EXACT_OWNED_PENDING_ON_MANUAL_STOP",
    "newPlaceConsentRevokedFirst": True,
    "wirePermitMaximumSeconds": 15,
    "cancelOnly": True,
    "wholeRemainingQuantityFromCurrentBroker": True,
    "completeOfficialTerminalAndReconciliationRequired": True,
    "retainAccountWriterUntilTerminalReconciled": True,
    "retryAmbiguousCancelAutomatically": False,
}


def dispatch_business_hash(dispatch):
    """Only the existing verified-identity CAS may change these five fields."""
    excluded = {"order_date", "broker_order_key", "broker_order_identity_verified",
                "order_date_source", "broker_order_identity_receipt_hash"}
    return canonical_hash({key: value for key, value in dispatch.items() if key not in excluded})


def dispatch_ownership_hash(dispatch):
    # Generic execution polling may legitimately move accepted -> terminal.
    # A STOP recovery retains the original ACK/request identity while every
    # later cancel still binds the *current full row* and fresh broker proof.
    stable = {key: value for key, value in dispatch.items() if key not in {"state", "updated_at"}}
    return dispatch_business_hash(stable)


class KisScopedCleanupStore:
    def __init__(self, operational_store):
        self.operational = operational_store
        self.clock = operational_store.clock
        self.monotonic = operational_store.monotonic

    def _connect(self):
        connection = self.operational._connect()
        connection.executescript("""
          CREATE TABLE IF NOT EXISTS kis_operational_cleanups (
            cleanup_id TEXT PRIMARY KEY, session_id TEXT UNIQUE NOT NULL,
            context_json TEXT NOT NULL, context_hash TEXT NOT NULL,
            orders_json TEXT NOT NULL, orders_hash TEXT NOT NULL,
            status TEXT NOT NULL, detail TEXT NOT NULL,
            truth_json TEXT NOT NULL, truth_hash TEXT NOT NULL,
            issued REAL NOT NULL, truth_issued REAL NOT NULL,
            truth_expires REAL NOT NULL, truth_mono REAL NOT NULL,
            truth_expires_mono REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS kis_operational_cleanup_claims (
            cleanup_id TEXT NOT NULL, dispatch_key TEXT NOT NULL,
            intent_hash TEXT NOT NULL, truth_hash TEXT NOT NULL,
            state TEXT NOT NULL, consumed REAL NOT NULL,
            PRIMARY KEY(cleanup_id,dispatch_key));
        """)
        return connection

    def consent_context(self, session_id):
        if not self.operational.path.is_file():
            raise KisScopedAuthorityError("KIS cleanup has no original native consent")
        with closing(self.operational._connect()) as connection:
            row = connection.execute("SELECT * FROM kis_operational_permits WHERE session_id=?", (session_id,)).fetchone()
            active = None if row is None else connection.execute(
                "SELECT 1 FROM kis_operational_events WHERE permit_id=? AND event='ACTIVE' LIMIT 1", (row["permit_id"],)).fetchone()
            policy = None if row is None else connection.execute(
                "SELECT 1 FROM kis_operational_events WHERE permit_id=? AND event='NATIVE_STOP_CLEANUP_POLICY' AND detail=? LIMIT 1",
                (row["permit_id"], canonical_hash(CLEANUP_POLICY))).fetchone()
        if row is None or active is None or policy is None or row["status"] not in {"ACTIVE", "REVOKED"}:
            raise KisScopedAuthorityError("KIS cleanup requires a previously ACTIVE native operating consent")
        context = exact_context(json.loads(row["context_json"]))
        if canonical_hash(context) != row["context_hash"]:
            raise KisScopedAuthorityError("KIS cleanup original consent hash is invalid")
        return context

    def reserve(self, session_id, context, orders):
        context = exact_context(context)
        if context != self.consent_context(session_id):
            raise KisScopedAuthorityError("KIS cleanup original account/deployment context changed")
        if not isinstance(orders, list) or len(orders) > 64:
            raise KisScopedAuthorityError("KIS cleanup bounded complete dispatch set is required")
        keys = [item.get("dispatchKey") for item in orders if isinstance(item, dict)]
        if len(keys) != len(orders) or any(not isinstance(key, str) or not key for key in keys) or len(set(keys)) != len(keys):
            raise KisScopedAuthorityError("KIS cleanup exact unique dispatch keys are required")
        detached = json.loads(json.dumps(orders, allow_nan=False, sort_keys=True))
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("UPDATE kis_operational_permits SET status='REVOKED' WHERE session_id=?", (session_id,))
            row = connection.execute("SELECT * FROM kis_operational_cleanups WHERE session_id=?", (session_id,)).fetchone()
            if row is not None:
                if row["context_hash"] != canonical_hash(context) or row["orders_hash"] != canonical_hash(detached):
                    raise KisScopedAuthorityError("KIS cleanup reserved receipt set changed; recovery is required")
                return dict(row)
            cleanup_id = "kis-stop-cleanup-" + secrets.token_hex(16)
            connection.execute("INSERT INTO kis_operational_cleanups VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (cleanup_id, session_id, json.dumps(context, sort_keys=True), canonical_hash(context),
                 json.dumps(detached, sort_keys=True), canonical_hash(detached), "RESERVED", "PLACE_REVOKED_BEFORE_DRAIN", "{}", "", self.clock(), 0, 0, 0, 0))
            connection.execute("INSERT INTO kis_operational_events(permit_id,event,detail,occurred) SELECT permit_id,'STOP_CLEANUP_RESERVED',?,? FROM kis_operational_permits WHERE session_id=?", (cleanup_id, self.clock(), session_id))
            return dict(connection.execute("SELECT * FROM kis_operational_cleanups WHERE cleanup_id=?", (cleanup_id,)).fetchone())

    def read(self, cleanup_id):
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM kis_operational_cleanups WHERE cleanup_id=?", (cleanup_id,)).fetchone()
        if row is None:
            raise KisScopedAuthorityError("KIS cleanup reservation is missing")
        return dict(row)

    def for_session(self, session_id):
        if not self.operational.path.is_file():
            return None
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM kis_operational_cleanups WHERE session_id=?", (session_id,)).fetchone()
        return dict(row) if row is not None else None

    def bind_truth(self, cleanup_id, truth, *, expires_at, ownership):
        now, mono = self.clock(), self.monotonic()
        if type(expires_at) not in {int, float} or not now < expires_at <= now + 15:
            raise KisScopedAuthorityError("KIS cleanup fresh 15-second proof is required")
        detached = json.loads(json.dumps(truth, sort_keys=True, allow_nan=False))
        reserved = json.loads(self.read(cleanup_id)["orders_json"])
        expected = {item["dispatchKey"]: item["ownershipHash"] for item in reserved}
        actual = {item.get("dispatchKey"): item.get("ownershipHash") for item in ownership}
        if (len(detached.get("orders", [])) != len(expected) or len(ownership) != len(expected)
                or {item.get("dispatchKey") for item in detached.get("orders", [])} != set(expected) or actual != expected):
            raise KisScopedAuthorityError("KIS cleanup proof is not the exact reserved owned receipt set")
        with closing(self._connect()) as connection, connection:
            count = connection.execute("UPDATE kis_operational_cleanups SET truth_json=?,truth_hash=?,truth_issued=?,truth_expires=?,truth_mono=?,truth_expires_mono=?,status='RECONCILIATION_REQUIRED',detail='FRESH_PROOF_PENDING_TERMINAL_RECONCILIATION' WHERE cleanup_id=? AND status!='CLEAN'",
                (json.dumps(detached, sort_keys=True), canonical_hash(detached), now, expires_at, mono, mono + expires_at - now, cleanup_id)).rowcount
            if count != 1:
                raise KisScopedAuthorityError("KIS cleanup is already terminal or missing")

    def authorize(self, cleanup_id, *, context, dispatch_key):
        row = self.read(cleanup_id)
        if row["status"] not in {"RESERVED", "RECONCILIATION_REQUIRED"} or row["context_hash"] != canonical_hash(exact_context(context)):
            raise KisScopedAuthorityError("KIS cleanup scope is missing, completed or changed")
        if not row["truth_issued"] <= self.clock() < row["truth_expires"] or not row["truth_mono"] <= self.monotonic() < row["truth_expires_mono"]:
            raise KisScopedAuthorityError("KIS cleanup broker proof expired or clock rolled back")
        with closing(self._connect()) as connection:
            consent = connection.execute("SELECT status FROM kis_operational_permits WHERE session_id=?", (row["session_id"],)).fetchone()
            claim = connection.execute("SELECT 1 FROM kis_operational_cleanup_claims WHERE cleanup_id=? AND dispatch_key=?", (cleanup_id, dispatch_key)).fetchone()
        if consent is None or consent[0] != "REVOKED" or claim is not None:
            raise KisScopedAuthorityError("KIS cleanup requires revoked PLACE consent and an unused exact cancel")
        truth = json.loads(row["truth_json"])
        if canonical_hash(truth) != row["truth_hash"]:
            raise KisScopedAuthorityError("KIS cleanup broker proof hash changed")
        matches = [item for item in truth.get("orders", []) if item.get("dispatchKey") == dispatch_key]
        if len(matches) != 1 or matches[0].get("state") != "WORKING":
            raise KisScopedAuthorityError("KIS cleanup exact current working receipt is required")
        return row, matches[0]

    def consume(self, cleanup_id, *, context, dispatch_key, intent):
        row, _proof = self.authorize(cleanup_id, context=context, dispatch_key=dispatch_key)
        if intent.get("operation") not in {"SCOPED_CANCEL_KR", "SCOPED_CANCEL_US"}:
            raise KisScopedAuthorityError("KIS cleanup never grants PLACE, SELL or revision")
        if intent.get("accountFingerprint") != context["wireAccountFingerprint"] or intent.get("credentialConfigurationHash") != context["wireCredentialConfigurationHash"]:
            raise KisScopedAuthorityError("KIS cleanup wire account/credential changed")
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                current = connection.execute("SELECT * FROM kis_operational_cleanups WHERE cleanup_id=?", (cleanup_id,)).fetchone()
                consent = connection.execute("SELECT status FROM kis_operational_permits WHERE session_id=?", (row["session_id"],)).fetchone()
                if current is None or current["status"] == "CLEAN" or current["truth_hash"] != row["truth_hash"] or consent is None or consent[0] != "REVOKED" or not current["truth_issued"] <= self.clock() < current["truth_expires"] or not current["truth_mono"] <= self.monotonic() < current["truth_expires_mono"]:
                    raise KisScopedAuthorityError("KIS cleanup proof changed before exact wire consumption")
                connection.execute("INSERT INTO kis_operational_cleanup_claims VALUES(?,?,?,?,?,?)",
                    (cleanup_id, dispatch_key, canonical_hash(dict(intent)), row["truth_hash"], "POSSIBLY_SENT", self.clock()))
        except sqlite3.IntegrityError as exc:
            raise KisScopedAuthorityError("KIS cleanup exact cancel already consumed; reconcile instead of retrying") from exc

    def finish(self, cleanup_id, *, terminal_truth, owned_capital_settled, reconciliation_passed):
        row = self.read(cleanup_id)
        reserved = json.loads(row["orders_json"])
        orders = terminal_truth.get("orders", []) if isinstance(terminal_truth, dict) else []
        expected = {item["dispatchKey"] for item in reserved}
        if not (owned_capital_settled is True and reconciliation_passed is True and len(orders) == len(expected) and {item.get("dispatchKey") for item in orders} == expected and all(item.get("state") == "TERMINAL" for item in orders)):
            self.fail(cleanup_id, "OFFICIAL_TERMINAL_OWNED_CAPITAL_AND_RECONCILIATION_REQUIRED")
            raise KisScopedAuthorityError("KIS cleanup ACK or working-order absence is not terminal reconciliation")
        with closing(self._connect()) as connection, connection:
            connection.execute("UPDATE kis_operational_cleanups SET status='CLEAN',detail='OFFICIAL_TERMINAL_AND_RECONCILIATION_VERIFIED' WHERE cleanup_id=?", (cleanup_id,))

    def fail(self, cleanup_id, reason):
        with closing(self._connect()) as connection, connection:
            connection.execute("UPDATE kis_operational_cleanups SET status='RECONCILIATION_REQUIRED',detail=? WHERE cleanup_id=? AND status!='CLEAN'", (str(reason)[:240], cleanup_id))

    def ensure_recovery_block(self, session_id, reason):
        context = self.consent_context(session_id)
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT cleanup_id FROM kis_operational_cleanups WHERE session_id=?", (session_id,)).fetchone()
        if row is None:
            row = self.reserve(session_id, context, [])
            cleanup_id = row["cleanup_id"]
        else:
            cleanup_id = row[0]
        self.fail(cleanup_id, "OWNED_DISPATCH_CAPTURE_UNAVAILABLE:" + str(reason)[:160])
        return cleanup_id

    def pending_for_account(self, account):
        if not self.operational.path.is_file():
            return []
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT cleanup_id,session_id,context_json,status,detail FROM kis_operational_cleanups WHERE status!='CLEAN'").fetchall()
        return [{"cleanupId": row[0], "sessionId": row[1], "status": row[3], "reason": row[4],
                 "runtimeScopeId": json.loads(row[2])["runtimeScopeId"]}
                for row in rows if json.loads(row[2])["accountFingerprint"] == account]

    def public_status(self):
        if not self.operational.path.is_file():
            return {"schemaVersion": CLEANUP_POLICY["schemaVersion"], "status": "CLEAN", "scopes": []}
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT cleanup_id,session_id,context_json,status,detail,orders_json,truth_json FROM kis_operational_cleanups ORDER BY issued DESC LIMIT 100").fetchall()
        return {"schemaVersion": CLEANUP_POLICY["schemaVersion"], "status": "RECONCILIATION_REQUIRED" if any(row[3] != "CLEAN" for row in rows) else "CLEAN", "scopes": [
            {"cleanupId": row[0], "sessionId": row[1], "runtimeScopeId": json.loads(row[2])["runtimeScopeId"],
             "deploymentId": json.loads(row[2])["deploymentId"], "currency": json.loads(row[2])["capitalCurrency"],
             "status": row[3], "reason": row[4], "requiresReconciliation": row[3] != "CLEAN", "placementBlocked": True,
             "remainingOrderCount": 0 if row[3] == "CLEAN" else len(json.loads(row[5]))} for row in rows]}
