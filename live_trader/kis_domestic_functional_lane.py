from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence

from .kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    ACTIVE_END_LATEST,
    ARMED_LATEST,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    BAR_INTERVAL_MINUTES,
    CLEANUP_END_LATEST,
    KST,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from .program_ledger import ProgramLedger
from trading_runtime.market_calendar import session_bounds_utc


KIS_DOMESTIC_FUNCTIONAL_LANE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_LANE_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_LANE_MUTATION_AVAILABLE = False

_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_OFFICIAL_ID = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_BAR_SOURCE = "KIS_WEBSOCKET_H0STCNT0"
_TRIGGER_SOURCE = "KIS_WEBSOCKET"
_BAR_DOMAIN = b"kis-domestic-functional-bar-window/v1\x00"
_TRIGGER_DOMAIN = b"kis-domestic-functional-next-open/v1\x00"
_GRANT_DOMAIN = b"kis-domestic-functional-graph-grant-instant/v1\x00"
_RECORD_DOMAIN = b"kis-domestic-functional-lane-record/v1\x00"
_ACTION_STATES = {
    "CLAIMED",
    "SUBMITTING",
    "POST_MAY_HAVE_CROSSED",
    "ACKNOWLEDGED",
    "FILLED",
    "NOT_SENT",
}
_NONTERMINAL_ACTION_STATES = {
    "CLAIMED",
    "SUBMITTING",
    "POST_MAY_HAVE_CROSSED",
    "ACKNOWLEDGED",
}
_ZERO_HASH = "0" * 64
_LANE_SCHEMA_VERSION = "kis-domestic-functional-lane-schema/v2"
_SOURCE_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", flags=re.ASCII)
_RAW_SOURCE_SEQUENCE = re.compile(r"^[0-9]{1,20}$", flags=re.ASCII)
_BAR_WINDOW_KEYS = {
    "schemaVersion",
    "route",
    "origin",
    "pdno",
    "source",
    "interval",
    "artifactContentHash",
    "artifactFileSha256",
    "instanceContentHash",
    "instanceFileSha256",
    "sourceProvider",
    "sourceGeneration",
    "firstSourceSequence",
    "lastSourceSequence",
    "sourceEventCount",
    "sourceProofHash",
    "bars",
    "observedAt",
}
_NEXT_OPEN_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "source",
    "eventType",
    "evaluationId",
    "barOpenAt",
    "observedAt",
    "openPriceKrw",
    "sourceProvider",
    "sourceGeneration",
    "sourceSequence",
    "rawEventHash",
    "sourceProofHash",
}
_BAR_KEYS = {
    "openAt",
    "closeAt",
    "open",
    "high",
    "low",
    "close",
    "sourceSequenceStart",
    "sourceSequenceEnd",
    "eventCount",
    "rawEventChainHash",
}
_GRANT_RECEIPT_BODY_KEYS = {
    "schemaVersion", "route", "pdno", "source", "graphTransactionId",
    "graphRequestHash", "graphActionInputsHash", "graphIntentStepHash",
    "expectedStatusRevision", "expectedStatusHeadHash", "ownerEpochHash",
    "registryAcceptedHeadHash", "sessionId", "bootstrapId", "approvalId",
    "evaluationId", "triggerId", "triggerHash", "accountFingerprint",
    "preactivationBaselineHash", "codeManifestHash", "rollingReceiptHash",
    "quoteReceiptHash", "freshQuoteHash", "grantWallAt",
    "grantMonotonicNs", "capturedOnce", "serverAuthorityKeyIdHash",
}
_ACTIVATION_V2_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "sessionId",
    "bootstrapId",
    "bootstrapHash",
    "approvalId",
    "approvalHash",
    "evaluationId",
    "evaluationHash",
    "rawWindowHash",
    "triggerId",
    "triggerHash",
    "rawTriggerHash",
    "triggerBarOpenAt",
    "triggerObservedAt",
    "naturalBuyClaimId",
    "naturalBuyClaimHash",
    "naturalBuyLimitPriceKrw",
    "freshQuoteHash",
    "freshQuoteObservedAt",
    "freshQuotePriceKrw",
    "grantReceiptHash",
    "grantReceiptSignatureHash",
    "grantWallAt",
    "grantMonotonicNs",
    "graphTransactionId",
    "graphRequestHash",
    "graphActionInputsHash",
    "graphIntentStepHash",
    "expectedStatusRevision",
    "expectedStatusHeadHash",
    "ownerEpochHash",
    "registryAcceptedHeadHash",
    "rollingReceiptHash",
    "quoteReceiptHash",
    "accountFingerprint",
    "permitId",
    "permitHash",
    "sessionNonceHash",
    "preactivationBaselineHash",
    "contractEnvelopeHash",
    "codeManifestHash",
    "artifactContentHash",
    "artifactFileSha256",
    "instanceContentHash",
    "instanceFileSha256",
    "quantity",
    "maxOrderKrw",
    "maxGrossKrw",
    "ownerLossMustRemainBelowKrw",
    "activatedAt",
    "activationObservedAt",
    "expiresAt",
    "cleanupEndsAt",
    "activeSeconds",
    "realOrdersEnabled",
    "promotionEligible",
    "serverAuthorityKeyIdHash",
}


class KisDomesticFunctionalLaneBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def sign_kis_domestic_lane_capture(
    server_authority_key: bytes,
    domain: str,
    body: Mapping[str, Any],
) -> str:
    if not isinstance(server_authority_key, bytes) or len(server_authority_key) < 32:
        raise KisDomesticFunctionalLaneBlocked("server authority key is invalid")
    domains = {
        "BAR_WINDOW": _BAR_DOMAIN,
        "NEXT_OPEN": _TRIGGER_DOMAIN,
    }
    prefix = domains.get(domain)
    if prefix is None or not isinstance(body, Mapping):
        raise KisDomesticFunctionalLaneBlocked("capture signing domain is invalid")
    return hmac.new(server_authority_key, prefix + _canonical(body), hashlib.sha256).hexdigest()


def sign_kis_domestic_lane_grant_receipt(
    server_authority_key: bytes,
    body: Mapping[str, Any],
) -> dict[str, Any]:
    """Seal the one instant captured by the graph before lane activation.

    The helper has no authority to activate a lane.  It only produces the
    exact envelope later consumed once by ``activate``; production remains
    unavailable until the graph owns this signer outside the lane process.
    """

    if not isinstance(server_authority_key, bytes) or len(server_authority_key) < 32:
        raise KisDomesticFunctionalLaneBlocked("server authority key is invalid")
    if not isinstance(body, Mapping) or set(body) != _GRANT_RECEIPT_BODY_KEYS:
        raise KisDomesticFunctionalLaneBlocked(
            "graph grant instant receipt body is not exact"
        )
    copied = dict(body)
    record_hash = _hash(copied)
    signature_body = {**copied, "recordHash": record_hash}
    signature = hmac.new(
        server_authority_key,
        _GRANT_DOMAIN + _canonical(signature_body),
        hashlib.sha256,
    ).hexdigest()
    return {"body": copied, "recordHash": record_hash, "signature": signature}


def _signature(key: bytes, body: Mapping[str, Any]) -> str:
    return hmac.new(key, _RECORD_DOMAIN + _canonical(body), hashlib.sha256).hexdigest()


def _require_sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalLaneBlocked(f"{label} must be sha256 hex")
    return value


def _decimal(value: Any, label: str, *, positive: bool = False) -> Decimal:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalLaneBlocked(f"{label} must be canonical decimal text")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise KisDomesticFunctionalLaneBlocked(f"{label} is not decimal") from exc
    if not parsed.is_finite() or format(parsed, "f") != value:
        raise KisDomesticFunctionalLaneBlocked(f"{label} is not canonical finite decimal")
    if parsed < 0 or (positive and parsed <= 0):
        raise KisDomesticFunctionalLaneBlocked(f"{label} is out of range")
    return parsed


def _utc_text(value: datetime, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalLaneBlocked(f"{label} must be timezone-aware")
    epoch = value.timestamp()
    if not math.isfinite(epoch):
        raise KisDomesticFunctionalLaneBlocked(f"{label} is not finite")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalLaneBlocked(f"{label} must be canonical UTC text")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise KisDomesticFunctionalLaneBlocked(f"{label} is invalid") from exc
    if _utc_text(parsed, label) != value:
        raise KisDomesticFunctionalLaneBlocked(f"{label} is not canonical UTC text")
    return parsed


def _official_id(value: Any, label: str) -> str:
    if type(value) is not str or not _OFFICIAL_ID.fullmatch(value):
        raise KisDomesticFunctionalLaneBlocked(
            f"{label} must be exact ASCII digits 1..16"
        )
    return value


def _row_json(row: sqlite3.Row, key: str) -> Mapping[str, Any]:
    try:
        value = json.loads(str(row[key]))
    except (json.JSONDecodeError, TypeError) as exc:
        raise KisDomesticFunctionalLaneBlocked(f"durable {key} is invalid") from exc
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalLaneBlocked(f"durable {key} is not an object")
    return value


def _quoted_sqlite_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sqlite_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return the exact SQLite shape owned by this isolated lane.

    sqlite_master text alone is insufficient when a database has been altered
    through SQLite's writable-schema escape hatch.  The independent PRAGMA
    projections below make column, key, index, partial-index, and foreign-key
    shape part of the manifest as well.  Triggers/views attached to a lane
    table are included in ``objects`` and therefore fail closed.
    """

    objects = [
        {
            "type": str(row[0]),
            "name": str(row[1]),
            "table": str(row[2]),
            "sql": "" if row[3] is None else str(row[3]),
        }
        for row in conn.execute(
            """SELECT type, name, tbl_name, sql
               FROM sqlite_master
               WHERE name LIKE 'kis_functional_%'
                  OR tbl_name LIKE 'kis_functional_%'
               ORDER BY type, name"""
        ).fetchall()
    ]
    table_names = sorted(
        row["name"] for row in objects if row["type"] == "table"
    )
    tables: dict[str, Any] = {}
    for table_name in table_names:
        quoted_table = _quoted_sqlite_identifier(table_name)
        table_info = [
            tuple(row)
            for row in conn.execute(f"PRAGMA table_info({quoted_table})").fetchall()
        ]
        table_xinfo = [
            tuple(row)
            for row in conn.execute(f"PRAGMA table_xinfo({quoted_table})").fetchall()
        ]
        foreign_keys = [
            tuple(row)
            for row in conn.execute(
                f"PRAGMA foreign_key_list({quoted_table})"
            ).fetchall()
        ]
        indexes: list[dict[str, Any]] = []
        for index_row in conn.execute(
            f"PRAGMA index_list({quoted_table})"
        ).fetchall():
            index_name = str(index_row[1])
            quoted_index = _quoted_sqlite_identifier(index_name)
            indexes.append(
                {
                    "name": index_name,
                    "unique": int(index_row[2]),
                    "origin": str(index_row[3]),
                    "partial": int(index_row[4]),
                    "info": [
                        tuple(row)
                        for row in conn.execute(
                            f"PRAGMA index_info({quoted_index})"
                        ).fetchall()
                    ],
                    "xinfo": [
                        tuple(row)
                        for row in conn.execute(
                            f"PRAGMA index_xinfo({quoted_index})"
                        ).fetchall()
                    ],
                }
            )
        indexes.sort(key=lambda row: row["name"])
        tables[table_name] = {
            "tableInfo": table_info,
            "tableXinfo": table_xinfo,
            "foreignKeys": foreign_keys,
            "indexes": indexes,
        }
    return {"objects": objects, "tables": tables}


class DurableKisDomesticFunctionalLane:
    """Fail-closed KIS lane state stored inside the shared ProgramLedger DB.

    This module has no HTTP or broker sender surface.  It only seals authority,
    evaluates already authenticated raw bars, and records one BUY plus at most
    one cleanup SELL.  Production flags remain false until a state-owned graph
    supplies durable key custody, raw readers, fences, and mutation edges.
    """

    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        server_authority_key: bytes,
        server_authority_key_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalLaneBlocked("exact ProgramLedger is required")
        if not isinstance(server_authority_key, bytes) or len(server_authority_key) < 32:
            raise KisDomesticFunctionalLaneBlocked("server authority key is invalid")
        if type(server_authority_key_id) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", server_authority_key_id
        ):
            raise KisDomesticFunctionalLaneBlocked("server authority key id is invalid")
        self.program_ledger = program_ledger
        self._key = bytes(server_authority_key)
        self._key_id_hash = hashlib.sha256(server_authority_key_id.encode("utf-8")).hexdigest()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_schema()

    def _now(self) -> datetime:
        value = self.clock()
        _utc_text(value, "lane clock")
        return value.astimezone(timezone.utc)

    def _ensure_schema(self) -> None:
        with self.program_ledger.connection() as conn:
            schema_sql = (
                """
                CREATE TABLE IF NOT EXISTS kis_functional_schema_manifest (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    ddl_hash TEXT NOT NULL,
                    migration_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_public_arm (
                    arm_id TEXT PRIMARY KEY,
                    route TEXT NOT NULL,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revision INTEGER NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS kis_functional_public_arm_active_idx
                    ON kis_functional_public_arm(route)
                    WHERE state='ARMED_WAIT_PUBLIC';
                CREATE TABLE IF NOT EXISTS kis_functional_bootstrap (
                    route TEXT PRIMARY KEY,
                    bootstrap_id TEXT NOT NULL UNIQUE,
                    public_arm_id TEXT NOT NULL UNIQUE,
                    evaluation_id TEXT NOT NULL UNIQUE,
                    trigger_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    preactivation_baseline_hash TEXT NOT NULL,
                    approval_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_approval (
                    approval_id TEXT PRIMARY KEY,
                    bootstrap_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    session_id TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_evaluation (
                    evaluation_id TEXT PRIMARY KEY,
                    public_arm_id TEXT NOT NULL,
                    bootstrap_id TEXT NOT NULL DEFAULT '',
                    approval_id TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    raw_window_json TEXT NOT NULL,
                    raw_window_hash TEXT NOT NULL UNIQUE,
                    raw_window_signature TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_next_open (
                    trigger_id TEXT PRIMARY KEY,
                    evaluation_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    raw_trigger_json TEXT NOT NULL,
                    raw_trigger_hash TEXT NOT NULL UNIQUE,
                    raw_trigger_signature TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_session (
                    session_id TEXT PRIMARY KEY,
                    bootstrap_id TEXT NOT NULL UNIQUE,
                    approval_id TEXT NOT NULL UNIQUE,
                    evaluation_id TEXT NOT NULL UNIQUE,
                    trigger_id TEXT NOT NULL UNIQUE,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    preactivation_baseline_hash TEXT NOT NULL,
                    contract_envelope_hash TEXT NOT NULL,
                    code_manifest_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    activated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    cleanup_ends_at TEXT NOT NULL,
                    grant_monotonic_ns INTEGER NOT NULL CHECK(grant_monotonic_ns>=0),
                    grant_receipt_json TEXT NOT NULL,
                    grant_receipt_hash TEXT NOT NULL UNIQUE,
                    grant_receipt_signature TEXT NOT NULL,
                    activation_record_json TEXT NOT NULL,
                    activation_record_hash TEXT NOT NULL UNIQUE,
                    activation_signature TEXT NOT NULL,
                    cleanup_started_at TEXT NOT NULL DEFAULT '',
                    cleanup_reason TEXT NOT NULL DEFAULT '',
                    final_evidence_json TEXT NOT NULL DEFAULT '',
                    final_evidence_hash TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kis_functional_action (
                    claim_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    limit_price_krw TEXT NOT NULL,
                    gross_krw TEXT NOT NULL,
                    evaluation_id TEXT NOT NULL DEFAULT '',
                    trigger_id TEXT NOT NULL DEFAULT '',
                    broker_order_id TEXT NOT NULL DEFAULT '',
                    org_no TEXT NOT NULL DEFAULT '',
                    order_date TEXT NOT NULL DEFAULT '',
                    fill_price_krw TEXT NOT NULL DEFAULT '',
                    fee_krw TEXT NOT NULL DEFAULT '',
                    tax_krw TEXT NOT NULL DEFAULT '',
                    loan_interest_krw TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    post_boundary_at TEXT NOT NULL DEFAULT '',
                    acknowledged_at TEXT NOT NULL DEFAULT '',
                    filled_at TEXT NOT NULL DEFAULT '',
                    program_execution_event_id TEXT NOT NULL DEFAULT '',
                    record_hash TEXT NOT NULL,
                    transition_head_hash TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    UNIQUE(session_id, action_kind)
                );
                CREATE TABLE IF NOT EXISTS kis_functional_action_transition (
                    claim_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    record_hash TEXT NOT NULL UNIQUE,
                    signature TEXT NOT NULL,
                    PRIMARY KEY(claim_id, revision)
                );
                CREATE INDEX IF NOT EXISTS kis_functional_action_session_idx
                    ON kis_functional_action(session_id, created_at, claim_id);
                """
            )
            statements = [
                statement.strip()
                for statement in schema_sql.split(";")
                if statement.strip()
            ]
            expected_conn = sqlite3.connect(":memory:")
            try:
                for statement in statements:
                    expected_conn.execute(statement)
                expected_snapshot = _sqlite_schema_snapshot(expected_conn)
            finally:
                expected_conn.close()
            expected_hash = _hash(expected_snapshot)
            ddl_hash = hashlib.sha256(schema_sql.encode("utf-8")).hexdigest()
            migration_hash = _hash(
                {
                    "schemaVersion": _LANE_SCHEMA_VERSION,
                    "schemaHash": expected_hash,
                    "ddlHash": ddl_hash,
                    "migration": "FRESH_CREATE_ONLY_NO_IMPLICIT_REPAIR",
                }
            )

            preexisting_snapshot = _sqlite_schema_snapshot(conn)
            preexisting_objects = preexisting_snapshot["objects"]
            if preexisting_objects and not hmac.compare_digest(
                _hash(preexisting_snapshot), expected_hash
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "durable KIS lane SQLite schema fingerprint mismatch"
                )

            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                conn.execute(statement)
            actual_snapshot = _sqlite_schema_snapshot(conn)
            if not hmac.compare_digest(_hash(actual_snapshot), expected_hash):
                raise KisDomesticFunctionalLaneBlocked(
                    "durable KIS lane SQLite schema fingerprint mismatch"
                )

            manifest_rows = conn.execute(
                """SELECT singleton, schema_version, schema_hash, ddl_hash,
                          migration_hash
                   FROM kis_functional_schema_manifest"""
            ).fetchall()
            expected_manifest = (
                1,
                _LANE_SCHEMA_VERSION,
                expected_hash,
                ddl_hash,
                migration_hash,
            )
            if not manifest_rows:
                conn.execute(
                    """INSERT INTO kis_functional_schema_manifest
                       (singleton, schema_version, schema_hash, ddl_hash,
                        migration_hash)
                       VALUES (?, ?, ?, ?, ?)""",
                    expected_manifest,
                )
            elif len(manifest_rows) != 1 or tuple(manifest_rows[0]) != expected_manifest:
                raise KisDomesticFunctionalLaneBlocked(
                    "durable KIS lane schema version/migration manifest mismatch"
                )

    def _signed_record(self, body: Mapping[str, Any]) -> tuple[str, str, str]:
        text = _canonical(body).decode("utf-8")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return text, digest, _signature(self._key, body)

    def _verify_stored_record(self, row: sqlite3.Row) -> Mapping[str, Any]:
        record = _row_json(row, "record_json")
        record_hash = str(row["record_hash"])
        signature = str(row["signature"])
        if not hmac.compare_digest(record_hash, _hash(record)) or not hmac.compare_digest(
            signature, _signature(self._key, record)
        ):
            raise KisDomesticFunctionalLaneBlocked("durable signed record failed verification")
        return record

    def _verify_raw_capture(
        self,
        row: sqlite3.Row,
        *,
        json_column: str,
        hash_column: str,
        signature_column: str,
        domain: str,
    ) -> Mapping[str, Any]:
        raw = _row_json(row, json_column)
        raw_hash = str(row[hash_column])
        raw_signature = str(row[signature_column])
        if (
            not hmac.compare_digest(raw_hash, _hash(raw))
            or not _SHA256.fullmatch(raw_signature)
            or not hmac.compare_digest(
                raw_signature,
                sign_kis_domestic_lane_capture(self._key, domain, raw),
            )
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "durable raw authenticated capture failed verification"
            )
        return raw

    def _verify_grant_receipt(
        self, receipt_value: Any
    ) -> tuple[Mapping[str, Any], str, str]:
        if not isinstance(receipt_value, Mapping) or set(receipt_value) != {
            "body", "recordHash", "signature"
        }:
            raise KisDomesticFunctionalLaneBlocked(
                "trusted graph grant instant receipt is required; legacy backdated activation is unavailable"
            )
        body = receipt_value.get("body")
        if not isinstance(body, Mapping) or set(body) != _GRANT_RECEIPT_BODY_KEYS:
            raise KisDomesticFunctionalLaneBlocked(
                "graph grant instant receipt body is not exact"
            )
        body = dict(body)
        record_hash = _require_sha(
            receipt_value.get("recordHash"), "grant receipt hash"
        )
        signature = _require_sha(
            receipt_value.get("signature"), "grant receipt signature"
        )
        signature_body = {**body, "recordHash": record_hash}
        expected_signature = hmac.new(
            self._key,
            _GRANT_DOMAIN + _canonical(signature_body),
            hashlib.sha256,
        ).hexdigest()
        if (
            not hmac.compare_digest(record_hash, _hash(body))
            or not hmac.compare_digest(signature, expected_signature)
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "graph grant instant receipt signature is invalid"
            )
        if not (
            body.get("schemaVersion")
            == "kis-domestic-functional-lane-grant-instant/v1"
            and body.get("route") == ROUTE
            and body.get("pdno") == PDNO
            and body.get("source") == "KIS_DOMESTIC_FUNCTIONAL_GRAPH_V2"
            and type(body.get("capturedOnce")) is bool
            and body.get("capturedOnce") is True
            and body.get("serverAuthorityKeyIdHash") == self._key_id_hash
            and type(body.get("grantMonotonicNs")) is int
            and body.get("grantMonotonicNs") >= 0
            and type(body.get("expectedStatusRevision")) is int
            and body.get("expectedStatusRevision") >= 1
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "graph grant instant receipt authority is invalid"
            )
        for key in (
            "graphRequestHash", "graphActionInputsHash", "graphIntentStepHash",
            "expectedStatusHeadHash", "ownerEpochHash",
            "registryAcceptedHeadHash", "triggerHash", "accountFingerprint",
            "preactivationBaselineHash", "codeManifestHash",
            "rollingReceiptHash", "quoteReceiptHash", "freshQuoteHash",
        ):
            _require_sha(body.get(key), f"grant receipt {key}")
        _parse_utc(body.get("grantWallAt"), "grant receipt grantWallAt")
        for key in (
            "graphTransactionId", "sessionId", "bootstrapId", "approvalId",
            "evaluationId", "triggerId",
        ):
            if type(body.get(key)) is not str or not body[key]:
                raise KisDomesticFunctionalLaneBlocked(
                    f"grant receipt {key} is invalid"
                )
        return body, record_hash, signature

    def _verify_stored_grant_receipt(
        self, row: sqlite3.Row
    ) -> tuple[Mapping[str, Any], str, str]:
        try:
            body = json.loads(str(row["grant_receipt_json"]))
        except (json.JSONDecodeError, TypeError) as exc:
            raise KisDomesticFunctionalLaneBlocked(
                "durable graph grant instant receipt is invalid"
            ) from exc
        return self._verify_grant_receipt(
            {
                "body": body,
                "recordHash": str(row["grant_receipt_hash"]),
                "signature": str(row["grant_receipt_signature"]),
            }
        )

    def _verify_session_activation(self, row: sqlite3.Row) -> Mapping[str, Any]:
        grant, grant_hash, grant_signature = self._verify_stored_grant_receipt(row)
        activation = _row_json(row, "activation_record_json")
        if set(activation) != _ACTIVATION_V2_KEYS:
            raise KisDomesticFunctionalLaneBlocked(
                "durable activation schema is not exact"
            )
        activation_hash = str(row["activation_record_hash"])
        activation_signature = str(row["activation_signature"])
        activated = _parse_utc(activation.get("activatedAt"), "activation.activatedAt")
        observed = _parse_utc(
            activation.get("activationObservedAt"),
            "activation.activationObservedAt",
        )
        expires = _parse_utc(activation.get("expiresAt"), "activation.expiresAt")
        trigger_observed = _parse_utc(
            activation.get("triggerObservedAt"), "activation.triggerObservedAt"
        )
        trigger_bar_open = _parse_utc(
            activation.get("triggerBarOpenAt"), "activation.triggerBarOpenAt"
        )
        if (
            not hmac.compare_digest(activation_hash, _hash(activation))
            or not hmac.compare_digest(
                activation_signature, _signature(self._key, activation)
            )
            or activation.get("sessionId") != str(row["session_id"])
            or activation.get("bootstrapId") != str(row["bootstrap_id"])
            or activation.get("approvalId") != str(row["approval_id"])
            or activation.get("evaluationId") != str(row["evaluation_id"])
            or activation.get("triggerId") != str(row["trigger_id"])
            or activation.get("permitId") != str(row["permit_id"])
            or activation.get("permitHash") != str(row["permit_hash"])
            or activation.get("accountFingerprint")
            != str(row["account_fingerprint"])
            or activation.get("preactivationBaselineHash")
            != str(row["preactivation_baseline_hash"])
            or activation.get("contractEnvelopeHash")
            != str(row["contract_envelope_hash"])
            or activation.get("codeManifestHash")
            != str(row["code_manifest_hash"])
            or activation.get("activatedAt") != str(row["activated_at"])
            or activation.get("expiresAt") != str(row["expires_at"])
            or activation.get("cleanupEndsAt") != str(row["cleanup_ends_at"])
            or activation.get("schemaVersion")
            != "kis-domestic-functional-activation/v2"
            or activation.get("grantReceiptHash") != grant_hash
            or activation.get("grantReceiptSignatureHash")
            != hashlib.sha256(grant_signature.encode("ascii")).hexdigest()
            or activation.get("grantWallAt") != grant.get("grantWallAt")
            or activation.get("grantMonotonicNs") != grant.get("grantMonotonicNs")
            or activation.get("activatedAt") != grant.get("grantWallAt")
            or int(row["grant_monotonic_ns"]) != grant.get("grantMonotonicNs")
            or observed != activated
            or expires - activated != timedelta(seconds=ACTIVE_SECONDS)
            or activation.get("activeSeconds") != ACTIVE_SECONDS
            or trigger_bar_open > trigger_observed
            or trigger_observed > activated
            or activated > trigger_observed + timedelta(seconds=2)
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "durable activation reseal failed verification"
            )
        return activation

    def _insert_action_transition(
        self,
        conn: sqlite3.Connection,
        *,
        claim_id: str,
        session_id: str,
        action_kind: str,
        revision: int,
        state: str,
        occurred_at: str,
        previous_hash: str,
        details: Mapping[str, Any],
    ) -> str:
        body = {
            "schemaVersion": "kis-domestic-functional-action-transition/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "claimId": claim_id,
            "sessionId": session_id,
            "actionKind": action_kind,
            "revision": revision,
            "state": state,
            "occurredAt": occurred_at,
            "previousHash": previous_hash,
            "details": dict(details),
            "promotionEligible": False,
        }
        record_json, record_hash, signature = self._signed_record(body)
        conn.execute(
            """INSERT INTO kis_functional_action_transition
            (claim_id, revision, state, occurred_at, previous_hash,
             record_json, record_hash, signature)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                claim_id,
                revision,
                state,
                occurred_at,
                previous_hash,
                record_json,
                record_hash,
                signature,
            ),
        )
        return record_hash

    def _verify_action_chain(
        self, conn: sqlite3.Connection, action: sqlite3.Row
    ) -> list[Mapping[str, Any]]:
        rows = conn.execute(
            """SELECT * FROM kis_functional_action_transition
               WHERE claim_id=? ORDER BY revision""",
            (str(action["claim_id"]),),
        ).fetchall()
        if len(rows) != int(action["revision"]):
            raise KisDomesticFunctionalLaneBlocked(
                "action transition history is incomplete"
            )
        previous_hash = _ZERO_HASH
        previous_state = ""
        previous_time: datetime | None = None
        records: list[Mapping[str, Any]] = []
        allowed = {
            "": {"CLAIMED"},
            "CLAIMED": {"SUBMITTING", "NOT_SENT"},
            "SUBMITTING": {"POST_MAY_HAVE_CROSSED", "NOT_SENT"},
            "POST_MAY_HAVE_CROSSED": {"ACKNOWLEDGED"},
            "ACKNOWLEDGED": {"FILLED"},
        }
        for expected_revision, transition in enumerate(rows, 1):
            record = self._verify_stored_record(transition)
            occurred = _parse_utc(record.get("occurredAt"), "transition.occurredAt")
            if (
                int(transition["revision"]) != expected_revision
                or record.get("revision") != expected_revision
                or record.get("claimId") != str(action["claim_id"])
                or record.get("sessionId") != str(action["session_id"])
                or record.get("actionKind") != str(action["action_kind"])
                or record.get("state") != str(transition["state"])
                or record.get("occurredAt") != str(transition["occurred_at"])
                or record.get("previousHash") != previous_hash
                or str(transition["previous_hash"]) != previous_hash
                or record.get("state") not in allowed.get(previous_state, set())
                or (previous_time is not None and occurred < previous_time)
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "action transition hash-chain is invalid"
                )
            previous_hash = str(transition["record_hash"])
            previous_state = str(transition["state"])
            previous_time = occurred
            records.append(record)
        if (
            previous_hash != str(action["transition_head_hash"])
            or previous_state != str(action["state"])
            or records[0].get("details", {}).get("claimRecordHash")
            != str(action["record_hash"])
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "action transition head does not match durable action"
            )
        return records

    def arm_public_wait(
        self,
        *,
        account_fingerprint: str,
        permit_id: str,
        permit_hash: str,
        session_nonce_hash: str,
        preactivation_baseline_hash: str,
        operator_id: str,
        operator_confirmation_hash: str,
        contract_envelope_hash: str,
        code_manifest_hash: str,
        arm_ttl_seconds: int = 4 * 60 * 60,
    ) -> dict[str, Any]:
        """Seal a public-data-only wait without minting account/order authority."""

        account = _require_sha(account_fingerprint, "account fingerprint")
        permit_digest = _require_sha(permit_hash, "permit hash")
        nonce = _require_sha(session_nonce_hash, "session nonce hash")
        baseline = _require_sha(
            preactivation_baseline_hash, "preactivation baseline hash"
        )
        confirmation = _require_sha(
            operator_confirmation_hash, "operator confirmation hash"
        )
        contract = _require_sha(contract_envelope_hash, "contract envelope hash")
        code = _require_sha(code_manifest_hash, "code manifest hash")
        if type(permit_id) is not str or not permit_id:
            raise KisDomesticFunctionalLaneBlocked("permit id is missing")
        if type(operator_id) is not str or not re.fullmatch(
            r"[A-Za-z0-9._@-]{1,128}", operator_id
        ):
            raise KisDomesticFunctionalLaneBlocked("operator id is invalid")
        if type(arm_ttl_seconds) is not int or not 60 <= arm_ttl_seconds <= 6 * 60 * 60:
            raise KisDomesticFunctionalLaneBlocked("public arm TTL is invalid")
        now = self._now()
        arm_id = f"kis-public-arm-{uuid.uuid4().hex}"
        expires = now + timedelta(seconds=arm_ttl_seconds)
        body = {
            "schemaVersion": "kis-domestic-functional-public-arm/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "publicDataOnly": True,
            "accountAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "accountFingerprint": account,
            "permitId": permit_id,
            "permitHash": permit_digest,
            "sessionNonceHash": nonce,
            "preactivationBaselineHash": baseline,
            "operatorId": operator_id,
            "operatorConfirmationHash": confirmation,
            "contractEnvelopeHash": contract,
            "codeManifestHash": code,
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "quantity": ORDER_QUANTITY,
            "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
            "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
            "ownerLossMustRemainBelowKrw": format(
                OWNER_LOSS_LIMIT_KRW, "f"
            ),
            "activeSeconds": ACTIVE_SECONDS,
            "armId": arm_id,
            "armedAt": _utc_text(now, "armedAt"),
            "expiresAt": _utc_text(expires, "expiresAt"),
            "promotionEligible": False,
            "serverAuthorityKeyIdHash": self._key_id_hash,
        }
        record_json, record_hash, signature = self._signed_record(body)
        try:
            with self.program_ledger.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """INSERT INTO kis_functional_public_arm
                    (arm_id, route, state, record_json, record_hash, signature,
                     expires_at, revision)
                    VALUES (?, ?, 'ARMED_WAIT_PUBLIC', ?, ?, ?, ?, 1)""",
                    (
                        arm_id,
                        ROUTE,
                        record_json,
                        record_hash,
                        signature,
                        _utc_text(expires, "expiresAt"),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise KisDomesticFunctionalLaneBlocked(
                "a public KIS natural-signal wait is already armed"
            ) from exc
        return {"body": body, "recordHash": record_hash, "signature": signature}

    def expire_public_wait(self, *, arm_id: str, expected_revision: int) -> dict[str, Any]:
        now = self._now()
        with self.program_ledger.connection() as conn:
            row = conn.execute(
                "SELECT * FROM kis_functional_public_arm WHERE arm_id=?", (arm_id,)
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalLaneBlocked("public arm is missing")
            self._verify_stored_record(row)
            if now < _parse_utc(row["expires_at"], "publicArm.expiresAt"):
                raise KisDomesticFunctionalLaneBlocked("public arm has not expired")
            changed = conn.execute(
                """UPDATE kis_functional_public_arm
                   SET state='EXPIRED_NO_AUTHORITY', revision=revision+1
                   WHERE arm_id=? AND state='ARMED_WAIT_PUBLIC' AND revision=?""",
                (arm_id, expected_revision),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalLaneBlocked("public arm expiry CAS failed")
        return {
            "armId": arm_id,
            "state": "EXPIRED_NO_AUTHORITY",
            "bootstrapEverIssued": False,
            "orderAuthorityEverAvailable": False,
        }

    def issue_bootstrap(
        self,
        *,
        public_arm_id: str,
        evaluation_id: str,
        trigger_id: str,
        account_fingerprint: str,
        permit_id: str,
        permit_hash: str,
        session_nonce_hash: str,
        preactivation_baseline_hash: str,
        contract_envelope_hash: str,
        code_manifest_hash: str,
        approval_ttl_seconds: int = 300,
    ) -> dict[str, Any]:
        account = _require_sha(account_fingerprint, "account fingerprint")
        permit_digest = _require_sha(permit_hash, "permit hash")
        nonce = _require_sha(session_nonce_hash, "session nonce hash")
        baseline = _require_sha(
            preactivation_baseline_hash, "preactivation baseline hash"
        )
        contract = _require_sha(contract_envelope_hash, "contract envelope hash")
        code = _require_sha(code_manifest_hash, "code manifest hash")
        if type(permit_id) is not str or not permit_id:
            raise KisDomesticFunctionalLaneBlocked("permit id is missing")
        if type(approval_ttl_seconds) is not int or not 30 <= approval_ttl_seconds <= 300:
            raise KisDomesticFunctionalLaneBlocked("approval TTL is invalid")
        now = self._now()
        expires = now + timedelta(seconds=approval_ttl_seconds)
        bootstrap_id = f"kis-bootstrap-{uuid.uuid4().hex}"
        body = {
            "schemaVersion": "kis-domestic-functional-bootstrap/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "publicArmId": public_arm_id,
            "evaluationId": evaluation_id,
            "triggerId": trigger_id,
            "accountFingerprint": account,
            "permitId": permit_id,
            "permitHash": permit_digest,
            "sessionNonceHash": nonce,
            "preactivationBaselineHash": baseline,
            "contractEnvelopeHash": contract,
            "codeManifestHash": code,
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "quantity": ORDER_QUANTITY,
            "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
            "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
            "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
            "activeSeconds": ACTIVE_SECONDS,
            "promotionEligible": False,
            "bootstrapId": bootstrap_id,
            "issuedAt": _utc_text(now, "issuedAt"),
            "expiresAt": _utc_text(expires, "expiresAt"),
            "serverAuthorityKeyIdHash": self._key_id_hash,
        }
        record_json, record_hash, signature = self._signed_record(body)
        try:
            with self.program_ledger.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                arm_row = conn.execute(
                    "SELECT * FROM kis_functional_public_arm WHERE arm_id=?",
                    (public_arm_id,),
                ).fetchone()
                evaluation_row = conn.execute(
                    "SELECT * FROM kis_functional_evaluation WHERE evaluation_id=?",
                    (evaluation_id,),
                ).fetchone()
                trigger_row = conn.execute(
                    "SELECT * FROM kis_functional_next_open WHERE trigger_id=?",
                    (trigger_id,),
                ).fetchone()
                if (
                    arm_row is None
                    or evaluation_row is None
                    or trigger_row is None
                    or str(arm_row["state"]) != "ARMED_WAIT_PUBLIC"
                    or str(evaluation_row["state"]) != "SEALED_PUBLIC"
                    or str(trigger_row["state"]) != "SEALED_PUBLIC"
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural public proof is not ready for authority issuance"
                    )
                arm = self._verify_stored_record(arm_row)
                evaluation = self._verify_stored_record(evaluation_row)
                trigger = self._verify_stored_record(trigger_row)
                if (
                    now >= _parse_utc(arm["expiresAt"], "publicArm.expiresAt")
                    or str(evaluation_row["public_arm_id"]) != public_arm_id
                    or evaluation["signal"] != "BUY"
                    or trigger["evaluationId"] != evaluation_id
                    or evaluation["contractEnvelopeHash"] != contract
                    or evaluation["codeManifestHash"] != code
                    or arm["accountFingerprint"] != account
                    or arm["permitId"] != permit_id
                    or arm["permitHash"] != permit_digest
                    or arm["sessionNonceHash"] != nonce
                    or arm["preactivationBaselineHash"] != baseline
                    or arm["contractEnvelopeHash"] != contract
                    or arm["codeManifestHash"] != code
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural public proof authority lineage mismatch"
                    )
                body.update(
                    {
                        "publicArmHash": str(arm_row["record_hash"]),
                        "evaluationHash": str(evaluation_row["record_hash"]),
                        "triggerHash": str(trigger_row["record_hash"]),
                        "rawWindowHash": str(evaluation_row["raw_window_hash"]),
                        "rawTriggerHash": str(trigger_row["raw_trigger_hash"]),
                        "operatorId": arm["operatorId"],
                        "operatorConfirmationHash": arm[
                            "operatorConfirmationHash"
                        ],
                        "preapprovedAt": arm["armedAt"],
                        "preapprovalExpiresAt": arm["expiresAt"],
                    }
                )
                record_json, record_hash, signature = self._signed_record(body)
                conn.execute(
                    """INSERT INTO kis_functional_bootstrap
                    (route, bootstrap_id, public_arm_id, evaluation_id,
                     trigger_id, state, record_json, record_hash,
                     signature, preactivation_baseline_hash, revision)
                    VALUES (?, ?, ?, ?, ?, 'ISSUED', ?, ?, ?, ?, 1)""",
                    (
                        ROUTE,
                        bootstrap_id,
                        public_arm_id,
                        evaluation_id,
                        trigger_id,
                        record_json,
                        record_hash,
                        signature,
                        baseline,
                    ),
                )
                changed = conn.execute(
                    """UPDATE kis_functional_public_arm
                       SET state='AUTHORITY_ISSUED', revision=revision+1
                       WHERE arm_id=? AND state='ARMED_WAIT_PUBLIC' AND revision=?""",
                    (public_arm_id, int(arm_row["revision"])),
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalLaneBlocked(
                        "public arm authority issuance CAS failed"
                    )
        except sqlite3.IntegrityError as exc:
            raise KisDomesticFunctionalLaneBlocked(
                "route-global KIS bootstrap was already issued and cannot be reused"
            ) from exc
        return {"body": body, "recordHash": record_hash, "signature": signature}

    def approve_bootstrap(
        self,
        *,
        bootstrap_id: str,
        bootstrap_hash: str,
        operator_id: str,
        operator_confirmation_hash: str,
    ) -> dict[str, Any]:
        if type(operator_id) is not str or not re.fullmatch(r"[A-Za-z0-9._@-]{1,128}", operator_id):
            raise KisDomesticFunctionalLaneBlocked("operator id is invalid")
        now = self._now()
        confirmation = _require_sha(
            operator_confirmation_hash, "operator confirmation hash"
        )
        approval_id = f"kis-approval-{uuid.uuid4().hex}"
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM kis_functional_bootstrap WHERE bootstrap_id = ?",
                (bootstrap_id,),
            ).fetchone()
            if row is None or str(row["state"]) != "ISSUED":
                raise KisDomesticFunctionalLaneBlocked("bootstrap is not exactly ISSUED")
            bootstrap = self._verify_stored_record(row)
            if not hmac.compare_digest(str(row["record_hash"]), _require_sha(bootstrap_hash, "bootstrap hash")):
                raise KisDomesticFunctionalLaneBlocked("bootstrap hash mismatch")
            if now >= _parse_utc(bootstrap["expiresAt"], "bootstrap.expiresAt"):
                raise KisDomesticFunctionalLaneBlocked("bootstrap expired")
            if (
                bootstrap.get("operatorId") != operator_id
                or bootstrap.get("operatorConfirmationHash") != confirmation
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "bootstrap approval is not the pre-signal typed approval"
                )
            body = {
                "schemaVersion": "kis-domestic-functional-approval/v1",
                "route": ROUTE,
                "approvalId": approval_id,
                "bootstrapId": bootstrap_id,
                "bootstrapHash": str(row["record_hash"]),
                "publicArmId": bootstrap["publicArmId"],
                "publicArmHash": bootstrap["publicArmHash"],
                "evaluationId": bootstrap["evaluationId"],
                "evaluationHash": bootstrap["evaluationHash"],
                "rawWindowHash": bootstrap["rawWindowHash"],
                "triggerId": bootstrap["triggerId"],
                "triggerHash": bootstrap["triggerHash"],
                "rawTriggerHash": bootstrap["rawTriggerHash"],
                "accountFingerprint": bootstrap["accountFingerprint"],
                "permitId": bootstrap["permitId"],
                "permitHash": bootstrap["permitHash"],
                "sessionNonceHash": bootstrap["sessionNonceHash"],
                "preactivationBaselineHash": bootstrap["preactivationBaselineHash"],
                "contractEnvelopeHash": bootstrap["contractEnvelopeHash"],
                "codeManifestHash": bootstrap["codeManifestHash"],
                "artifactContentHash": bootstrap["artifactContentHash"],
                "artifactFileSha256": bootstrap["artifactFileSha256"],
                "instanceContentHash": bootstrap["instanceContentHash"],
                "instanceFileSha256": bootstrap["instanceFileSha256"],
                "quantity": bootstrap["quantity"],
                "maxOrderKrw": bootstrap["maxOrderKrw"],
                "maxGrossKrw": bootstrap["maxGrossKrw"],
                "ownerLossMustRemainBelowKrw": bootstrap[
                    "ownerLossMustRemainBelowKrw"
                ],
                "activeSeconds": bootstrap["activeSeconds"],
                "operatorId": operator_id,
                "operatorConfirmationHash": confirmation,
                "approvedAt": _utc_text(now, "approvedAt"),
                "expiresAt": bootstrap["expiresAt"],
                "promotionEligible": False,
            }
            record_json, record_hash, signature = self._signed_record(body)
            conn.execute(
                """INSERT INTO kis_functional_approval
                (approval_id, bootstrap_id, state, record_json, record_hash,
                 signature, revision) VALUES (?, ?, 'APPROVED', ?, ?, ?, 1)""",
                (approval_id, bootstrap_id, record_json, record_hash, signature),
            )
            changed = conn.execute(
                """UPDATE kis_functional_bootstrap
                   SET state='APPROVED', approval_id=?, revision=revision+1
                   WHERE bootstrap_id=? AND state='ISSUED' AND revision=?""",
                (approval_id, bootstrap_id, int(row["revision"])),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalLaneBlocked("bootstrap approval CAS failed")
        return {"body": body, "recordHash": record_hash, "signature": signature}

    def record_breakout_evaluation(
        self,
        *,
        public_arm_id: str,
        window_body: Mapping[str, Any],
        server_authority_signature: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(window_body, Mapping)
            or type(server_authority_signature) is not str
            or not _SHA256.fullmatch(server_authority_signature)
            or not hmac.compare_digest(
            sign_kis_domestic_lane_capture(self._key, "BAR_WINDOW", window_body),
            server_authority_signature,
            )
        ):
            raise KisDomesticFunctionalLaneBlocked("official bar window signature mismatch")
        if set(window_body) != _BAR_WINDOW_KEYS:
            raise KisDomesticFunctionalLaneBlocked("official bar window fields are not exact")
        exact = {
            "schemaVersion": "kis-domestic-official-5m-window/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "source": _BAR_SOURCE,
            "interval": "5m",
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
        }
        for key, expected in exact.items():
            if type(window_body.get(key)) is not type(expected) or window_body.get(key) != expected:
                raise KisDomesticFunctionalLaneBlocked(f"bar window {key} mismatch")
        if (
            window_body.get("sourceProvider") != "kis"
            or type(window_body.get("sourceGeneration")) is not str
            or not _SOURCE_GENERATION.fullmatch(window_body["sourceGeneration"])
            or type(window_body.get("firstSourceSequence")) is not str
            or not _RAW_SOURCE_SEQUENCE.fullmatch(window_body["firstSourceSequence"])
            or type(window_body.get("lastSourceSequence")) is not str
            or not _RAW_SOURCE_SEQUENCE.fullmatch(window_body["lastSourceSequence"])
            or int(window_body["firstSourceSequence"])
            > int(window_body["lastSourceSequence"])
            or type(window_body.get("sourceEventCount")) is not int
            or window_body["sourceEventCount"] < 11
            or type(window_body.get("sourceProofHash")) is not str
            or not _SHA256.fullmatch(window_body["sourceProofHash"])
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "H0STCNT0 source proof metadata is invalid"
            )
        bars = window_body.get("bars")
        if not isinstance(bars, list) or len(bars) != 11:
            raise KisDomesticFunctionalLaneBlocked("exactly 11 finalized bars are required")
        parsed: list[dict[str, Any]] = []
        for index, value in enumerate(bars):
            if not isinstance(value, Mapping) or set(value) != _BAR_KEYS:
                raise KisDomesticFunctionalLaneBlocked(f"bar[{index}] fields are not exact")
            opened = _parse_utc(value["openAt"], f"bar[{index}].openAt")
            closed = _parse_utc(value["closeAt"], f"bar[{index}].closeAt")
            if closed - opened != timedelta(minutes=BAR_INTERVAL_MINUTES):
                raise KisDomesticFunctionalLaneBlocked("bar duration is not exactly five minutes")
            if index and opened != parsed[-1]["closed"]:
                raise KisDomesticFunctionalLaneBlocked("official bars are not contiguous")
            start_sequence = value["sourceSequenceStart"]
            end_sequence = value["sourceSequenceEnd"]
            event_count = value["eventCount"]
            if (
                type(start_sequence) is not str
                or not _RAW_SOURCE_SEQUENCE.fullmatch(start_sequence)
                or type(end_sequence) is not str
                or not _RAW_SOURCE_SEQUENCE.fullmatch(end_sequence)
                or int(start_sequence) > int(end_sequence)
                or type(event_count) is not int
                or event_count < 1
                or type(value["rawEventChainHash"]) is not str
                or not _SHA256.fullmatch(value["rawEventChainHash"])
                or (
                    index
                    and int(start_sequence)
                    <= int(parsed[-1]["sourceSequenceEnd"])
                )
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "H0STCNT0 bar source sequence proof is invalid"
                )
            prices = {
                name: _decimal(value[name], f"bar[{index}].{name}", positive=True)
                for name in ("open", "high", "low", "close")
            }
            if prices["low"] > min(prices["open"], prices["close"]) or prices["high"] < max(
                prices["open"], prices["close"]
            ) or prices["high"] < prices["low"]:
                raise KisDomesticFunctionalLaneBlocked("bar OHLC relationship is invalid")
            parsed.append(
                {
                    "opened": opened,
                    "closed": closed,
                    "sourceSequenceStart": start_sequence,
                    "sourceSequenceEnd": end_sequence,
                    "eventCount": event_count,
                    **prices,
                }
            )
        if (
            parsed[0]["sourceSequenceStart"]
            != window_body["firstSourceSequence"]
            or parsed[-1]["sourceSequenceEnd"]
            != window_body["lastSourceSequence"]
            or sum(bar["eventCount"] for bar in parsed)
            != window_body["sourceEventCount"]
            or not hmac.compare_digest(
                window_body["sourceProofHash"],
                _hash(
                    {
                        "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
                        "route": ROUTE,
                        "pdno": PDNO,
                        "sourceProvider": "kis",
                        "sourceGeneration": window_body["sourceGeneration"],
                        "firstSourceSequence": window_body[
                            "firstSourceSequence"
                        ],
                        "lastSourceSequence": window_body["lastSourceSequence"],
                        "sourceEventCount": window_body["sourceEventCount"],
                        "barRawEventChainHashes": [
                            bar["rawEventChainHash"] for bar in bars
                        ],
                    }
                ),
            )
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "H0STCNT0 window source proof hash mismatch"
            )
        observed = _parse_utc(window_body.get("observedAt"), "window.observedAt")
        last_close = parsed[-1]["closed"]
        if observed < last_close or observed > last_close + timedelta(seconds=5):
            raise KisDomesticFunctionalLaneBlocked("bar window is not freshly finalized")
        now = self._now()
        if now < observed or now > observed + timedelta(seconds=5):
            raise KisDomesticFunctionalLaneBlocked("bar window clock observation is stale")
        trading_date = last_close.astimezone(KST).date()
        try:
            session_open, session_close = session_bounds_utc("XKRX", trading_date)
        except ValueError as exc:
            raise KisDomesticFunctionalLaneBlocked(
                "bar window is not on an official XKRX session"
            ) from exc
        if (
            parsed[0]["opened"] < session_open
            or last_close > session_close
            or last_close.astimezone(KST).minute % BAR_INTERVAL_MINUTES
            or last_close.astimezone(KST).second
            or last_close.astimezone(KST).microsecond
            or last_close.astimezone(KST).time().replace(tzinfo=None) > ARMED_LATEST
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "bar window is outside the approved XKRX activation schedule"
            )
        prior = parsed[:-1]
        average_range = sum((bar["high"] - bar["low"] for bar in prior), Decimal("0")) / Decimal("10")
        trigger_price = prior[-1]["close"] + average_range * Decimal("0.3")
        signal = "BUY" if parsed[-1]["high"] >= trigger_price else "HOLD"
        evaluation_id = f"kis-eval-{uuid.uuid4().hex}"
        record = {
            "schemaVersion": "kis-domestic-breakout-evaluation/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "evaluationId": evaluation_id,
            "rawWindowHash": _hash(window_body),
            "rawWindowSignature": server_authority_signature,
            "sourceProvider": window_body["sourceProvider"],
            "sourceGeneration": window_body["sourceGeneration"],
            "firstSourceSequence": window_body["firstSourceSequence"],
            "lastSourceSequence": window_body["lastSourceSequence"],
            "sourceEventCount": window_body["sourceEventCount"],
            "sourceProofHash": window_body["sourceProofHash"],
            "barCloseAt": _utc_text(last_close, "barCloseAt"),
            "observedAt": _utc_text(observed, "observedAt"),
            "breakoutWindow": 10,
            "breakoutK": "0.3",
            "averageRange": format(average_range, "f"),
            "triggerPrice": format(trigger_price, "f"),
            "currentHigh": format(parsed[-1]["high"], "f"),
            "signal": signal,
            "executionTiming": "next-open",
            "openBoundaryAttestationRequired": _TRIGGER_SOURCE,
            "promotionEligible": False,
        }
        raw_window_json = _canonical(window_body).decode("utf-8")
        raw_window_hash = hashlib.sha256(raw_window_json.encode("utf-8")).hexdigest()
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            arm_row = conn.execute(
                "SELECT * FROM kis_functional_public_arm WHERE arm_id=?",
                (public_arm_id,),
            ).fetchone()
            if (
                arm_row is None
                or str(arm_row["state"]) != "ARMED_WAIT_PUBLIC"
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "public natural-signal wait is not exactly ARMED"
                )
            arm = self._verify_stored_record(arm_row)
            if now >= _parse_utc(arm["expiresAt"], "publicArm.expiresAt"):
                raise KisDomesticFunctionalLaneBlocked(
                    "public natural-signal wait expired"
                )
            record.update(
                {
                    "publicArmId": public_arm_id,
                    "publicArmHash": str(arm_row["record_hash"]),
                    "publicDataOnly": True,
                    "accountAuthorityAvailable": False,
                    "orderAuthorityAvailable": False,
                    "contractEnvelopeHash": arm["contractEnvelopeHash"],
                    "codeManifestHash": arm["codeManifestHash"],
                }
            )
            record_json, record_hash, signature = self._signed_record(record)
            conn.execute(
                """INSERT INTO kis_functional_evaluation
                (evaluation_id, public_arm_id, state, record_json,
                 record_hash, signature, raw_window_json, raw_window_hash,
                 raw_window_signature, created_at, revision)
                VALUES (?, ?, 'SEALED_PUBLIC', ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    evaluation_id,
                    public_arm_id,
                    record_json,
                    record_hash,
                    signature,
                    raw_window_json,
                    raw_window_hash,
                    server_authority_signature,
                    _utc_text(now, "createdAt"),
                ),
            )
        return {"body": record, "recordHash": record_hash, "signature": signature}

    def record_next_open_trigger(
        self,
        *,
        evaluation_id: str,
        trigger_body: Mapping[str, Any],
        server_authority_signature: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(trigger_body, Mapping)
            or type(server_authority_signature) is not str
            or not _SHA256.fullmatch(server_authority_signature)
            or not hmac.compare_digest(
                sign_kis_domestic_lane_capture(self._key, "NEXT_OPEN", trigger_body),
                server_authority_signature,
            )
        ):
            raise KisDomesticFunctionalLaneBlocked("next-open trigger signature mismatch")
        if set(trigger_body) != _NEXT_OPEN_KEYS:
            raise KisDomesticFunctionalLaneBlocked("next-open trigger fields are not exact")
        exact = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": _TRIGGER_SOURCE,
            "eventType": "NEXT_BAR_OPEN",
            "evaluationId": evaluation_id,
        }
        for key, expected in exact.items():
            if type(trigger_body.get(key)) is not type(expected) or trigger_body.get(key) != expected:
                raise KisDomesticFunctionalLaneBlocked(f"next-open trigger {key} mismatch")
        if (
            trigger_body.get("sourceProvider") != "kis"
            or type(trigger_body.get("sourceGeneration")) is not str
            or not _SOURCE_GENERATION.fullmatch(trigger_body["sourceGeneration"])
            or type(trigger_body.get("sourceSequence")) is not str
            or not _RAW_SOURCE_SEQUENCE.fullmatch(trigger_body["sourceSequence"])
            or type(trigger_body.get("rawEventHash")) is not str
            or not _SHA256.fullmatch(trigger_body["rawEventHash"])
            or type(trigger_body.get("sourceProofHash")) is not str
            or not _SHA256.fullmatch(trigger_body["sourceProofHash"])
            or not hmac.compare_digest(
                trigger_body["sourceProofHash"],
                _hash(
                    {
                        "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
                        "route": ROUTE,
                        "pdno": PDNO,
                        "sourceProvider": "kis",
                        "sourceGeneration": trigger_body["sourceGeneration"],
                        "sourceSequence": trigger_body["sourceSequence"],
                        "rawEventHash": trigger_body["rawEventHash"],
                        "barOpenAt": trigger_body["barOpenAt"],
                        "observedAt": trigger_body["observedAt"],
                    }
                ),
            )
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "H0STCNT0 next-open source proof is invalid"
            )
        bar_open = _parse_utc(trigger_body.get("barOpenAt"), "trigger.barOpenAt")
        observed = _parse_utc(trigger_body.get("observedAt"), "trigger.observedAt")
        price = _decimal(trigger_body.get("openPriceKrw"), "trigger.openPriceKrw", positive=True)
        if price * ORDER_QUANTITY > MAX_ORDER_KRW:
            raise KisDomesticFunctionalLaneBlocked("next-open price exceeds order cap")
        if observed < bar_open or observed > bar_open + timedelta(seconds=2):
            raise KisDomesticFunctionalLaneBlocked("next-open observation missed exact boundary")
        now = self._now()
        if now < observed or now > observed + timedelta(seconds=2):
            raise KisDomesticFunctionalLaneBlocked("next-open trigger clock is stale")
        raw_trigger_json = _canonical(trigger_body).decode("utf-8")
        raw_trigger_hash = hashlib.sha256(raw_trigger_json.encode("utf-8")).hexdigest()
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            evaluation_row = conn.execute(
                "SELECT * FROM kis_functional_evaluation WHERE evaluation_id=?",
                (evaluation_id,),
            ).fetchone()
            if evaluation_row is None or str(evaluation_row["state"]) != "SEALED_PUBLIC":
                raise KisDomesticFunctionalLaneBlocked(
                    "public evaluation is not exactly SEALED"
                )
            evaluation = self._verify_stored_record(evaluation_row)
            raw_window = self._verify_raw_capture(
                evaluation_row,
                json_column="raw_window_json",
                hash_column="raw_window_hash",
                signature_column="raw_window_signature",
                domain="BAR_WINDOW",
            )
            if (
                evaluation["rawWindowHash"] != str(evaluation_row["raw_window_hash"])
                or evaluation["rawWindowSignature"]
                != str(evaluation_row["raw_window_signature"])
                or raw_window["observedAt"] != evaluation["observedAt"]
                or raw_window["bars"][-1]["closeAt"] != evaluation["barCloseAt"]
                or raw_window["sourceGeneration"]
                != trigger_body["sourceGeneration"]
                or int(trigger_body["sourceSequence"])
                <= int(raw_window["lastSourceSequence"])
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "evaluation raw window lineage mismatch"
                )
            if evaluation["signal"] != "BUY" or evaluation["barCloseAt"] != _utc_text(
                bar_open, "barOpenAt"
            ):
                raise KisDomesticFunctionalLaneBlocked("trigger is not the evaluation next-open")
            trigger_id = f"kis-trigger-{uuid.uuid4().hex}"
            record = {
                **dict(trigger_body),
                "triggerId": trigger_id,
                "evaluationHash": str(evaluation_row["record_hash"]),
                "publicArmId": evaluation["publicArmId"],
                "publicArmHash": evaluation["publicArmHash"],
                "publicDataOnly": True,
                "accountAuthorityAvailable": False,
                "orderAuthorityAvailable": False,
                "contractEnvelopeHash": evaluation["contractEnvelopeHash"],
                "codeManifestHash": evaluation["codeManifestHash"],
                "rawTriggerHash": raw_trigger_hash,
                "rawTriggerSignature": server_authority_signature,
                "promotionEligible": False,
            }
            record_json, record_hash, signature = self._signed_record(record)
            conn.execute(
                """INSERT INTO kis_functional_next_open
                (trigger_id, evaluation_id, state, record_json, record_hash,
                 signature, raw_trigger_json, raw_trigger_hash,
                 raw_trigger_signature, created_at, revision)
                VALUES (?, ?, 'SEALED_PUBLIC', ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    trigger_id,
                    evaluation_id,
                    record_json,
                    record_hash,
                    signature,
                    raw_trigger_json,
                    raw_trigger_hash,
                    server_authority_signature,
                    _utc_text(now, "createdAt"),
                ),
            )
        return {"body": record, "recordHash": record_hash, "signature": signature}

    def activate(
        self,
        *,
        bootstrap_id: str,
        approval_id: str,
        evaluation_id: str,
        trigger_id: str,
        session_id: str,
        fresh_quote_hash: str,
        fresh_quote_observed_at: str,
        fresh_quote_price_krw: str,
        natural_buy_limit_price_krw: str,
        graph_grant_instant_receipt: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if type(session_id) is not str or not re.fullmatch(r"kis-session-[0-9a-f]{32}", session_id):
            raise KisDomesticFunctionalLaneBlocked("session id is invalid")
        now = self._now()
        grant, grant_receipt_hash, grant_receipt_signature = (
            self._verify_grant_receipt(graph_grant_instant_receipt)
        )
        grant_wall = _parse_utc(grant.get("grantWallAt"), "grant.grantWallAt")
        grant_monotonic_ns = int(grant["grantMonotonicNs"])
        if now < grant_wall:
            raise KisDomesticFunctionalLaneBlocked(
                "trusted lane clock rolled back before graph grant instant"
            )
        if now > grant_wall + timedelta(seconds=2):
            raise KisDomesticFunctionalLaneBlocked(
                "graph grant instant is stale at lane activation"
            )
        quote_hash = _require_sha(fresh_quote_hash, "fresh quote hash")
        quote_observed = _parse_utc(
            fresh_quote_observed_at, "freshQuote.observedAt"
        )
        quote_price = _decimal(
            fresh_quote_price_krw, "fresh quote price", positive=True
        )
        buy_limit = _decimal(
            natural_buy_limit_price_krw, "natural BUY limit", positive=True
        )
        if (
            grant_wall < quote_observed
            or grant_wall > quote_observed + timedelta(seconds=5)
            or quote_price != buy_limit
            or buy_limit * ORDER_QUANTITY > MAX_ORDER_KRW
            or buy_limit * ORDER_QUANTITY > MAX_GROSS_KRW
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "fresh quote and natural BUY limit are not exact"
            )
        if not (
            grant.get("sessionId") == session_id
            and grant.get("bootstrapId") == bootstrap_id
            and grant.get("approvalId") == approval_id
            and grant.get("evaluationId") == evaluation_id
            and grant.get("triggerId") == trigger_id
            and grant.get("freshQuoteHash") == quote_hash
        ):
            raise KisDomesticFunctionalLaneBlocked(
                "graph grant instant call binding is invalid"
            )
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            bootstrap_row = conn.execute(
                "SELECT * FROM kis_functional_bootstrap WHERE bootstrap_id=?", (bootstrap_id,)
            ).fetchone()
            approval_row = conn.execute(
                "SELECT * FROM kis_functional_approval WHERE approval_id=?", (approval_id,)
            ).fetchone()
            evaluation_row = conn.execute(
                "SELECT * FROM kis_functional_evaluation WHERE evaluation_id=?", (evaluation_id,)
            ).fetchone()
            trigger_row = conn.execute(
                "SELECT * FROM kis_functional_next_open WHERE trigger_id=?", (trigger_id,)
            ).fetchone()
            if any(row is None for row in (bootstrap_row, approval_row, evaluation_row, trigger_row)):
                raise KisDomesticFunctionalLaneBlocked("activation durable inputs are incomplete")
            if (
                str(bootstrap_row["state"]) != "APPROVED"
                or str(approval_row["state"]) != "APPROVED"
                or str(evaluation_row["state"]) != "SEALED_PUBLIC"
                or str(trigger_row["state"]) != "SEALED_PUBLIC"
            ):
                raise KisDomesticFunctionalLaneBlocked("activation input was already consumed")
            bootstrap = self._verify_stored_record(bootstrap_row)
            approval = self._verify_stored_record(approval_row)
            evaluation = self._verify_stored_record(evaluation_row)
            trigger = self._verify_stored_record(trigger_row)
            raw_window = self._verify_raw_capture(
                evaluation_row,
                json_column="raw_window_json",
                hash_column="raw_window_hash",
                signature_column="raw_window_signature",
                domain="BAR_WINDOW",
            )
            raw_trigger = self._verify_raw_capture(
                trigger_row,
                json_column="raw_trigger_json",
                hash_column="raw_trigger_hash",
                signature_column="raw_trigger_signature",
                domain="NEXT_OPEN",
            )
            if (
                approval["bootstrapId"] != bootstrap_id
                or approval["bootstrapHash"] != str(bootstrap_row["record_hash"])
                or str(bootstrap_row["approval_id"]) != approval_id
                or str(bootstrap_row["evaluation_id"]) != evaluation_id
                or str(bootstrap_row["trigger_id"]) != trigger_id
                or bootstrap["publicArmId"] != evaluation["publicArmId"]
                or evaluation["publicArmId"] != trigger["publicArmId"]
                or trigger["evaluationId"] != evaluation_id
                or trigger["evaluationHash"] != str(evaluation_row["record_hash"])
                or evaluation["signal"] != "BUY"
                or evaluation["rawWindowHash"] != str(evaluation_row["raw_window_hash"])
                or trigger["rawTriggerHash"] != str(trigger_row["raw_trigger_hash"])
                or raw_window["bars"][-1]["closeAt"] != trigger["barOpenAt"]
                or raw_trigger["evaluationId"] != evaluation_id
                or grant.get("triggerHash") != str(trigger_row["record_hash"])
            ):
                raise KisDomesticFunctionalLaneBlocked("activation lineage mismatch")
            lineage_fields = (
                "accountFingerprint",
                "permitId",
                "permitHash",
                "preactivationBaselineHash",
                "contractEnvelopeHash",
                "codeManifestHash",
            )
            if any(
                approval[field] != bootstrap[field]
                or (
                    field in {"contractEnvelopeHash", "codeManifestHash"}
                    and evaluation[field] != bootstrap[field]
                )
                or (
                    field in {"contractEnvelopeHash", "codeManifestHash"}
                    and trigger[field] != bootstrap[field]
                )
                for field in lineage_fields
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "activation authority binding mismatch"
                )
            if not (
                grant.get("accountFingerprint") == bootstrap["accountFingerprint"]
                and grant.get("preactivationBaselineHash")
                == bootstrap["preactivationBaselineHash"]
                and grant.get("codeManifestHash") == bootstrap["codeManifestHash"]
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "graph grant instant durable authority binding mismatch"
                )
            approval_expires = _parse_utc(
                approval["expiresAt"], "approval.expiresAt"
            )
            if grant_wall >= approval_expires or now >= approval_expires:
                raise KisDomesticFunctionalLaneBlocked("approval expired")
            trigger_observed = _parse_utc(trigger["observedAt"], "trigger.observedAt")
            if (
                grant_wall < trigger_observed
                or grant_wall > trigger_observed + timedelta(seconds=2)
            ):
                raise KisDomesticFunctionalLaneBlocked("activation is not at exact next-open")
            trigger_open_price = _decimal(
                trigger["openPriceKrw"], "trigger open price", positive=True
            )
            if buy_limit != trigger_open_price:
                raise KisDomesticFunctionalLaneBlocked(
                    "natural BUY limit is not the exact next-open price"
                )
            trigger_bar_open = _parse_utc(
                trigger["barOpenAt"], "trigger.barOpenAt"
            )
            activated = grant_wall
            trading_date = activated.astimezone(KST).date()
            try:
                session_open, session_close = session_bounds_utc("XKRX", trading_date)
            except ValueError as exc:
                raise KisDomesticFunctionalLaneBlocked(
                    "activation is not on an official XKRX session"
                ) from exc
            local_trigger = trigger_bar_open.astimezone(KST)
            expires = activated + timedelta(seconds=ACTIVE_SECONDS)
            cleanup_ends = datetime.combine(
                trading_date, CLEANUP_END_LATEST, tzinfo=KST
            ).astimezone(timezone.utc)
            if (
                not session_open <= trigger_bar_open <= activated < session_close
                or local_trigger.second
                or local_trigger.microsecond
                or local_trigger.minute % BAR_INTERVAL_MINUTES
                or local_trigger.time().replace(tzinfo=None) > ARMED_LATEST
                or expires.astimezone(KST).time().replace(tzinfo=None)
                > ACTIVE_END_LATEST
                or cleanup_ends != session_close
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "activation violates the approved XKRX 7200-second schedule"
                )
            natural_buy_claim_id = f"kis-claim-{uuid.uuid4().hex}"
            natural_buy_body = {
                "schemaVersion": "kis-domestic-functional-action/v1",
                "claimId": natural_buy_claim_id,
                "sessionId": session_id,
                "actionKind": "NATURAL_BUY",
                "quantity": str(ORDER_QUANTITY),
                "limitPriceKrw": natural_buy_limit_price_krw,
                "grossKrw": format(buy_limit * ORDER_QUANTITY, "f"),
                "evaluationId": evaluation_id,
                "evaluationHash": str(evaluation_row["record_hash"]),
                "triggerId": trigger_id,
                "triggerHash": str(trigger_row["record_hash"]),
                "triggerObservedAt": trigger["observedAt"],
                "freshQuoteHash": quote_hash,
                "freshQuoteObservedAt": fresh_quote_observed_at,
                "freshQuotePriceKrw": fresh_quote_price_krw,
                "grantReceiptHash": grant_receipt_hash,
                "grantWallAt": _utc_text(grant_wall, "grantWallAt"),
                "grantMonotonicNs": grant_monotonic_ns,
                "graphRequestHash": grant["graphRequestHash"],
                "graphActionInputsHash": grant["graphActionInputsHash"],
                "graphIntentStepHash": grant["graphIntentStepHash"],
                "bootstrapId": bootstrap_id,
                "bootstrapHash": str(bootstrap_row["record_hash"]),
                "approvalId": approval_id,
                "approvalHash": str(approval_row["record_hash"]),
                "accountFingerprint": bootstrap["accountFingerprint"],
                "permitId": bootstrap["permitId"],
                "permitHash": bootstrap["permitHash"],
                "preactivationBaselineHash": bootstrap[
                    "preactivationBaselineHash"
                ],
                "contractEnvelopeHash": bootstrap["contractEnvelopeHash"],
                "codeManifestHash": bootstrap["codeManifestHash"],
                "createdAt": _utc_text(grant_wall, "naturalBuy.createdAt"),
                "promotionEligible": False,
            }
            natural_buy_hash = _hash(natural_buy_body)
            activation_body = {
                "schemaVersion": "kis-domestic-functional-activation/v2",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": session_id,
                "bootstrapId": bootstrap_id,
                "bootstrapHash": str(bootstrap_row["record_hash"]),
                "approvalId": approval_id,
                "approvalHash": str(approval_row["record_hash"]),
                "evaluationId": evaluation_id,
                "evaluationHash": str(evaluation_row["record_hash"]),
                "rawWindowHash": str(evaluation_row["raw_window_hash"]),
                "triggerId": trigger_id,
                "triggerHash": str(trigger_row["record_hash"]),
                "rawTriggerHash": str(trigger_row["raw_trigger_hash"]),
                "triggerBarOpenAt": trigger["barOpenAt"],
                "triggerObservedAt": trigger["observedAt"],
                "naturalBuyClaimId": natural_buy_claim_id,
                "naturalBuyClaimHash": natural_buy_hash,
                "naturalBuyLimitPriceKrw": natural_buy_limit_price_krw,
                "freshQuoteHash": quote_hash,
                "freshQuoteObservedAt": fresh_quote_observed_at,
                "freshQuotePriceKrw": fresh_quote_price_krw,
                "grantReceiptHash": grant_receipt_hash,
                "grantReceiptSignatureHash": hashlib.sha256(
                    grant_receipt_signature.encode("ascii")
                ).hexdigest(),
                "grantWallAt": _utc_text(grant_wall, "grantWallAt"),
                "grantMonotonicNs": grant_monotonic_ns,
                "graphTransactionId": grant["graphTransactionId"],
                "graphRequestHash": grant["graphRequestHash"],
                "graphActionInputsHash": grant["graphActionInputsHash"],
                "graphIntentStepHash": grant["graphIntentStepHash"],
                "expectedStatusRevision": grant["expectedStatusRevision"],
                "expectedStatusHeadHash": grant["expectedStatusHeadHash"],
                "ownerEpochHash": grant["ownerEpochHash"],
                "registryAcceptedHeadHash": grant[
                    "registryAcceptedHeadHash"
                ],
                "rollingReceiptHash": grant["rollingReceiptHash"],
                "quoteReceiptHash": grant["quoteReceiptHash"],
                "accountFingerprint": bootstrap["accountFingerprint"],
                "permitId": bootstrap["permitId"],
                "permitHash": bootstrap["permitHash"],
                "sessionNonceHash": bootstrap["sessionNonceHash"],
                "preactivationBaselineHash": bootstrap[
                    "preactivationBaselineHash"
                ],
                "contractEnvelopeHash": bootstrap["contractEnvelopeHash"],
                "codeManifestHash": bootstrap["codeManifestHash"],
                "artifactContentHash": bootstrap["artifactContentHash"],
                "artifactFileSha256": bootstrap["artifactFileSha256"],
                "instanceContentHash": bootstrap["instanceContentHash"],
                "instanceFileSha256": bootstrap["instanceFileSha256"],
                "quantity": bootstrap["quantity"],
                "maxOrderKrw": bootstrap["maxOrderKrw"],
                "maxGrossKrw": bootstrap["maxGrossKrw"],
                "ownerLossMustRemainBelowKrw": bootstrap[
                    "ownerLossMustRemainBelowKrw"
                ],
                "activatedAt": _utc_text(activated, "activatedAt"),
                "activationObservedAt": _utc_text(grant_wall, "activationObservedAt"),
                "expiresAt": _utc_text(expires, "expiresAt"),
                "cleanupEndsAt": _utc_text(cleanup_ends, "cleanupEndsAt"),
                "activeSeconds": ACTIVE_SECONDS,
                "realOrdersEnabled": False,
                "promotionEligible": False,
                "serverAuthorityKeyIdHash": self._key_id_hash,
            }
            activation_json, activation_hash, activation_signature = self._signed_record(
                activation_body
            )
            conn.execute(
                """INSERT INTO kis_functional_session
                (session_id, bootstrap_id, approval_id, evaluation_id, trigger_id,
                 permit_id, permit_hash, account_fingerprint,
                 preactivation_baseline_hash, contract_envelope_hash,
                 code_manifest_hash, state, activated_at, expires_at,
                 cleanup_ends_at, grant_monotonic_ns, grant_receipt_json,
                 grant_receipt_hash, grant_receipt_signature, activation_record_json,
                 activation_record_hash, activation_signature, revision)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    session_id,
                    bootstrap_id,
                    approval_id,
                    evaluation_id,
                    trigger_id,
                    bootstrap["permitId"],
                    bootstrap["permitHash"],
                    bootstrap["accountFingerprint"],
                    bootstrap["preactivationBaselineHash"],
                    bootstrap["contractEnvelopeHash"],
                    bootstrap["codeManifestHash"],
                    _utc_text(activated, "activatedAt"),
                    _utc_text(expires, "expiresAt"),
                    _utc_text(cleanup_ends, "cleanupEndsAt"),
                    grant_monotonic_ns,
                    _canonical(grant).decode("utf-8"),
                    grant_receipt_hash,
                    grant_receipt_signature,
                    activation_json,
                    activation_hash,
                    activation_signature,
                ),
            )
            natural_buy_transition_hash = self._insert_action_transition(
                conn,
                claim_id=natural_buy_claim_id,
                session_id=session_id,
                action_kind="NATURAL_BUY",
                revision=1,
                state="CLAIMED",
                occurred_at=_utc_text(grant_wall, "naturalBuy.createdAt"),
                previous_hash=_ZERO_HASH,
                details={
                    "claimRecordHash": natural_buy_hash,
                    "quantity": str(ORDER_QUANTITY),
                    "limitPriceKrw": natural_buy_limit_price_krw,
                    "grossKrw": format(buy_limit * ORDER_QUANTITY, "f"),
                    "evaluationId": evaluation_id,
                    "triggerId": trigger_id,
                    "activationRecordHash": activation_hash,
                    "freshQuoteHash": quote_hash,
                    "grantReceiptHash": grant_receipt_hash,
                    "grantWallAt": _utc_text(grant_wall, "grantWallAt"),
                    "grantMonotonicNs": grant_monotonic_ns,
                    "triggerHash": str(trigger_row["record_hash"]),
                    "triggerObservedAt": trigger["observedAt"],
                    "preactivationBaselineHash": bootstrap[
                        "preactivationBaselineHash"
                    ],
                },
            )
            conn.execute(
                """INSERT INTO kis_functional_action
                (claim_id, session_id, action_kind, state, quantity,
                 limit_price_krw, gross_krw, evaluation_id, trigger_id,
                 created_at, updated_at, record_hash,
                 transition_head_hash, revision)
                VALUES (?, ?, 'NATURAL_BUY', 'CLAIMED', '1', ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    natural_buy_claim_id,
                    session_id,
                    natural_buy_limit_price_krw,
                    format(buy_limit * ORDER_QUANTITY, "f"),
                    evaluation_id,
                    trigger_id,
                    _utc_text(grant_wall, "naturalBuy.createdAt"),
                    _utc_text(grant_wall, "naturalBuy.updatedAt"),
                    natural_buy_hash,
                    natural_buy_transition_hash,
                ),
            )
            updates = (
                ("kis_functional_bootstrap", "bootstrap_id", bootstrap_id, "APPROVED", "CLAIMED", int(bootstrap_row["revision"])),
                ("kis_functional_approval", "approval_id", approval_id, "APPROVED", "CLAIMED", int(approval_row["revision"])),
                ("kis_functional_evaluation", "evaluation_id", evaluation_id, "SEALED_PUBLIC", "CONSUMED", int(evaluation_row["revision"])),
                ("kis_functional_next_open", "trigger_id", trigger_id, "SEALED_PUBLIC", "CONSUMED", int(trigger_row["revision"])),
            )
            for table, key, value, old, new, revision in updates:
                extra = ", session_id=?" if table in {"kis_functional_bootstrap", "kis_functional_approval"} else ""
                params: tuple[Any, ...] = (new,)
                if extra:
                    params += (session_id,)
                params += (value, old, revision)
                changed = conn.execute(
                    f"UPDATE {table} SET state=?, revision=revision+1{extra} "
                    f"WHERE {key}=? AND state=? AND revision=?",
                    params,
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalLaneBlocked("activation CAS failed")
        return {
            "schemaVersion": "kis-domestic-functional-activation/v2",
            "sessionId": session_id,
            "state": "ACTIVE",
            "activatedAt": _utc_text(activated, "activatedAt"),
            "activationObservedAt": _utc_text(grant_wall, "activationObservedAt"),
            "grantMonotonicNs": grant_monotonic_ns,
            "grantReceiptHash": grant_receipt_hash,
            "expiresAt": _utc_text(expires, "expiresAt"),
            "activeSeconds": ACTIVE_SECONDS,
            "evaluationId": evaluation_id,
            "triggerId": trigger_id,
            "activationRecordHash": activation_hash,
            "naturalBuyClaimId": natural_buy_claim_id,
            "naturalBuyClaimHash": natural_buy_hash,
            "realOrdersEnabled": False,
            "promotionEligible": False,
        }

    def claim_action(
        self,
        *,
        session_id: str,
        action_kind: str,
        limit_price_krw: str,
    ) -> dict[str, Any]:
        price = _decimal(limit_price_krw, "limit price", positive=True)
        gross = price * ORDER_QUANTITY
        if gross > MAX_ORDER_KRW or gross > MAX_GROSS_KRW:
            raise KisDomesticFunctionalLaneBlocked("action cap exceeded")
        if action_kind != "CLEANUP_SELL":
            raise KisDomesticFunctionalLaneBlocked(
                "natural BUY is pre-created atomically at activation; only cleanup SELL may be claimed later"
            )
        now = self._now()
        claim_id = f"kis-claim-{uuid.uuid4().hex}"
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM kis_functional_session WHERE session_id=?", (session_id,)
            ).fetchone()
            if session is None:
                raise KisDomesticFunctionalLaneBlocked("session is missing")
            activation = self._verify_session_activation(session)
            state = str(session["state"])
            activated = _parse_utc(session["activated_at"], "session.activatedAt")
            expires = _parse_utc(session["expires_at"], "session.expiresAt")
            cleanup_ends = _parse_utc(
                session["cleanup_ends_at"], "session.cleanupEndsAt"
            )
            if now < activated or now > cleanup_ends:
                raise KisDomesticFunctionalLaneBlocked(
                    "action is outside the sealed session schedule"
                )
            evaluation_id = trigger_id = ""
            if state != "CLEANUP":
                raise KisDomesticFunctionalLaneBlocked("cleanup SELL requires CLEANUP state")
            buy = conn.execute(
                """SELECT state FROM kis_functional_action
                   WHERE session_id=? AND action_kind='NATURAL_BUY'""",
                (session_id,),
            ).fetchone()
            if buy is None or str(buy["state"]) != "FILLED":
                raise KisDomesticFunctionalLaneBlocked("cleanup SELL requires filled owned BUY")
            body = {
                "schemaVersion": "kis-domestic-functional-action/v1",
                "claimId": claim_id,
                "sessionId": session_id,
                "actionKind": action_kind,
                "quantity": str(ORDER_QUANTITY),
                "limitPriceKrw": limit_price_krw,
                "grossKrw": format(gross, "f"),
                "evaluationId": evaluation_id,
                "triggerId": trigger_id,
                "activationRecordHash": str(session["activation_record_hash"]),
                "accountFingerprint": activation["accountFingerprint"],
                "permitId": activation["permitId"],
                "permitHash": activation["permitHash"],
                "preactivationBaselineHash": activation[
                    "preactivationBaselineHash"
                ],
                "contractEnvelopeHash": activation["contractEnvelopeHash"],
                "codeManifestHash": activation["codeManifestHash"],
                "createdAt": _utc_text(now, "createdAt"),
                "promotionEligible": False,
            }
            record_hash = _hash(body)
            transition_head_hash = self._insert_action_transition(
                conn,
                claim_id=claim_id,
                session_id=session_id,
                action_kind=action_kind,
                revision=1,
                state="CLAIMED",
                occurred_at=_utc_text(now, "createdAt"),
                previous_hash=_ZERO_HASH,
                details={
                    "claimRecordHash": record_hash,
                    "quantity": str(ORDER_QUANTITY),
                    "limitPriceKrw": limit_price_krw,
                    "grossKrw": format(gross, "f"),
                    "evaluationId": evaluation_id,
                    "triggerId": trigger_id,
                    "activationRecordHash": str(
                        session["activation_record_hash"]
                    ),
                    "preactivationBaselineHash": str(
                        session["preactivation_baseline_hash"]
                    ),
                },
            )
            try:
                conn.execute(
                    """INSERT INTO kis_functional_action
                    (claim_id, session_id, action_kind, state, quantity,
                     limit_price_krw, gross_krw, evaluation_id, trigger_id,
                     created_at, updated_at, record_hash,
                     transition_head_hash, revision)
                    VALUES (?, ?, ?, 'CLAIMED', '1', ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                    (
                        claim_id,
                        session_id,
                        action_kind,
                        limit_price_krw,
                        format(gross, "f"),
                        evaluation_id,
                        trigger_id,
                        _utc_text(now, "createdAt"),
                        _utc_text(now, "updatedAt"),
                        record_hash,
                        transition_head_hash,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise KisDomesticFunctionalLaneBlocked(
                    f"{action_kind} slot is one-use and already claimed"
                ) from exc
        return {"body": body, "recordHash": record_hash, "state": "CLAIMED"}

    def transition_action(
        self,
        *,
        claim_id: str,
        expected_revision: int,
        target_state: str,
        broker_order_id: str = "",
        org_no: str = "",
        order_date: str = "",
        fill_price_krw: str = "",
        fee_krw: str = "",
        tax_krw: str = "",
        loan_interest_krw: str = "",
        not_sent_reason: str = "",
    ) -> dict[str, Any]:
        if target_state not in _ACTION_STATES:
            raise KisDomesticFunctionalLaneBlocked("target action state is invalid")
        transitions = {
            "CLAIMED": {"SUBMITTING", "NOT_SENT"},
            "SUBMITTING": {"POST_MAY_HAVE_CROSSED", "NOT_SENT"},
            "POST_MAY_HAVE_CROSSED": {"ACKNOWLEDGED"},
            "ACKNOWLEDGED": {"FILLED"},
        }
        now = self._now()
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM kis_functional_action WHERE claim_id=?", (claim_id,)
            ).fetchone()
            if row is None or int(row["revision"]) != expected_revision:
                raise KisDomesticFunctionalLaneBlocked("action revision CAS mismatch")
            self._verify_action_chain(conn, row)
            current = str(row["state"])
            if target_state not in transitions.get(current, set()):
                raise KisDomesticFunctionalLaneBlocked("action state transition is invalid")
            session = conn.execute(
                "SELECT * FROM kis_functional_session WHERE session_id=?",
                (str(row["session_id"]),),
            ).fetchone()
            if session is None:
                raise KisDomesticFunctionalLaneBlocked("action session is missing")
            activation_record = self._verify_session_activation(session)
            session_state = str(session["state"])
            activated = _parse_utc(session["activated_at"], "session.activatedAt")
            expires = _parse_utc(session["expires_at"], "session.expiresAt")
            cleanup_ends = _parse_utc(
                session["cleanup_ends_at"], "session.cleanupEndsAt"
            )
            previous_updated = _parse_utc(row["updated_at"], "action.updatedAt")
            if now < activated or now < previous_updated or now > cleanup_ends:
                raise KisDomesticFunctionalLaneBlocked(
                    "action transition time is outside its sealed schedule"
                )
            transition_details: dict[str, Any] = {}
            action_kind = str(row["action_kind"])
            if action_kind == "NATURAL_BUY":
                activation_observed = _parse_utc(
                    activation_record["activationObservedAt"],
                    "activation.activationObservedAt",
                )
                trigger_observed = _parse_utc(
                    activation_record["triggerObservedAt"],
                    "activation.triggerObservedAt",
                )
                if (
                    activation_record.get("activatedAt")
                    != activation_record.get("grantWallAt")
                    or activation_record.get("activationObservedAt")
                    != activation_record.get("grantWallAt")
                    or str(row["created_at"]) != activation_record.get("grantWallAt")
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural BUY claim is not bound to the trusted graph grant instant"
                    )
                if target_state in {"SUBMITTING", "POST_MAY_HAVE_CROSSED"} and (
                    session_state != "ACTIVE" or not now < expires
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural BUY dispatch is outside ACTIVE authority"
                    )
                if target_state in {"SUBMITTING", "POST_MAY_HAVE_CROSSED"} and now > (
                    activation_observed + timedelta(seconds=2)
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural BUY dispatch missed the exact next-open boundary"
                    )
                if target_state in {"SUBMITTING", "POST_MAY_HAVE_CROSSED"} and now > (
                    trigger_observed + timedelta(seconds=2)
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural BUY dispatch missed the sealed trigger boundary"
                    )
                if target_state in {"ACKNOWLEDGED", "FILLED"} and session_state not in {
                    "ACTIVE",
                    "CLEANUP",
                }:
                    raise KisDomesticFunctionalLaneBlocked(
                        "natural BUY observation has no live session authority"
                    )
                if target_state == "FILLED" and not now < expires:
                    transition_details["lateFillCleanupOnly"] = True
                    transition_details["naturalWiringEligible"] = False
                transition_details.update(
                    {
                        "grantReceiptHash": activation_record[
                            "grantReceiptHash"
                        ],
                        "grantWallAt": activation_record["grantWallAt"],
                        "grantMonotonicNs": activation_record[
                            "grantMonotonicNs"
                        ],
                        "triggerHash": activation_record["triggerHash"],
                        "triggerObservedAt": activation_record[
                            "triggerObservedAt"
                        ],
                    }
                )
            elif session_state != "CLEANUP":
                raise KisDomesticFunctionalLaneBlocked(
                    "cleanup SELL transition requires CLEANUP authority"
                )
            updates: dict[str, Any] = {
                "state": target_state,
                "updated_at": _utc_text(now, "updatedAt"),
            }
            if target_state == "NOT_SENT":
                if type(not_sent_reason) is not str or not not_sent_reason:
                    raise KisDomesticFunctionalLaneBlocked(
                        "NOT_SENT requires a durable reason"
                    )
                transition_details["reason"] = not_sent_reason
            if target_state == "POST_MAY_HAVE_CROSSED":
                updates["post_boundary_at"] = _utc_text(now, "postBoundaryAt")
                transition_details["postBoundaryAt"] = updates["post_boundary_at"]
            if target_state == "ACKNOWLEDGED":
                updates["broker_order_id"] = _official_id(broker_order_id, "broker order id")
                updates["org_no"] = _official_id(org_no, "org number")
                if type(order_date) is not str or not re.fullmatch(r"[0-9]{8}", order_date):
                    raise KisDomesticFunctionalLaneBlocked("order date is invalid")
                updates["order_date"] = order_date
                updates["acknowledged_at"] = _utc_text(now, "acknowledgedAt")
                if order_date != activated.astimezone(KST).strftime("%Y%m%d"):
                    raise KisDomesticFunctionalLaneBlocked(
                        "broker order date does not match the sealed session"
                    )
                transition_details.update(
                    {
                        "brokerOrderId": updates["broker_order_id"],
                        "orgNo": updates["org_no"],
                        "orderDate": order_date,
                        "acknowledgedAt": updates["acknowledged_at"],
                    }
                )
            execution_event_id = ""
            if target_state == "FILLED":
                price = _decimal(fill_price_krw, "fill price", positive=True)
                fee = _decimal(fee_krw, "fee")
                tax = _decimal(tax_krw, "tax")
                loan = _decimal(loan_interest_krw, "loan interest")
                limit_price = _decimal(row["limit_price_krw"], "sealed limit price", positive=True)
                if (
                    action_kind == "NATURAL_BUY" and price > limit_price
                ) or (
                    action_kind == "CLEANUP_SELL" and price < limit_price
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "fill violates the sealed limit price"
                    )
                if not str(row["broker_order_id"]) or not str(row["org_no"]):
                    raise KisDomesticFunctionalLaneBlocked(
                        "fill is missing exact acknowledged broker identity"
                    )
                execution_event_id = f"kis-functional-fill-{uuid.uuid4().hex}"
                updates.update(
                    {
                        "fill_price_krw": fill_price_krw,
                        "fee_krw": fee_krw,
                        "tax_krw": tax_krw,
                        "loan_interest_krw": loan_interest_krw,
                        "filled_at": _utc_text(now, "filledAt"),
                        "program_execution_event_id": execution_event_id,
                    }
                )
                side = "BUY" if str(row["action_kind"]) == "NATURAL_BUY" else "SELL"
                raw = {
                    "schemaVersion": "kis-domestic-functional-program-fill/v1",
                    "claimId": claim_id,
                    "sessionId": str(row["session_id"]),
                    "actionKind": str(row["action_kind"]),
                    "quantity": "1",
                    "priceKrw": fill_price_krw,
                    "feeKrw": format(fee, "f"),
                    "taxKrw": format(tax, "f"),
                    "loanInterestKrw": format(loan, "f"),
                    "occurredAt": _utc_text(now, "occurredAt"),
                    "promotionEligible": False,
                }
                raw_hash = _hash(raw)
                raw["recordHash"] = raw_hash
                raw["signature"] = _signature(
                    self._key, {**raw, "recordHash": raw_hash}
                )
                conn.execute(
                    """INSERT INTO execution_events
                    (event_id, broker_id, order_id, broker_order_id, symbol,
                     side, quantity, price, state, occurred_at, trace_id, raw_json)
                    VALUES (?, 'kis', ?, ?, ?, ?, 1, ?, 'FILLED', ?, ?, ?)""",
                    (
                        execution_event_id,
                        claim_id,
                        str(row["broker_order_id"]),
                        PDNO,
                        side,
                        float(price),
                        _utc_text(now, "occurredAt"),
                        str(row["session_id"]),
                        _canonical(raw).decode("utf-8"),
                    ),
                )
                transition_details.update(
                    {
                        "fillPriceKrw": fill_price_krw,
                        "feeKrw": format(fee, "f"),
                        "taxKrw": format(tax, "f"),
                        "loanInterestKrw": format(loan, "f"),
                        "filledAt": updates["filled_at"],
                        "programExecutionEventId": execution_event_id,
                    }
                )
            next_revision = expected_revision + 1
            transition_head_hash = self._insert_action_transition(
                conn,
                claim_id=claim_id,
                session_id=str(row["session_id"]),
                action_kind=action_kind,
                revision=next_revision,
                state=target_state,
                occurred_at=_utc_text(now, "transition.occurredAt"),
                previous_hash=str(row["transition_head_hash"]),
                details=transition_details,
            )
            updates["transition_head_hash"] = transition_head_hash
            assignments = ", ".join(f"{key}=?" for key in updates)
            params = tuple(updates.values()) + (claim_id, current, expected_revision)
            changed = conn.execute(
                f"UPDATE kis_functional_action SET {assignments}, revision=revision+1 "
                "WHERE claim_id=? AND state=? AND revision=?",
                params,
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalLaneBlocked("action transition CAS failed")
            result = conn.execute(
                "SELECT * FROM kis_functional_action WHERE claim_id=?", (claim_id,)
            ).fetchone()
        return {
            "claimId": claim_id,
            "state": target_state,
            "revision": int(result["revision"]),
            "updatedAt": str(result["updated_at"]),
            "programExecutionEventId": execution_event_id,
        }

    def begin_cleanup(self, *, session_id: str, expected_revision: int, reason: str) -> dict[str, Any]:
        if type(reason) is not str or not reason:
            raise KisDomesticFunctionalLaneBlocked("cleanup reason is missing")
        now = self._now()
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT * FROM kis_functional_session WHERE session_id=?",
                (session_id,),
            ).fetchone()
            if current is None:
                raise KisDomesticFunctionalLaneBlocked("cleanup session is missing")
            self._verify_session_activation(current)
            if now < _parse_utc(current["activated_at"], "session.activatedAt") or now > _parse_utc(
                current["cleanup_ends_at"], "session.cleanupEndsAt"
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "cleanup transition is outside the sealed schedule"
                )
            changed = conn.execute(
                """UPDATE kis_functional_session
                   SET state='CLEANUP', cleanup_started_at=?, cleanup_reason=?,
                       revision=revision+1
                   WHERE session_id=? AND state='ACTIVE' AND revision=?""",
                (
                    _utc_text(now, "cleanupStartedAt"),
                    reason,
                    session_id,
                    expected_revision,
                ),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalLaneBlocked("cleanup session CAS failed")
            row = conn.execute(
                "SELECT revision FROM kis_functional_session WHERE session_id=?", (session_id,)
            ).fetchone()
        return {
            "sessionId": session_id,
            "state": "CLEANUP",
            "reason": reason,
            "revision": int(row["revision"]),
            "realOrdersEnabled": False,
        }

    def finalize(self, *, session_id: str, expected_revision: int) -> dict[str, Any]:
        now = self._now()
        with self.program_ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM kis_functional_session WHERE session_id=?", (session_id,)
            ).fetchone()
            if (
                session is None
                or str(session["state"]) != "CLEANUP"
                or int(session["revision"]) != expected_revision
            ):
                raise KisDomesticFunctionalLaneBlocked("terminal session CAS mismatch")
            activation = self._verify_session_activation(session)
            activated = _parse_utc(session["activated_at"], "session.activatedAt")
            expires = _parse_utc(session["expires_at"], "session.expiresAt")
            cleanup_ends = _parse_utc(
                session["cleanup_ends_at"], "session.cleanupEndsAt"
            )
            if (
                expires - activated != timedelta(seconds=ACTIVE_SECONDS)
                or now < expires
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "scheduled 7200-second window has not elapsed"
                )
            bootstrap_row = conn.execute(
                "SELECT * FROM kis_functional_bootstrap WHERE bootstrap_id=?",
                (str(session["bootstrap_id"]),),
            ).fetchone()
            approval_row = conn.execute(
                "SELECT * FROM kis_functional_approval WHERE approval_id=?",
                (str(session["approval_id"]),),
            ).fetchone()
            evaluation_row = conn.execute(
                "SELECT * FROM kis_functional_evaluation WHERE evaluation_id=?",
                (str(session["evaluation_id"]),),
            ).fetchone()
            trigger_row = conn.execute(
                "SELECT * FROM kis_functional_next_open WHERE trigger_id=?",
                (str(session["trigger_id"]),),
            ).fetchone()
            if any(
                row is None
                for row in (
                    bootstrap_row,
                    approval_row,
                    evaluation_row,
                    trigger_row,
                )
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "terminal authority lineage is incomplete"
                )
            bootstrap = self._verify_stored_record(bootstrap_row)
            approval = self._verify_stored_record(approval_row)
            evaluation = self._verify_stored_record(evaluation_row)
            trigger = self._verify_stored_record(trigger_row)
            self._verify_raw_capture(
                evaluation_row,
                json_column="raw_window_json",
                hash_column="raw_window_hash",
                signature_column="raw_window_signature",
                domain="BAR_WINDOW",
            )
            self._verify_raw_capture(
                trigger_row,
                json_column="raw_trigger_json",
                hash_column="raw_trigger_hash",
                signature_column="raw_trigger_signature",
                domain="NEXT_OPEN",
            )
            if (
                str(bootstrap_row["state"]) != "CLAIMED"
                or str(approval_row["state"]) != "CLAIMED"
                or str(evaluation_row["state"]) != "CONSUMED"
                or str(trigger_row["state"]) != "CONSUMED"
                or activation["bootstrapHash"] != str(bootstrap_row["record_hash"])
                or activation["approvalHash"] != str(approval_row["record_hash"])
                or activation["evaluationHash"] != str(evaluation_row["record_hash"])
                or activation["triggerHash"] != str(trigger_row["record_hash"])
                or approval["bootstrapHash"] != str(bootstrap_row["record_hash"])
                or approval["evaluationHash"] != str(evaluation_row["record_hash"])
                or approval["triggerHash"] != str(trigger_row["record_hash"])
                or trigger["evaluationHash"] != str(evaluation_row["record_hash"])
                or bootstrap["preactivationBaselineHash"]
                != str(session["preactivation_baseline_hash"])
            ):
                raise KisDomesticFunctionalLaneBlocked(
                    "terminal authority lineage mismatch"
                )
            actions = conn.execute(
                "SELECT * FROM kis_functional_action WHERE session_id=? ORDER BY created_at, claim_id",
                (session_id,),
            ).fetchall()
            if any(str(row["state"]) in _NONTERMINAL_ACTION_STATES for row in actions):
                raise KisDomesticFunctionalLaneBlocked("terminal action remains nonterminal")
            transition_chains = {
                str(row["claim_id"]): self._verify_action_chain(conn, row)
                for row in actions
            }
            if len(actions) > 2 or [str(row["action_kind"]) for row in actions] not in (
                [],
                ["NATURAL_BUY"],
                ["NATURAL_BUY", "CLEANUP_SELL"],
            ):
                raise KisDomesticFunctionalLaneBlocked("terminal BUY1/cleanup SELL1 invariant failed")
            buy = next((row for row in actions if row["action_kind"] == "NATURAL_BUY"), None)
            sell = next((row for row in actions if row["action_kind"] == "CLEANUP_SELL"), None)
            if buy is not None and str(buy["state"]) == "FILLED":
                if sell is None or str(sell["state"]) != "FILLED":
                    raise KisDomesticFunctionalLaneBlocked("filled BUY is not exactly cleanup-sold")
            cleanup_started = _parse_utc(
                session["cleanup_started_at"], "session.cleanupStartedAt"
            )
            for action in actions:
                created = _parse_utc(action["created_at"], "action.createdAt")
                filled_at = (
                    _parse_utc(action["filled_at"], "action.filledAt")
                    if str(action["filled_at"])
                    else None
                )
                if str(action["action_kind"]) == "NATURAL_BUY":
                    if (
                        str(action["evaluation_id"]) != str(session["evaluation_id"])
                        or str(action["trigger_id"]) != str(session["trigger_id"])
                        or not activated <= created < expires
                        or (filled_at is not None and not activated <= filled_at <= cleanup_ends)
                    ):
                        raise KisDomesticFunctionalLaneBlocked(
                            "natural BUY durable lineage is invalid"
                        )
                elif (
                    str(action["evaluation_id"])
                    or str(action["trigger_id"])
                    or not cleanup_started <= created <= cleanup_ends
                    or (filled_at is not None and not cleanup_started <= filled_at <= cleanup_ends)
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "cleanup SELL durable lineage is invalid"
                    )
            fill_events = conn.execute(
                "SELECT * FROM execution_events WHERE trace_id=?",
                (session_id,),
            ).fetchall()
            expected_fill_ids = {
                str(row["program_execution_event_id"])
                for row in actions
                if str(row["state"]) == "FILLED"
            }
            if {str(row["event_id"]) for row in fill_events} != expected_fill_ids:
                raise KisDomesticFunctionalLaneBlocked("ProgramLedger fill join is incomplete")
            action_by_event = {
                str(row["program_execution_event_id"]): row
                for row in actions
                if str(row["state"]) == "FILLED"
            }
            for event in fill_events:
                action = action_by_event.get(str(event["event_id"]))
                if action is None:
                    raise KisDomesticFunctionalLaneBlocked(
                        "ProgramLedger contains an unowned fill"
                    )
                try:
                    raw_event = json.loads(str(event["raw_json"]))
                except json.JSONDecodeError as exc:
                    raise KisDomesticFunctionalLaneBlocked(
                        "ProgramLedger raw fill is invalid"
                    ) from exc
                raw_signature = raw_event.pop("signature", None)
                raw_hash = raw_event.get("recordHash")
                unsigned = dict(raw_event)
                unsigned.pop("recordHash", None)
                expected_side = (
                    "BUY"
                    if str(action["action_kind"]) == "NATURAL_BUY"
                    else "SELL"
                )
                if (
                    type(raw_signature) is not str
                    or type(raw_hash) is not str
                    or not hmac.compare_digest(raw_hash, _hash(unsigned))
                    or not hmac.compare_digest(
                        raw_signature,
                        _signature(self._key, {**unsigned, "recordHash": raw_hash}),
                    )
                    or str(event["order_id"]) != str(action["claim_id"])
                    or str(event["broker_order_id"])
                    != str(action["broker_order_id"])
                    or str(event["symbol"]) != PDNO
                    or str(event["side"]) != expected_side
                    or Decimal(str(event["quantity"])) != ORDER_QUANTITY
                    or Decimal(str(event["price"]))
                    != _decimal(action["fill_price_krw"], "action fill price")
                    or str(event["state"]) != "FILLED"
                    or str(event["trace_id"]) != session_id
                    or raw_event.get("claimId") != str(action["claim_id"])
                    or raw_event.get("sessionId") != session_id
                    or raw_event.get("actionKind") != str(action["action_kind"])
                ):
                    raise KisDomesticFunctionalLaneBlocked(
                        "ProgramLedger fill does not exactly match durable action"
                    )
            buy_cost = Decimal("0")
            sell_proceeds = Decimal("0")
            if buy is not None and str(buy["state"]) == "FILLED":
                buy_cost = (
                    _decimal(buy["fill_price_krw"], "BUY fill price")
                    + _decimal(buy["fee_krw"], "BUY fee")
                    + _decimal(buy["tax_krw"], "BUY tax")
                    + _decimal(buy["loan_interest_krw"], "BUY loan interest")
                )
            if sell is not None and str(sell["state"]) == "FILLED":
                sell_proceeds = (
                    _decimal(sell["fill_price_krw"], "SELL fill price")
                    - _decimal(sell["fee_krw"], "SELL fee")
                    - _decimal(sell["tax_krw"], "SELL tax")
                    - _decimal(sell["loan_interest_krw"], "SELL loan interest")
                )
            owner_loss = max(Decimal("0"), buy_cost - sell_proceeds)
            owner_loss_breach = owner_loss >= OWNER_LOSS_LIMIT_KRW
            evidence_body = {
                "schemaVersion": "kis-domestic-functional-terminal/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": session_id,
                "activatedAt": str(session["activated_at"]),
                "activeEndsAt": str(session["expires_at"]),
                "terminalObservedAt": _utc_text(now, "terminalObservedAt"),
                "actualRuntimeSeconds": format(Decimal(str((now - activated).total_seconds())), "f"),
                "scheduledWindowElapsed": True,
                "continuousRuntimeTruthAvailable": False,
                "exact7200ObservationPassed": False,
                "actionKinds": [str(row["action_kind"]) for row in actions],
                "actionStates": [str(row["state"]) for row in actions],
                "programExecutionEventIds": sorted(expected_fill_ids),
                "actionTransitionHeadHashes": sorted(
                    str(row["transition_head_hash"]) for row in actions
                ),
                "actionTransitionCounts": {
                    claim_id: len(chain)
                    for claim_id, chain in sorted(transition_chains.items())
                },
                "bootstrapHash": str(bootstrap_row["record_hash"]),
                "approvalHash": str(approval_row["record_hash"]),
                "activationRecordHash": str(session["activation_record_hash"]),
                "evaluationHash": str(evaluation_row["record_hash"]),
                "rawWindowHash": str(evaluation_row["raw_window_hash"]),
                "triggerHash": str(trigger_row["record_hash"]),
                "rawTriggerHash": str(trigger_row["raw_trigger_hash"]),
                "preactivationBaselineHash": str(
                    session["preactivation_baseline_hash"]
                ),
                "contractEnvelopeHash": str(session["contract_envelope_hash"]),
                "codeManifestHash": str(session["code_manifest_hash"]),
                "ownerLossKrw": format(owner_loss, "f"),
                "ownerLossTriggerReached": owner_loss_breach,
                "ownerLossMustRemainBelowKrw": format(
                    OWNER_LOSS_LIMIT_KRW, "f"
                ),
                "orderQuantity": str(ORDER_QUANTITY),
                "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
                "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
                "buyCountAtMostOne": sum(row["action_kind"] == "NATURAL_BUY" for row in actions) <= 1,
                "cleanupSellCountAtMostOne": sum(row["action_kind"] == "CLEANUP_SELL" for row in actions) <= 1,
                "naturalSellSupported": False,
                "terminalOutcome": (
                    "SAFE_INCOMPLETE_OWNER_LOSS_TRIGGER_REACHED"
                    if owner_loss_breach
                    else "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT"
                ),
                "promotionEligible": False,
                "realE2EPromotionReleased": False,
                "capabilityRevoked": False,
                "internalLedgerAuthorityConsumed": True,
                "officialTerminalAccountTruthAvailable": False,
                "durablePreactivationBaselineCasAvailable": False,
                "networkMutationSurfaceAvailable": False,
            }
            evidence_hash = _hash(evidence_body)
            evidence = {
                **evidence_body,
                "evidenceHash": evidence_hash,
                "signature": _signature(self._key, {**evidence_body, "evidenceHash": evidence_hash}),
            }
            changed = conn.execute(
                """UPDATE kis_functional_session SET state='FINALIZED',
                   final_evidence_json=?, final_evidence_hash=?, revision=revision+1
                   WHERE session_id=? AND state='CLEANUP' AND revision=?""",
                (
                    _canonical(evidence).decode("utf-8"),
                    evidence_hash,
                    session_id,
                    expected_revision,
                ),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalLaneBlocked("terminal session CAS failed")
            for table, key, value in (
                ("kis_functional_bootstrap", "bootstrap_id", str(session["bootstrap_id"])),
                ("kis_functional_approval", "approval_id", str(session["approval_id"])),
            ):
                changed = conn.execute(
                    f"UPDATE {table} SET state='CONSUMED', revision=revision+1 "
                    f"WHERE {key}=? AND state='CLAIMED'",
                    (value,),
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalLaneBlocked("terminal authority consume failed")
        return evidence

    def status(self) -> dict[str, Any]:
        with self.program_ledger.connection() as conn:
            bootstrap = conn.execute(
                "SELECT bootstrap_id, state, revision FROM kis_functional_bootstrap WHERE route=?",
                (ROUTE,),
            ).fetchone()
            sessions = conn.execute(
                "SELECT session_id, state, activated_at, expires_at, revision FROM kis_functional_session"
            ).fetchall()
        return {
            "available": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "atomicTriggerActivationAvailable": False,
            "nativeLaneGrantInstantAvailable": True,
            "graphGrantReceiptProductionAuthorityAvailable": False,
            "freshQuoteAuthorityAvailable": False,
            "rollingSignedAccountPreflightAvailable": False,
            "rawMutationTruthAvailable": False,
            "ambiguityRecoveryAvailable": False,
            "trustedMonotonicHeartbeatAvailable": False,
            "activationBackdatedToBarOpen": False,
            "activationRelative7200ProductionReady": True,
            "officialTerminalTruthAvailable": False,
            "durablePreactivationBaselineCasAvailable": False,
            "sharedRouteLockAvailable": False,
            "externalCapabilityRevokeAvailable": False,
            "route": ROUTE,
            "bootstrap": dict(bootstrap) if bootstrap is not None else None,
            "sessions": [dict(row) for row in sessions],
            "reason": "ISOLATED_DURABLE_LEDGER_ONLY_NO_PRODUCTION_GRAPH",
        }


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": KIS_DOMESTIC_FUNCTIONAL_LANE_PRODUCTION_AVAILABLE,
        "networkAvailable": KIS_DOMESTIC_FUNCTIONAL_LANE_NETWORK_AVAILABLE,
        "mutationAvailable": KIS_DOMESTIC_FUNCTIONAL_LANE_MUTATION_AVAILABLE,
        "atomicTriggerActivationAvailable": False,
        "nativeLaneGrantInstantAvailable": True,
        "graphGrantReceiptProductionAuthorityAvailable": False,
        "freshQuoteAuthorityAvailable": False,
        "rollingSignedAccountPreflightAvailable": False,
        "rawMutationTruthAvailable": False,
        "ambiguityRecoveryAvailable": False,
        "trustedMonotonicHeartbeatAvailable": False,
        "activationBackdatedToBarOpen": False,
        "activationRelative7200ProductionReady": True,
        "officialTerminalTruthAvailable": False,
        "durablePreactivationBaselineCasAvailable": False,
        "sharedRouteLockAvailable": False,
        "externalCapabilityRevokeAvailable": False,
        "route": ROUTE,
        "programLedgerDerived": True,
        "naturalBuyCap": 1,
        "cleanupSellCap": 1,
        "activeSeconds": ACTIVE_SECONDS,
        "terminalOutcome": "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT",
        "promotionEligible": False,
        "reason": "NO_STATE_SERVER_STREAM_BROKER_OR_DURABLE_KEY_CUSTODY_GRAPH",
    }


__all__ = [
    "DurableKisDomesticFunctionalLane",
    "KisDomesticFunctionalLaneBlocked",
    "production_entrypoint_status",
    "sign_kis_domestic_lane_capture",
    "sign_kis_domestic_lane_grant_receipt",
]
