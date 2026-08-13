from __future__ import annotations

"""Disabled, broker-neutral durable mutation journal for the KIS canary.

There is deliberately no sender or HTTP surface in this module.  A later
state-owned graph may consume the sealed request only after installing the
shared final authority boundary and independent official-truth reconciliation.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import re
import sqlite3
from typing import Any, Callable, Mapping

from .kis_domestic_functional_contract import KST, LIVE_ORIGIN, PDNO, ROUTE
from .program_ledger import ProgramLedger


KIS_DOMESTIC_FUNCTIONAL_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MUTATION_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MUTATION_SENDER_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MUTATION_OFFICIAL_TRUTH_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MUTATION_RAW_ARCHIVE_AVAILABLE = False

_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_OFFICIAL_ID = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
_CANCEL_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_SCHEMA_VERSION = "kis-domestic-functional-mutation-schema/v1"
_ZERO_HASH = "0" * 64
_TRUTH_ENDPOINT = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
_TRUTH_TR_ID = "TTTC0081R"
_TRUTH_DOMAIN = b"kis-domestic-functional-trusted-get-truth/v1\x00"
_REQUEST_KEYS = {
    "schemaVersion", "method", "origin", "endpoint", "trId", "orderedQuery",
    "pageCount", "complete", "observedAt", "captureId", "baselineCapturedAt",
    "baselineOrderKeys", "postMarkerAt", "limitPriceKrw",
    "claimId", "sessionId", "accountFingerprint", "credentialConfigurationHash",
    "serverAuthorityKeyIdHash", "signatureHash",
}
_RESPONSE_KEYS = {
    "schemaVersion", "method", "origin", "endpoint", "trId", "httpStatus",
    "captureId", "requestEnvelopeHash", "pages", "observedAt",
    "serverAuthorityKeyIdHash", "signatureHash",
}
_ORDERED_QUERY_TEMPLATE = [
    ["CANO", "ACCOUNT_BOUND_REDACTED"],
    ["ACNT_PRDT_CD", "ACCOUNT_BOUND_REDACTED"],
    ["INQR_STRT_DT", "{date}"], ["INQR_END_DT", "{date}"],
    ["SLL_BUY_DVSN_CD", "00"], ["PDNO", ""], ["CCLD_DVSN", "00"],
    ["INQR_DVSN", "00"], ["INQR_DVSN_3", "00"], ["ORD_GNO_BRNO", ""],
    ["ODNO", ""], ["INQR_DVSN_1", ""], ["EXCG_ID_DVSN_CD", "ALL"],
    ["CTX_AREA_FK100", ""], ["CTX_AREA_NK100", ""],
]


def _expected_ordered_query(observed: datetime) -> list[list[str]]:
    trading_date = observed.astimezone(KST).strftime("%Y%m%d")
    return [
        [key, trading_date if value == "{date}" else value]
        for key, value in _ORDERED_QUERY_TEMPLATE
    ]
_OFFICIAL_STATES = {
    "ACKNOWLEDGED",
    "PARTIAL",
    "FILLED",
    "CANCEL_PENDING",
    "CANCELED",
    "REJECTED",
    "UNKNOWN",
}

_OPERATIONS = {
    "NATURAL_BUY": (_ORDER_ENDPOINT, "TTTC0012U", "BUY", False),
    "CLEANUP_SELL": (_ORDER_ENDPOINT, "TTTC0011U", "SELL", True),
    "CLEANUP_CANCEL": (_CANCEL_ENDPOINT, "TTTC0013U", "CANCEL", True),
}
_TRANSITIONS = {
    "SEALED": {"SENDER_ENTERED", "NOT_SENT"},
    "SENDER_ENTERED": {"POST_MAY_HAVE_CROSSED"},
    "POST_MAY_HAVE_CROSSED": {
        "ACKNOWLEDGED",
        "PARTIAL",
        "FILLED",
        "CANCEL_PENDING",
        "CANCELED",
        "REJECTED",
        "UNKNOWN",
    },
    "ACKNOWLEDGED": {
        "PARTIAL",
        "FILLED",
        "CANCEL_PENDING",
        "CANCELED",
        "REJECTED",
        "UNKNOWN",
    },
    "PARTIAL": {"PARTIAL", "FILLED", "CANCEL_PENDING", "CANCELED", "UNKNOWN"},
    "CANCEL_PENDING": {"CANCELED", "FILLED", "PARTIAL", "UNKNOWN"},
}


class KisDomesticFunctionalMutationBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _utc(value: datetime, label: str) -> str:
    if type(value) is not datetime or value.tzinfo is None:
        raise KisDomesticFunctionalMutationBlocked(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalMutationBlocked(f"{label} is not canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalMutationBlocked(f"{label} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or _utc(parsed, label) != value:
        raise KisDomesticFunctionalMutationBlocked(f"{label} is not canonical UTC")
    return parsed


def _truth_signature(key: bytes, value: Mapping[str, Any]) -> str:
    return hmac.new(
        key, _TRUTH_DOMAIN + _canonical(dict(value)).encode("utf-8"), hashlib.sha256
    ).hexdigest()


def sign_kis_domestic_mutation_truth_capture(
    key: bytes, value: Mapping[str, Any]
) -> str:
    if not isinstance(key, bytes) or len(key) < 32 or not isinstance(value, Mapping):
        raise KisDomesticFunctionalMutationBlocked("truth signer input is invalid")
    return _truth_signature(key, value)


def _identity(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTITY.fullmatch(value):
        raise KisDomesticFunctionalMutationBlocked(f"{label} is invalid")
    return value


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalMutationBlocked(f"{label} is invalid")
    return value


def _official_tuple(value: Mapping[str, Any] | None, *, required: bool) -> dict[str, str]:
    raw = dict(value or {})
    if set(raw) != {"orderDate", "organizationNo", "orderNo"}:
        raise KisDomesticFunctionalMutationBlocked("official order tuple shape is invalid")
    result: dict[str, str] = {}
    for key in ("orderDate", "organizationNo", "orderNo"):
        item = raw[key]
        if type(item) is not str or (item and not _OFFICIAL_ID.fullmatch(item)):
            raise KisDomesticFunctionalMutationBlocked("official order tuple is invalid")
        result[key] = item
    if required and any(not result[key] for key in result):
        raise KisDomesticFunctionalMutationBlocked("exact official order tuple is required")
    if not required and any(result[key] for key in result):
        raise KisDomesticFunctionalMutationBlocked("new order cannot carry an ACK tuple")
    return result


def _validate_payload(
    operation: str, payload: Mapping[str, Any], target: Mapping[str, str]
) -> dict[str, str]:
    body = dict(payload)
    if operation in {"NATURAL_BUY", "CLEANUP_SELL"}:
        required = {"PDNO", "ORD_DVSN", "ORD_QTY", "ORD_UNPR"}
        if set(body) != required:
            raise KisDomesticFunctionalMutationBlocked("order payload schema is invalid")
        if body["PDNO"] != PDNO or body["ORD_DVSN"] != "00" or body["ORD_QTY"] != "1":
            raise KisDomesticFunctionalMutationBlocked("order payload identity/quantity changed")
        price_text = body["ORD_UNPR"]
        if type(price_text) is not str or not price_text.isascii() or not price_text.isdigit():
            raise KisDomesticFunctionalMutationBlocked("order limit price is invalid")
        price = int(price_text)
        if not 1 <= price <= 100_000:
            raise KisDomesticFunctionalMutationBlocked("order notional cap changed")
    elif operation == "CLEANUP_CANCEL":
        required = {
            "KRX_FWDG_ORD_ORGNO",
            "ORGN_ODNO",
            "ORD_DVSN",
            "RVSE_CNCL_DVSN_CD",
            "ORD_QTY",
            "ORD_UNPR",
            "QTY_ALL_ORD_YN",
            "EXCG_ID_DVSN_CD",
        }
        if set(body) != required:
            raise KisDomesticFunctionalMutationBlocked("cancel payload schema is invalid")
        exact = {
            "KRX_FWDG_ORD_ORGNO": target["organizationNo"],
            "ORGN_ODNO": target["orderNo"],
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "1",
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",
            "EXCG_ID_DVSN_CD": "KRX",
        }
        if body != exact:
            raise KisDomesticFunctionalMutationBlocked("cancel payload/owned target changed")
    else:
        raise KisDomesticFunctionalMutationBlocked("mutation operation is invalid")
    if any(type(key) is not str or type(value) is not str for key, value in body.items()):
        raise KisDomesticFunctionalMutationBlocked("mutation payload must be exact text")
    return body


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = [
        tuple(row)
        for row in conn.execute(
            """SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master
               WHERE name LIKE 'kis_mutation_%' OR tbl_name LIKE 'kis_mutation_%'
               ORDER BY type,name"""
        ).fetchall()
    ]
    tables: dict[str, Any] = {}
    for _, name, _, _ in objects:
        if not str(name).startswith("kis_mutation_"):
            continue
        table = str(name).replace('"', '""')
        table_row = conn.execute(
            "SELECT type FROM sqlite_master WHERE name=?", (name,)
        ).fetchone()
        if table_row is None or str(table_row[0]) != "table":
            continue
        indexes = []
        for row in conn.execute(f'PRAGMA index_list("{table}")').fetchall():
            index_name = str(row[1]).replace('"', '""')
            indexes.append(
                (tuple(row), tuple(tuple(x) for x in conn.execute(f'PRAGMA index_xinfo("{index_name}")')))
            )
        tables[str(name)] = {
            "info": tuple(tuple(row) for row in conn.execute(f'PRAGMA table_info("{table}")')),
            "xinfo": tuple(tuple(row) for row in conn.execute(f'PRAGMA table_xinfo("{table}")')),
            "foreignKeys": tuple(tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{table}")')),
            "indexes": tuple(indexes),
        }
    return {"objects": objects, "tables": tables}


class DurableKisDomesticFunctionalMutationJournal:
    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        signer_key: bytes,
        signer_key_id: str,
        official_truth_key: bytes,
        official_truth_key_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalMutationBlocked("exact ProgramLedger is required")
        if not isinstance(signer_key, bytes) or len(signer_key) < 32:
            raise KisDomesticFunctionalMutationBlocked("signer key is invalid")
        if not isinstance(official_truth_key, bytes) or len(official_truth_key) < 32:
            raise KisDomesticFunctionalMutationBlocked("official truth key is invalid")
        signer_key_id = _identity(signer_key_id, "signer key id")
        self.ledger = program_ledger
        self._key = bytes(signer_key)
        self._key_id_hash = hashlib.sha256(signer_key_id.encode("utf-8")).hexdigest()
        self._truth_key = bytes(official_truth_key)
        self._truth_key_id_hash = hashlib.sha256(
            _identity(official_truth_key_id, "official truth key id").encode("utf-8")
        ).hexdigest()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_schema()

    def _now(self) -> str:
        return _utc(self.clock(), "mutation clock")

    def _ensure_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS kis_mutation_schema (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            version TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            signer_key_id_hash TEXT NOT NULL,
            official_truth_key_id_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kis_mutation_request (
            claim_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            tr_id TEXT NOT NULL,
            side TEXT NOT NULL,
            cleanup_only INTEGER NOT NULL CHECK(cleanup_only IN (0,1)),
            account_fingerprint TEXT NOT NULL,
            credential_configuration_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            authority_intent_json TEXT NOT NULL,
            authority_intent_hash TEXT NOT NULL UNIQUE,
            authority_revision INTEGER NOT NULL,
            state TEXT NOT NULL,
            sender_entered_at TEXT NOT NULL DEFAULT '',
            post_marker_at TEXT NOT NULL DEFAULT '',
            target_order_date TEXT NOT NULL DEFAULT '',
            target_organization_no TEXT NOT NULL DEFAULT '',
            target_order_no TEXT NOT NULL DEFAULT '',
            target_order_side TEXT NOT NULL DEFAULT '',
            ack_order_date TEXT NOT NULL DEFAULT '',
            ack_organization_no TEXT NOT NULL DEFAULT '',
            ack_order_no TEXT NOT NULL DEFAULT '',
            transition_head_hash TEXT NOT NULL,
            revision INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK((operation='CLEANUP_CANCEL') =
                  (target_order_date<>'' AND target_organization_no<>'' AND target_order_no<>'')),
            CHECK((operation='CLEANUP_CANCEL') = (target_order_side IN ('BUY','SELL')))
        );
        CREATE TABLE IF NOT EXISTS kis_mutation_transition (
            claim_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            state TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            previous_hash TEXT NOT NULL,
            record_json TEXT NOT NULL,
            record_hash TEXT NOT NULL UNIQUE,
            signature TEXT NOT NULL,
            signer_key_id_hash TEXT NOT NULL,
            PRIMARY KEY(claim_id, revision)
        );
        CREATE TABLE IF NOT EXISTS kis_mutation_raw_archive (
            archive_id TEXT PRIMARY KEY,
            claim_id TEXT NOT NULL,
            archive_kind TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            tr_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            envelope_json TEXT NOT NULL,
            envelope_hash TEXT NOT NULL,
            signature TEXT NOT NULL,
            signer_key_id_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS kis_mutation_official_truth (
            truth_id TEXT PRIMARY KEY,
            claim_id TEXT NOT NULL,
            request_archive_id TEXT NOT NULL UNIQUE,
            response_archive_id TEXT NOT NULL UNIQUE,
            complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            page_count INTEGER NOT NULL,
            official_state TEXT NOT NULL,
            order_date TEXT NOT NULL,
            organization_no TEXT NOT NULL,
            order_no TEXT NOT NULL,
            record_json TEXT NOT NULL,
            record_hash TEXT NOT NULL UNIQUE,
            signature TEXT NOT NULL,
            signer_key_id_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS kis_mutation_session_idx
            ON kis_mutation_request(session_id, created_at, claim_id);
        CREATE UNIQUE INDEX IF NOT EXISTS kis_mutation_ack_identity_idx
            ON kis_mutation_request(ack_order_date, ack_organization_no, ack_order_no)
            WHERE ack_order_date<>'' AND ack_organization_no<>'' AND ack_order_no<>'';
        CREATE UNIQUE INDEX IF NOT EXISTS kis_mutation_cancel_target_idx
            ON kis_mutation_request(session_id, target_order_date,
                                    target_organization_no, target_order_no)
            WHERE operation='CLEANUP_CANCEL';
        """
        statements = [item.strip() for item in ddl.split(";") if item.strip()]
        expected = sqlite3.connect(":memory:")
        try:
            for statement in statements:
                expected.execute(statement)
            expected_hash = _hash(_schema_snapshot(expected))
        finally:
            expected.close()
        with self.ledger.connection() as conn:
            before = _schema_snapshot(conn)
            if before["objects"] and not hmac.compare_digest(_hash(before), expected_hash):
                raise KisDomesticFunctionalMutationBlocked("mutation SQLite schema fingerprint mismatch")
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                conn.execute(statement)
            if not hmac.compare_digest(_hash(_schema_snapshot(conn)), expected_hash):
                raise KisDomesticFunctionalMutationBlocked("mutation SQLite schema fingerprint mismatch")
            row = conn.execute(
                """SELECT singleton,version,schema_hash,signer_key_id_hash,
                          official_truth_key_id_hash FROM kis_mutation_schema"""
            ).fetchall()
            expected_row = (
                1, _SCHEMA_VERSION, expected_hash, self._key_id_hash,
                self._truth_key_id_hash,
            )
            if not row:
                conn.execute("INSERT INTO kis_mutation_schema VALUES (?,?,?,?,?)", expected_row)
            elif len(row) != 1 or tuple(row[0]) != expected_row:
                raise KisDomesticFunctionalMutationBlocked("mutation schema manifest mismatch")

    def _transition(
        self,
        conn: sqlite3.Connection,
        *,
        claim_id: str,
        revision: int,
        state: str,
        previous_hash: str,
        occurred_at: str,
    ) -> str:
        body = {
            "schemaVersion": "kis-domestic-functional-mutation-transition/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "claimId": claim_id,
            "revision": revision,
            "state": state,
            "occurredAt": occurred_at,
            "previousHash": previous_hash,
            "promotionEligible": False,
        }
        record = _canonical(body)
        digest = hashlib.sha256(record.encode("utf-8")).hexdigest()
        signature = hmac.new(self._key, record.encode("utf-8"), hashlib.sha256).hexdigest()
        conn.execute(
            "INSERT INTO kis_mutation_transition VALUES (?,?,?,?,?,?,?,?,?)",
            (
                claim_id,
                revision,
                state,
                occurred_at,
                previous_hash,
                record,
                digest,
                signature,
                self._key_id_hash,
            ),
        )
        return digest

    def seal_request(
        self,
        *,
        claim_id: str,
        session_id: str,
        operation: str,
        endpoint: str,
        tr_id: str,
        side: str,
        account_fingerprint: str,
        credential_configuration_hash: str,
        authority_revision: int,
        payload: Mapping[str, Any],
        owned_order_key: Mapping[str, Any],
        owned_order_side: str = "",
    ) -> dict[str, Any]:
        claim_id = _identity(claim_id, "claim id")
        session_id = _identity(session_id, "session id")
        spec = _OPERATIONS.get(operation)
        if spec is None or (endpoint, tr_id, side, operation != "NATURAL_BUY") != spec:
            raise KisDomesticFunctionalMutationBlocked("operation endpoint/TR/side allowlist mismatch")
        account = _sha(account_fingerprint, "account fingerprint")
        credential = _sha(credential_configuration_hash, "credential configuration hash")
        if type(authority_revision) is not int or authority_revision < 1:
            raise KisDomesticFunctionalMutationBlocked("authority revision is invalid")
        if not isinstance(payload, Mapping):
            raise KisDomesticFunctionalMutationBlocked("payload is invalid")
        order_key = _official_tuple(owned_order_key, required=operation == "CLEANUP_CANCEL")
        if operation == "CLEANUP_CANCEL":
            if type(owned_order_side) is not str or owned_order_side not in {"BUY", "SELL"}:
                raise KisDomesticFunctionalMutationBlocked("cancel target original side is required")
        elif owned_order_side != "":
            raise KisDomesticFunctionalMutationBlocked("new order cannot carry target side")
        payload_value = _validate_payload(operation, payload, order_key)
        payload_hash = _hash(payload_value)
        intent = {
            "route": ROUTE,
            "pdno": PDNO,
            "operation": operation,
            "claimId": claim_id,
            "sessionId": session_id,
            "authorityRevision": authority_revision,
            "ownedOrderKey": order_key,
            "ownedOrderSide": owned_order_side,
            "accountFingerprint": account,
            "credentialConfigurationHash": credential,
            "endpoint": endpoint,
            "trId": tr_id,
            "side": side,
            "cleanupOnly": bool(spec[3]),
            "payloadHash": payload_hash,
        }
        intent_hash = _hash(intent)
        now = self._now()
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            head = self._transition(
                conn,
                claim_id=claim_id,
                revision=1,
                state="SEALED",
                previous_hash=_ZERO_HASH,
                occurred_at=now,
            )
            try:
                conn.execute(
                    """INSERT INTO kis_mutation_request
                    (claim_id,session_id,operation,endpoint,tr_id,side,cleanup_only,
                     account_fingerprint,credential_configuration_hash,payload_json,
                     payload_hash,authority_intent_json,authority_intent_hash,
                     authority_revision,state,target_order_date,target_organization_no,
                     target_order_no,target_order_side,ack_order_date,ack_organization_no,ack_order_no,
                     transition_head_hash,
                     revision,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'SEALED',?,?,?,?,?,?,?,?,1,?,?)""",
                    (
                        claim_id, session_id, operation, endpoint, tr_id, side,
                        int(spec[3]), account, credential, _canonical(payload_value),
                        payload_hash, _canonical(intent), intent_hash, authority_revision,
                        order_key["orderDate"], order_key["organizationNo"],
                        order_key["orderNo"], owned_order_side, "", "", "", head, now, now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise KisDomesticFunctionalMutationBlocked("mutation request identity is not unique") from exc
        return {"claimId": claim_id, "state": "SEALED", "revision": 1, "requestHash": payload_hash, "authorityIntent": intent, "authorityIntentHash": intent_hash}

    def _verify_request_locked(
        self, conn: sqlite3.Connection, request: sqlite3.Row
    ) -> None:
        try:
            payload = json.loads(str(request["payload_json"]))
            intent = json.loads(str(request["authority_intent_json"]))
        except (json.JSONDecodeError, TypeError) as exc:
            raise KisDomesticFunctionalMutationBlocked(
                "mutation request archive is malformed"
            ) from exc
        operation = str(request["operation"])
        spec = _OPERATIONS.get(operation)
        if spec is None:
            raise KisDomesticFunctionalMutationBlocked(
                "mutation request operation is invalid"
            )
        target = {
            "orderDate": str(request["target_order_date"]),
            "organizationNo": str(request["target_organization_no"]),
            "orderNo": str(request["target_order_no"]),
        }
        target = _official_tuple(target, required=operation == "CLEANUP_CANCEL")
        normalized_payload = _validate_payload(operation, payload, target)
        payload_hash = _hash(normalized_payload)
        expected_intent = {
            "route": ROUTE,
            "pdno": PDNO,
            "operation": operation,
            "claimId": str(request["claim_id"]),
            "sessionId": str(request["session_id"]),
            "authorityRevision": int(request["authority_revision"]),
            "ownedOrderKey": target,
            "ownedOrderSide": str(request["target_order_side"]),
            "accountFingerprint": str(request["account_fingerprint"]),
            "credentialConfigurationHash": str(
                request["credential_configuration_hash"]
            ),
            "endpoint": str(request["endpoint"]),
            "trId": str(request["tr_id"]),
            "side": str(request["side"]),
            "cleanupOnly": bool(request["cleanup_only"]),
            "payloadHash": payload_hash,
        }
        if (
            (request["endpoint"], request["tr_id"], request["side"], bool(request["cleanup_only"]))
            != spec
            or type(request["cleanup_only"]) is not int
            or int(request["cleanup_only"]) not in {0, 1}
            or type(request["authority_revision"]) is not int
            or int(request["authority_revision"]) < 1
            or (
                operation == "CLEANUP_CANCEL"
                and str(request["target_order_side"]) not in {"BUY", "SELL"}
            )
            or (
                operation != "CLEANUP_CANCEL"
                and str(request["target_order_side"]) != ""
            )
            or not _SHA256.fullmatch(str(request["account_fingerprint"]))
            or not _SHA256.fullmatch(
                str(request["credential_configuration_hash"])
            )
            or not hmac.compare_digest(payload_hash, str(request["payload_hash"]))
            or intent != expected_intent
            or not hmac.compare_digest(
                _hash(expected_intent), str(request["authority_intent_hash"])
            )
        ):
            raise KisDomesticFunctionalMutationBlocked(
                "mutation request archive failed verification"
            )
        ack = {
            "orderDate": str(request["ack_order_date"]),
            "organizationNo": str(request["ack_organization_no"]),
            "orderNo": str(request["ack_order_no"]),
        }
        if any(ack.values()):
            _official_tuple(ack, required=True)

        transitions = conn.execute(
            "SELECT * FROM kis_mutation_transition WHERE claim_id=? ORDER BY revision",
            (str(request["claim_id"]),),
        ).fetchall()
        previous = _ZERO_HASH
        for revision, transition in enumerate(transitions, start=1):
            try:
                body = json.loads(str(transition["record_json"]))
            except (json.JSONDecodeError, TypeError) as exc:
                raise KisDomesticFunctionalMutationBlocked(
                    "mutation transition chain is malformed"
                ) from exc
            text = _canonical(body)
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            signature = hmac.new(
                self._key, text.encode("utf-8"), hashlib.sha256
            ).hexdigest()
            if (
                int(transition["revision"]) != revision
                or str(transition["previous_hash"]) != previous
                or body
                != {
                    "schemaVersion": "kis-domestic-functional-mutation-transition/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "claimId": str(request["claim_id"]),
                    "revision": revision,
                    "state": str(transition["state"]),
                    "occurredAt": str(transition["occurred_at"]),
                    "previousHash": previous,
                    "promotionEligible": False,
                }
                or not hmac.compare_digest(
                    digest, str(transition["record_hash"])
                )
                or not hmac.compare_digest(
                    signature, str(transition["signature"])
                )
                or not hmac.compare_digest(
                    self._key_id_hash, str(transition["signer_key_id_hash"])
                )
            ):
                raise KisDomesticFunctionalMutationBlocked(
                    "mutation transition chain failed verification"
                )
            previous = digest
        if (
            len(transitions) != int(request["revision"])
            or previous != str(request["transition_head_hash"])
            or str(transitions[-1]["state"]) != str(request["state"])
        ):
            raise KisDomesticFunctionalMutationBlocked(
                "mutation transition chain is incomplete"
            )

    def transition(
        self,
        *,
        claim_id: str,
        expected_revision: int,
        target_state: str,
        official_order_key: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        claim_id = _identity(claim_id, "claim id")
        if type(expected_revision) is not int or expected_revision < 1:
            raise KisDomesticFunctionalMutationBlocked("expected revision is invalid")
        now = self._now()
        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM kis_mutation_request WHERE claim_id=?", (claim_id,)).fetchone()
            if row is None or int(row["revision"]) != expected_revision:
                raise KisDomesticFunctionalMutationBlocked("mutation revision changed")
            self._verify_request_locked(conn, row)
            current = str(row["state"])
            if target_state not in _TRANSITIONS.get(current, set()):
                raise KisDomesticFunctionalMutationBlocked("mutation state transition is invalid")
            if target_state in _OFFICIAL_STATES:
                raise KisDomesticFunctionalMutationBlocked(
                    "terminal mutation transition requires complete official capture"
                )
            if target_state == "NOT_SENT" and current != "SEALED":
                raise KisDomesticFunctionalMutationBlocked("NOT_SENT is only valid before sender entry")
            order_key = {
                "orderDate": str(row["ack_order_date"]),
                "organizationNo": str(row["ack_organization_no"]),
                "orderNo": str(row["ack_order_no"]),
            }
            if target_state in {"ACKNOWLEDGED", "PARTIAL", "FILLED", "CANCEL_PENDING", "CANCELED"}:
                supplied = _official_tuple(official_order_key, required=True)
                if any(order_key.values()) and supplied != order_key:
                    raise KisDomesticFunctionalMutationBlocked("official ACK tuple changed")
                order_key = supplied
            elif official_order_key is not None:
                raise KisDomesticFunctionalMutationBlocked("official tuple is not allowed for this state")
            revision = expected_revision + 1
            head = self._transition(
                conn,
                claim_id=claim_id,
                revision=revision,
                state=target_state,
                previous_hash=str(row["transition_head_hash"]),
                occurred_at=now,
            )
            sender_at = now if target_state == "SENDER_ENTERED" else str(row["sender_entered_at"])
            marker_at = now if target_state == "POST_MAY_HAVE_CROSSED" else str(row["post_marker_at"])
            try:
                changed = conn.execute(
                    """UPDATE kis_mutation_request SET state=?,sender_entered_at=?,
                       post_marker_at=?,ack_order_date=?,ack_organization_no=?,ack_order_no=?,
                       transition_head_hash=?,revision=?,updated_at=?
                       WHERE claim_id=? AND revision=?""",
                    (target_state, sender_at, marker_at, order_key["orderDate"],
                     order_key["organizationNo"], order_key["orderNo"], head,
                     revision, now, claim_id, expected_revision),
                ).rowcount
            except sqlite3.IntegrityError as exc:
                raise KisDomesticFunctionalMutationBlocked("official ACK tuple is not unique") from exc
            if changed != 1:
                raise KisDomesticFunctionalMutationBlocked("mutation CAS failed")
        return {"claimId": claim_id, "state": target_state, "revision": revision, "officialOrderKey": order_key}

    def read(self, claim_id: str) -> dict[str, Any]:
        claim_id = _identity(claim_id, "claim id")
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT * FROM kis_mutation_request WHERE claim_id=?", (claim_id,)).fetchone()
            if row is None:
                raise KisDomesticFunctionalMutationBlocked("mutation request is missing")
            self._verify_request_locked(conn, row)
            result = dict(row)
        if result["state"] in _OFFICIAL_STATES:
            self.verify_integrity(claim_id)
        return result

    def _archive(
        self,
        conn: sqlite3.Connection,
        *,
        archive_id: str,
        claim_id: str,
        archive_kind: str,
        endpoint: str,
        tr_id: str,
        observed_at: str,
        http_status: int,
        envelope: Mapping[str, Any],
    ) -> str:
        archive_id = _identity(archive_id, "archive id")
        if archive_kind not in {"OFFICIAL_REQUEST", "OFFICIAL_RESPONSE"}:
            raise KisDomesticFunctionalMutationBlocked("archive kind is invalid")
        if type(http_status) is not int or not 100 <= http_status <= 599:
            raise KisDomesticFunctionalMutationBlocked("HTTP status is invalid")
        text = _canonical(dict(envelope))
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        signature = hmac.new(self._key, text.encode("utf-8"), hashlib.sha256).hexdigest()
        conn.execute(
            """INSERT INTO kis_mutation_raw_archive
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                archive_id,
                claim_id,
                archive_kind,
                endpoint,
                tr_id,
                observed_at,
                http_status,
                text,
                digest,
                signature,
                self._key_id_hash,
            ),
        )
        return digest

    def _verify_truth_capture_pair(
        self,
        request_envelope: Mapping[str, Any],
        response_envelope: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], datetime]:
        if not isinstance(request_envelope, Mapping) or set(request_envelope) != _REQUEST_KEYS:
            raise KisDomesticFunctionalMutationBlocked("official request envelope is invalid")
        request = dict(request_envelope)
        request_signature = request.pop("signatureHash")
        if (
            type(request_signature) is not str
            or not _SHA256.fullmatch(request_signature)
            or not hmac.compare_digest(
                request_signature, _truth_signature(self._truth_key, request)
            )
        ):
            raise KisDomesticFunctionalMutationBlocked(
                "official request trusted GET signature mismatch"
            )
        observed = _parse_utc(request.get("observedAt"), "official request observedAt")
        baseline_at = _parse_utc(
            request.get("baselineCapturedAt"), "official baseline capturedAt"
        )
        post_marker = _parse_utc(request.get("postMarkerAt"), "official post marker")
        if not baseline_at <= post_marker <= observed:
            raise KisDomesticFunctionalMutationBlocked(
                "official request causal time lineage is invalid"
            )
        exact_request = {
            "schemaVersion": "kis-domestic-functional-mutation-official-request/v2",
            "method": "GET", "origin": LIVE_ORIGIN, "endpoint": _TRUTH_ENDPOINT,
            "trId": _TRUTH_TR_ID, "orderedQuery": _expected_ordered_query(observed),
            "complete": True, "serverAuthorityKeyIdHash": self._truth_key_id_hash,
        }
        for key, expected in exact_request.items():
            if type(request.get(key)) is not type(expected) or request.get(key) != expected:
                raise KisDomesticFunctionalMutationBlocked(
                    f"official request {key} mismatch"
                )
        if (
            type(request.get("pageCount")) is not int
            or request["pageCount"] < 1
            or type(request.get("captureId")) is not str
            or not _IDENTITY.fullmatch(request["captureId"])
            or type(request.get("claimId")) is not str
            or not _IDENTITY.fullmatch(request["claimId"])
            or type(request.get("sessionId")) is not str
            or not _IDENTITY.fullmatch(request["sessionId"])
            or type(request.get("accountFingerprint")) is not str
            or not _SHA256.fullmatch(request["accountFingerprint"])
            or type(request.get("credentialConfigurationHash")) is not str
            or not _SHA256.fullmatch(request["credentialConfigurationHash"])
        ):
            raise KisDomesticFunctionalMutationBlocked("official truth request is incomplete")
        limit_price = request.get("limitPriceKrw")
        if type(limit_price) is not str or not limit_price.isascii() or not limit_price.isdigit() or int(limit_price) < 1:
            raise KisDomesticFunctionalMutationBlocked("official request limit price is invalid")
        baseline_rows = request.get("baselineOrderKeys")
        if type(baseline_rows) is not list:
            raise KisDomesticFunctionalMutationBlocked("official baseline rows are invalid")
        normalized_baseline: list[dict[str, Any]] = []
        baseline_identities: set[tuple[str, str, str]] = set()
        for item in baseline_rows:
            if not isinstance(item, Mapping) or set(item) != {
                "orderDate", "organizationNo", "orderNo", "pdno", "side",
                "limitPriceKrw",
            }:
                raise KisDomesticFunctionalMutationBlocked("official baseline row is malformed")
            key = _official_tuple(
                {name: item[name] for name in ("orderDate", "organizationNo", "orderNo")},
                required=True,
            )
            identity = tuple(key.values())
            if (
                identity in baseline_identities
                or type(item.get("pdno")) is not str
                or not re.fullmatch(r"[0-9]{6}", item["pdno"])
                or item.get("side") not in {"BUY", "SELL"}
            ):
                raise KisDomesticFunctionalMutationBlocked("official baseline row is not unique/exact")
            price = item.get("limitPriceKrw")
            if type(price) is not str or not price.isascii() or not price.isdigit() or int(price) < 1:
                raise KisDomesticFunctionalMutationBlocked("official baseline price is invalid")
            baseline_identities.add(identity)
            normalized_baseline.append({
                **key, "pdno": item["pdno"], "side": item["side"],
                "limitPriceKrw": price,
            })
        request["baselineOrderKeys"] = normalized_baseline
        request["signatureHash"] = request_signature

        if not isinstance(response_envelope, Mapping) or set(response_envelope) != _RESPONSE_KEYS:
            raise KisDomesticFunctionalMutationBlocked("official response envelope is invalid")
        response = dict(response_envelope)
        response_signature = response.pop("signatureHash")
        if (
            type(response_signature) is not str
            or not _SHA256.fullmatch(response_signature)
            or not hmac.compare_digest(
                response_signature, _truth_signature(self._truth_key, response)
            )
        ):
            raise KisDomesticFunctionalMutationBlocked(
                "official response trusted GET signature mismatch"
            )
        exact_response = {
            "schemaVersion": "kis-domestic-functional-mutation-official-response/v2",
            "method": "GET", "origin": LIVE_ORIGIN, "endpoint": _TRUTH_ENDPOINT,
            "trId": _TRUTH_TR_ID, "httpStatus": 200,
            "captureId": request["captureId"],
            "requestEnvelopeHash": _hash(request),
            "observedAt": request["observedAt"],
            "serverAuthorityKeyIdHash": self._truth_key_id_hash,
        }
        for key, expected in exact_response.items():
            if type(response.get(key)) is not type(expected) or response.get(key) != expected:
                raise KisDomesticFunctionalMutationBlocked(
                    f"official response {key} mismatch"
                )
        pages = response.get("pages")
        if type(pages) is not list or len(pages) != request["pageCount"]:
            raise KisDomesticFunctionalMutationBlocked("official response page count mismatch")
        official_rows: list[dict[str, Any]] = []
        expected_cursor = {"fk100": "", "nk100": ""}
        seen_keys: set[tuple[str, str, str]] = set()
        for index, page in enumerate(pages, start=1):
            if not isinstance(page, Mapping) or set(page) != {
                "pageNumber", "requestCursor", "responseCursor", "trCont",
                "rawBody", "rawBodyHash",
            }:
                raise KisDomesticFunctionalMutationBlocked("official truth page is malformed")
            if (
                type(page.get("pageNumber")) is not int
                or page["pageNumber"] != index
                or page.get("requestCursor") != expected_cursor
                or not isinstance(page.get("responseCursor"), Mapping)
                or set(page["responseCursor"]) != {"fk100", "nk100"}
                or any(type(value) is not str for value in page["responseCursor"].values())
            ):
                raise KisDomesticFunctionalMutationBlocked("official truth cursor chain is malformed")
            raw_body = page.get("rawBody")
            if (
                not isinstance(raw_body, Mapping)
                or set(raw_body) != {"rt_cd", "output1"}
                or raw_body.get("rt_cd") != "0"
                or type(raw_body.get("output1")) is not list
                or type(page.get("rawBodyHash")) is not str
                or not hmac.compare_digest(page["rawBodyHash"], _hash(raw_body))
            ):
                raise KisDomesticFunctionalMutationBlocked("official raw page/hash is malformed")
            terminal = index == len(pages)
            response_cursor = dict(page["responseCursor"])
            if terminal:
                if page.get("trCont") not in {"", "D", "E"} or response_cursor != {"fk100": "", "nk100": ""}:
                    raise KisDomesticFunctionalMutationBlocked("official truth pagination is truncated")
            elif page.get("trCont") not in {"M", "F"} or response_cursor == {"fk100": "", "nk100": ""}:
                raise KisDomesticFunctionalMutationBlocked("official truth pagination is truncated")
            expected_cursor = response_cursor
            for raw in raw_body["output1"]:
                if not isinstance(raw, Mapping) or set(raw) != {
                    "orderDate", "organizationNo", "orderNo", "pdno", "side",
                    "state", "orderedQty", "filledQty", "limitPriceKrw",
                    "orderObservedAt",
                }:
                    raise KisDomesticFunctionalMutationBlocked("official truth row is malformed")
                key = _official_tuple(
                    {name: raw[name] for name in ("orderDate", "organizationNo", "orderNo")},
                    required=True,
                )
                identity = tuple(key.values())
                if identity in seen_keys:
                    raise KisDomesticFunctionalMutationBlocked("official truth contains duplicate identity")
                seen_keys.add(identity)
                row_observed = _parse_utc(raw.get("orderObservedAt"), "official row observedAt")
                if row_observed > observed:
                    raise KisDomesticFunctionalMutationBlocked("official row is future-dated")
                if (
                    type(raw.get("pdno")) is not str
                    or not re.fullmatch(r"[0-9]{6}", raw["pdno"])
                    or raw.get("side") not in {"BUY", "SELL"}
                    or raw.get("state") not in _OFFICIAL_STATES
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "official row identity/state is invalid"
                    )
                for label in ("orderedQty", "filledQty", "limitPriceKrw"):
                    value = raw.get(label)
                    try:
                        parsed = Decimal(value) if type(value) is str else Decimal("NaN")
                    except InvalidOperation:
                        parsed = Decimal("NaN")
                    if not parsed.is_finite() or parsed < 0:
                        raise KisDomesticFunctionalMutationBlocked(f"official row {label} is invalid")
                if Decimal(raw["limitPriceKrw"]) <= 0:
                    raise KisDomesticFunctionalMutationBlocked(
                        "official row limit price is invalid"
                    )
                official_rows.append({**dict(raw), **key})
        response["signatureHash"] = response_signature
        return request, response, official_rows, observed

    def reconcile_official_truth(
        self,
        *,
        claim_id: str,
        expected_revision: int,
        truth_id: str,
        request_archive_id: str,
        response_archive_id: str,
        request_envelope: Mapping[str, Any],
        response_envelope: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Resolve a mutation only from a complete exact official GET capture."""

        claim_id = _identity(claim_id, "claim id")
        truth_id = _identity(truth_id, "truth id")
        request_envelope, response_envelope, official_rows, observed = (
            self._verify_truth_capture_pair(request_envelope, response_envelope)
        )
        observed_at = request_envelope["observedAt"]
        trusted_now = _parse_utc(self._now(), "mutation reconciliation clock")
        if trusted_now < observed or trusted_now > observed + timedelta(seconds=5):
            raise KisDomesticFunctionalMutationBlocked(
                "official truth capture is stale or future-dated"
            )

        with self.ledger.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM kis_mutation_request WHERE claim_id=?", (claim_id,)
            ).fetchone()
            if row is None or int(row["revision"]) != expected_revision:
                raise KisDomesticFunctionalMutationBlocked("mutation revision changed")
            self._verify_request_locked(conn, row)
            for key, expected in {
                "claimId": str(row["claim_id"]),
                "sessionId": str(row["session_id"]),
                "accountFingerprint": str(row["account_fingerprint"]),
                "credentialConfigurationHash": str(
                    row["credential_configuration_hash"]
                ),
            }.items():
                actual = request_envelope.get(key)
                if type(actual) is not str or not hmac.compare_digest(actual, expected):
                    raise KisDomesticFunctionalMutationBlocked(
                        f"official truth {key} binding mismatch"
                    )
            if str(row["state"]) not in {
                "POST_MAY_HAVE_CROSSED",
                "ACKNOWLEDGED",
                "PARTIAL",
                "CANCEL_PENDING",
                "UNKNOWN",
            }:
                raise KisDomesticFunctionalMutationBlocked("official truth cannot resolve this state")
            if (
                not str(row["post_marker_at"])
                or request_envelope["postMarkerAt"] != str(row["post_marker_at"])
                or _parse_utc(str(row["post_marker_at"]), "stored post marker")
                > observed
            ):
                raise KisDomesticFunctionalMutationBlocked(
                    "official truth post marker lineage mismatch"
                )
            payload = json.loads(str(row["payload_json"]))
            operation = str(row["operation"])
            if operation == "CLEANUP_CANCEL":
                baseline_target = [
                    item
                    for item in request_envelope["baselineOrderKeys"]
                    if (
                        item["orderDate"] == str(row["target_order_date"])
                        and item["organizationNo"] == str(row["target_organization_no"])
                        and item["orderNo"] == str(row["target_order_no"])
                        and item["pdno"] == PDNO
                    )
                ]
                if (
                    len(baseline_target) != 1
                    or baseline_target[0]["side"] != str(row["target_order_side"])
                    or request_envelope["limitPriceKrw"]
                    != baseline_target[0]["limitPriceKrw"]
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "cleanup cancel original order baseline mapping mismatch"
                    )
            elif request_envelope["limitPriceKrw"] != payload["ORD_UNPR"]:
                raise KisDomesticFunctionalMutationBlocked(
                    "official truth request price changed"
                )

            expected_key = {
                "orderDate": str(row["ack_order_date"]),
                "organizationNo": str(row["ack_organization_no"]),
                "orderNo": str(row["ack_order_no"]),
            }
            if not any(expected_key.values()) and str(row["operation"]) == "CLEANUP_CANCEL":
                expected_key = {
                    "orderDate": str(row["target_order_date"]),
                    "organizationNo": str(row["target_organization_no"]),
                    "orderNo": str(row["target_order_no"]),
                }
            expected_side = (
                str(row["target_order_side"])
                if operation == "CLEANUP_CANCEL"
                else str(row["side"])
            )
            baseline_identities = {
                (item["orderDate"], item["organizationNo"], item["orderNo"])
                for item in request_envelope["baselineOrderKeys"]
            }
            post_marker = _parse_utc(request_envelope["postMarkerAt"], "post marker")
            matches: list[dict[str, Any]] = []
            for raw in official_rows:
                key = {
                    "orderDate": raw["orderDate"],
                    "organizationNo": raw["organizationNo"],
                    "orderNo": raw["orderNo"],
                }
                key = _official_tuple(key, required=True)
                if raw["pdno"] != PDNO:
                    continue
                if any(expected_key.values()) and key != expected_key:
                    continue
                if raw["side"] != expected_side:
                    continue
                if not any(expected_key.values()):
                    if tuple(key.values()) in baseline_identities:
                        continue
                    row_observed = _parse_utc(
                        raw["orderObservedAt"], "official row observedAt"
                    )
                    if (
                        row_observed < post_marker
                        or raw["limitPriceKrw"] != request_envelope["limitPriceKrw"]
                    ):
                        continue
                state = raw["state"]
                if state not in _OFFICIAL_STATES:
                    raise KisDomesticFunctionalMutationBlocked("official truth state is invalid")
                ordered_qty = Decimal(raw["orderedQty"])
                filled_qty = Decimal(raw["filledQty"])
                if ordered_qty != 1 or not 0 <= filled_qty <= 1:
                    raise KisDomesticFunctionalMutationBlocked("official truth quantities changed")
                matches.append({"key": key, "state": state, "filledQty": filled_qty})
            if len(matches) > 1:
                raise KisDomesticFunctionalMutationBlocked("official truth identity is not unique")
            if not matches:
                target_state = "UNKNOWN"
                resolved_key = expected_key
            else:
                target_state = matches[0]["state"]
                resolved_key = matches[0]["key"]
                if target_state == "ACKNOWLEDGED" and matches[0]["filledQty"] != 0:
                    raise KisDomesticFunctionalMutationBlocked("official truth status/quantity mismatch")
                if target_state == "PARTIAL" and not 0 < matches[0]["filledQty"] < 1:
                    raise KisDomesticFunctionalMutationBlocked("partial fill quantity is not exactly provable")
                if target_state == "FILLED" and matches[0]["filledQty"] != 1:
                    raise KisDomesticFunctionalMutationBlocked("filled quantity is not exact")

            request_hash = self._archive(
                conn,
                archive_id=request_archive_id,
                claim_id=claim_id,
                archive_kind="OFFICIAL_REQUEST",
                endpoint=_TRUTH_ENDPOINT,
                tr_id=_TRUTH_TR_ID,
                observed_at=observed_at,
                http_status=200,
                envelope=request_envelope,
            )
            response_hash = self._archive(
                conn,
                archive_id=response_archive_id,
                claim_id=claim_id,
                archive_kind="OFFICIAL_RESPONSE",
                endpoint=_TRUTH_ENDPOINT,
                tr_id=_TRUTH_TR_ID,
                observed_at=observed_at,
                http_status=200,
                envelope=response_envelope,
            )
            truth_created_at = self._now()
            truth_body = {
                "schemaVersion": "kis-domestic-functional-mutation-truth/v1",
                "claimId": claim_id,
                "requestArchiveId": request_archive_id,
                "requestArchiveHash": request_hash,
                "responseArchiveId": response_archive_id,
                "responseArchiveHash": response_hash,
                "complete": True,
                "pageCount": request_envelope["pageCount"],
                "officialState": target_state,
                "officialOrderKey": resolved_key,
                "observedAt": observed_at,
                "requestEnvelopeHash": _hash(request_envelope),
                "responseEnvelopeHash": _hash(response_envelope),
                "trustedGetAuthorityKeyIdHash": self._truth_key_id_hash,
            }
            truth_text = _canonical(truth_body)
            truth_hash = hashlib.sha256(truth_text.encode("utf-8")).hexdigest()
            truth_signature = hmac.new(self._key, truth_text.encode("utf-8"), hashlib.sha256).hexdigest()
            conn.execute(
                """INSERT INTO kis_mutation_official_truth
                   (truth_id,claim_id,request_archive_id,response_archive_id,
                    complete,page_count,official_state,order_date,
                    organization_no,order_no,record_json,record_hash,signature,
                    signer_key_id_hash,created_at)
                   VALUES (?,?,?,?,1,?,?,?,?,?,?,?,?,?,?)""",
                (
                    truth_id,
                    claim_id,
                    request_archive_id,
                    response_archive_id,
                    request_envelope["pageCount"],
                    target_state,
                    resolved_key["orderDate"],
                    resolved_key["organizationNo"],
                    resolved_key["orderNo"],
                    truth_text,
                    truth_hash,
                    truth_signature,
                    self._key_id_hash,
                    truth_created_at,
                ),
            )
            revision = expected_revision + 1
            head = self._transition(
                conn,
                claim_id=claim_id,
                revision=revision,
                state=target_state,
                previous_hash=str(row["transition_head_hash"]),
                occurred_at=truth_created_at,
            )
            persisted_ack = (
                {"orderDate": "", "organizationNo": "", "orderNo": ""}
                if operation == "CLEANUP_CANCEL"
                else resolved_key
            )
            try:
                changed = conn.execute(
                    """UPDATE kis_mutation_request SET state=?,ack_order_date=?,
                       ack_organization_no=?,ack_order_no=?,transition_head_hash=?,
                       revision=?,updated_at=? WHERE claim_id=? AND revision=?""",
                    (
                        target_state,
                        persisted_ack["orderDate"],
                        persisted_ack["organizationNo"],
                        persisted_ack["orderNo"],
                        head,
                        revision,
                        truth_created_at,
                        claim_id,
                        expected_revision,
                    ),
                ).rowcount
            except sqlite3.IntegrityError as exc:
                raise KisDomesticFunctionalMutationBlocked("official truth ACK tuple is not unique") from exc
            if changed != 1:
                raise KisDomesticFunctionalMutationBlocked("official truth CAS failed")
        return {
            "truthId": truth_id,
            "claimId": claim_id,
            "state": target_state,
            "revision": revision,
            "retryAllowed": False,
            "requestArchiveHash": request_hash,
            "responseArchiveHash": response_hash,
            "truthHash": truth_hash,
        }

    def verify_integrity(self, claim_id: str) -> dict[str, Any]:
        claim_id = _identity(claim_id, "claim id")
        with self.ledger.connection() as conn:
            request = conn.execute("SELECT * FROM kis_mutation_request WHERE claim_id=?", (claim_id,)).fetchone()
            if request is None:
                raise KisDomesticFunctionalMutationBlocked("mutation request is missing")
            self._verify_request_locked(conn, request)
            transitions = conn.execute(
                "SELECT * FROM kis_mutation_transition WHERE claim_id=? ORDER BY revision", (claim_id,)
            ).fetchall()

            archives = conn.execute("SELECT * FROM kis_mutation_raw_archive WHERE claim_id=?", (claim_id,)).fetchall()
            archive_by_id: dict[str, sqlite3.Row] = {}
            for archive in archives:
                envelope = json.loads(str(archive["envelope_json"]))
                text = _canonical(envelope)
                digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
                signature = hmac.new(self._key, text.encode("utf-8"), hashlib.sha256).hexdigest()
                if (
                    not hmac.compare_digest(digest, str(archive["envelope_hash"]))
                    or not hmac.compare_digest(signature, str(archive["signature"]))
                    or not hmac.compare_digest(self._key_id_hash, str(archive["signer_key_id_hash"]))
                ):
                    raise KisDomesticFunctionalMutationBlocked("raw response archive failed verification")
                if (
                    str(archive["archive_id"]) in archive_by_id
                    or str(archive["endpoint"]) != envelope.get("endpoint")
                    or str(archive["tr_id"]) != envelope.get("trId")
                    or str(archive["observed_at"]) != envelope.get("observedAt")
                    or int(archive["http_status"])
                    != (200 if archive["archive_kind"] == "OFFICIAL_REQUEST" else envelope.get("httpStatus"))
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "raw archive row/envelope projection mismatch"
                    )
                archive_by_id[str(archive["archive_id"])] = archive

            truths = conn.execute(
                "SELECT * FROM kis_mutation_official_truth WHERE claim_id=? ORDER BY rowid",
                (claim_id,),
            ).fetchall()
            official_transitions = [
                item for item in transitions if str(item["state"]) in _OFFICIAL_STATES
            ]
            referenced_archives: set[str] = set()
            for truth_index, truth in enumerate(truths):
                body = json.loads(str(truth["record_json"]))
                text = _canonical(body)
                if (
                    not hmac.compare_digest(hashlib.sha256(text.encode("utf-8")).hexdigest(), str(truth["record_hash"]))
                    or not hmac.compare_digest(hmac.new(self._key, text.encode("utf-8"), hashlib.sha256).hexdigest(), str(truth["signature"]))
                    or not hmac.compare_digest(self._key_id_hash, str(truth["signer_key_id_hash"]))
                ):
                    raise KisDomesticFunctionalMutationBlocked("official truth record failed verification")
                request_id = str(truth["request_archive_id"])
                response_id = str(truth["response_archive_id"])
                if (
                    request_id in referenced_archives
                    or response_id in referenced_archives
                    or request_id not in archive_by_id
                    or response_id not in archive_by_id
                    or str(archive_by_id[request_id]["archive_kind"]) != "OFFICIAL_REQUEST"
                    or str(archive_by_id[response_id]["archive_kind"]) != "OFFICIAL_RESPONSE"
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "official truth archive join is incomplete or orphaned"
                    )
                referenced_archives.update({request_id, response_id})
                request_envelope = json.loads(str(archive_by_id[request_id]["envelope_json"]))
                response_envelope = json.loads(str(archive_by_id[response_id]["envelope_json"]))
                request_value, response_value, raw_rows, _ = self._verify_truth_capture_pair(
                    request_envelope, response_envelope
                )
                exact_body = {
                    "schemaVersion": "kis-domestic-functional-mutation-truth/v1",
                    "claimId": claim_id,
                    "requestArchiveId": request_id,
                    "requestArchiveHash": str(archive_by_id[request_id]["envelope_hash"]),
                    "responseArchiveId": response_id,
                    "responseArchiveHash": str(archive_by_id[response_id]["envelope_hash"]),
                    "complete": True,
                    "pageCount": request_value["pageCount"],
                    "officialState": str(truth["official_state"]),
                    "officialOrderKey": {
                        "orderDate": str(truth["order_date"]),
                        "organizationNo": str(truth["organization_no"]),
                        "orderNo": str(truth["order_no"]),
                    },
                    "observedAt": request_value["observedAt"],
                    "requestEnvelopeHash": _hash(request_value),
                    "responseEnvelopeHash": _hash(response_value),
                    "trustedGetAuthorityKeyIdHash": self._truth_key_id_hash,
                }
                if body != exact_body:
                    raise KisDomesticFunctionalMutationBlocked(
                        "official truth record/archive projection mismatch"
                    )
                key = exact_body["officialOrderKey"]
                state = exact_body["officialState"]
                matching_rows = [
                    raw for raw in raw_rows
                    if all(raw[name] == key[name] for name in key)
                ] if any(key.values()) else []
                operation = str(request["operation"])
                expected_side = (
                    str(request["target_order_side"])
                    if operation == "CLEANUP_CANCEL"
                    else str(request["side"])
                )
                baseline_identities = {
                    (item["orderDate"], item["organizationNo"], item["orderNo"])
                    for item in request_value["baselineOrderKeys"]
                }
                post_marker = _parse_utc(
                    request_value["postMarkerAt"], "verified post marker"
                )
                causal_rows = [
                    raw for raw in raw_rows
                    if raw["pdno"] == PDNO
                    and raw["side"] == expected_side
                    and (
                        raw["orderDate"], raw["organizationNo"], raw["orderNo"]
                    ) not in baseline_identities
                    and _parse_utc(raw["orderObservedAt"], "verified row observedAt")
                    >= post_marker
                    and raw["limitPriceKrw"] == request_value["limitPriceKrw"]
                ]
                if state == "UNKNOWN":
                    # UNKNOWN may retain a pre-known key, but that exact key must
                    # be absent from the complete official capture.
                    if matching_rows:
                        raise KisDomesticFunctionalMutationBlocked(
                            "official UNKNOWN contradicts raw capture"
                        )
                    if not any(key.values()) and causal_rows:
                        raise KisDomesticFunctionalMutationBlocked(
                            "official UNKNOWN hides a causal post-marker row"
                        )
                elif (
                    len(matching_rows) != 1
                    or matching_rows[0]["state"] != state
                    or matching_rows[0]["side"] != expected_side
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "official truth state/key does not reduce from raw capture"
                    )
                if (
                    int(truth["complete"]) != 1
                    or int(truth["page_count"]) != request_value["pageCount"]
                    or truth_index >= len(official_transitions)
                    or str(truth["created_at"])
                    != str(official_transitions[truth_index]["occurred_at"])
                    or state != str(official_transitions[truth_index]["state"])
                    or _parse_utc(str(truth["created_at"]), "truth createdAt")
                    < _parse_utc(request_value["observedAt"], "truth observedAt")
                    or _parse_utc(str(truth["created_at"]), "truth createdAt")
                    > _parse_utc(request_value["observedAt"], "truth observedAt")
                    + timedelta(seconds=5)
                ):
                    raise KisDomesticFunctionalMutationBlocked(
                        "official truth row/transition projection mismatch"
                    )
            if referenced_archives != set(archive_by_id):
                raise KisDomesticFunctionalMutationBlocked(
                    "official truth has orphan raw archives"
                )
            if len(official_transitions) != len(truths):
                raise KisDomesticFunctionalMutationBlocked(
                    "terminal transition lacks official truth capture"
                )
        return {"claimId": claim_id, "verified": True, "transitionCount": len(transitions), "archiveCount": len(archives), "truthCount": len(truths)}

    def status(self) -> dict[str, Any]:
        return production_entrypoint_status()


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False,
        "networkAvailable": False,
        "senderAvailable": False,
        "officialTruthReconcileAvailable": False,
        "rawResponseArchiveAvailable": False,
        "trustedGetProductionAvailable": False,
        "terminalTransitionProductionAvailable": False,
        "promotionAvailable": False,
        "releaseEvidenceAvailable": False,
        "route": ROUTE,
        "pdno": PDNO,
        "reason": "OFFLINE_MUTATION_JOURNAL_ONLY_NO_SENDER_OR_OFFICIAL_TRUTH",
    }


__all__ = [
    "DurableKisDomesticFunctionalMutationJournal",
    "KIS_DOMESTIC_FUNCTIONAL_MUTATION_AVAILABLE",
    "KisDomesticFunctionalMutationBlocked",
    "production_entrypoint_status",
    "sign_kis_domestic_mutation_truth_capture",
]
