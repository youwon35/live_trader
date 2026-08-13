from __future__ import annotations

"""Broker-neutral final authority boundary for every KIS order mutation.

The module intentionally owns no broker transport and has no default authority
reader.  A production state graph must register its one durable reader before
any ordinary or functional mutation can cross this boundary.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping

from .emergency_stop import emergency_stop_dispatch_boundary


class KisOrderAuthorityError(RuntimeError):
    pass


_ROUTE_LOCK = threading.RLock()
_PROVIDER_LOCK = threading.RLock()
_TRANSPORT_CONSUMPTION_LOCK = threading.Lock()
_THREAD_BOUNDARY = threading.local()
_AUTHORITY_READER: Callable[[], Mapping[str, Any]] | None = None
_KILL_CANCEL_JOURNAL_PATH: Path | None = None
_CONSUMED_KILL_CANCEL_LEASES: set[tuple[str, int, str]] = set()
_CONSUMED_TRANSPORT_LEASES: set[str] = set()
_CONSUMED_TRANSPORT_INTENTS: set[tuple[str, int, str]] = set()
_CONSUMED_READ_TRANSPORTS: set[tuple[str, str]] = set()

_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_OFFICIAL_ID = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_OPERATION = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$", flags=re.ASCII)
_ENDPOINT = re.compile(r"^/uapi/[A-Za-z0-9._~!$&'()*+,;=:@%/-]{1,255}$", flags=re.ASCII)

_OPEN_PHASES = frozenset(
    {
        "ARMED_WAIT_PUBLIC",
        "BOOTSTRAP_ISSUED",
        "APPROVED",
        "ACTIVE",
        "CLEANUP",
        "FINAL_RESET",
        "RECONCILIATION_REQUIRED",
    }
)
_SESSION_PHASES = frozenset(
    {"ACTIVE", "CLEANUP", "FINAL_RESET", "RECONCILIATION_REQUIRED"}
)
_ALL_PHASES = _OPEN_PHASES | {"IDLE", "FINALIZED"}
_ENTRY_OPERATIONS = frozenset({"NATURAL_BUY"})
_CLEANUP_OPERATIONS = frozenset({"CLEANUP_CANCEL", "CLEANUP_SELL"})
_OPERATION_ENDPOINTS = {
    "PLACE_ORDER": frozenset(
        {
            "/uapi/domestic-stock/v1/trading/order-cash",
            "/uapi/overseas-stock/v1/trading/order",
        }
    ),
    "NATURAL_BUY": frozenset(
        {"/uapi/domestic-stock/v1/trading/order-cash"}
    ),
    "CLEANUP_SELL": frozenset(
        {"/uapi/domestic-stock/v1/trading/order-cash"}
    ),
    "CANCEL_ORDER": frozenset(
        {"/uapi/domestic-stock/v1/trading/order-rvsecncl"}
    ),
    "CLEANUP_CANCEL": frozenset(
        {"/uapi/domestic-stock/v1/trading/order-rvsecncl"}
    ),
    "KILL_ORDINARY_CANCEL": frozenset(
        {"/uapi/domestic-stock/v1/trading/order-rvsecncl"}
    ),
    "OVERSEAS_CANCEL_ORDER": frozenset(
        {"/uapi/overseas-stock/v1/trading/order-rvsecncl"}
    ),
}


@dataclass(frozen=True)
class _InheritedLease:
    mode: str
    owner_thread_id: int
    nonce: str
    operation: str
    intent_hash: str
    owned_order_key: Mapping[str, str]
    session_id: str
    revision: int
    state_revision: int
    owner_epoch_id: str
    owner_epoch_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    control_reservation_hash: str
    cleanup_only: bool
    public_snapshot: Mapping[str, Any]
    read: Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class _AuthenticatedScope:
    mode: str
    owner_thread_id: int
    nonce: str
    state_revision: int
    owner_epoch_id: str
    owner_epoch_hash: str
    account_fingerprint: str
    credential_configuration_hash: str
    control_reservation_hash: str


def _validate_operation(operation: str) -> str:
    if type(operation) is not str or not _OPERATION.fullmatch(operation):
        raise KisOrderAuthorityError("KIS mutation operation is invalid")
    return operation


def _normalize_intent(
    value: Mapping[str, Any], *, expected_operation: str
) -> dict[str, Any]:
    required = {
        "operation",
        "claimId",
        "ownedOrderKey",
        "accountFingerprint",
        "credentialConfigurationHash",
        "endpoint",
        "payloadHash",
    }
    if not isinstance(value, Mapping) or set(value) != required:
        raise KisOrderAuthorityError("KIS mutation intent shape is invalid")
    operation = _validate_operation(value.get("operation"))
    if not secrets.compare_digest(operation, expected_operation):
        raise KisOrderAuthorityError("KIS mutation intent operation changed")
    claim_id = value.get("claimId")
    if type(claim_id) is not str or not _IDENTITY.fullmatch(claim_id):
        raise KisOrderAuthorityError("KIS mutation claim id is invalid")
    account_fingerprint = value.get("accountFingerprint")
    credential_hash = value.get("credentialConfigurationHash")
    payload_hash = value.get("payloadHash")
    if type(account_fingerprint) is not str or not _SHA256.fullmatch(
        account_fingerprint
    ):
        raise KisOrderAuthorityError("KIS mutation account fingerprint is invalid")
    if type(credential_hash) is not str or not _SHA256.fullmatch(credential_hash):
        raise KisOrderAuthorityError(
            "KIS mutation credential configuration hash is invalid"
        )
    if type(payload_hash) is not str or not _SHA256.fullmatch(payload_hash):
        raise KisOrderAuthorityError("KIS mutation payload hash is invalid")
    endpoint = value.get("endpoint")
    if type(endpoint) is not str or not _ENDPOINT.fullmatch(endpoint):
        raise KisOrderAuthorityError("KIS mutation endpoint is invalid")
    if endpoint not in _OPERATION_ENDPOINTS.get(operation, frozenset()):
        raise KisOrderAuthorityError(
            "KIS mutation operation/endpoint binding is invalid"
        )

    owned = value.get("ownedOrderKey")
    owned_keys = {"orderDate", "organizationNo", "orderNo"}
    if not isinstance(owned, Mapping) or set(owned) != owned_keys:
        raise KisOrderAuthorityError("KIS owned order key shape is invalid")
    normalized_owned = {key: owned.get(key) for key in sorted(owned_keys)}
    values = tuple(normalized_owned.values())
    if any(type(item) is not str for item in values):
        raise KisOrderAuthorityError("KIS owned order key values are invalid")
    cancel_operation = operation in {
        "CANCEL_ORDER",
        "CLEANUP_CANCEL",
        "KILL_ORDINARY_CANCEL",
    }
    if cancel_operation:
        if any(not _OFFICIAL_ID.fullmatch(item) for item in values):
            raise KisOrderAuthorityError(
                "KIS cancel requires an exact owned official order key"
            )
    elif any(values):
        raise KisOrderAuthorityError(
            "new KIS order intent cannot claim a broker order key before ACK"
        )

    return {
        "operation": operation,
        "claimId": claim_id,
        "ownedOrderKey": normalized_owned,
        "accountFingerprint": account_fingerprint,
        "credentialConfigurationHash": credential_hash,
        "endpoint": endpoint,
        "payloadHash": payload_hash,
    }


def _intent_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _normalize_control_reservation(
    value: Any, *, state_revision: int, functional_phase: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise KisOrderAuthorityError(
            "durable KIS control reservation is invalid"
        )
    if not value:
        return {}
    required = {
        "reservationId",
        "reservationKind",
        "reservationRevision",
        "stateRevision",
        "phase",
        "reservationBindingHash",
    }
    if set(value) != required:
        raise KisOrderAuthorityError(
            "durable KIS control reservation shape is invalid"
        )
    reservation_id = value.get("reservationId")
    kind = value.get("reservationKind")
    revision = value.get("reservationRevision")
    reserved_state_revision = value.get("stateRevision")
    phase = value.get("phase")
    binding_hash = value.get("reservationBindingHash")
    if type(reservation_id) is not str or not _IDENTITY.fullmatch(
        reservation_id
    ):
        raise KisOrderAuthorityError(
            "durable KIS control reservation id is invalid"
        )
    if kind not in {"START", "STOP", "KILL", "SETTINGS"}:
        raise KisOrderAuthorityError(
            "durable KIS control reservation kind is invalid"
        )
    if type(revision) is not int or revision < 2:
        raise KisOrderAuthorityError(
            "durable KIS control reservation revision is invalid"
        )
    if (
        type(reserved_state_revision) is not int
        or reserved_state_revision != state_revision
        or reserved_state_revision < revision
    ):
        raise KisOrderAuthorityError(
            "durable KIS control reservation state revision changed"
        )
    if type(phase) is not str or not secrets.compare_digest(
        phase, functional_phase
    ):
        raise KisOrderAuthorityError(
            "durable KIS control reservation phase changed"
        )
    if type(binding_hash) is not str or not _SHA256.fullmatch(binding_hash):
        raise KisOrderAuthorityError(
            "durable KIS control reservation binding is invalid"
        )
    return {key: value[key] for key in sorted(required)}


def _validate_transport(
    intent: Mapping[str, Any], *, endpoint: str, payload_hash: str
) -> None:
    if (
        type(endpoint) is not str
        or type(payload_hash) is not str
        or not secrets.compare_digest(endpoint, str(intent["endpoint"]))
        or not secrets.compare_digest(payload_hash, str(intent["payloadHash"]))
    ):
        raise KisOrderAuthorityError(
            "KIS transport endpoint/payload differs from the sealed intent"
        )


def register_kis_order_authority_reader(
    reader: Callable[[], Mapping[str, Any]],
    *,
    kill_cancel_journal_path: str | Path | None = None,
) -> None:
    """Register the single state-owned durable KIS authority reader."""

    if not callable(reader):
        raise KisOrderAuthorityError("KIS authority reader is invalid")
    journal_path: Path | None = None
    if kill_cancel_journal_path is not None:
        journal_path = Path(kill_cancel_journal_path)
        if not journal_path.is_absolute() or not journal_path.name:
            raise KisOrderAuthorityError(
                "KIS Kill-cancel journal path must be absolute"
            )
    global _AUTHORITY_READER
    global _KILL_CANCEL_JOURNAL_PATH
    with _PROVIDER_LOCK:
        if _AUTHORITY_READER is not None and _AUTHORITY_READER is not reader:
            raise KisOrderAuthorityError(
                "KIS authority reader is already owned by another state graph"
            )
        if (
            journal_path is not None
            and _KILL_CANCEL_JOURNAL_PATH is not None
            and journal_path != _KILL_CANCEL_JOURNAL_PATH
        ):
            raise KisOrderAuthorityError(
                "KIS Kill-cancel journal is already owned by another state graph"
            )
        _AUTHORITY_READER = reader
        if journal_path is not None:
            _KILL_CANCEL_JOURNAL_PATH = journal_path


_KILL_CANCEL_JOURNAL_COLUMNS = (
    "sequence_no",
    "grant_burn_key",
    "owned_order_burn_key",
    "owner_epoch_id",
    "owner_epoch_hash",
    "state_revision",
    "kill_revision",
    "intent_hash",
    "intent_json",
    "previous_entry_hash",
    "entry_hash",
    "burned_at",
)
_KILL_CANCEL_COLUMN_CONTRACT = (
    ("sequence_no", "INTEGER", 0, None, 1),
    ("grant_burn_key", "TEXT", 1, None, 0),
    ("owned_order_burn_key", "TEXT", 1, None, 0),
    ("owner_epoch_id", "TEXT", 1, None, 0),
    ("owner_epoch_hash", "TEXT", 1, None, 0),
    ("state_revision", "INTEGER", 1, None, 0),
    ("kill_revision", "INTEGER", 1, None, 0),
    ("intent_hash", "TEXT", 1, None, 0),
    ("intent_json", "TEXT", 1, None, 0),
    ("previous_entry_hash", "TEXT", 1, None, 0),
    ("entry_hash", "TEXT", 1, None, 0),
    ("burned_at", "TEXT", 1, None, 0),
)
_KILL_CANCEL_UNIQUE_COLUMNS = frozenset(
    {"grant_burn_key", "owned_order_burn_key", "entry_hash"}
)
_KILL_CANCEL_CREATE_SQL = """
        CREATE TABLE kis_kill_cancel_burns (
            sequence_no INTEGER PRIMARY KEY,
            grant_burn_key TEXT NOT NULL UNIQUE,
            owned_order_burn_key TEXT NOT NULL UNIQUE,
            owner_epoch_id TEXT NOT NULL,
            owner_epoch_hash TEXT NOT NULL,
            state_revision INTEGER NOT NULL,
            kill_revision INTEGER NOT NULL,
            intent_hash TEXT NOT NULL,
            intent_json TEXT NOT NULL,
            previous_entry_hash TEXT NOT NULL,
            entry_hash TEXT NOT NULL UNIQUE,
            burned_at TEXT NOT NULL
        )
"""


def _normalized_sql(value: object) -> str:
    return " ".join(str(value or "").replace("\n", " ").split()).lower()


def _journal_entry_hash(entry: Mapping[str, Any]) -> str:
    body = {key: entry[key] for key in _KILL_CANCEL_JOURNAL_COLUMNS[:-2]}
    body["previous_entry_hash"] = entry["previous_entry_hash"]
    body["burned_at"] = entry["burned_at"]
    return hashlib.sha256(
        json.dumps(
            body,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _verify_journal_entry(row: Mapping[str, Any]) -> None:
    entry = {key: row[key] for key in _KILL_CANCEL_JOURNAL_COLUMNS}
    if not secrets.compare_digest(
        str(entry["entry_hash"]), _journal_entry_hash(entry)
    ):
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal hash chain is invalid"
        )


def _verify_kill_cancel_journal_chain(
    conn: sqlite3.Connection,
) -> sqlite3.Row | None:
    previous_hash = ""
    previous_row: sqlite3.Row | None = None
    expected_sequence = 1
    for row in conn.execute(
        "SELECT * FROM kis_kill_cancel_burns ORDER BY sequence_no"
    ).fetchall():
        if (
            int(row["sequence_no"]) != expected_sequence
            or not secrets.compare_digest(
                str(row["previous_entry_hash"]), previous_hash
            )
        ):
            raise KisOrderAuthorityError(
                "durable KIS Kill-cancel journal chain is discontinuous"
            )
        _verify_journal_entry(row)
        previous_hash = str(row["entry_hash"])
        previous_row = row
        expected_sequence += 1
    return previous_row


def _ensure_kill_cancel_journal_schema(conn: sqlite3.Connection) -> None:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version not in {0, 1}:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal schema is unsupported"
        )
    conn.execute(
        _KILL_CANCEL_CREATE_SQL.replace(
            "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1
        )
    )
    columns = tuple(
        (
            str(row[1]),
            str(row[2]).upper(),
            int(row[3]),
            row[4],
            int(row[5]),
        )
        for row in conn.execute(
            "PRAGMA table_info(kis_kill_cancel_burns)"
        ).fetchall()
    )
    if columns != _KILL_CANCEL_COLUMN_CONTRACT:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal schema is invalid"
        )
    schema_objects = conn.execute(
        """
        SELECT type, name, tbl_name FROM sqlite_master
        WHERE type IN ('table','index','trigger','view')
        ORDER BY type, name
        """
    ).fetchall()
    expected_objects = {
        ("table", "kis_kill_cancel_burns", "kis_kill_cancel_burns"),
        (
            "index",
            "sqlite_autoindex_kis_kill_cancel_burns_1",
            "kis_kill_cancel_burns",
        ),
        (
            "index",
            "sqlite_autoindex_kis_kill_cancel_burns_2",
            "kis_kill_cancel_burns",
        ),
        (
            "index",
            "sqlite_autoindex_kis_kill_cancel_burns_3",
            "kis_kill_cancel_burns",
        ),
    }
    if {
        (str(row[0]), str(row[1]), str(row[2])) for row in schema_objects
    } != expected_objects:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal object set is invalid"
        )
    table_sql = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type='table' AND name='kis_kill_cancel_burns'
        """
    ).fetchone()
    if (
        table_sql is None
        or _normalized_sql(table_sql[0])
        != _normalized_sql(_KILL_CANCEL_CREATE_SQL)
    ):
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal DDL is invalid"
        )
    unique_columns: set[str] = set()
    index_rows = conn.execute(
        "PRAGMA index_list(kis_kill_cancel_burns)"
    ).fetchall()
    if len(index_rows) != 3:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal index count is invalid"
        )
    for index in index_rows:
        index_name = str(index[1])
        unique = int(index[2]) == 1
        origin = str(index[3]) if len(index) > 3 else ""
        partial = int(index[4]) == 1 if len(index) > 4 else False
        indexed = tuple(
            str(row[2])
            for row in conn.execute(
                f'PRAGMA index_xinfo("{index_name.replace(chr(34), chr(34) * 2)}")'
            ).fetchall()
            if int(row[5]) == 1
        )
        if partial or not unique or origin != "u" or len(indexed) != 1:
            raise KisOrderAuthorityError(
                "durable KIS Kill-cancel journal index is invalid"
            )
        unique_columns.add(indexed[0])
    if unique_columns != _KILL_CANCEL_UNIQUE_COLUMNS:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal uniqueness is invalid"
        )
    if version == 0:
        conn.execute("PRAGMA user_version = 1")
    conn.commit()


def _durably_burn_kill_cancel(
    *,
    snapshot: Mapping[str, Any],
    expected_revision: int,
    normalized_intent: Mapping[str, Any],
    normalized_intent_hash: str,
) -> None:
    """Commit a one-use exact cancel burn before any socket is reachable."""

    with _PROVIDER_LOCK:
        journal_path = _KILL_CANCEL_JOURNAL_PATH
    if journal_path is None:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel journal is not configured"
        )
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    grant_body = {
        "schemaVersion": "kis-kill-cancel-grant-burn/v1",
        "ownerEpochId": str(snapshot["ownerEpochId"]),
        "ownerEpochHash": str(snapshot["ownerEpochHash"]),
        "stateRevision": int(snapshot["stateRevision"]),
        "killRevision": expected_revision,
        "intentHash": normalized_intent_hash,
    }
    owned_body = {
        "schemaVersion": "kis-owned-order-cancel-burn/v1",
        "accountFingerprint": str(normalized_intent["accountFingerprint"]),
        "endpoint": str(normalized_intent["endpoint"]),
        "ownedOrderKey": dict(normalized_intent["ownedOrderKey"]),
    }
    grant_burn_key = _intent_hash(grant_body)
    owned_order_burn_key = _intent_hash(owned_body)
    intent_json = json.dumps(
        dict(normalized_intent),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    try:
        conn = sqlite3.connect(journal_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA synchronous = FULL")
            _ensure_kill_cancel_journal_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            previous = _verify_kill_cancel_journal_chain(conn)
            sequence_no = int(previous["sequence_no"] if previous else 0) + 1
            previous_hash = str(previous["entry_hash"] if previous else "")
            entry = {
                "sequence_no": sequence_no,
                "grant_burn_key": grant_burn_key,
                "owned_order_burn_key": owned_order_burn_key,
                "owner_epoch_id": grant_body["ownerEpochId"],
                "owner_epoch_hash": grant_body["ownerEpochHash"],
                "state_revision": grant_body["stateRevision"],
                "kill_revision": expected_revision,
                "intent_hash": normalized_intent_hash,
                "intent_json": intent_json,
                "previous_entry_hash": previous_hash,
                "burned_at": datetime.now(timezone.utc).isoformat(),
            }
            entry["entry_hash"] = _journal_entry_hash(entry)
            conn.execute(
                """
                INSERT OR ABORT INTO kis_kill_cancel_burns (
                    sequence_no, grant_burn_key, owned_order_burn_key,
                    owner_epoch_id, owner_epoch_hash, state_revision,
                    kill_revision, intent_hash, intent_json,
                    previous_entry_hash, entry_hash, burned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(entry[key] for key in _KILL_CANCEL_JOURNAL_COLUMNS),
            )
            # This FULL-synchronous COMMIT is the irreversible one-use burn.
            # The caller cannot install its inherited transport lease, and
            # therefore cannot reach the socket, until commit returns.
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        verify = sqlite3.connect(journal_path, timeout=5.0)
        verify.row_factory = sqlite3.Row
        try:
            _ensure_kill_cancel_journal_schema(verify)
            head = _verify_kill_cancel_journal_chain(verify)
            row = verify.execute(
                "SELECT * FROM kis_kill_cancel_burns WHERE sequence_no=?",
                (sequence_no,),
            ).fetchone()
            if (
                row is None
                or head is None
                or int(head["sequence_no"]) != sequence_no
                or not secrets.compare_digest(
                    str(head["entry_hash"]), str(entry["entry_hash"])
                )
                or not secrets.compare_digest(
                    str(row["grant_burn_key"]), grant_burn_key
                )
                or not secrets.compare_digest(
                    str(row["owned_order_burn_key"]), owned_order_burn_key
                )
            ):
                raise KisOrderAuthorityError(
                    "durable KIS Kill-cancel burn was not committed"
                )
            _verify_journal_entry(row)
        finally:
            verify.close()
    except sqlite3.IntegrityError as exc:
        raise KisOrderAuthorityError(
            "KIS Kill-cancel grant or exact owned order was already consumed"
        ) from exc
    except KisOrderAuthorityError:
        raise
    except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel burn failed closed"
        ) from exc


def _snapshot() -> dict[str, Any]:
    with _PROVIDER_LOCK:
        reader = _AUTHORITY_READER
    if reader is None:
        raise KisOrderAuthorityError(
            "state-owned KIS authority reader is not registered"
        )
    try:
        raw = reader()
        if not isinstance(raw, Mapping):
            raise TypeError("authority reader returned a non-mapping")
        value = dict(raw)
    except Exception as exc:
        raise KisOrderAuthorityError("durable KIS authority is unreadable") from exc

    required = {
        "durableAuthorityReadable",
        "functionalAuthorityOpen",
        "functionalPhase",
        "functionalRevision",
        "functionalSessionId",
        "functionalAccountFingerprint",
        "credentialConfigurationHash",
        "functionalMutationIntent",
        "killOrdinaryCancelAllowed",
        "killOrdinaryCancelRevision",
        "killOrdinaryCancelIntent",
        "applicationInstanceLeaseHeld",
        "ordinaryRoutesClosed",
        "ownerEpochId",
        "ownerEpochHash",
        "stateRevision",
        "controlReservation",
    }
    if not required.issubset(value):
        raise KisOrderAuthorityError("durable KIS authority snapshot is incomplete")
    if any(
        type(value[key]) is not bool
        for key in (
            "durableAuthorityReadable",
            "functionalAuthorityOpen",
            "killOrdinaryCancelAllowed",
            "applicationInstanceLeaseHeld",
            "ordinaryRoutesClosed",
        )
    ):
        raise KisOrderAuthorityError("durable KIS authority booleans are invalid")
    if value["durableAuthorityReadable"] is not True:
        raise KisOrderAuthorityError("durable KIS authority is not readable")

    phase = value["functionalPhase"]
    revision = value["functionalRevision"]
    session_id = value["functionalSessionId"]
    account_fingerprint = value["functionalAccountFingerprint"]
    credential_hash = value["credentialConfigurationHash"]
    if type(phase) is not str or phase not in _ALL_PHASES:
        raise KisOrderAuthorityError("durable KIS functional phase is invalid")
    if type(revision) is not int or revision < 0:
        raise KisOrderAuthorityError("durable KIS functional revision is invalid")
    if type(session_id) is not str or (
        session_id and not _IDENTITY.fullmatch(session_id)
    ):
        raise KisOrderAuthorityError("durable KIS functional session is invalid")
    if type(account_fingerprint) is not str or (
        account_fingerprint and not _SHA256.fullmatch(account_fingerprint)
    ):
        raise KisOrderAuthorityError(
            "durable KIS account fingerprint is invalid"
        )
    if type(credential_hash) is not str or (
        credential_hash and not _SHA256.fullmatch(credential_hash)
    ):
        raise KisOrderAuthorityError(
            "durable KIS credential configuration hash is invalid"
        )
    if not isinstance(value["functionalMutationIntent"], Mapping) or not isinstance(
        value["killOrdinaryCancelIntent"], Mapping
    ):
        raise KisOrderAuthorityError("durable KIS mutation intent is invalid")
    if (
        type(value["killOrdinaryCancelRevision"]) is not int
        or value["killOrdinaryCancelRevision"] < 0
    ):
        raise KisOrderAuthorityError(
            "durable KIS Kill-cancel revision is invalid"
        )

    owner_epoch_id = value["ownerEpochId"]
    owner_epoch_hash = value["ownerEpochHash"]
    state_revision = value["stateRevision"]
    if type(owner_epoch_id) is not str or not _IDENTITY.fullmatch(owner_epoch_id):
        raise KisOrderAuthorityError("durable KIS owner epoch id is invalid")
    if type(owner_epoch_hash) is not str or not _SHA256.fullmatch(
        owner_epoch_hash
    ):
        raise KisOrderAuthorityError("durable KIS owner epoch hash is invalid")
    if type(state_revision) is not int or state_revision < 1:
        raise KisOrderAuthorityError("durable KIS state revision is invalid")
    value["controlReservation"] = _normalize_control_reservation(
        value["controlReservation"],
        state_revision=state_revision,
        functional_phase=phase,
    )

    expected_open = phase in _OPEN_PHASES
    if value["functionalAuthorityOpen"] is not expected_open:
        raise KisOrderAuthorityError(
            "durable KIS authority-open/phase invariant failed"
        )
    if expected_open and (
        revision < 1
        or not account_fingerprint
        or not credential_hash
        or value["ordinaryRoutesClosed"] is not True
    ):
        raise KisOrderAuthorityError(
            "durable KIS open-authority isolation invariant failed"
        )
    if phase in _SESSION_PHASES and not session_id:
        raise KisOrderAuthorityError(
            "durable KIS session authority identity is missing"
        )
    if phase in {"ARMED_WAIT_PUBLIC", "BOOTSTRAP_ISSUED", "APPROVED"} and session_id:
        raise KisOrderAuthorityError(
            "preactivation KIS authority cannot carry a functional session"
        )
    if not expected_open and value["ordinaryRoutesClosed"] is True:
        raise KisOrderAuthorityError(
            "durable KIS terminal authority left ordinary routes closed"
        )
    return value


def kis_order_authority_binding() -> Mapping[str, Any]:
    """Return the exact state-owned identity used to seal one KIS mutation.

    This is a read, never a grant.  The returned values are re-read and
    compared while the final route lock is held, so a caller cannot reuse a
    binding after settings, owner epoch, or durable state changes.
    """

    snapshot = _snapshot()
    return MappingProxyType(
        {
            "ownerEpochId": snapshot["ownerEpochId"],
            "ownerEpochHash": snapshot["ownerEpochHash"],
            "stateRevision": snapshot["stateRevision"],
            "accountFingerprint": snapshot["functionalAccountFingerprint"],
            "credentialConfigurationHash": snapshot[
                "credentialConfigurationHash"
            ],
        }
    )


def kis_functional_authority_open_fail_closed() -> bool:
    try:
        snapshot = _snapshot()
    except Exception:
        return True
    return bool(
        snapshot["functionalAuthorityOpen"] is True
        or snapshot["functionalPhase"] in _OPEN_PHASES
        or snapshot["ordinaryRoutesClosed"] is True
    )


def _inherited() -> _InheritedLease | None:
    lease = getattr(_THREAD_BOUNDARY, "lease", None)
    if lease is None:
        return None
    if type(lease) is not _InheritedLease or lease.owner_thread_id != threading.get_ident():
        raise KisOrderAuthorityError("KIS inherited mutation lease is invalid")
    return lease


def _auth_scope() -> _AuthenticatedScope | None:
    stack = getattr(_THREAD_BOUNDARY, "auth_scopes", None)
    if stack is None:
        return None
    if type(stack) is not list or not stack:
        raise KisOrderAuthorityError("KIS authenticated scope is invalid")
    scope = stack[-1]
    if (
        type(scope) is not _AuthenticatedScope
        or scope.owner_thread_id != threading.get_ident()
    ):
        raise KisOrderAuthorityError("KIS authenticated scope is invalid")
    return scope


def _push_auth_scope(scope: _AuthenticatedScope) -> None:
    stack = getattr(_THREAD_BOUNDARY, "auth_scopes", None)
    if stack is None:
        stack = []
        _THREAD_BOUNDARY.auth_scopes = stack
    if type(stack) is not list:
        raise KisOrderAuthorityError("KIS authenticated scope is invalid")
    stack.append(scope)


def _pop_auth_scope(scope: _AuthenticatedScope) -> None:
    stack = getattr(_THREAD_BOUNDARY, "auth_scopes", None)
    if type(stack) is not list or not stack or stack[-1] is not scope:
        raise KisOrderAuthorityError("KIS authenticated scope changed")
    stack.pop()
    if not stack:
        del _THREAD_BOUNDARY.auth_scopes


def _new_auth_scope(mode: str, snapshot: Mapping[str, Any]) -> _AuthenticatedScope:
    return _AuthenticatedScope(
        mode=mode,
        owner_thread_id=threading.get_ident(),
        nonce=secrets.token_hex(32),
        state_revision=int(snapshot["stateRevision"]),
        owner_epoch_id=str(snapshot["ownerEpochId"]),
        owner_epoch_hash=str(snapshot["ownerEpochHash"]),
        account_fingerprint=str(snapshot["functionalAccountFingerprint"]),
        credential_configuration_hash=str(
            snapshot["credentialConfigurationHash"]
        ),
        control_reservation_hash=_intent_hash(
            snapshot["controlReservation"]
        ),
    )


def _validate_auth_scope_snapshot(
    scope: _AuthenticatedScope, snapshot: Mapping[str, Any]
) -> None:
    if (
        snapshot.get("stateRevision") != scope.state_revision
        or not secrets.compare_digest(
            str(snapshot.get("ownerEpochId") or ""), scope.owner_epoch_id
        )
        or not secrets.compare_digest(
            str(snapshot.get("ownerEpochHash") or ""), scope.owner_epoch_hash
        )
        or not secrets.compare_digest(
            str(snapshot.get("functionalAccountFingerprint") or ""),
            scope.account_fingerprint,
        )
        or not secrets.compare_digest(
            str(snapshot.get("credentialConfigurationHash") or ""),
            scope.credential_configuration_hash,
        )
        or snapshot.get("applicationInstanceLeaseHeld") is not True
        or not secrets.compare_digest(
            _intent_hash(snapshot.get("controlReservation") or {}),
            scope.control_reservation_hash,
        )
    ):
        raise KisOrderAuthorityError(
            "KIS authenticated owner/state/account authority changed"
        )


def require_kis_token_authority() -> Mapping[str, Any]:
    """Allow token material only in a route-held mutation/read scope."""

    scope = _auth_scope()
    if scope is None:
        raise KisOrderAuthorityError(
            "KIS token transport requires authenticated route authority"
        )
    snapshot = _snapshot()
    _validate_auth_scope_snapshot(scope, snapshot)
    return snapshot


def _validate_kis_read_transport(
    *,
    endpoint: str,
    request_hash: str,
    account_fingerprint: str,
    credential_configuration_hash: str,
) -> tuple[_AuthenticatedScope, Mapping[str, Any]]:
    scope = _auth_scope()
    if scope is None or scope.mode != "READ_ONLY":
        raise KisOrderAuthorityError(
            "KIS read transport requires inherited READ_ONLY authority"
        )
    if (
        type(endpoint) is not str
        or not _ENDPOINT.fullmatch(endpoint)
        or type(request_hash) is not str
        or not _SHA256.fullmatch(request_hash)
        or type(account_fingerprint) is not str
        or not _SHA256.fullmatch(account_fingerprint)
        or type(credential_configuration_hash) is not str
        or not _SHA256.fullmatch(credential_configuration_hash)
    ):
        raise KisOrderAuthorityError("KIS read transport binding is invalid")
    snapshot = _snapshot()
    _validate_auth_scope_snapshot(scope, snapshot)
    if (
        not secrets.compare_digest(
            account_fingerprint, scope.account_fingerprint
        )
        or not secrets.compare_digest(
            credential_configuration_hash,
            scope.credential_configuration_hash,
        )
    ):
        raise KisOrderAuthorityError(
            "KIS read transport account/credential binding changed"
        )
    return scope, snapshot


def require_kis_read_transport_authority(
    *,
    endpoint: str,
    request_hash: str,
    account_fingerprint: str,
    credential_configuration_hash: str,
) -> Mapping[str, Any]:
    """Validate one exact owned KIS GET without consuming its socket grant."""

    scope, snapshot = _validate_kis_read_transport(
        endpoint=endpoint,
        request_hash=request_hash,
        account_fingerprint=account_fingerprint,
        credential_configuration_hash=credential_configuration_hash,
    )
    with _TRANSPORT_CONSUMPTION_LOCK:
        if (scope.nonce, request_hash) in _CONSUMED_READ_TRANSPORTS:
            raise KisOrderAuthorityError(
                "KIS exact read transport was already consumed"
            )
    return MappingProxyType(
        {
            **snapshot,
            "readScopeNonceHash": hashlib.sha256(
                scope.nonce.encode("utf-8")
            ).hexdigest(),
            "readRequestHash": request_hash,
            "readEndpoint": endpoint,
        }
    )


def consume_kis_read_transport_authority(
    *,
    endpoint: str,
    request_hash: str,
    account_fingerprint: str,
    credential_configuration_hash: str,
) -> Mapping[str, Any]:
    """Atomically burn one exact KIS read immediately before its opener."""

    scope, snapshot = _validate_kis_read_transport(
        endpoint=endpoint,
        request_hash=request_hash,
        account_fingerprint=account_fingerprint,
        credential_configuration_hash=credential_configuration_hash,
    )
    key = (scope.nonce, request_hash)
    with _TRANSPORT_CONSUMPTION_LOCK:
        if key in _CONSUMED_READ_TRANSPORTS:
            raise KisOrderAuthorityError(
                "KIS exact read transport was already consumed"
            )
        _CONSUMED_READ_TRANSPORTS.add(key)
    return snapshot


@contextmanager
def kis_read_diagnostic_boundary() -> Iterator[None]:
    """Authorize token issuance only for account/read-only diagnostics."""

    with _ROUTE_LOCK:
        snapshot = _snapshot()
        if snapshot["applicationInstanceLeaseHeld"] is not True:
            raise KisOrderAuthorityError(
                "KIS read diagnostic requires the official application lease"
            )
        scope = _new_auth_scope("READ_ONLY", snapshot)
        _push_auth_scope(scope)
        try:
            yield
        finally:
            _pop_auth_scope(scope)


@contextmanager
def kis_authenticated_mutation_preflight(
    *,
    mode: str,
    intent: Mapping[str, Any] | None = None,
    expected_revision: int | None = None,
) -> Iterator[None]:
    """Hold route -> emergency before token/build and exact final leasing."""

    normalized_mode = str(mode or "").strip().upper()
    if normalized_mode not in {"ORDINARY", "KILL_PREPARE", "KILL_EXACT"}:
        raise KisOrderAuthorityError("KIS authenticated mutation mode is invalid")
    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            snapshot = _snapshot()
            if snapshot["applicationInstanceLeaseHeld"] is not True:
                raise KisOrderAuthorityError(
                    "KIS authenticated mutation requires the official application lease"
                )
            control = snapshot["controlReservation"]
            if normalized_mode == "ORDINARY":
                if emergency.get("active") is True:
                    raise KisOrderAuthorityError(
                        "ordinary KIS authentication blocked by durable emergency stop"
                    )
                if control:
                    raise KisOrderAuthorityError(
                        "ordinary KIS authentication blocked by control reservation"
                    )
                if (
                    snapshot["functionalAuthorityOpen"] is True
                    or snapshot["functionalPhase"] in _OPEN_PHASES
                    or snapshot["ordinaryRoutesClosed"] is True
                ):
                    raise KisOrderAuthorityError(
                        "ordinary KIS authentication blocked by functional authority"
                    )
            else:
                if emergency.get("active") is not True:
                    raise KisOrderAuthorityError(
                        "KIS Kill authentication requires durable emergency stop"
                    )
                if control and not (
                    control["reservationKind"] == "KILL"
                    and control["phase"] == "CLEANUP"
                ):
                    raise KisOrderAuthorityError(
                        "KIS Kill authentication blocked by control reservation"
                    )
                if normalized_mode == "KILL_EXACT":
                    if type(expected_revision) is not int or expected_revision < 1:
                        raise KisOrderAuthorityError(
                            "KIS Kill authentication revision is invalid"
                        )
                    normalized = _normalize_intent(
                        intent or {}, expected_operation="KILL_ORDINARY_CANCEL"
                    )
                    durable = _normalize_intent(
                        snapshot["killOrdinaryCancelIntent"],
                        expected_operation="KILL_ORDINARY_CANCEL",
                    )
                    if (
                        snapshot["killOrdinaryCancelAllowed"] is not True
                        or snapshot["killOrdinaryCancelRevision"]
                        != expected_revision
                        or not secrets.compare_digest(
                            _intent_hash(normalized), _intent_hash(durable)
                        )
                    ):
                        raise KisOrderAuthorityError(
                            "KIS Kill authenticated reservation changed"
                        )
            scope = _new_auth_scope(normalized_mode, snapshot)
            _push_auth_scope(scope)
            try:
                yield
            finally:
                _pop_auth_scope(scope)


def _validate_inherited_snapshot(
    lease: _InheritedLease, snapshot: Mapping[str, Any]
) -> None:
    if (
        snapshot.get("stateRevision") != lease.state_revision
        or not secrets.compare_digest(
            str(snapshot.get("ownerEpochId") or ""), lease.owner_epoch_id
        )
        or not secrets.compare_digest(
            str(snapshot.get("ownerEpochHash") or ""), lease.owner_epoch_hash
        )
        or not secrets.compare_digest(
            str(snapshot.get("functionalAccountFingerprint") or ""),
            lease.account_fingerprint,
        )
        or not secrets.compare_digest(
            str(snapshot.get("credentialConfigurationHash") or ""),
            lease.credential_configuration_hash,
        )
        or not secrets.compare_digest(
            _intent_hash(snapshot.get("controlReservation") or {}),
            lease.control_reservation_hash,
        )
    ):
        raise KisOrderAuthorityError(
            "KIS owner epoch/state/account/credential lease changed"
        )


def require_inherited_kis_transport_authority(
    *,
    endpoint: str,
    payload_hash: str,
    account_fingerprint: str,
    credential_configuration_hash: str,
) -> Mapping[str, Any]:
    """Reject a KIS trading POST that bypassed a final mutation boundary."""

    lease = _inherited()
    if lease is None:
        raise KisOrderAuthorityError(
            "KIS trading transport requires an inherited final mutation lease"
        )
    snapshot = lease.read(endpoint=endpoint, payload_hash=payload_hash)
    _validate_inherited_snapshot(lease, snapshot)
    if (
        not secrets.compare_digest(
            account_fingerprint, lease.account_fingerprint
        )
        or not secrets.compare_digest(
            credential_configuration_hash,
            lease.credential_configuration_hash,
        )
    ):
        raise KisOrderAuthorityError(
            "KIS final transport account/credential differs from inherited lease"
        )
    return MappingProxyType(
        {
            **snapshot,
            "inheritedOperation": lease.operation,
            "inheritedMode": lease.mode,
            "inheritedCleanupOnly": lease.cleanup_only,
            "inheritedOwnedOrderKey": dict(
                lease.owned_order_key
            ),
        }
    )


def consume_inherited_kis_transport_authority(
    *,
    endpoint: str,
    payload_hash: str,
    account_fingerprint: str,
    credential_configuration_hash: str,
) -> Mapping[str, Any]:
    """Atomically burn one exact inherited lease immediately before I/O.

    The final state read happens before the burn.  Once this function returns,
    every outcome is conservatively ambiguous: the same lease nonce and the
    same owner-epoch/state/intent tuple remain consumed for this process even
    if socket setup fails before bytes are observed by the caller.
    """

    snapshot = require_inherited_kis_transport_authority(
        endpoint=endpoint,
        payload_hash=payload_hash,
        account_fingerprint=account_fingerprint,
        credential_configuration_hash=credential_configuration_hash,
    )
    lease = _inherited()
    if lease is None:  # pragma: no cover - guarded by require above.
        raise KisOrderAuthorityError(
            "KIS trading transport requires an inherited final mutation lease"
        )
    intent_key = (
        lease.owner_epoch_hash,
        lease.state_revision,
        lease.intent_hash,
    )
    with _TRANSPORT_CONSUMPTION_LOCK:
        if (
            lease.nonce in _CONSUMED_TRANSPORT_LEASES
            or intent_key in _CONSUMED_TRANSPORT_INTENTS
        ):
            raise KisOrderAuthorityError(
                "KIS final transport lease was already consumed"
            )
        _CONSUMED_TRANSPORT_LEASES.add(lease.nonce)
        _CONSUMED_TRANSPORT_INTENTS.add(intent_key)
    return snapshot


@contextmanager
def kis_route_authority_serialization() -> Iterator[None]:
    """Serialize KIS start/stop/settings changes with the final sender edge."""

    with _ROUTE_LOCK:
        yield


@contextmanager
def ordinary_kis_final_mutation_boundary(
    *, operation: str, intent: Mapping[str, Any]
) -> Iterator[Callable[..., Mapping[str, Any]]]:
    """Block every ordinary KIS mutation while functional authority is open."""

    operation = _validate_operation(operation)
    normalized_intent = _normalize_intent(intent, expected_operation=operation)
    normalized_intent_hash = _intent_hash(normalized_intent)
    inherited = _inherited()
    if inherited is not None:
        if (
            inherited.mode != "ORDINARY"
            or inherited.operation != operation
            or not secrets.compare_digest(
                inherited.intent_hash, normalized_intent_hash
            )
        ):
            raise KisOrderAuthorityError(
                "ordinary KIS mutation cannot change inherited authority"
            )
        inherited.read(
            endpoint=normalized_intent["endpoint"],
            payload_hash=normalized_intent["payloadHash"],
        )
        yield inherited.read
        return

    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is True:
                raise KisOrderAuthorityError(
                    f"ordinary KIS {operation} blocked by durable emergency stop"
                )

            def read(*, endpoint: str, payload_hash: str) -> Mapping[str, Any]:
                _validate_transport(
                    normalized_intent,
                    endpoint=endpoint,
                    payload_hash=payload_hash,
                )
                snapshot = _snapshot()
                if initial_snapshot is not None:
                    _validate_inherited_snapshot(initial_snapshot, snapshot)
                if snapshot["applicationInstanceLeaseHeld"] is not True:
                    raise KisOrderAuthorityError(
                        "ordinary KIS mutation requires the official application lease"
                    )
                if snapshot["controlReservation"]:
                    raise KisOrderAuthorityError(
                        "ordinary KIS mutation blocked by active control reservation"
                    )
                if (
                    snapshot["functionalAuthorityOpen"] is True
                    or snapshot["functionalPhase"] in _OPEN_PHASES
                    or snapshot["ordinaryRoutesClosed"] is True
                ):
                    raise KisOrderAuthorityError(
                        "ordinary KIS mutation blocked by functional authority"
                    )
                if (
                    not secrets.compare_digest(
                        str(snapshot["functionalAccountFingerprint"]),
                        normalized_intent["accountFingerprint"],
                    )
                    or not secrets.compare_digest(
                        str(snapshot["credentialConfigurationHash"]),
                        normalized_intent["credentialConfigurationHash"],
                    )
                ):
                    raise KisOrderAuthorityError(
                        "ordinary KIS account/credential authority changed"
                    )
                return MappingProxyType(
                    {
                        **snapshot,
                        "operation": operation,
                        "intentHash": normalized_intent_hash,
                        "emergencyActive": False,
                        "emergencyRevision": str(
                            emergency.get("revision") or ""
                        ),
                    }
                )

            initial_snapshot: _InheritedLease | None = None
            initial = read(
                endpoint=normalized_intent["endpoint"],
                payload_hash=normalized_intent["payloadHash"],
            )
            lease = _InheritedLease(
                mode="ORDINARY",
                owner_thread_id=threading.get_ident(),
                nonce=secrets.token_hex(32),
                operation=operation,
                intent_hash=normalized_intent_hash,
                owned_order_key=MappingProxyType(
                    dict(normalized_intent["ownedOrderKey"])
                ),
                session_id="",
                revision=int(initial["functionalRevision"]),
                state_revision=int(initial["stateRevision"]),
                owner_epoch_id=str(initial["ownerEpochId"]),
                owner_epoch_hash=str(initial["ownerEpochHash"]),
                account_fingerprint=str(initial["functionalAccountFingerprint"]),
                credential_configuration_hash=str(
                    initial["credentialConfigurationHash"]
                ),
                control_reservation_hash=_intent_hash(
                    initial["controlReservation"]
                ),
                cleanup_only=False,
                public_snapshot=initial,
                read=read,
            )
            initial_snapshot = lease
            _THREAD_BOUNDARY.lease = lease
            try:
                yield read
            finally:
                if getattr(_THREAD_BOUNDARY, "lease", None) is lease:
                    del _THREAD_BOUNDARY.lease


@contextmanager
def kill_ordinary_kis_cancel_boundary(
    *, intent: Mapping[str, Any], expected_revision: int
) -> Iterator[Callable[..., Mapping[str, Any]]]:
    """Permit only a pre-authorized exact ordinary cancel while Kill is ON."""

    operation = "KILL_ORDINARY_CANCEL"
    if type(expected_revision) is not int or expected_revision < 1:
        raise KisOrderAuthorityError("KIS Kill-cancel expected revision is invalid")
    normalized_intent = _normalize_intent(intent, expected_operation=operation)
    normalized_intent_hash = _intent_hash(normalized_intent)
    inherited = _inherited()
    if inherited is not None:
        if (
            inherited.mode != "KILL_ORDINARY_CANCEL"
            or inherited.operation != operation
            or inherited.revision != expected_revision
            or not secrets.compare_digest(
                inherited.intent_hash, normalized_intent_hash
            )
        ):
            raise KisOrderAuthorityError(
                "KIS Kill-cancel cannot change inherited authority"
            )
        inherited.read(
            endpoint=normalized_intent["endpoint"],
            payload_hash=normalized_intent["payloadHash"],
        )
        yield inherited.read
        return

    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is not True:
                raise KisOrderAuthorityError(
                    "KIS Kill-cancel requires durable emergency stop"
                )

            def read(*, endpoint: str, payload_hash: str) -> Mapping[str, Any]:
                _validate_transport(
                    normalized_intent,
                    endpoint=endpoint,
                    payload_hash=payload_hash,
                )
                snapshot = _snapshot()
                if initial_snapshot is not None:
                    _validate_inherited_snapshot(initial_snapshot, snapshot)
                if snapshot["applicationInstanceLeaseHeld"] is not True:
                    raise KisOrderAuthorityError(
                        "KIS Kill-cancel requires the official application lease"
                    )
                control = snapshot["controlReservation"]
                if control and not (
                    control["reservationKind"] == "KILL"
                    and control["phase"] == "CLEANUP"
                ):
                    raise KisOrderAuthorityError(
                        "KIS Kill-cancel blocked by unrelated control reservation"
                    )
                if (
                    snapshot["killOrdinaryCancelAllowed"] is not True
                    or snapshot["killOrdinaryCancelRevision"] != expected_revision
                ):
                    raise KisOrderAuthorityError(
                        "durable KIS Kill-cancel authority changed"
                    )
                durable_intent = _normalize_intent(
                    snapshot["killOrdinaryCancelIntent"],
                    expected_operation=operation,
                )
                if (
                    not secrets.compare_digest(
                        _intent_hash(durable_intent), normalized_intent_hash
                    )
                    or not secrets.compare_digest(
                        str(snapshot["functionalAccountFingerprint"]),
                        normalized_intent["accountFingerprint"],
                    )
                    or not secrets.compare_digest(
                        str(snapshot["credentialConfigurationHash"]),
                        normalized_intent["credentialConfigurationHash"],
                    )
                ):
                    raise KisOrderAuthorityError(
                        "durable KIS Kill-cancel identity/payload changed"
                    )
                return MappingProxyType(
                    {
                        **snapshot,
                        "operation": operation,
                        "intentHash": normalized_intent_hash,
                        "emergencyActive": True,
                        "emergencyRevision": str(
                            emergency.get("revision") or ""
                        ),
                    }
                )

            initial_snapshot: _InheritedLease | None = None
            initial = read(
                endpoint=normalized_intent["endpoint"],
                payload_hash=normalized_intent["payloadHash"],
            )
            burn_key = (
                str(initial["ownerEpochHash"]),
                expected_revision,
                normalized_intent_hash,
            )
            if burn_key in _CONSUMED_KILL_CANCEL_LEASES:
                raise KisOrderAuthorityError(
                    "KIS Kill-cancel lease was already consumed"
                )
            # The durable, FULL-synchronous commit happens before installing
            # the inherited lease.  Crash/timeout ambiguity therefore stays
            # consumed across process restart and requires reconciliation.
            _durably_burn_kill_cancel(
                snapshot=initial,
                expected_revision=expected_revision,
                normalized_intent=normalized_intent,
                normalized_intent_hash=normalized_intent_hash,
            )
            _CONSUMED_KILL_CANCEL_LEASES.add(burn_key)
            lease = _InheritedLease(
                mode="KILL_ORDINARY_CANCEL",
                owner_thread_id=threading.get_ident(),
                nonce=secrets.token_hex(32),
                operation=operation,
                intent_hash=normalized_intent_hash,
                owned_order_key=MappingProxyType(
                    dict(normalized_intent["ownedOrderKey"])
                ),
                session_id="",
                revision=expected_revision,
                state_revision=int(initial["stateRevision"]),
                owner_epoch_id=str(initial["ownerEpochId"]),
                owner_epoch_hash=str(initial["ownerEpochHash"]),
                account_fingerprint=str(initial["functionalAccountFingerprint"]),
                credential_configuration_hash=str(
                    initial["credentialConfigurationHash"]
                ),
                control_reservation_hash=_intent_hash(
                    initial["controlReservation"]
                ),
                cleanup_only=True,
                public_snapshot=initial,
                read=read,
            )
            initial_snapshot = lease
            _THREAD_BOUNDARY.lease = lease
            try:
                yield read
            finally:
                if getattr(_THREAD_BOUNDARY, "lease", None) is lease:
                    del _THREAD_BOUNDARY.lease


@contextmanager
def functional_kis_final_mutation_boundary(
    *,
    operation: str,
    session_id: str,
    cleanup_only: bool,
    expected_revision: int,
    intent: Mapping[str, Any],
) -> Iterator[Callable[..., Mapping[str, Any]]]:
    """Bind an immediate functional mutation to exact phase/session/revision."""

    operation = _validate_operation(operation)
    if type(session_id) is not str or not _IDENTITY.fullmatch(session_id):
        raise KisOrderAuthorityError("functional KIS session id is invalid")
    if type(cleanup_only) is not bool:
        raise KisOrderAuthorityError("functional KIS cleanup flag is invalid")
    if type(expected_revision) is not int or expected_revision < 1:
        raise KisOrderAuthorityError("functional KIS expected revision is invalid")
    allowed = _CLEANUP_OPERATIONS if cleanup_only else _ENTRY_OPERATIONS
    if operation not in allowed:
        raise KisOrderAuthorityError(
            "functional KIS operation does not match entry/cleanup authority"
        )
    normalized_intent = _normalize_intent(intent, expected_operation=operation)
    normalized_intent_hash = _intent_hash(normalized_intent)

    inherited = _inherited()
    expected_mode = "FUNCTIONAL_CLEANUP" if cleanup_only else "FUNCTIONAL_ENTRY"
    if inherited is not None:
        if (
            inherited.mode != expected_mode
            or inherited.operation != operation
            or not secrets.compare_digest(
                inherited.intent_hash, normalized_intent_hash
            )
            or not secrets.compare_digest(inherited.session_id, session_id)
            or inherited.revision != expected_revision
            or inherited.cleanup_only is not cleanup_only
            or inherited.read is None
        ):
            raise KisOrderAuthorityError(
                "nested KIS mutation cannot change inherited authority"
            )
        inherited.read(
            endpoint=normalized_intent["endpoint"],
            payload_hash=normalized_intent["payloadHash"],
        )
        yield inherited.read
        return

    with _ROUTE_LOCK:
        with emergency_stop_dispatch_boundary() as emergency:
            if emergency.get("active") is True and not cleanup_only:
                raise KisOrderAuthorityError(
                    "functional KIS entry blocked by durable emergency stop"
                )

            def read(*, endpoint: str, payload_hash: str) -> Mapping[str, Any]:
                _validate_transport(
                    normalized_intent,
                    endpoint=endpoint,
                    payload_hash=payload_hash,
                )
                snapshot = _snapshot()
                if initial_snapshot is not None:
                    _validate_inherited_snapshot(initial_snapshot, snapshot)
                expected_phases = (
                    {"CLEANUP", "RECONCILIATION_REQUIRED"}
                    if cleanup_only
                    else {"ACTIVE"}
                )
                control = snapshot["controlReservation"]
                control_allows_cleanup = bool(
                    cleanup_only
                    and control
                    and control["reservationKind"] in {"STOP", "KILL"}
                    and control["phase"] == "CLEANUP"
                )
                if control and not control_allows_cleanup:
                    raise KisOrderAuthorityError(
                        "functional KIS mutation blocked by control reservation"
                    )
                try:
                    durable_intent = _normalize_intent(
                        snapshot["functionalMutationIntent"],
                        expected_operation=operation,
                    )
                except KisOrderAuthorityError:
                    durable_intent = {}
                matches = bool(
                    snapshot["functionalAuthorityOpen"] is True
                    and snapshot["applicationInstanceLeaseHeld"] is True
                    and snapshot["ordinaryRoutesClosed"] is True
                    and secrets.compare_digest(
                        str(snapshot["functionalSessionId"]), session_id
                    )
                    and snapshot["functionalPhase"] in expected_phases
                    and snapshot["functionalRevision"] == expected_revision
                    and snapshot["stateRevision"] == expected_revision
                    and secrets.compare_digest(
                        str(snapshot["functionalAccountFingerprint"]),
                        normalized_intent["accountFingerprint"],
                    )
                    and secrets.compare_digest(
                        str(snapshot["credentialConfigurationHash"]),
                        normalized_intent["credentialConfigurationHash"],
                    )
                    and bool(durable_intent)
                    and secrets.compare_digest(
                        _intent_hash(durable_intent), normalized_intent_hash
                    )
                    and not (emergency.get("active") is True and not cleanup_only)
                )
                if not matches:
                    raise KisOrderAuthorityError(
                        "functional KIS phase/session/revision authority changed"
                    )
                return MappingProxyType(
                    {
                        **snapshot,
                        "active": True,
                        "operation": operation,
                        "intentHash": normalized_intent_hash,
                        "sessionId": session_id,
                        "cleanupOnly": cleanup_only,
                        "expectedRevision": expected_revision,
                        "emergencyActive": emergency.get("active") is True,
                        "emergencyRevision": str(
                            emergency.get("revision") or ""
                        ),
                    }
                )

            initial_snapshot: _InheritedLease | None = None
            initial = read(
                endpoint=normalized_intent["endpoint"],
                payload_hash=normalized_intent["payloadHash"],
            )
            lease = _InheritedLease(
                mode=expected_mode,
                owner_thread_id=threading.get_ident(),
                nonce=secrets.token_hex(32),
                operation=operation,
                intent_hash=normalized_intent_hash,
                owned_order_key=MappingProxyType(
                    dict(normalized_intent["ownedOrderKey"])
                ),
                session_id=session_id,
                revision=expected_revision,
                state_revision=int(initial["stateRevision"]),
                owner_epoch_id=str(initial["ownerEpochId"]),
                owner_epoch_hash=str(initial["ownerEpochHash"]),
                account_fingerprint=str(initial["functionalAccountFingerprint"]),
                credential_configuration_hash=str(
                    initial["credentialConfigurationHash"]
                ),
                control_reservation_hash=_intent_hash(
                    initial["controlReservation"]
                ),
                cleanup_only=cleanup_only,
                public_snapshot=initial,
                read=read,
            )
            initial_snapshot = lease
            _THREAD_BOUNDARY.lease = lease
            try:
                yield read
            finally:
                if getattr(_THREAD_BOUNDARY, "lease", None) is lease:
                    del _THREAD_BOUNDARY.lease


def _reset_kis_order_authority_reader_for_tests() -> None:
    global _AUTHORITY_READER
    global _KILL_CANCEL_JOURNAL_PATH
    with _PROVIDER_LOCK:
        _AUTHORITY_READER = None
        _KILL_CANCEL_JOURNAL_PATH = None
    with _ROUTE_LOCK:
        _CONSUMED_KILL_CANCEL_LEASES.clear()
    with _TRANSPORT_CONSUMPTION_LOCK:
        _CONSUMED_TRANSPORT_LEASES.clear()
        _CONSUMED_TRANSPORT_INTENTS.clear()
        _CONSUMED_READ_TRANSPORTS.clear()
    try:
        del _THREAD_BOUNDARY.lease
    except AttributeError:
        pass
    try:
        del _THREAD_BOUNDARY.auth_scopes
    except AttributeError:
        pass


__all__ = [
    "KisOrderAuthorityError",
    "consume_kis_read_transport_authority",
    "consume_inherited_kis_transport_authority",
    "functional_kis_final_mutation_boundary",
    "kill_ordinary_kis_cancel_boundary",
    "kis_authenticated_mutation_preflight",
    "kis_functional_authority_open_fail_closed",
    "kis_order_authority_binding",
    "kis_read_diagnostic_boundary",
    "kis_route_authority_serialization",
    "ordinary_kis_final_mutation_boundary",
    "register_kis_order_authority_reader",
    "require_kis_token_authority",
    "require_inherited_kis_transport_authority",
    "require_kis_read_transport_authority",
]
