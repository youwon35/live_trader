from __future__ import annotations

"""Pinned, durable account-exclusivity proof consumer for Upbit.

The trading process deliberately has no signing primitive.  An independently
owned authority writes one Ed25519-signed JSON proof for each exact truth-read
request.  This module verifies that proof, consumes its per-session hash chain
with a durable SQLite cursor, and exposes only the already-existing read-only
proof callback/verifier injection surfaces.

Nothing in this module enables broker networking or mutation.  The production
release latch remains false until the shared server composition root is wired
and independently reviewed.
"""

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
import types
from typing import Any, Callable, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .upbit_continuous_functional import (
    ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION_V2,
    ACCOUNT_EXCLUSIVITY_VERIFIER_PIN_SCHEMA_VERSION,
    AccountExclusivityProofVerifier,
    UpbitFunctionalBlocked,
    _account_exclusivity_request_payload,
    _normalized_account_exclusivity_verifier_pin,
    _strict_stable_hash,
    _utc,
    _utc_text,
    _verify_account_exclusivity_proof,
    account_exclusivity_verifier_wiring_status,
)


UPBIT_ACCOUNT_EXCLUSIVITY_PRODUCTION_RELEASED = False
UPBIT_ACCOUNT_EXCLUSIVITY_NETWORK_ALLOWED = False
UPBIT_ACCOUNT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT = False
UPBIT_ACCOUNT_EXCLUSIVITY_PROVIDER_SCHEMA_VERSION = (
    "upbit-account-exclusivity-durable-provider/v1"
)
UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_VERSION = (
    "upbit-account-exclusivity-consumer-cursor/v1"
)
UPBIT_ACCOUNT_EXCLUSIVITY_OUTBOX_SCHEMA_VERSION = (
    "upbit-account-exclusivity-proof-request-outbox/v1"
)
UPBIT_ACCOUNT_EXCLUSIVITY_VERIFIER_TYPE = (
    "PINNED_ED25519_DURABLE_UPBIT_EXCLUSIVITY_V1"
)
UPBIT_ACCOUNT_EXCLUSIVITY_ALGORITHM = "ED25519_RFC8032_SHA512"
MAX_PROOF_FILE_BYTES = 256 * 1024
MAX_PROOF_AGE_SECONDS = 15

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_PHASE_RE = re.compile(r"^[A-Z][A-Z0-9_]{2,63}$")
_SIGNATURE_RE = re.compile(r"^[A-Za-z0-9_-]{86}$")
_ZERO_HASH = "0" * 64
_CURSOR_APPLICATION_ID = 0x55504254
_CURSOR_USER_VERSION = 1
_CURSOR_TABLE_SQL = """CREATE TABLE upbit_exclusivity_cursor (
    session_id TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    credential_binding_sha256 TEXT NOT NULL,
    server_owner_identity_sha256 TEXT NOT NULL,
    authority_journal_id TEXT NOT NULL,
    authority_sequence INTEGER NOT NULL,
    proof_hash TEXT NOT NULL,
    proof_request_hash TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    terminal_verified INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    row_hash TEXT NOT NULL
)"""
_CURSOR_SESSION_INDEX_SQL = """CREATE UNIQUE INDEX
upbit_exclusivity_cursor_session_uq
ON upbit_exclusivity_cursor(session_id)"""
_CURSOR_EVENT_TABLE_SQL = """CREATE TABLE upbit_exclusivity_cursor_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    authority_sequence INTEGER NOT NULL,
    proof_hash TEXT NOT NULL,
    proof_request_hash TEXT NOT NULL,
    previous_event_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL
)"""
_CURSOR_EVENT_SEQUENCE_INDEX_SQL = """CREATE UNIQUE INDEX
upbit_exclusivity_event_session_sequence_uq
ON upbit_exclusivity_cursor_event(session_id, authority_sequence)"""
_CURSOR_EVENT_REQUEST_INDEX_SQL = """CREATE UNIQUE INDEX
upbit_exclusivity_event_session_request_uq
ON upbit_exclusivity_cursor_event(session_id, proof_request_hash)"""


def _text(value: object) -> str:
    return str(value or "").strip()


def _hash(value: object) -> str:
    return _strict_stable_hash(value)


def _normalized_sql(value: object) -> str:
    return " ".join(_text(value).split())


def _expected_cursor_schema_snapshot() -> dict[str, Any]:
    def columns(*rows: tuple[str, str, int, int]) -> list[dict[str, Any]]:
        return [
            {
                "cid": index,
                "name": name,
                "type": column_type,
                "notNull": not_null,
                "default": None,
                "primaryKey": primary_key,
            }
            for index, (name, column_type, not_null, primary_key) in enumerate(
                rows
            )
        ]

    return {
        "applicationId": _CURSOR_APPLICATION_ID,
        "userVersion": _CURSOR_USER_VERSION,
        "journalMode": "wal",
        "objects": sorted(
            [
                {
                    "type": "table",
                    "name": "sqlite_sequence",
                    "tableName": "sqlite_sequence",
                    "sql": "CREATE TABLE sqlite_sequence(name,seq)",
                },
                {
                    "type": "table",
                    "name": "upbit_exclusivity_cursor",
                    "tableName": "upbit_exclusivity_cursor",
                    "sql": _normalized_sql(_CURSOR_TABLE_SQL),
                },
                {
                    "type": "index",
                    "name": "upbit_exclusivity_cursor_session_uq",
                    "tableName": "upbit_exclusivity_cursor",
                    "sql": _normalized_sql(_CURSOR_SESSION_INDEX_SQL),
                },
                {
                    "type": "table",
                    "name": "upbit_exclusivity_cursor_event",
                    "tableName": "upbit_exclusivity_cursor_event",
                    "sql": _normalized_sql(_CURSOR_EVENT_TABLE_SQL),
                },
                {
                    "type": "index",
                    "name": "upbit_exclusivity_event_session_request_uq",
                    "tableName": "upbit_exclusivity_cursor_event",
                    "sql": _normalized_sql(_CURSOR_EVENT_REQUEST_INDEX_SQL),
                },
                {
                    "type": "index",
                    "name": "upbit_exclusivity_event_session_sequence_uq",
                    "tableName": "upbit_exclusivity_cursor_event",
                    "sql": _normalized_sql(_CURSOR_EVENT_SEQUENCE_INDEX_SQL),
                },
            ],
            key=lambda row: (row["type"], row["name"]),
        ),
        "tables": {
            "sqlite_sequence": {
                "columns": columns(
                    ("name", "", 0, 0),
                    ("seq", "", 0, 0),
                ),
                "foreignKeys": [],
                "indexes": [],
            },
            "upbit_exclusivity_cursor": {
                "columns": columns(
                    ("session_id", "TEXT", 1, 0),
                    ("account_fingerprint", "TEXT", 1, 0),
                    ("credential_binding_sha256", "TEXT", 1, 0),
                    ("server_owner_identity_sha256", "TEXT", 1, 0),
                    ("authority_journal_id", "TEXT", 1, 0),
                    ("authority_sequence", "INTEGER", 1, 0),
                    ("proof_hash", "TEXT", 1, 0),
                    ("proof_request_hash", "TEXT", 1, 0),
                    ("observed_at", "TEXT", 1, 0),
                    ("terminal_verified", "INTEGER", 1, 0),
                    ("revision", "INTEGER", 1, 0),
                    ("row_hash", "TEXT", 1, 0),
                ),
                "foreignKeys": [],
                "indexes": [
                    {
                        "name": "upbit_exclusivity_cursor_session_uq",
                        "unique": 1,
                        "origin": "c",
                        "partial": 0,
                        "columns": ["session_id"],
                    }
                ],
            },
            "upbit_exclusivity_cursor_event": {
                "columns": columns(
                    ("event_id", "INTEGER", 0, 1),
                    ("session_id", "TEXT", 1, 0),
                    ("authority_sequence", "INTEGER", 1, 0),
                    ("proof_hash", "TEXT", 1, 0),
                    ("proof_request_hash", "TEXT", 1, 0),
                    ("previous_event_hash", "TEXT", 1, 0),
                    ("event_hash", "TEXT", 1, 0),
                ),
                "foreignKeys": [],
                "indexes": sorted(
                    [
                        {
                            "name": (
                                "upbit_exclusivity_event_session_request_uq"
                            ),
                            "unique": 1,
                            "origin": "c",
                            "partial": 0,
                            "columns": [
                                "session_id",
                                "proof_request_hash",
                            ],
                        },
                        {
                            "name": (
                                "upbit_exclusivity_event_session_sequence_uq"
                            ),
                            "unique": 1,
                            "origin": "c",
                            "partial": 0,
                            "columns": [
                                "session_id",
                                "authority_sequence",
                            ],
                        },
                    ],
                    key=lambda row: row["name"],
                ),
            },
        },
    }


UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT = _hash(
    _expected_cursor_schema_snapshot()
)


def _require_hash(value: object, label: str) -> str:
    text = _text(value)
    if (
        type(value) is not str
        or value != text
        or _HASH_RE.fullmatch(text) is None
    ):
        raise UpbitFunctionalBlocked(
            f"upbit-account-exclusivity-{label}-invalid"
        )
    return text


def _require_id(value: object, label: str) -> str:
    text = _text(value)
    if (
        type(value) is not str
        or value != text
        or _SAFE_ID_RE.fullmatch(text) is None
    ):
        raise UpbitFunctionalBlocked(
            f"upbit-account-exclusivity-{label}-invalid"
        )
    return text


def _require_phase(value: object) -> str:
    text = _text(value)
    if (
        type(value) is not str
        or value != text
        or _PHASE_RE.fullmatch(text) is None
    ):
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-phase-invalid"
        )
    return text


def upbit_spot_credential_binding_sha256(
    access_key: str,
    secret_key: str,
) -> str:
    """Hash the exact access/secret pair without persisting either value."""

    if (
        type(access_key) is not str
        or type(secret_key) is not str
        or not access_key
        or not secret_key
    ):
        return ""
    return hashlib.sha256(
        b"UPBIT_SPOT_CREDENTIAL_SET\0"
        + access_key.encode("utf-8")
        + b"\0"
        + secret_key.encode("utf-8")
    ).hexdigest()


def canonical_exclusivity_signature_message(
    payload: Mapping[str, Any],
) -> bytes:
    """Return the only byte encoding accepted by the Ed25519 verifier."""

    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _code_value(value: object) -> object:
    if isinstance(value, types.CodeType):
        return {
            "bytecode": value.co_code.hex(),
            "constants": [_code_value(item) for item in value.co_consts],
            "names": list(value.co_names),
            "variables": list(value.co_varnames),
            "argcount": value.co_argcount,
            "kwonlyargcount": value.co_kwonlyargcount,
        }
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}"}


def _verifier_code_sha256() -> str:
    methods = (
        PinnedEd25519UpbitAccountExclusivityVerifier.identity,
        PinnedEd25519UpbitAccountExclusivityVerifier.__call__,
        PinnedEd25519UpbitAccountExclusivityVerifier._payload_context_exact,
    )
    return _hash(
        {
            "contract": "upbit-pinned-ed25519-exclusivity-verifier/v1",
            "methods": [_code_value(method.__code__) for method in methods],
        }
    )


class PinnedEd25519UpbitAccountExclusivityVerifier:
    """Public-key-only verifier bound to one account, credential and owner."""

    def __init__(
        self,
        *,
        public_key: bytes | str,
        verifier_id: str,
        key_id: str,
        authority_journal_id: str,
        expected_account_fingerprint: str,
        expected_credential_binding_sha256: str,
        expected_server_owner_identity_sha256: str,
    ) -> None:
        self.verifier_id = _require_id(verifier_id, "verifier-id")
        self.key_id = _require_id(key_id, "key-id")
        self.authority_journal_id = _require_id(
            authority_journal_id, "authority-journal-id"
        )
        self.expected_account_fingerprint = _require_hash(
            expected_account_fingerprint, "expected-account-fingerprint"
        )
        self.expected_credential_binding_sha256 = _require_hash(
            expected_credential_binding_sha256,
            "expected-credential-binding",
        )
        self.expected_server_owner_identity_sha256 = _require_hash(
            expected_server_owner_identity_sha256,
            "expected-server-owner-identity",
        )
        try:
            key = ECC.import_key(public_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-public-key-invalid"
            ) from exc
        if key.has_private() or getattr(key, "curve", None) != "Ed25519":
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-public-key-only-ed25519-required"
            )
        self._public_key = key
        public_der = key.export_key(format="DER")
        key_fingerprint = hashlib.sha256(public_der).hexdigest()
        config = {
            "schemaVersion": "upbit-account-exclusivity-verifier-config/v1",
            "verifierId": self.verifier_id,
            "keyId": self.key_id,
            "authorityJournalId": self.authority_journal_id,
            "algorithm": UPBIT_ACCOUNT_EXCLUSIVITY_ALGORITHM,
            "verifierType": UPBIT_ACCOUNT_EXCLUSIVITY_VERIFIER_TYPE,
            "accountFingerprint": self.expected_account_fingerprint,
            "credentialBindingSha256": (
                self.expected_credential_binding_sha256
            ),
            "serverOwnerIdentitySha256": (
                self.expected_server_owner_identity_sha256
            ),
            "maxProofAgeSeconds": MAX_PROOF_AGE_SECONDS,
            "publicKeyFingerprintSha256": key_fingerprint,
        }
        self._pin = {
            "schemaVersion": ACCOUNT_EXCLUSIVITY_VERIFIER_PIN_SCHEMA_VERSION,
            "verifierId": self.verifier_id,
            "keyId": self.key_id,
            "algorithm": UPBIT_ACCOUNT_EXCLUSIVITY_ALGORITHM,
            "verifierType": UPBIT_ACCOUNT_EXCLUSIVITY_VERIFIER_TYPE,
            "verifierCodeSha256": _verifier_code_sha256(),
            "verifierConfigSha256": _hash(config),
            "keyFingerprintSha256": key_fingerprint,
            "authorityPinned": True,
        }

    def identity(self) -> Mapping[str, Any]:
        return dict(self._pin)

    def _payload_context_exact(self, payload: Mapping[str, Any]) -> bool:
        return bool(
            payload.get("schemaVersion")
            == ACCOUNT_EXCLUSIVITY_PROOF_SCHEMA_VERSION_V2
            and hmac.compare_digest(
                _text(payload.get("accountFingerprint")),
                self.expected_account_fingerprint,
            )
            and hmac.compare_digest(
                _text(payload.get("credentialBindingSha256")),
                self.expected_credential_binding_sha256,
            )
            and hmac.compare_digest(
                _text(payload.get("serverOwnerIdentitySha256")),
                self.expected_server_owner_identity_sha256,
            )
            and hmac.compare_digest(
                _text(payload.get("authorityJournalId")),
                self.authority_journal_id,
            )
            and isinstance(payload.get("authority"), Mapping)
            and dict(payload["authority"]) == self._pin
        )

    def __call__(
        self,
        *,
        payload: Mapping[str, Any],
        signature: str,
        verifier_pin: Mapping[str, Any],
    ) -> bool:
        if (
            _normalized_account_exclusivity_verifier_pin(verifier_pin)
            != self._pin
            or not self._payload_context_exact(payload)
            or type(signature) is not str
            or _SIGNATURE_RE.fullmatch(signature) is None
        ):
            return False
        try:
            raw_signature = base64.urlsafe_b64decode(signature + "==")
            if len(raw_signature) != 64:
                return False
            eddsa.new(self._public_key, "rfc8032").verify(
                canonical_exclusivity_signature_message(payload),
                raw_signature,
            )
        except (ValueError, TypeError):
            return False
        return True


def _strict_json_file(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-proof-file-missing-or-link"
        )
    size = path.stat().st_size
    if size <= 1 or size > MAX_PROOF_FILE_BYTES:
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-proof-file-size-invalid"
        )

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
        if len(raw) != size or raw.startswith(b"\xef\xbb\xbf"):
            raise ValueError("unstable or BOM-prefixed proof")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON number")
            ),
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError) as exc:
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-proof-file-invalid"
        ) from exc
    if not isinstance(value, dict):
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-proof-file-not-object"
        )
    return value


class DurableUpbitAccountExclusivityProofProvider:
    """Consume exact signed proof files with a restart-verifiable local cursor."""

    def __init__(
        self,
        *,
        proof_directory: str | Path,
        cursor_database_path: str | Path,
        verifier: PinnedEd25519UpbitAccountExclusivityVerifier,
        expected_verifier_pin: Mapping[str, Any],
        account_fingerprint_reader: Callable[[], str],
        credential_binding_reader: Callable[[], str],
        server_owner_identity_reader: Callable[[], str],
        clock: Callable[[], datetime],
        proof_wait_seconds: float = 0.0,
    ) -> None:
        raw_proof_directory = Path(proof_directory)
        if raw_proof_directory.is_symlink():
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-durable-proof-directory-invalid"
            )
        self.proof_directory = raw_proof_directory.resolve()
        self._cursor_raw_path = Path(cursor_database_path).absolute()
        if (
            self._cursor_raw_path.is_symlink()
            or (
                self._cursor_raw_path.exists()
                and not self._cursor_raw_path.is_file()
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-path-invalid"
            )
        raw_parent = self._cursor_raw_path.parent
        if raw_parent.exists() and (
            raw_parent.is_symlink()
            or raw_parent.absolute() != raw_parent.resolve()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-parent-link-invalid"
            )
        self.cursor_database_path = (
            raw_parent.resolve() / self._cursor_raw_path.name
        )
        self._cursor_file_identity: tuple[int, int] | None = None
        self.verifier = verifier
        normalized_pin = _normalized_account_exclusivity_verifier_pin(
            expected_verifier_pin
        )
        if (
            normalized_pin is None
            or normalized_pin != dict(verifier.identity())
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-durable-verifier-pin-mismatch"
            )
        self.verifier_pin = normalized_pin
        self.account_fingerprint_reader = account_fingerprint_reader
        self.credential_binding_reader = credential_binding_reader
        self.server_owner_identity_reader = server_owner_identity_reader
        self.clock = clock
        try:
            wait_seconds = float(proof_wait_seconds)
        except (TypeError, ValueError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-proof-wait-invalid"
            ) from exc
        if (
            isinstance(proof_wait_seconds, bool)
            or not math.isfinite(wait_seconds)
            or wait_seconds < 0
            or wait_seconds > 5
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-proof-wait-invalid"
            )
        self.proof_wait_seconds = wait_seconds
        self._lock = threading.RLock()
        self._last_failure = ""
        if (
            not self.proof_directory.is_dir()
            or self.proof_directory.is_symlink()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-durable-proof-directory-invalid"
            )
        self.cursor_database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_cursor()

    @staticmethod
    def _file_identity(path: Path) -> tuple[int, int]:
        try:
            stat = path.stat()
        except OSError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-path-unavailable"
            ) from exc
        identity = (int(stat.st_dev), int(stat.st_ino))
        if identity[1] <= 0:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-file-id-unavailable"
            )
        return identity

    def _assert_cursor_path_exact(self) -> tuple[int, int]:
        raw = self._cursor_raw_path
        try:
            if (
                raw.is_symlink()
                or not raw.is_file()
                or raw.resolve() != self.cursor_database_path
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-account-exclusivity-cursor-path-drift"
                )
        except OSError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-path-unavailable"
            ) from exc
        identity = self._file_identity(raw)
        if (
            self._cursor_file_identity is not None
            and identity != self._cursor_file_identity
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-file-replaced"
            )
        return identity

    def _assert_connection_path_exact(
        self, connection: sqlite3.Connection
    ) -> None:
        rows = connection.execute("PRAGMA database_list").fetchall()
        if len(rows) != 1:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-database-list-invalid"
            )
        row = dict(rows[0])
        try:
            actual = Path(str(row.get("file") or "")).resolve()
        except OSError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-connected-path-invalid"
            ) from exc
        if row.get("name") != "main" or actual != self.cursor_database_path:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-connected-path-invalid"
            )

    @staticmethod
    def _schema_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
        objects = [
            {
                "type": str(row["type"]),
                "name": str(row["name"]),
                "tableName": str(row["tbl_name"]),
                "sql": _normalized_sql(row["sql"]),
            }
            for row in connection.execute(
                """SELECT type,name,tbl_name,sql FROM sqlite_master
                   ORDER BY type,name"""
            ).fetchall()
        ]
        tables: dict[str, Any] = {}
        table_names = sorted(
            row["name"]
            for row in objects
            if row["type"] == "table"
        )
        for table_name in table_names:
            columns = [
                {
                    "cid": int(row["cid"]),
                    "name": str(row["name"]),
                    "type": str(row["type"]),
                    "notNull": int(row["notnull"]),
                    "default": row["dflt_value"],
                    "primaryKey": int(row["pk"]),
                }
                for row in connection.execute(
                    f'PRAGMA table_info("{table_name}")'
                ).fetchall()
            ]
            foreign_keys = [
                dict(row)
                for row in connection.execute(
                    f'PRAGMA foreign_key_list("{table_name}")'
                ).fetchall()
            ]
            indexes: list[dict[str, Any]] = []
            for raw_index in connection.execute(
                f'PRAGMA index_list("{table_name}")'
            ).fetchall():
                index = dict(raw_index)
                index_name = str(index["name"])
                indexes.append(
                    {
                        "name": index_name,
                        "unique": int(index["unique"]),
                        "origin": str(index["origin"]),
                        "partial": int(index["partial"]),
                        "columns": [
                            str(row["name"])
                            for row in connection.execute(
                                f'PRAGMA index_info("{index_name}")'
                            ).fetchall()
                        ],
                    }
                )
            tables[table_name] = {
                "columns": columns,
                "foreignKeys": foreign_keys,
                "indexes": sorted(indexes, key=lambda row: row["name"]),
            }
        return {
            "applicationId": int(
                connection.execute("PRAGMA application_id").fetchone()[0]
            ),
            "userVersion": int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            ),
            "journalMode": str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower(),
            "objects": objects,
            "tables": tables,
        }

    def _validate_cursor_schema(
        self, connection: sqlite3.Connection
    ) -> str:
        snapshot = self._schema_snapshot(connection)
        fingerprint = _hash(snapshot)
        if (
            snapshot != _expected_cursor_schema_snapshot()
            or not hmac.compare_digest(
                fingerprint,
                UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-schema-fingerprint-invalid"
            )
        return fingerprint

    def _connect(self) -> sqlite3.Connection:
        self._assert_cursor_path_exact()
        try:
            connection = sqlite3.connect(
                self.cursor_database_path,
                timeout=2,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=2000")
            connection.execute("PRAGMA synchronous=FULL")
            self._assert_connection_path_exact(connection)
            self._assert_cursor_path_exact()
            self._validate_cursor_schema(connection)
            return connection
        except Exception:
            try:
                connection.close()
            except (NameError, sqlite3.Error):
                pass
            raise

    def _initialize_cursor(self) -> None:
        raw = self._cursor_raw_path
        fresh = not raw.exists()
        if raw.is_symlink() or (not fresh and not raw.is_file()):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-cursor-path-invalid"
            )
        try:
            connection = sqlite3.connect(
                self.cursor_database_path,
                timeout=2,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=2000")
            connection.execute("PRAGMA synchronous=FULL")
            self._assert_connection_path_exact(connection)
            if fresh:
                existing = connection.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master"
                ).fetchall()
                if existing:
                    raise UpbitFunctionalBlocked(
                        "upbit-account-exclusivity-cursor-not-fresh"
                    )
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                ).lower()
                if journal_mode != "wal":
                    raise UpbitFunctionalBlocked(
                        "upbit-account-exclusivity-cursor-wal-required"
                    )
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(_CURSOR_TABLE_SQL)
                    connection.execute(_CURSOR_SESSION_INDEX_SQL)
                    connection.execute(_CURSOR_EVENT_TABLE_SQL)
                    connection.execute(_CURSOR_EVENT_SEQUENCE_INDEX_SQL)
                    connection.execute(_CURSOR_EVENT_REQUEST_INDEX_SQL)
                    connection.execute(
                        f"PRAGMA application_id={_CURSOR_APPLICATION_ID}"
                    )
                    connection.execute(
                        f"PRAGMA user_version={_CURSOR_USER_VERSION}"
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise
        except Exception:
            try:
                connection.close()
            except (NameError, sqlite3.Error):
                pass
            raise
        else:
            connection.close()
        self._cursor_file_identity = self._assert_cursor_path_exact()
        with closing(self._connect()) as verified:
            self._validate_cursor_schema(verified)

    @staticmethod
    def _row_body(value: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: value[key]
            for key in (
                "session_id",
                "account_fingerprint",
                "credential_binding_sha256",
                "server_owner_identity_sha256",
                "authority_journal_id",
                "authority_sequence",
                "proof_hash",
                "proof_request_hash",
                "observed_at",
                "terminal_verified",
                "revision",
            )
        }

    @classmethod
    def _row_valid(cls, row: Mapping[str, Any]) -> bool:
        try:
            return hmac.compare_digest(
                _text(row.get("row_hash")), _hash(cls._row_body(row))
            )
        except (KeyError, TypeError, ValueError):
            return False

    def request_descriptor(
        self,
        *,
        session_id: str,
        phase: str,
        account_fingerprint: str,
        session_started_at: datetime,
        observation_started_at: datetime,
        observed_at: datetime,
    ) -> dict[str, Any]:
        actual_account = _require_hash(
            self.account_fingerprint_reader(), "current-account-fingerprint"
        )
        supplied_account = _require_hash(
            account_fingerprint, "requested-account-fingerprint"
        )
        credential_binding = _require_hash(
            self.credential_binding_reader(), "current-credential-binding"
        )
        owner_identity = _require_hash(
            self.server_owner_identity_reader(), "current-server-owner-identity"
        )
        if (
            not hmac.compare_digest(
                actual_account, self.verifier.expected_account_fingerprint
            )
            or not hmac.compare_digest(actual_account, supplied_account)
            or not hmac.compare_digest(
                credential_binding,
                self.verifier.expected_credential_binding_sha256,
            )
            or not hmac.compare_digest(
                owner_identity,
                self.verifier.expected_server_owner_identity_sha256,
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-current-identity-rotated"
            )
        request = _account_exclusivity_request_payload(
            session_id=_require_id(session_id, "session-id"),
            phase=_require_phase(phase),
            account_fingerprint=supplied_account,
            credential_binding_sha256=credential_binding,
            server_owner_identity_sha256=owner_identity,
            session_started_at=_utc(session_started_at, "session-started-at"),
            observation_started_at=_utc(
                observation_started_at, "observation-started-at"
            ),
            observed_at=_utc(observed_at, "observed-at"),
        )
        return {**request, "proofRequestHash": _hash(request)}

    def _proof_path(self, request_hash: str) -> Path:
        path = self.proof_directory / f"{request_hash}.json"
        try:
            if path.resolve().parent != self.proof_directory:
                raise ValueError("proof path escaped")
        except (OSError, ValueError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-proof-path-invalid"
            ) from exc
        return path

    def _publish_request(self, descriptor: Mapping[str, Any]) -> Path:
        request_hash = _require_hash(
            descriptor.get("proofRequestHash"), "outbox-request-hash"
        )
        body = {
            "schemaVersion": UPBIT_ACCOUNT_EXCLUSIVITY_OUTBOX_SCHEMA_VERSION,
            "authorityJournalId": self.verifier.authority_journal_id,
            "verifierPinHash": _hash(self.verifier_pin),
            "request": dict(descriptor),
        }
        envelope = {**body, "contentHash": _hash(body)}
        encoded = json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        path = self.proof_directory / f"{request_hash}.request.json"
        try:
            with path.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            try:
                if path.is_symlink() or path.read_bytes() != encoded:
                    raise UpbitFunctionalBlocked(
                        "upbit-account-exclusivity-request-outbox-conflict"
                    )
            except OSError as exc:
                raise UpbitFunctionalBlocked(
                    "upbit-account-exclusivity-request-outbox-unavailable"
                ) from exc
        except OSError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-request-outbox-unavailable"
            ) from exc
        return path

    def _consume_cursor(
        self,
        *,
        proof: Mapping[str, Any],
        proof_hash: str,
    ) -> None:
        session_id = _text(proof.get("sessionId"))
        sequence = int(proof.get("authoritySequence"))
        request_hash = _require_hash(
            proof.get("proofRequestHash"), "proof-request-hash"
        )
        observed_at = _text(proof.get("observedAt"))
        phase = _text(proof.get("phase")).upper()
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._assert_cursor_path_exact()
                self._validate_cursor_schema(connection)
                row = connection.execute(
                    "SELECT * FROM upbit_exclusivity_cursor WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if row is None:
                    if (
                        sequence != 1
                        or proof.get("previousAuthorityProofHash") != _ZERO_HASH
                        or phase != "BASELINE"
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-account-exclusivity-chain-genesis-invalid"
                        )
                    revision = 1
                    previous_event_hash = _ZERO_HASH
                else:
                    current = dict(row)
                    if not self._row_valid(current):
                        raise UpbitFunctionalBlocked(
                            "upbit-account-exclusivity-cursor-tampered"
                        )
                    event_state = self._validate_event_chain_locked(
                        connection, session_id
                    )
                    if (
                        event_state["authoritySequence"]
                        != int(current["authority_sequence"])
                        or event_state["proofHash"]
                        != current["proof_hash"]
                        or event_state["proofRequestHash"]
                        != current["proof_request_hash"]
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-account-exclusivity-event-cursor-mismatch"
                        )
                    if (
                        current["proof_request_hash"] == request_hash
                        and current["proof_hash"] == proof_hash
                        and int(current["authority_sequence"]) == sequence
                    ):
                        self._validate_cursor_schema(connection)
                        self._assert_cursor_path_exact()
                        connection.commit()
                        self._validate_cursor_schema(connection)
                        self._assert_cursor_path_exact()
                        return
                    if (
                        int(current["terminal_verified"]) == 1
                        and phase != "FINAL"
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-account-exclusivity-proof-after-terminal"
                        )
                    if (
                        sequence != int(current["authority_sequence"]) + 1
                        or not hmac.compare_digest(
                            _text(proof.get("previousAuthorityProofHash")),
                            _text(current["proof_hash"]),
                        )
                        or _utc(proof.get("observedAt"), "proof-observed-at")
                        <= _utc(current["observed_at"], "cursor-observed-at")
                    ):
                        raise UpbitFunctionalBlocked(
                            "upbit-account-exclusivity-chain-discontinuity"
                        )
                    revision = int(current["revision"]) + 1
                    previous_event_hash = event_state["eventHash"]
                event_body = {
                    "schemaVersion": UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_VERSION,
                    "sessionId": session_id,
                    "authoritySequence": sequence,
                    "proofHash": proof_hash,
                    "proofRequestHash": request_hash,
                    "previousEventHash": previous_event_hash,
                }
                event_hash = _hash(event_body)
                connection.execute(
                    """INSERT INTO upbit_exclusivity_cursor_event
                       (session_id,authority_sequence,proof_hash,
                        proof_request_hash,previous_event_hash,event_hash)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        session_id,
                        sequence,
                        proof_hash,
                        request_hash,
                        previous_event_hash,
                        event_hash,
                    ),
                )
                row_body = {
                    "session_id": session_id,
                    "account_fingerprint": proof["accountFingerprint"],
                    "credential_binding_sha256": proof[
                        "credentialBindingSha256"
                    ],
                    "server_owner_identity_sha256": proof[
                        "serverOwnerIdentitySha256"
                    ],
                    "authority_journal_id": proof["authorityJournalId"],
                    "authority_sequence": sequence,
                    "proof_hash": proof_hash,
                    "proof_request_hash": request_hash,
                    "observed_at": observed_at,
                    "terminal_verified": int(phase == "FINAL"),
                    "revision": revision,
                }
                connection.execute(
                    """INSERT INTO upbit_exclusivity_cursor
                       (session_id,account_fingerprint,
                        credential_binding_sha256,
                        server_owner_identity_sha256,authority_journal_id,
                        authority_sequence,proof_hash,proof_request_hash,
                        observed_at,terminal_verified,revision,row_hash)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(session_id) DO UPDATE SET
                        account_fingerprint=excluded.account_fingerprint,
                        credential_binding_sha256=excluded.credential_binding_sha256,
                        server_owner_identity_sha256=excluded.server_owner_identity_sha256,
                        authority_journal_id=excluded.authority_journal_id,
                        authority_sequence=excluded.authority_sequence,
                        proof_hash=excluded.proof_hash,
                        proof_request_hash=excluded.proof_request_hash,
                        observed_at=excluded.observed_at,
                        terminal_verified=excluded.terminal_verified,
                        revision=excluded.revision,row_hash=excluded.row_hash""",
                    (*row_body.values(), _hash(row_body)),
                )
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
                connection.commit()
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _validate_event_chain_locked(
        connection: sqlite3.Connection,
        session_id: str,
    ) -> dict[str, Any]:
        rows = connection.execute(
            """SELECT authority_sequence,proof_hash,proof_request_hash,
                      previous_event_hash,event_hash
               FROM upbit_exclusivity_cursor_event WHERE session_id=?
               ORDER BY authority_sequence ASC""",
            (session_id,),
        ).fetchall()
        previous = _ZERO_HASH
        last: dict[str, Any] | None = None
        for index, raw in enumerate(rows, 1):
            row = dict(raw)
            body = {
                "schemaVersion": UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_VERSION,
                "sessionId": session_id,
                "authoritySequence": int(row["authority_sequence"]),
                "proofHash": row["proof_hash"],
                "proofRequestHash": row["proof_request_hash"],
                "previousEventHash": row["previous_event_hash"],
            }
            if (
                int(row["authority_sequence"]) != index
                or row["previous_event_hash"] != previous
                or row["event_hash"] != _hash(body)
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-account-exclusivity-event-chain-tampered"
                )
            previous = row["event_hash"]
            last = {
                "authoritySequence": index,
                "proofHash": row["proof_hash"],
                "proofRequestHash": row["proof_request_hash"],
                "eventHash": row["event_hash"],
            }
        if last is None:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-event-chain-missing"
            )
        return last

    def read_strict(
        self,
        *,
        session_id: str,
        phase: str,
        account_fingerprint: str,
        session_started_at: datetime,
        observation_started_at: datetime,
        observed_at: datetime,
    ) -> Mapping[str, Any]:
        descriptor = self.request_descriptor(
            session_id=session_id,
            phase=phase,
            account_fingerprint=account_fingerprint,
            session_started_at=session_started_at,
            observation_started_at=observation_started_at,
            observed_at=observed_at,
        )
        current = _utc(self.clock(), "current-time")
        proof_observed_at = _utc(observed_at, "proof-observed-at")
        age = (current - proof_observed_at).total_seconds()
        if age < 0 or age > MAX_PROOF_AGE_SECONDS:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-proof-stale-or-future"
            )
        self._publish_request(descriptor)
        proof_path = self._proof_path(descriptor["proofRequestHash"])
        deadline = time.monotonic() + self.proof_wait_seconds
        while not proof_path.is_file() and time.monotonic() < deadline:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        proof = _strict_json_file(proof_path)
        consumed_at = _utc(self.clock(), "proof-consumed-at")
        consumed_age = (consumed_at - proof_observed_at).total_seconds()
        if consumed_age < 0 or consumed_age > MAX_PROOF_AGE_SECONDS:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-proof-stale-or-future"
            )
        normalized, proof_hash, verified = _verify_account_exclusivity_proof(
            proof,
            session_id=session_id,
            account_fingerprint=account_fingerprint,
            session_started_at=session_started_at,
            observation_started_at=observation_started_at,
            observed_at=observed_at,
            verifier=self.verifier,
            verifier_pin=self.verifier_pin,
            phase=phase,
        )
        if verified is not True or normalized != proof:
            raise UpbitFunctionalBlocked(
                "upbit-account-exclusivity-signed-proof-invalid"
            )
        with self._lock:
            self._consume_cursor(proof=normalized, proof_hash=proof_hash)
            self._last_failure = ""
        return normalized

    def __call__(self, **kwargs: Any) -> Mapping[str, Any]:
        """Fail closed as evidence, while never blocking cleanup reads."""

        try:
            return self.read_strict(**kwargs)
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, UpbitFunctionalBlocked)
                else "upbit-account-exclusivity-provider-unavailable"
            )
            with self._lock:
                self._last_failure = reason
            return {
                "schemaVersion": "upbit-account-exclusivity-safe-incomplete/v1",
                "verificationState": "SAFE_INCOMPLETE",
                "verificationReason": reason,
            }

    def session_status(self, session_id: str) -> dict[str, Any]:
        event_verified = False
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM upbit_exclusivity_cursor WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if row is not None:
                try:
                    event = self._validate_event_chain_locked(
                        connection, _text(session_id)
                    )
                    event_verified = bool(
                        event["authoritySequence"]
                        == int(row["authority_sequence"])
                        and event["proofHash"] == row["proof_hash"]
                        and event["proofRequestHash"]
                        == row["proof_request_hash"]
                    )
                except UpbitFunctionalBlocked:
                    event_verified = False
            self._validate_cursor_schema(connection)
            self._assert_cursor_path_exact()
        if row is None:
            return {
                "present": False,
                "recordHashVerified": False,
                "terminalVerified": False,
            }
        value = dict(row)
        return {
            "present": True,
            "recordHashVerified": (
                self._row_valid(value) and event_verified
            ),
            "authoritySequence": int(value["authority_sequence"]),
            "proofHash": value["proof_hash"],
            "observedAt": value["observed_at"],
            "terminalVerified": bool(value["terminal_verified"]),
            "revision": int(value["revision"]),
        }

    def consumed_payload_verified(
        self,
        payload: Mapping[str, Any],
        *,
        signature: str,
    ) -> bool:
        """Prove the signed payload is the exact durable consumer head."""

        try:
            session_id = _text(payload.get("sessionId"))
            sequence = int(payload.get("authoritySequence"))
            request_hash = _require_hash(
                payload.get("proofRequestHash"), "consumed-request-hash"
            )
            current_account = _require_hash(
                self.account_fingerprint_reader(),
                "reverified-account-fingerprint",
            )
            current_credential = _require_hash(
                self.credential_binding_reader(),
                "reverified-credential-binding",
            )
            current_owner = _require_hash(
                self.server_owner_identity_reader(),
                "reverified-server-owner-identity",
            )
            if type(signature) is not str:
                return False
            payload_hash = _hash(dict(payload))
            proof_hash = _hash(
                {
                    **dict(payload),
                    "payloadHash": payload_hash,
                    "signature": signature,
                }
            )
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """SELECT * FROM upbit_exclusivity_cursor
                       WHERE session_id=?""",
                    (session_id,),
                ).fetchone()
                if row is None:
                    return False
                current = dict(row)
                event = self._validate_event_chain_locked(
                    connection, session_id
                )
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
            return bool(
                self._row_valid(current)
                and int(current["authority_sequence"]) == sequence
                and current["proof_request_hash"] == request_hash
                and current["proof_hash"] == proof_hash
                and current["account_fingerprint"]
                == payload.get("accountFingerprint")
                and current["account_fingerprint"] == current_account
                and current["credential_binding_sha256"]
                == payload.get("credentialBindingSha256")
                and current["credential_binding_sha256"]
                == current_credential
                and current["server_owner_identity_sha256"]
                == payload.get("serverOwnerIdentitySha256")
                and current["server_owner_identity_sha256"]
                == current_owner
                and current["authority_journal_id"]
                == payload.get("authorityJournalId")
                and current["observed_at"] == payload.get("observedAt")
                and event["authoritySequence"] == sequence
                and event["proofRequestHash"] == request_hash
                and event["proofHash"] == proof_hash
                and (
                    _text(payload.get("phase")).upper() != "FINAL"
                    or int(current["terminal_verified"]) == 1
                )
            )
        except (TypeError, ValueError, UpbitFunctionalBlocked):
            return False

    def _cursor_store_restart_verifiable(self) -> bool:
        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    "SELECT * FROM upbit_exclusivity_cursor"
                ).fetchall()
                event_sessions = {
                    _text(row["session_id"])
                    for row in connection.execute(
                        """SELECT DISTINCT session_id
                           FROM upbit_exclusivity_cursor_event"""
                    ).fetchall()
                }
                cursor_sessions: set[str] = set()
                for raw in rows:
                    row = dict(raw)
                    session_id = _text(row.get("session_id"))
                    if not session_id or not self._row_valid(row):
                        return False
                    event = self._validate_event_chain_locked(
                        connection, session_id
                    )
                    if (
                        event["authoritySequence"]
                        != int(row["authority_sequence"])
                        or event["proofHash"] != row["proof_hash"]
                        or event["proofRequestHash"]
                        != row["proof_request_hash"]
                    ):
                        return False
                    cursor_sessions.add(session_id)
                result = cursor_sessions == event_sessions
                self._validate_cursor_schema(connection)
                self._assert_cursor_path_exact()
                return result
        except (OSError, sqlite3.Error, TypeError, ValueError, UpbitFunctionalBlocked):
            return False

    def status(self) -> dict[str, Any]:
        verifier_status = account_exclusivity_verifier_wiring_status(
            self.verifier, self.verifier_pin
        )
        identity_matched = False
        try:
            identity_matched = bool(
                hmac.compare_digest(
                    _require_hash(
                        self.account_fingerprint_reader(), "current-account"
                    ),
                    self.verifier.expected_account_fingerprint,
                )
                and hmac.compare_digest(
                    _require_hash(
                        self.credential_binding_reader(), "current-credential"
                    ),
                    self.verifier.expected_credential_binding_sha256,
                )
                and hmac.compare_digest(
                    _require_hash(
                        self.server_owner_identity_reader(), "current-owner"
                    ),
                    self.verifier.expected_server_owner_identity_sha256,
                )
            )
        except UpbitFunctionalBlocked:
            identity_matched = False
        restart_verifiable = self._cursor_store_restart_verifiable()
        ready = bool(
            verifier_status.get("ready") is True
            and identity_matched
            and self.proof_directory.is_dir()
            and self.cursor_database_path.is_file()
            and restart_verifiable
        )
        return {
            "schemaVersion": UPBIT_ACCOUNT_EXCLUSIVITY_PROVIDER_SCHEMA_VERSION,
            "injectionReady": ready,
            "liveActivationReleased": (
                UPBIT_ACCOUNT_EXCLUSIVITY_PRODUCTION_RELEASED
            ),
            "networkAllowed": UPBIT_ACCOUNT_EXCLUSIVITY_NETWORK_ALLOWED,
            "signingPrimitivePresent": (
                UPBIT_ACCOUNT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT
            ),
            "durableProofSource": True,
            "durableRequestOutbox": True,
            "durableConsumerCursor": True,
            "durable": True,
            "restartVerifiable": restart_verifiable,
            "cursorSchemaFingerprint": (
                UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT
                if restart_verifiable
                else ""
            ),
            "cursorPathIdentityPinned": bool(
                restart_verifiable
                and self._cursor_file_identity is not None
            ),
            "asymmetricPublicKeyOnly": True,
            "exactCurrentIdentityMatched": identity_matched,
            "continuousPerSessionHashChainRequired": True,
            "terminalProofRequiredForPass": True,
            "cleanupAllowedOnProofLoss": True,
            "verifier": verifier_status,
            "proofDirectoryHash": hashlib.sha256(
                str(self.proof_directory).encode("utf-8")
            ).hexdigest(),
            "cursorDatabasePathHash": hashlib.sha256(
                str(self.cursor_database_path).encode("utf-8")
            ).hexdigest(),
            "lastFailure": self._last_failure,
            "proofWaitSeconds": self.proof_wait_seconds,
            "networkOrderPostAllowed": False,
        }


class DurableCursorBoundUpbitAccountExclusivityVerifier:
    """Reverify Ed25519 plus the exact durable consumer-chain head."""

    def __init__(
        self,
        *,
        cryptographic_verifier: PinnedEd25519UpbitAccountExclusivityVerifier,
        provider: DurableUpbitAccountExclusivityProofProvider,
    ) -> None:
        self.cryptographic_verifier = cryptographic_verifier
        self.provider = provider

    def identity(self) -> Mapping[str, Any]:
        return self.cryptographic_verifier.identity()

    def __call__(
        self,
        *,
        payload: Mapping[str, Any],
        signature: str,
        verifier_pin: Mapping[str, Any],
    ) -> bool:
        return bool(
            self.cryptographic_verifier(
                payload=payload,
                signature=signature,
                verifier_pin=verifier_pin,
            )
            and self.provider.consumed_payload_verified(
                payload,
                signature=signature,
            )
        )


@dataclass(frozen=True, slots=True)
class UpbitAccountExclusivityInjection:
    proof_reader: DurableUpbitAccountExclusivityProofProvider
    verifier: AccountExclusivityProofVerifier
    verifier_pin: Mapping[str, Any]

    def status(self) -> dict[str, Any]:
        return self.proof_reader.status()


def build_upbit_account_exclusivity_injection(
    *,
    proof_directory: str | Path,
    cursor_database_path: str | Path,
    public_key_path: str | Path,
    verifier_pin_path: str | Path,
    verifier_id: str,
    key_id: str,
    authority_journal_id: str,
    expected_account_fingerprint: str,
    expected_credential_binding_sha256: str,
    expected_server_owner_identity_sha256: str,
    account_fingerprint_reader: Callable[[], str],
    credential_binding_reader: Callable[[], str],
    server_owner_identity_reader: Callable[[], str],
    clock: Callable[[], datetime],
    proof_wait_seconds: float = 0.0,
) -> UpbitAccountExclusivityInjection:
    """Build the three values consumed by backend/entrypoint composition."""

    raw_key_path = Path(public_key_path)
    key_path = raw_key_path.resolve()
    if (
        raw_key_path.is_symlink()
        or
        not key_path.is_file()
        or key_path.is_symlink()
        or key_path.stat().st_size > 16 * 1024
    ):
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-public-key-file-invalid"
        )
    verifier = PinnedEd25519UpbitAccountExclusivityVerifier(
        public_key=key_path.read_bytes(),
        verifier_id=verifier_id,
        key_id=key_id,
        authority_journal_id=authority_journal_id,
        expected_account_fingerprint=expected_account_fingerprint,
        expected_credential_binding_sha256=(
            expected_credential_binding_sha256
        ),
        expected_server_owner_identity_sha256=(
            expected_server_owner_identity_sha256
        ),
    )
    raw_pin_path = Path(verifier_pin_path)
    if raw_pin_path.is_symlink():
        raise UpbitFunctionalBlocked(
            "upbit-account-exclusivity-verifier-pin-file-invalid"
        )
    pin = _strict_json_file(raw_pin_path.resolve())
    provider = DurableUpbitAccountExclusivityProofProvider(
        proof_directory=proof_directory,
        cursor_database_path=cursor_database_path,
        verifier=verifier,
        expected_verifier_pin=pin,
        account_fingerprint_reader=account_fingerprint_reader,
        credential_binding_reader=credential_binding_reader,
        server_owner_identity_reader=server_owner_identity_reader,
        clock=clock,
        proof_wait_seconds=proof_wait_seconds,
    )
    durable_verifier = DurableCursorBoundUpbitAccountExclusivityVerifier(
        cryptographic_verifier=verifier,
        provider=provider,
    )
    return UpbitAccountExclusivityInjection(
        proof_reader=provider,
        verifier=durable_verifier,
        verifier_pin=dict(pin),
    )


__all__ = [
    "DurableUpbitAccountExclusivityProofProvider",
    "DurableCursorBoundUpbitAccountExclusivityVerifier",
    "PinnedEd25519UpbitAccountExclusivityVerifier",
    "UPBIT_ACCOUNT_EXCLUSIVITY_NETWORK_ALLOWED",
    "UPBIT_ACCOUNT_EXCLUSIVITY_PRODUCTION_RELEASED",
    "UPBIT_ACCOUNT_EXCLUSIVITY_SIGNING_PRIMITIVE_PRESENT",
    "UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT",
    "UpbitAccountExclusivityInjection",
    "build_upbit_account_exclusivity_injection",
    "canonical_exclusivity_signature_message",
    "upbit_spot_credential_binding_sha256",
]
