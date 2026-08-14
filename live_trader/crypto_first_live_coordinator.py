from __future__ import annotations

"""Durable cross-exchange ownership for crypto first-live functional runs.

The Upbit and Binance functional lanes each have their own one-shot permit and
cleanup ledger.  This store is the deliberately smaller, shared authority that
prevents both lanes from becoming active at the same time.  It owns no broker
transport and cannot submit, cancel, transfer, borrow, or withdraw anything.

An ``APPROVED_INERT`` reservation is still a HOLD state.  Only a caller that
retains the process-local owner token can activate it, and every mutation is a
SQLite ``BEGIN IMMEDIATE`` compare-and-swap with an append-only hash chain.
"""

from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any, Callable, Iterator, Mapping


SCHEMA_VERSION = "crypto-first-live-coordinator/v3"
HIGH_WATER_SCHEMA_VERSION = "crypto-first-live-high-water-anchor/v1"
GLOBAL_SCOPE = "CRYPTO_FIRST_LIVE_GLOBAL"
LANES = frozenset({"UPBIT", "BINANCE_SPOT"})
PHASES = frozenset(
    {
        "PREPARING",
        "APPROVED_INERT",
        "ACTIVATION_PREPARING",
        "ACTIVE",
        "CLEANUP_ONLY",
        "FINALIZED",
        "RECONCILIATION_REQUIRED",
    }
)
REVOCABLE_PHASES = frozenset(
    {"PREPARING", "APPROVED_INERT", "ACTIVATION_PREPARING", "ACTIVE"}
)
MAX_OWNER_LEASE_SECONDS = 60.0
EXACT_FIRST_LIVE_SECONDS = 7200.0
MAX_RESERVATION_EVIDENCE_AGE_SECONDS = 30.0
# Deliberately code-owned.  Preparation may be shipped and exercised offline,
# but no caller or environment variable can open ACTIVE authority yet.
CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED = False
# A separate SQLite file catches one-sided rollback/replacement.  It cannot,
# by itself, prove that an attacker did not restore both files to the same
# valid prefix.  Production activation therefore also remains code-blocked
# until an independently administered monotonic/WORM checkpoint is wired.
CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED = False

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class CryptoFirstLiveCoordinatorError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


def _exact_hash(value: object, label: str) -> str:
    text = _text(value)
    if _HASH_RE.fullmatch(text) is None:
        raise CryptoFirstLiveCoordinatorError(f"{label}-must-be-lowercase-sha256")
    return text


def _exact_id(value: object, label: str) -> str:
    text = _text(value)
    if _ID_RE.fullmatch(text) is None:
        raise CryptoFirstLiveCoordinatorError(f"{label}-is-not-exact")
    return text


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _account_lease_scope(lane: str, account_fingerprint: str) -> str:
    return f"crypto-first-live-account:{lane}:{account_fingerprint}"


class DurableCryptoFirstLiveCoordinator:
    """One global first-live owner across Upbit and Binance Spot."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
        high_water_anchor: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
        reservation_evidence_verifier: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
        final_approval_consumer: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
        terminal_evidence_verifier: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
        startup_owner_absent_reader: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
        installation_validator: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        owner_identity_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        route_lock_verifier: (
            Callable[[Mapping[str, Any]], Mapping[str, Any]] | None
        ) = None,
    ) -> None:
        self.path = Path(path)
        self.clock = clock
        self.monotonic_clock = monotonic_clock
        self.high_water_anchor = high_water_anchor
        self.reservation_evidence_verifier = reservation_evidence_verifier
        self.final_approval_consumer = final_approval_consumer
        self.terminal_evidence_verifier = terminal_evidence_verifier
        self.startup_owner_absent_reader = startup_owner_absent_reader
        self.installation_validator = installation_validator
        self.owner_identity_verifier = owner_identity_verifier
        self.route_lock_verifier = route_lock_verifier
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @contextmanager
    def _write(
        self, *, purpose: str = "MUTATION"
    ) -> Iterator[sqlite3.Connection]:
        before = self._require_anchor_exact(purpose=purpose + "_PREPARE")
        conn = self._connect()
        committed = False
        after: dict[str, Any] | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._verify_schema(conn)
            self._verify_integrity(conn)
            current = self._local_high_water(conn)
            if (
                current["databaseId"] != before["databaseId"]
                or current["revision"] != before["revision"]
                or current["publicationHash"]
                != before["publicationHash"]
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-publication-cas-changed"
                )
            yield conn
            conn.execute("COMMIT")
            committed = True
            after = self._local_high_water(conn)
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        if committed and after is not None and (
            after["revision"] != before["revision"]
            or after["publicationHash"] != before["publicationHash"]
        ):
            self._advance_anchor(
                purpose=purpose + "_COMMIT",
                before=before,
                after=after,
            )

    def _initialize(self) -> None:
        existed = self.path.is_file()
        size = self.path.stat().st_size if existed else 0
        if not existed or size == 0:
            validator = self.installation_validator
            request = {
                "schemaVersion": SCHEMA_VERSION,
                "databaseExists": existed,
                "databaseSize": size,
                "requiredCrossAudit": (
                    "UPBIT_AND_BINANCE_NONTERMINAL_POINTERS_ABSENT"
                ),
            }
            if validator is None or validator(request) is not True:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-installation-unverified"
                )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_metadata (
                    scope_key TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    database_id TEXT NOT NULL,
                    created_epoch REAL NOT NULL
                )
                """
            )
            metadata = conn.execute(
                "SELECT * FROM crypto_first_live_metadata WHERE scope_key=?",
                (GLOBAL_SCOPE,),
            ).fetchone()
            if metadata is None:
                conn.execute(
                    """
                    INSERT INTO crypto_first_live_metadata(
                        scope_key, schema_version, database_id, created_epoch
                    ) VALUES(?,?,?,?)
                    """,
                    (
                        GLOBAL_SCOPE,
                        SCHEMA_VERSION,
                        "crypto-first-live-db-" + secrets.token_hex(24),
                        self._now(),
                    ),
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_control (
                    scope_key TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    baseline_hash TEXT NOT NULL,
                    code_hash TEXT NOT NULL,
                    approval_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    reservation_evidence_hash TEXT NOT NULL,
                    reservation_receipt_hash TEXT NOT NULL,
                    final_approval_hash TEXT NOT NULL,
                    final_approval_receipt_hash TEXT NOT NULL,
                    owner_identity_json TEXT NOT NULL,
                    owner_identity_hash TEXT NOT NULL,
                    owner_token_hash TEXT NOT NULL,
                    owner_epoch INTEGER NOT NULL,
                    owner_lease_expires_epoch REAL NOT NULL,
                    hard_stop_epoch REAL NOT NULL,
                    hard_stop_monotonic REAL NOT NULL,
                    updated_monotonic REAL NOT NULL,
                    monotonic_boot_id TEXT NOT NULL,
                    terminal_evidence_hash TEXT NOT NULL,
                    created_epoch REAL NOT NULL,
                    updated_epoch REAL NOT NULL,
                    revision INTEGER NOT NULL,
                    detail TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_events (
                    event_id TEXT PRIMARY KEY,
                    scope_key TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_epoch REAL NOT NULL,
                    revision INTEGER NOT NULL,
                    previous_hash TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS crypto_first_live_events_scope_idx
                ON crypto_first_live_events(scope_key, revision, event_id)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_approval_consumptions (
                    approval_id TEXT PRIMARY KEY,
                    approval_consumption_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    claim_revision INTEGER NOT NULL,
                    publication_hash TEXT NOT NULL,
                    receipt_hash TEXT NOT NULL,
                    consumed_epoch REAL NOT NULL
                )
                """
            )
            self._verify_schema(conn)
            self._verify_integrity(conn)
            conn.execute("COMMIT")
            self._verify_empty_store_authority(
                conn, purpose="INITIALIZE"
            )
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        self._require_anchor_exact(purpose="INITIALIZE")

    @staticmethod
    def _column_names(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
        return tuple(
            str(row[1]) for row in conn.execute(f"PRAGMA table_xinfo({table})")
        )

    @classmethod
    def _verify_schema(cls, conn: sqlite3.Connection) -> None:
        metadata = cls._column_names(
            conn, "crypto_first_live_metadata"
        )
        control = cls._column_names(conn, "crypto_first_live_control")
        events = cls._column_names(conn, "crypto_first_live_events")
        consumptions = cls._column_names(
            conn, "crypto_first_live_approval_consumptions"
        )
        if metadata != (
            "scope_key",
            "schema_version",
            "database_id",
            "created_epoch",
        ) or control != (
            "scope_key",
            "schema_version",
            "phase",
            "run_id",
            "lane",
            "session_id",
            "permit_id",
            "account_fingerprint",
            "baseline_hash",
            "code_hash",
            "approval_id",
            "permit_hash",
            "reservation_evidence_hash",
            "reservation_receipt_hash",
            "final_approval_hash",
            "final_approval_receipt_hash",
            "owner_identity_json",
            "owner_identity_hash",
            "owner_token_hash",
            "owner_epoch",
            "owner_lease_expires_epoch",
            "hard_stop_epoch",
            "hard_stop_monotonic",
            "updated_monotonic",
            "monotonic_boot_id",
            "terminal_evidence_hash",
            "created_epoch",
            "updated_epoch",
            "revision",
            "detail",
        ) or events != (
            "event_id",
            "scope_key",
            "run_id",
            "event_type",
            "occurred_epoch",
            "revision",
            "previous_hash",
            "content_json",
            "content_hash",
        ) or consumptions != (
            "approval_id",
            "approval_consumption_id",
            "run_id",
            "lane",
            "session_id",
            "permit_id",
            "account_fingerprint",
            "permit_hash",
            "claim_revision",
            "publication_hash",
            "receipt_hash",
            "consumed_epoch",
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-schema-mismatch"
            )
        indexes = {
            str(row[1]): int(row[2])
            for row in conn.execute(
                "PRAGMA index_list(crypto_first_live_events)"
            )
        }
        if indexes.get("crypto_first_live_events_scope_idx") != 0:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-event-index-mismatch"
            )
        index_columns = tuple(
            _text(row[2])
            for row in conn.execute(
                "PRAGMA index_info(crypto_first_live_events_scope_idx)"
            )
        )
        if index_columns != ("scope_key", "revision", "event_id"):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-event-index-columns-mismatch"
            )
        objects = {
            (_text(row["type"]), _text(row["name"]), _text(row["tbl_name"]))
            for row in conn.execute(
                """
                SELECT type, name, tbl_name FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                """
            )
        }
        if objects != {
            ("table", "crypto_first_live_metadata", "crypto_first_live_metadata"),
            ("table", "crypto_first_live_control", "crypto_first_live_control"),
            ("table", "crypto_first_live_events", "crypto_first_live_events"),
            (
                "table",
                "crypto_first_live_approval_consumptions",
                "crypto_first_live_approval_consumptions",
            ),
            ("index", "crypto_first_live_events_scope_idx", "crypto_first_live_events"),
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-sqlite-objects-mismatch"
            )

    @staticmethod
    def _control_hash(row: sqlite3.Row) -> str:
        return _digest(
            {
                str(key): row[key]
                for key in row.keys()
            }
        )

    @classmethod
    def _verify_integrity(cls, conn: sqlite3.Connection) -> None:
        metadata = conn.execute(
            "SELECT * FROM crypto_first_live_metadata"
        ).fetchall()
        controls = conn.execute(
            "SELECT * FROM crypto_first_live_control"
        ).fetchall()
        events = conn.execute(
            """
            SELECT scope_key, run_id, event_type, occurred_epoch, revision,
                   previous_hash, content_json, content_hash
            FROM crypto_first_live_events
            ORDER BY revision, event_id
            """,
        ).fetchall()
        if (
            len(metadata) != 1
            or _text(metadata[0]["scope_key"]) != GLOBAL_SCOPE
            or _text(metadata[0]["schema_version"]) != SCHEMA_VERSION
            or _ID_RE.fullmatch(_text(metadata[0]["database_id"])) is None
            or not math.isfinite(float(metadata[0]["created_epoch"]))
            or float(metadata[0]["created_epoch"]) <= 0
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-metadata-integrity-invalid"
            )
        if any(_text(row["scope_key"]) != GLOBAL_SCOPE for row in controls) or any(
            _text(row["scope_key"]) != GLOBAL_SCOPE for row in events
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-foreign-scope-row-rejected"
            )
        if not controls and not events:
            return
        if len(controls) != 1 or not events:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-control-event-integrity-invalid"
            )
        previous_hash = ""
        previous_revision = 0
        latest_payload: dict[str, Any] = {}
        activated_consumptions: list[tuple[Any, ...]] = []
        for event in events:
            content_json = _text(event["content_json"])
            content_hash = hashlib.sha256(
                content_json.encode("utf-8")
            ).hexdigest()
            if not secrets.compare_digest(
                content_hash, _text(event["content_hash"])
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-event-chain-invalid"
                )
            payload = json.loads(content_json)
            if (
                not isinstance(payload, dict)
                or int(event["revision"]) != previous_revision + 1
                or int(payload.get("revision", -1)) != int(event["revision"])
                or _text(payload.get("scope")) != _text(event["scope_key"])
                or _text(payload.get("runId")) != _text(event["run_id"])
                or _text(payload.get("eventType")) != _text(event["event_type"])
                or float(payload.get("occurredEpoch", -1))
                != float(event["occurred_epoch"])
                or _text(event["previous_hash"]) != previous_hash
                or _text(payload.get("previousHash")) != previous_hash
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-event-chain-invalid"
                )
            latest_payload = payload
            if _text(payload.get("eventType")) == "ACTIVATED":
                activated_consumptions.append(
                    (
                        _text(payload.get("approvalId")),
                        _text(payload.get("approvalConsumptionId")),
                        _text(payload.get("runId")),
                        _text(payload.get("lane")),
                        _text(payload.get("sessionId")),
                        _text(payload.get("permitId")),
                        _text(payload.get("accountFingerprint")),
                        _text(payload.get("permitHash")),
                        int(payload.get("approvalClaimRevision", -1)),
                        _text(payload.get("approvalPublicationHash")),
                        _text(payload.get("finalApprovalReceiptHash")),
                        float(payload.get("approvalConsumedEpoch", -1)),
                    )
                )
            previous_hash = content_hash
            previous_revision = int(event["revision"])
        if not secrets.compare_digest(
            _text(latest_payload.get("controlHash")),
            cls._control_hash(controls[0]),
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-control-hash-invalid"
            )
        consumption_rows = conn.execute(
            """
            SELECT approval_id, approval_consumption_id, run_id, lane,
                   session_id, permit_id, account_fingerprint, permit_hash,
                   claim_revision, publication_hash, receipt_hash,
                   consumed_epoch
            FROM crypto_first_live_approval_consumptions
            ORDER BY approval_id
            """
        ).fetchall()
        durable_consumptions = sorted(
            tuple(row[key] for key in row.keys()) for row in consumption_rows
        )
        if sorted(activated_consumptions) != durable_consumptions:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-approval-consumption-integrity-invalid"
            )

    @staticmethod
    def _local_high_water(conn: sqlite3.Connection) -> dict[str, Any]:
        metadata = conn.execute(
            """
            SELECT database_id FROM crypto_first_live_metadata
            WHERE scope_key=?
            """,
            (GLOBAL_SCOPE,),
        ).fetchone()
        if metadata is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-metadata-missing"
            )
        event = conn.execute(
            """
            SELECT revision, content_hash FROM crypto_first_live_events
            WHERE scope_key=? ORDER BY revision DESC, event_id DESC LIMIT 1
            """,
            (GLOBAL_SCOPE,),
        ).fetchone()
        return {
            "databaseId": _text(metadata["database_id"]),
            "revision": int(event["revision"]) if event is not None else 0,
            "publicationHash": (
                _text(event["content_hash"]) if event is not None else ""
            ),
        }

    @staticmethod
    def _validate_anchor_response(
        value: Mapping[str, Any],
    ) -> dict[str, Any]:
        response = dict(value)
        if set(response) != {
            "schemaVersion",
            "scope",
            "databaseId",
            "revision",
            "publicationHash",
            "durable",
            "restartVerifiable",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-response-fields-not-exact"
            )
        try:
            revision = int(response["revision"])
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-response-invalid"
            ) from exc
        publication_hash = _text(response["publicationHash"])
        if (
            _text(response["schemaVersion"])
            != HIGH_WATER_SCHEMA_VERSION
            or _text(response["scope"]) != GLOBAL_SCOPE
            or _ID_RE.fullmatch(_text(response["databaseId"])) is None
            or revision < 0
            or (revision == 0 and publication_hash != "")
            or (revision > 0 and _HASH_RE.fullmatch(publication_hash) is None)
            or response["durable"] is not True
            or response["restartVerifiable"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-response-invalid"
            )
        return {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "scope": GLOBAL_SCOPE,
            "databaseId": _text(response["databaseId"]),
            "revision": revision,
            "publicationHash": publication_hash,
            "durable": True,
            "restartVerifiable": True,
        }

    def _observe_anchor(
        self,
        *,
        purpose: str,
        local: Mapping[str, Any],
    ) -> dict[str, Any]:
        authority = self.high_water_anchor
        if authority is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-authority-unavailable"
            )
        request = {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "REGISTER_OR_OBSERVE",
            "purpose": purpose,
            "scope": GLOBAL_SCOPE,
            "databaseId": _text(local["databaseId"]),
            "localRevision": int(local["revision"]),
            "localPublicationHash": _text(local["publicationHash"]),
        }
        response = authority(request)
        if not isinstance(response, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-response-invalid"
            )
        return self._validate_anchor_response(response)

    def _require_anchor_exact(self, *, purpose: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            local = self._local_high_water(conn)
        finally:
            conn.close()
        anchor = self._observe_anchor(purpose=purpose, local=local)
        if anchor["databaseId"] != local["databaseId"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-database-identity-mismatch"
            )
        if anchor["revision"] > local["revision"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-local-rollback-detected"
            )
        if anchor["revision"] < local["revision"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-anchor-publication-incomplete"
            )
        if anchor["publicationHash"] != local["publicationHash"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-divergence"
            )
        return local

    def _advance_anchor(
        self,
        *,
        purpose: str,
        before: Mapping[str, Any],
        after: Mapping[str, Any],
    ) -> None:
        if (
            after["databaseId"] != before["databaseId"]
            or int(after["revision"]) != int(before["revision"]) + 1
            or _HASH_RE.fullmatch(_text(after["publicationHash"])) is None
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-publication-transition-invalid"
            )
        authority = self.high_water_anchor
        if authority is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-authority-unavailable"
            )
        request = {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "action": "ADVANCE",
            "purpose": purpose,
            "scope": GLOBAL_SCOPE,
            "databaseId": _text(before["databaseId"]),
            "expectedRevision": int(before["revision"]),
            "expectedPublicationHash": _text(before["publicationHash"]),
            "newRevision": int(after["revision"]),
            "newPublicationHash": _text(after["publicationHash"]),
        }
        response = authority(request)
        if not isinstance(response, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-response-invalid"
            )
        anchored = self._validate_anchor_response(response)
        if (
            anchored["databaseId"] != after["databaseId"]
            or anchored["revision"] != after["revision"]
            or anchored["publicationHash"] != after["publicationHash"]
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-high-water-advance-unconfirmed"
            )

    def repair_pending_publication(self) -> dict[str, Any]:
        """Publish exactly one locally committed event after an anchor outage."""

        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            local = self._local_high_water(conn)
            latest = conn.execute(
                """
                SELECT previous_hash FROM crypto_first_live_events
                WHERE scope_key=? ORDER BY revision DESC, event_id DESC LIMIT 1
                """,
                (GLOBAL_SCOPE,),
            ).fetchone()
        finally:
            conn.close()
        anchor = self._observe_anchor(
            purpose="REPAIR_PENDING_PUBLICATION", local=local
        )
        if anchor["databaseId"] != local["databaseId"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-database-identity-mismatch"
            )
        if anchor["revision"] > local["revision"]:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-local-rollback-detected"
            )
        if anchor["revision"] == local["revision"]:
            if anchor["publicationHash"] != local["publicationHash"]:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-high-water-divergence"
                )
            return self.status()
        if (
            anchor["revision"] + 1 != local["revision"]
            or latest is None
            or _text(latest["previous_hash"])
            != anchor["publicationHash"]
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-publication-gap-not-repairable"
            )
        self._advance_anchor(
            purpose="REPAIR_PENDING_PUBLICATION",
            before=anchor,
            after=local,
        )
        return self.status()

    def _verify_empty_store_authority(
        self,
        conn: sqlite3.Connection,
        *,
        purpose: str,
    ) -> None:
        control_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM crypto_first_live_control"
            ).fetchone()[0]
        )
        event_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM crypto_first_live_events"
            ).fetchone()[0]
        )
        if control_count or event_count:
            return
        request = {
            "schemaVersion": SCHEMA_VERSION,
            "purpose": purpose,
            "databaseExists": self.path.is_file(),
            "databaseSize": (
                self.path.stat().st_size if self.path.is_file() else 0
            ),
            "emptyCoordinator": True,
            "requiredCrossAudit": (
                "UPBIT_AND_BINANCE_NONTERMINAL_POINTERS_ABSENT"
            ),
        }
        validator = self.installation_validator
        if validator is None or validator(request) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-empty-store-authority-unverified"
            )

    def _now(self) -> float:
        value = float(self.clock())
        if not math.isfinite(value) or value <= 0:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-clock-invalid"
            )
        return value

    def _monotonic_now(self) -> float:
        value = float(self.monotonic_clock())
        if not math.isfinite(value) or value < 0:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-monotonic-clock-invalid"
            )
        return value

    @staticmethod
    def _lease_seconds(value: float) -> float:
        seconds = float(value)
        if (
            not math.isfinite(seconds)
            or seconds <= 0
            or seconds > MAX_OWNER_LEASE_SECONDS
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-lease-out-of-range"
            )
        return seconds

    @staticmethod
    def _row(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM crypto_first_live_control WHERE scope_key=?",
            (GLOBAL_SCOPE,),
        ).fetchone()

    def _verified_row_snapshot(
        self,
        *,
        purpose: str,
    ) -> sqlite3.Row | None:
        self._require_anchor_exact(purpose=purpose + "_ANCHOR")
        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            self._verify_empty_store_authority(conn, purpose=purpose)
            return self._row(conn)
        finally:
            conn.close()

    @staticmethod
    def _assert_owner(
        row: sqlite3.Row,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        now: float,
        monotonic_now: float | None = None,
        require_live_lease: bool = True,
    ) -> None:
        if (
            _text(row["run_id"]) != run_id
            or int(row["owner_epoch"]) != int(owner_epoch)
            or not secrets.compare_digest(
                _text(row["owner_token_hash"]), _token_hash(owner_token)
            )
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-changed"
            )
        if now < float(row["updated_epoch"]):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-clock-anomaly"
            )
        if require_live_lease and now >= float(
            row["owner_lease_expires_epoch"]
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-lease-expired"
            )
        if (
            monotonic_now is not None
            and _text(row["monotonic_boot_id"])
            == _text(json.loads(_text(row["owner_identity_json"]))["bootId"])
            and monotonic_now < float(row["updated_monotonic"])
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-monotonic-clock-anomaly"
            )

    @staticmethod
    def _normalize_owner_identity(
        value: Mapping[str, Any],
        *,
        lane: str,
        account_fingerprint: str,
    ) -> tuple[dict[str, Any], str, str]:
        identity = dict(value)
        if set(identity) != {
            "pid",
            "processStartEpoch",
            "bootId",
            "applicationLeaseEpoch",
            "accountLeaseScope",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-identity-fields-not-exact"
            )
        try:
            pid = int(identity["pid"])
            process_start = float(identity["processStartEpoch"])
            application_lease = float(identity["applicationLeaseEpoch"])
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-identity-invalid"
            ) from exc
        boot_id = _text(identity["bootId"])
        account_lease_scope = _text(identity["accountLeaseScope"])
        expected_scope = _account_lease_scope(lane, account_fingerprint)
        if (
            pid <= 0
            or not math.isfinite(process_start)
            or process_start <= 0
            or not math.isfinite(application_lease)
            or application_lease <= 0
            or _ID_RE.fullmatch(boot_id) is None
            or account_lease_scope != expected_scope
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-identity-invalid"
            )
        normalized = {
            "pid": pid,
            "processStartEpoch": process_start,
            "bootId": boot_id,
            "applicationLeaseEpoch": application_lease,
            "accountLeaseScope": account_lease_scope,
        }
        serialized = _canonical(normalized)
        return (
            normalized,
            serialized,
            hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        )

    def _require_owner_identity_authority(
        self,
        *,
        purpose: str,
        lane: str,
        account_fingerprint: str,
        identity: Mapping[str, Any],
        identity_hash: str,
        run_id: str = "",
        owner_epoch: int = 0,
        coordinator_revision: int = 0,
    ) -> None:
        request = {
            "schemaVersion": "crypto-first-live-owner-identity/v1",
            "purpose": purpose,
            "scope": GLOBAL_SCOPE,
            "runId": run_id,
            "lane": lane,
            "accountFingerprint": account_fingerprint,
            "ownerIdentity": dict(identity),
            "ownerIdentityHash": identity_hash,
            "ownerEpoch": int(owner_epoch),
            "coordinatorRevision": int(coordinator_revision),
        }
        verifier = self.owner_identity_verifier
        if verifier is None or verifier(request) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-identity-unverified"
            )

    def _public(
        self,
        row: sqlite3.Row | None,
        *,
        now: float | None = None,
    ) -> dict[str, Any]:
        if row is None:
            return {
                "schemaVersion": SCHEMA_VERSION,
                "scope": GLOBAL_SCOPE,
                "phase": "IDLE",
                "activationReleased": CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED,
                "coordinatedRollbackProtectionReleased": (
                    CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
                ),
                "hardStopEpoch": 0.0,
                "entryAuthorityOpen": False,
                "networkOrderPostAllowed": False,
            }
        phase = _text(row["phase"])
        observed_now = self._now() if now is None else float(now)
        observed_monotonic = self._monotonic_now()
        lease_active = bool(
            phase
            in {
                "PREPARING",
                "APPROVED_INERT",
                "ACTIVATION_PREPARING",
                "ACTIVE",
                "CLEANUP_ONLY",
            }
            and observed_now >= float(row["updated_epoch"])
            and observed_now < float(row["owner_lease_expires_epoch"])
        )
        hard_stop_active = bool(
            phase != "ACTIVE"
            or (
                float(row["hard_stop_epoch"]) > 0
                and observed_now < float(row["hard_stop_epoch"])
                and float(row["hard_stop_monotonic"]) > 0
                and observed_monotonic
                < float(row["hard_stop_monotonic"])
            )
        )
        return {
            "schemaVersion": _text(row["schema_version"]),
            "scope": _text(row["scope_key"]),
            "phase": phase,
            "runId": _text(row["run_id"]),
            "lane": _text(row["lane"]),
            "sessionId": _text(row["session_id"]),
            "permitId": _text(row["permit_id"]),
            "accountFingerprint": _text(row["account_fingerprint"]),
            "baselineHash": _text(row["baseline_hash"]),
            "codeHash": _text(row["code_hash"]),
            "approvalId": _text(row["approval_id"]),
            "permitHash": _text(row["permit_hash"]),
            "ownerIdentity": json.loads(_text(row["owner_identity_json"])),
            "ownerIdentityHash": _text(row["owner_identity_hash"]),
            "ownerEpoch": int(row["owner_epoch"]),
            "ownerLeaseExpiresEpoch": float(
                row["owner_lease_expires_epoch"]
            ),
            "hardStopEpoch": float(row["hard_stop_epoch"]),
            "hardStopMonotonic": float(row["hard_stop_monotonic"]),
            "updatedMonotonic": float(row["updated_monotonic"]),
            "monotonicBootId": _text(row["monotonic_boot_id"]),
            "reservationEvidenceHash": _text(
                row["reservation_evidence_hash"]
            ),
            "reservationReceiptHash": _text(
                row["reservation_receipt_hash"]
            ),
            "finalApprovalHash": _text(row["final_approval_hash"]),
            "finalApprovalReceiptHash": _text(
                row["final_approval_receipt_hash"]
            ),
            "terminalEvidenceHash": _text(row["terminal_evidence_hash"]),
            "revision": int(row["revision"]),
            "detail": _text(row["detail"]),
            "ownerLeaseActive": lease_active,
            "clockAnomaly": observed_now < float(row["updated_epoch"]),
            "activationReleased": CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED,
            "coordinatedRollbackProtectionReleased": (
                CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
            ),
            "entryAuthorityOpen": bool(
                CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED
                and CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
                and phase == "ACTIVE"
                and lease_active
                and hard_stop_active
                and observed_monotonic >= float(row["updated_monotonic"])
            ),
            # The coordinator never grants broker transport authority by
            # itself; a separate final approval boundary must still agree.
            "networkOrderPostAllowed": False,
        }

    def _append_event(
        self,
        conn: sqlite3.Connection,
        *,
        run_id: str,
        event_type: str,
        revision: int,
        occurred_epoch: float,
        payload: Mapping[str, Any],
    ) -> str:
        previous = conn.execute(
            """
            SELECT content_hash FROM crypto_first_live_events
            WHERE scope_key=? ORDER BY revision DESC, event_id DESC LIMIT 1
            """,
            (GLOBAL_SCOPE,),
        ).fetchone()
        previous_hash = _text(previous[0]) if previous is not None else ""
        control = self._row(conn)
        if control is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-control-missing-before-event"
            )
        content = {
            "schemaVersion": SCHEMA_VERSION,
            "scope": GLOBAL_SCOPE,
            "runId": run_id,
            "eventType": event_type,
            "occurredEpoch": occurred_epoch,
            "revision": int(revision),
            "previousHash": previous_hash,
            "controlHash": self._control_hash(control),
            **dict(payload),
        }
        content_json = _canonical(content)
        content_hash = hashlib.sha256(content_json.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO crypto_first_live_events(
                event_id, scope_key, run_id, event_type, occurred_epoch,
                revision, previous_hash, content_json, content_hash
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                "crypto-first-live-event-" + secrets.token_hex(18),
                GLOBAL_SCOPE,
                run_id,
                event_type,
                occurred_epoch,
                int(revision),
                previous_hash,
                content_json,
                content_hash,
            ),
        )
        return content_hash

    def begin_reservation(
        self,
        *,
        lane: str,
        session_id: str,
        permit_id: str,
        account_fingerprint: str,
        baseline_hash: str,
        code_hash: str,
        approval_id: str,
        permit_hash: str,
        owner_identity: Mapping[str, Any],
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        preflight = self.status()
        if preflight.get("phase") not in {"IDLE", "FINALIZED"}:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-global-owner-active"
            )
        normalized_lane = _text(lane).upper()
        if normalized_lane not in LANES:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-lane-not-allowed"
            )
        session = _exact_id(session_id, "session-id")
        permit_identifier = _exact_id(permit_id, "permit-id")
        account = _exact_hash(account_fingerprint, "account-fingerprint")
        baseline = _exact_hash(baseline_hash, "baseline-hash")
        code = _exact_hash(code_hash, "code-hash")
        approval = _exact_id(approval_id, "approval-id")
        permit = _exact_hash(permit_hash, "permit-hash")
        identity, identity_json, identity_hash = self._normalize_owner_identity(
            owner_identity,
            lane=normalized_lane,
            account_fingerprint=account,
        )
        self._require_owner_identity_authority(
            purpose="BEGIN_RESERVATION",
            lane=normalized_lane,
            account_fingerprint=account,
            identity=identity,
            identity_hash=identity_hash,
        )
        lease = self._lease_seconds(lease_seconds)
        run_id = "crypto-first-live-run-" + secrets.token_hex(18)
        owner_token = secrets.token_urlsafe(48)
        publication_hash = ""
        with self._write(purpose="BEGIN_RESERVATION") as conn:
            previous = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if previous is not None and commit_now < float(
                previous["updated_epoch"]
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-clock-anomaly"
                )
            if previous is not None and _text(previous["phase"]) != "FINALIZED":
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-global-owner-active"
                )
            owner_epoch = (
                int(previous["owner_epoch"]) + 1
                if previous is not None
                else 1
            )
            revision = (
                int(previous["revision"]) + 1
                if previous is not None
                else 1
            )
            conn.execute(
                """
                INSERT INTO crypto_first_live_control(
                    scope_key, schema_version, phase, run_id, lane,
                    session_id, permit_id,
                    account_fingerprint, baseline_hash, code_hash,
                    approval_id, permit_hash, reservation_evidence_hash,
                    reservation_receipt_hash, final_approval_hash,
                    final_approval_receipt_hash, owner_identity_json,
                    owner_identity_hash, owner_token_hash, owner_epoch,
                    owner_lease_expires_epoch, hard_stop_epoch,
                    hard_stop_monotonic, updated_monotonic,
                    monotonic_boot_id,
                    terminal_evidence_hash,
                    created_epoch, updated_epoch, revision, detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    schema_version=excluded.schema_version,
                    phase=excluded.phase, run_id=excluded.run_id,
                    lane=excluded.lane,
                    session_id=excluded.session_id,
                    permit_id=excluded.permit_id,
                    account_fingerprint=excluded.account_fingerprint,
                    baseline_hash=excluded.baseline_hash,
                    code_hash=excluded.code_hash,
                    approval_id=excluded.approval_id,
                    permit_hash=excluded.permit_hash,
                    reservation_evidence_hash='',
                    reservation_receipt_hash='',
                    final_approval_hash='',
                    final_approval_receipt_hash='',
                    owner_identity_json=excluded.owner_identity_json,
                    owner_identity_hash=excluded.owner_identity_hash,
                    owner_token_hash=excluded.owner_token_hash,
                    owner_epoch=excluded.owner_epoch,
                    owner_lease_expires_epoch=excluded.owner_lease_expires_epoch,
                    hard_stop_epoch=0,
                    hard_stop_monotonic=0,
                    updated_monotonic=excluded.updated_monotonic,
                    monotonic_boot_id=excluded.monotonic_boot_id,
                    terminal_evidence_hash='',
                    created_epoch=excluded.created_epoch,
                    updated_epoch=excluded.updated_epoch,
                    revision=excluded.revision,
                    detail=excluded.detail
                """,
                (
                    GLOBAL_SCOPE,
                    SCHEMA_VERSION,
                    "PREPARING",
                    run_id,
                    normalized_lane,
                    session,
                    permit_identifier,
                    account,
                    baseline,
                    code,
                    approval,
                    permit,
                    "",
                    "",
                    "",
                    "",
                    identity_json,
                    identity_hash,
                    _token_hash(owner_token),
                    owner_epoch,
                    commit_now + lease,
                    0.0,
                    0.0,
                    commit_monotonic,
                    _text(identity["bootId"]),
                    "",
                    commit_now,
                    commit_now,
                    revision,
                    "durable reservation claim; evidence not yet sealed",
                ),
            )
            publication_hash = self._append_event(
                conn,
                run_id=run_id,
                event_type="RESERVATION_PREPARING_CLAIMED",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "lane": normalized_lane,
                    "sessionId": session,
                    "permitId": permit_identifier,
                    "accountFingerprint": account,
                    "baselineHash": baseline,
                    "codeHash": code,
                    "approvalId": approval,
                    "permitHash": permit,
                    "ownerIdentityHash": identity_hash,
                    "ownerEpoch": owner_epoch,
                },
            )
            row = self._row(conn)
        return {
            **self._public(row, now=self._now()),
            "ownerToken": owner_token,
            "preparingPublicationHash": publication_hash,
        }

    @staticmethod
    def _validate_reservation_receipt(
        value: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        now: float,
        claim_epoch: float,
    ) -> dict[str, Any]:
        receipt = dict(value)
        exact_fields = {
            "schemaVersion",
            "scope",
            "runId",
            "lane",
            "sessionId",
            "permitId",
            "accountFingerprint",
            "baselineHash",
            "codeHash",
            "approvalId",
            "permitHash",
            "ownerIdentityHash",
            "coordinatorRevision",
            "publicationHash",
            "presentedHash",
            "evidenceId",
            "observedEpoch",
            "expiresEpoch",
            "verified",
            "durable",
            "restartVerifiable",
        }
        if set(receipt) != exact_fields:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-receipt-fields-not-exact"
            )
        bound_fields = (
            "scope",
            "runId",
            "lane",
            "accountFingerprint",
            "baselineHash",
            "codeHash",
            "approvalId",
            "permitHash",
            "ownerIdentityHash",
            "coordinatorRevision",
            "publicationHash",
            "presentedHash",
        )
        try:
            observed = float(receipt["observedEpoch"])
            expires = float(receipt["expiresEpoch"])
            revision = int(receipt["coordinatorRevision"])
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-receipt-invalid"
            ) from exc
        if (
            _text(receipt["schemaVersion"])
            != "crypto-first-live-reservation-receipt/v1"
            or any(_text(receipt[key]) != _text(request[key]) for key in bound_fields if key != "coordinatorRevision")
            or revision != int(request["coordinatorRevision"])
            or _ID_RE.fullmatch(_text(receipt["evidenceId"])) is None
            or not math.isfinite(observed)
            or not math.isfinite(expires)
            or observed < claim_epoch
            or observed > now
            or expires <= now
            or expires - observed > MAX_RESERVATION_EVIDENCE_AGE_SECONDS
            or receipt["verified"] is not True
            or receipt["durable"] is not True
            or receipt["restartVerifiable"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-receipt-invalid"
            )
        return receipt

    def seal_reservation(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        reservation_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        run = _exact_id(run_id, "run-id")
        token = _text(owner_token)
        observed_at = self._now()
        observed_monotonic = self._monotonic_now()
        local = self._require_anchor_exact(purpose="SEAL_RESERVATION_READ")
        snapshot = self._verified_row_snapshot(purpose="SEAL_RESERVATION_READ")
        if snapshot is None or _text(snapshot["phase"]) != "PREPARING":
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-not-preparing"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=token,
            owner_epoch=owner_epoch,
            now=observed_at,
            monotonic_now=observed_monotonic,
        )
        if int(local["revision"]) != int(snapshot["revision"]):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-publication-changed"
            )
        presented = dict(reservation_evidence)
        request = {
            "schemaVersion": "crypto-first-live-reservation-evidence/v2",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot["lane"]),
            "sessionId": _text(snapshot["session_id"]),
            "permitId": _text(snapshot["permit_id"]),
            "accountFingerprint": _text(snapshot["account_fingerprint"]),
            "baselineHash": _text(snapshot["baseline_hash"]),
            "codeHash": _text(snapshot["code_hash"]),
            "approvalId": _text(snapshot["approval_id"]),
            "permitHash": _text(snapshot["permit_hash"]),
            "ownerIdentityHash": _text(snapshot["owner_identity_hash"]),
            "coordinatorRevision": int(snapshot["revision"]),
            "publicationHash": _text(local["publicationHash"]),
            "presentedHash": _digest(presented),
            "presented": presented,
        }
        verifier = self.reservation_evidence_verifier
        if verifier is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-evidence-unverified"
            )
        response = verifier(request)
        if not isinstance(response, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-evidence-unverified"
            )
        validated = self._validate_reservation_receipt(
            response,
            request=request,
            now=self._now(),
            claim_epoch=float(snapshot["updated_epoch"]),
        )
        evidence_hash = _digest(request)
        receipt_hash = _digest(validated)
        with self._write(purpose="SEAL_RESERVATION") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if (
                row is None
                or _text(row["phase"]) != "PREPARING"
                or _text(row["run_id"]) != run
                or int(row["revision"]) != int(snapshot["revision"])
                or _text(row["owner_identity_hash"])
                != _text(snapshot["owner_identity_hash"])
                or commit_now >= float(validated["expiresEpoch"])
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-reservation-seal-cas-changed"
                )
            self._assert_owner(
                row,
                run_id=run,
                owner_token=token,
                owner_epoch=owner_epoch,
                now=commit_now,
                monotonic_now=commit_monotonic,
            )
            revision = int(row["revision"]) + 1
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase='APPROVED_INERT', reservation_evidence_hash=?,
                    reservation_receipt_hash=?, updated_epoch=?,
                    updated_monotonic=?, revision=?,
                    detail='fresh reservation evidence sealed; transport held'
                WHERE scope_key=? AND run_id=? AND phase='PREPARING'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    evidence_hash,
                    receipt_hash,
                    commit_now,
                    commit_monotonic,
                    revision,
                    GLOBAL_SCOPE,
                    run,
                    int(owner_epoch),
                    _text(snapshot["owner_identity_hash"]),
                    int(snapshot["revision"]),
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-reservation-seal-cas-changed"
                )
            self._append_event(
                conn,
                run_id=run,
                event_type="RESERVATION_EVIDENCE_SEALED",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "reservationEvidenceHash": evidence_hash,
                    "reservationReceiptHash": receipt_hash,
                    "preparingPublicationHash": _text(
                        local["publicationHash"]
                    ),
                },
            )
            result = self._row(conn)
        return {
            **self._public(result, now=self._now()),
            "ownerToken": token,
        }

    def reserve_inert(
        self,
        *,
        lane: str,
        session_id: str,
        permit_id: str,
        account_fingerprint: str,
        baseline_hash: str,
        code_hash: str,
        approval_id: str,
        permit_hash: str,
        reservation_evidence: Mapping[str, Any],
        owner_identity: Mapping[str, Any],
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        claim = self.begin_reservation(
            lane=lane,
            session_id=session_id,
            permit_id=permit_id,
            account_fingerprint=account_fingerprint,
            baseline_hash=baseline_hash,
            code_hash=code_hash,
            approval_id=approval_id,
            permit_hash=permit_hash,
            owner_identity=owner_identity,
            lease_seconds=lease_seconds,
        )
        return self.seal_reservation(
            run_id=str(claim["runId"]),
            owner_token=str(claim["ownerToken"]),
            owner_epoch=int(claim["ownerEpoch"]),
            reservation_evidence=reservation_evidence,
        )

    @staticmethod
    def _validate_final_approval_receipt(
        value: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        receipt = dict(value)
        bound_fields = {
            "scope",
            "runId",
            "lane",
            "sessionId",
            "permitId",
            "accountFingerprint",
            "baselineHash",
            "codeHash",
            "approvalId",
            "permitHash",
            "reservationEvidenceHash",
            "reservationReceiptHash",
            "finalApprovalHash",
            "ownerEpoch",
            "coordinatorRevision",
            "publicationHash",
        }
        if set(receipt) != bound_fields | {
            "schemaVersion",
            "approvalConsumptionId",
            "consumed",
            "oneUse",
            "durable",
            "restartVerifiable",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-final-approval-receipt-fields-not-exact"
            )
        if (
            _text(receipt["schemaVersion"])
            != "crypto-first-live-final-approval-receipt/v1"
            or any(
                (
                    int(receipt[key]) != int(request[key])
                    if key in {"ownerEpoch", "coordinatorRevision"}
                    else _text(receipt[key]) != _text(request[key])
                )
                for key in bound_fields
            )
            or _ID_RE.fullmatch(
                _text(receipt["approvalConsumptionId"])
            )
            is None
            or receipt["consumed"] is not True
            or receipt["oneUse"] is not True
            or receipt["durable"] is not True
            or receipt["restartVerifiable"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-final-approval-receipt-invalid"
            )
        return receipt

    def activate(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        final_approval: Mapping[str, Any],
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        if (
            not CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED
            or not CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-activation-not-released"
            )
        run = _exact_id(run_id, "run-id")
        token = _text(owner_token)
        if not token:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-token-required"
            )
        lease = self._lease_seconds(lease_seconds)
        observed_at = self._now()
        observed_monotonic = self._monotonic_now()
        observed_monotonic = self._monotonic_now()
        snapshot = self._verified_row_snapshot(purpose="ACTIVATE_READ")
        if snapshot is None or _text(snapshot["phase"]) not in {
            "APPROVED_INERT",
            "ACTIVATION_PREPARING",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-not-inert"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=token,
            owner_epoch=owner_epoch,
            now=observed_at,
            monotonic_now=observed_monotonic,
        )
        owner_identity = json.loads(_text(snapshot["owner_identity_json"]))
        try:
            self._require_owner_identity_authority(
                purpose="ACTIVATE",
                lane=_text(snapshot["lane"]),
                account_fingerprint=_text(snapshot["account_fingerprint"]),
                identity=owner_identity,
                identity_hash=_text(snapshot["owner_identity_hash"]),
                run_id=run,
                owner_epoch=int(snapshot["owner_epoch"]),
                coordinator_revision=int(snapshot["revision"]),
            )
        except Exception:
            self.mark_reconciliation_required(
                run_id=run,
                reason="owner identity lost before activation",
            )
            raise
        approval_presented = dict(final_approval)
        approval_hash = _digest(approval_presented)
        if _text(snapshot["phase"]) == "APPROVED_INERT":
            with self._write(purpose="CLAIM_ACTIVATION") as conn:
                row = self._row(conn)
                commit_now = self._now()
                commit_monotonic = self._monotonic_now()
                if (
                    row is None
                    or _text(row["phase"]) != "APPROVED_INERT"
                    or _text(row["run_id"]) != run
                    or int(row["revision"]) != int(snapshot["revision"])
                    or not _text(row["reservation_evidence_hash"])
                    or not _text(row["reservation_receipt_hash"])
                ):
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-activation-claim-cas-changed"
                    )
                self._assert_owner(
                    row,
                    run_id=run,
                    owner_token=token,
                    owner_epoch=owner_epoch,
                    now=commit_now,
                    monotonic_now=commit_monotonic,
                )
                revision = int(row["revision"]) + 1
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='ACTIVATION_PREPARING', final_approval_hash=?,
                        final_approval_receipt_hash='', updated_epoch=?,
                        updated_monotonic=?, revision=?,
                        detail='durable activation claim; one-use approval pending'
                    WHERE scope_key=? AND run_id=? AND phase='APPROVED_INERT'
                      AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                    """,
                    (
                        approval_hash,
                        commit_now,
                        commit_monotonic,
                        revision,
                        GLOBAL_SCOPE,
                        run,
                        int(owner_epoch),
                        _text(snapshot["owner_identity_hash"]),
                        int(snapshot["revision"]),
                    ),
                ).rowcount
                if updated != 1:
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-activation-claim-cas-changed"
                    )
                self._append_event(
                    conn,
                    run_id=run,
                    event_type="ACTIVATION_PREPARING_CLAIMED",
                    revision=revision,
                    occurred_epoch=commit_now,
                    payload={
                        "finalApprovalHash": approval_hash,
                        "reservationEvidenceHash": _text(
                            row["reservation_evidence_hash"]
                        ),
                        "reservationReceiptHash": _text(
                            row["reservation_receipt_hash"]
                        ),
                    },
                )
            snapshot = self._verified_row_snapshot(
                purpose="ACTIVATION_CLAIM_PUBLISHED"
            )
        if (
            snapshot is None
            or _text(snapshot["phase"]) != "ACTIVATION_PREPARING"
            or _text(snapshot["run_id"]) != run
            or not secrets.compare_digest(
                _text(snapshot["final_approval_hash"]), approval_hash
            )
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-activation-claim-changed"
            )
        local = self._require_anchor_exact(
            purpose="CONSUME_FINAL_APPROVAL_READ"
        )
        authoritative_approval = {
            "schemaVersion": "crypto-first-live-final-approval/v2",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot["lane"]),
            "sessionId": _text(snapshot["session_id"]),
            "permitId": _text(snapshot["permit_id"]),
            "accountFingerprint": _text(snapshot["account_fingerprint"]),
            "baselineHash": _text(snapshot["baseline_hash"]),
            "codeHash": _text(snapshot["code_hash"]),
            "approvalId": _text(snapshot["approval_id"]),
            "permitHash": _text(snapshot["permit_hash"]),
            "reservationEvidenceHash": _text(
                snapshot["reservation_evidence_hash"]
            ),
            "reservationReceiptHash": _text(
                snapshot["reservation_receipt_hash"]
            ),
            "finalApprovalHash": approval_hash,
            "ownerEpoch": int(snapshot["owner_epoch"]),
            "coordinatorRevision": int(snapshot["revision"]),
            "publicationHash": _text(local["publicationHash"]),
            "presented": approval_presented,
        }
        consumer = self.final_approval_consumer
        if consumer is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-final-approval-unverified"
            )
        consumed = consumer(authoritative_approval)
        if not isinstance(consumed, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-final-approval-unverified"
            )
        receipt = self._validate_final_approval_receipt(
            consumed, request=authoritative_approval
        )
        receipt_hash = _digest(receipt)
        with self._write(purpose="COMPLETE_ACTIVATION") as conn:
            row = self._row(conn)
            if (
                row is None
                or _text(row["phase"]) != "ACTIVATION_PREPARING"
                or int(row["revision"]) != int(snapshot["revision"])
                or _text(row["owner_identity_hash"])
                != _text(snapshot["owner_identity_hash"])
                or not secrets.compare_digest(
                    _text(row["final_approval_hash"]), approval_hash
                )
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-activation-cas-changed"
                )
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            self._assert_owner(
                row,
                run_id=run,
                owner_token=token,
                owner_epoch=owner_epoch,
                now=commit_now,
                monotonic_now=commit_monotonic,
            )
            revision = int(row["revision"]) + 1
            hard_stop_epoch = commit_now + EXACT_FIRST_LIVE_SECONDS
            hard_stop_monotonic = (
                commit_monotonic + EXACT_FIRST_LIVE_SECONDS
            )
            try:
                conn.execute(
                    """
                    INSERT INTO crypto_first_live_approval_consumptions(
                        approval_id, approval_consumption_id, run_id, lane,
                        session_id, permit_id, account_fingerprint,
                        permit_hash, claim_revision, publication_hash,
                        receipt_hash, consumed_epoch
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _text(row["approval_id"]),
                        _text(receipt["approvalConsumptionId"]),
                        run,
                        _text(row["lane"]),
                        _text(row["session_id"]),
                        _text(row["permit_id"]),
                        _text(row["account_fingerprint"]),
                        _text(row["permit_hash"]),
                        int(snapshot["revision"]),
                        _text(local["publicationHash"]),
                        receipt_hash,
                        commit_now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-final-approval-already-consumed"
                ) from exc
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase='ACTIVE', owner_lease_expires_epoch=?,
                    hard_stop_epoch=?, hard_stop_monotonic=?,
                    final_approval_receipt_hash=?, updated_epoch=?,
                    updated_monotonic=?, revision=?, detail=?
                WHERE scope_key=? AND run_id=?
                  AND phase='ACTIVATION_PREPARING'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                  AND final_approval_hash=?
                """,
                (
                    min(commit_now + lease, hard_stop_epoch),
                    hard_stop_epoch,
                    hard_stop_monotonic,
                    receipt_hash,
                    commit_now,
                    commit_monotonic,
                    revision,
                    "activated only after external final approval boundary",
                    GLOBAL_SCOPE,
                    run,
                    int(owner_epoch),
                    _text(snapshot["owner_identity_hash"]),
                    int(row["revision"]),
                    approval_hash,
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-activation-cas-changed"
                )
            self._append_event(
                conn,
                run_id=run,
                event_type="ACTIVATED",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "ownerEpoch": int(owner_epoch),
                    "hardStopEpoch": hard_stop_epoch,
                    "hardStopMonotonic": hard_stop_monotonic,
                    "exactRunSeconds": int(EXACT_FIRST_LIVE_SECONDS),
                    "finalApprovalHash": approval_hash,
                    "finalApprovalReceiptHash": receipt_hash,
                    "approvalId": _text(row["approval_id"]),
                    "approvalConsumptionId": _text(
                        receipt["approvalConsumptionId"]
                    ),
                    "lane": _text(row["lane"]),
                    "sessionId": _text(row["session_id"]),
                    "permitId": _text(row["permit_id"]),
                    "accountFingerprint": _text(
                        row["account_fingerprint"]
                    ),
                    "permitHash": _text(row["permit_hash"]),
                    "approvalClaimRevision": int(snapshot["revision"]),
                    "approvalPublicationHash": _text(
                        local["publicationHash"]
                    ),
                    "approvalConsumedEpoch": commit_now,
                },
            )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def heartbeat(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        run = _exact_id(run_id, "run-id")
        token = _text(owner_token)
        lease = self._lease_seconds(lease_seconds)
        observed_at = self._now()
        observed_monotonic = self._monotonic_now()
        snapshot = self._verified_row_snapshot(purpose="HEARTBEAT_READ")
        if snapshot is None or _text(snapshot["phase"]) not in {
            "ACTIVE",
            "CLEANUP_ONLY",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-heartbeat-phase-closed"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=token,
            owner_epoch=owner_epoch,
            now=observed_at,
            monotonic_now=observed_monotonic,
        )
        owner_identity = json.loads(_text(snapshot["owner_identity_json"]))
        try:
            self._require_owner_identity_authority(
                purpose="HEARTBEAT",
                lane=_text(snapshot["lane"]),
                account_fingerprint=_text(snapshot["account_fingerprint"]),
                identity=owner_identity,
                identity_hash=_text(snapshot["owner_identity_hash"]),
                run_id=run,
                owner_epoch=int(snapshot["owner_epoch"]),
                coordinator_revision=int(snapshot["revision"]),
            )
        except Exception:
            self.mark_reconciliation_required(
                run_id=run,
                reason="owner identity lost during active heartbeat",
            )
            raise
        with self._write(purpose="HEARTBEAT") as conn:
            row = self._row(conn)
            if (
                row is None
                or _text(row["phase"]) not in {"ACTIVE", "CLEANUP_ONLY"}
                or int(row["revision"]) != int(snapshot["revision"])
                or _text(row["owner_identity_hash"])
                != _text(snapshot["owner_identity_hash"])
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-heartbeat-cas-changed"
                )
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            self._assert_owner(
                row,
                run_id=run,
                owner_token=token,
                owner_epoch=owner_epoch,
                now=commit_now,
                monotonic_now=commit_monotonic,
            )
            revision = int(row["revision"]) + 1
            phase = _text(row["phase"])
            hard_stop_epoch = float(row["hard_stop_epoch"])
            hard_stop_monotonic = float(row["hard_stop_monotonic"])
            if phase == "ACTIVE" and (
                hard_stop_epoch <= 0
                or hard_stop_monotonic <= 0
                or commit_now >= hard_stop_epoch
                or commit_monotonic >= hard_stop_monotonic
            ):
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='CLEANUP_ONLY',
                        owner_lease_expires_epoch=?, updated_epoch=?,
                        updated_monotonic=?, revision=?,
                        detail='exact 7200 second dual-clock hard stop reached'
                    WHERE scope_key=? AND run_id=? AND phase='ACTIVE'
                      AND owner_epoch=? AND revision=?
                      AND owner_identity_hash=?
                    """,
                    (
                        commit_now + lease,
                        commit_now,
                        commit_monotonic,
                        revision,
                        GLOBAL_SCOPE,
                        run,
                        int(owner_epoch),
                        int(row["revision"]),
                        _text(snapshot["owner_identity_hash"]),
                    ),
                ).rowcount
                if updated != 1:
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-hard-stop-cas-changed"
                    )
                self._append_event(
                    conn,
                    run_id=run,
                    event_type="HARD_STOP_REACHED",
                    revision=revision,
                    occurred_epoch=commit_now,
                    payload={
                        "hardStopEpoch": hard_stop_epoch,
                        "hardStopMonotonic": hard_stop_monotonic,
                        "wallDeadlineReached": commit_now >= hard_stop_epoch,
                        "monotonicDeadlineReached": (
                            commit_monotonic >= hard_stop_monotonic
                        ),
                    },
                )
                result = self._row(conn)
                return self._public(result, now=commit_now)
            next_lease_expiry = (
                min(commit_now + lease, hard_stop_epoch)
                if phase == "ACTIVE"
                else commit_now + lease
            )
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET owner_lease_expires_epoch=?, updated_epoch=?,
                    updated_monotonic=?, revision=?
                WHERE scope_key=? AND run_id=? AND owner_epoch=? AND revision=?
                  AND owner_identity_hash=?
                """,
                (
                    next_lease_expiry,
                    commit_now,
                    commit_monotonic,
                    revision,
                    GLOBAL_SCOPE,
                    run,
                    int(owner_epoch),
                    int(row["revision"]),
                    _text(snapshot["owner_identity_hash"]),
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-heartbeat-cas-changed"
                )
            self._append_event(
                conn,
                run_id=run,
                event_type="HEARTBEAT",
                revision=revision,
                occurred_epoch=commit_now,
                payload={"ownerEpoch": int(owner_epoch)},
            )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def assert_dispatch_authority(
        self,
        *,
        purpose: str,
        lane: str,
        run_id: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        account_fingerprint: str,
        baseline_hash: str,
        code_hash: str,
        owner_token: str,
        owner_epoch: int,
        expected_revision: int,
        route_lock_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Project exact authority while the caller's common route lock is held.

        ``ENTRY_ORDER`` is only valid for ACTIVE before both deadlines.
        ``CLEANUP_MUTATION`` remains valid in ACTIVE/CLEANUP_ONLY, but conveys
        cancel/flatten cleanup authority only.  This method owns no transport.
        """

        normalized_purpose = _text(purpose).upper()
        if normalized_purpose not in {"ENTRY_ORDER", "CLEANUP_MUTATION"}:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-dispatch-purpose-invalid"
            )
        if (
            normalized_purpose == "ENTRY_ORDER"
            and (
                not CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED
                or not CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
            )
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-activation-not-released"
            )
        expected = {
            "lane": _text(lane).upper(),
            "runId": _exact_id(run_id, "run-id"),
            "sessionId": _exact_id(session_id, "session-id"),
            "permitId": _exact_id(permit_id, "permit-id"),
            "permitHash": _exact_hash(permit_hash, "permit-hash"),
            "accountFingerprint": _exact_hash(
                account_fingerprint, "account-fingerprint"
            ),
            "baselineHash": _exact_hash(baseline_hash, "baseline-hash"),
            "codeHash": _exact_hash(code_hash, "code-hash"),
        }
        if expected["lane"] not in LANES or int(expected_revision) <= 0:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-dispatch-binding-invalid"
            )
        local = self._require_anchor_exact(purpose="DISPATCH_AUTHORITY_READ")
        snapshot = self.status()
        if (
            int(snapshot.get("revision", -1)) != int(expected_revision)
            or any(snapshot.get(key) != value for key, value in expected.items())
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-dispatch-binding-changed"
            )
        owner_identity = dict(snapshot.get("ownerIdentity", {}))
        try:
            self._require_owner_identity_authority(
                purpose="DISPATCH_" + normalized_purpose,
                lane=expected["lane"],
                account_fingerprint=expected["accountFingerprint"],
                identity=owner_identity,
                identity_hash=_text(snapshot.get("ownerIdentityHash")),
                run_id=expected["runId"],
                owner_epoch=int(owner_epoch),
                coordinator_revision=int(expected_revision),
            )
        except Exception:
            self.mark_reconciliation_required(
                run_id=expected["runId"],
                reason="owner identity lost at dispatch boundary",
            )
            raise
        route_request = {
            "schemaVersion": "crypto-first-live-route-lock/v1",
            "scope": GLOBAL_SCOPE,
            "purpose": normalized_purpose,
            **expected,
            "ownerEpoch": int(owner_epoch),
            "coordinatorRevision": int(expected_revision),
            "publicationHash": _text(local["publicationHash"]),
            "routeLockEvidenceHash": _digest(dict(route_lock_evidence)),
            "presented": dict(route_lock_evidence),
        }
        verifier = self.route_lock_verifier
        if verifier is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-route-lock-unverified"
            )
        proof_value = verifier(route_request)
        if not isinstance(proof_value, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-route-lock-unverified"
            )
        proof = dict(proof_value)
        proof_bound = set(route_request) - {"schemaVersion", "presented"}
        if set(proof) != proof_bound | {
            "schemaVersion",
            "proofId",
            "held",
            "exclusive",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-route-lock-proof-fields-not-exact"
            )
        try:
            exact_proof = all(
                (
                    int(proof[key]) == int(route_request[key])
                    if key in {"ownerEpoch", "coordinatorRevision"}
                    else _text(proof[key]) == _text(route_request[key])
                )
                for key in proof_bound
            )
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-route-lock-proof-invalid"
            ) from exc
        if (
            _text(proof["schemaVersion"])
            != "crypto-first-live-route-lock-proof/v1"
            or not exact_proof
            or _ID_RE.fullmatch(_text(proof["proofId"])) is None
            or proof["held"] is not True
            or proof["exclusive"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-route-lock-proof-invalid"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._verify_schema(conn)
            self._verify_integrity(conn)
            current_high_water = self._local_high_water(conn)
            if current_high_water != local:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-dispatch-publication-changed"
                )
            row = self._row(conn)
            observed_epoch = self._now()
            observed_monotonic = self._monotonic_now()
            if (
                row is None
                or int(row["revision"]) != int(expected_revision)
                or any(
                    _text(row[column]) != expected[public]
                    for column, public in (
                        ("lane", "lane"),
                        ("run_id", "runId"),
                        ("session_id", "sessionId"),
                        ("permit_id", "permitId"),
                        ("permit_hash", "permitHash"),
                        ("account_fingerprint", "accountFingerprint"),
                        ("baseline_hash", "baselineHash"),
                        ("code_hash", "codeHash"),
                    )
                )
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-dispatch-binding-changed"
                )
            self._assert_owner(
                row,
                run_id=expected["runId"],
                owner_token=_text(owner_token),
                owner_epoch=owner_epoch,
                now=observed_epoch,
                monotonic_now=observed_monotonic,
            )
            public = self._public(row, now=observed_epoch)
            phase = _text(row["phase"])
            if normalized_purpose == "ENTRY_ORDER":
                if (
                    phase != "ACTIVE"
                    or public["entryAuthorityOpen"] is not True
                    or not _text(row["final_approval_receipt_hash"])
                ):
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-entry-authority-closed"
                    )
            elif phase not in {"ACTIVE", "CLEANUP_ONLY"}:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-authority-closed"
                )
            projection = {
                "schemaVersion": "crypto-first-live-dispatch-snapshot/v1",
                "scope": GLOBAL_SCOPE,
                "purpose": normalized_purpose,
                "lane": expected["lane"],
                "phase": phase,
                "runId": expected["runId"],
                "sessionId": expected["sessionId"],
                "permitId": expected["permitId"],
                "permitHash": expected["permitHash"],
                "accountFingerprint": expected["accountFingerprint"],
                "baselineHash": expected["baselineHash"],
                "codeHash": expected["codeHash"],
                "ownerEpoch": int(owner_epoch),
                "ownerLeaseActive": public["ownerLeaseActive"],
                "entryAuthorityOpen": public["entryAuthorityOpen"],
                "cleanupOnly": normalized_purpose == "CLEANUP_MUTATION",
                "hardStopEpoch": public["hardStopEpoch"],
                "hardStopMonotonic": public["hardStopMonotonic"],
                "revision": int(row["revision"]),
                "publicationHash": _text(local["publicationHash"]),
                "observedEpoch": observed_epoch,
                "observedMonotonic": observed_monotonic,
            }
            result = {
                **projection,
                "authorityHash": _digest(projection),
            }
            conn.execute("COMMIT")
            return result
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def revoke_entry(
        self,
        *,
        run_id: str,
        expected_revision: int,
        reason: str,
    ) -> dict[str, Any]:
        """Risk-reducing transition callable by STOP/Kill without a token."""

        run = _exact_id(run_id, "run-id")
        detail = _text(reason)
        if not detail:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-cleanup-reason-required"
            )
        expected = int(expected_revision)
        if expected <= 0:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-cleanup-revision-invalid"
            )
        with self._write(purpose="REVOKE_ENTRY") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if row is None or _text(row["run_id"]) != run:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-run-changed"
                )
            if int(row["revision"]) != expected:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-cas-changed"
                )
            phase = _text(row["phase"])
            if phase in {"FINALIZED", "RECONCILIATION_REQUIRED"}:
                return self._public(row, now=commit_now)
            if phase not in REVOCABLE_PHASES | {"CLEANUP_ONLY"}:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-phase-invalid"
                )
            if phase != "CLEANUP_ONLY":
                mutation_time = max(
                    commit_now, float(row["updated_epoch"])
                )
                mutation_monotonic = max(
                    commit_monotonic, float(row["updated_monotonic"])
                )
                revision = int(row["revision"]) + 1
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='CLEANUP_ONLY', updated_epoch=?,
                        updated_monotonic=?, revision=?, detail=?
                    WHERE scope_key=? AND run_id=? AND phase=?
                      AND owner_epoch=? AND owner_identity_hash=?
                      AND revision=?
                    """,
                    (
                        mutation_time,
                        mutation_monotonic,
                        revision,
                        detail[:240],
                        GLOBAL_SCOPE,
                        run,
                        phase,
                        int(row["owner_epoch"]),
                        _text(row["owner_identity_hash"]),
                        int(row["revision"]),
                    ),
                ).rowcount
                if updated != 1:
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-cleanup-cas-changed"
                    )
                self._append_event(
                    conn,
                    run_id=run,
                    event_type="ENTRY_REVOKED",
                    revision=revision,
                    occurred_epoch=mutation_time,
                    payload={
                        "reason": detail[:240],
                        "revokedPhase": phase,
                        "expectedRevision": expected,
                    },
                )
            result = self._row(conn)
        return self._public(result, now=self._now())

    @staticmethod
    def _validate_absence_receipt(
        value: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        now: float,
    ) -> dict[str, Any]:
        receipt = dict(value)
        bound = {
            "scope",
            "runId",
            "lane",
            "sessionId",
            "permitId",
            "accountFingerprint",
            "priorOwnerIdentityHash",
            "priorOwnerEpoch",
            "coordinatorRevision",
            "publicationHash",
        }
        if set(receipt) != bound | {
            "schemaVersion",
            "absenceProofId",
            "observedEpoch",
            "expiresEpoch",
            "absent",
            "durable",
            "restartVerifiable",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-absence-fields-not-exact"
            )
        try:
            observed = float(receipt["observedEpoch"])
            expires = float(receipt["expiresEpoch"])
            exact = all(
                (
                    int(receipt[key]) == int(request[key])
                    if key in {"priorOwnerEpoch", "coordinatorRevision"}
                    else _text(receipt[key]) == _text(request[key])
                )
                for key in bound
            )
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-absence-invalid"
            ) from exc
        if (
            _text(receipt["schemaVersion"])
            != "crypto-first-live-owner-absence-receipt/v1"
            or not exact
            or _ID_RE.fullmatch(_text(receipt["absenceProofId"])) is None
            or not math.isfinite(observed)
            or not math.isfinite(expires)
            or observed > now
            or expires <= now
            or expires - observed > MAX_RESERVATION_EVIDENCE_AGE_SECONDS
            or receipt["absent"] is not True
            or receipt["durable"] is not True
            or receipt["restartVerifiable"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-absence-invalid"
            )
        return receipt

    def takeover_expired_cleanup(
        self,
        *,
        run_id: str,
        expected_revision: int,
        owner_identity: Mapping[str, Any],
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        """Rotate only cleanup authority after the prior owner lease expires."""

        run = _exact_id(run_id, "run-id")
        lease = self._lease_seconds(lease_seconds)
        token = secrets.token_urlsafe(48)
        snapshot = self.status()
        if (
            snapshot.get("runId") != run
            or snapshot.get("phase") != "CLEANUP_ONLY"
            or int(snapshot.get("revision", -1)) != int(expected_revision)
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-cleanup-takeover-cas-changed"
            )
        if snapshot.get("ownerLeaseActive") is True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-owner-lease-still-active"
            )
        if snapshot.get("clockAnomaly") is True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-clock-anomaly"
            )
        identity, identity_json, identity_hash = self._normalize_owner_identity(
            owner_identity,
            lane=_text(snapshot.get("lane")),
            account_fingerprint=_text(snapshot.get("accountFingerprint")),
        )
        self._require_owner_identity_authority(
            purpose="TAKEOVER_CLEANUP_ONLY",
            lane=_text(snapshot.get("lane")),
            account_fingerprint=_text(snapshot.get("accountFingerprint")),
            identity=identity,
            identity_hash=identity_hash,
            run_id=run,
            owner_epoch=int(snapshot.get("ownerEpoch", 0)) + 1,
            coordinator_revision=int(expected_revision),
        )
        absence_reader = self.startup_owner_absent_reader
        local = self._require_anchor_exact(purpose="TAKEOVER_ABSENCE_READ")
        absence_request = {
            "schemaVersion": "crypto-first-live-owner-absence/v1",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot.get("lane")),
            "sessionId": _text(snapshot.get("sessionId")),
            "permitId": _text(snapshot.get("permitId")),
            "accountFingerprint": _text(
                snapshot.get("accountFingerprint")
            ),
            "priorOwnerIdentityHash": _text(
                snapshot.get("ownerIdentityHash")
            ),
            "priorOwnerEpoch": int(snapshot.get("ownerEpoch", 0)),
            "coordinatorRevision": int(expected_revision),
            "publicationHash": _text(local["publicationHash"]),
        }
        if absence_reader is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-prior-owner-absence-unproven"
            )
        absence_value = absence_reader(absence_request)
        if not isinstance(absence_value, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-prior-owner-absence-unproven"
            )
        absence_receipt = self._validate_absence_receipt(
            absence_value, request=absence_request, now=self._now()
        )
        absence_hash = _digest(absence_receipt)
        with self._write(purpose="TAKEOVER_CLEANUP") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if (
                row is None
                or _text(row["run_id"]) != run
                or _text(row["phase"]) != "CLEANUP_ONLY"
                or int(row["revision"]) != int(expected_revision)
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-takeover-cas-changed"
                )
            if commit_now < float(row["updated_epoch"]):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-clock-anomaly"
                )
            if commit_monotonic < float(row["updated_monotonic"]):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-monotonic-clock-anomaly"
                )
            if commit_now < float(row["owner_lease_expires_epoch"]):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-owner-lease-still-active"
                )
            owner_epoch = int(row["owner_epoch"]) + 1
            revision = int(row["revision"]) + 1
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET owner_identity_json=?, owner_identity_hash=?,
                    owner_token_hash=?, owner_epoch=?,
                    owner_lease_expires_epoch=?, updated_epoch=?,
                    updated_monotonic=?, monotonic_boot_id=?, revision=?,
                    detail='expired owner rotated for cleanup only'
                WHERE scope_key=? AND run_id=? AND phase='CLEANUP_ONLY'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    identity_json,
                    identity_hash,
                    _token_hash(token),
                    owner_epoch,
                    commit_now + lease,
                    commit_now,
                    commit_monotonic,
                    _text(identity["bootId"]),
                    revision,
                    GLOBAL_SCOPE,
                    run,
                    int(row["owner_epoch"]),
                    _text(row["owner_identity_hash"]),
                    int(expected_revision),
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-takeover-cas-changed"
                )
            self._append_event(
                conn,
                run_id=run,
                event_type="CLEANUP_OWNER_ROTATED",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "ownerEpoch": owner_epoch,
                    "ownerIdentityHash": identity_hash,
                    "priorOwnerAbsenceReceiptHash": absence_hash,
                    "expectedRevision": int(expected_revision),
                },
            )
            result = self._row(conn)
        return {
            **self._public(result, now=self._now()),
            "ownerToken": token,
        }

    @staticmethod
    def _validate_terminal_receipt(
        value: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        receipt = dict(value)
        bound = {
            "scope",
            "runId",
            "lane",
            "sessionId",
            "permitId",
            "accountFingerprint",
            "baselineHash",
            "codeHash",
            "approvalId",
            "permitHash",
            "ownerEpoch",
            "coordinatorRevision",
            "publicationHash",
            "terminalEvidenceHash",
        }
        if set(receipt) != bound | {
            "schemaVersion",
            "terminalEvidenceId",
            "verified",
            "durable",
            "restartVerifiable",
        }:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-receipt-fields-not-exact"
            )
        try:
            exact = all(
                (
                    int(receipt[key]) == int(request[key])
                    if key in {"ownerEpoch", "coordinatorRevision"}
                    else _text(receipt[key]) == _text(request[key])
                )
                for key in bound
            )
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-receipt-invalid"
            ) from exc
        if (
            _text(receipt["schemaVersion"])
            != "crypto-first-live-terminal-receipt/v1"
            or not exact
            or _ID_RE.fullmatch(_text(receipt["terminalEvidenceId"])) is None
            or receipt["verified"] is not True
            or receipt["durable"] is not True
            or receipt["restartVerifiable"] is not True
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-receipt-invalid"
            )
        return receipt

    def finalize(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        expected_revision: int,
        terminal_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        run = _exact_id(run_id, "run-id")
        evidence = dict(terminal_evidence)
        observed_at = self._now()
        observed_monotonic = self._monotonic_now()
        snapshot = self._verified_row_snapshot(purpose="FINALIZE_READ")
        if (
            snapshot is None
            or _text(snapshot["phase"]) != "CLEANUP_ONLY"
            or int(snapshot["revision"]) != int(expected_revision)
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-finalize-requires-cleanup"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=_text(owner_token),
            owner_epoch=owner_epoch,
            now=observed_at,
            monotonic_now=observed_monotonic,
        )
        owner_identity = json.loads(_text(snapshot["owner_identity_json"]))
        self._require_owner_identity_authority(
            purpose="FINALIZE",
            lane=_text(snapshot["lane"]),
            account_fingerprint=_text(snapshot["account_fingerprint"]),
            identity=owner_identity,
            identity_hash=_text(snapshot["owner_identity_hash"]),
            run_id=run,
            owner_epoch=int(snapshot["owner_epoch"]),
            coordinator_revision=int(snapshot["revision"]),
        )
        local = self._require_anchor_exact(purpose="FINALIZE_EVIDENCE_READ")
        authoritative_evidence = {
            "schemaVersion": "crypto-first-live-terminal-evidence/v2",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot["lane"]),
            "sessionId": _text(snapshot["session_id"]),
            "permitId": _text(snapshot["permit_id"]),
            "accountFingerprint": _text(snapshot["account_fingerprint"]),
            "baselineHash": _text(snapshot["baseline_hash"]),
            "codeHash": _text(snapshot["code_hash"]),
            "approvalId": _text(snapshot["approval_id"]),
            "permitHash": _text(snapshot["permit_hash"]),
            "ownerEpoch": int(snapshot["owner_epoch"]),
            "coordinatorRevision": int(snapshot["revision"]),
            "publicationHash": _text(local["publicationHash"]),
            "terminalEvidenceHash": _digest(evidence),
            "presented": evidence,
        }
        verifier = self.terminal_evidence_verifier
        if verifier is None:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-evidence-unverified"
            )
        verified = verifier(authoritative_evidence)
        if not isinstance(verified, Mapping):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-evidence-unverified"
            )
        receipt = self._validate_terminal_receipt(
            verified, request=authoritative_evidence
        )
        terminal_hash = _digest(
            {
                "authoritativeEvidenceHash": _digest(
                    authoritative_evidence
                ),
                "receiptHash": _digest(receipt),
            }
        )
        with self._write(purpose="FINALIZE") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if (
                row is None
                or _text(row["phase"]) != "CLEANUP_ONLY"
                or int(row["revision"]) != int(snapshot["revision"])
                or _text(row["owner_identity_hash"])
                != _text(snapshot["owner_identity_hash"])
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-finalize-cas-changed"
                )
            commit_now = self._now()
            self._assert_owner(
                row,
                run_id=run,
                owner_token=_text(owner_token),
                owner_epoch=owner_epoch,
                now=commit_now,
                monotonic_now=commit_monotonic,
            )
            revision = int(row["revision"]) + 1
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase='FINALIZED', owner_token_hash='',
                    owner_lease_expires_epoch=0, hard_stop_epoch=0,
                    hard_stop_monotonic=0,
                    terminal_evidence_hash=?, updated_epoch=?,
                    updated_monotonic=?, revision=?,
                    detail='terminal evidence sealed; global owner released'
                WHERE scope_key=? AND run_id=? AND phase='CLEANUP_ONLY'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    terminal_hash,
                    commit_now,
                    commit_monotonic,
                    revision,
                    GLOBAL_SCOPE,
                    run,
                    int(owner_epoch),
                    _text(snapshot["owner_identity_hash"]),
                    int(row["revision"]),
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-finalize-cas-changed"
                )
            self._append_event(
                conn,
                run_id=run,
                event_type="FINALIZED",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "terminalEvidenceHash": terminal_hash,
                    "terminalEvidenceReceiptHash": _digest(receipt),
                    "expectedRevision": int(expected_revision),
                },
            )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def mark_reconciliation_required(
        self, *, run_id: str, reason: str
    ) -> dict[str, Any]:
        run = _exact_id(run_id, "run-id")
        detail = _text(reason)
        if not detail:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reconciliation-reason-required"
            )
        with self._write(purpose="MARK_RECONCILIATION_REQUIRED") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if row is None or _text(row["run_id"]) != run:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-run-changed"
                )
            if _text(row["phase"]) == "FINALIZED":
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-finalized-cannot-reopen"
                )
            if _text(row["phase"]) != "RECONCILIATION_REQUIRED":
                mutation_time = max(
                    commit_now, float(row["updated_epoch"])
                )
                mutation_monotonic = max(
                    commit_monotonic, float(row["updated_monotonic"])
                )
                revision = int(row["revision"]) + 1
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='RECONCILIATION_REQUIRED', owner_token_hash='',
                        owner_lease_expires_epoch=0, updated_epoch=?,
                        updated_monotonic=?, revision=?, detail=?
                    WHERE scope_key=? AND run_id=? AND revision=?
                    """,
                    (
                        mutation_time,
                        mutation_monotonic,
                        revision,
                        detail[:240],
                        GLOBAL_SCOPE,
                        run,
                        int(row["revision"]),
                    ),
                ).rowcount
                if updated != 1:
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-reconciliation-cas-changed"
                    )
                self._append_event(
                    conn,
                    run_id=run,
                    event_type="RECONCILIATION_REQUIRED",
                    revision=revision,
                    occurred_epoch=mutation_time,
                    payload={"reason": detail[:240]},
                )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def audit_startup(self) -> dict[str, Any]:
        """Fail an expired/anomalous owner closed before any dispatch reader.

        A positive process-absence proof demotes an expired owner to cleanup;
        without that proof the global run becomes reconciliation-required and
        no authority token survives.
        """

        observed_at = self._now()
        observed_monotonic = self._monotonic_now()
        snapshot = self.status()
        if snapshot.get("phase") == "IDLE":
            return snapshot
        phase = _text(snapshot.get("phase"))
        if phase not in REVOCABLE_PHASES | {"CLEANUP_ONLY"}:
            return snapshot
        clock_anomaly = bool(
            snapshot.get("clockAnomaly") is True
            or observed_monotonic
            < float(snapshot.get("updatedMonotonic", 0))
        )
        hard_stop_reached = bool(
            phase == "ACTIVE"
            and (
                float(snapshot.get("hardStopEpoch", 0)) <= 0
                or float(snapshot.get("hardStopMonotonic", 0)) <= 0
                or observed_at >= float(snapshot.get("hardStopEpoch", 0))
                or observed_monotonic
                >= float(snapshot.get("hardStopMonotonic", 0))
            )
        )
        expired = hard_stop_reached or observed_at >= float(
            snapshot.get("ownerLeaseExpiresEpoch", 0)
        )
        if not clock_anomaly and not expired:
            return snapshot
        absent = False
        absence_receipt_hash = ""
        if not clock_anomaly and self.startup_owner_absent_reader is not None:
            local = self._require_anchor_exact(
                purpose="STARTUP_OWNER_ABSENCE_READ"
            )
            request = {
                "schemaVersion": "crypto-first-live-owner-absence/v1",
                "scope": GLOBAL_SCOPE,
                "runId": _text(snapshot.get("runId")),
                "lane": _text(snapshot.get("lane")),
                "sessionId": _text(snapshot.get("sessionId")),
                "permitId": _text(snapshot.get("permitId")),
                "accountFingerprint": _text(
                    snapshot.get("accountFingerprint")
                ),
                "priorOwnerIdentityHash": _text(
                    snapshot.get("ownerIdentityHash")
                ),
                "priorOwnerEpoch": int(snapshot.get("ownerEpoch", 0)),
                "coordinatorRevision": int(snapshot.get("revision", 0)),
                "publicationHash": _text(local["publicationHash"]),
            }
            value = self.startup_owner_absent_reader(request)
            if isinstance(value, Mapping):
                receipt = self._validate_absence_receipt(
                    value, request=request, now=self._now()
                )
                absent = True
                absence_receipt_hash = _digest(receipt)

        with self._write(purpose="AUDIT_STARTUP") as conn:
            row = self._row(conn)
            commit_now = self._now()
            commit_monotonic = self._monotonic_now()
            if (
                row is None
                or _text(row["run_id"]) != _text(snapshot.get("runId"))
                or int(row["revision"]) != int(snapshot.get("revision", -1))
                or _text(row["phase"])
                not in REVOCABLE_PHASES | {"CLEANUP_ONLY"}
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-startup-audit-cas-changed"
                )
            clock_anomaly = bool(
                commit_now < float(row["updated_epoch"])
                or commit_monotonic < float(row["updated_monotonic"])
            )
            hard_stop_reached = bool(
                _text(row["phase"]) == "ACTIVE"
                and (
                    float(row["hard_stop_epoch"]) <= 0
                    or float(row["hard_stop_monotonic"]) <= 0
                    or commit_now >= float(row["hard_stop_epoch"])
                    or commit_monotonic
                    >= float(row["hard_stop_monotonic"])
                )
            )
            expired = hard_stop_reached or commit_now >= float(
                row["owner_lease_expires_epoch"]
            )
            if not clock_anomaly and not expired:
                return self._public(row, now=commit_now)
            if clock_anomaly:
                absent = False
            next_phase = (
                "CLEANUP_ONLY"
                if absent or hard_stop_reached
                else "RECONCILIATION_REQUIRED"
            )
            revision = int(row["revision"]) + 1
            owner_token_hash = ""
            lease_expires = 0.0
            mutation_time = max(commit_now, float(row["updated_epoch"]))
            mutation_monotonic = max(
                commit_monotonic, float(row["updated_monotonic"])
            )
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                    SET phase=?, owner_token_hash=?, owner_lease_expires_epoch=?,
                    updated_epoch=?, updated_monotonic=?, revision=?, detail=?
                WHERE scope_key=? AND run_id=? AND phase=?
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    next_phase,
                    owner_token_hash,
                    lease_expires,
                    mutation_time,
                    mutation_monotonic,
                    revision,
                    (
                        "exact 7200 second hard stop reached; cleanup takeover required"
                        if hard_stop_reached
                        else (
                            "startup proved prior owner absent; cleanup takeover required"
                            if absent
                            else "startup owner/clock proof incomplete; reconciliation required"
                        )
                    ),
                    GLOBAL_SCOPE,
                    _text(row["run_id"]),
                    _text(row["phase"]),
                    int(row["owner_epoch"]),
                    _text(row["owner_identity_hash"]),
                    int(row["revision"]),
                ),
            ).rowcount
            if updated != 1:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-startup-audit-cas-changed"
                )
            self._append_event(
                conn,
                run_id=_text(row["run_id"]),
                event_type=(
                    "STARTUP_CLEANUP_REQUIRED"
                    if absent or hard_stop_reached
                    else "RECONCILIATION_REQUIRED"
                ),
                revision=revision,
                occurred_epoch=mutation_time,
                payload={
                    "priorOwnerAbsent": absent,
                    "clockAnomaly": clock_anomaly,
                    "leaseExpired": expired,
                    "hardStopReached": hard_stop_reached,
                    "monotonicClockAnomaly": (
                        commit_monotonic < float(row["updated_monotonic"])
                    ),
                    "priorOwnerAbsenceReceiptHash": absence_receipt_hash,
                },
            )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def status(self) -> dict[str, Any]:
        self._require_anchor_exact(purpose="STATUS")
        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            self._verify_empty_store_authority(conn, purpose="STATUS")
            return self._public(self._row(conn), now=self._now())
        finally:
            conn.close()

    def events(self) -> list[dict[str, Any]]:
        self._require_anchor_exact(purpose="EVENT_READ")
        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            self._verify_empty_store_authority(conn, purpose="EVENT_READ")
            rows = conn.execute(
                """
                SELECT event_id, run_id, event_type, occurred_epoch,
                       revision, previous_hash, content_json, content_hash
                FROM crypto_first_live_events
                WHERE scope_key=? ORDER BY revision, event_id
                """,
                (GLOBAL_SCOPE,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            previous_hash = ""
            for row in rows:
                content_json = _text(row["content_json"])
                content_hash = hashlib.sha256(
                    content_json.encode("utf-8")
                ).hexdigest()
                if (
                    _text(row["previous_hash"]) != previous_hash
                    or _text(row["content_hash"]) != content_hash
                ):
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-event-chain-invalid"
                    )
                payload = json.loads(content_json)
                if not isinstance(payload, dict):
                    raise CryptoFirstLiveCoordinatorError(
                        "crypto-first-live-event-payload-invalid"
                    )
                result.append(
                    {
                        "eventId": _text(row["event_id"]),
                        "contentHash": content_hash,
                        "payload": payload,
                    }
                )
                previous_hash = content_hash
            return result
        finally:
            conn.close()


__all__ = [
    "CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED",
    "CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED",
    "CryptoFirstLiveCoordinatorError",
    "DurableCryptoFirstLiveCoordinator",
    "GLOBAL_SCOPE",
    "HIGH_WATER_SCHEMA_VERSION",
    "MAX_OWNER_LEASE_SECONDS",
    "SCHEMA_VERSION",
]
