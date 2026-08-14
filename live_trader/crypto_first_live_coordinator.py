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


SCHEMA_VERSION = "crypto-first-live-coordinator/v2"
GLOBAL_SCOPE = "CRYPTO_FIRST_LIVE_GLOBAL"
LANES = frozenset({"UPBIT", "BINANCE_SPOT"})
PHASES = frozenset(
    {
        "APPROVED_INERT",
        "ACTIVE",
        "CLEANUP_ONLY",
        "FINALIZED",
        "RECONCILIATION_REQUIRED",
    }
)
ENTRY_PHASES = frozenset({"APPROVED_INERT", "ACTIVE"})
MAX_OWNER_LEASE_SECONDS = 60.0
EXACT_FIRST_LIVE_SECONDS = 7200.0
# Deliberately code-owned.  Preparation may be shipped and exercised offline,
# but no caller or environment variable can open ACTIVE authority yet.
CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED = False

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
        reservation_evidence_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        final_approval_consumer: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        terminal_evidence_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        startup_owner_absent_reader: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        installation_validator: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
        owner_identity_verifier: (
            Callable[[Mapping[str, Any]], bool] | None
        ) = None,
    ) -> None:
        self.path = Path(path)
        self.clock = clock
        self.reservation_evidence_verifier = reservation_evidence_verifier
        self.final_approval_consumer = final_approval_consumer
        self.terminal_evidence_verifier = terminal_evidence_verifier
        self.startup_owner_absent_reader = startup_owner_absent_reader
        self.installation_validator = installation_validator
        self.owner_identity_verifier = owner_identity_verifier
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._verify_schema(conn)
            self._verify_integrity(conn)
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

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
                CREATE TABLE IF NOT EXISTS crypto_first_live_control (
                    scope_key TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    baseline_hash TEXT NOT NULL,
                    code_hash TEXT NOT NULL,
                    approval_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    owner_identity_json TEXT NOT NULL,
                    owner_identity_hash TEXT NOT NULL,
                    owner_token_hash TEXT NOT NULL,
                    owner_epoch INTEGER NOT NULL,
                    owner_lease_expires_epoch REAL NOT NULL,
                    hard_stop_epoch REAL NOT NULL,
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

    @staticmethod
    def _column_names(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
        return tuple(
            str(row[1]) for row in conn.execute(f"PRAGMA table_xinfo({table})")
        )

    @classmethod
    def _verify_schema(cls, conn: sqlite3.Connection) -> None:
        control = cls._column_names(conn, "crypto_first_live_control")
        events = cls._column_names(conn, "crypto_first_live_events")
        if control != (
            "scope_key",
            "schema_version",
            "phase",
            "run_id",
            "lane",
            "account_fingerprint",
            "baseline_hash",
            "code_hash",
            "approval_id",
            "permit_hash",
            "owner_identity_json",
            "owner_identity_hash",
            "owner_token_hash",
            "owner_epoch",
            "owner_lease_expires_epoch",
            "hard_stop_epoch",
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
                or int(event["revision"]) <= previous_revision
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
            previous_hash = content_hash
            previous_revision = int(event["revision"])
        if not secrets.compare_digest(
            _text(latest_payload.get("controlHash")),
            cls._control_hash(controls[0]),
        ):
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-control-hash-invalid"
            )

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
                "hardStopEpoch": 0.0,
                "entryAuthorityOpen": False,
                "networkOrderPostAllowed": False,
            }
        phase = _text(row["phase"])
        observed_now = self._now() if now is None else float(now)
        lease_active = bool(
            phase in {"APPROVED_INERT", "ACTIVE", "CLEANUP_ONLY"}
            and observed_now >= float(row["updated_epoch"])
            and observed_now < float(row["owner_lease_expires_epoch"])
        )
        hard_stop_active = bool(
            phase != "ACTIVE"
            or (
                float(row["hard_stop_epoch"]) > 0
                and observed_now < float(row["hard_stop_epoch"])
            )
        )
        return {
            "schemaVersion": _text(row["schema_version"]),
            "scope": _text(row["scope_key"]),
            "phase": phase,
            "runId": _text(row["run_id"]),
            "lane": _text(row["lane"]),
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
            "terminalEvidenceHash": _text(row["terminal_evidence_hash"]),
            "revision": int(row["revision"]),
            "detail": _text(row["detail"]),
            "ownerLeaseActive": lease_active,
            "clockAnomaly": observed_now < float(row["updated_epoch"]),
            "activationReleased": CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED,
            "entryAuthorityOpen": bool(
                CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED
                and phase == "ACTIVE"
                and lease_active
                and hard_stop_active
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

    def reserve_inert(
        self,
        *,
        lane: str,
        account_fingerprint: str,
        baseline_hash: str,
        code_hash: str,
        approval_id: str,
        permit_hash: str,
        reservation_evidence: Mapping[str, Any],
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
            purpose="RESERVE_INERT",
            lane=normalized_lane,
            account_fingerprint=account,
            identity=identity,
            identity_hash=identity_hash,
        )
        authoritative_evidence = {
            "schemaVersion": "crypto-first-live-reservation-evidence/v1",
            "scope": GLOBAL_SCOPE,
            "lane": normalized_lane,
            "accountFingerprint": account,
            "baselineHash": baseline,
            "codeHash": code,
            "approvalId": approval,
            "permitHash": permit,
            "ownerIdentity": identity,
            "ownerIdentityHash": identity_hash,
            "presented": dict(reservation_evidence),
        }
        verifier = self.reservation_evidence_verifier
        if verifier is None or verifier(authoritative_evidence) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-evidence-unverified"
            )
        evidence_hash = _digest(authoritative_evidence)
        lease = self._lease_seconds(lease_seconds)
        run_id = "crypto-first-live-run-" + secrets.token_hex(18)
        owner_token = secrets.token_urlsafe(48)
        with self._write() as conn:
            previous = self._row(conn)
            commit_now = self._now()
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
                    account_fingerprint, baseline_hash, code_hash,
                    approval_id, permit_hash, owner_identity_json,
                    owner_identity_hash, owner_token_hash, owner_epoch,
                    owner_lease_expires_epoch, hard_stop_epoch,
                    terminal_evidence_hash,
                    created_epoch, updated_epoch, revision, detail
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    schema_version=excluded.schema_version,
                    phase=excluded.phase, run_id=excluded.run_id,
                    lane=excluded.lane,
                    account_fingerprint=excluded.account_fingerprint,
                    baseline_hash=excluded.baseline_hash,
                    code_hash=excluded.code_hash,
                    approval_id=excluded.approval_id,
                    permit_hash=excluded.permit_hash,
                    owner_identity_json=excluded.owner_identity_json,
                    owner_identity_hash=excluded.owner_identity_hash,
                    owner_token_hash=excluded.owner_token_hash,
                    owner_epoch=excluded.owner_epoch,
                    owner_lease_expires_epoch=excluded.owner_lease_expires_epoch,
                    hard_stop_epoch=0,
                    terminal_evidence_hash='',
                    created_epoch=excluded.created_epoch,
                    updated_epoch=excluded.updated_epoch,
                    revision=excluded.revision,
                    detail=excluded.detail
                """,
                (
                    GLOBAL_SCOPE,
                    SCHEMA_VERSION,
                    "APPROVED_INERT",
                    run_id,
                    normalized_lane,
                    account,
                    baseline,
                    code,
                    approval,
                    permit,
                    identity_json,
                    identity_hash,
                    _token_hash(owner_token),
                    owner_epoch,
                    commit_now + lease,
                    0.0,
                    "",
                    commit_now,
                    commit_now,
                    revision,
                    "durable cross-exchange reservation; broker transport held",
                ),
            )
            self._append_event(
                conn,
                run_id=run_id,
                event_type="RESERVED_INERT",
                revision=revision,
                occurred_epoch=commit_now,
                payload={
                    "lane": normalized_lane,
                    "accountFingerprint": account,
                    "baselineHash": baseline,
                    "codeHash": code,
                    "approvalId": approval,
                    "permitHash": permit,
                    "ownerIdentityHash": identity_hash,
                    "ownerEpoch": owner_epoch,
                    "reservationEvidenceHash": evidence_hash,
                },
            )
            row = self._row(conn)
        return {
            **self._public(row, now=self._now()),
            "ownerToken": owner_token,
        }

    def activate(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        final_approval: Mapping[str, Any],
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        if not CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED:
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
        snapshot = self._verified_row_snapshot(purpose="ACTIVATE_READ")
        if snapshot is None or _text(snapshot["phase"]) != "APPROVED_INERT":
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-reservation-not-inert"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=token,
            owner_epoch=owner_epoch,
            now=observed_at,
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
        authoritative_approval = {
            "schemaVersion": "crypto-first-live-final-approval/v1",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot["lane"]),
            "accountFingerprint": _text(snapshot["account_fingerprint"]),
            "baselineHash": _text(snapshot["baseline_hash"]),
            "codeHash": _text(snapshot["code_hash"]),
            "approvalId": _text(snapshot["approval_id"]),
            "permitHash": _text(snapshot["permit_hash"]),
            "ownerEpoch": int(snapshot["owner_epoch"]),
            "coordinatorRevision": int(snapshot["revision"]),
            "presented": dict(final_approval),
        }
        consumer = self.final_approval_consumer
        if consumer is None or consumer(authoritative_approval) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-final-approval-unverified"
            )
        with self._write() as conn:
            row = self._row(conn)
            if (
                row is None
                or _text(row["phase"]) != "APPROVED_INERT"
                or int(row["revision"]) != int(snapshot["revision"])
                or _text(row["owner_identity_hash"])
                != _text(snapshot["owner_identity_hash"])
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-activation-cas-changed"
                )
            commit_now = self._now()
            self._assert_owner(
                row,
                run_id=run,
                owner_token=token,
                owner_epoch=owner_epoch,
                now=commit_now,
            )
            revision = int(row["revision"]) + 1
            hard_stop_epoch = commit_now + EXACT_FIRST_LIVE_SECONDS
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase='ACTIVE', owner_lease_expires_epoch=?,
                    hard_stop_epoch=?, updated_epoch=?, revision=?, detail=?
                WHERE scope_key=? AND run_id=? AND phase='APPROVED_INERT'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    commit_now + lease,
                    hard_stop_epoch,
                    commit_now,
                    revision,
                    "activated only after external final approval boundary",
                    GLOBAL_SCOPE,
                    run,
                    int(owner_epoch),
                    _text(snapshot["owner_identity_hash"]),
                    int(row["revision"]),
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
                    "exactRunSeconds": int(EXACT_FIRST_LIVE_SECONDS),
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
        with self._write() as conn:
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
            self._assert_owner(
                row,
                run_id=run,
                owner_token=token,
                owner_epoch=owner_epoch,
                now=commit_now,
            )
            revision = int(row["revision"]) + 1
            phase = _text(row["phase"])
            hard_stop_epoch = float(row["hard_stop_epoch"])
            if phase == "ACTIVE" and (
                hard_stop_epoch <= 0 or commit_now >= hard_stop_epoch
            ):
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='CLEANUP_ONLY',
                        owner_lease_expires_epoch=?, updated_epoch=?,
                        revision=?, detail='exact 7200 second hard stop reached'
                    WHERE scope_key=? AND run_id=? AND phase='ACTIVE'
                      AND owner_epoch=? AND revision=?
                      AND owner_identity_hash=?
                    """,
                    (
                        commit_now + lease,
                        commit_now,
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
                    payload={"hardStopEpoch": hard_stop_epoch},
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
                SET owner_lease_expires_epoch=?, updated_epoch=?, revision=?
                WHERE scope_key=? AND run_id=? AND owner_epoch=? AND revision=?
                  AND owner_identity_hash=?
                """,
                (
                    next_lease_expiry,
                    commit_now,
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

    def revoke_entry(self, *, run_id: str, reason: str) -> dict[str, Any]:
        """Risk-reducing transition callable by STOP/Kill without a token."""

        run = _exact_id(run_id, "run-id")
        detail = _text(reason)
        if not detail:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-cleanup-reason-required"
            )
        with self._write() as conn:
            row = self._row(conn)
            commit_now = self._now()
            if row is None or _text(row["run_id"]) != run:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-run-changed"
                )
            phase = _text(row["phase"])
            if phase in {"FINALIZED", "RECONCILIATION_REQUIRED"}:
                return self._public(row, now=commit_now)
            if phase not in ENTRY_PHASES | {"CLEANUP_ONLY"}:
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-cleanup-phase-invalid"
                )
            if phase != "CLEANUP_ONLY":
                mutation_time = max(
                    commit_now, float(row["updated_epoch"])
                )
                revision = int(row["revision"]) + 1
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='CLEANUP_ONLY', updated_epoch=?, revision=?,
                        detail=?
                    WHERE scope_key=? AND run_id=? AND revision=?
                    """,
                    (
                        mutation_time,
                        revision,
                        detail[:240],
                        GLOBAL_SCOPE,
                        run,
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
                    payload={"reason": detail[:240]},
                )
            result = self._row(conn)
        return self._public(result, now=self._now())

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
        if absence_reader is None or absence_reader(snapshot) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-prior-owner-absence-unproven"
            )
        with self._write() as conn:
            row = self._row(conn)
            commit_now = self._now()
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
                    owner_lease_expires_epoch=?, updated_epoch=?, revision=?,
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
                },
            )
            result = self._row(conn)
        return {
            **self._public(result, now=self._now()),
            "ownerToken": token,
        }

    def finalize(
        self,
        *,
        run_id: str,
        owner_token: str,
        owner_epoch: int,
        terminal_evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        run = _exact_id(run_id, "run-id")
        evidence = dict(terminal_evidence)
        observed_at = self._now()
        snapshot = self._verified_row_snapshot(purpose="FINALIZE_READ")
        if snapshot is None or _text(snapshot["phase"]) != "CLEANUP_ONLY":
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-finalize-requires-cleanup"
            )
        self._assert_owner(
            snapshot,
            run_id=run,
            owner_token=_text(owner_token),
            owner_epoch=owner_epoch,
            now=observed_at,
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
        authoritative_evidence = {
            "schemaVersion": "crypto-first-live-terminal-evidence/v1",
            "scope": GLOBAL_SCOPE,
            "runId": run,
            "lane": _text(snapshot["lane"]),
            "accountFingerprint": _text(snapshot["account_fingerprint"]),
            "baselineHash": _text(snapshot["baseline_hash"]),
            "codeHash": _text(snapshot["code_hash"]),
            "approvalId": _text(snapshot["approval_id"]),
            "permitHash": _text(snapshot["permit_hash"]),
            "ownerEpoch": int(snapshot["owner_epoch"]),
            "coordinatorRevision": int(snapshot["revision"]),
            "presented": evidence,
        }
        verifier = self.terminal_evidence_verifier
        if verifier is None or verifier(authoritative_evidence) is not True:
            raise CryptoFirstLiveCoordinatorError(
                "crypto-first-live-terminal-evidence-unverified"
            )
        terminal_hash = _digest(authoritative_evidence)
        with self._write() as conn:
            row = self._row(conn)
            commit_now = self._now()
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
            )
            revision = int(row["revision"]) + 1
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase='FINALIZED', owner_token_hash='',
                    owner_lease_expires_epoch=0, hard_stop_epoch=0,
                    terminal_evidence_hash=?, updated_epoch=?, revision=?,
                    detail='terminal evidence sealed; global owner released'
                WHERE scope_key=? AND run_id=? AND phase='CLEANUP_ONLY'
                  AND owner_epoch=? AND owner_identity_hash=? AND revision=?
                """,
                (
                    terminal_hash,
                    commit_now,
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
                payload={"terminalEvidenceHash": terminal_hash},
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
        with self._write() as conn:
            row = self._row(conn)
            commit_now = self._now()
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
                revision = int(row["revision"]) + 1
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_control
                    SET phase='RECONCILIATION_REQUIRED', owner_token_hash='',
                        owner_lease_expires_epoch=0, updated_epoch=?,
                        revision=?, detail=?
                    WHERE scope_key=? AND run_id=? AND revision=?
                    """,
                    (
                        mutation_time,
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
        snapshot = self.status()
        if snapshot.get("phase") == "IDLE":
            return snapshot
        phase = _text(snapshot.get("phase"))
        if phase not in {"APPROVED_INERT", "ACTIVE", "CLEANUP_ONLY"}:
            return snapshot
        clock_anomaly = snapshot.get("clockAnomaly") is True
        hard_stop_reached = bool(
            phase == "ACTIVE"
            and float(snapshot.get("hardStopEpoch", 0)) > 0
            and observed_at >= float(snapshot.get("hardStopEpoch", 0))
        )
        expired = hard_stop_reached or observed_at >= float(
            snapshot.get("ownerLeaseExpiresEpoch", 0)
        )
        if not clock_anomaly and not expired:
            return snapshot
        absent = False
        if not clock_anomaly and self.startup_owner_absent_reader is not None:
            absent = self.startup_owner_absent_reader(snapshot) is True

        with self._write() as conn:
            row = self._row(conn)
            commit_now = self._now()
            if (
                row is None
                or _text(row["run_id"]) != _text(snapshot.get("runId"))
                or int(row["revision"]) != int(snapshot.get("revision", -1))
                or _text(row["phase"])
                not in {"APPROVED_INERT", "ACTIVE", "CLEANUP_ONLY"}
            ):
                raise CryptoFirstLiveCoordinatorError(
                    "crypto-first-live-startup-audit-cas-changed"
                )
            clock_anomaly = commit_now < float(row["updated_epoch"])
            hard_stop_reached = bool(
                _text(row["phase"]) == "ACTIVE"
                and float(row["hard_stop_epoch"]) > 0
                and commit_now >= float(row["hard_stop_epoch"])
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
            owner_token_hash = _text(row["owner_token_hash"]) if absent else ""
            lease_expires = 0.0
            mutation_time = max(commit_now, float(row["updated_epoch"]))
            updated = conn.execute(
                """
                UPDATE crypto_first_live_control
                SET phase=?, owner_token_hash=?, owner_lease_expires_epoch=?,
                    updated_epoch=?, revision=?, detail=?
                WHERE scope_key=? AND run_id=? AND revision=?
                """,
                (
                    next_phase,
                    owner_token_hash,
                    lease_expires,
                    mutation_time,
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
                },
            )
            result = self._row(conn)
        return self._public(result, now=self._now())

    def status(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            self._verify_schema(conn)
            self._verify_integrity(conn)
            self._verify_empty_store_authority(conn, purpose="STATUS")
            return self._public(self._row(conn), now=self._now())
        finally:
            conn.close()

    def events(self) -> list[dict[str, Any]]:
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
    "CryptoFirstLiveCoordinatorError",
    "DurableCryptoFirstLiveCoordinator",
    "GLOBAL_SCOPE",
    "MAX_OWNER_LEASE_SECONDS",
    "SCHEMA_VERSION",
]
