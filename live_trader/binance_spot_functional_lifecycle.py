from __future__ import annotations

"""Durable owner/expiry lifecycle for the isolated Binance Spot test lane.

This is an adapter-ready managed entrypoint, not a production enablement.  It
keeps ordinary/smoke routing closed, persists owner leases and exact authority,
and can be exercised only with an explicitly injected mock lifecycle while
``PRODUCTION_LIFECYCLE_AVAILABLE`` remains false.
"""

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .binance_spot_continuous_functional import (
    BinanceSpotBoundaryBlocked,
    BinanceSpotContinuousFunctionalService,
    DurableFunctionalLedger,
    ExactPermit,
    PRODUCTION_AVAILABLE as CORE_PRODUCTION_AVAILABLE,
)
from .binance_spot_functional_approval import (
    DurableBinanceSpotApprovedPermitStore,
)


PRODUCTION_LIFECYCLE_AVAILABLE = False
PRODUCTION_STREAM_JOURNAL_AVAILABLE = False
PRODUCTION_SIGNAL_SCHEDULER_AVAILABLE = False
PRODUCTION_STARTUP_RECOVERY_AVAILABLE = False
PRODUCTION_STATE_SERVER_WIRING_AVAILABLE = False
ROUTE_KEY = "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m"
MAX_OWNER_LEASE_SECONDS = 60.0


class BinanceSpotLifecycleError(RuntimeError):
    pass


def composite_production_available() -> bool:
    """Single release gate; a partial component flip can never arm the lane."""

    from .binance_spot_functional_mutation import PRODUCTION_MUTATION_AVAILABLE

    return all(
        (
            CORE_PRODUCTION_AVAILABLE,
            PRODUCTION_MUTATION_AVAILABLE,
            PRODUCTION_LIFECYCLE_AVAILABLE,
            PRODUCTION_STREAM_JOURNAL_AVAILABLE,
            PRODUCTION_SIGNAL_SCHEDULER_AVAILABLE,
            PRODUCTION_STARTUP_RECOVERY_AVAILABLE,
            PRODUCTION_STATE_SERVER_WIRING_AVAILABLE,
        )
    )


def _text(value: object) -> str:
    return str(value or "").strip()


def _secret_hash(value: str) -> str:
    return hashlib.sha256(_text(value).encode("utf-8")).hexdigest()


def _canonical_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def _owner_prefix(session_id: str) -> str:
    return f"ftb-{hashlib.sha256(_text(session_id).encode()).hexdigest()[:12]}-"


@dataclass(frozen=True)
class LifecycleHandle:
    session_id: str
    capability: str
    owner_id: str
    owner_token: str
    expires_epoch: float
    cleanup_deadline_epoch: float


class DurableBinanceSpotFunctionalControl:
    """Persist functional-only authority and a short renewable owner lease."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS binance_spot_functional_control (
                    route_key TEXT PRIMARY KEY,
                    phase TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    capability_hash TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    owner_token_hash TEXT NOT NULL,
                    owner_lease_expires_epoch REAL NOT NULL,
                    entry_expires_epoch REAL NOT NULL,
                    cleanup_deadline_epoch REAL NOT NULL,
                    revision INTEGER NOT NULL,
                    detail TEXT NOT NULL,
                    updated_epoch REAL NOT NULL
                )
                """
            )
            connection.commit()

    def _row(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
        return dict(row) if row is not None else None

    def _require_owner(
        self,
        row: Mapping[str, Any],
        *,
        owner_id: str,
        owner_token: str,
        allow_expired: bool = False,
    ) -> None:
        if (
            _text(row.get("owner_id")) != _text(owner_id)
            or not secrets.compare_digest(
                _text(row.get("owner_token_hash")), _secret_hash(owner_token)
            )
        ):
            raise BinanceSpotLifecycleError("functional lifecycle owner changed")
        if not allow_expired and float(self.clock()) >= float(
            row.get("owner_lease_expires_epoch") or 0
        ):
            raise BinanceSpotLifecycleError("functional lifecycle owner lease expired")

    def _update(self, *, expected_phases: set[str], values: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if row is None or _text(row["phase"]).upper() not in expected_phases:
                connection.rollback()
                raise BinanceSpotLifecycleError("functional lifecycle phase changed")
            assignments = [f"{field}=?" for field in values]
            params = list(values.values())
            assignments.extend(["revision=revision+1", "updated_epoch=?"])
            params.extend([float(self.clock()), ROUTE_KEY])
            connection.execute(
                f"UPDATE binance_spot_functional_control SET {', '.join(assignments)} WHERE route_key=?",
                params,
            )
            connection.commit()
        updated = self._row()
        assert updated is not None
        return updated

    def arm(
        self,
        permit: ExactPermit,
        *,
        owner_id: str,
        owner_token: str,
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        now = float(self.clock())
        lease = float(lease_seconds)
        if not _text(owner_id) or len(_text(owner_token)) < 24:
            raise BinanceSpotLifecycleError("owner id/token is invalid")
        if lease <= 0 or lease > MAX_OWNER_LEASE_SECONDS:
            raise BinanceSpotLifecycleError("owner lease must be within 60 seconds")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if prior is not None and _text(prior["phase"]).upper() not in {
                "FINALIZED",
                "FAILED",
            }:
                connection.rollback()
                raise BinanceSpotLifecycleError("functional lifecycle is already armed")
            revision = int(prior["revision"] if prior is not None else 0) + 1
            connection.execute(
                """
                INSERT INTO binance_spot_functional_control (
                    route_key, phase, permit_id, permit_hash, session_id,
                    capability_hash, owner_id, owner_token_hash,
                    owner_lease_expires_epoch, entry_expires_epoch,
                    cleanup_deadline_epoch, revision, detail, updated_epoch
                ) VALUES (?, 'ARMED', ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(route_key) DO UPDATE SET
                    phase=excluded.phase,
                    permit_id=excluded.permit_id,
                    permit_hash=excluded.permit_hash,
                    session_id='', capability_hash='',
                    owner_id=excluded.owner_id,
                    owner_token_hash=excluded.owner_token_hash,
                    owner_lease_expires_epoch=excluded.owner_lease_expires_epoch,
                    entry_expires_epoch=excluded.entry_expires_epoch,
                    cleanup_deadline_epoch=excluded.cleanup_deadline_epoch,
                    revision=excluded.revision,
                    detail=excluded.detail,
                    updated_epoch=excluded.updated_epoch
                """,
                (
                    ROUTE_KEY,
                    permit.permit_id,
                    permit.permit_hash,
                    _text(owner_id),
                    _secret_hash(owner_token),
                    min(now + lease, permit.expires_epoch),
                    permit.expires_epoch,
                    permit.cleanup_deadline_epoch,
                    revision,
                    "armed with ordinary/smoke routes closed",
                    now,
                ),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return result

    def activate(
        self,
        *,
        session_id: str,
        capability_hash: str,
        owner_id: str,
        owner_token: str,
    ) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is not armed")
        self._require_owner(row, owner_id=owner_id, owner_token=owner_token)
        return self._update(
            expected_phases={"ARMED"},
            values={
                "phase": "ACTIVE",
                "session_id": _text(session_id),
                "capability_hash": _text(capability_hash).lower(),
                "detail": "exact functional capability active",
            },
        )

    def fail_armed(self, *, owner_id: str, owner_token: str, detail: str) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        self._require_owner(
            row, owner_id=owner_id, owner_token=owner_token, allow_expired=True
        )
        return self._update(
            expected_phases={"ARMED"},
            values={"phase": "FAILED", "detail": _text(detail)[:500]},
        )

    def mark_start_abort_pending(
        self,
        *,
        session_id: str,
        owner_id: str,
        owner_token: str,
        detail: str,
    ) -> dict[str, Any]:
        """Fence a caught post-create failure for official startup audit."""

        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        self._require_owner(row, owner_id=owner_id, owner_token=owner_token)
        return self._update(
            expected_phases={"ARMED"},
            values={
                "session_id": _text(session_id),
                "detail": _text(detail)[:500],
            },
        )

    def audit_incomplete_startup(self) -> dict[str, Any]:
        """Repair hard-crash windows before any new start is considered.

        ARMED without a session is terminally failed after the short owner
        lease.  ARMED plus one matching unactivated RUNNING session cannot have
        crossed an order boundary because real-order authority was never
        activated.  It is handed to the manager for a fresh official
        balance/order attestation and terminal START_FAILED seal; it is never
        rotated into cleanup authority.
        """

        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if row is None or _text(row["phase"]).upper() != "ARMED":
                connection.commit()
                return dict(row) if row is not None else {"phase": "IDLE"}
            if now < float(row["owner_lease_expires_epoch"]):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "ARMED startup owner lease is still active"
                )
            sessions = connection.execute(
                """
                SELECT * FROM binance_spot_functional_sessions
                WHERE permit_id=? AND permit_hash=? AND state='RUNNING'
                ORDER BY started_epoch, session_id
                """,
                (_text(row["permit_id"]), _text(row["permit_hash"])),
            ).fetchall()
            if not sessions:
                failed_sessions = connection.execute(
                    """
                    SELECT * FROM binance_spot_functional_sessions
                    WHERE permit_id=? AND permit_hash=? AND state='FAILED'
                        AND final_evidence_hash!=''
                    ORDER BY finalized_epoch, session_id
                    """,
                    (_text(row["permit_id"]), _text(row["permit_hash"])),
                ).fetchall()
                failed_sessions = [
                    item
                    for item in failed_sessions
                    if (
                        lambda evidence: isinstance(evidence, dict)
                        and evidence.get("startupAbortAttestation", {}).get(
                            "durableActionCount"
                        )
                        == 0
                    )(
                        json.loads(_text(item["final_evidence_json"]))
                        if _text(item["final_evidence_json"])
                        else {}
                    )
                ]
                if len(failed_sessions) == 1:
                    connection.commit()
                    failed = failed_sessions[0]
                    return {
                        **dict(row),
                        "session_id": _text(failed["session_id"]),
                        "final_evidence_hash": _text(
                            failed["final_evidence_hash"]
                        ),
                        "startupRecovery": "STARTUP_ABORT_FINALIZE_READY",
                    }
                if len(failed_sessions) > 1:
                    connection.rollback()
                    raise BinanceSpotLifecycleError(
                        "startup audit found ambiguous failed sessions"
                    )
                connection.execute(
                    """
                    UPDATE binance_spot_functional_control
                    SET phase='FAILED', capability_hash='', owner_token_hash='',
                        owner_lease_expires_epoch=0, revision=revision+1,
                        detail='startup audit: ARMED expired before core session',
                        updated_epoch=? WHERE route_key=? AND phase='ARMED'
                    """,
                    (now, ROUTE_KEY),
                )
                connection.commit()
                result = self._row()
                assert result is not None
                return {**result, "startupRecovery": "FAILED_NO_SESSION"}
            if len(sessions) != 1:
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "startup audit found ambiguous unactivated sessions"
                )
            session = sessions[0]
            action_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM binance_spot_functional_actions
                    WHERE session_id=?
                    """,
                    (_text(session["session_id"]),),
                ).fetchone()[0]
            )
            if action_count != 0 or now >= float(session["cleanup_deadline_epoch"]):
                connection.execute(
                    """
                    UPDATE binance_spot_functional_sessions
                    SET state='RECONCILIATION_REQUIRED', capability_hash='',
                        cleanup_started=1, final_new_entries_blocked=1,
                        detail='startup audit found actions or exceeded cleanup deadline'
                    WHERE session_id=? AND state='RUNNING'
                    """,
                    (_text(session["session_id"]),),
                )
                connection.execute(
                    """
                    UPDATE binance_spot_functional_control
                    SET phase='FAILED', capability_hash='', owner_token_hash='',
                        owner_lease_expires_epoch=0, session_id=?,
                        revision=revision+1,
                        detail='startup audit: preactivation state requires manual review',
                        updated_epoch=? WHERE route_key=? AND phase='ARMED'
                    """,
                    (_text(session["session_id"]), now, ROUTE_KEY),
                )
                connection.commit()
                result = self._row()
                assert result is not None
                return {**result, "startupRecovery": "MANUAL_RECONCILIATION"}
            connection.commit()
        return {
            **dict(row),
            "session_id": _text(session["session_id"]),
            "startupRecovery": "STARTUP_ABORT_AUDIT_REQUIRED",
            "actionCount": 0,
        }

    def audit_all_incomplete_startup(self) -> dict[str, Any]:
        """Cross-audit control and every nonterminal core session on restart.

        This covers crashes after activation as well as the two ARMED windows.
        It never restores entry authority.  A recoverable orphan gets freshly
        rotated cleanup-only secrets; ambiguous/multiple/deadline-expired rows
        have their capabilities revoked for manual reconciliation.
        """

        row = self._row()
        if row is not None and _text(row["phase"]).upper() == "ARMED":
            return self.audit_incomplete_startup()
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            control = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            sessions = connection.execute(
                """
                SELECT * FROM binance_spot_functional_sessions
                WHERE state IN ('RUNNING','CLEANUP','RECONCILIATION_REQUIRED')
                ORDER BY started_epoch, session_id
                """
            ).fetchall()
            phase = _text(control["phase"]).upper() if control is not None else "IDLE"
            if phase == "FAILED" and control is not None and _text(
                control["session_id"]
            ):
                failed = connection.execute(
                    """
                    SELECT state, final_evidence_hash, final_evidence_json
                    FROM binance_spot_functional_sessions
                    WHERE session_id=? AND permit_id=? AND permit_hash=?
                    """,
                    (
                        _text(control["session_id"]),
                        _text(control["permit_id"]),
                        _text(control["permit_hash"]),
                    ),
                ).fetchone()
                if (
                    failed is not None
                    and _text(failed["state"]).upper() == "FAILED"
                    and len(_text(failed["final_evidence_hash"])) == 64
                    and (
                        lambda evidence: isinstance(evidence, dict)
                        and evidence.get("startupAbortAttestation", {}).get(
                            "durableActionCount"
                        )
                        == 0
                    )(
                        json.loads(_text(failed["final_evidence_json"]))
                        if _text(failed["final_evidence_json"])
                        else {}
                    )
                ):
                    connection.commit()
                    return {
                        **dict(control),
                        "startupRecovery": "STARTUP_ABORT_TERMINAL_RETIRE_READY",
                        "final_evidence_hash": _text(
                            failed["final_evidence_hash"]
                        ),
                    }
            if phase == "FINAL_RESET" and control is not None:
                finalized = connection.execute(
                    """
                    SELECT state, final_evidence_hash
                    FROM binance_spot_functional_sessions WHERE session_id=?
                    """,
                    (_text(control["session_id"]),),
                ).fetchone()
                if (
                    finalized is not None
                    and _text(finalized["state"]).upper()
                    in {"FINAL_PREPARED", "FINALIZED"}
                    and len(_text(finalized["final_evidence_hash"])) == 64
                ):
                    connection.commit()
                    return {
                        **dict(control),
                        "startupRecovery": "FINAL_RESET_READY",
                    }
            if len(sessions) > 1:
                connection.execute(
                    """
                    UPDATE binance_spot_functional_sessions
                    SET state='RECONCILIATION_REQUIRED', capability_hash='',
                        cleanup_started=1, final_new_entries_blocked=1,
                        detail='startup audit: multiple nonterminal sessions'
                    WHERE state IN ('RUNNING','CLEANUP','RECONCILIATION_REQUIRED')
                    """
                )
                if control is not None:
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_control
                        SET phase='FAILED', capability_hash='', owner_token_hash='',
                            owner_lease_expires_epoch=0, revision=revision+1,
                            detail='startup audit: ambiguous nonterminal sessions',
                            updated_epoch=? WHERE route_key=?
                        """,
                        (now, ROUTE_KEY),
                    )
                connection.commit()
                return {
                    "phase": "FAILED",
                    "startupRecovery": "MANUAL_RECONCILIATION_MULTIPLE_SESSIONS",
                }
            if not sessions:
                if phase in {"ACTIVE", "CLEANUP", "FINAL_RESET"} and control is not None:
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_control
                        SET phase='FAILED', capability_hash='', owner_token_hash='',
                            owner_lease_expires_epoch=0, revision=revision+1,
                            detail='startup audit: authority without core session',
                            updated_epoch=? WHERE route_key=?
                        """,
                        (now, ROUTE_KEY),
                    )
                    connection.commit()
                    result = self._row()
                    assert result is not None
                    return {
                        **result,
                        "startupRecovery": "FAILED_ORPHAN_AUTHORITY",
                    }
                connection.commit()
                return dict(control) if control is not None else {"phase": "IDLE"}
            session = sessions[0]
            session_id = _text(session["session_id"])
            permit_id = _text(session["permit_id"])
            permit_hash = _text(session["permit_hash"])
            if _text(session["state"]).upper() == "RECONCILIATION_REQUIRED" or now >= float(
                session["cleanup_deadline_epoch"]
            ):
                connection.execute(
                    """
                    UPDATE binance_spot_functional_sessions
                    SET state='RECONCILIATION_REQUIRED', capability_hash='',
                        cleanup_started=1, final_new_entries_blocked=1,
                        detail='startup audit: cleanup cannot be automated'
                    WHERE session_id=?
                    """,
                    (session_id,),
                )
                if control is not None:
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_control
                        SET phase='FAILED', session_id=?, capability_hash='',
                            owner_token_hash='', owner_lease_expires_epoch=0,
                            revision=revision+1,
                            detail='startup audit: manual reconciliation required',
                            updated_epoch=? WHERE route_key=?
                        """,
                        (session_id, now, ROUTE_KEY),
                    )
                connection.commit()
                return {
                    "phase": "FAILED",
                    "session_id": session_id,
                    "permit_id": permit_id,
                    "permit_hash": permit_hash,
                    "startupRecovery": "MANUAL_RECONCILIATION",
                }
            if control is not None and phase in {"ACTIVE", "CLEANUP", "FINAL_RESET"}:
                if (
                    _text(control["session_id"]) != session_id
                    or _text(control["permit_id"]) != permit_id
                    or not secrets.compare_digest(
                        _text(control["permit_hash"]), permit_hash
                    )
                ):
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_sessions
                        SET state='RECONCILIATION_REQUIRED', capability_hash='',
                            cleanup_started=1, final_new_entries_blocked=1,
                            detail='startup audit: control/core identity mismatch'
                        WHERE session_id=?
                        """,
                        (session_id,),
                    )
                    connection.execute(
                        """
                        UPDATE binance_spot_functional_control
                        SET phase='FAILED', capability_hash='', owner_token_hash='',
                            owner_lease_expires_epoch=0, revision=revision+1,
                            detail='startup audit: control/core identity mismatch',
                            updated_epoch=? WHERE route_key=?
                        """,
                        (now, ROUTE_KEY),
                    )
                    connection.commit()
                    return {
                        "phase": "FAILED",
                        "session_id": session_id,
                        "startupRecovery": "MANUAL_RECONCILIATION_IDENTITY_MISMATCH",
                    }
                if now < float(control["owner_lease_expires_epoch"]):
                    connection.rollback()
                    raise BinanceSpotLifecycleError(
                        "startup audit owner lease is still active"
                    )
            cleanup_capability = secrets.token_urlsafe(32)
            owner_token = secrets.token_urlsafe(32)
            owner_id = "startup-recovery-" + secrets.token_hex(8)
            capability_hash = _secret_hash(cleanup_capability)
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='CLEANUP', cleanup_started=1, capability_hash=?,
                    detail='startup audit rotated orphan cleanup authority'
                WHERE session_id=? AND state IN ('RUNNING','CLEANUP')
                """,
                (capability_hash, session_id),
            )
            revision = int(control["revision"] if control is not None else 0) + 1
            entry_expires = float(session["expires_epoch"])
            cleanup_deadline = float(session["cleanup_deadline_epoch"])
            connection.execute(
                """
                INSERT INTO binance_spot_functional_control (
                    route_key, phase, permit_id, permit_hash, session_id,
                    capability_hash, owner_id, owner_token_hash,
                    owner_lease_expires_epoch, entry_expires_epoch,
                    cleanup_deadline_epoch, revision, detail, updated_epoch
                ) VALUES (?, 'CLEANUP', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(route_key) DO UPDATE SET
                    phase='CLEANUP', permit_id=excluded.permit_id,
                    permit_hash=excluded.permit_hash,
                    session_id=excluded.session_id,
                    capability_hash=excluded.capability_hash,
                    owner_id=excluded.owner_id,
                    owner_token_hash=excluded.owner_token_hash,
                    owner_lease_expires_epoch=excluded.owner_lease_expires_epoch,
                    entry_expires_epoch=excluded.entry_expires_epoch,
                    cleanup_deadline_epoch=excluded.cleanup_deadline_epoch,
                    revision=excluded.revision, detail=excluded.detail,
                    updated_epoch=excluded.updated_epoch
                """,
                (
                    ROUTE_KEY,
                    permit_id,
                    permit_hash,
                    session_id,
                    capability_hash,
                    owner_id,
                    _secret_hash(owner_token),
                    min(now + MAX_OWNER_LEASE_SECONDS, cleanup_deadline),
                    entry_expires,
                    cleanup_deadline,
                    revision,
                    "startup audit: all orphan entry authority revoked",
                    now,
                ),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return {
            **result,
            "startupRecovery": "CLEANUP_ONLY",
            "cleanupCapability": cleanup_capability,
            "ownerId": owner_id,
            "ownerToken": owner_token,
        }

    def seal_attested_startup_abort(
        self,
        *,
        session_id: str,
        expected_revision: int,
        final_evidence_hash: str,
    ) -> dict[str, Any]:
        """Finish ARMED→FAILED only after core stored official abort proof."""

        now = float(self.clock())
        evidence_hash = _text(final_evidence_hash).lower()
        if len(evidence_hash) != 64:
            raise BinanceSpotLifecycleError("startup abort evidence hash is invalid")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            control = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            session = connection.execute(
                """
                SELECT * FROM binance_spot_functional_sessions
                WHERE session_id=?
                """,
                (_text(session_id),),
            ).fetchone()
            action_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM binance_spot_functional_actions
                    WHERE session_id=?
                    """,
                    (_text(session_id),),
                ).fetchone()[0]
            )
            if (
                control is None
                or session is None
                or _text(control["phase"]).upper() != "ARMED"
                or int(control["revision"]) != int(expected_revision)
                or now < float(control["owner_lease_expires_epoch"])
                or _text(control["permit_id"]) != _text(session["permit_id"])
                or not secrets.compare_digest(
                    _text(control["permit_hash"]), _text(session["permit_hash"])
                )
                or _text(session["state"]).upper() != "FAILED"
                or not secrets.compare_digest(
                    _text(session["final_evidence_hash"]).lower(), evidence_hash
                )
                or action_count != 0
            ):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "startup abort control/core/action fence changed"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='FAILED', session_id=?, capability_hash='',
                    owner_token_hash='', owner_lease_expires_epoch=0,
                    revision=revision+1,
                    detail='startup audit: official baseline unchanged; start failed',
                    updated_epoch=?
                WHERE route_key=? AND phase='ARMED' AND revision=?
                """,
                (_text(session_id), now, ROUTE_KEY, int(expected_revision)),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotLifecycleError("startup abort control CAS failed")
            connection.commit()
        result = self._row()
        assert result is not None
        return {**result, "startupRecovery": "START_FAILED_ATTESTED"}

    def reject_startup_abort_attestation(
        self,
        *,
        session_id: str,
        expected_revision: int,
        detail: str,
    ) -> dict[str, Any]:
        """Revoke an ARMED orphan when official baseline proof mismatches."""

        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            control = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            action_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM binance_spot_functional_actions
                    WHERE session_id=?
                    """,
                    (_text(session_id),),
                ).fetchone()[0]
            )
            if (
                control is None
                or _text(control["phase"]).upper() != "ARMED"
                or int(control["revision"]) != int(expected_revision)
                or now < float(control["owner_lease_expires_epoch"])
            ):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "startup rejection authority fence changed"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='RECONCILIATION_REQUIRED', capability_hash='',
                    cleanup_started=1, final_new_entries_blocked=1, detail=?
                WHERE session_id=? AND state='RUNNING'
                """,
                (_text(detail)[:1000], _text(session_id)),
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='FAILED', session_id=?, capability_hash='',
                    owner_token_hash='', owner_lease_expires_epoch=0,
                    revision=revision+1, detail=?, updated_epoch=?
                WHERE route_key=? AND phase='ARMED' AND revision=?
                """,
                (
                    _text(session_id),
                    _text(detail)[:500],
                    now,
                    ROUTE_KEY,
                    int(expected_revision),
                ),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return {
            **result,
            "startupRecovery": "MANUAL_RECONCILIATION",
            "durableActionCount": action_count,
        }

    def verify_handle(self, handle: LifecycleHandle) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        phase = _text(row["phase"]).upper()
        self._require_owner(
            row,
            owner_id=handle.owner_id,
            owner_token=handle.owner_token,
            allow_expired=phase == "CLEANUP",
        )
        if (
            _text(row["session_id"]) != handle.session_id
            or not secrets.compare_digest(
                _text(row["capability_hash"]), _secret_hash(handle.capability)
            )
        ):
            raise BinanceSpotLifecycleError("lifecycle handle session/capability changed")
        if phase not in {"ACTIVE", "CLEANUP"}:
            raise BinanceSpotLifecycleError("lifecycle handle is not active")
        return row

    def heartbeat(
        self,
        *,
        owner_id: str,
        owner_token: str,
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        self._require_owner(row, owner_id=owner_id, owner_token=owner_token)
        if _text(row["phase"]).upper() != "ACTIVE":
            raise BinanceSpotLifecycleError("only ACTIVE lifecycle can heartbeat")
        lease = float(lease_seconds)
        if lease <= 0 or lease > MAX_OWNER_LEASE_SECONDS:
            raise BinanceSpotLifecycleError("owner lease must be within 60 seconds")
        now = float(self.clock())
        if now >= float(row["entry_expires_epoch"]):
            raise BinanceSpotLifecycleError("entry permit expired")
        return self._update(
            expected_phases={"ACTIVE"},
            values={
                "owner_lease_expires_epoch": min(
                    now + lease, float(row["entry_expires_epoch"])
                ),
                "detail": "owner heartbeat renewed",
            },
        )

    def begin_cleanup(
        self,
        *,
        owner_id: str,
        owner_token: str,
        detail: str,
    ) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        self._require_owner(
            row,
            owner_id=owner_id,
            owner_token=owner_token,
            allow_expired=True,
        )
        return self._update(
            expected_phases={"ACTIVE", "CLEANUP"},
            values={"phase": "CLEANUP", "detail": _text(detail)[:500]},
        )

    def expire_cleanup_to_reconciliation(
        self,
        *,
        session_id: str,
        capability_hash: str,
        detail: str,
    ) -> dict[str, Any]:
        """Revoke every lane capability when automated cleanup time is over."""

        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if (
                row is None
                or _text(row["phase"]).upper()
                not in {"ACTIVE", "CLEANUP", "FINAL_RESET"}
                or _text(row["session_id"]) != _text(session_id)
                or not secrets.compare_digest(
                    _text(row["capability_hash"]), _text(capability_hash)
                )
                or now < float(row["cleanup_deadline_epoch"])
            ):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "cleanup deadline revocation authority changed"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='RECONCILIATION_REQUIRED', capability_hash='',
                    cleanup_started=1, final_new_entries_blocked=1, detail=?
                WHERE session_id=? AND state IN (
                    'RUNNING','CLEANUP','RECONCILIATION_REQUIRED'
                )
                """,
                (_text(detail)[:1000], _text(session_id)),
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='FAILED', capability_hash='', owner_token_hash='',
                    owner_lease_expires_epoch=0, revision=revision+1,
                    detail=?, updated_epoch=? WHERE route_key=?
                """,
                (_text(detail)[:500], now, ROUTE_KEY),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return result

    def fail_closed_owner_health(
        self,
        *,
        session_id: str,
        capability_hash: str,
        detail: str,
    ) -> dict[str, Any]:
        """Atomically revoke a lane whose heartbeat could not latch CLEANUP."""

        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if (
                row is None
                or _text(row["session_id"]) != _text(session_id)
                or not secrets.compare_digest(
                    _text(row["capability_hash"]), _text(capability_hash)
                )
                or _text(row["phase"]).upper() not in {"ACTIVE", "CLEANUP"}
            ):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "owner-health revocation authority changed"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='RECONCILIATION_REQUIRED', capability_hash='',
                    cleanup_started=1, final_new_entries_blocked=1, detail=?
                WHERE session_id=? AND state IN ('RUNNING','CLEANUP')
                """,
                (_text(detail)[:1000], _text(session_id)),
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='FAILED', capability_hash='', owner_token_hash='',
                    owner_lease_expires_epoch=0, revision=revision+1,
                    detail=?, updated_epoch=? WHERE route_key=?
                """,
                (_text(detail)[:500], now, ROUTE_KEY),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return result

    def revoke_startup_recovery(
        self, *, session_id: str, detail: str
    ) -> dict[str, Any]:
        """Fail closed a startup matrix mismatch before exposing raw secrets."""

        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if row is None or _text(row["session_id"]) != _text(session_id):
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "startup recovery revocation session changed"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='RECONCILIATION_REQUIRED', capability_hash='',
                    cleanup_started=1, final_new_entries_blocked=1, detail=?
                WHERE session_id=? AND state IN (
                    'RUNNING','CLEANUP','RECONCILIATION_REQUIRED'
                )
                """,
                (_text(detail)[:1000], _text(session_id)),
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='FAILED', capability_hash='', owner_token_hash='',
                    owner_lease_expires_epoch=0, revision=revision+1,
                    detail=?, updated_epoch=? WHERE route_key=?
                """,
                (_text(detail)[:500], now, ROUTE_KEY),
            )
            connection.commit()
        result = self._row()
        assert result is not None
        return result

    def takeover_expired_cleanup(
        self,
        *,
        session_id: str,
        owner_id: str,
        owner_token: str,
        capability: str | None = None,
        lease_seconds: float = MAX_OWNER_LEASE_SECONDS,
    ) -> dict[str, Any]:
        now = float(self.clock())
        if len(_text(owner_token)) < 24:
            raise BinanceSpotLifecycleError("recovery owner token is invalid")
        lease = float(lease_seconds)
        if lease <= 0 or lease > MAX_OWNER_LEASE_SECONDS:
            raise BinanceSpotLifecycleError("owner lease must be within 60 seconds")
        # The prior raw capability intentionally is never persisted and is
        # expected to be gone after a process crash.  Rotate both the control
        # pointer and core ledger capability in one SQLite transaction.  The
        # durable session is moved to CLEANUP before the new secret is exposed,
        # so this minted handle can never authorize BUY/re-entry.
        cleanup_capability = secrets.token_urlsafe(32)
        cleanup_hash = _secret_hash(cleanup_capability)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_control WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise BinanceSpotLifecycleError("functional lifecycle is missing")
            phase = _text(row["phase"]).upper()
            if phase not in {"ACTIVE", "CLEANUP", "FINAL_RESET"}:
                connection.rollback()
                raise BinanceSpotLifecycleError("functional lifecycle phase changed")
            if now < min(
                float(row["owner_lease_expires_epoch"]),
                float(row["entry_expires_epoch"]),
            ):
                connection.rollback()
                raise BinanceSpotLifecycleError("active owner lease has not expired")
            if _text(row["session_id"]) != _text(session_id):
                connection.rollback()
                raise BinanceSpotLifecycleError("recovery session changed")
            if now >= float(row["cleanup_deadline_epoch"]):
                connection.rollback()
                raise BinanceSpotLifecycleError("cleanup deadline exceeded")
            session = connection.execute(
                "SELECT * FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if session is None or _text(session["state"]).upper() == "FINALIZED":
                connection.rollback()
                raise BinanceSpotLifecycleError(
                    "durable functional session is not cleanup-recoverable"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_sessions
                SET state='CLEANUP', cleanup_started=1, capability_hash=?,
                    detail='owner-loss attested; cleanup capability rotated'
                WHERE session_id=?
                """,
                (cleanup_hash, _text(session_id)),
            )
            connection.execute(
                """
                UPDATE binance_spot_functional_control
                SET phase='CLEANUP', capability_hash=?, owner_id=?,
                    owner_token_hash=?, owner_lease_expires_epoch=?,
                    revision=revision+1,
                    detail='expired owner replaced; fresh cleanup-only capability',
                    updated_epoch=?
                WHERE route_key=?
                """,
                (
                    cleanup_hash,
                    _text(owner_id),
                    _secret_hash(owner_token),
                    min(now + lease, float(row["cleanup_deadline_epoch"])),
                    now,
                    ROUTE_KEY,
                ),
            )
            connection.commit()
        updated = self._row()
        assert updated is not None
        return {**updated, "cleanup_capability": cleanup_capability}

    def prepare_final_reset(
        self, *, owner_id: str, owner_token: str
    ) -> dict[str, Any]:
        row = self._row()
        if row is None:
            raise BinanceSpotLifecycleError("functional lifecycle is missing")
        self._require_owner(
            row,
            owner_id=owner_id,
            owner_token=owner_token,
            allow_expired=True,
        )
        return self._update(
            expected_phases={"ACTIVE", "CLEANUP"},
            values={"phase": "FINAL_RESET", "detail": "authority pointers reset before final seal"},
        )

    def restore_cleanup_after_failed_final(self, *, detail: str) -> dict[str, Any]:
        return self._update(
            expected_phases={"FINAL_RESET"},
            values={"phase": "CLEANUP", "detail": _text(detail)[:500]},
        )

    def seal_final(self) -> dict[str, Any]:
        row = self._row()
        if row is not None and _text(row["phase"]).upper() == "FINALIZED":
            return row
        return self._update(
            expected_phases={"FINAL_RESET"},
            values={
                "phase": "FINALIZED",
                "capability_hash": "",
                "owner_token_hash": "",
                "owner_lease_expires_epoch": 0.0,
                "detail": "baseline-flat final evidence sealed; capabilities reset",
            },
        )

    def resume_final_reset(self, *, session_id: str) -> dict[str, Any]:
        """Idempotently seal a core-finalized session after a process crash."""

        row = self._row()
        if row is None or _text(row["session_id"]) != _text(session_id):
            raise BinanceSpotLifecycleError("final-reset session changed")
        if _text(row["phase"]).upper() == "FINALIZED":
            return row
        if _text(row["phase"]).upper() != "FINAL_RESET":
            raise BinanceSpotLifecycleError("functional lifecycle is not in FINAL_RESET")
        with closing(self._connect()) as connection:
            session = connection.execute(
                "SELECT state FROM binance_spot_functional_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
        if session is None or _text(session["state"]).upper() not in {
            "FINAL_PREPARED",
            "FINALIZED",
        }:
            raise BinanceSpotLifecycleError(
                "core final seal is absent; wait for owner expiry then rotate cleanup authority"
            )
        return self.seal_final()

    def authority_snapshot(self) -> dict[str, object]:
        row = self._row()
        if row is None:
            phase = "IDLE"
            row = {
                "permit_id": "",
                "permit_hash": "",
                "session_id": "",
                "capability_hash": "",
                "revision": 0,
            }
        else:
            phase = _text(row["phase"]).upper()
            now = float(self.clock())
            if phase == "ACTIVE" and now >= min(
                float(row["owner_lease_expires_epoch"]),
                float(row["entry_expires_epoch"]),
            ):
                row = self._update(
                    expected_phases={"ACTIVE"},
                    values={
                        "phase": "CLEANUP",
                        "detail": "owner lease or entry permit expired; cleanup only",
                    },
                )
                phase = "CLEANUP"
        routed = phase in {"ARMED", "ACTIVE", "CLEANUP"}
        active = phase in {"ACTIVE", "CLEANUP"}
        cleanup = phase == "CLEANUP"
        return {
            "realOrdersEnabled": active,
            "dryRun": False,
            "killSwitch": cleanup,
            "newEntriesBlocked": True,
            "ordinaryLiveAllowed": False,
            "smokeAllowed": False,
            "functionalOnlyRouting": routed,
            "activePermitId": _text(row["permit_id"]) if routed else "",
            "activePermitHash": _text(row["permit_hash"]) if routed else "",
            "activeSessionId": _text(row["session_id"]) if active else "",
            "functionalCapabilityHash": (
                _text(row["capability_hash"]) if active else ""
            ),
            "cleanupOnlyAuthority": cleanup,
            "cleanupSessionId": _text(row["session_id"]) if cleanup else "",
            "cleanupCapabilityHash": (
                _text(row["capability_hash"]) if cleanup else ""
            ),
            "authorityRevision": f"binance-functional-control-{int(row['revision'])}",
        }

    def status(self) -> dict[str, object]:
        row = self._row()
        if row is None:
            return {
                "phase": "IDLE",
                "productionAvailable": composite_production_available(),
            }
        return {
            "phase": _text(row["phase"]).upper(),
            "permitId": row["permit_id"],
            "sessionId": row["session_id"],
            "ownerId": row["owner_id"],
            "ownerLeaseExpiresEpoch": row["owner_lease_expires_epoch"],
            "entryExpiresEpoch": row["entry_expires_epoch"],
            "cleanupDeadlineEpoch": row["cleanup_deadline_epoch"],
            "revision": row["revision"],
            "detail": row["detail"],
            "functionalCapabilityReset": not bool(_text(row["capability_hash"])),
            "ownerTokenReset": not bool(_text(row["owner_token_hash"])),
            "productionAvailable": composite_production_available(),
        }


class BinanceSpotFunctionalLifecycleManager:
    """One managed start/tick/recover/finalize entrypoint."""

    def __init__(
        self,
        *,
        ledger: DurableFunctionalLedger,
        control: DurableBinanceSpotFunctionalControl,
        service: BinanceSpotContinuousFunctionalService,
        truth_reader: Any,
        mutation_edge: Callable[..., Mapping[str, Any]],
        permit_store: DurableBinanceSpotApprovedPermitStore | None = None,
        signal_reader: Callable[[], Mapping[str, Any] | None] | None = None,
        stream_owner_binder: Callable[[str, str, str, str], None] | None = None,
        stream_terminal_barrier: Callable[[], Mapping[str, Any]] | None = None,
        stream_cleanup_recovery_latcher: Callable[..., Mapping[str, Any]] | None = None,
        stream_startup_recovery_latcher: Callable[..., Mapping[str, Any]] | None = None,
        stream_terminal_retirer: Callable[..., Mapping[str, Any]] | None = None,
        startup_owner_process_absence_attested: bool = False,
        activation_permit_issuer: Callable[
            [object, float], Mapping[str, Any]
        ] | None = None,
        clock: Callable[[], float] = time.time,
        allow_mock_lifecycle: bool = False,
    ) -> None:
        self.ledger = ledger
        self.control = control
        self.service = service
        self.truth_reader = truth_reader
        self.mutation_edge = mutation_edge
        self.permit_store = permit_store
        self.signal_reader = signal_reader
        self.stream_owner_binder = stream_owner_binder
        self.stream_terminal_barrier = stream_terminal_barrier
        self.stream_cleanup_recovery_latcher = stream_cleanup_recovery_latcher
        self.stream_startup_recovery_latcher = stream_startup_recovery_latcher
        self.stream_terminal_retirer = stream_terminal_retirer
        self._startup_owner_process_absence_attested = bool(
            startup_owner_process_absence_attested
        )
        self.activation_permit_issuer = activation_permit_issuer
        self.clock = clock
        self.allow_mock_lifecycle = bool(allow_mock_lifecycle)

    def _assert_available(self) -> None:
        if not self.allow_mock_lifecycle and not composite_production_available():
            raise BinanceSpotLifecycleError(
                "Binance Spot managed functional lifecycle is not production-available"
            )

    def start(
        self,
        permit_payload: Mapping[str, Any],
        *,
        owner_id: str,
        owner_token: str,
    ) -> LifecycleHandle:
        self._assert_available()
        now = float(self.clock())
        approval_claim_token = ""
        if self.permit_store is not None:
            if set(permit_payload) != {"permitId", "permitHash"}:
                raise BinanceSpotLifecycleError(
                    "production start accepts only a server-approved permit id/hash reference"
                )
            permit_payload, approval_claim_token = self.permit_store.claim(
                permit_id=_text(permit_payload.get("permitId")),
                permit_hash=_text(permit_payload.get("permitHash")),
                owner_id=owner_id,
                activation_epoch=now,
                activation_permit_issuer=self.activation_permit_issuer,
            )
        permit = ExactPermit.parse(permit_payload, now_epoch=now)
        try:
            armed = self.control.arm(
                permit, owner_id=owner_id, owner_token=owner_token
            )
        except Exception:
            if self.permit_store is not None and approval_claim_token:
                self.permit_store.fail_claim(
                    permit_id=permit.permit_id,
                    claim_token=approval_claim_token,
                    detail="control arm failed after single-use approval claim",
                )
            raise
        started: Mapping[str, Any] | None = None
        try:
            truth, _ = self.truth_reader.read(
                baseline_epoch=now, owner_prefix=""
            )
            started = self.service.start(
                permit_payload,
                truth,
                activation_fence={
                    "routeKey": ROUTE_KEY,
                    "revision": int(armed["revision"]),
                    "ownerId": _text(owner_id),
                    "ownerTokenHash": _secret_hash(owner_token),
                },
            )
            if self.stream_owner_binder is not None:
                self.stream_owner_binder(
                    _owner_prefix(_text(started["sessionId"])),
                    _text(started["sessionId"]),
                    permit.permit_id,
                    permit.permit_hash,
                )
            if self.permit_store is not None:
                self.permit_store.bind_session(
                    permit_id=permit.permit_id,
                    claim_token=approval_claim_token,
                    session_id=_text(started["sessionId"]),
                )
            self.control.activate(
                session_id=_text(started["sessionId"]),
                capability_hash=_text(started["functionalCapabilityHash"]),
                owner_id=owner_id,
                owner_token=owner_token,
            )
        except Exception as exc:
            if started is None:
                try:
                    self.control.fail_armed(
                        owner_id=owner_id,
                        owner_token=owner_token,
                        detail=(
                            f"prestart failed:{type(exc).__name__}:"
                            f"{str(exc)[:300]}"
                        ),
                    )
                except BinanceSpotLifecycleError:
                    current = self.control.status()
                    if _text(current.get("phase")).upper() != "FAILED":
                        raise
            else:
                # No mutation authority was activated and no handle escaped.
                # Keep the core RUNNING + control ARMED as a durable
                # START_ABORT_PENDING pair.  The startup auditor waits out the
                # owner lease, takes fresh official account/order/fill truth,
                # then seals START_FAILED and archives either a bound or
                # unbound prebaseline stream.  Creating unattested FAILED
                # evidence here would strand a crash between these steps.
                self.control.mark_start_abort_pending(
                    session_id=_text(started["sessionId"]),
                    owner_id=owner_id,
                    owner_token=owner_token,
                    detail=(
                        "post-create start abort pending official attestation:"
                        f"{type(exc).__name__}:{str(exc)[:250]}"
                    ),
                )
            if self.permit_store is not None and approval_claim_token:
                try:
                    self.permit_store.fail_claim(
                        permit_id=permit.permit_id,
                        claim_token=approval_claim_token,
                        detail=f"managed start failed:{type(exc).__name__}",
                    )
                except Exception:
                    # A successful bind made the permit ACTIVE; never roll it
                    # back to reusable APPROVED state.
                    pass
            raise
        assert started is not None
        return LifecycleHandle(
            session_id=_text(started["sessionId"]),
            capability=_text(started["functionalCapability"]),
            owner_id=_text(owner_id),
            owner_token=_text(owner_token),
            expires_epoch=float(started["expiresEpoch"]),
            cleanup_deadline_epoch=float(started["cleanupDeadlineEpoch"]),
        )

    def heartbeat(self, handle: LifecycleHandle) -> dict[str, Any]:
        self._assert_available()
        self.control.verify_handle(handle)
        try:
            # A short owner lease is not proof that the authenticated private
            # stream is still lossless.  Renew only after the complete official
            # reader has revalidated its inbound liveness/gap attestation.
            self._truth(handle)
        except Exception as exc:
            try:
                self.control.begin_cleanup(
                    owner_id=handle.owner_id,
                    owner_token=handle.owner_token,
                    detail=(
                        "owner heartbeat stream/truth proof failed:"
                        f"{type(exc).__name__}"
                    ),
                )
            except Exception as cleanup_exc:
                # Never let the scheduler infer CLEANUP from a local exception
                # while durable control may still be ACTIVE.  Revoke the exact
                # capability/session in one transaction; subsequent ticks and
                # mutations are impossible and startup/manual reconciliation
                # owns recovery.
                self.control.fail_closed_owner_health(
                    session_id=handle.session_id,
                    capability_hash=_secret_hash(handle.capability),
                    detail=(
                        "heartbeat truth failed and cleanup latch failed:"
                        f"{type(exc).__name__}/{type(cleanup_exc).__name__}"
                    ),
                )
            raise
        return self.control.heartbeat(
            owner_id=handle.owner_id, owner_token=handle.owner_token
        )

    def _complete_startup_abort_terminal(
        self,
        *,
        session_id: str,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        """Idempotently finish approval/control/stream seals for START_FAILED."""

        session = self.ledger.session(session_id)
        durable = self.ledger.final_evidence(session_id)
        evidence = durable["evidence"]
        if (
            _text(session.get("state")).upper() != "FAILED"
            or _text(evidence.get("outcome")).upper()
            != "START_FAILED_BEFORE_ACTIVATION"
            or evidence.get("functionalCapabilityReset") is not True
            or evidence.get("startupAbortAttestation", {}).get(
                "durableActionCount"
            )
            != 0
        ):
            raise BinanceSpotLifecycleError(
                "durable startup-abort evidence is not terminal"
            )
        status = self.control.status()
        if _text(status.get("phase")).upper() == "ARMED":
            if expected_revision is None:
                raise BinanceSpotLifecycleError(
                    "startup-abort control revision is missing"
                )
            terminal = self.control.seal_attested_startup_abort(
                session_id=session_id,
                expected_revision=int(expected_revision),
                final_evidence_hash=_text(durable["evidenceHash"]),
            )
        elif (
            _text(status.get("phase")).upper() == "FAILED"
            and _text(status.get("sessionId")) == session_id
        ):
            terminal = status
        else:
            raise BinanceSpotLifecycleError(
                "startup-abort control terminal identity changed"
            )
        if self.permit_store is not None:
            approval = self.permit_store.status(_text(session["permit_id"]))
            approval_state = _text(approval.get("state")).upper()
            if approval_state in {"CLAIMED", "ACTIVE"}:
                self.permit_store.fail_start_for_session(
                    permit_id=_text(session["permit_id"]),
                    session_id=session_id,
                    detail="startup audit sealed preactivation failure",
                )
            elif approval_state != "FAILED":
                raise BinanceSpotLifecycleError(
                    "startup-abort approval terminal state changed"
                )
        if self.stream_terminal_retirer is None and not self.allow_mock_lifecycle:
            raise BinanceSpotLifecycleError(
                "production startup abort requires durable stream retirement"
            )
        if self.stream_terminal_retirer is not None:
            retired = self.stream_terminal_retirer(
                session_id=session_id,
                permit_id=_text(session["permit_id"]),
                permit_hash=_text(session["permit_hash"]),
                final_evidence_hash=_text(durable["evidenceHash"]),
                terminal_reason="START_FAILED",
            )
            if (
                retired.get("retired") is not True
                or _text(retired.get("sessionId")) != session_id
                or _text(retired.get("finalEvidenceHash")).lower()
                != _text(durable["evidenceHash"]).lower()
            ):
                raise BinanceSpotLifecycleError(
                    "startup-abort stream retirement attestation changed"
                )
        return {
            **dict(terminal),
            "session_id": session_id,
            "startupRecovery": "START_FAILED_ATTESTED",
            "finalEvidenceHash": _text(durable["evidenceHash"]),
            "brokerMutationCount": 0,
            "coreState": "FAILED",
        }

    def audit_incomplete_startup(self) -> LifecycleHandle | dict[str, Any]:
        """Backend restart entrypoint; never resumes orphaned entry authority."""

        result = self.control.audit_all_incomplete_startup()
        recovery = _text(result.get("startupRecovery")).upper()
        permit_id = _text(result.get("permit_id"))
        permit_hash = _text(result.get("permit_hash")).lower()
        approval_audit: Mapping[str, Any] | None = None
        # Process-owner absence is a startup fact, never a lifetime authority.
        # Consume it before any durable operation so recover/status calls in
        # this same process cannot retire a freshly approved candidate.
        owner_process_absence_attested = (
            self._startup_owner_process_absence_attested
        )
        self._startup_owner_process_absence_attested = False
        if self.permit_store is not None:
            approval_audit = self.permit_store.audit_orphaned_claims(
                owner_lease_seconds=MAX_OWNER_LEASE_SECONDS,
                owner_process_absence_attested=owner_process_absence_attested,
            )
            failed_claims = list(approval_audit.get("failedPermitIds") or [])
            if (
                failed_claims
                and not recovery
                and _text(result.get("phase")).upper() in {"IDLE", "FAILED"}
            ):
                return {
                    **dict(result),
                    "startupRecovery": "FAILED_CLAIM_ONLY",
                    "failedPermitIds": failed_claims,
                    "manualReviewRequired": bool(
                        approval_audit.get("manualReviewRequired")
                    ),
                }
        if recovery == "FINAL_RESET_READY":
            return self.resume_final_reset(session_id=_text(result["session_id"]))
        if recovery in {
            "STARTUP_ABORT_FINALIZE_READY",
            "STARTUP_ABORT_TERMINAL_RETIRE_READY",
        }:
            return self._complete_startup_abort_terminal(
                session_id=_text(result["session_id"]),
                expected_revision=(
                    int(result["revision"])
                    if recovery == "STARTUP_ABORT_FINALIZE_READY"
                    else None
                ),
            )
        if recovery == "STARTUP_ABORT_AUDIT_REQUIRED":
            session_id = _text(result.get("session_id"))
            session = self.ledger.session(session_id)
            reader = getattr(
                self.truth_reader, "read_startup_abort_attestation", None
            )
            if callable(reader):
                truth, _ = reader(
                    baseline_epoch=float(session["started_epoch"]),
                    owner_prefix=_owner_prefix(session_id),
                    session_id=session_id,
                    permit_id=_text(session["permit_id"]),
                    permit_hash=_text(session["permit_hash"]),
                )
            else:
                # Mock-only readers still provide the same complete official
                # account/order/fill shape.  Production release requires the
                # dedicated reader and remains behind the composite false gate.
                if not self.allow_mock_lifecycle:
                    raise BinanceSpotLifecycleError(
                        "startup abort official attestation reader is missing"
                    )
                truth, _ = self.truth_reader.read(
                    baseline_epoch=float(session["started_epoch"]),
                    owner_prefix=_owner_prefix(session_id),
                )
            balances = truth.get("balances")
            if not isinstance(balances, list):
                raise BinanceSpotLifecycleError(
                    "startup abort account balances are incomplete"
                )
            totals: dict[str, Decimal] = {}
            try:
                for item in balances:
                    if not isinstance(item, Mapping):
                        raise BinanceSpotLifecycleError(
                            "startup abort account balance row is malformed"
                        )
                    asset = _text(item.get("asset")).upper()
                    if not asset or asset in totals:
                        raise BinanceSpotLifecycleError(
                            "startup abort balance assets are not unique"
                        )
                    totals[asset] = Decimal(_text(item.get("free"))) + Decimal(
                        _text(item.get("locked"))
                    )
            except InvalidOperation as exc:
                raise BinanceSpotLifecycleError(
                    "startup abort account balance is malformed"
                ) from exc
            prefix = _owner_prefix(session_id)
            open_orders = truth.get("openOrders")
            closed_orders = truth.get("closedOrders")
            fills = truth.get("fills")
            if not all(isinstance(rows, list) for rows in (open_orders, closed_orders, fills)):
                raise BinanceSpotLifecycleError(
                    "startup abort account/order/fill truth is incomplete"
                )
            baseline_ids = json.loads(_text(session["baseline_open_ids_json"]))
            current_open_ids = sorted(
                _text(row.get("clientOrderId") or row.get("orderId"))
                for row in open_orders
                if isinstance(row, Mapping)
            )
            owned_activity = any(
                _text(row.get("clientOrderId") or row.get("origClientOrderId")).startswith(prefix)
                for rows in (open_orders, closed_orders, fills)
                for row in rows
                if isinstance(row, Mapping)
            )
            complete = all(
                truth.get(field) is True
                for field in (
                    "accountComplete",
                    "balancesComplete",
                    "openOrdersComplete",
                    "closedOrdersComplete",
                    "fillsComplete",
                    "feesComplete",
                )
            )
            complete = (
                complete
                and truth.get("externalActivityAbsent") is True
                and (
                    self.allow_mock_lifecycle
                    or truth.get("startupAbortAttestation") is True
                )
            )
            balances_unchanged = (
                totals.get("BTC") == Decimal(_text(session["baseline_base"]))
                and totals.get("USDT") == Decimal(_text(session["baseline_quote"]))
            )
            working_unchanged = current_open_ids == list(baseline_ids)
            if not complete or not balances_unchanged or not working_unchanged or owned_activity:
                self.control.reject_startup_abort_attestation(
                    session_id=session_id,
                    expected_revision=int(result["revision"]),
                    detail="startup abort official truth mismatch; manual reconciliation",
                )
                raise BinanceSpotLifecycleError(
                    "startup abort official truth requires manual reconciliation"
                )
            official_truth_hash = _canonical_hash(dict(truth))
            attestation = {
                "startupAbortAttestation": True,
                "officialTruthHash": official_truth_hash,
                "baselineBalancesUnchanged": True,
                "baselineWorkingOrdersUnchanged": True,
                "ownedOrderFillActivityAbsent": True,
                "durableActionCount": 0,
            }
            failed = self.ledger.abort_session_before_activation(
                session_id,
                detail="startup hard crash before authority activation",
                now_epoch=float(self.clock()),
                attestation=attestation,
            )
            durable = self.ledger.final_evidence(session_id)
            _ = failed, durable
            return self._complete_startup_abort_terminal(
                session_id=session_id,
                expected_revision=int(result["revision"]),
            )
        if self.permit_store is not None and recovery in {
            "FAILED_NO_SESSION",
            "FAILED_ORPHAN_AUTHORITY",
            "MANUAL_RECONCILIATION",
            "MANUAL_RECONCILIATION_MULTIPLE_SESSIONS",
            "MANUAL_RECONCILIATION_IDENTITY_MISMATCH",
        } and permit_id and permit_hash:
            status = self.permit_store.status(permit_id)
            if _text(status.get("state")).upper() == "CLAIMED":
                self.permit_store.startup_fail_lost_claim(
                    permit_id=permit_id,
                    permit_hash=permit_hash,
                    detail=f"startup audit terminalized {recovery}",
                )
        if recovery != "CLEANUP_ONLY":
            return result
        session = self.ledger.session(_text(result["session_id"]))
        if self.permit_store is not None:
            approval = self.permit_store.status(_text(session["permit_id"]))
            approval_state = _text(approval.get("state")).upper()
            if approval_state == "CLAIMED":
                approval = self.permit_store.startup_bind_lost_claim_to_cleanup(
                    permit_id=_text(session["permit_id"]),
                    permit_hash=_text(session["permit_hash"]),
                    session_id=_text(session["session_id"]),
                )
                approval_state = _text(approval.get("state")).upper()
            if (
                approval_state != "ACTIVE"
                or _text(approval.get("session_id")) != _text(session["session_id"])
                or _text(approval.get("permit_hash")).lower()
                != _text(session["permit_hash"]).lower()
            ):
                self.control.revoke_startup_recovery(
                    session_id=_text(session["session_id"]),
                    detail="startup approval/core/control matrix mismatch",
                )
                raise BinanceSpotLifecycleError(
                    "startup approval matrix requires manual reconciliation"
                )
        if self.stream_startup_recovery_latcher is not None:
            self.stream_startup_recovery_latcher(
                session_id=_text(session["session_id"]),
                detail=(
                    "startup owner/socket lost; preserved stream is REST-only"
                ),
            )
            session = self.ledger.set_session(
                _text(session["session_id"]),
                state="CLEANUP",
                cleanup_started=True,
                cleanup_recovery_used=True,
                detail="startup owner loss requires REST-only cleanup truth",
            )
        elif not self.allow_mock_lifecycle:
            self.control.revoke_startup_recovery(
                session_id=_text(session["session_id"]),
                detail="startup recovery stream-gap latcher is missing",
            )
            raise BinanceSpotLifecycleError(
                "startup recovery stream-gap latcher is missing"
            )
        return LifecycleHandle(
            session_id=_text(result["session_id"]),
            capability=_text(result["cleanupCapability"]),
            owner_id=_text(result["ownerId"]),
            owner_token=_text(result["ownerToken"]),
            expires_epoch=float(session["expires_epoch"]),
            cleanup_deadline_epoch=float(session["cleanup_deadline_epoch"]),
        )

    def takeover_expired_cleanup(
        self,
        *,
        session_id: str,
        owner_id: str,
        owner_token: str,
        capability: str | None = None,
    ) -> LifecycleHandle:
        self._assert_available()
        row = self.control.takeover_expired_cleanup(
            session_id=session_id,
            owner_id=owner_id,
            owner_token=owner_token,
            capability=capability,
        )
        cleanup_capability = _text(row["cleanup_capability"])
        return LifecycleHandle(
            session_id=_text(session_id),
            capability=cleanup_capability,
            owner_id=_text(owner_id),
            owner_token=_text(owner_token),
            expires_epoch=float(row["entry_expires_epoch"]),
            cleanup_deadline_epoch=float(row["cleanup_deadline_epoch"]),
        )

    def resume_final_reset(self, *, session_id: str) -> dict[str, Any]:
        self._assert_available()
        durable = self.ledger.final_evidence(session_id)
        session = self.ledger.session(session_id)
        if self.stream_terminal_retirer is None and not self.allow_mock_lifecycle:
            raise BinanceSpotLifecycleError(
                "production final reset requires durable stream retirement"
            )
        # FINAL_RESET is the durable pending phase.  Archive the exact bound
        # stream first; a crash/failure leaves FINAL_RESET intact so startup
        # audit retries the same session/evidence hash instead of declaring the
        # route reusable with a stale binding.
        if self.stream_terminal_retirer is not None:
            recovered = bool(
                durable["evidence"].get(
                    "privateStreamGapRecoveredCleanupOnly"
                )
            )
            retired = self.stream_terminal_retirer(
                session_id=session_id,
                permit_id=_text(session["permit_id"]),
                permit_hash=_text(session["permit_hash"]),
                final_evidence_hash=_text(durable["evidenceHash"]),
                terminal_reason=(
                    "RECOVERED_FINALIZED" if recovered else "FINALIZED"
                ),
                expected_journal_seal_hash=_text(
                    durable["evidence"].get("streamJournalSealHash")
                ),
            )
            if (
                retired.get("retired") is not True
                or _text(retired.get("sessionId")) != session_id
                or _text(retired.get("finalEvidenceHash")).lower()
                != _text(durable["evidenceHash"]).lower()
            ):
                raise BinanceSpotLifecycleError(
                    "durable stream retirement attestation changed"
                )
        if self.permit_store is not None:
            approval = self.permit_store.status(_text(session["permit_id"]))
            if _text(approval.get("state")).upper() == "ACTIVE":
                self.permit_store.consume(session_id=session_id)
            elif _text(approval.get("state")).upper() != "CONSUMED":
                raise BinanceSpotLifecycleError(
                    "final-reset approval state is not resumable"
                )
        self.ledger.commit_prepared_final(
            session_id, now_epoch=float(self.clock())
        )
        control = self.control.resume_final_reset(session_id=session_id)
        return {
            "control": control,
            "evidence": durable["evidence"],
            "evidenceHash": durable["evidenceHash"],
        }

    def prove_ambiguous_not_accepted(
        self,
        handle: LifecycleHandle,
        *,
        claim_id: str,
        permit_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one trusted official nonacceptance observation.

        No bar, signal, query response, or account snapshot is accepted from a
        caller.  The manager derives the exact client id from the durable
        sealed claim and asks the production truth reader to perform the full
        REST/private-stream read plus signed ``GET /api/v3/order`` itself.
        """

        self._assert_available()
        permit_payload = self._permit(handle, permit_payload)
        self.control.verify_handle(handle)
        claim = self.ledger.action(claim_id)
        if _text(claim.get("session_id")) != handle.session_id:
            raise BinanceSpotLifecycleError("ambiguous claim session changed")
        action = json.loads(_text(claim.get("sealed_action_json")))
        client_order_id = _text(action.get("clientOrderId"))
        session = self.ledger.session(handle.session_id)
        reader = getattr(self.truth_reader, "read_nonacceptance_observation", None)
        if not callable(reader):
            raise BinanceSpotLifecycleError(
                "official exact-order nonacceptance reader is unavailable"
            )
        truth, _, exact_query = reader(
            baseline_epoch=float(session["started_epoch"]),
            owner_prefix=_owner_prefix(handle.session_id),
            client_order_id=client_order_id,
        )
        result = self.service.prove_ambiguous_not_accepted(
            handle.session_id,
            handle.capability,
            permit_payload,
            truth,
            claim_id,
            exact_client_order_query=exact_query,
        )
        terminal = _text(result.get("state")).upper() == (
            "AMBIGUOUS_PROVEN_NOT_ACCEPTED"
        )
        return {
            "ok": True,
            "status": (
                "AMBIGUOUS_PROVEN_NOT_ACCEPTED"
                if terminal
                else "NONACCEPTANCE_OBSERVATION_RECORDED"
            ),
            "claim": result,
            "retryAttempted": False,
        }

    def next_due_ambiguous_claim(
        self, handle: LifecycleHandle
    ) -> dict[str, Any] | None:
        """Select the sole exact no-retry claim when its proof window is due."""

        self._assert_available()
        self.control.verify_handle(handle)
        candidates = [
            action
            for action in self.ledger.actions(handle.session_id)
            if _text(action.get("state")).upper()
            in {"POST_MAY_HAVE_CROSSED", "RECONCILIATION_REQUIRED"}
        ]
        if not candidates:
            return None
        if len(candidates) != 1:
            raise BinanceSpotLifecycleError(
                "multiple ambiguous mutation claims require manual reconciliation"
            )
        claim = candidates[0]
        now = float(self.clock())
        observations = int(claim.get("absence_proof_count") or 0)
        due_epoch = (
            float(claim.get("updated_epoch") or 0) + 60.0
            if observations == 0
            else float(claim.get("absence_last_epoch") or 0) + 5.0
        )
        if now < due_epoch:
            return None
        return {
            "claimId": _text(claim.get("claim_id")),
            "state": _text(claim.get("state")).upper(),
            "absenceProofCount": observations,
            "dueEpoch": due_epoch,
        }

    def fail_ambiguous_reconciliation(
        self, handle: LifecycleHandle, *, reason: str
    ) -> dict[str, Any]:
        """Revoke the lane when trusted broker absence cannot be proved."""

        self._assert_available()
        self.control.verify_handle(handle)
        return self.control.revoke_startup_recovery(
            session_id=handle.session_id,
            detail=f"ambiguous broker outcome requires manual reconciliation:{reason}",
        )

    def _truth(
        self,
        handle: LifecycleHandle,
        *,
        cleanup_only: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        session = self.ledger.session(handle.session_id)
        try:
            return self.truth_reader.read(
                baseline_epoch=float(session["started_epoch"]),
                owner_prefix=_owner_prefix(handle.session_id),
            )
        except Exception:
            if not cleanup_only:
                raise
            recovery_reader = getattr(
                self.truth_reader, "read_cleanup_recovery", None
            )
            if not callable(recovery_reader):
                raise
            return recovery_reader(
                baseline_epoch=float(session["started_epoch"]),
                owner_prefix=_owner_prefix(handle.session_id),
            )

    def _permit(
        self,
        handle: LifecycleHandle,
        supplied: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]:
        if self.permit_store is None:
            if supplied is None:
                raise BinanceSpotLifecycleError("mock permit payload is required")
            return supplied
        stored = self.permit_store.resolve_active(session_id=handle.session_id)
        if supplied is not None and set(supplied) not in (
            set(),
            {"permitId", "permitHash"},
        ):
            raise BinanceSpotLifecycleError(
                "active production lifecycle rejects caller permit JSON"
            )
        if supplied:
            if (
                _text(supplied.get("permitId")) != _text(stored.get("permitId"))
                or _text(supplied.get("permitHash")).lower()
                != _text(stored.get("permitHash")).lower()
            ):
                raise BinanceSpotLifecycleError(
                    "active approved permit reference changed"
                )
        return stored

    def tick(
        self,
        handle: LifecycleHandle,
        permit_payload: Mapping[str, Any] | None = None,
        *,
        finalized_bar: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._assert_available()
        permit_payload = self._permit(handle, permit_payload)
        authority = self.control.authority_snapshot()
        self.control.verify_handle(handle)
        cleanup_authority = authority["cleanupOnlyAuthority"] is True
        truth, rules = self._truth(handle, cleanup_only=cleanup_authority)
        if not cleanup_authority:
            risk = self.service.risk_status(
                handle.session_id,
                handle.capability,
                permit_payload,
                truth,
            )
            if risk["cleanupRequired"] is True:
                self.control.begin_cleanup(
                    owner_id=handle.owner_id,
                    owner_token=handle.owner_token,
                    detail="owner loss limit reached; exact-owned cleanup only",
                )
                authority = self.control.authority_snapshot()
        if authority["cleanupOnlyAuthority"] is True:
            result = self.service.recover(
                handle.session_id,
                handle.capability,
                permit_payload,
                truth,
                rules,
            )
        elif not self.allow_mock_lifecycle and finalized_bar is not None:
            raise BinanceSpotLifecycleError(
                "production scheduler rejects caller-supplied bar/signal input"
            )
        elif self.signal_reader is not None:
            internal_bar = self.signal_reader()
            if internal_bar is None:
                result = {"ok": True, "status": "NO_FINALIZED_BAR", "action": None}
            else:
                try:
                    close_epoch = datetime.fromisoformat(
                        _text(internal_bar.get("barCloseAt")).replace("Z", "+00:00")
                    ).astimezone(timezone.utc).timestamp()
                except (TypeError, ValueError) as exc:
                    raise BinanceSpotLifecycleError(
                        "internal finalized bar close time is invalid"
                    ) from exc
                durable = self.ledger.session(handle.session_id)
                if close_epoch <= float(durable.get("last_bar_close_epoch") or 0):
                    result = {
                        "ok": True,
                        "status": "NO_NEW_FINALIZED_BAR",
                        "action": None,
                        "barCloseAt": _text(internal_bar.get("barCloseAt")),
                    }
                else:
                    result = self.service.observe_bar(
                        handle.session_id,
                        handle.capability,
                        permit_payload,
                        truth,
                        rules,
                        internal_bar,
                    )
        elif finalized_bar is not None:
            result = self.service.observe_bar(
                handle.session_id,
                handle.capability,
                permit_payload,
                truth,
                rules,
                finalized_bar,
            )
        else:
            result = {"ok": True, "status": "ACTIVE", "action": None}
        if result.get("claim") is not None:
            # The strategy/risk decision cannot authorize a POST from its old
            # snapshot.  Re-read the complete official account/order/fill/
            # stream/rules truth after the durable claim and pass only that
            # fresh snapshot into the final service boundary.
            final_truth, final_rules = self._truth(
                handle,
                cleanup_only=(
                    self.control.authority_snapshot()["cleanupOnlyAuthority"]
                    is True
                ),
            )
            dispatched = self.service.dispatch_claim(
                handle.session_id,
                handle.capability,
                permit_payload,
                final_truth,
                final_rules,
                _text(result["claim"]["claim_id"]),
                submitter=self.mutation_edge,
            )
            if dispatched.get("ok") is not True:
                self.control.begin_cleanup(
                    owner_id=handle.owner_id,
                    owner_token=handle.owner_token,
                    detail=f"dispatch ended {dispatched.get('status')}",
                )
            return {**result, "dispatch": dispatched}
        return result

    def begin_cleanup(self, handle: LifecycleHandle, *, reason: str) -> dict[str, Any]:
        self._assert_available()
        return self.control.begin_cleanup(
            owner_id=handle.owner_id,
            owner_token=handle.owner_token,
            detail=reason,
        )

    def fail_cleanup_deadline(
        self, handle: LifecycleHandle, *, reason: str
    ) -> dict[str, Any]:
        """Terminally revoke automation; leave explicit manual reconciliation."""

        return self.control.expire_cleanup_to_reconciliation(
            session_id=handle.session_id,
            capability_hash=_secret_hash(handle.capability),
            detail=reason,
        )

    def runtime_window(self, handle: LifecycleHandle) -> dict[str, Any]:
        """Return the durable activation-relative two-hour evidence window."""

        session = self.ledger.session(handle.session_id)
        elapsed = max(
            0.0, float(self.clock()) - float(session["started_epoch"])
        )
        return {
            "sessionId": handle.session_id,
            "startedEpoch": float(session["started_epoch"]),
            "elapsedSeconds": elapsed,
            "requiredSeconds": 2 * 60 * 60,
            "complete": elapsed >= 2 * 60 * 60,
        }

    def finalize(
        self,
        handle: LifecycleHandle,
        permit_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._assert_available()
        if self.stream_terminal_retirer is None and not self.allow_mock_lifecycle:
            raise BinanceSpotLifecycleError(
                "production final seal requires durable stream retirement"
            )
        if self.stream_terminal_barrier is None and not self.allow_mock_lifecycle:
            raise BinanceSpotLifecycleError(
                "production final seal requires an inbound stream barrier"
            )
        permit_payload = self._permit(handle, permit_payload)
        self.control.verify_handle(handle)
        reset = self.control.prepare_final_reset(
            owner_id=handle.owner_id, owner_token=handle.owner_token
        )
        barrier_crossed = False
        try:
            # Hide every mutation pointer first, then take the final complete
            # broker snapshot.  First fence socket recv->journal handoff and
            # drain every frame already received by the authenticated writer;
            # otherwise a callback queued behind the bridge lock could be lost
            # after a stale PASS archive.
            session = self.ledger.session(handle.session_id)
            cleanup_recovery_used = bool(
                session.get("cleanup_recovery_used")
            )
            if self.stream_terminal_barrier is not None and not cleanup_recovery_used:
                # From the instant the terminal reader barrier is requested,
                # timeout/exception is an ambiguous cutover: its in-band ACK
                # may still arrive and stop the sole socket reader just after
                # this caller times out.  Therefore every barrier exception
                # must take the durable REST-only recovery path.
                barrier_crossed = True
                barrier = dict(self.stream_terminal_barrier())
                if (
                    barrier.get("barrierClosed") is not True
                    or barrier.get("readerJoined") is not True
                    or barrier.get("inBandMarkerReceived") is not True
                    or not _text(barrier.get("terminalMarkerId"))
                ):
                    raise BinanceSpotLifecycleError(
                        "terminal inbound stream barrier is incomplete"
                    )
            truth, rules = self._truth(
                handle,
                cleanup_only=cleanup_recovery_used,
            )
            final_authority = self.control.authority_snapshot()
            if _text(final_authority.get("authorityRevision")) != (
                f"binance-functional-control-{int(reset['revision'])}"
            ):
                raise BinanceSpotLifecycleError(
                    "final-reset authority revision changed during truth read"
                )
            result = self.service.finalize(
                handle.session_id,
                handle.capability,
                permit_payload,
                truth,
                rules,
                prepare_only=True,
            )
        except Exception as exc:
            if barrier_crossed:
                if self.stream_cleanup_recovery_latcher is None:
                    if not self.allow_mock_lifecycle:
                        # Production construction rejects this graph before
                        # start; retain the original exception for mock-only
                        # lifecycle tests that do not own a real stream.
                        raise BinanceSpotLifecycleError(
                            "terminal barrier failure lacks REST cleanup recovery latch"
                        ) from exc
                else:
                    self.stream_cleanup_recovery_latcher(
                        session_id=handle.session_id,
                        detail=(
                            "terminal barrier crossed before final seal failure:"
                            f"{type(exc).__name__}:{str(exc)[:240]}"
                        ),
                    )
                    self.ledger.set_session(
                        handle.session_id,
                        state="CLEANUP",
                        cleanup_started=True,
                        cleanup_recovery_used=True,
                        detail=(
                            "terminal stream stopped; durable REST-only "
                            "cleanup recovery required"
                        ),
                    )
            self.control.restore_cleanup_after_failed_final(
                detail=f"final seal blocked:{type(exc).__name__}:{str(exc)[:300]}"
            )
            raise
        # Do not make the route reusable before the exact session stream is
        # durably archived.  Failure here deliberately leaves FINAL_RESET +
        # core FINALIZED for resume_final_reset() after process restart.
        if self.stream_terminal_retirer is not None:
            recovered = bool(
                result["evidence"].get(
                    "privateStreamGapRecoveredCleanupOnly"
                )
            )
            retired = self.stream_terminal_retirer(
                session_id=handle.session_id,
                permit_id=_text(result["evidence"]["permitId"]),
                permit_hash=_text(result["evidence"]["permitHash"]),
                final_evidence_hash=_text(result["evidenceHash"]),
                terminal_reason=(
                    "RECOVERED_FINALIZED" if recovered else "FINALIZED"
                ),
                expected_journal_seal_hash=_text(
                    result["evidence"].get("streamJournalSealHash")
                ),
            )
            if (
                retired.get("retired") is not True
                or _text(retired.get("sessionId")) != handle.session_id
                or _text(retired.get("finalEvidenceHash")).lower()
                != _text(result["evidenceHash"]).lower()
            ):
                raise BinanceSpotLifecycleError(
                    "durable stream retirement attestation changed"
                )
        if self.permit_store is not None:
            self.permit_store.consume(session_id=handle.session_id)
        self.ledger.commit_prepared_final(
            handle.session_id, now_epoch=float(self.clock())
        )
        self.control.seal_final()
        return {**result, "status": "FINALIZED"}

    def status(self) -> dict[str, object]:
        return self.control.status()


def production_entrypoint_status() -> dict[str, object]:
    """Server-safe status endpoint; starting production remains impossible."""

    return {
        "available": composite_production_available(),
        "route": ROUTE_KEY,
        "reason": (
            "production remains gated: ordinary Binance Spot/Futures final "
            "mutation isolation needs explicit approval, activation-relative "
            "exact-two-hour E2E is unproven, and live credential E2E has not run"
        ),
        "ordinaryLiveRouteChanged": False,
        "smokeRouteChanged": False,
        "components": {
            "core": CORE_PRODUCTION_AVAILABLE,
            "mutation": __import__(
                "live_trader.binance_spot_functional_mutation",
                fromlist=["PRODUCTION_MUTATION_AVAILABLE"],
            ).PRODUCTION_MUTATION_AVAILABLE,
            "lifecycle": PRODUCTION_LIFECYCLE_AVAILABLE,
            "streamJournal": PRODUCTION_STREAM_JOURNAL_AVAILABLE,
            "signalScheduler": PRODUCTION_SIGNAL_SCHEDULER_AVAILABLE,
            "startupRecovery": PRODUCTION_STARTUP_RECOVERY_AVAILABLE,
            "stateServerWiring": PRODUCTION_STATE_SERVER_WIRING_AVAILABLE,
            "ordinaryBinanceFinalMutationIsolation": False,
            "activationRelativeTwoHourE2E": False,
            "credentialedLiveE2E": False,
        },
    }


def build_binance_spot_production_lifecycle(
    *,
    database_path: str | Path,
    binding_reader: Callable[[], Mapping[str, Any]],
    publication_proof_path: str | Path,
    account_fingerprint: str,
    stream_reader: Callable[[], Mapping[str, Any]],
    stream_owner_binder: Callable[[str, str, str, str], None],
    stream_terminal_barrier: Callable[[], Mapping[str, Any]] | None = None,
    stream_cleanup_recovery_latcher: Callable[..., Mapping[str, Any]] | None = None,
    stream_startup_recovery_latcher: Callable[..., Mapping[str, Any]] | None = None,
    stream_terminal_retirer: Callable[..., Mapping[str, Any]] | None = None,
    dispatch_lease_factory: Callable[..., Any] | None = None,
    permit_approval_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
    signal_reader: Callable[[], Mapping[str, Any] | None] | None = None,
    startup_owner_process_absence_attested: bool = False,
    activation_permit_issuer: Callable[
        [object, float], Mapping[str, Any]
    ] | None = None,
    clock: Callable[[], float] = time.time,
) -> BinanceSpotFunctionalLifecycleManager:
    """Build the exact production graph without enabling or starting it.

    State/server code can own this single graph once durable stream journaling
    and red-team E2E are complete.  Until then ``start`` fails before any REST
    read or mutation because the manager is created with production disabled.
    """

    from .binance_spot_functional_mutation import (
        BinanceSpotFunctionalMutationEdge,
    )
    from .binance_spot_functional_transport import (
        BinanceSpotOfficialTruthReader,
        OfficialBinanceSpotGetClient,
    )
    from .binance_spot_publication import verify_binance_spot_publication
    from .binance_spot_functional_strategy import (
        BinanceSpotOfficialNaturalSignalReader,
        OfficialBinanceSpotFinalizedKlineReader,
        SealedBinanceSpotMovingAverageEvaluator,
    )

    if signal_reader is not None:
        raise BinanceSpotLifecycleError(
            "production graph rejects injected bar/signal readers"
        )
    if stream_terminal_retirer is None:
        raise BinanceSpotLifecycleError(
            "production graph requires an exact durable stream terminal retirer"
        )
    if stream_terminal_barrier is None:
        raise BinanceSpotLifecycleError(
            "production graph requires an inbound stream terminal barrier"
        )
    if stream_cleanup_recovery_latcher is None:
        raise BinanceSpotLifecycleError(
            "production graph requires a terminal-failure cleanup recovery latch"
        )
    if stream_startup_recovery_latcher is None:
        raise BinanceSpotLifecycleError(
            "production graph requires a startup owner-loss stream-gap latch"
        )
    if dispatch_lease_factory is None:
        raise BinanceSpotLifecycleError(
            "production graph requires a cross-route dispatch lease"
        )
    if activation_permit_issuer is None:
        raise BinanceSpotLifecycleError(
            "production graph requires a server-owned activation permit resealer"
        )

    ledger = DurableFunctionalLedger(database_path)
    control = DurableBinanceSpotFunctionalControl(database_path, clock=clock)
    permit_store = DurableBinanceSpotApprovedPermitStore(
        database_path,
        approval_verifier=permit_approval_verifier,
        clock=clock,
    )
    publication_verifier = lambda binding: verify_binance_spot_publication(
        binding, proof_path=publication_proof_path
    )
    service = BinanceSpotContinuousFunctionalService(
        ledger=ledger,
        binding_reader=binding_reader,
        authority_reader=control.authority_snapshot,
        publication_verifier=publication_verifier,
        clock=clock,
    )
    client = OfficialBinanceSpotGetClient(
        expected_account_fingerprint=account_fingerprint,
        clock=clock,
    )
    truth_reader = BinanceSpotOfficialTruthReader(
        client=client,
        account_fingerprint=account_fingerprint,
        stream_reader=stream_reader,
        clock=clock,
    )
    return BinanceSpotFunctionalLifecycleManager(
        ledger=ledger,
        control=control,
        service=service,
        truth_reader=truth_reader,
        mutation_edge=BinanceSpotFunctionalMutationEdge(
            authority_reader=control.authority_snapshot,
            claim_reader=ledger.action,
            claim_marker=lambda claim_id: ledger.mark_post_may_have_crossed(
                claim_id, now_epoch=float(clock())
            ),
            dispatch_lease_factory=dispatch_lease_factory,
        ),
        permit_store=permit_store,
        signal_reader=BinanceSpotOfficialNaturalSignalReader(
            kline_reader=OfficialBinanceSpotFinalizedKlineReader(
                client=client,
                clock=clock,
            ),
            evaluator=SealedBinanceSpotMovingAverageEvaluator(
                binding_reader=binding_reader,
                publication_verifier=publication_verifier,
            ),
        ),
        stream_owner_binder=stream_owner_binder,
        stream_terminal_barrier=stream_terminal_barrier,
        stream_cleanup_recovery_latcher=stream_cleanup_recovery_latcher,
        stream_startup_recovery_latcher=stream_startup_recovery_latcher,
        stream_terminal_retirer=stream_terminal_retirer,
        startup_owner_process_absence_attested=(
            startup_owner_process_absence_attested
        ),
        activation_permit_issuer=activation_permit_issuer,
        clock=clock,
        allow_mock_lifecycle=False,
    )


__all__ = [
    "BinanceSpotFunctionalLifecycleManager",
    "BinanceSpotLifecycleError",
    "DurableBinanceSpotFunctionalControl",
    "LifecycleHandle",
    "MAX_OWNER_LEASE_SECONDS",
    "PRODUCTION_LIFECYCLE_AVAILABLE",
    "ROUTE_KEY",
    "build_binance_spot_production_lifecycle",
    "composite_production_available",
    "production_entrypoint_status",
]
