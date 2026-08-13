from __future__ import annotations

"""Durable Binance Spot user-data evidence journal.

The stream writer and managed lifecycle reader may be different processes.  A
reader trusts the journal only while the exact writer maintains a fresh
heartbeat and no disconnect/parser/queue gap was recorded.  Reconnecting after
the functional baseline creates a new subscription epoch and therefore fails
the core's pre-baseline proof instead of pretending continuity.
"""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping
import uuid

from .binance_spot_functional_transport import (
    BinanceSpotTruthError,
    normalize_binance_user_stream_event,
)


WRITER_HEARTBEAT_MAX_AGE_SECONDS = 5.0
MAX_DURABLE_EVENTS = 20_000
ROUTE_KEY = "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m"
_OWNER_PREFIX = re.compile(r"^ftb-[0-9a-f]{12}-$")


def _text(value: object) -> str:
    return str(value or "").strip()


def _iso(epoch: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _token_hash(value: str) -> str:
    return hashlib.sha256(_text(value).encode("utf-8")).hexdigest()


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _journal_seal(row: Mapping[str, Any], events: list[Mapping[str, Any]]) -> str:
    """Hash only continuity/account-event state, not benign pong timestamps."""

    material = {
        "routeKey": ROUTE_KEY,
        "accountFingerprint": _text(row.get("account_fingerprint")),
        "writerId": _text(row.get("writer_id")),
        "ownerPrefix": _text(row.get("owner_prefix")),
        "sessionId": _text(row.get("session_id")),
        "permitId": _text(row.get("permit_id")),
        "permitHash": _text(row.get("permit_hash")).lower(),
        "subscribedEpoch": float(row.get("subscribed_epoch") or 0),
        "connected": bool(row.get("connected")),
        "authenticated": bool(row.get("authenticated")),
        "gapDetected": bool(row.get("gap_detected")),
        "externalActivityAbsent": bool(row.get("external_activity_absent")),
        "retired": bool(row.get("retired")),
        "terminalMarkerId": _text(row.get("terminal_marker_id")),
        "terminalMarkerServerEpoch": float(
            row.get("terminal_marker_server_epoch") or 0
        ),
        "terminalMarkerEpoch": float(row.get("terminal_marker_epoch") or 0),
        "events": [
            {
                "eventId": _text(item.get("event_id")),
                "eventEpoch": float(item.get("event_epoch") or 0),
                "payloadJson": _text(item.get("payload_json")),
            }
            for item in events
        ],
    }
    return hashlib.sha256(_canonical(material).encode("utf-8")).hexdigest()


class DurableBinanceSpotUserStreamJournal:
    def __init__(
        self,
        path: str | Path,
        *,
        account_fingerprint: str,
        clock: Callable[[], float] = time.time,
        terminal_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> None:
        fingerprint = _text(account_fingerprint).lower()
        if len(fingerprint) != 64 or any(
            character not in "0123456789abcdef" for character in fingerprint
        ):
            raise BinanceSpotTruthError("stream account fingerprint must be SHA-256")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.account_fingerprint = fingerprint
        self.clock = clock
        self.terminal_verifier = terminal_verifier or (lambda _: False)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_meta (
                    route_key TEXT PRIMARY KEY,
                    account_fingerprint TEXT NOT NULL,
                    writer_id TEXT NOT NULL,
                    writer_token_hash TEXT NOT NULL,
                    owner_prefix TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    subscribed_epoch REAL NOT NULL,
                    observed_epoch REAL NOT NULL,
                    heartbeat_epoch REAL NOT NULL,
                    connected INTEGER NOT NULL,
                    authenticated INTEGER NOT NULL,
                    gap_detected INTEGER NOT NULL,
                    external_activity_absent INTEGER NOT NULL,
                    retired INTEGER NOT NULL DEFAULT 0,
                    retired_epoch REAL NOT NULL DEFAULT 0,
                    retirement_evidence_hash TEXT NOT NULL DEFAULT '',
                    terminal_marker_id TEXT NOT NULL DEFAULT '',
                    terminal_marker_server_epoch REAL NOT NULL DEFAULT 0,
                    terminal_marker_epoch REAL NOT NULL DEFAULT 0,
                    detail TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_events (
                    route_key TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_epoch REAL NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(route_key, event_id)
                );
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_archives (
                    archive_id TEXT PRIMARY KEY,
                    route_key TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    permit_id TEXT NOT NULL,
                    permit_hash TEXT NOT NULL,
                    final_evidence_hash TEXT NOT NULL,
                    retired_epoch REAL NOT NULL,
                    meta_json TEXT NOT NULL,
                    archive_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS binance_spot_stream_journal_archive_events (
                    archive_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_epoch REAL NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(archive_id, event_id),
                    FOREIGN KEY(archive_id)
                        REFERENCES binance_spot_stream_journal_archives(archive_id)
                );
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(binance_spot_stream_journal_meta)"
                ).fetchall()
            }
            for name, declaration in {
                "retired": "INTEGER NOT NULL DEFAULT 0",
                "retired_epoch": "REAL NOT NULL DEFAULT 0",
                "retirement_evidence_hash": "TEXT NOT NULL DEFAULT ''",
                "terminal_marker_id": "TEXT NOT NULL DEFAULT ''",
                "terminal_marker_server_epoch": "REAL NOT NULL DEFAULT 0",
                "terminal_marker_epoch": "REAL NOT NULL DEFAULT 0",
            }.items():
                if name not in columns:
                    connection.execute(
                        "ALTER TABLE binance_spot_stream_journal_meta "
                        f"ADD COLUMN {name} {declaration}"
                    )
            connection.commit()

    def _writer_row(
        self,
        connection: sqlite3.Connection,
        *,
        writer_id: str,
        writer_token: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
            (ROUTE_KEY,),
        ).fetchone()
        if (
            row is None
            or _text(row["account_fingerprint"]) != self.account_fingerprint
            or _text(row["writer_id"]) != _text(writer_id)
            or bool(row["retired"])
            or not secrets.compare_digest(
                _text(row["writer_token_hash"]), _token_hash(writer_token)
            )
        ):
            raise BinanceSpotTruthError("stream writer identity/token changed")
        return row

    def begin_authenticated_subscription(
        self,
        *,
        writer_id: str,
        owner_prefix: str,
        subscribed_epoch: float,
    ) -> str:
        now = float(self.clock())
        subscribed = float(subscribed_epoch)
        if (
            not _text(writer_id)
            or _text(owner_prefix)
            or subscribed <= 0
            or subscribed > now + 1
        ):
            raise BinanceSpotTruthError("stream writer/subscription identity is invalid")
        token = secrets.token_urlsafe(32)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if prior is not None:
                if _text(prior["account_fingerprint"]) != self.account_fingerprint:
                    connection.rollback()
                    raise BinanceSpotTruthError(
                        "stream account fingerprint changed"
                    )
                prior_bound = any(
                    _text(prior[field])
                    for field in (
                        "owner_prefix",
                        "session_id",
                        "permit_id",
                        "permit_hash",
                    )
                )
                prior_writer_fresh = (
                    bool(prior["connected"])
                    and -1.0
                    <= now - float(prior["heartbeat_epoch"])
                    <= WRITER_HEARTBEAT_MAX_AGE_SECONDS
                )
                prior_retired = bool(prior["retired"])
                if prior_bound and not prior_retired:
                    connection.rollback()
                    raise BinanceSpotTruthError(
                        "active functional stream binding cannot be replaced or erased"
                    )
                if prior_writer_fresh and not prior_retired:
                    connection.rollback()
                    raise BinanceSpotTruthError(
                        "authenticated stream writer lease is still active"
                    )
            connection.execute(
                "DELETE FROM binance_spot_stream_journal_events WHERE route_key=?",
                (ROUTE_KEY,),
            )
            connection.execute(
                """
                INSERT INTO binance_spot_stream_journal_meta (
                    route_key, account_fingerprint, writer_id,
                    writer_token_hash, owner_prefix, session_id, permit_id,
                    permit_hash, subscribed_epoch,
                    observed_epoch, heartbeat_epoch, connected,
                    authenticated, gap_detected, external_activity_absent,
                    retired, retired_epoch, retirement_evidence_hash, detail
                ) VALUES (?, ?, ?, ?, ?, '', '', '', ?, ?, ?, 1, 1, 0, 1, 0, 0, '', ?)
                ON CONFLICT(route_key) DO UPDATE SET
                    account_fingerprint=excluded.account_fingerprint,
                    writer_id=excluded.writer_id,
                    writer_token_hash=excluded.writer_token_hash,
                    owner_prefix=excluded.owner_prefix,
                    session_id='', permit_id='', permit_hash='',
                    subscribed_epoch=excluded.subscribed_epoch,
                    observed_epoch=excluded.observed_epoch,
                    heartbeat_epoch=excluded.heartbeat_epoch,
                    connected=1, authenticated=1, gap_detected=0,
                    external_activity_absent=1, retired=0, retired_epoch=0,
                    retirement_evidence_hash='', terminal_marker_id='',
                    terminal_marker_server_epoch=0, terminal_marker_epoch=0,
                    detail=excluded.detail
                """,
                (
                    ROUTE_KEY,
                    self.account_fingerprint,
                    _text(writer_id),
                    _token_hash(token),
                    "",
                    subscribed,
                    now,
                    now,
                    "authenticated subscription began; event journal reset",
                ),
            )
            connection.commit()
        return token

    def retire_terminal_session(
        self,
        *,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        final_evidence_hash: str,
        terminal_reason: str = "FINALIZED",
        expected_journal_seal_hash: str = "",
    ) -> dict[str, Any]:
        """Archive a terminal binding without erasing its historical proof.

        A backend-owned verifier must attest the corresponding core/control/
        approval terminal state.  The active route is only made reusable after
        the immutable metadata and every event have been copied to archive
        tables in the same transaction.
        """

        normalized_hash = _text(permit_hash).lower()
        evidence_hash = _text(final_evidence_hash).lower()
        expected_seal = _text(expected_journal_seal_hash).lower()
        attestation = {
            "routeKey": ROUTE_KEY,
            "accountFingerprint": self.account_fingerprint,
            "sessionId": _text(session_id),
            "permitId": _text(permit_id),
            "permitHash": normalized_hash,
            "finalEvidenceHash": evidence_hash,
            "terminalReason": _text(terminal_reason).upper(),
        }
        if (
            not attestation["sessionId"].startswith("bnsft-")
            or not attestation["permitId"].startswith("functional-test-")
            or len(normalized_hash) != 64
            or len(evidence_hash) != 64
            or any(ch not in "0123456789abcdef" for ch in normalized_hash + evidence_hash)
            or attestation["terminalReason"] not in {
                "FINALIZED",
                "RECOVERED_FINALIZED",
                "START_FAILED",
            }
            or (
                attestation["terminalReason"]
                in {"FINALIZED", "RECOVERED_FINALIZED"}
                and (
                    len(expected_seal) != 64
                    or any(ch not in "0123456789abcdef" for ch in expected_seal)
                )
            )
            or self.terminal_verifier(attestation) is not True
        ):
            raise BinanceSpotTruthError(
                "durable core/control/approval terminal attestation is absent"
            )
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise BinanceSpotTruthError("terminal stream binding is missing")
            if bool(row["retired"]):
                if (
                    _text(row["session_id"]) == attestation["sessionId"]
                    and _text(row["retirement_evidence_hash"]) == evidence_hash
                ):
                    connection.commit()
                    return {
                        "retired": True,
                        "sessionId": attestation["sessionId"],
                        "finalEvidenceHash": evidence_hash,
                    }
                connection.rollback()
                raise BinanceSpotTruthError("stream retirement identity changed")
            bound_identity = (
                _text(row["owner_prefix"]),
                _text(row["session_id"]),
                _text(row["permit_id"]),
                _text(row["permit_hash"]).lower(),
            )
            expected_owner_prefix = (
                f"ftb-{hashlib.sha256(attestation['sessionId'].encode()).hexdigest()[:12]}-"
            )
            exact_identity = (
                expected_owner_prefix,
                attestation["sessionId"],
                attestation["permitId"],
                normalized_hash,
            )
            startup_unbound = (
                attestation["terminalReason"] == "START_FAILED"
                and bound_identity == ("", "", "", "")
            )
            if (
                _text(row["account_fingerprint"]) != self.account_fingerprint
                or (bound_identity != exact_identity and not startup_unbound)
            ):
                connection.rollback()
                raise BinanceSpotTruthError("terminal stream binding identity changed")
            meta = dict(row)
            if startup_unbound:
                # service.start committed before stream_owner_binder.  The
                # official startup-abort proof shows no action/balance change;
                # archive the original prebaseline epoch without pretending it
                # was ever an active functional stream.
                meta["startupUnboundRetirement"] = True
            event_rows = connection.execute(
                """
                SELECT event_id, event_epoch, payload_json
                FROM binance_spot_stream_journal_events
                WHERE route_key=? ORDER BY event_epoch, event_id
                """,
                (ROUTE_KEY,),
            ).fetchall()
            current_seal = _journal_seal(
                dict(row), [dict(item) for item in event_rows]
            )
            gapless_final = attestation["terminalReason"] == "FINALIZED"
            recovered_final = (
                attestation["terminalReason"] == "RECOVERED_FINALIZED"
            )
            invalid_final_cutoff = not secrets.compare_digest(
                current_seal, expected_seal
            ) or (
                gapless_final
                and (
                    not bool(row["connected"])
                    or not bool(row["authenticated"])
                    or bool(row["gap_detected"])
                    or not bool(row["external_activity_absent"])
                    or not _text(row["terminal_marker_id"])
                    or float(row["terminal_marker_server_epoch"] or 0) <= 0
                )
            ) or (recovered_final and not bool(row["gap_detected"]))
            if (
                attestation["terminalReason"]
                in {"FINALIZED", "RECOVERED_FINALIZED"}
                and invalid_final_cutoff
            ):
                connection.rollback()
                raise BinanceSpotTruthError(
                    "terminal stream changed after final truth cutoff"
                )
            archive_material = {
                "attestation": attestation,
                "meta": meta,
                "events": [dict(item) for item in event_rows],
            }
            archive_hash = hashlib.sha256(
                json.dumps(
                    archive_material,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            ).hexdigest()
            archive_id = "bssja-" + archive_hash[:32]
            connection.execute(
                """
                INSERT INTO binance_spot_stream_journal_archives (
                    archive_id, route_key, account_fingerprint, session_id,
                    permit_id, permit_hash, final_evidence_hash,
                    retired_epoch, meta_json, archive_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    archive_id,
                    ROUTE_KEY,
                    self.account_fingerprint,
                    attestation["sessionId"],
                    attestation["permitId"],
                    normalized_hash,
                    evidence_hash,
                    now,
                    _canonical(meta),
                    archive_hash,
                ),
            )
            for event in event_rows:
                connection.execute(
                    """
                    INSERT INTO binance_spot_stream_journal_archive_events (
                        archive_id, event_id, event_epoch, payload_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        archive_id,
                        event["event_id"],
                        event["event_epoch"],
                        event["payload_json"],
                    ),
                )
            connection.execute(
                "DELETE FROM binance_spot_stream_journal_events WHERE route_key=?",
                (ROUTE_KEY,),
            )
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET connected=0, authenticated=0, writer_token_hash='',
                    retired=1, retired_epoch=?, retirement_evidence_hash=?,
                    owner_prefix=?, session_id=?, permit_id=?, permit_hash=?,
                    detail=?
                WHERE route_key=?
                """,
                (
                    now,
                    evidence_hash,
                    expected_owner_prefix,
                    attestation["sessionId"],
                    attestation["permitId"],
                    normalized_hash,
                    f"terminal stream archived:{attestation['terminalReason']}",
                    ROUTE_KEY,
                ),
            )
            connection.commit()
        return {
            "retired": True,
            "archiveId": archive_id,
            "archiveHash": archive_hash,
            "sessionId": attestation["sessionId"],
            "finalEvidenceHash": evidence_hash,
        }

    def heartbeat(self, *, writer_id: str, writer_token: str) -> None:
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._writer_row(
                connection, writer_id=writer_id, writer_token=writer_token
            )
            if int(row["gap_detected"]) or not int(row["connected"]):
                connection.rollback()
                raise BinanceSpotTruthError(
                    "stream writer heartbeat cannot clear a durable gap"
                )
            if now - float(row["heartbeat_epoch"]) > WRITER_HEARTBEAT_MAX_AGE_SECONDS:
                connection.execute(
                    """
                    UPDATE binance_spot_stream_journal_meta
                    SET connected=0, gap_detected=1,
                        external_activity_absent=0, observed_epoch=?,
                        detail='writer heartbeat deadline missed; sticky gap'
                    WHERE route_key=?
                    """,
                    (now, ROUTE_KEY),
                )
                connection.commit()
                raise BinanceSpotTruthError(
                    "stream writer heartbeat deadline was missed"
                )
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET heartbeat_epoch=?, observed_epoch=?
                WHERE route_key=?
                """,
                (now, now, ROUTE_KEY),
            )
            connection.commit()

    def record_terminal_marker(
        self,
        *,
        writer_id: str,
        writer_token: str,
        marker_id: str,
        server_time_ms: int,
    ) -> dict[str, Any]:
        """Seal a reader-owned in-band response on the authenticated socket.

        Binance WebSocket API responses are ordered on the same connection.
        The marker therefore proves that every frame ahead of its response was
        received by the sole reader before terminal intake is closed.  Merely
        sending a ping, or holding a process-local lock, is not such proof.
        """

        marker = _text(marker_id)
        now = float(self.clock())
        try:
            server_epoch = int(server_time_ms) / 1000.0
        except (TypeError, ValueError) as exc:
            raise BinanceSpotTruthError(
                "terminal stream marker server time is invalid"
            ) from exc
        if (
            len(marker) < 24
            or len(marker) > 100
            or not re.fullmatch(r"[A-Za-z0-9._:-]+", marker)
            or server_epoch <= 0
            or abs(now - server_epoch) > 30.0
        ):
            raise BinanceSpotTruthError(
                "terminal stream marker identity/time is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._writer_row(
                connection, writer_id=writer_id, writer_token=writer_token
            )
            if (
                not int(row["connected"])
                or not int(row["authenticated"])
                or int(row["gap_detected"])
                or not _text(row["session_id"])
            ):
                connection.rollback()
                raise BinanceSpotTruthError(
                    "terminal marker requires a gapless bound stream"
                )
            existing = _text(row["terminal_marker_id"])
            if existing and existing != marker:
                connection.rollback()
                raise BinanceSpotTruthError(
                    "terminal stream marker was already sealed"
                )
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET terminal_marker_id=?, terminal_marker_server_epoch=?,
                    terminal_marker_epoch=?, observed_epoch=?,
                    heartbeat_epoch=?, detail=?
                WHERE route_key=?
                """,
                (
                    marker,
                    server_epoch,
                    now,
                    now,
                    now,
                    "authenticated in-band terminal marker received",
                    ROUTE_KEY,
                ),
            )
            connection.commit()
        return {
            "terminalMarkerAcknowledged": True,
            "terminalMarkerId": marker,
            "terminalMarkerServerEpoch": server_epoch,
            "terminalMarkerEpoch": now,
        }

    def mark_bound_owner_loss_gap(
        self, *, session_id: str, detail: str
    ) -> dict[str, Any]:
        """One-way restart/final-barrier failure latch without a lost token."""

        normalized_session = _text(session_id)
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            if (
                row is None
                or bool(row["retired"])
                or _text(row["session_id"]) != normalized_session
                or not _text(row["permit_id"])
                or not _text(row["permit_hash"])
            ):
                connection.rollback()
                raise BinanceSpotTruthError(
                    "owner-loss stream gap identity is not the bound session"
                )
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET connected=0, gap_detected=1,
                    external_activity_absent=0, writer_token_hash='',
                    terminal_marker_id='', terminal_marker_server_epoch=0,
                    terminal_marker_epoch=0, observed_epoch=?,
                    heartbeat_epoch=?, detail=?
                WHERE route_key=?
                """,
                (now, now, _text(detail)[:500], ROUTE_KEY),
            )
            connection.commit()
        return {
            "cleanupRecoveryOnly": True,
            "preservedStreamGap": True,
            "sessionId": normalized_session,
        }

    def bind_functional_session(
        self,
        owner_prefix: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        *,
        writer_id: str,
        writer_token: str,
    ) -> None:
        """Bind the session prefix once without resetting the pre-baseline stream."""

        prefix = _text(owner_prefix)
        normalized_session = _text(session_id)
        normalized_permit = _text(permit_id)
        normalized_hash = _text(permit_hash).lower()
        expected_prefix = (
            f"ftb-{hashlib.sha256(normalized_session.encode()).hexdigest()[:12]}-"
        )
        if (
            _OWNER_PREFIX.fullmatch(prefix) is None
            or prefix != expected_prefix
            or not normalized_session.startswith("bnsft-")
            or not normalized_permit.startswith("functional-test-")
            or len(normalized_hash) != 64
            or any(character not in "0123456789abcdef" for character in normalized_hash)
        ):
            raise BinanceSpotTruthError(
                "stream session/permit/owner binding is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._writer_row(
                connection, writer_id=writer_id, writer_token=writer_token
            )
            existing = _text(row["owner_prefix"])
            existing_identity = (
                _text(row["session_id"]),
                _text(row["permit_id"]),
                _text(row["permit_hash"]),
            )
            requested_identity = (
                normalized_session,
                normalized_permit,
                normalized_hash,
            )
            if existing not in {"", prefix} or existing_identity not in {
                ("", "", ""),
                requested_identity,
            }:
                connection.rollback()
                raise BinanceSpotTruthError(
                    "stream functional identity was already bound"
                )
            if existing == "":
                external = not bool(row["external_activity_absent"])
                execution_count = 0
                for item in connection.execute(
                    "SELECT payload_json FROM binance_spot_stream_journal_events WHERE route_key=?",
                    (ROUTE_KEY,),
                ).fetchall():
                    payload = json.loads(item["payload_json"])
                    execution_count += payload.get("eventType") == "executionReport"
                if external or execution_count:
                    connection.rollback()
                    raise BinanceSpotTruthError(
                        "owner prefix cannot bind after pre-session account activity"
                    )
                connection.execute(
                    """
                    UPDATE binance_spot_stream_journal_meta
                    SET owner_prefix=?, session_id=?, permit_id=?, permit_hash=?,
                        detail='exact session/permit/owner prefix bound'
                    WHERE route_key=?
                    """,
                    (
                        prefix,
                        normalized_session,
                        normalized_permit,
                        normalized_hash,
                        ROUTE_KEY,
                    ),
                )
            connection.commit()

    def ingest(
        self,
        payload: Mapping[str, Any],
        *,
        writer_id: str,
        writer_token: str,
    ) -> dict[str, Any]:
        event = normalize_binance_user_stream_event(payload)
        event_id = _text(event.get("eventId"))
        event_epoch = float(event.get("eventTime") or 0) / 1000.0
        now = float(self.clock())
        if not event_id or event_epoch <= 0 or event_epoch > now + 1:
            raise BinanceSpotTruthError("durable stream event id/time is invalid")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = self._writer_row(
                connection, writer_id=writer_id, writer_token=writer_token
            )
            if (
                not int(row["connected"])
                or not int(row["authenticated"])
                or int(row["gap_detected"])
            ):
                raise BinanceSpotTruthError("stream writer is not continuous")
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM binance_spot_stream_journal_events WHERE route_key=?",
                    (ROUTE_KEY,),
                ).fetchone()[0]
            )
            if count >= MAX_DURABLE_EVENTS:
                connection.execute(
                    """
                    UPDATE binance_spot_stream_journal_meta
                    SET gap_detected=1, external_activity_absent=0,
                        detail='durable stream journal capacity exceeded'
                    WHERE route_key=?
                    """,
                    (ROUTE_KEY,),
                )
                connection.commit()
                raise BinanceSpotTruthError("durable stream journal capacity exceeded")
            owner_prefix = _text(row["owner_prefix"])
            external = event.get("eventType") == "balanceUpdate" or (
                event.get("eventType") == "executionReport"
                and not (
                    owner_prefix
                    and _text(event.get("clientOrderId")).startswith(owner_prefix)
                )
            )
            try:
                connection.execute(
                    """
                    INSERT INTO binance_spot_stream_journal_events (
                        route_key, event_id, event_epoch, payload_json
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (ROUTE_KEY, event_id, event_epoch, _canonical(event)),
                )
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotTruthError("duplicate durable user-data event") from exc
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET observed_epoch=?, heartbeat_epoch=?,
                    external_activity_absent=(
                        CASE WHEN ? THEN 0 ELSE external_activity_absent END
                    )
                WHERE route_key=?
                """,
                (now, now, 1 if external else 0, ROUTE_KEY),
            )
            connection.commit()
        return event

    def mark_gap(
        self,
        *,
        writer_id: str,
        writer_token: str,
        detail: str,
        disconnected: bool = False,
    ) -> None:
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._writer_row(
                connection, writer_id=writer_id, writer_token=writer_token
            )
            connection.execute(
                """
                UPDATE binance_spot_stream_journal_meta
                SET connected=?, gap_detected=1,
                    external_activity_absent=0, observed_epoch=?,
                    heartbeat_epoch=?, detail=?
                WHERE route_key=?
                """,
                (
                    0 if disconnected else 1,
                    now,
                    now,
                    _text(detail)[:500],
                    ROUTE_KEY,
                ),
            )
            connection.commit()

    def snapshot(self) -> dict[str, Any]:
        now = float(self.clock())
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
                (ROUTE_KEY,),
            ).fetchone()
            events = connection.execute(
                """
                SELECT event_id, event_epoch, payload_json
                FROM binance_spot_stream_journal_events
                WHERE route_key=? ORDER BY event_epoch, event_id
                """,
                (ROUTE_KEY,),
            ).fetchall()
            if row is not None and (
                now - float(row["heartbeat_epoch"])
                > WRITER_HEARTBEAT_MAX_AGE_SECONDS
            ):
                connection.execute(
                    """
                    UPDATE binance_spot_stream_journal_meta
                    SET connected=0, gap_detected=1,
                        external_activity_absent=0,
                        detail='reader observed missed writer heartbeat; sticky gap'
                    WHERE route_key=?
                    """,
                    (ROUTE_KEY,),
                )
                connection.commit()
                row = connection.execute(
                    "SELECT * FROM binance_spot_stream_journal_meta WHERE route_key=?",
                    (ROUTE_KEY,),
                ).fetchone()
            else:
                connection.commit()
        if row is None or _text(row["account_fingerprint"]) != self.account_fingerprint:
            raise BinanceSpotTruthError("durable stream subscription is missing")
        if bool(row["retired"]):
            raise BinanceSpotTruthError("durable stream session is terminally retired")
        heartbeat_fresh = (
            -1.0
            <= now - float(row["heartbeat_epoch"])
            <= WRITER_HEARTBEAT_MAX_AGE_SECONDS
        )
        connected = bool(row["connected"]) and heartbeat_fresh
        gap = bool(row["gap_detected"]) or not heartbeat_fresh
        event_dicts = [dict(item) for item in events]
        journal_seal = _journal_seal(dict(row), event_dicts)
        return {
            "connected": connected,
            "authenticated": bool(row["authenticated"]) and heartbeat_fresh,
            "sequenceComplete": not gap,
            "gapDetected": gap,
            "subscribedAt": _iso(float(row["subscribed_epoch"])),
            "observedAt": _iso(float(row["observed_epoch"])),
            "externalActivityAbsent": (
                bool(row["external_activity_absent"]) and not gap
            ),
            "events": [json.loads(item["payload_json"]) for item in events],
            "sessionId": _text(row["session_id"]),
            "permitId": _text(row["permit_id"]),
            "permitHash": _text(row["permit_hash"]),
            "writerHeartbeatFresh": heartbeat_fresh,
            "durableJournal": True,
            "durableJournalEventCount": len(events),
            "durableJournalSealHash": journal_seal,
            "terminalMarkerAcknowledged": bool(
                _text(row["terminal_marker_id"])
            ),
            "terminalMarkerId": _text(row["terminal_marker_id"]),
            "terminalMarkerServerEpoch": float(
                row["terminal_marker_server_epoch"] or 0
            ),
            "terminalMarkerEpoch": float(row["terminal_marker_epoch"] or 0),
        }


class BinanceSpotDurableStreamBridge:
    """Optional hook consumed by the existing Binance execution stream."""

    def __init__(
        self,
        journal: DurableBinanceSpotUserStreamJournal,
        *,
        writer_id: str = "",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.journal = journal
        self.writer_id = _text(writer_id) or f"binance-stream-{uuid.uuid4().hex}"
        self.clock = clock
        self._token = ""
        self._lock = threading.RLock()
        self._drained = threading.Condition(self._lock)
        self._accepting_inbound = False
        self._terminal_barrier = False
        self._inflight_received = 0

    def on_subscription_confirmed(self) -> None:
        with self._lock:
            self._token = self.journal.begin_authenticated_subscription(
                writer_id=self.writer_id,
                owner_prefix="",
                subscribed_epoch=float(self.clock()),
            )
            self._accepting_inbound = True
            self._terminal_barrier = False
            self._inflight_received = 0

    def begin_inbound_frame(self) -> str:
        """Fence a frame immediately after socket recv, before parsing."""

        with self._lock:
            if not self._token or not self._accepting_inbound:
                raise BinanceSpotTruthError(
                    "functional stream intake is not accepting inbound frames"
                )
            self._inflight_received += 1
            nonce = str(uuid.uuid4()).replace("-", "")
            return f"{self.writer_id}:{self._inflight_received}:{nonce}"

    def finish_inbound_frame(self, ticket: str) -> None:
        with self._lock:
            if not _text(ticket).startswith(f"{self.writer_id}:"):
                raise BinanceSpotTruthError("inbound frame fence ticket changed")
            if self._inflight_received <= 0:
                raise BinanceSpotTruthError("inbound frame fence underflow")
            self._inflight_received -= 1
            if self._inflight_received == 0:
                self._drained.notify_all()

    def close_terminal_intake(self, *, timeout_seconds: float = 5.0) -> None:
        """Stop new frames and wait for every already-received callback."""

        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        with self._lock:
            if not self._token:
                raise BinanceSpotTruthError(
                    "terminal inbound barrier has no authenticated writer"
                )
            self._accepting_inbound = False
            self._terminal_barrier = True
            while self._inflight_received:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BinanceSpotTruthError(
                        "terminal inbound callbacks did not drain"
                    )
                self._drained.wait(remaining)

    def on_terminal_marker(
        self, *, marker_id: str, server_time_ms: int
    ) -> dict[str, Any]:
        """Persist the exact in-band ACK before the reader closes intake."""

        with self._lock:
            if not self._token or not self._accepting_inbound:
                raise BinanceSpotTruthError(
                    "terminal marker arrived without an active writer"
                )
            return self.journal.record_terminal_marker(
                writer_id=self.writer_id,
                writer_token=self._token,
                marker_id=marker_id,
                server_time_ms=server_time_ms,
            )

    def latch_terminal_failure_cleanup(
        self, *, session_id: str, detail: str
    ) -> dict[str, Any]:
        """Convert a stopped terminal stream into durable REST-only cleanup."""

        with self._lock:
            result = self.journal.mark_bound_owner_loss_gap(
                session_id=session_id, detail=detail
            )
            self._token = ""
            self._accepting_inbound = False
            self._terminal_barrier = False
            return result

    def bind_functional_session(
        self,
        owner_prefix: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
    ) -> None:
        with self._lock:
            if not self._token:
                raise BinanceSpotTruthError(
                    "stream subscription is not confirmed before owner binding"
                )
            self.journal.bind_functional_session(
                owner_prefix,
                session_id,
                permit_id,
                permit_hash,
                writer_id=self.writer_id,
                writer_token=self._token,
            )

    def on_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if not self._token:
                raise BinanceSpotTruthError(
                    "stream payload arrived before subscription confirmation"
                )
            return self.journal.ingest(
                payload,
                writer_id=self.writer_id,
                writer_token=self._token,
            )

    def on_transport_liveness(self) -> None:
        """Renew continuity only after an authenticated inbound frame/pong.

        Process-local timers and outbound PING writes are not evidence that a
        half-open socket can still receive account events.
        """

        with self._lock:
            if not self._token:
                return
            self.journal.heartbeat(
                writer_id=self.writer_id,
                writer_token=self._token,
            )

    def on_disconnect(self, detail: str) -> None:
        with self._lock:
            if not self._token:
                return
            if self._terminal_barrier:
                # A state-owned terminal receive fence already stopped new
                # frames and drained all callbacks.  Preserve the last
                # authenticated cursor for final REST/journal seal instead of
                # fabricating a transport gap from the intentional close.
                return
            try:
                self.journal.mark_gap(
                    writer_id=self.writer_id,
                    writer_token=self._token,
                    detail=detail,
                    disconnected=True,
                )
            finally:
                self._token = ""

    def retire_terminal_session(
        self,
        *,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        final_evidence_hash: str,
        terminal_reason: str = "FINALIZED",
        expected_journal_seal_hash: str = "",
    ) -> dict[str, Any]:
        with self._lock:
            result = self.journal.retire_terminal_session(
                session_id=session_id,
                permit_id=permit_id,
                permit_hash=permit_hash,
                final_evidence_hash=final_evidence_hash,
                terminal_reason=terminal_reason,
                expected_journal_seal_hash=expected_journal_seal_hash,
            )
            self._token = ""
            self._accepting_inbound = False
            self._terminal_barrier = False
        return result

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self.journal.snapshot()


__all__ = [
    "BinanceSpotDurableStreamBridge",
    "DurableBinanceSpotUserStreamJournal",
    "MAX_DURABLE_EVENTS",
    "ROUTE_KEY",
    "WRITER_HEARTBEAT_MAX_AGE_SECONDS",
]
