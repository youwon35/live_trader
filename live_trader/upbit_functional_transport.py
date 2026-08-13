from __future__ import annotations

"""GET-only Upbit transport and durable ``myOrder`` evidence journal.

This module is intentionally separate from the ordinary Upbit broker router.
Its HTTP client exposes only six allow-listed read endpoints, while the
SQLite journal records every private order event before it is offered to the
continuous functional lane.  A disconnect, parse failure, credential change,
or conflicting replay permanently marks that journal session incomplete.
"""

import base64
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
from typing import Any, Callable, Mapping, Sequence
import urllib.parse
import uuid

from .live_adapters import PreparedRequest, UPBIT_BASE_URL, env_value, send_prepared_request
from .upbit_continuous_functional import SYMBOL, UpbitFunctionalBlocked
from .upbit_functional_truth import (
    UPBIT_ACCOUNTS_ENDPOINT,
    UPBIT_CLOSED_ORDERS_ENDPOINT,
    UPBIT_OPEN_ORDERS_ENDPOINT,
    UPBIT_ORDER_CHANCE_ENDPOINT,
    UPBIT_ORDER_DETAIL_ENDPOINT,
    UPBIT_TICKER_ENDPOINT,
)


UPBIT_FUNCTIONAL_GET_ENDPOINTS = frozenset(
    {
        UPBIT_ACCOUNTS_ENDPOINT,
        UPBIT_ORDER_CHANCE_ENDPOINT,
        UPBIT_OPEN_ORDERS_ENDPOINT,
        UPBIT_CLOSED_ORDERS_ENDPOINT,
        UPBIT_ORDER_DETAIL_ENDPOINT,
        UPBIT_TICKER_ENDPOINT,
    }
)
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_SESSION_RE = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_EVENT_STATES = frozenset({"wait", "watch", "trade", "done", "cancel", "reject"})
_WRITER_LEASE_SECONDS = 30
_OFFICIAL_UPBIT_ORIGIN = "https://api.upbit.com"


def _text(value: object) -> str:
    return str(value or "").strip()


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitFunctionalBlocked(f"upbit-functional-{label}-invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitFunctionalBlocked(f"upbit-functional-{label}-timezone-missing")
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def resolve_upbit_functional_base_url(*, allow_mock_origin: bool = False) -> str:
    """Return a canonical API origin before any credential is read or signed.

    The functional lane's evidence is explicitly for Upbit production.  An
    environment override therefore cannot redirect its Authorization header.
    A non-official origin exists only for a dependency-injected mock transport
    which never reaches the network.
    """

    configured = _text(env_value("UPBIT_BASE_URL") or UPBIT_BASE_URL).rstrip("/")
    try:
        parsed = urllib.parse.urlsplit(configured)
        port = parsed.port
    except ValueError as exc:
        raise UpbitFunctionalBlocked(
            "upbit-functional-api-origin-not-official"
        ) from exc
    if allow_mock_origin:
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-mock-api-origin-invalid"
            )
        return configured
    if (
        parsed.scheme.lower() != "https"
        or parsed.hostname != "api.upbit.com"
        or parsed.netloc != "api.upbit.com"
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise UpbitFunctionalBlocked(
            "upbit-functional-api-origin-not-official"
        )
    return _OFFICIAL_UPBIT_ORIGIN


def upbit_credential_fingerprint(access_key: str | None = None) -> str:
    """Return the non-secret identity bound to the exact configured API key."""

    key = _text(access_key if access_key is not None else env_value("UPBIT_ACCESS_KEY"))
    if not key:
        return ""
    return hashlib.sha256(f"UPBIT_SPOT\0{key}".encode("utf-8")).hexdigest()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def build_upbit_functional_authorization(
    access_key: str,
    secret_key: str,
    query: Sequence[tuple[str, str]],
) -> str:
    """Sign the exact ordered query, preserving repeated ``states[]`` keys."""

    if not _text(access_key) or not _text(secret_key):
        return ""
    # Upbit's official signing examples unquote the encoded query before
    # hashing so array keys remain ``states[]``/``uuids[]`` while the actual
    # wire URL stays percent encoded.  Ordering and repeated keys are retained.
    query_string = urllib.parse.unquote(urllib.parse.urlencode(tuple(query)))
    payload: dict[str, object] = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
    }
    if query_string:
        payload.update(
            {
                "query_hash": hashlib.sha512(query_string.encode("utf-8")).hexdigest(),
                "query_hash_alg": "SHA512",
            }
        )
    header = _b64url(
        json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode()
    )
    claims = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header}.{claims}"
    signature = hmac.new(
        secret_key.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha512
    ).digest()
    return f"Bearer {signing_input}.{_b64url(signature)}"


@dataclass(frozen=True)
class UpbitFunctionalGetRequest(PreparedRequest):
    def preview(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "method": self.method,
            "url": self.url,
            "endpoint": self.endpoint,
            "headers": self.safe_headers,
            "body": {},
            "query": list(self.query.get("ordered", ())) if self.query else [],
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


def build_upbit_functional_get_request(
    endpoint: str,
    query: Sequence[tuple[str, str]],
    *,
    allow_mock_origin: bool = False,
) -> UpbitFunctionalGetRequest:
    normalized_endpoint = _text(endpoint)
    if normalized_endpoint not in UPBIT_FUNCTIONAL_GET_ENDPOINTS:
        raise UpbitFunctionalBlocked("upbit-functional-get-endpoint-not-allowlisted")
    ordered = tuple((_text(key), _text(value)) for key, value in query)
    if any(not key for key, _value in ordered):
        raise UpbitFunctionalBlocked("upbit-functional-get-query-key-empty")
    base_url = resolve_upbit_functional_base_url(
        allow_mock_origin=allow_mock_origin
    )
    access_key = env_value("UPBIT_ACCESS_KEY")
    secret_key = env_value("UPBIT_SECRET_KEY")
    blocked = []
    if not access_key:
        blocked.append("UPBIT_ACCESS_KEY")
    if not secret_key:
        blocked.append("UPBIT_SECRET_KEY")
    encoded = urllib.parse.urlencode(ordered)
    authorization = build_upbit_functional_authorization(
        access_key, secret_key, ordered
    )
    return UpbitFunctionalGetRequest(
        provider="upbit-functional-read",
        method="GET",
        url=(
            f"{base_url.rstrip('/')}{normalized_endpoint}"
            + (f"?{encoded}" if encoded else "")
        ),
        endpoint=normalized_endpoint,
        headers={"Authorization": authorization},
        safe_headers={"authorization_configured": bool(authorization)},
        body=None,
        query={"ordered": ordered},
        blocked_reasons=blocked,
    )


class OfficialUpbitFunctionalGetClient:
    """Authenticated, account-bound client with no mutation surface."""

    def __init__(
        self,
        *,
        expected_account_fingerprint: str,
        credential_fingerprint_reader: Callable[[], str] = upbit_credential_fingerprint,
        sender: Callable[[PreparedRequest], Mapping[str, Any]] = send_prepared_request,
        allow_mock_transport: bool = False,
    ) -> None:
        if allow_mock_transport and sender is send_prepared_request:
            raise ValueError(
                "mock transport requires an explicitly injected sender"
            )
        expected = _text(expected_account_fingerprint).lower()
        if _HASH_RE.fullmatch(expected) is None:
            raise UpbitFunctionalBlocked("upbit-functional-account-fingerprint-invalid")
        self.expected_account_fingerprint = expected
        self.credential_fingerprint_reader = credential_fingerprint_reader
        self.sender = sender
        self.allow_mock_transport = allow_mock_transport

    def get(
        self,
        endpoint: str,
        query: Sequence[tuple[str, str]],
    ) -> object:
        actual = _text(self.credential_fingerprint_reader()).lower()
        if not secrets.compare_digest(actual, self.expected_account_fingerprint):
            raise UpbitFunctionalBlocked(
                "upbit-functional-credential-account-fingerprint-mismatch"
            )
        prepared = build_upbit_functional_get_request(
            endpoint,
            query,
            allow_mock_origin=self.allow_mock_transport,
        )
        if prepared.method != "GET" or not prepared.can_send:
            raise UpbitFunctionalBlocked(
                "upbit-functional-get-credential-or-request-not-ready"
            )
        response = self.sender(prepared)
        if not isinstance(response, Mapping):
            raise UpbitFunctionalBlocked("upbit-functional-get-response-invalid")
        payload = response.get("json")
        if endpoint == UPBIT_ORDER_DETAIL_ENDPOINT and int(
            response.get("statusCode") or 0
        ) == 404:
            error = payload.get("error") if isinstance(payload, Mapping) else None
            name = _text(error.get("name")) if isinstance(error, Mapping) else ""
            if name != "order_not_found":
                raise UpbitFunctionalBlocked(
                    "upbit-functional-order-absence-not-proven"
                )
            return {"_notFound": True}
        if response.get("ok") is not True:
            raise UpbitFunctionalBlocked(
                f"upbit-functional-get-failed:{normalized_endpoint(endpoint)}"
            )
        return payload


def normalized_endpoint(value: object) -> str:
    return _text(value).replace("/", "_").strip("_")


def normalize_upbit_myorder_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one official private ``myOrder`` event without float math."""

    if _text(payload.get("type")) not in {"myOrder", "my_order"}:
        raise UpbitFunctionalBlocked("upbit-functional-myorder-type-invalid")
    order_uuid = _text(payload.get("uuid"))
    trade_uuid = _text(payload.get("trade_uuid") or payload.get("tradeUuid"))
    identifier = _text(payload.get("identifier"))
    market = _text(payload.get("code") or payload.get("market")).upper()
    state = _text(payload.get("state")).lower()
    side = _text(payload.get("ask_bid") or payload.get("side")).upper()
    if (
        not order_uuid
        or not market
        or state not in _EVENT_STATES
        or side not in {"BID", "ASK"}
    ):
        raise UpbitFunctionalBlocked("upbit-functional-myorder-identity-invalid")
    try:
        timestamp = int(payload.get("timestamp") or 0)
    except (TypeError, ValueError) as exc:
        raise UpbitFunctionalBlocked("upbit-functional-myorder-time-invalid") from exc
    if timestamp <= 0:
        raise UpbitFunctionalBlocked("upbit-functional-myorder-time-invalid")
    divisor = 1_000_000 if timestamp >= 100_000_000_000_000 else 1_000
    occurred_at = datetime.fromtimestamp(timestamp / divisor, tz=timezone.utc)
    raw_hash = _stable_hash(dict(payload))
    component = trade_uuid or raw_hash[:24]
    return {
        "eventId": f"upbit-myorder:{order_uuid}:{component}:{state}",
        "orderUuid": order_uuid,
        "tradeUuid": trade_uuid,
        "identifier": identifier,
        "market": market,
        "side": side,
        "state": state,
        "occurredAt": _utc_text(occurred_at),
        "rawHash": raw_hash,
    }


class DurableUpbitMyOrderJournal:
    """Lossless session journal; gaps can be marked but never cleared in place."""

    def __init__(self, path: str | Path, *, clock: Callable[[], datetime]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._lock = threading.RLock()
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS upbit_myorder_sessions (
                    session_id TEXT PRIMARY KEY,
                    account_fingerprint TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    connected INTEGER NOT NULL DEFAULT 0,
                    authenticated INTEGER NOT NULL DEFAULT 0,
                    gap_detected INTEGER NOT NULL DEFAULT 1,
                    completed INTEGER NOT NULL DEFAULT 0,
                    cleanup_recovery INTEGER NOT NULL DEFAULT 0,
                    writer_token_hash TEXT NOT NULL DEFAULT '',
                    writer_generation INTEGER NOT NULL DEFAULT 0,
                    writer_lease_expires_at TEXT NOT NULL DEFAULT '',
                    revision INTEGER NOT NULL DEFAULT 0,
                    terminal_seal_json TEXT NOT NULL DEFAULT '',
                    terminal_seal_hash TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS upbit_myorder_events (
                    session_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    order_uuid TEXT NOT NULL,
                    trade_uuid TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    market TEXT NOT NULL,
                    raw_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(session_id, event_id)
                );
                """
            )
            session_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(upbit_myorder_sessions)"
                )
            }
            if "cleanup_recovery" not in session_columns:
                connection.execute(
                    """ALTER TABLE upbit_myorder_sessions
                    ADD COLUMN cleanup_recovery INTEGER NOT NULL DEFAULT 0"""
                )
            for name, definition in (
                ("writer_token_hash", "TEXT NOT NULL DEFAULT ''"),
                ("writer_generation", "INTEGER NOT NULL DEFAULT 0"),
                ("writer_lease_expires_at", "TEXT NOT NULL DEFAULT ''"),
                ("revision", "INTEGER NOT NULL DEFAULT 0"),
                ("terminal_seal_json", "TEXT NOT NULL DEFAULT ''"),
                ("terminal_seal_hash", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in session_columns:
                    connection.execute(
                        f"ALTER TABLE upbit_myorder_sessions ADD COLUMN {name} {definition}"
                    )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    def begin_authenticated_session(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        started_at: datetime,
    ) -> dict[str, Any]:
        normalized_session = _text(session_id)
        fingerprint = _text(account_fingerprint).lower()
        if _SESSION_RE.fullmatch(normalized_session) is None:
            raise UpbitFunctionalBlocked("upbit-functional-journal-session-invalid")
        if _HASH_RE.fullmatch(fingerprint) is None:
            raise UpbitFunctionalBlocked("upbit-functional-journal-account-invalid")
        started = _utc(started_at, "journal-started-at")
        now = _utc(self.clock(), "journal-current-time")
        if started > now:
            raise UpbitFunctionalBlocked("upbit-functional-journal-start-future")
        writer_token = secrets.token_urlsafe(48)
        writer_token_hash = hashlib.sha256(writer_token.encode("utf-8")).hexdigest()
        writer_generation = 1
        lease_expires_at = now + timedelta(seconds=_WRITER_LEASE_SECONDS)
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                active = connection.execute(
                    "SELECT session_id FROM upbit_myorder_sessions WHERE completed=0"
                ).fetchone()
                if active is not None:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-journal-owner-already-active"
                    )
                connection.execute(
                    """INSERT INTO upbit_myorder_sessions
                    (session_id,account_fingerprint,started_at,observed_at,
                     connected,authenticated,gap_detected,writer_token_hash,
                     writer_generation,writer_lease_expires_at)
                    VALUES (?,?,?,?,0,0,1,?,?,?)""",
                    (
                        normalized_session,
                        fingerprint,
                        _utc_text(started),
                        _utc_text(now),
                        writer_token_hash,
                        writer_generation,
                        _utc_text(lease_expires_at),
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return {
            "writerToken": writer_token,
            "writerGeneration": writer_generation,
            "leaseExpiresAt": _utc_text(lease_expires_at),
        }

    @staticmethod
    def _assert_writer(
        connection: sqlite3.Connection,
        *,
        session_id: str,
        writer_token: str,
        writer_generation: int,
        now: datetime,
        allow_completed: bool = False,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
            (_text(session_id),),
        ).fetchone()
        supplied_hash = hashlib.sha256(
            _text(writer_token).encode("utf-8")
        ).hexdigest()
        if (
            row is None
            or not secrets.compare_digest(
                _text(row["writer_token_hash"]), supplied_hash
            )
            or int(row["writer_generation"]) != int(writer_generation)
            or (row["completed"] != 0 and not allow_completed)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-writer-authority-invalid"
            )
        lease_expires_at = _utc(
            row["writer_lease_expires_at"],
            "journal-writer-lease-expires-at",
        )
        if now > lease_expires_at:
            connection.execute(
                """UPDATE upbit_myorder_sessions
                SET gap_detected=1,connected=0,
                    revision=revision+1,detail='writer-lease-expired'
                WHERE session_id=?""",
                (_text(session_id),),
            )
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-writer-lease-expired"
            )
        return row

    def attest_authenticated_connection(
        self,
        session_id: str,
        *,
        writer_token: str,
        writer_generation: int,
    ) -> None:
        """Mark complete only after the real private WS subscription ACK."""

        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = self._assert_writer(
                connection,
                session_id=session_id,
                writer_token=writer_token,
                writer_generation=writer_generation,
                now=now,
            )
            cleanup_recovery = int(session["cleanup_recovery"]) == 1
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET connected=1,authenticated=1,gap_detected=?,
                    observed_at=?,writer_lease_expires_at=?,
                    revision=revision+1,
                    detail='authenticated-myorder-ack'
                WHERE session_id=? AND completed=0 AND connected=0
                AND authenticated=0 AND gap_detected=1""",
                (
                    1 if cleanup_recovery else 0,
                    _utc_text(now),
                    _utc_text(now + timedelta(seconds=_WRITER_LEASE_SECONDS)),
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-auth-handshake-invalid"
                )
            connection.commit()

    def observe(
        self,
        session_id: str,
        *,
        writer_token: str,
        writer_generation: int,
    ) -> None:
        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_writer(
                connection,
                session_id=session_id,
                writer_token=writer_token,
                writer_generation=writer_generation,
                now=now,
            )
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET observed_at=?,writer_lease_expires_at=?
                    ,revision=revision+1
                WHERE session_id=? AND connected=1 AND authenticated=1
                AND (gap_detected=0 OR cleanup_recovery=1) AND completed=0""",
                (
                    _utc_text(now),
                    _utc_text(now + timedelta(seconds=_WRITER_LEASE_SECONDS)),
                    _text(session_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-not-continuous"
                )
            connection.commit()

    def ingest(
        self,
        session_id: str,
        payload: Mapping[str, Any],
        *,
        writer_token: str,
        writer_generation: int,
    ) -> dict[str, Any]:
        try:
            event = normalize_upbit_myorder_event(payload)
        except Exception:
            self.mark_gap(
                session_id,
                detail="parser-failure",
                writer_token=writer_token,
                writer_generation=writer_generation,
            )
            raise
        now = _utc(self.clock(), "journal-current-time")
        occurred = _utc(event["occurredAt"], "myorder-occurred-at")
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                session = connection.execute(
                    "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                    (_text(session_id),),
                ).fetchone()
                self._assert_writer(
                    connection,
                    session_id=session_id,
                    writer_token=writer_token,
                    writer_generation=writer_generation,
                    now=now,
                )
                if (
                    session is None
                    or session["connected"] != 1
                    or session["authenticated"] != 1
                    or (
                        session["gap_detected"] != 0
                        and session["cleanup_recovery"] != 1
                    )
                    or session["completed"] != 0
                ):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-journal-not-continuous"
                    )
                started = _utc(session["started_at"], "journal-started-at")
                if occurred < started or occurred > now:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-myorder-event-outside-session"
                    )
                prior = connection.execute(
                    """SELECT raw_hash FROM upbit_myorder_events
                    WHERE session_id=? AND event_id=?""",
                    (_text(session_id), event["eventId"]),
                ).fetchone()
                if prior is not None:
                    if not secrets.compare_digest(prior["raw_hash"], event["rawHash"]):
                        connection.execute(
                            """UPDATE upbit_myorder_sessions
                            SET gap_detected=1,connected=0,detail='conflicting-replay'
                                ,revision=revision+1
                            WHERE session_id=?""",
                            (_text(session_id),),
                        )
                        connection.commit()
                        raise UpbitFunctionalBlocked(
                            "upbit-functional-myorder-conflicting-replay"
                        )
                    connection.rollback()
                    return event
                connection.execute(
                    """INSERT INTO upbit_myorder_events
                    (session_id,event_id,occurred_at,order_uuid,trade_uuid,
                     identifier,market,raw_hash,payload)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        _text(session_id),
                        event["eventId"],
                        event["occurredAt"],
                        event["orderUuid"],
                        event["tradeUuid"],
                        event["identifier"],
                        event["market"],
                        event["rawHash"],
                        json.dumps(
                            {
                                "schemaVersion": (
                                    "upbit-functional-myorder-raw-envelope/v1"
                                ),
                                "rawPayload": dict(payload),
                                "normalized": event,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    ),
                )
                connection.execute(
                    """UPDATE upbit_myorder_sessions
                    SET observed_at=?,writer_lease_expires_at=?
                        ,revision=revision+1
                    WHERE session_id=?""",
                    (
                        _utc_text(now),
                        _utc_text(now + timedelta(seconds=_WRITER_LEASE_SECONDS)),
                        _text(session_id),
                    ),
                )
                connection.commit()
                return event
            except Exception:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def mark_gap(
        self,
        session_id: str,
        *,
        detail: str,
        writer_token: str,
        writer_generation: int,
    ) -> None:
        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_writer(
                connection,
                session_id=session_id,
                writer_token=writer_token,
                writer_generation=writer_generation,
                now=now,
            )
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET gap_detected=1,connected=0,detail=?
                    ,revision=revision+1
                WHERE session_id=? AND completed=0""",
                (_text(detail)[:500], _text(session_id)),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-functional-journal-session-missing")
            connection.commit()

    def recover_cleanup_authenticated(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
    ) -> dict[str, Any]:
        """Record a recovery connection without erasing any historical gap.

        A previously incomplete private-stream proof can never become complete
        again.  Cleanup after owner loss therefore remains blocked in the
        production graph until a separate REST recovery attestation exists.
        """

        now = _utc(self.clock(), "journal-current-time")
        fingerprint = _text(account_fingerprint).lower()
        writer_token = secrets.token_urlsafe(48)
        writer_token_hash = hashlib.sha256(writer_token.encode("utf-8")).hexdigest()
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if (
                row is None
                or row["completed"] != 0
                or not secrets.compare_digest(
                    _text(row["account_fingerprint"]), fingerprint
                )
                or row["gap_detected"] != 1
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-cleanup-recovery-invalid"
                )
            connection.execute(
                """UPDATE upbit_myorder_sessions
                SET connected=0,authenticated=0,cleanup_recovery=1,
                    writer_token_hash=?,writer_generation=writer_generation+1,
                    writer_lease_expires_at=?,observed_at=?,
                    revision=revision+1,
                    detail='cleanup-only-writer-rotated'
                WHERE session_id=?""",
                (
                    writer_token_hash,
                    _utc_text(now + timedelta(seconds=_WRITER_LEASE_SECONDS)),
                    _utc_text(now),
                    _text(session_id),
                ),
            )
            generation = int(row["writer_generation"]) + 1
            connection.commit()
        return {
            "writerToken": writer_token,
            "writerGeneration": generation,
            "leaseExpiresAt": _utc_text(
                now + timedelta(seconds=_WRITER_LEASE_SECONDS)
            ),
        }

    def abort(
        self,
        session_id: str,
        *,
        detail: str,
        writer_token: str,
        writer_generation: int,
    ) -> None:
        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_writer(
                connection,
                session_id=session_id,
                writer_token=writer_token,
                writer_generation=writer_generation,
                now=now,
            )
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET completed=1,connected=0,authenticated=0,
                    writer_token_hash='',writer_lease_expires_at='',
                    revision=revision+1,detail=? WHERE session_id=?""",
                (_text(detail)[:500], _text(session_id)),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked("upbit-functional-journal-session-missing")
            connection.commit()

    def complete(
        self,
        session_id: str,
        *,
        writer_token: str,
        writer_generation: int,
    ) -> None:
        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._assert_writer(
                connection,
                session_id=session_id,
                writer_token=writer_token,
                writer_generation=writer_generation,
                now=now,
            )
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET completed=1,connected=0,authenticated=0,
                    writer_token_hash='',writer_lease_expires_at='',
                    revision=revision+1
                WHERE session_id=? AND gap_detected=0""",
                (_text(session_id),),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-incomplete-cannot-seal"
                )
            connection.commit()

    @staticmethod
    def _terminal_attestation_locked(
        connection: sqlite3.Connection,
        *,
        session: sqlite3.Row,
        identifiers: tuple[str, ...],
    ) -> dict[str, Any]:
        normalized_identifiers = tuple(
            sorted({_text(identifier) for identifier in identifiers if _text(identifier)})
        )
        events = connection.execute(
            """SELECT event_id,occurred_at,identifier,market,raw_hash,payload
            FROM upbit_myorder_events WHERE session_id=?
            ORDER BY occurred_at,event_id""",
            (_text(session["session_id"]),),
        ).fetchall()
        event_chain = [
            {
                "eventId": _text(row["event_id"]),
                "occurredAt": _text(row["occurred_at"]),
                "identifier": _text(row["identifier"]),
                "market": _text(row["market"]).upper(),
                "rawHash": _text(row["raw_hash"]).lower(),
            }
            for row in events
        ]
        owned = set(normalized_identifiers)
        external_absent = not any(
            row["identifier"] not in owned for row in event_chain
        )
        cleanup_recovery = int(session["cleanup_recovery"]) == 1
        continuous = bool(
            int(session["completed"]) == 0
            and int(session["connected"]) == 1
            and int(session["authenticated"]) == 1
            and int(session["gap_detected"]) == 0
            and not cleanup_recovery
        )
        body = {
            "schemaVersion": "upbit-functional-private-terminal-seal/v1",
            "sessionId": _text(session["session_id"]),
            "accountFingerprint": _text(session["account_fingerprint"]).lower(),
            "channel": "myOrder",
            "writerGeneration": int(session["writer_generation"]),
            "journalRevision": int(session["revision"]),
            "eventCursor": len(event_chain),
            "lastEventId": event_chain[-1]["eventId"] if event_chain else "",
            "eventHeadHash": _stable_hash(event_chain),
            "ownedIdentifiers": list(normalized_identifiers),
            "ownedIdentifiersHash": _stable_hash(list(normalized_identifiers)),
            "externalActivityAbsent": external_absent,
            "streamContinuous": continuous,
            "cleanupOnlyRecovery": cleanup_recovery,
            "gapDetected": int(session["gap_detected"]) == 1,
            "observedAt": _text(session["observed_at"]),
        }
        return {**body, "sealHash": _stable_hash(body)}

    def prepare_terminal_attestation(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
    ) -> dict[str, Any]:
        """Freeze the exact private-stream cursor used by final evidence.

        This is a read transaction, not completion.  A later event, heartbeat,
        gap, or writer rotation increments ``revision`` and makes the commit
        CAS fail instead of preserving stale PASS evidence.
        """

        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if session is None or int(session["completed"]) != 0:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-session-invalid"
                )
            attestation = self._terminal_attestation_locked(
                connection,
                session=session,
                identifiers=identifiers,
            )
            if attestation["externalActivityAbsent"] is not True:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-external-activity"
                )
            if not (
                attestation["streamContinuous"] is True
                or (
                    attestation["cleanupOnlyRecovery"] is True
                    and attestation["gapDetected"] is True
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-continuity-invalid"
                )
            connection.commit()
        return attestation

    def complete_with_attestation(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
        expected: Mapping[str, Any],
        writer_token: str = "",
        writer_generation: int = 0,
        startup_recovery: bool = False,
    ) -> dict[str, Any]:
        """CAS-complete only the exact cursor embedded in final evidence."""

        supplied = dict(expected)
        supplied_hash = _text(supplied.get("sealHash")).lower()
        body = {key: value for key, value in supplied.items() if key != "sealHash"}
        if not supplied_hash or not secrets.compare_digest(
            supplied_hash, _stable_hash(body)
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-seal-hash-invalid"
            )
        now = _utc(self.clock(), "journal-current-time")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if session is None:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-session-invalid"
                )
            if int(session["completed"]) == 1:
                stored_hash = _text(session["terminal_seal_hash"]).lower()
                stored_json = _text(session["terminal_seal_json"])
                if not secrets.compare_digest(stored_hash, supplied_hash):
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-journal-terminal-seal-mismatch"
                    )
                try:
                    stored = json.loads(stored_json)
                except json.JSONDecodeError as exc:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-journal-terminal-seal-invalid"
                    ) from exc
                if stored != supplied:
                    connection.rollback()
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-journal-terminal-seal-mismatch"
                    )
                connection.commit()
                return supplied
            if not startup_recovery:
                self._assert_writer(
                    connection,
                    session_id=session_id,
                    writer_token=writer_token,
                    writer_generation=writer_generation,
                    now=now,
                )
                session = connection.execute(
                    "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                    (_text(session_id),),
                ).fetchone()
            actual = self._terminal_attestation_locked(
                connection,
                session=session,
                identifiers=identifiers,
            )
            if actual != supplied:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-cursor-changed"
                )
            if actual["externalActivityAbsent"] is not True or not (
                actual["streamContinuous"] is True
                or (
                    actual["cleanupOnlyRecovery"] is True
                    and actual["gapDetected"] is True
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-continuity-invalid"
                )
            cursor = connection.execute(
                """UPDATE upbit_myorder_sessions
                SET completed=1,connected=0,authenticated=0,
                    writer_token_hash='',writer_lease_expires_at='',
                    terminal_seal_json=?,terminal_seal_hash=?,
                    revision=revision+1,detail='terminal-cursor-sealed'
                WHERE session_id=? AND completed=0 AND revision=?
                AND writer_generation=?""",
                (
                    json.dumps(supplied, sort_keys=True, separators=(",", ":")),
                    supplied_hash,
                    _text(session_id),
                    int(supplied["journalRevision"]),
                    int(supplied["writerGeneration"]),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-terminal-cas-failed"
                )
            connection.commit()
        return supplied

    def terminal_seal(self, *, session_id: str) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT completed,terminal_seal_json,terminal_seal_hash
                FROM upbit_myorder_sessions WHERE session_id=?""",
                (_text(session_id),),
            ).fetchone()
        if row is None or int(row["completed"]) != 1:
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-seal-missing"
            )
        try:
            value = json.loads(_text(row["terminal_seal_json"]))
        except json.JSONDecodeError as exc:
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-seal-invalid"
            ) from exc
        seal_hash = _text(row["terminal_seal_hash"]).lower()
        if not isinstance(value, Mapping) or not secrets.compare_digest(
            seal_hash, _text(value.get("sealHash")).lower()
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-terminal-seal-invalid"
            )
        return dict(value)

    def snapshot(
        self,
        *,
        session_id: str,
        identifiers: tuple[str, ...],
    ) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as connection:
            now = _utc(self.clock(), "journal-current-time")
            connection.execute("BEGIN IMMEDIATE")
            session = connection.execute(
                "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            events = connection.execute(
                """SELECT payload FROM upbit_myorder_events
                WHERE session_id=? ORDER BY occurred_at,event_id""",
                (_text(session_id),),
            ).fetchall()
            if (
                session is not None
                and session["completed"] == 0
                and session["connected"] == 1
                and _text(session["writer_lease_expires_at"])
                and now
                > _utc(
                    session["writer_lease_expires_at"],
                    "journal-writer-lease-expires-at",
                )
            ):
                connection.execute(
                    """UPDATE upbit_myorder_sessions SET connected=0,
                    gap_detected=1,detail='writer-lease-expired'
                    ,revision=revision+1
                    WHERE session_id=?""",
                    (_text(session_id),),
                )
                session = connection.execute(
                    "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                    (_text(session_id),),
                ).fetchone()
            terminal_attestation = self._terminal_attestation_locked(
                connection,
                session=session,
                identifiers=identifiers,
            ) if session is not None else None
            connection.commit()
        if session is None:
            raise UpbitFunctionalBlocked("upbit-functional-journal-session-missing")
        rows: list[dict[str, Any]] = []
        for stored_row in events:
            stored = json.loads(stored_row["payload"])
            if (
                isinstance(stored, Mapping)
                and stored.get("schemaVersion")
                == "upbit-functional-myorder-raw-envelope/v1"
                and isinstance(stored.get("normalized"), Mapping)
                and isinstance(stored.get("rawPayload"), Mapping)
            ):
                normalized = dict(stored["normalized"])
                normalized["rawPayload"] = dict(stored["rawPayload"])
                normalized["rawPayloadHash"] = _stable_hash(
                    dict(stored["rawPayload"])
                )
                rows.append(normalized)
            elif isinstance(stored, Mapping):
                # Legacy normalized-only rows cannot support a first-live
                # wiring proof, but remain readable for fail-closed cleanup.
                rows.append(dict(stored))
            else:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-payload-invalid"
                )
        owned = {_text(identifier) for identifier in identifiers}
        external_absent = not any(
            _text(row.get("identifier")) not in owned
            for row in rows
        )
        complete = (
            session["connected"] == 1
            and session["authenticated"] == 1
            and session["gap_detected"] == 0
            and session["completed"] == 0
            and session["cleanup_recovery"] == 0
        )
        return {
            "connected": session["connected"] == 1,
            "authenticated": session["authenticated"] == 1,
            "eventsComplete": complete,
            "gapDetected": session["gap_detected"] == 1,
            "channel": "myOrder",
            "accountFingerprint": session["account_fingerprint"],
            "startedAt": session["started_at"],
            "observedAt": session["observed_at"],
            "externalActivityAbsent": external_absent,
            "cleanupOnlyRecovery": session["cleanup_recovery"] == 1,
            "writerGeneration": int(session["writer_generation"]),
            "journalRevision": int(session["revision"]),
            "eventCursor": int(terminal_attestation["eventCursor"]),
            "lastEventId": str(terminal_attestation["lastEventId"]),
            "eventHeadHash": str(terminal_attestation["eventHeadHash"]),
            "terminalSealHash": _text(session["terminal_seal_hash"]).lower(),
            "completed": session["completed"] == 1,
            "events": rows,
        }

    def active_sessions(self) -> list[dict[str, Any]]:
        with self._lock, closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM upbit_myorder_sessions
                WHERE completed=0 ORDER BY started_at,session_id"""
            ).fetchall()
        return [dict(row) for row in rows]

    def startup_fail_closed(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        detail: str,
        completed: bool,
    ) -> dict[str, Any]:
        """Revoke a writer token lost with the previous process.

        The operation only reduces authority: disconnect, sticky gap, clear
        the durable writer hash/lease, and optionally seal an orphaned journal.
        """

        fingerprint = _text(account_fingerprint).lower()
        if _HASH_RE.fullmatch(fingerprint) is None:
            raise UpbitFunctionalBlocked(
                "upbit-functional-journal-account-invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM upbit_myorder_sessions WHERE session_id=?",
                (_text(session_id),),
            ).fetchone()
            if (
                row is None
                or row["completed"] != 0
                or not secrets.compare_digest(
                    _text(row["account_fingerprint"]), fingerprint
                )
            ):
                connection.rollback()
                raise UpbitFunctionalBlocked(
                    "upbit-functional-journal-startup-revoke-invalid"
                )
            connection.execute(
                """UPDATE upbit_myorder_sessions SET connected=0,
                authenticated=0,gap_detected=1,completed=?,
                writer_token_hash='',writer_lease_expires_at='',detail=?
                ,revision=revision+1
                WHERE session_id=?""",
                (1 if completed else 0, _text(detail)[:500], _text(session_id)),
            )
            connection.commit()
        return {
            "sessionId": _text(session_id),
            "completed": bool(completed),
            "gapDetected": True,
            "writerRevoked": True,
        }


__all__ = [
    "DurableUpbitMyOrderJournal",
    "OfficialUpbitFunctionalGetClient",
    "UPBIT_FUNCTIONAL_GET_ENDPOINTS",
    "UpbitFunctionalGetRequest",
    "build_upbit_functional_authorization",
    "build_upbit_functional_get_request",
    "resolve_upbit_functional_base_url",
    "normalize_upbit_myorder_event",
    "upbit_credential_fingerprint",
]
