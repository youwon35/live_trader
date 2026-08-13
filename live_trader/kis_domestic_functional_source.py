from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import re
import secrets
import sqlite3
import threading
import uuid
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .kis_domestic_functional_contract import (
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    BAR_INTERVAL_MINUTES,
    KST,
    LIVE_ORIGIN,
    PDNO,
    ROUTE,
    canonical_content_hash,
)

from trading_runtime.continuous_runtime import ClosedBar, OpenBoundary
from trading_runtime.market_calendar import session_bounds_utc
from trading_runtime.realtime_feeds import (
    FeedSubscription,
    KIS_DOMESTIC_TRADE_FIELD_COUNT,
    KisWebSocketClosedBarFeed,
)


KIS_DOMESTIC_FUNCTIONAL_SOURCE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SOURCE_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SOURCE_TOKEN_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_SOURCE_MUTATION_AVAILABLE = False

_SOURCE = "KIS_WEBSOCKET_H0STCNT0"
_SOURCE_PROVIDER = "kis"
_TR_ID = "H0STCNT0"
_INSTRUMENT_ID = "KRX:010140"
_SYMBOL = "010140.KS"
_TIMEFRAME = "5m"
_ZERO_HASH = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", flags=re.ASCII)
_EVALUATION = re.compile(r"^kis-eval-[0-9a-f]{32}$", flags=re.ASCII)
_SOCKET_IDENTITY = re.compile(
    r"^kis-ws-socket-[A-Za-z0-9._:-]{16,128}$", flags=re.ASCII
)
_ARM_ID = re.compile(r"^kis-public-source-arm-[0-9a-f]{32}$", flags=re.ASCII)
_ACTIVE_ARM_STATES = (
    "ARMED_WAIT_PUBLIC",
    "NATURAL_BUY_OBSERVED",
    "NEXT_OPEN_TRIGGER_SEALED",
)
MARKET_SOURCE_ACK_SCHEMA = "kis-domestic-functional-market-source-durable-ack/v2"
MARKET_SOURCE_LINK_SCHEMA = "kis-domestic-functional-source-market-link/v1"
_MARKET_SOURCE_LINK_KEYS = {
    "marketSourceLinkSchema", "marketSourceSessionId",
    "marketSourceAccountFingerprint", "marketSourceOwnerEpoch",
    "marketSourceOwnerEpochId", "marketSourceOwnerEpochHash",
    "marketSourceProcessGeneration", "marketSourceIngressOrdinal",
    "marketSourceRawRecordHash", "marketSourceRawFrameHash",
    "marketSourcePreviousIngressHeadHash", "marketSourceAuthorityKeyIdHash",
    "marketSourceAuthorityPurpose", "marketSourceArmTransitionHeadHash",
}


class KisDomesticFunctionalSourceBlocked(RuntimeError):
    pass


CaptureSigner = Callable[[str, Mapping[str, Any]], str]
CaptureVerifier = Callable[[str, Mapping[str, Any], str], bool]


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


def _signature_text(value: Any) -> bool:
    if type(value) is not str:
        return False
    if _SHA256.fullmatch(value):
        return True
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return len(decoded) == 64 and base64.b64encode(decoded).decode("ascii") == value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-aware")
    converted = value.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-finite")
    return converted


def _utc_text(value: datetime, label: str) -> str:
    return _utc(value, label).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-canonical-utc")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalSourceBlocked(
            f"{label}-not-canonical-utc"
        ) from None
    if _utc_text(parsed, label) != value:
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-canonical-utc")
    return parsed


def _parse_runtime_utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-runtime-utc")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalSourceBlocked(
            f"{label}-not-runtime-utc"
        ) from None
    return _utc(parsed, label)


def _decimal(value: Any, label: str, *, zero_allowed: bool = False) -> Decimal:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-decimal") from None
    if not parsed.is_finite() or parsed < 0 or (not zero_allowed and parsed <= 0):
        raise KisDomesticFunctionalSourceBlocked(f"{label}-not-positive-finite")
    return parsed


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _frame_hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _raw_event_hash_body(
    *,
    source_generation: str,
    socket_identity_hash: str,
    source_sequence: str,
    record_index: int,
    raw_frame_hash: str,
    record_fields: list[str],
    received_at: str,
) -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-h0stcnt0-raw-event/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "sourceGeneration": source_generation,
        "socketIdentityHash": socket_identity_hash,
        "sourceSequence": source_sequence,
        "recordIndex": record_index,
        "rawFrameHash": raw_frame_hash,
        "recordFields": record_fields,
        "receivedAt": received_at,
    }


@dataclass(frozen=True)
class _RawTrade:
    source_sequence: str
    feed_source_sequence: str
    raw_frame_hash: str
    raw_event_hash: str
    record_index: int
    trade_at: datetime
    received_at: datetime
    price: Decimal
    volume: Decimal
    bucket_open: datetime
    bucket_close: datetime
    fields: tuple[str, ...]

    def durable_envelope(
        self,
        *,
        arm_id: str,
        source_generation: str,
        socket_identity_hash: str,
        frame_envelope_hash: str,
    ) -> dict[str, Any]:
        return {
            "schemaVersion": "kis-domestic-h0stcnt0-raw-event-envelope/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "armId": arm_id,
            "sourceGeneration": source_generation,
            "socketIdentityHash": socket_identity_hash,
            "sourceSequence": self.source_sequence,
            "feedSourceSequence": self.feed_source_sequence,
            "frameEnvelopeHash": frame_envelope_hash,
            "rawFrameHash": self.raw_frame_hash,
            "rawEventHash": self.raw_event_hash,
            "recordIndex": self.record_index,
            "tradeAt": _utc_text(self.trade_at, "tradeAt"),
            "receivedAt": _utc_text(self.received_at, "receivedAt"),
            "bucketOpenAt": _utc_text(self.bucket_open, "bucketOpenAt"),
            "bucketCloseAt": _utc_text(self.bucket_close, "bucketCloseAt"),
            "recordFields": list(self.fields),
        }

    def archive_row(self) -> dict[str, Any]:
        return {
            "sourceSequence": self.source_sequence,
            "feedSourceSequence": self.feed_source_sequence,
            "recordIndex": self.record_index,
            "tradeAt": _utc_text(self.trade_at, "tradeAt"),
            "receivedAt": _utc_text(self.received_at, "receivedAt"),
            "bucketOpenAt": _utc_text(self.bucket_open, "bucketOpenAt"),
            "bucketCloseAt": _utc_text(self.bucket_close, "bucketCloseAt"),
            "rawFrameHash": self.raw_frame_hash,
            "rawEventHash": self.raw_event_hash,
            "recordFields": list(self.fields),
        }


@dataclass
class _BarAccumulator:
    opened: datetime
    closed: datetime
    events: list[_RawTrade] = field(default_factory=list)

    def add(self, event: _RawTrade) -> None:
        if event.bucket_open != self.opened or event.bucket_close != self.closed:
            raise KisDomesticFunctionalSourceBlocked("raw-event-bucket-mismatch")
        self.events.append(event)

    def proof(self, closed_bar: ClosedBar) -> dict[str, Any]:
        if not self.events:
            raise KisDomesticFunctionalSourceBlocked("closed-bar-raw-events-missing")
        if (
            _parse_runtime_utc(closed_bar.start_time, "closedBar.startTime")
            != self.opened
            or _parse_runtime_utc(closed_bar.end_time, "closedBar.endTime")
            != self.closed
            or closed_bar.instrument_id != _INSTRUMENT_ID
            or closed_bar.symbol != _SYMBOL
            or closed_bar.timeframe != _TIMEFRAME
            or closed_bar.source_provider != _SOURCE_PROVIDER
            or closed_bar.event_count != len(self.events)
        ):
            raise KisDomesticFunctionalSourceBlocked("closed-bar-feed-lineage-mismatch")
        prices = [event.price for event in self.events]
        expected = (
            prices[0],
            max(prices),
            min(prices),
            prices[-1],
        )
        actual = tuple(
            Decimal(str(value))
            for value in (
                closed_bar.open,
                closed_bar.high,
                closed_bar.low,
                closed_bar.close,
            )
        )
        if expected != actual:
            raise KisDomesticFunctionalSourceBlocked("closed-bar-feed-ohlc-mismatch")
        chain = _ZERO_HASH
        for event in self.events:
            chain = _hash(
                {
                    "previousHash": chain,
                    "rawEventHash": event.raw_event_hash,
                    "sourceSequence": event.source_sequence,
                }
            )
        return {
            "openAt": _utc_text(self.opened, "bar.openAt"),
            "closeAt": _utc_text(self.closed, "bar.closeAt"),
            "open": _decimal_text(expected[0]),
            "high": _decimal_text(expected[1]),
            "low": _decimal_text(expected[2]),
            "close": _decimal_text(expected[3]),
            "sourceSequenceStart": self.events[0].source_sequence,
            "sourceSequenceEnd": self.events[-1].source_sequence,
            "eventCount": len(self.events),
            "rawEventChainHash": chain,
        }


def _independent_record_truth(
    record: tuple[str, ...],
    *,
    received_at: datetime,
) -> dict[str, Any]:
    if len(record) != KIS_DOMESTIC_TRADE_FIELD_COUNT or record[0] != PDNO:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-identity-invalid"
        )
    if not re.fullmatch(r"[0-9]{8}", record[33]) or not re.fullmatch(
        r"[0-9]{6}", record[1]
    ):
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-time-invalid"
        )
    try:
        trade_local = datetime.strptime(
            record[33] + record[1], "%Y%m%d%H%M%S"
        ).replace(tzinfo=KST)
    except ValueError:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-time-invalid"
        ) from None
    trade_at = trade_local.astimezone(timezone.utc)
    received = _utc(received_at, "authenticatedFrame.receivedAt")
    deadline = trade_local.replace(
        hour=13, minute=15, second=0, microsecond=0
    ).astimezone(timezone.utc)
    if trade_at > deadline or received > deadline:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-after-armed-deadline"
        )
    lag = (received - trade_at).total_seconds()
    if lag < 0 or lag > 2:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-observation-lag-invalid"
        )
    price = _decimal(record[2], "authenticatedFrame.price")
    _decimal(record[12], "authenticatedFrame.volume", zero_allowed=True)
    try:
        session_open, session_close = session_bounds_utc(
            "XKRX", trade_at.astimezone(KST).date()
        )
    except ValueError:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-not-xkrx-session"
        ) from None
    if not session_open <= trade_at < session_close:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-outside-xkrx-session"
        )
    interval_seconds = BAR_INTERVAL_MINUTES * 60
    offset = int((trade_at - session_open).total_seconds())
    bucket_open = session_open + timedelta(
        seconds=(offset // interval_seconds) * interval_seconds
    )
    bucket_close = min(
        bucket_open + timedelta(seconds=interval_seconds), session_close
    )
    if bucket_close - bucket_open != timedelta(minutes=BAR_INTERVAL_MINUTES):
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-partial-bar-forbidden"
        )
    return {
        "tradeAt": _utc_text(trade_at, "authenticatedFrame.tradeAt"),
        "receivedAt": _utc_text(received, "authenticatedFrame.receivedAt"),
        "bucketOpenAt": _utc_text(
            bucket_open, "authenticatedFrame.bucketOpenAt"
        ),
        "bucketCloseAt": _utc_text(
            bucket_close, "authenticatedFrame.bucketCloseAt"
        ),
        "price": price,
    }


def _independent_parse_authenticated_frame(
    frame: Mapping[str, Any],
) -> list[tuple[str, ...]]:
    raw = frame.get("rawFrame")
    if type(raw) is not str or not raw.startswith(f"0|{_TR_ID}|"):
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-raw-frame-channel-invalid"
        )
    parts = raw.split("|", 3)
    if len(parts) != 4:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-raw-frame-malformed"
        )
    try:
        count = int(parts[2])
    except (TypeError, ValueError):
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-raw-frame-count-invalid"
        ) from None
    fields = parts[3].split("^")
    if (
        not 1 <= count <= 512
        or len(fields) != count * KIS_DOMESTIC_TRADE_FIELD_COUNT
        or frame.get("recordCount") != count
        or frame.get("rawFrameHash") != _frame_hash(raw)
        or frame.get("trId") != _TR_ID
        or frame.get("route") != ROUTE
        or frame.get("pdno") != PDNO
    ):
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-raw-frame-content-invalid"
        )
    records = [
        tuple(
            fields[
                index * KIS_DOMESTIC_TRADE_FIELD_COUNT :
                (index + 1) * KIS_DOMESTIC_TRADE_FIELD_COUNT
            ]
        )
        for index in range(count)
    ]
    if frame.get("recordFields") != [list(record) for record in records]:
        raise KisDomesticFunctionalSourceBlocked(
            "authenticated-frame-record-fields-mismatch"
        )
    return records


def _independent_bars_from_events(
    events: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups: list[list[Mapping[str, Any]]] = []
    for event in events:
        if not groups or groups[-1][0]["bucketOpenAt"] != event["bucketOpenAt"]:
            groups.append([])
        groups[-1].append(event)
    bars: list[dict[str, Any]] = []
    for rows in groups:
        opened = _parse_utc(rows[0]["bucketOpenAt"], "archive.bar.openAt")
        closed = _parse_utc(rows[0]["bucketCloseAt"], "archive.bar.closeAt")
        if any(
            row["bucketOpenAt"] != rows[0]["bucketOpenAt"]
            or row["bucketCloseAt"] != rows[0]["bucketCloseAt"]
            for row in rows
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "archive-event-bucket-membership-invalid"
            )
        prices = [
            _decimal(row["recordFields"][2], "archive.event.price")
            for row in rows
        ]
        chain = _ZERO_HASH
        for row in rows:
            chain = _hash(
                {
                    "previousHash": chain,
                    "rawEventHash": row["rawEventHash"],
                    "sourceSequence": row["sourceSequence"],
                }
            )
        bars.append(
            {
                "openAt": _utc_text(opened, "archive.bar.openAt"),
                "closeAt": _utc_text(closed, "archive.bar.closeAt"),
                "open": _decimal_text(prices[0]),
                "high": _decimal_text(max(prices)),
                "low": _decimal_text(min(prices)),
                "close": _decimal_text(prices[-1]),
                "sourceSequenceStart": rows[0]["sourceSequence"],
                "sourceSequenceEnd": rows[-1]["sourceSequence"],
                "eventCount": len(rows),
                "rawEventChainHash": chain,
            }
        )
    return bars


SOURCE_JOURNAL_SCHEMA_VERSION = "kis-domestic-functional-source-schema/v2"
_TRANSITION_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "armId",
    "sequence",
    "armRevision",
    "transitionKind",
    "fromState",
    "toState",
    "occurredAt",
    "reason",
    "anchorHash",
    "previousHash",
    "authorityKeyIdHash",
}
_SOURCE_SCHEMA_SQL = """
CREATE TABLE kis_public_source_schema_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_public_source_arm (
    arm_id TEXT PRIMARY KEY,
    route TEXT NOT NULL,
    pdno TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'ARMED_WAIT_PUBLIC','NATURAL_BUY_OBSERVED',
        'NEXT_OPEN_TRIGGER_SEALED','TERMINATED_OWNER_LOST',
        'TERMINATED_FAIL_CLOSED'
    )),
    source_generation TEXT NOT NULL UNIQUE,
    socket_identity_hash TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL,
    connected_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    arm_record_json TEXT NOT NULL,
    arm_record_hash TEXT NOT NULL UNIQUE,
    arm_signature TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    last_sequence INTEGER NOT NULL CHECK(last_sequence>=0),
    raw_event_count INTEGER NOT NULL CHECK(raw_event_count>=0),
    raw_frame_count INTEGER NOT NULL CHECK(raw_frame_count>=0),
    raw_head_hash TEXT NOT NULL,
    observation_id TEXT NOT NULL DEFAULT '',
    observation_hash TEXT NOT NULL DEFAULT '',
    terminal_reason TEXT NOT NULL DEFAULT '',
    terminal_at TEXT NOT NULL DEFAULT '',
    transition_count INTEGER NOT NULL DEFAULT 0 CHECK(transition_count>=0),
    transition_head_hash TEXT NOT NULL DEFAULT
        '0000000000000000000000000000000000000000000000000000000000000000',
    revision INTEGER NOT NULL CHECK(revision>=0)
);
CREATE UNIQUE INDEX kis_public_source_arm_active_idx
    ON kis_public_source_arm(route)
    WHERE state IN (
        'ARMED_WAIT_PUBLIC','NATURAL_BUY_OBSERVED','NEXT_OPEN_TRIGGER_SEALED'
    );
CREATE TABLE kis_public_source_frame (
    arm_id TEXT NOT NULL,
    frame_index INTEGER NOT NULL CHECK(frame_index>=1),
    first_sequence INTEGER NOT NULL CHECK(first_sequence>=1),
    last_sequence INTEGER NOT NULL CHECK(last_sequence>=first_sequence),
    received_at TEXT NOT NULL,
    raw_frame_hash TEXT NOT NULL,
    frame_record_json TEXT NOT NULL,
    frame_record_hash TEXT NOT NULL UNIQUE,
    frame_signature TEXT NOT NULL,
    frame_head_hash TEXT NOT NULL,
    PRIMARY KEY (arm_id, frame_index),
    UNIQUE (arm_id, raw_frame_hash),
    FOREIGN KEY (arm_id) REFERENCES kis_public_source_arm(arm_id)
);
CREATE TABLE kis_public_source_event (
    arm_id TEXT NOT NULL,
    source_sequence INTEGER NOT NULL CHECK(source_sequence>=1),
    frame_index INTEGER NOT NULL CHECK(frame_index>=1),
    raw_event_hash TEXT NOT NULL,
    event_record_json TEXT NOT NULL,
    event_record_hash TEXT NOT NULL UNIQUE,
    PRIMARY KEY (arm_id, source_sequence),
    UNIQUE (arm_id, raw_event_hash),
    FOREIGN KEY (arm_id, frame_index)
        REFERENCES kis_public_source_frame(arm_id, frame_index)
);
CREATE TABLE kis_public_source_observation (
    observation_id TEXT PRIMARY KEY,
    arm_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN (
        'NATURAL_BUY_OBSERVED','NEXT_OPEN_TRIGGER_SEALED'
    )),
    observation_record_json TEXT NOT NULL,
    observation_record_hash TEXT NOT NULL UNIQUE,
    observation_signature TEXT NOT NULL,
    trigger_record_json TEXT NOT NULL DEFAULT '',
    trigger_record_hash TEXT NOT NULL DEFAULT '',
    trigger_signature TEXT NOT NULL DEFAULT '',
    evaluation_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=0),
    FOREIGN KEY (arm_id) REFERENCES kis_public_source_arm(arm_id)
);
CREATE TABLE kis_public_source_arm_transition (
    arm_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    arm_revision INTEGER NOT NULL CHECK(arm_revision>=0),
    transition_kind TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    anchor_hash TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    record_json TEXT NOT NULL,
    record_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    PRIMARY KEY (arm_id, sequence),
    UNIQUE (arm_id, arm_revision),
    FOREIGN KEY (arm_id) REFERENCES kis_public_source_arm(arm_id)
);
CREATE TRIGGER kis_public_source_transition_insert_guard
BEFORE INSERT ON kis_public_source_arm_transition
BEGIN
    SELECT CASE WHEN NOT (
        (
            NEW.transition_kind='ARM_CREATED'
            AND NEW.sequence=1 AND NEW.arm_revision=0
            AND NEW.from_state=''
            AND NEW.to_state=(SELECT state FROM kis_public_source_arm
                              WHERE arm_id=NEW.arm_id)
            AND NEW.previous_hash=
                '0000000000000000000000000000000000000000000000000000000000000000'
            AND (SELECT transition_count FROM kis_public_source_arm
                 WHERE arm_id=NEW.arm_id)=0
        )
        OR
        (
            NEW.transition_kind<>'ARM_CREATED'
            AND NEW.sequence=(SELECT transition_count+1
                              FROM kis_public_source_arm
                              WHERE arm_id=NEW.arm_id)
            AND NEW.arm_revision=(SELECT revision+1
                                  FROM kis_public_source_arm
                                  WHERE arm_id=NEW.arm_id)
            AND NEW.from_state=(SELECT state FROM kis_public_source_arm
                                WHERE arm_id=NEW.arm_id)
            AND NEW.previous_hash=(SELECT transition_head_hash
                                   FROM kis_public_source_arm
                                   WHERE arm_id=NEW.arm_id)
        )
    ) THEN RAISE(ABORT, 'kis-source-transition-insert-invalid') END;
END;
CREATE TRIGGER kis_public_source_transition_projection_guard
BEFORE UPDATE OF state, transition_count, transition_head_hash
ON kis_public_source_arm
WHEN NEW.state<>OLD.state
  OR NEW.transition_count<>OLD.transition_count
  OR NEW.transition_head_hash<>OLD.transition_head_hash
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM kis_public_source_arm_transition t
        WHERE t.arm_id=OLD.arm_id
          AND t.sequence=OLD.transition_count+1
          AND t.record_hash=NEW.transition_head_hash
          AND t.to_state=NEW.state
          AND (
              (
                  NEW.state=OLD.state AND OLD.transition_count=0
                  AND NEW.transition_count=1 AND NEW.revision=OLD.revision
                  AND t.transition_kind='ARM_CREATED'
                  AND t.arm_revision=0
              )
              OR
              (
                  NEW.state<>OLD.state
                  AND NEW.transition_count=OLD.transition_count+1
                  AND NEW.revision=OLD.revision+1
                  AND t.from_state=OLD.state
                  AND t.arm_revision=NEW.revision
              )
          )
    ) THEN RAISE(ABORT, 'kis-source-transition-projection-missing') END;
END;
CREATE TRIGGER kis_public_source_transition_update_forbidden
BEFORE UPDATE ON kis_public_source_arm_transition
BEGIN
    SELECT RAISE(ABORT, 'kis-source-transition-immutable');
END;
CREATE TRIGGER kis_public_source_transition_delete_forbidden
BEFORE DELETE ON kis_public_source_arm_transition
BEGIN
    SELECT RAISE(ABORT, 'kis-source-transition-immutable');
END;
"""


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _source_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): {"type": str(row[1]), "sql": _normalize_sql(row[2])}
        for row in conn.execute(
            "SELECT name,type,sql FROM sqlite_master "
            "WHERE name LIKE 'kis_public_source_%' "
            "OR tbl_name LIKE 'kis_public_source_%' ORDER BY name"
        )
    }
    tables: dict[str, Any] = {}
    for name, item in objects.items():
        if item["type"] != "table":
            continue
        indexes = [tuple(row) for row in conn.execute(f'PRAGMA index_list("{name}")')]
        tables[name] = {
            "tableInfo": [tuple(row) for row in conn.execute(f'PRAGMA table_info("{name}")')],
            "foreignKeys": [tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')],
            "indexes": indexes,
            "indexXInfo": {
                str(row[1]): [
                    tuple(value)
                    for value in conn.execute(f'PRAGMA index_xinfo("{row[1]}")')
                ]
                for row in indexes
            },
        }
    return {"objects": objects, "tables": tables}


def _expected_source_schema_snapshot() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SOURCE_SCHEMA_SQL)
        return _source_schema_snapshot(conn)
    finally:
        conn.close()


SOURCE_JOURNAL_SCHEMA_FINGERPRINT = _hash(
    {
        "schemaVersion": SOURCE_JOURNAL_SCHEMA_VERSION,
        "canonicalSql": _normalize_sql(_SOURCE_SCHEMA_SQL),
    }
)
_EXPECTED_SOURCE_SCHEMA_SNAPSHOT = _expected_source_schema_snapshot()


def _verify_exact_source_schema(conn: sqlite3.Connection) -> None:
    if _source_schema_snapshot(conn) != _EXPECTED_SOURCE_SCHEMA_SNAPSHOT:
        raise KisDomesticFunctionalSourceBlocked("public-source-schema-dirty")
    rows = [
        tuple(row)
        for row in conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_public_source_schema_meta"
        )
    ]
    if rows != [
        (1, SOURCE_JOURNAL_SCHEMA_VERSION, SOURCE_JOURNAL_SCHEMA_FINGERPRINT)
    ]:
        raise KisDomesticFunctionalSourceBlocked("public-source-schema-meta-dirty")


class DurableKisDomesticPublicArmJournal:
    """Durable, public-market-data-only ingress journal.

    The journal has no broker, OAuth, account, approval, permit, or mutation
    dependency.  A new arm generation terminalizes any unfinished predecessor
    instead of resuming its socket identity.  Raw H0STCNT0 frames and their
    independently parsed records are committed before the in-memory bar
    reducer is allowed to advance.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        capture_signer: CaptureSigner,
        capture_verifier: CaptureVerifier | None = None,
        server_authority_key_id: str | None = None,
        server_authority_public_key_pem: str | None = None,
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        if self.path.name in {"", ".", ".."} or self.path == self.path.parent:
            raise KisDomesticFunctionalSourceBlocked("public-arm-journal-path-invalid")
        if not self.path.parent.is_dir():
            raise KisDomesticFunctionalSourceBlocked(
                "public-arm-journal-parent-missing"
            )
        if not callable(capture_signer) or (
            capture_verifier is not None and not callable(capture_verifier)
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "capture-signer-and-verifier-required"
            )
        if (server_authority_key_id is None) == (
            server_authority_public_key_pem is None
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "server-authority-identity-not-exact"
            )
        self._capture_signer = capture_signer
        if server_authority_public_key_pem is not None:
            if type(server_authority_public_key_pem) is not str:
                raise KisDomesticFunctionalSourceBlocked(
                    "server-authority-public-key-invalid"
                )
            try:
                public_key = ECC.import_key(server_authority_public_key_pem)
                exported = public_key.public_key().export_key(format="PEM")
            except (ValueError, TypeError, IndexError):
                raise KisDomesticFunctionalSourceBlocked(
                    "server-authority-public-key-invalid"
                ) from None
            if (
                public_key.has_private()
                or public_key.curve != "Ed25519"
                or exported != server_authority_public_key_pem
            ):
                raise KisDomesticFunctionalSourceBlocked(
                    "server-authority-public-key-not-canonical-ed25519"
                )

            def bound_verifier(
                domain: str, body: Mapping[str, Any], signature: str
            ) -> bool:
                try:
                    raw_signature = base64.b64decode(signature, validate=True)
                    if (
                        len(raw_signature) != 64
                        or base64.b64encode(raw_signature).decode("ascii")
                        != signature
                    ):
                        return False
                    eddsa.new(public_key, mode="rfc8032").verify(
                        domain.encode("ascii") + b"\x00" + _canonical(body),
                        raw_signature,
                    )
                    return bool(
                        capture_verifier is None
                        or capture_verifier(
                            domain, deepcopy(dict(body)), signature
                        ) is True
                    )
                except BaseException:
                    return False

            self._capture_verifier = bound_verifier
            self.server_authority_key_id_hash = hashlib.sha256(
                exported.encode("utf-8")
            ).hexdigest()
            self.server_authority_identity_mode = "REGISTRY_ED25519_PUBLIC_KEY"
            self.server_authority_offline_mock_only = False
            self._signature_encoding = "ED25519_BASE64"
        else:
            if (
                type(server_authority_key_id) is not str
                or not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
                    server_authority_key_id,
                )
                or capture_verifier is None
            ):
                raise KisDomesticFunctionalSourceBlocked(
                    "server-authority-key-id-invalid"
                )
            self._capture_verifier = capture_verifier
            self.server_authority_key_id_hash = hashlib.sha256(
                server_authority_key_id.encode("utf-8")
            ).hexdigest()
            self.server_authority_identity_mode = "OFFLINE_MOCK_STRING_KEY_ID"
            self.server_authority_offline_mock_only = True
            self._signature_encoding = "HMAC_SHA256_HEX"
        self._lock = threading.RLock()
        self._schema_ready = False
        self._ensure_schema()
        self._schema_ready = True

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        if self._schema_ready:
            try:
                _verify_exact_source_schema(conn)
            except BaseException:
                conn.close()
                raise
        return conn

    def _ensure_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                existing = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE name LIKE 'kis_public_source_%'"
                ).fetchall()
                if not existing:
                    conn.executescript(_SOURCE_SCHEMA_SQL)
                    conn.execute(
                        "INSERT INTO kis_public_source_schema_meta VALUES(1,?,?)",
                        (
                            SOURCE_JOURNAL_SCHEMA_VERSION,
                            SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
                        ),
                    )
                    conn.commit()
                _verify_exact_source_schema(conn)
            finally:
                conn.close()

    @staticmethod
    def _decode_mapping(text: Any, label: str) -> dict[str, Any]:
        if type(text) is not str or not text:
            raise KisDomesticFunctionalSourceBlocked(f"{label}-missing")
        try:
            value = json.loads(text)
        except (TypeError, ValueError):
            raise KisDomesticFunctionalSourceBlocked(f"{label}-invalid") from None
        if not isinstance(value, dict) or _canonical(value).decode("utf-8") != text:
            raise KisDomesticFunctionalSourceBlocked(f"{label}-not-canonical")
        return value

    @staticmethod
    def _assert_hash(actual: Any, body: Mapping[str, Any], label: str) -> None:
        if (
            type(actual) is not str
            or not _SHA256.fullmatch(actual)
            or not hmac.compare_digest(actual, _hash(body))
        ):
            raise KisDomesticFunctionalSourceBlocked(f"{label}-hash-mismatch")

    def _assert_signature(self, signature: Any, label: str) -> None:
        valid = _signature_text(signature)
        if self._signature_encoding == "ED25519_BASE64":
            try:
                decoded = base64.b64decode(signature, validate=True)
                valid = bool(
                    len(decoded) == 64
                    and base64.b64encode(decoded).decode("ascii") == signature
                )
            except (TypeError, binascii.Error, ValueError):
                valid = False
        if not valid:
            raise KisDomesticFunctionalSourceBlocked(f"{label}-signature-invalid")

    def _verify_signature(
        self,
        domain: str,
        body: Mapping[str, Any],
        signature: Any,
        label: str,
    ) -> None:
        self._assert_signature(signature, label)
        try:
            verified = self._capture_verifier(
                domain,
                deepcopy(dict(body)),
                str(signature),
            )
        except BaseException as exc:
            raise KisDomesticFunctionalSourceBlocked(
                f"{label}-trusted-verifier-failed:{type(exc).__name__}"
            ) from None
        if verified is not True:
            raise KisDomesticFunctionalSourceBlocked(
                f"{label}-trusted-signature-mismatch"
            )

    def _sign_transition(
        self, body: Mapping[str, Any]
    ) -> tuple[str, str, str]:
        record = deepcopy(dict(body))
        record_hash = _hash(record)
        signed = {**record, "recordHash": record_hash}
        try:
            signature = self._capture_signer("PUBLIC_ARM_TRANSITION", signed)
        except BaseException as exc:
            raise KisDomesticFunctionalSourceBlocked(
                f"arm-transition-signer-failed:{type(exc).__name__}"
            ) from None
        self._verify_signature(
            "PUBLIC_ARM_TRANSITION",
            signed,
            signature,
            "arm-transition",
        )
        return _canonical(record).decode("utf-8"), record_hash, signature

    def _append_transition(
        self,
        conn: sqlite3.Connection,
        *,
        arm: sqlite3.Row,
        transition_kind: str,
        to_state: str,
        occurred_at: str,
        reason: str,
        anchor_hash: str,
        initial: bool = False,
    ) -> str:
        _parse_utc(occurred_at, "armTransition.occurredAt")
        if type(reason) is not str or not re.fullmatch(r"[A-Z0-9_]{3,96}", reason):
            raise KisDomesticFunctionalSourceBlocked("arm-transition-reason-invalid")
        if type(anchor_hash) is not str or not _SHA256.fullmatch(anchor_hash):
            raise KisDomesticFunctionalSourceBlocked(
                "arm-transition-anchor-hash-invalid"
            )
        sequence = int(arm["transition_count"]) + 1
        previous_hash = str(arm["transition_head_hash"])
        arm_revision = int(arm["revision"]) if initial else int(arm["revision"]) + 1
        body = {
            "schemaVersion": "kis-domestic-functional-source-arm-transition/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "armId": str(arm["arm_id"]),
            "sequence": sequence,
            "armRevision": arm_revision,
            "transitionKind": transition_kind,
            "fromState": "" if initial else str(arm["state"]),
            "toState": to_state,
            "occurredAt": occurred_at,
            "reason": reason,
            "anchorHash": anchor_hash,
            "previousHash": previous_hash,
            "authorityKeyIdHash": self.server_authority_key_id_hash,
        }
        record_json, record_hash, signature = self._sign_transition(body)
        conn.execute(
            """INSERT INTO kis_public_source_arm_transition
               (arm_id,sequence,arm_revision,transition_kind,from_state,
                to_state,occurred_at,reason,anchor_hash,previous_hash,
                record_json,record_hash,signature,authority_key_id_hash)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                body["armId"], sequence, arm_revision, transition_kind,
                body["fromState"], to_state, occurred_at, reason, anchor_hash,
                previous_hash, record_json, record_hash, signature,
                self.server_authority_key_id_hash,
            ),
        )
        return record_hash

    def _verify_transition_chain(
        self, conn: sqlite3.Connection, arm: sqlite3.Row
    ) -> None:
        rows = conn.execute(
            "SELECT * FROM kis_public_source_arm_transition "
            "WHERE arm_id=? ORDER BY sequence",
            (arm["arm_id"],),
        ).fetchall()
        if len(rows) != int(arm["transition_count"]) or not rows:
            raise KisDomesticFunctionalSourceBlocked(
                "arm-transition-count-incomplete"
            )
        previous = _ZERO_HASH
        previous_state = ""
        previous_revision = -1
        previous_occurred: datetime | None = None
        for index, row in enumerate(rows, start=1):
            body = self._decode_mapping(
                row["record_json"], "arm-transition-record"
            )
            if set(body) != _TRANSITION_KEYS:
                raise KisDomesticFunctionalSourceBlocked(
                    "arm-transition-record-fields-not-exact"
                )
            self._assert_hash(
                row["record_hash"], body, "arm-transition-record"
            )
            self._verify_signature(
                "PUBLIC_ARM_TRANSITION",
                {**body, "recordHash": str(row["record_hash"])},
                row["signature"],
                "arm-transition-record",
            )
            projection = {
                "armId": str(row["arm_id"]),
                "sequence": int(row["sequence"]),
                "armRevision": int(row["arm_revision"]),
                "transitionKind": str(row["transition_kind"]),
                "fromState": str(row["from_state"]),
                "toState": str(row["to_state"]),
                "occurredAt": str(row["occurred_at"]),
                "reason": str(row["reason"]),
                "anchorHash": str(row["anchor_hash"]),
                "previousHash": str(row["previous_hash"]),
                "authorityKeyIdHash": str(row["authority_key_id_hash"]),
            }
            if (
                body.get("schemaVersion")
                != "kis-domestic-functional-source-arm-transition/v1"
                or body.get("route") != ROUTE
                or body.get("pdno") != PDNO
                or any(body.get(key) != value for key, value in projection.items())
                or int(row["sequence"]) != index
                or str(row["previous_hash"]) != previous
                or str(row["authority_key_id_hash"])
                != self.server_authority_key_id_hash
                or (index == 1 and (
                    row["transition_kind"] != "ARM_CREATED"
                    or row["from_state"] != ""
                    or int(row["arm_revision"]) != 0
                ))
                or (index > 1 and (
                    row["from_state"] != previous_state
                    or int(row["arm_revision"]) <= previous_revision
                ))
            ):
                raise KisDomesticFunctionalSourceBlocked(
                    "arm-transition-chain-mismatch"
                )
            occurred = _parse_utc(
                body["occurredAt"], "armTransition.occurredAt"
            )
            if previous_occurred is not None and occurred < previous_occurred:
                raise KisDomesticFunctionalSourceBlocked(
                    "arm-transition-time-regressed"
                )
            kind = str(row["transition_kind"])
            from_state = str(row["from_state"])
            to_state = str(row["to_state"])
            anchor_hash = str(row["anchor_hash"])
            exact_semantics = (
                kind == "ARM_CREATED"
                and from_state == ""
                and to_state == "ARMED_WAIT_PUBLIC"
                and row["reason"] == "PUBLIC_ARM_CREATED"
                and anchor_hash == str(arm["arm_record_hash"])
            ) or (
                kind == "OWNER_LOSS_TERMINAL"
                and from_state in _ACTIVE_ARM_STATES
                and to_state == "TERMINATED_OWNER_LOST"
                and row["reason"] == "SOURCE_OWNER_OR_PROCESS_REPLACED"
                and conn.execute(
                    "SELECT COUNT(*) FROM kis_public_source_arm "
                    "WHERE arm_record_hash=? AND arm_id<>?",
                    (anchor_hash, arm["arm_id"]),
                ).fetchone()[0] == 1
            ) or (
                kind == "NATURAL_BUY_OBSERVATION_SEALED"
                and from_state == "ARMED_WAIT_PUBLIC"
                and to_state == "NATURAL_BUY_OBSERVED"
                and row["reason"] == "NATURAL_BUY_OBSERVATION_SEALED"
                and anchor_hash == str(arm["observation_hash"])
                and conn.execute(
                    "SELECT COUNT(*) FROM kis_public_source_observation "
                    "WHERE arm_id=? AND observation_record_hash=?",
                    (arm["arm_id"], anchor_hash),
                ).fetchone()[0] == 1
            ) or (
                kind == "NEXT_OPEN_TRIGGER_SEALED"
                and from_state == "NATURAL_BUY_OBSERVED"
                and to_state == "NEXT_OPEN_TRIGGER_SEALED"
                and row["reason"] == "NEXT_OPEN_TRIGGER_SEALED"
                and conn.execute(
                    "SELECT COUNT(*) FROM kis_public_source_observation "
                    "WHERE arm_id=? AND trigger_record_hash=?",
                    (arm["arm_id"], anchor_hash),
                ).fetchone()[0] == 1
            ) or (
                kind == "FAIL_CLOSED_TERMINAL"
                and from_state in _ACTIVE_ARM_STATES
                and to_state == "TERMINATED_FAIL_CLOSED"
                and row["reason"] == str(arm["terminal_reason"])
                and body["occurredAt"] == str(arm["terminal_at"])
                and anchor_hash == _hash(
                    {
                        "reason": str(arm["terminal_reason"]),
                        "terminalAt": str(arm["terminal_at"]),
                        "rawHeadHash": str(arm["raw_head_hash"]),
                        "observationHash": str(arm["observation_hash"]),
                    }
                )
            )
            if not exact_semantics:
                raise KisDomesticFunctionalSourceBlocked(
                    "arm-transition-semantic-anchor-mismatch"
                )
            previous = str(row["record_hash"])
            previous_state = str(row["to_state"])
            previous_revision = int(row["arm_revision"])
            previous_occurred = occurred
        if (
            previous != str(arm["transition_head_hash"])
            or previous_state != str(arm["state"])
            or previous_revision > int(arm["revision"])
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "arm-transition-head-or-state-mismatch"
            )

    @staticmethod
    def _active_sql() -> str:
        return "('ARMED_WAIT_PUBLIC','NATURAL_BUY_OBSERVED','NEXT_OPEN_TRIGGER_SEALED')"

    def begin_arm(
        self,
        *,
        arm_record: Mapping[str, Any],
        arm_signature: str,
        owner_token_hash: str,
    ) -> tuple[str, tuple[str, ...]]:
        body = deepcopy(dict(arm_record))
        arm_id = body.get("armId")
        generation = body.get("sourceGeneration")
        socket_hash = body.get("socketIdentityHash")
        connected_at = body.get("connectedAt")
        created_at = body.get("createdAt")
        if (
            type(arm_id) is not str
            or not _ARM_ID.fullmatch(arm_id)
            or type(generation) is not str
            or not _GENERATION.fullmatch(generation)
            or type(socket_hash) is not str
            or not _SHA256.fullmatch(socket_hash)
            or type(owner_token_hash) is not str
            or not _SHA256.fullmatch(owner_token_hash)
            or body.get("route") != ROUTE
            or body.get("pdno") != PDNO
            or body.get("state") != "ARMED_WAIT_PUBLIC"
            or body.get("serverAuthorityKeyIdHash")
            != self.server_authority_key_id_hash
        ):
            raise KisDomesticFunctionalSourceBlocked("public-arm-record-invalid")
        _parse_utc(connected_at, "arm.connectedAt")
        _parse_utc(created_at, "arm.createdAt")
        self._verify_signature("PUBLIC_ARM", body, arm_signature, "public-arm")
        record_text = _canonical(body).decode("utf-8")
        record_hash = _hash(body)
        terminated: list[str] = []
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    "SELECT * FROM kis_public_source_arm "
                    f"WHERE route=? AND state IN {self._active_sql()}",
                    (ROUTE,),
                ).fetchall()
                for row in rows:
                    self._verify_transition_chain(conn, row)
                    old_arm_id = str(row["arm_id"])
                    transition_head = self._append_transition(
                        conn,
                        arm=row,
                        transition_kind="OWNER_LOSS_TERMINAL",
                        to_state="TERMINATED_OWNER_LOST",
                        occurred_at=created_at,
                        reason="SOURCE_OWNER_OR_PROCESS_REPLACED",
                        anchor_hash=record_hash,
                    )
                    changed = conn.execute(
                        """UPDATE kis_public_source_arm
                           SET state='TERMINATED_OWNER_LOST',
                               terminal_reason='SOURCE_OWNER_OR_PROCESS_REPLACED',
                               terminal_at=?, transition_count=transition_count+1,
                               transition_head_hash=?, revision=revision+1
                           WHERE arm_id=? AND revision=?
                             AND state IN ('ARMED_WAIT_PUBLIC',
                                           'NATURAL_BUY_OBSERVED',
                                           'NEXT_OPEN_TRIGGER_SEALED')""",
                        (
                            created_at, transition_head, old_arm_id,
                            int(row["revision"]),
                        ),
                    )
                    if changed.rowcount != 1:
                        raise KisDomesticFunctionalSourceBlocked(
                            "public-arm-owner-loss-cas-failed"
                        )
                    terminated.append(old_arm_id)
                conn.execute(
                    """INSERT INTO kis_public_source_arm
                       (arm_id, route, pdno, state, source_generation,
                        socket_identity_hash, owner_token_hash, connected_at,
                        created_at, arm_record_json, arm_record_hash,
                        arm_signature, authority_key_id_hash, last_sequence, raw_event_count,
                        raw_frame_count, raw_head_hash, revision)
                       VALUES (?, ?, ?, 'ARMED_WAIT_PUBLIC', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                               0, 0, 0, ?, 0)""",
                    (
                        arm_id,
                        ROUTE,
                        PDNO,
                        generation,
                        socket_hash,
                        owner_token_hash,
                        connected_at,
                        created_at,
                        record_text,
                        record_hash,
                        arm_signature,
                        self.server_authority_key_id_hash,
                        _ZERO_HASH,
                    ),
                )
                new_arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                initial_head = self._append_transition(
                    conn,
                    arm=new_arm,
                    transition_kind="ARM_CREATED",
                    to_state="ARMED_WAIT_PUBLIC",
                    occurred_at=created_at,
                    reason="PUBLIC_ARM_CREATED",
                    anchor_hash=record_hash,
                    initial=True,
                )
                projected = conn.execute(
                    """UPDATE kis_public_source_arm
                       SET transition_count=1,transition_head_hash=?
                       WHERE arm_id=? AND state='ARMED_WAIT_PUBLIC'
                         AND transition_count=0 AND revision=0""",
                    (initial_head, arm_id),
                )
                if projected.rowcount != 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "public-arm-initial-transition-projection-failed"
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
        return arm_id, tuple(terminated)

    def append_frame(
        self,
        *,
        arm_id: str,
        owner_token_hash: str,
        frame_record: Mapping[str, Any],
        frame_signature: str,
        event_records: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        frame = deepcopy(dict(frame_record))
        events = [deepcopy(dict(row)) for row in event_records]
        if not events:
            raise KisDomesticFunctionalSourceBlocked("raw-frame-events-missing")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                if arm is not None:
                    self._verify_transition_chain(conn, arm)
                if (
                    arm is None
                    or str(arm["state"]) != "ARMED_WAIT_PUBLIC"
                    or not hmac.compare_digest(
                        str(arm["owner_token_hash"]), owner_token_hash
                    )
                    or str(arm["authority_key_id_hash"])
                    != self.server_authority_key_id_hash
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "public-arm-owner-or-state-invalid"
                    )
                expected_frame = int(arm["raw_frame_count"]) + 1
                expected_first = int(arm["last_sequence"]) + 1
                expected_last = expected_first + len(events) - 1
                if (
                    frame.get("schemaVersion")
                    != "kis-domestic-h0stcnt0-raw-frame-envelope/v1"
                    or frame.get("armId") != arm_id
                    or frame.get("route") != ROUTE
                    or frame.get("pdno") != PDNO
                    or frame.get("sourceGeneration")
                    != str(arm["source_generation"])
                    or frame.get("socketIdentityHash")
                    != str(arm["socket_identity_hash"])
                    or frame.get("frameIndex") != expected_frame
                    or frame.get("firstSourceSequence") != str(expected_first)
                    or frame.get("lastSourceSequence") != str(expected_last)
                    or frame.get("recordCount") != len(events)
                    or frame.get("previousFrameHeadHash")
                    != str(arm["raw_head_hash"])
                    or type(frame.get("rawFrame")) is not str
                    or _frame_hash(frame["rawFrame"]) != frame.get("rawFrameHash")
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "raw-frame-envelope-invalid"
                    )
                self._verify_signature(
                    "RAW_H0STCNT0_FRAME",
                    frame,
                    frame_signature,
                    "raw-frame",
                )
                _parse_utc(frame.get("receivedAt"), "rawFrame.receivedAt")
                frame_hash = _hash(frame)
                frame_head = _hash(
                    {
                        "previousHash": str(arm["raw_head_hash"]),
                        "frameEnvelopeHash": frame_hash,
                        "frameIndex": expected_frame,
                    }
                )
                conn.execute(
                    """INSERT INTO kis_public_source_frame
                       (arm_id, frame_index, first_sequence, last_sequence,
                        received_at, raw_frame_hash, frame_record_json,
                        frame_record_hash, frame_signature, frame_head_hash)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        arm_id,
                        expected_frame,
                        expected_first,
                        expected_last,
                        frame["receivedAt"],
                        frame["rawFrameHash"],
                        _canonical(frame).decode("utf-8"),
                        frame_hash,
                        frame_signature,
                        frame_head,
                    ),
                )
                for offset, event in enumerate(events):
                    sequence = expected_first + offset
                    if (
                        event.get("armId") != arm_id
                        or event.get("sourceGeneration")
                        != str(arm["source_generation"])
                        or event.get("socketIdentityHash")
                        != str(arm["socket_identity_hash"])
                        or event.get("sourceSequence") != str(sequence)
                        or event.get("frameEnvelopeHash") != frame_hash
                        or event.get("rawFrameHash") != frame["rawFrameHash"]
                        or event.get("recordIndex") != offset
                        or event.get("recordFields")
                        != frame.get("recordFields", [])[offset]
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "raw-event-envelope-invalid"
                        )
                    expected_event_hash = _hash(
                        _raw_event_hash_body(
                            source_generation=str(arm["source_generation"]),
                            socket_identity_hash=str(arm["socket_identity_hash"]),
                            source_sequence=str(sequence),
                            record_index=offset,
                            raw_frame_hash=frame["rawFrameHash"],
                            record_fields=list(event["recordFields"]),
                            received_at=event["receivedAt"],
                        )
                    )
                    if not hmac.compare_digest(
                        str(event.get("rawEventHash", "")), expected_event_hash
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "raw-event-proof-hash-mismatch"
                        )
                    conn.execute(
                        """INSERT INTO kis_public_source_event
                           (arm_id, source_sequence, frame_index, raw_event_hash,
                            event_record_json, event_record_hash)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            arm_id,
                            sequence,
                            expected_frame,
                            event["rawEventHash"],
                            _canonical(event).decode("utf-8"),
                            _hash(event),
                        ),
                    )
                changed = conn.execute(
                    """UPDATE kis_public_source_arm
                       SET last_sequence=?, raw_event_count=?, raw_frame_count=?,
                           raw_head_hash=?, revision=revision+1
                       WHERE arm_id=? AND state='ARMED_WAIT_PUBLIC'
                         AND owner_token_hash=? AND revision=?""",
                    (
                        expected_last,
                        expected_last,
                        expected_frame,
                        frame_head,
                        arm_id,
                        owner_token_hash,
                        int(arm["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "raw-frame-arm-cas-failed"
                    )
                conn.commit()
                return {
                    "frameIndex": expected_frame,
                    "frameEnvelopeHash": frame_hash,
                    "frameHeadHash": frame_head,
                    "lastSourceSequence": str(expected_last),
                }
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def window_archive(
        self,
        *,
        arm_id: str,
        owner_token_hash: str,
        first_sequence: int,
        last_sequence: int,
        next_open_sequence: int,
        expected_bars: list[Mapping[str, Any]],
        expected_source_proof_hash: str,
    ) -> dict[str, Any]:
        if (
            first_sequence < 1
            or last_sequence < first_sequence
            or next_open_sequence != last_sequence + 1
            or len(expected_bars) != 11
            or type(expected_source_proof_hash) is not str
            or not _SHA256.fullmatch(expected_source_proof_hash)
        ):
            raise KisDomesticFunctionalSourceBlocked("raw-window-range-invalid")
        with self._lock:
            conn = self._connect()
            try:
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                if arm is not None:
                    self._verify_transition_chain(conn, arm)
                if (
                    arm is None
                    or str(arm["state"]) != "ARMED_WAIT_PUBLIC"
                    or not hmac.compare_digest(
                        str(arm["owner_token_hash"]), owner_token_hash
                    )
                    or int(arm["last_sequence"]) != next_open_sequence
                    or int(arm["raw_event_count"]) != next_open_sequence
                    or str(arm["authority_key_id_hash"])
                    != self.server_authority_key_id_hash
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "raw-window-arm-invalid"
                    )
                arm_record = self._decode_mapping(
                    arm["arm_record_json"], "durable-public-arm"
                )
                self._assert_hash(
                    arm["arm_record_hash"], arm_record, "durable-public-arm"
                )
                self._verify_signature(
                    "PUBLIC_ARM",
                    arm_record,
                    arm["arm_signature"],
                    "durable-public-arm",
                )
                if (
                    arm_record.get("armId") != arm_id
                    or arm_record.get("sourceGeneration")
                    != str(arm["source_generation"])
                    or arm_record.get("socketIdentityHash")
                    != str(arm["socket_identity_hash"])
                    or arm_record.get("serverAuthorityKeyIdHash")
                    != self.server_authority_key_id_hash
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "durable-public-arm-lineage-mismatch"
                    )
                event_rows = conn.execute(
                    """SELECT * FROM kis_public_source_event
                       WHERE arm_id=? ORDER BY source_sequence""",
                    (arm_id,),
                ).fetchall()
                expected_sequences = list(range(1, next_open_sequence + 1))
                if [int(row["source_sequence"]) for row in event_rows] != expected_sequences:
                    raise KisDomesticFunctionalSourceBlocked(
                        "raw-window-sequence-gap-or-duplicate"
                    )
                events_by_sequence = {
                    int(row["source_sequence"]): row for row in event_rows
                }
                verified_events: list[dict[str, Any]] = []
                frames: list[dict[str, Any]] = []
                frame_rows = conn.execute(
                    """SELECT * FROM kis_public_source_frame
                       WHERE arm_id=? ORDER BY frame_index""",
                    (arm_id,),
                ).fetchall()
                if [int(row["frame_index"]) for row in frame_rows] != list(
                    range(1, int(arm["raw_frame_count"]) + 1)
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "durable-raw-frame-gap-or-duplicate"
                    )
                previous_head = _ZERO_HASH
                expected_sequence = 1
                for row in frame_rows:
                    frame_index = int(row["frame_index"])
                    frame = self._decode_mapping(
                        row["frame_record_json"], "durable-raw-frame"
                    )
                    self._assert_hash(
                        row["frame_record_hash"], frame, "durable-raw-frame"
                    )
                    self._verify_signature(
                        "RAW_H0STCNT0_FRAME",
                        frame,
                        row["frame_signature"],
                        "durable-raw-frame",
                    )
                    records = _independent_parse_authenticated_frame(frame)
                    frame_hash = str(row["frame_record_hash"])
                    expected_last = expected_sequence + len(records) - 1
                    if (
                        frame.get("armId") != arm_id
                        or frame.get("sourceGeneration")
                        != str(arm["source_generation"])
                        or frame.get("socketIdentityHash")
                        != str(arm["socket_identity_hash"])
                        or frame.get("frameIndex") != frame_index
                        or frame.get("firstSourceSequence")
                        != str(expected_sequence)
                        or frame.get("lastSourceSequence") != str(expected_last)
                        or int(row["first_sequence"]) != expected_sequence
                        or int(row["last_sequence"]) != expected_last
                        or frame.get("previousFrameHeadHash") != previous_head
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "durable-raw-frame-lineage-mismatch"
                        )
                    expected_head = _hash(
                        {
                            "previousHash": previous_head,
                            "frameEnvelopeHash": frame_hash,
                            "frameIndex": frame_index,
                        }
                    )
                    if not hmac.compare_digest(
                        str(row["frame_head_hash"]), expected_head
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "durable-raw-frame-head-mismatch"
                        )
                    received = _parse_utc(
                        frame["receivedAt"], "authenticatedFrame.receivedAt"
                    )
                    for record_index, record in enumerate(records):
                        sequence = expected_sequence + record_index
                        event_row = events_by_sequence.get(sequence)
                        if (
                            event_row is None
                            or int(event_row["frame_index"]) != frame_index
                        ):
                            raise KisDomesticFunctionalSourceBlocked(
                                "durable-raw-event-frame-join-mismatch"
                            )
                        temporal = _independent_record_truth(
                            record,
                            received_at=received,
                        )
                        raw_event_body = _raw_event_hash_body(
                            source_generation=str(arm["source_generation"]),
                            socket_identity_hash=str(arm["socket_identity_hash"]),
                            source_sequence=str(sequence),
                            record_index=record_index,
                            raw_frame_hash=frame["rawFrameHash"],
                            record_fields=list(record),
                            received_at=frame["receivedAt"],
                        )
                        raw_event_hash = _hash(raw_event_body)
                        expected_event = {
                            "schemaVersion": (
                                "kis-domestic-h0stcnt0-raw-event-envelope/v1"
                            ),
                            "route": ROUTE,
                            "pdno": PDNO,
                            "armId": arm_id,
                            "sourceGeneration": str(arm["source_generation"]),
                            "socketIdentityHash": str(
                                arm["socket_identity_hash"]
                            ),
                            "sourceSequence": str(sequence),
                            "feedSourceSequence": ":".join(
                                (PDNO, record[33], record[1], str(record_index))
                            ),
                            "frameEnvelopeHash": frame_hash,
                            "rawFrameHash": frame["rawFrameHash"],
                            "rawEventHash": raw_event_hash,
                            "recordIndex": record_index,
                            "tradeAt": temporal["tradeAt"],
                            "receivedAt": temporal["receivedAt"],
                            "bucketOpenAt": temporal["bucketOpenAt"],
                            "bucketCloseAt": temporal["bucketCloseAt"],
                            "recordFields": list(record),
                        }
                        stored_event = self._decode_mapping(
                            event_row["event_record_json"], "durable-raw-event"
                        )
                        if (
                            stored_event != expected_event
                            or str(event_row["raw_event_hash"])
                            != raw_event_hash
                            or not hmac.compare_digest(
                                str(event_row["event_record_hash"]),
                                _hash(expected_event),
                            )
                        ):
                            raise KisDomesticFunctionalSourceBlocked(
                                "durable-raw-event-authenticated-frame-mismatch"
                            )
                        verified_events.append(expected_event)
                    frames.append(
                        {
                            "body": frame,
                            "envelopeHash": str(row["frame_record_hash"]),
                            "serverSignature": str(row["frame_signature"]),
                            "frameHeadHash": str(row["frame_head_hash"]),
                        }
                    )
                    previous_head = expected_head
                    expected_sequence = expected_last + 1
                if (
                    expected_sequence != next_open_sequence + 1
                    or not hmac.compare_digest(
                        previous_head, str(arm["raw_head_hash"])
                    )
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "durable-ingress-head-or-count-mismatch"
                    )
                window_events = verified_events[
                    first_sequence - 1 : last_sequence
                ]
                if [int(row["sourceSequence"]) for row in window_events] != list(
                    range(first_sequence, last_sequence + 1)
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "authenticated-window-event-range-mismatch"
                    )
                recomputed_bars = _independent_bars_from_events(window_events)
                canonical_expected_bars = [
                    deepcopy(dict(row)) for row in expected_bars
                ]
                if recomputed_bars != canonical_expected_bars:
                    raise KisDomesticFunctionalSourceBlocked(
                        "authenticated-archive-window-bars-mismatch"
                    )
                for previous, current in zip(
                    recomputed_bars, recomputed_bars[1:]
                ):
                    if previous["closeAt"] != current["openAt"]:
                        raise KisDomesticFunctionalSourceBlocked(
                            "authenticated-archive-window-not-contiguous"
                        )
                source_proof = {
                    "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "sourceProvider": _SOURCE_PROVIDER,
                    "sourceGeneration": str(arm["source_generation"]),
                    "firstSourceSequence": str(first_sequence),
                    "lastSourceSequence": str(last_sequence),
                    "sourceEventCount": len(window_events),
                    "barRawEventChainHashes": [
                        bar["rawEventChainHash"] for bar in recomputed_bars
                    ],
                }
                if not hmac.compare_digest(
                    _hash(source_proof), expected_source_proof_hash
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "authenticated-archive-source-proof-mismatch"
                    )
                next_open_event = verified_events[next_open_sequence - 1]
                if (
                    next_open_event["sourceSequence"] != str(next_open_sequence)
                    or next_open_event["bucketOpenAt"]
                    != recomputed_bars[-1]["closeAt"]
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "authenticated-next-open-event-mismatch"
                    )
                linked_frames = [
                    item for item in frames
                    if any(key in item["body"] for key in _MARKET_SOURCE_LINK_KEYS)
                ]
                market_links: list[dict[str, Any]] = []
                market_head = _ZERO_HASH
                if linked_frames and len(linked_frames) != len(frames):
                    raise KisDomesticFunctionalSourceBlocked(
                        "market-source-frame-link-partial"
                    )
                for expected_ordinal, item in enumerate(linked_frames, 1):
                    frame_body = item["body"]
                    if not _MARKET_SOURCE_LINK_KEYS <= set(frame_body):
                        raise KisDomesticFunctionalSourceBlocked(
                            "market-source-frame-link-incomplete"
                        )
                    exact = {
                        "marketSourceLinkSchema": MARKET_SOURCE_LINK_SCHEMA,
                        "marketSourceIngressOrdinal": expected_ordinal,
                        "marketSourceRawFrameHash": frame_body["rawFrameHash"],
                        "marketSourcePreviousIngressHeadHash": market_head,
                        "marketSourceAuthorityPurpose": (
                            "MARKET_SOURCE_RECORD_VERIFY"
                        ),
                    }
                    if any(
                        type(frame_body.get(key)) is not type(wanted)
                        or frame_body.get(key) != wanted
                        for key, wanted in exact.items()
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "market-source-frame-link-lineage-mismatch"
                        )
                    for key in (
                        "marketSourceAccountFingerprint",
                        "marketSourceOwnerEpochHash",
                        "marketSourceRawRecordHash",
                        "marketSourceRawFrameHash",
                        "marketSourcePreviousIngressHeadHash",
                        "marketSourceAuthorityKeyIdHash",
                        "marketSourceArmTransitionHeadHash",
                    ):
                        if type(frame_body.get(key)) is not str or not _SHA256.fullmatch(
                            frame_body[key]
                        ):
                            raise KisDomesticFunctionalSourceBlocked(
                                "market-source-frame-link-hash-invalid"
                            )
                    if (
                        type(frame_body.get("marketSourceSessionId")) is not str
                        or not frame_body["marketSourceSessionId"]
                        or type(frame_body.get("marketSourceOwnerEpoch")) is not int
                        or frame_body["marketSourceOwnerEpoch"] < 1
                        or type(frame_body.get("marketSourceOwnerEpochId")) is not str
                        or not frame_body["marketSourceOwnerEpochId"]
                        or type(frame_body.get("marketSourceProcessGeneration"))
                        is not str
                        or not frame_body["marketSourceProcessGeneration"]
                    ):
                        raise KisDomesticFunctionalSourceBlocked(
                            "market-source-frame-link-identity-invalid"
                        )
                    market_head = _hash(
                        {
                            "schemaVersion": (
                                "kis-domestic-functional-market-source-head/v2"
                            ),
                            "sourceGeneration": str(arm["source_generation"]),
                            "ingressOrdinal": expected_ordinal,
                            "previousIngressHeadHash": frame_body[
                                "marketSourcePreviousIngressHeadHash"
                            ],
                            "rawRecordHash": frame_body[
                                "marketSourceRawRecordHash"
                            ],
                            "sourceArmId": arm_id,
                            "sourceFrameIndex": frame_body["frameIndex"],
                            "sourceFrameEnvelopeHash": item["envelopeHash"],
                            "sourceFrameHeadHash": item["frameHeadHash"],
                            "sourceArmTransitionHeadHash": frame_body[
                                "marketSourceArmTransitionHeadHash"
                            ],
                        }
                    )
                    market_links.append(
                        {
                            "sourceGeneration": str(arm["source_generation"]),
                            "ingressOrdinal": expected_ordinal,
                            "rawRecordHash": frame_body[
                                "marketSourceRawRecordHash"
                            ],
                            "rawFrameHash": frame_body[
                                "marketSourceRawFrameHash"
                            ],
                            "sourceFrameIndex": frame_body["frameIndex"],
                            "sourceFrameEnvelopeHash": item["envelopeHash"],
                            "sourceFrameHeadHash": item["frameHeadHash"],
                            "sourceArmTransitionHeadHash": frame_body[
                                "marketSourceArmTransitionHeadHash"
                            ],
                            "computedMarketIngressHeadHash": market_head,
                        }
                    )
                return {
                    "schemaVersion": "kis-domestic-h0stcnt0-durable-window-archive/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "armId": arm_id,
                    "sourceGeneration": str(arm["source_generation"]),
                    "socketIdentityHash": str(arm["socket_identity_hash"]),
                    "firstSourceSequence": str(first_sequence),
                    "lastSourceSequence": str(last_sequence),
                    "sourceEventCount": len(window_events),
                    "captureHeadHash": str(arm["raw_head_hash"]),
                    "authorityKeyIdHash": self.server_authority_key_id_hash,
                    "upstreamExchangeSequenceAvailable": False,
                    "upstreamPacketCompletenessAttested": False,
                    "acceptedIngressContinuityOnly": True,
                    "marketSourceIntegrationComplete": bool(market_links),
                    "marketSourceIngressLinkCount": len(market_links),
                    "marketSourceIngressLinkHeadHash": market_head,
                    "marketSourceIngressLinks": market_links,
                    "frames": frames,
                    "events": window_events,
                    "nextOpenEvent": next_open_event,
                    "recomputedBars": recomputed_bars,
                }
            finally:
                conn.close()

    def seal_observation(
        self,
        *,
        arm_id: str,
        owner_token_hash: str,
        observation_record: Mapping[str, Any],
        observation_signature: str,
        created_at: str,
    ) -> str:
        record = deepcopy(dict(observation_record))
        observation_id = record.get("observationId")
        if type(observation_id) is not str or not observation_id.startswith(
            "kis-source-observation-"
        ):
            raise KisDomesticFunctionalSourceBlocked("observation-id-invalid")
        self._verify_signature(
            "SOURCE_OBSERVATION",
            record,
            observation_signature,
            "source-observation",
        )
        _parse_utc(created_at, "observation.createdAt")
        record_hash = _hash(record)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                if arm is not None:
                    self._verify_transition_chain(conn, arm)
                if (
                    arm is None
                    or str(arm["state"]) != "ARMED_WAIT_PUBLIC"
                    or not hmac.compare_digest(
                        str(arm["owner_token_hash"]), owner_token_hash
                    )
                    or record.get("armId") != arm_id
                    or record.get("sourceGeneration")
                    != str(arm["source_generation"])
                    or record.get("socketIdentityHash")
                    != str(arm["socket_identity_hash"])
                    or record.get("captureHeadHash") != str(arm["raw_head_hash"])
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "observation-arm-lineage-invalid"
                    )
                conn.execute(
                    """INSERT INTO kis_public_source_observation
                       (observation_id, arm_id, state, observation_record_json,
                        observation_record_hash, observation_signature,
                        created_at, updated_at, revision)
                       VALUES (?, ?, 'NATURAL_BUY_OBSERVED', ?, ?, ?, ?, ?, 0)""",
                    (
                        observation_id,
                        arm_id,
                        _canonical(record).decode("utf-8"),
                        record_hash,
                        observation_signature,
                        created_at,
                        created_at,
                    ),
                )
                transition_head = self._append_transition(
                    conn,
                    arm=arm,
                    transition_kind="NATURAL_BUY_OBSERVATION_SEALED",
                    to_state="NATURAL_BUY_OBSERVED",
                    occurred_at=created_at,
                    reason="NATURAL_BUY_OBSERVATION_SEALED",
                    anchor_hash=record_hash,
                )
                changed = conn.execute(
                    """UPDATE kis_public_source_arm
                       SET state='NATURAL_BUY_OBSERVED', observation_id=?,
                           observation_hash=?, transition_count=transition_count+1,
                           transition_head_hash=?, revision=revision+1
                       WHERE arm_id=? AND state='ARMED_WAIT_PUBLIC'
                         AND owner_token_hash=? AND revision=?""",
                    (
                        observation_id,
                        record_hash,
                        transition_head,
                        arm_id,
                        owner_token_hash,
                        int(arm["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "observation-arm-cas-failed"
                    )
                conn.commit()
                return record_hash
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def seal_trigger(
        self,
        *,
        arm_id: str,
        owner_token_hash: str,
        observation_id: str,
        evaluation_id: str,
        trigger_record: Mapping[str, Any],
        trigger_signature: str,
        updated_at: str,
    ) -> str:
        trigger = deepcopy(dict(trigger_record))
        self._verify_signature(
            "NEXT_OPEN",
            trigger,
            trigger_signature,
            "next-open-trigger",
        )
        _parse_utc(updated_at, "trigger.updatedAt")
        trigger_hash = _hash(trigger)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                observation = conn.execute(
                    "SELECT * FROM kis_public_source_observation WHERE observation_id=?",
                    (observation_id,),
                ).fetchone()
                if arm is not None:
                    self._verify_transition_chain(conn, arm)
                if (
                    arm is None
                    or observation is None
                    or str(arm["state"]) != "NATURAL_BUY_OBSERVED"
                    or str(observation["state"]) != "NATURAL_BUY_OBSERVED"
                    or str(observation["arm_id"]) != arm_id
                    or not hmac.compare_digest(
                        str(arm["owner_token_hash"]), owner_token_hash
                    )
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "trigger-observation-state-invalid"
                    )
                transition_head = self._append_transition(
                    conn,
                    arm=arm,
                    transition_kind="NEXT_OPEN_TRIGGER_SEALED",
                    to_state="NEXT_OPEN_TRIGGER_SEALED",
                    occurred_at=updated_at,
                    reason="NEXT_OPEN_TRIGGER_SEALED",
                    anchor_hash=trigger_hash,
                )
                conn.execute(
                    """UPDATE kis_public_source_observation
                       SET state='NEXT_OPEN_TRIGGER_SEALED', trigger_record_json=?,
                           trigger_record_hash=?, trigger_signature=?, evaluation_id=?,
                           updated_at=?, revision=revision+1
                       WHERE observation_id=? AND state='NATURAL_BUY_OBSERVED'
                         AND revision=?""",
                    (
                        _canonical(trigger).decode("utf-8"),
                        trigger_hash,
                        trigger_signature,
                        evaluation_id,
                        updated_at,
                        observation_id,
                        int(observation["revision"]),
                    ),
                )
                changed = conn.execute(
                    """UPDATE kis_public_source_arm
                       SET state='NEXT_OPEN_TRIGGER_SEALED',
                           transition_count=transition_count+1,
                           transition_head_hash=?, revision=revision+1
                       WHERE arm_id=? AND state='NATURAL_BUY_OBSERVED'
                         AND owner_token_hash=? AND revision=?""",
                    (
                        transition_head, arm_id, owner_token_hash,
                        int(arm["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "trigger-arm-cas-failed"
                    )
                conn.commit()
                return trigger_hash
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    def terminalize(
        self,
        *,
        arm_id: str,
        owner_token_hash: str,
        reason: str,
        terminal_at: str,
    ) -> dict[str, Any]:
        if type(reason) is not str or not re.fullmatch(r"[A-Z0-9_]{3,96}", reason):
            raise KisDomesticFunctionalSourceBlocked("terminal-reason-invalid")
        _parse_utc(terminal_at, "terminalAt")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                if row is None:
                    raise KisDomesticFunctionalSourceBlocked("public-arm-missing")
                self._verify_transition_chain(conn, row)
                state = str(row["state"])
                if state.startswith("TERMINATED_"):
                    conn.commit()
                    return self._snapshot_row(row)
                if (
                    state not in _ACTIVE_ARM_STATES
                    or not hmac.compare_digest(
                        str(row["owner_token_hash"]), owner_token_hash
                    )
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "public-arm-owner-or-state-invalid"
                    )
                terminal_anchor = _hash(
                    {
                        "reason": reason,
                        "terminalAt": terminal_at,
                        "rawHeadHash": str(row["raw_head_hash"]),
                        "observationHash": str(row["observation_hash"]),
                    }
                )
                transition_head = self._append_transition(
                    conn,
                    arm=row,
                    transition_kind="FAIL_CLOSED_TERMINAL",
                    to_state="TERMINATED_FAIL_CLOSED",
                    occurred_at=terminal_at,
                    reason=reason,
                    anchor_hash=terminal_anchor,
                )
                changed = conn.execute(
                    """UPDATE kis_public_source_arm
                       SET state='TERMINATED_FAIL_CLOSED', terminal_reason=?,
                           terminal_at=?, transition_count=transition_count+1,
                           transition_head_hash=?, revision=revision+1
                       WHERE arm_id=? AND owner_token_hash=? AND revision=?""",
                    (
                        reason,
                        terminal_at,
                        transition_head,
                        arm_id,
                        owner_token_hash,
                        int(row["revision"]),
                    ),
                )
                if changed.rowcount != 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "public-arm-terminal-cas-failed"
                    )
                conn.commit()
                final_row = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                return self._snapshot_row(final_row)
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def _snapshot_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "armId": str(row["arm_id"]),
            "state": str(row["state"]),
            "sourceGeneration": str(row["source_generation"]),
            "socketIdentityHash": str(row["socket_identity_hash"]),
            "lastSourceSequence": str(row["last_sequence"]),
            "rawEventCount": int(row["raw_event_count"]),
            "rawFrameCount": int(row["raw_frame_count"]),
            "rawHeadHash": str(row["raw_head_hash"]),
            "observationId": str(row["observation_id"]),
            "terminalReason": str(row["terminal_reason"]),
            "terminalAt": str(row["terminal_at"]),
            "transitionCount": int(row["transition_count"]),
            "transitionHeadHash": str(row["transition_head_hash"]),
            "revision": int(row["revision"]),
        }

    def snapshot(self, arm_id: str) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (arm_id,),
                ).fetchone()
                if row is None:
                    raise KisDomesticFunctionalSourceBlocked("public-arm-missing")
                self._verify_transition_chain(conn, row)
                arm_record = self._decode_mapping(
                    row["arm_record_json"], "durable-public-arm"
                )
                self._assert_hash(
                    row["arm_record_hash"], arm_record, "durable-public-arm"
                )
                self._verify_signature(
                    "PUBLIC_ARM",
                    arm_record,
                    row["arm_signature"],
                    "durable-public-arm",
                )
                return self._snapshot_row(row)
            finally:
                conn.close()

    def read_observation(self, observation_id: str) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM kis_public_source_observation WHERE observation_id=?",
                    (observation_id,),
                ).fetchone()
                if row is None:
                    raise KisDomesticFunctionalSourceBlocked("observation-not-found")
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (row["arm_id"],),
                ).fetchone()
                if arm is None:
                    raise KisDomesticFunctionalSourceBlocked(
                        "observation-arm-missing"
                    )
                self._verify_transition_chain(conn, arm)
                record = self._decode_mapping(
                    row["observation_record_json"], "durable-observation"
                )
                self._assert_hash(
                    row["observation_record_hash"], record, "durable-observation"
                )
                self._verify_signature(
                    "SOURCE_OBSERVATION",
                    record,
                    row["observation_signature"],
                    "durable-observation",
                )
                return {
                    "state": str(row["state"]),
                    "record": record,
                    "recordHash": str(row["observation_record_hash"]),
                    "triggerHash": str(row["trigger_record_hash"]),
                    "evaluationId": str(row["evaluation_id"]),
                    "revision": int(row["revision"]),
                }
            finally:
                conn.close()


def exact_kis_domestic_functional_subscription() -> FeedSubscription:
    return FeedSubscription(_INSTRUMENT_ID, _SYMBOL, _TIMEFRAME)


class KisDomesticFunctionalMarketSourceDurableWriter:
    """Verify-only bridge from signed market ingress into the source journal."""

    _RAW_KEYS = {
        "schemaVersion", "route", "pdno", "trId", "sessionId",
        "accountFingerprint", "ownerEpoch", "ownerEpochId", "ownerEpochHash",
        "processGeneration", "sourceGeneration", "socketIdentityHash",
        "ingressOrdinal", "receivedAt", "rawFrame", "rawFrameHash",
        "recordCount", "previousIngressHeadHash", "authorityKeyIdHash",
        "upstreamExchangeSequenceAvailable", "upstreamPacketCompletenessAttested",
        "productionAvailable", "authorityPurpose", "recordHash", "signature",
    }

    def __init__(
        self,
        *,
        journal: DurableKisDomesticPublicArmJournal,
        arm_id: str,
        owner_token_hash: str,
        market_record_verifier: Callable[[Mapping[str, Any]], bool],
        trusted_clock: Callable[[], datetime],
    ) -> None:
        if type(journal) is not DurableKisDomesticPublicArmJournal:
            raise KisDomesticFunctionalSourceBlocked(
                "exact-public-source-journal-required"
            )
        if type(arm_id) is not str or not _ARM_ID.fullmatch(arm_id):
            raise KisDomesticFunctionalSourceBlocked("market-writer-arm-id-invalid")
        if type(owner_token_hash) is not str or not _SHA256.fullmatch(
            owner_token_hash
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-owner-token-hash-invalid"
            )
        if not callable(market_record_verifier) or not callable(trusted_clock):
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-verify-clock-required"
            )
        self.journal = journal
        self.arm_id = arm_id
        self.owner_token_hash = owner_token_hash
        self.market_record_verifier = market_record_verifier
        self.trusted_clock = trusted_clock

    @staticmethod
    def _market_record_hash(value: Mapping[str, Any]) -> str:
        unsigned = dict(value)
        unsigned.pop("signature", None)
        unsigned.pop("recordHash", None)
        return _hash(unsigned)

    def _verify_market_record(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or set(raw) != self._RAW_KEYS:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-record-not-exact"
            )
        value = deepcopy(dict(raw))
        if (
            value.get("schemaVersion")
            != "kis-domestic-functional-market-source-raw-frame/v1"
            or value.get("route") != ROUTE
            or value.get("pdno") != PDNO
            or value.get("trId") != _TR_ID
            or value.get("authorityPurpose") != "MARKET_SOURCE_RECORD_VERIFY"
            or value.get("upstreamExchangeSequenceAvailable") is not False
            or value.get("upstreamPacketCompletenessAttested") is not False
            or value.get("productionAvailable") is not False
            or not _signature_text(value.get("signature"))
            or type(value.get("recordHash")) is not str
            or not _SHA256.fullmatch(value["recordHash"])
            or not hmac.compare_digest(
                value["recordHash"], self._market_record_hash(value)
            )
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-record-binding-invalid"
            )
        try:
            valid = self.market_record_verifier(deepcopy(value))
        except BaseException:
            valid = False
        if valid is not True:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-record-signature-unverified"
            )
        records = _independent_parse_authenticated_frame(
            {
                "route": value["route"],
                "pdno": value["pdno"],
                "trId": value["trId"],
                "rawFrame": value["rawFrame"],
                "rawFrameHash": value["rawFrameHash"],
                "recordCount": value["recordCount"],
                "recordFields": [
                    list(row)
                    for row in self._split_records(value["rawFrame"])
                ],
            }
        )
        if len(records) != value["recordCount"]:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-record-count-invalid"
            )
        return value

    @staticmethod
    def _split_records(raw: str) -> list[tuple[str, ...]]:
        if type(raw) is not str:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-frame-invalid"
            )
        parts = raw.split("|", 3)
        if len(parts) != 4:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-frame-invalid"
            )
        try:
            count = int(parts[2])
        except (TypeError, ValueError):
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-frame-count-invalid"
            ) from None
        fields = parts[3].split("^")
        if not 1 <= count <= 512 or len(fields) != count * KIS_DOMESTIC_TRADE_FIELD_COUNT:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-raw-frame-width-invalid"
            )
        return [
            tuple(
                fields[index * KIS_DOMESTIC_TRADE_FIELD_COUNT :
                       (index + 1) * KIS_DOMESTIC_TRADE_FIELD_COUNT]
            )
            for index in range(count)
        ]

    def __call__(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        value = self._verify_market_record(raw)
        now = _utc(self.trusted_clock(), "marketWriter.trustedNow")
        try:
            received = datetime.fromisoformat(
                str(value["receivedAt"]).replace("Z", "+00:00")
            )
        except (TypeError, ValueError):
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-received-at-invalid"
            ) from None
        received = _utc(received, "marketWriter.receivedAt")
        source_received_at = _utc_text(received, "marketWriter.receivedAt")
        if not received <= now or (now - received).total_seconds() > 2:
            raise KisDomesticFunctionalSourceBlocked(
                "market-writer-trusted-time-lineage-invalid"
            )
        with self.journal._lock:
            conn = self.journal._connect()
            try:
                arm = conn.execute(
                    "SELECT * FROM kis_public_source_arm WHERE arm_id=?",
                    (self.arm_id,),
                ).fetchone()
                if arm is not None:
                    self.journal._verify_transition_chain(conn, arm)
                if (
                    arm is None
                    or str(arm["state"]) != "ARMED_WAIT_PUBLIC"
                    or not hmac.compare_digest(
                        str(arm["owner_token_hash"]), self.owner_token_hash
                    )
                    or value["sourceGeneration"] != str(arm["source_generation"])
                    or value["socketIdentityHash"]
                    != str(arm["socket_identity_hash"])
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "market-writer-arm-generation-owner-mismatch"
                    )
                arm_record = self.journal._decode_mapping(
                    arm["arm_record_json"], "market-writer-arm-record"
                )
                exact_arm_bindings = {
                    "marketSourceSessionId": value["sessionId"],
                    "marketSourceAccountFingerprint": value["accountFingerprint"],
                    "marketSourceOwnerEpoch": value["ownerEpoch"],
                    "marketSourceOwnerEpochId": value["ownerEpochId"],
                    "marketSourceOwnerEpochHash": value["ownerEpochHash"],
                    "marketSourceProcessGeneration": value["processGeneration"],
                    "marketSourceAuthorityKeyIdHash": value[
                        "authorityKeyIdHash"
                    ],
                }
                if any(
                    type(arm_record.get(key)) is not type(wanted)
                    or arm_record.get(key) != wanted
                    for key, wanted in exact_arm_bindings.items()
                ):
                    raise KisDomesticFunctionalSourceBlocked(
                        "market-writer-signed-arm-binding-mismatch"
                    )
                frame_index = int(arm["raw_frame_count"]) + 1
                first_sequence = int(arm["last_sequence"]) + 1
                records = self._split_records(value["rawFrame"])
                last_sequence = first_sequence + len(records) - 1
                frame_body = {
                    "schemaVersion": "kis-domestic-h0stcnt0-raw-frame-envelope/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "trId": _TR_ID,
                    "armId": self.arm_id,
                    "sourceGeneration": value["sourceGeneration"],
                    "socketIdentityHash": value["socketIdentityHash"],
                    "frameIndex": frame_index,
                    "firstSourceSequence": str(first_sequence),
                    "lastSourceSequence": str(last_sequence),
                    "recordCount": len(records),
                    "receivedAt": source_received_at,
                    "rawFrame": value["rawFrame"],
                    "rawFrameHash": value["rawFrameHash"],
                    "recordFields": [list(row) for row in records],
                    "previousFrameHeadHash": str(arm["raw_head_hash"]),
                    "marketSourceLinkSchema": MARKET_SOURCE_LINK_SCHEMA,
                    "marketSourceSessionId": value["sessionId"],
                    "marketSourceAccountFingerprint": value[
                        "accountFingerprint"
                    ],
                    "marketSourceOwnerEpoch": value["ownerEpoch"],
                    "marketSourceOwnerEpochId": value["ownerEpochId"],
                    "marketSourceOwnerEpochHash": value["ownerEpochHash"],
                    "marketSourceProcessGeneration": value[
                        "processGeneration"
                    ],
                    "marketSourceIngressOrdinal": value["ingressOrdinal"],
                    "marketSourceRawRecordHash": value["recordHash"],
                    "marketSourceRawFrameHash": value["rawFrameHash"],
                    "marketSourcePreviousIngressHeadHash": value[
                        "previousIngressHeadHash"
                    ],
                    "marketSourceAuthorityKeyIdHash": value[
                        "authorityKeyIdHash"
                    ],
                    "marketSourceAuthorityPurpose": (
                        "MARKET_SOURCE_RECORD_VERIFY"
                    ),
                    "marketSourceArmTransitionHeadHash": str(
                        arm["transition_head_hash"]
                    ),
                }
                frame_signature = self.journal._capture_signer(
                    "RAW_H0STCNT0_FRAME", deepcopy(frame_body)
                )
                self.journal._verify_signature(
                    "RAW_H0STCNT0_FRAME", frame_body, frame_signature,
                    "market-writer-source-frame",
                )
                frame_hash = _hash(frame_body)
                events: list[dict[str, Any]] = []
                for index, record in enumerate(records):
                    sequence = first_sequence + index
                    temporal = _independent_record_truth(
                        record, received_at=received
                    )
                    raw_event_hash = _hash(
                        _raw_event_hash_body(
                            source_generation=value["sourceGeneration"],
                            socket_identity_hash=value["socketIdentityHash"],
                            source_sequence=str(sequence),
                            record_index=index,
                            raw_frame_hash=value["rawFrameHash"],
                            record_fields=list(record),
                            received_at=source_received_at,
                        )
                    )
                    events.append(
                        {
                            "schemaVersion": (
                                "kis-domestic-h0stcnt0-raw-event-envelope/v1"
                            ),
                            "route": ROUTE,
                            "pdno": PDNO,
                            "armId": self.arm_id,
                            "sourceGeneration": value["sourceGeneration"],
                            "socketIdentityHash": value["socketIdentityHash"],
                            "sourceSequence": str(sequence),
                            "feedSourceSequence": ":".join(
                                (PDNO, record[33], record[1], str(index))
                            ),
                            "frameEnvelopeHash": frame_hash,
                            "rawFrameHash": value["rawFrameHash"],
                            "rawEventHash": raw_event_hash,
                            "recordIndex": index,
                            "tradeAt": temporal["tradeAt"],
                            "receivedAt": temporal["receivedAt"],
                            "bucketOpenAt": temporal["bucketOpenAt"],
                            "bucketCloseAt": temporal["bucketCloseAt"],
                            "recordFields": list(record),
                        }
                    )
                transition_head = str(arm["transition_head_hash"])
            finally:
                conn.close()
            committed = self.journal.append_frame(
                arm_id=self.arm_id,
                owner_token_hash=self.owner_token_hash,
                frame_record=frame_body,
                frame_signature=frame_signature,
                event_records=events,
            )
        durable_head = _hash(
            {
                "schemaVersion": "kis-domestic-functional-market-source-head/v2",
                "sourceGeneration": value["sourceGeneration"],
                "ingressOrdinal": value["ingressOrdinal"],
                "previousIngressHeadHash": value["previousIngressHeadHash"],
                "rawRecordHash": value["recordHash"],
                "sourceArmId": self.arm_id,
                "sourceFrameIndex": committed["frameIndex"],
                "sourceFrameEnvelopeHash": committed["frameEnvelopeHash"],
                "sourceFrameHeadHash": committed["frameHeadHash"],
                "sourceArmTransitionHeadHash": transition_head,
            }
        )
        body = {
            "schemaVersion": MARKET_SOURCE_ACK_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": value["sessionId"],
            "ownerEpochHash": value["ownerEpochHash"],
            "sourceGeneration": value["sourceGeneration"],
            "ingressOrdinal": value["ingressOrdinal"],
            "rawFrameHash": value["rawFrameHash"],
            "rawRecordHash": value["recordHash"],
            "previousIngressHeadHash": value["previousIngressHeadHash"],
            "durableRecordHash": committed["frameEnvelopeHash"],
            "durableHeadHash": durable_head,
            "ackedAt": _utc_text(now, "marketWriter.ackedAt"),
            "authorityKeyIdHash": self.journal.server_authority_key_id_hash,
            "authorityPurpose": "SOURCE_RECORD_VERIFY",
            "sourceArmId": self.arm_id,
            "sourceFrameIndex": committed["frameIndex"],
            "firstSourceSequence": first_sequence,
            "lastSourceSequence": last_sequence,
            "sourceFrameEnvelopeHash": committed["frameEnvelopeHash"],
            "sourceFrameHeadHash": committed["frameHeadHash"],
            "sourceArmTransitionHeadHash": transition_head,
            "productionAvailable": False,
        }
        ack_hash = _hash(body)
        signed = {**body, "ackHash": ack_hash}
        signature = self.journal._capture_signer(
            "MARKET_SOURCE_DURABLE_ACK", deepcopy(signed)
        )
        self.journal._verify_signature(
            "MARKET_SOURCE_DURABLE_ACK", signed, signature,
            "market-source-durable-ack",
        )
        return {**signed, "signature": signature}

    def verify_ack(self, candidate: Mapping[str, Any]) -> bool:
        try:
            value = deepcopy(dict(candidate))
            signature = value.pop("signature")
            ack_hash = value.pop("ackHash")
            return bool(
                set(candidate)
                == {
                    "schemaVersion", "route", "pdno", "sessionId",
                    "ownerEpochHash", "sourceGeneration", "ingressOrdinal",
                    "rawFrameHash", "rawRecordHash", "previousIngressHeadHash",
                    "durableRecordHash", "durableHeadHash", "ackedAt",
                    "authorityKeyIdHash", "authorityPurpose", "sourceArmId",
                    "sourceFrameIndex", "firstSourceSequence",
                    "lastSourceSequence", "sourceFrameEnvelopeHash",
                    "sourceFrameHeadHash", "sourceArmTransitionHeadHash",
                    "productionAvailable", "ackHash", "signature",
                }
                and hmac.compare_digest(ack_hash, _hash(value))
                and self.journal._capture_verifier(
                    "MARKET_SOURCE_DURABLE_ACK",
                    {**value, "ackHash": ack_hash},
                    signature,
                ) is True
            )
        except BaseException:
            return False


class KisDomesticFunctionalPublicSource:
    """Mock-only public H0STCNT0 provenance adapter for the ARMED phase.

    The adapter deliberately has no connect/token/account/order method.  It
    reuses the existing KIS feed's tested H0STCNT0 reducer, but independently
    archives every public record and cross-checks the feed's OHLC/open event.
    A future production pump may hand it already-received public frames only
    after this module's production/network flags are reviewed and enabled.
    """

    def __init__(
        self,
        *,
        feed: KisWebSocketClosedBarFeed,
        capture_signer: CaptureSigner,
        journal: DurableKisDomesticPublicArmJournal,
        generation_factory: Callable[[], str] | None = None,
        socket_identity_factory: Callable[[], str] | None = None,
        owner_token_factory: Callable[[], bytes] | None = None,
        allow_mock_source: bool = False,
    ) -> None:
        if type(feed) is not KisWebSocketClosedBarFeed:
            raise KisDomesticFunctionalSourceBlocked("exact-kis-feed-required")
        if allow_mock_source is not True:
            raise KisDomesticFunctionalSourceBlocked("mock-source-flag-required")
        if not callable(capture_signer):
            raise KisDomesticFunctionalSourceBlocked("capture-signer-required")
        if type(journal) is not DurableKisDomesticPublicArmJournal:
            raise KisDomesticFunctionalSourceBlocked("exact-public-arm-journal-required")
        if tuple(feed.subscriptions) != (exact_kis_domestic_functional_subscription(),):
            raise KisDomesticFunctionalSourceBlocked("subscription-not-exact")
        if (
            feed.demo
            or feed.app_key
            or feed.app_secret
            or feed.private_tr_key
            or feed.private_execution_sink is not None
            or feed.kis_account_id
            or feed.websocket_owner_id
            or feed.websocket_app_key_id
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "armed-source-must-have-no-private-account-or-app-authority"
            )
        if feed.socket is not None:
            raise KisDomesticFunctionalSourceBlocked("network-socket-forbidden")
        self._feed = feed
        self._signer = capture_signer
        self._journal = journal
        self._generation_factory = generation_factory or (
            lambda: f"kis-ws-generation-{uuid.uuid4().hex}"
        )
        self._socket_identity_factory = socket_identity_factory or (
            lambda: f"kis-ws-socket-{secrets.token_hex(24)}"
        )
        token_factory = owner_token_factory or (lambda: secrets.token_bytes(32))
        owner_token = token_factory()
        if type(owner_token) is not bytes or len(owner_token) < 32:
            raise KisDomesticFunctionalSourceBlocked("process-owner-token-invalid")
        self._owner_token_hash = hashlib.sha256(owner_token).hexdigest()
        self._generation = ""
        self._socket_identity_hash = ""
        self._arm_id = ""
        self._terminated_predecessor_arm_ids: tuple[str, ...] = ()
        self._connected_at: datetime | None = None
        self._last_trade_at: datetime | None = None
        self._sequences: set[str] = set()
        self._sequence_counter = 0
        self._accumulators: dict[str, _BarAccumulator] = {}
        self._raw_events: dict[str, _RawTrade] = {}
        self._raw_events_by_feed_sequence: dict[tuple[str, str], _RawTrade] = {}
        self._closed_proofs: deque[dict[str, Any]] = deque(maxlen=11)
        self._observations: dict[str, dict[str, Any]] = {}
        self._observation_attestations: dict[str, dict[str, str]] = {}
        self._sealed_triggers: dict[str, dict[str, Any]] = {}
        self._ingested_frames = 0

    def __repr__(self) -> str:
        return (
            "KisDomesticFunctionalPublicSource("
            "route='KIS_KR_LIVE_CONTINUOUS',pdno='010140',"
            "authority='public-market-data-only',network=False)"
        )

    def begin_mock_generation(self, *, connected_at: datetime) -> str:
        connected = _utc(connected_at, "connectedAt")
        generation = self._generation_factory()
        if type(generation) is not str or not _GENERATION.fullmatch(generation):
            raise KisDomesticFunctionalSourceBlocked("source-generation-invalid")
        socket_identity = self._socket_identity_factory()
        if type(socket_identity) is not str or not _SOCKET_IDENTITY.fullmatch(
            socket_identity
        ):
            raise KisDomesticFunctionalSourceBlocked("socket-identity-invalid")
        socket_identity_hash = hashlib.sha256(
            socket_identity.encode("utf-8")
        ).hexdigest()
        arm_id = f"kis-public-source-arm-{uuid.uuid4().hex}"
        arm_record = {
            "schemaVersion": "kis-domestic-functional-public-source-arm/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "trId": _TR_ID,
            "source": _SOURCE,
            "sourceProvider": _SOURCE_PROVIDER,
            "armId": arm_id,
            "state": "ARMED_WAIT_PUBLIC",
            "sourceGeneration": generation,
            "socketIdentityHash": socket_identity_hash,
            "serverAuthorityKeyIdHash": (
                self._journal.server_authority_key_id_hash
            ),
            "connectedAt": _utc_text(connected, "connectedAt"),
            "createdAt": _utc_text(connected, "createdAt"),
            "publicMarketDataOnly": True,
            "accountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "networkAvailable": False,
            "productionAvailable": False,
        }
        arm_signature = self._sign("PUBLIC_ARM", arm_record)
        durable_arm_id, terminated = self._journal.begin_arm(
            arm_record=arm_record,
            arm_signature=arm_signature,
            owner_token_hash=self._owner_token_hash,
        )
        if durable_arm_id != arm_id:
            raise KisDomesticFunctionalSourceBlocked("public-arm-identity-mismatch")
        self._generation = generation
        self._socket_identity_hash = socket_identity_hash
        self._arm_id = arm_id
        self._terminated_predecessor_arm_ids = terminated
        self._connected_at = connected
        self._last_trade_at = None
        self._sequences.clear()
        self._sequence_counter = 0
        self._accumulators.clear()
        self._raw_events.clear()
        self._raw_events_by_feed_sequence.clear()
        self._closed_proofs.clear()
        self._observations.clear()
        self._observation_attestations.clear()
        self._sealed_triggers.clear()
        self._ingested_frames = 0
        self._feed._reset_websocket_buckets()
        self._feed._connected_at = connected.timestamp()
        self._feed.transport = "kis-websocket"
        return generation

    def status(self) -> dict[str, Any]:
        return {
            "schemaVersion": "kis-domestic-functional-public-source-status/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "trId": _TR_ID,
            "source": _SOURCE,
            "sourceProvider": _SOURCE_PROVIDER,
            "sourceGeneration": self._generation,
            "upstreamExchangeSequenceAvailable": False,
            "upstreamPacketCompletenessAttested": False,
            "acceptedIngressContinuityOnly": True,
            "socketIdentityHash": self._socket_identity_hash,
            "publicArmId": self._arm_id,
            "durablePublicArmState": (
                self._journal.snapshot(self._arm_id)["state"]
                if self._arm_id
                else "NOT_ARMED"
            ),
            "terminatedPredecessorArmIds": list(
                self._terminated_predecessor_arm_ids
            ),
            "armedPublicDataOnly": True,
            "privateStreamConfigured": False,
            "accountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "cancelAuthorityAvailable": False,
            "networkAvailable": KIS_DOMESTIC_FUNCTIONAL_SOURCE_NETWORK_AVAILABLE,
            "productionAvailable": (
                KIS_DOMESTIC_FUNCTIONAL_SOURCE_PRODUCTION_AVAILABLE
            ),
            "mutationAvailable": KIS_DOMESTIC_FUNCTIONAL_SOURCE_MUTATION_AVAILABLE,
            "ingestedFrameCount": self._ingested_frames,
            "closedWindowSize": len(self._closed_proofs),
            "observationCount": len(self._observations),
        }

    def durable_arm_snapshot(self) -> dict[str, Any]:
        if not self._arm_id:
            raise KisDomesticFunctionalSourceBlocked("source-generation-not-started")
        return self._journal.snapshot(self._arm_id)

    @staticmethod
    def _bucket(trade_at: datetime) -> tuple[datetime, datetime]:
        local_date = trade_at.astimezone(KST).date()
        try:
            opened, closed = session_bounds_utc("XKRX", local_date)
        except ValueError:
            raise KisDomesticFunctionalSourceBlocked(
                "trade-not-on-xkrx-session"
            ) from None
        if not opened <= trade_at < closed:
            raise KisDomesticFunctionalSourceBlocked("trade-outside-xkrx-session")
        seconds = BAR_INTERVAL_MINUTES * 60
        position = (trade_at - opened).total_seconds()
        bucket_open = opened + timedelta(seconds=int(position // seconds) * seconds)
        bucket_close = min(bucket_open + timedelta(seconds=seconds), closed)
        if bucket_close - bucket_open != timedelta(minutes=BAR_INTERVAL_MINUTES):
            raise KisDomesticFunctionalSourceBlocked("partial-session-bar-forbidden")
        return bucket_open, bucket_close

    @staticmethod
    def _armed_deadline(value: datetime) -> datetime:
        local = _utc(value, "armedDeadline.reference").astimezone(KST)
        return local.replace(hour=13, minute=15, second=0, microsecond=0).astimezone(
            timezone.utc
        )

    def _require_ingest_owner(self) -> None:
        if not self._arm_id:
            raise KisDomesticFunctionalSourceBlocked("source-generation-not-started")
        snapshot = self._journal.snapshot(self._arm_id)
        if snapshot["state"] != "ARMED_WAIT_PUBLIC":
            raise KisDomesticFunctionalSourceBlocked(
                "public-arm-not-owned-or-no-longer-armed"
            )

    def _terminalize_after_ingress_failure(
        self,
        *,
        reason: str,
        terminal_at: datetime,
    ) -> None:
        if not self._arm_id:
            return
        try:
            snapshot = self._journal.snapshot(self._arm_id)
            if snapshot["state"] in _ACTIVE_ARM_STATES:
                self._journal.terminalize(
                    arm_id=self._arm_id,
                    owner_token_hash=self._owner_token_hash,
                    reason=reason,
                    terminal_at=_utc_text(terminal_at, "terminalAt"),
                )
        except KisDomesticFunctionalSourceBlocked:
            # Preserve the first fail-closed exception.  A later caller still
            # re-reads the durable state before any further ingestion.
            return

    def _parse_frame(self, raw: str, received_at: datetime) -> list[_RawTrade]:
        if type(raw) is not str or not raw.startswith(f"0|{_TR_ID}|"):
            raise KisDomesticFunctionalSourceBlocked("h0stcnt0-frame-required")
        parts = raw.split("|", 3)
        if len(parts) != 4:
            raise KisDomesticFunctionalSourceBlocked("h0stcnt0-frame-malformed")
        try:
            count = int(parts[2])
        except (TypeError, ValueError):
            raise KisDomesticFunctionalSourceBlocked("h0stcnt0-count-invalid") from None
        if not 1 <= count <= 512:
            raise KisDomesticFunctionalSourceBlocked("h0stcnt0-count-invalid")
        fields = parts[3].split("^")
        if len(fields) != count * KIS_DOMESTIC_TRADE_FIELD_COUNT:
            raise KisDomesticFunctionalSourceBlocked("h0stcnt0-width-invalid")
        received = _utc(received_at, "receivedAt")
        if received > self._armed_deadline(received):
            raise KisDomesticFunctionalSourceBlocked(
                "h0stcnt0-observation-after-armed-deadline"
            )
        raw_frame_hash = _frame_hash(raw)
        parsed: list[_RawTrade] = []
        previous_trade_at = self._last_trade_at
        for index in range(count):
            record = tuple(
                fields[
                    index * KIS_DOMESTIC_TRADE_FIELD_COUNT :
                    (index + 1) * KIS_DOMESTIC_TRADE_FIELD_COUNT
                ]
            )
            if record[0] != PDNO:
                raise KisDomesticFunctionalSourceBlocked("h0stcnt0-pdno-mismatch")
            if not re.fullmatch(r"[0-9]{8}", record[33]) or not re.fullmatch(
                r"[0-9]{6}", record[1]
            ):
                raise KisDomesticFunctionalSourceBlocked("h0stcnt0-time-invalid")
            try:
                local = datetime.strptime(
                    record[33] + record[1], "%Y%m%d%H%M%S"
                ).replace(tzinfo=KST)
            except ValueError:
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-time-invalid"
                ) from None
            trade_at = local.astimezone(timezone.utc)
            if trade_at > self._armed_deadline(trade_at):
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-trade-after-armed-deadline"
                )
            lag = (received - trade_at).total_seconds()
            if lag < 0 or lag > 2:
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-observation-outside-two-seconds"
                )
            if self._connected_at is None or self._connected_at > trade_at:
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-generation-did-not-observe-bucket-open"
                )
            if previous_trade_at is not None and trade_at < previous_trade_at:
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-trade-time-regressed"
                )
            price = _decimal(record[2], "h0stcnt0.price")
            volume = _decimal(record[12], "h0stcnt0.volume", zero_allowed=True)
            bucket_open, bucket_close = self._bucket(trade_at)
            sequence = str(self._sequence_counter + index + 1)
            feed_sequence = ":".join(
                (PDNO, record[33], record[1], str(index))
            )
            if sequence in self._sequences:
                raise KisDomesticFunctionalSourceBlocked(
                    "h0stcnt0-source-sequence-duplicate"
                )
            event_body = _raw_event_hash_body(
                source_generation=self._generation,
                socket_identity_hash=self._socket_identity_hash,
                source_sequence=sequence,
                record_index=index,
                raw_frame_hash=raw_frame_hash,
                record_fields=list(record),
                received_at=_utc_text(received, "receivedAt"),
            )
            parsed.append(
                _RawTrade(
                    source_sequence=sequence,
                    feed_source_sequence=feed_sequence,
                    raw_frame_hash=raw_frame_hash,
                    raw_event_hash=_hash(event_body),
                    record_index=index,
                    trade_at=trade_at,
                    received_at=received,
                    price=price,
                    volume=volume,
                    bucket_open=bucket_open,
                    bucket_close=bucket_close,
                    fields=record,
                )
            )
            previous_trade_at = trade_at
        return parsed

    def ingest_h0stcnt0_frame(
        self,
        raw: str,
        *,
        received_at: datetime,
    ) -> dict[str, Any] | None:
        received = _utc(received_at, "receivedAt")
        if not self._generation or self._connected_at is None:
            raise KisDomesticFunctionalSourceBlocked("source-generation-not-started")
        try:
            self._require_ingest_owner()
            trades = self._parse_frame(raw, received)
            frame_index = self._ingested_frames + 1
            frame_body = {
                "schemaVersion": "kis-domestic-h0stcnt0-raw-frame-envelope/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "trId": _TR_ID,
                "armId": self._arm_id,
                "sourceGeneration": self._generation,
                "socketIdentityHash": self._socket_identity_hash,
                "frameIndex": frame_index,
                "firstSourceSequence": trades[0].source_sequence,
                "lastSourceSequence": trades[-1].source_sequence,
                "recordCount": len(trades),
                "receivedAt": _utc_text(received, "receivedAt"),
                "rawFrame": raw,
                "rawFrameHash": trades[0].raw_frame_hash,
                "recordFields": [list(trade.fields) for trade in trades],
                "previousFrameHeadHash": self._journal.snapshot(self._arm_id)[
                    "rawHeadHash"
                ],
            }
            frame_signature = self._sign("RAW_H0STCNT0_FRAME", frame_body)
            frame_envelope_hash = _hash(frame_body)
            event_records = [
                trade.durable_envelope(
                    arm_id=self._arm_id,
                    source_generation=self._generation,
                    socket_identity_hash=self._socket_identity_hash,
                    frame_envelope_hash=frame_envelope_hash,
                )
                for trade in trades
            ]
            committed = self._journal.append_frame(
                arm_id=self._arm_id,
                owner_token_hash=self._owner_token_hash,
                frame_record=frame_body,
                frame_signature=frame_signature,
                event_records=event_records,
            )
            if (
                committed["frameIndex"] != frame_index
                or committed["frameEnvelopeHash"] != frame_envelope_hash
                or committed["lastSourceSequence"] != trades[-1].source_sequence
            ):
                raise KisDomesticFunctionalSourceBlocked(
                    "durable-frame-commit-attestation-mismatch"
                )
            self._sequence_counter = int(trades[-1].source_sequence)
            for trade in trades:
                if int(trade.source_sequence) != len(self._sequences) + 1:
                    raise KisDomesticFunctionalSourceBlocked(
                        "h0stcnt0-source-sequence-gap-or-duplicate"
                    )
                self._sequences.add(trade.source_sequence)
                self._raw_events[trade.source_sequence] = trade
                feed_key = (trade.feed_source_sequence, trade.raw_frame_hash)
                if feed_key in self._raw_events_by_feed_sequence:
                    raise KisDomesticFunctionalSourceBlocked(
                        "h0stcnt0-feed-event-duplicate"
                    )
                self._raw_events_by_feed_sequence[feed_key] = trade
                self._last_trade_at = trade.trade_at
                key = _utc_text(trade.bucket_open, "bucketOpenAt")
                accumulator = self._accumulators.get(key)
                if accumulator is None:
                    accumulator = _BarAccumulator(
                        opened=trade.bucket_open,
                        closed=trade.bucket_close,
                    )
                    self._accumulators[key] = accumulator
                accumulator.add(trade)
            events: list[ClosedBar | OpenBoundary] = []
            self._feed._consume_socket_frame(
                raw,
                events,
                received_epoch=received.timestamp(),
            )
            self._ingested_frames += 1
            closed_events = [event for event in events if isinstance(event, ClosedBar)]
            boundary_events = [
                event for event in events if isinstance(event, OpenBoundary)
            ]
            for closed in closed_events:
                key = _utc_text(
                    _parse_runtime_utc(closed.start_time, "closedBar.startTime"),
                    "closedBar.startTime",
                )
                accumulator = self._accumulators.get(key)
                if accumulator is None:
                    raise KisDomesticFunctionalSourceBlocked(
                        "closed-bar-accumulator-missing"
                    )
                self._closed_proofs.append(accumulator.proof(closed))
            candidate: dict[str, Any] | None = None
            for boundary in boundary_events:
                produced = self._candidate_at_boundary(boundary)
                if produced is not None:
                    if candidate is not None:
                        raise KisDomesticFunctionalSourceBlocked(
                            "multiple-natural-boundaries-in-one-frame"
                        )
                    candidate = produced
            return deepcopy(candidate) if candidate is not None else None
        except KisDomesticFunctionalSourceBlocked:
            self._terminalize_after_ingress_failure(
                reason="PUBLIC_INGRESS_OR_REDUCER_REJECTED",
                terminal_at=received,
            )
            raise
        except BaseException as exc:
            self._terminalize_after_ingress_failure(
                reason="PUBLIC_INGRESS_OR_REDUCER_EXCEPTION",
                terminal_at=received,
            )
            raise KisDomesticFunctionalSourceBlocked(
                f"kis-feed-reducer-failed:{type(exc).__name__}"
            ) from None

    def _candidate_at_boundary(
        self,
        boundary: OpenBoundary,
    ) -> dict[str, Any] | None:
        if (
            boundary.instrument_id != _INSTRUMENT_ID
            or boundary.symbol != _SYMBOL
            or boundary.timeframe != _TIMEFRAME
            or boundary.source_provider != _SOURCE_PROVIDER
            or boundary.attestation != "KIS_WEBSOCKET"
            or boundary.recovery_only
            or not boundary.promotion_eligible
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "next-open-feed-attestation-invalid"
            )
        opened = _parse_runtime_utc(boundary.start_time, "boundary.startTime")
        observed = _parse_runtime_utc(
            boundary.observed_time, "boundary.observedTime"
        )
        if observed < opened or observed > opened + timedelta(seconds=2):
            raise KisDomesticFunctionalSourceBlocked(
                "next-open-observation-outside-two-seconds"
            )
        if opened > self._armed_deadline(opened) or observed > self._armed_deadline(
            observed
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "next-open-after-armed-deadline"
            )
        if len(self._closed_proofs) != 11:
            return None
        bars = list(self._closed_proofs)
        for previous, current in zip(bars, bars[1:]):
            if previous["closeAt"] != current["openAt"]:
                raise KisDomesticFunctionalSourceBlocked(
                    "finalized-window-not-contiguous"
                )
        if bars[-1]["closeAt"] != _utc_text(opened, "boundary.startTime"):
            return None
        prior = bars[:-1]
        average_range = sum(
            (
                _decimal(bar["high"], "bar.high")
                - _decimal(bar["low"], "bar.low")
                for bar in prior
            ),
            Decimal("0"),
        ) / Decimal("10")
        trigger = _decimal(prior[-1]["close"], "prior.close") + (
            average_range * Decimal("0.3")
        )
        if _decimal(bars[-1]["high"], "current.high") < trigger:
            return None
        event = self._raw_events_by_feed_sequence.get(
            (boundary.source_sequence, boundary.raw_hash)
        )
        if event is None or event.raw_frame_hash != boundary.raw_hash:
            raise KisDomesticFunctionalSourceBlocked(
                "next-open-raw-event-lineage-mismatch"
            )
        first_sequence = bars[0]["sourceSequenceStart"]
        last_sequence = bars[-1]["sourceSequenceEnd"]
        if int(event.source_sequence) != int(last_sequence) + 1:
            raise KisDomesticFunctionalSourceBlocked(
                "next-open-source-sequence-not-immediate"
            )
        source_event_count = sum(int(bar["eventCount"]) for bar in bars)
        source_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceProvider": _SOURCE_PROVIDER,
            "sourceGeneration": self._generation,
            "firstSourceSequence": first_sequence,
            "lastSourceSequence": last_sequence,
            "sourceEventCount": source_event_count,
            "barRawEventChainHashes": [
                bar["rawEventChainHash"] for bar in bars
            ],
        }
        window_body = {
            "schemaVersion": "kis-domestic-official-5m-window/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "source": _SOURCE,
            "sourceProvider": _SOURCE_PROVIDER,
            "sourceGeneration": self._generation,
            "firstSourceSequence": first_sequence,
            "lastSourceSequence": last_sequence,
            "sourceEventCount": source_event_count,
            "sourceProofHash": _hash(source_proof),
            "interval": _TIMEFRAME,
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "bars": bars,
            "observedAt": _utc_text(observed, "window.observedAt"),
        }
        window_signature = self._sign("BAR_WINDOW", window_body)
        for bar in bars:
            self._sequences_for_bar(bar)
        archive_body = self._journal.window_archive(
            arm_id=self._arm_id,
            owner_token_hash=self._owner_token_hash,
            first_sequence=int(first_sequence),
            last_sequence=int(last_sequence),
            next_open_sequence=int(event.source_sequence),
            expected_bars=bars,
            expected_source_proof_hash=window_body["sourceProofHash"],
        )
        arm_snapshot = self._journal.snapshot(self._arm_id)
        observation_id = "kis-source-observation-" + _hash(
            {
                "armId": self._arm_id,
                "sourceGeneration": self._generation,
                "socketIdentityHash": self._socket_identity_hash,
                "windowHash": _hash(window_body),
                "boundaryMarketDataHash": boundary.market_data_hash,
            }
        )[:32]
        evaluation_body = {
            "schemaVersion": "kis-domestic-natural-breakout-evaluation-proof/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "armId": self._arm_id,
            "sourceGeneration": self._generation,
            "socketIdentityHash": self._socket_identity_hash,
            "windowHash": _hash(window_body),
            "rawArchiveHash": _hash(archive_body),
            "strategy": "KIS_DOMESTIC_VOLATILITY_BREAKOUT_10X0.3",
            "priorBarCount": 10,
            "averageRange": _decimal_text(average_range),
            "breakoutMultiplier": "0.3",
            "priorClose": prior[-1]["close"],
            "currentHigh": bars[-1]["high"],
            "triggerPrice": _decimal_text(trigger),
            "naturalSignal": "BUY",
            "barCloseAt": bars[-1]["closeAt"],
            "nextOpenAt": _utc_text(opened, "boundary.barOpenAt"),
            "nextOpenObservedAt": _utc_text(observed, "boundary.observedAt"),
        }
        evaluation_signature = self._sign(
            "NATURAL_BREAKOUT_EVALUATION", evaluation_body
        )
        stored = {
            "schemaVersion": "kis-domestic-functional-source-observation-record/v1",
            "observationId": observation_id,
            "armId": self._arm_id,
            "sourceGeneration": self._generation,
            "socketIdentityHash": self._socket_identity_hash,
            "captureHeadHash": arm_snapshot["rawHeadHash"],
            "windowBody": window_body,
            "windowSignature": window_signature,
            "rawArchive": archive_body,
            "rawArchiveHash": _hash(archive_body),
            "evaluationProof": evaluation_body,
            "evaluationProofHash": _hash(evaluation_body),
            "evaluationSignature": evaluation_signature,
            "averageRange": _decimal_text(average_range),
            "triggerPrice": _decimal_text(trigger),
            "naturalSignal": "BUY",
            "boundary": {
                "barOpenAt": _utc_text(opened, "boundary.barOpenAt"),
                "observedAt": _utc_text(observed, "boundary.observedAt"),
                "openPriceKrw": _decimal_text(Decimal(str(boundary.open))),
                "sourceSequence": event.source_sequence,
                "rawEventHash": event.raw_event_hash,
            },
        }
        observation_signature = self._sign("SOURCE_OBSERVATION", stored)
        existing = self._observations.get(observation_id)
        if existing is not None and existing != stored:
            raise KisDomesticFunctionalSourceBlocked(
                "natural-observation-identity-conflict"
            )
        durable_hash = self._journal.seal_observation(
            arm_id=self._arm_id,
            owner_token_hash=self._owner_token_hash,
            observation_record=stored,
            observation_signature=observation_signature,
            created_at=_utc_text(observed, "observation.createdAt"),
        )
        if not hmac.compare_digest(durable_hash, _hash(stored)):
            raise KisDomesticFunctionalSourceBlocked(
                "durable-observation-hash-mismatch"
            )
        self._observations[observation_id] = stored
        self._observation_attestations[observation_id] = {
            "durableObservationHash": durable_hash,
            "durableObservationSignature": observation_signature,
        }
        return self._public_observation(
            stored,
            self._observation_attestations[observation_id],
        )

    def _sequences_for_bar(self, bar: Mapping[str, Any]) -> list[str]:
        opened = _parse_utc(bar["openAt"], "bar.openAt")
        closed = _parse_utc(bar["closeAt"], "bar.closeAt")
        rows = [
            event.source_sequence
            for event in self._raw_events.values()
            if event.bucket_open == opened and event.bucket_close == closed
        ]
        if (
            len(rows) != int(bar["eventCount"])
            or not rows
            or rows[0] != bar["sourceSequenceStart"]
            or rows[-1] != bar["sourceSequenceEnd"]
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "raw-archive-bar-membership-mismatch"
            )
        return rows

    def _sign(self, domain: str, body: Mapping[str, Any]) -> str:
        try:
            signature = self._signer(domain, deepcopy(dict(body)))
        except BaseException as exc:
            raise KisDomesticFunctionalSourceBlocked(
                f"capture-signing-failed:{type(exc).__name__}"
            ) from None
        if not _signature_text(signature):
            raise KisDomesticFunctionalSourceBlocked("capture-signature-invalid")
        return signature

    def _durable_stored_observation(
        self,
        observation_id: str,
    ) -> dict[str, Any]:
        cached = self._observations.get(str(observation_id or ""))
        if cached is None:
            raise KisDomesticFunctionalSourceBlocked("observation-not-found")
        durable = self._journal.read_observation(str(observation_id))
        if (
            durable["state"]
            not in {"NATURAL_BUY_OBSERVED", "NEXT_OPEN_TRIGGER_SEALED"}
            or durable["record"] != cached
            or not hmac.compare_digest(durable["recordHash"], _hash(cached))
            or cached.get("armId") != self._arm_id
        ):
            raise KisDomesticFunctionalSourceBlocked(
                "durable-observation-cache-or-state-mismatch"
            )
        return deepcopy(cached)

    @staticmethod
    def _public_observation(
        stored: Mapping[str, Any],
        durable_attestation: Mapping[str, str],
    ) -> dict[str, Any]:
        return {
            "schemaVersion": "kis-domestic-functional-public-observation/v1",
            "observationId": stored["observationId"],
            "naturalSignal": "BUY",
            "averageRange": stored["averageRange"],
            "triggerPrice": stored["triggerPrice"],
            "windowHash": _hash(stored["windowBody"]),
            "windowSignature": stored["windowSignature"],
            "rawArchiveHash": stored["rawArchiveHash"],
            "evaluationProofHash": stored["evaluationProofHash"],
            "evaluationSignature": stored["evaluationSignature"],
            "durableObservationHash": durable_attestation[
                "durableObservationHash"
            ],
            "durableObservationSignature": durable_attestation[
                "durableObservationSignature"
            ],
            "publicArmId": stored["armId"],
            "socketIdentityHash": stored["socketIdentityHash"],
            "upstreamExchangeSequenceAvailable": False,
            "upstreamPacketCompletenessAttested": False,
            "acceptedIngressContinuityOnly": True,
            "sourceGeneration": stored["windowBody"]["sourceGeneration"],
            "barCloseAt": stored["windowBody"]["bars"][-1]["closeAt"],
            "nextOpenObservedAt": stored["boundary"]["observedAt"],
            "armedPublicDataOnly": True,
            "accountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "mutationAuthorityAvailable": False,
            "networkAvailable": False,
            "productionAvailable": False,
        }

    def lane_window_arguments(self, observation_id: str) -> dict[str, Any]:
        stored = self._durable_stored_observation(observation_id)
        return {
            "window_body": deepcopy(stored["windowBody"]),
            "server_authority_signature": stored["windowSignature"],
        }

    def raw_archive(self, observation_id: str) -> dict[str, Any]:
        stored = self._durable_stored_observation(observation_id)
        return {
            "body": deepcopy(stored["rawArchive"]),
            "archiveHash": stored["rawArchiveHash"],
        }

    def lane_next_open_arguments(
        self,
        observation_id: str,
        *,
        evaluation_id: str,
    ) -> dict[str, Any]:
        stored = self._durable_stored_observation(observation_id)
        if type(evaluation_id) is not str or not _EVALUATION.fullmatch(evaluation_id):
            raise KisDomesticFunctionalSourceBlocked("evaluation-id-invalid")
        boundary = stored["boundary"]
        proof = {
            "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceProvider": _SOURCE_PROVIDER,
            "sourceGeneration": stored["windowBody"]["sourceGeneration"],
            "sourceSequence": boundary["sourceSequence"],
            "rawEventHash": boundary["rawEventHash"],
            "barOpenAt": boundary["barOpenAt"],
            "observedAt": boundary["observedAt"],
        }
        trigger_body = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": "KIS_WEBSOCKET",
            "sourceProvider": _SOURCE_PROVIDER,
            "sourceGeneration": stored["windowBody"]["sourceGeneration"],
            "sourceSequence": boundary["sourceSequence"],
            "rawEventHash": boundary["rawEventHash"],
            "sourceProofHash": _hash(proof),
            "eventType": "NEXT_BAR_OPEN",
            "evaluationId": evaluation_id,
            "barOpenAt": boundary["barOpenAt"],
            "observedAt": boundary["observedAt"],
            "openPriceKrw": boundary["openPriceKrw"],
        }
        signature = self._sign("NEXT_OPEN", trigger_body)
        result = {
            "trigger_body": trigger_body,
            "server_authority_signature": signature,
        }
        previous = self._sealed_triggers.get(observation_id)
        if previous is not None:
            if previous != result:
                raise KisDomesticFunctionalSourceBlocked(
                    "observation-already-bound-to-another-evaluation"
                )
            return deepcopy(previous)
        durable_trigger_hash = self._journal.seal_trigger(
            arm_id=self._arm_id,
            owner_token_hash=self._owner_token_hash,
            observation_id=str(observation_id),
            evaluation_id=evaluation_id,
            trigger_record=trigger_body,
            trigger_signature=signature,
            updated_at=boundary["observedAt"],
        )
        if not hmac.compare_digest(durable_trigger_hash, _hash(trigger_body)):
            raise KisDomesticFunctionalSourceBlocked(
                "durable-trigger-hash-mismatch"
            )
        self._sealed_triggers[observation_id] = deepcopy(result)
        return deepcopy(result)


def source_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-source-component/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "trId": _TR_ID,
        "source": _SOURCE,
        "reuses": "trading_runtime.realtime_feeds.KisWebSocketClosedBarFeed",
        "upstreamExchangeSequenceAvailable": False,
        "upstreamPacketCompletenessAttested": False,
        "acceptedIngressContinuityOnly": True,
        "journalSchemaVersion": SOURCE_JOURNAL_SCHEMA_VERSION,
        "journalSchemaFingerprint": SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
        "signedArmTransitionChainAvailable": True,
        "registryEd25519PublicKeyIdentitySupported": True,
        "legacyStringKeyIdentityOfflineMockOnly": True,
        "armedPublicDataOnly": True,
        "networkAvailable": False,
        "productionAvailable": False,
        "accountAuthorityAvailable": False,
        "tokenAuthorityAvailable": False,
        "mutationAuthorityAvailable": False,
        "releaseEvidenceEligible": False,
    }


__all__ = [
    "KIS_DOMESTIC_FUNCTIONAL_SOURCE_ACCOUNT_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_SOURCE_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_SOURCE_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_SOURCE_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_SOURCE_TOKEN_AUTHORITY_AVAILABLE",
    "SOURCE_JOURNAL_SCHEMA_FINGERPRINT",
    "SOURCE_JOURNAL_SCHEMA_VERSION",
    "MARKET_SOURCE_ACK_SCHEMA",
    "MARKET_SOURCE_LINK_SCHEMA",
    "DurableKisDomesticPublicArmJournal",
    "KisDomesticFunctionalMarketSourceDurableWriter",
    "KisDomesticFunctionalPublicSource",
    "KisDomesticFunctionalSourceBlocked",
    "exact_kis_domestic_functional_subscription",
    "source_component_status",
]
