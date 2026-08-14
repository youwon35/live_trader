from __future__ import annotations

"""Independent durable high-water authority for crypto first-live state.

This database must live at a path distinct from the coordinator database.  It
does not know about broker transports.  Its only mutation is an exact
compare-and-advance of the coordinator's latest published event hash.

Keeping this on a separate durability domain detects one-sided coordinator
rollback.  No pair of ordinary mutable local files can prove that both were
not restored to the same valid prefix; production release additionally needs
an independently administered monotonic/WORM checkpoint.
"""

import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time
from typing import Any, Callable, Mapping


HIGH_WATER_SCHEMA_VERSION = "crypto-first-live-high-water-anchor/v1"
GLOBAL_SCOPE = "CRYPTO_FIRST_LIVE_GLOBAL"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class CryptoFirstLiveHighWaterError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


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


class DurableCryptoFirstLiveHighWaterAnchor:
    """Callable implementation of the coordinator high-water protocol."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.clock = clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _now(self) -> float:
        value = float(self.clock())
        if value <= 0 or value != value or value in {float("inf"), float("-inf")}:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-clock-invalid"
            )
        return value

    def _initialize(self) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_high_water_control (
                    scope_key TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    database_id TEXT NOT NULL,
                    coordinator_revision INTEGER NOT NULL,
                    publication_hash TEXT NOT NULL,
                    anchor_revision INTEGER NOT NULL,
                    updated_epoch REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_first_live_high_water_events (
                    event_id TEXT PRIMARY KEY,
                    scope_key TEXT NOT NULL,
                    anchor_revision INTEGER NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    occurred_epoch REAL NOT NULL,
                    previous_hash TEXT NOT NULL,
                    content_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE
                )
                """
            )
            self._verify(conn)
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
        return tuple(str(row[1]) for row in conn.execute(
            f"PRAGMA table_xinfo({table})"
        ))

    @classmethod
    def _verify(cls, conn: sqlite3.Connection) -> None:
        if cls._columns(conn, "crypto_first_live_high_water_control") != (
            "scope_key",
            "schema_version",
            "database_id",
            "coordinator_revision",
            "publication_hash",
            "anchor_revision",
            "updated_epoch",
        ) or cls._columns(conn, "crypto_first_live_high_water_events") != (
            "event_id",
            "scope_key",
            "anchor_revision",
            "event_type",
            "occurred_epoch",
            "previous_hash",
            "content_json",
            "content_hash",
        ):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-schema-mismatch"
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
            (
                "table",
                "crypto_first_live_high_water_control",
                "crypto_first_live_high_water_control",
            ),
            (
                "table",
                "crypto_first_live_high_water_events",
                "crypto_first_live_high_water_events",
            ),
        }:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-sqlite-objects-mismatch"
            )
        controls = conn.execute(
            "SELECT * FROM crypto_first_live_high_water_control"
        ).fetchall()
        events = conn.execute(
            """
            SELECT * FROM crypto_first_live_high_water_events
            ORDER BY anchor_revision, event_id
            """
        ).fetchall()
        if not controls and not events:
            return
        if len(controls) != 1 or not events:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-control-event-invalid"
            )
        control = controls[0]
        if (
            _text(control["scope_key"]) != GLOBAL_SCOPE
            or _text(control["schema_version"])
            != HIGH_WATER_SCHEMA_VERSION
            or _ID_RE.fullmatch(_text(control["database_id"])) is None
            or int(control["coordinator_revision"]) < 0
            or int(control["anchor_revision"]) <= 0
        ):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-control-invalid"
            )
        publication_hash = _text(control["publication_hash"])
        if (
            int(control["coordinator_revision"]) == 0
            and publication_hash != ""
        ) or (
            int(control["coordinator_revision"]) > 0
            and _HASH_RE.fullmatch(publication_hash) is None
        ):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-control-invalid"
            )
        previous_hash = ""
        previous_anchor_revision = 0
        latest: dict[str, Any] = {}
        for event in events:
            content_json = _text(event["content_json"])
            content_hash = hashlib.sha256(
                content_json.encode("utf-8")
            ).hexdigest()
            try:
                payload = json.loads(content_json)
            except json.JSONDecodeError as exc:
                raise CryptoFirstLiveHighWaterError(
                    "crypto-first-live-high-water-event-chain-invalid"
                ) from exc
            if (
                not isinstance(payload, dict)
                or _text(event["scope_key"]) != GLOBAL_SCOPE
                or int(event["anchor_revision"])
                != previous_anchor_revision + 1
                or int(payload.get("anchorRevision", -1))
                != int(event["anchor_revision"])
                or _text(payload.get("eventType"))
                != _text(event["event_type"])
                or float(payload.get("occurredEpoch", -1))
                != float(event["occurred_epoch"])
                or _text(event["previous_hash"]) != previous_hash
                or _text(payload.get("previousHash")) != previous_hash
                or not secrets.compare_digest(
                    _text(event["content_hash"]), content_hash
                )
            ):
                raise CryptoFirstLiveHighWaterError(
                    "crypto-first-live-high-water-event-chain-invalid"
                )
            latest = payload
            previous_hash = content_hash
            previous_anchor_revision = int(event["anchor_revision"])
        control_hash = _digest(
            {str(key): control[key] for key in control.keys()}
        )
        if (
            int(control["anchor_revision"]) != previous_anchor_revision
            or not secrets.compare_digest(
                _text(latest.get("controlHash")), control_hash
            )
        ):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-control-hash-invalid"
            )

    @staticmethod
    def _response(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "scope": GLOBAL_SCOPE,
            "databaseId": _text(row["database_id"]),
            "revision": int(row["coordinator_revision"]),
            "publicationHash": _text(row["publication_hash"]),
            "durable": True,
            "restartVerifiable": True,
        }

    @staticmethod
    def _control(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT * FROM crypto_first_live_high_water_control
            WHERE scope_key=?
            """,
            (GLOBAL_SCOPE,),
        ).fetchone()

    def _append_event(
        self,
        conn: sqlite3.Connection,
        *,
        event_type: str,
        occurred_epoch: float,
    ) -> None:
        row = self._control(conn)
        if row is None:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-control-missing"
            )
        previous = conn.execute(
            """
            SELECT content_hash FROM crypto_first_live_high_water_events
            ORDER BY anchor_revision DESC, event_id DESC LIMIT 1
            """
        ).fetchone()
        previous_hash = _text(previous[0]) if previous is not None else ""
        content = {
            "schemaVersion": HIGH_WATER_SCHEMA_VERSION,
            "scope": GLOBAL_SCOPE,
            "eventType": event_type,
            "occurredEpoch": occurred_epoch,
            "anchorRevision": int(row["anchor_revision"]),
            "previousHash": previous_hash,
            "databaseId": _text(row["database_id"]),
            "coordinatorRevision": int(row["coordinator_revision"]),
            "publicationHash": _text(row["publication_hash"]),
            "controlHash": _digest(
                {str(key): row[key] for key in row.keys()}
            ),
        }
        content_json = _canonical(content)
        content_hash = hashlib.sha256(
            content_json.encode("utf-8")
        ).hexdigest()
        conn.execute(
            """
            INSERT INTO crypto_first_live_high_water_events(
                event_id, scope_key, anchor_revision, event_type,
                occurred_epoch, previous_hash, content_json, content_hash
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                "crypto-first-live-high-water-event-"
                + secrets.token_hex(18),
                GLOBAL_SCOPE,
                int(row["anchor_revision"]),
                event_type,
                occurred_epoch,
                previous_hash,
                content_json,
                content_hash,
            ),
        )

    def __call__(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        value = dict(request)
        if (
            _text(value.get("schemaVersion"))
            != HIGH_WATER_SCHEMA_VERSION
            or _text(value.get("scope")) != GLOBAL_SCOPE
        ):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-request-invalid"
            )
        action = _text(value.get("action"))
        if action not in {"REGISTER_OR_OBSERVE", "ADVANCE"}:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-action-invalid"
            )
        expected_fields = (
            {
                "schemaVersion",
                "action",
                "purpose",
                "scope",
                "databaseId",
                "localRevision",
                "localPublicationHash",
            }
            if action == "REGISTER_OR_OBSERVE"
            else {
                "schemaVersion",
                "action",
                "purpose",
                "scope",
                "databaseId",
                "expectedRevision",
                "expectedPublicationHash",
                "newRevision",
                "newPublicationHash",
            }
        )
        if set(value) != expected_fields or not _text(value.get("purpose")):
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-request-fields-not-exact"
            )
        database_id = _text(value.get("databaseId"))
        if _ID_RE.fullmatch(database_id) is None:
            raise CryptoFirstLiveHighWaterError(
                "crypto-first-live-high-water-database-id-invalid"
            )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._verify(conn)
            row = self._control(conn)
            if row is None:
                try:
                    registration_revision = int(
                        value.get("localRevision", -1)
                    )
                except (TypeError, ValueError) as exc:
                    raise CryptoFirstLiveHighWaterError(
                        "crypto-first-live-high-water-registration-invalid"
                    ) from exc
                if (
                    action != "REGISTER_OR_OBSERVE"
                    or registration_revision != 0
                    or _text(value.get("localPublicationHash")) != ""
                ):
                    raise CryptoFirstLiveHighWaterError(
                        "crypto-first-live-high-water-registration-invalid"
                    )
                now = self._now()
                conn.execute(
                    """
                    INSERT INTO crypto_first_live_high_water_control(
                        scope_key, schema_version, database_id,
                        coordinator_revision, publication_hash,
                        anchor_revision, updated_epoch
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        GLOBAL_SCOPE,
                        HIGH_WATER_SCHEMA_VERSION,
                        database_id,
                        0,
                        "",
                        1,
                        now,
                    ),
                )
                self._append_event(
                    conn, event_type="REGISTERED", occurred_epoch=now
                )
                row = self._control(conn)
            elif _text(row["database_id"]) != database_id:
                raise CryptoFirstLiveHighWaterError(
                    "crypto-first-live-high-water-database-replaced"
                )
            if action == "ADVANCE":
                try:
                    expected_revision = int(
                        value.get("expectedRevision", -1)
                    )
                    new_revision = int(value.get("newRevision", -1))
                except (TypeError, ValueError) as exc:
                    raise CryptoFirstLiveHighWaterError(
                        "crypto-first-live-high-water-advance-invalid"
                    ) from exc
                expected_hash = _text(value.get("expectedPublicationHash"))
                new_hash = _text(value.get("newPublicationHash"))
                if (
                    row is None
                    or expected_revision != int(row["coordinator_revision"])
                    or expected_hash != _text(row["publication_hash"])
                    or new_revision != expected_revision + 1
                    or _HASH_RE.fullmatch(new_hash) is None
                ):
                    raise CryptoFirstLiveHighWaterError(
                        "crypto-first-live-high-water-advance-cas-changed"
                    )
                now = self._now()
                updated = conn.execute(
                    """
                    UPDATE crypto_first_live_high_water_control
                    SET coordinator_revision=?, publication_hash=?,
                        anchor_revision=anchor_revision+1, updated_epoch=?
                    WHERE scope_key=? AND database_id=?
                      AND coordinator_revision=? AND publication_hash=?
                    """,
                    (
                        new_revision,
                        new_hash,
                        now,
                        GLOBAL_SCOPE,
                        database_id,
                        expected_revision,
                        expected_hash,
                    ),
                ).rowcount
                if updated != 1:
                    raise CryptoFirstLiveHighWaterError(
                        "crypto-first-live-high-water-advance-cas-changed"
                    )
                self._append_event(
                    conn, event_type="ADVANCED", occurred_epoch=now
                )
                row = self._control(conn)
            if row is None:
                raise CryptoFirstLiveHighWaterError(
                    "crypto-first-live-high-water-control-missing"
                )
            result = self._response(row)
            self._verify(conn)
            conn.execute("COMMIT")
            return result
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def status(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            self._verify(conn)
            row = self._control(conn)
            return {"phase": "UNREGISTERED"} if row is None else self._response(row)
        finally:
            conn.close()


__all__ = [
    "CryptoFirstLiveHighWaterError",
    "DurableCryptoFirstLiveHighWaterAnchor",
    "GLOBAL_SCOPE",
    "HIGH_WATER_SCHEMA_VERSION",
]
