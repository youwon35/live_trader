from __future__ import annotations

"""Durable server-owned approval store for the Binance Spot functional lane.

A permit content hash proves integrity, not operator authorization.  This store
accepts a permit only through an injected server-side approval verifier, then
allows one atomic claim and binds that claim to the single durable session.
Client code receives/uses only the permit id/hash reference after approval.
"""

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping

from .binance_spot_continuous_functional import ExactPermit


ROUTE_KEY = "BINANCE_SPOT_CONTINUOUS:BTCUSDT:5m"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_APPROVAL_FIELDS = frozenset(
    {
        "approvalId",
        "operatorId",
        "operatorAuthenticated",
        "operatorApproved",
        "permitId",
        "permitHash",
        "accountFingerprint",
        "executionRoute",
        "symbol",
        "approvedAt",
        "nonce",
        "activationResealAuthorized",
        "activeDurationSeconds",
        "exclusiveAccountConfirmed",
        "noManualTradingConfirmed",
        "noBotsConfirmed",
        "noOtherApiKeysConfirmed",
        "firstLiveBootstrapAuthorized",
        "firstLiveBootstrapRequired",
        "firstLiveBootstrapId",
        "firstLiveBootstrapHash",
        "firstLiveSessionNonceHash",
        "firstLiveCodeHash",
    }
)
_CANDIDATE_FIELDS = frozenset(
    {
        "schemaVersion",
        "approvalId",
        "permitId",
        "permitHash",
        "accountFingerprint",
        "executionRoute",
        "symbol",
        "serverManaged",
        "singleUse",
        "issuer",
        "issuedAt",
        "expiresAt",
        "permitExpiresAt",
        "nonce",
        "firstLiveBootstrapRequired",
        "firstLiveBootstrapId",
        "firstLiveBootstrapHash",
        "firstLiveSessionNonceHash",
        "firstLiveCodeHash",
        "serverSignature",
    }
)


class BinanceSpotPermitApprovalError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: str) -> str:
    return hashlib.sha256(_text(value).encode("utf-8")).hexdigest()


def _approval_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _epoch(value: object) -> float:
    try:
        parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise BinanceSpotPermitApprovalError("approval time is invalid") from exc
    if parsed.tzinfo is None:
        raise BinanceSpotPermitApprovalError("approval time needs a UTC offset")
    return parsed.astimezone(timezone.utc).timestamp()


def _bootstrap_envelope(
    value: Mapping[str, Any],
) -> tuple[bool, str, str, str, str]:
    required = value.get("firstLiveBootstrapRequired") is True
    bootstrap_id = _text(value.get("firstLiveBootstrapId"))
    bootstrap_hash = _text(value.get("firstLiveBootstrapHash")).lower()
    nonce_hash = _text(value.get("firstLiveSessionNonceHash")).lower()
    code_hash = _text(value.get("firstLiveCodeHash")).lower()
    if required:
        if (
            value.get("firstLiveBootstrapAuthorized") is not True
            or len(bootstrap_id) < 20
            or _SHA256_RE.fullmatch(bootstrap_hash) is None
            or _SHA256_RE.fullmatch(nonce_hash) is None
            or _SHA256_RE.fullmatch(code_hash) is None
        ):
            raise BinanceSpotPermitApprovalError(
                "first-live bootstrap approval envelope is invalid"
            )
    elif any((bootstrap_id, bootstrap_hash, nonce_hash, code_hash)):
        raise BinanceSpotPermitApprovalError(
            "non-bootstrap approval contains a bootstrap identity"
        )
    return required, bootstrap_id, bootstrap_hash, nonce_hash, code_hash


class DurableBinanceSpotApprovedPermitStore:
    """Append-safe approval/claim state owned by the backend process."""

    def __init__(
        self,
        path: str | Path,
        *,
        approval_verifier: Callable[[Mapping[str, Any]], bool] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.approval_verifier = approval_verifier or (lambda _: False)
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
                CREATE TABLE IF NOT EXISTS binance_spot_functional_approvals (
                    permit_id TEXT PRIMARY KEY,
                    permit_hash TEXT NOT NULL UNIQUE,
                    permit_json TEXT NOT NULL,
                    account_fingerprint TEXT NOT NULL,
                    strategy_artifact_hash TEXT NOT NULL,
                    strategy_instance_hash TEXT NOT NULL,
                    route_key TEXT NOT NULL,
                    approval_id TEXT NOT NULL UNIQUE,
                    approval_hash TEXT NOT NULL,
                    operator_id TEXT NOT NULL,
                    approved_epoch REAL NOT NULL,
                    state TEXT NOT NULL,
                    claim_token_hash TEXT NOT NULL DEFAULT '',
                    claimed_owner_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '',
                    first_live_bootstrap_required INTEGER NOT NULL DEFAULT 0,
                    first_live_bootstrap_id TEXT NOT NULL DEFAULT '',
                    first_live_bootstrap_hash TEXT NOT NULL DEFAULT '',
                    first_live_session_nonce_hash TEXT NOT NULL DEFAULT '',
                    first_live_code_hash TEXT NOT NULL DEFAULT '',
                    updated_epoch REAL NOT NULL
                )
                """
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(binance_spot_functional_approvals)"
                ).fetchall()
            }
            migrations = {
                "first_live_bootstrap_required": (
                    "INTEGER NOT NULL DEFAULT 0"
                ),
                "first_live_bootstrap_id": "TEXT NOT NULL DEFAULT ''",
                "first_live_bootstrap_hash": "TEXT NOT NULL DEFAULT ''",
                "first_live_session_nonce_hash": "TEXT NOT NULL DEFAULT ''",
                "first_live_code_hash": "TEXT NOT NULL DEFAULT ''",
            }
            for column, declaration in migrations.items():
                if column not in columns:
                    connection.execute(
                        "ALTER TABLE binance_spot_functional_approvals "
                        f"ADD COLUMN {column} {declaration}"
                    )
            connection.commit()

    def approve(
        self,
        permit_payload: Mapping[str, Any],
        approval_attestation: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = float(self.clock())
        if set(approval_attestation) != _APPROVAL_FIELDS:
            raise BinanceSpotPermitApprovalError(
                "operator approval attestation fields are not exact"
            )
        if (
            approval_attestation.get("operatorAuthenticated") is not True
            or approval_attestation.get("operatorApproved") is not True
            or approval_attestation.get("activationResealAuthorized") is not True
            or int(approval_attestation.get("activeDurationSeconds") or 0) != 7200
            or approval_attestation.get("exclusiveAccountConfirmed") is not True
            or approval_attestation.get("noManualTradingConfirmed") is not True
            or approval_attestation.get("noBotsConfirmed") is not True
            or approval_attestation.get("noOtherApiKeysConfirmed") is not True
            or approval_attestation.get("firstLiveBootstrapAuthorized") is not True
            or not self.approval_verifier(dict(approval_attestation))
        ):
            raise BinanceSpotPermitApprovalError(
                "server-authenticated operator approval is absent"
            )
        (
            bootstrap_required,
            bootstrap_id,
            bootstrap_hash,
            bootstrap_nonce_hash,
            bootstrap_code_hash,
        ) = _bootstrap_envelope(approval_attestation)
        permit = ExactPermit.parse(permit_payload, now_epoch=now)
        exact = {
            "permitId": permit.permit_id,
            "permitHash": permit.permit_hash,
            "accountFingerprint": permit.binding.account_fingerprint,
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
        }
        for field, expected in exact.items():
            actual = _text(approval_attestation.get(field))
            if field.endswith("Hash") or field == "accountFingerprint":
                actual = actual.lower()
            if not secrets.compare_digest(actual, expected):
                raise BinanceSpotPermitApprovalError(
                    f"operator approval changed exact {field}"
                )
        approved_epoch = _epoch(approval_attestation.get("approvedAt"))
        if abs(now - approved_epoch) > 300:
            raise BinanceSpotPermitApprovalError("operator approval is stale")
        approval_id = _text(approval_attestation.get("approvalId"))
        operator_id = _text(approval_attestation.get("operatorId"))
        nonce = _text(approval_attestation.get("nonce"))
        if len(approval_id) < 12 or len(operator_id) < 3 or len(nonce) < 24:
            raise BinanceSpotPermitApprovalError(
                "operator approval identity/nonce is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO binance_spot_functional_approvals (
                        permit_id, permit_hash, permit_json,
                        account_fingerprint, strategy_artifact_hash,
                        strategy_instance_hash, route_key, approval_id,
                        approval_hash, operator_id, approved_epoch, state,
                        detail, first_live_bootstrap_required,
                        first_live_bootstrap_id, first_live_bootstrap_hash,
                        first_live_session_nonce_hash, first_live_code_hash,
                        updated_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'APPROVED',
                              ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        permit.permit_id,
                        permit.permit_hash,
                        _canonical(permit_payload),
                        permit.binding.account_fingerprint,
                        permit.binding.strategy_artifact_hash,
                        permit.binding.strategy_instance_hash,
                        ROUTE_KEY,
                        approval_id,
                        _approval_hash(approval_attestation),
                        operator_id,
                        approved_epoch,
                        "server-authenticated one-shot operator approval",
                        1 if bootstrap_required else 0,
                        bootstrap_id,
                        bootstrap_hash,
                        bootstrap_nonce_hash,
                        bootstrap_code_hash,
                        now,
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                raise BinanceSpotPermitApprovalError(
                    "permit/approval is already published and cannot be replayed"
                ) from exc
        return self.status(permit.permit_id)

    def issue_candidate(
        self,
        permit_payload: Mapping[str, Any],
        server_record: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one inert server-selected permit; it cannot be claimed.

        The raw permit and its caps never cross the HTTP command boundary.
        Only a later authenticated operator confirmation may CAS ``ISSUED``
        to ``APPROVED``.  A candidate is single-use even when start fails.
        """

        now = float(self.clock())
        permit = ExactPermit.parse(permit_payload, now_epoch=now)
        if set(server_record) != _CANDIDATE_FIELDS:
            raise BinanceSpotPermitApprovalError(
                "server permit candidate fields are not exact"
            )
        exact = {
            "schemaVersion": "binance-spot-functional-permit-candidate/v1",
            "permitId": permit.permit_id,
            "permitHash": permit.permit_hash,
            "accountFingerprint": permit.binding.account_fingerprint,
            "executionRoute": "BINANCE_SPOT_CONTINUOUS",
            "symbol": "BTCUSDT",
            "permitExpiresAt": datetime.fromtimestamp(
                permit.expires_epoch, tz=timezone.utc
            ).isoformat().replace("+00:00", "Z"),
        }
        bootstrap_required = server_record.get("firstLiveBootstrapRequired") is True
        bootstrap_id = _text(server_record.get("firstLiveBootstrapId"))
        bootstrap_hash = _text(server_record.get("firstLiveBootstrapHash")).lower()
        bootstrap_nonce_hash = _text(
            server_record.get("firstLiveSessionNonceHash")
        ).lower()
        bootstrap_code_hash = _text(
            server_record.get("firstLiveCodeHash")
        ).lower()
        if bootstrap_required:
            if (
                len(bootstrap_id) < 20
                or _SHA256_RE.fullmatch(bootstrap_hash) is None
                or _SHA256_RE.fullmatch(bootstrap_nonce_hash) is None
                or _SHA256_RE.fullmatch(bootstrap_code_hash) is None
            ):
                raise BinanceSpotPermitApprovalError(
                    "server first-live bootstrap envelope is invalid"
                )
        elif any(
            (bootstrap_id, bootstrap_hash, bootstrap_nonce_hash, bootstrap_code_hash)
        ):
            raise BinanceSpotPermitApprovalError(
                "ordinary candidate contains a first-live bootstrap identity"
            )
        for field, expected in exact.items():
            actual = _text(server_record.get(field))
            if field.endswith("Hash") or field == "accountFingerprint":
                actual = actual.lower()
            if not secrets.compare_digest(actual, expected):
                raise BinanceSpotPermitApprovalError(
                    f"server permit candidate changed exact {field}"
                )
        issued_epoch = _epoch(server_record.get("issuedAt"))
        expires_epoch = _epoch(server_record.get("expiresAt"))
        approval_id = _text(server_record.get("approvalId"))
        nonce = _text(server_record.get("nonce"))
        if (
            server_record.get("serverManaged") is not True
            or server_record.get("singleUse") is not True
            or _text(server_record.get("issuer")) != "LIVE_TRADER_SERVER"
            or len(approval_id) < 12
            or len(nonce) < 24
            or not _text(server_record.get("serverSignature"))
            or abs(now - issued_epoch) > 15
            or expires_epoch <= now
            or expires_epoch - issued_epoch > 300
            or self.approval_verifier(dict(server_record)) is not True
        ):
            raise BinanceSpotPermitApprovalError(
                "server permit candidate identity/signature/window is invalid"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                """
                SELECT permit_id FROM binance_spot_functional_approvals
                WHERE state IN ('ISSUED','APPROVED','CLAIMED','ACTIVE')
                """
            ).fetchall()
            if active:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "another Binance functional permit candidate is active"
                )
            try:
                connection.execute(
                    """
                    INSERT INTO binance_spot_functional_approvals (
                        permit_id, permit_hash, permit_json,
                        account_fingerprint, strategy_artifact_hash,
                        strategy_instance_hash, route_key, approval_id,
                        approval_hash, operator_id, approved_epoch, state,
                        detail, first_live_bootstrap_required,
                        first_live_bootstrap_id, first_live_bootstrap_hash,
                        first_live_session_nonce_hash, first_live_code_hash,
                        updated_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ISSUED',
                              ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        permit.permit_id,
                        permit.permit_hash,
                        _canonical(permit_payload),
                        permit.binding.account_fingerprint,
                        permit.binding.strategy_artifact_hash,
                        permit.binding.strategy_instance_hash,
                        ROUTE_KEY,
                        approval_id,
                        _approval_hash(server_record),
                        "LIVE_TRADER_SERVER",
                        issued_epoch,
                        "server-selected inert candidate",
                        1 if bootstrap_required else 0,
                        bootstrap_id,
                        bootstrap_hash,
                        bootstrap_nonce_hash,
                        bootstrap_code_hash,
                        now,
                    ),
                )
                connection.commit()
            except sqlite3.IntegrityError as exc:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "permit candidate is a replay"
                ) from exc
        return self.candidate_status(approval_id)

    def approve_issued_candidate(
        self,
        *,
        approval_id: str,
        approval_attestation: Mapping[str, Any],
    ) -> dict[str, Any]:
        """CAS the exact inert record after typed operator confirmation."""

        now = float(self.clock())
        if set(approval_attestation) != _APPROVAL_FIELDS:
            raise BinanceSpotPermitApprovalError(
                "operator approval attestation fields are not exact"
            )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM binance_spot_functional_approvals
                WHERE approval_id=?
                """,
                (_text(approval_id),),
            ).fetchone()
            if row is None or _text(row["state"]).upper() != "ISSUED":
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "server permit candidate is absent or already consumed"
                )
            permit = ExactPermit.parse(
                json.loads(row["permit_json"]), now_epoch=now
            )
            (
                bootstrap_required,
                bootstrap_id,
                bootstrap_hash,
                bootstrap_nonce_hash,
                bootstrap_code_hash,
            ) = _bootstrap_envelope(approval_attestation)
            exact = {
                "approvalId": _text(row["approval_id"]),
                "permitId": permit.permit_id,
                "permitHash": permit.permit_hash,
                "accountFingerprint": permit.binding.account_fingerprint,
                "executionRoute": "BINANCE_SPOT_CONTINUOUS",
                "symbol": "BTCUSDT",
            }
            valid = (
                approval_attestation.get("operatorAuthenticated") is True
                and approval_attestation.get("operatorApproved") is True
                and self.approval_verifier(dict(approval_attestation)) is True
                and approval_attestation.get("activationResealAuthorized") is True
                and int(approval_attestation.get("activeDurationSeconds") or 0)
                == 7200
                and approval_attestation.get("exclusiveAccountConfirmed") is True
                and approval_attestation.get("noManualTradingConfirmed") is True
                and approval_attestation.get("noBotsConfirmed") is True
                and approval_attestation.get("noOtherApiKeysConfirmed") is True
                and approval_attestation.get("firstLiveBootstrapAuthorized") is True
                and bootstrap_required
                == bool(row["first_live_bootstrap_required"])
                and secrets.compare_digest(
                    bootstrap_id, _text(row["first_live_bootstrap_id"])
                )
                and secrets.compare_digest(
                    bootstrap_hash,
                    _text(row["first_live_bootstrap_hash"]).lower(),
                )
                and secrets.compare_digest(
                    bootstrap_nonce_hash,
                    _text(row["first_live_session_nonce_hash"]).lower(),
                )
                and secrets.compare_digest(
                    bootstrap_code_hash,
                    _text(row["first_live_code_hash"]).lower(),
                )
            )
            for field, expected in exact.items():
                actual = _text(approval_attestation.get(field))
                if field.endswith("Hash") or field == "accountFingerprint":
                    actual = actual.lower()
                valid = valid and secrets.compare_digest(actual, expected)
            approved_epoch = _epoch(approval_attestation.get("approvedAt"))
            valid = (
                valid
                and abs(now - approved_epoch) <= 15
                and 0 <= now - float(row["updated_epoch"]) <= 300
                and len(_text(approval_attestation.get("operatorId"))) >= 3
                and len(_text(approval_attestation.get("nonce"))) >= 24
            )
            if not valid:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "typed operator approval changed the server candidate"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='APPROVED', approval_hash=?, operator_id=?,
                    approved_epoch=?, detail=?, updated_epoch=?
                WHERE approval_id=? AND permit_id=? AND state='ISSUED'
                """,
                (
                    _approval_hash(approval_attestation),
                    _text(approval_attestation.get("operatorId")),
                    approved_epoch,
                    "typed operator confirmation approved exact candidate",
                    now,
                    _text(approval_id),
                    permit.permit_id,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "permit candidate approval CAS changed"
                )
            connection.commit()
        return self.status(permit.permit_id)

    def candidate_status(self, approval_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT permit_id, permit_hash, account_fingerprint,
                       route_key, approval_id, state, session_id, detail,
                       updated_epoch, first_live_bootstrap_required,
                       first_live_bootstrap_id, first_live_bootstrap_hash,
                       first_live_session_nonce_hash, first_live_code_hash
                FROM binance_spot_functional_approvals WHERE approval_id=?
                """,
                (_text(approval_id),),
            ).fetchone()
        if row is None:
            raise BinanceSpotPermitApprovalError(
                "server permit candidate is missing"
            )
        return dict(row)

    def server_permit_for_approval(self, approval_id: str) -> dict[str, Any]:
        """Resolve the immutable/resealed permit only inside the backend graph."""

        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT permit_json, state
                FROM binance_spot_functional_approvals WHERE approval_id=?
                """,
                (_text(approval_id),),
            ).fetchone()
        if row is None or _text(row["state"]).upper() not in {
            "APPROVED",
            "CLAIMED",
            "ACTIVE",
        }:
            raise BinanceSpotPermitApprovalError(
                "server approval permit is unavailable"
            )
        payload = json.loads(row["permit_json"])
        ExactPermit.parse(payload, now_epoch=float(self.clock()))
        return payload

    def issued_pointer(self) -> dict[str, Any] | None:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT approval_id, permit_id, permit_hash, account_fingerprint,
                       state, updated_epoch
                FROM binance_spot_functional_approvals WHERE state='ISSUED'
                """
            ).fetchall()
        if len(rows) > 1:
            raise BinanceSpotPermitApprovalError(
                "multiple inert permit candidates require manual review"
            )
        return dict(rows[0]) if rows else None

    def authority_pointer(self) -> dict[str, Any] | None:
        """Return the sole approved/claimed/active order-authority pointer."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT approval_id, permit_id, permit_hash,
                       account_fingerprint, state, session_id, updated_epoch
                FROM binance_spot_functional_approvals
                WHERE state IN ('APPROVED','CLAIMED','ACTIVE')
                """
            ).fetchall()
        if len(rows) > 1:
            raise BinanceSpotPermitApprovalError(
                "multiple Binance functional authorities require manual review"
            )
        return dict(rows[0]) if rows else None

    def order_authority_pointer(self) -> dict[str, Any] | None:
        """Return any candidate/session state that must exclude ordinary Binance."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT approval_id, permit_id, permit_hash,
                       account_fingerprint, state, session_id, updated_epoch,
                       first_live_bootstrap_required,
                       first_live_bootstrap_id, first_live_bootstrap_hash,
                       first_live_session_nonce_hash, first_live_code_hash
                FROM binance_spot_functional_approvals
                WHERE state IN ('ISSUED','APPROVED','CLAIMED','ACTIVE')
                """
            ).fetchall()
        if len(rows) > 1:
            raise BinanceSpotPermitApprovalError(
                "multiple Binance order-authority pointers require manual review"
            )
        return dict(rows[0]) if rows else None

    def retire_issued_candidate(
        self, *, approval_id: str, detail: str
    ) -> dict[str, Any]:
        """Fail an inert, unapproved candidate with a one-way CAS."""

        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='FAILED', detail=?, updated_epoch=?
                WHERE approval_id=? AND state='ISSUED'
                """,
                (
                    _text(detail)[:500],
                    float(self.clock()),
                    _text(approval_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "issued candidate retirement CAS changed"
                )
            connection.commit()
        return self.candidate_status(approval_id)

    def fail_approved_candidate(
        self, *, approval_id: str, detail: str
    ) -> dict[str, Any]:
        """Consume a typed approval that could not safely begin a session."""

        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='FAILED', detail=?, updated_epoch=?
                WHERE approval_id=? AND state='APPROVED'
                """,
                (
                    _text(detail)[:500],
                    float(self.clock()),
                    _text(approval_id),
                ),
            )
            if cursor.rowcount not in {0, 1}:
                raise BinanceSpotPermitApprovalError(
                    "approved candidate failure CAS changed"
                )
            connection.commit()
        return self.candidate_status(approval_id)

    def claim(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        owner_id: str,
        activation_epoch: float | None = None,
        activation_permit_issuer: Callable[
            [object, float], Mapping[str, Any]
        ] | None = None,
    ) -> tuple[dict[str, Any], str]:
        observed_now = float(self.clock())
        now = (
            observed_now
            if activation_epoch is None
            else float(activation_epoch)
        )
        if abs(observed_now - now) > 5.0:
            raise BinanceSpotPermitApprovalError(
                "activation epoch is not fresh at the server approval boundary"
            )
        token = secrets.token_urlsafe(32)
        normalized_hash = _text(permit_hash).lower()
        if _SHA256_RE.fullmatch(normalized_hash) is None or not _text(owner_id):
            raise BinanceSpotPermitApprovalError("permit claim reference is invalid")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM binance_spot_functional_approvals WHERE permit_id=?",
                (_text(permit_id),),
            ).fetchone()
            if (
                row is None
                or _text(row["state"]).upper() != "APPROVED"
                or not secrets.compare_digest(
                    _text(row["permit_hash"]), normalized_hash
                )
                or _text(row["route_key"]) != ROUTE_KEY
            ):
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "active server-approved permit reference is absent/consumed"
                )
            payload = json.loads(row["permit_json"])
            original = ExactPermit.parse(payload, now_epoch=now)
            active_payload = dict(payload)
            if activation_permit_issuer is not None:
                active_payload = dict(
                    activation_permit_issuer(original.binding, now)
                )
                active = ExactPermit.parse(active_payload, now_epoch=now)
                if (
                    active.binding != original.binding
                    or abs(active.issued_epoch - now) > 0.001
                    or abs(active.expires_epoch - now - 7200) > 0.001
                    or abs(active.cleanup_deadline_epoch - now - 10800) > 0.001
                    or not active.activation_reseal_required
                    or not active.exclusive_account_required
                ):
                    connection.rollback()
                    raise BinanceSpotPermitApprovalError(
                        "activation reseal changed immutable permit scope"
                    )
            else:
                active = original
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET permit_id=?, permit_hash=?, permit_json=?, state='CLAIMED',
                    claim_token_hash=?, claimed_owner_id=?,
                    detail='single-use approval atomically claimed and activation-resealed',
                    updated_epoch=?
                WHERE permit_id=? AND state='APPROVED'
                """,
                (
                    active.permit_id,
                    active.permit_hash,
                    _canonical(active_payload),
                    _hash(token),
                    _text(owner_id),
                    now,
                    _text(permit_id),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "activation permit claim CAS changed"
                )
            connection.commit()
        return active_payload, token

    def bind_session(
        self,
        *,
        permit_id: str,
        claim_token: str,
        session_id: str,
    ) -> None:
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='ACTIVE', session_id=?, claim_token_hash='',
                    detail='approval bound to exact durable session', updated_epoch=?
                WHERE permit_id=? AND state='CLAIMED' AND claim_token_hash=?
                    AND session_id=''
                """,
                (
                    _text(session_id),
                    float(self.clock()),
                    _text(permit_id),
                    _hash(claim_token),
                ),
            )
            if cursor.rowcount != 1:
                raise BinanceSpotPermitApprovalError(
                    "permit claim/session binding changed"
                )
            connection.commit()

    def fail_claim(self, *, permit_id: str, claim_token: str, detail: str) -> None:
        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='FAILED', claim_token_hash='', detail=?, updated_epoch=?
                WHERE permit_id=? AND state='CLAIMED' AND claim_token_hash=?
                """,
                (
                    _text(detail)[:500],
                    float(self.clock()),
                    _text(permit_id),
                    _hash(claim_token),
                ),
            )
            if cursor.rowcount != 1:
                raise BinanceSpotPermitApprovalError(
                    "permit claim failure transition changed"
                )
            connection.commit()

    def fail_start_for_session(
        self,
        *,
        permit_id: str,
        session_id: str,
        detail: str,
    ) -> None:
        """Terminalize a claimed/bound approval after session creation fails.

        This is intentionally not a rollback to APPROVED: the authenticated
        operator approval remains consumed and can never be replayed.
        """

        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM binance_spot_functional_approvals
                WHERE permit_id=?
                """,
                (_text(permit_id),),
            ).fetchone()
            if row is None or _text(row["state"]).upper() not in {
                "CLAIMED",
                "ACTIVE",
            }:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "start-failure approval state changed"
                )
            if _text(row["state"]).upper() == "ACTIVE" and _text(
                row["session_id"]
            ) != _text(session_id):
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "start-failure approval session changed"
                )
            connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='FAILED', session_id=?, claim_token_hash='',
                    detail=?, updated_epoch=?
                WHERE permit_id=? AND state IN ('CLAIMED','ACTIVE')
                """,
                (
                    _text(session_id),
                    _text(detail)[:500],
                    float(self.clock()),
                    _text(permit_id),
                ),
            )
            connection.commit()

    def startup_fail_lost_claim(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        detail: str,
    ) -> dict[str, Any]:
        """Consume a crash-orphaned CLAIMED approval without its lost token.

        This transition is startup-auditor-only: it requires the exact permit
        id/hash, an empty session binding, and never returns the approval to a
        reusable state.
        """

        with self._lock, closing(self._connect()) as connection:
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='FAILED', claim_token_hash='', detail=?, updated_epoch=?
                WHERE permit_id=? AND permit_hash=? AND state='CLAIMED'
                    AND session_id=''
                """,
                (
                    _text(detail)[:500],
                    float(self.clock()),
                    _text(permit_id),
                    _text(permit_hash).lower(),
                ),
            )
            if cursor.rowcount not in {0, 1}:
                raise BinanceSpotPermitApprovalError(
                    "startup lost-claim transition was ambiguous"
                )
            connection.commit()
        return self.status(permit_id)

    def startup_bind_lost_claim_to_cleanup(
        self,
        *,
        permit_id: str,
        permit_hash: str,
        session_id: str,
    ) -> dict[str, Any]:
        """Bind a lost claim only after startup has fenced entry to CLEANUP."""

        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            control = connection.execute(
                """
                SELECT phase, permit_id, permit_hash, session_id
                FROM binance_spot_functional_control WHERE route_key=?
                """,
                (ROUTE_KEY,),
            ).fetchone()
            session = connection.execute(
                """
                SELECT state, permit_id, permit_hash
                FROM binance_spot_functional_sessions WHERE session_id=?
                """,
                (_text(session_id),),
            ).fetchone()
            if (
                control is None
                or _text(control["phase"]).upper() != "CLEANUP"
                or _text(control["session_id"]) != _text(session_id)
                or _text(control["permit_id"]) != _text(permit_id)
                or not secrets.compare_digest(
                    _text(control["permit_hash"]), _text(permit_hash).lower()
                )
                or session is None
                or _text(session["state"]).upper() != "CLEANUP"
                or _text(session["permit_id"]) != _text(permit_id)
                or not secrets.compare_digest(
                    _text(session["permit_hash"]), _text(permit_hash).lower()
                )
            ):
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "startup cleanup approval binding attestation changed"
                )
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='ACTIVE', session_id=?, claim_token_hash='',
                    detail='startup recovery bound lost claim cleanup-only',
                    updated_epoch=?
                WHERE permit_id=? AND permit_hash=? AND state='CLAIMED'
                    AND session_id=''
                """,
                (
                    _text(session_id),
                    float(self.clock()),
                    _text(permit_id),
                    _text(permit_hash).lower(),
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise BinanceSpotPermitApprovalError(
                    "startup lost approval claim is not cleanup-bindable"
                )
            connection.commit()
        return self.status(permit_id)

    def audit_orphaned_claims(
        self,
        *,
        owner_lease_seconds: float = 60.0,
        owner_process_absence_attested: bool = False,
    ) -> dict[str, Any]:
        """Terminalize unowned APPROVED/CLAIMED rows with no live authority.

        The approval claim can commit immediately before ``control.arm``.  A
        process death then loses the raw token while control/core remain
        absent.  This startup-only audit waits out the owner lease, checks the
        shared durable control/session tables, and consumes such approvals as
        FAILED (never reusable).  A matching live ARMED/core identity is left
        for the lifecycle auditor; conflicting durable state is also consumed
        but surfaced for manual review.
        """

        now = float(self.clock())
        lease = float(owner_lease_seconds)
        if lease <= 0 or lease > 60.0:
            raise BinanceSpotPermitApprovalError(
                "startup approval audit lease must be within 60 seconds"
            )
        failed: list[str] = []
        matched: list[str] = []
        pending: list[str] = []
        conflicts: list[str] = []
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT * FROM binance_spot_functional_approvals
                WHERE state IN ('APPROVED','CLAIMED')
                ORDER BY updated_epoch, permit_id
                """
            ).fetchall()
            tables = {
                _text(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for row in rows:
                permit_id = _text(row["permit_id"])
                permit_hash = _text(row["permit_hash"]).lower()
                approval_state = _text(row["state"]).upper()
                if (
                    not owner_process_absence_attested
                    and now - float(row["updated_epoch"]) < lease
                ):
                    pending.append(permit_id)
                    continue
                control_rows: list[sqlite3.Row] = []
                session_rows: list[sqlite3.Row] = []
                if "binance_spot_functional_control" in tables:
                    control_rows = connection.execute(
                        """
                        SELECT * FROM binance_spot_functional_control
                        WHERE phase IN ('ARMED','ACTIVE','CLEANUP','FINAL_RESET')
                        """
                    ).fetchall()
                if "binance_spot_functional_sessions" in tables:
                    session_rows = connection.execute(
                        """
                        SELECT * FROM binance_spot_functional_sessions
                        WHERE state IN ('RUNNING','CLEANUP','RECONCILIATION_REQUIRED')
                        """
                    ).fetchall()
                exact_control = any(
                    _text(item["permit_id"]) == permit_id
                    and secrets.compare_digest(
                        _text(item["permit_hash"]).lower(), permit_hash
                    )
                    for item in control_rows
                )
                exact_session = any(
                    _text(item["permit_id"]) == permit_id
                    and secrets.compare_digest(
                        _text(item["permit_hash"]).lower(), permit_hash
                    )
                    for item in session_rows
                )
                if exact_control or exact_session:
                    matched.append(permit_id)
                    continue
                conflict = bool(control_rows or session_rows)
                detail = (
                    "startup approval claim conflicts with other durable authority; manual review"
                    if conflict
                    else "startup approval claim expired before control/session creation"
                )
                cursor = connection.execute(
                    """
                    UPDATE binance_spot_functional_approvals
                    SET state='FAILED', claim_token_hash='', detail=?, updated_epoch=?
                    WHERE permit_id=? AND permit_hash=? AND state=?
                        AND session_id=''
                    """,
                    (detail, now, permit_id, permit_hash, approval_state),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    raise BinanceSpotPermitApprovalError(
                        "startup approval orphan CAS changed"
                    )
                failed.append(permit_id)
                if conflict:
                    conflicts.append(permit_id)
            connection.commit()
        return {
            "failedPermitIds": failed,
            "matchingAuthorityPermitIds": matched,
            "pendingPermitIds": pending,
            "conflictingPermitIds": conflicts,
            "manualReviewRequired": bool(conflicts) or len(failed) > 1,
        }

    def resolve_active(self, *, session_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT permit_json FROM binance_spot_functional_approvals
                WHERE session_id=? AND state='ACTIVE'
                """,
                (_text(session_id),),
            ).fetchall()
        if len(rows) != 1:
            raise BinanceSpotPermitApprovalError(
                "exact active server-approved permit is unavailable"
            )
        return dict(json.loads(rows[0]["permit_json"]))

    def consume(self, *, session_id: str) -> None:
        with self._lock, closing(self._connect()) as connection:
            existing = connection.execute(
                """
                SELECT state FROM binance_spot_functional_approvals
                WHERE session_id=?
                """,
                (_text(session_id),),
            ).fetchone()
            if existing is not None and _text(existing["state"]).upper() == "CONSUMED":
                return
            cursor = connection.execute(
                """
                UPDATE binance_spot_functional_approvals
                SET state='CONSUMED', detail='final session seal consumed approval',
                    updated_epoch=?
                WHERE session_id=? AND state='ACTIVE'
                """,
                (float(self.clock()), _text(session_id)),
            )
            if cursor.rowcount != 1:
                raise BinanceSpotPermitApprovalError(
                    "active approval consumption changed"
                )
            connection.commit()

    def status(self, permit_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT permit_id, permit_hash, account_fingerprint,
                       strategy_artifact_hash, strategy_instance_hash,
                       route_key, approval_id, approval_hash, operator_id,
                       approved_epoch, state, claimed_owner_id, session_id,
                       detail, updated_epoch
                FROM binance_spot_functional_approvals WHERE permit_id=?
                """,
                (_text(permit_id),),
            ).fetchone()
        if row is None:
            raise BinanceSpotPermitApprovalError("approved permit is missing")
        return dict(row)


__all__ = [
    "BinanceSpotPermitApprovalError",
    "DurableBinanceSpotApprovedPermitStore",
]
