from __future__ import annotations

"""Lower-assurance Binance exclusivity for one supervised first-live run.

This module is intentionally not an independent account-administration or
causal-closure authority.  It can verify a narrowly bounded supervised run,
but every returned and persisted record is permanently non-promotion and
REAL_E2E-ineligible.
"""

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping
import urllib.parse

from . import crypto_first_live_supervised_release as supervised_release
from .binance_spot_functional_exclusivity import BinanceSpotExclusivityError
from .binance_spot_functional_transport import (
    BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
    BINANCE_SPOT_PRODUCTION_ORIGIN,
    assert_binance_spot_production_origin,
    build_binance_spot_get_request,
)
from .live_adapters import (
    PreparedRequest,
    binance_timestamp_ms,
    env_value,
    send_prepared_request,
    sign_binance_query,
)


ASSURANCE_MODE = "SUPERVISED_NON_PROMOTION"
PROOF_SCHEMA_VERSION = (
    "binance-spot-functional-supervised-exclusivity-proof/v1"
)
OFFICIAL_EVIDENCE_SCHEMA_VERSION = (
    "binance-spot-supervised-official-get-evidence/v1"
)
LOCAL_AUDIT_SCHEMA_VERSION = (
    "binance-spot-supervised-local-process-bot-audit/v1"
)
USER_ATTESTATION_SCHEMA_VERSION = (
    "binance-spot-supervised-user-attestation/v1"
)
MAX_EVIDENCE_AGE_SECONDS = 5.0
API_RESTRICTIONS_ENDPOINT = "/sapi/v1/account/apiRestrictions"
API_TRADING_STATUS_ENDPOINT = "/sapi/v1/account/apiTradingStatus"
BINANCE_SPOT_SUPERVISED_GET_NETWORK_RELEASED = False
_SUPERVISED_GET_NETWORK_CAPABILITY = object()
_PHASES = frozenset({"BASELINE", "ACTIVATION", "PRE_POST", "TERMINAL"})
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


def _protected_binance_spot_supervised_get_network_capability() -> object:
    """Issue the identity-only token exclusively to protected observer wiring."""
    if (
        BINANCE_SPOT_SUPERVISED_GET_NETWORK_RELEASED is not True
        or supervised_release.SUPERVISED_NON_PROMOTION_RELEASED is not True
        or supervised_release.SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED
        is not True
    ):
        raise BinanceSpotExclusivityError(
            "supervised official GET network release is held"
        )
    return _SUPERVISED_GET_NETWORK_CAPABILITY


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _is_hash(value: object) -> bool:
    return type(value) is str and _HASH_RE.fullmatch(value) is not None


def _finite_epoch(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BinanceSpotExclusivityError(f"{label} is not an exact epoch")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise BinanceSpotExclusivityError(f"{label} is invalid")
    return result


def _iso_epoch(value: object, label: str) -> float:
    if type(value) is not str:
        raise BinanceSpotExclusivityError(f"{label} is not an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BinanceSpotExclusivityError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BinanceSpotExclusivityError(f"{label} lacks a timezone")
    return parsed.astimezone(timezone.utc).timestamp()


def _fresh(observed: object, *, now: float, label: str) -> float:
    epoch = _finite_epoch(observed, label)
    age = float(now) - epoch
    if age < -1.0 or age > MAX_EVIDENCE_AGE_SECONDS:
        raise BinanceSpotExclusivityError(f"{label} is stale or future-dated")
    return epoch


class BinanceSpotSupervisedOfficialGetProvider:
    """Exactly three signed official GETs, with no redirect or retry path."""

    def __init__(
        self,
        *,
        sender: Callable[[PreparedRequest], Mapping[str, Any]] = (
            send_prepared_request
        ),
        clock: Callable[[], float] = time.time,
        network_capability: object | None = None,
    ) -> None:
        self.sender = sender
        self.clock = clock
        self._network_capability = network_capability

    def _assert_network_capability(self) -> None:
        if (
            BINANCE_SPOT_SUPERVISED_GET_NETWORK_RELEASED is not True
            or supervised_release.SUPERVISED_NON_PROMOTION_RELEASED is not True
            or supervised_release.SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED
            is not True
            or self._network_capability
            is not _SUPERVISED_GET_NETWORK_CAPABILITY
        ):
            raise BinanceSpotExclusivityError(
                "supervised official GET network capability is closed"
            )

    @staticmethod
    def _digest(value: object) -> str:
        return _hash(value)

    def _sapi_request(self, endpoint: str) -> PreparedRequest:
        if endpoint not in {
            API_RESTRICTIONS_ENDPOINT,
            API_TRADING_STATUS_ENDPOINT,
        }:
            raise BinanceSpotExclusivityError(
                "supervised signed GET endpoint is not allowlisted"
            )
        origin = assert_binance_spot_production_origin(
            env_value("BINANCE_BASE_URL") or BINANCE_SPOT_PRODUCTION_ORIGIN
        )
        api_key = env_value("BINANCE_API_KEY")
        api_secret = env_value("BINANCE_API_SECRET")
        if not api_key or not api_secret:
            raise BinanceSpotExclusivityError(
                "supervised signed GET credentials are unavailable"
            )
        query = {"recvWindow": 5000, "timestamp": binance_timestamp_ms()}
        encoded = sign_binance_query(query, api_secret)
        return PreparedRequest(
            provider="binance-supervised-exclusivity-get",
            method="GET",
            url=origin + endpoint + "?" + encoded,
            endpoint=endpoint,
            headers={"X-MBX-APIKEY": api_key},
            safe_headers={"X-MBX-APIKEY_configured": True},
            body=None,
            query={**query, "signature": "***"},
            blocked_reasons=[],
        )

    def _send_once(self, request: PreparedRequest) -> object:
        self._assert_network_capability()
        parsed = urllib.parse.urlsplit(request.url)
        if (
            request.method != "GET"
            or request.endpoint
            not in {
                API_RESTRICTIONS_ENDPOINT,
                API_TRADING_STATUS_ENDPOINT,
                BINANCE_SPOT_OPEN_ORDERS_ENDPOINT,
            }
            or parsed.scheme != "https"
            or parsed.netloc != "api.binance.com"
            or parsed.hostname != "api.binance.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != request.endpoint
            or parsed.fragment
            or request.body is not None
            or not request.can_send
        ):
            raise BinanceSpotExclusivityError(
                "supervised signed GET request shape/origin changed"
            )
        response = self.sender(request)
        if (
            not isinstance(response, Mapping)
            or response.get("ok") is not True
            or response.get("redirectBlocked") is True
            or "json" not in response
        ):
            raise BinanceSpotExclusivityError(
                "supervised official GET failed without retry"
            )
        return response["json"]

    def __call__(self, **_request: Any) -> dict[str, Any]:
        self._assert_network_capability()
        restrictions = self._send_once(
            self._sapi_request(API_RESTRICTIONS_ENDPOINT)
        )
        trading = self._send_once(
            self._sapi_request(API_TRADING_STATUS_ENDPOINT)
        )
        open_orders = self._send_once(
            build_binance_spot_get_request(
                BINANCE_SPOT_OPEN_ORDERS_ENDPOINT, {}
            )
        )
        if (
            not isinstance(restrictions, Mapping)
            or not isinstance(trading, Mapping)
            or not isinstance(trading.get("data"), Mapping)
            or not isinstance(open_orders, list)
            or any(not isinstance(row, Mapping) for row in open_orders)
        ):
            raise BinanceSpotExclusivityError(
                "supervised official GET response shape is invalid"
            )
        restriction_fields = {
            field: restrictions.get(field)
            for field in (
                "enableReading",
                "enableSpotAndMarginTrading",
                "ipRestrict",
                "enableWithdrawals",
                "enableMargin",
                "enableFutures",
            )
        }
        if any(type(value) is not bool for value in restriction_fields.values()):
            raise BinanceSpotExclusivityError(
                "supervised API restrictions booleans are malformed"
            )
        locked = trading["data"].get("isLocked")
        if type(locked) is not bool:
            raise BinanceSpotExclusivityError(
                "supervised API trading status is malformed"
            )
        now = float(self.clock())
        body = {
            "schemaVersion": OFFICIAL_EVIDENCE_SCHEMA_VERSION,
            "origin": BINANCE_SPOT_PRODUCTION_ORIGIN,
            "observedEpoch": now,
            "apiRestrictions": {
                **restriction_fields,
                "responseHash": self._digest(dict(restrictions)),
            },
            "apiTradingStatus": {
                "locked": locked,
                "responseHash": self._digest(dict(trading)),
            },
            "accountWideOpenOrders": {
                "scope": "ACCOUNT_WIDE_ALL_SYMBOLS",
                "openOrderCount": len(open_orders),
                "responseHash": self._digest(
                    [dict(row) for row in open_orders]
                ),
            },
            "transport": {
                "physicalGetAttemptCount": 3,
                "retryCount": 0,
                "redirectCount": 0,
                "nonGetAttemptCount": 0,
                "mutationAttemptCount": 0,
            },
        }
        return {**body, "evidenceHash": _hash(body)}


def _validate_official_get_evidence(
    value: object, *, now: float
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError(
            "supervised official GET evidence is missing"
        )
    row = dict(value)
    if set(row) != {
        "schemaVersion",
        "origin",
        "observedEpoch",
        "apiRestrictions",
        "apiTradingStatus",
        "accountWideOpenOrders",
        "transport",
        "evidenceHash",
    }:
        raise BinanceSpotExclusivityError(
            "supervised official GET evidence fields are not exact"
        )
    _fresh(row["observedEpoch"], now=now, label="official GET observation")
    restrictions = row.get("apiRestrictions")
    trading = row.get("apiTradingStatus")
    orders = row.get("accountWideOpenOrders")
    transport = row.get("transport")
    restriction_fields = {
        "enableReading",
        "enableSpotAndMarginTrading",
        "ipRestrict",
        "enableWithdrawals",
        "enableMargin",
        "enableFutures",
        "responseHash",
    }
    if (
        row.get("schemaVersion") != OFFICIAL_EVIDENCE_SCHEMA_VERSION
        or row.get("origin") != BINANCE_SPOT_PRODUCTION_ORIGIN
        or not isinstance(restrictions, Mapping)
        or set(restrictions) != restriction_fields
        or any(
            type(restrictions.get(field)) is not bool
            for field in restriction_fields - {"responseHash"}
        )
        or restrictions.get("enableReading") is not True
        or restrictions.get("enableSpotAndMarginTrading") is not True
        or restrictions.get("ipRestrict") is not True
        or not _is_hash(restrictions.get("responseHash"))
        or not isinstance(trading, Mapping)
        or set(trading) != {"locked", "responseHash"}
        or trading.get("locked") is not False
        or not _is_hash(trading.get("responseHash"))
        or not isinstance(orders, Mapping)
        or dict(orders)
        != {
            "scope": "ACCOUNT_WIDE_ALL_SYMBOLS",
            "openOrderCount": 0,
            "responseHash": orders.get("responseHash"),
        }
        or not _is_hash(orders.get("responseHash"))
        or not isinstance(transport, Mapping)
        or dict(transport)
        != {
            "physicalGetAttemptCount": 3,
            "retryCount": 0,
            "redirectCount": 0,
            "nonGetAttemptCount": 0,
            "mutationAttemptCount": 0,
        }
    ):
        raise BinanceSpotExclusivityError(
            "supervised official GET controls are not satisfied"
        )
    body = {key: item for key, item in row.items() if key != "evidenceHash"}
    if not _is_hash(row.get("evidenceHash")) or not hmac.compare_digest(
        row["evidenceHash"], _hash(body)
    ):
        raise BinanceSpotExclusivityError(
            "supervised official GET evidence hash changed"
        )
    return row


def _validate_local_audit(value: object, *, now: float) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError(
            "supervised local process/bot audit is missing"
        )
    row = dict(value)
    fields = {
        "schemaVersion",
        "source",
        "observedEpoch",
        "applicationInstanceLeaseHeld",
        "accountProcessLeaseHeld",
        "authorizedLiveTraderProcessCount",
        "otherLiveTraderProcessCount",
        "authorizedFunctionalBotCount",
        "otherRegisteredBotCount",
        "auditHash",
    }
    body = {key: item for key, item in row.items() if key != "auditHash"}
    _fresh(row.get("observedEpoch"), now=now, label="local audit observation")
    if (
        set(row) != fields
        or row.get("schemaVersion") != LOCAL_AUDIT_SCHEMA_VERSION
        or row.get("source") != "LOCAL_OS_LEASE_AND_SERVER_BOT_REGISTRY"
        or row.get("applicationInstanceLeaseHeld") is not True
        or row.get("accountProcessLeaseHeld") is not True
        or row.get("authorizedLiveTraderProcessCount") != 1
        or row.get("otherLiveTraderProcessCount") != 0
        or row.get("authorizedFunctionalBotCount") != 1
        or row.get("otherRegisteredBotCount") != 0
        or not _is_hash(row.get("auditHash"))
        or not hmac.compare_digest(row["auditHash"], _hash(body))
    ):
        raise BinanceSpotExclusivityError(
            "supervised local process/bot audit is incomplete"
        )
    return row


def _validate_user_attestation(
    value: object,
    *,
    contract: Mapping[str, Any],
    session_id: str,
    permit_id: str,
    permit_hash: str,
    now: float,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError(
            "supervised authenticated user attestation is missing"
        )
    row = dict(value)
    fields = {
        "schemaVersion",
        "source",
        "sessionId",
        "permitId",
        "permitHash",
        "authenticatedUser",
        "exactUserApproval",
        "noManualTrading",
        "noOtherBots",
        "otherApiKeyInventoryUnknown",
        "exactSessionAndCapsAccepted",
        "attestedEpoch",
        "auditAnchorReceiptHash",
        "attestationHash",
    }
    body = {key: item for key, item in row.items() if key != "attestationHash"}
    attested = _finite_epoch(row.get("attestedEpoch"), "user attestation epoch")
    approval = dict(contract["operatorApproval"])
    anchor = dict(contract["auditAnchor"])
    if (
        set(row) != fields
        or row.get("schemaVersion") != USER_ATTESTATION_SCHEMA_VERSION
        or row.get("source") != "AUTHENTICATED_SERVER_USER_CONFIRMATION"
        or row.get("sessionId") != session_id
        or row.get("permitId") != permit_id
        or row.get("permitHash") != permit_hash
        or any(
            row.get(field) is not True
            for field in (
                "authenticatedUser",
                "exactUserApproval",
                "noManualTrading",
                "noOtherBots",
                "otherApiKeyInventoryUnknown",
                "exactSessionAndCapsAccepted",
            )
        )
        or abs(attested - float(approval["approvedEpoch"])) > 1.0
        or attested > now + 1.0
        or row.get("auditAnchorReceiptHash") != anchor["receiptHash"]
        or not _is_hash(row.get("attestationHash"))
        or not hmac.compare_digest(row["attestationHash"], _hash(body))
    ):
        raise BinanceSpotExclusivityError(
            "supervised authenticated user attestation changed"
        )
    return row


def _validate_stream(
    value: object,
    *,
    phase: str,
    session_id: str,
    permit_id: str,
    permit_hash: str,
    coverage_started_epoch: float,
    now: float,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise BinanceSpotExclusivityError(
            "supervised user-data stream snapshot is missing"
        )
    row = dict(value)
    subscribed = _iso_epoch(row.get("subscribedAt"), "stream subscribedAt")
    observed = _iso_epoch(row.get("observedAt"), "stream observedAt")
    if now - observed < -1.0 or now - observed > MAX_EVIDENCE_AGE_SECONDS:
        raise BinanceSpotExclusivityError(
            "supervised user-data stream observation is stale"
        )
    expected_session = "" if phase == "BASELINE" else session_id
    expected_permit = "" if phase == "BASELINE" else permit_id
    expected_hash = "" if phase == "BASELINE" else permit_hash
    if (
        row.get("connected") is not True
        or row.get("authenticated") is not True
        or row.get("sequenceComplete") is not True
        or row.get("gapDetected") is not False
        or row.get("writerHeartbeatFresh") is not True
        or row.get("durableJournal") is not True
        or row.get("sessionId") != expected_session
        or row.get("permitId") != expected_permit
        or row.get("permitHash") != expected_hash
        or subscribed > float(coverage_started_epoch) + 1.0
        or observed < subscribed
        or isinstance(row.get("durableJournalEventCount"), bool)
        or not isinstance(row.get("durableJournalEventCount"), int)
        or row["durableJournalEventCount"] < 0
        or not _is_hash(row.get("durableJournalSealHash"))
    ):
        raise BinanceSpotExclusivityError(
            "supervised continuous user-data stream is incomplete"
        )
    return row


class DurableBinanceSpotSupervisedExclusivityStore:
    """Immutable local phase seals; explicitly not a formal external WORM."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS binance_supervised_exclusivity_proofs (
                    proof_hash TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    boundary_id TEXT NOT NULL,
                    contract_hash TEXT NOT NULL,
                    proof_json TEXT NOT NULL,
                    observed_epoch REAL NOT NULL,
                    created_epoch REAL NOT NULL,
                    UNIQUE(session_id,phase,boundary_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS binance_supervised_proof_no_update
                BEFORE UPDATE ON binance_supervised_exclusivity_proofs
                BEGIN SELECT RAISE(ABORT,'binance-supervised-proof-update-forbidden'); END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS binance_supervised_proof_no_delete
                BEFORE DELETE ON binance_supervised_exclusivity_proofs
                BEGIN SELECT RAISE(ABORT,'binance-supervised-proof-delete-forbidden'); END
                """
            )
            connection.execute("COMMIT")

    @staticmethod
    def _row(value: sqlite3.Row) -> dict[str, Any]:
        try:
            proof = json.loads(value["proof_json"])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise BinanceSpotExclusivityError(
                "durable supervised proof JSON is malformed"
            ) from exc
        proof_hash = _text(value["proof_hash"]).lower()
        contract = proof.get("supervisedContract") if isinstance(proof, Mapping) else None
        if (
            not isinstance(proof, Mapping)
            or not _is_hash(proof_hash)
            or not hmac.compare_digest(proof_hash, _hash(proof))
            or proof.get("sessionId") != value["session_id"]
            or proof.get("phase") != value["phase"]
            or proof.get("boundaryId") != value["boundary_id"]
            or not isinstance(contract, Mapping)
            or contract.get("contractHash") != value["contract_hash"]
            or float(value["created_epoch"]) < float(value["observed_epoch"])
        ):
            raise BinanceSpotExclusivityError(
                "durable supervised proof identity/hash changed"
            )
        return {**dict(value), "proof": dict(proof)}

    def record(self, proof: Mapping[str, Any], *, now_epoch: float) -> dict[str, Any]:
        value = dict(proof)
        proof_hash = _hash(value)
        contract = dict(value["supervisedContract"])
        values = (
            proof_hash,
            value["sessionId"],
            value["phase"],
            value["boundaryId"],
            contract["contractHash"],
            _canonical(value),
            float(value["observedEpoch"]),
            float(now_epoch),
        )
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT * FROM binance_supervised_exclusivity_proofs
                   WHERE session_id=? AND phase=? AND boundary_id=?""",
                (value["sessionId"], value["phase"], value["boundaryId"]),
            ).fetchone()
            if existing is not None:
                row = self._row(existing)
                if not hmac.compare_digest(_text(row["proof_hash"]), proof_hash):
                    raise BinanceSpotExclusivityError(
                        "durable supervised phase proof changed"
                    )
                connection.execute("ROLLBACK")
                return row
            baseline = connection.execute(
                """SELECT contract_hash FROM binance_supervised_exclusivity_proofs
                   WHERE session_id=? ORDER BY created_epoch LIMIT 1""",
                (value["sessionId"],),
            ).fetchone()
            if baseline is not None and not hmac.compare_digest(
                _text(baseline["contract_hash"]), contract["contractHash"]
            ):
                raise BinanceSpotExclusivityError(
                    "supervised session contract changed between phases"
                )
            connection.execute(
                """INSERT INTO binance_supervised_exclusivity_proofs(
                   proof_hash,session_id,phase,boundary_id,contract_hash,
                   proof_json,observed_epoch,created_epoch)
                   VALUES(?,?,?,?,?,?,?,?)""",
                values,
            )
            connection.execute("COMMIT")
        return self.record_for(
            session_id=value["sessionId"],
            phase=value["phase"],
            boundary_id=value["boundaryId"],
        )

    def record_for(self, *, session_id: str, phase: str, boundary_id: str) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """SELECT * FROM binance_supervised_exclusivity_proofs
                   WHERE session_id=? AND phase=? AND boundary_id=?""",
                (_text(session_id), _text(phase).upper(), _text(boundary_id)),
            ).fetchone()
        if row is None:
            raise BinanceSpotExclusivityError(
                "durable supervised phase proof is absent"
            )
        return self._row(row)

    def session_records(self, session_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """SELECT * FROM binance_supervised_exclusivity_proofs
                   WHERE session_id=? ORDER BY created_epoch,phase,boundary_id""",
                (_text(session_id),),
            ).fetchall()
        return [self._row(row) for row in rows]


class BinanceSpotSupervisedExclusivityGuard:
    """Validate and seal lower-assurance controls without promotion claims."""

    assurance_mode = ASSURANCE_MODE

    def __init__(
        self,
        *,
        store: DurableBinanceSpotSupervisedExclusivityStore,
        contract_reader: Callable[..., Mapping[str, Any]] | None,
        official_get_reader: Callable[..., Mapping[str, Any]] | None,
        local_process_bot_audit_reader: (
            Callable[..., Mapping[str, Any]] | None
        ),
        user_attestation_reader: Callable[..., Mapping[str, Any]] | None,
        stream_reader: Callable[[], Mapping[str, Any]] | None,
        independent_authority_reader: (
            Callable[..., Mapping[str, Any]] | None
        ) = None,
        independent_authority_verifier: Any | None = None,
        allow_inprocess_test_evidence: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.contract_reader = contract_reader
        self.official_get_reader = official_get_reader
        self.local_process_bot_audit_reader = local_process_bot_audit_reader
        self.user_attestation_reader = user_attestation_reader
        self.stream_reader = stream_reader
        self.independent_authority_reader = independent_authority_reader
        self.independent_authority_verifier = independent_authority_verifier
        self.allow_inprocess_test_evidence = bool(allow_inprocess_test_evidence)
        self.clock = clock

    def status(self) -> dict[str, Any]:
        independent_ready = bool(
            callable(self.independent_authority_reader)
            and callable(
                getattr(
                    self.independent_authority_verifier,
                    "verify_snapshot",
                    None,
                )
            )
        )
        local_test_ready = bool(
            self.allow_inprocess_test_evidence
            and all(
                callable(value)
                for value in (
                    self.official_get_reader,
                    self.local_process_bot_audit_reader,
                    self.stream_reader,
                )
            )
        )
        callbacks_ready = bool(
            callable(self.contract_reader)
            and callable(self.user_attestation_reader)
            and (independent_ready or local_test_ready)
        )
        released = bool(
            supervised_release.SUPERVISED_NON_PROMOTION_RELEASED
            and supervised_release.SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED
        )
        return {
            "ready": callbacks_ready and released,
            "assuranceMode": ASSURANCE_MODE,
            "callbacksReady": callbacks_ready,
            "independentObserverReady": independent_ready,
            "inprocessTestEvidence": local_test_ready,
            "released": supervised_release.SUPERVISED_NON_PROMOTION_RELEASED,
            "oneUseNetworkCapabilityReleased": (
                supervised_release.SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED
            ),
            "strictIndependentProof": False,
            "formalExternalWorm": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }

    def verify_and_record(
        self,
        *,
        phase: str,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        credential_fingerprint: str,
        boundary_id: str,
        boundary_hash: str,
        coverage_started_epoch: float,
        require_causal_closure: bool = False,
    ) -> dict[str, Any]:
        normalized_phase = _text(phase).upper()
        session = _text(session_id)
        permit = _text(permit_id)
        permit_digest = _text(permit_hash).lower()
        credential = _text(credential_fingerprint).lower()
        boundary = _text(boundary_id)
        boundary_digest = _text(boundary_hash).lower()
        if (
            self.status()["ready"] is not True
            or normalized_phase not in _PHASES
            or _ID_RE.fullmatch(session) is None
            or _ID_RE.fullmatch(permit) is None
            or _ID_RE.fullmatch(boundary) is None
            or any(
                not _is_hash(value)
                for value in (permit_digest, credential, boundary_digest)
            )
            or require_causal_closure
        ):
            raise BinanceSpotExclusivityError(
                "supervised non-promotion exclusivity context is unavailable"
            )
        now = float(self.clock())
        request = {
            "assuranceMode": ASSURANCE_MODE,
            "phase": normalized_phase,
            "sessionId": session,
            "permitId": permit,
            "permitHash": permit_digest,
            "credentialFingerprint": credential,
            "boundaryId": boundary,
            "boundaryHash": boundary_digest,
            "coverageStartedEpoch": float(coverage_started_epoch),
            "requestedEpoch": now,
        }
        try:
            contract = dict(self.contract_reader(**request))  # type: ignore[misc]
            approved_epoch = float(contract["operatorApproval"]["approvedEpoch"])
            # Fresh user approval is mandatory at the opening boundary.  The
            # same immutable contract may then be reverified historically for
            # the full 7200-second run.
            contract_clock = now if normalized_phase == "BASELINE" else approved_epoch
            supervised_release.validate_supervised_non_promotion_contract(
                contract, clock=lambda: contract_clock
            )
            independent_snapshot: dict[str, Any] = {}
            independent_ready = bool(
                callable(self.independent_authority_reader)
                and callable(
                    getattr(
                        self.independent_authority_verifier,
                        "verify_snapshot",
                        None,
                    )
                )
            )
            if independent_ready:
                owner_prefix = (
                    "ftb-"
                    + hashlib.sha256(session.encode("utf-8")).hexdigest()[:12]
                    + "-"
                )
                raw_snapshot = self.independent_authority_reader(  # type: ignore[misc]
                    **request
                )
                independent_snapshot = dict(
                    self.independent_authority_verifier.verify_snapshot(
                        raw_snapshot,
                        session_id=session,
                        permit_id=permit,
                        permit_hash=permit_digest,
                        credential_fingerprint=credential,
                        owner_client_order_prefix=owner_prefix,
                        coverage_started_epoch=float(coverage_started_epoch),
                        now_epoch=now,
                    )
                )
                official = dict(independent_snapshot["officialBaseline"])
                local_audit = dict(independent_snapshot["processAudit"])
                stream = dict(independent_snapshot["userDataStreamAudit"])
            elif self.allow_inprocess_test_evidence:
                official = _validate_official_get_evidence(
                    self.official_get_reader(**request), now=now  # type: ignore[misc]
                )
                local_audit = _validate_local_audit(
                    self.local_process_bot_audit_reader(**request),  # type: ignore[misc]
                    now=now,
                )
                stream = _validate_stream(
                    self.stream_reader(),  # type: ignore[misc]
                    phase=normalized_phase,
                    session_id=session,
                    permit_id=permit,
                    permit_hash=permit_digest,
                    coverage_started_epoch=float(coverage_started_epoch),
                    now=now,
                )
            else:
                raise BinanceSpotExclusivityError(
                    "independent supervised observer is not wired"
                )
            user_attestation = _validate_user_attestation(
                self.user_attestation_reader(**request),  # type: ignore[misc]
                contract=contract,
                session_id=session,
                permit_id=permit,
                permit_hash=permit_digest,
                now=now,
            )
        except BinanceSpotExclusivityError:
            raise
        except Exception as exc:
            raise BinanceSpotExclusivityError(
                "supervised non-promotion evidence reader failed closed"
            ) from exc
        if (
            contract.get("mode") != ASSURANCE_MODE
            or contract.get("lane") != "BINANCE_SPOT"
            or contract.get("sessionId") != session
            or contract.get("permitId") != permit
            or contract.get("permitHash") != permit_digest
        ):
            raise BinanceSpotExclusivityError(
                "supervised contract session/permit binding changed"
            )
        body = {
            "schemaVersion": PROOF_SCHEMA_VERSION,
            "assuranceMode": ASSURANCE_MODE,
            "phase": normalized_phase,
            "sessionId": session,
            "permitId": permit,
            "permitHash": permit_digest,
            "credentialFingerprint": credential,
            "boundaryId": boundary,
            "boundaryHash": boundary_digest,
            "coverageStartedEpoch": float(coverage_started_epoch),
            "observedEpoch": now,
            "supervisedContract": contract,
            "officialGetEvidence": official,
            "localProcessBotAudit": local_audit,
            "userAttestation": user_attestation,
            "continuousUserDataStream": stream,
            "independentObserverSnapshot": independent_snapshot,
            "manualOrderCausalAuditIndependentlyVerified": bool(
                independent_snapshot.get(
                    "manualOrderCausalAuditIndependentlyVerified"
                )
                is True
            ),
            "botRegistryIndependentlyVerified": bool(
                independent_snapshot.get("botRegistryIndependentlyVerified")
                is True
            ),
            "otherApiKeyInventoryProven": False,
            "strictIndependentProof": False,
            "formalExternalWorm": False,
            "independentCausalClosureProven": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }
        proof = {**body, "proofHash": _hash(body)}
        # The durable store hashes the complete document, including the inner
        # proofHash, to match the existing service phase-chain contract.
        durable = self.store.record(proof, now_epoch=now)
        return {
            "verified": True,
            "assuranceMode": ASSURANCE_MODE,
            "phase": normalized_phase,
            "sessionId": session,
            "boundaryId": boundary,
            "proofHash": _text(durable["proof_hash"]),
            "proof": proof,
            "durable": True,
            "restartVerifiable": True,
            "supervisedControlsVerified": True,
            "supervisedNoManualTradingAttested": True,
            "supervisedNoOtherBotsAttested": True,
            "supervisedAccountWideOpenOrdersZero": True,
            "exclusiveAccountConfirmed": False,
            "noManualTradingConfirmed": bool(
                independent_snapshot.get(
                    "manualOrderCausalAuditIndependentlyVerified"
                )
                is True
            ),
            "noBotsConfirmed": bool(
                independent_snapshot.get("botRegistryIndependentlyVerified")
                is True
            ),
            "noOtherApiKeysConfirmed": False,
            "accountWideCausalClosureProven": False,
            "strictIndependentProof": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }

    def assert_continuous_health(
        self,
        *,
        session_id: str,
        permit_id: str,
        permit_hash: str,
        credential_fingerprint: str,
        coverage_started_epoch: float,
        purpose: str = "CONTINUOUS_HEALTH",
    ) -> dict[str, Any]:
        """Revalidate the independent observer without creating a phase proof.

        Heartbeats and the final non-cleanup mutation fence use this path.  It
        intentionally accepts no in-process evidence fallback: a production
        supervised run loses entry authority as soon as the independently
        signed snapshot is stale, revoked, disconnected, gap-marked, crashed,
        or reports an unowned order event.  Cleanup remains a separate lane.
        """

        session = _text(session_id)
        permit = _text(permit_id)
        permit_digest = _text(permit_hash).lower()
        credential = _text(credential_fingerprint).lower()
        purpose_value = _text(purpose).upper()
        verifier = self.independent_authority_verifier
        reader = self.independent_authority_reader
        if (
            self.status()["ready"] is not True
            or not callable(reader)
            or not callable(getattr(verifier, "verify_snapshot", None))
            or _ID_RE.fullmatch(session) is None
            or _ID_RE.fullmatch(permit) is None
            or not _is_hash(permit_digest)
            or not _is_hash(credential)
            or purpose_value
            not in {"CONTINUOUS_HEALTH", "MUTATION_FINAL_PRE_MARKER"}
        ):
            raise BinanceSpotExclusivityError(
                "independent supervised observer health is unavailable"
            )
        try:
            coverage = float(coverage_started_epoch)
            now = float(self.clock())
            request = {
                "assuranceMode": ASSURANCE_MODE,
                "purpose": purpose_value,
                "sessionId": session,
                "permitId": permit,
                "permitHash": permit_digest,
                "credentialFingerprint": credential,
                "coverageStartedEpoch": coverage,
                "requestedEpoch": now,
            }
            raw_snapshot = reader(**request)
            snapshot = dict(
                verifier.verify_snapshot(
                    raw_snapshot,
                    session_id=session,
                    permit_id=permit,
                    permit_hash=permit_digest,
                    credential_fingerprint=credential,
                    owner_client_order_prefix=(
                        "ftb-"
                        + hashlib.sha256(session.encode("utf-8")).hexdigest()[:12]
                        + "-"
                    ),
                    coverage_started_epoch=coverage,
                    now_epoch=now,
                )
            )
        except BinanceSpotExclusivityError:
            raise
        except Exception as exc:
            raise BinanceSpotExclusivityError(
                "independent supervised observer health read failed closed"
            ) from exc
        return {
            "healthy": True,
            "assuranceMode": ASSURANCE_MODE,
            "purpose": purpose_value,
            "sessionId": session,
            "permitId": permit,
            "permitHash": permit_digest,
            "credentialFingerprint": credential,
            "authorityId": _text(snapshot.get("authorityId")),
            "authoritySequence": int(snapshot.get("authoritySequence") or 0),
            "observedEpoch": float(snapshot.get("observedEpoch") or 0),
            "payloadHash": _text(snapshot.get("payloadHash")).lower(),
            "manualOrderCausalAuditIndependentlyVerified": True,
            "botRegistryIndependentlyVerified": True,
            "otherApiKeyInventoryProven": False,
            "accountWideCausalClosureProven": False,
            "promotionEligible": False,
            "realE2EEligible": False,
            "productionPromotionAllowed": False,
        }

    def session_records(self, session_id: str) -> list[dict[str, Any]]:
        return self.store.session_records(session_id)


__all__ = [
    "API_RESTRICTIONS_ENDPOINT",
    "API_TRADING_STATUS_ENDPOINT",
    "ASSURANCE_MODE",
    "BinanceSpotSupervisedExclusivityGuard",
    "BinanceSpotSupervisedOfficialGetProvider",
    "DurableBinanceSpotSupervisedExclusivityStore",
    "LOCAL_AUDIT_SCHEMA_VERSION",
    "OFFICIAL_EVIDENCE_SCHEMA_VERSION",
    "PROOF_SCHEMA_VERSION",
    "USER_ATTESTATION_SCHEMA_VERSION",
]
