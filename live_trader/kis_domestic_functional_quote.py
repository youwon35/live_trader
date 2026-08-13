from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo


KIS_DOMESTIC_FUNCTIONAL_QUOTE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_QUOTE_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_QUOTE_ORDER_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_QUOTE_PROMOTION_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
QUOTE_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-price"
QUOTE_TR_ID = "FHKST01010100"
MAX_ORDER_KRW = Decimal("100000")
OWNER_LOSS_LIMIT_KRW = Decimal("5000")
MAX_LOCAL_AGE_SECONDS = Decimal("5")
MAX_NEXT_OPEN_AGE_SECONDS = Decimal("2")
MAX_SPREAD_BPS = Decimal("100")
ENTRY_FEE_RESERVE_BPS = Decimal("20")
EXIT_FEE_RESERVE_BPS = Decimal("20")
SELL_TAX_RESERVE_BPS = Decimal("20")

_KST = ZoneInfo("Asia/Seoul")
_SHA256 = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_TRIGGER_ID = re.compile(r"^kis-trigger-[0-9a-f]{32}$", flags=re.ASCII)
_EVALUATION_ID = re.compile(r"^kis-eval-[0-9a-f]{32}$", flags=re.ASCII)
_SOURCE_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", flags=re.ASCII)
_SEQUENCE = re.compile(r"^[0-9]{1,20}$", flags=re.ASCII)
_CAPTURE_DOMAIN = b"kis-domestic-functional-capture/v1\x00"
_RECORD_DOMAIN = b"kis-domestic-functional-quote-record/v1\x00"
_LANE_RECORD_DOMAIN = b"kis-domestic-functional-lane-record/v1\x00"
_NEXT_OPEN_DOMAIN = b"kis-domestic-functional-next-open/v1\x00"
_SCHEMA_VERSION = "kis-domestic-functional-quote-schema/v1"

_QUOTE_KEYS = {
    "schemaVersion", "method", "origin", "endpoint", "trId", "query",
    "publicRequestHeaders", "accountFingerprint", "credentialConfigurationHash",
    "observedAt", "elapsedSeconds", "body", "bodyHash", "priceKrw", "quantity",
    "notionalKrw", "orderCapSatisfied", "durableCasPersisted", "quoteHash",
    "serverAuthoritySignature",
}
_AUDIT_KEYS = {
    "schemaVersion", "origin", "accountFingerprint", "credentialConfigurationHash",
    "serverAuthorityKeyIdHash", "serverAuthorityRestartVerifiable",
    "authenticationTokenReadCount", "oauthTokenIssuanceMayUsePost",
    "authenticationOauthPostDispatchCount", "authenticationOauthPostCountComplete",
    "authenticationOauthPostAuthOnly", "authenticationOauthHiddenRetryCount",
    "authenticationOauthRedirectFollowCount", "officialGetDispatchCount",
    "physicalOfficialGetAttemptCount", "physicalOfficialGetAttemptCountComplete",
    "hiddenGetRetryCount", "redirectFollowCount", "tradingPostDeleteDispatchCount",
    "minimumRequestIntervalSeconds", "pacingWaitSeconds", "dispatches", "signatureHash",
}
_DISPATCH_KEYS = {
    "ordinal", "monotonicStartedAt", "endpoint", "trId", "continuation",
    "accountFingerprint", "queryHmacSha256", "method", "bodyAbsent",
    "physicalAttemptCount", "physicalAttemptCountComplete", "effectiveUrlExact",
    "redirectFollowed", "transportOutcome", "statusCode",
}
_RAW_TRIGGER_KEYS = {
    "schemaVersion", "route", "pdno", "source", "eventType", "evaluationId",
    "barOpenAt", "observedAt", "openPriceKrw", "sourceProvider", "sourceGeneration",
    "sourceSequence", "rawEventHash", "sourceProofHash",
}
_LANE_RECORD_KEYS = _RAW_TRIGGER_KEYS | {
    "triggerId", "evaluationHash", "publicArmId", "publicArmHash", "publicDataOnly",
    "accountAuthorityAvailable", "orderAuthorityAvailable", "contractEnvelopeHash",
    "codeManifestHash", "rawTriggerHash", "rawTriggerSignature", "promotionEligible",
}
_ROLLING_RECEIPT_KEYS = {
    "schemaVersion", "route", "pdno", "snapshotId", "snapshotHash", "diagnosticHash",
    "captureBundleHash", "accountFingerprint", "credentialConfigurationHash",
    "preactivationBaselineHash", "contractEnvelopeHash", "codeManifestHash",
    "publicArmId", "preapprovalHash", "evaluationId", "evaluationHash", "triggerId",
    "triggerHash", "triggerEnvelopeHash", "sourceGeneration", "barOpenAt",
    "completedAt", "expiresAt", "consumedAt", "sessionId", "sessionNonceHash",
    "singleUseConsumed", "privateAccountAuthorityAvailable", "tokenAuthorityAvailable",
    "orderAuthorityAvailable", "networkOrderPostAllowed", "tradingMutationCount",
    "finalQuoteAvailable", "releaseEvidenceEligible",
}
_CAPTURE_BINDING_KEYS = {
    "schemaVersion", "captureId", "quoteHash", "dispatchOrdinal", "queryHmacSha256",
    "auditBeforeHash", "auditAfterHash", "observedAt", "endpoint", "trId",
}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS kis_functional_quote_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    owner_hash TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    credential_configuration_hash TEXT NOT NULL
)
""".strip()
_RECEIPT_SQL = """
CREATE TABLE IF NOT EXISTS kis_functional_quote_receipt (
    receipt_id TEXT PRIMARY KEY,
    trigger_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('ISSUED','CONSUMED')),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    order_authority_fresh INTEGER NOT NULL CHECK(order_authority_fresh IN (0,1)),
    account_fingerprint TEXT NOT NULL,
    credential_configuration_hash TEXT NOT NULL,
    authority_key_id_hash TEXT NOT NULL,
    trigger_hash TEXT NOT NULL,
    quote_hash TEXT NOT NULL,
    rolling_session_id TEXT NOT NULL,
    rolling_session_nonce_hash TEXT NOT NULL,
    rolling_consumed_at TEXT NOT NULL,
    rolling_expires_at TEXT NOT NULL,
    rolling_receipt_hash TEXT NOT NULL,
    record_json TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    signature TEXT NOT NULL,
    created_at TEXT NOT NULL,
    consumed_at TEXT NOT NULL DEFAULT ''
)
""".strip()
_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_kis_functional_quote_consumed_trigger
ON kis_functional_quote_receipt(trigger_id, quote_hash)
""".strip()


class KisDomesticFunctionalQuoteBlocked(RuntimeError):
    """Fail-closed offline quote-authority rejection."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sql_norm(value: str) -> str:
    return " ".join(value.replace("\n", " ").split()).lower()


def _utc(value: object, label: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is not canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is not UTC")
    return parsed


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise KisDomesticFunctionalQuoteBlocked("clock is timezone-naive")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decimal(value: object, label: str, *, positive: bool = False) -> Decimal:
    if type(value) is not str or not value or value != value.strip():
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is not an exact numeric string")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is invalid") from None
    if not parsed.is_finite() or (positive and parsed <= 0) or (not positive and parsed < 0):
        raise KisDomesticFunctionalQuoteBlocked(f"{label} is out of range")
    return parsed


def _reserve(amount: Decimal, bps: Decimal) -> Decimal:
    return (amount * bps / Decimal("10000")).quantize(Decimal("1"), rounding=ROUND_CEILING)


def _capture_signature(key: bytes, value: Mapping[str, Any]) -> str:
    return hmac.new(key, _CAPTURE_DOMAIN + _canonical(value), hashlib.sha256).hexdigest()


def _record_signature(key: bytes, value: Mapping[str, Any]) -> str:
    return hmac.new(key, _RECORD_DOMAIN + _canonical(value), hashlib.sha256).hexdigest()


class DurableKisDomesticFunctionalQuoteStore:
    """Offline, one-shot diagnostic quote receipt. It never sends an HTTP request or order."""

    def __init__(
        self,
        path: str | Path,
        *,
        server_authority_key: bytes,
        server_authority_key_id: str,
        owner_id: str,
        account_fingerprint: str,
        credential_configuration_hash: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(server_authority_key, bytes) or len(server_authority_key) < 32:
            raise KisDomesticFunctionalQuoteBlocked("server authority key is invalid")
        if type(server_authority_key_id) is not str or not server_authority_key_id:
            raise KisDomesticFunctionalQuoteBlocked("durable server authority key id is required")
        if type(owner_id) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", owner_id
        ):
            raise KisDomesticFunctionalQuoteBlocked("quote owner id is invalid")
        if not _SHA256.fullmatch(account_fingerprint or ""):
            raise KisDomesticFunctionalQuoteBlocked("account fingerprint is invalid")
        if not _SHA256.fullmatch(credential_configuration_hash or ""):
            raise KisDomesticFunctionalQuoteBlocked("credential configuration hash is invalid")
        self.path = Path(path)
        self._key = bytes(server_authority_key)
        self._key_id_hash = hashlib.sha256(server_authority_key_id.encode("utf-8")).hexdigest()
        self._owner_hash = hashlib.sha256(owner_id.encode("utf-8")).hexdigest()
        self.account_fingerprint = account_fingerprint
        self.credential_configuration_hash = credential_configuration_hash
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._ensure_schema()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            try:
                conn.execute(_SCHEMA_SQL)
                conn.execute(_RECEIPT_SQL)
                conn.execute(_INDEX_SQL)
            except sqlite3.DatabaseError:
                raise KisDomesticFunctionalQuoteBlocked("quote schema object is dirty") from None
            expected_sql = {
                "kis_functional_quote_schema": _SCHEMA_SQL.replace(" IF NOT EXISTS", ""),
                "kis_functional_quote_receipt": _RECEIPT_SQL.replace(" IF NOT EXISTS", ""),
                "ux_kis_functional_quote_consumed_trigger": _INDEX_SQL.replace(" IF NOT EXISTS", ""),
            }
            actual_objects = {
                str(row[0])
                for row in conn.execute(
                    """SELECT name FROM sqlite_master
                       WHERE name LIKE 'kis_functional_quote_%'
                          OR name='ux_kis_functional_quote_consumed_trigger'
                       ORDER BY name"""
                )
            }
            if actual_objects != set(expected_sql):
                raise KisDomesticFunctionalQuoteBlocked(
                    "quote schema contains missing or extra objects"
                )
            for name, expected in expected_sql.items():
                row = conn.execute("SELECT sql FROM sqlite_master WHERE name=?", (name,)).fetchone()
                if row is None or _sql_norm(str(row["sql"])) != _sql_norm(expected):
                    raise KisDomesticFunctionalQuoteBlocked(f"quote schema object is dirty: {name}")
            schema_hash = _hash({key: _sql_norm(value) for key, value in expected_sql.items()})
            row = conn.execute("SELECT * FROM kis_functional_quote_schema WHERE singleton=1").fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO kis_functional_quote_schema VALUES (1,?,?,?,?,?,?)",
                    (
                        _SCHEMA_VERSION, schema_hash, self._owner_hash,
                        self._key_id_hash, self.account_fingerprint,
                        self.credential_configuration_hash,
                    ),
                )
            elif tuple(row) != (
                1, _SCHEMA_VERSION, schema_hash, self._owner_hash,
                self._key_id_hash, self.account_fingerprint,
                self.credential_configuration_hash,
            ):
                raise KisDomesticFunctionalQuoteBlocked("quote schema fingerprint mismatch")

    def _trusted_now(self) -> datetime:
        now = self._clock()
        if type(now) is not datetime or now.tzinfo is None:
            raise KisDomesticFunctionalQuoteBlocked("trusted clock is not an aware datetime")
        converted = now.astimezone(timezone.utc)
        if not math.isfinite(converted.timestamp()):
            raise KisDomesticFunctionalQuoteBlocked("trusted clock is invalid")
        return converted

    def _verify_signature(self, value: Mapping[str, Any], signature: object) -> None:
        if type(signature) is not str or not _SHA256.fullmatch(signature):
            raise KisDomesticFunctionalQuoteBlocked("capture signature is invalid")
        expected = _capture_signature(self._key, value)
        if not hmac.compare_digest(signature, expected):
            raise KisDomesticFunctionalQuoteBlocked("capture signature mismatch")

    def _verify_quote(self, quote: Mapping[str, Any]) -> tuple[dict[str, Any], Mapping[str, Any], datetime]:
        if set(quote) != _QUOTE_KEYS:
            raise KisDomesticFunctionalQuoteBlocked("quote envelope fields are not exact")
        signed = dict(quote)
        signature = signed.pop("serverAuthoritySignature")
        self._verify_signature(signed, signature)
        quote_hash = signed.pop("quoteHash")
        if type(quote_hash) is not str or not _SHA256.fullmatch(quote_hash) or not hmac.compare_digest(quote_hash, _hash(signed)):
            raise KisDomesticFunctionalQuoteBlocked("quote hash mismatch")
        exact = {
            "schemaVersion": "kis-domestic-functional-quote-preflight/v1",
            "method": "GET", "origin": LIVE_ORIGIN, "endpoint": QUOTE_ENDPOINT,
            "trId": QUOTE_TR_ID,
            "query": {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": PDNO},
            "publicRequestHeaders": {"custtype": "P", "tr_id": QUOTE_TR_ID, "tr_cont": ""},
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "quantity": 1, "orderCapSatisfied": True, "durableCasPersisted": False,
        }
        for key, expected in exact.items():
            if type(signed.get(key)) is not type(expected) or signed.get(key) != expected:
                raise KisDomesticFunctionalQuoteBlocked(f"quote {key} mismatch")
        body = signed.get("body")
        if not isinstance(body, Mapping) or body.get("rt_cd") != "0" or not isinstance(body.get("output"), Mapping):
            raise KisDomesticFunctionalQuoteBlocked("quote response schema is invalid")
        if signed.get("bodyHash") != _hash(body):
            raise KisDomesticFunctionalQuoteBlocked("quote body hash mismatch")
        price = _decimal(signed.get("priceKrw"), "quote.priceKrw", positive=True)
        if _decimal(signed.get("notionalKrw"), "quote.notionalKrw", positive=True) != price:
            raise KisDomesticFunctionalQuoteBlocked("quote notional mismatch")
        if _decimal(body["output"].get("stck_prpr"), "quote.output.stck_prpr", positive=True) != price:
            raise KisDomesticFunctionalQuoteBlocked("quote raw price mismatch")
        observed = _utc(signed.get("observedAt"), "quote.observedAt")
        _decimal(signed.get("elapsedSeconds"), "quote.elapsedSeconds")
        return dict(quote), body["output"], observed

    def _verify_audit(self, audit: Mapping[str, Any]) -> dict[str, Any]:
        if set(audit) != _AUDIT_KEYS:
            raise KisDomesticFunctionalQuoteBlocked("GET audit fields are not exact")
        signed = dict(audit)
        signature = signed.pop("signatureHash")
        self._verify_signature(signed, signature)
        exact = {
            "schemaVersion": "kis-domestic-functional-get-audit/v1",
            "origin": LIVE_ORIGIN,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "serverAuthorityKeyIdHash": self._key_id_hash,
            "serverAuthorityRestartVerifiable": True,
            "physicalOfficialGetAttemptCountComplete": True,
            "hiddenGetRetryCount": 0,
            "redirectFollowCount": 0,
            "tradingPostDeleteDispatchCount": 0,
        }
        for key, expected in exact.items():
            if type(signed.get(key)) is not type(expected) or signed.get(key) != expected:
                raise KisDomesticFunctionalQuoteBlocked(f"GET audit {key} mismatch")
        for key in (
            "serverAuthorityRestartVerifiable", "authenticationOauthPostCountComplete",
            "authenticationOauthPostAuthOnly", "physicalOfficialGetAttemptCountComplete",
        ):
            if signed.get(key) is not True:
                raise KisDomesticFunctionalQuoteBlocked(f"GET audit {key} mismatch")
        for key in (
            "authenticationOauthHiddenRetryCount", "authenticationOauthRedirectFollowCount",
            "hiddenGetRetryCount", "redirectFollowCount", "tradingPostDeleteDispatchCount",
        ):
            if type(signed.get(key)) is not int or signed.get(key) != 0:
                raise KisDomesticFunctionalQuoteBlocked(f"GET audit {key} mismatch")
        for key in (
            "authenticationTokenReadCount", "authenticationOauthPostDispatchCount",
            "officialGetDispatchCount", "physicalOfficialGetAttemptCount",
        ):
            if type(signed.get(key)) is not int or signed.get(key) < 0:
                raise KisDomesticFunctionalQuoteBlocked(f"GET audit {key} is invalid")
        interval = signed.get("minimumRequestIntervalSeconds")
        if (
            type(interval) not in (int, float)
            or isinstance(interval, bool)
            or not math.isfinite(float(interval))
            or Decimal(str(interval)) != Decimal("2.1")
        ):
            raise KisDomesticFunctionalQuoteBlocked("GET audit pacing interval is invalid")
        wait = signed.get("pacingWaitSeconds")
        if type(wait) not in (int, float) or isinstance(wait, bool) or not math.isfinite(float(wait)) or float(wait) < 0:
            raise KisDomesticFunctionalQuoteBlocked("GET audit pacing wait is invalid")
        dispatches = signed.get("dispatches")
        if type(dispatches) is not list or len(dispatches) != signed["officialGetDispatchCount"]:
            raise KisDomesticFunctionalQuoteBlocked("GET audit dispatch count mismatch")
        for ordinal, row in enumerate(dispatches, start=1):
            if not isinstance(row, Mapping) or set(row) not in (_DISPATCH_KEYS, _DISPATCH_KEYS - {"statusCode"}):
                raise KisDomesticFunctionalQuoteBlocked("GET audit dispatch schema is not exact")
            if type(row.get("ordinal")) is not int or row.get("ordinal") != ordinal:
                raise KisDomesticFunctionalQuoteBlocked("GET audit dispatch ordinal mismatch")
            started = row.get("monotonicStartedAt")
            if (
                type(started) not in (int, float)
                or isinstance(started, bool)
                or not math.isfinite(float(started))
                or float(started) < 0
            ):
                raise KisDomesticFunctionalQuoteBlocked(
                    "GET audit dispatch monotonic time is invalid"
                )
            if ordinal > 1:
                previous_started = dispatches[ordinal - 2]["monotonicStartedAt"]
                if Decimal(str(started)) - Decimal(str(previous_started)) < Decimal("2.1"):
                    raise KisDomesticFunctionalQuoteBlocked(
                        "GET audit physical pacing interval is too short"
                    )
        if signed["physicalOfficialGetAttemptCount"] != signed["officialGetDispatchCount"]:
            raise KisDomesticFunctionalQuoteBlocked("GET physical attempt accounting mismatch")
        return signed

    def _verify_trigger_authority(
        self,
        lane_envelope: Mapping[str, Any],
        rolling_envelope: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], datetime, datetime]:
        if set(lane_envelope) != {"body", "recordHash", "signature"}:
            raise KisDomesticFunctionalQuoteBlocked("lane trigger envelope is not exact")
        lane = lane_envelope.get("body")
        if not isinstance(lane, Mapping) or set(lane) != _LANE_RECORD_KEYS:
            raise KisDomesticFunctionalQuoteBlocked("lane trigger record fields are not exact")
        lane = dict(lane)
        lane_hash = lane_envelope.get("recordHash")
        lane_signature = lane_envelope.get("signature")
        if (
            type(lane_hash) is not str or not _SHA256.fullmatch(lane_hash)
            or not hmac.compare_digest(lane_hash, _hash(lane))
            or type(lane_signature) is not str or not _SHA256.fullmatch(lane_signature)
            or not hmac.compare_digest(
                lane_signature,
                hmac.new(self._key, _LANE_RECORD_DOMAIN + _canonical(lane), hashlib.sha256).hexdigest(),
            )
        ):
            raise KisDomesticFunctionalQuoteBlocked("lane trigger record signature mismatch")
        raw = {key: lane[key] for key in _RAW_TRIGGER_KEYS}
        if (
            lane.get("rawTriggerHash") != _hash(raw)
            or type(lane.get("rawTriggerSignature")) is not str
            or not hmac.compare_digest(
                lane["rawTriggerSignature"],
                hmac.new(self._key, _NEXT_OPEN_DOMAIN + _canonical(raw), hashlib.sha256).hexdigest(),
            )
        ):
            raise KisDomesticFunctionalQuoteBlocked("source NEXT_OPEN signature mismatch")
        exact = {
            "schemaVersion": "kis-domestic-next-open-trigger/v1", "route": ROUTE,
            "pdno": PDNO, "source": "KIS_WEBSOCKET", "eventType": "NEXT_BAR_OPEN",
            "sourceProvider": "kis", "publicDataOnly": True,
            "accountAuthorityAvailable": False, "orderAuthorityAvailable": False,
            "promotionEligible": False,
        }
        for key, expected in exact.items():
            if type(lane.get(key)) is not type(expected) or lane.get(key) != expected:
                raise KisDomesticFunctionalQuoteBlocked(f"lane trigger {key} mismatch")
        if not _TRIGGER_ID.fullmatch(str(lane.get("triggerId") or "")) or not _EVALUATION_ID.fullmatch(str(lane.get("evaluationId") or "")):
            raise KisDomesticFunctionalQuoteBlocked("lane trigger identity is invalid")
        if type(lane.get("publicArmId")) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", lane["publicArmId"]
        ):
            raise KisDomesticFunctionalQuoteBlocked("lane public arm identity is invalid")
        _decimal(lane.get("openPriceKrw"), "lane trigger openPriceKrw", positive=True)
        for key in (
            "evaluationHash", "publicArmHash", "contractEnvelopeHash", "codeManifestHash",
            "rawEventHash", "sourceProofHash",
        ):
            if type(lane.get(key)) is not str or not _SHA256.fullmatch(lane[key]):
                raise KisDomesticFunctionalQuoteBlocked(f"lane trigger {key} is invalid")
        if not _SOURCE_GENERATION.fullmatch(str(lane.get("sourceGeneration") or "")) or not _SEQUENCE.fullmatch(str(lane.get("sourceSequence") or "")):
            raise KisDomesticFunctionalQuoteBlocked("lane trigger source lineage is invalid")
        bar_open = _utc(lane.get("barOpenAt"), "trigger.barOpenAt")
        observed = _utc(lane.get("observedAt"), "trigger.observedAt")
        if observed < bar_open or observed > bar_open + timedelta(seconds=2):
            raise KisDomesticFunctionalQuoteBlocked("trigger missed next-open boundary")
        proof = {
            "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1", "route": ROUTE,
            "pdno": PDNO, "sourceProvider": "kis", "sourceGeneration": lane["sourceGeneration"],
            "sourceSequence": lane["sourceSequence"], "rawEventHash": lane["rawEventHash"],
            "barOpenAt": lane["barOpenAt"], "observedAt": lane["observedAt"],
        }
        if not hmac.compare_digest(lane["sourceProofHash"], _hash(proof)):
            raise KisDomesticFunctionalQuoteBlocked("trigger source proof mismatch")

        if set(rolling_envelope) != {"body", "receiptHash", "serverAuthoritySignature"}:
            raise KisDomesticFunctionalQuoteBlocked("rolling receipt envelope is not exact")
        rolling = rolling_envelope.get("body")
        if not isinstance(rolling, Mapping) or set(rolling) != _ROLLING_RECEIPT_KEYS:
            raise KisDomesticFunctionalQuoteBlocked("rolling receipt fields are not exact")
        rolling = dict(rolling)
        receipt_hash = rolling_envelope.get("receiptHash")
        signature = rolling_envelope.get("serverAuthoritySignature")
        if (
            type(receipt_hash) is not str or not _SHA256.fullmatch(receipt_hash)
            or not hmac.compare_digest(receipt_hash, _hash(rolling))
            or type(signature) is not str or not _SHA256.fullmatch(signature)
            or not hmac.compare_digest(
                signature,
                hmac.new(
                    self._key,
                    b"ROLLING_PREFLIGHT_RECEIPT\n" + _canonical(rolling),
                    hashlib.sha256,
                ).hexdigest(),
            )
        ):
            raise KisDomesticFunctionalQuoteBlocked("rolling receipt signature mismatch")
        joins = {
            "schemaVersion": "kis-domestic-rolling-preflight-consumption/v1",
            "route": ROUTE, "pdno": PDNO, "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "evaluationId": lane["evaluationId"], "evaluationHash": lane["evaluationHash"],
            "triggerId": lane["triggerId"], "triggerHash": lane_hash,
            "sourceGeneration": lane["sourceGeneration"], "barOpenAt": lane["barOpenAt"],
            "contractEnvelopeHash": lane["contractEnvelopeHash"],
            "codeManifestHash": lane["codeManifestHash"], "publicArmId": lane["publicArmId"],
            "singleUseConsumed": True, "privateAccountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False, "orderAuthorityAvailable": False,
            "networkOrderPostAllowed": False, "tradingMutationCount": 0,
            "finalQuoteAvailable": False, "releaseEvidenceEligible": False,
        }
        for key, expected in joins.items():
            if type(rolling.get(key)) is not type(expected) or rolling.get(key) != expected:
                raise KisDomesticFunctionalQuoteBlocked(f"rolling/lane {key} mismatch")
        for key in (
            "snapshotHash", "diagnosticHash", "captureBundleHash", "preactivationBaselineHash",
            "preapprovalHash", "triggerEnvelopeHash", "sessionNonceHash",
        ):
            if type(rolling.get(key)) is not str or not _SHA256.fullmatch(rolling[key]):
                raise KisDomesticFunctionalQuoteBlocked(f"rolling {key} is invalid")
        if type(rolling.get("sessionId")) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", rolling["sessionId"]
        ):
            raise KisDomesticFunctionalQuoteBlocked("rolling sessionId is invalid")
        for key in ("completedAt", "expiresAt", "consumedAt"):
            _utc(rolling.get(key), f"rolling.{key}")
        completed = _utc(rolling["completedAt"], "rolling.completedAt")
        expires = _utc(rolling["expiresAt"], "rolling.expiresAt")
        consumed = _utc(rolling["consumedAt"], "rolling.consumedAt")
        if not (
            completed <= bar_open <= expires
            and observed <= consumed <= observed + timedelta(seconds=2)
            and consumed <= expires
        ):
            raise KisDomesticFunctionalQuoteBlocked(
                "rolling receipt time lineage is not bound to next-open"
            )
        return lane, rolling, bar_open, observed

    def issue(
        self,
        *,
        lane_trigger_envelope: Mapping[str, Any],
        rolling_receipt_envelope: Mapping[str, Any],
        signed_quote_capture: Mapping[str, Any],
        signed_client_audit_before: Mapping[str, Any],
        signed_client_audit_after: Mapping[str, Any],
        signed_capture_binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        trigger, rolling, bar_open, trigger_observed = self._verify_trigger_authority(
            lane_trigger_envelope, rolling_receipt_envelope
        )
        quote, output, quote_observed = self._verify_quote(signed_quote_capture)
        before = self._verify_audit(signed_client_audit_before)
        after = self._verify_audit(signed_client_audit_after)
        if set(signed_capture_binding) != {
            "body", "bindingHash", "serverAuthoritySignature"
        }:
            raise KisDomesticFunctionalQuoteBlocked("capture binding envelope is not exact")
        binding = signed_capture_binding.get("body")
        if not isinstance(binding, Mapping) or set(binding) != _CAPTURE_BINDING_KEYS:
            raise KisDomesticFunctionalQuoteBlocked("capture binding fields are not exact")
        binding = dict(binding)
        binding_hash = signed_capture_binding.get("bindingHash")
        binding_signature = signed_capture_binding.get("serverAuthoritySignature")
        if (
            type(binding_hash) is not str
            or not _SHA256.fullmatch(binding_hash)
            or not hmac.compare_digest(binding_hash, _hash(binding))
            or type(binding_signature) is not str
            or not _SHA256.fullmatch(binding_signature)
            or not hmac.compare_digest(
                binding_signature,
                _capture_signature(self._key, {**binding, "bindingHash": binding_hash}),
            )
        ):
            raise KisDomesticFunctionalQuoteBlocked("capture binding signature mismatch")
        expected_ordinal = before["officialGetDispatchCount"] + 1
        if (
            type(binding.get("captureId")) is not str
            or not re.fullmatch(r"kis-quote-capture-[0-9a-f]{32}", binding["captureId"])
            or binding.get("schemaVersion")
            != "kis-domestic-functional-quote-capture-binding/v1"
            or binding.get("quoteHash") != quote["quoteHash"]
            or binding.get("dispatchOrdinal") != expected_ordinal
            or binding.get("auditBeforeHash") != _hash(signed_client_audit_before)
            or binding.get("auditAfterHash") != _hash(signed_client_audit_after)
            or binding.get("observedAt") != quote["observedAt"]
            or binding.get("endpoint") != QUOTE_ENDPOINT
            or binding.get("trId") != QUOTE_TR_ID
        ):
            raise KisDomesticFunctionalQuoteBlocked("capture/audit binding mismatch")
        for key in (
            "authenticationTokenReadCount", "officialGetDispatchCount",
            "physicalOfficialGetAttemptCount",
        ):
            if after[key] != before[key] + 1:
                raise KisDomesticFunctionalQuoteBlocked("quote GET audit delta is not one")
        oauth_delta = (
            after["authenticationOauthPostDispatchCount"]
            - before["authenticationOauthPostDispatchCount"]
        )
        if oauth_delta not in (0, 1):
            raise KisDomesticFunctionalQuoteBlocked("quote OAuth audit delta is invalid")
        if (
            after["dispatches"][:-1] != before["dispatches"]
            or len(after["dispatches"]) != len(before["dispatches"]) + 1
            or after["minimumRequestIntervalSeconds"]
            != before["minimumRequestIntervalSeconds"]
            or after["pacingWaitSeconds"] < before["pacingWaitSeconds"]
        ):
            raise KisDomesticFunctionalQuoteBlocked("quote GET audit chain mismatch")
        dispatch = after["dispatches"][-1]
        expected_query_hmac = _capture_signature(
            self._key,
            {
                "endpoint": QUOTE_ENDPOINT,
                "trId": QUOTE_TR_ID,
                "queryItems": [
                    ["FID_COND_MRKT_DIV_CODE", "J"],
                    ["FID_INPUT_ISCD", PDNO],
                ],
                "continuation": "",
                "accountFingerprint": self.account_fingerprint,
            },
        )
        dispatch_exact = {
            "ordinal": expected_ordinal, "endpoint": QUOTE_ENDPOINT,
            "trId": QUOTE_TR_ID, "continuation": "",
            "accountFingerprint": self.account_fingerprint,
            "queryHmacSha256": expected_query_hmac, "method": "GET",
            "bodyAbsent": True, "physicalAttemptCount": 1,
            "physicalAttemptCountComplete": True, "effectiveUrlExact": True,
            "redirectFollowed": False, "transportOutcome": "RESPONSE", "statusCode": 200,
        }
        for key, expected in dispatch_exact.items():
            if type(dispatch.get(key)) is not type(expected) or dispatch.get(key) != expected:
                raise KisDomesticFunctionalQuoteBlocked(
                    f"quote GET causal dispatch {key} mismatch"
                )
        started = dispatch.get("monotonicStartedAt")
        if (
            type(started) not in (int, float)
            or isinstance(started, bool)
            or not math.isfinite(float(started))
            or float(started) < 0
        ):
            raise KisDomesticFunctionalQuoteBlocked(
                "quote dispatch monotonic time is invalid"
            )
        if binding.get("queryHmacSha256") != expected_query_hmac:
            raise KisDomesticFunctionalQuoteBlocked("capture query HMAC mismatch")
        now = self._trusted_now()
        if now > _utc(rolling["expiresAt"], "rolling.expiresAt"):
            raise KisDomesticFunctionalQuoteBlocked(
                "rolling snapshot expired before quote receipt"
            )
        local_age = Decimal(str((now - quote_observed).total_seconds()))
        local_diagnostic_fresh = Decimal("0") <= local_age <= MAX_LOCAL_AGE_SECONDS
        reasons: list[str] = []
        broker_time: datetime | None = None
        date_raw, time_raw = output.get("stck_bsop_date"), output.get("stck_cntg_hour")
        if type(date_raw) is str and re.fullmatch(r"[0-9]{8}", date_raw) and type(time_raw) is str and re.fullmatch(r"[0-9]{6}", time_raw):
            try:
                broker_time = datetime.strptime(date_raw + time_raw, "%Y%m%d%H%M%S").replace(tzinfo=_KST).astimezone(timezone.utc)
            except ValueError:
                broker_time = None
        if broker_time is None:
            reasons.append("BROKER_TRADE_TIMESTAMP_ABSENT")
        else:
            broker_age = Decimal(str((now - broker_time).total_seconds()))
            if (
                broker_time < bar_open
                or broker_time > quote_observed
                or broker_age < 0
                or broker_age > MAX_LOCAL_AGE_SECONDS
            ):
                reasons.append("BROKER_TRADE_TIMESTAMP_STALE_OR_UNBOUND")
        if not local_diagnostic_fresh:
            reasons.append("LOCAL_OBSERVATION_STALE_DIAGNOSTIC")
        if quote_observed < trigger_observed or quote_observed > trigger_observed + timedelta(seconds=2):
            reasons.append("QUOTE_NOT_BOUND_TO_NEXT_OPEN")

        price = _decimal(quote["priceKrw"], "quote.priceKrw", positive=True)
        policy_values: dict[str, Decimal] = {}
        for field in ("askp1", "bidp1", "askp_rsqn1", "stck_mxpr", "stck_llam"):
            raw = output.get(field)
            if type(raw) is not str:
                reasons.append(f"MISSING_{field.upper()}")
                continue
            try:
                policy_values[field] = _decimal(raw, f"quote.output.{field}", positive=True)
            except KisDomesticFunctionalQuoteBlocked:
                reasons.append(f"INVALID_{field.upper()}")
        ask = policy_values.get("askp1", Decimal("0"))
        bid = policy_values.get("bidp1", Decimal("0"))
        ask_qty = policy_values.get("askp_rsqn1", Decimal("0"))
        upper = policy_values.get("stck_mxpr", Decimal("0"))
        lower = policy_values.get("stck_llam", Decimal("0"))
        if ask and bid and (bid > price or price > ask or ask < bid):
            reasons.append("PRICE_ORDERING_INVALID")
        if ask_qty and ask_qty < 1:
            reasons.append("ASK_LIQUIDITY_INSUFFICIENT")
        spread_bps = ((ask - bid) * Decimal("10000") / ask) if ask and bid else Decimal("Infinity")
        if not spread_bps.is_finite() or spread_bps > MAX_SPREAD_BPS:
            reasons.append("SPREAD_RESERVE_UNBOUNDED")
        if upper and lower and not (lower <= bid <= price <= ask <= upper):
            reasons.append("PRICE_LIMIT_POLICY_FAILED")
        conservative_limit = ask
        entry_fee = _reserve(conservative_limit, ENTRY_FEE_RESERVE_BPS) if ask else Decimal("0")
        exit_fee = _reserve(bid, EXIT_FEE_RESERVE_BPS) if bid else Decimal("0")
        sell_tax = _reserve(bid, SELL_TAX_RESERVE_BPS) if bid else Decimal("0")
        all_cost_reserve = entry_fee + exit_fee + sell_tax
        total_cap = conservative_limit + all_cost_reserve
        owner_loss_reserve = (conservative_limit - bid) + all_cost_reserve if ask and bid else Decimal("Infinity")
        if not total_cap.is_finite() or total_cap > MAX_ORDER_KRW:
            reasons.append("ORDER_AND_COST_RESERVE_EXCEEDS_CAP")
        if not owner_loss_reserve.is_finite() or owner_loss_reserve >= OWNER_LOSS_LIMIT_KRW:
            reasons.append("OWNER_LOSS_RESERVE_NOT_BELOW_LIMIT")
        authority_fresh = not reasons
        receipt_id = f"kis-quote-{uuid.uuid4().hex}"
        record = {
            "schemaVersion": "kis-domestic-functional-quote-receipt/v1",
            "receiptId": receipt_id, "route": ROUTE, "pdno": PDNO,
            "accountFingerprint": self.account_fingerprint,
            "credentialConfigurationHash": self.credential_configuration_hash,
            "authorityKeyIdHash": self._key_id_hash,
            "trigger": {
                "triggerId": trigger["triggerId"],
                "triggerHash": lane_trigger_envelope["recordHash"],
                "evaluationId": trigger["evaluationId"],
                "evaluationHash": trigger["evaluationHash"],
                "sourceGeneration": trigger["sourceGeneration"],
                "sourceProofHash": trigger["sourceProofHash"],
                "barOpenAt": trigger["barOpenAt"],
                "observedAt": trigger["observedAt"],
                "rollingSnapshotId": rolling["snapshotId"],
                "rollingSnapshotHash": rolling["snapshotHash"],
                "rollingReceiptHash": rolling_receipt_envelope["receiptHash"],
                "rollingSessionId": rolling["sessionId"],
                "rollingSessionNonceHash": rolling["sessionNonceHash"],
                "rollingConsumedAt": rolling["consumedAt"],
                "rollingExpiresAt": rolling["expiresAt"],
            },
            "captureId": binding["captureId"],
            "captureBindingHash": binding_hash,
            "quoteHash": quote["quoteHash"],
            "quoteObservedAt": quote["observedAt"],
            "brokerTradeObservedAt": _utc_text(broker_time) if broker_time else "",
            "localObservationDiagnosticFresh": local_diagnostic_fresh,
            "quantity": 1, "currentPriceKrw": format(price, "f"),
            "conservativeLimitKrw": format(conservative_limit, "f"),
            "spreadBps": format(spread_bps, "f") if spread_bps.is_finite() else "UNAVAILABLE",
            "entryFeeReserveKrw": format(entry_fee, "f"),
            "exitFeeReserveKrw": format(exit_fee, "f"),
            "sellTaxReserveKrw": format(sell_tax, "f"),
            "allCostReserveKrw": format(all_cost_reserve, "f"),
            "orderAndCostReserveKrw": format(total_cap, "f") if total_cap.is_finite() else "UNAVAILABLE",
            "ownerLossReserveKrw": format(owner_loss_reserve, "f") if owner_loss_reserve.is_finite() else "UNAVAILABLE",
            "orderAuthorityFresh": authority_fresh,
            "blockedReasons": reasons, "state": "ISSUED", "revision": 1,
            "createdAt": _utc_text(now), "productionAvailable": False,
            "networkAvailable": False, "mutationAvailable": False,
            "promotionEligible": False,
        }
        record_hash = _hash(record)
        signature = _record_signature(self._key, {**record, "recordHash": record_hash})
        with self._connect() as conn:
            try:
                conn.execute(
                    """INSERT INTO kis_functional_quote_receipt
                    (receipt_id,trigger_id,state,revision,order_authority_fresh,
                    account_fingerprint,credential_configuration_hash,authority_key_id_hash,
                     trigger_hash,quote_hash,rolling_session_id,rolling_session_nonce_hash,
                     rolling_consumed_at,rolling_expires_at,rolling_receipt_hash,
                     record_json,record_hash,signature,created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (receipt_id, trigger["triggerId"], "ISSUED", 1, int(authority_fresh),
                     self.account_fingerprint, self.credential_configuration_hash,
                     self._key_id_hash, lane_trigger_envelope["recordHash"], quote["quoteHash"],
                     rolling["sessionId"], rolling["sessionNonceHash"],
                     rolling["consumedAt"], rolling["expiresAt"],
                     rolling_receipt_envelope["receiptHash"],
                     _canonical(record).decode("utf-8"), record_hash, signature, _utc_text(now)),
                )
            except sqlite3.IntegrityError:
                raise KisDomesticFunctionalQuoteBlocked("trigger already has a quote receipt") from None
        return {"body": record, "recordHash": record_hash, "signature": signature}

    def _read_verified(self, receipt_id: str) -> tuple[sqlite3.Row, dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM kis_functional_quote_receipt WHERE receipt_id=?", (receipt_id,)).fetchone()
        if row is None:
            raise KisDomesticFunctionalQuoteBlocked("quote receipt not found")
        try:
            body = json.loads(row["record_json"])
        except (TypeError, json.JSONDecodeError):
            raise KisDomesticFunctionalQuoteBlocked("quote receipt JSON is invalid") from None
        if _hash(body) != row["record_hash"] or not hmac.compare_digest(
            row["signature"], _record_signature(self._key, {**body, "recordHash": row["record_hash"]})
        ):
            raise KisDomesticFunctionalQuoteBlocked("quote receipt integrity mismatch")
        projection = {
            "receipt_id": body.get("receiptId"), "trigger_id": body.get("trigger", {}).get("triggerId"),
            "state": body.get("state"), "revision": body.get("revision"),
            "order_authority_fresh": int(body.get("orderAuthorityFresh") is True),
            "account_fingerprint": body.get("accountFingerprint"),
            "credential_configuration_hash": body.get("credentialConfigurationHash"),
            "authority_key_id_hash": body.get("authorityKeyIdHash"),
            "trigger_hash": body.get("trigger", {}).get("triggerHash"), "quote_hash": body.get("quoteHash"),
            "rolling_session_id": body.get("trigger", {}).get("rollingSessionId"),
            "rolling_session_nonce_hash": body.get("trigger", {}).get("rollingSessionNonceHash"),
            "rolling_consumed_at": body.get("trigger", {}).get("rollingConsumedAt"),
            "rolling_expires_at": body.get("trigger", {}).get("rollingExpiresAt"),
            "rolling_receipt_hash": body.get("trigger", {}).get("rollingReceiptHash"),
        }
        for key, expected in projection.items():
            if row[key] != expected:
                raise KisDomesticFunctionalQuoteBlocked("quote receipt row projection mismatch")
        return row, body

    def consume(self, *, receipt_id: str, trigger_id: str, expected_revision: int) -> dict[str, Any]:
        if type(receipt_id) is not str or not re.fullmatch(
            r"kis-quote-[0-9a-f]{32}", receipt_id
        ):
            raise KisDomesticFunctionalQuoteBlocked("quote receipt id is invalid")
        if type(trigger_id) is not str or not _TRIGGER_ID.fullmatch(trigger_id):
            raise KisDomesticFunctionalQuoteBlocked("quote trigger id is invalid")
        if type(expected_revision) is not int or expected_revision < 1:
            raise KisDomesticFunctionalQuoteBlocked("quote expected revision is invalid")
        row, body = self._read_verified(receipt_id)
        if row["state"] != "ISSUED" or row["revision"] != expected_revision:
            raise KisDomesticFunctionalQuoteBlocked("quote receipt state or revision mismatch")
        if body["trigger"]["triggerId"] != trigger_id:
            raise KisDomesticFunctionalQuoteBlocked("quote receipt trigger mismatch")
        if body["orderAuthorityFresh"] is not True:
            raise KisDomesticFunctionalQuoteBlocked("quote receipt has no order authority")
        now = self._trusted_now()
        if now > _utc(body["trigger"]["rollingExpiresAt"], "rollingExpiresAt"):
            raise KisDomesticFunctionalQuoteBlocked(
                "rolling snapshot expired before quote consumption"
            )
        quote_age = now - _utc(body["quoteObservedAt"], "quoteObservedAt")
        if quote_age < timedelta(0) or quote_age > timedelta(seconds=5):
            raise KisDomesticFunctionalQuoteBlocked("quote receipt expired before consumption")
        consumed = {**body, "state": "CONSUMED", "revision": expected_revision + 1, "consumedAt": _utc_text(now)}
        record_hash = _hash(consumed)
        signature = _record_signature(self._key, {**consumed, "recordHash": record_hash})
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE kis_functional_quote_receipt SET state='CONSUMED', revision=?,
                record_json=?,record_hash=?,signature=?,consumed_at=?
                WHERE receipt_id=? AND state='ISSUED' AND revision=?""",
                (expected_revision + 1, _canonical(consumed).decode("utf-8"), record_hash,
                 signature, consumed["consumedAt"], receipt_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise KisDomesticFunctionalQuoteBlocked("quote receipt consume CAS failed")
        return {"body": consumed, "recordHash": record_hash, "signature": signature}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-quote-status/v1",
        "available": False,
        "networkAvailable": False,
        "orderAuthorityAvailable": False,
        "mutationAvailable": False,
        "promotionAvailable": False,
        "route": ROUTE,
        "pdno": PDNO,
        "reason": "OFFLINE_SIGNED_QUOTE_RECEIPT_ONLY_NO_PRODUCTION_WIRING",
    }
