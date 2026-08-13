from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
import threading
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from .kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    ARMED_LATEST,
    KST,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from trading_runtime.market_calendar import session_bounds_utc


KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ACCOUNT_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_TOKEN_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ORDER_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_RELEASE_AVAILABLE = False

SCHEMA_VERSION = "kis-domestic-functional-rolling-preflight-schema/v1"
DIAGNOSTIC_SCHEMA = "kis-domestic-rolling-full-account-diagnostic/v1"
SNAPSHOT_SCHEMA = "kis-domestic-rolling-preflight-snapshot/v1"
TRIGGER_SCHEMA = "kis-domestic-rolling-preflight-trigger/v1"
RECEIPT_SCHEMA = "kis-domestic-rolling-preflight-consumption/v1"

SNAPSHOT_FRESHNESS_SECONDS = 60
NEXT_OPEN_OBSERVATION_SECONDS = 2
MAX_STABLE_READ_SECONDS = 120
MIN_REQUEST_INTERVAL_SECONDS = Decimal("2.1")

_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_PROCESS_GENERATION = re.compile(
    r"^kis-rolling-generation-[0-9a-f]{32}$", re.ASCII
)
_SOURCE_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@-]{1,160}$", re.ASCII)
_ZERO_HASH = "0" * 64

_ROUTES = (
    ("/uapi/domestic-stock/v1/trading/inquire-balance", "TTTC8434R"),
    ("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "TTTC0081R"),
    ("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", "TTTC0084R"),
    ("/uapi/domestic-stock/v1/trading/inquire-period-trade-profit", "TTTC8715R"),
    ("/uapi/domestic-stock/v1/trading/inquire-period-profit", "TTTC8708R"),
    ("/uapi/domestic-stock/v1/quotations/chk-holiday", "CTCA0903R"),
)
MIN_OFFICIAL_GET_REQUESTS = 2 * len(_ROUTES)

_DIAGNOSTIC_KEYS = {
    "schemaVersion",
    "route",
    "origin",
    "pdno",
    "accountFingerprint",
    "credentialConfigurationHash",
    "artifactContentHash",
    "artifactFileSha256",
    "instanceContentHash",
    "instanceFileSha256",
    "contractEnvelopeHash",
    "codeManifestHash",
    "publicArmId",
    "preapprovalHash",
    "tradingDate",
    "intendedNextOpenAt",
    "startedAt",
    "completedAt",
    "captureBundleHash",
    "preactivationBaselineHash",
    "causalProjectionHash",
    "rawCaptureHashes",
    "captureCount",
    "routePages",
    "officialGetRequestCount",
    "physicalGetAttemptCount",
    "physicalGetAttemptCountComplete",
    "minimumRequestIntervalSeconds",
    "physicalPacingElapsedSeconds",
    "stableReadElapsedSeconds",
    "allGetPaginationComplete",
    "allGetPagesSigned",
    "officialTradingDayOpen",
    "stableRepeatedReads",
    "stableComparison",
    "accountWideWorkingOrdersZero",
    "balanceBaselineComplete",
    "costBaselineComplete",
    "hiddenGetRetryCount",
    "redirectFollowCount",
    "tradingPostDeleteDispatchCount",
    "caps",
    "serverAuthorityKeyIdHash",
    "serverAuthorityRestartVerifiable",
    "trustedDiagnosticResult",
    "durableDiagnosticPersisted",
    "rollingWatcherPrivateAccountAuthorityAvailable",
    "rollingWatcherTokenAuthorityAvailable",
    "rollingWatcherOrderAuthorityAvailable",
    "finalQuoteIncluded",
    "finalQuoteAvailable",
    "finalQuoteAuthoritative",
    "promotionEligible",
    "releaseEvidenceEligible",
}

_TRIGGER_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "snapshotId",
    "publicArmId",
    "preapprovalHash",
    "evaluationId",
    "evaluationHash",
    "triggerId",
    "triggerHash",
    "sourceGeneration",
    "barOpenAt",
    "observedAt",
    "accountFingerprint",
    "credentialConfigurationHash",
    "contractEnvelopeHash",
    "codeManifestHash",
    "sessionId",
    "sessionNonceHash",
    "accountAuthorityAvailable",
    "tokenAuthorityAvailable",
    "orderAuthorityAvailable",
}

_SNAPSHOT_KEYS = {
    "schemaVersion", "route", "origin", "pdno", "snapshotId",
    "diagnosticHash", "captureBundleHash", "accountFingerprint",
    "credentialConfigurationHash", "preactivationBaselineHash",
    "contractEnvelopeHash", "codeManifestHash", "publicArmId",
    "preapprovalHash", "completedAt", "expiresAt", "nextBoundaryAt",
    "processGeneration", "ownerTokenHash", "state", "singleUse",
    "privateAccountAuthorityAvailable", "tokenAuthorityAvailable",
    "orderAuthorityAvailable", "networkOrderPostAllowed",
    "finalQuoteAvailable", "releaseEvidenceEligible",
}


class KisDomesticFunctionalRollingPreflightBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    return value


def _parse_time(value: Any, label: str) -> datetime:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            f"{label}-invalid"
        ) from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    canonical = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if value != canonical:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            f"{label}-not-canonical-utc"
        )
    return parsed.astimezone(timezone.utc)


def _time_text(value: datetime, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    converted = value.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    return converted.isoformat().replace("+00:00", "Z")


def _decimal(value: Any, label: str, *, nonnegative: bool = True) -> Decimal:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            f"{label}-invalid"
        ) from exc
    if not parsed.is_finite() or format(parsed, "f") != value:
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    if nonnegative and parsed < 0:
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-invalid")
    return parsed


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalRollingPreflightBlocked(f"{label}-not-object")
    return value


def _signature(value: Any, label: str) -> str:
    return _sha(value, f"{label}-signature")


@dataclass(frozen=True)
class _Binding:
    account_fingerprint: str
    credential_configuration_hash: str
    contract_envelope_hash: str
    code_manifest_hash: str
    public_arm_id: str
    preapproval_hash: str
    authority_key_id_hash: str


_SCHEMA_SQL = """
CREATE TABLE kis_functional_rolling_preflight_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_functional_rolling_preflight_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN (
        'READY','CONSUMED','REJECTED_STALE','REJECTED_TRIGGER_MISMATCH',
        'INVALIDATED_RESTART')),
    process_generation TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL,
    next_boundary_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    credential_configuration_hash TEXT NOT NULL,
    preactivation_baseline_hash TEXT NOT NULL,
    capture_bundle_hash TEXT NOT NULL,
    contract_envelope_hash TEXT NOT NULL,
    code_manifest_hash TEXT NOT NULL,
    public_arm_id TEXT NOT NULL,
    preapproval_hash TEXT NOT NULL,
    diagnostic_json TEXT NOT NULL,
    diagnostic_hash TEXT NOT NULL UNIQUE,
    diagnostic_signature TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL UNIQUE,
    snapshot_signature TEXT NOT NULL,
    trigger_json TEXT NOT NULL DEFAULT '',
    trigger_envelope_hash TEXT NOT NULL DEFAULT '',
    trigger_signature TEXT NOT NULL DEFAULT '',
    terminal_json TEXT NOT NULL DEFAULT '',
    terminal_hash TEXT NOT NULL DEFAULT '',
    terminal_signature TEXT NOT NULL DEFAULT '',
    consumed_at TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK (revision>=1)
);
CREATE UNIQUE INDEX kis_functional_rolling_preflight_ready_boundary_idx
    ON kis_functional_rolling_preflight_snapshot(next_boundary_at)
    WHERE state='READY';
"""

_SCHEMA_DESCRIPTOR = {
    "tables": {
        "kis_functional_rolling_preflight_meta": [
            "singleton", "schema_version", "schema_fingerprint"
        ],
        "kis_functional_rolling_preflight_snapshot": [
            "snapshot_id", "state", "process_generation", "owner_token_hash",
            "next_boundary_at", "completed_at", "expires_at",
            "account_fingerprint", "credential_configuration_hash",
            "preactivation_baseline_hash", "capture_bundle_hash",
            "contract_envelope_hash", "code_manifest_hash", "public_arm_id",
            "preapproval_hash", "diagnostic_json", "diagnostic_hash",
            "diagnostic_signature", "snapshot_json", "snapshot_hash",
            "snapshot_signature", "trigger_json", "trigger_envelope_hash",
            "trigger_signature", "terminal_json", "terminal_hash",
            "terminal_signature", "consumed_at", "revision",
        ],
    },
    "states": [
        "READY", "CONSUMED", "REJECTED_STALE",
        "REJECTED_TRIGGER_MISMATCH", "INVALIDATED_RESTART",
    ],
    "freshnessSeconds": SNAPSHOT_FRESHNESS_SECONDS,
    "nextOpenObservationSeconds": NEXT_OPEN_OBSERVATION_SECONDS,
    "minimumRequestIntervalSeconds": "2.1",
}
SCHEMA_FINGERPRINT = _hash(
    {
        "descriptor": _SCHEMA_DESCRIPTOR,
        "canonicalSql": " ".join(_SCHEMA_SQL.strip().split()),
    }
)


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _expected_schema() -> tuple[dict[str, str], dict[str, list[tuple[Any, ...]]]]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(_SCHEMA_SQL)
        sql = {
            str(row[0]): _normalize_sql(row[1])
            for row in conn.execute(
                "SELECT name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        }
        columns = {
            table: [
                (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
                for row in conn.execute(f'PRAGMA table_info("{table}")')
            ]
            for table in _SCHEMA_DESCRIPTOR["tables"]
        }
        return sql, columns
    finally:
        conn.close()


_EXPECTED_SQL, _EXPECTED_COLUMNS = _expected_schema()


def _verify_schema(conn: sqlite3.Connection) -> None:
    actual_sql = {
        str(row[0]): _normalize_sql(row[1])
        for row in conn.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    }
    if actual_sql != _EXPECTED_SQL:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-definition-schema-dirty"
        )
    for table, expected in _EXPECTED_COLUMNS.items():
        actual = [
            (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
            for row in conn.execute(f'PRAGMA table_info("{table}")')
        ]
        if actual != expected:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-column-schema-dirty"
            )
    indexes = [
        (str(row[1]), int(row[2]), str(row[3]), int(row[4]))
        for row in conn.execute(
            "PRAGMA index_list('kis_functional_rolling_preflight_snapshot')"
        )
    ]
    expected_indexes = [
        ("kis_functional_rolling_preflight_ready_boundary_idx", 1, "c", 1),
        ("sqlite_autoindex_kis_functional_rolling_preflight_snapshot_3", 1, "u", 0),
        ("sqlite_autoindex_kis_functional_rolling_preflight_snapshot_2", 1, "u", 0),
        ("sqlite_autoindex_kis_functional_rolling_preflight_snapshot_1", 1, "pk", 0),
    ]
    if indexes != expected_indexes:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-index-schema-dirty"
        )
    expected_xinfo = {
        "kis_functional_rolling_preflight_ready_boundary_idx": ["next_boundary_at"],
        "sqlite_autoindex_kis_functional_rolling_preflight_snapshot_3": ["snapshot_hash"],
        "sqlite_autoindex_kis_functional_rolling_preflight_snapshot_2": ["diagnostic_hash"],
        "sqlite_autoindex_kis_functional_rolling_preflight_snapshot_1": ["snapshot_id"],
    }
    for index, expected in expected_xinfo.items():
        actual = [
            str(row[2])
            for row in conn.execute(f'PRAGMA index_xinfo("{index}")')
            if int(row[5]) == 1
        ]
        if actual != expected:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-index-columns-dirty"
            )
    meta = [tuple(row) for row in conn.execute(
        "SELECT singleton,schema_version,schema_fingerprint "
        "FROM kis_functional_rolling_preflight_meta"
    ).fetchall()]
    if meta != [(1, SCHEMA_VERSION, SCHEMA_FINGERPRINT)]:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-schema-fingerprint-mismatch"
        )


def _boundary(value: Any) -> datetime:
    boundary = _parse_time(value, "next-boundary")
    local = boundary.astimezone(KST)
    if (
        local.second != 0
        or local.microsecond != 0
        or local.minute % 5 != 0
        or local.time().replace(tzinfo=None) < time(9, 5)
        or local.time().replace(tzinfo=None) > ARMED_LATEST
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-next-boundary-invalid"
        )
    try:
        session_open, session_close = session_bounds_utc("XKRX", local.date())
    except ValueError as exc:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-boundary-not-official-xkrx-session"
        ) from exc
    if not session_open < boundary < session_close:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-boundary-outside-official-xkrx-session"
        )
    return boundary


def _verify_signed(
    verifier: Callable[[str, Mapping[str, Any], str], bool],
    domain: str,
    body: Mapping[str, Any],
    signature: Any,
    label: str,
) -> str:
    parsed = _signature(signature, label)
    try:
        valid = verifier(domain, body, parsed)
    except BaseException:
        valid = False
    if valid is not True:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            f"{label}-signature-invalid"
        )
    return parsed


def _validate_diagnostic(
    value: Mapping[str, Any],
    *,
    binding: _Binding,
    verifier: Callable[[str, Mapping[str, Any], str], bool],
) -> tuple[dict[str, Any], str, str, datetime, datetime, datetime]:
    envelope = _mapping(value, "diagnostic-envelope")
    if set(envelope) != {"body", "diagnosticHash", "serverAuthoritySignature"}:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-diagnostic-envelope-not-exact"
        )
    body = dict(_mapping(envelope.get("body"), "diagnostic-body"))
    if set(body) != _DIAGNOSTIC_KEYS:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-diagnostic-fields-not-exact"
        )
    diagnostic_hash = _sha(envelope.get("diagnosticHash"), "diagnostic-hash")
    if not hmac.compare_digest(diagnostic_hash, _hash(body)):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-diagnostic-hash-mismatch"
        )
    signature = _verify_signed(
        verifier,
        "ROLLING_PREFLIGHT_DIAGNOSTIC",
        {**body, "diagnosticHash": diagnostic_hash},
        envelope.get("serverAuthoritySignature"),
        "diagnostic",
    )
    exact = {
        "schemaVersion": DIAGNOSTIC_SCHEMA,
        "route": ROUTE,
        "origin": LIVE_ORIGIN,
        "pdno": PDNO,
        "accountFingerprint": binding.account_fingerprint,
        "credentialConfigurationHash": binding.credential_configuration_hash,
        "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
        "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
        "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
        "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
        "contractEnvelopeHash": binding.contract_envelope_hash,
        "codeManifestHash": binding.code_manifest_hash,
        "publicArmId": binding.public_arm_id,
        "preapprovalHash": binding.preapproval_hash,
        "captureCount": 2,
        "physicalGetAttemptCountComplete": True,
        "minimumRequestIntervalSeconds": "2.1",
        "allGetPaginationComplete": True,
        "allGetPagesSigned": True,
        "officialTradingDayOpen": True,
        "stableRepeatedReads": True,
        "stableComparison": "PARSED_CAUSAL_PROJECTION",
        "accountWideWorkingOrdersZero": True,
        "balanceBaselineComplete": True,
        "costBaselineComplete": True,
        "hiddenGetRetryCount": 0,
        "redirectFollowCount": 0,
        "tradingPostDeleteDispatchCount": 0,
        "serverAuthorityKeyIdHash": binding.authority_key_id_hash,
        "serverAuthorityRestartVerifiable": True,
        "trustedDiagnosticResult": True,
        "durableDiagnosticPersisted": True,
        "rollingWatcherPrivateAccountAuthorityAvailable": False,
        "rollingWatcherTokenAuthorityAvailable": False,
        "rollingWatcherOrderAuthorityAvailable": False,
        "finalQuoteIncluded": False,
        "finalQuoteAvailable": False,
        "finalQuoteAuthoritative": False,
        "promotionEligible": False,
        "releaseEvidenceEligible": False,
    }
    if any(
        type(body.get(key)) is not type(expected) or body.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-diagnostic-binding-or-truth-mismatch"
        )
    hashes = (
        "captureBundleHash", "preactivationBaselineHash",
        "causalProjectionHash",
    )
    for field in hashes:
        _sha(body.get(field), field)
    captures = body.get("rawCaptureHashes")
    if (
        not isinstance(captures, list)
        or len(captures) != 2
        or any(type(item) is not str or not _SHA256.fullmatch(item) for item in captures)
        or len(set(captures)) != 2
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-raw-capture-hashes-invalid"
        )
    expected_bundle = _hash(
        {
            "rawCaptureHashes": captures,
            "causalProjectionHash": body["causalProjectionHash"],
            "preactivationBaselineHash": body["preactivationBaselineHash"],
        }
    )
    if not hmac.compare_digest(body["captureBundleHash"], expected_bundle):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-capture-bundle-mismatch"
        )
    caps = _mapping(body.get("caps"), "diagnostic-caps")
    expected_caps = {
        "quantity": ORDER_QUANTITY,
        "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
        "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
        "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
        "activeSeconds": ACTIVE_SECONDS,
    }
    if (
        set(caps) != set(expected_caps)
        or any(
            type(caps.get(key)) is not type(expected)
            or caps.get(key) != expected
            for key, expected in expected_caps.items()
        )
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-caps-mismatch"
        )
    route_pages = body.get("routePages")
    if not isinstance(route_pages, list) or len(route_pages) != len(_ROUTES):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-route-pages-incomplete"
        )
    page_total = 0
    for index, (endpoint, tr_id) in enumerate(_ROUTES):
        item = _mapping(route_pages[index], f"route-page-{index}")
        if (
            set(item) != {
                "endpoint", "trId", "pageCountAcrossTwoCaptures",
                "terminalContinuationObserved", "allPagesSigned",
            }
            or item.get("endpoint") != endpoint
            or item.get("trId") != tr_id
            or type(item.get("pageCountAcrossTwoCaptures")) is not int
            or item["pageCountAcrossTwoCaptures"] < 2
            or item.get("terminalContinuationObserved") is not True
            or item.get("allPagesSigned") is not True
        ):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-route-pagination-invalid"
            )
        page_total += item["pageCountAcrossTwoCaptures"]
    gets = body.get("officialGetRequestCount")
    physical = body.get("physicalGetAttemptCount")
    if (
        type(gets) is not int
        or gets < MIN_OFFICIAL_GET_REQUESTS
        or gets != page_total
        or physical != gets
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-physical-attempt-count-mismatch"
        )
    pacing = _decimal(body.get("physicalPacingElapsedSeconds"), "pacing-elapsed")
    stable = _decimal(body.get("stableReadElapsedSeconds"), "stable-elapsed")
    floor = Decimal(gets - 1) * MIN_REQUEST_INTERVAL_SECONDS
    if pacing < floor or stable < pacing or stable > MAX_STABLE_READ_SECONDS:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-physical-pacing-or-stability-invalid"
        )
    started = _parse_time(body.get("startedAt"), "diagnostic-started-at")
    completed = _parse_time(body.get("completedAt"), "diagnostic-completed-at")
    boundary = _boundary(body.get("intendedNextOpenAt"))
    if started > completed or Decimal(str((completed - started).total_seconds())) != stable:
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-stable-elapsed-timestamp-mismatch"
        )
    if body.get("tradingDate") != boundary.astimezone(KST).date().isoformat():
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-trading-date-mismatch"
        )
    expires = completed + timedelta(seconds=SNAPSHOT_FRESHNESS_SECONDS)
    if (
        not completed < boundary
        or boundary - completed > timedelta(seconds=SNAPSHOT_FRESHNESS_SECONDS)
        or expires < boundary + timedelta(seconds=NEXT_OPEN_OBSERVATION_SECONDS)
    ):
        raise KisDomesticFunctionalRollingPreflightBlocked(
            "rolling-preflight-diagnostic-not-fresh-for-next-open"
        )
    return body, diagnostic_hash, signature, completed, expires, boundary


class DurableKisDomesticFunctionalRollingPreflight:
    def __init__(
        self,
        database_path: str | Path,
        *,
        capture_signer: Callable[[str, Mapping[str, Any]], str],
        capture_verifier: Callable[[str, Mapping[str, Any], str], bool],
        server_authority_key_id: str,
        process_generation: str,
        account_fingerprint: str,
        credential_configuration_hash: str,
        contract_envelope_hash: str,
        code_manifest_hash: str,
        public_arm_id: str,
        preapproval_hash: str,
        wall_clock: Callable[[], datetime],
        owner_token_factory: Callable[[], bytes],
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.path = Path(database_path).expanduser().resolve()
        self.signer = capture_signer
        self.verifier = capture_verifier
        self.wall_clock = wall_clock
        self.id_factory = id_factory or (lambda: uuid.uuid4().hex)
        if not callable(capture_signer) or not callable(capture_verifier):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-authority-callable-invalid"
            )
        if type(server_authority_key_id) is not str or len(server_authority_key_id) < 16:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-authority-key-id-invalid"
            )
        if type(process_generation) is not str or not _PROCESS_GENERATION.fullmatch(process_generation):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-process-generation-invalid"
            )
        token = owner_token_factory()
        if not isinstance(token, bytes) or len(token) < 32:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-owner-token-invalid"
            )
        self.process_generation = process_generation
        self.owner_token_hash = hashlib.sha256(token).hexdigest()
        self.binding = _Binding(
            account_fingerprint=_sha(account_fingerprint, "account-fingerprint"),
            credential_configuration_hash=_sha(
                credential_configuration_hash, "credential-configuration-hash"
            ),
            contract_envelope_hash=_sha(contract_envelope_hash, "contract-envelope-hash"),
            code_manifest_hash=_sha(code_manifest_hash, "code-manifest-hash"),
            public_arm_id=_identifier(public_arm_id, "public-arm-id"),
            preapproval_hash=_sha(preapproval_hash, "preapproval-hash"),
            authority_key_id_hash=hashlib.sha256(
                server_authority_key_id.encode("utf-8")
            ).hexdigest(),
        )
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        self.startup_invalidated_snapshot_ids = self._invalidate_ready_on_restart()

    def _connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            conn = sqlite3.connect(
                f"file:{self.path.as_posix()}?mode=ro", uri=True, timeout=5
            )
        else:
            conn = sqlite3.connect(self.path, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize(self) -> None:
        with closing(self._connect()) as conn, conn:
            objects = conn.execute(
                "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
            if not objects:
                conn.executescript(_SCHEMA_SQL)
                conn.execute(
                    "INSERT INTO kis_functional_rolling_preflight_meta VALUES (1,?,?)",
                    (SCHEMA_VERSION, SCHEMA_FINGERPRINT),
                )
            _verify_schema(conn)

    def _now(self) -> datetime:
        value = self.wall_clock()
        return _parse_time(_time_text(value, "trusted-now"), "trusted-now")

    def _sign(self, domain: str, body: Mapping[str, Any]) -> str:
        signature = self.signer(domain, body)
        parsed = _signature(signature, domain.lower())
        _verify_signed(self.verifier, domain, body, parsed, domain.lower())
        return parsed

    def _terminal(
        self,
        *,
        row: sqlite3.Row,
        state: str,
        reason: str,
        at: datetime,
    ) -> tuple[str, str, str]:
        body = {
            "schemaVersion": "kis-domestic-rolling-preflight-terminal/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "snapshotId": str(row["snapshot_id"]),
            "snapshotHash": str(row["snapshot_hash"]),
            "priorState": str(row["state"]),
            "state": state,
            "reason": reason,
            "occurredAt": _time_text(at, "terminal-at"),
            "processGeneration": self.process_generation,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return (
            _canonical(body).decode("utf-8"),
            _hash(body),
            self._sign("ROLLING_PREFLIGHT_TERMINAL", body),
        )

    def _invalidate_ready_on_restart(self) -> tuple[str, ...]:
        now = self._now()
        invalidated: list[str] = []
        with self._lock, closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            rows = conn.execute(
                "SELECT * FROM kis_functional_rolling_preflight_snapshot "
                "WHERE state='READY' ORDER BY snapshot_id"
            ).fetchall()
            for row in rows:
                terminal_json, terminal_hash, terminal_signature = self._terminal(
                    row=row,
                    state="INVALIDATED_RESTART",
                    reason="PROCESS_OR_OWNER_RESTART",
                    at=now,
                )
                changed = conn.execute(
                    "UPDATE kis_functional_rolling_preflight_snapshot "
                    "SET state='INVALIDATED_RESTART',terminal_json=?,terminal_hash=?,"
                    "terminal_signature=?,revision=revision+1 "
                    "WHERE snapshot_id=? AND state='READY' AND revision=?",
                    (
                        terminal_json, terminal_hash, terminal_signature,
                        row["snapshot_id"], row["revision"],
                    ),
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalRollingPreflightBlocked(
                        "rolling-preflight-restart-cas-failed"
                    )
                invalidated.append(str(row["snapshot_id"]))
        return tuple(invalidated)

    def accept_snapshot(self, diagnostic: Mapping[str, Any]) -> dict[str, Any]:
        body, diagnostic_hash, diagnostic_signature, completed, expires, boundary = (
            _validate_diagnostic(
                diagnostic,
                binding=self.binding,
                verifier=self.verifier,
            )
        )
        now = self._now()
        if now < completed or now >= boundary or now > expires:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-acceptance-not-before-fresh-boundary"
            )
        suffix = self.id_factory()
        if type(suffix) is not str or not re.fullmatch(r"[0-9a-f]{32}", suffix):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-id-source-invalid"
            )
        snapshot_id = f"kis-rolling-snapshot-{suffix}"
        snapshot_body = {
            "schemaVersion": SNAPSHOT_SCHEMA,
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "snapshotId": snapshot_id,
            "diagnosticHash": diagnostic_hash,
            "captureBundleHash": body["captureBundleHash"],
            "accountFingerprint": self.binding.account_fingerprint,
            "credentialConfigurationHash": self.binding.credential_configuration_hash,
            "preactivationBaselineHash": body["preactivationBaselineHash"],
            "contractEnvelopeHash": self.binding.contract_envelope_hash,
            "codeManifestHash": self.binding.code_manifest_hash,
            "publicArmId": self.binding.public_arm_id,
            "preapprovalHash": self.binding.preapproval_hash,
            "completedAt": body["completedAt"],
            "expiresAt": _time_text(expires, "expires-at"),
            "nextBoundaryAt": body["intendedNextOpenAt"],
            "processGeneration": self.process_generation,
            "ownerTokenHash": self.owner_token_hash,
            "state": "READY",
            "singleUse": True,
            "privateAccountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "networkOrderPostAllowed": False,
            "finalQuoteAvailable": False,
            "releaseEvidenceEligible": False,
        }
        snapshot_hash = _hash(snapshot_body)
        snapshot_signature = self._sign("ROLLING_PREFLIGHT_SNAPSHOT", snapshot_body)
        try:
            with self._lock, closing(self._connect()) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                _verify_schema(conn)
                conn.execute(
                    "INSERT INTO kis_functional_rolling_preflight_snapshot "
                    "(snapshot_id,state,process_generation,owner_token_hash,"
                    "next_boundary_at,completed_at,expires_at,account_fingerprint,"
                    "credential_configuration_hash,preactivation_baseline_hash,"
                    "capture_bundle_hash,contract_envelope_hash,code_manifest_hash,"
                    "public_arm_id,preapproval_hash,diagnostic_json,diagnostic_hash,"
                    "diagnostic_signature,snapshot_json,snapshot_hash,snapshot_signature,"
                    "revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                    (
                        snapshot_id, "READY", self.process_generation,
                        self.owner_token_hash, body["intendedNextOpenAt"],
                        body["completedAt"], _time_text(expires, "expires-at"),
                        self.binding.account_fingerprint,
                        self.binding.credential_configuration_hash,
                        body["preactivationBaselineHash"], body["captureBundleHash"],
                        self.binding.contract_envelope_hash,
                        self.binding.code_manifest_hash, self.binding.public_arm_id,
                        self.binding.preapproval_hash,
                        _canonical(body).decode("utf-8"), diagnostic_hash,
                        diagnostic_signature, _canonical(snapshot_body).decode("utf-8"),
                        snapshot_hash, snapshot_signature,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-snapshot-duplicate-or-boundary-already-ready"
            ) from exc
        return {
            "body": snapshot_body,
            "snapshotHash": snapshot_hash,
            "serverAuthoritySignature": snapshot_signature,
        }

    def _reject(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        state: str,
        reason: str,
        now: datetime,
    ) -> None:
        terminal_json, terminal_hash, terminal_signature = self._terminal(
            row=row, state=state, reason=reason, at=now
        )
        changed = conn.execute(
            "UPDATE kis_functional_rolling_preflight_snapshot "
            "SET state=?,terminal_json=?,terminal_hash=?,terminal_signature=?,"
            "revision=revision+1 WHERE snapshot_id=? AND state='READY' AND revision=?",
            (
                state, terminal_json, terminal_hash, terminal_signature,
                row["snapshot_id"], row["revision"],
            ),
        ).rowcount
        if changed != 1:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-rejection-cas-failed"
            )

    def _verify_ready_row(self, row: sqlite3.Row) -> None:
        try:
            diagnostic_body = json.loads(str(row["diagnostic_json"]))
            snapshot_body = json.loads(str(row["snapshot_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-stored-json-invalid"
            ) from exc
        validated, diagnostic_hash, _signature_value, completed, expires, boundary = (
            _validate_diagnostic(
                {
                    "body": diagnostic_body,
                    "diagnosticHash": str(row["diagnostic_hash"]),
                    "serverAuthoritySignature": str(row["diagnostic_signature"]),
                },
                binding=self.binding,
                verifier=self.verifier,
            )
        )
        if (
            diagnostic_hash != str(row["diagnostic_hash"])
            or validated["captureBundleHash"] != str(row["capture_bundle_hash"])
            or validated["preactivationBaselineHash"]
            != str(row["preactivation_baseline_hash"])
            or validated["accountFingerprint"] != str(row["account_fingerprint"])
            or validated["credentialConfigurationHash"]
            != str(row["credential_configuration_hash"])
            or validated["contractEnvelopeHash"]
            != str(row["contract_envelope_hash"])
            or validated["codeManifestHash"] != str(row["code_manifest_hash"])
            or validated["publicArmId"] != str(row["public_arm_id"])
            or validated["preapprovalHash"] != str(row["preapproval_hash"])
            or _time_text(completed, "stored-completed") != str(row["completed_at"])
            or _time_text(expires, "stored-expires") != str(row["expires_at"])
            or _time_text(boundary, "stored-boundary")
            != str(row["next_boundary_at"])
        ):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-stored-diagnostic-row-mismatch"
            )
        if set(snapshot_body) != _SNAPSHOT_KEYS:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-stored-snapshot-fields-not-exact"
            )
        snapshot_hash = str(row["snapshot_hash"])
        if not hmac.compare_digest(_hash(snapshot_body), snapshot_hash):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-stored-snapshot-hash-mismatch"
            )
        _verify_signed(
            self.verifier,
            "ROLLING_PREFLIGHT_SNAPSHOT",
            snapshot_body,
            row["snapshot_signature"],
            "stored-snapshot",
        )
        expected = {
            "schemaVersion": SNAPSHOT_SCHEMA,
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "snapshotId": str(row["snapshot_id"]),
            "diagnosticHash": str(row["diagnostic_hash"]),
            "captureBundleHash": str(row["capture_bundle_hash"]),
            "accountFingerprint": str(row["account_fingerprint"]),
            "credentialConfigurationHash": str(
                row["credential_configuration_hash"]
            ),
            "preactivationBaselineHash": str(row["preactivation_baseline_hash"]),
            "contractEnvelopeHash": str(row["contract_envelope_hash"]),
            "codeManifestHash": str(row["code_manifest_hash"]),
            "publicArmId": str(row["public_arm_id"]),
            "preapprovalHash": str(row["preapproval_hash"]),
            "completedAt": str(row["completed_at"]),
            "expiresAt": str(row["expires_at"]),
            "nextBoundaryAt": str(row["next_boundary_at"]),
            "processGeneration": str(row["process_generation"]),
            "ownerTokenHash": str(row["owner_token_hash"]),
            "state": "READY",
            "singleUse": True,
            "privateAccountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "networkOrderPostAllowed": False,
            "finalQuoteAvailable": False,
            "releaseEvidenceEligible": False,
        }
        if any(
            type(snapshot_body.get(key)) is not type(value)
            or snapshot_body.get(key) != value
            for key, value in expected.items()
        ):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-stored-snapshot-row-mismatch"
            )

    def consume_for_trigger(self, trigger_envelope: Mapping[str, Any]) -> dict[str, Any]:
        envelope = _mapping(trigger_envelope, "trigger-envelope")
        if set(envelope) != {"body", "triggerEnvelopeHash", "serverAuthoritySignature"}:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-trigger-envelope-not-exact"
            )
        trigger = dict(_mapping(envelope.get("body"), "trigger-body"))
        if set(trigger) != _TRIGGER_KEYS:
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-trigger-fields-not-exact"
            )
        trigger_envelope_hash = _sha(
            envelope.get("triggerEnvelopeHash"), "trigger-envelope-hash"
        )
        if not hmac.compare_digest(trigger_envelope_hash, _hash(trigger)):
            raise KisDomesticFunctionalRollingPreflightBlocked(
                "rolling-preflight-trigger-hash-mismatch"
            )
        trigger_signature = _verify_signed(
            self.verifier,
            "ROLLING_PREFLIGHT_TRIGGER",
            {**trigger, "triggerEnvelopeHash": trigger_envelope_hash},
            envelope.get("serverAuthoritySignature"),
            "trigger",
        )
        snapshot_id = _identifier(trigger.get("snapshotId"), "snapshot-id")
        now = self._now()
        with self._lock, closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_rolling_preflight_snapshot "
                "WHERE snapshot_id=?", (snapshot_id,)
            ).fetchone()
            if row is None:
                conn.rollback()
                raise KisDomesticFunctionalRollingPreflightBlocked(
                    "rolling-preflight-snapshot-missing-post-zero"
                )
            if str(row["state"]) != "READY":
                conn.rollback()
                raise KisDomesticFunctionalRollingPreflightBlocked(
                    "rolling-preflight-snapshot-not-ready-or-already-consumed"
                )
            self._verify_ready_row(row)
            boundary = _parse_time(row["next_boundary_at"], "stored-boundary")
            expires = _parse_time(row["expires_at"], "stored-expires")
            observed = _parse_time(trigger.get("observedAt"), "trigger-observed")
            bar_open = _parse_time(trigger.get("barOpenAt"), "trigger-bar-open")
            exact = {
                "schemaVersion": TRIGGER_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "snapshotId": snapshot_id,
                "publicArmId": self.binding.public_arm_id,
                "preapprovalHash": self.binding.preapproval_hash,
                "accountFingerprint": self.binding.account_fingerprint,
                "credentialConfigurationHash": self.binding.credential_configuration_hash,
                "contractEnvelopeHash": self.binding.contract_envelope_hash,
                "codeManifestHash": self.binding.code_manifest_hash,
                "accountAuthorityAvailable": False,
                "tokenAuthorityAvailable": False,
                "orderAuthorityAvailable": False,
            }
            identity_valid = (
                all(
                    type(trigger.get(key)) is type(expected)
                    and trigger.get(key) == expected
                    for key, expected in exact.items()
                )
                and self.process_generation == str(row["process_generation"])
                and self.owner_token_hash == str(row["owner_token_hash"])
                and type(trigger.get("sourceGeneration")) is str
                and _SOURCE_GENERATION.fullmatch(trigger["sourceGeneration"]) is not None
                and all(
                    type(trigger.get(key)) is str and _SHA256.fullmatch(trigger[key])
                    for key in (
                        "evaluationHash", "triggerHash", "sessionNonceHash"
                    )
                )
                and all(
                    type(trigger.get(key)) is str and _IDENTIFIER.fullmatch(trigger[key])
                    for key in ("evaluationId", "triggerId", "sessionId")
                )
                and bar_open == boundary
            )
            fresh = (
                boundary <= observed <= boundary + timedelta(seconds=NEXT_OPEN_OBSERVATION_SECONDS)
                and observed <= now <= observed + timedelta(seconds=NEXT_OPEN_OBSERVATION_SECONDS)
                and now <= expires
            )
            if not identity_valid or not fresh:
                state = "REJECTED_TRIGGER_MISMATCH" if not identity_valid else "REJECTED_STALE"
                reason = "TRIGGER_BINDING_CHANGED" if not identity_valid else "SNAPSHOT_STALE_AT_TRIGGER"
                self._reject(conn, row, state=state, reason=reason, now=now)
                conn.commit()
                raise KisDomesticFunctionalRollingPreflightBlocked(
                    "rolling-preflight-trigger-rejected-post-zero"
                )
            receipt = {
                "schemaVersion": RECEIPT_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "snapshotId": snapshot_id,
                "snapshotHash": str(row["snapshot_hash"]),
                "diagnosticHash": str(row["diagnostic_hash"]),
                "captureBundleHash": str(row["capture_bundle_hash"]),
                "accountFingerprint": str(row["account_fingerprint"]),
                "credentialConfigurationHash": str(row["credential_configuration_hash"]),
                "preactivationBaselineHash": str(row["preactivation_baseline_hash"]),
                "contractEnvelopeHash": str(row["contract_envelope_hash"]),
                "codeManifestHash": str(row["code_manifest_hash"]),
                "publicArmId": str(row["public_arm_id"]),
                "preapprovalHash": str(row["preapproval_hash"]),
                "evaluationId": trigger["evaluationId"],
                "evaluationHash": trigger["evaluationHash"],
                "triggerId": trigger["triggerId"],
                "triggerHash": trigger["triggerHash"],
                "triggerEnvelopeHash": trigger_envelope_hash,
                "sourceGeneration": trigger["sourceGeneration"],
                "barOpenAt": trigger["barOpenAt"],
                "completedAt": str(row["completed_at"]),
                "expiresAt": str(row["expires_at"]),
                "consumedAt": _time_text(now, "consumed-at"),
                "sessionId": trigger["sessionId"],
                "sessionNonceHash": trigger["sessionNonceHash"],
                "singleUseConsumed": True,
                "privateAccountAuthorityAvailable": False,
                "tokenAuthorityAvailable": False,
                "orderAuthorityAvailable": False,
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
                "finalQuoteAvailable": False,
                "releaseEvidenceEligible": False,
            }
            receipt_hash = _hash(receipt)
            receipt_signature = self._sign("ROLLING_PREFLIGHT_RECEIPT", receipt)
            changed = conn.execute(
                "UPDATE kis_functional_rolling_preflight_snapshot SET "
                "state='CONSUMED',trigger_json=?,trigger_envelope_hash=?,"
                "trigger_signature=?,terminal_json=?,terminal_hash=?,"
                "terminal_signature=?,consumed_at=?,revision=revision+1 "
                "WHERE snapshot_id=? AND state='READY' AND revision=?",
                (
                    _canonical(trigger).decode("utf-8"), trigger_envelope_hash,
                    trigger_signature, _canonical(receipt).decode("utf-8"),
                    receipt_hash, receipt_signature, receipt["consumedAt"],
                    snapshot_id, row["revision"],
                ),
            ).rowcount
            if changed != 1:
                conn.rollback()
                raise KisDomesticFunctionalRollingPreflightBlocked(
                    "rolling-preflight-consumption-cas-failed"
                )
            conn.commit()
        return {
            "body": receipt,
            "receiptHash": receipt_hash,
            "serverAuthoritySignature": receipt_signature,
        }

    def snapshot(self, snapshot_id: str) -> dict[str, Any]:
        with closing(self._connect(readonly=True)) as conn:
            _verify_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_rolling_preflight_snapshot "
                "WHERE snapshot_id=?", (snapshot_id,)
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalRollingPreflightBlocked(
                    "rolling-preflight-snapshot-missing"
                )
            return {
                "snapshotId": str(row["snapshot_id"]),
                "state": str(row["state"]),
                "snapshotHash": str(row["snapshot_hash"]),
                "diagnosticHash": str(row["diagnostic_hash"]),
                "nextBoundaryAt": str(row["next_boundary_at"]),
                "completedAt": str(row["completed_at"]),
                "expiresAt": str(row["expires_at"]),
                "revision": int(row["revision"]),
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
            }


def rolling_preflight_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-rolling-preflight-status/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "productionAvailable": False,
        "networkAvailable": False,
        "accountAuthorityAvailable": False,
        "tokenAuthorityAvailable": False,
        "orderAuthorityAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "durableSnapshotJournalAvailable": True,
        "singleUseTriggerConsumptionAvailable": True,
        "finalQuoteAvailable": False,
        "snapshotFreshnessSeconds": SNAPSHOT_FRESHNESS_SECONDS,
        "nextOpenObservationSeconds": NEXT_OPEN_OBSERVATION_SECONDS,
        "schemaFingerprint": SCHEMA_FINGERPRINT,
        "integrationStatus": "OFFLINE_ISOLATED_NOT_WIRED",
        "networkOrderPostAllowed": False,
    }


__all__ = [
    "DIAGNOSTIC_SCHEMA",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ACCOUNT_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_ORDER_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_ROLLING_PREFLIGHT_TOKEN_AUTHORITY_AVAILABLE",
    "KisDomesticFunctionalRollingPreflightBlocked",
    "DurableKisDomesticFunctionalRollingPreflight",
    "NEXT_OPEN_OBSERVATION_SECONDS",
    "SCHEMA_FINGERPRINT",
    "SNAPSHOT_FRESHNESS_SECONDS",
    "rolling_preflight_component_status",
]
