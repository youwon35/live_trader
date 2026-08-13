from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import sqlite3
import threading
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from .kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from .program_ledger import ProgramLedger
from .kis_domestic_functional_lane import sign_kis_domestic_lane_grant_receipt
from .kis_domestic_functional_key_registry import (
    GRAPH_BINDING_SCHEMA,
    VerifyOnlyKeyRegistry,
)
from .kis_domestic_functional_readers import (
    READERS_COMPONENT_PROTOCOL_HASH,
    READERS_COMPONENT_SCHEMA_FINGERPRINT,
    readers_component_status,
)


KIS_DOMESTIC_FUNCTIONAL_GRAPH_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_GRAPH_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_GRAPH_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_GRAPH_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_GRAPH_STATE_SERVER_WIRED = False
KIS_DOMESTIC_FUNCTIONAL_GRAPH_V2_PRODUCTION_AVAILABLE = False

SCHEMA_VERSION = "kis-domestic-functional-graph-schema/v1"
PREALLOCATION_SCHEMA = "kis-domestic-functional-graph-preallocation/v1"
ACTIVATION_SCHEMA = "kis-domestic-functional-graph-activation/v1"
HEARTBEAT_START_SCHEMA = "kis-domestic-functional-graph-heartbeat-start/v1"
CAPABILITY_GRANT_SCHEMA = "kis-domestic-functional-graph-capability-grant/v1"
ROLLING_CONSUMPTION_SCHEMA = (
    "kis-domestic-functional-graph-rolling-consumption/v1"
)

V2_REQUEST_SCHEMA = "kis-domestic-functional-graph-v2-request/v1"
V2_PORT_STATUS_SCHEMA = "kis-domestic-functional-graph-v2-port-status/v1"
V2_PORT_RECEIPT_SCHEMA = "kis-domestic-functional-graph-v2-port-receipt/v1"
V2_OPERATION_SCHEMA = "kis-domestic-functional-graph-v2-operation/v1"
V2_STEP_SCHEMA = "kis-domestic-functional-graph-v2-step/v1"
V2_VERIFY_SCHEMA = "kis-domestic-functional-graph-v2-union-verifier/v1"

V2_COMPONENTS = (
    "owner",
    "key_registry",
    "readers",
    "market_source",
    "rolling",
    "quote",
    "capability",
    "heartbeat",
    "lane",
    "source",
    "manager",
)

V2_CRASH_STAGES = (
    "AFTER_PREPARE",
    "AFTER_ROLLING_PORT_RETURN_BEFORE_RECORD",
    "AFTER_ROLLING_CAS",
    "AFTER_QUOTE_PORT_RETURN_BEFORE_RECORD",
    "AFTER_QUOTE_CAS",
    "AFTER_LANE_PORT_RETURN_BEFORE_RECORD",
    "AFTER_LANE_CAS",
    "AFTER_HEARTBEAT_PORT_RETURN_BEFORE_RECORD",
    "AFTER_HEARTBEAT_CAS",
    "AFTER_CAPABILITY_PORT_RETURN_BEFORE_RECORD",
    "AFTER_CAPABILITY_CAS",
    "BEFORE_FINALIZE",
)

_V2_REQUEST_KEYS = {
    "schemaVersion", "operationId", "preallocationId", "sessionId",
    "publicArmId", "publicArmHash", "bootstrapId", "bootstrapHash",
    "approvalId", "approvalHash", "evaluationId", "evaluationHash",
    "triggerId", "triggerHash", "sourceObservationId", "sourceGeneration",
    "rollingSnapshotId", "rollingSnapshotHash", "rollingTriggerEnvelopeHash",
    "quoteReceiptId", "quoteReceiptHash", "capabilityId", "permitId",
    "permitHash", "sessionNonceHash", "accountFingerprint",
    "credentialConfigurationHash", "preactivationBaselineHash",
    "contractEnvelopeHash", "codeManifestHash", "ownerEpoch",
    "ownerEpochId", "ownerEpochHash", "registryEpoch",
    "registryManifestHash", "registryAcceptedHeadHash",
    "registryAcceptanceRevision", "registryGraphBindingHash",
    "registryId", "registryManifestFileHash", "registryRootKeyIdHash",
    "registryFactoryBindingHash", "registryClockGeneration",
    "processGeneration", "socketGeneration", "requestedAt",
    "freshQuoteHash", "freshQuoteObservedAt", "freshQuotePriceKrw",
    "naturalBuyLimitPriceKrw",
    "actionInputsHash", "productionAvailable", "requestHash",
}

_V2_STATUS_KEYS = {
    "schemaVersion", "route", "pdno", "component", "implementationType",
    "codeHash", "protocolHash", "statusRevision", "statusHeadHash",
    "sessionId", "accountFingerprint", "credentialConfigurationHash",
    "ownerEpoch", "ownerEpochId", "ownerEpochHash", "registryEpoch",
    "registryManifestHash", "registryAcceptedHeadHash",
    "registryAcceptanceRevision", "registryGraphBindingHash",
    "registryId", "registryManifestFileHash", "registryRootKeyIdHash",
    "registryFactoryBindingHash", "registryClockGeneration",
    "preflightReady", "readinessBlockers", "exactCasAvailable",
    "verifyOnly", "nativeGrantInstantAccepted", "offlineSimulation",
    "productionAvailable", "networkAvailable", "mutationAvailable",
    "releaseAvailable", "statusHash",
}

_V2_PORT_RAW_KEYS = {
    "schemaVersion", "component", "action", "outcome", "result",
    "authoritativeReceiptHash", "singleUseBurned", "exactCas",
    "operationId", "sessionId", "requestHash", "actionInputsHash",
    "grantWallAt", "grantMonotonicNs", "expectedStatusRevision",
    "expectedStatusHeadHash", "intentStepHash", "resultRevision",
    "resultHeadHash", "productionAvailable",
}

_V2_LANE_GRANT_BODY_KEYS = {
    "schemaVersion", "route", "pdno", "source", "graphTransactionId",
    "graphRequestHash", "graphActionInputsHash", "graphIntentStepHash",
    "expectedStatusRevision", "expectedStatusHeadHash", "ownerEpochHash",
    "registryAcceptedHeadHash", "sessionId", "bootstrapId", "approvalId",
    "evaluationId", "triggerId", "triggerHash", "accountFingerprint",
    "preactivationBaselineHash", "codeManifestHash", "rollingReceiptHash",
    "quoteReceiptHash", "freshQuoteHash", "grantWallAt",
    "grantMonotonicNs", "capturedOnce", "serverAuthorityKeyIdHash",
}
_V2_LANE_ACTIVATION_ARGUMENT_KEYS = {
    "bootstrap_id", "approval_id", "evaluation_id", "trigger_id", "session_id",
    "fresh_quote_hash", "fresh_quote_observed_at", "fresh_quote_price_krw",
    "natural_buy_limit_price_krw", "graph_grant_instant_receipt",
}
_V2_LANE_RESULT_KEYS = {
    "schemaVersion", "sessionId", "state", "activatedAt",
    "activationObservedAt", "grantMonotonicNs", "grantReceiptHash",
    "expiresAt", "activeSeconds", "evaluationId", "triggerId",
    "activationRecordHash", "naturalBuyClaimId", "naturalBuyClaimHash",
    "realOrdersEnabled", "promotionEligible", "laneGrantInstantReceipt",
}

V2_FROZEN_COMPONENT_FILE_SHA256 = {
    "owner": "5480bcee4cf935e860bd6d9f49606ccd9dd55cf9aae49b90bc1ee4d4391426d6",
    "key_registry": "d13b0d10cf365a05a7e9691f5b0d1fe3fa7e9cdf06dd36fa0ff39a06539eddfb",
    "market_source": "078c312a0f8bbb0ae9ac2d50299a9406c20823b1afb382562d1a77b8bb27757b",
    "rolling": "949dfed9eb778ce69edef71fbeff6b02a2f76585a61de2d87e42c13e251ccb7b",
    "quote": "4b518526cf36215b3b63b6df7ad5d682180e88a5b35c799fb2732b815fe0c1b9",
    "capability": "09bbfe9e4842fdedf6eb88cdcb6b2dd6a1af89a1ccd5ca89df6db63da673bb89",
    "heartbeat": "f298834f21abdcc7c43c108ef444cbb92fe7fc32441ba058118b264520fa51ec",
    "lane": "e5ff57817b8f25008454df3147d91ee93a69ff4227925fcfa03913fd1994643e",
    "source": "49818454f1ed9b9a6caf3d6da4ef951500cd3cce98e85c205f98ec54bee84af5",
    "manager": "5330590eb1b46ab5ac07b83263a1dbb6066e2649a4180f449eb8d7b396cb80cb",
}

_V2_EXPECTED_IMPLEMENTATION_TYPES = {
    "owner": "live_trader.kis_domestic_functional_owner.DurableKisDomesticFunctionalOwner",
    "key_registry": "live_trader.kis_domestic_functional_key_registry.VerifyOnlyKeyRegistry",
    "readers": "live_trader.kis_domestic_functional_readers.KisDomesticFunctionalVerifyOnlyReaders",
    "market_source": "live_trader.kis_domestic_functional_market_source.DisabledKisDomesticFunctionalMarketSource",
    "rolling": "live_trader.kis_domestic_functional_rolling_preflight.DurableKisDomesticFunctionalRollingPreflight",
    "quote": "live_trader.kis_domestic_functional_quote.DurableKisDomesticFunctionalQuoteStore",
    "capability": "live_trader.kis_domestic_functional_capability.DurableKisDomesticFunctionalCapabilityLedger",
    "heartbeat": "live_trader.kis_domestic_functional_heartbeat.DurableKisDomesticFunctionalHeartbeat",
    "lane": "live_trader.kis_domestic_functional_lane.DurableKisDomesticFunctionalLane",
    "source": "live_trader.kis_domestic_functional_source.DurableKisDomesticPublicArmJournal",
    "manager": "live_trader.kis_domestic_functional_manager.DisabledKisDomesticFunctionalManager",
}

V2_FROZEN_PROTOCOL_HASHES = {
    component: hashlib.sha256(
        (
            "kis-domestic-functional-graph-v2-frozen-port/v1\n"
            + component
            + "\nstatus+verify+exact-cas+compensate"
        ).encode("utf-8")
    ).hexdigest()
    for component in V2_COMPONENTS
}
V2_FROZEN_PROTOCOL_HASHES["lane"] = hashlib.sha256(
    (
        "kis-domestic-functional-graph-v2-frozen-port/v2\n"
        "lane\nstatus+verify+exact-cas+compensate+signed-grant-instant/v1"
    ).encode("utf-8")
).hexdigest()

CRASH_STAGES = (
    "AFTER_VERIFY",
    "AFTER_ACTIVATION_INSERT",
    "AFTER_ROLLING_CONSUME",
    "AFTER_HEARTBEAT_START",
    "BEFORE_COMMIT",
    "AFTER_COMMIT",
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@-]{1,160}$", re.ASCII)
_PROCESS_GENERATION = re.compile(
    r"^kis-graph-generation-[0-9a-f]{32}$", re.ASCII
)
_SOURCE_GENERATION = re.compile(r"^kis-ws-generation-[0-9a-f]{32}$", re.ASCII)

_PREALLOCATION_KEYS = {
    "schemaVersion",
    "route",
    "origin",
    "pdno",
    "preallocationId",
    "sessionId",
    "publicArmId",
    "publicArmHash",
    "bootstrapId",
    "bootstrapHash",
    "approvalId",
    "approvalHash",
    "evaluationId",
    "evaluationHash",
    "triggerId",
    "triggerHash",
    "triggerBarOpenAt",
    "triggerObservedAt",
    "sourceGeneration",
    "rollingSnapshotId",
    "rollingSnapshotHash",
    "rollingReceiptHash",
    "rollingReceiptSignatureHash",
    "rollingCompletedAt",
    "rollingExpiresAt",
    "permitId",
    "permitHash",
    "sessionNonceHash",
    "accountFingerprint",
    "preactivationBaselineHash",
    "contractEnvelopeHash",
    "codeManifestHash",
    "artifactContentHash",
    "artifactFileSha256",
    "instanceContentHash",
    "instanceFileSha256",
    "quantity",
    "maxOrderKrw",
    "maxGrossKrw",
    "maxGrossSemantics",
    "ownerLossMustRemainBelowKrw",
    "activeSeconds",
    "preallocatedAt",
    "accountAuthorityAvailable",
    "orderAuthorityAvailable",
    "networkOrderPostAllowed",
    "promotionEligible",
}

_SCHEMA_SQL = """
CREATE TABLE kis_functional_graph_manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_functional_graph_preallocation (
    preallocation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    rolling_snapshot_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('READY','CONSUMED','ABORTED')),
    record_json TEXT NOT NULL,
    record_hash TEXT NOT NULL UNIQUE,
    signature TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1)
);
CREATE UNIQUE INDEX kis_functional_graph_ready_idx
    ON kis_functional_graph_preallocation(state) WHERE state='READY';
CREATE TABLE kis_functional_graph_rolling_projection (
    rolling_snapshot_id TEXT PRIMARY KEY,
    preallocation_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('READY','CONSUMED')),
    rolling_snapshot_hash TEXT NOT NULL,
    rolling_receipt_hash TEXT NOT NULL,
    rolling_receipt_signature_hash TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT NOT NULL DEFAULT '',
    consumption_json TEXT NOT NULL DEFAULT '',
    consumption_hash TEXT NOT NULL DEFAULT '',
    consumption_signature TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK(revision>=1),
    FOREIGN KEY(preallocation_id)
        REFERENCES kis_functional_graph_preallocation(preallocation_id)
);
CREATE TABLE kis_functional_graph_grant (
    session_id TEXT PRIMARY KEY,
    preallocation_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','RECONCILIATION_REQUIRED')),
    owner_process_generation TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    activated_monotonic_ns INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    cleanup_ends_at TEXT NOT NULL,
    authority_open INTEGER NOT NULL CHECK(authority_open IN (0,1)),
    activation_json TEXT NOT NULL,
    activation_hash TEXT NOT NULL UNIQUE,
    activation_signature TEXT NOT NULL,
    failure_json TEXT NOT NULL DEFAULT '',
    failure_hash TEXT NOT NULL DEFAULT '',
    failure_signature TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK(revision>=1),
    FOREIGN KEY(preallocation_id)
        REFERENCES kis_functional_graph_preallocation(preallocation_id)
);
CREATE UNIQUE INDEX kis_functional_graph_active_grant_idx
    ON kis_functional_graph_grant(authority_open) WHERE authority_open=1;
CREATE TABLE kis_functional_graph_capability (
    session_id TEXT PRIMARY KEY,
    capability_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','RECONCILIATION_REQUIRED')),
    authority_open INTEGER NOT NULL CHECK(authority_open IN (0,1)),
    activated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    capability_json TEXT NOT NULL,
    capability_hash TEXT NOT NULL UNIQUE,
    capability_signature TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1),
    FOREIGN KEY(session_id) REFERENCES kis_functional_graph_grant(session_id)
);
CREATE UNIQUE INDEX kis_functional_graph_active_capability_idx
    ON kis_functional_graph_capability(authority_open) WHERE authority_open=1;
CREATE TABLE kis_functional_graph_heartbeat_start (
    session_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','RECONCILIATION_REQUIRED')),
    wall_at TEXT NOT NULL,
    monotonic_ns INTEGER NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence=1),
    sample_json TEXT NOT NULL,
    sample_hash TEXT NOT NULL UNIQUE,
    sample_signature TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1),
    FOREIGN KEY(session_id) REFERENCES kis_functional_graph_grant(session_id)
);
"""

_SCHEMA_DESCRIPTOR = {
    "objects": [
        "kis_functional_graph_manifest",
        "kis_functional_graph_preallocation",
        "kis_functional_graph_ready_idx",
        "kis_functional_graph_rolling_projection",
        "kis_functional_graph_grant",
        "kis_functional_graph_active_grant_idx",
        "kis_functional_graph_capability",
        "kis_functional_graph_active_capability_idx",
        "kis_functional_graph_heartbeat_start",
    ],
    "crashStages": list(CRASH_STAGES),
    "activeSeconds": ACTIVE_SECONDS,
    "productionAvailable": False,
}

_V2_SCHEMA_VERSION = "kis-domestic-functional-graph-v2-schema/v1"
_V2_SCHEMA_SQL = """
CREATE TABLE kis_functional_v2_graph_manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_functional_v2_graph_operation (
    operation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN (
        'PREPARED','APPLYING','ACTIVE_OFFLINE_SIMULATION','ABORTED',
        'SAFE_INCOMPLETE','RECONCILIATION_REQUIRED'
    )),
    request_json TEXT NOT NULL,
    request_hash TEXT NOT NULL UNIQUE,
    component_statuses_json TEXT NOT NULL,
    component_statuses_hash TEXT NOT NULL,
    port_bindings_hash TEXT NOT NULL,
    captured_wall_at TEXT NOT NULL,
    captured_monotonic_ns INTEGER NOT NULL CHECK(captured_monotonic_ns>=0),
    owner_epoch INTEGER NOT NULL CHECK(owner_epoch>=1),
    owner_epoch_hash TEXT NOT NULL,
    registry_accepted_head_hash TEXT NOT NULL,
    burned_rolling INTEGER NOT NULL CHECK(burned_rolling IN (0,1)),
    burned_quote INTEGER NOT NULL CHECK(burned_quote IN (0,1)),
    lane_active INTEGER NOT NULL CHECK(lane_active IN (0,1)),
    heartbeat_active INTEGER NOT NULL CHECK(heartbeat_active IN (0,1)),
    capability_active INTEGER NOT NULL CHECK(capability_active IN (0,1)),
    hazardous_authority_open INTEGER NOT NULL CHECK(hazardous_authority_open IN (0,1)),
    failure_reason TEXT NOT NULL DEFAULT '',
    step_count INTEGER NOT NULL CHECK(step_count>=0),
    step_head_hash TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=1)
);
CREATE UNIQUE INDEX kis_functional_v2_graph_open_operation_idx
    ON kis_functional_v2_graph_operation(state)
    WHERE state IN ('PREPARED','APPLYING','ACTIVE_OFFLINE_SIMULATION');
CREATE TABLE kis_functional_v2_graph_step (
    operation_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal>=1),
    component TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN (
        'INTENT','SUCCEEDED','FAILED','COMPENSATED','COMPENSATION_FAILED','ORPHANED'
    )),
    input_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    result_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    step_hash TEXT NOT NULL UNIQUE,
    PRIMARY KEY(operation_id,ordinal),
    FOREIGN KEY(operation_id)
        REFERENCES kis_functional_v2_graph_operation(operation_id)
);
"""

class KisDomesticFunctionalGraphBlocked(RuntimeError):
    pass


class KisDomesticFunctionalGraphInjectedCrash(RuntimeError):
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
        raise KisDomesticFunctionalGraphBlocked("graph-json-invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


SCHEMA_FINGERPRINT = _hash(
    {
        "descriptor": _SCHEMA_DESCRIPTOR,
        "canonicalSql": " ".join(_SCHEMA_SQL.strip().split()),
    }
)

V2_SCHEMA_FINGERPRINT = _hash(
    {
        "schemaVersion": _V2_SCHEMA_VERSION,
        "canonicalSql": " ".join(_V2_SCHEMA_SQL.strip().split()),
        "components": list(V2_COMPONENTS),
        "crashStages": list(V2_CRASH_STAGES),
        "productionAvailable": False,
    }
)
GRAPH_COMPONENT_PROTOCOL_HASH = _hash(
    {
        "schemaVersion": "kis-domestic-functional-graph-component-protocol/v2",
        "requestSchema": V2_REQUEST_SCHEMA,
        "statusSchema": V2_PORT_STATUS_SCHEMA,
        "receiptSchema": V2_PORT_RECEIPT_SCHEMA,
        "unionSchema": V2_VERIFY_SCHEMA,
        "schemaFingerprint": V2_SCHEMA_FINGERPRINT,
        "acceptedRegistryReadersBindingRequired": True,
        "marketSourceStatusPortRequired": True,
        "productionAvailable": False,
    }
)


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    return value


def _parse_time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid") from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    canonical = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if canonical != value:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-not-canonical-utc")
    return parsed.astimezone(timezone.utc)


def _time_text(value: datetime, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    converted = value.astimezone(timezone.utc)
    if not math.isfinite(converted.timestamp()):
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    return converted.isoformat().replace("+00:00", "Z")


def _positive_canonical_decimal(value: Any, label: str) -> Decimal:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid") from None
    if not parsed.is_finite() or parsed <= 0 or format(parsed, "f") != value:
        raise KisDomesticFunctionalGraphBlocked(f"{label}-invalid")
    return parsed


def _monotonic(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise KisDomesticFunctionalGraphBlocked("grant-monotonic-invalid")
    return value


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().rstrip(";").split())


def _schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): _normalize_sql(row[1])
        for row in conn.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE name LIKE 'kis_functional_graph_%' ORDER BY name"
        )
    }
    tables: dict[str, Any] = {}
    for name in sorted(
        key for key in objects if not key.endswith("_idx")
    ):
        tables[name] = {
            "tableInfo": [tuple(row) for row in conn.execute(f'PRAGMA table_info("{name}")')],
            "foreignKeys": [tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')],
            "indexes": [tuple(row) for row in conn.execute(f'PRAGMA index_list("{name}")')],
        }
        for index_row in tables[name]["indexes"]:
            index_name = str(index_row[1])
            tables[name][f"indexXInfo:{index_name}"] = [
                tuple(row) for row in conn.execute(f'PRAGMA index_xinfo("{index_name}")')
            ]
    return {"objects": objects, "tables": tables}


def _expected_schema_snapshot() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(_SCHEMA_SQL)
        return _schema_snapshot(conn)
    finally:
        conn.close()


_EXPECTED_SCHEMA_SNAPSHOT = _expected_schema_snapshot()


def _verify_schema(conn: sqlite3.Connection) -> None:
    if _schema_snapshot(conn) != _EXPECTED_SCHEMA_SNAPSHOT:
        raise KisDomesticFunctionalGraphBlocked("graph-schema-fingerprint-dirty")
    rows = [
        tuple(row)
        for row in conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_functional_graph_manifest"
        )
    ]
    if rows != [(1, SCHEMA_VERSION, SCHEMA_FINGERPRINT)]:
        raise KisDomesticFunctionalGraphBlocked("graph-schema-manifest-dirty")


_REGISTRY_COMPONENT_RESULT_KEYS = {
    "schemaVersion", "route", "pdno", "registryId", "registryEpoch",
    "manifestHash", "manifestFileHash", "rootKeyIdHash",
    "accountFingerprint", "credentialConfigurationHash", "codeManifestHash",
    "acceptedManifestHeadHash", "acceptanceRevision", "factoryBindingHash",
    "graphBindingHash", "clockGeneration", "componentBinding",
    "componentBindingHash", "asymmetricRootVerified",
    "durableAcceptanceVerified", "trustedWallMonotonicLineageVerified",
    "productionFactoryAuthorityPinned", "verifyOnly", "productionAvailable",
    "bindingResultHash",
}
_READERS_REGISTRY_BINDING_KEYS = {
    "schemaVersion", "route", "pdno", "component", "sourceFileHash",
    "protocolHash", "schemaFingerprint", "statusHash",
    "authorityKeyIdHash", "authorityPurpose", "signatureDomain",
}


def _verify_readers_registry_binding(
    registry: VerifyOnlyKeyRegistry | None,
    *,
    expected_code_hash: str,
) -> dict[str, Any]:
    if type(registry) is not VerifyOnlyKeyRegistry:
        raise KisDomesticFunctionalGraphBlocked(
            "v2-readers-accepted-registry-required"
        )
    try:
        result = registry.component_binding("readers")
        registry_status = registry.status()
        current_key_id = registry.active_key_id_for(
            "READERS_COMPONENT_VERIFY"
        )
    except BaseException as exc:
        raise KisDomesticFunctionalGraphBlocked(
            f"v2-readers-registry-binding-unavailable:{type(exc).__name__}"
        ) from None
    if not isinstance(result, Mapping) or set(result) != _REGISTRY_COMPONENT_RESULT_KEYS:
        raise KisDomesticFunctionalGraphBlocked(
            "v2-readers-registry-binding-result-not-exact"
        )
    binding = result.get("componentBinding")
    if not isinstance(binding, Mapping) or set(binding) != _READERS_REGISTRY_BINDING_KEYS:
        raise KisDomesticFunctionalGraphBlocked(
            "v2-readers-registry-entry-not-exact"
        )
    result_body = dict(result)
    result_hash = result_body.pop("bindingResultHash")
    current_status = readers_component_status()
    readers_path = Path(__file__).resolve().with_name(
        "kis_domestic_functional_readers.py"
    )
    actual_file_hash = hashlib.sha256(readers_path.read_bytes()).hexdigest()
    graph_file_hash = hashlib.sha256(Path(__file__).resolve().read_bytes()).hexdigest()
    expected_graph_binding = {
        "schemaVersion": GRAPH_BINDING_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "registryId": result["registryId"],
        "registryEpoch": result["registryEpoch"],
        "manifestHash": result["manifestHash"],
        "rootKeyIdHash": result["rootKeyIdHash"],
        "accountFingerprint": result["accountFingerprint"],
        "credentialConfigurationHash": result[
            "credentialConfigurationHash"
        ],
        "codeManifestHash": result["codeManifestHash"],
        "graphFileHash": graph_file_hash,
        "graphProtocolHash": GRAPH_COMPONENT_PROTOCOL_HASH,
        "graphSchemaFingerprint": V2_SCHEMA_FINGERPRINT,
    }
    exact_binding = {
        "schemaVersion": (
            "kis-domestic-functional-key-registry-component-binding/v1"
        ),
        "route": ROUTE,
        "pdno": PDNO,
        "component": "readers",
        "sourceFileHash": actual_file_hash,
        "protocolHash": READERS_COMPONENT_PROTOCOL_HASH,
        "schemaFingerprint": READERS_COMPONENT_SCHEMA_FINGERPRINT,
        "statusHash": current_status["statusHash"],
        "authorityKeyIdHash": current_key_id,
        "authorityPurpose": "READERS_COMPONENT_VERIFY",
        "signatureDomain": "KIS_DOMESTIC_FUNCTIONAL_READERS_COMPONENT",
    }
    if (
        dict(binding) != exact_binding
        or not hmac.compare_digest(expected_code_hash, actual_file_hash)
        or result.get("componentBindingHash") != _hash(exact_binding)
        or result_hash != _hash(result_body)
        or result.get("asymmetricRootVerified") is not True
        or result.get("durableAcceptanceVerified") is not True
        or result.get("trustedWallMonotonicLineageVerified") is not True
        or result.get("productionFactoryAuthorityPinned") is not True
        or result.get("verifyOnly") is not True
        or result.get("productionAvailable") is not False
        or result.get("graphBindingHash") != _hash(expected_graph_binding)
        or registry_status.get("manifestFresh") is not True
        or registry_status.get("durableAcceptanceVerified") is not True
        or registry_status.get("productionFactoryAuthorityPinned") is not True
        or registry_status.get("trustedWallMonotonicLineageVerified") is not True
    ):
        raise KisDomesticFunctionalGraphBlocked(
            "v2-readers-registry-binding-not-current-or-pinned"
        )
    return deepcopy(dict(result))


@dataclass(frozen=True)
class FrozenKisDomesticGraphLedgerPort:
    """Verify-only adapter around one frozen component's public CAS surface.

    The coordinator cannot manufacture component truth: status and mutation
    receipts must be independently accepted by the injected verifier.  The
    adapter is deliberately disabled/offline and exposes neither credentials
    nor a network sender.
    """

    component: str
    implementation_type: str
    code_hash: str
    protocol_hash: str
    status_reader: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    status_verifier: Callable[[Mapping[str, Any]], bool]
    cas_runner: Callable[
        [str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]],
        Mapping[str, Any]
    ]
    receipt_verifier: Callable[[Mapping[str, Any]], bool]
    compensator: Callable[
        [str, Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]],
        Mapping[str, Any]
    ] | None = None
    offline_simulation: bool = True
    key_registry: VerifyOnlyKeyRegistry | None = None

    def __post_init__(self) -> None:
        if self.component not in V2_COMPONENTS:
            raise KisDomesticFunctionalGraphBlocked("v2-port-component-invalid")
        expected_type = _V2_EXPECTED_IMPLEMENTATION_TYPES[self.component]
        offline_type = f"offline.{self.component}.frozen-ledger-port/v1"
        if self.implementation_type not in {expected_type, offline_type}:
            raise KisDomesticFunctionalGraphBlocked(
                "v2-port-implementation-type-not-pinned"
            )
        code_hash = _sha(self.code_hash, "v2-port-code-hash")
        if self.component == "readers":
            if type(self.key_registry) is not VerifyOnlyKeyRegistry:
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-readers-accepted-registry-required"
                )
            _verify_readers_registry_binding(
                self.key_registry, expected_code_hash=code_hash
            )
        else:
            if self.key_registry is not None:
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-non-readers-registry-forbidden"
                )
            if not hmac.compare_digest(
                code_hash, V2_FROZEN_COMPONENT_FILE_SHA256[self.component]
            ):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-port-code-pin-mismatch"
                )
        if not hmac.compare_digest(
            _sha(self.protocol_hash, "v2-port-protocol-hash"),
            V2_FROZEN_PROTOCOL_HASHES[self.component],
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-port-protocol-pin-mismatch"
            )
        if type(self.offline_simulation) is not bool:
            raise KisDomesticFunctionalGraphBlocked("v2-port-offline-flag-invalid")
        if self.offline_simulation != self.implementation_type.startswith("offline."):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-port-type-offline-flag-mismatch"
            )
        if not all(
            callable(value)
            for value in (
                self.status_reader,
                self.status_verifier,
                self.cas_runner,
                self.receipt_verifier,
            )
        ) or (self.compensator is not None and not callable(self.compensator)):
            raise KisDomesticFunctionalGraphBlocked("v2-port-callback-invalid")

    def binding(self) -> dict[str, Any]:
        body = {
            "component": self.component,
            "implementationType": self.implementation_type,
            "codeHash": self.code_hash,
            "protocolHash": self.protocol_hash,
            "verifyOnly": True,
            "offlineSimulation": self.offline_simulation,
            "productionAvailable": False,
        }
        if self.component == "readers":
            body["acceptedRegistryComponentBindingHash"] = (
                _verify_readers_registry_binding(
                    self.key_registry, expected_code_hash=self.code_hash
                )["componentBindingHash"]
            )
        return body

    def status(self, request: Mapping[str, Any]) -> dict[str, Any]:
        try:
            raw = self.status_reader(dict(request))
        except Exception as exc:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-read-failed:{type(exc).__name__}"
            ) from None
        if not isinstance(raw, Mapping) or set(raw) != _V2_STATUS_KEYS:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-not-exact"
            )
        value = dict(raw)
        status_hash = value.pop("statusHash")
        exact = {
            "schemaVersion": V2_PORT_STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "component": self.component,
            "implementationType": self.implementation_type,
            "codeHash": self.code_hash,
            "protocolHash": self.protocol_hash,
            "sessionId": request["sessionId"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request[
                "credentialConfigurationHash"
            ],
            "ownerEpoch": request["ownerEpoch"],
            "ownerEpochId": request["ownerEpochId"],
            "ownerEpochHash": request["ownerEpochHash"],
            "registryEpoch": request["registryEpoch"],
            "registryManifestHash": request["registryManifestHash"],
            "registryAcceptedHeadHash": request["registryAcceptedHeadHash"],
            "registryAcceptanceRevision": request[
                "registryAcceptanceRevision"
            ],
            "registryGraphBindingHash": request[
                "registryGraphBindingHash"
            ],
            "registryId": request["registryId"],
            "registryManifestFileHash": request["registryManifestFileHash"],
            "registryRootKeyIdHash": request["registryRootKeyIdHash"],
            "registryFactoryBindingHash": request[
                "registryFactoryBindingHash"
            ],
            "registryClockGeneration": request["registryClockGeneration"],
            "verifyOnly": True,
            "offlineSimulation": self.offline_simulation,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
        }
        if any(
            type(value.get(key)) is not type(expected)
            or value.get(key) != expected
            for key, expected in exact.items()
        ):
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-binding-mismatch"
            )
        if (
            type(value.get("statusRevision")) is not int
            or value["statusRevision"] < 1
            or type(value.get("preflightReady")) is not bool
            or type(value.get("exactCasAvailable")) is not bool
            or type(value.get("nativeGrantInstantAccepted")) is not bool
            or type(value.get("readinessBlockers")) is not list
            or value["readinessBlockers"] != sorted(set(value["readinessBlockers"]))
            or any(
                type(item) is not str or not _IDENTIFIER.fullmatch(item)
                for item in value["readinessBlockers"]
            )
        ):
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-values-invalid"
            )
        for key in ("statusHeadHash",):
            _sha(value[key], f"v2-{self.component}-{key}")
        if type(status_hash) is not str or not hmac.compare_digest(
            status_hash, _hash(value)
        ):
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-hash-invalid"
            )
        candidate = {**value, "statusHash": status_hash}
        try:
            verified = self.status_verifier(candidate)
        except Exception:
            verified = False
        if verified is not True:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-status-unverified"
            )
        return candidate

    def invoke(
        self,
        *,
        action: str,
        request: Mapping[str, Any],
        grant_instant: Mapping[str, Any],
        expectation: Mapping[str, Any],
        compensate: bool = False,
    ) -> dict[str, Any]:
        runner = self.compensator if compensate else self.cas_runner
        if runner is None:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-compensation-unavailable"
            )
        try:
            raw = runner(
                action,
                dict(request),
                dict(grant_instant),
                dict(expectation),
            )
        except Exception as exc:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-{action}-failed:{type(exc).__name__}"
            ) from None
        if not isinstance(raw, Mapping) or set(raw) != _V2_PORT_RAW_KEYS:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-{action}-receipt-not-exact"
            )
        value = dict(raw)
        exact = {
            "schemaVersion": V2_PORT_RECEIPT_SCHEMA,
            "component": self.component,
            "action": action,
            "operationId": request["operationId"],
            "sessionId": request["sessionId"],
            "requestHash": request["requestHash"],
            "actionInputsHash": request["actionInputsHash"],
            "grantWallAt": grant_instant["wallAt"],
            "grantMonotonicNs": grant_instant["monotonicNs"],
            "expectedStatusRevision": expectation["expectedStatusRevision"],
            "expectedStatusHeadHash": expectation[
                "expectedStatusHeadHash"
            ],
            "intentStepHash": expectation["intentStepHash"],
            "productionAvailable": False,
        }
        if any(
            type(value.get(key)) is not type(expected)
            or value.get(key) != expected
            for key, expected in exact.items()
        ):
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-{action}-receipt-binding-mismatch"
            )
        if (
            value.get("outcome") not in {
                "SUCCEEDED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED"
            }
            or type(value.get("result")) is not dict
            or type(value.get("singleUseBurned")) is not bool
            or type(value.get("exactCas")) is not bool
            or type(value.get("resultRevision")) is not int
            or value["resultRevision"] < value["expectedStatusRevision"]
        ):
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-{action}-receipt-values-invalid"
            )
        _sha(value.get("authoritativeReceiptHash"), "v2-authoritative-receipt")
        _sha(value.get("resultHeadHash"), "v2-result-head-hash")
        try:
            verified = self.receipt_verifier(dict(value))
        except Exception:
            verified = False
        if verified is not True or value["exactCas"] is not True:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{self.component}-{action}-receipt-unverified"
            )
        receipt_hash = _hash(value)
        return {**value, "receiptHash": receipt_hash}


def _v2_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): _normalize_sql(row[1])
        for row in conn.execute(
            "SELECT name,sql FROM sqlite_master "
            "WHERE name LIKE 'kis_functional_v2_graph_%' ORDER BY name"
        )
    }
    tables: dict[str, Any] = {}
    for name in sorted(
        key for key, sql in objects.items() if sql.startswith("CREATE TABLE")
    ):
        tables[name] = {
            "tableInfo": [
                tuple(row) for row in conn.execute(f'PRAGMA table_info("{name}")')
            ],
            "foreignKeys": [
                tuple(row)
                for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')
            ],
            "indexes": [
                tuple(row) for row in conn.execute(f'PRAGMA index_list("{name}")')
            ],
        }
        for index_row in tables[name]["indexes"]:
            index_name = str(index_row[1])
            tables[name][f"indexXInfo:{index_name}"] = [
                tuple(row)
                for row in conn.execute(f'PRAGMA index_xinfo("{index_name}")')
            ]
    return {"objects": objects, "tables": tables}


def _v2_expected_schema_snapshot() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(_V2_SCHEMA_SQL)
        return _v2_schema_snapshot(conn)
    finally:
        conn.close()


_V2_EXPECTED_SCHEMA_SNAPSHOT = _v2_expected_schema_snapshot()


def _verify_v2_schema(conn: sqlite3.Connection) -> None:
    if _v2_schema_snapshot(conn) != _V2_EXPECTED_SCHEMA_SNAPSHOT:
        raise KisDomesticFunctionalGraphBlocked("v2-graph-schema-dirty")
    rows = [
        tuple(row)
        for row in conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_functional_v2_graph_manifest"
        )
    ]
    if rows != [(1, _V2_SCHEMA_VERSION, V2_SCHEMA_FINGERPRINT)]:
        raise KisDomesticFunctionalGraphBlocked("v2-graph-manifest-dirty")


def _v2_request(raw: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _V2_REQUEST_KEYS:
        raise KisDomesticFunctionalGraphBlocked("v2-request-not-exact")
    value = dict(raw)
    request_hash = value.pop("requestHash")
    exact = {
        "schemaVersion": V2_REQUEST_SCHEMA,
        "productionAvailable": False,
    }
    if any(
        type(value.get(key)) is not type(expected)
        or value.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalGraphBlocked("v2-request-constant-mismatch")
    for key in (
        "operationId", "preallocationId", "sessionId", "publicArmId",
        "bootstrapId", "approvalId", "evaluationId", "triggerId",
        "sourceObservationId", "rollingSnapshotId", "quoteReceiptId",
        "capabilityId", "permitId", "ownerEpochId",
        "registryId", "registryClockGeneration",
    ):
        _identifier(value.get(key), f"v2-{key}")
    for key in (
        "publicArmHash", "bootstrapHash", "approvalHash", "evaluationHash",
        "triggerHash", "rollingSnapshotHash", "rollingTriggerEnvelopeHash",
        "quoteReceiptHash", "freshQuoteHash", "permitHash", "sessionNonceHash",
        "accountFingerprint", "credentialConfigurationHash",
        "preactivationBaselineHash", "contractEnvelopeHash",
        "codeManifestHash", "ownerEpochHash", "registryManifestHash",
        "registryAcceptedHeadHash", "registryGraphBindingHash",
        "registryManifestFileHash", "registryRootKeyIdHash",
        "registryFactoryBindingHash",
        "actionInputsHash",
    ):
        _sha(value.get(key), f"v2-{key}")
    if (
        type(value.get("sourceGeneration")) is not str
        or not _SOURCE_GENERATION.fullmatch(value["sourceGeneration"])
        or type(value.get("processGeneration")) is not str
        or not _PROCESS_GENERATION.fullmatch(value["processGeneration"])
        or type(value.get("socketGeneration")) is not str
        or not re.fullmatch(r"kis-(?:ws|socket)-generation-[0-9a-f]{32}", value["socketGeneration"])
    ):
        raise KisDomesticFunctionalGraphBlocked("v2-request-generation-invalid")
    for key in ("ownerEpoch", "registryEpoch", "registryAcceptanceRevision"):
        if type(value.get(key)) is not int or value[key] < 1:
            raise KisDomesticFunctionalGraphBlocked(f"v2-{key}-invalid")
    requested = _parse_time(value.get("requestedAt"), "v2-requested-at")
    quote_observed = _parse_time(
        value.get("freshQuoteObservedAt"), "v2-fresh-quote-observed-at"
    )
    quote_price = _positive_canonical_decimal(
        value.get("freshQuotePriceKrw"), "v2-fresh-quote-price"
    )
    buy_limit = _positive_canonical_decimal(
        value.get("naturalBuyLimitPriceKrw"), "v2-natural-buy-limit"
    )
    if (
        quote_price != buy_limit
        or buy_limit * ORDER_QUANTITY > MAX_ORDER_KRW
        or buy_limit * ORDER_QUANTITY > MAX_GROSS_KRW
        or requested < quote_observed
        or requested > quote_observed + timedelta(seconds=5)
    ):
        raise KisDomesticFunctionalGraphBlocked(
            "v2-fresh-quote-action-binding-invalid"
        )
    action_material = {
        "schemaVersion": "kis-domestic-functional-graph-v2-action-inputs/v1",
        **{
            key: value[key]
            for key in sorted(value)
            if key not in {"actionInputsHash"}
        },
    }
    if not hmac.compare_digest(
        value["actionInputsHash"], _hash(action_material)
    ):
        raise KisDomesticFunctionalGraphBlocked(
            "v2-action-inputs-hash-not-derived"
        )
    if type(request_hash) is not str or not hmac.compare_digest(
        request_hash, _hash(value)
    ):
        raise KisDomesticFunctionalGraphBlocked("v2-request-hash-invalid")
    return {**value, "requestHash": request_hash}


def _v2_port_bindings(ports: Mapping[str, FrozenKisDomesticGraphLedgerPort]) -> tuple[dict[str, Any], str]:
    if not isinstance(ports, Mapping) or set(ports) != set(V2_COMPONENTS):
        raise KisDomesticFunctionalGraphBlocked("v2-port-set-not-exact")
    if any(
        type(ports[name]) is not FrozenKisDomesticGraphLedgerPort
        or ports[name].component != name
        for name in V2_COMPONENTS
    ):
        raise KisDomesticFunctionalGraphBlocked("v2-port-type-or-name-mismatch")
    bindings = {name: ports[name].binding() for name in V2_COMPONENTS}
    return bindings, _hash(bindings)


_OWNER_LOCK = threading.RLock()
_LIVE_OWNERS: dict[str, str] = {}


class DurableKisDomesticFunctionalGraph:
    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        capture_signer: Callable[[str, Mapping[str, Any]], str],
        capture_verifier: Callable[[str, Mapping[str, Any], str], bool],
        server_authority_key_id: str,
        process_generation: str,
        wall_clock: Callable[[], datetime],
        monotonic_clock: Callable[[], int],
        owner_token_factory: Callable[[], bytes],
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalGraphBlocked("exact-program-ledger-required")
        if not callable(capture_signer) or not callable(capture_verifier):
            raise KisDomesticFunctionalGraphBlocked("graph-authority-callable-invalid")
        if (
            not callable(wall_clock)
            or not callable(monotonic_clock)
            or not callable(owner_token_factory)
        ):
            raise KisDomesticFunctionalGraphBlocked("graph-clock-or-owner-factory-invalid")
        if type(server_authority_key_id) is not str or len(server_authority_key_id) < 16:
            raise KisDomesticFunctionalGraphBlocked("graph-authority-key-id-invalid")
        if type(process_generation) is not str or not _PROCESS_GENERATION.fullmatch(process_generation):
            raise KisDomesticFunctionalGraphBlocked("graph-process-generation-invalid")
        token = owner_token_factory()
        if not isinstance(token, bytes) or len(token) < 32:
            raise KisDomesticFunctionalGraphBlocked("graph-owner-token-invalid")
        self.ledger = program_ledger
        self.signer = capture_signer
        self.verifier = capture_verifier
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self.process_generation = process_generation
        self.owner_token_hash = hashlib.sha256(token).hexdigest()
        self.authority_key_id_hash = hashlib.sha256(
            server_authority_key_id.encode("utf-8")
        ).hexdigest()
        self._owner_key = str(self.ledger.path.resolve())
        self._closed = False
        with _OWNER_LOCK:
            if self._owner_key in _LIVE_OWNERS:
                raise KisDomesticFunctionalGraphBlocked("graph-singleton-owner-active")
            _LIVE_OWNERS[self._owner_key] = self.owner_token_hash
        try:
            self._initialize_schema()
            self.startup_reconciled_session_ids = self._audit_restart()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        with _OWNER_LOCK:
            if _LIVE_OWNERS.get(self._owner_key) == self.owner_token_hash:
                del _LIVE_OWNERS[self._owner_key]
        self._closed = True

    def __enter__(self) -> "DurableKisDomesticFunctionalGraph":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _assert_owner(self) -> None:
        if self._closed:
            raise KisDomesticFunctionalGraphBlocked("graph-owner-closed")
        with _OWNER_LOCK:
            if _LIVE_OWNERS.get(self._owner_key) != self.owner_token_hash:
                raise KisDomesticFunctionalGraphBlocked("graph-owner-fence-lost")

    def _connect(self) -> sqlite3.Connection:
        conn = self.ledger.connect()
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize_schema(self) -> None:
        conn = self._connect()
        try:
            existing = conn.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'kis_functional_graph_%'"
            ).fetchall()
            if not existing:
                conn.executescript(_SCHEMA_SQL)
                conn.execute(
                    "INSERT INTO kis_functional_graph_manifest VALUES(1,?,?)",
                    (SCHEMA_VERSION, SCHEMA_FINGERPRINT),
                )
                conn.commit()
            _verify_schema(conn)
        finally:
            conn.close()

    def _now(self) -> datetime:
        return _parse_time(_time_text(self.wall_clock(), "grant-wall"), "grant-wall")

    def _mono(self) -> int:
        return _monotonic(self.monotonic_clock())

    def _sign(self, domain: str, body: Mapping[str, Any]) -> tuple[str, str, str]:
        digest = _hash(body)
        signed = {**body, "recordHash": digest}
        signature = self.signer(domain, signed)
        _sha(signature, f"{domain}-signature")
        try:
            valid = self.verifier(domain, signed, signature)
        except BaseException:
            valid = False
        if valid is not True:
            raise KisDomesticFunctionalGraphBlocked(f"{domain}-signature-invalid")
        return _canonical(body).decode("utf-8"), digest, signature

    def _verify_envelope(
        self, envelope_value: Mapping[str, Any]
    ) -> tuple[dict[str, Any], str, str]:
        if not isinstance(envelope_value, Mapping) or set(envelope_value) != {
            "body", "recordHash", "signature"
        }:
            raise KisDomesticFunctionalGraphBlocked("preallocation-envelope-not-exact")
        body = dict(envelope_value["body"])
        if set(body) != _PREALLOCATION_KEYS:
            raise KisDomesticFunctionalGraphBlocked("preallocation-fields-not-exact")
        record_hash = _sha(envelope_value.get("recordHash"), "preallocation-record-hash")
        signature = _sha(envelope_value.get("signature"), "preallocation-signature")
        if not hmac.compare_digest(record_hash, _hash(body)):
            raise KisDomesticFunctionalGraphBlocked("preallocation-record-hash-mismatch")
        try:
            valid = self.verifier(
                "GRAPH_PREALLOCATION",
                {**body, "recordHash": record_hash},
                signature,
            )
        except BaseException:
            valid = False
        if valid is not True:
            raise KisDomesticFunctionalGraphBlocked("preallocation-signature-invalid")
        return body, record_hash, signature

    def _validate_preallocation(self, body: Mapping[str, Any]) -> None:
        exact = {
            "schemaVersion": PREALLOCATION_SCHEMA,
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "quantity": ORDER_QUANTITY,
            "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
            "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
            "maxGrossSemantics": (
                "OWNER_ENTRY_CAPITAL_AT_RISK_NATURAL_BUY_NOTIONAL_PLUS_ENTRY_COSTS"
            ),
            "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
            "activeSeconds": ACTIVE_SECONDS,
            "accountAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "networkOrderPostAllowed": False,
            "promotionEligible": False,
        }
        if any(
            type(body.get(key)) is not type(value) or body.get(key) != value
            for key, value in exact.items()
        ):
            raise KisDomesticFunctionalGraphBlocked("preallocation-contract-mismatch")
        for key in (
            "preallocationId", "sessionId", "publicArmId", "bootstrapId",
            "approvalId", "evaluationId", "triggerId", "rollingSnapshotId",
            "permitId",
        ):
            _identifier(body.get(key), key)
        for key in (
            "publicArmHash", "bootstrapHash", "approvalHash", "evaluationHash",
            "triggerHash", "rollingSnapshotHash", "rollingReceiptHash",
            "rollingReceiptSignatureHash", "permitHash", "sessionNonceHash",
            "accountFingerprint", "preactivationBaselineHash",
            "contractEnvelopeHash", "codeManifestHash",
        ):
            _sha(body.get(key), key)
        if type(body.get("sourceGeneration")) is not str or not _SOURCE_GENERATION.fullmatch(body["sourceGeneration"]):
            raise KisDomesticFunctionalGraphBlocked("preallocation-source-generation-invalid")
        trigger_open = _parse_time(body.get("triggerBarOpenAt"), "trigger-bar-open")
        trigger_observed = _parse_time(body.get("triggerObservedAt"), "trigger-observed")
        rolling_completed = _parse_time(body.get("rollingCompletedAt"), "rolling-completed")
        rolling_expires = _parse_time(body.get("rollingExpiresAt"), "rolling-expires")
        preallocated = _parse_time(body.get("preallocatedAt"), "preallocated-at")
        if not (
            rolling_completed < trigger_open
            <= trigger_observed
            <= trigger_open + timedelta(seconds=2)
            and trigger_observed <= preallocated <= trigger_observed + timedelta(seconds=2)
            and preallocated <= rolling_expires
        ):
            raise KisDomesticFunctionalGraphBlocked("preallocation-causal-window-invalid")

    def preallocate(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        self._assert_owner()
        body, record_hash, signature = self._verify_envelope(envelope)
        self._validate_preallocation(body)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            conn.execute(
                "INSERT INTO kis_functional_graph_preallocation "
                "(preallocation_id,session_id,rolling_snapshot_id,state,record_json,"
                "record_hash,signature,revision) VALUES(?,?,?,'READY',?,?,?,1)",
                (
                    body["preallocationId"], body["sessionId"],
                    body["rollingSnapshotId"], _canonical(body).decode("utf-8"),
                    record_hash, signature,
                ),
            )
            conn.execute(
                "INSERT INTO kis_functional_graph_rolling_projection "
                "(rolling_snapshot_id,preallocation_id,state,rolling_snapshot_hash,"
                "rolling_receipt_hash,rolling_receipt_signature_hash,completed_at,"
                "expires_at,revision) VALUES(?,?,'READY',?,?,?,?,?,1)",
                (
                    body["rollingSnapshotId"], body["preallocationId"],
                    body["rollingSnapshotHash"], body["rollingReceiptHash"],
                    body["rollingReceiptSignatureHash"], body["rollingCompletedAt"],
                    body["rollingExpiresAt"],
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise KisDomesticFunctionalGraphBlocked("preallocation-not-unique") from exc
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return self.snapshot(body["sessionId"])

    @staticmethod
    def _inject(stage: str, injector: Callable[[str], None] | None) -> None:
        if injector is not None:
            injector(stage)

    def grant(
        self,
        *,
        preallocation_id: str,
        failure_injector: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        self._assert_owner()
        _identifier(preallocation_id, "preallocation-id")
        committed = False
        session_id = ""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_graph_preallocation "
                "WHERE preallocation_id=?", (preallocation_id,)
            ).fetchone()
            rolling = conn.execute(
                "SELECT * FROM kis_functional_graph_rolling_projection "
                "WHERE preallocation_id=?", (preallocation_id,)
            ).fetchone()
            if row is None or rolling is None or row["state"] != "READY" or rolling["state"] != "READY":
                raise KisDomesticFunctionalGraphBlocked("grant-preallocation-not-ready")
            body = json.loads(str(row["record_json"]))
            envelope = {
                "body": body,
                "recordHash": str(row["record_hash"]),
                "signature": str(row["signature"]),
            }
            verified, _record_hash, _signature_value = self._verify_envelope(envelope)
            self._validate_preallocation(verified)
            session_id = str(verified["sessionId"])
            if any(
                str(rolling[column]) != verified[field]
                for column, field in (
                    ("rolling_snapshot_id", "rollingSnapshotId"),
                    ("rolling_snapshot_hash", "rollingSnapshotHash"),
                    ("rolling_receipt_hash", "rollingReceiptHash"),
                    ("rolling_receipt_signature_hash", "rollingReceiptSignatureHash"),
                    ("completed_at", "rollingCompletedAt"),
                    ("expires_at", "rollingExpiresAt"),
                )
            ):
                raise KisDomesticFunctionalGraphBlocked("grant-rolling-row-mismatch")
            # Capture the grant's sole trusted wall/monotonic instant only
            # after the state-owned write transaction is fenced and all
            # immutable projections have been reverified.  Capturing before
            # BEGIN IMMEDIATE would silently backdate activation by lock wait.
            wall = self._now()
            mono = self._mono()
            trigger_observed = _parse_time(verified["triggerObservedAt"], "trigger-observed")
            rolling_expires = _parse_time(verified["rollingExpiresAt"], "rolling-expires")
            if not trigger_observed <= wall <= trigger_observed + timedelta(seconds=2) or wall > rolling_expires:
                raise KisDomesticFunctionalGraphBlocked("grant-trusted-instant-stale")
            self._inject("AFTER_VERIFY", failure_injector)

            activated_at = _time_text(wall, "activated-at")
            expires_at = _time_text(wall + timedelta(seconds=ACTIVE_SECONDS), "expires-at")
            cleanup_ends_at = _time_text(
                wall + timedelta(seconds=ACTIVE_SECONDS, minutes=15),
                "cleanup-ends-at",
            )
            capability_id = "kis-capability-" + _hash(
                {
                    "domain": "kis-domestic-functional-graph-capability-id/v1",
                    "sessionId": session_id,
                    "preallocationHash": str(row["record_hash"]),
                }
            )[:32]
            activation = {
                "schemaVersion": ACTIVATION_SCHEMA,
                "route": ROUTE,
                "origin": LIVE_ORIGIN,
                "pdno": PDNO,
                "sessionId": session_id,
                "capabilityId": capability_id,
                "preallocationId": preallocation_id,
                "preallocationHash": str(row["record_hash"]),
                "publicArmId": verified["publicArmId"],
                "publicArmHash": verified["publicArmHash"],
                "bootstrapId": verified["bootstrapId"],
                "bootstrapHash": verified["bootstrapHash"],
                "approvalId": verified["approvalId"],
                "approvalHash": verified["approvalHash"],
                "evaluationId": verified["evaluationId"],
                "evaluationHash": verified["evaluationHash"],
                "triggerId": verified["triggerId"],
                "triggerHash": verified["triggerHash"],
                "triggerBarOpenAt": verified["triggerBarOpenAt"],
                "triggerObservedAt": verified["triggerObservedAt"],
                "sourceGeneration": verified["sourceGeneration"],
                "rollingSnapshotId": verified["rollingSnapshotId"],
                "rollingSnapshotHash": verified["rollingSnapshotHash"],
                "rollingReceiptHash": verified["rollingReceiptHash"],
                "rollingReceiptSignatureHash": verified[
                    "rollingReceiptSignatureHash"
                ],
                "accountFingerprint": verified["accountFingerprint"],
                "permitId": verified["permitId"],
                "permitHash": verified["permitHash"],
                "sessionNonceHash": verified["sessionNonceHash"],
                "preactivationBaselineHash": verified["preactivationBaselineHash"],
                "contractEnvelopeHash": verified["contractEnvelopeHash"],
                "codeManifestHash": verified["codeManifestHash"],
                "activatedAt": activated_at,
                "activationObservedAt": activated_at,
                "activatedMonotonicNs": mono,
                "expiresAt": expires_at,
                "cleanupEndsAt": cleanup_ends_at,
                "activeSeconds": ACTIVE_SECONDS,
                "maxGrossKrw": verified["maxGrossKrw"],
                "maxGrossSemantics": verified["maxGrossSemantics"],
                "backdatedToTriggerBarOpen": False,
                "stateCapabilityOpen": True,
                "realOrdersEnabled": False,
                "networkOrderPostAllowed": False,
                "promotionEligible": False,
                "serverAuthorityKeyIdHash": self.authority_key_id_hash,
            }
            activation_json, activation_hash, activation_signature = self._sign(
                "GRAPH_ACTIVATION", activation
            )
            conn.execute(
                "INSERT INTO kis_functional_graph_grant "
                "(session_id,preallocation_id,state,owner_process_generation,"
                "owner_token_hash,account_fingerprint,activated_at,"
                "activated_monotonic_ns,expires_at,cleanup_ends_at,authority_open,"
                "activation_json,activation_hash,activation_signature,revision) "
                "VALUES(?,?,'ACTIVE',?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    session_id, preallocation_id, self.process_generation,
                    self.owner_token_hash, verified["accountFingerprint"], activated_at,
                    mono, expires_at, cleanup_ends_at, 1, activation_json,
                    activation_hash, activation_signature,
                ),
            )

            capability = {
                "schemaVersion": CAPABILITY_GRANT_SCHEMA,
                "route": ROUTE,
                "origin": LIVE_ORIGIN,
                "pdno": PDNO,
                "sessionId": session_id,
                "capabilityId": capability_id,
                "activationHash": activation_hash,
                "preallocationHash": str(row["record_hash"]),
                "permitHash": verified["permitHash"],
                "accountFingerprint": verified["accountFingerprint"],
                "ownerProcessGeneration": self.process_generation,
                "ownerTokenHash": self.owner_token_hash,
                "activatedAt": activated_at,
                "activatedMonotonicNs": mono,
                "expiresAt": expires_at,
                "stateCapabilityOpen": True,
                "accountAuthorityAvailable": False,
                "orderAuthorityAvailable": False,
                "networkOrderPostAllowed": False,
                "productionAvailable": False,
                "networkAvailable": False,
                "mutationAvailable": False,
            }
            capability_json, capability_hash, capability_signature = self._sign(
                "GRAPH_CAPABILITY_GRANT", capability
            )
            conn.execute(
                "INSERT INTO kis_functional_graph_capability "
                "(session_id,capability_id,state,authority_open,activated_at,"
                "expires_at,capability_json,capability_hash,capability_signature,"
                "revision) VALUES(?,?,'ACTIVE',1,?,?,?,?,?,1)",
                (
                    session_id,
                    capability_id,
                    activated_at,
                    expires_at,
                    capability_json,
                    capability_hash,
                    capability_signature,
                ),
            )
            self._inject("AFTER_ACTIVATION_INSERT", failure_injector)

            consumption = {
                "schemaVersion": ROLLING_CONSUMPTION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": session_id,
                "preallocationId": preallocation_id,
                "rollingSnapshotId": verified["rollingSnapshotId"],
                "rollingSnapshotHash": verified["rollingSnapshotHash"],
                "rollingReceiptHash": verified["rollingReceiptHash"],
                "rollingReceiptSignatureHash": verified[
                    "rollingReceiptSignatureHash"
                ],
                "evaluationId": verified["evaluationId"],
                "evaluationHash": verified["evaluationHash"],
                "triggerId": verified["triggerId"],
                "triggerHash": verified["triggerHash"],
                "accountFingerprint": verified["accountFingerprint"],
                "consumedAt": activated_at,
                "singleUseConsumed": True,
                "accountAuthorityAvailable": False,
                "orderAuthorityAvailable": False,
                "networkOrderPostAllowed": False,
            }
            consumption_json, consumption_hash, consumption_signature = self._sign(
                "GRAPH_ROLLING_CONSUMPTION", consumption
            )
            changed = conn.execute(
                "UPDATE kis_functional_graph_rolling_projection SET "
                "state='CONSUMED',consumed_at=?,consumption_json=?,consumption_hash=?,"
                "consumption_signature=?,revision=revision+1 "
                "WHERE preallocation_id=? AND state='READY' AND revision=1",
                (
                    activated_at, consumption_json, consumption_hash,
                    consumption_signature, preallocation_id,
                ),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalGraphBlocked("grant-rolling-consume-cas-failed")
            self._inject("AFTER_ROLLING_CONSUME", failure_injector)

            heartbeat = {
                "schemaVersion": HEARTBEAT_START_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": session_id,
                "activationHash": activation_hash,
                "processGeneration": self.process_generation,
                "sourceGeneration": verified["sourceGeneration"],
                "sequence": 1,
                "kind": "ACTIVE_START",
                "wallAt": activated_at,
                "monotonicNs": mono,
                "previousHash": "0" * 64,
                "productionAvailable": False,
                "networkAvailable": False,
                "mutationAvailable": False,
            }
            heartbeat_json, heartbeat_hash, heartbeat_signature = self._sign(
                "GRAPH_HEARTBEAT_START", heartbeat
            )
            conn.execute(
                "INSERT INTO kis_functional_graph_heartbeat_start "
                "(session_id,state,wall_at,monotonic_ns,sequence,sample_json,"
                "sample_hash,sample_signature,revision) "
                "VALUES(?,'ACTIVE',?,?,1,?,?,?,1)",
                (
                    session_id, activated_at, mono, heartbeat_json,
                    heartbeat_hash, heartbeat_signature,
                ),
            )
            self._inject("AFTER_HEARTBEAT_START", failure_injector)

            changed = conn.execute(
                "UPDATE kis_functional_graph_preallocation SET "
                "state='CONSUMED',revision=revision+1 "
                "WHERE preallocation_id=? AND state='READY' AND revision=1",
                (preallocation_id,),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalGraphBlocked("grant-preallocation-cas-failed")
            self._inject("BEFORE_COMMIT", failure_injector)
            conn.commit()
            committed = True
            self._inject("AFTER_COMMIT", failure_injector)
        except BaseException:
            if not committed:
                conn.rollback()
            else:
                self._compensate_committed_failure(session_id, "FAILURE_AFTER_COMMIT")
            raise
        finally:
            conn.close()
        return self.snapshot(session_id)

    def _failure_record(
        self, *, session_id: str, reason: str, occurred_at: datetime
    ) -> tuple[str, str, str]:
        body = {
            "schemaVersion": "kis-domestic-functional-graph-failure/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": session_id,
            "state": "RECONCILIATION_REQUIRED",
            "reason": reason,
            "occurredAt": _time_text(occurred_at, "failure-at"),
            "authorityOpen": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return self._sign("GRAPH_FAILURE", body)

    def _compensate_committed_failure(self, session_id: str, reason: str) -> None:
        now = self._now()
        failure_json, failure_hash, failure_signature = self._failure_record(
            session_id=session_id, reason=reason, occurred_at=now
        )
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE kis_functional_graph_grant SET "
                "state='RECONCILIATION_REQUIRED',authority_open=0,failure_json=?,"
                "failure_hash=?,failure_signature=?,revision=revision+1 "
                "WHERE session_id=? AND state='ACTIVE' AND authority_open=1",
                (
                    failure_json, failure_hash, failure_signature, session_id,
                ),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalGraphBlocked("graph-failure-compensation-cas-failed")
            capability_changed = conn.execute(
                "UPDATE kis_functional_graph_capability SET "
                "state='RECONCILIATION_REQUIRED',authority_open=0,"
                "revision=revision+1 WHERE session_id=? AND state='ACTIVE' "
                "AND authority_open=1",
                (session_id,),
            ).rowcount
            heartbeat_changed = conn.execute(
                "UPDATE kis_functional_graph_heartbeat_start SET "
                "state='RECONCILIATION_REQUIRED',revision=revision+1 "
                "WHERE session_id=? AND state='ACTIVE'", (session_id,),
            ).rowcount
            if capability_changed != 1 or heartbeat_changed != 1:
                raise KisDomesticFunctionalGraphBlocked(
                    "graph-failure-compensation-projection-mismatch"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _audit_restart(self) -> tuple[str, ...]:
        now = self._now()
        reconciled: list[str] = []
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_schema(conn)
            rows = conn.execute(
                "SELECT session_id FROM kis_functional_graph_grant "
                "WHERE state='ACTIVE' AND authority_open=1 ORDER BY session_id"
            ).fetchall()
            for row in rows:
                session_id = str(row["session_id"])
                failure_json, failure_hash, failure_signature = self._failure_record(
                    session_id=session_id,
                    reason="PROCESS_OR_SINGLETON_OWNER_RESTART",
                    occurred_at=now,
                )
                conn.execute(
                    "UPDATE kis_functional_graph_grant SET "
                    "state='RECONCILIATION_REQUIRED',authority_open=0,failure_json=?,"
                    "failure_hash=?,failure_signature=?,revision=revision+1 "
                    "WHERE session_id=? AND state='ACTIVE' AND authority_open=1",
                    (
                        failure_json, failure_hash, failure_signature, session_id,
                    ),
                )
                conn.execute(
                    "UPDATE kis_functional_graph_capability SET "
                    "state='RECONCILIATION_REQUIRED',authority_open=0,"
                    "revision=revision+1 WHERE session_id=? AND state='ACTIVE'",
                    (session_id,),
                )
                conn.execute(
                    "UPDATE kis_functional_graph_heartbeat_start SET "
                    "state='RECONCILIATION_REQUIRED',revision=revision+1 "
                    "WHERE session_id=? AND state='ACTIVE'", (session_id,),
                )
                reconciled.append(session_id)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return tuple(reconciled)

    def snapshot(self, session_id: str) -> dict[str, Any]:
        self._assert_owner()
        _identifier(session_id, "session-id")
        conn = self._connect()
        try:
            _verify_schema(conn)
            preallocation = conn.execute(
                "SELECT * FROM kis_functional_graph_preallocation WHERE session_id=?",
                (session_id,),
            ).fetchone()
            grant = conn.execute(
                "SELECT * FROM kis_functional_graph_grant WHERE session_id=?",
                (session_id,),
            ).fetchone()
            heartbeat = conn.execute(
                "SELECT * FROM kis_functional_graph_heartbeat_start WHERE session_id=?",
                (session_id,),
            ).fetchone()
            capability = conn.execute(
                "SELECT * FROM kis_functional_graph_capability WHERE session_id=?",
                (session_id,),
            ).fetchone()
            rolling = None
            if preallocation is not None:
                rolling = conn.execute(
                    "SELECT * FROM kis_functional_graph_rolling_projection "
                    "WHERE preallocation_id=?", (preallocation["preallocation_id"],),
                ).fetchone()
            return {
                "schemaVersion": "kis-domestic-functional-graph-snapshot/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": session_id,
                "preallocationState": (
                    None if preallocation is None else str(preallocation["state"])
                ),
                "rollingState": None if rolling is None else str(rolling["state"]),
                "grantState": None if grant is None else str(grant["state"]),
                "capabilityState": (
                    None if capability is None else str(capability["state"])
                ),
                "heartbeatState": None if heartbeat is None else str(heartbeat["state"]),
                "authorityOpen": bool(grant["authority_open"]) if grant is not None else False,
                "capabilityAuthorityOpen": (
                    bool(capability["authority_open"])
                    if capability is not None else False
                ),
                "activatedAt": None if grant is None else str(grant["activated_at"]),
                "activatedMonotonicNs": (
                    None if grant is None else int(grant["activated_monotonic_ns"])
                ),
                "networkOrderPostAllowed": False,
                "tradingMutationCount": 0,
                "productionAvailable": False,
            }
        finally:
            conn.close()


class DurableKisDomesticFunctionalGraphV2Coordinator:
    """Disabled all-or-fail-closed coordinator for frozen component ledgers.

    SQLite cannot atomically commit across the independent frozen ledgers.
    This coordinator therefore never claims cross-ledger atomicity.  It binds
    one captured wall/monotonic grant instant, serially executes exact CAS
    ports, burns one-use rolling/quote receipts, and compensates every opened
    lane/heartbeat/capability projection in reverse order on any failure.
    """

    _CAS_STEPS = (
        ("rolling", "CONSUME", "AFTER_ROLLING_CAS"),
        ("quote", "CONSUME", "AFTER_QUOTE_CAS"),
        ("lane", "ACTIVATE", "AFTER_LANE_CAS"),
        ("heartbeat", "START", "AFTER_HEARTBEAT_CAS"),
        ("capability", "MINT", "AFTER_CAPABILITY_CAS"),
    )
    _COMPENSATION_ACTION = {
        "capability": "BEGIN_RECONCILIATION",
        "heartbeat": "RECORD_STOP",
        "lane": "BEGIN_CLEANUP",
    }

    def __init__(
        self,
        *,
        program_ledger: ProgramLedger,
        ports: Mapping[str, FrozenKisDomesticGraphLedgerPort],
        wall_clock: Callable[[], datetime],
        monotonic_clock: Callable[[], int],
        lane_grant_authority_key: bytes,
        lane_grant_authority_key_id: str,
        key_registry: VerifyOnlyKeyRegistry,
    ) -> None:
        if type(program_ledger) is not ProgramLedger:
            raise KisDomesticFunctionalGraphBlocked(
                "v2-exact-program-ledger-required"
            )
        if not callable(wall_clock) or not callable(monotonic_clock):
            raise KisDomesticFunctionalGraphBlocked("v2-clock-invalid")
        if (
            type(key_registry) is not VerifyOnlyKeyRegistry
            or ports.get("readers") is None
            or ports["readers"].key_registry is not key_registry
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-exact-accepted-key-registry-required"
            )
        if (
            type(lane_grant_authority_key) is not bytes
            or len(lane_grant_authority_key) < 32
            or type(lane_grant_authority_key_id) is not str
            or not _IDENTIFIER.fullmatch(lane_grant_authority_key_id)
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-grant-authority-invalid"
            )
        bindings, bindings_hash = _v2_port_bindings(ports)
        lane_key_id_hash = hashlib.sha256(
            lane_grant_authority_key_id.encode("utf-8")
        ).hexdigest()
        bindings = {
            **bindings,
            "laneGrantReceiptAuthority": {
                "schemaVersion": "kis-domestic-functional-graph-v2-lane-grant-authority/v1",
                "authorityKeyIdHash": lane_key_id_hash,
                "signingImplementation": (
                    "live_trader.kis_domestic_functional_lane."
                    "sign_kis_domestic_lane_grant_receipt"
                ),
                "productionAvailable": False,
            },
        }
        bindings_hash = _hash(bindings)
        self.ledger = program_ledger
        self.ports = dict(ports)
        self.key_registry = key_registry
        self.port_bindings = bindings
        self.port_bindings_hash = bindings_hash
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self._lane_grant_authority_key = lane_grant_authority_key
        self._lane_grant_authority_key_id_hash = lane_key_id_hash
        self._lock = threading.RLock()
        self._initialize_v2_schema()
        self.startup_orphaned_operations = self._audit_v2_startup()

    def _lane_grant_body(
        self,
        *,
        request: Mapping[str, Any],
        grant_instant: Mapping[str, Any],
        expectation: Mapping[str, Any],
    ) -> dict[str, Any]:
        body = {
            "schemaVersion": "kis-domestic-functional-lane-grant-instant/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "source": "KIS_DOMESTIC_FUNCTIONAL_GRAPH_V2",
            "graphTransactionId": request["operationId"],
            "graphRequestHash": request["requestHash"],
            "graphActionInputsHash": request["actionInputsHash"],
            "graphIntentStepHash": expectation["intentStepHash"],
            "expectedStatusRevision": expectation["expectedStatusRevision"],
            "expectedStatusHeadHash": expectation["expectedStatusHeadHash"],
            "ownerEpochHash": request["ownerEpochHash"],
            "registryAcceptedHeadHash": request["registryAcceptedHeadHash"],
            "sessionId": request["sessionId"],
            "bootstrapId": request["bootstrapId"],
            "approvalId": request["approvalId"],
            "evaluationId": request["evaluationId"],
            "triggerId": request["triggerId"],
            "triggerHash": request["triggerHash"],
            "accountFingerprint": request["accountFingerprint"],
            "preactivationBaselineHash": request["preactivationBaselineHash"],
            "codeManifestHash": request["codeManifestHash"],
            "rollingReceiptHash": request["rollingTriggerEnvelopeHash"],
            "quoteReceiptHash": request["quoteReceiptHash"],
            "freshQuoteHash": request["freshQuoteHash"],
            "grantWallAt": grant_instant["wallAt"],
            "grantMonotonicNs": grant_instant["monotonicNs"],
            "capturedOnce": True,
            "serverAuthorityKeyIdHash": self._lane_grant_authority_key_id_hash,
        }
        if set(body) != _V2_LANE_GRANT_BODY_KEYS:
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-grant-body-not-exact"
            )
        return body

    def _issue_lane_grant_context(
        self,
        *,
        request: Mapping[str, Any],
        grant_instant: Mapping[str, Any],
        expectation: Mapping[str, Any],
    ) -> dict[str, Any]:
        body = self._lane_grant_body(
            request=request,
            grant_instant=grant_instant,
            expectation=expectation,
        )
        receipt = sign_kis_domestic_lane_grant_receipt(
            self._lane_grant_authority_key, body
        )
        self._verify_lane_grant_receipt(receipt, expected_body=body)
        arguments = {
            "bootstrap_id": request["bootstrapId"],
            "approval_id": request["approvalId"],
            "evaluation_id": request["evaluationId"],
            "trigger_id": request["triggerId"],
            "session_id": request["sessionId"],
            "fresh_quote_hash": request["freshQuoteHash"],
            "fresh_quote_observed_at": request["freshQuoteObservedAt"],
            "fresh_quote_price_krw": request["freshQuotePriceKrw"],
            "natural_buy_limit_price_krw": request["naturalBuyLimitPriceKrw"],
            "graph_grant_instant_receipt": receipt,
        }
        if set(arguments) != _V2_LANE_ACTIVATION_ARGUMENT_KEYS:
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-activation-arguments-not-exact"
            )
        return {**grant_instant, "laneActivationArguments": arguments}

    def _verify_lane_grant_receipt(
        self,
        receipt: Any,
        *,
        expected_body: Mapping[str, Any],
    ) -> dict[str, Any]:
        if (
            not isinstance(receipt, Mapping)
            or set(receipt) != {"body", "recordHash", "signature"}
            or not isinstance(receipt.get("body"), Mapping)
            or set(receipt["body"]) != _V2_LANE_GRANT_BODY_KEYS
            or dict(receipt["body"]) != dict(expected_body)
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-grant-receipt-not-exact"
            )
        expected = sign_kis_domestic_lane_grant_receipt(
            self._lane_grant_authority_key, dict(expected_body)
        )
        if not hmac.compare_digest(_canonical(dict(receipt)), _canonical(expected)):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-grant-receipt-unverified"
            )
        return dict(receipt)

    def _verify_lane_activation_result(
        self,
        result: Any,
        *,
        request: Mapping[str, Any],
        grant_instant: Mapping[str, Any],
        expectation: Mapping[str, Any],
    ) -> None:
        if not isinstance(result, Mapping) or set(result) != _V2_LANE_RESULT_KEYS:
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-activation-result-not-exact"
            )
        expected_body = self._lane_grant_body(
            request=request,
            grant_instant=grant_instant,
            expectation=expectation,
        )
        receipt = self._verify_lane_grant_receipt(
            result.get("laneGrantInstantReceipt"), expected_body=expected_body
        )
        activated = _parse_time(result.get("activatedAt"), "v2-lane-activated-at")
        observed = _parse_time(
            result.get("activationObservedAt"), "v2-lane-activation-observed-at"
        )
        expires = _parse_time(result.get("expiresAt"), "v2-lane-expires-at")
        if (
            result.get("schemaVersion")
            != "kis-domestic-functional-activation/v2"
            or result.get("sessionId") != request["sessionId"]
            or result.get("state") != "ACTIVE"
            or result.get("evaluationId") != request["evaluationId"]
            or result.get("triggerId") != request["triggerId"]
            or result.get("grantMonotonicNs") != grant_instant["monotonicNs"]
            or result.get("grantReceiptHash") != receipt["recordHash"]
            or result.get("activeSeconds") != ACTIVE_SECONDS
            or result.get("realOrdersEnabled") is not False
            or result.get("promotionEligible") is not False
            or activated != _parse_time(grant_instant["wallAt"], "v2-grant-wall")
            or observed != activated
            or expires - activated != timedelta(seconds=ACTIVE_SECONDS)
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-lane-native-grant-result-binding-invalid"
            )
        _identifier(result.get("naturalBuyClaimId"), "v2-natural-buy-claim-id")
        for key in (
            "activationRecordHash", "naturalBuyClaimHash", "grantReceiptHash"
        ):
            _sha(result.get(key), f"v2-lane-{key}")

    def _connect_v2(self) -> sqlite3.Connection:
        conn = self.ledger.connect()
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initialize_v2_schema(self) -> None:
        conn = self._connect_v2()
        try:
            existing = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE name LIKE 'kis_functional_v2_graph_%'"
            ).fetchall()
            if not existing:
                conn.executescript(_V2_SCHEMA_SQL)
                conn.execute(
                    "INSERT INTO kis_functional_v2_graph_manifest VALUES(1,?,?)",
                    (_V2_SCHEMA_VERSION, V2_SCHEMA_FINGERPRINT),
                )
                conn.commit()
            _verify_v2_schema(conn)
        finally:
            conn.close()

    def _capture_instant(self) -> dict[str, Any]:
        wall = self.wall_clock()
        wall_text = _time_text(wall, "v2-grant-wall")
        monotonic_ns = _monotonic(self.monotonic_clock())
        return {
            "schemaVersion": "kis-domestic-functional-graph-v2-grant-instant/v1",
            "wallAt": wall_text,
            "monotonicNs": monotonic_ns,
            "productionAvailable": False,
        }

    def _statuses(
        self,
        request: Mapping[str, Any],
        *,
        require_ready: bool,
    ) -> tuple[dict[str, dict[str, Any]], str]:
        values = {
            name: self.ports[name].status(request) for name in V2_COMPONENTS
        }
        if require_ready:
            for name, value in values.items():
                if (
                    value["preflightReady"] is not True
                    or value["exactCasAvailable"] is not True
                    or value["readinessBlockers"]
                ):
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-{name}-preflight-hold"
                    )
            for name in ("lane", "heartbeat"):
                if values[name]["nativeGrantInstantAccepted"] is not True:
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-{name}-native-grant-instant-not-accepted"
                    )
            if not all(value["offlineSimulation"] is True for value in values.values()):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-production-components-remain-disabled"
                )
        return values, _hash(values)

    def _fresh_owner_registry_guard(
        self,
        request: Mapping[str, Any],
    ) -> str:
        owner = self.ports["owner"].status(request)
        registry = self.ports["key_registry"].status(request)
        readers_binding_hash = self._fresh_readers_registry_guard(request)
        if (
            owner["preflightReady"] is not True
            or registry["preflightReady"] is not True
            or owner["ownerEpoch"] != request["ownerEpoch"]
            or owner["ownerEpochId"] != request["ownerEpochId"]
            or owner["ownerEpochHash"] != request["ownerEpochHash"]
            or registry["registryEpoch"] != request["registryEpoch"]
            or registry["registryManifestHash"]
            != request["registryManifestHash"]
            or registry["registryAcceptedHeadHash"]
            != request["registryAcceptedHeadHash"]
            or registry["registryAcceptanceRevision"]
            != request["registryAcceptanceRevision"]
            or registry["registryGraphBindingHash"]
            != request["registryGraphBindingHash"]
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-owner-or-registry-fence-changed"
            )
        return _hash(
            {
                "owner": owner,
                "keyRegistry": registry,
                "readersRegistryBindingHash": readers_binding_hash,
            }
        )

    def _fresh_readers_registry_guard(
        self, request: Mapping[str, Any]
    ) -> str:
        result = _verify_readers_registry_binding(
            self.key_registry,
            expected_code_hash=self.ports["readers"].code_hash,
        )
        exact = {
            "registryId": request["registryId"],
            "registryEpoch": request["registryEpoch"],
            "manifestHash": request["registryManifestHash"],
            "manifestFileHash": request["registryManifestFileHash"],
            "rootKeyIdHash": request["registryRootKeyIdHash"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request[
                "credentialConfigurationHash"
            ],
            "codeManifestHash": request["codeManifestHash"],
            "acceptedManifestHeadHash": request[
                "registryAcceptedHeadHash"
            ],
            "acceptanceRevision": request["registryAcceptanceRevision"],
            "factoryBindingHash": request["registryFactoryBindingHash"],
            "graphBindingHash": request["registryGraphBindingHash"],
            "clockGeneration": request["registryClockGeneration"],
        }
        if any(
            type(result.get(key)) is not type(value)
            or result.get(key) != value
            for key, value in exact.items()
        ):
            raise KisDomesticFunctionalGraphBlocked(
                "v2-readers-registry-request-binding-mismatch"
            )
        return str(result["componentBindingHash"])

    def _assert_no_account_hazard(
        self,
        request: Mapping[str, Any],
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        owned_connection = conn is None
        current = conn or self._connect_v2()
        try:
            rows = current.execute(
                "SELECT operation_id,request_json,state,hazardous_authority_open "
                "FROM kis_functional_v2_graph_operation WHERE "
                "state='RECONCILIATION_REQUIRED' OR "
                "hazardous_authority_open=1 ORDER BY operation_id"
            ).fetchall()
            for row in rows:
                try:
                    prior = json.loads(str(row["request_json"]))
                    prior = _v2_request(prior)
                except (TypeError, json.JSONDecodeError, KisDomesticFunctionalGraphBlocked):
                    raise KisDomesticFunctionalGraphBlocked(
                        "v2-prior-hazard-request-unreadable"
                    ) from None
                if prior.get("accountFingerprint") == request["accountFingerprint"]:
                    raise KisDomesticFunctionalGraphBlocked(
                        "v2-account-has-unresolved-reconciliation"
                    )
        finally:
            if owned_connection:
                current.close()

    def _prepare_port_intent(
        self,
        *,
        request: Mapping[str, Any],
        component: str,
        action: str,
        grant_instant: Mapping[str, Any],
        compensate: bool,
    ) -> dict[str, Any]:
        fresh = self.ports[component].status(request)
        if fresh["ownerEpoch"] != request["ownerEpoch"] or fresh[
            "registryAcceptedHeadHash"
        ] != request["registryAcceptedHeadHash"]:
            raise KisDomesticFunctionalGraphBlocked(
                f"v2-{component}-fresh-status-fence-changed"
            )
        body = {
            "schemaVersion": "kis-domestic-functional-graph-v2-port-intent/v1",
            "operationId": request["operationId"],
            "sessionId": request["sessionId"],
            "component": component,
            "action": action,
            "requestHash": request["requestHash"],
            "actionInputsHash": request["actionInputsHash"],
            "grantWallAt": grant_instant["wallAt"],
            "grantMonotonicNs": grant_instant["monotonicNs"],
            "expectedStatusRevision": fresh["statusRevision"],
            "expectedStatusHeadHash": fresh["statusHeadHash"],
            "compensation": compensate,
            "productionAvailable": False,
        }
        conn = self._connect_v2()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_v2_schema(conn)
            step_hash = self._append_step_conn(
                conn,
                operation_id=request["operationId"],
                component=component,
                action=f"INTENT_{action}",
                outcome="INTENT",
                input_hash=request["actionInputsHash"],
                result=body,
                occurred_at=_time_text(self.wall_clock(), "v2-intent-wall"),
            )
            assignments = ["hazardous_authority_open=1"]
            if not compensate and component == "rolling":
                assignments.append("burned_rolling=1")
            if not compensate and component == "quote":
                assignments.append("burned_quote=1")
            conn.execute(
                "UPDATE kis_functional_v2_graph_operation SET "
                + ",".join(assignments)
                + " WHERE operation_id=?",
                (request["operationId"],),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {
            **body,
            "intentStepHash": step_hash,
        }

    @staticmethod
    def _inject(stage: str, injector: Callable[[str], None] | None) -> None:
        if injector is not None:
            injector(stage)

    def _append_step_conn(
        self,
        conn: sqlite3.Connection,
        *,
        operation_id: str,
        component: str,
        action: str,
        outcome: str,
        input_hash: str,
        result: Mapping[str, Any],
        occurred_at: str,
    ) -> str:
        operation = conn.execute(
            "SELECT step_count,step_head_hash FROM "
            "kis_functional_v2_graph_operation WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if operation is None:
            raise KisDomesticFunctionalGraphBlocked("v2-operation-missing")
        ordinal = int(operation["step_count"]) + 1
        previous = str(operation["step_head_hash"])
        result_value = dict(result)
        result_hash = _hash(result_value)
        body = {
            "schemaVersion": V2_STEP_SCHEMA,
            "operationId": operation_id,
            "ordinal": ordinal,
            "component": component,
            "action": action,
            "outcome": outcome,
            "inputHash": _sha(input_hash, "v2-step-input-hash"),
            "resultHash": result_hash,
            "occurredAt": _time_text(
                _parse_time(occurred_at, "v2-step-occurred-at"),
                "v2-step-occurred-at",
            ),
            "previousHash": previous,
        }
        step_hash = _hash(body)
        conn.execute(
            "INSERT INTO kis_functional_v2_graph_step "
            "(operation_id,ordinal,component,action,outcome,input_hash,result_json,"
            "result_hash,occurred_at,previous_hash,step_hash) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                operation_id, ordinal, component, action, outcome,
                input_hash, _canonical(result_value).decode("utf-8"), result_hash,
                body["occurredAt"], previous, step_hash,
            ),
        )
        changed = conn.execute(
            "UPDATE kis_functional_v2_graph_operation SET "
            "step_count=?,step_head_hash=?,revision=revision+1 "
            "WHERE operation_id=? AND step_count=? AND step_head_hash=?",
            (ordinal, step_hash, operation_id, ordinal - 1, previous),
        ).rowcount
        if changed != 1:
            raise KisDomesticFunctionalGraphBlocked("v2-step-head-cas-failed")
        return step_hash

    def _record_step(
        self,
        *,
        request: Mapping[str, Any],
        component: str,
        action: str,
        receipt: Mapping[str, Any],
        outcome: str,
        flags: Mapping[str, int] | None = None,
    ) -> None:
        conn = self._connect_v2()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_v2_schema(conn)
            self._append_step_conn(
                conn,
                operation_id=request["operationId"],
                component=component,
                action=action,
                outcome=outcome,
                input_hash=request["actionInputsHash"],
                result=receipt,
                occurred_at=_time_text(
                    self.wall_clock(), "v2-step-wall"
                ),
            )
            if flags:
                allowed = {
                    "burned_rolling", "burned_quote", "lane_active",
                    "heartbeat_active", "capability_active",
                    "hazardous_authority_open",
                }
                if set(flags) - allowed or any(value not in {0, 1} for value in flags.values()):
                    raise KisDomesticFunctionalGraphBlocked("v2-step-flags-invalid")
                assignments = ",".join(f"{name}=?" for name in flags)
                conn.execute(
                    f"UPDATE kis_functional_v2_graph_operation SET {assignments} "
                    "WHERE operation_id=?",
                    (*flags.values(), request["operationId"]),
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _operation_row(self, operation_id: str) -> sqlite3.Row:
        conn = self._connect_v2()
        try:
            row = conn.execute(
                "SELECT * FROM kis_functional_v2_graph_operation "
                "WHERE operation_id=?", (operation_id,),
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalGraphBlocked("v2-operation-missing")
            return row
        finally:
            conn.close()

    def _terminalize(
        self,
        *,
        operation_id: str,
        state: str,
        reason: str,
        hazardous: bool,
    ) -> None:
        conn = self._connect_v2()
        try:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE kis_functional_v2_graph_operation SET state=?,"
                "failure_reason=?,hazardous_authority_open=?,revision=revision+1 "
                "WHERE operation_id=? AND state IN ('PREPARED','APPLYING')",
                (state, reason, int(hazardous), operation_id),
            ).rowcount
            if changed != 1:
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-operation-terminal-cas-failed"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _compensate(
        self,
        *,
        request: Mapping[str, Any],
        grant_instant: Mapping[str, Any],
        succeeded: list[str],
        reason: str,
    ) -> tuple[str, bool]:
        compensation_failed = False
        for component in reversed(succeeded):
            action = self._COMPENSATION_ACTION.get(component)
            if action is None:
                continue
            try:
                self._fresh_owner_registry_guard(request)
                expectation = self._prepare_port_intent(
                    request=request,
                    component=component,
                    action=action,
                    grant_instant=grant_instant,
                    compensate=True,
                )
                receipt = self.ports[component].invoke(
                    action=action,
                    request=request,
                    grant_instant=grant_instant,
                    expectation=expectation,
                    compensate=True,
                )
                if receipt["outcome"] != "COMPENSATED":
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-{component}-compensation-not-acknowledged"
                    )
                before = self._operation_row(request["operationId"])
                remaining_open = any(
                    bool(before[f"{name}_active"])
                    for name in ("lane", "heartbeat", "capability")
                    if name != component
                )
                flags = {
                    f"{component}_active": 0,
                    "hazardous_authority_open": int(remaining_open),
                }
                self._record_step(
                    request=request,
                    component=component,
                    action=action,
                    receipt=receipt,
                    outcome="COMPENSATED",
                    flags=flags,
                )
            except BaseException as exc:
                compensation_failed = True
                internal = {
                    "schemaVersion": "kis-domestic-functional-graph-v2-compensation-failure/v1",
                    "component": component,
                    "action": action,
                    "errorType": type(exc).__name__,
                    "productionAvailable": False,
                }
                self._record_step(
                    request=request,
                    component=component,
                    action=action,
                    receipt=internal,
                    outcome="COMPENSATION_FAILED",
                    flags={"hazardous_authority_open": 1},
                )
        row = self._operation_row(request["operationId"])
        open_projection = bool(
            row["lane_active"]
            or row["heartbeat_active"]
            or row["capability_active"]
        )
        terminal = (
            "RECONCILIATION_REQUIRED"
            if compensation_failed or open_projection
            else "SAFE_INCOMPLETE"
        )
        hazardous = compensation_failed or open_projection
        self._terminalize(
            operation_id=request["operationId"],
            state=terminal,
            reason=reason,
            hazardous=hazardous,
        )
        return terminal, hazardous

    def execute(
        self,
        *,
        request: Mapping[str, Any],
        failure_injector: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        value = _v2_request(request)
        with self._lock:
            self._assert_no_account_hazard(value)
            statuses, statuses_hash = self._statuses(value, require_ready=True)
            self._fresh_owner_registry_guard(value)
            grant_instant = self._capture_instant()
            requested_at = _parse_time(value["requestedAt"], "v2-requested-at")
            captured = _parse_time(grant_instant["wallAt"], "v2-grant-wall")
            if captured < requested_at or captured > requested_at + timedelta(seconds=2):
                raise KisDomesticFunctionalGraphBlocked("v2-grant-instant-stale")
            conn = self._connect_v2()
            try:
                conn.execute("BEGIN IMMEDIATE")
                _verify_v2_schema(conn)
                self._assert_no_account_hazard(value, conn=conn)
                conn.execute(
                    "INSERT INTO kis_functional_v2_graph_operation "
                    "(operation_id,session_id,state,request_json,request_hash,"
                    "component_statuses_json,component_statuses_hash,port_bindings_hash,"
                    "captured_wall_at,captured_monotonic_ns,owner_epoch,owner_epoch_hash,"
                    "registry_accepted_head_hash,burned_rolling,burned_quote,lane_active,"
                    "heartbeat_active,capability_active,hazardous_authority_open,"
                    "step_count,step_head_hash,revision) "
                    "VALUES(?,?,'PREPARED',?,?,?,?,?,?,?,?,?,?,0,0,0,0,0,0,0,?,1)",
                    (
                        value["operationId"], value["sessionId"],
                        _canonical(value).decode("utf-8"), value["requestHash"],
                        _canonical(statuses).decode("utf-8"), statuses_hash,
                        self.port_bindings_hash, grant_instant["wallAt"],
                        grant_instant["monotonicNs"], value["ownerEpoch"],
                        value["ownerEpochHash"], value["registryAcceptedHeadHash"],
                        "0" * 64,
                    ),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-operation-or-session-already-burned"
                ) from exc
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
            try:
                self._inject("AFTER_PREPARE", failure_injector)
            except BaseException:
                self._terminalize(
                    operation_id=value["operationId"],
                    state="ABORTED",
                    reason="FAILURE_AFTER_PREPARE_POST_ZERO",
                    hazardous=False,
                )
                raise
            conn = self._connect_v2()
            try:
                conn.execute("BEGIN IMMEDIATE")
                changed = conn.execute(
                    "UPDATE kis_functional_v2_graph_operation SET "
                    "state='APPLYING',revision=revision+1 "
                    "WHERE operation_id=? AND state='PREPARED'",
                    (value["operationId"],),
                ).rowcount
                if changed != 1:
                    raise KisDomesticFunctionalGraphBlocked(
                        "v2-operation-apply-cas-failed"
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()

            succeeded: list[str] = []
            attempted_component = ""
            try:
                for component, action, crash_stage in self._CAS_STEPS:
                    attempted_component = component
                    self._fresh_owner_registry_guard(value)
                    expectation = self._prepare_port_intent(
                        request=value,
                        component=component,
                        action=action,
                        grant_instant=grant_instant,
                        compensate=False,
                    )
                    invocation_instant = grant_instant
                    if component == "lane":
                        invocation_instant = self._issue_lane_grant_context(
                            request=value,
                            grant_instant=grant_instant,
                            expectation=expectation,
                        )
                    receipt = self.ports[component].invoke(
                        action=action,
                        request=value,
                        grant_instant=invocation_instant,
                        expectation=expectation,
                    )
                    try:
                        self._inject(
                            f"AFTER_{component.upper()}_PORT_RETURN_BEFORE_RECORD",
                            failure_injector,
                        )
                    except KisDomesticFunctionalGraphInjectedCrash:
                        # The component CAS may have committed while this
                        # coordinator has only a durable INTENT.  A restarted
                        # owner must reconcile it; this process may not invent
                        # either success or compensation evidence.
                        raise
                    if receipt["outcome"] != "SUCCEEDED":
                        raise KisDomesticFunctionalGraphBlocked(
                            f"v2-{component}-{action}-not-acknowledged"
                        )
                    if component == "lane":
                        self._verify_lane_activation_result(
                            receipt["result"],
                            request=value,
                            grant_instant=grant_instant,
                            expectation=expectation,
                        )
                    elif component == "heartbeat":
                        result = receipt["result"]
                        if result.get("activatedAt") != grant_instant["wallAt"]:
                            raise KisDomesticFunctionalGraphBlocked(
                                f"v2-{component}-grant-instant-backdated"
                            )
                    flags: dict[str, int] = {}
                    if component == "rolling":
                        if receipt["singleUseBurned"] is not True:
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-rolling-not-burned-after-consume"
                            )
                        flags["burned_rolling"] = 1
                    elif component == "quote":
                        if receipt["singleUseBurned"] is not True:
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-quote-not-burned-after-consume"
                            )
                        flags["burned_quote"] = 1
                    elif component in {"lane", "heartbeat", "capability"}:
                        flags[f"{component}_active"] = 1
                    flags["hazardous_authority_open"] = 0
                    self._record_step(
                        request=value,
                        component=component,
                        action=action,
                        receipt=receipt,
                        outcome="SUCCEEDED",
                        flags=flags,
                    )
                    succeeded.append(component)
                    attempted_component = ""
                    self._inject(crash_stage, failure_injector)
                self._inject("BEFORE_FINALIZE", failure_injector)
                conn = self._connect_v2()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    changed = conn.execute(
                        "UPDATE kis_functional_v2_graph_operation SET "
                        "state='ACTIVE_OFFLINE_SIMULATION',revision=revision+1 "
                        "WHERE operation_id=? AND state='APPLYING' AND "
                        "burned_rolling=1 AND burned_quote=1 AND lane_active=1 AND "
                        "heartbeat_active=1 AND capability_active=1 AND "
                        "hazardous_authority_open=0",
                        (value["operationId"],),
                    ).rowcount
                    if changed != 1:
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-finalize-cas-failed"
                        )
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
            except BaseException as exc:
                if (
                    isinstance(exc, KisDomesticFunctionalGraphInjectedCrash)
                    and attempted_component
                ):
                    raise
                compensation_targets = list(succeeded)
                if attempted_component:
                    failure = {
                        "schemaVersion": "kis-domestic-functional-graph-v2-cas-failure/v1",
                        "component": attempted_component,
                        "errorType": type(exc).__name__,
                        "mutationMayHaveOccurred": True,
                        "productionAvailable": False,
                    }
                    uncertain_flags: dict[str, int] = {
                        "hazardous_authority_open": 1,
                    }
                    if attempted_component == "rolling":
                        uncertain_flags["burned_rolling"] = 1
                    elif attempted_component == "quote":
                        uncertain_flags["burned_quote"] = 1
                    elif attempted_component in {"lane", "heartbeat", "capability"}:
                        uncertain_flags[f"{attempted_component}_active"] = 1
                        compensation_targets.append(attempted_component)
                    self._record_step(
                        request=value,
                        component=attempted_component,
                        action=dict(
                            (name, action) for name, action, _stage in self._CAS_STEPS
                        )[attempted_component],
                        receipt=failure,
                        outcome="FAILED",
                        flags=uncertain_flags,
                    )
                terminal, _hazardous = self._compensate(
                    request=value,
                    grant_instant=grant_instant,
                    succeeded=compensation_targets,
                    reason=f"CAS_FAILURE:{type(exc).__name__}",
                )
                if isinstance(exc, KisDomesticFunctionalGraphInjectedCrash):
                    raise
                return self.snapshot(value["operationId"], expected_state=terminal)
            return self.snapshot(
                value["operationId"],
                expected_state="ACTIVE_OFFLINE_SIMULATION",
            )

    def _audit_v2_startup(self) -> tuple[str, ...]:
        conn = self._connect_v2()
        orphaned: list[str] = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            _verify_v2_schema(conn)
            rows = conn.execute(
                "SELECT operation_id,state,lane_active,heartbeat_active,"
                "capability_active,step_count,step_head_hash FROM "
                "kis_functional_v2_graph_operation "
                "WHERE state IN ('PREPARED','APPLYING') ORDER BY operation_id"
            ).fetchall()
            now = _time_text(self.wall_clock(), "v2-startup-audit-at")
            for row in rows:
                operation_id = str(row["operation_id"])
                result = {
                    "schemaVersion": "kis-domestic-functional-graph-v2-orphan/v1",
                    "priorState": str(row["state"]),
                    "openProjectionCount": int(row["lane_active"])
                    + int(row["heartbeat_active"])
                    + int(row["capability_active"]),
                    "productionAvailable": False,
                }
                self._append_step_conn(
                    conn,
                    operation_id=operation_id,
                    component="graph",
                    action="STARTUP_OWNER_LOSS_AUDIT",
                    outcome="ORPHANED",
                    input_hash="0" * 64,
                    result=result,
                    occurred_at=now,
                )
                # PREPARED contains no external-CAS intent and is POST0.
                # APPLYING is always ambiguous after owner loss, even when
                # the scalar projection flags have not yet been updated: a
                # durable INTENT may precede a committed external CAS.
                hazardous = bool(
                    result["openProjectionCount"]
                    or str(row["state"]) == "APPLYING"
                )
                conn.execute(
                    "UPDATE kis_functional_v2_graph_operation SET "
                    "state='RECONCILIATION_REQUIRED',"
                    "hazardous_authority_open=?,failure_reason=?,revision=revision+1 "
                    "WHERE operation_id=?",
                    (
                        int(hazardous),
                        "STARTUP_OWNER_LOSS_REQUIRES_EXTERNAL_COMPENSATION",
                        operation_id,
                    ),
                )
                orphaned.append(operation_id)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return tuple(orphaned)

    def verify_union(self, operation_id: str) -> dict[str, Any]:
        _identifier(operation_id, "v2-operation-id")
        conn = self._connect_v2()
        try:
            _verify_v2_schema(conn)
            row = conn.execute(
                "SELECT * FROM kis_functional_v2_graph_operation "
                "WHERE operation_id=?", (operation_id,),
            ).fetchone()
            if row is None:
                raise KisDomesticFunctionalGraphBlocked("v2-operation-missing")
            try:
                request = json.loads(str(row["request_json"]))
                statuses = json.loads(str(row["component_statuses_json"]))
            except (TypeError, json.JSONDecodeError):
                raise KisDomesticFunctionalGraphBlocked("v2-operation-json-invalid") from None
            request = _v2_request(request)
            readers_binding_hash = self._fresh_readers_registry_guard(request)
            if (
                request["operationId"] != operation_id
                or request["sessionId"] != str(row["session_id"])
                or request["requestHash"] != str(row["request_hash"])
                or _hash(statuses) != str(row["component_statuses_hash"])
                or self.port_bindings_hash != str(row["port_bindings_hash"])
                or request["ownerEpoch"] != int(row["owner_epoch"])
                or request["ownerEpochHash"] != str(row["owner_epoch_hash"])
                or request["registryAcceptedHeadHash"]
                != str(row["registry_accepted_head_hash"])
            ):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-operation-row-projection-mismatch"
                )
            if set(statuses) != set(V2_COMPONENTS):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-status-cardinality-mismatch"
                )
            for name in V2_COMPONENTS:
                candidate = statuses[name]
                if not isinstance(candidate, Mapping) or set(candidate) != _V2_STATUS_KEYS:
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-stored-{name}-status-not-exact"
                    )
                unsigned = dict(candidate)
                digest = unsigned.pop("statusHash")
                if (
                    not hmac.compare_digest(digest, _hash(unsigned))
                    or candidate["component"] != name
                    or (
                        name != "readers"
                        and candidate["codeHash"]
                        != V2_FROZEN_COMPONENT_FILE_SHA256[name]
                    )
                    or (
                        name == "readers"
                        and candidate["codeHash"]
                        != self.ports["readers"].code_hash
                    )
                    or candidate["protocolHash"] != V2_FROZEN_PROTOCOL_HASHES[name]
                ):
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-stored-{name}-status-dirty"
                    )
                try:
                    valid = self.ports[name].status_verifier(dict(candidate))
                except Exception:
                    valid = False
                if valid is not True:
                    raise KisDomesticFunctionalGraphBlocked(
                        f"v2-stored-{name}-status-unverified"
                    )
            steps = conn.execute(
                "SELECT * FROM kis_functional_v2_graph_step "
                "WHERE operation_id=? ORDER BY ordinal", (operation_id,),
            ).fetchall()
            previous = "0" * 64
            successful: list[tuple[str, str]] = []
            compensated: set[str] = set()
            failed_attempts: set[str] = set()
            pending_intent: dict[str, Any] | None = None
            derived_flags = {
                "burnedRolling": False,
                "burnedQuote": False,
                "laneActive": False,
                "heartbeatActive": False,
                "capabilityActive": False,
                "hazardousAuthorityOpen": False,
            }
            for ordinal, step in enumerate(steps, 1):
                try:
                    result = json.loads(str(step["result_json"]))
                except (TypeError, json.JSONDecodeError):
                    raise KisDomesticFunctionalGraphBlocked("v2-step-result-json-invalid") from None
                body = {
                    "schemaVersion": V2_STEP_SCHEMA,
                    "operationId": operation_id,
                    "ordinal": ordinal,
                    "component": str(step["component"]),
                    "action": str(step["action"]),
                    "outcome": str(step["outcome"]),
                    "inputHash": str(step["input_hash"]),
                    "resultHash": str(step["result_hash"]),
                    "occurredAt": str(step["occurred_at"]),
                    "previousHash": previous,
                }
                if (
                    int(step["ordinal"]) != ordinal
                    or str(step["previous_hash"]) != previous
                    or _hash(result) != str(step["result_hash"])
                    or _hash(body) != str(step["step_hash"])
                ):
                    raise KisDomesticFunctionalGraphBlocked("v2-step-chain-dirty")
                outcome = str(step["outcome"])
                component = str(step["component"])
                action = str(step["action"])
                if outcome == "INTENT":
                    if pending_intent is not None or not action.startswith("INTENT_"):
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-port-intent-topology-invalid"
                        )
                    intended_action = action.removeprefix("INTENT_")
                    expected_intent = {
                        "schemaVersion": "kis-domestic-functional-graph-v2-port-intent/v1",
                        "operationId": operation_id,
                        "sessionId": request["sessionId"],
                        "component": component,
                        "action": intended_action,
                        "requestHash": request["requestHash"],
                        "actionInputsHash": request["actionInputsHash"],
                        "grantWallAt": str(row["captured_wall_at"]),
                        "grantMonotonicNs": int(row["captured_monotonic_ns"]),
                        "expectedStatusRevision": result.get(
                            "expectedStatusRevision"
                        ),
                        "expectedStatusHeadHash": result.get(
                            "expectedStatusHeadHash"
                        ),
                        "compensation": result.get("compensation"),
                        "productionAvailable": False,
                    }
                    if result != expected_intent:
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-port-intent-binding-invalid"
                        )
                    if (
                        type(result["expectedStatusRevision"]) is not int
                        or result["expectedStatusRevision"] < 1
                        or type(result["compensation"]) is not bool
                    ):
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-port-intent-values-invalid"
                        )
                    _sha(
                        result["expectedStatusHeadHash"],
                        "v2-intent-status-head",
                    )
                    pending_intent = {
                        **result,
                        "intentStepHash": str(step["step_hash"]),
                    }
                    if not result["compensation"] and component == "rolling":
                        derived_flags["burnedRolling"] = True
                    if not result["compensation"] and component == "quote":
                        derived_flags["burnedQuote"] = True
                    derived_flags["hazardousAuthorityOpen"] = True
                elif outcome in {
                    "SUCCEEDED", "FAILED", "COMPENSATED", "COMPENSATION_FAILED"
                }:
                    if pending_intent is None:
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-port-result-without-intent"
                        )
                    if (
                        component != pending_intent["component"]
                        or action != pending_intent["action"]
                    ):
                        raise KisDomesticFunctionalGraphBlocked(
                            "v2-port-result-intent-mismatch"
                        )
                    if outcome in {"SUCCEEDED", "COMPENSATED"}:
                        if not isinstance(result, Mapping) or set(result) != (
                            _V2_PORT_RAW_KEYS | {"receiptHash"}
                        ):
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-stored-port-receipt-not-exact"
                            )
                        raw_receipt = dict(result)
                        stored_receipt_hash = raw_receipt.pop("receiptHash")
                        if not hmac.compare_digest(
                            str(stored_receipt_hash), _hash(raw_receipt)
                        ):
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-stored-port-receipt-hash-invalid"
                            )
                        expected_receipt = {
                            "component": component,
                            "action": action,
                            "operationId": operation_id,
                            "sessionId": request["sessionId"],
                            "requestHash": request["requestHash"],
                            "actionInputsHash": request["actionInputsHash"],
                            "grantWallAt": str(row["captured_wall_at"]),
                            "grantMonotonicNs": int(row["captured_monotonic_ns"]),
                            "expectedStatusRevision": pending_intent[
                                "expectedStatusRevision"
                            ],
                            "expectedStatusHeadHash": pending_intent[
                                "expectedStatusHeadHash"
                            ],
                            "intentStepHash": pending_intent["intentStepHash"],
                            "productionAvailable": False,
                            "exactCas": True,
                        }
                        if any(
                            type(raw_receipt.get(key)) is not type(expected)
                            or raw_receipt.get(key) != expected
                            for key, expected in expected_receipt.items()
                        ):
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-stored-port-receipt-binding-invalid"
                            )
                        try:
                            receipt_verified = self.ports[
                                component
                            ].receipt_verifier(dict(raw_receipt))
                        except Exception:
                            receipt_verified = False
                        if receipt_verified is not True:
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-stored-port-receipt-unverified"
                            )
                        if component == "lane" and action == "ACTIVATE":
                            self._verify_lane_activation_result(
                                raw_receipt["result"],
                                request=request,
                                grant_instant={
                                    "wallAt": str(row["captured_wall_at"]),
                                    "monotonicNs": int(
                                        row["captured_monotonic_ns"]
                                    ),
                                },
                                expectation=pending_intent,
                            )
                    if outcome == "SUCCEEDED":
                        if pending_intent["compensation"] is not False:
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-success-receipt-is-compensation"
                            )
                        if component in {"lane", "heartbeat", "capability"}:
                            derived_flags[f"{component}Active"] = True
                        derived_flags["hazardousAuthorityOpen"] = False
                    elif outcome == "FAILED":
                        if component in {"lane", "heartbeat", "capability"}:
                            derived_flags[f"{component}Active"] = True
                            derived_flags["hazardousAuthorityOpen"] = True
                        else:
                            derived_flags["hazardousAuthorityOpen"] = False
                    elif outcome == "COMPENSATED":
                        if pending_intent["compensation"] is not True:
                            raise KisDomesticFunctionalGraphBlocked(
                                "v2-compensation-receipt-is-primary"
                            )
                        if component in {"lane", "heartbeat", "capability"}:
                            derived_flags[f"{component}Active"] = False
                        derived_flags["hazardousAuthorityOpen"] = any(
                            derived_flags[f"{name}Active"]
                            for name in ("lane", "heartbeat", "capability")
                        )
                    else:
                        derived_flags["hazardousAuthorityOpen"] = True
                    pending_intent = None
                elif outcome == "ORPHANED":
                    if result.get("priorState") == "APPLYING":
                        derived_flags["hazardousAuthorityOpen"] = True
                else:
                    raise KisDomesticFunctionalGraphBlocked(
                        "v2-step-outcome-invalid"
                    )
                if outcome == "SUCCEEDED":
                    successful.append((str(step["component"]), str(step["action"])))
                elif outcome == "COMPENSATED":
                    compensated.add(str(step["component"]))
                elif outcome == "FAILED":
                    failed_attempts.add(str(step["component"]))
                previous = str(step["step_hash"])
            if (
                len(steps) != int(row["step_count"])
                or previous != str(row["step_head_hash"])
            ):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-step-cardinality-or-head-mismatch"
                )
            expected_prefix = [
                (component, action) for component, action, _stage in self._CAS_STEPS
            ]
            if successful != expected_prefix[: len(successful)]:
                raise KisDomesticFunctionalGraphBlocked("v2-success-step-order-invalid")
            state = str(row["state"])
            if state == "ACTIVE_OFFLINE_SIMULATION" and successful != expected_prefix:
                raise KisDomesticFunctionalGraphBlocked("v2-active-step-set-incomplete")
            if state in {"SAFE_INCOMPLETE", "RECONCILIATION_REQUIRED"}:
                required_compensation = {
                    component for component, _action in successful
                    if component in self._COMPENSATION_ACTION
                } | (failed_attempts & set(self._COMPENSATION_ACTION))
                if state == "SAFE_INCOMPLETE" and compensated != required_compensation:
                    raise KisDomesticFunctionalGraphBlocked(
                        "v2-safe-incomplete-compensation-incomplete"
                    )
            flags = {
                "burnedRolling": bool(row["burned_rolling"]),
                "burnedQuote": bool(row["burned_quote"]),
                "laneActive": bool(row["lane_active"]),
                "heartbeatActive": bool(row["heartbeat_active"]),
                "capabilityActive": bool(row["capability_active"]),
                "hazardousAuthorityOpen": bool(row["hazardous_authority_open"]),
            }
            if pending_intent is not None:
                derived_flags["hazardousAuthorityOpen"] = True
            if flags != derived_flags:
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-operation-flags-do-not-match-step-chain"
                )
            if state == "SAFE_INCOMPLETE" and any(
                flags[name] for name in (
                    "laneActive", "heartbeatActive", "capabilityActive",
                    "hazardousAuthorityOpen",
                )
            ):
                raise KisDomesticFunctionalGraphBlocked(
                    "v2-safe-incomplete-authority-remains-open"
                )
            body = {
                "schemaVersion": V2_VERIFY_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "operationId": operation_id,
                "sessionId": request["sessionId"],
                "state": state,
                "requestHash": request["requestHash"],
                "componentStatusesHash": str(row["component_statuses_hash"]),
                "portBindingsHash": str(row["port_bindings_hash"]),
                "readersRegistryComponentBindingHash": readers_binding_hash,
                "stepCount": len(steps),
                "stepHeadHash": previous,
                **flags,
                "allExactJoinsPassed": True,
                "crossLedgerAtomicityClaimed": False,
                "allOrFailClosedCompensationVerified": state != "RECONCILIATION_REQUIRED",
                "sameOneUseRetryAllowed": False,
                "productionAvailable": False,
                "networkAvailable": False,
                "releaseAvailable": False,
            }
            return {**body, "verificationHash": _hash(body)}
        finally:
            conn.close()

    def snapshot(self, operation_id: str, *, expected_state: str | None = None) -> dict[str, Any]:
        evidence = self.verify_union(operation_id)
        if expected_state is not None and evidence["state"] != expected_state:
            raise KisDomesticFunctionalGraphBlocked("v2-snapshot-state-mismatch")
        return evidence


def graph_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-graph-status/v1",
        "schemaFingerprint": SCHEMA_FINGERPRINT,
        "route": ROUTE,
        "pdno": PDNO,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "stateServerWired": False,
        "atomicGrantTransactionAvailable": True,
        "atomicCapabilityGrantAvailable": True,
        "activationBackdatedToBarOpen": False,
        "restartReconciliationAvailable": True,
        "networkOrderPostAllowed": False,
        "v2CoordinatorAvailableOffline": True,
        "frozenLedgerAdaptersAvailableOffline": True,
        "ownerEpochGuardWiredOffline": True,
        "registryAcceptedHeadGuardWiredOffline": True,
        "readersAcceptedRegistryComponentBindingRequired": True,
        "marketSourcePortAndStatusGuardWiredOffline": True,
        "marketSourcePostObservationPrefixExtensionJoined": False,
        "marketArchiveExternalAuthorityPinned": False,
        "independentUnionVerifierAvailableOffline": True,
        "durableIntentBeforeEveryPortCas": True,
        "portReceiptExactGrantAndHeadBinding": True,
        "accountWideUnresolvedHazardBlocksNewSession": True,
        "unionReplaysReceiptVerifiers": True,
        "actionInputsHashDerivedByConsumer": True,
        "allOrFailClosedCompensationAvailableOffline": True,
        "crossLedgerAtomicityAvailable": False,
        "nativeLaneGrantInstantAvailable": True,
        "laneGrantReceiptIssuedAndReverifiedOffline": True,
        "laneGrantReceiptProductionAuthorityAvailable": False,
        "registryGraphBindingWired": False,
        "productionV2PreflightAllowed": False,
    }


__all__ = [
    "ACTIVATION_SCHEMA",
    "CAPABILITY_GRANT_SCHEMA",
    "CRASH_STAGES",
    "DurableKisDomesticFunctionalGraph",
    "DurableKisDomesticFunctionalGraphV2Coordinator",
    "FrozenKisDomesticGraphLedgerPort",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_RELEASE_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_STATE_SERVER_WIRED",
    "KIS_DOMESTIC_FUNCTIONAL_GRAPH_V2_PRODUCTION_AVAILABLE",
    "KisDomesticFunctionalGraphBlocked",
    "KisDomesticFunctionalGraphInjectedCrash",
    "PREALLOCATION_SCHEMA",
    "SCHEMA_FINGERPRINT",
    "V2_COMPONENTS",
    "V2_CRASH_STAGES",
    "V2_FROZEN_COMPONENT_FILE_SHA256",
    "V2_FROZEN_PROTOCOL_HASHES",
    "V2_OPERATION_SCHEMA",
    "V2_PORT_RECEIPT_SCHEMA",
    "V2_PORT_STATUS_SCHEMA",
    "V2_REQUEST_SCHEMA",
    "V2_SCHEMA_FINGERPRINT",
    "V2_VERIFY_SCHEMA",
    "graph_component_status",
]
