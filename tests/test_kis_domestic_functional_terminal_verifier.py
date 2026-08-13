from __future__ import annotations

import copy
import hashlib
import hmac
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    LIVE_ORIGIN,
    PDNO,
    ROUTE,
)
from live_trader import kis_domestic_functional_terminal_verifier as terminal_module
from live_trader.kis_domestic_functional_terminal_verifier import (
    KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PROMOTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_RELEASE_AVAILABLE,
    OUTCOME,
    OUTCOME_OWNER_LOSS_LIMIT_REACHED,
    OUTCOME_OWNED_CLEANUP_INCOMPLETE,
    PROTOCOL_FINGERPRINT,
    KisDomesticFunctionalTerminalArchiveAdapters,
    KisDomesticFunctionalTerminalVerifier,
    KisDomesticFunctionalTerminalVerifierBlocked,
    terminal_verifier_component_status,
)
from live_trader.kis_domestic_functional_readers import (
    ImmutableSqliteComponentArchiveReader,
    ImmutableTruthArchiveReader,
)
from tests.test_kis_domestic_functional_readers import (
    ACCOUNT as READER_ACCOUNT,
    BASELINE as READER_BASELINE,
    COMPONENT_TYPES as READER_COMPONENT_TYPES,
    SESSION_ID as READER_SESSION,
    _Fixture as _ReaderFixture,
)


KEY = b"terminal-verifier-test-server-authority-key-48bytes"
SESSION = READER_SESSION
BOOTSTRAP = "kis-bootstrap-1"
APPROVAL = "kis-approval-1"
ARM = "kis-public-arm-1"
EVALUATION = "kis-evaluation-1"
TRIGGER = "kis-trigger-1"
SNAPSHOT = "kis-rolling-snapshot-1"
ACCOUNT = READER_ACCOUNT
BASELINE = READER_BASELINE
CONTRACT = "c" * 64
CODE = "d" * 64
PERMIT = "e" * 64
CAPTURE_BUNDLE = "f" * 64
SOURCE_GENERATION = "kis-ws-generation-11111111111111111111111111111111"
ACTIVATED = datetime(2026, 8, 14, 3, 0, tzinfo=timezone.utc)
EXPIRES = ACTIVATED + timedelta(seconds=ACTIVE_SECONDS)
FINALIZED = EXPIRES + timedelta(minutes=5)
NOW = FINALIZED + timedelta(minutes=1)


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sign(domain: str, body) -> str:
    return hmac.new(
        KEY,
        domain.encode("ascii") + b"\n" + _canonical(body),
        hashlib.sha256,
    ).hexdigest()


def _verify(domain: str, body, signature: str) -> bool:
    return type(signature) is str and hmac.compare_digest(
        _sign(domain, body), signature
    )


def _env(domain: str, body: dict) -> dict:
    digest = _hash(body)
    return {
        "body": body,
        "recordHash": digest,
        "signature": _sign(domain, {**body, "recordHash": digest}),
    }


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _mutation_item(
    *,
    claim: str,
    kind: str,
    broker: str,
    fill_hash: str,
    previous: str,
) -> dict:
    endpoint = "/uapi/domestic-stock/v1/trading/order-cash"
    unsigned = {
        "claimId": claim,
        "actionKind": kind,
        "requestHash": _hash({"claim": claim, "request": 1}),
        "responseHash": _hash({"claim": claim, "response": 1}),
        "brokerOrderId": broker,
        "physicalAttemptCount": 1,
        "method": "POST",
        "endpoint": endpoint,
        "endpointHash": _hash({"method": "POST", "endpoint": endpoint}),
        "terminalState": "FILLED",
        "officialFillHash": fill_hash,
        "previousHash": previous,
    }
    return {**unsigned, "recordHash": _hash(unsigned)}


def _bundle(*, sell_price: int = 79_000, include_mutation=True, include_capability=True):
    raw_frames = ["1" * 64, "2" * 64]
    raw_window = "3" * 64
    raw_trigger = "4" * 64
    source_archive_value = _hash(
        {
            "sourceGeneration": SOURCE_GENERATION,
            "rawWindowHash": raw_window,
            "rawTriggerHash": raw_trigger,
            "rawFrameHashes": raw_frames,
            "firstSourceSequence": "1",
            "lastSourceSequence": "2",
        }
    )
    source = _env(
        "SOURCE_RAW_ARCHIVE",
        {
            "schemaVersion": "kis-domestic-source-raw-archive/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "publicArmId": ARM,
            "evaluationId": EVALUATION,
            "triggerId": TRIGGER,
            "sourceGeneration": SOURCE_GENERATION,
            "rawWindowHash": raw_window,
            "rawTriggerHash": raw_trigger,
            "sourceArchiveHash": source_archive_value,
            "rawFrameHashes": raw_frames,
            "rawFrameCount": 2,
            "firstSourceSequence": "1",
            "lastSourceSequence": "2",
            "sequenceGapDetected": False,
            "duplicateDetected": False,
            "upstreamExchangeSequenceAvailable": False,
            "archiveRecomputedFromAuthenticatedFrames": True,
        },
    )
    evaluation = _env(
        "LANE_EVALUATION",
        {
            "schemaVersion": "kis-domestic-lane-evaluation/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "evaluationId": EVALUATION,
            "publicArmId": ARM,
            "signal": "BUY",
            "rawWindowHash": raw_window,
            "sourceArchiveHash": source["recordHash"],
            "barCloseAt": _iso(ACTIVATED - timedelta(minutes=5)),
            "evaluatedAt": _iso(ACTIVATED - timedelta(minutes=5)),
            "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
            "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
            "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
            "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
            "codeManifestHash": CODE,
            "state": "CONSUMED",
        },
    )
    trigger = _env(
        "LANE_TRIGGER",
        {
            "schemaVersion": "kis-domestic-lane-trigger/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "triggerId": TRIGGER,
            "evaluationId": EVALUATION,
            "rawTriggerHash": raw_trigger,
            "sourceGeneration": SOURCE_GENERATION,
            "barOpenAt": _iso(ACTIVATED),
            "observedAt": _iso(ACTIVATED + timedelta(seconds=1)),
            "state": "CONSUMED",
        },
    )
    session = _env(
        "LANE_SESSION",
        {
            "schemaVersion": "kis-domestic-lane-session/v1",
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "sessionId": SESSION,
            "bootstrapId": BOOTSTRAP,
            "approvalId": APPROVAL,
            "evaluationId": EVALUATION,
            "triggerId": TRIGGER,
            "permitId": "kis-permit-1",
            "permitHash": PERMIT,
            "accountFingerprint": ACCOUNT,
            "preactivationBaselineHash": BASELINE,
            "contractEnvelopeHash": CONTRACT,
            "codeManifestHash": CODE,
            "state": "FINALIZED",
            "activatedAt": _iso(ACTIVATED),
            "expiresAt": _iso(EXPIRES),
            "cleanupEndsAt": _iso(EXPIRES + timedelta(minutes=15)),
            "cleanupStartedAt": _iso(EXPIRES),
            "finalizedAt": _iso(FINALIZED),
            "revision": 7,
        },
    )
    bootstrap = _env(
        "LANE_BOOTSTRAP",
        {
            "schemaVersion": "kis-domestic-lane-bootstrap/v1",
            "route": ROUTE,
            "bootstrapId": BOOTSTRAP,
            "publicArmId": ARM,
            "evaluationId": EVALUATION,
            "triggerId": TRIGGER,
            "approvalId": APPROVAL,
            "sessionId": SESSION,
            "preactivationBaselineHash": BASELINE,
            "state": "CONSUMED",
            "revision": 4,
        },
    )
    approval = _env(
        "LANE_APPROVAL",
        {
            "schemaVersion": "kis-domestic-lane-approval/v1",
            "route": ROUTE,
            "approvalId": APPROVAL,
            "bootstrapId": BOOTSTRAP,
            "evaluationId": EVALUATION,
            "triggerId": TRIGGER,
            "sessionId": SESSION,
            "state": "CONSUMED",
            "revision": 3,
        },
    )
    buy_fill_hash = "5" * 64
    sell_fill_hash = "6" * 64
    action_specs = (
        (
            "buy-claim", "NATURAL_BUY", "buy-order", "80000", "10",
            EVALUATION, TRIGGER, buy_fill_hash, ACTIVATED + timedelta(seconds=1),
        ),
        (
            "sell-claim", "CLEANUP_SELL", "sell-order", str(sell_price), "10",
            "", "", sell_fill_hash, EXPIRES + timedelta(seconds=1),
        ),
    )
    actions = []
    fills = []
    for claim, kind, broker, price, fee, eval_id, trigger_id, fill_hash, occurred in action_specs:
        actions.append(
            _env(
                "LANE_ACTION",
                {
                    "schemaVersion": "kis-domestic-lane-action/v1",
                    "route": ROUTE,
                    "pdno": PDNO,
                    "claimId": claim,
                    "sessionId": SESSION,
                    "actionKind": kind,
                    "state": "FILLED",
                    "quantity": "1",
                    "limitPriceKrw": price,
                    "grossKrw": price,
                    "evaluationId": eval_id,
                    "triggerId": trigger_id,
                    "brokerOrderId": broker,
                    "fillPriceKrw": price,
                    "feeKrw": fee,
                    "taxKrw": "0",
                    "loanInterestKrw": "0",
                    "createdAt": _iso(occurred),
                    "postBoundaryAt": _iso(occurred),
                    "filledAt": _iso(occurred),
                    "rawMutationHash": "7" * 64,
                    "officialFillHash": fill_hash,
                    "transitionHeadHash": "8" * 64,
                    "revision": 5,
                },
            )
        )
        fills.append(
            {
                "claimId": claim,
                "brokerOrderId": broker,
                "side": "BUY" if kind == "NATURAL_BUY" else "SELL",
                "quantity": "1",
                "fillPriceKrw": price,
                "feeKrw": fee,
                "taxKrw": "0",
                "loanInterestKrw": "0",
                "filledAt": _iso(occurred),
                "officialFillHash": fill_hash,
                "terminalFilled": True,
            }
        )
    final_cash = 100_000 - 80_000 - 10 + sell_price - 10
    heartbeat = _env(
        "HEARTBEAT_RESULT",
        {
            "schemaVersion": "kis-domestic-functional-heartbeat-evidence/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": SESSION,
            "activatedAt": _iso(ACTIVATED),
            "activeEndsAt": _iso(EXPIRES),
            "processGeneration": "kis-process-generation-" + "1" * 32,
            "socketGeneration": SOURCE_GENERATION,
            "sampleCount": 721,
            "sampleHeadHash": "9" * 64,
            "actualMonotonicElapsedSeconds": "7200",
            "maxHeartbeatGapSeconds": "10",
            "uninterrupted": True,
            "exact7200ObservationPassed": True,
            "outcome": "ELIGIBLE_FOR_INDEPENDENT_WIRING_VERIFICATION",
            "functionalTestPassed": False,
            "promotionEligible": False,
            "releaseAvailable": False,
        },
    )
    rolling = _env(
        "ROLLING_PREFLIGHT_RECEIPT",
        {
            "schemaVersion": "kis-domestic-rolling-preflight-consumption/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "snapshotId": SNAPSHOT,
            "snapshotHash": "a" * 64,
            "diagnosticHash": "b" * 64,
            "captureBundleHash": CAPTURE_BUNDLE,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": "c" * 64,
            "preactivationBaselineHash": BASELINE,
            "contractEnvelopeHash": CONTRACT,
            "codeManifestHash": CODE,
            "publicArmId": ARM,
            "preapprovalHash": "d" * 64,
            "evaluationId": EVALUATION,
            "evaluationHash": evaluation["recordHash"],
            "triggerId": TRIGGER,
            "triggerHash": trigger["recordHash"],
            "sourceGeneration": SOURCE_GENERATION,
            "barOpenAt": _iso(ACTIVATED),
            "completedAt": _iso(ACTIVATED - timedelta(seconds=5)),
            "expiresAt": _iso(ACTIVATED + timedelta(seconds=55)),
            "consumedAt": _iso(ACTIVATED + timedelta(seconds=1)),
            "sessionId": SESSION,
            "sessionNonceHash": "e" * 64,
            "singleUseConsumed": True,
            "privateAccountAuthorityAvailable": False,
            "tokenAuthorityAvailable": False,
            "orderAuthorityAvailable": False,
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
            "finalQuoteAvailable": False,
            "releaseEvidenceEligible": False,
        },
    )
    baseline = _env(
        "BASELINE_TRUTH",
        {
            "schemaVersion": "kis-domestic-baseline-truth/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "accountFingerprint": ACCOUNT,
            "baselineHash": BASELINE,
            "targetQuantity": "0",
            "targetOrderableQuantity": "0",
            "cashKrw": "100000",
            "accountWideWorkingOrdersZero": True,
            "stableRepeatedReads": True,
            "costBaselineComplete": True,
            "captureBundleHash": CAPTURE_BUNDLE,
        },
    )
    terminal = _env(
        "TERMINAL_TRUTH",
        {
            "schemaVersion": "kis-domestic-terminal-truth/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "accountFingerprint": ACCOUNT,
            "baselineHash": BASELINE,
            "targetQuantity": "0",
            "targetOrderableQuantity": "0",
            "cashKrw": str(final_cash),
            "accountWideWorkingOrdersZero": True,
            "ownedWorkingOrdersZero": True,
            "stableRepeatedReads": True,
            "allPagesComplete": True,
            "officialFills": fills,
            "observedAt": _iso(FINALIZED),
        },
    )
    mutation = None
    if include_mutation:
        first = _mutation_item(
            claim="buy-claim", kind="NATURAL_BUY", broker="buy-order",
            fill_hash=buy_fill_hash, previous="0" * 64,
        )
        second = _mutation_item(
            claim="sell-claim", kind="CLEANUP_SELL", broker="sell-order",
            fill_hash=sell_fill_hash, previous=first["recordHash"],
        )
        for action, item in zip(actions, (first, second), strict=True):
            action["body"]["rawMutationHash"] = item["recordHash"]
            replacement = _env("LANE_ACTION", action["body"])
            action.clear()
            action.update(replacement)
        mutation = _env(
            "RAW_MUTATION_RECORD",
            {
                "schemaVersion": "kis-domestic-raw-mutation-record/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": SESSION,
                "records": [first, second],
                "allPhysicalMutationAttemptsCounted": True,
                "postAmbiguityAbsent": True,
                "nonOwnedMutationObserved": False,
                "recordHeadHash": second["recordHash"],
            },
        )
    capability = None
    if include_capability:
        unsigned = {
            "schemaVersion": "kis-domestic-capability-revoke-proof/v1",
            "route": ROUTE,
            "sessionId": SESSION,
            "capabilityHash": "f" * 64,
            "revokedAt": _iso(FINALIZED),
            "runtimeReaderConfirmedClear": True,
            "globalReaderConfirmedClear": True,
            "functionalOrderAuthorityOpen": False,
        }
        capability = _env(
            "CAPABILITY_REVOKE_PROOF",
            {**unsigned, "recordHash": _hash(unsigned)},
        )
    return {
        "schemaVersion": "kis-domestic-functional-terminal-input/v1",
        "laneSession": session,
        "bootstrap": bootstrap,
        "approval": approval,
        "evaluation": evaluation,
        "trigger": trigger,
        "actions": actions,
        "heartbeatResult": heartbeat,
        "rollingPreflightReceipt": rolling,
        "sourceRawArchive": source,
        "baselineTruth": baseline,
        "terminalTruth": terminal,
        "mutationRecord": mutation,
        "capabilityRevokeProof": capability,
    }


def _verifier(
    adapters: KisDomesticFunctionalTerminalArchiveAdapters | None = None,
) -> KisDomesticFunctionalTerminalVerifier:
    return KisDomesticFunctionalTerminalVerifier(
        record_verifier=_verify,
        trusted_wall_clock=lambda: NOW,
        archive_adapters=adapters,
    )


def _resign(bundle: dict, key: str, domain: str) -> None:
    bundle[key] = _env(domain, bundle[key]["body"])


def _raw_source_evidence(bundle: dict) -> dict:
    frames = []
    events = []
    bars = []
    frame_hashes = []
    previous_frame_head = "0" * 64
    socket_hash = _hash({"socket": "terminal-test"})
    for offset in range(12):
        trade_at = ACTIVATED - timedelta(minutes=55) + timedelta(
            minutes=5 * offset, seconds=1
        )
        local = trade_at.astimezone(timezone(timedelta(hours=9)))
        fields = [""] * 46
        fields[0] = PDNO
        fields[1] = local.strftime("%H%M%S")
        fields[2] = "100"
        fields[33] = local.strftime("%Y%m%d")
        raw = "0|H0STCNT0|1|" + "^".join(fields)
        received_at = _iso(trade_at + timedelta(milliseconds=500))
        raw_hash = hashlib.sha256(raw.encode()).hexdigest()
        sequence = str(offset + 1)
        frame_unsigned = {
            "schemaVersion": "kis-domestic-h0stcnt0-frame/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "trId": "H0STCNT0",
            "sourceGeneration": SOURCE_GENERATION,
            "socketIdentityHash": socket_hash,
            "frameIndex": offset + 1,
            "firstSourceSequence": sequence,
            "lastSourceSequence": sequence,
            "recordCount": 1,
            "receivedAt": received_at,
            "rawFrame": raw,
            "rawFrameHash": raw_hash,
            "recordFields": [fields],
            "previousFrameHeadHash": previous_frame_head,
        }
        frame_envelope_hash = _hash(frame_unsigned)
        frame_head_hash = _hash(
            {
                "previousHash": previous_frame_head,
                "frameEnvelopeHash": frame_envelope_hash,
                "frameIndex": offset + 1,
            }
        )
        frames.append(
            {
                **frame_unsigned,
                "frameEnvelopeHash": frame_envelope_hash,
                "frameHeadHash": frame_head_hash,
            }
        )
        frame_hashes.append(frame_envelope_hash)
        bucket_open = trade_at.replace(second=0, microsecond=0)
        bucket_close = bucket_open + timedelta(minutes=5)
        event_unsigned = {
            "schemaVersion": "kis-domestic-h0stcnt0-raw-event/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": SOURCE_GENERATION,
            "socketIdentityHash": socket_hash,
            "sourceSequence": sequence,
            "recordIndex": 0,
            "rawFrameHash": raw_hash,
            "recordFields": fields,
            "receivedAt": received_at,
        }
        event = {
            "sourceSequence": sequence,
            "recordIndex": 0,
            "rawFrameHash": raw_hash,
            "rawEventHash": _hash(event_unsigned),
            "tradeAt": _iso(trade_at),
            "receivedAt": received_at,
            "bucketOpenAt": _iso(bucket_open),
            "bucketCloseAt": _iso(bucket_close),
            "recordFields": fields,
        }
        events.append(event)
        if offset < 11:
            bars.append(
                {
                    "openAt": event["bucketOpenAt"],
                    "closeAt": event["bucketCloseAt"],
                    "open": "100",
                    "high": "100",
                    "low": "100",
                    "close": "100",
                    "sourceSequenceStart": sequence,
                    "sourceSequenceEnd": sequence,
                    "eventCount": 1,
                    "rawEventChainHash": _hash(
                        {
                            "previousHash": "0" * 64,
                            "rawEventHash": event["rawEventHash"],
                            "sourceSequence": sequence,
                        }
                    ),
                }
            )
        previous_frame_head = frame_head_hash
    raw_window = _hash(
        {"sourceGeneration": SOURCE_GENERATION, "bars": bars}
    )
    raw_trigger = _hash(events[-1])
    source_archive_hash = _hash(
        {
            "sourceGeneration": SOURCE_GENERATION,
            "rawWindowHash": raw_window,
            "rawTriggerHash": raw_trigger,
            "rawFrameHashes": frame_hashes,
            "firstSourceSequence": "1",
            "lastSourceSequence": "11",
        }
    )
    source = bundle["sourceRawArchive"]["body"]
    source.update(
        {
            "rawWindowHash": raw_window,
            "rawTriggerHash": raw_trigger,
            "sourceArchiveHash": source_archive_hash,
            "rawFrameHashes": frame_hashes,
            "rawFrameCount": len(frame_hashes),
            "firstSourceSequence": "1",
            "lastSourceSequence": "11",
        }
    )
    _resign(bundle, "sourceRawArchive", "SOURCE_RAW_ARCHIVE")
    evaluation = bundle["evaluation"]["body"]
    evaluation["rawWindowHash"] = raw_window
    evaluation["sourceArchiveHash"] = bundle["sourceRawArchive"]["recordHash"]
    evaluation["barCloseAt"] = _iso(ACTIVATED)
    evaluation["evaluatedAt"] = _iso(ACTIVATED)
    _resign(bundle, "evaluation", "LANE_EVALUATION")
    trigger = bundle["trigger"]["body"]
    trigger["rawTriggerHash"] = raw_trigger
    trigger["observedAt"] = events[-1]["receivedAt"]
    _resign(bundle, "trigger", "LANE_TRIGGER")
    rolling = bundle["rollingPreflightReceipt"]["body"]
    rolling["evaluationHash"] = bundle["evaluation"]["recordHash"]
    rolling["triggerHash"] = bundle["trigger"]["recordHash"]
    _resign(bundle, "rollingPreflightReceipt", "ROLLING_PREFLIGHT_RECEIPT")
    return {
        "recordType": "SOURCE_WINDOW_ARCHIVE",
        "schemaVersion": "kis-domestic-terminal-source-raw/v1",
        "sourceGeneration": SOURCE_GENERATION,
        "socketIdentityHash": socket_hash,
        "frames": frames,
        "events": events,
        "recomputedBars": bars,
        "nextOpenEvent": events[-1],
        "rawWindowHash": raw_window,
        "rawTriggerHash": raw_trigger,
        "sourceArchiveHash": source_archive_hash,
    }


def _raw_heartbeat_evidence(bundle: dict) -> dict:
    samples = []
    previous = "0" * 64
    for index in range(721):
        unsigned = {
            "sequence": index + 1,
            "kind": (
                "ACTIVE_START"
                if index == 0
                else "ACTIVE_END_OBSERVED"
                if index == 720
                else "HEARTBEAT"
            ),
            "wallAt": _iso(ACTIVATED + timedelta(seconds=index * 10)),
            "monotonicNs": index * 10_000_000_000,
            "previousHash": previous,
        }
        item = {**unsigned, "recordHash": _hash(unsigned)}
        samples.append(item)
        previous = item["recordHash"]
    heartbeat = bundle["heartbeatResult"]["body"]
    heartbeat.update(
        {
            "sampleCount": len(samples),
            "sampleHeadHash": previous,
            "actualMonotonicElapsedSeconds": "7200",
            "maxHeartbeatGapSeconds": "10",
        }
    )
    _resign(bundle, "heartbeatResult", "HEARTBEAT_RESULT")
    return {
        "recordType": "HEARTBEAT_EVIDENCE",
        "schemaVersion": "kis-domestic-terminal-heartbeat-raw/v1",
        "activatedAt": _iso(ACTIVATED),
        "activeEndsAt": _iso(EXPIRES),
        "processGeneration": heartbeat["processGeneration"],
        "socketGeneration": heartbeat["socketGeneration"],
        "sampleHeadHash": previous,
        "samples": samples,
    }


_RAW_ENDPOINTS = {
    "balance": ("/uapi/domestic-stock/v1/trading/inquire-balance", "TTTC8434R"),
    "dailyCcld": ("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "TTTC0081R"),
    "workingOrders": ("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", "TTTC0084R"),
    "periodTradeProfit": ("/uapi/domestic-stock/v1/trading/inquire-period-trade-profit", "TTTC8715R"),
    "periodProfit": ("/uapi/domestic-stock/v1/trading/inquire-period-profit", "TTTC8708R"),
    "holiday": ("/uapi/domestic-stock/v1/quotations/chk-holiday", "CTCA0903R"),
}


def _raw_page(name: str, rows: list[dict], summary: dict) -> dict:
    endpoint, tr_id = _RAW_ENDPOINTS[name]
    raw_body = _canonical({"rows": rows, "summary": summary}).decode("utf-8")
    unsigned = {
        "schemaVersion": "kis-domestic-terminal-official-rest-page/v1",
        "method": "GET",
        "origin": LIVE_ORIGIN,
        "endpoint": endpoint,
        "trId": tr_id,
        "pageIndex": 0,
        "continuationSent": "",
        "continuationReceived": "",
        "cursorReceived": "",
        "statusCode": 200,
        "effectiveUrl": LIVE_ORIGIN + endpoint,
        "rawResponseBody": raw_body,
        "rawResponseSha256": hashlib.sha256(raw_body.encode()).hexdigest(),
        "rows": rows,
        "summary": summary,
    }
    return {**unsigned, "pageHash": _hash(unsigned)}


def _raw_capture(phase: str, *, terminal: bool) -> dict:
    zero_cost = {
        "buyAmtKrw": "0",
        "sellAmtKrw": "0",
        "buyFeeKrw": "0",
        "sellFeeKrw": "0",
        "buyTaxKrw": "0",
        "sellTaxKrw": "0",
        "loanInterestKrw": "0",
        "realizedProfitLossKrw": "0",
    }
    cost = (
        {
            "buyAmtKrw": "80000",
            "sellAmtKrw": "79000",
            "buyFeeKrw": "10",
            "sellFeeKrw": "10",
            "buyTaxKrw": "0",
            "sellTaxKrw": "0",
            "loanInterestKrw": "0",
            "realizedProfitLossKrw": "-1020",
        }
        if terminal
        else zero_cost
    )
    orders = []
    if terminal:
        orders = [
            {
                "ord_dt": "20260814",
                "ord_gno_brno": "001",
                "odno": "0001",
                "pdno": PDNO,
                "sll_buy_dvsn_cd": "02",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "cncl_cfrm_qty": "0",
                "rjct_qty": "0",
                "tot_ccld_amt": "80000",
                "avg_prvs": "80000",
                "filled_at": _iso(ACTIVATED + timedelta(seconds=1)),
            },
            {
                "ord_dt": "20260814",
                "ord_gno_brno": "001",
                "odno": "0002",
                "pdno": PDNO,
                "sll_buy_dvsn_cd": "01",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "cncl_cfrm_qty": "0",
                "rjct_qty": "0",
                "tot_ccld_amt": "79000",
                "avg_prvs": "79000",
                "filled_at": _iso(EXPIRES + timedelta(seconds=1)),
            },
        ]
    endpoints = {
        "balance": [
            _raw_page(
                "balance",
                [{"pdno": PDNO, "hldg_qty": "0", "ord_psbl_qty": "0"}],
                {"dnca_tot_amt": "98980" if terminal else "100000"},
            )
        ],
        "dailyCcld": [_raw_page("dailyCcld", orders, {})],
        "workingOrders": [_raw_page("workingOrders", [], {})],
        "periodTradeProfit": [_raw_page("periodTradeProfit", [], cost)],
        "periodProfit": [_raw_page("periodProfit", [], cost)],
        "holiday": [_raw_page("holiday", [{"opnd_yn": "Y"}], {})],
    }
    unsigned = {
        "schemaVersion": "kis-domestic-terminal-official-rest-capture/v1",
        "phase": phase,
        "endpoints": endpoints,
    }
    return {**unsigned, "captureHash": _hash(unsigned)}


def _rehash_capture(capture: dict) -> None:
    for pages in capture["endpoints"].values():
        for page in pages:
            raw_body = _canonical(
                {"rows": page["rows"], "summary": page["summary"]}
            ).decode("utf-8")
            page["rawResponseBody"] = raw_body
            page["rawResponseSha256"] = hashlib.sha256(
                raw_body.encode()
            ).hexdigest()
            unsigned_page = {
                key: value for key, value in page.items() if key != "pageHash"
            }
            page["pageHash"] = _hash(unsigned_page)
    unsigned_capture = {
        key: value for key, value in capture.items() if key != "captureHash"
    }
    capture["captureHash"] = _hash(unsigned_capture)


def _rebind_official_order_ids(bundle: dict) -> None:
    broker_ids = ("20260814:001:0001", "20260814:001:0002")
    for action, broker_id in zip(bundle["actions"], broker_ids, strict=True):
        action["body"]["brokerOrderId"] = broker_id
    for fill, broker_id in zip(
        bundle["terminalTruth"]["body"]["officialFills"], broker_ids, strict=True
    ):
        fill["brokerOrderId"] = broker_id
    records = bundle["mutationRecord"]["body"]["records"]
    previous = "0" * 64
    for action, record, broker_id in zip(
        bundle["actions"], records, broker_ids, strict=True
    ):
        record["brokerOrderId"] = broker_id
        record["previousHash"] = previous
        unsigned = dict(record)
        unsigned.pop("recordHash")
        record["recordHash"] = _hash(unsigned)
        action["body"]["rawMutationHash"] = record["recordHash"]
        previous = record["recordHash"]
    bundle["mutationRecord"]["body"]["recordHeadHash"] = previous
    for index, action in enumerate(bundle["actions"]):
        bundle["actions"][index] = _env("LANE_ACTION", action["body"])
    _resign(bundle, "terminalTruth", "TERMINAL_TRUTH")
    _resign(bundle, "mutationRecord", "RAW_MUTATION_RECORD")


def _archive_evidence(bundle: dict) -> dict[str, list[dict]]:
    _rebind_official_order_ids(bundle)
    source = _raw_source_evidence(bundle)
    heartbeat = _raw_heartbeat_evidence(bundle)
    action_refs = [
        {
            "recordType": "ACTION",
            "schemaVersion": "kis-domestic-terminal-action-reference/v1",
            "claimId": item["body"]["claimId"],
            "recordHash": item["recordHash"],
        }
        for item in bundle["actions"]
    ]
    mutation_actions = [
        {
            "recordType": "MUTATION_ACTION",
            "schemaVersion": "kis-domestic-terminal-mutation-action/v1",
            "claimId": item["body"]["claimId"],
            "actionRecordHash": item["recordHash"],
            "rawMutationHash": item["body"]["rawMutationHash"],
            "officialFillHash": item["body"]["officialFillHash"],
            "brokerOrderId": item["body"]["brokerOrderId"],
        }
        for item in bundle["actions"]
    ]
    refs = lambda record_type, key: {
        "recordType": record_type,
        "schemaVersion": "kis-domestic-terminal-record-reference/v1",
        "recordHash": bundle[key]["recordHash"],
    }
    before = _raw_capture("PREACTIVATION", terminal=False)
    after = _raw_capture("TERMINAL", terminal=True)
    return {
        "lane": [
            refs("LANE_SESSION", "laneSession"),
            refs("BOOTSTRAP", "bootstrap"),
            refs("APPROVAL", "approval"),
            refs("EVALUATION", "evaluation"),
            refs("TRIGGER", "trigger"),
            *action_refs,
        ],
        "source": [source],
        "rolling": [
            {"recordType": "ROLLING_DIAGNOSTIC", "schemaVersion": "test/v1"},
            {"recordType": "ROLLING_BASELINE", "schemaVersion": "test/v1"},
            refs("ROLLING_CONSUMPTION", "rollingPreflightReceipt"),
        ],
        "heartbeat": [heartbeat],
        "mutation": [
            {
                "recordType": "MUTATION_INTEGRITY",
                "schemaVersion": "kis-domestic-terminal-mutation-integrity/v1",
                "sessionId": SESSION,
                "mutationRecordHash": bundle["mutationRecord"]["recordHash"],
                "actionRecordHashes": sorted(
                    item["recordHash"] for item in bundle["actions"]
                ),
                "officialFillHashes": sorted(
                    item["body"]["officialFillHash"] for item in bundle["actions"]
                ),
                "integrityPassed": True,
            },
            *mutation_actions,
        ],
        "capability": [
            {
                "recordType": "CAPABILITY_REVOKE",
                "schemaVersion": "kis-domestic-terminal-capability-join/v1",
                "sessionId": SESSION,
                "revokeProofHash": bundle["capabilityRevokeProof"]["recordHash"],
                "externallyRevoked": True,
            }
        ],
        "quote": [
            {
                "recordType": "QUOTE_RECEIPT",
                "schemaVersion": "kis-domestic-terminal-quote-join/v1",
                "sessionId": SESSION,
                "evaluationHash": bundle["evaluation"]["recordHash"],
                "triggerHash": bundle["trigger"]["recordHash"],
                "rollingReceiptHash": bundle["rollingPreflightReceipt"]["recordHash"],
                "quoteReceiptHash": _hash({"quote": "terminal-test"}),
                "consumed": True,
                "orderAuthorityFresh": False,
            }
        ],
        "graph": [
            {
                "recordType": "GRAPH_ACTIVATION",
                "schemaVersion": "kis-domestic-terminal-graph-join/v1",
                "sessionId": SESSION,
                "laneSessionHash": bundle["laneSession"]["recordHash"],
                "rollingReceiptHash": bundle["rollingPreflightReceipt"]["recordHash"],
                "heartbeatResultHash": bundle["heartbeatResult"]["recordHash"],
                "activationCommitted": True,
                "productionGraphWired": False,
            }
        ],
        "truth": [
            {
                "recordType": "PREACTIVATION_BASELINE",
                "schemaVersion": "kis-domestic-terminal-official-rest-raw/v1",
                "captures": [copy.deepcopy(before), copy.deepcopy(before)],
            },
            {
                "recordType": "TERMINAL_TRUTH",
                "schemaVersion": "kis-domestic-terminal-official-rest-raw/v1",
                "captures": [copy.deepcopy(after), copy.deepcopy(after)],
            },
        ],
    }


class _TerminalArchiveFixture(_ReaderFixture):
    def __init__(self, evidence: dict[str, list[dict]]) -> None:
        self.evidence = evidence
        super().__init__()

    def _body(self, component: str, record_type: str, ordinal: int) -> dict:
        body = {
            "schemaVersion": "kis-domestic-functional-terminal-adapter-record/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": SESSION,
            "accountFingerprint": ACCOUNT,
            "preactivationBaselineHash": BASELINE,
            "recordOrdinal": ordinal,
            "evidence": copy.deepcopy(self.evidence[component][ordinal - 1]),
        }
        if component == "rolling" and record_type == "ROLLING_BASELINE":
            body["normalized"] = {"accountWideOrderRowsByKey": {}}
        if component == "mutation" and record_type == "MUTATION_INTEGRITY":
            body["baselineOrderKeys"] = []
        if component == "capability" and record_type == "CAPABILITY_REVOKE":
            body.update(
                {
                    "externallyRevoked": True,
                    "runtimeReaderConfirmedClear": True,
                    "globalReaderConfirmedClear": True,
                }
            )
        return body


def _archive_adapters(
    directory: Path, evidence: dict[str, list[dict]]
) -> tuple[KisDomesticFunctionalTerminalArchiveAdapters, _TerminalArchiveFixture]:
    fixture = _TerminalArchiveFixture(evidence)
    sqlite_archives = {}
    for component in set(READER_COMPONENT_TYPES) - {"truth"}:
        path = directory / f"{component}.sqlite3"
        fixture.write_sqlite_archive(path, component)
        sqlite_archives[component] = ImmutableSqliteComponentArchiveReader(
            path, component=component
        )
    truth_path = directory / "truth.json"
    fixture.write_truth_archive(truth_path)
    return (
        KisDomesticFunctionalTerminalArchiveAdapters(
            verify_only_readers=fixture.reader,
            sqlite_archives=sqlite_archives,
            truth_archive=ImmutableTruthArchiveReader(truth_path),
        ),
        fixture,
    )


def _canonical_raw_records(archive_evidence: dict[str, list[dict]]) -> dict:
    return {
        component: [
            {
                "schemaVersion": "kis-domestic-functional-terminal-adapter-record/v1",
                "route": ROUTE,
                "pdno": PDNO,
                "sessionId": SESSION,
                "accountFingerprint": ACCOUNT,
                "preactivationBaselineHash": BASELINE,
                "recordOrdinal": ordinal,
                "evidence": item,
                **(
                    {"normalized": {"accountWideOrderRowsByKey": {}}}
                    if component == "rolling" and item["recordType"] == "ROLLING_BASELINE"
                    else {}
                ),
                **(
                    {"baselineOrderKeys": []}
                    if component == "mutation" and item["recordType"] == "MUTATION_INTEGRITY"
                    else {}
                ),
                **(
                    {
                        "externallyRevoked": True,
                        "runtimeReaderConfirmedClear": True,
                        "globalReaderConfirmedClear": True,
                    }
                    if component == "capability"
                    else {}
                ),
            }
            for ordinal, item in enumerate(values, 1)
        ]
        for component, values in archive_evidence.items()
    }


def _archive_lineage_inputs(bundle: dict) -> tuple[dict, dict, list[dict]]:
    records = {
        key: bundle[key]["body"]
        for key in (
            "laneSession", "bootstrap", "approval", "evaluation", "trigger",
            "heartbeatResult", "rollingPreflightReceipt", "sourceRawArchive",
            "baselineTruth", "terminalTruth",
        )
    }
    hashes = {key: bundle[key]["recordHash"] for key in records}
    hashes["mutationRecord"] = bundle["mutationRecord"]["recordHash"]
    hashes["capabilityRevokeProof"] = bundle["capabilityRevokeProof"]["recordHash"]
    actions = [
        {**item["body"], "_recordHash": item["recordHash"]}
        for item in bundle["actions"]
    ]
    return records, hashes, actions


class KisDomesticFunctionalTerminalVerifierTest(unittest.TestCase):
    def test_component_is_offline_nonrelease_and_protocol_pinned(self) -> None:
        status = terminal_verifier_component_status()
        self.assertRegex(PROTOCOL_FINGERPRINT, r"^[0-9a-f]{64}$")
        for value in (
            KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PRODUCTION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_NETWORK_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_MUTATION_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_RELEASE_AVAILABLE,
            KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PROMOTION_AVAILABLE,
            status["networkOrderPostAllowed"],
        ):
            self.assertFalse(value)

    def test_complete_join_is_still_safe_incomplete_and_wiring_false(self) -> None:
        evidence = _verifier().verify(_bundle())
        self.assertEqual(OUTCOME, evidence["terminalOutcome"])
        self.assertFalse(evidence["functionalWiringPassed"])
        self.assertFalse(evidence["functionalTestPassed"])
        self.assertTrue(evidence["rawMutationTruthAvailable"])
        self.assertTrue(evidence["capabilityRevoked"])
        self.assertTrue(evidence["ownedCleanupComplete"])
        self.assertEqual(
            [
                "FULL_AUTHENTICATED_SOURCE_ARCHIVE_NOT_JOINED",
                "FULL_HEARTBEAT_SAMPLE_JOURNAL_NOT_JOINED",
                "FULL_OFFICIAL_REST_RAW_TRUTH_NOT_JOINED",
                "OPERATOR_EXCLUSIVITY_NOT_PROVEN",
                "PRODUCTION_GRAPH_NOT_WIRED",
                "PRODUCTION_VERIFY_ONLY_REGISTRY_NOT_PINNED",
                "SHARED_KIS_ROUTE_NOT_WIRED",
                "UPSTREAM_PACKET_COMPLETENESS_UNAVAILABLE",
            ],
            evidence["verificationBlockers"],
        )
        self.assertTrue(evidence["reconciliationRequired"])
        self.assertFalse(evidence["independentHeartbeatSampleJournalJoined"])
        self.assertFalse(evidence["independentAuthenticatedSourceArchiveJoined"])
        self.assertFalse(evidence["independentOfficialRestRawTruthJoined"])
        self.assertFalse(evidence["allRequiredImmutableRecordsPresent"])
        self.assertEqual(0, evidence["tradingMutationCount"])

    def test_raw_reducers_independently_recompute_duration_source_truth_and_loss(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        actions = [dict(item["body"]) for item in bundle["actions"]]
        terminal_module._verify_raw_heartbeat(
            archive_evidence["heartbeat"][0], bundle["heartbeatResult"]["body"]
        )
        terminal_module._verify_raw_source(
            archive_evidence["source"][0],
            bundle["sourceRawArchive"]["body"],
            bundle["evaluation"]["body"],
            bundle["trigger"]["body"],
        )
        terminal_module._verify_raw_truth(
            archive_evidence["truth"][0],
            archive_evidence["truth"][1],
            bundle["baselineTruth"]["body"],
            bundle["terminalTruth"]["body"],
            actions,
            terminal_module.Decimal("1020"),
        )
        records, hashes, actions = _archive_lineage_inputs(bundle)
        terminal_module._verify_archive_lineage(
            _canonical_raw_records(archive_evidence),
            records=records,
            hashes=hashes,
            actions=actions,
            session_id=SESSION,
            raw_mutation=True,
            capability_revoked=True,
        )

    def test_concrete_adapter_keeps_missing_market_archive_safe_incomplete(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        with tempfile.TemporaryDirectory() as directory:
            adapters, fixture = _archive_adapters(Path(directory), archive_evidence)
            evidence = _verifier(adapters).verify(bundle)
            self.assertIsNotNone(fixture.reader)
        reader_blockers = [
            item
            for item in evidence["verificationBlockers"]
            if item.startswith("IMMUTABLE_READER_SAFE_INCOMPLETE:")
        ]
        self.assertIn(
            "IMMUTABLE_READER_SAFE_INCOMPLETE:"
            "MARKET_SOURCE_SPECIALIZED_ARCHIVE_NOT_FETCHED",
            reader_blockers,
        )
        self.assertIn(
            "IMMUTABLE_READER_SAFE_INCOMPLETE:"
            "MARKET_SOURCE_POST_OBSERVATION_PREFIX_EXTENSION_NOT_JOINED",
            reader_blockers,
        )
        self.assertFalse(evidence["independentHeartbeatSampleJournalJoined"])
        self.assertFalse(evidence["independentAuthenticatedSourceArchiveJoined"])
        self.assertFalse(evidence["independentOfficialRestRawTruthJoined"])
        self.assertFalse(evidence["immutableVerifyOnlyArchivesJoined"])
        self.assertFalse(evidence["rawActionMutationOfficialBijectionJoined"])
        self.assertFalse(evidence["rawBaselineOwnerLossRecomputed"])
        self.assertFalse(evidence["offlineRawEvidenceVerificationPassed"])
        self.assertFalse(evidence["allRequiredImmutableRecordsPresent"])
        self.assertFalse(evidence["functionalWiringPassed"])
        self.assertFalse(evidence["releaseEvidenceEligible"])

    def test_preactivation_same_day_orders_and_costs_are_exact_delta_baseline(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        prior_orders = [
            {
                "ord_dt": "20260814",
                "ord_gno_brno": "001",
                "odno": "0998",
                "pdno": PDNO,
                "sll_buy_dvsn_cd": "02",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "cncl_cfrm_qty": "0",
                "rjct_qty": "0",
                "tot_ccld_amt": "50000",
                "avg_prvs": "50000",
                "filled_at": _iso(ACTIVATED - timedelta(hours=1)),
            },
            {
                "ord_dt": "20260814",
                "ord_gno_brno": "001",
                "odno": "0999",
                "pdno": PDNO,
                "sll_buy_dvsn_cd": "01",
                "ord_qty": "1",
                "tot_ccld_qty": "1",
                "rmn_qty": "0",
                "cncl_cfrm_qty": "0",
                "rjct_qty": "0",
                "tot_ccld_amt": "50000",
                "avg_prvs": "50000",
                "filled_at": _iso(ACTIVATED - timedelta(minutes=59)),
            },
        ]
        other_symbol_pair = copy.deepcopy(prior_orders)
        other_symbol_pair[0].update(
            {"odno": "0996", "pdno": "005930", "tot_ccld_amt": "60000", "avg_prvs": "60000"}
        )
        other_symbol_pair[1].update(
            {"odno": "0997", "pdno": "005930", "tot_ccld_amt": "60000", "avg_prvs": "60000"}
        )
        prior_orders.extend(other_symbol_pair)
        baseline_cost = {
            "buyAmtKrw": "50000",
            "sellAmtKrw": "50000",
            "buyFeeKrw": "5",
            "sellFeeKrw": "5",
            "buyTaxKrw": "0",
            "sellTaxKrw": "0",
            "loanInterestKrw": "0",
            "realizedProfitLossKrw": "-10",
        }
        terminal_cost = {
            "buyAmtKrw": "130000",
            "sellAmtKrw": "129000",
            "buyFeeKrw": "15",
            "sellFeeKrw": "15",
            "buyTaxKrw": "0",
            "sellTaxKrw": "0",
            "loanInterestKrw": "0",
            "realizedProfitLossKrw": "-1030",
        }
        for capture in archive_evidence["truth"][0]["captures"]:
            capture["endpoints"]["dailyCcld"][0]["rows"] = copy.deepcopy(
                prior_orders
            )
            for name in ("periodTradeProfit", "periodProfit"):
                capture["endpoints"][name][0]["summary"] = copy.deepcopy(
                    baseline_cost
                )
            _rehash_capture(capture)
        for capture in archive_evidence["truth"][1]["captures"]:
            current = capture["endpoints"]["dailyCcld"][0]["rows"]
            capture["endpoints"]["dailyCcld"][0]["rows"] = [
                *copy.deepcopy(prior_orders),
                *current,
            ]
            for name in ("periodTradeProfit", "periodProfit"):
                capture["endpoints"][name][0]["summary"] = copy.deepcopy(
                    terminal_cost
                )
            _rehash_capture(capture)
        terminal_module._verify_raw_truth(
            archive_evidence["truth"][0],
            archive_evidence["truth"][1],
            bundle["baselineTruth"]["body"],
            bundle["terminalTruth"]["body"],
            [dict(item["body"]) for item in bundle["actions"]],
            terminal_module.Decimal("1020"),
        )
        changed = archive_evidence["truth"][1]["captures"][0]
        changed["endpoints"]["dailyCcld"][0]["rows"][0][
            "filled_at"
        ] = _iso(ACTIVATED - timedelta(minutes=58))
        _rehash_capture(changed)
        archive_evidence["truth"][1]["captures"][1] = copy.deepcopy(changed)
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "preactivation-order-baseline-not-preserved",
        ):
            terminal_module._verify_raw_truth(
                archive_evidence["truth"][0],
                archive_evidence["truth"][1],
                bundle["baselineTruth"]["body"],
                bundle["terminalTruth"]["body"],
                [dict(item["body"]) for item in bundle["actions"]],
                terminal_module.Decimal("1020"),
            )

    def test_raw_heartbeat_chain_and_duration_tamper_fail_closed(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        archive_evidence["heartbeat"][0]["samples"][400]["monotonicNs"] += 1
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "heartbeat-sample-chain-invalid",
        ):
            terminal_module._verify_raw_heartbeat(
                archive_evidence["heartbeat"][0],
                bundle["heartbeatResult"]["body"],
            )

    def test_raw_source_frame_and_event_tamper_fail_closed(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        archive_evidence["source"][0]["frames"][3]["rawFrame"] += "x"
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "source-frame-chain-invalid",
        ):
            terminal_module._verify_raw_source(
                archive_evidence["source"][0],
                bundle["sourceRawArchive"]["body"],
                bundle["evaluation"]["body"],
                bundle["trigger"]["body"],
            )

    def test_raw_official_page_hidden_order_and_action_bijection_fail_closed(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        capture = archive_evidence["truth"][1]["captures"][0]
        page = capture["endpoints"]["dailyCcld"][0]
        page["rows"][1]["odno"] = "9999"
        _rehash_capture(capture)
        archive_evidence["truth"][1]["captures"][1] = copy.deepcopy(capture)
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "action-official-order-bijection-invalid",
        ):
            terminal_module._verify_raw_truth(
                archive_evidence["truth"][0],
                archive_evidence["truth"][1],
                bundle["baselineTruth"]["body"],
                bundle["terminalTruth"]["body"],
                [dict(item["body"]) for item in bundle["actions"]],
                terminal_module.Decimal("1020"),
            )

    def test_mutation_archive_action_substitution_fail_closed(self) -> None:
        bundle = _bundle()
        archive_evidence = _archive_evidence(bundle)
        archive_evidence["mutation"][1]["brokerOrderId"] = "20260814:001:9999"
        raw_records = _canonical_raw_records(archive_evidence)
        records, hashes, actions = _archive_lineage_inputs(bundle)
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "mutation-action-adapter-bijection-invalid",
        ):
            terminal_module._verify_archive_lineage(
                raw_records,
                records=records,
                hashes=hashes,
                actions=actions,
                session_id=SESSION,
                raw_mutation=True,
                capability_revoked=True,
            )

    def test_missing_mutation_and_capability_are_explicit_false_blockers(self) -> None:
        evidence = _verifier().verify(
            _bundle(include_mutation=False, include_capability=False)
        )
        self.assertFalse(evidence["rawMutationTruthAvailable"])
        self.assertFalse(evidence["capabilityRevoked"])
        self.assertIn(
            "IMMUTABLE_RAW_MUTATION_RECORD_ABSENT",
            evidence["verificationBlockers"],
        )
        self.assertIn(
            "EXTERNAL_CAPABILITY_REVOKE_PROOF_ABSENT",
            evidence["verificationBlockers"],
        )
        self.assertFalse(evidence["allRequiredImmutableRecordsPresent"])

    def test_lane_lineage_or_artifact_change_is_rejected(self) -> None:
        bundle = _bundle()
        bundle["evaluation"]["body"]["artifactContentHash"] = "0" * 64
        _resign(bundle, "evaluation", "LANE_EVALUATION")
        evidence = _verifier().verify(bundle)
        self.assertIn("LANE_AUTHORITY_LINEAGE_MISMATCH", evidence["verificationBlockers"])

    def test_heartbeat_rolling_and_source_mismatch_are_blockers(self) -> None:
        bundle = _bundle()
        bundle["heartbeatResult"]["body"]["exact7200ObservationPassed"] = False
        _resign(bundle, "heartbeatResult", "HEARTBEAT_RESULT")
        bundle["rollingPreflightReceipt"]["body"]["sourceGeneration"] = (
            "kis-ws-generation-" + "2" * 32
        )
        _resign(bundle, "rollingPreflightReceipt", "ROLLING_PREFLIGHT_RECEIPT")
        bundle["sourceRawArchive"]["body"]["sequenceGapDetected"] = True
        _resign(bundle, "sourceRawArchive", "SOURCE_RAW_ARCHIVE")
        evidence = _verifier().verify(bundle)
        self.assertIn("HEARTBEAT_JOIN_INCOMPLETE", evidence["verificationBlockers"])
        self.assertIn("ROLLING_PREFLIGHT_JOIN_INCOMPLETE", evidence["verificationBlockers"])
        self.assertIn("SOURCE_RAW_ARCHIVE_JOIN_INCOMPLETE", evidence["verificationBlockers"])

    def test_official_fill_cost_open_and_owned_cleanup_mismatch_block(self) -> None:
        bundle = _bundle()
        terminal = bundle["terminalTruth"]["body"]
        terminal["ownedWorkingOrdersZero"] = False
        terminal["targetQuantity"] = "1"
        terminal["cashKrw"] = "1"
        terminal["officialFills"][0]["feeKrw"] = "999"
        _resign(bundle, "terminalTruth", "TERMINAL_TRUTH")
        evidence = _verifier().verify(bundle)
        self.assertIn("OFFICIAL_ACCOUNT_TRUTH_INCOMPLETE", evidence["verificationBlockers"])
        self.assertIn("OFFICIAL_FILL_ACTION_MISMATCH", evidence["verificationBlockers"])
        self.assertIn("BASELINE_QUANTITY_NOT_RECONCILED", evidence["verificationBlockers"])
        self.assertIn("BASELINE_CASH_OR_COST_NOT_RECONCILED", evidence["verificationBlockers"])

    def test_loss_breach_is_evidence_but_does_not_change_terminal_taxonomy(self) -> None:
        evidence = _verifier().verify(_bundle(sell_price=70_000))
        self.assertEqual(
            OUTCOME_OWNER_LOSS_LIMIT_REACHED, evidence["terminalOutcome"]
        )
        self.assertTrue(evidence["ownerLossTriggerReached"])
        self.assertEqual("10020", evidence["ownerLossKrw"])
        self.assertIn("OWNER_LOSS_LIMIT_REACHED", evidence["verificationBlockers"])
        self.assertFalse(evidence["functionalWiringPassed"])

    def test_missing_cleanup_sell_has_distinct_reconciliation_taxonomy(self) -> None:
        bundle = _bundle()
        bundle["actions"] = bundle["actions"][:1]
        terminal = bundle["terminalTruth"]["body"]
        terminal["officialFills"] = terminal["officialFills"][:1]
        terminal["targetQuantity"] = "1"
        terminal["targetOrderableQuantity"] = "1"
        terminal["cashKrw"] = "19990"
        _resign(bundle, "terminalTruth", "TERMINAL_TRUTH")
        bundle["mutationRecord"] = None
        evidence = _verifier().verify(bundle)
        self.assertEqual(
            OUTCOME_OWNED_CLEANUP_INCOMPLETE, evidence["terminalOutcome"]
        )
        self.assertFalse(evidence["ownedCleanupComplete"])
        self.assertIn(
            "NATURAL_BUY_AND_EXACT_CLEANUP_SELL_REQUIRED",
            evidence["verificationBlockers"],
        )
        self.assertIn(
            "OWNED_CLEANUP_OR_FINAL_DELTA_INCOMPLETE",
            evidence["verificationBlockers"],
        )

    def test_exact_schema_and_keyed_record_integrity_fail_closed(self) -> None:
        extra = _bundle()
        extra["unexpected"] = None
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "bundle-fields-not-exact",
        ):
            _verifier().verify(extra)
        tampered = _bundle()
        tampered["laneSession"]["body"]["state"] = "ACTIVE"
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "record-hash-mismatch",
        ):
            _verifier().verify(tampered)

    def test_negative_costs_and_baselines_fail_closed(self) -> None:
        negative_fee = _bundle()
        negative_fee["terminalTruth"]["body"]["officialFills"][0]["feeKrw"] = "-1"
        _resign(negative_fee, "terminalTruth", "TERMINAL_TRUTH")
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "fill-fee-negative",
        ):
            _verifier().verify(negative_fee)

        negative_baseline = _bundle()
        negative_baseline["baselineTruth"]["body"]["cashKrw"] = "-1"
        _resign(negative_baseline, "baselineTruth", "BASELINE_TRUTH")
        with self.assertRaisesRegex(
            KisDomesticFunctionalTerminalVerifierBlocked,
            "baseline-cash-negative",
        ):
            _verifier().verify(negative_baseline)

    def test_action_gross_and_official_fill_notional_are_recomputed(self) -> None:
        gross_tamper = _bundle()
        gross_tamper["actions"][0]["body"]["grossKrw"] = "79999"
        gross_tamper["actions"][0] = _env(
            "LANE_ACTION", gross_tamper["actions"][0]["body"]
        )
        evidence = _verifier().verify(gross_tamper)
        self.assertIn("ACTION_CAP_OR_BINDING_INVALID", evidence["verificationBlockers"])

        over_cap = _bundle()
        action = over_cap["actions"][0]["body"]
        action["limitPriceKrw"] = "100001"
        action["grossKrw"] = "100001"
        action["fillPriceKrw"] = "100001"
        over_cap["actions"][0] = _env("LANE_ACTION", action)
        fill = over_cap["terminalTruth"]["body"]["officialFills"][0]
        fill["fillPriceKrw"] = "100001"
        _resign(over_cap, "terminalTruth", "TERMINAL_TRUTH")
        evidence = _verifier().verify(over_cap)
        self.assertIn("ACTION_CAP_OR_BINDING_INVALID", evidence["verificationBlockers"])
        self.assertIn("OFFICIAL_FILL_ACTION_MISMATCH", evidence["verificationBlockers"])

    def test_action_fill_and_terminal_time_order_is_enforced(self) -> None:
        bundle = _bundle()
        action = bundle["actions"][0]["body"]
        action["postBoundaryAt"] = _iso(ACTIVATED + timedelta(seconds=3))
        action["filledAt"] = _iso(ACTIVATED + timedelta(seconds=2))
        bundle["actions"][0] = _env("LANE_ACTION", action)
        bundle["terminalTruth"]["body"]["officialFills"][0]["filledAt"] = (
            action["filledAt"]
        )
        _resign(bundle, "terminalTruth", "TERMINAL_TRUTH")
        evidence = _verifier().verify(bundle)
        self.assertIn(
            "NATURAL_BUY_CAUSAL_TIME_ORDER_INVALID",
            evidence["verificationBlockers"],
        )

        terminal_before_cleanup = _bundle()
        terminal_before_cleanup["terminalTruth"]["body"]["observedAt"] = _iso(EXPIRES)
        _resign(terminal_before_cleanup, "terminalTruth", "TERMINAL_TRUTH")
        evidence = _verifier().verify(terminal_before_cleanup)
        self.assertIn("OFFICIAL_ACCOUNT_TRUTH_INCOMPLETE", evidence["verificationBlockers"])

    def test_mutation_records_require_exact_bijection_and_cross_fields(self) -> None:
        bundle = _bundle()
        records = bundle["mutationRecord"]["body"]["records"]
        records[0]["actionKind"] = "CLEANUP_SELL"
        unsigned = dict(records[0])
        unsigned.pop("recordHash")
        records[0]["recordHash"] = _hash(unsigned)
        records[1]["previousHash"] = records[0]["recordHash"]
        unsigned = dict(records[1])
        unsigned.pop("recordHash")
        records[1]["recordHash"] = _hash(unsigned)
        bundle["mutationRecord"]["body"]["recordHeadHash"] = records[1]["recordHash"]
        _resign(bundle, "mutationRecord", "RAW_MUTATION_RECORD")
        evidence = _verifier().verify(bundle)
        self.assertFalse(evidence["rawMutationTruthAvailable"])
        self.assertIn(
            "IMMUTABLE_RAW_MUTATION_RECORD_INVALID",
            evidence["verificationBlockers"],
        )


if __name__ == "__main__":
    unittest.main()
