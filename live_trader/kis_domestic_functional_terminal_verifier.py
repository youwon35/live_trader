from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence

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
from .kis_domestic_functional_readers import (
    ImmutableSqliteComponentArchiveReader,
    ImmutableTruthArchiveReader,
    KisDomesticFunctionalReadersBlocked,
    KisDomesticFunctionalVerifyOnlyReaders,
    READER_OUTPUT_SCHEMA,
)


KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_RELEASE_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PROMOTION_AVAILABLE = False

OUTCOME = "SAFE_INCOMPLETE_NATURAL_SELL_ABSENT"
OUTCOME_OWNER_LOSS_LIMIT_REACHED = "SAFE_INCOMPLETE_OWNER_LOSS_LIMIT_REACHED"
OUTCOME_OWNED_CLEANUP_INCOMPLETE = (
    "RECONCILIATION_REQUIRED_OWNED_CLEANUP_INCOMPLETE"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:@-]{1,160}$", re.ASCII)
_KIS_TRADE_FIELDS = 46
_ZERO_HASH = "0" * 64
_RAW_ADAPTER_COMPONENTS = {
    "lane", "source", "rolling", "heartbeat", "mutation",
    "capability", "quote", "graph", "truth",
}
_RAW_ADAPTER_BODY_KEYS = {
    "schemaVersion", "route", "pdno", "sessionId",
    "accountFingerprint", "preactivationBaselineHash", "recordOrdinal",
    "evidence",
}
_RAW_ADAPTER_COMPONENT_FIELDS = {
    ("rolling", "ROLLING_BASELINE"): {"normalized"},
    ("mutation", "MUTATION_INTEGRITY"): {"baselineOrderKeys"},
    ("capability", "CAPABILITY_REVOKE"): {
        "externallyRevoked", "runtimeReaderConfirmedClear",
        "globalReaderConfirmedClear",
    },
}
_PRODUCTION_BLOCKERS = {
    "PRODUCTION_VERIFY_ONLY_REGISTRY_NOT_PINNED",
    "PRODUCTION_GRAPH_NOT_WIRED",
    "SHARED_KIS_ROUTE_NOT_WIRED",
    "OPERATOR_EXCLUSIVITY_NOT_PROVEN",
    "UPSTREAM_PACKET_COMPLETENESS_UNAVAILABLE",
}

_BUNDLE_KEYS = {
    "schemaVersion",
    "laneSession",
    "bootstrap",
    "approval",
    "evaluation",
    "trigger",
    "actions",
    "heartbeatResult",
    "rollingPreflightReceipt",
    "sourceRawArchive",
    "baselineTruth",
    "terminalTruth",
    "mutationRecord",
    "capabilityRevokeProof",
}

_SESSION_KEYS = {
    "schemaVersion", "route", "origin", "pdno", "sessionId",
    "bootstrapId", "approvalId", "evaluationId", "triggerId", "permitId",
    "permitHash", "accountFingerprint", "preactivationBaselineHash",
    "contractEnvelopeHash", "codeManifestHash", "state", "activatedAt",
    "expiresAt", "cleanupEndsAt", "cleanupStartedAt", "finalizedAt",
    "revision",
}
_BOOTSTRAP_KEYS = {
    "schemaVersion", "route", "bootstrapId", "publicArmId", "evaluationId",
    "triggerId", "approvalId", "sessionId", "preactivationBaselineHash",
    "state", "revision",
}
_APPROVAL_KEYS = {
    "schemaVersion", "route", "approvalId", "bootstrapId", "evaluationId",
    "triggerId", "sessionId", "state", "revision",
}
_EVALUATION_KEYS = {
    "schemaVersion", "route", "pdno", "evaluationId", "publicArmId",
    "signal", "rawWindowHash", "sourceArchiveHash", "barCloseAt",
    "evaluatedAt", "artifactContentHash", "artifactFileSha256",
    "instanceContentHash", "instanceFileSha256", "codeManifestHash", "state",
}
_TRIGGER_KEYS = {
    "schemaVersion", "route", "pdno", "triggerId", "evaluationId",
    "rawTriggerHash", "sourceGeneration", "barOpenAt", "observedAt", "state",
}
_ACTION_KEYS = {
    "schemaVersion", "route", "pdno", "claimId", "sessionId", "actionKind",
    "state", "quantity", "limitPriceKrw", "grossKrw", "evaluationId",
    "triggerId", "brokerOrderId", "fillPriceKrw", "feeKrw", "taxKrw",
    "loanInterestKrw", "createdAt", "postBoundaryAt", "filledAt",
    "rawMutationHash", "officialFillHash", "transitionHeadHash", "revision",
}
_HEARTBEAT_KEYS = {
    "schemaVersion", "route", "pdno", "sessionId", "activatedAt",
    "activeEndsAt", "processGeneration", "socketGeneration", "sampleCount",
    "sampleHeadHash", "actualMonotonicElapsedSeconds", "maxHeartbeatGapSeconds",
    "uninterrupted", "exact7200ObservationPassed", "outcome",
    "functionalTestPassed", "promotionEligible", "releaseAvailable",
}
_ROLLING_KEYS = {
    "schemaVersion", "route", "pdno", "snapshotId", "snapshotHash",
    "diagnosticHash", "captureBundleHash", "accountFingerprint",
    "credentialConfigurationHash", "preactivationBaselineHash",
    "contractEnvelopeHash", "codeManifestHash", "publicArmId", "preapprovalHash",
    "evaluationId", "evaluationHash", "triggerId", "triggerHash",
    "sourceGeneration", "barOpenAt", "completedAt", "expiresAt", "consumedAt",
    "sessionId", "sessionNonceHash", "singleUseConsumed",
    "privateAccountAuthorityAvailable", "tokenAuthorityAvailable",
    "orderAuthorityAvailable", "networkOrderPostAllowed", "tradingMutationCount",
    "finalQuoteAvailable", "releaseEvidenceEligible",
}
_SOURCE_KEYS = {
    "schemaVersion", "route", "pdno", "publicArmId", "evaluationId", "triggerId",
    "sourceGeneration", "rawWindowHash", "rawTriggerHash", "sourceArchiveHash",
    "rawFrameHashes", "rawFrameCount", "firstSourceSequence",
    "lastSourceSequence", "sequenceGapDetected", "duplicateDetected",
    "upstreamExchangeSequenceAvailable", "archiveRecomputedFromAuthenticatedFrames",
}
_BASELINE_KEYS = {
    "schemaVersion", "route", "pdno", "accountFingerprint", "baselineHash",
    "targetQuantity", "targetOrderableQuantity", "cashKrw",
    "accountWideWorkingOrdersZero", "stableRepeatedReads", "costBaselineComplete",
    "captureBundleHash",
}
_FILL_KEYS = {
    "claimId", "brokerOrderId", "side", "quantity", "fillPriceKrw", "feeKrw",
    "taxKrw", "loanInterestKrw", "filledAt", "officialFillHash",
    "terminalFilled",
}
_TERMINAL_KEYS = {
    "schemaVersion", "route", "pdno", "accountFingerprint", "baselineHash",
    "targetQuantity", "targetOrderableQuantity", "cashKrw",
    "accountWideWorkingOrdersZero", "ownedWorkingOrdersZero",
    "stableRepeatedReads", "allPagesComplete", "officialFills", "observedAt",
}
_MUTATION_KEYS = {
    "schemaVersion", "route", "pdno", "sessionId", "records",
    "allPhysicalMutationAttemptsCounted", "postAmbiguityAbsent",
    "nonOwnedMutationObserved", "recordHeadHash",
}
_MUTATION_RECORD_KEYS = {
    "claimId", "actionKind", "requestHash", "responseHash", "brokerOrderId",
    "physicalAttemptCount", "method", "endpoint", "endpointHash", "terminalState",
    "officialFillHash", "previousHash", "recordHash",
}
_CAPABILITY_KEYS = {
    "schemaVersion", "route", "sessionId", "capabilityHash", "revokedAt",
    "runtimeReaderConfirmedClear", "globalReaderConfirmedClear",
    "functionalOrderAuthorityOpen", "recordHash",
}

_SCHEMA_DESCRIPTOR = {
    "bundle": sorted(_BUNDLE_KEYS),
    "laneSession": sorted(_SESSION_KEYS),
    "bootstrap": sorted(_BOOTSTRAP_KEYS),
    "approval": sorted(_APPROVAL_KEYS),
    "evaluation": sorted(_EVALUATION_KEYS),
    "trigger": sorted(_TRIGGER_KEYS),
    "action": sorted(_ACTION_KEYS),
    "heartbeat": sorted(_HEARTBEAT_KEYS),
    "rolling": sorted(_ROLLING_KEYS),
    "source": sorted(_SOURCE_KEYS),
    "baseline": sorted(_BASELINE_KEYS),
    "terminal": sorted(_TERMINAL_KEYS),
    "officialFill": sorted(_FILL_KEYS),
    "mutation": sorted(_MUTATION_KEYS),
    "mutationRecord": sorted(_MUTATION_RECORD_KEYS),
    "capability": sorted(_CAPABILITY_KEYS),
    "rawAdapterBody": sorted(_RAW_ADAPTER_BODY_KEYS),
    "rawAdapterComponentFields": {
        f"{component}:{record_type}": sorted(fields)
        for (component, record_type), fields in sorted(
            _RAW_ADAPTER_COMPONENT_FIELDS.items()
        )
    },
    "rawAdapterComponents": sorted(_RAW_ADAPTER_COMPONENTS),
    "productionBlockers": sorted(_PRODUCTION_BLOCKERS),
}


class KisDomesticFunctionalTerminalVerifierBlocked(RuntimeError):
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
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-json-invalid"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


PROTOCOL_FINGERPRINT = _hash(
    {
        "schemaVersion": "kis-domestic-functional-terminal-verifier-protocol/v1",
        "descriptor": _SCHEMA_DESCRIPTOR,
        "terminalOutcomes": sorted(
            {
                OUTCOME,
                OUTCOME_OWNER_LOSS_LIMIT_REACHED,
                OUTCOME_OWNED_CLEANUP_INCOMPLETE,
            }
        ),
        "naturalSellSupported": False,
        "productionAvailable": False,
    }
)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-not-object")
    return value


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    return value


def _identifier(value: Any, label: str) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    return value


def _decimal(value: Any, label: str) -> Decimal:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-invalid"
        ) from exc
    if not parsed.is_finite() or format(parsed, "f") != value:
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    return parsed


def _nonnegative_decimal(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed < 0:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-negative"
        )
    return parsed


def _positive_decimal(value: Any, label: str) -> Decimal:
    parsed = _decimal(value, label)
    if parsed <= 0:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-not-positive"
        )
    return parsed


def _time(value: Any, label: str) -> datetime:
    if type(value) is not str:
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-invalid"
        ) from exc
    if parsed.tzinfo is None or not math.isfinite(parsed.timestamp()):
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    canonical = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if canonical != value:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-not-canonical-utc"
        )
    return parsed.astimezone(timezone.utc)


def _exact_bool(body: Mapping[str, Any], key: str, expected: bool) -> bool:
    return type(body.get(key)) is bool and body.get(key) is expected


def _verify_envelope(
    envelope_value: Any,
    *,
    domain: str,
    keys: set[str],
    verifier: Callable[[str, Mapping[str, Any], str], bool],
    label: str,
) -> tuple[dict[str, Any], str]:
    envelope = _mapping(envelope_value, f"{label}-envelope")
    if set(envelope) != {"body", "recordHash", "signature"}:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-envelope-not-exact"
        )
    body = dict(_mapping(envelope.get("body"), f"{label}-body"))
    if set(body) != keys:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-fields-not-exact"
        )
    record_hash = _sha(envelope.get("recordHash"), f"{label}-record-hash")
    signature = _sha(envelope.get("signature"), f"{label}-signature")
    if not hmac.compare_digest(record_hash, _hash(body)):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-record-hash-mismatch"
        )
    try:
        valid = verifier(domain, {**body, "recordHash": record_hash}, signature)
    except BaseException:
        valid = False
    if valid is not True:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"{label}-signature-invalid"
        )
    return body, record_hash


class KisDomesticFunctionalTerminalArchiveAdapters:
    """Exact immutable-archive seam into the frozen verify-only reader.

    The terminal verifier never accepts caller-declared booleans as raw truth.
    This adapter owns only exact concrete reader/archive objects; each archive
    is reopened immutable and every row, projection, signature and file hash is
    reverified by ``KisDomesticFunctionalVerifyOnlyReaders``.
    """

    def __init__(
        self,
        *,
        verify_only_readers: KisDomesticFunctionalVerifyOnlyReaders,
        sqlite_archives: Mapping[str, ImmutableSqliteComponentArchiveReader],
        truth_archive: ImmutableTruthArchiveReader,
    ) -> None:
        if type(verify_only_readers) is not KisDomesticFunctionalVerifyOnlyReaders:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-archive-reader-type-invalid"
            )
        expected = _RAW_ADAPTER_COMPONENTS - {"truth"}
        if not isinstance(sqlite_archives, Mapping) or set(sqlite_archives) != expected:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-sqlite-archive-adapters-not-exact"
            )
        copied: dict[str, ImmutableSqliteComponentArchiveReader] = {}
        for component in sorted(expected):
            value = sqlite_archives[component]
            if (
                type(value) is not ImmutableSqliteComponentArchiveReader
                or value.component != component
            ):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-archive-adapter-type-invalid:{component}"
                )
            copied[component] = value
        if type(truth_archive) is not ImmutableTruthArchiveReader:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-archive-adapter-type-invalid"
            )
        self._readers = verify_only_readers
        self._sqlite = copied
        self._truth = truth_archive

    def read(
        self,
        *,
        session_id: str,
        account_fingerprint: str,
        preactivation_baseline_hash: str,
    ) -> dict[str, Any]:
        try:
            result = self._readers.read_from_archives(
                session_id=session_id,
                account_fingerprint=account_fingerprint,
                preactivation_baseline_hash=preactivation_baseline_hash,
                sqlite_archives=self._sqlite,
                truth_archive=self._truth,
            )
        except KisDomesticFunctionalReadersBlocked as exc:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                f"terminal-immutable-reader-rejected:{exc}"
            ) from None
        return deepcopy(result)


def _adapter_record(
    raw_records: Mapping[str, Any], component: str, record_type: str,
    *, many: bool = False,
) -> Any:
    values = raw_records.get(component)
    if not isinstance(values, list):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"terminal-adapter-record-list-invalid:{component}"
        )
    selected = []
    for value in values:
        body = dict(_mapping(value, f"terminal-adapter-record:{component}"))
        evidence = _mapping(
            body.get("evidence"), f"terminal-adapter-evidence:{component}"
        )
        expected_fields = _RAW_ADAPTER_BODY_KEYS | _RAW_ADAPTER_COMPONENT_FIELDS.get(
            (component, str(evidence.get("recordType"))), set()
        )
        if set(body) != expected_fields:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                f"terminal-adapter-record-fields-not-exact:{component}"
            )
        if body.get("schemaVersion") != (
            "kis-domestic-functional-terminal-adapter-record/v1"
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                f"terminal-adapter-record-schema-invalid:{component}"
            )
        if evidence.get("recordType") == record_type:
            selected.append(dict(evidence))
    if many:
        return selected
    if len(selected) != 1:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"terminal-adapter-cardinality-invalid:{component}:{record_type}"
        )
    return selected[0]


def _verify_raw_heartbeat(
    evidence: Mapping[str, Any], summary: Mapping[str, Any],
) -> None:
    keys = {
        "recordType", "schemaVersion", "activatedAt", "activeEndsAt",
        "processGeneration", "socketGeneration", "sampleHeadHash", "samples",
    }
    if set(evidence) != keys or evidence.get("recordType") != "HEARTBEAT_EVIDENCE":
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-heartbeat-raw-schema-invalid"
        )
    if evidence.get("schemaVersion") != "kis-domestic-terminal-heartbeat-raw/v1":
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-heartbeat-raw-version-invalid"
        )
    samples = evidence.get("samples")
    if not isinstance(samples, list) or len(samples) < 2:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-heartbeat-samples-incomplete"
        )
    previous = _ZERO_HASH
    previous_wall: datetime | None = None
    previous_mono: int | None = None
    maximum_gap = 0
    for ordinal, item_value in enumerate(samples, 1):
        item = dict(_mapping(item_value, "terminal-heartbeat-sample"))
        if set(item) != {
            "sequence", "kind", "wallAt", "monotonicNs", "previousHash",
            "recordHash",
        }:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-heartbeat-sample-fields-not-exact"
            )
        unsigned = {key: item[key] for key in item if key != "recordHash"}
        if (
            type(item.get("sequence")) is not int
            or item["sequence"] != ordinal
            or type(item.get("monotonicNs")) is not int
            or item["monotonicNs"] < 0
            or item.get("previousHash") != previous
            or item.get("recordHash") != _hash(unsigned)
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-heartbeat-sample-chain-invalid"
            )
        wall = _time(item.get("wallAt"), "terminal-heartbeat-wall")
        mono = item["monotonicNs"]
        if previous_wall is not None and previous_mono is not None:
            mono_delta = mono - previous_mono
            wall_delta = Decimal(str((wall - previous_wall).total_seconds()))
            if mono_delta < 0 or abs(
                wall_delta - Decimal(mono_delta) / Decimal(1_000_000_000)
            ) > Decimal("1"):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "terminal-heartbeat-clock-lineage-invalid"
                )
            maximum_gap = max(maximum_gap, mono_delta)
        previous = item["recordHash"]
        previous_wall = wall
        previous_mono = mono
    kinds = [item["kind"] for item in samples]
    activated = _time(evidence.get("activatedAt"), "raw-heartbeat-activated")
    ends = _time(evidence.get("activeEndsAt"), "raw-heartbeat-ends")
    elapsed = Decimal(samples[-1]["monotonicNs"] - samples[0]["monotonicNs"]) / Decimal(
        1_000_000_000
    )
    if (
        kinds[0] != "ACTIVE_START"
        or kinds[-1] != "ACTIVE_END_OBSERVED"
        or any(kind != "HEARTBEAT" for kind in kinds[1:-1])
        or _time(samples[0]["wallAt"], "raw-heartbeat-first") != activated
        or _time(samples[-1]["wallAt"], "raw-heartbeat-last") != ends
        or ends - activated != timedelta(seconds=ACTIVE_SECONDS)
        or elapsed < Decimal(ACTIVE_SECONDS)
        or maximum_gap > 15 * 1_000_000_000
        or evidence.get("sampleHeadHash") != previous
        or summary.get("sampleHeadHash") != previous
        or summary.get("sampleCount") != len(samples)
        or _decimal(summary.get("actualMonotonicElapsedSeconds"), "raw-heartbeat-summary-elapsed")
        != elapsed
        or _decimal(summary.get("maxHeartbeatGapSeconds"), "raw-heartbeat-summary-gap")
        != Decimal(maximum_gap) / Decimal(1_000_000_000)
        or summary.get("activatedAt") != evidence.get("activatedAt")
        or summary.get("activeEndsAt") != evidence.get("activeEndsAt")
        or summary.get("processGeneration") != evidence.get("processGeneration")
        or summary.get("socketGeneration") != evidence.get("socketGeneration")
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-heartbeat-raw-summary-mismatch"
        )


def _source_event_hash_body(
    *, generation: str, socket_hash: str, sequence: str, record_index: int,
    raw_frame_hash: str, fields: list[str], received_at: str,
) -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-h0stcnt0-raw-event/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "sourceGeneration": generation,
        "socketIdentityHash": socket_hash,
        "sourceSequence": sequence,
        "recordIndex": record_index,
        "rawFrameHash": raw_frame_hash,
        "recordFields": fields,
        "receivedAt": received_at,
    }


def _verify_raw_source(
    evidence: Mapping[str, Any], source: Mapping[str, Any],
    evaluation: Mapping[str, Any], trigger: Mapping[str, Any],
) -> None:
    keys = {
        "recordType", "schemaVersion", "sourceGeneration",
        "socketIdentityHash", "frames", "events", "recomputedBars",
        "nextOpenEvent", "rawWindowHash", "rawTriggerHash",
        "sourceArchiveHash",
    }
    if (
        set(evidence) != keys
        or evidence.get("recordType") != "SOURCE_WINDOW_ARCHIVE"
        or evidence.get("schemaVersion") != "kis-domestic-terminal-source-raw/v1"
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-source-raw-schema-invalid"
        )
    frames = evidence.get("frames")
    expected_events = evidence.get("events")
    if not isinstance(frames, list) or not frames or not isinstance(expected_events, list):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-source-raw-records-missing"
        )
    generation = str(evidence.get("sourceGeneration"))
    socket_hash = _sha(evidence.get("socketIdentityHash"), "source-socket-hash")
    events: list[dict[str, Any]] = []
    envelope_hashes: list[str] = []
    sequence = 1
    previous_head = _ZERO_HASH
    for frame_index, frame_value in enumerate(frames, 1):
        frame = dict(_mapping(frame_value, "terminal-source-frame"))
        if set(frame) != {
            "schemaVersion", "route", "pdno", "trId", "sourceGeneration",
            "socketIdentityHash", "frameIndex", "firstSourceSequence",
            "lastSourceSequence", "recordCount", "receivedAt", "rawFrame",
            "rawFrameHash", "recordFields", "previousFrameHeadHash",
            "frameEnvelopeHash", "frameHeadHash",
        }:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-source-frame-fields-not-exact"
            )
        raw = frame.get("rawFrame")
        if type(raw) is not str:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-source-frame-raw-invalid"
            )
        pieces = raw.split("|", 3)
        try:
            count = int(pieces[2])
        except (IndexError, ValueError):
            count = 0
        fields = pieces[3].split("^") if len(pieces) == 4 else []
        records = [
            fields[index * _KIS_TRADE_FIELDS:(index + 1) * _KIS_TRADE_FIELDS]
            for index in range(count)
        ]
        unsigned = {key: frame[key] for key in frame if key not in {"frameEnvelopeHash", "frameHeadHash"}}
        envelope_hash = _hash(unsigned)
        head = _hash({
            "previousHash": previous_head,
            "frameEnvelopeHash": envelope_hash,
            "frameIndex": frame_index,
        })
        raw_hash = hashlib.sha256(raw.encode()).hexdigest()
        if (
            pieces[:2] != ["0", "H0STCNT0"]
            or count < 1
            or len(fields) != count * _KIS_TRADE_FIELDS
            or frame.get("recordFields") != records
            or frame.get("recordCount") != count
            or frame.get("frameIndex") != frame_index
            or frame.get("route") != ROUTE
            or frame.get("pdno") != PDNO
            or frame.get("trId") != "H0STCNT0"
            or frame.get("sourceGeneration") != generation
            or frame.get("socketIdentityHash") != socket_hash
            or frame.get("rawFrameHash") != raw_hash
            or frame.get("firstSourceSequence") != str(sequence)
            or frame.get("lastSourceSequence") != str(sequence + count - 1)
            or frame.get("previousFrameHeadHash") != previous_head
            or frame.get("frameEnvelopeHash") != envelope_hash
            or frame.get("frameHeadHash") != head
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-source-frame-chain-invalid"
            )
        received = _time(frame.get("receivedAt"), "terminal-source-received")
        for index, fields_row in enumerate(records):
            if (
                fields_row[0] != PDNO
                or not re.fullmatch(r"[0-9]{6}", fields_row[1] or "")
                or not re.fullmatch(r"[0-9]{8}", fields_row[33] or "")
            ):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "terminal-source-event-identity-or-time-invalid"
                )
            local = datetime.strptime(
                fields_row[33] + fields_row[1], "%Y%m%d%H%M%S"
            ).replace(tzinfo=timezone(timedelta(hours=9)))
            trade = local.astimezone(timezone.utc)
            if not timedelta(0) <= received - trade <= timedelta(seconds=2):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "terminal-source-event-observation-lag-invalid"
                )
            minute = (local.minute // 5) * 5
            opened = local.replace(minute=minute, second=0, microsecond=0).astimezone(timezone.utc)
            closed = opened + timedelta(minutes=5)
            event_hash = _hash(_source_event_hash_body(
                generation=generation, socket_hash=socket_hash,
                sequence=str(sequence), record_index=index,
                raw_frame_hash=raw_hash, fields=fields_row,
                received_at=frame["receivedAt"],
            ))
            events.append({
                "sourceSequence": str(sequence), "recordIndex": index,
                "rawFrameHash": raw_hash, "rawEventHash": event_hash,
                "tradeAt": trade.isoformat().replace("+00:00", "Z"),
                "receivedAt": frame["receivedAt"],
                "bucketOpenAt": opened.isoformat().replace("+00:00", "Z"),
                "bucketCloseAt": closed.isoformat().replace("+00:00", "Z"),
                "recordFields": fields_row,
            })
            sequence += 1
        envelope_hashes.append(envelope_hash)
        previous_head = head
    if events != expected_events:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-source-frame-event-bijection-invalid"
        )
    bars: list[dict[str, Any]] = []
    for event in events[:-1]:
        if not bars or bars[-1]["openAt"] != event["bucketOpenAt"]:
            bars.append({
                "openAt": event["bucketOpenAt"], "closeAt": event["bucketCloseAt"],
                "open": event["recordFields"][2], "high": event["recordFields"][2],
                "low": event["recordFields"][2], "close": event["recordFields"][2],
                "sourceSequenceStart": event["sourceSequence"],
                "sourceSequenceEnd": event["sourceSequence"], "eventCount": 1,
                "rawEventChainHash": _hash({
                    "previousHash": _ZERO_HASH,
                    "rawEventHash": event["rawEventHash"],
                    "sourceSequence": event["sourceSequence"],
                }),
            })
        else:
            bar = bars[-1]
            price = _positive_decimal(event["recordFields"][2], "source-price")
            bar["high"] = format(max(_decimal(bar["high"], "source-high"), price), "f")
            bar["low"] = format(min(_decimal(bar["low"], "source-low"), price), "f")
            bar["close"] = format(price, "f")
            bar["sourceSequenceEnd"] = event["sourceSequence"]
            bar["eventCount"] += 1
            bar["rawEventChainHash"] = _hash({
                "previousHash": bar["rawEventChainHash"],
                "rawEventHash": event["rawEventHash"],
                "sourceSequence": event["sourceSequence"],
            })
    next_open = events[-1]
    ranges = [
        _decimal(bar["high"], "source-high") - _decimal(bar["low"], "source-low")
        for bar in bars[:-1]
    ]
    threshold = _decimal(bars[-2]["close"], "source-prior-close") + (
        sum(ranges, Decimal("0")) / Decimal(len(ranges)) * Decimal("0.3")
    )
    raw_window = _hash({"sourceGeneration": generation, "bars": bars})
    raw_trigger = _hash(next_open)
    expected_archive = _hash({
        "sourceGeneration": generation,
        "rawWindowHash": raw_window,
        "rawTriggerHash": raw_trigger,
        "rawFrameHashes": envelope_hashes,
        "firstSourceSequence": bars[0]["sourceSequenceStart"],
        "lastSourceSequence": bars[-1]["sourceSequenceEnd"],
    })
    if (
        len(bars) != 11
        or any(left["closeAt"] != right["openAt"] for left, right in zip(bars, bars[1:]))
        or next_open["bucketOpenAt"] != bars[-1]["closeAt"]
        or _decimal(bars[-1]["high"], "source-breakout-high") < threshold
        or evidence.get("recomputedBars") != bars
        or evidence.get("nextOpenEvent") != next_open
        or evidence.get("rawWindowHash") != raw_window
        or evidence.get("rawTriggerHash") != raw_trigger
        or evidence.get("sourceArchiveHash") != expected_archive
        or source.get("rawFrameHashes") != envelope_hashes
        or source.get("rawFrameCount") != len(envelope_hashes)
        or source.get("firstSourceSequence") != bars[0]["sourceSequenceStart"]
        or source.get("lastSourceSequence") != bars[-1]["sourceSequenceEnd"]
        or source.get("sourceGeneration") != generation
        or source.get("rawWindowHash") != raw_window
        or source.get("rawTriggerHash") != raw_trigger
        or source.get("sourceArchiveHash") != expected_archive
        or evaluation.get("rawWindowHash") != raw_window
        or evaluation.get("barCloseAt") != bars[-1]["closeAt"]
        or trigger.get("rawTriggerHash") != raw_trigger
        or trigger.get("sourceGeneration") != generation
        or trigger.get("barOpenAt") != next_open["bucketOpenAt"]
        or trigger.get("observedAt") != next_open["receivedAt"]
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-source-raw-reduction-mismatch"
        )


_TRUTH_ENDPOINTS = {
    "balance": ("/uapi/domestic-stock/v1/trading/inquire-balance", "TTTC8434R"),
    "dailyCcld": ("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "TTTC0081R"),
    "workingOrders": ("/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl", "TTTC0084R"),
    "periodTradeProfit": ("/uapi/domestic-stock/v1/trading/inquire-period-trade-profit", "TTTC8715R"),
    "periodProfit": ("/uapi/domestic-stock/v1/trading/inquire-period-profit", "TTTC8708R"),
    "holiday": ("/uapi/domestic-stock/v1/quotations/chk-holiday", "CTCA0903R"),
}


def _reduce_raw_capture(capture_value: Any, phase: str) -> dict[str, Any]:
    capture = dict(_mapping(capture_value, f"terminal-{phase}-capture"))
    if set(capture) != {"schemaVersion", "phase", "endpoints", "captureHash"}:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-capture-fields-not-exact"
        )
    unsigned_capture = {key: capture[key] for key in capture if key != "captureHash"}
    if (
        capture.get("schemaVersion") != "kis-domestic-terminal-official-rest-capture/v1"
        or capture.get("phase") != phase
        or capture.get("captureHash") != _hash(unsigned_capture)
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-capture-hash-invalid"
        )
    endpoints = _mapping(capture.get("endpoints"), "terminal-truth-endpoints")
    if set(endpoints) != set(_TRUTH_ENDPOINTS):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-endpoint-set-incomplete"
        )
    reduced: dict[str, Any] = {}
    for name, (endpoint, tr_id) in _TRUTH_ENDPOINTS.items():
        pages = endpoints.get(name)
        if not isinstance(pages, list) or not pages:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                f"terminal-truth-pages-missing:{name}"
            )
        rows: list[Mapping[str, Any]] = []
        summary: Mapping[str, Any] | None = None
        previous_cursor = ""
        for index, page_value in enumerate(pages):
            page = dict(_mapping(page_value, f"terminal-truth-page:{name}"))
            if set(page) != {
                "schemaVersion", "method", "origin", "endpoint", "trId",
                "pageIndex", "continuationSent", "continuationReceived",
                "cursorReceived", "statusCode", "effectiveUrl",
                "rawResponseBody", "rawResponseSha256", "rows", "summary",
                "pageHash",
            }:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-page-fields-not-exact:{name}"
                )
            unsigned = {key: page[key] for key in page if key != "pageHash"}
            continuation = page.get("continuationReceived")
            expected_sent = "" if index == 0 else "N"
            raw_body = page.get("rawResponseBody")
            try:
                parsed_body = json.loads(raw_body) if type(raw_body) is str else None
            except json.JSONDecodeError:
                parsed_body = None
            if (
                page.get("schemaVersion") != "kis-domestic-terminal-official-rest-page/v1"
                or page.get("method") != "GET"
                or page.get("origin") != LIVE_ORIGIN
                or page.get("endpoint") != endpoint
                or page.get("trId") != tr_id
                or page.get("pageIndex") != index
                or page.get("continuationSent") != expected_sent
                or continuation not in {"", "M", "F", "D", "E"}
                or page.get("statusCode") != 200
                or page.get("effectiveUrl") != LIVE_ORIGIN + endpoint
                or type(raw_body) is not str
                or page.get("rawResponseSha256")
                != hashlib.sha256(raw_body.encode("utf-8")).hexdigest()
                or not isinstance(parsed_body, Mapping)
                or set(parsed_body) != {"rows", "summary"}
                or parsed_body.get("rows") != page.get("rows")
                or parsed_body.get("summary") != page.get("summary")
                or page.get("pageHash") != _hash(unsigned)
                or not isinstance(page.get("rows"), list)
                or not isinstance(page.get("summary"), Mapping)
            ):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-page-invalid:{name}"
                )
            cursor = page.get("cursorReceived")
            if type(cursor) is not str:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-cursor-invalid:{name}"
                )
            if index and not previous_cursor:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-page-chain-truncated:{name}"
                )
            if index < len(pages) - 1 and continuation not in {"M", "F"}:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-premature-terminal-page:{name}"
                )
            if index == len(pages) - 1 and continuation not in {"", "D", "E"}:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-pagination-incomplete:{name}"
                )
            rows.extend(_mapping(row, f"terminal-truth-row:{name}") for row in page["rows"])
            if summary is None:
                summary = dict(page["summary"])
            elif dict(page["summary"]) != dict(summary):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    f"terminal-truth-summary-changed-across-pages:{name}"
                )
            previous_cursor = cursor
        reduced[name] = {"rows": [dict(row) for row in rows], "summary": dict(summary or {})}
    return reduced


def _truth_decimal(value: Any, label: str) -> Decimal:
    if type(value) is not str:
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation as exc:
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid") from exc
    if not parsed.is_finite():
        raise KisDomesticFunctionalTerminalVerifierBlocked(f"{label}-invalid")
    return parsed


def _raw_truth_projection(reduced: Mapping[str, Any]) -> dict[str, Any]:
    balance_rows = reduced["balance"]["rows"]
    targets = [row for row in balance_rows if row.get("pdno") == PDNO]
    if len(targets) > 1:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-target-balance-duplicated"
        )
    target = targets[0] if targets else {"hldg_qty": "0", "ord_psbl_qty": "0"}
    quantity = _truth_decimal(target.get("hldg_qty"), "raw-target-quantity")
    orderable = _truth_decimal(target.get("ord_psbl_qty"), "raw-target-orderable")
    cash = _truth_decimal(
        reduced["balance"]["summary"].get("dnca_tot_amt"), "raw-cash"
    )
    if (
        quantity < 0
        or orderable < 0
        or cash < 0
        or quantity != quantity.to_integral_value()
        or orderable != orderable.to_integral_value()
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-balance-quantity-or-cash-invalid"
        )
    if reduced["workingOrders"]["rows"]:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-account-working-orders-not-zero"
        )
    holiday = reduced["holiday"]["rows"]
    if len(holiday) != 1 or holiday[0].get("opnd_yn") != "Y":
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-trading-day-not-open"
        )
    orders: list[dict[str, Any]] = []
    identities: set[tuple[str, str, str]] = set()
    for row_value in reduced["dailyCcld"]["rows"]:
        row = dict(row_value)
        identity = (row.get("ord_dt"), row.get("ord_gno_brno"), row.get("odno"))
        if (
            any(type(item) is not str or not re.fullmatch(r"[0-9]{1,16}", item) for item in identity)
            or len(identity[0]) != 8
            or identity in identities
            or type(row.get("pdno")) is not str
            or re.fullmatch(r"[0-9]{6}", row.get("pdno")) is None
            or row.get("sll_buy_dvsn_cd") not in {"01", "02"}
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-official-order-identity-invalid"
            )
        identities.add(identity)
        raw_requested = _truth_decimal(row.get("ord_qty"), "raw-order-qty")
        raw_filled = _truth_decimal(row.get("tot_ccld_qty"), "raw-filled-qty")
        remainder_values = [
            _truth_decimal(row.get(key), f"raw-order-{key}")
            for key in ("rmn_qty", "cncl_cfrm_qty", "rjct_qty")
        ]
        if any(
            value < 0 or value != value.to_integral_value()
            for value in [raw_requested, raw_filled, *remainder_values]
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-order-quantity-invalid"
            )
        requested = int(raw_requested)
        filled = int(raw_filled)
        remainder = sum(int(value) for value in remainder_values)
        amount = _truth_decimal(row.get("tot_ccld_amt"), "raw-filled-amount")
        average = _truth_decimal(row.get("avg_prvs"), "raw-average-price")
        filled_at = row.get("filled_at")
        _time(filled_at, "raw-order-filled-at")
        if (
            amount <= 0
            or average <= 0
            or requested != 1
            or requested != filled + remainder
            or filled != 1
            or average * filled != amount
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-order-quantity-or-amount-invalid"
            )
        orders.append({
            "brokerOrderId": ":".join(identity),
            "pdno": row["pdno"],
            "side": "BUY" if row["sll_buy_dvsn_cd"] == "02" else "SELL",
            "quantity": Decimal(filled),
            "amount": amount,
            "price": average,
            "filledAt": filled_at,
        })
    cost_keys = {
        "buyAmtKrw", "sellAmtKrw", "buyFeeKrw", "sellFeeKrw",
        "buyTaxKrw", "sellTaxKrw", "loanInterestKrw", "realizedProfitLossKrw",
    }
    cost_summaries = []
    for name in ("periodTradeProfit", "periodProfit"):
        value = reduced[name]["summary"]
        if set(value) != cost_keys:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-cost-summary-fields-not-exact"
            )
        parsed_costs = {
            key: _truth_decimal(value[key], f"raw-cost-{key}")
            for key in cost_keys
        }
        if any(
            parsed < 0
            for key, parsed in parsed_costs.items()
            if key != "realizedProfitLossKrw"
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-cost-negative"
            )
        cost_summaries.append(parsed_costs)
    if cost_summaries[0] != cost_summaries[1]:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-independent-cost-tr-disagreement"
        )
    costs = cost_summaries[0]
    target_orders = [row for row in orders if row["pdno"] == PDNO]
    buy_amount = sum(
        (row["amount"] for row in target_orders if row["side"] == "BUY"),
        Decimal("0"),
    )
    sell_amount = sum(
        (row["amount"] for row in target_orders if row["side"] == "SELL"),
        Decimal("0"),
    )
    realized = (
        sell_amount - costs["sellFeeKrw"] - costs["sellTaxKrw"]
        - buy_amount - costs["buyFeeKrw"] - costs["buyTaxKrw"]
        - costs["loanInterestKrw"]
    )
    if (
        costs["buyAmtKrw"] != buy_amount
        or costs["sellAmtKrw"] != sell_amount
        or costs["realizedProfitLossKrw"] != realized
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-order-cost-reduction-mismatch"
        )
    return {
        "quantity": quantity, "orderable": orderable, "cash": cash,
        "orders": orders, "costs": costs, "ownerLoss": max(Decimal("0"), -realized),
    }


def _verify_raw_truth(
    baseline_evidence: Mapping[str, Any], terminal_evidence: Mapping[str, Any],
    baseline: Mapping[str, Any], terminal: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]], owner_loss: Decimal,
) -> None:
    expected_keys = {"recordType", "schemaVersion", "captures"}
    if (
        set(baseline_evidence) != expected_keys
        or set(terminal_evidence) != expected_keys
        or baseline_evidence.get("recordType") != "PREACTIVATION_BASELINE"
        or terminal_evidence.get("recordType") != "TERMINAL_TRUTH"
        or baseline_evidence.get("schemaVersion") != "kis-domestic-terminal-official-rest-raw/v1"
        or terminal_evidence.get("schemaVersion") != "kis-domestic-terminal-official-rest-raw/v1"
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-raw-adapter-schema-invalid"
        )
    baseline_captures = baseline_evidence.get("captures")
    terminal_captures = terminal_evidence.get("captures")
    if (
        not isinstance(baseline_captures, list) or len(baseline_captures) != 2
        or not isinstance(terminal_captures, list) or len(terminal_captures) != 2
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-stable-pair-missing"
        )
    before_values = [
        _raw_truth_projection(_reduce_raw_capture(item, "PREACTIVATION"))
        for item in baseline_captures
    ]
    after_values = [
        _raw_truth_projection(_reduce_raw_capture(item, "TERMINAL"))
        for item in terminal_captures
    ]
    if before_values[0] != before_values[1] or after_values[0] != after_values[1]:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-causal-fields-not-stable"
        )
    before = before_values[0]
    after = after_values[0]
    before_orders = {item["brokerOrderId"]: item for item in before["orders"]}
    after_orders = {item["brokerOrderId"]: item for item in after["orders"]}
    if (
        len(before_orders) != len(before["orders"])
        or len(after_orders) != len(after["orders"])
        or not set(before_orders).issubset(after_orders)
        or any(after_orders[key] != value for key, value in before_orders.items())
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-preactivation-order-baseline-not-preserved"
        )
    new_orders = [
        after_orders[key] for key in sorted(set(after_orders) - set(before_orders))
    ]
    action_by_broker = {str(item.get("brokerOrderId")): item for item in actions}
    if set(action_by_broker) != {item["brokerOrderId"] for item in new_orders}:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-action-official-order-bijection-invalid"
        )
    for raw_order in new_orders:
        action = action_by_broker[raw_order["brokerOrderId"]]
        if (
            raw_order["pdno"] != PDNO
            or raw_order["side"] != ("BUY" if action.get("actionKind") == "NATURAL_BUY" else "SELL")
            or raw_order["quantity"] != _decimal(action.get("quantity"), "raw-action-quantity")
            or raw_order["price"] != _decimal(action.get("fillPriceKrw"), "raw-action-fill-price")
            or raw_order["filledAt"] != action.get("filledAt")
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-action-official-order-fields-mismatch"
            )
    delta_costs = {
        key: after["costs"][key] - before["costs"][key]
        for key in after["costs"]
    }
    for key, value in delta_costs.items():
        if key != "realizedProfitLossKrw" and value < 0:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-truth-cumulative-cost-regressed"
            )
    buys = [item for item in actions if item.get("actionKind") == "NATURAL_BUY"]
    sells = [item for item in actions if item.get("actionKind") == "CLEANUP_SELL"]
    if len(buys) != 1 or len(sells) != 1:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-owned-action-cardinality-invalid"
        )
    expected_fees = {
        "buyFeeKrw": _decimal(buys[0].get("feeKrw"), "raw-buy-fee"),
        "sellFeeKrw": _decimal(sells[0].get("feeKrw"), "raw-sell-fee"),
        "buyTaxKrw": _decimal(buys[0].get("taxKrw"), "raw-buy-tax"),
        "sellTaxKrw": _decimal(sells[0].get("taxKrw"), "raw-sell-tax"),
        "loanInterestKrw": _decimal(buys[0].get("loanInterestKrw"), "raw-buy-loan")
        + _decimal(sells[0].get("loanInterestKrw"), "raw-sell-loan"),
    }
    if any(delta_costs[key] != value for key, value in expected_fees.items()):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-action-cost-bijection-invalid"
        )
    buy_amount = sum(
        _decimal(item.get("fillPriceKrw"), "raw-buy-amount")
        * _decimal(item.get("quantity"), "raw-buy-quantity")
        for item in buys
    )
    sell_amount = sum(
        _decimal(item.get("fillPriceKrw"), "raw-sell-amount")
        * _decimal(item.get("quantity"), "raw-sell-quantity")
        for item in sells
    )
    if (
        delta_costs["buyAmtKrw"] != buy_amount
        or delta_costs["sellAmtKrw"] != sell_amount
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-action-amount-bijection-invalid"
        )
    expected_cash = before["cash"] + delta_costs["realizedProfitLossKrw"]
    raw_owner_loss = max(Decimal("0"), -delta_costs["realizedProfitLossKrw"])
    if (
        before["quantity"] != _decimal(baseline.get("targetQuantity"), "raw-baseline-quantity")
        or before["orderable"] != _decimal(baseline.get("targetOrderableQuantity"), "raw-baseline-orderable")
        or before["cash"] != _decimal(baseline.get("cashKrw"), "raw-baseline-cash")
        or after["quantity"] != _decimal(terminal.get("targetQuantity"), "raw-terminal-quantity")
        or after["orderable"] != _decimal(terminal.get("targetOrderableQuantity"), "raw-terminal-orderable")
        or after["cash"] != _decimal(terminal.get("cashKrw"), "raw-terminal-cash")
        or after["cash"] != expected_cash
        or raw_owner_loss != owner_loss
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-truth-baseline-cash-loss-reduction-mismatch"
        )


def _verify_adapter_record_hash(
    evidence: Mapping[str, Any], *, record_type: str, record_hash: str,
) -> None:
    if (
        set(evidence) != {"recordType", "schemaVersion", "recordHash"}
        or evidence.get("recordType") != record_type
        or evidence.get("schemaVersion")
        != "kis-domestic-terminal-record-reference/v1"
        or evidence.get("recordHash") != record_hash
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            f"terminal-adapter-record-reference-invalid:{record_type}"
        )


def _verify_archive_lineage(
    raw_records: Mapping[str, Any], *, records: Mapping[str, Mapping[str, Any]],
    hashes: Mapping[str, str], actions: Sequence[Mapping[str, Any]],
    session_id: str, raw_mutation: bool, capability_revoked: bool,
) -> None:
    lane_references = {
        "LANE_SESSION": "laneSession",
        "BOOTSTRAP": "bootstrap",
        "APPROVAL": "approval",
        "EVALUATION": "evaluation",
        "TRIGGER": "trigger",
    }
    for record_type, name in lane_references.items():
        _verify_adapter_record_hash(
            _adapter_record(raw_records, "lane", record_type),
            record_type=record_type,
            record_hash=hashes[name],
        )
    lane_actions = _adapter_record(raw_records, "lane", "ACTION", many=True)
    if len(lane_actions) != len(actions):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-lane-action-adapter-cardinality-invalid"
        )
    action_hashes = {str(item.get("claimId")): str(item.get("_recordHash")) for item in actions}
    observed_lane_actions: dict[str, str] = {}
    for evidence in lane_actions:
        if set(evidence) != {
            "recordType", "schemaVersion", "claimId", "recordHash",
        } or evidence.get("recordType") != "ACTION" or evidence.get(
            "schemaVersion"
        ) != "kis-domestic-terminal-action-reference/v1":
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-lane-action-adapter-schema-invalid"
            )
        claim = _identifier(evidence.get("claimId"), "adapter-action-claim")
        if claim in observed_lane_actions:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-lane-action-adapter-duplicate"
            )
        observed_lane_actions[claim] = _sha(
            evidence.get("recordHash"), "adapter-action-record-hash"
        )
    if observed_lane_actions != action_hashes:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-lane-action-adapter-bijection-invalid"
        )

    rolling = _adapter_record(raw_records, "rolling", "ROLLING_CONSUMPTION")
    _verify_adapter_record_hash(
        rolling, record_type="ROLLING_CONSUMPTION",
        record_hash=hashes["rollingPreflightReceipt"],
    )
    quote = _adapter_record(raw_records, "quote", "QUOTE_RECEIPT")
    if set(quote) != {
        "recordType", "schemaVersion", "sessionId", "evaluationHash",
        "triggerHash", "rollingReceiptHash", "quoteReceiptHash", "consumed",
        "orderAuthorityFresh",
    } or not (
        quote.get("recordType") == "QUOTE_RECEIPT"
        and quote.get("schemaVersion") == "kis-domestic-terminal-quote-join/v1"
        and quote.get("sessionId") == session_id
        and quote.get("evaluationHash") == hashes["evaluation"]
        and quote.get("triggerHash") == hashes["trigger"]
        and quote.get("rollingReceiptHash") == hashes["rollingPreflightReceipt"]
        and type(quote.get("consumed")) is bool
        and quote.get("consumed") is True
        and type(quote.get("orderAuthorityFresh")) is bool
        and quote.get("orderAuthorityFresh") is False
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-quote-adapter-lineage-invalid"
        )
    _sha(quote.get("quoteReceiptHash"), "adapter-quote-receipt-hash")

    graph = _adapter_record(raw_records, "graph", "GRAPH_ACTIVATION")
    if set(graph) != {
        "recordType", "schemaVersion", "sessionId", "laneSessionHash",
        "rollingReceiptHash", "heartbeatResultHash", "activationCommitted",
        "productionGraphWired",
    } or not (
        graph.get("recordType") == "GRAPH_ACTIVATION"
        and graph.get("schemaVersion") == "kis-domestic-terminal-graph-join/v1"
        and graph.get("sessionId") == session_id
        and graph.get("laneSessionHash") == hashes["laneSession"]
        and graph.get("rollingReceiptHash") == hashes["rollingPreflightReceipt"]
        and graph.get("heartbeatResultHash") == hashes["heartbeatResult"]
        and type(graph.get("activationCommitted")) is bool
        and graph.get("activationCommitted") is True
        and type(graph.get("productionGraphWired")) is bool
        and graph.get("productionGraphWired") is False
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-graph-adapter-lineage-invalid"
        )

    mutation = _adapter_record(raw_records, "mutation", "MUTATION_INTEGRITY")
    mutation_actions = _adapter_record(
        raw_records, "mutation", "MUTATION_ACTION", many=True
    )
    expected_action_records = {
        str(item.get("claimId")): {
            "recordHash": str(item.get("_recordHash")),
            "rawMutationHash": str(item.get("rawMutationHash")),
            "officialFillHash": str(item.get("officialFillHash")),
            "brokerOrderId": str(item.get("brokerOrderId")),
        }
        for item in actions
    }
    if set(mutation) != {
        "recordType", "schemaVersion", "sessionId", "mutationRecordHash",
        "actionRecordHashes", "officialFillHashes", "integrityPassed",
    } or not (
        raw_mutation
        and mutation.get("recordType") == "MUTATION_INTEGRITY"
        and mutation.get("schemaVersion") == "kis-domestic-terminal-mutation-integrity/v1"
        and mutation.get("sessionId") == session_id
        and mutation.get("mutationRecordHash") == hashes.get("mutationRecord")
        and mutation.get("actionRecordHashes")
        == sorted(item["recordHash"] for item in expected_action_records.values())
        and mutation.get("officialFillHashes")
        == sorted(item["officialFillHash"] for item in expected_action_records.values())
        and type(mutation.get("integrityPassed")) is bool
        and mutation.get("integrityPassed") is True
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-mutation-integrity-adapter-invalid"
        )
    observed_mutations: dict[str, dict[str, str]] = {}
    for evidence in mutation_actions:
        if set(evidence) != {
            "recordType", "schemaVersion", "claimId", "actionRecordHash",
            "rawMutationHash", "officialFillHash", "brokerOrderId",
        } or evidence.get("recordType") != "MUTATION_ACTION" or evidence.get(
            "schemaVersion"
        ) != "kis-domestic-terminal-mutation-action/v1":
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-mutation-action-adapter-schema-invalid"
            )
        claim = _identifier(evidence.get("claimId"), "adapter-mutation-claim")
        if claim in observed_mutations:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-mutation-action-adapter-duplicate"
            )
        observed_mutations[claim] = {
            "recordHash": _sha(evidence.get("actionRecordHash"), "adapter-action-record"),
            "rawMutationHash": _sha(evidence.get("rawMutationHash"), "adapter-mutation-record"),
            "officialFillHash": _sha(evidence.get("officialFillHash"), "adapter-fill-record"),
            "brokerOrderId": _identifier(evidence.get("brokerOrderId"), "adapter-broker-order"),
        }
    if observed_mutations != expected_action_records:
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-mutation-action-adapter-bijection-invalid"
        )

    capability = _adapter_record(raw_records, "capability", "CAPABILITY_REVOKE")
    if set(capability) != {
        "recordType", "schemaVersion", "sessionId", "revokeProofHash",
        "externallyRevoked",
    } or not (
        capability_revoked
        and capability.get("recordType") == "CAPABILITY_REVOKE"
        and capability.get("schemaVersion") == "kis-domestic-terminal-capability-join/v1"
        and capability.get("sessionId") == session_id
        and capability.get("revokeProofHash") == hashes.get("capabilityRevokeProof")
        and type(capability.get("externallyRevoked")) is bool
        and capability.get("externallyRevoked") is True
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-capability-adapter-lineage-invalid"
        )


def _verify_archive_reader_result(result_value: Any) -> Mapping[str, Any]:
    result = _mapping(result_value, "terminal-archive-reader-result")
    required = {
        "schemaVersion", "route", "pdno", "canonicalRawRecords",
        "allRequiredRawRecordsJoined", "allExactJoinsPassed",
        "allImmutableArchivesFetched", "independentTruthArchiveFetched",
        "productionAuthorityPinned", "operatorExclusivityProven",
        "accountWideCausalClosureProven", "stateServerWired",
        "productionAvailable", "releaseAvailable", "networkOrderPostAllowed",
        "tradingMutationCount", "readinessBlockers",
    }
    if not required.issubset(result) or not (
        result.get("schemaVersion") == READER_OUTPUT_SCHEMA
        and result.get("route") == ROUTE
        and result.get("pdno") == PDNO
        and type(result.get("allRequiredRawRecordsJoined")) is bool
        and type(result.get("allExactJoinsPassed")) is bool
        and result.get("allRequiredRawRecordsJoined")
        == result.get("allExactJoinsPassed")
        and _exact_bool(result, "allImmutableArchivesFetched", True)
        and _exact_bool(result, "independentTruthArchiveFetched", True)
        and _exact_bool(result, "productionAuthorityPinned", False)
        and _exact_bool(result, "operatorExclusivityProven", False)
        and _exact_bool(result, "accountWideCausalClosureProven", False)
        and _exact_bool(result, "stateServerWired", False)
        and _exact_bool(result, "productionAvailable", False)
        and _exact_bool(result, "releaseAvailable", False)
        and _exact_bool(result, "networkOrderPostAllowed", False)
        and type(result.get("tradingMutationCount")) is int
        and result.get("tradingMutationCount") == 0
        and isinstance(result.get("canonicalRawRecords"), Mapping)
        and set(result["canonicalRawRecords"]) == _RAW_ADAPTER_COMPONENTS
        and isinstance(result.get("readinessBlockers"), list)
        and all(
            type(item) is str and item
            for item in result.get("readinessBlockers", [])
        )
        and result.get("readinessBlockers")
        == sorted(set(result.get("readinessBlockers", [])))
    ):
        raise KisDomesticFunctionalTerminalVerifierBlocked(
            "terminal-archive-reader-result-invalid"
        )
    return result


class KisDomesticFunctionalTerminalVerifier:
    """Read-only independent consumer. It has no transport or authority surface."""

    def __init__(
        self,
        *,
        record_verifier: Callable[[str, Mapping[str, Any], str], bool],
        trusted_wall_clock: Callable[[], datetime],
        archive_adapters: KisDomesticFunctionalTerminalArchiveAdapters | None = None,
    ) -> None:
        if not callable(record_verifier) or not callable(trusted_wall_clock):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-verifier-constructor-invalid"
            )
        self.verifier = record_verifier
        self.clock = trusted_wall_clock
        if (
            archive_adapters is not None
            and type(archive_adapters) is not KisDomesticFunctionalTerminalArchiveAdapters
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-archive-adapters-type-invalid"
            )
        self.archive_adapters = archive_adapters

    def _now(self) -> datetime:
        value = self.clock()
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-trusted-clock-invalid"
            )
        return _time(
            value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "terminal-trusted-now",
        )

    def verify(self, bundle_value: Mapping[str, Any]) -> dict[str, Any]:
        bundle = _mapping(bundle_value, "terminal-bundle")
        if set(bundle) != _BUNDLE_KEYS:
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-bundle-fields-not-exact"
            )
        if bundle.get("schemaVersion") != "kis-domestic-functional-terminal-input/v1":
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-bundle-schema-invalid"
            )
        records = {}
        hashes = {}
        specifications = (
            ("laneSession", "LANE_SESSION", _SESSION_KEYS),
            ("bootstrap", "LANE_BOOTSTRAP", _BOOTSTRAP_KEYS),
            ("approval", "LANE_APPROVAL", _APPROVAL_KEYS),
            ("evaluation", "LANE_EVALUATION", _EVALUATION_KEYS),
            ("trigger", "LANE_TRIGGER", _TRIGGER_KEYS),
            ("heartbeatResult", "HEARTBEAT_RESULT", _HEARTBEAT_KEYS),
            ("rollingPreflightReceipt", "ROLLING_PREFLIGHT_RECEIPT", _ROLLING_KEYS),
            ("sourceRawArchive", "SOURCE_RAW_ARCHIVE", _SOURCE_KEYS),
            ("baselineTruth", "BASELINE_TRUTH", _BASELINE_KEYS),
            ("terminalTruth", "TERMINAL_TRUTH", _TERMINAL_KEYS),
        )
        missing: list[str] = []
        for name, domain, keys in specifications:
            if bundle.get(name) is None:
                missing.append(name)
                continue
            body, digest = _verify_envelope(
                bundle[name],
                domain=domain,
                keys=keys,
                verifier=self.verifier,
                label=name,
            )
            records[name] = body
            hashes[name] = digest
        actions_value = bundle.get("actions")
        if not isinstance(actions_value, list):
            raise KisDomesticFunctionalTerminalVerifierBlocked(
                "terminal-actions-not-array"
            )
        actions: list[dict[str, Any]] = []
        for index, value in enumerate(actions_value):
            body, digest = _verify_envelope(
                value,
                domain="LANE_ACTION",
                keys=_ACTION_KEYS,
                verifier=self.verifier,
                label=f"action-{index}",
            )
            body["_recordHash"] = digest
            actions.append(body)

        blockers = [*missing, *_PRODUCTION_BLOCKERS]
        if missing:
            return self._result(
                session_id="",
                records=records,
                hashes=hashes,
                actions=actions,
                blockers=blockers,
                owner_loss=Decimal("0"),
                gross=Decimal("0"),
                raw_mutation=False,
                capability_revoked=False,
                owned_cleanup_complete=False,
                heartbeat_raw_joined=False,
                source_raw_joined=False,
                official_rest_raw_joined=False,
                immutable_archives_joined=False,
                action_truth_bijection_joined=False,
            )

        session = records["laneSession"]
        bootstrap = records["bootstrap"]
        approval = records["approval"]
        evaluation = records["evaluation"]
        trigger = records["trigger"]
        heartbeat = records["heartbeatResult"]
        rolling = records["rollingPreflightReceipt"]
        source = records["sourceRawArchive"]
        baseline = records["baselineTruth"]
        terminal = records["terminalTruth"]
        now = self._now()

        session_id = _identifier(session.get("sessionId"), "session-id")
        activated = _time(session.get("activatedAt"), "session-activated")
        expires = _time(session.get("expiresAt"), "session-expires")
        cleanup_started = _time(
            session.get("cleanupStartedAt"), "session-cleanup-started"
        )
        cleanup_ends = _time(session.get("cleanupEndsAt"), "session-cleanup-ends")
        finalized = _time(session.get("finalizedAt"), "session-finalized")
        if expires - activated != timedelta(seconds=ACTIVE_SECONDS):
            blockers.append("SESSION_NOT_EXACT_7200")
        if not (
            activated < expires
            <= cleanup_started
            <= finalized
            <= cleanup_ends
            and now >= finalized
        ):
            blockers.append("TRUSTED_NOW_OR_FINALIZATION_INVALID")
        lineage = {
            "route": ROUTE,
            "origin": LIVE_ORIGIN,
            "pdno": PDNO,
            "accountFingerprint": session.get("accountFingerprint"),
            "preactivationBaselineHash": session.get("preactivationBaselineHash"),
            "contractEnvelopeHash": session.get("contractEnvelopeHash"),
            "codeManifestHash": session.get("codeManifestHash"),
            "sessionId": session_id,
            "bootstrapId": session.get("bootstrapId"),
            "approvalId": session.get("approvalId"),
            "evaluationId": session.get("evaluationId"),
            "triggerId": session.get("triggerId"),
        }
        for key in (
            "accountFingerprint", "preactivationBaselineHash",
            "contractEnvelopeHash", "codeManifestHash", "permitHash",
        ):
            _sha(session.get(key), f"session-{key}")
        exact_checks = (
            session.get("schemaVersion") == "kis-domestic-lane-session/v1",
            session.get("route") == ROUTE,
            session.get("origin") == LIVE_ORIGIN,
            session.get("pdno") == PDNO,
            session.get("state") == "FINALIZED",
            bootstrap.get("schemaVersion") == "kis-domestic-lane-bootstrap/v1",
            bootstrap.get("route") == ROUTE,
            bootstrap.get("state") == "CONSUMED",
            bootstrap.get("bootstrapId") == lineage["bootstrapId"],
            bootstrap.get("approvalId") == lineage["approvalId"],
            bootstrap.get("evaluationId") == lineage["evaluationId"],
            bootstrap.get("triggerId") == lineage["triggerId"],
            bootstrap.get("sessionId") == session_id,
            bootstrap.get("preactivationBaselineHash") == lineage["preactivationBaselineHash"],
            approval.get("schemaVersion") == "kis-domestic-lane-approval/v1",
            approval.get("route") == ROUTE,
            approval.get("state") == "CONSUMED",
            approval.get("approvalId") == lineage["approvalId"],
            approval.get("bootstrapId") == lineage["bootstrapId"],
            approval.get("evaluationId") == lineage["evaluationId"],
            approval.get("triggerId") == lineage["triggerId"],
            approval.get("sessionId") == session_id,
            evaluation.get("schemaVersion") == "kis-domestic-lane-evaluation/v1",
            evaluation.get("route") == ROUTE,
            evaluation.get("pdno") == PDNO,
            evaluation.get("evaluationId") == lineage["evaluationId"],
            evaluation.get("publicArmId") == bootstrap.get("publicArmId"),
            evaluation.get("signal") == "BUY",
            evaluation.get("state") == "CONSUMED",
            evaluation.get("artifactContentHash") == APPROVED_ARTIFACT_CONTENT_HASH,
            evaluation.get("artifactFileSha256") == APPROVED_ARTIFACT_FILE_SHA256,
            evaluation.get("instanceContentHash") == APPROVED_INSTANCE_CONTENT_HASH,
            evaluation.get("instanceFileSha256") == APPROVED_INSTANCE_FILE_SHA256,
            evaluation.get("codeManifestHash") == lineage["codeManifestHash"],
            trigger.get("schemaVersion") == "kis-domestic-lane-trigger/v1",
            trigger.get("route") == ROUTE,
            trigger.get("pdno") == PDNO,
            trigger.get("triggerId") == lineage["triggerId"],
            trigger.get("evaluationId") == lineage["evaluationId"],
            trigger.get("state") == "CONSUMED",
        )
        if not all(exact_checks):
            blockers.append("LANE_AUTHORITY_LINEAGE_MISMATCH")

        for body in (heartbeat, rolling, source, baseline, terminal):
            if body.get("route") != ROUTE or body.get("pdno") != PDNO:
                blockers.append("CROSS_COMPONENT_ROUTE_OR_PDNO_MISMATCH")
                break
        if not (
            heartbeat.get("schemaVersion")
            == "kis-domestic-functional-heartbeat-evidence/v1"
            and heartbeat.get("sessionId") == session_id
            and heartbeat.get("activatedAt") == session.get("activatedAt")
            and heartbeat.get("activeEndsAt") == session.get("expiresAt")
            and _exact_bool(heartbeat, "uninterrupted", True)
            and _exact_bool(heartbeat, "exact7200ObservationPassed", True)
            and _decimal(heartbeat.get("actualMonotonicElapsedSeconds"), "heartbeat-elapsed")
            >= Decimal(ACTIVE_SECONDS)
            and _exact_bool(heartbeat, "functionalTestPassed", False)
            and _exact_bool(heartbeat, "promotionEligible", False)
            and _exact_bool(heartbeat, "releaseAvailable", False)
        ):
            blockers.append("HEARTBEAT_JOIN_INCOMPLETE")
        if not (
            rolling.get("schemaVersion")
            == "kis-domestic-rolling-preflight-consumption/v1"
            and rolling.get("sessionId") == session_id
            and rolling.get("evaluationId") == lineage["evaluationId"]
            and rolling.get("triggerId") == lineage["triggerId"]
            and rolling.get("accountFingerprint") == lineage["accountFingerprint"]
            and rolling.get("preactivationBaselineHash")
            == lineage["preactivationBaselineHash"]
            and rolling.get("contractEnvelopeHash") == lineage["contractEnvelopeHash"]
            and rolling.get("codeManifestHash") == lineage["codeManifestHash"]
            and rolling.get("publicArmId") == bootstrap.get("publicArmId")
            and rolling.get("evaluationHash") == hashes["evaluation"]
            and rolling.get("triggerHash") == hashes["trigger"]
            and rolling.get("sourceGeneration") == trigger.get("sourceGeneration")
            and rolling.get("barOpenAt") == trigger.get("barOpenAt")
            and _exact_bool(rolling, "singleUseConsumed", True)
            and _exact_bool(rolling, "privateAccountAuthorityAvailable", False)
            and _exact_bool(rolling, "tokenAuthorityAvailable", False)
            and _exact_bool(rolling, "orderAuthorityAvailable", False)
            and _exact_bool(rolling, "networkOrderPostAllowed", False)
            and rolling.get("tradingMutationCount") == 0
            and _exact_bool(rolling, "finalQuoteAvailable", False)
        ):
            blockers.append("ROLLING_PREFLIGHT_JOIN_INCOMPLETE")

        raw_frames = source.get("rawFrameHashes")
        expected_source_archive_hash = _hash(
            {
                "sourceGeneration": source.get("sourceGeneration"),
                "rawWindowHash": source.get("rawWindowHash"),
                "rawTriggerHash": source.get("rawTriggerHash"),
                "rawFrameHashes": raw_frames,
                "firstSourceSequence": source.get("firstSourceSequence"),
                "lastSourceSequence": source.get("lastSourceSequence"),
            }
        )
        if (
            source.get("schemaVersion") != "kis-domestic-source-raw-archive/v1"
            or source.get("publicArmId") != bootstrap.get("publicArmId")
            or source.get("evaluationId") != lineage["evaluationId"]
            or source.get("triggerId") != lineage["triggerId"]
            or source.get("sourceGeneration") != trigger.get("sourceGeneration")
            or source.get("rawWindowHash") != evaluation.get("rawWindowHash")
            or source.get("rawTriggerHash") != trigger.get("rawTriggerHash")
            or evaluation.get("sourceArchiveHash") != hashes["sourceRawArchive"]
            or source.get("sourceArchiveHash") != expected_source_archive_hash
            or not isinstance(raw_frames, list)
            or len(raw_frames) != source.get("rawFrameCount")
            or any(type(item) is not str or not _SHA256.fullmatch(item) for item in raw_frames)
            or not _exact_bool(source, "sequenceGapDetected", False)
            or not _exact_bool(source, "duplicateDetected", False)
            or not _exact_bool(source, "upstreamExchangeSequenceAvailable", False)
            or not _exact_bool(source, "archiveRecomputedFromAuthenticatedFrames", True)
        ):
            blockers.append("SOURCE_RAW_ARCHIVE_JOIN_INCOMPLETE")

        baseline_qty = _nonnegative_decimal(
            baseline.get("targetQuantity"), "baseline-quantity"
        )
        baseline_orderable = _nonnegative_decimal(
            baseline.get("targetOrderableQuantity"), "baseline-orderable"
        )
        baseline_cash = _nonnegative_decimal(
            baseline.get("cashKrw"), "baseline-cash"
        )
        final_qty = _nonnegative_decimal(
            terminal.get("targetQuantity"), "terminal-quantity"
        )
        final_orderable = _nonnegative_decimal(
            terminal.get("targetOrderableQuantity"), "terminal-orderable"
        )
        final_cash = _nonnegative_decimal(terminal.get("cashKrw"), "terminal-cash")
        terminal_observed = _time(terminal.get("observedAt"), "terminal-observed")
        if not (
            baseline.get("schemaVersion") == "kis-domestic-baseline-truth/v1"
            and baseline.get("accountFingerprint") == lineage["accountFingerprint"]
            and baseline.get("baselineHash") == lineage["preactivationBaselineHash"]
            and baseline.get("captureBundleHash") == rolling.get("captureBundleHash")
            and _exact_bool(baseline, "accountWideWorkingOrdersZero", True)
            and _exact_bool(baseline, "stableRepeatedReads", True)
            and _exact_bool(baseline, "costBaselineComplete", True)
            and terminal.get("schemaVersion") == "kis-domestic-terminal-truth/v1"
            and terminal.get("accountFingerprint") == lineage["accountFingerprint"]
            and terminal.get("baselineHash") == lineage["preactivationBaselineHash"]
            and _exact_bool(terminal, "accountWideWorkingOrdersZero", True)
            and _exact_bool(terminal, "ownedWorkingOrdersZero", True)
            and _exact_bool(terminal, "stableRepeatedReads", True)
            and _exact_bool(terminal, "allPagesComplete", True)
            and finalized <= terminal_observed <= now
        ):
            blockers.append("OFFICIAL_ACCOUNT_TRUTH_INCOMPLETE")

        kinds = [action.get("actionKind") for action in actions]
        exact_buy_cleanup = kinds == ["NATURAL_BUY", "CLEANUP_SELL"]
        if not exact_buy_cleanup:
            blockers.append("NATURAL_BUY_AND_EXACT_CLEANUP_SELL_REQUIRED")
        gross = Decimal("0")
        buy_qty = Decimal("0")
        sell_qty = Decimal("0")
        buy_cost = Decimal("0")
        sell_proceeds = Decimal("0")
        fills_value = terminal.get("officialFills")
        fills = fills_value if isinstance(fills_value, list) else []
        fill_by_claim: dict[str, Mapping[str, Any]] = {}
        for fill in fills:
            item = _mapping(fill, "official-fill")
            if set(item) != _FILL_KEYS:
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "official-fill-fields-not-exact"
                )
            claim_id = _identifier(item.get("claimId"), "fill-claim-id")
            _identifier(item.get("brokerOrderId"), "fill-broker-order-id")
            _sha(item.get("officialFillHash"), "fill-official-fill-hash")
            if claim_id in fill_by_claim:
                blockers.append("DUPLICATE_OFFICIAL_FILL")
            fill_by_claim[claim_id] = item
        for action in actions:
            claim_id = _identifier(action.get("claimId"), "action-claim-id")
            quantity = _positive_decimal(action.get("quantity"), "action-quantity")
            limit_price = _positive_decimal(
                action.get("limitPriceKrw"), "action-limit"
            )
            action_gross = _positive_decimal(
                action.get("grossKrw"), "action-gross"
            )
            action_fill_price = _positive_decimal(
                action.get("fillPriceKrw"), "action-fill-price"
            )
            action_fee = _nonnegative_decimal(action.get("feeKrw"), "action-fee")
            action_tax = _nonnegative_decimal(action.get("taxKrw"), "action-tax")
            action_interest = _nonnegative_decimal(
                action.get("loanInterestKrw"), "action-interest"
            )
            action_broker_id = _identifier(
                action.get("brokerOrderId"), "action-broker-order-id"
            )
            _sha(action.get("rawMutationHash"), "action-raw-mutation-hash")
            _sha(action.get("officialFillHash"), "action-official-fill-hash")
            _sha(action.get("transitionHeadHash"), "action-transition-head-hash")
            derived_action_gross = limit_price * quantity
            created_at = _time(action.get("createdAt"), "action-created")
            post_at = _time(action.get("postBoundaryAt"), "action-post-boundary")
            filled_at = _time(action.get("filledAt"), "action-filled")
            if (
                action.get("schemaVersion") != "kis-domestic-lane-action/v1"
                or action.get("route") != ROUTE
                or action.get("pdno") != PDNO
                or action.get("sessionId") != session_id
                or action.get("state") != "FILLED"
                or quantity != ORDER_QUANTITY
                or action_gross != derived_action_gross
                or action_gross > MAX_ORDER_KRW
            ):
                blockers.append("ACTION_CAP_OR_BINDING_INVALID")
            kind = action.get("actionKind")
            if kind == "NATURAL_BUY":
                if not (
                    activated <= created_at <= post_at <= filled_at < expires
                ):
                    blockers.append("NATURAL_BUY_CAUSAL_TIME_ORDER_INVALID")
                gross += action_gross
                if (
                    action.get("evaluationId") != lineage["evaluationId"]
                    or action.get("triggerId") != lineage["triggerId"]
                ):
                    blockers.append("NATURAL_BUY_PROVENANCE_INVALID")
                buy_qty += quantity
            elif kind == "CLEANUP_SELL":
                if not (
                    cleanup_started
                    <= created_at
                    <= post_at
                    <= filled_at
                    <= finalized
                    <= cleanup_ends
                ):
                    blockers.append("CLEANUP_SELL_CAUSAL_TIME_ORDER_INVALID")
                if action.get("evaluationId") or action.get("triggerId"):
                    blockers.append("CLEANUP_SELL_PROVENANCE_INVALID")
                sell_qty += quantity
            else:
                blockers.append("ACTION_KIND_INVALID")
            fill = fill_by_claim.get(claim_id)
            if fill is None:
                blockers.append("OFFICIAL_FILL_MISSING")
                continue
            fill_quantity = _positive_decimal(fill.get("quantity"), "fill-quantity")
            fill_price = _positive_decimal(fill.get("fillPriceKrw"), "fill-price")
            fee = _nonnegative_decimal(fill.get("feeKrw"), "fill-fee")
            tax = _nonnegative_decimal(fill.get("taxKrw"), "fill-tax")
            interest = _nonnegative_decimal(
                fill.get("loanInterestKrw"), "fill-interest"
            )
            official_filled_at = _time(fill.get("filledAt"), "fill-filled")
            official_fill_notional = fill_price * fill_quantity
            if (
                fill.get("brokerOrderId") != action_broker_id
                or fill.get("side") != ("BUY" if kind == "NATURAL_BUY" else "SELL")
                or fill_quantity != quantity
                or fill.get("officialFillHash") != action.get("officialFillHash")
                or fill_price != action_fill_price
                or fee != action_fee
                or tax != action_tax
                or interest != action_interest
                or official_filled_at != filled_at
                or official_fill_notional > MAX_ORDER_KRW
                or not _exact_bool(fill, "terminalFilled", True)
            ):
                blockers.append("OFFICIAL_FILL_ACTION_MISMATCH")
            if kind == "NATURAL_BUY":
                buy_cost += fill_price * quantity + fee + tax + interest
            elif kind == "CLEANUP_SELL":
                sell_proceeds += fill_price * quantity - fee - tax - interest
        if set(fill_by_claim) != {
            str(action.get("claimId")) for action in actions
        }:
            blockers.append("EXTRA_OR_MISSING_OFFICIAL_FILL")
        if gross > MAX_GROSS_KRW or sell_qty > buy_qty:
            blockers.append("GROSS_OR_OWNED_CLEANUP_BOUND_EXCEEDED")
        expected_qty = baseline_qty + buy_qty - sell_qty
        expected_orderable = baseline_orderable + buy_qty - sell_qty
        expected_cash = baseline_cash - buy_cost + sell_proceeds
        if final_qty != expected_qty or final_orderable != expected_orderable:
            blockers.append("BASELINE_QUANTITY_NOT_RECONCILED")
        if final_cash != expected_cash:
            blockers.append("BASELINE_CASH_OR_COST_NOT_RECONCILED")
        owner_loss = max(Decimal("0"), buy_cost - sell_proceeds)
        owned_cleanup_complete = (
            exact_buy_cleanup
            and buy_qty == ORDER_QUANTITY
            and sell_qty == ORDER_QUANTITY
            and final_qty == baseline_qty
            and final_orderable == baseline_orderable
            and _exact_bool(terminal, "ownedWorkingOrdersZero", True)
        )
        if not owned_cleanup_complete:
            blockers.append("OWNED_CLEANUP_OR_FINAL_DELTA_INCOMPLETE")
        if owner_loss >= OWNER_LOSS_LIMIT_KRW:
            blockers.append("OWNER_LOSS_LIMIT_REACHED")

        raw_mutation = False
        mutation_value = bundle.get("mutationRecord")
        if mutation_value is None:
            blockers.append("IMMUTABLE_RAW_MUTATION_RECORD_ABSENT")
        else:
            mutation, mutation_hash = _verify_envelope(
                mutation_value,
                domain="RAW_MUTATION_RECORD",
                keys=_MUTATION_KEYS,
                verifier=self.verifier,
                label="mutationRecord",
            )
            hashes["mutationRecord"] = mutation_hash
            mutation_records = mutation.get("records")
            if not isinstance(mutation_records, list):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "mutation-records-not-array"
                )
            previous = "0" * 64
            mutation_claims: set[str] = set()
            action_by_claim = {
                str(action.get("claimId")): action for action in actions
            }
            valid = (
                mutation.get("schemaVersion") == "kis-domestic-raw-mutation-record/v1"
                and mutation.get("route") == ROUTE
                and mutation.get("pdno") == PDNO
                and mutation.get("sessionId") == session_id
                and _exact_bool(mutation, "allPhysicalMutationAttemptsCounted", True)
                and _exact_bool(mutation, "postAmbiguityAbsent", True)
                and _exact_bool(mutation, "nonOwnedMutationObserved", False)
                and len(mutation_records) == len(actions)
            )
            for item_value in mutation_records:
                item = _mapping(item_value, "mutation-record")
                if set(item) != _MUTATION_RECORD_KEYS:
                    valid = False
                    continue
                unsigned = dict(item)
                claimed = unsigned.pop("recordHash", None)
                claim_id = item.get("claimId")
                action = action_by_claim.get(str(claim_id))
                request_hash = item.get("requestHash")
                response_hash = item.get("responseHash")
                endpoint_hash = item.get("endpointHash")
                endpoint = item.get("endpoint")
                valid = valid and (
                    action is not None
                    and claim_id not in mutation_claims
                    and item.get("previousHash") == previous
                    and type(claimed) is str
                    and _SHA256.fullmatch(claimed) is not None
                    and hmac.compare_digest(claimed, _hash(unsigned))
                    and type(request_hash) is str
                    and _SHA256.fullmatch(request_hash) is not None
                    and type(response_hash) is str
                    and _SHA256.fullmatch(response_hash) is not None
                    and item.get("method") == "POST"
                    and endpoint == "/uapi/domestic-stock/v1/trading/order-cash"
                    and endpoint_hash
                    == _hash({"method": "POST", "endpoint": endpoint})
                    and type(item.get("physicalAttemptCount")) is int
                    and item.get("physicalAttemptCount") == 1
                    and item.get("terminalState") == "FILLED"
                    and item.get("actionKind") == action.get("actionKind")
                    and item.get("brokerOrderId") == action.get("brokerOrderId")
                    and item.get("officialFillHash")
                    == action.get("officialFillHash")
                    and claimed == action.get("rawMutationHash")
                )
                previous = str(claimed)
                if type(claim_id) is str:
                    mutation_claims.add(claim_id)
            valid = valid and (
                mutation.get("recordHeadHash") == previous
                and mutation_claims
                == set(action_by_claim)
            )
            raw_mutation = bool(valid)
            if not raw_mutation:
                blockers.append("IMMUTABLE_RAW_MUTATION_RECORD_INVALID")

        capability_revoked = False
        capability_value = bundle.get("capabilityRevokeProof")
        if capability_value is not None:
            capability, capability_hash = _verify_envelope(
                capability_value,
                domain="CAPABILITY_REVOKE_PROOF",
                keys=_CAPABILITY_KEYS,
                verifier=self.verifier,
                label="capabilityRevokeProof",
            )
            hashes["capabilityRevokeProof"] = capability_hash
            unsigned = dict(capability)
            claimed = unsigned.pop("recordHash", None)
            capability_revoked = (
                capability.get("schemaVersion")
                == "kis-domestic-capability-revoke-proof/v1"
                and capability.get("route") == ROUTE
                and capability.get("sessionId") == session_id
                and type(claimed) is str
                and hmac.compare_digest(str(claimed), _hash(unsigned))
                and _exact_bool(capability, "runtimeReaderConfirmedClear", True)
                and _exact_bool(capability, "globalReaderConfirmedClear", True)
                and _exact_bool(capability, "functionalOrderAuthorityOpen", False)
            )
            if not capability_revoked:
                blockers.append("CAPABILITY_REVOKE_PROOF_INVALID")
        else:
            blockers.append("EXTERNAL_CAPABILITY_REVOKE_PROOF_ABSENT")

        heartbeat_raw_joined = False
        source_raw_joined = False
        official_rest_raw_joined = False
        immutable_archives_joined = False
        action_truth_bijection_joined = False
        if self.archive_adapters is None:
            blockers.extend(
                (
                    "FULL_HEARTBEAT_SAMPLE_JOURNAL_NOT_JOINED",
                    "FULL_AUTHENTICATED_SOURCE_ARCHIVE_NOT_JOINED",
                    "FULL_OFFICIAL_REST_RAW_TRUTH_NOT_JOINED",
                )
            )
        else:
            try:
                reader_value = self.archive_adapters.read(
                    session_id=session_id,
                    account_fingerprint=str(lineage["accountFingerprint"]),
                    preactivation_baseline_hash=str(
                        lineage["preactivationBaselineHash"]
                    ),
                )
            except KisDomesticFunctionalTerminalVerifierBlocked as exc:
                marker = "terminal-immutable-reader-rejected:reader-component-file-drift:"
                message = str(exc)
                if not message.startswith(marker):
                    raise
                component = message[len(marker):]
                if component not in _RAW_ADAPTER_COMPONENTS:
                    raise
                blockers.extend(
                    (
                        f"IMMUTABLE_VERIFY_ONLY_COMPONENT_FILE_DRIFT:{component}",
                        "FULL_HEARTBEAT_SAMPLE_JOURNAL_NOT_JOINED",
                        "FULL_AUTHENTICATED_SOURCE_ARCHIVE_NOT_JOINED",
                        "FULL_OFFICIAL_REST_RAW_TRUTH_NOT_JOINED",
                    )
                )
                reader_value = None
            if reader_value is None:
                return self._result(
                    session_id=session_id,
                    records=records,
                    hashes=hashes,
                    actions=actions,
                    blockers=blockers,
                    owner_loss=owner_loss,
                    gross=gross,
                    raw_mutation=raw_mutation,
                    capability_revoked=capability_revoked,
                    owned_cleanup_complete=owned_cleanup_complete,
                    heartbeat_raw_joined=False,
                    source_raw_joined=False,
                    official_rest_raw_joined=False,
                    immutable_archives_joined=False,
                    action_truth_bijection_joined=False,
                )
            reader_result = _verify_archive_reader_result(reader_value)
            if not (
                reader_result.get("sessionId") == session_id
                and reader_result.get("accountFingerprint")
                == lineage["accountFingerprint"]
                and reader_result.get("preactivationBaselineHash")
                == lineage["preactivationBaselineHash"]
            ):
                raise KisDomesticFunctionalTerminalVerifierBlocked(
                    "terminal-archive-reader-lineage-invalid"
                )
            if not reader_result["allExactJoinsPassed"]:
                reader_blockers = reader_result["readinessBlockers"]
                if not reader_blockers:
                    raise KisDomesticFunctionalTerminalVerifierBlocked(
                        "terminal-incomplete-reader-blockers-absent"
                    )
                blockers.extend(
                    f"IMMUTABLE_READER_SAFE_INCOMPLETE:{item}"
                    for item in reader_blockers
                )
                blockers.extend(
                    (
                        "FULL_HEARTBEAT_SAMPLE_JOURNAL_NOT_JOINED",
                        "FULL_AUTHENTICATED_SOURCE_ARCHIVE_NOT_JOINED",
                        "FULL_OFFICIAL_REST_RAW_TRUTH_NOT_JOINED",
                    )
                )
                return self._result(
                    session_id=session_id,
                    records=records,
                    hashes=hashes,
                    actions=actions,
                    blockers=blockers,
                    owner_loss=owner_loss,
                    gross=gross,
                    raw_mutation=raw_mutation,
                    capability_revoked=capability_revoked,
                    owned_cleanup_complete=owned_cleanup_complete,
                    heartbeat_raw_joined=False,
                    source_raw_joined=False,
                    official_rest_raw_joined=False,
                    immutable_archives_joined=False,
                    action_truth_bijection_joined=False,
                )
            raw_records = _mapping(
                reader_result.get("canonicalRawRecords"),
                "terminal-canonical-raw-records",
            )
            _verify_raw_heartbeat(
                _adapter_record(raw_records, "heartbeat", "HEARTBEAT_EVIDENCE"),
                heartbeat,
            )
            heartbeat_raw_joined = True
            _verify_raw_source(
                _adapter_record(raw_records, "source", "SOURCE_WINDOW_ARCHIVE"),
                source,
                evaluation,
                trigger,
            )
            source_raw_joined = True
            _verify_raw_truth(
                _adapter_record(raw_records, "truth", "PREACTIVATION_BASELINE"),
                _adapter_record(raw_records, "truth", "TERMINAL_TRUTH"),
                baseline,
                terminal,
                actions,
                owner_loss,
            )
            official_rest_raw_joined = True
            _verify_archive_lineage(
                raw_records,
                records=records,
                hashes=hashes,
                actions=actions,
                session_id=session_id,
                raw_mutation=raw_mutation,
                capability_revoked=capability_revoked,
            )
            immutable_archives_joined = True
            action_truth_bijection_joined = True

        return self._result(
            session_id=session_id,
            records=records,
            hashes=hashes,
            actions=actions,
            blockers=blockers,
            owner_loss=owner_loss,
            gross=gross,
            raw_mutation=raw_mutation,
            capability_revoked=capability_revoked,
            owned_cleanup_complete=owned_cleanup_complete,
            heartbeat_raw_joined=heartbeat_raw_joined,
            source_raw_joined=source_raw_joined,
            official_rest_raw_joined=official_rest_raw_joined,
            immutable_archives_joined=immutable_archives_joined,
            action_truth_bijection_joined=action_truth_bijection_joined,
        )

    def _result(
        self,
        *,
        session_id: str,
        records: Mapping[str, Mapping[str, Any]],
        hashes: Mapping[str, str],
        actions: Sequence[Mapping[str, Any]],
        blockers: Sequence[str],
        owner_loss: Decimal,
        gross: Decimal,
        raw_mutation: bool,
        capability_revoked: bool,
        owned_cleanup_complete: bool,
        heartbeat_raw_joined: bool,
        source_raw_joined: bool,
        official_rest_raw_joined: bool,
        immutable_archives_joined: bool,
        action_truth_bijection_joined: bool,
    ) -> dict[str, Any]:
        deduped = sorted(set(blockers))
        if not owned_cleanup_complete:
            terminal_outcome = OUTCOME_OWNED_CLEANUP_INCOMPLETE
        elif owner_loss >= OWNER_LOSS_LIMIT_KRW:
            terminal_outcome = OUTCOME_OWNER_LOSS_LIMIT_REACHED
        else:
            terminal_outcome = OUTCOME
        body = {
            "schemaVersion": "kis-domestic-functional-terminal-verification/v1",
            "protocolFingerprint": PROTOCOL_FINGERPRINT,
            "route": ROUTE,
            "pdno": PDNO,
            "sessionId": session_id,
            "terminalOutcome": terminal_outcome,
            "naturalSellSupported": False,
            "naturalSellObserved": False,
            "functionalWiringPassed": False,
            "functionalTestPassed": False,
            "promotionEligible": False,
            "releaseEvidenceEligible": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "rawMutationTruthAvailable": raw_mutation,
            "capabilityRevoked": capability_revoked,
            "ownedCleanupComplete": owned_cleanup_complete,
            "independentHeartbeatSampleJournalJoined": heartbeat_raw_joined,
            "independentAuthenticatedSourceArchiveJoined": source_raw_joined,
            "independentOfficialRestRawTruthJoined": official_rest_raw_joined,
            "immutableVerifyOnlyArchivesJoined": immutable_archives_joined,
            "rawActionMutationOfficialBijectionJoined": (
                action_truth_bijection_joined
            ),
            "rawBaselineOwnerLossRecomputed": official_rest_raw_joined,
            "offlineRawEvidenceVerificationPassed": bool(
                heartbeat_raw_joined
                and source_raw_joined
                and official_rest_raw_joined
                and immutable_archives_joined
                and action_truth_bijection_joined
                and raw_mutation
                and capability_revoked
                and owned_cleanup_complete
                and owner_loss < OWNER_LOSS_LIMIT_KRW
            ),
            "ownerLossKrw": format(owner_loss, "f"),
            "ownerLossTriggerReached": owner_loss >= OWNER_LOSS_LIMIT_KRW,
            "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
            "grossKrw": format(gross, "f"),
            "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
            "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
            "quantity": str(ORDER_QUANTITY),
            "actionKinds": [str(action.get("actionKind")) for action in actions],
            "joinedRecordHashes": {
                name: hashes[name] for name in sorted(hashes)
            },
            "verificationBlockers": deduped,
            "allRequiredImmutableRecordsPresent": bool(
                heartbeat_raw_joined
                and source_raw_joined
                and official_rest_raw_joined
                and immutable_archives_joined
            ),
            "reconciliationRequired": bool(deduped),
            "networkOrderPostAllowed": False,
            "tradingMutationCount": 0,
        }
        return {**body, "evidenceHash": _hash(body)}


def terminal_verifier_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-terminal-verifier-status/v1",
        "protocolFingerprint": PROTOCOL_FINGERPRINT,
        "route": ROUTE,
        "pdno": PDNO,
        "productionAvailable": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "releaseAvailable": False,
        "promotionAvailable": False,
        "naturalSellSupported": False,
        "terminalOutcome": OUTCOME,
        "rawMutationRecordRequired": True,
        "capabilityRevokeProofRequired": True,
        "immutableVerifyOnlyArchiveAdapterAvailable": True,
        "productionVerifyOnlyRegistryPinned": False,
        "productionGraphWired": False,
        "sharedKisRouteWired": False,
        "operatorExclusivityProven": False,
        "upstreamPacketCompletenessAvailable": False,
        "networkOrderPostAllowed": False,
    }


__all__ = [
    "KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_PROMOTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_TERMINAL_VERIFIER_RELEASE_AVAILABLE",
    "KisDomesticFunctionalTerminalVerifier",
    "KisDomesticFunctionalTerminalArchiveAdapters",
    "KisDomesticFunctionalTerminalVerifierBlocked",
    "OUTCOME",
    "OUTCOME_OWNER_LOSS_LIMIT_REACHED",
    "OUTCOME_OWNED_CLEANUP_INCOMPLETE",
    "PROTOCOL_FINGERPRINT",
    "terminal_verifier_component_status",
]
