from __future__ import annotations

"""Offline-only atomic archive for the two KIS public market ledgers.

The builder owns no socket, credential, account, token, or order surface.  It
accepts an externally supplied route/owner observation fence, holds that fence
while reading both producer databases, and publishes an immutable SQLite
archive with create-if-absent semantics.  Production remains disabled until a
reviewed shared route fence and external asymmetric key registry are wired.
"""

from contextlib import AbstractContextManager
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import base64
import binascii
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid
from typing import Any, Callable, Mapping

from . import kis_domestic_functional_market_source as market
from . import kis_domestic_functional_source as source
from .kis_domestic_functional_archive_builder import _fsync_directory, _fsync_file


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
ARCHIVE_SCHEMA_VERSION = "kis-domestic-functional-market-archive-sqlite/v1"
CAPTURE_SCHEMA = "kis-domestic-functional-market-archive-capture/v1"
POST_CAPTURE_SCHEMA = (
    "kis-domestic-functional-market-post-observation-archive-capture/v1"
)
FENCE_SCHEMA = "kis-domestic-functional-market-archive-fence/v1"
CAPTURE_SIGNATURE_DOMAIN = "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_CAPTURE"
POST_CAPTURE_SIGNATURE_DOMAIN = (
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_POST_OBSERVATION_ARCHIVE_CAPTURE"
)
CAPTURE_AUTHORITY_PURPOSE = "MARKET_ARCHIVE_CAPTURE_VERIFY"
KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_MUTATION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_RELEASE_AVAILABLE = False

_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,159}$", re.ASCII)
_ZERO = "0" * 64
_FENCE_KEYS = {
    "schemaVersion", "route", "sourceGeneration", "armId", "ownerEpochId",
    "ownerEpochHash", "routeFenceRevision", "observedAt", "routeLockHeld",
    "accountAuthorityAvailable", "mutationAuthorityAvailable",
    "productionAvailable", "fenceHash", "signature",
}
_CAPTURE_KEYS = {
    "schemaVersion", "route", "pdno", "sourceGeneration", "armId", "fence",
    "fenceObservedAt", "trustedNowBeforeRead", "trustedNowAfterRead",
    "marketDatabaseBundleHashBefore", "marketDatabaseBundleHashAfter",
    "sourceDatabaseBundleHashBefore", "sourceDatabaseBundleHashAfter",
    "marketSchemaVersion", "marketSchemaFingerprint", "sourceSchemaVersion",
    "sourceSchemaFingerprint", "logicalSnapshotHashBefore",
    "logicalSnapshotHashAfter", "replaySummary",
    "freshDedicatedProducerDatabasesRequired",
    "atomicRouteOwnerObservationFenceHeld", "createIfAbsentPublicationRequired",
    "archiveAuthorityKeyIdHash", "archiveAuthorityPurpose",
    "externalAsymmetricArchiveAuthorityPinned", "networkAvailable",
    "mutationAvailable", "productionAvailable", "releaseAvailable",
}
_POST_CAPTURE_KEYS = _CAPTURE_KEYS | {
    "preObservationArchiveFileHash", "preObservationCaptureHash",
    "preObservationLogicalSnapshotHash", "selectedObservationId",
    "selectedObservationHash", "selectedTriggerHash",
    "prefixExtensionSummary", "postObservationPrefixExtensionRequired",
}
_SOURCE_ARM_RECORD_KEYS = {
    "schemaVersion", "route", "pdno", "state", "armId", "source",
    "sourceProvider", "sourceGeneration", "socketIdentityHash", "connectedAt",
    "createdAt", "serverAuthorityKeyIdHash", "publicMarketDataOnly",
    "accountAuthorityAvailable", "tokenAuthorityAvailable",
    "mutationAuthorityAvailable", "networkAvailable", "productionAvailable",
    "marketSourceSessionId", "marketSourceAccountFingerprint",
    "marketSourceOwnerEpoch", "marketSourceOwnerEpochId",
    "marketSourceOwnerEpochHash", "marketSourceProcessGeneration",
    "marketSourceAuthorityKeyIdHash",
}
_SOURCE_FRAME_RECORD_KEYS = {
    "schemaVersion", "route", "pdno", "trId", "armId", "sourceGeneration",
    "socketIdentityHash", "frameIndex", "firstSourceSequence",
    "lastSourceSequence", "recordCount", "receivedAt", "rawFrame",
    "rawFrameHash", "recordFields", "previousFrameHeadHash",
    *source._MARKET_SOURCE_LINK_KEYS,
}
_OBSERVATION_RECORD_KEYS = {
    "schemaVersion", "observationId", "armId", "sourceGeneration",
    "socketIdentityHash", "captureHeadHash", "windowBody",
    "windowSignature", "rawArchive", "rawArchiveHash", "evaluationProof",
    "evaluationProofHash", "evaluationSignature", "averageRange",
    "triggerPrice", "naturalSignal", "boundary",
}
_WINDOW_KEYS = {
    "schemaVersion", "route", "origin", "pdno", "source", "interval",
    "artifactContentHash", "artifactFileSha256", "instanceContentHash",
    "instanceFileSha256", "sourceProvider", "sourceGeneration",
    "firstSourceSequence", "lastSourceSequence", "sourceEventCount",
    "sourceProofHash", "bars", "observedAt",
}
_BAR_KEYS = {
    "openAt", "closeAt", "open", "high", "low", "close",
    "sourceSequenceStart", "sourceSequenceEnd", "eventCount",
    "rawEventChainHash",
}
_EVALUATION_KEYS = {
    "schemaVersion", "route", "pdno", "armId", "sourceGeneration",
    "socketIdentityHash", "windowHash", "rawArchiveHash", "strategy",
    "priorBarCount", "averageRange", "breakoutMultiplier", "priorClose",
    "currentHigh", "triggerPrice", "naturalSignal", "barCloseAt",
    "nextOpenAt", "nextOpenObservedAt",
}
_BOUNDARY_KEYS = {
    "barOpenAt", "observedAt", "openPriceKrw", "sourceSequence",
    "rawEventHash",
}
_TRIGGER_KEYS = {
    "schemaVersion", "route", "pdno", "source", "sourceProvider",
    "sourceGeneration", "sourceSequence", "rawEventHash", "sourceProofHash",
    "eventType", "evaluationId", "barOpenAt", "observedAt", "openPriceKrw",
}
_RAW_ARCHIVE_KEYS = {
    "schemaVersion", "route", "pdno", "armId", "sourceGeneration",
    "socketIdentityHash", "firstSourceSequence", "lastSourceSequence",
    "sourceEventCount", "captureHeadHash", "authorityKeyIdHash",
    "upstreamExchangeSequenceAvailable",
    "upstreamPacketCompletenessAttested", "acceptedIngressContinuityOnly",
    "marketSourceIntegrationComplete", "marketSourceIngressLinkCount",
    "marketSourceIngressLinkHeadHash", "marketSourceIngressLinks", "frames",
    "events", "nextOpenEvent", "recomputedBars",
}
_MARKET_TABLES = (
    "kis_functional_market_source_manifest",
    "kis_functional_market_source_generation",
    "kis_functional_market_source_ingress",
    "kis_functional_market_source_transition",
)
_SOURCE_TABLES = (
    "kis_public_source_schema_meta",
    "kis_public_source_arm",
    "kis_public_source_frame",
    "kis_public_source_event",
    "kis_public_source_observation",
    "kis_public_source_arm_transition",
)

_ARCHIVE_SQL = """
CREATE TABLE kis_market_archive_manifest (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    schema_version TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL
);
CREATE TABLE kis_market_archive_capture (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    source_generation TEXT NOT NULL,
    arm_id TEXT NOT NULL,
    capture_json TEXT NOT NULL,
    capture_hash TEXT NOT NULL UNIQUE,
    capture_signature TEXT NOT NULL,
    archive_authority_key_id_hash TEXT NOT NULL
);
CREATE TABLE kis_market_archive_row (
    component TEXT NOT NULL CHECK(component IN ('MARKET_SOURCE','SOURCE')),
    table_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal>=1),
    row_json TEXT NOT NULL,
    row_hash TEXT NOT NULL UNIQUE,
    PRIMARY KEY(component,table_name,ordinal)
);
CREATE TRIGGER kis_market_archive_manifest_update_forbidden
BEFORE UPDATE ON kis_market_archive_manifest
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
CREATE TRIGGER kis_market_archive_manifest_delete_forbidden
BEFORE DELETE ON kis_market_archive_manifest
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
CREATE TRIGGER kis_market_archive_capture_update_forbidden
BEFORE UPDATE ON kis_market_archive_capture
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
CREATE TRIGGER kis_market_archive_capture_delete_forbidden
BEFORE DELETE ON kis_market_archive_capture
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
CREATE TRIGGER kis_market_archive_row_update_forbidden
BEFORE UPDATE ON kis_market_archive_row
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
CREATE TRIGGER kis_market_archive_row_delete_forbidden
BEFORE DELETE ON kis_market_archive_row
BEGIN SELECT RAISE(ABORT,'kis-market-archive-immutable'); END;
"""


class KisDomesticFunctionalMarketArchiveBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-archive-value-not-canonical"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _signature_text(value: Any) -> bool:
    """Accept only the two frozen offline signature encodings.

    Legacy mechanical fixtures use a lowercase SHA-256 HMAC hex digest.  The
    production-shaped verify-only registry uses canonical base64-encoded
    Ed25519 signatures (exactly 64 decoded bytes).  Cryptographic acceptance
    still belongs exclusively to the caller-supplied verifier; this helper
    only rejects malformed encodings before that verifier is invoked.
    """

    if type(value) is not str:
        return False
    if _SHA.fullmatch(value):
        return True
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return len(decoded) == 64 and base64.b64encode(decoded).decode("ascii") == value


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _bundle_hash(path: Path) -> str:
    rows: list[dict[str, Any]] = []
    for suffix in ("", "-wal"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            rows.append(
                {
                    "suffix": suffix or "MAIN",
                    "size": candidate.stat().st_size,
                    "sha256": _file_hash(candidate),
                }
            )
    if not rows:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-database-missing"
        )
    return _hash(rows)


def _utc(value: Any, label: str) -> datetime:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-not-aware")
    return parsed.astimezone(timezone.utc)


def _trusted_datetime(
    clock: Callable[[], datetime], label: str
) -> datetime:
    try:
        value = clock()
    except BaseException as exc:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            f"{label}-failed:{type(exc).__name__}"
        ) from None
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-invalid")
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decode(text: Any, label: str) -> dict[str, Any]:
    if type(text) is not str or not text:
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-missing")
    try:
        value = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-invalid") from None
    if not isinstance(value, dict) or _canonical(value) != text:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            f"{label}-not-canonical"
        )
    return value


def _normalize_sql(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _archive_schema_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    objects = {
        str(row[0]): _normalize_sql(row[1])
        for row in conn.execute(
            "SELECT name,sql FROM sqlite_master WHERE "
            "name LIKE 'kis_market_archive_%' ORDER BY name"
        )
    }
    tables: dict[str, Any] = {}
    for name, sql in objects.items():
        if not sql.startswith("CREATE TABLE"):
            continue
        indexes = [tuple(row) for row in conn.execute(f'PRAGMA index_list("{name}")')]
        tables[name] = {
            "tableInfo": [tuple(row) for row in conn.execute(f'PRAGMA table_info("{name}")')],
            "foreignKeys": [tuple(row) for row in conn.execute(f'PRAGMA foreign_key_list("{name}")')],
            "indexes": indexes,
            "indexXInfo": {
                str(row[1]): [
                    tuple(item)
                    for item in conn.execute(f'PRAGMA index_xinfo("{row[1]}")')
                ]
                for row in indexes
            },
        }
    return {"objects": objects, "tables": tables}


def _expected_archive_schema() -> dict[str, Any]:
    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(_ARCHIVE_SQL)
        return _archive_schema_snapshot(conn)
    finally:
        conn.close()


_EXPECTED_ARCHIVE_SCHEMA = _expected_archive_schema()
ARCHIVE_SCHEMA_FINGERPRINT = _hash(
    {"schemaVersion": ARCHIVE_SCHEMA_VERSION, "schema": _EXPECTED_ARCHIVE_SCHEMA}
)


def _verify_archive_schema(conn: sqlite3.Connection) -> None:
    if _archive_schema_snapshot(conn) != _EXPECTED_ARCHIVE_SCHEMA:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-archive-schema-dirty"
        )
    rows = [
        tuple(row) for row in conn.execute(
            "SELECT singleton,schema_version,schema_fingerprint "
            "FROM kis_market_archive_manifest"
        )
    ]
    if rows != [(1, ARCHIVE_SCHEMA_VERSION, ARCHIVE_SCHEMA_FINGERPRINT)]:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-archive-manifest-dirty"
        )


def _rows(conn: sqlite3.Connection, schema: str, table: str) -> list[dict[str, Any]]:
    prefix = "" if schema == "main" else f"{schema}."
    return [
        dict(row) for row in conn.execute(
            f'SELECT * FROM {prefix}"{table}" ORDER BY rowid'
        ).fetchall()
    ]


def _snapshot(
    conn: sqlite3.Connection,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {
        "MARKET_SOURCE": {
            table: _rows(conn, "main", table) for table in _MARKET_TABLES
        },
        "SOURCE": {
            table: _rows(conn, "source_db", table) for table in _SOURCE_TABLES
        },
    }


def _archive_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    snapshot: dict[str, dict[str, list[dict[str, Any]]]] = {
        "MARKET_SOURCE": {table: [] for table in _MARKET_TABLES},
        "SOURCE": {table: [] for table in _SOURCE_TABLES},
    }
    rows = conn.execute(
        "SELECT * FROM kis_market_archive_row ORDER BY "
        "component,table_name,ordinal"
    ).fetchall()
    for item in rows:
        component = str(item["component"])
        table = str(item["table_name"])
        body = _decode(item["row_json"], "archive-row")
        if _hash(body) != item["row_hash"]:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-row-hash-mismatch"
            )
        if component not in snapshot or table not in snapshot[component]:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-row-table-not-allowed"
            )
        target_rows = snapshot[component][table]
        if int(item["ordinal"]) != len(target_rows) + 1:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-row-ordinal-gap"
            )
        target_rows.append(body)
    return snapshot


def _load_archive_snapshot(
    path: str | Path, *, expected_file_hash: str
) -> dict[str, Any]:
    archive_path = Path(path).expanduser().resolve()
    if (
        type(expected_file_hash) is not str
        or not _SHA.fullmatch(expected_file_hash)
        or not archive_path.is_file()
        or not hmac.compare_digest(_file_hash(archive_path), expected_file_hash)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "prefix-archive-file-drift"
        )
    conn = sqlite3.connect(
        f"file:{archive_path.as_posix()}?mode=ro&immutable=1", uri=True
    )
    conn.row_factory = sqlite3.Row
    try:
        _verify_archive_schema(conn)
        return _archive_snapshot(conn)
    finally:
        conn.close()


def _prefix_rows(
    before: list[Mapping[str, Any]],
    after: list[Mapping[str, Any]],
    *,
    label: str,
    added: int,
) -> None:
    if len(after) != len(before) + added or [dict(row) for row in after[:len(before)]] != [
        dict(row) for row in before
    ]:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            f"post-prefix-{label}-rewrite-fork-or-cardinality-invalid"
        )


def _verify_prefix_extension(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    source_generation: str,
    arm_id: str,
) -> dict[str, Any]:
    if set(before) != {"MARKET_SOURCE", "SOURCE"} or set(after) != set(before):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-component-set-invalid"
        )
    for component, tables in (
        ("MARKET_SOURCE", _MARKET_TABLES), ("SOURCE", _SOURCE_TABLES)
    ):
        if set(before[component]) != set(tables) or set(after[component]) != set(tables):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "post-prefix-table-set-invalid"
            )
    market_before = before["MARKET_SOURCE"]
    market_after = after["MARKET_SOURCE"]
    source_before = before["SOURCE"]
    source_after = after["SOURCE"]
    if (
        market_before[_MARKET_TABLES[0]] != market_after[_MARKET_TABLES[0]]
        or source_before[_SOURCE_TABLES[0]] != source_after[_SOURCE_TABLES[0]]
        or len(market_before[_MARKET_TABLES[1]]) != 1
        or len(market_after[_MARKET_TABLES[1]]) != 1
        or len(source_before[_SOURCE_TABLES[1]]) != 1
        or len(source_after[_SOURCE_TABLES[1]]) != 1
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-identity-cardinality-invalid"
        )
    generation_before = dict(market_before[_MARKET_TABLES[1]][0])
    generation_after = dict(market_after[_MARKET_TABLES[1]][0])
    arm_before = dict(source_before[_SOURCE_TABLES[1]][0])
    arm_after = dict(source_after[_SOURCE_TABLES[1]][0])
    generation_identity = {
        "source_generation", "session_id", "account_fingerprint",
        "owner_epoch", "owner_epoch_id", "owner_epoch_hash",
        "process_generation", "socket_identity_hash", "authority_key_id_hash",
        "handshake_json", "handshake_hash", "handshake_signature",
        "connected_at", "reconnect_predecessor_generation",
    }
    arm_identity = {
        "arm_id", "route", "pdno", "source_generation",
        "socket_identity_hash", "owner_token_hash", "connected_at", "created_at",
        "arm_record_json", "arm_record_hash", "arm_signature",
        "authority_key_id_hash",
    }
    if (
        source_generation != generation_before.get("source_generation")
        or source_generation != generation_after.get("source_generation")
        or arm_id != arm_before.get("arm_id")
        or arm_id != arm_after.get("arm_id")
        or any(generation_before[key] != generation_after[key] for key in generation_identity)
        or any(arm_before[key] != arm_after[key] for key in arm_identity)
        or generation_before.get("state") != "ARMED_WAIT_PUBLIC"
        or generation_after.get("state") != "ARMED_WAIT_PUBLIC"
        or arm_before.get("state") != "ARMED_WAIT_PUBLIC"
        or arm_after.get("state") != "NEXT_OPEN_TRIGGER_SEALED"
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-owner-generation-arm-identity-mismatch"
        )
    _prefix_rows(
        market_before[_MARKET_TABLES[2]], market_after[_MARKET_TABLES[2]],
        label="market-ingress", added=1,
    )
    _prefix_rows(
        market_before[_MARKET_TABLES[3]], market_after[_MARKET_TABLES[3]],
        label="market-transition", added=4,
    )
    _prefix_rows(
        source_before[_SOURCE_TABLES[2]], source_after[_SOURCE_TABLES[2]],
        label="source-frame", added=1,
    )
    _prefix_rows(
        source_before[_SOURCE_TABLES[3]], source_after[_SOURCE_TABLES[3]],
        label="source-event", added=1,
    )
    _prefix_rows(
        source_before[_SOURCE_TABLES[5]], source_after[_SOURCE_TABLES[5]],
        label="source-transition", added=2,
    )
    if source_before[_SOURCE_TABLES[4]] or len(source_after[_SOURCE_TABLES[4]]) != 1:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-observation-cardinality-invalid"
        )
    new_ingress = market_after[_MARKET_TABLES[2]][-1]
    new_frame = source_after[_SOURCE_TABLES[2]][-1]
    new_event = source_after[_SOURCE_TABLES[3]][-1]
    new_transition = source_after[_SOURCE_TABLES[5]][-2]
    if (
        int(generation_before["last_ingress_ordinal"])
        != len(market_before[_MARKET_TABLES[2]])
        or new_ingress.get("previous_head_hash")
        != generation_before.get("ingress_head_hash")
        or int(arm_before["last_sequence"])
        != len(source_before[_SOURCE_TABLES[3]])
        or int(new_frame.get("first_sequence", 0))
        != int(arm_before["last_sequence"]) + 1
        or int(new_event.get("source_sequence", 0))
        != int(arm_before["last_sequence"]) + 1
        or new_transition.get("previous_hash")
        != arm_before.get("transition_head_hash")
        or generation_after.get("ingress_head_hash")
        != new_ingress.get("durable_head_hash")
        or arm_after.get("raw_head_hash") != new_frame.get("frame_head_hash")
        or arm_after.get("observation_id")
        != source_after[_SOURCE_TABLES[4]][0].get("observation_id")
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-head-high-water-continuity-invalid"
        )
    summary = {
        "schemaVersion": "kis-domestic-functional-market-prefix-extension/v1",
        "sourceGeneration": source_generation,
        "armId": arm_id,
        "preMarketIngressCount": len(market_before[_MARKET_TABLES[2]]),
        "postMarketIngressCount": len(market_after[_MARKET_TABLES[2]]),
        "preSourceFrameCount": len(source_before[_SOURCE_TABLES[2]]),
        "postSourceFrameCount": len(source_after[_SOURCE_TABLES[2]]),
        "preSourceEventCount": len(source_before[_SOURCE_TABLES[3]]),
        "postSourceEventCount": len(source_after[_SOURCE_TABLES[3]]),
        "preMarketIngressHeadHash": generation_before["ingress_head_hash"],
        "postMarketIngressHeadHash": generation_after["ingress_head_hash"],
        "preSourceFrameHeadHash": arm_before["raw_head_hash"],
        "postSourceFrameHeadHash": arm_after["raw_head_hash"],
        "preSourceTransitionHeadHash": arm_before["transition_head_hash"],
        "postSourceTransitionHeadHash": arm_after["transition_head_hash"],
        "immutablePredecessorRowsByteIdentical": True,
        "countsAndHeadsExtendContiguously": True,
        "sameArmGenerationSocketOwnerAndKeys": True,
        "observationAndTriggerExactJoined": True,
        "externalAsymmetricArchiveAuthorityPinned": False,
        "releaseCompletenessProven": False,
    }
    return {**summary, "prefixExtensionHash": _hash(summary)}


def _verify_signed_envelope(
    value: Mapping[str, Any],
    *,
    hash_key: str,
    verifier: Callable[[Mapping[str, Any]], bool],
    label: str,
) -> None:
    body = dict(value)
    signature = body.pop("signature", None)
    digest = body.pop(hash_key, None)
    if (
        not _signature_text(signature)
        or type(digest) is not str or not _SHA.fullmatch(digest)
        or not hmac.compare_digest(digest, _hash(body))
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-hash-invalid")
    try:
        valid = verifier(deepcopy(dict(value)))
    except BaseException:
        valid = False
    if valid is not True:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            f"{label}-signature-unverified"
        )


def _exact_row(
    row: Mapping[str, Any], expected: Mapping[str, Any], label: str
) -> None:
    if dict(row) != dict(expected):
        raise KisDomesticFunctionalMarketArchiveBlocked(f"{label}-row-projection-invalid")


def _verify_capture_signature(
    body: Mapping[str, Any],
    *,
    capture_hash: str,
    signature: str,
    authority_key_id_hash: str,
    verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    domain: str = CAPTURE_SIGNATURE_DOMAIN,
) -> None:
    if (
        type(capture_hash) is not str
        or not _SHA.fullmatch(capture_hash)
        or not hmac.compare_digest(capture_hash, _hash(body))
        or not _signature_text(signature)
        or type(authority_key_id_hash) is not str
        or not _SHA.fullmatch(authority_key_id_hash)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-capture-signature-projection-invalid"
        )
    signed = {**dict(body), "captureHash": capture_hash}
    try:
        valid = verifier(
            domain,
            deepcopy(signed),
            signature,
            authority_key_id_hash,
        )
    except BaseException:
        valid = False
    if valid is not True:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-capture-signature-unverified"
        )


def _source_signature_valid(
    verifier: Callable[[str, Mapping[str, Any], str], bool],
    domain: str,
    body: Mapping[str, Any],
    signature: Any,
) -> bool:
    if not _signature_text(signature):
        return False
    try:
        return verifier(domain, deepcopy(dict(body)), signature) is True
    except BaseException:
        return False


def _verify_post_observation_evidence(
    *,
    observation_row: Mapping[str, Any],
    arm: Mapping[str, Any],
    generation: Mapping[str, Any],
    source_generation: str,
    arm_id: str,
    source_frames: list[dict[str, Any]],
    source_events: list[dict[str, Any]],
    market_links: list[dict[str, Any]],
    market_head: str,
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    cutoff: datetime,
    selected_observation_id: str,
) -> dict[str, Any]:
    record = _decode(
        observation_row.get("observation_record_json"), "source-observation"
    )
    trigger = _decode(
        observation_row.get("trigger_record_json"), "source-trigger"
    )
    record_hash = source._hash(record)
    trigger_hash = source._hash(trigger)
    window = record.get("windowBody")
    raw_archive = record.get("rawArchive")
    evaluation = record.get("evaluationProof")
    boundary = record.get("boundary")
    if not all(
        isinstance(value, Mapping)
        for value in (window, raw_archive, evaluation, boundary)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-nested-evidence-invalid"
        )
    window = dict(window)
    raw_archive = dict(raw_archive)
    evaluation = dict(evaluation)
    boundary = dict(boundary)
    if (
        set(record) != _OBSERVATION_RECORD_KEYS
        or set(window) != _WINDOW_KEYS
        or set(raw_archive) != _RAW_ARCHIVE_KEYS
        or set(evaluation) != _EVALUATION_KEYS
        or set(boundary) != _BOUNDARY_KEYS
        or set(trigger) != _TRIGGER_KEYS
        or record.get("schemaVersion")
        != "kis-domestic-functional-source-observation-record/v1"
        or record.get("observationId") != selected_observation_id
        or record.get("armId") != arm_id
        or record.get("sourceGeneration") != source_generation
        or record.get("socketIdentityHash") != arm.get("socket_identity_hash")
        or record.get("captureHeadHash") != arm.get("raw_head_hash")
        or record.get("naturalSignal") != "BUY"
        or not _source_signature_valid(
            source_verifier,
            "SOURCE_OBSERVATION",
            record,
            observation_row.get("observation_signature"),
        )
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-record-binding-invalid"
        )
    if (
        window.get("schemaVersion")
        != "kis-domestic-official-5m-window/v1"
        or window.get("route") != ROUTE
        or window.get("origin") != source.LIVE_ORIGIN
        or window.get("pdno") != PDNO
        or window.get("source") != "KIS_WEBSOCKET_H0STCNT0"
        or window.get("sourceProvider") != "kis"
        or window.get("sourceGeneration") != source_generation
        or window.get("interval") != "5m"
        or window.get("artifactContentHash")
        != source.APPROVED_ARTIFACT_CONTENT_HASH
        or window.get("artifactFileSha256")
        != source.APPROVED_ARTIFACT_FILE_SHA256
        or window.get("instanceContentHash")
        != source.APPROVED_INSTANCE_CONTENT_HASH
        or window.get("instanceFileSha256")
        != source.APPROVED_INSTANCE_FILE_SHA256
        or not _source_signature_valid(
            source_verifier,
            "BAR_WINDOW",
            window,
            record.get("windowSignature"),
        )
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-window-binding-invalid"
        )
    try:
        first_sequence = int(window["firstSourceSequence"])
        last_sequence = int(window["lastSourceSequence"])
        next_sequence = int(boundary["sourceSequence"])
    except (KeyError, TypeError, ValueError):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-sequence-invalid"
        ) from None
    if (
        type(window.get("firstSourceSequence")) is not str
        or type(window.get("lastSourceSequence")) is not str
        or type(boundary.get("sourceSequence")) is not str
        or first_sequence < 1
        or last_sequence < first_sequence
        or next_sequence != last_sequence + 1
        or next_sequence != len(source_events)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-sequence-not-exact-prefix-boundary"
        )
    window_events = source_events[first_sequence - 1:last_sequence]
    next_open_event = source_events[next_sequence - 1]
    bars = source._independent_bars_from_events(window_events)
    if (
        len(bars) != 11
        or any(set(item) != _BAR_KEYS for item in bars)
        or window.get("bars") != bars
        or window.get("sourceEventCount") != len(window_events)
        or next_open_event.get("sourceSequence") != str(next_sequence)
        or next_open_event.get("bucketOpenAt") != bars[-1]["closeAt"]
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-window-event-replay-invalid"
        )
    source_proof = {
        "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "sourceProvider": "kis",
        "sourceGeneration": source_generation,
        "firstSourceSequence": str(first_sequence),
        "lastSourceSequence": str(last_sequence),
        "sourceEventCount": len(window_events),
        "barRawEventChainHashes": [
            item["rawEventChainHash"] for item in bars
        ],
    }
    if window.get("sourceProofHash") != source._hash(source_proof):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-source-proof-invalid"
        )
    expected_raw_archive = {
        "schemaVersion": "kis-domestic-h0stcnt0-durable-window-archive/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "armId": arm_id,
        "sourceGeneration": source_generation,
        "socketIdentityHash": arm["socket_identity_hash"],
        "firstSourceSequence": str(first_sequence),
        "lastSourceSequence": str(last_sequence),
        "sourceEventCount": len(window_events),
        "captureHeadHash": arm["raw_head_hash"],
        "authorityKeyIdHash": arm["authority_key_id_hash"],
        "upstreamExchangeSequenceAvailable": False,
        "upstreamPacketCompletenessAttested": False,
        "acceptedIngressContinuityOnly": True,
        "marketSourceIntegrationComplete": True,
        "marketSourceIngressLinkCount": len(market_links),
        "marketSourceIngressLinkHeadHash": market_head,
        "marketSourceIngressLinks": market_links,
        "frames": source_frames,
        "events": window_events,
        "nextOpenEvent": next_open_event,
        "recomputedBars": bars,
    }
    if (
        raw_archive != expected_raw_archive
        or record.get("rawArchiveHash") != source._hash(raw_archive)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-raw-archive-invalid"
        )
    prior = bars[:10]
    average_range = sum(
        (
            source._decimal(item["high"], "archive.high")
            - source._decimal(item["low"], "archive.low")
            for item in prior
        ),
        Decimal("0"),
    ) / Decimal("10")
    trigger_price = source._decimal(
        prior[-1]["close"], "archive.prior.close"
    ) + average_range * Decimal("0.3")
    evaluation_expected = {
        "schemaVersion": "kis-domestic-natural-breakout-evaluation-proof/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "armId": arm_id,
        "sourceGeneration": source_generation,
        "socketIdentityHash": arm["socket_identity_hash"],
        "windowHash": source._hash(window),
        "rawArchiveHash": source._hash(raw_archive),
        "strategy": "KIS_DOMESTIC_VOLATILITY_BREAKOUT_10X0.3",
        "priorBarCount": 10,
        "averageRange": source._decimal_text(average_range),
        "breakoutMultiplier": "0.3",
        "priorClose": prior[-1]["close"],
        "currentHigh": bars[-1]["high"],
        "triggerPrice": source._decimal_text(trigger_price),
        "naturalSignal": "BUY",
        "barCloseAt": bars[-1]["closeAt"],
        "nextOpenAt": next_open_event["bucketOpenAt"],
        "nextOpenObservedAt": next_open_event["receivedAt"],
    }
    if (
        evaluation != evaluation_expected
        or source._decimal(bars[-1]["high"], "archive.current.high")
        < trigger_price
        or record.get("evaluationProofHash") != source._hash(evaluation)
        or not _source_signature_valid(
            source_verifier,
            "NATURAL_BREAKOUT_EVALUATION",
            evaluation,
            record.get("evaluationSignature"),
        )
        or record.get("averageRange") != evaluation["averageRange"]
        or record.get("triggerPrice") != evaluation["triggerPrice"]
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-evaluation-replay-invalid"
        )
    expected_boundary = {
        "barOpenAt": next_open_event["bucketOpenAt"],
        "observedAt": next_open_event["receivedAt"],
        "openPriceKrw": next_open_event["recordFields"][2],
        "sourceSequence": str(next_sequence),
        "rawEventHash": next_open_event["rawEventHash"],
    }
    trigger_proof = {
        "schemaVersion": "kis-h0stcnt0-next-open-source-proof/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "sourceProvider": "kis",
        "sourceGeneration": source_generation,
        "sourceSequence": str(next_sequence),
        "rawEventHash": next_open_event["rawEventHash"],
        "barOpenAt": next_open_event["bucketOpenAt"],
        "observedAt": next_open_event["receivedAt"],
    }
    expected_trigger = {
        "schemaVersion": "kis-domestic-next-open-trigger/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "source": "KIS_WEBSOCKET",
        "sourceProvider": "kis",
        "sourceGeneration": source_generation,
        "sourceSequence": str(next_sequence),
        "rawEventHash": next_open_event["rawEventHash"],
        "sourceProofHash": source._hash(trigger_proof),
        "eventType": "NEXT_BAR_OPEN",
        "evaluationId": observation_row.get("evaluation_id"),
        "barOpenAt": next_open_event["bucketOpenAt"],
        "observedAt": next_open_event["receivedAt"],
        "openPriceKrw": next_open_event["recordFields"][2],
    }
    if (
        boundary != expected_boundary
        or trigger != expected_trigger
        or type(trigger.get("evaluationId")) is not str
        or not source._EVALUATION.fullmatch(trigger["evaluationId"])
        or not _source_signature_valid(
            source_verifier,
            "NEXT_OPEN",
            trigger,
            observation_row.get("trigger_signature"),
        )
        or _utc(window["observedAt"], "post-window-observed-at") > cutoff
        or _utc(boundary["observedAt"], "post-boundary-observed-at") > cutoff
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-observation-trigger-replay-invalid"
        )
    expected_row = {
        "observation_id": selected_observation_id,
        "arm_id": arm_id,
        "state": "NEXT_OPEN_TRIGGER_SEALED",
        "observation_record_json": _canonical(record),
        "observation_record_hash": record_hash,
        "observation_signature": observation_row.get("observation_signature"),
        "trigger_record_json": _canonical(trigger),
        "trigger_record_hash": trigger_hash,
        "trigger_signature": observation_row.get("trigger_signature"),
        "evaluation_id": trigger["evaluationId"],
        "created_at": boundary["observedAt"],
        "updated_at": trigger["observedAt"],
        "revision": 1,
    }
    _exact_row(observation_row, expected_row, "source-observation")
    return {
        "observationId": selected_observation_id,
        "observationHash": record_hash,
        "triggerHash": trigger_hash,
        "evaluationId": trigger["evaluationId"],
        "lastSourceSequence": next_sequence,
    }


def _replay(
    snapshot: Mapping[str, Any],
    *,
    source_generation: str,
    arm_id: str,
    market_verifiers: Mapping[str, Callable[[Mapping[str, Any]], bool]],
    transition_verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    trusted_cutoff: datetime,
    capture_phase: str = "PRE_OBSERVATION",
    selected_observation_id: str = "",
) -> dict[str, Any]:
    if (
        not isinstance(trusted_cutoff, datetime)
        or trusted_cutoff.tzinfo is None
        or capture_phase not in {"PRE_OBSERVATION", "POST_OBSERVATION_TRIGGER"}
        or (
            capture_phase == "POST_OBSERVATION_TRIGGER"
            and (
                type(selected_observation_id) is not str
                or not selected_observation_id.startswith(
                    "kis-source-observation-"
                )
            )
        )
        or (capture_phase == "PRE_OBSERVATION" and selected_observation_id)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-replay-trusted-cutoff-invalid"
        )
    cutoff = trusted_cutoff.astimezone(timezone.utc)
    if set(market_verifiers) != {"handshake", "raw", "ack", "reducer"}:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-verifier-set-not-exact"
        )
    market_rows = snapshot.get("MARKET_SOURCE")
    source_rows = snapshot.get("SOURCE")
    if not isinstance(market_rows, Mapping) or not isinstance(source_rows, Mapping):
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-components-missing")
    if (
        set(snapshot) != {"MARKET_SOURCE", "SOURCE"}
        or set(market_rows) != set(_MARKET_TABLES)
        or set(source_rows) != set(_SOURCE_TABLES)
        or any(
            not isinstance(rows, list)
            or any(not isinstance(row, Mapping) for row in rows)
            for rows in (*market_rows.values(), *source_rows.values())
        )
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-snapshot-table-set-not-exact"
        )
    generations = list(market_rows.get(_MARKET_TABLES[1], []))
    arms = list(source_rows.get(_SOURCE_TABLES[1], []))
    if list(market_rows.get(_MARKET_TABLES[0], [])) != [
        {
            "singleton": 1,
            "schema_version": market.SCHEMA_VERSION,
            "schema_fingerprint": market.SCHEMA_FINGERPRINT,
        }
    ] or list(source_rows.get(_SOURCE_TABLES[0], [])) != [
        {
            "singleton": 1,
            "schema_version": source.SOURCE_JOURNAL_SCHEMA_VERSION,
            "schema_fingerprint": source.SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
        }
    ]:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-schema-manifest-row-invalid"
        )
    if len(generations) != 1 or len(arms) != 1:
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-route-cardinality-not-exact"
        )
    generation = dict(generations[0]); arm = dict(arms[0])
    if (
        generation.get("source_generation") != source_generation
        or arm.get("arm_id") != arm_id
        or arm.get("source_generation") != source_generation
        or generation.get("socket_identity_hash") != arm.get("socket_identity_hash")
        or generation.get("state") != "ARMED_WAIT_PUBLIC"
        or arm.get("state")
        != (
            "ARMED_WAIT_PUBLIC"
            if capture_phase == "PRE_OBSERVATION"
            else "NEXT_OPEN_TRIGGER_SEALED"
        )
        or generation.get("terminal_at") != ""
        or generation.get("failure_reason") != ""
        or generation.get("reconnect_predecessor_generation") != ""
        or arm.get("terminal_reason") != ""
        or arm.get("terminal_at") != ""
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "producer-generation-arm-binding-invalid"
        )
    handshake = _decode(generation.get("handshake_json"), "market-handshake")
    _verify_signed_envelope(
        handshake, hash_key="handshakeHash",
        verifier=market_verifiers["handshake"], label="market-handshake",
    )
    handshake_connected = _utc(
        handshake.get("connectedAt"), "market-handshake-connected-at"
    )
    handshake_ack = _utc(
        handshake.get("subscriptionAckAt"), "market-handshake-ack-at"
    )
    handshake_semantics = {
        "schemaVersion": market.HANDSHAKE_SCHEMA,
        "route": ROUTE,
        "pdno": PDNO,
        "trId": market.TR_ID,
        "approvalOrigin": market.LIVE_ORIGIN,
        "approvalEndpoint": market.APPROVAL_ENDPOINT,
        "websocketUrl": market.LIVE_WEBSOCKET_URL,
        "subscriptionBodyHash": market._subscription_body_hash(),
        "ackRtCd": "0",
        "ackTrId": market.TR_ID,
        "ackTrKey": PDNO,
        "publicMarketDataOnly": True,
        "privateStreamConfigured": False,
        "accountAuthorityAvailable": False,
        "mutationAuthorityAvailable": False,
        "networkExecutorAvailable": False,
        "productionAvailable": False,
        "authorityPurpose": market.MARKET_SOURCE_AUTHORITY_PURPOSE,
    }
    if (
        set(handshake) != market._HANDSHAKE_KEYS
        or any(
            type(handshake.get(key)) is not type(wanted)
            or handshake.get(key) != wanted
            for key, wanted in handshake_semantics.items()
        )
        or handshake.get("sourceGeneration") != source_generation
        or handshake.get("handshakeHash") != generation.get("handshake_hash")
        or handshake.get("signature") != generation.get("handshake_signature")
        or handshake.get("authorityPurpose") != "MARKET_SOURCE_RECORD_VERIFY"
        or generation.get("session_id") != handshake.get("sessionId")
        or generation.get("account_fingerprint")
        != handshake.get("accountFingerprint")
        or generation.get("owner_epoch") != handshake.get("ownerEpoch")
        or generation.get("owner_epoch_id") != handshake.get("ownerEpochId")
        or generation.get("owner_epoch_hash") != handshake.get("ownerEpochHash")
        or generation.get("process_generation")
        != handshake.get("processGeneration")
        or generation.get("socket_identity_hash")
        != handshake.get("socketIdentityHash")
        or generation.get("authority_key_id_hash")
        != handshake.get("authorityKeyIdHash")
        or generation.get("connected_at") != handshake.get("connectedAt")
        or not handshake_connected <= handshake_ack <= cutoff
        or (handshake_ack - handshake_connected).total_seconds() > 2
        or type(handshake.get("ownerEpoch")) is not int
        or handshake["ownerEpoch"] < 1
        or type(handshake.get("sessionId")) is not str
        or not _ID.fullmatch(handshake["sessionId"])
        or type(handshake.get("ownerEpochId")) is not str
        or not _ID.fullmatch(handshake["ownerEpochId"])
        or type(handshake.get("processGeneration")) is not str
        or not market._PROCESS_GENERATION.fullmatch(
            handshake["processGeneration"]
        )
        or type(handshake.get("sourceGeneration")) is not str
        or not market._GENERATION.fullmatch(handshake["sourceGeneration"])
        or any(
            type(handshake.get(key)) is not str
            or not _SHA.fullmatch(handshake[key])
            for key in (
                "accountFingerprint", "ownerEpochHash", "socketIdentityHash",
                "appKeyIdHash", "approvalKeyHash", "authorityKeyIdHash",
            )
        )
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-handshake-row-projection-invalid"
        )
    arm_record = _decode(arm.get("arm_record_json"), "source-arm")
    try:
        source_arm_valid = source_verifier(
            "PUBLIC_ARM", deepcopy(arm_record), str(arm.get("arm_signature"))
        )
    except BaseException:
        source_arm_valid = False
    if (
        source_arm_valid is not True
        or set(arm_record) != _SOURCE_ARM_RECORD_KEYS
        or arm_record.get("schemaVersion")
        != "kis-domestic-functional-public-arm/v1"
        or arm_record.get("state") != "ARMED_WAIT_PUBLIC"
        or arm_record.get("source") != "KIS_WEBSOCKET_H0STCNT0"
        or arm_record.get("sourceProvider") != "kis"
        or arm_record.get("publicMarketDataOnly") is not True
        or any(
            arm_record.get(key) is not False
            for key in (
                "accountAuthorityAvailable", "tokenAuthorityAvailable",
                "mutationAuthorityAvailable", "networkAvailable",
                "productionAvailable",
            )
        )
        or source._hash(arm_record) != arm.get("arm_record_hash")
        or arm_record.get("marketSourceSessionId") != generation.get("session_id")
        or arm_record.get("marketSourceAccountFingerprint")
        != generation.get("account_fingerprint")
        or arm_record.get("marketSourceOwnerEpoch")
        != generation.get("owner_epoch")
        or arm_record.get("marketSourceOwnerEpochId")
        != generation.get("owner_epoch_id")
        or arm_record.get("marketSourceOwnerEpochHash")
        != generation.get("owner_epoch_hash")
        or arm_record.get("marketSourceProcessGeneration")
        != generation.get("process_generation")
        or arm_record.get("marketSourceAuthorityKeyIdHash")
        != generation.get("authority_key_id_hash")
        or arm_record.get("route") != ROUTE
        or arm_record.get("pdno") != PDNO
        or arm_record.get("armId") != arm_id
        or arm_record.get("sourceGeneration") != source_generation
        or arm_record.get("socketIdentityHash") != arm.get("socket_identity_hash")
        or arm_record.get("serverAuthorityKeyIdHash")
        != arm.get("authority_key_id_hash")
        or arm.get("route") != ROUTE
        or arm.get("pdno") != PDNO
        or arm.get("connected_at") != arm_record.get("connectedAt")
        or arm.get("created_at") != arm_record.get("createdAt")
        or arm.get("arm_signature") is None
        or type(arm.get("owner_token_hash")) is not str
        or not _SHA.fullmatch(str(arm.get("owner_token_hash")))
        or type(arm_record.get("marketSourceAccountFingerprint")) is not str
        or not _SHA.fullmatch(arm_record["marketSourceAccountFingerprint"])
        or type(arm_record.get("marketSourceOwnerEpoch")) is not int
        or arm_record["marketSourceOwnerEpoch"] < 1
        or type(arm_record.get("marketSourceOwnerEpochId")) is not str
        or not _ID.fullmatch(arm_record["marketSourceOwnerEpochId"])
        or type(arm_record.get("marketSourceProcessGeneration")) is not str
        or not market._PROCESS_GENERATION.fullmatch(
            arm_record["marketSourceProcessGeneration"]
        )
        or _utc(arm_record.get("connectedAt"), "source-arm-connected-at")
        > cutoff
        or _utc(arm_record.get("createdAt"), "source-arm-created-at")
        > cutoff
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "source-arm-market-binding-invalid"
        )

    market_transitions = list(market_rows.get(_MARKET_TABLES[3], []))
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in market_transitions:
        if row.get("source_generation") != source_generation:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-transition-generation-mismatch"
            )
        grouped.setdefault(int(row["ingress_ordinal"]), []).append(dict(row))
    expected_transition_total = int(generation["transition_count"])
    ingresses = [dict(row) for row in market_rows.get(_MARKET_TABLES[2], [])]
    if not ingresses or [int(row["ingress_ordinal"]) for row in ingresses] != list(
        range(1, len(ingresses) + 1)
    ) or int(generation["transition_count"]) != len(ingresses) + 1 or any(
        int(row["transition_count"]) != 3 for row in ingresses
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-ingress-ordinal-gap"
        )
    expected_transition_total += sum(int(row["transition_count"]) for row in ingresses)
    if expected_transition_total != len(market_transitions):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-transition-cardinality-mismatch"
        )
    projections = {0: generation, **{int(row["ingress_ordinal"]): row for row in ingresses}}
    for ordinal, projection in projections.items():
        rows = sorted(grouped.get(ordinal, []), key=lambda item: int(item["sequence"]))
        if len(rows) != int(projection["transition_count"]):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-transition-projection-count-mismatch"
            )
        previous = _ZERO
        state = "ARMED_WAIT_PUBLIC" if ordinal == 0 else "INTENT"
        previous_occurred: datetime | None = None
        for index, row in enumerate(rows, 1):
            body = _decode(row.get("record_json"), "market-transition")
            record_hash = market._hash(body)
            occurred = _utc(body.get("occurredAt"), "market-transition-occurred-at")
            if previous_occurred is not None and occurred < previous_occurred:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "market-transition-time-regressed"
                )
            if occurred > cutoff:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "market-transition-after-capture-cutoff"
                )
            exact_projection = {
                "source_generation": source_generation,
                "ingress_ordinal": ordinal,
                "sequence": index,
                "revision": index,
                "transition_kind": body.get("transitionKind"),
                "from_state": body.get("fromState"),
                "to_state": body.get("toState"),
                "occurred_at": body.get("occurredAt"),
                "reason": body.get("reason"),
                "anchor_hash": body.get("anchorHash"),
                "previous_hash": body.get("previousHash"),
                "record_json": _canonical(body),
                "record_hash": record_hash,
                "signature": row.get("signature"),
                "authority_key_id_hash": body.get("authorityKeyIdHash"),
            }
            if (
                set(body) != market._TRANSITION_KEYS
                or body.get("schemaVersion") != market.TRANSITION_SCHEMA
                or body.get("route") != ROUTE
                or body.get("pdno") != PDNO
                or body.get("sourceGeneration") != source_generation
                or body.get("sequence") != index
                or body.get("revision") != index
                or body.get("ingressOrdinal") != ordinal
                or body.get("fromState") != state
                or body.get("previousHash") != previous
                or body.get("authorityPurpose") != "MARKET_SOURCE_RECORD_VERIFY"
                or record_hash != row.get("record_hash")
                or row.get("authority_key_id_hash")
                != body.get("authorityKeyIdHash")
            ):
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "market-transition-replay-mismatch"
                )
            _exact_row(row, exact_projection, "market-transition")
            try:
                valid = transition_verifier(
                    "KIS_DOMESTIC_FUNCTIONAL_MARKET_SOURCE_TRANSITION",
                    {**body, "recordHash": record_hash},
                    str(row.get("signature")),
                    str(body.get("authorityKeyIdHash")),
                )
            except BaseException:
                valid = False
            if valid is not True:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "market-transition-signature-unverified"
                )
            if ordinal == 0:
                if index == 1:
                    semantic = (
                        body.get("transitionKind") == "GENERATION_CREATED"
                        and body.get("toState") == "ARMED_WAIT_PUBLIC"
                        and body.get("reason")
                        == "SIGNED_LIVE_PUBLIC_GENERATION_ACCEPTED"
                        and body.get("anchorHash") == handshake["handshakeHash"]
                        and body.get("occurredAt") == generation["connected_at"]
                    )
                else:
                    linked = ingresses[index - 2]
                    linked_ack = _decode(linked.get("ack_json"), "market-ack")
                    semantic = (
                        body.get("transitionKind") == "INGRESS_HEAD_ADVANCED"
                        and body.get("toState") == "ARMED_WAIT_PUBLIC"
                        and body.get("reason")
                        == "SOURCE_JOURNAL_ACK_ADVANCED_LOCAL_HEAD"
                        and body.get("anchorHash") == linked_ack.get("ackHash")
                        and body.get("occurredAt") == linked_ack.get("ackedAt")
                    )
            else:
                linked = projection
                linked_raw = _decode(linked.get("raw_record_json"), "market-raw")
                linked_ack = _decode(linked.get("ack_json"), "market-ack")
                linked_reducer = _decode(
                    linked.get("reducer_receipt_json"), "market-reducer"
                )
                semantic_rows = (
                    (
                        "INGRESS_INTENT_PERSISTED", "INTENT", "INTENT",
                        "RAW_FRAME_VERIFIED_AND_INTENT_DURABLE",
                        linked_raw.get("recordHash"), linked_raw.get("receivedAt"),
                    ),
                    (
                        "SOURCE_JOURNAL_ACK_VERIFIED", "INTENT", "ACKED",
                        "SIGNED_SOURCE_JOURNAL_ACK_BOUND",
                        linked_ack.get("ackHash"), linked_ack.get("ackedAt"),
                    ),
                    (
                        "REDUCER_RECEIPT_VERIFIED", "ACKED", "REDUCED",
                        "SIGNED_REDUCER_RECEIPT_BOUND",
                        linked_reducer.get("receiptHash"),
                        linked_reducer.get("reducedAt"),
                    ),
                )
                semantic = tuple(
                    body.get(key)
                    for key in (
                        "transitionKind", "fromState", "toState", "reason",
                        "anchorHash", "occurredAt",
                    )
                ) == semantic_rows[index - 1]
            if not semantic:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "market-transition-semantic-mismatch"
                )
            previous = record_hash
            state = str(body["toState"])
            previous_occurred = occurred
        if (
            previous != projection.get("transition_head_hash")
            or state != projection.get("state")
            or int(projection.get("revision")) != len(rows)
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-transition-terminal-projection-mismatch"
            )

    source_frames = [dict(row) for row in source_rows.get(_SOURCE_TABLES[2], [])]
    source_events = [dict(row) for row in source_rows.get(_SOURCE_TABLES[3], [])]
    if [int(row["frame_index"]) for row in source_frames] != list(
        range(1, len(source_frames) + 1)
    ) or [int(row["source_sequence"]) for row in source_events] != list(
        range(1, len(source_events) + 1)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "source-frame-or-event-cardinality-gap"
        )
    if len(source_frames) != len(ingresses):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-source-frame-bijection-mismatch"
        )
    event_by_sequence = {int(row["source_sequence"]): row for row in source_events}
    previous_frame_head = _ZERO
    expected_sequence = 1
    source_frame_bodies: dict[int, dict[str, Any]] = {}
    source_frame_archives: list[dict[str, Any]] = []
    source_event_bodies: list[dict[str, Any]] = []
    for frame_row in source_frames:
        frame_index = int(frame_row["frame_index"])
        frame = _decode(frame_row.get("frame_record_json"), "source-frame")
        frame_hash = source._hash(frame)
        try:
            frame_valid = source_verifier(
                "RAW_H0STCNT0_FRAME", deepcopy(frame),
                str(frame_row.get("frame_signature")),
            )
        except BaseException:
            frame_valid = False
        records = source._independent_parse_authenticated_frame(frame)
        last_sequence = expected_sequence + len(records) - 1
        received_at = _utc(frame.get("receivedAt"), "source-frame-received-at")
        if received_at < _utc(arm["connected_at"], "source-arm-connected-at"):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-frame-before-arm-connected"
            )
        if received_at > cutoff:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-frame-after-capture-cutoff"
            )
        expected_frame_head = source._hash(
            {
                "previousHash": previous_frame_head,
                "frameEnvelopeHash": frame_hash,
                "frameIndex": frame_index,
            }
        )
        if (
            frame_valid is not True
            or set(frame) != _SOURCE_FRAME_RECORD_KEYS
            or frame.get("schemaVersion")
            != "kis-domestic-h0stcnt0-raw-frame-envelope/v1"
            or frame.get("route") != ROUTE
            or frame.get("pdno") != PDNO
            or frame.get("trId") != "H0STCNT0"
            or frame.get("armId") != arm_id
            or frame.get("sourceGeneration") != source_generation
            or frame.get("socketIdentityHash") != arm.get("socket_identity_hash")
            or frame_hash != frame_row.get("frame_record_hash")
            or frame.get("frameIndex") != frame_index
            or frame.get("firstSourceSequence") != str(expected_sequence)
            or frame.get("lastSourceSequence") != str(last_sequence)
            or frame.get("previousFrameHeadHash") != previous_frame_head
            or expected_frame_head != frame_row.get("frame_head_hash")
            or frame.get("marketSourceIngressOrdinal") != frame_index
            or frame.get("marketSourceSessionId") != generation.get("session_id")
            or frame.get("marketSourceAccountFingerprint")
            != generation.get("account_fingerprint")
            or frame.get("marketSourceOwnerEpoch") != generation.get("owner_epoch")
            or frame.get("marketSourceOwnerEpochId")
            != generation.get("owner_epoch_id")
            or frame.get("marketSourceOwnerEpochHash")
            != generation.get("owner_epoch_hash")
            or frame.get("marketSourceProcessGeneration")
            != generation.get("process_generation")
            or frame.get("marketSourceAuthorityKeyIdHash")
            != generation.get("authority_key_id_hash")
            or frame.get("marketSourceAuthorityPurpose")
            != "MARKET_SOURCE_RECORD_VERIFY"
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-frame-replay-or-market-link-invalid"
            )
        _exact_row(
            frame_row,
            {
                "arm_id": arm_id,
                "frame_index": frame_index,
                "first_sequence": expected_sequence,
                "last_sequence": last_sequence,
                "received_at": frame["receivedAt"],
                "raw_frame_hash": frame["rawFrameHash"],
                "frame_record_json": _canonical(frame),
                "frame_record_hash": frame_hash,
                "frame_signature": frame_row.get("frame_signature"),
                "frame_head_hash": expected_frame_head,
            },
            "source-frame",
        )
        for index, record in enumerate(records):
            sequence = expected_sequence + index
            event_row = event_by_sequence.get(sequence)
            if event_row is None or int(event_row["frame_index"]) != frame_index:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "source-event-frame-join-mismatch"
                )
            received = source._parse_utc(frame["receivedAt"], "archive.frame.receivedAt")
            temporal = source._independent_record_truth(record, received_at=received)
            raw_event_hash = source._hash(
                source._raw_event_hash_body(
                    source_generation=source_generation,
                    socket_identity_hash=str(arm["socket_identity_hash"]),
                    source_sequence=str(sequence), record_index=index,
                    raw_frame_hash=frame["rawFrameHash"],
                    record_fields=list(record), received_at=frame["receivedAt"],
                )
            )
            expected_event = {
                "schemaVersion": "kis-domestic-h0stcnt0-raw-event-envelope/v1",
                "route": ROUTE, "pdno": PDNO, "armId": arm_id,
                "sourceGeneration": source_generation,
                "socketIdentityHash": str(arm["socket_identity_hash"]),
                "sourceSequence": str(sequence),
                "feedSourceSequence": ":".join((PDNO, record[33], record[1], str(index))),
                "frameEnvelopeHash": frame_hash,
                "rawFrameHash": frame["rawFrameHash"],
                "rawEventHash": raw_event_hash, "recordIndex": index,
                "tradeAt": temporal["tradeAt"], "receivedAt": temporal["receivedAt"],
                "bucketOpenAt": temporal["bucketOpenAt"],
                "bucketCloseAt": temporal["bucketCloseAt"],
                "recordFields": list(record),
            }
            stored_event = _decode(event_row.get("event_record_json"), "source-event")
            if (
                stored_event != expected_event
                or event_row.get("raw_event_hash") != raw_event_hash
                or event_row.get("event_record_hash") != source._hash(expected_event)
            ):
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "source-event-independent-replay-mismatch"
                )
            source_event_bodies.append(expected_event)
            _exact_row(
                event_row,
                {
                    "arm_id": arm_id,
                    "source_sequence": sequence,
                    "frame_index": frame_index,
                    "raw_event_hash": raw_event_hash,
                    "event_record_json": _canonical(expected_event),
                    "event_record_hash": source._hash(expected_event),
                },
                "source-event",
            )
        source_frame_bodies[frame_index] = frame
        source_frame_archives.append(
            {
                "body": frame,
                "envelopeHash": frame_hash,
                "serverSignature": str(frame_row.get("frame_signature")),
                "frameHeadHash": expected_frame_head,
            }
        )
        previous_frame_head = expected_frame_head
        expected_sequence = last_sequence + 1
    if (
        previous_frame_head != arm.get("raw_head_hash")
        or len(source_events) != int(arm.get("raw_event_count"))
        or len(source_frames) != int(arm.get("raw_frame_count"))
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "source-high-water-head-mismatch"
        )

    previous_market_head = _ZERO
    market_links: list[dict[str, Any]] = []
    for ingress in ingresses:
        ordinal = int(ingress["ingress_ordinal"])
        if ingress.get("state") != "REDUCED":
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-ingress-not-reduced"
            )
        raw = _decode(ingress.get("raw_record_json"), "market-raw")
        ack = _decode(ingress.get("ack_json"), "market-ack")
        reducer = _decode(ingress.get("reducer_receipt_json"), "market-reducer")
        _verify_signed_envelope(
            raw, hash_key="recordHash", verifier=market_verifiers["raw"],
            label="market-raw",
        )
        _verify_signed_envelope(
            ack, hash_key="ackHash", verifier=market_verifiers["ack"],
            label="market-ack",
        )
        _verify_signed_envelope(
            reducer, hash_key="receiptHash", verifier=market_verifiers["reducer"],
            label="market-reducer",
        )
        _, records = market.DisabledKisDomesticFunctionalMarketSource._frame(
            raw["rawFrame"]
        )
        parsed_text = _canonical(records)
        frame = source_frame_bodies[ordinal]
        frame_row = source_frames[ordinal - 1]
        received = _utc(raw.get("receivedAt"), "market-raw-received-at")
        acked = _utc(ack.get("ackedAt"), "market-ack-acked-at")
        reduced = _utc(reducer.get("reducedAt"), "market-reducer-reduced-at")
        if (
            set(raw) != market._RAW_KEYS
            or set(ack) != market._ACK_KEYS
            or set(reducer) != market._REDUCER_KEYS
            or raw.get("schemaVersion") != market.RAW_RECORD_SCHEMA
            or raw.get("route") != ROUTE
            or raw.get("pdno") != PDNO
            or raw.get("trId") != "H0STCNT0"
            or raw.get("sessionId") != generation.get("session_id")
            or raw.get("accountFingerprint")
            != generation.get("account_fingerprint")
            or raw.get("ownerEpoch") != generation.get("owner_epoch")
            or raw.get("ownerEpochId") != generation.get("owner_epoch_id")
            or raw.get("ownerEpochHash") != generation.get("owner_epoch_hash")
            or raw.get("processGeneration")
            != generation.get("process_generation")
            or raw.get("socketIdentityHash")
            != generation.get("socket_identity_hash")
            or raw.get("authorityKeyIdHash")
            != generation.get("authority_key_id_hash")
            or raw.get("authorityPurpose")
            != market.MARKET_SOURCE_AUTHORITY_PURPOSE
            or raw.get("recordCount") != len(records)
            or raw.get("rawFrameHash")
            != hashlib.sha256(raw["rawFrame"].encode("utf-8")).hexdigest()
            or raw.get("upstreamExchangeSequenceAvailable") is not False
            or raw.get("upstreamPacketCompletenessAttested") is not False
            or raw.get("productionAvailable") is not False
            or raw.get("sourceGeneration") != source_generation
            or raw.get("ingressOrdinal") != ordinal
            or raw.get("previousIngressHeadHash") != previous_market_head
            or raw.get("recordHash") != ingress.get("raw_record_hash")
            or raw.get("signature") != ingress.get("raw_record_signature")
            or raw.get("previousIngressHeadHash")
            != ingress.get("previous_head_hash")
            or parsed_text != ingress.get("parsed_records_json")
            or hashlib.sha256(parsed_text.encode()).hexdigest()
            != ingress.get("parsed_records_hash")
            or ack.get("schemaVersion") != market.ACK_SCHEMA
            or ack.get("route") != ROUTE
            or ack.get("pdno") != PDNO
            or ack.get("sessionId") != generation.get("session_id")
            or ack.get("ownerEpochHash") != generation.get("owner_epoch_hash")
            or ack.get("sourceGeneration") != source_generation
            or ack.get("ingressOrdinal") != ordinal
            or ack.get("rawFrameHash") != raw.get("rawFrameHash")
            or ack.get("previousIngressHeadHash") != previous_market_head
            or ack.get("rawRecordHash") != raw.get("recordHash")
            or ack.get("durableRecordHash") != frame_row.get("frame_record_hash")
            or ack.get("productionAvailable") is not False
            or ack.get("authorityPurpose") != market.SOURCE_ACK_AUTHORITY_PURPOSE
            or ack.get("authorityKeyIdHash")
            != ingress.get("ack_authority_key_id_hash")
            or ack.get("sourceArmId") != arm_id
            or ack.get("sourceFrameIndex") != ordinal
            or ack.get("firstSourceSequence") != frame_row.get("first_sequence")
            or ack.get("lastSourceSequence") != frame_row.get("last_sequence")
            or ack.get("sourceFrameEnvelopeHash") != frame_row.get("frame_record_hash")
            or ack.get("sourceFrameHeadHash") != frame_row.get("frame_head_hash")
            or ack.get("sourceArmTransitionHeadHash")
            != frame.get("marketSourceArmTransitionHeadHash")
            or frame.get("marketSourceRawRecordHash") != raw.get("recordHash")
            or frame.get("marketSourceRawFrameHash") != raw.get("rawFrameHash")
            or frame.get("marketSourcePreviousIngressHeadHash")
            != previous_market_head
            or reducer.get("schemaVersion") != market.REDUCER_SCHEMA
            or reducer.get("route") != ROUTE
            or reducer.get("pdno") != PDNO
            or reducer.get("sessionId") != generation.get("session_id")
            or reducer.get("sourceGeneration") != source_generation
            or reducer.get("ingressOrdinal") != ordinal
            or reducer.get("rawRecordHash") != raw.get("recordHash")
            or reducer.get("durableRecordHash") != ack.get("durableRecordHash")
            or reducer.get("durableHeadHash") != ack.get("durableHeadHash")
            or reducer.get("reducerState") != "ACCEPTED"
            or type(reducer.get("closedBarCount")) is not int
            or reducer.get("closedBarCount") < 0
            or type(reducer.get("nextOpenObserved")) is not bool
            or reducer.get("productionAvailable") is not False
            or reducer.get("authorityPurpose")
            != market.SOURCE_ACK_AUTHORITY_PURPOSE
            or reducer.get("authorityKeyIdHash") != ack.get("authorityKeyIdHash")
            or reducer.get("authorityKeyIdHash")
            != ingress.get("reducer_authority_key_id_hash")
            or not (
                _utc(generation["connected_at"], "market-connected-at")
                <= received <= acked <= reduced <= cutoff
            )
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-source-ack-frame-event-bijection-mismatch"
            )
        expected_market_head = _hash(
            {
                "schemaVersion": "kis-domestic-functional-market-source-head/v2",
                "sourceGeneration": source_generation,
                "ingressOrdinal": ordinal,
                "previousIngressHeadHash": previous_market_head,
                "rawRecordHash": raw["recordHash"],
                "sourceArmId": arm_id,
                "sourceFrameIndex": ordinal,
                "sourceFrameEnvelopeHash": frame_row["frame_record_hash"],
                "sourceFrameHeadHash": frame_row["frame_head_hash"],
                "sourceArmTransitionHeadHash": frame[
                    "marketSourceArmTransitionHeadHash"
                ],
            }
        )
        if expected_market_head != ack.get("durableHeadHash"):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "market-source-cross-ledger-head-mismatch"
            )
        market_links.append(
            {
                "sourceGeneration": source_generation,
                "ingressOrdinal": ordinal,
                "rawRecordHash": raw["recordHash"],
                "rawFrameHash": raw["rawFrameHash"],
                "sourceFrameIndex": ordinal,
                "sourceFrameEnvelopeHash": frame_row["frame_record_hash"],
                "sourceFrameHeadHash": frame_row["frame_head_hash"],
                "sourceArmTransitionHeadHash": frame[
                    "marketSourceArmTransitionHeadHash"
                ],
                "computedMarketIngressHeadHash": expected_market_head,
            }
        )
        _exact_row(
            ingress,
            {
                "source_generation": source_generation,
                "ingress_ordinal": ordinal,
                "state": "REDUCED",
                "raw_record_json": _canonical(raw),
                "raw_record_hash": raw["recordHash"],
                "raw_record_signature": raw["signature"],
                "parsed_records_json": parsed_text,
                "parsed_records_hash": hashlib.sha256(
                    parsed_text.encode("utf-8")
                ).hexdigest(),
                "previous_head_hash": previous_market_head,
                "durable_record_hash": ack["durableRecordHash"],
                "durable_head_hash": ack["durableHeadHash"],
                "ack_json": _canonical(ack),
                "ack_hash": ack["ackHash"],
                "ack_authority_key_id_hash": ack["authorityKeyIdHash"],
                "reducer_receipt_json": _canonical(reducer),
                "reducer_receipt_hash": reducer["receiptHash"],
                "reducer_authority_key_id_hash": reducer["authorityKeyIdHash"],
                "transition_count": 3,
                "transition_head_hash": ingress["transition_head_hash"],
                "revision": 3,
            },
            "market-ingress",
        )
        previous_market_head = expected_market_head
    if (
        previous_market_head != generation.get("ingress_head_hash")
        or len(ingresses) != int(generation.get("last_ingress_ordinal"))
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "market-high-water-head-mismatch"
        )
    generation_transition_head = market._hash(
        _decode(
            sorted(grouped[0], key=lambda item: int(item["sequence"]))[-1][
                "record_json"
            ],
            "market-generation-transition-head",
        )
    )
    _exact_row(
        generation,
        {
            "source_generation": source_generation,
            "session_id": handshake["sessionId"],
            "account_fingerprint": handshake["accountFingerprint"],
            "owner_epoch": handshake["ownerEpoch"],
            "owner_epoch_id": handshake["ownerEpochId"],
            "owner_epoch_hash": handshake["ownerEpochHash"],
            "process_generation": handshake["processGeneration"],
            "socket_identity_hash": handshake["socketIdentityHash"],
            "authority_key_id_hash": handshake["authorityKeyIdHash"],
            "state": "ARMED_WAIT_PUBLIC",
            "handshake_json": _canonical(handshake),
            "handshake_hash": handshake["handshakeHash"],
            "handshake_signature": handshake["signature"],
            "connected_at": handshake["connectedAt"],
            "terminal_at": "",
            "failure_reason": "",
            "last_ingress_ordinal": len(ingresses),
            "ingress_head_hash": previous_market_head,
            "reconnect_predecessor_generation": "",
            "transition_count": len(ingresses) + 1,
            "transition_head_hash": generation_transition_head,
            "revision": len(ingresses) + 1,
        },
        "market-generation",
    )

    source_transitions = [
        dict(row) for row in source_rows.get(_SOURCE_TABLES[5], [])
    ]
    observations = [
        dict(row) for row in source_rows.get(_SOURCE_TABLES[4], [])
    ]
    post_evidence: dict[str, Any] | None = None
    if capture_phase == "PRE_OBSERVATION":
        if observations:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "fresh-dedicated-source-observation-table-not-empty"
            )
        expected_source_transition_count = 1
    else:
        if len(observations) != 1:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "post-observation-cardinality-not-exact"
            )
        post_evidence = _verify_post_observation_evidence(
            observation_row=observations[0], arm=arm, generation=generation,
            source_generation=source_generation, arm_id=arm_id,
            source_frames=source_frame_archives,
            source_events=source_event_bodies, market_links=market_links,
            market_head=previous_market_head, source_verifier=source_verifier,
            cutoff=cutoff, selected_observation_id=selected_observation_id,
        )
        expected_source_transition_count = 3
    if (
        len(source_transitions) != int(arm.get("transition_count"))
        or len(source_transitions) != expected_source_transition_count
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "source-transition-cardinality-mismatch"
        )
    previous = _ZERO; previous_state = ""; previous_occurred = None
    for index, row in enumerate(source_transitions, 1):
        body = _decode(row.get("record_json"), "source-transition")
        record_hash = source._hash(body)
        try:
            valid = source_verifier(
                "PUBLIC_ARM_TRANSITION", {**body, "recordHash": record_hash},
                str(row.get("signature")),
            )
        except BaseException:
            valid = False
        occurred = _utc(body.get("occurredAt"), "source-transition-occurred-at")
        if index == 1:
            expected_transition = {
                "armRevision": 0,
                "transitionKind": "ARM_CREATED",
                "fromState": "",
                "toState": "ARMED_WAIT_PUBLIC",
                "reason": "PUBLIC_ARM_CREATED",
                "anchorHash": arm.get("arm_record_hash"),
                "occurredAt": arm.get("created_at"),
            }
        elif index == 2 and post_evidence is not None:
            expected_transition = {
                "armRevision": len(source_frames) + 1,
                "transitionKind": "NATURAL_BUY_OBSERVATION_SEALED",
                "fromState": "ARMED_WAIT_PUBLIC",
                "toState": "NATURAL_BUY_OBSERVED",
                "reason": "NATURAL_BUY_OBSERVATION_SEALED",
                "anchorHash": post_evidence["observationHash"],
                "occurredAt": observations[0]["created_at"],
            }
        elif index == 3 and post_evidence is not None:
            expected_transition = {
                "armRevision": len(source_frames) + 2,
                "transitionKind": "NEXT_OPEN_TRIGGER_SEALED",
                "fromState": "NATURAL_BUY_OBSERVED",
                "toState": "NEXT_OPEN_TRIGGER_SEALED",
                "reason": "NEXT_OPEN_TRIGGER_SEALED",
                "anchorHash": post_evidence["triggerHash"],
                "occurredAt": observations[0]["updated_at"],
            }
        else:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-transition-phase-cardinality-invalid"
            )
        if (
            valid is not True or body.get("sequence") != index
            or set(body) != source._TRANSITION_KEYS
            or body.get("schemaVersion")
            != "kis-domestic-functional-source-arm-transition/v1"
            or body.get("route") != ROUTE
            or body.get("pdno") != PDNO
            or body.get("armId") != arm_id
            or any(
                body.get(key) != value
                for key, value in expected_transition.items()
            )
            or body.get("authorityKeyIdHash")
            != arm.get("authority_key_id_hash")
            or body.get("previousHash") != previous
            or body.get("fromState") != previous_state
            or record_hash != row.get("record_hash")
            or row.get("authority_key_id_hash") != body.get("authorityKeyIdHash")
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-transition-replay-mismatch"
            )
        _exact_row(
            row,
            {
                "arm_id": arm_id,
                "sequence": index,
                "arm_revision": body["armRevision"],
                "transition_kind": body["transitionKind"],
                "from_state": body["fromState"],
                "to_state": body["toState"],
                "occurred_at": body["occurredAt"],
                "reason": body["reason"],
                "anchor_hash": body["anchorHash"],
                "previous_hash": body["previousHash"],
                "record_json": _canonical(body),
                "record_hash": record_hash,
                "signature": row.get("signature"),
                "authority_key_id_hash": body["authorityKeyIdHash"],
            },
            "source-transition",
        )
        if previous_occurred is not None and occurred < previous_occurred:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-transition-time-regressed"
            )
        if occurred > cutoff:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "source-transition-after-capture-cutoff"
            )
        previous = record_hash; previous_state = str(body["toState"])
        previous_occurred = occurred
    if previous != arm.get("transition_head_hash") or previous_state != arm.get("state"):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "source-transition-terminal-projection-mismatch"
        )
    _exact_row(
        arm,
        {
            "arm_id": arm_id,
            "route": ROUTE,
            "pdno": PDNO,
            "state": (
                "ARMED_WAIT_PUBLIC"
                if capture_phase == "PRE_OBSERVATION"
                else "NEXT_OPEN_TRIGGER_SEALED"
            ),
            "source_generation": source_generation,
            "socket_identity_hash": arm_record["socketIdentityHash"],
            "owner_token_hash": arm["owner_token_hash"],
            "connected_at": arm_record["connectedAt"],
            "created_at": arm_record["createdAt"],
            "arm_record_json": _canonical(arm_record),
            "arm_record_hash": source._hash(arm_record),
            "arm_signature": arm["arm_signature"],
            "authority_key_id_hash": arm_record["serverAuthorityKeyIdHash"],
            "last_sequence": len(source_events),
            "raw_event_count": len(source_events),
            "raw_frame_count": len(source_frames),
            "raw_head_hash": previous_frame_head,
            "observation_id": (
                "" if post_evidence is None else post_evidence["observationId"]
            ),
            "observation_hash": (
                "" if post_evidence is None else post_evidence["observationHash"]
            ),
            "terminal_reason": "",
            "terminal_at": "",
            "transition_count": expected_source_transition_count,
            "transition_head_hash": previous,
            "revision": len(source_frames) + (0 if post_evidence is None else 2),
        },
        "source-arm",
    )
    summary = {
        "sourceGeneration": source_generation,
        "armId": arm_id,
        "marketIngressCount": len(ingresses),
        "marketTransitionCount": len(market_transitions),
        "marketIngressHeadHash": previous_market_head,
        "sourceFrameCount": len(source_frames),
        "sourceEventCount": len(source_events),
        "sourceFrameHeadHash": previous_frame_head,
        "sourceTransitionCount": len(source_transitions),
        "sourceTransitionHeadHash": previous,
        "sourceObservationCount": len(observations),
        "allObservationRowsIndependentlyReplayed": True,
        "allProducerRowProjectionsExact": True,
        "freshDedicatedProducerDatabasesVerified": True,
        "allRawFramesReparsed46Fields": all(
            len(record) == 46
            for frame in source_frame_bodies.values()
            for record in source._independent_parse_authenticated_frame(frame)
        ),
        "allProducerSignaturesVerified": True,
        "allTransitionChainsVerified": True,
        "marketSourceBijectionVerified": True,
        "capturePhase": capture_phase,
        "postObservationEvidence": post_evidence,
    }
    return {**summary, "summaryHash": _hash(summary)}


def _verified_fence(
    value: Mapping[str, Any],
    *,
    source_generation: str,
    arm_id: str,
    verifier: Callable[[Mapping[str, Any]], bool],
    trusted_now: datetime,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _FENCE_KEYS:
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-fence-not-exact")
    fence = deepcopy(dict(value))
    expected = {
        "schemaVersion": FENCE_SCHEMA, "route": ROUTE,
        "sourceGeneration": source_generation, "armId": arm_id,
        "routeLockHeld": True, "accountAuthorityAvailable": False,
        "mutationAuthorityAvailable": False, "productionAvailable": False,
    }
    if any(
        type(fence.get(key)) is not type(wanted) or fence.get(key) != wanted
        for key, wanted in expected.items()
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-fence-binding-invalid"
        )
    if (
        type(fence.get("ownerEpochId")) is not str
        or not _ID.fullmatch(fence["ownerEpochId"])
        or type(fence.get("ownerEpochHash")) is not str
        or not _SHA.fullmatch(fence["ownerEpochHash"])
        or type(fence.get("routeFenceRevision")) is not int
        or fence["routeFenceRevision"] < 1
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-fence-owner-revision-invalid"
        )
    _verify_signed_envelope(
        fence, hash_key="fenceHash", verifier=verifier, label="archive-fence"
    )
    observed = _utc(fence["observedAt"], "archive-fence-observed-at")
    now = trusted_now.astimezone(timezone.utc)
    if observed > now or (now - observed).total_seconds() > 2:
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-fence-stale")
    return fence


def build_market_source_archive(
    *,
    market_database: str | Path,
    source_database: str | Path,
    destination: str | Path,
    source_generation: str,
    arm_id: str,
    observation_fence: Callable[[], AbstractContextManager[Mapping[str, Any]]],
    fence_verifier: Callable[[Mapping[str, Any]], bool],
    market_verifiers: Mapping[str, Callable[[Mapping[str, Any]], bool]],
    transition_verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    archive_capture_signer: Callable[[str, Mapping[str, Any]], str],
    archive_capture_verifier: Callable[
        [str, Mapping[str, Any], str, str], bool
    ],
    archive_authority_key_id_hash: str,
    trusted_clock: Callable[[], datetime],
) -> dict[str, Any]:
    market_path = Path(market_database).expanduser().resolve()
    source_path = Path(source_database).expanduser().resolve()
    target = Path(destination).expanduser().resolve()
    if market_path == source_path or target in {market_path, source_path}:
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-path-alias-invalid")
    if not market_path.is_file() or not source_path.is_file():
        raise KisDomesticFunctionalMarketArchiveBlocked("producer-database-missing")
    if not target.parent.is_dir():
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-destination-parent-missing"
        )
    if target.exists():
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-destination-exists"
        )
    if (
        not callable(observation_fence)
        or not callable(trusted_clock)
        or not callable(archive_capture_signer)
        or not callable(archive_capture_verifier)
        or type(archive_authority_key_id_hash) is not str
        or not _SHA.fullmatch(archive_authority_key_id_hash)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-fence-clock-required")
    with observation_fence() as raw_fence:
        before_read = _trusted_datetime(trusted_clock, "archive-trusted-clock")
        fence = _verified_fence(
            raw_fence, source_generation=source_generation, arm_id=arm_id,
            verifier=fence_verifier, trusted_now=before_read,
        )
        market_before = _bundle_hash(market_path)
        source_before = _bundle_hash(source_path)
        market_schema_conn = sqlite3.connect(str(market_path)); market_schema_conn.row_factory = sqlite3.Row
        source_schema_conn = sqlite3.connect(str(source_path)); source_schema_conn.row_factory = sqlite3.Row
        try:
            market._verify_schema(market_schema_conn)
            source._verify_exact_source_schema(source_schema_conn)
        except BaseException as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                f"producer-schema-invalid:{type(exc).__name__}"
            ) from None
        finally:
            market_schema_conn.close(); source_schema_conn.close()
        conn = sqlite3.connect(str(market_path)); conn.row_factory = sqlite3.Row
        try:
            conn.execute("ATTACH DATABASE ? AS source_db", (str(source_path),))
            conn.execute("BEGIN IMMEDIATE")
            first = _snapshot(conn)
            first_hash = _hash(first)
            second = _snapshot(conn)
            second_hash = _hash(second)
            if first != second or first_hash != second_hash:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "producer-snapshot-not-repeatable"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        market_after = _bundle_hash(market_path)
        source_after = _bundle_hash(source_path)
        if market_before != market_after or source_before != source_after:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "producer-database-changed-under-route-fence"
            )
        after_read = _trusted_datetime(trusted_clock, "archive-trusted-clock")
        if after_read < before_read:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-trusted-clock-regressed-under-fence"
            )
        summary = _replay(
            first, source_generation=source_generation, arm_id=arm_id,
            market_verifiers=market_verifiers,
            transition_verifier=transition_verifier,
            source_verifier=source_verifier,
            trusted_cutoff=after_read,
        )
        generation = first["MARKET_SOURCE"][_MARKET_TABLES[1]][0]
        if (
            fence["ownerEpochId"] != generation["owner_epoch_id"]
            or fence["ownerEpochHash"] != generation["owner_epoch_hash"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-fence-owner-generation-mismatch"
            )
        capture_body = {
            "schemaVersion": CAPTURE_SCHEMA, "route": ROUTE, "pdno": PDNO,
            "sourceGeneration": source_generation, "armId": arm_id,
            "fence": fence,
            "fenceObservedAt": fence["observedAt"],
            "trustedNowBeforeRead": _utc_text(before_read),
            "trustedNowAfterRead": _utc_text(after_read),
            "marketDatabaseBundleHashBefore": market_before,
            "marketDatabaseBundleHashAfter": market_after,
            "sourceDatabaseBundleHashBefore": source_before,
            "sourceDatabaseBundleHashAfter": source_after,
            "marketSchemaVersion": market.SCHEMA_VERSION,
            "marketSchemaFingerprint": market.SCHEMA_FINGERPRINT,
            "sourceSchemaVersion": source.SOURCE_JOURNAL_SCHEMA_VERSION,
            "sourceSchemaFingerprint": source.SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
            "logicalSnapshotHashBefore": first_hash,
            "logicalSnapshotHashAfter": second_hash,
            "replaySummary": summary,
            "freshDedicatedProducerDatabasesRequired": True,
            "atomicRouteOwnerObservationFenceHeld": True,
            "createIfAbsentPublicationRequired": True,
            "archiveAuthorityKeyIdHash": archive_authority_key_id_hash,
            "archiveAuthorityPurpose": CAPTURE_AUTHORITY_PURPOSE,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "networkAvailable": False, "mutationAvailable": False,
            "productionAvailable": False, "releaseAvailable": False,
        }
        capture_hash = _hash(capture_body)
        try:
            capture_signature = archive_capture_signer(
                CAPTURE_SIGNATURE_DOMAIN,
                {**deepcopy(capture_body), "captureHash": capture_hash},
            )
        except BaseException as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                f"archive-capture-sign-failed:{type(exc).__name__}"
            ) from None
        _verify_capture_signature(
            capture_body,
            capture_hash=capture_hash,
            signature=capture_signature,
            authority_key_id_hash=archive_authority_key_id_hash,
            verifier=archive_capture_verifier,
        )
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        archive = sqlite3.connect(str(temporary))
        try:
            archive.executescript(_ARCHIVE_SQL)
            archive.execute(
                "INSERT INTO kis_market_archive_manifest VALUES(1,?,?)",
                (ARCHIVE_SCHEMA_VERSION, ARCHIVE_SCHEMA_FINGERPRINT),
            )
            archive.execute(
                "INSERT INTO kis_market_archive_capture VALUES(1,?,?,?,?,?,?)",
                (
                    source_generation, arm_id, _canonical(capture_body),
                    capture_hash, capture_signature,
                    archive_authority_key_id_hash,
                ),
            )
            for component in ("MARKET_SOURCE", "SOURCE"):
                for table in sorted(first[component]):
                    for ordinal, row in enumerate(first[component][table], 1):
                        archive.execute(
                            "INSERT INTO kis_market_archive_row VALUES(?,?,?,?,?)",
                            (component, table, ordinal, _canonical(row), _hash(row)),
                        )
            archive.commit()
            _verify_archive_schema(archive)
        finally:
            archive.close()
        _fsync_file(temporary)
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-destination-race-lost"
            ) from exc
        except OSError as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-atomic-create-if-absent-failed"
            ) from exc
        _fsync_file(target)
        directory_sync_method = _fsync_directory(target.parent)
        archive_file_hash = _file_hash(target)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return {
        "schemaVersion": "kis-domestic-functional-market-archive-result/v1",
        "archivePath": str(target), "archiveFileHash": archive_file_hash,
        "captureHash": capture_hash, "replaySummaryHash": summary["summaryHash"],
        "archiveAuthorityKeyIdHash": archive_authority_key_id_hash,
        "directorySyncMethod": directory_sync_method,
        "atomicCreateIfAbsent": True, "fileAndParentSynced": True,
        "externalAsymmetricArchiveAuthorityPinned": False,
        "networkAvailable": False, "mutationAvailable": False,
        "productionAvailable": False, "releaseAvailable": False,
    }


def _publish_archive_snapshot(
    *,
    target: Path,
    snapshot: Mapping[str, Any],
    source_generation: str,
    arm_id: str,
    capture_body: Mapping[str, Any],
    capture_hash: str,
    capture_signature: str,
    archive_authority_key_id_hash: str,
) -> tuple[str, str]:
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        archive = sqlite3.connect(str(temporary))
        try:
            archive.executescript(_ARCHIVE_SQL)
            archive.execute(
                "INSERT INTO kis_market_archive_manifest VALUES(1,?,?)",
                (ARCHIVE_SCHEMA_VERSION, ARCHIVE_SCHEMA_FINGERPRINT),
            )
            archive.execute(
                "INSERT INTO kis_market_archive_capture VALUES(1,?,?,?,?,?,?)",
                (
                    source_generation, arm_id, _canonical(capture_body),
                    capture_hash, capture_signature,
                    archive_authority_key_id_hash,
                ),
            )
            for component in ("MARKET_SOURCE", "SOURCE"):
                for table in sorted(snapshot[component]):
                    for ordinal, row in enumerate(snapshot[component][table], 1):
                        archive.execute(
                            "INSERT INTO kis_market_archive_row VALUES(?,?,?,?,?)",
                            (component, table, ordinal, _canonical(row), _hash(row)),
                        )
            archive.commit()
            _verify_archive_schema(archive)
        finally:
            archive.close()
        _fsync_file(temporary)
        try:
            os.link(temporary, target)
        except FileExistsError as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-destination-race-lost"
            ) from exc
        except OSError as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-atomic-create-if-absent-failed"
            ) from exc
        _fsync_file(target)
        directory_sync_method = _fsync_directory(target.parent)
        return _file_hash(target), directory_sync_method
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def build_market_source_post_observation_archive(
    *,
    market_database: str | Path,
    source_database: str | Path,
    destination: str | Path,
    pre_observation_archive: str | Path,
    pre_observation_file_hash: str,
    pre_observation_capture_hash: str,
    source_generation: str,
    arm_id: str,
    observation_id: str,
    observation_fence: Callable[[], AbstractContextManager[Mapping[str, Any]]],
    fence_verifier: Callable[[Mapping[str, Any]], bool],
    market_verifiers: Mapping[str, Callable[[Mapping[str, Any]], bool]],
    transition_verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    archive_capture_signer: Callable[[str, Mapping[str, Any]], str],
    archive_capture_verifier: Callable[
        [str, Mapping[str, Any], str, str], bool
    ],
    archive_authority_key_id_hash: str,
    trusted_clock: Callable[[], datetime],
) -> dict[str, Any]:
    pre_result = verify_market_source_archive(
        pre_observation_archive,
        expected_file_hash=pre_observation_file_hash,
        source_generation=source_generation,
        arm_id=arm_id,
        fence_verifier=fence_verifier,
        market_verifiers=market_verifiers,
        transition_verifier=transition_verifier,
        source_verifier=source_verifier,
        archive_capture_verifier=archive_capture_verifier,
        expected_archive_authority_key_id_hash=archive_authority_key_id_hash,
    )
    if not hmac.compare_digest(
        str(pre_result["captureHash"]), pre_observation_capture_hash
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-predecessor-capture-hash-mismatch"
        )
    before_snapshot = _load_archive_snapshot(
        pre_observation_archive,
        expected_file_hash=pre_observation_file_hash,
    )
    market_path = Path(market_database).expanduser().resolve()
    source_path = Path(source_database).expanduser().resolve()
    target = Path(destination).expanduser().resolve()
    predecessor_path = Path(pre_observation_archive).expanduser().resolve()
    if (
        len({market_path, source_path, target, predecessor_path}) != 4
        or not market_path.is_file()
        or not source_path.is_file()
        or not predecessor_path.is_file()
        or not target.parent.is_dir()
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-archive-path-or-producer-invalid"
        )
    if target.exists():
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-destination-exists"
        )
    if (
        not callable(observation_fence)
        or not callable(trusted_clock)
        or not callable(archive_capture_signer)
        or not callable(archive_capture_verifier)
        or type(archive_authority_key_id_hash) is not str
        or not _SHA.fullmatch(archive_authority_key_id_hash)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "archive-fence-clock-required"
        )
    with observation_fence() as raw_fence:
        before_read = _trusted_datetime(trusted_clock, "archive-trusted-clock")
        fence = _verified_fence(
            raw_fence, source_generation=source_generation, arm_id=arm_id,
            verifier=fence_verifier, trusted_now=before_read,
        )
        market_before = _bundle_hash(market_path)
        source_before = _bundle_hash(source_path)
        market_schema_conn = sqlite3.connect(str(market_path))
        source_schema_conn = sqlite3.connect(str(source_path))
        market_schema_conn.row_factory = sqlite3.Row
        source_schema_conn.row_factory = sqlite3.Row
        try:
            market._verify_schema(market_schema_conn)
            source._verify_exact_source_schema(source_schema_conn)
        except BaseException as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                f"producer-schema-invalid:{type(exc).__name__}"
            ) from None
        finally:
            market_schema_conn.close()
            source_schema_conn.close()
        conn = sqlite3.connect(str(market_path))
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("ATTACH DATABASE ? AS source_db", (str(source_path),))
            conn.execute("BEGIN IMMEDIATE")
            first = _snapshot(conn)
            first_hash = _hash(first)
            second = _snapshot(conn)
            second_hash = _hash(second)
            if first != second or first_hash != second_hash:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "producer-snapshot-not-repeatable"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        market_after = _bundle_hash(market_path)
        source_after = _bundle_hash(source_path)
        if market_before != market_after or source_before != source_after:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "producer-database-changed-under-route-fence"
            )
        after_read = _trusted_datetime(trusted_clock, "archive-trusted-clock")
        if after_read < before_read:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-trusted-clock-regressed-under-fence"
            )
        summary = _replay(
            first, source_generation=source_generation, arm_id=arm_id,
            market_verifiers=market_verifiers,
            transition_verifier=transition_verifier,
            source_verifier=source_verifier, trusted_cutoff=after_read,
            capture_phase="POST_OBSERVATION_TRIGGER",
            selected_observation_id=observation_id,
        )
        prefix = _verify_prefix_extension(
            before_snapshot, first, source_generation=source_generation,
            arm_id=arm_id,
        )
        generation = first["MARKET_SOURCE"][_MARKET_TABLES[1]][0]
        if (
            fence["ownerEpochId"] != generation["owner_epoch_id"]
            or fence["ownerEpochHash"] != generation["owner_epoch_hash"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-fence-owner-generation-mismatch"
            )
        evidence = summary["postObservationEvidence"]
        capture_body = {
            "schemaVersion": POST_CAPTURE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": source_generation,
            "armId": arm_id,
            "fence": fence,
            "fenceObservedAt": fence["observedAt"],
            "trustedNowBeforeRead": _utc_text(before_read),
            "trustedNowAfterRead": _utc_text(after_read),
            "marketDatabaseBundleHashBefore": market_before,
            "marketDatabaseBundleHashAfter": market_after,
            "sourceDatabaseBundleHashBefore": source_before,
            "sourceDatabaseBundleHashAfter": source_after,
            "marketSchemaVersion": market.SCHEMA_VERSION,
            "marketSchemaFingerprint": market.SCHEMA_FINGERPRINT,
            "sourceSchemaVersion": source.SOURCE_JOURNAL_SCHEMA_VERSION,
            "sourceSchemaFingerprint": source.SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
            "logicalSnapshotHashBefore": first_hash,
            "logicalSnapshotHashAfter": second_hash,
            "replaySummary": summary,
            "freshDedicatedProducerDatabasesRequired": True,
            "atomicRouteOwnerObservationFenceHeld": True,
            "createIfAbsentPublicationRequired": True,
            "archiveAuthorityKeyIdHash": archive_authority_key_id_hash,
            "archiveAuthorityPurpose": CAPTURE_AUTHORITY_PURPOSE,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "productionAvailable": False,
            "releaseAvailable": False,
            "preObservationArchiveFileHash": pre_observation_file_hash,
            "preObservationCaptureHash": pre_observation_capture_hash,
            "preObservationLogicalSnapshotHash": _hash(before_snapshot),
            "selectedObservationId": observation_id,
            "selectedObservationHash": evidence["observationHash"],
            "selectedTriggerHash": evidence["triggerHash"],
            "prefixExtensionSummary": prefix,
            "postObservationPrefixExtensionRequired": True,
        }
        capture_hash = _hash(capture_body)
        try:
            capture_signature = archive_capture_signer(
                POST_CAPTURE_SIGNATURE_DOMAIN,
                {**deepcopy(capture_body), "captureHash": capture_hash},
            )
        except BaseException as exc:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                f"archive-capture-sign-failed:{type(exc).__name__}"
            ) from None
        _verify_capture_signature(
            capture_body, capture_hash=capture_hash,
            signature=capture_signature,
            authority_key_id_hash=archive_authority_key_id_hash,
            verifier=archive_capture_verifier,
            domain=POST_CAPTURE_SIGNATURE_DOMAIN,
        )
    archive_file_hash, directory_sync_method = _publish_archive_snapshot(
        target=target, snapshot=first, source_generation=source_generation,
        arm_id=arm_id, capture_body=capture_body,
        capture_hash=capture_hash, capture_signature=capture_signature,
        archive_authority_key_id_hash=archive_authority_key_id_hash,
    )
    return {
        "schemaVersion": (
            "kis-domestic-functional-market-post-observation-archive-result/v1"
        ),
        "archivePath": str(target),
        "archiveFileHash": archive_file_hash,
        "captureHash": capture_hash,
        "replaySummaryHash": summary["summaryHash"],
        "prefixExtensionHash": prefix["prefixExtensionHash"],
        "preObservationArchiveFileHash": pre_observation_file_hash,
        "archiveAuthorityKeyIdHash": archive_authority_key_id_hash,
        "directorySyncMethod": directory_sync_method,
        "atomicCreateIfAbsent": True,
        "fileAndParentSynced": True,
        "postObservationPrefixExtensionProven": True,
        "externalAsymmetricArchiveAuthorityPinned": False,
        "networkAvailable": False,
        "mutationAvailable": False,
        "productionAvailable": False,
        "releaseAvailable": False,
    }


def verify_market_source_archive(
    path: str | Path,
    *,
    expected_file_hash: str,
    source_generation: str,
    arm_id: str,
    fence_verifier: Callable[[Mapping[str, Any]], bool],
    market_verifiers: Mapping[str, Callable[[Mapping[str, Any]], bool]],
    transition_verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    archive_capture_verifier: Callable[
        [str, Mapping[str, Any], str, str], bool
    ],
    expected_archive_authority_key_id_hash: str,
) -> dict[str, Any]:
    archive_path = Path(path).expanduser().resolve()
    if (
        type(expected_file_hash) is not str
        or not _SHA.fullmatch(expected_file_hash)
        or type(expected_archive_authority_key_id_hash) is not str
        or not _SHA.fullmatch(expected_archive_authority_key_id_hash)
        or not callable(archive_capture_verifier)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-file-hash-invalid")
    if not archive_path.is_file() or not hmac.compare_digest(
        _file_hash(archive_path), expected_file_hash
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-file-drift")
    conn = sqlite3.connect(
        f"file:{archive_path.as_posix()}?mode=ro&immutable=1", uri=True
    ); conn.row_factory = sqlite3.Row
    try:
        _verify_archive_schema(conn)
        capture_row = conn.execute(
            "SELECT * FROM kis_market_archive_capture"
        ).fetchall()
        if len(capture_row) != 1:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-cardinality-invalid"
            )
        row = capture_row[0]
        capture = _decode(row["capture_json"], "archive-capture")
        if (
            set(capture) != _CAPTURE_KEYS
            or capture.get("sourceGeneration") != source_generation
            or capture.get("armId") != arm_id
            or _hash(capture) != row["capture_hash"]
            or row["archive_authority_key_id_hash"]
            != expected_archive_authority_key_id_hash
            or capture.get("archiveAuthorityKeyIdHash")
            != expected_archive_authority_key_id_hash
            or capture.get("archiveAuthorityPurpose")
            != CAPTURE_AUTHORITY_PURPOSE
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-projection-invalid"
            )
        _exact_row(
            row,
            {
                "singleton": 1,
                "source_generation": source_generation,
                "arm_id": arm_id,
                "capture_json": _canonical(capture),
                "capture_hash": row["capture_hash"],
                "capture_signature": row["capture_signature"],
                "archive_authority_key_id_hash": (
                    expected_archive_authority_key_id_hash
                ),
            },
            "archive-capture",
        )
        _verify_capture_signature(
            capture,
            capture_hash=str(row["capture_hash"]),
            signature=str(row["capture_signature"]),
            authority_key_id_hash=expected_archive_authority_key_id_hash,
            verifier=archive_capture_verifier,
        )
        before_read = _utc(
            capture["trustedNowBeforeRead"], "archive-capture-before-read"
        )
        after_read = _utc(
            capture["trustedNowAfterRead"], "archive-capture-after-read"
        )
        if after_read < before_read:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-clock-regressed"
            )
        fence = _verified_fence(
            capture["fence"], source_generation=source_generation,
            arm_id=arm_id, verifier=fence_verifier, trusted_now=before_read,
        )
        if capture["fenceObservedAt"] != fence["observedAt"]:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-fence-observed-at-mismatch"
            )
        snapshot: dict[str, dict[str, list[dict[str, Any]]]] = {
            "MARKET_SOURCE": {table: [] for table in _MARKET_TABLES},
            "SOURCE": {table: [] for table in _SOURCE_TABLES},
        }
        rows = conn.execute(
            "SELECT * FROM kis_market_archive_row ORDER BY "
            "component,table_name,ordinal"
        ).fetchall()
        for item in rows:
            component = str(item["component"]); table = str(item["table_name"])
            body = _decode(item["row_json"], "archive-row")
            if _hash(body) != item["row_hash"]:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "archive-row-hash-mismatch"
                )
            if component not in snapshot or table not in snapshot[component]:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "archive-row-table-not-allowed"
                )
            target_rows = snapshot[component][table]
            if int(item["ordinal"]) != len(target_rows) + 1:
                raise KisDomesticFunctionalMarketArchiveBlocked(
                    "archive-row-ordinal-gap"
                )
            target_rows.append(body)
        if set(snapshot["MARKET_SOURCE"]) != set(_MARKET_TABLES) or set(
            snapshot["SOURCE"]
        ) != set(_SOURCE_TABLES):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-table-set-not-exact"
            )
        if _hash(snapshot) != capture["logicalSnapshotHashBefore"] or (
            capture["logicalSnapshotHashBefore"]
            != capture["logicalSnapshotHashAfter"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-logical-snapshot-hash-mismatch"
            )
        summary = _replay(
            snapshot, source_generation=source_generation, arm_id=arm_id,
            market_verifiers=market_verifiers,
            transition_verifier=transition_verifier,
            source_verifier=source_verifier,
            trusted_cutoff=after_read,
        )
        generation = snapshot["MARKET_SOURCE"][_MARKET_TABLES[1]][0]
        bundle_fields = (
            "marketDatabaseBundleHashBefore", "marketDatabaseBundleHashAfter",
            "sourceDatabaseBundleHashBefore", "sourceDatabaseBundleHashAfter",
        )
        if any(
            type(capture.get(key)) is not str
            or not _SHA.fullmatch(capture[key])
            for key in bundle_fields
        ) or (
            capture["marketDatabaseBundleHashBefore"]
            != capture["marketDatabaseBundleHashAfter"]
            or capture["sourceDatabaseBundleHashBefore"]
            != capture["sourceDatabaseBundleHashAfter"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-producer-bundle-lineage-invalid"
            )
        expected_capture = {
            "schemaVersion": CAPTURE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": source_generation,
            "armId": arm_id,
            "fence": fence,
            "fenceObservedAt": fence["observedAt"],
            "trustedNowBeforeRead": _utc_text(before_read),
            "trustedNowAfterRead": _utc_text(after_read),
            "marketDatabaseBundleHashBefore": capture[
                "marketDatabaseBundleHashBefore"
            ],
            "marketDatabaseBundleHashAfter": capture[
                "marketDatabaseBundleHashAfter"
            ],
            "sourceDatabaseBundleHashBefore": capture[
                "sourceDatabaseBundleHashBefore"
            ],
            "sourceDatabaseBundleHashAfter": capture[
                "sourceDatabaseBundleHashAfter"
            ],
            "marketSchemaVersion": market.SCHEMA_VERSION,
            "marketSchemaFingerprint": market.SCHEMA_FINGERPRINT,
            "sourceSchemaVersion": source.SOURCE_JOURNAL_SCHEMA_VERSION,
            "sourceSchemaFingerprint": source.SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
            "logicalSnapshotHashBefore": _hash(snapshot),
            "logicalSnapshotHashAfter": _hash(snapshot),
            "replaySummary": summary,
            "freshDedicatedProducerDatabasesRequired": True,
            "atomicRouteOwnerObservationFenceHeld": True,
            "createIfAbsentPublicationRequired": True,
            "archiveAuthorityKeyIdHash": expected_archive_authority_key_id_hash,
            "archiveAuthorityPurpose": CAPTURE_AUTHORITY_PURPOSE,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "productionAvailable": False,
            "releaseAvailable": False,
        }
        if (
            capture != expected_capture
            or fence["ownerEpochId"] != generation["owner_epoch_id"]
            or fence["ownerEpochHash"] != generation["owner_epoch_hash"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-independent-reconstruction-mismatch"
            )
        return {
            "schemaVersion": "kis-domestic-functional-market-archive-verification/v1",
            "archiveFileHash": expected_file_hash,
            "captureHash": row["capture_hash"], "replaySummary": summary,
            "atomicRouteOwnerObservationFenceHeld": True,
            "allProducerRecordsIndependentlyReplayed": True,
            "freshDedicatedProducerDatabasesVerified": True,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "releaseCompletenessProven": False,
            "productionAvailable": False, "releaseAvailable": False,
        }
    finally:
        conn.close()


def verify_market_source_post_observation_archive(
    path: str | Path,
    *,
    expected_file_hash: str,
    pre_observation_archive: str | Path,
    pre_observation_file_hash: str,
    pre_observation_capture_hash: str,
    source_generation: str,
    arm_id: str,
    observation_id: str,
    fence_verifier: Callable[[Mapping[str, Any]], bool],
    market_verifiers: Mapping[str, Callable[[Mapping[str, Any]], bool]],
    transition_verifier: Callable[[str, Mapping[str, Any], str, str], bool],
    source_verifier: Callable[[str, Mapping[str, Any], str], bool],
    archive_capture_verifier: Callable[
        [str, Mapping[str, Any], str, str], bool
    ],
    expected_archive_authority_key_id_hash: str,
) -> dict[str, Any]:
    pre_result = verify_market_source_archive(
        pre_observation_archive,
        expected_file_hash=pre_observation_file_hash,
        source_generation=source_generation,
        arm_id=arm_id,
        fence_verifier=fence_verifier,
        market_verifiers=market_verifiers,
        transition_verifier=transition_verifier,
        source_verifier=source_verifier,
        archive_capture_verifier=archive_capture_verifier,
        expected_archive_authority_key_id_hash=(
            expected_archive_authority_key_id_hash
        ),
    )
    if not hmac.compare_digest(
        str(pre_result["captureHash"]), pre_observation_capture_hash
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked(
            "post-prefix-predecessor-capture-hash-mismatch"
        )
    before_snapshot = _load_archive_snapshot(
        pre_observation_archive,
        expected_file_hash=pre_observation_file_hash,
    )
    archive_path = Path(path).expanduser().resolve()
    if (
        type(expected_file_hash) is not str
        or not _SHA.fullmatch(expected_file_hash)
        or type(expected_archive_authority_key_id_hash) is not str
        or not _SHA.fullmatch(expected_archive_authority_key_id_hash)
        or not archive_path.is_file()
        or not hmac.compare_digest(_file_hash(archive_path), expected_file_hash)
    ):
        raise KisDomesticFunctionalMarketArchiveBlocked("archive-file-drift")
    conn = sqlite3.connect(
        f"file:{archive_path.as_posix()}?mode=ro&immutable=1", uri=True
    )
    conn.row_factory = sqlite3.Row
    try:
        _verify_archive_schema(conn)
        rows = conn.execute("SELECT * FROM kis_market_archive_capture").fetchall()
        if len(rows) != 1:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-cardinality-invalid"
            )
        row = rows[0]
        capture = _decode(row["capture_json"], "archive-capture")
        if (
            set(capture) != _POST_CAPTURE_KEYS
            or capture.get("schemaVersion") != POST_CAPTURE_SCHEMA
            or capture.get("sourceGeneration") != source_generation
            or capture.get("armId") != arm_id
            or capture.get("selectedObservationId") != observation_id
            or _hash(capture) != row["capture_hash"]
            or row["archive_authority_key_id_hash"]
            != expected_archive_authority_key_id_hash
            or capture.get("archiveAuthorityKeyIdHash")
            != expected_archive_authority_key_id_hash
            or capture.get("archiveAuthorityPurpose")
            != CAPTURE_AUTHORITY_PURPOSE
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "post-archive-capture-projection-invalid"
            )
        _exact_row(
            row,
            {
                "singleton": 1,
                "source_generation": source_generation,
                "arm_id": arm_id,
                "capture_json": _canonical(capture),
                "capture_hash": row["capture_hash"],
                "capture_signature": row["capture_signature"],
                "archive_authority_key_id_hash": (
                    expected_archive_authority_key_id_hash
                ),
            },
            "archive-capture",
        )
        _verify_capture_signature(
            capture, capture_hash=str(row["capture_hash"]),
            signature=str(row["capture_signature"]),
            authority_key_id_hash=expected_archive_authority_key_id_hash,
            verifier=archive_capture_verifier,
            domain=POST_CAPTURE_SIGNATURE_DOMAIN,
        )
        before_read = _utc(
            capture["trustedNowBeforeRead"], "archive-capture-before-read"
        )
        after_read = _utc(
            capture["trustedNowAfterRead"], "archive-capture-after-read"
        )
        if after_read < before_read:
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-clock-regressed"
            )
        fence = _verified_fence(
            capture["fence"], source_generation=source_generation,
            arm_id=arm_id, verifier=fence_verifier, trusted_now=before_read,
        )
        snapshot = _archive_snapshot(conn)
        if (
            _hash(snapshot) != capture["logicalSnapshotHashBefore"]
            or capture["logicalSnapshotHashBefore"]
            != capture["logicalSnapshotHashAfter"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-logical-snapshot-hash-mismatch"
            )
        summary = _replay(
            snapshot, source_generation=source_generation, arm_id=arm_id,
            market_verifiers=market_verifiers,
            transition_verifier=transition_verifier,
            source_verifier=source_verifier, trusted_cutoff=after_read,
            capture_phase="POST_OBSERVATION_TRIGGER",
            selected_observation_id=observation_id,
        )
        prefix = _verify_prefix_extension(
            before_snapshot, snapshot, source_generation=source_generation,
            arm_id=arm_id,
        )
        generation = snapshot["MARKET_SOURCE"][_MARKET_TABLES[1]][0]
        evidence = summary["postObservationEvidence"]
        bundle_fields = (
            "marketDatabaseBundleHashBefore", "marketDatabaseBundleHashAfter",
            "sourceDatabaseBundleHashBefore", "sourceDatabaseBundleHashAfter",
        )
        if any(
            type(capture.get(key)) is not str
            or not _SHA.fullmatch(capture[key])
            for key in bundle_fields
        ) or (
            capture["marketDatabaseBundleHashBefore"]
            != capture["marketDatabaseBundleHashAfter"]
            or capture["sourceDatabaseBundleHashBefore"]
            != capture["sourceDatabaseBundleHashAfter"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "archive-capture-producer-bundle-lineage-invalid"
            )
        expected_capture = {
            "schemaVersion": POST_CAPTURE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "sourceGeneration": source_generation,
            "armId": arm_id,
            "fence": fence,
            "fenceObservedAt": fence["observedAt"],
            "trustedNowBeforeRead": _utc_text(before_read),
            "trustedNowAfterRead": _utc_text(after_read),
            "marketDatabaseBundleHashBefore": capture[
                "marketDatabaseBundleHashBefore"
            ],
            "marketDatabaseBundleHashAfter": capture[
                "marketDatabaseBundleHashAfter"
            ],
            "sourceDatabaseBundleHashBefore": capture[
                "sourceDatabaseBundleHashBefore"
            ],
            "sourceDatabaseBundleHashAfter": capture[
                "sourceDatabaseBundleHashAfter"
            ],
            "marketSchemaVersion": market.SCHEMA_VERSION,
            "marketSchemaFingerprint": market.SCHEMA_FINGERPRINT,
            "sourceSchemaVersion": source.SOURCE_JOURNAL_SCHEMA_VERSION,
            "sourceSchemaFingerprint": source.SOURCE_JOURNAL_SCHEMA_FINGERPRINT,
            "logicalSnapshotHashBefore": _hash(snapshot),
            "logicalSnapshotHashAfter": _hash(snapshot),
            "replaySummary": summary,
            "freshDedicatedProducerDatabasesRequired": True,
            "atomicRouteOwnerObservationFenceHeld": True,
            "createIfAbsentPublicationRequired": True,
            "archiveAuthorityKeyIdHash": expected_archive_authority_key_id_hash,
            "archiveAuthorityPurpose": CAPTURE_AUTHORITY_PURPOSE,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "productionAvailable": False,
            "releaseAvailable": False,
            "preObservationArchiveFileHash": pre_observation_file_hash,
            "preObservationCaptureHash": pre_observation_capture_hash,
            "preObservationLogicalSnapshotHash": _hash(before_snapshot),
            "selectedObservationId": observation_id,
            "selectedObservationHash": evidence["observationHash"],
            "selectedTriggerHash": evidence["triggerHash"],
            "prefixExtensionSummary": prefix,
            "postObservationPrefixExtensionRequired": True,
        }
        if (
            capture != expected_capture
            or fence["ownerEpochId"] != generation["owner_epoch_id"]
            or fence["ownerEpochHash"] != generation["owner_epoch_hash"]
        ):
            raise KisDomesticFunctionalMarketArchiveBlocked(
                "post-archive-capture-independent-reconstruction-mismatch"
            )
        return {
            "schemaVersion": (
                "kis-domestic-functional-market-post-observation-archive-"
                "verification/v1"
            ),
            "archiveFileHash": expected_file_hash,
            "captureHash": row["capture_hash"],
            "preObservationArchiveFileHash": pre_observation_file_hash,
            "replaySummary": summary,
            "prefixExtensionSummary": prefix,
            "atomicRouteOwnerObservationFenceHeld": True,
            "allProducerRecordsIndependentlyReplayed": True,
            "postObservationPrefixExtensionProven": True,
            "externalAsymmetricArchiveAuthorityPinned": False,
            "releaseCompletenessProven": False,
            "productionAvailable": False,
            "releaseAvailable": False,
        }
    finally:
        conn.close()


def market_archive_component_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-market-archive-component/v1",
        "route": ROUTE, "pdno": PDNO,
        "marketSchemaVersion": market.SCHEMA_VERSION,
        "sourceSchemaVersion": source.SOURCE_JOURNAL_SCHEMA_VERSION,
        "archiveSchemaVersion": ARCHIVE_SCHEMA_VERSION,
        "archiveSchemaFingerprint": ARCHIVE_SCHEMA_FINGERPRINT,
        "atomicRouteOwnerObservationFenceRequired": True,
        "independentProducerReplayRequired": True,
        "freshDedicatedProducerDatabasesRequired": True,
        "archiveCaptureSignatureRequired": True,
        "postObservationPrefixArchiveSupported": True,
        "immutablePredecessorPrefixReplayRequired": True,
        "postObservationPrefixExtensionProven": False,
        "archiveAuthorityPurpose": CAPTURE_AUTHORITY_PURPOSE,
        "externalAsymmetricArchiveAuthorityPinned": False,
        "releaseCompletenessProven": False,
        "networkAvailable": False, "mutationAvailable": False,
        "productionAvailable": False, "releaseAvailable": False,
    }


__all__ = [
    "ARCHIVE_SCHEMA_FINGERPRINT", "ARCHIVE_SCHEMA_VERSION",
    "CAPTURE_AUTHORITY_PURPOSE", "CAPTURE_SCHEMA", "CAPTURE_SIGNATURE_DOMAIN",
    "FENCE_SCHEMA", "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_MUTATION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MARKET_ARCHIVE_RELEASE_AVAILABLE",
    "KisDomesticFunctionalMarketArchiveBlocked", "POST_CAPTURE_SCHEMA",
    "POST_CAPTURE_SIGNATURE_DOMAIN", "build_market_source_archive",
    "build_market_source_post_observation_archive",
    "market_archive_component_status", "verify_market_source_archive",
    "verify_market_source_post_observation_archive",
]
