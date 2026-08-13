from __future__ import annotations

"""Offline dual-source diagnostic for the KIS domestic functional lane.

This reducer deliberately creates no market-data or order authority.  It
replays the raw official minute GET bytes a second time, verifies concrete
registry-derived archive/source/grant envelopes, and compares the resulting
11 diagnostic 5m OHLC bars with an authenticated H0STCNT0 observation archive.
Even a perfect match is only ``DIAGNOSTIC_DUAL_SOURCE_CONFIRMATION_ONLY``:
KIS's primary minute-chart samples do not promise a native 5m interval, an
explicit finalized flag, an official server timestamp, or continuation
completeness.
"""

import base64
import hashlib
import hmac
import json
import math
import re
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .kis_domestic_functional_candle_get import (
    BAR_COUNT,
    ENDPOINT,
    LIVE_ORIGIN,
    PDNO,
    ROUTE,
    TR_ID,
    KisDomesticFunctionalCandleGetBlocked,
    KisDomesticFunctionalCandleGetVerifier,
)
from .kis_domestic_functional_production_factory import RegistryDerivedVerifier


KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_PRODUCTION_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_ORDER_AUTHORITY_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_RELEASE_AVAILABLE = False

DIAGNOSTIC_CLASSIFICATION = "DIAGNOSTIC_DUAL_SOURCE_CONFIRMATION_ONLY"
MINUTE_ROWS = 55
_KST = ZoneInfo("Asia/Seoul")
_SHA = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$", re.ASCII)
_NUMBER = re.compile(r"^[0-9]+(?:\.[0-9]+)?$", re.ASCII)

_GET_ARCHIVE_DOMAIN = "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GET_ARCHIVE"
_GRANT_DOMAIN = "KIS_DOMESTIC_FUNCTIONAL_CANDLE_GRANT_PROJECTION"
_DUAL_ARCHIVE_DOMAIN = "KIS_DOMESTIC_FUNCTIONAL_CANDLE_DUAL_SOURCE_ARCHIVE"
_SOURCE_OBSERVATION_DOMAIN = "SOURCE_OBSERVATION"
_SOURCE_WINDOW_DOMAIN = "BAR_WINDOW"
_SOURCE_EVALUATION_DOMAIN = "NATURAL_BREAKOUT_EVALUATION"
_SOURCE_FRAME_DOMAIN = "RAW_H0STCNT0_FRAME"

_ENVELOPE_KEYS = {"body", "recordHash", "signature", "keyIdHash"}
_GET_ARCHIVE_KEYS = {
    "schemaVersion", "route", "pdno", "origin", "endpoint", "trId",
    "accountFingerprint", "credentialConfigurationHash",
    "registryAcceptedHeadHash", "candleBundleHash", "candleResultHash",
    "captureId", "dispatchOrdinal", "queryHmacSha256", "rawRequestSha256",
    "rawResponseSha256", "bodyHash", "signedClientAuditBeforeHash",
    "signedClientAuditAfterHash", "capturedAt", "rawBytesIncluded",
    "singlePhysicalAttempt", "hiddenRetryCount", "redirectFollowCount",
    "productionAvailable", "orderAuthorityAvailable", "releaseAvailable",
}
_GRANT_KEYS = {
    "schemaVersion", "route", "pdno", "accountFingerprint",
    "registryAcceptedHeadHash", "armId", "sourceGeneration",
    "grantReceiptHash", "grantWallAt", "grantMonotonicNs", "capturedOnce",
    "productionAvailable", "orderAuthorityAvailable", "releaseAvailable",
}
_DUAL_ARCHIVE_KEYS = {
    "schemaVersion", "route", "pdno", "accountFingerprint",
    "registryAcceptedHeadHash", "armId", "sourceGeneration",
    "marketArchiveFileHash", "marketArchiveCaptureHash",
    "archiveAuthorityKeyIdHash", "marketArchivePrefix",
    "sourceObservation", "capturedAt", "productionAvailable",
    "orderAuthorityAvailable", "releaseAvailable",
}
_PREFIX_KEYS = {
    "sourceGeneration", "armId", "marketIngressCount",
    "marketTransitionCount", "marketIngressHeadHash", "sourceFrameCount",
    "sourceEventCount", "sourceFrameHeadHash", "sourceTransitionCount",
    "sourceTransitionHeadHash", "sourceObservationCount",
    "allObservationRowsIndependentlyReplayed",
    "allProducerRowProjectionsExact", "freshDedicatedProducerDatabasesVerified",
    "allRawFramesReparsed46Fields", "allProducerSignaturesVerified",
    "allTransitionChainsVerified", "marketSourceBijectionVerified", "summaryHash",
}
_OBSERVATION_KEYS = {
    "schemaVersion", "observationId", "armId", "sourceGeneration",
    "socketIdentityHash", "captureHeadHash", "windowBody", "windowSignature",
    "rawArchive", "rawArchiveHash", "evaluationProof", "evaluationProofHash",
    "evaluationSignature", "averageRange", "triggerPrice", "naturalSignal",
    "boundary",
}
_WINDOW_KEYS = {
    "schemaVersion", "route", "origin", "pdno", "source", "sourceProvider",
    "sourceGeneration", "firstSourceSequence", "lastSourceSequence",
    "sourceEventCount", "sourceProofHash", "interval",
    "artifactContentHash", "artifactFileSha256", "instanceContentHash",
    "instanceFileSha256", "bars", "observedAt",
}
_SOURCE_BAR_KEYS = {
    "openAt", "closeAt", "open", "high", "low", "close",
    "sourceSequenceStart", "sourceSequenceEnd", "eventCount",
    "rawEventChainHash",
}
_RAW_ARCHIVE_KEYS = {
    "schemaVersion", "route", "pdno", "armId", "sourceGeneration",
    "socketIdentityHash", "firstSourceSequence", "lastSourceSequence",
    "sourceEventCount", "captureHeadHash", "authorityKeyIdHash",
    "upstreamExchangeSequenceAvailable", "upstreamPacketCompletenessAttested",
    "acceptedIngressContinuityOnly", "marketSourceIntegrationComplete",
    "marketSourceIngressLinkCount", "marketSourceIngressLinkHeadHash",
    "marketSourceIngressLinks", "frames", "events", "nextOpenEvent",
    "recomputedBars",
}
_FRAME_ENVELOPE_KEYS = {"body", "envelopeHash", "serverSignature", "frameHeadHash"}


class KisDomesticFunctionalCandleArchiveBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError):
        raise KisDomesticFunctionalCandleArchiveBlocked(
            "candle-archive-value-not-canonical"
        ) from None


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, field: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid")
    return value


def _registry_signature(value: object, field: str) -> str:
    if type(value) is not str:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid") from None
    if len(decoded) != 64 or base64.b64encode(decoded).decode("ascii") != value:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid")
    return value


def _identifier(value: object, field: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid")
    return value


def _utc(value: object, field: str) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-invalid") from None
    canonical = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    if parsed.tzinfo is None or canonical != value:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-noncanonical")
    return parsed.astimezone(timezone.utc)


def _decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    if type(value) is not str:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-not-string")
    raw = value.strip()
    if not raw or not _NUMBER.fullmatch(raw):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-malformed")
    try:
        parsed = Decimal(raw)
    except InvalidOperation:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-malformed") from None
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{field}-range-invalid")
    return parsed


def _number_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _json_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "candle-get-raw-json-duplicate-key"
            )
        result[key] = value
    return result


def _trusted_now(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise KisDomesticFunctionalCandleArchiveBlocked("archive-trusted-clock-invalid")
    result = value.astimezone(timezone.utc)
    if not math.isfinite(result.timestamp()):
        raise KisDomesticFunctionalCandleArchiveBlocked("archive-trusted-clock-invalid")
    return result


def _verifier_binding(
    verifier: RegistryDerivedVerifier,
    purpose: str,
) -> dict[str, Any]:
    if type(verifier) is not RegistryDerivedVerifier:
        raise KisDomesticFunctionalCandleArchiveBlocked(
            f"registry-derived-verifier-type-invalid:{purpose}"
        )
    binding = verifier.binding_status()
    if (
        not isinstance(binding, Mapping)
        or binding.get("purpose") != purpose
        or binding.get("verifyOnly") is not True
        or binding.get("productionAvailable") is not False
    ):
        raise KisDomesticFunctionalCandleArchiveBlocked(
            f"registry-derived-verifier-binding-invalid:{purpose}"
        )
    return dict(binding)


def _verify_registry_envelope(
    value: object,
    *,
    keys: set[str],
    verifier: RegistryDerivedVerifier,
    domain: str,
    label: str,
    observed_at: datetime | None = None,
) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping) or set(value) != _ENVELOPE_KEYS:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{label}-envelope-not-exact")
    body_value = value.get("body")
    if not isinstance(body_value, Mapping) or set(body_value) != keys:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{label}-fields-not-exact")
    body = dict(body_value)
    record_hash = _sha(value.get("recordHash"), f"{label}-record-hash")
    signature = _registry_signature(value.get("signature"), f"{label}-signature")
    key_id = _sha(value.get("keyIdHash"), f"{label}-key-id")
    if not hmac.compare_digest(record_hash, _hash(body)):
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{label}-record-hash-mismatch")
    observed = observed_at
    if observed is None:
        observed_raw = body.get("capturedAt") or body.get("grantWallAt")
        observed = _utc(observed_raw, f"{label}-observed-at")
    elif type(observed) is not datetime or observed.tzinfo is None:
        raise KisDomesticFunctionalCandleArchiveBlocked(
            f"{label}-observed-at-invalid"
        )
    if verifier.verify(
        domain=domain,
        body=body,
        signature=signature,
        key_id_hash=key_id,
        observed_at=observed,
    ) is not True:
        raise KisDomesticFunctionalCandleArchiveBlocked(f"{label}-signature-invalid")
    return body, record_hash


def _independent_get_bars(
    candle_bundle: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str, str]:
    body = candle_bundle.get("body")
    if not isinstance(body, Mapping):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-bundle-body-invalid")
    pages = body.get("pages")
    if type(pages) is not list or len(pages) != 1 or not isinstance(pages[0], Mapping):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-page-cardinality-invalid")
    page = pages[0]
    raw_encoded = page.get("rawResponseBytesBase64")
    if type(raw_encoded) is not str:
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-raw-bytes-missing")
    try:
        raw = base64.b64decode(raw_encoded, validate=True)
    except (ValueError, TypeError):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-raw-bytes-invalid") from None
    raw_hash = hashlib.sha256(raw).hexdigest()
    if not hmac.compare_digest(raw_hash, _sha(page.get("rawResponseSha256"), "raw-response-hash")):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-raw-byte-hash-mismatch")
    try:
        parsed = json.loads(raw.decode("utf-8"), object_pairs_hook=_json_no_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-raw-json-invalid") from None
    if parsed != page.get("body") or not isinstance(parsed, Mapping):
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-raw-body-mismatch")
    rows = parsed.get("output2")
    if type(rows) is not list or len(rows) < MINUTE_ROWS:
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-window-truncated")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, row_value in enumerate(rows):
        if not isinstance(row_value, Mapping):
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-row-invalid")
        row = row_value
        date_raw = row.get("stck_bsop_date")
        time_raw = row.get("stck_cntg_hour")
        if (
            type(date_raw) is not str
            or not re.fullmatch(r"[0-9]{8}", date_raw, re.ASCII)
            or type(time_raw) is not str
            or not re.fullmatch(r"[0-9]{6}", time_raw, re.ASCII)
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-trade-time-invalid")
        key = date_raw + time_raw
        if key in seen:
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-timestamp-duplicate")
        seen.add(key)
        try:
            local = datetime.strptime(key, "%Y%m%d%H%M%S").replace(tzinfo=_KST)
        except ValueError:
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-trade-time-invalid") from None
        if local.second or not (time(9, 0) <= local.time() < time(15, 30)):
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-grid-invalid")
        opened = _decimal(row.get("stck_oprc"), f"minute-{index}-open", positive=True)
        high = _decimal(row.get("stck_hgpr"), f"minute-{index}-high", positive=True)
        low = _decimal(row.get("stck_lwpr"), f"minute-{index}-low", positive=True)
        close = _decimal(row.get("stck_prpr"), f"minute-{index}-close", positive=True)
        if not (low <= opened <= high and low <= close <= high):
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-ohlc-invalid")
        normalized.append(
            {
                "at": local.astimezone(timezone.utc),
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
            }
        )
    normalized.sort(key=lambda item: item["at"])
    selected = normalized[-MINUTE_ROWS:]
    if selected[0]["at"].astimezone(_KST).minute % 5:
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-window-not-five-minute-aligned")
    for left, right in zip(selected, selected[1:]):
        if right["at"] - left["at"] != timedelta(minutes=1):
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-minute-window-not-contiguous")
    bars: list[dict[str, Any]] = []
    for offset in range(0, MINUTE_ROWS, 5):
        group = selected[offset : offset + 5]
        opened = group[0]["at"]
        comparable = {
            "schemaVersion": "kis-h0stcnt0-rest-ohlc-link/v1",
            "pdno": PDNO,
            "openAt": opened.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "closeAt": (opened + timedelta(minutes=5)).isoformat(
                timespec="microseconds"
            ).replace("+00:00", "Z"),
            "open": _number_text(group[0]["open"]),
            "high": _number_text(max(item["high"] for item in group)),
            "low": _number_text(min(item["low"] for item in group)),
            "close": _number_text(group[-1]["close"]),
        }
        bars.append({**comparable, "h0stcnt0ComparableHash": _hash(comparable)})
    if len(bars) != BAR_COUNT:
        raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-five-minute-count-invalid")
    return bars, raw_hash, _hash(parsed)


class KisDomesticFunctionalCandleArchiveVerifier:
    """Concrete verify-only composition.  No signer, writer, or sender exists."""

    def __init__(
        self,
        *,
        candle_get_verifier: KisDomesticFunctionalCandleGetVerifier,
        signed_get_verifier: RegistryDerivedVerifier,
        lane_grant_verifier: RegistryDerivedVerifier,
        source_record_verifier: RegistryDerivedVerifier,
        archive_extraction_verifier: RegistryDerivedVerifier,
        trusted_clock: Callable[[], datetime],
    ) -> None:
        if type(candle_get_verifier) is not KisDomesticFunctionalCandleGetVerifier:
            raise KisDomesticFunctionalCandleArchiveBlocked("candle-get-verifier-type-invalid")
        bindings = {
            "signedGet": _verifier_binding(signed_get_verifier, "SIGNED_GET_CAPTURE_VERIFY"),
            "lane": _verifier_binding(lane_grant_verifier, "LANE_RECORD_VERIFY"),
            "source": _verifier_binding(source_record_verifier, "SOURCE_RECORD_VERIFY"),
            "archive": _verifier_binding(archive_extraction_verifier, "ARCHIVE_EXTRACTION_VERIFY"),
        }
        join_fields = (
            "factoryBindingHash", "registryAcceptedHeadHash", "accountFingerprint",
            "credentialConfigurationHash", "codeManifestHash",
        )
        first = bindings["signedGet"]
        if any(
            binding.get(field) != first.get(field)
            for binding in bindings.values()
            for field in join_fields
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "registry-derived-verifier-lineage-mismatch"
            )
        if not callable(trusted_clock):
            raise KisDomesticFunctionalCandleArchiveBlocked("archive-trusted-clock-missing")
        self._candle = candle_get_verifier
        self._signed_get = signed_get_verifier
        self._lane = lane_grant_verifier
        self._source = source_record_verifier
        self._archive = archive_extraction_verifier
        self._binding = first
        self._clock = trusted_clock

    def _verify_get_archive(
        self,
        value: object,
        *,
        candle_result: Mapping[str, Any],
        bundle_hash: str,
    ) -> tuple[dict[str, Any], str]:
        body, record_hash = _verify_registry_envelope(
            value,
            keys=_GET_ARCHIVE_KEYS,
            verifier=self._signed_get,
            domain=_GET_ARCHIVE_DOMAIN,
            label="signed-get-archive",
        )
        exact = {
            "schemaVersion": "kis-domestic-functional-candle-get-archive/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "endpoint": ENDPOINT,
            "trId": TR_ID,
            "accountFingerprint": self._binding["accountFingerprint"],
            "credentialConfigurationHash": self._binding["credentialConfigurationHash"],
            "registryAcceptedHeadHash": self._binding["registryAcceptedHeadHash"],
            "candleBundleHash": bundle_hash,
            "candleResultHash": candle_result["resultHash"],
            "captureId": candle_result["captureId"],
            "dispatchOrdinal": candle_result["dispatchOrdinal"],
            "queryHmacSha256": candle_result["queryHmacSha256"],
            "rawRequestSha256": candle_result["rawRequestSha256"],
            "rawResponseSha256": candle_result["rawResponseSha256"],
            "bodyHash": candle_result["bodyHash"],
            "signedClientAuditBeforeHash": candle_result["signedClientAuditBeforeHash"],
            "signedClientAuditAfterHash": candle_result["signedClientAuditAfterHash"],
            "rawBytesIncluded": True,
            "singlePhysicalAttempt": True,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        for field, wanted in exact.items():
            if type(body.get(field)) is not type(wanted) or body.get(field) != wanted:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"signed-get-archive-binding-mismatch:{field}"
                )
        return body, record_hash

    def _verify_grant(self, value: object) -> tuple[dict[str, Any], str, datetime]:
        body, record_hash = _verify_registry_envelope(
            value,
            keys=_GRANT_KEYS,
            verifier=self._lane,
            domain=_GRANT_DOMAIN,
            label="lane-grant-projection",
        )
        exact = {
            "schemaVersion": "kis-domestic-functional-candle-grant-projection/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "accountFingerprint": self._binding["accountFingerprint"],
            "registryAcceptedHeadHash": self._binding["registryAcceptedHeadHash"],
            "capturedOnce": True,
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        for field, wanted in exact.items():
            if type(body.get(field)) is not type(wanted) or body.get(field) != wanted:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"lane-grant-binding-mismatch:{field}"
                )
        _identifier(body.get("armId"), "lane-grant-arm-id")
        _identifier(body.get("sourceGeneration"), "lane-grant-source-generation")
        _sha(body.get("grantReceiptHash"), "lane-grant-receipt-hash")
        if type(body.get("grantMonotonicNs")) is not int or body["grantMonotonicNs"] < 0:
            raise KisDomesticFunctionalCandleArchiveBlocked("lane-grant-monotonic-invalid")
        grant_at = _utc(body["grantWallAt"], "lane-grant-wall-at")
        now = _trusted_now(self._clock)
        if grant_at > now + timedelta(seconds=1):
            raise KisDomesticFunctionalCandleArchiveBlocked("lane-grant-from-future")
        return body, record_hash, grant_at

    def _verify_source_observation(
        self,
        value: object,
        *,
        arm_id: str,
        source_generation: str,
    ) -> tuple[dict[str, Any], str, list[dict[str, Any]], dict[str, Any]]:
        if not isinstance(value, Mapping) or not isinstance(value.get("body"), Mapping):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "source-observation-envelope-not-exact"
            )
        unsigned_window = value["body"].get("windowBody")
        if not isinstance(unsigned_window, Mapping):
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-missing")
        source_observed_at = _utc(
            unsigned_window.get("observedAt"), "source-window-observed-at"
        )
        observation, observation_hash = _verify_registry_envelope(
            value,
            keys=_OBSERVATION_KEYS,
            verifier=self._source,
            domain=_SOURCE_OBSERVATION_DOMAIN,
            label="source-observation",
            observed_at=source_observed_at,
        )
        if (
            observation.get("schemaVersion")
            != "kis-domestic-functional-source-observation-record/v1"
            or observation.get("armId") != arm_id
            or observation.get("sourceGeneration") != source_generation
            or observation.get("naturalSignal") != "BUY"
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "source-observation-lineage-mismatch"
            )
        window = observation.get("windowBody")
        raw_archive = observation.get("rawArchive")
        evaluation = observation.get("evaluationProof")
        if not isinstance(window, Mapping) or set(window) != _WINDOW_KEYS:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-fields-not-exact")
        if not isinstance(raw_archive, Mapping) or set(raw_archive) != _RAW_ARCHIVE_KEYS:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-raw-archive-fields-not-exact")
        if not isinstance(evaluation, Mapping):
            raise KisDomesticFunctionalCandleArchiveBlocked("source-evaluation-missing")
        authority_key_id = _sha(raw_archive.get("authorityKeyIdHash"), "source-authority-key-id")
        if self._source.verify(
            domain=_SOURCE_WINDOW_DOMAIN,
            body=window,
            signature=_registry_signature(
                observation.get("windowSignature"), "source-window-signature"
            ),
            key_id_hash=authority_key_id,
            observed_at=_utc(window.get("observedAt"), "source-window-observed-at"),
        ) is not True:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-signature-invalid")
        if self._source.verify(
            domain=_SOURCE_EVALUATION_DOMAIN,
            body=evaluation,
            signature=_registry_signature(
                observation.get("evaluationSignature"), "source-evaluation-signature"
            ),
            key_id_hash=authority_key_id,
            observed_at=_utc(window.get("observedAt"), "source-window-observed-at"),
        ) is not True:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-evaluation-signature-invalid")
        if (
            observation.get("rawArchiveHash") != _hash(raw_archive)
            or observation.get("evaluationProofHash") != _hash(evaluation)
            or observation.get("captureHeadHash") != raw_archive.get("captureHeadHash")
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "source-observation-content-hash-mismatch"
            )
        bars_value = window.get("bars")
        recomputed_value = raw_archive.get("recomputedBars")
        if type(bars_value) is not list or len(bars_value) != BAR_COUNT or recomputed_value != bars_value:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-bar-projection-mismatch")
        bars: list[dict[str, Any]] = []
        for index, value_bar in enumerate(bars_value):
            if not isinstance(value_bar, Mapping) or set(value_bar) != _SOURCE_BAR_KEYS:
                raise KisDomesticFunctionalCandleArchiveBlocked("source-bar-fields-not-exact")
            bar = dict(value_bar)
            opened = _utc(bar.get("openAt"), f"source-bar-{index}-open-at")
            closed = _utc(bar.get("closeAt"), f"source-bar-{index}-close-at")
            if closed - opened != timedelta(minutes=5):
                raise KisDomesticFunctionalCandleArchiveBlocked("source-bar-duration-invalid")
            opened_price = _decimal(bar.get("open"), f"source-bar-{index}-open", positive=True)
            high = _decimal(bar.get("high"), f"source-bar-{index}-high", positive=True)
            low = _decimal(bar.get("low"), f"source-bar-{index}-low", positive=True)
            close = _decimal(bar.get("close"), f"source-bar-{index}-close", positive=True)
            if not (low <= opened_price <= high and low <= close <= high):
                raise KisDomesticFunctionalCandleArchiveBlocked("source-bar-ohlc-invalid")
            if type(bar.get("eventCount")) is not int or bar["eventCount"] < 1:
                raise KisDomesticFunctionalCandleArchiveBlocked("source-bar-event-count-invalid")
            _sha(bar.get("rawEventChainHash"), "source-bar-raw-event-chain-hash")
            bars.append(bar)
        if any(left["closeAt"] != right["openAt"] for left, right in zip(bars, bars[1:])):
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-not-contiguous")
        source_proof = {
            "schemaVersion": "kis-h0stcnt0-bar-source-proof/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "sourceProvider": "kis",
            "sourceGeneration": source_generation,
            "firstSourceSequence": bars[0]["sourceSequenceStart"],
            "lastSourceSequence": bars[-1]["sourceSequenceEnd"],
            "sourceEventCount": sum(bar["eventCount"] for bar in bars),
            "barRawEventChainHashes": [bar["rawEventChainHash"] for bar in bars],
        }
        if (
            window.get("schemaVersion") != "kis-domestic-official-5m-window/v1"
            or window.get("route") != ROUTE
            or window.get("origin") != LIVE_ORIGIN
            or window.get("pdno") != PDNO
            or window.get("source") != "KIS_WEBSOCKET_H0STCNT0"
            or window.get("sourceProvider") != "kis"
            or window.get("sourceGeneration") != source_generation
            or window.get("interval") != "5m"
            or window.get("firstSourceSequence") != source_proof["firstSourceSequence"]
            or window.get("lastSourceSequence") != source_proof["lastSourceSequence"]
            or window.get("sourceEventCount") != source_proof["sourceEventCount"]
            or window.get("sourceProofHash") != _hash(source_proof)
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked("source-window-proof-mismatch")
        if (
            raw_archive.get("schemaVersion")
            != "kis-domestic-h0stcnt0-durable-window-archive/v1"
            or raw_archive.get("route") != ROUTE
            or raw_archive.get("pdno") != PDNO
            or raw_archive.get("armId") != arm_id
            or raw_archive.get("sourceGeneration") != source_generation
            or raw_archive.get("sourceEventCount") != source_proof["sourceEventCount"]
            or raw_archive.get("firstSourceSequence") != source_proof["firstSourceSequence"]
            or raw_archive.get("lastSourceSequence") != source_proof["lastSourceSequence"]
            or raw_archive.get("upstreamExchangeSequenceAvailable") is not False
            or raw_archive.get("upstreamPacketCompletenessAttested") is not False
            or raw_archive.get("acceptedIngressContinuityOnly") is not True
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked("source-raw-archive-lineage-mismatch")
        frames = raw_archive.get("frames")
        if type(frames) is not list or not frames:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-raw-frames-missing")
        for frame_value in frames:
            if not isinstance(frame_value, Mapping) or set(frame_value) != _FRAME_ENVELOPE_KEYS:
                raise KisDomesticFunctionalCandleArchiveBlocked("source-frame-envelope-not-exact")
            frame = frame_value.get("body")
            if not isinstance(frame, Mapping):
                raise KisDomesticFunctionalCandleArchiveBlocked("source-frame-body-invalid")
            frame_hash = _sha(frame_value.get("envelopeHash"), "source-frame-envelope-hash")
            if frame_hash != _hash(frame):
                raise KisDomesticFunctionalCandleArchiveBlocked("source-frame-envelope-hash-mismatch")
            if self._source.verify(
                domain=_SOURCE_FRAME_DOMAIN,
                body=frame,
                signature=_registry_signature(
                    frame_value.get("serverSignature"), "source-frame-signature"
                ),
                key_id_hash=authority_key_id,
                observed_at=_utc(frame.get("receivedAt"), "source-frame-received-at"),
            ) is not True:
                raise KisDomesticFunctionalCandleArchiveBlocked("source-frame-signature-invalid")
            _sha(frame_value.get("frameHeadHash"), "source-frame-head-hash")
        boundary = observation.get("boundary")
        if not isinstance(boundary, Mapping) or set(boundary) != {
            "barOpenAt", "observedAt", "openPriceKrw", "sourceSequence", "rawEventHash"
        }:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-boundary-fields-not-exact")
        if boundary.get("barOpenAt") != bars[-1]["closeAt"]:
            raise KisDomesticFunctionalCandleArchiveBlocked("source-boundary-window-mismatch")
        _utc(boundary.get("observedAt"), "source-boundary-observed-at")
        return observation, observation_hash, bars, dict(raw_archive)

    def _verify_dual_source_archive(
        self,
        value: object,
        *,
        grant: Mapping[str, Any],
        rest_bars: Sequence[Mapping[str, Any]],
        grant_at: datetime,
    ) -> tuple[dict[str, Any], str, str]:
        body, record_hash = _verify_registry_envelope(
            value,
            keys=_DUAL_ARCHIVE_KEYS,
            verifier=self._archive,
            domain=_DUAL_ARCHIVE_DOMAIN,
            label="dual-source-archive",
        )
        exact = {
            "schemaVersion": "kis-domestic-functional-candle-dual-source-archive/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "accountFingerprint": self._binding["accountFingerprint"],
            "registryAcceptedHeadHash": self._binding["registryAcceptedHeadHash"],
            "armId": grant["armId"],
            "sourceGeneration": grant["sourceGeneration"],
            "productionAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
        }
        for field, wanted in exact.items():
            if type(body.get(field)) is not type(wanted) or body.get(field) != wanted:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"dual-source-archive-binding-mismatch:{field}"
                )
        for field in (
            "marketArchiveFileHash", "marketArchiveCaptureHash", "archiveAuthorityKeyIdHash"
        ):
            _sha(body.get(field), f"dual-source-{field}")
        prefix_value = body.get("marketArchivePrefix")
        if not isinstance(prefix_value, Mapping) or set(prefix_value) != _PREFIX_KEYS:
            raise KisDomesticFunctionalCandleArchiveBlocked("market-archive-prefix-fields-not-exact")
        prefix = dict(prefix_value)
        prefix_unsigned = dict(prefix)
        summary_hash = prefix_unsigned.pop("summaryHash", None)
        if summary_hash != _hash(prefix_unsigned):
            raise KisDomesticFunctionalCandleArchiveBlocked("market-archive-prefix-hash-mismatch")
        required_prefix = {
            "sourceGeneration": grant["sourceGeneration"],
            "armId": grant["armId"],
            "sourceObservationCount": 0,
            "allObservationRowsIndependentlyReplayed": True,
            "allProducerRowProjectionsExact": True,
            "freshDedicatedProducerDatabasesVerified": True,
            "allRawFramesReparsed46Fields": True,
            "allProducerSignaturesVerified": True,
            "allTransitionChainsVerified": True,
            "marketSourceBijectionVerified": True,
        }
        for field, wanted in required_prefix.items():
            if type(prefix.get(field)) is not type(wanted) or prefix.get(field) != wanted:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"market-archive-prefix-mismatch:{field}"
                )
        observation, observation_hash, source_bars, raw_archive = (
            self._verify_source_observation(
                body.get("sourceObservation"),
                arm_id=grant["armId"],
                source_generation=grant["sourceGeneration"],
            )
        )
        if (
            prefix.get("sourceFrameHeadHash") != raw_archive.get("captureHeadHash")
            or prefix.get("sourceEventCount") != raw_archive.get("sourceEventCount") + 1
            or prefix.get("sourceFrameCount") != len(raw_archive.get("frames", []))
            or prefix.get("marketIngressCount")
            != raw_archive.get("marketSourceIngressLinkCount")
            or prefix.get("marketIngressHeadHash")
            != raw_archive.get("marketSourceIngressLinkHeadHash")
            or raw_archive.get("marketSourceIntegrationComplete") is not True
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "market-prefix-source-observation-head-join-mismatch"
            )
        if len(source_bars) != len(rest_bars):
            raise KisDomesticFunctionalCandleArchiveBlocked("dual-source-bar-count-mismatch")
        comparison_hashes: list[str] = []
        for index, (rest, source) in enumerate(zip(rest_bars, source_bars)):
            comparable = {
                "schemaVersion": "kis-h0stcnt0-rest-ohlc-link/v1",
                "pdno": PDNO,
                "openAt": source["openAt"],
                "closeAt": source["closeAt"],
                "open": _number_text(_decimal(source["open"], f"source-{index}-open")),
                "high": _number_text(_decimal(source["high"], f"source-{index}-high")),
                "low": _number_text(_decimal(source["low"], f"source-{index}-low")),
                "close": _number_text(_decimal(source["close"], f"source-{index}-close")),
            }
            if comparable != {key: rest[key] for key in comparable}:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"dual-source-ohlc-mismatch:{index}"
                )
            comparable_hash = _hash(comparable)
            if rest.get("h0stcnt0ComparableHash") != comparable_hash:
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"dual-source-comparable-hash-mismatch:{index}"
                )
            comparison_hashes.append(comparable_hash)
        source_last_close = _utc(source_bars[-1]["closeAt"], "source-last-close")
        if source_last_close >= grant_at:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "dual-source-selected-window-not-strictly-before-grant"
            )
        if _utc(observation["boundary"]["observedAt"], "source-observed-at") > grant_at:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "source-observation-after-grant"
            )
        return body, record_hash, _hash(comparison_hashes)

    def verify(
        self,
        *,
        candle_bundle: Mapping[str, Any],
        signed_get_archive: Mapping[str, Any],
        lane_grant_projection: Mapping[str, Any],
        market_source_archive: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        try:
            candle_result = self._candle.verify(candle_bundle)
        except KisDomesticFunctionalCandleGetBlocked as exc:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                f"candle-get-verifier-rejected:{exc}"
            ) from None
        if candle_result.get("finalizedElevenBarAuthorityAvailable") is not False:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "candle-get-authority-must-remain-false"
            )
        bundle_hash = _sha(candle_bundle.get("bundleHash"), "candle-bundle-hash")
        get_archive, get_archive_hash = self._verify_get_archive(
            signed_get_archive,
            candle_result=candle_result,
            bundle_hash=bundle_hash,
        )
        rest_bars, raw_response_hash, raw_body_hash = _independent_get_bars(candle_bundle)
        if (
            raw_response_hash != candle_result["rawResponseSha256"]
            or raw_body_hash != candle_result["bodyHash"]
            or len(candle_result.get("diagnosticBars", [])) != BAR_COUNT
        ):
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "candle-get-independent-replay-mismatch"
            )
        for index, (replayed, published) in enumerate(
            zip(rest_bars, candle_result["diagnosticBars"])
        ):
            if any(published.get(key) != value for key, value in replayed.items()):
                raise KisDomesticFunctionalCandleArchiveBlocked(
                    f"candle-get-published-bar-replay-mismatch:{index}"
                )
        grant, grant_hash, grant_at = self._verify_grant(lane_grant_projection)
        rest_last_close = _utc(rest_bars[-1]["closeAt"], "rest-last-close")
        if rest_last_close >= grant_at:
            raise KisDomesticFunctionalCandleArchiveBlocked(
                "candle-get-selected-window-not-strictly-before-grant"
            )
        dual_matched = False
        dual_archive_hash = ""
        comparison_hash = ""
        blockers = {
            "OFFICIAL_API_NATIVE_5M_INTERVAL_NOT_DOCUMENTED",
            "OFFICIAL_API_EXPLICIT_BAR_FINALIZATION_NOT_DOCUMENTED",
            "OFFICIAL_API_SERVER_TIME_NOT_AVAILABLE",
            "OFFICIAL_API_CONTINUATION_PAGINATION_NOT_DOCUMENTED",
            "UPSTREAM_EXCHANGE_PACKET_COMPLETENESS_NOT_ATTESTED",
            "SHARED_ROUTE_AND_ORDER_AUTHORITY_NOT_WIRED",
            "DURABLE_PRODUCTION_ARCHIVE_PUBLICATION_NOT_WIRED",
        }
        if market_source_archive is None:
            blockers.add("AUTHENTICATED_MARKET_SOURCE_ARCHIVE_NOT_AVAILABLE")
        else:
            _, dual_archive_hash, comparison_hash = self._verify_dual_source_archive(
                market_source_archive,
                grant=grant,
                rest_bars=rest_bars,
                grant_at=grant_at,
            )
            dual_matched = True
        now = _trusted_now(self._clock)
        if _utc(get_archive["capturedAt"], "get-archive-captured-at") > now + timedelta(seconds=1):
            raise KisDomesticFunctionalCandleArchiveBlocked("get-archive-from-future")
        result = {
            "schemaVersion": "kis-domestic-functional-candle-dual-source-result/v1",
            "classification": DIAGNOSTIC_CLASSIFICATION,
            "route": ROUTE,
            "pdno": PDNO,
            "accountFingerprint": self._binding["accountFingerprint"],
            "credentialConfigurationHash": self._binding["credentialConfigurationHash"],
            "registryAcceptedHeadHash": self._binding["registryAcceptedHeadHash"],
            "factoryBindingHash": self._binding["factoryBindingHash"],
            "candleBundleHash": bundle_hash,
            "candleResultHash": candle_result["resultHash"],
            "signedGetArchiveHash": get_archive_hash,
            "laneGrantProjectionHash": grant_hash,
            "marketSourceArchiveHash": dual_archive_hash,
            "dualSourceComparisonHash": comparison_hash,
            "grantWallAt": grant["grantWallAt"],
            "selectedWindowOpenAt": rest_bars[0]["openAt"],
            "selectedWindowCloseAt": rest_bars[-1]["closeAt"],
            "selectedWindowStrictlyBeforeGrant": True,
            "officialMinuteRowsIndependentlyReplayed": True,
            "diagnosticFiveMinuteBarsIndependentlyRecomputed": True,
            "authenticatedMarketSourceArchiveAvailable": dual_matched,
            "dualSourceOhlcHashesMatched": dual_matched,
            "upstreamPacketCompletenessAttested": False,
            "officialFinalizationGuaranteed": False,
            "officialServerTimeAvailable": False,
            "officialPaginationCompletenessProven": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "orderAuthorityAvailable": False,
            "releaseAvailable": False,
            "promotionEligible": False,
            "blockedReasons": sorted(blockers),
        }
        return {**result, "resultHash": _hash(result)}


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-functional-candle-archive-status/v1",
        "classification": DIAGNOSTIC_CLASSIFICATION,
        "route": ROUTE,
        "pdno": PDNO,
        "concreteRegistryDerivedVerifierRequired": True,
        "officialFinalizationGuaranteed": False,
        "upstreamPacketCompletenessAttested": False,
        "productionAvailable": False,
        "networkAvailable": False,
        "orderAuthorityAvailable": False,
        "releaseAvailable": False,
        "promotionAvailable": False,
        "reason": "DIAGNOSTIC_DUAL_SOURCE_CONFIRMATION_ONLY_NO_ORDER_OR_RELEASE_AUTHORITY",
    }


__all__ = [
    "DIAGNOSTIC_CLASSIFICATION",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_ORDER_AUTHORITY_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_PRODUCTION_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_CANDLE_ARCHIVE_RELEASE_AVAILABLE",
    "KisDomesticFunctionalCandleArchiveBlocked",
    "KisDomesticFunctionalCandleArchiveVerifier",
    "production_entrypoint_status",
]
