from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .env_loader import (
    default_runtime_data_root,
    live_secret_name,
    live_secret_store,
    load_local_env,
)
from .kis_domestic_functional_get_client import (
    KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
    KisDomesticFunctionalGetClient,
    kis_domestic_functional_account_fingerprint,
)
from .kis_domestic_functional_truth import KisDomesticFunctionalTruthReader


KST = ZoneInfo("Asia/Seoul")
AUTHORITY_SECRET_NAME = live_secret_name(
    "KIS_DOMESTIC_FUNCTIONAL_GET_AUTHORITY_V1"
)
EXECUTION_ENV_GATE = "KIS_DOMESTIC_FUNCTIONAL_SIGNED_GET_PREFLIGHT_ENABLED"
_AUTHORITY_VALUE = re.compile(r"^v1:([0-9a-f]{96})$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL_CONTINUATIONS = frozenset({"", "D", "E"})
_BASELINE_ROUTES = {
    "balance": (
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "TTTC8434R",
    ),
    "dailyCcld": (
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "TTTC0081R",
    ),
    "workingOrders": (
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
        "TTTC0084R",
    ),
    "periodTradeProfit": (
        "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
        "TTTC8715R",
    ),
    "periodProfit": (
        "/uapi/domestic-stock/v1/trading/inquire-period-profit",
        "TTTC8708R",
    ),
    "holiday": (
        "/uapi/domestic-stock/v1/quotations/chk-holiday",
        "CTCA0903R",
    ),
}
_QUOTE_ROUTE = (
    "/uapi/domestic-stock/v1/quotations/inquire-price",
    "FHKST01010100",
)
_SENSITIVE_OUTPUT_KEYS = frozenset(
    {
        "cano",
        "acnt_prdt_cd",
        "accountno",
        "accountproductcode",
        "appkey",
        "appsecret",
        "authorization",
        "token",
        "body",
        "normalized",
        "rawcaptures",
        "balance",
        "cashkrw",
        "targetquantity",
    }
)


class KisDomesticSignedGetPreflightBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise KisDomesticSignedGetPreflightBlocked(f"{label}-not-object")
    return value


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise KisDomesticSignedGetPreflightBlocked(f"{label}-invalid")
    return value


@dataclass(frozen=True, repr=False)
class KisSignedGetAuthority:
    key: bytes
    key_id: str
    key_id_hash: str
    restart_verifiable: bool
    source: str

    def __repr__(self) -> str:
        return (
            "KisSignedGetAuthority("
            f"key_id_hash={self.key_id_hash!r},"
            f"restart_verifiable={self.restart_verifiable!r},"
            f"source={self.source!r},key='<redacted>')"
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "keyIdHash": self.key_id_hash,
            "restartVerifiable": self.restart_verifiable,
            "durable": self.restart_verifiable,
        }


def _authority_from_key(
    key: bytes,
    *,
    source: str,
    restart_verifiable: bool,
) -> KisSignedGetAuthority:
    if not isinstance(key, bytes) or len(key) != 48:
        raise KisDomesticSignedGetPreflightBlocked(
            "kis-signed-get-authority-key-invalid"
        )
    fingerprint = hashlib.sha256(key).hexdigest()
    key_id = f"{source}:sha256:{fingerprint}"
    return KisSignedGetAuthority(
        key=bytes(key),
        key_id=key_id,
        key_id_hash=hashlib.sha256(key_id.encode("utf-8")).hexdigest(),
        restart_verifiable=restart_verifiable,
        source=source,
    )


def provision_durable_kis_signed_get_authority(
    *,
    store: Any | None = None,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> Mapping[str, Any]:
    """Explicitly provision the server-owned key; never runs as GET side effect."""

    secret_store = store or live_secret_store()
    existing = str(secret_store.get(AUTHORITY_SECRET_NAME) or "").strip()
    if existing:
        match = _AUTHORITY_VALUE.fullmatch(existing)
        if match is None:
            raise KisDomesticSignedGetPreflightBlocked(
                "kis-signed-get-durable-authority-malformed"
            )
        authority = _authority_from_key(
            bytes.fromhex(match.group(1)),
            source="secret-store",
            restart_verifiable=True,
        )
        return {**authority.snapshot(), "created": False}
    key = random_bytes(48)
    if not isinstance(key, bytes) or len(key) != 48:
        raise KisDomesticSignedGetPreflightBlocked(
            "kis-signed-get-authority-random-source-invalid"
        )
    try:
        secret_store.set(AUTHORITY_SECRET_NAME, "v1:" + key.hex())
        reread = str(secret_store.get(AUTHORITY_SECRET_NAME) or "").strip()
    except BaseException as exc:
        raise KisDomesticSignedGetPreflightBlocked(
            f"kis-signed-get-authority-store-failed:{type(exc).__name__}"
        ) from None
    if not hmac.compare_digest(reread, "v1:" + key.hex()):
        raise KisDomesticSignedGetPreflightBlocked(
            "kis-signed-get-authority-store-verification-failed"
        )
    authority = _authority_from_key(
        key,
        source="secret-store",
        restart_verifiable=True,
    )
    return {**authority.snapshot(), "created": True}


def load_kis_signed_get_authority(
    *,
    store: Any | None = None,
    allow_ephemeral: bool = False,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> KisSignedGetAuthority:
    secret_store = store or live_secret_store()
    try:
        encoded = str(secret_store.get(AUTHORITY_SECRET_NAME) or "").strip()
    except BaseException as exc:
        raise KisDomesticSignedGetPreflightBlocked(
            f"kis-signed-get-authority-load-failed:{type(exc).__name__}"
        ) from None
    if encoded:
        match = _AUTHORITY_VALUE.fullmatch(encoded)
        if match is None:
            raise KisDomesticSignedGetPreflightBlocked(
                "kis-signed-get-durable-authority-malformed"
            )
        return _authority_from_key(
            bytes.fromhex(match.group(1)),
            source="secret-store",
            restart_verifiable=True,
        )
    if not allow_ephemeral:
        raise KisDomesticSignedGetPreflightBlocked(
            "kis-signed-get-durable-authority-not-provisioned"
        )
    key = random_bytes(48)
    return _authority_from_key(
        key,
        source="ephemeral-process",
        restart_verifiable=False,
    )


def _normalize_account_from_environment() -> tuple[str, str]:
    account = os.getenv("KIS_ACCOUNT_NO", "").strip().replace(" ", "")
    product = os.getenv("KIS_ACCOUNT_PRODUCT_CODE", "").strip() or "01"
    if "-" in account:
        cano, suffix = account.split("-", 1)
        product = suffix.strip() or product
    elif len(account) > 8:
        cano, embedded = account[:8], account[8:10]
        product = embedded or product
    else:
        cano = account
    return cano.strip(), product.strip()


def _verify_baseline_pages(
    baseline: Mapping[str, Any],
    *,
    client: KisDomesticFunctionalGetClient,
) -> dict[str, Any]:
    body = _mapping(baseline.get("body"), "baseline-body")
    baseline_hash = _sha(baseline.get("baselineHash"), "baseline-hash")
    if not hmac.compare_digest(_hash(body), baseline_hash):
        raise KisDomesticSignedGetPreflightBlocked("baseline-hash-mismatch")
    signed = {**dict(body), "baselineHash": baseline_hash}
    if not client.verify_capture_envelope(
        signed,
        baseline.get("serverAuthoritySignature"),
    ):
        raise KisDomesticSignedGetPreflightBlocked("baseline-signature-invalid")
    if body.get("durableCasPersisted") is not False:
        raise KisDomesticSignedGetPreflightBlocked(
            "baseline-preflight-must-not-claim-durable-cas"
        )
    if body.get("accountFingerprint") != client.account_fingerprint:
        raise KisDomesticSignedGetPreflightBlocked("baseline-account-mismatch")
    if body.get("credentialConfigurationHash") != client.credential_configuration_hash:
        raise KisDomesticSignedGetPreflightBlocked("baseline-credential-mismatch")
    captures = baseline.get("rawCaptures")
    if not isinstance(captures, list) or len(captures) != 2:
        raise KisDomesticSignedGetPreflightBlocked("baseline-capture-count-invalid")
    page_counts: dict[str, int] = {name: 0 for name in _BASELINE_ROUTES}
    capture_hashes: list[str] = []
    for capture_index, capture_value in enumerate(captures):
        capture = _mapping(capture_value, f"capture-{capture_index}")
        capture_hashes.append(_sha(capture.get("captureHash"), "capture-hash"))
        signed_capture = dict(capture)
        capture_signature = signed_capture.pop("serverAuthoritySignature", None)
        signed_capture.pop("serverAuthorityEvidenceDurable", None)
        if not client.verify_capture_envelope(signed_capture, capture_signature):
            raise KisDomesticSignedGetPreflightBlocked(
                "baseline-capture-signature-invalid"
            )
        endpoints = _mapping(capture.get("endpoints"), "capture-endpoints")
        if set(endpoints) != set(_BASELINE_ROUTES):
            raise KisDomesticSignedGetPreflightBlocked(
                "baseline-endpoint-set-not-exact"
            )
        for name, (expected_endpoint, expected_tr_id) in _BASELINE_ROUTES.items():
            endpoint = _mapping(endpoints.get(name), f"baseline-{name}")
            pages = endpoint.get("pages")
            if not isinstance(pages, list) or not pages:
                raise KisDomesticSignedGetPreflightBlocked(
                    f"baseline-{name}-pages-missing"
                )
            page_counts[name] += len(pages)
            for index, page_value in enumerate(pages):
                page = _mapping(page_value, f"baseline-{name}-page")
                if (
                    page.get("pageIndex") != index
                    or page.get("method") != "GET"
                    or page.get("origin") != KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN
                    or page.get("endpoint") != expected_endpoint
                    or page.get("trId") != expected_tr_id
                ):
                    raise KisDomesticSignedGetPreflightBlocked(
                        f"baseline-{name}-page-identity-invalid"
                    )
            if pages[-1].get("continuationReceived") not in _TERMINAL_CONTINUATIONS:
                raise KisDomesticSignedGetPreflightBlocked(
                    f"baseline-{name}-pagination-incomplete"
                )
    request_count = baseline.get("officialGetRequestCount")
    if type(request_count) is not int or request_count != sum(page_counts.values()):
        raise KisDomesticSignedGetPreflightBlocked(
            "baseline-request-count-mismatch"
        )
    return {
        "baselineHash": baseline_hash,
        "serverAuthoritySignature": _sha(
            baseline.get("serverAuthoritySignature"),
            "baseline-signature",
        ),
        "capturedAt": body.get("capturedAt"),
        "causalProjectionHash": _sha(
            body.get("causalProjectionHash"),
            "baseline-projection-hash",
        ),
        "rawCaptureHashes": capture_hashes,
        "officialGetRequestCount": request_count,
        "minimumGetPacingFloorSeconds": baseline.get(
            "minimumGetPacingFloorSeconds"
        ),
        "pageCountsAcrossTwoCaptures": [
            {
                "endpoint": endpoint,
                "trId": tr_id,
                "pageCount": page_counts[name],
            }
            for name, (endpoint, tr_id) in _BASELINE_ROUTES.items()
        ],
        "durableCasPersisted": False,
    }


def _verify_quote(
    quote: Mapping[str, Any],
    *,
    client: KisDomesticFunctionalGetClient,
) -> dict[str, Any]:
    if quote.get("method") != "GET" or (
        quote.get("endpoint"), quote.get("trId")
    ) != _QUOTE_ROUTE:
        raise KisDomesticSignedGetPreflightBlocked("quote-route-not-exact")
    if quote.get("origin") != KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN:
        raise KisDomesticSignedGetPreflightBlocked("quote-origin-not-exact")
    if quote.get("accountFingerprint") != client.account_fingerprint:
        raise KisDomesticSignedGetPreflightBlocked("quote-account-mismatch")
    if quote.get("credentialConfigurationHash") != client.credential_configuration_hash:
        raise KisDomesticSignedGetPreflightBlocked("quote-credential-mismatch")
    if quote.get("durableCasPersisted") is not False:
        raise KisDomesticSignedGetPreflightBlocked(
            "quote-preflight-must-not-claim-durable-cas"
        )
    quote_hash = _sha(quote.get("quoteHash"), "quote-hash")
    signed_body = dict(quote)
    signature = signed_body.pop("serverAuthoritySignature", None)
    envelope = dict(signed_body)
    envelope.pop("quoteHash", None)
    if not hmac.compare_digest(_hash(envelope), quote_hash):
        raise KisDomesticSignedGetPreflightBlocked("quote-hash-mismatch")
    if not client.verify_capture_envelope(signed_body, signature):
        raise KisDomesticSignedGetPreflightBlocked("quote-signature-invalid")
    if quote.get("orderCapSatisfied") is not True:
        raise KisDomesticSignedGetPreflightBlocked("quote-order-cap-not-satisfied")
    return {
        "quoteHash": quote_hash,
        "serverAuthoritySignature": _sha(signature, "quote-signature"),
        "observedAt": quote.get("observedAt"),
        "elapsedSeconds": quote.get("elapsedSeconds"),
        "priceKrw": quote.get("priceKrw"),
        "quantity": quote.get("quantity"),
        "notionalKrw": quote.get("notionalKrw"),
        "orderCapSatisfied": True,
        "durableCasPersisted": False,
    }


def _assert_safe_output(value: Mapping[str, Any], forbidden_values: Sequence[str]) -> None:
    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                normalized = re.sub(r"[^a-z0-9_]", "", str(key).lower())
                if normalized in _SENSITIVE_OUTPUT_KEYS:
                    raise KisDomesticSignedGetPreflightBlocked(
                        "preflight-output-sensitive-field-forbidden"
                    )
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    serialized = _canonical(value).decode("utf-8")
    for secret in forbidden_values:
        if secret and secret in serialized:
            raise KisDomesticSignedGetPreflightBlocked(
                "preflight-output-secret-detected"
            )
    if "?" in serialized or "Bearer " in serialized:
        raise KisDomesticSignedGetPreflightBlocked(
            "preflight-output-request-secret-detected"
        )


class KisDomesticSignedGetPreflightRunner:
    def __init__(
        self,
        *,
        client: KisDomesticFunctionalGetClient,
        reader: KisDomesticFunctionalTruthReader,
        authority: KisSignedGetAuthority,
    ) -> None:
        if type(client) is not KisDomesticFunctionalGetClient:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-runner-client-not-exact"
            )
        if type(reader) is not KisDomesticFunctionalTruthReader:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-runner-reader-not-exact"
            )
        if reader.client is not client:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-runner-client-reader-mismatch"
            )
        self.client = client
        self.reader = reader
        self.authority = authority

    def run(self) -> dict[str, Any]:
        before = self.client.audit_snapshot()
        baseline = self.reader.read_preactivation_baseline()
        quote = self.reader.read_fresh_quote_preflight()
        after = self.client.audit_snapshot()
        audit_signature = after.get("signatureHash")
        audit_body = dict(after)
        audit_body.pop("signatureHash", None)
        if not self.client.verify_capture_envelope(audit_body, audit_signature):
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-client-audit-signature-invalid"
            )
        before_gets = int(before.get("officialGetDispatchCount") or 0)
        run_gets = int(after.get("officialGetDispatchCount") or 0) - before_gets
        if (
            after.get("hiddenGetRetryCount") != 0
            or after.get("redirectFollowCount") != 0
            or after.get("authenticationOauthHiddenRetryCount") != 0
            or after.get("authenticationOauthRedirectFollowCount") != 0
        ):
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-hidden-retry-or-redirect-observed"
            )
        before_oauth_posts = before.get("authenticationOauthPostDispatchCount")
        after_oauth_posts = after.get("authenticationOauthPostDispatchCount")
        if (
            type(before_oauth_posts) is not int
            or type(after_oauth_posts) is not int
            or before_oauth_posts < 0
            or after_oauth_posts < before_oauth_posts
            or after.get("authenticationOauthPostAuthOnly") is not True
        ):
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-oauth-post-audit-invalid"
            )
        run_oauth_posts = after_oauth_posts - before_oauth_posts
        baseline_summary = _verify_baseline_pages(baseline, client=self.client)
        quote_summary = _verify_quote(quote, client=self.client)
        expected_gets = baseline_summary["officialGetRequestCount"] + 1
        if run_gets != expected_gets:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-audit-request-count-mismatch"
            )
        if after.get("tradingPostDeleteDispatchCount") != 0:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-trading-mutation-observed"
            )
        physical_complete = after.get(
            "physicalOfficialGetAttemptCountComplete"
        )
        if type(physical_complete) is not bool:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-physical-attempt-audit-invalid"
            )
        before_physical = before.get("physicalOfficialGetAttemptCount")
        after_physical = after.get("physicalOfficialGetAttemptCount")
        if type(before_physical) is not int or type(after_physical) is not int:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-physical-attempt-audit-invalid"
            )
        run_physical = after_physical - before_physical
        dispatches = after.get("dispatches")
        if not isinstance(dispatches, list) or len(dispatches) < run_gets:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-dispatch-trace-invalid"
            )
        run_dispatches = dispatches[-run_gets:] if run_gets else []
        interval = after.get("minimumRequestIntervalSeconds")
        if (
            not isinstance(interval, (int, float))
            or isinstance(interval, bool)
            or not math.isfinite(float(interval))
            or float(interval) < 0
        ):
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-pacing-audit-invalid"
            )
        starts: list[float] = []
        for record in run_dispatches:
            if not isinstance(record, Mapping):
                raise KisDomesticSignedGetPreflightBlocked(
                    "signed-get-dispatch-trace-invalid"
                )
            started = record.get("monotonicStartedAt")
            if (
                not isinstance(started, (int, float))
                or isinstance(started, bool)
                or not math.isfinite(float(started))
            ):
                raise KisDomesticSignedGetPreflightBlocked(
                    "signed-get-pacing-audit-invalid"
                )
            starts.append(float(started))
            if physical_complete and (
                record.get("physicalAttemptCount") != 1
                or record.get("physicalAttemptCountComplete") is not True
                or record.get("effectiveUrlExact") is not True
                or record.get("redirectFollowed") is not False
                or record.get("transportOutcome") != "RESPONSE"
                or type(record.get("statusCode")) is not int
            ):
                raise KisDomesticSignedGetPreflightBlocked(
                    "signed-get-physical-dispatch-provenance-invalid"
                )
        for previous, current in zip(starts, starts[1:]):
            if current - previous + 1e-9 < float(interval):
                raise KisDomesticSignedGetPreflightBlocked(
                    "signed-get-physical-pacing-floor-not-met"
                )
        physical_elapsed = starts[-1] - starts[0] if len(starts) > 1 else 0.0
        if physical_complete and run_physical != run_gets:
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-physical-attempt-count-mismatch"
            )
        authority_snapshot = self.authority.snapshot()
        if (
            after.get("serverAuthorityKeyIdHash")
            != authority_snapshot["keyIdHash"]
            or after.get("serverAuthorityRestartVerifiable")
            is not authority_snapshot["restartVerifiable"]
        ):
            raise KisDomesticSignedGetPreflightBlocked(
                "signed-get-authority-audit-mismatch"
            )
        evidence = {
            "schemaVersion": "kis-domestic-signed-get-preflight/v1",
            "environment": "KIS_LIVE",
            "mode": "SIGNED_GET_ONLY_PREFLIGHT",
            "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "accountFingerprint": self.client.account_fingerprint,
            "credentialConfigurationHash": self.client.credential_configuration_hash,
            "authority": authority_snapshot,
            "baseline": baseline_summary,
            "quote": quote_summary,
            "officialRead": {
                "routePairs": [
                    {"endpoint": endpoint, "trId": tr_id}
                    for endpoint, tr_id in [*_BASELINE_ROUTES.values(), _QUOTE_ROUTE]
                ],
                "getRequestCount": run_gets,
                "authenticationTokenReadCount": int(
                    after.get("authenticationTokenReadCount") or 0
                )
                - int(before.get("authenticationTokenReadCount") or 0),
                "oauthTokenIssuanceMayUsePost": True,
                "authenticationOauthPostDispatchCount": run_oauth_posts,
                "authenticationOauthPostCountComplete": after.get(
                    "authenticationOauthPostCountComplete"
                ),
                "authenticationOauthPostAuthOnly": True,
                "authenticationOauthHiddenRetryCount": 0,
                "authenticationOauthRedirectFollowCount": 0,
                "physicalGetAttemptCount": run_physical,
                "physicalGetAttemptCountComplete": physical_complete,
                "hiddenGetRetryCount": 0,
                "redirectFollowCount": 0,
                "physicalPacingElapsedSeconds": physical_elapsed,
                "physicalPacingFloorSeconds": max(
                    0.0,
                    (run_gets - 1) * float(interval),
                ),
                "tradingPostDeleteDispatchCount": 0,
                "allTradingMutationsForbidden": True,
            },
            "clientAudit": {
                "auditHash": _hash(audit_body),
                "signatureHash": _sha(audit_signature, "client-audit-signature"),
                "minimumRequestIntervalSeconds": after.get(
                    "minimumRequestIntervalSeconds"
                ),
                "pacingWaitSeconds": after.get("pacingWaitSeconds"),
            },
            "tradingAuthorityIssued": False,
            "durableBaselineCasPersisted": False,
            "networkOrderPostAllowed": False,
            "releaseEvidenceEligible": False,
        }
        forbidden_values = (
            os.getenv("KIS_ACCOUNT_NO", ""),
            os.getenv("KIS_APP_KEY", ""),
            os.getenv("KIS_APP_SECRET", ""),
            os.getenv("KIS_HTS_ID", ""),
        )
        _assert_safe_output(evidence, forbidden_values)
        signature = self.client.sign_capture_envelope(evidence)
        return {
            **evidence,
            "evidenceHash": _hash(evidence),
            "serverAuthoritySignature": signature,
        }


def write_redacted_signed_get_evidence(
    evidence: Mapping[str, Any],
    path: str | Path,
    *,
    client: KisDomesticFunctionalGetClient,
) -> Path:
    if type(client) is not KisDomesticFunctionalGetClient:
        raise KisDomesticSignedGetPreflightBlocked(
            "evidence-writer-client-not-exact"
        )
    envelope = dict(_mapping(evidence, "evidence"))
    signature = envelope.pop("serverAuthoritySignature", None)
    evidence_hash = envelope.pop("evidenceHash", None)
    if (
        type(evidence_hash) is not str
        or not hmac.compare_digest(_hash(envelope), evidence_hash)
    ):
        raise KisDomesticSignedGetPreflightBlocked("evidence-hash-invalid")
    if not client.verify_capture_envelope(envelope, signature):
        raise KisDomesticSignedGetPreflightBlocked("evidence-signature-invalid")
    _assert_safe_output(
        envelope,
        (
            os.getenv("KIS_ACCOUNT_NO", ""),
            os.getenv("KIS_APP_KEY", ""),
            os.getenv("KIS_APP_SECRET", ""),
            os.getenv("KIS_HTS_ID", ""),
        ),
    )
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        dict(evidence),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        indent=2,
    ) + "\n"
    temporary = target.with_name(
        f".{target.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def build_production_signed_get_preflight_runner(
    *,
    authority: KisSignedGetAuthority,
    clock: Callable[[], datetime] | None = None,
) -> KisDomesticSignedGetPreflightRunner:
    load_local_env()
    cano, product = _normalize_account_from_environment()
    fingerprint = kis_domestic_functional_account_fingerprint(cano, product)
    client = KisDomesticFunctionalGetClient.from_environment(
        expected_account_fingerprint=fingerprint,
        server_authority_key=authority.key,
        server_authority_key_id=authority.key_id,
        server_authority_restart_verifiable=authority.restart_verifiable,
    )
    now = (clock or (lambda: datetime.now(KST)))()
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise KisDomesticSignedGetPreflightBlocked(
            "signed-get-production-clock-invalid"
        )
    reader = KisDomesticFunctionalTruthReader(
        client=client,
        cano=cano,
        account_product_code=product,
        trading_date=now.astimezone(KST).date(),
        clock=clock,
        max_stable_read_seconds=120.0,
    )
    return KisDomesticSignedGetPreflightRunner(
        client=client,
        reader=reader,
        authority=authority,
    )


def signed_get_preflight_plan() -> dict[str, Any]:
    return {
        "schemaVersion": "kis-domestic-signed-get-preflight-plan/v1",
        "environment": "KIS_LIVE",
        "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
        "method": "GET_ONLY",
        "routePairs": [
            {"endpoint": endpoint, "trId": tr_id}
            for endpoint, tr_id in [*_BASELINE_ROUTES.values(), _QUOTE_ROUTE]
        ],
        "authenticationTokenIssuanceMayUsePost": True,
        "authenticationOauthPostDispatchCount": 0,
        "authenticationOauthPostAuthOnly": True,
        "tradingPostDeleteDispatchCount": 0,
        "requiresExplicitExecuteFlag": True,
        "requiresEnvironmentGate": EXECUTION_ENV_GATE,
        "networkExecuted": False,
    }


def _default_output_path() -> Path:
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S%z")
    return (
        default_runtime_data_root()
        / "functional_evidence"
        / f"kis-signed-get-preflight-{stamp}.json"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="KIS Live signed official GET-only preflight (never trades)."
    )
    parser.add_argument("--execute-signed-get", action="store_true")
    parser.add_argument("--allow-ephemeral-authority", action="store_true")
    parser.add_argument("--provision-durable-authority", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.provision_durable_authority:
        if args.execute_signed_get or args.allow_ephemeral_authority:
            raise KisDomesticSignedGetPreflightBlocked(
                "authority-provision-mode-not-exclusive"
            )
        status = provision_durable_kis_signed_get_authority()
        print(json.dumps(status, ensure_ascii=False, sort_keys=True))
        return 0
    if not args.execute_signed_get:
        print(json.dumps(signed_get_preflight_plan(), ensure_ascii=False, sort_keys=True))
        return 0
    if os.getenv(EXECUTION_ENV_GATE, "").strip().lower() != "true":
        raise KisDomesticSignedGetPreflightBlocked(
            "signed-get-preflight-environment-gate-disabled"
        )
    authority = load_kis_signed_get_authority(
        allow_ephemeral=args.allow_ephemeral_authority
    )
    runner = build_production_signed_get_preflight_runner(authority=authority)
    evidence = runner.run()
    target = write_redacted_signed_get_evidence(
        evidence,
        args.output or _default_output_path(),
        client=runner.client,
    )
    print(
        json.dumps(
            {
                "ok": True,
                "evidencePath": str(target),
                "evidenceHash": evidence["evidenceHash"],
                "networkOrderPostAllowed": False,
                "tradingPostDeleteDispatchCount": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KisDomesticSignedGetPreflightBlocked as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(2) from None


__all__ = [
    "AUTHORITY_SECRET_NAME",
    "EXECUTION_ENV_GATE",
    "KisDomesticSignedGetPreflightBlocked",
    "KisDomesticSignedGetPreflightRunner",
    "KisSignedGetAuthority",
    "build_production_signed_get_preflight_runner",
    "load_kis_signed_get_authority",
    "main",
    "provision_durable_kis_signed_get_authority",
    "signed_get_preflight_plan",
    "write_redacted_signed_get_evidence",
]
