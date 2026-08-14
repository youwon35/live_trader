from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit

from .functional_test_workspace import kis_account_binding_id
from .operational_governance import stable_sha256


KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
_KIS_AUTH_TOKEN_ENDPOINT = "/oauth2/tokenP"


class KisDomesticFunctionalGetBlocked(RuntimeError):
    """A fail-closed rejection that never includes credentials or account data."""


@dataclass(frozen=True)
class _Route:
    endpoint: str
    tr_id: str
    account_bound: bool


_ROUTES = (
    _Route(
        "/uapi/domestic-stock/v1/trading/inquire-balance",
        "TTTC8434R",
        True,
    ),
    _Route(
        "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl",
        "TTTC0084R",
        True,
    ),
    _Route(
        "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
        "TTTC0081R",
        True,
    ),
    _Route(
        "/uapi/domestic-stock/v1/trading/inquire-period-trade-profit",
        "TTTC8715R",
        True,
    ),
    _Route(
        "/uapi/domestic-stock/v1/trading/inquire-period-profit",
        "TTTC8708R",
        True,
    ),
    _Route(
        "/uapi/domestic-stock/v1/quotations/chk-holiday",
        "CTCA0903R",
        False,
    ),
    _Route(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100",
        False,
    ),
)
_ROUTE_BY_PAIR = {(route.endpoint, route.tr_id): route for route in _ROUTES}
ALLOWED_KIS_DOMESTIC_FUNCTIONAL_GET_PAIRS = frozenset(_ROUTE_BY_PAIR)

_PUBLIC_HEADER_KEYS = frozenset({"custtype", "tr_id", "tr_cont"})
_REQUEST_CONTINUATIONS = frozenset({"", "N"})
_RESPONSE_CONTINUATIONS = frozenset({"", "M", "F", "D", "E"})
_ACCOUNT_QUERY_KEYS = frozenset({"CANO", "ACNT_PRDT_CD"})
_FORBIDDEN_QUERY_KEYS = frozenset(
    {
        "ACCESS_TOKEN",
        "APPKEY",
        "APPSECRET",
        "AUTHORIZATION",
        "HASHKEY",
        "TOKEN",
    }
)
_QUERY_KEY = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^[0-9]{8}$")
_AUTH_ATTESTATION_DOMAIN = b"kis-domestic-functional-authenticated-get/v1\x00"
_CAPTURE_ENVELOPE_DOMAIN = b"kis-domestic-functional-capture/v1\x00"
_PDNO = "010140"

_QUERY_KEYS_BY_TR_ID = {
    "TTTC8434R": frozenset(
        {
            "CANO",
            "ACNT_PRDT_CD",
            "AFHR_FLPR_YN",
            "OFL_YN",
            "INQR_DVSN",
            "UNPR_DVSN",
            "FUND_STTL_ICLD_YN",
            "FNCG_AMT_AUTO_RDPT_YN",
            "PRCS_DVSN",
            "CTX_AREA_FK100",
            "CTX_AREA_NK100",
        }
    ),
    "TTTC0081R": frozenset(
        {
            "CANO",
            "ACNT_PRDT_CD",
            "INQR_STRT_DT",
            "INQR_END_DT",
            "SLL_BUY_DVSN_CD",
            "PDNO",
            "CCLD_DVSN",
            "INQR_DVSN",
            "INQR_DVSN_3",
            "ORD_GNO_BRNO",
            "ODNO",
            "INQR_DVSN_1",
            "EXCG_ID_DVSN_CD",
            "CTX_AREA_FK100",
            "CTX_AREA_NK100",
        }
    ),
    "TTTC0084R": frozenset(
        {
            "CANO",
            "ACNT_PRDT_CD",
            "INQR_DVSN_1",
            "INQR_DVSN_2",
            "CTX_AREA_FK100",
            "CTX_AREA_NK100",
        }
    ),
    "TTTC8715R": frozenset(
        {
            "CANO",
            "ACNT_PRDT_CD",
            "SORT_DVSN",
            "INQR_STRT_DT",
            "INQR_END_DT",
            "CBLC_DVSN",
            "PDNO",
            "CTX_AREA_FK100",
            "CTX_AREA_NK100",
        }
    ),
    "TTTC8708R": frozenset(
        {
            "CANO",
            "ACNT_PRDT_CD",
            "INQR_STRT_DT",
            "INQR_END_DT",
            "SORT_DVSN",
            "INQR_DVSN",
            "CBLC_DVSN",
            "PDNO",
            "CTX_AREA_FK100",
            "CTX_AREA_NK100",
        }
    ),
    "CTCA0903R": frozenset(
        {"BASS_DT", "CTX_AREA_FK", "CTX_AREA_NK"}
    ),
    "FHKST01010100": frozenset(
        {"FID_COND_MRKT_DIV_CODE", "FID_INPUT_ISCD"}
    ),
}

_FIXED_QUERY_VALUES_BY_TR_ID = {
    "TTTC8434R": {
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
    },
    "TTTC0081R": {
        "SLL_BUY_DVSN_CD": "00",
        "PDNO": "",
        "CCLD_DVSN": "00",
        "INQR_DVSN": "00",
        "INQR_DVSN_3": "00",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_1": "",
        "EXCG_ID_DVSN_CD": "ALL",
    },
    "TTTC0084R": {"INQR_DVSN_1": "1", "INQR_DVSN_2": "0"},
    "TTTC8715R": {"SORT_DVSN": "01", "CBLC_DVSN": "00", "PDNO": _PDNO},
    "TTTC8708R": {
        "SORT_DVSN": "01",
        "INQR_DVSN": "00",
        "CBLC_DVSN": "00",
        "PDNO": _PDNO,
    },
    "CTCA0903R": {},
    "FHKST01010100": {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": _PDNO,
    },
}


def kis_domestic_functional_account_fingerprint(
    cano: str,
    account_product_code: str,
) -> str:
    """Derive the same non-secret account binding used by shared permits."""

    normalized_cano = str(cano or "").strip()
    normalized_product = str(account_product_code or "").strip()
    if not re.fullmatch(r"[0-9]{8}", normalized_cano):
        raise KisDomesticFunctionalGetBlocked("kis-functional-cano-invalid")
    if not re.fullmatch(r"[0-9]{2}", normalized_product):
        raise KisDomesticFunctionalGetBlocked(
            "kis-functional-account-product-code-invalid"
        )
    account_id = kis_account_binding_id(normalized_cano, normalized_product)
    if not account_id:
        raise KisDomesticFunctionalGetBlocked(
            "kis-functional-account-binding-unavailable"
        )
    return stable_sha256({"functionalTestAccount": account_id})


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _transport_error_code(exc: BaseException) -> str:
    name = type(exc).__name__
    safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
    return f"kis-functional-get-transport-error:{safe_name}"


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _ = (req, fp, code, msg, headers, newurl)
        return None


def _owned_no_redirect_json_request(
    method: str,
    url: str,
    *,
    body: Mapping[str, Any] | None,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> Mapping[str, Any]:
    """Perform exactly one physical attempt with redirects disabled."""

    if method not in {"GET", "POST"}:
        raise KisDomesticFunctionalGetBlocked(
            "kis-functional-transport-method-forbidden"
        )
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "openapi.koreainvestment.com"
        or parsed.port != 9443
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or parsed.path
        not in {
            *(route.endpoint for route in _ROUTES),
            _KIS_AUTH_TOKEN_ENDPOINT,
        }
        or (method == "POST" and parsed.path != _KIS_AUTH_TOKEN_ENDPOINT)
        or (method == "GET" and parsed.path == _KIS_AUTH_TOKEN_ENDPOINT)
        or (method == "POST" and parsed.query)
    ):
        raise KisDomesticFunctionalGetBlocked(
            "kis-functional-transport-origin-not-exact"
        )
    data = (
        json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        if body is not None
        else None
    )
    request = urllib.request.Request(
        url,
        data=data,
        headers=dict(headers),
        method=method,
    )
    opener = urllib.request.build_opener(_NoRedirectHandler())
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            effective_url = str(response.geturl())
            if not hmac.compare_digest(effective_url, url):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-transport-effective-url-mismatch"
                )
            text = response.read().decode("utf-8", errors="replace")
            return {
                "statusCode": int(response.status),
                "json": json.loads(text) if text else {},
                "trCont": str(response.headers.get("tr_cont") or ""),
                "effectiveUrlExact": True,
                "redirectFollowed": False,
                "physicalAttemptCount": 1,
            }
    except urllib.error.HTTPError as exc:
        if int(exc.code) in {301, 302, 303, 307, 308}:
            exc.close()
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-transport-redirect-forbidden"
            ) from None
        try:
            text = exc.read().decode("utf-8", errors="replace")
        finally:
            exc.close()
        try:
            payload = json.loads(text) if text else {}
        except json.JSONDecodeError:
            payload = {}
        return {
            "statusCode": int(exc.code),
            "json": payload,
            "trCont": str(exc.headers.get("tr_cont") or "") if exc.headers else "",
            "effectiveUrlExact": True,
            "redirectFollowed": False,
            "physicalAttemptCount": 1,
        }
    except KisDomesticFunctionalGetBlocked:
        raise
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise KisDomesticFunctionalGetBlocked(_transport_error_code(exc)) from None
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise KisDomesticFunctionalGetBlocked(_transport_error_code(exc)) from None


@dataclass(frozen=True, repr=False)
class KisDomesticFunctionalPreparedGet:
    """One already-authorized GET request with a deliberately safe repr."""

    method: str
    origin: str
    endpoint: str
    tr_id: str
    url: str
    headers: Mapping[str, str]
    query: Mapping[str, str]
    continuation: str
    account_fingerprint: str
    body: None = None

    def safe_snapshot(self) -> dict[str, Any]:
        return {
            "schemaVersion": "kis-domestic-functional-get-preflight/v1",
            "method": "GET",
            "origin": self.origin,
            "endpoint": self.endpoint,
            "trId": self.tr_id,
            "bodyAbsent": self.body is None,
            "queryKeys": list(self.query),
            "continuation": self.continuation,
            "accountFingerprint": self.account_fingerprint,
            "authenticated": bool(self.headers.get("authorization")),
            "appKeyConfigured": bool(self.headers.get("appkey")),
            "appSecretConfigured": bool(self.headers.get("appsecret")),
        }

    def __repr__(self) -> str:
        return f"KisDomesticFunctionalPreparedGet({self.safe_snapshot()!r})"


GetSender = Callable[[KisDomesticFunctionalPreparedGet], Mapping[str, Any]]


@dataclass(frozen=True, repr=False)
class KisDomesticFunctionalBoundAccessToken:
    access_token: str
    credential_configuration_hash: str

    def __repr__(self) -> str:
        return (
            "KisDomesticFunctionalBoundAccessToken("
            f"credential_configuration_hash={self.credential_configuration_hash!r},"
            "access_token='<redacted>')"
        )


TokenReader = Callable[[], str | KisDomesticFunctionalBoundAccessToken]


def _credential_configuration_hash(
    *,
    app_key: str,
    app_secret: str,
    account_fingerprint: str,
) -> str:
    return stable_sha256(
        {
            "schemaVersion": "kis-domestic-functional-credential-config/v1",
            "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "appKeySha256": hashlib.sha256(app_key.encode("utf-8")).hexdigest(),
            "appSecretSha256": hashlib.sha256(app_secret.encode("utf-8")).hexdigest(),
            "accountFingerprint": account_fingerprint,
        }
    )


def _environment_credential_snapshot() -> dict[str, str]:
    environment = dict(os.environ)
    account = environment.get("KIS_ACCOUNT_NO", "").strip().replace(" ", "")
    product = environment.get("KIS_ACCOUNT_PRODUCT_CODE", "").strip() or "01"
    if "-" in account:
        cano, suffix = account.split("-", 1)
        product = suffix.strip() or product
    elif len(account) > 8:
        cano, embedded_product = account[:8], account[8:10]
        product = embedded_product or product
    else:
        cano = account
    return {
        "origin": environment.get("KIS_BASE_URL", "").strip()
        or KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
        "appKey": environment.get("KIS_APP_KEY", "").strip(),
        "appSecret": environment.get("KIS_APP_SECRET", "").strip(),
        "cano": cano.strip(),
        "accountProductCode": product.strip(),
    }


class _ProductionBoundTokenReader:
    """One exact-origin auth-only token issuer with independently counted POSTs.

    This private callable has no order/cancel surface.  Keeping the token POST
    here lets the GET-only evidence distinguish its exact authentication I/O
    from the invariant trading POST/DELETE count of zero; the shared KIS token
    cache otherwise makes the actual POST count unobservable to this client.
    """

    def __init__(
        self,
        *,
        expected_snapshot: Mapping[str, str],
        credential_configuration_hash: str,
    ) -> None:
        self._sealed_snapshot = dict(expected_snapshot)
        self._credential_configuration_hash = credential_configuration_hash
        self._lock = threading.Lock()
        self._access_token = ""
        self._expires_at_monotonic = 0.0
        self._oauth_post_dispatches = 0

    def __repr__(self) -> str:
        return (
            "_ProductionBoundTokenReader("
            "endpoint='/oauth2/tokenP',credentials='<redacted>')"
        )

    def __call__(self) -> KisDomesticFunctionalBoundAccessToken:
        with self._lock:
            if _environment_credential_snapshot() != self._sealed_snapshot:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-credential-snapshot-changed"
                )
            now = float(time.monotonic())
            if not math.isfinite(now) or now < 0:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-clock-invalid"
                )
            if self._access_token and self._expires_at_monotonic > now:
                return KisDomesticFunctionalBoundAccessToken(
                    access_token=self._access_token,
                    credential_configuration_hash=(
                        self._credential_configuration_hash
                    ),
                )
            # This diagnostic reader owns a strict one-shot authentication
            # budget.  A short-but-valid token lifetime must fail closed before
            # a second physical /oauth2/tokenP request can leave the process.
            if self._oauth_post_dispatches >= 1:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-oauth-post-one-shot-exhausted"
                )
            self._oauth_post_dispatches += 1
            try:
                response = _owned_no_redirect_json_request(
                    "POST",
                    KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN
                    + _KIS_AUTH_TOKEN_ENDPOINT,
                    body={
                        "grant_type": "client_credentials",
                        "appkey": self._sealed_snapshot["appKey"],
                        "appsecret": self._sealed_snapshot["appSecret"],
                    },
                    headers={"content-type": "application/json; charset=utf-8"},
                    timeout_seconds=10.0,
                )
            except KisDomesticFunctionalGetBlocked:
                raise
            except BaseException as exc:
                raise KisDomesticFunctionalGetBlocked(
                    _transport_error_code(exc)
                ) from None
            if _environment_credential_snapshot() != self._sealed_snapshot:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-credential-snapshot-changed"
                )
            if not isinstance(response, Mapping):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-not-object"
                )
            if (
                response.get("effectiveUrlExact") is not True
                or response.get("redirectFollowed") is not False
                or response.get("physicalAttemptCount") != 1
            ):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-transport-provenance-invalid"
                )
            status_code = response.get("statusCode")
            if type(status_code) is not int or not 200 <= status_code < 300:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-status-invalid"
                )
            payload = response.get("json")
            if not isinstance(payload, Mapping):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-body-invalid"
                )
            access_token = payload.get("access_token")
            if type(access_token) is not str or not access_token.strip():
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-token-missing"
                )
            try:
                expires_in = float(payload.get("expires_in") or 86400.0)
            except (TypeError, ValueError, OverflowError):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-expiry-invalid"
                ) from None
            if not math.isfinite(expires_in) or expires_in <= 60:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-response-expiry-invalid"
                )
            self._access_token = access_token.strip()
            self._expires_at_monotonic = now + expires_in - 60.0
            return KisDomesticFunctionalBoundAccessToken(
                access_token=self._access_token,
                credential_configuration_hash=self._credential_configuration_hash,
            )

    def oauth_post_dispatch_count(self) -> int:
        with self._lock:
            return self._oauth_post_dispatches


def _default_sender(
    request: KisDomesticFunctionalPreparedGet,
) -> Mapping[str, Any]:
    return _owned_no_redirect_json_request(
        "GET",
        request.url,
        body=None,
        headers=dict(request.headers),
        timeout_seconds=10.0,
    )


class KisDomesticFunctionalGetClient:
    """Exact-origin, exact-route KIS Live reader with no mutation API.

    The only network-capable public method is ``get`` and it always constructs a
    body-less GET. ``preflight`` accepts method/body arguments solely so callers
    and tests can prove that an attempted broader HTTP surface is rejected
    before token acquisition or transport dispatch.
    """

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        cano: str,
        account_product_code: str,
        account_fingerprint: str,
        server_authority_key: bytes,
        server_authority_key_id: str = "",
        server_authority_restart_verifiable: bool = False,
        token_reader: TokenReader | None = None,
        sender: GetSender | None = None,
        allow_mock_transport: bool = False,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        min_request_interval_seconds: float = 2.1,
    ) -> None:
        normalized_app_key = str(app_key or "").strip()
        normalized_app_secret = str(app_secret or "").strip()
        normalized_cano = str(cano or "").strip()
        normalized_product = str(account_product_code or "").strip()
        normalized_fingerprint = str(account_fingerprint or "").strip().lower()
        if not normalized_app_key:
            raise KisDomesticFunctionalGetBlocked("kis-functional-app-key-missing")
        if not normalized_app_secret:
            raise KisDomesticFunctionalGetBlocked("kis-functional-app-secret-missing")
        if not _SHA256.fullmatch(normalized_fingerprint):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-account-fingerprint-invalid"
            )
        expected = kis_domestic_functional_account_fingerprint(
            normalized_cano,
            normalized_product,
        )
        if not hmac.compare_digest(normalized_fingerprint, expected):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-account-fingerprint-mismatch"
            )
        if not isinstance(server_authority_key, bytes) or len(server_authority_key) < 32:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-server-authority-key-invalid"
            )
        normalized_authority_key_id = str(server_authority_key_id or "").strip()
        if normalized_authority_key_id and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}",
            normalized_authority_key_id,
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-server-authority-key-id-invalid"
            )
        if type(server_authority_restart_verifiable) is not bool:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-server-authority-restart-state-invalid"
            )
        if server_authority_restart_verifiable and not normalized_authority_key_id:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-durable-server-authority-key-id-required"
            )
        if type(allow_mock_transport) is not bool:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-mock-transport-flag-invalid"
            )
        if token_reader is not None and not callable(token_reader):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-token-reader-invalid"
            )
        if sender is not None and not callable(sender):
            raise KisDomesticFunctionalGetBlocked("kis-functional-sender-invalid")
        if (token_reader is not None or sender is not None) and not allow_mock_transport:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-custom-transport-production-forbidden"
            )
        if not isinstance(min_request_interval_seconds, (int, float)) or isinstance(
            min_request_interval_seconds, bool
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-request-interval-invalid"
            )
        interval = float(min_request_interval_seconds)
        if (
            not math.isfinite(interval)
            or interval < 0
            or interval > 30
            or (not allow_mock_transport and interval < 2.0)
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-request-interval-invalid"
            )
        if monotonic_clock is not None and not callable(monotonic_clock):
            raise KisDomesticFunctionalGetBlocked("kis-functional-clock-invalid")
        if sleeper is not None and not callable(sleeper):
            raise KisDomesticFunctionalGetBlocked("kis-functional-sleeper-invalid")
        if (monotonic_clock is not None or sleeper is not None) and not allow_mock_transport:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-custom-timing-production-forbidden"
            )
        self._app_key = normalized_app_key
        self._app_secret = normalized_app_secret
        self._cano = normalized_cano
        self._account_product_code = normalized_product
        self._account_fingerprint = normalized_fingerprint
        self._server_authority_key = bytes(server_authority_key)
        self._server_authority_key_id_hash = (
            hashlib.sha256(normalized_authority_key_id.encode("utf-8")).hexdigest()
            if normalized_authority_key_id
            else ""
        )
        self._server_authority_restart_verifiable = (
            server_authority_restart_verifiable
        )
        self._credential_configuration_hash = _credential_configuration_hash(
            app_key=normalized_app_key,
            app_secret=normalized_app_secret,
            account_fingerprint=normalized_fingerprint,
        )
        if token_reader is None:
            snapshot = _environment_credential_snapshot()
            expected_snapshot = {
                "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
                "appKey": normalized_app_key,
                "appSecret": normalized_app_secret,
                "cano": normalized_cano,
                "accountProductCode": normalized_product,
            }
            if snapshot != expected_snapshot:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-credential-snapshot-mismatch"
                )
            production_token_reader = _ProductionBoundTokenReader(
                expected_snapshot=expected_snapshot,
                credential_configuration_hash=self._credential_configuration_hash,
            )
            self._token_reader = production_token_reader
            self._production_token_reader: _ProductionBoundTokenReader | None = (
                production_token_reader
            )
        else:
            self._token_reader = token_reader
            self._production_token_reader = None
        self._sender = sender or _default_sender
        self._production_transport = sender is None
        self._allow_mock_transport = allow_mock_transport
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._min_request_interval_seconds = interval
        self._dispatch_lock = threading.Lock()
        self._last_dispatch_at: float | None = None
        self._token_reads = 0
        self._get_dispatches = 0
        self._pacing_wait_seconds = 0.0
        self._dispatch_records: list[dict[str, Any]] = []

    @classmethod
    def from_environment(
        cls,
        *,
        expected_account_fingerprint: str,
        server_authority_key: bytes,
        server_authority_key_id: str = "",
        server_authority_restart_verifiable: bool = False,
    ) -> "KisDomesticFunctionalGetClient":
        snapshot = _environment_credential_snapshot()
        if snapshot["origin"] != KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-environment-origin-not-exact"
            )
        return cls(
            app_key=snapshot["appKey"],
            app_secret=snapshot["appSecret"],
            cano=snapshot["cano"],
            account_product_code=snapshot["accountProductCode"],
            account_fingerprint=expected_account_fingerprint,
            server_authority_key=server_authority_key,
            server_authority_key_id=server_authority_key_id,
            server_authority_restart_verifiable=(
                server_authority_restart_verifiable
            ),
        )

    @property
    def account_fingerprint(self) -> str:
        return self._account_fingerprint

    @property
    def credential_configuration_hash(self) -> str:
        return self._credential_configuration_hash

    def __repr__(self) -> str:
        return (
            "KisDomesticFunctionalGetClient("
            f"origin={KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN!r},"
            f"account_fingerprint={self._account_fingerprint!r},"
            "methods=('GET',),credentialed=True)"
        )

    def _authority_signature(self, domain: bytes, value: Any) -> str:
        return hmac.new(
            self._server_authority_key,
            domain + _canonical_bytes(value),
            hashlib.sha256,
        ).hexdigest()

    def authenticated_attestation(self) -> dict[str, Any]:
        body = {
            "schemaVersion": "kis-authenticated-get-attestation/v1",
            "environment": "KIS_LIVE",
            "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            "custtype": "P",
            "accountFingerprint": self._account_fingerprint,
            "credentialConfigurationHash": self._credential_configuration_hash,
            "authenticated": True,
            "allowedMethods": ["GET"],
        }
        signature = self._authority_signature(_AUTH_ATTESTATION_DOMAIN, body)
        return {**body, "signatureHash": signature}

    def verify_authenticated_attestation(self, candidate: object) -> bool:
        if not isinstance(candidate, Mapping):
            return False
        expected = self.authenticated_attestation()
        if set(candidate) != set(expected):
            return False
        for key in expected:
            actual = candidate.get(key)
            wanted = expected[key]
            if type(actual) is not type(wanted):
                return False
            if isinstance(wanted, str):
                if not hmac.compare_digest(actual, wanted):
                    return False
            elif actual != wanted:
                return False
        return True

    def sign_capture_envelope(self, value: Mapping[str, Any]) -> str:
        if not isinstance(value, Mapping):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-capture-envelope-not-object"
            )
        try:
            return self._authority_signature(_CAPTURE_ENVELOPE_DOMAIN, value)
        except (TypeError, ValueError, OverflowError):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-capture-envelope-not-json"
            ) from None

    def verify_capture_envelope(
        self,
        value: Mapping[str, Any],
        signature: object,
    ) -> bool:
        if type(signature) is not str or not _SHA256.fullmatch(signature):
            return False
        try:
            expected = self.sign_capture_envelope(value)
        except KisDomesticFunctionalGetBlocked:
            return False
        return hmac.compare_digest(signature, expected)

    def _pace_and_record_dispatch(
        self,
        request: KisDomesticFunctionalPreparedGet,
    ) -> int:
        with self._dispatch_lock:
            now = float(self._monotonic_clock())
            if not math.isfinite(now) or now < 0:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-monotonic-clock-invalid"
                )
            wait_seconds = 0.0
            if self._last_dispatch_at is not None:
                wait_seconds = max(
                    0.0,
                    self._last_dispatch_at
                    + self._min_request_interval_seconds
                    - now,
                )
            if wait_seconds:
                self._sleeper(wait_seconds)
                after_sleep = float(self._monotonic_clock())
                if (
                    not math.isfinite(after_sleep)
                    or after_sleep + 1e-9 < now + wait_seconds
                ):
                    raise KisDomesticFunctionalGetBlocked(
                        "kis-functional-pacing-clock-did-not-advance"
                    )
                now = after_sleep
            self._last_dispatch_at = now
            self._get_dispatches += 1
            self._pacing_wait_seconds += wait_seconds
            query_hmac = self._authority_signature(
                _CAPTURE_ENVELOPE_DOMAIN,
                {
                    "endpoint": request.endpoint,
                    "trId": request.tr_id,
                    "queryItems": list(request.query.items()),
                    "continuation": request.continuation,
                    "accountFingerprint": self._account_fingerprint,
                },
            )
            self._dispatch_records.append(
                {
                    "ordinal": self._get_dispatches,
                    "monotonicStartedAt": now,
                    "endpoint": request.endpoint,
                    "trId": request.tr_id,
                    "continuation": request.continuation,
                    "accountFingerprint": self._account_fingerprint,
                    "queryHmacSha256": query_hmac,
                    "method": "GET",
                    "bodyAbsent": True,
                    "physicalAttemptCount": (
                        1 if self._production_transport else 0
                    ),
                    "physicalAttemptCountComplete": self._production_transport,
                    "effectiveUrlExact": (
                        True if self._production_transport else None
                    ),
                    "redirectFollowed": (
                        False if self._production_transport else None
                    ),
                    "transportOutcome": "PENDING",
                }
            )
            return self._get_dispatches

    def _record_transport_outcome(
        self,
        ordinal: int,
        *,
        outcome: str,
        status_code: int | None = None,
    ) -> None:
        with self._dispatch_lock:
            if ordinal <= 0 or ordinal > len(self._dispatch_records):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-dispatch-audit-ordinal-invalid"
                )
            record = self._dispatch_records[ordinal - 1]
            if record.get("ordinal") != ordinal:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-dispatch-audit-order-invalid"
                )
            record["transportOutcome"] = outcome
            if status_code is not None:
                record["statusCode"] = status_code

    def audit_snapshot(self) -> dict[str, Any]:
        with self._dispatch_lock:
            oauth_post_dispatches = (
                self._production_token_reader.oauth_post_dispatch_count()
                if self._production_token_reader is not None
                else 0
            )
            body = {
                "schemaVersion": "kis-domestic-functional-get-audit/v1",
                "origin": KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
                "accountFingerprint": self._account_fingerprint,
                "credentialConfigurationHash": self._credential_configuration_hash,
                "serverAuthorityKeyIdHash": self._server_authority_key_id_hash,
                "serverAuthorityRestartVerifiable": (
                    self._server_authority_restart_verifiable
                ),
                "authenticationTokenReadCount": self._token_reads,
                "oauthTokenIssuanceMayUsePost": True,
                "authenticationOauthPostDispatchCount": (
                    oauth_post_dispatches
                ),
                "authenticationOauthPostCountComplete": (
                    self._production_token_reader is not None
                ),
                "authenticationOauthPostAuthOnly": True,
                "authenticationOauthHiddenRetryCount": 0,
                "authenticationOauthRedirectFollowCount": 0,
                "officialGetDispatchCount": self._get_dispatches,
                "physicalOfficialGetAttemptCount": (
                    self._get_dispatches if self._production_transport else 0
                ),
                "physicalOfficialGetAttemptCountComplete": (
                    self._production_transport
                ),
                "hiddenGetRetryCount": 0,
                "redirectFollowCount": 0,
                "tradingPostDeleteDispatchCount": 0,
                "minimumRequestIntervalSeconds": self._min_request_interval_seconds,
                "pacingWaitSeconds": self._pacing_wait_seconds,
                "dispatches": deepcopy(self._dispatch_records),
            }
        return {**body, "signatureHash": self.sign_capture_envelope(body)}

    def _prepare(
        self,
        *,
        method: str,
        origin: str,
        endpoint: str,
        tr_id: str,
        query: Mapping[str, str],
        continuation: str,
        public_headers: Mapping[str, str],
        body: object,
        access_token: str | None,
    ) -> KisDomesticFunctionalPreparedGet:
        if type(method) is not str or method != "GET":
            raise KisDomesticFunctionalGetBlocked("kis-functional-method-not-get")
        if body is not None:
            raise KisDomesticFunctionalGetBlocked("kis-functional-get-body-forbidden")
        if type(origin) is not str or origin != KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN:
            raise KisDomesticFunctionalGetBlocked("kis-functional-live-origin-not-exact")
        if type(endpoint) is not str or type(tr_id) is not str:
            raise KisDomesticFunctionalGetBlocked("kis-functional-route-malformed")
        route = _ROUTE_BY_PAIR.get((endpoint, tr_id))
        if route is None:
            raise KisDomesticFunctionalGetBlocked("kis-functional-get-route-not-allowed")
        if not isinstance(query, Mapping):
            raise KisDomesticFunctionalGetBlocked("kis-functional-query-not-object")
        normalized_query: dict[str, str] = {}
        for key, value in query.items():
            if type(key) is not str or not _QUERY_KEY.fullmatch(key):
                raise KisDomesticFunctionalGetBlocked("kis-functional-query-key-invalid")
            if key in _FORBIDDEN_QUERY_KEYS:
                raise KisDomesticFunctionalGetBlocked("kis-functional-query-secret-forbidden")
            if type(value) is not str:
                raise KisDomesticFunctionalGetBlocked("kis-functional-query-value-invalid")
            normalized_query[key] = value
        expected_query_keys = _QUERY_KEYS_BY_TR_ID[tr_id]
        if set(normalized_query) != expected_query_keys:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-query-fields-not-exact"
            )
        for key, expected_value in _FIXED_QUERY_VALUES_BY_TR_ID[tr_id].items():
            if normalized_query.get(key) != expected_value:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-query-fixed-value-mismatch"
                )
        for key in ("INQR_STRT_DT", "INQR_END_DT", "BASS_DT"):
            if key in normalized_query and not _DATE.fullmatch(normalized_query[key]):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-query-date-invalid"
                )
        if "INQR_STRT_DT" in normalized_query and (
            normalized_query["INQR_STRT_DT"] != normalized_query["INQR_END_DT"]
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-query-date-window-not-exact"
            )
        account_keys = _ACCOUNT_QUERY_KEYS.intersection(normalized_query)
        if route.account_bound:
            if account_keys != _ACCOUNT_QUERY_KEYS:
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-account-query-incomplete"
                )
            if not hmac.compare_digest(normalized_query["CANO"], self._cano):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-account-query-mismatch"
                )
            if not hmac.compare_digest(
                normalized_query["ACNT_PRDT_CD"],
                self._account_product_code,
            ):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-account-query-mismatch"
                )
        elif account_keys:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-public-query-account-forbidden"
            )
        if type(continuation) is not str or continuation not in _REQUEST_CONTINUATIONS:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-request-continuation-invalid"
            )
        cursor_suffix = "" if tr_id in {"CTCA0903R"} else "100"
        cursor_fk = f"CTX_AREA_FK{cursor_suffix}"
        cursor_nk = f"CTX_AREA_NK{cursor_suffix}"
        if cursor_fk in normalized_query:
            fk_value = normalized_query[cursor_fk]
            nk_value = normalized_query[cursor_nk]
            if continuation == "" and (fk_value or nk_value):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-initial-cursor-not-empty"
                )
            if continuation == "N" and not (fk_value or nk_value):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-next-cursor-missing"
                )
        elif continuation != "":
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-nonpaged-continuation-forbidden"
            )
        if not isinstance(public_headers, Mapping):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-public-headers-not-object"
            )
        if set(public_headers) != _PUBLIC_HEADER_KEYS or any(
            type(key) is not str or type(value) is not str
            for key, value in public_headers.items()
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-public-headers-not-exact"
            )
        if (
            public_headers.get("custtype") != "P"
            or public_headers.get("tr_id") != tr_id
            or public_headers.get("tr_cont") != continuation
        ):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-public-headers-mismatch"
            )
        if access_token is not None and (
            type(access_token) is not str or not access_token.strip()
        ):
            raise KisDomesticFunctionalGetBlocked("kis-functional-access-token-missing")
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": self._app_key,
            "appsecret": self._app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "tr_cont": continuation,
        }
        if access_token is not None:
            headers["authorization"] = f"Bearer {access_token.strip()}"
        encoded_query = urlencode(list(normalized_query.items()), doseq=False)
        url = KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN + endpoint
        if encoded_query:
            url += "?" + encoded_query
        return KisDomesticFunctionalPreparedGet(
            method="GET",
            origin=KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN,
            endpoint=endpoint,
            tr_id=tr_id,
            url=url,
            headers=headers,
            query=normalized_query,
            continuation=continuation,
            account_fingerprint=self._account_fingerprint,
            body=None,
        )

    def preflight(
        self,
        *,
        method: str,
        origin: str,
        endpoint: str,
        tr_id: str,
        query: Mapping[str, str],
        continuation: str,
        public_headers: Mapping[str, str],
        body: object = None,
    ) -> Mapping[str, Any]:
        request = self._prepare(
            method=method,
            origin=origin,
            endpoint=endpoint,
            tr_id=tr_id,
            query=query,
            continuation=continuation,
            public_headers=public_headers,
            body=body,
            access_token=None,
        )
        return request.safe_snapshot()

    def get(
        self,
        *,
        origin: str,
        endpoint: str,
        tr_id: str,
        query: Mapping[str, str],
        continuation: str,
        public_headers: Mapping[str, str],
    ) -> Mapping[str, Any]:
        # Every non-network boundary is checked before credentials/token access.
        self.preflight(
            method="GET",
            origin=origin,
            endpoint=endpoint,
            tr_id=tr_id,
            query=query,
            continuation=continuation,
            public_headers=public_headers,
            body=None,
        )
        try:
            token = self._token_reader()
        except KisDomesticFunctionalGetBlocked:
            raise
        except BaseException as exc:
            raise KisDomesticFunctionalGetBlocked(_transport_error_code(exc)) from None
        self._token_reads += 1
        if isinstance(token, KisDomesticFunctionalBoundAccessToken):
            if not hmac.compare_digest(
                token.credential_configuration_hash,
                self._credential_configuration_hash,
            ):
                raise KisDomesticFunctionalGetBlocked(
                    "kis-functional-token-credential-binding-mismatch"
                )
            access_token = token.access_token
        elif self._allow_mock_transport and type(token) is str:
            access_token = token
        else:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-token-not-credential-bound"
            )
        request = self._prepare(
            method="GET",
            origin=origin,
            endpoint=endpoint,
            tr_id=tr_id,
            query=query,
            continuation=continuation,
            public_headers=public_headers,
            body=None,
            access_token=access_token,
        )
        ordinal = self._pace_and_record_dispatch(request)
        try:
            response = self._sender(request)
        except KisDomesticFunctionalGetBlocked:
            self._record_transport_outcome(
                ordinal,
                outcome="BLOCKED_TRANSPORT",
            )
            raise
        except BaseException as exc:
            self._record_transport_outcome(
                ordinal,
                outcome=f"ERROR:{type(exc).__name__}",
            )
            raise KisDomesticFunctionalGetBlocked(_transport_error_code(exc)) from None
        if not isinstance(response, Mapping):
            self._record_transport_outcome(ordinal, outcome="INVALID_RESPONSE")
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-get-response-not-object"
            )
        if self._production_transport and (
            response.get("effectiveUrlExact") is not True
            or response.get("redirectFollowed") is not False
            or response.get("physicalAttemptCount") != 1
        ):
            self._record_transport_outcome(
                ordinal,
                outcome="INVALID_TRANSPORT_PROVENANCE",
            )
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-get-transport-provenance-invalid"
            )
        status_code = response.get("statusCode")
        if type(status_code) is not int or not 0 <= status_code <= 599:
            self._record_transport_outcome(ordinal, outcome="INVALID_STATUS")
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-get-status-code-invalid"
            )
        self._record_transport_outcome(
            ordinal,
            outcome="RESPONSE",
            status_code=status_code,
        )
        tr_cont = response.get("trCont")
        if type(tr_cont) is not str or tr_cont not in _RESPONSE_CONTINUATIONS:
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-response-continuation-invalid"
            )
        body = response.get("json") if "json" in response else response.get("body")
        if not isinstance(body, Mapping):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-get-response-body-not-object"
            )
        try:
            copied_body = deepcopy(dict(body))
            _canonical_bytes(copied_body)
        except (TypeError, ValueError, OverflowError):
            raise KisDomesticFunctionalGetBlocked(
                "kis-functional-get-response-body-not-json"
            ) from None
        return {
            "statusCode": status_code,
            "trCont": tr_cont,
            "body": copied_body,
        }


__all__ = [
    "ALLOWED_KIS_DOMESTIC_FUNCTIONAL_GET_PAIRS",
    "KIS_DOMESTIC_FUNCTIONAL_LIVE_ORIGIN",
    "KisDomesticFunctionalGetBlocked",
    "KisDomesticFunctionalBoundAccessToken",
    "KisDomesticFunctionalGetClient",
    "KisDomesticFunctionalPreparedGet",
    "kis_domestic_functional_account_fingerprint",
]
