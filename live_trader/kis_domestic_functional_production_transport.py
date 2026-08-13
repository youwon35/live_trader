from __future__ import annotations

"""Owned, single-attempt KIS trading HTTP transport (compile-disabled).

This module deliberately cannot be reached by the current production graph.
Mock-socket tests exercise the exact request and response boundary without
opening a network connection.  A future graph may receive only the bound gate
adapter; the raw transport requires a per-factory authenticated one-shot lease.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import threading
import time
from typing import Any, Callable, Mapping
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from .kis_domestic_functional_get_client import (
    _credential_configuration_hash as _get_credential_configuration_hash,
    kis_domestic_functional_account_fingerprint,
)


KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT_NETWORK_COMPILED = False
KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT_RELEASE_AVAILABLE = False

ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
LIVE_ORIGIN = "https://openapi.koreainvestment.com:9443"
_NETWORK_ENV = "LIVE_TRADER_KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT"
_NETWORK_ENV_VALUE = "EXPLICITLY_ENABLED_AFTER_REVIEW"
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_ACCOUNT = re.compile(r"^[0-9]{8}$", flags=re.ASCII)
_PRODUCT = re.compile(r"^[0-9]{2}$", flags=re.ASCII)
_HEADER_SECRET = re.compile(r"^[A-Za-z0-9._~+/=-]{8,8192}$", flags=re.ASCII)
_MAX_RESPONSE_BYTES = 1_048_576
_BINDING_SCHEMA = "kis-domestic-functional-production-binding/v1"
_ENVIRONMENT_SCHEMA = "kis-domestic-functional-credential-environment/v1"

_OPERATIONS = {
    "NATURAL_BUY": (
        "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0012U", "BUY", False,
    ),
    "CLEANUP_SELL": (
        "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0011U", "SELL", True,
    ),
    "CLEANUP_CANCEL": (
        "/uapi/domestic-stock/v1/trading/order-rvsecncl", "TTTC0013U", "CANCEL", True,
    ),
}
_REQUEST_KEYS = {
    "schemaVersion", "route", "pdno", "origin", "method", "endpoint",
    "trId", "query", "headers", "operation", "side", "cleanupOnly",
    "claimId", "sessionId", "authorityRevision", "accountFingerprint",
    "credentialConfigurationHash", "payload", "payloadHash",
}
_FORBIDDEN_PAYLOAD_KEYS = {
    "CANO", "ACNT_PRDT_CD", "authorization", "appkey", "appsecret",
}


class KisDomesticFunctionalProductionTransportBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _callable_code_hash(value: Callable[..., Any], label: str) -> str:
    """Return a redacted, deterministic identity for a pinned callback body."""

    target = getattr(value, "__func__", value)
    code = getattr(target, "__code__", None)
    if code is None:
        raise KisDomesticFunctionalProductionTransportBlocked(
            f"{label} code identity is unavailable"
        )
    return _hash({
        "schemaVersion": "kis-domestic-functional-callback-code/v1",
        "module": str(getattr(target, "__module__", "")),
        "qualname": str(getattr(target, "__qualname__", "")),
        "bytecodeHash": hashlib.sha256(code.co_code).hexdigest(),
        "constantsHash": hashlib.sha256(repr(code.co_consts).encode()).hexdigest(),
        "namesHash": hashlib.sha256(repr(code.co_names).encode()).hexdigest(),
    })


def kis_domestic_functional_callback_code_hash(
    value: Callable[..., Any],
) -> str:
    """Pure helper used by the state-owned caller to pin callback code."""

    return _callable_code_hash(value, "callback")


def _exact_sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalProductionTransportBlocked(f"{label} is invalid")
    return value


def _exact_id(value: Any, label: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise KisDomesticFunctionalProductionTransportBlocked(f"{label} is invalid")
    return value


def _safe_json_object(raw: bytes) -> dict[str, Any]:
    if type(raw) is not bytes or len(raw) > _MAX_RESPONSE_BYTES:
        raise KisDomesticFunctionalProductionTransportBlocked(
            "KIS response size is invalid"
        )

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if type(key) is not str or key in result:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "KIS response JSON is not exact"
                )
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs) if raw else {}
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise KisDomesticFunctionalProductionTransportBlocked(
            "KIS response JSON is invalid"
        ) from None
    if type(value) is not dict:
        raise KisDomesticFunctionalProductionTransportBlocked(
            "KIS response JSON object is required"
        )
    return value


@dataclass(frozen=True, repr=False)
class _PreparedTradingRequest:
    method: str
    url: str
    endpoint: str
    tr_id: str
    headers: Mapping[str, str]
    body: bytes
    timeout_seconds: float


@dataclass(frozen=True, repr=False)
class _GateLease:
    factory_id: str
    gate_owner_hash: str
    gate_code_hash: str
    request_hash: str
    sequence: int
    nonce: str
    signature: str


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        _ = (request, fp, code, msg, headers, newurl)
        return None


def _owned_urllib_single_attempt(request: _PreparedTradingRequest) -> Mapping[str, Any]:
    """Exactly one urllib opener call; redirects are rejected, never followed."""

    if type(request) is not _PreparedTradingRequest:
        raise KisDomesticFunctionalProductionTransportBlocked(
            "owned transport request type is invalid"
        )
    http_request = urllib.request.Request(
        request.url, data=request.body, headers=dict(request.headers), method="POST"
    )
    opener = urllib.request.build_opener(_NoRedirectHandler())
    try:
        with opener.open(http_request, timeout=request.timeout_seconds) as response:
            effective_url = str(response.geturl())
            body = response.read(_MAX_RESPONSE_BYTES + 1)
            return {
                "statusCode": int(response.status), "effectiveUrl": effective_url,
                "responseHeaders": {
                    "content-type": str(response.headers.get("content-type") or ""),
                },
                "bodyBytes": body, "redirectFollowed": False,
            }
    except urllib.error.HTTPError as exc:
        if int(exc.code) in {301, 302, 303, 307, 308}:
            exc.close()
            raise KisDomesticFunctionalProductionTransportBlocked(
                "KIS trading redirect is forbidden"
            ) from None
        try:
            body = exc.read(_MAX_RESPONSE_BYTES + 1)
        finally:
            exc.close()
        return {
            "statusCode": int(exc.code), "effectiveUrl": request.url,
            "responseHeaders": {
                "content-type": str(exc.headers.get("content-type") or "")
                if exc.headers else "",
            },
            "bodyBytes": body, "redirectFollowed": False,
        }
    except KisDomesticFunctionalProductionTransportBlocked:
        raise
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        name = type(exc).__name__
        safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
        raise KisDomesticFunctionalProductionTransportBlocked(
            f"KIS trading physical attempt failed:{safe_name}"
        ) from None


class BoundKisDomesticFunctionalTransportGate:
    """The only public call surface; it mints a private one-shot gate lease."""

    def __init__(self, *, token: object, transport: "DisabledKisDomesticFunctionalProductionTransport",
                 gate_owner_id: str, gate_code_hash: str) -> None:
        if token is not transport._factory_token:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "private transport factory token mismatch"
            )
        self._transport = transport
        self._owner_hash = hashlib.sha256(
            _exact_id(gate_owner_id, "gate owner id").encode()
        ).hexdigest()
        self._code_hash = _exact_sha(gate_code_hash, "gate code hash")

    def __repr__(self) -> str:
        return "BoundKisDomesticFunctionalTransportGate(productionAvailable=False)"

    def send(self, request: Mapping[str, Any], *, gate_call_token: object) -> Mapping[str, Any]:
        if gate_call_token is not self._transport._gate_call_token:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "private transport gate caller token mismatch"
            )
        request_hash = _hash(request)
        lease = self._transport._mint_lease(
            gate_owner_hash=self._owner_hash, gate_code_hash=self._code_hash,
            request_hash=request_hash,
        )
        return self._transport._dispatch(request=request, lease=lease)


class DisabledKisDomesticFunctionalProductionTransport:
    def __init__(
        self, *,
        token_reader: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        token_attestation_verifier: Callable[[Mapping[str, Any]], bool],
        token_verifier_key_id_hash: str,
        auth_header_builder: Callable[[Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]],
        auth_attestation_verifier: Callable[[Mapping[str, Any]], bool],
        auth_verifier_key_id_hash: str,
        credential_environment_reader: Callable[[], Mapping[str, Any]],
        credential_environment_verifier: Callable[[Mapping[str, Any]], bool],
        credential_environment_key_id_hash: str,
        credential_configuration_hash: str,
        gate_owner_id: str,
        gate_code_hash: str,
        timeout_seconds: float = 5.0,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        wall_clock: Callable[[], float] = time.time,
        mock_socket: Callable[[_PreparedTradingRequest], Mapping[str, Any]] | None = None,
        allow_mock_socket: bool = False,
    ) -> None:
        for value, label in (
            (token_reader, "token reader"),
            (token_attestation_verifier, "token verifier"),
            (auth_header_builder, "authorization header builder"),
            (auth_attestation_verifier, "authorization verifier"),
            (credential_environment_reader, "credential environment reader"),
            (credential_environment_verifier, "credential environment verifier"),
            (monotonic_ns, "monotonic clock"), (wall_clock, "wall clock"),
        ):
            if not callable(value):
                raise KisDomesticFunctionalProductionTransportBlocked(
                    f"{label} is invalid"
                )
        if type(timeout_seconds) not in {int, float} or not math.isfinite(float(timeout_seconds)) or not 0 < float(timeout_seconds) <= 30:
            raise KisDomesticFunctionalProductionTransportBlocked("timeout is invalid")
        if type(allow_mock_socket) is not bool:
            raise KisDomesticFunctionalProductionTransportBlocked("mock socket flag is invalid")
        if allow_mock_socket:
            if not callable(mock_socket):
                raise KisDomesticFunctionalProductionTransportBlocked("mock socket is required")
            executor = mock_socket
        elif mock_socket is not None:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "mock socket requires explicit test authorization"
            )
        else:
            executor = _owned_urllib_single_attempt
        self._token_reader = token_reader
        self._token_verifier = token_attestation_verifier
        self._token_verifier_key_id_hash = _exact_sha(
            token_verifier_key_id_hash, "token verifier key id hash"
        )
        self._header_builder = auth_header_builder
        self._auth_verifier = auth_attestation_verifier
        self._auth_verifier_key_id_hash = _exact_sha(
            auth_verifier_key_id_hash, "authorization verifier key id hash"
        )
        self._credential_environment_reader = credential_environment_reader
        self._credential_environment_verifier = credential_environment_verifier
        self._credential_environment_key_id_hash = _exact_sha(
            credential_environment_key_id_hash,
            "credential environment verifier key id hash",
        )
        self._credential_configuration_hash = _exact_sha(
            credential_configuration_hash, "credential configuration hash"
        )
        self._callback_code_hashes = {
            "tokenReaderCodeHash": _callable_code_hash(
                token_reader, "token reader"
            ),
            "tokenVerifierCodeHash": _callable_code_hash(
                token_attestation_verifier, "token verifier"
            ),
            "authorizationBuilderCodeHash": _callable_code_hash(
                auth_header_builder, "authorization builder"
            ),
            "authorizationVerifierCodeHash": _callable_code_hash(
                auth_attestation_verifier, "authorization verifier"
            ),
            "credentialEnvironmentReaderCodeHash": _callable_code_hash(
                credential_environment_reader, "credential environment reader"
            ),
            "credentialEnvironmentVerifierCodeHash": _callable_code_hash(
                credential_environment_verifier,
                "credential environment verifier",
            ),
        }
        self._timeout = float(timeout_seconds)
        self._monotonic_ns = monotonic_ns
        self._wall_clock = wall_clock
        self._executor = executor
        self._mock = allow_mock_socket
        self._factory_token = object()
        self._gate_call_token = object()
        self._factory_id = secrets.token_hex(16)
        self._lease_key = secrets.token_bytes(32)
        self._lease_lock = threading.Lock()
        self._next_sequence = 0
        self._consumed_sequences: set[int] = set()
        self._gate_owner_hash = hashlib.sha256(
            _exact_id(gate_owner_id, "gate owner id").encode()
        ).hexdigest()
        self._gate_code_hash = _exact_sha(gate_code_hash, "gate code hash")
        self._bound_gate = BoundKisDomesticFunctionalTransportGate(
            token=self._factory_token, transport=self,
            gate_owner_id=gate_owner_id, gate_code_hash=gate_code_hash,
        )

    def __repr__(self) -> str:
        return "DisabledKisDomesticFunctionalProductionTransport(productionAvailable=False)"

    def bound_gate(self) -> BoundKisDomesticFunctionalTransportGate:
        return self._bound_gate

    def _gate_binding_for_transport_owner(self) -> tuple[BoundKisDomesticFunctionalTransportGate, object]:
        """Private integration seam for the isolated durable transport owner."""

        return self._bound_gate, self._gate_call_token

    def production_binding_status(self) -> dict[str, Any]:
        body = {
            "schemaVersion": _BINDING_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "origin": LIVE_ORIGIN,
            "gateOwnerHash": self._gate_owner_hash,
            "gateCodeHash": self._gate_code_hash,
            "credentialConfigurationHash": self._credential_configuration_hash,
            "credentialEnvironmentKeyIdHash": self._credential_environment_key_id_hash,
            "tokenVerifierKeyIdHash": self._token_verifier_key_id_hash,
            "authorizationVerifierKeyIdHash": self._auth_verifier_key_id_hash,
            **self._callback_code_hashes,
            "networkCompiled": False,
            "productionAvailable": False,
        }
        digest = _hash(body)
        return {
            **body, "bindingHash": digest,
            "bindingSignature": hmac.new(
                self._lease_key,
                ("KIS_PRODUCTION_BINDING\n" + digest).encode(), hashlib.sha256,
            ).hexdigest(),
        }

    def verify_production_binding_status(self, candidate: Mapping[str, Any]) -> bool:
        if not isinstance(candidate, Mapping):
            return False
        value = dict(candidate)
        signature = value.pop("bindingSignature", "")
        digest = value.pop("bindingHash", "")
        expected = self.production_binding_status()
        return (
            set(candidate) == set(expected)
            and value == {
                key: item for key, item in expected.items()
                if key not in {"bindingHash", "bindingSignature"}
            }
            and type(digest) is str and hmac.compare_digest(digest, _hash(value))
            and type(signature) is str
            and hmac.compare_digest(
                signature,
                hmac.new(
                    self._lease_key,
                    ("KIS_PRODUCTION_BINDING\n" + digest).encode(),
                    hashlib.sha256,
                ).hexdigest(),
            )
        )

    def _credential_environment(
        self, *, headers: Mapping[str, str], account: Mapping[str, str],
        request: Mapping[str, Any], immediately_before_socket: float,
    ) -> dict[str, Any]:
        try:
            raw = self._credential_environment_reader()
            if not isinstance(raw, Mapping):
                raise TypeError("non-mapping")
            value = dict(raw)
        except Exception as exc:
            name = type(exc).__name__
            safe = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"credential environment reader failed:{safe}"
            ) from None
        keys = {
            "schemaVersion", "route", "origin", "environmentRevision",
            "appKeyHash", "appSecretHash", "accountFieldsHash",
            "accountFingerprint", "credentialConfigurationHash",
            "observedEpoch", "verifierKeyIdHash", "productionAvailable",
            "signature",
        }
        if set(value) != keys:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "credential environment snapshot is not exact"
            )
        expected_hash = _get_credential_configuration_hash(
            app_key=headers["appkey"], app_secret=headers["appsecret"],
            account_fingerprint=request["accountFingerprint"],
        ) if type(value.get("environmentRevision")) is int and value["environmentRevision"] >= 1 else ""
        exact = {
            "schemaVersion": _ENVIRONMENT_SCHEMA, "route": ROUTE,
            "origin": LIVE_ORIGIN, "appKeyHash": _secret_hash(headers["appkey"]),
            "appSecretHash": _secret_hash(headers["appsecret"]),
            "accountFieldsHash": _hash(account),
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request["credentialConfigurationHash"],
            "verifierKeyIdHash": self._credential_environment_key_id_hash,
            "productionAvailable": False,
        }
        if (
            any(type(value.get(k)) is not type(v) or value.get(k) != v for k, v in exact.items())
            or request["credentialConfigurationHash"]
            != self._credential_configuration_hash
            or expected_hash != request["credentialConfigurationHash"]
            or type(value.get("observedEpoch")) not in {int, float}
            or not math.isfinite(float(value["observedEpoch"]))
            or float(value["observedEpoch"]) > immediately_before_socket
            or immediately_before_socket - float(value["observedEpoch"]) > 1.0
            or type(value.get("signature")) is not str
            or not _SHA.fullmatch(value["signature"])
        ):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "credential environment binding mismatch"
            )
        try:
            verified = self._credential_environment_verifier(dict(value))
        except Exception as exc:
            name = type(exc).__name__
            safe = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"credential environment verifier failed:{safe}"
            ) from None
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "credential environment signature is unverified"
            )
        return value

    def _lease_signature(self, body: Mapping[str, Any]) -> str:
        return hmac.new(
            self._lease_key,
            ("KIS_PRODUCTION_TRANSPORT_GATE_LEASE\n" + _canonical(body)).encode(),
            hashlib.sha256,
        ).hexdigest()

    def _mint_lease(self, *, gate_owner_hash: str, gate_code_hash: str,
                    request_hash: str) -> _GateLease:
        if gate_owner_hash != self._gate_owner_hash or gate_code_hash != self._gate_code_hash:
            raise KisDomesticFunctionalProductionTransportBlocked("gate binding changed")
        with self._lease_lock:
            self._next_sequence += 1
            sequence = self._next_sequence
        nonce = secrets.token_hex(16)
        body = {
            "factoryId": self._factory_id, "gateOwnerHash": gate_owner_hash,
            "gateCodeHash": gate_code_hash, "requestHash": request_hash,
            "sequence": sequence, "nonce": nonce,
        }
        return _GateLease(
            factory_id=self._factory_id, gate_owner_hash=gate_owner_hash,
            gate_code_hash=gate_code_hash, request_hash=request_hash,
            sequence=sequence, nonce=nonce, signature=self._lease_signature(body),
        )

    def _consume_lease(self, lease: Any, request_hash: str) -> None:
        if type(lease) is not _GateLease:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "exact private transport gate lease is required"
            )
        body = {
            "factoryId": lease.factory_id, "gateOwnerHash": lease.gate_owner_hash,
            "gateCodeHash": lease.gate_code_hash, "requestHash": lease.request_hash,
            "sequence": lease.sequence, "nonce": lease.nonce,
        }
        if (
            lease.factory_id != self._factory_id
            or lease.gate_owner_hash != self._gate_owner_hash
            or lease.gate_code_hash != self._gate_code_hash
            or lease.request_hash != request_hash
            or type(lease.sequence) is not int or lease.sequence < 1
            or type(lease.nonce) is not str
            or not hmac.compare_digest(lease.signature, self._lease_signature(body))
        ):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "transport gate lease binding is invalid"
            )
        with self._lease_lock:
            if lease.sequence in self._consumed_sequences:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "transport gate lease was already consumed"
                )
            self._consumed_sequences.add(lease.sequence)

    @staticmethod
    def _request(value: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(value, Mapping) or set(value) != _REQUEST_KEYS:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "transport gate request is not exact"
            )
        request = dict(value)
        operation = request.get("operation")
        spec = _OPERATIONS.get(operation)
        if spec is None:
            raise KisDomesticFunctionalProductionTransportBlocked("operation is invalid")
        endpoint, tr_id, side, cleanup = spec
        exact = {
            "schemaVersion": "kis-domestic-functional-transport-request/v1",
            "route": ROUTE, "pdno": PDNO, "origin": LIVE_ORIGIN,
            "method": "POST", "endpoint": endpoint, "trId": tr_id,
            "query": [], "headers": {"custtype": "P", "tr_id": tr_id},
            "side": side, "cleanupOnly": cleanup,
        }
        for key, expected in exact.items():
            if type(request.get(key)) is not type(expected) or request.get(key) != expected:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    f"transport gate request {key} mismatch"
                )
        _exact_id(request.get("claimId"), "claim id")
        _exact_id(request.get("sessionId"), "session id")
        if type(request.get("authorityRevision")) is not int or request["authorityRevision"] < 1:
            raise KisDomesticFunctionalProductionTransportBlocked("authority revision is invalid")
        _exact_sha(request.get("accountFingerprint"), "account fingerprint")
        _exact_sha(request.get("credentialConfigurationHash"), "credential configuration hash")
        _exact_sha(request.get("payloadHash"), "payload hash")
        if type(request.get("payload")) is not dict or _hash(request["payload"]) != request["payloadHash"]:
            raise KisDomesticFunctionalProductionTransportBlocked("payload hash mismatch")
        payload = request["payload"]
        if any(key in payload for key in _FORBIDDEN_PAYLOAD_KEYS):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "payload attempts to override account or authorization"
            )
        if operation in {"NATURAL_BUY", "CLEANUP_SELL"}:
            if set(payload) != {"PDNO", "ORD_DVSN", "ORD_QTY", "ORD_UNPR"}:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "order payload schema mismatch"
                )
            if payload["PDNO"] != PDNO or payload["ORD_DVSN"] != "00" or payload["ORD_QTY"] != "1":
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "order payload PDNO/quantity mismatch"
                )
            price = payload["ORD_UNPR"]
            if type(price) is not str or not price.isascii() or not price.isdigit() or not 1 <= int(price) <= 100_000:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "order payload price/cap mismatch"
                )
        else:
            if set(payload) != {
                "KRX_FWDG_ORD_ORGNO", "ORGN_ODNO", "ORD_DVSN",
                "RVSE_CNCL_DVSN_CD", "ORD_QTY", "ORD_UNPR",
                "QTY_ALL_ORD_YN", "EXCG_ID_DVSN_CD",
            }:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "cancel payload schema mismatch"
                )
            if (
                type(payload["KRX_FWDG_ORD_ORGNO"]) is not str
                or not re.fullmatch(r"[0-9]{1,16}", payload["KRX_FWDG_ORD_ORGNO"])
                or type(payload["ORGN_ODNO"]) is not str
                or not re.fullmatch(r"[0-9]{1,16}", payload["ORGN_ODNO"])
                or {key: payload[key] for key in (
                    "ORD_DVSN", "RVSE_CNCL_DVSN_CD", "ORD_QTY", "ORD_UNPR",
                    "QTY_ALL_ORD_YN", "EXCG_ID_DVSN_CD",
                )} != {
                    "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02",
                    "ORD_QTY": "1", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y",
                    "EXCG_ID_DVSN_CD": "KRX",
                }
            ):
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "cancel payload owned tuple mismatch"
                )
        return request

    @staticmethod
    def _token(raw: Any, request: Mapping[str, Any], verifier: Callable[[Mapping[str, Any]], bool], verifier_key_id_hash: str, now: float) -> dict[str, Any]:
        keys = {
            "schemaVersion", "accessToken", "tokenHash", "expiresEpoch",
            "accountFingerprint", "credentialConfigurationHash", "attestation",
        }
        if not isinstance(raw, Mapping) or set(raw) != keys:
            raise KisDomesticFunctionalProductionTransportBlocked("token envelope is not exact")
        value = dict(raw)
        if value.get("schemaVersion") != "kis-domestic-functional-token-envelope/v1":
            raise KisDomesticFunctionalProductionTransportBlocked(
                "token envelope schema mismatch"
            )
        token = value.get("accessToken")
        if type(token) is not str or not _HEADER_SECRET.fullmatch(token) or value.get("tokenHash") != _secret_hash(token):
            raise KisDomesticFunctionalProductionTransportBlocked("token envelope hash mismatch")
        if type(value.get("expiresEpoch")) not in {int, float} or not math.isfinite(float(value["expiresEpoch"])) or float(value["expiresEpoch"]) <= now:
            raise KisDomesticFunctionalProductionTransportBlocked("token envelope is expired")
        for key in ("accountFingerprint", "credentialConfigurationHash"):
            if value.get(key) != request[key]:
                raise KisDomesticFunctionalProductionTransportBlocked("token binding mismatch")
        attestation = value.get("attestation")
        if not isinstance(attestation, Mapping):
            raise KisDomesticFunctionalProductionTransportBlocked("token attestation is unverified")
        expected_attestation = {
            "schemaVersion": "kis-domestic-functional-token-attestation/v1",
            "tokenHash": value["tokenHash"], "expiresEpoch": value["expiresEpoch"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request["credentialConfigurationHash"],
            "verifierKeyIdHash": verifier_key_id_hash,
        }
        signed = dict(attestation)
        if set(signed) != set(expected_attestation) | {"signature"} or type(signed.get("signature")) is not str or not _SHA.fullmatch(signed["signature"]) or any(
            signed.get(key) != expected for key, expected in expected_attestation.items()
        ):
            raise KisDomesticFunctionalProductionTransportBlocked("token attestation binding mismatch")
        try:
            verified = verifier(signed)
        except Exception as exc:
            name = type(exc).__name__
            safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"token attestation verifier failed:{safe_name}"
            ) from None
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalProductionTransportBlocked("token attestation is unverified")
        return value

    @staticmethod
    def _authorization(raw: Any, *, token: Mapping[str, Any], request: Mapping[str, Any],
                       verifier: Callable[[Mapping[str, Any]], bool], verifier_key_id_hash: str) -> tuple[dict[str, str], dict[str, str]]:
        keys = {"schemaVersion", "headers", "accountFields", "attestation"}
        if not isinstance(raw, Mapping) or set(raw) != keys:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "authorization envelope is not exact"
            )
        value = dict(raw)
        if value.get("schemaVersion") != "kis-domestic-functional-auth-header-envelope/v1":
            raise KisDomesticFunctionalProductionTransportBlocked(
                "authorization envelope schema mismatch"
            )
        headers = value.get("headers"); account = value.get("accountFields")
        if not isinstance(headers, Mapping) or set(headers) != {
            "authorization", "appkey", "appsecret", "custtype", "tr_id", "content-type"
        } or any(type(item) is not str for item in headers.values()):
            raise KisDomesticFunctionalProductionTransportBlocked("authorization headers are not exact")
        headers = dict(headers)
        if (
            headers["authorization"] != "Bearer " + token["accessToken"]
            or not _HEADER_SECRET.fullmatch(headers["appkey"])
            or not _HEADER_SECRET.fullmatch(headers["appsecret"])
            or len(headers["appsecret"]) < 16
            or headers["custtype"] != "P" or headers["tr_id"] != request["trId"]
            or headers["content-type"] != "application/json; charset=utf-8"
        ):
            raise KisDomesticFunctionalProductionTransportBlocked("authorization header binding mismatch")
        if not isinstance(account, Mapping) or set(account) != {"CANO", "ACNT_PRDT_CD"}:
            raise KisDomesticFunctionalProductionTransportBlocked("account fields are not exact")
        account = dict(account)
        if type(account["CANO"]) is not str or not _ACCOUNT.fullmatch(account["CANO"]) or type(account["ACNT_PRDT_CD"]) is not str or not _PRODUCT.fullmatch(account["ACNT_PRDT_CD"]):
            raise KisDomesticFunctionalProductionTransportBlocked("account fields are invalid")
        try:
            derived_account = kis_domestic_functional_account_fingerprint(
                account["CANO"], account["ACNT_PRDT_CD"]
            )
        except Exception:
            raise KisDomesticFunctionalProductionTransportBlocked(
                "account fields cannot derive the sealed account fingerprint"
            ) from None
        if not hmac.compare_digest(derived_account, request["accountFingerprint"]):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "account fields/sealed fingerprint mismatch"
            )
        attestation = value.get("attestation")
        if not isinstance(attestation, Mapping):
            raise KisDomesticFunctionalProductionTransportBlocked("authorization attestation is absent")
        expected = {
            "schemaVersion": "kis-domestic-functional-auth-header-attestation/v1",
            "tokenHash": token["tokenHash"],
            "appKeyHash": _secret_hash(headers["appkey"]),
            "appSecretHash": _secret_hash(headers["appsecret"]),
            "accountFieldsHash": _hash(account), "trId": request["trId"],
            "claimId": request["claimId"], "payloadHash": request["payloadHash"],
            "sessionId": request["sessionId"],
            "authorityRevision": request["authorityRevision"],
            "operation": request["operation"], "endpoint": request["endpoint"],
            "requestHash": _hash(request),
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request["credentialConfigurationHash"],
            "verifierKeyIdHash": verifier_key_id_hash,
        }
        signed = dict(attestation)
        if set(signed) != set(expected) | {"signature"} or type(signed.get("signature")) is not str or not _SHA.fullmatch(signed["signature"]) or any(
            signed.get(key) != wanted for key, wanted in expected.items()
        ):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "authorization attestation binding mismatch"
            )
        try:
            verified = verifier(signed)
        except Exception as exc:
            name = type(exc).__name__
            safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"authorization attestation verifier failed:{safe_name}"
            ) from None
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalProductionTransportBlocked("authorization attestation is unverified")
        return headers, account

    def _dispatch(self, *, request: Mapping[str, Any], lease: Any) -> Mapping[str, Any]:
        normalized = self._request(request)
        request_hash = _hash(normalized)
        self._consume_lease(lease, request_hash)
        # A failure trace belongs to exactly one consumed request lease.  Never
        # let an earlier physical attempt masquerade as evidence for a later
        # token/auth/pre-socket failure that made no socket call.
        self._last_failed_attempt = None
        if not self._mock and (
            KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT_NETWORK_COMPILED is not True
            or os.environ.get(_NETWORK_ENV) != _NETWORK_ENV_VALUE
        ):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "KIS production trading network is compile-disabled"
            )
        now = self._wall_clock()
        if type(now) not in {int, float} or not math.isfinite(float(now)):
            raise KisDomesticFunctionalProductionTransportBlocked("wall clock is invalid")
        token_binding = {
                "claimId": normalized["claimId"], "sessionId": normalized["sessionId"],
                "accountFingerprint": normalized["accountFingerprint"],
                "credentialConfigurationHash": normalized["credentialConfigurationHash"],
                "requestHash": request_hash,
            }
        try:
            raw_token = self._token_reader(token_binding)
        except Exception as exc:
            name = type(exc).__name__
            safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"token reader failed:{safe_name}"
            ) from None
        token = self._token(
            raw_token, normalized, self._token_verifier,
            self._token_verifier_key_id_hash, float(now),
        )
        try:
            authorization = self._header_builder({
                "accessToken": token["accessToken"], "tokenHash": token["tokenHash"],
                "expiresEpoch": token["expiresEpoch"],
            }, {
                "claimId": normalized["claimId"], "trId": normalized["trId"],
                "payloadHash": normalized["payloadHash"],
                "sessionId": normalized["sessionId"],
                "authorityRevision": normalized["authorityRevision"],
                "operation": normalized["operation"],
                "endpoint": normalized["endpoint"], "requestHash": request_hash,
                "accountFingerprint": normalized["accountFingerprint"],
                "credentialConfigurationHash": normalized["credentialConfigurationHash"],
            })
        except Exception as exc:
            name = type(exc).__name__
            safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"authorization header builder failed:{safe_name}"
            ) from None
        headers, account = self._authorization(
            authorization, token=token, request=normalized,
            verifier=self._auth_verifier,
            verifier_key_id_hash=self._auth_verifier_key_id_hash,
        )
        body_object = {**account, **normalized["payload"]}
        body_bytes = _canonical(body_object).encode()
        url = LIVE_ORIGIN + normalized["endpoint"]
        prepared = _PreparedTradingRequest(
            method="POST", url=url, endpoint=normalized["endpoint"],
            tr_id=normalized["trId"], headers=headers, body=body_bytes,
            timeout_seconds=self._timeout,
        )
        started = self._monotonic_ns()
        if type(started) is not int or started < 0:
            raise KisDomesticFunctionalProductionTransportBlocked("monotonic clock is invalid")
        immediately_before_socket = self._wall_clock()
        if (
            type(immediately_before_socket) not in {int, float}
            or not math.isfinite(float(immediately_before_socket))
            or float(immediately_before_socket) >= float(token["expiresEpoch"])
        ):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "token expired immediately before physical attempt"
            )
        credential_environment = self._credential_environment(
            headers=headers, account=account, request=normalized,
            immediately_before_socket=float(immediately_before_socket),
        )
        attempt_binding = {
            "schemaVersion": "kis-domestic-functional-pre-socket-binding/v1",
            "requestHash": request_hash,
            "sessionId": normalized["sessionId"],
            "authorityRevision": normalized["authorityRevision"],
            "operation": normalized["operation"],
            "endpoint": normalized["endpoint"],
            "accountFingerprint": normalized["accountFingerprint"],
            "credentialConfigurationHash": normalized[
                "credentialConfigurationHash"
            ],
            "environmentRevision": credential_environment[
                "environmentRevision"
            ],
            "credentialEnvironmentKeyIdHash": self._credential_environment_key_id_hash,
            "productionBindingHash": self.production_binding_status()["bindingHash"],
        }
        try:
            raw_response = self._executor(prepared)
        except KisDomesticFunctionalProductionTransportBlocked as exc:
            ended = self._monotonic_ns()
            self._last_failed_attempt = self._failure_attempt(
                normalized=normalized, request_hash=request_hash, started=started,
                ended=ended, error=exc, attempt_binding=attempt_binding,
            )
            raise
        except Exception as exc:
            ended = self._monotonic_ns()
            self._last_failed_attempt = self._failure_attempt(
                normalized=normalized, request_hash=request_hash, started=started,
                ended=ended, error=exc, attempt_binding=attempt_binding,
            )
            name = type(exc).__name__
            safe_name = name if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", name) else "Error"
            raise KisDomesticFunctionalProductionTransportBlocked(
                f"KIS trading mock physical attempt failed:{safe_name}"
            ) from None
        ended = self._monotonic_ns()
        # Once the executor returns, one physical attempt definitely happened.
        # Keep a conservative owned failure record until every response field,
        # URL, body and privacy check below succeeds.
        self._last_failed_attempt = self._failure_attempt(
            normalized=normalized, request_hash=request_hash, started=started,
            ended=ended,
            error=KisDomesticFunctionalProductionTransportBlocked(
                "physical response validation failed"
            ),
            attempt_binding=attempt_binding,
        )
        if type(ended) is not int or ended < started:
            raise KisDomesticFunctionalProductionTransportBlocked("monotonic trace is invalid")
        if not isinstance(raw_response, Mapping) or set(raw_response) != {
            "statusCode", "effectiveUrl", "responseHeaders", "bodyBytes", "redirectFollowed"
        }:
            raise KisDomesticFunctionalProductionTransportBlocked("physical response is not exact")
        response = dict(raw_response)
        if type(response["statusCode"]) is not int or not 100 <= response["statusCode"] <= 599:
            raise KisDomesticFunctionalProductionTransportBlocked("physical response status is invalid")
        if type(response["effectiveUrl"]) is not str or not hmac.compare_digest(response["effectiveUrl"], url):
            raise KisDomesticFunctionalProductionTransportBlocked("physical response effective URL changed")
        if type(response["redirectFollowed"]) is not bool or response["redirectFollowed"] is not False:
            raise KisDomesticFunctionalProductionTransportBlocked("physical response followed a redirect")
        if not isinstance(response["responseHeaders"], Mapping) or set(response["responseHeaders"]) != {"content-type"} or type(response["responseHeaders"]["content-type"]) is not str:
            raise KisDomesticFunctionalProductionTransportBlocked("physical response headers are not exact")
        body = _safe_json_object(response["bodyBytes"])
        observed_epoch = self._wall_clock()
        if type(observed_epoch) not in {int, float} or not math.isfinite(float(observed_epoch)):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "response observation wall clock is invalid"
            )
        observed_at = datetime.fromtimestamp(
            float(observed_epoch), tz=timezone.utc
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        response_text = _canonical(body)
        for private_value in (
            token["accessToken"], headers["appkey"], headers["appsecret"],
            account["CANO"],
        ):
            if private_value and private_value in response_text:
                raise KisDomesticFunctionalProductionTransportBlocked(
                    "KIS response contained forbidden private material"
                )
        trace = {
            "schemaVersion": "kis-domestic-functional-physical-attempt/v1",
            "requestHash": request_hash, "method": "POST",
            "origin": LIVE_ORIGIN, "endpoint": normalized["endpoint"],
            "trId": normalized["trId"], "startedMonotonicNs": started,
            "endedMonotonicNs": ended, "elapsedMonotonicNs": ended - started,
            "physicalAttemptCount": 1, "physicalAttemptComplete": True,
            "hiddenRetryCount": 0, "redirectFollowCount": 0,
            "effectiveUrl": url, "effectiveUrlExact": True,
            "statusCode": response["statusCode"],
            "observedAt": observed_at,
            "responseBodyHash": _hash(body), "mockSocket": self._mock,
            "attemptBindingHash": _hash(attempt_binding),
            "credentialEnvironmentRevision": credential_environment[
                "environmentRevision"
            ],
            "productionAvailable": False,
        }
        result = {
            "schemaVersion": "kis-domestic-functional-production-transport-result/v1",
            "requestHash": request_hash, "method": "POST", "origin": LIVE_ORIGIN,
            "endpoint": normalized["endpoint"], "trId": normalized["trId"],
            "effectiveUrl": url, "statusCode": response["statusCode"],
            "observedAt": observed_at, "body": body,
            "physicalAttemptCount": 1, "hiddenRetryCount": 0,
            "redirectFollowCount": 0, "physicalTrace": trace,
            "physicalTraceHash": _hash(trace),
            "attemptBinding": attempt_binding,
            "attemptBindingHash": _hash(attempt_binding),
            "physicalTraceOwned": True,
            "errorArchive": {},
            "errorArchiveHash": "",
            "authorizationMaterialArchived": False,
            "accountIdentifiersArchived": False,
            "releaseEligible": False,
        }
        self._last_failed_attempt = None
        return result

    @staticmethod
    def _safe_error_kind(error: BaseException) -> str:
        value = type(error).__name__
        return value if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", value) else "Error"

    def _failure_attempt(
        self, *, normalized: Mapping[str, Any], request_hash: str,
        started: int, ended: Any, error: BaseException,
        attempt_binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        if type(ended) is not int or ended < started:
            ended = started
        error_archive = {
            "schemaVersion": "kis-domestic-functional-physical-error/v1",
            "requestHash": request_hash,
            "errorKind": self._safe_error_kind(error),
            "physicalAttemptCount": 1,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
            "productionAvailable": False,
        }
        observed_epoch = self._wall_clock()
        if type(observed_epoch) not in {int, float} or not math.isfinite(float(observed_epoch)):
            observed_epoch = 0.0
        observed_at = datetime.fromtimestamp(
            float(observed_epoch), tz=timezone.utc
        ).isoformat(timespec="microseconds").replace("+00:00", "Z")
        body = {
            "transportOutcome": "PHYSICAL_ATTEMPT_FAILED",
            "errorArchiveHash": _hash(error_archive),
        }
        trace = {
            "schemaVersion": "kis-domestic-functional-physical-attempt/v1",
            "requestHash": request_hash, "method": "POST", "origin": LIVE_ORIGIN,
            "endpoint": normalized["endpoint"], "trId": normalized["trId"],
            "startedMonotonicNs": started, "endedMonotonicNs": ended,
            "elapsedMonotonicNs": ended - started,
            "physicalAttemptCount": 1, "physicalAttemptComplete": False,
            "hiddenRetryCount": 0, "redirectFollowCount": 0,
            "effectiveUrl": LIVE_ORIGIN + normalized["endpoint"],
            "effectiveUrlExact": True, "statusCode": 599,
            "observedAt": observed_at,
            "responseBodyHash": _hash(body), "mockSocket": self._mock,
            "attemptBindingHash": _hash(attempt_binding),
            "credentialEnvironmentRevision": attempt_binding["environmentRevision"],
            "errorArchiveHash": _hash(error_archive),
            "productionAvailable": False,
        }
        return {
            "schemaVersion": "kis-domestic-functional-production-transport-error/v1",
            "requestHash": request_hash, "method": "POST", "origin": LIVE_ORIGIN,
            "endpoint": normalized["endpoint"], "trId": normalized["trId"],
            "effectiveUrl": LIVE_ORIGIN + normalized["endpoint"],
            "statusCode": 599, "observedAt": observed_at, "body": body,
            "physicalAttemptCount": 1, "hiddenRetryCount": 0,
            "redirectFollowCount": 0, "physicalTrace": trace,
            "physicalTraceHash": _hash(trace), "physicalTraceOwned": True,
            "attemptBinding": dict(attempt_binding),
            "attemptBindingHash": _hash(attempt_binding),
            "errorArchive": error_archive,
            "errorArchiveHash": _hash(error_archive),
            "authorizationMaterialArchived": False,
            "accountIdentifiersArchived": False,
            "releaseEligible": False,
        }

    def last_failed_attempt(self) -> dict[str, Any]:
        value = getattr(self, "_last_failed_attempt", None)
        if not isinstance(value, Mapping):
            raise KisDomesticFunctionalProductionTransportBlocked(
                "physical failure trace is absent"
            )
        return json.loads(_canonical(value))


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False, "networkCompiled": False,
        "networkAvailable": False, "releaseEvidenceAvailable": False,
        "transportGateLeaseRequired": True, "ownedNoRedirectSenderImplemented": True,
        "route": ROUTE, "pdno": PDNO,
        "reason": "OWNED_KIS_TRADING_TRANSPORT_COMPILE_DISABLED_NO_PRODUCTION_GRAPH",
    }


__all__ = [
    "BoundKisDomesticFunctionalTransportGate",
    "DisabledKisDomesticFunctionalProductionTransport",
    "KisDomesticFunctionalProductionTransportBlocked",
    "kis_domestic_functional_callback_code_hash",
    "production_entrypoint_status",
]
