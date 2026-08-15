from __future__ import annotations

"""Pinned client for an independently administered first-live WORM service.

The authority endpoint and its storage must be outside the LiveTrader host's
coordinator/high-water backup and restore domain.  This module owns no local
fallback: TLS pin drift, signature drift, a redirect, an unavailable endpoint,
or a non-monotonic receipt all fail closed.
"""

import base64
import hashlib
import http.client
import json
import math
import re
import secrets
import ssl
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from .crypto_first_live_high_water import (
    EXTERNAL_WORM_SCHEMA_VERSION,
    GLOBAL_SCOPE,
)


WIRE_REQUEST_SCHEMA = "crypto-first-live-external-worm-wire-request/v1"
WIRE_RESPONSE_SCHEMA = "crypto-first-live-external-worm-wire-response/v1"
SIGNATURE_DOMAIN = b"CRYPTO_FIRST_LIVE_EXTERNAL_WORM_RECEIPT_V1\x00"
MAX_RESPONSE_BYTES = 64 * 1024
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")


class CryptoFirstLiveExternalWormError(RuntimeError):
    pass


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _exact_id(value: object, label: str) -> str:
    result = _text(value)
    if _ID_RE.fullmatch(result) is None:
        raise CryptoFirstLiveExternalWormError(f"{label}-invalid")
    return result


def _exact_hash(value: object, label: str) -> str:
    result = _text(value).lower()
    if _HASH_RE.fullmatch(result) is None:
        raise CryptoFirstLiveExternalWormError(f"{label}-invalid")
    return result


def https_json_transport(
    endpoint_url: str,
    body: Mapping[str, Any],
    *,
    timeout_seconds: float,
) -> Mapping[str, Any]:
    """One non-redirecting HTTPS POST that returns the peer cert hash."""

    parsed = urlsplit(endpoint_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
    ):
        raise CryptoFirstLiveExternalWormError(
            "crypto-first-live-external-worm-endpoint-invalid"
        )
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    connection = http.client.HTTPSConnection(
        parsed.hostname,
        parsed.port or 443,
        context=context,
        timeout=float(timeout_seconds),
    )
    encoded = _canonical(body)
    try:
        connection.request(
            "POST",
            parsed.path,
            body=encoded,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Content-Length": str(len(encoded)),
            },
        )
        response = connection.getresponse()
        peer = (
            connection.sock.getpeercert(binary_form=True)
            if connection.sock is not None
            else b""
        )
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-response-too-large"
            )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-response-not-json"
            ) from exc
        return {
            "status": int(response.status),
            "peerCertificateSha256": hashlib.sha256(peer).hexdigest(),
            "json": payload,
        }
    finally:
        connection.close()


class PinnedExternalWormAuthorityClient:
    """Verify TLS identity and an Ed25519-signed monotonic WORM receipt."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        namespace_id: str,
        authority_id: str,
        key_id: str,
        public_key: bytes | str,
        tls_certificate_sha256: str,
        transport: Callable[..., Mapping[str, Any]] = https_json_transport,
        clock: Callable[[], float] = time.time,
        timeout_seconds: float = 10.0,
    ) -> None:
        parsed = urlsplit(_text(endpoint_url))
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or not parsed.path.startswith("/")
        ):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-endpoint-invalid"
            )
        self.endpoint_url = parsed.geturl()
        self.namespace_id = _exact_id(namespace_id, "namespace-id")
        self.authority_id = _exact_id(authority_id, "authority-id")
        self.key_id = _exact_id(key_id, "key-id")
        self.tls_certificate_sha256 = _exact_hash(
            tls_certificate_sha256, "tls-certificate-sha256"
        )
        try:
            key = ECC.import_key(public_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-public-key-invalid"
            ) from exc
        if key.has_private() or getattr(key, "curve", None) != "Ed25519":
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-public-key-only-required"
            )
        timeout = float(timeout_seconds)
        if not math.isfinite(timeout) or not 0 < timeout <= 15:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-timeout-invalid"
            )
        self._public_key = key
        self.transport = transport
        self.clock = clock
        self.timeout_seconds = timeout

    def __call__(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        checkpoint = dict(request)
        if (
            set(checkpoint)
            != {
                "schemaVersion",
                "action",
                "purpose",
                "scope",
                "databaseId",
                "priorRevision",
                "priorPublicationHash",
                "revision",
                "publicationHash",
                "localAnchorRevision",
                "localAnchorHeadHash",
            }
            or checkpoint.get("schemaVersion")
            != EXTERNAL_WORM_SCHEMA_VERSION
            or checkpoint.get("action") != "OBSERVE_OR_ADVANCE"
            or checkpoint.get("scope") != GLOBAL_SCOPE
        ):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-checkpoint-request-invalid"
            )
        now = float(self.clock())
        if not math.isfinite(now) or now <= 0:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-clock-invalid"
            )
        checkpoint_hash = _digest(checkpoint)
        wire_body = {
            "schemaVersion": WIRE_REQUEST_SCHEMA,
            "operation": "CHECKPOINT",
            "namespaceId": self.namespace_id,
            "authorityId": self.authority_id,
            "keyId": self.key_id,
            "nonce": "worm-request-" + secrets.token_hex(24),
            "issuedEpoch": now,
            "checkpointRequest": checkpoint,
            "checkpointRequestHash": checkpoint_hash,
        }
        try:
            transport_result = self.transport(
                self.endpoint_url,
                wire_body,
                timeout_seconds=self.timeout_seconds,
            )
        except Exception as exc:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-transport-failed"
            ) from exc
        if not isinstance(transport_result, Mapping):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-transport-invalid"
            )
        transport_value = dict(transport_result)
        if (
            set(transport_value)
            != {"status", "peerCertificateSha256", "json"}
            or transport_value.get("status") != 200
            or not secrets.compare_digest(
                _text(transport_value.get("peerCertificateSha256")).lower(),
                self.tls_certificate_sha256,
            )
        ):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-tls-or-status-invalid"
            )
        raw = transport_value.get("json")
        if not isinstance(raw, Mapping):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-wire-response-invalid"
            )
        response = dict(raw)
        if set(response) != {
            "schemaVersion",
            "namespaceId",
            "authorityId",
            "keyId",
            "requestHash",
            "receipt",
            "signatureBase64",
        }:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-wire-fields-not-exact"
            )
        signed_body = {
            key: item
            for key, item in response.items()
            if key != "signatureBase64"
        }
        if (
            response.get("schemaVersion") != WIRE_RESPONSE_SCHEMA
            or response.get("namespaceId") != self.namespace_id
            or response.get("authorityId") != self.authority_id
            or response.get("keyId") != self.key_id
            or response.get("requestHash") != _digest(wire_body)
            or not isinstance(response.get("receipt"), Mapping)
        ):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-wire-response-invalid"
            )
        try:
            signature = base64.b64decode(
                _text(response.get("signatureBase64")), validate=True
            )
            if len(signature) != 64:
                raise ValueError("signature length")
            eddsa.new(self._public_key, "rfc8032").verify(
                SIGNATURE_DOMAIN + _canonical(signed_body), signature
            )
        except (ValueError, TypeError) as exc:
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-signature-invalid"
            ) from exc
        receipt = dict(response["receipt"])
        if (
            receipt.get("authorityId") != self.authority_id
            or receipt.get("databaseId") != checkpoint.get("databaseId")
        ):
            raise CryptoFirstLiveExternalWormError(
                "crypto-first-live-external-worm-receipt-binding-invalid"
            )
        return receipt


def provisioning_request(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    """Build the only permitted initial namespace checkpoint (revision 0)."""

    value = dict(descriptor)
    if (
        set(value)
        != {
            "databaseId",
            "priorRevision",
            "priorPublicationHash",
            "revision",
            "publicationHash",
            "localAnchorRevision",
            "localAnchorHeadHash",
        }
        or value.get("priorRevision") != 0
        or value.get("priorPublicationHash") != ""
        or value.get("revision") != 0
        or value.get("publicationHash") != ""
    ):
        raise CryptoFirstLiveExternalWormError(
            "crypto-first-live-external-worm-provision-requires-revision-zero"
        )
    return {
        "schemaVersion": EXTERNAL_WORM_SCHEMA_VERSION,
        "action": "OBSERVE_OR_ADVANCE",
        "purpose": "PROVISION_NAMESPACE",
        "scope": GLOBAL_SCOPE,
        **value,
    }


__all__ = [
    "CryptoFirstLiveExternalWormError",
    "PinnedExternalWormAuthorityClient",
    "SIGNATURE_DOMAIN",
    "WIRE_REQUEST_SCHEMA",
    "WIRE_RESPONSE_SCHEMA",
    "https_json_transport",
    "provisioning_request",
]
