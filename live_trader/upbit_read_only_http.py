from __future__ import annotations

"""Minimal Upbit GET-only HTTP primitive for the detached authority bundle.

This module deliberately contains no POST/DELETE/body path and imports no
broker order adapter.  It is safe to copy into the SYSTEM-owned authority
bundle without bringing the live trader's mutation builders with it.
"""

from dataclasses import dataclass
import json
import os
from typing import Any
import urllib.error
import urllib.parse
import urllib.request


UPBIT_BASE_URL = "https://api.upbit.com"
UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED = False
UPBIT_READ_ONLY_GET_ENDPOINTS = frozenset(
    {
        "/v1/accounts",
        "/v1/api_keys",
        "/v1/orders/chance",
        "/v1/orders/open",
        "/v1/orders/closed",
        "/v1/order",
        "/v1/ticker",
    }
)
_READ_ONLY_HTTP_NETWORK_CAPABILITY = object()


def _protected_upbit_read_only_http_network_capability() -> object:
    """Issue the identity-only socket capability to protected authority wiring."""

    if UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED is not True:
        raise RuntimeError("upbit-read-only-http-network-release-held")
    return _READ_ONLY_HTTP_NETWORK_CAPABILITY


def _assert_read_only_http_network_capability(capability: object) -> None:
    if (
        UPBIT_READ_ONLY_HTTP_NETWORK_RELEASED is not True
        or capability is not _READ_ONLY_HTTP_NETWORK_CAPABILITY
    ):
        raise RuntimeError("upbit-read-only-http-network-capability-closed")


def env_value(name: str) -> str:
    return os.getenv(name, "").strip()


@dataclass(frozen=True)
class PreparedRequest:
    provider: str
    method: str
    url: str
    endpoint: str
    headers: dict[str, str]
    safe_headers: dict[str, object]
    body: dict[str, object] | None
    query: dict[str, object] | None
    blocked_reasons: list[str]

    @property
    def can_send(self) -> bool:
        return not self.blocked_reasons

    def preview(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "method": self.method,
            "url": self.url,
            "endpoint": self.endpoint,
            "headers": dict(self.safe_headers),
            "body": {},
            "query": dict(self.query or {}),
            "blocked_reasons": list(self.blocked_reasons),
            "can_send": self.can_send,
        }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _parse_json(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _require_exact_get(prepared: PreparedRequest) -> None:
    try:
        parsed = urllib.parse.urlsplit(prepared.url)
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("upbit-read-only-url-invalid") from exc
    if (
        prepared.provider != "upbit-functional-read"
        or prepared.method != "GET"
        or prepared.body is not None
        or prepared.endpoint not in UPBIT_READ_ONLY_GET_ENDPOINTS
        or prepared.endpoint != parsed.path
        or parsed.scheme != "https"
        or parsed.hostname != "api.upbit.com"
        or parsed.netloc != "api.upbit.com"
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or prepared.safe_headers.get("authorization_configured") is not True
    ):
        raise RuntimeError("upbit-read-only-request-shape-invalid")


def send_prepared_request(
    prepared: PreparedRequest,
    *,
    timeout_seconds: float = 10.0,
    network_capability: object | None = None,
) -> dict[str, object]:
    """Perform exactly one no-redirect GET to the official Upbit origin."""

    # The release/capability check is deliberately first. Even a perfectly
    # shaped signed request cannot construct an opener from a public call.
    _assert_read_only_http_network_capability(network_capability)
    if not prepared.can_send:
        return {
            "ok": False,
            "status": "blocked",
            "preview": prepared.preview(),
            "physicalAttemptCount": 0,
            "retryAllowed": False,
        }
    _require_exact_get(prepared)
    request = urllib.request.Request(
        prepared.url,
        data=None,
        headers=dict(prepared.headers),
        method="GET",
    )
    opener = urllib.request.build_opener(_NoRedirect()).open
    try:
        with opener(request, timeout=timeout_seconds) as response:
            status_code = int(response.status)
            effective_url = str(response.geturl())
            if effective_url != prepared.url or 300 <= status_code <= 399:
                return {
                    "ok": False,
                    "statusCode": status_code,
                    "text": "HTTP redirect/effective URL change blocked",
                    "json": {},
                    "redirectBlocked": True,
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            text = response.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= status_code < 300,
                "statusCode": status_code,
                "text": text,
                "json": _parse_json(text),
                "trCont": str(response.headers.get("tr_cont") or ""),
                "physicalAttemptCount": 1,
                "retryAllowed": False,
            }
    except urllib.error.HTTPError as exc:
        try:
            if 300 <= int(exc.code) <= 399:
                return {
                    "ok": False,
                    "statusCode": int(exc.code),
                    "text": "HTTP redirect blocked before follow",
                    "json": {},
                    "redirectBlocked": True,
                    "physicalAttemptCount": 1,
                    "retryAllowed": False,
                }
            text = exc.read().decode("utf-8", errors="replace")
            return {
                "ok": False,
                "statusCode": int(exc.code),
                "text": text,
                "json": _parse_json(text),
                "trCont": (
                    str(exc.headers.get("tr_cont") or "")
                    if exc.headers
                    else ""
                ),
                "physicalAttemptCount": 1,
                "retryAllowed": False,
            }
        finally:
            exc.close()
    except (urllib.error.URLError, TimeoutError) as exc:
        reason = getattr(exc, "reason", exc)
        return {
            "ok": False,
            "statusCode": 0,
            "text": str(reason),
            "json": {},
            "physicalAttemptCount": 1,
            "retryAllowed": False,
        }


__all__ = [
    "PreparedRequest",
    "UPBIT_BASE_URL",
    "UPBIT_READ_ONLY_GET_ENDPOINTS",
    "env_value",
    "send_prepared_request",
]
