from __future__ import annotations

from dataclasses import dataclass
from http.cookies import CookieError, SimpleCookie
import ipaddress
import secrets
import threading
from typing import Mapping
from urllib.parse import urlsplit
import time


CSRF_HEADER = "X-LiveTrader-CSRF"
APP_SESSION_COOKIE = "LiveTrader-AppSession"
_TOKEN_BYTES = 32


class FunctionalHttpSessionError(RuntimeError):
    """A functional or safety HTTP request did not cross the trusted app edge."""


def normalize_loopback_bind_host(host: object) -> str:
    raw = str(host or "")
    normalized = raw.strip()
    if raw != normalized:
        raise FunctionalHttpSessionError(
            "Live Trader production HTTP bind host is not canonical"
        )
    if normalized not in {"127.0.0.1", "::1"}:
        raise FunctionalHttpSessionError(
            "Live Trader production HTTP bind host must be exact 127.0.0.1 or ::1"
        )
    return normalized


def _authority(host: str, port: int) -> str:
    return f"[{host}]:{port}" if host == "::1" else f"{host}:{port}"


@dataclass(frozen=True)
class FunctionalHttpSessionAuthority:
    host: str
    port: int
    app_session_token: str
    csrf_token: str
    bootstrap_nonce: str
    _bootstrap_lock: threading.Lock
    _bootstrap_consumed: list[bool]
    bootstrap_expires_epoch: float

    @classmethod
    def mint(cls, *, host: object, port: object) -> "FunctionalHttpSessionAuthority":
        normalized_host = normalize_loopback_bind_host(host)
        try:
            normalized_port = int(port)
        except (TypeError, ValueError) as exc:
            raise FunctionalHttpSessionError("HTTP bind port is invalid") from exc
        if not 0 < normalized_port <= 65535:
            raise FunctionalHttpSessionError("HTTP bind port is invalid")
        return cls(
            host=normalized_host,
            port=normalized_port,
            app_session_token=secrets.token_urlsafe(_TOKEN_BYTES),
            csrf_token=secrets.token_urlsafe(_TOKEN_BYTES),
            bootstrap_nonce=secrets.token_urlsafe(_TOKEN_BYTES),
            _bootstrap_lock=threading.Lock(),
            _bootstrap_consumed=[False],
            bootstrap_expires_epoch=time.time() + 30.0,
        )

    @property
    def expected_host_header(self) -> str:
        return _authority(self.host, self.port)

    @property
    def expected_origin(self) -> str:
        return f"http://{self.expected_host_header}"

    def trusted_native_bootstrap(self) -> dict[str, str]:
        """Return only the independent CSRF secret through pywebview."""

        return {
            "csrfToken": self.csrf_token,
            "csrfHeader": CSRF_HEADER,
            "origin": self.expected_origin,
        }

    @property
    def native_bootstrap_url(self) -> str:
        return (
            f"{self.expected_origin}/__lt_native_bootstrap?"
            f"nonce={self.bootstrap_nonce}"
        )

    @property
    def set_cookie_header(self) -> str:
        return (
            f"{APP_SESSION_COOKIE}={self.app_session_token}; "
            "HttpOnly; SameSite=Strict; Path=/api/"
        )

    def consume_native_bootstrap(
        self,
        *,
        nonce: object,
        host_header: object,
        peer_host: object,
        now_epoch: float | None = None,
    ) -> bool:
        try:
            peer = ipaddress.ip_address(str(peer_host or ""))
        except ValueError:
            return False
        if (
            not peer.is_loopback
            or str(host_header or "") != self.expected_host_header
            or float(time.time() if now_epoch is None else now_epoch)
            > self.bootstrap_expires_epoch
            or not secrets.compare_digest(
                str(nonce or ""), self.bootstrap_nonce
            )
        ):
            return False
        with self._bootstrap_lock:
            if float(time.time() if now_epoch is None else now_epoch) > self.bootstrap_expires_epoch:
                return False
            if self._bootstrap_consumed[0]:
                return False
            self._bootstrap_consumed[0] = True
            return True

    def assert_request(
        self,
        *,
        headers: Mapping[str, object],
        peer_host: object,
        require_origin: bool,
    ) -> None:
        try:
            peer = ipaddress.ip_address(str(peer_host or "").strip())
        except ValueError as exc:
            raise FunctionalHttpSessionError(
                "functional HTTP peer is not an IP loopback address"
            ) from exc
        if not peer.is_loopback:
            raise FunctionalHttpSessionError(
                "functional HTTP peer is not loopback"
            )
        get_all = getattr(headers, "get_all", None)
        if callable(get_all):
            host_headers = list(get_all("Host") or [])
        elif headers.get("Host") is not None:
            host_headers = [str(headers.get("Host"))]
        else:
            host_headers = []
        if len(host_headers) != 1:
            raise FunctionalHttpSessionError(
                "functional HTTP Host header is missing or duplicated"
            )
        host_header = str(host_headers[0]).strip()
        if not secrets.compare_digest(host_header, self.expected_host_header):
            raise FunctionalHttpSessionError(
                "functional HTTP Host header changed"
            )
        cookie_headers = []
        if callable(get_all):
            cookie_headers = list(get_all("Cookie") or [])
        elif headers.get("Cookie") is not None:
            cookie_headers = [str(headers.get("Cookie"))]
        if len(cookie_headers) != 1:
            raise FunctionalHttpSessionError(
                "trusted application session cookie is missing or duplicated"
            )
        cookie_text = cookie_headers[0]
        if cookie_text.count(APP_SESSION_COOKIE + "=") != 1:
            raise FunctionalHttpSessionError(
                "trusted application session cookie name is duplicated"
            )
        parsed_cookie = SimpleCookie()
        try:
            parsed_cookie.load(cookie_text)
        except CookieError as exc:
            raise FunctionalHttpSessionError(
                "trusted application session cookie is malformed"
            ) from exc
        morsel = parsed_cookie.get(APP_SESSION_COOKIE)
        app_token = morsel.value if morsel is not None else ""
        if not secrets.compare_digest(app_token, self.app_session_token):
            raise FunctionalHttpSessionError(
                "trusted application session token is missing or changed"
            )
        if require_origin:
            csrf_headers = []
            if callable(get_all):
                csrf_headers = list(get_all(CSRF_HEADER) or [])
            elif headers.get(CSRF_HEADER) is not None:
                csrf_headers = [str(headers.get(CSRF_HEADER))]
            if len(csrf_headers) != 1 or not secrets.compare_digest(
                csrf_headers[0], self.csrf_token
            ):
                raise FunctionalHttpSessionError(
                    "functional HTTP CSRF token is missing, duplicated, or changed"
                )
        if require_origin:
            if callable(get_all):
                origin_headers = list(get_all("Origin") or [])
            elif headers.get("Origin") is not None:
                origin_headers = [str(headers.get("Origin"))]
            else:
                origin_headers = []
            if len(origin_headers) != 1:
                raise FunctionalHttpSessionError(
                    "functional HTTP Origin is missing or duplicated"
                )
            origin = str(origin_headers[0]).strip()
            parsed = urlsplit(origin)
            if (
                origin != self.expected_origin
                or parsed.scheme != "http"
                or parsed.hostname != self.host
                or parsed.port != self.port
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
            ):
                raise FunctionalHttpSessionError(
                    "functional HTTP Origin is missing or not exact loopback"
                )


__all__ = [
    "APP_SESSION_COOKIE",
    "CSRF_HEADER",
    "FunctionalHttpSessionAuthority",
    "FunctionalHttpSessionError",
    "normalize_loopback_bind_host",
]
