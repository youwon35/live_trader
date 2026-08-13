from __future__ import annotations

"""Official read/stream sources for the isolated Upbit functional lane.

Nothing in this module sends an order.  The private source performs only an
authenticated WebSocket subscription to ``myOrder`` plus ping/pong liveness;
the candle source performs only public GET requests for finalized 5-minute
candles.  Both are dependency-injectable so production behavior can be proven
without touching the network in unit tests.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import secrets
import threading
import time
from typing import Any, Callable, Mapping, Sequence
import urllib.parse
import urllib.request

from .upbit_continuous_functional import SYMBOL, UpbitFunctionalBlocked
from .upbit_functional_transport import (
    DurableUpbitMyOrderJournal,
    build_upbit_functional_authorization,
    upbit_credential_fingerprint,
)


_PRIVATE_WS_URL = "wss://api.upbit.com/websocket/v1/private"
_PUBLIC_API_URL = "https://api.upbit.com"
_CANDLE_ENDPOINT = "/v1/candles/minutes/5"
_OPCODE_TEXT = 1
_OPCODE_BINARY = 2
_OPCODE_PING = 9
_OPCODE_PONG = 10


def _text(value: object) -> str:
    return str(value or "").strip()


def _utc(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise UpbitFunctionalBlocked(
                f"upbit-functional-source-{label}-invalid"
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UpbitFunctionalBlocked(
            f"upbit-functional-source-{label}-timezone-missing"
        )
    return parsed.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise UpbitFunctionalBlocked(
            f"upbit-functional-source-{label}-invalid"
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise UpbitFunctionalBlocked(
            f"upbit-functional-source-{label}-invalid"
        )
    return parsed


def _default_socket_factory(authorization: str):
    import websocket  # type: ignore[import-not-found]

    return websocket.create_connection(
        _PRIVATE_WS_URL,
        header=[f"Authorization: {authorization}"],
        timeout=2,
        enable_multithread=True,
    )


class OfficialUpbitFunctionalMyOrderPump:
    """Single-owner authenticated ``myOrder`` socket with durable journaling.

    Upbit documents that a quiet ``myOrder`` subscription emits no initial
    data/ACK.  Authentication is therefore proved by the successful private
    WebSocket upgrade, then subscription errors are rejected and a server PONG
    must be observed before this method returns.  Only received frames/PONGs,
    never locally sent heartbeats, advance liveness.
    """

    def __init__(
        self,
        *,
        expected_account_fingerprint: str,
        clock: Callable[[], datetime],
        socket_factory: Callable[[str], Any] = _default_socket_factory,
        credential_reader: Callable[[], tuple[str, str]] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.expected_account_fingerprint = _text(
            expected_account_fingerprint
        ).lower()
        self.clock = clock
        self.socket_factory = socket_factory
        self.credential_reader = credential_reader or (
            lambda: (
                _text(os.getenv("UPBIT_ACCESS_KEY")),
                _text(os.getenv("UPBIT_SECRET_KEY")),
            )
        )
        self.monotonic = monotonic
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._socket: Any = None
        self._journal: DurableUpbitMyOrderJournal | None = None
        self._session_id = ""
        self._writer: dict[str, Any] = {}
        self._writer_token_hash = ""
        self._last_frame_at: datetime | None = None
        self._connected = False
        self._authenticated = False
        self._subscribed = False
        self._pending: list[dict[str, Any]] = []
        self._intentional_close = False
        self._terminal_probe = b""
        self._terminal_barrier = threading.Event()
        self._terminal_cutoff = False

    def handshake(
        self,
        *,
        session_id: str,
        journal: DurableUpbitMyOrderJournal,
        writer_authority: Mapping[str, Any],
        cleanup_only: bool = False,
    ) -> dict[str, Any]:
        del cleanup_only  # The stream is read-only in both lifecycle phases.
        with self._lock:
            if self._socket is not None or self._thread is not None:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-pump-already-owned"
                )
        access_key, secret_key = self.credential_reader()
        if (
            not access_key
            or not secret_key
            or not secrets.compare_digest(
                upbit_credential_fingerprint(access_key),
                self.expected_account_fingerprint,
            )
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-myorder-pump-credential-mismatch"
            )
        token = _text(writer_authority.get("writerToken"))
        generation = int(writer_authority.get("writerGeneration") or 0)
        if not token or generation <= 0:
            raise UpbitFunctionalBlocked(
                "upbit-functional-myorder-pump-writer-invalid"
            )
        authorization = build_upbit_functional_authorization(
            access_key, secret_key, ()
        )
        socket = self.socket_factory(authorization)
        ticket = "upbit-functional-" + secrets.token_hex(16)
        subscription = [
            {"ticket": ticket},
            {"type": "myOrder", "codes": [SYMBOL]},
            {"format": "DEFAULT"},
        ]
        try:
            socket.send(
                json.dumps(subscription, separators=(",", ":"))
            )
            probe = secrets.token_bytes(16)
            socket.ping(probe)
            pending: list[dict[str, Any]] = []
            deadline = self.monotonic() + 5.0
            while self.monotonic() <= deadline:
                opcode, raw = socket.recv_data(control_frame=True)
                if opcode == _OPCODE_PONG:
                    if bytes(raw or b"") != probe:
                        raise UpbitFunctionalBlocked(
                            "upbit-functional-myorder-pong-mismatch"
                        )
                    break
                if opcode == _OPCODE_PING:
                    socket.pong(raw)
                    continue
                if opcode not in {_OPCODE_TEXT, _OPCODE_BINARY}:
                    continue
                payload = self._payload(raw)
                if isinstance(payload.get("error"), Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-myorder-subscription-rejected"
                    )
                pending.append(payload)
            else:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-authenticated-pong-timeout"
                )
        except Exception:
            try:
                socket.close()
            finally:
                pass
            raise
        now = _utc(self.clock(), "myorder-handshake-time")
        with self._lock:
            self._socket = socket
            self._journal = journal
            self._session_id = _text(session_id)
            self._writer = dict(writer_authority)
            self._writer_token_hash = hashlib.sha256(
                token.encode("utf-8")
            ).hexdigest()
            self._last_frame_at = now
            self._connected = True
            self._authenticated = True
            self._subscribed = True
            self._pending = pending
            self._intentional_close = False
            self._terminal_probe = b""
            self._terminal_barrier.clear()
            self._terminal_cutoff = False
            self._stop.clear()
        return {
            **self._liveness_snapshot(),
            "livenessReader": self.liveness,
            "closePump": self.close,
        }

    @staticmethod
    def _payload(raw: object) -> dict[str, Any]:
        try:
            value = json.loads(
                raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-functional-myorder-frame-invalid"
            ) from exc
        if not isinstance(value, Mapping):
            raise UpbitFunctionalBlocked(
                "upbit-functional-myorder-frame-invalid"
            )
        return dict(value)

    def _liveness_snapshot(self) -> dict[str, Any]:
        with self._lock:
            last = self._last_frame_at
            return {
                "sessionId": self._session_id,
                "writerGeneration": int(
                    self._writer.get("writerGeneration") or 0
                ),
                "writerTokenHash": self._writer_token_hash,
                "connected": self._connected,
                "authenticated": self._authenticated,
                "myOrderSubscribed": self._subscribed,
                "lastFrameAt": _utc_text(last) if last is not None else "",
            }

    def liveness(self) -> dict[str, Any]:
        self._start_once()
        snapshot = self._liveness_snapshot()
        last = _utc(snapshot.get("lastFrameAt"), "myorder-last-frame-at")
        now = _utc(self.clock(), "myorder-liveness-time")
        if now - last > timedelta(seconds=5):
            with self._lock:
                socket = self._socket
            if socket is None:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-pump-socket-missing"
                )
            socket.ping(secrets.token_bytes(16))
            deadline = time.monotonic() + 5.0
            while time.monotonic() <= deadline:
                current = self._liveness_snapshot()
                observed = _utc(
                    current.get("lastFrameAt"),
                    "myorder-last-frame-at",
                )
                if observed > last:
                    return current
                time.sleep(0.01)
        return self._liveness_snapshot()

    def _start_once(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            if self._socket is None or self._journal is None:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-pump-not-handshaken"
                )
            for payload in self._pending:
                self._journal.ingest(
                    self._session_id,
                    payload,
                    writer_token=_text(self._writer.get("writerToken")),
                    writer_generation=int(
                        self._writer.get("writerGeneration") or 0
                    ),
                )
            self._pending.clear()
            thread = threading.Thread(
                target=self._run,
                name=f"upbit-functional-myorder-{self._session_id}",
                daemon=True,
            )
            self._thread = thread
            thread.start()

    def _observe_frame(self) -> None:
        now = _utc(self.clock(), "myorder-frame-time")
        with self._lock:
            self._last_frame_at = now

    def _run(self) -> None:
        last_ping = self.monotonic()
        try:
            while not self._stop.is_set():
                try:
                    opcode, raw = self._socket.recv_data(control_frame=True)
                except Exception as exc:
                    if type(exc).__name__ != "WebSocketTimeoutException":
                        raise
                    if self.monotonic() - last_ping >= 5.0:
                        self._socket.ping(secrets.token_bytes(16))
                        last_ping = self.monotonic()
                    continue
                if opcode == _OPCODE_PONG:
                    self._observe_frame()
                    with self._lock:
                        terminal_probe = self._terminal_probe
                    if terminal_probe and bytes(raw or b"") == terminal_probe:
                        # recv_data and journal.ingest are strictly serialized
                        # in this one owner thread.  Reaching the terminal PONG
                        # therefore proves every earlier received data frame
                        # has committed before the cutoff cursor is sealed.
                        self._journal.observe(
                            self._session_id,
                            writer_token=_text(
                                self._writer.get("writerToken")
                            ),
                            writer_generation=int(
                                self._writer.get("writerGeneration") or 0
                            ),
                        )
                        with self._lock:
                            self._terminal_cutoff = True
                            self._terminal_barrier.set()
                        return
                    continue
                if opcode == _OPCODE_PING:
                    self._socket.pong(raw)
                    self._observe_frame()
                    continue
                if opcode not in {_OPCODE_TEXT, _OPCODE_BINARY}:
                    continue
                payload = self._payload(raw)
                if isinstance(payload.get("error"), Mapping):
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-myorder-subscription-rejected"
                    )
                self._journal.ingest(
                    self._session_id,
                    payload,
                    writer_token=_text(self._writer.get("writerToken")),
                    writer_generation=int(
                        self._writer.get("writerGeneration") or 0
                    ),
                )
                self._observe_frame()
        except Exception as exc:
            with self._lock:
                self._connected = False
            if not self._intentional_close:
                try:
                    self._journal.mark_gap(
                        self._session_id,
                        detail=f"owned-myorder-pump-failed:{type(exc).__name__}",
                        writer_token=_text(self._writer.get("writerToken")),
                        writer_generation=int(
                            self._writer.get("writerGeneration") or 0
                        ),
                    )
                except Exception:
                    pass

    def terminal_barrier(self, *, session_id: str) -> dict[str, Any]:
        """Drain received frames through a server PONG, then freeze intake.

        The caller performs fresh REST reconciliation only after this method
        returns.  Events after the deterministic PONG cutoff are therefore
        visible through REST rather than being silently queued behind a stale
        journal seal.
        """

        self._start_once()
        with self._lock:
            if (
                self._terminal_cutoff
                or not self._connected
                or not self._authenticated
                or not self._subscribed
                or self._session_id != _text(session_id)
                or self._socket is None
                or self._thread is None
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-terminal-barrier-state-invalid"
                )
            probe = secrets.token_bytes(24)
            self._terminal_probe = probe
            self._terminal_barrier.clear()
            socket = self._socket
            generation = int(self._writer.get("writerGeneration") or 0)
            token_hash = self._writer_token_hash
        try:
            socket.ping(probe)
            if not self._terminal_barrier.wait(timeout=5):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-myorder-terminal-barrier-timeout"
                )
        except Exception:
            try:
                self._journal.mark_gap(
                    self._session_id,
                    detail="terminal-pong-barrier-failed",
                    writer_token=_text(self._writer.get("writerToken")),
                    writer_generation=generation,
                )
            except Exception:
                pass
            raise
        with self._lock:
            self._intentional_close = True
            self._stop.set()
            thread = self._thread
            self._connected = False
        try:
            socket.close()
        except Exception:
            pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3)
            if thread.is_alive():
                try:
                    self._journal.mark_gap(
                        self._session_id,
                        detail="terminal-pong-barrier-thread-not-drained",
                        writer_token=_text(self._writer.get("writerToken")),
                        writer_generation=generation,
                    )
                finally:
                    raise UpbitFunctionalBlocked(
                        "upbit-functional-myorder-terminal-barrier-not-drained"
                    )
        return {
            "cutoffEstablished": True,
            "sessionId": self._session_id,
            "writerGeneration": generation,
            "writerTokenHash": token_hash,
        }

    def close(self) -> None:
        with self._lock:
            self._intentional_close = True
            self._stop.set()
            socket = self._socket
            thread = self._thread
            self._connected = False
        if socket is not None:
            try:
                socket.close()
            except Exception:
                pass
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3)


def _default_public_get(
    endpoint: str, query: Sequence[tuple[str, str]]
) -> object:
    url = f"{_PUBLIC_API_URL}{endpoint}?{urllib.parse.urlencode(tuple(query))}"
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


class OfficialUpbitFinalizedFiveMinuteWindowReader:
    """Build one strict 11-bar finalized window from Upbit's public REST API."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime],
        public_get: Callable[
            [str, Sequence[tuple[str, str]]], object
        ] = _default_public_get,
    ) -> None:
        self.clock = clock
        self.public_get = public_get

    def __call__(self) -> dict[str, Any]:
        now = _utc(self.clock(), "candle-current-time")
        endpoint = _CANDLE_ENDPOINT
        query = (("market", SYMBOL), ("count", "20"))
        raw = self.public_get(
            endpoint,
            query,
        )
        if not isinstance(raw, list) or any(
            not isinstance(row, Mapping) for row in raw
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-source-candle-response-invalid"
            )
        by_start: dict[datetime, dict[str, Any]] = {}
        response_timestamps: list[int] = []
        for source in raw:
            if _text(source.get("market")).upper() != SYMBOL:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-market-mismatch"
                )
            try:
                response_timestamp = int(source.get("timestamp"))
            except (TypeError, ValueError) as exc:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-server-time-missing"
                ) from exc
            if (
                response_timestamp <= 0
                or datetime.fromtimestamp(
                    response_timestamp / 1000, tz=timezone.utc
                )
                > now + timedelta(seconds=15)
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-server-time-invalid"
                )
            response_timestamps.append(response_timestamp)
            start_text = _text(source.get("candle_date_time_utc"))
            try:
                start = datetime.fromisoformat(start_text).replace(
                    tzinfo=timezone.utc
                )
            except ValueError as exc:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-time-invalid"
                ) from exc
            if (
                start.second != 0
                or start.microsecond != 0
                or start.minute % 5 != 0
            ):
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-boundary-invalid"
                )
            closed_at = start + timedelta(minutes=5)
            if closed_at > now:
                continue
            if start in by_start:
                raise UpbitFunctionalBlocked(
                    "upbit-functional-source-candle-duplicate"
                )
            close = _decimal(
                source.get("trade_price"), "candle-trade-price"
            )
            by_start[start] = {
                "barId": "upbit-rest-five-minute-"
                + start.strftime("%Y%m%dT%H%M%SZ"),
                "closedAt": _utc_text(closed_at),
                "close": format(close, "f"),
                "finalized": True,
                "closed": True,
            }
        ordered = [by_start[key] for key in sorted(by_start)][-11:]
        if len(ordered) != 11:
            raise UpbitFunctionalBlocked(
                "upbit-functional-source-finalized-history-incomplete"
            )
        times = [_utc(row["closedAt"], "candle-closed-at") for row in ordered]
        if any(
            current - previous != timedelta(minutes=5)
            for previous, current in zip(times, times[1:])
        ):
            raise UpbitFunctionalBlocked(
                "upbit-functional-source-finalized-history-not-contiguous"
            )
        try:
            raw_response = json.loads(
                json.dumps(
                    raw,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise UpbitFunctionalBlocked(
                "upbit-functional-source-candle-response-not-canonical"
            ) from exc
        response_hash = hashlib.sha256(
            json.dumps(
                raw_response,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return {
            "schemaVersion": "upbit-official-finalized-5m-window-v1",
            "symbol": SYMBOL,
            "interval": "5m",
            "source": "UPBIT_REST",
            "finalized": True,
            "closed": True,
            "barId": ordered[-1]["barId"],
            "closedAt": ordered[-1]["closedAt"],
            "bars": ordered,
            "officialCandleEvidence": {
                "schemaVersion": "upbit-official-candle-rest-evidence/v1",
                "origin": _PUBLIC_API_URL,
                "endpoint": endpoint,
                "orderedQuery": [list(item) for item in query],
                "observedAt": _utc_text(now),
                "maxResponseTimestampMs": max(response_timestamps),
                "rawResponse": raw_response,
                "rawResponseHash": response_hash,
            },
        }


__all__ = [
    "OfficialUpbitFinalizedFiveMinuteWindowReader",
    "OfficialUpbitFunctionalMyOrderPump",
]
