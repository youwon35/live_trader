from __future__ import annotations

import base64
from collections import deque
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Iterable
import urllib.error
import urllib.request
import uuid

from .live_adapters import binance_timestamp_ms, refresh_binance_time_offset
from trading_runtime.kis_rate_limiter import GLOBAL_KIS_REST_LIMITERS
from trading_runtime.kis_websocket_owner import GLOBAL_KIS_WEBSOCKET_OWNERS


_AUTHENTICATED_HTTP_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_BINANCE_FUTURES_PRIVATE_REST_BASE = "https://fapi.binance.com"
_BINANCE_FUTURES_PRIVATE_STREAM_BASE = "wss://fstream.binance.com/ws"


class _AuthenticatedBrokerNoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep authenticated broker requests pinned to their original URL."""

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


def _open_authenticated_broker_request(
    request: urllib.request.Request,
    *,
    timeout: float,
) -> Any:
    """Open once without redirects and reject any effective-URL change."""

    expected_url = str(request.full_url)
    opener = urllib.request.build_opener(_AuthenticatedBrokerNoRedirectHandler())
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if int(exc.code or 0) in _AUTHENTICATED_HTTP_REDIRECT_STATUSES:
            exc.close()
            raise RuntimeError(
                "Authenticated broker HTTP redirect is forbidden"
            ) from None
        raise

    status = getattr(response, "status", None)
    if not isinstance(status, int):
        getcode = getattr(response, "getcode", None)
        status = getcode() if callable(getcode) else None
    geturl = getattr(response, "geturl", None)
    effective_url = geturl() if callable(geturl) else None
    if (
        status in _AUTHENTICATED_HTTP_REDIRECT_STATUSES
        or not isinstance(effective_url, str)
        or effective_url != expected_url
    ):
        response.close()
        raise RuntimeError(
            "Authenticated broker HTTP response URL changed"
        )
    return response


def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def upbit_websocket_token(access_key: str, secret_key: str, nonce: str | None = None) -> str:
    header = _b64url(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url(json.dumps({"access_key": access_key, "nonce": nonce or str(uuid.uuid4())}, separators=(",", ":")).encode())
    signature = hmac.new(secret_key.encode(), f"{header}.{payload}".encode(), hashlib.sha512).digest()
    return f"{header}.{payload}.{_b64url(signature)}"


def binance_stream_subscription_params(api_key: str, api_secret: str) -> dict[str, Any]:
    refresh_binance_time_offset()
    params: dict[str, Any] = {"apiKey": api_key, "timestamp": binance_timestamp_ms()}
    signing_payload = "&".join(f"{key}={params[key]}" for key in sorted(params))
    params["signature"] = hmac.new(api_secret.encode(), signing_payload.encode(), hashlib.sha256).hexdigest()
    return params


def parse_upbit_my_order(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or str(payload.get("type") or "") not in {"myOrder", "my_order"}:
        return None
    trade_uuid = str(payload.get("trade_uuid") or payload.get("tradeUuid") or "")
    order_uuid = str(payload.get("uuid") or "")
    market = str(payload.get("code") or payload.get("market") or "").upper()
    state = str(payload.get("state") or "").lower()
    executed_value = next(
        (
            payload.get(key)
            for key in ("executed_volume", "executedVolume")
            if payload.get(key) not in {None, ""}
        ),
        None,
    )
    paid_fee_value = next(
        (
            payload.get(key)
            for key in ("paid_fee", "paidFee")
            if payload.get(key) not in {None, ""}
        ),
        None,
    )
    executed = _float(executed_value)
    paid_fee = _float(paid_fee_value)
    trade_volume_value = next(
        (
            payload.get(key)
            for key in ("volume", "trade_volume", "tradeVolume")
            if payload.get(key) not in {None, ""}
        ),
        None,
    )
    trade_fee_value = next(
        (
            payload.get(key)
            for key in ("trade_fee", "tradeFee")
            if payload.get(key) not in {None, ""}
        ),
        None,
    )
    is_trade_delta = (
        state == "trade"
        and bool(trade_uuid)
        and trade_volume_value is not None
    )
    quantity = _float(trade_volume_value) if is_trade_delta else executed
    fee_is_delta = is_trade_delta and trade_fee_value is not None
    fee = _float(trade_fee_value) if fee_is_delta else paid_fee
    remaining_value = payload.get("remaining_volume")
    if remaining_value in {None, ""}:
        remaining_value = payload.get("remainingVolume")
    remaining_volume = (
        _float(remaining_value)
        if remaining_value not in {None, ""}
        else None
    )
    normalized_state = {
        "done": "filled",
        "cancel": "canceled",
        "wait": "accepted",
        "watch": "accepted",
    }.get(state, state or "accepted")
    if state == "trade":
        normalized_state = (
            "filled"
            if remaining_volume is not None and remaining_volume <= 0
            else "partially_filled"
        )
    price = _float(
        payload.get("trade_price")
        or payload.get("tradePrice")
        or (
            payload.get("price")
            if is_trade_delta
            else payload.get("avg_price")
            or payload.get("avgPrice")
            or payload.get("price")
        )
        or 0
    )
    event_component = (
        f"{trade_uuid}:{state}"
        if trade_uuid
        else (
            f"{state}:{executed:.16g}:{paid_fee:.16g}:"
            f"{payload.get('trades_count') or payload.get('tradesCount') or 0}"
        )
    )
    return {
        "event_id": f"upbit:{order_uuid}:{event_component}",
        "broker_id": "upbit",
        "order_id": str(payload.get("identifier") or ""),
        "broker_order_id": order_uuid,
        "symbol": market,
        "side": "BUY" if str(payload.get("ask_bid") or payload.get("side") or "").upper() in {"BID", "BUY"} else "SELL",
        "quantity": quantity,
        "price": price,
        "fee": fee,
        "state": normalized_state,
        "occurred_at": datetime.now(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "raw_type": "upbit_my_order",
        # Upbit exposes order-level cumulative watermarks alongside an
        # execution-level volume/fee.  Keeping both contracts explicit lets
        # the state layer suppress replayed/out-of-order fills without
        # changing Binance's already-incremental execution contract.
        "quantity_mode": "delta" if is_trade_delta else "cumulative",
        "fee_mode": "delta" if fee_is_delta else "cumulative",
        "cumulative_quantity": (
            executed if executed_value is not None else None
        ),
        "cumulative_fee": (
            paid_fee if paid_fee_value is not None else None
        ),
        "cumulative_average_price": _float(
            payload.get("avg_price")
            or payload.get("avgPrice")
            or 0
        ),
    }


def parse_binance_execution_report(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    event = payload.get("event") if isinstance(payload.get("event"), dict) else payload
    if str(event.get("e") or event.get("eventType") or "") != "executionReport":
        return None
    order_id = str(event.get("i") or event.get("orderId") or "")
    client_order_id = str(event.get("c") or event.get("clientOrderId") or "")
    execution_type = str(event.get("x") or event.get("executionType") or "").upper()
    order_status = str(event.get("X") or event.get("orderStatus") or "").upper()
    state = {
        "FILLED": "filled",
        "PARTIALLY_FILLED": "partially_filled",
        "CANCELED": "canceled",
        "REJECTED": "rejected",
        "EXPIRED": "expired",
        "NEW": "accepted",
    }.get(order_status, execution_type.lower() or "accepted")
    quantity = _float(event.get("l") or event.get("lastExecutedQuantity") or 0)
    price = _float(event.get("L") or event.get("lastExecutedPrice") or 0)
    trade_id = str(event.get("t") or event.get("tradeId") or "")
    return {
        "event_id": f"binance:{order_id}:{trade_id or execution_type}:{order_status}:{quantity}",
        "broker_id": "binance",
        "order_id": client_order_id,
        "broker_order_id": order_id,
        "symbol": str(event.get("s") or event.get("symbol") or "").upper(),
        "side": str(event.get("S") or event.get("side") or "").upper(),
        "quantity": quantity,
        "price": price,
        "fee": _float(event.get("n") or event.get("commissionAmount") or 0),
        "state": state,
        "occurred_at": datetime.fromtimestamp(_float(event.get("E") or event.get("eventTime") or 0) / 1000).strftime("%Y-%m-%d %H:%M:%S") if _float(event.get("E") or event.get("eventTime") or 0) > 0 else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_type": "binance_execution_report",
    }


def parse_binance_futures_order_update(
    payload: Any,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    event = (
        payload.get("data")
        if isinstance(payload.get("data"), dict)
        else payload
    )
    if str(event.get("e") or "") != "ORDER_TRADE_UPDATE":
        return None
    order = event.get("o") if isinstance(event.get("o"), dict) else {}
    order_id = str(order.get("i") or "")
    client_order_id = str(order.get("c") or "")
    execution_type = str(order.get("x") or "").upper()
    order_status = str(order.get("X") or "").upper()
    state = {
        "FILLED": "filled",
        "PARTIALLY_FILLED": "partially_filled",
        "CANCELED": "canceled",
        "REJECTED": "rejected",
        "EXPIRED": "expired",
        "NEW": "accepted",
    }.get(order_status, execution_type.lower() or "accepted")
    quantity = _float(order.get("l") or 0)
    price = _float(order.get("L") or order.get("ap") or 0)
    trade_id = str(order.get("t") or "")
    event_time = _float(event.get("E") or order.get("T") or 0)
    return {
        "event_id": (
            f"binance-futures:{order_id}:"
            f"{trade_id or execution_type}:{order_status}:{quantity}"
        ),
        "broker_id": "binance-futures",
        "order_id": client_order_id,
        "broker_order_id": order_id,
        "symbol": str(order.get("s") or "").upper(),
        "side": str(order.get("S") or "").upper(),
        "position_side": str(order.get("ps") or "BOTH").upper(),
        "reduce_only": order.get("R") is True,
        "quantity": quantity,
        "price": price,
        "fee": _float(order.get("n") or 0),
        "state": state,
        "occurred_at": (
            datetime.fromtimestamp(event_time / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if event_time > 0
            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ),
        "raw_type": "binance_futures_order_trade_update",
    }


def parse_kis_domestic_execution(fields: list[str]) -> dict[str, Any] | None:
    if len(fields) < 14:
        return None
    order_no = str(fields[2])
    symbol = str(fields[8]).upper()
    quantity = _float(fields[9])
    price = _float(fields[10])
    rejected = str(fields[12]).upper() == "Y"
    filled = str(fields[13]) in {"1", "2", "Y"} or quantity > 0
    return {
        "event_id": f"kis:{order_no}:{fields[11]}:{quantity}:{price}",
        "broker_id": "kis",
        "order_id": "",
        "broker_order_id": order_no,
        "symbol": f"{symbol}.KS" if symbol.isdigit() and len(symbol) == 6 else symbol,
        "side": "SELL" if str(fields[4]) in {"01", "1", "S"} else "BUY",
        "quantity": quantity,
        "price": price,
        "fee": 0.0,
        "state": "rejected" if rejected else "filled" if filled else "accepted",
        "occurred_at": datetime.now(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "raw_type": "kis_domestic_execution",
    }


def parse_kis_overseas_execution(fields: list[str]) -> dict[str, Any] | None:
    """Normalize official H0GSCNI0/H0GSCNI9 25-column notice."""
    if len(fields) < 25:
        return None
    order_no = str(fields[2])
    symbol = str(fields[7]).upper()
    quantity = _float(fields[8])
    price = _float(fields[9]) or _float(fields[24])
    rejected = str(fields[11]).upper() == "Y"
    filled = str(fields[12]) in {"1", "2", "Y"} or quantity > 0
    return {
        "event_id": f"kis-overseas:{order_no}:{fields[10]}:{quantity}:{price}",
        "broker_id": "kis",
        "order_id": "",
        "broker_order_id": order_no,
        "symbol": symbol,
        "side": "SELL" if str(fields[4]) in {"01", "1", "S"} else "BUY",
        "quantity": quantity,
        "price": price,
        "fee": 0.0,
        "state": "rejected" if rejected else "filled" if filled else "accepted",
        "occurred_at": datetime.now(timezone.utc).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z"),
        "raw_type": "kis_overseas_execution",
    }


def _float(value: Any) -> float:
    try:
        return float(str(value or "0").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


class ExecutionStreamManager:
    """Reconnectable private order/execution streams with a durable audit log."""

    def __init__(
        self,
        data_root: Path,
        *,
        log_max_bytes: int = 5 * 1024 * 1024,
        log_backup_count: int = 3,
        binance_functional_stream_bridge: Any | None = None,
    ) -> None:
        self.data_root = Path(data_root)
        self.log_max_bytes = max(1024, int(log_max_bytes))
        self.log_backup_count = max(1, int(log_backup_count))
        self._lock = threading.RLock()
        self._stop_events: dict[str, threading.Event] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._events: dict[str, deque[dict[str, Any]]] = {
            "kis": deque(maxlen=1000),
            "binance": deque(maxlen=1000),
            "binance-futures": deque(maxlen=1000),
            "upbit": deque(maxlen=1000),
        }
        self._status: dict[str, dict[str, Any]] = {}
        # Optional and fail-closed.  Ordinary Binance stream consumers retain
        # their existing queue/audit behavior when no functional bridge is
        # provided; the continuous functional lane never infers proof from
        # those ordinary queues.
        self._binance_functional_stream_bridge = (
            binance_functional_stream_bridge
        )
        self._binance_functional_receive_lock = threading.RLock()
        self._binance_functional_terminal_condition = threading.Condition()
        self._binance_functional_terminal_request_id = ""
        self._binance_functional_terminal_done = False
        self._binance_functional_terminal_error = ""
        self._binance_functional_terminal_result: dict[str, Any] = {}

    def begin_binance_functional_terminal_barrier(
        self,
        *,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        """Receive a same-socket in-band cutoff ACK, then drain and stop.

        A process lock cannot prove the socket receive buffer is empty.  The
        sole reader sends an application-level ``time`` request on the exact
        authenticated connection and keeps reading/journaling until its
        ordered response arrives.  Only that reader may close intake.
        """

        bridge = self._binance_functional_stream_bridge
        if bridge is None:
            raise RuntimeError("Binance functional stream bridge is missing")
        with self._lock:
            event = self._stop_events.get("binance")
            thread = self._threads.get("binance")
            if event is None or thread is None or not thread.is_alive():
                raise RuntimeError("Binance functional stream owner is missing")
        deadline = time.monotonic() + max(0.1, float(timeout))
        with self._binance_functional_terminal_condition:
            if (
                self._binance_functional_terminal_request_id
                and not self._binance_functional_terminal_done
            ):
                raise RuntimeError("Binance terminal barrier is already pending")
            marker_id = "binance-terminal-" + uuid.uuid4().hex
            self._binance_functional_terminal_request_id = marker_id
            self._binance_functional_terminal_done = False
            self._binance_functional_terminal_error = ""
            self._binance_functional_terminal_result = {}
            self._binance_functional_terminal_condition.notify_all()
            while not self._binance_functional_terminal_done:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        "Binance in-band terminal marker response timed out"
                    )
                self._binance_functional_terminal_condition.wait(remaining)
            error = self._binance_functional_terminal_error
            marker_result = dict(self._binance_functional_terminal_result)
        if error:
            raise RuntimeError(error)
        thread.join(timeout=max(0.1, float(timeout)))
        if thread.is_alive():
            raise RuntimeError(
                "Binance functional stream did not stop at terminal barrier"
            )
        return {
            "barrierClosed": True,
            "readerJoined": True,
            **marker_result,
            "stream": bridge.snapshot(),
        }

    def start(self, brokers: Iterable[str] = ("kis", "binance", "upbit")) -> dict[str, Any]:
        with self._lock:
            for broker_id in brokers:
                name = str(broker_id).lower().strip()
                if name not in {
                    "kis",
                    "binance",
                    "binance-futures",
                    "upbit",
                } or (
                    name in self._threads
                    and self._threads[name].is_alive()
                ):
                    continue
                stop_event = threading.Event()
                if name == "binance" and self._binance_functional_stream_bridge is not None:
                    with self._binance_functional_terminal_condition:
                        self._binance_functional_terminal_request_id = ""
                        self._binance_functional_terminal_done = False
                        self._binance_functional_terminal_error = ""
                        self._binance_functional_terminal_result = {}
                self._stop_events[name] = stop_event
                thread = threading.Thread(
                    target=self._run,
                    args=(name, stop_event),
                    daemon=True,
                    name=f"{name}-execution-stream",
                )
                self._threads[name] = thread
                self._status[name] = {"running": True, "connected": False, "lastEventAt": "", "lastError": "", "reconnectCount": 0}
                thread.start()
        return self.snapshot()

    def stop(
        self,
        timeout: float = 5.0,
        *,
        brokers: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        selected = {
            str(item).lower().strip()
            for item in (
                tuple(brokers)
                if brokers is not None
                else tuple(self._threads)
            )
            if str(item).strip()
        }
        with self._lock:
            for name in selected:
                event = self._stop_events.get(name)
                if event is not None:
                    event.set()
            threads = [
                thread
                for name, thread in self._threads.items()
                if name in selected
            ]
        for thread in threads:
            thread.join(timeout=max(0.0, timeout))
        with self._lock:
            for name in selected:
                status = self._status.get(name)
                thread = self._threads.get(name)
                if status is not None:
                    status["running"] = bool(thread and thread.is_alive())
                    if not status["running"]:
                        status["connected"] = False
        return self.snapshot()

    def stop_brokers(
        self,
        brokers: Iterable[str],
        *,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        """Stop only streams owned by one runtime, preserving shared peers."""

        return self.stop(timeout, brokers=brokers)

    def drain(self, broker_id: str) -> list[dict[str, Any]]:
        with self._lock:
            queue = self._events.setdefault(str(broker_id).lower(), deque(maxlen=1000))
            rows = list(queue)
            queue.clear()
            return rows

    def ingest_kis_domestic_fields(
        self,
        tr_id: str,
        fields: Iterable[str],
    ) -> dict[str, Any]:
        """Accept one H0STCNI0 notice from the shared market/private mux."""

        normalized_tr_id = str(tr_id or "").strip().upper()
        if normalized_tr_id != "H0STCNI0":
            raise ValueError("unsupported-kis-private-execution-tr-id")
        event = parse_kis_domestic_execution(
            [str(item) for item in fields]
        )
        if event is None:
            raise ValueError("malformed-kis-domestic-execution-notice")
        self._record("kis", event)
        return dict(event)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schemaVersion": "broker-execution-streams-v1",
                "running": any(thread.is_alive() for thread in self._threads.values()),
                "brokers": {key: {**value, "queuedEvents": len(self._events.get(key, ()))} for key, value in self._status.items()},
            }

    def _run(self, broker_id: str, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                if broker_id == "upbit":
                    self._run_upbit(stop_event)
                elif broker_id == "binance-futures":
                    self._run_binance_futures(stop_event)
                elif broker_id == "binance":
                    self._run_binance(stop_event)
                else:
                    self._run_kis(stop_event)
            except Exception as exc:  # process boundary; status is deliberately credential-free.
                with self._lock:
                    status = self._status[broker_id]
                    status["connected"] = False
                    status["lastError"] = f"{type(exc).__name__}: {exc}"[:500]
                    status["reconnectCount"] = int(status.get("reconnectCount") or 0) + 1
                stop_event.wait(2.0)
        with self._lock:
            status = self._status.get(broker_id)
            if status is not None:
                status["running"] = False
                status["connected"] = False

    def _run_upbit(self, stop_event: threading.Event) -> None:
        import websocket  # type: ignore[import-not-found]

        access_key = os.getenv("UPBIT_ACCESS_KEY", "").strip()
        secret_key = os.getenv("UPBIT_SECRET_KEY", "").strip()
        if not access_key or not secret_key:
            raise RuntimeError("Upbit private stream credentials missing")
        token = upbit_websocket_token(access_key, secret_key)
        socket = websocket.create_connection(
            "wss://api.upbit.com/websocket/v1/private",
            header=[f"Authorization: Bearer {token}"],
            timeout=20,
        )
        socket.send(json.dumps([{"ticket": f"live-trader-{uuid.uuid4()}"}, {"type": "myOrder"}, {"format": "DEFAULT"}]))
        with self._lock:
            self._status["upbit"].update({"connected": True, "lastError": ""})
        try:
            while not stop_event.is_set():
                try:
                    raw = socket.recv()
                except Exception as exc:
                    if type(exc).__name__ == "WebSocketTimeoutException":
                        socket.ping()
                        continue
                    raise
                payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                event = parse_upbit_my_order(payload)
                if event:
                    self._record("upbit", event)
        finally:
            socket.close()

    def _run_binance(self, stop_event: threading.Event) -> None:
        import websocket  # type: ignore[import-not-found]

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Binance private stream credentials missing")
        bridge = self._binance_functional_stream_bridge
        # The functional journal's continuity SLA is five seconds.  A
        # 20-second blocking recv would hide a half-open socket far too long;
        # ordinary stream behavior remains unchanged when no bridge is bound.
        socket = websocket.create_connection(
            "wss://ws-api.binance.com:443/ws-api/v3",
            timeout=2 if bridge is not None else 20,
        )
        request_id = str(uuid.uuid4())
        params = binance_stream_subscription_params(api_key, api_secret)
        socket.send(json.dumps({
            "id": request_id,
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }))
        subscription_confirmed = bridge is None
        liveness_request_id = ""
        liveness_sent_monotonic = 0.0
        terminal_sent_id = ""
        if bridge is None:
            # Preserve the ordinary stream's existing status semantics.  The
            # stricter ACK proof below exists only when the isolated
            # functional bridge was explicitly installed.
            with self._lock:
                self._status["binance"].update(
                    {"connected": True, "lastError": ""}
                )
        try:
            while not stop_event.is_set():
                inbound_ticket = ""
                terminal_ack: dict[str, Any] | None = None
                if bridge is not None and subscription_confirmed:
                    with self._binance_functional_terminal_condition:
                        pending_terminal_id = (
                            self._binance_functional_terminal_request_id
                        )
                    if pending_terminal_id and not terminal_sent_id:
                        # This write alone proves nothing.  Continue receiving
                        # every preceding frame until this exact response is
                        # observed on the same authenticated connection.
                        socket.send(
                            json.dumps(
                                {
                                    "id": pending_terminal_id,
                                    "method": "time",
                                }
                            )
                        )
                        terminal_sent_id = pending_terminal_id
                try:
                    if bridge is None or not subscription_confirmed:
                        raw = socket.recv()
                    else:
                        # Pair the socket receive and durable intake ticket
                        # under the same state-owned fence.  A terminal barrier
                        # can therefore never archive while a frame is already
                        # read but not yet visible as in-flight.
                        with self._binance_functional_receive_lock:
                            raw = socket.recv()
                            begin_inbound = getattr(
                                bridge, "begin_inbound_frame", None
                            )
                            if callable(begin_inbound):
                                inbound_ticket = begin_inbound()
                except Exception as exc:
                    if type(exc).__name__ == "WebSocketTimeoutException":
                        if bridge is None:
                            socket.ping()
                            continue
                        now_monotonic = time.monotonic()
                        if liveness_request_id and (
                            now_monotonic - liveness_sent_monotonic >= 5.0
                        ):
                            raise RuntimeError(
                                "Binance functional stream liveness response deadline missed"
                            )
                        if not liveness_request_id:
                            # Binance WebSocket API ``time`` is an
                            # application-level round trip on this exact
                            # authenticated socket.  Only its matching inbound
                            # response renews durable continuity; the write
                            # itself proves nothing.
                            liveness_request_id = str(uuid.uuid4())
                            liveness_sent_monotonic = now_monotonic
                            socket.send(
                                json.dumps(
                                    {
                                        "id": liveness_request_id,
                                        "method": "time",
                                    }
                                )
                            )
                        continue
                    raise
                try:
                    payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                    if terminal_sent_id and payload.get("id") == terminal_sent_id:
                        status = int(payload.get("status") or 0)
                        result = payload.get("result")
                        server_time_ms = (
                            int(result.get("serverTime") or 0)
                            if isinstance(result, dict)
                            else 0
                        )
                        if status != 200 or server_time_ms <= 0:
                            raise RuntimeError(
                                "Binance functional terminal marker response is invalid"
                            )
                        if bridge is None:
                            raise RuntimeError(
                                "Binance functional terminal marker bridge disappeared"
                            )
                        terminal_ack = dict(
                            bridge.on_terminal_marker(
                                marker_id=terminal_sent_id,
                                server_time_ms=server_time_ms,
                            )
                        )
                    elif liveness_request_id and payload.get("id") == liveness_request_id:
                        status = int(payload.get("status") or 0)
                        result = payload.get("result")
                        if (
                            status != 200
                            or not isinstance(result, dict)
                            or int(result.get("serverTime") or 0) <= 0
                        ):
                            raise RuntimeError(
                                "Binance functional stream liveness response is invalid"
                            )
                        if bridge is not None:
                            bridge.on_transport_liveness()
                        liveness_request_id = ""
                        liveness_sent_monotonic = 0.0
                        continue
                    if terminal_ack is not None:
                        pass
                    elif payload.get("id") == request_id:
                        status = int(payload.get("status") or 0)
                        if bridge is None:
                            if status >= 400:
                                raise RuntimeError(
                                    "Binance user stream subscription failed: "
                                    f"{payload.get('error') or payload.get('status')}"
                                )
                            continue
                        if status != 200:
                            raise RuntimeError(
                                "Binance user stream subscription failed: "
                                f"{payload.get('error') or payload.get('status')}"
                            )
                        if subscription_confirmed:
                            raise RuntimeError(
                                "duplicate Binance user stream subscription ACK"
                            )
                        if bridge is not None:
                            bridge.on_subscription_confirmed()
                        subscription_confirmed = True
                        with self._lock:
                            self._status["binance"].update(
                                {"connected": True, "lastError": ""}
                            )
                        continue
                    elif not subscription_confirmed:
                        raise RuntimeError(
                            "Binance user-data event arrived before subscription ACK"
                        )
                    else:
                        if bridge is not None:
                            # The durable bridge sees execution, account-position, and
                            # balance events before the ordinary execution-only parser.
                            # Unsupported events deliberately tear down and mark a gap.
                            bridge.on_payload(payload)
                        event = parse_binance_execution_report(payload)
                        if event:
                            self._record("binance", event)
                finally:
                    if bridge is not None and inbound_ticket:
                        finish_inbound = getattr(
                            bridge, "finish_inbound_frame", None
                        )
                        if not callable(finish_inbound):
                            raise RuntimeError(
                                "Binance functional inbound fence is incomplete"
                            )
                        finish_inbound(inbound_ticket)
                if terminal_ack is not None:
                    bridge.close_terminal_intake(timeout_seconds=5.0)
                    stop_event.set()
                    terminal_ack.update(
                        {
                            "inBandMarkerReceived": True,
                            "markerConnection": "AUTHENTICATED_BINANCE_WS_API_V3",
                        }
                    )
                    with self._binance_functional_terminal_condition:
                        self._binance_functional_terminal_result = terminal_ack
                        self._binance_functional_terminal_done = True
                        self._binance_functional_terminal_condition.notify_all()
                    break
        finally:
            with self._binance_functional_terminal_condition:
                if (
                    self._binance_functional_terminal_request_id
                    and not self._binance_functional_terminal_done
                ):
                    self._binance_functional_terminal_error = (
                        "Binance stream closed before the in-band terminal marker"
                    )
                    self._binance_functional_terminal_done = True
                    self._binance_functional_terminal_condition.notify_all()
            if bridge is not None:
                bridge.on_disconnect(
                    "Binance user stream socket closed or reconnecting"
                )
            socket.close()

    def _run_binance_futures(self, stop_event: threading.Event) -> None:
        import websocket  # type: ignore[import-not-found]

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "Binance Futures private stream credentials missing"
            )
        base_url = (
            os.getenv("BINANCE_FUTURES_BASE_URL", "").strip()
            or _BINANCE_FUTURES_PRIVATE_REST_BASE
        ).rstrip("/")
        stream_base = (
            os.getenv("BINANCE_FUTURES_STREAM_URL", "").strip()
            or _BINANCE_FUTURES_PRIVATE_STREAM_BASE
        ).rstrip("/")
        if base_url != _BINANCE_FUTURES_PRIVATE_REST_BASE:
            raise RuntimeError(
                "Binance Futures private REST origin must be the official "
                "production endpoint"
            )
        if stream_base != _BINANCE_FUTURES_PRIVATE_STREAM_BASE:
            raise RuntimeError(
                "Binance Futures private stream origin must be the official "
                "production endpoint"
            )
        listen_key_url = f"{base_url}/fapi/v1/listenKey"
        request = urllib.request.Request(
            listen_key_url,
            data=b"",
            headers={"X-MBX-APIKEY": api_key},
            method="POST",
        )
        with _open_authenticated_broker_request(
            request,
            timeout=10,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
        listen_key = str(payload.get("listenKey") or "")
        if not listen_key:
            raise RuntimeError(
                "Binance Futures listenKey 발급에 실패했습니다."
            )
        socket = websocket.create_connection(
            f"{stream_base}/{listen_key}",
            timeout=20,
        )
        last_keepalive = time.monotonic()
        with self._lock:
            self._status["binance-futures"].update(
                {"connected": True, "lastError": ""}
            )
        try:
            while not stop_event.is_set():
                if time.monotonic() - last_keepalive >= 30 * 60:
                    keepalive = urllib.request.Request(
                        listen_key_url,
                        data=b"",
                        headers={"X-MBX-APIKEY": api_key},
                        method="PUT",
                    )
                    with _open_authenticated_broker_request(
                        keepalive,
                        timeout=10,
                    ):
                        pass
                    last_keepalive = time.monotonic()
                try:
                    raw = socket.recv()
                except Exception as exc:
                    if type(exc).__name__ == "WebSocketTimeoutException":
                        socket.ping()
                        continue
                    raise
                payload = json.loads(
                    raw.decode("utf-8")
                    if isinstance(raw, bytes)
                    else raw
                )
                event = parse_binance_futures_order_update(payload)
                if event:
                    self._record("binance-futures", event)
        finally:
            socket.close()
            try:
                close_request = urllib.request.Request(
                    listen_key_url,
                    data=b"",
                    headers={"X-MBX-APIKEY": api_key},
                    method="DELETE",
                )
                with _open_authenticated_broker_request(
                    close_request,
                    timeout=10,
                ):
                    pass
            except Exception:
                pass

    def _run_kis(self, stop_event: threading.Event) -> None:
        import websocket  # type: ignore[import-not-found]

        app_key = os.getenv("KIS_APP_KEY", "").strip()
        app_secret = os.getenv("KIS_APP_SECRET", "").strip()
        hts_id = os.getenv("KIS_HTS_ID", "").strip()
        if not app_key or not app_secret or not hts_id:
            raise RuntimeError("KIS private stream credentials/HTS ID missing")
        account_id = (
            f"{os.getenv('KIS_ACCOUNT_NO', '').strip()}-"
            f"{os.getenv('KIS_ACCOUNT_PRODUCT_CODE', '').strip()}"
        )
        app_key_id = "sha256:" + hashlib.sha256(
            app_key.encode("utf-8")
        ).hexdigest()
        owner_id = f"live_trader:legacy-private:{os.getpid()}:{id(self)}"
        GLOBAL_KIS_WEBSOCKET_OWNERS.claim(
            account_id=account_id,
            app_key_id=app_key_id,
            owner_id=owner_id,
            environment="LIVE",
        )
        request = urllib.request.Request(
            "https://openapi.koreainvestment.com:9443/oauth2/Approval",
            data=json.dumps({"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}).encode(),
            headers={"content-type": "application/json; charset=utf-8"},
            method="POST",
        )
        # KIS throttles Approval independently from normal REST and tokenP.
        # The app-key-only SQLite bucket is shared with the market-data feed
        # and every sibling process, so reconnect storms cannot burst here.
        GLOBAL_KIS_REST_LIMITERS.get_approval(app_key).acquire()
        try:
            with _open_authenticated_broker_request(
                request,
                timeout=10,
            ) as response:
                approval = json.loads(response.read().decode())
        except Exception:
            GLOBAL_KIS_WEBSOCKET_OWNERS.release(
                account_id=account_id,
                app_key_id=app_key_id,
                owner_id=owner_id,
            )
            raise
        approval_key = str(approval.get("approval_key") or "")
        if not approval_key:
            GLOBAL_KIS_WEBSOCKET_OWNERS.release(
                account_id=account_id,
                app_key_id=app_key_id,
                owner_id=owner_id,
            )
            raise RuntimeError("KIS private stream approval failed")
        try:
            socket = websocket.create_connection("ws://ops.koreainvestment.com:21000/tryitout", timeout=20)
            for tr_id in ("H0STCNI0", "H0GSCNI0"):
                socket.send(json.dumps({
                    "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                    "body": {"input": {"tr_id": tr_id, "tr_key": hts_id}},
                }))
        except Exception:
            try:
                socket.close()  # type: ignore[possibly-undefined]
            except Exception:
                pass
            GLOBAL_KIS_WEBSOCKET_OWNERS.release(
                account_id=account_id,
                app_key_id=app_key_id,
                owner_id=owner_id,
            )
            raise
        aes: dict[str, tuple[str, str]] = {}
        with self._lock:
            self._status["kis"].update({"connected": True, "lastError": ""})
        try:
            while not stop_event.is_set():
                try:
                    raw = socket.recv()
                except Exception as exc:
                    if type(exc).__name__ == "WebSocketTimeoutException":
                        socket.ping()
                        continue
                    raise
                text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
                if text.startswith("{"):
                    payload = json.loads(text)
                    header = payload.get("header") if isinstance(payload, dict) else {}
                    body = payload.get("body") if isinstance(payload, dict) else {}
                    tr_id = str(header.get("tr_id") or "") if isinstance(header, dict) else ""
                    output = body.get("output") if isinstance(body, dict) else {}
                    if isinstance(output, dict) and output.get("key") and output.get("iv"):
                        aes[tr_id] = (str(output["key"]), str(output["iv"]))
                    if tr_id.upper() == "PINGPONG":
                        socket.send(text)
                    continue
                if not text.startswith("1|"):
                    continue
                parts = text.split("|", 3)
                if len(parts) < 4 or parts[1] not in aes:
                    continue
                fields = self._decrypt_kis(parts[3], *aes[parts[1]]).split("^")
                event = parse_kis_domestic_execution(fields) if parts[1] == "H0STCNI0" else parse_kis_overseas_execution(fields)
                if event:
                    self._record("kis", event)
        finally:
            socket.close()
            GLOBAL_KIS_WEBSOCKET_OWNERS.release(
                account_id=account_id,
                app_key_id=app_key_id,
                owner_id=owner_id,
            )

    @staticmethod
    def _decrypt_kis(ciphertext: str, key: str, iv: str) -> str:
        try:
            from Crypto.Cipher import AES  # type: ignore[import-not-found]
            from Crypto.Util.Padding import unpad  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("pycryptodome is required for KIS execution notifications") from exc
        decoded = AES.new(key.encode(), AES.MODE_CBC, iv.encode()).decrypt(base64.b64decode(ciphertext))
        return unpad(decoded, AES.block_size).decode("utf-8")

    def _record(self, broker_id: str, event: dict[str, Any]) -> None:
        with self._lock:
            self._events[broker_id].append(dict(event))
            status = self._status.setdefault(
                broker_id,
                {
                    "running": False,
                    "connected": False,
                    "lastEventAt": "",
                    "lastError": "",
                    "reconnectCount": 0,
                    "source": "external-multiplexed-feed",
                },
            )
            status["lastEventAt"] = str(event.get("occurred_at") or "")
            log_dir = self.data_root / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            path = log_dir / "broker_execution_stream.jsonl"
            line = json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
            self._rotate_log_if_needed(path, len(line.encode("utf-8")))
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line)

    def _rotate_log_if_needed(self, path: Path, incoming_bytes: int) -> None:
        try:
            current_size = path.stat().st_size
        except FileNotFoundError:
            return
        if current_size + incoming_bytes <= self.log_max_bytes:
            return
        oldest = path.with_name(f"{path.name}.{self.log_backup_count}")
        if oldest.exists():
            oldest.unlink()
        for index in range(self.log_backup_count - 1, 0, -1):
            source = path.with_name(f"{path.name}.{index}")
            if source.exists():
                source.replace(path.with_name(f"{path.name}.{index + 1}"))
        path.replace(path.with_name(f"{path.name}.1"))
