from __future__ import annotations

import base64
from collections import deque
from datetime import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Iterable
import urllib.request
import uuid

from .live_adapters import binance_timestamp_ms, refresh_binance_time_offset


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
    executed = float(payload.get("executed_volume") or payload.get("executedVolume") or payload.get("trade_volume") or 0)
    price = float(payload.get("trade_price") or payload.get("price") or 0)
    return {
        "event_id": trade_uuid or f"upbit:{order_uuid}:{state}:{executed}",
        "broker_id": "upbit",
        "order_id": str(payload.get("identifier") or ""),
        "broker_order_id": order_uuid,
        "symbol": market,
        "side": "BUY" if str(payload.get("ask_bid") or payload.get("side") or "").upper() in {"BID", "BUY"} else "SELL",
        "quantity": executed,
        "price": price,
        "fee": float(payload.get("paid_fee") or payload.get("paidFee") or 0),
        "state": "filled" if state == "done" else state or "accepted",
        "occurred_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_type": "upbit_my_order",
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
        "occurred_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        "occurred_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "raw_type": "kis_overseas_execution",
    }


def _float(value: Any) -> float:
    try:
        return float(str(value or "0").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


class ExecutionStreamManager:
    """Reconnectable private order/execution streams with a durable audit log."""

    def __init__(self, data_root: Path, *, log_max_bytes: int = 5 * 1024 * 1024, log_backup_count: int = 3) -> None:
        self.data_root = Path(data_root)
        self.log_max_bytes = max(1024, int(log_max_bytes))
        self.log_backup_count = max(1, int(log_backup_count))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._events: dict[str, deque[dict[str, Any]]] = {
            "kis": deque(maxlen=1000),
            "binance": deque(maxlen=1000),
            "binance-futures": deque(maxlen=1000),
            "upbit": deque(maxlen=1000),
        }
        self._status: dict[str, dict[str, Any]] = {}

    def start(self, brokers: Iterable[str] = ("kis", "binance", "upbit")) -> dict[str, Any]:
        with self._lock:
            self._stop.clear()
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
                thread = threading.Thread(target=self._run, args=(name,), daemon=True, name=f"{name}-execution-stream")
                self._threads[name] = thread
                self._status[name] = {"running": True, "connected": False, "lastEventAt": "", "lastError": "", "reconnectCount": 0}
                thread.start()
        return self.snapshot()

    def stop(self, timeout: float = 5.0) -> dict[str, Any]:
        self._stop.set()
        for thread in tuple(self._threads.values()):
            thread.join(timeout=max(0.0, timeout))
        with self._lock:
            for status in self._status.values():
                status["running"] = False
                status["connected"] = False
        return self.snapshot()

    def drain(self, broker_id: str) -> list[dict[str, Any]]:
        with self._lock:
            queue = self._events.setdefault(str(broker_id).lower(), deque(maxlen=1000))
            rows = list(queue)
            queue.clear()
            return rows

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schemaVersion": "broker-execution-streams-v1",
                "running": any(thread.is_alive() for thread in self._threads.values()),
                "brokers": {key: {**value, "queuedEvents": len(self._events.get(key, ()))} for key, value in self._status.items()},
            }

    def _run(self, broker_id: str) -> None:
        while not self._stop.is_set():
            try:
                if broker_id == "upbit":
                    self._run_upbit()
                elif broker_id == "binance-futures":
                    self._run_binance_futures()
                elif broker_id == "binance":
                    self._run_binance()
                else:
                    self._run_kis()
            except Exception as exc:  # process boundary; status is deliberately credential-free.
                with self._lock:
                    status = self._status[broker_id]
                    status["connected"] = False
                    status["lastError"] = f"{type(exc).__name__}: {exc}"[:500]
                    status["reconnectCount"] = int(status.get("reconnectCount") or 0) + 1
                self._stop.wait(2.0)

    def _run_upbit(self) -> None:
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
            while not self._stop.is_set():
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

    def _run_binance(self) -> None:
        import websocket  # type: ignore[import-not-found]

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Binance private stream credentials missing")
        socket = websocket.create_connection("wss://ws-api.binance.com:443/ws-api/v3", timeout=20)
        request_id = str(uuid.uuid4())
        params = binance_stream_subscription_params(api_key, api_secret)
        socket.send(json.dumps({
            "id": request_id,
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }))
        with self._lock:
            self._status["binance"].update({"connected": True, "lastError": ""})
        try:
            while not self._stop.is_set():
                try:
                    raw = socket.recv()
                except Exception as exc:
                    if type(exc).__name__ == "WebSocketTimeoutException":
                        socket.ping()
                        continue
                    raise
                payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                if payload.get("id") == request_id and int(payload.get("status") or 0) >= 400:
                    raise RuntimeError(f"Binance user stream subscription failed: {payload.get('error') or payload.get('status')}")
                event = parse_binance_execution_report(payload)
                if event:
                    self._record("binance", event)
        finally:
            socket.close()

    def _run_binance_futures(self) -> None:
        import websocket  # type: ignore[import-not-found]

        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "Binance Futures private stream credentials missing"
            )
        base_url = (
            os.getenv("BINANCE_FUTURES_BASE_URL", "").strip()
            or "https://fapi.binance.com"
        ).rstrip("/")
        listen_key_url = f"{base_url}/fapi/v1/listenKey"
        request = urllib.request.Request(
            listen_key_url,
            data=b"",
            headers={"X-MBX-APIKEY": api_key},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        listen_key = str(payload.get("listenKey") or "")
        if not listen_key:
            raise RuntimeError(
                "Binance Futures listenKey 발급에 실패했습니다."
            )
        stream_base = os.getenv(
            "BINANCE_FUTURES_STREAM_URL",
            "wss://fstream.binance.com/ws",
        ).rstrip("/")
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
            while not self._stop.is_set():
                if time.monotonic() - last_keepalive >= 30 * 60:
                    keepalive = urllib.request.Request(
                        listen_key_url,
                        data=b"",
                        headers={"X-MBX-APIKEY": api_key},
                        method="PUT",
                    )
                    with urllib.request.urlopen(
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
                with urllib.request.urlopen(close_request, timeout=10):
                    pass
            except Exception:
                pass

    def _run_kis(self) -> None:
        import websocket  # type: ignore[import-not-found]

        app_key = os.getenv("KIS_APP_KEY", "").strip()
        app_secret = os.getenv("KIS_APP_SECRET", "").strip()
        hts_id = os.getenv("KIS_HTS_ID", "").strip()
        if not app_key or not app_secret or not hts_id:
            raise RuntimeError("KIS private stream credentials/HTS ID missing")
        request = urllib.request.Request(
            "https://openapi.koreainvestment.com:9443/oauth2/Approval",
            data=json.dumps({"grant_type": "client_credentials", "appkey": app_key, "secretkey": app_secret}).encode(),
            headers={"content-type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - official fixed endpoint.
            approval = json.loads(response.read().decode())
        approval_key = str(approval.get("approval_key") or "")
        if not approval_key:
            raise RuntimeError("KIS private stream approval failed")
        socket = websocket.create_connection("ws://ops.koreainvestment.com:21000/tryitout", timeout=20)
        for tr_id in ("H0STCNI0", "H0GSCNI0"):
            socket.send(json.dumps({
                "header": {"approval_key": approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                "body": {"input": {"tr_id": tr_id, "tr_key": hts_id}},
            }))
        aes: dict[str, tuple[str, str]] = {}
        with self._lock:
            self._status["kis"].update({"connected": True, "lastError": ""})
        try:
            while not self._stop.is_set():
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
            self._status[broker_id]["lastEventAt"] = str(event.get("occurred_at") or "")
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
