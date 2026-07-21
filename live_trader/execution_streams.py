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


def _b64url(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def upbit_websocket_token(access_key: str, secret_key: str, nonce: str | None = None) -> str:
    header = _b64url(json.dumps({"alg": "HS512", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64url(json.dumps({"access_key": access_key, "nonce": nonce or str(uuid.uuid4())}, separators=(",", ":")).encode())
    signature = hmac.new(secret_key.encode(), f"{header}.{payload}".encode(), hashlib.sha512).digest()
    return f"{header}.{payload}.{_b64url(signature)}"


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


def _float(value: Any) -> float:
    try:
        return float(str(value or "0").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


class ExecutionStreamManager:
    """Reconnectable private order/execution streams with a durable audit log."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(data_root)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._events: dict[str, deque[dict[str, Any]]] = {"kis": deque(maxlen=1000), "upbit": deque(maxlen=1000)}
        self._status: dict[str, dict[str, Any]] = {}

    def start(self, brokers: Iterable[str] = ("kis", "upbit")) -> dict[str, Any]:
        with self._lock:
            self._stop.clear()
            for broker_id in brokers:
                name = str(broker_id).lower().strip()
                if name not in {"kis", "upbit"} or (name in self._threads and self._threads[name].is_alive()):
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
                event = parse_kis_domestic_execution(fields) if parts[1] == "H0STCNI0" else None
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
        with (log_dir / "broker_execution_stream.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
