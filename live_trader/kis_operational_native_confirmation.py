"""Same-process, one-use native confirmation for an exact KIS operational start.

This store never grants order authority. Only state may arm/consume it; the
renderer bridge exposes request(nonce) with no supplied phrase or context.
"""
from __future__ import annotations
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

CONTEXT_SCHEMA = "kis-scoped-manual-start-context/v1"
ATTESTATION_SCHEMA = "kis-operational-native-start-attestation/v1"
MAX_SECONDS = 45.0
CONTEXT_FIELDS = frozenset((
    "schemaVersion", "scopeHash", "profileId", "mode", "portfolioId",
    "deploymentId", "strategyId", "runtimeScopeId", "capitalCurrency",
    "accountFingerprint", "credentialConfigurationHash", "wireAccountFingerprint",
    "wireCredentialConfigurationHash", "ownerEpochHash",
))
HASH_FIELDS = frozenset((
    "scopeHash", "accountFingerprint", "credentialConfigurationHash",
    "wireAccountFingerprint", "wireCredentialConfigurationHash", "ownerEpochHash",
))
SHA = re.compile(r"[0-9a-f]{64}")
NONCE = re.compile(r"[A-Za-z0-9_-]{40,128}")

def _encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")

def _hash(value):
    return hashlib.sha256(_encoded(value)).hexdigest()

def _context(value):
    if type(value) is not dict or set(value) != CONTEXT_FIELDS:
        raise ValueError("CONTEXT_FIELDS")
    if (value["schemaVersion"] != CONTEXT_SCHEMA or value["profileId"] != "stock"
            or value["mode"] not in {"SMALL_LIVE", "FULL_LIVE"}
            or value["capitalCurrency"] not in {"KRW", "USD"}):
        raise ValueError("CONTEXT_SCOPE")
    if any(type(value[k]) is not str or not SHA.fullmatch(value[k]) for k in HASH_FIELDS):
        raise ValueError("CONTEXT_HASH")
    for key in ("portfolioId", "deploymentId", "strategyId", "runtimeScopeId"):
        text = value[key]
        if (type(text) is not str or len(text) > 240 or
                any(ord(char) < 32 or ord(char) == 127 for char in text)):
            raise ValueError("CONTEXT_ID")
    if not value["deploymentId"] or not value["runtimeScopeId"] or not (
            value["portfolioId"] or value["strategyId"]):
        raise ValueError("CONTEXT_ID")
    return json.loads(_encoded(value))

@dataclass(frozen=True)
class KisNativeStartPrompt:
    context: dict
    expected_phrase: str
    expires_epoch: float
    monotonic_deadline: float

class WindowsKisStartPhrasePrompt:
    """OS-owned input window; no phrase is accepted from JavaScript or HTTP."""
    def __call__(self, request):
        if os.name != "nt":
            return None
        import tkinter as tk
        result = [None]
        root = tk.Tk()
        root.title("Live Trader — KIS 실거래 시작 확인")
        width = min(740, max(620, root.winfo_screenwidth() - 80))
        height = min(640, max(440, root.winfo_screenheight() - 120))
        root.geometry(f"{width}x{height}")
        root.minsize(620, 440)
        root.resizable(True, True)
        root.attributes("-topmost", True)
        context = request.context
        message = (
            "선택한 한국투자증권 자동매매를 시작합니다.\n"
            "시작 조건이 모두 충족되면 실제 주문이 전송될 수 있습니다.\n"
            "같은 계좌·배포·전략·한도·정책으로 자동매매를 계속합니다.\n"
            "매 주문은 새 사전점검과 최신 계좌·시세 검증을 다시 통과해야 합니다.\n"
            "전송 권한은 최대 15초·1회이며 중지·설정 변경·실패 시 회수됩니다.\n\n"
            "중지를 요청하면 이 실행이 접수한 미체결 주문의 남은 수량만 취소 요청합니다.\n"
            "최종 상태를 확인하지 못한 주문은 복구 대기로 남고 재시작을 차단합니다.\n"
            "보유 포지션은 유지됩니다.\n\n"
            f"계좌 지문: {context['accountFingerprint'][:12]}\n"
            f"통화: {context['capitalCurrency']}   모드: {context['mode']}\n"
            f"포트폴리오: {context['portfolioId'] or context['strategyId']}\n"
            f"배포: {context['deploymentId']}"
        )
        body = tk.Frame(root)
        body.pack(padx=20, pady=(15, 8), fill="both", expand=True)
        scrollbar = tk.Scrollbar(body)
        scrollbar.pack(side="right", fill="y")
        summary = tk.Text(body, wrap="word", height=16, font=("Malgun Gothic", 10),
                          relief="flat", takefocus=False, yscrollcommand=scrollbar.set)
        summary.insert("1.0", message)
        summary.configure(state="disabled")
        summary.pack(side="left", fill="both", expand=True)
        scrollbar.configure(command=summary.yview)
        tk.Label(root, text="표시된 문구를 직접 입력한 뒤 시작을 확인하세요.",
                 font=("Malgun Gothic", 10)).pack(padx=20, anchor="w")
        tk.Label(root, text=request.expected_phrase,
                 font=("Consolas", 12, "bold")).pack(padx=20, pady=(3, 7), anchor="w")
        entry = tk.Entry(root, font=("Consolas", 12))
        entry.pack(padx=20, fill="x")
        status = tk.Label(root, text="", fg="#aa0000")
        status.pack()
        def confirm(_event=None):
            if time.monotonic() >= request.monotonic_deadline:
                root.destroy()
            elif hmac.compare_digest(entry.get(), request.expected_phrase):
                result[0] = entry.get()
                root.destroy()
            else:
                status.config(text="표시된 문구를 정확히 입력해 주세요.")
        buttons = tk.Frame(root)
        buttons.pack(pady=10)
        tk.Button(buttons, text="취소", command=root.destroy, width=14).pack(side="left", padx=8)
        tk.Button(buttons, text="실거래 시작 확인", command=confirm, width=22).pack(side="left", padx=8)
        entry.bind("<Return>", confirm)
        root.bind("<Escape>", lambda _event: root.destroy())
        root.protocol("WM_DELETE_WINDOW", root.destroy)
        def expire():
            if time.monotonic() >= request.monotonic_deadline:
                root.destroy()
            else:
                root.after(200, expire)
        root.after(200, expire)
        entry.focus_set()
        root.mainloop()
        return result[0]

class KisNativeOperationalStartConfirmations:
    def __init__(self, *, prompt=None, clock=time.time, monotonic_clock=time.monotonic):
        self._prompt = prompt if prompt is not None else WindowsKisStartPhrasePrompt()
        self._clock, self._monotonic = clock, monotonic_clock
        self._pid = os.getpid()
        self._instance = secrets.token_hex(32)
        self._secret = secrets.token_bytes(32)
        self._lock = threading.RLock()
        self._pending, self._issued = {}, {}
        self._invalidation_listeners = []

    def register_invalidation_listener(self, listener):
        """Internal nonblocking revocation only; listeners grant no authority."""
        if not callable(listener):
            raise TypeError("native invalidation listener must be callable")
        with self._lock:
            if listener not in self._invalidation_listeners:
                self._invalidation_listeners.append(listener)

    def _invalidate_nonce(self, nonce):
        for listener in tuple(self._invalidation_listeners):
            listener(nonce)

    @staticmethod
    def _failure(code):
        return {"ok": False, "code": "KIS_NATIVE_" + code,
                "message": "KIS 시작 확인이 유효하지 않거나 만료되었습니다. 다시 확인해 주세요."}

    def _times(self):
        wall, mono = self._clock(), self._monotonic()
        if (os.getpid() != self._pid or type(wall) not in (float, int)
                or type(mono) not in (float, int)
                or not math.isfinite(wall) or not math.isfinite(mono)):
            raise ValueError("PROCESS_OR_CLOCK")
        return float(wall), float(mono)

    def _live(self, record):
        wall, mono = self._times()
        return (record["issuedAt"] <= wall < record["expiresAt"]
                and record["issuedMono"] <= mono < record["deadline"])

    def revoke_scope(self, runtime_scope_id):
        with self._lock:
            for store in (self._pending, self._issued):
                for key, record in list(store.items()):
                    if record["context"]["runtimeScopeId"] == runtime_scope_id:
                        del store[key]
                        self._invalidate_nonce(key)

    def revoke_all(self):
        with self._lock:
            nonces = set(self._pending) | set(self._issued)
            self._pending.clear()
            self._issued.clear()
            for nonce in nonces:
                self._invalidate_nonce(nonce)

    def arm(self, context):
        try:
            expected = _context(context)
            with self._lock:
                wall, mono = self._times()
                for store in (self._pending, self._issued):
                    for key, record in list(store.items()):
                        if not self._live(record):
                            del store[key]
                            self._invalidate_nonce(key)
                self.revoke_scope(expected["runtimeScopeId"])
                if len(self._pending) + len(self._issued) >= 16:
                    return self._failure("CAPACITY")
                nonce = secrets.token_urlsafe(32)
                self._pending[nonce] = {
                    "context": expected, "issuedAt": wall, "expiresAt": wall + MAX_SECONDS,
                    "issuedMono": mono, "deadline": mono + MAX_SECONDS,
                    "phrase": "KIS START " + secrets.token_hex(4).upper(),
                }
                return {"ok": True, "nativeNonce": nonce, "context": json.loads(_encoded(expected)),
                        "expiresAt": datetime.fromtimestamp(wall + MAX_SECONDS, timezone.utc).isoformat()}
        except (ValueError, TypeError, KeyError, OverflowError):
            return self._failure("CONTEXT_INVALID")

    def request(self, native_nonce):
        if type(native_nonce) is not str or not NONCE.fullmatch(native_nonce):
            return self._failure("NONCE_INVALID")
        with self._lock:
            record = self._pending.pop(native_nonce, None)
            try:
                if record is None:
                    return self._failure("EXPIRED")
                if not self._live(record):
                    self._invalidate_nonce(native_nonce)
                    return self._failure("EXPIRED")
                # Track the in-flight native window so stop/rearm can revoke it.
                self._issued[native_nonce] = record
            except (ValueError, TypeError):
                self._invalidate_nonce(native_nonce)
                return self._failure("CLOCK_INVALID")
        try:
            phrase = self._prompt(KisNativeStartPrompt(
                json.loads(_encoded(record["context"])), record["phrase"],
                record["expiresAt"], record["deadline"]))
        except Exception:
            phrase = None
        with self._lock:
            if self._issued.get(native_nonce) is not record:
                self._invalidate_nonce(native_nonce)
                return self._failure("REVOKED")
            del self._issued[native_nonce]
            try:
                if not self._live(record):
                    self._invalidate_nonce(native_nonce)
                    return self._failure("EXPIRED")
                if type(phrase) is not str or not hmac.compare_digest(phrase, record["phrase"]):
                    self._invalidate_nonce(native_nonce)
                    return self._failure("CANCELLED")
                body = {
                    "schemaVersion": ATTESTATION_SCHEMA,
                    "contextHash": _hash(record["context"]),
                    "oneUseNonce": native_nonce, "instanceHash": _hash(self._instance),
                    "issuedAt": record["issuedAt"], "expiresAt": record["expiresAt"],
                }
                proof = hmac.new(self._secret, _encoded(body), hashlib.sha256).hexdigest()
                attestation = {**body, "proofHmac": proof}
                self._issued[native_nonce] = {**record, "attestation": attestation}
                return {"ok": True, "attestation": json.loads(_encoded(attestation))}
            except (ValueError, TypeError):
                self._invalidate_nonce(native_nonce)
                return self._failure("CLOCK_INVALID")

    def consume(self, attestation, expected_context):
        result = self.consume_prepared_start(attestation, expected_context)
        return {key: value for key, value in result.items() if key != "nativeNonce"}

    def consume_prepared_start(self, attestation, expected_context):
        """Internal-only validated nonce selects a process-private feed object.

        This is the same one-use proof check as consume(), not a new permission
        and not a client-provided prepared context or serializable capability.
        """
        with self._lock:
            # Burn a known nonce even if the submitted scope/proof is wrong.
            nonce = attestation.get("oneUseNonce") if type(attestation) is dict else None
            record = self._issued.pop(nonce, None) if type(nonce) is str else None
            if record is None:
                return self._failure("EXPIRED_OR_USED")
            try:
                expected = _context(expected_context)
                if not self._live(record):
                    self._invalidate_nonce(nonce)
                    return self._failure("EXPIRED_OR_USED")
                if (record.get("attestation") != attestation
                        or record["context"] != expected):
                    self._invalidate_nonce(nonce)
                    return self._failure("SCOPE_OR_PROOF_INVALID")
                return {"ok": True, "context": expected, "nativeNonce": nonce}
            except (ValueError, TypeError, KeyError):
                self._invalidate_nonce(nonce)
                return self._failure("CONTEXT_INVALID")

KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS = KisNativeOperationalStartConfirmations()

