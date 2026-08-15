from __future__ import annotations

"""Signed supervised audit anchor backed by a fast-forward-only remote ref.

This is not formal WORM: a remote administrator may still rewrite the ref.
It is suitable only for the explicitly accepted non-promotion contract.  The
trader process receives a public-key verifier and never the authority key.
"""

import base64
import ctypes
import hashlib
import hmac
import json
import os
import re
import secrets
import struct
import sys
import time
from typing import Any, Callable, Mapping

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa


REQUEST_SCHEMA = "crypto-first-live-supervised-anchor-request/v1"
STATE_SCHEMA = "crypto-first-live-supervised-git-anchor-state/v1"
RECEIPT_SCHEMA = "crypto-first-live-supervised-git-anchor-receipt/v1"
PROJECTION_SCHEMA = "crypto-first-live-supervised-audit-anchor/v1"
COMMAND_SCHEMA = "crypto-first-live-supervised-authority-command/v1"
RESPONSE_SCHEMA = "crypto-first-live-supervised-authority-response/v1"
SIGNATURE_DOMAIN = b"CRYPTO_FIRST_LIVE_SUPERVISED_GIT_ANCHOR_V1\x00"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_PIPE_RE = re.compile(r"^\\\\\.\\pipe\\[A-Za-z0-9._-]{8,120}$")
_SID_RE = re.compile(r"^S-1-(?:\d+-){1,14}\d+$", re.IGNORECASE)
_MAX_WIRE_BYTES = 65_536
_MAX_FRAME_BYTES = _MAX_WIRE_BYTES + 4
_TRANSPORT_REQUEST_SCHEMA = (
    "crypto-first-live-supervised-pipe-request/v1"
)
_TRANSPORT_RESPONSE_SCHEMA = (
    "crypto-first-live-supervised-pipe-response/v1"
)
_TRANSPORT_REQUEST_DOMAIN = (
    b"CRYPTO_FIRST_LIVE_SUPERVISED_PIPE_REQUEST_V1\x00"
)
_TRANSPORT_RESPONSE_DOMAIN = (
    b"CRYPTO_FIRST_LIVE_SUPERVISED_PIPE_RESPONSE_V1\x00"
)


class CryptoFirstLiveSupervisedAnchorError(RuntimeError):
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
        raise CryptoFirstLiveSupervisedAnchorError(f"{label}-invalid")
    return result


def _exact_hash(value: object, label: str, *, empty: bool = False) -> str:
    result = _text(value).lower()
    if empty and result == "":
        return result
    if _HASH_RE.fullmatch(result) is None:
        raise CryptoFirstLiveSupervisedAnchorError(f"{label}-invalid")
    return result


def validate_anchor_request(value: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(value)
    if set(request) != {
        "schemaVersion",
        "kind",
        "authorityId",
        "namespaceId",
        "lane",
        "sessionId",
        "permitId",
        "permitHash",
        "coordinatorDatabaseId",
        "coordinatorRevision",
        "publicationHash",
        "priorCheckpointHash",
        "eventType",
    }:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-request-fields-not-exact"
        )
    lane = _text(request.get("lane")).upper()
    event_type = _text(request.get("eventType")).upper()
    try:
        revision = int(request.get("coordinatorRevision"))
    except (TypeError, ValueError) as exc:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-revision-invalid"
        ) from exc
    if (
        request.get("schemaVersion") != REQUEST_SCHEMA
        or request.get("kind") != "REMOTE_FAST_FORWARD_GIT_SIGNED"
        or lane not in {"UPBIT", "BINANCE_SPOT"}
        or event_type
        not in {"APPROVAL", "ACTIVATION", "HEARTBEAT", "REVOKED", "FINALIZED"}
        or isinstance(request.get("coordinatorRevision"), bool)
        or revision < 0
    ):
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-request-invalid"
        )
    for field in (
        "authorityId",
        "namespaceId",
        "sessionId",
        "permitId",
        "coordinatorDatabaseId",
    ):
        _exact_id(request.get(field), field)
    _exact_hash(request.get("permitHash"), "permit-hash")
    if revision == 0:
        if _text(request.get("publicationHash")) != "":
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-publication-hash-invalid"
            )
    else:
        _exact_hash(request.get("publicationHash"), "publication-hash")
    _exact_hash(
        request.get("priorCheckpointHash"),
        "prior-checkpoint-hash",
        empty=True,
    )
    return request


def _exact_sid(value: object, label: str) -> str:
    sid = _text(value).upper()
    if _SID_RE.fullmatch(sid) is None:
        raise CryptoFirstLiveSupervisedAnchorError(f"{label}-invalid")
    return sid


def _windows_pipe_sddl(trader_os_sid: str) -> str:
    """Return a protected allow-only DACL for the exact trader SID.

    A literal Everyone deny ACE cannot be combined with a trader allow ACE:
    every trader token is also a member of Everyone and the deny would win.
    Anonymous is explicitly denied; every principal other than SYSTEM,
    Administrators, and the exact trader SID is denied by absence of an allow
    ACE in this protected DACL.
    """

    sid = _exact_sid(trader_os_sid, "trader-os-sid")
    if sid in {"S-1-5-18", "S-1-5-32-544"}:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-trader-os-sid-not-independent"
        )
    return (
        "D:P"
        "(D;;GA;;;AN)"
        "(A;;GA;;;SY)"
        "(A;;GA;;;BA)"
        f"(A;;GRGW;;;{sid})"
    )


class _Win32PipeApi:
    """Small ctypes surface used by the bounded local pipe transport."""

    ERROR_FILE_NOT_FOUND = 2
    ERROR_ACCESS_DENIED = 5
    ERROR_BROKEN_PIPE = 109
    ERROR_INSUFFICIENT_BUFFER = 122
    ERROR_PIPE_BUSY = 231
    ERROR_NO_DATA = 232
    ERROR_PIPE_NOT_CONNECTED = 233
    ERROR_IO_PENDING = 997
    ERROR_PIPE_CONNECTED = 535
    WAIT_OBJECT_0 = 0
    WAIT_TIMEOUT = 258
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
    FILE_FLAG_OVERLAPPED = 0x40000000
    FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000
    PIPE_ACCESS_DUPLEX = 0x00000003
    PIPE_TYPE_BYTE = 0x00000000
    PIPE_READMODE_BYTE = 0x00000000
    PIPE_WAIT = 0x00000000
    PIPE_REJECT_REMOTE_CLIENTS = 0x00000008
    PIPE_UNLIMITED_INSTANCES = 255
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    TOKEN_QUERY = 0x0008
    TOKEN_USER = 1

    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [
            ("nLength", ctypes.c_uint32),
            ("lpSecurityDescriptor", ctypes.c_void_p),
            ("bInheritHandle", ctypes.c_int),
        ]

    class OVERLAPPED(ctypes.Structure):
        _fields_ = [
            ("Internal", ctypes.c_size_t),
            ("InternalHigh", ctypes.c_size_t),
            ("Offset", ctypes.c_uint32),
            ("OffsetHigh", ctypes.c_uint32),
            ("hEvent", ctypes.c_void_p),
        ]

    def __init__(self) -> None:
        if sys.platform != "win32":
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-windows-pipe-required"
            )
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self.advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        self.kernel32.CreateNamedPipeW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.POINTER(self.SECURITY_ATTRIBUTES),
        ]
        self.kernel32.CreateNamedPipeW.restype = ctypes.c_void_p
        self.kernel32.CreateFileW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        self.kernel32.CreateFileW.restype = ctypes.c_void_p
        self.kernel32.WaitNamedPipeW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
        ]
        self.kernel32.WaitNamedPipeW.restype = ctypes.c_int
        self.kernel32.ConnectNamedPipe.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(self.OVERLAPPED),
        ]
        self.kernel32.ConnectNamedPipe.restype = ctypes.c_int
        self.kernel32.ReadFile.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(self.OVERLAPPED),
        ]
        self.kernel32.ReadFile.restype = ctypes.c_int
        self.kernel32.WriteFile.argtypes = list(
            self.kernel32.ReadFile.argtypes
        )
        self.kernel32.WriteFile.restype = ctypes.c_int
        self.kernel32.GetOverlappedResult.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(self.OVERLAPPED),
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.c_int,
        ]
        self.kernel32.GetOverlappedResult.restype = ctypes.c_int
        self.kernel32.CancelIoEx.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(self.OVERLAPPED),
        ]
        self.kernel32.CancelIoEx.restype = ctypes.c_int
        self.kernel32.CreateEventW.argtypes = [
            ctypes.c_void_p,
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_wchar_p,
        ]
        self.kernel32.CreateEventW.restype = ctypes.c_void_p
        self.kernel32.WaitForSingleObject.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        self.kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        self.kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        self.kernel32.CloseHandle.restype = ctypes.c_int
        self.kernel32.DisconnectNamedPipe.argtypes = [ctypes.c_void_p]
        self.kernel32.DisconnectNamedPipe.restype = ctypes.c_int
        self.kernel32.GetNamedPipeClientProcessId.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        self.kernel32.GetNamedPipeClientProcessId.restype = ctypes.c_int
        self.kernel32.GetNamedPipeServerProcessId.argtypes = list(
            self.kernel32.GetNamedPipeClientProcessId.argtypes
        )
        self.kernel32.GetNamedPipeServerProcessId.restype = ctypes.c_int
        self.kernel32.OpenProcess.argtypes = [
            ctypes.c_uint32,
            ctypes.c_int,
            ctypes.c_uint32,
        ]
        self.kernel32.OpenProcess.restype = ctypes.c_void_p
        self.kernel32.GetCurrentProcess.argtypes = []
        self.kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        self.advapi32.OpenProcessToken.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        self.advapi32.OpenProcessToken.restype = ctypes.c_int
        self.advapi32.GetTokenInformation.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        self.advapi32.GetTokenInformation.restype = ctypes.c_int
        self.advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        self.advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = (
            ctypes.c_int
        )
        self.advapi32.ConvertSidToStringSidW.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_wchar_p),
        ]
        self.advapi32.ConvertSidToStringSidW.restype = ctypes.c_int
        self.kernel32.LocalFree.argtypes = [ctypes.c_void_p]
        self.kernel32.LocalFree.restype = ctypes.c_void_p

    @staticmethod
    def _raise_last(label: str) -> None:
        code = ctypes.get_last_error()
        raise CryptoFirstLiveSupervisedAnchorError(
            f"{label}:win32-{code}"
        )


_WIN32_API: _Win32PipeApi | None = None


def _win32_api() -> _Win32PipeApi:
    global _WIN32_API
    if _WIN32_API is None:
        _WIN32_API = _Win32PipeApi()
    return _WIN32_API


def _windows_process_sid(process_id: int | None = None) -> str:
    api = _win32_api()
    process_handle: int | None = None
    close_process = False
    token_handle = ctypes.c_void_p()
    try:
        if process_id is None:
            process_handle = api.kernel32.GetCurrentProcess()
        else:
            process_handle = api.kernel32.OpenProcess(
                api.PROCESS_QUERY_LIMITED_INFORMATION,
                False,
                int(process_id),
            )
            close_process = True
            if not process_handle:
                api._raise_last("supervised-anchor-peer-process-unreadable")
        if not api.advapi32.OpenProcessToken(
            process_handle, api.TOKEN_QUERY, ctypes.byref(token_handle)
        ):
            api._raise_last("supervised-anchor-peer-token-unreadable")
        required = ctypes.c_uint32()
        api.advapi32.GetTokenInformation(
            token_handle,
            api.TOKEN_USER,
            None,
            0,
            ctypes.byref(required),
        )
        if (
            ctypes.get_last_error() != api.ERROR_INSUFFICIENT_BUFFER
            or required.value <= ctypes.sizeof(ctypes.c_void_p)
        ):
            api._raise_last("supervised-anchor-peer-token-invalid")
        buffer = ctypes.create_string_buffer(required.value)
        if not api.advapi32.GetTokenInformation(
            token_handle,
            api.TOKEN_USER,
            buffer,
            required.value,
            ctypes.byref(required),
        ):
            api._raise_last("supervised-anchor-peer-token-unreadable")
        sid_pointer = ctypes.cast(
            buffer, ctypes.POINTER(ctypes.c_void_p)
        ).contents.value
        sid_text = ctypes.c_wchar_p()
        if not sid_pointer or not api.advapi32.ConvertSidToStringSidW(
            sid_pointer, ctypes.byref(sid_text)
        ):
            api._raise_last("supervised-anchor-peer-sid-unreadable")
        try:
            return _exact_sid(sid_text.value, "peer-os-sid")
        finally:
            api.kernel32.LocalFree(sid_text)
    finally:
        if token_handle.value:
            api.kernel32.CloseHandle(token_handle)
        if close_process and process_handle:
            api.kernel32.CloseHandle(process_handle)


def _pipe_peer_process_id(handle: int, *, server_side: bool) -> int:
    api = _win32_api()
    process_id = ctypes.c_uint32()
    getter = (
        api.kernel32.GetNamedPipeClientProcessId
        if server_side
        else api.kernel32.GetNamedPipeServerProcessId
    )
    if not getter(handle, ctypes.byref(process_id)) or process_id.value <= 0:
        api._raise_last("supervised-anchor-pipe-peer-pid-unreadable")
    return int(process_id.value)


def _pipe_peer_sid(handle: int, *, server_side: bool) -> str:
    return _windows_process_sid(
        _pipe_peer_process_id(handle, server_side=server_side)
    )


def _remaining_milliseconds(deadline: float) -> int:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("supervised-anchor-pipe-timeout")
    return max(1, min(0xFFFFFFFE, int(remaining * 1000)))


def _wait_overlapped(
    handle: int,
    operation: Callable[[Any], tuple[bool, int]],
    *,
    deadline: float,
) -> int:
    api = _win32_api()
    event = api.kernel32.CreateEventW(None, True, False, None)
    if not event:
        api._raise_last("supervised-anchor-pipe-event-failed")
    overlapped = api.OVERLAPPED()
    overlapped.hEvent = event
    try:
        completed, immediate_error = operation(ctypes.byref(overlapped))
        if completed:
            transferred = ctypes.c_uint32()
            if not api.kernel32.GetOverlappedResult(
                handle,
                ctypes.byref(overlapped),
                ctypes.byref(transferred),
                False,
            ):
                api._raise_last("supervised-anchor-pipe-io-failed")
            return int(transferred.value)
        if immediate_error == api.ERROR_PIPE_CONNECTED:
            return 0
        if immediate_error != api.ERROR_IO_PENDING:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-io-failed:win32-"
                + str(immediate_error)
            )
        wait = api.kernel32.WaitForSingleObject(
            event, _remaining_milliseconds(deadline)
        )
        if wait == api.WAIT_TIMEOUT:
            api.kernel32.CancelIoEx(handle, ctypes.byref(overlapped))
            # The OVERLAPPED structure must remain alive until cancellation
            # completes.  Local named-pipe cancellation is expected to signal
            # immediately; the bounded grace wait prevents a use-after-free.
            api.kernel32.WaitForSingleObject(event, 1_000)
            raise TimeoutError("supervised-anchor-pipe-timeout")
        if wait != api.WAIT_OBJECT_0:
            api.kernel32.CancelIoEx(handle, ctypes.byref(overlapped))
            api.kernel32.WaitForSingleObject(event, 1_000)
            api._raise_last("supervised-anchor-pipe-wait-failed")
        transferred = ctypes.c_uint32()
        if not api.kernel32.GetOverlappedResult(
            handle,
            ctypes.byref(overlapped),
            ctypes.byref(transferred),
            False,
        ):
            api._raise_last("supervised-anchor-pipe-io-failed")
        return int(transferred.value)
    finally:
        api.kernel32.CloseHandle(event)


def _read_exact(handle: int, length: int, *, deadline: float) -> bytes:
    api = _win32_api()
    chunks: list[bytes] = []
    remaining = int(length)
    while remaining:
        size = min(remaining, 16_384)
        buffer = ctypes.create_string_buffer(size)

        def operation(overlapped: Any) -> tuple[bool, int]:
            ctypes.set_last_error(0)
            result = api.kernel32.ReadFile(
                handle, buffer, size, None, overlapped
            )
            return bool(result), ctypes.get_last_error()

        transferred = _wait_overlapped(
            handle, operation, deadline=deadline
        )
        if transferred <= 0 or transferred > size:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-truncated"
            )
        chunks.append(buffer.raw[:transferred])
        remaining -= transferred
    return b"".join(chunks)


def _write_all(handle: int, value: bytes, *, deadline: float) -> None:
    api = _win32_api()
    offset = 0
    while offset < len(value):
        chunk = value[offset : offset + 16_384]
        buffer = ctypes.create_string_buffer(chunk, len(chunk))

        def operation(overlapped: Any) -> tuple[bool, int]:
            ctypes.set_last_error(0)
            result = api.kernel32.WriteFile(
                handle, buffer, len(chunk), None, overlapped
            )
            return bool(result), ctypes.get_last_error()

        transferred = _wait_overlapped(
            handle, operation, deadline=deadline
        )
        if transferred <= 0 or transferred > len(chunk):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-write-truncated"
            )
        offset += transferred


class _WindowsLengthPrefixedPipeConnection:
    def __init__(
        self,
        handle: int,
        *,
        timeout_seconds: float,
        peer_process_id: int,
        peer_os_sid: str,
    ) -> None:
        self.handle = handle
        self.timeout_seconds = float(timeout_seconds)
        self.peer_process_id = int(peer_process_id)
        self.peer_os_sid = _exact_sid(peer_os_sid, "peer-os-sid")

    def send_bytes(self, raw: bytes) -> None:
        if not isinstance(raw, bytes) or not (0 < len(raw) <= _MAX_WIRE_BYTES):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-frame-invalid"
            )
        frame = struct.pack(">I", len(raw)) + raw
        _write_all(
            self.handle,
            frame,
            deadline=time.monotonic() + self.timeout_seconds,
        )

    def recv_bytes(self, maximum: int = _MAX_WIRE_BYTES) -> bytes:
        deadline = time.monotonic() + self.timeout_seconds
        prefix = _read_exact(self.handle, 4, deadline=deadline)
        length = int(struct.unpack(">I", prefix)[0])
        if not (0 < length <= min(int(maximum), _MAX_WIRE_BYTES)):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-frame-invalid"
            )
        return _read_exact(self.handle, length, deadline=deadline)

    def close(self, *, server_side: bool = False) -> None:
        del server_side
        handle, self.handle = self.handle, 0
        if not handle:
            return
        api = _win32_api()
        # Closing the server handle after an overlapped write preserves the
        # already-buffered response for the client.  DisconnectNamedPipe here
        # would discard unread response bytes and creates a timing-dependent
        # truncation race.
        api.kernel32.CloseHandle(handle)


def _create_secure_server_pipe(address: str, trader_os_sid: str) -> int:
    api = _win32_api()
    descriptor = ctypes.c_void_p()
    descriptor_size = ctypes.c_uint32()
    sddl = _windows_pipe_sddl(trader_os_sid)
    if not api.advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW(
        sddl,
        1,
        ctypes.byref(descriptor),
        ctypes.byref(descriptor_size),
    ):
        api._raise_last("supervised-anchor-pipe-sddl-invalid")
    attributes = api.SECURITY_ATTRIBUTES(
        ctypes.sizeof(api.SECURITY_ATTRIBUTES), descriptor, False
    )
    try:
        handle = api.kernel32.CreateNamedPipeW(
            address,
            api.PIPE_ACCESS_DUPLEX
            | api.FILE_FLAG_OVERLAPPED
            | api.FILE_FLAG_FIRST_PIPE_INSTANCE,
            api.PIPE_TYPE_BYTE
            | api.PIPE_READMODE_BYTE
            | api.PIPE_WAIT
            | api.PIPE_REJECT_REMOTE_CLIENTS,
            1,
            _MAX_FRAME_BYTES,
            _MAX_FRAME_BYTES,
            5_000,
            ctypes.byref(attributes),
        )
    finally:
        api.kernel32.LocalFree(descriptor)
    if not handle or handle == api.INVALID_HANDLE_VALUE:
        api._raise_last("supervised-anchor-pipe-create-failed")
    return int(handle)


class WindowsNamedPipeSupervisedAuthorityServer:
    """Single-client, local-only pipe server with exact peer SID checking."""

    def __init__(
        self,
        *,
        pipe_address: str,
        trader_os_sid: str,
        timeout_seconds: float = 5.0,
    ) -> None:
        address = _text(pipe_address)
        if _PIPE_RE.fullmatch(address) is None:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-address-invalid"
            )
        timeout = float(timeout_seconds)
        if not (0.1 <= timeout <= 15.0):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-timeout-invalid"
            )
        self.pipe_address = address
        self.trader_os_sid = _exact_sid(
            trader_os_sid, "trader-os-sid"
        )
        _windows_pipe_sddl(self.trader_os_sid)
        self.timeout_seconds = timeout

    def accept(
        self, *, connect_timeout_seconds: float = 1.0
    ) -> _WindowsLengthPrefixedPipeConnection | None:
        handle = _create_secure_server_pipe(
            self.pipe_address, self.trader_os_sid
        )
        api = _win32_api()
        try:
            deadline = time.monotonic() + float(connect_timeout_seconds)

            def operation(overlapped: Any) -> tuple[bool, int]:
                ctypes.set_last_error(0)
                result = api.kernel32.ConnectNamedPipe(handle, overlapped)
                return bool(result), ctypes.get_last_error()

            try:
                _wait_overlapped(handle, operation, deadline=deadline)
            except TimeoutError:
                api.kernel32.CloseHandle(handle)
                return None
            peer_process_id = _pipe_peer_process_id(
                handle, server_side=True
            )
            peer_sid = _windows_process_sid(peer_process_id)
            if not secrets.compare_digest(peer_sid, self.trader_os_sid):
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-pipe-client-sid-changed"
                )
            return _WindowsLengthPrefixedPipeConnection(
                handle,
                timeout_seconds=self.timeout_seconds,
                peer_process_id=peer_process_id,
                peer_os_sid=peer_sid,
            )
        except BaseException:
            api.kernel32.DisconnectNamedPipe(handle)
            api.kernel32.CloseHandle(handle)
            raise


def _connect_secure_client_pipe(
    address: str,
    *,
    expected_authority_os_sid: str,
    timeout_seconds: float,
) -> _WindowsLengthPrefixedPipeConnection:
    api = _win32_api()
    expected_sid = _exact_sid(
        expected_authority_os_sid, "authority-os-sid"
    )
    deadline = time.monotonic() + float(timeout_seconds)
    while True:
        try:
            milliseconds = min(250, _remaining_milliseconds(deadline))
        except TimeoutError as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-pipe-unavailable"
            ) from exc
        api.kernel32.WaitNamedPipeW(address, milliseconds)
        ctypes.set_last_error(0)
        handle = api.kernel32.CreateFileW(
            address,
            api.GENERIC_READ | api.GENERIC_WRITE,
            0,
            None,
            api.OPEN_EXISTING,
            api.FILE_FLAG_OVERLAPPED,
            None,
        )
        if handle and handle != api.INVALID_HANDLE_VALUE:
            peer_process_id = _pipe_peer_process_id(
                int(handle), server_side=False
            )
            peer_sid = _windows_process_sid(peer_process_id)
            if not secrets.compare_digest(peer_sid, expected_sid):
                api.kernel32.CloseHandle(handle)
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-pipe-server-sid-changed"
                )
            return _WindowsLengthPrefixedPipeConnection(
                int(handle),
                timeout_seconds=timeout_seconds,
                peer_process_id=peer_process_id,
                peer_os_sid=peer_sid,
            )
        error = ctypes.get_last_error()
        if error not in {
            api.ERROR_FILE_NOT_FOUND,
            api.ERROR_PIPE_BUSY,
            api.ERROR_ACCESS_DENIED,
        }:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-pipe-unavailable:win32-"
                + str(error)
            )
        if time.monotonic() >= deadline:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-pipe-unavailable"
            )
        time.sleep(0.01)


def _transport_mac(
    key: bytes, domain: bytes, value: Mapping[str, Any]
) -> str:
    return hmac.new(key, domain + _canonical(value), hashlib.sha256).hexdigest()


def encode_authenticated_pipe_request(
    command: Mapping[str, Any], *, authkey: bytes
) -> tuple[bytes, str]:
    nonce = secrets.token_hex(32)
    body = {
        "schemaVersion": _TRANSPORT_REQUEST_SCHEMA,
        "requestNonce": nonce,
        "command": dict(command),
    }
    return _canonical(
        {**body, "authMac": _transport_mac(authkey, _TRANSPORT_REQUEST_DOMAIN, body)}
    ), nonce


def decode_authenticated_pipe_request(
    raw: bytes, *, authkey: bytes
) -> tuple[dict[str, Any], str]:
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-request-invalid"
        ) from exc
    if not isinstance(envelope, dict) or set(envelope) != {
        "schemaVersion",
        "requestNonce",
        "command",
        "authMac",
    }:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-request-invalid"
        )
    body = {
        "schemaVersion": envelope.get("schemaVersion"),
        "requestNonce": envelope.get("requestNonce"),
        "command": envelope.get("command"),
    }
    nonce = _text(envelope.get("requestNonce")).lower()
    supplied = _text(envelope.get("authMac")).lower()
    if (
        envelope.get("schemaVersion") != _TRANSPORT_REQUEST_SCHEMA
        or _HASH_RE.fullmatch(nonce) is None
        or not isinstance(envelope.get("command"), dict)
        or _HASH_RE.fullmatch(supplied) is None
        or not hmac.compare_digest(
            supplied,
            _transport_mac(authkey, _TRANSPORT_REQUEST_DOMAIN, body),
        )
    ):
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-request-auth-invalid"
        )
    return dict(envelope["command"]), nonce


def encode_authenticated_pipe_response(
    response: Mapping[str, Any],
    *,
    request_nonce: str,
    request_id: str,
    authkey: bytes,
) -> bytes:
    body = {
        "schemaVersion": _TRANSPORT_RESPONSE_SCHEMA,
        "requestNonce": request_nonce,
        "requestId": request_id,
        "response": dict(response),
    }
    return _canonical(
        {**body, "authMac": _transport_mac(authkey, _TRANSPORT_RESPONSE_DOMAIN, body)}
    )


def decode_authenticated_pipe_response(
    raw: bytes,
    *,
    request_nonce: str,
    request_id: str,
    authkey: bytes,
) -> dict[str, Any]:
    try:
        envelope = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-response-invalid"
        ) from exc
    if not isinstance(envelope, dict) or set(envelope) != {
        "schemaVersion",
        "requestNonce",
        "requestId",
        "response",
        "authMac",
    }:
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-response-invalid"
        )
    body = {
        "schemaVersion": envelope.get("schemaVersion"),
        "requestNonce": envelope.get("requestNonce"),
        "requestId": envelope.get("requestId"),
        "response": envelope.get("response"),
    }
    supplied = _text(envelope.get("authMac")).lower()
    if (
        envelope.get("schemaVersion") != _TRANSPORT_RESPONSE_SCHEMA
        or envelope.get("requestNonce") != request_nonce
        or envelope.get("requestId") != request_id
        or not isinstance(envelope.get("response"), dict)
        or _HASH_RE.fullmatch(supplied) is None
        or not hmac.compare_digest(
            supplied,
            _transport_mac(authkey, _TRANSPORT_RESPONSE_DOMAIN, body),
        )
    ):
        raise CryptoFirstLiveSupervisedAnchorError(
            "supervised-anchor-pipe-response-auth-invalid"
        )
    return dict(envelope["response"])


class WindowsNamedPipeSupervisedAuthorityClient:
    """Bounded authenticated client for the separate authority process."""

    def __init__(
        self,
        *,
        pipe_address: str,
        pipe_authkey: bytes,
        timeout_seconds: float = 5.0,
        authority_os_sid: str = "S-1-5-18",
        trader_os_sid: str = "",
        connector: Callable[[str, bytes, float], Any] | None = None,
    ) -> None:
        address = _text(pipe_address)
        if _PIPE_RE.fullmatch(address) is None:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-address-invalid"
            )
        if not isinstance(pipe_authkey, bytes) or len(pipe_authkey) != 32:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-authkey-invalid"
            )
        try:
            timeout = float(timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-timeout-invalid"
            ) from exc
        if not (0.1 <= timeout <= 15.0):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-pipe-timeout-invalid"
            )
        self.pipe_address = address
        self._pipe_authkey = bytes(pipe_authkey)
        self.timeout_seconds = timeout
        self.authority_os_sid = _exact_sid(
            authority_os_sid, "authority-os-sid"
        )
        self._connector = connector
        if connector is None:
            current_sid = _windows_process_sid()
            self.trader_os_sid = _exact_sid(
                trader_os_sid or current_sid, "trader-os-sid"
            )
            if not secrets.compare_digest(current_sid, self.trader_os_sid):
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-trader-os-sid-changed"
                )
        else:
            self.trader_os_sid = (
                _exact_sid(trader_os_sid, "trader-os-sid")
                if trader_os_sid
                else ""
            )

    def __call__(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_anchor_request(value)
        request_id = "supervised-command-" + secrets.token_hex(18)
        command = {
            "schemaVersion": COMMAND_SCHEMA,
            "requestId": request_id,
            "request": request,
        }
        encoded, nonce = encode_authenticated_pipe_request(
            command, authkey=self._pipe_authkey
        )
        if len(encoded) > _MAX_WIRE_BYTES:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-command-too-large"
            )
        if self._connector is None:
            if not secrets.compare_digest(
                _windows_process_sid(), self.trader_os_sid
            ):
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-trader-os-sid-changed"
                )
            connection = _connect_secure_client_pipe(
                self.pipe_address,
                expected_authority_os_sid=self.authority_os_sid,
                timeout_seconds=self.timeout_seconds,
            )
        else:
            connection = self._connector(
                self.pipe_address, self._pipe_authkey, self.timeout_seconds
            )
        try:
            connection.send_bytes(encoded)
            raw = connection.recv_bytes(_MAX_WIRE_BYTES)
        except CryptoFirstLiveSupervisedAnchorError:
            raise
        except Exception as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-transport-failed:"
                + type(exc).__name__
            ) from exc
        finally:
            try:
                connection.close()
            except Exception:
                pass
        response = decode_authenticated_pipe_response(
            raw,
            request_nonce=nonce,
            request_id=request_id,
            authkey=self._pipe_authkey,
        )
        if (
            set(response)
            != {"schemaVersion", "requestId", "ok", "receipt", "error"}
            or response.get("schemaVersion") != RESPONSE_SCHEMA
            or response.get("requestId") != request_id
            or type(response.get("ok")) is not bool
            or not isinstance(response.get("error"), str)
        ):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-response-invalid"
            )
        if response["ok"] is not True:
            error = _text(response["error"])
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-rejected:"
                + (error if _ID_RE.fullmatch(error) else "authority-error")
            )
        if response["error"] != "" or not isinstance(
            response.get("receipt"), Mapping
        ):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-response-invalid"
            )
        return dict(response["receipt"])

    def status(self) -> dict[str, Any]:
        return {
            "configured": True,
            "transport": "WINDOWS_NAMED_PIPE_CTYPES_LENGTH_PREFIXED_HMAC",
            "pipeAddress": self.pipe_address,
            "timeoutSeconds": self.timeout_seconds,
            "authorityOsSid": self.authority_os_sid,
            "traderOsSid": self.trader_os_sid,
            "remoteClientsRejected": True,
            "maximumWireBytes": _MAX_WIRE_BYTES,
            "authorityPrivateKeyPresentInTrader": False,
            "networkOrderPostAllowed": False,
        }


class FastForwardGitSupervisedAuthority:
    """Authority-side monotonic CAS and Ed25519 receipt generator."""

    def __init__(
        self,
        *,
        authority_id: str,
        namespace_id: str,
        key_id: str,
        private_key: bytes | str,
        read_head: Callable[[], Mapping[str, Any] | None],
        fast_forward_append: Callable[[Mapping[str, Any]], str],
    ) -> None:
        self.authority_id = _exact_id(authority_id, "authority-id")
        self.namespace_id = _exact_id(namespace_id, "namespace-id")
        self.key_id = _exact_id(key_id, "key-id")
        try:
            key = ECC.import_key(private_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-private-key-invalid"
            ) from exc
        if not key.has_private() or getattr(key, "curve", None) != "Ed25519":
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-private-ed25519-key-required"
            )
        self._private_key = key
        self.read_head = read_head
        self.fast_forward_append = fast_forward_append

    def _validate_prior_state(self, value: Mapping[str, Any]) -> dict[str, Any]:
        prior = dict(value)
        state_fields = {
            "schemaVersion",
            "authorityId",
            "namespaceId",
            "sequence",
            "requestHash",
            "coordinatorDatabaseId",
            "coordinatorRevision",
            "publicationHash",
            "checkpointHash",
            "signedReceipt",
            "stateHash",
        }
        receipt_fields = {
            "schemaVersion",
            "authorityId",
            "namespaceId",
            "keyId",
            "checkpointId",
            "sequence",
            "requestHash",
            "coordinatorDatabaseId",
            "coordinatorRevision",
            "publicationHash",
            "priorCheckpointHash",
            "fastForwardOnly",
            "appendOnlyObserved",
            "durable",
            "restartVerifiable",
            "formalWorm",
            "checkpointHash",
            "signatureBase64",
        }
        if set(prior) != state_fields:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-head-invalid"
            )
        raw_receipt = prior.get("signedReceipt")
        if not isinstance(raw_receipt, Mapping):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-head-invalid"
            )
        signed_receipt = dict(raw_receipt)
        if set(signed_receipt) != receipt_fields:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-head-invalid"
            )
        signature_text = signed_receipt.pop("signatureBase64")
        checkpoint_body = {
            key: item
            for key, item in signed_receipt.items()
            if key != "checkpointHash"
        }
        try:
            signature = base64.b64decode(
                _text(signature_text), validate=True
            )
            if len(signature) != 64:
                raise ValueError("signature length")
            eddsa.new(
                self._private_key.public_key(), "rfc8032"
            ).verify(
                SIGNATURE_DOMAIN + _canonical(signed_receipt), signature
            )
        except (ValueError, TypeError) as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-head-invalid"
            ) from exc
        sequence = prior.get("sequence")
        if (
            prior.get("schemaVersion") != STATE_SCHEMA
            or prior.get("authorityId") != self.authority_id
            or prior.get("namespaceId") != self.namespace_id
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence <= 0
            or prior.get("stateHash")
            != _digest(
                {
                    key: item
                    for key, item in prior.items()
                    if key != "stateHash"
                }
            )
            or signed_receipt.get("schemaVersion") != RECEIPT_SCHEMA
            or signed_receipt.get("authorityId") != self.authority_id
            or signed_receipt.get("namespaceId") != self.namespace_id
            or signed_receipt.get("keyId") != self.key_id
            or signed_receipt.get("checkpointId")
            != f"supervised-git-checkpoint-{sequence:012d}"
            or signed_receipt.get("sequence") != sequence
            or signed_receipt.get("requestHash")
            != prior.get("requestHash")
            or signed_receipt.get("coordinatorDatabaseId")
            != prior.get("coordinatorDatabaseId")
            or signed_receipt.get("coordinatorRevision")
            != prior.get("coordinatorRevision")
            or signed_receipt.get("publicationHash")
            != prior.get("publicationHash")
            or signed_receipt.get("checkpointHash")
            != prior.get("checkpointHash")
            or signed_receipt.get("checkpointHash")
            != _digest(checkpoint_body)
            or signed_receipt.get("fastForwardOnly") is not True
            or signed_receipt.get("appendOnlyObserved") is not True
            or signed_receipt.get("durable") is not True
            or signed_receipt.get("restartVerifiable") is not True
            or signed_receipt.get("formalWorm") is not False
        ):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-head-invalid"
            )
        return prior

    def checkpoint(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_anchor_request(value)
        if (
            request["authorityId"] != self.authority_id
            or request["namespaceId"] != self.namespace_id
        ):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-authority-binding-changed"
            )
        prior_value = self.read_head()
        prior = dict(prior_value) if isinstance(prior_value, Mapping) else None
        request_hash = _digest(request)
        if prior is None:
            if request["priorCheckpointHash"] != "":
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-initial-cas-changed"
                )
            sequence = 1
        else:
            prior = self._validate_prior_state(prior)
            if request_hash == prior.get("requestHash"):
                return dict(prior["signedReceipt"])
            if (
                request["priorCheckpointHash"] != prior["checkpointHash"]
                or int(request["coordinatorRevision"])
                < int(prior["coordinatorRevision"])
                or request["coordinatorDatabaseId"]
                != prior["coordinatorDatabaseId"]
            ):
                raise CryptoFirstLiveSupervisedAnchorError(
                    "supervised-anchor-fast-forward-cas-changed"
                )
            sequence = int(prior["sequence"]) + 1
        checkpoint_body = {
            "schemaVersion": RECEIPT_SCHEMA,
            "authorityId": self.authority_id,
            "namespaceId": self.namespace_id,
            "keyId": self.key_id,
            "checkpointId": (
                f"supervised-git-checkpoint-{sequence:012d}"
            ),
            "sequence": sequence,
            "requestHash": request_hash,
            "coordinatorDatabaseId": request["coordinatorDatabaseId"],
            "coordinatorRevision": request["coordinatorRevision"],
            "publicationHash": request["publicationHash"],
            "priorCheckpointHash": request["priorCheckpointHash"],
            "fastForwardOnly": True,
            "appendOnlyObserved": True,
            "durable": True,
            "restartVerifiable": True,
            "formalWorm": False,
        }
        checkpoint_hash = _digest(checkpoint_body)
        receipt_body = {**checkpoint_body, "checkpointHash": checkpoint_hash}
        signature = eddsa.new(self._private_key, "rfc8032").sign(
            SIGNATURE_DOMAIN + _canonical(receipt_body)
        )
        signed_receipt = {
            **receipt_body,
            "signatureBase64": base64.b64encode(signature).decode("ascii"),
        }
        state_body = {
            "schemaVersion": STATE_SCHEMA,
            "authorityId": self.authority_id,
            "namespaceId": self.namespace_id,
            "sequence": sequence,
            "requestHash": request_hash,
            "coordinatorDatabaseId": request["coordinatorDatabaseId"],
            "coordinatorRevision": request["coordinatorRevision"],
            "publicationHash": request["publicationHash"],
            "checkpointHash": checkpoint_hash,
            "signedReceipt": signed_receipt,
        }
        state = {**state_body, "stateHash": _digest(state_body)}
        # The ref update happens before a receipt can leave the authority.
        # The Git object id is deliberately not embedded in the signed state:
        # doing so would create a commit-hash/content-hash cycle and would make
        # an idempotent retry return a different receipt shape.  The signed
        # checkpoint hash is the stable public receipt identity; the authority
        # process separately records the commit id in its operational log.
        remote_commit_id = _text(self.fast_forward_append(state)).lower()
        if _GIT_OBJECT_RE.fullmatch(remote_commit_id) is None:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-remote-commit-id-invalid"
            )
        return signed_receipt


class PinnedSupervisedAuditReceiptVerifier:
    """Trader-side public-key-only verifier and residual-risk projection."""

    def __init__(
        self,
        *,
        authority_id: str,
        namespace_id: str,
        key_id: str,
        public_key: bytes | str,
        receipt_reader: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    ) -> None:
        self.authority_id = _exact_id(authority_id, "authority-id")
        self.namespace_id = _exact_id(namespace_id, "namespace-id")
        self.key_id = _exact_id(key_id, "key-id")
        try:
            key = ECC.import_key(public_key)
        except (ValueError, TypeError, IndexError) as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-public-key-invalid"
            ) from exc
        if key.has_private() or getattr(key, "curve", None) != "Ed25519":
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-public-key-only-required"
            )
        self._public_key = key
        self.receipt_reader = receipt_reader

    def checkpoint(self, value: Mapping[str, Any]) -> dict[str, Any]:
        request = validate_anchor_request(value)
        raw = self.receipt_reader(request)
        if not isinstance(raw, Mapping):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-receipt-missing"
            )
        receipt = dict(raw)
        signature_text = receipt.pop("signatureBase64", "")
        if set(receipt) != {
            "schemaVersion",
            "authorityId",
            "namespaceId",
            "keyId",
            "checkpointId",
            "sequence",
            "requestHash",
            "coordinatorDatabaseId",
            "coordinatorRevision",
            "publicationHash",
            "priorCheckpointHash",
            "fastForwardOnly",
            "appendOnlyObserved",
            "durable",
            "restartVerifiable",
            "formalWorm",
            "checkpointHash",
        }:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-receipt-fields-not-exact"
            )
        checkpoint_body = {
            key: item for key, item in receipt.items() if key != "checkpointHash"
        }
        try:
            signature = base64.b64decode(_text(signature_text), validate=True)
            if len(signature) != 64:
                raise ValueError("signature length")
            eddsa.new(self._public_key, "rfc8032").verify(
                SIGNATURE_DOMAIN + _canonical(receipt), signature
            )
        except (ValueError, TypeError) as exc:
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-signature-invalid"
            ) from exc
        if (
            receipt.get("schemaVersion") != RECEIPT_SCHEMA
            or receipt.get("authorityId") != self.authority_id
            or receipt.get("namespaceId") != self.namespace_id
            or receipt.get("keyId") != self.key_id
            or receipt.get("requestHash") != _digest(request)
            or receipt.get("coordinatorDatabaseId")
            != request["coordinatorDatabaseId"]
            or receipt.get("coordinatorRevision")
            != request["coordinatorRevision"]
            or receipt.get("publicationHash") != request["publicationHash"]
            or receipt.get("priorCheckpointHash")
            != request["priorCheckpointHash"]
            or receipt.get("fastForwardOnly") is not True
            or receipt.get("appendOnlyObserved") is not True
            or receipt.get("durable") is not True
            or receipt.get("restartVerifiable") is not True
            or receipt.get("formalWorm") is not False
            or receipt.get("checkpointHash") != _digest(checkpoint_body)
        ):
            raise CryptoFirstLiveSupervisedAnchorError(
                "supervised-anchor-receipt-invalid"
            )
        projection_body = {
            "schemaVersion": PROJECTION_SCHEMA,
            "kind": "REMOTE_FAST_FORWARD_GIT_SIGNED",
            "authorityId": self.authority_id,
            "checkpointId": receipt["checkpointId"],
            "receiptHash": _digest(
                {**receipt, "signatureBase64": _text(signature_text)}
            ),
            "signatureVerified": True,
            "appendOnlyObserved": True,
            "durable": True,
            "restartVerifiable": True,
            "formalWorm": False,
        }
        return projection_body


__all__ = [
    "COMMAND_SCHEMA",
    "CryptoFirstLiveSupervisedAnchorError",
    "FastForwardGitSupervisedAuthority",
    "PinnedSupervisedAuditReceiptVerifier",
    "PROJECTION_SCHEMA",
    "RECEIPT_SCHEMA",
    "REQUEST_SCHEMA",
    "RESPONSE_SCHEMA",
    "SIGNATURE_DOMAIN",
    "STATE_SCHEMA",
    "WindowsNamedPipeSupervisedAuthorityClient",
    "WindowsNamedPipeSupervisedAuthorityServer",
    "decode_authenticated_pipe_request",
    "decode_authenticated_pipe_response",
    "encode_authenticated_pipe_request",
    "encode_authenticated_pipe_response",
    "validate_anchor_request",
]
