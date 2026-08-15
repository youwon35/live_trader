from __future__ import annotations

import hashlib
import json
import os
import secrets
import sys
import threading
import unittest

from Crypto.PublicKey import ECC

from live_trader.crypto_first_live_supervised_anchor import (
    COMMAND_SCHEMA,
    RESPONSE_SCHEMA,
    CryptoFirstLiveSupervisedAnchorError,
    FastForwardGitSupervisedAuthority,
    PinnedSupervisedAuditReceiptVerifier,
    WindowsNamedPipeSupervisedAuthorityClient,
    WindowsNamedPipeSupervisedAuthorityServer,
    _windows_process_sid,
    _windows_pipe_sddl,
    decode_authenticated_pipe_request,
    decode_authenticated_pipe_response,
    encode_authenticated_pipe_request,
    encode_authenticated_pipe_response,
)


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def request(*, revision: int, prior: str = "") -> dict:
    return {
        "schemaVersion": "crypto-first-live-supervised-anchor-request/v1",
        "kind": "REMOTE_FAST_FORWARD_GIT_SIGNED",
        "authorityId": "supervised-authority-0001",
        "namespaceId": "supervised-namespace-0001",
        "lane": "UPBIT",
        "sessionId": "supervised-session-0001",
        "permitId": "supervised-permit-0001",
        "permitHash": digest({"permit": 1}),
        "coordinatorDatabaseId": "coordinator-database-0001",
        "coordinatorRevision": revision,
        "publicationHash": digest({"publication": revision}),
        "priorCheckpointHash": prior,
        "eventType": "ACTIVATION",
    }


class MemoryFastForwardRemote:
    def __init__(self) -> None:
        self.head = None
        self.append_count = 0

    def read(self):
        if self.head is None:
            return None
        return json.loads(json.dumps(self.head))

    def append(self, value):
        self.append_count += 1
        self.head = json.loads(json.dumps(value))
        return digest({"commit": self.append_count, "state": self.head})


class SupervisedAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.private = ECC.generate(curve="Ed25519")
        self.public = self.private.public_key()
        self.remote = MemoryFastForwardRemote()
        self.authority = FastForwardGitSupervisedAuthority(
            authority_id="supervised-authority-0001",
            namespace_id="supervised-namespace-0001",
            key_id="supervised-key-0001",
            private_key=self.private.export_key(format="PEM"),
            read_head=self.remote.read,
            fast_forward_append=self.remote.append,
        )
        self.verifier = PinnedSupervisedAuditReceiptVerifier(
            authority_id="supervised-authority-0001",
            namespace_id="supervised-namespace-0001",
            key_id="supervised-key-0001",
            public_key=self.public.export_key(format="PEM"),
            receipt_reader=self.authority.checkpoint,
        )

    def test_signed_fast_forward_projection_is_non_promotion_only(self) -> None:
        projection = self.verifier.checkpoint(request(revision=1))
        self.assertEqual(
            "crypto-first-live-supervised-audit-anchor/v1",
            projection["schemaVersion"],
        )
        self.assertTrue(projection["signatureVerified"])
        self.assertTrue(projection["appendOnlyObserved"])
        self.assertFalse(projection["formalWorm"])
        self.assertFalse(self.verifier._public_key.has_private())
        self.assertEqual(1, self.remote.append_count)

    def test_retry_after_push_is_idempotent_and_shape_stable(self) -> None:
        value = request(revision=1)
        first = self.authority.checkpoint(value)
        second = self.authority.checkpoint(value)
        self.assertEqual(first, second)
        self.assertNotIn("remoteCommitId", first)
        self.assertEqual(1, self.remote.append_count)

    def test_monotonic_second_checkpoint_and_rollback_rejection(self) -> None:
        first = self.authority.checkpoint(request(revision=2))
        second_request = request(
            revision=3, prior=first["checkpointHash"]
        )
        second = self.authority.checkpoint(second_request)
        self.assertEqual(2, second["sequence"])
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError,
            "fast-forward-cas-changed",
        ):
            self.authority.checkpoint(
                request(
                    revision=1,
                    prior=second["checkpointHash"],
                )
            )

    def test_signature_tamper_is_rejected(self) -> None:
        signed = self.authority.checkpoint(request(revision=1))
        signed["coordinatorRevision"] = 99
        verifier = PinnedSupervisedAuditReceiptVerifier(
            authority_id="supervised-authority-0001",
            namespace_id="supervised-namespace-0001",
            key_id="supervised-key-0001",
            public_key=self.public.export_key(format="PEM"),
            receipt_reader=lambda _request: signed,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError, "signature-invalid"
        ):
            verifier.checkpoint(request(revision=1))

    def test_forged_remote_prior_state_is_rejected_before_signing(self) -> None:
        first = self.authority.checkpoint(request(revision=1))
        self.remote.head["signedReceipt"]["coordinatorRevision"] = 999
        state_body = {
            key: item
            for key, item in self.remote.head.items()
            if key != "stateHash"
        }
        self.remote.head["stateHash"] = digest(state_body)
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError,
            "remote-head-invalid",
        ):
            self.authority.checkpoint(
                request(revision=2, prior=first["checkpointHash"])
            )

    def test_named_pipe_client_uses_exact_bounded_json_protocol(self) -> None:
        authority = self.authority

        class Connection:
            def __init__(self) -> None:
                self.sent = b""
                self.closed = False

            def send_bytes(self, raw: bytes) -> None:
                self.sent = raw

            def poll(self, _timeout: float) -> bool:
                return True

            def recv_bytes(self, _maximum: int) -> bytes:
                command, nonce = decode_authenticated_pipe_request(
                    self.sent, authkey=b"x" * 32
                )
                if command["schemaVersion"] != COMMAND_SCHEMA:
                    raise AssertionError("command schema drift")
                receipt = authority.checkpoint(command["request"])
                return encode_authenticated_pipe_response(
                    {
                        "schemaVersion": RESPONSE_SCHEMA,
                        "requestId": command["requestId"],
                        "ok": True,
                        "receipt": receipt,
                        "error": "",
                    },
                    request_nonce=nonce,
                    request_id=command["requestId"],
                    authkey=b"x" * 32,
                )

            def close(self) -> None:
                self.closed = True

        connection = Connection()
        client = WindowsNamedPipeSupervisedAuthorityClient(
            pipe_address=r"\\.\pipe\crypto-first-live-supervised",
            pipe_authkey=b"x" * 32,
            connector=lambda _address, _key, _timeout: connection,
        )
        verifier = PinnedSupervisedAuditReceiptVerifier(
            authority_id="supervised-authority-0001",
            namespace_id="supervised-namespace-0001",
            key_id="supervised-key-0001",
            public_key=self.public.export_key(format="PEM"),
            receipt_reader=client,
        )
        projected = verifier.checkpoint(request(revision=1))
        self.assertTrue(projected["signatureVerified"])
        self.assertTrue(connection.closed)

    def test_pipe_acl_is_protected_exact_allow_only(self) -> None:
        trader_sid = "S-1-5-21-1-2-3-1001"
        sddl = _windows_pipe_sddl(trader_sid)
        self.assertTrue(sddl.startswith("D:P"))
        self.assertIn("(D;;GA;;;AN)", sddl)
        self.assertIn("(A;;GA;;;SY)", sddl)
        self.assertIn("(A;;GA;;;BA)", sddl)
        self.assertIn(f"(A;;GRGW;;;{trader_sid})", sddl)
        self.assertNotIn(";;;WD)", sddl)
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError, "not-independent"
        ):
            _windows_pipe_sddl("S-1-5-18")

    def test_pipe_hmac_nonce_and_response_binding_fail_closed(self) -> None:
        command = {
            "schemaVersion": COMMAND_SCHEMA,
            "requestId": "supervised-command-0001",
            "request": request(revision=1),
        }
        encoded, nonce = encode_authenticated_pipe_request(
            command, authkey=b"a" * 32
        )
        decoded, decoded_nonce = decode_authenticated_pipe_request(
            encoded, authkey=b"a" * 32
        )
        self.assertEqual(command, decoded)
        self.assertEqual(nonce, decoded_nonce)
        tampered = json.loads(encoded.decode("utf-8"))
        tampered["command"]["requestId"] = "supervised-command-9999"
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError, "auth-invalid"
        ):
            decode_authenticated_pipe_request(
                json.dumps(tampered).encode("utf-8"), authkey=b"a" * 32
            )
        response = {
            "schemaVersion": RESPONSE_SCHEMA,
            "requestId": command["requestId"],
            "ok": False,
            "receipt": None,
            "error": "authority-request-rejected",
        }
        response_raw = encode_authenticated_pipe_response(
            response,
            request_nonce=nonce,
            request_id=command["requestId"],
            authkey=b"a" * 32,
        )
        with self.assertRaisesRegex(
            CryptoFirstLiveSupervisedAnchorError, "auth-invalid"
        ):
            decode_authenticated_pipe_response(
                response_raw,
                request_nonce="0" * 64,
                request_id=command["requestId"],
                authkey=b"a" * 32,
            )

    def test_pipe_authkey_is_exactly_32_bytes(self) -> None:
        for invalid in (b"x" * 31, b"x" * 33):
            with self.subTest(length=len(invalid)), self.assertRaisesRegex(
                CryptoFirstLiveSupervisedAnchorError, "authkey-invalid"
            ):
                WindowsNamedPipeSupervisedAuthorityClient(
                    pipe_address=r"\\.\pipe\crypto-first-live-supervised",
                    pipe_authkey=invalid,
                    connector=lambda *_args: None,
                )

    @unittest.skipUnless(sys.platform == "win32", "Windows pipe only")
    def test_ctypes_pipe_round_trip_revalidates_exact_peer_sid(self) -> None:
        current_sid = _windows_process_sid()
        address = (
            r"\\.\pipe\crypto-first-live-test-" + secrets.token_hex(8)
        )
        authkey = b"p" * 32
        authority = self.authority
        failures: list[BaseException] = []
        peer_process_ids: list[int] = []

        def server_worker() -> None:
            connection = None
            try:
                server = WindowsNamedPipeSupervisedAuthorityServer(
                    pipe_address=address,
                    trader_os_sid=current_sid,
                    timeout_seconds=3.0,
                )
                while connection is None:
                    connection = server.accept(connect_timeout_seconds=1.0)
                peer_process_ids.append(connection.peer_process_id)
                raw = connection.recv_bytes()
                command, nonce = decode_authenticated_pipe_request(
                    raw, authkey=authkey
                )
                receipt = authority.checkpoint(command["request"])
                response = {
                    "schemaVersion": RESPONSE_SCHEMA,
                    "requestId": command["requestId"],
                    "ok": True,
                    "receipt": receipt,
                    "error": "",
                }
                connection.send_bytes(
                    encode_authenticated_pipe_response(
                        response,
                        request_nonce=nonce,
                        request_id=command["requestId"],
                        authkey=authkey,
                    )
                )
            except BaseException as exc:  # pragma: no cover - surfaced below
                failures.append(exc)
            finally:
                if connection is not None:
                    connection.close(server_side=True)

        thread = threading.Thread(target=server_worker, daemon=True)
        thread.start()
        client = WindowsNamedPipeSupervisedAuthorityClient(
            pipe_address=address,
            pipe_authkey=authkey,
            timeout_seconds=3.0,
            authority_os_sid=current_sid,
            trader_os_sid=current_sid,
        )
        verifier = PinnedSupervisedAuditReceiptVerifier(
            authority_id="supervised-authority-0001",
            namespace_id="supervised-namespace-0001",
            key_id="supervised-key-0001",
            public_key=self.public.export_key(format="PEM"),
            receipt_reader=client,
        )
        projection = verifier.checkpoint(request(revision=1))
        thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertEqual([os.getpid()], peer_process_ids)
        self.assertTrue(projection["signatureVerified"])

    @unittest.skipUnless(sys.platform == "win32", "Windows pipe only")
    def test_ctypes_pipe_wrong_trader_sid_is_denied_or_rejected(self) -> None:
        current_sid = _windows_process_sid()
        wrong_sid = "S-1-5-21-1-2-3-424242"
        address = (
            r"\\.\pipe\crypto-first-live-hostile-" + secrets.token_hex(8)
        )
        server_failures: list[BaseException] = []
        received_frames: list[bytes] = []

        def server_worker() -> None:
            connection = None
            try:
                server = WindowsNamedPipeSupervisedAuthorityServer(
                    pipe_address=address,
                    trader_os_sid=wrong_sid,
                    timeout_seconds=0.5,
                )
                connection = server.accept(connect_timeout_seconds=0.75)
                if connection is not None:
                    received_frames.append(connection.recv_bytes())
            except BaseException as exc:
                server_failures.append(exc)
            finally:
                if connection is not None:
                    connection.close(server_side=True)

        thread = threading.Thread(target=server_worker, daemon=True)
        thread.start()
        client = WindowsNamedPipeSupervisedAuthorityClient(
            pipe_address=address,
            pipe_authkey=b"h" * 32,
            timeout_seconds=0.5,
            authority_os_sid=current_sid,
            trader_os_sid=current_sid,
        )
        with self.assertRaises(CryptoFirstLiveSupervisedAnchorError):
            client(request(revision=1))
        thread.join(timeout=3.0)
        self.assertFalse(thread.is_alive())
        # Either the DACL prevents CreateFile entirely or an elevated
        # Administrators token reaches the server and exact peer-SID checking
        # rejects it.  Neither route may produce a response.
        self.assertEqual([], received_frames)


if __name__ == "__main__":
    unittest.main()
