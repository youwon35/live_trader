from __future__ import annotations

import hashlib
import json
import unittest

from live_trader.binance_spot_supervised_observer_launch import (
    BinanceSupervisedObserverLaunchError,
    LAUNCH_ACK_SCHEMA,
    LAUNCH_RESPONSE_SCHEMA,
    PrearmedBinanceObserverPipeClient,
    build_prearmed_observer_launch_request,
)
from live_trader.crypto_first_live_supervised_anchor import (
    decode_authenticated_pipe_request,
    encode_authenticated_pipe_response,
)


NOW = 1_800_000_000.0
AUTHKEY = b"b" * 32


def digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def prepared_plan() -> dict[str, object]:
    body: dict[str, object] = {
        "schemaVersion": "binance-spot-supervised-prepared-inert-plan/v1",
        "assuranceMode": "SUPERVISED_NON_PROMOTION",
        "lane": "BINANCE_SPOT",
        "approvalId": "binance-observer-launch-approval-0001",
        "sessionId": "binance-observer-launch-session-0001",
        "permitId": "binance-observer-launch-permit-0001",
        "permitHash": "1" * 64,
        "accountFingerprint": "2" * 64,
        "preparedEpoch": NOW,
        "entryExpiresEpoch": NOW + 7200,
        "cleanupDeadlineEpoch": NOW + 10800,
        "controlRevision": 4,
        "networkCapabilityOpen": False,
        "promotionEligible": False,
        "realE2EEligible": False,
        "productionPromotionAllowed": False,
    }
    return {**body, "preparedPlanHash": digest(body)}


class FakeAuthorityConnection:
    def __init__(self, *, nonzero_get: bool = False) -> None:
        self.nonzero_get = nonzero_get
        self.sent = b""
        self.closed = False

    def send_bytes(self, value: bytes) -> None:
        self.sent = bytes(value)

    def recv_bytes(self, _maximum: int) -> bytes:
        command, nonce = decode_authenticated_pipe_request(
            self.sent, authkey=AUTHKEY
        )
        request_id = str(command["requestId"])
        request = dict(command["request"])
        body: dict[str, object] = {
            "schemaVersion": LAUNCH_ACK_SCHEMA,
            "requestId": request_id,
            "authorityId": request["authorityId"],
            "keyId": request["keyId"],
            "lane": request["lane"],
            "sessionId": request["sessionId"],
            "permitId": request["permitId"],
            "permitHash": request["permitHash"],
            "preparedPlanHash": request["preparedPlanHash"],
            "observerProcessId": 4242,
            "acceptedEpoch": NOW + 0.25,
            "coverageDeadlineEpoch": request["coverageDeadlineEpoch"],
            "prearmedBeforeRequest": True,
            "pipePeerVerified": True,
            "signedGetAttemptCountBeforeAck": 1 if self.nonzero_get else 0,
            "orderMutationAttemptCountBeforeAck": 0,
            "cancelMutationAttemptCountBeforeAck": 0,
            "transferMutationAttemptCountBeforeAck": 0,
            "withdrawMutationAttemptCountBeforeAck": 0,
            "marginMutationAttemptCountBeforeAck": 0,
            "futuresMutationAttemptCountBeforeAck": 0,
            "networkCapabilityOpen": False,
        }
        ack = {**body, "ackHash": digest(body)}
        response = {
            "schemaVersion": LAUNCH_RESPONSE_SCHEMA,
            "requestId": request_id,
            "ok": True,
            "ack": ack,
            "error": "",
        }
        return encode_authenticated_pipe_response(
            response,
            request_nonce=nonce,
            request_id=request_id,
            authkey=AUTHKEY,
        )

    def close(self) -> None:
        self.closed = True


class BinanceSupervisedObserverLaunchTest(unittest.TestCase):
    def request(self, *, clock=lambda: NOW):
        return build_prearmed_observer_launch_request(
            prepared_plan(),
            authority_id="binance-observer-authority-0001",
            key_id="binance-observer-key-0001",
            authorized_trader_pid=1234,
            authorized_trader_command_sha256="3" * 64,
            clock=clock,
        )

    def client(self, connection: FakeAuthorityConnection, *, released: bool):
        return PrearmedBinanceObserverPipeClient(
            pipe_address=r"\\.\pipe\binance-observer-launch-test",
            pipe_authkey=AUTHKEY,
            authority_os_sid="S-1-5-18",
            trader_os_sid="S-1-5-21-100-200-300-400",
            clock=lambda: NOW + 0.5,
            connector=lambda *_args: connection,
            allow_unreleased_for_test=released,
        )

    def test_release_hold_refuses_pipe_before_transport(self) -> None:
        connection = FakeAuthorityConnection()
        with self.assertRaisesRegex(
            BinanceSupervisedObserverLaunchError, "release-held"
        ):
            self.client(connection, released=False)(self.request())
        self.assertEqual(b"", connection.sent)

    def test_exact_prearmed_ack_has_zero_get_and_mutation_before_ack(self) -> None:
        connection = FakeAuthorityConnection()
        ack = self.client(connection, released=True)(self.request())
        self.assertEqual(0, ack["signedGetAttemptCountBeforeAck"])
        self.assertEqual(0, ack["orderMutationAttemptCountBeforeAck"])
        self.assertFalse(ack["networkCapabilityOpen"])
        self.assertTrue(connection.closed)

    def test_nonzero_get_before_ack_is_rejected(self) -> None:
        connection = FakeAuthorityConnection(nonzero_get=True)
        with self.assertRaisesRegex(
            BinanceSupervisedObserverLaunchError, "stale-or-capability-open"
        ):
            self.client(connection, released=True)(self.request())

    def test_manual_six_second_launch_gap_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            BinanceSupervisedObserverLaunchError, "stale-or-enabling"
        ):
            self.request(clock=lambda: NOW + 6.0)

    def test_nonfinite_or_noncanonical_plan_is_rejected(self) -> None:
        for field, value in (
            ("preparedEpoch", float("nan")),
            ("preparedPlanHash", "A" * 64),
        ):
            with self.subTest(field=field):
                plan = prepared_plan()
                plan[field] = value
                with self.assertRaises(BinanceSupervisedObserverLaunchError):
                    build_prearmed_observer_launch_request(
                        plan,
                        authority_id="binance-observer-authority-0001",
                        key_id="binance-observer-key-0001",
                        authorized_trader_pid=1234,
                        authorized_trader_command_sha256="3" * 64,
                        clock=lambda: NOW,
                    )


if __name__ == "__main__":
    unittest.main()
