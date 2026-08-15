from __future__ import annotations

"""False-gated authenticated launch seam for the Binance observer sidecar.

The trader sends only an already-durable PREPARED_INERT plan to a process
that was listening before that plan existed.  The acknowledgement is emitted
before the sidecar performs its user-stream subscription or three signed
baseline GETs.  A healthy signed observer snapshot remains a separate,
mandatory activation input; this acknowledgement never grants broker or
network authority by itself.
"""

import hashlib
import json
import math
import re
import secrets
import time
from typing import Any, Callable, Mapping

from .crypto_first_live_supervised_anchor import (
    _connect_secure_client_pipe,
    _windows_process_sid,
    decode_authenticated_pipe_response,
    encode_authenticated_pipe_request,
)


LAUNCH_REQUEST_SCHEMA = "binance-supervised-observer-launch-request/v1"
LAUNCH_COMMAND_SCHEMA = "binance-supervised-observer-launch-command/v1"
LAUNCH_RESPONSE_SCHEMA = "binance-supervised-observer-launch-response/v1"
LAUNCH_ACK_SCHEMA = "binance-supervised-observer-launch-ack/v1"
PREPARED_PLAN_SCHEMA = "binance-spot-supervised-prepared-inert-plan/v1"
PREARMED_OBSERVER_LAUNCH_RELEASED = False
MAX_COVERAGE_START_DELAY_SECONDS = 5.0
MAX_WIRE_BYTES = 65_536
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
_PIPE_RE = re.compile(r"^\\\\\.\\pipe\\[A-Za-z0-9._-]{8,120}$")
_SID_RE = re.compile(r"^S-1-(?:\d+-){1,14}\d+$", re.IGNORECASE)


class BinanceSupervisedObserverLaunchError(RuntimeError):
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
        raise BinanceSupervisedObserverLaunchError(label + "-invalid")
    return result


def _exact_hash(value: object, label: str) -> str:
    result = _text(value)
    if _HASH_RE.fullmatch(result) is None:
        raise BinanceSupervisedObserverLaunchError(label + "-invalid")
    return result


def validate_prearmed_observer_launch_request(
    value: Mapping[str, Any],
    *,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    request = dict(value)
    fields = {
        "schemaVersion",
        "assuranceMode",
        "lane",
        "authorityId",
        "keyId",
        "approvalId",
        "sessionId",
        "permitId",
        "permitHash",
        "accountFingerprint",
        "preparedPlanHash",
        "preparedEpoch",
        "coverageDeadlineEpoch",
        "authorizedTraderPid",
        "authorizedTraderCommandSha256",
        "requestedEpoch",
        "prearmedSidecarRequired",
        "networkCapabilityOpen",
    }
    if set(request) != fields:
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-request-fields-not-exact"
        )
    for field in (
        "authorityId",
        "keyId",
        "approvalId",
        "sessionId",
        "permitId",
    ):
        _exact_id(request.get(field), field)
    for field in (
        "permitHash",
        "accountFingerprint",
        "preparedPlanHash",
        "authorizedTraderCommandSha256",
    ):
        _exact_hash(request.get(field), field)
    if (
        type(request.get("preparedEpoch")) not in {int, float}
        or type(request.get("coverageDeadlineEpoch")) not in {int, float}
        or type(request.get("requestedEpoch")) not in {int, float}
        or type(request.get("authorizedTraderPid")) is not int
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-request-time-process-invalid"
        )
    prepared = float(request["preparedEpoch"])
    deadline = float(request["coverageDeadlineEpoch"])
    requested = float(request["requestedEpoch"])
    now = float(clock())
    if (
        request.get("schemaVersion") != LAUNCH_REQUEST_SCHEMA
        or request.get("assuranceMode") != "SUPERVISED_NON_PROMOTION"
        or request.get("lane") != "BINANCE_SPOT"
        or request.get("prearmedSidecarRequired") is not True
        or request.get("networkCapabilityOpen") is not False
        or int(request["authorizedTraderPid"]) <= 0
        or not all(math.isfinite(item) for item in (prepared, deadline, requested, now))
        or abs((deadline - prepared) - MAX_COVERAGE_START_DELAY_SECONDS) > 0.001
        or requested < prepared - 1.0
        or requested > deadline
        or requested > now + 1.0
        or now > deadline
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-request-stale-or-enabling"
        )
    return request


def build_prearmed_observer_launch_request(
    prepared_plan: Mapping[str, Any],
    *,
    authority_id: str,
    key_id: str,
    authorized_trader_pid: int,
    authorized_trader_command_sha256: str,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    plan = dict(prepared_plan)
    expected_plan_fields = {
        "schemaVersion",
        "assuranceMode",
        "lane",
        "approvalId",
        "sessionId",
        "permitId",
        "permitHash",
        "accountFingerprint",
        "preparedEpoch",
        "entryExpiresEpoch",
        "cleanupDeadlineEpoch",
        "controlRevision",
        "networkCapabilityOpen",
        "promotionEligible",
        "realE2EEligible",
        "productionPromotionAllowed",
        "preparedPlanHash",
    }
    if set(plan) != expected_plan_fields:
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-prepared-plan-fields-not-exact"
        )
    body = {key: item for key, item in plan.items() if key != "preparedPlanHash"}
    if (
        type(plan.get("preparedEpoch")) not in {int, float}
        or type(plan.get("entryExpiresEpoch")) not in {int, float}
        or type(plan.get("cleanupDeadlineEpoch")) not in {int, float}
        or type(plan.get("controlRevision")) is not int
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-prepared-plan-time-invalid"
        )
    try:
        prepared = float(plan["preparedEpoch"])
        expires = float(plan["entryExpiresEpoch"])
        cleanup = float(plan["cleanupDeadlineEpoch"])
        calculated_plan_hash = _digest(body)
    except (TypeError, ValueError) as exc:
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-prepared-plan-time-invalid"
        ) from exc
    if (
        plan.get("schemaVersion") != PREPARED_PLAN_SCHEMA
        or plan.get("assuranceMode") != "SUPERVISED_NON_PROMOTION"
        or plan.get("lane") != "BINANCE_SPOT"
        or plan.get("networkCapabilityOpen") is not False
        or plan.get("promotionEligible") is not False
        or plan.get("realE2EEligible") is not False
        or plan.get("productionPromotionAllowed") is not False
        or not all(math.isfinite(item) for item in (prepared, expires, cleanup))
        or abs((expires - prepared) - 7200.0) > 0.001
        or cleanup < expires
        or cleanup - prepared > 10800.001
        or int(plan["controlRevision"]) <= 0
        or _exact_hash(plan.get("preparedPlanHash"), "prepared-plan-hash")
        != calculated_plan_hash
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-prepared-plan-invalid"
        )
    request = {
        "schemaVersion": LAUNCH_REQUEST_SCHEMA,
        "assuranceMode": "SUPERVISED_NON_PROMOTION",
        "lane": "BINANCE_SPOT",
        "authorityId": authority_id,
        "keyId": key_id,
        "approvalId": plan["approvalId"],
        "sessionId": plan["sessionId"],
        "permitId": plan["permitId"],
        "permitHash": plan["permitHash"],
        "accountFingerprint": plan["accountFingerprint"],
        "preparedPlanHash": plan["preparedPlanHash"],
        "preparedEpoch": prepared,
        "coverageDeadlineEpoch": (
            prepared + MAX_COVERAGE_START_DELAY_SECONDS
        ),
        "authorizedTraderPid": authorized_trader_pid,
        "authorizedTraderCommandSha256": authorized_trader_command_sha256,
        "requestedEpoch": float(clock()),
        "prearmedSidecarRequired": True,
        "networkCapabilityOpen": False,
    }
    return validate_prearmed_observer_launch_request(request, clock=clock)


def validate_prearmed_observer_launch_ack(
    value: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    request_id: str,
    clock: Callable[[], float] = time.time,
) -> dict[str, Any]:
    ack = dict(value)
    fields = {
        "schemaVersion",
        "requestId",
        "authorityId",
        "keyId",
        "lane",
        "sessionId",
        "permitId",
        "permitHash",
        "preparedPlanHash",
        "observerProcessId",
        "acceptedEpoch",
        "coverageDeadlineEpoch",
        "prearmedBeforeRequest",
        "pipePeerVerified",
        "signedGetAttemptCountBeforeAck",
        "orderMutationAttemptCountBeforeAck",
        "cancelMutationAttemptCountBeforeAck",
        "transferMutationAttemptCountBeforeAck",
        "withdrawMutationAttemptCountBeforeAck",
        "marginMutationAttemptCountBeforeAck",
        "futuresMutationAttemptCountBeforeAck",
        "networkCapabilityOpen",
        "ackHash",
    }
    if set(ack) != fields:
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-ack-fields-not-exact"
        )
    body = {key: item for key, item in ack.items() if key != "ackHash"}
    expected = {
        "schemaVersion": LAUNCH_ACK_SCHEMA,
        "requestId": request_id,
        "authorityId": request["authorityId"],
        "keyId": request["keyId"],
        "lane": "BINANCE_SPOT",
        "sessionId": request["sessionId"],
        "permitId": request["permitId"],
        "permitHash": request["permitHash"],
        "preparedPlanHash": request["preparedPlanHash"],
        "coverageDeadlineEpoch": request["coverageDeadlineEpoch"],
    }
    if any(ack.get(key) != item for key, item in expected.items()):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-ack-binding-changed"
        )
    if (
        type(ack.get("observerProcessId")) is not int
        or type(ack.get("acceptedEpoch")) not in {int, float}
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-ack-process-time-invalid"
        )
    accepted = float(ack["acceptedEpoch"])
    now = float(clock())
    zero_fields = (
        "signedGetAttemptCountBeforeAck",
        "orderMutationAttemptCountBeforeAck",
        "cancelMutationAttemptCountBeforeAck",
        "transferMutationAttemptCountBeforeAck",
        "withdrawMutationAttemptCountBeforeAck",
        "marginMutationAttemptCountBeforeAck",
        "futuresMutationAttemptCountBeforeAck",
    )
    if (
        int(ack["observerProcessId"]) <= 0
        or not math.isfinite(accepted)
        or not math.isfinite(now)
        or accepted < float(request["requestedEpoch"]) - 1.0
        or accepted > float(request["coverageDeadlineEpoch"])
        or accepted > now + 1.0
        or any(type(ack.get(field)) is not int or ack.get(field) != 0 for field in zero_fields)
        or ack.get("prearmedBeforeRequest") is not True
        or ack.get("pipePeerVerified") is not True
        or ack.get("networkCapabilityOpen") is not False
        or _HASH_RE.fullmatch(_text(ack.get("ackHash"))) is None
        or not secrets.compare_digest(_text(ack["ackHash"]), _digest(body))
    ):
        raise BinanceSupervisedObserverLaunchError(
            "observer-launch-ack-stale-or-capability-open"
        )
    return ack


class PrearmedBinanceObserverPipeClient:
    """Authenticated exact-plan client for the already-listening sidecar."""

    def __init__(
        self,
        *,
        pipe_address: str,
        pipe_authkey: bytes,
        authority_os_sid: str,
        trader_os_sid: str = "",
        timeout_seconds: float = 5.0,
        clock: Callable[[], float] = time.time,
        connector: Callable[[str, bytes, float], Any] | None = None,
        allow_unreleased_for_test: bool = False,
    ) -> None:
        address = _text(pipe_address)
        if _PIPE_RE.fullmatch(address) is None:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-pipe-address-invalid"
            )
        if not isinstance(pipe_authkey, bytes) or len(pipe_authkey) != 32:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-pipe-authkey-invalid"
            )
        authority_sid = _text(authority_os_sid).upper()
        if _SID_RE.fullmatch(authority_sid) is None:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-authority-sid-invalid"
            )
        timeout = float(timeout_seconds)
        if not 0.1 <= timeout <= 15.0:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-timeout-invalid"
            )
        self.pipe_address = address
        self._pipe_authkey = bytes(pipe_authkey)
        self.authority_os_sid = authority_sid
        self.trader_os_sid = _text(trader_os_sid).upper()
        self.timeout_seconds = timeout
        self.clock = clock
        self.connector = connector
        self.allow_unreleased_for_test = bool(allow_unreleased_for_test)

    def __call__(self, value: Mapping[str, Any]) -> dict[str, Any]:
        if (
            not PREARMED_OBSERVER_LAUNCH_RELEASED
            and not self.allow_unreleased_for_test
        ):
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-release-held"
            )
        request = validate_prearmed_observer_launch_request(
            value, clock=self.clock
        )
        request_id = "binance-observer-launch-" + secrets.token_hex(18)
        command = {
            "schemaVersion": LAUNCH_COMMAND_SCHEMA,
            "requestId": request_id,
            "request": request,
        }
        encoded, nonce = encode_authenticated_pipe_request(
            command, authkey=self._pipe_authkey
        )
        if len(encoded) > MAX_WIRE_BYTES:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-command-too-large"
            )
        try:
            if self.connector is None:
                current_sid = _windows_process_sid()
                if self.trader_os_sid and not secrets.compare_digest(
                    current_sid, self.trader_os_sid
                ):
                    raise BinanceSupervisedObserverLaunchError(
                        "observer-launch-trader-sid-changed"
                    )
                connection = _connect_secure_client_pipe(
                    self.pipe_address,
                    expected_authority_os_sid=self.authority_os_sid,
                    timeout_seconds=self.timeout_seconds,
                )
            else:
                connection = self.connector(
                    self.pipe_address,
                    self._pipe_authkey,
                    self.timeout_seconds,
                )
            try:
                connection.send_bytes(encoded)
                raw = connection.recv_bytes(MAX_WIRE_BYTES)
            finally:
                connection.close()
        except BinanceSupervisedObserverLaunchError:
            raise
        except Exception as exc:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-transport-failed:" + type(exc).__name__
            ) from exc
        response = decode_authenticated_pipe_response(
            raw,
            request_nonce=nonce,
            request_id=request_id,
            authkey=self._pipe_authkey,
        )
        if (
            set(response)
            != {"schemaVersion", "requestId", "ok", "ack", "error"}
            or response.get("schemaVersion") != LAUNCH_RESPONSE_SCHEMA
            or response.get("requestId") != request_id
            or type(response.get("ok")) is not bool
            or not isinstance(response.get("error"), str)
        ):
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-response-invalid"
            )
        if response["ok"] is not True:
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-authority-rejected"
            )
        if response["error"] or not isinstance(response.get("ack"), Mapping):
            raise BinanceSupervisedObserverLaunchError(
                "observer-launch-response-invalid"
            )
        return validate_prearmed_observer_launch_ack(
            response["ack"],
            request=request,
            request_id=request_id,
            clock=self.clock,
        )

    def status(self) -> dict[str, Any]:
        return {
            "configured": True,
            "released": PREARMED_OBSERVER_LAUNCH_RELEASED,
            "transport": "WINDOWS_NAMED_PIPE_LENGTH_PREFIXED_HMAC_PEER_SID",
            "pipeAddress": self.pipe_address,
            "authorityOsSid": self.authority_os_sid,
            "maximumCoverageStartDelaySeconds": (
                MAX_COVERAGE_START_DELAY_SECONDS
            ),
            "ackBeforeBrokerGet": True,
            "ackBeforeBrokerMutation": True,
            "networkCapabilityOpen": False,
            "promotionEligible": False,
            "realE2EEligible": False,
        }


__all__ = [
    "BinanceSupervisedObserverLaunchError",
    "LAUNCH_ACK_SCHEMA",
    "LAUNCH_COMMAND_SCHEMA",
    "LAUNCH_REQUEST_SCHEMA",
    "LAUNCH_RESPONSE_SCHEMA",
    "MAX_COVERAGE_START_DELAY_SECONDS",
    "PREARMED_OBSERVER_LAUNCH_RELEASED",
    "PrearmedBinanceObserverPipeClient",
    "build_prearmed_observer_launch_request",
    "validate_prearmed_observer_launch_ack",
    "validate_prearmed_observer_launch_request",
]
