from __future__ import annotations

"""Disabled, offline-only KIS functional manager composition.

The manager deliberately has no production sender or shared-state entrypoint.  It
accepts only exact pinned mock adapters and proves that a mutation attempt was
made while the state-owned final mutation boundary was held.  All availability
flags remain false until a separate production integration replaces these
offline adapters with independently verified readers.
"""

from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
import re
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
KIS_DOMESTIC_FUNCTIONAL_MANAGER_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MANAGER_NETWORK_AVAILABLE = False
KIS_DOMESTIC_FUNCTIONAL_MANAGER_RELEASE_AVAILABLE = False

_COMPONENTS = (
    "state",
    "owner",
    "capability",
    "quote",
    "rolling",
    "heartbeat",
    "mutation",
    "transport",
)
_COMMANDS = frozenset({"START", "STOP", "KILL", "SETTINGS"})
_OPERATIONS = frozenset({"NATURAL_BUY", "CLEANUP_CANCEL", "CLEANUP_SELL"})
_ORDER_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-cash"
_CANCEL_ENDPOINT = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
_SHA = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", flags=re.ASCII)
_OFFICIAL = re.compile(r"^[0-9]{1,16}$", flags=re.ASCII)
_DATE = re.compile(r"^[0-9]{8}$", flags=re.ASCII)

_RESERVATION_KEYS = {
    "reservationId",
    "reservationKind",
    "revision",
    "sessionId",
    "reservedAt",
    "previousAccountFingerprint",
    "previousCredentialConfigurationHash",
    "reservedAccountFingerprint",
    "reservedCredentialConfigurationHash",
    "ownerEpochId",
    "ownerEpochHash",
    "componentReadersHash",
    "finalMutationBoundaryRequired",
    "finalMutationBoundaryHandleSchema",
    "finalMutationBoundaryHandle",
    "productionAvailable",
}
_BOUNDARY_KEYS = {
    "schemaVersion",
    "route",
    "reservationId",
    "reservationKind",
    "reservationRevision",
    "sessionId",
    "accountFingerprint",
    "credentialConfigurationHash",
    "ownerEpochHash",
    "componentReadersHash",
    "productionAvailable",
    "finalMutationBoundaryHandle",
    "routeLockHeld",
}
_STATUS_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "component",
    "implementationType",
    "codeHash",
    "protocolHash",
    "statusRevision",
    "statusHeadHash",
    "stateRevision",
    "sessionId",
    "accountFingerprint",
    "credentialConfigurationHash",
    "ownerEpochId",
    "ownerEpochHash",
    "hazards",
    "readable",
    "ready",
    "productionAvailable",
    "networkAvailable",
    "releaseEvidenceAvailable",
}
_MUTATION_STATUS_KEYS = _STATUS_KEYS | {
    "authoritativeMutationPlanHash",
    "ownedProjectionHash",
    "ownedProjectionHeadHash",
    "ownedProjectionRevision",
    "ownedProjectionObservedAt",
    "mutationPlanKeyIdHash",
}
_POSITION_KEYS = {
    "pdno",
    "baselineAccountQuantity",
    "currentAccountQuantity",
    "ownedDeltaQuantity",
    "sourceClaimId",
    "positionProofHash",
}
_ORDER_KEY_KEYS = {"orderDate", "organizationNo", "orderNo"}
_REQUEST_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "command",
    "operation",
    "claimId",
    "sessionId",
    "stateRevision",
    "ownerEpochId",
    "ownerEpochHash",
    "accountFingerprint",
    "credentialConfigurationHash",
    "endpoint",
    "payload",
    "payloadHash",
    "ownedOrderKey",
    "ownedPosition",
    "requestHash",
    "productionAvailable",
}
_STATE_RECEIPT_KEYS = {
    "schemaVersion",
    "route",
    "reservationId",
    "reservationKind",
    "reservationRevision",
    "sessionId",
    "accountFingerprint",
    "credentialConfigurationHash",
    "ownerEpochHash",
    "componentReadersHash",
    "managerReceiptHash",
    "executionProofHash",
    "mutationPlanHash",
    "ownedProjectionHash",
    "ownedProjectionHeadHash",
    "boundaryEntryProofHash",
    "attemptChainHead",
    "transportReceiptSetHash",
    "detachedBoundaryHazard",
    "pendingReservation",
    "reservationFinishAllowed",
    "reconciliationRequired",
    "ok",
    "mutationMayHaveOccurred",
    "occurredAt",
    "keyIdHash",
    "productionAvailable",
    "receiptHash",
    "signature",
}
_OWNED_PROJECTION_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "sessionId",
    "stateRevision",
    "ownerEpochId",
    "ownerEpochHash",
    "accountFingerprint",
    "credentialConfigurationHash",
    "observedAt",
    "revision",
    "headHash",
    "ownedWorkingOrders",
    "ownedPosition",
    "ownedWorkingOrderCount",
    "ownedPositionDelta",
    "ownedWorking0",
    "ownedDelta0",
    "productionAvailable",
    "projectionHash",
}
_AUTHORITY_PLAN_KEYS = {
    "schemaVersion",
    "route",
    "pdno",
    "planId",
    "planRevision",
    "reservationId",
    "reservationKind",
    "reservationRevision",
    "sessionId",
    "stateRevision",
    "ownerEpochId",
    "ownerEpochHash",
    "accountFingerprint",
    "credentialConfigurationHash",
    "ownedProjection",
    "ownedProjectionHash",
    "mutationRequests",
    "requestCount",
    "keyIdHash",
    "productionAvailable",
    "planHash",
    "signature",
}


class KisDomesticFunctionalManagerBlocked(RuntimeError):
    pass


def _canonical(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise KisDomesticFunctionalManagerBlocked(
            "manager evidence is not canonical"
        ) from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _sha(value: Any, label: str) -> str:
    if type(value) is not str or not _SHA.fullmatch(value):
        raise KisDomesticFunctionalManagerBlocked(f"{label} is invalid")
    return value


def _identity(value: Any, label: str, *, allow_empty: bool = False) -> str:
    if type(value) is not str or (
        not value and not allow_empty
    ) or (value and not _ID.fullmatch(value)):
        raise KisDomesticFunctionalManagerBlocked(f"{label} is invalid")
    return value


def _utc(value: Any, label: str) -> str:
    if type(value) is not str or not value:
        raise KisDomesticFunctionalManagerBlocked(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise KisDomesticFunctionalManagerBlocked(f"{label} is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise KisDomesticFunctionalManagerBlocked(f"{label} is not UTC-aware")
    return parsed.astimezone(timezone.utc).isoformat()


def _now_text(clock: Callable[[], float]) -> str:
    raw = clock()
    if type(raw) not in {int, float} or not math.isfinite(float(raw)):
        raise KisDomesticFunctionalManagerBlocked("manager wall clock is invalid")
    return datetime.fromtimestamp(float(raw), tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class OfflinePinnedKisManagerAdapter:
    """Exact offline reader adapter; it deliberately exposes no mutation method."""

    component: str
    implementation_type: str
    code_hash: str
    protocol_hash: str
    status_reader: Callable[[], Mapping[str, Any]]
    allow_mock: bool

    def __post_init__(self) -> None:
        if self.component not in _COMPONENTS:
            raise KisDomesticFunctionalManagerBlocked("adapter component is invalid")
        _identity(self.implementation_type, "adapter implementation type")
        _sha(self.code_hash, "adapter code hash")
        _sha(self.protocol_hash, "adapter protocol hash")
        if not callable(self.status_reader) or self.allow_mock is not True:
            raise KisDomesticFunctionalManagerBlocked(
                "only an explicit offline mock adapter is accepted"
            )

    def binding(self) -> dict[str, Any]:
        return {
            "component": self.component,
            "implementationType": self.implementation_type,
            "codeHash": self.code_hash,
            "protocolHash": self.protocol_hash,
            "mockOnly": True,
            "productionAvailable": False,
        }

    def status(self) -> dict[str, Any]:
        try:
            raw = self.status_reader()
        except Exception as exc:
            raise KisDomesticFunctionalManagerBlocked(
                f"{self.component} status reader failed"
            ) from exc
        expected_keys = (
            _MUTATION_STATUS_KEYS if self.component == "mutation" else _STATUS_KEYS
        )
        if not isinstance(raw, Mapping) or set(raw) != expected_keys:
            raise KisDomesticFunctionalManagerBlocked(
                f"{self.component} status shape is not exact"
            )
        value = dict(raw)
        exact = {
            "schemaVersion": "kis-domestic-functional-manager-component-status/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "component": self.component,
            "implementationType": self.implementation_type,
            "codeHash": self.code_hash,
            "protocolHash": self.protocol_hash,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseEvidenceAvailable": False,
        }
        for key, expected in exact.items():
            if type(value.get(key)) is not type(expected) or value.get(key) != expected:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{self.component} status {key} mismatch"
                )
        for key in ("statusRevision", "stateRevision"):
            if type(value[key]) is not int or value[key] < 1:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{self.component} {key} is invalid"
                )
        for key in (
            "statusHeadHash",
            "accountFingerprint",
            "credentialConfigurationHash",
            "ownerEpochHash",
        ):
            _sha(value[key], f"{self.component} {key}")
        _identity(value["ownerEpochId"], f"{self.component} owner epoch")
        _identity(value["sessionId"], f"{self.component} session", allow_empty=True)
        if type(value["hazards"]) is not list or any(
            type(item) is not str or not _ID.fullmatch(item)
            for item in value["hazards"]
        ) or value["hazards"] != sorted(set(value["hazards"])):
            raise KisDomesticFunctionalManagerBlocked(
                f"{self.component} hazards are invalid"
            )
        for key in ("readable", "ready"):
            if type(value[key]) is not bool:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{self.component} status {key} is invalid"
                )
        if value["readable"] is not True:
            raise KisDomesticFunctionalManagerBlocked(
                f"{self.component} is unreadable"
            )
        if self.component == "mutation":
            for key in (
                "authoritativeMutationPlanHash",
                "ownedProjectionHash",
                "ownedProjectionHeadHash",
                "mutationPlanKeyIdHash",
            ):
                _sha(value[key], f"mutation {key}")
            if (
                type(value["ownedProjectionRevision"]) is not int
                or value["ownedProjectionRevision"] < 1
            ):
                raise KisDomesticFunctionalManagerBlocked(
                    "mutation owned projection revision is invalid"
                )
            _utc(
                value["ownedProjectionObservedAt"],
                "mutation owned projection observation",
            )
        return value


class OfflinePinnedKisStateAdapter(OfflinePinnedKisManagerAdapter):
    def __init__(
        self,
        *,
        implementation_type: str,
        code_hash: str,
        protocol_hash: str,
        status_reader: Callable[[], Mapping[str, Any]],
        final_boundary_factory: Callable[
            [Mapping[str, Any]], AbstractContextManager[Mapping[str, Any]]
        ],
        allow_mock: bool,
    ) -> None:
        super().__init__(
            component="state",
            implementation_type=implementation_type,
            code_hash=code_hash,
            protocol_hash=protocol_hash,
            status_reader=status_reader,
            allow_mock=allow_mock,
        )
        if not callable(final_boundary_factory):
            raise KisDomesticFunctionalManagerBlocked(
                "state final boundary factory is invalid"
            )
        object.__setattr__(self, "_final_boundary_factory", final_boundary_factory)

    def final_mutation_boundary(
        self, reservation: Mapping[str, Any]
    ) -> AbstractContextManager[Mapping[str, Any]]:
        return self._final_boundary_factory(dict(reservation))


class OfflinePinnedKisMutationAdapter(OfflinePinnedKisManagerAdapter):
    """Verify-only authoritative plan/projection reader.

    The state reservation binds this adapter's signed plan and owned-ledger
    heads through ``componentReadersHash``.  The manager re-reads the same plan
    after entering the state-owned route boundary before any transport call.
    """

    def __init__(
        self,
        *,
        implementation_type: str,
        code_hash: str,
        protocol_hash: str,
        status_reader: Callable[[], Mapping[str, Any]],
        plan_reader: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
        plan_verifier: Callable[[Mapping[str, Any]], bool],
        allow_mock: bool,
    ) -> None:
        super().__init__(
            component="mutation",
            implementation_type=implementation_type,
            code_hash=code_hash,
            protocol_hash=protocol_hash,
            status_reader=status_reader,
            allow_mock=allow_mock,
        )
        if not callable(plan_reader) or not callable(plan_verifier):
            raise KisDomesticFunctionalManagerBlocked(
                "mutation plan reader/verifier is invalid"
            )
        object.__setattr__(self, "_plan_reader", plan_reader)
        object.__setattr__(self, "_plan_verifier", plan_verifier)

    def authoritative_plan(
        self,
        *,
        reservation: Mapping[str, Any],
        command: str,
    ) -> dict[str, Any]:
        try:
            raw = self._plan_reader(dict(reservation), command)
        except Exception as exc:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative mutation plan read failed"
            ) from exc
        value = _authority_plan(raw, reservation=reservation, command=command)
        try:
            verified = self._plan_verifier(dict(value))
        except Exception:
            verified = False
        if verified is not True:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative mutation plan signature is unverified"
            )
        return value


class OfflinePinnedKisTransportAdapter(OfflinePinnedKisManagerAdapter):
    def __init__(
        self,
        *,
        implementation_type: str,
        code_hash: str,
        protocol_hash: str,
        status_reader: Callable[[], Mapping[str, Any]],
        mutation_sender: Callable[
            [Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]
        ],
        receipt_verifier: Callable[[Mapping[str, Any]], bool],
        allow_mock: bool,
    ) -> None:
        super().__init__(
            component="transport",
            implementation_type=implementation_type,
            code_hash=code_hash,
            protocol_hash=protocol_hash,
            status_reader=status_reader,
            allow_mock=allow_mock,
        )
        if not callable(mutation_sender) or not callable(receipt_verifier):
            raise KisDomesticFunctionalManagerBlocked(
                "transport mock sender/verifier is invalid"
            )
        object.__setattr__(self, "_mutation_sender", mutation_sender)
        object.__setattr__(self, "_receipt_verifier", receipt_verifier)

    def mutate(
        self,
        request: Mapping[str, Any],
        attempt: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            raw = self._mutation_sender(dict(request), dict(attempt))
        except Exception:
            raise
        if not isinstance(raw, Mapping):
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt is not a mapping"
            )
        value = dict(raw)
        keys = {
            "schemaVersion",
            "route",
            "pdno",
            "operation",
            "claimId",
            "requestHash",
            "attemptProofHash",
            "status",
            "mutationMayHaveOccurred",
            "occurredAt",
            "signerKeyIdHash",
            "productionAvailable",
            "networkAvailable",
            "receiptHash",
            "signature",
        }
        if set(value) != keys:
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt shape is not exact"
            )
        expected = {
            "schemaVersion": "kis-domestic-functional-mock-transport-receipt/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "operation": request["operation"],
            "claimId": request["claimId"],
            "requestHash": request["requestHash"],
            "attemptProofHash": attempt["attemptProofHash"],
            "productionAvailable": False,
            "networkAvailable": False,
        }
        if any(
            type(value.get(key)) is not type(wanted) or value.get(key) != wanted
            for key, wanted in expected.items()
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt binding mismatch"
            )
        if value["status"] not in {"ACKNOWLEDGED", "NOT_SENT", "UNKNOWN"}:
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt status is invalid"
            )
        if type(value["mutationMayHaveOccurred"]) is not bool:
            raise KisDomesticFunctionalManagerBlocked(
                "transport ambiguity flag is invalid"
            )
        if value["status"] == "ACKNOWLEDGED" and value["mutationMayHaveOccurred"] is not True:
            raise KisDomesticFunctionalManagerBlocked(
                "transport ACK cannot deny mutation"
            )
        _utc(value["occurredAt"], "transport receipt time")
        for key in ("signerKeyIdHash", "receiptHash", "signature"):
            _sha(value[key], f"transport {key}")
        unsigned = dict(value)
        unsigned.pop("signature")
        receipt_hash = unsigned.pop("receiptHash")
        if not hmac.compare_digest(receipt_hash, _hash(unsigned)):
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt hash mismatch"
            )
        try:
            verified = self._receipt_verifier(dict(value))
        except Exception as exc:
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt verifier failed"
            ) from exc
        if type(verified) is not bool or verified is not True:
            raise KisDomesticFunctionalManagerBlocked(
                "transport receipt signature is unverified"
            )
        return value


def _reservation(value: Mapping[str, Any], *, command: str) -> dict[str, Any]:
    if command not in _COMMANDS:
        raise KisDomesticFunctionalManagerBlocked("manager command is invalid")
    if not isinstance(value, Mapping) or set(value) != _RESERVATION_KEYS:
        raise KisDomesticFunctionalManagerBlocked("state reservation is not exact")
    result = dict(value)
    if result["reservationKind"] != command:
        raise KisDomesticFunctionalManagerBlocked("reservation command mismatch")
    _identity(result["reservationId"], "reservation id")
    _identity(result["sessionId"], "reservation session", allow_empty=command == "SETTINGS")
    if type(result["revision"]) is not int or result["revision"] < 2:
        raise KisDomesticFunctionalManagerBlocked("reservation revision is invalid")
    _utc(result["reservedAt"], "reservation time")
    for key in (
        "previousAccountFingerprint",
        "previousCredentialConfigurationHash",
        "reservedAccountFingerprint",
        "reservedCredentialConfigurationHash",
        "ownerEpochHash",
        "componentReadersHash",
        "finalMutationBoundaryHandle",
    ):
        _sha(result[key], key)
    _identity(result["ownerEpochId"], "reservation owner epoch")
    exact = {
        "finalMutationBoundaryRequired": True,
        "finalMutationBoundaryHandleSchema": "kis-domestic-functional-final-reservation/v1",
        "productionAvailable": False,
    }
    if any(
        type(result.get(key)) is not type(expected) or result.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "reservation final boundary contract mismatch"
        )
    return result


def _empty_order_key(value: Mapping[str, Any]) -> bool:
    return set(value) == _ORDER_KEY_KEYS and all(value[key] == "" for key in value)


def _empty_position(value: Mapping[str, Any]) -> bool:
    return set(value) == _POSITION_KEYS and all(value[key] == "" for key in value)


def _mutation_request(
    raw: Mapping[str, Any], *, reservation: Mapping[str, Any], command: str
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _REQUEST_KEYS:
        raise KisDomesticFunctionalManagerBlocked("mutation request is not exact")
    value = dict(raw)
    exact = {
        "schemaVersion": "kis-domestic-functional-manager-mutation-request/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "command": command,
        "sessionId": reservation["sessionId"],
        "stateRevision": reservation["revision"],
        "ownerEpochId": reservation["ownerEpochId"],
        "ownerEpochHash": reservation["ownerEpochHash"],
        "accountFingerprint": reservation["reservedAccountFingerprint"],
        "credentialConfigurationHash": reservation[
            "reservedCredentialConfigurationHash"
        ],
        "productionAvailable": False,
    }
    if any(
        type(value.get(key)) is not type(expected) or value.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalManagerBlocked("mutation request binding mismatch")
    operation = value["operation"]
    if operation not in _OPERATIONS:
        raise KisDomesticFunctionalManagerBlocked("mutation operation is invalid")
    if command == "START" and operation != "NATURAL_BUY":
        raise KisDomesticFunctionalManagerBlocked("START mutation is not natural BUY")
    if command in {"STOP", "KILL"} and operation not in {
        "CLEANUP_CANCEL",
        "CLEANUP_SELL",
    }:
        raise KisDomesticFunctionalManagerBlocked(
            "STOP/Kill mutation is not exact-owned cleanup"
        )
    if command == "SETTINGS":
        raise KisDomesticFunctionalManagerBlocked("SETTINGS cannot mutate broker state")
    _identity(value["claimId"], "mutation claim id")
    _sha(value["payloadHash"], "mutation payload hash")
    _sha(value["requestHash"], "mutation request hash")
    if not isinstance(value["payload"], Mapping):
        raise KisDomesticFunctionalManagerBlocked("mutation payload is invalid")
    payload = dict(value["payload"])
    if not hmac.compare_digest(value["payloadHash"], _hash(payload)):
        raise KisDomesticFunctionalManagerBlocked("mutation payload hash mismatch")
    owned = value["ownedOrderKey"]
    position = value["ownedPosition"]
    if not isinstance(owned, Mapping) or not isinstance(position, Mapping):
        raise KisDomesticFunctionalManagerBlocked("owned cleanup identity is invalid")
    owned = dict(owned)
    position = dict(position)
    if operation in {"NATURAL_BUY", "CLEANUP_SELL"}:
        if value["endpoint"] != _ORDER_ENDPOINT or set(payload) != {
            "PDNO",
            "ORD_DVSN",
            "ORD_QTY",
            "ORD_UNPR",
        }:
            raise KisDomesticFunctionalManagerBlocked("order payload shape is invalid")
        if (
            payload["PDNO"] != PDNO
            or payload["ORD_DVSN"] != "00"
            or payload["ORD_QTY"] != "1"
            or type(payload["ORD_UNPR"]) is not str
            or not payload["ORD_UNPR"].isascii()
            or not payload["ORD_UNPR"].isdigit()
            or not 1 <= int(payload["ORD_UNPR"]) <= 100_000
            or not _empty_order_key(owned)
        ):
            raise KisDomesticFunctionalManagerBlocked("order mutation contract changed")
        if operation == "NATURAL_BUY":
            if not _empty_position(position):
                raise KisDomesticFunctionalManagerBlocked(
                    "natural BUY cannot claim existing ownership"
                )
        else:
            if set(position) != _POSITION_KEYS:
                raise KisDomesticFunctionalManagerBlocked(
                    "cleanup SELL position shape is invalid"
                )
            if (
                position["pdno"] != PDNO
                or type(position["baselineAccountQuantity"]) is not str
                or not position["baselineAccountQuantity"].isascii()
                or not position["baselineAccountQuantity"].isdigit()
                or type(position["currentAccountQuantity"]) is not str
                or not position["currentAccountQuantity"].isascii()
                or not position["currentAccountQuantity"].isdigit()
                or position["ownedDeltaQuantity"] != "1"
                or int(position["currentAccountQuantity"])
                != int(position["baselineAccountQuantity"])
                + int(position["ownedDeltaQuantity"])
                or payload["ORD_QTY"] != position["ownedDeltaQuantity"]
            ):
                raise KisDomesticFunctionalManagerBlocked(
                    "cleanup SELL exact-owned position is invalid"
                )
            _identity(position["sourceClaimId"], "cleanup source claim")
            _sha(position["positionProofHash"], "cleanup position proof")
    else:
        if value["endpoint"] != _CANCEL_ENDPOINT or set(payload) != {
            "KRX_FWDG_ORD_ORGNO",
            "ORGN_ODNO",
            "ORD_DVSN",
            "RVSE_CNCL_DVSN_CD",
            "ORD_QTY",
            "ORD_UNPR",
            "QTY_ALL_ORD_YN",
            "EXCG_ID_DVSN_CD",
        }:
            raise KisDomesticFunctionalManagerBlocked("cancel payload shape is invalid")
        if set(owned) != _ORDER_KEY_KEYS or any(
            type(owned[key]) is not str for key in owned
        ) or not _DATE.fullmatch(owned["orderDate"]) or not _OFFICIAL.fullmatch(
            owned["organizationNo"]
        ) or not _OFFICIAL.fullmatch(owned["orderNo"]):
            raise KisDomesticFunctionalManagerBlocked(
                "cleanup cancel exact-owned tuple is invalid"
            )
        if (
            payload["KRX_FWDG_ORD_ORGNO"] != owned["organizationNo"]
            or payload["ORGN_ODNO"] != owned["orderNo"]
            or payload["ORD_DVSN"] != "00"
            or payload["RVSE_CNCL_DVSN_CD"] != "02"
            or payload["ORD_QTY"] != "1"
            or payload["ORD_UNPR"] != "0"
            or payload["QTY_ALL_ORD_YN"] != "Y"
            or payload["EXCG_ID_DVSN_CD"] != "KRX"
            or not _empty_position(position)
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "cleanup cancel tuple/payload mismatch"
            )
    unsigned = dict(value)
    request_hash = unsigned.pop("requestHash")
    if not hmac.compare_digest(request_hash, _hash(unsigned)):
        raise KisDomesticFunctionalManagerBlocked("mutation request hash mismatch")
    return value


def _owned_projection(
    raw: Mapping[str, Any], *, reservation: Mapping[str, Any]
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _OWNED_PROJECTION_KEYS:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned projection is not exact"
        )
    value = dict(raw)
    projection_hash = value.pop("projectionHash")
    exact = {
        "schemaVersion": "kis-domestic-functional-manager-owned-projection/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "sessionId": reservation["sessionId"],
        "stateRevision": reservation["revision"],
        "ownerEpochId": reservation["ownerEpochId"],
        "ownerEpochHash": reservation["ownerEpochHash"],
        "accountFingerprint": reservation["reservedAccountFingerprint"],
        "credentialConfigurationHash": reservation[
            "reservedCredentialConfigurationHash"
        ],
        "observedAt": reservation["reservedAt"],
        "productionAvailable": False,
    }
    if any(
        type(value.get(key)) is not type(expected) or value.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned projection reservation binding mismatch"
        )
    if type(value.get("revision")) is not int or value["revision"] < 1:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned projection revision is invalid"
        )
    _sha(value.get("headHash"), "authoritative owned projection head")
    orders = value.get("ownedWorkingOrders")
    if type(orders) is not list:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned working orders are invalid"
        )
    normalized_orders: list[dict[str, str]] = []
    for raw_order in orders:
        if not isinstance(raw_order, Mapping) or set(raw_order) != _ORDER_KEY_KEYS:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative owned order tuple is invalid"
            )
        order = dict(raw_order)
        if (
            any(type(order[key]) is not str for key in _ORDER_KEY_KEYS)
            or not _DATE.fullmatch(order["orderDate"])
            or not _OFFICIAL.fullmatch(order["organizationNo"])
            or not _OFFICIAL.fullmatch(order["orderNo"])
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative owned order tuple is invalid"
            )
        normalized_orders.append(order)
    order_tokens = [_canonical(item) for item in normalized_orders]
    if order_tokens != sorted(set(order_tokens)):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned order tuples are duplicate or unsorted"
        )
    position_raw = value.get("ownedPosition")
    if not isinstance(position_raw, Mapping) or set(position_raw) != _POSITION_KEYS:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned position is invalid"
        )
    position = dict(position_raw)
    empty_position = _empty_position(position)
    if not empty_position:
        if (
            position["pdno"] != PDNO
            or type(position["baselineAccountQuantity"]) is not str
            or not position["baselineAccountQuantity"].isascii()
            or not position["baselineAccountQuantity"].isdigit()
            or type(position["currentAccountQuantity"]) is not str
            or not position["currentAccountQuantity"].isascii()
            or not position["currentAccountQuantity"].isdigit()
            or position["ownedDeltaQuantity"] not in {"0", "1"}
            or int(position["currentAccountQuantity"])
            != int(position["baselineAccountQuantity"])
            + int(position["ownedDeltaQuantity"])
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative owned position quantity is invalid"
            )
        _identity(position["sourceClaimId"], "authoritative source claim")
        _sha(position["positionProofHash"], "authoritative position proof")
    delta = value.get("ownedPositionDelta")
    if type(delta) is not str or delta not in {"0", "1"}:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned position delta is invalid"
        )
    expected_delta = (
        0 if empty_position else int(position["ownedDeltaQuantity"])
    )
    if expected_delta not in {0, 1} or int(delta) != expected_delta:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned position delta does not match projection"
        )
    if (
        type(value.get("ownedWorkingOrderCount")) is not int
        or value["ownedWorkingOrderCount"] != len(normalized_orders)
        or type(value.get("ownedWorking0")) is not bool
        or value["ownedWorking0"] is not (len(normalized_orders) == 0)
        or type(value.get("ownedDelta0")) is not bool
        or value["ownedDelta0"] is not (expected_delta == 0)
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned-zero proof is inconsistent"
        )
    if type(projection_hash) is not str or not hmac.compare_digest(
        projection_hash, _hash(value)
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative owned projection hash mismatch"
        )
    return {**value, "projectionHash": projection_hash}


def _authority_plan(
    raw: Mapping[str, Any], *, reservation: Mapping[str, Any], command: str
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _AUTHORITY_PLAN_KEYS:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative mutation plan is not exact"
        )
    value = dict(raw)
    signature = value.pop("signature")
    plan_hash = value.pop("planHash")
    exact = {
        "schemaVersion": "kis-domestic-functional-manager-authority-plan/v1",
        "route": ROUTE,
        "pdno": PDNO,
        "reservationId": reservation["reservationId"],
        "reservationKind": command,
        "reservationRevision": reservation["revision"],
        "sessionId": reservation["sessionId"],
        "stateRevision": reservation["revision"],
        "ownerEpochId": reservation["ownerEpochId"],
        "ownerEpochHash": reservation["ownerEpochHash"],
        "accountFingerprint": reservation["reservedAccountFingerprint"],
        "credentialConfigurationHash": reservation[
            "reservedCredentialConfigurationHash"
        ],
        "productionAvailable": False,
    }
    if any(
        type(value.get(key)) is not type(expected) or value.get(key) != expected
        for key, expected in exact.items()
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative mutation plan reservation binding mismatch"
        )
    _identity(value.get("planId"), "authoritative plan id")
    if type(value.get("planRevision")) is not int or value["planRevision"] < 1:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative plan revision is invalid"
        )
    _sha(value.get("keyIdHash"), "authoritative plan key id")
    _sha(signature, "authoritative plan signature")
    projection = _owned_projection(
        value.get("ownedProjection"), reservation=reservation
    )
    if not hmac.compare_digest(
        str(value.get("ownedProjectionHash")), projection["projectionHash"]
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative plan projection hash mismatch"
        )
    raw_requests = value.get("mutationRequests")
    if type(raw_requests) is not list:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative plan mutation requests are invalid"
        )
    requests = [
        _mutation_request(item, reservation=reservation, command=command)
        for item in raw_requests
    ]
    if (
        type(value.get("requestCount")) is not int
        or value["requestCount"] != len(requests)
        or len(requests) > 2
        or len({item["claimId"] for item in requests}) != len(requests)
        or len({item["operation"] for item in requests}) != len(requests)
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative plan operation cardinality is invalid"
        )
    operations = {item["operation"] for item in requests}
    if command == "START":
        if operations != {"NATURAL_BUY"} or len(requests) != 1:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative START plan is not exact natural BUY"
            )
    elif command in {"STOP", "KILL"}:
        if not operations <= {"CLEANUP_CANCEL", "CLEANUP_SELL"}:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative cleanup plan operation is invalid"
            )
    elif requests:
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative SETTINGS plan must POST zero"
        )
    cancel_orders = sorted(
        (
            dict(item["ownedOrderKey"])
            for item in requests
            if item["operation"] == "CLEANUP_CANCEL"
        ),
        key=_canonical,
    )
    projected_orders = [dict(item) for item in projection["ownedWorkingOrders"]]
    sell_requests = [
        item for item in requests if item["operation"] == "CLEANUP_SELL"
    ]
    if command in {"STOP", "KILL"}:
        if projected_orders != cancel_orders:
            raise KisDomesticFunctionalManagerBlocked(
                "cleanup plan does not exactly cover owned working orders"
            )
        if projection["ownedDelta0"]:
            if sell_requests or not _empty_position(projection["ownedPosition"]):
                raise KisDomesticFunctionalManagerBlocked(
                    "cleanup zero-delta proof conflicts with SELL"
                )
        elif (
            len(sell_requests) != 1
            or dict(sell_requests[0]["ownedPosition"])
            != dict(projection["ownedPosition"])
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "cleanup SELL does not exactly cover owned position delta"
            )
        if not requests and not (
            projection["ownedWorking0"] and projection["ownedDelta0"]
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "zero cleanup plan lacks signed owned-zero proof"
            )
    else:
        if not (projection["ownedWorking0"] and projection["ownedDelta0"]):
            raise KisDomesticFunctionalManagerBlocked(
                "entry/settings requires a clean authoritative projection"
            )
    if type(plan_hash) is not str or not hmac.compare_digest(
        plan_hash, _hash(value)
    ):
        raise KisDomesticFunctionalManagerBlocked(
            "authoritative mutation plan hash mismatch"
        )
    return {**value, "planHash": plan_hash, "signature": signature}


class DisabledKisDomesticFunctionalManager:
    def __init__(
        self,
        *,
        adapters: Mapping[str, OfflinePinnedKisManagerAdapter],
        manager_id: str,
        signer_key: bytes,
        signer_key_id: str,
        timeout_seconds: float = 0.25,
        wall_clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
        allow_offline_signer: bool,
    ) -> None:
        _identity(manager_id, "manager id")
        _identity(signer_key_id, "manager signer key id")
        if type(signer_key) is not bytes or len(signer_key) < 32:
            raise KisDomesticFunctionalManagerBlocked("manager signer key is invalid")
        if allow_offline_signer is not True:
            raise KisDomesticFunctionalManagerBlocked(
                "offline manager signer must be explicit"
            )
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(float(timeout_seconds))
            or not 0.01 <= float(timeout_seconds) <= 5.0
        ):
            raise KisDomesticFunctionalManagerBlocked("manager timeout is invalid")
        if not callable(wall_clock) or not callable(monotonic_clock):
            raise KisDomesticFunctionalManagerBlocked("manager clocks are invalid")
        if not isinstance(adapters, Mapping) or set(adapters) != set(_COMPONENTS):
            raise KisDomesticFunctionalManagerBlocked("manager adapter set is not exact")
        values = dict(adapters)
        if type(values["state"]) is not OfflinePinnedKisStateAdapter:
            raise KisDomesticFunctionalManagerBlocked("exact offline state adapter required")
        if type(values["transport"]) is not OfflinePinnedKisTransportAdapter:
            raise KisDomesticFunctionalManagerBlocked(
                "exact offline transport adapter required"
            )
        if type(values["mutation"]) is not OfflinePinnedKisMutationAdapter:
            raise KisDomesticFunctionalManagerBlocked(
                "exact offline authoritative mutation adapter required"
            )
        if any(
            type(values[name]) is not OfflinePinnedKisManagerAdapter
            for name in _COMPONENTS
            if name not in {"state", "mutation", "transport"}
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "exact offline component adapters required"
            )
        if any(values[name].component != name for name in _COMPONENTS):
            raise KisDomesticFunctionalManagerBlocked("manager adapter identity mismatch")
        self.adapters = values
        self.manager_id_hash = hashlib.sha256(manager_id.encode()).hexdigest()
        self.signer_key = signer_key
        self.signer_key_id_hash = hashlib.sha256(signer_key_id.encode()).hexdigest()
        self.timeout_seconds = float(timeout_seconds)
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self.component_bindings = {
            name: values[name].binding() for name in _COMPONENTS
        }
        self.component_bindings_hash = _hash(self.component_bindings)
        self._control_lock = threading.RLock()
        self._running = False
        self._detached_hazard = False
        self._last_receipt_hash = ""

    def _statuses(
        self, reservation: Mapping[str, Any], *, require_ready: bool
    ) -> tuple[dict[str, dict[str, Any]], str]:
        values = {name: self.adapters[name].status() for name in _COMPONENTS}
        expected = (
            reservation["revision"],
            reservation["sessionId"],
            reservation["reservedAccountFingerprint"],
            reservation["reservedCredentialConfigurationHash"],
            reservation["ownerEpochId"],
            reservation["ownerEpochHash"],
        )
        for name, value in values.items():
            actual = (
                value["stateRevision"],
                value["sessionId"],
                value["accountFingerprint"],
                value["credentialConfigurationHash"],
                value["ownerEpochId"],
                value["ownerEpochHash"],
            )
            if actual != expected:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{name} reservation/owner/account join mismatch"
                )
            if value["hazards"]:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{name} has durable hazards"
                )
            if require_ready and value["ready"] is not True:
                raise KisDomesticFunctionalManagerBlocked(
                    f"{name} is not ready for START"
                )
        join_hash = _hash(values)
        if not hmac.compare_digest(
            join_hash, str(reservation["componentReadersHash"])
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "state reservation component-reader hash changed"
            )
        return values, join_hash

    def _bound_authoritative_plan(
        self,
        *,
        reservation: Mapping[str, Any],
        command: str,
        statuses: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        mutation = self.adapters["mutation"]
        if type(mutation) is not OfflinePinnedKisMutationAdapter:
            raise KisDomesticFunctionalManagerBlocked(
                "authoritative mutation adapter disappeared"
            )
        plan = mutation.authoritative_plan(
            reservation=reservation,
            command=command,
        )
        projection = plan["ownedProjection"]
        status = statuses["mutation"]
        expected = {
            "authoritativeMutationPlanHash": plan["planHash"],
            "ownedProjectionHash": plan["ownedProjectionHash"],
            "ownedProjectionHeadHash": projection["headHash"],
            "ownedProjectionRevision": projection["revision"],
            "ownedProjectionObservedAt": projection["observedAt"],
            "mutationPlanKeyIdHash": plan["keyIdHash"],
        }
        if any(
            type(status.get(key)) is not type(value)
            or status.get(key) != value
            for key, value in expected.items()
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "state-bound mutation plan/projection head changed"
            )
        return plan

    @staticmethod
    def _boundary(
        raw: Mapping[str, Any], *, reservation: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(raw, Mapping) or set(raw) != _BOUNDARY_KEYS:
            raise KisDomesticFunctionalManagerBlocked(
                "final mutation boundary lease is not exact"
            )
        value = dict(raw)
        expected = {
            "schemaVersion": "kis-domestic-functional-final-reservation/v1",
            "route": ROUTE,
            "reservationId": reservation["reservationId"],
            "reservationKind": reservation["reservationKind"],
            "reservationRevision": reservation["revision"],
            "sessionId": reservation["sessionId"],
            "accountFingerprint": reservation["reservedAccountFingerprint"],
            "credentialConfigurationHash": reservation[
                "reservedCredentialConfigurationHash"
            ],
            "ownerEpochHash": reservation["ownerEpochHash"],
            "componentReadersHash": reservation["componentReadersHash"],
            "productionAvailable": False,
            "finalMutationBoundaryHandle": reservation[
                "finalMutationBoundaryHandle"
            ],
            "routeLockHeld": True,
        }
        if any(
            type(value.get(key)) is not type(wanted) or value.get(key) != wanted
            for key, wanted in expected.items()
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "final mutation boundary binding mismatch"
            )
        return value

    def _sign(self, body: Mapping[str, Any]) -> dict[str, Any]:
        receipt_hash = _hash(body)
        signature = hmac.new(
            self.signer_key,
            ("KIS_FUNCTIONAL_MANAGER_RECEIPT\n" + receipt_hash).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {**body, "receiptHash": receipt_hash, "signature": signature}

    def _verify_state_envelope(
        self,
        candidate: Mapping[str, Any],
        *,
        require_finish_allowed: bool,
    ) -> bool:
        """Verify the exact receipt consumed by the frozen state owner.

        This is an offline HMAC verifier seam only.  Production authority stays
        false until an external verify-only registry replaces the local signer.
        """

        try:
            if not isinstance(candidate, Mapping) or set(candidate) != _STATE_RECEIPT_KEYS:
                return False
            value = dict(candidate)
            signature = value.pop("signature")
            receipt_hash = value.pop("receiptHash")
            if (
                type(signature) is not str
                or not _SHA.fullmatch(signature)
                or type(receipt_hash) is not str
                or not _SHA.fullmatch(receipt_hash)
                or value.get("schemaVersion")
                != "kis-domestic-functional-manager-receipt/v2"
                or value.get("route") != ROUTE
                or value.get("keyIdHash") != self.signer_key_id_hash
                or value.get("productionAvailable") is not False
                or type(value.get("ok")) is not bool
                or type(value.get("mutationMayHaveOccurred")) is not bool
                or type(value.get("detachedBoundaryHazard")) is not bool
                or type(value.get("pendingReservation")) is not bool
                or type(value.get("reservationFinishAllowed")) is not bool
                or type(value.get("reconciliationRequired")) is not bool
                or value.get("reservationFinishAllowed") is not require_finish_allowed
                or value.get("pendingReservation") is require_finish_allowed
                or value.get("detachedBoundaryHazard") is require_finish_allowed
                or (
                    not require_finish_allowed
                    and value.get("reconciliationRequired") is not True
                )
                or (
                    value.get("ok") is True
                    and value.get("reconciliationRequired") is not False
                )
                or any(
                    type(value.get(key)) is not str
                    or not _SHA.fullmatch(value[key])
                    for key in (
                        "managerReceiptHash",
                        "executionProofHash",
                        "mutationPlanHash",
                        "ownedProjectionHash",
                        "ownedProjectionHeadHash",
                        "boundaryEntryProofHash",
                        "attemptChainHead",
                        "transportReceiptSetHash",
                    )
                )
                or not hmac.compare_digest(receipt_hash, _hash(value))
            ):
                return False
            return hmac.compare_digest(
                signature,
                hmac.new(
                    self.signer_key,
                    ("KIS_MANAGER_RECEIPT\n" + receipt_hash).encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest(),
            )
        except Exception:
            return False

    def verify_state_manager_receipt(self, candidate: Mapping[str, Any]) -> bool:
        """Verify only a finish-eligible state receipt.

        A detached final-boundary worker deliberately never yields such a
        receipt, preventing the caller from synchronously finishing the state
        reservation while the route lease may still be held.
        """

        return self._verify_state_envelope(
            candidate,
            require_finish_allowed=True,
        )

    def verify_pending_reservation_proof(
        self, candidate: Mapping[str, Any]
    ) -> bool:
        return self._verify_state_envelope(
            candidate,
            require_finish_allowed=False,
        )

    def execute(
        self,
        *,
        reservation: Mapping[str, Any],
        command: str,
        mutation_requests: list[Mapping[str, Any]],
    ) -> dict[str, Any]:
        reserved = _reservation(reservation, command=command)
        if type(mutation_requests) is not list:
            raise KisDomesticFunctionalManagerBlocked("mutation request list is invalid")
        requests = [
            _mutation_request(item, reservation=reserved, command=command)
            for item in mutation_requests
        ]
        if (
            len(requests) > 2
            or len({item["claimId"] for item in requests}) != len(requests)
            or len({item["operation"] for item in requests}) != len(requests)
        ):
            raise KisDomesticFunctionalManagerBlocked(
                "mutation request cardinality/claim/operation uniqueness failed"
            )
        if command == "START" and len(requests) != 1:
            raise KisDomesticFunctionalManagerBlocked("START requires exact natural BUY")
        if command == "SETTINGS" and requests:
            raise KisDomesticFunctionalManagerBlocked("SETTINGS must POST zero")
        with self._control_lock:
            if self._running:
                raise KisDomesticFunctionalManagerBlocked("manager operation already running")
            if self._detached_hazard:
                raise KisDomesticFunctionalManagerBlocked(
                    "detached mutation hazard requires reconciliation"
                )
            self._running = True
        started_at = _now_text(self.wall_clock)
        if datetime.fromisoformat(started_at).timestamp() < datetime.fromisoformat(
            reserved["reservedAt"]
        ).timestamp():
            with self._control_lock:
                self._running = False
            raise KisDomesticFunctionalManagerBlocked(
                "manager wall clock precedes the state reservation"
            )
        before_mono = self.monotonic_clock()
        if type(before_mono) not in {int, float} or not math.isfinite(float(before_mono)):
            with self._control_lock:
                self._running = False
            raise KisDomesticFunctionalManagerBlocked("manager monotonic clock is invalid")
        shared: dict[str, Any] = {
            "boundary": {},
            "attempts": [],
            "receipts": [],
            "joinHash": "",
            "plan": {},
            "projection": {},
            "cleanupExactOwned": False,
            "error": None,
            "attemptStarted": False,
            "boundaryExited": False,
        }

        def worker() -> None:
            try:
                statuses_before, first_join = self._statuses(
                    reserved, require_ready=command == "START"
                )
                first_plan = self._bound_authoritative_plan(
                    reservation=reserved,
                    command=command,
                    statuses=statuses_before,
                )
                if first_plan["mutationRequests"] != requests:
                    raise KisDomesticFunctionalManagerBlocked(
                        "caller mutation requests differ from authoritative plan"
                    )
                shared["plan"] = first_plan
                shared["projection"] = first_plan["ownedProjection"]
                shared["cleanupExactOwned"] = bool(
                    command in {"STOP", "KILL"}
                    and all(
                        item["operation"] in {"CLEANUP_CANCEL", "CLEANUP_SELL"}
                        for item in requests
                    )
                    and (
                        bool(requests)
                        or (
                            first_plan["ownedProjection"]["ownedWorking0"]
                            and first_plan["ownedProjection"]["ownedDelta0"]
                        )
                    )
                )
                state = self.adapters["state"]
                assert type(state) is OfflinePinnedKisStateAdapter
                with state.final_mutation_boundary(reserved) as raw_boundary:
                    boundary = self._boundary(raw_boundary, reservation=reserved)
                    statuses_final, final_join = self._statuses(
                        reserved, require_ready=command == "START"
                    )
                    if not hmac.compare_digest(first_join, final_join):
                        raise KisDomesticFunctionalManagerBlocked(
                            "component status changed before final edge"
                        )
                    final_plan = self._bound_authoritative_plan(
                        reservation=reserved,
                        command=command,
                        statuses=statuses_final,
                    )
                    if final_plan != first_plan:
                        raise KisDomesticFunctionalManagerBlocked(
                            "authoritative plan changed before final edge"
                        )
                    boundary_body = {
                        "schemaVersion": "kis-domestic-functional-manager-boundary-entry/v1",
                        "route": ROUTE,
                        "pdno": PDNO,
                        "command": command,
                        "reservationId": reserved["reservationId"],
                        "reservationRevision": reserved["revision"],
                        "sessionId": reserved["sessionId"],
                        "ownerEpochId": reserved["ownerEpochId"],
                        "ownerEpochHash": reserved["ownerEpochHash"],
                        "componentBindingsHash": self.component_bindings_hash,
                        "componentJoinHash": final_join,
                        "mutationPlanHash": final_plan["planHash"],
                        "ownedProjectionHash": final_plan["ownedProjectionHash"],
                        "ownedProjectionHeadHash": final_plan["ownedProjection"][
                            "headHash"
                        ],
                        "finalMutationBoundaryHandle": boundary[
                            "finalMutationBoundaryHandle"
                        ],
                        "routeLockHeld": True,
                        "productionAvailable": False,
                    }
                    boundary_proof = {
                        **boundary_body,
                        "boundaryEntryProofHash": _hash(boundary_body),
                    }
                    shared["boundary"] = boundary_proof
                    shared["joinHash"] = final_join
                    previous = "0" * 64
                    transport = self.adapters["transport"]
                    assert type(transport) is OfflinePinnedKisTransportAdapter
                    for index, request in enumerate(requests, 1):
                        attempt_body = {
                            "schemaVersion": "kis-domestic-functional-manager-attempt/v1",
                            "route": ROUTE,
                            "pdno": PDNO,
                            "reservationId": reserved["reservationId"],
                            "reservationRevision": reserved["revision"],
                            "sessionId": reserved["sessionId"],
                            "attemptIndex": index,
                            "operation": request["operation"],
                            "claimId": request["claimId"],
                            "requestHash": request["requestHash"],
                            "boundaryEntryProofHash": boundary_proof[
                                "boundaryEntryProofHash"
                            ],
                            "previousAttemptProofHash": previous,
                            "productionAvailable": False,
                        }
                        attempt = {
                            **attempt_body,
                            "attemptProofHash": _hash(attempt_body),
                        }
                        shared["attemptStarted"] = True
                        shared["attempts"].append(attempt)
                        receipt = transport.mutate(request, attempt)
                        shared["receipts"].append(receipt)
                        previous = attempt["attemptProofHash"]
                shared["boundaryExited"] = True
            except BaseException as exc:  # captured and classified by owner thread
                shared["error"] = exc

        thread = threading.Thread(
            target=worker,
            name=f"kis-offline-manager-{command.lower()}",
            daemon=True,
        )
        thread.start()
        thread.join(self.timeout_seconds)
        detached_boundary_hazard = thread.is_alive()
        timed_out = detached_boundary_hazard
        clock_discontinuity_hazard = False
        if detached_boundary_hazard:
            with self._control_lock:
                self._detached_hazard = True
        else:
            with self._control_lock:
                self._running = False
        after_mono = self.monotonic_clock()
        after_mono_valid = bool(
            type(after_mono) in {int, float}
            and math.isfinite(float(after_mono))
            and float(after_mono) >= float(before_mono)
        )
        if not after_mono_valid:
            timed_out = True
            clock_discontinuity_hazard = True
            with self._control_lock:
                self._detached_hazard = True
        try:
            finished_at = _now_text(self.wall_clock)
            if datetime.fromisoformat(finished_at).timestamp() < datetime.fromisoformat(
                started_at
            ).timestamp():
                raise KisDomesticFunctionalManagerBlocked(
                    "manager wall clock rolled back"
                )
        except Exception:
            finished_at = started_at
            timed_out = True
            clock_discontinuity_hazard = True
            with self._control_lock:
                self._detached_hazard = True
        receipts = [dict(item) for item in shared["receipts"]]
        error = shared["error"]
        if error is None:
            reserved_epoch = datetime.fromisoformat(
                reserved["reservedAt"]
            ).timestamp()
            finished_epoch = datetime.fromisoformat(finished_at).timestamp()
            for item in receipts:
                occurred_epoch = datetime.fromisoformat(
                    _utc(item["occurredAt"], "transport receipt time")
                ).timestamp()
                if not reserved_epoch <= occurred_epoch <= finished_epoch:
                    error = KisDomesticFunctionalManagerBlocked(
                        "transport receipt time is outside the reserved boundary"
                    )
                    break
        mutation_may_have_occurred = bool(
            timed_out
            or shared["attemptStarted"]
            and (
                error is not None
                or any(item["mutationMayHaveOccurred"] for item in receipts)
            )
        )
        acknowledged = bool(
            len(receipts) == len(requests)
            and all(item["status"] == "ACKNOWLEDGED" for item in receipts)
        )
        boundary_release_observed = bool(
            not detached_boundary_hazard and shared["boundaryExited"]
        )
        ok = bool(
            not timed_out
            and error is None
            and acknowledged
            and boundary_release_observed
            and shared["plan"]
        )
        if (
            not requests
            and not timed_out
            and error is None
            and boundary_release_observed
            and shared["plan"]
        ):
            ok = True
        reconciliation_required = not ok
        failure_code = (
            "TIMEOUT_DETACHED_MUTATION_HAZARD"
            if timed_out
            else (
                "BOUNDARY_OR_COMPONENT_FAILURE"
                if error is not None
                else "TRANSPORT_NOT_ACKNOWLEDGED"
                if not ok
                else ""
            )
        )
        elapsed_monotonic = (
            float(after_mono) - float(before_mono)
            if after_mono_valid
            else 0.0
        )
        plan = dict(shared["plan"])
        projection = dict(shared["projection"])
        plan_hash = str(plan.get("planHash") or "0" * 64)
        projection_hash = str(plan.get("ownedProjectionHash") or "0" * 64)
        projection_head = str(projection.get("headHash") or "0" * 64)
        boundary_hash = str(
            shared["boundary"].get("boundaryEntryProofHash") or "0" * 64
        )
        attempt_chain_head = (
            shared["attempts"][-1]["attemptProofHash"]
            if shared["attempts"]
            else "0" * 64
        )
        transport_receipt_hashes = [item["receiptHash"] for item in receipts]
        transport_receipt_set_hash = _hash(transport_receipt_hashes)
        execution_proof_body = {
            "schemaVersion": "kis-domestic-functional-manager-execution-proof/v1",
            "route": ROUTE,
            "reservationId": reserved["reservationId"],
            "reservationRevision": reserved["revision"],
            "mutationPlanHash": plan_hash,
            "ownedProjectionHash": projection_hash,
            "ownedProjectionHeadHash": projection_head,
            "boundaryEntryProofHash": boundary_hash,
            "attemptCount": len(shared["attempts"]),
            "attemptChainHead": attempt_chain_head,
            "transportReceiptSetHash": transport_receipt_set_hash,
            "detachedBoundaryHazard": detached_boundary_hazard,
            "boundaryReleaseObserved": boundary_release_observed,
            "productionAvailable": False,
        }
        execution_proof_hash = _hash(execution_proof_body)
        pending_reservation = detached_boundary_hazard
        reservation_finish_allowed = not detached_boundary_hazard
        body = {
            "schemaVersion": "kis-domestic-functional-manager-receipt/v2",
            "route": ROUTE,
            "pdno": PDNO,
            "managerIdHash": self.manager_id_hash,
            "command": command,
            "reservationId": reserved["reservationId"],
            "reservationRevision": reserved["revision"],
            "sessionId": reserved["sessionId"],
            "ownerEpochId": reserved["ownerEpochId"],
            "ownerEpochHash": reserved["ownerEpochHash"],
            "accountFingerprint": reserved["reservedAccountFingerprint"],
            "credentialConfigurationHash": reserved[
                "reservedCredentialConfigurationHash"
            ],
            "componentBindingsHash": self.component_bindings_hash,
            "componentJoinHash": str(shared["joinHash"]),
            "mutationPlanHash": plan_hash,
            "ownedProjectionHash": projection_hash,
            "ownedProjectionHeadHash": projection_head,
            "authoritativePlanVerified": bool(plan),
            "boundaryEntryProofHash": boundary_hash,
            "attemptCount": len(shared["attempts"]),
            "attemptChainHead": attempt_chain_head,
            "transportReceiptHashes": transport_receipt_hashes,
            "transportReceiptSetHash": transport_receipt_set_hash,
            "executionProofHash": execution_proof_hash,
            "cleanupExactOwned": bool(shared["cleanupExactOwned"]),
            "ok": ok,
            "mutationMayHaveOccurred": mutation_may_have_occurred,
            "reconciliationRequired": reconciliation_required,
            "detachedMutationHazard": timed_out,
            "detachedBoundaryHazard": detached_boundary_hazard,
            "clockDiscontinuityHazard": clock_discontinuity_hazard,
            "boundaryReleaseObserved": boundary_release_observed,
            "operationDeadlineComplete": not detached_boundary_hazard,
            "pendingReservation": pending_reservation,
            "reservationFinishAllowed": reservation_finish_allowed,
            "durablePendingReservationRequired": pending_reservation,
            "failureCode": failure_code,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "elapsedMonotonicSeconds": elapsed_monotonic,
            "signerKeyIdHash": self.signer_key_id_hash,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseEvidenceAvailable": False,
        }
        receipt = self._sign(body)
        state_receipt_body = {
            "schemaVersion": "kis-domestic-functional-manager-receipt/v2",
            "route": ROUTE,
            "reservationId": reserved["reservationId"],
            "reservationKind": reserved["reservationKind"],
            "reservationRevision": reserved["revision"],
            "sessionId": reserved["sessionId"],
            "accountFingerprint": reserved["reservedAccountFingerprint"],
            "credentialConfigurationHash": reserved[
                "reservedCredentialConfigurationHash"
            ],
            "ownerEpochHash": reserved["ownerEpochHash"],
            "componentReadersHash": reserved["componentReadersHash"],
            "managerReceiptHash": receipt["receiptHash"],
            "executionProofHash": execution_proof_hash,
            "mutationPlanHash": plan_hash,
            "ownedProjectionHash": projection_hash,
            "ownedProjectionHeadHash": projection_head,
            "boundaryEntryProofHash": boundary_hash,
            "attemptChainHead": attempt_chain_head,
            "transportReceiptSetHash": transport_receipt_set_hash,
            "detachedBoundaryHazard": detached_boundary_hazard,
            "pendingReservation": pending_reservation,
            "reservationFinishAllowed": reservation_finish_allowed,
            "reconciliationRequired": reconciliation_required,
            "ok": ok,
            "mutationMayHaveOccurred": mutation_may_have_occurred,
            "occurredAt": finished_at,
            "keyIdHash": self.signer_key_id_hash,
            "productionAvailable": False,
        }
        state_receipt_hash = _hash(state_receipt_body)
        state_receipt = {
            **state_receipt_body,
            "receiptHash": state_receipt_hash,
            "signature": hmac.new(
                self.signer_key,
                ("KIS_MANAGER_RECEIPT\n" + state_receipt_hash).encode("utf-8"),
                hashlib.sha256,
            ).hexdigest(),
        }
        with self._control_lock:
            self._last_receipt_hash = receipt["receiptHash"]
        return {
            "receipt": MappingProxyType(receipt),
            "stateManagerReceipt": (
                MappingProxyType(state_receipt)
                if reservation_finish_allowed
                else None
            ),
            "pendingReservationProof": (
                MappingProxyType(state_receipt)
                if not reservation_finish_allowed
                else None
            ),
            "boundaryEntryProof": MappingProxyType(dict(shared["boundary"])),
            "attemptProofs": tuple(
                MappingProxyType(dict(item)) for item in shared["attempts"]
            ),
            "transportReceipts": tuple(
                MappingProxyType(dict(item)) for item in receipts
            ),
        }

    def status(self) -> dict[str, Any]:
        with self._control_lock:
            running = self._running
            detached = self._detached_hazard
            last = self._last_receipt_hash
        return {
            "schemaVersion": "kis-domestic-functional-manager-status/v1",
            "route": ROUTE,
            "pdno": PDNO,
            "managerIdHash": self.manager_id_hash,
            "componentBindingsHash": self.component_bindings_hash,
            "componentCount": len(_COMPONENTS),
            "running": running,
            "detachedMutationHazard": detached,
            "hazardousAuthorityOpen": bool(running or detached),
            "lastReceiptHash": last,
            "stateFinalMutationBoundaryRequired": True,
            "authoritativeMutationPlanRequired": True,
            "ownedProjectionHeadRequired": True,
            "zeroCleanupRequiresSignedOwnedZero": True,
            "duplicateCleanupOperationAllowed": False,
            "detachedBoundaryCanFinishReservation": False,
            "stateReceiptV2IntegrationWired": False,
            "exactOwnedCleanupRequired": True,
            "managerControllerLockHeldAcrossRouteBoundary": False,
            "productionAvailable": False,
            "networkAvailable": False,
            "releaseEvidenceAvailable": False,
            "sharedStateWired": False,
            "ordinaryRouteFenceWired": False,
        }


def production_entrypoint_status() -> dict[str, Any]:
    return {
        "available": False,
        "networkAvailable": False,
        "productionManagerAvailable": False,
        "releaseEvidenceAvailable": False,
        "sharedStateWired": False,
        "ordinaryRouteFenceWired": False,
        "stateReceiptV2IntegrationWired": False,
        "route": ROUTE,
        "pdno": PDNO,
        "reason": "ISOLATED_OFFLINE_MANAGER_MOCK_ADAPTERS_ONLY",
    }


__all__ = [
    "DisabledKisDomesticFunctionalManager",
    "KIS_DOMESTIC_FUNCTIONAL_MANAGER_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MANAGER_NETWORK_AVAILABLE",
    "KIS_DOMESTIC_FUNCTIONAL_MANAGER_RELEASE_AVAILABLE",
    "KisDomesticFunctionalManagerBlocked",
    "OfflinePinnedKisManagerAdapter",
    "OfflinePinnedKisMutationAdapter",
    "OfflinePinnedKisStateAdapter",
    "OfflinePinnedKisTransportAdapter",
    "production_entrypoint_status",
]
