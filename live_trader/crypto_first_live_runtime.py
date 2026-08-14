from __future__ import annotations

"""Server composition for the shared crypto first-live authority.

This module owns no broker transport.  It keeps the coordinator bearer token
only in process memory, projects exact broker-specific snapshots while the
broker route lock is retained, and makes STOP/Kill a durable entry revocation
before broker cleanup can begin.

The local coordinator plus its second SQLite high-water database detect a
one-sided rollback.  They do not prove that both files were not restored to a
matching prefix, so the root production release latch deliberately remains
closed until an independently administered monotonic/WORM authority exists.
"""

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
import hashlib
import json
import re
import secrets
import threading
from typing import Any, Callable, Iterator, Mapping

from .crypto_first_live_coordinator import (
    CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED,
    CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED,
    CryptoFirstLiveCoordinatorError,
    DurableCryptoFirstLiveCoordinator,
)


CRYPTO_FIRST_LIVE_ROOT_COMPOSITION_RELEASED = False
CRYPTO_FIRST_LIVE_EXTERNAL_WORM_AUTHORITY_RELEASED = False

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_UPBIT_REQUEST_FIELDS = {
    "schemaVersion",
    "scope",
    "lane",
    "action",
    "cleanup",
    "runId",
    "sessionId",
    "permitId",
    "permitHash",
    "accountFingerprint",
    "routeScopeHash",
    "ownerIdentityHash",
    "claimId",
    "requestHash",
}
_BINANCE_REQUEST_FIELDS = {
    "purpose",
    "session_id",
    "permit_id",
    "permit_hash",
    "account_fingerprint",
    "cleanup_only",
}


class CryptoFirstLiveRuntimeError(RuntimeError):
    """A shared first-live boundary failed before broker transport."""


def _text(value: object) -> str:
    return str(value or "").strip()


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _without_token(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result.pop("ownerToken", None)
    result["ownerTokenPersisted"] = False
    result["ownerTokenReturned"] = False
    return result


@dataclass(frozen=True, slots=True)
class _MemoryOwner:
    lane: str
    run_id: str
    session_id: str
    permit_id: str
    permit_hash: str
    account_fingerprint: str
    baseline_hash: str
    code_hash: str
    owner_token: str = field(repr=False)
    owner_epoch: int
    route_scope_hash: str
    broker_owner_identity_hash: str


class InProcessRouteLockAuthority:
    """Unforgeable same-thread evidence for the coordinator route callback."""

    def __init__(self) -> None:
        self._local = threading.local()

    @contextmanager
    def held(
        self,
        lane: str,
        boundary: Callable[[], Any] | None,
    ) -> Iterator[Mapping[str, Any]]:
        normalized_lane = _text(lane).upper()
        manager = boundary() if boundary is not None else nullcontext()
        with manager:
            previous = getattr(self._local, "evidence", None)
            evidence = {
                "schemaVersion": "crypto-first-live-in-process-route-lock/v1",
                "lane": normalized_lane,
                "ownerThreadId": int(threading.get_ident()),
                "nonce": secrets.token_hex(24),
                "held": True,
            }
            self._local.evidence = evidence
            try:
                yield dict(evidence)
            finally:
                if previous is None:
                    try:
                        del self._local.evidence
                    except AttributeError:
                        pass
                else:
                    self._local.evidence = previous

    def verify(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        value = dict(request)
        presented = value.get("presented")
        current = getattr(self._local, "evidence", None)
        if (
            value.get("schemaVersion")
            != "crypto-first-live-route-lock/v1"
            or not isinstance(presented, Mapping)
            or not isinstance(current, Mapping)
            or dict(presented) != dict(current)
            or value.get("lane") != current.get("lane")
            or current.get("held") is not True
            or int(current.get("ownerThreadId", 0))
            != int(threading.get_ident())
        ):
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-route-lock-not-retained"
            )
        bound = {
            key: item
            for key, item in value.items()
            if key not in {"schemaVersion", "presented"}
        }
        return {
            "schemaVersion": "crypto-first-live-route-lock-proof/v1",
            **bound,
            "proofId": "route-lock-proof-" + secrets.token_hex(16),
            "held": True,
            "exclusive": True,
        }


class CryptoFirstLiveRuntime:
    """Process-local owner of one durable Upbit/Binance first-live run."""

    def __init__(
        self,
        *,
        coordinator: DurableCryptoFirstLiveCoordinator,
        route_lock_authority: InProcessRouteLockAuthority,
        account_lease_holder: Callable[[str, str], Mapping[str, Any]],
        owner_identity_reader: Callable[[str, str], Mapping[str, Any]],
        upbit_route_boundary: Callable[[], Any] | None = None,
        binance_route_boundary: Callable[[], Any] | None = None,
        kill_switch_reader: Callable[[], bool] = lambda: False,
        stop_requested_reader: Callable[[], bool] = lambda: False,
    ) -> None:
        self.coordinator = coordinator
        self.route_lock_authority = route_lock_authority
        self.account_lease_holder = account_lease_holder
        self.owner_identity_reader = owner_identity_reader
        self.upbit_route_boundary = upbit_route_boundary
        self.binance_route_boundary = binance_route_boundary
        self.kill_switch_reader = kill_switch_reader
        self.stop_requested_reader = stop_requested_reader
        self._lock = threading.RLock()
        self._owner: _MemoryOwner | None = None
        self._prepared = False
        self._prepare_status: dict[str, Any] = {
            "prepared": False,
            "available": False,
            "networkOrderPostAllowed": False,
            "reason": "crypto-first-live-runtime-not-prepared",
        }

    @staticmethod
    def release_status() -> dict[str, bool]:
        return {
            "rootCompositionReleased": (
                CRYPTO_FIRST_LIVE_ROOT_COMPOSITION_RELEASED
            ),
            "coordinatorActivationReleased": (
                CRYPTO_FIRST_LIVE_ACTIVATION_RELEASED
            ),
            "coordinatorRollbackProtectionReleased": (
                CRYPTO_FIRST_LIVE_WORM_ROLLBACK_RELEASED
            ),
            "externalWormAuthorityReleased": (
                CRYPTO_FIRST_LIVE_EXTERNAL_WORM_AUTHORITY_RELEASED
            ),
        }

    @classmethod
    def production_entry_released(cls) -> bool:
        return all(cls.release_status().values())

    def prepare(self) -> dict[str, Any]:
        """Repair a single pending publication, then audit startup ownership."""

        with self._lock:
            # A newly constructed runtime naturally starts without a token.
            # A repeated prepare on the same official process must not erase
            # a live bearer and strand cleanup; keep it only if the audited
            # durable row still matches below.
            prior_owner = self._owner
        try:
            repaired = self.coordinator.repair_pending_publication()
            audited = self.coordinator.audit_startup()
            if (
                repaired.get("runId")
                and audited.get("runId")
                and repaired.get("runId") != audited.get("runId")
            ):
                raise CryptoFirstLiveRuntimeError(
                    "crypto-first-live-startup-run-changed"
                )
            if prior_owner is not None and not (
                audited.get("runId") == prior_owner.run_id
                and audited.get("lane") == prior_owner.lane
                and audited.get("sessionId") == prior_owner.session_id
                and audited.get("ownerLeaseActive") is True
                and audited.get("phase")
                in {
                    "PREPARING",
                    "APPROVED_INERT",
                    "ACTIVATION_PREPARING",
                    "ACTIVE",
                    "CLEANUP_ONLY",
                }
            ):
                with self._lock:
                    self._owner = None
            status = {
                "ok": True,
                "prepared": True,
                "available": False,
                "safeHold": True,
                "networkOrderPostAllowed": False,
                "ownerTokenPersisted": False,
                "startupAuditBeforeBrokerPrepare": True,
                "phase": audited.get("phase", "UNREADABLE"),
                "coordinator": dict(audited),
                "release": self.release_status(),
                "reason": (
                    "crypto-first-live-production-release-held"
                    if not self.production_entry_released()
                    else "crypto-first-live-external-evidence-not-bound"
                ),
            }
            with self._lock:
                self._prepared = True
                self._prepare_status = status
            return dict(status)
        except Exception as exc:
            status = {
                "ok": False,
                "prepared": False,
                "available": False,
                "safeHold": True,
                "networkOrderPostAllowed": False,
                "ownerTokenPersisted": False,
                "startupAuditBeforeBrokerPrepare": False,
                "release": self.release_status(),
                "reason": (
                    "crypto-first-live-startup-audit-failed:"
                    + type(exc).__name__
                ),
            }
            with self._lock:
                self._prepared = False
                self._prepare_status = status
            return dict(status)

    def status(self) -> dict[str, Any]:
        with self._lock:
            prepared = dict(self._prepare_status)
            memory_owner = self._owner
        try:
            durable = dict(self.coordinator.status())
        except Exception as exc:
            return {
                **prepared,
                "ok": False,
                "available": False,
                "networkOrderPostAllowed": False,
                "processMemoryOwnerPresent": memory_owner is not None,
                "reason": (
                    "crypto-first-live-status-unreadable:"
                    + type(exc).__name__
                ),
            }
        return {
            **prepared,
            "coordinator": durable,
            "phase": durable.get("phase", "UNREADABLE"),
            "processMemoryOwnerPresent": memory_owner is not None,
            "processMemoryOwnerMatches": bool(
                memory_owner is not None
                and durable.get("runId") == memory_owner.run_id
                and durable.get("lane") == memory_owner.lane
            ),
            "networkOrderPostAllowed": False,
        }

    def reserve_inert(
        self,
        *,
        lane: str,
        session_id: str,
        permit_id: str,
        account_fingerprint: str,
        baseline_hash: str,
        code_hash: str,
        approval_id: str,
        permit_hash: str,
        reservation_evidence: Mapping[str, Any],
        route_scope_hash: str = "",
        broker_owner_identity_hash: str = "",
        lease_seconds: float = 60.0,
    ) -> dict[str, Any]:
        """Claim and seal one inert owner; no activation or transport occurs."""

        normalized_lane = _text(lane).upper()
        if not self._prepared:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-runtime-not-prepared"
            )
        if self.coordinator.reservation_evidence_verifier is None:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-reservation-proof-authority-missing"
            )
        lease = dict(
            self.account_lease_holder(normalized_lane, account_fingerprint)
        )
        if lease.get("acquired") is not True:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-account-os-lease-unavailable"
            )
        identity = dict(
            self.owner_identity_reader(normalized_lane, account_fingerprint)
        )
        claim = self.coordinator.begin_reservation(
            lane=normalized_lane,
            session_id=session_id,
            permit_id=permit_id,
            account_fingerprint=account_fingerprint,
            baseline_hash=baseline_hash,
            code_hash=code_hash,
            approval_id=approval_id,
            permit_hash=permit_hash,
            owner_identity=identity,
            lease_seconds=lease_seconds,
        )
        owner = _MemoryOwner(
            lane=normalized_lane,
            run_id=_text(claim["runId"]),
            session_id=_text(claim["sessionId"]),
            permit_id=_text(claim["permitId"]),
            permit_hash=_text(claim["permitHash"]),
            account_fingerprint=_text(claim["accountFingerprint"]),
            baseline_hash=_text(claim["baselineHash"]),
            code_hash=_text(claim["codeHash"]),
            owner_token=_text(claim["ownerToken"]),
            owner_epoch=int(claim["ownerEpoch"]),
            route_scope_hash=_text(route_scope_hash).lower(),
            broker_owner_identity_hash=(
                _text(broker_owner_identity_hash).lower()
            ),
        )
        with self._lock:
            self._owner = owner
        sealed = self.coordinator.seal_reservation(
            run_id=owner.run_id,
            owner_token=owner.owner_token,
            owner_epoch=owner.owner_epoch,
            reservation_evidence=dict(reservation_evidence),
        )
        return _without_token(sealed)

    def activate(self, final_approval: Mapping[str, Any]) -> dict[str, Any]:
        """Attempt activation; all code-owned release latches are enforced."""

        owner = self._memory_owner()
        value = self.coordinator.activate(
            run_id=owner.run_id,
            owner_token=owner.owner_token,
            owner_epoch=owner.owner_epoch,
            final_approval=dict(final_approval),
        )
        return _without_token(value)

    def heartbeat(self) -> dict[str, Any]:
        owner = self._memory_owner()
        boundary = self._boundary(owner.lane)
        with self.route_lock_authority.held(owner.lane, boundary):
            value = self.coordinator.heartbeat(
                run_id=owner.run_id,
                owner_token=owner.owner_token,
                owner_epoch=owner.owner_epoch,
            )
        return _without_token(value)

    def revoke_entry_before_cleanup(self, reason: str) -> dict[str, Any]:
        """Durably close entry under the lane route lock before STOP/Kill."""

        try:
            durable = dict(self.coordinator.status())
        except Exception as exc:
            return {
                "ok": False,
                "entryAuthorityRevoked": False,
                "state": "RECONCILIATION_REQUIRED",
                "reason": "crypto-first-live-revoke-status-unreadable:"
                + type(exc).__name__,
            }
        phase = _text(durable.get("phase")).upper()
        if phase == "RECONCILIATION_REQUIRED":
            return {
                "ok": False,
                "entryAuthorityRevoked": False,
                "state": phase,
                "reason": "crypto-first-live-entry-revocation-unverifiable:"
                "durable-reconciliation-required",
            }
        if phase in {"IDLE", "FINALIZED"}:
            return {
                "ok": True,
                "entryAuthorityRevoked": True,
                "state": phase,
            }
        lane = _text(durable.get("lane")).upper()
        boundary = self._boundary(lane)
        with self.route_lock_authority.held(lane, boundary):
            for attempt in range(2):
                current = dict(self.coordinator.status())
                try:
                    revoked = self.coordinator.revoke_entry(
                        run_id=_text(current.get("runId")),
                        expected_revision=int(current.get("revision", 0)),
                        reason=_text(reason) or "entry-revoked-before-cleanup",
                    )
                    return {
                        "ok": True,
                        "entryAuthorityRevoked": True,
                        "state": revoked.get("phase"),
                        "coordinator": dict(revoked),
                    }
                except CryptoFirstLiveCoordinatorError as exc:
                    if attempt == 0 and "cas-changed" in str(exc):
                        continue
                    raise
        raise CryptoFirstLiveRuntimeError(
            "crypto-first-live-entry-revocation-incomplete"
        )

    def upbit_authority(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        value = dict(request)
        if set(value) != _UPBIT_REQUEST_FIELDS:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-upbit-request-fields-not-exact"
            )
        owner = self._memory_owner()
        cleanup = value.get("cleanup") is True
        exact = {
            "schemaVersion": (
                "upbit-global-first-live-dispatch-authority-request/v1"
            ),
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "UPBIT",
            "sessionId": owner.session_id,
            "permitId": owner.permit_id,
            "permitHash": owner.permit_hash,
            "accountFingerprint": owner.account_fingerprint,
            "routeScopeHash": owner.route_scope_hash,
            "ownerIdentityHash": owner.broker_owner_identity_hash,
        }
        if (
            owner.lane != "UPBIT"
            or any(value.get(key) != expected for key, expected in exact.items())
            or type(value.get("cleanup")) is not bool
        ):
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-upbit-request-binding-changed"
            )
        if (self.kill_switch_reader() or self.stop_requested_reader()) and not cleanup:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-upbit-entry-stop-active"
            )
        snapshot = self._assert_under_route(owner, cleanup=cleanup)
        durable = snapshot["durable"]
        dispatch = snapshot["dispatch"]
        body = {
            "schemaVersion": "upbit-global-first-live-dispatch-authority/v1",
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "UPBIT",
            "phase": dispatch["phase"],
            "runId": dispatch["runId"],
            "sessionId": dispatch["sessionId"],
            "permitId": dispatch["permitId"],
            "permitHash": dispatch["permitHash"],
            "accountFingerprint": dispatch["accountFingerprint"],
            "routeScopeHash": owner.route_scope_hash,
            "ownerIdentityHash": owner.broker_owner_identity_hash,
            "ownerLeaseActive": dispatch["ownerLeaseActive"],
            "entryAuthorityOpen": bool(
                dispatch["entryAuthorityOpen"] and not cleanup
            ),
            "cleanupAuthorityOpen": cleanup,
            "hardStopEpoch": dispatch["hardStopEpoch"],
            "ownerLeaseExpiresEpoch": durable["ownerLeaseExpiresEpoch"],
            "revision": dispatch["revision"],
            "observedEpoch": dispatch["observedEpoch"],
            "killSwitch": bool(self.kill_switch_reader()),
            "stopRequested": bool(self.stop_requested_reader()),
        }
        return {**body, "authorityHash": _canonical_hash(body)}

    def binance_authority(self, **request: Any) -> Mapping[str, Any]:
        if set(request) != _BINANCE_REQUEST_FIELDS:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-binance-request-fields-not-exact"
            )
        owner = self._memory_owner()
        cleanup = request.get("cleanup_only") is True
        exact = {
            "session_id": owner.session_id,
            "permit_id": owner.permit_id,
            "permit_hash": owner.permit_hash,
            "account_fingerprint": owner.account_fingerprint,
        }
        if (
            owner.lane != "BINANCE_SPOT"
            or any(_text(request.get(key)).lower() != expected.lower()
                   for key, expected in exact.items())
            or type(request.get("cleanup_only")) is not bool
        ):
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-binance-request-binding-changed"
            )
        if (self.kill_switch_reader() or self.stop_requested_reader()) and not cleanup:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-binance-entry-stop-active"
            )
        snapshot = self._assert_under_route(owner, cleanup=cleanup)
        dispatch = snapshot["dispatch"]
        body = {
            "schemaVersion": (
                "crypto-first-live-binance-authority-snapshot/v1"
            ),
            "scope": "CRYPTO_FIRST_LIVE_GLOBAL",
            "lane": "BINANCE_SPOT",
            "phase": dispatch["phase"],
            "runId": dispatch["runId"],
            "sessionId": dispatch["sessionId"],
            "permitId": dispatch["permitId"],
            "permitHash": dispatch["permitHash"],
            "accountFingerprint": dispatch["accountFingerprint"],
            "ownerLeaseActive": dispatch["ownerLeaseActive"],
            "entryAuthorityOpen": dispatch["entryAuthorityOpen"],
            "hardStopEpoch": dispatch["hardStopEpoch"],
            "revision": dispatch["revision"],
            "observedEpoch": dispatch["observedEpoch"],
        }
        return {**body, "authorityHash": _canonical_hash(body)}

    def _assert_under_route(
        self, owner: _MemoryOwner, *, cleanup: bool
    ) -> dict[str, Mapping[str, Any]]:
        boundary = self._boundary(owner.lane)
        with self.route_lock_authority.held(
            owner.lane, boundary
        ) as route_evidence:
            durable = dict(self.coordinator.status())
            if (
                durable.get("runId") != owner.run_id
                or durable.get("sessionId") != owner.session_id
                or durable.get("lane") != owner.lane
            ):
                raise CryptoFirstLiveRuntimeError(
                    "crypto-first-live-process-owner-no-longer-current"
                )
            dispatch = self.coordinator.assert_dispatch_authority(
                purpose=("CLEANUP_MUTATION" if cleanup else "ENTRY_ORDER"),
                lane=owner.lane,
                run_id=owner.run_id,
                session_id=owner.session_id,
                permit_id=owner.permit_id,
                permit_hash=owner.permit_hash,
                account_fingerprint=owner.account_fingerprint,
                baseline_hash=owner.baseline_hash,
                code_hash=owner.code_hash,
                owner_token=owner.owner_token,
                owner_epoch=owner.owner_epoch,
                expected_revision=int(durable["revision"]),
                route_lock_evidence=route_evidence,
            )
        return {"durable": durable, "dispatch": dict(dispatch)}

    def _memory_owner(self) -> _MemoryOwner:
        with self._lock:
            owner = self._owner
        if owner is None:
            raise CryptoFirstLiveRuntimeError(
                "crypto-first-live-process-owner-token-absent"
            )
        return owner

    def _boundary(self, lane: str) -> Callable[[], Any] | None:
        if lane == "UPBIT":
            return self.upbit_route_boundary
        if lane == "BINANCE_SPOT":
            return self.binance_route_boundary
        raise CryptoFirstLiveRuntimeError(
            "crypto-first-live-route-lane-invalid"
        )


__all__ = [
    "CRYPTO_FIRST_LIVE_EXTERNAL_WORM_AUTHORITY_RELEASED",
    "CRYPTO_FIRST_LIVE_ROOT_COMPOSITION_RELEASED",
    "CryptoFirstLiveRuntime",
    "CryptoFirstLiveRuntimeError",
    "InProcessRouteLockAuthority",
]
