from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_owner import (
    HAZARD_SCHEMA,
    DurableKisDomesticFunctionalOwner,
    KisDomesticFunctionalOwnerBlocked,
    owner_component_status,
)


NOW = datetime(2026, 8, 14, 0, 0, tzinfo=timezone.utc)
COMPONENTS = (
    "lane", "source", "rolling", "heartbeat", "mutation",
    "capability", "quote", "graph", "truth",
)
SESSION_ID = "kis-owner-session-0001"


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class _Clock:
    def __init__(self) -> None:
        self.value = NOW
        self.monotonic_ns = 1_000_000_000

    def __call__(self):
        return self.value

    def monotonic(self):
        return self.monotonic_ns

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        self.monotonic_ns += int(seconds * 1_000_000_000)


class _Authority:
    def __init__(self) -> None:
        self.key = hashlib.sha256(b"owner-authority").digest()

    def sign(self, domain, body):
        return hmac.new(
            self.key, domain.encode() + b"\0" + _canonical(body), hashlib.sha256
        ).hexdigest()

    def verify(self, domain, body, signature):
        return hmac.compare_digest(signature, self.sign(domain, body))


class _FakeLease:
    def __init__(self, coordinator, scope) -> None:
        self.coordinator = coordinator
        self.scope = scope
        self.path = Path(hashlib.sha256(scope.encode()).hexdigest() + ".lock")
        self.held = True

    def is_held(self):
        return self.held and self.coordinator.current is self

    def status(self, *, reused=False):
        return {
            "acquired": self.is_held(),
            "scopeHash": self.path.stem,
            "ownerPid": 42,
            "acquiredAt": NOW.isoformat().replace("+00:00", "Z"),
            "reused": reused,
        }

    def release(self):
        if self.coordinator.current is self:
            self.coordinator.current = None
        self.held = False


class _LeaseCoordinator:
    def __init__(self) -> None:
        self.current = None

    def acquire(self, scope):
        if self.current is not None and self.current.is_held():
            return None
        lease = _FakeLease(self, scope)
        self.current = lease
        return lease

    def simulate_process_death(self):
        if self.current is not None:
            self.current.held = False
        self.current = None


class _Hazards:
    def __init__(self, authority) -> None:
        self.authority = authority
        self.values = {
            component: {
                "hazardousAuthorityOpen": False,
                "ownedWorkingExposurePresent": False,
                "ownedPositionExposurePresent": False,
                "nonterminalOrphanCount": 0,
                "timedOutCallCount": 0,
                "detachedCallCount": 0,
            }
            for component in COMPONENTS
        }
        self.pins = {
            component: {
                "componentReaderHash": hashlib.sha256(
                    f"{component}:reader".encode()
                ).hexdigest(),
                "componentFileHash": hashlib.sha256(
                    f"{component}:file".encode()
                ).hexdigest(),
                "componentProtocolHash": hashlib.sha256(
                    f"{component}:protocol".encode()
                ).hexdigest(),
                "authorityKeyIdHash": hashlib.sha256(
                    f"{component}:authority".encode()
                ).hexdigest(),
            }
            for component in COMPONENTS
        }

    def readers(self):
        result = {}
        for component in COMPONENTS:
            def reader(request, component=component):
                value = self.values[component]
                body = {
                    "schemaVersion": HAZARD_SCHEMA,
                    "component": component,
                    **self.pins[component],
                    "routeObservationId": request["routeObservationId"],
                    "routeFenceRevision": request["routeFenceRevision"],
                    "routeFenceHash": request["routeFenceHash"],
                    "observedAt": request["observedAt"],
                    "observedMonotonicNs": request["observedMonotonicNs"],
                    "componentRevision": 1,
                    "componentHeadHash": hashlib.sha256(
                        f"{component}:head".encode()
                    ).hexdigest(),
                    "sessionId": SESSION_ID,
                    "authorityExpiresAt": (
                        datetime.fromisoformat(
                            request["observedAt"].replace("Z", "+00:00")
                        )
                        + timedelta(hours=2)
                    ).isoformat().replace("+00:00", "Z"),
                    **value,
                }
                domain = f"KIS_FUNCTIONAL_OWNER_HAZARD:{component.upper()}"
                return {
                    **body,
                    "recordHash": hashlib.sha256(_canonical(body)).hexdigest(),
                    "signature": self.authority.sign(domain, body),
                }
            result[component] = reader
        return result


class _Fixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "owner.sqlite3"
        self.clock = _Clock()
        self.authority = _Authority()
        self.coordinator = _LeaseCoordinator()
        self.hazards = _Hazards(self.authority)

    def owner(self, **kwargs):
        lease_factory = kwargs.pop("lease_factory", self.coordinator.acquire)
        allow_mock = kwargs.pop("allow_mock_lease_factory", True)
        return DurableKisDomesticFunctionalOwner(
            self.path,
            hazard_readers=self.hazards.readers(),
            hazard_reader_pins=self.hazards.pins,
            hazard_verifiers={
                component: self.authority.verify for component in COMPONENTS
            },
            signer=self.authority.sign,
            verifier=self.authority.verify,
            server_authority_key_id="test-owner-authority-v1",
            trusted_wall_clock=self.clock,
            trusted_monotonic_clock=self.clock.monotonic,
            lease_factory=lease_factory,
            allow_mock_lease_factory=allow_mock,
            heartbeat_timeout_seconds=30,
            **kwargs,
        )

    def cleanup(self):
        self.coordinator.simulate_process_death()
        self.temp.cleanup()


class KisDomesticFunctionalOwnerTest(unittest.TestCase):
    def test_first_acquire_heartbeat_guard_and_clean_release(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            self.assertEqual(1, owner.epoch)
            self.assertEqual("ACTIVE", owner.status(expected_epoch=1)["state"])
            heartbeat = owner.heartbeat(expected_epoch=1)
            self.assertEqual(2, heartbeat["heartbeatCount"])
            guard = owner.guard_operation(expected_epoch=1)
            self.assertTrue(guard["authorityFresh"])
            released = owner.release(expected_epoch=1)
            self.assertEqual("RELEASED", released["state"])
            self.assertFalse(released["osLeaseHeld"])
        finally:
            fixture.cleanup()

    def test_second_process_is_blocked_by_kernel_lease(self):
        fixture = _Fixture()
        try:
            first = fixture.owner()
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "os-lease-unavailable"):
                fixture.owner()
            self.assertEqual("ACTIVE", first.status(expected_epoch=1)["state"])
            first.release(expected_epoch=1)
        finally:
            fixture.cleanup()

    def test_clean_release_allows_new_epoch_but_stale_epoch_fails(self):
        fixture = _Fixture()
        try:
            first = fixture.owner()
            first.release(expected_epoch=1)
            second = fixture.owner()
            self.assertEqual(2, second.epoch)
            self.assertEqual("ACTIVE", second.status(expected_epoch=2)["state"])
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "epoch-stale"):
                second.heartbeat(expected_epoch=1)
            self.assertEqual(1, second.status(expected_epoch=2)["heartbeatCount"])
            second.release(expected_epoch=2)
        finally:
            fixture.cleanup()

    def test_old_process_absence_nonterminal_restart_never_reissues_active(self):
        fixture = _Fixture()
        try:
            first = fixture.owner()
            self.assertEqual(1, first.epoch)
            fixture.coordinator.simulate_process_death()
            second = fixture.owner()
            status = second.status(expected_epoch=2)
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual(
                "OLD_PROCESS_ABSENT_NONTERMINAL_OR_HAZARDOUS_EPOCH",
                status["reason"],
            )
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "release-forbidden"):
                second.release(expected_epoch=2)
        finally:
            fixture.cleanup()

    def test_acquire_transaction_failure_rolls_back_and_releases_os_lease(self):
        fixture = _Fixture()
        try:
            def fail(stage):
                if stage == "BEFORE_ACQUIRE_COMMIT":
                    raise RuntimeError("inject")

            with self.assertRaisesRegex(RuntimeError, "inject"):
                fixture.owner(failure_injector=fail)
            self.assertIsNone(fixture.coordinator.current)
            with closing(sqlite3.connect(fixture.path)) as conn:
                self.assertEqual(
                    0,
                    conn.execute("SELECT COUNT(*) FROM kis_functional_route_owner").fetchone()[0],
                )
            owner = fixture.owner()
            self.assertEqual(1, owner.epoch)
            owner.release(expected_epoch=1)
        finally:
            fixture.cleanup()

    def test_heartbeat_gap_terminalizes_and_cannot_be_revived(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            fixture.clock.value += timedelta(seconds=31)
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "hazard-or-stale"):
                owner.heartbeat(expected_epoch=1)
            status = owner.status(expected_epoch=1)
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual("HEARTBEAT_WALL_LEASE_STALE", status["reason"])
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "release-forbidden"):
                owner.release(expected_epoch=1)
        finally:
            fixture.cleanup()

    def test_orphan_and_owned_exposure_union_blocks_operation_and_release(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            fixture.hazards.values["lane"]["nonterminalOrphanCount"] = 2
            fixture.hazards.values["mutation"]["ownedWorkingExposurePresent"] = True
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "hazard-or-stale"):
                owner.guard_operation(expected_epoch=1)
            status = owner.status(expected_epoch=1)
            self.assertEqual(2, status["orphanCount"])
            self.assertTrue(status["ownedExposurePresent"])
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "release-forbidden"):
                owner.release(expected_epoch=1)
        finally:
            fixture.cleanup()

    def test_timeout_and_detached_call_hazard_blocks_reissue(self):
        fixture = _Fixture()
        try:
            fixture.hazards.values["graph"]["timedOutCallCount"] = 1
            fixture.hazards.values["graph"]["detachedCallCount"] = 1
            first = fixture.owner()
            status = first.status(expected_epoch=1)
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual(1, status["timedOutCallCount"])
            self.assertEqual(1, status["detachedCallCount"])
            fixture.coordinator.simulate_process_death()
            fixture.hazards.values["graph"]["timedOutCallCount"] = 0
            fixture.hazards.values["graph"]["detachedCallCount"] = 0
            second = fixture.owner()
            self.assertEqual(
                "RECONCILIATION_REQUIRED",
                second.status(expected_epoch=2)["state"],
            )
        finally:
            fixture.cleanup()

    def test_lost_os_lease_blocks_every_operation_without_db_mutation(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            fixture.coordinator.simulate_process_death()
            for method in (
                lambda: owner.status(expected_epoch=1),
                lambda: owner.heartbeat(expected_epoch=1),
                lambda: owner.guard_operation(expected_epoch=1),
                lambda: owner.release(expected_epoch=1),
            ):
                with self.subTest(method=method):
                    with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "os-lease-lost"):
                        method()
            with closing(sqlite3.connect(fixture.path)) as conn:
                row = conn.execute(
                    "SELECT epoch,state,revision FROM kis_functional_route_owner"
                ).fetchone()
            self.assertEqual((1, "ACTIVE", 1), row)
        finally:
            fixture.cleanup()

    def test_schema_transition_tamper_and_disabled_status_fail_closed(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            with closing(sqlite3.connect(fixture.path)) as conn:
                conn.execute(
                    "UPDATE kis_functional_owner_transition SET previous_hash=?",
                    ("f" * 64,),
                )
                conn.commit()
            with self.assertRaisesRegex(KisDomesticFunctionalOwnerBlocked, "transition-chain-integrity"):
                owner.status(expected_epoch=1)
            component = owner_component_status()
            self.assertFalse(component["productionAvailable"])
            self.assertFalse(component["networkAvailable"])
            self.assertFalse(component["mutationAvailable"])
            self.assertFalse(component["networkOrderPostAllowed"])
            self.assertEqual(0, component["tradingMutationCount"])
            self.assertRegex(component["schemaFingerprint"], r"^[0-9a-f]{64}$")
        finally:
            fixture.cleanup()

    def test_stale_release_terminalizes_and_keeps_os_lease(self):
        cases = (
            ("stale", "HEARTBEAT_WALL_LEASE_STALE"),
            ("wall-rollback", "TRUSTED_WALL_CLOCK_ROLLBACK"),
            ("monotonic-rollback", "TRUSTED_MONOTONIC_CLOCK_ROLLBACK"),
        )
        for kind, expected_reason in cases:
            with self.subTest(kind=kind):
                fixture = _Fixture()
                try:
                    owner = fixture.owner()
                    if kind == "stale":
                        fixture.clock.advance(31)
                    elif kind == "wall-rollback":
                        fixture.clock.value -= timedelta(seconds=1)
                    else:
                        fixture.clock.monotonic_ns -= 1
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalOwnerBlocked, "release-forbidden"
                    ):
                        owner.release(expected_epoch=1)
                    status = owner.status(expected_epoch=1)
                    self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
                    self.assertEqual(expected_reason, status["reason"])
                    self.assertTrue(status["osLeaseHeld"])
                    self.assertIsNotNone(fixture.coordinator.current)
                finally:
                    fixture.cleanup()

    def test_wall_and_monotonic_rollback_each_terminalize_guard(self):
        for kind in ("wall", "monotonic"):
            with self.subTest(kind=kind):
                fixture = _Fixture()
                try:
                    owner = fixture.owner()
                    fixture.clock.advance(1)
                    owner.heartbeat(expected_epoch=1)
                    if kind == "wall":
                        fixture.clock.value -= timedelta(seconds=2)
                        expected = "TRUSTED_WALL_CLOCK_ROLLBACK"
                    else:
                        fixture.clock.monotonic_ns -= 2_000_000_000
                        expected = "TRUSTED_MONOTONIC_CLOCK_ROLLBACK"
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalOwnerBlocked, "hazard-or-stale"
                    ):
                        owner.guard_operation(expected_epoch=1)
                    status = owner.status(expected_epoch=1)
                    self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
                    self.assertEqual(expected, status["reason"])
                    self.assertFalse(status["heartbeatLeaseFresh"])
                finally:
                    fixture.cleanup()

    def test_signed_hazard_pin_drift_is_rejected_before_owner_mutation(self):
        for kind in ("file-pin", "signature"):
            with self.subTest(kind=kind):
                fixture = _Fixture()
                try:
                    owner = fixture.owner()
                    if kind == "file-pin":
                        fixture.hazards.pins["lane"]["componentFileHash"] = (
                            "f" * 64
                        )
                        message = "reader-pin-mismatch"
                    else:
                        original = owner.hazard_readers["lane"]

                        def tampered(request):
                            return {
                                **original(request),
                                "signature": "f" * 64,
                            }

                        owner.hazard_readers["lane"] = tampered
                        message = "signature-mismatch"
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalOwnerBlocked, message
                    ):
                        owner.guard_operation(expected_epoch=1)
                    with closing(sqlite3.connect(fixture.path)) as conn:
                        row = conn.execute(
                            "SELECT state,revision FROM kis_functional_route_owner"
                        ).fetchone()
                    self.assertEqual(("ACTIVE", 1), row)
                finally:
                    fixture.cleanup()

    def test_monotonic_only_forward_gap_terminalizes(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            fixture.clock.monotonic_ns += 31_000_000_000
            with self.assertRaisesRegex(
                KisDomesticFunctionalOwnerBlocked, "hazard-or-stale"
            ):
                owner.heartbeat(expected_epoch=1)
            status = owner.status(expected_epoch=1)
            self.assertEqual("RECONCILIATION_REQUIRED", status["state"])
            self.assertEqual("HEARTBEAT_MONOTONIC_LEASE_STALE", status["reason"])
        finally:
            fixture.cleanup()

    def test_mock_lease_requires_explicit_offline_flag_and_exact_scope(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalOwnerBlocked, "lease-factory-not-code-pinned"
            ):
                DurableKisDomesticFunctionalOwner(
                    fixture.path,
                    hazard_readers=fixture.hazards.readers(),
                    hazard_reader_pins=fixture.hazards.pins,
                    hazard_verifiers={
                        component: fixture.authority.verify
                        for component in COMPONENTS
                    },
                    signer=fixture.authority.sign,
                    verifier=fixture.authority.verify,
                    server_authority_key_id="test-owner-authority-v1",
                    trusted_wall_clock=fixture.clock,
                    trusted_monotonic_clock=fixture.clock.monotonic,
                    lease_factory=fixture.coordinator.acquire,
                )

            def wrong_scope_factory(scope):
                lease = _FakeLease(fixture.coordinator, scope + ":wrong")
                fixture.coordinator.current = lease
                return lease

            with self.assertRaisesRegex(
                KisDomesticFunctionalOwnerBlocked, "scope-or-factory-mismatch"
            ):
                fixture.owner(lease_factory=wrong_scope_factory)
        finally:
            fixture.cleanup()

    def test_status_keeps_external_verify_only_authority_blocker(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            status = owner.status(expected_epoch=1)
            self.assertFalse(status["verifyOnlyProductionAuthorityPinned"])
            self.assertIn(
                "EXTERNAL_VERIFY_ONLY_HAZARD_AUTHORITY_REGISTRY_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertFalse(status["leaseFactoryPinned"])
            self.assertFalse(status["productionAvailable"])
            owner.release(expected_epoch=1)
        finally:
            fixture.cleanup()

    def test_route_instant_hazard_revision_head_session_and_expiry_are_exact(self):
        cases = (
            ("observedAt", "2026-08-14T00:00:01Z", "contract-invalid"),
            ("componentRevision", 0, "contract-invalid"),
            ("componentHeadHash", "not-a-hash", "component-head"),
            ("sessionId", "another-session", "session-or-expiry-mismatch"),
            ("authorityExpiresAt", "2026-08-13T23:59:59Z", "expiry-invalid"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                fixture = _Fixture()
                try:
                    owner = fixture.owner()
                    original = owner.hazard_readers["lane"]

                    def tampered(request, field=field, value=value):
                        record = original(request)
                        unsigned = {
                            key: item
                            for key, item in record.items()
                            if key not in {"recordHash", "signature"}
                        }
                        unsigned[field] = value
                        domain = "KIS_FUNCTIONAL_OWNER_HAZARD:LANE"
                        return {
                            **unsigned,
                            "recordHash": hashlib.sha256(
                                _canonical(unsigned)
                            ).hexdigest(),
                            "signature": fixture.authority.sign(
                                domain, unsigned
                            ),
                        }

                    owner.hazard_readers["lane"] = tampered
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalOwnerBlocked, message
                    ):
                        owner.guard_operation(expected_epoch=1)
                    with closing(sqlite3.connect(fixture.path)) as conn:
                        self.assertEqual(
                            ("ACTIVE", 1),
                            conn.execute(
                                "SELECT state,revision FROM "
                                "kis_functional_route_owner"
                            ).fetchone(),
                        )
                finally:
                    fixture.cleanup()

    def test_every_authorization_row_projection_is_verified(self):
        cases = (
            ("pdno", "99"),
            ("process_identity_hash", "f" * 64),
            ("lease_scope_hash", "f" * 64),
            ("hazardous_authority_open", 1),
            ("owned_exposure_present", 1),
            ("orphan_count", 1),
            ("timed_out_call_count", 1),
            ("detached_call_count", 1),
            ("reason", "TAMPERED"),
            ("session_id", "tampered-session"),
            ("route_observation_id", "f" * 64),
            ("authority_expires_at", "2027-01-01T00:00:00Z"),
        )
        for column, value in cases:
            with self.subTest(column=column):
                fixture = _Fixture()
                try:
                    owner = fixture.owner()
                    conn = sqlite3.connect(fixture.path)
                    try:
                        conn.execute(
                            f"UPDATE kis_functional_route_owner SET {column}=?",
                            (value,),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalOwnerBlocked,
                        "projection-mismatch|record-integrity-invalid",
                    ):
                        owner.status(expected_epoch=1)
                finally:
                    fixture.cleanup()

    def test_hazard_route_observation_is_single_instant_but_not_live_fenced(self):
        fixture = _Fixture()
        try:
            owner = fixture.owner()
            status = owner.status(expected_epoch=1)
            self.assertEqual(SESSION_ID, status["sessionId"])
            self.assertEqual(status["routeObservationId"], status["routeFenceHash"])
            self.assertEqual(1, status["routeFenceRevision"])
            self.assertFalse(status["sharedRouteFenceWired"])
            self.assertIn("SHARED_ROUTE_FENCE_NOT_WIRED", status["readinessBlockers"])
            with closing(sqlite3.connect(fixture.path)) as conn:
                persisted = json.loads(
                    conn.execute(
                        "SELECT record_json FROM kis_functional_route_owner"
                    ).fetchone()[0]
                )
            observed = {
                item["observedAt"]
                for item in persisted["hazardComponents"].values()
            }
            self.assertEqual({status["hazardObservedAt"]}, observed)
        finally:
            fixture.cleanup()


if __name__ == "__main__":
    unittest.main()
