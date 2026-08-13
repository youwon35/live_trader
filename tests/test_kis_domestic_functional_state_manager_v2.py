from __future__ import annotations

import hashlib
import hmac
import json
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone

from live_trader.kis_domestic_functional_state import (
    DurableKisDomesticFunctionalState,
    KisDomesticFunctionalStateBlocked,
)
from live_trader.program_ledger import ProgramLedger
from tests.test_kis_domestic_functional_manager import (
    ACCOUNT,
    CREDENTIAL,
    MANAGER_KEY,
    OWNER_EPOCH,
    KisDomesticFunctionalManagerTests as _ManagerFixture,
    digest,
)


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
OWNER_ID = "state-owned-kis-composition-v2-integration"
OWNER_EPOCH_ID = "kis-owner-epoch-one"
OWNER_KEY_ID_HASH = hashlib.sha256(b"owner-v2-test-key").hexdigest()
MANAGER_KEY_ID_HASH = hashlib.sha256(
    b"offline-kis-manager-key-v1"
).hexdigest()
MANAGER_TYPE = (
    "live_trader.kis_domestic_functional_manager."
    "DisabledKisDomesticFunctionalManager"
)
MANAGER_CODE_HASH = hashlib.sha256(b"manager-code-v2-test").hexdigest()
MANAGER_PROTOCOL_HASH = hashlib.sha256(
    b"state-manager-receipt-v2-integration"
).hexdigest()
STATE_KEY = b"state-v2-integration-test-key-material-32-bytes"
OWNERS = {
    "graph": "kis-graph-owner-v2",
    "backend": "kis-backend-owner-v2",
    "capability": "kis-capability-owner-v2",
    "transport": "kis-transport-owner-v2",
}


def canonical(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


class _Clock:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> datetime:
        self.calls += 1
        second = 0 if self.calls == 1 else 1 if self.calls == 2 else 3
        return datetime(2026, 8, 14, 4, 15, second, tzinfo=timezone.utc)


class _ManagerBridge:
    def __init__(self, *, timeout: bool = False) -> None:
        self.timeout = timeout
        self.state: DurableKisDomesticFunctionalState | None = None
        self.fixture: _ManagerFixture | None = None
        self.manager = None
        self.request = None
        self._release = None

    @staticmethod
    def _verify_hmac(value, *, domain: bytes) -> bool:
        try:
            raw = dict(value)
            signature = raw.pop("signature")
            receipt_hash = raw.pop("receiptHash")
            return bool(
                hmac.compare_digest(receipt_hash, digest(raw))
                and hmac.compare_digest(
                    signature,
                    hmac.new(
                        MANAGER_KEY,
                        domain + receipt_hash.encode(),
                        hashlib.sha256,
                    ).hexdigest(),
                )
            )
        except Exception:
            return False

    def verify_receipt(self, value) -> bool:
        domain = (
            b"KIS_FUNCTIONAL_MANAGER_RECEIPT\n"
            if "pdno" in value
            else b"KIS_MANAGER_RECEIPT\n"
        )
        return self._verify_hmac(value, domain=domain)

    @staticmethod
    def verify_binding(value) -> bool:
        try:
            raw = dict(value)
            signature = raw.pop("signature")
            binding_hash = raw.pop("bindingHash")
            return bool(
                hmac.compare_digest(binding_hash, digest(raw))
                and hmac.compare_digest(
                    signature,
                    hmac.new(
                        MANAGER_KEY,
                        b"KIS_STATE_MANAGER_BINDING\n"
                        + binding_hash.encode(),
                        hashlib.sha256,
                    ).hexdigest(),
                )
            )
        except Exception:
            return False

    def binding(self, request):
        fixture = _ManagerFixture(methodName="runTest")
        fixture.setUp()
        fixture.session = request["sessionId"]
        fixture.revision = request["reservationRevision"]
        fixture.owner_epoch_id = request["ownerEpochId"]
        fixture.statuses = {
            name: fixture._status(name)
            for name in (
                "state", "owner", "capability", "quote", "rolling",
                "heartbeat", "mutation", "transport",
            )
        }
        for status in fixture.statuses.values():
            status["ownerEpochHash"] = request["ownerEpochHash"]
            status["accountFingerprint"] = request["accountFingerprint"]
            status["credentialConfigurationHash"] = request[
                "credentialConfigurationHash"
            ]
        provisional = {
            "reservationId": request["reservationId"],
            "reservationKind": request["reservationKind"],
            "revision": request["reservationRevision"],
            "sessionId": request["sessionId"],
            "reservedAt": request["reservedAt"],
            "previousAccountFingerprint": request["accountFingerprint"],
            "previousCredentialConfigurationHash": request[
                "credentialConfigurationHash"
            ],
            "reservedAccountFingerprint": request["accountFingerprint"],
            "reservedCredentialConfigurationHash": request[
                "credentialConfigurationHash"
            ],
            "ownerEpochId": request["ownerEpochId"],
            "ownerEpochHash": request["ownerEpochHash"],
            "componentReadersHash": "0" * 64,
            "finalMutationBoundaryRequired": True,
            "finalMutationBoundaryHandleSchema":
                "kis-domestic-functional-final-reservation/v1",
            "finalMutationBoundaryHandle": "d" * 64,
            "productionAvailable": False,
        }
        requests = [
            fixture._request("NATURAL_BUY", request["reservationKind"])
        ]
        reservation = fixture._bind_plan(
            provisional, request["reservationKind"], requests
        )
        if self.timeout:
            fixture.sender_release.clear()
            self._release = fixture.sender_release
        manager = fixture._manager(timeout=0.02 if self.timeout else 0.25)
        if self.state is None:
            raise AssertionError("state must be attached before reservation")

        @contextmanager
        def actual_state_boundary(value):
            fixture.boundary_entries += 1
            fixture.boundary_active = True
            try:
                with self.state.final_mutation_boundary(
                    reservation=value
                ) as boundary:
                    yield boundary
            finally:
                fixture.boundary_active = False

        object.__setattr__(
            manager.adapters["state"],
            "_final_boundary_factory",
            actual_state_boundary,
        )
        body = {
            **dict(request),
            "schemaVersion":
                "kis-domestic-functional-state-manager-binding/v1",
            "componentReadersHash": reservation["componentReadersHash"],
            "managerImplementationType": MANAGER_TYPE,
            "managerIdHash": manager.manager_id_hash,
            "managerCodeHash": MANAGER_CODE_HASH,
            "managerProtocolHash": MANAGER_PROTOCOL_HASH,
            "managerKeyIdHash": MANAGER_KEY_ID_HASH,
            "managerComponentBindingsHash": manager.component_bindings_hash,
            "mutationPlanHash": fixture.current_plan["planHash"],
            "ownedProjectionHash": fixture.current_plan[
                "ownedProjectionHash"
            ],
            "ownedProjectionHeadHash": fixture.current_plan[
                "ownedProjection"
            ]["headHash"],
            "finalMutationBoundarySchema":
                "kis-domestic-functional-final-reservation/v1",
            "receiptSchemaVersion":
                "kis-domestic-functional-manager-receipt/v2",
            "verifyOnly": True,
        }
        binding_hash = digest(body)
        binding = {
            **body,
            "bindingHash": binding_hash,
            "signature": hmac.new(
                MANAGER_KEY,
                b"KIS_STATE_MANAGER_BINDING\n" + binding_hash.encode(),
                hashlib.sha256,
            ).hexdigest(),
        }
        self.fixture = fixture
        self.manager = manager
        self.request = requests
        return binding

    def configure_reservation(self, _reservation):
        return None

    def __call__(self, reservation):
        if self.manager is None or self.request is None:
            raise AssertionError("binding reader did not construct manager")
        return self.manager.execute(
            reservation=reservation,
            command=reservation["reservationKind"],
            mutation_requests=self.request,
        )

    def release_timeout(self) -> None:
        if self._release is not None:
            self._release.set()
        if self.fixture is not None:
            deadline = time.monotonic() + 1
            while self.fixture.boundary_active and time.monotonic() < deadline:
                time.sleep(0.005)


class KisDomesticFunctionalStateManagerV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "state-v2.sqlite3"
        self.ledger = ProgramLedger(self.path)
        self.session = ""
        self.clock = _Clock()
        self.bridge = _ManagerBridge()
        self.state = self._state(self.bridge)
        self.bridge.state = self.state

    def _component_reader(self, name):
        def read():
            return {
                "schemaVersion":
                    "kis-domestic-functional-component-status/v1",
                "component": name,
                "ownerHash": hashlib.sha256(OWNERS[name].encode()).hexdigest(),
                "route": ROUTE,
                "readable": True,
                "sessionId": self.session,
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "hazards": [],
                "functionalMutationIntent": {},
                "killOrdinaryCancelAllowed": False,
                "killOrdinaryCancelRevision": 0,
                "killOrdinaryCancelIntent": {},
                "productionAvailable": False,
            }

        return read

    @staticmethod
    def _owner_epoch():
        return {
            "schemaVersion": "kis-domestic-functional-owner-epoch/v1",
            "route": ROUTE,
            "ownerHash": hashlib.sha256(OWNER_ID.encode()).hexdigest(),
            "ownerEpochId": OWNER_EPOCH_ID,
            "applicationLeaseHeld": True,
            "observedAt": "2026-08-14T04:15:00.000000Z",
            "keyIdHash": OWNER_KEY_ID_HASH,
            "productionAvailable": False,
            "ownerEpochHash": OWNER_EPOCH,
            "signature": "e" * 64,
        }

    def _state(self, bridge, *, clock=None):
        return DurableKisDomesticFunctionalState(
            program_ledger=self.ledger,
            owner_id=OWNER_ID,
            component_owner_ids=OWNERS,
            component_readers={
                name: self._component_reader(name) for name in OWNERS
            },
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            application_lease_held=True,
            owner_epoch_reader=self._owner_epoch,
            owner_epoch_verifier=lambda _value: True,
            owner_epoch_key_id_hash=OWNER_KEY_ID_HASH,
            manager_receipt_verifier=bridge.verify_receipt,
            manager_receipt_key_id_hash=MANAGER_KEY_ID_HASH,
            manager_binding_reader=bridge.binding,
            manager_binding_verifier=bridge.verify_binding,
            manager_implementation_type=MANAGER_TYPE,
            manager_code_hash=MANAGER_CODE_HASH,
            manager_protocol_hash=MANAGER_PROTOCOL_HASH,
            state_signer_key=STATE_KEY,
            state_signer_key_id="state-v2-integration-key-v1",
            clock=clock or self.clock,
        )

    def test_finish_receipt_is_durable_and_restart_replays_it(self) -> None:
        result = self.state.start(
            session_id="kis-state-manager-v2-success",
            manager=self.bridge,
        )
        self.session = "kis-state-manager-v2-success"
        self.assertEqual("ACTIVE", result["phase"])
        self.assertFalse(result["reservationPending"])
        with self.ledger.connection() as conn:
            receipt = conn.execute(
                "SELECT * FROM kis_functional_state_manager_receipt"
            ).fetchone()
            binding = conn.execute(
                "SELECT * FROM kis_functional_state_manager_binding"
            ).fetchone()
            reserve_transition = conn.execute(
                "SELECT body_json FROM kis_functional_state_transition "
                "WHERE revision=2"
            ).fetchone()
        self.assertEqual("FINISH", receipt["receipt_kind"])
        self.assertEqual(result["managerReceiptHash"], receipt["manager_receipt_hash"])
        self.assertEqual(
            binding["reservation_binding_hash"],
            json.loads(reserve_transition[0])["reservationBindingHash"],
        )
        restarted_bridge = _ManagerBridge()
        restarted = self._state(
            restarted_bridge,
            clock=lambda: datetime(
                2026, 8, 14, 4, 15, 4, tzinfo=timezone.utc
            ),
        )
        restarted_bridge.state = restarted
        snapshot = restarted.authority_snapshot()
        self.assertEqual("ACTIVE", snapshot["functionalPhase"])
        self.assertTrue(snapshot["stateReceiptV2IntegrationWired"])

    def test_detached_timeout_persists_pending_and_cannot_finish_or_reopen(self) -> None:
        bridge = _ManagerBridge(timeout=True)
        state = self._state(bridge)
        bridge.state = state
        result = state.start(
            session_id="kis-state-manager-v2-timeout",
            manager=bridge,
        )
        self.assertEqual("RECONCILIATION_REQUIRED", result["phase"])
        self.assertTrue(result["reservationPending"])
        self.assertTrue(result["reservationId"])
        snapshot = state.authority_snapshot()
        self.assertTrue(snapshot["ordinaryRoutesClosed"])
        self.assertEqual(result["reservationId"], snapshot["reservationId"])
        self.assertEqual("PENDING", snapshot["managerReceiptKind"])
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "phase|reservation"
        ):
            state.start(
                session_id="kis-state-manager-v2-reopen",
                manager=bridge,
            )
        bridge.release_timeout()

    def test_durable_receipt_tamper_fails_closed_after_restart(self) -> None:
        self.state.start(
            session_id="kis-state-manager-v2-tamper",
            manager=self.bridge,
        )
        self.session = "kis-state-manager-v2-tamper"
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_state_manager_receipt "
                "SET execution_proof_hash=?",
                ("f" * 64,),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "receipt integrity"
        ):
            self.state.authority_snapshot()

    def test_durable_manager_binding_tamper_fails_closed_after_restart(self) -> None:
        self.state.start(
            session_id="kis-state-manager-v2-binding-tamper",
            manager=self.bridge,
        )
        self.session = "kis-state-manager-v2-binding-tamper"
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_state_manager_binding "
                "SET manager_binding_hash=?",
                ("f" * 64,),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked, "binding integrity"
        ):
            self.state.authority_snapshot()

    def test_graph_manager_projection_hash_substitution_is_rejected_before_boundary(self) -> None:
        original_binding = self.bridge.binding

        def substituted(request):
            value = original_binding(request)
            body = dict(value)
            body.pop("bindingHash")
            body.pop("signature")
            body["ownedProjectionHeadHash"] = "f" * 64
            binding_hash = digest(body)
            return {
                **body,
                "bindingHash": binding_hash,
                "signature": hmac.new(
                    MANAGER_KEY,
                    b"KIS_STATE_MANAGER_BINDING\n" + binding_hash.encode(),
                    hashlib.sha256,
                ).hexdigest(),
            }

        self.state._manager_binding_reader = substituted
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked,
            "plan/projection binding changed|projection|ownedProjectionHeadHash",
        ):
            self.state.start(
                session_id="kis-state-manager-v2-graph-substitution",
                manager=self.bridge,
            )
        if self.bridge.fixture is not None:
            self.assertEqual(0, self.bridge.fixture.boundary_entries)

    def test_partial_or_prior_schema_is_not_migrated_in_place(self) -> None:
        prior_path = Path(self.temp.name) / "state-v3-partial.sqlite3"
        prior_ledger = ProgramLedger(prior_path)
        with prior_ledger.connection() as conn:
            conn.execute(
                "CREATE TABLE kis_functional_state_schema("
                "singleton INTEGER PRIMARY KEY,version TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO kis_functional_state_schema VALUES(1,?)",
                ("kis-domestic-functional-state-schema/v3",),
            )
        original = self.ledger
        self.ledger = prior_ledger
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalStateBlocked, "schema fingerprint"
            ):
                self._state(_ManagerBridge())
        finally:
            self.ledger = original

    def test_deleted_finish_receipt_is_detected_from_signed_transition_history(self) -> None:
        self.state.start(
            session_id="kis-state-manager-v2-delete-receipt",
            manager=self.bridge,
        )
        self.session = "kis-state-manager-v2-delete-receipt"
        with self.ledger.connection() as conn:
            conn.execute("DELETE FROM kis_functional_state_manager_receipt")
        restarted_bridge = _ManagerBridge()
        restarted = self._state(
            restarted_bridge,
            clock=lambda: datetime(
                2026, 8, 14, 4, 15, 4, tzinfo=timezone.utc
            ),
        )
        restarted_bridge.state = restarted
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked,
            "terminal receipt is missing",
        ):
            restarted.authority_snapshot()

    def test_deleted_binding_and_receipt_are_detected_from_signed_transitions(self) -> None:
        self.state.start(
            session_id="kis-state-manager-v2-delete-both",
            manager=self.bridge,
        )
        self.session = "kis-state-manager-v2-delete-both"
        with self.ledger.connection() as conn:
            conn.execute("DELETE FROM kis_functional_state_manager_receipt")
            conn.execute("DELETE FROM kis_functional_state_manager_binding")
        restarted_bridge = _ManagerBridge()
        restarted = self._state(
            restarted_bridge,
            clock=lambda: datetime(
                2026, 8, 14, 4, 15, 4, tzinfo=timezone.utc
            ),
        )
        restarted_bridge.state = restarted
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked,
            "reservation/binding cardinality mismatch",
        ):
            restarted.authority_snapshot()

    def test_deleted_pending_receipt_cannot_be_downgraded_to_inflight_crash(self) -> None:
        bridge = _ManagerBridge(timeout=True)
        state = self._state(bridge)
        bridge.state = state
        state.start(
            session_id="kis-state-manager-v2-delete-pending",
            manager=bridge,
        )
        self.session = "kis-state-manager-v2-delete-pending"
        bridge.release_timeout()
        with self.ledger.connection() as conn:
            conn.execute("DELETE FROM kis_functional_state_manager_receipt")
        restarted_bridge = _ManagerBridge()
        restarted = self._state(
            restarted_bridge,
            clock=lambda: datetime(
                2026, 8, 14, 4, 15, 4, tzinfo=timezone.utc
            ),
        )
        restarted_bridge.state = restarted
        with self.assertRaisesRegex(
            KisDomesticFunctionalStateBlocked,
            "zero-receipt reservation is not fail-closed",
        ):
            restarted.authority_snapshot()


if __name__ == "__main__":
    unittest.main()
