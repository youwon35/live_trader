import base64
import hashlib
import hmac
import json
from datetime import timedelta
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_app_facade import (
    DurableKisDomesticFunctionalAppFacade,
    KisDomesticFunctionalAppFacadeBlocked,
    derive_facade_anchor_ledger_id,
    derive_state_owner_epoch_binding,
    production_entrypoint_status,
)
from live_trader.kis_domestic_functional_facade_anchor import (
    INSTALLATION_SCHEMA as ANCHOR_INSTALLATION_SCHEMA,
    ROOT_SIGNATURE_DOMAIN as ANCHOR_ROOT_DOMAIN,
    WRITER_PURPOSE as ANCHOR_WRITER_PURPOSE,
    WRITER_SIGNATURE_DOMAIN as ANCHOR_WRITER_DOMAIN,
    AppendOnlyKisDomesticFunctionalFacadeAnchor,
    ExternalFacadeAnchorPins,
    KisDomesticFunctionalFacadeAnchorBlocked,
    _path_hash as anchor_path_hash,
)
from live_trader.kis_domestic_functional_manager_authority import (
    MANAGER_KEY_PURPOSE,
    MANIFEST_SCHEMA as MANAGER_MANIFEST_SCHEMA,
    ROOT_SIGNATURE_DOMAIN as MANAGER_ROOT_DOMAIN,
    ManagerAuthorityPins,
    VerifyOnlyKisDomesticFunctionalManagerAuthority,
)
from live_trader.kis_domestic_functional_state import (
    DurableKisDomesticFunctionalState,
)
from live_trader.program_ledger import ProgramLedger
from live_trader.kis_order_authority import kis_route_authority_serialization
from tests.test_kis_domestic_functional_key_registry import (
    ACCOUNT,
    CREDENTIAL,
)
from tests.test_kis_domestic_functional_owner import _Fixture as OwnerFixture
from tests.test_kis_domestic_functional_production_factory import (
    _FactoryFixture,
)


OWNERS = {
    "graph": "facade-graph-owner-v1",
    "backend": "facade-backend-owner-v1",
    "capability": "facade-capability-owner-v1",
    "transport": "facade-transport-owner-v1",
}
STATE_OWNER_ID = "facade-state-owner-v1"


def _hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _canonical_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _public_pem(key):
    return key.public_key().export_key(format="PEM")


def _key_hash(key):
    return hashlib.sha256(_public_pem(key).encode("utf-8")).hexdigest()


def _ed_sign(key, domain, body):
    return base64.b64encode(
        eddsa.new(key, mode="rfc8032").sign(
            domain + _canonical_bytes(body)
        )
    ).decode("ascii")


class _Fixture:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.owner_fixture = OwnerFixture()
        self.factory_fixture = _FactoryFixture()
        self.owner_fixture.clock.value = self.factory_fixture.now
        self.owner_fixture.clock.monotonic_ns = self.factory_fixture.monotonic_ns
        import tests.test_kis_domestic_functional_owner as owner_test_module

        self.original_owner_test_now = owner_test_module.NOW
        owner_test_module.NOW = self.factory_fixture.now
        self.owner = self.owner_fixture.owner()
        self.factory_fixture.owner_path = self.owner_fixture.path
        self.factory_fixture.owner_body = self._owner_record()
        pins = self.factory_fixture.pins(
            owner_epoch=self.owner.epoch,
            owner_authority_key_id_hash=self.owner.authority_key_id_hash,
        )
        owner_private, owner_key_hash = self.factory_fixture.registry_fixture.keys[
            "OWNER_STATE_VERIFY"
        ]
        self.owner.authority_key_id_hash = owner_key_hash
        pins = self.factory_fixture.pins(
            owner_epoch=self.owner.epoch,
            owner_authority_key_id_hash=owner_key_hash,
        )
        self.owner.signer = lambda domain, body: self._registry_sign(
            owner_private, domain, body
        )
        self.owner.verifier = lambda domain, body, signature: hmac.compare_digest(
            signature, self._registry_sign(owner_private, domain, body)
        )
        self._resign_owner_database(owner_private, owner_key_hash)
        self.factory = self.factory_fixture.factory(
            owner_epoch=self.owner.epoch,
            owner_authority_key_id_hash=owner_key_hash,
        )

        self.ledger_path = Path(self.temp.name) / "facade-program.sqlite3"
        self.ledger = ProgramLedger(self.ledger_path)
        self.high_water_path = Path(self.temp.name) / "facade-high-water.json"
        self.session = ""
        self.mutation_intent = {}
        self.kill_allowed = False
        self.kill_revision = 0
        self.kill_intent = {}
        self.state_owner_epoch = self._state_owner_epoch()
        self.state = self.new_state()
        self.facade = self.new_facade()

    @staticmethod
    def _registry_sign(private_key, domain, body):
        from tests.test_kis_domestic_functional_key_registry import _signature

        return _signature(
            private_key,
            body,
            prefix=domain.encode("utf-8") + b"\0",
        )

    def _owner_record(self):
        conn = sqlite3.connect(self.owner_fixture.path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT record_json FROM kis_functional_route_owner "
                "WHERE route=? AND epoch=?",
                ("KIS_KR_LIVE_CONTINUOUS", self.owner.epoch),
            ).fetchone()
            return json.loads(row["record_json"])
        finally:
            conn.close()

    def _resign_owner_database(self, private_key, key_id_hash):
        conn = sqlite3.connect(self.owner_fixture.path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM kis_functional_owner_transition ORDER BY revision"
            ).fetchall()
            previous = "0" * 64
            last_owner = None
            for row in rows:
                body = json.loads(row["record_json"])
                body["authorityKeyIdHash"] = key_id_hash
                body["previousHash"] = previous
                record_hash = _hash(body)
                signature = self._registry_sign(
                    private_key, "KIS_FUNCTIONAL_OWNER_TRANSITION", body
                )
                conn.execute(
                    "UPDATE kis_functional_owner_transition SET "
                    "previous_hash=?,record_json=?,record_hash=?,signature=?,"
                    "authority_key_id_hash=? WHERE revision=?",
                    (
                        previous,
                        json.dumps(body, sort_keys=True, separators=(",", ":")),
                        record_hash,
                        signature,
                        key_id_hash,
                        row["revision"],
                    ),
                )
                previous = record_hash
                last_owner = {
                    key: value
                    for key, value in body.items()
                    if key
                    not in {"previousHash", "occurredAt", "occurredMonotonicNs"}
                }
            if last_owner is None:
                raise AssertionError("owner transition required")
            owner_hash = _hash(last_owner)
            owner_signature = self._registry_sign(
                private_key, "KIS_FUNCTIONAL_OWNER", last_owner
            )
            conn.execute(
                "UPDATE kis_functional_route_owner SET authority_key_id_hash=?,"
                "record_json=?,record_hash=?,signature=? WHERE route=? AND epoch=?",
                (
                    key_id_hash,
                    json.dumps(
                        last_owner, sort_keys=True, separators=(",", ":")
                    ),
                    owner_hash,
                    owner_signature,
                    "KIS_KR_LIVE_CONTINUOUS",
                    self.owner.epoch,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _state_owner_epoch(self):
        binding = derive_state_owner_epoch_binding(
            owner_status=self.owner.status(expected_epoch=self.owner.epoch),
            factory_status=self.factory.status(),
            owner_authority_key_id_hash=self.owner.authority_key_id_hash,
        )
        body = {
            "schemaVersion": "kis-domestic-functional-owner-epoch/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "ownerHash": hashlib.sha256(STATE_OWNER_ID.encode()).hexdigest(),
            "ownerEpochId": binding["ownerEpochId"],
            "applicationLeaseHeld": True,
            "observedAt": "2026-08-14T00:00:00Z",
            "keyIdHash": hashlib.sha256(b"facade-state-owner-key").hexdigest(),
            "productionAvailable": False,
        }
        return {
            **body,
            "ownerEpochHash": binding["ownerEpochHash"],
            "signature": "e" * 64,
        }

    def _component_reader(self, name):
        def reader():
            return {
                "schemaVersion": "kis-domestic-functional-component-status/v1",
                "component": name,
                "ownerHash": hashlib.sha256(OWNERS[name].encode()).hexdigest(),
                "route": "KIS_KR_LIVE_CONTINUOUS",
                "readable": True,
                "sessionId": self.session,
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "hazards": [],
                "functionalMutationIntent": (
                    dict(self.mutation_intent) if name == "graph" else {}
                ),
                "killOrdinaryCancelAllowed": (
                    self.kill_allowed if name == "graph" else False
                ),
                "killOrdinaryCancelRevision": (
                    self.kill_revision if name == "graph" else 0
                ),
                "killOrdinaryCancelIntent": (
                    dict(self.kill_intent) if name == "graph" else {}
                ),
                "productionAvailable": False,
            }

        return reader

    def new_state(self):
        constructors = self.factory.state_manager_constructors()
        _private, graph_key_hash = self.factory_fixture.registry_fixture.keys[
            "GRAPH_RECORD_VERIFY"
        ]
        verifier_kwargs = constructors.state_verifier_kwargs(
            manager_binding_reader=self._manager_binding,
            manager_key_id_hash=graph_key_hash,
        )
        return constructors.state_constructor(
            program_ledger=self.ledger,
            owner_id=STATE_OWNER_ID,
            component_owner_ids=OWNERS,
            component_readers={name: self._component_reader(name) for name in OWNERS},
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            application_lease_held=True,
            owner_epoch_reader=lambda: dict(self.state_owner_epoch),
            owner_epoch_verifier=lambda value: value == self.state_owner_epoch,
            owner_epoch_key_id_hash=self.state_owner_epoch["keyIdHash"],
            state_signer_key=b"facade-state-signing-key-material-0001",
            state_signer_key_id="facade-state-signing-key-v1",
            clock=self.owner_fixture.clock,
            **verifier_kwargs,
        )

    def _manager_binding(self, request):
        constructors = self.factory.state_manager_constructors()
        private, graph_key_hash = self.factory_fixture.registry_fixture.keys[
            "GRAPH_RECORD_VERIFY"
        ]
        body = {
            **dict(request),
            "schemaVersion": "kis-domestic-functional-state-manager-binding/v1",
            "componentReadersHash": request["stateComponentReadersHash"],
            "managerImplementationType": constructors.manager_implementation_type,
            "managerIdHash": hashlib.sha256(b"facade-manager-id").hexdigest(),
            "managerCodeHash": constructors.manager_code_hash,
            "managerProtocolHash": constructors.manager_protocol_hash,
            "managerKeyIdHash": graph_key_hash,
            "managerComponentBindingsHash": hashlib.sha256(
                b"facade-manager-component-bindings"
            ).hexdigest(),
            "mutationPlanHash": hashlib.sha256(b"facade-plan").hexdigest(),
            "ownedProjectionHash": hashlib.sha256(
                b"facade-owned-projection"
            ).hexdigest(),
            "ownedProjectionHeadHash": hashlib.sha256(
                b"facade-owned-projection-head"
            ).hexdigest(),
            "finalMutationBoundarySchema": (
                "kis-domestic-functional-final-reservation/v1"
            ),
            "receiptSchemaVersion": "kis-domestic-functional-manager-receipt/v2",
            "verifyOnly": True,
        }
        digest = _hash(body)
        signed_body = {**body, "bindingHash": digest}
        return {
            **signed_body,
            "signature": self._sign_graph_record(
                "KIS_STATE_MANAGER_BINDING", signed_body
            ),
        }

    def _sign_graph_record(self, domain, body):
        private, _key_hash = self.factory_fixture.registry_fixture.keys[
            "GRAPH_RECORD_VERIFY"
        ]
        return self._registry_sign(private, domain, body)

    def new_facade(self):
        return DurableKisDomesticFunctionalAppFacade(
            state=self.state,
            owner=self.owner,
            factory=self.factory,
            high_water_path=self.high_water_path,
        )

    def cleanup(self):
        import tests.test_kis_domestic_functional_owner as owner_test_module

        owner_test_module.NOW = self.original_owner_test_now
        try:
            if hasattr(self, "facade") and not self.facade._closed:
                self.facade.close()
        except Exception:
            pass
        self.owner_fixture.coordinator.simulate_process_death()
        self.factory_fixture.cleanup()
        self.owner_fixture.cleanup()
        try:
            self.temp.cleanup()
        except PermissionError:
            pass

    @staticmethod
    def intent(operation, claim="facade-claim-0001"):
        return {
            "operation": operation,
            "claimId": claim,
            "ownedOrderKey": (
                {
                    "orderDate": "20260814",
                    "organizationNo": "0001",
                    "orderNo": "12345678",
                }
                if operation != "CLEANUP_SELL"
                else {"orderDate": "", "organizationNo": "", "orderNo": ""}
            ),
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "endpoint": "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            "payloadHash": hashlib.sha256(claim.encode()).hexdigest(),
        }

    def install_active_control(self, *, kind="STOP", intent=None):
        intent = intent or self.intent("CLEANUP_CANCEL")
        self.mutation_intent = dict(intent)
        reservation = self.state._reserve(
            kind=kind,
            allowed_phases={"IDLE"},
            pending_session="facade-session-0001",
        )
        self.session = reservation["sessionId"]
        return intent


class _IntegratedFixture(_Fixture):
    def __init__(self):
        super().__init__()
        self.facade.close()
        self._provision_offline_components()
        self.facade = self.new_integrated_facade(opening_writer=True)

    @staticmethod
    def _time_text(value):
        return value.isoformat().replace("+00:00", "Z")

    def _provision_offline_components(self):
        now = self.owner_fixture.clock.value
        factory_status = self.factory.status()
        self.manager_root = ECC.generate(curve="Ed25519")
        self.manager_key = ECC.generate(curve="Ed25519")
        manager_body = {
            "schemaVersion": MANAGER_MANIFEST_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "registryId": "facade-manager-authority-integration",
            "registryEpoch": 1,
            "previousManifestHash": "0" * 64,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "codeManifestHash": factory_status["codeManifestHash"],
            "rootKeyIdHash": _key_hash(self.manager_root),
            "managerKey": {
                "purpose": MANAGER_KEY_PURPOSE,
                "algorithm": "ED25519",
                "keyIdHash": _key_hash(self.manager_key),
                "publicKeyPem": _public_pem(self.manager_key),
                "rotationEpoch": 1,
                "notBefore": self._time_text(now - timedelta(days=1)),
                "notAfter": self._time_text(now + timedelta(days=1)),
                "signatureDomains": [
                    "KIS_STATE_MANAGER_BINDING",
                    "KIS_MANAGER_RECEIPT",
                    "KIS_FUNCTIONAL_MANAGER_RECEIPT",
                ],
            },
            "issuedAt": self._time_text(now),
            "issuedMonotonicNs": self.owner_fixture.clock.monotonic_ns,
            "productionProvisioned": False,
        }
        manager_envelope = {
            "body": manager_body,
            "manifestHash": _hash(manager_body),
            "rootKeyIdHash": _key_hash(self.manager_root),
            "rootSignature": _ed_sign(
                self.manager_root, MANAGER_ROOT_DOMAIN, manager_body
            ),
        }
        raw = _canonical_bytes(manager_envelope)
        self.manager_manifest_path = (
            Path(self.temp.name) / "manager-authority.json"
        )
        self.manager_manifest_path.write_bytes(raw)
        self.manager_authority = (
            VerifyOnlyKisDomesticFunctionalManagerAuthority(
                self.manager_manifest_path,
                pins=ManagerAuthorityPins(
                    registry_id="facade-manager-authority-integration",
                    registry_epoch=1,
                    manifest_file_hash=hashlib.sha256(raw).hexdigest(),
                    root_public_key_pem=_public_pem(self.manager_root),
                    root_key_id_hash=_key_hash(self.manager_root),
                    manager_key_id_hash=_key_hash(self.manager_key),
                    account_fingerprint=ACCOUNT,
                    credential_configuration_hash=CREDENTIAL,
                    code_manifest_hash=factory_status["codeManifestHash"],
                ),
                trusted_clock=lambda: self.owner_fixture.clock.value,
            )
        )

        self.anchor_root = ECC.generate(curve="Ed25519")
        self.anchor_writer_key = ECC.generate(curve="Ed25519")
        self.anchor_path = Path(self.temp.name) / "external" / "facade.anchor"
        ledger_id = derive_facade_anchor_ledger_id(self.ledger_path)
        anchor_body = {
            "schemaVersion": ANCHOR_INSTALLATION_SCHEMA,
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "pdno": "010140",
            "anchorId": "facade-integration-anchor",
            "anchorPathHash": anchor_path_hash(self.anchor_path.resolve()),
            "facadeLedgerIdHash": ledger_id,
            "rootKeyIdHash": _key_hash(self.anchor_root),
            "writerKeyIdHash": _key_hash(self.anchor_writer_key),
            "writerPublicKeyPem": _public_pem(self.anchor_writer_key),
            "writerPurpose": ANCHOR_WRITER_PURPOSE,
            "writerNotBefore": self._time_text(now - timedelta(days=1)),
            "writerNotAfter": self._time_text(now + timedelta(days=1)),
            "createdAt": self._time_text(now),
            "createdMonotonicNs": self.owner_fixture.clock.monotonic_ns,
            "productionProvisioned": False,
        }
        installation = {
            "body": anchor_body,
            "recordHash": _hash(anchor_body),
            "keyIdHash": _key_hash(self.anchor_root),
            "signature": _ed_sign(
                self.anchor_root, ANCHOR_ROOT_DOMAIN, anchor_body
            ),
        }
        self.anchor_pins = ExternalFacadeAnchorPins(
            anchor_id="facade-integration-anchor",
            facade_ledger_id_hash=ledger_id,
            root_public_key_pem=_public_pem(self.anchor_root),
            root_key_id_hash=_key_hash(self.anchor_root),
            writer_key_id_hash=_key_hash(self.anchor_writer_key),
            minimum_anchor_epoch=0,
            minimum_anchor_head_hash=_hash(installation),
        )
        self.anchor = (
            AppendOnlyKisDomesticFunctionalFacadeAnchor.provision_disabled(
                self.anchor_path,
                pins=self.anchor_pins,
                root_signed_installation=installation,
            )
        )

    def anchor_writer(self, body):
        return {
            "body": dict(body),
            "recordHash": _hash(body),
            "keyIdHash": _key_hash(self.anchor_writer_key),
            "signature": _ed_sign(
                self.anchor_writer_key, ANCHOR_WRITER_DOMAIN, body
            ),
        }

    def new_integrated_facade(
        self,
        *,
        opening_writer=True,
        manager_authority=None,
        anchor=None,
    ):
        return DurableKisDomesticFunctionalAppFacade(
            state=self.state,
            owner=self.owner,
            factory=self.factory,
            high_water_path=self.high_water_path,
            manager_receipt_authority=(
                self.manager_authority
                if manager_authority is None
                else manager_authority
            ),
            independent_monotonic_anchor=(
                self.anchor if anchor is None else anchor
            ),
            opening_anchor_writer=(
                self.anchor_writer if opening_writer else None
            ),
        )

    def cleanup(self):
        try:
            if hasattr(self, "facade") and not self.facade._closed:
                self.facade.close(external_anchor_writer=self.anchor_writer)
        except Exception:
            pass
        try:
            if hasattr(self, "anchor"):
                self.anchor.close()
        except Exception:
            pass
        super().cleanup()


class KisDomesticFunctionalAppFacadeTest(unittest.TestCase):
    def setUp(self):
        self.fixture = _Fixture()
        self.addCleanup(self.fixture.cleanup)

    def test_entrypoint_and_live_status_are_offline_and_unregistered(self):
        entry = production_entrypoint_status()
        status = self.fixture.facade.status()
        for value in (entry, status):
            self.assertFalse(value["productionAvailable"])
            self.assertFalse(value["networkAvailable"])
            self.assertFalse(value["releaseAvailable"])
            self.assertFalse(value["networkOrderPostAllowed"])
            self.assertEqual(0, value["tradingMutationCount"])
        self.assertFalse(status["readerRegistered"])
        self.assertTrue(status["factoryPinnedStateManagerGraph"])
        self.assertTrue(status["singleProcessLifetimeOsLeaseHeld"])

    def test_snapshot_is_signed_fresh_exact_join_and_never_cached(self):
        first = self.fixture.facade.authority_snapshot()
        self.fixture.owner_fixture.clock.advance(0.25)
        second = self.fixture.facade.authority_snapshot()
        self.assertEqual(first["snapshotSequence"] + 1, second["snapshotSequence"])
        self.assertEqual(first["snapshotBodyHash"], second["previousSnapshotHash"])
        self.assertNotEqual(first["snapshotBodyHash"], second["snapshotBodyHash"])
        self.assertEqual(self.fixture.owner.epoch, second["ownerEpoch"])
        self.assertEqual(ACCOUNT, second["functionalAccountFingerprint"])
        self.assertEqual(CREDENTIAL, second["credentialConfigurationHash"])
        self.assertEqual({}, second["controlReservation"])
        self.fixture.facade.verify_snapshot(second)

    def test_snapshot_signature_or_account_tamper_is_rejected(self):
        snapshot = self.fixture.facade.authority_snapshot()
        for changed in (
            {**snapshot, "snapshotSignature": "bad"},
            {**snapshot, "functionalAccountFingerprint": "0" * 64},
        ):
            with self.subTest(keys=set(changed)):
                with self.assertRaises(KisDomesticFunctionalAppFacadeBlocked):
                    self.fixture.facade.verify_snapshot(changed)

    def test_reader_freshness_expires_by_wall_and_monotonic_clock(self):
        snapshot = self.fixture.facade.authority_snapshot()
        self.fixture.owner_fixture.clock.advance(2.1)
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "stale-or-wrong-epoch"
        ):
            self.fixture.facade.verify_snapshot(snapshot)

    def test_snapshot_clock_rollback_fails_before_new_journal_entry(self):
        self.fixture.facade.authority_snapshot()
        before = self.fixture.facade.status()["snapshotSequence"]
        self.fixture.owner_fixture.clock.value = (
            self.fixture.owner_fixture.clock.value.replace(microsecond=0)
        )
        self.fixture.owner_fixture.clock.monotonic_ns -= 1
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "rollback|join-invalid"
        ):
            self.fixture.facade.authority_snapshot()
        self.fixture.owner_fixture.clock.monotonic_ns += 1
        with sqlite3.connect(self.fixture.ledger_path) as conn:
            after = conn.execute(
                "SELECT COUNT(*) FROM kis_functional_app_facade_snapshot"
            ).fetchone()[0]
        self.assertEqual(before, after)

    def test_second_facade_same_owner_epoch_is_cross_process_fail_closed(self):
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "active-epoch-already-owned"
        ):
            self.fixture.new_facade()

    def test_close_waits_for_canonical_route_final_edge(self):
        entered = threading.Event()
        release = threading.Event()
        edge_done = threading.Event()
        close_done = threading.Event()

        def final_edge():
            with kis_route_authority_serialization():
                entered.set()
                release.wait(2)
                edge_done.set()

        def closer():
            self.fixture.facade.close()
            close_done.set()

        edge_thread = threading.Thread(target=final_edge)
        close_thread = threading.Thread(target=closer)
        edge_thread.start()
        self.assertTrue(entered.wait(1))
        close_thread.start()
        self.assertFalse(close_done.wait(0.1))
        release.set()
        edge_thread.join(2)
        close_thread.join(2)
        self.assertTrue(edge_done.is_set())
        self.assertTrue(close_done.is_set())
        self.assertTrue(self.fixture.facade._closed)

    def test_clean_facade_restart_advances_cross_process_facade_epoch(self):
        first_epoch = self.fixture.facade.facade_epoch
        self.fixture.facade.close()
        self.fixture.facade = self.fixture.new_facade()
        self.assertEqual(first_epoch + 1, self.fixture.facade.facade_epoch)
        self.assertEqual(self.fixture.owner.epoch, self.fixture.facade.status()["ownerEpoch"])

    def test_epoch_journal_rewrite_fails_restart(self):
        self.fixture.facade.close()
        with sqlite3.connect(self.fixture.ledger_path) as conn:
            conn.execute(
                "UPDATE kis_functional_app_facade_epoch_transition "
                "SET record_hash=? WHERE sequence_no=1",
                ("0" * 64,),
            )
        with self.assertRaises(KisDomesticFunctionalAppFacadeBlocked):
            self.fixture.new_facade()

    def test_ledger_snapshot_rollback_below_independent_high_water_fails(self):
        self.fixture.facade.authority_snapshot()
        backup = Path(self.fixture.temp.name) / "before.sqlite3"
        shutil.copy2(self.fixture.ledger_path, backup)
        self.fixture.owner_fixture.clock.advance(0.1)
        self.fixture.facade.authority_snapshot()
        self.fixture.facade.close()
        shutil.copy2(backup, self.fixture.ledger_path)
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "rollback-below-high-water"
        ):
            self.fixture.new_facade()

    def test_paired_ledger_and_high_water_replay_is_explicit_production_blocker(self):
        self.fixture.facade.authority_snapshot()
        self.fixture.facade.close()
        old_ledger = Path(self.fixture.temp.name) / "paired-old.sqlite3"
        old_high = Path(self.fixture.temp.name) / "paired-old-high.json"
        shutil.copy2(self.fixture.ledger_path, old_ledger)
        shutil.copy2(self.fixture.high_water_path, old_high)
        self.fixture.facade = self.fixture.new_facade()
        self.fixture.owner_fixture.clock.advance(0.1)
        self.fixture.facade.authority_snapshot()
        self.fixture.facade.close()
        shutil.copy2(old_ledger, self.fixture.ledger_path)
        shutil.copy2(old_high, self.fixture.high_water_path)
        self.fixture.facade = self.fixture.new_facade()
        status = self.fixture.facade.status()
        self.assertFalse(status["productionAvailable"])
        self.assertIn(
            "EXTERNAL_INDEPENDENT_HIGH_WATER_NOT_WIRED",
            status["readinessBlockers"],
        )

    def test_high_water_tamper_or_delete_fails_restart(self):
        for mode in ("tamper", "delete"):
            with self.subTest(mode=mode):
                fixture = _Fixture()
                try:
                    fixture.facade.close()
                    if mode == "tamper":
                        document = json.loads(fixture.high_water_path.read_text())
                        document["snapshotHeadHash"] = "f" * 64
                        fixture.high_water_path.write_text(json.dumps(document) + "\n")
                    else:
                        fixture.high_water_path.unlink()
                    with self.assertRaises(KisDomesticFunctionalAppFacadeBlocked):
                        fixture.new_facade()
                finally:
                    fixture.cleanup()

    def test_active_stop_control_reservation_is_exact_in_signed_snapshot(self):
        self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        control = snapshot["controlReservation"]
        self.assertEqual("STOP", control["reservationKind"])
        self.assertEqual(snapshot["stateRevision"], control["stateRevision"])
        self.assertEqual("CLEANUP", control["phase"])
        self.assertEqual(_hash(control), snapshot["controlReservationHash"])

    def test_exact_cleanup_grant_burn_and_replay_rejected(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        burn = self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="CLEANUP_CANCEL",
            intent=intent,
        )
        self.assertTrue(burn["grantBurnCommitted"])
        self.assertFalse(burn["socketReachable"])
        self.assertEqual(0, burn["tradingMutationCount"])
        self.assertEqual(1, self.fixture.facade.status()["cleanupBurnSequence"])
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "already-burned"
        ):
            self.fixture.facade.burn_cleanup_grant(
                expected_snapshot=snapshot,
                operation="CLEANUP_CANCEL",
                intent=intent,
            )

    def test_cleanup_burn_final_cas_rejects_state_revision_change(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        self.fixture.mutation_intent = self.fixture.intent(
            "CLEANUP_CANCEL", "changed-cleanup-claim"
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "final-cas-changed"
        ):
            self.fixture.facade.burn_cleanup_grant(
                expected_snapshot=snapshot,
                operation="CLEANUP_CANCEL",
                intent=intent,
            )
        self.assertEqual(0, self.fixture.facade.status()["cleanupBurnSequence"])

    def test_cleanup_burn_and_high_water_survive_clean_facade_restart(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        burn = self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="CLEANUP_CANCEL",
            intent=intent,
        )
        first_epoch = self.fixture.facade.facade_epoch
        high_before = json.loads(self.fixture.high_water_path.read_text())
        self.assertEqual(burn["burnRecordHash"], high_before["burnHeadHash"])
        self.fixture.facade.close()
        self.fixture.facade = self.fixture.new_facade()
        status = self.fixture.facade.status()
        self.assertEqual(first_epoch + 1, status["facadeEpoch"])
        self.assertEqual(1, status["cleanupBurnSequence"])
        high_after = json.loads(self.fixture.high_water_path.read_text())
        self.assertEqual(burn["burnRecordHash"], high_after["burnHeadHash"])

    def test_cleanup_burn_requires_state_owned_stop_or_kill_cleanup(self):
        intent = self.fixture.intent("CLEANUP_CANCEL")
        snapshot = self.fixture.facade.authority_snapshot()
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "cleanup-grant-mismatch"
        ):
            self.fixture.facade.burn_cleanup_grant(
                expected_snapshot=snapshot,
                operation="CLEANUP_CANCEL",
                intent=intent,
            )

    def test_kill_grant_is_exact_and_burned_once(self):
        intent = self.fixture.intent("KILL_ORDINARY_CANCEL", "kill-claim-0001")
        self.fixture.kill_allowed = True
        self.fixture.kill_revision = 7
        self.fixture.kill_intent = dict(intent)
        snapshot = self.fixture.facade.authority_snapshot()
        self.assertEqual(7, snapshot["killGrant"]["killRevision"])
        self.assertEqual(_hash(intent), snapshot["killGrant"]["intentHash"])
        burn = self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="KILL_ORDINARY_CANCEL",
            intent=intent,
        )
        self.assertEqual(7, burn["killRevision"])
        self.assertEqual("KILL", burn["reservationKind"])

    def test_burn_row_or_snapshot_row_deletion_fails_closed(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="CLEANUP_CANCEL",
            intent=intent,
        )
        with sqlite3.connect(self.fixture.ledger_path) as conn:
            conn.execute(
                "DELETE FROM kis_functional_app_facade_cleanup_burn"
            )
        with self.assertRaises(KisDomesticFunctionalAppFacadeBlocked):
            self.fixture.facade.status()

    def test_resigned_inconsistent_burn_and_high_water_substitution_fails(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot()
        self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="CLEANUP_CANCEL",
            intent=intent,
        )
        with sqlite3.connect(self.fixture.ledger_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM kis_functional_app_facade_cleanup_burn"
            ).fetchone()
            body = json.loads(row["body_json"])
            body["grantHash"] = "0" * 64
            record_hash = _hash(body)
            signature = self.fixture.facade._sign(
                "KIS_FUNCTIONAL_APP_CLEANUP_BURN", body
            )
            conn.execute(
                "UPDATE kis_functional_app_facade_cleanup_burn SET "
                "grant_hash=?,body_json=?,record_hash=?,signature=?",
                (
                    body["grantHash"],
                    json.dumps(body, sort_keys=True, separators=(",", ":")),
                    record_hash,
                    signature,
                ),
            )
        high = json.loads(self.fixture.high_water_path.read_text())
        high_body = {
            key: value
            for key, value in high.items()
            if key not in {"recordHash", "signature"}
        }
        high_body["revision"] += 1
        high_body["burnHeadHash"] = record_hash
        high_record = {
            **high_body,
            "recordHash": _hash(high_body),
            "signature": self.fixture.facade._sign(
                "KIS_FUNCTIONAL_APP_HIGH_WATER", high_body
            ),
        }
        self.fixture.high_water_path.write_text(
            json.dumps(high_record, sort_keys=True, separators=(",", ":")) + "\n"
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked,
            "burn-snapshot-grant-binding-invalid",
        ):
            self.fixture.facade.status()

    def test_factory_owner_or_state_identity_substitution_is_rejected(self):
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "exact-state-required"
        ):
            DurableKisDomesticFunctionalAppFacade(
                state=object(),
                owner=self.fixture.owner,
                factory=self.fixture.factory,
                high_water_path=self.fixture.high_water_path,
            )
        original = self.fixture.state.authority_snapshot
        with patch.object(
            DurableKisDomesticFunctionalState,
            "authority_snapshot",
            lambda _state: {
                **original(),
                "functionalAccountFingerprint": "0" * 64,
            },
        ):
            with self.assertRaisesRegex(
                KisDomesticFunctionalAppFacadeBlocked, "join-invalid"
            ):
                self.fixture.facade.status()

    def test_state_owner_epoch_substitution_cannot_join_verified_owner(self):
        original = dict(self.fixture.state_owner_epoch)
        self.fixture.state_owner_epoch = {
            **original,
            "ownerEpochId": "kis-substituted-owner-epoch",
            "ownerEpochHash": "0" * 64,
        }
        try:
            with self.assertRaises(KisDomesticFunctionalAppFacadeBlocked):
                self.fixture.facade.status()
        finally:
            self.fixture.state_owner_epoch = original


class KisDomesticFunctionalAppFacadeAuthorityAnchorIntegrationTest(
    unittest.TestCase
):
    def setUp(self):
        self.fixture = _IntegratedFixture()
        self.addCleanup(self.fixture.cleanup)

    def test_offline_integrated_status_separates_capability_from_provisioning(self):
        status = self.fixture.facade.status()
        self.assertTrue(status["dedicatedManagerKeyPurposeAvailable"])
        self.assertTrue(status["independentFacadeMonotonicAnchorAvailable"])
        self.assertTrue(status["managerAuthorityFacadeIntegrated"])
        self.assertTrue(status["externalAnchorFacadeIntegrated"])
        self.assertTrue(status["facadeIntegrationWired"])
        self.assertTrue(status["offlineAuthorityAnchorCompositionIntegrated"])
        self.assertTrue(status["externalAnchorExactJoin"])
        self.assertFalse(status["externalAnchorReconciliationPending"])
        self.assertFalse(
            status["pairedLedgerAndLocalHighWaterRollbackAccepted"]
        )
        self.assertTrue(
            status["pairedLedgerAndLocalHighWaterRollbackDetected"]
        )
        self.assertFalse(status["managerAuthorityProductionProvisioned"])
        self.assertFalse(status["externalAnchorProductionProvisioned"])
        self.assertFalse(
            status["externalAnchorHardwareOrWormMonotonicityProven"]
        )
        self.assertFalse(status["externalAnchorWriterRetainedByFacade"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])

    def test_snapshot_requires_external_writer_and_advances_exact_anchor(self):
        before = self.fixture.anchor.read()["anchorEpoch"]
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked,
            "external-anchor-writer-required",
        ):
            self.fixture.facade.authority_snapshot()
        snapshot = self.fixture.facade.authority_snapshot(
            external_anchor_writer=self.fixture.anchor_writer
        )
        status = self.fixture.facade.status()
        self.assertEqual(1, snapshot["snapshotSequence"])
        self.assertEqual(before + 1, status["externalAnchorEpoch"])
        self.assertTrue(status["externalAnchorExactJoin"])

    def test_external_signer_is_call_scoped_and_not_retained(self):
        self.fixture.facade.authority_snapshot(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.assertFalse(
            any("writer_key" in name for name in vars(self.fixture.facade))
        )
        self.assertFalse(
            any(
                value is self.fixture.anchor_writer
                for value in vars(self.fixture.facade).values()
            )
        )
        self.assertFalse(
            hasattr(self.fixture.manager_authority, "sign")
        )

    def test_external_anchor_signed_body_tamper_fails_without_acceptance(self):
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.fixture.facade = self.fixture.new_integrated_facade(
            opening_writer=False
        )
        body = self.fixture.facade.external_anchor_next_transition_body()
        envelope = self.fixture.anchor_writer(body)
        envelope["body"]["epochHeadHash"] = "f" * 64
        with self.assertRaises(
            (
                KisDomesticFunctionalAppFacadeBlocked,
                KisDomesticFunctionalFacadeAnchorBlocked,
            )
        ):
            self.fixture.facade.accept_external_anchor_transition(envelope)
        status = self.fixture.facade.status()
        self.assertTrue(status["externalAnchorReconciliationPending"])
        self.assertFalse(status["externalAnchorExactJoin"])

    def test_export_and_accept_external_transition_reconciles_pending_open(self):
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.fixture.facade = self.fixture.new_integrated_facade(
            opening_writer=False
        )
        status = self.fixture.facade.status()
        self.assertTrue(status["externalAnchorReconciliationPending"])
        body = self.fixture.facade.external_anchor_next_transition_body()
        receipt = self.fixture.facade.accept_external_anchor_transition(
            self.fixture.anchor_writer(body)
        )
        self.assertTrue(receipt["externalAnchorExactJoin"])
        self.assertFalse(receipt["productionAvailable"])
        self.assertEqual(0, receipt["tradingMutationCount"])
        self.assertTrue(self.fixture.facade.status()["externalAnchorExactJoin"])

    def test_paired_ledger_and_local_highwater_rollback_is_rejected_by_anchor(self):
        self.fixture.facade.authority_snapshot(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        old_ledger = Path(self.fixture.temp.name) / "integrated-old.sqlite3"
        old_high = Path(self.fixture.temp.name) / "integrated-old-high.json"
        shutil.copy2(self.fixture.ledger_path, old_ledger)
        shutil.copy2(self.fixture.high_water_path, old_high)
        self.fixture.facade = self.fixture.new_integrated_facade(
            opening_writer=True
        )
        self.fixture.facade.authority_snapshot(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        shutil.copy2(old_ledger, self.fixture.ledger_path)
        shutil.copy2(old_high, self.fixture.high_water_path)
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked,
            "paired-ledger-high-water-rollback|history-prefix-mismatch|facade-epoch-rollback",
        ):
            self.fixture.new_integrated_facade(opening_writer=False)

    def test_manager_manifest_account_or_code_substitution_fails_join(self):
        authority = self.fixture.manager_authority
        original_status = (
            VerifyOnlyKisDomesticFunctionalManagerAuthority.status
        )
        with patch.object(
            VerifyOnlyKisDomesticFunctionalManagerAuthority,
            "status",
            lambda _authority: {
                **original_status(_authority),
                "managerKeyPurpose": "GRAPH_RECORD_VERIFY",
            },
        ):
            with self.assertRaisesRegex(
                KisDomesticFunctionalAppFacadeBlocked,
                "status-hash-invalid|status-binding-invalid",
            ):
                self.fixture.facade.status()

    def test_exact_concrete_pair_and_ledger_id_are_required(self):
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalAppFacadeBlocked, "pair-required"
        ):
            DurableKisDomesticFunctionalAppFacade(
                state=self.fixture.state,
                owner=self.fixture.owner,
                factory=self.fixture.factory,
                high_water_path=self.fixture.high_water_path,
                manager_receipt_authority=self.fixture.manager_authority,
            )
        self.assertEqual(
            self.fixture.anchor.pins.facade_ledger_id_hash,
            derive_facade_anchor_ledger_id(self.fixture.ledger_path),
        )

    def test_cleanup_burn_advances_anchor_and_remains_socket_zero(self):
        intent = self.fixture.install_active_control()
        snapshot = self.fixture.facade.authority_snapshot(
            external_anchor_writer=self.fixture.anchor_writer
        )
        before = self.fixture.anchor.read()["anchorEpoch"]
        burn = self.fixture.facade.burn_cleanup_grant(
            expected_snapshot=snapshot,
            operation="CLEANUP_CANCEL",
            intent=intent,
            external_anchor_writer=self.fixture.anchor_writer,
        )
        self.assertEqual(before + 1, self.fixture.anchor.read()["anchorEpoch"])
        self.assertTrue(burn["grantBurnCommitted"])
        self.assertFalse(burn["socketReachable"])
        self.assertEqual(0, burn["tradingMutationCount"])

    def test_restart_close_and_anchor_are_one_serialized_offline_lifecycle(self):
        first_epoch = self.fixture.facade.facade_epoch
        before = self.fixture.anchor.read()["anchorEpoch"]
        self.fixture.facade.close(
            external_anchor_writer=self.fixture.anchor_writer
        )
        self.assertEqual(before + 1, self.fixture.anchor.read()["anchorEpoch"])
        self.fixture.facade = self.fixture.new_integrated_facade(
            opening_writer=True
        )
        self.assertEqual(first_epoch + 1, self.fixture.facade.facade_epoch)
        self.assertTrue(self.fixture.facade.status()["externalAnchorExactJoin"])


if __name__ == "__main__":
    unittest.main()
