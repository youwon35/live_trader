from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.kis_domestic_functional_contract import (
    ACTIVE_SECONDS,
    APPROVED_ARTIFACT_CONTENT_HASH,
    APPROVED_ARTIFACT_FILE_SHA256,
    APPROVED_INSTANCE_CONTENT_HASH,
    APPROVED_INSTANCE_FILE_SHA256,
    LIVE_ORIGIN,
    MAX_GROSS_KRW,
    MAX_ORDER_KRW,
    ORDER_QUANTITY,
    OWNER_LOSS_LIMIT_KRW,
    PDNO,
    ROUTE,
)
from live_trader.kis_domestic_functional_graph import (
    CRASH_STAGES,
    KIS_DOMESTIC_FUNCTIONAL_GRAPH_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_GRAPH_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_GRAPH_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_GRAPH_RELEASE_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_GRAPH_STATE_SERVER_WIRED,
    PREALLOCATION_SCHEMA,
    SCHEMA_FINGERPRINT,
    DurableKisDomesticFunctionalGraph,
    DurableKisDomesticFunctionalGraphV2Coordinator,
    FrozenKisDomesticGraphLedgerPort,
    GRAPH_COMPONENT_PROTOCOL_HASH,
    KisDomesticFunctionalGraphBlocked,
    KisDomesticFunctionalGraphInjectedCrash,
    V2_COMPONENTS,
    V2_CRASH_STAGES,
    V2_FROZEN_COMPONENT_FILE_SHA256,
    V2_FROZEN_PROTOCOL_HASHES,
    V2_PORT_RECEIPT_SCHEMA,
    V2_PORT_STATUS_SCHEMA,
    V2_REQUEST_SCHEMA,
    V2_SCHEMA_FINGERPRINT,
    graph_component_status,
)
from live_trader.program_ledger import ProgramLedger
from live_trader.kis_domestic_functional_readers import (
    READERS_COMPONENT_PROTOCOL_HASH,
    READERS_COMPONENT_SCHEMA_FINGERPRINT,
    readers_component_status,
)
from tests.test_kis_domestic_functional_key_registry import (
    _Fixture as _RegistryFixture,
)


KEY = b"kis-graph-offline-test-server-authority-key-0001"
GRANT_AT = datetime(2026, 8, 14, 3, 0, 1, tzinfo=timezone.utc)
MONOTONIC_NS = 991_000_000_000
SESSION = "kis-session-" + "9" * 32
PREALLOCATION = "kis-preallocation-graph-1"
SNAPSHOT = "kis-rolling-snapshot-graph-1"
SOURCE_GENERATION = "kis-ws-generation-" + "1" * 32
PROCESS_GENERATION = "kis-graph-generation-" + "2" * 32


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sign(domain: str, body) -> str:
    return hmac.new(
        KEY,
        domain.encode("ascii") + b"\n" + _canonical(body),
        hashlib.sha256,
    ).hexdigest()


def _verify(domain: str, body, signature: str) -> bool:
    return type(signature) is str and hmac.compare_digest(
        _sign(domain, body), signature
    )


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


class _Clock:
    def __init__(self) -> None:
        self.wall_calls = 0
        self.monotonic_calls = 0

    def wall(self) -> datetime:
        self.wall_calls += 1
        return GRANT_AT

    def monotonic(self) -> int:
        self.monotonic_calls += 1
        return MONOTONIC_NS


def _preallocation_body() -> dict:
    trigger_open = GRANT_AT - timedelta(seconds=1)
    return {
        "schemaVersion": PREALLOCATION_SCHEMA,
        "route": ROUTE,
        "origin": LIVE_ORIGIN,
        "pdno": PDNO,
        "preallocationId": PREALLOCATION,
        "sessionId": SESSION,
        "publicArmId": "kis-public-arm-graph-1",
        "publicArmHash": "1" * 64,
        "bootstrapId": "kis-bootstrap-graph-1",
        "bootstrapHash": "2" * 64,
        "approvalId": "kis-approval-graph-1",
        "approvalHash": "3" * 64,
        "evaluationId": "kis-evaluation-graph-1",
        "evaluationHash": "4" * 64,
        "triggerId": "kis-trigger-graph-1",
        "triggerHash": "5" * 64,
        "triggerBarOpenAt": _iso(trigger_open),
        "triggerObservedAt": _iso(GRANT_AT),
        "sourceGeneration": SOURCE_GENERATION,
        "rollingSnapshotId": SNAPSHOT,
        "rollingSnapshotHash": "6" * 64,
        "rollingReceiptHash": "7" * 64,
        "rollingReceiptSignatureHash": "8" * 64,
        "rollingCompletedAt": _iso(trigger_open - timedelta(seconds=5)),
        "rollingExpiresAt": _iso(GRANT_AT + timedelta(seconds=30)),
        "permitId": "kis-permit-graph-1",
        "permitHash": "9" * 64,
        "sessionNonceHash": "a" * 64,
        "accountFingerprint": "b" * 64,
        "preactivationBaselineHash": "c" * 64,
        "contractEnvelopeHash": "d" * 64,
        "codeManifestHash": "e" * 64,
        "artifactContentHash": APPROVED_ARTIFACT_CONTENT_HASH,
        "artifactFileSha256": APPROVED_ARTIFACT_FILE_SHA256,
        "instanceContentHash": APPROVED_INSTANCE_CONTENT_HASH,
        "instanceFileSha256": APPROVED_INSTANCE_FILE_SHA256,
        "quantity": ORDER_QUANTITY,
        "maxOrderKrw": format(MAX_ORDER_KRW, "f"),
        "maxGrossKrw": format(MAX_GROSS_KRW, "f"),
        "maxGrossSemantics": (
            "OWNER_ENTRY_CAPITAL_AT_RISK_NATURAL_BUY_NOTIONAL_PLUS_ENTRY_COSTS"
        ),
        "ownerLossMustRemainBelowKrw": format(OWNER_LOSS_LIMIT_KRW, "f"),
        "activeSeconds": ACTIVE_SECONDS,
        "preallocatedAt": _iso(GRANT_AT),
        "accountAuthorityAvailable": False,
        "orderAuthorityAvailable": False,
        "networkOrderPostAllowed": False,
        "promotionEligible": False,
    }


def _envelope(body: dict | None = None) -> dict:
    value = _preallocation_body() if body is None else body
    digest = _hash(value)
    return {
        "body": value,
        "recordHash": digest,
        "signature": _sign(
            "GRAPH_PREALLOCATION", {**value, "recordHash": digest}
        ),
    }


class _Fixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = ProgramLedger(Path(self.temp.name) / "program.sqlite3")
        self.clock = _Clock()
        self.graph = DurableKisDomesticFunctionalGraph(
            program_ledger=self.ledger,
            capture_signer=_sign,
            capture_verifier=_verify,
            server_authority_key_id="kis-graph-test-authority-key-id",
            process_generation=PROCESS_GENERATION,
            wall_clock=self.clock.wall,
            monotonic_clock=self.clock.monotonic,
            owner_token_factory=lambda: b"o" * 32,
        )

    def close(self) -> None:
        self.graph.close()
        self.temp.cleanup()


class KisDomesticFunctionalGraphTest(unittest.TestCase):
    def _crash(self, stage: str, *, committed: bool) -> None:
        fixture = _Fixture()
        try:
            fixture.graph.preallocate(_envelope())

            def inject(observed: str) -> None:
                if observed == stage:
                    raise KisDomesticFunctionalGraphInjectedCrash(stage)

            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphInjectedCrash, stage
            ):
                fixture.graph.grant(
                    preallocation_id=PREALLOCATION,
                    failure_injector=inject,
                )
            snapshot = fixture.graph.snapshot(SESSION)
            self.assertFalse(snapshot["authorityOpen"])
            self.assertFalse(snapshot["capabilityAuthorityOpen"])
            if committed:
                self.assertEqual("CONSUMED", snapshot["preallocationState"])
                self.assertEqual("CONSUMED", snapshot["rollingState"])
                self.assertEqual("RECONCILIATION_REQUIRED", snapshot["grantState"])
                self.assertEqual(
                    "RECONCILIATION_REQUIRED", snapshot["capabilityState"]
                )
                self.assertEqual(
                    "RECONCILIATION_REQUIRED", snapshot["heartbeatState"]
                )
            else:
                self.assertEqual("READY", snapshot["preallocationState"])
                self.assertEqual("READY", snapshot["rollingState"])
                self.assertIsNone(snapshot["grantState"])
                self.assertIsNone(snapshot["capabilityState"])
                self.assertIsNone(snapshot["heartbeatState"])
        finally:
            fixture.close()

    def test_atomic_grant_uses_one_instant_and_never_backdates(self) -> None:
        fixture = _Fixture()
        try:
            status = graph_component_status()
            self.assertRegex(SCHEMA_FINGERPRINT, r"^[0-9a-f]{64}$")
            for value in (
                KIS_DOMESTIC_FUNCTIONAL_GRAPH_PRODUCTION_AVAILABLE,
                KIS_DOMESTIC_FUNCTIONAL_GRAPH_NETWORK_AVAILABLE,
                KIS_DOMESTIC_FUNCTIONAL_GRAPH_MUTATION_AVAILABLE,
                KIS_DOMESTIC_FUNCTIONAL_GRAPH_RELEASE_AVAILABLE,
                KIS_DOMESTIC_FUNCTIONAL_GRAPH_STATE_SERVER_WIRED,
                status["networkOrderPostAllowed"],
            ):
                self.assertFalse(value)
            fixture.graph.preallocate(_envelope())
            wall_before = fixture.clock.wall_calls
            mono_before = fixture.clock.monotonic_calls
            snapshot = fixture.graph.grant(preallocation_id=PREALLOCATION)
            self.assertEqual(wall_before + 1, fixture.clock.wall_calls)
            self.assertEqual(mono_before + 1, fixture.clock.monotonic_calls)
            self.assertEqual("ACTIVE", snapshot["grantState"])
            self.assertEqual("ACTIVE", snapshot["capabilityState"])
            self.assertEqual("ACTIVE", snapshot["heartbeatState"])
            self.assertTrue(snapshot["authorityOpen"])
            self.assertTrue(snapshot["capabilityAuthorityOpen"])
            self.assertEqual(_iso(GRANT_AT), snapshot["activatedAt"])
            self.assertEqual(MONOTONIC_NS, snapshot["activatedMonotonicNs"])

            conn = fixture.ledger.connect()
            try:
                activation = json.loads(
                    conn.execute(
                        "SELECT activation_json FROM kis_functional_graph_grant"
                    ).fetchone()[0]
                )
                capability = json.loads(
                    conn.execute(
                        "SELECT capability_json FROM kis_functional_graph_capability"
                    ).fetchone()[0]
                )
                heartbeat = json.loads(
                    conn.execute(
                        "SELECT sample_json FROM kis_functional_graph_heartbeat_start"
                    ).fetchone()[0]
                )
                consumption = json.loads(
                    conn.execute(
                        "SELECT consumption_json FROM kis_functional_graph_rolling_projection"
                    ).fetchone()[0]
                )
            finally:
                conn.close()
            self.assertNotEqual(
                activation["triggerBarOpenAt"], activation["activatedAt"]
            )
            self.assertFalse(activation["backdatedToTriggerBarOpen"])
            self.assertEqual(_iso(GRANT_AT), activation["activatedAt"])
            self.assertEqual(_iso(GRANT_AT), capability["activatedAt"])
            self.assertEqual(_iso(GRANT_AT), heartbeat["wallAt"])
            self.assertEqual(_iso(GRANT_AT), consumption["consumedAt"])
            self.assertEqual(MONOTONIC_NS, activation["activatedMonotonicNs"])
            self.assertEqual(MONOTONIC_NS, capability["activatedMonotonicNs"])
            self.assertEqual(MONOTONIC_NS, heartbeat["monotonicNs"])
            self.assertEqual(activation["capabilityId"], capability["capabilityId"])
            expected = _preallocation_body()
            for name in (
                "publicArmId",
                "publicArmHash",
                "bootstrapId",
                "bootstrapHash",
                "approvalId",
                "approvalHash",
                "evaluationId",
                "evaluationHash",
                "triggerId",
                "triggerHash",
                "sourceGeneration",
                "rollingSnapshotId",
                "rollingSnapshotHash",
                "rollingReceiptHash",
                "rollingReceiptSignatureHash",
                "sessionNonceHash",
                "accountFingerprint",
            ):
                self.assertEqual(expected[name], activation[name], name)
            self.assertEqual(
                expected["rollingReceiptSignatureHash"],
                consumption["rollingReceiptSignatureHash"],
            )
        finally:
            fixture.close()

    def test_crash_after_verify_rolls_back(self) -> None:
        self._crash("AFTER_VERIFY", committed=False)

    def test_crash_after_activation_insert_rolls_back(self) -> None:
        self._crash("AFTER_ACTIVATION_INSERT", committed=False)

    def test_crash_after_rolling_consume_rolls_back(self) -> None:
        self._crash("AFTER_ROLLING_CONSUME", committed=False)

    def test_crash_after_heartbeat_start_rolls_back(self) -> None:
        self._crash("AFTER_HEARTBEAT_START", committed=False)

    def test_crash_before_commit_rolls_back(self) -> None:
        self._crash("BEFORE_COMMIT", committed=False)

    def test_crash_after_commit_is_durably_reconciled(self) -> None:
        self._crash("AFTER_COMMIT", committed=True)

    def test_singleton_blocks_second_owner_and_restart_terminalizes(self) -> None:
        fixture = _Fixture()
        replacement = None
        try:
            fixture.graph.preallocate(_envelope())
            fixture.graph.grant(preallocation_id=PREALLOCATION)
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "singleton-owner-active"
            ):
                DurableKisDomesticFunctionalGraph(
                    program_ledger=fixture.ledger,
                    capture_signer=_sign,
                    capture_verifier=_verify,
                    server_authority_key_id="kis-graph-test-authority-key-id",
                    process_generation="kis-graph-generation-" + "3" * 32,
                    wall_clock=fixture.clock.wall,
                    monotonic_clock=fixture.clock.monotonic,
                    owner_token_factory=lambda: b"x" * 32,
                )
            fixture.graph.close()
            replacement = DurableKisDomesticFunctionalGraph(
                program_ledger=fixture.ledger,
                capture_signer=_sign,
                capture_verifier=_verify,
                server_authority_key_id="kis-graph-test-authority-key-id",
                process_generation="kis-graph-generation-" + "4" * 32,
                wall_clock=fixture.clock.wall,
                monotonic_clock=fixture.clock.monotonic,
                owner_token_factory=lambda: b"y" * 32,
            )
            self.assertEqual((SESSION,), replacement.startup_reconciled_session_ids)
            snapshot = replacement.snapshot(SESSION)
            self.assertFalse(snapshot["authorityOpen"])
            self.assertFalse(snapshot["capabilityAuthorityOpen"])
            self.assertEqual("RECONCILIATION_REQUIRED", snapshot["grantState"])
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "owner-closed"
            ):
                fixture.graph.snapshot(SESSION)
        finally:
            if replacement is not None:
                replacement.close()
            fixture.graph.close()
            fixture.temp.cleanup()

    def test_exact_join_and_dirty_schema_fail_closed_without_authority(self) -> None:
        fixture = _Fixture()
        try:
            body = _preallocation_body()
            body["maxGrossSemantics"] = "TURNOVER"
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "contract-mismatch"
            ):
                fixture.graph.preallocate(_envelope(body))
            fixture.graph.preallocate(_envelope())
            conn = fixture.ledger.connect()
            try:
                conn.execute(
                    "UPDATE kis_functional_graph_rolling_projection "
                    "SET rolling_snapshot_hash=?",
                    ("0" * 64,),
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "rolling-row-mismatch"
            ):
                fixture.graph.grant(preallocation_id=PREALLOCATION)
            snapshot = fixture.graph.snapshot(SESSION)
            self.assertFalse(snapshot["authorityOpen"])
            self.assertIsNone(snapshot["grantState"])
            fixture.graph.close()
            conn = sqlite3.connect(fixture.ledger.path)
            try:
                conn.execute(
                    "ALTER TABLE kis_functional_graph_grant "
                    "ADD COLUMN hostile TEXT"
                )
                conn.commit()
            finally:
                conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "schema-fingerprint-dirty"
            ):
                DurableKisDomesticFunctionalGraph(
                    program_ledger=fixture.ledger,
                    capture_signer=_sign,
                    capture_verifier=_verify,
                    server_authority_key_id="kis-graph-test-authority-key-id",
                    process_generation="kis-graph-generation-" + "5" * 32,
                    wall_clock=fixture.clock.wall,
                    monotonic_clock=fixture.clock.monotonic,
                    owner_token_factory=lambda: b"z" * 32,
                )
        finally:
            fixture.graph.close()
            fixture.temp.cleanup()

    def test_crash_stage_protocol_is_exact(self) -> None:
        self.assertEqual(
            (
                "AFTER_VERIFY",
                "AFTER_ACTIVATION_INSERT",
                "AFTER_ROLLING_CONSUME",
                "AFTER_HEARTBEAT_START",
                "BEFORE_COMMIT",
                "AFTER_COMMIT",
            ),
            CRASH_STAGES,
        )


V2_OPERATION = "kis-graph-v2-operation-one"
V2_OWNER_EPOCH_ID = "kis-owner-epoch-v2-one"
V2_OWNER_EPOCH_HASH = "1" * 64
V2_REGISTRY_MANIFEST = "2" * 64
V2_REGISTRY_HEAD = "3" * 64
V2_REGISTRY_BINDING = "4" * 64


class _V2Clock:
    def __init__(self) -> None:
        self.wall_calls = 0
        self.monotonic_calls = 0

    def wall(self) -> datetime:
        self.wall_calls += 1
        return GRANT_AT

    def monotonic(self) -> int:
        self.monotonic_calls += 1
        return MONOTONIC_NS


def _v2_request(
    *, operation_id: str = V2_OPERATION, registry_binding: dict | None = None
) -> dict:
    body = {
        "schemaVersion": V2_REQUEST_SCHEMA,
        "operationId": operation_id,
        "preallocationId": PREALLOCATION,
        "sessionId": SESSION,
        "publicArmId": "kis-public-arm-graph-1",
        "publicArmHash": "5" * 64,
        "bootstrapId": "kis-bootstrap-graph-1",
        "bootstrapHash": "6" * 64,
        "approvalId": "kis-approval-graph-1",
        "approvalHash": "7" * 64,
        "evaluationId": "kis-evaluation-graph-1",
        "evaluationHash": "8" * 64,
        "triggerId": "kis-trigger-graph-1",
        "triggerHash": "9" * 64,
        "sourceObservationId": "kis-source-observation-graph-1",
        "sourceGeneration": SOURCE_GENERATION,
        "rollingSnapshotId": SNAPSHOT,
        "rollingSnapshotHash": "a" * 64,
        "rollingTriggerEnvelopeHash": "b" * 64,
        "quoteReceiptId": "kis-quote-" + "c" * 32,
        "quoteReceiptHash": "c" * 64,
        "capabilityId": "kis-capability-graph-v2-one",
        "permitId": "kis-permit-graph-1",
        "permitHash": "d" * 64,
        "sessionNonceHash": "e" * 64,
        "accountFingerprint": "f" * 64,
        "credentialConfigurationHash": "0" * 64,
        "preactivationBaselineHash": "1" * 64,
        "contractEnvelopeHash": "2" * 64,
        "codeManifestHash": "3" * 64,
        "ownerEpoch": 7,
        "ownerEpochId": V2_OWNER_EPOCH_ID,
        "ownerEpochHash": V2_OWNER_EPOCH_HASH,
        "registryEpoch": 11,
        "registryManifestHash": V2_REGISTRY_MANIFEST,
        "registryAcceptedHeadHash": V2_REGISTRY_HEAD,
        "registryAcceptanceRevision": 4,
        "registryGraphBindingHash": V2_REGISTRY_BINDING,
        "registryId": "test-registry-placeholder",
        "registryManifestFileHash": "5" * 64,
        "registryRootKeyIdHash": "6" * 64,
        "registryFactoryBindingHash": "7" * 64,
        "registryClockGeneration": "test-process-generation-0001",
        "processGeneration": PROCESS_GENERATION,
        "socketGeneration": SOURCE_GENERATION,
        "requestedAt": _iso(GRANT_AT),
        "freshQuoteHash": "4" * 64,
        "freshQuoteObservedAt": _iso(GRANT_AT),
        "freshQuotePriceKrw": "100",
        "naturalBuyLimitPriceKrw": "100",
        "productionAvailable": False,
    }
    if registry_binding is not None:
        body.update(
            {
                "registryId": registry_binding["registryId"],
                "registryEpoch": registry_binding["registryEpoch"],
                "registryManifestHash": registry_binding["manifestHash"],
                "registryManifestFileHash": registry_binding[
                    "manifestFileHash"
                ],
                "registryRootKeyIdHash": registry_binding["rootKeyIdHash"],
                "registryAcceptedHeadHash": registry_binding[
                    "acceptedManifestHeadHash"
                ],
                "registryAcceptanceRevision": registry_binding[
                    "acceptanceRevision"
                ],
                "registryFactoryBindingHash": registry_binding[
                    "factoryBindingHash"
                ],
                "registryGraphBindingHash": registry_binding[
                    "graphBindingHash"
                ],
                "registryClockGeneration": registry_binding[
                    "clockGeneration"
                ],
                "accountFingerprint": registry_binding[
                    "accountFingerprint"
                ],
                "credentialConfigurationHash": registry_binding[
                    "credentialConfigurationHash"
                ],
                "codeManifestHash": registry_binding["codeManifestHash"],
            }
        )
    action_material = {
        "schemaVersion": "kis-domestic-functional-graph-v2-action-inputs/v1",
        **{key: body[key] for key in sorted(body)},
    }
    body["actionInputsHash"] = _hash(action_material)
    return {**body, "requestHash": _hash(body)}


class _V2Harness:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = ProgramLedger(Path(self.temp.name) / "program.sqlite3")
        self.clock = _V2Clock()
        self.calls: list[tuple[str, str]] = []
        self.compensations: list[tuple[str, str]] = []
        self.fail_component = ""
        self.fail_compensation = ""
        self.backdate_lane = False
        self.blocked_component = ""
        self.receipt_verification_enabled = True
        self.receipt_overrides: dict[str, object] = {}
        self.tamper_lane_grant = False
        self.lane_activation_arguments: dict | None = None
        self.registry_fixture = _RegistryFixture()
        readers_binding = self.registry_fixture.manifest[
            "componentBindings"
        ][0]
        readers_binding.update(
            {
                "sourceFileHash": hashlib.sha256(
                    (
                        Path(__file__).resolve().parents[1]
                        / "live_trader"
                        / "kis_domestic_functional_readers.py"
                    ).read_bytes()
                ).hexdigest(),
                "protocolHash": READERS_COMPONENT_PROTOCOL_HASH,
                "schemaFingerprint": READERS_COMPONENT_SCHEMA_FINGERPRINT,
                "statusHash": readers_component_status()["statusHash"],
            }
        )
        self.registry_fixture.write()
        self.registry = self.registry_fixture.production_registry(
            graph_file_hash=hashlib.sha256(
                (
                    Path(__file__).resolve().parents[1]
                    / "live_trader"
                    / "kis_domestic_functional_graph.py"
                ).read_bytes()
            ).hexdigest(),
            graph_protocol_hash=GRAPH_COMPONENT_PROTOCOL_HASH,
            graph_schema_fingerprint=V2_SCHEMA_FINGERPRINT,
        )
        self.registry_binding = self.registry.component_binding("readers")
        self.status_revisions = {name: 1 for name in V2_COMPONENTS}
        self.ports = {name: self._port(name) for name in V2_COMPONENTS}
        self.coordinator = DurableKisDomesticFunctionalGraphV2Coordinator(
            program_ledger=self.ledger,
            ports=self.ports,
            wall_clock=self.clock.wall,
            monotonic_clock=self.clock.monotonic,
            lane_grant_authority_key=KEY,
            lane_grant_authority_key_id="test-kis-lane-key-v1",
            key_registry=self.registry,
        )

    def close(self) -> None:
        self.registry_fixture.cleanup()
        self.temp.cleanup()

    def request(self, *, operation_id: str = V2_OPERATION) -> dict:
        return _v2_request(
            operation_id=operation_id,
            registry_binding=self.registry_binding,
        )

    def _status(self, component: str, request: dict) -> dict:
        blockers = (
            ["NATIVE_GRANT_INSTANT_NOT_ACCEPTED"]
            if self.blocked_component == component
            else []
        )
        body = {
            "schemaVersion": V2_PORT_STATUS_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "component": component,
            "implementationType": f"offline.{component}.frozen-ledger-port/v1",
            "codeHash": self.ports[component].code_hash,
            "protocolHash": V2_FROZEN_PROTOCOL_HASHES[component],
            "statusRevision": self.status_revisions[component],
            "statusHeadHash": hashlib.sha256(
                f"{component}:{self.status_revisions[component]}".encode()
            ).hexdigest(),
            "sessionId": request["sessionId"],
            "accountFingerprint": request["accountFingerprint"],
            "credentialConfigurationHash": request["credentialConfigurationHash"],
            "ownerEpoch": request["ownerEpoch"],
            "ownerEpochId": request["ownerEpochId"],
            "ownerEpochHash": request["ownerEpochHash"],
            "registryEpoch": request["registryEpoch"],
            "registryManifestHash": request["registryManifestHash"],
            "registryAcceptedHeadHash": request["registryAcceptedHeadHash"],
            "registryAcceptanceRevision": request["registryAcceptanceRevision"],
            "registryGraphBindingHash": request["registryGraphBindingHash"],
            "registryId": request["registryId"],
            "registryManifestFileHash": request["registryManifestFileHash"],
            "registryRootKeyIdHash": request["registryRootKeyIdHash"],
            "registryFactoryBindingHash": request["registryFactoryBindingHash"],
            "registryClockGeneration": request["registryClockGeneration"],
            "preflightReady": not blockers,
            "readinessBlockers": blockers,
            "exactCasAvailable": True,
            "verifyOnly": True,
            "nativeGrantInstantAccepted": not blockers,
            "offlineSimulation": True,
            "productionAvailable": False,
            "networkAvailable": False,
            "mutationAvailable": False,
            "releaseAvailable": False,
        }
        return {**body, "statusHash": _hash(body)}

    @staticmethod
    def _status_verify(value) -> bool:
        body = dict(value)
        digest = body.pop("statusHash", "")
        return hmac.compare_digest(digest, _hash(body))

    def _receipt_verify(self, value) -> bool:
        return bool(
            self.receipt_verification_enabled
            and hmac.compare_digest(
            value["authoritativeReceiptHash"], _hash(value["result"])
            )
        )

    def _cas(self, component: str, action: str, request, instant, expectation) -> dict:
        self.calls.append((component, action))
        if self.fail_component == component:
            raise RuntimeError("injected CAS uncertainty")
        activated_at = instant["wallAt"]
        if component == "lane" and self.backdate_lane:
            activated_at = _iso(GRANT_AT - timedelta(seconds=1))
        if component == "lane":
            arguments = dict(instant["laneActivationArguments"])
            self.lane_activation_arguments = arguments
            lane_receipt = {
                **arguments["graph_grant_instant_receipt"]
            }
            if self.tamper_lane_grant:
                lane_receipt["signature"] = "0" * 64
            activated_dt = datetime.fromisoformat(
                activated_at.replace("Z", "+00:00")
            )
            result = {
                "schemaVersion": "kis-domestic-functional-activation/v2",
                "sessionId": request["sessionId"],
                "state": "ACTIVE",
                "activatedAt": activated_at,
                "activationObservedAt": activated_at,
                "grantMonotonicNs": instant["monotonicNs"],
                "grantReceiptHash": lane_receipt["recordHash"],
                "expiresAt": _iso(activated_dt + timedelta(seconds=ACTIVE_SECONDS)),
                "activeSeconds": ACTIVE_SECONDS,
                "evaluationId": request["evaluationId"],
                "triggerId": request["triggerId"],
                "activationRecordHash": "5" * 64,
                "naturalBuyClaimId": "kis-claim-" + "6" * 32,
                "naturalBuyClaimHash": "7" * 64,
                "realOrdersEnabled": False,
                "promotionEligible": False,
                "laneGrantInstantReceipt": lane_receipt,
            }
        else:
            result = {
                "state": "ACTIVE_OFFLINE_SIMULATION",
                "activatedAt": activated_at,
                "grantMonotonicNs": instant["monotonicNs"],
                "component": component,
            }
        receipt = {
            "schemaVersion": V2_PORT_RECEIPT_SCHEMA,
            "component": component,
            "action": action,
            "outcome": "SUCCEEDED",
            "result": result,
            "authoritativeReceiptHash": _hash(result),
            "singleUseBurned": component in {"rolling", "quote"},
            "exactCas": True,
            "operationId": request["operationId"],
            "sessionId": request["sessionId"],
            "requestHash": request["requestHash"],
            "actionInputsHash": request["actionInputsHash"],
            "grantWallAt": instant["wallAt"],
            "grantMonotonicNs": instant["monotonicNs"],
            "expectedStatusRevision": expectation["expectedStatusRevision"],
            "expectedStatusHeadHash": expectation["expectedStatusHeadHash"],
            "intentStepHash": expectation["intentStepHash"],
            "resultRevision": expectation["expectedStatusRevision"] + 1,
            "resultHeadHash": hashlib.sha256(
                (component + ":result:" + action).encode()
            ).hexdigest(),
            "productionAvailable": False,
        }
        receipt.update(self.receipt_overrides)
        return receipt

    def _compensate(self, component: str, action: str, request, instant, expectation) -> dict:
        self.compensations.append((component, action))
        if self.fail_compensation == component:
            raise RuntimeError("injected compensation failure")
        result = {
            "state": "RECONCILIATION_LATCHED_OFFLINE",
            "activatedAt": instant["wallAt"],
            "component": component,
        }
        return {
            "schemaVersion": V2_PORT_RECEIPT_SCHEMA,
            "component": component,
            "action": action,
            "outcome": "COMPENSATED",
            "result": result,
            "authoritativeReceiptHash": _hash(result),
            "singleUseBurned": False,
            "exactCas": True,
            "operationId": request["operationId"],
            "sessionId": request["sessionId"],
            "requestHash": request["requestHash"],
            "actionInputsHash": request["actionInputsHash"],
            "grantWallAt": instant["wallAt"],
            "grantMonotonicNs": instant["monotonicNs"],
            "expectedStatusRevision": expectation["expectedStatusRevision"],
            "expectedStatusHeadHash": expectation["expectedStatusHeadHash"],
            "intentStepHash": expectation["intentStepHash"],
            "resultRevision": expectation["expectedStatusRevision"] + 1,
            "resultHeadHash": hashlib.sha256(
                (component + ":compensated:" + action).encode()
            ).hexdigest(),
            "productionAvailable": False,
        }

    def _port(self, component: str) -> FrozenKisDomesticGraphLedgerPort:
        code_hash = (
            self.registry_binding["componentBinding"]["sourceFileHash"]
            if component == "readers"
            else V2_FROZEN_COMPONENT_FILE_SHA256[component]
        )
        return FrozenKisDomesticGraphLedgerPort(
            component=component,
            implementation_type=f"offline.{component}.frozen-ledger-port/v1",
            code_hash=code_hash,
            protocol_hash=V2_FROZEN_PROTOCOL_HASHES[component],
            status_reader=lambda request, component=component: self._status(
                component, dict(request)
            ),
            status_verifier=self._status_verify,
            cas_runner=lambda action, request, instant, expectation, component=component: self._cas(
                component, action, request, instant, expectation
            ),
            receipt_verifier=self._receipt_verify,
            compensator=lambda action, request, instant, expectation, component=component: self._compensate(
                component, action, request, instant, expectation
            ),
            offline_simulation=True,
            key_registry=self.registry if component == "readers" else None,
        )


class KisDomesticFunctionalGraphV2Test(unittest.TestCase):
    def test_all_five_cas_use_one_grant_monotonic_and_exact_union(self) -> None:
        fixture = _V2Harness()
        try:
            status = graph_component_status()
            self.assertTrue(status["v2CoordinatorAvailableOffline"])
            self.assertFalse(status["crossLedgerAtomicityAvailable"])
            self.assertTrue(status["nativeLaneGrantInstantAvailable"])
            self.assertTrue(status["laneGrantReceiptIssuedAndReverifiedOffline"])
            self.assertFalse(
                status["laneGrantReceiptProductionAuthorityAvailable"]
            )
            self.assertFalse(status["registryGraphBindingWired"])
            self.assertFalse(status["productionV2PreflightAllowed"])
            self.assertTrue(status["durableIntentBeforeEveryPortCas"])
            self.assertTrue(status["portReceiptExactGrantAndHeadBinding"])
            self.assertTrue(status["accountWideUnresolvedHazardBlocksNewSession"])
            self.assertTrue(status["unionReplaysReceiptVerifiers"])
            self.assertTrue(status["actionInputsHashDerivedByConsumer"])
            result = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("ACTIVE_OFFLINE_SIMULATION", result["state"])
            self.assertEqual(10, result["stepCount"])
            self.assertTrue(result["burnedRolling"])
            self.assertTrue(result["burnedQuote"])
            self.assertTrue(result["laneActive"])
            self.assertTrue(result["heartbeatActive"])
            self.assertTrue(result["capabilityActive"])
            self.assertFalse(result["crossLedgerAtomicityClaimed"])
            self.assertFalse(result["productionAvailable"])
            self.assertEqual(1, fixture.clock.monotonic_calls)
            lane_arguments = fixture.lane_activation_arguments
            self.assertIsNotNone(lane_arguments)
            self.assertEqual(
                {
                    "bootstrap_id", "approval_id", "evaluation_id", "trigger_id",
                    "session_id", "fresh_quote_hash", "fresh_quote_observed_at",
                    "fresh_quote_price_krw", "natural_buy_limit_price_krw",
                    "graph_grant_instant_receipt",
                },
                set(lane_arguments),
            )
            grant = lane_arguments["graph_grant_instant_receipt"]["body"]
            self.assertEqual(fixture.request()["requestHash"], grant["graphRequestHash"])
            self.assertEqual(_iso(GRANT_AT), grant["grantWallAt"])
            self.assertEqual(MONOTONIC_NS, grant["grantMonotonicNs"])
            self.assertEqual(
                [
                    ("rolling", "CONSUME"),
                    ("quote", "CONSUME"),
                    ("lane", "ACTIVATE"),
                    ("heartbeat", "START"),
                    ("capability", "MINT"),
                ],
                fixture.calls,
            )
        finally:
            fixture.close()

    def test_crash_after_each_cas_compensates_and_burns_without_retry(self) -> None:
        stages = (
            "AFTER_ROLLING_CAS",
            "AFTER_QUOTE_CAS",
            "AFTER_LANE_CAS",
            "AFTER_HEARTBEAT_CAS",
            "AFTER_CAPABILITY_CAS",
        )
        for index, stage in enumerate(stages, 1):
            with self.subTest(stage=stage):
                fixture = _V2Harness()
                try:
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalGraphInjectedCrash, stage
                    ):
                        fixture.coordinator.execute(
                            request=fixture.request(),
                            failure_injector=lambda observed, stage=stage: (
                                (_ for _ in ()).throw(
                                    KisDomesticFunctionalGraphInjectedCrash(stage)
                                )
                                if observed == stage else None
                            ),
                        )
                    result = fixture.coordinator.snapshot(V2_OPERATION)
                    self.assertEqual("SAFE_INCOMPLETE", result["state"])
                    self.assertFalse(result["laneActive"])
                    self.assertFalse(result["heartbeatActive"])
                    self.assertFalse(result["capabilityActive"])
                    self.assertFalse(result["sameOneUseRetryAllowed"])
                    self.assertEqual(index >= 1, result["burnedRolling"])
                    self.assertEqual(index >= 2, result["burnedQuote"])
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalGraphBlocked, "already-burned"
                    ):
                        fixture.coordinator.execute(request=fixture.request())
                finally:
                    fixture.close()

    def test_uncertain_lane_cas_is_compensated_after_one_use_receipts_burn(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.fail_component = "lane"
            result = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("SAFE_INCOMPLETE", result["state"])
            self.assertTrue(result["burnedRolling"])
            self.assertTrue(result["burnedQuote"])
            self.assertFalse(result["laneActive"])
            self.assertIn(("lane", "BEGIN_CLEANUP"), fixture.compensations)
        finally:
            fixture.close()

    def test_compensation_failure_is_sticky_reconciliation(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.fail_component = "heartbeat"
            fixture.fail_compensation = "heartbeat"
            result = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
            self.assertTrue(result["heartbeatActive"])
            self.assertTrue(result["hazardousAuthorityOpen"])
            self.assertFalse(result["allOrFailClosedCompensationVerified"])
        finally:
            fixture.close()

    def test_backdated_lane_activation_is_cleanup_only(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.backdate_lane = True
            result = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("SAFE_INCOMPLETE", result["state"])
            self.assertTrue(result["burnedRolling"])
            self.assertTrue(result["burnedQuote"])
            self.assertFalse(result["laneActive"])
            self.assertIn(("lane", "BEGIN_CLEANUP"), fixture.compensations)
        finally:
            fixture.close()

    def test_lane_grant_signature_is_reverified_before_active_projection(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.tamper_lane_grant = True
            result = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("SAFE_INCOMPLETE", result["state"])
            self.assertTrue(result["burnedRolling"])
            self.assertTrue(result["burnedQuote"])
            self.assertFalse(result["laneActive"])
            self.assertIn(("lane", "BEGIN_CLEANUP"), fixture.compensations)
            self.assertFalse(result["sameOneUseRetryAllowed"])
        finally:
            fixture.close()

    def test_frozen_component_preflight_hold_is_post_zero(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.blocked_component = "lane"
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "lane-preflight-hold"
            ):
                fixture.coordinator.execute(request=fixture.request())
            self.assertEqual([], fixture.calls)
            with fixture.ledger.connection() as conn:
                count = conn.execute(
                    "SELECT COUNT(*) FROM kis_functional_v2_graph_operation"
                ).fetchone()[0]
            self.assertEqual(0, count)
        finally:
            fixture.close()

    def test_request_port_pin_and_status_tamper_fail_before_cas(self) -> None:
        fixture = _V2Harness()
        try:
            request = fixture.request()
            request["ownerEpochHash"] = "0" * 64
            with self.assertRaisesRegex(KisDomesticFunctionalGraphBlocked, "not-derived"):
                fixture.coordinator.execute(request=request)
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "code-pin-mismatch"
            ):
                FrozenKisDomesticGraphLedgerPort(
                    component="owner",
                    implementation_type="offline.owner.frozen-ledger-port/v1",
                    code_hash="0" * 64,
                    protocol_hash=V2_FROZEN_PROTOCOL_HASHES["owner"],
                    status_reader=lambda _request: {},
                    status_verifier=lambda _value: True,
                    cas_runner=lambda *_args: {},
                    receipt_verifier=lambda _value: True,
                    offline_simulation=True,
                )
            self.assertEqual([], fixture.calls)
        finally:
            fixture.close()

    def test_readers_registry_entry_is_rechecked_at_final_fence(self) -> None:
        cases = (
            ("protocolHash", "0" * 64, "binding-not-current-or-pinned"),
            ("schemaFingerprint", "1" * 64, "binding-not-current-or-pinned"),
            ("statusHash", "2" * 64, "binding-not-current-or-pinned"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                fixture = _V2Harness()
                try:
                    fixture.registry._component_bindings["readers"][field] = value
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalGraphBlocked, message
                    ):
                        fixture.coordinator.execute(request=fixture.request())
                    self.assertEqual([], fixture.calls)
                finally:
                    fixture.close()

        fixture = _V2Harness()
        try:
            fixture.registry._component_bindings.pop("readers")
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked,
                "binding-unavailable",
            ):
                fixture.coordinator.execute(request=fixture.request())
            self.assertEqual([], fixture.calls)
        finally:
            fixture.close()

        fixture = _V2Harness()
        try:
            fixture.registry._keys["READERS_COMPONENT_VERIFY"][0]["revoked"] = True
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked,
                "binding-unavailable",
            ):
                fixture.coordinator.execute(request=fixture.request())
            self.assertEqual([], fixture.calls)
        finally:
            fixture.close()

    def test_union_verifier_rejects_result_and_chain_tamper(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.coordinator.execute(request=fixture.request())
            with fixture.ledger.connection() as conn:
                conn.execute(
                    "UPDATE kis_functional_v2_graph_step SET result_json=? "
                    "WHERE operation_id=? AND ordinal=1",
                    ("{}", V2_OPERATION),
                )
                conn.commit()
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "step-chain-dirty"
            ):
                fixture.coordinator.verify_union(V2_OPERATION)
        finally:
            fixture.close()

    def test_startup_owner_loss_marks_open_projection_reconciliation(self) -> None:
        fixture = _V2Harness()
        replacement = None
        try:
            fixture.coordinator.execute(request=fixture.request())
            with fixture.ledger.connection() as conn:
                conn.execute(
                    "UPDATE kis_functional_v2_graph_operation SET state='APPLYING' "
                    "WHERE operation_id=?", (V2_OPERATION,),
                )
                conn.commit()
            replacement = DurableKisDomesticFunctionalGraphV2Coordinator(
                program_ledger=fixture.ledger,
                ports=fixture.ports,
                wall_clock=fixture.clock.wall,
                monotonic_clock=fixture.clock.monotonic,
                lane_grant_authority_key=KEY,
                lane_grant_authority_key_id="test-kis-lane-key-v1",
                key_registry=fixture.registry,
            )
            self.assertEqual((V2_OPERATION,), replacement.startup_orphaned_operations)
            result = replacement.snapshot(V2_OPERATION)
            self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
            self.assertTrue(result["hazardousAuthorityOpen"])
        finally:
            fixture.close()

    def test_v2_dirty_schema_fails_closed(self) -> None:
        fixture = _V2Harness()
        try:
            self.assertRegex(V2_SCHEMA_FINGERPRINT, r"^[0-9a-f]{64}$")
            with fixture.ledger.connection() as conn:
                conn.execute(
                    "ALTER TABLE kis_functional_v2_graph_operation "
                    "ADD COLUMN hostile TEXT"
                )
                conn.commit()
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked, "v2-graph-schema-dirty"
            ):
                DurableKisDomesticFunctionalGraphV2Coordinator(
                    program_ledger=fixture.ledger,
                    ports=fixture.ports,
                    wall_clock=fixture.clock.wall,
                    monotonic_clock=fixture.clock.monotonic,
                    lane_grant_authority_key=KEY,
                    lane_grant_authority_key_id="test-kis-lane-key-v1",
                    key_registry=fixture.registry,
                )
        finally:
            fixture.close()

    def test_receipt_exact_request_session_grant_and_head_bindings_reject(self) -> None:
        fields = {
            "operationId": "wrong-operation",
            "sessionId": "wrong-session",
            "requestHash": "f" * 64,
            "grantWallAt": _iso(GRANT_AT + timedelta(seconds=1)),
            "grantMonotonicNs": MONOTONIC_NS + 1,
            "expectedStatusRevision": 999,
            "expectedStatusHeadHash": "e" * 64,
            "intentStepHash": "d" * 64,
        }
        for field, value in fields.items():
            with self.subTest(field=field):
                fixture = _V2Harness()
                try:
                    fixture.receipt_overrides = {field: value}
                    result = fixture.coordinator.execute(request=fixture.request())
                    self.assertEqual("SAFE_INCOMPLETE", result["state"])
                    self.assertTrue(result["burnedRolling"])
                    self.assertEqual(1, len(fixture.calls))
                finally:
                    fixture.close()

    def test_crash_after_port_return_leaves_hazard_and_restart_orphans(self) -> None:
        fixture = _V2Harness()
        try:
            stage = "AFTER_ROLLING_PORT_RETURN_BEFORE_RECORD"
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphInjectedCrash, stage
            ):
                fixture.coordinator.execute(
                    request=fixture.request(),
                    failure_injector=lambda observed: (
                        (_ for _ in ()).throw(
                            KisDomesticFunctionalGraphInjectedCrash(stage)
                        )
                        if observed == stage else None
                    ),
                )
            with fixture.ledger.connection() as conn:
                row = conn.execute(
                    "SELECT state,burned_rolling,hazardous_authority_open "
                    "FROM kis_functional_v2_graph_operation WHERE operation_id=?",
                    (V2_OPERATION,),
                ).fetchone()
                self.assertEqual("APPLYING", row["state"])
                self.assertEqual(1, row["burned_rolling"])
                self.assertEqual(1, row["hazardous_authority_open"])
            replacement = DurableKisDomesticFunctionalGraphV2Coordinator(
                program_ledger=fixture.ledger,
                ports=fixture.ports,
                wall_clock=fixture.clock.wall,
                monotonic_clock=fixture.clock.monotonic,
                lane_grant_authority_key=KEY,
                lane_grant_authority_key_id="test-kis-lane-key-v1",
                key_registry=fixture.registry,
            )
            result = replacement.snapshot(V2_OPERATION)
            self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
            self.assertTrue(result["hazardousAuthorityOpen"])
            self.assertFalse(result["allOrFailClosedCompensationVerified"])
        finally:
            fixture.close()

    def test_prior_account_reconciliation_blocks_distinct_operation_post_zero(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.fail_component = "heartbeat"
            fixture.fail_compensation = "heartbeat"
            first = fixture.coordinator.execute(request=fixture.request())
            self.assertEqual("RECONCILIATION_REQUIRED", first["state"])
            fixture.calls.clear()
            second = fixture.request(operation_id="kis-graph-v2-operation-two")
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked,
                "account-has-unresolved-reconciliation",
            ):
                fixture.coordinator.execute(request=second)
            self.assertEqual([], fixture.calls)
        finally:
            fixture.close()

    def test_union_reverifies_receipt_and_recomputes_all_scalar_flags(self) -> None:
        fixture = _V2Harness()
        try:
            fixture.coordinator.execute(request=fixture.request())
            fixture.receipt_verification_enabled = False
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked,
                "stored-port-receipt-unverified",
            ):
                fixture.coordinator.verify_union(V2_OPERATION)
            fixture.receipt_verification_enabled = True
            with fixture.ledger.connection() as conn:
                conn.execute(
                    "UPDATE kis_functional_v2_graph_operation SET "
                    "burned_quote=0 WHERE operation_id=?",
                    (V2_OPERATION,),
                )
                conn.commit()
            with self.assertRaisesRegex(
                KisDomesticFunctionalGraphBlocked,
                "flags-do-not-match-step-chain",
            ):
                fixture.coordinator.verify_union(V2_OPERATION)
        finally:
            fixture.close()


if __name__ == "__main__":
    unittest.main()
