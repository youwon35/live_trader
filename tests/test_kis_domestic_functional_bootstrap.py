from __future__ import annotations

import base64
import hashlib
import json
import shutil
import sqlite3
import tempfile
import threading
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_bootstrap import (
    BootstrapBindings,
    DurableKisDomesticFunctionalBootstrap,
    ISSUANCE_BUNDLE_SCHEMA,
    KisDomesticFunctionalBootstrapBlocked,
    PREAPPROVAL_SCHEMA,
    bootstrap_component_status,
)
from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_high_water import (
    AppendOnlyKisBootstrapHighWater,
    ExternalHighWaterPins,
    INSTALLATION_SCHEMA,
    ROOT_DOMAIN,
    WRITER_DOMAIN,
)


RESTATEMENT = hashlib.sha256(b"user exact KIS bootstrap restatement").hexdigest()
OWNER = hashlib.sha256(b"owner-record").hexdigest()
REGISTRY = hashlib.sha256(b"registry-head").hexdigest()
ACCOUNT = hashlib.sha256(b"account").hexdigest()
CREDENTIAL = hashlib.sha256(b"credential").hexdigest()
CODE = hashlib.sha256(b"code").hexdigest()
ARTIFACT = hashlib.sha256(b"artifact").hexdigest()
INSTANCE = hashlib.sha256(b"instance").hexdigest()
APPROVED = datetime(2026, 8, 13, 22, 55, tzinfo=timezone.utc)  # 07:55 KST
ARMED_AT = APPROVED + timedelta(minutes=1)
EXPIRES = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)  # 13:15 KST
TRIGGER = datetime(2026, 8, 14, 0, 5, tzinfo=timezone.utc)  # 09:05 KST
GRANT = TRIGGER + timedelta(seconds=1)
MONO = 100_000_000_000
CAPS = {
    "orderQuantity": 1,
    "maxOrderKrw": "100000",
    "maxGrossKrw": "100000",
    "ownerLossTriggerStrictlyBelowKrw": "5000",
    "activeSeconds": 7200,
    "naturalSellExpected": False,
    "promotionAllowed": False,
}


def _canonical(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _hash(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class _Fixture:
    def __init__(self, *, restatement: str | None = RESTATEMENT) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.main = root / "bootstrap.sqlite3"
        self.high_path = root / "high-water.anchor.jsonl"
        self.private = ECC.generate(curve="Ed25519")
        self.public_pem = self.private.public_key().export_key(format="PEM")
        self.key_id = hashlib.sha256(self.public_pem.encode()).hexdigest()
        self.root_private = ECC.generate(curve="Ed25519")
        self.writer_private = ECC.generate(curve="Ed25519")
        self.root_public = self.root_private.public_key().export_key(format="PEM")
        self.writer_public = self.writer_private.public_key().export_key(format="PEM")
        self.root_id = hashlib.sha256(self.root_public.encode()).hexdigest()
        self.writer_id = hashlib.sha256(self.writer_public.encode()).hexdigest()
        self.anchor_id = "kis-bootstrap-external-anchor-0001"
        anchor_path_hash = _hash({
            "schemaVersion": "kis-domestic-functional-high-water-path/v1",
            "absolutePath": str(self.high_path.resolve()),
        })
        header_body = {
            "schemaVersion": INSTALLATION_SCHEMA, "route": ROUTE, "pdno": PDNO,
            "anchorId": self.anchor_id, "anchorPathHash": anchor_path_hash,
            "epoch": 0, "everIssued": False, "issuanceBindingHash": None,
            "previousHeadHash": "0" * 64, "ownerEpoch": 7,
            "ownerRecordHash": OWNER, "registryId": "kis-production-registry-0001",
            "registryEpoch": 11, "registryAcceptedHeadHash": REGISTRY,
            "rootKeyIdHash": self.root_id, "writerKeyIdHash": self.writer_id,
            "writerPublicKeyPem": self.writer_public,
            "writerPurpose": "KIS_BOOTSTRAP_HIGH_WATER_APPEND_ONLY",
            "writerNotBefore": _iso(APPROVED - timedelta(hours=1)),
            "writerNotAfter": _iso(APPROVED + timedelta(days=2)),
            "createdAt": _iso(APPROVED),
            "createdMonotonicNs": MONO - 10_000_000_000,
            "productionProvisioned": False,
        }
        header = self._sign_with(
            self.root_private, ROOT_DOMAIN, header_body, self.root_id
        )
        self.high_pins = ExternalHighWaterPins(
            anchor_id=self.anchor_id, owner_epoch=7, owner_record_hash=OWNER,
            registry_id="kis-production-registry-0001", registry_epoch=11,
            registry_accepted_head_hash=REGISTRY,
            root_public_key_pem=self.root_public, root_key_id_hash=self.root_id,
            writer_key_id_hash=self.writer_id, minimum_epoch=0,
            minimum_head_hash=_hash(header),
        )
        self.high = AppendOnlyKisBootstrapHighWater.provision_disabled(
            self.high_path, pins=self.high_pins, root_signed_installation=header
        )
        self.bindings = BootstrapBindings(
            owner_epoch=7,
            owner_record_hash=OWNER,
            registry_accepted_head_hash=REGISTRY,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            code_manifest_hash=CODE,
            artifact_canonical_hash=ARTIFACT,
            instance_canonical_hash=INSTANCE,
            user_exact_restatement_hash=restatement,
            authority_key_id_hash=self.key_id,
        )
        self.ledger = self.open()

    def open(self, **kwargs) -> DurableKisDomesticFunctionalBootstrap:
        return DurableKisDomesticFunctionalBootstrap(
            self.main,
            bindings=self.bindings,
            preapproval_public_key_pem=self.public_pem,
            high_water=self.high,
            **kwargs,
        )

    def cleanup(self) -> None:
        self.high.close()
        self.temp.cleanup()

    def _sign(self, domain: bytes, body: dict) -> dict:
        return self._sign_with(self.private, domain, body, self.key_id)

    @staticmethod
    def _sign_with(private, domain: bytes, body: dict, key_id: str) -> dict:
        signature = base64.b64encode(
            eddsa.new(private, mode="rfc8032").sign(domain + _canonical(body))
        ).decode()
        return {
            "body": body,
            "recordHash": _hash(body),
            "signature": signature,
            ("authorityKeyIdHash" if domain in {
                b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_PREAPPROVAL\0",
                b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0",
            } else "keyIdHash"): key_id,
        }

    def preapproval(self, *, arm_id: str = "kis-arm-0001", **changes) -> dict:
        body = {
            "schemaVersion": PREAPPROVAL_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "armId": arm_id,
            "templateId": "kis-bootstrap-template-0001",
            "tradingDate": "2026-08-14",
            "ownerEpoch": 7,
            "ownerRecordHash": OWNER,
            "registryAcceptedHeadHash": REGISTRY,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "codeManifestHash": CODE,
            "artifactCanonicalHash": ARTIFACT,
            "instanceCanonicalHash": INSTANCE,
            "userExactRestatementHash": RESTATEMENT,
            "approvedAt": _iso(APPROVED),
            "expiresAt": _iso(EXPIRES),
            "caps": deepcopy(CAPS),
            "publicMarketDataOnlyBeforeIssue": True,
            "privateAccountAuthority": False,
            "orderAuthority": False,
            "oneUse": True,
            "nonPromotion": True,
            "productionMinted": False,
            "authorityKeyIdHash": self.key_id,
        }
        body.update(changes)
        return self._sign(b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_PREAPPROVAL\0", body)

    def arm(self, *, arm_id: str = "kis-arm-0001") -> dict:
        return self.ledger.provision_arm(
            self.preapproval(arm_id=arm_id),
            observed_at=ARMED_AT,
            observed_monotonic_ns=MONO,
        )

    def bundle(self, *, arm_id: str = "kis-arm-0001", **changes) -> dict:
        signal = {
            "classification": "NATURAL_BUY",
            "present": True,
            "evaluationId": "kis-evaluation-0001",
            "evaluationHash": hashlib.sha256(b"evaluation").hexdigest(),
            "triggerId": "kis-trigger-0001",
            "triggerHash": hashlib.sha256(b"trigger").hexdigest(),
            "triggerOpenAt": _iso(TRIGGER),
            "observedAt": _iso(TRIGGER + timedelta(milliseconds=100)),
            "source": "KIS_WEBSOCKET_H0STCNT0",
        }
        rolling = {
            "snapshotId": "kis-rolling-0001",
            "snapshotHash": hashlib.sha256(b"rolling").hexdigest(),
            "receiptHash": hashlib.sha256(b"rolling-receipt").hexdigest(),
            "state": "READY",
            "completedAt": _iso(TRIGGER - timedelta(seconds=30)),
            "expiresAt": _iso(GRANT + timedelta(seconds=30)),
        }
        quote = {
            "receiptId": "kis-quote-0001",
            "receiptHash": hashlib.sha256(b"quote").hexdigest(),
            "state": "READY",
            "observedAt": _iso(TRIGGER + timedelta(milliseconds=500)),
            "expiresAt": _iso(GRANT + timedelta(seconds=5)),
            "orderAuthorityFresh": True,
        }
        grant = {
            "receiptId": "kis-lane-grant-0001",
            "receiptHash": hashlib.sha256(b"lane-grant").hexdigest(),
            "grantWallAt": _iso(GRANT),
            "grantMonotonicNs": MONO + 1_000_000_000,
            "state": "READY",
        }
        body = {
            "schemaVersion": ISSUANCE_BUNDLE_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "issuanceId": "kis-bootstrap-issuance-0001",
            "armId": arm_id,
            "approvalRecordHash": self.preapproval(arm_id=arm_id)["recordHash"],
            "ownerEpoch": 7,
            "ownerRecordHash": OWNER,
            "registryAcceptedHeadHash": REGISTRY,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "codeManifestHash": CODE,
            "artifactCanonicalHash": ARTIFACT,
            "instanceCanonicalHash": INSTANCE,
            "userExactRestatementHash": RESTATEMENT,
            "naturalSignal": signal,
            "rollingPreflight": rolling,
            "freshQuote": quote,
            "laneGrant": grant,
            "observedAt": _iso(GRANT),
            "observedMonotonicNs": MONO + 1_000_000_000,
            "externalComponentCasRequested": True,
            "productionMinted": False,
            "authorityKeyIdHash": self.key_id,
        }
        body.update(changes)
        return self._sign(b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0", body)

    def issue(self, envelope: dict | None = None) -> dict:
        bundle = envelope or self.bundle()
        preparation = self.ledger.prepare_external_high_water_burn(
            bundle,
            observed_at=GRANT,
            observed_monotonic_ns=MONO + 1_000_000_000,
        )
        burn = self._sign_with(
            self.writer_private,
            WRITER_DOMAIN,
            preparation["externalHighWaterBurnBody"],
            self.writer_id,
        )
        return self.ledger.consume_and_issue(
            bundle,
            external_high_water_burn_envelope=burn,
            observed_at=GRANT,
            observed_monotonic_ns=MONO + 1_000_000_000,
        )


class KisDomesticFunctionalBootstrapTest(unittest.TestCase):
    def test_bootstrap_requires_exact_external_high_water_concrete_type(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "high-water-type-invalid"
            ):
                DurableKisDomesticFunctionalBootstrap(
                    Path(fixture.temp.name) / "wrong-type.sqlite3",
                    bindings=fixture.bindings,
                    preapproval_public_key_pem=fixture.public_pem,
                    high_water=object(),
                )
        finally:
            fixture.cleanup()

    def test_missing_exact_user_restatement_stays_post_zero(self):
        fixture = _Fixture(restatement=None)
        try:
            status = fixture.ledger.status()
            self.assertIn(
                "USER_EXACT_BOOTSTRAP_RESTATEMENT_MISSING",
                status["readinessBlockers"],
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked,
                "USER_EXACT_BOOTSTRAP_RESTATEMENT_MISSING",
            ):
                fixture.ledger.provision_arm(
                    fixture.preapproval(),
                    observed_at=ARMED_AT,
                    observed_monotonic_ns=MONO,
                )
            self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_public_armed_wait_has_zero_private_and_order_authority(self):
        fixture = _Fixture()
        try:
            status = fixture.arm()
            self.assertEqual("ARMED_WAIT", status["phase"])
            self.assertTrue(status["publicArmedWaitOnly"])
            self.assertFalse(status["preSignalPrivateAccountAuthority"])
            self.assertFalse(status["preSignalOrderAuthority"])
            self.assertFalse(status["routeEverIssuedBurned"])
            self.assertEqual(0, status["issueCount"])
        finally:
            fixture.cleanup()

    def test_hold_no_signal_expiry_is_post_zero_and_can_rearm(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            expired = fixture.ledger.expire_arm(
                arm_id="kis-arm-0001",
                observed_at=EXPIRES,
                observed_monotonic_ns=MONO + 10,
            )
            self.assertEqual("EXPIRED", expired["phase"])
            self.assertFalse(expired["routeEverIssuedBurned"])
            self.assertEqual(0, expired["issueCount"])
            next_approved = APPROVED + timedelta(days=4)
            next_observed = ARMED_AT + timedelta(days=4)
            next_expires = EXPIRES + timedelta(days=4)
            fixture.ledger.provision_arm(
                fixture.preapproval(
                    arm_id="kis-arm-0002",
                    tradingDate="2026-08-18",
                    approvedAt=_iso(next_approved),
                    expiresAt=_iso(next_expires),
                ),
                observed_at=next_observed,
                observed_monotonic_ns=MONO + 345_600_000_000_000,
            )
            self.assertEqual("ARMED_WAIT", fixture.ledger.status()["phase"])
            self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_preapproval_signature_binding_caps_and_time_tamper_do_not_burn(self):
        fixture = _Fixture()
        try:
            candidates = [
                fixture.preapproval(accountFingerprint="0" * 64),
                fixture.preapproval(caps={**CAPS, "orderQuantity": 2}),
                fixture.preapproval(approvedAt=_iso(TRIGGER)),
                fixture.preapproval(
                    tradingDate="2026-08-15",
                    approvedAt=_iso(APPROVED + timedelta(days=1)),
                    expiresAt=_iso(EXPIRES + timedelta(days=1)),
                ),
                fixture.preapproval(
                    tradingDate="2026-08-17",
                    approvedAt=_iso(APPROVED + timedelta(days=3)),
                    expiresAt=_iso(EXPIRES + timedelta(days=3)),
                ),
            ]
            candidate = fixture.preapproval()
            candidate["signature"] = "bad"
            candidates.append(candidate)
            for value in candidates:
                with self.subTest(value=value["body"].get("caps")):
                    with self.assertRaises(KisDomesticFunctionalBootstrapBlocked):
                        fixture.ledger.provision_arm(
                            value,
                            observed_at=ARMED_AT,
                            observed_monotonic_ns=MONO,
                        )
                    self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_invalid_signal_rolling_quote_or_lane_grant_never_burns(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            candidates = []
            for part, key, value in (
                ("naturalSignal", "present", False),
                ("rollingPreflight", "state", "STALE"),
                ("freshQuote", "orderAuthorityFresh", False),
                ("laneGrant", "grantWallAt", _iso(TRIGGER + timedelta(seconds=3))),
            ):
                envelope = fixture.bundle()
                body = deepcopy(envelope["body"])
                body[part][key] = value
                if part == "laneGrant":
                    body["observedAt"] = value
                candidates.append(fixture._sign(
                    b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0", body
                ))
            for value in candidates:
                with self.assertRaises(KisDomesticFunctionalBootstrapBlocked):
                    fixture.issue(value)
                self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_bundle_trigger_and_grant_are_inside_approved_xkrx_activation_window(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            for invalid_trigger in (
                datetime(2026, 8, 13, 23, 59, 59, tzinfo=timezone.utc),
                datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc),
            ):
                candidate = fixture.bundle()
                body = deepcopy(candidate["body"])
                body["naturalSignal"]["triggerOpenAt"] = _iso(invalid_trigger)
                body["naturalSignal"]["observedAt"] = _iso(GRANT)
                body["rollingPreflight"]["completedAt"] = _iso(
                    min(invalid_trigger, GRANT) - timedelta(seconds=1)
                )
                body["rollingPreflight"]["expiresAt"] = _iso(
                    GRANT + timedelta(minutes=1)
                )
                body["freshQuote"]["observedAt"] = _iso(GRANT)
                body["freshQuote"]["expiresAt"] = _iso(
                    GRANT + timedelta(seconds=5)
                )
                signed = fixture._sign(
                    b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0", body
                )
                with self.subTest(invalid_trigger=invalid_trigger):
                    with self.assertRaisesRegex(
                        KisDomesticFunctionalBootstrapBlocked,
                        "trading-session|bundle-time",
                    ):
                        fixture.ledger.consume_and_issue(
                            signed,
                            external_high_water_burn_envelope={},
                            observed_at=GRANT,
                            observed_monotonic_ns=MONO + 1_000_000_000,
                        )
                    self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_issue_clock_rollback_is_rejected_before_irreversible_reserve(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            candidate = fixture.bundle()
            body = deepcopy(candidate["body"])
            body["laneGrant"]["grantMonotonicNs"] = MONO - 1
            body["observedMonotonicNs"] = MONO - 1
            candidate = fixture._sign(
                b"KIS_DOMESTIC_FUNCTIONAL_BOOTSTRAP_ISSUANCE\0", body
            )
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked,
                "clock-rollback-before-high-water",
            ):
                fixture.ledger.consume_and_issue(
                    candidate,
                    external_high_water_burn_envelope={},
                    observed_at=GRANT,
                    observed_monotonic_ns=MONO - 1,
                )
            self.assertFalse(fixture.high.read()["everIssued"])
            self.assertEqual("ARMED_WAIT", fixture.ledger.status()["phase"])
        finally:
            fixture.cleanup()

    def test_status_rejects_active_and_consumed_arm_projection_tamper(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            conn = sqlite3.connect(fixture.main)
            conn.execute(
                "UPDATE kis_functional_bootstrap_arm SET state='EXPIRED' WHERE arm_id=?",
                ("kis-arm-0001",),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked,
                "route-related-row-mismatch",
            ):
                fixture.ledger.status()
        finally:
            fixture.cleanup()
        fixture = _Fixture()
        try:
            fixture.arm()
            fixture.issue()
            conn = sqlite3.connect(fixture.main)
            conn.execute(
                "UPDATE kis_functional_bootstrap_arm SET state='EXPIRED' WHERE arm_id=?",
                ("kis-arm-0001",),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked,
                "route-related-row-mismatch",
            ):
                fixture.ledger.status()
        finally:
            fixture.cleanup()

    def test_exact_bundle_consumes_once_and_route_is_ever_burned(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            status = fixture.issue()
            self.assertEqual("ISSUED", status["phase"])
            self.assertTrue(status["routeEverIssuedBurned"])
            self.assertEqual(1, status["issueCount"])
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "ever-issued-burned"
            ):
                fixture.ledger.provision_arm(
                    fixture.preapproval(arm_id="kis-arm-0002"),
                    observed_at=ARMED_AT,
                    observed_monotonic_ns=MONO + 2_000_000_000,
                )
        finally:
            fixture.cleanup()

    def test_external_burn_envelope_must_match_candidate_and_writer_signature(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            bundle = fixture.bundle()
            preparation = fixture.ledger.prepare_external_high_water_burn(
                bundle,
                observed_at=GRANT,
                observed_monotonic_ns=MONO + 1_000_000_000,
            )
            correct = fixture._sign_with(
                fixture.writer_private,
                WRITER_DOMAIN,
                preparation["externalHighWaterBurnBody"],
                fixture.writer_id,
            )
            for candidate in (
                {},
                {**correct, "signature": "bad"},
                {
                    **correct,
                    "body": {
                        **correct["body"],
                        "issuanceBindingHash": "0" * 64,
                    },
                },
            ):
                with self.subTest(candidate=candidate):
                    with self.assertRaises(Exception):
                        fixture.ledger.consume_and_issue(
                            bundle,
                            external_high_water_burn_envelope=candidate,
                            observed_at=GRANT,
                            observed_monotonic_ns=MONO + 1_000_000_000,
                        )
                    self.assertFalse(fixture.high.read()["everIssued"])
            status = fixture.ledger.consume_and_issue(
                bundle,
                external_high_water_burn_envelope=correct,
                observed_at=GRANT,
                observed_monotonic_ns=MONO + 1_000_000_000,
            )
            self.assertTrue(status["routeEverIssuedBurned"])
        finally:
            fixture.cleanup()

    def test_status_exposes_external_anchor_binding_and_pin_store_hold(self):
        fixture = _Fixture()
        try:
            status = fixture.ledger.status()
            self.assertEqual(0, status["externalHighWaterEpoch"])
            self.assertEqual(
                fixture.high.read()["headHash"],
                status["externalHighWaterHeadHash"],
            )
            self.assertTrue(status["externalRollbackPinSuppliedAndVerified"])
            self.assertFalse(status["externalRollbackPinStoreAvailable"])
            self.assertFalse(status["powerLossDurabilityIndependentlyProven"])
            self.assertFalse(
                status["externalWriterBurnCommitPrecedesLocalAppend"]
            )
            self.assertIn(
                "EXTERNAL_MINIMUM_ROLLBACK_PIN_STORE_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertIn(
                "PRODUCTION_EXTERNAL_HIGH_WATER_WRITER_NOT_PROVISIONED",
                status["readinessBlockers"],
            )
            self.assertIn(
                "EXTERNAL_WRITER_BURN_COMMIT_PRECEDES_LOCAL_APPEND_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertFalse(status["productionAvailable"])
        finally:
            fixture.cleanup()

    def test_si_failure_or_expiry_after_issue_persists_burn_across_restart(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            fixture.issue()
            status = fixture.ledger.mark_failure(
                reason="SI_FAILURE",
                observed_at=GRANT + timedelta(seconds=1),
                observed_monotonic_ns=MONO + 2_000_000_000,
            )
            self.assertEqual("BURNED", status["phase"])
            restarted = fixture.open()
            self.assertEqual("BURNED", restarted.status()["phase"])
            self.assertTrue(restarted.status()["routeEverIssuedBurned"])
        finally:
            fixture.cleanup()

    def test_crash_after_high_water_reservation_is_burned_without_main_issue(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            crashing = fixture.open(
                failure_injector=lambda stage: (_ for _ in ()).throw(
                    RuntimeError(stage)
                )
            )
            with self.assertRaisesRegex(RuntimeError, "AFTER_HIGH_WATER_RESERVED"):
                bundle = fixture.bundle()
                preparation = crashing.prepare_external_high_water_burn(
                    bundle,
                    observed_at=GRANT,
                    observed_monotonic_ns=MONO + 1_000_000_000,
                )
                burn = fixture._sign_with(
                    fixture.writer_private,
                    WRITER_DOMAIN,
                    preparation["externalHighWaterBurnBody"],
                    fixture.writer_id,
                )
                crashing.consume_and_issue(
                    bundle,
                    external_high_water_burn_envelope=burn,
                    observed_at=GRANT,
                    observed_monotonic_ns=MONO + 1_000_000_000,
                )
            self.assertTrue(fixture.high.read()["everIssued"])
            restarted = fixture.open()
            status = restarted.status()
            self.assertEqual("BURNED", status["phase"])
            self.assertEqual(0, status["issueCount"])
            with self.assertRaises(KisDomesticFunctionalBootstrapBlocked):
                restarted.provision_arm(
                    fixture.preapproval(arm_id="kis-arm-0002"),
                    observed_at=ARMED_AT,
                    observed_monotonic_ns=MONO + 3_000_000_000,
                )
        finally:
            fixture.cleanup()

    def test_main_database_rollback_or_copy_cannot_clear_external_burn(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            snapshot = Path(fixture.temp.name) / "pre-issue-copy.sqlite3"
            shutil.copy2(fixture.main, snapshot)
            fixture.issue()
            shutil.copy2(snapshot, fixture.main)
            restarted = fixture.open()
            status = restarted.status()
            self.assertEqual("BURNED", status["phase"])
            self.assertTrue(status["routeEverIssuedBurned"])
            self.assertEqual(0, status["issueCount"])
        finally:
            fixture.cleanup()

    def test_high_water_binding_copy_or_reprovision_mismatch_fails_closed(self):
        fixture = _Fixture()
        try:
            fixture.high.close()
            copied = Path(fixture.temp.name) / "copied.anchor.jsonl"
            shutil.copy2(fixture.high_path, copied)
            with self.assertRaisesRegex(
                Exception, "binding|path|header"
            ):
                AppendOnlyKisBootstrapHighWater(
                    copied, pins=fixture.high_pins
                )
            with self.assertRaisesRegex(
                Exception, "target-exists"
            ):
                AppendOnlyKisBootstrapHighWater.provision_disabled(
                    fixture.high_path,
                    pins=fixture.high_pins,
                    root_signed_installation=json.loads(
                        fixture.high_path.read_text().splitlines()[0]
                    ),
                )
        finally:
            fixture.temp.cleanup()

    def test_dirty_main_and_corrupt_high_water_fail_closed(self):
        fixture = _Fixture()
        try:
            conn = sqlite3.connect(fixture.main)
            conn.execute("ALTER TABLE kis_functional_bootstrap_route ADD COLUMN injected TEXT")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "schema-dirty"
            ):
                fixture.open()
        finally:
            fixture.cleanup()
        fixture = _Fixture()
        try:
            fixture.high.close()
            fixture.high_path.write_bytes(fixture.high_path.read_bytes()[:-1])
            with self.assertRaisesRegex(
                Exception, "truncated|noncanonical"
            ):
                AppendOnlyKisBootstrapHighWater(
                    fixture.high_path, pins=fixture.high_pins
                )
        finally:
            fixture.temp.cleanup()

    def test_route_clock_rollback_is_rejected_before_commit(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "clock-rollback"
            ):
                fixture.ledger.expire_arm(
                    arm_id="kis-arm-0001",
                    observed_at=EXPIRES,
                    observed_monotonic_ns=MONO - 1,
                )
            self.assertEqual("ARMED_WAIT", fixture.ledger.status()["phase"])
            self.assertFalse(fixture.high.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_arm_issue_and_transition_row_tamper_fail_closed(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            conn = sqlite3.connect(fixture.main)
            conn.execute(
                "UPDATE kis_functional_bootstrap_arm SET expires_at=?",
                (_iso(EXPIRES + timedelta(hours=1)),),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "arm-row-dirty"
            ):
                fixture.ledger.status()
        finally:
            fixture.cleanup()
        fixture = _Fixture()
        try:
            fixture.arm()
            fixture.issue()
            conn = sqlite3.connect(fixture.main)
            conn.execute(
                "UPDATE kis_functional_bootstrap_issue SET bundle_hash=?",
                ("0" * 64,),
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "issue-row-dirty"
            ):
                fixture.ledger.status()
        finally:
            fixture.cleanup()
        fixture = _Fixture()
        try:
            fixture.arm()
            conn = sqlite3.connect(fixture.main)
            conn.execute(
                "UPDATE kis_functional_bootstrap_transition SET phase='EXPIRED'"
            )
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(
                KisDomesticFunctionalBootstrapBlocked, "transition-chain-dirty"
            ):
                fixture.ledger.status()
        finally:
            fixture.cleanup()

    def test_concurrent_issue_has_one_winner_and_no_retry(self):
        fixture = _Fixture()
        try:
            fixture.arm()
            results = []
            barrier = threading.Barrier(2)

            def run() -> None:
                barrier.wait()
                try:
                    fixture.issue()
                    results.append("issued")
                except BaseException:
                    results.append("blocked")

            threads = [threading.Thread(target=run) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)
            self.assertEqual(["blocked", "issued"], sorted(results))
            self.assertEqual(1, fixture.high.read()["epoch"])
            self.assertEqual(1, fixture.ledger.status()["issueCount"])
        finally:
            fixture.cleanup()

    def test_component_status_is_explicitly_disabled(self):
        status = bootstrap_component_status()
        self.assertTrue(status["publicArmedWaitImplemented"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["mintAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["releaseAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertEqual(0, status["tradingMutationCount"])


if __name__ == "__main__":
    unittest.main()
