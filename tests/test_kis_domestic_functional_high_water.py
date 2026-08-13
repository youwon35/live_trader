from __future__ import annotations

import base64
import hashlib
import json
import shutil
import tempfile
import threading
import unittest
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_contract import PDNO, ROUTE
from live_trader.kis_domestic_functional_high_water import (
    AppendOnlyKisBootstrapHighWater,
    ExternalHighWaterPins,
    INSTALLATION_SCHEMA,
    MAIN_PROJECTION_SCHEMA,
    ROOT_DOMAIN,
    TRANSITION_SCHEMA,
    WRITER_DOMAIN,
    KisDomesticFunctionalHighWaterBlocked,
    high_water_component_status,
)


OWNER_RECORD = hashlib.sha256(b"owner-record").hexdigest()
REGISTRY_HEAD = hashlib.sha256(b"registry-accepted-head").hexdigest()
ISSUANCE = hashlib.sha256(b"issuance-binding").hexdigest()
CREATED = datetime(2026, 8, 13, 22, 0, tzinfo=timezone.utc)
OCCURRED = CREATED + timedelta(hours=2)
MONO = 100_000_000_000


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


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _path_hash(path: Path) -> str:
    return _hash(
        {
            "schemaVersion": "kis-domestic-functional-high-water-path/v1",
            "absolutePath": str(path.resolve()),
        }
    )


def _sign(private, domain: bytes, body: dict, key_id: str) -> dict:
    return {
        "body": body,
        "recordHash": _hash(body),
        "signature": base64.b64encode(
            eddsa.new(private, mode="rfc8032").sign(domain + _canonical(body))
        ).decode("ascii"),
        "keyIdHash": key_id,
    }


class _Fixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temp.name)
        self.path = self.root_path / "protected" / "kis-bootstrap.anchor.jsonl"
        self.root_private = ECC.generate(curve="Ed25519")
        self.writer_private = ECC.generate(curve="Ed25519")
        self.root_public = self.root_private.public_key().export_key(format="PEM")
        self.writer_public = self.writer_private.public_key().export_key(format="PEM")
        self.root_id = hashlib.sha256(self.root_public.encode()).hexdigest()
        self.writer_id = hashlib.sha256(self.writer_public.encode()).hexdigest()
        self.anchor_id = "kis-bootstrap-external-anchor-0001"
        self.header_body = {
            "schemaVersion": INSTALLATION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": self.anchor_id,
            "anchorPathHash": _path_hash(self.path),
            "epoch": 0,
            "everIssued": False,
            "issuanceBindingHash": None,
            "previousHeadHash": "0" * 64,
            "ownerEpoch": 7,
            "ownerRecordHash": OWNER_RECORD,
            "registryId": "kis-production-registry-0001",
            "registryEpoch": 11,
            "registryAcceptedHeadHash": REGISTRY_HEAD,
            "rootKeyIdHash": self.root_id,
            "writerKeyIdHash": self.writer_id,
            "writerPublicKeyPem": self.writer_public,
            "writerPurpose": "KIS_BOOTSTRAP_HIGH_WATER_APPEND_ONLY",
            "writerNotBefore": _iso(CREATED - timedelta(minutes=1)),
            "writerNotAfter": _iso(CREATED + timedelta(days=2)),
            "createdAt": _iso(CREATED),
            "createdMonotonicNs": MONO,
            "productionProvisioned": False,
        }
        self.header = _sign(
            self.root_private, ROOT_DOMAIN, self.header_body, self.root_id
        )
        self.header_head = _hash(self.header)
        self.pins = ExternalHighWaterPins(
            anchor_id=self.anchor_id,
            owner_epoch=7,
            owner_record_hash=OWNER_RECORD,
            registry_id="kis-production-registry-0001",
            registry_epoch=11,
            registry_accepted_head_hash=REGISTRY_HEAD,
            root_public_key_pem=self.root_public,
            root_key_id_hash=self.root_id,
            writer_key_id_hash=self.writer_id,
            minimum_epoch=0,
            minimum_head_hash=self.header_head,
        )
        self.anchor = AppendOnlyKisBootstrapHighWater.provision_disabled(
            self.path,
            pins=self.pins,
            root_signed_installation=self.header,
        )

    def burn_envelope(self, **body_changes) -> dict:
        body = self.anchor.next_burn_body(
            issuance_binding_hash=ISSUANCE,
            occurred_at=OCCURRED,
            occurred_monotonic_ns=MONO + 7_200_000_000_000,
        )
        body.update(body_changes)
        return _sign(self.writer_private, WRITER_DOMAIN, body, self.writer_id)

    def burn(self) -> dict:
        return self.anchor.append_signed_burn(self.burn_envelope())

    def main_projection(self, *, burned: bool) -> dict:
        read = self.anchor.read()
        if burned:
            return {
                "schemaVersion": MAIN_PROJECTION_SCHEMA,
                "route": ROUTE,
                "pdno": PDNO,
                "anchorId": self.anchor_id,
                "anchorEpoch": read["epoch"],
                "anchorHeadHash": read["headHash"],
                "everIssued": True,
                "issuanceBindingHash": ISSUANCE,
                "ownerEpoch": 7,
                "ownerRecordHash": OWNER_RECORD,
                "registryAcceptedHeadHash": REGISTRY_HEAD,
            }
        return {
            "schemaVersion": MAIN_PROJECTION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": self.anchor_id,
            "anchorEpoch": 0,
            "anchorHeadHash": self.header_head,
            "everIssued": False,
            "issuanceBindingHash": None,
            "ownerEpoch": 7,
            "ownerRecordHash": OWNER_RECORD,
            "registryAcceptedHeadHash": REGISTRY_HEAD,
        }

    def cleanup(self) -> None:
        self.anchor.close()
        self.temp.cleanup()


class KisDomesticFunctionalExternalHighWaterTest(unittest.TestCase):
    def test_root_signed_installation_is_verify_only_and_disabled(self):
        fixture = _Fixture()
        try:
            status = fixture.anchor.read()
            self.assertEqual(0, status["epoch"])
            self.assertFalse(status["everIssued"])
            self.assertTrue(status["rootRegistrySignatureVerified"])
            self.assertTrue(status["minimumRollbackPinSuppliedAndVerified"])
            self.assertFalse(status["minimumRollbackPinExternallyPersisted"])
            self.assertTrue(status["anchorLossFailsClosed"])
            self.assertFalse(status["powerLossDurabilityIndependentlyProven"])
            self.assertFalse(
                status["externalWriterBurnCommitPrecedesLocalAppend"]
            )
            self.assertTrue(status["osProcessLeaseHeld"])
            self.assertTrue(status["verifyOnlyConsumer"])
            self.assertFalse(status["privateSignerPresent"])
            self.assertFalse(status["productionWriterAvailable"])
            self.assertFalse(status["productionAvailable"])
            self.assertFalse(status["networkOrderPostAllowed"])
            self.assertEqual(0, status["tradingMutationCount"])
            self.assertIn(
                "INDEPENDENT_PRODUCTION_WRITER_NOT_PROVISIONED",
                status["readinessBlockers"],
            )
            self.assertIn(
                "EXTERNAL_MINIMUM_ROLLBACK_PIN_STORE_NOT_WIRED",
                status["readinessBlockers"],
            )
            self.assertIn(
                "EXTERNAL_WRITER_BURN_COMMIT_PRECEDES_LOCAL_APPEND_NOT_WIRED",
                status["readinessBlockers"],
            )
        finally:
            fixture.cleanup()

    def test_header_root_signature_path_and_registry_binding_are_exact(self):
        fixture = _Fixture()
        try:
            fixture.anchor.close()
            fixture.path.unlink()
            for key, value in (
                ("registryAcceptedHeadHash", "0" * 64),
                ("anchorPathHash", "0" * 64),
                ("productionProvisioned", True),
            ):
                body = deepcopy(fixture.header_body)
                body[key] = value
                envelope = _sign(
                    fixture.root_private, ROOT_DOMAIN, body, fixture.root_id
                )
                with self.subTest(key=key):
                    with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                        AppendOnlyKisBootstrapHighWater.provision_disabled(
                            fixture.path,
                            pins=fixture.pins,
                            root_signed_installation=envelope,
                        )
                    self.assertFalse(fixture.path.exists())
            bad = deepcopy(fixture.header)
            bad["signature"] = "bad"
            with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                AppendOnlyKisBootstrapHighWater.provision_disabled(
                    fixture.path,
                    pins=fixture.pins,
                    root_signed_installation=bad,
                )
        finally:
            fixture.temp.cleanup()

    def test_signed_burn_is_one_way_and_restart_pin_is_returned(self):
        fixture = _Fixture()
        try:
            result = fixture.burn()
            self.assertTrue(result["everIssued"])
            self.assertEqual(1, result["epoch"])
            self.assertEqual(ISSUANCE, result["issuanceBindingHash"])
            self.assertEqual(1, result["restartMinimumEpoch"])
            self.assertEqual(result["headHash"], result["restartMinimumHeadHash"])
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "ever-issued-burned"
            ):
                fixture.anchor.next_burn_body(
                    issuance_binding_hash=ISSUANCE,
                    occurred_at=OCCURRED,
                    occurred_monotonic_ns=MONO + 7_200_000_000_001,
                )
        finally:
            fixture.cleanup()

    def test_main_database_copy_restore_or_delete_reconciles_to_burn(self):
        fixture = _Fixture()
        try:
            stale = fixture.main_projection(burned=False)
            fixture.burn()
            for projection in (stale, None):
                result = fixture.anchor.reconcile_main(projection)
                self.assertEqual(
                    "BURNED_RECONCILIATION_REQUIRED", result["classification"]
                )
                self.assertTrue(result["routeEverIssuedBurned"])
                self.assertFalse(result["mayIssue"])
            exact = fixture.anchor.reconcile_main(
                fixture.main_projection(burned=True)
            )
            self.assertEqual("BURNED_CONFIRMED", exact["classification"])
            self.assertFalse(exact["mayIssue"])
        finally:
            fixture.cleanup()

    def test_missing_or_unreadable_anchor_is_hold_never_reissue(self):
        fixture = _Fixture()
        try:
            fixture.anchor.close()
            fixture.path.unlink()
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "missing-hold"
            ):
                AppendOnlyKisBootstrapHighWater(fixture.path, pins=fixture.pins)
            fixture.path.write_bytes(b"not-json\n")
            with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                AppendOnlyKisBootstrapHighWater(fixture.path, pins=fixture.pins)
        finally:
            fixture.temp.cleanup()

    def test_anchor_rollback_below_postburn_minimum_pin_is_hold(self):
        fixture = _Fixture()
        try:
            initial = fixture.path.read_bytes()
            receipt = fixture.burn()
            postburn = replace(
                fixture.pins,
                minimum_epoch=receipt["restartMinimumEpoch"],
                minimum_head_hash=receipt["restartMinimumHeadHash"],
            )
            fixture.anchor.close()
            fixture.path.write_bytes(initial)
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "rollback-or-substitution"
            ):
                AppendOnlyKisBootstrapHighWater(fixture.path, pins=postburn)
        finally:
            fixture.temp.cleanup()

    def test_crash_after_append_write_is_recovered_as_burn_not_retry(self):
        fixture = _Fixture()
        try:
            envelope = fixture.burn_envelope()
            fixture.anchor.close()

            def crash(stage: str) -> None:
                if stage == "AFTER_APPEND_WRITE_BEFORE_FSYNC":
                    raise RuntimeError(stage)

            crashing = AppendOnlyKisBootstrapHighWater(
                fixture.path, pins=fixture.pins, failure_injector=crash
            )
            with self.assertRaisesRegex(RuntimeError, "AFTER_APPEND_WRITE"):
                crashing.append_signed_burn(envelope)
            status = crashing.read()
            self.assertTrue(status["everIssued"])
            self.assertFalse(crashing.reconcile_main(None)["mayIssue"])
            crashing.close()
        finally:
            fixture.temp.cleanup()

    def test_process_lease_is_exclusive_and_restart_safe(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "os-lease-unavailable"
            ):
                AppendOnlyKisBootstrapHighWater(fixture.path, pins=fixture.pins)
            fixture.anchor.close()
            restarted = AppendOnlyKisBootstrapHighWater(
                fixture.path, pins=fixture.pins
            )
            self.assertTrue(restarted.read()["osProcessLeaseHeld"])
            restarted.close()
        finally:
            fixture.temp.cleanup()

    def test_transition_signature_binding_and_clock_tamper_do_not_append(self):
        fixture = _Fixture()
        try:
            initial = fixture.path.read_bytes()
            candidates = [
                fixture.burn_envelope(previousHeadHash="0" * 64),
            ]
            changed_after_signing = fixture.burn_envelope()
            changed_after_signing["body"]["issuanceBindingHash"] = "0" * 64
            candidates.append(changed_after_signing)
            bad_signature = fixture.burn_envelope()
            bad_signature["signature"] = "bad"
            candidates.append(bad_signature)
            for candidate in candidates:
                with self.subTest(candidate=candidate["body"]):
                    with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                        fixture.anchor.append_signed_burn(candidate)
                    self.assertEqual(initial, fixture.path.read_bytes())
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "clock-outside-certificate"
            ):
                fixture.anchor.next_burn_body(
                    issuance_binding_hash=ISSUANCE,
                    occurred_at=CREATED - timedelta(seconds=1),
                    occurred_monotonic_ns=MONO - 1,
                )
            self.assertEqual(initial, fixture.path.read_bytes())
        finally:
            fixture.cleanup()

    def test_truncation_extra_line_and_noncanonical_rewrite_are_hold(self):
        mutators = (
            lambda raw: raw[:-1],
            lambda raw: raw + raw,
            lambda raw: json.dumps(json.loads(raw.decode())).encode() + b"\n",
        )
        for mutate in mutators:
            fixture = _Fixture()
            try:
                fixture.anchor.close()
                fixture.path.write_bytes(mutate(fixture.path.read_bytes()))
                with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                    AppendOnlyKisBootstrapHighWater(
                        fixture.path, pins=fixture.pins
                    )
            finally:
                fixture.temp.cleanup()

    def test_existing_target_and_copied_path_are_not_accepted(self):
        fixture = _Fixture()
        try:
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "target-exists"
            ):
                AppendOnlyKisBootstrapHighWater.provision_disabled(
                    fixture.path,
                    pins=fixture.pins,
                    root_signed_installation=fixture.header,
                )
            fixture.anchor.close()
            copied = fixture.root_path / "copied.anchor.jsonl"
            shutil.copy2(fixture.path, copied)
            with self.assertRaisesRegex(
                KisDomesticFunctionalHighWaterBlocked, "header-binding-invalid"
            ):
                AppendOnlyKisBootstrapHighWater(copied, pins=fixture.pins)
        finally:
            fixture.temp.cleanup()

    def test_main_projection_substitution_and_main_ahead_fail_closed(self):
        fixture = _Fixture()
        try:
            exact = fixture.main_projection(burned=False)
            self.assertEqual(
                "UNISSUED_EXACT",
                fixture.anchor.reconcile_main(exact)["classification"],
            )
            for change in (
                {"ownerEpoch": 8},
                {"everIssued": True, "anchorEpoch": 1,
                 "issuanceBindingHash": ISSUANCE},
                {"unexpected": True},
            ):
                candidate = deepcopy(exact)
                candidate.update(change)
                with self.subTest(change=change):
                    with self.assertRaises(KisDomesticFunctionalHighWaterBlocked):
                        fixture.anchor.reconcile_main(candidate)
        finally:
            fixture.cleanup()

    def test_concurrent_signed_burn_has_exactly_one_append(self):
        fixture = _Fixture()
        try:
            envelope = fixture.burn_envelope()
            barrier = threading.Barrier(2)
            results: list[str] = []

            def run() -> None:
                barrier.wait()
                try:
                    fixture.anchor.append_signed_burn(envelope)
                    results.append("burned")
                except KisDomesticFunctionalHighWaterBlocked:
                    results.append("blocked")

            threads = [threading.Thread(target=run) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)
            self.assertEqual(["blocked", "burned"], sorted(results))
            self.assertEqual(2, len(fixture.path.read_bytes().splitlines()))
            self.assertTrue(fixture.anchor.read()["everIssued"])
        finally:
            fixture.cleanup()

    def test_static_component_status_has_no_authority_or_signer(self):
        status = high_water_component_status()
        self.assertFalse(status["externalAnchorPresent"])
        self.assertFalse(status["privateSignerPresent"])
        self.assertFalse(status["productionWriterAvailable"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertEqual(0, status["tradingMutationCount"])
        self.assertIn(
            "INDEPENDENT_PRODUCTION_WRITER_NOT_PROVISIONED",
            status["readinessBlockers"],
        )


if __name__ == "__main__":
    unittest.main()
