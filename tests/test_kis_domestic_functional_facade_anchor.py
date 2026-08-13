from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Crypto.PublicKey import ECC
from Crypto.Signature import eddsa

from live_trader.kis_domestic_functional_facade_anchor import (
    INSTALLATION_SCHEMA,
    PROJECTION_SCHEMA,
    ROOT_SIGNATURE_DOMAIN,
    WRITER_PURPOSE,
    WRITER_SIGNATURE_DOMAIN,
    AppendOnlyKisDomesticFunctionalFacadeAnchor,
    ExternalFacadeAnchorPins,
    KisDomesticFunctionalFacadeAnchorBlocked,
    _path_hash,
    production_entrypoint_status,
)


ROUTE = "KIS_KR_LIVE_CONTINUOUS"
PDNO = "010140"
ZERO = "0" * 64
NOW = datetime(2026, 8, 14, 6, 0, tzinfo=timezone.utc)


def canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def public_pem(key) -> str:
    return key.public_key().export_key(format="PEM")


def key_hash(key) -> str:
    return hashlib.sha256(public_pem(key).encode("utf-8")).hexdigest()


def sign(key, domain: bytes, body) -> str:
    return base64.b64encode(
        eddsa.new(key, mode="rfc8032").sign(domain + canonical(body))
    ).decode("ascii")


class KisDomesticFunctionalFacadeAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root_dir = Path(self.temp.name)
        self.path = self.root_dir / "external" / "facade.anchor.jsonl"
        self.old_lock_dir = os.environ.get("LIVE_TRADER_PROCESS_LOCK_DIR")
        os.environ["LIVE_TRADER_PROCESS_LOCK_DIR"] = str(
            self.root_dir / "os-locks"
        )
        self.addCleanup(self._restore_environment)
        self.root = ECC.generate(curve="Ed25519")
        self.writer = ECC.generate(curve="Ed25519")
        self.ledger_id = hashlib.sha256(b"facade-ledger-id").hexdigest()

    def _restore_environment(self) -> None:
        if self.old_lock_dir is None:
            os.environ.pop("LIVE_TRADER_PROCESS_LOCK_DIR", None)
        else:
            os.environ["LIVE_TRADER_PROCESS_LOCK_DIR"] = self.old_lock_dir

    def _header_body(self):
        return {
            "schemaVersion": INSTALLATION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "anchorId": "kis-facade-anchor-test",
            "anchorPathHash": _path_hash(self.path.resolve()),
            "facadeLedgerIdHash": self.ledger_id,
            "rootKeyIdHash": key_hash(self.root),
            "writerKeyIdHash": key_hash(self.writer),
            "writerPublicKeyPem": public_pem(self.writer),
            "writerPurpose": WRITER_PURPOSE,
            "writerNotBefore": "2026-08-13T06:00:00Z",
            "writerNotAfter": "2026-08-16T06:00:00Z",
            "createdAt": "2026-08-14T06:00:00Z",
            "createdMonotonicNs": 100,
            "productionProvisioned": False,
        }

    def _envelope(self, body, *, key=None, domain=ROOT_SIGNATURE_DOMAIN):
        key = self.root if key is None else key
        return {
            "body": deepcopy(body),
            "recordHash": digest(body),
            "keyIdHash": (
                key_hash(self.root)
                if domain == ROOT_SIGNATURE_DOMAIN
                else key_hash(self.writer)
            ),
            "signature": sign(key, domain, body),
        }

    def _pins(self, installation, *, minimum_epoch=0, minimum_head=None):
        return ExternalFacadeAnchorPins(
            anchor_id="kis-facade-anchor-test",
            facade_ledger_id_hash=self.ledger_id,
            root_public_key_pem=public_pem(self.root),
            root_key_id_hash=key_hash(self.root),
            writer_key_id_hash=key_hash(self.writer),
            minimum_anchor_epoch=minimum_epoch,
            minimum_anchor_head_hash=(
                digest(installation) if minimum_head is None else minimum_head
            ),
        )

    def _create(self, *, injector=None):
        body = self._header_body()
        installation = self._envelope(body)
        pins = self._pins(installation)
        anchor = AppendOnlyKisDomesticFunctionalFacadeAnchor.provision_disabled(
            self.path,
            pins=pins,
            root_signed_installation=installation,
            failure_injector=injector,
        )
        self.addCleanup(anchor.close)
        return anchor, installation, pins

    def _projection(self, facade_epoch=1, epoch=1, snapshot=0, burn=0):
        def head(label, sequence):
            return (
                ZERO
                if sequence == 0
                else hashlib.sha256(f"{label}-{sequence}".encode()).hexdigest()
            )

        return {
            "schemaVersion": PROJECTION_SCHEMA,
            "route": ROUTE,
            "pdno": PDNO,
            "facadeLedgerIdHash": self.ledger_id,
            "facadeEpoch": facade_epoch,
            "epochSequence": epoch,
            "epochHeadHash": head("epoch", epoch),
            "snapshotSequence": snapshot,
            "snapshotHeadHash": head("snapshot", snapshot),
            "burnSequence": burn,
            "burnHeadHash": head("burn", burn),
        }

    def _signed_next(self, anchor, projection, *, second=1, key=None, domain=None):
        body = anchor.next_transition_body(
            projection,
            observed_at=NOW + timedelta(seconds=second),
            observed_monotonic_ns=100 + second,
        )
        key = self.writer if key is None else key
        domain = WRITER_SIGNATURE_DOMAIN if domain is None else domain
        envelope = {
            "body": body,
            "recordHash": digest(body),
            "keyIdHash": key_hash(self.writer),
            "signature": sign(key, domain, body),
        }
        return envelope

    def test_provisioned_anchor_is_verify_only_and_production_false(self):
        anchor, _, _ = self._create()
        status = anchor.read()
        self.assertEqual(status["anchorEpoch"], 0)
        self.assertTrue(status["rootInstallationSignatureVerified"])
        self.assertTrue(status["writerPurposeVerified"])
        self.assertTrue(status["osProcessLeaseHeld"])
        self.assertTrue(status["verifyOnlyConsumer"])
        self.assertFalse(status["privateSignerPresent"])
        self.assertFalse(status["productionProvisioningAvailable"])
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertFalse(hasattr(anchor, "sign"))
        self.assertFalse(any("private" in name for name in vars(anchor)))

    def test_append_fsync_restart_and_external_minimum_pin(self):
        anchor, _, pins = self._create()
        projection = self._projection(snapshot=1)
        status = anchor.append_signed_transition(
            self._signed_next(anchor, projection)
        )
        self.assertEqual(status["anchorEpoch"], 1)
        self.assertEqual(anchor.current_projection(), projection)
        head = status["anchorHeadHash"]
        anchor.close()
        values = asdict(pins)
        values.update(minimum_anchor_epoch=1, minimum_anchor_head_hash=head)
        restarted = AppendOnlyKisDomesticFunctionalFacadeAnchor(
            self.path, pins=ExternalFacadeAnchorPins(**values)
        )
        self.addCleanup(restarted.close)
        self.assertEqual(restarted.current_projection(), projection)
        self.assertEqual(restarted.read()["minimumAnchorEpochVerified"], 1)

    def test_same_process_duplicate_os_lease_is_rejected_then_released(self):
        anchor, _, pins = self._create()
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "os-lease-unavailable",
        ):
            AppendOnlyKisDomesticFunctionalFacadeAnchor(self.path, pins=pins)
        anchor.close()
        reopened = AppendOnlyKisDomesticFunctionalFacadeAnchor(
            self.path, pins=pins
        )
        self.addCleanup(reopened.close)
        self.assertTrue(reopened.read()["osProcessLeaseHeld"])

    def test_actual_child_process_cannot_acquire_same_os_lease(self):
        anchor, _, pins = self._create()
        pins_path = self.root_dir / "pins.json"
        pins_path.write_text(json.dumps(asdict(pins)), encoding="utf-8")
        script = (
            "import json,sys; from pathlib import Path; "
            "from live_trader.kis_domestic_functional_facade_anchor import "
            "AppendOnlyKisDomesticFunctionalFacadeAnchor as A, "
            "ExternalFacadeAnchorPins as P, "
            "KisDomesticFunctionalFacadeAnchorBlocked as B; "
            "d=json.loads(Path(sys.argv[2]).read_text()); "
            "\ntry:\n A(sys.argv[1],pins=P(**d)); print('UNSAFE')"
            "\nexcept B:\n print('BLOCKED')"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-c", script, str(self.path), str(pins_path)],
            cwd=Path(__file__).parents[1],
            env=os.environ.copy(),
            text=True,
            capture_output=True,
            timeout=15,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "BLOCKED")
        self.assertTrue(anchor.read()["osProcessLeaseHeld"])

    def test_paired_facade_ledger_and_local_highwater_restore_is_detected(self):
        anchor, _, _ = self._create()
        facade_db = self.root_dir / "facade.sqlite3"
        local_high = self.root_dir / "facade.high-water.json"
        first = self._projection(epoch=1, snapshot=1)
        anchor.append_signed_transition(self._signed_next(anchor, first))
        facade_db.write_bytes(b"facade-version-one")
        local_high.write_bytes(b"local-high-water-one")
        saved_pair = (facade_db.read_bytes(), local_high.read_bytes(), deepcopy(first))
        second = self._projection(epoch=2, snapshot=2, burn=1)
        anchor.append_signed_transition(
            self._signed_next(anchor, second, second=2)
        )
        facade_db.write_bytes(b"facade-version-two")
        local_high.write_bytes(b"local-high-water-two")
        facade_db.write_bytes(saved_pair[0])
        local_high.write_bytes(saved_pair[1])
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "paired-local-rollback-detected",
        ):
            anchor.assert_current_projection(saved_pair[2])
        self.assertEqual(anchor.current_projection(), second)

    def test_equal_sequence_head_substitution_is_rejected(self):
        anchor, _, _ = self._create()
        first = self._projection(epoch=1)
        anchor.append_signed_transition(self._signed_next(anchor, first))
        substituted = deepcopy(first)
        substituted["epochHeadHash"] = hashlib.sha256(b"other").hexdigest()
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "equal-sequence-head-substitution",
        ):
            anchor.next_transition_body(
                substituted,
                observed_at=NOW + timedelta(seconds=2),
                observed_monotonic_ns=102,
            )

    def test_sequence_and_facade_epoch_rollback_are_rejected(self):
        anchor, _, _ = self._create()
        latest = self._projection(facade_epoch=2, epoch=3, snapshot=2)
        anchor.append_signed_transition(self._signed_next(anchor, latest))
        for candidate in (
            self._projection(facade_epoch=2, epoch=2, snapshot=2),
            self._projection(facade_epoch=1, epoch=4, snapshot=3),
            self._projection(facade_epoch=3, epoch=3, snapshot=3),
        ):
            with self.subTest(candidate=candidate):
                with self.assertRaises(KisDomesticFunctionalFacadeAnchorBlocked):
                    anchor.next_transition_body(
                        candidate,
                        observed_at=NOW + timedelta(seconds=2),
                        observed_monotonic_ns=102,
                    )

    def test_writer_signature_wrong_domain_wrong_key_and_body_tamper_fail(self):
        for mode in ("domain", "key", "body"):
            with self.subTest(mode=mode):
                path = self.path.with_name(f"{mode}.anchor")
                original_path = self.path
                self.path = path
                try:
                    anchor, _, _ = self._create()
                    other = ECC.generate(curve="Ed25519")
                    if mode == "domain":
                        envelope = self._signed_next(
                            anchor,
                            self._projection(),
                            domain=ROOT_SIGNATURE_DOMAIN,
                        )
                    elif mode == "key":
                        envelope = self._signed_next(
                            anchor, self._projection(), key=other
                        )
                    else:
                        envelope = self._signed_next(anchor, self._projection())
                        envelope["body"]["facadeEpoch"] = 2
                    with self.assertRaises(KisDomesticFunctionalFacadeAnchorBlocked):
                        anchor.append_signed_transition(envelope)
                finally:
                    self.path = original_path

    def test_duplicate_signed_transition_is_not_replayable(self):
        anchor, _, _ = self._create()
        envelope = self._signed_next(anchor, self._projection())
        anchor.append_signed_transition(envelope)
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "does-not-advance|not-current-next",
        ):
            anchor.append_signed_transition(envelope)

    def test_deleted_transition_fails_external_minimum_pin_on_restart(self):
        anchor, _, pins = self._create()
        anchor.append_signed_transition(
            self._signed_next(anchor, self._projection(snapshot=1))
        )
        status = anchor.read()
        lines = self.path.read_bytes().splitlines(keepends=True)
        anchor.close()
        self.path.write_bytes(lines[0])
        values = asdict(pins)
        values.update(
            minimum_anchor_epoch=1,
            minimum_anchor_head_hash=status["anchorHeadHash"],
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "minimum-rollback-pin-failed",
        ):
            AppendOnlyKisDomesticFunctionalFacadeAnchor(
                self.path, pins=ExternalFacadeAnchorPins(**values)
            )

    def test_same_inode_truncation_is_detected_by_process_lifetime_head(self):
        anchor, _, _ = self._create()
        anchor.append_signed_transition(
            self._signed_next(anchor, self._projection(snapshot=1))
        )
        header = self.path.read_bytes().splitlines(keepends=True)[0]
        self.path.write_bytes(header)
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "process-lifetime-rollback-detected",
        ):
            anchor.read()

    def test_truncation_noncanonical_and_unknown_field_fail_closed(self):
        for mode in ("truncate", "noncanonical", "unknown"):
            with self.subTest(mode=mode):
                original_path = self.path
                self.path = self.path.with_name(f"{mode}.anchor")
                try:
                    anchor, _, pins = self._create()
                    raw = self.path.read_bytes()
                    anchor.close()
                    if mode == "truncate":
                        self.path.write_bytes(raw[:-1])
                    elif mode == "noncanonical":
                        self.path.write_bytes(raw[:-1] + b" \n")
                    else:
                        value = json.loads(raw)
                        value["unexpected"] = True
                        self.path.write_bytes(canonical(value) + b"\n")
                    with self.assertRaises(KisDomesticFunctionalFacadeAnchorBlocked):
                        AppendOnlyKisDomesticFunctionalFacadeAnchor(
                            self.path, pins=pins
                        )
                finally:
                    self.path = original_path

    def test_path_file_identity_replacement_is_detected_while_open(self):
        anchor, _, _ = self._create()
        replacement = self.path.with_suffix(".replacement")
        replacement.write_bytes(self.path.read_bytes())
        os.replace(replacement, self.path)
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "identity-replaced",
        ):
            anchor.read()

    def test_root_signed_wrong_writer_purpose_and_root_tamper_fail(self):
        for mode in ("purpose", "signature"):
            with self.subTest(mode=mode):
                original_path = self.path
                self.path = self.path.with_name(f"header-{mode}.anchor")
                try:
                    body = self._header_body()
                    if mode == "purpose":
                        body["writerPurpose"] = "UNRELATED_PURPOSE"
                    installation = self._envelope(body)
                    if mode == "signature":
                        installation["signature"] = base64.b64encode(
                            b"x" * 64
                        ).decode()
                    pins = self._pins(installation)
                    with self.assertRaises(KisDomesticFunctionalFacadeAnchorBlocked):
                        AppendOnlyKisDomesticFunctionalFacadeAnchor.provision_disabled(
                            self.path,
                            pins=pins,
                            root_signed_installation=installation,
                        )
                finally:
                    self.path = original_path

    def test_before_append_failure_leaves_chain_unchanged(self):
        def fail(phase):
            if phase == "before-append":
                raise KisDomesticFunctionalFacadeAnchorBlocked("injected")

        anchor, _, _ = self._create(injector=fail)
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked, "injected"
        ):
            anchor.append_signed_transition(
                self._signed_next(anchor, self._projection())
            )
        self.assertEqual(anchor.read()["anchorEpoch"], 0)

    def test_future_projection_requires_external_reconciliation(self):
        anchor, _, _ = self._create()
        current = self._projection(epoch=1)
        anchor.append_signed_transition(self._signed_next(anchor, current))
        future = self._projection(epoch=2)
        with self.assertRaisesRegex(
            KisDomesticFunctionalFacadeAnchorBlocked,
            "reconciliation-required",
        ):
            anchor.assert_current_projection(future)

    def test_wrong_external_minimum_head_and_anchor_path_fail(self):
        anchor, _, pins = self._create()
        anchor.close()
        with self.subTest("minimum"):
            values = asdict(pins)
            values["minimum_anchor_head_hash"] = hashlib.sha256(b"wrong").hexdigest()
            with self.assertRaisesRegex(
                KisDomesticFunctionalFacadeAnchorBlocked,
                "minimum-rollback-pin-failed",
            ):
                AppendOnlyKisDomesticFunctionalFacadeAnchor(
                    self.path, pins=ExternalFacadeAnchorPins(**values)
                )
        with self.subTest("path"):
            moved = self.path.with_name("moved.anchor")
            moved.write_bytes(self.path.read_bytes())
            with self.assertRaisesRegex(
                KisDomesticFunctionalFacadeAnchorBlocked,
                "header-binding-invalid",
            ):
                AppendOnlyKisDomesticFunctionalFacadeAnchor(moved, pins=pins)

    def test_entrypoint_and_hardware_worm_limit_are_explicit(self):
        anchor, _, _ = self._create()
        status = anchor.read()
        self.assertFalse(status["hardwareOrWormMonotonicityProven"])
        self.assertFalse(status["externalMinimumRollbackPinStoreWired"])
        self.assertIn(
            "HARDWARE_OR_WORM_MONOTONIC_COUNTER_NOT_WIRED",
            status["readinessBlockers"],
        )
        entrypoint = production_entrypoint_status()
        self.assertTrue(entrypoint["externalAppendOnlyAnchorImplemented"])
        self.assertFalse(entrypoint["productionProvisioningAvailable"])
        self.assertFalse(entrypoint["productionAvailable"])
        self.assertFalse(entrypoint["networkAvailable"])
        self.assertEqual(entrypoint["tradingMutationCount"], 0)


if __name__ == "__main__":
    unittest.main()
