from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from live_trader import binance_spot_functional_state as state_bridge
from live_trader.binance_spot_functional_state import (
    BinanceSpotFunctionalStateFacade,
    load_or_create_server_secret,
)
from live_trader.binance_spot_functional_exclusivity_provider import (
    BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
)


class BinanceSpotFunctionalStateFacadeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.proof = root / "publication-proof.json"
        self.proof.write_text("{}", encoding="utf-8")
        self.facade = BinanceSpotFunctionalStateFacade(
            database_path=root / "functional.sqlite3",
            publication_proof_path=self.proof,
            data_root=root,
            server_secret_path=root / "server-secret.key",
            ordinary_routes_closed_reader=lambda: True,
        )

    def test_server_signing_secret_is_durable_and_not_process_random(self) -> None:
        path = Path(self.temporary.name) / "stable.key"
        first = load_or_create_server_secret(path)
        second = load_or_create_server_secret(path)
        self.assertEqual(first, second)
        self.assertEqual(48, len(first))
        self.assertNotIn(first.hex(), repr(self.facade.__dict__))

    def test_prepare_without_official_application_lease_is_fail_closed(self) -> None:
        with (
            patch.object(
                state_bridge,
                "live_trader_instance_lease_status",
                return_value={"acquired": False},
            ),
            patch.object(
                state_bridge,
                "prepare_binance_spot_functional_backend",
            ) as prepare,
            patch.dict(
                "os.environ",
                {
                    "BINANCE_API_KEY": "configured-key",
                    "BINANCE_API_SECRET": "configured-secret",
                },
                clear=False,
            ),
        ):
            result = self.facade.prepare()
        self.assertFalse(result["prepared"])
        self.assertFalse(result["networkOrderPostAllowed"])
        self.assertIn("applicationInstanceLease", result["reason"])
        prepare.assert_not_called()

    def test_prepare_is_composite_false_and_never_opens_stream_or_order(self) -> None:
        with (
            patch.object(
                state_bridge,
                "live_trader_instance_lease_status",
                return_value={"acquired": True, "ownerPid": 77},
            ),
            patch.object(
                state_bridge,
                "prepare_binance_spot_functional_backend",
                return_value={
                    "ok": True,
                    "prepared": True,
                    "status": {
                        "available": False,
                        "networkOrderPostAllowed": False,
                    },
                },
            ) as prepare,
            patch.dict(
                "os.environ",
                {
                    "BINANCE_BASE_URL": "https://api.binance.com",
                    "BINANCE_API_KEY": "configured-key",
                    "BINANCE_API_SECRET": "configured-secret",
                },
                clear=False,
            ),
        ):
            result = self.facade.prepare()
        self.assertTrue(result["prepared"])
        self.assertFalse(result["available"])
        self.assertFalse(result["networkOrderPostAllowed"])
        self.assertTrue(result["officialServerProcessOnly"])
        self.assertFalse(result["standaloneProductionLauncherAllowed"])
        self.assertEqual(1, prepare.call_count)

    def test_start_approves_server_candidate_and_exposes_no_raw_authority(self) -> None:
        self.facade._secret = b"x" * 48
        store = Mock()
        store.candidate_status.return_value = {
            "approval_id": "binance-functional-approval-00000001",
            "permit_id": "functional-permit-0000000000000001",
            "permit_hash": "a" * 64,
            "account_fingerprint": "b" * 64,
        }
        self.facade._store = store
        with patch.object(
            state_bridge,
            "start_binance_spot_functional_backend",
            return_value={"ok": True, "sessionId": "session-1"},
        ) as start:
            result = self.facade.start(
                "binance-functional-approval-00000001"
            )
        self.assertTrue(result["ok"])
        approval = store.approve_issued_candidate.call_args.kwargs[
            "approval_attestation"
        ]
        self.assertTrue(self.facade._verify_approval_record(approval))
        command = start.call_args.args[0]
        self.assertEqual(
            {"approvalId", "operatorConfirmation"}, set(command)
        )
        self.assertNotIn("permit", str(command).lower())
        self.assertNotIn("capability", str(command).lower())

    def test_dispatch_lease_requires_exact_active_account_and_application(self) -> None:
        store = Mock()
        store.authority_pointer.return_value = {
            "state": "ACTIVE",
            "session_id": "session-1",
            "account_fingerprint": "c" * 64,
        }
        self.facade._store = store

        @contextmanager
        def exact_shared_boundary(**_kwargs):
            def read():
                pointer = store.authority_pointer()
                active = bool(
                    pointer
                    and pointer.get("state") == "ACTIVE"
                    and pointer.get("session_id") == "session-1"
                )
                return {
                    "active": active,
                    "functionalAccountFingerprint": "c" * 64,
                    "ordinaryRoutesClosed": True,
                    "applicationInstanceLeaseHeld": True,
                }

            yield read

        with (
            patch.object(
                state_bridge,
                "live_trader_instance_lease_status",
                return_value={"acquired": True, "ownerPid": 77},
            ),
            patch.object(
                state_bridge,
                "binance_api_key_fingerprint",
                return_value="c" * 64,
            ),
            patch.object(
                state_bridge,
                "functional_binance_final_mutation_boundary",
                exact_shared_boundary,
            ),
            self.facade.dispatch_lease(
                session_id="session-1", claim_id="claim-1"
            ) as reader,
        ):
            self.assertTrue(reader()["active"])
            store.authority_pointer.return_value["session_id"] = "other"
            self.assertFalse(reader()["active"])

    def test_durable_provider_gate_requires_schema_and_path_identity_pins(
        self,
    ) -> None:
        pin = {
            "schemaVersion": "binance-account-exclusivity-verifier-pin/v1",
            "verifierId": "binance-verifier-0001",
            "keyId": "binance-authority-key-0001",
            "algorithm": "ED25519_RFC8032_SHA512",
            "verifierType": "PINNED_ED25519_DURABLE_BINANCE_EXCLUSIVITY_V1",
            "verifierCodeSha256": "a" * 64,
            "verifierConfigSha256": "b" * 64,
            "keyFingerprintSha256": "c" * 64,
            "authorityPinned": True,
        }

        class Verifier:
            @staticmethod
            def identity():
                return dict(pin)

        class Reader:
            def __init__(self) -> None:
                self.value = {
                    "injectionReady": True,
                    "asymmetricPublicKeyOnly": True,
                    "durableProofSource": True,
                    "durableRequestOutbox": True,
                    "durableConsumerCursor": True,
                    "restartVerifiable": True,
                    "signingPrimitivePresent": False,
                    "networkOrderPostAllowed": False,
                }

            def __call__(self, **_kwargs):
                raise AssertionError("gate snapshot must not read a proof")

            def status(self):
                return dict(self.value)

        reader = Reader()
        root = Path(self.temporary.name)
        facade = BinanceSpotFunctionalStateFacade(
            database_path=root / "provider-gate.sqlite3",
            publication_proof_path=self.proof,
            data_root=root,
            server_secret_path=root / "provider-gate.key",
            ordinary_routes_closed_reader=lambda: True,
            account_exclusivity_proof_reader=reader,
            account_exclusivity_verifier=Verifier(),
            account_exclusivity_verifier_pin=pin,
            account_identity_fingerprint="d" * 64,
        )
        self.assertFalse(
            facade._first_live_gate_snapshot()[
                "accountExclusivityDurableProviderReady"
            ]
        )
        reader.value.update(
            {
                "cursorSchemaFingerprint": (
                    BINANCE_SPOT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT
                ),
                "cursorPathIdentityPinned": True,
            }
        )
        self.assertTrue(
            facade._first_live_gate_snapshot()[
                "accountExclusivityDurableProviderReady"
            ]
        )


if __name__ == "__main__":
    unittest.main()
