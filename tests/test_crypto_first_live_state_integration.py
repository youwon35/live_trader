from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
import unittest
from unittest import mock

from live_trader import state
from live_trader.server import prepare_server_state


class CryptoFirstLiveStateIntegrationTests(unittest.TestCase):
    def test_missing_runtime_never_claims_global_entry_revocation(self) -> None:
        with mock.patch.object(state, "_CRYPTO_FIRST_LIVE_RUNTIME", None):
            result = state._revoke_crypto_first_live_entry_before_cleanup(
                "hostile-runtime-loss"
            )
        self.assertFalse(result["ok"])
        self.assertFalse(result["entryAuthorityRevoked"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertIn("unverifiable", result["reason"])

    def test_missing_application_lease_blocks_candidate_and_start_mutations(
        self,
    ) -> None:
        confirmation = {
            "challengeId": "challenge-0001",
            "token": "token-0001",
            "typedPhrase": "LIVE ABCD",
        }
        upbit_store = mock.Mock()
        binance_facade = mock.Mock()
        upbit_consume = mock.Mock()
        binance_consume = mock.Mock()
        with (
            mock.patch.object(
                state,
                "live_trader_instance_lease_status",
                return_value={"acquired": False},
            ),
            mock.patch.object(
                state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", upbit_store
            ),
            mock.patch.object(
                state,
                "_binance_spot_functional_facade",
                return_value=binance_facade,
            ),
            mock.patch.object(
                state,
                "_consume_upbit_functional_operator_confirmation",
                upbit_consume,
            ),
            mock.patch.object(
                state,
                "_consume_binance_spot_functional_confirmation",
                binance_consume,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "lease-required"):
                state._preissue_upbit_functional_permit_candidate()
            with self.assertRaisesRegex(RuntimeError, "lease-required"):
                state._preissue_binance_spot_functional_candidate()
            upbit_start = state.start_upbit_functional_backend_state(
                {
                    "approvalId": "upbit-approval-0001",
                    "operatorConfirmation": confirmation,
                }
            )
            binance_start = state.start_binance_spot_functional_backend_state(
                {
                    "approvalId": "binance-approval-0001",
                    "operatorConfirmation": confirmation,
                }
            )
        self.assertFalse(upbit_start["ok"])
        self.assertFalse(binance_start["ok"])
        self.assertIn("lease-required", upbit_start["reason"])
        self.assertIn("lease-required", binance_start["reason"])
        upbit_store.assert_not_called()
        binance_facade.assert_not_called()
        upbit_consume.assert_not_called()
        binance_consume.assert_not_called()

    def test_official_server_holds_before_upbit_candidate_creation(self) -> None:
        runtime = mock.Mock()
        runtime.status.return_value = {
            "coordinator": {"phase": "IDLE"},
            "processMemoryOwnerMatches": False,
        }
        runtime.production_entry_released.return_value = False
        prepared = {
            "prepared": True,
            "startReservationCompositionConnected": False,
            "heartbeatCompositionConnected": False,
            "terminalFinalizeCompositionConnected": False,
        }
        with (
            mock.patch.object(
                state,
                "live_trader_instance_lease_status",
                return_value={"acquired": True},
            ),
            mock.patch.object(state, "_CRYPTO_FIRST_LIVE_RUNTIME", runtime),
            mock.patch.object(
                state, "_CRYPTO_FIRST_LIVE_PREPARE_STATUS", prepared
            ),
            mock.patch.object(
                state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE"
            ) as store,
            self.assertRaisesRegex(RuntimeError, "lifecycle-not-composed"),
        ):
            state._preissue_upbit_functional_permit_candidate()
        store.active_pointer.assert_not_called()

    def test_official_server_holds_before_binance_candidate_creation(self) -> None:
        runtime = mock.Mock()
        runtime.status.return_value = {
            "coordinator": {"phase": "IDLE"},
            "processMemoryOwnerMatches": False,
        }
        runtime.production_entry_released.return_value = False
        prepared = {
            "prepared": True,
            "startReservationCompositionConnected": False,
            "heartbeatCompositionConnected": False,
            "terminalFinalizeCompositionConnected": False,
        }
        facade = mock.Mock()
        with (
            mock.patch.object(
                state,
                "live_trader_instance_lease_status",
                return_value={"acquired": True},
            ),
            mock.patch.object(state, "_CRYPTO_FIRST_LIVE_RUNTIME", runtime),
            mock.patch.object(
                state, "_CRYPTO_FIRST_LIVE_PREPARE_STATUS", prepared
            ),
            mock.patch.object(
                state, "_binance_spot_functional_facade", return_value=facade
            ),
            self.assertRaisesRegex(RuntimeError, "lifecycle-not-composed"),
        ):
            state._preissue_binance_spot_functional_candidate()
        facade.preissue.assert_not_called()

    def test_upbit_manifest_seals_all_shared_first_live_sources(self) -> None:
        files = state._upbit_functional_code_manifest()["files"]
        self.assertTrue(
            {
                "live_trader/upbit_account_exclusivity.py",
                "live_trader/crypto_first_live_coordinator.py",
                "live_trader/crypto_first_live_high_water.py",
                "live_trader/crypto_first_live_runtime.py",
                "live_trader/crypto_first_live_supervised_anchor.py",
                "live_trader/crypto_first_live_supervised_release.py",
            }.issubset(files)
        )

    def test_supervised_candidate_and_consumption_remain_non_enabling(
        self,
    ) -> None:
        store = mock.Mock()
        store.issue.return_value = {
            "approvalId": "supervised-approval-0001",
            "networkCapabilityOpen": False,
        }
        store.consume.return_value = {
            "schemaVersion": (
                "crypto-first-live-supervised-user-approval-receipt/v1"
            ),
            "approvalId": "supervised-approval-0001",
            "consumed": True,
        }
        with (
            mock.patch.object(
                state,
                "live_trader_instance_lease_status",
                return_value={"acquired": True},
            ),
            mock.patch.object(
                state,
                "_CRYPTO_FIRST_LIVE_SUPERVISED_APPROVAL_STORE",
                store,
            ),
        ):
            candidate = state.issue_crypto_first_live_supervised_approval(
                {"schemaVersion": "test-request"}
            )
            receipt = state.consume_crypto_first_live_supervised_approval(
                {"schemaVersion": "test-confirmation"}
            )
        self.assertFalse(candidate["networkCapabilityOpen"])
        self.assertFalse(receipt["networkCapabilityOpen"])
        self.assertFalse(receipt["promotionEligible"])

    def test_supervised_anchor_projection_cannot_claim_formal_worm(self) -> None:
        with mock.patch.object(
            state,
            "_CRYPTO_FIRST_LIVE_SUPERVISED_ANCHOR_READER",
            lambda _request: {"formalWorm": True},
        ), self.assertRaisesRegex(RuntimeError, "projection-invalid"):
            state.crypto_first_live_supervised_audit_anchor(
                {"schemaVersion": "hostile-request"}
            )

    def test_inert_heartbeat_request_is_built_only_from_server_state(
        self,
    ) -> None:
        coordinator = {
            "phase": "APPROVED_INERT",
            "lane": "UPBIT",
            "runId": "crypto-first-live-run-0001",
            "sessionId": "session-upbit-0001",
            "permitId": "permit-upbit-0001",
            "permitHash": "a" * 64,
            "accountFingerprint": "b" * 64,
            "baselineHash": "c" * 64,
            "codeHash": "d" * 64,
            "ownerIdentityHash": "e" * 64,
            "revision": 7,
        }
        runtime = mock.Mock()
        runtime.status.return_value = {"coordinator": coordinator}
        runtime.heartbeat_inert.return_value = {
            **coordinator,
            "networkCapabilityOpen": False,
            "entryAuthorityOpen": False,
            "networkOrderPostAllowed": False,
        }
        with mock.patch.object(
            state, "_CRYPTO_FIRST_LIVE_RUNTIME", runtime
        ):
            result = state.heartbeat_crypto_first_live_inert_state()
        self.assertEqual("APPROVED_INERT", result["phase"])
        runtime.heartbeat_inert.assert_called_once_with(
            {
                "schemaVersion": (
                    "crypto-first-live-runtime-inert-heartbeat/v1"
                ),
                "lane": "UPBIT",
                "runId": "crypto-first-live-run-0001",
                "sessionId": "session-upbit-0001",
                "permitId": "permit-upbit-0001",
                "permitHash": "a" * 64,
                "accountFingerprint": "b" * 64,
                "baselineHash": "c" * 64,
                "codeHash": "d" * 64,
                "ownerIdentityHash": "e" * 64,
                "expectedRevision": 7,
            }
        )

    def test_inert_heartbeat_rejects_any_open_capability_projection(
        self,
    ) -> None:
        runtime = mock.Mock()
        runtime.status.return_value = {
            "coordinator": {
                "phase": "APPROVED_INERT",
                "lane": "UPBIT",
                "runId": "crypto-first-live-run-0001",
                "sessionId": "session-upbit-0001",
                "permitId": "permit-upbit-0001",
                "permitHash": "a" * 64,
                "accountFingerprint": "b" * 64,
                "baselineHash": "c" * 64,
                "codeHash": "d" * 64,
                "ownerIdentityHash": "e" * 64,
                "revision": 7,
            }
        }
        runtime.heartbeat_inert.return_value = {
            "phase": "APPROVED_INERT",
            "networkCapabilityOpen": True,
            "entryAuthorityOpen": False,
            "networkOrderPostAllowed": False,
        }
        with (
            mock.patch.object(
                state, "_CRYPTO_FIRST_LIVE_RUNTIME", runtime
            ),
            self.assertRaisesRegex(RuntimeError, "opened-capability"),
        ):
            state.heartbeat_crypto_first_live_inert_state()

    def test_upbit_authority_handshake_wait_is_bounded_and_automatic(
        self,
    ) -> None:
        injection = SimpleNamespace(
            status=lambda: {
                "injectionReady": True,
                "durable": True,
                "restartVerifiable": True,
                "cursorPathIdentityPinned": True,
                "cursorSchemaFingerprint": (
                    state.__dict__.get(
                        "UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT",
                        "",
                    )
                ),
                "networkOrderPostAllowed": False,
            }
        )
        from live_trader.upbit_account_exclusivity import (
            UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT,
        )

        injection.status = lambda: {
            "injectionReady": True,
            "durable": True,
            "restartVerifiable": True,
            "cursorPathIdentityPinned": True,
            "cursorSchemaFingerprint": (
                UPBIT_ACCOUNT_EXCLUSIVITY_CURSOR_SCHEMA_FINGERPRINT
            ),
            "networkOrderPostAllowed": False,
        }
        builder = mock.Mock(return_value=injection)
        configured = {
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_PROOF_DIR": "C:/proof",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_CURSOR_DB": "C:/cursor.sqlite3",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_PUBLIC_KEY": "C:/public.pem",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_VERIFIER_PIN": "C:/pin.json",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_VERIFIER_ID": "verifier-0001",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_KEY_ID": "key-00000001",
            "LIVE_TRADER_UPBIT_EXCLUSIVITY_AUTHORITY_JOURNAL_ID": (
                "journal-0001"
            ),
        }
        with (
            mock.patch.dict(state.os.environ, configured, clear=False),
            mock.patch.object(
                state,
                "env_value",
                side_effect=lambda name: {
                    "UPBIT_ACCESS_KEY": "access-key",
                    "UPBIT_SECRET_KEY": "secret-key",
                }.get(name, ""),
            ),
            mock.patch(
                "live_trader.upbit_account_exclusivity."
                "build_upbit_account_exclusivity_injection",
                builder,
            ),
        ):
            _value, status = (
                state._prepare_upbit_account_exclusivity_injection(
                    account_fingerprint="a" * 64,
                    owner_identity={"owner": "server-owned"},
                )
            )
        self.assertTrue(status["ready"])
        self.assertEqual(8.0, builder.call_args.kwargs["proof_wait_seconds"])

    def test_upbit_prepare_wires_exact_global_lifecycle_callbacks(self) -> None:
        runtime = mock.Mock()
        injection = SimpleNamespace(
            proof_reader=mock.Mock(),
            verifier=mock.Mock(),
            verifier_pin={"schemaVersion": "test-pin/v1"},
        )
        prepared_backend = mock.Mock(
            return_value={"status": {"prepared": True}}
        )
        with (
            mock.patch.object(
                Path, "is_file", autospec=True, return_value=True
            ),
            mock.patch.object(
                state,
                "env_value",
                side_effect=lambda name: "configured-" + name,
            ),
            mock.patch(
                "live_trader.upbit_functional_transport."
                "resolve_upbit_functional_base_url"
            ),
            mock.patch(
                "live_trader.upbit_functional_transport."
                "upbit_credential_fingerprint",
                return_value="a" * 64,
            ),
            mock.patch(
                "live_trader.upbit_functional_backend."
                "prepare_upbit_functional_backend",
                prepared_backend,
            ),
            mock.patch.object(
                state,
                "hold_crypto_first_live_account_lease",
                return_value={"acquired": True},
            ),
            mock.patch.object(
                state,
                "crypto_first_live_owner_identity",
                return_value={"kind": "test-owner"},
            ),
            mock.patch.object(
                state,
                "_prepare_upbit_account_exclusivity_injection",
                return_value=(injection, {"ready": True}),
            ),
            mock.patch.object(
                state, "_CRYPTO_FIRST_LIVE_RUNTIME", runtime
            ),
            mock.patch.object(
                state,
                "_UPBIT_FUNCTIONAL_APPROVAL_STORE",
                mock.Mock(),
            ),
            mock.patch.object(
                state, "_UPBIT_FUNCTIONAL_PREPARE_STATUS", {}
            ),
        ):
            state.prepare_upbit_functional_backend_state()
        kwargs = prepared_backend.call_args.kwargs
        self.assertIs(
            runtime.upbit_dispatch_reservation,
            kwargs["global_first_live_dispatch_reserver"],
        )
        self.assertIs(
            runtime.heartbeat, kwargs["global_first_live_heartbeat"]
        )
        self.assertIs(
            runtime.revoke_entry_before_cleanup,
            kwargs["global_first_live_entry_revoker"],
        )
        self.assertIs(
            runtime.finalize_terminal,
            kwargs["global_first_live_terminal_finalizer"],
        )

    def test_upbit_stop_attempts_global_revoke_before_local_cleanup(self) -> None:
        sequence: list[str] = []
        payload = {
            "operatorConfirmation": {
                "challengeId": "challenge-0001",
                "token": "token-0001",
                "typedPhrase": "STOP ABCD",
            }
        }
        with (
            mock.patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value={"prepared": True, "sessionId": "session-1"},
            ),
            mock.patch.object(
                state,
                "_consume_upbit_functional_operator_confirmation",
                return_value={"ok": True},
            ),
            mock.patch.object(
                state,
                "_revoke_crypto_first_live_entry_before_cleanup",
                side_effect=lambda _reason: (
                    sequence.append("global")
                    or {"ok": True, "entryAuthorityRevoked": True}
                ),
            ),
            mock.patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                side_effect=lambda _reason: (
                    sequence.append("broker-latch") or {"ok": True}
                ),
            ),
            mock.patch(
                "live_trader.upbit_functional_backend."
                "stop_upbit_functional_backend",
                side_effect=lambda _command: (
                    sequence.append("broker-cleanup") or {"ok": True}
                ),
            ),
        ):
            result = state.stop_upbit_functional_backend_state(payload)
        self.assertTrue(result["ok"])
        self.assertEqual(
            ["global", "broker-latch", "broker-cleanup"], sequence
        )

    def test_binance_stop_attempts_global_revoke_before_cleanup(self) -> None:
        sequence: list[str] = []
        payload = {
            "operatorConfirmation": {
                "challengeId": "challenge-0001",
                "token": "token-0001",
                "typedPhrase": "STOP ABCD",
            }
        }
        facade = SimpleNamespace(
            stop=lambda: sequence.append("broker-cleanup") or {"ok": True}
        )
        with (
            mock.patch.object(
                state,
                "binance_spot_functional_backend_state_status",
                return_value={"prepared": True, "sessionId": "session-1"},
            ),
            mock.patch.object(
                state,
                "_consume_binance_spot_functional_confirmation",
                return_value={"ok": True},
            ),
            mock.patch.object(
                state,
                "_revoke_crypto_first_live_entry_before_cleanup",
                side_effect=lambda _reason: (
                    sequence.append("global")
                    or {"ok": True, "entryAuthorityRevoked": True}
                ),
            ),
            mock.patch.object(
                state, "_binance_spot_functional_facade", return_value=facade
            ),
        ):
            result = state.stop_binance_spot_functional_backend_state(payload)
        self.assertTrue(result["ok"])
        self.assertEqual(["global", "broker-cleanup"], sequence)


if __name__ == "__main__":
    unittest.main()
