from __future__ import annotations

from types import SimpleNamespace
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
            }.issubset(files)
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
