from __future__ import annotations

import unittest
from unittest.mock import patch

from live_trader import state


class CryptoFirstLiveReprepareTest(unittest.TestCase):
    def setUp(self) -> None:
        self.owner = {
            "application": {
                "scopeHash": "a" * 64,
                "ownerPid": 1234,
                "acquiredAt": "2026-08-15T00:00:00.000000Z",
            },
            "lanes": {
                "UPBIT": {
                    "accountFingerprint": "b" * 64,
                    "ownerIdentity": {"pid": 1234},
                },
                "BINANCE_SPOT": {
                    "accountFingerprint": "c" * 64,
                    "ownerIdentity": {"pid": 1234},
                },
            },
        }
        self.crypto = {
            "ok": True,
            "prepared": True,
            "networkOrderPostAllowed": False,
        }
        self.upbit = {
            "ok": True,
            "prepared": True,
            "available": False,
            "networkOrderPostAllowed": False,
        }
        self.binance = {
            "ok": True,
            "prepared": True,
            "available": False,
            "networkOrderPostAllowed": False,
        }

    def test_exact_empty_command_rereads_both_without_mutation(self) -> None:
        with (
            patch.object(
                state,
                "_crypto_first_live_reprepare_owner_snapshot",
                side_effect=(self.owner, self.owner),
            ),
            patch.object(
                state,
                "_crypto_first_live_reprepare_hold_reason",
                return_value="",
            ),
            patch.object(
                state,
                "prepare_crypto_first_live_coordinator_state",
                return_value=self.crypto,
            ) as crypto,
            patch.object(
                state,
                "prepare_upbit_functional_backend_state",
                return_value=self.upbit,
            ) as upbit,
            patch.object(
                state,
                "prepare_binance_spot_functional_backend_state",
                return_value=self.binance,
            ) as binance,
        ):
            result = state.reprepare_crypto_first_live_functional_state({})
        self.assertTrue(result["ok"])
        self.assertTrue(result["ownerIdentityMaintained"])
        self.assertTrue(result["startupAuditRerun"])
        self.assertEqual(0, result["networkRequestCount"])
        self.assertEqual(0, result["orderMutationCount"])
        self.assertEqual(0, result["candidateMutationCount"])
        self.assertEqual(0, result["approvalMutationCount"])
        self.assertFalse(result["networkOrderPostAllowed"])
        crypto.assert_called_once_with()
        upbit.assert_called_once_with()
        binance.assert_called_once_with()

    def test_nonempty_payload_is_rejected_before_any_prepare(self) -> None:
        with patch.object(
            state, "prepare_crypto_first_live_coordinator_state"
        ) as prepare:
            result = state.reprepare_crypto_first_live_functional_state(
                {"lane": "UPBIT"}
            )
        self.assertFalse(result["ok"])
        self.assertIn("fields-not-exact", result["reason"])
        prepare.assert_not_called()

    def test_active_lane_rejects_before_reread(self) -> None:
        with (
            patch.object(
                state,
                "_crypto_first_live_reprepare_owner_snapshot",
                return_value=self.owner,
            ),
            patch.object(
                state,
                "_crypto_first_live_reprepare_hold_reason",
                return_value=(
                    "crypto-first-live-reprepare-upbit-lifecycle-active"
                ),
            ),
            patch.object(
                state, "prepare_crypto_first_live_coordinator_state"
            ) as prepare,
        ):
            result = state.reprepare_crypto_first_live_functional_state({})
        self.assertFalse(result["ok"])
        self.assertIn("lifecycle-active", result["reason"])
        prepare.assert_not_called()

    def test_owner_change_fails_closed_after_status_only_prepare(self) -> None:
        changed = {
            **self.owner,
            "application": {
                **self.owner["application"],
                "ownerPid": 9999,
            },
        }
        with (
            patch.object(
                state,
                "_crypto_first_live_reprepare_owner_snapshot",
                side_effect=(self.owner, changed),
            ),
            patch.object(
                state,
                "_crypto_first_live_reprepare_hold_reason",
                return_value="",
            ),
            patch.object(
                state,
                "prepare_crypto_first_live_coordinator_state",
                return_value=self.crypto,
            ),
            patch.object(
                state,
                "prepare_upbit_functional_backend_state",
                return_value=self.upbit,
            ),
            patch.object(
                state,
                "prepare_binance_spot_functional_backend_state",
                return_value=self.binance,
            ),
        ):
            result = state.reprepare_crypto_first_live_functional_state({})
        self.assertFalse(result["ok"])
        self.assertFalse(result["ownerIdentityMaintained"])
        self.assertFalse(result["networkOrderPostAllowed"])


if __name__ == "__main__":
    unittest.main()
