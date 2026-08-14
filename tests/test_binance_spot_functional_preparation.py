from __future__ import annotations

from contextlib import ExitStack
from decimal import Decimal
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_trader import binance_spot_functional_backend as backend
from live_trader.binance_spot_continuous_functional import (
    BinanceSpotFunctionalError,
    ExactBinding,
    ExactPermit,
    owner_metrics,
)
from live_trader.binance_spot_functional_backend import (
    binance_spot_functional_hold_preparation_status,
    issue_binance_spot_functional_permit,
)
from live_trader.binance_spot_functional_bootstrap import (
    default_binance_spot_functional_code_paths,
    default_binance_spot_functional_code_hash,
)
from live_trader.binance_spot_functional_preparation import (
    HOLD_PREPARATION_SCHEMA_VERSION,
)
from tests.test_binance_spot_continuous_functional import (
    ACCOUNT_FINGERPRINT,
    binding,
)


class BinanceSpotFunctionalPreparationTests(unittest.TestCase):
    def test_hold_status_verifies_internal_contract_but_never_releases_post(
        self,
    ) -> None:
        status = binance_spot_functional_hold_preparation_status()

        self.assertEqual(
            HOLD_PREPARATION_SCHEMA_VERSION, status["schemaVersion"]
        )
        self.assertTrue(status["preparedPrerequisites"])
        self.assertTrue(status["holdEnforced"])
        self.assertFalse(status["rootIntegrationReleased"])
        self.assertFalse(status["releaseAvailable"])
        self.assertFalse(status["candidateIssuanceAllowed"])
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertTrue(all(status["verifiedHooks"].values()))
        self.assertEqual(
            default_binance_spot_functional_code_hash(),
            status["productionCodeHash"],
        )
        self.assertIn(
            "binance_spot_functional_preparation.py",
            {path.name for path in default_binance_spot_functional_code_paths()},
        )
        contract = dict(status["preparationContract"])
        calculated = hashlib.sha256(
            json.dumps(
                contract,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(calculated, status["preparationContractHash"])
        self.assertEqual(7200, contract["activeDurationSeconds"])
        self.assertEqual(10800, contract["cleanupDeadlineFromActivationSeconds"])
        self.assertEqual("IMMUTABLE_BASELINE", contract["preexistingBaseBalancePolicy"])
        self.assertEqual("SESSION_OWNED_DELTA_ONLY", contract["cleanupQuantityPolicy"])
        self.assertFalse(contract["promotionEligible"])

    def test_root_release_latch_blocks_full_and_first_live_composites(self) -> None:
        full_flags = {
            "BINANCE_SPOT_FUNCTIONAL_BACKEND_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_STATE_SERVER_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_FIRST_LIVE_BOOTSTRAP_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_ORDINARY_FENCE_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_EMERGENCY_FENCE_AVAILABLE": True,
            "BINANCE_SPOT_FUNCTIONAL_EXCLUSIVE_ACCOUNT_AVAILABLE": True,
        }
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    backend, "composite_production_available", return_value=True
                )
            )
            for name, value in full_flags.items():
                stack.enter_context(patch.object(backend, name, value))
            self.assertFalse(
                backend.BINANCE_SPOT_FUNCTIONAL_ROOT_INTEGRATION_RELEASED
            )
            self.assertFalse(backend.binance_spot_functional_composite_available())

        first_live_flags = dict(full_flags)
        first_live_flags["BINANCE_SPOT_FUNCTIONAL_REAL_E2E_AVAILABLE"] = False
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    backend, "composite_production_available", return_value=True
                )
            )
            for name, value in first_live_flags.items():
                stack.enter_context(patch.object(backend, name, value))
            self.assertFalse(
                backend.binance_spot_first_live_bootstrap_available()
            )

    def test_server_issued_permit_is_activation_relative_exactly_7200_seconds(
        self,
    ) -> None:
        now = 1_800_000_000.0
        payload = issue_binance_spot_functional_permit(
            binding=ExactBinding.parse(binding()), now_epoch=now
        )
        parsed = ExactPermit.parse(payload, now_epoch=now)

        self.assertEqual(7200, parsed.expires_epoch - parsed.issued_epoch)
        self.assertEqual(10800, parsed.cleanup_deadline_epoch - parsed.issued_epoch)
        self.assertTrue(parsed.activation_reseal_required)
        self.assertTrue(parsed.exclusive_account_required)
        self.assertFalse(payload["futuresAllowed"])
        self.assertFalse(payload["marginAllowed"])
        self.assertFalse(payload["withdrawalAllowed"])

    def test_owner_metrics_preserve_existing_btc_and_cleanup_never_uses_it(
        self,
    ) -> None:
        session_id = "bnsft-preparation-owner-delta-0001"
        prefix = "ftb-" + hashlib.sha256(session_id.encode()).hexdigest()[:12] + "-"
        baseline = Decimal("0.00100000")
        owned = Decimal("0.00010000")
        fill = {
            "clientOrderId": prefix + "b",
            "tradeId": "owned-trade-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": str(owned),
            "quoteQuantity": "6",
            "commission": "0",
            "commissionAsset": "USDT",
            "feeQuoteValue": "0",
            "feeQuoteValueExact": True,
        }
        truth = SimpleNamespace(
            fills=(fill,),
            base_total=baseline + owned,
            mark_price=Decimal("60000"),
            cleanup_recovery_only=False,
            external_activity_absent=True,
            fee_quote_valuation_complete=True,
        )
        metrics = owner_metrics(
            truth, session_id=session_id, baseline_base=baseline
        )
        self.assertEqual(owned, metrics["ownedQuantity"])

        recovery = SimpleNamespace(
            **{
                **truth.__dict__,
                "cleanup_recovery_only": True,
                "base_total": baseline + owned - Decimal("0.00000001"),
            }
        )
        with self.assertRaisesRegex(
            BinanceSpotFunctionalError, "preserve the pre-existing BTC baseline"
        ):
            owner_metrics(
                recovery,
                session_id=session_id,
                baseline_base=baseline,
                allow_cleanup_recovery=True,
            )


if __name__ == "__main__":
    unittest.main()
