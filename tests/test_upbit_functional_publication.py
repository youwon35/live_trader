from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from live_trader.upbit_continuous_functional import UpbitFunctionalBlocked
from live_trader.upbit_functional_publication import (
    load_upbit_functional_selection,
)


ACCOUNT = "c" * 64
ROOT = Path(__file__).resolve().parents[3]
PROOF = (
    ROOT
    / "apps"
    / "backtester"
    / "tmp"
    / "crypto-dual-5m-publication-proof-v1.json"
)


class UpbitFunctionalPublicationTest(unittest.TestCase):
    def test_actual_saved_krw_btc_strategy_and_instance_reverify(self) -> None:
        selection = load_upbit_functional_selection(
            PROOF, account_fingerprint=ACCOUNT
        )
        self.assertEqual("KRW-BTC", selection["symbol"])
        self.assertEqual(
            "UPBIT_KRW_SPOT_CONTINUOUS", selection["executionRoute"]
        )
        self.assertNotEqual(
            selection["strategyArtifactHash"],
            selection["strategyArtifactFileSha256"],
        )
        self.assertNotEqual(
            selection["strategyInstanceHash"],
            selection["strategyInstanceFileSha256"],
        )
        self.assertFalse(selection["publishedPromotionEligible"])

    def test_tampered_proof_cannot_select_an_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "proof.json"
            payload = json.loads(PROOF.read_text(encoding="utf-8"))
            payload["naturalSignalsOnly"] = False
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(
                UpbitFunctionalBlocked, "proof-policy-invalid"
            ):
                load_upbit_functional_selection(
                    path, account_fingerprint=ACCOUNT
                )


if __name__ == "__main__":
    unittest.main()
