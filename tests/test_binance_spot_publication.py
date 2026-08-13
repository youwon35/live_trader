from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from live_trader.binance_spot_continuous_functional import ExactBinding
from live_trader.binance_spot_publication import (
    BinancePublicationError,
    load_binance_spot_publication_binding,
    verify_binance_spot_publication,
)


WORKSPACE = Path(__file__).resolve().parents[3]
PROOF = (
    WORKSPACE
    / "apps"
    / "backtester"
    / "tmp"
    / "crypto-dual-5m-publication-proof-v1.json"
)


def actual_binding(**updates: object) -> ExactBinding:
    payload: dict[str, object] = {
        "strategyArtifactId": "ft-continuous-crypto-binance-btcusdt-5m-20260813-v1-crypto-dual-5x5m-natural-ma",
        "strategyArtifactHash": "b084be743748d3954db57a05e6c6ec64941bc0f359ec7b3632217f27ec82fc59",
        "artifactFileSha256": "e56b465c00d79c286a5e6e87e737f1261313354995c019a62190d49dde58b2cc",
        "strategyInstanceId": "si-ft-continuous-crypto-binance-btcusdt-5m-20260813-v1-crypto-dual-5x5m-natural-ma",
        "strategyInstanceHash": "9e13343f52be54edaff33e2b667eb4313a6bf08babb4070232d4d58ef780d563",
        "instanceFileSha256": "20bea2a07adeb83eb9592f597a02d4748d4d1b2371cb9c982b54d16fe21de321",
        "publicationProofHash": "10563192d05edc4bea37d94de1087246540cd0ea84fcd7637ecc0811597f0e2a",
        "publicationProofFileSha256": "79dc606fdb24a71d14c1a6b85da1b0fb9625d91466cc9f75cdba52ce4a042215",
        "accountFingerprint": "c" * 64,
        "broker": "BINANCE",
        "venue": "BINANCE_SPOT",
        "asset": "CRYPTO",
        "market": "CRYPTO_SPOT",
        "executionRoute": "BINANCE_SPOT_CONTINUOUS",
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "interval": "5m",
    }
    payload.update(updates)
    return ExactBinding.parse(payload)


class BinanceSpotPublicationTest(unittest.TestCase):
    def test_backend_resolves_unique_active_btcusdt_pair_without_client_selection(self) -> None:
        loaded = load_binance_spot_publication_binding(
            proof_path=PROOF, account_fingerprint="c" * 64
        )
        self.assertEqual(actual_binding(), loaded)

    def test_actual_published_btcusdt_pair_has_distinct_exact_identities(self) -> None:
        binding = actual_binding()
        result = verify_binance_spot_publication(binding, proof_path=PROOF)
        self.assertTrue(result["complete"])
        self.assertNotEqual(
            result["strategyArtifactHash"], result["artifactFileSha256"]
        )
        self.assertNotEqual(
            result["strategyInstanceHash"], result["instanceFileSha256"]
        )
        self.assertEqual(binding.publication_proof_hash, result["publicationProofHash"])

    def test_declared_hash_cannot_be_substituted_for_file_sha(self) -> None:
        with self.assertRaisesRegex(BinancePublicationError, "FileSha256"):
            verify_binance_spot_publication(
                actual_binding(artifactFileSha256="b084be743748d3954db57a05e6c6ec64941bc0f359ec7b3632217f27ec82fc59"),
                proof_path=PROOF,
            )

    def test_tampered_proof_content_is_rejected_before_path_trust(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            clone = json.loads(PROOF.read_text(encoding="utf-8"))
            clone["naturalSignalsOnly"] = False
            path = Path(temporary) / "tampered-proof.json"
            path.write_text(json.dumps(clone), encoding="utf-8")
            with self.assertRaises(BinancePublicationError):
                verify_binance_spot_publication(actual_binding(), proof_path=path)


if __name__ == "__main__":
    unittest.main()
