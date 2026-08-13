from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader.kis_domestic_functional_get_client import (
    KisDomesticFunctionalGetClient,
)
from live_trader.kis_domestic_functional_truth import (
    KisDomesticFunctionalTruthReader,
)
from live_trader.kis_domestic_signed_get_preflight import (
    AUTHORITY_SECRET_NAME,
    EXECUTION_ENV_GATE,
    KisDomesticSignedGetPreflightBlocked,
    KisDomesticSignedGetPreflightRunner,
    load_kis_signed_get_authority,
    main,
    provision_durable_kis_signed_get_authority,
    signed_get_preflight_plan,
    write_redacted_signed_get_evidence,
)
from tests.test_kis_domestic_functional_truth import (
    ACCOUNT_FINGERPRINT,
    KST,
    _Client,
    _fixture,
)
from datetime import date, datetime


SERVER_KEY = b"P" * 48
CANO = "12345678"
PRODUCT = "01"
APP_KEY = "runner-app-key-secret-marker"
APP_SECRET = "runner-app-secret-marker"
TOKEN = "runner-token-secret-marker"


class _Store:
    def __init__(self, value: str = "") -> None:
        self.value = value
        self.set_calls = []

    def get(self, key: str) -> str:
        if key != AUTHORITY_SECRET_NAME:
            raise AssertionError("unexpected secret key")
        return self.value

    def set(self, key: str, value: str) -> None:
        if key != AUTHORITY_SECRET_NAME:
            raise AssertionError("unexpected secret key")
        self.set_calls.append((key, value))
        self.value = value


def _runner():
    store = _Store("v1:" + SERVER_KEY.hex())
    authority = load_kis_signed_get_authority(store=store)
    fixture = _fixture()
    fixture["FHKST01010100"] = [
        {
            "statusCode": 200,
            "trCont": "",
            "body": {
                "rt_cd": "0",
                "output": {"stck_prpr": "80000", "CANO": CANO},
            },
        }
    ]
    transport = _Client(fixture)
    client = KisDomesticFunctionalGetClient(
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        cano=CANO,
        account_product_code=PRODUCT,
        account_fingerprint=ACCOUNT_FINGERPRINT,
        server_authority_key=authority.key,
        server_authority_key_id=authority.key_id,
        server_authority_restart_verifiable=True,
        token_reader=lambda: TOKEN,
        sender=transport.send,
        allow_mock_transport=True,
        min_request_interval_seconds=0,
    )
    reader = KisDomesticFunctionalTruthReader(
        client=client,
        cano=CANO,
        account_product_code=PRODUCT,
        trading_date=date(2026, 8, 13),
        clock=lambda: datetime(2026, 8, 13, 14, 0, tzinfo=KST),
        max_stable_read_seconds=120,
    )
    runner = KisDomesticSignedGetPreflightRunner(
        client=client,
        reader=reader,
        authority=authority,
    )
    return runner, client, reader, transport, authority


class KisDomesticSignedGetPreflightTest(unittest.TestCase):
    def test_plan_is_network_zero_and_exact_seven_get_routes(self) -> None:
        plan = signed_get_preflight_plan()
        self.assertFalse(plan["networkExecuted"])
        self.assertEqual("GET_ONLY", plan["method"])
        self.assertEqual(7, len(plan["routePairs"]))
        self.assertEqual(
            {
                "TTTC8434R",
                "TTTC0081R",
                "TTTC0084R",
                "TTTC8715R",
                "TTTC8708R",
                "CTCA0903R",
                "FHKST01010100",
            },
            {route["trId"] for route in plan["routePairs"]},
        )
        self.assertEqual(0, plan["tradingPostDeleteDispatchCount"])
        self.assertTrue(plan["authenticationTokenIssuanceMayUsePost"])
        self.assertEqual(0, plan["authenticationOauthPostDispatchCount"])
        self.assertTrue(plan["authenticationOauthPostAuthOnly"])

    def test_durable_authority_is_explicitly_provisioned_and_stable_without_leak(self) -> None:
        store = _Store()
        created = provision_durable_kis_signed_get_authority(
            store=store,
            random_bytes=lambda length: b"A" * length,
        )
        loaded = load_kis_signed_get_authority(store=store)
        again = provision_durable_kis_signed_get_authority(store=store)
        self.assertTrue(created["created"])
        self.assertFalse(again["created"])
        self.assertTrue(created["restartVerifiable"])
        self.assertEqual(created["keyIdHash"], loaded.key_id_hash)
        self.assertEqual(1, len(store.set_calls))
        serialized = json.dumps({"created": created, "loaded": repr(loaded)})
        self.assertNotIn((b"A" * 48).hex(), serialized)

    def test_missing_durable_authority_fails_and_ephemeral_is_honestly_labeled(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticSignedGetPreflightBlocked,
            "not-provisioned",
        ):
            load_kis_signed_get_authority(store=_Store())
        ephemeral = load_kis_signed_get_authority(
            store=_Store(),
            allow_ephemeral=True,
            random_bytes=lambda length: b"E" * length,
        )
        self.assertFalse(ephemeral.restart_verifiable)
        self.assertEqual("ephemeral-process", ephemeral.source)
        self.assertNotIn((b"E" * 48).hex(), repr(ephemeral))

    def test_runner_calls_only_baseline_and_quote_and_seals_redacted_evidence(self) -> None:
        runner, client, reader, transport, authority = _runner()
        with patch.object(
            reader,
            "read",
            side_effect=AssertionError("terminal read must not run"),
        ) as terminal_read:
            evidence = runner.run()
        terminal_read.assert_not_called()
        self.assertEqual("SIGNED_GET_ONLY_PREFLIGHT", evidence["mode"])
        self.assertTrue(evidence["authority"]["restartVerifiable"])
        self.assertEqual(authority.key_id_hash, evidence["authority"]["keyIdHash"])
        self.assertEqual(15, evidence["officialRead"]["getRequestCount"])
        self.assertEqual(15, evidence["officialRead"]["authenticationTokenReadCount"])
        self.assertEqual(
            0,
            evidence["officialRead"]["authenticationOauthPostDispatchCount"],
        )
        self.assertFalse(
            evidence["officialRead"]["authenticationOauthPostCountComplete"]
        )
        self.assertTrue(evidence["officialRead"]["authenticationOauthPostAuthOnly"])
        self.assertEqual(0, evidence["officialRead"]["physicalGetAttemptCount"])
        self.assertFalse(
            evidence["officialRead"]["physicalGetAttemptCountComplete"]
        )
        self.assertEqual(0, evidence["officialRead"]["hiddenGetRetryCount"])
        self.assertEqual(0, evidence["officialRead"]["redirectFollowCount"])
        self.assertEqual(0, evidence["officialRead"]["tradingPostDeleteDispatchCount"])
        self.assertFalse(evidence["tradingAuthorityIssued"])
        self.assertFalse(evidence["networkOrderPostAllowed"])
        self.assertFalse(evidence["releaseEvidenceEligible"])
        self.assertFalse(evidence["durableBaselineCasPersisted"])
        self.assertEqual(15, len(transport.calls))
        self.assertEqual(0, transport.post_calls)
        self.assertEqual(
            {
                "TTTC8434R",
                "TTTC0081R",
                "TTTC0084R",
                "TTTC8715R",
                "TTTC8708R",
                "CTCA0903R",
                "FHKST01010100",
            },
            {call["tr_id"] for call in transport.calls},
        )
        body = dict(evidence)
        signature = body.pop("serverAuthoritySignature")
        evidence_hash = body.pop("evidenceHash")
        self.assertEqual(evidence_hash, __import__("hashlib").sha256(
            json.dumps(body, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest())
        self.assertTrue(client.verify_capture_envelope(body, signature))
        serialized = json.dumps(evidence, ensure_ascii=False, sort_keys=True)
        for secret in (CANO, APP_KEY, APP_SECRET, TOKEN, SERVER_KEY.hex()):
            self.assertNotIn(secret, serialized)
        self.assertNotIn("rawCaptures", serialized)
        self.assertNotIn("normalized", serialized)
        self.assertNotIn("Bearer ", serialized)
        self.assertNotIn("?", serialized)

    def test_tampered_pagination_or_quote_signature_is_rejected(self) -> None:
        runner, _client, reader, _transport, _authority = _runner()
        original_baseline = reader.read_preactivation_baseline
        baseline = original_baseline()
        baseline["rawCaptures"][0]["endpoints"]["balance"]["pages"][-1][
            "continuationReceived"
        ] = "M"
        with patch.object(reader, "read_preactivation_baseline", return_value=baseline):
            with self.assertRaises(KisDomesticSignedGetPreflightBlocked):
                runner.run()

        runner, _client, reader, _transport, _authority = _runner()
        quote = reader.read_fresh_quote_preflight()
        quote["priceKrw"] = "1"
        with patch.object(reader, "read_fresh_quote_preflight", return_value=quote):
            with self.assertRaisesRegex(
                KisDomesticSignedGetPreflightBlocked,
                "quote-hash-mismatch",
            ):
                runner.run()

    def test_output_writer_contains_only_preverified_evidence(self) -> None:
        runner, client, _reader, _transport, _authority = _runner()
        evidence = runner.run()
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "nested" / "evidence.json"
            result = write_redacted_signed_get_evidence(
                evidence,
                target,
                client=client,
            )
            self.assertEqual(target.resolve(), result)
            loaded = json.loads(target.read_text(encoding="utf-8"))
        self.assertEqual(evidence, loaded)
        self.assertNotIn(CANO, json.dumps(loaded, ensure_ascii=False))

    def test_output_writer_rejects_hash_or_signature_tampering(self) -> None:
        runner, client, _reader, _transport, _authority = _runner()
        evidence = runner.run()
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "evidence.json"
            tampered_hash = dict(evidence)
            tampered_hash["releaseEvidenceEligible"] = True
            with self.assertRaisesRegex(
                KisDomesticSignedGetPreflightBlocked,
                "evidence-hash-invalid",
            ):
                write_redacted_signed_get_evidence(
                    tampered_hash,
                    target,
                    client=client,
                )
            self.assertFalse(target.exists())

            tampered_signature = dict(evidence)
            tampered_signature["releaseEvidenceEligible"] = True
            unsigned = dict(tampered_signature)
            unsigned.pop("serverAuthoritySignature")
            unsigned.pop("evidenceHash")
            tampered_signature["evidenceHash"] = __import__("hashlib").sha256(
                json.dumps(
                    unsigned,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            with self.assertRaisesRegex(
                KisDomesticSignedGetPreflightBlocked,
                "evidence-signature-invalid",
            ):
                write_redacted_signed_get_evidence(
                    tampered_signature,
                    target,
                    client=client,
                )
            self.assertFalse(target.exists())

    def test_cli_without_execute_is_plan_only_and_execute_gate_blocks_before_factory(self) -> None:
        with patch("builtins.print") as output:
            self.assertEqual(0, main([]))
        plan = json.loads(output.call_args.args[0])
        self.assertFalse(plan["networkExecuted"])
        with patch.dict(os.environ, {EXECUTION_ENV_GATE: "false"}, clear=False), patch(
            "live_trader.kis_domestic_signed_get_preflight.build_production_signed_get_preflight_runner"
        ) as build:
            with self.assertRaisesRegex(
                KisDomesticSignedGetPreflightBlocked,
                "environment-gate-disabled",
            ):
                main(["--execute-signed-get"])
        build.assert_not_called()


if __name__ == "__main__":
    unittest.main()
