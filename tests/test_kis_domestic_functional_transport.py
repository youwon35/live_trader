from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone

from live_trader import kis_order_authority as kis_order_authority_module
from live_trader.emergency_stop import (
    _reset_emergency_stop_sticky_for_tests,
    engage_emergency_stop,
)
from live_trader.kis_domestic_functional_capability import (
    DurableKisDomesticFunctionalCapabilityLedger,
)
from live_trader.kis_domestic_functional_mutation import (
    DurableKisDomesticFunctionalMutationJournal,
)
from live_trader.kis_domestic_functional_transport import (
    DurableKisDomesticFunctionalTransport,
    KisDomesticFunctionalTransportBlocked,
    production_entrypoint_status,
)
from live_trader.kis_domestic_functional_production_transport import (
    DisabledKisDomesticFunctionalProductionTransport,
    kis_domestic_functional_callback_code_hash,
)
from live_trader.kis_domestic_functional_get_client import (
    _credential_configuration_hash as get_credential_configuration_hash,
    kis_domestic_functional_account_fingerprint,
)
from live_trader.kis_order_authority import (
    _reset_kis_order_authority_reader_for_tests,
    register_kis_order_authority_reader,
)
from live_trader.program_ledger import ProgramLedger


ACCOUNT = kis_domestic_functional_account_fingerprint("12345678", "01")
APP_KEY = "integration-app-key"
APP_SECRET = "integration-app-secret-value"
ENV_REVISION = 11
KEY = b"isolated-kis-transport-test-key-0001"
CAP_KEY = b"isolated-kis-capability-test-key-01"
REVOKE_KEY = b"isolated-kis-revoke-provider-key-01"
MUTATION_KEY = b"isolated-kis-mutation-test-key-001"
TRUTH_KEY = b"isolated-kis-truth-test-key-000001"
NOW = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


CREDENTIAL = get_credential_configuration_hash(
    app_key=APP_KEY,
    app_secret=APP_SECRET,
    account_fingerprint=ACCOUNT,
)


class KisFunctionalTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.original_kis_authority_reader = (
            kis_order_authority_module._AUTHORITY_READER
        )
        self.original_kis_kill_cancel_journal_path = (
            kis_order_authority_module._KILL_CANCEL_JOURNAL_PATH
        )
        self.addCleanup(self._restore_kis_authority_provider)
        self.old_stop = os.environ.get("LIVE_TRADER_EMERGENCY_STOP_PATH")
        os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = str(
            Path(self.temp.name) / "emergency.json"
        )
        _reset_emergency_stop_sticky_for_tests()
        _reset_kis_order_authority_reader_for_tests()
        self.path = Path(self.temp.name) / "program.sqlite3"
        self.ledger = ProgramLedger(self.path)
        self.mutation = DurableKisDomesticFunctionalMutationJournal(
            program_ledger=self.ledger,
            signer_key=MUTATION_KEY,
            signer_key_id="mutation-test-key-v1",
            official_truth_key=TRUTH_KEY,
            official_truth_key_id="truth-test-key-v1",
            clock=lambda: NOW,
        )
        self.capability = DurableKisDomesticFunctionalCapabilityLedger(
            program_ledger=self.ledger,
            signer_key=CAP_KEY,
            signer_key_id="capability-test-key-v1",
            owner_id="state-owned-kis-graph-v1",
            revoke_provider_key=REVOKE_KEY,
            revoke_provider_key_id="external-revoke-test-v1",
            clock=lambda: NOW,
        )
        self.raw_capability = "raw-capability-transport-one"
        self.capability.mint(
            capability_id="kis-capability-transport-one",
            raw_capability=self.raw_capability,
            arm_id="kis-arm-transport-one",
            session_id="kis-session-transport-one",
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            permit_id="permit-transport-one",
            permit_hash="1" * 64,
            code_manifest_hash="2" * 64,
            baseline_hash="3" * 64,
            caps_hash="4" * 64,
            rolling_snapshot_hash="5" * 64,
            heartbeat_binding_hash="6" * 64,
        )
        self.sent: list[dict] = []
        self.sender_entered = threading.Event()
        self.sender_release = threading.Event(); self.sender_release.set()
        self.snapshot = {
            "durableAuthorityReadable": True,
            "functionalAuthorityOpen": True,
            "functionalPhase": "ACTIVE",
            "functionalRevision": 1,
            "stateRevision": 1,
            "ownerEpochId": "owner-epoch-transport-one",
            "ownerEpochHash": "e" * 64,
            "functionalSessionId": "kis-session-transport-one",
            "functionalAccountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "functionalMutationIntent": {},
            "killOrdinaryCancelAllowed": False,
            "killOrdinaryCancelRevision": 0,
            "killOrdinaryCancelIntent": {},
            "applicationInstanceLeaseHeld": True,
            "ordinaryRoutesClosed": True,
            "controlReservation": {},
        }

        def authority_reader():
            return dict(self.snapshot)

        register_kis_order_authority_reader(authority_reader)
        self._seal("claim-transport-one")
        self._set_authority_intent("claim-transport-one")
        self.transport = self._transport()

    def tearDown(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        _reset_emergency_stop_sticky_for_tests()
        if self.old_stop is None:
            os.environ.pop("LIVE_TRADER_EMERGENCY_STOP_PATH", None)
        else:
            os.environ["LIVE_TRADER_EMERGENCY_STOP_PATH"] = self.old_stop

    def _restore_kis_authority_provider(self) -> None:
        _reset_kis_order_authority_reader_for_tests()
        if self.original_kis_authority_reader is not None:
            register_kis_order_authority_reader(
                self.original_kis_authority_reader,
                kill_cancel_journal_path=(
                    self.original_kis_kill_cancel_journal_path
                ),
            )

    def _seal(self, claim: str) -> None:
        self.mutation.seal_request(
            claim_id=claim,
            session_id="kis-session-transport-one",
            operation="NATURAL_BUY",
            endpoint="/uapi/domestic-stock/v1/trading/order-cash",
            tr_id="TTTC0012U",
            side="BUY",
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            authority_revision=1,
            payload={"PDNO": "010140", "ORD_DVSN": "00", "ORD_QTY": "1", "ORD_UNPR": "80000"},
            owned_order_key={"orderDate": "", "organizationNo": "", "orderNo": ""},
        )

    def _set_authority_intent(self, claim: str) -> None:
        row = self.mutation.read(claim)
        sealed = json.loads(row["authority_intent_json"])
        self.snapshot["functionalMutationIntent"] = {
            "operation": sealed["operation"],
            "claimId": sealed["claimId"],
            "ownedOrderKey": sealed["ownedOrderKey"],
            "accountFingerprint": sealed["accountFingerprint"],
            "credentialConfigurationHash": sealed["credentialConfigurationHash"],
            "endpoint": sealed["endpoint"],
            "payloadHash": sealed["payloadHash"],
        }

    def _sender(self, request):
        self.sent.append(dict(request))
        self.sender_entered.set()
        self.sender_release.wait(2)
        return {
            "schemaVersion": "kis-domestic-functional-mock-response/v1",
            "method": "POST",
            "origin": "https://openapi.koreainvestment.com:9443",
            "endpoint": request["endpoint"],
            "trId": request["trId"],
            "effectiveUrl": "https://openapi.koreainvestment.com:9443" + request["endpoint"],
            "requestHash": digest(request),
            "physicalAttemptCount": 1,
            "hiddenRetryCount": 0,
            "redirectFollowCount": 0,
            "statusCode": 200,
            "observedAt": "2026-08-14T04:15:00.000000Z",
            "body": {
                "rt_cd": "0", "msg_cd": "MOCK",
                "output": {
                    "ORD_DT": "20260814", "KRX_FWDG_ORD_ORGNO": "001",
                    "ODNO": "0000000001",
                },
            },
        }

    def _transport(self, sender=None, path=None):
        return DurableKisDomesticFunctionalTransport(
            program_ledger=self.ledger,
            mutation_journal=self.mutation,
            capability_ledger=self.capability,
            signer_key=KEY,
            signer_key_id="transport-test-key-v1",
            owner_id="state-owned-kis-transport-v1",
            mock_sender=sender or self._sender,
            allow_mock_sender=True,
            clock=lambda: NOW,
        )

    def _lease(self):
        return self.capability.authorize(
            raw_capability=self.raw_capability,
            operation="NATURAL_BUY",
            expected_revision=1,
        )

    def test_flags_false_and_mock_sender_must_be_explicit(self) -> None:
        for key in (
            "available", "networkAvailable", "senderAvailable",
            "productionSenderAvailable", "releaseEvidenceAvailable",
        ):
            self.assertFalse(production_entrypoint_status()[key])
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "explicit mock"):
            DurableKisDomesticFunctionalTransport(
                program_ledger=self.ledger, mutation_journal=self.mutation,
                capability_ledger=self.capability, signer_key=KEY,
                signer_key_id="transport-test-key-v1", owner_id="other-owner",
                mock_sender=self._sender, allow_mock_sender=False,
            )

    def test_exact_request_final_authority_and_raw_response_archive(self) -> None:
        result = self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        self.assertEqual(1, result["physicalAttemptCount"])
        self.assertFalse(result["retryAllowed"])
        self.assertEqual(1, len(self.sent))
        request = self.sent[0]
        self.assertEqual([], request["query"])
        self.assertEqual("TTTC0012U", request["headers"]["tr_id"])
        archived = self.transport.read("claim-transport-one")
        self.assertEqual("RESPONSE_ARCHIVED", archived["state"])
        self.assertTrue(archived["response_hash"])
        self.assertEqual("POST_MAY_HAVE_CROSSED", self.mutation.read("claim-transport-one")["state"])

    def test_forged_or_stale_capability_lease_fails_before_sender(self) -> None:
        lease = self._lease(); lease["baselineHash"] = "9" * 64
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "capability lease"):
            self.transport.dispatch(claim_id="claim-transport-one", capability_lease=lease)
        self.assertEqual([], self.sent)

    def test_sealed_payload_or_account_tamper_fails_before_sender(self) -> None:
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_mutation_request SET account_fingerprint=? WHERE claim_id=?",
                ("f" * 64, "claim-transport-one"),
            )
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "sealed mutation"):
            self.transport.dispatch(
                claim_id="claim-transport-one", capability_lease=self._lease()
            )
        self.assertEqual([], self.sent)

    def test_sender_entered_crash_is_retryable_but_pre_post_crash_is_not(self) -> None:
        def early(stage):
            if stage == "AFTER_SENDER_ENTERED":
                raise RuntimeError("crash-early")
        with self.assertRaisesRegex(RuntimeError, "crash-early"):
            self.transport.dispatch(
                claim_id="claim-transport-one", capability_lease=self._lease(),
                crash_hook=early,
            )
        self.assertEqual("SENDER_ENTERED", self.mutation.read("claim-transport-one")["state"])
        retried = self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        self.assertEqual("POST_MAY_HAVE_CROSSED", retried["state"])
        self.assertEqual(
            "RESPONSE_ARCHIVED", self.transport.read("claim-transport-one")["state"]
        )

        claim = "claim-transport-two"; self._seal(claim); self._set_authority_intent(claim)
        def late(stage):
            if stage == "AFTER_PRE_POST_MARKER":
                raise RuntimeError("crash-late")
        with self.assertRaisesRegex(RuntimeError, "crash-late"):
            self.transport.dispatch(claim_id=claim, capability_lease=self._lease(), crash_hook=late)
        self.assertEqual("POST_MAY_HAVE_CROSSED", self.mutation.read(claim)["state"])
        self.assertIn(
            "POST_MAY_HAVE_CROSSED_WITHOUT_RESPONSE",
            self.transport.authority_snapshot()["hazards"],
        )
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "not retryable"):
            self.transport.dispatch(claim_id=claim, capability_lease=self._lease())

    def test_bad_response_retry_redirect_or_second_attempt_is_archived_as_ambiguity(self) -> None:
        def bad(request):
            value = self._sender(request)
            value["hiddenRetryCount"] = 1
            value["physicalAttemptCount"] = 2
            return value
        transport = self._transport(sender=bad)
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "physicalAttemptCount|hiddenRetry"):
            transport.dispatch(claim_id="claim-transport-one", capability_lease=self._lease())
        self.assertEqual(1, len(self.sent))
        self.assertEqual("POST_MAY_HAVE_CROSSED", self.mutation.read("claim-transport-one")["state"])
        self.assertEqual("RESPONSE_ARCHIVED", transport.read("claim-transport-one")["state"])

    def test_final_kill_and_revision_reread_block_entry(self) -> None:
        def kill(stage):
            if stage == "AFTER_SENDER_ENTERED":
                engage_emergency_stop("transport-final-edge", source="unit-test")
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "final Kill/STOP"):
            self.transport.dispatch(
                claim_id="claim-transport-one", capability_lease=self._lease(), crash_hook=kill
            )
        self.assertEqual([], self.sent)

    def test_final_authority_revision_change_blocks_before_marker(self) -> None:
        def revise(stage):
            if stage == "AFTER_SENDER_ENTERED":
                self.snapshot["functionalRevision"] = 2
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "final authority"):
            self.transport.dispatch(
                claim_id="claim-transport-one", capability_lease=self._lease(), crash_hook=revise
            )
        self.assertEqual("SENDER_ENTERED", self.mutation.read("claim-transport-one")["state"])
        self.assertEqual([], self.sent)

    def test_paused_sender_serializes_durable_kill_until_attempt_finishes(self) -> None:
        self.sender_release.clear()
        result = {}; error = []
        thread = threading.Thread(
            target=lambda: self._dispatch_thread(result, error), daemon=True
        )
        thread.start(); self.assertTrue(self.sender_entered.wait(1))
        kill_done = threading.Event()
        killer = threading.Thread(
            target=lambda: (engage_emergency_stop("paused", source="unit-test"), kill_done.set()),
            daemon=True,
        )
        killer.start(); time.sleep(0.05); self.assertFalse(kill_done.is_set())
        self.sender_release.set(); thread.join(2); killer.join(2)
        self.assertFalse(error); self.assertTrue(kill_done.is_set())
        self.assertEqual("POST_MAY_HAVE_CROSSED", result["state"])

    def _dispatch_thread(self, result, error):
        try:
            result.update(self.transport.dispatch(
                claim_id="claim-transport-one", capability_lease=self._lease()
            ))
        except Exception as exc:  # pragma: no cover - asserted by caller
            error.append(exc)

    def test_dirty_schema_and_archive_tamper_fail_closed(self) -> None:
        self.transport.dispatch(claim_id="claim-transport-one", capability_lease=self._lease())
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_transport_dispatch SET response_json='{}'"
            )
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "response archive"):
            self.transport.read("claim-transport-one")
        with self.ledger.connection() as conn:
            conn.execute("CREATE TABLE kis_functional_transport_extra(value TEXT)")
        with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, "schema fingerprint"):
            self._transport()

    def test_union_reader_flags_post_ack_until_official_truth_join(self) -> None:
        self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        snapshot = self.transport.authority_snapshot()
        self.assertIn("POST_ACK_NOT_OFFICIALLY_RECONCILED", snapshot["hazards"])
        self.assertEqual("transport", snapshot["component"])
        self.assertEqual(ACCOUNT, snapshot["accountFingerprint"])
        self.assertFalse(self.transport.integration_status()["officialAckJoinComplete"])

    def test_union_reader_flags_mutation_marker_without_transport_row(self) -> None:
        claim = "claim-orphan-mutation-marker"
        self._seal(claim)
        entered = self.mutation.transition(
            claim_id=claim, expected_revision=1, target_state="SENDER_ENTERED"
        )
        self.mutation.transition(
            claim_id=claim, expected_revision=entered["revision"],
            target_state="POST_MAY_HAVE_CROSSED",
        )
        snapshot = self.transport.authority_snapshot()
        self.assertIn("MUTATION_POST_MARKER_WITHOUT_TRANSPORT", snapshot["hazards"])

    def test_union_reader_flags_transport_row_without_mutation(self) -> None:
        self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        with self.ledger.connection() as conn:
            conn.execute(
                "DELETE FROM kis_mutation_transition WHERE claim_id=?",
                ("claim-transport-one",),
            )
            conn.execute(
                "DELETE FROM kis_mutation_request WHERE claim_id=?",
                ("claim-transport-one",),
            )
        snapshot = self.transport.authority_snapshot()
        self.assertIn("TRANSPORT_REQUEST_WITHOUT_MUTATION", snapshot["hazards"])

    def test_response_time_rollback_is_rejected_by_reader(self) -> None:
        self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_transport_dispatch SET response_at=?",
                ("2026-08-14T04:14:59.999999Z",),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalTransportBlocked, "time lineage"
        ):
            self.transport.read("claim-transport-one")

    def test_ack_row_tamper_or_official_tuple_mismatch_fails_closed(self) -> None:
        self.transport.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_mutation_request SET ack_order_date='20260814',"
                "ack_organization_no='001',ack_order_no='9999999999' "
                "WHERE claim_id='claim-transport-one'"
            )
        snapshot = self.transport.authority_snapshot()
        self.assertIn("POST_ACK_NOT_OFFICIALLY_RECONCILED", snapshot["hazards"])
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_transport_dispatch SET response_ack_hash=?",
                ("0" * 64,),
            )
        with self.assertRaisesRegex(
            KisDomesticFunctionalTransportBlocked, "ACK archive"
        ):
            self.transport.read("claim-transport-one")

    def test_exact_disabled_production_sender_factory_owns_physical_trace(self) -> None:
        token_key = b"transport-integration-token-verify-key"
        auth_key = b"transport-integration-auth-verify-key"
        token_key_id = hashlib.sha256(b"transport-token-key-v1").hexdigest()
        auth_key_id = hashlib.sha256(b"transport-auth-key-v1").hexdigest()
        access_token = "integration-access-token-value"
        app_key = APP_KEY
        app_secret = APP_SECRET
        environment_key = b"transport-integration-environment-key"
        environment_key_id = hashlib.sha256(
            b"transport-environment-key-v1"
        ).hexdigest()

        def signature(key, value):
            return hmac.new(key, canonical(value).encode(), hashlib.sha256).hexdigest()

        def verify(key):
            def reader(value):
                body = {name: item for name, item in value.items() if name != "signature"}
                return hmac.compare_digest(value.get("signature", ""), signature(key, body))
            return reader

        def token_reader(_binding):
            body = {
                "schemaVersion": "kis-domestic-functional-token-attestation/v1",
                "tokenHash": hashlib.sha256(access_token.encode()).hexdigest(),
                "expiresEpoch": NOW.timestamp() + 60,
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "verifierKeyIdHash": token_key_id,
            }
            return {
                "schemaVersion": "kis-domestic-functional-token-envelope/v1",
                "accessToken": access_token, "tokenHash": body["tokenHash"],
                "expiresEpoch": body["expiresEpoch"],
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "attestation": {**body, "signature": signature(token_key, body)},
            }

        def header_builder(token, binding):
            account = {"CANO": "12345678", "ACNT_PRDT_CD": "01"}
            body = {
                "schemaVersion": "kis-domestic-functional-auth-header-attestation/v1",
                "tokenHash": token["tokenHash"],
                "appKeyHash": hashlib.sha256(app_key.encode()).hexdigest(),
                "appSecretHash": hashlib.sha256(app_secret.encode()).hexdigest(),
                "accountFieldsHash": digest(account), "trId": binding["trId"],
                "claimId": binding["claimId"], "payloadHash": binding["payloadHash"],
                "sessionId": binding["sessionId"],
                "authorityRevision": binding["authorityRevision"],
                "operation": binding["operation"], "endpoint": binding["endpoint"],
                "requestHash": binding["requestHash"],
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "verifierKeyIdHash": auth_key_id,
            }
            return {
                "schemaVersion": "kis-domestic-functional-auth-header-envelope/v1",
                "headers": {
                    "authorization": "Bearer " + access_token, "appkey": app_key,
                    "appsecret": app_secret, "custtype": "P",
                    "tr_id": binding["trId"],
                    "content-type": "application/json; charset=utf-8",
                },
                "accountFields": account,
                "attestation": {**body, "signature": signature(auth_key, body)},
            }

        def environment_reader():
            body = {
                "schemaVersion": "kis-domestic-functional-credential-environment/v1",
                "route": "KIS_KR_LIVE_CONTINUOUS",
                "origin": "https://openapi.koreainvestment.com:9443",
                "environmentRevision": ENV_REVISION,
                "appKeyHash": hashlib.sha256(app_key.encode()).hexdigest(),
                "appSecretHash": hashlib.sha256(app_secret.encode()).hexdigest(),
                "accountFieldsHash": digest({
                    "CANO": "12345678", "ACNT_PRDT_CD": "01"
                }),
                "accountFingerprint": ACCOUNT,
                "credentialConfigurationHash": CREDENTIAL,
                "observedEpoch": NOW.timestamp(),
                "verifierKeyIdHash": environment_key_id,
                "productionAvailable": False,
            }
            return {
                **body, "signature": signature(environment_key, body)
            }

        token_verifier = verify(token_key)
        auth_verifier = verify(auth_key)
        environment_verifier = verify(environment_key)
        production_binding_pins = {
            "credentialEnvironmentKeyIdHash": environment_key_id,
            "tokenVerifierKeyIdHash": token_key_id,
            "authorizationVerifierKeyIdHash": auth_key_id,
            "tokenReaderCodeHash": kis_domestic_functional_callback_code_hash(
                token_reader
            ),
            "tokenVerifierCodeHash": kis_domestic_functional_callback_code_hash(
                token_verifier
            ),
            "authorizationBuilderCodeHash": kis_domestic_functional_callback_code_hash(
                header_builder
            ),
            "authorizationVerifierCodeHash": kis_domestic_functional_callback_code_hash(
                auth_verifier
            ),
            "credentialEnvironmentReaderCodeHash": kis_domestic_functional_callback_code_hash(
                environment_reader
            ),
            "credentialEnvironmentVerifierCodeHash": kis_domestic_functional_callback_code_hash(
                environment_verifier
            ),
        }

        socket_calls = []
        fail_socket = [False]
        def socket(prepared):
            socket_calls.append(prepared)
            if fail_socket[0]:
                raise TimeoutError("private values must never be archived")
            return {
                "statusCode": 200, "effectiveUrl": prepared.url,
                "responseHeaders": {"content-type": "application/json"},
                "bodyBytes": json.dumps({
                    "rt_cd": "0", "msg_cd": "MOCK",
                    "output": {"ORD_DT": "20260814", "KRX_FWDG_ORD_ORGNO": "001", "ODNO": "0000000001"},
                }).encode(),
                "redirectFollowed": False,
            }

        monotonic = iter((1000, 1125, 2000, 2250))
        production = DisabledKisDomesticFunctionalProductionTransport(
            token_reader=token_reader, token_attestation_verifier=token_verifier,
            token_verifier_key_id_hash=token_key_id,
            auth_header_builder=header_builder,
            auth_attestation_verifier=auth_verifier,
            auth_verifier_key_id_hash=auth_key_id,
            credential_environment_reader=environment_reader,
            credential_environment_verifier=environment_verifier,
            credential_environment_key_id_hash=environment_key_id,
            credential_configuration_hash=CREDENTIAL,
            gate_owner_id="state-owned-kis-transport-v1",
            gate_code_hash="9" * 64, monotonic_ns=lambda: next(monotonic),
            wall_clock=lambda: NOW.timestamp(), mock_socket=socket,
            allow_mock_socket=True,
        )
        for changed, reason in (
            ({"sender_owner_id": "state-owned-kis-production-sender-v1"}, "gateOwnerHash"),
            ({"sender_code_hash": "8" * 64}, "gateCodeHash"),
            ({"credential_configuration_hash": "7" * 64}, "credentialConfigurationHash"),
        ):
            kwargs = {
                "program_ledger": self.ledger,
                "mutation_journal": self.mutation,
                "capability_ledger": self.capability,
                "signer_key": KEY, "signer_key_id": "transport-test-key-v1",
                "owner_id": "state-owned-kis-transport-v1",
                "production_transport": production,
                "sender_owner_id": "state-owned-kis-transport-v1",
                "sender_code_hash": "9" * 64,
                "credential_configuration_hash": CREDENTIAL,
                "production_binding_pins": production_binding_pins,
                "clock": lambda: NOW,
            }
            kwargs.update(changed)
            with self.assertRaisesRegex(KisDomesticFunctionalTransportBlocked, reason):
                DurableKisDomesticFunctionalTransport.from_disabled_production_sender(
                    **kwargs
                )
        bad_pins = dict(production_binding_pins)
        bad_pins["tokenVerifierKeyIdHash"] = "0" * 64
        with self.assertRaisesRegex(
            KisDomesticFunctionalTransportBlocked,
            "tokenVerifierKeyIdHash binding mismatch",
        ):
            DurableKisDomesticFunctionalTransport.from_disabled_production_sender(
                program_ledger=self.ledger,
                mutation_journal=self.mutation,
                capability_ledger=self.capability,
                signer_key=KEY,
                signer_key_id="transport-test-key-v1",
                owner_id="state-owned-kis-transport-v1",
                production_transport=production,
                sender_owner_id="state-owned-kis-transport-v1",
                sender_code_hash="9" * 64,
                credential_configuration_hash=CREDENTIAL,
                production_binding_pins=bad_pins,
                clock=lambda: NOW,
            )
        integrated = DurableKisDomesticFunctionalTransport.from_disabled_production_sender(
            program_ledger=self.ledger, mutation_journal=self.mutation,
            capability_ledger=self.capability, signer_key=KEY,
            signer_key_id="transport-test-key-v1",
            owner_id="state-owned-kis-transport-v1",
            production_transport=production,
            sender_owner_id="state-owned-kis-transport-v1",
            sender_code_hash="9" * 64,
            credential_configuration_hash=CREDENTIAL,
            production_binding_pins=production_binding_pins,
            clock=lambda: NOW,
        )
        result = integrated.dispatch(
            claim_id="claim-transport-one", capability_lease=self._lease()
        )
        self.assertEqual(1, len(socket_calls))
        self.assertEqual(1, result["physicalAttemptCount"])
        archive = integrated.read("claim-transport-one")
        self.assertEqual("OWNED_PRODUCTION_DISABLED", archive["sender_kind"])
        response = json.loads(archive["response_json"])
        self.assertEqual(125, response["physicalTrace"]["elapsedMonotonicNs"])
        self.assertTrue(integrated.integration_status()["physicalTraceOwned"])

        self._seal("claim-transport-failed-physical")
        self._set_authority_intent("claim-transport-failed-physical")
        fail_socket[0] = True
        failed = integrated.dispatch(
            claim_id="claim-transport-failed-physical",
            capability_lease=self._lease(),
        )
        self.assertEqual(1, failed["physicalAttemptCount"])
        failed_row = integrated.read("claim-transport-failed-physical")
        failed_response = json.loads(failed_row["response_json"])
        self.assertTrue(failed_response["physicalTraceOwned"])
        self.assertFalse(
            failed_response["physicalTrace"]["physicalAttemptComplete"]
        )
        self.assertEqual(
            digest(failed_response["errorArchive"]),
            failed_response["errorArchiveHash"],
        )
        self.assertIn(
            "POST_RESPONSE_UNPROVEN",
            integrated.authority_snapshot()["hazards"],
        )

    def test_authority_snapshot_is_one_route_fenced_sqlite_projection(self) -> None:
        self.assertFalse(self.transport.integration_status()["physicalTraceOwned"])
        original_mutation_read = self.mutation.read
        original_transport_read = self.transport.read
        self.mutation.read = lambda _claim: (_ for _ in ()).throw(
            AssertionError("secondary mutation connection forbidden")
        )
        self.transport.read = lambda _claim: (_ for _ in ()).throw(
            AssertionError("secondary transport connection forbidden")
        )
        try:
            snapshot = self.transport.authority_snapshot()
        finally:
            self.mutation.read = original_mutation_read
            self.transport.read = original_transport_read
        self.assertTrue(snapshot["readable"])

        entered = threading.Event(); release = threading.Event(); acquired = threading.Event()
        original_verify = self.mutation._verify_request_locked
        def paused_verify(conn, row):
            entered.set(); release.wait(2); return original_verify(conn, row)
        self.mutation._verify_request_locked = paused_verify
        reader = threading.Thread(target=self.transport.authority_snapshot)
        reader.start(); self.assertTrue(entered.wait(1))
        from live_trader.kis_order_authority import kis_route_authority_serialization
        # Use a proper context in a helper so release is also verified.
        def acquire_route():
            with kis_route_authority_serialization():
                acquired.set()
        contender = threading.Thread(target=acquire_route)
        contender.start(); self.assertFalse(acquired.wait(0.1))
        release.set(); reader.join(2); contender.join(2)
        self.mutation._verify_request_locked = original_verify
        self.assertTrue(acquired.is_set())


if __name__ == "__main__":
    unittest.main()
