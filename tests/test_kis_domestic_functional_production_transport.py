from __future__ import annotations

import hashlib
import hmac
import json
import os
import unittest

from live_trader.kis_domestic_functional_production_transport import (
    DisabledKisDomesticFunctionalProductionTransport,
    KisDomesticFunctionalProductionTransportBlocked,
    kis_domestic_functional_callback_code_hash,
    production_entrypoint_status,
)
from live_trader.kis_domestic_functional_get_client import (
    _credential_configuration_hash as get_credential_configuration_hash,
    kis_domestic_functional_account_fingerprint,
)


ACCOUNT = kis_domestic_functional_account_fingerprint("12345678", "01")
GATE_CODE = "c" * 64
TOKEN_KEY = b"verify-only-token-authority-test-key"
AUTH_KEY = b"verify-only-auth-authority-test-key-1"
ENV_KEY = b"verify-only-environment-authority-key"
TOKEN_KEY_ID = hashlib.sha256(b"token-authority-v1").hexdigest()
AUTH_KEY_ID = hashlib.sha256(b"auth-authority-v1").hexdigest()
ENV_KEY_ID = hashlib.sha256(b"environment-authority-v1").hexdigest()
ACCESS_TOKEN = "access-token-private-never-archive"
APP_KEY = "app-key-private-value"
APP_SECRET = "app-secret-private-value-long"


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def secret_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


ENV_REVISION = 7
CREDENTIAL = get_credential_configuration_hash(
    app_key=APP_KEY,
    app_secret=APP_SECRET,
    account_fingerprint=ACCOUNT,
)


def sign(key, body):
    return hmac.new(key, canonical(body).encode(), hashlib.sha256).hexdigest()


def verifier(key):
    def verify(value):
        if not isinstance(value, dict) or type(value.get("signature")) is not str:
            return False
        body = {key_name: item for key_name, item in value.items() if key_name != "signature"}
        return hmac.compare_digest(value["signature"], sign(key, body))
    return verify


class KisProductionTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.socket_calls = []
        self.token_calls = []
        self.header_calls = []
        self.monotonic_values = iter((1_000, 1_125, 2_000, 2_250, 3_000, 3_500))
        self.transport = self._transport()
        self.gate, self.gate_token = self.transport._gate_binding_for_transport_owner()

    def request(self, operation="NATURAL_BUY"):
        specs = {
            "NATURAL_BUY": (
                "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0012U",
                "BUY", False,
                {"PDNO": "010140", "ORD_DVSN": "00", "ORD_QTY": "1", "ORD_UNPR": "80000"},
            ),
            "CLEANUP_SELL": (
                "/uapi/domestic-stock/v1/trading/order-cash", "TTTC0011U",
                "SELL", True,
                {"PDNO": "010140", "ORD_DVSN": "00", "ORD_QTY": "1", "ORD_UNPR": "79000"},
            ),
            "CLEANUP_CANCEL": (
                "/uapi/domestic-stock/v1/trading/order-rvsecncl", "TTTC0013U",
                "CANCEL", True,
                {
                    "KRX_FWDG_ORD_ORGNO": "001", "ORGN_ODNO": "0000000001",
                    "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02",
                    "ORD_QTY": "1", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y",
                    "EXCG_ID_DVSN_CD": "KRX",
                },
            ),
        }
        endpoint, tr_id, side, cleanup, payload = specs[operation]
        return {
            "schemaVersion": "kis-domestic-functional-transport-request/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS", "pdno": "010140",
            "origin": "https://openapi.koreainvestment.com:9443",
            "method": "POST", "endpoint": endpoint, "trId": tr_id,
            "query": [], "headers": {"custtype": "P", "tr_id": tr_id},
            "operation": operation, "side": side, "cleanupOnly": cleanup,
            "claimId": f"claim-{operation.lower()}", "sessionId": "kis-session-production-transport",
            "authorityRevision": 7, "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "payload": payload, "payloadHash": digest(payload),
        }

    def _token_reader(self, binding):
        self.token_calls.append(dict(binding))
        attestation = {
            "schemaVersion": "kis-domestic-functional-token-attestation/v1",
            "tokenHash": secret_hash(ACCESS_TOKEN), "expiresEpoch": 2_000.0,
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "verifierKeyIdHash": TOKEN_KEY_ID,
        }
        return {
            "schemaVersion": "kis-domestic-functional-token-envelope/v1",
            "accessToken": ACCESS_TOKEN, "tokenHash": secret_hash(ACCESS_TOKEN),
            "expiresEpoch": 2_000.0, "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "attestation": {**attestation, "signature": sign(TOKEN_KEY, attestation)},
        }

    def _header_builder(self, token, binding):
        self.header_calls.append((dict(token), dict(binding)))
        account_fields = {"CANO": "12345678", "ACNT_PRDT_CD": "01"}
        attestation = {
            "schemaVersion": "kis-domestic-functional-auth-header-attestation/v1",
            "tokenHash": token["tokenHash"], "appKeyHash": secret_hash(APP_KEY),
            "appSecretHash": secret_hash(APP_SECRET),
            "accountFieldsHash": digest(account_fields), "trId": binding["trId"],
            "claimId": binding["claimId"], "payloadHash": binding["payloadHash"],
            "sessionId": binding["sessionId"],
            "authorityRevision": binding["authorityRevision"],
            "operation": binding["operation"], "endpoint": binding["endpoint"],
            "requestHash": binding["requestHash"],
            "accountFingerprint": binding["accountFingerprint"],
            "credentialConfigurationHash": binding["credentialConfigurationHash"],
            "verifierKeyIdHash": AUTH_KEY_ID,
        }
        return {
            "schemaVersion": "kis-domestic-functional-auth-header-envelope/v1",
            "headers": {
                "authorization": "Bearer " + token["accessToken"],
                "appkey": APP_KEY, "appsecret": APP_SECRET, "custtype": "P",
                "tr_id": binding["trId"],
                "content-type": "application/json; charset=utf-8",
            },
            "accountFields": account_fields,
            "attestation": {**attestation, "signature": sign(AUTH_KEY, attestation)},
        }

    def _environment_reader(self):
        body = {
            "schemaVersion": "kis-domestic-functional-credential-environment/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS",
            "origin": "https://openapi.koreainvestment.com:9443",
            "environmentRevision": ENV_REVISION,
            "appKeyHash": secret_hash(APP_KEY),
            "appSecretHash": secret_hash(APP_SECRET),
            "accountFieldsHash": digest({"CANO": "12345678", "ACNT_PRDT_CD": "01"}),
            "accountFingerprint": ACCOUNT,
            "credentialConfigurationHash": CREDENTIAL,
            "observedEpoch": 1_000.0,
            "verifierKeyIdHash": ENV_KEY_ID,
            "productionAvailable": False,
        }
        return {**body, "signature": sign(ENV_KEY, body)}

    def _socket(self, prepared):
        self.socket_calls.append(prepared)
        return {
            "statusCode": 200, "effectiveUrl": prepared.url,
            "responseHeaders": {"content-type": "application/json"},
            "bodyBytes": b'{"msg_cd":"MOCK","rt_cd":"0"}',
            "redirectFollowed": False,
        }

    def _transport(self, *, socket=None, allow_mock=True, token_reader=None,
                   token_verifier=None, header_builder=None, auth_verifier=None,
                   environment_reader=None, environment_verifier=None):
        return DisabledKisDomesticFunctionalProductionTransport(
            token_reader=token_reader or self._token_reader,
            token_attestation_verifier=token_verifier or verifier(TOKEN_KEY),
            token_verifier_key_id_hash=TOKEN_KEY_ID,
            auth_header_builder=header_builder or self._header_builder,
            auth_attestation_verifier=auth_verifier or verifier(AUTH_KEY),
            auth_verifier_key_id_hash=AUTH_KEY_ID,
            credential_environment_reader=environment_reader or self._environment_reader,
            credential_environment_verifier=environment_verifier or verifier(ENV_KEY),
            credential_environment_key_id_hash=ENV_KEY_ID,
            credential_configuration_hash=CREDENTIAL,
            gate_owner_id="isolated-kis-transport-gate-v1", gate_code_hash=GATE_CODE,
            timeout_seconds=5.0, monotonic_ns=lambda: next(self.monotonic_values),
            wall_clock=lambda: 1_000.0, mock_socket=socket or self._socket,
            allow_mock_socket=allow_mock,
        )

    def test_flags_false_and_real_network_is_compile_disabled_before_auth(self):
        for key in ("available", "networkCompiled", "networkAvailable", "releaseEvidenceAvailable"):
            self.assertFalse(production_entrypoint_status()[key])
        calls = []
        transport = DisabledKisDomesticFunctionalProductionTransport(
            token_reader=lambda item: calls.append(item),
            token_attestation_verifier=verifier(TOKEN_KEY),
            token_verifier_key_id_hash=TOKEN_KEY_ID,
            auth_header_builder=self._header_builder,
            auth_attestation_verifier=verifier(AUTH_KEY),
            auth_verifier_key_id_hash=AUTH_KEY_ID,
            credential_environment_reader=self._environment_reader,
            credential_environment_verifier=verifier(ENV_KEY),
            credential_environment_key_id_hash=ENV_KEY_ID,
            credential_configuration_hash=CREDENTIAL,
            gate_owner_id="real-disabled-gate", gate_code_hash=GATE_CODE,
        )
        old = os.environ.get("LIVE_TRADER_KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT")
        os.environ["LIVE_TRADER_KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT"] = "EXPLICITLY_ENABLED_AFTER_REVIEW"
        try:
            with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "compile-disabled"):
                gate, token = transport._gate_binding_for_transport_owner()
                gate.send(self.request(), gate_call_token=token)
        finally:
            if old is None:
                os.environ.pop("LIVE_TRADER_KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT", None)
            else:
                os.environ["LIVE_TRADER_KIS_DOMESTIC_FUNCTIONAL_PRODUCTION_TRANSPORT"] = old
        self.assertEqual([], calls)

    def test_exact_buy_single_attempt_trace_and_no_private_result(self):
        result = self.gate.send(self.request(), gate_call_token=self.gate_token)
        self.assertEqual(1, len(self.socket_calls))
        self.assertEqual(1, result["physicalAttemptCount"])
        self.assertEqual(125, result["physicalTrace"]["elapsedMonotonicNs"])
        self.assertEqual(0, result["hiddenRetryCount"])
        self.assertEqual(0, result["redirectFollowCount"])
        self.assertFalse(result["authorizationMaterialArchived"])
        serialized = canonical(result) + repr(self.transport) + repr(self.gate)
        for secret in (ACCESS_TOKEN, APP_KEY, APP_SECRET, "12345678"):
            self.assertNotIn(secret, serialized)
        sent = self.socket_calls[0]
        self.assertEqual("Bearer " + ACCESS_TOKEN, sent.headers["authorization"])
        self.assertEqual("12345678", json.loads(sent.body)["CANO"])

    def test_production_binding_envelope_is_exact_signed_and_tamper_rejected(self):
        binding = self.transport.production_binding_status()
        self.assertTrue(self.transport.verify_production_binding_status(binding))
        self.assertEqual(GATE_CODE, binding["gateCodeHash"])
        self.assertEqual(CREDENTIAL, binding["credentialConfigurationHash"])
        self.assertEqual(
            kis_domestic_functional_callback_code_hash(self._token_reader),
            binding["tokenReaderCodeHash"],
        )
        for key in (
            "tokenVerifierKeyIdHash", "authorizationVerifierKeyIdHash",
            "credentialEnvironmentKeyIdHash", "tokenVerifierCodeHash",
            "authorizationBuilderCodeHash", "authorizationVerifierCodeHash",
            "credentialEnvironmentReaderCodeHash",
            "credentialEnvironmentVerifierCodeHash",
        ):
            self.assertRegex(binding[key], r"^[0-9a-f]{64}$")
        self.assertFalse(self.transport.verify_production_binding_status({
            **binding, "gateCodeHash": "0" * 64,
        }))
        self.assertFalse(self.transport.verify_production_binding_status({
            **binding, "extra": False,
        }))

    def test_credential_environment_is_independently_derived_pre_socket(self):
        def changed_revision():
            value = self._environment_reader()
            body = {key: item for key, item in value.items() if key != "signature"}
            body["environmentRevision"] += 1
            body["signature"] = sign(ENV_KEY, body)
            return body

        def stale():
            value = self._environment_reader()
            body = {key: item for key, item in value.items() if key != "signature"}
            body["observedEpoch"] = 998.0
            body["signature"] = sign(ENV_KEY, body)
            return body

        changed = self._transport(environment_reader=changed_revision)
        changed_gate, changed_token = changed._gate_binding_for_transport_owner()
        changed_result = changed_gate.send(
            self.request(), gate_call_token=changed_token
        )
        self.assertEqual(
            ENV_REVISION + 1,
            changed_result["attemptBinding"]["environmentRevision"],
        )
        self.socket_calls.clear()

        for kwargs, reason in (
            ({"environment_reader": stale}, "binding mismatch"),
            ({"environment_verifier": lambda _value: False}, "unverified"),
        ):
            with self.subTest(reason=reason):
                transport = self._transport(**kwargs)
                gate, token = transport._gate_binding_for_transport_owner()
                with self.assertRaisesRegex(
                    KisDomesticFunctionalProductionTransportBlocked, reason
                ):
                    gate.send(self.request(), gate_call_token=token)
        self.assertEqual([], self.socket_calls)

    def test_cleanup_sell_and_cancel_use_exact_endpoint_tr_and_side(self):
        for operation, tr_id, endpoint in (
            ("CLEANUP_SELL", "TTTC0011U", "order-cash"),
            ("CLEANUP_CANCEL", "TTTC0013U", "order-rvsecncl"),
        ):
            with self.subTest(operation=operation):
                transport = self._transport()
                gate, token = transport._gate_binding_for_transport_owner()
                result = gate.send(self.request(operation), gate_call_token=token)
                self.assertEqual(tr_id, result["trId"])
                self.assertTrue(result["endpoint"].endswith(endpoint))

    def test_forged_gate_lease_and_request_substitution_are_rejected(self):
        request = self.request()
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "private transport gate lease"):
            self.transport._dispatch(request=request, lease={})
        changed = {**request, "trId": "TTTC0011U"}
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "trId mismatch"):
            self.gate.send(changed, gate_call_token=self.gate_token)
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "caller token"):
            self.gate.send(request, gate_call_token=object())
        self.assertEqual([], self.socket_calls)

    def test_valid_gate_lease_is_one_shot_and_request_bound(self):
        request = self.request(); request_hash = digest(request)
        lease = self.transport._mint_lease(
            gate_owner_hash=self.transport._gate_owner_hash,
            gate_code_hash=self.transport._gate_code_hash, request_hash=request_hash,
        )
        self.transport._dispatch(request=request, lease=lease)
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "already consumed"):
            self.transport._dispatch(request=request, lease=lease)
        self.assertEqual(1, len(self.socket_calls))

    def test_token_expired_or_verify_only_rejection_blocks_socket(self):
        def expired(binding):
            value = self._token_reader(binding); value["expiresEpoch"] = 999.0
            return value
        for kwargs, reason in (
            ({"token_reader": expired}, "expired"),
            ({"token_verifier": lambda _value: False}, "unverified"),
        ):
            with self.subTest(reason=reason):
                transport = self._transport(**kwargs)
                with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, reason):
                    gate, token = transport._gate_binding_for_transport_owner()
                    gate.send(self.request(), gate_call_token=token)
        self.assertEqual([], self.socket_calls)

    def test_authorization_attestation_or_account_binding_rejected(self):
        def wrong_account(token, binding):
            value = self._header_builder(token, binding)
            value["accountFields"] = {"CANO": "87654321", "ACNT_PRDT_CD": "01"}
            return value
        for kwargs, reason in (
            ({"auth_verifier": lambda _value: False}, "unverified"),
            ({"header_builder": wrong_account}, "fingerprint mismatch"),
        ):
            with self.subTest(reason=reason):
                transport = self._transport(**kwargs)
                with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, reason):
                    gate, token = transport._gate_binding_for_transport_owner()
                    gate.send(self.request(), gate_call_token=token)
        self.assertEqual([], self.socket_calls)

    def test_redirect_or_effective_url_change_is_rejected_after_one_call(self):
        def redirected(prepared):
            self.socket_calls.append(prepared)
            return {
                "statusCode": 302, "effectiveUrl": prepared.url + "/elsewhere",
                "responseHeaders": {"content-type": "application/json"},
                "bodyBytes": b"{}", "redirectFollowed": True,
            }
        transport = self._transport(socket=redirected)
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "effective URL"):
            gate, token = transport._gate_binding_for_transport_owner()
            gate.send(self.request(), gate_call_token=token)
        self.assertEqual(1, len(self.socket_calls))
        failed = transport.last_failed_attempt()
        self.assertTrue(failed["physicalTraceOwned"])
        self.assertFalse(failed["physicalTrace"]["physicalAttemptComplete"])
        self.assertEqual(1, failed["physicalAttemptCount"])

    def test_malformed_duplicate_or_oversized_response_fails_closed(self):
        bodies = (b'{"a":1,"a":2}', b"x" * 1_048_577)
        for body in bodies:
            with self.subTest(size=len(body)):
                def bad(prepared, response_body=body):
                    return {
                        "statusCode": 200, "effectiveUrl": prepared.url,
                        "responseHeaders": {"content-type": "application/json"},
                        "bodyBytes": response_body, "redirectFollowed": False,
                    }
                transport = self._transport(socket=bad)
                with self.assertRaises(KisDomesticFunctionalProductionTransportBlocked):
                    gate, token = transport._gate_binding_for_transport_owner()
                    gate.send(self.request(), gate_call_token=token)

    def test_socket_exception_and_echoed_credentials_never_leak(self):
        def explode(_prepared):
            raise RuntimeError("contains " + ACCESS_TOKEN + APP_SECRET)
        transport = self._transport(socket=explode)
        with self.assertRaises(KisDomesticFunctionalProductionTransportBlocked) as caught:
            gate, token = transport._gate_binding_for_transport_owner()
            gate.send(self.request(), gate_call_token=token)
        self.assertNotIn(ACCESS_TOKEN, str(caught.exception))
        self.assertNotIn(APP_SECRET, str(caught.exception))
        failed = transport.last_failed_attempt()
        self.assertTrue(failed["physicalTraceOwned"])
        self.assertEqual(1, failed["physicalAttemptCount"])
        self.assertFalse(failed["physicalTrace"]["physicalAttemptComplete"])
        self.assertEqual(
            failed["errorArchiveHash"],
            digest(failed["errorArchive"]),
        )
        serialized_failure = canonical(failed)
        for secret in (ACCESS_TOKEN, APP_KEY, APP_SECRET, "12345678"):
            self.assertNotIn(secret, serialized_failure)

        def echo(prepared):
            return {
                "statusCode": 200, "effectiveUrl": prepared.url,
                "responseHeaders": {"content-type": "application/json"},
                "bodyBytes": json.dumps({"token": ACCESS_TOKEN}).encode(),
                "redirectFollowed": False,
            }
        transport = self._transport(socket=echo)
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "forbidden private"):
            gate, token = transport._gate_binding_for_transport_owner()
            gate.send(self.request(), gate_call_token=token)

    def test_previous_failure_trace_is_cleared_before_a_new_consumed_lease(self):
        token_fails = [False]

        def token_reader(binding):
            if token_fails[0]:
                raise RuntimeError("pre-socket")
            return self._token_reader(binding)

        def socket_fails(_prepared):
            raise TimeoutError("physical")

        transport = self._transport(
            socket=socket_fails, token_reader=token_reader
        )
        gate, token = transport._gate_binding_for_transport_owner()
        with self.assertRaises(KisDomesticFunctionalProductionTransportBlocked):
            gate.send(self.request(), gate_call_token=token)
        self.assertEqual(
            digest(self.request()),
            transport.last_failed_attempt()["requestHash"],
        )
        token_fails[0] = True
        second = self.request("CLEANUP_SELL")
        with self.assertRaisesRegex(
            KisDomesticFunctionalProductionTransportBlocked, "token reader failed"
        ):
            gate.send(second, gate_call_token=token)
        with self.assertRaisesRegex(
            KisDomesticFunctionalProductionTransportBlocked,
            "physical failure trace is absent",
        ):
            transport.last_failed_attempt()

    def test_final_payload_caps_account_override_and_cancel_tuple_reject(self):
        cases = []
        order = self.request(); order["payload"] = {**order["payload"], "ORD_QTY": "2"}
        order["payloadHash"] = digest(order["payload"]); cases.append((order, "quantity"))
        account = self.request(); account["payload"] = {**account["payload"], "CANO": "87654321"}
        account["payloadHash"] = digest(account["payload"]); cases.append((account, "override"))
        cancel = self.request("CLEANUP_CANCEL"); cancel["payload"] = {**cancel["payload"], "ORGN_ODNO": ""}
        cancel["payloadHash"] = digest(cancel["payload"]); cases.append((cancel, "tuple"))
        for request, reason in cases:
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, reason):
                    self.gate.send(request, gate_call_token=self.gate_token)
        self.assertEqual([], self.socket_calls)

    def test_token_expiring_between_auth_and_socket_is_rejected(self):
        wall = iter((1_000.0, 2_000.0))
        transport = DisabledKisDomesticFunctionalProductionTransport(
            token_reader=self._token_reader,
            token_attestation_verifier=verifier(TOKEN_KEY),
            token_verifier_key_id_hash=TOKEN_KEY_ID,
            auth_header_builder=self._header_builder,
            auth_attestation_verifier=verifier(AUTH_KEY),
            auth_verifier_key_id_hash=AUTH_KEY_ID,
            credential_environment_reader=self._environment_reader,
            credential_environment_verifier=verifier(ENV_KEY),
            credential_environment_key_id_hash=ENV_KEY_ID,
            credential_configuration_hash=CREDENTIAL,
            gate_owner_id="isolated-kis-transport-gate-v1", gate_code_hash=GATE_CODE,
            monotonic_ns=lambda: 1000, wall_clock=lambda: next(wall),
            mock_socket=self._socket, allow_mock_socket=True,
        )
        gate, token = transport._gate_binding_for_transport_owner()
        with self.assertRaisesRegex(KisDomesticFunctionalProductionTransportBlocked, "immediately before"):
            gate.send(self.request(), gate_call_token=token)
        self.assertEqual([], self.socket_calls)


if __name__ == "__main__":
    unittest.main()
