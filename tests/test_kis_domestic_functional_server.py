from __future__ import annotations

import hashlib
import json
from email.message import Message
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timezone

from live_trader.functional_http_session import (
    APP_SESSION_COOKIE,
    CSRF_HEADER,
    FunctionalHttpSessionAuthority,
)
from live_trader.kis_domestic_functional_server import (
    DisabledKisDomesticFunctionalServer,
    DisabledKisFunctionalOfflineManager,
    production_entrypoint_status,
)
from live_trader.kis_domestic_functional_state import DurableKisDomesticFunctionalState
from live_trader.program_ledger import ProgramLedger
from live_trader.safety_confirmation import SafetyConfirmationStore


ACCOUNT = "a" * 64
CREDENTIAL = "b" * 64
OWNERS = {
    "graph": "server-graph-owner", "backend": "server-backend-owner",
    "capability": "server-capability-owner", "transport": "server-transport-owner",
}
NOW = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)


class KisFunctionalServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.ledger = ProgramLedger(Path(self.temp.name) / "server.sqlite3")
        self.session = ""; self.account = ACCOUNT; self.credential = CREDENTIAL
        self.hazards = {name: [] for name in OWNERS}
        self.component_readers = {name: self._reader(name) for name in OWNERS}
        self.state = DurableKisDomesticFunctionalState(
            program_ledger=self.ledger, owner_id="server-state-owner",
            component_owner_ids=OWNERS, component_readers=self.component_readers,
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            application_lease_held=True, clock=lambda: NOW,
        )
        self.authority = FunctionalHttpSessionAuthority.mint(host="127.0.0.1", port=18765)
        self.safety = SafetyConfirmationStore(clock=lambda: NOW.timestamp())
        self.manager_calls = {"start": 0, "stop": 0, "recover": 0}
        self.manager_status = {
            name: {"networkDispatchCount": 0, "tradingMutationCount": 0}
            for name in self.manager_calls
        }
        self.context_base = {
            "schemaVersion": "kis-domestic-functional-safety-context/v1",
            "route": "KIS_KR_LIVE_CONTINUOUS", "pdno": "010140",
            "publicArmId": "kis-public-arm-server-one", "publicArmHash": "1" * 64,
            "accountFingerprint": ACCOUNT,
            "artifactCanonicalHash": "2" * 64, "artifactFileSha256": "3" * 64,
            "instanceCanonicalHash": "4" * 64, "instanceFileSha256": "5" * 64,
            "codeManifestHash": "6" * 64, "capsHash": "7" * 64,
            "oneShotId": "kis-one-shot-server-one", "oneShotHash": "8" * 64,
            "credentialConfigurationHash": CREDENTIAL,
            "productionAvailable": False,
        }
        managers = {
            name: DisabledKisFunctionalOfflineManager(
                name=name, owner_id=f"server-{name}-manager-owner",
                code_hash=hashlib.sha256(f"server-{name}-manager-code".encode()).hexdigest(),
                status_reader=self._manager_reader(name), callback=self._manager(name),
            )
            for name in self.manager_calls
        }
        self.server = DisabledKisDomesticFunctionalServer(
            state=self.state, http_authority=self.authority,
            safety_confirmations=self.safety,
            approved_context_base=self.context_base,
            offline_managers=managers,
            allow_offline_managers=True,
            server_signer_key=b"server-test-one-shot-signing-key-32-bytes-minimum",
            server_signer_key_id="server-one-shot-key-v1",
            clock=lambda: NOW.timestamp(),
        )

    def _reader(self, name):
        def read():
            return {
                "schemaVersion": "kis-domestic-functional-component-status/v1",
                "component": name,
                "ownerHash": hashlib.sha256(OWNERS[name].encode()).hexdigest(),
                "route": "KIS_KR_LIVE_CONTINUOUS", "readable": True,
                "sessionId": self.session,
                "accountFingerprint": self.account,
                "credentialConfigurationHash": self.credential,
                "hazards": list(self.hazards[name]), "functionalMutationIntent": {},
                "killOrdinaryCancelAllowed": False, "killOrdinaryCancelRevision": 0,
                "killOrdinaryCancelIntent": {}, "productionAvailable": False,
            }
        return read

    def _manager(self, name):
        def manager(reservation):
            self.manager_calls[name] += 1
            if name in {"start", "recover"}:
                self.session = reservation["sessionId"]
            elif name == "stop":
                self.session = ""
            return {"ok": True, "mutationMayHaveOccurred": False, "receiptHash": "f" * 64}
        return manager

    def _manager_reader(self, name):
        owner_hash = hashlib.sha256(f"server-{name}-manager-owner".encode()).hexdigest()
        code_hash = hashlib.sha256(f"server-{name}-manager-code".encode()).hexdigest()
        def read():
            return {
                "schemaVersion": "kis-domestic-functional-offline-manager-status/v1",
                "manager": name, "ownerHash": owner_hash, "codeHash": code_hash,
                "readable": True,
                "networkDispatchCount": self.manager_status[name]["networkDispatchCount"],
                "tradingMutationCount": self.manager_status[name]["tradingMutationCount"],
                "productionAvailable": False,
            }
        return read

    def headers(self, *, mutation=True, cookie=True, origin=True, csrf=True):
        headers = Message(); headers.add_header("Host", self.authority.expected_host_header)
        if cookie:
            headers.add_header("Cookie", f"{APP_SESSION_COOKIE}={self.authority.app_session_token}")
        if mutation and origin:
            headers.add_header("Origin", self.authority.expected_origin)
        if mutation and csrf:
            headers.add_header(CSRF_HEADER, self.authority.csrf_token)
        headers.add_header("Content-Type", "application/json")
        return headers

    def context(self, action):
        return self.server.safety_context(action)

    def call(self, method, path, payload=None, *, headers=None, peer="127.0.0.1", reads=None):
        raw = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode()
        def reader():
            if reads is not None: reads.append(True)
            return raw
        return self.server.handle(
            method=method, path=path,
            headers=headers or self.headers(mutation=method == "POST"),
            peer_host=peer, body_reader=reader,
        )

    def confirmation(self, action):
        context = self.context(action)
        challenge = self.call("POST", "/api/kis-domestic-functional/safety-confirmation/challenge", {
            "action": action, "safetyContext": context,
        })
        self.assertEqual(200, challenge.status)
        return context, {
            "challengeId": challenge.body["challengeId"], "token": challenge.body["token"],
            "typedPhrase": challenge.body["expectedPhrase"],
        }

    def test_flags_false_and_native_bootstrap_is_unreachable(self) -> None:
        for key in ("available", "networkAvailable", "bootstrapAvailable", "backendAvailable", "brokerMutationAvailable", "releaseEvidenceAvailable"):
            self.assertFalse(production_entrypoint_status()[key])
        reads = []
        result = self.call("GET", "/__lt_native_bootstrap?nonce=anything", reads=reads)
        self.assertEqual(503, result.status); self.assertFalse(result.body["setCookiePerformed"])
        self.assertNotIn("Set-Cookie", result.headers); self.assertEqual([], reads)

    def test_protected_status_uses_session_without_csrf_or_body(self) -> None:
        reads = []
        result = self.call("GET", "/api/kis-domestic-functional/status",
                           headers=self.headers(mutation=False), reads=reads)
        self.assertEqual(200, result.status); self.assertTrue(result.body["ok"])
        self.assertFalse(result.body["available"]); self.assertEqual([], reads)

    def test_status_missing_cookie_rejects_before_body(self) -> None:
        reads = []
        result = self.call("GET", "/api/kis-domestic-functional/status",
                           headers=self.headers(mutation=False, cookie=False), reads=reads)
        self.assertEqual(403, result.status); self.assertEqual([], reads)
        self.assertFalse(result.body["brokerSubmissionPerformed"])

    def test_mutation_missing_csrf_or_wrong_origin_rejects_pre_body(self) -> None:
        for headers in (
            self.headers(csrf=False),
            self.headers(),
        ):
            if headers.get("Origin") and headers.get(CSRF_HEADER):
                headers.replace_header("Origin", "http://127.0.0.1:9999")
            reads = []
            result = self.call("POST", "/api/kis-domestic-functional/start", {}, headers=headers, reads=reads)
            self.assertEqual(403, result.status); self.assertEqual([], reads)
        self.assertEqual(0, self.manager_calls["start"])

    def test_exact_challenge_and_start_consume_one_shot(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        result = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-http-one", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(200, result.status); self.assertTrue(result.body["ok"])
        self.assertFalse(result.body["brokerSubmissionPerformed"])
        self.assertEqual(1, self.manager_calls["start"])
        with self.ledger.connection() as conn:
            row = conn.execute("SELECT state,consumed_action,revision FROM kis_functional_server_safety").fetchone()
            transition = conn.execute(
                "SELECT reservation_id,state_revision,session_id FROM kis_functional_server_safety_transition WHERE revision=2"
            ).fetchone()
        self.assertEqual(("CONSUMED", "KIS_START", 2), tuple(row))
        self.assertEqual("kis-session-http-one", transition["session_id"])
        self.assertTrue(transition["reservation_id"])
        self.assertEqual(2, transition["state_revision"])

    def test_confirmation_context_substitution_is_one_shot_and_no_state_call(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        context = {**context, "capsHash": "9" * 64}
        result = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-bad-context", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(409, result.status); self.assertEqual(0, self.manager_calls["start"])
        # The challenge was consumed before its mismatched digest could authorize anything.
        again = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-bad-context", "safetyContext": self.context("KIS_START"),
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(409, again.status); self.assertIn("used", again.body["reason"])

    def test_second_approved_start_is_blocked_by_durable_one_shot(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        first = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-first", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(200, first.status)
        # Stop so phase eligibility cannot mask the one-shot assertion.
        stop_context, stop_confirmation = self.confirmation("KIS_STOP")
        self.call("POST", "/api/kis-domestic-functional/stop", {
            "safetyContext": stop_context, "safetyConfirmation": stop_confirmation,
        })
        second = self.call("POST", "/api/kis-domestic-functional/safety-confirmation/challenge", {
            "action": "KIS_START", "safetyContext": self.context("KIS_START"),
        })
        self.assertEqual(409, second.status); self.assertEqual("start-one-shot-consumed", second.body["reason"])
        self.assertEqual(1, self.manager_calls["start"])

    def test_stop_and_recover_have_independent_exact_confirmations(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-cleanup", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        context, confirmation = self.confirmation("KIS_STOP")
        stopped = self.call("POST", "/api/kis-domestic-functional/stop", {
            "safetyContext": context, "safetyConfirmation": confirmation,
        })
        self.assertEqual(200, stopped.status); self.assertEqual(1, self.manager_calls["stop"])
        context, confirmation = self.confirmation("KIS_RECOVER")
        recovered = self.call("POST", "/api/kis-domestic-functional/recover", {
            "safetyContext": context, "safetyConfirmation": confirmation,
        })
        self.assertEqual(200, recovered.status); self.assertEqual(1, self.manager_calls["recover"])
        self.assertEqual("CLEANUP", recovered.body["stateResult"]["phase"])

    def test_malformed_duplicate_or_extra_json_never_calls_state(self) -> None:
        reads = []
        malformed = self.server.handle(
            method="POST", path="/api/kis-domestic-functional/start",
            headers=self.headers(), peer_host="127.0.0.1",
            body_reader=lambda: reads.append(True) or b'{"sessionId":"a","sessionId":"b"}',
        )
        self.assertEqual(400, malformed.status); self.assertEqual([True], reads)
        context, confirmation = self.confirmation("KIS_START")
        extra = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-extra", "safetyContext": context,
            "safetyConfirmation": confirmation, "extra": True,
        })
        self.assertEqual(400, extra.status); self.assertEqual(0, self.manager_calls["start"])

    def test_remote_peer_and_duplicate_origin_cookie_are_pre_body_denials(self) -> None:
        cases = []
        duplicate_origin = self.headers(); duplicate_origin.add_header("Origin", self.authority.expected_origin)
        cases.append((duplicate_origin, "127.0.0.1"))
        duplicate_cookie = self.headers(); duplicate_cookie.add_header("Cookie", f"{APP_SESSION_COOKIE}={self.authority.app_session_token}")
        cases.append((duplicate_cookie, "127.0.0.1"))
        cases.append((self.headers(), "192.0.2.10"))
        for headers, peer in cases:
            reads = []
            result = self.call("POST", "/api/kis-domestic-functional/recover", {}, headers=headers, peer=peer, reads=reads)
            self.assertEqual(403, result.status); self.assertEqual([], reads)
        self.assertEqual(0, self.manager_calls["recover"])

    def test_stale_state_revision_confirmation_is_burned_before_manager(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        self.state.apply_settings(
            account_fingerprint=ACCOUNT,
            credential_configuration_hash=CREDENTIAL,
            manager=lambda reservation: {
                "ok": True, "mutationMayHaveOccurred": False,
                "receiptHash": "e" * 64,
            },
        )
        result = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-stale-state", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(409, result.status)
        self.assertEqual("safety-context-not-approved", result.body["reason"])
        self.assertEqual(0, self.manager_calls["start"])
        self.assertEqual(0, self.safety.pending_count_for_tests())

    def test_account_and_credential_rotation_invalidates_approved_base(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        new_account, new_credential = "c" * 64, "d" * 64
        def settings_manager(reservation):
            self.account = new_account; self.credential = new_credential
            return {"ok": True, "mutationMayHaveOccurred": False, "receiptHash": "d" * 64}
        self.state.apply_settings(
            account_fingerprint=new_account,
            credential_configuration_hash=new_credential,
            manager=settings_manager,
        )
        result = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-rotated-account", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(409, result.status)
        self.assertIn("account/credential", result.body["reason"])
        self.assertEqual(0, self.manager_calls["start"])

    def test_signed_one_shot_history_tamper_blocks_challenge(self) -> None:
        with self.ledger.connection() as conn:
            conn.execute(
                "UPDATE kis_functional_server_safety_transition SET signature=? WHERE revision=1",
                ("0" * 64,),
            )
        result = self.call("POST", "/api/kis-domestic-functional/safety-confirmation/challenge", {
            "action": "KIS_START", "safetyContext": self.context("KIS_START"),
        })
        self.assertEqual(409, result.status)
        self.assertIn("integrity", result.body["reason"])

    def test_manager_reader_detects_any_dispatch_before_callback(self) -> None:
        context, confirmation = self.confirmation("KIS_START")
        self.manager_status["start"]["networkDispatchCount"] = 1
        result = self.call("POST", "/api/kis-domestic-functional/start", {
            "sessionId": "kis-session-manager-drift", "safetyContext": context,
            "safetyConfirmation": confirmation,
        })
        self.assertEqual(409, result.status)
        self.assertIn("networkDispatchCount", result.body["reason"])
        self.assertEqual(0, self.manager_calls["start"])

    def test_unknown_functional_path_authenticates_before_body_and_path_types_fail(self) -> None:
        reads = []
        denied = self.call(
            "POST", "/api/kis-domestic-functional/unknown", {},
            headers=self.headers(cookie=False), reads=reads,
        )
        self.assertEqual(403, denied.status); self.assertEqual([], reads)
        bad = self.server.handle(
            method="GET", path=None, headers=self.headers(mutation=False),
            peer_host="127.0.0.1", body_reader=lambda: b"{}",
        )
        self.assertEqual(404, bad.status)


if __name__ == "__main__":
    unittest.main()
