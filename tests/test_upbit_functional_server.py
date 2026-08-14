from __future__ import annotations

from contextlib import closing, contextmanager
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
import unittest
import sqlite3
import tempfile
from unittest.mock import Mock, patch

from live_trader import state
from live_trader import upbit_order_authority
from live_trader.functional_http_session import (
    APP_SESSION_COOKIE,
    CSRF_HEADER,
    FunctionalHttpSessionAuthority,
)
from live_trader.server import LiveTraderHandler


class UpbitFunctionalServerContractTest(unittest.TestCase):
    def trusted_handler(
        self, path: str, payload: dict[str, object] | None = None
    ) -> LiveTraderHandler:
        authority = FunctionalHttpSessionAuthority.mint(
            host="127.0.0.1", port=18795
        )
        handler = object.__new__(LiveTraderHandler)
        handler.path = path
        handler.server = SimpleNamespace(
            functional_http_session_authority=authority
        )
        handler.client_address = ("127.0.0.1", 50000)
        handler.headers = Message()
        handler.headers["Host"] = authority.expected_host_header
        handler.headers["Origin"] = authority.expected_origin
        handler.headers["Cookie"] = (
            f"{APP_SESSION_COOKIE}={authority.app_session_token}"
        )
        handler.headers[CSRF_HEADER] = authority.csrf_token
        handler.read_json = Mock(return_value=dict(payload or {}))
        handler.send_json = Mock()
        return handler

    def test_ordinary_upbit_boundary_yields_once_and_rechecks_authority(
        self,
    ) -> None:
        events: list[str] = []

        @contextmanager
        def emergency_boundary():
            events.append("emergency-enter")
            yield {"active": False, "revision": "clear-1"}
            events.append("emergency-exit")

        authority = Mock(side_effect=(False, False))
        with (
            patch.object(
                state,
                "_upbit_functional_durable_authority_open",
                authority,
            ),
            patch.object(
                state,
                "live_trader_instance_lease_status",
                return_value={"acquired": True},
            ),
            patch.object(state, "real_orders_enabled", return_value=True),
            patch.object(
                upbit_order_authority,
                "emergency_stop_dispatch_boundary",
                emergency_boundary,
            ),
        ):
            with state._ordinary_upbit_final_mutation_boundary(
                "upbit"
            ) as boundary:
                events.append("sender")
                self.assertFalse(boundary["active"])
        self.assertEqual(
            ["emergency-enter", "sender", "emergency-exit"], events
        )
        self.assertEqual(2, authority.call_count)

    def test_keyword_kill_recovers_once_after_all_mutation_locks_are_released(
        self,
    ) -> None:
        observed: dict[str, bool] = {}

        def recover_once() -> dict[str, object]:
            observed["safetyOwned"] = bool(
                state.SAFETY_CONFIRMATION_MUTATION_LOCK._is_owned()
            )
            observed["upbitOwned"] = bool(
                state.UPBIT_ORDER_AUTHORITY_MUTATION_LOCK.owned_by_current_thread()
            )
            observed["runtimeOwned"] = bool(
                state.RUNTIME_CONTROL_LOCK._is_owned()
            )
            return {
                "ok": True,
                "upbit_functional_cleanup": {
                    "ok": True,
                    "state": "CLEANUP",
                },
                "binance_functional_cleanup": {
                    "ok": True,
                    "state": "CLEANUP",
                },
            }

        with (
            patch.dict(
                state.STATE,
                {
                    "kill_switch": False,
                    "kill_switch_rearm_required": False,
                    "new_entries_blocked": False,
                },
            ),
            patch.object(
                state,
                "emergency_stop_active",
                return_value=False,
            ),
            patch.object(
                state,
                "engage_emergency_stop",
                return_value={"ok": True, "active": True},
            ),
            patch.object(state, "append_audit"),
            patch.object(state, "snapshot", return_value={}),
            patch.object(
                state,
                "recover_durable_emergency_stop",
                side_effect=recover_once,
            ) as recover,
        ):
            result = state.set_flag(name="kill_switch", value=True)

        recover.assert_called_once_with()
        self.assertEqual(
            {"safetyOwned": False, "upbitOwned": False, "runtimeOwned": False},
            observed,
        )
        self.assertEqual("CLEANUP", result["upbitFunctionalCleanup"]["state"])
        self.assertEqual(
            "CLEANUP", result["binanceFunctionalCleanup"]["state"]
        )

    def test_ordinary_upbit_boundary_blocks_before_emergency_when_functional_active(
        self,
    ) -> None:
        emergency = Mock()
        with (
            patch.object(
                state,
                "_upbit_functional_durable_authority_open",
                return_value=True,
            ),
            patch.object(
                upbit_order_authority,
                "emergency_stop_dispatch_boundary",
                emergency,
            ),
            self.assertRaisesRegex(
                RuntimeError,
                "upbit-functional-authority-blocks-ordinary-mutation",
            ),
        ):
            with state._ordinary_upbit_final_mutation_boundary("upbit"):
                self.fail("ordinary sender must not be reached")
        emergency.assert_not_called()

    def test_binance_boundary_is_owned_only_by_binance_broker_edge(self) -> None:
        emergency = Mock(
            side_effect=AssertionError("outer emergency lock is forbidden")
        )
        with patch.object(
            state, "emergency_stop_dispatch_boundary", emergency
        ):
            with state._ordinary_upbit_final_mutation_boundary(
                "binance"
            ) as boundary:
                self.assertEqual(
                    "BINANCE_BROKER_EDGE", boundary["boundaryOwner"]
                )
        emergency.assert_not_called()

    def test_cleanup_only_pointer_blocks_strategy_but_allows_exact_cleanup_claim(
        self,
    ) -> None:
        class Store:
            cleanup = False

            def cleanup_claim_authority(self, **_kwargs):
                return self.cleanup

            @staticmethod
            def active_pointer():
                return {
                    "claimed_session_id": "upbit-stop-session-0001",
                    "permit_hash": "a" * 64,
                    "cleanup_only": 1,
                }

        store = Store()

        @contextmanager
        def emergency_boundary():
            yield {"active": False, "revision": "clear-1"}

        patches = (
            patch.object(state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", store),
            patch.object(
                state,
                "_upbit_functional_ordinary_routes_closed",
                return_value=True,
            ),
            patch.object(
                state,
                "emergency_stop_dispatch_boundary",
                emergency_boundary,
            ),
            patch.object(
                state,
                "hold_process_lease",
                return_value={
                    "acquired": True,
                    "ownerPid": 1,
                    "scopeHash": "b" * 64,
                },
            ),
        )
        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(
                RuntimeError, "cleanup-only-entry-forbidden"
            ):
                with state._upbit_functional_dispatch_lease(
                    session_id="upbit-stop-session-0001",
                    claim_id="strategy-claim-0001",
                ):
                    self.fail("strategy POST must not be reached")
            store.cleanup = True
            with state._upbit_functional_dispatch_lease(
                session_id="upbit-stop-session-0001",
                claim_id="cleanup-claim-0001",
            ) as lease_reader:
                lease = lease_reader()
            self.assertTrue(lease["active"])
            self.assertTrue(lease["durableCleanupOnly"])
            self.assertTrue(lease["killCleanupOnly"])

    def test_kill_durable_cleanup_without_manager_requires_reconciliation(
        self,
    ) -> None:
        latch = {
            "ok": True,
            "state": "CLEANUP_ONLY",
            "sessionId": "upbit-orphan-cleanup-session-0001",
            "cleanupOnly": True,
        }
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                return_value=latch,
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "upbit_functional_backend_status",
                return_value={"prepared": False, "sessionId": ""},
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "stop_upbit_functional_backend"
            ) as stop,
        ):
            result = state._upbit_functional_emergency_cleanup_after_latch()
        self.assertFalse(result["ok"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertEqual(latch["sessionId"], result["sessionId"])
        self.assertTrue(result["entryAuthorityRevoked"])
        stop.assert_not_called()

    def test_kill_without_durable_pointer_or_manager_is_no_active_success(
        self,
    ) -> None:
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                return_value={"ok": True, "state": "NO_ACTIVE_FUNCTIONAL_SESSION"},
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "upbit_functional_backend_status",
                return_value={"prepared": False, "sessionId": ""},
            ),
        ):
            result = state._upbit_functional_emergency_cleanup_after_latch()
        self.assertTrue(result["ok"])
        self.assertEqual("NO_ACTIVE_FUNCTIONAL_SESSION", result["state"])

    def test_missing_singleton_reads_durable_pointer_and_blocks_ordinary_route(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "upbit-functional.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """CREATE TABLE upbit_functional_approvals (
                    approval_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    "INSERT INTO upbit_functional_approvals VALUES (?,?)",
                    ("upbit-durable-active-0001", "ACTIVE"),
                )
                connection.commit()
            with (
                patch.object(state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", None),
                patch.object(state, "UPBIT_FUNCTIONAL_DATABASE_PATH", path),
            ):
                self.assertTrue(
                    state._upbit_functional_durable_authority_open()
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "authority-blocks-ordinary-mutation",
                ):
                    with state._ordinary_upbit_final_mutation_boundary("upbit"):
                        self.fail("ordinary route must stay closed")

    def test_missing_singleton_empty_db_has_no_durable_authority(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "upbit-functional.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """CREATE TABLE upbit_functional_approvals (
                    approval_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_sessions (
                    session_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_claims (
                    claim_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                    state TEXT NOT NULL)"""
                )
                connection.commit()
            with (
                patch.object(state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", None),
                patch.object(state, "UPBIT_FUNCTIONAL_DATABASE_PATH", path),
            ):
                self.assertFalse(
                    state._upbit_functional_durable_authority_open()
                )

    def test_missing_singleton_orphan_nonterminal_session_keeps_authority_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "upbit-functional.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """CREATE TABLE upbit_functional_approvals (
                    approval_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_sessions (
                    session_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_claims (
                    claim_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                    state TEXT NOT NULL)"""
                )
                connection.execute(
                    "INSERT INTO upbit_functional_sessions VALUES (?,?)",
                    ("upbit-orphan-durable-session-0001", "CLEANUP"),
                )
                connection.commit()
            with (
                patch.object(state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", None),
                patch.object(state, "UPBIT_FUNCTIONAL_DATABASE_PATH", path),
            ):
                self.assertTrue(
                    state._upbit_functional_durable_authority_open()
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "authority-blocks-ordinary-mutation",
                ):
                    with state._ordinary_upbit_final_mutation_boundary("upbit"):
                        self.fail("orphan cleanup authority must stay closed")

    def test_present_store_cannot_hide_orphan_durable_session(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "upbit-functional.sqlite3"
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """CREATE TABLE upbit_functional_approvals (
                    approval_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_sessions (
                    session_id TEXT PRIMARY KEY,state TEXT NOT NULL)"""
                )
                connection.execute(
                    """CREATE TABLE upbit_functional_claims (
                    claim_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                    state TEXT NOT NULL)"""
                )
                connection.execute(
                    "INSERT INTO upbit_functional_sessions VALUES (?,?)",
                    ("upbit-present-store-orphan-0001", "ACTIVE"),
                )
                connection.commit()

            class PointerlessStore:
                def __init__(self, database_path: Path) -> None:
                    self.path = database_path

                @staticmethod
                def order_authority_pointer():
                    return None

            with patch.object(
                state,
                "_UPBIT_FUNCTIONAL_APPROVAL_STORE",
                PointerlessStore(path),
            ):
                self.assertTrue(
                    state._upbit_functional_durable_authority_open()
                )

    def test_kill_store_unavailable_with_durable_pointer_requires_reconciliation(
        self,
    ) -> None:
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                return_value={
                    "ok": False,
                    "state": "RECONCILIATION_REQUIRED",
                    "reason": "durable-authority-store-unavailable",
                },
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "upbit_functional_backend_status",
                return_value={"prepared": False, "sessionId": ""},
            ),
        ):
            result = state._upbit_functional_emergency_cleanup_after_latch()
        self.assertFalse(result["ok"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertFalse(result["entryAuthorityRevoked"])

    def test_kill_latch_and_manager_session_mismatch_stops_neither(self) -> None:
        latch = {
            "ok": True,
            "state": "CLEANUP_ONLY",
            "sessionId": "upbit-cleanup-session-a-0001",
            "cleanupOnly": True,
        }
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                return_value=latch,
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "upbit_functional_backend_status",
                return_value={
                    "prepared": True,
                    "sessionId": "upbit-cleanup-session-b-0001",
                },
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "stop_upbit_functional_backend"
            ) as stop,
        ):
            result = state._upbit_functional_emergency_cleanup_after_latch()
        self.assertFalse(result["ok"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertTrue(result["entryAuthorityRevoked"])
        stop.assert_not_called()

    def test_kill_cleanup_exception_never_claims_entry_revocation(self) -> None:
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                side_effect=RuntimeError("durable-store-unreadable"),
            ),
        ):
            result = state._upbit_functional_emergency_cleanup_after_latch()
        self.assertFalse(result["ok"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])
        self.assertFalse(result["entryAuthorityRevoked"])

    def test_stop_durably_revokes_entry_before_waiting_for_manager(self) -> None:
        calls: list[str] = []
        status = {
            "prepared": True,
            "sessionId": "upbit-stop-session-0001",
        }
        payload = {
            "operatorConfirmation": {
                "challengeId": "challenge-1",
                "token": "token-1",
                "typedPhrase": "LIVE ABCD",
            }
        }
        with (
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value=status,
            ),
            patch.object(
                state,
                "_consume_upbit_functional_operator_confirmation",
                return_value={"ok": True},
            ),
            patch.object(
                state,
                "_revoke_crypto_first_live_entry_before_cleanup",
                return_value={"ok": True, "entryAuthorityRevoked": True},
            ),
            patch.object(
                state,
                "_request_upbit_functional_cleanup_only",
                side_effect=lambda _reason: (
                    calls.append("durable-entry-revoked")
                    or {"ok": True, "cleanupOnly": True}
                ),
            ),
            patch(
                "live_trader.upbit_functional_backend."
                "stop_upbit_functional_backend",
                side_effect=lambda _command: (
                    calls.append("manager-cleanup") or {"ok": True}
                ),
            ),
        ):
            result = state.stop_upbit_functional_backend_state(payload)
        self.assertTrue(result["ok"])
        self.assertEqual(
            ["durable-entry-revoked", "manager-cleanup"], calls
        )

    def test_route_fence_is_never_held_while_waiting_for_backend_manager(self) -> None:
        with patch.object(
            state, "_upbit_functional_durable_authority_open", return_value=True
        ), patch.object(
            state, "_upbit_functional_ordinary_routes_closed", return_value=True
        ), patch(
            "live_trader.upbit_functional_backend."
            "preissue_upbit_functional_recovery_candidate",
            side_effect=lambda _requested="": (
                self.assertFalse(state._upbit_order_authority_lock_owned())
                or {
                    "recoveryId": "upbit-lock-order-recovery-0001",
                    "sessionId": "upbit-lock-order-session-0001",
                    "expiresAt": "2026-08-13T03:00:00Z",
                }
            ),
        ):
            result = state._preissue_upbit_functional_recovery_candidate("")
        self.assertEqual(
            "upbit-lock-order-recovery-0001", result["recoveryId"]
        )

        with patch(
            "live_trader.upbit_functional_backend."
            "upbit_functional_backend_status",
            side_effect=AssertionError("manager must not be entered"),
        ):
            with state.UPBIT_ORDER_AUTHORITY_MUTATION_LOCK:
                status = state.upbit_functional_backend_state_status()
        self.assertFalse(status["networkOrderPostAllowed"])
        self.assertTrue(status["liveStatusDeferredByRouteFence"])

    def test_start_challenge_bootstraps_server_owned_candidate_without_client_id(
        self,
    ) -> None:
        candidate = {
            "approvalId": "upbit-server-candidate-0001",
            "candidateHash": "a" * 64,
            "expiresAt": "2026-08-13T03:00:00Z",
        }

        def env_value(key: str):
            return "upbit-access-for-confirmation" if key == "UPBIT_ACCESS_KEY" else ""

        with (
            patch.object(state, "env_value", side_effect=env_value),
            patch.object(
                state,
                "_preissue_upbit_functional_permit_candidate",
                return_value=candidate,
            ) as preissue,
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value={
                    "prepared": True,
                    "available": True,
                    "networkOrderPostAllowed": True,
                    "terminalState": "IDLE",
                },
            ),
        ):
            issued = state.issue_safety_confirmation(
                "UPBIT_FUNCTIONAL_START", {}
            )
        self.assertTrue(issued["ok"])
        self.assertEqual(candidate["approvalId"], issued["approvalId"])
        self.assertEqual(candidate["expiresAt"], issued["permitExpiresAt"])
        preissue.assert_called_once_with("")

    def test_recovery_challenge_bootstraps_server_owned_id_without_client_evidence(
        self,
    ) -> None:
        candidate = {
            "recoveryId": "upbit-recovery-candidate-0001",
            "sessionId": "upbit-recovery-session-0001",
            "expiresAt": "2026-08-13T03:00:00Z",
        }

        def env_value(key: str):
            return "upbit-access-for-confirmation" if key == "UPBIT_ACCESS_KEY" else ""

        with (
            patch.object(state, "env_value", side_effect=env_value),
            patch.object(
                state,
                "_preissue_upbit_functional_recovery_candidate",
                return_value=candidate,
            ) as preissue,
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value={
                    "prepared": True,
                    "available": True,
                    "networkOrderPostAllowed": True,
                    "sessionId": candidate["sessionId"],
                    "terminalState": "RECONCILIATION_REQUIRED",
                },
            ),
        ):
            issued = state.issue_safety_confirmation(
                "UPBIT_FUNCTIONAL_RECOVER", {}
            )
        self.assertTrue(issued["ok"])
        self.assertEqual(candidate["recoveryId"], issued["recoveryId"])
        self.assertEqual(candidate["sessionId"], issued["sessionId"])
        preissue.assert_called_once_with("")

    def test_recover_consumes_confirmation_then_server_builds_fresh_rest_approval(
        self,
    ) -> None:
        status = {
            "prepared": True,
            "available": True,
            "sessionId": "upbit-recovery-session-0001",
        }
        payload = {
            "recoveryId": "upbit-recovery-candidate-0001",
            "operatorConfirmation": {
                "challengeId": "challenge-1",
                "token": "token-1",
                "typedPhrase": "LIVE ABCD",
            },
        }
        with (
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value=status,
            ),
            patch.object(
                state,
                "_consume_upbit_functional_operator_confirmation",
                return_value={"ok": True},
            ),
            patch.object(
                state,
                "_approve_upbit_functional_recovery_candidate",
                return_value={"recoveryHash": "a" * 64},
            ) as approve,
            patch(
                "live_trader.upbit_functional_backend."
                "recover_upbit_functional_backend",
                return_value={"ok": True},
            ) as recover,
        ):
            result = state.recover_upbit_functional_backend_state(payload)
        self.assertTrue(result["ok"])
        approve.assert_called_once_with(payload["recoveryId"])
        command = recover.call_args.args[0]
        self.assertEqual(
            {"recoveryId", "operatorConfirmation"}, set(command)
        )
        self.assertNotIn("officialRestTruth", command)

    def test_status_route_is_read_only_delegate(self) -> None:
        handler = self.trusted_handler("/api/upbit-functional/status")
        expected = {
            "prepared": False,
            "available": False,
            "networkOrderPostAllowed": False,
        }
        with patch(
            "live_trader.server.state.upbit_functional_backend_state_status",
            return_value=expected,
        ) as status:
            handler.do_GET()
        status.assert_called_once_with()
        handler.send_json.assert_called_once_with(expected)

    def test_mutation_routes_forward_whole_contract_without_sanitizing_extras(
        self,
    ) -> None:
        confirmation = {
            "challengeId": "challenge-1",
            "token": "token-1",
            "typedPhrase": "LIVE ABCD",
        }
        cases = (
            (
                "/api/upbit-functional/start",
                "start_upbit_functional_backend_state",
                {
                    "approvalId": "approved-record-1",
                    "operatorConfirmation": confirmation,
                    "signal": "CLIENT-FORBIDDEN",
                },
            ),
            (
                "/api/upbit-functional/stop",
                "stop_upbit_functional_backend_state",
                {"operatorConfirmation": confirmation},
            ),
            (
                "/api/upbit-functional/recover",
                "recover_upbit_functional_backend_state",
                {
                    "recoveryId": "recovery-record-1",
                    "operatorConfirmation": confirmation,
                },
            ),
        )
        for path, function_name, payload in cases:
            with self.subTest(path=path):
                handler = self.trusted_handler(path, payload)
                expected = {"ok": False, "reason": "test-result"}
                with patch(
                    f"live_trader.server.state.{function_name}",
                    return_value=expected,
                ) as function:
                    handler.do_POST()
                function.assert_called_once_with(payload)
                handler.send_json.assert_called_once_with(expected)

    def test_all_upbit_http_routes_deny_before_body_or_state_without_session(
        self,
    ) -> None:
        cases = (
            (
                "GET",
                "/api/upbit-functional/status",
                "upbit_functional_backend_state_status",
            ),
            (
                "POST",
                "/api/upbit-functional/start",
                "start_upbit_functional_backend_state",
            ),
            (
                "POST",
                "/api/upbit-functional/stop",
                "stop_upbit_functional_backend_state",
            ),
            (
                "POST",
                "/api/upbit-functional/recover",
                "recover_upbit_functional_backend_state",
            ),
        )
        for method, path, function_name in cases:
            with self.subTest(path=path):
                handler = self.trusted_handler(path, {"caller": "untrusted"})
                del handler.headers["Cookie"]
                handler._send_functional_http_denial = Mock()
                with patch(
                    f"live_trader.server.state.{function_name}"
                ) as function:
                    getattr(handler, f"do_{method}")()
                handler._send_functional_http_denial.assert_called_once()
                function.assert_not_called()
                handler.read_json.assert_not_called()

    def test_upbit_start_rejects_each_mutation_edge_attestation_pre_body(
        self,
    ) -> None:
        mutations = (
            lambda handler: handler.headers.__delitem__("Host"),
            lambda handler: handler.headers.__delitem__("Origin"),
            lambda handler: handler.headers.__delitem__(CSRF_HEADER),
            lambda handler: setattr(
                handler, "client_address", ("192.0.2.1", 50000)
            ),
        )
        for mutate in mutations:
            handler = self.trusted_handler(
                "/api/upbit-functional/start", {"caller": "untrusted"}
            )
            mutate(handler)
            handler._send_functional_http_denial = Mock()
            with patch(
                "live_trader.server.state.start_upbit_functional_backend_state"
            ) as start:
                handler.do_POST()
            handler._send_functional_http_denial.assert_called_once()
            handler.read_json.assert_not_called()
            start.assert_not_called()

    def test_raw_permit_bar_signal_capability_and_extra_fields_are_rejected(
        self,
    ) -> None:
        confirmation = {
            "challengeId": "challenge-1",
            "token": "token-1",
            "typedPhrase": "LIVE ABCD",
        }
        base = {
            "approvalId": "approved-record-1",
            "operatorConfirmation": confirmation,
        }
        for field in (
            "permit",
            "bar",
            "signal",
            "capability",
            "accountFingerprint",
            "payload",
            "extra",
        ):
            with self.subTest(field=field), patch(
                "live_trader.upbit_functional_backend."
                "start_upbit_functional_backend"
            ) as start:
                result = state.start_upbit_functional_backend_state(
                    {**base, field: "forbidden"}
                )
            self.assertFalse(result["ok"])
            self.assertFalse(result["brokerSubmissionPerformed"])
            self.assertIn("fields-not-exact", result["reason"])
            start.assert_not_called()

    def test_start_consumes_existing_server_confirmation_then_replaces_it(
        self,
    ) -> None:
        status = {
            "ok": True,
            "prepared": True,
            "available": True,
            "networkOrderPostAllowed": True,
            "approvalId": "",
            "sessionId": "",
            "generation": 0,
            "terminalState": "IDLE",
        }

        def env_value(key: str):
            return "upbit-access-for-confirmation" if key == "UPBIT_ACCESS_KEY" else ""

        with (
            patch.object(state, "env_value", side_effect=env_value),
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value=status,
            ),
            patch.object(
                state,
                "_preissue_upbit_functional_permit_candidate",
                return_value={
                    "approvalId": "approved-record-1",
                    "candidateHash": "b" * 64,
                    "expiresAt": "2026-08-13T03:00:00Z",
                },
            ),
        ):
            issued = state.issue_safety_confirmation(
                "UPBIT_FUNCTIONAL_START",
                {"approvalId": "approved-record-1"},
            )
            self.assertTrue(issued["ok"])
            confirmation = {
                "challengeId": issued["challengeId"],
                "token": issued["token"],
                "typedPhrase": issued["expectedPhrase"],
            }
            with patch(
                "live_trader.upbit_functional_backend."
                "start_upbit_functional_backend",
                return_value={"ok": True},
            ) as start, patch.object(
                state,
                "_crypto_first_live_official_candidate_hold",
                return_value="",
            ), patch.object(
                state,
                "_approve_upbit_functional_permit_candidate",
                return_value={"state": "APPROVED"},
            ), patch.object(
                state,
                "_upbit_functional_ordinary_routes_closed",
                return_value=True,
            ), patch.object(
                state,
                "_UPBIT_FUNCTIONAL_APPROVAL_STORE",
                SimpleNamespace(
                    permit_status=lambda _approval_id: {
                        "candidate_hash": "b" * 64
                    }
                ),
            ):
                result = state.start_upbit_functional_backend_state(
                    {
                        "approvalId": "approved-record-1",
                        "operatorConfirmation": confirmation,
                    }
                )

        self.assertTrue(result["ok"])
        command = start.call_args.args[0]
        self.assertEqual(
            {"approvalId", "operatorConfirmation"}, set(command)
        )
        internal = command["operatorConfirmation"]
        self.assertTrue(internal["authenticated"])
        self.assertTrue(internal["confirmed"])
        self.assertEqual("SERVER_SAFETY_CONFIRMATION", internal["source"])
        self.assertNotEqual(confirmation["token"], internal["serverSignature"])
        self.assertNotIn("challengeId", internal)

    def test_unavailable_start_does_not_consume_confirmation_or_call_backend(
        self,
    ) -> None:
        payload = {
            "approvalId": "approved-record-1",
            "operatorConfirmation": {
                "challengeId": "challenge-1",
                "token": "token-1",
                "typedPhrase": "LIVE ABCD",
            },
        }
        with (
            patch.object(
                state,
                "upbit_functional_backend_state_status",
                return_value={"prepared": True, "available": False},
            ),
            patch.object(
                state, "_consume_upbit_functional_operator_confirmation"
            ) as consume,
            patch(
                "live_trader.upbit_functional_backend."
                "start_upbit_functional_backend"
            ) as start,
        ):
            result = state.start_upbit_functional_backend_state(payload)
        self.assertFalse(result["ok"])
        consume.assert_not_called()
        start.assert_not_called()

    def test_prepare_is_lazy_fail_closed_and_uses_production_dependencies(
        self,
    ) -> None:
        old_store = state._UPBIT_FUNCTIONAL_APPROVAL_STORE
        old_status = dict(state._UPBIT_FUNCTIONAL_PREPARE_STATUS)
        self.addCleanup(
            lambda: setattr(
                state, "_UPBIT_FUNCTIONAL_APPROVAL_STORE", old_store
            )
        )
        self.addCleanup(
            lambda: setattr(
                state, "_UPBIT_FUNCTIONAL_PREPARE_STATUS", old_status
            )
        )
        state._UPBIT_FUNCTIONAL_APPROVAL_STORE = None
        with patch.object(state, "env_value", return_value=""):
            missing = state.prepare_upbit_functional_backend_state()
        self.assertFalse(missing["prepared"])
        self.assertFalse(missing["networkOrderPostAllowed"])

        def configured(key: str):
            values = {
                "UPBIT_ACCESS_KEY": "access",
                "UPBIT_SECRET_KEY": "secret",
                "UPBIT_BASE_URL": "https://api.upbit.com",
            }
            return values.get(key, "")

        fake_store = object()
        with (
            patch.object(state, "env_value", side_effect=configured),
            patch.object(Path, "is_file", return_value=True),
            patch(
                "live_trader.upbit_functional_approval."
                "DurableUpbitFunctionalApprovalStore",
                return_value=fake_store,
            ) as store,
            patch(
                "live_trader.upbit_functional_transport."
                "resolve_upbit_functional_base_url",
                return_value="https://api.upbit.com",
            ) as origin,
            patch(
                "live_trader.upbit_functional_backend."
                "prepare_upbit_functional_backend",
                return_value={
                    "ok": True,
                    "prepared": True,
                    "status": {
                        "available": False,
                        "networkOrderPostAllowed": False,
                        "startupAudit": {"complete": True},
                    },
                },
            ) as prepare,
        ):
            ready = state.prepare_upbit_functional_backend_state()
        self.assertTrue(ready["prepared"])
        self.assertFalse(ready["available"])
        self.assertFalse(ready["networkOrderPostAllowed"])
        store.assert_called_once()
        origin.assert_called_once_with()
        kwargs = prepare.call_args.kwargs
        self.assertIs(fake_store, kwargs["approval_store"])
        self.assertIs(state.send_prepared_request, kwargs["sender"])
        self.assertNotIn("allow_mock_graph", kwargs)


if __name__ == "__main__":
    unittest.main()
