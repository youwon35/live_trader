from __future__ import annotations

import copy
import unittest
from email.message import Message
from types import SimpleNamespace
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.server import LiveTraderHandler
from live_trader.functional_http_session import (
    APP_SESSION_COOKIE,
    CSRF_HEADER,
    FunctionalHttpSessionAuthority,
)


class BinanceSpotFunctionalServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.addCleanup(self._restore_state)

    def _restore_state(self) -> None:
        state.STATE.clear()
        state.STATE.update(copy.deepcopy(self.original_state))

    def handler(self, path: str, payload: dict | None = None):
        handler = object.__new__(LiveTraderHandler)
        authority = FunctionalHttpSessionAuthority.mint(
            host="127.0.0.1", port=18795
        )
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

    def test_status_route_is_read_only(self) -> None:
        handler = self.handler("/api/binance-spot-functional/status")
        expected = {
            "prepared": True,
            "available": False,
            "networkOrderPostAllowed": False,
        }
        with patch(
            "live_trader.server.state."
            "binance_spot_functional_backend_state_status",
            return_value=expected,
        ) as status:
            handler.do_GET()
        status.assert_called_once_with()
        handler.send_json.assert_called_once_with(expected)

    def test_start_stop_recover_routes_forward_only_exact_server_commands(self) -> None:
        confirmation = {
            "challengeId": "challenge-1",
            "token": "token-1",
            "typedPhrase": "LIVE ABCD",
        }
        cases = (
            (
                "/api/binance-spot-functional/start",
                {
                    "approvalId": "binance-functional-approval-1",
                    "operatorConfirmation": confirmation,
                },
                "start_binance_spot_functional_backend_state",
            ),
            (
                "/api/binance-spot-functional/stop",
                {"operatorConfirmation": confirmation},
                "stop_binance_spot_functional_backend_state",
            ),
            (
                "/api/binance-spot-functional/recover",
                {"operatorConfirmation": confirmation},
                "recover_binance_spot_functional_backend_state",
            ),
        )
        for path, payload, name in cases:
            with self.subTest(path=path):
                handler = self.handler(path, payload)
                with patch(
                    "live_trader.server.state." + name,
                    return_value={"ok": False, "available": False},
                ) as command:
                    handler.do_POST()
                command.assert_called_once_with(payload)

    def test_http_start_rejects_raw_permit_bar_signal_and_capability(self) -> None:
        confirmation = {
            "challengeId": "challenge-1",
            "token": "token-1",
            "typedPhrase": "LIVE ABCD",
        }
        with patch.object(
            state,
            "binance_spot_functional_backend_state_status",
            return_value={"prepared": False, "available": False},
        ):
            for forbidden in ("permit", "bar", "signal", "capability"):
                with self.subTest(forbidden=forbidden):
                    result = state.start_binance_spot_functional_backend_state(
                        {
                            "approvalId": "approval-1",
                            "operatorConfirmation": confirmation,
                            forbidden: {"caller": "controlled"},
                        }
                    )
                    self.assertFalse(result["ok"])
                    self.assertFalse(result["brokerSubmissionPerformed"])
                    self.assertIn("fields-not-exact", result["reason"])

    def test_hostile_functional_post_is_rejected_before_body_or_state(self) -> None:
        handler = self.handler(
            "/api/binance-spot-functional/start",
            {"caller": "must-not-be-read"},
        )
        del handler.headers[CSRF_HEADER]
        handler._send_functional_http_denial = Mock()
        with patch(
            "live_trader.server.state."
            "start_binance_spot_functional_backend_state"
        ) as start:
            handler.do_POST()
        handler.read_json.assert_not_called()
        start.assert_not_called()
        handler._send_functional_http_denial.assert_called_once()

    def test_unauthenticated_challenge_never_exposes_phrase_or_token(self) -> None:
        handler = self.handler(
            "/api/safety-confirmation/challenge",
            {"action": "BINANCE_SPOT_FUNCTIONAL_START"},
        )
        handler.headers.replace_header("Origin", "https://evil.example")
        handler._send_functional_http_denial = Mock()
        with patch(
            "live_trader.server.state.issue_safety_confirmation",
            return_value={
                "ok": True,
                "expectedPhrase": "MUST NEVER LEAK",
                "token": "must-never-leak",
            },
        ) as issue:
            handler.do_POST()
        handler.read_json.assert_not_called()
        issue.assert_not_called()
        handler.send_json.assert_not_called()
        handler._send_functional_http_denial.assert_called_once()

    def test_safety_challenge_preissues_server_candidate(self) -> None:
        preissued = {
            "approvalId": "binance-functional-approval-00000001",
            "permitExpiresAt": "2026-08-13T10:00:00Z",
            "firstLiveBootstrapId": "binance-first-live-000000000001",
            "firstLiveBootstrapHash": "a" * 64,
            "firstLiveSessionNonceHash": "b" * 64,
            "firstLiveCodeHash": "c" * 64,
            "accountFingerprint": "d" * 64,
            "bindingHash": "e" * 64,
        }
        with (
            patch.object(
                state,
                "_preissue_binance_spot_functional_candidate",
                return_value=preissued,
            ) as preissue,
            patch.object(
                state,
                "binance_spot_functional_backend_state_status",
                return_value={
                    "prepared": True,
                    "available": False,
                    "networkOrderPostAllowed": False,
                },
            ),
            patch.object(
                state.SAFETY_CONFIRMATIONS,
                "issue",
                return_value={"ok": True, "challengeId": "challenge-1"},
            ) as issue,
        ):
            result = state.issue_safety_confirmation(
                "BINANCE_SPOT_FUNCTIONAL_START", {}
            )
        preissue.assert_called_once_with("")
        self.assertEqual(preissued["approvalId"], result["approvalId"])
        context = issue.call_args.kwargs["context"]
        self.assertEqual(
            preissued["approvalId"], context["request"]["approvalId"]
        )
        self.assertEqual(
            preissued["firstLiveCodeHash"], context["request"]["codeHash"]
        )
        self.assertEqual(
            "BTCUSDT", issue.call_args.kwargs["display_context"]["symbol"]
        )

    def test_start_consumes_safety_then_calls_hidden_facade(self) -> None:
        facade = Mock()
        facade.start.return_value = {
            "ok": True,
            "sessionId": "server-owned-session",
        }
        confirmation = {
            "challengeId": "challenge-1",
            "token": "token-1",
            "typedPhrase": "LIVE ABCD",
        }
        with (
            patch.object(
                state,
                "binance_spot_functional_backend_state_status",
                return_value={
                    "prepared": True,
                    "available": False,
                    "candidateIssuanceAvailable": True,
                },
            ),
            patch.object(
                state,
                "_preissue_binance_spot_functional_candidate",
                return_value={
                    "approvalId": "binance-functional-approval-00000001",
                    "firstLiveBootstrapId": "binance-first-live-000000000001",
                    "firstLiveBootstrapHash": "a" * 64,
                    "firstLiveSessionNonceHash": "b" * 64,
                    "firstLiveCodeHash": "c" * 64,
                    "accountFingerprint": "d" * 64,
                    "bindingHash": "e" * 64,
                },
            ),
            patch.object(
                state,
                "_consume_binance_spot_functional_confirmation",
                return_value={"ok": True},
            ) as consume,
            patch.object(
                state,
                "_crypto_first_live_official_candidate_hold",
                return_value="",
            ),
            patch.object(
                state,
                "_binance_spot_functional_ordinary_routes_closed",
                return_value=True,
            ),
            patch.object(
                state,
                "_binance_spot_functional_facade",
                return_value=facade,
            ),
        ):
            result = state.start_binance_spot_functional_backend_state(
                {
                    "approvalId": "binance-functional-approval-00000001",
                    "operatorConfirmation": confirmation,
                }
            )
        self.assertTrue(result["ok"])
        consume.assert_called_once()
        self.assertEqual(
            "c" * 64,
            consume.call_args.kwargs["context"]["codeHash"],
        )
        facade.start.assert_called_once_with(
            "binance-functional-approval-00000001"
        )

    def test_api_kill_attaches_binance_cleanup_after_safety_lock_release(
        self,
    ) -> None:
        lock_owned: list[bool] = []

        def recovery() -> dict[str, object]:
            checker = getattr(
                state.SAFETY_CONFIRMATION_MUTATION_LOCK,
                "_is_owned",
                lambda: False,
            )
            lock_owned.append(bool(checker()))
            return {
                "ok": True,
                "upbit_functional_cleanup": {"ok": True},
                "binance_functional_cleanup": {
                    "ok": True,
                    "state": "CLEANUP",
                    "entryAuthorityRevoked": True,
                },
            }

        def set_flag(name: str, value: bool) -> dict[str, object]:
            self.assertEqual("kill_switch", name)
            self.assertTrue(value)
            return {"ok": True}

        wrapped = state._serialized_safety_mutation(set_flag)
        with patch.object(
            state,
            "recover_durable_emergency_stop",
            side_effect=recovery,
        ) as recover:
            result = wrapped(name="kill_switch", value=True)

        self.assertTrue(result["binanceFunctionalCleanup"]["ok"])
        self.assertEqual([False], lock_owned)
        recover.assert_called_once_with()

    def test_durable_kill_recovery_requires_binance_cleanup_result(self) -> None:
        lock_owned: list[bool] = []

        def binance_cleanup() -> dict[str, object]:
            checker = getattr(
                state.RUNTIME_CONTROL_LOCK,
                "_is_owned",
                lambda: False,
            )
            lock_owned.append(bool(checker()))
            return {
                "ok": True,
                "state": "CLEANUP",
                "entryAuthorityRevoked": True,
                "cleanupSchedulerOwned": True,
            }

        with (
            patch.object(
                state,
                "emergency_stop_status",
                return_value={"active": True, "revision": "kill-1"},
            ),
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "transition_running",
                return_value={"ok": True},
            ),
            patch.object(state, "sync_runtime_profile_mode"),
            patch.object(
                state,
                "refresh_kis_order_truth_for_kill_switch",
                return_value={"truth": {}},
            ),
            patch.object(
                state,
                "cancel_working_orders_for_kill_switch",
                return_value={
                    "cleanup_complete": True,
                    "attempted": 0,
                    "nonOwnedCancellationCount": 0,
                },
            ),
            patch.object(
                state,
                "stop_operational_runtime_sessions_for_kill",
                return_value={},
            ),
            patch.object(
                state,
                "run_reconciliation",
                return_value={"ok": True},
            ),
            patch.object(
                state,
                "_revoke_crypto_first_live_entry_before_cleanup",
                return_value={"ok": True, "entryAuthorityRevoked": True},
            ),
            patch.object(
                state,
                "_upbit_functional_emergency_cleanup_after_latch",
                return_value={"ok": True},
            ),
            patch.object(
                state,
                "_binance_spot_functional_emergency_cleanup_after_latch",
                side_effect=binance_cleanup,
            ) as cleanup,
        ):
            result = state._apply_durable_emergency_stop_recovery()

        self.assertTrue(result["ok"])
        self.assertEqual(0, result["cancellation"]["attempted"])
        self.assertEqual(
            "CLEANUP", result["binance_functional_cleanup"]["state"]
        )
        self.assertEqual([False], lock_owned)
        cleanup.assert_called_once_with()

    def test_kill_during_hold_revokes_entry_and_retains_cleanup_owner(self) -> None:
        facade = Mock()
        facade.order_authority_snapshot.side_effect = [
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
                "functionalSessionId": "session-hold",
            },
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "CLEANUP",
                "functionalSessionId": "session-hold",
            },
        ]
        facade.stop.return_value = {
            "ok": True,
            "state": "CLEANUP",
            "cleanupSchedulerOwned": True,
        }
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(
                state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade
            ),
        ):
            result = (
                state._binance_spot_functional_emergency_cleanup_after_latch()
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["entryAuthorityRevoked"])
        self.assertTrue(result["cleanupSchedulerOwned"])
        self.assertEqual("CLEANUP", result["state"])
        facade.stop.assert_called_once_with()

    def test_kill_reconciliation_snapshot_never_claims_revocation(self) -> None:
        facade = Mock()
        facade.order_authority_snapshot.side_effect = [
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
            },
            {
                "functionalAuthorityOpen": False,
                "functionalPhase": "RECONCILIATION_REQUIRED",
            },
        ]
        facade.stop.return_value = {"ok": True}
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade),
        ):
            result = (
                state._binance_spot_functional_emergency_cleanup_after_latch()
            )
        self.assertFalse(result["ok"])
        self.assertFalse(result["entryAuthorityRevoked"])
        self.assertFalse(result["cleanupSchedulerOwned"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])

    def test_kill_unreadable_snapshot_never_claims_revocation(self) -> None:
        facade = Mock()
        facade.order_authority_snapshot.side_effect = [
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "ACTIVE",
            },
            RuntimeError("snapshot-unreadable"),
            {
                "functionalAuthorityOpen": True,
                "functionalPhase": "UNREADABLE",
            },
        ]
        facade.stop.return_value = {"ok": True}
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade),
        ):
            result = (
                state._binance_spot_functional_emergency_cleanup_after_latch()
            )
        self.assertFalse(result["ok"])
        self.assertFalse(result["entryAuthorityRevoked"])
        self.assertEqual("RECONCILIATION_REQUIRED", result["state"])

    def test_kill_failed_phase_requires_exact_closed_snapshot(self) -> None:
        for authority_open, expected_revoked in ((True, False), (False, True)):
            with self.subTest(authority_open=authority_open):
                facade = Mock()
                facade.order_authority_snapshot.side_effect = [
                    {
                        "functionalAuthorityOpen": True,
                        "functionalPhase": "ACTIVE",
                        "functionalSessionId": "session-failed-proof",
                    },
                    {
                        "functionalAuthorityOpen": authority_open,
                        "functionalPhase": "FAILED",
                        "functionalSessionId": "session-failed-proof",
                    },
                ]
                facade.stop.return_value = {"ok": False}
                with (
                    patch.object(
                        state, "emergency_stop_active", return_value=True
                    ),
                    patch.object(
                        state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade
                    ),
                ):
                    result = (
                        state._binance_spot_functional_emergency_cleanup_after_latch()
                    )
                self.assertFalse(result["ok"])
                self.assertEqual(
                    expected_revoked, result["entryAuthorityRevoked"]
                )

    def test_kill_early_reconciliation_is_not_closed(self) -> None:
        facade = Mock()
        facade.order_authority_snapshot.return_value = {
            "functionalAuthorityOpen": False,
            "functionalPhase": "RECONCILIATION_REQUIRED",
        }
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade),
        ):
            result = (
                state._binance_spot_functional_emergency_cleanup_after_latch()
            )
        self.assertFalse(result["ok"])
        self.assertFalse(result["entryAuthorityRevoked"])
        facade.stop.assert_not_called()

    def test_kill_early_non_boolean_authority_is_never_closed(self) -> None:
        for hostile in (None, "false", 0, "missing"):
            with self.subTest(hostile=hostile):
                facade = Mock()
                snapshot = {"functionalPhase": "IDLE"}
                if hostile != "missing":
                    snapshot["functionalAuthorityOpen"] = hostile
                facade.order_authority_snapshot.return_value = snapshot
                with (
                    patch.object(
                        state, "emergency_stop_active", return_value=True
                    ),
                    patch.object(
                        state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", facade
                    ),
                ):
                    result = (
                        state._binance_spot_functional_emergency_cleanup_after_latch()
                    )
                self.assertFalse(result["ok"])
                self.assertFalse(result["entryAuthorityRevoked"])
                facade.stop.assert_not_called()

    def test_missing_facade_samples_durable_authority_once(self) -> None:
        durable_open = Mock(return_value=False)
        with (
            patch.object(state, "emergency_stop_active", return_value=True),
            patch.object(state, "_BINANCE_SPOT_FUNCTIONAL_FACADE", None),
            patch.object(
                state,
                "_binance_functional_durable_authority_open",
                durable_open,
            ),
        ):
            result = (
                state._binance_spot_functional_emergency_cleanup_after_latch()
            )
        self.assertTrue(result["ok"])
        self.assertTrue(result["entryAuthorityRevoked"])
        durable_open.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
