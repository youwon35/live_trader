from __future__ import annotations

import copy
import hashlib
import threading
import time
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.safety_confirmation import SafetyConfirmationStore
from live_trader.server import LiveTraderHandler


def confirmation_payload(challenge: dict[str, object]) -> dict[str, str]:
    return {
        "challengeId": str(challenge["challengeId"]),
        "token": str(challenge["token"]),
        "typedPhrase": str(challenge["expectedPhrase"]),
    }


class SafetyConfirmationStoreTest(unittest.TestCase):
    @staticmethod
    def issue(
        store: SafetyConfirmationStore,
        *,
        action: str = "DRY_RUN_OFF",
        context: dict[str, object] | None = None,
        phrase: str = "LIVE A1B2",
    ) -> dict[str, object]:
        result = store.issue(
            action=action,
            context=context or {"revision": 7, "account": "fingerprint-a"},
            expected_phrase=phrase,
            display_context={"accountHint": "••••A1B2"},
        )
        if result.get("ok") is not True:
            raise AssertionError(result)
        return result

    def test_challenge_expires_after_bounded_ttl(self) -> None:
        now = [1000.0]
        store = SafetyConfirmationStore(ttl_seconds=60, clock=lambda: now[0])
        issued = self.issue(store)
        now[0] += 60.001

        result = store.consume(
            action="DRY_RUN_OFF",
            context={"revision": 7, "account": "fingerprint-a"},
            confirmation=confirmation_payload(issued),
        )

        self.assertFalse(result["ok"])
        self.assertEqual("safety-confirmation-expired", result["reason"])
        self.assertEqual(0, store.pending_count_for_tests())

    def test_challenge_is_one_time_and_cannot_be_reused(self) -> None:
        store = SafetyConfirmationStore()
        issued = self.issue(store)
        proof = confirmation_payload(issued)
        context = {"revision": 7, "account": "fingerprint-a"}

        first = store.consume(
            action="DRY_RUN_OFF", context=context, confirmation=proof
        )
        second = store.consume(
            action="DRY_RUN_OFF", context=context, confirmation=proof
        )

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual("safety-confirmation-missing-or-used", second["reason"])

    def test_concurrent_duplicate_submission_has_exactly_one_winner(self) -> None:
        store = SafetyConfirmationStore()
        issued = self.issue(store)
        context = {"revision": 7, "account": "fingerprint-a"}
        proof = confirmation_payload(issued)
        worker_count = 12
        barrier = threading.Barrier(worker_count)
        results: list[dict[str, object]] = []
        results_lock = threading.Lock()

        def submit() -> None:
            barrier.wait(timeout=2)
            result = store.consume(
                action="DRY_RUN_OFF",
                context=context,
                confirmation=proof,
            )
            with results_lock:
                results.append(result)

        threads = [threading.Thread(target=submit) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(worker_count, len(results))
        self.assertEqual(1, sum(result.get("ok") is True for result in results))
        self.assertTrue(
            all(
                result.get("reason") == "safety-confirmation-missing-or-used"
                for result in results
                if result.get("ok") is not True
            )
        )

    def test_wrong_phrase_consumes_challenge(self) -> None:
        store = SafetyConfirmationStore()
        issued = self.issue(store)
        context = {"revision": 7, "account": "fingerprint-a"}
        wrong = confirmation_payload(issued)
        wrong["typedPhrase"] = "LIVE WRONG"

        rejected = store.consume(
            action="DRY_RUN_OFF", context=context, confirmation=wrong
        )
        retry = store.consume(
            action="DRY_RUN_OFF",
            context=context,
            confirmation=confirmation_payload(issued),
        )

        self.assertEqual("safety-confirmation-phrase-invalid", rejected["reason"])
        self.assertEqual("safety-confirmation-missing-or-used", retry["reason"])

    def test_action_and_context_changes_fail_closed_and_consume(self) -> None:
        store = SafetyConfirmationStore()
        context = {"revision": 7, "account": "fingerprint-a"}
        action_challenge = self.issue(store, context=context)

        action_changed = store.consume(
            action="NEW_ENTRIES_BLOCKED_OFF",
            context=context,
            confirmation=confirmation_payload(action_challenge),
        )
        action_retry = store.consume(
            action="DRY_RUN_OFF",
            context=context,
            confirmation=confirmation_payload(action_challenge),
        )
        context_challenge = self.issue(store, context=context)
        context_changed = store.consume(
            action="DRY_RUN_OFF",
            context={**context, "revision": 8},
            confirmation=confirmation_payload(context_challenge),
        )

        self.assertEqual(
            "safety-confirmation-action-changed", action_changed["reason"]
        )
        self.assertEqual(
            "safety-confirmation-missing-or-used", action_retry["reason"]
        )
        self.assertEqual(
            "safety-confirmation-context-changed", context_changed["reason"]
        )

    def test_process_restart_store_cannot_consume_old_challenge(self) -> None:
        old_process_store = SafetyConfirmationStore()
        issued = self.issue(old_process_store)
        restarted_process_store = SafetyConfirmationStore()

        result = restarted_process_store.consume(
            action="DRY_RUN_OFF",
            context={"revision": 7, "account": "fingerprint-a"},
            confirmation=confirmation_payload(issued),
        )

        self.assertEqual("safety-confirmation-missing-or-used", result["reason"])

    def test_store_repr_contains_neither_raw_token_nor_phrase(self) -> None:
        store = SafetyConfirmationStore()
        phrase = "LIVE PHRASE-MUST-NEVER-BE-STORED"
        issued = self.issue(store, phrase=phrase)
        raw_token = str(issued["token"])
        stored_representation = repr(store.__dict__)

        self.assertNotIn(raw_token, stored_representation)
        self.assertNotIn(phrase, stored_representation)
        self.assertIn(hashlib.sha256(raw_token.encode()).hexdigest(), stored_representation)


class SafetyConfirmationStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_state = copy.deepcopy(state.STATE)
        self.original_store = state.SAFETY_CONFIRMATIONS
        self.original_soak_session = state.BINANCE_FUTURES_FILL_SOAK_SESSION
        self.original_soak_thread = state.BINANCE_FUTURES_FILL_SOAK_THREAD
        self.original_soak_internal = copy.deepcopy(
            state.BINANCE_FUTURES_FILL_SOAK_INTERNAL
        )
        self.original_real_orders_armed = state._REAL_ORDERS_PROCESS_ARMED
        state._REAL_ORDERS_PROCESS_ARMED = False
        state.SAFETY_CONFIRMATIONS = SafetyConfirmationStore()

    def tearDown(self) -> None:
        thread = state.BINANCE_FUTURES_FILL_SOAK_THREAD
        if thread is not None and thread is not self.original_soak_thread:
            thread.join(timeout=2)
        state.STATE.clear()
        state.STATE.update(self.original_state)
        state.SAFETY_CONFIRMATIONS = self.original_store
        state.BINANCE_FUTURES_FILL_SOAK_SESSION = self.original_soak_session
        state.BINANCE_FUTURES_FILL_SOAK_THREAD = self.original_soak_thread
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.clear()
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.update(
            self.original_soak_internal
        )
        state._REAL_ORDERS_PROCESS_ARMED = self.original_real_orders_armed

    @staticmethod
    def deterministic_identity() -> dict[str, str]:
        return {
            "provider": "KIS",
            "fingerprint": "account-fingerprint-fixed",
            "suffix": "4321",
        }

    def test_confirmed_true_alone_cannot_release_flag_but_valid_challenge_can(
        self,
    ) -> None:
        state.STATE["dry_run"] = True
        state.STATE["config_revision"] = 17
        with (
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
        ):
            old_contract = state.set_flag("dry_run", False, confirmed=True)
            self.assertFalse(old_contract["ok"])
            self.assertTrue(state.STATE["dry_run"])

            issued = state.issue_safety_confirmation(
                "DRY_RUN_OFF", {"name": "dry_run", "value": False}
            )
            accepted = state.set_flag(
                "dry_run",
                False,
                confirmed=True,
                safety_confirmation=confirmation_payload(issued),
            )

        self.assertTrue(accepted["ok"])
        self.assertFalse(state.STATE["dry_run"])
        self.assertEqual(18, state.STATE["config_revision"])

    def test_kill_latch_reengage_revision_invalidates_release_challenge(
        self,
    ) -> None:
        state.STATE["kill_switch"] = True
        latch = {
            "active": True,
            "updatedAt": "2026-08-09T12:00:00Z",
            "revision": "kill-revision-1",
        }
        with (
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(state, "emergency_stop_status", return_value=latch) as status,
        ):
            issued = state.issue_safety_confirmation(
                "KILL_SWITCH_OFF",
                {"name": "kill_switch", "value": False},
            )
            status.return_value = {
                **latch,
                "updatedAt": "2026-08-09T12:00:01Z",
                "revision": "kill-revision-2",
            }
            result = state.consume_safety_confirmation(
                "KILL_SWITCH_OFF",
                confirmation_payload(issued),
                {"name": "kill_switch", "value": False},
            )

        self.assertEqual("safety-confirmation-context-changed", result["reason"])

    def test_real_order_proposed_values_digest_change_blocks_before_env_write(
        self,
    ) -> None:
        original_values = {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}
        changed_values = {"LIVE_TRADER_ENABLE_REAL_ORDERS": "1"}
        issue_context = {
            "settingKeys": sorted(original_values),
            "enableRealOrders": True,
            "valuesDigest": state.safety_confirmation_values_digest(
                original_values
            ),
        }
        with (
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(
                state, "_safety_environment_fingerprint", return_value="env-a"
            ),
            patch.object(state, "real_orders_enabled", return_value=False),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
            patch(
                "live_trader.env_settings.env_settings_snapshot",
                return_value={"fields": []},
            ),
            patch("live_trader.env_settings.save_env_settings") as save_env,
        ):
            issued = state.issue_safety_confirmation(
                "REAL_ORDERS_ENABLE", issue_context
            )
            result = state.save_environment_settings(
                changed_values,
                confirmed=True,
                safety_confirmation=confirmation_payload(issued),
            )

        self.assertEqual("safety-confirmation-context-changed", result["reason"])
        save_env.assert_not_called()

    def test_persisted_true_is_disarmed_after_restart_until_new_challenge(self) -> None:
        proposed = {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}
        request_context = {
            "settingKeys": sorted(proposed),
            "enableRealOrders": True,
            "valuesDigest": state.safety_confirmation_values_digest(proposed),
        }
        settings = {
            "fields": [
                {
                    "key": "LIVE_TRADER_ENABLE_REAL_ORDERS",
                    "kind": "bool",
                    "value": "false",
                    "configured": True,
                }
            ]
        }
        with (
            patch.dict(
                state.os.environ,
                {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"},
                clear=False,
            ),
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(
                state, "_safety_environment_fingerprint", return_value="env-a"
            ),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
            patch(
                "live_trader.env_settings.env_settings_snapshot",
                return_value=settings,
            ),
            patch(
                "live_trader.env_settings.save_env_settings",
                return_value=settings,
            ),
        ):
            self.assertFalse(state.real_orders_enabled())
            issued = state.issue_safety_confirmation(
                "REAL_ORDERS_ENABLE", request_context
            )
            result = state.save_environment_settings(
                proposed,
                confirmed=True,
                safety_confirmation=confirmation_payload(issued),
            )
            self.assertTrue(result["ok"])
            self.assertTrue(state.real_orders_enabled())

    def test_env_rotation_cannot_interleave_after_challenge_consume(self) -> None:
        proposed = {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}
        request_context = {
            "settingKeys": sorted(proposed),
            "enableRealOrders": True,
            "valuesDigest": state.safety_confirmation_values_digest(proposed),
        }
        first_save_entered = threading.Event()
        release_first_save = threading.Event()
        saved_payloads: list[dict[str, object]] = []

        def settings_snapshot() -> dict[str, object]:
            return {
                "fields": [
                    {
                        "key": "BINANCE_API_KEY",
                        "kind": "secret",
                        "value": "",
                        "configured": True,
                    },
                    {
                        "key": "LIVE_TRADER_ENABLE_REAL_ORDERS",
                        "kind": "bool",
                        "value": state.os.environ.get(
                            "LIVE_TRADER_ENABLE_REAL_ORDERS", "false"
                        ),
                        "configured": True,
                    },
                ]
            }

        def save(values: dict[str, object]) -> dict[str, object]:
            saved_payloads.append(dict(values))
            if len(saved_payloads) == 1:
                first_save_entered.set()
                self.assertTrue(release_first_save.wait(timeout=5))
            if "LIVE_TRADER_ENABLE_REAL_ORDERS" in values:
                state.os.environ["LIVE_TRADER_ENABLE_REAL_ORDERS"] = (
                    "true"
                    if values["LIVE_TRADER_ENABLE_REAL_ORDERS"] in {True, "true"}
                    else "false"
                )
            return settings_snapshot()

        with (
            patch.dict(
                state.os.environ,
                {"LIVE_TRADER_ENABLE_REAL_ORDERS": "false"},
                clear=False,
            ),
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(
                state, "_safety_environment_fingerprint", return_value="env-a"
            ),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
            patch(
                "live_trader.env_settings.env_settings_snapshot",
                side_effect=settings_snapshot,
            ),
            patch(
                "live_trader.env_settings.save_env_settings",
                side_effect=save,
            ),
        ):
            issued = state.issue_safety_confirmation(
                "REAL_ORDERS_ENABLE", request_context
            )
            results: list[dict[str, object]] = []
            enable_thread = threading.Thread(
                target=lambda: results.append(
                    state.save_environment_settings(
                        proposed,
                        confirmed=True,
                        safety_confirmation=confirmation_payload(issued),
                    )
                )
            )
            enable_thread.start()
            self.assertTrue(first_save_entered.wait(timeout=5))
            rotate_thread = threading.Thread(
                target=lambda: results.append(
                    state.save_environment_settings(
                        {"BINANCE_API_KEY": "rotated-key"}
                    )
                )
            )
            rotate_thread.start()
            self.assertEqual(1, len(saved_payloads))
            release_first_save.set()
            enable_thread.join(timeout=5)
            rotate_thread.join(timeout=5)

        self.assertFalse(enable_thread.is_alive())
        self.assertFalse(rotate_thread.is_alive())
        self.assertEqual(2, len(saved_payloads))
        self.assertIs(
            False,
            saved_payloads[1]["LIVE_TRADER_ENABLE_REAL_ORDERS"],
        )
        self.assertFalse(state._REAL_ORDERS_PROCESS_ARMED)

    def test_credential_rotation_invalidates_real_order_enable_challenge(
        self,
    ) -> None:
        proposed = {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}
        request_context = {
            "settingKeys": sorted(proposed),
            "enableRealOrders": True,
            "valuesDigest": state.safety_confirmation_values_digest(proposed),
        }
        with (
            patch.object(
                state,
                "_safety_confirmation_identity",
                return_value=self.deterministic_identity(),
            ),
            patch.object(
                state,
                "_safety_environment_fingerprint",
                side_effect=["credentials-before", "credentials-after"],
            ),
            patch.object(state, "real_orders_enabled", return_value=False),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "append_audit"),
            patch(
                "live_trader.env_settings.env_settings_snapshot",
                return_value={"fields": []},
            ),
            patch("live_trader.env_settings.save_env_settings") as save_env,
        ):
            issued = state.issue_safety_confirmation(
                "REAL_ORDERS_ENABLE", request_context
            )
            result = state.save_environment_settings(
                proposed,
                confirmed=True,
                safety_confirmation=confirmation_payload(issued),
            )

        self.assertEqual("safety-confirmation-context-changed", result["reason"])
        save_env.assert_not_called()

    def test_selected_provider_account_change_invalidates_challenge(self) -> None:
        state.STATE["selected_strategy_id"] = "strategy-kis"
        state.STATE["selected_deployment_id"] = ""
        state.STATE["dry_run"] = True
        environment = {
            "KIS_ACCOUNT_NO": "12345678-01",
            "KIS_ACCOUNT_PRODUCT_CODE": "01",
        }
        with (
            patch.object(
                state,
                "strategy_rows",
                return_value=[
                    {"strategy_id": "strategy-kis", "brokerId": "kis"}
                ],
            ),
            patch.object(
                state, "env_value", side_effect=lambda key: environment.get(key, "")
            ),
        ):
            issued = state.issue_safety_confirmation("DRY_RUN_OFF")
            environment["KIS_ACCOUNT_NO"] = "87654321-01"
            result = state.consume_safety_confirmation(
                "DRY_RUN_OFF", confirmation_payload(issued)
            )

        self.assertEqual("safety-confirmation-context-changed", result["reason"])

    def test_global_provider_account_change_invalidates_challenge(self) -> None:
        state.STATE["selected_strategy_id"] = ""
        state.STATE["selected_deployment_id"] = ""
        state.STATE["dry_run"] = True
        environment = {
            "KIS_ACCOUNT_NO": "12345678-01",
            "KIS_ACCOUNT_PRODUCT_CODE": "01",
            "BINANCE_API_KEY": "binance-key-before",
            "UPBIT_ACCESS_KEY": "upbit-key",
        }
        with (
            patch.object(
                state, "env_value", side_effect=lambda key: environment.get(key, "")
            ),
            patch.object(
                state, "_safety_environment_fingerprint", return_value="env-fixed"
            ),
        ):
            issued = state.issue_safety_confirmation("DRY_RUN_OFF")
            environment["BINANCE_API_KEY"] = "binance-key-after"
            result = state.consume_safety_confirmation(
                "DRY_RUN_OFF", confirmation_payload(issued)
            )

        self.assertEqual("safety-confirmation-context-changed", result["reason"])

    def test_functional_start_old_confirmed_contract_is_blocked(self) -> None:
        state.STATE["operator_confirmed"] = True
        workspace = {
            "environment": state.FUNCTIONAL_TEST_ENVIRONMENT,
            "status": "ACTIVE",
            "current": {
                "ready": True,
                "selectedTargetKey": "target-1",
                "blockers": [],
            },
            "candidates": [
                {
                    "key": "target-1",
                    "available": True,
                    "runtimeStrategyId": "strategy-1",
                    "portfolioId": "portfolio-1",
                }
            ],
        }
        safe_context = (
            {"action": "FUNCTIONAL_TEST_START", "target": "target-1"},
            {},
            "LIVE 4321",
        )
        with (
            patch.object(
                state.LIVE_CONTINUOUS_CONTROLLER,
                "snapshot",
                return_value={"profiles": {"stock": {"phase": "STOPPED"}}},
            ),
            patch.object(
                state,
                "safety_confirmation_authoritative_context",
                return_value=safe_context,
            ),
            patch.object(state, "snapshot", return_value={}),
            patch.object(state, "start_continuous_runtime") as start_runtime,
        ):
            result = state.start_functional_test_runtime(
                workspace,
                confirmed=True,
                target_key="target-1",
            )

        self.assertFalse(result["ok"])
        self.assertEqual("safety-confirmation-required", result["reason"])
        start_runtime.assert_not_called()

    def test_functional_start_challenge_requires_exact_nonempty_target(self) -> None:
        safe_context = (
            {
                "action": "FUNCTIONAL_TEST_START",
                "request": {"targetKey": ""},
            },
            {},
            "LIVE 4321",
        )
        with patch.object(
            state,
            "safety_confirmation_authoritative_context",
            return_value=safe_context,
        ):
            result = state.issue_safety_confirmation(
                "FUNCTIONAL_TEST_START", {"targetKey": ""}
            )

        self.assertFalse(result["ok"])
        self.assertEqual(
            "safety-confirmation-functional-target-required",
            result["reason"],
        )

    def test_direct_generic_runtime_function_cannot_forge_functional_purpose(
        self,
    ) -> None:
        with patch.object(state, "snapshot", return_value={}):
            result = state.start_continuous_runtime(
                "stock",
                "SMALL_LIVE",
                execution_purpose="FUNCTIONAL_TEST",
                functional_test_context={"callerControlled": True},
            )

        self.assertFalse(result["ok"])
        self.assertFalse(result["runtimeStarted"])
        self.assertFalse(result["brokerSubmissionPerformed"])

    def test_fill_soak_old_preview_token_and_confirmed_contract_is_blocked(
        self,
    ) -> None:
        class ReadySession:
            @staticmethod
            def status() -> dict[str, str]:
                return {"phase": "CREATED"}

        preview_token = "legacy-preview-token"
        state.BINANCE_FUTURES_FILL_SOAK_SESSION = ReadySession()
        state.BINANCE_FUTURES_FILL_SOAK_THREAD = None
        state.BINANCE_FUTURES_FILL_SOAK_INTERNAL.update(
            {
                "phase": "ready",
                "status": "READY",
                "symbol": "BTCUSDT",
                "confirmation_token_hash": hashlib.sha256(
                    preview_token.encode()
                ).hexdigest(),
                "confirmation_issued_epoch": time.time(),
                "confirmation_expires_epoch": time.time() + 60,
                "confirmation_used": False,
            }
        )
        safe_context = (
            {"action": "BINANCE_FUTURES_FILL_SOAK_START", "symbol": "BTCUSDT"},
            {},
            "LIVE A1B2",
        )
        with patch.object(
            state,
            "safety_confirmation_authoritative_context",
            return_value=safe_context,
        ):
            result = state.start_binance_futures_fill_soak(
                preview_token,
                confirmed=True,
            )

        self.assertFalse(result["ok"])
        self.assertEqual("safety-confirmation-required", result["reason"])
        self.assertFalse(state.BINANCE_FUTURES_FILL_SOAK_INTERNAL["confirmation_used"])


class SafetyConfirmationServerContractTest(unittest.TestCase):
    def test_challenge_route_forwards_action_and_context(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/safety-confirmation/challenge"
        handler.read_json = Mock(
            return_value={
                "action": "FUNCTIONAL_TEST_START",
                "context": {"targetKey": "target-1"},
            }
        )
        handler.send_json = Mock()
        expected = {"ok": True, "challengeId": "challenge-1"}

        with patch(
            "live_trader.server.state.issue_safety_confirmation",
            return_value=expected,
        ) as issue:
            handler.do_POST()

        issue.assert_called_once_with(
            "FUNCTIONAL_TEST_START", {"targetKey": "target-1"}
        )
        handler.send_json.assert_called_once_with(expected)


if __name__ == "__main__":
    unittest.main()
