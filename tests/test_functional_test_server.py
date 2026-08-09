from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from live_trader.server import LiveTraderHandler


class FunctionalTestServerRoutesTests(unittest.TestCase):
    def test_snapshot_route_uses_readiness_workspace(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test"
        handler.send_json = Mock()
        expected = {"ok": True, "readinessOnly": True}

        with patch(
            "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
            return_value=expected,
        ) as snapshot:
            handler.do_GET()

        snapshot.assert_called_once_with()
        handler.send_json.assert_called_once_with(expected)

    def test_permit_route_forwards_only_configuration_payload(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/permit"
        payload = {
            "targetKey": "strategy:one",
            "durationValue": 10,
            "durationUnit": "DAYS",
        }
        handler.read_json = Mock(return_value=payload)
        handler.send_json = Mock()
        expected = {"ok": True, "brokerSubmissionPerformed": False}

        with (
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value={
                    "current": {
                        "permit": None,
                        "authorityReferencePresent": False,
                    }
                },
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.authority_scope",
                return_value={"present": False, "resolved": False},
            ),
            patch(
                "live_trader.server.state.functional_test_authority_mutation_assessment",
                return_value={"allowed": True, "blockers": []},
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.create_permit",
                return_value=expected,
            ) as create_permit,
        ):
            handler.do_POST()

        create_permit.assert_called_once_with(payload)
        handler.send_json.assert_called_once_with(expected)

    def test_permit_route_rejects_hidden_authority_reference(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/permit"
        handler.read_json = Mock(
            return_value={
                "targetKey": "strategy:one",
                "durationValue": 1,
                "durationUnit": "DAYS",
            }
        )
        handler.send_json = Mock()

        with (
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value={
                    "current": {
                        "permit": None,
                        "authorityReferencePresent": True,
                    }
                },
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.authority_scope",
                return_value={
                    "present": True,
                    "resolved": True,
                    "permitId": "permit-old",
                    "accountId": "account-old",
                },
            ),
            patch(
                "live_trader.server.state.functional_test_authority_mutation_assessment",
                return_value={"allowed": True, "blockers": []},
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.create_permit",
            ) as create_permit,
        ):
            handler.do_POST()

        create_permit.assert_not_called()
        response = handler.send_json.call_args.args[0]
        self.assertFalse(response["ok"])
        self.assertFalse(response["brokerSubmissionPerformed"])
        self.assertIn(
            "functional-test-permit-replacement-requires-stop",
            response["reason"],
        )

    def test_permit_route_rejects_unresolved_authority_scope(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/permit"
        handler.read_json = Mock(
            return_value={
                "targetKey": "strategy:one",
                "durationValue": 6,
                "durationUnit": "HOURS",
            }
        )
        handler.send_json = Mock()
        unresolved = {
            "present": True,
            "resolved": False,
            "reason": "functional-test-authority-scope-unresolved:current-permit-pointer-invalid",
        }

        with (
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value={
                    "current": {
                        "permit": None,
                        "authorityReferencePresent": False,
                    }
                },
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.authority_scope",
                return_value=unresolved,
            ),
            patch(
                "live_trader.server.state.functional_test_authority_mutation_assessment",
                return_value={"allowed": True, "blockers": []},
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.create_permit",
            ) as create_permit,
        ):
            handler.do_POST()

        create_permit.assert_not_called()
        response = handler.send_json.call_args.args[0]
        self.assertFalse(response["ok"])
        self.assertEqual(unresolved, response["workspace"]["authorityScope"])
        self.assertIn("current-permit-pointer-invalid", response["reason"])

    def test_activation_and_stop_routes_cannot_call_broker_submitter(self) -> None:
        expected = {"ok": True, "brokerSubmissionPerformed": False}
        cases = [
            (
                "/api/functional-test/activate",
                {"authorizedBy": "operator-a", "confirmed": True},
                "activate_today",
            ),
            (
                "/api/functional-test/stop",
                {"confirmed": True},
                "stop",
            ),
        ]
        for path, payload, method_name in cases:
            with self.subTest(path=path):
                handler = object.__new__(LiveTraderHandler)
                handler.path = path
                handler.read_json = Mock(return_value=payload)
                handler.send_json = Mock()
                with (
                    patch(
                        f"live_trader.server.FUNCTIONAL_TEST_WORKSPACE.{method_name}",
                        return_value=expected,
                    ) as command,
                    patch(
                        "live_trader.server.state.poll_execution_events",
                        return_value={"ok": True, "errors": []},
                    ),
                    patch(
                        "live_trader.server.state.functional_test_authority_mutation_assessment",
                        return_value={"allowed": True, "blockers": []},
                    ),
                    patch(
                        "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                        return_value={"current": {"permit": None}},
                    ),
                ):
                    handler.do_POST()

                command.assert_called_once_with(payload)
                handler.send_json.assert_called_once_with(expected)

    def test_start_route_requires_explicit_confirmation_and_only_starts_runtime(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/start"
        handler.read_json = Mock(
            return_value={
                "confirmed": True,
                "targetKey": "strategy:one",
                "safety_confirmation": {
                    "challengeId": "challenge-1",
                    "token": "token-1",
                    "typedPhrase": "LIVE 1234",
                },
            }
        )
        handler.send_json = Mock()
        workspace = {"status": "ACTIVE", "current": {"ready": True}}
        expected = {
            "ok": True,
            "runtimeStarted": True,
            "brokerSubmissionPerformed": False,
        }

        with (
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value=workspace,
            ),
            patch(
                "live_trader.server.state.start_functional_test_runtime",
                return_value=expected,
            ) as start,
        ):
            handler.do_POST()

        start.assert_called_once_with(
            workspace,
            confirmed=True,
            target_key="strategy:one",
            safety_confirmation={
                "challengeId": "challenge-1",
                "token": "token-1",
                "typedPhrase": "LIVE 1234",
            },
        )
        handler.send_json.assert_called_once_with(expected)

    def test_stop_closes_and_drains_before_revoking_workspace(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/stop"
        payload = {"confirmed": True}
        handler.read_json = Mock(return_value=payload)
        handler.send_json = Mock()
        result = {"ok": True, "brokerSubmissionPerformed": False}
        call_order: list[str] = []

        def revoke(_payload: dict) -> dict:
            call_order.append("revoke")
            return result

        permit = {
            "permitId": "permit-one",
            "binding": {"accountId": "account-one"},
        }

        def safe_stop(**_scope: str) -> dict:
            call_order.append("authority-close-runtime-drain-reconcile")
            return {"ok": True, "runtime": {"ok": True}}

        with (
            patch(
                "live_trader.server.functional_test_control_scope",
                return_value={
                    "present": True,
                    "resolved": True,
                    "permitId": "permit-one",
                    "accountFingerprint": "fingerprint-one",
                },
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.stop",
                side_effect=revoke,
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value={"current": {"permit": permit}},
            ),
            patch(
                "live_trader.server.state.stop_functional_test_runtime_safely",
                side_effect=safe_stop,
            ),
        ):
            handler.do_POST()

        self.assertEqual(
            ["authority-close-runtime-drain-reconcile", "revoke"],
            call_order,
        )
        self.assertTrue(result["runtimeStopped"])

    def test_failed_safe_stop_keeps_workspace_pointers(self) -> None:
        handler = object.__new__(LiveTraderHandler)
        handler.path = "/api/functional-test/stop"
        handler.read_json = Mock(return_value={"confirmed": True})
        handler.send_json = Mock()
        permit = {
            "permitId": "permit-one",
            "binding": {"accountId": "account-one"},
        }
        failure = {
            "ok": False,
            "status": "STOP_FAILED",
            "reason": "functional-test-runtime-drain-failed",
        }
        marked = {"ok": False, "workspace": {"status": "STOP_FAILED"}}
        with (
            patch(
                "live_trader.server.functional_test_control_scope",
                return_value={
                    "present": True,
                    "resolved": True,
                    "permitId": "permit-one",
                    "accountFingerprint": "fingerprint-one",
                },
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.snapshot",
                return_value={"current": {"permit": permit}},
            ),
            patch(
                "live_trader.server.state.stop_functional_test_runtime_safely",
                return_value=failure,
            ),
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.record_stop_failed",
                return_value=marked,
            ) as record_failed,
            patch(
                "live_trader.server.FUNCTIONAL_TEST_WORKSPACE.stop",
            ) as revoke,
        ):
            handler.do_POST()

        record_failed.assert_called_once()
        revoke.assert_not_called()
        response = handler.send_json.call_args.args[0]
        self.assertEqual("STOP_FAILED", response["workspace"]["status"])


if __name__ == "__main__":
    unittest.main()
