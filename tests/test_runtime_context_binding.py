from __future__ import annotations

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from live_trader import state
from live_trader.contracts import normalize_portfolio_artifact
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent
from trading_runtime.artifact_governance import (
    artifact_reference,
    seal_portfolio_artifact,
    seal_strategy_artifact,
)


def _strategy(
    strategy_id: str,
    *,
    deployment_id: str,
    symbol: str = "BTCUSDT",
) -> dict:
    return {
        "id": strategy_id,
        "strategy_id": strategy_id,
        "deployment_id": deployment_id,
        "symbol": symbol,
        "timeframe": "1h",
        "broker_id": "binance",
        "provider": "binance",
        "plugin": "ma-cross",
        "parameters": {},
        "lifecycle_status": "before-live-small",
        "backtester_verified": True,
        "live_small_eligible": True,
        "live_eligible": False,
        "permissions": {"live_small_eligible": True},
    }


class RuntimeContextBindingTest(unittest.TestCase):
    @staticmethod
    def _sealed_portfolio(*, policy_limit: float) -> dict:
        return normalize_portfolio_artifact(
            seal_portfolio_artifact(
                {
                    "artifactType": "portfolio",
                    "schemaVersion": "portfolio-artifact-v1",
                    "id": "portfolio-exact",
                    "lifecycle": {"status": "before-live-small"},
                    "permissions": {
                        "live_small_allowed": True,
                        "live_allowed": False,
                    },
                    "strategyInstances": [
                        {
                            "strategyId": "exact-strategy",
                            "instanceId": "exact-instance",
                            "symbol": "BTCUSDT",
                            "allocation": {"normalizedWeight": 0.1},
                        }
                    ],
                    "framework": {
                        "targetPortfolio": [
                            {
                                "strategyId": "exact-strategy",
                                "symbol": "BTCUSDT",
                                "targetWeight": 0.1,
                            }
                        ]
                    },
                    "riskPolicy": {
                        "maxSingleSymbolWeight": policy_limit,
                    },
                }
            )
        )

    @staticmethod
    def _normalized_strategy_member(
        strategy_id: str,
        *,
        label: str,
        portfolio_reference: dict[str, str],
    ) -> dict:
        sealed = seal_strategy_artifact(
            {
                "id": strategy_id,
                "artifactType": "strategy",
                "schemaVersion": "strategy-artifact-v1",
                "name": label,
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "parameters": {"label": label},
            }
        )
        reference = artifact_reference(sealed)
        return {
            "strategy_id": strategy_id,
            "symbol": "BTCUSDT",
            "broker_id": "binance",
            "artifact_reference": reference,
            "artifact_integrity": {
                "valid": True,
                "declaredHash": reference["artifactHash"],
                "computedHash": reference["artifactHash"],
            },
            "paper_live_qualification": {
                "required": True,
                "ready": True,
                "issues": [],
                "evidenceId": f"paper-{label}",
                "evidenceHash": "1" * 64,
                "evidenceBundleHash": "2" * 64,
                "bindingHash": "3" * 64,
                "paperGovernanceDeploymentId": f"paper-deployment-{label}",
                "strategyArtifactId": strategy_id,
                "strategyArtifactHash": reference["artifactHash"],
                "strategyInstanceId": "instance-exact",
                "portfolioRequired": True,
                "portfolioArtifactId": portfolio_reference["artifactId"],
                "portfolioArtifactHash": portfolio_reference["artifactHash"],
                "portfolioInstanceId": "portfolio-instance-exact",
            },
        }

    def test_deployment_selects_exact_portfolio_hash_not_newest_same_id(
        self,
    ) -> None:
        pinned = self._sealed_portfolio(policy_limit=0.2)
        newer = self._sealed_portfolio(policy_limit=0.3)
        pinned_reference = state.verified_portfolio_artifact_reference(
            pinned
        )
        newer_reference = state.verified_portfolio_artifact_reference(newer)
        self.assertNotEqual(
            pinned_reference["artifactHash"],
            newer_reference["artifactHash"],
        )
        strategy = _strategy(
            "exact-strategy",
            deployment_id="dep-exact",
        )
        strategy["deployment_portfolio_reference"] = pinned_reference

        selected = state.portfolio_gate_for_strategy(
            strategy,
            [newer, pinned],
            mode="SMALL_LIVE",
        )
        missing = state.portfolio_gate_for_strategy(
            strategy,
            [newer],
            mode="SMALL_LIVE",
        )
        empty_store = state.portfolio_gate_for_strategy(
            strategy,
            [],
            mode="SMALL_LIVE",
        )

        self.assertTrue(selected["active"])
        self.assertEqual(
            pinned_reference["artifactHash"],
            selected["portfolioArtifactHash"],
        )
        self.assertFalse(missing["allowed"])
        self.assertIn("exact Portfolio", missing["detail"])
        self.assertFalse(empty_store["allowed"])
        self.assertTrue(empty_store["active"])

        standalone_deployment = {
            **strategy,
            "deployment_source": "deployment-registry",
            "deployment_portfolio_reference": {},
        }
        not_auto_adopted = state.portfolio_gate_for_strategy(
            standalone_deployment,
            [pinned],
            mode="SMALL_LIVE",
        )
        self.assertFalse(not_auto_adopted["active"])
        self.assertIn("standalone", not_auto_adopted["detail"])

    def test_portfolio_member_selects_exact_strategy_hash_not_same_id(self) -> None:
        placeholder_portfolio_reference = {
            "artifactId": "portfolio-member-exact",
            "artifactHash": "4" * 64,
            "contentHash": "5" * 64,
        }
        exact = self._normalized_strategy_member(
            "member-strategy",
            label="exact",
            portfolio_reference=placeholder_portfolio_reference,
        )
        wrong = self._normalized_strategy_member(
            "member-strategy",
            label="newer-wrong",
            portfolio_reference=placeholder_portfolio_reference,
        )
        exact_reference = exact["artifact_reference"]
        self.assertNotEqual(
            exact_reference["artifactHash"],
            wrong["artifact_reference"]["artifactHash"],
        )
        portfolio = normalize_portfolio_artifact(
            seal_portfolio_artifact(
                {
                    "artifactType": "portfolio",
                    "schemaVersion": "portfolio-artifact-v1",
                    "id": "portfolio-member-exact",
                    "lifecycle": {"status": "before-live-small"},
                    "permissions": {"live_small_allowed": True},
                    "strategyInstances": [
                        {
                            "strategyId": "member-strategy",
                            "sourceStrategyId": "member-strategy",
                            "sourceArtifactHash": exact_reference[
                                "artifactHash"
                            ],
                            "instanceId": "instance-exact",
                            "symbol": "BTCUSDT",
                            "allocation": {"normalizedWeight": 0.1},
                        }
                    ],
                    "framework": {
                        "targetPortfolio": [
                            {
                                "strategyId": "member-strategy",
                                "symbol": "BTCUSDT",
                                "targetWeight": 0.1,
                            }
                        ]
                    },
                }
            )
        )
        portfolio_reference = state.verified_portfolio_artifact_reference(
            portfolio
        )
        for candidate in (exact, wrong):
            qualification = candidate["paper_live_qualification"]
            qualification["portfolioArtifactId"] = portfolio_reference[
                "artifactId"
            ]
            qualification["portfolioArtifactHash"] = portfolio_reference[
                "artifactHash"
            ]
        selected = {
            **exact,
            "deployment_id": "deployment-member-exact",
            "strategy_instance_id": "instance-exact",
            "portfolio_gate": {
                "active": True,
                "allowed": True,
                "portfolioId": portfolio_reference["artifactId"],
                "portfolioArtifactHash": portfolio_reference["artifactHash"],
            },
        }
        deployment_binding = {
            "source": "deployment-store",
            "deploymentId": "deployment-member-exact",
            "revision": 1,
            "lifecycle": "before-live-small",
            "environment": "LIVE",
            "mode": "SMALL_LIVE",
            "executionPermissionDigest": "6" * 64,
            "strategyArtifact": exact_reference,
            "portfolioArtifact": portfolio_reference,
        }
        deployment_binding["bindingHash"] = state.governance_sha256(
            deployment_binding
        )

        with (
            patch.object(
                state,
                "_current_live_deployment_binding",
                return_value=deployment_binding,
            ),
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(
                state,
                "load_strategy_artifacts",
                return_value=[wrong, exact],
            ),
        ):
            _portfolio_id, _portfolio_ref, members = (
                state._operational_portfolio_members(selected)
            )
        self.assertEqual(
            exact_reference["artifactHash"],
            members[0]["strategyArtifactReference"]["artifactHash"],
        )

        with (
            patch.object(
                state,
                "_current_live_deployment_binding",
                return_value=deployment_binding,
            ),
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(
                state,
                "load_strategy_artifacts",
                return_value=[wrong],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "exact Strategy Artifact"):
                state._operational_portfolio_members(selected)

    def test_runtime_session_blocks_binding_change_during_preflight(self) -> None:
        strategy = _strategy("race", deployment_id="dep-race")
        strategy["strategy_instance_id"] = "standalone:race"
        binding = {
            "source": "deployment-store",
            "deploymentId": "dep-race",
            "revision": 2,
            "lifecycle": "before-live-small",
            "environment": "LIVE",
            "mode": "SMALL_LIVE",
            "executionPermissionDigest": "1" * 64,
            "strategyArtifact": {
                "artifactId": "race",
                "artifactHash": "2" * 64,
                "contentHash": "3" * 64,
            },
            "portfolioArtifact": {
                "artifactId": "",
                "artifactHash": "",
                "contentHash": "",
            },
        }
        binding["bindingHash"] = state.governance_sha256(binding)
        manifest = SimpleNamespace(
            deployment_id="dep-race",
            manifest_hash="4" * 64,
            portfolio_artifact_hash="",
            metadata={
                "deploymentBinding": binding,
                "deploymentBindingHash": binding["bindingHash"],
                "paperFinalBindings": [{"ready": True}],
                "portfolioArtifact": {},
            },
        )
        preflight = SimpleNamespace(snapshot_id="preflight-race")

        with patch.object(
            state,
            "portfolio_rows",
            return_value=[],
        ), patch.object(
            state,
            "strategy_rows",
            return_value=[strategy],
        ), patch.object(
            state,
            "paper_live_qualification_gate_for_strategy",
            return_value={"required": True, "ready": True},
        ), patch.object(
            state,
            "ensure_operational_deployment_manifest",
            return_value=manifest,
        ), patch.object(
            state.OPERATIONAL_GOVERNANCE,
            "latest_preflight_for_deployment",
            return_value=preflight,
        ), patch.object(
            state.OPERATIONAL_GOVERNANCE,
            "preflight_validity",
            return_value={"valid": True, "reasons": []},
        ), patch.object(
            state,
            "real_orders_enabled",
            return_value=True,
        ), patch.object(
            state,
            "_operational_manifest_inputs",
            return_value={"metadata": {"deploymentBinding": {"revision": 3}}},
        ), patch.object(
            state,
            "_operational_manifest_matches_inputs",
            return_value=False,
        ), patch.object(
            state.OPERATIONAL_GOVERNANCE,
            "create_runtime_session",
        ) as create_session:
            with self.assertRaisesRegex(ValueError, "Preflight 중"):
                state._prepare_operational_runtime_session(
                    "crypto",
                    "SMALL_LIVE",
                    "",
                    "dep-race",
                    "race",
                )

        create_session.assert_not_called()

    def test_runtime_start_rejects_missing_selected_deployment_without_fallback(self) -> None:
        available = _strategy("available", deployment_id="dep-available")
        original_state = copy.deepcopy(state.STATE)
        try:
            with (
                patch.object(state, "portfolio_rows", return_value=[]),
                patch.object(state, "strategy_rows", return_value=[available]),
                patch.object(state, "snapshot", return_value={}),
                patch.object(state, "append_audit"),
                patch.object(state.LIVE_CONTINUOUS_CONTROLLER, "start") as start,
            ):
                result = state.start_continuous_runtime(
                    "crypto",
                    "MONITOR",
                    "",
                    "dep-missing",
                    "missing",
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        self.assertFalse(result["ok"])
        self.assertIn("일치하지 않습니다", result["reason"])
        start.assert_not_called()

    def test_explicit_standalone_selector_never_falls_back_to_first_eligible(self) -> None:
        controller = LiveContinuousController(Path("."))
        first = _strategy("first", deployment_id="dep-first")
        selected = _strategy("selected", deployment_id="dep-selected")

        with patch.object(state, "strategy_rows", return_value=[first, selected]):
            exact = controller._select_standalone_strategy(
                "crypto",
                "SMALL_LIVE",
                "selected",
                "dep-selected",
            )
            missing = controller._select_standalone_strategy(
                "crypto",
                "SMALL_LIVE",
                "missing",
                "dep-missing",
            )

        self.assertEqual("selected", exact["strategy_id"])
        self.assertIsNone(missing)

    def test_running_runtime_rejects_another_deployment_before_mode_transition(self) -> None:
        controller = LiveContinuousController(Path("."))
        spec = controller._standalone_spec(
            _strategy("selected", deployment_id="dep-selected")
        )
        supervisor = MagicMock()
        supervisor.running = True
        supervisor.snapshot.return_value = {"phase": "RUNNING", "running": True}
        supervisor.engine.specs = (spec,)
        controller.supervisor = supervisor
        controller.profile_id = "crypto"
        controller.mode = "MONITOR"
        controller.deployment_id = "dep-selected"
        controller.requested_strategy_id = "selected"
        controller.strategy_ids = ("selected",)
        controller.allowed_symbols = ("BTCUSDT",)

        with patch.object(state, "snapshot", return_value={}):
            result = controller.start(
                "crypto",
                "SMALL_LIVE",
                "",
                "other",
                "dep-other",
            )

        self.assertFalse(result["ok"])
        self.assertIn("컨텍스트", result["reason"])
        supervisor.engine.transition_mode.assert_not_called()

    def test_state_passes_resolved_governance_context_to_controller(self) -> None:
        original_state = copy.deepcopy(state.STATE)
        session = SimpleNamespace(
            session_id="session-1",
            deployment_id="dep-selected",
            metadata={"portfolioId": "portfolio-1", "strategyId": "selected"},
        )
        state.STATE.setdefault("active_runtime_session_ids", {})["crypto"] = (
            "previous-session"
        )

        def prepare(*_args):
            state.STATE["active_runtime_session_ids"]["crypto"] = session.session_id
            return session, "unit"

        restored_binding = ""
        try:
            with (
                patch.object(
                    state,
                    "_prepare_operational_runtime_session",
                    side_effect=prepare,
                ),
                patch.object(
                    state.LIVE_CONTINUOUS_CONTROLLER,
                    "start",
                    return_value={"ok": False, "reason": "context blocked"},
                ) as start,
                patch.object(state, "_finish_operational_runtime_start") as finish,
                patch.object(state, "snapshot", return_value={}),
            ):
                result = state.start_continuous_runtime(
                    "crypto",
                    "SMALL_LIVE",
                    "portfolio-1",
                    "dep-selected",
                    "selected",
                )
                restored_binding = str(
                    state.STATE["active_runtime_session_ids"].get("crypto") or ""
                )
        finally:
            state.STATE.clear()
            state.STATE.update(original_state)

        self.assertFalse(result["ok"])
        self.assertEqual("previous-session", restored_binding)
        start.assert_called_once_with(
            "crypto",
            "SMALL_LIVE",
            "portfolio-1",
            "selected",
            "dep-selected",
        )
        finish.assert_called_once_with(session, False, "context blocked")

    def test_mismatched_evaluation_cannot_create_order_intent(self) -> None:
        controller = LiveContinuousController(Path("."))
        spec = controller._standalone_spec(
            _strategy("selected", deployment_id="dep-selected")
        )
        controller.profile_id = "crypto"
        controller.mode = "SMALL_LIVE"
        controller.deployment_id = "dep-selected"
        controller.requested_strategy_id = "selected"
        controller.strategy_ids = ("selected",)
        controller.allowed_symbols = ("BTCUSDT",)
        controller.supervisor = MagicMock()
        controller.supervisor.engine.specs = (spec,)
        decision = SimpleNamespace(
            strategy_id="other",
            strategy_instance_id=spec.strategy_instance_id,
            signal="BUY",
            reason="unit mismatch",
            evaluation_key="evaluation-1",
            bar=SimpleNamespace(close=100.0, end_time="2026-08-01T00:00:00Z"),
        )

        with (
            patch.object(state, "append_audit"),
            patch.object(state, "submit_order_intent") as submit,
        ):
            result = controller._handle_cycle(
                SimpleNamespace(decisions=(decision,))
            )

        self.assertEqual("BLOCKED", result["results"][0]["action"])
        submit.assert_not_called()

    def test_portfolio_manifest_binds_every_strategy_symbol_pair(self) -> None:
        selected = _strategy("alpha", deployment_id="dep-alpha")
        selected["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-1",
        }
        beta = _strategy("beta", deployment_id="dep-beta", symbol="ETHUSDT")
        portfolio = {
            "id": "portfolio-1",
            "strategy_instances": [
                {
                    "sourceStrategyId": "alpha",
                    "strategyId": "alpha",
                    "symbol": "BTCUSDT",
                    "sourceArtifactHash": "source-alpha",
                },
                {
                    "sourceStrategyId": "beta",
                    "strategyId": "beta",
                    "symbol": "ETHUSDT",
                    "sourceArtifactHash": "source-beta",
                },
            ],
        }

        with (
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(state, "load_strategy_artifacts", return_value=[selected, beta]),
        ):
            inputs = state._operational_manifest_inputs(selected)

        metadata = inputs["metadata"]
        self.assertEqual(["alpha", "beta"], metadata["strategyIds"])
        self.assertEqual(["BTCUSDT", "ETHUSDT"], metadata["allowedSymbols"])
        self.assertEqual(
            {("alpha", "BTCUSDT"), ("beta", "ETHUSDT")},
            {
                (item["strategyId"], item["symbol"])
                for item in metadata["strategyMembers"]
            },
        )
        self.assertTrue(inputs["portfolio_artifact_hash"])

    def test_cross_broker_portfolio_is_fail_closed_until_scoped_preflight_exists(self) -> None:
        selected = _strategy("alpha", deployment_id="dep-alpha")
        selected["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-mixed",
        }
        upbit = _strategy(
            "beta",
            deployment_id="dep-beta",
            symbol="KRW-ETH",
        )
        upbit["broker_id"] = "upbit"
        upbit["provider"] = "upbit"
        portfolio = {
            "id": "portfolio-mixed",
            "strategy_instances": [
                {"strategyId": "alpha", "symbol": "BTCUSDT"},
                {"strategyId": "beta", "symbol": "KRW-ETH"},
            ],
        }

        with (
            patch.object(state, "portfolio_rows", return_value=[portfolio]),
            patch.object(
                state,
                "load_strategy_artifacts",
                return_value=[selected, upbit],
            ),
        ):
            with self.assertRaisesRegex(ValueError, "cross-broker"):
                state._operational_manifest_inputs(selected)

        specs = (
            LiveContinuousController._standalone_spec(selected),
            LiveContinuousController._standalone_spec(upbit),
        )
        blocker = LiveContinuousController._single_broker_context_blocker(specs)
        self.assertIn("cross-broker", blocker)

    def test_dispatch_authorization_accepts_portfolio_member_and_rejects_outsider(self) -> None:
        current = _strategy("beta", deployment_id="dep-beta", symbol="ETHUSDT")
        current["portfolio_gate"] = {
            "active": True,
            "allowed": True,
            "portfolioId": "portfolio-1",
        }
        member_hash = state._operational_artifact_hash(current)
        paper_bindings = [
            {
                "strategyId": strategy_id,
                "strategyInstanceId": f"instance-{strategy_id}",
                "paperEvidenceId": f"paper-{strategy_id}",
                "paperEvidenceHash": ("1" if strategy_id == "alpha" else "2") * 64,
                "paperEvidenceBundleHash": ("5" if strategy_id == "alpha" else "6") * 64,
                "paperFinalBindingHash": ("3" if strategy_id == "alpha" else "4") * 64,
                "paperGovernanceDeploymentId": f"paper-deployment-{strategy_id}",
                "paperPortfolioArtifactId": "portfolio-1",
                "paperPortfolioArtifactHash": "b" * 64,
                "paperPortfolioInstanceId": "portfolio-instance-1",
                "ready": True,
            }
            for strategy_id in ("alpha", "beta")
        ]
        portfolio_reference = {
            "artifactId": "portfolio-1",
            "artifactHash": "b" * 64,
            "contentHash": "9" * 64,
        }
        deployment_binding = {
            "source": "deployment-store",
            "deploymentId": "dep-alpha",
            "revision": 2,
            "lifecycle": "before-live-small",
            "environment": "LIVE",
            "mode": "SMALL_LIVE",
            "executionPermissionDigest": "8" * 64,
            "strategyArtifact": {
                "artifactId": "alpha",
                "artifactHash": "7" * 64,
                "contentHash": "6" * 64,
            },
            "portfolioArtifact": portfolio_reference,
        }
        deployment_binding["bindingHash"] = state.governance_sha256(
            deployment_binding
        )
        manifest = SimpleNamespace(
            deployment_id="dep-alpha",
            broker_route="binance",
            strategy_artifact_hash="a" * 64,
            portfolio_artifact_hash="b" * 64,
            risk_policy_hash="c" * 64,
            config_revision=2,
            config_hash="d" * 64,
            metadata={
                "portfolioId": "portfolio-1",
                "portfolioArtifact": portfolio_reference,
                "deploymentBinding": deployment_binding,
                "deploymentBindingHash": deployment_binding["bindingHash"],
                "brokerRoutes": ["binance"],
                "strategyMembers": [
                    {
                        "strategyId": "alpha",
                        "strategyInstanceId": "instance-alpha",
                        "symbol": "ETHUSDT",
                        "brokerId": "binance",
                        "artifactHash": "e" * 64,
                    },
                    {
                        "strategyId": "beta",
                        "strategyInstanceId": "instance-beta",
                        "symbol": "ETHUSDT",
                        "brokerId": "binance",
                        "artifactHash": member_hash,
                    },
                ],
                "paperFinalBindings": paper_bindings,
                "paperFinalBindingsHash": state.governance_sha256(
                    paper_bindings
                ),
            },
        )
        intent = OrderIntent(
            strategy_id="beta",
            asset="코인",
            symbol="ETHUSDT",
            side="BUY",
            quantity=0.001,
            reference_price=3000.0,
            mode="SMALL_LIVE",
            reason="unit",
            metadata={
                "broker_id": "binance",
                "portfolio_id": "portfolio-1",
                "strategy_instance_id": "instance-beta",
                "multi_strategy": True,
                "strategy_instance_ids": [
                    "instance-alpha",
                    "instance-beta",
                ],
            },
        )
        current_inputs = {
            "deployment_id": manifest.deployment_id,
            "strategy_artifact_hash": manifest.strategy_artifact_hash,
            "portfolio_artifact_hash": manifest.portfolio_artifact_hash,
            "account_fingerprint": "f" * 64,
            "broker_route": manifest.broker_route,
            "runtime_version": "live-trader-runtime-v2",
            "build_hash": "1" * 64,
            "execution_adapter": "binance-signed-adapter",
            "execution_adapter_version": "v2",
            "risk_policy_revision": 1,
            "risk_policy_hash": manifest.risk_policy_hash,
            "config_revision": manifest.config_revision,
            "config_hash": manifest.config_hash,
            "preflight_ttl_seconds": 300,
            "metadata": dict(manifest.metadata),
        }
        original_sessions = copy.deepcopy(state.STATE.get("active_runtime_session_ids"))
        original_mode = state.STATE.get("mode")
        state.STATE["active_runtime_session_ids"] = {"crypto": "session-1"}
        state.STATE["mode"] = "SMALL_LIVE"
        try:
            with (
                patch.object(
                    state.OPERATIONAL_GOVERNANCE,
                    "runtime_authorization",
                    return_value={
                        "allowed": True,
                        "reasons": [],
                        "session": {
                            "deploymentManifestHash": "manifest-1",
                            "mode": "SMALL_LIVE",
                            "metadata": {
                                "deploymentBinding": deployment_binding,
                                "deploymentBindingHash": deployment_binding[
                                    "bindingHash"
                                ],
                            },
                        },
                    },
                ) as authorize,
                patch.object(
                    state.OPERATIONAL_GOVERNANCE,
                    "get_deployment_manifest_by_hash",
                    return_value=manifest,
                ),
                patch.object(state, "portfolio_rows", return_value=[]),
                patch.object(state, "strategy_rows", return_value=[current]),
                patch.object(
                    state,
                    "_operational_manifest_inputs",
                    return_value=current_inputs,
                ),
            ):
                allowed = state.operational_runtime_dispatch_allowed(intent)
                missing_instance = state.operational_runtime_dispatch_allowed(
                    OrderIntent(
                        **{
                            **intent.__dict__,
                            "metadata": {
                                "broker_id": "binance",
                                "portfolio_id": "portfolio-1",
                            },
                        }
                    )
                )
                wrong_instance = state.operational_runtime_dispatch_allowed(
                    OrderIntent(
                        **{
                            **intent.__dict__,
                            "metadata": {
                                **intent.metadata,
                                "strategy_instance_id": "instance-alpha",
                            },
                        }
                    )
                )
                unbound_sleeve = state.operational_runtime_dispatch_allowed(
                    OrderIntent(
                        **{
                            **intent.__dict__,
                            "metadata": {
                                **intent.metadata,
                                "strategy_instance_ids": [
                                    "instance-beta",
                                    "instance-not-bound",
                                ],
                            },
                        }
                    )
                )
                outsider = state.operational_runtime_dispatch_allowed(
                    OrderIntent(
                        **{
                            **intent.__dict__,
                            "strategy_id": "outsider",
                        }
                    )
                )
                manifest.metadata["brokerRoutes"] = ["binance", "upbit"]
                mixed_route = state.operational_runtime_dispatch_allowed(intent)
                manifest.metadata["brokerRoutes"] = ["binance"]
                revised_binding = {
                    **deployment_binding,
                    "revision": 3,
                }
                revised_binding["bindingHash"] = state.governance_sha256(
                    {
                        key: value
                        for key, value in revised_binding.items()
                        if key != "bindingHash"
                    }
                )
                current_inputs["metadata"][
                    "deploymentBinding"
                ] = revised_binding
                current_inputs["metadata"][
                    "deploymentBindingHash"
                ] = revised_binding["bindingHash"]
                revised_deployment = (
                    state.operational_runtime_dispatch_allowed(intent)
                )
                current_inputs["metadata"][
                    "deploymentBinding"
                ] = deployment_binding
                current_inputs["metadata"][
                    "deploymentBindingHash"
                ] = deployment_binding["bindingHash"]
                current_inputs["deployment_id"] = "dep-replaced"
                replaced_deployment = state.operational_runtime_dispatch_allowed(
                    intent
                )
        finally:
            state.STATE["active_runtime_session_ids"] = original_sessions
            state.STATE["mode"] = original_mode

        self.assertTrue(allowed[0])
        self.assertEqual("operational-runtime-authorized", allowed[1])
        self.assertFalse(missing_instance[0])
        self.assertEqual(
            "operational-strategy-instance-context-missing",
            missing_instance[1],
        )
        self.assertFalse(wrong_instance[0])
        self.assertEqual(
            "operational-strategy-instance-context-mismatch",
            wrong_instance[1],
        )
        self.assertFalse(unbound_sleeve[0])
        self.assertEqual(
            "operational-multi-strategy-instance-context-mismatch",
            unbound_sleeve[1],
        )
        self.assertFalse(outsider[0])
        self.assertEqual("operational-strategy-context-mismatch", outsider[1])
        self.assertFalse(mixed_route[0])
        self.assertEqual(
            "operational-cross-broker-portfolio-blocked",
            mixed_route[1],
        )
        self.assertFalse(revised_deployment[0])
        self.assertEqual(
            "operational-deployment-binding-changed",
            revised_deployment[1],
        )
        self.assertFalse(replaced_deployment[0])
        self.assertEqual(
            "operational-deployment-context-changed",
            replaced_deployment[1],
        )
        self.assertEqual(8, authorize.call_count)
        self.assertTrue(
            all(
                call.kwargs.get("require_fresh_preflight") is True
                for call in authorize.call_args_list
            )
        )


if __name__ == "__main__":
    unittest.main()
