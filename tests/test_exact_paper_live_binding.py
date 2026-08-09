from __future__ import annotations

from copy import deepcopy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[3]
COMMON_TESTS = ROOT / "packages" / "trading_runtime" / "tests"
COMMON_PACKAGE = ROOT / "packages" / "trading_runtime"
PAPER_APP = ROOT / "apps" / "paper_trader"
if str(COMMON_PACKAGE) not in sys.path:
    sys.path.insert(0, str(COMMON_PACKAGE))
if str(COMMON_TESTS) not in sys.path:
    # Helpers are available without shadowing this app's same-named tests
    # during unittest discovery.
    sys.path.append(str(COMMON_TESTS))
if str(PAPER_APP) not in sys.path:
    sys.path.insert(0, str(PAPER_APP))

from desktop.forward_evidence import (  # noqa: E402
    FORWARD_EVIDENCE_SOURCE,
    ForwardEvidenceRecord,
    build_forward_boundary_coverage,
)
from desktop.paper_governance import (  # noqa: E402
    EvidenceBundle,
    EvidenceGateState,
    FinalPositionReconciliation,
    RuntimeIdentity,
    build_live_candidate_publication_v2,
    build_paper_forward_scope,
    build_paper_live_evidence_envelope,
)
from test_paper_live_contract import golden_paper_live_evidence  # noqa: E402
from trading_runtime import (  # noqa: E402
    EvidenceStore,
    artifact_reference,
    build_paper_portfolio_evidence,
    parse_paper_live_evidence,
    seal_strategy_artifact,
    stable_sha256,
)
from trading_runtime.paper_live_contract import (  # noqa: E402
    PAPER_PROMOTION_QUALIFICATION_SCHEMA_VERSION,
    PaperLiveContractError,
    paper_qualification_epoch_hash,
)
from live_trader import state  # noqa: E402
from live_trader.contracts import load_pinned_paper_live_qualification  # noqa: E402
from live_trader.operational_governance import (  # noqa: E402
    OperationalGovernanceStore,
)
from live_trader.order_management import OrderIntent  # noqa: E402


def _hash(label: str) -> str:
    return stable_sha256({"label": label})


def production_paper_v2_evidence(
    *,
    sparse_forged_coverage: bool = False,
) -> tuple[dict, dict]:
    """Produce evidence through Paper Trader's real v2 publication path."""

    strategy_artifact = seal_strategy_artifact(
        {
            "id": "strategy-paper-production-v2",
            "artifactType": "strategy",
            "schemaVersion": "strategy-artifact-v1",
            "name": "Paper production v2 integration",
            "plugin": "moving_average_cross",
            "symbol": "BTCUSDT",
            "timeframe": "1d",
            "parameters": {"shortMa": 3, "longMa": 5},
        }
    )
    reference = artifact_reference(strategy_artifact)
    session_id = "paper-session-production-v2"
    paper_deployment_id = "paper-governance-production-v2"
    manifest_hash = _hash("paper-production-manifest")
    ledger_hash = _hash("paper-production-ledger")
    source_selection_hash = _hash("paper-production-source-selection")
    forward_evidence_portfolio_hash = _hash(
        "paper-production-forward-evidence-portfolio"
    )
    started = datetime(2026, 6, 1, tzinfo=timezone.utc)
    ended = started + timedelta(days=30)
    qualification_epoch_hash = paper_qualification_epoch_hash(
        paper_governance_deployment_id=paper_deployment_id,
        session_id=session_id,
        deployment_manifest_hash=manifest_hash,
        ledger_generation=1,
        strategy_artifact_id=reference["artifactId"],
        strategy_artifact_hash=reference["artifactHash"],
        strategy_instance_id=f"standalone:{reference['artifactId']}",
        portfolio_required=False,
        forward_evidence_portfolio_hash=forward_evidence_portfolio_hash,
        started_at=started,
    )

    records = [
        ForwardEvidenceRecord(
            evidence_key=f"paper-production-boundary-{index}",
            recorded_at=boundary.isoformat(),
            bar_end=boundary.isoformat(),
            artifact_hash=reference["artifactHash"],
            portfolio_id="",
            portfolio_hash="",
            session_id=session_id,
            deployment_manifest_hash=manifest_hash,
            strategy_id=reference["artifactId"],
            strategy_instance_id=(
                f"standalone:{reference['artifactId']}"
            ),
            mode="PAPER",
            symbol="BTCUSDT",
            signal="HOLD",
            decision="OBSERVE",
            order_state="OBSERVED",
            order_ids=(),
            equity=100_000.0,
            cash=100_000.0,
            regime="RISK_ON" if index < 16 else "RISK_OFF",
            source=FORWARD_EVIDENCE_SOURCE,
            promotion_eligible=True,
            recovery_only=False,
            filled_count=0,
            rejected_count=0,
            risk_blocked_count=0,
        )
        for index, boundary in enumerate(
            started + timedelta(days=index) for index in range(1, 31)
        )
    ]
    coverage = build_forward_boundary_coverage(
        records,
        provider="binance",
        instrument_id="BTCUSDT",
        symbol="BTCUSDT",
        timeframe="1d",
        qualification_epoch_started_at=started.isoformat(),
        qualification_epoch_hash=qualification_epoch_hash,
    )
    if sparse_forged_coverage:
        # Simulate an attacker resealing only the first/last observation while
        # falsely declaring those two rows to be the entire 30-day calendar.
        coverage.update(
            {
                "expectedClosedBoundaryCount": 2,
                "observedPromotionEligibleBoundaryCount": 2,
                "coverageRatio": 1.0,
                "maximumConsecutiveMissedBoundaries": 0,
            }
        )
        coverage.pop("contentHash", None)
        coverage["contentHash"] = stable_sha256(coverage)

    metrics = {
        "returnPct": 1.25,
        "maxDrawdownPct": -0.75,
        "paperOrderCount": 5,
        "forwardTickCount": 30,
        "forwardObservedDays": 30,
        "forwardElapsedSeconds": 30 * 24 * 60 * 60,
        "forwardRegimeCount": 2,
        "recoveryVerified": True,
        "reconciliationMismatches": 0,
        "dataQualityFailures": 0,
        "implementationMismatch": False,
        "boundaryCoverage": coverage,
    }
    promotion = {
        "schemaVersion": PAPER_PROMOTION_QUALIFICATION_SCHEMA_VERSION,
        "paperOrderCount": metrics["paperOrderCount"],
        "filledCount": metrics["paperOrderCount"],
        "rejectedCount": 0,
        "forwardTickCount": metrics["forwardTickCount"],
        "forwardObservedDays": metrics["forwardObservedDays"],
        "forwardElapsedSeconds": metrics["forwardElapsedSeconds"],
        "forwardRegimeCount": metrics["forwardRegimeCount"],
        "recoveryVerified": metrics["recoveryVerified"],
        "reconciliationMismatches": metrics["reconciliationMismatches"],
        "dataQualityFailures": metrics["dataQualityFailures"],
        "implementationMismatch": metrics["implementationMismatch"],
        "boundaryCoverageHash": coverage["contentHash"],
    }
    bundle = EvidenceBundle(
        evidence_id="paper-production-v2",
        version=1,
        session_id=session_id,
        deployment_id=paper_deployment_id,
        deployment_manifest_hash=manifest_hash,
        strategy_artifact_hash=reference["artifactHash"],
        portfolio_artifact_hash="",
        finance_gate=EvidenceGateState.PASS,
        operations_gate=EvidenceGateState.PASS,
        runtime_identity=RuntimeIdentity(
            simulator_profile="LOCAL_PAPER_NEXT_OPEN_V3",
            simulator_version="3",
            risk_profile="paper-risk-v1",
            risk_version="1",
            data_profile="closed-bar-open-boundary-v1",
            data_version="1",
            random_seed=7,
        ),
        created_at=ended,
        finance={"returnPct": metrics["returnPct"]},
        operations={
            "recoveryVerified": True,
            "promotionQualification": promotion,
        },
    )
    final_reconciliation = FinalPositionReconciliation(
        session_id=session_id,
        deployment_id=paper_deployment_id,
        deployment_manifest_hash=manifest_hash,
        ledger_generation=1,
        runtime_status={
            "runtimeKey": _hash("paper-production-runtime-key")[:24],
            "contentHash": _hash("paper-production-runtime-status"),
            "phase": "STOPPED",
            "running": False,
            "pendingBarCount": 0,
            "pendingOpenBoundaryCount": 0,
            "pendingEventCount": 0,
            "sessionId": session_id,
            "deploymentManifestHash": manifest_hash,
            "ledgerGeneration": 1,
        },
        ledger={
            "sessionLedgerHash": ledger_hash,
            "headHash": _hash("paper-production-ledger-head"),
            "workingOrderCount": 0,
        },
        intent={
            "source": "PENDING_INTENT_V3",
            "headHash": _hash("paper-production-intent-head"),
            "terminalResolutionHash": _hash(
                "paper-production-intent-resolution"
            ),
            "nonTerminalCount": 0,
            "ambiguousCount": 0,
        },
        reconciliation={
            "internalSnapshotHash": _hash("paper-production-internal"),
            "brokerSnapshotHash": _hash("paper-production-broker"),
            "linesHash": _hash("paper-production-lines"),
            "matched": True,
            "mismatchCount": 0,
            "positionFlat": True,
            "internalPositionCount": 0,
            "brokerPositionCount": 0,
            "internalNetPosition": 0.0,
            "brokerNetPosition": 0.0,
        },
        trace={
            "lastEventHash": _hash("paper-production-last-event"),
            "sourceSelectionHash": source_selection_hash,
        },
        finalized_at=ended,
    )
    forward_scope = build_paper_forward_scope(
        paper_governance_deployment_id=paper_deployment_id,
        session_id=session_id,
        deployment_manifest_hash=manifest_hash,
        ledger_generation=1,
        strategy_artifact_id=reference["artifactId"],
        strategy_artifact_hash=reference["artifactHash"],
        strategy_instance_id=f"standalone:{reference['artifactId']}",
        portfolio_required=False,
        forward_evidence_portfolio_hash=forward_evidence_portfolio_hash,
        qualification_epoch_started_at=started,
        session_ledger_hash=ledger_hash,
        source_selection_hash=source_selection_hash,
    )
    publication = build_live_candidate_publication_v2(
        bundle,
        final_reconciliation,
        forward_scope,
        published_at=ended,
    )
    evidence = build_paper_live_evidence_envelope(
        bundle=bundle,
        publication=publication,
        final_reconciliation=final_reconciliation,
        forward_scope=forward_scope,
        strategy_artifact=strategy_artifact,
        portfolio_artifact=None,
        metrics=metrics,
    )
    return evidence, strategy_artifact


def exact_evidence_for_artifact(
    artifact: dict,
    *,
    evidence_id: str = "paper-resume-current",
    observed_days: int = 30,
    observed_seconds: int = 30 * 24 * 60 * 60,
    regime_count: int = 2,
    recovery_verified: bool = True,
    reconciliation_mismatches: int = 0,
    ended_at: str = "2026-07-25T00:00:00+00:00",
    promotion_source: str = "continuous-live-forward-next-open-v3",
) -> tuple[dict, dict]:
    """Rebind the common golden contract to one exact test Artifact."""

    result = deepcopy(golden_paper_live_evidence(portfolio=False))
    reference = artifact_reference(artifact)
    strategy_id = str(reference["artifactId"])
    strategy_hash = str(reference["artifactHash"])
    strategy_instance_id = f"standalone:{strategy_id}"
    paper_deployment_id = f"paper-governance:{strategy_id}"
    session_id = f"paper-session:{evidence_id}"
    manifest_hash = stable_sha256(
        {"deploymentId": paper_deployment_id, "evidenceId": evidence_id}
    )
    seed_forward = result["details"]["paperQualification"]["forwardScope"]
    forward_evidence_portfolio_hash = stable_sha256(
        {
            "evidenceId": evidence_id,
            "strategyArtifactHash": strategy_hash,
            "strategyInstanceId": strategy_instance_id,
        }
    )

    result["evidenceId"] = evidence_id
    result["strategyArtifact"] = reference
    result["deploymentId"] = paper_deployment_id
    result["endedAt"] = ended_at
    parsed_ended_at = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
    if parsed_ended_at.tzinfo is None:
        parsed_ended_at = parsed_ended_at.replace(tzinfo=timezone.utc)
    parsed_started_at = (
        parsed_ended_at.astimezone(timezone.utc)
        - timedelta(seconds=observed_seconds)
    )
    result["startedAt"] = parsed_started_at.isoformat()
    canonical_epoch_started_at = parsed_started_at.isoformat(
        timespec="microseconds"
    )
    qualification_epoch_hash = paper_qualification_epoch_hash(
        paper_governance_deployment_id=paper_deployment_id,
        session_id=session_id,
        deployment_manifest_hash=manifest_hash,
        ledger_generation=int(seed_forward["ledgerGeneration"]),
        strategy_artifact_id=strategy_id,
        strategy_artifact_hash=strategy_hash,
        strategy_instance_id=strategy_instance_id,
        portfolio_required=False,
        forward_evidence_portfolio_hash=forward_evidence_portfolio_hash,
        started_at=canonical_epoch_started_at,
    )
    expected_boundary_count = int(observed_seconds // 60)
    coverage = result["metrics"]["boundaryCoverage"]
    coverage.update(
        {
            "schemaVersion": "paper-forward-boundary-coverage-v2",
            "timeframe": "1m",
            "qualificationEpochStartedAt": canonical_epoch_started_at,
            "qualificationEpochHash": qualification_epoch_hash,
            "windowStart": canonical_epoch_started_at,
            "windowEnd": parsed_ended_at.astimezone(timezone.utc).isoformat(
                timespec="microseconds"
            ),
            "expectedClosedBoundaryCount": expected_boundary_count,
            "observedPromotionEligibleBoundaryCount": expected_boundary_count,
            "coverageRatio": 1.0,
            "maximumConsecutiveMissedBoundaries": 0,
        }
    )
    coverage.pop("contentHash", None)
    coverage["contentHash"] = stable_sha256(coverage)
    result.update(
        {
            "orderCount": 5,
            "filledCount": 5,
            "rejectedCount": 0,
        }
    )
    result["metrics"].update(
        {
            "paperOrderCount": 5,
            "forwardTickCount": expected_boundary_count,
            "forwardObservedDays": observed_days,
            "forwardElapsedSeconds": observed_seconds,
            "forwardRegimeCount": regime_count,
            "recoveryVerified": recovery_verified,
            "reconciliationMismatches": reconciliation_mismatches,
        }
    )
    result["details"]["evidencePolicy"] = {
        "promotionSource": promotion_source,
    }
    result["details"]["lifecyclePolicy"] = {
        "action": "PROMOTE",
        "targetStage": "before-live-small",
        "inputs": {
            "reconciliation_mismatches": reconciliation_mismatches,
        },
    }
    qualification = result["details"]["paperQualification"]
    bundle = qualification["evidence"]
    promotion = bundle["operations"]["promotionQualification"]
    promotion.update(
        {
            "paperOrderCount": 5,
            "filledCount": 5,
            "rejectedCount": 0,
            "forwardTickCount": expected_boundary_count,
            "forwardObservedDays": observed_days,
            "forwardElapsedSeconds": observed_seconds,
            "forwardRegimeCount": regime_count,
            "recoveryVerified": recovery_verified,
            "reconciliationMismatches": reconciliation_mismatches,
            "dataQualityFailures": 0,
            "implementationMismatch": False,
            "boundaryCoverageHash": coverage["contentHash"],
        }
    )
    bundle.update(
        {
            "evidenceId": evidence_id,
            "sessionId": session_id,
            "deploymentId": paper_deployment_id,
            "deploymentManifestHash": manifest_hash,
            "strategyArtifactHash": strategy_hash,
            "createdAt": ended_at,
        }
    )
    bundle.pop("contentHash", None)
    bundle["contentHash"] = stable_sha256(bundle)

    forward = qualification["forwardScope"]
    forward.update(
        {
            "schemaVersion": "paper-forward-scope-v2",
            "paperGovernanceDeploymentId": paper_deployment_id,
            "sessionId": session_id,
            "deploymentManifestHash": manifest_hash,
            "strategyArtifactId": strategy_id,
            "strategyArtifactHash": strategy_hash,
            "strategyInstanceId": strategy_instance_id,
            "forwardEvidencePortfolioHash": forward_evidence_portfolio_hash,
            "qualificationEpochStartedAt": canonical_epoch_started_at,
            "qualificationEpochHash": qualification_epoch_hash,
        }
    )
    forward.pop("contentHash", None)
    forward["contentHash"] = stable_sha256(forward)

    final = qualification["finalReconciliation"]
    final.update(
        {
            "sessionId": session_id,
            "deploymentId": paper_deployment_id,
            "deploymentManifestHash": manifest_hash,
            "finalizedAt": ended_at,
        }
    )
    final["runtimeStatus"].update(
        {
            "sessionId": session_id,
            "deploymentManifestHash": manifest_hash,
        }
    )
    final.pop("sealId", None)
    final.pop("contentHash", None)
    final_hash = stable_sha256(final)
    final["sealId"] = f"paper-final-reconciliation-{final_hash[:32]}"
    final["contentHash"] = final_hash

    publication = qualification["publication"]
    publication.update(
        {
            "publicationId": f"live-candidate:{evidence_id}",
            "evidenceId": evidence_id,
            "evidenceContentHash": bundle["contentHash"],
            "sessionId": session_id,
            "paperGovernanceDeploymentId": paper_deployment_id,
            "deploymentManifestHash": manifest_hash,
            "strategyArtifactId": strategy_id,
            "strategyArtifactHash": strategy_hash,
            "strategyInstanceId": strategy_instance_id,
            "finalReconciliationSealId": final["sealId"],
            "finalReconciliationContentHash": final_hash,
            "sourceSelectionHash": forward["sourceSelectionHash"],
            "forwardScopeHash": forward["contentHash"],
            "publishedAt": ended_at,
        }
    )
    publication.pop("publicationHash", None)
    publication["publicationHash"] = stable_sha256(publication)

    binding_source = {
        "schemaVersion": "paper-live-final-binding-v1",
        "evidenceId": evidence_id,
        "evidenceHash": bundle["contentHash"],
        "evidenceVersion": bundle["version"],
        "publicationId": publication["publicationId"],
        "publicationHash": publication["publicationHash"],
        "strategyArtifactId": strategy_id,
        "strategyArtifactHash": strategy_hash,
        "strategyInstanceId": strategy_instance_id,
        "portfolioRequired": False,
        "portfolioArtifactId": "",
        "portfolioArtifactHash": "",
        "portfolioInstanceId": "",
        "paperGovernanceDeploymentId": paper_deployment_id,
        "sessionId": session_id,
        "deploymentManifestHash": manifest_hash,
        "ledgerGeneration": forward["ledgerGeneration"],
        "sessionLedgerHash": forward["sessionLedgerHash"],
        "finalReconciliationSealId": final["sealId"],
        "finalReconciliationContentHash": final_hash,
        "sourceSelectionHash": forward["sourceSelectionHash"],
        "forwardScope": forward,
    }
    qualification["bindingHash"] = stable_sha256(binding_source)
    result["integrity"]["contentHash"] = stable_sha256(
        {key: value for key, value in result.items() if key != "integrity"}
    )
    return result, {
        "paperEvidenceId": evidence_id,
        "paperEvidenceHash": result["integrity"]["contentHash"],
        "paperEvidenceBundleHash": bundle["contentHash"],
        "paperFinalBindingHash": qualification["bindingHash"],
        "paperGovernanceDeploymentId": paper_deployment_id,
        "paperStrategyInstanceId": strategy_instance_id,
        "paperPortfolioInstanceId": "",
    }


class ExactPaperLiveBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_state = deepcopy(state.STATE)

    def tearDown(self) -> None:
        state.STATE.clear()
        state.STATE.update(deepcopy(self._original_state))

    def _deployment(self, evidence: dict, *, pins: bool = True) -> dict:
        qualification = parse_paper_live_evidence(evidence)
        permissions = {
            "live_small_eligible": True,
            "live_eligible": False,
        }
        if pins:
            permissions.update(
                {
                    "paperEvidenceId": qualification.evidence.evidence_id,
                    "paperEvidenceHash": qualification.evidence.envelope_hash,
                    "paperEvidenceBundleHash": qualification.evidence.bundle_hash,
                    "paperFinalBindingHash": qualification.binding_hash,
                    "paperGovernanceDeploymentId": (
                        qualification.evidence.paper_governance_deployment_id
                    ),
                    "paperStrategyInstanceId": (
                        qualification.forward_scope.strategy_instance_id
                    ),
                    "paperPortfolioInstanceId": (
                        qualification.forward_scope.portfolio_instance_id
                    ),
                }
            )
        return {
            "deploymentId": "live-deployment-distinct-from-paper",
            "environment": "SMALL_LIVE",
            "strategyArtifact": dict(evidence["strategyArtifact"]),
            "portfolioArtifact": (
                dict(evidence["portfolioArtifact"])
                if isinstance(evidence.get("portfolioArtifact"), dict)
                else None
            ),
            "permissions": permissions,
        }

    def _load(self, root: Path, evidence: dict, deployment: dict) -> dict:
        return load_pinned_paper_live_qualification(
            root,
            {"id": evidence["strategyArtifact"]["artifactId"]},
            deployment,
            strategy_reference=dict(evidence["strategyArtifact"]),
            portfolio_reference=(
                dict(evidence["portfolioArtifact"])
                if isinstance(evidence.get("portfolioArtifact"), dict)
                else {}
            ),
        )

    def test_exact_pinned_record_is_loaded_for_portfolio(self) -> None:
        evidence = golden_paper_live_evidence()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = self._load(root, evidence, self._deployment(evidence))
        self.assertTrue(result["ready"], result["issues"])
        self.assertEqual("live-deployment-distinct-from-paper", self._deployment(evidence)["deploymentId"])
        self.assertEqual("paper-deployment-golden", result["paperGovernanceDeploymentId"])

    def test_actual_paper_v2_producer_is_the_exact_pinned_source(self) -> None:
        evidence, _artifact = production_paper_v2_evidence()
        qualification = parse_paper_live_evidence(evidence)
        deployment = self._deployment(evidence)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = EvidenceStore(root)
            store.save_paper(evidence)
            # A newer generic/legacy record with the same Strategy ID is not
            # authority and must never replace the exact v2 pin.
            store.save_paper(
                build_paper_portfolio_evidence(
                    evidence_id="newer-legacy-record",
                    strategy_artifact={
                        "id": evidence["strategyArtifact"]["artifactId"]
                    },
                    portfolio_artifact=None,
                    deployment_id="paper-governance-newer-legacy",
                    runtime_version="paper-trader-legacy",
                    ended_at="2026-07-02T00:00:00+00:00",
                    status="submitted",
                    order_count=99,
                    filled_count=99,
                )
            )
            result = self._load(root, evidence, deployment)

        self.assertTrue(result["ready"], result["issues"])
        self.assertEqual("paper-live-candidate-publication-v2", qualification.publication.payload["schemaVersion"])
        self.assertEqual(evidence["evidenceId"], result["evidenceId"])
        self.assertEqual(
            qualification.binding_hash,
            result["bindingHash"],
        )

    def test_actual_producer_rejects_resealed_sparse_thirty_day_coverage(self) -> None:
        with self.assertRaises(PaperLiveContractError):
            production_paper_v2_evidence(sparse_forged_coverage=True)

    def test_v1_publication_is_not_implicitly_upgraded(self) -> None:
        evidence, _artifact = production_paper_v2_evidence()
        deployment = self._deployment(evidence)
        changed = deepcopy(evidence)
        publication = changed["details"]["paperQualification"]["publication"]
        publication["schemaVersion"] = "paper-live-candidate-publication-v1"
        publication.pop("publicationHash", None)
        publication["publicationHash"] = stable_sha256(publication)
        changed["integrity"]["contentHash"] = stable_sha256(
            {key: value for key, value in changed.items() if key != "integrity"}
        )
        deployment["permissions"]["paperEvidenceHash"] = changed["integrity"][
            "contentHash"
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(changed)
            result = self._load(root, changed, deployment)

        self.assertFalse(result["ready"])
        self.assertEqual(
            ["paper-live-publication-schema-invalid"],
            result["issues"],
        )

    def test_actual_paper_v2_reaches_manifest_session_and_final_edge(self) -> None:
        evidence, _artifact = production_paper_v2_evidence()
        live_deployment = self._deployment(evidence)
        live_deployment["deploymentId"] = "live-production-v2"
        strategy_id = str(evidence["strategyArtifact"]["artifactId"])
        strategy_instance_id = f"standalone:{strategy_id}"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            paper_gate = self._load(root, evidence, live_deployment)
            self.assertTrue(paper_gate["ready"], paper_gate["issues"])
            strategy = {
                "deployment_id": live_deployment["deploymentId"],
                "strategy_id": strategy_id,
                "broker_id": "binance",
                "provider": "binance",
                "asset": "CRYPTO",
                "symbol": "BTCUSDT",
                "timeframe": "1d",
                "live_allowed": True,
                "live_small_eligible": True,
                "live_eligible": False,
                "lifecycle_status": "before-live-small",
                "portfolio_gate": {},
                "artifact_reference": dict(evidence["strategyArtifact"]),
                "strategy_instance_id": strategy_instance_id,
                "paper_live_qualification": paper_gate,
            }
            deployment_binding = {
                "source": "deployment-store",
                "deploymentId": live_deployment["deploymentId"],
                "revision": 2,
                "lifecycle": "before-live-small",
                "environment": "LIVE",
                "mode": "SMALL_LIVE",
                "executionPermissionDigest": _hash(
                    "live-production-v2-permission"
                ),
                "strategyArtifact": dict(evidence["strategyArtifact"]),
                "portfolioArtifact": {
                    "artifactId": "",
                    "artifactHash": "",
                    "contentHash": "",
                },
            }
            deployment_binding["bindingHash"] = state.governance_sha256(
                deployment_binding
            )
            brokers = [
                {
                    "broker_id": "binance",
                    "status": "ready",
                    "live_order_adapter_ready": True,
                }
            ]
            reconciliation = {
                "summary": {"status": "pass", "status_label": "normal"},
                "positions": [{"broker_id": "binance", "status": "pass"}],
                "accounts": [{"broker_id": "binance", "status": "pass"}],
                "errors": [],
            }
            snapshot = {
                "strategies": [strategy],
                "brokers": brokers,
                "reconciliation": reconciliation,
                "execution_streams": {},
                "generated_at": datetime.now().isoformat(),
                "mode": "MONITOR",
                "final_preflight": [],
            }
            state.STATE.update(
                {
                    "mode": "SMALL_LIVE",
                    "dry_run": False,
                    "operator_confirmed": True,
                    "new_entries_blocked": False,
                    "manual_new_entries_blocked": False,
                    "kill_switch": False,
                    "kill_switch_rearm_required": False,
                    "config_revision": 17,
                    "risk_policy_revision": 5,
                    "latest_preflight_snapshot_id": "",
                    "active_runtime_session_ids": {},
                    "orders": [],
                }
            )

            def fresh_poll(
                broker_id: str,
                *,
                force_snapshot: bool | None = None,
                include_snapshot: bool = True,
            ) -> dict[str, object]:
                self.assertEqual("binance", broker_id)
                self.assertTrue(force_snapshot)
                self.assertFalse(include_snapshot)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                state.STATE["broker_reconciliation"] = {
                    "fetched_at": now,
                    "accounts": [],
                    "positions": [],
                    "errors": [],
                    "successful_account_brokers": ["binance"],
                    "successful_position_brokers": ["binance"],
                    "position_observations": {
                        "binance": {"observedAt": now}
                    },
                }
                state.STATE["execution_events"].update(
                    {"last_poll": now, "errors": []}
                )
                state.STATE["program_ledger"]["last_event_sync"] = now
                return {"ok": True, "errors": []}

            governance = OperationalGovernanceStore(
                root / "operational-governance.sqlite3"
            )
            router_factory = Mock(
                side_effect=AssertionError("broker router must not be called")
            )
            with (
                patch.object(state, "OPERATIONAL_GOVERNANCE", governance),
                patch.object(state, "snapshot", return_value=snapshot),
                patch.object(state, "portfolio_rows", return_value=[]),
                patch.object(state, "strategy_rows", return_value=[strategy]),
                patch.object(
                    state,
                    "_current_live_deployment_binding",
                    return_value=deployment_binding,
                ),
                patch.object(state, "checklist_rows", return_value=[]),
                patch.object(
                    state,
                    "order_queue_summary",
                    return_value={
                        "total": 0,
                        "active": 0,
                        "blocked": 0,
                        "dry_run": 0,
                        "retryable": 0,
                        "canceled": 0,
                    },
                ),
                patch.object(state, "real_orders_enabled", return_value=True),
                patch.object(
                    state,
                    "poll_execution_events",
                    side_effect=fresh_poll,
                ),
                patch.object(
                    state,
                    "reconciliation_snapshot",
                    return_value=reconciliation,
                ),
                patch.object(
                    state,
                    "persist_doctor_diagnostic_snapshot",
                    return_value={"latest": {}},
                ),
                patch.object(state, "append_audit"),
                patch.object(state, "LiveBrokerRouter", router_factory),
            ):
                preflight = state.run_final_preflight(
                    live_deployment["deploymentId"],
                    strategy_id,
                )
                self.assertTrue(preflight["ok"])
                session, _ = state._prepare_operational_runtime_session(
                    "crypto",
                    "SMALL_LIVE",
                    "",
                    live_deployment["deploymentId"],
                    strategy_id,
                )
                state._finish_operational_runtime_start(
                    session,
                    True,
                    "runtime started",
                )
                intent = OrderIntent(
                    strategy_id=strategy_id,
                    asset="CRYPTO",
                    symbol="BTCUSDT",
                    side="BUY",
                    quantity=0.0001,
                    reference_price=60_000,
                    mode="SMALL_LIVE",
                    reason="actual Paper v2 final-edge authorization",
                    metadata={
                        "broker_id": "binance",
                        "strategy_instance_id": strategy_instance_id,
                        "confirmed_bar_end": (
                            datetime.now().astimezone().isoformat()
                        ),
                    },
                )
                invariant_allowed, invariant_reason = (
                    state.live_broker_dispatch_allowed(intent, dry_run=False)
                )
                allowed, reason, authorization = (
                    state.operational_runtime_dispatch_allowed(intent)
                )

            self.assertTrue(invariant_allowed, invariant_reason)
            self.assertTrue(allowed, reason)
            self.assertTrue(authorization["allowed"])
            router_factory.assert_not_called()

    def test_newer_matching_legacy_evidence_is_not_auto_adopted(self) -> None:
        evidence = golden_paper_live_evidence()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = EvidenceStore(root)
            store.save_paper(evidence)
            newer = build_paper_portfolio_evidence(
                evidence_id="newer-unpinned-evidence",
                strategy_artifact={"id": "strategy-golden"},
                portfolio_artifact={"id": "portfolio-golden"},
                deployment_id="paper-deployment-newer",
                runtime_version="paper-trader-runtime-v99",
                ended_at="2030-01-01T00:00:00+00:00",
                status="submitted",
                order_count=999,
                filled_count=999,
            )
            store.save_paper(newer)
            result = self._load(root, evidence, self._deployment(evidence))
        self.assertTrue(result["ready"], result["issues"])
        self.assertEqual("paper-evidence-golden", result["evidenceId"])

    def test_missing_deployment_pins_fail_closed(self) -> None:
        evidence = golden_paper_live_evidence()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = self._load(
                root,
                evidence,
                self._deployment(evidence, pins=False),
            )
        self.assertFalse(result["ready"])
        self.assertIn("paper-live-deployment-pin-missing", result["issues"][0])

    def test_paper_environment_deployment_is_not_live_authority(self) -> None:
        evidence = golden_paper_live_evidence()
        deployment = self._deployment(evidence)
        deployment["environment"] = "PAPER"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = self._load(root, evidence, deployment)
        self.assertFalse(result["ready"])
        self.assertEqual(
            ["paper-live-live-environment-deployment-required"],
            result["issues"],
        )

    def test_artifact_permissions_never_fallback_for_live_authority(self) -> None:
        evidence = golden_paper_live_evidence()
        deployment = self._deployment(evidence)
        artifact_permissions = dict(deployment.pop("permissions"))
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = load_pinned_paper_live_qualification(
                root,
                {
                    "id": evidence["strategyArtifact"]["artifactId"],
                    "permissions": artifact_permissions,
                },
                deployment,
                strategy_reference=dict(evidence["strategyArtifact"]),
                portfolio_reference=dict(evidence["portfolioArtifact"]),
            )
        self.assertFalse(result["ready"])
        self.assertEqual(
            ["paper-live-deployment-permissions-required"],
            result["issues"],
        )

    def test_caller_permission_override_cannot_replace_deployment_permissions(self) -> None:
        evidence = golden_paper_live_evidence()
        deployment = self._deployment(evidence)
        caller_permissions = dict(deployment["permissions"])
        deployment["permissions"] = {}
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = load_pinned_paper_live_qualification(
                root,
                {"id": evidence["strategyArtifact"]["artifactId"]},
                deployment,
                permissions=caller_permissions,
                strategy_reference=dict(evidence["strategyArtifact"]),
                portfolio_reference=dict(evidence["portfolioArtifact"]),
            )
        self.assertFalse(result["ready"])
        self.assertIn("paper-live-deployment-pin-missing", result["issues"][0])

    def test_outer_envelope_change_cannot_reuse_old_deployment_pins(self) -> None:
        evidence = golden_paper_live_evidence()
        deployment = self._deployment(evidence)
        changed = deepcopy(evidence)
        changed["runtimeVersion"] = "paper-trader-runtime-v3-repacked"
        changed["integrity"]["contentHash"] = stable_sha256(
            {key: value for key, value in changed.items() if key != "integrity"}
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(changed)
            result = self._load(root, changed, deployment)
        self.assertFalse(result["ready"])
        self.assertIn("paper-live-pinned-evidence-hash-mismatch", result["issues"])

    def test_governance_and_strategy_instance_pins_are_mandatory(self) -> None:
        evidence = golden_paper_live_evidence(portfolio=False)
        for missing_key in (
            "paperGovernanceDeploymentId",
            "paperStrategyInstanceId",
            "paperEvidenceBundleHash",
        ):
            deployment = self._deployment(evidence)
            deployment["permissions"].pop(missing_key)
            with tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                EvidenceStore(root).save_paper(evidence)
                result = self._load(root, evidence, deployment)
            self.assertFalse(result["ready"], missing_key)
            self.assertIn(missing_key, result["issues"][0])

    def test_portfolio_requires_exact_instance_and_standalone_rejects_one(self) -> None:
        portfolio_evidence = golden_paper_live_evidence()
        portfolio_deployment = self._deployment(portfolio_evidence)
        portfolio_deployment["permissions"].pop("paperPortfolioInstanceId")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(portfolio_evidence)
            portfolio_result = self._load(
                root,
                portfolio_evidence,
                portfolio_deployment,
            )
        self.assertFalse(portfolio_result["ready"])
        self.assertIn(
            "paperPortfolioInstanceId",
            portfolio_result["issues"][0],
        )

        standalone_evidence = golden_paper_live_evidence(portfolio=False)
        standalone_deployment = self._deployment(standalone_evidence)
        standalone_deployment["permissions"]["paperPortfolioInstanceId"] = (
            "unexpected-instance"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(standalone_evidence)
            standalone_result = self._load(
                root,
                standalone_evidence,
                standalone_deployment,
            )
        self.assertFalse(standalone_result["ready"])
        self.assertEqual(
            ["paper-live-standalone-portfolio-pin-not-empty"],
            standalone_result["issues"],
        )

    def test_caller_references_cannot_override_live_deployment_scope(self) -> None:
        evidence = golden_paper_live_evidence()
        deployment = self._deployment(evidence)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            strategy_result = load_pinned_paper_live_qualification(
                root,
                {"id": evidence["strategyArtifact"]["artifactId"]},
                deployment,
                strategy_reference={
                    **evidence["strategyArtifact"],
                    "artifactHash": "f" * 64,
                },
                portfolio_reference=dict(evidence["portfolioArtifact"]),
            )
            portfolio_result = load_pinned_paper_live_qualification(
                root,
                {"id": evidence["strategyArtifact"]["artifactId"]},
                deployment,
                strategy_reference=dict(evidence["strategyArtifact"]),
                portfolio_reference={
                    **evidence["portfolioArtifact"],
                    "artifactHash": "e" * 64,
                },
            )
        self.assertFalse(strategy_result["ready"])
        self.assertEqual(
            ["paper-live-current-deployment-strategy-artifactHash-mismatch"],
            strategy_result["issues"],
        )
        self.assertFalse(portfolio_result["ready"])
        self.assertEqual(
            ["paper-live-current-deployment-portfolio-artifactHash-mismatch"],
            portfolio_result["issues"],
        )

    def test_tampered_pinned_final_seal_is_not_replaced_by_another_record(self) -> None:
        evidence = golden_paper_live_evidence()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            record = EvidenceStore(root).save_paper(evidence)
            tampered = deepcopy(evidence)
            tampered["details"]["paperQualification"]["finalReconciliation"][
                "reconciliation"
            ]["positionFlat"] = False
            tampered["integrity"]["contentHash"] = stable_sha256(
                {key: value for key, value in tampered.items() if key != "integrity"}
            )
            record.path.write_text(
                json.dumps(tampered, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            result = self._load(root, evidence, self._deployment(evidence))
        self.assertFalse(result["ready"])
        self.assertIn("paper-final-position-not-flat", result["issues"])

    def test_standalone_exact_binding_is_required_and_supported(self) -> None:
        evidence = golden_paper_live_evidence(portfolio=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            result = self._load(root, evidence, self._deployment(evidence))
        self.assertTrue(result["ready"], result["issues"])
        self.assertFalse(result["portfolioRequired"])

    def test_live_capable_resume_revalidates_only_the_pinned_binding(self) -> None:
        evidence, raw_artifact = production_paper_v2_evidence()
        evidence["details"]["lifecyclePolicy"] = {
            "action": "PROMOTE",
            "targetStage": "before-live-small",
            "inputs": {"reconciliation_mismatches": 0},
        }
        evidence["integrity"]["contentHash"] = stable_sha256(
            {key: value for key, value in evidence.items() if key != "integrity"}
        )
        deployment = self._deployment(evidence)
        normalized = {
            "paper_portfolio_evidence": {},
            "permissions": dict(deployment["permissions"]),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            EvidenceStore(root).save_paper(evidence)
            with (
                patch.object(
                    state,
                    "normalize_strategy_artifact",
                    return_value={
                        "capabilities": {
                            "finalTestPassed": True,
                            "blockingFailReasons": [],
                        },
                        "revalidation": {
                            "expired": False,
                            "lastRevalidatedAt": "",
                        },
                        "portfolio_candidate": {"required": False},
                        "lineage": {"blockingIssues": []},
                    },
                ),
                patch.object(
                    state,
                    "portfolio_gate_for_strategy",
                    return_value={"active": False, "allowed": True},
                ),
                patch.object(
                    state,
                    "paper_portfolio_evidence_gate_for_strategy",
                    return_value={"required": False, "ready": True},
                ),
            ):
                result = state.paper_live_forward_resume_assessment(
                    root,
                    raw_artifact,
                    normalized,
                    deployment,
                    permissions=dict(deployment["permissions"]),
                )
        self.assertTrue(result["ready"], result["blockers"])
        self.assertEqual(
            deployment["permissions"]["paperFinalBindingHash"],
            result["bindingHash"],
        )


if __name__ == "__main__":
    unittest.main()
