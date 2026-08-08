from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from live_trader.operational_governance import OperationalGovernanceStore


NOW = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class OperationalGovernanceStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database_path = Path(self.temporary_directory.name) / "governance.sqlite3"
        self.store = OperationalGovernanceStore(
            self.database_path,
            clock=lambda: NOW,
        )

    def create_manifest(
        self,
        *,
        deployment_id: str = "primary-live",
        config_revision: int = 1,
        config_label: str = "config-v1",
        created_at: datetime = NOW,
        metadata: dict[str, object] | None = None,
    ):
        return self.store.create_deployment_manifest(
            deployment_id=deployment_id,
            strategy_artifact_hash=digest("strategy-v1"),
            portfolio_artifact_hash=digest("portfolio-v1"),
            account_fingerprint=digest("caller-derived-account-fingerprint"),
            broker_route="KIS_PRODUCTION",
            runtime_version="3.14.0",
            build_hash=digest("build-v1"),
            execution_adapter="kis-rest",
            execution_adapter_version="2.4.1",
            risk_policy_revision=3,
            risk_policy_hash=digest("risk-policy-v3"),
            config_revision=config_revision,
            config_hash=digest(config_label),
            preflight_ttl_seconds=300,
            metadata=metadata or {"releaseChannel": "stable"},
            created_at=created_at,
        )

    def create_pass_preflight(
        self,
        manifest,
        *,
        issued_at: datetime = NOW,
        ttl_seconds: int = 60,
    ):
        return self.store.create_preflight_snapshot(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            checks=(
                {
                    "checkId": "broker-session",
                    "status": "PASS",
                    "detail": "broker session is authenticated",
                    "evidenceHash": digest("broker-session-evidence"),
                },
                {
                    "checkId": "position-reconciliation",
                    "status": "PASS",
                    "detail": "broker and local positions match",
                    "evidenceHash": digest("reconciliation-evidence"),
                },
            ),
            reconciliation_hash=digest("reconciliation-snapshot"),
            broker_snapshot_hash=digest("broker-snapshot"),
            ttl_seconds=ttl_seconds,
            issued_at=issued_at,
        )

    def create_running_live_session(self, manifest, snapshot):
        session = self.store.create_runtime_session(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            preflight_snapshot_id=snapshot.snapshot_id,
            profile="production",
            mode="SMALL_LIVE",
            runtime_instance_id="worker-01",
            actor="operator",
            occurred_at=NOW,
        )
        session = self.store.transition_runtime_session(
            session.session_id,
            "STARTING",
            actor="operator",
            occurred_at=NOW + timedelta(seconds=1),
        )
        return self.store.transition_runtime_session(
            session.session_id,
            "RUNNING",
            actor="runtime-supervisor",
            occurred_at=NOW + timedelta(seconds=2),
        )

    def test_manifest_revisions_are_hash_chained_and_immutable(self) -> None:
        first = self.create_manifest()
        second = self.create_manifest(
            config_revision=2,
            config_label="config-v2",
            created_at=NOW + timedelta(seconds=1),
        )

        self.assertEqual(first.revision, 1)
        self.assertEqual(second.revision, 2)
        self.assertEqual(second.previous_manifest_hash, first.manifest_hash)
        self.assertEqual(
            self.store.get_deployment_manifest("primary-live", 1),
            first,
        )
        self.assertEqual(
            self.store.get_deployment_manifest_by_hash(second.manifest_hash),
            second,
        )

        connection = sqlite3.connect(self.database_path)
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            connection.execute(
                "UPDATE live_deployment_manifests SET config_revision = 99 "
                "WHERE manifest_hash = ?",
                (first.manifest_hash,),
            )
        connection.rollback()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
            connection.execute(
                "DELETE FROM live_deployment_manifests WHERE manifest_hash = ?",
                (first.manifest_hash,),
            )

    def test_raw_account_identifier_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            self.store.create_deployment_manifest(
                deployment_id="unsafe-live",
                strategy_artifact_hash=digest("strategy"),
                account_fingerprint="12345678-01",
                broker_route="KIS_PRODUCTION",
                runtime_version="3.14.0",
                build_hash=digest("build"),
                execution_adapter="kis-rest",
                execution_adapter_version="1",
                risk_policy_revision=1,
                risk_policy_hash=digest("risk"),
                config_revision=1,
                config_hash=digest("config"),
                created_at=NOW,
            )

        with self.assertRaisesRegex(ValueError, "raw account identifier"):
            self.create_manifest(metadata={"credentials": {"accountNo": "12345678"}})

        manifest = self.create_manifest()
        self.assertEqual(manifest.account_fingerprint, digest("caller-derived-account-fingerprint"))
        self.assertNotIn(b"12345678", self.database_path.read_bytes())

    def test_preflight_status_and_expiry_are_fail_closed(self) -> None:
        manifest = self.create_manifest()
        snapshot = self.create_pass_preflight(manifest, ttl_seconds=60)

        before_expiry = self.store.preflight_validity(
            snapshot.snapshot_id,
            at=NOW + timedelta(seconds=59),
        )
        at_expiry = self.store.preflight_validity(
            snapshot.snapshot_id,
            at=NOW + timedelta(seconds=60),
        )
        self.assertTrue(before_expiry["valid"])
        self.assertFalse(at_expiry["valid"])
        self.assertIn("preflight-expired", at_expiry["reasons"])

        review = self.store.create_preflight_snapshot(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            checks=({"checkId": "clock-skew", "status": "WARN"},),
            reconciliation_hash=digest("review-reconciliation"),
            broker_snapshot_hash=digest("review-broker"),
            issued_at=NOW,
        )
        self.assertEqual(review.status, "REVIEW_REQUIRED")
        with self.assertRaisesRegex(ValueError, "not valid"):
            self.store.create_runtime_session(
                deployment_id=manifest.deployment_id,
                deployment_manifest_hash=manifest.manifest_hash,
                preflight_snapshot_id=review.snapshot_id,
                profile="production",
                mode="FULL_LIVE",
                runtime_instance_id="worker-review",
                occurred_at=NOW,
            )

    def test_live_runtime_session_is_bound_to_preflight_and_hash_chain(self) -> None:
        manifest = self.create_manifest()
        snapshot = self.create_pass_preflight(manifest)
        session = self.create_running_live_session(manifest, snapshot)

        self.assertEqual(session.lifecycle, "RUNNING")
        self.assertEqual(session.deployment_manifest_hash, manifest.manifest_hash)
        self.assertEqual(session.preflight_snapshot_hash, snapshot.snapshot_hash)
        heartbeat = self.store.append_runtime_event(
            session.session_id,
            event_type="HEARTBEAT",
            actor="runtime-supervisor",
            payload={"healthy": True},
            occurred_at=NOW + timedelta(seconds=3),
        )
        self.assertEqual(heartbeat.lifecycle, "RUNNING")

        events = self.store.runtime_events(session.session_id)
        self.assertEqual([event.sequence for event in events], [1, 2, 3, 4])
        self.assertEqual(events[0].previous_hash, "")
        self.assertEqual(events[-1].previous_hash, events[-2].event_hash)
        with self.assertRaisesRegex(ValueError, "invalid runtime transition"):
            self.store.transition_runtime_session(
                session.session_id,
                "PREFLIGHT",
                actor="operator",
                occurred_at=NOW + timedelta(seconds=4),
            )

        connection = sqlite3.connect(self.database_path)
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "UPDATE live_runtime_events SET actor = 'tampered' WHERE session_id = ?",
                (session.session_id,),
            )

        integrity = self.store.verify_integrity()
        self.assertTrue(integrity["ok"], integrity["issues"])
        self.assertEqual(integrity["counts"]["runtimeEvents"], 4)

    def test_expired_preflight_does_not_leave_partial_session(self) -> None:
        manifest = self.create_manifest()
        snapshot = self.create_pass_preflight(manifest, ttl_seconds=30)

        with self.assertRaisesRegex(ValueError, "preflight-expired"):
            self.store.create_runtime_session(
                deployment_id=manifest.deployment_id,
                deployment_manifest_hash=manifest.manifest_hash,
                preflight_snapshot_id=snapshot.snapshot_id,
                profile="production",
                mode="SMALL_LIVE",
                runtime_instance_id="late-worker",
                occurred_at=NOW + timedelta(seconds=30),
            )

        connection = sqlite3.connect(self.database_path)
        try:
            count = connection.execute(
                "SELECT COUNT(*) FROM live_runtime_sessions"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 0)

    def test_running_session_rebinds_to_fresh_exact_scope_preflight(self) -> None:
        manifest = self.create_manifest()
        initial = self.create_pass_preflight(manifest, ttl_seconds=60)
        session = self.create_running_live_session(manifest, initial)
        renewed = self.create_pass_preflight(
            manifest,
            issued_at=NOW + timedelta(seconds=55),
            ttl_seconds=60,
        )

        rebound = self.store.rebind_runtime_preflight(
            session.session_id,
            renewed.snapshot_id,
            actor="functional-test-supervisor",
            occurred_at=NOW + timedelta(seconds=55),
        )
        authorization = self.store.runtime_authorization(
            session.session_id,
            at=NOW + timedelta(seconds=70),
        )

        self.assertEqual(renewed.snapshot_id, rebound.preflight_snapshot_id)
        self.assertEqual(renewed.snapshot_hash, rebound.preflight_snapshot_hash)
        self.assertTrue(authorization["allowed"], authorization["reasons"])
        events = self.store.runtime_events(session.session_id)
        self.assertEqual("PREFLIGHT_REBOUND", events[-1].event_type)
        self.assertEqual(
            initial.snapshot_hash,
            events[-1].payload["previousPreflightSnapshotHash"],
        )

    def test_runtime_preflight_rebind_rejects_expired_or_other_scope_snapshot(self) -> None:
        manifest = self.create_manifest()
        initial = self.create_pass_preflight(manifest, ttl_seconds=60)
        session = self.create_running_live_session(manifest, initial)
        expired = self.create_pass_preflight(
            manifest,
            issued_at=NOW + timedelta(seconds=10),
            ttl_seconds=30,
        )
        with self.assertRaisesRegex(ValueError, "preflight-expired"):
            self.store.rebind_runtime_preflight(
                session.session_id,
                expired.snapshot_id,
                actor="functional-test-supervisor",
                occurred_at=NOW + timedelta(seconds=40),
            )

        other_manifest = self.create_manifest(
            deployment_id="other-live",
            created_at=NOW + timedelta(seconds=20),
        )
        other = self.create_pass_preflight(
            other_manifest,
            issued_at=NOW + timedelta(seconds=20),
            ttl_seconds=60,
        )
        with self.assertRaisesRegex(ValueError, "outside the runtime session scope"):
            self.store.rebind_runtime_preflight(
                session.session_id,
                other.snapshot_id,
                actor="functional-test-supervisor",
                occurred_at=NOW + timedelta(seconds=25),
            )

    def test_new_manifest_revision_supersedes_existing_preflight(self) -> None:
        first = self.create_manifest()
        snapshot = self.create_pass_preflight(first)
        self.create_manifest(
            config_revision=2,
            config_label="config-v2",
            created_at=NOW + timedelta(seconds=1),
        )

        validity = self.store.preflight_validity(snapshot.snapshot_id, at=NOW + timedelta(seconds=2))
        self.assertFalse(validity["valid"])
        self.assertIn("deployment-manifest-superseded", validity["reasons"])
        with self.assertRaisesRegex(ValueError, "superseded"):
            self.store.create_runtime_session(
                deployment_id=first.deployment_id,
                deployment_manifest_hash=first.manifest_hash,
                preflight_snapshot_id=snapshot.snapshot_id,
                profile="production",
                mode="SMALL_LIVE",
                runtime_instance_id="superseded-worker",
                occurred_at=NOW + timedelta(seconds=2),
            )

    def test_critical_incident_dedupes_and_blocks_live_authorization(self) -> None:
        manifest = self.create_manifest()
        snapshot = self.create_pass_preflight(manifest, ttl_seconds=300)
        session = self.create_running_live_session(manifest, snapshot)

        initial_authorization = self.store.runtime_authorization(
            session.session_id,
            at=NOW + timedelta(seconds=3),
        )
        self.assertTrue(initial_authorization["allowed"], initial_authorization["reasons"])

        incident, created = self.store.open_incident(
            code="ORDER_STATE_DIVERGENCE",
            scope_type="SESSION",
            scope_id=session.session_id,
            severity="CRITICAL",
            actor="oms-watchdog",
            deployment_id=manifest.deployment_id,
            session_id=session.session_id,
            account_fingerprint=manifest.account_fingerprint,
            evidence_hash=digest("incident-evidence-1"),
            occurred_at=NOW + timedelta(seconds=4),
        )
        duplicate, duplicate_created = self.store.open_incident(
            code="ORDER_STATE_DIVERGENCE",
            scope_type="SESSION",
            scope_id=session.session_id,
            severity="CRITICAL",
            actor="oms-watchdog",
            deployment_id=manifest.deployment_id,
            session_id=session.session_id,
            account_fingerprint=manifest.account_fingerprint,
            evidence_hash=digest("incident-evidence-2"),
            occurred_at=NOW + timedelta(seconds=5),
        )
        self.assertTrue(created)
        self.assertFalse(duplicate_created)
        self.assertEqual(duplicate.incident_id, incident.incident_id)
        self.assertEqual(len(self.store.incident_events(incident.incident_id)), 2)

        blocked = self.store.runtime_authorization(
            session.session_id,
            at=NOW + timedelta(seconds=6),
        )
        self.assertFalse(blocked["allowed"])
        self.assertIn("critical-incidents:1", blocked["reasons"])

        resolved = self.store.resolve_incident(
            incident.incident_id,
            actor="on-call-operator",
            evidence_hash=digest("resolution-evidence"),
            occurred_at=NOW + timedelta(seconds=7),
        )
        self.assertEqual(resolved.state, "RESOLVED")
        unblocked = self.store.runtime_authorization(
            session.session_id,
            at=NOW + timedelta(seconds=8),
        )
        self.assertTrue(unblocked["allowed"], unblocked["reasons"])

        connection = sqlite3.connect(self.database_path)
        self.addCleanup(connection.close)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "DELETE FROM live_incident_events WHERE incident_id = ?",
                (incident.incident_id,),
            )

        integrity = self.store.verify_integrity()
        self.assertTrue(integrity["ok"], integrity["issues"])
        self.assertEqual(integrity["counts"]["incidents"], 1)
        self.assertEqual(integrity["counts"]["incidentEvents"], 3)

    def test_monitor_session_does_not_require_preflight(self) -> None:
        manifest = self.create_manifest()
        session = self.store.create_runtime_session(
            deployment_id=manifest.deployment_id,
            deployment_manifest_hash=manifest.manifest_hash,
            profile="observation",
            mode="MONITOR",
            runtime_instance_id="monitor-01",
            occurred_at=NOW,
        )

        self.assertEqual(session.lifecycle, "PREFLIGHT")
        self.assertEqual(session.preflight_snapshot_id, "")
        self.store.transition_runtime_session(
            session.session_id,
            "STARTING",
            actor="operator",
            occurred_at=NOW + timedelta(seconds=1),
        )
        self.store.transition_runtime_session(
            session.session_id,
            "RUNNING",
            actor="runtime-supervisor",
            occurred_at=NOW + timedelta(seconds=2),
        )
        authorization = self.store.runtime_authorization(
            session.session_id,
            at=NOW + timedelta(seconds=3),
            require_fresh_preflight=False,
        )
        self.assertFalse(authorization["allowed"])
        self.assertIn("runtime-session-not-live", authorization["reasons"])


if __name__ == "__main__":
    unittest.main()
