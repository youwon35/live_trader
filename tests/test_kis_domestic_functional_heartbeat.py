from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from live_trader.kis_domestic_functional_heartbeat import (
    ACTIVE_SECONDS,
    DEFAULT_MAX_GAP_SECONDS,
    KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_MUTATION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_NETWORK_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_PRODUCTION_AVAILABLE,
    KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_RELEASE_AVAILABLE,
    SCHEMA_FINGERPRINT,
    DurableKisDomesticFunctionalHeartbeat,
    KisDomesticFunctionalHeartbeatBlocked,
    KisDomesticFunctionalHeartbeatVerifier,
    heartbeat_component_status,
)


KEY = b"kis-heartbeat-server-authority-key-value-48-bytes!!"
KEY_ID = "test-kis-heartbeat-authority-v1"
SESSION_ID = "kis-session-0123456789abcdef0123456789abcdef"
PROCESS_1 = "kis-process-generation-11111111111111111111111111111111"
PROCESS_2 = "kis-process-generation-22222222222222222222222222222222"
SOCKET_1 = "kis-ws-generation-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SOCKET_2 = "kis-ws-generation-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ACTIVATED = datetime(2026, 8, 14, 4, 15, tzinfo=timezone.utc)
_TEMPS: list[tempfile.TemporaryDirectory] = []


def tearDownModule() -> None:
    while _TEMPS:
        _TEMPS.pop().cleanup()


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sign(domain: str, body) -> str:
    return hmac.new(
        KEY,
        domain.encode("ascii") + b"\n" + _canonical(body),
        hashlib.sha256,
    ).hexdigest()


def _verify(domain: str, body, signature: str) -> bool:
    return type(signature) is str and hmac.compare_digest(
        _sign(domain, body), signature
    )


class _Clock:
    def __init__(self) -> None:
        self.wall = ACTIVATED
        self.monotonic_ns = 1_000_000_000_000

    def wall_now(self) -> datetime:
        return self.wall

    def monotonic_now(self) -> int:
        return self.monotonic_ns

    def advance(
        self,
        seconds: float,
        *,
        wall_seconds: float | None = None,
        monotonic_seconds: float | None = None,
    ) -> None:
        self.wall += timedelta(
            seconds=seconds if wall_seconds is None else wall_seconds
        )
        self.monotonic_ns += int(
            (seconds if monotonic_seconds is None else monotonic_seconds)
            * 1_000_000_000
        )


def _activation(*, session_id: str = SESSION_ID) -> dict:
    return {
        "schemaVersion": "kis-domestic-functional-activation/v1",
        "sessionId": session_id,
        "activatedAt": ACTIVATED.isoformat().replace("+00:00", "Z"),
        "expiresAt": (ACTIVATED + timedelta(seconds=ACTIVE_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "activeSeconds": ACTIVE_SECONDS,
        "activationRecordHash": "a" * 64,
    }


def _fixture(
    *,
    process_generation: str = PROCESS_1,
    max_gap_seconds: int = DEFAULT_MAX_GAP_SECONDS,
    clock: _Clock | None = None,
    path: Path | None = None,
    owner_token: bytes = b"heartbeat-owner-token-process-one-32-byte-value",
):
    clock = clock or _Clock()
    if path is None:
        temp = tempfile.TemporaryDirectory()
        _TEMPS.append(temp)
        path = Path(temp.name) / "heartbeat.sqlite3"
    journal = DurableKisDomesticFunctionalHeartbeat(
        path,
        capture_signer=_sign,
        capture_verifier=_verify,
        server_authority_key_id=KEY_ID,
        process_generation=process_generation,
        wall_clock=clock.wall_now,
        monotonic_clock=clock.monotonic_now,
        max_gap_seconds=max_gap_seconds,
        owner_token_factory=lambda: owner_token,
    )
    return journal, clock, path


def _verifier(path: Path, clock: _Clock) -> KisDomesticFunctionalHeartbeatVerifier:
    return KisDomesticFunctionalHeartbeatVerifier(
        path,
        capture_verifier=_verify,
        server_authority_key_id=KEY_ID,
        trusted_wall_clock=clock.wall_now,
    )


def _complete_7200(
    journal: DurableKisDomesticFunctionalHeartbeat,
    clock: _Clock,
) -> dict:
    journal.start(activation=_activation(), socket_generation=SOCKET_1)
    for _ in range(719):
        clock.advance(10)
        journal.observe(session_id=SESSION_ID, socket_generation=SOCKET_1)
    clock.advance(10)
    return journal.complete(session_id=SESSION_ID)


class KisDomesticFunctionalHeartbeatTest(unittest.TestCase):
    def test_component_is_offline_nonrelease_and_schema_is_pinned(self) -> None:
        status = heartbeat_component_status()
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_PRODUCTION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_NETWORK_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_MUTATION_AVAILABLE)
        self.assertFalse(KIS_DOMESTIC_FUNCTIONAL_HEARTBEAT_RELEASE_AVAILABLE)
        self.assertFalse(status["productionAvailable"])
        self.assertFalse(status["networkAvailable"])
        self.assertFalse(status["mutationAvailable"])
        self.assertFalse(status["releaseAvailable"])
        self.assertTrue(status["durableMonotonicJournalAvailable"])
        self.assertRegex(SCHEMA_FINGERPRINT, r"^[0-9a-f]{64}$")

    def test_independent_consumer_proves_exact_7200_without_promoting(self) -> None:
        journal, clock, path = _fixture()
        terminal = _complete_7200(journal, clock)
        self.assertEqual("OBSERVATION_COMPLETE", terminal["state"])
        evidence = _verifier(path, clock).verify(
            expected_activation=_activation(),
            expected_socket_generation=SOCKET_1,
        )
        self.assertTrue(evidence["uninterrupted"])
        self.assertTrue(evidence["exact7200ObservationPassed"])
        self.assertEqual("7200", evidence["actualMonotonicElapsedSeconds"])
        self.assertEqual("10", evidence["maxHeartbeatGapSeconds"])
        self.assertEqual(721, evidence["sampleCount"])
        self.assertEqual(
            "ELIGIBLE_FOR_INDEPENDENT_WIRING_VERIFICATION",
            evidence["outcome"],
        )
        self.assertFalse(evidence["functionalTestPassed"])
        self.assertFalse(evidence["promotionEligible"])
        self.assertFalse(evidence["releaseAvailable"])

    def test_early_complete_is_blocked_and_stop_is_safe_incomplete(self) -> None:
        journal, clock, path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        for _ in range(719):
            clock.advance(10)
            journal.observe(session_id=SESSION_ID, socket_generation=SOCKET_1)
        clock.advance(9)
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "before-active-end",
        ):
            journal.complete(session_id=SESSION_ID)
        stopped = journal.record_control(session_id=SESSION_ID, control="STOP")
        self.assertEqual("SAFE_INCOMPLETE_EARLY_STOP", stopped["state"])
        evidence = _verifier(path, clock).verify(
            expected_activation=_activation(),
            expected_socket_generation=SOCKET_1,
        )
        self.assertFalse(evidence["exact7200ObservationPassed"])
        self.assertFalse(evidence["uninterrupted"])
        self.assertEqual("SAFE_INCOMPLETE_EARLY_STOP", evidence["outcome"])

    def test_kill_is_always_safe_incomplete(self) -> None:
        journal, clock, path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        clock.advance(5)
        killed = journal.record_control(session_id=SESSION_ID, control="KILL")
        self.assertEqual("SAFE_INCOMPLETE_KILL", killed["state"])
        evidence = _verifier(path, clock).verify(
            expected_activation=_activation(),
            expected_socket_generation=SOCKET_1,
        )
        self.assertFalse(evidence["exact7200ObservationPassed"])
        self.assertEqual("SAFE_INCOMPLETE_KILL", evidence["outcome"])

    def test_gap_rollback_forward_jump_and_socket_change_terminalize(self) -> None:
        cases = (
            (
                lambda clock: clock.advance(11),
                SOCKET_1,
                "SAFE_INCOMPLETE_HEARTBEAT_GAP",
            ),
            (
                lambda clock: clock.advance(5, wall_seconds=1),
                SOCKET_1,
                "SAFE_INCOMPLETE_CLOCK_ROLLBACK",
            ),
            (
                lambda clock: clock.advance(5, wall_seconds=20),
                SOCKET_1,
                "SAFE_INCOMPLETE_CLOCK_FORWARD_JUMP",
            ),
            (
                lambda clock: clock.advance(5),
                SOCKET_2,
                "SAFE_INCOMPLETE_SOCKET_GENERATION_CHANGED",
            ),
            (
                lambda clock: clock.advance(1, monotonic_seconds=-1),
                SOCKET_1,
                "SAFE_INCOMPLETE_MONOTONIC_REGRESSION",
            ),
        )
        for advance, observed_socket, expected in cases:
            with self.subTest(expected=expected):
                journal, clock, path = _fixture()
                journal.start(activation=_activation(), socket_generation=SOCKET_1)
                advance(clock)
                terminal = journal.observe(
                    session_id=SESSION_ID,
                    socket_generation=observed_socket,
                )
                self.assertEqual(expected, terminal["state"])
                evidence = _verifier(path, clock).verify(
                    expected_activation=_activation(),
                    expected_socket_generation=SOCKET_1,
                )
                self.assertFalse(evidence["uninterrupted"])
                self.assertFalse(evidence["exact7200ObservationPassed"])
                self.assertEqual(expected, evidence["outcome"])

    def test_new_process_owner_terminalizes_active_observation(self) -> None:
        journal, clock, path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        clock.advance(5)
        successor, _same_clock, _path = _fixture(
            process_generation=PROCESS_2,
            clock=clock,
            path=path,
            owner_token=b"heartbeat-owner-token-process-two-32-byte-value",
        )
        self.assertEqual(
            (SESSION_ID,), successor.startup_terminalized_session_ids
        )
        self.assertEqual((), successor.audit_restart())
        snapshot = successor.snapshot(SESSION_ID)
        self.assertEqual("SAFE_INCOMPLETE_PROCESS_RESTART", snapshot["state"])
        evidence = _verifier(path, clock).verify(
            expected_activation=_activation(),
            expected_socket_generation=SOCKET_1,
        )
        self.assertTrue(evidence["processRestartDetected"])
        self.assertFalse(evidence["uninterrupted"])

    def test_exact_active_half_open_window_rejects_heartbeat_at_end(self) -> None:
        journal, clock, _path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        clock.advance(ACTIVE_SECONDS)
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "half-open-window",
        ):
            journal.observe(session_id=SESSION_ID, socket_generation=SOCKET_1)

    def test_trusted_now_prevents_future_dated_immediate_pass(self) -> None:
        journal, producer_clock, path = _fixture()
        _complete_7200(journal, producer_clock)
        verifier_clock = _Clock()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "future-dated",
        ):
            _verifier(path, verifier_clock).verify(
                expected_activation=_activation(),
                expected_socket_generation=SOCKET_1,
            )

    def test_activation_exact_7200_and_atomic_start_are_required(self) -> None:
        journal, clock, _path = _fixture()
        bad = _activation()
        bad["expiresAt"] = (ACTIVATED + timedelta(seconds=7199)).isoformat().replace(
            "+00:00", "Z"
        )
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "not-exact-7200",
        ):
            journal.start(activation=bad, socket_generation=SOCKET_1)
        clock.advance(0.001)
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "not-atomic",
        ):
            journal.start(activation=_activation(), socket_generation=SOCKET_1)

    def test_constructor_requires_exact_finite_code_policy(self) -> None:
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "max-gap-policy-mismatch",
        ):
            _fixture(max_gap_seconds=11)
        temp = tempfile.TemporaryDirectory()
        _TEMPS.append(temp)
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "clock-divergence-policy-mismatch",
        ):
            DurableKisDomesticFunctionalHeartbeat(
                Path(temp.name) / "heartbeat.sqlite3",
                capture_signer=_sign,
                capture_verifier=_verify,
                server_authority_key_id=KEY_ID,
                process_generation=PROCESS_1,
                wall_clock=_Clock().wall_now,
                monotonic_clock=_Clock().monotonic_now,
                clock_divergence_seconds=Decimal("NaN"),
            )

    def test_schema_extra_table_column_and_fingerprint_are_fail_closed(self) -> None:
        journal, _clock, path = _fixture()
        del journal
        conn = sqlite3.connect(path)
        try:
            conn.execute("CREATE TABLE hostile_extra(value TEXT)")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "schema-dirty",
        ):
            _fixture(path=path)

        temp = tempfile.TemporaryDirectory()
        _TEMPS.append(temp)
        clean_path = Path(temp.name) / "heartbeat.sqlite3"
        journal, _clock, _ = _fixture(path=clean_path)
        del journal
        conn = sqlite3.connect(clean_path)
        try:
            conn.execute(
                "UPDATE kis_functional_heartbeat_meta SET schema_fingerprint=?",
                ("0" * 64,),
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "fingerprint-mismatch",
        ):
            _fixture(path=clean_path)

        column_temp = tempfile.TemporaryDirectory()
        _TEMPS.append(column_temp)
        column_path = Path(column_temp.name) / "heartbeat.sqlite3"
        journal, _clock, _ = _fixture(path=column_path)
        del journal
        conn = sqlite3.connect(column_path)
        try:
            conn.execute(
                "ALTER TABLE kis_functional_heartbeat_sample "
                "ADD COLUMN attacker_claim TEXT"
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "schema-dirty",
        ):
            _fixture(path=column_path)

        definition_temp = tempfile.TemporaryDirectory()
        _TEMPS.append(definition_temp)
        definition_path = Path(definition_temp.name) / "heartbeat.sqlite3"
        journal, _clock, _ = _fixture(path=definition_path)
        del journal
        conn = sqlite3.connect(definition_path)
        try:
            conn.execute("PRAGMA writable_schema=ON")
            conn.execute(
                """UPDATE sqlite_master
                   SET sql=REPLACE(sql, ' CHECK (singleton=1)', '')
                   WHERE name='kis_functional_heartbeat_meta'"""
            )
            conn.execute("PRAGMA writable_schema=OFF")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "definition-schema-dirty",
        ):
            _fixture(path=definition_path)

    def test_sample_json_rehash_without_key_is_rejected_by_consumer(self) -> None:
        journal, clock, path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        clock.advance(5)
        journal.record_control(session_id=SESSION_ID, control="STOP")
        conn = sqlite3.connect(path)
        try:
            row = conn.execute(
                "SELECT record_json FROM kis_functional_heartbeat_sample "
                "WHERE sequence=1"
            ).fetchone()
            body = json.loads(row[0])
            body["wallAt"] = (ACTIVATED + timedelta(seconds=1)).isoformat().replace(
                "+00:00", "Z"
            )
            conn.execute(
                "UPDATE kis_functional_heartbeat_sample "
                "SET wall_at=?, record_json=?, record_hash=? WHERE sequence=1",
                (body["wallAt"], _canonical(body).decode("utf-8"), hashlib.sha256(_canonical(body)).hexdigest()),
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "signature-mismatch",
        ):
            _verifier(path, clock).verify(
                expected_activation=_activation(),
                expected_socket_generation=SOCKET_1,
            )

    def test_signed_unexpected_intermediate_terminal_kind_is_rejected(self) -> None:
        journal, clock, path = _fixture()
        journal.start(activation=_activation(), socket_generation=SOCKET_1)
        clock.advance(5)
        journal.observe(session_id=SESSION_ID, socket_generation=SOCKET_1)
        clock.advance(1)
        journal.record_control(session_id=SESSION_ID, control="STOP")

        conn = sqlite3.connect(path)
        try:
            middle = json.loads(
                conn.execute(
                    "SELECT record_json FROM kis_functional_heartbeat_sample "
                    "WHERE session_id=? AND sequence=2",
                    (SESSION_ID,),
                ).fetchone()[0]
            )
            middle["kind"] = "KILL"
            middle_json = _canonical(middle).decode("utf-8")
            middle_hash = hashlib.sha256(_canonical(middle)).hexdigest()
            conn.execute(
                "UPDATE kis_functional_heartbeat_sample "
                "SET kind=?, record_json=?, record_hash=?, signature=? "
                "WHERE session_id=? AND sequence=2",
                (
                    "KILL",
                    middle_json,
                    middle_hash,
                    _sign("HEARTBEAT_SAMPLE", middle),
                    SESSION_ID,
                ),
            )

            terminal = json.loads(
                conn.execute(
                    "SELECT record_json FROM kis_functional_heartbeat_sample "
                    "WHERE session_id=? AND sequence=3",
                    (SESSION_ID,),
                ).fetchone()[0]
            )
            terminal["previousHash"] = middle_hash
            terminal_json = _canonical(terminal).decode("utf-8")
            terminal_hash = hashlib.sha256(_canonical(terminal)).hexdigest()
            conn.execute(
                "UPDATE kis_functional_heartbeat_sample "
                "SET previous_hash=?, record_json=?, record_hash=?, signature=? "
                "WHERE session_id=? AND sequence=3",
                (
                    middle_hash,
                    terminal_json,
                    terminal_hash,
                    _sign("HEARTBEAT_SAMPLE", terminal),
                    SESSION_ID,
                ),
            )
            conn.execute(
                "UPDATE kis_functional_heartbeat_session "
                "SET sample_head_hash=? WHERE session_id=?",
                (terminal_hash, SESSION_ID),
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(
            KisDomesticFunctionalHeartbeatBlocked,
            "terminal-state-sample-mismatch",
        ):
            _verifier(path, clock).verify(
                expected_activation=_activation(),
                expected_socket_generation=SOCKET_1,
            )


if __name__ == "__main__":
    unittest.main()
