"""Synthetic clock fixtures; no product imports, network, accounts or orders."""
from __future__ import annotations

import ast
from copy import deepcopy
import math
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Any, Mapping
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT if (ROOT / "short_live_runtime.py").exists() else ROOT / "live_trader"

# Frozen pre-fix behavior used only to demonstrate the cumulative-drift bug.
# Keeping this fixture here makes the regression runnable from the repository
# without any task directory or historical installation files.
LEGACY_FRESH_CLOCK_SOURCE = '''
class FreshClock:
    def __init__(self, reader, controls):
        self.reader, self.controls = reader, controls
        self.wall, self.mono = time.time(), time.monotonic()
        self.last_positive = None
        self.failure = None
    def __call__(self, manifest):
        blockers = self.controls()
        wall, mono = time.time(), time.monotonic()
        if abs((wall-self.wall)-(mono-self.mono)) > .25:
            self.failure = "WALL_MONOTONIC_CONTINUITY_FAILED"
        if self.failure:
            blockers.append(self.failure)
        if not blockers and (self.last_positive is None or mono-self.last_positive > 10):
            try:
                started = time.time()
                tick = time.monotonic()
                body, _ = self.reader._get("binance", "/api/v3/time")
                received = time.time()
                elapsed = time.monotonic()-tick
                ms = body.get("serverTime") if isinstance(body, dict) else None
                if type(ms) is not int or elapsed > 2 or abs((received-started)-elapsed) > .25:
                    raise ValueError("CLOCK_RESPONSE_UNBOUNDED")
                if max(abs(ms/1000-started), abs(ms/1000-received)) > 2:
                    raise ValueError("CLOCK_OFFSET_EXCEEDED")
                self.last_positive = time.monotonic()
            except Exception:
                self.failure = "FRESH_EXCHANGE_CLOCK_UNVERIFIED"
                blockers.append(self.failure)
        return {"ready": not blockers, "blockers": list(dict.fromkeys(blockers))}
'''


class FakeTime:
    def __init__(self, ppm=0):
        self.wall, self.mono, self.server = 1_800_000_000.0, 100.0, 1_800_000_000.0
        self.ppm = ppm

    def advance(self, seconds):
        self.wall += seconds * (1 + self.ppm / 1_000_000)
        self.server += seconds
        self.mono += seconds

    def time(self):
        return self.wall

    def monotonic(self):
        return self.mono


def clock_class(clock, *, original=False):
    source = SOURCE_ROOT / "short_live_runtime.py"
    tree = ast.parse(LEGACY_FRESH_CLOCK_SOURCE if original else source.read_text(encoding="utf-8-sig"))
    cls = next(row for row in tree.body if isinstance(row, ast.ClassDef) and row.name == "FreshClock")
    namespace = {"time": clock, "threading": threading, "math": math}
    exec(compile(ast.Module(body=[cls], type_ignores=[]), str(source), "exec"), namespace)
    return namespace["FreshClock"]


class Reader:
    def __init__(self, clock):
        self.clock, self.calls = clock, 0
        self.delay, self.jump, self.mono_jump = .05, 0, 0
        self.offset, self.error, self.body = 0, None, None

    def _get(self, provider, path):
        assert (provider, path) == ("binance", "/api/v3/time")
        self.calls += 1
        self.clock.advance(self.delay)
        self.clock.wall += self.jump
        self.clock.mono += self.mono_jump
        if self.error:
            raise self.error
        return (self.body if self.body is not None else {
            "serverTime": int((self.clock.server + self.offset) * 1000)
        }), {}


class FreshClockContinuityTests(unittest.TestCase):
    def fixture(self, *, ppm=0, original=False):
        clock = FakeTime(ppm)
        reader = Reader(clock)
        controls = []
        guard = clock_class(clock, original=original)(reader, lambda: controls)
        return clock, reader, controls, guard

    def test_six_hours_of_measured_slew_old_fails_new_passes(self):
        # The observed ~0.9994s / 21660s is roughly 46.14ppm. These are
        # simulated clock readings, not an additional real six-hour run.
        outcomes = []
        for original in (True, False):
            clock, reader, _, guard = self.fixture(ppm=46.14, original=original)
            began = clock.mono
            first_failure = None
            while clock.mono - began <= 21660:
                result = guard({})
                if not result["ready"]:
                    first_failure = clock.mono - began
                    break
                clock.advance(1)
            outcomes.append(first_failure)
            if not original:
                self.assertGreater(reader.calls, 1900)
                self.assertLess(reader.calls, 2200)
                self.assertLess(abs(clock.wall-clock.server), 1.1)
        self.assertIsNotNone(outcomes[0])
        self.assertGreater(outcomes[0], 5000)
        self.assertLess(outcomes[0], 6000)
        self.assertIsNone(outcomes[1])

    def test_catalog_lifetime_is_not_first_proof_lifetime(self):
        clock, reader, _, guard = self.fixture(ppm=46.14)
        clock.advance(21660)
        self.assertIsNone(guard.wall)
        self.assertTrue(guard({})["ready"])
        self.assertEqual(reader.calls, 1)

    def test_idle_new_preparation_uses_independent_proof(self):
        clock, reader, _, guard = self.fixture(ppm=46.14)
        self.assertTrue(guard.verify_preparation({})["ready"])
        clock.advance(21660)
        self.assertTrue(guard.verify_preparation({})["ready"])
        self.assertEqual(reader.calls, 2)
        self.assertEqual(guard.wall, clock.wall)

    def test_active_observation_gap_does_not_rebase_without_verification(self):
        clock, reader, _, guard = self.fixture(ppm=46.14)
        self.assertTrue(guard({})["ready"])
        clock.advance(21660)
        self.assertFalse(guard({})["ready"])
        self.assertFalse(guard.verify_preparation({})["ready"])
        self.assertEqual(reader.calls, 1)

    def test_cached_checks_do_not_renew_anchor_or_extend_proof_ttl(self):
        clock, reader, _, guard = self.fixture()
        self.assertTrue(guard({})["ready"])
        anchor = guard.wall, guard.mono, guard.last_positive
        clock.advance(10)
        self.assertTrue(guard({})["ready"])
        self.assertEqual((guard.wall, guard.mono, guard.last_positive), anchor)
        self.assertEqual(reader.calls, 1)
        clock.advance(.01)
        self.assertTrue(guard({})["ready"])
        self.assertNotEqual((guard.wall, guard.mono, guard.last_positive), anchor)
        self.assertEqual(reader.calls, 2)

    def test_forward_and_backward_wall_jumps_remain_latched(self):
        for jump in (.3, -.3):
            with self.subTest(jump=jump):
                clock, reader, _, guard = self.fixture()
                self.assertTrue(guard({})["ready"])
                clock.advance(1)
                clock.wall += jump
                self.assertFalse(guard({})["ready"])
                clock.wall -= jump
                self.assertFalse(guard.verify_preparation({})["ready"])
                self.assertEqual(reader.calls, 1)

    def test_negative_monotonic_between_cached_checks_is_rejected(self):
        clock, reader, _, guard = self.fixture()
        guard({})
        clock.advance(1)
        guard({})
        clock.mono -= .1
        self.assertFalse(guard({})["ready"])
        self.assertFalse(guard.verify_preparation({})["ready"])
        self.assertEqual(reader.calls, 1)

    def test_negative_monotonic_during_http_is_rejected(self):
        _, reader, _, guard = self.fixture()
        reader.mono_jump = -.1
        self.assertFalse(guard({})["ready"])
        self.assertEqual(guard.failure, "FRESH_EXCHANGE_CLOCK_UNVERIFIED")

    def test_query_wall_jump_and_rtt_limits_are_preserved(self):
        for field, value in (("jump", .3), ("jump", -.3), ("delay", 2.01)):
            with self.subTest(field=field, value=value):
                _, reader, _, guard = self.fixture()
                setattr(reader, field, value)
                self.assertFalse(guard.verify_preparation({})["ready"])
                self.assertIsNone(guard.wall)
                self.assertFalse(guard({})["ready"])
                self.assertEqual(reader.calls, 1)

    def test_absolute_exchange_offset_limit_is_preserved_on_idle_preparation(self):
        clock, reader, _, guard = self.fixture(ppm=46.14)
        self.assertTrue(guard({})["ready"])
        clock.advance(100000)
        self.assertFalse(guard.verify_preparation({})["ready"])
        self.assertEqual(reader.calls, 2)

    def test_clock_response_types_and_nonfinite_clock_readings_fail(self):
        for body in ({}, {"serverTime": True}, {"serverTime": "1800000000000"}):
            with self.subTest(body=body):
                _, reader, _, guard = self.fixture()
                reader.body = body
                self.assertFalse(guard({})["ready"])
        for field in ("wall", "mono"):
            with self.subTest(field=field):
                clock, reader, _, guard = self.fixture()
                setattr(clock, field, math.nan)
                self.assertFalse(guard({})["ready"])
                self.assertEqual(reader.calls, 0)

    def test_failed_http_is_sticky_and_preparation_cannot_clear_it(self):
        _, reader, _, guard = self.fixture()
        reader.error = TimeoutError("synthetic unavailable")
        self.assertFalse(guard({})["ready"])
        reader.error = None
        self.assertFalse(guard.verify_preparation({})["ready"])
        self.assertEqual(reader.calls, 1)

    def test_controls_never_mutate_shared_blocker_list_or_allow_reproof(self):
        _, reader, controls, guard = self.fixture()
        controls.append("EMERGENCY_OR_RISK_HALT_ACTIVE")
        self.assertFalse(guard.verify_preparation({})["ready"])
        self.assertEqual(controls, ["EMERGENCY_OR_RISK_HALT_ACTIVE"])
        self.assertEqual(reader.calls, 0)
        self.assertIsNone(guard.wall)

    def test_simultaneous_callers_share_one_serial_proof(self):
        _, reader, _, guard = self.fixture()
        outcomes = []
        threads = [threading.Thread(target=lambda: outcomes.append(guard({})["ready"])) for _ in range(8)]
        for worker in threads:
            worker.start()
        for worker in threads:
            worker.join()
        self.assertEqual(outcomes, [True]*8)
        self.assertEqual(reader.calls, 1)


class ServiceGuardDispatchTests(unittest.TestCase):
    def harness(self):
        path = SOURCE_ROOT / "short_live_session.py"
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        cls = next(row for row in tree.body if isinstance(row, ast.ClassDef) and row.name == "ShortLiveTestService")
        methods = [row for row in cls.body if isinstance(row, ast.FunctionDef) and row.name in {"_guard", "prepare"}]
        namespace = {"Mapping": Mapping, "Any": Any, "deepcopy": deepcopy, "ShortLiveContractError": ValueError}
        code = ast.Module(body=[ast.ClassDef(name="Harness", bases=[], keywords=[], body=methods, decorator_list=[])], type_ignores=[])
        exec(compile(ast.fix_missing_locations(code), str(path), "exec"), namespace)
        result = namespace["Harness"]()
        result._lock = threading.RLock()
        return result, cls

    def test_preparation_reproof_dispatch_is_explicit_start_and_runtime_are_strict(self):
        class Guard:
            def __init__(self):
                self.calls = []
            def __call__(self, manifest):
                self.calls.append("strict")
                return {"ready": True}
            def verify_preparation(self, manifest):
                self.calls.append("preparation")
                return {"ready": True}
        service, source_class = self.harness()
        service.clock_guard = Guard()
        service._guard({}, preparing=True)
        service._guard({})
        self.assertEqual(service.clock_guard.calls, ["preparation", "strict"])
        for method in (row for row in source_class.body if isinstance(row, ast.FunctionDef)):
            for call in ast.walk(method):
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "_guard" and call.keywords):
                    self.assertEqual(method.name, "prepare")
                    self.assertEqual([(k.arg, k.value.value) for k in call.keywords], [("preparing", True)])

    def test_active_session_is_refused_before_any_preparation_reproof(self):
        service, _ = self.harness()
        service._session = {"status": "RUNNING"}
        service.clock_guard = lambda _: self.fail("must not inspect or renew an active clock")
        with self.assertRaisesRegex(ValueError, "ACTIVE_SCOPE_CANNOT_BE_REPLACED"):
            service.prepare({})

    def test_injected_callable_guard_remains_supported(self):
        service, _ = self.harness()
        service.clock_guard = lambda _: {"ready": False, "blockers": ["fixture failure"]}
        self.assertEqual(service._guard({}, preparing=True), ["fixture failure"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
