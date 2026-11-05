"""Clock guard through real service flow, with synthetic brokers and feeds."""
from datetime import timedelta
import unittest

import test_short_live_session as fixture
from test_fresh_clock_continuity import FakeTime, Reader, clock_class


class FreshClockServiceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.ShortLiveSessionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.service, self.brokers, _ = self.fixture.service()
        self.clock = FakeTime(46.14)
        self.reader = Reader(self.clock)
        self.guard = clock_class(self.clock)(self.reader, lambda: [])
        self.service.clock_guard = self.guard

    def start_payload(self, prepared):
        return {"preparationId": prepared["preparationId"], "scopeHash": prepared["scopeHash"],
                "confirmed": True, "confirmationText": fixture.CONFIRMATION_TEXT,
                "executionModelOverrideHash": (prepared.get("executionModelBinding") or {}).get("bindingHash")}

    def test_idle_reprepare_start_then_runtime_jump_and_active_reprepare_refusal(self):
        first = self.fixture.prepare(self.service)["prepared"]
        self.assertTrue(first["readyToStart"])
        self.clock.advance(21660)
        prepared = self.fixture.prepare(self.service)["prepared"]
        self.assertTrue(prepared["readyToStart"])
        self.assertEqual(self.reader.calls, 2)
        self.service.start(self.start_payload(prepared))
        self.assertEqual(self.service._session["status"], "RUNNING")
        with self.assertRaisesRegex(fixture.ShortLiveContractError, "ACTIVE_SCOPE_CANNOT_BE_REPLACED"):
            self.fixture.prepare(self.service)
        self.assertEqual(self.reader.calls, 2)
        self.clock.advance(1)
        self.clock.wall += .3
        self.service.tick()
        self.assertNotEqual(self.service._session["status"], "RUNNING")
        self.assertEqual(self.guard.failure, "WALL_MONOTONIC_CONTINUITY_FAILED")
        self.assertTrue(all(not broker.submits for broker in self.brokers.values()))

    def test_jump_between_preparation_and_start_is_not_rebased(self):
        prepared = self.fixture.prepare(self.service)["prepared"]
        self.clock.advance(1)
        self.clock.wall -= .3
        with self.assertRaises(fixture.ShortLiveContractError):
            self.service.start(self.start_payload(prepared))
        self.assertEqual(self.guard.failure, "WALL_MONOTONIC_CONTINUITY_FAILED")
        self.assertEqual(self.reader.calls, 1)
        self.assertTrue(all(not broker.submits for broker in self.brokers.values()))

    def test_preparation_ttl_still_expires_at_180_seconds(self):
        prepared = self.fixture.prepare(self.service)["prepared"]
        self.fixture.now += timedelta(seconds=180)
        self.clock.advance(180)
        with self.assertRaisesRegex(fixture.ShortLiveContractError, "PREPARATION_EXPIRED"):
            self.service.start(self.start_payload(prepared))
        self.assertEqual(self.reader.calls, 1)
        self.assertTrue(all(not broker.submits for broker in self.brokers.values()))


if __name__ == "__main__":
    unittest.main()
