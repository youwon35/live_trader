"""Synthetic dispatch diagnostics only; use the package-stub offline runner."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest

import test_short_live_brokers as broker_base
import test_short_live_session as session_base

SAFE = {"provider": "binance", "path": "/api/v3/account", "elapsedMs": 8000,
        "status": "UNAVAILABLE", "failureKind": "TIMEOUT"}


def dirty_diagnostics():
    return [{**SAFE, "query": "signature=private-secret", "body": "private-secret",
             "headers": {"key": "private-secret"}},
            {**SAFE, "path": "/api/v3/account?signature=private-secret"}]


class DispatchAdapterDiagnosticTests(unittest.TestCase):
    def test_sizing_and_dispatch_recheck_keep_failure_code_and_safe_diagnostics_without_submit(self):
        for source in ("account", "market"):
            for stage in ("sizing", "dispatch-recheck"):
                with self.subTest(source=source, stage=stage):
                    f = broker_base.Fixture("BINANCE_SPOT")
                    self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
                    proposal = f.proposal()
                    sized = f.adapter.size_order(proposal)
                    self.assertTrue(sized["ready"])
                    failed = {"known": False, "blockers": ["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"],
                              "readDiagnostics": dirty_diagnostics()}
                    if source == "account":
                        f.adapter.snapshot_reader = lambda *_: deepcopy(failed)
                    else:
                        f.adapter.market_reader = lambda *_: deepcopy(failed)
                    result = (f.adapter.size_order(proposal) if stage == "sizing"
                              else f.adapter.submit(sized))
                    self.assertEqual(["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"], result["blockers"])
                    self.assertEqual([SAFE], result["readDiagnostics"])
                    if stage == "dispatch-recheck":
                        self.assertEqual("REJECTED", result["status"])
                    else:
                        self.assertFalse(result["ready"])
                    self.assertEqual([], f.calls)
                    self.assertEqual({}, f.adapter._claims)
                    self.assertNotIn("private-secret", json.dumps(result))

    def test_error_after_claim_keeps_unknown_and_single_submission(self):
        f = broker_base.Fixture("BINANCE_SPOT")
        self.assertTrue(f.adapter.readiness(f.manifest)["ready"])
        sized = f.adapter.size_order(f.proposal())

        def lost_reply(route, order):
            f.calls.append(deepcopy(order))
            error = TimeoutError("private-secret")
            error.readDiagnostics = dirty_diagnostics()
            raise error

        f.adapter.transport.submit = lost_reply
        result = f.adapter.submit(sized)
        self.assertEqual("UNKNOWN", result["status"])
        self.assertEqual([SAFE], result["readDiagnostics"])
        self.assertNotIn("private-secret", json.dumps(result))
        self.assertEqual(result, f.adapter.submit(sized))
        self.assertEqual(1, len(f.calls))


class DispatchSessionDiagnosticTests(unittest.TestCase):
    setUp = session_base.ShortLiveSessionTests.setUp
    tearDown = session_base.ShortLiveSessionTests.tearDown
    service = session_base.ShortLiveSessionTests.service
    prepare = session_base.ShortLiveSessionTests.prepare
    start = session_base.ShortLiveSessionTests.start
    first_buy = session_base.ShortLiveSessionTests.first_buy

    def assert_safe_first_failure(self, service, expected):
        runtime = service.snapshot(include_catalog=False)["runtime"]
        self.assertEqual(expected, runtime["firstFailure"])
        self.assertEqual([SAFE], runtime["firstFailureDiagnostics"])
        self.assertTrue(runtime["newEntriesBlocked"])
        failures = [json.loads(path.read_text(encoding="utf-8"))
                    for path in service.store_root.glob("event-*.json")]
        failures = [row for row in failures if row["kind"] == "FIRST_FAILURE"]
        self.assertEqual(1, len(failures))
        self.assertEqual([SAFE], failures[0]["readDiagnostics"])
        self.assertNotIn("private-secret", json.dumps([runtime, failures]))

    def test_sizing_diagnostics_reach_step_and_worker_tick_without_order_attempt(self):
        for entry in ("step", "tick"):
            with self.subTest(entry=entry):
                self.now = session_base.NOW
                spec = session_base.runtime_spec()
                service, brokers, feeds = self.service([spec])
                self.start(service)
                broker = brokers["BINANCE_SPOT"]
                broker.size_order = lambda _: {"ready": False,
                    "blockers": ["OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN"],
                    "readDiagnostics": dirty_diagnostics()}
                if entry == "step":
                    self.first_buy(service, spec)
                else:
                    self.now += timedelta(milliseconds=200)
                    feeds["BINANCE_SPOT"].rows = [session_base.candle(spec, session_base.NOW, 6),
                                                  session_base.opening(spec, session_base.NOW, 7)]
                    service.tick()
                prefix = "STEP_BLOCKED" if entry == "step" else "RUNTIME_BLOCKED"
                self.assert_safe_first_failure(service, prefix + ":ShortLiveContractError:NATIVE_ORDER_SIZE_BLOCKED:OFFICIAL_MARKET_FEE_OR_FX_UNKNOWN")
                self.assertEqual([], broker.submits)
                self.assertEqual(0, service._session["orderCount"])
                service.tick()
                self.assertEqual([], broker.submits)

    def assert_submission_outcome(self, status, broker_id, expected):
        self.now = session_base.NOW
        spec = session_base.runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        broker = brokers["BINANCE_SPOT"]
        broker.status = "UNKNOWN"

        def submit(order):
            broker.submits.append(deepcopy(order))
            return {"status": status, "clientOrderId": order["clientOrderId"],
                    "brokerOrderId": broker_id, "readDiagnostics": dirty_diagnostics()}

        broker.submit = submit
        self.first_buy(service, spec)
        self.assert_safe_first_failure(service, expected)
        for _ in range(3):
            service.tick()
        self.assertEqual(1, len(broker.submits))
        self.assertEqual(1, service._session["orderCount"])
        self.assert_safe_first_failure(service, expected)
        # Rejected without a broker identity remains uncertain, as before this diagnostic change.
        recorded = next(iter(service._session["orders"].values()))["status"]
        self.assertEqual("REJECTED" if broker_id else "UNKNOWN", recorded)

    def test_unknown_submission_diagnostics_never_resubmit(self):
        self.assert_submission_outcome("UNKNOWN", None, "ORDER_RESULT_UNKNOWN_NO_RETRY")

    def test_rejection_without_broker_identity_keeps_unknown(self):
        self.assert_submission_outcome("REJECTED", None, "ORDER_RESULT_UNKNOWN_NO_RETRY")

    def test_confirmed_rejection_keeps_rejected(self):
        self.assert_submission_outcome("REJECTED", "fixture-reject", "BROKER_ORDER_REJECTED")

    def test_raised_submission_error_keeps_safe_diagnostics_and_unknown_once(self):
        spec = session_base.runtime_spec()
        service, brokers, _ = self.service([spec])
        self.start(service)
        broker = brokers["BINANCE_SPOT"]
        broker.status = "UNKNOWN"

        def submit(order):
            broker.submits.append(deepcopy(order))
            error = TimeoutError("private-secret")
            error.readDiagnostics = dirty_diagnostics()
            raise error

        broker.submit = submit
        self.first_buy(service, spec)
        self.assert_safe_first_failure(service, "ORDER_RESULT_UNKNOWN_NO_RETRY")
        service.tick()
        self.assertEqual(1, len(broker.submits))


if __name__ == "__main__":
    unittest.main()
