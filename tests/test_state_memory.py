import unittest

from live_trader import state


class StateMemoryTest(unittest.TestCase):
    def test_audit_log_is_capped_for_long_running_sessions(self) -> None:
        original_audit = list(state.STATE["audit"])
        try:
            state.STATE["audit"] = []
            for index in range(state.AUDIT_LOG_LIMIT + 25):
                state.append_audit("info", "unit", f"event {index}")

            self.assertEqual(state.AUDIT_LOG_LIMIT, len(state.STATE["audit"]))
            self.assertEqual("event 25", state.STATE["audit"][0]["detail"])
        finally:
            state.STATE["audit"] = original_audit


if __name__ == "__main__":
    unittest.main()
