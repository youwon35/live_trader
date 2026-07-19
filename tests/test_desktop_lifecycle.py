from __future__ import annotations

import unittest
from unittest.mock import patch

from live_trader.desktop import _default_window_state, _should_open_browser_fallback


class DesktopLifecycleTests(unittest.TestCase):
    def test_default_window_state_targets_right_monitor(self) -> None:
        self.assertEqual(
            _default_window_state(),
            {"width": 1360, "height": 820, "x": 3840, "y": 0},
        )

    def test_uses_browser_when_native_window_fails_before_start(self) -> None:
        self.assertTrue(_should_open_browser_fallback(None, False, False))

    def test_does_not_reopen_browser_after_visible_window_closes(self) -> None:
        self.assertFalse(_should_open_browser_fallback(100.0, True, True))

    def test_closing_signal_suppresses_fallback_even_before_visible_event(self) -> None:
        self.assertFalse(_should_open_browser_fallback(100.0, False, True))

    @patch("live_trader.desktop.time.monotonic", return_value=101.0)
    def test_immediate_native_start_failure_can_fallback(self, _monotonic: object) -> None:
        self.assertTrue(_should_open_browser_fallback(100.0, False, False))

    @patch("live_trader.desktop.time.monotonic", return_value=103.0)
    def test_late_lifecycle_error_does_not_fallback(self, _monotonic: object) -> None:
        self.assertFalse(_should_open_browser_fallback(100.0, False, False))


if __name__ == "__main__":
    unittest.main()
