from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

from live_trader.desktop import _default_window_state, _load_window_state, _save_window_state, _should_open_browser_fallback


class _NativeWindow:
    WindowState = "Maximized"


class _FakeWindow:
    width = 1520
    height = 920
    x = 3900
    y = 60
    native = _NativeWindow()


class DesktopLifecycleTests(unittest.TestCase):
    def test_default_window_state_targets_right_monitor(self) -> None:
        self.assertEqual(
            _default_window_state(),
            {"width": 1360, "height": 820, "x": 3840, "y": 0, "maximized": True},
        )

    def test_window_state_round_trip_includes_maximized_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch("live_trader.desktop._app_data_root", return_value=root),
                patch("live_trader.desktop._window_state_path", return_value=root / "window_state.json"),
            ):
                _save_window_state(_FakeWindow())
                self.assertEqual(
                    _load_window_state(),
                    {"width": 1520, "height": 920, "x": 3900, "y": 60, "maximized": True},
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
