from __future__ import annotations

import json
import os
import sys
import time
import webbrowser
from pathlib import Path

from .server import start_in_thread

DEFAULT_WINDOW_WIDTH = 1480
DEFAULT_WINDOW_HEIGHT = 980
MIN_WINDOW_WIDTH = 1180
MIN_WINDOW_HEIGHT = 760


def main() -> None:
    server, url = start_in_thread()
    webview_started_at: float | None = None
    window_was_visible = False
    shutdown_requested = False
    try:
        import webview

        window_state = _load_window_state()
        window = webview.create_window(
            "Live Trader",
            url,
            width=window_state["width"],
            height=window_state["height"],
            min_size=(MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT),
        )

        def remember_window_state(*_args: object) -> None:
            nonlocal shutdown_requested
            shutdown_requested = True
            _save_window_state(window)

        def mark_window_visible(*_args: object) -> None:
            nonlocal window_was_visible
            window_was_visible = True

        try:
            window.events.closing += remember_window_state
        except Exception:
            pass
        for event_name in ("shown", "loaded"):
            try:
                event = getattr(window.events, event_name)
                event += mark_window_visible
            except Exception:
                pass
        webview_started_at = time.monotonic()
        webview.start(gui="edgechromium")
        _save_window_state(window)
    except Exception as exc:
        if _should_open_browser_fallback(webview_started_at, window_was_visible, shutdown_requested):
            print(f"WebView unavailable ({exc}). Opening browser at {url}")
            webbrowser.open(url)
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        else:
            print(f"WebView 종료 중 예외가 발생해 서버를 정리합니다: {exc}")
    finally:
        server.shutdown()


def _should_open_browser_fallback(
    started_at: float | None,
    window_was_visible: bool,
    shutdown_requested: bool,
) -> bool:
    """Use browser fallback only when the native window genuinely failed to start."""
    if shutdown_requested or window_was_visible:
        return False
    if started_at is None:
        return True
    return time.monotonic() - started_at < 2.0


def _app_data_root() -> Path:
    base = os.environ.get("LOCALAPPDATA")
    if base:
        return Path(base) / "live_trader"
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "app_data"
    return Path.home() / ".live_trader"


def _window_state_path() -> Path:
    return _app_data_root() / "window_state.json"


def _load_window_state() -> dict[str, int]:
    defaults = {"width": DEFAULT_WINDOW_WIDTH, "height": DEFAULT_WINDOW_HEIGHT}
    try:
        data = json.loads(_window_state_path().read_text(encoding="utf-8"))
    except Exception:
        return defaults
    return {
        "width": _clamp_int(data.get("width"), DEFAULT_WINDOW_WIDTH, MIN_WINDOW_WIDTH, 7680),
        "height": _clamp_int(data.get("height"), DEFAULT_WINDOW_HEIGHT, MIN_WINDOW_HEIGHT, 4320),
    }


def _save_window_state(window: object | None) -> None:
    if window is None:
        return
    width = _read_window_dimension(window, "width", MIN_WINDOW_WIDTH, 7680)
    height = _read_window_dimension(window, "height", MIN_WINDOW_HEIGHT, 4320)
    try:
        root = _app_data_root()
        root.mkdir(parents=True, exist_ok=True)
        _window_state_path().write_text(
            json.dumps({"width": width, "height": height}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _read_window_dimension(window: object, name: str, minimum: int, maximum: int) -> int:
    value = getattr(window, name, None)
    if callable(value):
        try:
            value = value()
        except Exception:
            value = None
    return _clamp_int(value, DEFAULT_WINDOW_WIDTH if name == "width" else DEFAULT_WINDOW_HEIGHT, minimum, maximum)


def _clamp_int(value: object, fallback: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return min(maximum, max(minimum, number))
