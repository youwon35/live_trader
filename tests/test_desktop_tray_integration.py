"""Desktop lifecycle tests execute production functions with fake native APIs.

Do not import the server/state module or construct a real account runtime.
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


def production_function(relative, name, namespace):
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), relative, "exec"), namespace)
    return namespace[name]


class Event:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, callback):
        self.handlers.append(callback)
        return self

    def fire(self):
        return [callback() for callback in self.handlers]


class Window:
    def __init__(self):
        self.events = types.SimpleNamespace(closing=Event(), shown=Event(), loaded=Event(), closed=Event())
        self.hidden = False
        self.confirm = False

    def hide(self):
        self.hidden = True

    def show(self):
        self.hidden = False

    def restore(self):
        self.hidden = False

    def create_confirmation_dialog(self, *_args):
        return self.confirm

    def destroy(self):
        if False not in self.events.closing.fire():
            self.events.closed.fire()


class Tray:
    instance = None

    def __init__(self, window, **kwargs):
        type(self).instance = self
        self.window = window
        self.kwargs = kwargs
        self.exit_requested = False
        self.stopped = False

    def start(self):
        return True

    def on_closing(self, *_args):
        if self.exit_requested:
            return None
        self.window.hidden = True
        return False

    def stop(self):
        self.stopped = True


class DesktopTrayIntegrationTests(unittest.TestCase):
    def run_fake_desktop(self, scenario, controller_factory=Tray):
        window = Window()
        server = types.SimpleNamespace(
            functional_http_session_authority=types.SimpleNamespace(
                native_bootstrap_url="http://synthetic.invalid/native-bootstrap",
                trusted_native_bootstrap=Mock(),
            ), shutdown=Mock(),
        )
        service = types.SimpleNamespace(stop=Mock())
        owner = {"_LOCK": threading.RLock(), "_SERVICE": service,
                 "_NATIVE_WINDOW_VISIBLE": False, "_NATIVE_WINDOW_CLOSING": False,
                 "live_trader_instance_lease_status": lambda: {"acquired": True}}
        for name in ("desktop_window_shown", "desktop_window_closing", "genuine_owner_alive"):
            production_function("live_trader/short_live_runtime.py", name, owner)
        runtime = types.ModuleType("live_trader.short_live_runtime")
        runtime.desktop_window_shown = owner["desktop_window_shown"]
        runtime.desktop_window_closing = owner["desktop_window_closing"]
        view = types.ModuleType("webview")
        view.create_window = Mock(return_value=window)
        view.start = Mock(side_effect=lambda **_kw: scenario(window, server, service, owner))
        saved = Mock()
        namespace = {
            "__name__": "live_trader.desktop", "__package__": "live_trader",
            "DesktopTrayController": controller_factory, "DesktopEmergencyStopBridge": Mock(),
            "live_trader_instance_lease_status": lambda: {"acquired": True},
            "start_in_thread": lambda: (server, "http://synthetic.invalid"),
            "_load_window_state": lambda: {"width": 1360, "height": 820, "x": 0, "y": 0, "maximized": False},
            "_save_window_state": saved, "_should_open_browser_fallback": lambda *_a: False,
            "MIN_WINDOW_WIDTH": 1180, "MIN_WINDOW_HEIGHT": 760,
            "KIS_NATIVE_OPERATIONAL_START_CONFIRMATIONS": object(),
            "UPBIT_NATIVE_SUPERVISED_ACTIVATION_ATTESTATIONS": object(),
            "time": types.SimpleNamespace(monotonic=lambda: 100), "webbrowser": types.SimpleNamespace(open=Mock()),
        }
        run = production_function("live_trader/desktop.py", "main", namespace)
        with patch.dict(sys.modules, {"webview": view, "live_trader.short_live_runtime": runtime}):
            run()
        return window, server, service, owner, saved, namespace

    def test_x_hides_without_stopping_worker_or_genuine_owner(self):
        def scenario(window, server, service, owner):
            window.events.shown.fire()
            self.assertTrue(owner["genuine_owner_alive"]())
            self.assertEqual([False], window.events.closing.fire())
            self.assertTrue(window.hidden)
            self.assertTrue(owner["genuine_owner_alive"]())
            service.stop.assert_not_called()
            server.shutdown.assert_not_called()
            # A later explicit confirmed quit permits the original cleanup.
            Tray.instance.exit_requested = True
            self.assertEqual([None], window.events.closing.fire())
            self.assertFalse(owner["genuine_owner_alive"]())
            service.stop.assert_called_once_with({})
        window, server, service, owner, saved, namespace = self.run_fake_desktop(scenario)
        server.shutdown.assert_called_once()
        self.assertTrue(Tray.instance.stopped)
        self.assertGreaterEqual(saved.call_count, 2)
        namespace["webbrowser"].open.assert_not_called()

    def test_repeated_x_and_restore_do_not_retire_the_same_owner(self):
        def scenario(window, server, service, owner):
            window.events.loaded.fire()
            for _ in range(3):
                self.assertEqual([False], window.events.closing.fire())
                self.assertTrue(owner["genuine_owner_alive"]())
                window.hidden = False
                window.events.shown.fire()
                service.stop.assert_not_called()
                server.shutdown.assert_not_called()
            Tray.instance.exit_requested = True
            window.events.closing.fire()
        self.run_fake_desktop(scenario)

    def test_unexpected_native_return_still_performs_old_cleanup(self):
        _, server, service, _, _, namespace = self.run_fake_desktop(lambda *_a: None)
        service.stop.assert_called_once_with({})
        server.shutdown.assert_called_once()
        self.assertTrue(Tray.instance.stopped)
        namespace["webbrowser"].open.assert_not_called()

    def test_real_shared_controller_cancel_and_confirm_preserve_old_owner_cleanup(self):
        shared = ROOT.parents[1] / "packages/trading_runtime/trading_runtime/desktop_tray.py"
        spec = importlib.util.spec_from_file_location("synthetic_desktop_tray", shared)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        controllers = []

        class Shell:
            def __init__(self, **_kwargs):
                self.available = True

            def start(self):
                return True

            def notify(self, _message):
                pass

            def stop(self):
                pass

        def factory(window, **kwargs):
            controller = module.DesktopTrayController(window, tray_factory=Shell, **kwargs)
            controllers.append(controller)
            return controller

        def scenario(window, server, service, owner):
            window.events.shown.fire()
            self.assertEqual([False], window.events.closing.fire())
            controller = controllers[0]
            self.assertTrue(window.hidden)
            self.assertTrue(owner["genuine_owner_alive"]())
            controller.request_exit()  # Cancel the confirmation.
            self.assertFalse(window.hidden)
            service.stop.assert_not_called()
            self.assertFalse(controller.exit_requested)
            self.assertTrue(owner["genuine_owner_alive"]())
            window.confirm = True
            controller.request_exit()
            service.stop.assert_called_once_with({})
            self.assertFalse(owner["genuine_owner_alive"]())
            server.shutdown.assert_not_called()
        _, server, _, _, _, _ = self.run_fake_desktop(scenario, factory)
        server.shutdown.assert_called_once()

    def test_duplicate_launch_restores_only_after_official_lease_denial(self):
        restore = Mock(return_value=True)
        tray_module = types.ModuleType("trading_runtime.desktop_tray")
        tray_module.restore_existing_window = restore
        namespace = {"argparse": argparse, "hold_live_trader_instance_lease": lambda: {"acquired": False, "reason": "busy"}}
        entry = production_function("live_trader/__main__.py", "entrypoint", namespace)
        with patch.object(sys, "argv", ["LiveTrader"]), patch.dict(sys.modules, {"trading_runtime.desktop_tray": tray_module}):
            self.assertEqual(0, entry())
        restore.assert_called_once_with("live-trader")

    def test_duplicate_without_restore_ack_keeps_original_denial(self):
        tray_module = types.ModuleType("trading_runtime.desktop_tray")
        tray_module.restore_existing_window = Mock(return_value=False)
        namespace = {"argparse": argparse, "hold_live_trader_instance_lease": lambda: {"acquired": False, "reason": "busy"}}
        entry = production_function("live_trader/__main__.py", "entrypoint", namespace)
        with patch.object(sys, "argv", ["LiveTrader"]), patch.dict(sys.modules, {"trading_runtime.desktop_tray": tray_module}):
            self.assertEqual(2, entry())

    def test_daemon_duplicate_does_not_open_a_native_window(self):
        tray_module = types.ModuleType("trading_runtime.desktop_tray")
        tray_module.restore_existing_window = Mock(return_value=True)
        namespace = {"argparse": argparse, "hold_live_trader_instance_lease": lambda: {"acquired": False, "reason": "busy"}}
        entry = production_function("live_trader/__main__.py", "entrypoint", namespace)
        with patch.object(sys, "argv", ["LiveTrader", "--daemon"]), patch.dict(sys.modules, {"trading_runtime.desktop_tray": tray_module}):
            self.assertEqual(2, entry())
        tray_module.restore_existing_window.assert_not_called()


if __name__ == "__main__":
    unittest.main()
