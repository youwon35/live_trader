"""Header/status regressions without product startup, account or filesystem I/O."""
from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime, timezone
import importlib
from pathlib import Path
import sys
import threading
import types
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]/"live_trader"
PKG="_short_live_status_tests"
if PKG not in sys.modules:
    package=types.ModuleType(PKG); package.__path__=[str(ROOT)]; sys.modules[PKG]=package
runtime=importlib.import_module(PKG+".short_live_runtime")
emergency=importlib.import_module(PKG+".emergency_stop")
NOW=datetime(2026,10,22,tzinfo=timezone.utc)


def state_function(name,namespace=None):
    # Compile the actual function only, avoiding state.py's product globals.
    tree=ast.parse((ROOT/"state.py").read_text(encoding="utf-8-sig"))
    node=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name==name)
    node.decorator_list=[]
    env={"__package__":PKG,"sys":sys,"Any":object,**(namespace or {})}
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(ROOT/"state.py"),"exec"),env)
    return env[name]


class ShortLiveStatusTests(unittest.TestCase):
    def setUp(self):
        self.enter(patch.object(runtime,"_PUBLIC_STATUS",None))
        self.enter(patch.object(runtime,"_GLOBAL_STOP",threading.Event()))

    def enter(self,manager): result=manager.start(); self.addCleanup(manager.stop); return result

    def publish(self,status="RUNNING",**changes):
        service=object.__new__(runtime.ShortLiveTestService)
        service.clock=lambda:NOW
        service._session={"orders":{"accepted":{"status":"ACCEPTED"},"filled":{"status":"FILLED"}}}
        value={"sessionId":"fixture-session","status":status,"ownerAlive":True,"workerAlive":True,
               "newEntriesBlocked":False,"naturalExitObserved":False,"terminalVerified":False,"orderCount":2,
               "fillCount":1,"unknownOrderCount":0,**changes}
        with patch.object(runtime._ShortLiveTestService,"_public_runtime",return_value=value):
            service._public_runtime()
        return service

    def test_peek_never_constructs_service_or_acquires_locks(self):
        with patch.object(runtime,"get_service",side_effect=AssertionError("must not create")), \
             patch.object(runtime,"_LOCK",Mock(side_effect=AssertionError("must not lock"))):
            self.assertIsNone(runtime.peek_status())
            self.publish()
            self.assertTrue(runtime.peek_status()["active"])

    def test_published_status_is_detached_allowlisted_and_has_pending_count(self):
        self.publish(privateAccount={"token":"fixture-only-private"})
        view=runtime.peek_status()
        self.assertTrue(view["known"]); self.assertTrue(view["active"]); self.assertEqual(1,view["pendingOrderCount"])
        self.assertNotIn("privateAccount",view)
        view["status"]="TERMINATED"
        self.assertEqual("RUNNING",runtime.peek_status()["status"])
        self.assertFalse(runtime.peek_status()["promotionEligible"])

    def test_only_actual_verified_retirement_removes_active_header(self):
        self.publish("TERMINATED",terminalVerified=True,naturalExitObserved=False)
        self.assertTrue(runtime.peek_status()["active"])
        self.publish("TERMINATED",terminalVerified=True,naturalExitObserved=True)
        self.assertFalse(runtime.peek_status()["active"])

    def test_monitor_stop_latch_is_visible_before_waiting_for_worker(self):
        self.publish()
        runtime.request_global_stop()
        view=runtime.peek_status()
        self.assertTrue(view["active"]); self.assertTrue(view["globalStopRequested"])
        self.assertEqual("RUNNING",view["status"],"request intent is not an invented actual stop")

    def test_monitor_ordinary_mode_is_compatible_but_global_stop_and_money_controls_remain(self):
        state=types.ModuleType(PKG+".state")
        state.STATE={"mode":"MONITOR","dry_run":False,"operator_confirmed":True,"active_runtime_session_ids":{}}
        state.real_orders_enabled=lambda:True; state.durable_control_snapshot=lambda:{"halted":False}
        state.LIVE_CONTINUOUS_CONTROLLER=types.SimpleNamespace(snapshot=lambda:{"runtimes":{}})
        state._upbit_order_authority_snapshot=lambda:{"functionalAuthorityOpen":False}
        state._binance_order_authority_snapshot=lambda:{"functionalAuthorityOpen":False}
        with patch.dict(sys.modules,{state.__name__:state}), patch.object(emergency,"emergency_stop_active",return_value=False):
            self.assertEqual([],runtime._controls())
            runtime.request_global_stop(); self.assertIn("GLOBAL_MONITOR_STOP_REQUESTED",runtime._controls())
            state.STATE["dry_run"]=True; self.assertIn("DRY_RUN_ACTIVE",runtime._controls())
            state.real_orders_enabled=lambda:False; self.assertIn("REAL_ORDERS_DISABLED",runtime._controls())

    def test_only_new_inactive_preparation_clears_global_stop(self):
        service=types.SimpleNamespace(_session=None,snapshot=lambda:{"ok":True},prepare=lambda _: {})
        with patch.object(runtime,"get_service",return_value=service):
            runtime.request_global_stop(); runtime.request("snapshot"); self.assertTrue(runtime._GLOBAL_STOP.is_set())
            runtime.request("prepare",{}); self.assertFalse(runtime._GLOBAL_STOP.is_set())
            service._session={"status":"RUNNING"}; runtime.request_global_stop(); runtime.request("prepare",{})
            self.assertTrue(runtime._GLOBAL_STOP.is_set())

    def test_failed_new_preparation_restores_prior_stop_and_does_not_replace_prepared_scope(self):
        prior={"preparationId":"fixture-prior-exact-scope"}
        service=types.SimpleNamespace(_session=None,_prepared=prior,prepare=Mock(side_effect=ValueError("synthetic-invalid-selection")))
        with patch.object(runtime,"get_service",return_value=service):
            runtime.request_global_stop()
            result=runtime.request("prepare",{"selections":[]})
            self.assertFalse(result["ok"])
            self.assertTrue(runtime._GLOBAL_STOP.is_set())
            self.assertIs(prior,service._prepared)

    def test_regular_state_projection_uses_only_already_loaded_module_cache(self):
        project=state_function("short_live_status_snapshot")
        with patch.dict(sys.modules,{PKG+".short_live_runtime":None}): self.assertIsNone(project())
        self.publish()
        with patch.object(runtime,"get_service",side_effect=AssertionError("must not start")):
            self.assertEqual(runtime.peek_status(),project())
        tree=ast.parse((ROOT/"state.py").read_text(encoding="utf-8-sig"))
        snapshot=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=="snapshot")
        self.assertTrue(any(isinstance(node,ast.Constant) and node.value=="short_live_test_status" for node in ast.walk(snapshot)))

    def test_global_monitor_latch_precedes_even_failed_ordinary_worker_transition(self):
        events=[]
        state={"mode":"SMALL_LIVE","automation":{"crypto":{"enabled":True,"mode":"SMALL_LIVE"}}}
        def transition(_):
            self.assertTrue(runtime._GLOBAL_STOP.is_set()); events.append("ordinary-worker-stop"); raise RuntimeError("synthetic wait failed")
        namespace={"snapshot":lambda:{"summary":{"blocker_count":0,"warning_count":0},"watchdog":{}},
            "normalize_runtime_mode":lambda mode:mode,"STATE":state,"RUNTIME_MODE_LOCK":threading.RLock(),
            "LIVE_CONTINUOUS_CONTROLLER":types.SimpleNamespace(transition_running=transition),"append_audit":lambda *_:None}
        result=state_function("_set_mode_serialized",namespace)("MONITOR")
        self.assertFalse(result["ok"]); self.assertEqual(["ordinary-worker-stop"],events)
        self.assertEqual("MONITOR",state["mode"]); self.assertTrue(state["new_entries_blocked"])
        self.assertTrue(runtime._GLOBAL_STOP.is_set())


if __name__=="__main__": unittest.main()
