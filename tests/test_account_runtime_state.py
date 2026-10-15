from __future__ import annotations
from contextlib import ExitStack
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest
from live_trader import state

class AccountRuntimeStateTest(unittest.TestCase):
    def setUp(self):
        old = deepcopy(state.STATE)
        self.addCleanup(lambda: (state.STATE.clear(), state.STATE.update(old)))
        state.STATE["active_runtime_session_ids"] = {"runtime-b": "session-b", "runtime-u": "session-u"}
        state.STATE["mode"] = "FULL_LIVE"
        self.documents = {
            "session-b": {"sessionId": "session-b", "profile": "crypto", "deploymentId": "dep-b", "mode": "SMALL_LIVE", "metadata": {"runtimeScopeId": "runtime-b", "brokerId": "binance"}},
            "session-u": {"sessionId": "session-u", "profile": "crypto", "deploymentId": "dep-u", "mode": "FULL_LIVE", "metadata": {"runtimeScopeId": "runtime-u", "brokerId": "upbit"}},
        }
        self.sessions = {key: SimpleNamespace(session_id=key, mode=value["mode"], lifecycle="RUNNING", to_dict=lambda value=value: value) for key,value in self.documents.items()}
        self.governance = Mock()
        self.governance.get_runtime_session.side_effect=self.sessions.get
        def transition(session_id, lifecycle, **_):
            self.sessions[session_id].lifecycle=lifecycle
            return self.sessions[session_id]
        self.governance.transition_runtime_session.side_effect=transition
        self.stack=ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(state,"OPERATIONAL_GOVERNANCE",self.governance))
        self.stack.enter_context(patch.object(state,"snapshot",return_value={"isolated":True}))
        # Test the control body; the independent final authority fences have
        # their own tests and no broker transport is present in this fixture.
        self.stop = state.stop_continuous_runtime
        while hasattr(self.stop,"__wrapped__"): self.stop=self.stop.__wrapped__

    def intent(self, broker="binance", deployment="dep-b", scope="runtime-b"):
        return SimpleNamespace(metadata={"broker_id":broker,"deployment_id":deployment,"runtime_scope_id":scope}, symbol="BTCUSDT",asset="crypto",mode="SMALL_LIVE")

    def test_global_monitor_removes_live_authority_even_when_engine_and_stop_fail(self):
        manager=Mock();manager.transition_running.return_value={"ok":False,"reason":"engine and stop both failed"}
        for raises in (False,True):
            with self.subTest(raises=raises):
                state.STATE["mode"]="FULL_LIVE"
                state.STATE["automation"]["crypto"].update(mode="FULL_LIVE",enabled=True)
                manager.transition_running.side_effect=RuntimeError("stop failed") if raises else None
                with patch.object(state,"LIVE_CONTINUOUS_CONTROLLER",manager),patch.object(state,"append_audit"),patch.object(state,"snapshot",return_value={"summary":{"blocker_count":0,"warning_count":0}}):
                    result=state._set_mode_serialized("MONITOR")
                self.assertFalse(result["ok"])
                self.assertEqual("MONITOR",state.STATE["mode"])
                self.assertFalse(state.STATE["automation"]["crypto"]["enabled"])
                self.assertEqual("MONITOR",state._intent_runtime_mode(self.intent()))
                self.assertTrue(state.STATE["new_entries_blocked"])

    def test_each_intent_keeps_its_account_session_mode_under_global_maximum(self):
        self.assertEqual("SMALL_LIVE",state._intent_runtime_mode(self.intent()))
        self.assertEqual("FULL_LIVE",state._intent_runtime_mode(self.intent("upbit","dep-u","runtime-u")))
        self.assertEqual("MONITOR",state._intent_runtime_mode(self.intent("upbit","dep-u","runtime-b")))
        state.STATE["mode"]="MONITOR"
        self.assertEqual("MONITOR",state._intent_runtime_mode(self.intent()))

    def test_failed_start_cannot_restore_previous_session_from_another_account(self):
        failed=SimpleNamespace(session_id="failed-b",metadata={"runtimeScopeId":"runtime-b"})
        state.STATE["active_runtime_session_ids"]["runtime-b"]="failed-b"
        state._restore_previous_runtime_session_binding("crypto",failed,"session-u")
        self.assertEqual({"runtime-u":"session-u"},state.STATE["active_runtime_session_ids"])
        self.assertTrue(state.STATE["new_entries_blocked"])

    def test_replaced_session_retires_only_same_scope(self):
        previous=self.sessions["session-b"]
        previous.metadata=self.documents["session-b"]["metadata"]
        current=SimpleNamespace(session_id="successor",metadata={"runtimeScopeId":"runtime-b"})
        self.assertTrue(state._retire_superseded_runtime_session("session-b",current))
        self.assertEqual("STOPPED",previous.lifecycle)
        self.assertEqual("RUNNING",self.sessions["session-u"].lifecycle)
        self.assertFalse(state._retire_superseded_runtime_session("session-u",current))

    def test_unknown_stop_selectors_never_stop_other_account(self):
        manager=Mock(); manager.snapshot.return_value={"runtimes":{"runtime-b":{"running":True,"profileId":"crypto","deploymentId":"dep-b"}}}
        with patch.object(state,"LIVE_CONTINUOUS_CONTROLLER",manager):
            self.assertFalse(self.stop("typo")["ok"])
            self.assertFalse(self.stop("crypto",runtime_id="runtime-missing")["ok"])
        manager.stop.assert_not_called()
        self.governance.transition_runtime_session.assert_not_called()

    def test_scoped_stop_preserves_other_running_account_mode_and_session(self):
        before={"runtimes":{
            "runtime-b":{"running":True,"profileId":"crypto","deploymentId":"dep-b","mode":"SMALL_LIVE"},
            "runtime-u":{"running":True,"profileId":"crypto","deploymentId":"dep-u","mode":"FULL_LIVE"},
        }}
        after=deepcopy(before); after["runtimes"]["runtime-b"]["running"]=False
        manager=Mock(); manager.snapshot.side_effect=[before,after]; manager.stop.return_value={"ok":True}
        with patch.object(state,"LIVE_CONTINUOUS_CONTROLLER",manager):
            result=self.stop("crypto",runtime_id="runtime-b",deployment_id="dep-b")
        self.assertTrue(result["ok"])
        manager.stop.assert_called_once_with("crypto",runtime_id="runtime-b",deployment_id="dep-b")
        self.assertEqual({"runtime-u":"session-u"},state.STATE["active_runtime_session_ids"])
        self.assertEqual("FULL_LIVE",state.STATE["mode"])
        self.assertEqual("RUNNING",self.sessions["session-u"].lifecycle)
        self.assertEqual("STOPPED",self.sessions["session-b"].lifecycle)

    def test_profile_only_stop_is_ambiguous_and_has_no_lifecycle_side_effect(self):
        manager=Mock(); manager.snapshot.return_value={"runtimes":{key:{"running":True,"profileId":"crypto"} for key in ("runtime-b","runtime-u")}}
        with patch.object(state,"LIVE_CONTINUOUS_CONTROLLER",manager):
            self.assertFalse(self.stop("crypto")["ok"])
        manager.stop.assert_not_called()
        self.governance.transition_runtime_session.assert_not_called()

if __name__=="__main__": unittest.main()
