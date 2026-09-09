from __future__ import annotations
import ast
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from live_trader.operator_review import OperatorReviewStore, record_key, manifest_weights
ROOT=Path(__file__).resolve().parents[1]

def state_function(name, values):
    tree=ast.parse((ROOT/'live_trader/state.py').read_text(encoding='utf-8'))
    node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    node.decorator_list=[]
    scope={'sqlite3':sqlite3,**values}
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(ROOT/'live_trader/state.py'),'exec'),scope)
    return scope[name]

class OperatorReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'review.sqlite3';self.store=OperatorReviewStore(self.path)
        self.key=record_key({'event_id':'immutable-order-1'})
    def test_note_restart_edit_delete_history(self):
        first=self.store.save_note(self.key,'실행 근거 메모',0)
        restarted=OperatorReviewStore(self.path)
        self.assertEqual(restarted.note(self.key)['text'],'실행 근거 메모')
        second=restarted.save_note(self.key,'',first['revision'])
        self.assertEqual(second['text'],'');self.assertEqual(len(second['history']),2)
    def test_stale_write_cannot_overwrite(self):
        self.store.save_note(self.key,'first',0)
        with self.assertRaisesRegex(ValueError,'다른 창'):self.store.save_note(self.key,'stale',0)
        self.assertEqual(self.store.note(self.key)['text'],'first')
    def test_invalid_keys_lengths_revision(self):
        for key in ['', '../x', 'f'*63]:
            with self.assertRaises(ValueError):self.store.note(key)
        for text,rev in [('a'*501,0),('bad\x00',0),('ok',True),('ok',-1)]:
            with self.assertRaises(ValueError):self.store.save_note(self.key,text,rev)
    def test_key_ignores_display_order_and_keeps_event_identity(self):
        self.assertEqual(self.key,record_key({'event_id':'immutable-order-1','message':'redacted','operatorRecordKey':'x'}))
        self.assertNotEqual(record_key({'timestamp':'1','message':'a'}),record_key({'timestamp':'1','message':'b'}))
    def test_preflight_two_latest_are_scoped_and_survive_restart(self):
        for scope in ['a','b','a']:self.store.capture_preflight(scope,[{'label':'권한','status':'fail','detail':'근거','secret':'excluded'}],scope,{'limit':0})
        history=OperatorReviewStore(self.path).preflights('a')
        self.assertEqual(len(history),2);self.assertGreater(history[0]['id'],history[1]['id'])
        self.assertNotIn('secret',history[0]['checks'][0]);self.assertEqual(self.store.risk_at_manifest('a'),{'limit':0})
        self.assertIsNone(self.store.risk_at_manifest('unrecorded'))
    def test_weights_exact_artifact_missing_and_zero(self):
        manifest={'metadata':{'strategyMembers':[{'strategyInstanceId':'i1','strategyId':'s1','symbol':'A'}]}}
        portfolio={'strategy_instances':[{'instanceId':'i1','strategyId':'s1','symbol':'A','allocation':{'normalizedWeight':.5}}], 'portfolio_policy':{'allocations':[{'strategyInstanceId':'i1','targetWeight':0,'positionSizeFraction':.3}]}}
        self.assertEqual(manifest_weights(manifest,None),{'i1:A':None})
        self.assertEqual(manifest_weights(manifest,portfolio),{'i1:A':0})
        portfolio['portfolio_policy']['allocations'][0]['targetWeight']=.5
        self.assertEqual(manifest_weights(manifest,portfolio),{'i1:A':.15})
    def test_binding_uses_exact_active_session_not_latest_or_view(self):
        manifest=SimpleNamespace(to_dict=lambda:{'manifestHash':'exact'})
        session=SimpleNamespace(lifecycle='RUNNING',deployment_manifest_hash='exact',deployment_id='running',session_id='session',mode='MONITOR')
        calls=[]
        governance=SimpleNamespace(get_runtime_session=lambda key:(calls.append(key) or session),get_deployment_manifest_by_hash=lambda key:manifest)
        fn=state_function('operator_runtime_bindings',{'STATE':{'active_runtime_session_ids':{'stock':'session'}},'OPERATIONAL_GOVERNANCE':governance})
        result=fn({'profiles':{'stock':{'running':True,'deploymentId':'running'},'crypto':{'running':False}}})
        self.assertEqual(calls,['session']);self.assertEqual(result[0]['manifest']['manifestHash'],'exact')
        self.assertIsNone(fn({'profiles':{'stock':{'running':True,'deploymentId':'other'}}})[0]['manifest'])
        self.assertEqual(fn({'running':False}),[])
    def test_note_route_state_rejects_unrelated_record_without_touching_audit(self):
        rows=[{'event_id':'immutable-order-1','message':'original'}]
        fn=state_function('operator_note_document',{'durable_audit_rows':lambda:rows,'STATE':{'audit':[]},'record_key':record_key,'OPERATOR_REVIEW_STORE':self.store})
        self.assertTrue(fn(self.key,{'text':'note','revision':0})['ok'])
        self.assertEqual(rows,[{'event_id':'immutable-order-1','message':'original'}])
        self.assertFalse(fn('a'*64,{'text':'not owned','revision':0})['ok'])
    def test_capture_failure_never_changes_safety_flags(self):
        state={'kill_switch':True,'new_entries_blocked':True,'risk_settings':{}}
        def fail(*a):raise sqlite3.OperationalError('offline disk')
        fn=state_function('capture_operator_preflight',{'OPERATOR_REVIEW_STORE':SimpleNamespace(capture_preflight=fail),'STATE':state})
        fn('scope',[{'label':'check','status':'fail'}])
        self.assertTrue(state['kill_switch']);self.assertTrue(state['new_entries_blocked']);self.assertTrue(state['operator_review_error'])
    def test_preflight_capture_is_only_in_explicit_run_not_snapshot_poll(self):
        tree=ast.parse((ROOT/'live_trader/state.py').read_text(encoding='utf-8'))
        functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
        count=lambda n:sum(isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='capture_operator_preflight' for node in ast.walk(n))
        self.assertEqual(count(functions['run_final_preflight']),2);self.assertEqual(count(functions['snapshot']),0)
    def test_old_order_history_is_exact_paginated_and_read_only(self):
        from live_trader.operator_review import read_order_audit
        dbpath=Path(self.temp.name)/"audit.sqlite3"
        with sqlite3.connect(dbpath) as db:
            db.execute("CREATE TABLE audit_events(event_id TEXT,occurred_at TEXT,category TEXT,level TEXT,source TEXT,message TEXT,order_id TEXT,reason TEXT,state TEXT,trace_id TEXT)")
            for i in range(55):db.execute("INSERT INTO audit_events VALUES (?,?,?,?,?,?,?,?,?,?)",(str(i),f"2026-01-{i:02}","ORDER","WARN","reconcile","saved investigation","one","pending","UNKNOWN","trace"))
            db.execute("INSERT INTO audit_events VALUES (?,?,?,?,?,?,?,?,?,?)",("other","2026-09-10","ORDER","WARN","other","not this order","two","pending","UNKNOWN","trace"))
        db.close()
        before=dbpath.read_bytes()
        first=read_order_audit(dbpath,"one");second=read_order_audit(dbpath,"one",first['nextOffset'])
        self.assertEqual(first['total'],55);self.assertEqual(len(first['events']),50);self.assertEqual(len(second['events']),5);self.assertIsNone(second['nextOffset'])
        self.assertTrue(all(row['order_id']=='one' for row in first['events']+second['events']))
        self.assertEqual(dbpath.read_bytes(),before)
        self.assertEqual(read_order_audit(dbpath,"one' OR 1=1 --")['total'],0)
    def test_failed_requested_preflight_is_captured_from_actual_handler_body(self):
        from typing import Any
        calls=[]
        values={'Any':Any,'STATE':{},'datetime':__import__('datetime').datetime,'snapshot':lambda:{'strategies':[]},'_operational_strategy':lambda *a,**k:None,'launch_report':lambda checks:{'hard_stop_count':1,'warning_count':0},'persist_doctor_diagnostic_snapshot':lambda data:data,'append_audit':lambda *a:None,'capture_operator_preflight':lambda *args:calls.append(args)}
        fn=state_function('run_final_preflight',values)
        result=fn('missing-deployment','missing-strategy')
        self.assertFalse(result['ok']);self.assertEqual(calls[0][0],'missing-deployment');self.assertEqual(calls[0][1][0]['status'],'fail')
    def test_http_note_origin_and_content_type_boundary(self):
        from urllib.parse import urlparse
        tree=ast.parse((ROOT/'live_trader/server.py').read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LiveTraderHandler')
        method=next(n for n in node.body if isinstance(n,ast.FunctionDef) and n.name=='do_POST')
        calls=[];responses=[]
        scope={'urlparse':urlparse,'sqlite3':sqlite3,'_FUNCTIONAL_STATUS_PATHS':frozenset(),'_FUNCTIONAL_MUTATION_PATHS':frozenset(),'state':SimpleNamespace(operator_note_document=lambda *a:(calls.append(a) or {'ok':True}))}
        exec(compile(ast.Module(body=[method],type_ignores=[]),'server.py','exec'),scope)
        fake=SimpleNamespace(path='/api/operator-note',headers={'Host':'127.0.0.1:1','Origin':'https://untrusted.invalid','Content-Type':'application/json'},send_json=responses.append,read_json=lambda:{'record_key':self.key,'text':'memo','revision':0})
        scope['do_POST'](fake);self.assertFalse(responses[-1]['ok']);self.assertFalse(calls)
        fake.headers['Origin']='http://127.0.0.1:1';scope['do_POST'](fake);self.assertTrue(responses[-1]['ok']);self.assertEqual(len(calls),1)
        fake.headers['Content-Type']='text/plain';scope['do_POST'](fake);self.assertFalse(responses[-1]['ok']);self.assertEqual(len(calls),1)

if __name__=='__main__':unittest.main()
