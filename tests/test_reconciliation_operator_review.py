from __future__ import annotations
import ast
import copy
from contextlib import closing
from datetime import datetime, timezone
from email.message import Message
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlparse

from live_trader.operator_review import OperatorReviewStore
from live_trader.reconciliation_review import acknowledge_review, configured_review_scopes, review_candidates, review_document
from live_trader.functional_http_session import FunctionalHttpSessionAuthority, FunctionalHttpSessionError, APP_SESSION_COOKIE, CSRF_HEADER
from live_trader.status_observation import broker_account_observation, completed_broker_poll_observations, broker_execution_read_coverage

ROOT=Path(__file__).resolve().parents[1]
SCOPES={'upbit':'a'*64,'binance':'b'*64}

def fixture():
    account={'broker_id':'upbit','account':'fixture-account','currency':'KRW','program_currency':'KRW',
        'program_source':'event_poll','program_updated_at':'2026-07-30 02:30:36',
        'program_cash_value':100,'broker_cash_value':120,'status':'mismatch','comparison_issue':'',
        'observed_at':'2026-10-29T16:00:00+09:00','lookup_status':'stale','observation_source':'BROKER_ACCOUNT_SNAPSHOT'}
    return {'reconciliation':{'summary':{'last_run':'2026-10-29T07:00:00Z','account_count':1,'position_count':0,
        'mismatch_count':1,'api_required_count':0,'error_count':0},'accounts':[account],'positions':[],'errors':[]},
        'program_ledger':{'state':{'last_baseline':'','last_baseline_source':''},'fill_event_count':0,'other_event_count':0,
        'snapshot_event_count':2,'positions':[]}, 'broker_observations':{'account_review_scopes':SCOPES.copy(),'successful_position_brokers':['upbit'],
        'position_observations':{'upbit':{'observedAt':'2026-10-29T07:00:00Z'}},'positions':[]}}

def request_for(context, scopes=SCOPES, **changes):
    candidate=review_candidates(context,scopes)[0]
    return {'fingerprint':candidate['fingerprint'],'reason':'DEPOSIT','action':'ACKNOWLEDGE','confirmed':True,**changes}

def state_fn(name, values):
    node=next(n for n in ast.parse((ROOT/'live_trader/state.py').read_text(encoding='utf-8')).body
              if isinstance(n,ast.FunctionDef) and n.name==name)
    scope={'__package__':'live_trader','os':os,'sqlite3':sqlite3,'Any':Any,**values}
    exec(compile(ast.Module(body=[node],type_ignores=[]),'state-review-test','exec'),scope)
    return scope[name]

class ReconciliationReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'nested/review.sqlite3';self.store=OperatorReviewStore(self.path)
        self.data=fixture()

    def test_snapshot_does_not_create_store_or_invent_authority(self):
        result=review_document(self.data,SCOPES,self.store)
        self.assertEqual(len(result['items']),1);self.assertFalse(self.path.exists())
        self.assertFalse(result['cashFlowVerified']);self.assertFalse(result['executionAuthorityGranted'])
        self.assertNotIn('api_connected',self.data)  # Backend does not have renderer flag.

    def test_ack_is_append_only_idempotent_durable_and_never_modifies_ledger(self):
        original=copy.deepcopy(self.data);payload=request_for(self.data)
        self.assertFalse(acknowledge_review(payload,self.data,SCOPES,self.store)['idempotent'])
        restarted=OperatorReviewStore(self.path)
        self.assertTrue(acknowledge_review(payload,self.data,SCOPES,restarted)['idempotent'])
        view=review_document(self.data,SCOPES,restarted)
        self.assertTrue(view['items'][0]['acknowledged']);self.assertEqual(self.data,original)
        payload['action']='REOPEN';acknowledge_review(payload,self.data,SCOPES,restarted)
        self.assertFalse(review_document(self.data,SCOPES,restarted)['items'][0]['acknowledged'])
        with closing(sqlite3.connect(self.path)) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM balance_reviews').fetchone()[0],2)

    def test_same_facts_new_observation_time_keep_ack(self):
        key=request_for(self.data)['fingerprint'];acknowledge_review(request_for(self.data),self.data,SCOPES,self.store)
        self.data['reconciliation']['summary']['last_run']='2026-10-29T08:00:00Z'
        self.data['reconciliation']['accounts'][0]['observed_at']='2026-10-29T17:00:00+09:00'
        self.assertEqual(review_candidates(self.data,SCOPES)[0]['fingerprint'],key)
        self.assertTrue(review_document(self.data,SCOPES,self.store)['items'][0]['acknowledged'])

    def test_changed_scope_amount_base_or_side_rejects_stale_action(self):
        payload=request_for(self.data)
        for field,value in [('account','different-account'),('broker_cash_value',121),('program_cash_value',101),
            ('program_updated_at','2026-07-31 02:30:36'),('position_side','OTHER')]:
            changed=copy.deepcopy(self.data);changed['reconciliation']['accounts'][0][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):acknowledge_review(payload,changed,SCOPES,self.store)
        with self.assertRaises(ValueError):acknowledge_review(payload,self.data,{'upbit':'c'*64},self.store)
        self.assertFalse(self.path.exists())

    def test_unknown_error_future_and_malformed_observation_keep_visible(self):
        for status in ['unknown','error',None]:
            item=copy.deepcopy(self.data);item['reconciliation']['accounts'][0]['lookup_status']=status
            self.assertEqual(review_candidates(item,SCOPES),[])
        for key,value in [('errors',None),('accounts',{}),('positions',None)]:
            item=copy.deepcopy(self.data);item['reconciliation'][key]=value
            self.assertEqual(review_candidates(item,SCOPES),[])
        for key,value in [('account_count',2),('error_count',False),('mismatch_count',0),('api_required_count',None)]:
            item=copy.deepcopy(self.data);item['reconciliation']['summary'][key]=value
            self.assertEqual(review_candidates(item,SCOPES),[])
        item=copy.deepcopy(self.data);item['reconciliation']['errors']=[{'detail':'failed'}];item['reconciliation']['summary']['error_count']=1
        self.assertEqual(review_candidates(item,SCOPES),[])
        item=copy.deepcopy(self.data);item['reconciliation']['accounts'][0]['observed_at']='2099-01-01T00:00:00Z'
        self.assertEqual(review_candidates(item,SCOPES),[])

    def test_time_offsets_compare_instants_and_legacy_naive_is_kst(self):
        row=self.data['reconciliation']['accounts'][0]
        row['program_updated_at']='2026-10-29T16:00:00+09:00';row['observed_at']='2026-10-29T07:00:00Z'
        self.assertEqual(len(review_candidates(self.data,SCOPES)),1)
        row['program_updated_at']='2026-10-29 16:00:00'
        self.assertEqual(len(review_candidates(self.data,SCOPES)),1)
        row['program_updated_at']='2026-10-29T08:00:00Z'
        self.assertEqual(review_candidates(self.data,SCOPES),[])

    def test_actual_execution_unknown_counts_and_real_baseline_cannot_ack(self):
        payload=request_for(self.data)
        for source in ['execution_fill','approved_baseline','missing']:
            item=copy.deepcopy(self.data);item['reconciliation']['accounts'][0]['program_source']=source
            self.assertEqual(review_candidates(item,SCOPES),[])
        for field,value in [('fill_event_count',1),('fill_event_count',None),('other_event_count',1),('snapshot_event_count',False)]:
            item=copy.deepcopy(self.data);item['program_ledger'][field]=value
            with self.assertRaises(ValueError):acknowledge_review(payload,item,SCOPES,self.store)
        self.data['program_ledger']['state']['last_baseline']='2026-10-29T08:00:00Z'
        self.assertEqual(review_candidates(self.data,SCOPES),[])

    def test_unmanaged_spot_holding_needs_actual_position_read(self):
        account=self.data['reconciliation']['accounts'][0];account['status']='pass';account['broker_cash_value']=100
        self.data['reconciliation']['positions']=[{'broker_id':'upbit','currency':'KRW','symbol':'KRW-XRP','position_side':'',
            'program_source':'missing','program_qty_value':0.0,'broker_qty_value':5.0,'status':'mismatch'}]
        self.data['reconciliation']['summary']['position_count']=1
        self.data['broker_observations']['positions']=[{'broker_id':'upbit','symbol':'KRW-XRP','broker_qty':5.0}]
        items=review_candidates(self.data,SCOPES);self.assertEqual(len(items),1)
        self.assertEqual(items[0]['classification'],'UNMANAGED_SPOT_INVENTORY')
        self.data['broker_observations']['successful_position_brokers']=[]
        self.assertEqual(review_candidates(self.data,SCOPES),[])

    def test_duplicate_identity_and_nan_reject(self):
        self.data['reconciliation']['accounts'].append(copy.deepcopy(self.data['reconciliation']['accounts'][0]))
        self.data['reconciliation']['summary'].update(account_count=2,mismatch_count=2)
        self.assertEqual(review_candidates(self.data,SCOPES),[])
        self.data=fixture();self.data['reconciliation']['accounts'][0]['broker_cash_value']=float('nan')
        self.assertEqual(review_candidates(self.data,SCOPES),[])

    def test_explicit_input_only_and_no_extra_capabilities(self):
        valid=request_for(self.data)
        for changes in [{'confirmed':False},{'reason':'AUTO'},{'reason':[]},{'action':{}},{'action':'APPROVE'},{'account':'injected'},{'fingerprint':'bad'}]:
            with self.assertRaises(ValueError):acknowledge_review({**valid,**changes},self.data,SCOPES,self.store)
        self.assertFalse(self.path.exists())

    def test_existing_store_read_never_creates_review_table(self):
        self.path.parent.mkdir(parents=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE unrelated(id INTEGER)');db.commit()
        before=self.path.read_bytes();self.assertEqual(self.store.balance_reviews(),[]);self.assertEqual(self.path.read_bytes(),before)

    def test_store_failure_preserves_all_differences(self):
        def failure():raise sqlite3.OperationalError('unavailable')
        result=review_document(self.data,SCOPES,SimpleNamespace(balance_reviews=failure))
        self.assertEqual(result['items'],[]);self.assertIn('error',result)

    def test_configured_scope_separates_brokers_and_rotated_identity(self):
        values={'BINANCE_API_KEY':'fixture','UPBIT_ACCESS_KEY':'other'}
        scopes=configured_review_scopes(values);self.assertNotEqual(scopes['binance'],scopes['binance-futures'])
        self.assertNotIn('kis',scopes);self.assertNotIn('fixture',str(scopes))
        self.assertNotEqual(scopes['upbit'],configured_review_scopes({**values,'UPBIT_ACCESS_KEY':'changed'})['upbit'])

    def test_observed_account_configuration_must_match_current_scope(self):
        self.data['broker_observations']['account_review_scopes']={}
        self.assertEqual(review_candidates(self.data,SCOPES),[])
        self.data['broker_observations']['account_review_scopes']={'upbit':'c'*64}
        self.assertEqual(review_candidates(self.data,SCOPES),[])

    def test_normal_account_read_binds_pre_read_identity_without_additional_reads(self):
        env={'UPBIT_ACCESS_KEY':'before-key'};state={};calls=[]
        expected=configured_review_scopes(env)
        def synthetic_read(router, brokers):
            calls.append(tuple(brokers));env['UPBIT_ACCESS_KEY']='after-key'
            return {'upbit':{'accounts':[{'broker_id':'upbit'}],'positions':[]}}
        fn=state_fn('refresh_broker_reconciliation',{'STATE':state,'os':SimpleNamespace(environ=env),
            'datetime':datetime,'LiveBrokerRouter':lambda:object(),'fetch_broker_snapshots':synthetic_read,
            'refresh_account_risk_budget':lambda rows:None})
        result=fn();self.assertEqual(result['account_review_scopes'],expected)
        self.assertNotEqual(result['account_review_scopes'],configured_review_scopes(env));self.assertEqual(len(calls),1)

    def test_manual_execution_refresh_merge_propagates_pre_fetch_identity_only(self):
        for changed_in_flight in (False,True):
            with self.subTest(changed=changed_in_flight),patch.dict(os.environ,{'UPBIT_ACCESS_KEY':'fixture-key'},clear=True):
                expected=configured_review_scopes(os.environ)
                state={'program_ledger':{},'new_entries_blocked':True}
                ns={'datetime':datetime,'timezone':timezone,'time':time,'hashlib':hashlib,'json':json,
                    'completed_broker_poll_observations':completed_broker_poll_observations,'broker_execution_read_coverage':broker_execution_read_coverage,
                    'STATE':state,'LiveBrokerRouter':Mock(),'LIVE_EXECUTION_STREAMS':Mock(),'PROGRAM_LEDGER':Mock()}
                ns['LIVE_EXECUTION_STREAMS'].drain.return_value=[]
                ns['PROGRAM_LEDGER'].existing_execution_event_ids.return_value=set()
                ns['PROGRAM_LEDGER'].record_execution_events.return_value=0
                for name in ['observe_execution_stream_connectivity','mark_broker_snapshot_failure','record_connectivity_state',
                    'stable_broker_snapshot_digest','record_new_execution_traces','append_audit']:
                    ns[name]=Mock()
                for name,value in {'broker_snapshot_poll_interval_seconds':60,'broker_snapshot_is_due':True,'mark_broker_snapshot_success':True,
                    'apply_execution_events_to_local_orders':0,'kis_order_truth_snapshot':{},'notify_new_live_fills':0,
                    'should_append_execution_sync_audit':False,'program_ledger_snapshot':{},'execution_event_snapshot':{},
                    'automatic_live_promotion_sweep':[]}.items():ns[name]=Mock(return_value=value)
                merge=state_fn('merge_broker_reconciliation_cache',{'STATE':state,'datetime':datetime,'timezone':timezone,'refresh_account_risk_budget':Mock()})
                ns['merge_broker_reconciliation_cache']=merge
                account={'broker_id':'upbit','account':'fixture-account','currency':'KRW','broker_cash':120}
                def fetch(router,brokers):
                    if changed_in_flight:os.environ['UPBIT_ACCESS_KEY']='changed-key'
                    return {'upbit':{'events':[],'accounts':[account],'positions':[],'source':'upbit_accounts_poll'}}
                ns['fetch_broker_snapshots']=Mock(side_effect=fetch);ns['deduplicate_execution_events']=lambda events:events
                state_fn('_poll_execution_events_unlocked',ns)('upbit',force_snapshot=True,include_snapshot=False,display_only=True)
                cache=state['broker_reconciliation']
                self.assertEqual(cache['account_review_scopes'],expected);self.assertTrue(state['new_entries_blocked'])
                ns['fetch_broker_snapshots'].assert_called_once();ns['automatic_live_promotion_sweep'].assert_not_called()
                context=fixture();context['broker_observations']=cache
                context['reconciliation']['accounts'][0].update(broker_account_observation(cache,'upbit'))
                self.assertEqual(len(review_candidates(context,configured_review_scopes(os.environ))),0 if changed_in_flight else 1)

    def test_failed_or_unbound_new_account_read_clears_only_review_scope(self):
        state={'broker_reconciliation':{'accounts':[],'positions':[],'errors':[],'account_review_scopes':SCOPES.copy()}}
        merge=state_fn('merge_broker_reconciliation_cache',{'STATE':state,'datetime':datetime,'timezone':timezone,'refresh_account_risk_budget':Mock()})
        merge([],[],set(),[{'broker_id':'upbit','detail':'timeout'}],account_review_scopes=SCOPES)
        self.assertNotIn('upbit',state['broker_reconciliation']['account_review_scopes'])
        self.assertEqual(state['broker_reconciliation']['account_review_scopes']['binance'],SCOPES['binance'])
        merge([{'broker_id':'binance'}],[],{'binance'},[],account_review_scopes=None)
        self.assertEqual(state['broker_reconciliation']['account_review_scopes'],{})

    def test_private_context_reads_only_caches_and_keeps_raw_account_server_side(self):
        state={'broker_reconciliation':self.data['broker_observations'],'kill_switch':True}
        fn=state_fn('reconciliation_review_context',{'STATE':state,'reconciliation_snapshot':lambda:self.data['reconciliation'],
            'program_ledger_snapshot':lambda:self.data['program_ledger']})
        result=fn();self.assertNotIn('api_connected',result)
        self.assertEqual(result['reconciliation']['accounts'][0]['account'],'fixture-account')
        result['broker_observations']['positions'].append({})
        self.assertEqual(state['broker_reconciliation']['positions'],[]);self.assertTrue(state['kill_switch'])

    def test_state_action_exact_candidate_only_and_no_broker_or_guard_mutation(self):
        scopes=configured_review_scopes({'UPBIT_ACCESS_KEY':'fixture-key'})
        self.data['broker_observations']['account_review_scopes']=scopes
        payload=request_for(self.data,scopes)
        before=copy.deepcopy(self.data);calls=[]
        fake_env=SimpleNamespace(environ={'UPBIT_ACCESS_KEY':'fixture-key'})
        fn=state_fn('review_reconciliation_difference',{'os':fake_env,'reconciliation_review_context':lambda:self.data,
            'OPERATOR_REVIEW_STORE':self.store,'snapshot':lambda:(calls.append('cached-display') or {'reconciliation':self.data['reconciliation']})})
        result=fn(payload);self.assertTrue(result['ok']);self.assertEqual(calls,['cached-display']);self.assertEqual(self.data,before)
        self.assertFalse(result['executionAuthorityGranted']);self.assertFalse(result['cashFlowVerified'])
        self.assertFalse(fn({**payload,'fingerprint':'0'*64})['ok']);self.assertEqual(len(calls),1)

    def test_private_identity_roundtrip_document_ack_snapshot_reopen(self):
        scopes=configured_review_scopes({'UPBIT_ACCESS_KEY':'fixture-key'})
        self.data['broker_observations']['account_review_scopes']=scopes
        def public_snapshot():
            public=copy.deepcopy(self.data)
            public['reconciliation_review']=review_document(self.data,scopes,self.store)
            public['reconciliation']['accounts'][0]['account']='***'
            public.pop('broker_observations')
            return public
        displayed=public_snapshot()['reconciliation_review']['items'][0]
        payload={'fingerprint':displayed['fingerprint'],'reason':'DEPOSIT','action':'ACKNOWLEDGE','confirmed':True}
        fn=state_fn('review_reconciliation_difference',{'os':SimpleNamespace(environ={'UPBIT_ACCESS_KEY':'fixture-key'}),
            'reconciliation_review_context':lambda:self.data,'OPERATOR_REVIEW_STORE':self.store,'snapshot':public_snapshot})
        result=fn(payload);self.assertTrue(result['ok']);self.assertEqual(result['snapshot']['reconciliation']['accounts'][0]['account'],'***')
        self.assertTrue(result['snapshot']['reconciliation_review']['items'][0]['acknowledged'])
        self.assertEqual(result['snapshot']['reconciliation']['summary']['mismatch_count'],1)
        payload['action']='REOPEN';result=fn(payload)
        self.assertTrue(result['ok']);self.assertFalse(result['snapshot']['reconciliation_review']['items'][0]['acknowledged'])

    def test_http_same_origin_json_exact_route_boundary(self):
        tree=ast.parse((ROOT/'live_trader/server.py').read_text(encoding='utf-8'))
        cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LiveTraderHandler')
        node=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='do_POST')
        calls=[];replies=[]
        scope={'urlparse':urlparse,'sqlite3':sqlite3,'_FUNCTIONAL_STATUS_PATHS':frozenset(),'_FUNCTIONAL_MUTATION_PATHS':frozenset({'/api/reconciliation/review'}),
            'state':SimpleNamespace(review_reconciliation_difference=lambda p:(calls.append(p) or {'ok':True}))}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'review-handler-test','exec'),scope)
        authority=FunctionalHttpSessionAuthority.mint(host='127.0.0.1',port=18795)
        headers=Message();headers['Host']=authority.expected_host_header;headers['Origin']=authority.expected_origin
        headers['Content-Type']='application/json'
        def authorize(*,require_origin):
            try:authority.assert_request(headers=headers,peer_host='127.0.0.1',require_origin=require_origin)
            except FunctionalHttpSessionError:
                replies.append({'ok':False});return False
            return True
        request=SimpleNamespace(path='/api/reconciliation/review',headers=headers,_authorize_functional_http=authorize,
            send_json=replies.append,read_json=lambda:request_for(self.data))
        scope['do_POST'](request);self.assertFalse(replies[-1]['ok']);self.assertEqual(calls,[])
        headers['Cookie']=f'{APP_SESSION_COOKIE}={authority.app_session_token}';headers[CSRF_HEADER]='wrong'
        scope['do_POST'](request);self.assertFalse(replies[-1]['ok']);self.assertEqual(calls,[])
        headers.replace_header(CSRF_HEADER,authority.csrf_token)
        scope['do_POST'](request);self.assertTrue(replies[-1]['ok']);self.assertEqual(len(calls),1)
        request.path+='?scope=injected';scope['do_POST'](request);self.assertFalse(replies[-1]['ok']);self.assertEqual(len(calls),1)
        request.path='/api/reconciliation/review';request.headers.replace_header('Content-Type','text/plain')
        scope['do_POST'](request);self.assertFalse(replies[-1]['ok']);self.assertEqual(len(calls),1)

    def test_native_review_route_registered_on_client_and_server(self):
        tree=ast.parse((ROOT/'live_trader/server.py').read_text(encoding='utf-8'))
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='_FUNCTIONAL_MUTATION_PATHS' for t in n.targets))
        self.assertIn('/api/reconciliation/review',ast.literal_eval(assignment.value.args[0]))
        source=(ROOT/'src/api.js').read_text(encoding='utf-8')
        block=source.split('const FUNCTIONAL_HTTP_MUTATION_PATHS = new Set([',1)[1].split(']);',1)[0]
        self.assertIn('"/api/reconciliation/review"',block)

if __name__=='__main__':unittest.main()
