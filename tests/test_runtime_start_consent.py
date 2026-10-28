"""Offline consent, admission and rollback tests; transport is never called."""
from __future__ import annotations

import copy
import inspect
import os
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.safety_confirmation import SafetyConfirmationStore


class RuntimeStartConsentTests(unittest.TestCase):
    def setUp(self):
        self.original_state = copy.deepcopy(state.STATE)
        self.original_store = state.SAFETY_CONFIRMATIONS
        self.original_arm = state._REAL_ORDERS_PROCESS_ARMED
        self.original_authorities = set(state._REAL_ORDER_RUNTIME_AUTHORITIES)
        self.original_env = os.environ.get('LIVE_TRADER_ENABLE_REAL_ORDERS')
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.now = 1000.0
        state.SAFETY_CONFIRMATIONS = SafetyConfirmationStore(clock=lambda: self.now)
        state.disarm_real_orders_for_process_start()
        state.STATE.update(selected_deployment_id='', selected_strategy_id='', active_runtime_session_ids={}, mode='MONITOR')
        self.selection = dict(profile_id='crypto', mode='SMALL_LIVE', portfolio_id='', deployment_id='dep-one', strategy_id='strategy-one')
        self.scope = dict(runtimeScopeId='runtime-one', accountFingerprint='account-one', credentialConfigurationHash='credential-one')
        self.inputs = {'revision': 1}
        self.strategy = dict(strategy_id='strategy-one', deployment_id='dep-one', broker_id='binance', symbol='BTCUSDT')
        self.preflight = SimpleNamespace(snapshot_id='preflight-one')
        self.session = SimpleNamespace(session_id='session-one', mode='SMALL_LIVE', lifecycle='STARTING', deployment_id='dep-one', metadata={'runtimeScopeId':'runtime-one'})
        self.controller = Mock()
        self.controller.start.return_value = {'ok': False, 'reason': 'offline controller refusal'}
        self.controller.snapshot.return_value = {'runtimes': {}}
        self.governance = Mock()
        self.governance.latest_preflight_for_deployment.return_value = self.preflight
        self.governance.preflight_validity.return_value = {'valid': True}
        replacements = {
            '_safety_confirmation_identity': lambda _: {'fingerprint':'identity','suffix':'1234','provider':'BINANCE'},
            '_safety_environment_fingerprint': lambda:'environment-one',
            'portfolio_rows': lambda:[], 'strategy_rows': lambda *_: [self.strategy],
            '_operational_strategy': lambda *_,**__: self.strategy,
            'resolve_continuous_runtime_scope': lambda *_,**__: dict(self.scope),
            '_operational_manifest_inputs': lambda *_: dict(self.inputs),
            '_upbit_functional_durable_authority_open': lambda:False,
            '_binance_functional_durable_authority_open': lambda:False,
            'snapshot': lambda:{}, 'append_audit': lambda *_:None,
            'LIVE_CONTINUOUS_CONTROLLER': self.controller,
            'OPERATIONAL_GOVERNANCE': self.governance,
            'sync_runtime_profile_mode': lambda *_,**__:None,
            '_retire_superseded_runtime_session': lambda *_:True,
        }
        for key,value in replacements.items(): self.stack.enter_context(patch.object(state,key,value))
        self.prepare = self.stack.enter_context(patch.object(state,'_prepare_operational_runtime_session',side_effect=self.admit))
        self.start = inspect.unwrap(state.start_continuous_runtime)

    def tearDown(self):
        state.STATE.clear(); state.STATE.update(self.original_state)
        state.SAFETY_CONFIRMATIONS=self.original_store
        state._REAL_ORDER_RUNTIME_AUTHORITIES.clear(); state._REAL_ORDER_RUNTIME_AUTHORITIES.update(self.original_authorities)
        state._REAL_ORDERS_PROCESS_ARMED=self.original_arm
        if self.original_env is None: os.environ.pop('LIVE_TRADER_ENABLE_REAL_ORDERS',None)
        else: os.environ['LIVE_TRADER_ENABLE_REAL_ORDERS']=self.original_env

    def admit(self,*_):
        state.STATE['active_runtime_session_ids']['runtime-one']=self.session.session_id
        return self.session,'admitted offline'

    def consent(self):
        context,display,phrase=state.safety_confirmation_authoritative_context('RUNTIME_START', self.selection)
        issued=state.SAFETY_CONFIRMATIONS.issue(action='RUNTIME_START',context=context,display_context=display,expected_phrase=phrase)
        return {key:issued[key] for key in ('challengeId','token')} | {'typedPhrase':phrase}

    def execute(self,confirmation=None,**changes):
        selection=self.selection | changes
        return self.start(**selection,safety_confirmation=confirmation)

    def test_no_consent_cannot_create_session_arm_or_start(self):
        for value in (None,{}, {'confirmed':True}):
            self.assertFalse(self.execute(value)['ok'])
        self.prepare.assert_not_called(); self.controller.start.assert_not_called()
        self.assertFalse(state.real_orders_enabled())

    def test_exact_scope_credentials_manifest_and_preflight_replacement_invalidate(self):
        for key in ('accountFingerprint','credentialConfigurationHash'):
            with self.subTest(key=key):
                confirmation=self.consent(); prior=self.scope[key]; self.scope[key]='changed'
                self.assertFalse(self.execute(confirmation)['ok']); self.scope[key]=prior
        confirmation=self.consent(); self.inputs['revision']=2
        self.assertFalse(self.execute(confirmation)['ok']); self.inputs['revision']=1
        confirmation=self.consent(); self.preflight.snapshot_id='other-preflight'
        self.assertFalse(self.execute(confirmation)['ok']); self.preflight.snapshot_id='preflight-one'
        confirmation=self.consent()
        self.assertFalse(self.execute(confirmation,deployment_id='other-deployment')['ok'])
        self.prepare.assert_not_called(); self.controller.start.assert_not_called()

    def test_expired_start_and_consumed_confirmation_never_retry_controller(self):
        confirmation=self.consent(); self.now+=91
        self.assertFalse(self.execute(confirmation)['ok']); self.controller.start.assert_not_called()
        confirmation=self.consent()
        self.assertFalse(self.execute(confirmation)['ok'])
        self.assertEqual(self.controller.start.call_count,1)
        self.assertFalse(self.execute(confirmation)['ok'])
        self.assertEqual(self.controller.start.call_count,1)
        self.assertFalse(state.real_orders_enabled())

    def test_governance_admission_failure_never_arms(self):
        confirmation=self.consent(); self.prepare.side_effect=ValueError('preflight refused')
        self.assertFalse(self.execute(confirmation)['ok'])
        self.controller.start.assert_not_called(); self.assertFalse(state.real_orders_enabled())

    def test_only_admitted_session_is_armed_and_failed_start_rolls_back(self):
        observations=[]
        def controller(*_):
            observations.append((state.real_orders_enabled(),set(state._REAL_ORDER_RUNTIME_AUTHORITIES)))
            return {'ok':False,'reason':'fixture refusal'}
        self.controller.start.side_effect=controller
        self.assertFalse(self.execute(self.consent())['ok'])
        self.assertEqual(observations,[(True,{'session-one'})])
        self.assertFalse(state.real_orders_enabled())

    def test_exception_rolls_back_only_this_start_not_another_account(self):
        other=SimpleNamespace(session_id='other-account',mode='FULL_LIVE',lifecycle='RUNNING')
        state._arm_real_orders_for_runtime_start(other)
        self.controller.start.side_effect=RuntimeError('offline initialization failed')
        with self.assertRaisesRegex(RuntimeError,'initialization failed'): self.execute(self.consent())
        self.assertEqual(state._REAL_ORDER_RUNTIME_AUTHORITIES,{'other-account'})
        self.assertTrue(state.real_orders_enabled())
        state._revoke_runtime_order_authority('other-account'); self.assertFalse(state.real_orders_enabled())

    def test_functional_exclusion_wins_before_any_start_or_arm(self):
        confirmation=self.consent()
        with patch.object(state,'_upbit_functional_durable_authority_open',return_value=True):
            self.assertFalse(self.execute(confirmation)['ok'])
        self.prepare.assert_not_called(); self.controller.start.assert_not_called()
        self.assertFalse(state.real_orders_enabled())

    def test_successful_explicit_start_keeps_its_session_authority(self):
        self.controller.start.return_value={'ok':True,'reason':'offline supervisor admitted'}
        self.assertTrue(self.execute(self.consent())['ok'])
        self.assertEqual(state._REAL_ORDER_RUNTIME_AUTHORITIES,{'session-one'})
        self.assertTrue(state.real_orders_enabled())

    def test_binance_functional_exclusion_wins_before_start_arm(self):
        confirmation=self.consent()
        with patch.object(state,'_binance_functional_durable_authority_open',return_value=True):
            self.assertFalse(self.execute(confirmation)['ok'])
        self.prepare.assert_not_called(); self.controller.start.assert_not_called()
        self.assertFalse(state.real_orders_enabled())

    def test_confirmation_displays_exact_requested_account_not_global_selection(self):
        context,display,phrase=state.safety_confirmation_authoritative_context('RUNTIME_START',self.selection)
        self.assertEqual(context['accountFingerprint'],'account-one')
        self.assertEqual(display['provider'],'BINANCE')
        self.assertEqual(display['accountHint'],'••••-ONE')
        self.assertEqual(display['deploymentId'],'dep-one')
        self.assertEqual(phrase,'LIVE -ONE')

    def stop_fixture(self, *, cleanup=False, governance_error=False):
        other=SimpleNamespace(session_id='other-account',mode='FULL_LIVE',lifecycle='RUNNING')
        state._arm_real_orders_for_runtime_start(other)
        self.session.lifecycle='RUNNING'
        state._arm_real_orders_for_runtime_start(self.session)
        self.session.lifecycle='STOPPING'
        self.governance.get_runtime_session.return_value=self.session
        if governance_error: self.governance.get_runtime_session.side_effect=ValueError('offline journal unavailable')
        self.controller.snapshot.return_value={'runtimes':{'runtime-one':{'profileId':'crypto','running':True,'mode':'SMALL_LIVE','deploymentId':'dep-one'}}}
        self.controller.stop.return_value={'ok':False,'reason':'worker drain not acknowledged'}
        state._CONTINUOUS_STOP_SCOPE.plan={'cleanupKeys':{'runtime-one'} if cleanup else set()}
        try:
            with patch('live_trader.account_runtime_governance.select_session_bindings',return_value=[('runtime-one',self.session)]):
                return inspect.unwrap(state.stop_continuous_runtime)(runtime_id='runtime-one')
        finally:
            del state._CONTINUOUS_STOP_SCOPE.plan

    def test_failed_stop_revokes_only_failed_session_even_if_journal_unavailable(self):
        for governance_error in (False,True):
            with self.subTest(governance_error=governance_error):
                self.assertFalse(self.stop_fixture(governance_error=governance_error)['ok'])
                self.assertEqual(state._REAL_ORDER_RUNTIME_AUTHORITIES,{'other-account'})
                self.assertTrue(state.real_orders_enabled())
        self.governance.transition_runtime_session.assert_called_with('session-one','FAILED',actor='live_trader.desktop',event_type='RUNTIME_STOP_FAILED',payload={'runtimeId':'runtime-one','detail':'worker drain not acknowledged'})

    def test_exact_cleanup_pending_preserves_authority_until_cleanup_retirement(self):
        self.assertFalse(self.stop_fixture(cleanup=True)['ok'])
        self.assertEqual(state._REAL_ORDER_RUNTIME_AUTHORITIES,{'other-account','session-one'})

    def test_current_manifest_config_hash_does_not_depend_on_process_arm(self):
        # Execute the actual function; only external artifact/account reads are fake.
        import ast
        from pathlib import Path
        source=ast.parse(Path(state.__file__).read_text(encoding='utf-8'))
        node=next(n for n in source.body if isinstance(n,ast.FunctionDef) and n.name=='_operational_manifest_inputs')
        namespace=dict(vars(state))
        member={'strategyId':'strategy-one','symbol':'BTCUSDT','brokerId':'binance','artifactHash':'a'*64}
        namespace.update(_current_live_deployment_binding=lambda _:{'deploymentId':'dep-one'},
            _operational_portfolio_members=lambda _:('',{},[member]),_restore_account_scope=lambda _:{'account':'one'},
            broker_adapter_contract=lambda:[], real_orders_enabled=lambda:False)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-manifest','exec'),namespace)
        before=namespace['_operational_manifest_inputs'](self.strategy)
        namespace['real_orders_enabled']=lambda:True
        state._arm_real_orders_for_runtime_start(self.session)
        after=namespace['_operational_manifest_inputs'](self.strategy)
        self.assertEqual(before['config_hash'],after['config_hash'])
        self.assertEqual(before,after)


if __name__=='__main__': unittest.main()
