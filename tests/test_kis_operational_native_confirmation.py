import copy
import inspect
import threading
import unittest
from unittest.mock import patch
from live_trader.kis_operational_native_confirmation import (
    CONTEXT_SCHEMA, KisNativeOperationalStartConfirmations,
)
from live_trader.emergency_stop import DesktopEmergencyStopBridge

class Clock:
    def __init__(self, value): self.value=value
    def __call__(self): return self.value

class NativeStartTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock(1800000000.0); self.mono=Clock(1000.0)
        self.calls=[]
        def prompt(value):
            self.calls.append(value)
            return value.expected_phrase
        self.authority=KisNativeOperationalStartConfirmations(
            prompt=prompt,clock=self.clock,monotonic_clock=self.mono)
        self.context=dict(schemaVersion=CONTEXT_SCHEMA,scopeHash='a'*64,
            profileId='stock',mode='SMALL_LIVE',portfolioId='portfolio-1',
            deploymentId='deployment-1',strategyId='strategy-1',
            runtimeScopeId='scope-1',capitalCurrency='KRW',
            accountFingerprint='b'*64,credentialConfigurationHash='c'*64,
            wireAccountFingerprint='d'*64,wireCredentialConfigurationHash='e'*64,
            ownerEpochHash='f'*64)
    def mint(self):
        ticket=self.authority.arm(self.context); self.assertTrue(ticket['ok'],ticket)
        result=self.authority.request(ticket['nativeNonce']); self.assertTrue(result['ok'],result)
        return ticket,result['attestation']
    def test_native_only_single_use(self):
        ticket,proof=self.mint()
        self.assertEqual(self.calls[0].context,self.context)
        self.assertNotIn(self.calls[0].expected_phrase,str(ticket)+str(proof))
        self.assertTrue(self.authority.consume(proof,self.context)['ok'])
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_every_exact_scope_field_change_burns_proof(self):
        for key in self.context:
            if key=='schemaVersion':continue
            with self.subTest(key=key):
                _,proof=self.mint();changed=copy.deepcopy(self.context)
                changed[key]=('0'*64 if key.endswith('Hash') or 'Fingerprint' in key else 'other')
                self.assertFalse(self.authority.consume(proof,changed)['ok'])
                self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_tampered_proof_burns_nonce(self):
        _,proof=self.mint();tampered={**proof,'proofHmac':'0'*64}
        self.assertFalse(self.authority.consume(tampered,self.context)['ok'])
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_direct_post_style_fake_attestation(self):
        self.assertFalse(self.authority.consume({'oneUseNonce':'a'*43},self.context)['ok'])
    def test_unknown_nonce_does_not_open_prompt(self):
        self.assertFalse(self.authority.request('a'*43)['ok'])
        self.assertEqual(self.calls,[])
    def test_wall_expiry(self):
        _,proof=self.mint();self.clock.value+=45
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_monotonic_expiry_despite_frozen_wall(self):
        _,proof=self.mint();self.mono.value+=45
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_clock_rollback(self):
        _,proof=self.mint();self.clock.value-=1
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_monotonic_rollback(self):
        _,proof=self.mint();self.mono.value-=1
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_same_scope_rearm_revokes_prior_proof(self):
        _,proof=self.mint();self.authority.arm(self.context)
        self.assertFalse(self.authority.consume(proof,self.context)['ok'])
    def test_revoke_inflight_native_window(self):
        entered=threading.Event();release=threading.Event()
        def prompt(value):
            entered.set();release.wait(2);return value.expected_phrase
        self.authority._prompt=prompt
        ticket=self.authority.arm(self.context);result=[]
        thread=threading.Thread(target=lambda:result.append(self.authority.request(ticket['nativeNonce'])))
        thread.start();self.assertTrue(entered.wait(1))
        self.authority.revoke_scope(self.context['runtimeScopeId'])
        release.set();thread.join(2)
        self.assertFalse(result[0]['ok'])
    def test_expiry_during_native_prompt(self):
        def prompt(value):
            self.mono.value+=46;return value.expected_phrase
        self.authority._prompt=prompt
        ticket=self.authority.arm(self.context)
        self.assertFalse(self.authority.request(ticket['nativeNonce'])['ok'])
    def test_cancel_burns_prompt_ticket(self):
        self.authority._prompt=lambda value:None
        ticket=self.authority.arm(self.context)
        self.assertFalse(self.authority.request(ticket['nativeNonce'])['ok'])
        self.assertFalse(self.authority.request(ticket['nativeNonce'])['ok'])
    def test_cross_instance_proof_rejected(self):
        _,proof=self.mint()
        other=KisNativeOperationalStartConfirmations(prompt=lambda value:value.expected_phrase,
                    clock=self.clock,monotonic_clock=self.mono)
        self.assertFalse(other.consume(proof,self.context)['ok'])
    def test_process_fork_binding(self):
        ticket=self.authority.arm(self.context)
        with patch('live_trader.kis_operational_native_confirmation.os.getpid',return_value=1):
            self.assertFalse(self.authority.request(ticket['nativeNonce'])['ok'])
    def test_malformed_scope_rejected_without_prompt(self):
        for value in ({},{**self.context,'extra':1},{**self.context,'capitalCurrency':'USDT'},
                      {**self.context,'profileId':'crypto'},{**self.context,'mode':'MONITOR'},
                      {**self.context,'deploymentId':'unsafe\nlabel'}):
            self.assertFalse(self.authority.arm(value)['ok'])
        self.assertEqual(self.calls,[])
    def test_capacity_is_bounded(self):
        for i in range(16):
            self.assertTrue(self.authority.arm({**self.context,'runtimeScopeId':f'scope-{i}'})['ok'])
        self.assertFalse(self.authority.arm({**self.context,'runtimeScopeId':'scope-overflow'})['ok'])
    def test_returned_context_is_detached(self):
        ticket=self.authority.arm(self.context)
        ticket['context']['capitalCurrency']='USD'
        proof=self.authority.request(ticket['nativeNonce'])['attestation']
        self.assertTrue(self.authority.consume(proof,self.context)['ok'])
    def test_bridge_exposes_only_opaque_request(self):
        bridge=DesktopEmergencyStopBridge(kis_operational_start_confirmations=self.authority)
        self.assertEqual(list(inspect.signature(bridge.request_kis_operational_start_confirmation).parameters),['native_nonce'])
        self.assertFalse(hasattr(bridge,'arm'));self.assertFalse(hasattr(bridge,'consume'))
        ticket=self.authority.arm(self.context)
        self.assertTrue(bridge.request_kis_operational_start_confirmation(ticket['nativeNonce'])['ok'])

if __name__=='__main__':unittest.main()

