import copy
import tempfile
import unittest
from pathlib import Path

from live_trader.kis_scoped_operational_authority import KisScopedAuthorityError, KisScopedOperationalAuthority


def context():
    return dict(schemaVersion='kis-scoped-manual-start-context/v1', scopeHash='a'*64,
        profileId='stock', mode='SMALL_LIVE', portfolioId='portfolio-alpha',
        deploymentId='deployment-alpha', strategyId='strategy-alpha', runtimeScopeId='runtime-alpha',
        capitalCurrency='KRW', accountFingerprint='b'*64, credentialConfigurationHash='c'*64,
        wireAccountFingerprint='d'*64, wireCredentialConfigurationHash='e'*64, ownerEpochHash='f'*64)


class ScopedStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.now=1000.0
        self.store=KisScopedOperationalAuthority(Path(self.directory.name)/'authority.sqlite3', clock=lambda:self.now)
        self.context=context()
        self.proof=dict(routeQuiescent=True,specializedClosureInstalled=True,sameAccountLeaseHeld=True,reconciliationClear=True)

    def evidence(self):
        return dict(schemaVersion='kis-operational-wire-evidence/v1',preflightHash='1'*64,preflightExpiresAt=self.now+100,
            nativeTicketHash='2'*64,nativeAsOf=self.now,boundaryHash='3'*64,boundaryStartsAt=self.now)

    def pending(self):
        self.store.establish_exclusion(self.context,proof=self.proof)
        return self.store.reserve(self.context,session_id='session-a',reservation_id='reservation-a',expires_at=1100)

    def active(self):
        self.pending()
        self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=True,session_running=True)

    def test_missing_unknown_and_pending_are_not_authority(self):
        self.assertEqual(self.store.exclusion('d'*64), {'status':'UNKNOWN'})
        self.assertFalse(self.store.path.exists())
        with self.assertRaises(KisScopedAuthorityError): self.store.authorize_consent('session-a',context=self.context)
        with self.assertRaises(KisScopedAuthorityError): self.store.reserve(self.context,session_id='s',reservation_id='r',expires_at=1100)
        self.pending()
        with self.assertRaises(KisScopedAuthorityError): self.store.authorize_consent('session-a',context=self.context)

    def test_positive_exclusion_requires_every_exact_proof(self):
        for key in self.proof:
            proof=copy.deepcopy(self.proof); proof[key]=False
            with self.subTest(key=key),self.assertRaises(KisScopedAuthorityError): self.store.establish_exclusion(self.context,proof=proof)
        with self.assertRaises(KisScopedAuthorityError): self.store.establish_exclusion(self.context,proof={**self.proof,'graphAbsent':True})

    def test_activation_requires_finalized_reservation_and_exact_running(self):
        for finalized,running in [(False,False),(False,True),(True,False)]:
            self.pending()
            with self.assertRaises(KisScopedAuthorityError):
                self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=finalized,session_running=running)
            with self.assertRaises(KisScopedAuthorityError): self.store.authorize_consent('session-a',context=self.context)
            # Test a new immutable session next time; session IDs never recycle.
            self.store.path.unlink()

    def test_cross_identity_currency_revision_and_paper_policy_are_rejected(self):
        self.active()
        for key in self.context:
            if key in {'schemaVersion','profileId'}: continue
            changed=copy.deepcopy(self.context)
            changed[key]=('0'*64 if key.endswith('Hash') or key.endswith('Fingerprint') else 'other')
            if key=='mode': changed[key]='FULL_LIVE'
            if key=='capitalCurrency': changed[key]='USD'
            with self.subTest(key=key),self.assertRaises(KisScopedAuthorityError): self.store.authorize_consent('session-a',context=changed)
        self.assertTrue(self.store.authorize_consent('session-a',context=self.context)['consentId'])

    def test_expiry_stop_and_start_stop_race_fail_closed(self):
        self.pending(); self.store.revoke(session_id='session-a',reason='stop-before-start-returned')
        with self.assertRaises(KisScopedAuthorityError): self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=True,session_running=True)
        self.store.path.unlink();self.pending();self.now=1100
        with self.assertRaises(KisScopedAuthorityError): self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=True,session_running=True)
        self.store.path.unlink();self.now=1000;self.active();self.now=999
        with self.assertRaises(KisScopedAuthorityError): self.store.authorize_consent('session-a',context=self.context)

    def test_wire_one_use_is_durable_and_never_grants_cancel(self):
        self.active()
        wire=dict(operation='PLACE_ORDER',accountFingerprint='d'*64,credentialConfigurationHash='e'*64,payloadHash='1'*64,endpoint='/uapi/domestic-stock/v1/trading/order-cash',claimId='claim-a',ownedOrderKey={})
        self.store.consume_wire('session-a',context=self.context,intent=wire,dispatch_key='2'*64,fresh_evidence=self.evidence())
        reopened=KisScopedOperationalAuthority(self.store.path,clock=lambda:self.now)
        with self.assertRaises(KisScopedAuthorityError): reopened.consume_wire('session-a',context=self.context,intent=wire,dispatch_key='2'*64,fresh_evidence=self.evidence())
        with self.assertRaises(KisScopedAuthorityError): reopened.consume_wire('session-a',context=self.context,intent={**wire,'claimId':'new-claim','payloadHash':'3'*64},dispatch_key='2'*64,fresh_evidence=self.evidence())
        for key,value in [('operation','CANCEL_ORDER'),('accountFingerprint','0'*64),('credentialConfigurationHash','0'*64)]:
            with self.subTest(key=key),self.assertRaises(KisScopedAuthorityError): reopened.consume_wire('session-a',context=self.context,intent={**wire,key:value},dispatch_key='4'*64,fresh_evidence=self.evidence())

    def test_short_wire_grants_require_new_fresh_preflight_and_native_truth_for_later_sell(self):
        self.active()
        initial=self.evidence()
        first=self.store.authorize('session-a',context=self.context,fresh_evidence=initial)
        self.assertLessEqual(first['expiresAt']-self.now,15)
        self.now+=3600
        # Human consent survives; an old preflight or native ticket does not.
        self.assertTrue(self.store.authorize_consent('session-a',context=self.context)['consentId'])
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize('session-a',context=self.context,fresh_evidence=initial)
        fresh=self.evidence();fresh['preflightHash']='5'*64;fresh['nativeTicketHash']='6'*64
        second=self.store.authorize('session-a',context=self.context,fresh_evidence=fresh)
        self.assertNotEqual(first['permitId'],second['permitId'])
        self.store.revoke(session_id='session-a',reason='user-stop')
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize('session-a',context=self.context,fresh_evidence=fresh)

    def test_revoke_all_is_durable_and_copied_permit_is_not_a_grant(self):
        permit=self.pending(); self.store.activate('session-a',context=self.context,reservation_id='reservation-a',reservation_finalized=True,session_running=True)
        self.store.revoke(reason='kill')
        self.assertEqual(permit.session_id,'session-a')
        with self.assertRaises(KisScopedAuthorityError): KisScopedOperationalAuthority(self.store.path,clock=lambda:self.now).authorize_consent(permit.session_id,context=permit.context)


if __name__=='__main__': unittest.main()
