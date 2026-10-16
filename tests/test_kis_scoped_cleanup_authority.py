from contextlib import contextmanager, closing
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import os
import tempfile
import unittest
from unittest.mock import patch

from live_trader.kis_scoped_operational_authority import KisScopedAuthorityError, KisScopedOperationalAuthority
from live_trader.kis_scoped_cleanup_authority import KisScopedCleanupStore, dispatch_business_hash, CLEANUP_POLICY
from live_trader.kis_scoped_cleanup_adapter import build_kis_scoped_cleanup_request
from live_trader import kis_order_authority as auth
from test_kis_scoped_operational_authority import context


class CleanupStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.wall, self.mono = 2000., 1000.
        self.ops = KisScopedOperationalAuthority(Path(self.temp.name)/'authority.sqlite3', clock=lambda:self.wall, monotonic=lambda:self.mono)
        self.context = context()
        self.ops.establish_exclusion(self.context, proof={key:True for key in ('routeQuiescent','specializedClosureInstalled','sameAccountLeaseHeld','reconciliationClear')})
        self.ops.reserve(self.context, session_id='session-1', reservation_id='reservation-1', expires_at=self.wall+200)
        self.ops.record_native_cleanup_policy('session-1',context=self.context,policy=CLEANUP_POLICY)
        self.ops.activate('session-1', context=self.context, reservation_id='reservation-1', reservation_finalized=True, session_running=True)
        self.store = KisScopedCleanupStore(self.ops)
        self.orders = [{'dispatchKey':'dispatch-1','ownershipHash':'a'*64}]
        self.proof = {'orders':[{'dispatchKey':'dispatch-1','businessHash':'a'*64,'state':'WORKING','remainingQuantity':'2'}]}
        self.row = self.store.reserve('session-1', self.context, self.orders)
        self.key = self.row['cleanup_id']
        self.intent = {'operation':'SCOPED_CANCEL_US','accountFingerprint':self.context['wireAccountFingerprint'],
            'credentialConfigurationHash':self.context['wireCredentialConfigurationHash'],'payloadHash':'b'*64}

    def bind(self): self.store.bind_truth(self.key, self.proof, expires_at=self.wall+15,ownership=self.orders)

    def test_reservation_durably_revokes_place_before_drain_and_blocks_restart(self):
        with self.assertRaises(KisScopedAuthorityError):self.ops.authorize_consent('session-1',context=self.context)
        self.assertEqual(self.store.pending_for_account(self.context['accountFingerprint'])[0]['sessionId'],'session-1')
        self.assertEqual(self.store.pending_for_account('c'*64),[])
        self.assertFalse(self.store.public_status()['scopes'][0]['requiresReconciliation'] is False)
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize(self.key,context=self.context,dispatch_key='dispatch-1')

    def test_fresh_exact_cancel_is_one_use_even_after_new_truth_or_reopen(self):
        self.bind();self.store.consume(self.key,context=self.context,dispatch_key='dispatch-1',intent=self.intent)
        self.wall+=1;self.mono+=1;self.bind()
        reopened=KisScopedCleanupStore(self.ops)
        with self.assertRaises(KisScopedAuthorityError):reopened.consume(self.key,context=self.context,dispatch_key='dispatch-1',intent={**self.intent,'payloadHash':'f'*64})
        self.assertEqual(reopened.read(self.key)['status'],'RECONCILIATION_REQUIRED')

    def test_expiry_and_wall_or_monotonic_rollback_never_create_wire(self):
        self.bind()
        for wall,mono in [(2015,1001),(2001,1015),(1999,1001),(2001,999)]:
            with self.subTest(wall=wall,mono=mono):
                self.wall,self.mono=wall,mono
                with self.assertRaises(KisScopedAuthorityError):self.store.authorize(self.key,context=self.context,dispatch_key='dispatch-1')

    def test_changed_account_receipt_set_or_place_operation_never_granted(self):
        with self.assertRaises(KisScopedAuthorityError):self.store.reserve('session-1',self.context,[{'dispatchKey':'someone-else','businessHash':'a'*64}])
        with self.assertRaises(KisScopedAuthorityError):self.store.bind_truth(self.key,{'orders':[]},expires_at=self.wall+15,ownership=self.orders)
        self.bind()
        for operation in ['PLACE_ORDER','CLEANUP_SELL','OVERSEAS_CANCEL_ORDER']:
            with self.subTest(operation=operation),self.assertRaises(KisScopedAuthorityError):
                self.store.consume(self.key,context=self.context,dispatch_key='dispatch-1',intent={**self.intent,'operation':operation})
        with self.assertRaises(KisScopedAuthorityError):self.store.authorize(self.key,context={**self.context,'scopeHash':'0'*64},dispatch_key='dispatch-1')

    def test_ack_absence_or_unsettled_capital_does_not_clear_cleanup(self):
        self.bind()
        for truth,capital,reconciled in [(self.proof,True,True),({'orders':[]},True,True),
                ({'orders':[{'dispatchKey':'dispatch-1','state':'TERMINAL'}]},False,True),
                ({'orders':[{'dispatchKey':'dispatch-1','state':'TERMINAL'}]},True,False)]:
            with self.subTest(truth=truth,capital=capital,reconciled=reconciled),self.assertRaises(KisScopedAuthorityError):
                self.store.finish(self.key,terminal_truth=truth,owned_capital_settled=capital,reconciliation_passed=reconciled)
        self.assertEqual(self.store.public_status()['status'],'RECONCILIATION_REQUIRED')
        self.store.finish(self.key,terminal_truth={'orders':[{'dispatchKey':'dispatch-1','state':'TERMINAL'}]},owned_capital_settled=True,reconciliation_passed=True)
        self.assertEqual(self.store.pending_for_account(self.context['accountFingerprint']),[])

    def test_pending_native_reservation_cannot_mint_cleanup(self):
        self.ops.reserve(self.context,session_id='never-running',reservation_id='pending-2',expires_at=self.wall+30)
        self.ops.revoke(session_id='never-running')
        with self.assertRaises(KisScopedAuthorityError):self.store.reserve('never-running',self.context,[])

    def test_old_native_consent_cannot_gain_cleanup_policy_after_activation(self):
        with self.assertRaises(KisScopedAuthorityError):
            self.ops.record_native_cleanup_policy('session-1',context=self.context,policy=CLEANUP_POLICY)
        with closing(self.ops._connect()) as connection, connection:
            connection.execute("DELETE FROM kis_operational_events WHERE event='NATIVE_STOP_CLEANUP_POLICY'")
        with self.assertRaises(KisScopedAuthorityError):self.store.consent_context('session-1')

    def test_identity_recovery_only_excludes_the_five_existing_cas_fields(self):
        row={'response':{'ODNO':'2'},'metadata':{'native_quantity_model':{'quantity':'3'}},'state':'accepted','organization_no':'1','broker_order_id':'2','order_date':'provisional'}
        recovered={**row,'order_date':'20261016','broker_order_key':'20261016:1:2','broker_order_identity_verified':True,
            'order_date_source':'OFFICIAL_ORDER_QUERY_EXACT_COMPOSITE_IDENTITY','broker_order_identity_receipt_hash':'a'*64}
        self.assertEqual(dispatch_business_hash(row),dispatch_business_hash(recovered))
        for field,value in [('state','filled'),('organization_no','7'),('broker_order_id','8'),('response',{})]:
            self.assertNotEqual(dispatch_business_hash(row),dispatch_business_hash({**recovered,field:value}))
        from live_trader.kis_scoped_cleanup_authority import dispatch_ownership_hash
        self.assertEqual(dispatch_ownership_hash(row),dispatch_ownership_hash({**recovered,'state':'cancelled','updated_at':'new poll'}))
        self.assertNotEqual(dispatch_ownership_hash(row),dispatch_ownership_hash({**recovered,'response':{}}))


class CleanupWireTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{'KIS_APP_KEY':'synthetic-key','KIS_APP_SECRET':'synthetic-secret',
            'KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01','KIS_ENV':'real',
            'KIS_BASE_URL':'https://openapi.koreainvestment.com:9443'})
        self.env.start();self.addCleanup(self.env.stop)
        self.proof={'state':'WORKING','market':'US','currency':'USD','venue':'NASD','symbol':'AAPL','side':'BUY',
            'orderDate':'20261016','organizationNo':'001','orderNo':'0002','remainingQuantity':'2','filledQuantity':'1','originalQuantity':'3'}

    def test_us_exact_current_partial_remainder_and_explicit_venue(self):
        wire=build_kis_scoped_cleanup_request(self.proof,access_token='synthetic-token')
        self.assertEqual(wire.headers['tr_id'],'TTTT1004U')
        self.assertEqual(wire.body['ORD_QTY'],'2');self.assertEqual(wire.body['OVRS_EXCG_CD'],'NASD')
        self.assertEqual(wire.body['RVSE_CNCL_DVSN_CD'],'02');self.assertEqual(wire.body['OVRS_ORD_UNPR'],'0')
        self.assertNotIn('KRX_FWDG_ORD_ORGNO',wire.body)

    def test_domestic_is_exact_quantity_never_all_remaining_flag(self):
        proof={**self.proof,'market':'KR','currency':'KRW','venue':'KRX','symbol':'005930'}
        wire=build_kis_scoped_cleanup_request(proof,access_token='synthetic-token')
        self.assertEqual(wire.headers['tr_id'],'TTTC0013U');self.assertEqual(wire.body['QTY_ALL_ORD_YN'],'N')
        self.assertEqual(wire.body['KRX_FWDG_ORD_ORGNO'],'001');self.assertEqual(wire.body['ORD_QTY'],'2')

    def test_missing_venue_fractional_float_negative_zero_and_changed_scope_block(self):
        for field,value in [('venue',''),('venue','NAS'),('remainingQuantity','2.5'),('remainingQuantity',2.0),
                ('remainingQuantity','0'),('remainingQuantity','1e1'),('remainingQuantity','-1'),
                ('filledQuantity','2'),('orderDate',''),('organizationNo',''),('state','TERMINAL'),('currency','USDT')]:
            with self.subTest(field=field,value=value),self.assertRaises(auth.KisOrderAuthorityError):
                build_kis_scoped_cleanup_request({**self.proof,field:value},access_token='synthetic-token')


class CleanupFinalBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.saved=(auth._AUTHORITY_READER,auth._SCOPED_OPERATIONAL_READER,auth._SPECIALIZED_EXCLUSION_READER,auth._SCOPED_CLEANUP_READER)
        auth._reset_kis_order_authority_reader_for_tests();self.addCleanup(self.restore)
        self.snapshot=dict(durableAuthorityReadable=True,functionalAuthorityOpen=True,functionalPhase='ARMED_WAIT_PUBLIC',
            functionalRevision=1,stateRevision=1,ownerEpochId='owner-test',ownerEpochHash='f'*64,
            functionalSessionId='',functionalAccountFingerprint='d'*64,credentialConfigurationHash='e'*64,
            functionalMutationIntent={},killOrdinaryCancelAllowed=False,killOrdinaryCancelRevision=0,killOrdinaryCancelIntent={},
            applicationInstanceLeaseHeld=True,ordinaryRoutesClosed=True,controlReservation={})
        auth.register_kis_order_authority_reader(lambda:deepcopy(self.snapshot))
        @contextmanager
        def emergency(): yield {'active':False,'revision':'emergency-1'}
        self.emergency=patch.object(auth,'emergency_stop_dispatch_boundary',emergency);self.emergency.start();self.addCleanup(self.emergency.stop)
        self.allowed=True;self.consumed=[]
        def resolver(selector,*,authority_intent=None,prepared_request=None,consume=False):
            if not self.allowed or selector!={'cleanupId':'cleanup-1','dispatchKey':'dispatch-1'}:raise ValueError('scope revoked')
            if consume:
                if self.consumed:raise ValueError('durable semantic claim consumed')
                self.consumed.append(authority_intent)
            return {'allowed':True}
        auth.register_kis_scoped_cleanup_reader(resolver)
        self.selector={'cleanupId':'cleanup-1','dispatchKey':'dispatch-1'}
        self.wire=dict(operation='SCOPED_CANCEL_US',claimId='cleanup-exact',ownedOrderKey={'orderDate':'20261016','organizationNo':'001','orderNo':'0002'},
            accountFingerprint='d'*64,credentialConfigurationHash='e'*64,endpoint='/uapi/overseas-stock/v1/trading/order-rvsecncl',payloadHash='1'*64)

    def restore(self):
        auth._reset_kis_order_authority_reader_for_tests()
        auth._AUTHORITY_READER,auth._SCOPED_OPERATIONAL_READER,auth._SPECIALIZED_EXCLUSION_READER,auth._SCOPED_CLEANUP_READER=self.saved

    def consume(self):
        return auth.consume_inherited_kis_transport_authority(endpoint=self.wire['endpoint'],payload_hash=self.wire['payloadHash'],account_fingerprint='d'*64,credential_configuration_hash='e'*64)

    def test_cancel_only_distinct_mode_and_final_one_use(self):
        with auth.kis_scoped_cleanup_dispatch_boundary(self.selector),auth.scoped_kis_cleanup_authenticated_preflight():
            with auth.scoped_kis_cleanup_final_boundary(operation='SCOPED_CANCEL_US',intent=self.wire,prepared_request=object()):
                snapshot=self.consume();self.assertEqual(snapshot['inheritedMode'],'SCOPED_CLEANUP');self.assertTrue(snapshot['inheritedCleanupOnly'])
                with self.assertRaises(auth.KisOrderAuthorityError):self.consume()
        self.assertEqual(len(self.consumed),1)
        with self.assertRaises(auth.KisOrderAuthorityError):self.consume()

    def test_place_ordinary_cancel_and_forged_or_missing_context_remain_closed(self):
        with self.assertRaises(auth.KisOrderAuthorityError):
            with auth.scoped_kis_cleanup_authenticated_preflight():pass
        with self.assertRaises(auth.KisOrderAuthorityError):
            with auth.kis_scoped_cleanup_dispatch_boundary({**self.selector,'dispatchKey':'someone-else'}):pass
        self.assertIsNone(getattr(auth._THREAD_BOUNDARY,'scoped_cleanup',None))
        with auth.kis_scoped_cleanup_dispatch_boundary(self.selector),auth.scoped_kis_cleanup_authenticated_preflight():
            for operation in ['PLACE_ORDER','OVERSEAS_CANCEL_ORDER','CLEANUP_SELL']:
                with self.subTest(operation=operation),self.assertRaises(auth.KisOrderAuthorityError):
                    with auth.scoped_kis_cleanup_final_boundary(operation=operation,intent=self.wire,prepared_request=object()):pass
        with self.assertRaises(auth.KisOrderAuthorityError):
            with auth.kis_authenticated_mutation_preflight(mode='ORDINARY'):pass

    def test_owner_or_stop_scope_change_after_prepare_blocks_before_consumption(self):
        for mutation in ['scope','revision','credentials','account']:
            self.allowed=True;self.snapshot['stateRevision']=1;self.snapshot['credentialConfigurationHash']='e'*64;self.snapshot['functionalAccountFingerprint']='d'*64
            with self.subTest(mutation=mutation),auth.kis_scoped_cleanup_dispatch_boundary(self.selector),auth.scoped_kis_cleanup_authenticated_preflight():
                with auth.scoped_kis_cleanup_final_boundary(operation='SCOPED_CANCEL_US',intent=self.wire,prepared_request=object()):
                    if mutation=='scope':self.allowed=False
                    if mutation=='revision':self.snapshot['stateRevision']=2
                    if mutation=='credentials':self.snapshot['credentialConfigurationHash']='0'*64
                    if mutation=='account':self.snapshot['functionalAccountFingerprint']='0'*64
                    with self.assertRaises(auth.KisOrderAuthorityError):self.consume()
        self.assertEqual(self.consumed,[])
