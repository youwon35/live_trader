from contextlib import contextmanager
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from live_trader import kis_order_authority as authority
from live_trader.kis_scoped_operational_authority import KisScopedOperationalAuthority


class ScopedFinalBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.saved=(authority._AUTHORITY_READER,authority._SCOPED_OPERATIONAL_READER,authority._SPECIALIZED_EXCLUSION_READER)
        authority._reset_kis_order_authority_reader_for_tests()
        self.addCleanup(self.restore)
        self.snapshot=dict(durableAuthorityReadable=True,functionalAuthorityOpen=True,functionalPhase='ARMED_WAIT_PUBLIC',
            functionalRevision=1,stateRevision=1,ownerEpochId='owner-test',ownerEpochHash='f'*64,
            functionalSessionId='',functionalAccountFingerprint='d'*64,credentialConfigurationHash='e'*64,
            functionalMutationIntent={},killOrdinaryCancelAllowed=False,killOrdinaryCancelRevision=0,killOrdinaryCancelIntent={},
            applicationInstanceLeaseHeld=True,ordinaryRoutesClosed=True,controlReservation={})
        authority.register_kis_order_authority_reader(lambda:copy.deepcopy(self.snapshot))
        @contextmanager
        def emergency(): yield {'active':False,'revision':'emergency-1'}
        self.addCleanup(patch.stopall)
        patch.object(authority,'emergency_stop_dispatch_boundary',emergency).start()
        self.allowed=True;self.consumed=[]
        def resolver(dispatch,*,broker_payload,authority_intent=None,prepared_request=None,consume=False):
            if dispatch is not self.dispatch or not self.allowed:
                raise ValueError('exact state-owned dispatch revoked')
            if consume:
                if authority_intent in self.consumed: raise ValueError('durable one-use claim consumed')
                self.consumed.append(copy.deepcopy(authority_intent))
            return {'allowed':True,'permitId':'permit-1'}
        self.reader=resolver
        self.dispatch=object()
        self.payload={'broker_id':'kis','symbol':'005930','quantity':1,'price':50000,'order_type':'00','identifier':'evaluation-1'}
        self.wire=dict(operation='PLACE_ORDER',claimId='exact-claim',ownedOrderKey={'orderDate':'','organizationNo':'','orderNo':''},
            accountFingerprint='d'*64,credentialConfigurationHash='e'*64,endpoint='/uapi/domestic-stock/v1/trading/order-cash',payloadHash='1'*64)

    def restore(self):
        authority._reset_kis_order_authority_reader_for_tests()
        authority._AUTHORITY_READER,authority._SCOPED_OPERATIONAL_READER,authority._SPECIALIZED_EXCLUSION_READER=self.saved

    def register(self): authority.register_kis_scoped_operational_reader(self.reader,specialized_exclusion_reader=lambda:{'status':'EXCLUDED'})

    def consume(self,**overrides):
        return authority.consume_inherited_kis_transport_authority(endpoint=overrides.get('endpoint',self.wire['endpoint']),
            payload_hash=overrides.get('payload_hash',self.wire['payloadHash']),account_fingerprint=overrides.get('account_fingerprint','d'*64),credential_configuration_hash='e'*64)

    def test_ordinary_and_direct_post_remain_closed_without_and_with_operational_reader(self):
        for register in (False,True):
            if register:self.register()
            with self.assertRaises(authority.KisOrderAuthorityError):
                with authority.kis_authenticated_mutation_preflight(mode='ORDINARY'): pass
            with self.assertRaises(authority.KisOrderAuthorityError):
                with authority.ordinary_kis_final_mutation_boundary(operation='PLACE_ORDER',intent=self.wire):pass
            with self.assertRaises(authority.KisOrderAuthorityError):self.consume()

    def test_no_reader_or_no_state_dispatch_cannot_create_authority(self):
        with self.assertRaises(authority.KisOrderAuthorityError):
            with authority.kis_scoped_operational_dispatch_boundary(self.dispatch,self.payload):pass
        self.register()
        with self.assertRaises(authority.KisOrderAuthorityError):
            with authority.scoped_kis_authenticated_mutation_preflight(self.payload):pass
        self.assertFalse(authority.has_scoped_kis_dispatch_context())

    def test_only_exact_state_dispatch_payload_can_inherit_token_and_one_wire(self):
        self.register()
        with authority.kis_scoped_operational_dispatch_boundary(self.dispatch,self.payload):
            with self.assertRaises(authority.KisOrderAuthorityError):
                with authority.scoped_kis_authenticated_mutation_preflight({**self.payload,'quantity':2}):pass
            with authority.scoped_kis_authenticated_mutation_preflight(self.payload):
                self.assertTrue(authority.require_kis_token_authority()['ordinaryRoutesClosed'])
                with authority.scoped_kis_final_mutation_boundary(operation='PLACE_ORDER',intent=self.wire):
                    self.assertEqual(self.consume()['inheritedMode'],'SCOPED_OPERATIONAL')
                    with self.assertRaises(authority.KisOrderAuthorityError):self.consume()
        self.assertEqual(len(self.consumed),1)

    def test_stop_revision_account_endpoint_and_wire_changes_block_at_last_edge(self):
        self.register()
        for mutation in ('stop','revision','account','payload','endpoint'):
            self.allowed=True;self.snapshot['stateRevision']=1;self.consumed=[]
            wire={**self.wire,'claimId':'claim-'+mutation}
            with self.subTest(mutation=mutation):
                with authority.kis_scoped_operational_dispatch_boundary(self.dispatch,self.payload):
                    with authority.scoped_kis_authenticated_mutation_preflight(self.payload):
                        with authority.scoped_kis_final_mutation_boundary(operation='PLACE_ORDER',intent=wire):
                            kwargs={}
                            if mutation=='stop':self.allowed=False
                            if mutation=='revision':self.snapshot['stateRevision']=2
                            if mutation=='account':kwargs['account_fingerprint']='a'*64
                            if mutation=='payload':kwargs['payload_hash']='a'*64
                            if mutation=='endpoint':kwargs['endpoint']='/uapi/overseas-stock/v1/trading/order'
                            with self.assertRaises(authority.KisOrderAuthorityError):self.consume(**kwargs)
            self.assertFalse(self.consumed)

    def test_operational_never_grants_cancel_or_nested_other_intent(self):
        self.register()
        with authority.kis_scoped_operational_dispatch_boundary(self.dispatch,self.payload):
            with self.assertRaises(authority.KisOrderAuthorityError):
                with authority.scoped_kis_final_mutation_boundary(operation='CANCEL_ORDER',intent=self.wire):pass
            with self.assertRaises(authority.KisOrderAuthorityError):
                with authority.kis_scoped_operational_dispatch_boundary(object(),self.payload):pass

    def test_specialized_new_order_is_blocked_by_unknown_and_positive_exclusion(self):
        self.register()
        self.snapshot.update(functionalPhase='ACTIVE',functionalSessionId='specialized-session',functionalMutationIntent={**self.wire,'operation':'NATURAL_BUY'})
        for status in ('UNKNOWN','EXCLUDED'):
            authority._SPECIALIZED_EXCLUSION_READER=lambda:{'status':status}
            with self.subTest(status=status),self.assertRaises(authority.KisOrderAuthorityError):
                with authority.functional_kis_final_mutation_boundary(operation='NATURAL_BUY',session_id='specialized-session',cleanup_only=False,expected_revision=1,intent={**self.wire,'operation':'NATURAL_BUY'}):pass


if __name__=='__main__':unittest.main()
