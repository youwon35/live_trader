"""USD diagnosis is never a substitute for a missing positive broker producer."""
from copy import deepcopy
from datetime import timedelta
import importlib
import socket
import unittest
from unittest.mock import patch

import test_kis_scoped_cleanup_truth as fixtures

gate = importlib.import_module(fixtures.NAME+'.kis_native_usd_reconciliation')
service = fixtures.service
NOW = fixtures.NOW


class NativeUsdReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.no_socket=patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.no_socket_ex=patch.object(socket.socket,'connect_ex',side_effect=AssertionError('network forbidden'))
        self.no_socket.start();self.no_socket_ex.start();self.addCleanup(self.no_socket.stop);self.addCleanup(self.no_socket_ex.stop)
        self.scope,_,_,_=fixtures.fixture()
        self.observation=service.seal({'version':service.OBSERVATION,'brokerId':'kis','currency':'USD',
            'exchange':'NASD','symbol':'AAPL','profileId':self.scope['accountProfileId'],
            'accountFingerprint':self.scope['accountFingerprint'],
            'credentialConfigurationHash':self.scope['credentialConfigurationHash'],
            'complete':True,'orderAuthorityGranted':False,'side':'BUY','orderable':True,
            'openOrders':'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE',
            'actualPersonalFeeObserved':False,'availableCash':'1000','maximumBuyQuantity':'3',
            'startedAt':NOW.isoformat(),'asOf':NOW.isoformat()})
        self.projection={'capitalScopeHash':self.scope['scopeHash'],'equityScope':'OWNED_DEPLOYMENT_ONLY',
            'capitalAccounting':'OWNED_CONSERVATIVE_COST_RESERVE','actualFeesReconciled':False,
            'orderAuthorityGranted':False,'netPerformanceStatus':'ESTIMATED_UNTIL_ACTUAL_COST_RECONCILIATION',
            'fillCount':0,'grossCash':'1000','ownedCash':'1000','costReserveOutstanding':'0',
            'ownedPositions':{'member':'0'}}
    def status(self,**kwargs):
        values={'operation':'START','scope':self.scope,'native_observation':self.observation,
            'owned_projection':self.projection,'now':NOW}
        values.update(kwargs)
        return gate.native_usd_reconciliation_status(**values)
    def seal_observation(self,**values):
        self.observation=service.seal({**self.observation,**values})
    def assert_blocked(self,result):
        self.assertEqual(result['status'],'BLOCKED')
        self.assertIn(gate.PRODUCER_UNAVAILABLE,result['reasons'])
        for key in ('threeWayVerified','usdCashReconciled','actualFeesReconciled',
                    'actualPersonalFeeObserved','positiveProofProducerAvailable','orderAuthorityGranted'):
            self.assertIs(result[key],False,key)
        self.assertEqual(result['contentHash'],fixtures.truth._hash({k:v for k,v in result.items() if k!='contentHash'}))
    def test_initial_flat_generic_krw_pass_never_becomes_usd_proof(self):
        result=self.status(generic_reconciliation={'summary':{'status':'pass','fresh':True,'three_way_verified':True},
            'accounts':[{'broker_id':'kis','currency':'KRW','status':'pass'}]})
        self.assert_blocked(result)
        self.assertTrue(result['genericBrokerReconciliationIgnored'])
        self.assertTrue(result['checks']['exactNativeUsdScope'])
        self.assertTrue(result['checks']['currentUsdCapacityObserved'])
        self.assertTrue(result['checks']['ownedProjectionConsistent'])
        self.assertIn('ORDER_CAPACITY_IS_NOT_ACCOUNT_CASH_RECONCILIATION',','.join(result['reasons']))
    def test_no_account_no_book_no_values_still_returns_explicit_block(self):
        result=gate.native_usd_reconciliation_status(operation='START',now=NOW)
        self.assert_blocked(result)
        self.assertFalse(result['checks']['exactNativeUsdScope'])
        self.assertIn('KIS_USD_CURRENT_ORDER_CAPACITY_UNAVAILABLE',result['reasons'])
    def test_start_renewal_and_clean_all_require_separate_usd_proof(self):
        for operation in ('START','RENEWAL','CLEAN'):
            with self.subTest(operation=operation):
                result=self.status(operation=operation);self.assert_blocked(result)
                with self.assertRaisesRegex(ValueError,'KIS_USD_NATIVE_RECONCILIATION_REQUIRED'):
                    gate.require_native_usd_reconciliation(operation=operation,scope=self.scope,
                        native_observation=self.observation,owned_projection=self.projection,now=NOW)
    def test_unknown_operation_or_unhashable_input_is_not_an_exception_or_allow(self):
        for operation in ('STOP',None,[],True):
            with self.subTest(operation=operation):
                result=self.status(operation=operation);self.assert_blocked(result)
                self.assertIn('KIS_USD_RECONCILIATION_OPERATION_INVALID',result['reasons'])
    def test_krw_or_usdt_scope_cannot_claim_usd_reconciliation(self):
        for currency in ('KRW','USDT'):
            with self.subTest(currency=currency):
                scope={**self.scope,'quoteCurrency':currency}
                scope['scopeHash']=fixtures.truth._hash({k:v for k,v in scope.items() if k!='scopeHash'})
                result=self.status(scope=scope);self.assert_blocked(result)
                self.assertFalse(result['checks']['exactNativeUsdScope'])
    def test_changed_scope_account_credential_or_hash_invalidates_capacity(self):
        for key in ('accountFingerprint','credentialConfigurationHash','profileId','currency','symbol'):
            with self.subTest(key=key):
                observation=service.seal({**self.observation,key:'changed'})
                result=self.status(native_observation=observation);self.assert_blocked(result)
                self.assertFalse(result['checks']['currentUsdCapacityObserved'])
        result=self.status(scope={**self.scope,'capitalPercent':'6'})
        self.assertFalse(result['checks']['exactNativeUsdScope'])
    def test_future_stale_naive_or_multiread_over15_capacity_is_unverified(self):
        for change in ({'asOf':(NOW+timedelta(seconds=1)).isoformat()},
            {'startedAt':(NOW-timedelta(seconds=16)).isoformat()},
            {'startedAt':NOW.replace(tzinfo=None).isoformat()},
            {'asOf':(NOW-timedelta(seconds=16)).isoformat()}):
            with self.subTest(change=change):
                result=self.status(native_observation=service.seal({**self.observation,**change}))
                self.assert_blocked(result);self.assertFalse(result['checks']['currentUsdCapacityObserved'])
    def test_incomplete_open_orders_or_invented_fee_observation_is_not_accepted(self):
        for change in ({'complete':False},{'openOrders':'PRESENT'},
            {'actualPersonalFeeObserved':True},{'orderAuthorityGranted':True},{'maximumBuyQuantity':'1.5'}):
            with self.subTest(change=change):
                result=self.status(native_observation=service.seal({**self.observation,**change}))
                self.assertFalse(result['checks']['currentUsdCapacityObserved']);self.assert_blocked(result)
    def test_unknown_actual_fees_remain_unknown_and_reserves_remain_estimated(self):
        projection={**self.projection,'fillCount':1,'grossCash':'700','ownedCash':'697',
            'costReserveOutstanding':'3','ownedPositions':{'member':'3'}}
        result=self.status(owned_projection=projection)
        self.assert_blocked(result);self.assertTrue(result['checks']['ownedProjectionConsistent'])
        self.assertIn(gate.ACTUAL_COST_UNPROVEN,result['reasons'])
        self.assertEqual(result['cashRoles']['costReserve'],'ESTIMATED_HOLDBACK_NOT_ACTUAL_FEE')
        self.assertNotIn('actualFee',result)
    def test_marking_reserve_as_actual_fee_is_rejected_even_if_cash_arithmetic_matches(self):
        for change in ({'actualFeesReconciled':True},{'capitalAccounting':'ACTUAL_FEE_RECONCILED'},
            {'netPerformanceStatus':'RECONCILED_NET_PNL'},{'costReserveOutstanding':'1'}):
            with self.subTest(change=change):
                result=self.status(owned_projection={**self.projection,**change})
                self.assert_blocked(result);self.assertFalse(result['checks']['ownedProjectionConsistent'])
    def test_owned_book_native_decimal_and_whole_shares_are_never_float_coerced(self):
        for change in ({'ownedCash':1000.0},{'grossCash':'NaN'},{'costReserveOutstanding':'-1'},
            {'ownedPositions':{'member':'0.5'}},{'fillCount':True},{'ownedPositions':{'foreign':'0'}}):
            with self.subTest(change=change):
                result=self.status(owned_projection={**self.projection,**change})
                self.assert_blocked(result);self.assertFalse(result['checks']['ownedProjectionConsistent'])
    def test_gross_terminal_delta_or_fake_cost_flags_never_prove_account_cash(self):
        for projection in ({'status':'READY','source':'KIS_REST_TERMINAL','actualFeesReconciled':True},
            {'status':'PASS','fee':'0','privateEventObserved':True}):
            with self.subTest(projection=projection):
                result=self.status(terminal_projection=projection)
                self.assert_blocked(result)
                self.assertIn('KIS_USD_TERMINAL_GROSS_PROJECTION_IS_NOT_ACCOUNT_CASH_PROOF',result['reasons'])
    def test_current_present_balance_posting_delay_is_not_relabelled_fresh_final_cash(self):
        result=self.status()
        self.assertEqual(result['cashRoles']['presentBalanceDeposit'],
            'CURRENCY_FOREIGN_FUNDS_AVAILABLE_WITH_DOCUMENTED_SETTLEMENT_AND_POSTING_DELAY')
        self.assertEqual(result['cashRoles']['presentBalanceFinality'],
            'FRESH_HTTP_RESPONSE_DOES_NOT_PROVE_REALTIME_OR_FINALIZED_CASH')
        self.assert_blocked(result)
    def test_decimal_reserve_identity_uses_full_precision(self):
        projection={**self.projection,'fillCount':1,
            'grossCash':'999999999999999999999999.9999999999999999',
            'ownedCash':'999999999999999999999999.9999999999999998',
            'costReserveOutstanding':'0.0000000000000001'}
        self.assertTrue(self.status(owned_projection=projection)['checks']['ownedProjectionConsistent'])
        projection['costReserveOutstanding']='0'
        self.assertFalse(self.status(owned_projection=projection)['checks']['ownedProjectionConsistent'])
    def test_inputs_are_not_mutated_and_no_production_state_is_imported(self):
        before=deepcopy((self.scope,self.observation,self.projection))
        self.status()
        self.assertEqual(before,(self.scope,self.observation,self.projection))
        import sys
        self.assertNotIn(fixtures.NAME+'.state',sys.modules)
        self.assertNotIn(fixtures.NAME+'.server',sys.modules)


if __name__=='__main__':unittest.main()
