from dataclasses import replace
import unittest
from unittest.mock import Mock, patch

from live_trader import state
from live_trader.continuous_live import LiveContinuousController
from live_trader.order_management import OrderIntent
from live_trader.server import LiveTraderHandler


class KisDeferredSubmissionTests(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(state, 'runtime_market_integrity_dispatch_allowed', return_value=(True, ''))
        patcher.start()
        self.addCleanup(patcher.stop)

    def intent(self,broker='kis',mode='SMALL_LIVE'):
        return OrderIntent('source','equity','005930','BUY',1,100,mode,'fixture',
            {'broker_id':broker,'native_open_boundary':{}})

    def test_failed_refresh_never_validates_ticket_or_submits(self):
        with patch.object(state,'refresh_kis_scoped_runtime_preflight',return_value={'ok':False,'reason':'fresh proof rejected'}) as refresh,patch('live_trader.native_order_ticket.validate_native_ticket') as ticket,patch.object(state,'submit_order_intent') as submit:
            result=LiveContinuousController._submit_deferred_intent(self.intent())
        self.assertFalse(result['ok']);self.assertFalse(result['orderSubmitted'])
        refresh.assert_called_once();ticket.assert_not_called();submit.assert_not_called()

    def test_current_or_refreshed_still_validates_native_ticket_before_one_submission(self):
        for action in ('CURRENT','REFRESHED'):
            calls=[]
            with self.subTest(action=action),patch.object(state,'refresh_kis_scoped_runtime_preflight',side_effect=lambda intent:calls.append('preflight') or {'ok':True,'action':action}),patch('live_trader.native_order_ticket.validate_native_ticket',side_effect=lambda intent:calls.append('ticket')),patch.object(state,'snapshot',return_value={}),patch.object(state,'submit_order_intent',side_effect=lambda *args,**kwargs:calls.append('submit') or {'ok':True}) as submit:
                self.assertTrue(LiveContinuousController._submit_deferred_intent(self.intent())['ok'])
                self.assertEqual(calls,['preflight','ticket','submit']);submit.assert_called_once()

    def test_expired_native_ticket_after_network_refresh_never_submits(self):
        with patch.object(state,'refresh_kis_scoped_runtime_preflight',return_value={'ok':True,'action':'REFRESHED'}),patch('live_trader.native_order_ticket.validate_native_ticket',side_effect=ValueError('native 15-second expiry')),patch.object(state,'submit_order_intent') as submit:
            result=LiveContinuousController._submit_deferred_intent(self.intent())
        self.assertFalse(result['ok']);submit.assert_not_called()

    def test_crypto_keeps_existing_submission_path_without_kis_refresh(self):
        with patch.object(state,'refresh_kis_scoped_runtime_preflight') as refresh,patch('live_trader.native_order_ticket.validate_native_ticket') as ticket,patch.object(state,'snapshot',return_value={}),patch.object(state,'submit_order_intent',return_value={'ok':True}) as submit:
            self.assertTrue(LiveContinuousController._submit_deferred_intent(self.intent('binance'))['ok'])
        refresh.assert_not_called();ticket.assert_called_once();submit.assert_called_once()


class KisNativeChallengeHttpTests(unittest.TestCase):
    def test_unauthenticated_or_absolute_form_challenge_never_reads_body_or_prepares(self):
        for target in ('/api/runtime/kis-manual-start-challenge','http://localhost/api/runtime/kis-manual-start-challenge'):
            with self.subTest(target=target):
                handler=object.__new__(LiveTraderHandler);handler.path=target
                handler._authorize_functional_http=Mock(return_value=False)
                handler._send_functional_http_denial=Mock();handler.read_json=Mock();handler.send_json=Mock()
                with patch.object(state,'prepare_kis_scoped_manual_start') as prepare:
                    handler.do_POST()
                handler.read_json.assert_not_called();prepare.assert_not_called()

    def test_authenticated_challenge_only_arms_review_and_never_starts_runtime(self):
        handler=object.__new__(LiveTraderHandler);handler.path='/api/runtime/kis-manual-start-challenge'
        handler._authorize_functional_http=Mock(return_value=True)
        handler.read_json=Mock(return_value={'profile_id':'stock','mode':'SMALL_LIVE','portfolio_id':'portfolio','deployment_id':'deployment','strategy_id':'source'})
        handler.send_json=Mock()
        with patch.object(state,'prepare_kis_scoped_manual_start',return_value={'ok':True,'nativeNonce':'fixture'}) as prepare,patch.object(state,'start_continuous_runtime') as start:
            handler.do_POST()
        handler._authorize_functional_http.assert_called_once_with(require_origin=True)
        prepare.assert_called_once_with('stock','SMALL_LIVE','portfolio','deployment','source');start.assert_not_called()

    def test_unauthenticated_or_absolute_cleanup_never_reads_body_stops_or_cancels(self):
        for target in ('/api/runtime/kis-stop-and-reconcile','http://localhost/api/runtime/kis-stop-and-reconcile'):
            with self.subTest(target=target):
                handler=object.__new__(LiveTraderHandler);handler.path=target
                handler._authorize_functional_http=Mock(return_value=False)
                handler._send_functional_http_denial=Mock();handler.read_json=Mock();handler.send_json=Mock()
                with patch.object(state,'stop_continuous_runtime') as stop:
                    handler.do_POST()
                handler.read_json.assert_not_called();stop.assert_not_called()

    def test_protected_cleanup_has_exact_scope_and_server_owned_financial_flag(self):
        handler=object.__new__(LiveTraderHandler);handler.path='/api/runtime/kis-stop-and-reconcile'
        handler._authorize_functional_http=Mock(return_value=True);handler.send_json=Mock()
        handler.read_json=Mock(return_value={'profile_id':'stock','runtime_id':'runtime-1','deployment_id':'deployment-1'})
        with patch.object(state,'stop_continuous_runtime',return_value={'ok':False,'runtimeStopped':True}) as stop:
            handler.do_POST()
        handler._authorize_functional_http.assert_called_once_with(require_origin=True)
        stop.assert_called_once_with('stock',runtime_id='runtime-1',deployment_id='deployment-1',cleanup_requested=True)

    def test_legacy_stop_ignores_body_financial_flag_and_new_route_rejects_extra_or_missing_scope(self):
        handler=object.__new__(LiveTraderHandler);handler.path='/api/runtime/stop'
        handler._authorize_functional_http=Mock(return_value=True);handler.send_json=Mock()
        handler.read_json=Mock(return_value={'profile_id':'stock','runtime_id':'runtime-1','deployment_id':'deployment-1','cleanup_requested':True})
        with patch.object(state,'stop_continuous_runtime',return_value={'ok':True}) as stop:
            handler.do_POST()
        stop.assert_called_once_with('stock',runtime_id='runtime-1',deployment_id='deployment-1')
        for body in ({'profile_id':'crypto','runtime_id':'runtime-1','deployment_id':'deployment-1'},
                {'profile_id':'stock','runtime_id':'runtime-1'},
                {'profile_id':'stock','runtime_id':'runtime-1','deployment_id':'deployment-1','cleanup_requested':True}):
            with self.subTest(body=body),patch.object(state,'stop_continuous_runtime') as stop:
                handler.path='/api/runtime/kis-stop-and-reconcile';handler.read_json.return_value=body;handler.do_POST()
                stop.assert_not_called()
