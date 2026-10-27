"""Offline boundary regression. Never imports state or sends a request."""
import os
import unittest
from unittest.mock import patch
from live_trader import live_adapters as a
class KisReadEnvironmentDefaultTests(unittest.TestCase):
    def setUp(self):
        self.env=patch.dict(os.environ,{'KIS_BASE_URL':a.KIS_LIVE_BASE_URL,'KIS_APP_KEY':'synthetic-key','KIS_APP_SECRET':'synthetic-secret'},clear=True)
        self.env.start();self.addCleanup(self.env.stop)
    def token(self, **changes):
        request=dict(method='POST',url=a.KIS_LIVE_BASE_URL+a.KIS_TOKEN_ENDPOINT,body={'grant_type':'client_credentials','appkey':'synthetic-key','appsecret':'synthetic-secret'},headers={'content-type':'application/json; charset=utf-8'},kis_specific=True)
        request.update(changes)
        return a._guard_kis_owned_http_edge(**request)
    def test_unspecified_normal_real_read_environment_accepts_token_only_with_authority(self):
        with patch.object(a,'require_kis_token_authority') as authority:
            self.assertEqual(self.token(),'TOKEN');authority.assert_called_once_with()
        with self.assertRaises(a.KisOrderAuthorityError): self.token()
    def test_explicit_non_real_environment_remains_blocked(self):
        for value in ['paper','demo','invalid']:
            os.environ['KIS_ENV']=value
            with patch.object(a,'require_kis_token_authority') as authority:
                with self.assertRaisesRegex(a.KisOrderAuthorityError,'token wire tuple'): self.token()
                authority.assert_not_called()
    def test_official_origin_body_and_headers_remain_exact(self):
        for changes in [{'url':a.KIS_LIVE_BASE_URL+a.KIS_TOKEN_ENDPOINT+'?x=1'}, {'body':{'grant_type':'client_credentials','appkey':'wrong','appsecret':'synthetic-secret'}}, {'headers':{'content-type':'application/json'}}]:
            with self.assertRaises(a.KisOrderAuthorityError):self.token(**changes)
        os.environ['KIS_BASE_URL']='https://evil.invalid'
        with self.assertRaises(a.KisOrderAuthorityError):self.token(url='https://evil.invalid'+a.KIS_TOKEN_ENDPOINT)
    def test_default_get_still_requires_exact_read_lease(self):
        with patch.object(a,'_require_kis_read_wire_authority') as authority:
            self.assertEqual(a._guard_kis_owned_http_edge(method='GET',url=a.KIS_LIVE_BASE_URL+a.KIS_DOMESTIC_BALANCE_ENDPOINT,body=None,headers={'authorization':'Bearer synthetic'},kis_specific=True),'READ')
            authority.assert_called_once()
        with patch.object(a,'_kis_read_final_binding_raw',return_value=('a'*64,'b'*64)), self.assertRaises(a.KisOrderAuthorityError):
            a._guard_kis_owned_http_edge(method='GET',url=a.KIS_LIVE_BASE_URL+a.KIS_DOMESTIC_BALANCE_ENDPOINT,body=None,headers={'authorization':'Bearer synthetic'},kis_specific=True)
    def test_default_does_not_open_mutation_route(self):
        with self.assertRaises(a.KisOrderAuthorityError):
            a._guard_kis_owned_http_edge(method='POST',url=a.KIS_LIVE_BASE_URL+a.KIS_DOMESTIC_ORDER_ENDPOINT,body={},headers={},kis_specific=True)
if __name__=='__main__':unittest.main()
