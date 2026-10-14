from __future__ import annotations
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP.parents[1]/'packages/trading_runtime'))
PACKAGE = '_kis_equity_observation_isolated'
pkg=types.ModuleType(PACKAGE);pkg.__path__=[str(APP/'live_trader')];sys.modules[PACKAGE]=pkg
service=importlib.import_module(PACKAGE+'.native_equity_observation')
from trading_runtime.execution_quantity import seal_quantity_contract
NOW=datetime(2026,10,15,tzinfo=timezone.utc)


def stamp(offset):
    return (NOW+timedelta(seconds=offset)).isoformat(timespec='microseconds').replace('+00:00','Z')


def contract(exchange='KRX'):
    return seal_quantity_contract({'schemaVersion':'native-equity-quantity-v1','provider':'kis',
        'assetClass':'EQUITY','quantityUnit':'SHARE','executionScope':'LOCAL_MODEL_ONLY',
        'symbol':'005930' if exchange=='KRX' else 'AAPL','baseCurrency':'005930' if exchange=='KRX' else 'AAPL',
        'quoteCurrency':'KRW' if exchange=='KRX' else 'USD','exchange':exchange,'orderType':'MARKET',
        'stepSize':'1','minQuantity':'1','maxQuantity':'1000000','minNotional':'0','maxNotional':'0',
        'notionalReferenceMinutes':0,'capabilityStatus':'UNKNOWN','provenance':{'references':['isolated model fixture']}})


def reseal(payload):
    payload=deepcopy(payload)
    request=payload['request']
    request['requestHash']=service.observation_content_hash({k:v for k,v in request.items() if k!='requestHash'})
    response=payload['response']
    response['projectionHash']=service.observation_content_hash({k:v for k,v in response.items() if k!='projectionHash'})
    payload['contentHash']=service.observation_content_hash({k:v for k,v in payload.items() if k!='contentHash'})
    return payload


def fixture(exchange='KRX',portfolio=False):
    source=contract(exchange);domestic=exchange=='KRX'
    account={'accountAlias':'저장된 KIS 별칭','accountFingerprint':'a'*64,'credentialConfigurationHash':'b'*64}
    scope={'deploymentId':'fixture-deployment','deploymentRevision':1,'sourceArtifactHash':'c'*64,
        'sourceQuantityContractHash':source['contractHash'],'strategyInstanceId':'fixture-instance',
        'strategyInstanceHash':'d'*64,'portfolioId':'fixture-portfolio' if portfolio else '',
        'portfolioHash':'e'*64 if portfolio else ''}
    query=({'PDNO':source['symbol'],'ORD_UNPR':'100','ORD_DVSN':'00','CMA_EVLU_AMT_ICLD_YN':'N','OVRS_ICLD_YN':'N'}
        if domestic else {'OVRS_EXCG_CD':exchange,'OVRS_ORD_UNPR':'100','ITEM_CD':source['symbol']})
    body={'schemaVersion':service.SCHEMA,'purpose':'READ_ONLY_PREPARATION','provider':'kis','environment':'KIS_LIVE',
        'assetClass':'EQUITY','market':'KR_STOCK' if domestic else 'US_STOCK','currency':source['quoteCurrency'],
        'symbol':source['symbol'],'exchange':exchange,'side':'BUY','account':account,'scope':scope,
        'request':{'method':'GET','endpoint':'/uapi/domestic-stock/v1/trading/inquire-psbl-order' if domestic else '/uapi/overseas-stock/v1/trading/inquire-psamount',
          'trId':'TTTC8908R' if domestic else 'TTTS3007R','bodyAbsent':True,'queryProjection':query,'requestHash':'f'*64},
        'response':{'currency':source['quoteCurrency'],'availableCash':'1000','maximumBuyQuantity':'10','calculationPrice':'100',
          'rounding':'UNSUPPORTED_BROKER_ROUNDING_REFERENCE_ONLY','projectionHash':'0'*64},
        'fee':{'kind':'UNKNOWN','rate':None,'unit':None,'currency':source['quoteCurrency'],'side':'BUY','uri':None},
        'requestStartedAt':stamp(-1),'responseReceivedAt':stamp(0),'expiresAt':stamp(14),'contentHash':'0'*64}
    expected={**{key:body[key] for key in service.IDENTITY_KEYS},**account,**scope,'queryProjection':deepcopy(query)}
    return reseal(body),expected,source


class KisEquityObservationTests(unittest.TestCase):
    def validate(self,payload,expected,source,now=NOW):
        with patch('socket.socket',side_effect=AssertionError('network forbidden')), \
             patch('builtins.open',side_effect=AssertionError('file IO forbidden')):
            return service.validate_kis_equity_observation(payload,expected_scope=expected,source_quantity_contract=source,now=now)

    def test_all_venues_valid_shape_remains_unverified_and_authority_false(self):
        for venue in ('KRX','NASD','NYSE','AMEX'):
            for portfolio in (False,True):
                p,e,c=fixture(venue,portfolio)
                result=self.validate(p,e,c)
                self.assertEqual('VALID',result.schema_validation)
                self.assertEqual('UNVERIFIED',result.verification_status)
                self.assertEqual('UNKNOWN',result.native_eligibility)
                self.assertFalse(result.live_allowed or result.order_authority_granted or result.actual_fee_observed)
                self.assertEqual(p,result.receipt)
                self.assertEqual('UNKNOWN',c['capabilityStatus'])

    def test_rehashed_identity_mutations_fail_exact_expected_scope(self):
        p,e,c=fixture('NASD',True)
        paths=[('provider',),('environment',),('market',),('currency',),('symbol',),('exchange',),('side',),
            ('account','accountAlias'),('account','accountFingerprint'),('account','credentialConfigurationHash'),
            ('scope','deploymentId'),('scope','deploymentRevision'),('scope','sourceArtifactHash'),
            ('scope','sourceQuantityContractHash'),('scope','strategyInstanceId'),('scope','strategyInstanceHash'),
            ('scope','portfolioId'),('scope','portfolioHash')]
        for path in paths:
            q=deepcopy(p);target=q
            for part in path[:-1]:target=target[part]
            original=target[path[-1]];target[path[-1]]=original+1 if type(original)is int else ('SELL' if path==('side',) else 'changed')
            with self.subTest(path=path),self.assertRaises(ValueError):self.validate(reseal(q),e,c)

    def test_expected_bool_revision_is_not_integer_identity(self):
        p,e,c=fixture();e['deploymentRevision']=True
        with self.assertRaises(ValueError):self.validate(p,e,c)

    def test_reference_query_cannot_be_rehashed_into_another_scope(self):
        p,e,c=fixture()
        for key,value in [('method','POST'),('trId','VTTC8908R'),('endpoint','/orders'),('bodyAbsent',False)]:
            q=deepcopy(p);q['request'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate(reseal(q),e,c)
        for key,value in [('PDNO','000000'),('ORD_UNPR','99'),('ORD_DVSN','01'),('CMA_EVLU_AMT_ICLD_YN','Y'),('OVRS_ICLD_YN','Y')]:
            q=deepcopy(p);q['request']['queryProjection'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate(reseal(q),e,c)
        p,e,c=fixture('NYSE');p['request']['queryProjection']['OVRS_EXCG_CD']='NASD'
        with self.assertRaises(ValueError):self.validate(reseal(p),e,c)

    def test_exact_freshness_endpoints_and_time_order(self):
        p,e,c=fixture();p['requestStartedAt']=stamp(-15);p['expiresAt']=stamp(0)
        self.assertEqual('VALID',self.validate(reseal(p),e,c).schema_validation)
        for key,value in [('requestStartedAt',stamp(-15.000001)),('responseReceivedAt',stamp(1)),
            ('requestStartedAt',stamp(1)),('expiresAt',stamp(-.000001)),('expiresAt',stamp(.000001)),('expiresAt',stamp(15.000001)),
            ('responseReceivedAt','2026-10-15T00:00:00'),('responseReceivedAt','2026-10-15T09:00:00.000000+09:00')]:
            q=deepcopy(p);q[key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):self.validate(reseal(q),e,c)
        with self.assertRaises(ValueError):self.validate(reseal(p),e,c,datetime(2026,10,15))

    def test_decimal_values_canonical_finite_and_no_rounding(self):
        p,e,c=fixture()
        for field,values in [('availableCash',['-1','-0','NaN','Infinity','1e3','001','1.0',True,1.5,'1'*257]),
                             ('maximumBuyQuantity',['0.5','-1','10.0']),('calculationPrice',['0','99.99999999'])]:
            for value in values:
                q=deepcopy(p);q['response'][field]=value
                with self.subTest(field=field,value=value),self.assertRaises(ValueError):self.validate(reseal(q),e,c)
        p['response']['rounding']='ROUND_HALF_UP'
        with self.assertRaises(ValueError):self.validate(reseal(p),e,c)

    def test_zero_cash_and_quantity_do_not_claim_orderability(self):
        p,e,c=fixture();p['response'].update(availableCash='0',maximumBuyQuantity='0')
        result=self.validate(reseal(p),e,c)
        self.assertFalse(result.live_allowed)
        self.assertEqual('UNKNOWN',result.native_eligibility)

    def test_public_fee_is_never_personal_observation(self):
        p,e,c=fixture();p['fee'].update(kind='PUBLIC_REFERENCE',rate='0.0025',unit='FRACTION',uri='https://securities.koreainvestment.com/fees')
        result=self.validate(reseal(p),e,c)
        self.assertFalse(result.actual_fee_observed)
        self.assertEqual('UNVERIFIED',result.verification_status)
        self.assertFalse(result.live_allowed)
        for kind in ('ACCOUNT_CURRENT_OBSERVED','VERIFIED'):
            p['fee']['kind']=kind
            with self.assertRaises(ValueError):self.validate(reseal(p),e,c)

    def test_unknown_fee_cannot_have_default_and_public_fee_scope_matters(self):
        p,e,c=fixture();p['fee']['rate']='0'
        with self.assertRaises(ValueError):self.validate(reseal(p),e,c)
        for key,value in [('side','SELL'),('currency','USD'),('rate','1'),('rate','0.0010'),('unit','PERCENT')]:
            p,e,c=fixture();p['fee'].update(kind='PUBLIC_REFERENCE',rate='0.0025',unit='FRACTION',uri='https://securities.koreainvestment.com/fees')
            p['fee'][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate(reseal(p),e,c)

    def test_uri_userinfo_fragment_query_or_nonhttps_rejected(self):
        for uri in ('http://example.com/fee','https://user:secret@example.com/fee','https://@example.com/fee',
                    'https://example.com/fee#secret','https://example.com/fee#','https://example.com/fee?key=secret',
                    'https://example.com/fee?','https://example.com:123/fee','https://example.com/\nsecret'):
            p,e,c=fixture();p['fee'].update(kind='PUBLIC_REFERENCE',rate='0',unit='FRACTION',uri=uri)
            with self.subTest(uri=uri),self.assertRaises(ValueError):self.validate(reseal(p),e,c)

    def test_immutable_result_and_detached_copies_cannot_authorize(self):
        p,e,c=fixture();result=self.validate(p,e,c);original=result.receipt
        p['account']['accountAlias']='later mutation';copy=result.receipt;copy['account']['accountAlias']='changed copy'
        self.assertEqual(original,result.receipt)
        for key,value in [('live_allowed',True),('verification_status','VERIFIED'),('native_eligibility','VERIFIED')]:
            with self.assertRaises(FrozenInstanceError):setattr(result,key,value)
            with self.assertRaises(TypeError):service.KisReferenceValidation(result._receipt_json,**{key:value})
        rendered=result.to_dict();rendered['live_allowed']=True
        self.assertFalse(result.to_dict()['live_allowed'])

    def test_wrong_hash_and_projection_hash_fail(self):
        p,e,c=fixture();p['contentHash']='0'*64
        with self.assertRaises(ValueError):self.validate(p,e,c)
        p,e,c=fixture();p['response']['projectionHash']='0'*64;p['contentHash']=service.observation_content_hash({k:v for k,v in p.items() if k!='contentHash'})
        with self.assertRaises(ValueError):self.validate(p,e,c)

    def test_public_request_hash_is_not_opaque_and_wrong_hash_rejected(self):
        p,e,c=fixture();p['request']['requestHash']='f'*64
        p['contentHash']=service.observation_content_hash({k:v for k,v in p.items() if k!='contentHash'})
        with self.assertRaisesRegex(ValueError,'REQUEST_PROJECTION_CHANGED'):self.validate(p,e,c)

    def test_unknown_secret_fields_and_nonjson_are_rejected(self):
        p,e,c=fixture()
        for holder,key,value in [('top','live_allowed',True),('account','CANO','synthetic-secret'),('request','headers',{'appkey':'synthetic-secret'})]:
            q=deepcopy(p);(q if holder=='top' else q[holder])[key]=value
            with self.subTest(holder=holder),self.assertRaises(ValueError):self.validate(reseal(q),e,c)
        for value in ((1,2),object(),1.0):
            q=deepcopy(p);q['fee']['rate']=value
            with self.assertRaises(ValueError):self.validate(q,e,c)
        q=deepcopy(p);q['self']=q
        with self.assertRaises(ValueError):self.validate(q,e,c)

    def test_source_hash_or_native_model_claim_cannot_be_rewritten(self):
        p,e,c=fixture();wrong=deepcopy(c);wrong['symbol']='000000'
        with self.assertRaises(ValueError):self.validate(p,e,wrong)
        for field,value in [('capabilityStatus','VERIFIED'),('executionScope','NATIVE_LIVE')]:
            bad=deepcopy(c);bad[field]=value
            with self.assertRaises(ValueError):self.validate(p,e,seal_quantity_contract(bad))


if __name__=='__main__':unittest.main()
