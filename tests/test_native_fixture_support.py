"""Explicit offline KIS account producer for the normal native start fixture.

No account/network/order implementation is installed at import. These synthetic
responses are test data only and have no production evidence or authority.
"""
from contextlib import ExitStack,contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timezone
from unittest.mock import patch

from live_trader.account_profiles import saved_account_profiles
from live_trader.kis_equity_execution import prepare_reviewed_policies
from live_trader.kis_equity_runtime import NativeEquityRuntime
from live_trader.runtime_scope import scope_from_mapping

# This fixture exercises the existing cash-only native path. Integrated-margin
# production remains blocked until its separate financial producer is verified.
SETTINGS={'KIS_OVERSEAS_FUNDING_MODE':'FOREIGN_CASH','KIS_APP_KEY':'SYNTHETIC_NATIVE_FLOW_APP','KIS_APP_SECRET':'SYNTHETIC_NATIVE_FLOW_SECRET',
    'KIS_ACCOUNT_NO':'12345678','KIS_ACCOUNT_PRODUCT_CODE':'01','KIS_ENV':'real'}


def native_fixture_account():
    return next(row for row in saved_account_profiles(SETTINGS) if row['brokerId']=='kis')


class OfflineKisReader:
    def __init__(self,specs,clock):
        self.settings=dict(SETTINGS);self.clock=clock
        self.symbols={spec.symbol for spec in specs};self.events=[]
        self.prices={symbol:'95000' if symbol=='000660' else '103000' for symbol in self.symbols}
        self.positions=[];self.working_orders=[];self.terminal_orders=[]
    def _credentials(self,provider):
        if provider!='kis':raise AssertionError('synthetic KIS provider only')
    def _kis_token(self):return 'synthetic-memory-token'
    def _pace_kis(self):pass
    def _get(self,provider,path,*,query,headers):
        if provider!='kis':raise AssertionError('synthetic KIS GET only')
        self.events.append({'path':path,'query':deepcopy(query),'trId':headers.get('tr_id')})
        if path=='/uapi/domestic-stock/v1/quotations/inquire-price':
            symbol=query.get('FID_INPUT_ISCD')
            if symbol not in self.symbols:raise AssertionError('foreign synthetic quote symbol')
            payload={'rt_cd':'0','output':{'stck_shrn_iscd':symbol,'temp_stop_yn':'N','stck_prpr':self.prices[symbol],
                'aspr_unit':'1','hts_deal_qty_unit_val':'1'}}
        elif path=='/uapi/domestic-stock/v1/trading/inquire-balance':
            payload={'rt_cd':'0','output1':deepcopy(self.positions)}
        elif path=='/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl':
            payload={'rt_cd':'0','output':deepcopy(self.working_orders)}
        elif path=='/uapi/domestic-stock/v1/trading/inquire-psbl-order':
            if query.get('PDNO') not in self.symbols or query.get('ORD_DVSN') not in {'00','01'}:
                raise AssertionError('foreign synthetic cap scope')
            payload={'rt_cd':'0','output':{'ord_psbl_cash':'9000000','nrcvb_buy_amt':'9000000',
                'nrcvb_buy_qty':'100','psbl_qty_calc_unpr':self.prices[query['PDNO']],'max_buy_qty':'100'}}
        elif path=='/uapi/domestic-stock/v1/trading/inquire-daily-ccld':
            payload={'rt_cd':'0','output1':deepcopy(self.terminal_orders)}
        else:
            raise AssertionError('unapproved synthetic GET route')
        # A current official-clock fixture may be needed by STOP cleanup. It is
        # still synthetic; it is not a real HTTP response or private WS notice.
        date=self.clock().astimezone(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
        return payload,{'tr_cont':'D','Date':date,'Age':'0'}


@contextmanager
def native_fixture_clock(clock):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls,tz=None):
            value=clock()
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)
    with ExitStack() as stack:
        for module in ('kis_equity_runtime','kis_owned_capital','kis_equity_execution'):
            stack.enter_context(patch('live_trader.'+module+'.datetime',FrozenDateTime))
        yield


@dataclass
class NativeFixture:
    runtime: NativeEquityRuntime
    reader: OfflineKisReader
    account: dict
    policies: dict
    deployment: dict


def build_native_runtime(root,*,specs,runtime_scope,clock,dispatch_reader=None):
    """Call under native_fixture_clock; scope must use native_fixture_account."""
    scope=scope_from_mapping(runtime_scope);account=native_fixture_account();specs=tuple(specs)
    if (scope.broker_id!='kis' or scope.capital_currency!='KRW' or any(spec.broker_id!='kis' for spec in specs)
            or scope.account_fingerprint!=account['accountFingerprint']
            or scope.credential_configuration_hash!=account['credentialConfigurationHash']
            or scope.account_binding_id!=account['accountBindingId']):
        raise ValueError('synthetic native runtime needs its exact synthetic saved-account scope')
    reviewed=[prepare_reviewed_policies(account,spec,
        authorization_text='OFFLINE SYNTHETIC native fixture only; no real financial authorization',now=clock()) for spec in specs]
    fee=reviewed[0]['feeReservePolicy']
    if any(item['feeReservePolicy']!=fee for item in reviewed):raise AssertionError('synthetic fixture fee scope differs')
    policies={'executionPolicies':{spec.artifact_hash:item['executionPolicy'] for spec,item in zip(specs,reviewed)},
        'feeReservePolicy':fee,'savedSlippageBps':50}
    permissions={'liveReviewBindingHash':'a'*64,'liveAccountProfileId':account['profileId'],
        'liveAccountFingerprint':account['accountFingerprint'],
        'liveAccountCredentialConfigurationHash':account['credentialConfigurationHash'],
        'liveCapitalPolicy':{'basis':'ACCOUNT_AVAILABLE_CASH','percent':'100'},
        'kisEquityExecutionPolicies':policies['executionPolicies'],'kisEquityFeeReservePolicy':fee,'kisSavedSlippageBps':50}
    deployment={'deploymentId':scope.deployment_id,'environment':'SMALL_LIVE','lifecycle':'before-live-small',
        'route':'kis','accountId':account['accountBindingId'],'permissions':permissions}
    reader=OfflineKisReader(specs,clock)
    runtime=NativeEquityRuntime(root,scope.deployment_id,specs,reader=reader,
        deployment_reader=lambda *args:deepcopy(deployment),dispatch_reader=dispatch_reader or (lambda:[]))
    runtime.observer.clock=clock
    return NativeFixture(runtime,reader,account,policies,deployment)
