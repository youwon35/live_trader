"""Authenticated GET-only KIS v2 observations with exact venue/query scope.

Credentials and raw account payloads stay in memory. This service cannot POST an
order or cancel. Only the existing restricted reader may acquire an OAuth token.
"""
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
import re
from .account_profiles import saved_account_profiles
from .live_adapters import split_kis_account
from .native_spot_quantity import decimal_value
from .read_only_account import ReadFailure, KIS_TR_IDS
from .kis_equity_execution import VENUES, OBSERVATION, limit_price, seal, text
from trading_runtime.artifact_governance import stable_sha256


class KisEquityReader:
    def __init__(self, reader, *, clock=None):
        self.reader=reader; self.clock=clock or (lambda:datetime.now(timezone.utc))

    def account(self):
        self.reader._credentials('kis')
        if self.reader.settings.get('KIS_ENV','').lower() not in {'','real','live'}:
            raise ReadFailure('KIS_V2_REAL_ENVIRONMENT_REQUIRED')
        accounts=[p for p in saved_account_profiles(self.reader.settings) if p['brokerId']=='kis']
        if len(accounts)!=1: raise ReadFailure('KIS_V2_CURRENT_SAVED_ACCOUNT_REQUIRED')
        return accounts[0]

    def _get(self,path,query,*,continuation=''):
        token=self.reader._kis_token()
        self.reader._pace_kis()
        headers={'authorization':'Bearer '+token,'appkey':self.reader.settings['KIS_APP_KEY'],
                 'appsecret':self.reader.settings['KIS_APP_SECRET'],'tr_id':KIS_TR_IDS[path],'custtype':'P'}
        if continuation:headers['tr_cont']=continuation
        payload,response_headers=self.reader._get('kis',path,query=query,headers=headers)
        if not isinstance(payload,dict) or str(payload.get('rt_cd'))!='0':raise ReadFailure('KIS_V2_READ_REJECTED')
        return payload,{str(k).lower():str(v).strip() for k,v in response_headers.items()}

    @staticmethod
    def _output(payload,headers):
        if headers.get('tr_cont','').upper() not in {'','D','E'}:raise ReadFailure('KIS_V2_SINGLE_RESPONSE_CONTINUATION_UNSUPPORTED')
        value=payload.get('output')
        if isinstance(value,list) and len(value)==1:value=value[0]
        if not isinstance(value,dict):raise ReadFailure('KIS_V2_INVALID_RESPONSE')
        return value

    def _pages(self,path,extra,*,context,output='output'):
        cano,product=split_kis_account(self.reader.settings['KIS_ACCOUNT_NO'],self.reader.settings['KIS_ACCOUNT_PRODUCT_CODE'])
        rows=[]; seen=set(); fk=nk=''
        for index in range(100):
            payload,headers=self._get(path,{'CANO':cano,'ACNT_PRDT_CD':product,**extra,'CTX_AREA_FK'+context:fk,'CTX_AREA_NK'+context:nk},continuation='N' if index else '')
            page=payload.get(output)
            if not isinstance(page,list) or any(not isinstance(row,dict) for row in page):raise ReadFailure('KIS_V2_PAGINATION_INVALID')
            rows.extend(page)
            if len(rows)>10000:raise ReadFailure('KIS_V2_ACCOUNT_SCOPE_TOO_LARGE')
            continuation=headers.get('tr_cont','').upper()
            if continuation not in {'','D','E','F','M'}:raise ReadFailure('KIS_V2_CONTINUATION_STATUS_UNKNOWN')
            if continuation not in {'F','M'}:return rows
            fk,nk=str(payload.get('ctx_area_fk'+context) or ''),str(payload.get('ctx_area_nk'+context) or '')
            if not (fk or nk) or (fk,nk) in seen:raise ReadFailure('KIS_V2_PAGINATION_INCOMPLETE')
            seen.add((fk,nk))
        raise ReadFailure('KIS_V2_PAGINATION_LIMIT')

    @staticmethod
    def _identity(exchange,symbol):
        if exchange not in VENUES or (exchange=='KRX' and not re.fullmatch('[0-9]{6}',symbol)) or (exchange!='KRX' and not re.fullmatch(r'[A-Z][A-Z.\-]{0,9}',symbol)):
            raise ReadFailure('KIS_V2_EXACT_INSTRUMENT_REQUIRED')

    def quote(self,exchange,symbol):
        self._identity(exchange,symbol); self.account()
        if exchange=='KRX':
            payload,headers=self._get('/uapi/domestic-stock/v1/quotations/inquire-price',{'FID_COND_MRKT_DIV_CODE':'J','FID_INPUT_ISCD':symbol})
            quote=self._output(payload,headers)
            if quote.get('stck_shrn_iscd')!=symbol or quote.get('temp_stop_yn')!='N':raise ReadFailure('KIS_V2_DOMESTIC_QUOTE_SCOPE_OR_HALT')
            price=decimal_value(quote.get('stck_prpr'),'current mark',positive=True)
            tick=decimal_value(quote.get('aspr_unit'),'current price tick',positive=True)
            lot=decimal_value(quote.get('hts_deal_qty_unit_val'),'current lot',positive=True)
        else:
            code=VENUES[exchange][1]
            payload,headers=self._get('/uapi/overseas-price/v1/quotations/price-detail',{'AUTH':'','EXCD':code,'SYMB':symbol})
            quote=self._output(payload,headers)
            # rsym is an official real-time instrument identifier, not an echo
            # of the query price. All US venues were independently observed.
            enabled=str(quote.get('e_ordyn') or '').replace(' ','')
            if quote.get('rsym')!='D'+code+symbol or quote.get('curr')!='USD' or enabled not in {'거래가능','매매가능'}:
                raise ReadFailure('KIS_V2_US_QUOTE_SCOPE_CURRENCY_OR_HALTED')
            price=decimal_value(quote.get('last'),'current mark',positive=True)
            tick=decimal_value(quote.get('e_hogau'),'current price tick',positive=True)
            lot=decimal_value(quote.get('vnit'),'current lot',positive=True)
            places=decimal_value(quote.get('zdiv'),'wire decimal places')
            if places!=places.to_integral_value() or places>4 or tick<Decimal(10)**-int(places):
                raise ReadFailure('KIS_V2_US_PRICE_PRECISION_UNSUPPORTED')
        if lot!=1:raise ReadFailure('KIS_V2_WHOLE_SHARE_LOT_ONE_REQUIRED')
        return {'exchange':exchange,'symbol':symbol,'currency':VENUES[exchange][0],
                'price':text(price),'priceTick':text(tick),'lotSize':'1','orderable':True,
                'quoteHash':stable_sha256(quote),'quotePriceRole':'CURRENT_MARK_NOT_NEXT_OPEN',
                'marketEventFreshness':'NOT_PROVEN_BY_RESPONSE_RECEIPT_TIME'}

    def account_scope(self,exchange,symbol):
        """Complete balance/working-order scope. NASD balance includes US venues;
        every returned position must still carry its own exact exchange/currency.
        """
        domestic=exchange=='KRX'
        prefix='/uapi/'+('domestic' if domestic else 'overseas')+'-stock/v1/trading/'
        balance_params=({'AFHR_FLPR_YN':'N','OFL_YN':'','INQR_DVSN':'02','UNPR_DVSN':'01',
                         'FUND_STTL_ICLD_YN':'N','FNCG_AMT_AUTO_RDPT_YN':'N','PRCS_DVSN':'00'} if domestic else
                        {'OVRS_EXCG_CD':exchange,'TR_CRCY_CD':'USD'})
        positions=self._pages(prefix+'inquire-balance',balance_params,context='100' if domestic else '200',output='output1')
        pending=self._pages(prefix+('inquire-psbl-rvsecncl' if domestic else 'inquire-nccs'),
            {'INQR_DVSN_1':'0','INQR_DVSN_2':'0'} if domestic else {'OVRS_EXCG_CD':'NASD','SORT_SQN':'DS'},context='100' if domestic else '200')
        matches=[]
        for row in positions:
            if str(row.get('pdno' if domestic else 'ovrs_pdno') or '')!=symbol:continue
            if not domestic and (row.get('ovrs_excg_cd')!=exchange or row.get('tr_crcy_cd')!='USD'):
                raise ReadFailure('KIS_V2_POSITION_VENUE_OR_CURRENCY_MISMATCH')
            if domestic and str(row.get('loan_dt') or '').strip() not in {'','00000000'}:
                raise ReadFailure('KIS_V2_CASH_ONLY_NO_CREDIT_POSITIONS')
            matches.append(row)
        if len(matches)>1:raise ReadFailure('KIS_V2_AMBIGUOUS_POSITION_SCOPE')
        row=matches[0] if matches else {}
        holding=decimal_value(row.get('hldg_qty' if domestic else 'ovrs_cblc_qty',0),'holding quantity')
        sellable=decimal_value(row.get('ord_psbl_qty',0),'sellable quantity')
        if holding!=holding.to_integral_value() or sellable!=sellable.to_integral_value() or sellable>holding:
            raise ReadFailure('KIS_V2_POSITION_WHOLE_SHARE_TRUTH_INVALID')
        return {'holdingQuantity':text(holding),'sellableQuantity':text(sellable),
            'openOrders':'PRESENT' if pending else 'NONE_OBSERVED_COMPLETE_ACCOUNT_CURRENCY_SCOPE',
            'positionResponseHash':stable_sha256(positions),'workingOrdersResponseHash':stable_sha256(pending)}

    def observe(self,contract,*,side,raw_open,protective_bps):
        started=self.clock(); account=self.account(); exchange=contract['exchange'];symbol=contract['symbol']
        if contract['quoteCurrency']!=VENUES[exchange][0] or side not in {'BUY','SELL'}:raise ReadFailure('KIS_V2_EXACT_SCOPE_REQUIRED')
        quote=self.quote(exchange,symbol)
        limit=limit_price(raw_open,side,quote['priceTick'],protective_bps)
        positions=self.account_scope(exchange,symbol)
        cano,product=split_kis_account(self.reader.settings['KIS_ACCOUNT_NO'],self.reader.settings['KIS_ACCOUNT_PRODUCT_CODE'])
        if exchange=='KRX':
            base={'CANO':cano,'ACNT_PRDT_CD':product,'PDNO':symbol,'ORD_UNPR':limit,'ORD_DVSN':'00','CMA_EVLU_AMT_ICLD_YN':'N','OVRS_ICLD_YN':'N'}
            payload,headers=self._get('/uapi/domestic-stock/v1/trading/inquire-psbl-order',base)
            caps=self._output(payload,headers)
            # Official API warns class00 quantity may omit the security's margin
            # ratio. Also obtain no-margin market-class quantity and take min.
            market_payload,market_headers=self._get('/uapi/domestic-stock/v1/trading/inquire-psbl-order',{**base,'ORD_DVSN':'01'})
            market=self._output(market_payload,market_headers)
            cash=min(decimal_value(caps.get('ord_psbl_cash'),'cash'),decimal_value(caps.get('nrcvb_buy_amt'),'no margin cash'),decimal_value(market.get('nrcvb_buy_amt'),'market no margin cash'))
            maximum=min(decimal_value(caps.get('nrcvb_buy_qty'),'limit cap'),decimal_value(market.get('nrcvb_buy_qty'),'market no margin cap'))
            cap_hash=stable_sha256({'limit':caps,'marketNoMarginCap':market})
            calculation=market.get('psbl_qty_calc_unpr')
        else:
            query={'CANO':cano,'ACNT_PRDT_CD':product,'OVRS_EXCG_CD':exchange,'OVRS_ORD_UNPR':limit,'ITEM_CD':symbol}
            payload,headers=self._get('/uapi/overseas-stock/v1/trading/inquire-psamount',query)
            caps=self._output(payload,headers)
            if caps.get('tr_crcy_cd')!='USD':raise ReadFailure('KIS_V2_ORDERABLE_CURRENCY_MISMATCH')
            cash=min(decimal_value(caps.get('ord_psbl_frcr_amt'),'own foreign cash'),decimal_value(caps.get('ovrs_ord_psbl_amt'),'overseas cash cap'))
            maximum=decimal_value(caps.get('ord_psbl_qty'),'no auto fx quantity')
            cap_hash=stable_sha256(caps);calculation=None
        if maximum!=maximum.to_integral_value():raise ReadFailure('KIS_V2_BROKER_WHOLE_SHARE_CAP_REQUIRED')
        finished=self.clock()
        if not 0<=(finished-started).total_seconds()<=15 or self.account()!=account:raise ReadFailure('KIS_V2_MULTIPLE_READS_STALE_OR_ACCOUNT_CHANGED')
        return seal({'version':OBSERVATION,'brokerId':'kis','exchange':exchange,'symbol':symbol,'side':side,
            'currency':VENUES[exchange][0],'profileId':account['profileId'],'accountFingerprint':account['accountFingerprint'],
            'credentialConfigurationHash':account['credentialConfigurationHash'],'orderClass':'00',
            'queryPrice':limit,'queryPriceRole':'REQUEST_LIMIT_PRICE_NOT_BROKER_ECHO',
            'brokerCalculationPrice':str(calculation) if calculation is not None else None,
            'brokerCalculationPriceRole':'KR_MARKET_QUANTITY_CALCULATION_ONLY' if calculation is not None else 'NOT_PROVIDED',
            'availableCash':text(cash),'maximumBuyQuantity':text(maximum),**positions,
            'priceTick':quote['priceTick'],'lotSize':'1','quotePrice':quote['price'],'quoteHash':quote['quoteHash'],
            'quotePriceRole':'CURRENT_MARK_NOT_NEXT_OPEN','orderable':True,'complete':True,
            'capResponseHash':cap_hash,'startedAt':started.isoformat(),'asOf':finished.isoformat(),
            'actualPersonalFeeObserved':False,'orderAuthorityGranted':False})

    def terminal_orders(self,exchange,day):
        self.account()
        if not re.fullmatch('[0-9]{8}',day):raise ReadFailure('KIS_V2_EXACT_ORDER_DATE_REQUIRED')
        if exchange=='KRX':
            return self._pages('/uapi/domestic-stock/v1/trading/inquire-daily-ccld',
                {'INQR_STRT_DT':day,'INQR_END_DT':day,'SLL_BUY_DVSN_CD':'00','INQR_DVSN':'00','PDNO':'',
                 'CCLD_DVSN':'00','ORD_GNO_BRNO':'','ODNO':'','INQR_DVSN_3':'00','INQR_DVSN_1':'','INQR_DVSN_2':''},context='100',output='output1')
        return self._pages('/uapi/overseas-stock/v1/trading/inquire-ccnl',
            {'PDNO':'%','ORD_STRT_DT':day,'ORD_END_DT':day,'SLL_BUY_DVSN':'00','CCLD_NCCS_DVSN':'00',
             'OVRS_EXCG_CD':exchange,'SORT_SQN':'DS','ORD_DT':'','ORD_GNO_BRNO':'','ODNO':''},context='200',output='output')
