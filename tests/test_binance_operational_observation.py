"""No-network fixtures for current Binance caps and bounded official clock age."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
import importlib
import types
import unittest

import test_native_spot_preflight as fixture
from trading_runtime.artifact_governance import stable_sha256
from trading_runtime.execution_quantity import seal_quantity_contract

P = fixture.P
pre = fixture.pre
bounds = importlib.import_module(P + '.spot_operational_bounds')
clock = importlib.import_module(P + '.binance_reference_clock')


class BinanceOperationalObservationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.spec = fixture.spec()
        self.source = self.spec.artifact['executionSizing']['quantityContract']
        self.original_artifact = deepcopy(self.spec.artifact)

    def receipt(self, cap='1.23456', *, side='BUY', skew_ms=1780, trade_age_ms=200, minutes=5):
        reader = fixture.Reader(self.spec, cash='1000000')
        market = fixture.market(self.spec)
        market['filters'][1]['maxQty'] = cap
        server = int(self.now.timestamp() * 1000) + skew_ms
        reader.mutations.update({'/api/v3/exchangeInfo': {'symbols': [market]},
                                 '/api/v3/time': {'serverTime': server},
                                 '/api/v3/avgPrice': {'mins': minutes, 'price': '100', 'closeTime': server - trade_age_ms}})
        receipt = pre.observe_native_spot(reader, self.source, side=side, now=self.now)
        self.assertEqual(2, reader.calls.count(('binance', '/api/v3/time')))
        return receipt

    def calculate(self, receipt, *, side='BUY', held='10'):
        return bounds.calculate_live_spot_model(self.spec.artifact['executionSizing'], market_observation=receipt,
            now=self.now, side=side, price='100', deployment_equity='1000000', available_cash='1000000',
            allocation_weight=1, position_percent=100, source_fee_rate='0.001', source_slippage_rate='0.01',
            actual_fee_rate=receipt['feeRate'], position_quantity=held, notional_reference_price='100')

    def binding(self, model, receipt):
        return {'sourceQuantityContract': self.source, 'sourceContractHash': self.source['contractHash'],
                'nativeMarketObservation': receipt, 'marketObservationHash': receipt['contentHash'],
                'currentQuantityContractHash': receipt['currentQuantityContract']['contractHash'],
                'operationalQuantityPolicyHash': model['operationalQuantityPolicy']['contentHash']}

    def test_lower_live_cap_floors_step_preserves_source_and_recomputes_buy_money(self):
        receipt = self.receipt()
        model = self.calculate(receipt)
        self.assertEqual('READY', model['status'])
        # Fraction oracle is independent of Decimal implementation and formatting.
        cap, step = Fraction('1.23456'), Fraction(self.source['stepSize'])
        quantity = (cap // step) * step
        self.assertEqual(Fraction(model['quantity']), quantity)
        self.assertEqual(Fraction(model['notional']), quantity * Fraction('101'))
        self.assertEqual(Fraction(model['fee']), quantity * Fraction('101') * Fraction(receipt['feeRate']))
        self.assertEqual(Fraction(model['totalSpend']), Fraction(model['notional']) + Fraction(model['fee']))
        self.assertEqual(self.original_artifact, self.spec.artifact)
        self.assertEqual(self.source['contractHash'], model['contractHash'])
        self.assertEqual('LIVE_OPERATIONAL_BOUND_NOT_SOURCE_CORRECTION', model['operationalQuantityPolicy']['role'])
        self.assertGreater(Decimal(model['sourceModel']['quantity']), Decimal(model['quantity']))
        self.assertFalse(bounds.validate_operational_ticket(model, self.binding(model, receipt), side='BUY', now=self.now)['orderAuthorityGranted'])

    def test_lower_cap_sell_preserves_unsold_owned_quantity_and_net_proceeds(self):
        receipt = self.receipt(side='SELL')
        model = self.calculate(receipt, side='SELL', held='1500')
        self.assertEqual('READY', model['status'])
        self.assertEqual('BLOCKED', model['sourceModel']['status'])
        self.assertEqual(Fraction('1500') - Fraction(model['quantity']), Fraction(model['residualQuantity']))
        self.assertEqual(Fraction(model['quantity']) * 99 * (1-Fraction(receipt['feeRate'])), Fraction(model['netProceeds']))
        bounds.validate_operational_ticket(model, self.binding(model, receipt), side='SELL', now=self.now)

    def test_raised_current_maximum_still_uses_frozen_source_cap(self):
        receipt = self.receipt('2000')
        model = self.calculate(receipt)
        self.assertEqual(self.source['maxQuantity'], model['operationalQuantityPolicy']['effectiveMaximumQuantity'])
        self.assertEqual(model['sourceModel']['quantity'], model['quantity'])

    def test_lower_cap_cannot_bypass_min_quantity_or_min_notional(self):
        for cap in ('0.00001', '0.01'):
            with self.subTest(cap=cap):
                if cap == '0.00001':
                    with self.assertRaises(ValueError):
                        self.receipt(cap)
                else:
                    self.assertEqual('BLOCKED', self.calculate(self.receipt(cap))['status'])

    def test_unrelated_current_rule_change_remains_blocked(self):
        for changes in ({'stepSize': '0.0001'}, {'minQuantity': '0.01'}, {'minNotional': '6'}, {'notionalReferenceMinutes': 1}):
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, 'CURRENT_NATIVE_RULES_DIFFER_FROM_SOURCE'):
                pre.source_rules_fit_current(self.source, seal_quantity_contract({**self.source, **changes}))

    def test_final_binding_rejects_changed_receipt_cap_source_model_or_quantity(self):
        receipt = self.receipt()
        model = self.calculate(receipt)
        binding = self.binding(model, receipt)
        for label in ('receipt', 'policy', 'source', 'quantity', 'spend'):
            changed_model, changed_binding = deepcopy(model), deepcopy(binding)
            if label == 'receipt':
                changed_binding['nativeMarketObservation'] = self.receipt('1.1')
            elif label == 'policy':
                changed_model['operationalQuantityPolicy']['effectiveMaximumQuantity'] = '2'
            elif label == 'source':
                changed_model['sourceModel']['quantity'] = '42'
            elif label == 'quantity':
                changed_model['quantity'] = '1.235'
            else:
                changed_model['totalSpend'] = '1'
            with self.subTest(label=label), self.assertRaises(ValueError):
                bounds.validate_operational_ticket(changed_model, changed_binding, side='BUY', now=self.now)

    def test_official_clock_accepts_local_future_trade_only_when_past_server(self):
        receipt = self.receipt(skew_ms=1780, trade_age_ms=200)
        reference = receipt['marketReference']
        self.assertGreater(pre.timestamp(reference['closeTime']), self.now)
        self.assertEqual('LAST_TRADE_TIME', reference['closeTimeRole'])
        self.assertEqual(clock.ROLE, reference['clockObservation']['role'])
        pre.valid_observation(receipt, self.source, side='BUY', now=self.now)

    def test_skew_over_fifteen_seconds_stale_trade_wrong_interval_future_server_block(self):
        for kwargs in ({'skew_ms': 15001}, {'skew_ms': -15001}, {'trade_age_ms': 15001}, {'trade_age_ms': -1}, {'minutes': 1}, {'minutes': True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.receipt(**kwargs)

    def test_clock_missing_slow_rtt_reordered_and_queued_expiry_block(self):
        receipt = self.receipt()
        for label in ('missing', 'roundtrip', 'sequence', 'expired'):
            altered = deepcopy(receipt)
            reference = altered['marketReference']
            at = self.now
            if label == 'missing':
                reference.pop('clockObservation')
            elif label == 'roundtrip':
                reference['clockObservation']['roundTripNanoseconds'] = 15_000_000_001
            elif label == 'sequence':
                reference['responseReceivedAt'] = (self.now+timedelta(seconds=1)).isoformat()
            else:
                at += timedelta(seconds=15)
            altered = pre.seal(altered)
            with self.subTest(label=label), self.assertRaises(ValueError):
                pre.valid_observation(altered, self.source, side='BUY', now=at)

    def test_live_consumer_and_preview_expose_capped_model_and_exact_binding(self):
        receipt = self.receipt()
        context = {**fixture.base.context(self.spec.artifact['executionSizing']), 'asOf': receipt['asOf'],
                   'sourceArtifactHash': fixture.SHA, 'portfolioHash': fixture.PH,
                   'deploymentEquity':'1000000', 'availableCash':'1000000',
                   'currentQuantityContract':receipt['currentQuantityContract'], 'nativeMarketObservation':receipt,
                   'actualFeeRate':receipt['feeRate'], 'notionalReferencePrice':'100'}
        result = fixture.base.service.live_quantity(self.spec, side='BUY', price='100', allocation_weight=1,
            identity={'deploymentId':'deployment'}, context=context, now=self.now)
        self.assertEqual('READY', result['status'], result.get('reasons'))
        self.assertEqual('1.234', result['modelResult']['quantity'])
        self.assertEqual(receipt, result['preflightBinding']['nativeMarketObservation'])
        preview = importlib.import_module(P+'.cash_preparation_preview')
        instrument = {'executionSizing':self.spec.artifact['executionSizing'], 'allocationWeight':1,
                      'sourceMaxOrderPercent':100, 'executionPolicy':{'feePercent':'0.1','slippagePercent':0}}
        observation = {**receipt, 'nativeMarketObservation':receipt, 'currentQuantityContract':receipt['currentQuantityContract'],
                       'feeRate':receipt['feeRate'], 'notionalReferencePrice':'100'}
        proposal = preview.cash_preview(instrument, observation, {'side':'BUY','limitPrice':'100'})
        self.assertEqual('1.234', proposal['proposedInitialQuantity'])
        self.assertFalse(proposal['orderSubmissionEnabled'])


if __name__ == '__main__':
    unittest.main()
