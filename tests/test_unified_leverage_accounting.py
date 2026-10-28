"""Offline actual adapter sizing against the shared Decimal golden cases."""
from decimal import Decimal
import copy,json
from pathlib import Path
import unittest
import test_short_live_brokers as fixture
import test_short_live_composition as comp
from live_trader.leverage_accounting import model_binding

GOLDEN=Path(__file__).resolve().parents[3]/'packages/trading_runtime/tests/fixtures/unified_futures_accounting_v1.json'

class UnifiedLeverageSizingTests(unittest.TestCase):
    def test_actual_live_adapter_matches_every_margin_budget_golden_without_http(self):
        for case in json.loads(GOLDEN.read_text())['cases']:
            with self.subTest(case=case['id']):
                f=fixture.Fixture('BINANCE_USDM_FUTURES');lev=case['executionLeverage']
                f.manifest['futuresPolicy'].update(leverage=lev,stopLossPercent=.1)
                f.manifest['routeBudgetKrw']=1000
                f.quote.update(nativePrice=case['entryPrice'],markPrice=case['entryPrice'],leverage=str(lev),
                    feeRate=case['entryFeeRate'],fundingRate='0',lotStep='0.00000000001',minQuantity='0.00000000001')
                ready=f.adapter.readiness(f.manifest);self.assertTrue(ready['ready'],ready)
                side='BUY' if case['direction']=='long' else 'SELL'
                proposal=f.proposal(str(1000*lev),side=side,direction=case['direction'])
                proposal['capitalBudgetKrw']='1000'
                result=f.adapter.size_order(proposal)
                self.assertTrue(result['ready'],result)
                accounting=result['executionAccounting'];expected=case['expected']
                for key,expected_key in [('positionNotionalKrw','positionNotional'),('initialMarginKrw','initialMargin'),
                    ('entryFeeEstimateKrw','entryFee'),('costReserveKrw','costReserve')]:
                    actual=Decimal(accounting[key]);target=Decimal(expected[expected_key])
                    self.assertLessEqual(abs(actual-target),max(Decimal('0.00000001'),abs(target)*Decimal('1e-10')))
                self.assertLessEqual(Decimal(result['requiredCapitalKrw']),Decimal(1000))
                self.assertEqual(lev,accounting['executionLeverage'])
                self.assertEqual([],f.calls)

    def test_partial_capital_budget_includes_cost_instead_of_spending_other_sleeves_cash(self):
        f=fixture.Fixture('BINANCE_USDM_FUTURES')
        f.manifest['futuresPolicy'].update(leverage=10,stopLossPercent=1)
        f.quote.update(leverage='10',fundingRate='0',lotStep='.0000001',minQuantity='.0000001')
        ready=f.adapter.readiness(f.manifest);self.assertTrue(ready['ready'],ready)
        result=f.adapter.size_order({**f.proposal('2500'),'capitalBudgetKrw':'250'})
        self.assertTrue(result['ready'],result)
        self.assertLessEqual(Decimal(result['requiredCapitalKrw']),Decimal(250))
        self.assertLess(Decimal(result['estimatedNotionalKrw']),Decimal(2500))

    def test_maximum_is_a_ceiling_and_different_execution_multiplier_is_bound(self):
        spec={'route':'BINANCE_USDM_FUTURES','strategyInstanceId':'one','artifactHash':'a'*64,'artifact':{}}
        first=model_binding([spec],2);second=model_binding([spec],10)
        self.assertNotEqual(first['bindingHash'],second['bindingHash'])
        self.assertTrue(first['overrideRequired']);self.assertFalse(first['promotionEligible'])
        self.assertEqual('UNVERIFIED_LEGACY',first['sources'][0]['status'])
        spec['artifact']={'futuresExecutionPolicy':{'marginMode':'ISOLATED','maxLeverageMultiplier':2,'perTradeRiskPercent':1,'maxNotionalPercent':100}}
        self.assertEqual(2,model_binding([spec],1)['maxLeverageMultiplier'])
        with self.assertRaisesRegex(ValueError,'EXCEEDS_SOURCE_MAXIMUM'):model_binding([spec],3)


class ModelConsentBoundaryTests(unittest.TestCase):
    setUp=comp.ShortLiveCompositionTests.setUp
    enter=comp.ShortLiveCompositionTests.enter
    fixture_for=comp.ShortLiveCompositionTests.fixture_for
    def test_partial_futures_portfolio_does_not_renormalize_its_unallocated_cash(self):
        from trading_runtime.artifact_governance import seal_portfolio_artifact
        f=self.fixture_for('BINANCE_USDM_FUTURES',futures_leverage=2,portfolio=True)
        path=f.catalog_root/'portfolios/fixture-portfolio.json'
        body=json.loads(path.read_text());body['strategyInstances'][0]['allocation']['normalizedWeight']=.4
        path.write_text(json.dumps(seal_portfolio_artifact(body)))
        targets=[row for row in f.catalog.snapshot()['candidates'] if row['kind']=='PORTFOLIO_INSTANCE']
        self.assertFalse(targets[0]['blockers'],targets)
        f.selections=[{key:row[key] for key in ('selectionKey','artifactFileSha256','instanceFileSha256')} for row in targets]
        prepared=f.prepare()
        preview=prepared['brokerReadiness'][0]['executionAccounting']['previews'][0]
        self.assertEqual(Decimal(40000),Decimal(preview['capitalBudgetKrw']))
        f.start();view=f.forward()['runtime']
        self.assertEqual('RUNNING',view['status'],view)
        order=f.mutations[0]['order']
        self.assertAlmostEqual(40000,order['capitalBudgetKrw'])
        self.assertLessEqual(Decimal(str(order['requiredCapitalKrw'])),Decimal(40000))
        self.assertLess(Decimal(str(order['estimatedNotionalKrw'])),Decimal(80000))
        self.assertTrue(f.service.stop({})['runtime']['terminalVerified'])

    def test_missing_or_wrong_model_binding_is_rejected_before_start_and_any_order(self):
        f=self.fixture_for('BINANCE_USDM_FUTURES',futures_leverage=10,direction='short')
        prepared=f.prepare()
        for override in (None,'0'*64):
            with self.assertRaisesRegex(Exception,'EXECUTION_MODEL_OVERRIDE_CONFIRMATION_REQUIRED'):
                f.service.start({'preparationId':prepared['preparationId'],'scopeHash':prepared['scopeHash'],
                    'confirmed':True,'confirmationText':'실제 주문 시험 시작','executionModelOverrideHash':override})
        self.assertEqual([],f.mutations);self.assertEqual([],f.protection_mutations)
        self.assertIsNone(f.service._session)


if __name__=='__main__':unittest.main()
