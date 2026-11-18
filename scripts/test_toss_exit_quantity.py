"""Real Live dispatch/ledger code with the existing fake broker only."""
from dataclasses import replace
from decimal import Decimal
import unittest
import test_toss_cash_operation as fixture

class TossExitQuantityTests(unittest.TestCase):
    def fixture(self,*,percent=50,second=False):
        case=fixture.OperationTests(methodName='runTest');case.setUp();self.addCleanup(case.doCleanups)
        s=fixture.spec();s=replace(s,parameters={**s.parameters,**({'positionSize':percent} if percent else {})})
        case.specs=[s]+([replace(s,strategy_instance_id='other')] if second else [])
        config={'targets':[{'fixture':'immutable'}],'tossBindingConfirmed':True,
            'budgetKrwByMarket':{'KR':2000 if second else 1000,'US':300000},'durationHours':6}
        p=case.op.prepare(config)['preparation']
        case.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
        return case
    def dispatch(self,case,s,side):
        case.clock.advance(1)
        case.op.dispatch_signal(s,case.decision(s=s,side=side),case.clock())
        return list(case.api.book.values())[-1]
    def fill(self,case,row):case.api.fill(row['orderId']);case.op.reconcile()
    def test_percentage_entry_many_then_owned_exit_all_excludes_existing_shares(self):
        c=self.fixture();s=c.specs[0];buy=self.dispatch(c,s,'BUY');self.assertEqual('4',buy['quantity']);self.fill(c,buy)
        sell=self.dispatch(c,s,'SELL');self.assertEqual('4',sell['quantity']);self.fill(c,sell)
        self.assertEqual(0,Decimal(c.op.ledger.project(c.op.owner)['positions'][s.strategy_instance_id]))
        self.assertEqual(c.api.original,c.api.positions)
    def test_fixed_entry_one_still_exits_one(self):
        c=self.fixture(percent=None);s=c.specs[0];buy=self.dispatch(c,s,'BUY');self.assertEqual('1',buy['quantity']);self.fill(c,buy)
        self.assertEqual('1',self.dispatch(c,s,'SELL')['quantity'])
    def test_owned_four_broker_sellable_two_caps_to_two(self):
        c=self.fixture();s=c.specs[0];self.fill(c,self.dispatch(c,s,'BUY'))
        c.api.sellable_quantity=lambda symbol:{'sellableQuantity':'2'}
        sell=self.dispatch(c,s,'SELL');self.assertEqual('2',sell['quantity']);self.fill(c,sell)
        self.assertEqual(2,Decimal(c.op.ledger.project(c.op.owner)['positions'][s.strategy_instance_id]))
        self.assertEqual(Decimal(6),c.api.positions['005930'])
    def test_same_symbol_other_strategy_and_external_shares_excluded(self):
        c=self.fixture(second=True)
        for s in c.specs:self.fill(c,self.dispatch(c,s,'BUY'))
        self.assertEqual(Decimal(12),c.api.positions['005930'])
        self.fill(c,self.dispatch(c,c.specs[0],'SELL'))
        positions=c.op.ledger.project(c.op.owner)['positions']
        self.assertEqual(0,Decimal(positions[c.specs[0].strategy_instance_id]))
        self.assertEqual(4,Decimal(positions[c.specs[1].strategy_instance_id]))
        self.assertEqual(Decimal(8),c.api.positions['005930']);self.assertEqual(Decimal(2),c.api.positions['AAPL'])
    def test_native_and_partial_exit_source_rejected_before_broker_reads(self):
        for change in ({'executionSizing':{'quantityContract':{'symbol':'005930'}}},
                       {'strategyInstance':{'executionSizing':{'quantityContract':{'schemaVersion':'native-spot-v1'}}}},
                       {'parameters':{'closeFraction':.5}}):
            c=fixture.OperationTests(methodName='runTest');c.setUp();self.addCleanup(c.doCleanups)
            s=c.specs[0];c.specs[0]=replace(s,artifact={**s.artifact,**change},parameters=change.get('parameters',s.parameters))
            c.op.factory=lambda *a:self.fail('Unsupported source must fail before broker reads')
            with self.assertRaisesRegex(ValueError,'TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED'):c.prepare()
    def test_partial_exit_decision_rejected_before_broker_submission(self):
        c=self.fixture();s=c.specs[0];self.fill(c,self.dispatch(c,s,'BUY'));before=len(c.api.calls)
        decision=replace(c.decision(s=s,side='SELL'),metadata={'closeFraction':.5})
        with self.assertRaisesRegex(ValueError,'TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED'):
            c.op.dispatch_signal(s,decision,c.clock())
        self.assertEqual(before,len(c.api.calls))

if __name__=='__main__':unittest.main()
