from dataclasses import replace
from decimal import Decimal
import unittest
from unittest.mock import patch
import test_toss_cash_operation as fixture
from trading_runtime.portfolio_runtime import BuiltinBarSignalEvaluator

class ZeroExitSizingTests(unittest.TestCase):
 def operation(self,extra=None):
  case=fixture.OperationTests(methodName='runTest');case.setUp();self.addCleanup(case.doCleanups)
  s=fixture.spec();s=replace(s,plugin_id='threshold_momentum',parameters={**s.parameters,
    'positionSize':50,'entry_below':102,'exit_above':110,**(extra or {})});case.specs=[s]
  config={'targets':[{'fixture':'immutable'}],'tossBindingConfirmed':True,'budgetKrwByMarket':{'KR':1000,'US':300000},'durationHours':6}
  p=case.op.prepare(config)['preparation'];case.op.start({'preparationId':p['preparationId'],'sourceHash':p['sourceHash'],'liveConsent':True},spawn=False)
  case.op.evaluator=BuiltinBarSignalEvaluator(lambda spec:float(case.op.ledger.project(case.op.owner)['positions'].get(spec.strategy_instance_id,0)))
  case.clock.advance(60);end=int(case.clock()//60)*60
  case.op.ingest_bar(replace(fixture.bar(s,end),open=100,high=102,low=89,close=90))
  buy=list(case.api.book.values())[-1];case.api.fill(buy['orderId']);case.op.reconcile()
  self.assertEqual('4',buy['quantity'])
  return case,s
 def test_actual_bar_evaluator_pending_dispatch_zero_never_sells(self):
  for field in ('quantity','exitPercent','closeFraction'):
   for value in (0,0.0,Decimal('0')):
    with self.subTest(field=field,value=repr(value)):
     case,s=self.operation();evaluator=case.op.evaluator;signals=[]
     def evaluate(*args):
      decision=evaluator(*args);signals.append(decision.signal)
      return replace(decision,metadata={**decision.metadata,field:value})
     case.op.evaluator=evaluate;case.clock.advance(60);end=int(case.clock()//60)*60
     with patch.object(case.api,'orderbook',wraps=case.api.orderbook)as book,self.assertRaisesRegex(ValueError,'TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED'):
      case.op.ingest_bar(replace(fixture.bar(s,end),open=100,high=121,low=99,close=120))
     book.assert_not_called();self.assertEqual(['SELL'],signals);self.assertEqual(1,len(case.api.book))
     self.assertEqual(4,Decimal(case.op.ledger.project(case.op.owner)['positions'][s.strategy_instance_id]))
     self.assertEqual(Decimal(8),case.api.positions['005930'])
 def test_zero_exit_source_declaration_is_rejected_before_broker_reads(self):
  for field in ('exitPercent','closeFraction'):
   case=fixture.OperationTests(methodName='runTest');case.setUp();self.addCleanup(case.doCleanups)
   case.specs[0]=replace(case.specs[0],parameters={**case.specs[0].parameters,field:0})
   case.op.factory=lambda *args:self.fail('No broker read for an unsupported zero contract')
   with self.subTest(field=field),self.assertRaisesRegex(ValueError,'TOSS_EXIT_QUANTITY_CONTRACT_UNSUPPORTED'):case.prepare()
 def test_disabled_false_preserves_owned_full_exit_and_external_holdings(self):
  case,s=self.operation();evaluator=case.op.evaluator
  def evaluate(*args):
   decision=evaluator(*args);return replace(decision,metadata={**decision.metadata,'partialExit':False,'positionQuantity':0})
  case.op.evaluator=evaluate;case.clock.advance(60);end=int(case.clock()//60)*60
  case.op.ingest_bar(replace(fixture.bar(s,end),open=100,high=121,low=99,close=120))
  sell=list(case.api.book.values())[-1];self.assertEqual('4',sell['quantity']);case.api.fill(sell['orderId']);case.op.reconcile()
  self.assertEqual(case.api.original,case.api.positions)

if __name__=='__main__':unittest.main()
