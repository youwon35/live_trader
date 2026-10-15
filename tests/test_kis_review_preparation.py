from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import tempfile,unittest
from live_trader.paper_candidate_review import _review_execution_policies
from test_kis_equity_operational import fixture, ACCOUNT, NOW

class KisReviewPreparationTest(unittest.TestCase):
    def prepare(self,specs,**kwargs):
        return _review_execution_policies(Path("unused-no-files"),None,None,ACCOUNT,{"savedSlippageBps":50},specs=specs,now=kwargs.get("now",NOW))
    def test_same_review_hour_preserves_challenge_context_without_writes(self):
        spec,_,_=fixture();before=deepcopy(spec.artifact)
        self.assertEqual(self.prepare([spec]),self.prepare([spec],now=NOW.replace(minute=0,second=0,microsecond=0)))
        self.assertEqual(before,spec.artifact)
    def test_three_us_venues_share_currency_fee_policy_and_pin_each_source(self):
        specs=[fixture(exchange)[0] for exchange in ("NASD","NYSE","AMEX")]
        for index,spec in enumerate(specs):spec.artifact_hash=str(index+1)*64
        result=self.prepare(specs)
        self.assertEqual(["AMEX","NASD","NYSE"],result["kisEquityFeeReservePolicy"]["venues"])
        self.assertEqual(3,len(result["kisEquityExecutionPolicies"]))
        self.assertFalse(result["kisEquityFeeReservePolicy"]["actualPersonalFeeObserved"])
        self.assertEqual("LIMIT",result["kisEquityExecutionPolicies"][specs[0].artifact_hash]["executionOrderType"])
    def test_currency_mix_rejected_instead_of_implicit_fx_or_double_budget(self):
        with self.assertRaisesRegex(ValueError,"SEPARATE_KRW_USD"):
            self.prepare([fixture("KRX")[0],fixture("NASD")[0]])
    def test_later_review_and_risk_limit_change_invalidate_prior_document_hashes(self):
        spec,_,_=fixture();a=self.prepare([spec]);b=self.prepare([spec],now=NOW+timedelta(hours=1))
        self.assertNotEqual(a,b)
        changed=_review_execution_policies(Path("unused"),None,None,ACCOUNT,{"savedSlippageBps":25},specs=[spec],now=NOW)
        self.assertNotEqual(a,changed)
        self.assertEqual("25",changed["kisEquityExecutionPolicies"][spec.artifact_hash]["protectiveLimitBps"])

if __name__=="__main__":unittest.main()
