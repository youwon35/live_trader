"""Synthetic exact REST facts only; no file, account or broker producer."""
from copy import deepcopy
import importlib
import socket
import unittest
from unittest.mock import patch

import test_kis_scoped_cleanup_truth as cleanup
import test_kis_equity_operational as equity

projection = importlib.import_module(cleanup.NAME+'.kis_us_terminal_ledger_projection')


def fixture(side='BUY', filled='3', amount='300', status='체결'):
    scope, owned, working, official = cleanup.fixture()
    spec, _, _ = equity.fixture()
    dispatch = owned['dispatch']
    dispatch['side'] = side
    ticket = {k:v for k,v in dispatch['metadata']['native_quantity_model'].items() if k != 'contentHash'}
    ticket['side'] = side
    dispatch['metadata']['native_quantity_model'] = cleanup.service.seal(ticket)
    official.update(ft_ccld_qty=filled, ft_ccld_amt3=amount, nccs_qty='0', prcs_stat_name=status,
        sll_buy_dvsn_cd='02' if side=='BUY' else '01')
    return scope, spec, dispatch, official


class KisUsTerminalLedgerProjectionTest(unittest.TestCase):
    def setUp(self):
        self.net = patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.net_ex = patch.object(socket.socket,'connect_ex',side_effect=AssertionError('network forbidden'))
        self.net.start();self.net_ex.start();self.addCleanup(self.net.stop);self.addCleanup(self.net_ex.stop)

    def plan(self, *args, **kwargs):
        return projection.terminal_gross_projection(*(args or fixture()), **kwargs)

    def test_exact_rest_cumulative_gross_is_planned_without_actual_fee_or_write_claim(self):
        result = self.plan();plan = result['plan']
        self.assertEqual(result['action'],'BLOCKED_COST_EVIDENCE')
        self.assertEqual(plan['proposedPositionDelta'],'3')
        self.assertEqual(plan['proposedGrossCashDelta'],'-300')
        self.assertEqual(plan['sourceTransport'],'KIS_AUTHENTICATED_REST_CCNL')
        self.assertFalse(plan['genericLedgerWriteAllowed'])
        self.assertFalse(plan['actualFeeObserved'])
        self.assertIsNone(plan['actualFee']);self.assertIsNone(plan['actualTax'])
        self.assertIsNone(plan['actualSettlementCashDelta'])
        self.assertEqual(plan['blockers'],[projection.ACTUAL_COST_BLOCKER])
        self.assertNotIn('costReserve',plan)
        self.assertNotIn('privateWebsocket',str(plan))

    def test_exact_replay_is_noop_changed_terminal_facts_are_never_rebooked(self):
        inputs = fixture();first = self.plan(*inputs)['plan']
        self.assertEqual(self.plan(*inputs,previous=first)['action'],'NOOP_EXACT_REPLAY')
        inputs[-1]['ft_ccld_amt3']='301'
        with self.assertRaisesRegex(ValueError,'TERMINAL_PROOF_OR_IMMUTABLE_SCOPE_CHANGED'):
            self.plan(*inputs,previous=first)

    def test_partial_cancel_only_books_proposed_executed_shares_not_requested_or_cancelled_shares(self):
        result = self.plan(*fixture(filled='1',amount='100',status='취소완료'))
        self.assertEqual(result['plan']['cumulativeFilledQuantity'],'1')
        self.assertEqual(result['plan']['proposedPositionDelta'],'1')
        self.assertEqual(result['plan']['proposedGrossCashDelta'],'-100')
        self.assertEqual(result['action'],'BLOCKED_COST_EVIDENCE')

    def test_zero_fill_cancel_or_reject_creates_no_financial_projection(self):
        for status in ('취소완료','주문거부'):
            result=self.plan(*fixture(filled='0',amount='0',status=status))
            self.assertEqual(result['action'],'NOOP_ZERO_FILL')
            self.assertEqual(result['plan']['proposedPositionDelta'],'0')
            self.assertFalse(result['plan']['genericLedgerWriteAllowed'])
            self.assertFalse(result['plan']['actualFeeObserved'])

    def test_zero_executed_quantity_with_positive_gross_amount_is_contradictory_not_noop(self):
        with self.assertRaisesRegex(ValueError,'CUMULATIVE_QUANTITY_NOTIONAL_CONTRADICTION'):
            self.plan(*fixture(filled='0',amount='300',status='취소완료'))

    def test_sell_proposed_native_usd_direction_is_reversed_and_costs_still_unknown(self):
        plan=self.plan(*fixture(side='SELL'))['plan']
        self.assertEqual(plan['proposedPositionDelta'],'-3')
        self.assertEqual(plan['proposedGrossCashDelta'],'300')
        self.assertIsNone(plan['actualSettlementCashDelta'])

    def test_exact_decimal_notional_sign_never_rounds_under_default_decimal_context(self):
        amount='123456789012345678901234.1234567890123456'
        plan=self.plan(*fixture(amount=amount))['plan']
        self.assertEqual(plan['cumulativeGrossNotional'],amount)
        self.assertEqual(plan['proposedGrossCashDelta'],'-'+amount)

    def test_nonterminal_remaining_unknown_terminal_or_fractional_fills_fail_closed(self):
        for field,value in (('nccs_qty','1'),('prcs_stat_name','UNKNOWN'),('ft_ccld_qty','1.5'),('ft_ccld_qty','4'),('ft_ccld_qty',3.0),('ft_ccld_amt3','NaN'),('ft_ccld_amt3',300.0)):
            inputs=fixture();inputs[-1][field]=value
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):self.plan(*inputs)

    def test_wrong_account_credentials_venue_currency_date_branch_and_order_fail_closed(self):
        for field,value in (('ord_dt','20261016'),('ord_gno_brno','002'),('odno','999'),('pdno','SPY'),('tr_crcy_cd','KRW'),('ovrs_excg_cd','NYSE')):
            inputs=fixture();inputs[-1][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):self.plan(*inputs)
        for field in ('accountFingerprint','credentialConfigurationHash'):
            inputs=fixture();inputs[0][field]='f'*64
            inputs[0]['scopeHash']=projection.stable_sha256({k:v for k,v in inputs[0].items() if k!='scopeHash'})
            with self.subTest(field=field),self.assertRaises(ValueError):self.plan(*inputs)

    def test_unverified_provisional_dispatch_and_tampered_prior_plan_are_rejected(self):
        for change in ({'broker_order_identity_verified':False},{'order_date_source':'KST_SUBMISSION_DATE_PROVISIONAL'}, {'broker_order_key':'20261015:001:OTHER'}):
            inputs=fixture();inputs[2].update(change)
            with self.assertRaises(ValueError):self.plan(*inputs)
        inputs=fixture();previous=deepcopy(self.plan(*inputs)['plan']);previous['proposedPositionDelta']='999'
        with self.assertRaisesRegex(ValueError,'PREVIOUS_PLAN_INVALID'):self.plan(*inputs,previous=previous)

    def test_inputs_are_not_modified_and_no_implicit_io_or_actual_cost_producer_exists(self):
        inputs=fixture();before=deepcopy(inputs)
        with patch('sqlite3.connect',side_effect=AssertionError('no ledger initialization')):
            self.plan(*inputs)
        self.assertEqual(inputs,before)


if __name__ == '__main__':unittest.main()
