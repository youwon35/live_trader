from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]


def load_function(name, namespace):
    path = ROOT / 'live_trader/state.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    node.decorator_list = []
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return namespace[name]


class ReconciliationAccountScopeTests(unittest.TestCase):
    def setUp(self):
        self.current = [{'broker_id': 'binance', 'account': 'Binance Spot', 'currency': 'USDT', 'broker_cash': 0.0}]
        self.ledger = [{'broker_id': 'binance', 'account': 'Binance Spot', 'currency': 'USDT', 'cash': 0.0, 'source': 'event_poll', 'updated_at': '2026-07-30 02:30:36'}]
        self.ns = {
            'STATE': {'broker_reconciliation': {'accounts': self.current}},
            'PROGRAM_LEDGER': SimpleNamespace(cash_rows=Mock(side_effect=lambda: self.ledger)),
            'ACCOUNT_RECONCILIATION_BOOK': ({'broker_id': 'binance', 'broker_name': 'Binance', 'account': 'Binance Spot', 'currency': 'USDT', 'broker_cash': None, 'program_cash': None, 'detail': 'account read required'},),
            'broker_reconciliation_errors': lambda: {},
            'format_money': lambda value, currency: f'{float(value):.2f} {currency}',
        }
        self.run_rows = load_function('account_reconciliation_rows', self.ns)

    def test_single_matching_zero_balance_passes_and_retains_original_timestamp(self):
        row = self.run_rows()[0]
        self.assertEqual('pass', row['status'])
        self.assertEqual('0.00 USDT', row['broker_cash'])
        self.assertEqual('2026-07-30 02:30:36', row['program_updated_at'])
        self.ns['PROGRAM_LEDGER'].cash_rows.assert_called_once_with()

    def test_real_balance_difference_remains_mismatch_without_mutation(self):
        self.current[0]['broker_cash'] = 32.25
        self.ledger[0]['cash'] = 42.25
        original = deepcopy(self.ledger)
        row = self.run_rows()[0]
        self.assertEqual('mismatch', row['status'])
        self.assertEqual('10.00 USDT', row['delta_cash'])
        self.assertEqual(original, self.ledger)

    def test_different_account_cannot_pass_even_with_identical_balance(self):
        self.current[0]['account'] = 'Other account'
        row = self.run_rows()[0]
        self.assertEqual('api_required', row['status'])
        self.assertEqual('account_scope_changed', row['comparison_issue'])
        self.assertEqual('-', row['delta_cash'])

    def test_different_currency_never_relabels_ledger_amount_as_current_currency(self):
        self.current[0]['currency'] = 'KRW'
        row = self.run_rows()[0]
        self.assertEqual('api_required', row['status'])
        self.assertEqual('USDT', row['program_currency'])
        self.assertEqual('0.00 USDT', row['program_cash'])
        self.assertEqual('0.00 KRW', row['broker_cash'])

    def test_missing_identity_never_falls_back_to_default_provider_label(self):
        for source, field in [('current', 'account'), ('current', 'currency'), ('ledger', 'account'), ('ledger', 'currency')]:
            with self.subTest(source=source, field=field):
                target = self.current[0] if source == 'current' else self.ledger[0]
                old = target.pop(field)
                row = self.run_rows()[0]
                self.assertEqual('api_required', row['status'])
                self.assertEqual('account_scope_unverified', row['comparison_issue'])
                target[field] = old

    def test_duplicate_current_accounts_are_not_silently_overwritten(self):
        self.current.append({**self.current[0], 'account': 'Second account', 'broker_cash': 999.0})
        row = self.run_rows()[0]
        self.assertEqual('api_required', row['status'])
        self.assertEqual('ambiguous_account_scope', row['comparison_issue'])
        self.assertIsNone(row['broker_cash_value'])

    def test_duplicate_ledger_accounts_are_not_silently_overwritten(self):
        self.ledger.append({**self.ledger[0], 'currency': 'FDUSD', 'cash': 999.0})
        row = self.run_rows()[0]
        self.assertEqual('api_required', row['status'])
        self.assertEqual('ambiguous_account_scope', row['comparison_issue'])
        self.assertIsNone(row['program_cash_value'])
        self.assertEqual('', row['program_updated_at'])

    def test_duplicate_identical_rows_are_still_ambiguous(self):
        self.current.append(dict(self.current[0]))
        self.assertEqual('ambiguous_account_scope', self.run_rows()[0]['comparison_issue'])

    def test_missing_source_stays_unverified_and_does_not_create_balance(self):
        self.ledger.clear()
        row = self.run_rows()[0]
        self.assertEqual('api_required', row['status'])
        self.assertEqual('missing', row['program_source'])
        self.assertIsNone(row['program_cash_value'])
        self.assertEqual('', row['program_updated_at'])

    def test_position_timestamp_is_observed_basis_not_current_or_synthetic_time(self):
        ledger = {'broker_id': 'binance', 'symbol': 'BTC', 'quantity': 1.0, 'source': 'event_poll', 'updated_at': '2026-07-30 02:30:36'}
        observed = {'broker_id': 'binance', 'symbol': 'BTC', 'broker_qty': 1.0}
        ns = {
            'live_position_rows': lambda: {('binance', 'BTC', ''): observed, ('binance', 'FDUSD', ''): {'broker_id': 'binance', 'symbol': 'FDUSD', 'broker_qty': .25}},
            'program_position_rows': lambda: {('binance', 'BTC', ''): ledger},
            'broker_reconciliation_errors': lambda: {},
            'successful_position_brokers': lambda: {'binance'},
            'normalized_reconciliation_position_side': lambda item, source: '',
            'format_quantity': str,
            'format_money': lambda value, currency: f'{value} {currency}',
            'safe_float': lambda value, default=0: default if value is None else float(value),
        }
        rows = load_function('positions', ns)()
        self.assertEqual('2026-07-30 02:30:36', rows[0]['program_updated_at'])
        self.assertEqual('', rows[1]['program_updated_at'])
        self.assertEqual('mismatch', rows[1]['status'])


if __name__ == '__main__':
    unittest.main()
