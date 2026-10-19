from copy import deepcopy
import importlib
import json
from pathlib import Path
import sys
import types
import unittest


# The production app, settings and transport are never imported. Only the pure
# observation parser and the pure public summary execute under a private name.
ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_kis_settlement_history_fixture_isolated'
package = types.ModuleType(PACKAGE)
package.__path__ = [str(ROOT / 'live_trader')]
sys.modules[PACKAGE] = package
usd = importlib.import_module(PACKAGE + '.kis_usd_observations')
history = importlib.import_module(PACKAGE + '.kis_settlement_history_preparation')
START = '20260721'
END = '20261016'
SETTINGS = {'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
    'KIS_ACCOUNT_NO': '12345678-01', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
    'KIS_ENV': 'real', 'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443'}


def row(**updates):
    value = {'trad_dt': '20261014', 'sttl_dt': '20261016', 'sll_buy_dvsn_cd': '02',
        'pdno': 'PRIVATE_TICKER', 'crcy_cd': 'USD', 'ccld_qty': '91.125',
        'tr_frcr_amt2': '90127345.6789123', 'frcr_excc_amt_1': '90127346.6789123',
        'dmst_frcr_fee1': '0.123456789', 'frcr_fee1': '-0.0123456789',
        'wcrc_excc_amt': '999812346.12345', 'dmst_wcrc_fee': '161.98765',
        'ovrs_wcrc_fee': '16.198765', 'erlm_exrt': '1386.4321'}
    value.update(updates)
    return value


def page(rows=None, *, continuation='D', cursor=''):
    return ({'rt_cd': '0', 'msg1': 'PRIVATE_REMOTE_MESSAGE',
        'output1': deepcopy([row()] if rows is None else rows), 'output2': {},
        'ctx_area_fk100': cursor, 'ctx_area_nk100': cursor},
        {'tr_id': 'CTOS4001R', 'tr_cont': continuation,
         'date': 'Mon, 19 Oct 2026 04:00:00 GMT', 'age': '0'})


class FakeReader:
    def __init__(self, pages):
        self.settings = dict(SETTINGS)
        self.pages = list(pages)
        self.calls = []

    def _credentials(self, provider):
        assert provider == 'kis'

    def _kis_token(self):
        return 'SYNTHETIC_TOKEN'

    def _pace_kis(self):
        pass

    def _get(self, provider, path, **kwargs):
        assert provider == 'kis' and path == history.PATH
        self.calls.append(deepcopy(kwargs))
        return deepcopy(self.pages.pop(0))


def capture(pages=None, *, start=START, end=END, max_pages=2):
    reader = FakeReader([page()] if pages is None else pages)
    result = usd.KisUsdObservationReader(reader,
        utc_now=lambda: '2026-10-19T04:00:00+00:00', monotonic=lambda: 10.0).observe(
            history.KIND, max_pages=max_pages, start_date=start, end_date=end)
    return result


def summary(value=None, **kwargs):
    return history.summarize_settlement_history(capture() if value is None else value,
        expected_start_date=START, expected_end_date=END, **kwargs)


def rehash_summary(value):
    value = deepcopy(value)
    value.pop('contentHash', None)
    return {**value, 'contentHash': usd.canonical_hash(value)}


def rehash_capture(value):
    value = deepcopy(value)
    for item in value['pages']:
        item['canonicalParsedBodyHash'] = usd.canonical_hash(item['body'])
    value.pop('contentHash', None)
    return {**value, 'contentHash': usd.canonical_hash(value)}


class SettlementHistoryTests(unittest.TestCase):
    def test_public_request_budget_validation_is_pure_and_precedes_any_reader(self):
        self.assertEqual(history.validate_history_request(start_date=START, end_date=END),
            {'startDate': START, 'endDate': END, 'calendarDays': 88, 'maxPages': 2})
        for changes in ({'start_date': '20260230'}, {'start_date': '20260101'},
                {'max_pages': True}, {'max_pages': 3}, {'max_pages': 2.0},
                {'end_date': '20260720'}):
            with self.subTest(changes=changes), self.assertRaises(history.KisSettlementHistoryError):
                history.validate_history_request(**{'start_date': START, 'end_date': END,
                    'max_pages': 2, **changes})

    def test_complete_capture_is_bounded_observation_not_finality(self):
        value = summary()
        self.assertEqual(value['querySpan'], {'startDate': START, 'endDate': END,
            'calendarDays': 88, 'meaning': 'EXPLICIT_REGISTRATION_QUERY_DATES_NOT_ALL_ACCOUNT_HISTORY'})
        self.assertEqual(value['paginationStatus'], 'COMPLETE')
        self.assertEqual(value['settlementFinality'], 'UNKNOWN')
        self.assertEqual(value['providerMaximumLookback'], 'UNCONFIRMED_IN_REVIEWED_PUBLIC_CONTRACT')
        self.assertEqual(value['counts']['usdRowCount'], 1)
        self.assertTrue(all(value[key] is False for key in history.FLAGS))

    def test_official_decimal_fields_only_report_counts_and_separate_roles(self):
        value = summary()
        for key in history.FIELD_ROLES:
            self.assertEqual(value['numericFieldStates'][key], {
                'ABSENT': 0, 'EMPTY_TEXT': 0, 'REPORTED_DECIMAL_TEXT': 1, 'INVALID_OR_UNPROVEN': 0})
        self.assertIn('REGISTERED_FX_NOT_ACTUAL_CONVERSION', value['reportedRoleNames']['erlm_exrt'])
        self.assertIn('NOT_AUTOMATIC_FX_PROOF', value['reportedRoleNames']['wcrc_excc_amt'])

    def test_summary_never_contains_private_literals_names_dates_or_credentials(self):
        encoded = json.dumps(summary(), sort_keys=True)
        for private in ('PRIVATE_', 'SYNTHETIC_', '12345678', '90127345.6789123',
                '999812346.12345', '1386.4321', '20261014', '0.123456789'):
            self.assertNotIn(private, encoded)
        for forbidden in ('availableCash', 'feeRate', 'actualFxRate', 'actualSettlementCashDelta'):
            self.assertNotIn(forbidden, summary())

    def test_capture_and_returned_summaries_are_independent_and_immutable_to_caller(self):
        value = capture()
        before = deepcopy(value)
        result = summary(value)
        self.assertEqual(value, before)
        checked = history.validate_settlement_history_summary(result)
        checked['counts']['usdRowCount'] = 999
        checked['querySpan']['startDate'] = '20000101'
        checked['numericFieldStates']['erlm_exrt']['ABSENT'] = 999
        self.assertEqual(value, before)
        self.assertEqual(result['counts']['usdRowCount'], 1)
        self.assertEqual(result['numericFieldStates']['erlm_exrt']['ABSENT'], 0)

    def test_empty_period_rows_do_not_prove_zero_usd_or_no_full_history(self):
        result = summary(capture([page([])]))
        self.assertEqual(result['counts']['returnedRowCount'], 0)
        self.assertEqual(result['rowStatus'], 'NO_REPORTED_USD_ROWS_IN_CAPTURE_NOT_ZERO_BALANCE')
        self.assertFalse(result['allAccountHistoryVerified'])
        self.assertFalse(result['usdCashReconciled'])

    def test_other_currency_is_counted_without_substitution_into_usd(self):
        result = summary(capture([page([row(crcy_cd='HKD')])]))
        self.assertEqual(result['counts']['otherCurrencyRowCount'], 1)
        self.assertEqual(result['counts']['usdRowCount'], 0)
        self.assertEqual(result['numericFieldStates']['erlm_exrt']['REPORTED_DECIMAL_TEXT'], 0)

    def test_optional_decimal_absent_empty_invalid_and_explicit_zero_stay_distinct(self):
        for item, state in [({}, 'ABSENT'), ({'wcrc_excc_amt': ''}, 'EMPTY_TEXT'),
                ({'wcrc_excc_amt': '0'}, 'REPORTED_DECIMAL_TEXT'),
                ({'wcrc_excc_amt': 'NaN'}, 'INVALID_OR_UNPROVEN'),
                ({'wcrc_excc_amt': 0}, 'INVALID_OR_UNPROVEN'),
                ({'wcrc_excc_amt': False}, 'INVALID_OR_UNPROVEN'),
                ({'wcrc_excc_amt': None}, 'INVALID_OR_UNPROVEN')]:
            with self.subTest(state=state, kind=type(item.get('wcrc_excc_amt')).__name__):
                value = row()
                value.pop('wcrc_excc_amt')
                value.update(item)
                result = summary(capture([page([value])]))
                self.assertEqual(result['numericFieldStates']['wcrc_excc_amt'][state], 1)
                self.assertFalse(result['actualFxExecutionObserved'])

    def test_misleading_final_or_fx_fields_never_open_authority(self):
        result = summary(capture([page([row(final=True, fx_finality='CONFIRMED',
            actualFxRate='1386.4321', remoteMessage='PRIVATE_NEW_MESSAGE')])]))
        self.assertEqual(result['counts']['unreviewedRowFieldCount'], 4)
        self.assertEqual(result['settlementFinality'], 'UNKNOWN')
        self.assertTrue(all(result[key] is False for key in history.FLAGS))
        self.assertNotIn('PRIVATE_NEW_MESSAGE', json.dumps(result))

    def test_trade_date_outside_registration_window_does_not_imply_wrong_or_final_posting(self):
        result = summary(capture([page([row(trad_dt='20200102', sttl_dt='20200106')])]))
        self.assertEqual(result['counts']['usdRowCount'], 1)
        self.assertEqual(result['settlementFinality'], 'UNKNOWN')

    def test_two_page_limit_and_more_cursor_preserve_partial(self):
        result = summary(capture([page(continuation='M', cursor='CURSOR1'),
            page([row(pdno='OTHER_PRIVATE_TICKER')], continuation='F', cursor='CURSOR2')]))
        self.assertEqual(result['pageCount'], 2)
        self.assertEqual(result['paginationStatus'], 'PARTIAL')
        self.assertEqual(result['paginationReason'], 'KIS_USD_PAGE_LIMIT')
        self.assertFalse(result['allAccountHistoryVerified'])

    def test_absent_terminal_header_is_unknown_not_complete(self):
        result = summary(capture([page(continuation='')]))
        self.assertEqual(result['paginationStatus'], 'UNKNOWN')
        self.assertEqual(result['settlementFinality'], 'UNKNOWN')

    def test_duplicate_rows_are_counted_and_not_deduplicated_or_allocated(self):
        result = summary(capture([page([row(), row()])]))
        self.assertEqual(result['counts']['usdRowCount'], 2)
        self.assertEqual(result['counts']['duplicateUsdRowCount'], 1)
        self.assertEqual(result['numericFieldStates']['dmst_frcr_fee1']['REPORTED_DECIMAL_TEXT'], 2)
        self.assertFalse(result['orderAttributionVerified'])

    def test_explicit_date_binding_mismatch_rejected(self):
        with self.assertRaisesRegex(history.KisSettlementHistoryError, 'DATE_BINDING_MISMATCH'):
            history.summarize_settlement_history(capture(), expected_start_date='20260722',
                expected_end_date=END)

    def test_local_date_budget_is_not_provider_guarantee(self):
        for start, end in [('20260230', END), ('20260720', '20261018'),
                (END, START), ('', END), (None, END)]:
            with self.subTest(start=start, end=end), self.assertRaises(history.KisSettlementHistoryError):
                history.summarize_settlement_history(capture(), expected_start_date=start, expected_end_date=end)

    def test_local_page_budget_rejects_more_than_two_and_boolean_aliases(self):
        for maximum in (True, 0, 3, 2.0, '2'):
            with self.subTest(maximum=maximum), self.assertRaises(history.KisSettlementHistoryError):
                summary(max_pages=maximum)
        value = capture([page(continuation='M', cursor='C1'),
            page([row(pdno='PRIVATE_2')], continuation='M', cursor='C2'),
            page([row(pdno='PRIVATE_3')])], max_pages=3)
        with self.assertRaisesRegex(history.KisSettlementHistoryError, 'PAGE_BUDGET_EXCEEDED'):
            summary(value)

    def test_hash_tampering_and_rehashed_positive_capture_authority_rejected(self):
        original = capture()
        bad = deepcopy(original)
        bad['pages'][0]['body']['output1'][0]['erlm_exrt'] = '1'
        with self.assertRaisesRegex(history.KisSettlementHistoryError, 'CAPTURE_UNVERIFIED'):
            summary(bad)
        bad = deepcopy(original)
        bad['orderAuthorityGranted'] = True
        bad = rehash_capture(bad)
        with self.assertRaisesRegex(history.KisSettlementHistoryError, 'CAPTURE_UNVERIFIED'):
            summary(bad)

    def test_wrong_kind_of_valid_private_capture_rejected(self):
        reader = FakeReader([({'rt_cd': '0', 'output2': []}, {'tr_id': 'CTRP6010R', 'tr_cont': 'D'})])
        reader._get = lambda *args, **kwargs: deepcopy(reader.pages.pop(0))
        value = usd.KisUsdObservationReader(reader).observe('SETTLEMENT_BASIS_CASH', basis_date=END)
        with self.assertRaisesRegex(history.KisSettlementHistoryError, 'SCOPE_MISMATCH'):
            summary(value)

    def test_public_validator_rejects_extra_private_fields_or_wrong_roles_even_if_rehashed(self):
        original = summary()
        changes = [lambda v: v.update(privateAmount='PRIVATE_SENTINEL'),
            lambda v: v['counts'].update(accountNumber='PRIVATE_SENTINEL'),
            lambda v: v['numericFieldStates']['erlm_exrt'].update(value='PRIVATE_SENTINEL'),
            lambda v: v['reportedRoleNames'].update(erlm_exrt='ACTUAL_EXECUTED_FX_RATE'),
            lambda v: v.update(settlementFinality='CONFIRMED'),
            lambda v: v.update(actualFxExecutionObserved=True),
            lambda v: v.update(actualFeeObserved=0),
            lambda v: v.update(ledgerInitialized=1),
            lambda v: v['counts'].update(usdRowCount=True),
            lambda v: v['querySpan'].update(calendarDays=True)]
        for change in changes:
            with self.subTest(change=change):
                bad = deepcopy(original)
                change(bad)
                with self.assertRaises(history.KisSettlementHistoryError):
                    history.validate_settlement_history_summary(rehash_summary(bad))

    def test_public_validator_rejects_stale_checksum_naive_time_and_count_contradictions(self):
        original = summary()
        bad = deepcopy(original)
        bad['counts']['usdRowCount'] = 0
        with self.assertRaises(history.KisSettlementHistoryError):
            history.validate_settlement_history_summary(bad)
        for change in [lambda v: v.update(retrievalCompletedAt='2026-10-19T04:00:00'),
                lambda v: v['numericFieldStates']['erlm_exrt'].update(ABSENT=2),
                lambda v: v['counts'].update(duplicateUsdRowCount=2),
                lambda v: v.update(pageCount=False)]:
            bad = deepcopy(original)
            change(bad)
            with self.assertRaises(history.KisSettlementHistoryError):
                history.validate_settlement_history_summary(rehash_summary(bad))


if __name__ == '__main__':
    unittest.main()
