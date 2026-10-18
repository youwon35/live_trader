from copy import deepcopy
import importlib
import json
from pathlib import Path
import sys
import types
import unittest

# A fixture package cannot initialize the app or its environment/secret store.
ROOT = Path(__file__).resolve().parents[1]
PACKAGE = '_kis_integrated_margin_fixture_isolated'
pkg = types.ModuleType(PACKAGE)
pkg.__path__ = [str(ROOT / 'live_trader')]
sys.modules[PACKAGE] = pkg
adapters = types.ModuleType(PACKAGE + '.live_adapters')
def forbidden(*args, **kwargs):
    raise AssertionError('ORDER_OR_SIGNING_HELPER_USED')
for key in ('build_upbit_authorization', 'sign_binance_query', 'split_kis_account'):
    setattr(adapters, key, forbidden)
sys.modules[PACKAGE + '.live_adapters'] = adapters
integrated = importlib.import_module(PACKAGE + '.kis_integrated_margin_observation')
ro = importlib.import_module(PACKAGE + '.read_only_account')
profiles = importlib.import_module(PACKAGE + '.account_profiles')

SETTINGS = {
    'KIS_APP_KEY': 'SYNTHETIC_KEY', 'KIS_APP_SECRET': 'SYNTHETIC_SECRET',
    'KIS_ACCOUNT_NO': '12345678-01', 'KIS_ACCOUNT_PRODUCT_CODE': '01',
    'KIS_ENV': 'real', 'KIS_BASE_URL': 'https://openapi.koreainvestment.com:9443',
    'KIS_OVERSEAS_FUNDING_MODE': 'INTEGRATED_MARGIN',
}


def reported_body(**overrides):
    fields = {key: '123456.7800' for key in integrated.ROLE_FIELDS}
    fields.update(ovrs_stck_itgr_mgna_dvsn_name='SYNTHETIC_REMOTE_LABEL')
    fields.update(overrides)
    return {'rt_cd': '0', 'msg_cd': 'SYNTHETIC', 'msg1': 'SYNTHETIC_REMOTE_MESSAGE', 'output': fields}


class Reader:
    def __init__(self, *, body=None, headers=None):
        self.settings = dict(SETTINGS)
        self.body = deepcopy(reported_body() if body is None else body)
        self.headers = {'tr_id': integrated.TR_ID, 'tr_cont': 'D',
                        'date': 'Mon, 19 Oct 2026 01:00:01 GMT', 'age': '0'} if headers is None else deepcopy(headers)
        self.requests = []
        self.credentials = self.tokens = self.paces = 0
        self.on_get = self.on_token = self.on_pace = None
    def _credentials(self, provider):
        assert provider == 'kis'
        self.credentials += 1
    def _kis_token(self):
        self.tokens += 1
        if self.on_token:
            self.on_token()
        return 'SYNTHETIC_TOKEN'
    def _pace_kis(self):
        self.paces += 1
        if self.on_pace:
            self.on_pace()
    def _get(self, provider, path, **kwargs):
        self.requests.append((provider, path, deepcopy(kwargs)))
        if self.on_get:
            self.on_get()
        return deepcopy(self.body), deepcopy(self.headers)


def capture(reader=None, *, times=None, monotonic=None, **kwargs):
    reader = Reader() if reader is None else reader
    times = iter(('2026-10-19T01:00:00+00:00', '2026-10-19T01:00:01+00:00') if times is None else times)
    mono = iter((10.0, 11.0) if monotonic is None else monotonic)
    return integrated.KisIntegratedMarginObservationReader(
        reader, utc_now=lambda: next(times), monotonic=lambda: next(mono)).observe(**kwargs)


def rehash(value):
    result = deepcopy(value)
    result.pop('contentHash', None)
    result['contentHash'] = integrated.canonical_hash(result)
    return result


class IntegratedObservationTests(unittest.TestCase):
    def test_constructor_no_io(self):
        reader = Reader()
        integrated.KisIntegratedMarginObservationReader(reader)
        self.assertEqual((reader.credentials, reader.tokens, reader.paces, reader.requests), (0, 0, 0, []))

    def test_single_get_exact_krw_query_and_separate_role_literals(self):
        reader = Reader()
        value = capture(reader)
        checked = integrated.validate_observation(value)
        self.assertEqual(reader.requests[0][0:2], ('kis', integrated.PATH))
        self.assertEqual(reader.requests[0][2]['query'], {
            'CANO': '12345678', 'ACNT_PRDT_CD': '01', 'CMA_EVLU_AMT_ICLD_YN': 'N',
            'WCRC_FRCR_DVSN_CD': '02', 'FWEX_CTRT_FRCR_DVSN_CD': '02'})
        self.assertEqual(reader.requests[0][2]['headers']['tr_id'], 'TTTC0869R')
        self.assertEqual((reader.tokens, reader.paces, len(reader.requests)), (1, 1, 1))
        self.assertEqual(checked['reportedValues']['usd_itgr_ord_psbl_amt'], '123456.7800')
        self.assertNotIn('availableCash', checked)
        self.assertNotIn('commonAvailableCash', checked)
        self.assertTrue(all(value[key] is False for key in integrated.FALSE_CLAIMS))

    def test_exact_saved_profile_binding_is_distinct_from_reader_hash(self):
        value = capture()
        profile = next(p for p in profiles.saved_account_profiles(SETTINGS) if p['brokerId'] == 'kis')
        expected = {key: profile[key] for key in ('profileId', 'accountFingerprint', 'credentialConfigurationHash')}
        self.assertEqual(value['accountBinding'], expected)
        self.assertEqual(integrated.validate_observation(value)['accountBinding'], expected)
        self.assertNotEqual(value['credentialConfigurationHash'], expected['credentialConfigurationHash'])

    def test_sanitized_summary_never_leaks_account_amount_remote_text_or_identity(self):
        value = capture()
        summary = integrated.safe_observation_summary(value)
        text = json.dumps(summary, ensure_ascii=False)
        for secret in ('123456.7800', '12345678', 'SYNTHETIC_KEY', 'SYNTHETIC_SECRET',
                       'SYNTHETIC_TOKEN', 'SYNTHETIC_REMOTE_LABEL', 'SYNTHETIC_REMOTE_MESSAGE',
                       value['accountQueryHash'], value['credentialConfigurationHash'],
                       value['accountBinding']['accountFingerprint']):
            self.assertNotIn(secret, text)
        self.assertEqual(summary['reportedNumericFieldCount'], len(integrated.ROLE_FIELDS))
        self.assertTrue(all(summary[key] is False for key in integrated.FALSE_CLAIMS))

    def test_missing_empty_and_explicit_zero_stay_distinct(self):
        body = reported_body(stck_cash_objt_amt='', usd_itgr_ord_psbl_amt='0')
        body['output'].pop('usd_use_amt')
        checked = integrated.validate_observation(capture(Reader(body=body)))
        self.assertEqual(checked['numericFieldStates']['stck_cash_objt_amt'], 'EMPTY_UNREPORTED')
        self.assertEqual(checked['numericFieldStates']['usd_use_amt'], 'ABSENT')
        self.assertEqual(checked['reportedValues']['usd_itgr_ord_psbl_amt'], '0')
        self.assertNotIn('stck_cash_objt_amt', checked['reportedValues'])

    def test_negative_reported_amount_preserved_without_cash_arithmetic(self):
        checked = integrated.validate_observation(capture(Reader(body=reported_body(stck_cash_use_amt='-3.50'))))
        self.assertEqual(checked['reportedValues']['stck_cash_use_amt'], '-3.50')

    def test_enrollment_never_inferred_from_labels_including_misleading_substring(self):
        for label in ('통합증거금 신청', '통합증거금 미신청', '신청', '미신청', '', None):
            body = reported_body()
            if label is None:
                body['output'].pop('ovrs_stck_itgr_mgna_dvsn_name')
            else:
                body['output']['ovrs_stck_itgr_mgna_dvsn_name'] = label
            summary = integrated.safe_observation_summary(capture(Reader(body=body)))
            self.assertEqual(summary['enrollmentLabelPresence'], bool(label))
            self.assertEqual(summary['enrollmentClassification'], 'UNKNOWN_REQUIRES_EXPLICIT_BROKER_CONFIRMATION')
            self.assertFalse(summary['brokerEnrollmentVerified'])

    def test_no_autopaging_even_more_header(self):
        reader = Reader(headers={'tr_id': integrated.TR_ID, 'tr_cont': 'M'})
        summary = integrated.safe_observation_summary(capture(reader))
        self.assertEqual(summary['paginationStatus'], 'PARTIAL')
        self.assertEqual(len(reader.requests), 1)

    def test_absent_terminal_header_unknown_not_complete(self):
        self.assertEqual(integrated.safe_observation_summary(capture(Reader(headers={'tr_id': integrated.TR_ID})))['paginationStatus'], 'UNKNOWN')

    def test_one_page_limit_rejected_before_oauth(self):
        for maximum in (True, False, 0, 2, 1.0, '1', None):
            reader = Reader()
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(reader, max_pages=maximum)
            self.assertEqual((reader.credentials, reader.tokens, reader.requests), (0, 0, []))

    def test_invalid_funding_scope_rejected_before_oauth(self):
        for key, value in (('KIS_OVERSEAS_FUNDING_MODE', 'AUTO'), ('KIS_BASE_URL', 'https://evil.invalid'),
                           ('KIS_ENV', 'paper'), ('KIS_ACCOUNT_PRODUCT_CODE', '02'),
                           ('KIS_ACCOUNT_NO', '87654321-99'), ('KIS_APP_KEY', 1)):
            reader = Reader()
            reader.settings[key] = value
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(reader)
            self.assertEqual((reader.tokens, reader.requests), (0, []))

    def test_changes_after_token_after_pace_or_get_block_capture(self):
        for stage in ('on_token', 'on_pace', 'on_get'):
            for key, value in (('KIS_OVERSEAS_FUNDING_MODE', 'FOREIGN_CASH'),
                               ('KIS_ACCOUNT_NO', '87654321-01'), ('KIS_APP_SECRET', 'CHANGED_SECRET')):
                reader = Reader()
                setattr(reader, stage, lambda r=reader, k=key, v=value: r.settings.__setitem__(k, v))
                with self.assertRaisesRegex(integrated.KisUsdObservationError, 'ACCOUNT_SCOPE_CHANGED'):
                    capture(reader)
                self.assertLessEqual(len(reader.requests), 1)

    def test_strict_decimal_strings_reject_float_boolean_null_exponent_unicode(self):
        for value in (True, False, 1, 1.5, None, 'NaN', 'Infinity', '-Infinity', '1e3',
                      '1,000', ' 3', '3 ', '３', '1\n', '9' * 129):
            with self.assertRaisesRegex(integrated.KisUsdObservationError, 'REPORTED_NUMBER_INVALID'):
                capture(Reader(body=reported_body(usd_itgr_ord_psbl_amt=value)))

    def test_strict_json_decoder_rejects_duplicate_keys_and_nonfinite(self):
        for raw in (b'{"rt_cd":"0","rt_cd":"1"}', b'{"output":{"usd_use_amt":"1","usd_use_amt":"2"}}',
                    b'{"other":NaN}', b'{"other":Infinity}', b'{"other":1e999}'):
            with self.assertRaises(integrated.KisUsdObservationError):
                integrated.decode_integrated_margin_response(raw)

    def test_output_shape_must_be_single_object(self):
        for output in ([], None, 'SYNTHETIC', {}, {'ovrs_stck_itgr_mgna_dvsn_name': False}):
            if output == {}:
                summary = integrated.safe_observation_summary(capture(Reader(body={'rt_cd': '0', 'output': output})))
                self.assertEqual(summary['reportedNumericFieldCount'], 0)
                continue
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(Reader(body={'rt_cd': '0', 'output': output}))

    def test_exact_response_trid_and_success_required(self):
        for headers in ({}, {'tr_id': 'TTTC2101R'}, {'tr_id': integrated.TR_ID, 'tr_cont': 'X'}):
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(Reader(headers=headers))
        for rt_cd in ('1', 0, True, None):
            body = reported_body()
            body['rt_cd'] = rt_cd
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(Reader(body=body))

    def test_receive_clocks_require_utc_and_monotonic_nondecreasing(self):
        for times, mono in ((('2026-10-19T01:00:00', '2026-10-19T01:00:01'), (1, 2)),
                            (('2026-10-19T01:00:01Z', '2026-10-19T01:00:00Z'), (1, 2)),
                            (('2026-10-19T01:00:00Z', '2026-10-19T01:00:01Z'), (2, 1)),
                            (('2026-10-19T01:00:00Z', '2026-10-19T01:00:01Z'), (True, 2))):
            with self.assertRaises(integrated.KisUsdObservationError):
                capture(times=times, monotonic=mono)

    def test_mutated_or_rehashed_authority_and_query_captures_rejected(self):
        original = capture()
        altered = deepcopy(original)
        altered['body']['output']['usd_use_amt'] = '999.99'
        with self.assertRaisesRegex(integrated.KisUsdObservationError, 'HASH_MISMATCH'):
            integrated.validate_observation(altered)
        for key, value in (('orderAuthorityGranted', True), ('productionReady', True),
                           ('brokerEnrollmentVerified', True), ('maxPages', True),
                           ('queryWithoutAccount', {**integrated.PUBLIC_QUERY, 'WCRC_FRCR_DVSN_CD': '01'}),
                           ('accountBinding', {'profileId': '1' * 64})):
            altered = rehash({**original, key: value})
            with self.assertRaises(integrated.KisUsdObservationError):
                integrated.validate_observation(altered)


class Response:
    status = 200
    def __init__(self, raw, url, headers):
        self.raw, self.url, self.headers = raw, url, headers
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return None
    def read(self, maximum):
        return self.raw[:maximum]
    def geturl(self):
        return self.url


class Opener:
    def __init__(self, raw):
        self.raw = raw
        self.calls = []
    def open(self, req, timeout):
        self.calls.append(req)
        return Response(self.raw, req.full_url, {'tr_id': integrated.TR_ID, 'tr_cont': 'D'})


class NarrowTransportTests(unittest.TestCase):
    def test_exact_route_get_only_and_trid_remain_required(self):
        transport = ro.ReadOnlyTransport()
        transport.opener = Opener(json.dumps(reported_body()).encode())
        for method, path, kwargs in (
            ('POST', integrated.PATH, {}), ('DELETE', integrated.PATH, {}),
            ('GET', integrated.PATH + '/', {}), ('GET', integrated.PATH, {'body': {}}),
            ('GET', integrated.PATH, {'headers': {'tr_id': 'TTTC2101R'}}),
            ('POST', '/uapi/overseas-stock/v1/trading/order', {})):
            with self.assertRaises(ro.ReadBoundaryError):
                transport.request('kis', method, path, **kwargs)
        self.assertEqual(transport.opener.calls, [])
        result, headers = transport.request('kis', 'GET', integrated.PATH,
                                           headers={'tr_id': integrated.TR_ID}, query=integrated.PUBLIC_QUERY)
        self.assertEqual(headers['tr_id'], integrated.TR_ID)
        self.assertEqual(result['rt_cd'], '0')
        self.assertEqual(len(transport.opener.calls), 1)

    def test_duplicate_decoder_used_by_only_new_transport_route(self):
        transport = ro.ReadOnlyTransport()
        transport.opener = Opener(b'{"rt_cd":"0","output":{"usd_use_amt":"1","usd_use_amt":"2"}}')
        with self.assertRaises(ro.ReadFailure):
            transport.request('kis', 'GET', integrated.PATH, headers={'tr_id': integrated.TR_ID})
        self.assertEqual(transport.events[-1]['status'], 'UNAVAILABLE')


if __name__ == '__main__':
    unittest.main()
