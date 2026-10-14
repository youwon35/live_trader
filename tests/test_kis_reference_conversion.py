from __future__ import annotations

from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import timedelta
from decimal import Decimal
import hashlib
import importlib
import json
from pathlib import Path
import sys
import traceback
import types
import unittest
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP.parents[1] / 'packages/trading_runtime'))
sys.path.insert(0, str(APP / 'tests'))
PACKAGE = '_kis_reference_conversion_isolated'
pkg = types.ModuleType(PACKAGE)
pkg.__path__ = [str(APP / 'live_trader')]
sys.modules[PACKAGE] = pkg
service = importlib.import_module(PACKAGE + '.kis_reference_conversion')
from test_native_equity_observation import NOW, fixture, stamp

SECRET = 'SYNTHETIC_RAW_PRIVATE_SENTINEL_NOT_REAL'


def raw_fixture(*, shape='object'):
    output = {'ord_psbl_cash': '0001000.0000', 'nrcvb_buy_amt': '000900.0000',
              'nrcvb_buy_qty': '0009', 'psbl_qty_calc_unpr': '00100.0000',
              'max_buy_amt': '999999999', 'max_buy_qty': '9999999',
              'CANO': SECRET, 'appsecret': SECRET}
    return {'rt_cd': '0', 'msg1': SECRET,
            'output': [output] if shape == 'list' else output}


def encoded(body):
    return json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def arguments(exchange='KRX', portfolio=False):
    payload, expected, source = fixture(exchange, portfolio)
    request = {key: value for key, value in payload['request'].items() if key != 'requestHash'}
    return {'request_public': request, 'request_origin': service.PRODUCTION_ORIGIN,
            'response_continuation': '', 'expected_scope': expected,
            'source_quantity_contract': source, 'request_started_at': payload['requestStartedAt'],
            'response_received_at': payload['responseReceivedAt'], 'expires_at': payload['expiresAt'],
            'now': NOW}


class KisReferenceConversionTests(unittest.TestCase):
    def convert(self, raw=None, args=None):
        raw = encoded(raw_fixture()) if raw is None else raw
        args = arguments() if args is None else args
        with patch('socket.socket', side_effect=AssertionError('network forbidden')), \
             patch('builtins.open', side_effect=AssertionError('file IO forbidden')):
            return service.convert_kis_buy_reference_response(raw, **args)

    def rejected(self, raw=None, args=None, code=None):
        with self.assertRaises(service.KisReferenceConversionError) as caught:
            self.convert(raw, args)
        error = caught.exception
        self.assertIsNone(error.__context__)
        self.assertIsNone(error.__cause__)
        self.assertNotIn(SECRET, str(error))
        self.assertNotIn(SECRET, repr(error))
        self.assertNotIn(SECRET, ''.join(traceback.format_exception(error)))
        if code is not None:
            self.assertIn(code, str(error))
        return error

    def test_domestic_object_and_singleton_list_are_reference_only(self):
        for shape in ('object', 'list'):
            for portfolio in (False, True):
                with self.subTest(shape=shape, portfolio=portfolio):
                    result = self.convert(encoded(raw_fixture(shape=shape)), arguments(portfolio=portfolio))
                    response = result.reference['response']
                    self.assertEqual(('900', '9', '100'), tuple(response[key] for key in
                                     ('availableCash', 'maximumBuyQuantity', 'calculationPrice')))
                    self.assertEqual('UNKNOWN', result.reference['fee']['kind'])
                    self.assertEqual('UNVERIFIED', result.verification_status)
                    self.assertEqual('UNKNOWN', result.native_eligibility)
                    self.assertFalse(result.live_allowed or result.order_authority_granted
                                     or result.actual_fee_observed or result.authenticated_transport)
                    self.assertEqual('output.singleton-list' if shape == 'list' else 'output.object',
                                     result.provenance['outputShape'])

    def test_field_roles_and_conservative_min_are_explicit_and_not_capital(self):
        for first, second, want in [('1000', '900', '900'), ('400', '900', '400')]:
            raw = raw_fixture(); raw['output'].update(ord_psbl_cash=first, nrcvb_buy_amt=second)
            result = self.convert(encoded(raw))
            self.assertEqual(want, result.reference['response']['availableCash'])
            mapping = result.provenance['fieldMappings']['availableCash']
            self.assertEqual(['ord_psbl_cash', 'nrcvb_buy_amt'], mapping['rawFields'])
            self.assertEqual(['REPORTED_ORDERABLE_CASH', 'NO_MARGIN_BUY_AMOUNT'], mapping['roles'])
            self.assertEqual('CONSERVATIVE_MIN', mapping['operation'])
            self.assertEqual('QUERY_REFERENCE_NOT_OWNED_CAPITAL', mapping['meaning'])
            self.assertFalse(result.provenance['capitalOwnershipVerified'])
            self.assertFalse(result.provenance['affordabilityVerified'])
            self.assertFalse(result.provenance['nativeMarketMaximumGuaranteed'])
        # No inference that quantity * price is affordable under that cash reference.
        raw = raw_fixture(); raw['output'].update(ord_psbl_cash='1', nrcvb_buy_amt='1', nrcvb_buy_qty='9')
        result = self.convert(encoded(raw))
        self.assertEqual('9', result.reference['response']['maximumBuyQuantity'])
        self.assertFalse(result.to_dict()['affordabilityVerified'])

    def test_raw_digest_request_and_scope_hashes_bind_provenance(self):
        raw = encoded(raw_fixture()); args = arguments(); result = self.convert(raw, args)
        provenance = result.provenance
        self.assertEqual(hashlib.sha256(raw).hexdigest(), provenance['rawResponseSha256'])
        self.assertEqual(len(raw), provenance['rawResponseBytes'])
        self.assertEqual(result.reference['contentHash'], provenance['referenceClaimContentHash'])
        self.assertEqual(service.observation_content_hash(args['expected_scope']), provenance['expectedScopeClaimHash'])
        self.assertEqual(args['source_quantity_contract']['contractHash'], provenance['sourceQuantityContractHash'])
        body = {key: value for key, value in provenance.items() if key != 'contentHash'}
        self.assertEqual(service.observation_content_hash(body), provenance['contentHash'])
        public = dict(provenance['publicRequest']); supplied_hash = public.pop('requestHash')
        self.assertEqual(service.observation_content_hash(public), supplied_hash)

    def test_result_and_inputs_are_immutable_detached_and_private_free(self):
        raw = encoded(raw_fixture()); args = arguments(); before = deepcopy(args)
        result = self.convert(raw, args); reference = result.reference; provenance = result.provenance
        self.assertEqual(before, args)
        args['expected_scope']['accountAlias'] = 'later edit'
        result.reference['response']['availableCash'] = '0'
        result.provenance['fieldMappings']['availableCash']['roles'].append('changed')
        self.assertEqual(reference, result.reference); self.assertEqual(provenance, result.provenance)
        for value in (repr(result), json.dumps(result.to_dict()), json.dumps(result.provenance)):
            self.assertNotIn(SECRET, value)
            self.assertNotIn('000900.0000', value)
            self.assertNotIn('999999999', value)
            self.assertNotIn('CANO', value)
            self.assertNotIn('appsecret', value)
        for key, value in [('verification_status', 'VERIFIED'), ('live_allowed', True),
                           ('native_eligibility', 'VERIFIED'), ('authenticated_transport', True)]:
            with self.assertRaises(FrozenInstanceError): setattr(result, key, value)
            with self.assertRaises(TypeError): service.KisReferenceConversion(result._validation, result._provenance_json, **{key: value})

    def test_selected_decimal_conversion_is_exact_without_binary_float(self):
        raw = encoded(raw_fixture())
        raw = raw.replace(b'"0001000.0000"', b'1e3').replace(b'"000900.0000"', b'900.000000000000000000000000000001')
        raw = raw.replace(b'"0009"', b'9').replace(b'"00100.0000"', b'100.0')
        result = self.convert(raw)
        self.assertEqual('900.000000000000000000000000000001', result.reference['response']['availableCash'])
        self.assertEqual('9', result.reference['response']['maximumBuyQuantity'])
        self.assertEqual('100', result.reference['response']['calculationPrice'])

    def test_zero_values_still_have_no_order_authority(self):
        raw = raw_fixture(); raw['output'].update(ord_psbl_cash='0', nrcvb_buy_amt='0', nrcvb_buy_qty='0')
        result = self.convert(encoded(raw))
        self.assertEqual('0', result.reference['response']['availableCash'])
        self.assertEqual('0', result.reference['response']['maximumBuyQuantity'])
        self.assertFalse(result.live_allowed)

    def test_selected_fields_missing_never_use_max_fields_or_fallback(self):
        for key in ('ord_psbl_cash', 'nrcvb_buy_amt', 'nrcvb_buy_qty', 'psbl_qty_calc_unpr'):
            raw = raw_fixture(); del raw['output'][key]
            with self.subTest(key=key): self.rejected(encoded(raw), code='SELECTED_RESPONSE_FIELDS_REQUIRED')

    def test_selected_numbers_reject_bad_type_nonfinite_negative_and_fractional_qty(self):
        for field in ('ord_psbl_cash', 'nrcvb_buy_amt', 'nrcvb_buy_qty', 'psbl_qty_calc_unpr'):
            for value in (None, True, '', 'NaN', 'Infinity', '-1', '-0', ' 1', '1e3', [], {}, '1' * 257):
                raw = raw_fixture(); raw['output'][field] = value
                with self.subTest(field=field, value=type(value).__name__): self.rejected(encoded(raw))
        for value in ('0.5', 1.5):
            raw = raw_fixture(); raw['output']['nrcvb_buy_qty'] = value
            self.rejected(encoded(raw), code='WHOLE_SHARE_REQUIRED')
        for value in ('0', 0):
            raw = raw_fixture(); raw['output']['psbl_qty_calc_unpr'] = value
            self.rejected(encoded(raw))
        for number in (b'-0', b'1e257', b'1e-257'):
            self.rejected(encoded(raw_fixture()).replace(b'"0001000.0000"', number))

    def test_broker_calculation_price_mismatch_is_not_query_substitution(self):
        raw = raw_fixture(); raw['output']['psbl_qty_calc_unpr'] = '99'
        self.rejected(encoded(raw), code='REFERENCE_VALIDATION_FAILED')

    def test_duplicate_json_keys_are_rejected_even_in_ignored_fields(self):
        raw = encoded(raw_fixture())
        for altered in (raw.replace(b'"rt_cd":"0"', b'"rt_cd":"0","rt_cd":"0"'),
                        raw.replace(b'"nrcvb_buy_qty":"0009"', b'"nrcvb_buy_qty":"0009","nrcvb_buy_qty":"0009"'),
                        raw[:-1] + b',"ignored":{"secret":"x","secret":"y"}}'):
            self.rejected(altered, code='DUPLICATE_JSON_KEY')

    def test_nonfinite_json_constants_anywhere_are_rejected(self):
        for constant in (b'NaN', b'Infinity', b'-Infinity'):
            raw = encoded(raw_fixture())[:-1] + b',"ignored":' + constant + b'}'
            self.rejected(raw, code='NONFINITE_JSON_CONSTANT')

    def test_failure_rt_code_and_malformed_output_shapes_fail_closed(self):
        for rt in ('1', 0, False, None):
            raw = raw_fixture(); raw['rt_cd'] = rt
            self.rejected(encoded(raw), code='SUCCESS_RESPONSE_REQUIRED')
        for output in (None, [], [{}, {}], [1], 'bad', 1):
            raw = raw_fixture(); raw['output'] = output
            self.rejected(encoded(raw))
        for raw in (b'null', b'[]', b'1', b'"string"', b'{}'):
            self.rejected(raw)

    def test_parser_failure_context_does_not_contain_raw_private_content(self):
        for raw in (b'{"private":"' + SECRET.encode() + b'",', b'\xff' + SECRET.encode(),
                    b'{"ignored":' + b'[' * 1100 + b'0' + b']' * 1100 + b'}'):
            self.rejected(raw)

    def test_bytes_size_and_type_are_bounded(self):
        for raw in ('not-bytes-' + SECRET, bytearray(encoded(raw_fixture())), b'', b' ' * (service.MAX_RESPONSE_BYTES + 1)):
            self.rejected(raw, code='RAW_BYTES_BOUND')

    def test_incomplete_or_unknown_continuation_does_not_auto_paginate(self):
        for marker in ('F', 'M', 'D', 'E', ' ', None, False, 0):
            args = arguments(); args['response_continuation'] = marker
            self.rejected(args=args, code='INCOMPLETE_OR_UNSUPPORTED_CONTINUATION')

    def test_exact_public_request_origin_route_and_query_scope_are_required(self):
        for origin in ('https://openapi.koreainvestment.com', 'https://openapivts.koreainvestment.com:29443',
                       'https://user:' + SECRET + '@openapi.koreainvestment.com:9443', service.PRODUCTION_ORIGIN + '?secret=' + SECRET):
            args = arguments(); args['request_origin'] = origin
            self.rejected(args=args, code='DECLARED_PRODUCTION_ORIGIN_REQUIRED')
        for key, value in [('method', 'POST'), ('endpoint', '/orders'), ('trId', 'VTTC8908R'), ('bodyAbsent', False)]:
            args = arguments(); args['request_public'][key] = value
            self.rejected(args=args)
        for key, value in [('PDNO', '000000'), ('ORD_UNPR', '99'), ('ORD_DVSN', '01'),
                           ('CMA_EVLU_AMT_ICLD_YN', 'Y'), ('OVRS_ICLD_YN', 'Y')]:
            args = arguments(); args['request_public']['queryProjection'][key] = value
            self.rejected(args=args)
        for holder in ('request_public', 'expected_scope'):
            args = arguments(); args[holder]['secret'] = SECRET
            self.rejected(args=args)

    def test_scope_and_source_binding_remain_strict_with_no_live_upgrade(self):
        for key, value in [('provider', 'upbit'), ('environment', 'KIS_DEMO'), ('side', 'SELL'),
                           ('currency', 'USD'), ('market', 'CRYPTO'), ('symbol', '000000'), ('exchange', 'OTHER'),
                           ('deploymentRevision', True), ('sourceQuantityContractHash', 'f' * 64),
                           ('portfolioId', 'partial')]:
            args = arguments(); args['expected_scope'][key] = value
            self.rejected(args=args)
        for key, value in [('contractHash', 'f' * 64), ('symbol', '000000'), ('capabilityStatus', 'VERIFIED')]:
            args = arguments(); args['source_quantity_contract'][key] = value
            self.rejected(args=args)
        for holder in ('request_public', 'expected_scope', 'source_quantity_contract'):
            args = arguments(); args[holder]['self'] = args[holder]
            self.rejected(args=args)

    def test_existing_v1_time_order_and_exact_freshness_endpoints_are_preserved(self):
        args = arguments(); args.update(request_started_at=stamp(-15), expires_at=stamp(0))
        self.assertEqual('VALID', self.convert(args=args).to_dict()['schema_validation'])
        for key, value in [('request_started_at', stamp(-15.000001)), ('request_started_at', stamp(1)),
                           ('response_received_at', stamp(1)), ('expires_at', stamp(-.000001)),
                           ('expires_at', stamp(14.000001)), ('response_received_at', '2026-10-15T00:00:00'),
                           ('now', NOW + timedelta(seconds=16)), ('now', NOW.replace(tzinfo=None))]:
            args = arguments(); args[key] = value
            self.rejected(args=args)

    def test_us_is_explicitly_unsupported_even_with_fake_calculation_echo(self):
        for exchange in ('NASD', 'NYSE', 'AMEX'):
            for raw in ({'rt_cd': '0', 'output': {'tr_crcy_cd': 'USD', 'ord_psbl_qty': '9', 'ovrs_ord_psbl_amt': '900'}}, raw_fixture()):
                self.rejected(encoded(raw), arguments(exchange), code='US_BROKER_CALCULATION_PRICE_MISSING_UNSUPPORTED')

    def test_pure_converter_has_no_file_network_clock_state_or_entry_imports(self):
        # Source inspection protects the intentionally unwired transformation boundary.
        text = (APP / 'live_trader/kis_reference_conversion.py').read_text(encoding='utf-8')
        for forbidden in ('datetime.now(', 'time.time(', 'monotonic(', 'requests.', 'urlopen(',
                          'read_bytes(', 'write_text(', 'SecretStore', 'LiveState', 'native_order_ticket'):
            self.assertNotIn(forbidden, text)
        self.assertFalse(self.convert().live_allowed)


if __name__ == '__main__':
    unittest.main()
