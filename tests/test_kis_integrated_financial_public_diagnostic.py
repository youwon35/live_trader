"""Synthetic public diagnostic privacy schema; no API, DB or settings reads."""
from copy import deepcopy
from datetime import datetime
import importlib
import json
import unittest

import test_kis_integrated_margin_observation as fixture

public = importlib.import_module(fixture.PACKAGE + '.kis_integrated_financial_preparation')
shared = importlib.import_module(fixture.PACKAGE + '.kis_shared_krw_readiness')


def unavailable(status='UNAVAILABLE'):
    return {'schemaVersion': public.SCHEMA, 'status': status,
            'integratedObservation': None, 'sharedKrwReadiness': None,
            'localOwnedInventory': None, **public.FLAGS}


def reported():
    capture = fixture.capture()
    summary = fixture.integrated.safe_observation_summary(capture)
    dependency = shared.inspect_shared_krw_readiness(
        integrated_observation=capture, owned_books=None, open_orders=None,
        now=datetime.fromisoformat(capture['completedAt']))
    return {**unavailable(), 'status': 'REPORTED_REFERENCE_AND_LOCAL_DEPENDENCIES_ONLY',
            'integratedObservation': summary,
            'sharedKrwReadiness': shared.safe_shared_krw_readiness_summary(dependency),
            'localOwnedInventory': {
                'status': 'UNKNOWN', 'reasons': ['KIS_LOCAL_OWNED_DATABASE_MISSING_NOT_ZERO_FUNDS'],
                'localGeneration': None, 'dependencyHash': None,
                'sourceMeaning': 'EXISTING_LOCAL_OWNED_BOOKS_NOT_ACCOUNT_WIDE_CASH'}}


class PublicDiagnosticTests(unittest.TestCase):
    def reject(self, value):
        with self.assertRaisesRegex(ValueError, '^KIS_INTEGRATED_FINANCIAL_PUBLIC_DIAGNOSTIC_INVALID$'):
            public.validate_integrated_financial_preparation_diagnostic(value)

    def test_normal_public_summaries_roundtrip_to_new_nested_dicts(self):
        original = reported()
        actual = public.validate_integrated_financial_preparation_diagnostic(original)
        self.assertEqual(actual, original)
        self.assertIsNot(actual, original)
        for key in ('integratedObservation', 'sharedKrwReadiness', 'localOwnedInventory'):
            self.assertIsNot(actual[key], original[key])
        self.assertIsNot(actual['integratedObservation']['numericFieldStates'], original['integratedObservation']['numericFieldStates'])
        self.assertIsNot(actual['sharedKrwReadiness']['checks'], original['sharedKrwReadiness']['checks'])
        self.assertIsNot(actual['localOwnedInventory']['reasons'], original['localOwnedInventory']['reasons'])
        for key in public.FLAGS:
            self.assertIs(actual[key], False)

    def test_unavailable_and_foreign_cash_accept_only_empty_nested_reports(self):
        for status in ('UNAVAILABLE', 'NOT_REQUESTED_FOR_FOREIGN_CASH'):
            self.assertEqual(public.validate_integrated_financial_preparation_diagnostic(unavailable(status)), unavailable(status))
            for key in ('integratedObservation', 'sharedKrwReadiness', 'localOwnedInventory'):
                bad = unavailable(status); bad[key] = reported()[key]
                self.reject(bad)

    def test_top_schema_extra_fields_status_and_false_types_are_strict(self):
        for key, value in (('rawAccount', 'SYNTHETIC_PRIVATE'), ('status', 'READY'),
                           ('schemaVersion', 'other'), ('actualFeeObserved', 0),
                           ('productionReady', True), ('orderAuthorityGranted', None)):
            self.reject({**reported(), key: value})
        bad = reported(); bad.pop('orderAuthorityGranted'); self.reject(bad)

    def test_original_nested_private_balance_and_remote_message_reproduction_rejected(self):
        for parent in ('integratedObservation', 'sharedKrwReadiness', 'localOwnedInventory'):
            for key, value in (('privateBalance', 'SYNTHETIC_PRIVATE_LEAK'),
                               ('remoteMessage', 'SYNTHETIC_REMOTE'), ('body', {'account': 'SYNTHETIC_ACCOUNT'})):
                bad = reported(); bad[parent][key] = value
                self.reject(bad)

    def test_collector_fixed_meanings_roles_and_query_cannot_carry_remote_text(self):
        for key, value in (('enrollmentClassification', '신청계좌'),
                           ('savedProfileBinding', 'SYNTHETIC_ACCOUNT'),
                           ('reportedCurrencyBasis', 'USD_CASH'), ('hashMeaning', 'WIRE_PROOF'),
                           ('retrievalTimeMeaning', 'SETTLED'), ('trId', 'OTHER'),
                           ('paginationStatus', 'READY'), ('enrollmentLabelPresence', 1),
                           ('brokerEnrollmentVerified', True), ('pageCount', True)):
            bad = reported(); bad['integratedObservation'][key] = value
            self.reject(bad)
        for parent in ('queryWithoutAccount', 'reportedRoleNames', 'numericFieldStates'):
            bad = reported(); bad['integratedObservation'][parent]['PRIVATE_ACCOUNT'] = 'SYNTHETIC_VALUE'
            self.reject(bad)
        bad = reported(); bad['integratedObservation']['reportedRoleNames']['usd_use_amt'] = 'SYNTHETIC_REMOTE'
        self.reject(bad)
        bad = reported(); bad['integratedObservation']['queryWithoutAccount']['WCRC_FRCR_DVSN_CD'] = '01'
        self.reject(bad)

    def test_numeric_states_and_counts_are_consistent_plain_ints(self):
        for state in ('0', 'PRIVATE_12345', 0, None, True):
            bad = reported(); bad['integratedObservation']['numericFieldStates']['usd_use_amt'] = state
            self.reject(bad)
        for key, value in (('reportedNumericFieldCount', True), ('reportedNumericFieldCount', 33),
                           ('emptyNumericFieldCount', 1), ('absentNumericFieldCount', -1),
                           ('unreportedOtherFieldCount', '1000.00'), ('unreportedOtherFieldCount', 257)):
            bad = reported(); bad['integratedObservation'][key] = value
            self.reject(bad)

    def test_collector_public_utc_times_reject_naive_offset_and_reversal(self):
        for start, end in (('2026-10-19T01:00:00', '2026-10-19T01:00:01'),
                           ('2026-10-19T10:00:00+09:00', '2026-10-19T10:00:01+09:00'),
                           ('2026-10-19T01:00:01Z', '2026-10-19T01:00:00Z'),
                           (True, '2026-10-19T01:00:01Z')):
            bad = reported(); bad['integratedObservation'].update(startedAt=start, completedAt=end)
            self.reject(bad)

    def test_public_hashes_reject_plain_values_uppercase_and_nonstrings(self):
        for parent, fields in (('integratedObservation', ('contentHash',)),
                               ('sharedKrwReadiness', ('contentHash', 'dependencyHash'))):
            for key in fields:
                for value in ('PRIVATE_ACCOUNT_VALUE', 'A' * 64, 'a' * 63, True, None, 0):
                    bad = reported(); bad[parent][key] = value
                    self.reject(bad)

    def test_shared_reason_allowlist_rejects_prefix_and_duplicate_injection(self):
        for reason in ('KIS_SHARED_KRW_PRIVATE_ACCOUNT_12345678', 'SYNTHETIC_REMOTE_MESSAGE', 'KIS_LOCAL_OWNED_SCHEMA_INVALID'):
            bad = reported(); bad['sharedKrwReadiness']['reasons'].append(reason)
            self.reject(bad)
        bad = reported(); bad['sharedKrwReadiness']['reasons'].append(bad['sharedKrwReadiness']['reasons'][0]); self.reject(bad)
        bad = reported(); bad['sharedKrwReadiness']['reasons'] = []; self.reject(bad)
        bad = reported(); bad['sharedKrwReadiness']['reasons'].remove('KIS_SHARED_KRW_GLOBAL_RESERVATION_PRODUCER_UNAVAILABLE'); self.reject(bad)

    def test_shared_cannot_add_cash_authority_unsupported_currencies_or_remote_checks(self):
        for key, value in (('status', 'READY'), ('sharedKrwAvailableAmount', '12345.67'),
                           ('productionReady', True), ('currencyConversionPerformed', 0),
                           ('hashMeaning', 'AUTHENTICATED_FUNDS')):
            bad = reported(); bad['sharedKrwReadiness'][key] = value
            self.reject(bad)
        for currencies in (['GBP'], ['USD', 'USD'], ['USD', 'KRW'], ['KRW', 1], ('USD',)):
            bad = reported(); bad['sharedKrwReadiness']['observedBookCurrencies'] = currencies
            self.reject(bad)
        for key, value in (('completeAccountWorkingOrdersVerified', True),
                           ('reportedIntegratedObservationValid', 1), ('REMOTE_CHECK', False)):
            bad = reported(); bad['sharedKrwReadiness']['checks'][key] = value
            self.reject(bad)

    def test_shared_counts_are_bounded_not_booleans_and_pending_cannot_exceed_rows(self):
        for key, value in (('localOwnedBookCount', True), ('localOwnedBookCount', 10001),
                           ('localOrderRowCount', 1000000), ('localNonterminalLabelCount', -1)):
            bad = reported(); bad['sharedKrwReadiness'][key] = value
            self.reject(bad)
        bad = reported(); bad['sharedKrwReadiness'].update(localOrderRowCount=1, localNonterminalLabelCount=2); self.reject(bad)

    def test_local_inventory_status_reason_revision_and_hash_form_consistent_tuple(self):
        for key, value in (('status', 'READY'), ('reasons', ['KIS_LOCAL_OWNED_PRIVATE_ACCOUNT_12345678']),
                           ('sourceMeaning', 'SYNTHETIC_REMOTE'), ('localGeneration', True),
                           ('dependencyHash', 'a' * 64)):
            bad = reported(); bad['localOwnedInventory'][key] = value
            self.reject(bad)
        valid = reported(); valid['localOwnedInventory'].update(
            status='OBSERVED_LOCAL_INVENTORY_ONLY', reasons=['KIS_LOCAL_OWNED_INVENTORY_NOT_EXTERNAL_ACCOUNT_PROOF'],
            localGeneration=1, dependencyHash='a' * 64)
        self.assertEqual(public.validate_integrated_financial_preparation_diagnostic(valid), valid)
        for key, value in (('localGeneration', '1'), ('localGeneration', -1), ('localGeneration', True),
                           ('dependencyHash', None), ('dependencyHash', 'PRIVATE_AMOUNT'),
                           ('reasons', ['KIS_LOCAL_OWNED_DATABASE_UNVERIFIED'])):
            bad = deepcopy(valid); bad['localOwnedInventory'][key] = value
            self.reject(bad)

    def test_every_authority_flag_must_be_exact_false_at_each_public_boundary(self):
        for container, flags in ((None, public.FLAGS), ('integratedObservation', fixture.integrated.FALSE_CLAIMS),
                                  ('sharedKrwReadiness', public.SHARED_FLAGS)):
            for key in flags:
                bad = reported(); target = bad if container is None else bad[container]
                target[key] = True
                self.reject(bad)

    def test_missing_nested_field_is_not_silently_filled_from_defaults(self):
        for parent, key in (('integratedObservation', 'contentHash'),
                            ('integratedObservation', 'reportedNumericFieldCount'),
                            ('sharedKrwReadiness', 'localOrderRowCount'),
                            ('localOwnedInventory', 'localGeneration')):
            bad = reported(); bad[parent].pop(key)
            self.reject(bad)

    def test_caller_mutation_of_validated_result_does_not_change_input(self):
        source = reported(); before = deepcopy(source)
        validated = public.validate_integrated_financial_preparation_diagnostic(source)
        validated['integratedObservation']['numericFieldStates']['usd_use_amt'] = 'ABSENT'
        validated['sharedKrwReadiness']['checks']['reportedIntegratedObservationValid'] = False
        validated['localOwnedInventory']['reasons'].append('SYNTHETIC')
        self.assertEqual(source, before)
        for private in ('SYNTHETIC_REMOTE', '123456.7800', '12345678', 'SYNTHETIC_TOKEN'):
            self.assertNotIn(private, json.dumps(before))


if __name__ == '__main__':
    unittest.main()
