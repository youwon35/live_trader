"""Offline public-FX recovery: no credentials, private APIs, orders or apps."""
from datetime import datetime, timezone, timedelta
from email.utils import format_datetime
import importlib
import json
import unittest
from unittest.mock import Mock, patch

from test_short_live_brokers import PKG
from test_short_live_reader_diagnostics import build_readers

readers = importlib.import_module(PKG + '.short_live_broker_readers')
recovery = importlib.import_module(PKG + '.short_live_fx_recovery')
fx = importlib.import_module(PKG + '.account_currency_limits')
safe = importlib.import_module(PKG + '.short_live_read_diagnostics')
clockproof = importlib.import_module(PKG + '.native_fx_observation')


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 30, 15, 56, 36, 250000, tzinfo=timezone.utc).timestamp()
        self.waits = []

    def now(self): return self.value
    def sleep(self, duration):
        self.waits.append(duration)
        self.value += duration


class FxRecoveryTests(unittest.TestCase):
    def run_observation(self, ages, *, durations=None, stop=None):
        clock = self.clock = Clock()
        self.calls, self.budgets = [], []
        remaining = iter(ages)
        durations = iter(durations or [.1] * len(ages))
        parent = self

        class ClockDateTime(datetime):
            @classmethod
            def now(cls, tz=None): return datetime.fromtimestamp(clock.value, timezone.utc)

        class Public:
            def _get(self, provider, path, *, query):
                parent.calls.append((provider, path, query))
                age = next(remaining)
                if isinstance(age, Exception):
                    raise age
                stamp = int((clock.value-age)*1000)
                clock.value += next(durations)
                return [{'market':'KRW-USDT', 'timestamp':stamp, 'level':0,
                    'total_ask_size':'10', 'total_bid_size':'20',
                    'orderbook_units':[{'ask_price':'1401','bid_price':'1400','ask_size':'1','bid_size':'2'}]}], {
                    'date':format_datetime(datetime.fromtimestamp(clock.value, timezone.utc).replace(microsecond=0), usegmt=True)}

        def factory(budget):
            self.budgets.append(budget)
            return Public()

        observe = readers.production_fx_reader(None, clock=clock.now, reader_factory=factory,
            monotonic=clock.now, sleeper=clock.sleep,
            can_continue=(lambda: True) if stop is None else lambda: stop(clock))
        with patch.object(fx, 'datetime', ClockDateTime):
            return observe('USDT')

    def test_actual_failure_is_source_age_not_http_latency(self):
        start = datetime(2026,10,30,15,56,36,2665,tzinfo=timezone.utc)
        received = datetime(2026,10,30,15,56,36,275847,tzinfo=timezone.utc)
        evidence = clockproof.fx_clock_observation(source_ms=1793375781865,
            started=start, received=received, checked=received,
            date_header='Fri, 30 Oct 2026 15:56:36 GMT', age_header=None,
            round_trip_ns=273165500)
        self.assertEqual(14.410847, evidence['sourceAgeSeconds'])
        self.assertEqual(15.408182, evidence['sourceAgeUpperSeconds'])
        self.assertEqual(.273182, evidence['readElapsedSeconds'])
        self.assertIsNotNone(recovery._aged_source(evidence))

    def test_stale_public_book_then_fresh_is_recovered_without_relabeling(self):
        result = self.run_observation([20, 0])
        self.assertTrue(result['known'])
        detail = result['fxRecovery']
        self.assertEqual(('RECOVERED',2), (detail['status'],detail['attemptCount']))
        self.assertEqual('BLOCKED', detail['firstObservation']['status'])
        self.assertEqual('FRESH', detail['lastObservation']['status'])
        self.assertNotEqual(detail['firstObservation']['sourceTimestampMs'],detail['lastObservation']['sourceTimestampMs'])
        self.assertEqual(detail['lastObservation']['sourceObservedAt'],
            datetime.fromisoformat(result['observedAt']).isoformat(timespec='microseconds'))
        self.assertEqual([.5], self.clock.waits)
        self.assertTrue(all(p=='upbit' and path=='/v1/orderbook' for p,path,q in self.calls))
        self.assertTrue(all(q=={'markets':'KRW-USDT','count':1,'level':'0'} for p,path,q in self.calls))
        self.assertFalse(detail['orderResubmitted'])
        r = build_readers(); r.fx_reader = lambda _: result
        r._fx('USDT')
        self.assertEqual(detail, r._failure_reads()[-1]['fxRecovery'])
        self.assertEqual('AVAILABLE', r._failure_reads()[-1]['status'])

    def test_permanent_old_book_exhausts_three_attempts_preserves_first_failure(self):
        with self.assertRaises(fx.CurrentFxObservationError) as raised:
            self.run_observation([20,20,20])
        detail = raised.exception.fxRecovery
        self.assertEqual(('EXHAUSTED',3),(detail['status'],detail['attemptCount']))
        self.assertEqual(3,len(self.calls)); self.assertEqual([.5,.5],self.clock.waits)
        self.assertEqual('NATIVE_FX_OBSERVATION_STALE_OR_FUTURE',str(raised.exception))
        self.assertEqual('BLOCKED',detail['firstObservation']['status'])
        self.assertLess(detail['elapsedMs'],3000)

    def test_future_source_and_transport_errors_never_retry(self):
        for value, error in [(-3,fx.CurrentFxObservationError),(TimeoutError('SECRET'),TimeoutError)]:
            with self.subTest(value=value), self.assertRaises(error):
                self.run_observation([value])
            self.assertEqual(1,len(self.calls)); self.assertEqual([],self.clock.waits)

    def test_stop_during_wait_prevents_next_public_read(self):
        with self.assertRaises(TimeoutError) as raised:
            self.run_observation([20,0],stop=lambda clock: not clock.waits)
        self.assertEqual(1,len(self.calls))
        self.assertEqual('ABORTED',raised.exception.fxRecovery['status'])

    def test_whole_deadline_is_not_reset_per_retry(self):
        with self.assertRaises(fx.CurrentFxObservationError) as raised:
            self.run_observation([20,20,20],durations=[1,1,1])
        self.assertEqual(2,len(self.calls))
        self.assertEqual('EXHAUSTED',raised.exception.fxRecovery['status'])
        self.assertAlmostEqual(3,self.budgets[0]); self.assertAlmostEqual(1.5,self.budgets[1])

    def test_late_success_cannot_bypass_whole_call_deadline(self):
        with self.assertRaises(TimeoutError):
            self.run_observation([0],durations=[3.1])
        self.assertEqual(1,len(self.calls))

    def test_public_reader_has_no_credentials_and_bounded_timeout(self):
        account = importlib.import_module(PKG + '.read_only_account')
        transport = Mock(); base = Mock()
        with patch.object(account,'ReadOnlyTransport',return_value=transport) as make_transport, \
                patch.object(account,'ReadOnlyBrokerReader',return_value=base) as make_reader:
            reader = recovery.public_reader(.7)
        make_transport.assert_called_once_with(timeout=.7)
        make_reader.assert_called_once_with({},transport=transport)
        with self.assertRaisesRegex(ValueError,'READ_ONLY_ENDPOINT_BLOCKED'):
            reader._get('upbit','/v1/accounts',query={})
        base._get.assert_not_called()

    def test_recovery_diagnostic_is_allowlisted_and_does_not_grant_authority(self):
        result=self.run_observation([20,0]); detail=result['fxRecovery']
        raw={**detail,'apiKey':'SECRET','balances':['PRIVATE']}
        self.assertEqual(detail,recovery.sanitize_fx_recovery(raw))
        self.assertNotIn('SECRET',json.dumps(recovery.sanitize_fx_recovery(raw)))
        for changed in ({'orderResubmitted':True},{'attemptCount':100},{'elapsedMs':float('nan')},{'elapsedMs':10**10000},
                {'firstObservation':result['fxObservation']},{'lastObservation':detail['firstObservation']}):
            self.assertIsNone(recovery.sanitize_fx_recovery({**detail,**changed}))


if __name__ == '__main__': unittest.main()
