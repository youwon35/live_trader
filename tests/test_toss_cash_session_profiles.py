from types import SimpleNamespace
import unittest
from trading_runtime.stock_session_profile import normalize_stock_session_profile, profile_hash
from live_trader.toss_cash_sources import execution_session_profile, session_label


class TossCashSessionSourceTests(unittest.TestCase):
    def spec(self,provider='kis',mode='REGULAR'):
        profile=normalize_stock_session_profile(mode,market='US',provider=provider)
        return SimpleNamespace(provider=provider,broker_id=provider,symbol='AAPL',parameters={'stockSessionProfile':profile})

    def test_explicit_regular_rebinding_preserves_original_and_records_actual_toss_scope(self):
        spec=self.spec();original=dict(spec.parameters['stockSessionProfile'])
        source,actual=execution_session_profile(spec)
        self.assertEqual(original,source)
        self.assertEqual('US',source['venue'])
        self.assertEqual('TOSS_US',actual['venue'])
        self.assertEqual(['REGULAR'],actual['segments'])
        self.assertNotEqual(profile_hash(source),profile_hash(actual))
        self.assertEqual(original,spec.parameters['stockSessionProfile'])

    def test_kis_extended_source_cannot_silently_become_toss_extended_history(self):
        with self.assertRaisesRegex(ValueError,'TOSS_EXTENDED_SOURCE_REEXPORT_REQUIRED'):
            execution_session_profile(self.spec(mode='EXTENDED'))

    def test_native_toss_extended_source_keeps_its_exact_profile(self):
        source,actual=execution_session_profile(self.spec(provider='toss',mode='EXTENDED'))
        self.assertEqual(source,actual)
        self.assertEqual(profile_hash(source),profile_hash(actual))
        self.assertIn('시간외',session_label(actual))


if __name__=='__main__': unittest.main()
