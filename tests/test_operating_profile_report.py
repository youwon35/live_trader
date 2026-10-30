import json
from pathlib import Path
import tempfile
import unittest

from live_trader.operating_profile import load_profile, save_profile
from live_trader.session_report import terminal_report


class OperatingProfileTests(unittest.TestCase):
    def test_only_configuration_is_persisted_and_hashes_survive(self):
        with tempfile.TemporaryDirectory() as root:
            contract = dict(durationHours=6, capitalKrwPerBroker=100000,
                dailyLossStopKrwPerBroker=50000, preparedAt='2026-10-31T00:00:00Z',
                accountId='PRIVATE', confirmed=True, sessionId='old-session',
                futuresPolicy=dict(leverage=1, marginMode='ISOLATED', stopLossPercent=50))
            selections = [dict(selectionKey='a', instanceFileSha256='a'*64,
                artifactFileSha256='b'*64, secret='PRIVATE')]
            save_profile(root, contract, selections)
            encoded = (Path(root) / 'operating-profile.json').read_text(encoding='utf-8')
            self.assertNotIn('PRIVATE', encoded)
            self.assertNotIn('old-session', encoded)
            self.assertNotIn('confirmed', encoded)
            self.assertEqual(load_profile(root)['selections'][0]['instanceFileSha256'], 'a'*64)
            self.assertFalse(json.loads(encoded)['automaticStartAllowed'])
            contract['durationHours'] = 7
            save_profile(root, contract, selections)
            self.assertEqual(load_profile(root)['durationHours'], 7)
            self.assertFalse(list(Path(root).glob('*.tmp')))

    def test_corrupt_profile_never_becomes_configuration(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertIsNone(load_profile(root))
            target = Path(root)/'operating-profile.json'
            target.write_text('{incomplete', encoding='utf-8')
            self.assertIsNone(load_profile(root))

    def test_no_signal_is_distinct_from_no_evaluation_and_error(self):
        session = dict(sessionId='fixture', startedAt='2026-10-31T00:00:00Z',
            terminatedAt='2026-10-31T06:00:00Z', terminalVerified=True, orderCount=0,
            fillCount=0, feesKnown=True, firstFailure=None, sleeves={'s': {}},
            contract={'specs': [dict(strategyInstanceId='s', route='BINANCE_SPOT',
                                     symbol='BTCUSDT', timeframe='1h')]})
        self.assertEqual(terminal_report(session)['outcome'], 'NO_EVALUATIONS')
        session['sleeves']['s']['decisionCount'] = 6
        report = terminal_report(session)
        self.assertEqual(report['outcome'], 'NO_ORDERS')
        self.assertEqual(report['strategies'][0]['outcome'], 'NO_ORDER_ATTEMPT')
        session['firstFailure'] = 'READ_FAILED'
        self.assertEqual(terminal_report(session)['outcome'], 'ERROR_RECORDED')
        self.assertFalse(report['promotionEligible'])
        self.assertFalse(report['profitabilityVerified'])


if __name__ == '__main__':
    unittest.main()
