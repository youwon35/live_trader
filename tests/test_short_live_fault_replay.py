"""New fault/cadence replay against the production service and fake brokers.

Simulated elapsed time is explicitly not an actual six-hour live test.
"""
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from itertools import permutations
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

import test_short_live_session as fixture

ROUTES = ('BINANCE_SPOT', 'BINANCE_USDM_FUTURES', 'UPBIT_KRW_SPOT')


class OvernightLiveReplayTests(unittest.TestCase):
    def setUp(self):
        for name in ('socket.create_connection', 'socket.socket.connect'):
            guard = patch(name, side_effect=AssertionError('external network prohibited'))
            guard.start()
            self.addCleanup(guard.stop)
        self.h = fixture.ShortLiveSessionTests()
        self.h.setUp()
        self.addCleanup(self.h.tearDown)

    def create(self, timeframe='1h', *, neutral_history=False):
        self.h.now, self.h.mono = fixture.NOW, 10.0
        specs = [fixture.runtime_spec(route, route, timeframe) for route in ROUTES]
        service, brokers, feeds = self.h.service(specs)
        if neutral_history:
            for feed in feeds.values():
                original = feed.warmup
                feed.warmup = lambda spec, limit, original=original: [
                    replace(bar, open=100, high=100, low=100, close=100)
                    for bar in original(spec, limit)]
        self.h.start(service)
        return service, brokers, feeds, specs

    def test_same_boundary_three_routes_all_orderings_and_replayed_frames(self):
        for route_order in permutations(ROUTES):
            with self.subTest(route_order=route_order):
                service, brokers, feeds, specs = self.create()
                # Network delivery can interleave open/closed messages. The
                # service must evaluate every route before its matching open.
                self.h.now = fixture.NOW + timedelta(milliseconds=200)
                by_route = {s['route']: s for s in specs}
                service._feeds = {r: feeds[r] for r in route_order}
                for route in route_order:
                    spec = by_route[route]
                    close = fixture.candle(spec, fixture.NOW, 6)
                    opened = fixture.opening(spec, fixture.NOW, 7)
                    feeds[route].rows = [opened, close, close, opened]
                state = service.tick()['runtime']
                self.assertEqual('RUNNING', state['status'], state)
                self.assertEqual([1, 1, 1], [len(brokers[r].submits) for r in ROUTES])
                self.assertEqual([1, 1, 1], [s['decisionCount'] for s in state['strategyRows']])
                for _ in range(3):
                    for route in route_order:
                        spec = by_route[route]
                        feeds[route].rows = [fixture.candle(spec, fixture.NOW, 6), fixture.opening(spec, fixture.NOW, 7)]
                    service.tick()
                self.assertEqual([1, 1, 1], [len(brokers[r].submits) for r in ROUTES])
                self.assertTrue(service.stop({})['runtime']['terminalVerified'])

    def test_disconnect_429_and_slow_feed_fail_once_without_duplicate_cleanup(self):
        for exception in (TimeoutError('offline delayed feed'), ConnectionError('offline disconnected'),
                HTTPError('https://example.invalid', 429, 'offline rate limit', {}, None)):
            with self.subTest(error=type(exception).__name__):
                service, brokers, feeds, specs = self.create()
                def fail(_timeout, error=exception):
                    self.h.mono += 12
                    self.h.now += timedelta(seconds=12)
                    raise error
                feeds['BINANCE_USDM_FUTURES'].poll = fail
                self.h.now = fixture.NOW + timedelta(milliseconds=200)
                for spec in specs:
                    feeds[spec['route']].rows = [fixture.candle(spec, fixture.NOW, 6)]
                first = service.tick()['runtime']
                self.assertIn(type(exception).__name__, first['firstFailure'])
                for _ in range(5):
                    final = service.tick()['runtime']
                self.assertEqual(first['firstFailure'], final['firstFailure'])
                self.assertFalse(any(b.submits for b in brokers.values()))
                self.assertTrue(all(b.cleanups == 1 for b in brokers.values()))
                self.assertTrue(final['newEntriesBlocked'])

    def test_ui_reads_do_not_trigger_account_queries_and_six_hour_replay_is_bounded(self):
        service, brokers, feeds, specs = self.create('5m', neutral_history=True)
        counts = Counter()
        for route, broker in brokers.items():
            original = broker.reconcile
            def counted(session, route=route, original=original):
                counts[route] += 1
                return original(session)
            broker.reconcile = counted
        for _ in range(1000):
            service.snapshot(include_catalog=False)
        self.assertEqual({}, dict(counts))
        # Neutral MA histories never manufacture a trading opportunity.
        for second in range(0, 21600, 5):
            self.h.now = fixture.NOW + timedelta(seconds=second, milliseconds=200)
            self.h.mono = 10 + second
            if second % 300 == 0:
                for spec in specs:
                    bar = fixture.candle(spec, fixture.NOW + timedelta(seconds=second), 100)
                    feeds[spec['route']].rows = [bar, bar]
            state = service.tick()['runtime']
            self.assertEqual('RUNNING', state['status'], (second, state['firstFailure']))
        self.assertFalse(any(b.submits for b in brokers.values()))
        self.assertEqual([72, 72, 72], [s['decisionCount'] for s in state['strategyRows']])
        self.assertLessEqual(len(service._monitor_events), 60)
        for engine in service._engines.values():
            self.assertTrue(all(len(h) <= engine.history_limit for h in engine.histories.values()))
            self.assertLessEqual(len(engine.cycles), 100)
        self.h.now = fixture.NOW + timedelta(hours=6, milliseconds=200)
        self.h.mono = 21610
        terminal = service.tick()['runtime']
        self.assertTrue(terminal['terminalVerified'], terminal)
        self.assertEqual([1, 1, 1], [b.cleanups for b in brokers.values()])

    def test_unknown_submit_reply_remains_unresolved_after_replays_and_restart_attempt(self):
        service, brokers, feeds, specs = self.create()
        broker = brokers['BINANCE_SPOT']
        broker.status = 'RAISE'
        spec = specs[0]
        self.h.first_buy(service, spec)
        first = service.snapshot(include_catalog=False)['runtime']
        for _ in range(10):
            with self.assertRaises(fixture.ShortLiveContractError):
                service.step(spec['route'], fixture.candle(spec, fixture.NOW, 6))
            with self.assertRaises(fixture.ShortLiveContractError):
                service.step(spec['route'], fixture.opening(spec, fixture.NOW, 7))
            service.tick()
        last = service.snapshot(include_catalog=False)['runtime']
        self.assertEqual('ORDER_RESULT_UNKNOWN_NO_RETRY', first['firstFailure'])
        self.assertEqual(first['firstFailure'], last['firstFailure'])
        self.assertEqual(1, len(broker.submits))
        self.assertFalse(last['terminalVerified'])
        self.assertFalse(list(service.store_root.glob('session-terminal-*.json')))
        with self.assertRaises(fixture.ShortLiveContractError):
            self.h.start(service)
