from __future__ import annotations
import os
os.environ["TELEGRAM_ENABLED"] = "false"
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from live_trader import state
from trading_runtime.telegram_notifications import TelegramDispatcher, TelegramSettings


class ImportantTelegramIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.sent = []
        settings = TelegramSettings("live_trader", True, "test-token", "test-chat", min_severity="critical")
        patch = mock.patch.object(TelegramDispatcher, "settings", new_callable=mock.PropertyMock, return_value=settings)
        patch.start(); self.addCleanup(patch.stop)
        self.dispatcher = TelegramDispatcher("live_trader", config_path=Path(self.tmp.name)/"telegram.json", sender=lambda settings,text:self.sent.append(text))
        patch = mock.patch.object(state, "TELEGRAM_DISPATCHER", self.dispatcher)
        patch.start(); self.addCleanup(patch.stop)

    def event(self, level, detail):
        state.queue_live_audit_telegram(level, "체결 스트림", detail)
        self.dispatcher.queue.join()

    def test_measurement_drift_recovery_and_recurrence(self):
        self.event("info", "binance 연결 복구")
        self.event("warning", "binance timeout 3회 지연 50초")
        self.event("warning", "binance timeout 8회 지연 70초")
        self.event("info", "binance 연결 복구")
        self.event("info", "binance 연결 복구")
        self.event("warning", "binance timeout 8회 지연 90초")
        self.assertEqual(3, len(self.sent))
        self.assertIn("복구 확인", self.sent[1])

    def test_another_broker_and_critical_escalation_still_notify(self):
        self.event("warning", "binance timeout 1회")
        self.event("warning", "upbit timeout 2회")
        self.event("danger", "upbit timeout 3회")
        self.assertEqual(3, len(self.sent))


if __name__ == "__main__": unittest.main()
