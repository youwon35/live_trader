from __future__ import annotations

import base64
import hashlib
import hmac
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from live_trader import state
from live_trader.execution_streams import ExecutionStreamManager, binance_stream_subscription_params, parse_binance_execution_report, parse_kis_domestic_execution, parse_kis_overseas_execution, parse_upbit_my_order, upbit_websocket_token


class ExecutionStreamTest(unittest.TestCase):
    def test_binance_subscription_uses_server_adjusted_timestamp(self) -> None:
        with patch("live_trader.execution_streams.refresh_binance_time_offset") as refresh, patch(
            "live_trader.execution_streams.binance_timestamp_ms",
            return_value=1700000005123,
        ):
            params = binance_stream_subscription_params("key", "secret")

        refresh.assert_called_once_with()
        self.assertEqual(1700000005123, params["timestamp"])
        expected = hmac.new(
            b"secret",
            b"apiKey=key&timestamp=1700000005123",
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(expected, params["signature"])

    def test_binance_execution_report_normalizes_fill(self) -> None:
        event = parse_binance_execution_report({
            "subscriptionId": 0,
            "event": {
                "e": "executionReport", "E": 1700000000123, "s": "BTCUSDT", "c": "client-1",
                "S": "BUY", "x": "TRADE", "X": "FILLED", "i": 42, "l": "0.01", "L": "43000", "n": "0.00001", "t": 7,
            },
        })
        self.assertIsNotNone(event)
        self.assertEqual("binance", event["broker_id"])
        self.assertEqual("filled", event["state"])
        self.assertEqual("42", event["broker_order_id"])
        self.assertEqual(0.01, event["quantity"])

    def test_execution_log_rotates_with_bounded_backups(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manager = ExecutionStreamManager(Path(temporary), log_max_bytes=1024, log_backup_count=2)
            manager._status["upbit"] = {"lastEventAt": ""}
            for index in range(30):
                manager._record("upbit", {"event_id": f"event-{index}", "occurred_at": "now", "detail": "x" * 120})
            log_dir = Path(temporary) / "logs"
            self.assertTrue((log_dir / "broker_execution_stream.jsonl").exists())
            self.assertTrue((log_dir / "broker_execution_stream.jsonl.1").exists())
            self.assertFalse((log_dir / "broker_execution_stream.jsonl.3").exists())

    def test_upbit_jwt_is_hs512_and_signature_is_valid(self) -> None:
        token = upbit_websocket_token("access", "secret", "fixed-nonce")
        header, payload, signature = token.split(".")
        decoded_header = json.loads(base64.urlsafe_b64decode(header + "=="))
        decoded_payload = json.loads(base64.urlsafe_b64decode(payload + "=="))
        expected = base64.urlsafe_b64encode(
            hmac.new(b"secret", f"{header}.{payload}".encode(), hashlib.sha512).digest()
        ).rstrip(b"=").decode()
        self.assertEqual("HS512", decoded_header["alg"])
        self.assertEqual("fixed-nonce", decoded_payload["nonce"])
        self.assertEqual(expected, signature)

    def test_upbit_my_order_normalizes_fill(self) -> None:
        event = parse_upbit_my_order({
            "type": "myOrder", "uuid": "broker-id", "identifier": "client-id", "code": "KRW-BTC",
            "ask_bid": "BID", "state": "done", "executed_volume": "0.1", "trade_price": "100",
        })
        self.assertIsNotNone(event)
        self.assertEqual("filled", event["state"])
        self.assertEqual("BUY", event["side"])
        self.assertEqual(0.1, event["quantity"])

    def test_kis_domestic_execution_normalizes_fill(self) -> None:
        fields = ["customer", "account", "order-1", "", "02", "", "", "", "069500", "2", "35000", "090001", "N", "2"]
        event = parse_kis_domestic_execution(fields)
        self.assertIsNotNone(event)
        self.assertEqual("069500.KS", event["symbol"])
        self.assertEqual("filled", event["state"])
        self.assertEqual(2.0, event["quantity"])

    def test_kis_overseas_execution_normalizes_official_25_columns(self) -> None:
        fields = [""] * 25
        fields[2] = "order-us-1"
        fields[4] = "02"
        fields[7] = "AAPL"
        fields[8] = "1"
        fields[9] = "327.69"
        fields[10] = "160001"
        fields[11] = "N"
        fields[12] = "2"
        event = parse_kis_overseas_execution(fields)
        self.assertIsNotNone(event)
        self.assertEqual("AAPL", event["symbol"])
        self.assertEqual("filled", event["state"])
        self.assertEqual(327.69, event["price"])

    def test_new_fill_queues_telegram_with_ledger_position(self) -> None:
        event = {
            "broker_id": "binance",
            "event_id": "trade-telegram-1",
            "order_id": "client-1",
            "broker_order_id": "broker-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.001,
            "price": 50_000,
            "fee": 0.01,
            "state": "filled",
            "occurred_at": "2026-07-25T00:00:00Z",
            "raw": {},
        }
        with patch.object(state.PROGRAM_LEDGER, "position_rows", return_value=[
            {"broker_id": "binance", "symbol": "BTCUSDT", "quantity": 0.002, "value": 100, "currency": "USDT"}
        ]), patch.object(state.PROGRAM_LEDGER, "cash_rows", return_value=[
            {"broker_id": "binance", "cash": 20, "currency": "USDT"}
        ]), patch.object(state.TELEGRAM_DISPATCHER, "send_async", return_value=True) as send:
            count = state.notify_new_live_fills([event], set())

        self.assertEqual(1, count)
        send.assert_called_once()
        message = send.call_args.args[0]
        self.assertIn("BTCUSDT", message)
        self.assertIn("0.001 → 0.002", message)


if __name__ == "__main__":
    unittest.main()
