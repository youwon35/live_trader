from __future__ import annotations

import base64
import hashlib
import hmac
import json
import unittest

from live_trader.execution_streams import parse_kis_domestic_execution, parse_kis_overseas_execution, parse_upbit_my_order, upbit_websocket_token


class ExecutionStreamTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
