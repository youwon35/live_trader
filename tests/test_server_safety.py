from __future__ import annotations

import json
import math
import unittest

from live_trader.server import json_safe_value, requests_real_order_enable


class ServerSafetyTests(unittest.TestCase):
    def test_non_finite_metrics_are_serialized_as_null(self) -> None:
        payload = {
            "finite": 12.5,
            "nested": [math.inf, -math.inf, math.nan, {"value": 3}],
        }

        safe = json_safe_value(payload)
        encoded = json.dumps(safe, allow_nan=False)

        self.assertEqual(safe["finite"], 12.5)
        self.assertEqual(safe["nested"][:3], [None, None, None])
        self.assertIn('"value": 3', encoded)

    def test_real_order_enable_request_is_detected_before_env_write(self) -> None:
        self.assertTrue(requests_real_order_enable({"values": {"LIVE_TRADER_ENABLE_REAL_ORDERS": "true"}}))
        self.assertFalse(requests_real_order_enable({"values": {"LIVE_TRADER_ENABLE_REAL_ORDERS": "false"}}))
        self.assertFalse(requests_real_order_enable({"values": {"KIS_ACCOUNT_NO": "12345678"}}))


if __name__ == "__main__":
    unittest.main()
