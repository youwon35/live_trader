from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from live_trader.live_adapters import (
    KIS_DOMESTIC_ORDER_ENDPOINT,
    build_kis_live_order_request,
)
from trading_runtime.realtime_feeds import infer_market_route


class InverseEtfRouteTests(unittest.TestCase):
    def test_korean_inverse_etf_is_a_normal_long_cash_etf_order(
        self,
    ) -> None:
        with patch.dict(
            os.environ,
            {
                "KIS_APP_KEY": "key",
                "KIS_APP_SECRET": "secret",
                "KIS_ACCOUNT_NO": "12345678",
                "KIS_ACCOUNT_PRODUCT_CODE": "01",
            },
            clear=False,
        ):
            request = build_kis_live_order_request(
                {
                    "broker_id": "kis",
                    "symbol": "114800",
                    "asset": "KR_STOCK ETF",
                    "side": "BUY",
                    "quantity": 2,
                    "price": 0,
                    "order_type": "01",
                },
                access_token="access-token",
            )

        self.assertTrue(request.can_send)
        self.assertEqual(KIS_DOMESTIC_ORDER_ENDPOINT, request.endpoint)
        self.assertEqual("114800", request.body["PDNO"])
        self.assertEqual("2", request.body["ORD_QTY"])
        self.assertEqual("01", request.body["ORD_DVSN"])
        self.assertEqual("TTTC0012U", request.safe_headers["tr_id"])
        self.assertEqual(
            ("yahoo", "kis", "114800.KS"),
            infer_market_route("114800"),
        )


if __name__ == "__main__":
    unittest.main()
