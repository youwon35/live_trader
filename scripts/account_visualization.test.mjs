import assert from "node:assert/strict";

import {
  buildAccountVisualization,
  formatAllocationValue,
  valuationBasisLabel,
} from "../src/accountVisualization.js";

const model = buildAccountVisualization(
  [
    {
      broker_id: "kis",
      broker_name: "한국투자증권",
      currency: "KRW",
      broker_cash_value: 400_000,
      broker_equity_value: 600_000,
      valuation_basis: "broker_equity",
    },
    {
      broker_id: "upbit",
      broker_name: "Upbit",
      currency: "KRW",
      broker_cash_value: 400_000,
      valuation_basis: "cash_only",
    },
    {
      broker_id: "binance-futures",
      broker_name: "Binance Futures",
      currency: "USDT",
      broker_cash_value: 80,
      broker_equity_value: 100,
      valuation_basis: "margin_balance",
    },
  ],
  [
    {
      broker_id: "kis",
      broker_name: "한국투자증권",
      symbol: "005930.KS",
      currency: "KRW",
      broker_qty_value: 10,
      broker_value: 500_000,
      valuation_basis: "market_value",
    },
    {
      broker_id: "binance-futures",
      broker_name: "Binance Futures",
      symbol: "BTCUSDT",
      position_side: "SHORT",
      currency: "USDT",
      broker_qty_value: -0.01,
      broker_value: 650,
      valuation_basis: "market_notional",
    },
    {
      broker_id: "binance",
      broker_name: "Binance Spot",
      symbol: "BTC",
      currency: "BTC",
      broker_qty_value: 0.001,
      broker_value: 0,
      valuation_basis: "unavailable",
    },
  ],
);

assert.equal(model.accountCount, 3);
assert.equal(model.positionCount, 3);
assert.equal(model.capitalGroups.length, 2);
const krw = model.capitalGroups.find((group) => group.currency === "KRW");
assert.equal(krw.total, 1_000_000);
assert.equal(krw.items[0].label, "한국투자증권");
assert.equal(krw.items[0].ratio, 0.6);
assert.equal(krw.items[1].ratio, 0.4);

const futures = model.exposureGroups.find((group) => group.brokerId === "binance-futures");
assert.equal(futures.total, 650);
assert.equal(futures.items[0].label, "BTCUSDT · SHORT");
assert.equal(futures.items[0].basis, "명목 노출");
assert.equal(model.missingValuationCount, 1);

assert.equal(formatAllocationValue(123456.4, "KRW"), "123,456 KRW");
assert.equal(formatAllocationValue(12.34567, "USDT"), "12.3457 USDT");
assert.equal(valuationBasisLabel("cost_basis"), "매입 원가 기준");

console.log("account visualization contracts passed");
