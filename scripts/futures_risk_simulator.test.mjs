import assert from "node:assert/strict";

import {
  futuresRiskLeverageMaximum,
  futuresRiskStrategyDefaults,
  isFuturesRiskStrategy,
  shouldHydrateRiskStrategy,
} from "../src/futuresRiskSimulator.js";

assert.equal(isFuturesRiskStrategy({ broker_id: "binance-futures" }), true);
assert.equal(isFuturesRiskStrategy({ market_type: "perpetual" }), true);
assert.equal(isFuturesRiskStrategy({ broker_id: "binance" }), false);

assert.deepEqual(
  futuresRiskStrategyDefaults({
    symbol: "eth-usdt",
    position_direction: "short",
    futures_execution_policy: { maxLeverageMultiplier: 4 },
  }),
  { symbol: "ETHUSDT", direction: "SHORT", leverage: "4" },
);

assert.equal(futuresRiskLeverageMaximum(4), 4);
assert.equal(futuresRiskLeverageMaximum(150), 150);
assert.equal(futuresRiskLeverageMaximum(1.5), 1);
assert.equal(futuresRiskLeverageMaximum(Number.MAX_SAFE_INTEGER+1), 1);
assert.deepEqual(futuresRiskStrategyDefaults({futures_execution_policy:{maxLeverageMultiplier:150}}).leverage,"150");

assert.equal(
  shouldHydrateRiskStrategy("strategy-1", "strategy-1"),
  false,
  "Replacing the snapshot object for the same strategy must preserve form/result state.",
);
assert.equal(shouldHydrateRiskStrategy("strategy-1", "strategy-2"), true);

console.log("futures risk simulator regression checks passed");
