import assert from "node:assert/strict";

import {
  futuresRiskLeverageOptions,
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

assert.deepEqual(futuresRiskLeverageOptions(4, "4"), [1, 2, 3, 4]);
assert.deepEqual(futuresRiskLeverageOptions(5, "5"), [1, 2, 3, 4, 5]);
assert.deepEqual(
  futuresRiskLeverageOptions(3, "4"),
  [1, 2, 3, 4],
  "A refreshed lower policy must not silently rewrite the operator's current input.",
);

assert.equal(
  shouldHydrateRiskStrategy("strategy-1", "strategy-1"),
  false,
  "Replacing the snapshot object for the same strategy must preserve form/result state.",
);
assert.equal(shouldHydrateRiskStrategy("strategy-1", "strategy-2"), true);

console.log("futures risk simulator regression checks passed");
