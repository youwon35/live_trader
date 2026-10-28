export const FUTURES_LEVERAGE_INTEGER_LIMIT = Number.MAX_SAFE_INTEGER;

export function futuresRiskLeverageMaximum(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 1 ? number : 1;
}

export function isFuturesRiskStrategy(strategy = {}) {
  const brokerId = String(strategy?.broker_id || "").trim().toLowerCase();
  const marketType = String(strategy?.market_type || "").trim().toLowerCase();
  return brokerId === "binance-futures"
    || ["future", "futures", "perpetual"].includes(marketType);
}

export function futuresRiskStrategyDefaults(strategy = {}) {
  const rawSymbol = String(strategy?.symbol || "ETHUSDT").toUpperCase();
  const rawDirection = String(strategy?.position_direction || "long").toLowerCase();
  const rawPolicyLeverage = Number(
    strategy?.futures_execution_policy?.maxLeverageMultiplier,
  );
  const policyLeverage = futuresRiskLeverageMaximum(rawPolicyLeverage);
  return {
    symbol: rawSymbol.replace(/[^A-Z0-9]/g, "") || "ETHUSDT",
    direction: rawDirection === "short" ? "SHORT" : "LONG",
    leverage: String(policyLeverage),
  };
}

export function shouldHydrateRiskStrategy(previousStrategyId, nextStrategyId) {
  return String(previousStrategyId ?? "") !== String(nextStrategyId ?? "");
}
