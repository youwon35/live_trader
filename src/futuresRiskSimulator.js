export const FUTURES_RISK_HARD_MAX_LEVERAGE = 5;

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
  const policyLeverage = Number.isFinite(rawPolicyLeverage)
    ? Math.min(
      FUTURES_RISK_HARD_MAX_LEVERAGE,
      Math.max(1, Math.floor(rawPolicyLeverage)),
    )
    : 1;
  return {
    symbol: rawSymbol.replace(/[^A-Z0-9]/g, "") || "ETHUSDT",
    direction: rawDirection === "short" ? "SHORT" : "LONG",
    leverage: String(policyLeverage),
  };
}

export function shouldHydrateRiskStrategy(previousStrategyId, nextStrategyId) {
  return String(previousStrategyId ?? "") !== String(nextStrategyId ?? "");
}

export function futuresRiskLeverageOptions(policyMaxLeverage, currentLeverage) {
  const rawPolicyMax = Number(policyMaxLeverage);
  const policyMax = Number.isFinite(rawPolicyMax)
    ? Math.min(
      FUTURES_RISK_HARD_MAX_LEVERAGE,
      Math.max(1, Math.floor(rawPolicyMax)),
    )
    : 1;
  const values = Array.from({ length: policyMax }, (_, index) => index + 1);
  const selected = Number(currentLeverage);
  if (
    Number.isInteger(selected)
    && selected >= 1
    && selected <= FUTURES_RISK_HARD_MAX_LEVERAGE
    && !values.includes(selected)
  ) {
    values.push(selected);
    values.sort((left, right) => left - right);
  }
  return values;
}
