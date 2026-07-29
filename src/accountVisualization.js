const DEFAULT_COLORS = [
  "#2563eb",
  "#16a34a",
  "#f59e0b",
  "#7c3aed",
  "#0891b2",
  "#dc2626",
  "#4f46e5",
  "#0f766e",
];

function finiteNumber(value) {
  if (value == null || value === "") return null;
  const parsed = Number(String(value).replaceAll(",", "").replace(/[^0-9.+-]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function positiveValue(value) {
  const parsed = finiteNumber(value);
  return parsed != null && parsed > 0 ? parsed : 0;
}

function normalizedCurrency(value) {
  return String(value || "기타").trim().toUpperCase() || "기타";
}

function normalizedBrokerId(value) {
  return String(value || "unknown").trim().toLowerCase() || "unknown";
}

function withRatios(items) {
  const total = items.reduce((sum, item) => sum + item.value, 0);
  let cumulative = 0;
  return {
    total,
    items: items.map((item, index) => {
      const ratio = total > 0 ? item.value / total : 0;
      const result = {
        ...item,
        color: DEFAULT_COLORS[index % DEFAULT_COLORS.length],
        ratio,
        offset: cumulative,
      };
      cumulative += ratio;
      return result;
    }),
  };
}
export function formatAllocationValue(value, currency) {
  const numeric = Number(value || 0);
  const unit = normalizedCurrency(currency);
  if (unit === "KRW") {
    return `${Math.round(numeric).toLocaleString("ko-KR")} KRW`;
  }
  return `${numeric.toLocaleString("ko-KR", {
    maximumFractionDigits: numeric >= 100 ? 2 : 4,
    minimumFractionDigits: numeric >= 100 ? 0 : 2,
  })} ${unit}`;
}

export function valuationBasisLabel(value) {
  const basis = String(value || "").trim().toLowerCase();
  if (basis === "broker_equity") return "브로커 총 평가";
  if (basis === "margin_balance") return "선물 마진 잔액";
  if (basis === "market_value") return "시장 평가";
  if (basis === "market_notional") return "명목 노출";
  if (basis === "cost_basis") return "매입 원가 기준";
  if (basis === "cash_only") return "현금성 잔고";
  return "평가 대기";
}

export function buildAccountVisualization(accounts = [], positions = []) {
  const capitalByCurrency = new Map();
  for (const account of accounts) {
    const currency = normalizedCurrency(account.currency);
    const equity = finiteNumber(account.broker_equity_value);
    const cash = finiteNumber(account.broker_cash_value);
    const value = Math.max(0, equity ?? cash ?? 0);
    const group = capitalByCurrency.get(currency) || [];
    group.push({
      id: normalizedBrokerId(account.broker_id),
      label: String(account.broker_name || account.account || account.broker_id || "계좌"),
      value,
      basis: valuationBasisLabel(account.valuation_basis),
    });
    capitalByCurrency.set(currency, group);
  }

  const capitalGroups = Array.from(capitalByCurrency.entries())
    .map(([currency, items]) => ({
      currency,
      ...withRatios(items.sort((left, right) => right.value - left.value)),
    }))
    .sort((left, right) => right.total - left.total);

  const exposureByBrokerCurrency = new Map();
  let missingValuationCount = 0;
  for (const position of positions) {
    const quantity = Math.abs(finiteNumber(position.broker_qty_value) ?? finiteNumber(position.broker_qty) ?? 0);
    if (quantity <= 0) continue;
    const brokerId = normalizedBrokerId(position.broker_id);
    const currency = normalizedCurrency(position.currency);
    const key = `${brokerId}:${currency}`;
    const value = positiveValue(position.broker_value);
    if (value <= 0) missingValuationCount += 1;
    const group = exposureByBrokerCurrency.get(key) || {
      brokerId,
      brokerLabel: String(position.broker_name || position.broker_id || "브로커"),
      currency,
      items: [],
    };
    group.items.push({
      id: String(position.id || `${brokerId}:${position.symbol}:${position.position_side || "NET"}`),
      label: `${position.symbol || "-"}${position.position_side ? ` · ${position.position_side}` : ""}`,
      value,
      basis: valuationBasisLabel(position.valuation_basis),
    });
    exposureByBrokerCurrency.set(key, group);
  }

  const exposureGroups = Array.from(exposureByBrokerCurrency.values())
    .map((group) => ({
      ...group,
      ...withRatios(group.items.filter((item) => item.value > 0).sort((left, right) => right.value - left.value)),
      pendingCount: group.items.filter((item) => item.value <= 0).length,
    }))
    .sort((left, right) => right.total - left.total);

  return {
    capitalGroups,
    exposureGroups,
    accountCount: accounts.length,
    positionCount: positions.filter((position) => {
      const quantity = Math.abs(finiteNumber(position.broker_qty_value) ?? finiteNumber(position.broker_qty) ?? 0);
      return quantity > 0;
    }).length,
    missingValuationCount,
  };
}
