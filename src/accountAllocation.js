// Adapter for the read-only reconciliation snapshot. Displayed strings are never parsed as money.
import { allocationNumber } from "../../../packages/design/allocation-model.js";
const labels = { kis: "KIS 국내", binance: "Binance 현물", "binance-futures": "Binance 선물", upbit: "Upbit 현물" };
function positionRow(position, currency, futures = false) {
  const quantity = allocationNumber(position.broker_qty_value);
  const price = allocationNumber(position.current_price);
  // KIS may label a purchase-cost fallback as market_value. Only a quote can value holdings.
  const value = quantity !== null && price !== null && price > 0 ? Math.abs(quantity) * price : null;
  const side = String(position.position_side || position.positionSide || "").toUpperCase();
  return { id: `${position.symbol}:${side || "SPOT"}`, symbol: position.symbol,
    name: position.name || position.symbol, quantity, currency: position.currency || currency,
    value, priced: value !== null && ["market_value", "market_notional"].includes(position.valuation_basis),
    side: futures ? (["LONG", "SHORT"].includes(side) ? side : "UNKNOWN") : "SPOT" };
}
export function liveAllocationAccounts(accounts = [], positions = []) {
  const held = positions.filter(position => allocationNumber(position.broker_qty_value) !== 0);
  const result = accounts.map((account) => {
    const provider = String(account.broker_id || "");
    const currency = String(account.currency || "UNKNOWN").toUpperCase();
    const items = held.filter(position => String(position.broker_id || "") === provider)
      .filter(position => provider !== "kis" || String(position.currency || "").toUpperCase() === currency)
      .map(position => positionRow(position, currency, provider === "binance-futures"));
    return { id: provider, label: provider === "kis" && currency === "USD" ? "KIS 미국" : labels[provider] || account.broker_name || provider,
      currency: /^[A-Z]{3,5}$/.test(currency) ? currency : "UNKNOWN",
      cash: allocationNumber(account.broker_cash_value), equity: allocationNumber(account.broker_equity_value),
      cashOnly: account.valuation_basis === "cash_only", futures: provider === "binance-futures",
      ready: allocationNumber(account.broker_cash_value) !== null || allocationNumber(account.broker_equity_value) !== null,
      positions: items, reason: items.some(position => !position.priced) ? "현재 평가 시세가 없는 보유 종목이 있습니다. 매입 원가를 현재 평가액으로 대신하지 않습니다." : undefined };
  });
  // The KIS account endpoint reports domestic cash, while holdings includes US positions.
  // Preserve those separate routes without inventing foreign cash/equity or invalidating KRW allocation.
  for (const position of held) {
    const provider = String(position.broker_id || "unmatched");
    const currency = String(position.currency || "UNKNOWN").toUpperCase();
    if (accounts.some(account => String(account.broker_id) === provider && (provider !== "kis" || String(account.currency).toUpperCase() === currency))) continue;
    const id = provider === "kis" ? `kis-${currency.toLowerCase()}` : provider;
    const row = positionRow(position, currency, provider === "binance-futures");
    const unmatched = result.find(account => account.id === id);
    if (unmatched) { unmatched.positions.push(row); continue; }
    result.push({ id, label: provider === "kis" ? (currency === "USD" ? "KIS 미국" : `KIS 해외 ${currency}`) : position.broker_name || "계좌 미확인",
      currency, ready: false, positions: [row],
      reason: "보유 수량과 확인된 시세 평가액은 표시하지만, 이 계좌의 현금·총평가액이 없어 총액과 비중을 표시하지 않습니다." });
  }
  return result;
}
