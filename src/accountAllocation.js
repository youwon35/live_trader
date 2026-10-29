// Adapter for read-only reconciliation snapshots. Never parses display strings as money.
import { allocationNumber } from "../../../packages/design/allocation-model.js";
const labels = { kis: "KIS 국내", binance: "Binance 현물", "binance-futures": "Binance 선물", upbit: "Upbit 현물" };
const providerOf = row => String(row.broker_id || "unmatched").trim().toLowerCase();
const currencyOf = value => String(value || "UNKNOWN").trim().toUpperCase();
const accountScope = row => String(row.account_id ?? row.accountId ?? row.account ?? "").trim();
const spotProvider = provider => ["binance", "upbit"].includes(provider);
const cashCurrency = provider => provider === "binance" ? "USDT" : provider === "upbit" ? "KRW" : null;
function cashPosition(position) {
  const currency = cashCurrency(providerOf(position));
  // A quote currency is not a held asset. BTC/USDT and KRW-BTC stay holdings.
  return currency !== null && String(position.symbol || "").trim().toUpperCase() === currency;
}
function positionRow(position, currency, futures = false, nowMs = Date.now(), preserveObserved = false) {
  const quantity = allocationNumber(position.broker_qty_value);
  const provider = providerOf(position), spot = spotProvider(provider);
  const display = spot && position.display_valuation && typeof position.display_valuation === "object" ? position.display_valuation : null;
  const price = allocationNumber(display ? display.current_price : position.current_price);
  const expectedCurrency = cashCurrency(provider);
  const observed = display ? Date.parse(display.observed_at || "") : NaN;
  const expires = display ? Date.parse(display.expires_at || "") : NaN;
  const quoteReady = display ? display.status === "OBSERVED" && display.order_authority_granted === false
    && display.currency === expectedCurrency && Number.isFinite(observed) && Number.isFinite(expires)
    && observed <= nowMs + 5000 && (preserveObserved || expires > nowMs) && expires > observed && expires - observed <= 61000
    : ["market_value", "market_notional"].includes(position.valuation_basis);
  const evaluated = quantity !== null && price !== null && price > 0 && (!spot || quantity >= 0) ? Math.abs(quantity) * price : null;
  const value = quoteReady && Number.isFinite(evaluated) ? evaluated : null;
  const side = String(position.position_side || position.positionSide || "").toUpperCase();
  return { id: `${position.symbol}:${side || "SPOT"}`, symbol: position.symbol,
    name: position.name || position.symbol, quantity, currency: display ? expectedCurrency : currencyOf(position.currency || currency),
    value, priced: value !== null, observedAt: display?.observed_at || null,
    side: futures ? (["LONG", "SHORT"].includes(side) ? side : "UNKNOWN") : "SPOT" };
}
export function liveAllocationAccounts(accounts = [], positions = [], { nowMs = Date.now(), preserveObserved = false } = {}) {
  const held = positions.filter(position => allocationNumber(position.broker_qty_value) !== 0 && !cashPosition(position));
  const normalized = accounts.map(account => {
    const provider = providerOf(account), currency = currencyOf(account.currency), scope = accountScope(account);
    const scoped = account.account_id != null || account.accountId != null || accounts.filter(other => providerOf(other) === provider).length > 1;
    return { source: account, provider, scope, currency, id: scoped ? JSON.stringify([provider, currency, scope || "UNVERIFIED"]) : provider, positions: [], ambiguous: false };
  });
  const unmatched = [];
  for (const position of held) {
    const provider = providerOf(position), scope = accountScope(position);
    let candidates = normalized.filter(account => account.provider === provider)
      .filter(account => provider !== "kis" || account.currency === currencyOf(position.currency));
    if (scope) candidates = candidates.filter(account => account.scope === scope);
    if (candidates.length === 1) candidates[0].positions.push(positionRow(position, candidates[0].currency, provider === "binance-futures", nowMs, preserveObserved));
    else {
      for (const candidate of candidates) candidate.ambiguous = true;
      unmatched.push(position);
    }
  }
  const result = normalized.map(({ source: account, provider, scope, currency, id, positions: items, ambiguous }) => {
    const repeated = normalized.filter(other => other.provider === provider && other.currency === currency).length > 1;
    const label = provider === "kis" && currency === "USD" ? "KIS 미국" : labels[provider] || account.broker_name || provider;
    const failed = account.ready === false || ["FAILED", "ERROR", "UNAVAILABLE"].includes(String(account.lookup_status || account.query_status || account.read_status || "").toUpperCase());
    return { id, label: repeated ? `${label} · ${scope || "계좌 미확인"}` : label, currency: /^[A-Z]{3,5}$/.test(currency) ? currency : "UNKNOWN",
      cash: allocationNumber(account.broker_cash_value), equity: allocationNumber(account.broker_equity_value),
      observedAt: typeof account.observed_at === "string" ? account.observed_at : null,
      lookupStatus: ["error","failed","unavailable"].includes(String(account.lookup_status || account.query_status || account.read_status || "").toLowerCase()) ? "error" : ["success","stale"].includes(account.lookup_status) ? account.lookup_status : "unknown",
      cashOnly: account.valuation_basis === "cash_only", futures: provider === "binance-futures",
      ready: !failed && !ambiguous && (allocationNumber(account.broker_cash_value) !== null || allocationNumber(account.broker_equity_value) !== null),
      positions: items, reason: ambiguous ? "보유 종목의 계좌 식별 정보가 없어 여러 계좌 중 어느 계좌에 속하는지 확인하지 못했습니다. 중복 배정하지 않습니다."
        : failed ? "계좌 조회가 실패했거나 미확인입니다. 저장된 금액을 새 조회 결과로 간주하지 않습니다."
        : items.some(position => !position.priced) ? "현재 평가 시세가 없는 보유 종목이 있습니다. 매입 원가를 현재 평가액으로 대신하지 않습니다." : undefined };
  });
  for (const position of unmatched) {
    const provider = providerOf(position), scope = accountScope(position);
    const currency = cashCurrency(provider) || currencyOf(position.currency);
    const id = provider === "kis" && !scope ? `kis-${currency.toLowerCase()}` : JSON.stringify(["unassigned", provider, currency, scope]);
    const row = positionRow(position, currency, provider === "binance-futures", nowMs, preserveObserved);
    const group = result.find(account => account.id === id);
    if (group) { group.positions.push(row); continue; }
    result.push({ id, label: provider === "kis" ? (currency === "USD" ? "KIS 미국" : `KIS 해외 ${currency}`) : `${labels[provider] || position.broker_name || "거래소"} · 계좌 미확인`,
      currency, ready: false, positions: [row], reason: "보유 수량과 확인된 시세는 표시하지만 이 계좌의 현금·총평가액 또는 계좌 범위를 확인하지 못해 총액과 비중을 표시하지 않습니다." });
  }
  return result;
}
