import test from "node:test";
import assert from "node:assert/strict";
import { liveAllocationAccounts } from "../src/accountAllocation.js";
import { allocationView } from "../../../packages/design/allocation-model.js";
test("live view uses numeric broker balances instead of formatted strings or internal ledger", () => {
 const result = liveAllocationAccounts([{ broker_id: "kis", currency: "KRW", broker_cash: "999원", broker_cash_value: 100, broker_equity_value: 200 }], [{ broker_id: "kis", symbol: "005930", broker_qty_value: 1, program_qty: 99, broker_value: 100, current_price: 100, valuation_basis: "market_value", currency: "KRW" }]);
 assert.equal(allocationView(result).groups[0].total, 200); assert.equal(result[0].positions[0].quantity, 1);
});
test("cash-only crypto with a cost-basis or unknown price cannot masquerade as current equity", () => {
 for (const basis of ["cost_basis", "unavailable"]) {
  const result = liveAllocationAccounts([{ broker_id: "upbit", currency: "KRW", broker_cash_value: 100, broker_equity_value: 100, valuation_basis: "cash_only" }], [{ broker_id: "upbit", symbol: "BTC", broker_qty_value: 1, broker_value: 20, currency: "KRW", valuation_basis: basis }]);
  assert.equal(allocationView(result).groups[0].total, null);
 }
});

test("KIS market_value flag cannot turn a purchase-cost fallback into a current quote", () => {
 const accounts = [{ broker_id: "kis", currency: "KRW", broker_cash_value: 0, broker_equity_value: 120 }];
 const positions = [{ broker_id: "kis", symbol: "A", broker_qty_value: 2, broker_value: 100, currency: "KRW", valuation_basis: "market_value", current_price: 0 }];
 assert.equal(liveAllocationAccounts(accounts, positions)[0].positions[0].priced, false);
 positions[0].current_price = 60;
 assert.equal(liveAllocationAccounts(accounts, positions)[0].positions[0].value, 120);
});

test("KIS combined holdings keep Korean allocation complete while US cash remains unconfirmed", () => {
 const accounts=[{broker_id:"kis",currency:"KRW",broker_cash_value:100,broker_equity_value:300}];
 const positions=[{broker_id:"kis",symbol:"005930",broker_qty_value:2,current_price:100,broker_value:200,valuation_basis:"market_value",currency:"KRW"}, {broker_id:"kis",symbol:"AAPL",broker_qty_value:2,current_price:50,broker_value:100,valuation_basis:"market_value",currency:"USD"}];
 const normalized=liveAllocationAccounts(accounts,positions);
 assert.equal(normalized.length,2);
 const domestic=allocationView(normalized,{accountId:"kis"});assert.equal(domestic.groups[0].total,300);assert.equal(domestic.rows.length,1);assert.equal(domestic.groups[0].items.length,2);
 const overseas=allocationView(normalized,{accountId:"kis-usd"});assert.equal(overseas.rows[0].value,100);assert.equal(overseas.rows[0].quantity,2);assert.equal(overseas.groups[0].total,null);assert.equal(overseas.rows[0].percent,null);
});

test("KIS zero balance fallback with unpriced holdings cannot appear as an empty account", () => {
 const accounts = [{ broker_id: "kis", currency: "KRW", broker_cash_value: 0, broker_equity_value: 0, valuation_basis: "broker_equity" },
  { broker_id: "upbit", currency: "KRW", broker_cash_value: 100, broker_equity_value: 100, valuation_basis: "cash_only" },
  { broker_id: "binance-futures", currency: "USDT", broker_cash_value: 80, broker_equity_value: 100 }];
 for (const current_price of [0, null, undefined]) {
  // parse_kis_accounts defaults omitted totals to zero; positions can still contain held shares.
  const positions = [{ broker_id: "kis", symbol: "005930", currency: "KRW", broker_qty_value: 2, current_price, broker_value: 0, valuation_basis: "market_value" },
   { broker_id: "upbit", symbol: "KRW-BTC", currency: "KRW", broker_qty_value: 1, current_price: 100, valuation_basis: "market_value" },
   { broker_id: "binance-futures", symbol: "BTCUSDT", currency: "USDT", broker_qty_value: 2, current_price: 200, position_side: "LONG", valuation_basis: "market_notional" }];
  const normalized = liveAllocationAccounts(accounts, positions);
  const kis = allocationView(normalized, { accountId: "kis" });
  assert.equal(kis.groups[0].exact, false); assert.equal(kis.groups[0].total, null);
  assert.equal(kis.rows[0].quantity, 2); assert.equal(kis.rows[0].value, null); assert.equal(kis.rows[0].percent, null);
  const upbit = allocationView(normalized, { accountId: "upbit" });
  assert.equal(upbit.groups[0].total, 200); assert.equal(upbit.rows[0].percent, 50);
  const futures = allocationView(normalized, { accountId: "binance-futures" });
  assert.equal(futures.groups[0].total, 100); assert.equal(futures.groups[0].items.length, 1);
  assert.equal(futures.rows[0].value, 400); assert.equal(futures.rows[0].percent, 400);
 }
});

const valuationNow = Date.parse("2026-10-28T03:00:00Z");
const freshValuation = (currency="USDT") => ({ status: "OBSERVED", currency, current_price: 120, value: 240,
 observed_at: new Date(valuationNow).toISOString(), expires_at: new Date(valuationNow + 60000).toISOString(), order_authority_granted: false });
test("fresh display quote values held spot assets without changing broker cash or native cost basis", () => {
 const accounts=[{broker_id:"binance",currency:"USDT",broker_cash_value:10,broker_equity_value:10,valuation_basis:"cash_only"}];
 const positions=[{broker_id:"binance",symbol:"SOL",currency:"SOL",broker_qty_value:2,current_price:0,broker_value:0,valuation_basis:"unavailable",display_valuation:freshValuation()}];
 const before=structuredClone({accounts,positions});
 const result=allocationView(liveAllocationAccounts(accounts,positions,{nowMs:valuationNow}));
 assert.equal(result.groups[0].total,250); assert.equal(result.rows[0].value,240); assert.equal(result.rows[0].currency,"USDT");
 assert.deepEqual({accounts,positions},before);
});
test("upbit direct KRW display valuation leaves BTC purchase basis out of KRW totals", () => {
 const accounts=[{broker_id:"upbit",currency:"KRW",broker_cash_value:100,broker_equity_value:100,valuation_basis:"cash_only"}];
 const positions=[{broker_id:"upbit",symbol:"BTC-SOL",currency:"BTC",broker_qty_value:2,current_price:0,broker_value:0.02,valuation_basis:"cost_basis",display_valuation:freshValuation("KRW")}];
 const result=allocationView(liveAllocationAccounts(accounts,positions,{nowMs:valuationNow}));
 assert.equal(result.groups[0].total,340); assert.equal(result.rows[0].currency,"KRW");
});
test("expired, unknown, wrong-currency or unbounded display observations cannot appear fresh", () => {
 for (const change of [{status:"UNKNOWN"},{currency:"USD"},{order_authority_granted:true},{expires_at:new Date(valuationNow).toISOString()},
  {observed_at:new Date(valuationNow+6000).toISOString()},{expires_at:new Date(valuationNow+62000).toISOString()},
  {current_price:"Infinity"},{current_price:null}]) {
  const accounts=[{broker_id:"binance",currency:"USDT",broker_cash_value:10,broker_equity_value:10,valuation_basis:"cash_only"}];
  const positions=[{broker_id:"binance",symbol:"SOL",currency:"SOL",broker_qty_value:2,current_price:999,valuation_basis:"market_value",display_valuation:{...freshValuation(),...change}}];
  const result=allocationView(liveAllocationAccounts(accounts,positions,{nowMs:valuationNow}));
  assert.equal(result.groups[0].total,null); assert.equal(result.rows[0].value,null);
 }
});
test("spot display fields cannot change futures margin equity into leveraged notional", () => {
 const accounts=[{broker_id:"binance-futures",currency:"USDT",broker_cash_value:80,broker_equity_value:100}];
 const positions=[{broker_id:"binance-futures",symbol:"SOLUSDT",currency:"USDT",broker_qty_value:2,current_price:200,valuation_basis:"market_notional",position_side:"LONG",display_valuation:freshValuation()}];
 const result=allocationView(liveAllocationAccounts(accounts,positions,{nowMs:valuationNow}));
 assert.equal(result.groups[0].total,100); assert.equal(result.rows[0].value,400); assert.equal(result.groups[0].items.length,1);
});

test("manual account view retains valid past observations without presenting them as fresh quotes", () => {
 const accounts=[{broker_id:"binance",currency:"USDT",broker_cash_value:10,broker_equity_value:10,valuation_basis:"cash_only"}];
 const positions=[{broker_id:"binance",symbol:"SOL",broker_qty_value:2,display_valuation:freshValuation()}];
 const nowMs=valuationNow+3600000;
 assert.equal(liveAllocationAccounts(accounts,positions,{nowMs})[0].positions[0].value,null);
 assert.equal(liveAllocationAccounts(accounts,positions,{nowMs,preserveObserved:true})[0].positions[0].value,240);
 positions[0].display_valuation.status="UNKNOWN";
 assert.equal(liveAllocationAccounts(accounts,positions,{nowMs,preserveObserved:true})[0].positions[0].value,null);
});
