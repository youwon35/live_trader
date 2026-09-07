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
