# Account portfolio preparation — 2026-10-16

The five applications share immutable research artifacts and runtime contracts. Stock Data Scraper supplies market data; Backtester creates canonical Sources, strategy instances and portfolios; Paper Trader validates those exact artifacts on live public feeds and publishes sealed evidence; Hub Trader launches the applications; Live Trader performs final account, evidence, cost, limit and order checks.

## Account portfolio execution

An account portfolio is one sealed, single-broker portfolio. Each running deployment has its own exact account/credential/currency binding, session, journal and engine. Binance and Upbit can coexist. The selected runtime determines its displayed mode and stop target. Global MONITOR immediately removes dispatch authority even if a worker cannot stop. Unknown or ambiguous stop identities fail without stopping another account. Multiple deployments cannot independently own the same account writer. KIS KRW and USD books do not mix principal, receipts or external holdings; simultaneous cross-market KIS feeds remain blocked.

All member dispatches must retain the current parent portfolio permission and all Source/instance/Paper hashes. A standalone member permission cannot substitute for the parent deployment. Session replacement and rollback use the exact predecessor scope.

## Native broker boundaries

Binance current exchange bounds may narrow a frozen Source quantity but never rewrite the research Source. Price event time, exchange clock and receipt age are separate checks. Absolute saved KRW limits are converted only using a fresh, internally obtained, timestamped native-currency observation. Missing or mismatched currency evidence yields zero permission. USDT is not treated as USD.

KIS supports KRX, NASD, NYSE and AMEX whole-share protective limit tickets. Review explicitly records the execution overlay and cost reserve without claiming research market fills are equivalent. The conservative reserve is not the account's actual fee or a broker-observed minimum. Broker order dates and branch/order keys require exact official verification; no Korean-date fallback is used for a missing US order date. Verified terminal receipts may be cached, while fresh current account and instrument checks remain mandatory.

## Readiness limits

Production KIS authority remains closed. The implementation does not assert that all accounts are ready to start. A positive orderable balance and quantity, trusted conversion evidence when required, exact natural Paper qualification, reconciliation, fresh preflight, reviewed policies and explicit manual start are required. Local Paper failures remain failures; elapsed time alone never qualifies a portfolio. No real orders, cancellations, live runtime starts or production principal initialization were performed during this work.
