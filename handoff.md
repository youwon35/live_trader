# Live Trader Handoff

Last updated: 2026-07-11 KST
Project path: `D:\github\PROGRAM\trading-system\apps\live_trader`
Repository: `https://github.com/youwon35/live_trader.git`
Primary branch: `develop`
Latest known pushed code commit before this rewrite: `25d2bd33fdbffaab92428bd24e428d844ad6543e`

## One Sentence Summary

`live_trader` is the real-money execution console in the `trading-system` workspace. It prepares broker/API connections, reads approved strategy artifacts, checks account/risk state, runs dry-run/test order gates, and packages as a desktop EXE, but actual real-money broker order sending must remain blocked until every live safety condition is explicitly satisfied and tested.

## Workspace Relationship

The broader workspace is `D:\github\PROGRAM\trading-system`.

The four user-facing programs are expected to work as one trading pipeline:

1. `stock_data_scraper`
   - Collects and prepares market data.
   - It is upstream of research/backtest workflows.
   - Its layout-editing behavior has been used as a reference for resizable panels.
2. `backtester`
   - Researches and validates strategy ideas on historical data.
   - Exports strategy artifacts into the shared strategy area.
   - UI styling is a key design reference for Live Trader and Paper Trader.
3. `paper_trader`
   - Runs approved strategies in paper/shadow mode.
   - Shares the same core order intent and risk gate runtime with Live Trader.
   - Its job is to prove execution logic before any real-money route is enabled.
4. `live_trader`
   - Consumes approved artifacts and manages live-readiness, broker capability checks, account/position reconciliation, Watchdog, order gates, and audit logs.
   - It is the final real-money console, so it must be conservative and fail closed.

Important shared packages:

- `packages\design`
  - Shared design tokens and UI primitives.
  - Live Trader imports `../../../packages/design/design_tokens.json` and CSS tokens from `../../../packages/design/design-tokens.css`.
- `packages\trading_runtime`
  - Shared order/risk/strategy runner runtime.
  - Paper Trader and Live Trader both depend on this package for core trading contracts.
- `packages\strategy-core`
  - Shared strategy artifact and plugin area.
  - Live Trader can scan this folder for strategy artifacts unless overridden by env vars.

## Current Product Direction

The user wants a real desktop trading application, not a browser-only mock.

Key expectations:

- Python local server.
- React frontend.
- PyWebView desktop wrapper.
- PyInstaller EXE.
- Backtester/Paper-like dense professional UI.
- Real trading readiness with safety gates before broker orders.

Supported or planned broker routes:

- KIS / Korea Investment Securities Open API for Korean stocks, US stocks, ETFs, gold/oil ETFs.
- Binance and/or Upbit for crypto.

Important product rule:

- Do not build one global "start everything" button.
- Live automation must be route-separated:
  - `stock`: KIS stock/ETF route.
  - `crypto`: Binance or Upbit crypto route.
- Each route supports:
  - `MONITOR`: observe only.
  - `SMALL_LIVE`: small live mode after gates pass.
  - `FULL_LIVE`: full live mode only after stricter gates pass.

## Current UI Structure

Left navigation:

1. `사전점검`
2. `실거래 준비`
3. `자동화`
4. `로그`
5. `API`
6. `설정`

Recently removed or merged:

- The old dashboard became `사전점검`.
- A separate `최종점검` tab was removed because Doctor covers it.
- A standalone `전략` tab was merged into `실거래 준비`.
- A standalone `주문` tab was merged into execution/preparation surfaces.
- Repeated top title/description headers were removed from tabs to reduce wasted vertical space.
- The top KRX/NYSE/Binance/Risk Engine status strip was removed from all tabs.

Current sidebar branding:

- Title: `실시간거래소`
- Subtitle `주문 운영 데스크` was removed.

## Tab Responsibilities

### 사전점검

Main frontend pieces:

- `PreTradeDoctorPanel`
- `buildDoctorItems`
- `Doctor` detail rows in `src\App.jsx`

Purpose:

- One-click preflight surface.
- Runs reconciliation/final preflight.
- Shows compact status cards for:
  - API/broker connection.
  - checklist.
  - risk limits.
  - strategy live permission.
  - reconciliation.
  - Live Watchdog.
  - final preflight.
- Clicking a card shows detailed rows and a related-tab shortcut.

Recent UI requirements:

- The tab must be scrollable so lower detail rows are not clipped.
- Doctor cards are compact: roughly 360px wide and 72px high at desktop width.
- Success/warning/danger cards use light green/yellow/red backgrounds with weak gray borders, not saturated colored outlines.
- Detail rows are capped narrower than the full page when possible, currently around 680px.

### 실거래 준비

Main frontend pieces:

- `LivePreparationPanel`
- `StrategyPanel`
- `LiveStrategySelectorPanel`
- `OperationalSafeguardsPanel`
- `RiskSettingsPanel`
- `RetryPolicyPanel`
- `WatchdogPanel`

Purpose:

- Prepare route-level live trading for `주식/ETF` and `코인`.
- Review strategy artifacts, risk settings, retry policy, operational safeguards, and Watchdog state.

Current internal tabs:

- `주식/ETF`
- `코인`

Current operational safeguards:

- `Dry Run`
- `신규 진입 차단`
- `테스트 주문 게이트`
- inline emergency/kill switch state

Recent UI changes:

- The right-side risk/retry panel stacks below the main content at narrower desktop widths to avoid overlap.
- Global tab title/description headers were removed.
- Strategy artifact panel spacing was improved so select/status/metric/note/parameter boxes no longer look glued together.

### 자동화

Main frontend pieces:

- `AutomationLauncherPanel`
- route automation cards
- provider selector for crypto

Purpose:

- Start/stop route-level automation profiles.
- Show mode controls for each route.

Current automation profiles:

- `stock`
  - provider: `kis`
  - assets: Korean stocks, US stocks, ETFs.
- `crypto`
  - provider: `binance` or `upbit`
  - assets: spot crypto routes.

Important behavior:

- Mode buttons currently update local state and create audit entries.
- They do not yet run a complete long-lived production automation engine.
- Real broker sending remains blocked by backend readiness, broker adapter status, risk gate, strategy permission, and operational flags.

### 로그

Main frontend pieces:

- `AuditPanel`
- `AuditExportPanel`
- `inferLogChannel`

Purpose:

- Compact trading-console log view.
- Shows audit events, filters, search, sort, and export controls.

Current inferred channels:

- `ORDER`
- `API`
- `STRATEGY`
- `RISK`
- `SYSTEM`

Exports:

- CSV
- HTML

Important:

- The audit log is currently app-state/SQLite backed, not yet a true streaming engine log from a production order runner.

### API

Main frontend/backend areas:

- `src\App.jsx`
- `live_trader\brokers.py`
- `live_trader\live_adapters.py`
- `live_trader\env_settings.py`

Purpose:

- Check broker connection readiness.
- Show broker capability matrix.
- Manage or inspect KIS/Binance/Upbit real-account env fields.

Recent UI changes:

- Broker capability cards are compact one-line cards.
- Card title and detail are on the same line.
- Capability card height is about 43px.
- Success/failure/warning backgrounds remain light green/red/yellow with weak gray borders.
- Placeholder/example text in settings/API fields is intentionally pale and slightly smaller so it is not confused with real user input.

Broker env fields:

- KIS:
  - `KIS_APP_KEY`
  - `KIS_APP_SECRET`
  - `KIS_ACCOUNT_NO`
  - `KIS_ACCOUNT_PRODUCT_CODE`
  - optional/used fields include `KIS_BASE_URL`, `KIS_HTS_ID`
- Binance:
  - `BINANCE_API_KEY`
  - `BINANCE_API_SECRET`
- Upbit:
  - `UPBIT_ACCESS_KEY`
  - `UPBIT_SECRET_KEY`

Current broker capability state:

- KIS:
  - OAuth token request implemented.
  - domestic balance/holding snapshot request implemented.
  - cash order request builder implemented.
  - cancel/correction and overseas holdings still need official integration.
- Binance:
  - signed spot order request builder implemented.
  - signed `/api/v3/account` balance snapshot implemented.
  - cancel and user stream still need integration.
- Upbit:
  - JWT order request builder implemented.
  - `/v1/accounts` snapshot implemented.
  - cancel/status still need integration.

Important:

- Never show fake account data as if it were real.
- Missing credentials or unimplemented broker actions should stay visibly missing/blocked.

### 설정

Purpose:

- Theme controls.
- Layout lock/edit/reset controls.
- `.env` connection assistant.

Persistence:

```text
%APPDATA%\LiveTrader\ui-settings.json
```

UI settings endpoints:

- `GET /api/ui-settings`
- `POST /api/ui-settings`

Env workflow:

- `.env` is ignored by git.
- `.env.example` is allowed in git.
- App loads `.env` through `live_trader\env_loader.py`.
- Existing OS environment variables win over `.env`.
- Empty `.env` values are ignored.
- Real secrets should be stored in `.env` or OS env, not committed.

## Backend Structure

Main backend files:

- `live_trader\server.py`
- `live_trader\state.py`
- `live_trader\order_management.py`
- `live_trader\risk_engine.py`
- `live_trader\brokers.py`
- `live_trader\contracts.py`
- `live_trader\live_adapters.py`
- `live_trader\env_loader.py`
- `live_trader\env_settings.py`
- `live_trader\audit_store.py`
- `live_trader\program_ledger.py`
- `live_trader\desktop.py`
- `live_trader\__main__.py`

Server:

- Standard-library `ThreadingHTTPServer`.
- Default host: `127.0.0.1`.
- Default port: `8795`.
- Serves Vite `dist`.
- Exposes JSON API endpoints.

Important endpoints:

- `GET /api/snapshot`
- `GET /api/ui-settings`
- `POST /api/ui-settings`
- `GET /api/env-settings`
- `POST /api/env-settings`
- `POST /api/mode`
- `POST /api/flag`
- `POST /api/automation`
- `POST /api/risk-setting`
- `POST /api/retry-policy`
- `POST /api/order-retry`
- `POST /api/order-cancel`
- `POST /api/broker-check`
- `POST /api/reconcile`
- `POST /api/preflight`
- `POST /api/audit-export`
- `POST /api/test-intent`
- `POST /api/strategy-cycle`
- `POST /api/watchdog`

## Shared Runtime Boundary

Paper Trader and Live Trader must keep using the same core boundary:

```text
strategy signal -> OrderIntent -> PreTradeRiskGate -> adapter or dry-run ledger
```

Implemented shared runtime:

- `packages\trading_runtime\trading_runtime\order_management.py`
- `packages\trading_runtime\trading_runtime\risk_engine.py`
- `packages\trading_runtime\trading_runtime\strategy_runner.py`

Live Trader local wrappers:

- `live_trader\order_management.py`
- `live_trader\risk_engine.py`

Reason for wrappers:

- Preserve local imports while sharing real implementation with Paper Trader.

Current risk gate behavior:

- `live_trader\state.py` converts test/strategy/retry orders into `OrderIntent`.
- It builds `PreTradeContext` from current mode, dry-run, kill switch, readiness, reconciliation, broker readiness, Watchdog, and risk settings.
- It evaluates through `PreTradeRiskGate`.
- Orders keep a serializable `risk_report`.
- Non-dry-run broker transmission remains blocked until explicitly implemented and verified.

Important safety invariant:

- No test order, retry order, strategy-cycle order, or future automation order should bypass `OrderIntent -> PreTradeRiskGate`.

## Strategy Artifacts

Main file:

- `live_trader\contracts.py`

Artifact search order:

1. `LIVE_TRADER_STRATEGY_ARTIFACT_DIR`
2. `TRADER_STRATEGY_ARTIFACT_DIR`
3. `trading-system\packages\strategy-core`
4. `%APPDATA%\trading_programs\strategies`

Plugin search order:

1. `LIVE_TRADER_STRATEGY_PLUGIN_DIR`
2. `TRADER_STRATEGY_PLUGIN_DIR`
3. each artifact folder's `plugins` subfolder

Normalized artifact fields include:

- `strategy_id`
- `name`
- `symbol`
- `asset`
- `plugin`
- `lifecycle_status`
- `final_test_status`
- `score`
- `permissions`
- `verification`
- `backtester_verified`
- `paper_trader_verified`

Live permission logic:

- `permissions.live_allowed === true`
- or top-level `live_allowed === true`

Frontend shows:

- Backtester verification badge.
- Paper Trader verification badge.
- Live permission/read-only/live-blocked badges.

## Live Watchdog

Main implementation:

- `live_trader\state.py`

API:

- `POST /api/watchdog`

Server loop:

- `live_trader\server.py` starts a daemon Watchdog worker while the desktop server runs.

Watchdog checks:

- heartbeat age.
- strategy/market data freshness.
- active broker/API readiness.
- recent order burst count.
- retry queue size.
- blocked order accumulation.
- account/position reconciliation state.

Fail-closed behavior:

- Critical Watchdog conditions force `MONITOR`.
- They turn on `new_entries_blocked`.
- They disable active automation profiles.
- They are appended to `PreTradeRiskReport` so order audit logs show the safety reason.

Current limitation:

- Watchdog is local/state based.
- It does not yet consume broker user streams, exchange websocket heartbeats, or true market-data latency feeds.

## Account And Position Reconciliation

Main files:

- `live_trader\live_adapters.py`
- `live_trader\brokers.py`
- `live_trader\state.py`
- `live_trader\program_ledger.py`

Read-only broker snapshot requests:

- KIS domestic stock balance:
  - `GET /uapi/domestic-stock/v1/trading/inquire-balance`
- Binance Spot account:
  - `GET /api/v3/account`
- Upbit accounts:
  - `GET /v1/accounts`

State storage:

- `STATE["broker_reconciliation"]`
  - `accounts`
  - `positions`
  - `errors`
  - `fetched_at`

Behavior:

- `run_reconciliation()` refreshes broker snapshots first.
- It then compares broker-side rows with the program ledger.
- Unknown broker positions are surfaced as `불일치`.
- Broker cash with missing program cash ledger remains blocked as `원장 필요`.
- UI shows a `조회 오류` metric.

Current limits:

- KIS overseas stock/ETF reconciliation is not fully wired.
- Binance/Upbit order status and cancel/user-stream reconciliation are not done.
- Program cash ledger persistence is started but not yet enough to make all cash rows fully pass.

## Real Trading Safety State

The app is intentionally conservative.

Real order submission must remain blocked unless all relevant conditions pass:

- `LIVE_TRADER_ENABLE_REAL_ORDERS=true`.
- broker env credentials exist.
- signed broker adapter is implemented and verified.
- strategy artifact has `live_allowed=true`.
- operational checklist is complete.
- reconciliation blockers are resolved.
- kill switch is off.
- route mode gates pass.
- Watchdog has no critical items.
- `FULL_LIVE` has zero warnings.

Known state flags:

- `dry_run`
- `new_entries_blocked`
- `kill_switch`
- `operator_confirmed`
- `mode`
- `watchdog`

Important terms:

- `Dry Run`: generate and audit intent, do not send to broker.
- `신규 진입 차단`: block new entry/buy orders.
- `Kill Switch`: hard stop.

## Desktop / EXE Build

Build command:

```powershell
.\build_exe.ps1
```

Output:

```text
release\LiveTrader.exe
```

Build script responsibilities:

- install/check Python desktop requirements.
- run `npm run build`.
- create/update icon files.
- run PyInstaller.
- include shared `packages\trading_runtime`.

User preference:

- When code/UI/runtime changes are made, always rebuild the EXE.
- The user explicitly said: "exe파일은 항상 만들어줘."
- Documentation-only changes do not alter the EXE, but future implementation turns should include EXE generation before final.

Recent known EXE state:

- `release\LiveTrader.exe` was successfully rebuilt after the compact status-card UI changes.
- PyInstaller warnings about Android/webview or pycparser hidden imports were non-blocking.

## Testing And Validation

Preferred validation commands:

```powershell
npm run build
.\.venv\Scripts\python.exe -m unittest discover -s tests
.\build_exe.ps1
```

Use `unittest`, not pytest, unless pytest is intentionally added.

Recent browser checks from the compact UI pass:

- API capability cards render at about 43px high.
- Capability cards use flex and keep title/detail on one line.
- Doctor cards render at about 360px x 72px.
- Doctor/detail/Watchdog status rows use light state backgrounds with weak gray borders.
- 1280px viewport had no horizontal document overflow.

## Git Workflow

Repository:

```text
https://github.com/youwon35/live_trader.git
```

Branch:

```text
develop
```

Rules:

- Commit and push changes to `develop`.
- Commit/push inside the `live_trader` repo, not the parent `trading-system` repo.
- Do not include ignored generated artifacts unless the repo already tracks them.
- `logs/`, `.env`, `.venv`, `dist`, `build`, `release`, and cache folders are local/ignored or should stay out of ordinary commits.

Current note:

- A local untracked `logs/` folder may appear after running the app. It is runtime data and should not be staged unless the user explicitly asks.

## User-Specific Record-Keeping

When important work is done, append a readable summary to:

```text
F:\동기화용 파일\인쇄용\live_trader_print.py
```

Also append the same general-document summary to the Notion page for folder name `live_trader`.

Known Notion page:

```text
3818d558-4385-8048-8fe4-d5c2c9695fce
```

The page already contains multiple `요약` toggles. Continue appending in the same style unless the user asks to restructure it.

## Current Known Gaps

Critical before real money:

- No complete continuous automation engine yet.
- KIS overseas stock/ETF account and position reconciliation still needs official API wiring.
- Binance/Upbit order status, cancel, and user-stream reconciliation are incomplete.
- Program-side cash ledger must be completed before cash reconciliation can fully pass.
- Real broker send layer must be tested with fixtures, sandbox/paper equivalents, and tiny live-order procedures before enabling.
- Strategy plugin execution needs production-grade market data, scheduling, and stricter artifact lifecycle gates.
- Audit logs need persistent production-grade retention and streaming behavior.

UI gaps to keep watching:

- All pages must remain scrollable.
- Narrow desktop widths must avoid panel overlap.
- Placeholder text must remain visibly different from actual user-entered values.
- Colored status cards should use soft backgrounds and weak gray borders.
- Avoid duplicate explanatory panels and oversized cards.
- Keep layout dense and professional rather than landing-page-like.

## Selected Roadmap

User-selected roadmap from earlier work:

1. Strengthen real-time order/risk audit logs.
   - implemented foundation: risk report is included on submitted/blocked/retried order events.
2. Share the strategy plugin runner boundary.
   - implemented foundation: shared `StrategyExecutionRunner`, Paper integration, Live `/api/strategy-cycle`.
3. Implement a Live Trader Watchdog.
   - implemented foundation: local Watchdog state, `/api/watchdog`, background loop, UI, risk-report integration.
4. Implement real account/position reconciliation.
   - partially implemented: KIS domestic balance, Binance account, Upbit account snapshots, normalized rows, UI error summary.

Next best engineering work:

1. Expand reconciliation to KIS overseas, Binance/Upbit order status, and user streams.
2. Finish program cash ledger persistence and connect it to reconciliation.
3. Add durable order/audit storage and export paths.
4. Only after that, implement the real broker send layer with explicit approval and tests.

## Files Most Likely To Touch Next

Frontend:

- `src\App.jsx`
- `src\styles.css`
- `src\api.js`

Backend:

- `live_trader\state.py`
- `live_trader\server.py`
- `live_trader\brokers.py`
- `live_trader\live_adapters.py`
- `live_trader\contracts.py`
- `live_trader\env_settings.py`
- `live_trader\program_ledger.py`
- `live_trader\audit_store.py`

Tests:

- `tests\test_live_adapters.py`
- `tests\test_order_gate.py`
- `tests\test_contracts.py`
- `tests\test_state_memory.py`

Build:

- `build_exe.ps1`
- `tools\create_icon.py`

## Mental Model For The Next Chat

Think of Live Trader as three layers:

1. Preparation layer:
   - strategy artifacts, broker/API readiness, env setup, risk settings, retry policy.
2. Automation layer:
   - stock/ETF and crypto route profiles with monitor/small/full modes.
3. Safety/audit layer:
   - Doctor, Watchdog, reconciliation, order risk gate, audit log, exports.

The most important product truth:

Live Trader should feel like a real trading desk, but real money must remain blocked until every broker route, account snapshot, strategy artifact, order adapter, risk gate, Watchdog, and audit trail is verified end to end.
