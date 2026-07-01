# Live Trader Handoff

Last updated: 2026-07-02 KST
Project path: `D:\github\PROGRAM\trading-system\apps\live_trader`
Branch: `develop`
Latest pushed commit before this handoff: `69a1316654f40cc7137c2e27806e2315174d5549`
Remote: `https://github.com/youwon35/live_trader.git`

## One Sentence Summary

`live_trader` is the real-money execution console for the `trading-system` workspace, but it is still intentionally gated: it can prepare, inspect, simulate, route, and build official broker API requests, while actual real-money order sending must remain blocked until credentials, adapter verification, account/position APIs, operator approval, and live strategy permissions are fully satisfied.

## Product Direction

The user wants the app to behave like a real desktop trading application:

- Python local server
- React frontend
- PyWebView desktop wrapper
- PyInstaller EXE
- Backtester-like UI design
- Real-money workflow, not a mock trading toy

The app should eventually trade:

- Korean stocks, US stocks, gold ETF, oil ETF through Korea Investment Securities Open API.
- Crypto through Binance and/or Upbit.

Important architectural decision:

- Do not use one global "start everything" button.
- Split real automation by asset route:
  - Stock/ETF automation: KIS route.
  - Crypto automation: Binance or Upbit route.
- Each route has its own mode:
  - `MONITOR`: observe only.
  - `SMALL_LIVE`: small live mode after gates pass.
  - `FULL_LIVE`: full live mode only after stricter gates pass.

## Relationship To Other Apps

The surrounding workspace is `D:\github\PROGRAM\trading-system`.

Expected workflow:

1. `stock_data_scraper`: collects and prepares market data.
2. `backtester`: researches strategies and exports strategy artifacts.
3. `paper_trader`: validates strategies in paper/shadow mode.
4. `live_trader`: consumes approved artifacts and manages real-money execution readiness.

Design references:

- Main UI should continue following `apps\backtester`.
- Layout editing behavior should continue moving toward `apps\stock_data_scraper`.
- Shared design assets live under `packages\design`.
- React imports `../../../packages/design/design_tokens.json`.
- CSS imports `../../../packages/design/design-tokens.css`.

## Current Navigation

Current left navigation order:

1. `사전점검`
2. `실거래 준비`
3. `자동화`
4. `로그`
5. `API`
6. `설정`

Removed/merged concepts:

- Old `대시보드` was renamed/reworked into `사전점검`.
- `최종점검` was removed because Doctor covers that role.
- `전략` standalone tab was removed; strategy artifact checks now belong under `실거래 준비`.
- `주문` standalone tab was merged into execution/preparation flows.
- `Live Readiness`, `운용 요약`, and broad duplicate check panels were removed from most tabs.

## Current UI Responsibilities

### 사전점검

File area: `src\App.jsx`, `PreTradeDoctorPanel`, `buildDoctorItems`.

Purpose:

- Acts like a Doctor / quick tester.
- User clicks `점검 실행`.
- It runs reconciliation and final preflight.
- It shows compact cards for API/broker, checklist, risk, strategy permission, reconciliation, and preflight.
- Clicking a card shows detail rows and has a button to open the related tab.

Known UI requirement:

- This page must remain scrollable so lower detail rows are visible.
- Top global status pills were intentionally removed from several pages where they duplicated Doctor.

### 실거래 준비

File area: `src\App.jsx`, `LivePreparationPanel`.

Purpose:

- Prepare each asset route before automation.
- Has internal tabs: `주식/ETF`, `코인`.
- Contains route-specific strategy artifacts, risk settings, retry policy, and operational safeguards.

Recently removed from this tab:

- Preparation summary cards such as current mode / broker / live strategy count.
- `주문 큐 요약`.
- `주문 기록`.
- `운용자 확인` button.

Current main panels:

- `StrategyPanel`
- `OperationalSafeguardsPanel`
- `RiskSettingsPanel`
- `RetryPolicyPanel`

Operational safeguards currently include:

- `Dry Run`
- `신규 진입 차단`
- `테스트 주문 게이트`
- inline kill switch status

### 자동화

File area: `src\App.jsx`, `AutomationLauncherPanel`.

Purpose:

- Start/stop route-level automation.
- Has internal tabs: `주식/ETF`, `코인`.
- Each tab shows a route-specific automation card.
- Crypto tab can switch provider between Binance and Upbit.
- Each automation card has mode buttons: `MONITOR`, `SMALL LIVE`, `FULL LIVE`.

Recently changed:

- Old explanatory `자동거래 흐름` panel was removed.
- The right side now shows `주문 큐 요약` and `주문 기록`.
- `Order Blotter` text was renamed to Korean `주문 기록`.

Important behavior:

- Automation mode buttons currently update app state and create audit entries.
- They do not yet run a continuous production trading engine.
- Real broker sending remains gated by backend readiness and adapter status.

### 로그

File area: `src\App.jsx`, `AuditPanel`, `AuditExportPanel`.

Purpose:

- Show execution/audit logs in a compact log-console style similar to backtester.

Recently changed:

- Removed right-side `운용 리포트` and risk panels.
- Removed large export card.
- Export is now a small action panel with CSV/HTML buttons.
- Log table now has search input, channel filter, level filter, sort dropdown, and dense rows.

Current log channels are inferred in frontend by `inferLogChannel`:

- `ORDER`
- `API`
- `STRATEGY`
- `RISK`
- `SYSTEM`

### API

File area:

- `src\App.jsx`
- `live_trader\brokers.py`
- `live_trader\live_adapters.py`

Purpose:

- Manage/check broker API readiness and adapter capabilities.
- Shows KIS, Binance, and Upbit readiness.

Current broker specs:

- KIS:
  - env: `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`, `KIS_ACCOUNT_PRODUCT_CODE`
  - order request building implemented
  - auth token request implemented
  - account/positions/cancel still need real API integration
- Binance:
  - env: `BINANCE_API_KEY`, `BINANCE_API_SECRET`
  - signed spot order request building implemented
  - balances/positions/cancel/user stream still need integration
- Upbit:
  - env: `UPBIT_ACCESS_KEY`, `UPBIT_SECRET_KEY`
  - JWT order request building implemented
  - balances/positions/cancel still need integration

Important:

- The app should not display fake real account data.
- If an API is not implemented or credentials are missing, show that it is required/missing.

### 설정

Purpose:

- Theme and layout controls.
- User wanted white mode to persist after app restart.
- UI settings are persisted by the Python server.

Persistence file:

```text
%APPDATA%\LiveTrader\ui-settings.json
```

API endpoints:

- `GET /api/ui-settings`
- `POST /api/ui-settings`

## Current Backend Structure

Main backend files:

- `live_trader\server.py`
- `live_trader\state.py`
- `live_trader\order_management.py`: compatibility wrapper; actual `OrderIntent` comes from shared `packages\trading_runtime`.
- `live_trader\risk_engine.py`: compatibility wrapper; actual `PreTradeContext`, `PreTradeRiskGate`, and report classes come from shared `packages\trading_runtime`.
- `live_trader\brokers.py`
- `live_trader\contracts.py`
- `live_trader\live_adapters.py`
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

### Order Intent And Risk Gate

As of 2026-07-02, live order test/retry paths no longer use only a simple readiness/dry-run check.

- Paper Trader and Live Trader share the real order/risk implementation in `trading-system\packages\trading_runtime`.
- `live_trader\order_management.py` and `live_trader\risk_engine.py` are local compatibility wrappers so existing imports remain stable.
- `live_trader\state.py` converts a strategy/test order into `OrderIntent`, builds `PreTradeContext` from current mode, dry-run, kill switch, readiness, reconciliation, broker readiness, and risk settings, then evaluates the intent through `PreTradeRiskGate`.
- Orders now keep a `risk_report` payload so the UI/log layer can later explain exactly which checks passed, warned, or blocked the order.
- Non-dry-run broker transmission still remains blocked until the real send layer and adapter verification are intentionally enabled.

## Strategy Artifacts

Main file: `live_trader\contracts.py`.

Artifact search order:

1. `LIVE_TRADER_STRATEGY_ARTIFACT_DIR`
2. `TRADER_STRATEGY_ARTIFACT_DIR`
3. `trading-system\packages\strategy-core`
4. `%APPDATA%\trading_programs\strategies`

Strategy plugin folders:

1. `LIVE_TRADER_STRATEGY_PLUGIN_DIR`
2. `TRADER_STRATEGY_PLUGIN_DIR`
3. each artifact folder's `plugins` subfolder

Current artifact normalization produces:

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

Verification badges:

- Backtester badge
- Paper Trader badge
- Live permission badge

Live permission logic:

- `permissions.live_allowed === true`
- or top-level `live_allowed === true`

The UI shows strategy rows with Backtester/Paper verification pills and live permission.

## Real Trading Safety State

The app is intentionally conservative.

Real order submission must remain blocked unless:

- `LIVE_TRADER_ENABLE_REAL_ORDERS=true`
- broker env credentials exist
- signed order adapter is enabled/verified
- strategy artifact has `live_allowed=true`
- operational checklist is done
- reconciliation blockers are resolved
- kill switch is off
- required mode gates pass
- `FULL_LIVE` has zero warnings

Known safety flags in state:

- `dry_run`
- `new_entries_blocked`
- `kill_switch`
- `operator_confirmed`
- `mode`

Important nuance:

- `Dry Run` means generated order intents must not be sent to broker.
- `신규 진입 차단` blocks new entry/buy orders.
- `Kill Switch` is a hard stop.

## Automation Model

Main file: `live_trader\state.py`, `automation_profiles`.

Current profiles:

- `stock`
  - title: `주식/ETF 자동화`
  - provider: `kis`
  - assets: Korean stocks, US stocks, gold ETF, oil ETF
- `crypto`
  - title: `코인 자동화`
  - provider: `binance` or `upbit`
  - assets: Binance spot, Upbit KRW market

Important:

- `MONITOR`, `SMALL_LIVE`, and `FULL_LIVE` are route-level modes.
- User wants stock/ETF and crypto separated because capital and broker accounts are separate.
- The UI now reflects this separation.
- The actual long-running automation engine is still a future implementation step.

## EXE / Desktop Build

Build script:

```powershell
.\build_exe.ps1
```

Output:

```text
release\LiveTrader.exe
```

The build script:

- installs Python desktop requirements
- runs `npm run build`
- creates/updates app icon files
- runs PyInstaller
- adds `packages\trading_runtime` to the PyInstaller search path and hidden imports so the EXE uses the shared order/risk engine.

Notes from recent builds:

- Build succeeded after the latest UI changes.
- PyInstaller may warn about Android/webview or pycparser hidden imports; these were non-blocking in recent runs.
- Current output EXE was rebuilt on 2026-07-02 after removing the risk-gate settings toggle and returning to an always-on shared risk gate.

User preference:

- When code changes, rebuild the EXE too.
- Documentation-only changes do not necessarily require EXE rebuild unless code/runtime behavior changed.

## Testing

Recent verification:

```powershell
npm run build
.\.venv\Scripts\python.exe -m unittest discover -s tests
.\build_exe.ps1
```

Results:

- `npm run build`: passed.
- `python -m unittest discover -s tests`: passed.
- `pytest`: not installed in the current venv, so use `unittest` unless adding pytest intentionally.
- EXE build: passed.

## Git / Workflow Rules

Repository:

```text
https://github.com/youwon35/live_trader.git
```

Branch:

```text
develop
```

Recent commit:

```text
69a1316 Refine automation and log workspace
```

User wants changes committed and pushed to `develop`.

Important:

- Commit/push in the `live_trader` repo, not the parent `trading-system` repo.
- The working tree was clean before this handoff rewrite.
- After modifying `handoff.md`, commit and push the doc update.

## User-Specific Logging Requirement

The user asked that important work summaries be appended to:

```text
F:\동기화용 파일\인쇄용\live_trader_print.py
```

They also asked that summaries be appended to the Notion project page under a `요약` toggle.

Known Notion page used previously:

```text
3818d558-4385-8048-8fe4-d5c2c9695fce
```

Use the Notion update tool when available.

## Current Known Gaps

Critical gaps before real money should be enabled:

- No complete continuous automation engine yet.
- KIS account/position reconciliation APIs still need real implementation.
- Binance account/balance/user stream/cancel APIs still need real implementation.
- Upbit account/balance/cancel APIs still need real implementation.
- Real broker submission must be audited with sandbox or tiny live orders before enabling.
- Strategy plugin execution pipeline from Backtester artifacts to live signals still needs production-grade implementation.
- Shared risk engine now runs for live test/retry order intents, but the future continuous automation engine must use the same `OrderIntent -> PreTradeRiskGate -> adapter` boundary before any broker transmission.
- Logs are currently derived from audit events, not yet a true streaming engine log source.

UI gaps to watch:

- Keep all pages scrollable at desktop and narrow widths.
- Avoid duplicate panels across tabs.
- Keep tabs and buttons aligned vertically centered.
- Keep white mode text high-contrast.
- Keep selected nav using user accent color where applicable.

## Best Next Engineering Steps

1. Implement real strategy plugin runner boundary:
   - load approved Backtester/Paper artifact
   - normalize signal into `OrderIntent`
   - route to KIS/Binance/Upbit adapter
   - always pass through risk gate before broker call
2. Add real account/position read APIs:
   - KIS balance/positions
   - Binance account/balances
   - Upbit accounts
3. Add order adapter send layer behind explicit safety flag:
   - request preview first
   - signed request second
   - tiny live test only after operator approval
4. Add persistent order/audit storage:
   - SQLite or local append-only JSONL
   - exportable logs
5. Add Playwright/Electron-style UI smoke sweep:
   - each tab renders
   - no blank screen
   - no major overlap
   - white/dark mode readable

## Files Most Likely To Touch Next

Frontend:

- `src\App.jsx`
- `src\styles.css`
- `src\api.js`

Backend:

- `live_trader\state.py`
- `live_trader\order_management.py`
- `live_trader\risk_engine.py`
- `live_trader\brokers.py`
- `live_trader\live_adapters.py`
- `live_trader\contracts.py`
- `live_trader\server.py`

Tests:

- `tests\test_contracts.py`
- add adapter/risk/order tests as real integration grows

Build:

- `build_exe.ps1`
- `tools\create_icon.py`

## Mental Model For The Next Chat

Think of `live_trader` as three layers:

1. Preparation layer:
   - strategies, risk settings, retry policy, broker/API readiness
2. Automation layer:
   - stock/ETF route and crypto route, each with monitor/small/full modes
3. Safety/audit layer:
   - Doctor, order queue, order record, logs, exports

The most important product truth:

The UI is becoming a real trading desk, but real money must remain blocked until each broker route has verified official API implementation and the strategy-to-order pipeline is auditable end to end.
