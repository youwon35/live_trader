# Live Trader Handoff

Last updated: 2026-06-22 KST
Project path: `D:\github\PROGRAM\trading-system\apps\live_trader`
Branch: `develop`
Latest pushed commit before this handoff: `0631639b71eeafd8f181e4ee76775924c5e48196`
Remote: `https://github.com/youwon35/live_trader.git`

## What This App Is

`live_trader` is the real-money trading console in the larger `trading-system` workspace. It is intended to be the final execution layer after data collection, backtesting, and paper/shadow validation.

The app is intentionally conservative. It currently blocks real order flow unless all required live-trading conditions are satisfied:

- `LIVE_TRADER_ENABLE_REAL_ORDERS=true`
- Required broker credentials exist in environment variables.
- Broker signed order adapters are implemented and ready.
- Strategy artifacts have live permission, such as `permissions.live_allowed === true` or `live_allowed === true`.
- Required runbook checklist items are confirmed.
- Position/account reconciliation has no API-required or mismatch blockers.
- Kill Switch is off.
- Mode transition gate allows the requested mode.
- `FULL_LIVE` requires zero readiness warnings.

Important: as of the latest work, KIS/Binance real signed order placement is still not implemented. The UI and Python state layer expose readiness, diagnostics, audit, order-gate simulation, retry/cancel queue behavior, and final preflight checks, but actual broker order submission remains blocked until provider-specific signed order adapters and account/position APIs are added.

## Relationship To Other Workspace Apps

The app was built to fit into the broader `trading-system` structure:

- `stock_data_scraper`: market data collection/preparation.
- `backtester`: strategy research, optimization, and artifact export.
- `paper_trader`: shadow/paper validation and approval workflow.
- `live_trader`: final real-money execution console.

Design direction:

- The user specifically asked that `live_trader` visually follow `D:\github\PROGRAM\trading-system\apps\backtester`.
- Shared design tokens are imported from `../../../packages/design/design-tokens.css`.
- React UI also imports `../../../packages/design/design_tokens.json` for accent palette data.

Strategy compatibility:

- Strategy artifacts are loaded via `live_trader/contracts.py`.
- Search order is documented in `README.md`: `F:\stock_market_data\strategies` first, then `%APPDATA%\trading_programs\strategies`.
- Permission logic mirrors shared trading-contract expectations.

## Tech Stack

Frontend:

- React 19
- Vite 7
- Lucide React icons
- Main files:
  - `src/App.jsx`
  - `src/styles.css`
  - `src/api.js`
  - `src/main.jsx`

Backend/Desktop:

- Python standard-library HTTP server, no FastAPI.
- PyWebView for desktop window.
- PyInstaller one-file EXE build.
- Main files:
  - `live_trader/server.py`
  - `live_trader/state.py`
  - `live_trader/brokers.py`
  - `live_trader/contracts.py`
  - `live_trader/desktop.py`
  - `live_trader/__main__.py`

Build/package:

- `build_exe.ps1`
- `tools/create_icon.py`
- `assets/app-icon.svg`
- `assets/app-icon.png`
- `assets/app-icon.ico`
- Output EXE: `release\LiveTrader.exe`

## Commands

Install/build frontend:

```powershell
npm install
npm run build
```

Compile Python:

```powershell
.\.venv\Scripts\python.exe -m compileall live_trader
```

Run local Python server only:

```powershell
.\.venv\Scripts\python.exe -m live_trader.server --host 127.0.0.1 --port 8797
```

Run desktop app:

```powershell
.\.venv\Scripts\python.exe -m live_trader
```

Build EXE:

```powershell
.\build_exe.ps1
```

EXE output:

```text
release\LiveTrader.exe
```

Recent EXE validation:

- `build_exe.ps1` succeeded on 2026-06-22.
- `release\LiveTrader.exe` was launched for 8 seconds.
- Process stayed alive and was then stopped.
- Latest observed size: about 16.18 MB.

## Current UI Structure

The app has seven primary tabs:

- `Overview`
- `Live Gate`
- `Orders`
- `Brokers`
- `Strategies`
- `Audit`
- `Preflight`

Each page is wrapped by `PageView` and has a consistent:

- page heading
- status pills
- section tab row
- scrollable content area

Important UI capabilities:

- Dark/white theme toggle
- Accent color selector
- Backtester-aligned shell, tabs, buttons, dense panels
- Layout edit mode
- Panel drag/resize in layout edit mode
- Layout reset in Appearance/Layout panel
- Search results panel
- Notification panel
- Kill Switch
- Audit CSV/HTML export

## Recent Completed Work

### Commit `aa25e1b`

Aligned the app shell with the backtester design:

- sidebar/topbar rework
- denser panel style
- tab-like controls
- backtester-style spacing and typography
- white mode readability improvements

### Commit `b1e0a29`

Created and applied a custom app icon:

- generated via `tools/create_icon.py`
- stored in `assets/app-icon.*`
- applied during PyInstaller build

### Commit `f25b3a2`

Added reconciliation and final preflight:

- position/account reconciliation summary
- account and position panels
- final preflight hard stop/warning model
- launch report

### Commit `2514826`

Added page-level tabs and backtester-like workspace structure:

- `pageProfiles`
- `PageView`
- section tab row
- mobile topbar fixes
- desktop/mobile Playwright verification

### Commit `0631639`

Added final QA fixes after a full tab-by-tab run:

- Search input now has real results.
- Search covers strategies, orders, brokers, readiness, positions, accounts, final preflight, and audit.
- Search result click navigates to the relevant tab.
- Notification button now opens a live notification panel.
- Notifications cover API error, Kill Switch, blockers, warnings, broker readiness, retryable orders, reconciliation, and final preflight issues.
- Notification row click navigates to the related tab.
- Notification panel closes on Escape and outside click.
- Mobile search/notification layout was reinforced.
- Status pill central alignment CSS was cleaned up.

## Latest Full QA Result

Date: 2026-06-22 KST
Automation: Chrome + temporary Playwright install in `%TEMP%\live_trader_qa_playwright`
Screenshots: `C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621`

Note: Browser plugin and Node REPL routes were not usable in that environment. Playwright was installed into a temporary npm prefix and was not added to repo dependencies.

Final QA result: all checks passed.

Validated:

- app shell visible
- 7 sidebar nav items
- 7 section tabs
- desktop no horizontal overflow at 1440x980
- topbar refresh button
- search panel with results
- notification panel with rows
- white mode toggle and dark restore
- layout edit toggle
- Kill Switch on/off
- Live Gate navigation
- operator confirmation toggle
- checklist checkbox toggle
- risk setting input commit
- Orders navigation
- test order gate creates order row
- order retry button
- order cancel button
- retry policy input commit
- Brokers navigation
- broker check buttons
- Strategies table rows
- Audit CSV download
- Audit HTML download
- Preflight navigation
- reconciliation run
- final preflight run
- preflight status rows visible
- mobile no horizontal overflow at 390x900
- status pills center-aligned
- no console errors

Key screenshot files:

```text
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\01_overview_dark.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\02_search_results.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\03_notifications.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\04_overview_light.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\05_layout_edit.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\06_gate.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\07_orders_after_actions.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\08_brokers_after_checks.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\09_strategies.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\10_audit_after_exports.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\11_preflight_after_checks.png
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\12_mobile_overview.png
```

Latest audit downloads from QA:

```text
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\live-trader-audit-20260622-033132.csv
C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621\live-trader-audit-20260622-033132.html
```

## Python API Endpoints

Defined in `live_trader/server.py`.

GET:

- `/api/snapshot`

POST:

- `/api/mode`
- `/api/flag`
- `/api/risk-setting`
- `/api/checklist`
- `/api/retry-policy`
- `/api/order-retry`
- `/api/order-cancel`
- `/api/broker-check`
- `/api/reconcile`
- `/api/preflight`
- `/api/audit-export`
- `/api/test-intent`

The frontend API wrapper is `src/api.js`.

## Runtime State Model

Main state lives in `live_trader/state.py` as in-memory `STATE`.

Important state fields:

- `mode`
- `dry_run`
- `kill_switch`
- `new_entries_blocked`
- `operator_confirmed`
- `risk_settings`
- `checklist`
- `retry_policy`
- `reconciliation_last_run`
- `preflight_last_run`
- `orders`
- `audit`

Important functions:

- `snapshot()`
- `set_mode()`
- `set_flag()`
- `set_risk_setting()`
- `set_checklist_item()`
- `set_retry_policy()`
- `run_broker_check()`
- `run_reconciliation()`
- `run_final_preflight()`
- `export_audit()`
- `submit_test_intent()`
- `retry_order()`
- `cancel_order()`

## Safety Semantics

Mode rules:

- `MONITOR` is always the safe fallback.
- `SMALL_LIVE` and `FULL_LIVE` are blocked if readiness blockers exist.
- `FULL_LIVE` is additionally blocked if warnings exist.
- Turning on Kill Switch forces `mode = MONITOR` and `new_entries_blocked = true`.

Order test behavior:

- `submit_test_intent()` creates an order queue row.
- If `dry_run` is true, order state can be `dry_run`/simulated depending on gate conditions.
- If new entries are blocked and the test side is BUY, it is blocked.
- If readiness blockers exist, it is blocked.
- If `dry_run` is off but real adapter is not ready, it is held/blocked.
- No actual broker order is sent by current code.

Retry/cancel behavior:

- Retry uses retry policy settings.
- Canceled orders move to `state = canceled` and `queue_state = canceled`.
- Retry exhaustion can move to `retry_exhausted`.

## Broker/API Status

`live_trader/brokers.py` checks environment state.

Expected environment variables are documented in `README.md`:

```text
LIVE_TRADER_ENABLE_REAL_ORDERS=false
LIVE_TRADER_REQUIRE_OPERATOR_CONFIRMATION=true
KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=
KIS_ACCOUNT_PRODUCT_CODE=
BINANCE_API_KEY=
BINANCE_API_SECRET=
```

Important caveat:

- The code may refer to KIS env names in broker diagnostics/readiness. Confirm exact names in `live_trader/brokers.py` before wiring real credentials.
- Do not commit real secrets.
- Real broker integration should use environment variables or a local secret manager, never hardcoded keys.

## Files To Know

Frontend:

- `src/App.jsx`: most UI, layout, search, notifications, panels, actions.
- `src/styles.css`: all layout/design/theme/mobile styling.
- `src/api.js`: endpoint wrappers.

Python:

- `live_trader/server.py`: HTTP API + static frontend serving.
- `live_trader/state.py`: state, readiness, risk, audit, order queue, preflight, reconciliation.
- `live_trader/brokers.py`: broker readiness and adapter diagnostics.
- `live_trader/contracts.py`: strategy artifact loading and live permission checks.
- `live_trader/desktop.py`: server thread + WebView/browser launch.
- `live_trader/__main__.py`: desktop entry point for Python/PyInstaller.

Build/assets:

- `build_exe.ps1`: installs desktop deps, runs frontend build, creates icon, runs PyInstaller.
- `tools/create_icon.py`: generates `assets/app-icon.png` and `.ico`.
- `assets/app-icon.svg`: source icon.
- `release/LiveTrader.exe`: distributable executable.

QA:

- `qa/`: older manually saved QA screenshots.
- Temporary automated QA artifacts: `C:\Users\youwo\AppData\Local\Temp\live_trader_qa_20260621`.

## User Standing Instructions

The user expects:

- When code changes are made, also build the EXE.
- After code changes, commit and push to `develop`.
- Important work summaries should be appended to `F:\동기화용 파일\인쇄용\live_trader_print.py`.
- The same summary should be appended in Notion under the project page's `요약` toggle.
- Current Notion page used for this project: `3818d558-4385-8048-8fe4-d5c2c9695fce`.

Previous summaries have already been appended to the print file and Notion for recent UI/QA work.

## Known Tooling Notes

- Use `rg` for searching.
- Use `apply_patch` for manual file edits.
- This environment may restrict writes outside the repo; use escalation when writing to `F:\동기화용 파일\인쇄용`.
- Network can be restricted. The temporary Playwright install required escalation previously.
- Browser plugin was not available in the last QA run.
- Node REPL failed with `codex/sandbox-state-meta: missing field sandboxPolicy` in the last QA run.
- Direct Chrome CDP page sessions were unreliable; temporary Playwright was the successful route.

## Next Good Steps

High priority:

1. Implement real KIS signed order adapter behind current safety gates.
2. Implement real Binance signed order adapter behind current safety gates.
3. Implement account cash and position fetch APIs for KIS/Binance so reconciliation can pass.
4. Add `.env` loading or clear Windows environment setup docs if not already handled.
5. Add persistent local state or controlled session reset rules. Current `STATE` is in-memory.
6. Add automated regression tests for `state.py` safety gates.
7. Add frontend test harness for search/notification/Kill Switch/order queue flows.

Medium priority:

1. Add broker connection latency and error code diagnostics.
2. Add audit log persistence to local file or SQLite.
3. Add signed order dry-run preview showing exact broker payload without secrets.
4. Add explicit "real orders unavailable because..." explanation panel.
5. Add settings import/export.
6. Add session market calendar integration instead of static market strip.

Before any real trading:

- Confirm API credentials are in environment only.
- Confirm order adapter code is provider-specific and signed correctly.
- Add small-live guardrails: max notional, allowed symbols, trading hours, duplicate order lock, manual approval, and emergency stop.
- Verify with sandbox/paper endpoints where available.
- Run an end-to-end dry-run with audit export.
- Run a small-live checklist with user confirmation.

## Final Known Good Verification

Commands that passed most recently:

```powershell
npm run build
.\.venv\Scripts\python.exe -m compileall live_trader
.\build_exe.ps1
```

EXE run check:

```powershell
$p = Start-Process -FilePath .\release\LiveTrader.exe -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 8
$alive = -not $p.HasExited
if ($alive) { Stop-Process -Id $p.Id -Force }
```

Observed:

```text
EXE_PID:43884 ALIVE:True
```

## Current Risk Summary

The UI is now usable and verified across all tabs, but the app is not yet a complete real-money trading system. It is a real-trading control console with strong blocking semantics and readiness visibility. The missing pieces are the actual broker-side signed order adapters and real reconciliation APIs.

Treat any request to enable real orders as a high-risk change. Implement adapter code and tests first, then verify with explicit user approval.
