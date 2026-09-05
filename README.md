# Live Trader

Real-money trading console for the `trading-system` workspace.

`live_trader` is the final execution layer after:

- `stock_data_scraper`: market data collection and preparation
- `backtester`: strategy research, optimization, and artifact export
- `paper_trader`: shadow/paper validation, approval workflow, and operational safety checks

This app intentionally blocks live orders until an immutable Deployment, a fresh Preflight Snapshot, broker/account reconciliation, live strategy permissions, risk checks, and broker order adapters are ready. Enabling the real-order environment flag never submits an order by itself.

The desktop workspace has eight top-level tabs:

- 시작 점검
- 운용 전략
- 실거래 운용 (실행·중지 / 한도·안전장치)
- 계좌·잔고
- 주문·체결
- 실행 기록 (운영 이력 / 상세 로그)
- 연결·설정
- 주문 연결 시험

See [the architecture review](docs/architecture-review-20260906.md) for the current implementation limits and [the manual tab guide](docs/manual-tab-validation.md) for each tab's purpose, internal behavior, steps, and acceptance criteria.

As of 2026-09-06, ordinary continuous execution supports observation but its final live dispatch remains blocked by `CONTINUOUS_FINAL_DISPATCH_LOCK_ORDER_UNAVAILABLE`. The UI reads this capability from the server; missing capability data is unverified. The separate supervised crypto release also remains closed. Paper candidate evidence is verified read-only; first-time Live adoption and initial limited-live approval are not connected yet. Adapter code and completed validation stages do not establish that a production route is available.

## Run The Desktop App

```powershell
npm install
npm run build
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-desktop.txt
.\.venv\Scripts\python.exe -m live_trader
```

The Python process starts a local HTTP API and opens a WebView window when `pywebview` is installed. If WebView is unavailable, it opens the local URL in the default browser.

## Build EXE

```powershell
.\build_exe.ps1
```

Output:

```text
release\LiveTrader.exe
```

## Required Broker/API Settings

Create `.env` next to this README for non-secret runtime flags. Do not commit real credentials.
The desktop app loads this file automatically on startup, without overriding OS-level environment variables.
Broker keys entered through the settings screen are stored in the app's protected secret store. On Windows this uses user-scoped DPAPI encryption; legacy plaintext secret values are migrated out of `.env` and the UI never returns them after saving.

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

Current implementation includes signed KIS/Binance/Upbit order adapters and private KIS/Upbit execution streams. Real orders still require an eligible immutable portfolio, Paper evidence, account reconciliation, risk checks, explicit mode/route enablement, and a natural confirmed-bar signal.

Route availability must be implemented and verified before a live test can proceed. For a supported route, the backend additionally requires exactly one Deployment, operator confirmation, appropriate mode and safety controls, and a fresh Preflight for that exact Deployment. These conditions alone cannot open the currently blocked ordinary continuous route. A Deployment, risk policy, account route, or risk-opening control change invalidates the Preflight and requires it to be run again. Tightening a safety control takes effect immediately; enabling new-entry protection still permits only independently verified position-reducing orders. The global Kill Switch blocks orders and requests cancellation of working orders; it never creates a position-flattening order.

Operational identity and recovery evidence are persisted as append-only records:

- immutable Deployment Manifest revisions and hashes
- expiring Preflight Snapshots
- Runtime Sessions and lifecycle events
- order/fill/audit events
- Incident state transitions

The selected Deployment broker's REST snapshot, private execution event state, and local event ledger must be freshly reconciled within 60 seconds before Preflight can pass. Cross-broker portfolios remain blocked until account-scoped Preflight supports every route. An unknown submit result is reconciled by Client Order ID and is never blindly retransmitted. A broker cancel acknowledgement remains `cancel_pending` until a later status/event reconciliation proves cancellation; a fill always takes precedence over a pending cancel.

## Binance Futures safety contract

- Each Futures strategy carries an immutable, sealed execution policy (margin mode, leverage, risk per trade, and maximum exposure). A malformed policy, seal mismatch, or broker-side drift fails closed.
- The read-only preflight simulator uses the current mark price, leverage bracket, maintenance margin, fees, funding, existing position, proposed notional, and protective-stop distance. Missing or non-finite inputs block the order.
- Capital is promoted in stages: `CANARY` (up to 10 USDT), `SMALL` (up to 25 USDT after at least 3 verified fills), then `FULL` (after at least 20 fills, 168 hours, and a clean `PASS` soak). `PASS_WITH_WARNING` never unlocks `FULL`.
- Dataset lineage from Scraper through Backtester and Paper is preserved and checked against the immutable revision before Live eligibility.
- Risk-increasing Futures entries remain deliberately blocked until the entry and an exchange-native reduce-only `STOP_MARKET` order can be submitted and acknowledged as one fail-closed workflow. Position-reducing orders still require broker position truth.

## Headless continuous monitor

```powershell
release\LiveTrader.exe --daemon --profiles stock,crypto --mode MONITOR --poll-seconds 30
```

`scripts\install_monitor_task.ps1` installs the per-user Windows task `TradingSystem-LiveTrader-Monitor`; `scripts\uninstall_monitor_task.ps1` removes it. It deliberately starts in `MONITOR`, so a reboot never upgrades itself to live-order mode. Status is written to `%LOCALAPPDATA%\live_trader\logs\daemon_status.json`.

The unattended MONITOR soak report uses three terminal verdicts. `PASS` means
no incident was observed. `PASS_WITH_WARNING` is limited to a read-only broker
poll connectivity failure that automatically recovers within 120 seconds
(`LIVE_TRADER_SOAK_TRANSIENT_RECOVERY_SECONDS`, bounded to 15–300 seconds) while
the heartbeat, order/fill ledgers, program and broker position fingerprints,
real-order count, and daily-loss gate remain unchanged. The incident and its
recovery evidence stay in the JSON/SQLite report. Authentication or payload
errors, private-stream ambiguity, any order/position change, a real order in
MONITOR, heartbeat gap, loss-gate event, late/unrecovered disconnect, or
`FAILED`/`CRASHED`/`STALE` runtime remains `FAIL`.

- KIS domestic trades use `H0STCNT0` and are aggregated into the configured timeframe. Only a completed bucket reaches the strategy.
- Upbit `myOrder` and KIS domestic/overseas execution notifications use private WebSockets and reconnect independently from market data.
- REST account reconciliation remains a backstop. An ambiguous order outcome is never blindly resubmitted.

## Compatibility Notes

- Strategy artifacts are read from `LIVE_TRADER_STRATEGY_ARTIFACT_DIR` or `TRADER_STRATEGY_ARTIFACT_DIR` first, then `trading-system\packages\strategy-core`, then `%APPDATA%\trading_programs\strategies`.
- Strategy plugin folders are read from `LIVE_TRADER_STRATEGY_PLUGIN_DIR` or `TRADER_STRATEGY_PLUGIN_DIR`; when unset, each artifact folder's `plugins` subfolder is used.
- Shared live permission checks mirror `packages/trading-contracts`.
- The React UI imports `../../../packages/design/design-tokens.css`.
- Live execution requires artifacts with `permissions.live_allowed === true` or `live_allowed === true`.
