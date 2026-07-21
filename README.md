# Live Trader

Real-money trading console for the `trading-system` workspace.

`live_trader` is the final execution layer after:

- `stock_data_scraper`: market data collection and preparation
- `backtester`: strategy research, optimization, and artifact export
- `paper_trader`: shadow/paper validation, approval workflow, and operational safety checks

This app intentionally blocks live orders until broker API credentials, live strategy permissions, risk checks, and broker order adapters are ready.

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

Create `.env` next to this README. Do not commit real secrets.
The desktop app loads this file automatically on startup, without overriding OS-level environment variables.
Use the in-app settings for UI/risk controls only; broker secrets such as `KIS_APP_SECRET` should stay in `.env` or a safer OS secret store.

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

## Headless continuous monitor

```powershell
release\LiveTrader.exe --daemon --profiles stock,crypto --mode MONITOR --poll-seconds 30
```

`scripts\install_monitor_task.ps1` installs the per-user Windows task `TradingSystem-LiveTrader-Monitor`; `scripts\uninstall_monitor_task.ps1` removes it. It deliberately starts in `MONITOR`, so a reboot never upgrades itself to live-order mode. Status is written to `%LOCALAPPDATA%\live_trader\logs\daemon_status.json`.

- KIS domestic trades use `H0STCNT0` and are aggregated into the configured timeframe. Only a completed bucket reaches the strategy.
- Upbit `myOrder` and KIS domestic/overseas execution notifications use private WebSockets and reconnect independently from market data.
- REST account reconciliation remains a backstop. An ambiguous order outcome is never blindly resubmitted.

## Compatibility Notes

- Strategy artifacts are read from `LIVE_TRADER_STRATEGY_ARTIFACT_DIR` or `TRADER_STRATEGY_ARTIFACT_DIR` first, then `trading-system\packages\strategy-core`, then `%APPDATA%\trading_programs\strategies`.
- Strategy plugin folders are read from `LIVE_TRADER_STRATEGY_PLUGIN_DIR` or `TRADER_STRATEGY_PLUGIN_DIR`; when unset, each artifact folder's `plugins` subfolder is used.
- Shared live permission checks mirror `packages/trading-contracts`.
- The React UI imports `../../../packages/design/design-tokens.css`.
- Live execution requires artifacts with `permissions.live_allowed === true` or `live_allowed === true`.
