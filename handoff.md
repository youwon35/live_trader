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

## 2026-07-14 Tab Audit And Safety Hardening

The six current tabs were exercised against the packaged Python API, not only static fallback data.

Important root cause fixed:

- `execution_calibration` could contain `Infinity`.
- Python's default JSON encoder emitted that non-standard value, so the browser rejected the entire `/api/snapshot` response.
- The UI then remained on its fallback snapshot and could show unrelated checks as passing.
- `live_trader/server.py` now recursively converts non-finite floats to JSON `null` and uses strict JSON encoding.
- The frontend now marks snapshots with `api_connected`, clears stale data on failure, shows a visible connection banner, and treats Doctor checks as unavailable instead of passed.

Safety changes:

- Releasing Kill Switch, disabling Dry Run, and unblocking new entries require an explicit confirmation in both UI and Python state logic.
- Saving a broker snapshot as the program-ledger baseline requires explicit confirmation at the API boundary.
- Enabling `LIVE_TRADER_ENABLE_REAL_ORDERS=true` requires explicit confirmation at the UI and API boundary.
- API requests now have a 10-second timeout, strict JSON-response validation, and more useful error messages.
- When the API is unavailable, top-level emergency state becomes `확인 불가` and sensitive controls are disabled.

Tab cleanup:

- `사전점검`: Doctor's API detail was reduced to actionable broker/global blockers rather than repeating every passing capability.
- `실거래 준비`: removed the duplicate Strategy table and duplicate Watchdog; empty asset groups no longer show misleading UNKNOWN/PORT LEGACY badges or an empty portfolio panel.
- `자동화`: retained automation profile, Watchdog, queue, and order history as the operational set.
- `로그`: verified filters, sort, scope/level controls, and CSV availability with real audit rows.
- `API`: added a direct `연결 설정 열기` path to Settings.
- `설정`: retained appearance/layout and broker connection assistant; real-order route enable now has a high-risk confirmation.
- Added an explicit SVG favicon to eliminate the previous 404 console error.

Verification completed:

- `npm run build`: PASS.
- `python -m unittest discover -s tests -v`: 65 tests PASS.
- Playwright desktop review at 1440x1000: all six tabs exercised with real snapshot data; browser console 0 errors.
- API-disconnect drill: visible fail-closed banner and `확인 불가` state verified.
- UI confirmation drill: Dry Run release dialog verified and dismissed without changing state.
- Direct API rejection drill: unconfirmed Dry Run release, ledger-baseline save, and real-order enable all rejected.
- `build_exe.ps1`: PASS, including 100%/125%/150% desktop-scale contracts.
- `release/LiveTrader.exe`: launched successfully and remained running during the smoke interval.

The rebuilt executable is:

```text
D:\github\PROGRAM\trading-system\apps\live_trader\release\LiveTrader.exe
```

## 2026-07-18 API 없는 전체 탭 검증과 안전 보강

현재 실제 상태:

- `MONITOR`, `Dry Run ON`, 신규 진입 차단 ON, Kill Switch OFF.
- KIS/Binance/Upbit 자격 증명은 모두 미등록이며 주문 준비 브로커는 0개다.
- 공유 전략 5개와 Portfolio 3개를 읽으며, 최신 Portfolio `portfolio-20260717T191245-36dd3b8d`는 `parameter_surface`, `cross_market` 미통과라 Live 승인을 정확히 차단한다.

API 없이 실제 실행한 기능:

- Watchdog, Final Preflight, 브로커 3종 점검, 계좌/포지션 대조, stock/crypto 전략 사이클, 테스트 주문 게이트, Shadow Live, 정책 Replay, Recovery Drill, 체결 이벤트 동기화, CSV/HTML 감사 내보내기.
- 테스트 주문은 Portfolio lifecycle/evidence, 신규 진입 차단, readiness, 대조 불일치 사유로 `risk_blocked` 됐다.
- Shadow Live는 브로커 전송 없이 해시 증거를 만들었고 정책 Replay와 감사 내보내기는 성공했다.
- 계좌/체결 동기화는 KIS 2개, Binance 2개, Upbit 2개 키 누락을 구체적으로 기록했다.

수정한 안전/품질 문제:

- 주문 게이트가 전달받은 snapshot 대신 디스크 Portfolio를 중간에 다시 읽던 TOCTOU 경계를 제거했다.
- 환경 변수로 지정한 Strategy Artifact 경로가 기본 경로와 섞이지 않고 명시적 override로 동작한다.
- `MONITOR`라는 이유만으로 Recovery Drill의 브로커 대조를 통과시키던 오류를 제거했다. 실제 대조가 `pass`가 아니면 `safeMode=true`, `newEntriesAllowed=false`다.
- KIS 실계좌 번호와 HTS ID는 설정 응답/UI에서 원문을 재표시하지 않고 마스킹한다. 계좌번호(CANO)는 로그인 ID가 아니라는 설명을 명시했다.
- 1280px 설정 화면에서 버튼/탭/입력칸이 패널 밖으로 나가던 반응형 배치를 수정했다.
- `npm run ui:smoke`를 추가해 1707×960과 1280×800에서 6개 탭, API 연결, 가로 넘침, 화면 밖 컨트롤, 콘솔 오류를 반복 검증한다.
- PyInstaller EXE가 임시 `_MEI` 경로를 workspace로 오인해 샘플 전략만 읽던 문제를 수정했다. 최종 EXE는 공유 전략 5개와 Portfolio 3개를 확인했다.

검증:

- Python unittest 71개 통과.
- frontend build 통과.
- 100/125/150% desktop-scale 계약 통과.
- 소스 및 최종 EXE 대상 UI smoke 12개 조합 통과.
- 최신 EXE: `D:\github\PROGRAM\trading-system\apps\live_trader\release\LiveTrader.exe`.

### API 등록 후 다음 작업

1. 먼저 Paper Trader를 기존 이력 보존 상태로 Artifact 탭부터 전체 재검증하고, 정규장 알고리즘 신호가 있을 때만 KIS 모의 주문 접수/체결을 확인한다.
2. Live Trader KIS를 쓸 때 `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`(실계좌 CANO 앞 8자리), `KIS_ACCOUNT_PRODUCT_CODE`(보통 01), 필요 시 `KIS_HTS_ID`를 등록한다.
3. Binance를 쓸 때 `BINANCE_API_KEY`, `BINANCE_API_SECRET`; Upbit를 쓸 때 `UPBIT_ACCESS_KEY`, `UPBIT_SECRET_KEY`를 등록한다. 사용할 거래소 한 종류만 등록해도 된다.
4. 키 등록 후에는 먼저 읽기 전용 인증·잔고·포지션 대조와 체결 이벤트 동기화부터 확인한다. 성공 전에는 `LIVE_TRADER_ENABLE_REAL_ORDERS=false`를 유지한다.
5. Strategy/Portfolio 전문 게이트와 Paper Portfolio Evidence가 모두 일치하고, 취소/정정·체결 스트림 등 미구현 기능까지 완료된 뒤에만 별도 확인을 받아 SMALL_LIVE 소액 절차를 설계한다. 자동으로 실주문을 켜지 않는다.
## 2026-07-19 공용 토글·상태 pill·containment 보강

- 환경변수 credential chip과 capability 상태 카드가 공용 밝은 배경 대비 계약을 사용하도록 바꾸고, 밝은 초록 배경의 글자는 검은색으로 통일했다.
- 승급 체크리스트의 긴 lifecycle/evidence 문장이 카드 밖으로 빠지지 않도록 `min-width: 0`, 줄바꿈과 word-break 계약을 추가했다.
- Python unittest 71개, frontend build, PyInstaller 패키징, 실제 최신 EXE 대상 UI smoke 12조합(6탭 x 1707x960/1280x800)을 통과했다. 넘침·화면 밖 컨트롤·콘솔 오류는 모두 0건이다.
- 최신 `release\LiveTrader.exe`를 사용자가 지정한 오른쪽 모니터 논리 좌표 2560,1600(물리 1707x960)에 최대화해 실제 렌더를 확인했다.
- 후속 사용자 입력: 실거래 API 검증 전 KIS `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`, `KIS_ACCOUNT_PRODUCT_CODE`와 선택한 거래소(Binance/Upbit) 자격 증명을 설정한다. 실제 주문 활성화는 별도 최종 승인과 최소 단위 점검 뒤에만 수행한다.

## 2026-07-20 실계좌 API 읽기 전용 검증과 EXE 영속화 보강

실제 등록값을 사용하되 주문 전송과 실거래 활성화는 하지 않고 KIS, Binance, Upbit를 읽기 전용으로 검증했다.

- KIS OAuth 인증과 국내 계좌 조회 성공: 현금성 잔고 346 KRW.
- Binance signed account 조회 성공: 현금성 잔고 약 0.08 USDT, BTC 포지션 0.00000451.
- Upbit 전체 계좌 조회 성공: 현금성 잔고 약 1 KRW.
- 세 브로커 최종 대조의 조회 오류 0건, 프로그램 원장과 불일치 0건을 확인했다.
- 남은 `API 필요 1건`은 KIS 해외주식(SPY) 잔고 조회 어댑터가 아직 없기 때문이며 자격 증명 오류가 아니다.

실제 EXE에서 발견하고 수정한 문제:

- PyInstaller 임시 `_MEI...\.env`에 설정을 저장하던 문제를 고쳐 `%LOCALAPPDATA%\live_trader\.env`에 영구 저장한다.
- 감사 DB, 프로그램 원장, 복구 저널도 `_MEI...\logs`가 아니라 `%LOCALAPPDATA%\live_trader\logs`를 사용한다.
- KIS 점검 한 번에 토큰을 두 번 발급해 `EGW00133`(1분당 1회)에 걸리던 문제를 만료 직전까지 안전하게 재사용하는 캐시로 고쳤다.
- 성공한 빈 포지션 조회를 `API 필요`로 오판하지 않고 국내주식/코인 0포지션으로 판정한다. 아직 구현되지 않은 KIS 해외주식 범위는 계속 차단한다.
- 창 상태 버전을 올리고 사용자 지정 오른쪽 모니터 좌표 `(3840, 0)`에서 항상 최대화해 열도록 고정했다.

실제 흐름 검증:

- 브로커 스냅샷을 영구 프로그램 원장 기준으로 저장해 현금 3개, 포지션 1개가 일치했다.
- EXE를 완전히 종료하고 다시 실행한 뒤 설정과 원장이 유지되는 것을 확인했고, 재대조도 오류 0·불일치 0으로 복원됐다.
- `MONITOR`, Dry Run ON, 신규 진입 차단 ON, 전체 차단 OFF 상태를 유지했다.
- `사전점검 → API → 실거래 준비 → 자동화 → 로그 → 설정`을 실제 EXE에서 순서대로 확인했다.
- `npm run ui:smoke`: 오른쪽 모니터/compact desktop의 12개 조합 모두 document/workspace overflow 0, escaped control 0.
- Python unittest 76개 통과, frontend/desktop-scale/PyInstaller 빌드 통과.

다음 안전 출발점:

1. Live Trader의 KIS 해외주식 잔고 조회, 브로커별 주문 상태/취소·정정, 체결 스트림 어댑터를 구현·검증한다.
2. 그 전까지 `LIVE_TRADER_ENABLE_REAL_ORDERS=false`와 MONITOR를 유지한다.
3. 사용자가 원래 지정한 다음 큰 작업은 최신 Paper Trader EXE를 열어 기존 이력을 변경하지 않고 전략 Artifact 탭부터 화면 순서대로 재검증하는 것이다.

## 2026-07-20 Upbit 소액 실주문과 공통 계좌·포지션 화면

- 사용자가 승인한 범위에서 Upbit `KRW-BTC` 시장가 매수 5,000원을 1회 전송했다. 주문 UUID는 `16fa4eb3-4945-48c8-843d-788dafdb0fe5`, 체결금액은 4,999.104원, 수수료는 2.50원이다.
- 거래소 최종 상태 `cancel`에 실제 체결 내역이 있으면 실패/단순 접수가 아니라 `체결·잔여취소`로 판정하도록 보강했다.
- 주문 뒤 Upbit 원화 잔고와 BTC 보유 수량이 계좌·포지션 대조에 반영되는 것을 확인했고, 실거래 주문 route는 즉시 다시 OFF로 저장했다.
- KIS·Binance·Upbit 잔고/포지션을 같은 카드와 표 형식으로 통일하고, 브로커 필터·수동 갱신·10초 자동 갱신·`포지션 크게 보기` 팝업을 추가했다.
- Upbit 평균단가와 평가금액은 보유자산 BTC가 아니라 결제통화 KRW로 표시한다. 시세/평가 데이터가 없는 Binance 잔고는 `0.00 BTC` 대신 `평가 대기`로 표시한다.
- 최신 `release\LiveTrader.exe`를 오른쪽 모니터 `(3840, 0)`에 최대화하고 실제 계좌 3개와 포지션 2개가 갱신되는 것을 확인했다.
- Python unittest 81개, frontend/desktop-scale/PyInstaller 빌드를 통과했다.

다음 실제 알고리즘 검증은 승급된 `KRW-BTC` 전략이 준비된 뒤 신호→비중계산→주문→체결→원장 대조 전체 경로로 수행한다. 이번 소액 주문은 거래소 커넥터 검증이며 알고리즘 전략 검증을 대신하지 않는다.

## 2026-07-20 새 Portfolio Live 차단 검증과 노출 계약 수정

- 새 Portfolio `portfolio-20260720-kodex-btc-robust-v1`과 두 Strategy Instance를 Live Trader가 읽는 것을 확인했다. KODEX는 `Shadowed`, BTCUSDT는 `Papered`지만 둘 다 `LIVE BLOCKED / PORT BLOCK`이다.
- Live Portfolio gate도 Backtester/Paper와 동일하게 `targetWeight × positionSizeFraction`을 사용하도록 수정했다. KODEX는 60%×0.2=12%, BTC는 40%×0.00001=0.0004%가 실효 목표다.
- 새 후보는 lifecycle `backtested`, `live_export_allowed=false`, `portfolio-candidate-not-approved`, `failed-stage:parameter_surface`, `failed-stage:cross_market`로 정확히 차단된다.
- DryRun 전략 사이클은 `crypto 프로필에 live_small_eligible/live_eligible 전략이 없습니다`로 종료됐고 주문 큐 0건을 유지했다. 계좌 읽기 전용 상태는 KIS 100,346원, Upbit 44,999원, Binance 0.0763 USDT 및 BTC 0.00000451이며 불일치 0건이다.
- 최종 Preflight는 hard stop 5개를 유지한다. 실거래 route OFF, MONITOR, Dry Run ON, 신규 진입 차단 ON 상태이며 실제 주문은 전송하지 않았다.
- Python 82 tests, frontend build, 100/125/150% desktop scale, PyInstaller, 12-view UI smoke가 통과했다. 최신 EXE는 `release\LiveTrader.exe`다.

다음 단계는 새 불변 전략/Portfolio가 전문 연구 게이트와 Paper Portfolio Evidence를 모두 통과한 뒤에만 before-live-small 승급을 재검증하는 것이다. 현재 후보의 차단을 우회하지 않는다.
