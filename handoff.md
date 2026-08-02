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
- Default port: `18795`.
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

## 2026-07-21 FX 신선도 hard gate와 실제 계좌 재점검

- 외화 자산의 Portfolio gate가 Artifact에 고정된 환율의 기준일·허용 일수·환율 값을 읽어 신선도를 판정하도록 했다. 기준통화와 동일한 자산은 1.0으로 통과하고, 환율 누락·0 이하·기준일 초과는 `fx-stale` hard blocker로 차단한다.
- 실거래 준비 화면에 `FX 기준시각` 카드를 추가해 통화쌍, 환율, 기준일, 경과 일수와 STALE 여부를 바로 확인할 수 있게 했다.
- 실제 저장 자격 증명을 노출하지 않고 KIS·Binance·Upbit를 읽기 전용으로 재대조했다. KIS 100,346원, Upbit 44,999원, Binance 0.08 USDT가 프로그램 원장과 일치했고 조회 오류·수량 불일치는 0건이었다. KIS 해외주식 범위 1건은 어댑터 미구현으로 계속 `API 필요`다.
- Upbit 주문 가능 정보 Preview는 KRW-BTC 시장가 5,000원, 수수료율 0.05%, 수수료 포함 필요액 5,002.5원, 주문 가능 잔고 44,999원으로 통과했다. 이는 커넥터 점검일 뿐 통과 전략 주문이 아니며 실주문은 전송하지 않았다.
- Live-Small/Live 승인 전략은 0개다. 현재 전략은 Paper 관찰 1일·1 regime으로 최소 30일·2 regime에 미달하고 Portfolio도 live 권한이 없어, 사용자의 손실 감수 의사와 별개로 실전 알고리즘 주문을 만들지 않았다.
- 최신 EXE 실화면에서 `Shadowed 현재 단계`, `Papered 대기`, `Before Live-Small 대기`, Portfolio Evidence BLOCK을 확인했다. 실거래 route OFF, MONITOR, Dry Run ON, 신규 진입 차단 ON을 유지했다.
- Python unittest 83개, frontend build, 100/125/150% desktop scale, PyInstaller 빌드를 통과했다. 최신 `release\LiveTrader.exe` SHA-256은 `06E3A08981EEB6A7BB9F60C789A9D1502C3C93AAF83C9586F37B0218DDDD1D76`이다.

## 2026-07-22 Portfolio 기반 Live 연속 감시와 실제 주문 경로 연결

- Live Trader 자동화를 1회 진단과 별도의 `지속 감시 Run/Stop`으로 분리했다. stock/crypto 프로필은 서로 독립된 장기 실행 루프를 가질 수 있고, 선택 Portfolio의 Strategy Instance를 각 시장에 맞춰 계속 감시한다.
- Binance/Upbit는 공식 WebSocket의 확정 봉을 우선 사용하고 REST warm-up/fallback을 둔다. 주식은 Yahoo 완료 봉을 polling한다. 전략은 새 확정 봉당 정확히 한 번 평가하며 HOLD 뒤에도 종료하지 않는다.
- 연결 끊김은 지수 backoff로 재연결하고, heartbeat·마지막 봉·마지막 평가·재연결 수를 상태 파일과 UI에 남긴다. 시세 지연 또는 intraday 누락 봉이 있으면 주문 신호를 fail-closed HOLD로 바꾼다. Portfolio hash별 checkpoint로 재시작 중복 주문을 막는다.
- MONITOR 모드는 실제 공개 시세와 전략을 계속 실행하되 주문하지 않는다. SMALL_LIVE/FULL_LIVE는 Portfolio permissions와 기존 승급·Dry Run·신규 진입·kill switch·계좌 대조·리스크 게이트를 모두 통과해야 시작된다.
- 기존 `submit_order_intent`가 리스크 판정과 OMS 기록까지만 하고 broker adapter를 호출하지 않던 단절을 수정했다. 모든 게이트 통과 및 dry-run 해제 시에만 `LiveBrokerRouter.place_order`를 실제 호출하며, OMS를 SUBMITTING → ACKNOWLEDGED/REJECTED/UNKNOWN으로 전이한다. 네트워크 결과 불명은 자동 재주문하지 않는다.
- 실제 AAPL/AMZN Portfolio를 MONITOR로 실행해 Yahoo warm-up과 지속 대기를 확인했다. 실제 주문은 보내지 않았다. 현재 Portfolio는 Live-Small 승인 조건 미충족이므로 실전 모드는 계속 안전 차단된다.
- Live Python 83개 테스트, frontend build, 최신 EXE 빌드와 실제 EXE API/UI smoke 12조합(2 viewport × 6 tab)을 통과했다. 최신 실행 파일은 `release\LiveTrader.exe`다.

전문 운용의 핵심은 CPU가 허용하는 만큼 전략을 반복 실행하는 것이 아니라, 데이터 수집은 지속하되 확정 봉 이벤트에만 정확히 한 번 판단하는 것이다. 초단타가 필요하면 tick/order-book 전용 전략과 별도 이벤트 엔진을 사용해야 하며, 현재 봉 전략과 섞지 않는다.

## 2026-07-22 KIS/Upbit 네이티브 실시간 감시와 Windows 상시 실행

- 공용 런타임에 KIS 국내 체결 `H0STCNT0` WebSocket feed를 추가했다. 틱은 전략 timeframe의 OHLCV로 모으고 완료된 버킷만 내보낸다. historical warm-up 및 KIS VTS가 제공하지 않는 미국 모의 시세는 Yahoo 완료 봉을 사용한다.
- Live Trader에 Upbit private `myOrder`와 KIS `H0STCNI0`/`H0GSCNI0` 체결 스트림을 추가했다. KIS 암호화 통지는 AES-CBC로 복호화하며 이벤트는 durable JSONL과 프로그램 원장 동기화 경계로 전달된다.
- `--daemon` CLI와 Windows 예약 작업 설치/제거 스크립트를 추가했다. `TradingSystem-LiveTrader-Monitor` 작업은 로그온 시 최신 EXE를 MONITOR로 시작하고 실패 시 재시작한다. 재부팅이 실거래 모드로 자동 승격시키지는 않는다.
- 실제 계좌 읽기 결과는 KIS 100,346원/포지션 없음, Upbit 44,999.37843058원과 KRW-BTC 0.0000526이다. KIS/Upbit 개인 WebSocket은 모두 connected, reconnect 0, REST 대조 오류 0으로 확인했다.
- Windows 작업의 최신 EXE heartbeat, stock/crypto RUNNING, KIS/Upbit connected, execution poll ok를 확인했다. Live Python 87개, 공용 runtime 86개, frontend/DPI/PyInstaller 및 격리 daemon EXE smoke를 통과했다.
- 현재 Portfolio 16개는 모두 live_small_allowed=false이며 Upbit `KRW-*` Portfolio가 없다. 따라서 사용자 자금 사용 허가는 보유하지만, 검증되지 않은 심볼 치환이나 승급 우회 주문은 내지 않았다. 기존 Upbit 5,000원 커넥터 체결 외에 이번 작업에서 새 실주문은 0건이다.
- 장마감 관찰 중 Windows가 상태 JSON 교체를 잠깐 거부해 Paper runtime이 한 번 정지했다. `DurableRuntimeState.write()`가 WinError 5/32를 지수형 짧은 재시도로 흡수하도록 보강했고 회귀 테스트를 추가했다.
- 수정 후 05:00 KST 확정 봉에서 AAPL RSI(5)=73.65, AMZN RSI(7)=54.78을 각각 정확히 한 번 평가해 둘 다 자연 HOLD였다. 15초 뒤에도 RUNNING, bar 2, decision 2, HOLD 2, duplicate 0, error 없음으로 다음 봉 대기를 확인했다.
- 공식 `overseas_stock_functions_ws.py`의 H0GSCNI0 25개 컬럼 순서에 맞춰 해외 체결도 symbol/side/quantity/price/state로 정규화하고 회귀 테스트를 추가했다.

## 2026-07-22 장기 실행 자원 상한과 무중단 대기 빌드

- private execution queue는 broker별 1,000개 상한을 유지한다. `broker_execution_stream.jsonl`은 5MB 단위로 회전하며 백업은 3개까지만 보존한다.
- 공용 realtime feed 중복키와 runtime 평가키는 각각 10,000개로 제한했다. REST fallback 최소 간격은 일봉 60초, 1시간봉 15초, 15분봉 5초다.
- 30초 실측에서 MONITOR daemon은 CPU 1.063초(단일 코어 약 3.54%), working set 61.55~64.63MB, private memory 58.72~60.66MB, handle 294→295, thread 8개였다.
- Live 88 tests와 새 EXE `--help` smoke가 통과했다.
- 운영 중인 `TradingSystem-LiveTrader-Monitor` 중단 승인이 없어 기존 `release\LiveTrader.exe`는 계속 감시 중이다. 새 코드는 `release_pending\LiveTrader.exe`에 무중단 빌드했다.
- pending EXE SHA-256은 `6D26987EB90632A9DE4BA3A4BAA50CCBCC2AE9CAA8E8262E34D988B01799F979`다. active EXE SHA-256은 `9D75EDE07B5A83BA5AC9E6B9901F62C348FF9671B8C7A3C540839C5EFD9DFD1F`다.
- `build_exe.ps1`은 LiveTrader 실행 중이면 `release_pending`/`build_pending`을 사용한다. pending EXE를 예약 감시에 적용하려면 사용자에게 짧은 daemon 중단·교체·재시작을 명시적으로 승인받아야 한다.

## 2026-07-23 사전점검·실거래 준비 UI 정리와 Doctor 판정 분리

- 상단 알림 배지는 흰 배경·진한 빨강 숫자·빨강 외곽선으로 바꿔 숫자가 배경에 묻히지 않게 했다. 긴급 차단은 빨간 배경, 회색 외곽선, 검은 글자로 고정해 안전 조작임을 즉시 구분하게 했다.
- 실거래 Doctor의 `API / 브로커 연결` 판정에서 전역 readiness, 실주문 활성화 플래그, 선택 기능 capability를 제거했다. 이제 이 카드는 API 인증정보 누락과 실제 계좌 조회 오류만 차단으로 판단하고, 전략 승격·운영 체크리스트·리스크·대조·Preflight는 각각의 전용 카드가 책임진다.
- Doctor 7개 요약 카드 우측의 `조치/주의/API/차단/검토` 배지를 제거하고, 카드 배경과 설명으로 상태를 전달하도록 단순화했다. 상세 점검의 상태 표시는 진단 정보이므로 유지했다.
- 포지션·계좌 대조의 상태·실행 버튼을 8px 간격으로 한쪽에 묶고, 요약 상자와 다음 조치 칩 사이에 12px 여백을 추가했다.
- 사전점검 화면의 `Upbit 실제 주문 1회 점검` 패널과 프런트엔드 전용 호출 연결을 제거했다. 서버의 안전 제한 및 테스트 API는 회귀 테스트와 내부 진단을 위해 유지했다.
- 계좌 카드의 `10초 자동 갱신`과 계좌 상태 배지 글자를 검은색으로 통일했다. 승급 체크리스트 카드는 모든 외곽선을 회색으로 통일하고 PASS는 초록, WAIT는 노랑, BLOCK은 빨강 배경으로 구분한다.
- 검증: Python unittest 88개 통과, Vite production build 통과, 100%/125%/150% desktop-scale 계약 통과, Playwright 실제 렌더 확인, PyInstaller 재패키징, 최신 EXE 5초 실행 유지 smoke 통과.
- 최신 실행 파일: `release\LiveTrader.exe`, SHA-256 `0D5033CE002CAE76DD1A90E3FEBB7DD87A9C3A0B815D37A5537802833A7F3BC9`.

## 2026-07-23 실거래 준비 전략 검색·필터·저장 검색

- 실거래 준비의 활성 전략 선택 앞에 이름/ID/종목/파라미터/차단 사유 통합 검색을 추가했다.
- 기존 주식·ETF/코인 범위와 함께 라이프사이클, 주기, 플러그인·전략 유형 필터와 최근 수정/이름/단계 정렬을 사용할 수 있다.
- 활성 조건 칩과 결과 수를 표시하고, 검색 결과가 달라지면 현재 선택을 보이는 첫 전략으로 안전하게 맞춘다.
- 자주 쓰는 조건은 이름을 붙여 최대 30개까지 로컬 저장하며 자산 탭 범위도 함께 복원한다. 실제 실행 화면에서 `AAPL 모니터` 조건이 재접속 뒤 남고 다시 적용했을 때 15개 중 AAPL 3개만 복원되는 것을 확인했다.
- 라이프사이클 `Draft` 표기도 다른 프로그램과 통일했다. production build, 100/125/150% 데스크톱 계약, PyInstaller 패키징과 10개 화면 UI smoke가 통과했다.

## 2026-07-23 공용 검색 프리셋·Binance SMALL_LIVE 실체결 검증

- 저장 검색 프리셋을 `%APPDATA%\trading_programs\strategy-search-presets.json` 공용 저장소로 옮겨 Backtester/Paper Trader와 공유하고 기존 localStorage 프리셋을 병합한다.
- 승인된 단일 Strategy Artifact도 Portfolio 없이 연속 실행할 수 있게 했고, 포트폴리오가 명시된 주문에만 universe hard gate를 적용한다.
- Binance signed REST와 사설 WebSocket 모두 `/api/v3/time` 기준 오프셋을 사용하고 `-1021`이면 한 번 재동기화한다.
- 브로커별 readiness/대조를 분리해 KIS 해외 대조 경고가 Binance를 막지 않게 했다. Watchdog은 실행 중인 브로커만 감시하고, 완료 봉 전략은 연속 feed heartbeat와 사설 체결 스트림 연결을 정상 근거로 사용한다.
- MONITOR→SMALL_LIVE는 실행기를 재시작하지 않고 워밍업·heartbeat를 유지한 채 원자적으로 전환한다. Binance `BTC` 잔고를 `BTCUSDT` 전략 포지션으로 매핑한다.
- 1회용 확인 토큰과 5~10 USDT 하드 한도를 둔 Binance broker smoke를 추가했다. 이 주문은 전략 신호와 구분해 `BROKER_SMOKE`로 기록한다.
- 실제 계좌에서 0.0001 BTC 시장가 매수가 65,588.48 USDT에 FILLED 됐다. USDT는 약 30.36→23.80, BTC는 약 0.00000451→0.00010441로 반영됐고 체결 스트림 5건 저장, 대조 mismatch 0을 확인했다.
- 검증 뒤 실주문 플래그를 false, MONITOR/Dry Run/신규 진입 차단 상태로 복구하고 runtime을 중지했다. 구매 BTC는 계좌에 남아 다음 실행 시 보유 포지션으로 인식된다.
- Live unittest 103개, 공통 runtime 90개, Vite build, 10화면 UI smoke, PyInstaller 패키징이 통과했다. Windows 시장 캘린더를 위해 `tzdata`도 실행 파일에 포함했다.

## 2026-07-24 브로커 기준 운영·자동 승급/중지·재시작 복구

- 주문 의도, OMS, 브로커 접수, 체결 이벤트, 프로그램 원장을 같은 `trace_id`로 연결하고 주문–trace 인덱스를 체크포인트에 포함했다.
- 브로커 포지션을 최종 진실로 원장 기대 수량과 대조한다. API 미확인이나 수량 불일치가 있으면 신규 진입을 자동 차단한다.
- 해당 브로커의 fresh position snapshot이 있을 때만 Before Live-Small 후보를 자동 승급하거나, Live 전략을 대조 불일치로 자동 일시정지한다.
- 재시작 시 체크포인트·미확정 주문·누락 봉·멱등성 인덱스를 합친 복구 계획을 만들며 MONITOR에서 안전하게 복구한다.
- 운영 대조 패널에 진실 원본과 접힌 체결 Trace·복구 계획을 추가해 기존 화면 밀도를 유지했다.
- Live Python 104개, Vite build, 2개 뷰포트×5개 탭 UI smoke에서 overflow·이탈 control·console 오류 0건이 통과했다.
- 최신 `release\LiveTrader.exe`를 재생성하고 `--help` 기동 smoke를 통과했다. SHA-256 `A5FC5559E7ACBCB72D737182583594F133D654593708CCB23A5776AFB0F58F1B`.

## 2026-07-24 Artifact 탐색·메타데이터 통합

- 실거래 준비의 전략과 포트폴리오 모두에 공용 즐겨찾기·태그·메모를 추가하고 검색 대상에 태그와 메모를 포함했다.
- `최근 사용`, `최근 승급`, `현재 실행 중`, 실패 이유 필터를 두 Artifact 유형에 동일하게 제공한다.
- 실제 연속 실행을 시작한 Portfolio는 최근 사용으로, 실제 라이프사이클 승급은 최근 승급으로 기록된다.
- 공용 메타데이터 API는 `%APPDATA%\trading_programs\artifact-user-metadata.json`을 원자적으로 갱신한다.
- 반응형 검사에서 발견한 검색 패널의 고정 min-content 폭을 제거했다. 1707×960과 1280×800의 5개 탭에서 가로 넘침·경계 이탈·콘솔 오류 0건이다.
- Python 104개와 production build, 강화된 UI smoke를 통과했다.

## 2026-07-24 전 프로그램 테마 대비 통일

- Live Trader를 포함한 다섯 프로그램의 상태·시장·소스·로그 배지와 보조 버튼을 불투명 테마 표면으로 통일했다.
- 다크 모드의 SETTINGS/INFO/WARN/STORAGE와 같은 짧은 영문 라벨은 흰 글자와 의미별 단색 배경을 사용하며, 화이트 모드도 밝은 표면과 짙은 글자로 대비를 유지한다.
- production build와 Playwright 실제 브라우저 계산값 검증에서 대표 배지의 `opacity: 1`을 확인했다.

## 2026-07-24 라이트 팔레트 배지의 전 테마 고정

- Live Trader를 포함한 다섯 프로그램의 운영 배지는 다크 모드에서도 화이트 모드의 파스텔 배경을 그대로 사용한다.
- 글자는 `#0f172a`, 외곽선은 `#aab9cc` 회색 1px 실선이며, 상태와 시장·소스 값별 배경만 의미에 따라 달라진다.
- KOSPI/KOSDAQ/Korea ETF/미국 시장/crypto와 KIS/pykrx/yfinance/Upbit/Binance 식별색을 공용 계층에 추가했다.
- Playwright 5×2×22 검사에서 라이트·다크 계산 스타일이 전부 동일했고 흰 글자·비회색 외곽선은 0건이었다.
- production build, 최신 `release/LiveTrader.exe` 패키징, scale/click 계약과 `--help` 실행 검증이 통과했다.

## 2026-07-25 배지 팔레트 자동 회귀 CI

- 공용 Playwright 검사 `badge:check`가 다섯 앱 × 두 테마 × 22종 배지의 배경·글자·회색 실선·opacity·테마 동일성을 검사한다.
- 첫 실행에서 앱 로컬 CSS에 가려진 공용 글자색 불일치 40건을 차단했고, 공용 selector를 보강한 뒤 220/220을 통과했다.
- 현행 compact UI를 계약 버전 6과 30개 시각 기준으로 갱신했으며 최종 30/30, pixel diff 0.000%다.
- GitHub Actions가 두 검사를 자동 실행한다. 최신 LiveTrader EXE 패키징, scale/click 계약과 `--help` 검증도 통과했다.

## 2026-07-25 Binance 연속 감시·주문 경계 감사

- Binance 연속 실행은 시장가 주문을 사용하고 매수는 quote notional, 매도는 거래소 step size/min notional 규칙으로 정규화한다. Paper의 큰 replay 수량을 실주문에 재사용하지 않고 기본 실거래 크기를 5.5 USDT로 제한한다.
- v8에는 통과 Artifact가 없어 MONITOR/Shadow/Paper/Live 승급 요청과 신규 실주문을 만들지 않았다. 과거 실제 0.0001 BTC 체결은 커넥터 검증 증거로만 유지하며 새 전략의 성능 증거로 사용하지 않는다.
- 실제 연속 MONITOR에서 폐기된 v6 포트폴리오가 자동 선택되는 결함을 발견했다. 이제 Portfolio 구성 Strategy의 현재 배포 lifecycle과 Backtester 검증을 교차 확인해 paused/retired/미검증 구성요소를 자동 제외한다.
- SMALL_LIVE와 FULL_LIVE는 모든 구성 전략이 각각 해당 권한을 가진 경우만 선택되며, 명시한 Portfolio가 부적격이면 과거 단일 전략으로 대체하지 않고 즉시 차단한다.
- 실제 저장소에서 v6 자동 제외와 존재하지 않는 v8 Portfolio 명시 요청의 `running=false`를 확인했다. 최종 Live 회귀 테스트는 110개 모두 통과했다.

## 2026-07-25 실체결·안전 Telegram 알림

- KIS, Binance, Upbit execution stream에서 처음 들어온 `filled/done/executed` 이벤트만 Telegram으로 전송한다. 재시작이나 재동기화로 같은 체결을 다시 읽어도 broker event ID 기준으로 중복 발송하지 않는다.
- 알림에는 브로커·운용 모드, 종목·방향·수량·가격·주문금액, 전략·포트폴리오, 동기화 후 현금·평가액·포지션 전후, 수수료·주문 ID·체결 시각·Kill Switch 상태가 포함된다.
- Kill Switch, 심각 안전 경고, 포지션 불일치, 체결 스트림 이상도 별도 긴급 알림으로 보낸다. Telegram 네트워크 장애는 주문·감시 루프를 멈추지 않는다.

## 2026-07-26 장시간 감시·복구·Telegram P0 재검증

- 유휴 snapshot polling을 throttle하고 bounded audit/event 저장을 유지해 오래 켜 두어도 요청·메모리·로그가 무제한 늘지 않도록 했다.
- KIS/Binance/Upbit 연결과 사설 체결 스트림은 정상→장애→복구 전이에서만 Telegram을 보내며 같은 상태의 반복 경고를 억제한다.
- 체크포인트가 없거나 손상됐으면 시작 시 MONITOR, Dry Run, 신규 진입 차단으로 fail-closed하고 경고를 한 번만 보낸다.
- snapshot 응답은 계좌·서명 요청 자료를 마스킹하며, 실거래 주문은 현재 Artifact hash의 Forward evidence, 최신 재검증, Before Live-Small, 브로커 대조와 1회용 운용자 확인을 모두 요구한다.
- 최신 EXE 교체 전 `MONITOR + dry-run + 신규 진입 차단`, 주문 0건, continuous runtime 정지를 확인했다. 실행 중 감시는 유지한 채 `release_pending`에 빌드한 뒤 기존 EXE를 복구용으로 백업하고 교체했다.
- 최신 `release\LiveTrader.exe` SHA-256은 `0A99A70D439DF922A0879D3BBDD14030BE9AD3A4DAD07F635667311DB3DB0CDD`다.
- 재시작 뒤에도 MONITOR/Dry Run/신규 진입 차단/주문 0건을 유지했다. 20초 유휴 측정은 단일 코어 약 1.17%, working set 약 126.7MiB, memory·handle 증가 0이었다.
- Python unittest 137개, Node polling 회귀 1개, Vite/PyInstaller와 100/125/150% 데스크톱 계약이 통과했다.
- KIS, Binance, Upbit API 환경 변수는 존재하지만 `LIVE_TRADER_ENABLE_REAL_ORDERS=false`이고 현재 hash로 Before Live-Small에 도달한 새 전략이 없어 실주문은 정직하게 차단된 상태다.

## 2026-07-26 브로커별 주문 계약과 KIS 해외주식 원자 대조

- Binance는 현물 시장가 주문, Upbit는 원화 시장가 매수/수량 시장가 매도, KIS 국내는 시장가, KIS 미국주식은 최신 시세를 기준으로 한 지정가 계약을 각각 분리했다. 선택된 브로커·시장과 다른 주문형식은 실행 전에 차단한다.
- 계좌와 포지션 대조를 broker scope로 분리해 한 브로커의 미지원 기능이나 오류가 다른 브로커의 정상 감시를 오염시키지 않는다.
- KIS 해외주식은 공식 `TTTS3012R` 잔고 API를 사용하고 NASD/USD 전체 미국시장 조회와 연속조회 pagination을 처리한다. 국내·해외 중 하나라도 부분 응답, 반복 continuation key, 인증/API 오류가 나면 합쳐진 snapshot 전체를 fail-closed한다.
- 새 실행 파일에서 읽기 전용 실제 대조를 수행해 계좌 3개와 포지션 7개, 총 10개 항목이 모두 일치했다. capability gap, mismatch, blocking issue, API required 항목은 각각 0건이며 주문 제출이나 계좌 변경은 수행하지 않았다.
- Live Trader는 계속 `MONITOR`, dry-run, 운용자 미확인, 신규 주문 0건이다. 사용자의 자금 사용 승인은 주문 한도 권한이지만 Forward Shadow/Paper 승급 근거를 생략하는 권한은 아니므로, 실제 시간으로 쌓여야 하는 증거가 부족한 canonical 전략은 실거래로 올리지 않았다.
- 최신 `release\LiveTrader.exe` SHA-256은 `C22DBDC0697B54203329A72AA114BC91DD802AC83A341B3DC3A54AFE4798F414`다. 재시작 뒤 실제 잔고 대조와 10개 화면 UI smoke, polling 회귀, Python 153개 테스트가 통과했다.

## 2026-07-26 Live 승급·연속 실행 중지 안전성 통일

- Before Live-Small에서 Live로 승급할 때 주문 전송 성공이 아니라 공용 정책의 실제 canary 체결 3건을 요구한다. 세 체결은 서로 다른 broker ledger fill이고 non-dry, 양수 수량, broker order/event ID를 가지며 현재 Strategy/Artifact ID·artifact/content hash·deployment ID/revision과 Before Live-Small 진입시각 뒤의 경계가 정확히 일치해야 한다. pause/resume 전 과거 체결은 인정하지 않으며 1~2건은 사유와 현재 건수를 표시하고 차단한다.
- start/set_mode/stop은 하나의 공개 제어 잠금 순서를 사용한다. continuous runtime 중지는 엔진을 잠금 안에서 MONITOR로 전환한 뒤 잠금을 풀고 worker를 join하므로 due-bar flush가 operation lock을 기다리는 순간에도 교착하지 않는다.
- supervisor가 `FAILED` 또는 `runtime-stop-timeout`을 반환하면 controller, 다중 profile manager, API 응답까지 `ok: false`를 보존한다. 아직 살아 있는 실패/timeout thread를 STOPPED로 덮거나 새 supervisor로 교체하지 않으며, 전역 profile은 fail-closed MONITOR로 동기화한다.
- KIS/Binance/Upbit 주문 adapter와 place/cancel 라우팅은 구현되어 있다. 현재 `live_order_adapter_ready=false`인 직접 원인은 별도 배선 누락이 아니라 `LIVE_TRADER_ENABLE_REAL_ORDERS=false` 안전 플래그이며 이 작업에서는 변경하지 않았다.
- 소스 안전성 수정 뒤 Live Trader unittest 163개와 공용 runtime 155개가 통과했다.

## 2026-07-26 최종 실행 파일 교체와 실제 계좌 재대조

- 최신 소스로 Vite production build, 100/125/150% 데스크톱 계약, PyInstaller 패키징과 `--help` 기동 smoke를 통과했다.
- 교체 직전 기존 프로그램이 `MONITOR`, Dry Run, 운용자 미확인, 주문 큐 0건임을 확인하고 이전 EXE를 `release_backup`에 보존한 뒤 새 빌드로 교체했다.
- 최신 `release\LiveTrader.exe`는 19,421,996 bytes이며 SHA-256은 `E871160921BAC9E1EECF41753CB81B1275F28CDBCDF8B51B3BC4727CF085AF9B`다.
- 재기동 뒤에도 `MONITOR`, Dry Run, 운용자 미확인, Kill Switch OFF, 주문 0건을 유지했다. 읽기 전용 실제 브로커 대조를 다시 실행해 KIS·Binance·Upbit 계좌 3개와 포지션 7개가 모두 PASS였고 API 오류·불일치·차단 항목은 0건이었다.
- Node polling 회귀와 Live Trader unittest 163개가 통과했다. 실제 주문은 현재 Artifact와 정확히 일치하는 자연 Shadow/Paper 및 canary 체결 근거가 부족해 생성하지 않았다.

## 2026-07-27 daemon lease·예약 작업·실제 계좌 재검증

- daemon은 시작 즉시 PID, heartbeat lease, `STARTING`을 기록하고 startup 결과에 따라 `RUNNING` 또는 `DEGRADED`를 기록한다. 정상 종료는 `STOPPED`, PID가 없거나 lease가 만료된 기록은 조회 시 `STALE`, `running=false`로 원자 정정한다.
- 서버 시작 시에도 daemon 상태를 reconciliation하므로 죽은 프로세스가 과거 `RUNNING` 파일만 남겨 정상으로 보이는 문제를 막았다. 전용 테스트 4개를 추가해 정상 heartbeat, heartbeat timeout, dead PID, 정상 종료를 검증했다.
- 최신 EXE로 `TradingSystem-LiveTrader-Monitor` 예약 작업을 다시 설치했다. 30초 주기를 넘겨 PID가 살아 있고 heartbeat, execution poll, crypto runtime이 연속 갱신되는 것을 확인했다. stock profile은 실행 가능한 Portfolio/Strategy Artifact가 없어 `STOPPED`, 전체 daemon은 정직하게 `DEGRADED`로 표시된다.
- 패키지가 저장된 KIS·Binance·Upbit 인증정보를 정상 인식했다. 실제 읽기 전용 대조에서 계좌 3개·포지션 7개가 모두 일치했고 API 필요·불일치·조회 오류는 0건이었다. 주문/취소는 호출하지 않았고 `LIVE_TRADER_ENABLE_REAL_ORDERS=false`, MONITOR 안전 잠금은 유지했다.
- 전체 unittest 167개, Node polling, 5개 탭 × 2개 viewport UI smoke, production build와 실제 Windows 패키지를 확인했다. 최신 `release\LiveTrader.exe`는 19,425,916 bytes, SHA-256 `CF6D09CC9601D303C84101CFF98F87C1EE1104DB065E87D3FCBA098B2A85B135`다.

## 2026-07-27 daemon HTTPS read timeout 복구

- Windows의 `Unhandled exception in script` 창은 Live Trader 예약 작업이 브로커 HTTPS 응답 본문을 읽던 중 `TimeoutError: The read operation timed out`을 처리하지 못해 발생했다. 상태는 `STOPPED`였지만 PyInstaller 오류 창을 기다리는 자식 프로세스가 남아 작업 스케줄러만 `Running`으로 보였다.
- `http_json()`은 `urlopen()` 연결 뒤 `response.read()`에서 발생하는 bare `TimeoutError`도 네트워크 실패 응답으로 변환한다. 브로커 폴링은 어댑터별 예외를 격리해 한 브로커 장애가 전체 감시를 종료하지 않는다.
- daemon은 체결 스트림·profile runtime 시작, 주기적 체결 폴링, snapshot, 종료 작업을 안전 경계로 감싼다. 일시 실패 시 프로세스를 종료하지 않고 `DEGRADED`와 오류 유형을 기록하며 다음 주기에 실패한 시작만 자동 재시도한다. 예기치 않은 최상위 오류도 PyInstaller 대화상자로 빠뜨리지 않고 `FAILED`, 종료 코드 1로 기록해 예약 작업 재시작 정책에 맡긴다.
- 상태 파일에는 계좌·포지션·인증정보를 복사하지 않고 브로커명과 최대 500자의 오류 상세만 남긴다. 회귀 테스트로 응답 읽기 timeout, poll 생존, startup retry와 민감정보 제외를 검증했다.
- 전체 Live Python unittest 171개와 Node polling 회귀가 통과했다. 저장된 인증정보로 KIS·Binance·Upbit 읽기 전용 조회가 모두 성공했고 계좌 3개, 포지션 4개를 확인했다. 실제 주문·취소는 호출하지 않았다.
- 최종 예약 작업은 `Running`, PID 생존, 오류 창 없음, 마지막 poll 오류 0건, KIS/Binance/Upbit execution stream 연결, crypto runtime `RUNNING`이다. 전체 `DEGRADED`는 실행 가능한 stock Strategy/Portfolio Artifact가 없기 때문이며 timeout 장애와는 무관하다.
- production build, 100/125/150% desktop-scale, PyInstaller와 `--help` 기동 smoke를 통과했다. 최신 `release\LiveTrader.exe`는 19,428,106 bytes, SHA-256 `D4FC5DE54FC5F69577E39E830FC68028527EC9DAD547EB051A302CD2B7B6E292`다.

## 2026-07-28 Windows 소켓 10013 시작 오류 복구

- Live Trader 실행 직후 발생한 `[WinError 10013] 액세스 권한에 의해 숨겨진 소켓에 액세스를 시도했습니다`를 동일 PC에서 재현했다. 기존 기본 포트 `127.0.0.1:8795`만 바인딩이 거부됐고 인접 포트와 운영체제 자동 할당 포트는 정상 동작했다.
- 이 PC의 TCP 동적 포트 범위는 `1024-15000`으로 기존 `8795`를 포함한다. Live Trader 기본 포트를 현재 동적·제외 범위 밖에서 실제 바인딩을 확인한 `18795`로 옮기고 공용 앱 매니페스트와 UI smoke 주소도 함께 갱신했다.
- 데스크톱 실행기는 기본 포트가 Windows 권한 거부(10013) 또는 이미 사용 중(10048)이어도 종료하지 않는다. 해당 경우 `port=0`으로 한 번 재시도해 운영체제가 안전한 로컬 포트를 정하고, WebView에는 실제 바인딩된 포트의 URL을 전달한다. 관계없는 소켓 오류는 숨기지 않고 그대로 실패시킨다.
- 실제로 막힌 `8795`를 요청한 통합 smoke에서 자동 포트 `6420`으로 전환된 뒤 `/api/snapshot`이 HTTP 200을 반환했다. Live Trader 전체 Python unittest 176개가 통과했다.
- Vite production build, 100/125/150% desktop-scale 계약과 PyInstaller 패키징을 통과했다. 최신 `release\LiveTrader.exe`를 `TELEGRAM_ENABLED=false`, `LIVE_TRADER_ENABLE_REAL_ORDERS=false`로 실행해 `18795`의 API 응답과 `MONITOR` 모드를 확인했다.
- 검증용 LiveTrader PID 2개는 모두 종료했다. 최신 EXE는 19,444,188 bytes이며 SHA-256은 `E39E37DE5052F7BDE3A99B131B8101E76E9106EF4C370FE9DE940BEDDF618650`이다.

## 2026-07-28 중첩 탭 카드 규칙 적용

- 실거래 준비의 자산 선택, 연결 브로커 선택, 자동화 자산 선택을 공용 `NestedTabs` 카드형으로 명시해 다른 앱의 실제 작업 영역 전환과 같은 시각·클릭 규칙을 사용하도록 했다.
- 전체 Python unittest 176개, Vite production build, 100/125/150% 화면 배율·클릭 계약과 PyInstaller 패키징이 통과했다. 실주문과 Telegram은 비활성화했다.
- 최신 `release\LiveTrader.exe`는 19,445,350 bytes, SHA-256 `9AAC9A54296D0BB3828EF21A0B1E5E3D1D25CC7F2466CA23D774F6186C1572C5`다.

## 2026-07-28 휴지통 아이콘 공통 스타일

- 저장된 전략 검색 조건을 지우는 아이콘 전용 버튼에 공용 `trash-icon-button`을 적용했다. 수집기·Paper Trader와 같은 빨간 배경, 회색 외곽선, 흰색 아이콘을 사용한다.
- Vite production build, 100/125/150% 화면 계약, PyInstaller 패키징과 Telegram·실주문 비활성 `--help` 기동 smoke가 통과했다.
- 최신 `release\LiveTrader.exe`는 19,443,640 bytes, SHA-256 `7A8E75ECEECA7312A1CF492C1C93C3B15E10FC465D1374AEA3C5574F282A8F92`다.

## 2026-07-28 Telegram 실연결 상태 표시

- 설정 화면에 공용 Telegram 연결 상태 패널을 추가했다. 화면 진입 시 한 번 자동 확인하고 `다시 확인`으로 즉시 재검사할 수 있다.
- 확인은 저장된 bot token과 chat ID로 Telegram `getMe`, `getChat`만 호출한다. 테스트 메시지나 거래 알림을 보내지 않으며 오류 메시지에서도 토큰을 제거한다.
- 실제 저장 설정으로 봇 계정과 대상 채팅 접근을 확인했고, 브라우저 콘솔 오류 없이 `연결 정상` 상태가 표시됐다.
- Telegram runtime unittest 10개, Live Trader Telegram unittest 12개, Vite production build와 실제 브라우저 화면 검증을 통과했다.
- 최신 `release\LiveTrader.exe`는 19,445,286 bytes이며 SHA-256은 `A0707D133E5EE2F54CC97EA684D736CD2E4097C8AF0CF5F0FC8C62A3E18E4E66`다.

## 2026-07-28 Telegram 네트워크 복구·알림 정책 전문화

- 공용 상태 구성요소가 실제 `offline → online` 복구를 관찰했을 때만 Telegram 연결 검사를 한 번 자동 재실행한다. 중복 online 이벤트와 진행 중 검사와의 중복 호출은 차단한다.
- Bot·채팅 연결과 Live Trader 알림 활성화 여부를 분리해, 연결은 정상이지만 알림이 꺼진 경우 상단 pill과 상태 상자를 `MUTED` 경고 상태로 표시한다.
- 공통 마스터 알림 스위치가 앱별 설정보다 우선하도록 runtime 정책을 바로잡고, 연결 응답에서 불필요한 Bot/Chat 숫자 ID를 제거했다.
- Live Telegram 12개 테스트, Vite production build, PyInstaller 패키징과 Telegram·실주문 비활성 `--help` 기동 smoke가 통과했다.
- 최신 `release\LiveTrader.exe`는 19,448,453 bytes이며 SHA-256은 `87E55FCC8287DC1603FD939264783AF3FC058F4A0BA289BDB1AB3C27CF5069D1`다.

## 2026-07-29 직접 숏·인버스·provider 실거래 게이트

- Live Trader가 공용 노출 계약을 강제한다. 인버스 ETF는 현물 long만 허용하고, 직접 숏은 margin/futures 가능 경로와 short 주문이 함께 충족되어야 한다.
- 연구 데이터가 Yahoo/PyKRX인 전략은 KIS 봉 대조 결과가 없거나 기준을 통과하지 못하면 Live 승급과 실주문을 차단한다. Paper 운용은 계속 가능하다.
- Binance/KIS/Upbit와 Telegram 보호 설정을 공용 Windows 사용자 범위 암호화 저장소에서 읽고, 기존 `.env` 평문은 마이그레이션 뒤 제거한다.
- 외부 artifact 저장소, golden 전략 계약과 live gate 회귀를 포함한 Live unittest 193개, frontend build와 PyInstaller 패키징을 통과했다.
- 최신 실행 파일은 Telegram·실주문 비활성 `--help` 기동 smoke를 exit 0으로 통과했다. `release\LiveTrader.exe`는 19,513,445 bytes, SHA-256 `192F46B57A3DA7B550E631FEC86C6ACEC5FB75E9B4253D72842989FF43E7D96C`다.

## 2026-07-30 승급 준비 큐·확정 봉 카운트다운·5시간 무인 Soak

- 전략·포트폴리오 화면에 공용 `promotion-readiness` 판정을 연결해 현재 단계, 다음 단계의 필수 근거, 완료 수, 남은 수와 차단 사유를 보여 주는 `승급 준비 큐`를 추가했다. 실제 승급 정책은 바꾸지 않았고, 브로커가 최종 확인한 non-dry `FILLED`만 canary 체결 수로 인정한다.
- 주식/ETF와 코인 지속 감시에 시장·주기·마지막 확정 봉 기준 `다음 확정 봉까지 남은 시간`을 표시했다. UI 카운트다운은 미완성 봉을 평가하지 않으며 전략 엔진은 기존처럼 확정 봉마다 정확히 한 번만 판단한다.
- 자동화 화면에 5시간 무인 Soak 상태와 최신 보고서를 표시한다. 보고서는 heartbeat, 프로필 상태, 재연결, 확정 봉·판단·주문·체결·차단·오류, 최대 CPU·메모리, 최종 포지션과 일일 리스크를 SQLite 우선으로 집계하고 JSON·HTML을 원자 저장한다.
- 중간의 DEGRADED/FAILED/CRASHED/STALE 상태를 회복 뒤 숨기지 않도록 비정상 구간을 latch한다. MONITOR Soak는 실제 주문 0건이어야 하며, 5시간을 모두 채우기 전에는 최종 PASS가 아니다.
- Windows에서 살아 있는 daemon을 `os.kill(pid, 0)`으로 오판하던 부분을 Win32 process handle 기반 조회로 교체했다. 자동 시작은 오래된 부적격 Portfolio를 건너뛰고, 유효한 backtested 단일 전략이 있으면 MONITOR 감시를 시작하되 SMALL/FULL LIVE 권한을 만들지는 않는다.
- 현재 주문 없는 5시간 MONITOR Soak가 stock·crypto 두 프로필로 진행 중이다. real-order 환경은 꺼져 있으며, 중간 상태가 정상이어도 종료 보고서가 나오기 전에는 완료로 기록하지 않는다.

## 2026-07-30 계좌 기준 리스크 예산과 Binance USD-M Futures 체결 검증기

- 1회 주문 상한은 고정 숫자가 아니라 주문 직전 Binance USD-M Futures의 사용 가능 잔액 전부를 상한으로 계산한다. 상한이 전액이라는 뜻이지 매 주문마다 전액을 소진한다는 뜻은 아니며, 연결 검증 주문은 거래소 최소 계약을 만족하는 작은 수량을 우선한다.
- 일일 손실 한도는 로컬 거래일 시작 equity 대비 `-10%`로 영속 저장한다. 프로세스를 재시작해도 당일 기준 equity, 실현·미실현 손익과 차단 latch를 복원하고, 한도 도달 뒤에는 당일 신규 진입을 fail-closed한다.
- 전용 Futures 체결 검증기는 5시간 동안 ETHUSDT 최소 주문을 사용해 진입·청산 3회 왕복, 즉 최종 포지션을 0으로 끝내는 브로커 확인 체결 6건을 목표로 한다. 정확히 3건만 체결해 포지션을 남기는 방식은 사용하지 않는다.
- 실행 전 읽기 전용 Preview가 계정 권한, hedge mode, `ISOLATED 1x`, 사용 가능 잔액, 기존 포지션·미체결 주문, 최소 수량과 일일 손실 예산을 다시 확인한다. Preview token은 짧은 만료시간·1회 사용이며 실제 시작 전에 UI의 별도 명시 확인이 필요하다.
- 매 주문 뒤 브로커 주문 상태와 포지션을 순차 대조하고, 하나의 주문·포지션만 허용한다. 정지·오류 때는 세션이 만든 주문과 추적 수량만 취소·청산하며 다른 포지션을 건드리지 않는다. 최종 감사 JSON은 SHA-256으로 봉인하지만 전략 성과나 승급 근거로 사용하지 않는다.
- 현재 실제 체결은 실행하지 않았다. USD-M Futures의 주문 가능 잔액과 `ISOLATED 1x` 전제조건이 충족되지 않아 Preview가 안전 차단 중이며, Spot 자산 이체나 margin/leverage 변경을 프로그램이 임의로 수행하지 않았다.
- 관련 상태·Preview·Start·Stop·최신 Soak 보고 API와 실거래 준비 UI를 추가했다. 격리된 Live Python 회귀 275개, 공용 runtime 194개, 공용 design 테스트 9개와 Vite production build가 통과했다.

## 2026-07-31 Binance 자금 이체·선물 종목 설정 안전 워크플로

- 사용자 명시 요청으로 Binance Spot의 USDT 10을 USD-M Futures로 정확히 한 번 이체했다. 이체 전 Spot 23.8049427 USDT/Futures 0 USDT, 이체 후 Spot 13.8049427 USDT/Futures 10 USDT를 읽기 전용으로 재대조했다. 이체 뒤에도 선물 포지션과 미체결 주문은 각각 0건이다.
- ETHUSDT의 실제 현재 설정은 `CROSSED · 20x`다. `CROSSED`는 여러 포지션이 선물 지갑 증거금을 공유하는 마진 방식이고 `20x`는 초기 레버리지 배수이므로 서로 다른 설정이다. 20x만 낮춰도 CROSSED가 ISOLATED로 바뀌지는 않는다.
- 실거래 준비 화면에 종목별 Binance USD-M 증거금 설정 패널을 추가했다. 레버리지는 퍼센트가 아니라 1x·2x·3x·5x 안전 프리셋으로 표시하고, 손실 위험률은 주문 크기·손절 거리 정책과 별도임을 명시한다. 초기 버전은 `ISOLATED`만 적용할 수 있다.
- 변경 Preview는 계정 거래 권한, 현재 마진/레버리지, 전체 포지션과 미체결 주문을 새로 조회한다. 포지션·주문이 0일 때만 5분짜리 1회 토큰을 발급하며, Apply 직전에 같은 조건을 다시 조회한다. 상태 변경 POST는 응답 timeout 때 자동 재전송하지 않고, 적용 뒤 목표값과 flat 상태를 다시 조회해 확인한다.
- 화면을 열거나 Preview만 실행해서는 계정 설정을 바꾸지 않는다. 이번 작업에서도 ETHUSDT의 실제 `CROSSED · 20x`는 그대로 유지했다. 기존 실체결 Soak는 별도 안전 계약인 `HEDGE · ISOLATED · 1x`를 계속 강제한다.
- 실계좌의 영속 계좌 위험값이 unittest의 가정값을 오염시키던 테스트 비결정성을 제거했다. 주문 게이트 테스트는 시작 시 실제 계좌 snapshot을 비우고 종료 시 원본을 복원한다.
- 관련 안전 게이트·단일 전송·적용 후 재검증 테스트를 포함해 전체 Python unittest 283개가 통과했다. Vite production build와 100%·125%·150% 데스크톱 배율 계약, PyInstaller 패키징도 통과했다.
- 최신 `release\LiveTrader.exe`는 19,717,111 bytes이며 SHA-256은 `03F01DD892BA9E13E502736E79186375557DD5C97D6BCF4CEE8BD9A313C75D50`다.
- 이전 5시간 MONITOR Soak는 목표시간 18,000초를 채웠고 실주문·차단·heartbeat gap·손실 게이트 발생은 0이었다. 다만 KIS 읽기 연결이 약 34초간 1회 끊겼다가 자동 복구되어 엄격 정책상 최종 FAIL이다. 주문/포지션 모호성 사고는 아니며, 보고서는 `logs\reports\soak\20260730T043844Z-f078639b7e.json`과 같은 이름의 HTML이다.

## 2026-08-01 Live 운영센터 전면 개편과 실거래 테스트 경로 완성

- 화면은 `운영 현황`, `배포·승급`, `계좌·포지션`, `주문·체결`, `리스크·안전`, `실거래 운영`, `감사 기록`, `기술 로그`, `설정·진단`의 9개 운영 화면으로 구성한다. 상단에는 LIVE 환경 바와 계정·Deployment·Session 컨텍스트를 고정한다.
- Live의 중심 단위를 개별 전략 체크가 아니라 `Deployment Manifest → 만료되는 Preflight Snapshot → Runtime Session → Order/Fill/Audit Event`로 고정했다. Manifest에는 Portfolio와 모든 Strategy member, 허용 심볼, 단일 Broker route, 정책·Artifact hash를 묶으며 실행 중 다른 Deployment나 심볼로 컨텍스트가 섞이면 최종 전송 단계에서도 차단한다.
- 최종 Preflight는 선택한 Deployment와 계정만 대상으로 강제 REST Snapshot, 체결 Event poll, 로컬 Ledger를 3자 대조한다. 결과는 60초 안의 신선한 값만 인정하며 누락·오래됨·오류·불일치·활성 또는 결과 불확실 주문이 하나라도 있으면 위험 증가 주문을 차단한다. 다른 Broker의 오류는 현재 Deployment를 오염시키지 않고, 여러 Broker가 섞인 Portfolio는 다중 계정 Preflight가 생기기 전까지 차단한다.
- `0`과 `조회 불가`를 분리하고 시장 데이터 신선도는 실제 Event/확정 봉 시각으로 판정한다. 현재 모드에서 쓰지 않는 경로는 녹색이 아니라 `해당 없음`으로 표시한다. Risk 화면은 현재 사용량과 Soft Warning/Hard Block을 함께 보여 주며 조회 요청과 주문 요청의 재시도 정책을 분리했다.
- 주문은 생성·Risk 검증·전송 중·ACK·부분 체결·체결·취소 확인 중·취소 결과 불확실·거부 상태를 보존한다. 결정적 `client_order_id`와 `cancel_request_id`를 사용하고, 주문/취소 결과가 불확실하면 재전송하지 않고 Broker 조회와 대조를 먼저 수행한다. 취소와 체결이 경쟁하면 체결 사실을 최종 진실로 유지한다.
- Reduce-only는 화면이나 전략이 보낸 주장만 믿지 않는다. 60초 이내의 성공한 해당 Broker·심볼 포지션 Snapshot과 방향·수량을 다시 검증해 포지션 절대값을 줄이면서 0을 넘지 않는 주문만 허용한다. Kill Switch는 신규 주문 차단, 알려진 미체결 취소 요청, Runtime 정지를 수행하되 포지션 강제 청산과 분리했다.
- 감사·사고 저장소는 append-only SQLite로 구성하고 사고 확인·완화·해결 이력을 남긴다. 중앙 마스킹이 API Key, Bearer/JWT, Telegram Bot URL과 실제 환경 Secret을 메모리 로그·SQLite·Telegram 전송 전에 제거한다. Windows 보호 저장소를 계속 사용하며 화면에는 Secret 식별 상태와 마지막 검증만 표시한다.
- 사용자의 허용에 따라 로컬 `.env`의 `LIVE_TRADER_ENABLE_REAL_ORDERS=true`를 유지했다. 실제 테스트는 정확한 Deployment 선택 → 운용자 확인 → Dry Run 해제 → 신규 진입 허용 → 새 Preflight PASS → Canary 전환 순서로 가능하다. 앱 시작만으로 주문이 나가지는 않으며 이번 작업의 자동 테스트에서는 Broker를 모의 처리해 실제 주문·취소·자금 이동을 한 건도 보내지 않았다.
- Python 전체 회귀 387개, 계좌·시각화·polling·API 복원력·Futures Risk 프런트엔드 회귀, Workspace model 테스트와 production build가 통과했다. 9개 화면을 100%, 125%, 150%, compact desktop의 36개 조합으로 검사해 문서/작업영역 overflow, 화면 밖 조작 버튼, 콘솔 오류가 모두 0이었다.
- 최신 `release\LiveTrader.exe`는 `--help` 안전 기동이 종료 코드 0으로 통과했다. 크기는 19,924,832 bytes이며 SHA-256은 `396895A11EE8954C3EE9CD760AAA0FCFB223C0B9BF7A3590DEB4009D04A1F110`이다.
- 이번 단계에서 물리적으로 분리된 Windows Trading Engine/Risk Gateway/Watchdog 서비스, 원격 읽기 전용 Dashboard와 TOTP, 자동 자본 확대·Rollback, 실자금 장애 Drill, Broker 간 통화 정규화 성과 보고서는 만들지 않았다. 각각 별도 서비스 배포·외부 인증·실자금 승인·완전한 통합 원장 정책이 먼저 필요한 기능이며, 현재 데스크톱 앱에는 동일 목적의 논리적 Risk/Watchdog/감사 경계를 유지했다.

## 2026-08-03 상태 카드 외곽선 통일과 Deployment 선택 안정화

- 사고·감사 화면의 사고 카드와 배포·승급 화면의 Portfolio Artifact·승급 준비 카드는 색상 왼쪽 띠를 제거했다. Stock Data Scraper와 Backtester의 일반 카드처럼 회색 1px 사방 외곽선과 중립 배경을 사용하고, 위험 의미는 카드 안의 상태 배지와 문구가 담당한다.
- 배포·승급 화면에서 현재 Deployment가 Portfolio와 단일 Strategy 사이를 계속 왕복한 원인은 전역 `selectedStrategyId`와 패널 내부 `assetTab`을 동기화하는 두 effect가 서로 상대 선택을 덮어쓰던 순환이었다.
- 현재 Deployment를 controlled source of truth로 고정했다. 자산 탭은 목록 탐색만 바꾸며, 운용자가 Deployment select나 전략 행을 명시적으로 고를 때만 실제 Deployment가 바뀐다. controlled 선택을 찾을 수 없을 때 임의의 첫 전략으로 되돌리는 동작도 차단했다.
- `scripts/ui_smoke.mjs`에 운용자 입력 없는 Deployment 6회 연속 표본의 불변성 검사와 사고·Artifact·승급 카드의 계산된 사방 테두리 일치 검사를 추가했다. 레이아웃 스모크는 현재 버튼명인 `레이아웃 편집`을 사용하도록 갱신했다.
- Vite production build, workspace model·polling·계좌·계좌 시각화·API 복원력·Futures Risk·레이아웃 회귀와 4개 화면 크기 × 9개 탭 UI smoke 36/36을 통과했다. 실제 브라우저에서도 Deployment 12회 표본이 한 값으로 유지되고 대상 카드의 네 변이 모두 동일한 회색 1px임을 확인했다.
- PyInstaller로 `release\LiveTrader.exe`를 재생성했다. 파일 크기는 19,929,242 bytes, SHA-256은 `2CD7FBDC889C844E4978962DEADC30C1636D13EDEB659A7A737CDDCD415F971E`이다.
- 새 unsigned one-file의 최초 기동이 `_MEI` 추출 전 한 차례 지연됐지만 이후 `--help`는 약 2.6초에 exit 0으로 종료했다. 격리된 APPDATA에서 실주문·Telegram을 끄고 최신 EXE를 실행해 18795 API와 `/api/snapshot`의 `MONITOR`, `real-orders=false`, `Telegram=false`를 확인했으며, 이 실제 패키지를 대상으로 한 UI smoke도 36/36을 통과했다. 별도 새 one-file의 최초 기동은 5.06초에 정상 종료해 재현 가능한 소스·패키징 결함은 확인되지 않았고, 최초 지연은 Windows의 새 unsigned 파일 검사 가능성이 높은 환경성 현상으로 기록했다.
