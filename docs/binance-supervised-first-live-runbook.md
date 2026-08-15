# Binance Spot 감독형 2시간 기능시험 runbook

이 문서는 `BTCUSDT` Spot의 1회·10 USDT 상한 기능시험만 다룬다. 출금,
Universal Transfer 실행, Margin, Futures 주문·포지션 변경은 모든 단계에서
금지한다. 시험 결과는 `SUPERVISED_NON_PROMOTION`이며 production promotion이나
`REAL_E2E` 증거로 사용할 수 없다.

## 현재 결론: HOLD (실거래 미실행)

2026-08-15 현재 2시간 시험은 시작하지 않았다. 아래 compile-time fence가 모두
`false`이며, 어떤 설정 파일 생성·observer spawn·broker network/order보다 먼저
fail closed한다.

- `SUPERVISED_NON_PROMOTION_RELEASED = False`
- `SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED = False`
- `PREARMED_OBSERVER_LAUNCH_RELEASED = False`
- `BINANCE_OBSERVER_AUTHORITY_NETWORK_RELEASED = False`
- `BINANCE_SPOT_FUNCTIONAL_GET_NETWORK_RELEASED = False`
- `BINANCE_SPOT_SUPERVISED_GET_NETWORK_RELEASED = False`
- Binance root integration, transfer evidence 및 관련 production release fence

따라서 이 문서에서 유일하게 지금 실행 가능한 provisioning 명령은 plan-only다.
`-Apply`, Scheduled Task 활성화, release 상수 변경, observer 직접 실행, broker
POST/DELETE는 금지한다. 특히 release 상수만 수동으로 바꾸는 것은 승인 절차가
아니다.

## 지금까지 확인된 GET-only 사실

승인된 진단에서 공식 origin `https://api.binance.com`만 사용했다.

- signed/public GET 11회, redirect 0, retry/time-sync 0
- 주문 POST 0, 취소 DELETE 0, transfer/withdraw/margin/futures mutation 0
- account `canTrade/canDeposit/canWithdraw=true`, account-wide open order 0
- recent order 3건 모두 `FILLED`; recent trade 7건(BUY 2/SELL 5)
- API restriction: reading/Spot trading/IP restriction true, trading locked false
- key에는 withdrawal/margin/futures 권한도 있으나 이 시험에서는 전부 fence한다.
- 과거 `MAIN_UMFUTURE` 10 USDT Spot→USD-M transfer exact match 1건이 있다.
- BTCUSDT는 market `quoteOrderQty` 허용, minNotional 5 USDT,
  min/step `0.00001000`; 관측가 `63015.88`에서 보수적 10 USDT 수량
  `0.00015`가 당시 filter를 통과했다.

이 진단은 주문 권한이나 시작 승인이 아니다. 새 session에서는 독립 observer가
Spot user-data stream을 먼저 구독한 뒤 signed GET 3종(`apiRestrictions`,
`apiTradingStatus`, account-wide `openOrders`)을 다시 확인해야 한다.

## canonical trader와 보호 observer 경계

trader의 canonical 후보는 source다. 기존 `release\LiveTrader.exe`는 이번
transitive frozen hash로 재빌드되지 않았으므로 사용하지 않는다. 다만 현재는
HOLD이므로 다음 명령도 실제 2시간 시험을 시작하는 용도로 실행하지 않는다.

```powershell
cd D:\github\PROGRAM\trading-system\apps\live_trader
.\.venv\Scripts\python.exe -m live_trader
```

trader는 현재 사용자 DPAPI secret store를 사용한다. API key/secret을 CLI,
환경 변수, 로그, 임시 파일에 넣지 않는다.

독립 observer는 repo source나 repo `.venv`에서 실행하지 않는다. 최종 경로는
보호된 다음 bundle과 SYSTEM pre-armed service뿐이다.

```text
D:\crypto-first-live-authority\app\broker_authorities\binance\runtime
D:\crypto-first-live-authority\venv
Scheduled Task: CryptoFirstLive-BinanceObserver
Pipe: \\.\pipe\crypto-first-live-binance-observer-launch
```

보호 bundle은 full SHA-256 manifest, no-extra/reparse 검사, SYSTEM/Admin 전용 ACL,
authority/trader SID, server/daemon/Python pin을 검증한다. credential은 현재 사용자
DPAPI에서 승인된 rewrap 도구가 메모리 내에서 LocalMachine DPAPI envelope로
변환해야 하며 raw secret CLI/env/temp 파일을 만들면 안 된다. observer는 manifest
및 envelope/account fingerprint를 다시 검증한 뒤에만 메모리로 복호화한다.

## 현재 허용된 provisioning: plan-only

전용 빈 private Git repository를 준비한 뒤 다음 명령은 계획만 검증한다.

```powershell
pwsh.exe -NoProfile -File D:\github\PROGRAM\trading-system\apps\live_trader\scripts\provision_crypto_first_live_supervised_git_authority.ps1 `
  -GitHubRepository <DEDICATED_EMPTY_PRIVATE_OWNER_REPO> `
  -TraderDataRoot D:\github\PROGRAM\trading-system\apps\live_trader\data `
  -TraderOsSid S-1-5-21-4199057202-2653993499-446139946-1001
```

기대 결과는 `applyReleased=false`, `brokerNetworkReleaseAllowed=false`,
`mutationPerformed=false`다. descriptor
`scripts\crypto_first_live_supervised_broker_bundle.template.json`도
`readyForApply=false`여야 한다. 현재 `-Apply`는
`protected-bundle-provisioning-release-held`로 config/copy/task/network 전에
실패해야 한다. 이를 우회하지 않는다.

예전 `scripts\provision_binance_supervised_observer.py`로 repo-writable authority를
만들거나 `scripts\binance_supervised_observer_daemon.py`를 직접 실행하는 방식은
실거래에 사용할 수 없다. planned session/PID를 static pinned config에 복사하거나
prepared epoch 뒤 5초 안에 사람이 명령을 복사하는 절차도 금지한다.

## 해제 전 남은 P0

별도 승인·코드리뷰·재검증 없이는 다음을 해제하지 않는다.

1. 보호 bundle descriptor의 exact source pins와 machine credential artifact를
   동결하고 `readyForApply`를 승인된 한 tranche에서만 전환한다.
2. SYSTEM authority가 prepare 전에 pipe를 listen하고, authenticated pipe peer의
   PID/SID 및 그 PID의 command SHA를 request와 동적으로 결합한다.
3. observer child가 network 0인 pre-armed ready 상태가 된 뒤 ACK를 보내고,
   ACK 뒤에만 exact gate로 WS+3 GET을 시작하는 순서를 race 없이 증명한다.
   현재 gate-file freshness 계약은 완결되지 않았으므로 HOLD다.
4. root/state의 durable `APPROVED_INERT` heartbeat와 별도
   `activate_supervised_non_promotion` receipt를 Binance prepared lifecycle에 exact
   schema로 연결하고 restart/one-use/CAS hostile tests를 통과한다.
5. historical 10 USDT transfer mismatch를 fresh truth 및 detached GET receipt에
   묶어 durable ledger에서 한 번만 재분류한다. 새 transfer는 실행하지 않는다.
6. native pywebview session/CSRF UI에서 status/start/stop/recover와 typed one-use
   challenge를 검증한다. `curl`, DevTools token 복사, 외부 browser 우회는 금지다.

## 승인 후의 exact 운영 순서 (현재 실행 금지)

모든 P0가 닫히고 별도 one-use 사용 승인을 받은 release build에서만 아래 순서를
따른다.

1. 보호 authority/SYSTEM service가 먼저 pipe를 listen한다. 이 시점 broker
   GET/order/cancel/transfer/withdraw/margin/futures mutation count는 모두 0이다.
2. native UI가 exact planned session, permit, account/code/baseline, 7200초,
   BUY 1회, 10 USDT 및 손실 상한을 가진 `PREPARED_INERT` plan을 만든다.
3. global coordinator가 exact owner/bindings로 `APPROVED_INERT`를 heartbeat한다.
4. 사용자가 native UI의 server-issued phrase를 정확히 입력하고, 시험 동안 수동
   거래·다른 bot이 없다는 attestation 및 다른 API-key inventory를 Binance API가
   열거할 수 없다는 잔여 위험을 one-use approval로 승인한다.
5. trader가 authenticated named pipe로 exact prepared plan을 전달한다. authority는
   pipe peer PID/SID와 live command SHA, full bundle/ACL/account/credential pin을
   재검증한다.
6. authority가 network 0 child ready를 확인하고 zero-attempt signed ACK를 보낸다.
   현재 false build는 config/spawn/network보다 먼저 차단되어야 하고, 승인된
   release build에서도 config/spawn은 network 0으로만 수행되며 ACK 뒤 gate
   전에는 network가 열리지 않아야 한다.
7. authority-owned gate 뒤 observer가 user-data stream을 먼저 구독하고 signed GET
   3종 및 queued-event drain을 완료한다. known session clientOrderId prefix 밖의
   account order/execution event, stream gap, process mismatch 또는 crash는 즉시
   revoke/cleanup-only다.
8. state가 consumed approval, fresh observer snapshot(최대 age 5초), activation
   receipt를 다시 검증하고 exact 7200초 permit을 ACTIVE로 전환한다.
9. BUY는 finalized 5m natural signal에 최대 한 번, `quoteOrderQty <= 10 USDT`로만
   허용한다. SELL은 reducing/session-owned 한 번만 허용하고 no-reentry다.

## 7200초 관측과 안전 정지

- risk/market truth는 5초마다, owner heartbeat는 최대 20초마다 갱신한다.
- UI polling은 약 5~10초지만 authority로 간주하지 않는다.
- 비-cleanup sender는 global dispatch reservation과 Binance route lock 안에서
  observer health → durable `POST_MAY_HAVE_CROSSED` marker → final local lease →
  sender 순서를 지킨다. broker retry는 없다.
- revoke 뒤에는 session-owned cleanup SELL/CANCEL만 허용한다. 출금, transfer,
  margin/futures mutation은 cleanup에서도 0이다.
- STOP 또는 global Kill은 먼저 entry를 revoke하고 cleanup-only로 전환한다.
  recovery는 새 entry나 permit 재사용 없이 cleanup/final-reset만 재개한다.
- app/observer 종료, 절전, API key 변경, 수동 거래, 다른 bot 실행은 금지한다.

native UI safety action/endpoint는 다음 계약을 사용한다.

```text
BINANCE_SPOT_FUNCTIONAL_START   POST /api/binance-spot-functional/start
BINANCE_SPOT_FUNCTIONAL_STOP    POST /api/binance-spot-functional/stop
BINANCE_SPOT_FUNCTIONAL_RECOVER POST /api/binance-spot-functional/recover
STATUS                          GET  /api/binance-spot-functional/status
```

모든 POST는 trusted pywebview HttpOnly session과 `X-LiveTrader-CSRF`, server-issued
one-use challenge/token/typed phrase가 필요하다. release false에서는 UI도 disabled
상태여야 한다.

## terminal evidence

status가 `FINALIZED`이고 runtime의 exact `finalize_terminal` receipt가 확인된
뒤에만 observer를 종료한다. terminal evidence에는 다음이 모두 있어야 한다.

- exact runtime 7200초 및 clock consistency
- natural BUY+SELL 또는 명시적 safe-incomplete cleanup outcome
- BUY/SELL cap, no-reentry, fee/loss, preexisting BTC 보존 및 허용 dust
- account-wide open order 0, session-owned residual 0 또는 unorderable dust
- official REST truth hash, private stream journal seal/count
- supervised BASELINE/ACTIVATION/PRE_POST/TERMINAL durable phase chain
- manual/bot causal audit independently verified, observer gap/crash 0
- `otherApiKeyInventoryProven=false`
- `accountWideCausalClosureProven=false`
- `functionalWiringPassed=false`
- 성공적인 감독형 왕복에서만 `supervisedFunctionalWiringPassed=true`
- `promotionEligible=false`, `realE2EEligible=false`,
  `useAsPromotionEvidence=false`, `fullLiveAllowed=false`

terminal hash/DB/publication/cleanup truth가 다르면 FINALIZED로 간주하지 않고
cleanup-only reconciliation을 계속한다.
