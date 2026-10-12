# Binance Paper 경계 처리 지연 감사 — 2026-10-12

## 조사 범위와 결론

실제 공개 Binance 시세를 사용한 300초 Paper 관찰 기록을 읽기 전용으로 조사했다. 조사 과정에서 관찰 자료, 원장, 세션 상태, 미체결 의도, 전략 및 승급 증거를 변경하지 않았다. 실제 Live 주문과 Live arming은 0건이다.

03:10 UTC 경계의 BTC·SOL 시세는 공급자 이벤트 시각으로 보면 경계 직후 관측됐으나, 프로그램의 경계 처리 시각은 약 39~41초 뒤였다. 기존 30초 처리 지연 한도를 넘어서 안전 차단됐다. ETH BUY 의도도 03:10 UTC 경계를 기다리는 상태이며, 다음 03:15 경계를 기다리는 의도가 아니다. 완료된 실행·체결·승급 증거가 없으므로 이 관찰을 Paper→Live 통과로 보고할 수 없다.

## 실제 자료 위치

Paper 앱 아래 다음 사례 폴더의 logs와 soak 보고서를 조사했다.

`tmp/integration-promotion-20261012/native-execution-v3/cases/integration-5m-20261012-binance-portfolio-equal-execution-v3`

- `logs/continuous_runtime_status_241749dcaaeebf605362e8b5.json`
- `logs/continuous_runtime_engine_241749dcaaeebf605362e8b5.json`
- `logs/forward_signal_input_trace.jsonl`
- `logs/forward_evidence.sqlite3`
- `logs/paper_governance.sqlite3`
- `logs/paper_trading.sqlite3`
- `logs/reports/soak/20261012T030938Z-46fd38b7f6.json`

SQLite는 읽기 전용 연결로 확인했으며 서비스나 관찰 루프를 실행하지 않았다. 이 문서 작성은 사례 폴더 밖의 별도 기록이다.

## 시간축

모든 시각은 UTC다. 관찰 시작은 03:09:38.611, 종료는 03:14:39.032이다. 닫힌 봉의 구간은 03:05~03:10이며, 다음 봉의 시가는 03:10 경계다.

| 항목 | BTC | ETH | SOL |
| --- | --- | --- | --- |
| 닫힌 봉 파싱 시각 | 03:10:33.419 | 03:10:35.461 | 03:10:37.485 |
| 의도 생성 시각 | 03:10:33.441038 | 03:10:35.475729 | 03:10:37.500486 |
| 의도 | HOLD | BUY | HOLD |
| 목표 시가 경계 | 03:10:00 | 03:10:00 | 03:10:00 |
| 시가 이벤트의 공급자 관측 시각 | 03:10:02.016 | 실행 청구 없음 | 03:10:00.810 |
| 청구 처리 시각 | 03:10:40.531435 | 실행 청구 없음 | 03:10:39.515234 |
| 공급자 관측→청구 처리 지연 | 38.515435초 | 계산할 기록 없음 | 38.705234초 |
| 최종 의도 상태 | RECONCILIATION_REQUIRED | PENDING | RECONCILIATION_REQUIRED |
| 해소 사유 | LATE_OPEN_BOUNDARY | 없음 | LATE_OPEN_BOUNDARY |

BTC·SOL의 공급자 관측 지연은 각각 2.016초, 0.810초로 15초 관측 한도 안에 있었지만, 청구 처리 지연은 30초 한도를 초과했다. ETH의 boundary_json, claimed_at, resolved_at, execution_payload_hash는 비어 있었다. 과거 경계의 시가를 소급 체결하지 않았다.

종료 기록의 handlerErrorCount와 consecutiveHandlerErrorCount는 각각 12, pendingOpenBoundaryCount는 3, nextPendingOpenBoundaryStart는 03:10:00이다. 엔진의 openBoundaryCount는 0이고 processedOpenBoundaries는 비어 있다. 완료된 경계 처리로 해석하면 안 된다.

## 원장과 런타임 상태의 차이

거버넌스 세션 `paper-session-b867b177ad5e47818542225c96433ddc`는 CLEAN / PAPER_RUNNING이며 restart_count는 1이다. 세션 시작은 03:08:58.752672이고 종료 시각은 비어 있다. 반면 실제 관찰 런타임은 종료됐으며 과거 경계의 미해결 의도 3개를 가지고 있다.

CLEAN은 원장 및 admission 상태를 설명한다. 자연 관찰이 성공했거나 미해결 의도가 해소됐다는 뜻이 아니다. 실제 실행 청구, forward evidence, Paper 주문·체결, 세션 주문·체결, Live publication은 모두 0건이다.

## 입력과 의도의 결합 검증

입력 추적 체인의 검증 결과는 baseline 1개와 결정 3개, 총 4개 이벤트 모두 정상이다. 각 미해결 의도의 sourceSelectionHash 재계산 값, traceEventHash, inputHash, 모델·실행 신호, 세션·manifest·Portfolio 식별값이 실제 추적 이벤트와 일치했다. 이 자료에서는 입력 선택값 변조가 확인되지 않았다.

- 원본 trace 파일 SHA-256: `fb0783dda2940a0027c44199384a7ad10e143a24bed8ddd06086c21ff07819d5`
- manifestHash: `6761577b6760d91a1c1c9ad98e095a6fb06342cc675fdd8f058d05684de2f651`
- portfolioHash: `ada72ccfe31313f34b6c19b6e7d260d192b06c97e334122d25ecf5fbaba288f0`

## 지연 원인에 관한 확인 범위

현재 로그에는 소켓 프레임의 물리적 수신 시각과 콜백 enqueue 시각이 없다. 공급자 observed_time은 Binance 이벤트 본문의 E 값이다. 닫힌 봉의 received_time은 socket.recv 뒤 파싱 시각이며, 청구의 processReceivedTime은 경계 청구 처리 함수가 읽은 프로그램 시각이다. 따라서 외부 통신 지연과 내부 대기 시간을 정밀하게 분해할 수는 없다.

코드에서 확인한 내부 지연 요인은 다음과 같다.

1. WebSocketClosedBarFeed.poll은 호출당 프레임 1개를 읽는 경로였다. 봉 외의 공개 시세 프레임이 쌓이면 과거 시가 프레임이 늦게 처리될 수 있다.
2. 실패한 경계 이벤트는 PortfolioRuntimeManager 큐에 다시 들어가며, 해당 처리에서 반환한다. 이미 지연된 경계가 후속 경계 처리를 막을 수 있다.
3. load_settings_values는 격리 환경에서도 기존 strategy-core 전체를 대상으로 migrate_artifact_tree를 반복 호출했다. 기존 자료는 208개 파일·23,526,657바이트였고, 격리 대상은 259개 파일·47,453,607바이트였다. 대상 JSON 254개에는 기존 자료 JSON 193개가 포함됐다. 매 호출의 파일 순회·원본/대상 해시는 루프의 처리 비용을 높이는 요인이다.

이 조사는 성능 수정 전 자료의 원인 후보와 실제 차단 사유를 확인한 것이다. Paper 담당자는 프레임 묶음 처리, 변조 감지를 유지한 migration 비용 감소, 역사 warmup receipt 검증 및 세부 처리 시각 기록을 수정·검증 중이다. 새 관찰 결과가 나오기 전에는 수정 성공이나 자연 체결 성공을 주장하지 않는다.

## 유지한 검증 경계

30초 지연 한도를 늘리지 않았고 과거 시가를 소급 체결하지 않았다. 관찰을 짧게 재실행하더라도 실제 자연 경계, 주문·체결, 독립 신호 검증, 원장 및 최종 seal이 필요한 승급 항목은 그대로 충족해야 한다. 승급 기준 설정과 기능 검증, 역사 실제 자료의 신호 비교, 실제 forward 관찰은 각각 근거 범위가 다르다.


## 후속 실제 관찰 v5 — 두 자연 경계와 종료 점검

성능 수정 후 별도의 `native-execution-v5` 사례를 읽기 전용으로 다시 조사했다. 사례 이름은 동일한 `integration-5m-20261012-binance-portfolio-equal-execution-v3`이고 로그·원장은 새 v5 폴더에 있다. 이전 v3의 trace SHA는 앞서 기록한 값과 동일하여 그대로 보존됐다.

v5 관찰은 03:41:50.420 UTC에 시작해 03:51:50.678에 런타임을 멈췄다. 03:45와 03:50의 두 실제 자연 경계에서 BTC·ETH·SOL 각 1개씩, 총 6개 봉·결정·시가 경계가 처리됐다. 종료 시 runtime은 STOPPED / running=false이고 pending bar/open/event는 모두 0, handlerErrorCount와 consecutive errors도 0이다. 6개 의도와 실행 청구는 모두 TERMINAL이며 미해결 의도는 0이다.

| UTC 목표 경계 | 종목 | 입력 신호 | 의도 생성 | 시가 청구 | 의도 해소 | 실제 해소 결과 |
| --- | --- | --- | --- | --- | --- | --- |
| 03:45:00 | BTCUSDT | BUY | 03:45:01.031268 | 03:45:01.083473 | 03:45:01.663082 | OBSERVED, 주문 없음 |
| 03:45:00 | ETHUSDT | BUY | 03:45:01.051631 | 03:45:02.164103 | 03:45:02.695366 | 로컬 Paper FILLED |
| 03:45:00 | SOLUSDT | HOLD | 03:45:01.072694 | 03:45:01.670862 | 03:45:02.146411 | HOLD |
| 03:50:00 | BTCUSDT | BUY | 03:50:00.038031 | 03:50:02.594742 | 03:50:02.992722 | OBSERVED, 주문 없음 |
| 03:50:00 | ETHUSDT | HOLD | 03:50:00.060470 | 03:50:03.001015 | 03:50:03.420821 | HOLD |
| 03:50:00 | SOLUSDT | BUY | 03:50:00.082341 | 03:50:00.092314 | 03:50:00.560180 | 로컬 Paper FILLED |

처리 시각과 공급자 E 시각은 서로 다른 시계다. 실제 processReceiptLagSeconds 범위는 -0.941560~0.970189초다. 음수 원본값을 지우거나 0으로 바꾸지 않았으며, 이를 물리적 소켓 지연으로 해석하지 않는다. 기존 30초 안전 한도는 유지됐다.

로컬 가상 계좌의 실제 Paper 체결은 `LOCAL_IMMEDIATE_V1` 2건이다. ETH는 19개를 2545.643464에, SOL은 447개를 111.713504에 매수했다. 최종 원장에는 ETH 19개와 SOL 447개가 열려 있고 cash는 901598.534733896 USDT, 마지막 기록 equity는 999883.961021896 USDT다. 실제 Binance 계좌의 주문·체결은 아니다. 보고서의 executionRoute는 local virtual account, realOrderSubmissionEnabled는 false, realOrderCount는 0이다.

native 종료 요청은 열린 가상 포지션 때문에 세션 최종 종료를 차단했다. 런타임은 STOPPED지만 거버넌스는 CLEAN / DRAINING이고 세션 ended_at은 비어 있다. flat 상태나 최종 봉인이 완료됐다고 표시하지 않는다. 최종 reconciliation seal, Paper→Live Evidence bundle, Live publication은 모두 0건이다.

실제 soak는 600.289초·119개 표본이며 targetDurationSeconds=18000을 충족하지 못해 verdict=FAIL이다. 오류·재연결·실주문은 0이지만 이것으로 장기 검증 통과나 승급 통과를 주장하지 않는다.

검증 결과는 다음과 같다.

- runtime status와 engine의 contentHash 검증 모두 통과.
- 독립 Backtester reader로 baseline 1개와 실제 input 6개, 총 7개 trace 이벤트의 체인·입력·rolling history 검증 통과. trace SHA-256은 `c0bd4df3365e173c70844cea55de289bda6e43a750defc9a6c9ccc05a97d6a55`이다.
- 6개 PendingIntent의 state·command·content·sourceSelection hash 및 trace event/input/source/session/manifest 결합 검증 통과.
- 실제 실행 청구 6개의 transition chain과 현재 상태 검증 통과.
- 원본 Strategy 3개, Portfolio, execution edition의 새 Instance 3개의 seal 검증 통과.
- 봉인된 실제 Strategy를 사용한 독립 Backtester 재생에서 실제 forward input 6개 모두 모델·실행 신호와 일치. 새 참조 발행이나 Live 승급 Evidence 작성 없이 순수 함수로 비교했다.

이 결과는 자연 시세→결정→로컬 Paper 체결과 안전한 런타임 중지를 확인한다. 열린 포지션의 완결 처리와 최종 승급 봉인은 아직 완료되지 않았다.

## Upbit 종료 오류에서 확인한 canonical 봉 표현 차이

v5 Upbit의 종료 점검은 trace와 PendingIntent의 closedBarHash가 다르다고 차단했다. 같은 검사를 추가해 보니 Binance 6개와 Upbit 3개, 총 9개의 실제 입력에 같은 원인이 있었다.

`continuous_portfolio_runtime.py`의 봉 해시 재료에는 sourceProvider가 포함되지만, `signal_parity.py`가 저장한 trace의 봉 재료에는 그 필드가 없다. 종료 consumer가 trace의 마지막 봉 자체를 해시하면서 차이가 발생했다. 각 trace 봉에 sourceProvider=검증된 동일 provider를 넣어 읽기 전용으로 계산한 결과 9개 모두 기존 intent.closed_bar_hash와 정확히 같았다. OHLCV·시간 경계의 차이나 rolling history 변조가 원인은 아니다.

Binance에서는 열린 포지션 검사가 먼저 차단하므로 이 추가 오류가 아직 표면화되지 않았다. Paper 담당자가 producer·consumer의 canonical hash 재료를 통일하는 호환 수정과 회귀 검증을 진행하고 있다. 기존 trace·intent·원장·Final을 다시 봉인하거나 소급 수정하지 않는다. 수정 후 종료 consumer 검증 결과는 별도 확인해야 한다.

## canonical consumer 교정 후 최종 확인

Paper 담당자가 `pending_intent_closed_bar_payload` 공통 helper로 기존 producer의 봉 해시 재료를 통일했다. 기존 v5 Binance 6개·Upbit 3개 intent의 해시는 이 helper 결과와 모두 일치했다. 감사 전후 각 사례의 runtime·engine·trace·forward DB·Paper ledger DB·governance DB 바이트 해시가 같아, 감사가 원본 자료를 변경하지 않았음을 확인했다.

Upbit는 기존 자료를 유지한 native 종료 재시도에서 CLEAN / STOPPED로 종결됐다(세션 ended_at=04:06:22.282727 UTC). 최종 reconciliation seal을 순수 파서로 검증했고 현재 STOPPED runtime hash와 일치했다. positionFlat=true, nonTerminalIntents=0, ambiguousIntents=0이다. Binance는 열린 ETH·SOL 때문에 CLEAN / DRAINING을 유지하며 최종 seal은 없다.

독립 신호 재생의 결과는 구분한다. Binance의 실제 forward input 6개는 모두 PASS다. Upbit의 실제 input 3개는 warmup-incomplete로 BLOCKED다. Upbit의 HOLD 신호가 재생의 기본 HOLD와 같더라도 신호 parity PASS로 표시하지 않는다. Upbit는 봉·경계 처리·무결성·flat 종료 경로를 확인했으며 로컬 체결은 0건이다. 두 사례 모두 실제 broker 주문과 Live publication은 0건이다.

멤버별 원본 시각·hash·claim·로컬 계좌·독립 재생·최종 seal 결과는 Live 앱의 `output/promotion-20261012/paper-v5-read-only-audit.json`에 저장했다. 기능 동작, 자료 무결성, 장기 검증과 실제 승급은 이 보고서에서도 별도로 표시한다.

Upbit warmup 부족의 숫자도 확인했다. RSI14의 requiredHistoryBars는 16이며 baseline에는 모든 멤버가 16봉씩 있어 최초 요구를 충족했다. 마지막 seed는 03:35~03:40이었다. 03:41:48 중간 bucket에서 소켓을 연결하여 불완전한 03:40~03:45 봉이 제외됐고, 최초 완전한 03:45~03:50 봉이 들어올 때 missingBars=1 / gapStateReset=true가 각 멤버에 적용됐다. 이후 실제 history는 각 1봉, runtime gapCount는 3이다. 이번 BLOCKED는 REST의 200개 제한이나 300봉 기본 요청 부족 때문이 아니며, native 첫 봉 완전성 및 gap reset 가드의 결과다. 이 근거를 JSON의 warmupContext에도 추가했다.
