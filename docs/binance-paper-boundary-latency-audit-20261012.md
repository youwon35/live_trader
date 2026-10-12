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
- `reports/soak/20261012T030938Z-46fd38b7f6.json`

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
