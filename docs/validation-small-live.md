# 검증용 Small Live

`SMALL_LIVE`와 `FULL_LIVE`는 단순 화면 잠금이 아니다. 표준 lifecycle은 다음 근거를 순서대로 요구한다.

`Backtested → Before Shadow → Shadowed → Papered → Before Live-Small → Live`

- `SMALL_LIVE`: `before-live-small`, Paper 주문·관찰, 복구 훈련, 브로커 대조 근거가 필요하다.
- `FULL_LIVE`: 위 조건에 더해 Small Live 실제 canary 체결 3건, 차단 주문 0건, 운용자 확인 및 warning 0개가 필요하다.

Backtest와 Portfolio만 통과한 후보를 바로 표준 `before-live-small`로 바꾸면 Paper 근거를 위조하는 셈이 된다. 그래서 검증용 경로는 표준 lifecycle이나 실주문 권한을 변경하지 않고 별도의 `validation-before-live-small` plan을 만든다.

## 안전 계약

- 운용 모드: `MONITOR`
- Dry Run: 필수
- 네트워크: 불필요
- 브로커 주문 전송: 금지
- 최대 주문 금액: 0
- 표준 lifecycle 변경: 없음
- `live_small_eligible`/`live_eligible` 부여: 없음
- 원본 Strategy/Portfolio 파일 SHA-256이 달라지면 plan은 즉시 무효

## 생성 및 재검증

```powershell
.\.venv\Scripts\python.exe scripts\prepare_validation_small_live.py
.\.venv\Scripts\python.exe scripts\prepare_validation_small_live.py --verify
```

기본 결과는 `%LOCALAPPDATA%\live_trader\logs\validation-small-live-plan.json`에 저장된다. 파일을 쓰지 않고 후보만 확인하려면 `--preview`를 사용한다.

```powershell
.\.venv\Scripts\python.exe scripts\prepare_validation_small_live.py --preview
```

종목이나 봉을 제한할 수도 있다.

```powershell
.\.venv\Scripts\python.exe scripts\prepare_validation_small_live.py `
  --symbols BTCUSDT,BNBUSDT `
  --timeframes 1h,1d
```

## Live Trader에서 보이는 분류

- `일반 통합 Smoke`: 현재 plan의 Spot·주식·ETF 후보이다. SHORT/Futures 후보로 표시하거나 라우팅하지 않는다.
- `Futures SHORT 정식 후보`: `marketType=futures`, `positionDirection=short`, `allowShort=true`, `brokerHint=binance-futures`가 모두 일치하고 정식 Backtest·Portfolio 게이트를 통과한 후보만 집계한다.
- `승급 차단 SHORT`: Futures SHORT 계약은 있으나 Draft/Final/Portfolio 게이트에서 차단된 후보이다.
- `실제 평가 가능`: Strategy와 Portfolio가 canonical v2 lock으로 다시 발행되어 공유 `BuiltinBarSignalEvaluator`가 읽을 수 있는 후보이다. Legacy lock은 목록만 보여 주고 평가 버튼을 잠근다.

자동화 탭의 `1회 MONITOR 평가`는 최신 공개 확정 봉을 read-only로 불러와 Paper/Live 공용 지표 평가기를 실행한다. `OrderIntent`를 만들지 않으며 최대 주문금액은 0이다. 현재 검증 plan을 기존 지속 감시 runner에 우회 연결하지 않는다.

별도의 Binance Futures SHORT 연구 bundle은 SELL 진입, BUY reduce-only 청산, Portfolio, Shadow/Paper 기능 계약을 확인하기 위한 historical research evidence다. 화면에는 정식 후보와 분리해 표시하며 `researchOnly=true`, `artifactPromotionAllowed=false`, 실제 주문 0을 유지한다.

이 plan은 신호·레이아웃·메모리·다중 프로그램 병행 운용을 점검하기 위한 후보 목록이다. 실제 Small Live로 전환하려면 Shadow/Paper/복구/대조 근거를 정상 경로로 추가해야 하며, Full Live는 실제 canary 체결 전에는 열리지 않는다.
