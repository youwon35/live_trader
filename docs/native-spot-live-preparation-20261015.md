# 2026-10-15 Live 현물 준비

현재 구현은 실제 주문 직전의 정보와 정상 비활성 후보 등록을 준비한다. 실제 주문·취소·arming·자동매매 시작·Native 확인을 수행하지 않았다.

## 새 연결

- Source의 `next-open`/ `NEXT_BAR_OPEN` 선언을 확인하고 종가 신호를 저장한다. 동일 Source·Instance·종목·provider·봉 주기의 실제 다음 OpenBoundary에서 수량을 계산한다. 종가와 ticker를 시작 가격으로 대신하지 않는다.
- 경계 claim은 callback 전에 commit한다. callback/다음 멤버/engine checkpoint 예외가 나면 기존 dispatcher가 전송을 ABORTED 처리한다. 이전 프로세스 신호와 이미 소비한 경계는 재전송하지 않는다. 실제 시작 이벤트의 30초 기한을 유지한다.
- 기존 Native 계좌 심사를 거친 사용자의 직접 시작에서만 주문가능현금의 승인 비율을 최초 원금으로 한 번 배정한다. 현재 자본은 그 배포에 귀속된 실제 체결·수수료·현재 평가가격으로 계산한다. 다른 보유자산과 외부 입금은 자동 편입하지 않는다. 같은 계좌 원금을 다른 배포가 중복 배정하면 차단한다.
- 매수 예산은 배포 현재 자본 × canonical Portfolio 비중 × Source 주문 비율(기존 5%)이고, 현재 귀속 현금과 실제 주문가능현금은 별도 상한이다. 원장보다 줄어든 외부 현금은 대조 필요로 차단한다.
- 실제 체결은 기존 제출 원장의 정확한 배포·Source·Instance·계좌와 공식 GET 결과를 대조해 append-only로 귀속한다. 체결 ID에는 계좌와 종목을 포함한다. 부분/미확정 주문과 평가하지 못한 제3자산 수수료는 차단한다.
- Binance는 현재 종목 LOT_SIZE/MARKET_LOT_SIZE와 방향별 standard/special/tax 수수료를 읽는다. 할인은 확정해서 적용하지 않고 보수적인 상한을 사용한다. 평균가격은 Source 구간과 정확히 같은 mins, 종목, 최근 closeTime에 결속한다.
- 현재 Binance 최대 수량만 봉인 Source보다 높아진 경우에는 Source의 더 낮은 상한을 유지한다. 상한 감소 및 다른 규칙 변경은 차단한다.
- Upbit는 해당 방향의 현재 수수료와 원화 시장가 규칙을 읽는다. 시장가 매수는 quote 금액 주문이므로 최종 초안의 Decimal 금액을 그대로 전달한다. 실제 시장 체결 수량을 모델 수량과 같다고 주장하지 않는다.
- 입력 수수료·평균가격·현재 계약 hash·현금 상한은 typed 관측에 다시 결속한다. 공유 연구/LOCAL 모델의 UNKNOWN을 변경하지 않고 별도의 현재 Live 관측을 표시한다.

## 검증

관련 Python 회귀 173개, 후보/읽기 전용 UI 회귀 39개, 세 소비자와 독립 Fraction의 수량·비용 비교 13개, bounded 비활성 감독 회귀 13개를 통과했다. 실제 controller의 Source→종가 신호→다음 OpenBoundary→수량→fixture 초안을 단일 전략과 두 Portfolio 멤버로 검사했다. 실제 금융 POST·취소·arming은 모두 0이다.

저장 계좌로 Binance/Upbit 공식 GET 13회만 수행했다. 현재 규칙·방향별 수수료 및 필요한 Binance 평균가격을 확인했다. 계좌 금액·키를 보고서에 저장하지 않았고 credential/settings 파일이 바뀌지 않았다. Binance Source의 더 낮은 상한 유지 차이를 별도로 기록했다. 이 관측은 현재 배포 자본 초기화나 거래 권한이 아니다.

현재 새 여섯 Paper 세션에는 동일한 종료 시각과 외부 plan SHA를 고정한 비활성 import 감독을 연결했다. 정상 자연 왕복·flat·CLEAN STOPPED·독립 Finance/parity·최종 봉인·정상 canonical 발행을 충족한 경우에만 기존 CAS로 draft/MONITOR/unresolved 계좌의 후보를 등록한다. Native 계좌 심사·자본 배정·arming·실행은 자동 진행하지 않는다. 근거가 없는 후보는 등록하지 않는다.

## 사용자 직접 실행 직전

저장된 세 계좌를 다시 등록할 필요가 없다. 정당한 Paper 근거가 발행된 비활성 후보를 선택하고 기존 Native 창에서 저장 계좌와 최초 사용 비율을 직접 심사한다. 승인한 총 사용 한도 100%와 Source 5%·Portfolio 배분·주문별 위험 한도는 서로 다른 항목이다.

직접 시작 직전에 현재 계좌/credential 결합, 주문가능현금·보유/미체결, 정확한 거래 규칙, 방향별 수수료, 평균가격/시작 이벤트의 신선도, 모드·Kill·risk 한도를 다시 확인한다. 이 문서나 과거 계좌 snapshot은 최종 실행 허가가 아니다. API 키의 실제 주문 권한·IP 허용 설정과 Native 최종 확인은 사용자가 프로그램에서 확인한다.

KIS 새 정수수량 계약은 LOCAL_MODEL_ONLY/UNKNOWN이다. 현재 계좌 적용 수수료의 권위 있는 producer, KR/US exact exchange·orderable lot/cash·가격 계약을 결속한 현재 capability receipt가 아직 없다. 정수 모델 계산과 실제 broker 준비를 구분하며 실거래 수량을 승인하지 않는다. 이 항목은 개발·검증의 잔여 항목이며 사용자 재등록/입금 문제로 표시하지 않는다.

## 근거

[Binance 수수료](https://developers.binance.com/en/docs/products/spot/faqs/commission_faq), [현재 평균가격](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market), [거래소 필터](https://developers.binance.com/en/docs/products/spot/filters), [Upbit 주문 관련 문의](https://docs.upbit.com/kr/docs/faq-order), [현재 주문 가능 정보](https://docs.upbit.com/kr/reference/available-order-information).
