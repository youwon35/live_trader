# 2026-10-12 Live 전체 탭 점검과 Source 준비

12개 실행 단위와 8개 Portfolio를 실거래 권한 없는 준비 목록에 연결했다. 원래 Source, Final, 전략 매개변수, 원자료, 승인 기록, 기존 운영 배포는 수정하지 않았다. 실제 Paper 승급 근거가 없는 20개는 `PAPER_EVIDENCE_PENDING`이며 `liveAuthorized/orderSubmissionEnabled/armingEnabled=false`다. Source 준비를 Live 승급으로 표시하지 않는다.

## 원본·승인 결합

기준 Source는 공용 `strategy-core`의 실제 현재 봉인본이다. Backtester `campaign-result-execution-v3.json`, Paper `canonical-publication-result.json`, 기본 Paper `logs/strategy_approvals.json`을 읽어 Source·Instance·Portfolio hash, 매개변수/시간봉, PUBLISHED 원자료 SHA, 현재 PAPER stage 및 frozen 정책을 대조한다. Portfolio는 모든 멤버와 정규화 배분 비중을 검사한다.

과거 12개 PAPER 승인 기록에 Source hash가 기록되지 않은 사실은 `HISTORICAL_APPROVAL_SOURCE_HASH_NOT_RECORDED`로 표시한다. 현재 Source hash와 승인 기록 전체 hash를 준비 메타데이터에 묶는 것이 과거 승인 당시 결합을 입증하지는 않는다. 새 LOCAL-only 세션의 NativeAdmissionAttestation/Manifest는 오늘의 Source/Instance/Portfolio/정책 결합을 별도로 제공한다. Paper `native-recovery-v9/campaign-result.json`의 20개 입장 결과와 실제 기본 KR 새 세션의 `start-result.json`은 실제 완료 거래·기간·독립 parity·최종 종결 근거를 대신하지 않는다.

조회 전후 정확한 선택 그래프와 현재 승인 기록을 다시 읽는다. Source/Instance 재봉인, 승인 철회/변경, 정책 변경, 자료 변조, 저장 계좌 런타임 설정 변화가 있으면 기존 preview를 폐기한다. 오래된 등록은 유효 준비 목록에서 숨기고 해당 이전 요청은 거부한다. 캐시로 과거 허용 결과를 재사용하지 않는다.

## 사용자 확인 경로

1. `운용 전략 → 모의거래 검증 근거 확인 → 준비 자료 새로고침`을 누른다.
2. 12 실행 단위 또는 8 Portfolio 중 하나를 선택하고 `원본 확인 · 계좌 조회 없음`을 누른다.
3. 해당 원본의 종목, 매수/매도, 수량, 지정가를 직접 입력한다. 미입력은 미입력으로 남는다.
4. `계좌·미체결 읽기 및 입력 금액 계산`을 눌러 현금·자산·미체결과 한도 계산을 확인한다.
5. 실제 Paper 완료 근거가 생기면 기존 `Paper 검증 근거 새로고침 → 검토 대기 후보 등록 → 저장 계좌 선택 → 계좌 연결·제한 실거래 준비 확인` 경로에서 정확한 봉인 근거와 Native 확인을 직접 검토한다. 준비 successor는 `before-live-small/MONITOR`, 실주문 권한 false다. 사용자가 실제 운용 전 직접 확인해야 하며 이 작업에서 Native 확인 입력·arming·시작·실주문·취소는 하지 않았다.

## 실제 저장 계좌 읽기

저장 계좌는 KIS, Binance, Upbit 3개이고 시장 연결은 KIS 국내/미국, Binance Spot, Upbit 현물 4개다. 공식 GET과 메모리 OAuth 인증만 허용한 reader로 모두 현재 주문가능현금 관측에 성공했다. 보호 비밀 저장소와 `.env` SHA는 조회 전후 동일했다. 계좌 전체번호·credential·raw응답·실제 잔액은 검사 보고서와 외부 요약에 저장하지 않는다. 금액은 명시적으로 요청한 로컬 preview에서만 사용한다. 기록은 `output/live-tabs-20261012/actual-readonly-cash-summary.json`의 가린 계좌와 상태만 포함한다.

`SAVED_CREDENTIAL_CONFIGURATION_OBSERVED`는 저장된 런타임 credential 설정을 사용해 계좌 읽기가 성공했다는 뜻이며 Native 계좌 심사/거래 허가 완료가 아니다. 미체결 조회 범위는 선택 종목 또는 KIS 국내/NASD 조회 범위로 한정한다. 실거래 시작 직전에 최신 상태를 다시 조회해야 한다. Preview는 60초 후 만료한다. 선택한 주식의 정규장 상태는 공유 거래소 달력과 현재 시각으로 표시하며 미국 정규장 시작은 현지 09:30이다. 달력상 장중 표시와 실제 broker 주문 허가는 구분한다.

총 사용 의도는 `ACCOUNT_AVAILABLE_CASH 100%`로 표시한다. 각 주문의 계산 기준은 **현재 주문가능현금 × 원래 Portfolio 정규화 비중 × Source 주문 비율 5%**이며 원래 fee/slippage와 관측된 수수료 중 필요한 여유를 반영한다. 수량은 정확한 lot step으로 내림하며 최소/최대 수량·금액과 입력 호가 단위를 검사한다. 계좌 전체현금을 각 멤버마다 5%씩 배정하지 않는다. 이 계산은 예산 정보이며 실행 허가가 아니다.

- Binance: 현재 Spot free quote balance, 실제 commissionRates.taker, LOT_SIZE/PRICE_FILTER/NOTIONAL을 관측했다. 정적 단위 검사와 별도로 동적 가격 필터·API 주문 권한·현재 노출·승급 및 Native 승인 검사는 필요하다.
- Upbit: bid_account.balance, 실제 bid_fee, 최소/최대 notional, 지원 limit 유형을 관측했다. 현재 chance 응답으로 정확한 quantity step/price tick을 확인하지 못했으므로 실행 수량은 차단·미확정이다.
- KIS 국내: 주문가능현금과 미수없는 매수금액의 작은 값을 사용하며 CMA/해외 자금 합산은 제외한다. 가능 수량과 1주 단위는 읽었지만 실제 적용 commission/정확한 tick/orderability 미관측 때문에 실행 수량은 미확정이다.
- KIS 미국: NASD USD 주문가능외화와 해외 주문가능금액 범위에서 작은 값을 사용하고 자동 환전 금액을 합산하지 않는다. 실제 commission/tick 및 종목의 정확한 거래소 결합은 미완료다. NASD 조회 성공을 모든 미국 거래소 승인으로 표시하지 않는다.

공식 기준: [KIS 국내 매수가능 조회](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_psbl_order/inquire_psbl_order.py), [KIS 해외 매수가능금액](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_psamount/chk_inquire_psamount.py), [Binance Spot 필터](https://developers.binance.com/en/docs/products/spot/filters), [Upbit 주문 가능 정보](https://docs.upbit.com/kr/reference/available-order-information).

## 검증 범위

Python 80개 독립 fixture 회귀는 exact Source/evidence import, account review, source/approval/policy 변화, 자료 SHA, read-only endpoint/transaction ID, 현금 배분/lot/fee 차단을 검사했다. Frontend workflow 99개 회귀와 기존 추가 nested/action 39개 runner 검사도 통과했다. 합성 Native 확인·HTTP mutation은 고립 fixture에서만 사용했다.

실제 빌드 frontend의 8개 상위 탭을 1280/1920 두 크기에서 열고, 중첩 한도/로그/계좌 누락 상태 및 20개 정확한 원본 선택을 모두 확인했다. 22개 view 기록, Source 선택 preview 40회, renderer 오류·가로 넘침·외부 요청·broker I/O 0이다. 실제 계좌 read-only 결과와 합성 화면/행동 검증을 별도로 기록한다. 실제 Live evidence 발행/승급/주문 성공을 이 결과로 주장하지 않는다.

기존 70개 dirty 작업을 보존하고 이번 소유 파일만 선택 commit한다. 최종 실행 파일은 공동 warmup freeze `cf7fbb492f817bb1588afd47dc8cfc6c979ae830`를 포함해 다시 빌드하고, 실행 중인 사용자 프로세스는 종료하지 않는다.
