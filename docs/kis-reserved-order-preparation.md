# KIS 예약·조건 필드의 추가 읽기

KIS 종목을 선택한 뒤 사용자가 추가 조회 버튼을 눌렀을 때만 실행한다.
기존 계좌 조회는 새 예약 API를 자동 호출하지 않는다. 주문·취소·예약
등록이나 원장 초기화는 실행하지 않는다.

## 기록하는 범위

- 국내 예약 체결 조회 `CTSC0004R`: 최근 30개 한국 날짜의
  `CNCL_YN=Y`, `PRCS_DVSN_CD=0` 고정 필터.
- 미국 예약 목록 `TTTT3039R`: NASD, NYSE, AMEX 각각 같은 날짜 범위.
- 국내 정정·취소 가능 주문 `TTTC0084R`: 응답 행의 조건가격과 조건발동
  필드가 있는지 확인. 실제 가격과 상태값은 공개 요약에서 제외.

각 범위 최대 2페이지, 총 10 GET이다. 기존 인증·속도 제한을 사용하며
계좌나 설정이 조회 도중 변경되면 결과를 버린다. 전체 조회는 시작부터
15초의 UTC/monotonic 경계로 따로 평가한다. 페이지 제한이나 종료 표식
미확인은 PARTIAL/UNKNOWN이며, 오래된 관측은 기록으로만 남긴다.

## 이 조회로 확인되지 않는 것

공식 예제의 국내 예약 필터가 모든 활성·미처리 예약을 포함한다는 설명은
확인되지 않았다. 같은 공식 저장소의 과거 예제에도 매개변수 설명 차이가
있으므로 임의로 다른 필터를 추가하거나 상태코드를 해석하지 않는다.
미국 3개 거래소 응답도 서로 겹치지 않는다고 가정하거나 합산하지 않는다.
기간 밖 예약, 미발동 HTS/MTS 조건, 다른 국가, 여러 요청 사이의 단일
계좌 스냅샷, 실제 현금·수수료·자동환전·확정 정산은 별도 확인이 필요하다.

따라서 모든 행이 비어 있어도 전체 계좌에 주문이 없다는 증명이 아니다.
`reservedOrdersReconciled`, `completeConditionalOrdersVerified`,
`fullAccountReady`, `financialReady`, `orderAuthorityGranted`는 false를 유지한다.
계좌별 공통 원화 예약 및 실거래 권한의 기존 차단을 해제하지 않는다.

## 공식 원본

- https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/domestic_stock/order_resv_ccnl
- https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock/order_resv_list
- https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/domestic_stock/inquire_psbl_rvsecncl
- https://github.com/koreainvestment/open-trading-api/blob/main/legacy/Sample01/kis_domstk.py

공개 요약의 해시는 파싱한 JSON에 대한 로컬 무결성 표식이며, 증권사의
서명·원본 HTTP 바이트·현재 계좌 권한·Paper 증명으로 사용하지 않는다.
