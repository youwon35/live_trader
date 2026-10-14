# KIS 국내 read-only 응답 순수 변환 계약 (2026-10-15)

`live_trader/kis_reference_conversion.py`는 이미 확보한 응답 바이트와 독립 기대 scope를 기존 `kis-current-equity-reference-observation-v1` 검증기에 전달하는 순수 변환기다. 네트워크, 계좌 조회, 파일, 시계, 운영 상태, 주문 entry를 호출하지 않는다. 기존 v1과 Source·수량·승인 guard는 변경하지 않았다.

`convert_kis_buy_reference_response(raw_response: bytes, *, request_public, request_origin, response_continuation, expected_scope, source_quantity_contract, request_started_at, response_received_at, expires_at, now)`에 모든 시각과 기대 identity를 명시한다. public request는 GET method·endpoint·TR·bodyAbsent·비민감 query만 받는다. 원 계좌번호·key·token·header는 이 descriptor에 허용하지 않는다. production origin도 호출자가 제공하는 주장으로 취급한다.

## 국내 필드의 의미

| v1 필드 | 직접 응답 필드 | 변환과 의미 |
| --- | --- | --- |
| availableCash | ord_psbl_cash, nrcvb_buy_amt | 주문가능현금과 미수없는 매수금액 두 역할을 보존한 CONSERVATIVE_MIN. QUERY_REFERENCE_NOT_OWNED_CAPITAL. |
| maximumBuyQuantity | nrcvb_buy_qty | 미수없는 수량을 직접 읽고 정수 여부를 검사한다. 지정가 query class00의 참조 한도다. |
| calculationPrice | psbl_qty_calc_unpr | 브로커가 응답한 계산단가를 직접 읽고 query 가격과 수치 일치를 검사한다. 요청 가격으로 대입하지 않는다. |

각 금액 operand와 직접 응답 수량은 동일한 자본원장이라는 증거가 없다. 따라서 이 변환은 수량×가격의 affordability, 배포 소유 자본, 보유 자산, 실제 최대 주문, MARKET order 능력, 수수료 반영을 보장하지 않는다. max_buy_amt/max_buy_qty·담보·CMA·외화·재사용 필드로 누락값을 채우지 않는다. currency와 symbol은 국내 endpoint 및 동결 Source·public query의 scope에서 유도하며 원응답의 echo라고 표시하지 않는다.

공식 [국내 주문가능조회 예제](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_psbl_order/inquire_psbl_order.py)와 [응답 필드 검사 예제](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_psbl_order/chk_inquire_psbl_order.py)를 대조했다. query ORD_DVSN=00, CMA/해외 포함=N만 현재 v1 참조 범위로 허용한다. 실제 주문종류별 한도와 동일하다고 확대하지 않는다.

## 검증과 결과

원응답은 최대 4MiB의 UTF-8 JSON 바이트다. 모든 깊이의 중복 키·NaN·Infinity, 실패 rt_cd, 잘못된 root/output, 복수 output, 선택 필드 누락, bool·음수·부호있는 0·과도한 소수 exponent/자리수·분수 주식을 거부한다. output object와 길이1 object list만 지원한다. 숫자는 Decimal로 정확히 읽고 값이 같은 canonical 문자열로 변환하며 반올림이나 binary float 변환을 하지 않는다.

단일 응답 완료를 확인하기 위해 continuation은 명시적인 빈 문자열만 허용한다. 비어있지 않거나 미제공된 marker는 불완전 또는 미지원으로 거부하며 자동 pagination을 수행하지 않는다. 시각은 기존 v1의 UTC 순서·요청시작 기준 15초 형식 freshness 검사를 그대로 소비한다. 이것은 새 실제 주문 시한 정책이 아니다.

별도 불변 provenance는 원응답 SHA/길이·public endpoint/TR/query·독립 기대 scope hash·동결 Source 계약 hash·v1 claim hash·제공 시각·필드 역할을 결속한다. 그 contentHash와 public requestHash는 자기일관성 검사이며 인증 서명이나 브로커 관측 진실의 증명이 아니다. 결과 상수는 생성자 인자로 바꿀 수 없는 UNVERIFIED, nativeEligibility UNKNOWN, live_allowed false, orderAuthorityGranted false, actualFeeObserved false, authenticatedTransport false다. 개인 요율은 항상 UNKNOWN/null이며 공개 요율을 넣지 않는다.

`result.reference`는 검증된 v1 claim의 별도 복사본으로 scoped 금액·수량과 사용자가 제공한 계좌 별칭을 포함한다. 금융 값이 없는 출력으로 취급하면 안 된다. `result.provenance`에는 public query 가격·종목과 해시가 있지만 원응답의 private 필드와 금액 operand는 없다. `result.to_dict()`와 repr는 진단용 상태·해시·길이·의미만 제공한다. 모든 변환 오류는 원문·remote message·값이 없는 고정 코드이며 JSONDecodeError.doc가 예외 context에 남지 않게 catch 밖에서 발생시킨다. 원 raw JSON을 로깅하거나 예외에 넣지 않는다.

## 미국 및 남은 구현

[미국 주문가능조회 응답 예제](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_psamount/chk_inquire_psamount.py)에는 이 v1에 필요한 계산단가 echo가 선언되어 있지 않다. US는 US_BROKER_CALCULATION_PRICE_MISSING_UNSUPPORTED로 명시적으로 거부한다. 임의 raw body가 echo를 삽입해도 지원하지 않으며 요청 가격을 응답 가격으로 채우지 않는다.

순수 계약 변환과 격리검증을 완료한 범위와 실제 운영 관측을 분리한다. 실제 raw bytes/시각/continuation/인증 transport를 수집하는 producer, 개인 계좌의 현재 수수료, current native 능력, ownedDeploymentEquity, 실행 가격·boundary·ticket consumer 연결은 아직 구현되지 않았다. 이번 파일은 운영 entry에서 import하지 않으며 두 비활성 Live 후보·권한·계좌 설정·운영 세션을 바꾸지 않는다. build hidden import는 순수 모듈의 패키지 포함 목적뿐이다.

신규 `tests/test_kis_reference_conversion.py` 20개 격리 회귀는 합성 데이터만 사용하며 파일·네트워크 호출을 차단한다. 실제 계좌 API/심사/자본배정/arming/주문/취소/새 세션은 모두 0이다.