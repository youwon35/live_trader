# KIS 해외주식 결제 방식

`KIS_OVERSEAS_FUNDING_MODE=INTEGRATED_MARGIN`가 기본값이다. 저장된 계좌 설정의
‘해외주식 결제방식’에서 통합증거금 또는 사전환전 외화 현금을 선택할 수 있다.
이 설정은 KIS 해외주식 공통 선호이며, 서비스 신청·환전 요청·주문 권한을 만들지 않는다.
사용자는 통합증거금 가입 및 사용 사실을 확인했다. 프로그램은 별도의 계좌 검증 없이
그 진술을 증권사 응답이나 확정 정산 증거로 바꾸지 않는다.

공식 서비스 국가 표에는 미국·영국·일본·중국·홍콩·한국이 기재되어 있다.
영국은 USD 거래만 해당하고 GBP/GBX 거래는 제외되며, 홍콩의 USD 거래 종목도 제외된다.
애프터마켓 연장 서비스 가입 제한도 별도로 확인해야 한다. 프로그램의 현재 실행 지원은
국내 KRX 및 미국 NASD/NYSE/AMEX이며, 결제 방식 선택으로 다른 시장 주문 경로가 추가되지는 않는다.

읽기 전용 준비 화면의 기존 주문가능금액 조회에서 `echm_af_ord_psbl_amt`와
`echm_af_ord_psbl_qty`를 통화가 일치하는 별도 참고값으로만 파싱한다.
환전 후 주문가능액은 현재 달러 예수금·확정 정산금·배포 소유 자본이 아니므로
다른 금액과 더하거나 주문 수량에 사용하지 않는다. 빈 응답은 0원으로 간주하지 않는다.
설정을 바꾸면 KIS 계좌의 configuration hash가 바뀌어 이전 준비 결과를 재사용할 수 없다.

통합증거금 자동 주문은 국내·해외 공통 원화 자금 예약, 실제 수수료, 환전 사건 및
확정 정산 대조가 검증될 때까지 차단한다. `FOREIGN_CASH`는 기존 외화 현금 모델을
명시적으로 선택하는 옵션이며, 이미 가입된 증권사 서비스를 해지하거나 변경하지 않는다.

공식 근거:

- [통합증거금 약관](https://file.koreainvestment.com/Storage/customer/guide/regards/GDRs_deposit_money.htm)
- [해외증권 거래설명서](https://file.koreainvestment.com/Storage/customer/guide/regards/service_03_1%283%29.htm)
- [공식 주문가능금액 응답 예제](https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/overseas_stock/inquire_psamount/chk_inquire_psamount.py)
