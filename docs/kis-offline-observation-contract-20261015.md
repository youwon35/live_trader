# KIS reference-only 관측 계약

`validate_kis_equity_observation`은 독립 expected scope와 동결된 주식 수량 계약, 호출자가 제공하는 UTC `now`를 받아 JSON 구조·exact 정체성·신선도를 검사한다. 네트워크, 환경 설정, 계좌, 파일, DB, 런타임 또는 업무 시계를 읽지 않는다. 기존 entry/Source/guard 경로에 연결하지 않는다.

결과의 `schema_validation=VALID`는 형식과 선언된 scope가 맞다는 뜻이다. 관측 인증은 `verification_status=UNVERIFIED`로 구분한다. `nativeEligibility=UNKNOWN`, `live_allowed=False`, `orderAuthorityGranted=False`, `actualFeeObserved=False`는 생성자에서 변경할 수 없는 고정 값이다. 자기 content hash나 올바른 형식은 생산자·HTTP 응답·개인 수수료를 인증하지 않는다.

v1은 production KIS 국내/미국 BUY-cap query의 reference-only 자료를 표현한다. 공개 query projection만 포함하며 CANO, account product number, headers, app key/secret, token, raw body를 받지 않는다. 국내 `ORD_DVSN=00`와 no CMA/no overseas scope는 현재 읽기 경로와 같고, 이를 market-order 지원으로 바꾸지 않는다. 미국 venue는 Source와 query의 NASD/NYSE/AMEX를 exact 비교하지만 실제 producer는 현재 NASD 외 미구현이다. BUY-cap은 SELL 보유수량이나 실제 주문 능력의 증거가 아니다.

금액·가격·수량·공개 참고 요율은 finite canonical Decimal 문자열이다. 값이나 시간을 정규화·반올림하여 바꾸지 않고, 비정규 입력을 거부한다. 수량 cap은 정수이고 0은 형식상 허용되지만 주문 가능을 뜻하지 않는다. response calculation price가 query reference price와 다르면 거부한다. 실제 broker price grid, 최소 commission, tax 및 반올림 규칙은 여전히 미확정이며 새 실행정책을 정의하지 않는다.

시각은 `YYYY-MM-DDTHH:MM:SS.ffffffZ` UTC 형식이며 request ≤ response ≤ now ≤ expires를 요구한다. expires와 now 모두 request 시작 시각부터 15초 이내로 묶는다. 15초 범위는 관측 claim의 형식상 freshness 검사다. 새로운 주문 기준이나 기존 execution/freshness guard 완화가 아니다. 외부 실시간 관측이나 clock 교정을 수행하지 않는다.

개인 요율이 없으면 `fee.kind=UNKNOWN`이며 rate/unit/uri는 null이다. 공개 참고표는 `PUBLIC_REFERENCE`로 기록할 수 있으나 실제 개인 요율은 계속 UNKNOWN이다. ACCOUNT_CURRENT_OBSERVED·VERIFIED 주장은 현재 producer가 없어 거부한다. URL의 userinfo, query, fragment 및 비 HTTPS 형태는 거부한다. 공개 요율을 Source 또는 실제 fee에 주입하지 않는다.

결과는 frozen이며 저장된 canonical JSON은 변경되지 않는다. `receipt` 조회는 detached copy다. 기대 정체성도 변경된 input claim에서 유도하지 말고 독립한 현재 Source/Instance/Portfolio 및 저장된 계좌 metadata로 제공해야 한다.

계약 구현·격리검증과 실제 producer/consumer 연결은 별개다. 개인 현재 요율, 거래유형/venue/가격 규칙, KIS owned capital 및 fill/settlement projection, exact live boundary/ticket/transport 연결은 아직 미완료다. 이번 작업은 계좌 조회·심사·자본 배정·arming·주문·취소·새 세션을 하지 않는다.

`request.requestHash`는 method/endpoint/TR/bodyAbsent/public query projection에서 requestHash 자신을 제외한 canonical JSON SHA다. credentials를 포함한 원시 요청 digest나 transport 인증이 아니다. response.projectionHash와 최상위 contentHash도 각각 자신의 hash 필드를 제외한 내용의 자기 일관성만 검증한다.
