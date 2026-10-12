# Paper 후보의 계좌 심사와 Live 준비

## 현재 연결 흐름

Paper가 실제 관찰과 독립 검증을 마쳐 발행한 근거를 Live 후보 화면에서 읽는다. 전략·Instance·Portfolio 저장본, Evidence 내부/외부 hash, 최종 원장·flat 포지션 봉인, 발행 정보, 세션에 봉인한 승급 정책을 모두 대조한다. 검토 대기 등록은 `draft / MONITOR`이며 계좌와 실주문 권한을 부여하지 않는다.

등록 후 저장 계좌를 직접 조회하고 해당 broker의 계좌 별칭을 선택한다. 이 조회는 로컬에 저장한 설정만 읽으며 API key, 전체 계좌번호, 실제 잔액을 화면에 반환하지 않는다. KIS 실계좌, Binance Spot, Upbit 현물 프로필을 지원한다.

계좌 심사는 기존 native HTTP 세션·CSRF와 90초 일회용 확인을 사용한다. 후보·Evidence/정책·Instance·계좌 설정 fingerprint·총사용비율·설정 revision을 확인 문맥에 묶는다. 사용자가 화면의 확인 문구를 입력한 정확한 요청만 승인된다. 확인 후에는 이전 불변 배포 정의를 수정하지 않고 계좌를 연결한 새 정의를 만들며, 이전 배포 종료와 새 `before-live-small / MONITOR` 준비 상태를 하나의 원자 저장으로 기록한다. 중복 요청·경쟁 수정·계좌 설정 변경은 거부한다.

총사용한도 100%는 `ACCOUNT_AVAILABLE_CASH`의 100%라는 의도를 봉인한다. 심사 시 실제 주문가능액은 조회하지 않으며 시작 점검에서 확인해야 한다. 주문 수량과 위험 설정은 봉인된 전략 및 기존 주문 가드에 따른다. 이 준비 승인으로 실주문 ON, 런타임 시작, 계좌 조회, 주문 제출이 자동 실행되지 않는다. 실제 실행에는 현재 계좌 진실을 확인하는 시작 점검과 기존 정확한 native 주문 확인이 계속 필요하다.

## 사용자가 정한 승급 기준

새 세션은 `promotionPolicy` snapshot/hash를 봉인한다. `standard`는 기존 기준을 유지하며, 명시적인 `custom` 변경은 `CUSTOM_THRESHOLD`와 기준별 기본값/설정값 차이를 남긴다. 그 세션에서 정한 기준을 통과하면 실제 후보 발행과 Live import가 가능하다. 현재 설정으로 과거 근거의 기준을 낮추지 않는다. 기존 functional-test 근거는 `NON_PROMOTION`을 유지한다.

사용자 설정으로 Shadow 결정/의도 최소 표본만 0을 허용한다. 이를 독립 signal parity PASS로 표시하지 않는다. 실제 Live용 관찰 기간·주문·regime·완료 거래는 각각 양수이며 원장, 독립 검증, 최종 봉인, 계좌 확인을 생략할 수 없다.

Live 화면은 실제 관찰 초/일, 주문/상태 수, 정책 차이와 기본 forward 기간·주문·regime·boundary 기준 충족 여부를 표시한다. 이 기본 forward 표시의 범위에는 금융 성과 전체 기준이 포함되지 않는다. 짧은 custom 통과를 기본 30일 검증 통과로 표시하지 않는다.

새 정책 v3 Portfolio 근거는 전체 canonical Artifact snapshot과 모든 구성 Instance의 coverage를 함께 봉인한다. Live는 구성원 누락·중복·다른 source hash·stream·epoch·관찰 구간과 부족한 coverage를 거부한다. 기존 v2 근거의 기존 계약은 보존한다.

## 확인한 범위와 남은 관찰

- 공유 정책/계약/Artifact 회귀 42개, Live 후보·승인·native HTTP·dispatch 회귀 191개, 후보 화면 mocked API/UI 검증 36개 통과. Live 회귀는 임시 데이터·가짜 자격 증명과 외부 HTTP 차단을 사용했다.
- 실제 새 Backtester 8개 Portfolio의 Finance reference/source bundle을 공유 검증기로 읽어 모두 유효함을 확인했다. 이는 원본 snapshot, Final/OOS, 독립 재생, manifest·source graph·비용·통화 결합의 확인이며 Paper 관찰이나 수익성 통과를 뜻하지 않는다.
- 실제 새 Paper 후보 발행은 자연 발생 완료 거래와 관찰 근거가 갖춰진 후에 가능하다. 미발행 상태를 후보 PASS로 만들지 않는다.
- Live Portfolio 실행은 모든 child의 정확한 전략/Instance별 Paper qualification도 계속 요구한다. 대표 전략 한 건의 계좌 심사만으로 모든 child의 실행 조건이 충족되었다고 표시하지 않는다. 기존 broker/mode 및 주문 안전 가드는 유지한다.

실제 계좌 잔액 조회·실주문·실제 배포 심사는 이 개발 검증에서 실행하지 않았다.
