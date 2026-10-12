# 정상 Paper → 비활성 Live 후보 인계와 native 수량 계약 (2026-10-12)

현재 작업의 종착지는 실제 Paper 근거를 정상 발행하고 Live의 계좌 미연결 `draft / MONITOR` 후보로 가져오는 단계다. 실주문, 계좌 심사 확인 문구 입력, arming, 자동매매 시작은 수행하지 않는다.

## 현재 준비 원본과 승급 조건

기존 20개 원본의 Source/Instance/Portfolio/실제 PUBLISHED 데이터/현재 승인 record 결합을 읽기 전용으로 검사했다. 20개 모두 현재 봉인이 일치했고, standalone 12개의 실행 범위 hash와 Live reader가 일치했다. Live의 Portfolio member reader를 포함한 graph 구조 점검도 20/20이었다. 독립 Portfolio Finance 기준 8개는 8/8 유효했다. 이 결과는 Paper 운용 완료나 Live 자격을 뜻하지 않는다.

기존 19개 입장 시험은 자연 운용 표본·주문 없이 정상 종료했으므로 승급 근거가 없다. 실제 관측한 기존 Upbit 세션은 수동 LOCAL 정리 이력에 따라 TAINTED이며 사용하지 않는다. canonical 폴더의 7월 Evidence 두 파일은 publication-v2 qualification이 없어서 소비자가 거부했다. 과거 승인 record에 Source hash가 기록되지 않았다는 사실도 그대로 표시한다. 새 교정 graph와 새 Session 근거는 이전 세션을 소급해서 통과시키지 않는다.

현재 CUSTOM 정책은 Session에 봉인된 기준만 소비한다. 예를 들어 관찰 300초, 주문 1개 이상, 시장 상태 1개, 자연 완료 왕복 1개 등 최소 숫자를 정했더라도 무결성, 모든 Portfolio member 경계, 독립 신호 재생, 복구, 최신 원장 대조, 정상 STOPPED/flat/final seal이 모두 필요하다. 최소 시간이 지난 것만으로 승급하지 않으며 기본 30일 기준을 통과했다고 표시하지 않는다. 새 corrected Source의 known-holdout 재검증은 새로운 untouched Final로 부르지 않는다.

## 수량 모델과 자본 기준

새 `executionSizing.quantityContract`가 있는 long/spot Source만 공유 `execution_quantity` 모델을 소비한다. Source가 선언한 `PORTFOLIO_EQUITY_WITH_CASH_CAP` 기준은 현재 배포 자본 × 정규화된 canonical Portfolio 비중 × Source 포지션 비율이다. 현재 주문가능현금은 별도의 상한이다. 계좌 주문가능현금 100%를 초기 사용 한도로 정한 것과 매번 남은 현금의 5%를 사용하는 것은 다르다. 다른 보유자산을 배포 자본에 자동으로 포함하지 않는다.

현재 Live에는 정확한 현재 배포 자본·최신 계좌 수수료·현재 수량 규칙을 생산하는 native preflight 연결이 완성돼 있지 않다. 그 값이 없으면 native 수량은 BLOCKED이며 주문가능현금이나 계좌 전체 equity로 대신하지 않는다. 이번 작업에서 별도의 금융 운용 체계를 추가하지 않는다. 기존 short/legacy 모델은 유지하며 KIS는 이번 Binance/Upbit native spot 계약 대상이 아니다.

모델은 Decimal로 수수료와 슬리피지를 계산하고 수량 단위 아래로 내린다. 실제 계좌 수수료가 원본 가정보다 높으면 보수적인 수수료를 사용했다는 차이를 표시한다. min quantity/notional 미충족, 현재 규칙과 Source의 차이, 오래된 조회, 다른 배포/Source/Instance/Portfolio, 미확인 정밀도·시장 평균가격, float로 옮길 때 정확한 단위가 손실되는 값은 제출 수량으로 확정하지 않는다. 매도는 실제 보유 수량과 잔여 dust를 별도로 다룬다. `READY`는 수학 결과이며 `nativeEligibility=UNKNOWN`은 실제 주문 준비 완료가 아니다.

같은 가상 입력의 세 실제 순수 소비자를 Fraction 계산과 독립 대조했다. raw price 31, slippage 0.003, 현금 상한 31.092999999999996, step 0.001에서 Backtester의 사전 float 계산이 1.0 lot을 만들고 Paper/Live/정확 계산은 0.999를 만드는 반례를 보존했다. Backtester의 실제 native BUY/SELL 경로를 raw price/Decimal 비용으로 교정한 뒤 13개 입력에서 세 소비자의 수량과 매수 비용/매도 순유입 모델이 모두 같은지 독립 재검사했다. 이 수치 점검은 실제 자연 운용, 신호 재현성, 최종 원장, 수익률/MDD 또는 native 거래 권한을 대신하지 않는다.

## 정확한 비활성 가져오기 도구

`scripts/import_inactive_paper_candidate.py`는 기본적으로 읽기 전용 계획만 출력한다. 하나의 `evidence-id`, 기대 Source ID/hash, Instance ID, Portfolio ID/hash와 현재 configured root를 명시한다. `--apply`를 지정했을 때만 현재 publication과 registry/revision/hash를 다시 읽고 기존 `import_paper_candidate` CAS 서비스를 호출한다.

정상 등록 뒤 `draft`, `MONITOR`, `live-account-unresolved`와 `live_allowed/live_small_eligible/live_eligible=false`를 다시 검사한다. 중복 재실행은 동일 근거의 기존 비활성 후보를 유지한다. 잘못된 graph, 미설정 root, 자격 없는 Evidence, 이미 계좌에 연결됐거나 활성화된 배포를 덮어쓰지 않는다. 이 도구에는 계좌 심사, challenge/confirmation, arming, runtime 시작, broker 호출 기능이 없다.

실제 발행본이 isolated Paper 폴더에 있으면 동일한 봉인 graph와 동일한 genuine Evidence를 설정된 canonical root에 정상 인계한 뒤 가져온다. 아직 genuine publication이 없을 때는 가져오기 0개가 올바른 결과다.

## 검증 범위와 기록

- `tests/test_native_spot_quantity.py`: 네트워크를 차단한 14개 순수/fake consumer 시험.
- `tests/test_inactive_paper_candidate_tool.py`: 가상 저장소의 읽기 계획, 정확한 비활성 등록, idempotency, 다른 graph 및 기존 활성 배포 거부.
- 기존 inbox/import/review/읽기 전용 준비/Source 준비 회귀와 frontend workflow를 함께 검사한다.
- `output/live-promotion-20261012/current20-normal-promotion-readiness.json`: 기존 20개의 정확한 상태와 차단 항목.
- 같은 폴더의 `native-quantity-consumer-tests.json`, `three-consumer-independent-quantity-audit.json`: 현재 코드의 테스트/수치 범위.
- `three-consumer-independent-quantity-audit-before-float-fix.json`: 처음 검출한 실제 1 lot 반례를 보존한 기록.

QA 결과의 같은 입력 비교는 가상 수치 시험이다. 실제 자연 Paper 근거 발행 및 비활성 import 수는 별도 실제 수행 결과로 기록한다. 원래 변경 70개, Source/Final, 운영 원장, 계좌 설정과 비밀 저장소를 보존한다. 최종 executable은 종속 shared source 동결 후 다시 빌드하고 PyArrow Parquet 및 관련 compiled module을 확인한다.
