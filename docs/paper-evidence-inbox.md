# Paper 검증 근거 확인

Paper에서 발행한 검증 근거와 현재 불변 전략·인스턴스·포트폴리오를 대조하는 읽기 전용 화면입니다.
선택한 배포 전략 패널에서 **Paper 검증 근거 확인**을 펼치고 **Paper 검증 근거 새로고침**을 누릅니다.
아직 Live에 없는 신규 후보를 숨기지 않도록 전체 Paper 후보를 표시합니다.

1. 전략과 Evidence ID, 확인 결과를 읽습니다.
2. 일치한 항목은 봉인 정보 보기에서 Evidence/Bundle/Publication/Binding hash, 세션과 현재 인스턴스를 확인합니다.
3. 버전·인스턴스가 바뀌었거나 봉인이 손상되면 차단 사유를 확인합니다.
4. 조회 실패 시 이전 결과를 현재 정상 결과로 유지하지 않습니다. 새로고침 중에는 중복 요청을 막습니다.

API는 인증된 GET /api/paper-candidates만 추가했습니다. 자동 조회·POST 인수·배포 생성·권한 변경·주문 호출은 없습니다.
standalone은 현재 인스턴스 전체 hash로 봉인 당시 실행 범위를 재계산하고,
Portfolio는 모든 자식의 원본 Artifact, templateInstanceId/sourceInstanceHash와 실행 설정을 대조합니다.

공유 DeploymentStore에는 앱 간 동시 변경 CAS/공통 잠금이 없고,
새 draft/OFF 후보를 제한 실거래 준비 단계로 심사하는 최초 승인 절차도 연결되지 않았습니다.
이 두 계약을 먼저 해결해야 등록부에 쓰는 실제 인수 기능을 추가할 수 있습니다.
따라서 VERIFIED_READ_ONLY는 현재 저장본과 근거의 일치 표시이며, 인수 완료나 실거래 승인이 아닙니다.

검증: 신규 Python 11개와 기존 관련 계약 30개, JSX·실제 API의 격리 UI 검사 22개 통과.
UI 검사는 fetch를 전량 대체해 GET 횟수와 오류 처리를 검증했으며 실제 Live 프로그램/서버는 실행하지 않았습니다.
