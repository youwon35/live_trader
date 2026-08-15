# 감독형 비승급 최초 실거래 authority 운영 절차

이 절차는 formal WORM을 대신하는 승급 근거가 아니다. 원격 Git 관리자는
관리 권한으로 이력을 다시 쓸 수 있으므로, 사용자가 그 잔여 위험을 정확히
승인한 **1회·1개 lane·2시간 감독형 기능시험**에만 쓸 수 있다.

현재 두 코드 플래그는 모두 `false`다.

- `SUPERVISED_NON_PROMOTION_RELEASED = False`
- `SUPERVISED_NON_PROMOTION_NETWORK_CAPABILITY_RELEASED = False`

따라서 아래 authority를 구성하고 승인 영수증을 소비해도 현재 코드에서는
주문 네트워크 권한이 열리지 않는다. production promotion, REAL_E2E, formal
WORM 플래그도 계속 `false`다.

## 보안 경계

트레이더 계정과 authority 실행 SID는 서로 달라야 한다. 현재 고정 실행 SID는
`LocalSystem` (`S-1-5-18`)이며, 자동 시작 트리거가 없는 SYSTEM Scheduled Task로만
실행한다.

- 트레이더: 공개 Ed25519 키와 named-pipe transport key만 읽는다.
- authority: SYSTEM만 Ed25519 개인키, 전용 Git clone, repo-scoped SSH deploy key를
  읽으며 원격 exact ref push 자격을 가진다.
- 원격 Git: 같은 PC의 bare repository가 아닌 HTTPS/SSH 원격이어야 한다.
- 원격 ref: force push와 삭제를 금지하고 authority deploy key만 쓰기 권한을
  갖는다. 트레이더 계정/키에는 push 권한을 주지 않는다.
- 개인키와 전용 clone ACL: authority SID, LocalSystem, Administrators만 읽고
  쓸 수 있어야 한다. authority CLI가 SID, 경로 분리, ACL, URL/ref pin을
  시작 전에 다시 검사한다.
- authority Python bundle/venv/config/launcher도
  `D:\crypto-first-live-authority` 아래 SYSTEM/Administrators-only ACL과 exact
  SHA-256 manifest로 봉인한다. 트레이더가 수정할 수 있는 checkout나 `.venv`에서
  authority를 실행하지 않는다.
- named pipe는 `multiprocessing.connection`을 사용하지 않는다. Win32 ctypes로
  `PIPE_REJECT_REMOTE_CLIENTS`, 64 KiB length-prefix, bounded overlapped I/O,
  HMAC-SHA256 transport binding을 사용한다. 보호 DACL은 Anonymous를 명시적으로
  거부하고 SYSTEM/Administrators/정확한 trader SID만 허용한다. 서버와 클라이언트는
  상대 프로세스 SID를 매 연결마다 다시 확인한다.

이 구성도 formal WORM은 아니다. Git 관리자나 같은 호스트 관리자는 감사를
훼손할 수 있다는 잔여 위험이 계약에 영구 기록된다.

## authority exact config

authority 계정이 읽는 JSON은 아래 필드만 가져야 한다. 모든 경로는 절대
경로이고 `authorityRepoPath`, `privateKeyPath`는 trader data root 밖이다.

```json
{
  "schemaVersion": "crypto-first-live-supervised-git-authority-config/v1",
  "authorityId": "supervised-authority-0001",
  "namespaceId": "supervised-namespace-0001",
  "keyId": "supervised-key-0001",
  "authorityOsSid": "S-1-5-18",
  "traderOsSid": "S-1-5-21-...-1001",
  "authorityRepoPath": "D:\\crypto-first-live-authority\\repo",
  "traderDataRoot": "C:\\Users\\trader\\AppData\\Local\\live_trader",
  "privateKeyPath": "D:\\crypto-first-live-authority\\secrets\\ed25519-private.pem",
  "pipeAuthKeyPath": "D:\\crypto-first-live-shared\\pipe-auth.key",
  "pipeAddress": "\\\\.\\pipe\\crypto-first-live-supervised",
  "remoteName": "origin",
  "remoteRef": "refs/heads/crypto-first-live-supervised-anchor",
  "remoteUrlSha256": "<exact lowercase SHA-256 of git remote get-url origin>",
  "statePath": "audit/crypto-first-live-supervised-state.json"
}
```

개인키는 Ed25519 PEM이다. pipe key는 32바이트 raw 또는 64자 lowercase/uppercase
hex 파일이다. pipe key는 signing key가 아니며 두 프로세스의 transport
handshake에만 쓴다. 공개키 PEM만 트레이더가 읽을 수 있는 위치로 복사한다.

## protected bundle canonical launch

현재 canonical authority는 provisioning script가 exact source SHA pin에서 복사한
SYSTEM-protected bundle/venv다. 일반 checkout source나 기존 LiveTrader EXE는
canonical이 아니다. 기본 plan 모드는 파일·GitHub·task를 변경하지 않는다.
현재 소스의 `$ProtectedBundleProvisioningApplyReleased=false`와
`$BrokerNetworkReleaseAllowed=false`는 고정 HOLD다. 따라서 아래 plan 명령만
실행 가능하고, `-Apply`는 잔여위험 문구를 주더라도
`protected-bundle-provisioning-release-held`로 종료된다. broker agent의 최종
frozen descriptor/source pin, prearmed authority hostile regression, 별도 사용자
잔여위험 승인이 모두 끝나기 전 이 상수를 변경하면 안 된다.

```powershell
cd D:\github\PROGRAM\trading-system\apps\live_trader
.\scripts\provision_crypto_first_live_supervised_git_authority.ps1 `
  -GitHubRepository <independent-owner/private-empty-anchor-repo> `
  -TraderDataRoot C:\Users\youwo\AppData\Local\live_trader `
  -TraderOsSid S-1-5-21-4199057202-2653993499-446139946-1001
```

아래 `-Apply` 형식은 HOLD 해제 뒤의 검토용 예시일 뿐 현재 실행 가능한
명령이 아니다. 향후 사용자가 supervised Git이 formal WORM이 아니라는 잔여 위험을
명시적으로 승인한 뒤, 관리자 권한 PowerShell에서만 실행한다. private empty
dedicated GitHub repository와 그 repository를 관리할 별도 bootstrap token이
먼저 필요하다. 현재 개인 PAT/SSH 키는 bundle이나 task에 복사하지 않는다.
source hash 두 개는 바로 앞 plan 출력값과 정확히 같아야 한다.

```powershell
$token = Read-Host "Independent GitHub bootstrap token" -AsSecureString
.\scripts\provision_crypto_first_live_supervised_git_authority.ps1 `
  -Apply `
  -GitHubRepository <independent-owner/private-empty-anchor-repo> `
  -TraderDataRoot C:\Users\youwo\AppData\Local\live_trader `
  -TraderOsSid S-1-5-21-4199057202-2653993499-446139946-1001 `
  -ExpectedAuthorityToolSha256 <plan output> `
  -ExpectedAnchorModuleSha256 <plan output> `
  -ExpectedCredentialRewrapToolSha256 <plan output> `
  -BrokerBundleDescriptorPath <frozen ready descriptor> `
  -ExpectedBrokerBundleDescriptorSha256 <descriptor sha256> `
  -GitHubBootstrapToken $token `
  -ResidualRiskApprovalPhrase "I ACCEPT SUPERVISED GIT IS NOT FORMAL WORM"
```

HOLD가 별도 검토로 해제되고 승인된 뒤에만 이 명령은 다음 변경을 하도록
설계되어 있다.

- 별도 audit signing Ed25519 PEM, 별도 repo-scoped SSH deploy key, 32-byte pipe
  key를 만든다.
- private empty repository에 exact anchor writer/integrity, other-branch, all-tag
  ruleset을 만들고 deploy key만 정상 fast-forward update를 할 수 있게 한다.
  전용 저장소의 GitHub Actions를 끄고 재조회해 비활성 상태를 검증한다.
  authority는 각 원격 head의 Git tree가 초기 empty tree 또는 정확히
  `audit/crypto-first-live-supervised-state.json` 한 개의 regular blob인 경우만
  수용하므로 다른 파일이나 workflow가 섞이면 fail closed한다.
- SYSTEM one-shot check와 최초 ref provision을 수행한다.
- 자동 trigger가 없는 `CryptoFirstLive-SupervisedGitAuthority` SYSTEM task를
  등록하지만 시작하지 않는다.

사용자 감독 아래 authority를 시작할 때만 다음을 실행한다.

```powershell
Start-ScheduledTask -TaskName CryptoFirstLive-SupervisedGitAuthority
```

2시간 시험 중에는 SYSTEM task 상태·authority stdout/stderr·트레이더 화면을
계속 감시한다. task는 재부팅이나 로그온 때 자동으로 시작하지 않는다.

현재 `release\LiveTrader.exe`는 authority가 아니다. 추후 별도 authority EXE를
만들면 protected bundle과 동일 transitive manifest, public-key-only trader
bundle, private-key 미포함 검사를 통과한 빌드만 위 task를 대체할 수 있다.

### same GitHub administrator lower-assurance 예외

bootstrap token의 로그인 주체가 trader의 GitHub 관리자 `youwon35`와 같으면
기본적으로 fail closed다. 정말 별도 원격 관리자를 마련할 수 없고 사용자가
추가 잔여 위험을 정확히 승인한 경우에만 아래 두 인자를 더할 수 있다.

```text
-AllowTraderGitHubAdministrator
-TraderGitHubAdministratorApprovalPhrase "I ACCEPT TRADER GITHUB ADMIN CAN REWRITE THE ANCHOR"
```

이 예외는 SYSTEM SID/ACL 분리를 유지하지만 같은 GitHub 관리자가 ruleset과
ref를 바꿀 수 있으므로 명백히 lower-assurance이며 formal WORM이나 promotion
근거가 아니다.

## 트레이더 read-only 설정

트레이더 환경에는 다음 여섯 값만 둔다. 개인키, authority Git credential,
remote write token은 절대 넣지 않는다.

```text
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PIPE=\\.\pipe\crypto-first-live-supervised
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_AUTHORITY_ID=supervised-authority-0001
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_NAMESPACE_ID=supervised-namespace-0001
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_KEY_ID=supervised-key-0001
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PUBLIC_KEY=<absolute public PEM path>
LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PIPE_AUTHKEY=<absolute shared pipe-key path>
```

`prepare_crypto_first_live_coordinator_state()`는 파일·공개키·pipe 주소만
검사하고 authority에 연결하지 않는다. status의 `networkRequestCount`와
`networkOrderPostAllowed`는 0/false로 남는다.

## 비활성 승인 candidate

승인 전 준비 단계는 주문 권한을 열지 않는다.

1. IDLE coordinator와 단일 application/account OS lease를 확인한다.
2. exact session/permit/caps에 바인딩한 `APPROVAL` audit request를
   `crypto_first_live_supervised_audit_anchor(request)`에 전달한다.
3. 반환된 public-key-verified audit projection으로
   `issue_crypto_first_live_supervised_approval(request)`를 호출한다.
4. 화면에는 정확한 잔여 위험, lane, 7,200초, Upbit `10,000/1,000 KRW` 또는
   Binance `10/1 USDT` cap, 1회용 typed phrase를 함께 표시한다.
5. 사용자가 그대로 입력한 경우에만
   `consume_crypto_first_live_supervised_approval(request)`를 1회 호출한다.
6. consumed receipt는 재시작 후 검증되며 두 번째 소비는 거절된다. 이
   receipt에도 `networkCapabilityOpen=false`, `promotionEligible=false`다.

정확한 consumed receipt schema는
`crypto-first-live-supervised-user-approval-receipt/v1`이며 `approvalId`,
`approvalBindingHash`, `consumptionId`, `exactUserApproval=true`,
`consumed=true`, `oneUse=true`, `durable=true`, `restartVerifiable=true`,
`approvedEpoch`, `receiptHash`를 가진다.

## 승인 이후에만 가능한 one-use enabling 순서

아래 순서는 사용자의 감독형 잔여 위험 승인을 받은 뒤 별도 enabling
tranche가 두 supervised release latch를 좁게 열었을 때만 가능하다. 현재는
실행할 수 없다.

1. 다른 bot과 수동 거래가 없음을 fresh account-wide GET/stream으로 확인한다.
2. 한 broker가 exact 7,200초 local permit을 claim하고 session/permit/code/account
   binding을 확정한다.
3. 주문 없이 fresh official signed GET baseline과 외부 audit checkpoint를
   만든다.
4. global coordinator를 `reserve_inert`하고 evidence를 durable seal한다.
5. exact consumed user receipt를 global one-use final approval로 소비한다.
6. global ACTIVE가 확인된 뒤에만 broker control을 activate한다.
7. 각 final sender 경계에서 global reservation을 바깥쪽, broker owner lease를
   안쪽으로 잡고 Kill/STOP/hard-stop/account lease를 다시 검증한다.
8. 5초 heartbeat와 foreground monitor를 유지한다.
9. STOP/Kill/7,200초 종료 시 global entry를 먼저 revoke하고 broker cleanup,
   flat/account truth, terminal evidence seal, global finalize 순서로 닫는다.

두 lane을 동시에 열거나, consumed receipt를 재사용하거나, cap/permit/session을
바꾸거나, audit/heartbeat가 끊기면 새 진입은 fail-closed다.

## crash/장애 복구

- Git push 전 authority 장애: receipt 없음. reserve/activation 금지.
- Git push 후 pipe 응답 전 장애: 같은 exact request를 재시도한다. remote head의
  같은 request hash로 같은 서명 receipt를 반환하며 두 번째 commit은 만들지
  않는다.
- remote ref/CAS 변경: authority가 non-force push 또는 prior checkpoint 검증에서
  거절한다. 자동 force/rebase/retry 금지.
- 승인 candidate 소비 전 trader 장애: candidate는 만료되며 네트워크 권한은
  열린 적이 없다.
- 승인 receipt 소비 후 activation 전 장애: receipt는 재사용할 수 없다. startup
  audit 후 stale candidate를 폐기하고 새 exact session/approval을 발급한다.
- PREPARING/APPROVED_INERT에서 trader 장애: startup은 entry를 열지 않고
  CLEANUP_ONLY 또는 RECONCILIATION_REQUIRED로 복구한다.
- ACTIVE에서 trader/authority/anchor 장애: global entry를 우선 revoke하고
  cleanup-only reconciliation만 허용한다. 자동 재진입 금지.
- terminal ProgramLedger/coordinator commit 미확인: IDLE로 가정하지 않고 global
  reservation을 유지한 채 reconciliation한다.

어떤 장애에서도 자동으로 release flag를 바꾸거나 formal WORM/REAL_E2E/승급
근거로 변환하지 않는다.

## revoke / uninstall

revocation은 remote anchor ref와 ruleset을 지우지 않는다. task를 중지·제거하고,
repo-scoped deploy key를 먼저 원격에서 revoke한 뒤 local audit private key,
SSH private key, pipe key만 exact path로 파기한다. 재귀 삭제는 하지 않는다.

```powershell
$token = Read-Host "Independent GitHub bootstrap token" -AsSecureString
.\scripts\uninstall_crypto_first_live_supervised_git_authority.ps1 `
  -Apply `
  -GitHubRepository <independent-owner/private-empty-anchor-repo> `
  -GitHubBootstrapToken $token `
  -RevocationApprovalPhrase "REVOKE SUPERVISED AUTHORITY WITHOUT DELETING REMOTE ANCHOR"
```
