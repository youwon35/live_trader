# Upbit 독립 관측 2시간 기능시험 운영 Runbook

## 현재 결론: HOLD

기능시험 계약은 `KRW-BTC`, 최대 `10,000 KRW`, 7,200초, BUY 최대 1회,
SELL 최대 1회, 재진입 없음, 세션 소유 주문만 정리다. 출금·이체·마진·선물은
항상 금지다. 결과는 `SUPERVISED_NON_PROMOTION`이며 전략 승급 또는
`REAL_E2E` 근거가 아니다.

현재 source의 release/network latch는 모두 `false`다. 안전하게 구현·검증된
범위는 다음뿐이다.

- 공용 coordinator `APPROVED_INERT` owner heartbeat: network/entry authority 0
- 독립 authority의 Ed25519/SQLite/MyOrder/GET-only/outbox 구현
- exact-origin/no-redirect/no-retry GET 전송기
- protected bundle manifest/ACL/source-hash verifier
- native status/reprepare/STOP/recover UI

아직 허용되지 않은 P0는 SYSTEM prearmed pipe가 `APPROVED_INERT`의 exact
owner/session을 받은 뒤 MyOrder/GET 관측을 시작하는 launch capability다. 이
seam과 공용 `ACTIVE`/network 전이는 사용자가 그 잔여 위험을 정확히 승인하기
전에는 추가하거나 실행하지 않는다. 따라서 이 문서의 점검 명령은 주문,
취소, DELETE, Upbit GET, MyOrder 연결을 만들지 않는다.

## 신뢰 경계와 canonical 실행물

canonical trader는 현재 source 한 개다. 기존 `release\LiveTrader.exe`는 이번
frozen dependency manifest로 다시 빌드·검증되지 않았으므로 사용하지 않는다.

```powershell
Set-Location 'D:\github\PROGRAM\trading-system\apps\live_trader'
.\.venv\Scripts\python.exe -m live_trader
```

이 source/.venv는 trader 전용이다. authority를 이 경로에서 실행하면 안 된다.
authority의 유일한 허용 경계는 다음과 같다.

- private root: `D:\crypto-first-live-authority`
- shared root: `D:\crypto-first-live-shared`
- authority Python: `D:\crypto-first-live-authority\venv\Scripts\python.exe`
- authority source: `D:\crypto-first-live-authority\app\broker_authorities\upbit\runtime`
- launcher: `D:\crypto-first-live-authority\launch-authority.ps1`
- Windows identity: `LocalSystem` (`S-1-5-18`)
- scheduled task: `CryptoFirstLive-UpbitAuthority`

private root는 inheritance를 제거하고 SYSTEM/Administrators만 FullControl이어야
한다. shared root는 여기에 exact trader SID의 ReadAndExecute만 더한다. private
key, machine-DPAPI credential blob, private SQLite, pipe authority key는 trader가
읽을 수 없어야 한다.

launcher는 실행 직전 `bundle-manifest.json`의 모든 sealed file SHA256과
no-extra set을 검증한다. `pinnedFiles`와 external binaries도 각각 hash를
검증한다. `PYTHONNOUSERSITE=1`, `PYTHONDONTWRITEBYTECODE=1`, Python `-I`가
필수다. repo source나 repo `.venv`로 fallback하지 않는다.

## 1. trader 시작과 network-free 상태 확인

trader 사용자 문맥에서 위 canonical 명령으로 앱을 시작한다. native
`기능시험` 화면에서 Upbit 카드와 공용 상태를 확인한다.

필수 사실은 다음과 같다.

- `release=false`, `networkOrderPostAllowed=false`
- `phase`는 `IDLE`, `FINALIZED`, 또는 `APPROVED_INERT`; `ACTIVE`이면 중단
- Kill/STOP과 application/account lease 상태가 읽힘
- Binance lane이 active/cleanup이 아님
- Upbit account fingerprint와 owner identity hash가 64자 lowercase SHA256

상태·재준비는 pywebview HttpOnly cookie와 native CSRF bridge만 사용한다.
`curl`, 일반 브라우저, DevTools token 복사는 금지다.

## 2. protected bundle 계획만 검증

아래는 `-Apply`가 없는 network/mutation-free 계획 모드다. descriptor와 source
hash는 최종 frozen report의 값을 그대로 넣는다. private dedicated authority
repository 이름은 trader code repository와 달라야 한다.

```powershell
Set-Location 'D:\github\PROGRAM\trading-system\apps\live_trader'
$descriptor = '.\scripts\crypto_first_live_supervised_broker_bundle.json'
$descriptorSha = '<FROZEN_DESCRIPTOR_SHA256>'
$authorityToolSha = '<FROZEN_GIT_AUTHORITY_TOOL_SHA256>'
$anchorSha = '<FROZEN_SUPERVISED_ANCHOR_SHA256>'

.\scripts\provision_crypto_first_live_supervised_git_authority.ps1 `
  -GitHubRepository '<INDEPENDENT_ADMIN>/<EMPTY_PRIVATE_AUTHORITY_REPO>' `
  -TraderDataRoot 'C:\Users\youwo\AppData\Local\trading-system\live-trader' `
  -BrokerBundleDescriptorPath $descriptor `
  -ExpectedBrokerBundleDescriptorSha256 $descriptorSha `
  -ExpectedAuthorityToolSha256 $authorityToolSha `
  -ExpectedAnchorModuleSha256 $anchorSha
```

계획 출력은 `apply=false`, `mutationPerformed=false`, `brokerApiAllowed=false`,
`orderAllowed=false`, exact protected roots와 두 broker mode를 보여야 한다.

## 3. Apply는 현재 금지

`-Apply`는 protected roots, machine-DPAPI credential blobs, signing keys,
scheduled tasks, independent Git authority를 실제 생성하고 prearmed broker task를
시작한다. 현재 Upbit prearmed launch seam이 safety HOLD이므로 실행하지 않는다.

다음 조건이 모두 충족된 별도 release tranche 뒤에만 Apply 명령을 확정한다.

1. 사용자가 supervised non-promotion의 수동거래/API-key 인벤토리 잔여 위험과
   prearmed MyOrder/GET launch capability를 exact phrase로 승인한다.
2. public client와 SYSTEM server가 local-only named pipe, peer SID, 32-byte HMAC,
   one-use exact request를 검증한다.
3. ACK 전 authenticated GET/MyOrder/order/cancel/DELETE/withdraw count가 모두 0다.
4. ACK 뒤 first signed snapshot이 5초 window, current key exact 1, foreign activity
   0, exact bot 1, redirect/retry/mutation 0을 만족한다.
5. 실패·timeout·daemon crash·stream/sequence gap은 global revoke와
   `CLEANUP_ONLY`로 간다.
6. HTTP/UI body는 pipe key, private config, signed receipt, observer snapshot을
   제출하거나 돌려받지 않는다.

Git non-WORM 승인 문구 하나는 위 broker launch 위험 승인을 대신하지 않는다.

## 4. authority 구현의 검증 계약

frozen authority bundle에는 order/cancel builder가 없어야 한다. 필요한 runtime
dependency만 isolated import root에 포함하고 repo의 app-bootstrap
`live_trader/__init__.py` 및 대형 `trading_runtime/__init__.py`는 포함하지 않는다.
authority는 다음을 fail closed한다.

- official origin 외 URL, POST/DELETE/body, redirect, retry
- `/v1/api_keys` current nonexpired key exact 1이 아님
- account-wide open 모든 page 또는 recent closed <=7d가 불완전
- authenticated all-market MyOrder ACK/continuous coverage가 없음
- session identifier prefix 밖 account order event 한 건 이상
- application/account OS lease owner PID가 다름
- protected source/venv hash, manifest hash, ACL, private key/config가 변경됨
- daemon restart/crash, event cursor/chain/sequence gap

proof loss는 private durable latch이며 지우거나 다른 key로 덮어쓰지 않는다.
trader는 public key/verifier pin/signed proof와 durable cursor만 소비한다.

## 5. native 재준비

앱의 `기능시험` 화면에서 `설정·핀 다시 준비`를 누른다. exact body는 `{}`다.
정상 응답은 owner가 전후 동일하고 config/pin/path reread와 startup audit가
완료됐으며 network/GET/order/cancel/DELETE/candidate/approval mutation count가
모두 0임을 보여야 한다. active/candidate/cleanup/reconciliation 중에는
재준비가 거절돼야 한다.

현재 protected authority public material과 prearmed handshake가 연결되지
않았으므로 Upbit `시작`은 계속 disabled여야 한다. 이 HOLD를 환경변수나 DB
수정으로 우회하지 않는다.

## 6. 승인 뒤의 목표 순서(현재 실행 금지)

향후 허용되는 exact 순서는 다음 하나뿐이다.

1. broker candidate를 inert하게 준비한다.
2. 공용 coordinator가 exact owner를 `APPROVED_INERT`로 reserve한다.
3. 이미 실행 중인 SYSTEM authority에 server-owned launch request를 보낸다.
4. authority가 GET/MyOrder/mutation 0인 ACK를 먼저 반환한다.
5. authority가 MyOrder coverage를 연 뒤 signed GET baseline과 outbox proof를
   생성한다.
6. state가 signed snapshot/outbox high-water를 검증한다.
7. server-owned one-use approval을 issue/consume한다.
8. 별도 supervised activation release가 있을 때만 global/broker ACTIVE로 간다.

수동으로 daemon을 시작하거나 파일을 복사하는 단계는 없다. timeout 뒤 같은
approval을 재사용하지 않는다.

## 7. STOP, Kill, recovery 및 terminal

release가 HOLD여도 STOP/recover/Kill은 잠기면 안 된다. 정상 STOP은 신규 entry를
먼저 durable revoke한 뒤 세션 소유 cleanup만 허용한다. daemon crash, foreign
activity, 다른 key/bot, stream gap은 `SAFE_INCOMPLETE`, non-promotion 결과다.

최종 완료에는 scheduler stopped, working order 0, coordinator `FINALIZED`,
terminal evidence hash 검증, memory bearer 제거, authority FINAL proof chain,
restart-verifiable private/public high-water가 모두 필요하다. raw access/secret,
JWT, order UUID, private key, pipe auth key는 화면·로그·보고서에 복사하지 않는다.

현재는 위 terminal 경로를 실제로 시작할 authority launch가 HOLD이므로 2시간
실거래 완료를 주장할 수 없다.
