[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
  [switch]$Apply,
  [Parameter(Mandatory = $true)]
  [string]$GitHubRepository,
  [string]$AuthorityRoot = "D:\crypto-first-live-authority",
  [string]$SharedRoot = "D:\crypto-first-live-shared",
  [Security.SecureString]$GitHubBootstrapToken,
  [string]$RevocationApprovalPhrase = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ServeTaskName = "CryptoFirstLive-SupervisedGitAuthority"
$TransientTaskName = "CryptoFirstLive-SupervisedGitAuthority-Provision"
$BrokerTaskNames = @("CryptoFirstLive-UpbitAuthority", "CryptoFirstLive-BinanceObserver")
$RequiredApproval = "REVOKE SUPERVISED AUTHORITY WITHOUT DELETING REMOTE ANCHOR"

function Assert-ElevatedAdministrator {
  $principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
  if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "elevated-administrator-required"
  }
}

function Write-Utf8NoBom([string]$Path, [string]$Value) {
  $encoding = New-Object System.Text.UTF8Encoding($false)
  [IO.File]::WriteAllText($Path, $Value, $encoding)
}

function Convert-SecureTokenToPlain([Security.SecureString]$Value) {
  if ($null -eq $Value) { throw "github-bootstrap-token-required-for-key-revocation" }
  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
  try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
  finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
}

function Invoke-GitHubApi([string]$Method, [string]$Path, [string]$Token) {
  return Invoke-RestMethod `
    -Method $Method `
    -Uri "https://api.github.com$Path" `
    -Headers @{
      "Accept" = "application/vnd.github+json"
      "Authorization" = "Bearer $Token"
      "User-Agent" = "crypto-first-live-supervised-revoker"
      "X-GitHub-Api-Version" = "2022-11-28"
    }
}

function Assert-ExactProtectedTarget([string]$Path, [string]$ExpectedRoot) {
  $full = [IO.Path]::GetFullPath($Path)
  $root = [IO.Path]::GetFullPath($ExpectedRoot).TrimEnd('\') + '\'
  if (-not $full.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) {
    throw "revocation-target-outside-protected-root:$full"
  }
  return $full
}

$authorityFull = [IO.Path]::GetFullPath($AuthorityRoot).TrimEnd('\')
$sharedFull = [IO.Path]::GetFullPath($SharedRoot).TrimEnd('\')
if ($authorityFull -ne "D:\crypto-first-live-authority" -or $sharedFull -ne "D:\crypto-first-live-shared") {
  throw "supervised-authority-exact-roots-required"
}
if ($GitHubRepository -notmatch '^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$') {
  throw "github-repository-invalid"
}

$receiptPath = Join-Path $AuthorityRoot "provisioning-receipt.json"
$receipt = $null
if (Test-Path -LiteralPath $receiptPath) {
  $receipt = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
}
$plan = [ordered]@{
  apply = [bool]$Apply
  mutationPerformed = $false
  taskName = $ServeTaskName
  taskWillBeStoppedAndRemoved = $true
  deployKeyWillBeRevoked = $true
  privateSigningKeyWillBeDestroyed = $true
  privateSshKeyWillBeDestroyed = $true
  pipeAuthKeyWillBeDestroyed = $true
  remoteAnchorRefWillBeDeleted = $false
  remoteRulesetsWillBeDeleted = $false
  remoteRepositoryWillBeDeleted = $false
  formalWorm = $false
  requiredRevocationPhrase = $RequiredApproval
  provisioningReceiptPresent = ($null -ne $receipt)
}
if (-not $Apply) {
  $plan | ConvertTo-Json -Depth 6
  return
}

if ($RevocationApprovalPhrase -cne $RequiredApproval) {
  throw "exact-supervised-authority-revocation-approval-required"
}
Assert-ElevatedAdministrator
if ($null -eq $receipt -or $receipt.schemaVersion -ne "crypto-first-live-supervised-authority-provisioning-receipt/v1") {
  throw "valid-supervised-authority-provisioning-receipt-required"
}
if ([string]$receipt.githubRepository -ne $GitHubRepository -or [int64]$receipt.deployKeyId -le 0) {
  throw "supervised-authority-revocation-binding-changed"
}
if (-not $PSCmdlet.ShouldProcess(
  "$AuthorityRoot and https://github.com/$GitHubRepository/keys/$($receipt.deployKeyId)",
  "Stop authority, revoke deploy key, and destroy only local private keys; preserve anchor ref and rulesets"
)) { return }

$token = Convert-SecureTokenToPlain $GitHubBootstrapToken
try {
  foreach ($taskName in @($ServeTaskName, $TransientTaskName) + $BrokerTaskNames) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
      Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
      Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }
  }

  $me = Invoke-GitHubApi "GET" "/user" $token
  [void](Invoke-GitHubApi "DELETE" "/repos/$GitHubRepository/keys/$($receipt.deployKeyId)" $token)

  $privateTargets = @(
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\audit-signing-ed25519-private.pem") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\github-deploy-ed25519") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $SharedRoot "pipe-auth.key") $SharedRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\upbit-signing-ed25519-private.pem") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\binance-signing-ed25519-private.pem") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\upbit-credential.dpapi") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\binance-credential.dpapi") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\upbit-pipe-auth.key") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $AuthorityRoot "secrets\binance-pipe-auth.key") $AuthorityRoot),
    (Assert-ExactProtectedTarget (Join-Path $SharedRoot "upbit-pipe-auth.key") $SharedRoot),
    (Assert-ExactProtectedTarget (Join-Path $SharedRoot "binance-pipe-auth.key") $SharedRoot)
  )
  $destroyed = New-Object 'System.Collections.Generic.List[object]'
  foreach ($target in $privateTargets) {
    if (Test-Path -LiteralPath $target -PathType Leaf) {
      $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $target).Hash.ToLowerInvariant()
      Remove-Item -LiteralPath $target -Force
      [void]$destroyed.Add([ordered]@{ path = $target; priorSha256 = $hash; destroyed = $true })
    } else {
      [void]$destroyed.Add([ordered]@{ path = $target; priorSha256 = ""; destroyed = $false })
    }
  }
  $revocation = [ordered]@{
    schemaVersion = "crypto-first-live-supervised-authority-revocation-receipt/v1"
    revoked = $true
    revokedEpoch = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    githubRepository = $GitHubRepository
    githubRevocationAdministrator = [string]$me.login
    deployKeyId = [int64]$receipt.deployKeyId
    deployKeyRevoked = $true
    tasksRemoved = $true
    destroyedSecrets = @($destroyed)
    remoteAnchorRefPreserved = $true
    remoteRulesetsPreserved = $true
    remoteRepositoryPreserved = $true
    formalWorm = $false
    brokerApiRequestCount = 0
    orderMutationCount = 0
  }
  $revocationPath = Join-Path $AuthorityRoot "revocation-receipt.json"
  Write-Utf8NoBom $revocationPath (($revocation | ConvertTo-Json -Depth 10) + "`n")
  $plan.mutationPerformed = $true
  $revocation | ConvertTo-Json -Depth 10
} finally {
  $token = $null
  Remove-Variable GitHubBootstrapToken -ErrorAction SilentlyContinue
}
