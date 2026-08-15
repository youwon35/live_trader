[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
  [switch]$Apply,
  [Parameter(Mandatory = $true)]
  [string]$GitHubRepository,
  [Parameter(Mandatory = $true)]
  [string]$TraderDataRoot,
  [string]$TraderOsSid = ([Security.Principal.WindowsIdentity]::GetCurrent().User.Value),
  [string]$AuthorityRoot = "D:\crypto-first-live-authority",
  [string]$SharedRoot = "D:\crypto-first-live-shared",
  [string]$PythonExecutable = "C:\Python314\python.exe",
  [string]$GitExecutable = "C:\Program Files\Git\cmd\git.exe",
  [string]$SshExecutable = "C:\Windows\System32\OpenSSH\ssh.exe",
  [string]$SshKeygenExecutable = "C:\Windows\System32\OpenSSH\ssh-keygen.exe",
  [string]$ExpectedAuthorityToolSha256 = "",
  [string]$ExpectedAnchorModuleSha256 = "",
  [string]$ExpectedCredentialRewrapToolSha256 = "",
  [string]$BrokerBundleDescriptorPath = "",
  [string]$ExpectedBrokerBundleDescriptorSha256 = "",
  [Security.SecureString]$GitHubBootstrapToken,
  [string]$ResidualRiskApprovalPhrase = "",
  [switch]$AllowTraderGitHubAdministrator,
  [string]$TraderGitHubAdministratorApprovalPhrase = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$SourceRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $SourceRoot "..\..")).Path
$AuthorityToolSource = Join-Path $SourceRoot "tools\crypto_first_live_supervised_git_authority.py"
$AnchorModuleSource = Join-Path $SourceRoot "live_trader\crypto_first_live_supervised_anchor.py"
$CredentialRewrapToolSource = Join-Path $SourceRoot "tools\crypto_first_live_broker_credential_rewrap.py"
$RemoteRef = "refs/heads/crypto-first-live-supervised-anchor"
$PipeAddress = "\\.\pipe\crypto-first-live-supervised"
$ServeTaskName = "CryptoFirstLive-SupervisedGitAuthority"
$TransientTaskName = "CryptoFirstLive-SupervisedGitAuthority-Provision"
$BrokerTaskNames = [ordered]@{
  UPBIT_AUTHORITY = "CryptoFirstLive-UpbitAuthority"
  BINANCE_OBSERVER = "CryptoFirstLive-BinanceObserver"
}
$RequiredApproval = "I ACCEPT SUPERVISED GIT IS NOT FORMAL WORM"
$RequiredSameAdminApproval = "I ACCEPT TRADER GITHUB ADMIN CAN REWRITE THE ANCHOR"
$ProtectedBundleProvisioningApplyReleased = $false
$BrokerNetworkReleaseAllowed = $false
$SystemSid = "S-1-5-18"
$AdministratorsSid = "S-1-5-32-544"

$PythonExecutableSha256 = "7ca24f26d6e3f463419ee4f537ddd3acd312c38fe45e678cce08572f26a8bd1a"
$GitExecutableSha256 = "29ffa27024ead2b084fec79b732a811c3ab07634b2bece3f7d89228801975959"
$SshExecutableSha256 = "6250fd52163fe99a0dc49403ed1b4bbef9b764bdb7bada017a93d057d9376a42"
$SshKeygenExecutableSha256 = "44c6809b7bbc917f1310ba92857f983e2788e9b0015aa7896fa0362eddb6338b"
$PythonDll = "C:\Python314\python314.dll"
$PythonDllSha256 = "a07f7d09c3121492bb066535c6d0811df5fbc2090cbca7031a97bb47ce1480c9"
$PycryptodomeWheelUrl = "https://files.pythonhosted.org/packages/54/2f/e97a1b8294db0daaa87012c24a7bb714147c7ade7656973fd6c736b484ff/pycryptodome-3.23.0-cp37-abi3-win_amd64.whl"
$PycryptodomeWheelSha256 = "c75b52aacc6c0c260f204cbdd834f76edc9fb0d8e0da9fbf8352ef58202564e2"
$GitHubEd25519HostKey = "AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
$GitHubEd25519HostKeyRawSha256 = "f83898df0bef57a4ee24985ba598ac17fccb0c0d333cc4af1dd92be14bc23aa5"

function Get-Sha256Lower([string]$Path) {
  return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Write-Utf8NoBom([string]$Path, [string]$Value) {
  $encoding = New-Object System.Text.UTF8Encoding($false)
  [IO.File]::WriteAllText($Path, $Value, $encoding)
}

function Assert-ExactHash([string]$Path, [string]$Expected, [string]$Label) {
  $actual = Get-Sha256Lower $Path
  if ($actual -ne $Expected.ToLowerInvariant()) {
    throw "$Label-sha256-changed:$actual"
  }
}

function Assert-ExactObjectProperties([object]$Value, [string[]]$Expected, [string]$Label) {
  if ($null -eq $Value) { throw "$Label-missing" }
  $actualNames = @($Value.PSObject.Properties.Name | Sort-Object)
  $expectedNames = @($Expected | Sort-Object)
  if (($actualNames -join "`n") -cne ($expectedNames -join "`n")) {
    throw "$Label-fields-not-exact"
  }
}

function Get-FrozenBrokerBundle {
  if (-not $BrokerBundleDescriptorPath -or -not (Test-Path -LiteralPath $BrokerBundleDescriptorPath -PathType Leaf)) {
    throw "frozen-broker-bundle-descriptor-required"
  }
  if ($ExpectedBrokerBundleDescriptorSha256 -notmatch '^[0-9a-fA-F]{64}$') {
    throw "frozen-broker-bundle-descriptor-sha256-required"
  }
  $descriptorFull = (Resolve-Path -LiteralPath $BrokerBundleDescriptorPath).Path
  $sourcePrefix = $SourceRoot.TrimEnd('\') + '\'
  if (-not $descriptorFull.StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "frozen-broker-bundle-descriptor-outside-source-root"
  }
  Assert-ExactHash $descriptorFull $ExpectedBrokerBundleDescriptorSha256 "broker-bundle-descriptor"
  $descriptor = Get-Content -LiteralPath $descriptorFull -Raw | ConvertFrom-Json
  Assert-ExactObjectProperties $descriptor @("schemaVersion", "readyForApply", "pythonWheels", "modes") "broker-bundle-descriptor"
  if ($descriptor.schemaVersion -cne "crypto-first-live-supervised-broker-bundle/v1" -or $descriptor.readyForApply -ne $true) {
    throw "frozen-broker-bundle-descriptor-not-ready"
  }
  $modes = @($descriptor.modes)
  if ($modes.Count -ne 2) { throw "frozen-broker-bundle-exact-modes-required" }
  $seenModes = @{}
  $seenDestinations = @{}
  $normalizedWheels = New-Object 'System.Collections.Generic.List[object]'
  foreach ($wheel in @($descriptor.pythonWheels)) {
    Assert-ExactObjectProperties $wheel @("name", "version", "url", "sha256") "broker-bundle-wheel"
    $wheelName = [string]$wheel.name
    $wheelVersion = [string]$wheel.version
    $wheelUrl = [string]$wheel.url
    $wheelHash = ([string]$wheel.sha256).ToLowerInvariant()
    if (
      $wheelName -notmatch '^[A-Za-z0-9_.-]{1,80}$' -or $wheelVersion -notmatch '^[A-Za-z0-9_.+-]{1,80}$' -or
      $wheelUrl -notmatch '^https://files\.pythonhosted\.org/packages/[A-Za-z0-9/_.+-]+\.whl$' -or
      $wheelHash -notmatch '^[0-9a-f]{64}$'
    ) { throw "frozen-broker-bundle-wheel-invalid" }
    [void]$normalizedWheels.Add([ordered]@{ name = $wheelName; version = $wheelVersion; url = $wheelUrl; sha256 = $wheelHash })
  }
  $normalizedModes = New-Object 'System.Collections.Generic.List[object]'
  foreach ($mode in $modes) {
    Assert-ExactObjectProperties $mode @("mode", "pipeAddress", "entryPointDestinationRelativePath", "importRootDestinationRelativePath", "arguments", "environment", "files") "broker-bundle-mode"
    $modeName = [string]$mode.mode
    if (-not $BrokerTaskNames.Contains($modeName) -or $seenModes.ContainsKey($modeName)) {
      throw "frozen-broker-bundle-mode-invalid:$modeName"
    }
    $seenModes[$modeName] = $true
    $modePipeAddress = [string]$mode.pipeAddress
    if ($modePipeAddress -notmatch '^\\\\\.\\pipe\\[A-Za-z0-9._-]{8,120}$' -or $modePipeAddress -eq $PipeAddress) {
      throw "frozen-broker-bundle-pipe-invalid:$modeName"
    }
    $laneDirectory = if ($modeName -eq "UPBIT_AUTHORITY") { "upbit" } else { "binance" }
    $destinationPrefix = "app\broker_authorities\$laneDirectory\"
    $modeCredentialBlob = Join-Path $AuthorityRoot "secrets\$laneDirectory-credential.dpapi"
    $modeAuthorityPipeAuthKey = Join-Path $AuthorityRoot "secrets\$laneDirectory-pipe-auth.key"
    $modeTraderPipeAuthKey = Join-Path $SharedRoot "$laneDirectory-pipe-auth.key"
    $modePrivateKey = Join-Path $AuthorityRoot "secrets\$laneDirectory-signing-ed25519-private.pem"
    $modePublicKey = Join-Path $SharedRoot "$laneDirectory-signing-ed25519-public.pem"
    $normalizedFiles = New-Object 'System.Collections.Generic.List[object]'
    foreach ($file in @($mode.files)) {
      Assert-ExactObjectProperties $file @("sourceRelativePath", "destinationRelativePath", "sha256") "broker-bundle-file"
      $sourceRelative = ([string]$file.sourceRelativePath).Replace('/', '\')
      $destinationRelative = ([string]$file.destinationRelativePath).Replace('/', '\')
      $expectedHash = ([string]$file.sha256).ToLowerInvariant()
      if (
        -not $sourceRelative -or [IO.Path]::IsPathRooted($sourceRelative) -or
        -not $destinationRelative.StartsWith($destinationPrefix, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::IsPathRooted($destinationRelative) -or $destinationRelative.Split('\') -contains ".." -or
        $expectedHash -notmatch '^[0-9a-f]{64}$'
      ) { throw "frozen-broker-bundle-file-route-invalid:$modeName" }
      $sourceFull = [IO.Path]::GetFullPath((Join-Path $SourceRoot $sourceRelative))
      $projectPrefix = $ProjectRoot.TrimEnd('\') + '\'
      if (-not $sourceFull.StartsWith($projectPrefix, [StringComparison]::OrdinalIgnoreCase) -or -not (Test-Path -LiteralPath $sourceFull -PathType Leaf)) {
        throw "frozen-broker-bundle-source-missing:$sourceRelative"
      }
      Assert-ExactHash $sourceFull $expectedHash "broker-bundle-source"
      $destinationKey = $destinationRelative.ToLowerInvariant()
      if ($seenDestinations.ContainsKey($destinationKey)) { throw "frozen-broker-bundle-destination-duplicate" }
      $seenDestinations[$destinationKey] = $true
      [void]$normalizedFiles.Add([ordered]@{
        sourcePath = $sourceFull
        destinationRelativePath = $destinationRelative
        sha256 = $expectedHash
      })
    }
    if ($normalizedFiles.Count -eq 0) { throw "frozen-broker-bundle-mode-files-empty:$modeName" }
    $entryRelative = ([string]$mode.entryPointDestinationRelativePath).Replace('/', '\')
    if (-not $seenDestinations.ContainsKey($entryRelative.ToLowerInvariant()) -or -not $entryRelative.EndsWith(".py", [StringComparison]::OrdinalIgnoreCase)) {
      throw "frozen-broker-bundle-entrypoint-invalid:$modeName"
    }
    $importRootRelative = ([string]$mode.importRootDestinationRelativePath).Replace('/', '\').TrimEnd('\')
    if (-not ($importRootRelative + '\').StartsWith($destinationPrefix, [StringComparison]::OrdinalIgnoreCase) -or $importRootRelative.Split('\') -contains "..") {
      throw "frozen-broker-bundle-import-root-invalid:$modeName"
    }
    $resolvedArguments = New-Object 'System.Collections.Generic.List[string]'
    foreach ($argumentItem in @($mode.arguments)) {
      if ($argumentItem -isnot [string] -or $argumentItem.Length -gt 1024) { throw "frozen-broker-bundle-argument-invalid:$modeName" }
      $resolved = $argumentItem.Replace("{AUTHORITY_ROOT}", $AuthorityRoot).Replace("{SHARED_ROOT}", $SharedRoot).Replace("{TRADER_DATA_ROOT}", (Resolve-Path -LiteralPath $TraderDataRoot).Path).Replace("{TRADER_SID}", $TraderOsSid).Replace("{PIPE_ADDRESS}", $modePipeAddress).Replace("{PIPE_AUTH_KEY}", $modeAuthorityPipeAuthKey).Replace("{TRADER_PIPE_AUTH_KEY}", $modeTraderPipeAuthKey).Replace("{BROKER_CREDENTIAL_BLOB}", $modeCredentialBlob).Replace("{BROKER_PRIVATE_KEY}", $modePrivateKey).Replace("{BROKER_PUBLIC_KEY}", $modePublicKey)
      if ($resolved.Contains('{') -or $resolved.Contains('}')) { throw "frozen-broker-bundle-argument-placeholder-invalid:$modeName" }
      [void]$resolvedArguments.Add($resolved)
    }
    $normalizedEnvironment = New-Object 'System.Collections.Generic.List[object]'
    foreach ($environmentItem in @($mode.environment)) {
      Assert-ExactObjectProperties $environmentItem @("name", "value") "broker-bundle-environment"
      $environmentName = [string]$environmentItem.name
      $environmentValue = [string]$environmentItem.value
      if (
        $environmentName -notmatch '^[A-Z][A-Z0-9_]{2,100}$' -or
        $environmentName -match '(KEY|SECRET|TOKEN|PASSWORD|CREDENTIAL)' -or
        $environmentValue.Length -gt 256
      ) { throw "frozen-broker-bundle-environment-invalid:$modeName" }
      [void]$normalizedEnvironment.Add([ordered]@{ name = $environmentName; value = $environmentValue })
    }
    [void]$normalizedModes.Add([ordered]@{
      mode = $modeName
      taskName = [string]$BrokerTaskNames[$modeName]
      pipeAddress = $modePipeAddress
      entryPointRelativePath = $entryRelative
      importRootRelativePath = $importRootRelative
      arguments = @($resolvedArguments)
      environment = @($normalizedEnvironment)
      credentialBlobPath = $modeCredentialBlob
      pipeAuthKeyPath = $modeAuthorityPipeAuthKey
      traderPipeAuthKeyPath = $modeTraderPipeAuthKey
      privateKeyPath = $modePrivateKey
      publicKeyPath = $modePublicKey
      files = @($normalizedFiles)
    })
  }
  foreach ($requiredMode in @("UPBIT_AUTHORITY", "BINANCE_OBSERVER")) {
    if (-not $seenModes.ContainsKey($requiredMode)) { throw "frozen-broker-bundle-mode-missing:$requiredMode" }
  }
  return [ordered]@{
    descriptorPath = $descriptorFull
    descriptorSha256 = $ExpectedBrokerBundleDescriptorSha256.ToLowerInvariant()
    pythonWheels = @($normalizedWheels)
    modes = @($normalizedModes)
  }
}

function Assert-ElevatedAdministrator {
  $principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
  if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "elevated-administrator-required"
  }
}

function Assert-SafeExactRoots {
  $authorityFull = [IO.Path]::GetFullPath($AuthorityRoot).TrimEnd('\')
  $sharedFull = [IO.Path]::GetFullPath($SharedRoot).TrimEnd('\')
  if ($authorityFull -ne "D:\crypto-first-live-authority" -or $sharedFull -ne "D:\crypto-first-live-shared") {
    throw "supervised-authority-exact-roots-required"
  }
  if ($authorityFull -eq $sharedFull) {
    throw "supervised-authority-roots-overlap"
  }
}

function Set-ProtectedDirectoryAcl([string]$Path, [bool]$TraderRead) {
  $directory = New-Item -ItemType Directory -Path $Path -Force
  $acl = New-Object Security.AccessControl.DirectorySecurity
  $acl.SetAccessRuleProtection($true, $false)
  $inherit = [Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit
  $propagation = [Security.AccessControl.PropagationFlags]::None
  $allow = [Security.AccessControl.AccessControlType]::Allow
  foreach ($sid in @($SystemSid, $AdministratorsSid)) {
    $rule = New-Object Security.AccessControl.FileSystemAccessRule(
      (New-Object Security.Principal.SecurityIdentifier($sid)),
      [Security.AccessControl.FileSystemRights]::FullControl,
      $inherit,
      $propagation,
      $allow
    )
    [void]$acl.AddAccessRule($rule)
  }
  if ($TraderRead) {
    $rule = New-Object Security.AccessControl.FileSystemAccessRule(
      (New-Object Security.Principal.SecurityIdentifier($TraderOsSid)),
      [Security.AccessControl.FileSystemRights]::ReadAndExecute,
      $inherit,
      $propagation,
      $allow
    )
    [void]$acl.AddAccessRule($rule)
  }
  $acl.SetOwner((New-Object Security.Principal.SecurityIdentifier($AdministratorsSid)))
  Set-Acl -LiteralPath $directory.FullName -AclObject $acl
}

function Invoke-Native([string]$FilePath, [string[]]$Arguments) {
  & $FilePath @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "native-command-failed:$([IO.Path]::GetFileName($FilePath)):$LASTEXITCODE"
  }
}

function Convert-SecureTokenToPlain([Security.SecureString]$Value) {
  if ($null -eq $Value) {
    throw "independent-github-bootstrap-token-required"
  }
  $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Value)
  try {
    return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
  }
}

function Invoke-GitHubApi(
  [string]$Method,
  [string]$Path,
  [string]$Token,
  [object]$Body = $null
) {
  $headers = @{
    "Accept" = "application/vnd.github+json"
    "Authorization" = "Bearer $Token"
    "User-Agent" = "crypto-first-live-supervised-provisioner"
    "X-GitHub-Api-Version" = "2022-11-28"
  }
  $parameters = @{
    Method = $Method
    Uri = "https://api.github.com$Path"
    Headers = $headers
  }
  if ($null -ne $Body) {
    $parameters["Body"] = ($Body | ConvertTo-Json -Depth 20 -Compress)
    $parameters["ContentType"] = "application/json"
  }
  return Invoke-RestMethod @parameters
}

function New-BranchRulesetBody(
  [string]$Name,
  [string[]]$Include,
  [string[]]$Exclude,
  [object[]]$Rules,
  [object[]]$BypassActors
) {
  return @{
    name = $Name
    target = "branch"
    enforcement = "active"
    bypass_actors = $BypassActors
    conditions = @{ ref_name = @{ include = $Include; exclude = $Exclude } }
    rules = $Rules
  }
}

function Register-SystemTask([string]$TaskName, [string]$LauncherPath, [string]$Mode) {
  $action = New-ScheduledTaskAction `
    -Execute "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe" `
    -Argument ("-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$LauncherPath`" -Mode $Mode")
  $settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew
  $principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Settings $settings `
    -Principal $principal `
    -Description "Crypto first-live supervised Git authority; on-demand, non-promotion only" `
    -Force | Out-Null
}

function Invoke-TransientSystemMode([string]$LauncherPath, [string]$Mode) {
  if (Get-ScheduledTask -TaskName $TransientTaskName -ErrorAction SilentlyContinue) {
    throw "transient-supervised-authority-task-already-exists"
  }
  Register-SystemTask $TransientTaskName $LauncherPath $Mode
  try {
    Start-ScheduledTask -TaskName $TransientTaskName
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
      Start-Sleep -Milliseconds 250
      $task = Get-ScheduledTask -TaskName $TransientTaskName
      if ($task.State -ne "Running") { break }
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($task.State -eq "Running") {
      Stop-ScheduledTask -TaskName $TransientTaskName -ErrorAction SilentlyContinue
      throw "transient-supervised-authority-task-timeout"
    }
    $info = Get-ScheduledTaskInfo -TaskName $TransientTaskName
    if ([int64]$info.LastTaskResult -ne 0) {
      throw "transient-supervised-authority-task-failed:${Mode}:$($info.LastTaskResult)"
    }
  } finally {
    Unregister-ScheduledTask -TaskName $TransientTaskName -Confirm:$false -ErrorAction SilentlyContinue
  }
}

function Start-PrearmedSystemTask([string]$TaskName, [string]$ExpectedPipeAddress) {
  if (-not ("SupervisedNamedPipeProbe" -as [type])) {
    Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class SupervisedNamedPipeProbe {
  [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
  public static extern bool WaitNamedPipe(string name, uint timeoutMilliseconds);
}
"@
  }
  Start-ScheduledTask -TaskName $TaskName
  $deadline = [DateTime]::UtcNow.AddSeconds(15)
  do {
    if ([SupervisedNamedPipeProbe]::WaitNamedPipe($ExpectedPipeAddress, 100)) { return }
    $task = Get-ScheduledTask -TaskName $TaskName
    if ($task.State -ne "Running" -and [DateTime]::UtcNow -gt $deadline.AddSeconds(-12)) {
      $info = Get-ScheduledTaskInfo -TaskName $TaskName
      throw "prearmed-system-task-exited:${TaskName}:$($info.LastTaskResult)"
    }
    Start-Sleep -Milliseconds 100
  } while ([DateTime]::UtcNow -lt $deadline)
  Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
  throw "prearmed-system-task-pipe-timeout:$TaskName"
}

Assert-SafeExactRoots
if ($GitHubRepository -notmatch '^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$') {
  throw "github-repository-invalid"
}
if ($GitHubRepository.ToLowerInvariant() -eq "youwon35/live_trader") {
  throw "code-origin-cannot-be-supervised-authority-remote"
}
if ($TraderOsSid -notmatch '^S-1-(?:\d+-){1,14}\d+$' -or $TraderOsSid -eq $SystemSid) {
  throw "trader-os-sid-invalid"
}
$RemoteSshUrl = "git@github.com:$GitHubRepository.git"
$currentToolHash = Get-Sha256Lower $AuthorityToolSource
$currentAnchorHash = Get-Sha256Lower $AnchorModuleSource
$currentCredentialRewrapToolHash = Get-Sha256Lower $CredentialRewrapToolSource
$currentBrokerDescriptorHash = if ($BrokerBundleDescriptorPath -and (Test-Path -LiteralPath $BrokerBundleDescriptorPath -PathType Leaf)) { Get-Sha256Lower $BrokerBundleDescriptorPath } else { "" }

$plan = [ordered]@{
  apply = [bool]$Apply
  applyReleased = $ProtectedBundleProvisioningApplyReleased
  mutationPerformed = $false
  formalWorm = $false
  promotionEligible = $false
  brokerApiAllowed = $false
  orderAllowed = $false
  brokerNetworkReleaseAllowed = $BrokerNetworkReleaseAllowed
  authorityOsSid = $SystemSid
  traderOsSid = $TraderOsSid
  authorityRoot = $AuthorityRoot
  sharedRoot = $SharedRoot
  githubRepository = $GitHubRepository
  remoteSshUrl = $RemoteSshUrl
  remoteRef = $RemoteRef
  taskName = $ServeTaskName
  taskAutoStart = $false
  taskStartedDuringProvisioning = $false
  currentAuthorityToolSha256 = $currentToolHash
  currentAnchorModuleSha256 = $currentAnchorHash
  currentCredentialRewrapToolSha256 = $currentCredentialRewrapToolHash
  brokerBundleDescriptorProvided = [bool]$BrokerBundleDescriptorPath
  currentBrokerBundleDescriptorSha256 = $currentBrokerDescriptorHash
  requiredBrokerModes = @("UPBIT_AUTHORITY", "BINANCE_OBSERVER")
  brokerTaskNames = @($BrokerTaskNames.Values)
  requiredResidualRiskPhrase = $RequiredApproval
  requiresPrivateEmptyDedicatedRepository = $true
  requiresGitHubActionsDisabled = $true
  requiresIndependentBootstrapAdministrator = (-not $AllowTraderGitHubAdministrator)
}

if (-not $Apply) {
  $plan | ConvertTo-Json -Depth 8
  return
}

if (-not $ProtectedBundleProvisioningApplyReleased -or $BrokerNetworkReleaseAllowed) {
  throw "protected-bundle-provisioning-release-held"
}

if ($ResidualRiskApprovalPhrase -cne $RequiredApproval) {
  throw "exact-supervised-residual-risk-approval-required"
}
if ($ExpectedAuthorityToolSha256 -notmatch '^[0-9a-fA-F]{64}$' -or $ExpectedAnchorModuleSha256 -notmatch '^[0-9a-fA-F]{64}$' -or $ExpectedCredentialRewrapToolSha256 -notmatch '^[0-9a-fA-F]{64}$') {
  throw "exact-source-sha256-pins-required"
}
if ($currentToolHash -ne $ExpectedAuthorityToolSha256.ToLowerInvariant() -or $currentAnchorHash -ne $ExpectedAnchorModuleSha256.ToLowerInvariant() -or $currentCredentialRewrapToolHash -ne $ExpectedCredentialRewrapToolSha256.ToLowerInvariant()) {
  throw "supervised-authority-source-pin-changed"
}
$brokerBundle = Get-FrozenBrokerBundle
Assert-ElevatedAdministrator
if (([Security.Principal.WindowsIdentity]::GetCurrent().User.Value) -ne $TraderOsSid) {
  throw "provisioner-trader-sid-changed"
}
foreach ($path in @($PythonExecutable, $PythonDll, $GitExecutable, $SshExecutable, $SshKeygenExecutable, $TraderDataRoot)) {
  if (-not (Test-Path -LiteralPath $path)) { throw "required-path-missing:$path" }
}
Assert-ExactHash $PythonExecutable $PythonExecutableSha256 "python-executable"
Assert-ExactHash $GitExecutable $GitExecutableSha256 "git-executable"
Assert-ExactHash $SshExecutable $SshExecutableSha256 "ssh-executable"
Assert-ExactHash $SshKeygenExecutable $SshKeygenExecutableSha256 "ssh-keygen-executable"
Assert-ExactHash $PythonDll $PythonDllSha256 "python-dll"
if ((Test-Path -LiteralPath $AuthorityRoot) -or (Test-Path -LiteralPath $SharedRoot)) {
  throw "supervised-authority-root-already-exists"
}
if (-not $PSCmdlet.ShouldProcess(
  "$AuthorityRoot and https://github.com/$GitHubRepository",
  "Create SYSTEM authority bundle, deploy key, rulesets, anchor ref, and on-demand task"
)) { return }

$token = Convert-SecureTokenToPlain $GitHubBootstrapToken
$deployKeyId = $null
$rulesetIds = New-Object 'System.Collections.Generic.List[object]'
$actionsWereEnabled = $null
$actionsChanged = $false
$remoteRefProvisioned = $false
try {
  $me = Invoke-GitHubApi "GET" "/user" $token
  if ($me.login -eq "youwon35") {
    if (-not $AllowTraderGitHubAdministrator -or $TraderGitHubAdministratorApprovalPhrase -cne $RequiredSameAdminApproval) {
      throw "independent-github-administrator-required"
    }
  }
  $repo = Invoke-GitHubApi "GET" "/repos/$GitHubRepository" $token
  if ($repo.archived -or -not $repo.private) {
    throw "private-unarchived-dedicated-github-repository-required"
  }
  $branches = @(Invoke-GitHubApi "GET" "/repos/$GitHubRepository/branches?per_page=1" $token | Where-Object { $null -ne $_ })
  $keys = @(Invoke-GitHubApi "GET" "/repos/$GitHubRepository/keys" $token | Where-Object { $null -ne $_ })
  $rulesets = @(Invoke-GitHubApi "GET" "/repos/$GitHubRepository/rulesets" $token | Where-Object { $null -ne $_ })
  if ($branches.Count -ne 0 -or $keys.Count -ne 0 -or $rulesets.Count -ne 0) {
    throw "empty-unconfigured-dedicated-github-repository-required"
  }
  $actionsBefore = Invoke-GitHubApi "GET" "/repos/$GitHubRepository/actions/permissions" $token
  $actionsWereEnabled = [bool]$actionsBefore.enabled
  if ($actionsWereEnabled) {
    [void](Invoke-GitHubApi "PUT" "/repos/$GitHubRepository/actions/permissions" $token @{ enabled = $false })
    $actionsChanged = $true
  }
  $actionsAfter = Invoke-GitHubApi "GET" "/repos/$GitHubRepository/actions/permissions" $token
  if ([bool]$actionsAfter.enabled) {
    throw "github-actions-disable-not-verified"
  }

  Set-ProtectedDirectoryAcl $AuthorityRoot $false
  Set-ProtectedDirectoryAcl $SharedRoot $true
  foreach ($relative in @("app\tools", "app\live_trader", "secrets", "repo", "logs", "wheelhouse", "disabled-hooks", "system-home")) {
    [void](New-Item -ItemType Directory -Path (Join-Path $AuthorityRoot $relative) -Force)
  }
  Copy-Item -LiteralPath $AuthorityToolSource -Destination (Join-Path $AuthorityRoot "app\tools\crypto_first_live_supervised_git_authority.py")
  Copy-Item -LiteralPath $CredentialRewrapToolSource -Destination (Join-Path $AuthorityRoot "app\tools\crypto_first_live_broker_credential_rewrap.py")
  Copy-Item -LiteralPath $AnchorModuleSource -Destination (Join-Path $AuthorityRoot "app\live_trader\crypto_first_live_supervised_anchor.py")
  Write-Utf8NoBom (Join-Path $AuthorityRoot "app\live_trader\__init__.py") "__all__ = []`n"
  foreach ($mode in $brokerBundle.modes) {
    foreach ($file in $mode.files) {
      $destination = Join-Path $AuthorityRoot $file.destinationRelativePath
      [void](New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force)
      Copy-Item -LiteralPath $file.sourcePath -Destination $destination
      Assert-ExactHash $destination $file.sha256 "protected-broker-bundle-copy"
    }
  }
  $protectedBrokerDescriptor = Join-Path $AuthorityRoot "app\broker_authorities\frozen-bundle-descriptor.json"
  Copy-Item -LiteralPath $brokerBundle.descriptorPath -Destination $protectedBrokerDescriptor
  Assert-ExactHash $protectedBrokerDescriptor $brokerBundle.descriptorSha256 "protected-broker-bundle-descriptor"

  $venv = Join-Path $AuthorityRoot "venv"
  Invoke-Native $PythonExecutable @("-m", "venv", "--without-pip", $venv)
  $wheel = Join-Path $AuthorityRoot "wheelhouse\pycryptodome-3.23.0-cp37-abi3-win_amd64.whl"
  Invoke-WebRequest -Uri $PycryptodomeWheelUrl -OutFile $wheel -UseBasicParsing
  Assert-ExactHash $wheel $PycryptodomeWheelSha256 "pycryptodome-wheel"
  Add-Type -AssemblyName System.IO.Compression.FileSystem
  [IO.Compression.ZipFile]::ExtractToDirectory($wheel, (Join-Path $venv "Lib\site-packages"))
  $brokerWheelPaths = New-Object 'System.Collections.Generic.List[string]'
  foreach ($brokerWheel in $brokerBundle.pythonWheels) {
    $brokerWheelPath = Join-Path $AuthorityRoot ("wheelhouse\broker-" + $brokerWheel.name + "-" + $brokerWheel.version + ".whl")
    Invoke-WebRequest -Uri $brokerWheel.url -OutFile $brokerWheelPath -UseBasicParsing
    Assert-ExactHash $brokerWheelPath $brokerWheel.sha256 "broker-python-wheel"
    [IO.Compression.ZipFile]::ExtractToDirectory($brokerWheelPath, (Join-Path $venv "Lib\site-packages"))
    [void]$brokerWheelPaths.Add($brokerWheelPath)
  }
  $venvPython = Join-Path $venv "Scripts\python.exe"
  Invoke-Native $venvPython @("-I", "-c", "import Crypto; assert Crypto.__version__ == '3.23.0'")

  $auditPrivate = Join-Path $AuthorityRoot "secrets\audit-signing-ed25519-private.pem"
  $auditPublic = Join-Path $SharedRoot "audit-signing-ed25519-public.pem"
  $keyScript = "from pathlib import Path; from Crypto.PublicKey import ECC; import sys; k=ECC.generate(curve='Ed25519'); Path(sys.argv[1]).write_text(k.export_key(format='PEM')+'\n',encoding='ascii'); Path(sys.argv[2]).write_text(k.public_key().export_key(format='PEM')+'\n',encoding='ascii')"
  Invoke-Native $venvPython @("-I", "-c", $keyScript, $auditPrivate, $auditPublic)
  foreach ($mode in $brokerBundle.modes) {
    Invoke-Native $venvPython @("-I", "-c", $keyScript, $mode.privateKeyPath, $mode.publicKeyPath)
  }

  $sshPrivate = Join-Path $AuthorityRoot "secrets\github-deploy-ed25519"
  Invoke-Native $SshKeygenExecutable @("-q", "-t", "ed25519", "-N", "", "-C", "crypto-first-live-supervised-authority", "-f", $sshPrivate)
  $pipeKey = Join-Path $SharedRoot "pipe-auth.key"
  $bytes = New-Object byte[] 32
  $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
  try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
  [IO.File]::WriteAllBytes($pipeKey, $bytes)
  foreach ($mode in $brokerBundle.modes) {
    $modeBytes = New-Object byte[] 32
    $modeRng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $modeRng.GetBytes($modeBytes) } finally { $modeRng.Dispose() }
    [IO.File]::WriteAllBytes($mode.pipeAuthKeyPath, $modeBytes)
    [IO.File]::WriteAllBytes($mode.traderPipeAuthKeyPath, $modeBytes)
    [Array]::Clear($modeBytes, 0, $modeBytes.Length)
  }

  $hostKeyBytes = [Convert]::FromBase64String($GitHubEd25519HostKey)
  $sha = [Security.Cryptography.SHA256]::Create()
  try { $hostKeyDigest = ([BitConverter]::ToString($sha.ComputeHash($hostKeyBytes))).Replace("-", "").ToLowerInvariant() } finally { $sha.Dispose() }
  if ($hostKeyDigest -ne $GitHubEd25519HostKeyRawSha256) { throw "github-host-key-pin-invalid" }
  $knownHosts = Join-Path $AuthorityRoot "secrets\github-known-hosts"
  Write-Utf8NoBom $knownHosts ("github.com ssh-ed25519 $GitHubEd25519HostKey`n")

  $repoPath = Join-Path $AuthorityRoot "repo"
  $env:GIT_CONFIG_NOSYSTEM = "1"
  $env:GIT_CONFIG_GLOBAL = "NUL"
  $env:GIT_TERMINAL_PROMPT = "0"
  Invoke-Native $GitExecutable @("-C", $repoPath, "init")
  Invoke-Native $GitExecutable @("-C", $repoPath, "remote", "add", "origin", $RemoteSshUrl)
  $sshCommand = "`"$SshExecutable`" -i `"$sshPrivate`" -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=`"$knownHosts`""
  Invoke-Native $GitExecutable @("-C", $repoPath, "config", "--local", "core.sshCommand", $sshCommand)
  Invoke-Native $GitExecutable @("-C", $repoPath, "config", "--local", "credential.helper", "")
  Invoke-Native $GitExecutable @("-C", $repoPath, "config", "--local", "core.hooksPath", (Join-Path $AuthorityRoot "disabled-hooks"))

  $suffix = [Guid]::NewGuid().ToString("N")
  $remoteBytes = [Text.Encoding]::UTF8.GetBytes($RemoteSshUrl)
  $remoteHasher = [Security.Cryptography.SHA256]::Create()
  try { $remoteHash = ([BitConverter]::ToString($remoteHasher.ComputeHash($remoteBytes))).Replace("-", "").ToLowerInvariant() } finally { $remoteHasher.Dispose() }
  $configPath = Join-Path $AuthorityRoot "authority.json"
  $config = [ordered]@{
    schemaVersion = "crypto-first-live-supervised-git-authority-config/v1"
    authorityId = "supervised-authority-$suffix"
    namespaceId = "supervised-namespace-$suffix"
    keyId = "supervised-key-$suffix"
    authorityOsSid = $SystemSid
    traderOsSid = $TraderOsSid
    authorityRepoPath = $repoPath
    traderDataRoot = (Resolve-Path -LiteralPath $TraderDataRoot).Path
    privateKeyPath = $auditPrivate
    pipeAuthKeyPath = $pipeKey
    pipeAddress = $PipeAddress
    remoteName = "origin"
    remoteRef = $RemoteRef
    remoteUrlSha256 = $remoteHash
    statePath = "audit/crypto-first-live-supervised-state.json"
  }
  Write-Utf8NoBom $configPath (($config | ConvertTo-Json -Depth 6) + "`n")
  $upbitCredentialGenerationId = "credential-generation-" + [Guid]::NewGuid().ToString("N")
  $binanceCredentialGenerationId = "credential-generation-" + [Guid]::NewGuid().ToString("N")
  $protectedCredentialRewrapTool = Join-Path $AuthorityRoot "app\tools\crypto_first_live_broker_credential_rewrap.py"
  Assert-ExactHash $protectedCredentialRewrapTool $currentCredentialRewrapToolHash "protected-credential-rewrap-tool"
  $credentialInspectionRaw = & $venvPython -I $protectedCredentialRewrapTool inspect `
    --authority-id $config.authorityId `
    --upbit-generation-id $upbitCredentialGenerationId `
    --binance-generation-id $binanceCredentialGenerationId
  if ($LASTEXITCODE -ne 0) { throw "current-user-broker-credential-inspection-failed" }
  $credentialInspection = $credentialInspectionRaw | ConvertFrom-Json
  Assert-ExactObjectProperties $credentialInspection @("schemaVersion", "brokerNetworkRequestCount", "orderMutationCount", "credentials") "broker-credential-inspection"
  if (
    $credentialInspection.schemaVersion -cne "crypto-first-live-broker-credential-inspection/v1" -or
    $credentialInspection.brokerNetworkRequestCount -ne 0 -or $credentialInspection.orderMutationCount -ne 0 -or
    @($credentialInspection.credentials).Count -ne 2
  ) { throw "broker-credential-inspection-invalid" }
  foreach ($credentialRow in @($credentialInspection.credentials)) {
    Assert-ExactObjectProperties $credentialRow @("lane", "path", "credentialFingerprint", "accountFingerprint", "envelopeHash", "credentialGenerationId") "broker-credential-inspection-row"
    if (
      $credentialRow.lane -notin @("UPBIT", "BINANCE_SPOT") -or
      [string]$credentialRow.credentialFingerprint -notmatch '^[0-9a-f]{64}$' -or
      $credentialRow.credentialFingerprint -cne $credentialRow.accountFingerprint -or
      [string]$credentialRow.envelopeHash -notmatch '^[0-9a-f]{64}$'
    ) { throw "broker-credential-inspection-row-invalid" }
  }
  $traderEnv = @(
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PIPE=$PipeAddress",
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_AUTHORITY_ID=$($config.authorityId)",
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_NAMESPACE_ID=$($config.namespaceId)",
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_KEY_ID=$($config.keyId)",
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PUBLIC_KEY=$auditPublic",
    "LIVE_TRADER_CRYPTO_FIRST_LIVE_SUPERVISED_PIPE_AUTHKEY=$pipeKey",
    "LIVE_TRADER_UPBIT_AUTHORITY_PIPE=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'UPBIT_AUTHORITY' }))[0].pipeAddress)",
    "LIVE_TRADER_UPBIT_AUTHORITY_PUBLIC_KEY=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'UPBIT_AUTHORITY' }))[0].publicKeyPath)",
    "LIVE_TRADER_UPBIT_AUTHORITY_PIPE_AUTHKEY=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'UPBIT_AUTHORITY' }))[0].traderPipeAuthKeyPath)",
    "LIVE_TRADER_BINANCE_OBSERVER_LAUNCH_PIPE=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'BINANCE_OBSERVER' }))[0].pipeAddress)",
    "LIVE_TRADER_BINANCE_OBSERVER_PUBLIC_KEY=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'BINANCE_OBSERVER' }))[0].publicKeyPath)",
    "LIVE_TRADER_BINANCE_OBSERVER_LAUNCH_PIPE_AUTHKEY=$((@($brokerBundle.modes | Where-Object { $_.mode -eq 'BINANCE_OBSERVER' }))[0].traderPipeAuthKeyPath)"
  ) -join "`r`n"
  Write-Utf8NoBom (Join-Path $SharedRoot "trader-supervised-authority.env") ($traderEnv + "`r`n")

  $deployPublicLine = (Get-Content -LiteralPath ($sshPrivate + ".pub") -Raw).Trim()
  $deployKey = Invoke-GitHubApi "POST" "/repos/$GitHubRepository/keys" $token @{
    title = "crypto-first-live-supervised-authority"
    key = $deployPublicLine
    read_only = $false
  }
  $deployKeyId = [int64]$deployKey.id

  $restrict = @(@{ type = "creation" }, @{ type = "update" })
  $integrity = @(@{ type = "deletion" }, @{ type = "non_fast_forward" })
  $blockAll = @(@{ type = "creation" }, @{ type = "update" }, @{ type = "deletion" })
  $deployBypass = @(@{ actor_id = $null; actor_type = "DeployKey"; bypass_mode = "always" })
  $ruleBodies = @(
    (New-BranchRulesetBody "crypto-first-live-anchor-writer" @($RemoteRef) @() $restrict $deployBypass),
    (New-BranchRulesetBody "crypto-first-live-anchor-integrity" @($RemoteRef) @() $integrity @()),
    (New-BranchRulesetBody "crypto-first-live-block-other-branches" @("~ALL") @($RemoteRef) $blockAll @()),
    @{
      name = "crypto-first-live-block-all-tags"
      target = "tag"
      enforcement = "active"
      bypass_actors = @()
      conditions = @{ ref_name = @{ include = @("~ALL"); exclude = @() } }
      rules = $blockAll
    }
  )
  foreach ($body in $ruleBodies) {
    $created = Invoke-GitHubApi "POST" "/repos/$GitHubRepository/rulesets" $token $body
    [void]$rulesetIds.Add([ordered]@{ id = [int64]$created.id; name = [string]$created.name })
  }

  $launcherPath = Join-Path $AuthorityRoot "launch-authority.ps1"
  $launcher = @'
param([ValidateSet("Check", "Provision", "Serve", "UPBIT_AUTHORITY", "BINANCE_OBSERVER")][string]$Mode)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$manifestPath = Join-Path $root "bundle-manifest.json"
$manifestItem = Get-Item -LiteralPath $manifestPath -Force
if ($manifestItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "manifest-reparse-point" }
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
function Assert-ExactJsonFields([object]$Value, [string[]]$ExpectedNames, [string]$Label) {
  if ($null -eq $Value) { throw "$Label-missing" }
  $actualNames = @($Value.PSObject.Properties.Name | Sort-Object)
  $sortedExpected = @($ExpectedNames | Sort-Object)
  if (($actualNames -join "`n") -cne ($sortedExpected -join "`n")) { throw "$Label-fields-not-exact" }
}
Assert-ExactJsonFields $manifest @(
  "schemaVersion", "authorityOsSid", "traderOsSid", "authorityRoot", "sharedRoot",
  "sourcePins", "sealedRoots", "files", "pinnedFiles", "externalBinaries",
  "pycryptodomeWheelSha256", "githubHostKeyRawSha256", "remoteRef",
  "brokerBundleDescriptorSha256", "brokerModes", "brokerCredentialAuthorityId",
  "machineProtectedCredentials", "formalWorm", "promotionEligible"
) "manifest"
if (
  $manifest.schemaVersion -cne "crypto-first-live-supervised-authority-bundle-manifest/v1" -or
  $manifest.authorityOsSid -cne "S-1-5-18" -or $manifest.formalWorm -ne $false -or
  $manifest.promotionEligible -ne $false -or
  [IO.Path]::GetFullPath([string]$manifest.authorityRoot).TrimEnd('\') -cne $root.TrimEnd('\')
) { throw "manifest-binding-invalid" }
Assert-ExactJsonFields $manifest.sourcePins @("authorityToolSha256", "anchorModuleSha256", "credentialRewrapToolSha256", "brokerBundleDescriptorSha256") "manifest-source-pins"
if ([string]$manifest.brokerCredentialAuthorityId -notmatch '^[A-Za-z0-9._:-]{8,160}$') { throw "manifest-broker-credential-authority-invalid" }
$expectedSealedRoots = @((Join-Path $root "app"), (Join-Path $root "venv"))
if (@($manifest.sealedRoots).Count -ne 2) { throw "manifest-sealed-roots-invalid" }
for ($index = 0; $index -lt 2; $index++) {
  if ([IO.Path]::GetFullPath([string]$manifest.sealedRoots[$index]).TrimEnd('\') -cne $expectedSealedRoots[$index].TrimEnd('\')) {
    throw "manifest-sealed-roots-invalid"
  }
}
foreach ($item in @($manifest.files) + @($manifest.pinnedFiles) + @($manifest.externalBinaries)) {
  Assert-ExactJsonFields $item @("path", "sha256") "manifest-file-record"
  if ([string]$item.sha256 -notmatch '^[0-9a-f]{64}$') { throw "manifest-file-hash-invalid" }
}
$brokerModes = @($manifest.brokerModes)
if ($brokerModes.Count -ne 2 -or ((@($brokerModes.mode | Sort-Object) -join "`n") -cne (@("BINANCE_OBSERVER", "UPBIT_AUTHORITY") -join "`n"))) {
  throw "manifest-broker-modes-invalid"
}
foreach ($brokerMode in $brokerModes) {
  Assert-ExactJsonFields $brokerMode @("mode", "taskName", "pipeAddress", "entryPoint", "importRoot", "arguments", "environment") "manifest-broker-mode"
  foreach ($environmentItem in @($brokerMode.environment)) {
    Assert-ExactJsonFields $environmentItem @("name", "value") "manifest-broker-environment"
  }
}
$credentialRows = @($manifest.machineProtectedCredentials)
if ($credentialRows.Count -ne 2) { throw "manifest-machine-credentials-invalid" }
foreach ($credentialRow in $credentialRows) {
  Assert-ExactJsonFields $credentialRow @("lane", "path", "credentialFingerprint", "accountFingerprint", "envelopeHash", "credentialGenerationId") "manifest-machine-credential"
  $credentialPath = [IO.Path]::GetFullPath([string]$credentialRow.path)
  $privatePrefix = [IO.Path]::GetFullPath([string]$manifest.authorityRoot).TrimEnd('\') + '\secrets\'
  if (
    $credentialPath -notin @((Join-Path $manifest.authorityRoot "secrets\upbit-credential.dpapi"), (Join-Path $manifest.authorityRoot "secrets\binance-credential.dpapi")) -or
    -not $credentialPath.StartsWith($privatePrefix, [StringComparison]::OrdinalIgnoreCase) -or
    -not (Test-Path -LiteralPath $credentialPath -PathType Leaf)
  ) { throw "manifest-machine-credential-path-invalid" }
  $credentialItem = Get-Item -LiteralPath $credentialPath -Force
  if ($credentialItem.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "manifest-machine-credential-reparse-point" }
  $credentialAcl = Get-Acl -LiteralPath $credentialPath
  foreach ($rule in $credentialAcl.Access) {
    $sid = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
    $rights = [int64]$rule.FileSystemRights
    $readable = (($rights -band 0x1) -ne 0 -or ($rights -band 0x20000) -ne 0)
    if ($rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and $readable -and @("S-1-5-18", "S-1-5-32-544") -notcontains $sid) {
      throw "manifest-machine-credential-acl-too-broad"
    }
  }
}
$expected = @{}
$allManifestPaths = @{}
function Assert-ManifestFile([object]$item, [bool]$Sealed) {
  $full = [IO.Path]::GetFullPath([string]$item.path)
  $authorityPrefix = [IO.Path]::GetFullPath([string]$manifest.authorityRoot).TrimEnd('\') + '\'
  $sharedPrefix = [IO.Path]::GetFullPath([string]$manifest.sharedRoot).TrimEnd('\') + '\'
  $isAuthority = $full.StartsWith($authorityPrefix, [StringComparison]::OrdinalIgnoreCase)
  $isShared = $full.StartsWith($sharedPrefix, [StringComparison]::OrdinalIgnoreCase)
  if (-not $isAuthority -and -not $isShared) { throw "manifest-file-outside-roots:$full" }
  $insideSealed = $false
  foreach ($sealedRoot in $manifest.sealedRoots) {
    $sealedPrefix = [IO.Path]::GetFullPath([string]$sealedRoot).TrimEnd('\') + '\'
    if ($full.StartsWith($sealedPrefix, [StringComparison]::OrdinalIgnoreCase)) { $insideSealed = $true }
  }
  if ($insideSealed -ne $Sealed) { throw "manifest-file-class-invalid:$full" }
  if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { throw "manifest-file-missing:$full" }
  $file = Get-Item -LiteralPath $full -Force
  if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw "manifest-file-reparse-point:$full" }
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
  if ($actual -ne $item.sha256) { throw "manifest-file-changed:$full" }
  $allowed = @("S-1-5-18", "S-1-5-32-544")
  if ($isShared) { $allowed += [string]$manifest.traderOsSid }
  $acl = Get-Acl -LiteralPath $full
  foreach ($rule in $acl.Access) {
    $sid = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
    $rights = [int64]$rule.FileSystemRights
    $readable = (($rights -band 0x1) -ne 0 -or ($rights -band 0x20000) -ne 0)
    if ($rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Allow -and $readable -and $allowed -notcontains $sid) {
      throw "manifest-file-acl-too-broad:${full}:${sid}"
    }
  }
  return $full
}
foreach ($item in $manifest.files) {
  $full = Assert-ManifestFile $item $true
  $key = $full.ToLowerInvariant()
  if ($allManifestPaths.ContainsKey($key)) { throw "manifest-file-duplicate:$full" }
  $allManifestPaths[$key] = $true
  $expected[$key] = $true
}
foreach ($item in $manifest.pinnedFiles) {
  $full = Assert-ManifestFile $item $false
  $key = $full.ToLowerInvariant()
  if ($allManifestPaths.ContainsKey($key)) { throw "manifest-file-duplicate:$full" }
  $allManifestPaths[$key] = $true
}
foreach ($sealedRoot in $manifest.sealedRoots) {
  foreach ($file in Get-ChildItem -LiteralPath $sealedRoot -Recurse -File) {
    $full = [IO.Path]::GetFullPath($file.FullName).ToLowerInvariant()
    if (-not $expected.ContainsKey($full)) { throw "manifest-extra-file:$full" }
  }
}
foreach ($item in $manifest.externalBinaries) {
  $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $item.path).Hash.ToLowerInvariant()
  if ($actual -ne $item.sha256) { throw "manifest-external-binary-changed:$($item.path)" }
}
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONNOUSERSITE = "1"
$env:GIT_CONFIG_NOSYSTEM = "1"
$env:GIT_CONFIG_GLOBAL = "NUL"
$env:GIT_TERMINAL_PROMPT = "0"
$env:GIT_ASKPASS = ""
$env:HOME = Join-Path $root "system-home"
$env:USERPROFILE = $env:HOME
$env:PATH = "C:\Program Files\Git\cmd;C:\Windows\System32;C:\Windows;C:\Windows\System32\WindowsPowerShell\v1.0"
$python = Join-Path $root "venv\Scripts\python.exe"
$tool = Join-Path $root "app\tools\crypto_first_live_supervised_git_authority.py"
$config = Join-Path $root "authority.json"
$logs = Join-Path $root "logs"
$stdout = Join-Path $logs ("last-" + $Mode.ToLowerInvariant() + ".stdout.log")
$stderr = Join-Path $logs ("last-" + $Mode.ToLowerInvariant() + ".stderr.log")
$modeArg = if ($Mode -eq "Check") { "--check-config-only" } elseif ($Mode -eq "Provision") { "--provision-ref" } elseif ($Mode -eq "Serve") { "--serve" } else { "" }
if ($modeArg) {
  & $python -I $tool --config $config $modeArg 1>>$stdout 2>>$stderr
} else {
  $selected = @($manifest.brokerModes | Where-Object { $_.mode -ceq $Mode })
  if ($selected.Count -ne 1) { throw "manifest-broker-mode-invalid:$Mode" }
  $entryPoint = [string]$selected[0].entryPoint
  $importRoot = [string]$selected[0].importRoot
  if (-not (Test-Path -LiteralPath $entryPoint -PathType Leaf) -or -not (Test-Path -LiteralPath $importRoot -PathType Container)) {
    throw "manifest-broker-entrypoint-missing:$Mode"
  }
  foreach ($environmentItem in @($selected[0].environment)) {
    [Environment]::SetEnvironmentVariable([string]$environmentItem.name, [string]$environmentItem.value, "Process")
  }
  $brokerArguments = @($selected[0].arguments | ForEach-Object { [string]$_ })
  $isolatedRunner = "import runpy,sys; root=sys.argv.pop(1); entry=sys.argv.pop(1); sys.path.insert(0,root); runpy.run_path(entry,run_name='__main__')"
  & $python -I -c $isolatedRunner $importRoot $entryPoint @brokerArguments 1>>$stdout 2>>$stderr
}
exit $LASTEXITCODE
'@
  Write-Utf8NoBom $launcherPath ($launcher + "`r`n")

  $manifestFiles = New-Object 'System.Collections.Generic.List[object]'
  foreach ($root in @((Join-Path $AuthorityRoot "app"), $venv)) {
    foreach ($file in Get-ChildItem -LiteralPath $root -Recurse -File) {
      [void]$manifestFiles.Add([ordered]@{ path = $file.FullName; sha256 = (Get-Sha256Lower $file.FullName) })
    }
  }
  $brokerGeneratedFiles = @($brokerBundle.modes | ForEach-Object { @($_.pipeAuthKeyPath, $_.traderPipeAuthKeyPath, $_.privateKeyPath, $_.publicKeyPath) })
  $pinnedFiles = New-Object 'System.Collections.Generic.List[object]'
  foreach ($file in @($configPath, $auditPrivate, $sshPrivate, ($sshPrivate + ".pub"), $knownHosts, $auditPublic, $pipeKey, (Join-Path $SharedRoot "trader-supervised-authority.env"), (Join-Path $repoPath ".git\config"), $launcherPath, $wheel) + @($brokerWheelPaths) + @($brokerGeneratedFiles)) {
    [void]$pinnedFiles.Add([ordered]@{ path = $file; sha256 = (Get-Sha256Lower $file) })
  }
  $manifest = [ordered]@{
    schemaVersion = "crypto-first-live-supervised-authority-bundle-manifest/v1"
    authorityOsSid = $SystemSid
    traderOsSid = $TraderOsSid
    authorityRoot = $AuthorityRoot
    sharedRoot = $SharedRoot
    sourcePins = [ordered]@{
      authorityToolSha256 = $currentToolHash
      anchorModuleSha256 = $currentAnchorHash
      brokerBundleDescriptorSha256 = $brokerBundle.descriptorSha256
      credentialRewrapToolSha256 = $currentCredentialRewrapToolHash
    }
    sealedRoots = @((Join-Path $AuthorityRoot "app"), $venv)
    files = @($manifestFiles | Sort-Object path)
    pinnedFiles = @($pinnedFiles | Sort-Object path)
    externalBinaries = @(
      [ordered]@{ path = $PythonExecutable; sha256 = $PythonExecutableSha256 },
      [ordered]@{ path = $PythonDll; sha256 = $PythonDllSha256 },
      [ordered]@{ path = $GitExecutable; sha256 = $GitExecutableSha256 },
      [ordered]@{ path = $SshExecutable; sha256 = $SshExecutableSha256 },
      [ordered]@{ path = $SshKeygenExecutable; sha256 = $SshKeygenExecutableSha256 }
    )
    pycryptodomeWheelSha256 = $PycryptodomeWheelSha256
    githubHostKeyRawSha256 = $GitHubEd25519HostKeyRawSha256
    remoteRef = $RemoteRef
    brokerBundleDescriptorSha256 = $brokerBundle.descriptorSha256
    brokerModes = @($brokerBundle.modes | ForEach-Object {
      [ordered]@{
        mode = $_.mode
        taskName = $_.taskName
        pipeAddress = $_.pipeAddress
        entryPoint = (Join-Path $AuthorityRoot $_.entryPointRelativePath)
        importRoot = (Join-Path $AuthorityRoot $_.importRootRelativePath)
        arguments = @($_.arguments)
        environment = @($_.environment)
      }
    })
    brokerCredentialAuthorityId = $config.authorityId
    machineProtectedCredentials = @($credentialInspection.credentials | Sort-Object lane)
    formalWorm = $false
    promotionEligible = $false
  }
  $manifestPath = Join-Path $AuthorityRoot "bundle-manifest.json"
  Write-Utf8NoBom $manifestPath (($manifest | ConvertTo-Json -Depth 12) + "`n")
  $credentialRewrapRaw = & $venvPython -I $protectedCredentialRewrapTool rewrap --manifest $manifestPath
  if ($LASTEXITCODE -ne 0) { throw "current-user-broker-credential-rewrap-failed" }
  $credentialRewrapReceipt = $credentialRewrapRaw | ConvertFrom-Json
  Assert-ExactObjectProperties $credentialRewrapReceipt @("schemaVersion", "rewrapped", "manifestSha256", "brokerNetworkRequestCount", "orderMutationCount", "credentials") "broker-credential-rewrap-receipt"
  if (
    $credentialRewrapReceipt.schemaVersion -cne "crypto-first-live-broker-credential-rewrap-receipt/v1" -or
    $credentialRewrapReceipt.rewrapped -ne $true -or $credentialRewrapReceipt.manifestSha256 -cne (Get-Sha256Lower $manifestPath) -or
    $credentialRewrapReceipt.brokerNetworkRequestCount -ne 0 -or $credentialRewrapReceipt.orderMutationCount -ne 0 -or
    @($credentialRewrapReceipt.credentials).Count -ne 2
  ) { throw "broker-credential-rewrap-receipt-invalid" }

  Invoke-TransientSystemMode $launcherPath "Check"
  Invoke-TransientSystemMode $launcherPath "Provision"
  $remoteRefProvisioned = $true
  Register-SystemTask $ServeTaskName $launcherPath "Serve"
  foreach ($mode in $brokerBundle.modes) {
    Register-SystemTask $mode.taskName $launcherPath $mode.mode
  }
  Start-PrearmedSystemTask $ServeTaskName $PipeAddress
  foreach ($mode in $brokerBundle.modes) {
    Start-PrearmedSystemTask $mode.taskName $mode.pipeAddress
  }

  $receipt = [ordered]@{
    schemaVersion = "crypto-first-live-supervised-authority-provisioning-receipt/v1"
    provisioned = $true
    provisionedEpoch = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    authorityId = $config.authorityId
    namespaceId = $config.namespaceId
    keyId = $config.keyId
    authorityOsSid = $SystemSid
    traderOsSid = $TraderOsSid
    githubRepository = $GitHubRepository
    githubBootstrapAdministrator = [string]$me.login
    traderGitHubAdministratorAccepted = [bool]$AllowTraderGitHubAdministrator
    deployKeyId = $deployKeyId
    rulesets = @($rulesetIds)
    remoteRef = $RemoteRef
    bundleManifestSha256 = Get-Sha256Lower $manifestPath
    serveTaskName = $ServeTaskName
    serveTaskOnDemandOnly = $true
    serveTaskStarted = $true
    brokerTasks = @($brokerBundle.modes | ForEach-Object { [ordered]@{ mode = $_.mode; taskName = $_.taskName; pipeAddress = $_.pipeAddress; onDemandOnly = $true; started = $true; prearmed = $true } })
    githubActionsEnabled = $false
    brokerCredentialManifestSha256 = $credentialRewrapReceipt.manifestSha256
    brokerCredentialCiphertextHashes = @($credentialRewrapReceipt.credentials)
    formalWorm = $false
    promotionEligible = $false
    brokerApiRequestCount = 0
    orderMutationCount = 0
  }
  $receiptPath = Join-Path $AuthorityRoot "provisioning-receipt.json"
  Write-Utf8NoBom $receiptPath (($receipt | ConvertTo-Json -Depth 10) + "`n")
  $plan.mutationPerformed = $true
  $receipt | ConvertTo-Json -Depth 10
} catch {
  if (Get-ScheduledTask -TaskName $ServeTaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $ServeTaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $ServeTaskName -Confirm:$false -ErrorAction SilentlyContinue
  }
  if (Get-ScheduledTask -TaskName $TransientTaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TransientTaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TransientTaskName -Confirm:$false -ErrorAction SilentlyContinue
  }
  foreach ($brokerTaskName in $BrokerTaskNames.Values) {
    if (Get-ScheduledTask -TaskName $brokerTaskName -ErrorAction SilentlyContinue) {
      Stop-ScheduledTask -TaskName $brokerTaskName -ErrorAction SilentlyContinue
      Unregister-ScheduledTask -TaskName $brokerTaskName -Confirm:$false -ErrorAction SilentlyContinue
    }
  }
  if ($null -ne $deployKeyId) {
    try { [void](Invoke-GitHubApi "DELETE" "/repos/$GitHubRepository/keys/$deployKeyId" $token) } catch { }
  }
  if (-not $remoteRefProvisioned -and $actionsChanged -and $actionsWereEnabled) {
    try { [void](Invoke-GitHubApi "PUT" "/repos/$GitHubRepository/actions/permissions" $token @{ enabled = $true }) } catch { }
  }
  throw
} finally {
  $token = $null
  Remove-Variable GitHubBootstrapToken -ErrorAction SilentlyContinue
}
