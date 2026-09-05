param(
  [switch]$PendingOnly,
  [switch]$SkipDependencyInstall
)

$ErrorActionPreference = "Stop"

$liveTraderAppRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
if (-not [string]::Equals(
  [System.IO.Path]::GetFullPath((Get-Location).Path),
  $liveTraderAppRoot,
  [System.StringComparison]::OrdinalIgnoreCase
)) {
  throw "Run build_exe.ps1 from its Live Trader app directory: $liveTraderAppRoot"
}

function Resolve-LiveTraderBuildOutputPath([string]$Name, [string[]]$AllowedNames) {
  if ($AllowedNames -cnotcontains $Name) {
    throw "Unexpected Live Trader build output name: $Name"
  }
  $outputPath = [System.IO.Path]::GetFullPath((Join-Path $liveTraderAppRoot $Name))
  if (-not [string]::Equals(
    [System.IO.Path]::GetDirectoryName($outputPath),
    $liveTraderAppRoot,
    [System.StringComparison]::OrdinalIgnoreCase
  )) {
    throw "Build output must be an exact child of the Live Trader app directory: $outputPath"
  }
  foreach ($checkedPath in @($liveTraderAppRoot, $outputPath)) {
    if (Test-Path -LiteralPath $checkedPath) {
      $item = Get-Item -LiteralPath $checkedPath -Force
      if (-not $item.PSIsContainer -or ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
        throw "Build output and app root must be ordinary directories, without links or junctions: $checkedPath"
      }
    }
  }
  return $outputPath
}

function Assert-NativeCommandSucceeded([string]$Step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$Step failed with exit code $LASTEXITCODE"
  }
}

if (-not (Test-Path ".venv")) {
  py -m venv .venv
}

if (-not $SkipDependencyInstall) {
  .\.venv\Scripts\python.exe -m pip install --upgrade pip
  Assert-NativeCommandSucceeded "pip upgrade"
  .\.venv\Scripts\python.exe -m pip install -r requirements-desktop.txt
  Assert-NativeCommandSucceeded "desktop dependency install"

  npm install
  Assert-NativeCommandSucceeded "npm install"
} else {
  if (-not (Test-Path ".venv\Scripts\python.exe")) {
    throw "SkipDependencyInstall requires an existing project virtualenv"
  }
  if (-not (Test-Path "node_modules")) {
    throw "SkipDependencyInstall requires existing node_modules"
  }
}
npm run build
Assert-NativeCommandSucceeded "frontend build"
node ..\scripts\desktop_scale_click_contract.mjs --app live_trader --app-root $PWD
Assert-NativeCommandSucceeded "desktop scale/click contract"

.\.venv\Scripts\python.exe tools\create_icon.py
Assert-NativeCommandSucceeded "desktop icon generation"

$liveTraderRunning = @(Get-Process -Name "LiveTrader" -ErrorAction SilentlyContinue).Count -gt 0
$distName = if ($PendingOnly -or $liveTraderRunning) { "release_pending" } else { "release" }
$workName = if ($PendingOnly -or $liveTraderRunning) { "build_pending" } else { "build" }
# Validate both absolute destinations before either recursive removal.
$distPath = Resolve-LiveTraderBuildOutputPath $distName @("release", "release_pending")
$workPath = Resolve-LiveTraderBuildOutputPath $workName @("build", "build_pending")

if (Test-Path -LiteralPath $workPath) {
  Remove-Item -LiteralPath $workPath -Recurse -Force
}
if (Test-Path -LiteralPath $distPath) {
  Remove-Item -LiteralPath $distPath -Recurse -Force
}
if ($PendingOnly) {
  Write-Host "PendingOnly is set; building only in $distPath and leaving the active release untouched."
} elseif ($liveTraderRunning) {
  Write-Host "LiveTrader is running; building the replacement binary in $distPath without interrupting monitoring."
}

$root = $liveTraderAppRoot
$sharedRuntime = [System.IO.Path]::GetFullPath((Join-Path $root "..\..\packages\trading_runtime"))
if (-not (Test-Path -LiteralPath $sharedRuntime)) {
  throw "Shared trading_runtime package was not found: $sharedRuntime"
}
.\.venv\Scripts\python.exe -m PyInstaller `
  --noconfirm `
  --noconsole `
  --onefile `
  --name LiveTrader `
  --icon "$root\assets\app-icon.ico" `
  --distpath $distPath `
  --workpath $workPath `
  --specpath $workPath `
  --add-data "$root\dist;dist" `
  --add-data "$sharedRuntime\trading_runtime\data\market_calendars;trading_runtime\data\market_calendars" `
  --paths "$sharedRuntime" `
  --hidden-import trading_runtime.order_management `
  --hidden-import trading_runtime.audit_events `
  --hidden-import trading_runtime.artifact_governance `
  --hidden-import trading_runtime.risk_engine `
  --hidden-import trading_runtime.strategy_runner `
  --hidden-import trading_runtime.market_calendar `
  --hidden-import websocket `
  --hidden-import Crypto.Cipher.AES `
  --hidden-import Crypto.Util.Padding `
  --collect-submodules webview `
  live_trader\__main__.py
Assert-NativeCommandSucceeded "PyInstaller package"

Write-Host "Created LiveTrader executable: $(Join-Path $distPath "LiveTrader.exe")"
