$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
  py -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements-desktop.txt

npm install
npm run build
node ..\scripts\desktop_scale_click_contract.mjs --app live_trader --app-root $PWD

.\.venv\Scripts\python.exe tools\create_icon.py

$liveTraderRunning = @(Get-Process -Name "LiveTrader" -ErrorAction SilentlyContinue).Count -gt 0
$distPath = if ($liveTraderRunning) { "release_pending" } else { "release" }
$workPath = if ($liveTraderRunning) { "build_pending" } else { "build" }

if (Test-Path $workPath) {
  Remove-Item -Recurse -Force $workPath
}
if (Test-Path $distPath) {
  Remove-Item -Recurse -Force $distPath
}
if ($liveTraderRunning) {
  Write-Host "LiveTrader is running; building the replacement binary in $distPath without interrupting monitoring."
}

$root = (Get-Location).Path
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

Write-Host "Created LiveTrader executable: $(Join-Path $root "$distPath\LiveTrader.exe")"
