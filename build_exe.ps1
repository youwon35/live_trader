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

if (Test-Path "build") {
  Remove-Item -Recurse -Force "build"
}
if (Test-Path "release") {
  Remove-Item -Recurse -Force "release"
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
  --distpath release `
  --workpath build `
  --specpath build `
  --add-data "$root\dist;dist" `
  --add-data "$sharedRuntime\trading_runtime\data\market_calendars;trading_runtime\data\market_calendars" `
  --paths "$sharedRuntime" `
  --hidden-import trading_runtime.order_management `
  --hidden-import trading_runtime.audit_events `
  --hidden-import trading_runtime.artifact_governance `
  --hidden-import trading_runtime.risk_engine `
  --hidden-import trading_runtime.strategy_runner `
  --hidden-import trading_runtime.market_calendar `
  --collect-submodules webview `
  live_trader\__main__.py
