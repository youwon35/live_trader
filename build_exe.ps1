$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
  py -m venv .venv
}

.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements-desktop.txt

npm install
npm run build

.\.venv\Scripts\python.exe tools\create_icon.py

if (Test-Path "build") {
  Remove-Item -Recurse -Force "build"
}
if (Test-Path "release") {
  Remove-Item -Recurse -Force "release"
}

$root = (Get-Location).Path
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
  --collect-submodules webview `
  live_trader\__main__.py
