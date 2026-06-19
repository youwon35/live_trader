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

.\.venv\Scripts\python.exe -m PyInstaller `
  --noconfirm `
  --noconsole `
  --onefile `
  --name LiveTrader `
  --icon "assets\app-icon.ico" `
  --distpath release `
  --workpath build `
  --add-data "dist;dist" `
  --collect-submodules webview `
  live_trader\__main__.py
