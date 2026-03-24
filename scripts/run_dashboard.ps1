$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

Start-Process powershell -ArgumentList "-NoExit", "-File", "$root\\scripts\\run_dashboard_backend.ps1"
Start-Process powershell -ArgumentList "-NoExit", "-File", "$root\\scripts\\run_dashboard_frontend.ps1"
