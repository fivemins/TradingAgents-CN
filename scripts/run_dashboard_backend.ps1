$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

& "$root\\venv\\Scripts\\python.exe" -m dashboard_api.app
