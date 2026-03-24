$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location "$root\\dashboard-ui"

if (-not (Test-Path "node_modules")) {
    npm install
}

npm run dev
