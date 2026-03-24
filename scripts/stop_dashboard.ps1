$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $root "dashboard_data\logs"
$pidFile = Join-Path $logsDir "dashboard_backend.pid"
$healthUrl = "http://127.0.0.1:8000/api/health"

if (Test-Path $pidFile) {
    $pidValue = (Get-Content $pidFile | Select-Object -First 1).Trim()
    if ($pidValue) {
        try {
            Stop-Process -Id ([int]$pidValue) -Force -ErrorAction Stop
            Write-Host "Stopped dashboard backend process $pidValue."
        } catch {
            Write-Warning "Could not stop PID $pidValue directly: $($_.Exception.Message)"
        }
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}

try {
    Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 2 | Out-Null
    Write-Warning "Port 8000 is still responding. There may be another process serving the dashboard."
} catch {
    Write-Host "Dashboard is no longer responding on port 8000."
}
