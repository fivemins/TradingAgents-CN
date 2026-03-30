$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $root "dashboard_data\logs"
$pidFile = Join-Path $logsDir "dashboard_backend.pid"
$healthUrl = "http://127.0.0.1:8000/api/health"

function Get-DashboardBackendProcesses {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RootPath
    )

    Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -eq "python.exe" -and
            $_.CommandLine -and
            $_.CommandLine -match [regex]::Escape($RootPath) -and
            $_.CommandLine -match "dashboard_api\.app"
        }
}

$stoppedAny = $false

if (Test-Path $pidFile) {
    $pidValue = (Get-Content $pidFile | Select-Object -First 1).Trim()
    if ($pidValue) {
        try {
            Stop-Process -Id ([int]$pidValue) -Force -ErrorAction Stop
            Write-Host "Stopped dashboard backend process $pidValue."
            $stoppedAny = $true
        } catch {
            Write-Warning "Could not stop PID $pidValue directly: $($_.Exception.Message)"
        }
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}

$backendProcesses = Get-DashboardBackendProcesses -RootPath $root |
    Sort-Object ProcessId -Unique
foreach ($processInfo in $backendProcesses) {
    try {
        Stop-Process -Id $processInfo.ProcessId -Force -ErrorAction Stop
        Write-Host "Stopped dashboard backend process $($processInfo.ProcessId)."
        $stoppedAny = $true
    } catch {
        Write-Warning "Could not stop PID $($processInfo.ProcessId): $($_.Exception.Message)"
    }
}

if (-not $stoppedAny) {
    Write-Host "No managed dashboard backend process was running."
}

try {
    Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 2 | Out-Null
    Write-Warning "Port 8000 is still responding. There may be another process serving the dashboard."
} catch {
    Write-Host "Dashboard is no longer responding on port 8000."
}
