$ErrorActionPreference = "Stop"

function Sync-UserEnvVar {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $userValue = [Environment]::GetEnvironmentVariable($Name, "User")
    if (-not [string]::IsNullOrWhiteSpace($userValue)) {
        [Environment]::SetEnvironmentVariable($Name, $userValue, "Process")
    }
}

function Ensure-LocalNoProxy {
    $localHosts = "127.0.0.1,localhost"
    foreach ($name in @("NO_PROXY", "no_proxy")) {
        $currentValue = [Environment]::GetEnvironmentVariable($name, "Process")
        if ([string]::IsNullOrWhiteSpace($currentValue)) {
            [Environment]::SetEnvironmentVariable($name, $localHosts, "Process")
            continue
        }

        $entries = $currentValue.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        $updated = [System.Collections.Generic.List[string]]::new()
        foreach ($entry in $entries) {
            if (-not $updated.Contains($entry)) {
                $updated.Add($entry)
            }
        }
        foreach ($localHost in $localHosts.Split(",")) {
            if (-not $updated.Contains($localHost)) {
                $updated.Add($localHost)
            }
        }
        [Environment]::SetEnvironmentVariable($name, ($updated -join ","), "Process")
    }
}

function Test-DashboardHealth {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Get-LatestWriteTimeUtc {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Paths
    )

    $latest = [datetime]::MinValue
    foreach ($path in $Paths) {
        if (-not (Test-Path $path)) {
            continue
        }

        $item = Get-Item $path
        if ($item.PSIsContainer) {
            $candidate = Get-ChildItem $path -Recurse -File | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
            if ($candidate -and $candidate.LastWriteTimeUtc -gt $latest) {
                $latest = $candidate.LastWriteTimeUtc
            }
        } elseif ($item.LastWriteTimeUtc -gt $latest) {
            $latest = $item.LastWriteTimeUtc
        }
    }

    return $latest
}

function Get-EffectiveEnvVar {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Names
    )

    foreach ($name in $Names) {
        $processValue = [Environment]::GetEnvironmentVariable($name, "Process")
        if (-not [string]::IsNullOrWhiteSpace($processValue)) {
            return $processValue
        }

        $userValue = [Environment]::GetEnvironmentVariable($name, "User")
        if (-not [string]::IsNullOrWhiteSpace($userValue)) {
            return $userValue
        }
    }

    return $null
}

function Test-DirectoryWritable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    New-Item -ItemType Directory -Force -Path $Path | Out-Null
    $probe = Join-Path $Path ".write_probe"
    try {
        "ok" | Set-Content -Path $probe -Encoding ascii
        Remove-Item $probe -ErrorAction SilentlyContinue
        return $true
    } catch {
        return $false
    }
}

$root = Split-Path -Parent $PSScriptRoot
$frontendRoot = Join-Path $root "dashboard-ui"
$pythonExe = Join-Path $root "venv\Scripts\python.exe"
$distIndex = Join-Path $frontendRoot "dist\index.html"
$logsDir = Join-Path $root "dashboard_data\logs"
$stdoutLog = Join-Path $logsDir "dashboard_backend.stdout.log"
$stderrLog = Join-Path $logsDir "dashboard_backend.stderr.log"
$pidFile = Join-Path $logsDir "dashboard_backend.pid"
$healthUrl = "http://127.0.0.1:8000/api/health"
$dashboardUrl = "http://127.0.0.1:8000"
$dataDir = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_DASHBOARD_DATA_DIR")
if ([string]::IsNullOrWhiteSpace($dataDir)) {
    $dataDir = Join-Path $root "dashboard_data"
}

if (-not (Test-Path $pythonExe)) {
    throw "Missing Python runtime at $pythonExe. Please create the venv first."
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    throw "npm was not found in PATH. Please install Node.js before starting the dashboard."
}

foreach ($name in @(
    "TRADINGAGENTS_LLM_PROVIDER",
    "TRADINGAGENTS_LLM_BASE_URL",
    "TRADINGAGENTS_QUICK_LLM",
    "TRADINGAGENTS_DEEP_LLM",
    "TRADINGAGENTS_LLM_API_KEY",
    "TRADINGAGENTS_EMBEDDING_BASE_URL",
    "TRADINGAGENTS_EMBEDDING_MODEL",
    "TRADINGAGENTS_EMBEDDING_API_KEY",
    "ARK_API_KEY",
    "OPENAI_API_KEY",
    "FINNHUB_API_KEY",
    "QVERIS_API_KEYS",
    "QVERIS_API_KEY"
)) {
    Sync-UserEnvVar -Name $name
}

Ensure-LocalNoProxy

if (-not (Test-Path (Join-Path $frontendRoot "package.json"))) {
    throw "Missing dashboard-ui/package.json. The frontend workspace is incomplete."
}

$llmProvider = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_LLM_PROVIDER")
$llmBaseUrl = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_LLM_BASE_URL")
$llmApiKey = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_LLM_API_KEY", "ARK_API_KEY", "OPENAI_API_KEY")
$embeddingBaseUrl = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_EMBEDDING_BASE_URL")
$embeddingModel = Get-EffectiveEnvVar -Names @("TRADINGAGENTS_EMBEDDING_MODEL")

if ([string]::IsNullOrWhiteSpace($llmProvider)) {
    throw "Missing TRADINGAGENTS_LLM_PROVIDER. Please configure the default LLM provider before starting the dashboard."
}

if ($llmProvider -ne "ollama" -and [string]::IsNullOrWhiteSpace($llmBaseUrl)) {
    throw "Missing TRADINGAGENTS_LLM_BASE_URL for the configured LLM provider."
}

if ($llmProvider -ne "ollama" -and [string]::IsNullOrWhiteSpace($llmApiKey)) {
    throw "Missing TRADINGAGENTS_LLM_API_KEY / ARK_API_KEY / OPENAI_API_KEY for the configured LLM provider."
}

if (([string]::IsNullOrWhiteSpace($embeddingBaseUrl) -xor [string]::IsNullOrWhiteSpace($embeddingModel))) {
    throw "Embedding preflight failed. TRADINGAGENTS_EMBEDDING_BASE_URL and TRADINGAGENTS_EMBEDDING_MODEL must either both be set or both be empty."
}

if (-not (Test-DirectoryWritable -Path $dataDir)) {
    throw "Dashboard data directory is not writable: $dataDir"
}

$buildInputs = @(
    (Join-Path $frontendRoot "src"),
    (Join-Path $frontendRoot "index.html"),
    (Join-Path $frontendRoot "package.json"),
    (Join-Path $frontendRoot "vite.config.ts"),
    (Join-Path $frontendRoot "tsconfig.json"),
    (Join-Path $frontendRoot "tsconfig.node.json")
)
$needsBuild = -not (Test-Path $distIndex)

if (-not $needsBuild) {
    $latestInput = Get-LatestWriteTimeUtc -Paths $buildInputs
    $distTime = (Get-Item $distIndex).LastWriteTimeUtc
    $needsBuild = $latestInput -gt $distTime
}

if (-not (Test-Path (Join-Path $frontendRoot "node_modules"))) {
    Write-Host "Installing frontend dependencies..."
    Push-Location $frontendRoot
    try {
        npm install
    } finally {
        Pop-Location
    }
}

if ($needsBuild) {
    Write-Host "Building dashboard frontend..."
    Push-Location $frontendRoot
    try {
        npm run build
    } finally {
        Pop-Location
    }
}

if (Test-DashboardHealth -Url $healthUrl) {
    Write-Host "Dashboard is already running with the latest frontend build. Opening browser..."
    Start-Process $dashboardUrl | Out-Null
    exit 0
}

New-Item -ItemType Directory -Force -Path $logsDir | Out-Null
Remove-Item $stdoutLog, $stderrLog -ErrorAction SilentlyContinue

Write-Host "Starting TradingAgents dashboard backend..."
$process = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList "-m", "dashboard_api.app" `
    -WorkingDirectory $root `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden `
    -PassThru

$process.Id | Set-Content -Path $pidFile -Encoding ascii

$ready = $false
for ($attempt = 0; $attempt -lt 60; $attempt++) {
    Start-Sleep -Seconds 1

    if ($process.HasExited) {
        break
    }

    if (Test-DashboardHealth -Url $healthUrl) {
        $ready = $true
        break
    }
}

if (-not $ready) {
    $stdoutTail = ""
    $stderrTail = ""

    if (Test-Path $stdoutLog) {
        $stdoutTail = (Get-Content $stdoutLog -Tail 20) -join [Environment]::NewLine
    }
    if (Test-Path $stderrLog) {
        $stderrTail = (Get-Content $stderrLog -Tail 20) -join [Environment]::NewLine
    }

    throw @"
Dashboard failed to start.
Stdout log: $stdoutLog
Stderr log: $stderrLog

Recent stdout:
$stdoutTail

Recent stderr:
$stderrTail
"@
}

Write-Host "Dashboard is ready at $dashboardUrl"
Start-Process $dashboardUrl | Out-Null
