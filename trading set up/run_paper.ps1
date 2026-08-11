param(
    [Parameter(Mandatory = $true)]
    [string]$StreamUrl,
    [int]$DurationSeconds = 0
)

$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$executable = Join-Path $projectDir 'target\release\market-manager.exe'
if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) {
    throw "Release executable not found. Run cargo build --release first."
}

$logDir = Join-Path $projectDir 'data\logs'
$runtimeDir = Join-Path $projectDir 'data\runtime'
New-Item -ItemType Directory -Force -Path $logDir, $runtimeDir | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$stdout = Join-Path $logDir "paper_$stamp.stdout.log"
$stderr = Join-Path $logDir "paper_$stamp.stderr.log"
$arguments = @('paper', '--stream-url', $StreamUrl)
if ($DurationSeconds -gt 0) {
    $arguments += @('--duration-seconds', $DurationSeconds.ToString())
}

$process = Start-Process `
    -FilePath $executable `
    -ArgumentList $arguments `
    -WorkingDirectory $projectDir `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -PassThru

$record = [ordered]@{
    pid = $process.Id
    started_at = (Get-Date).ToString('o')
    stream_url = $StreamUrl
    stdout = $stdout
    stderr = $stderr
}
$record | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runtimeDir 'paper_process.json') -Encoding UTF8
$record | ConvertTo-Json
