<#
.SYNOPSIS
    Open the DuckDB web UI (http://localhost:4213) with the Bronze views loaded.

.DESCRIPTION
    Regenerates the view definitions from the running stack (so the newest
    committed runs are included), then starts the DuckDB CLI with its UI.
    Queries run locally in DuckDB and read Parquet straight from MinIO
    (localhost:9000). Type .exit in this terminal to stop the UI.

    Requires: the Docker stack running, and the DuckDB CLI
    (winget install DuckDB.cli).

.PARAMETER NoLaunch
    Only regenerate .duckdb/bronze_init.sql (e.g. for DBeaver or `duckdb -init`).

.EXAMPLE
    ./scripts/bronze-ui.ps1
#>
param([switch]$NoLaunch)
$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot

$duckdb = Get-Command duckdb -ErrorAction SilentlyContinue
if (-not $duckdb) {
    # winget adds duckdb to PATH, but only for shells started after the install.
    $duckdb = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\DuckDB.cli_*\duckdb.exe" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if (-not $duckdb) { throw 'DuckDB CLI not found. Install it with: winget install DuckDB.cli' }
}
$duckdbExe = if ($duckdb.Source) { $duckdb.Source } else { $duckdb.FullName }

# The generated file embeds the MinIO credentials; .duckdb/ is git-ignored.
$outDir = Join-Path $repo '.duckdb'
New-Item -ItemType Directory -Force $outDir | Out-Null
$initFile = Join-Path $outDir 'bronze_init.sql'

Write-Host 'Generating Bronze view definitions from the running stack...'
Push-Location $repo
try {
    $sql = docker compose exec -T airflow-scheduler python -m include.bronze_query --init-sql --s3-endpoint localhost:9000
    if ($LASTEXITCODE -ne 0) { throw 'Could not generate views. Is the stack running? (docker compose up -d)' }
} finally {
    Pop-Location
}
# UTF-8 without BOM: a BOM would corrupt the first SQL statement.
[IO.File]::WriteAllText($initFile, ($sql -join "`n") + "`n", (New-Object Text.UTF8Encoding $false))

if ($NoLaunch) { Write-Host "Wrote $initFile"; return }

Write-Host 'Starting DuckDB UI at http://localhost:4213  (type .exit here to stop)'
& $duckdbExe -init $initFile -ui
