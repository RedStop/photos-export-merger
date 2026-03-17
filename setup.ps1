# setup.ps1 — Photos Export Merger environment setup (Windows/PowerShell)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "`n=== Photos Export Merger — Setup ===" -ForegroundColor Cyan

# 1. Check Python
Write-Host "`n[1/4] Checking Python..." -ForegroundColor Yellow
try {
    $pyVersion = python --version 2>&1
    Write-Host "  Found: $pyVersion" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: Python not found. Install Python 3.10+ and ensure it is on your PATH." -ForegroundColor Red
    exit 1
}

# 2. Create virtual environment
Write-Host "`n[2/4] Setting up virtual environment..." -ForegroundColor Yellow
if (Test-Path ".venv") {
    Write-Host "  .venv already exists — skipping creation." -ForegroundColor Green
} else {
    Write-Host "  Creating .venv..."
    python -m venv .venv
    Write-Host "  Created .venv" -ForegroundColor Green
}

# 3. Activate venv and install packages
Write-Host "`n[3/4] Installing Python packages..." -ForegroundColor Yellow
& .venv\Scripts\Activate.ps1
pip install -r requirements.txt --quiet
Write-Host "  Packages installed." -ForegroundColor Green

# 4. Check / download ExifTool
Write-Host "`n[4/4] Checking ExifTool..." -ForegroundColor Yellow

$exiftoolFound = $false

# Check system PATH
if (Get-Command exiftool -ErrorAction SilentlyContinue) {
    $etVersion = exiftool -ver 2>&1
    Write-Host "  Found exiftool $etVersion on PATH." -ForegroundColor Green
    $exiftoolFound = $true
}

# Check project folder
if (-not $exiftoolFound -and (Test-Path ".\exiftool.exe")) {
    $etVersion = .\exiftool.exe -ver 2>&1
    Write-Host "  Found exiftool.exe $etVersion in project folder." -ForegroundColor Green
    $exiftoolFound = $true
}

if (-not $exiftoolFound) {
    Write-Host "  ExifTool not found. Attempting auto-download..." -ForegroundColor Yellow

    $zipUrl = "https://exiftool.org/exiftool-12.45.zip"
    $zipFile = "exiftool-12.45.zip"
    $extractDir = "exiftool-12.45_temp"

    try {
        Invoke-WebRequest -Uri $zipUrl -OutFile $zipFile -UseBasicParsing
        Expand-Archive -Path $zipFile -DestinationPath $extractDir -Force

        # The zip contains exiftool(-k).exe — rename it
        $kandExe = Get-ChildItem -Path $extractDir -Filter "exiftool(-k).exe" -Recurse | Select-Object -First 1
        if ($kandExe) {
            Copy-Item $kandExe.FullName -Destination ".\exiftool.exe"
        } else {
            # Some zip versions use a different name
            $anyExe = Get-ChildItem -Path $extractDir -Filter "*.exe" -Recurse | Select-Object -First 1
            if ($anyExe) {
                Copy-Item $anyExe.FullName -Destination ".\exiftool.exe"
            } else {
                throw "No exe found in the downloaded archive."
            }
        }

        # Clean up
        Remove-Item $zipFile -Force -ErrorAction SilentlyContinue
        Remove-Item $extractDir -Recurse -Force -ErrorAction SilentlyContinue

        $etVersion = .\exiftool.exe -ver 2>&1
        Write-Host "  Downloaded exiftool.exe $etVersion to project folder." -ForegroundColor Green
        $exiftoolFound = $true
    } catch {
        # Clean up partial downloads
        Remove-Item $zipFile -Force -ErrorAction SilentlyContinue
        Remove-Item $extractDir -Recurse -Force -ErrorAction SilentlyContinue

        Write-Host "  Auto-download failed: $_" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Please install ExifTool manually:" -ForegroundColor Yellow
        Write-Host "    1. Download from: https://exiftool.org/" -ForegroundColor White
        Write-Host "    2. Either:" -ForegroundColor White
        Write-Host "       a) Place exiftool.exe in this project folder, OR" -ForegroundColor White
        Write-Host "       b) Install to a directory on your system PATH" -ForegroundColor White
        Write-Host "    See: https://exiftool.org/install.html" -ForegroundColor White
    }
}

# Summary
Write-Host "`n=== Setup Summary ===" -ForegroundColor Cyan
Write-Host "  Python:       $pyVersion" -ForegroundColor Green
Write-Host "  Venv:         .venv (active)" -ForegroundColor Green
Write-Host "  Packages:     installed from requirements.txt" -ForegroundColor Green
if ($exiftoolFound) {
    Write-Host "  ExifTool:     ready" -ForegroundColor Green
    Write-Host "`nSetup complete! You can now run:" -ForegroundColor Cyan
} else {
    Write-Host "  ExifTool:     NOT FOUND — see instructions above" -ForegroundColor Red
    Write-Host "`nSetup partially complete. Install ExifTool to finish." -ForegroundColor Yellow
}
Write-Host "  python PhotosExportMerger.py" -ForegroundColor White
Write-Host ""