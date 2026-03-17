#!/usr/bin/env bash
# setup.sh — Photos Export Merger environment setup (Linux/macOS/WSL)

set -euo pipefail

echo ""
echo "=== Photos Export Merger — Setup ==="

# 1. Check Python
echo ""
echo "[1/4] Checking Python..."
if command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 --version 2>&1)
    echo "  Found: $PY_VERSION"
else
    echo "  ERROR: python3 not found. Install Python 3.10+ and ensure it is on your PATH."
    exit 1
fi

# 2. Create virtual environment
echo ""
echo "[2/4] Setting up virtual environment..."
if [ -d ".venv" ]; then
    echo "  .venv already exists — skipping creation."
else
    echo "  Creating .venv..."
    python3 -m venv .venv
    echo "  Created .venv"
fi

# 3. Activate venv and install packages
echo ""
echo "[3/4] Installing Python packages..."
source .venv/bin/activate
pip install -r requirements.txt --quiet
echo "  Packages installed."

# 4. Check / install ExifTool
echo ""
echo "[4/4] Checking ExifTool..."

EXIFTOOL_FOUND=false

if command -v exiftool &>/dev/null; then
    ET_VERSION=$(exiftool -ver 2>&1)
    echo "  Found exiftool $ET_VERSION on PATH."
    EXIFTOOL_FOUND=true
elif [ -x "./exiftool" ]; then
    ET_VERSION=$(./exiftool -ver 2>&1)
    echo "  Found ./exiftool $ET_VERSION in project folder."
    EXIFTOOL_FOUND=true
fi

if [ "$EXIFTOOL_FOUND" = false ]; then
    echo "  ExifTool not found. Attempting auto-install..."

    INSTALLED=false

    # Try apt (Debian/Ubuntu)
    if command -v apt &>/dev/null; then
        echo "  Detected apt — installing libimage-exiftool-perl..."
        if sudo apt install -y libimage-exiftool-perl; then
            INSTALLED=true
        fi
    # Try brew (macOS)
    elif command -v brew &>/dev/null; then
        echo "  Detected Homebrew — installing exiftool..."
        if brew install exiftool; then
            INSTALLED=true
        fi
    fi

    if [ "$INSTALLED" = true ] && command -v exiftool &>/dev/null; then
        ET_VERSION=$(exiftool -ver 2>&1)
        echo "  Installed exiftool $ET_VERSION"
        EXIFTOOL_FOUND=true
    else
        echo "  Auto-install failed or unsupported package manager."
        echo ""
        echo "  Please install ExifTool manually:"
        echo "    1. Download from: https://exiftool.org/"
        echo "    2. Either:"
        echo "       a) Place the exiftool executable in this project folder, OR"
        echo "       b) Install to a directory on your system PATH"
        echo "    See: https://exiftool.org/install.html"
    fi
fi

# Summary
echo ""
echo "=== Setup Summary ==="
echo "  Python:       $PY_VERSION"
echo "  Venv:         .venv (active)"
echo "  Packages:     installed from requirements.txt"
if [ "$EXIFTOOL_FOUND" = true ]; then
    echo "  ExifTool:     ready"
    echo ""
    echo "Setup complete! You can now run:"
else
    echo "  ExifTool:     NOT FOUND — see instructions above"
    echo ""
    echo "Setup partially complete. Install ExifTool to finish."
fi
echo "  python3 PhotosExportMerger.py"
echo ""