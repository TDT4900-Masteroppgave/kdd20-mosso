#!/bin/bash

# Exit immediately if any command fails
set -e

# Check for Python 3 and venv
if ! command -v python3 &> /dev/null; then
    echo "[!] Error: python3 is not installed."
    exit 1
fi

if ! python3 -c "import venv" &> /dev/null; then
    echo "[!] Error: python3-venv is missing. Run: sudo apt-get install python3-venv"
    exit 1
fi

# Setup and Activate Virtual Environment
if [ ! -d ".venv" ]; then
    echo "[*] Creating Python virtual environment (.venv)..."
    python3 -m venv .venv
fi
source .venv/bin/activate

# Install Python Dependencies
pip install -q -r requirements.txt

# Download fastutil (with corruption protection)
FASTUTIL_VER="8.2.2"
FASTUTIL_JAR="fastutil-${FASTUTIL_VER}.jar"
FASTUTIL_URL="https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/${FASTUTIL_VER}/${FASTUTIL_JAR}"

if [ ! -f "$FASTUTIL_JAR" ]; then
    echo "[*] Downloading ${FASTUTIL_JAR}..."
    if command -v wget &> /dev/null; then
        wget -q --show-progress "$FASTUTIL_URL"
    else
        curl -O "$FASTUTIL_URL"
    fi
fi