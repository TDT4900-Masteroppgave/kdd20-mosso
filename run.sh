#!/bin/bash
set -e

# Force execution from the repository root, making paths foolproof
cd "$(dirname "$0")"

# Ensure at least one argument is provided
if [ $# -lt 1 ]; then
    echo "[!] Error: No python script specified."
    echo "Usage: ./run.sh <script_name> [args...]"
    echo "Example: ./run.sh compare --runs 5"
    exit 1
fi

SCRIPT_NAME=$1
shift

# Check if the script exists
if [ ! -f "scripts/${SCRIPT_NAME}.py" ]; then
    echo "[!] Error: scripts/${SCRIPT_NAME}.py not found."
    exit 1
fi

# Run the centralized setup script
source setup.sh

# Run the Python script
python3 "scripts/${SCRIPT_NAME}.py" "$@"