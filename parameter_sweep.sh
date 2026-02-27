#!/bin/bash
set -e

# Force execution from the repository root, making paths foolproof
cd "$(dirname "$0")"

echo "============================================================"
echo " MoSSo vs Hybrid MoSSo - Parameter Sweep"
echo "============================================================"

# Run the centralized setup script
source setup.sh

# Run the Python script
python3 scripts/parameter_sweep.py "$@"