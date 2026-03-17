#!/bin/bash
set -e

if ! command -v python3 &> /dev/null; then
    echo "[!] Error: python3 is not installed."
    exit 1
fi

if [ ! -d ".venv" ]; then
    echo "[*] Creating Python virtual environment (.venv)..."
    python3 -m venv .venv
fi
source .venv/bin/activate

pip install -q -r requirements.txt --disable-pip-version-check

FASTUTIL_VER="8.2.2"
FASTUTIL_JAR="fastutil-${FASTUTIL_VER}.jar"
if [ ! -f "$FASTUTIL_JAR" ]; then
    echo "[*] Downloading ${FASTUTIL_JAR}..."
    curl -sO "https://repo1.maven.org/maven2/it/unimi/dsi/fastutil/${FASTUTIL_VER}/${FASTUTIL_JAR}"
fi

TYPE=""
for arg in "$@"; do
  case $arg in
    --type=*)
      TYPE="${arg#*=}"
      shift
      ;;
    --type)
      TYPE="$2"
      shift 2
      ;;
  esac
done

case $TYPE in
    compare)
        python3 scripts/benchmarks/compare.py "$@"
        ;;
    sweep)
        python3 scripts/benchmarks/parameter_sweep.py "$@"
        ;;
    latin)
        python3 scripts/benchmarks/latin_hypercube.py "$@"
        ;;
    *)
        echo "Usage: ./run.sh --type {compare|sweep|latin} [options]"
        echo "Example: ./run.sh --type compare --group one --algos mags"
        exit 1
        ;;
esac