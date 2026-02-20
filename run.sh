#!/usr/bin/env bash
set -euo pipefail

# Windows shells (Git Bash/MSYS/MINGW) set OS=Windows_NT
if [ "${OS:-}" = "Windows_NT" ]; then
  # WINDOWS branch
  CP="./mosso-1.0.jar;./fastutil-8.2.2.jar"
else
  # non-Windows (Linux/macOS/WSL)
  CP="./mosso-1.0.jar:./fastutil-8.2.2.jar"
fi

exec java -cp "$CP" mosso.Run "$@"