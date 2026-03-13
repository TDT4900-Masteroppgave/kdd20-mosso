#!/bin/bash
set -e

echo "[*] Compiling Java sources..."

PROJECT_NAME="mosso"
JAR_NAME="${PROJECT_NAME}-1.0.jar"

rm -rf class $JAR_NAME
mkdir -p class

# shellcheck disable=SC2012
FASTUTIL_JAR=$(ls fastutil-*.jar 2>/dev/null | head -n 1)

if [ -z "$FASTUTIL_JAR" ]; then
    echo "[!] Error: fastutil JAR not found in the current directory."
    exit 1
fi

# shellcheck disable=SC2046
javac -cp "./$FASTUTIL_JAR" -d class $(find ./src -name "*.java")

echo "[*] Creating JAR archive..."
cd class
jar cf $JAR_NAME ./
mv $JAR_NAME ../
cd ..

rm -rf class

echo "[*] Build complete: $JAR_NAME"