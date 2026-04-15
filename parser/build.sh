#!/usr/bin/env bash
# Build the native log-parser binary.
# Run this once after cloning, and again whenever parser/main.go changes.
#
# Requirements: Go 1.21+
#   https://go.dev/dl/
#
# Usage:
#   cd parser && bash build.sh
#   # or from repo root:
#   bash parser/build.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure Go is on PATH when invoked from a non-interactive shell (e.g. cmd.exe -> bash).
if ! command -v go >/dev/null 2>&1; then
    for candidate in "/c/Program Files/Go/bin" "$HOME/go/bin"; do
        if [ -x "$candidate/go.exe" ] || [ -x "$candidate/go" ]; then
            export PATH="$candidate:$PATH"
            break
        fi
    done
fi

echo "Building log-parser..."
GONOSUMDB="*" go build -o log-parser .
echo "Done: $(pwd)/log-parser"
