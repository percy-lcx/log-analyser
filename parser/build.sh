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

echo "Building log-parser..."
GONOSUMDB="*" go build -o log-parser .
echo "Done: $(pwd)/log-parser"
