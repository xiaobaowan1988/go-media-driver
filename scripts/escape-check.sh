#!/usr/bin/env bash
# Fails if a hot-path function contains an allocating construct or a heap escape.
set -euo pipefail
cd "$(dirname "$0")/.."
exec go run ./cmd/escapecheck
