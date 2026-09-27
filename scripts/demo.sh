#!/usr/bin/env bash
# Thin wrapper: delegates to `rake demo` (single source of truth).
set -euo pipefail

echo "scripts/demo.sh delegates to 'rake demo' (single source of truth)."
exec rake demo "$@"
