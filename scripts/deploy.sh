#!/usr/bin/env bash
# Deploy wrapper: runs `databricks bundle deploy` then re-enables AI Gateway.
# Usage: ./scripts/deploy.sh [-t target]
# Example: ./scripts/deploy.sh -t dev
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=== Deploying bundle ==="
databricks bundle deploy "$@"

echo ""
echo "=== Running post-deploy hooks ==="
"$SCRIPT_DIR/post_deploy.sh"
