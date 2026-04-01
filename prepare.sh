#!/usr/bin/env bash
# prepare.sh — Replace catalog name in dashboard JSON files before bundle deploy.
#
# Lakeview dashboard JSON does not support DAB variable interpolation in SQL
# queries, so this script does a find-and-replace before deployment.
#
# Usage:
#   ./prepare.sh <catalog_name>
#
# If no argument is provided, defaults to "red_bricks_insurance" (no-op).
#
# Example:
#   ./prepare.sh acme_health_plan
#   databricks bundle deploy --target my-workspace -var="catalog=acme_health_plan"

set -euo pipefail

CATALOG="${1:-red_bricks_insurance}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARD_DIR="$SCRIPT_DIR/src/dashboards"

if [ "$CATALOG" = "red_bricks_insurance" ]; then
    echo "Catalog is already red_bricks_insurance — no dashboard changes needed."
    exit 0
fi

echo "Replacing 'red_bricks_insurance' → '$CATALOG' in dashboard JSON files..."

for f in "$DASHBOARD_DIR"/*.lvdash.json; do
    if [ -f "$f" ]; then
        count=$(grep -c "red_bricks_insurance" "$f" 2>/dev/null || true)
        if [ "$count" -gt 0 ]; then
            sed -i '' "s/red_bricks_insurance/$CATALOG/g" "$f"
            echo "  $(basename "$f"): replaced $count occurrences"
        fi
    fi
done

echo "Done. Now run: databricks bundle deploy --target <target> -var=\"catalog=$CATALOG\""
