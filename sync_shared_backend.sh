#!/usr/bin/env bash
# sync_shared_backend.sh — Copy canonical shared backend modules to each app.
#
# The shared library lives in lib/shared_backend/. Each Databricks App deploys
# from its own source_code_path, so shared code must be physically present in
# each app's backend/ directory. This script copies the canonical versions and
# preserves per-app configuration (database name defaults, genie titles, etc.)
# via the DAB resource config env vars.
#
# Usage: ./sync_shared_backend.sh
#
# Run this after editing any file in lib/shared_backend/, then commit the
# resulting changes in each app's backend/ directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARED="$SCRIPT_DIR/lib/shared_backend"

# Apps that use Lakebase (have database.py)
DB_APPS=("app" "app-fwa" "app-underwriting-sim" "app-prior-auth")

echo "Syncing shared backend modules..."

# --- database.py → apps with Lakebase ---
for app in "${DB_APPS[@]}"; do
    target="$SCRIPT_DIR/$app/backend/database.py"
    if [ -d "$SCRIPT_DIR/$app/backend" ]; then
        cp "$SHARED/database.py" "$target"
        echo "  $app/backend/database.py ✓"
    fi
done

echo ""
echo "Done. Shared modules synced to ${#DB_APPS[@]} apps."
echo "Note: env_config.py and genie.py have per-app config — edit those in-place."
echo "      The canonical versions in lib/shared_backend/ serve as reference."
