#!/usr/bin/env bash
# prepare.sh — Replace catalog references in dashboard JSON files before bundle deploy.
#
# Lakeview dashboard JSON does not support DAB variable interpolation in SQL
# queries, so this script replaces the catalog portion of catalog.schema.table
# references before deployment.
#
# The script auto-detects whatever catalog name is currently in each dashboard
# file, so it works regardless of which workspace the dashboards were last
# deployed to (no more stale catalog references).
#
# Usage:
#   ./prepare.sh [target_catalog] [--profile <cli_profile>] [workspace_id]
#
# If no arguments are provided, defaults to "red_bricks_insurance" catalog and
# leaves the workspace_id unchanged.
#
# The workspace_id is needed for the System Tables dashboard (system.billing.usage
# and system.access.audit are account-scoped — Databricks SQL has no
# current_workspace_id() function). You can provide it three ways:
#   1. Auto-detect from CLI profile:  ./prepare.sh my_catalog --profile my-profile
#   2. Explicit numeric argument:     ./prepare.sh my_catalog 1234567890123456
#   3. Omit to leave unchanged:       ./prepare.sh my_catalog
#
# Examples:
#   ./prepare.sh                                       # Reset to default catalog, no workspace_id change
#   ./prepare.sh acme_health_plan --profile my-ws      # Auto-detect workspace_id from CLI profile
#   ./prepare.sh acme_health_plan 1234567890123456     # Explicit workspace_id

set -euo pipefail

TARGET_CATALOG="${1:-red_bricks_insurance}"
shift || true
TARGET_WORKSPACE_ID=""
CLI_PROFILE=""

# Parse remaining args: --profile <name> or a numeric workspace_id
while [ $# -gt 0 ]; do
    case "$1" in
        --profile)
            CLI_PROFILE="$2"
            shift 2
            ;;
        [0-9]*)
            TARGET_WORKSPACE_ID="$1"
            shift
            ;;
        *)
            echo "Warning: unknown argument '$1' — ignoring" >&2
            shift
            ;;
    esac
done

# Auto-detect workspace_id from CLI profile if --profile was provided
if [ -n "$CLI_PROFILE" ] && [ -z "$TARGET_WORKSPACE_ID" ]; then
    DETECTED_WS_ID=$(python3 -c "
import configparser, os, sys
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser('~/.databrickscfg'))
profile = sys.argv[1]
if cfg.has_section(profile) and cfg.has_option(profile, 'workspace_id'):
    print(cfg.get(profile, 'workspace_id'))
" "$CLI_PROFILE" 2>/dev/null)
    if [ -n "$DETECTED_WS_ID" ]; then
        TARGET_WORKSPACE_ID="$DETECTED_WS_ID"
        echo "Auto-detected workspace_id=$TARGET_WORKSPACE_ID from profile '$CLI_PROFILE'"
    else
        echo "Warning: could not read workspace_id from profile '$CLI_PROFILE' in ~/.databrickscfg" >&2
    fi
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARD_DIR="$SCRIPT_DIR/src/dashboards"

if [ ! -d "$DASHBOARD_DIR" ]; then
    echo "Error: Dashboard directory not found at $DASHBOARD_DIR" >&2
    exit 1
fi

echo "Preparing dashboards for catalog: '$TARGET_CATALOG'"

# Known schemas in the Red Bricks Insurance data model — used to auto-detect
# the current catalog name in each dashboard file.
KNOWN_SCHEMAS="members|claims|providers|clinical|documents|benefits|care_management|fwa|prior_auth|underwriting|risk_adjustment|network|adt|analytics|raw"

python3 - "$DASHBOARD_DIR" "$TARGET_CATALOG" "$KNOWN_SCHEMAS" << 'PYEOF'
import sys, os, re, json

dashboard_dir = sys.argv[1]
target_catalog = sys.argv[2]
known_schemas = sys.argv[3]

# Pattern: catalog_name.schema_name.table_name (with or without backtick quoting)
# Captures the catalog portion for replacement.
# Handles both underscored (red_bricks_insurance) and hyphenated (clinical-data-demo) catalogs.
pattern = re.compile(
    r'(?P<prefix>`?)(?P<catalog>[a-zA-Z_][a-zA-Z0-9_-]*)(?P=prefix)'
    r'\.'
    r'(?P<smid>`?)(?P<schema>' + known_schemas + r')(?P=smid)'
    r'\.'
    r'(?P<stail>`?)(?P<table>[a-zA-Z_][a-zA-Z0-9_]*)(?P=stail)'
)

total_replacements = 0

for fname in sorted(os.listdir(dashboard_dir)):
    if not fname.endswith('.lvdash.json'):
        continue
    fpath = os.path.join(dashboard_dir, fname)
    with open(fpath, 'r') as f:
        text = f.read()

    # Find all current catalog names in this file
    current_catalogs = set()
    for m in pattern.finditer(text):
        cat = m.group('catalog')
        if cat != 'system':  # Skip system.mlflow.* etc.
            current_catalogs.add(cat)

    if not current_catalogs:
        continue

    # Replace each stale catalog with the target
    file_count = 0
    for old_cat in current_catalogs:
        if old_cat == target_catalog:
            continue
        # Replace both backtick-quoted and unquoted forms
        for old, new in [
            (f'`{old_cat}`', f'`{target_catalog}`'),
            (old_cat, target_catalog),
        ]:
            count = text.count(old)
            if count > 0:
                text = text.replace(old, new)
                file_count += count

    if file_count > 0:
        with open(fpath, 'w') as f:
            f.write(text)
        print(f"  {fname}: replaced {file_count} catalog references ({', '.join(current_catalogs)} → {target_catalog})")
        total_replacements += file_count
    else:
        print(f"  {fname}: already using '{target_catalog}' — no changes")

if total_replacements == 0:
    print("All dashboards already use the target catalog. No changes made.")
else:
    print(f"\nDone. Replaced {total_replacements} total references across dashboard files.")
PYEOF

# Phase 2: Replace workspace_id in system tables dashboard (if provided)
if [ -n "$TARGET_WORKSPACE_ID" ]; then
    SYSTABLES="$DASHBOARD_DIR/system_tables_dashboard.lvdash.json"
    if [ -f "$SYSTABLES" ]; then
        python3 - "$SYSTABLES" "$TARGET_WORKSPACE_ID" << 'PYEOF2'
import sys, re

fpath = sys.argv[1]
new_ws_id = sys.argv[2]

with open(fpath, 'r') as f:
    text = f.read()

# Match workspace_id = <digits> in SQL WHERE clauses
new_text, count = re.subn(r'workspace_id\s*=\s*\d+', f'workspace_id = {new_ws_id}', text)

if count > 0:
    with open(fpath, 'w') as f:
        f.write(new_text)
    print(f"  system_tables_dashboard.lvdash.json: replaced {count} workspace_id references → {new_ws_id}")
else:
    print(f"  system_tables_dashboard.lvdash.json: no workspace_id references found")
PYEOF2
    fi
else
    echo "  (No workspace_id provided — system tables dashboard unchanged)"
fi

echo ""
echo "Next: databricks bundle deploy --target <target>"
echo "  (If using a non-default catalog, also pass: --var=\"catalog=$TARGET_CATALOG\")"
