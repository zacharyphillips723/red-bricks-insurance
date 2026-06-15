#!/usr/bin/env bash
# Post-deploy hook: re-enable AI Gateway inference tables on serving endpoints.
# Endpoint config updates (PUT /config) reset the AI Gateway settings,
# so this must run after every `databricks bundle deploy`.
set -eo pipefail

PROFILE="${DATABRICKS_PROFILE:-fe-vm-red-bricks-aws}"
HOST="https://fevm-red-bricks-insurance.cloud.databricks.com"
TOKEN=$(databricks auth token --profile "$PROFILE" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

enable_ai_gateway() {
    local ep_name="$1" catalog="$2" schema="$3" prefix="$4"

    status=$(curl -s -o /dev/null -w "%{http_code}" \
        "$HOST/api/2.0/serving-endpoints/$ep_name" \
        -H "Authorization: Bearer $TOKEN")

    if [ "$status" != "200" ]; then
        echo "[post_deploy] $ep_name: not found (HTTP $status), skipping"
        return
    fi

    http_code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        "$HOST/api/2.0/serving-endpoints/$ep_name/ai-gateway" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "{
            \"inference_table_config\": {
                \"catalog_name\": \"$catalog\",
                \"schema_name\": \"$schema\",
                \"table_name_prefix\": \"$prefix\",
                \"enabled\": true
            }
        }")

    if [ "$http_code" = "200" ]; then
        echo "[post_deploy] $ep_name: AI Gateway inference tables enabled -> $catalog.$schema.${prefix}_payload"
    else
        echo "[post_deploy] $ep_name: WARNING — could not enable AI Gateway (HTTP $http_code)"
    fi
}

# Endpoints that need AI Gateway inference tables
enable_ai_gateway "fwa-supervisor-agent" "red_bricks_insurance_catalog" "analytics" "fwa_supervisor"
