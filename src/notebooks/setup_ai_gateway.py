# Databricks notebook source

# MAGIC %md
# MAGIC # Red Bricks Insurance — AI Gateway External Model Endpoints
# MAGIC
# MAGIC This notebook registers **Gemini 2.5 Pro** and **GPT-4o** as external model endpoints
# MAGIC via **Mosaic AI Gateway** for multi-model comparison. Combined with the platform-hosted
# MAGIC `databricks-llama-4-maverick` Foundation Model API endpoint, this gives Red Bricks
# MAGIC Insurance three LLMs to evaluate for clinical and claims use cases.
# MAGIC
# MAGIC **Endpoints created:**
# MAGIC | Endpoint | Provider | Model |
# MAGIC |---|---|---|
# MAGIC | `gemini-2-5-pro-gateway` | Google Cloud Vertex AI | Gemini 2.5 Pro |
# MAGIC | `gpt-4o-gateway` | OpenAI | GPT-4o |
# MAGIC
# MAGIC Inference tables are auto-captured to `<catalog>.analytics` for downstream monitoring.

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Google AI Studio keys use the "google" provider (generativelanguage.googleapis.com).
# This is different from "google-cloud-vertex-ai" which requires a GCP service account.
ENDPOINTS = {
    "gemini-2-5-pro-gateway": {
        "provider": "google",
        "model_name": "gemini-2.5-pro",
        "secret_key": "google-api-key",
    },
    "gpt-4o-gateway": {
        "provider": "openai",
        "model_name": "gpt-4o",
        "secret_key": "openai-api-key",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create secret scope for API keys

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create scope if not exists
try:
    resp = requests.post(f"{host}/api/2.0/secrets/scopes/create", headers=headers, json={"scope": "ai-gateway"})
    if resp.status_code == 200:
        print("Created secret scope 'ai-gateway'")
    elif "RESOURCE_ALREADY_EXISTS" in resp.text:
        print("Secret scope 'ai-gateway' already exists")
    else:
        resp.raise_for_status()
except Exception as e:
    print(f"Secret scope: {e}")

print("\nIMPORTANT: Before running the next cell, store your API key(s) via the Databricks CLI:")
print()
print("  # Google AI Studio key (from aistudio.google.com):")
print('  databricks secrets put-secret ai-gateway google-api-key --string-value "YOUR_KEY"')
print()
print("  # OpenAI key (optional — skip if you don't have one):")
print('  databricks secrets put-secret ai-gateway openai-api-key --string-value "YOUR_KEY"')
print()
print("If keys are already set, continue. GPT-4o endpoint will be skipped if no OpenAI key is present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register external model endpoints

# COMMAND ----------

import time

def get_or_create_endpoint(name: str, config: dict):
    """Create an external model serving endpoint via REST API.

    Uses REST API directly because the SDK doesn't yet have a config class
    for the 'google' provider (Google AI Studio). Works for all providers.
    """
    try:
        existing = w.serving_endpoints.get(name)
        print(f"  Endpoint '{name}' already exists (state: {existing.state})")
        return existing
    except Exception:
        pass

    print(f"  Creating endpoint '{name}' ({config['provider']}: {config['model_name']})...")

    # Build provider-specific config block
    provider = config["provider"]
    secret_ref = f"{{{{secrets/ai-gateway/{config['secret_key']}}}}}"

    if provider == "openai":
        provider_config = {"openai_config": {"openai_api_key": secret_ref}}
    elif provider == "google":
        # Google AI Studio (generativelanguage.googleapis.com)
        provider_config = {"google_config": {"google_api_key": secret_ref}}
    else:
        raise ValueError(f"Unknown provider: {provider}")

    payload = {
        "name": name,
        "config": {
            "served_entities": [
                {
                    "external_model": {
                        "name": config["model_name"],
                        "provider": provider,
                        "task": "llm/v1/chat",
                        **provider_config,
                    }
                }
            ],
            "auto_capture_config": {
                "catalog_name": catalog,
                "schema_name": "analytics",
                "enabled": True,
            },
        },
    }

    resp = requests.post(
        f"{host}/api/2.0/serving-endpoints",
        headers=headers,
        json=payload,
    )
    if resp.ok:
        print(f"  Created '{name}'")
    else:
        print(f"  Error creating '{name}': {resp.text[:300]}")
    return resp.json() if resp.ok else None

# Check which secrets exist so we can skip endpoints without keys
existing_secrets = set()
try:
    resp = requests.get(f"{host}/api/2.0/secrets/list", headers=headers, params={"scope": "ai-gateway"})
    if resp.ok:
        existing_secrets = {s["key"] for s in resp.json().get("secrets", [])}
except Exception:
    pass

for ep_name, ep_config in ENDPOINTS.items():
    if ep_config["secret_key"] not in existing_secrets:
        print(f"  Skipping '{ep_name}' — no secret '{ep_config['secret_key']}' found in ai-gateway scope")
        continue
    get_or_create_endpoint(ep_name, ep_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable inference tables on existing Llama endpoint

# COMMAND ----------

try:
    llama_ep = w.serving_endpoints.get("databricks-llama-4-maverick")
    # FMAPI endpoints auto-capture; just note it
    print("databricks-llama-4-maverick is a Foundation Model API endpoint")
    print("  Inference logging is handled by the platform automatically")
except Exception as e:
    print(f"Could not check Llama endpoint: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test endpoints

# COMMAND ----------

def test_endpoint(name: str):
    print(f"\nTesting {name}...")
    try:
        resp = w.api_client.do("POST", f"/serving-endpoints/{name}/invocations", body={
            "messages": [{"role": "user", "content": "What is a prior authorization in health insurance? Answer in one sentence."}],
            "max_tokens": 100,
            "temperature": 0.0,
        })
        answer = resp.get("choices", [{}])[0].get("message", {}).get("content", "No response")
        print(f"  Response: {answer[:200]}")
        return True
    except Exception as e:
        print(f"  Error: {e}")
        print(f"  (This is expected if API keys aren't configured yet)")
        return False

# Only test endpoints that exist (Llama is always available; gateway endpoints may have been skipped)
endpoints_to_test = ["databricks-llama-4-maverick"]
for ep_name, ep_config in ENDPOINTS.items():
    if ep_config["secret_key"] in existing_secrets:
        endpoints_to_test.append(ep_name)

results = {}
for name in endpoints_to_test:
    results[name] = test_endpoint(name)

print(f"\n{'='*60}")
print("Endpoint Test Results:")
for name, ok in results.items():
    status = "PASS" if ok else "NEEDS API KEY"
    print(f"  {name}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant CAN_QUERY to app service principals

# COMMAND ----------

# Grant CAN_QUERY on gateway endpoints to all users (for demo purposes)
for ep_name, ep_config in ENDPOINTS.items():
    if ep_config["secret_key"] not in existing_secrets:
        continue
    try:
        resp = requests.put(
            f"{host}/api/2.0/permissions/serving-endpoints/{ep_name}",
            headers=headers,
            json={"access_control_list": [{"group_name": "users", "permission_level": "CAN_QUERY"}]},
        )
        if resp.ok:
            print(f"  Granted CAN_QUERY on '{ep_name}' to users")
        else:
            print(f"  Permission grant for '{ep_name}': {resp.text[:200]}")
    except Exception as e:
        print(f"  Permission error for '{ep_name}': {e}")
