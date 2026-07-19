# Databricks notebook source
# MAGIC %md
# MAGIC # Red Bricks Insurance — Plan Document Vector Search Index
# MAGIC
# MAGIC Generates synthetic **Summary of Benefits & Coverage (SBC)** / plan-design
# MAGIC documents for each Red Bricks plan, chunks them by section, and builds a
# MAGIC **Delta Sync Vector Search index** so the Group Reporting Sales Coach can
# MAGIC answer real benefit-design questions from documents (not just gold-table
# MAGIC metrics). Uses managed embeddings (`databricks-bge-large-en`).
# MAGIC
# MAGIC **Chunks table:** `analytics.plan_document_chunks`
# MAGIC **VS Index:** `analytics.plan_document_vs_index`

# COMMAND ----------

dbutils.widgets.text("catalog", "red_bricks_insurance_catalog", "Catalog")
catalog = dbutils.widgets.get("catalog")

VS_ENDPOINT_NAME = "red-bricks-vs-endpoint"
VS_INDEX_NAME = f"{catalog}.analytics.plan_document_vs_index"
SOURCE_TABLE = f"{catalog}.analytics.plan_document_chunks"
print(f"VS Index: {VS_INDEX_NAME}")
print(f"Source Table: {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic SBC / plan-design documents
# MAGIC
# MAGIC Five representative plans spanning funding types and richness. Each plan
# MAGIC becomes several section chunks (cost sharing, covered services, care
# MAGIC programs, exclusions, networks). Synthetic — no real member/PHI data.

# COMMAND ----------

PLANS = [
    {
        "plan_id": "RB-PPO-GOLD",
        "plan_name": "Red Bricks PPO Gold 1000",
        "funding_type": "Fully Insured",
        "metal_tier": "Gold",
        "sections": {
            "Cost Sharing": "Individual deductible $1,000; family $2,000. Coinsurance 20% after deductible. Out-of-pocket maximum $4,000 individual / $8,000 family. Primary care copay $20; specialist $40; urgent care $60; emergency room $300 (waived if admitted).",
            "Covered Services": "Preventive care covered 100% in-network with no cost share. Inpatient hospital, outpatient surgery, maternity, mental health, and substance use disorder covered at 20% coinsurance after deductible. Telehealth visits $10 copay.",
            "Prescription Drugs": "Four-tier formulary. Generic $10; preferred brand $35; non-preferred brand $70; specialty 25% up to $250. Mail-order 90-day supply at 2x retail copay. Biosimilar substitution encouraged.",
            "Care Management Programs": "Includes Complex Case Management for members with 2+ chronic conditions, Diabetes Prevention Program, Behavioral Health EAP+ (12 sessions), and Centers of Excellence steerage for joint replacement and cardiac surgery with bundled pricing.",
            "Networks and Referrals": "Broad PPO network; no referrals required for specialists. Out-of-network covered at 40% coinsurance after a separate $2,000 deductible. Tiered network option available for a 5% premium reduction.",
            "Exclusions and Limitations": "Excludes cosmetic surgery, experimental treatments, and non-FDA-approved therapies. Bariatric surgery covered only through a Center of Excellence with prior authorization.",
        },
    },
    {
        "plan_id": "RB-HDHP-HSA",
        "plan_name": "Red Bricks HDHP 3000 with HSA",
        "funding_type": "Self-Funded",
        "metal_tier": "Silver",
        "sections": {
            "Cost Sharing": "Individual deductible $3,000; family $6,000 (embedded). Coinsurance 10% after deductible. Out-of-pocket maximum $6,000 individual / $12,000 family. HSA-qualified; employer seeds $750 individual / $1,500 family annually.",
            "Covered Services": "Preventive care covered 100% pre-deductible per ACA. All other services subject to deductible then 10% coinsurance. Telehealth subject to deductible (HSA rules) then $0.",
            "Prescription Drugs": "Subject to medical deductible, then generic $5 / preferred brand $30 / specialty 20%. Preventive drug list (statins, insulin, antihypertensives) covered pre-deductible under IRS safe harbor.",
            "Care Management Programs": "Musculoskeletal (MSK) digital PT program, Pharmacy Benefit Optimization with biosimilar conversion, and Maternity Management with NICU avoidance. Diabetes Prevention Program available at no cost pre-deductible.",
            "Networks and Referrals": "Narrow high-value network. Out-of-network not covered except emergencies. Reference-based pricing applies to facility claims above the 200% Medicare benchmark.",
            "Exclusions and Limitations": "Excludes non-emergency out-of-network care, cosmetic procedures, and long-term custodial care. Spousal surcharge $100/month if spouse has other coverage available.",
        },
    },
    {
        "plan_id": "RB-HMO-BRONZE",
        "plan_name": "Red Bricks HMO Bronze 5000",
        "funding_type": "Fully Insured",
        "metal_tier": "Bronze",
        "sections": {
            "Cost Sharing": "Individual deductible $5,000; family $10,000. Coinsurance 40% after deductible. Out-of-pocket maximum $8,700 individual. Primary care $40 copay; specialist requires referral, $80 copay.",
            "Covered Services": "Preventive care 100%. Inpatient and outpatient services at 40% after deductible. Emergency room $500 copay then deductible. Mental health and SUD at parity with medical.",
            "Prescription Drugs": "Generic $15; preferred brand $60; non-preferred and specialty subject to deductible then 40%. Step therapy required for specialty and high-cost brand drugs.",
            "Care Management Programs": "Diabetes Prevention Program and Behavioral Health EAP+ included. Complex Case Management available for high-cost claimants above $50K annually.",
            "Networks and Referrals": "HMO network; PCP referral required for all specialist care. No out-of-network coverage except emergencies. Onsite/nearsite clinic access for groups of 200+ lives.",
            "Exclusions and Limitations": "Excludes out-of-network care, cosmetic and experimental treatments. Non-referred specialist visits are not covered.",
        },
    },
    {
        "plan_id": "RB-EPO-PLUS",
        "plan_name": "Red Bricks EPO Plus 2000",
        "funding_type": "Level Funded",
        "metal_tier": "Gold",
        "sections": {
            "Cost Sharing": "Individual deductible $2,000; family $4,000. Coinsurance 15% after deductible. Out-of-pocket maximum $5,500 individual. PCP $25; specialist $50 (no referral); telehealth $0.",
            "Covered Services": "Preventive 100%. Broad coverage of inpatient, outpatient, maternity, behavioral health at 15% after deductible. Advanced imaging (MRI/CT) requires prior authorization.",
            "Prescription Drugs": "Generic $10; preferred brand $40; non-preferred $75; specialty 20% up to $200. Mail-order and specialty pharmacy management included.",
            "Care Management Programs": "Full suite: Complex Case Management, Centers of Excellence, MSK program, Maternity Management, and Pharmacy Benefit Optimization. Level-funded groups receive a year-end claims surplus refund.",
            "Networks and Referrals": "EPO network — no out-of-network coverage except emergencies, but no referrals needed. Tiered hospital network with lower cost share at preferred facilities.",
            "Exclusions and Limitations": "Excludes out-of-network non-emergency care, cosmetic procedures. Prior authorization required for advanced imaging, DME over $1,000, and non-emergency inpatient admissions.",
        },
    },
    {
        "plan_id": "RB-PPO-PLATINUM",
        "plan_name": "Red Bricks PPO Platinum 500",
        "funding_type": "Fully Insured",
        "metal_tier": "Platinum",
        "sections": {
            "Cost Sharing": "Individual deductible $500; family $1,000. Coinsurance 10% after deductible. Out-of-pocket maximum $2,500 individual / $5,000 family. PCP $10; specialist $25; ER $150 (waived if admitted).",
            "Covered Services": "Preventive 100%. Rich coverage across all categories at 10% coinsurance. Includes adult dental and vision riders, and expanded fertility benefits up to $25,000 lifetime.",
            "Prescription Drugs": "Generic $5; preferred brand $25; non-preferred $50; specialty 15%. Zero-cost preventive and chronic-condition medications (diabetes, asthma, hypertension).",
            "Care Management Programs": "Concierge care navigation, Complex Case Management, Centers of Excellence, Maternity Management, Behavioral Health EAP+ (unlimited virtual), and onsite clinic for large groups.",
            "Networks and Referrals": "Broadest PPO network, no referrals. Out-of-network at 30% after $1,000 deductible. Nationwide travel coverage.",
            "Exclusions and Limitations": "Excludes cosmetic surgery and experimental treatments. Fertility benefit requires clinical eligibility criteria.",
        },
    },
]

chunks = []
for plan in PLANS:
    header = f"{plan['plan_name']} ({plan['metal_tier']} · {plan['funding_type']})"
    # Overview chunk
    chunks.append({
        "chunk_id": f"{plan['plan_id']}_overview",
        "plan_id": plan["plan_id"],
        "plan_name": plan["plan_name"],
        "funding_type": plan["funding_type"],
        "metal_tier": plan["metal_tier"],
        "section": "Overview",
        "chunk_text": f"{header}. Summary of Benefits and Coverage. This document summarizes cost sharing, covered services, prescription drug tiers, care management programs, network rules, and exclusions for the {plan['plan_name']}.",
    })
    for section, text in plan["sections"].items():
        chunks.append({
            "chunk_id": f"{plan['plan_id']}_{section.replace(' ', '_').lower()}",
            "plan_id": plan["plan_id"],
            "plan_name": plan["plan_name"],
            "funding_type": plan["funding_type"],
            "metal_tier": plan["metal_tier"],
            "section": section,
            "chunk_text": f"{header} | {section}\n\n{text}",
        })

print(f"Generated {len(chunks)} plan-document chunks from {len(PLANS)} plans")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("plan_id", StringType(), False),
    StructField("plan_name", StringType(), True),
    StructField("funding_type", StringType(), True),
    StructField("metal_tier", StringType(), True),
    StructField("section", StringType(), True),
    StructField("chunk_text", StringType(), True),
])
df = spark.createDataFrame(chunks, schema)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.analytics")
(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(SOURCE_TABLE))
print(f"Wrote {spark.table(SOURCE_TABLE).count()} chunks to {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/reuse VS endpoint + Delta Sync index (managed embeddings)

# COMMAND ----------

import requests, time

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def vs_get(path):
    r = requests.get(f"{host}/api/2.0/vector-search/{path}", headers=headers); r.raise_for_status(); return r.json()
def vs_post(path, body):
    r = requests.post(f"{host}/api/2.0/vector-search/{path}", headers=headers, json=body); r.raise_for_status(); return r.json()

# Reuse the shared VS endpoint (created by setup_vector_search / setup_medical_policy_vs).
eps = [e["name"] for e in vs_get("endpoints").get("endpoints", [])]
if VS_ENDPOINT_NAME not in eps:
    print(f"Creating VS endpoint {VS_ENDPOINT_NAME}...")
    vs_post("endpoints", {"name": VS_ENDPOINT_NAME, "endpoint_type": "STANDARD"})
    for i in range(60):
        st = vs_get(f"endpoints/{VS_ENDPOINT_NAME}").get("endpoint_status", {}).get("state", "UNKNOWN")
        if st == "ONLINE":
            break
        time.sleep(10)
else:
    print(f"Reusing VS endpoint {VS_ENDPOINT_NAME}")

# COMMAND ----------

try:
    info = vs_get(f"indexes/{VS_INDEX_NAME}")
    if info.get("status", {}).get("ready"):
        print("Index exists and ready — triggering sync for new chunks.")
        try:
            vs_post(f"indexes/{VS_INDEX_NAME}/sync", {})
        except requests.exceptions.HTTPError:
            pass
except requests.exceptions.HTTPError as e:
    if e.response.status_code == 404:
        print(f"Creating index {VS_INDEX_NAME}...")
        vs_post("indexes", {
            "name": VS_INDEX_NAME,
            "endpoint_name": VS_ENDPOINT_NAME,
            "primary_key": "chunk_id",
            "index_type": "DELTA_SYNC",
            "delta_sync_index_spec": {
                "source_table": SOURCE_TABLE,
                "pipeline_type": "TRIGGERED",
                "embedding_source_columns": [
                    {"name": "chunk_text", "embedding_model_endpoint_name": "databricks-bge-large-en"}
                ],
            },
        })
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for readiness + test query

# COMMAND ----------

for i in range(120):
    try:
        st = vs_get(f"indexes/{VS_INDEX_NAME}").get("status", {})
        if i % 6 == 0 or st.get("ready"):
            print(f"  [{i*10}s] state={st.get('detailed_state')}, ready={st.get('ready')}, rows={st.get('indexed_row_count', 0)}")
        if st.get("ready"):
            break
    except Exception as e:
        print(f"  [{i*10}s] waiting... ({e})")
    time.sleep(10)

for q in ["Which plans cover bariatric surgery?", "What is the HSA employer seed amount?",
          "Which plans require referrals for specialists?", "What care management programs help high-cost claimants?"]:
    try:
        res = vs_post(f"indexes/{VS_INDEX_NAME}/query", {
            "columns": ["chunk_id", "plan_name", "section", "chunk_text"],
            "query_text": q, "num_results": 3})
        data = res.get("result", {}).get("data_array", [])
        print(f"\nQ: {q}  -> {len(data)} chunks")
        for row in data[:2]:
            print(f"  - {row[1]} | {row[2]}: {str(row[3])[:90]}...")
    except Exception as e:
        print(f"\nQ failed: {q} — {e}")

print("\nPlan-document VS index ready for the Sales Coach RAG.")
