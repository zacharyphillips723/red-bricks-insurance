# Underwriting Simulation → "Future of Underwriting" — Phased Roadmap

**Source vision:** `underwriting_vision_b2.html` (Matthew Giglia, Oct 2026) — a quote-lifecycle
platform: **Submit → Assemble → Quote/Scenarios → Approve/Deliver → Govern**, across 4 personas
(CFO/Chief Actuary, Underwriter, AE/Broker, Actuarial Team), every funding arrangement, fully
governed, NAIC-AI aligned.

**Current app:** `app-underwriting-sim` — a deep actuarial **what-if simulator** (FastAPI + React),
state on Lakehouse Delta `app_state` (post the Lakebase→Delta conversion).

---

## The reframe (why this plan wraps, not rebuilds)

The actuarial **math already exists** and is strong. What the vision adds is the **funnel, the
workflow spine, persona framing, funding-arrangement routing**, and — as the capstone — an
**agentic memory / digital twin** that gives the system institutional judgment over time. The
credible pitch is *"the engine exists; we're adding the lifecycle, governance, and memory around
it,"* not *"we're building underwriting from scratch."*

### Current strengths — do NOT rebuild
- **11 simulation types** (`simulation_engine.py`): premium_rate, benefit_design, group_renewal
  (credibility-blended), population_mix, medical_trend, stop_loss (specific), aggregate_stop_loss,
  risk_adjustment/RAF, utilization_change, new_group_quote, ibnr_reserve.
- **Community-rated rate build-up** with step-by-step factors (`pricing_engine.py`) + a **governed
  factor overlay** (`analytics.gold_pricing_factors` over hardcoded fallbacks, 15-min TTL).
- Save / compare / status / **audit log** workflow (`scenarios.py`, `/simulations`, `/comparisons`).
- Conversational **agent** (`agent.py`, streaming SSE) + **observability** (traces + model cost) + **Genie**.

### Gap analysis (condensed)

| Capability | Today | Phase |
|---|---|---|
| Actuarial scenario math | ✅ strength | — |
| Multi-persona views | ❌ single workbench | 1 |
| Packaged scenarios (Standard/Competitive/Retention/Custom) | ❌ per-lever only | 1 |
| Explainability / NAIC surface | ⚠️ per-sim narratives | 1 |
| Funding-arrangement routing (FI / ASO / Level-Funded / Specialty / Stop-Loss / Disability) | ⚠️ generic | 2 |
| New-business vs renewal adaptive weighting | ⚠️ separate paths | 2 |
| Approval routing by authority matrix | ⚠️ free-form status | 2 |
| Factor catalog auto-computed from claims + actuary approve/version | ⚠️ UC overlay only | 2 |
| NL quote intake | ⚠️ analytical chat | 3 |
| Document intelligence (census/SBC/claims/competitor) | ❌ | 3 |
| Completeness gating / strategy-memo pre-fill | ❌ | 3 |
| Negotiation / re-rate loop | ⚠️ ad-hoc | 3 |
| Rated→Sold→Implemented reconciliation | ❌ | 3 |
| Operational analytics (cycle time, conversion, factor drift) | ⚠️ traces/cost only | 3 |
| **Agentic memory + Underwriting Digital Twin (pre-implementation check)** | ❌ | **4 (optional)** |
| Portfolio optimization (RUAH / MILP) | ❌ | Beyond roadmap |

---

## Architecture principle: hybrid Lakehouse + Lakebase (deliberate, not a reversal)

We converted this app's state **off Lakebase to Delta** this cycle. Phase 4 reintroduces Lakebase
**only for the agent's hot memory layer** — the correct split:

- **Delta (`app_state` + gold)** = system of record: governed decisions, audit, analytics, lineage.
  Keeps the NAIC/audit story.
- **Lakebase (Postgres + pgvector)** = the agent's *working memory*: millisecond point lookups,
  frequent small writes, semantic recall of past decisions. This is what Delta is bad at and
  Lakebase is for.
- A **periodic sync** snapshots Lakebase memory → Delta for governance/lineage.
- **Reuse** the existing Lakebase connection + OAuth-refresh code (still in git history / live in
  `app-provider-scrub`) — no greenfield plumbing.

---

## Roadmap at a glance

| Phase | Theme | Outcome | Effort | Depends on |
|---|---|---|---|---|
| **1** | Wrap the engine in the vision | Looks & feels like the artifact; high demo ROI | Low (mostly FE + thin BE) | — |
| **2** | Actuarial + workflow substance | Funding-arrangement routing, approvals, factor governance | Medium | 1 |
| **3** | Front-of-funnel AI | Doc intelligence, NL intake, negotiation, reconciliation, ops analytics | Medium-High | 1,2 |
| **4 (optional)** | Agentic memory + Digital Twin | Pre-implementation check grounded in institutional memory | High | memory accrues from 1+ |

> Memory capture (the `decision_memory` write) can be switched on as early as Phase 1 so that by the
> time Phase 4 lands, the twin isn't starting cold.

---

## Phase 1 — Wrap the engine in the vision (low risk, high demo ROI)

**1.1 Persona shell.** CFO / Underwriter / AE / Actuary selector that filters existing screens
(mirrors the artifact's persona bar). Pure frontend state; no backend change.

**1.2 Packaged scenarios.** A thin layer over `run_simulation` that generates **Standard /
Competitive / Retention / Custom** from one input set and returns a comparative recommendation.
- New backend module `scenario_packager.py` + endpoint `POST /api/scenarios/package`.
- Standard = hit target MLR/margin; Competitive = match a named competitor within N%; Retention =
  minimize modeled lapse; Custom = analyst params. Reuse `simulation_engine` for each leg.
- Recommendation narrative via the existing agent (BCR / margin / retention tradeoff).

**1.3 Governance / NAIC surface.** Expose what already exists: factor **provenance** badge
(`source: uc_table | fallback`), the rate build-up step trace, and an explainability/compliance
panel (explainable recommendation, model inventory stub, adverse-action note). Mostly surfacing.

---

## Phase 2 — Actuarial + workflow substance

**2.1 Funding-arrangement routing.** A quote intake that branches by arrangement and dispatches to
the right calc. You already have FI (`group_renewal`, `new_group_quote`), specific & aggregate
stop-loss. **Add**: ASO/self-funded (admin fee + claims projection + specific/agg stop-loss),
Level-Funded (FI-derivative + settlement), Specialty dental/vision (manual/blended), Disability
STD/LTD (payroll + occupation class). New module `funding_arrangements.py` + a routing dispatcher.

**2.2 Approval routing by authority matrix.** **Reuse `app-prior-auth`'s** routing-rules + approver
context + audit pattern (same repo, same app-state model). Extend the simulation status workflow
into authority-tiered routing (dollar/retention thresholds → approver level).

**2.3 Factor governance.** Add an **SDP gold pipeline** that derives `gold_pricing_factors` from
claims (fits the existing medallion), plus an in-app actuary **review → approve → version**
workflow on that table. Closes the vision's "actuaries govern, not compute" loop; `pricing_engine`
already consumes the governed table.

---

## Phase 3 — Front-of-funnel AI (real builds — don't over-promise)

**3.1 Document intelligence** — `ai_parse_document` / `ai_extract` over census, SBCs, claims
experience, competitor quotes, RFPs → structured extract → pre-fill. (Lands structured rows the
rest of the funnel consumes.)

**3.2 NL quote intake + completeness gating** — natural-language submission → structured strategy
memo with a **% complete + missing-fields** gate ("pre-filled 78%; missing: concessions, target
BCR, lasers"). AE *verifies*, doesn't fill from scratch.

**3.3 Negotiation / re-rate loop** — the agent re-rates a saved quote on an NL change
("$3,000 deductible", "+200 lives", "drop dental"), tracks every revision. Extends `agent.py` +
`/simulate`.

**3.4 Rated→Sold→Implemented reconciliation** — persist the quote lineage and reconcile against
sold/implemented; flag mismatches continuously (the audit trail "that never existed before").

**3.5 Operational analytics** — extend observability beyond traces/cost to **quote cycle time,
conversion rate, extraction accuracy, factor drift, approval velocity**.

---

## Phase 4 (FINAL, OPTIONAL) — Agentic memory + Underwriting Digital Twin

> **Role:** an *advisory* digital twin that sits as a **gate between scenario selection (Phase 1/3)
> and approve/implement (Phase 2)**. Before an analyst commits a rating/underwriting policy, it runs
> a **pre-implementation check** grounded in institutional memory and projects the dollar impact.
> **Human-in-the-loop, never autonomous** — on million-dollar decisions it checks, it doesn't decide.

### 4.1 Lakebase provisioning
- Lakebase Autoscaling project (reuse the existing `database.py` OAuth-refresh connection pattern
  from `app-provider-scrub`). Database e.g. `uw_twin`.
- Enable pgvector: `CREATE EXTENSION IF NOT EXISTS vector;`
- **Fallback if pgvector is unavailable** in the target Lakebase: mirror `decision_memory` to Delta
  and use **Databricks Vector Search** for recall; keep the structured store in Lakebase. (Decide at
  provisioning time; the recall interface is abstracted either way.)
- Embeddings via Databricks FM API `databricks-gte-large-en` (1024-dim; make dim a config constant).

### 4.2 Memory schema (Lakebase / Postgres)

```sql
CREATE EXTENSION IF NOT EXISTS vector;

-- One row per committed (or candidate) underwriting decision.
CREATE TABLE decision_memory (
    decision_id        TEXT PRIMARY KEY,              -- app-generated uuid
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    analyst_id         TEXT NOT NULL,                 -- forwarded identity (email)
    funding_arrangement TEXT,                         -- FI / ASO / level_funded / specialty / stop_loss / disability
    lob                TEXT,
    group_id           TEXT,
    group_size         INT,
    group_size_band    TEXT,                          -- <100 / 100-999 / 1000-4999 / 5000+
    industry           TEXT,
    scenario_chosen    TEXT,                          -- standard / competitive / retention / custom
    parameters_json    JSONB NOT NULL,                -- the levers (rate chg, trend, credibility, thresholds…)
    projected_json     JSONB NOT NULL,                -- margin / MLR / retention / premium at decision time
    projected_margin   DOUBLE PRECISION,
    projected_mlr      DOUBLE PRECISION,
    dollar_impact      DOUBLE PRECISION,              -- modeled $ swing vs standard
    rationale          TEXT,                          -- analyst's stated reasoning
    twin_verdict       TEXT,                          -- GREEN / AMBER / RED at decision time
    analyst_response   TEXT,                          -- accepted / overrode / adjusted (continuous learning)
    status             TEXT DEFAULT 'candidate'       -- candidate / committed / sold / implemented
);
CREATE INDEX idx_dm_analyst  ON decision_memory (analyst_id, created_at DESC);
CREATE INDEX idx_dm_cohort   ON decision_memory (funding_arrangement, lob, group_size_band);

-- Vectorized context for semantic recall (1:1 with decision_memory).
CREATE TABLE decision_embeddings (
    decision_id  TEXT PRIMARY KEY REFERENCES decision_memory(decision_id) ON DELETE CASCADE,
    embedding    vector(1024) NOT NULL,              -- embed(rationale + cohort + levers)
    embed_text   TEXT
);
CREATE INDEX idx_de_vec ON decision_embeddings USING hnsw (embedding vector_cosine_ops);

-- Learned per-analyst profile (the "twin" personalization).
CREATE TABLE analyst_profile (
    analyst_id        TEXT PRIMARY KEY,
    risk_appetite     TEXT,                           -- conservative / balanced / aggressive (learned)
    typical_margin    DOUBLE PRECISION,
    typical_overrides JSONB,                          -- patterns (e.g., leans on experience > manual)
    decision_count    INT DEFAULT 0,
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- Organizational conscience — actuary-OWNED and VERSIONED (not learned).
CREATE TABLE org_guardrails (
    guardrail_id   TEXT PRIMARY KEY,
    version        INT NOT NULL,
    rule_type      TEXT NOT NULL,                     -- margin_floor / mlr_ceiling / authority_limit / prohibited
    scope_json     JSONB,                             -- applies-to (lob, funding, size band)
    threshold      DOUBLE PRECISION,
    severity       TEXT,                              -- block / warn
    active         BOOLEAN DEFAULT TRUE,
    approved_by    TEXT,
    approved_at    TIMESTAMPTZ DEFAULT now()
);

-- Closes the loop: quoted vs actual at renewal.
CREATE TABLE outcome_feedback (
    decision_id   TEXT REFERENCES decision_memory(decision_id) ON DELETE CASCADE,
    observed_at   TIMESTAMPTZ DEFAULT now(),
    actual_mlr    DOUBLE PRECISION,
    actual_margin DOUBLE PRECISION,
    retained      BOOLEAN,
    note          TEXT,
    PRIMARY KEY (decision_id, observed_at)
);
```

### 4.3 Recall (hybrid semantic + structured)

```sql
-- Top-K similar past decisions, filtered to the relevant cohort, outcome-weighted.
SELECT dm.*, oe.actual_mlr, oe.actual_margin, oe.retained,
       1 - (de.embedding <=> :q_embedding) AS similarity
FROM decision_embeddings de
JOIN decision_memory dm USING (decision_id)
LEFT JOIN outcome_feedback oe USING (decision_id)
WHERE dm.funding_arrangement = :funding
  AND (:lob IS NULL OR dm.lob = :lob)
  AND dm.group_size_band = :size_band
ORDER BY de.embedding <=> :q_embedding      -- cosine distance (pgvector)
LIMIT 8;
```
Plus a second structured pull of **this analyst's** recent calls (`idx_dm_analyst`) and the active
`org_guardrails` for the cohort.

### 4.4 The pre-implementation check loop

New endpoint `POST /api/policy/preflight-check` (input: the proposed policy/quote params + analyst).
Steps:
1. **Embed** the proposed decision's context; **recall** (4.3) similar precedent + analyst history +
   guardrails.
2. **Simulate** financial impact with the existing `simulation_engine` (margin / MLR / retention /
   block $). This is the "millions" number.
3. **Evaluate guardrails deterministically** — margin_floor / mlr_ceiling / authority_limit /
   prohibited → hard `block` or `warn` (versioned, actuary-owned; NOT LLM-decided).
4. **Critique via the agent (grounded)** — the LLM compares against precedent and the analyst's own
   history and must **cite the specific `decision_memory` rows** it reasons from (no citation → no
   claim). Surfaces *dissent and uncertainty*, not just agreement.
5. **Verdict** — `GREEN / AMBER / RED` with: the dollar impact, the cited precedents (incl. their
   *actual* outcomes), guardrail results, required approval tier, and confidence. Analyst reviews.
6. **Write-back** — persist the candidate decision + the analyst's response (accepted / overrode /
   adjusted). Update `analyst_profile`. Memory grows → the twin tracks the team "in time."

```
preflight_check(policy):
    ctx       = build_context(policy)                 # cohort + levers + rationale
    q_emb     = embed(ctx)
    precedent = recall_semantic(q_emb, policy.cohort) + recall_analyst(policy.analyst)
    guardrails= eval_guardrails(policy, active_guardrails(policy.cohort))   # deterministic
    impact    = simulation_engine.run(...)            # $ margin / MLR / retention
    critique  = agent.critique(policy, precedent, guardrails, impact)       # grounded, cites rows
    verdict   = max_severity(guardrails, critique)    # GREEN/AMBER/RED
    record_candidate(policy, verdict)                 # Lakebase write
    return { verdict, impact, cited_precedents, guardrail_results,
             required_approval_tier, confidence, dissent }
```

### 4.5 Governance, NAIC, and the failure modes to design against
- **Delta snapshot** of `decision_memory` + verdicts = the auditable evidence that a human-oversight
  control existed (NAIC AI Bulletin alignment). Memory is PHI-adjacent → same UC governance.
- **Automation bias / anchoring** → the twin surfaces dissent + evidence, never a bare verdict.
- **Precedent lock-in** (faithfully reproducing past mistakes) → guardrails are actuary-owned &
  versioned, not learned; flag when reasoning is purely from precedent; periodic memory review.
- **Hallucinated recall** → every recall cites `decision_memory` rows; ungrounded claims suppressed.
- **Memory poisoning / drift** → outcome-weighted memory (known-bad actuals flagged/down-weighted).

### 4.6 Demo money-shot
Analyst about to approve an aggressive rate → twin recalls *"you priced a similar 3,000-life
manufacturing group at this level last quarter — it ran 94% MLR and lost $1.8M,"* simulates this one
at a $Z margin erosion, returns **RED**, analyst adjusts on stage. Millions saved, live.

---

## Beyond the roadmap
**RUAH portfolio optimization (MILP)** — optimize the whole block (margin × retention × competitive ×
regulatory) rather than one quote. Research/roadmap framing; not demo-near.

## Cross-cutting
- **Reuse `app-prior-auth`** for approval routing/authority matrix/audit (same repo, proven).
- **Reuse the medallion** (gold layer) for the factor-derivation pipeline.
- **NAIC framing** threads through every phase; the Phase 4 twin is itself a governance control.

## Open decisions
1. **Format** — this plan lives as repo Markdown; a Google Doc copy can be minted for circulation.
2. **Twin scope** — per-analyst twin vs team/institutional memory (the schema supports both; default
   is both layers).
3. **pgvector vs Vector Search** for recall — decide at Lakebase provisioning (4.1 fallback).
4. **Branch** — build on a fresh `feature/uw-vision-roadmap` branch off `main`, not the
   Lakehouse-writeback branch.
