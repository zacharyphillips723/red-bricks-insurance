# Phase 0 — Foundations: HCC grouper + coherent-data design

Status: **in progress** (started 2026-10-22). See DESIGN.md §19 for the full phased plan and §10.1 for the coherence problem this phase exists to fix.

## Goal
Remove the data-coherence blocker at the root and establish the single deterministic grouping truth that both the data-generation (Phase 1) and the agent's grouper tool (Phase 3) use.

## What this phase delivers
1. **`reference.cms_hcc_v28_grouper`** — a curated **demo-subset** ICD-10 → HCC → RAF table.
   - Built by `src/notebooks/setup_hcc_grouper.py`.
   - Aligned to the exact diagnoses the note generator emits
     (`src/data_generation/domains/documents.py :: PRESENTING_COMPLAINTS`):
     `E11.9, E11.65, I10, I50.9, J44.1, N18.3` — plus adjacent codes
     (E11.21/22/40/42, I50.22/32, J44.0/9, N18.4/5/6, I48.0/91, I21.4, I20.0).
   - **`I10` (hypertension) → no HCC (`is_hcc = false`)**: the deliberate "do-not-suspect"
     negative that keeps suspecting honest and feeds the over-coding metric (§15.1).
   - Labeled a demo subset, RAF values illustrative — **not** the full CMS grouper or
     official V28 coefficients (§16.5).
2. **The coherence spec for Phase 1** (below).

## Grouper mapping (as built)
| ICD-10 | HCC | RAF | Note |
|--------|-----|-----|------|
| E11.9 | HCC19 | 0.2 | diabetes, no complication |
| E11.65 / .21 / .22 / .40 / .42 | HCC18 | 0.4 | diabetes w/ chronic complications |
| I50.9 / .22 / .32 | HCC85 | 0.5 | CHF |
| J44.1 / .0 / .9 | HCC111 | 0.4 | COPD |
| N18.3 | HCC135 | 0.2 | CKD stage 3 |
| N18.4 / .5 / .6 | HCC134 | 0.6 | CKD stage 4-5 / ESRD |
| I48.0 / .91 | HCC96 | 0.2 | atrial fibrillation |
| I21.4 / I20.0 | HCC138 | 0.5 | NSTEMI / unstable angina |
| **I10** | **—** | **0.0** | **hypertension — NOT risk-adjusted (negative control)** |

## Coherence spec (carried into Phase 1)
Today HCCs are assigned **randomly** (`domains/risk_adjustment.py`) and are unrelated to a
member's notes. Phase 1 replaces that with a condition-driven flow so the story is consistent:

1. **Pick a condition set per member** (0–3 chronic conditions, weighted as today).
2. **Generate the member's notes from those conditions** — drive the note generator's
   `primary_dx` from the member's condition set (not an unrelated random dx).
3. **Derive HCCs from those conditions via the grouper** (`cms_hcc_v28_grouper`), not randomly.
4. **Inject the demo states** into `risk_adjustment.hcc_evidence`:
   - `documented` — HCC + clear supporting evidence in a note (baseline).
   - `suspected` — condition present and evidenced in a note but **not yet coded** → the
     suspecting agent's target (revenue-capture moment).
   - `weak` — HCC coded but evidence thin/inferential (e.g., CHF implied by "SOB on exertion"
     with no EF) → the RADV-defensibility target.
   - plus `I10`-style noise: hypertension documented but correctly **not** an HCC.

## Exit gate for Phase 0 — ✅ MET (2026-10-22)
- [x] Grouper setup notebook created (`setup_hcc_grouper.py`).
- [x] **Run on the workspace** → `red_bricks_insurance_catalog.reference.cms_hcc_v28_grouper` exists and verifies (21 ICD-10 rows → 8 HCCs; 1 non-HCC negative `I10`). Profile `fe-vm-red-bricks-aws`, Serverless Starter Warehouse.
- [x] Every ICD-10 in the note templates maps to a correct HCC or to `is_hcc=false`.
- [x] Clearly labeled "demo subset, not full CMS grouper."

**Phase 0 complete.** Next: Phase 1 (coherent data + `hcc_evidence` linking) — additive/reversible enrichment; see DESIGN.md §19 Phase 1.
