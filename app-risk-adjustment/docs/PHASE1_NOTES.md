# Phase 1 — Coherent HCC evidence layer

Status: **core built & verified on the workspace** (2026-10-22). See DESIGN.md §19 Phase 1.

## Approach (additive / reversible — chosen over regenerating the core data)
Instead of overwriting the (random) coded HCCs, we build an **evidence layer from the case
notes** and classify each (member, HCC) by the gap between note evidence and the coded set.
This is more realistic than making them identical: the gap *is* the RA workflow.

- `risk_adjustment.member_note_dx` — deterministic note-text → ICD-10 (`setup_ra_coherent_data.py` step 1).
- `risk_adjustment.hcc_evidence` — (member_id, hcc_code, hcc_label, raf_value, document_id, note_hits, **evidence_state**).
- `risk_adjustment.v_member_hcc_coherent` — per-member documented/suspected/weak HCCs + `potential_raf_gain` + `raf_at_radv_risk`.

Nothing existing was modified. To reverse: `DROP` the two tables + one view.

## Evidence states (verified counts)
| state | rows | members | demo role |
|-------|-----:|--------:|-----------|
| `weak` | 4,019 | 2,789 | coded, no note evidence → **RADV defect risk** |
| `suspected` | 3,547 | 3,547 | note-evidenced, not coded → **capture opportunity (RAF gain)** |
| `non_hcc_noise` | 403 | 403 | hypertension documented, correctly not an HCC → do-not-suspect control |
| `documented` | 369 | 369 | evidenced + coded → baseline |

## Coherence check (exit gate) — ✅
Member **MBR105480**: notes mention diabetes (`E11.9`) → **HCC19 suspected** (2 note hits, uncoded);
coded HCC85 (CHF) / HCC135 (CKD) / HCC18 (diabetes-complicated) are **weak** (coded, no note evidence).
Note text ↔ detected ICD-10 ↔ evidence states all agree, and both a suspected (capture) and a
weak (RADV) HCC exist for the member — the full workflow in one case.

## Notes / decisions
- Coherence is anchored on **case notes** (what the agent reads), not `clinical.silver_conditions`.
- `documented` is the smallest bucket because the pre-existing coded HCCs were randomly assigned,
  so they rarely coincide with note evidence — which is fine; `suspected` + `weak` are the demo stars.
  If a richer `documented` baseline is wanted later, seed a subset of coded HCCs from note evidence.
- Canonical artifact: `src/notebooks/setup_ra_coherent_data.py` (run live via the warehouse this round).

## Phase 1 polish — ✅ DONE (2026-10-22)
- Rebuilt at **chunk grain**: `member_note_dx` now detects dx from `documents.silver_case_notes_chunks`
  and `hcc_evidence` carries **`evidence_chunk_id`** → passage-level citation.
- Verified: a `suspected` HCC19 row cites `DOC108593_chunk_000` =
  *"PRESENTING COMPLAINT: Follow-up for uncontrolled type 2 diabetes mellitus"*. State counts unchanged.

## Deferred to Phase 6 wiring
- Wire `setup_hcc_grouper` + `setup_ra_coherent_data` into `full_demo_job.yml` (DAB).

Next: **Phase 2** — Lakebase memory tiers + case state + `agent_policy`.
