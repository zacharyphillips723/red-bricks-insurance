"""Group Sales Coach — direct LLM call with group data + enrichment context.

This is the app-level agent that enriches context from external sources (Slack,
Glean, Salesforce) before calling Foundation Model API. The UC-registered MLflow
model stays pure (Databricks data only) for governance; this app layer adds the
full experience.
"""

import json
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.sdk import WorkspaceClient

from .groups import get_report_card, get_experience, get_stop_loss, get_renewal, get_tcoc
from .enrichment import get_slack_context, get_glean_context, get_salesforce_context

LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-llama-4-maverick")

SYSTEM_PROMPT = """You are a Sales Strategy Coach for Red Bricks Insurance.
You help account executives prepare for employer group renewal meetings by
synthesizing financial data, utilization metrics, stop-loss exposure, and
cost-of-care analytics into actionable briefings.

You may also receive context from internal systems (Slack discussions, knowledge
base documents, CRM data). When available, weave this context into your
recommendations naturally.

Your response MUST include the following sections:

## Group Snapshot
Brief overview: group name, size, industry, funding type, health score, and
key financial metrics.

## Talking Points
3-5 data-backed talking points the AE should lead with. Include specific numbers
and peer comparisons.

## Risk Areas
Top 2-3 risk areas with specific numbers.

## Competitive Position
Position the group relative to peers based on percentile ranks.

## Objection Handling
Anticipate 2-3 likely objections with data-backed responses.

## Care Management & Plan Recommendations
Based on the group's risk profile, utilization patterns, and cost drivers,
recommend specific Red Bricks programs and plan design options the AE can
offer during the renewal conversation. Draw from these available programs:
- **Complex Case Management**: Nurse-led coordination for members with 2+
  chronic conditions or annual claims >$50K. Typical savings: 15-25%.
- **Centers of Excellence (COE)**: Steerage to high-quality, lower-cost
  facilities for joint replacement, cardiac, bariatric, and spinal surgery.
  Bundled pricing with 90-day warranty.
- **Diabetes Prevention Program (DPP)**: CDC-recognized lifestyle change
  program for pre-diabetic members. Reduces progression to T2DM by 58%.
- **Behavioral Health EAP+**: Enhanced EAP with 12 sessions (vs standard 6),
  virtual therapy access, and substance use disorder navigation.
- **Pharmacy Benefit Optimization**: Formulary tier review, biosimilar
  conversion, specialty drug prior auth, and mail-order incentives.
- **Musculoskeletal (MSK) Program**: Digital-first PT and coaching for back
  pain, joint issues. Reduces unnecessary MRIs and surgeries by 30-40%.
- **Maternity Management**: Risk-stratified prenatal care coordination,
  NICU avoidance, and postpartum support.
- **Onsite/Nearsite Clinic**: For groups 200+ lives, primary care and
  preventive services at or near the employer's location.
- **Plan Design Levers**: HDHP with HSA seeding, narrow/tiered networks,
  reference-based pricing, spousal surcharges, tobacco cessation incentives.

Match recommendations to the group's specific cost drivers.

Always cite specific data points. Never fabricate numbers."""

QUIZ_SYSTEM_PROMPT = """You are a Sales Strategy Coach for Red Bricks Insurance.
The account executive wants to practice for their renewal meeting. You will be
given the group's data.

ROLEPLAY MODE: Simulate a realistic renewal negotiation by playing the role of
the employer's benefits director, CFO, or HR VP. Stay in character and respond
as the employer representative would — pushing back on rate increases, asking for
concessions, citing competitor quotes, questioning claim trends, and demanding
value-adds.

When the AE responds, briefly break character to provide coaching feedback:
- Rate their response (Strong / Needs Work / Missed Opportunity)
- Suggest a stronger data-backed answer using the group's actual numbers
- Highlight any Red Bricks care management programs or plan design changes they
  could have offered (Complex Case Management, COE, DPP, Behavioral Health EAP+,
  Pharmacy Optimization, MSK Program, Maternity Management, Plan Design Levers)
- Then get back in character with the next challenge question

Common employer negotiation tactics to simulate:
- "We got a quote from [competitor] that's 12% lower"
- "Why should we pay more when our employees aren't using the plan?"
- "We need a rate cap — no more than 5% increase"
- "What are you doing about our high-cost claimants?"
- "Our employees are complaining about out-of-pocket costs"
- "We're considering going self-funded / moving to a PEO"
- "What value-adds can you offer to offset the increase?"

Be supportive but rigorous. The goal is to build the AE's confidence and ensure
they can handle tough conversations with real data."""


def _classify_intent(question: str) -> str:
    """Simple keyword-based intent classification."""
    q = question.lower()
    if any(kw in q for kw in ["prepare me", "briefing", "renewal meeting", "get me ready"]):
        return "full_briefing"
    if any(kw in q for kw in ["rate increase", "why", "pricing", "renewal"]):
        return "renewal_focus"
    if any(kw in q for kw in ["cost", "tcoc", "high cost", "expensive"]):
        return "cost_focus"
    if any(kw in q for kw in ["peer", "benchmark", "compare", "percentile"]):
        return "peer_comparison"
    if any(kw in q for kw in ["quiz", "practice", "test me", "challenge",
                               "roleplay", "role play", "simulate", "negotiate",
                               "negotiation", "play the role", "pretend"]):
        return "quiz"
    if any(kw in q for kw in ["care management", "programs", "what can we offer",
                               "value-add", "value add", "benefits", "plan design"]):
        return "full_briefing"
    return "full_briefing"


def query_sales_coach(group_id: str, question: str) -> dict:
    """Sales coach: retrieve group data + enrichment, then generate briefing."""
    try:
        print(f"[SalesCoach] Processing query for {group_id}: {question[:80]}...")

        intent = _classify_intent(question)

        # Step 1: Retrieve structured data from gold tables
        report_card = get_report_card(group_id) or {}
        context_sections = [f"## Group Report Card\n{json.dumps(report_card, indent=2, default=str)}"]

        if intent in ("full_briefing", "renewal_focus"):
            experience = get_experience(group_id) or {}
            context_sections.append(f"## Claims Experience\n{json.dumps(experience, indent=2, default=str)}")
            stop_loss = get_stop_loss(group_id) or {}
            context_sections.append(f"## Stop-Loss\n{json.dumps(stop_loss, indent=2, default=str)}")
            renewal = get_renewal(group_id) or {}
            context_sections.append(f"## Renewal\n{json.dumps(renewal, indent=2, default=str)}")

        if intent in ("full_briefing", "cost_focus"):
            tcoc = get_tcoc(group_id)
            context_sections.append(f"## Cost Tier Distribution\n{json.dumps(tcoc, indent=2, default=str)}")

        # Step 2: Enrichment layer — call external sources in parallel
        group_name = report_card.get("group_name", "")
        industry = report_card.get("industry", "")
        enrichment_sources = []
        enrichment_context = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(get_slack_context, group_name): "slack",
                executor.submit(get_glean_context, group_name, industry): "glean",
                executor.submit(get_salesforce_context, group_name): "salesforce",
            }
            for future in as_completed(futures, timeout=15):
                source_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        enrichment_sources.append(source_name)
                        enrichment_context.append(
                            f"## Internal Context ({source_name.title()})\n"
                            f"{json.dumps(result, indent=2, default=str)}"
                        )
                except Exception as e:
                    print(f"[SalesCoach] Enrichment {source_name} error: {e}")

        # Step 3: Assemble full prompt
        all_context = "\n\n".join(context_sections + enrichment_context)
        augmented_prompt = f"Question: {question}\n\n{all_context}"

        system_prompt = QUIZ_SYSTEM_PROMPT if intent == "quiz" else SYSTEM_PROMPT

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": augmented_prompt},
        ]

        # Step 4: Call Foundation Model API
        w = WorkspaceClient()
        data = w.api_client.do(
            "POST",
            f"/serving-endpoints/{LLM_ENDPOINT}/invocations",
            body={"messages": messages, "max_tokens": 2500, "temperature": 0.1},
        )
        answer = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "No response generated.")
        )

        print(f"[SalesCoach] Response: {len(answer)} chars, enrichment: {enrichment_sources}")
        return {"answer": answer, "enrichment_sources": enrichment_sources}

    except Exception as e:
        print(f"[SalesCoach] ERROR: {e}")
        traceback.print_exc()
        return {
            "answer": f"I encountered an error processing your request: {str(e)}",
            "enrichment_sources": [],
        }
