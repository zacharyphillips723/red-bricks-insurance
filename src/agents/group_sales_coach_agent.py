"""Group Sales Coach Agent — MLflow ChatModel for Unity Catalog.

Sales strategy coach for account executives preparing for employer group
renewal meetings. Retrieves group report card, experience, stop-loss, renewal,
and TCOC data, then generates structured briefings via Foundation Model API.

Input format: "[GRP-XXXX] <question>" (same bracket convention as [MBR-XXXX])
"""

import json
import os
from typing import List, Optional

import mlflow
from mlflow.pyfunc import ChatModel
from mlflow.types.llm import (
    ChatMessage,
    ChatParams,
    ChatCompletionResponse,
    ChatChoice,
)


class GroupSalesCoachAgent(ChatModel):
    """Sales strategy coach for employer group renewal preparation."""

    SYSTEM_PROMPT = (
        "You are a Sales Strategy Coach for Red Bricks Insurance. "
        "You help account executives and sales reps prepare for employer group "
        "renewal meetings by synthesizing financial, utilization, stop-loss, "
        "and cost-of-care data into actionable briefings.\n\n"
        "You will be given the group's report card data including claims experience, "
        "peer benchmarks, stop-loss exposure, renewal projections, and cost tier "
        "distributions. Synthesize this into a STRUCTURED response.\n\n"
        "Your response MUST include the following sections:\n\n"
        "## Group Snapshot\n"
        "Brief overview: group name, size, industry, funding type, health score, "
        "and key financial metrics (PMPM, loss ratio, renewal action).\n\n"
        "## Talking Points\n"
        "3-5 data-backed talking points the AE should lead with. Include specific "
        "numbers and peer comparisons. Frame positives as value stories, frame "
        "negatives as opportunities for partnership.\n\n"
        "## Risk Areas\n"
        "Identify the top 2-3 risk areas: high-cost claimants, stop-loss exposure, "
        "rising utilization, unfavorable loss ratio, etc. Include specific numbers.\n\n"
        "## Competitive Position\n"
        "Based on peer percentile ranks, position the group relative to similar "
        "employers. Highlight where Red Bricks delivers above-market value.\n\n"
        "## Objection Handling\n"
        "Anticipate 2-3 likely objections (rate increase, high costs, competitor "
        "quotes) and provide data-backed responses.\n\n"
        "## Care Management & Plan Recommendations\n"
        "Based on the group's risk profile, utilization patterns, and cost drivers, "
        "recommend specific Red Bricks programs and plan design options the AE can "
        "offer during the renewal conversation. Draw from these available programs:\n"
        "- **Complex Case Management**: Nurse-led coordination for members with 2+ "
        "chronic conditions or annual claims >$50K. Typical savings: 15-25% on "
        "targeted members.\n"
        "- **Centers of Excellence (COE)**: Steerage to high-quality, lower-cost "
        "facilities for joint replacement, cardiac, bariatric, and spinal surgery. "
        "Bundled pricing with 90-day warranty.\n"
        "- **Diabetes Prevention Program (DPP)**: CDC-recognized lifestyle change "
        "program for pre-diabetic members. Reduces progression to T2DM by 58%.\n"
        "- **Behavioral Health EAP+**: Enhanced EAP with 12 sessions (vs standard 6), "
        "virtual therapy access, and substance use disorder navigation.\n"
        "- **Pharmacy Benefit Optimization**: Formulary tier review, biosimilar "
        "conversion, specialty drug prior auth, and mail-order incentives.\n"
        "- **Musculoskeletal (MSK) Program**: Digital-first PT and coaching for back "
        "pain, joint issues. Reduces unnecessary MRIs and surgeries by 30-40%.\n"
        "- **Maternity Management**: Risk-stratified prenatal care coordination, "
        "NICU avoidance, and postpartum support.\n"
        "- **Onsite/Nearsite Clinic**: For groups 200+ lives, primary care and "
        "preventive services at or near the employer's location.\n"
        "- **Plan Design Levers**: HDHP with HSA seeding, narrow/tiered networks, "
        "reference-based pricing, spousal surcharges, tobacco cessation incentives.\n\n"
        "Match recommendations to the group's specific cost drivers. If ER utilization "
        "is high, recommend MSK + urgent care steering. If pharmacy costs are rising, "
        "recommend formulary optimization. If high-cost claimants dominate, recommend "
        "Complex Case Management + COE.\n\n"
        "Always cite specific data points. Never fabricate numbers."
    )

    QUIZ_SYSTEM_PROMPT = (
        "You are a Sales Strategy Coach for Red Bricks Insurance. "
        "The account executive wants to practice for their renewal meeting. "
        "You will be given the group's data.\n\n"
        "ROLEPLAY MODE: Simulate a realistic renewal negotiation by playing the "
        "role of the employer's benefits director, CFO, or HR VP. Stay in character "
        "and respond as the employer representative would — pushing back on rate "
        "increases, asking for concessions, citing competitor quotes, questioning "
        "claim trends, and demanding value-adds.\n\n"
        "When the AE responds, briefly break character to provide coaching feedback:\n"
        "- Rate their response (Strong / Needs Work / Missed Opportunity)\n"
        "- Suggest a stronger data-backed answer using the group's actual numbers\n"
        "- Highlight any Red Bricks care management programs or plan design changes "
        "they could have offered (Complex Case Management, COE, DPP, Behavioral "
        "Health EAP+, Pharmacy Optimization, MSK Program, Maternity Management, "
        "Plan Design Levers)\n"
        "- Then get back in character with the next challenge question\n\n"
        "Common employer negotiation tactics to simulate:\n"
        "- 'We got a quote from [competitor] that's 12% lower'\n"
        "- 'Why should we pay more when our employees aren't using the plan?'\n"
        "- 'We need a rate cap — no more than 5% increase'\n"
        "- 'What are you doing about our high-cost claimants?'\n"
        "- 'Our employees are complaining about out-of-pocket costs'\n"
        "- 'We're considering going self-funded / moving to a PEO'\n"
        "- 'What value-adds can you offer to offset the increase?'\n\n"
        "Be supportive but rigorous. The goal is to build the AE's confidence "
        "and ensure they can handle tough conversations with real data."
    )

    def load_context(self, context) -> None:
        """Initialize SDK clients at model load time."""
        from databricks.sdk import WorkspaceClient

        self.w = WorkspaceClient()

        self.catalog = os.environ.get("UC_CATALOG", "red_bricks_insurance")
        self.warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "781064a3466c0984")
        self.llm_endpoint = os.environ.get(
            "LLM_ENDPOINT", "databricks-llama-4-maverick"
        )
        self.report_card_table = f"{self.catalog}.analytics.gold_group_report_card"
        self.experience_table = f"{self.catalog}.analytics.gold_group_experience"
        self.stop_loss_table = f"{self.catalog}.analytics.gold_group_stop_loss"
        self.renewal_table = f"{self.catalog}.analytics.gold_group_renewal"
        self.tcoc_table = f"{self.catalog}.analytics.gold_member_tcoc"

    def predict(
        self, context, messages: List[ChatMessage], params: Optional[ChatParams] = None
    ) -> ChatCompletionResponse:
        """Process a sales coach query using group data retrieval."""
        user_msg = ""
        group_id = ""
        for m in reversed(messages):
            if m.role == "user":
                user_msg = m.content
                break

        # Extract group_id from "[GRP-XXXX] question" format
        if user_msg.startswith("[") and "]" in user_msg:
            bracket_end = user_msg.index("]")
            group_id = user_msg[1:bracket_end].strip()
            question = user_msg[bracket_end + 1:].strip()
        else:
            question = user_msg

        # Determine intent
        intent = self._classify_intent(question)

        # Retrieve data based on intent
        report_card = self._get_report_card(group_id) if group_id else {}
        context_sections = [f"## Group Report Card\n{json.dumps(report_card, indent=2, default=str)}"]

        if intent in ("full_briefing", "renewal_focus"):
            experience = self._get_experience_detail(group_id) if group_id else {}
            context_sections.append(f"## Claims Experience Detail\n{json.dumps(experience, indent=2, default=str)}")

            stop_loss = self._get_stop_loss_detail(group_id) if group_id else {}
            context_sections.append(f"## Stop-Loss Detail\n{json.dumps(stop_loss, indent=2, default=str)}")

            renewal = self._get_renewal_detail(group_id) if group_id else {}
            context_sections.append(f"## Renewal Detail\n{json.dumps(renewal, indent=2, default=str)}")

        if intent in ("full_briefing", "cost_focus"):
            tcoc = self._get_group_tcoc(group_id) if group_id else []
            context_sections.append(f"## Member Cost Tier Distribution\n{json.dumps(tcoc, indent=2, default=str)}")

        if intent in ("full_briefing", "peer_comparison"):
            peer = self._get_peer_comparison(group_id) if group_id else {}
            context_sections.append(f"## Peer Comparison\n{json.dumps(peer, indent=2, default=str)}")

        augmented_prompt = f"Question: {question}\n\n" + "\n\n".join(context_sections)

        system_prompt = self.QUIZ_SYSTEM_PROMPT if intent == "quiz" else self.SYSTEM_PROMPT

        llm_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": augmented_prompt},
        ]

        answer = self._call_llm(llm_messages)

        return ChatCompletionResponse(
            choices=[ChatChoice(index=0, message=ChatMessage(role="assistant", content=answer))],
            usage={},
            model=self.llm_endpoint,
        )

    def _classify_intent(self, question: str) -> str:
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

    def _execute_sql(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute SQL via Statement Execution API."""
        from databricks.sdk.service.sql import StatementParameterListItem

        try:
            kwargs = {
                "warehouse_id": self.warehouse_id,
                "statement": sql,
                "wait_timeout": "30s",
            }
            if params:
                kwargs["parameters"] = [
                    StatementParameterListItem(name=p["name"], value=p["value"], type=p.get("type", "STRING"))
                    for p in params
                ]
            stmt = self.w.statement_execution.execute_statement(**kwargs)
            if not stmt.result or not stmt.result.data_array:
                return []
            col_names = [c.name for c in stmt.manifest.schema.columns]
            return [dict(zip(col_names, row)) for row in stmt.result.data_array]
        except Exception as e:
            print(f"[SalesCoach] SQL error: {e}")
            return []

    def _get_report_card(self, group_id: str) -> dict:
        """Full report card — single row per group."""
        rows = self._execute_sql(
            f"SELECT * FROM {self.report_card_table} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else {}

    def _get_experience_detail(self, group_id: str) -> dict:
        """Claims experience breakdown."""
        rows = self._execute_sql(
            f"SELECT * FROM {self.experience_table} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else {}

    def _get_stop_loss_detail(self, group_id: str) -> dict:
        """Stop-loss / reinsurance exposure."""
        rows = self._execute_sql(
            f"SELECT * FROM {self.stop_loss_table} WHERE group_id = :gid ORDER BY claim_year DESC LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else {}

    def _get_renewal_detail(self, group_id: str) -> dict:
        """Renewal pricing projections."""
        rows = self._execute_sql(
            f"SELECT * FROM {self.renewal_table} WHERE group_id = :gid LIMIT 1",
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else {}

    def _get_group_tcoc(self, group_id: str) -> list[dict]:
        """Member cost tier distribution for this group."""
        return self._execute_sql(
            f"""
            SELECT
              t.cost_tier,
              COUNT(DISTINCT t.member_id) AS member_count,
              ROUND(AVG(t.tcoc), 2) AS avg_tcoc,
              ROUND(AVG(t.tci), 3) AS avg_tci,
              ROUND(SUM(t.total_paid), 2) AS total_paid
            FROM {self.tcoc_table} t
            INNER JOIN {self.catalog}.members.silver_enrollment e
              ON t.member_id = e.member_id
            WHERE e.group_number = :gid
            GROUP BY t.cost_tier
            ORDER BY avg_tcoc DESC
            """,
            [{"name": "gid", "value": group_id}],
        )

    def _get_peer_comparison(self, group_id: str) -> dict:
        """Percentile context from report card."""
        rows = self._execute_sql(
            f"""
            SELECT
              group_id, group_name, industry, group_size_tier,
              claims_pmpm_pctl, loss_ratio_pctl, er_visits_pctl, tci_pctl,
              group_health_score
            FROM {self.report_card_table}
            WHERE group_id = :gid
            LIMIT 1
            """,
            [{"name": "gid", "value": group_id}],
        )
        return rows[0] if rows else {}

    def _call_llm(self, messages: list[dict]) -> str:
        """Call Foundation Model API for chat completion."""
        try:
            data = self.w.api_client.do(
                "POST",
                f"/serving-endpoints/{self.llm_endpoint}/invocations",
                body={"messages": messages, "max_tokens": 2500, "temperature": 0.1},
            )
            return (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "No response generated.")
            )
        except Exception as e:
            return f"Error generating response: {e}"


# Required for MLflow code-based logging
mlflow.models.set_model(GroupSalesCoachAgent())
