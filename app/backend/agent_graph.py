"""Care Intelligence Supervisor Agent for Red Bricks Insurance.

Architecture:
  Multi-agent supervisor pattern: a planning LLM call routes the question
  to one or more specialist sub-agents (Clinical, Financial, Care Management,
  Document). Each specialist gathers domain-specific data and synthesizes
  a specialist response. The supervisor then merges all specialist outputs
  into a single, coherent answer. Three LLM calls for multi-agent, two for
  single-agent routing.

Uses ChatDatabricks (Foundation Model API) for LLM calls.
MLflow tracing decorators capture the full span tree for observability.
"""

import json
import os
import uuid
import concurrent.futures

try:
    import mlflow
    _trace = mlflow.trace
except ImportError:
    def _trace(*args, **kwargs):
        if args and callable(args[0]):
            return args[0]
        def decorator(fn):
            return fn
        return decorator

from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage

from .agents import AGENTS
from .env_config import LLM_ENDPOINT


def _get_llm(temperature: float = 0.1):
    return ChatDatabricks(
        endpoint=LLM_ENDPOINT,
        temperature=temperature,
        max_tokens=4000,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1: Route — classify question and select specialist agents
# ═══════════════════════════════════════════════════════════════════════════════

ROUTER_PROMPT = """You are a routing supervisor for a healthcare care management AI system.
Classify this care manager question and select which specialist agent(s) should handle it.
Respond with ONLY a JSON object (no markdown), like: {{"agents": ["clinical"], "category": "clinical"}}

Available agents:
- clinical: Labs, diagnoses, medications, assessments, health status, clinical trends
- financial: Claims, costs, spend, denials, billing, PMPM, utilization
- care_management: Care gaps, programs, SDOH, transitions of care, interventions, next steps
- document: Case notes, call transcripts, documentation search

Routing rules:
- Simple domain question → single agent (e.g., "What are the latest labs?" → ["clinical"])
- Cross-domain question → multiple agents (e.g., "Give me a full summary" → ["clinical", "financial", "care_management"])
- "Tell me about" / general summary → ["clinical", "care_management"]
- Intervention / action plan → ["care_management"] (it includes recommendation tools)
- Cost + clinical → ["clinical", "financial"]
- Select 1-3 agents. Prefer fewer when the question is focused.

Question: {question}"""


@_trace(name="route", span_type="CHAIN")
def _route(question: str) -> tuple[str, list[str]]:
    """Classify the question and return which specialist agents to invoke."""
    llm = _get_llm(temperature=0.0)
    response = llm.invoke([HumanMessage(content=ROUTER_PROMPT.format(question=question))])
    content = response.content.strip()

    # Parse JSON response
    try:
        # Strip markdown fences if present
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3].strip()
        parsed = json.loads(content)
        agents = parsed.get("agents", ["clinical", "care_management"])
        category = parsed.get("category", agents[0] if agents else "comprehensive")
    except (json.JSONDecodeError, KeyError):
        # Fallback: try to match known categories from raw text
        content_lower = content.lower()
        agents = []
        for key in AGENTS:
            if key in content_lower:
                agents.append(key)
        if not agents:
            agents = ["clinical", "care_management"]
        category = agents[0]

    # Validate agent names
    agents = [a for a in agents if a in AGENTS]
    if not agents:
        agents = ["clinical", "care_management"]

    print(f"[supervisor] Routed to agents: {agents} (category: {category})")
    return category, agents


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2: Dispatch — run specialist agents (parallel if multiple)
# ═══════════════════════════════════════════════════════════════════════════════

@_trace(name="dispatch", span_type="CHAIN")
def _dispatch(member_id: str, question: str, agent_names: list[str]) -> list[dict]:
    """Run specialist agents, in parallel if multiple."""
    if len(agent_names) == 1:
        agent_cls = AGENTS[agent_names[0]]
        return [agent_cls.run(member_id, question)]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(AGENTS[name].run, member_id, question): name
            for name in agent_names
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                agent_name = futures[future]
                results.append({"agent": agent_name, "answer": f"Error: {e}", "tools_used": []})
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Step 3: Merge — supervisor combines specialist responses
# ═══════════════════════════════════════════════════════════════════════════════

MERGE_PROMPT = """You are the Care Intelligence Supervisor for Red Bricks Insurance.
A care manager asked: "{question}"

You dispatched this to {agent_count} specialist agent(s) who each analyzed different
aspects of member {member_id}. Below are their responses.

{specialist_responses}

{history_section}

**Your task:** Merge these specialist analyses into a single, coherent response.
- Lead with the most critical findings across all domains
- Organize by clinical priority, not by agent
- Deduplicate overlapping information
- Cite specific data: lab values, dates, scores, dollar amounts
- End with a prioritized action plan combining all recommendations
- Use markdown formatting (headers, bullets, bold for emphasis)
- If only one specialist responded, refine and improve their response rather than repeating it verbatim"""


@_trace(name="merge", span_type="CHAIN")
def _merge(
    member_id: str,
    question: str,
    specialist_results: list[dict],
    conversation_history: list[dict] | None = None,
) -> str:
    """Merge specialist responses into a unified answer."""
    # If single agent, skip the merge LLM call and return directly
    if len(specialist_results) == 1:
        return specialist_results[0]["answer"]

    # Format specialist responses
    agent_labels = {
        "clinical": "Clinical Specialist",
        "financial": "Financial Specialist",
        "care_management": "Care Management Specialist",
        "document": "Document Analysis Specialist",
    }

    sections = []
    for result in specialist_results:
        label = agent_labels.get(result["agent"], result["agent"])
        sections.append(f"### {label}\n{result['answer']}")

    # Build conversation history section
    history_section = ""
    if conversation_history:
        lines = []
        for msg in conversation_history[-6:]:
            role_label = "Care Manager" if msg["role"] == "human" else "Agent"
            lines.append(f"**{role_label}**: {msg['content'][:500]}")
        history_section = (
            "\n**Previous conversation context:**\n"
            + "\n".join(lines)
            + "\n\nMaintain continuity with prior context.\n"
        )

    llm = _get_llm()
    prompt = MERGE_PROMPT.format(
        question=question,
        member_id=member_id,
        agent_count=len(specialist_results),
        specialist_responses="\n\n".join(sections),
        history_section=history_section,
    )
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_specialists(specialist_results: list[dict]) -> list[str]:
    return [r["agent"] for r in specialist_results]


@_trace(name="care_intelligence_agent", span_type="AGENT")
def query_supervisor_agent(
    member_id: str,
    question: str,
    conversation_id: str | None = None,
    conversation_history: list[dict] | None = None,
) -> dict:
    """Route → Dispatch → Merge multi-agent pipeline."""
    thread_id = conversation_id or str(uuid.uuid4())

    try:
        # Step 1: Route to specialist agents
        category, agent_names = _route(question)

        # Step 2: Dispatch to specialists (parallel execution)
        specialist_results = _dispatch(member_id, question, agent_names)

        # Step 3: Merge specialist responses
        answer = _merge(member_id, question, specialist_results, conversation_history)

        return {
            "answer": answer,
            "sources": [],
            "conversation_id": thread_id,
            "specialists_consulted": _detect_specialists(specialist_results),
            "category": category,
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "answer": f"I encountered an error processing your request: {e}",
            "sources": [],
            "conversation_id": thread_id,
            "specialists_consulted": [],
        }


async def stream_supervisor_agent(
    member_id: str,
    question: str,
    conversation_id: str | None = None,
    conversation_history: list[dict] | None = None,
):
    """Async generator that yields SSE events as the supervisor runs."""
    thread_id = conversation_id or str(uuid.uuid4())

    try:
        yield {"event": "start", "data": json.dumps({"conversation_id": thread_id})}

        # Step 1: Route
        category, agent_names = _route(question)
        yield {"event": "routing", "data": json.dumps({
            "category": category,
            "agents": agent_names,
        })}

        # Step 2: Dispatch to specialists
        for name in agent_names:
            yield {"event": "agent_start", "data": json.dumps({"agent": name})}
        specialist_results = _dispatch(member_id, question, agent_names)
        for result in specialist_results:
            yield {"event": "agent_done", "data": json.dumps({"agent": result["agent"]})}

        # Step 3: Merge
        answer = _merge(member_id, question, specialist_results, conversation_history)
        yield {"event": "token", "data": json.dumps({"content": answer})}
        yield {"event": "done", "data": json.dumps({"conversation_id": thread_id})}

    except Exception as e:
        yield {"event": "error", "data": json.dumps({"error": str(e)})}
