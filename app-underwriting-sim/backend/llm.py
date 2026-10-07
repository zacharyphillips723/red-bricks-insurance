"""Thin Foundation Model API helper for structured extraction and NL parsing.

Phase 3 of the underwriting vision roadmap. Document intelligence, NL quote intake,
and the negotiation re-rate loop all need the same primitive: send a prompt to a
serving endpoint and get back text or a parsed JSON object. This centralizes that
call (mirroring the pattern in agent.py) so the feature modules stay small.
"""

import json
import re
from typing import Optional

from databricks.sdk import WorkspaceClient

from .env_config import LLM_ENDPOINT


def _invoke(messages: list[dict], *, max_tokens: int = 1500, temperature: float = 0.0) -> str:
    """Call the chat serving endpoint and return the assistant text content."""
    w = WorkspaceClient()
    data = w.api_client.do(
        "POST",
        f"/serving-endpoints/{LLM_ENDPOINT}/invocations",
        body={"messages": messages, "max_tokens": max_tokens, "temperature": temperature},
    )
    choices = data.get("choices", [{}]) if isinstance(data, dict) else [{}]
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, list):  # some endpoints return typed blocks
        content = "\n".join(
            b.get("text", "") for b in content if isinstance(b, dict)
        )
    return content or ""


def complete_text(system: str, user: str, *, max_tokens: int = 1200) -> str:
    return _invoke(
        [{"role": "system", "content": system}, {"role": "user", "content": user}],
        max_tokens=max_tokens,
    )


def complete_json(system: str, user: str, *, max_tokens: int = 1500) -> dict:
    """Prompt for JSON and parse the first JSON object out of the response.

    Returns {} with an "_error" key if nothing parseable comes back, so callers can
    degrade gracefully rather than raise.
    """
    system = system + "\n\nReturn ONLY a single valid JSON object. No prose, no code fences."
    raw = _invoke(
        [{"role": "system", "content": system}, {"role": "user", "content": user}],
        max_tokens=max_tokens,
    )
    parsed = _extract_json(raw)
    if parsed is None:
        return {"_error": "could not parse JSON", "_raw": raw[:500]}
    return parsed


def _extract_json(raw: str) -> Optional[dict]:
    if not raw:
        return None
    # Strip code fences if present.
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL)
    candidate = fenced.group(1) if fenced else None
    if candidate is None:
        # Fall back to the first balanced {...} span.
        start = raw.find("{")
        if start < 0:
            return None
        depth = 0
        for i in range(start, len(raw)):
            if raw[i] == "{":
                depth += 1
            elif raw[i] == "}":
                depth -= 1
                if depth == 0:
                    candidate = raw[start:i + 1]
                    break
    if not candidate:
        return None
    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else None
    except (ValueError, TypeError):
        return None
