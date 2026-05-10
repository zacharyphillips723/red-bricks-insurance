"""Base class for specialist sub-agents."""

import concurrent.futures
from typing import ClassVar

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

from langchain_core.messages import HumanMessage
from databricks_langchain import ChatDatabricks

from ..env_config import LLM_ENDPOINT
from ..agent_tools import _tool_map


def _get_llm(temperature: float = 0.1):
    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=temperature, max_tokens=3000)


class BaseAgent:
    """Base specialist agent with a domain-specific system prompt and tool set."""

    name: ClassVar[str] = "base"
    system_prompt: ClassVar[str] = ""
    tool_names: ClassVar[list[str]] = []

    @classmethod
    @_trace(name="specialist_gather", span_type="CHAIN")
    def gather(cls, member_id: str) -> dict[str, str]:
        """Execute all tools for this specialist in parallel."""
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for tool_name in cls.tool_names:
                tool_fn = _tool_map.get(tool_name)
                if not tool_fn:
                    continue
                if tool_name == "search_case_notes":
                    futures[executor.submit(tool_fn.invoke, {"member_id": member_id, "query": "care history summary"})] = tool_name
                else:
                    futures[executor.submit(tool_fn.invoke, {"member_id": member_id})] = tool_name

            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    results[name] = f"Error: {e}"
        return results

    @classmethod
    @_trace(name="specialist_synthesize", span_type="CHAIN")
    def synthesize(cls, member_id: str, question: str, tool_results: dict[str, str]) -> str:
        """Synthesize gathered data into a specialist response."""
        formatted = []
        for name, result in tool_results.items():
            formatted.append(f"### {name}\n```json\n{result[:3000]}\n```")

        prompt = f"""{cls.system_prompt}

The care manager asked: "{question}"

Below is data for member {member_id}:

{chr(10).join(formatted)}

Provide your specialist analysis:"""

        llm = _get_llm()
        response = llm.invoke([HumanMessage(content=prompt)])
        return response.content

    @classmethod
    def run(cls, member_id: str, question: str) -> dict:
        """Full specialist pipeline: gather → synthesize."""
        tool_results = cls.gather(member_id)
        answer = cls.synthesize(member_id, question, tool_results)
        return {"agent": cls.name, "answer": answer, "tools_used": list(tool_results.keys())}
