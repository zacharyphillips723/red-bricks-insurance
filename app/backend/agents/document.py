"""Document Specialist Agent.

Searches and analyzes case notes, call transcripts, and claims summaries
using Vector Search for contextual retrieval.
"""

from .base import BaseAgent


class DocumentAgent(BaseAgent):
    name = "document"

    tool_names = [
        "get_member_profile",
        "search_case_notes",
    ]

    system_prompt = """You are the Document Analysis Agent for Red Bricks Insurance.
Your expertise: case notes, call transcripts, clinical documentation, and claims summaries.

Your role:
- Extract key themes and action items from case notes and call transcripts
- Identify documented barriers to care (transportation, literacy, compliance)
- Surface relevant clinical history from documentation
- Track care manager engagement patterns and follow-up gaps
- Highlight member-reported concerns and preferences

Quote specific passages from documents with dates and authors.
Format with markdown: blockquotes for key excerpts, bullets for findings."""

    @classmethod
    def gather(cls, member_id: str) -> dict[str, str]:
        """Override to pass the question as the search query for better relevance."""
        # The base class uses a generic query; we'll use it as-is
        # since we don't have the question at gather time.
        # The supervisor passes it through synthesize.
        return super().gather(member_id)
