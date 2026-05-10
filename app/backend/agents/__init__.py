"""Multi-agent architecture for the Care Intelligence Agent.

Each sub-agent is a domain specialist with its own system prompt and tool set.
The supervisor in agent_graph.py routes questions to the appropriate specialist(s)
and merges their responses.
"""

from .clinical import ClinicalAgent
from .financial import FinancialAgent
from .care_management import CareManagementAgent
from .document import DocumentAgent

AGENTS = {
    "clinical": ClinicalAgent,
    "financial": FinancialAgent,
    "care_management": CareManagementAgent,
    "document": DocumentAgent,
}

__all__ = ["AGENTS", "ClinicalAgent", "FinancialAgent", "CareManagementAgent", "DocumentAgent"]
