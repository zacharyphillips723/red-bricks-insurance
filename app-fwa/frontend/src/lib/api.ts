/**
 * API client for the FWA Investigation Portal backend.
 */

const API_BASE = "/api";

async function fetchApi<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `API error: ${res.status}`);
  }
  return res.json();
}

// --- Types ---

export interface DashboardStats {
  total_investigations: number;
  open_count: number;
  critical_count: number;
  high_count: number;
  total_estimated_overpayment: number;
  total_recovered: number;
  recovery_rate: number | null;
  closed_this_month: number;
  investigations_by_status: Record<string, number>;
  investigations_by_type: Record<string, number>;
  investigations_by_severity: Record<string, number>;
}

export interface InvestigationListItem {
  investigation_id: string;
  investigation_type: string | null;
  target_type: string | null;
  target_id: string | null;
  target_name: string | null;
  fraud_types: string[];
  severity: string | null;
  status: string | null;
  source: string | null;
  estimated_overpayment: number | null;
  claims_involved_count: number | null;
  composite_risk_score: number | null;
  rules_risk_score: number | null;
  ml_risk_score: number | null;
  investigator_name: string | null;
  investigator_role: string | null;
  assigned_at: string | null;
  created_at: string | null;
  time_open: string | null;
}

export interface AuditLogEntry {
  audit_id: string;
  investigation_id: string;
  investigator_name: string | null;
  action_type: string;
  previous_status: string | null;
  new_status: string | null;
  note: string | null;
  created_at: string;
}

export interface EvidenceEntry {
  evidence_id: string;
  investigation_id: string;
  evidence_type: string;
  reference_id: string | null;
  description: string;
  added_by_name: string | null;
  created_at: string;
}

export interface InvestigationDetail extends InvestigationListItem {
  assigned_investigator_id: string | null;
  confirmed_overpayment: number | null;
  recovered_amount: number | null;
  investigation_summary: string | null;
  evidence_summary: string | null;
  recommendation: string | null;
  audit_log: AuditLogEntry[];
  evidence: EvidenceEntry[];
  updated_at: string | null;
  closed_at: string | null;
}

export interface Investigator {
  investigator_id: string;
  email: string;
  display_name: string;
  role: string;
  department: string | null;
  max_caseload: number;
  is_active: boolean;
}

export interface InvestigatorCaseload {
  investigator_id: string;
  display_name: string;
  role: string;
  max_caseload: number;
  active_cases: number;
  critical_cases: number;
  evidence_gathering: number;
  recovery_in_progress: number;
  total_active_overpayment: number;
  total_recovered: number;
  available_capacity: number;
}

export interface ProviderRisk {
  provider_npi: string;
  provider_name: string | null;
  specialty: string | null;
  total_claims: string | null;
  total_billed: string | null;
  total_paid: string | null;
  billed_to_allowed_ratio: string | null;
  e5_visit_pct: string | null;
  denial_rate: string | null;
  fwa_signal_count: string | null;
  fwa_avg_score: string | null;
  fwa_estimated_overpayment: string | null;
  composite_risk_score: string | null;
  risk_tier: string | null;
  specialty_risk_rank: string | null;
  overall_risk_rank: string | null;
}

export interface PolicyChunk {
  chunk_id: string;
  policy_name: string;
  service_category: string;
  chunk_text: string;
}

export interface AgentResponse {
  answer: string;
  sources: Record<string, unknown>[];
  model_used?: string;
  policy_chunks?: PolicyChunk[];
}

// --- Streaming agent events (SSE) ---
export type AgentStreamEvent =
  | { type: "status"; stage: string; message: string }
  | {
      type: "gemini";
      analysis: string;
      tools_used: string[];
      tables_queried: number;
      policy_chunks: PolicyChunk[];
      model: string;
    }
  | { type: "genie"; results: Record<string, unknown>[]; questions_asked: number }
  | {
      type: "final";
      answer: string;
      sources: Record<string, unknown>[];
      model_used?: string;
      policy_chunks?: PolicyChunk[];
    }
  | { type: "error"; message: string };

export interface PolicySection {
  chunk_id: string;
  policy_name: string;
  service_category: string;
  rule_type: string;
  chunk_text: string;
  procedure_codes: string;
  diagnosis_codes: string;
}

export interface ObservabilityTrace {
  request_id?: string;
  timestamp_ms?: number;
  status?: string;
  execution_time_ms?: number;
  [key: string]: unknown;
}

export interface CostSummary {
  endpoint: string;
  request_count: number;
  total_input_tokens: number;
  total_output_tokens: number;
  avg_latency_ms: number;
  estimated_cost_usd?: number;
}

export interface GenieResponse {
  conversation_id: string;
  message_id: string;
  sql_query: string | null;
  columns: string[];
  rows: Record<string, string>[];
  row_count: number;
  description: string | null;
}

export interface NetworkNode {
  id: string;
  type: "provider" | "member";
  name: string;
  risk_score: number;
  investigation_count: number;
  claim_count?: number;
  estimated_overpayment?: number;
}

export interface NetworkEdge {
  source: string;
  target: string;
  weight: number;
  fraud_score?: number;
  claim_count?: number;
}

export interface NetworkGraphData {
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  stats: {
    total_providers: number;
    total_members: number;
    total_claims: number;
    total_overpayment: number;
  };
}

// --- API Functions ---

export const api = {
  // Dashboard
  getDashboardStats: () => fetchApi<DashboardStats>("/dashboard/stats"),

  // Investigations
  listInvestigations: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<InvestigationListItem[]>(`/investigations${qs}`);
  },

  getInvestigation: (id: string) =>
    fetchApi<InvestigationDetail>(`/investigations/${id}`),

  assignInvestigation: (id: string, investigatorId: string) =>
    fetchApi<InvestigationDetail>(`/investigations/${id}/assign`, {
      method: "POST",
      body: JSON.stringify({ investigator_id: investigatorId }),
    }),

  updateInvestigationStatus: (id: string, status: string, note?: string) =>
    fetchApi<InvestigationDetail>(`/investigations/${id}/status`, {
      method: "POST",
      body: JSON.stringify({ status, note }),
    }),

  addNote: (id: string, note: string) =>
    fetchApi<InvestigationDetail>(`/investigations/${id}/notes`, {
      method: "POST",
      body: JSON.stringify({ note }),
    }),

  recordRecovery: (id: string, amount: number, note?: string) =>
    fetchApi<InvestigationDetail>(`/investigations/${id}/recovery`, {
      method: "POST",
      body: JSON.stringify({ recovered_amount: amount, note }),
    }),

  // Investigators
  listInvestigators: () => fetchApi<Investigator[]>("/investigators"),
  getInvestigatorCaseload: () =>
    fetchApi<InvestigatorCaseload[]>("/investigators/caseload"),

  // Provider Analysis
  getProviderRisk: (npi: string) =>
    fetchApi<ProviderRisk>(`/providers/${npi}/risk-profile`),

  getProviderClaims: (npi: string) =>
    fetchApi<Record<string, unknown>[]>(`/providers/${npi}/claims`),

  getProviderMLScores: (npi: string) =>
    fetchApi<Record<string, unknown>[]>(`/providers/${npi}/ml-scores`),

  getProviderShapValues: (npi: string) =>
    fetchApi<Record<string, number>>(`/providers/${npi}/shap-values`),

  // Network Graph
  getNetworkGraph: () => fetchApi<NetworkGraphData>("/network-graph"),

  // Agent (supervisor pattern — routes to Genie + Gemini sub-agents)
  queryAgent: (question: string, targetId?: string, targetType?: string) =>
    fetchApi<AgentResponse>("/agent/query", {
      method: "POST",
      body: JSON.stringify({
        question,
        target_id: targetId,
        target_type: targetType,
      }),
    }),

  // Streaming agent: invokes onEvent for each SSE milestone as it arrives.
  queryAgentStream: async (
    question: string,
    targetId: string | undefined,
    targetType: string | undefined,
    onEvent: (event: AgentStreamEvent) => void,
    signal?: AbortSignal,
  ): Promise<void> => {
    const res = await fetch(`${API_BASE}/agent/query/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, target_id: targetId, target_type: targetType }),
      signal,
    });
    if (!res.ok || !res.body) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `API error: ${res.status}`);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    // Parse SSE frames: blocks separated by a blank line, each with
    // "event: <type>" and "data: <json>" lines.
    const flush = (block: string) => {
      let eventType = "message";
      const dataLines: string[] = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) eventType = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
      }
      if (!dataLines.length) return;
      try {
        const payload = JSON.parse(dataLines.join("\n"));
        onEvent({ type: eventType, ...payload } as AgentStreamEvent);
      } catch {
        // ignore malformed frame
      }
    };

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let sep: number;
      while ((sep = buffer.indexOf("\n\n")) !== -1) {
        const block = buffer.slice(0, sep);
        buffer = buffer.slice(sep + 2);
        if (block.trim()) flush(block);
      }
    }
    if (buffer.trim()) flush(buffer);
  },

  // Genie
  askGenie: (question: string, conversationId?: string) =>
    fetchApi<GenieResponse>("/genie/ask", {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId || "" }),
    }),

  // Observability
  getTraces: () =>
    fetchApi<{ traces: ObservabilityTrace[] }>("/observability/traces"),

  getCostSummary: () =>
    fetchApi<{ costs: CostSummary[] }>("/observability/costs"),

};
