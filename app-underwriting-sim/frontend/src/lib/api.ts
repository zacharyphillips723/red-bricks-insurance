const API_BASE = "/api";

async function fetchApi<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return res.json();
}

// --- Types ---

export interface BaselineSummary {
  total_premium: number;
  total_claims: number;
  total_members: number;
  total_member_months: number;
  overall_mlr: number;
  pmpm_by_lob: Record<string, number>;
  mlr_by_lob: Record<string, number>;
  member_count_by_lob: Record<string, number>;
}

export interface SimulationResult {
  simulation_id?: string;
  simulation_type: string;
  baseline: Record<string, number>;
  projected: Record<string, number>;
  delta: Record<string, number>;
  delta_pct: Record<string, number>;
  narrative: string;
  warnings: string[];
}

export interface SimulationListItem {
  simulation_id: string;
  simulation_name: string;
  simulation_type: string;
  status: string;
  scope_lob?: string;
  scope_group_id?: string;
  narrative?: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface SimulationDetail {
  simulation_id: string;
  simulation_name: string;
  simulation_type: string;
  status: string;
  parameters: Record<string, unknown>;
  results?: Record<string, unknown>;
  baseline_snapshot?: Record<string, unknown>;
  scope_lob?: string;
  scope_group_id?: string;
  notes?: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface AuditEntry {
  audit_id: string;
  simulation_id: string;
  action: string;
  actor: string;
  details?: Record<string, unknown>;
  created_at: string;
}

export interface ComparisonSet {
  comparison_id: string;
  comparison_name: string;
  simulation_ids: string[];
  simulations: SimulationDetail[];
  notes?: string;
  created_by: string;
  created_at: string;
}

export interface AgentResponse {
  response: string;
  simulation_results?: SimulationResult[];
}

export interface GenieResponse {
  conversation_id?: string;
  message_id?: string;
  sql_query?: string;
  description?: string;
  columns: string[];
  rows: unknown[][];
}

// --- API Methods ---

export const api = {
  // Baseline
  getBaseline: (lob?: string) =>
    fetchApi<BaselineSummary>(`/baseline${lob ? `?lob=${encodeURIComponent(lob)}` : ""}`),

  refreshBaseline: () =>
    fetchApi<{ status: string }>("/baseline/refresh", { method: "POST" }),

  // Simulate
  simulate: (body: {
    simulation_type: string;
    parameters: Record<string, unknown>;
    save?: boolean;
    name?: string;
  }) =>
    fetchApi<SimulationResult>("/simulate", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  // Simulations CRUD
  listSimulations: (params?: {
    simulation_type?: string;
    status?: string;
    lob?: string;
  }) => {
    const qs = new URLSearchParams();
    if (params?.simulation_type) qs.set("simulation_type", params.simulation_type);
    if (params?.status) qs.set("status", params.status);
    if (params?.lob) qs.set("lob", params.lob);
    const q = qs.toString();
    return fetchApi<SimulationListItem[]>(`/simulations${q ? `?${q}` : ""}`);
  },

  getSimulation: (id: string) =>
    fetchApi<SimulationDetail>(`/simulations/${id}`),

  updateSimulation: (id: string, body: { status?: string; notes?: string }) =>
    fetchApi<SimulationDetail>(`/simulations/${id}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),

  deleteSimulation: (id: string) =>
    fetchApi<{ status: string }>(`/simulations/${id}`, { method: "DELETE" }),

  getAuditLog: (id: string) =>
    fetchApi<AuditEntry[]>(`/simulations/${id}/audit`),

  // Comparisons
  createComparison: (body: {
    comparison_name: string;
    simulation_ids: string[];
    notes?: string;
  }) =>
    fetchApi<ComparisonSet>("/comparisons", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  listComparisons: () => fetchApi<ComparisonSet[]>("/comparisons"),

  getComparison: (id: string) =>
    fetchApi<ComparisonSet>(`/comparisons/${id}`),

  // Agent
  chatAgent: (message: string, conversationHistory: Array<{ role: string; content: string }>) =>
    fetchApi<AgentResponse>("/agent/chat", {
      method: "POST",
      body: JSON.stringify({ message, conversation_history: conversationHistory }),
    }),

  // Genie
  askGenie: (question: string, conversationId?: string) =>
    fetchApi<GenieResponse>("/genie/ask", {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId }),
    }),
};
