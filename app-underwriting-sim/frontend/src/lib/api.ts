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

// --- Rate Build-Up Types ---

export interface RateBuildupStep {
  step_name: string;
  factor_label: string;
  factor_value: number;
  running_total: number;
  description: string;
}

export interface RateBuildupResult {
  base_rate: number;
  steps: RateBuildupStep[];
  final_rate: number;
  current_rate?: number;
  rate_change?: number;
  rate_change_pct?: number;
  lob: string;
  narrative: string;
}

export interface RateBuildupInput {
  group_id?: string;
  avg_age_band?: string;
  county_type?: string;
  sic_code?: string;
  loss_ratio?: number;
  credibility_factor?: number;
  trend_pct?: number;
  lob?: string;
}

export interface FactorTableEntry {
  [key: string]: string | number;
}

export interface FactorTable {
  table_name: string;
  description: string;
  factors: FactorTableEntry[];
}

export interface FactorTables {
  age_factors: FactorTable;
  area_factors: FactorTable;
  industry_factors: FactorTable;
  trend_factors: FactorTable;
  experience_mod_ranges: FactorTable;
}

// --- Risk Pool Types ---

export interface DistributionBucket {
  label: string;
  group_value: number;
  book_value: number;
}

export interface ConditionPrevalence {
  condition: string;
  group_pct: number;
  book_pct: number;
  delta_pct: number;
}

export interface CostDriver {
  category: string;
  pmpm: number;
  pct_of_total: number;
}

export interface RiskPoolResult {
  group_id: string;
  group_member_count: number;
  group_avg_raf: number;
  book_avg_raf: number;
  raf_distribution: DistributionBucket[];
  age_distribution: DistributionBucket[];
  condition_prevalence: ConditionPrevalence[];
  top_cost_drivers: CostDriver[];
  adverse_selection_flag: boolean;
  adverse_selection_severity?: string;
  narrative: string;
}

export interface BookOfBusinessSummary {
  total_members: number;
  avg_raf: number;
  avg_age: number;
  raf_distribution: Record<string, unknown>[];
  age_distribution: Record<string, unknown>[];
  top_chronic_conditions: Record<string, unknown>[];
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

  // Pricing — Rate Build-Up
  computeRateBuildup: (input: RateBuildupInput) =>
    fetchApi<RateBuildupResult>("/pricing/rate-buildup", {
      method: "POST",
      body: JSON.stringify(input),
    }),

  getFactorTables: () =>
    fetchApi<FactorTables>("/pricing/factor-tables"),

  // Risk Pool
  getGroupRiskPool: (groupId: string) =>
    fetchApi<RiskPoolResult>(`/groups/${encodeURIComponent(groupId)}/risk-pool`),

  getBookOfBusinessSummary: () =>
    fetchApi<BookOfBusinessSummary>("/book-of-business/risk-summary"),
};
