/**
 * API client for the Group Reporting Portal backend.
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

export interface GroupListItem {
  group_id: string;
  group_name: string | null;
  industry: string | null;
  group_size_tier: string | null;
  funding_type: string | null;
  state: string | null;
  total_members: string | null;
  active_members: string | null;
  claims_pmpm: string | null;
  loss_ratio: string | null;
  renewal_action: string | null;
  group_health_score: string | null;
}

export interface GroupReportCard {
  group_id: string;
  group_name: string | null;
  industry: string | null;
  group_size_tier: string | null;
  funding_type: string | null;
  state: string | null;
  total_members: string | null;
  active_members: string | null;
  total_member_months: string | null;
  total_premium_revenue: string | null;
  total_claims_paid: string | null;
  medical_claims_paid: string | null;
  pharmacy_claims_paid: string | null;
  claims_pmpm: string | null;
  medical_pmpm: string | null;
  pharmacy_pmpm: string | null;
  loss_ratio: string | null;
  ip_admits_per_1000: string | null;
  er_visits_per_1000: string | null;
  high_cost_claimants: string | null;
  specific_sl_excess: string | null;
  aggregate_attachment_ratio: string | null;
  actual_to_expected: string | null;
  credibility_factor: string | null;
  projected_renewal_pmpm: string | null;
  renewal_action: string | null;
  trend_factor: string | null;
  renewal_date: string | null;
  avg_member_tcoc: string | null;
  avg_tci: string | null;
  pct_high_cost: string | null;
  high_cost_members: string | null;
  cost_tier_distribution: string | null;
  claims_pmpm_pctl: string | null;
  loss_ratio_pctl: string | null;
  er_visits_pctl: string | null;
  tci_pctl: string | null;
  group_health_score: string | null;
}

export interface GroupTcocItem {
  cost_tier: string | null;
  member_count: string | null;
  avg_tcoc: string | null;
  avg_tci: string | null;
  total_paid: string | null;
}

// --- Report Types ---

export interface HighCostMember {
  member_id: string | null;
  first_name: string | null;
  last_name: string | null;
  age: string | null;
  gender: string | null;
  cost_tier: string | null;
  tci: string | null;
  raf_score: string | null;
  total_paid: string | null;
  medical_paid: string | null;
  pharmacy_paid: string | null;
  top_diagnoses: string | null;
  risk_tier: string | null;
  hedis_gap_count: string | null;
  hedis_gap_measures: string | null;
  hcc_count: string | null;
  member_months: string | null;
}

export interface ClaimsTrendMonth {
  month: string | null;
  medical_paid: string | null;
  pharmacy_paid: string | null;
  total_paid: string | null;
  medical_claims: string | null;
  pharmacy_claims: string | null;
  members: string | null;
  total_pmpm: string | null;
  medical_pmpm: string | null;
  pharmacy_pmpm: string | null;
}

export interface TopDrug {
  drug_name: string | null;
  therapeutic_class: string | null;
  is_specialty: string | null;
  fill_count: string | null;
  member_count: string | null;
  total_plan_paid: string | null;
  total_cost: string | null;
  total_member_copay: string | null;
  avg_cost_per_fill: string | null;
}

export interface UtilizationRow {
  claim_type: string | null;
  claim_count: string | null;
  unique_members: string | null;
  total_paid: string | null;
  avg_paid_per_claim: string | null;
  total_billed: string | null;
  per_1000: string | null;
  top_diagnoses: string | null;
}

export interface RiskCareGapsResponse {
  cost_tiers: GroupTcocItem[];
  risk_tiers: { risk_tier: string | null; member_count: string | null; avg_raf: string | null }[];
  summary: {
    total_members: string | null;
    members_with_gaps: string | null;
    total_gaps: string | null;
    avg_raf_score: string | null;
  };
  rising_risk_members: {
    member_id: string | null;
    first_name: string | null;
    last_name: string | null;
    age: string | null;
    tci: string | null;
    cost_tier: string | null;
    total_paid: string | null;
    top_diagnoses: string | null;
    hedis_gap_count: string | null;
  }[];
}

// --- Renewal Scenario Types ---

export interface RenewalScenarioResult {
  current_pmpm: number;
  projected_pmpm: number;
  rate_change_pct: number;
  current_loss_ratio: number;
  projected_loss_ratio: number;
  churn_probability: number;
  health_score: number;
  group_tenure_years: number;
}

// --- Competitive Benchmark Types ---

export interface CompetitorBenchmark {
  carrier_name: string;
  pmpm: number;
  network_size: string;
  wellness_programs: string[];
  member_satisfaction: number;
}

export interface CompetitiveBenchmarkResponse {
  group_id: string;
  group_name: string;
  red_bricks_pmpm: number;
  sic_code: string;
  size_tier: string;
  competitors: CompetitorBenchmark[];
}

export interface AgentResponse {
  answer: string;
  enrichment_sources: string[];
}

export type AgentStreamEvent =
  | { type: "status"; stage: string; message: string }
  | { type: "final"; answer: string; enrichment_sources: string[]; plan_doc_sources?: string[] }
  | { type: "error"; message: string };

export interface ObservabilityTrace {
  request_id: string;
  timestamp_ms: number;
  execution_time_ms: number;
  status: string;
  span_count: number;
}

export interface CostSummary {
  endpoint: string;
  request_count: number;
  total_input_tokens: number;
  total_output_tokens: number;
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

// --- API Functions ---

export const api = {
  listGroups: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<GroupListItem[]>(`/groups${qs}`);
  },

  getReportCard: (id: string) =>
    fetchApi<GroupReportCard>(`/groups/${id}/report-card`),

  getTcoc: (id: string) =>
    fetchApi<GroupTcocItem[]>(`/groups/${id}/tcoc`),

  // Standard reports
  getHighCostMembers: (id: string) =>
    fetchApi<HighCostMember[]>(`/groups/${id}/reports/high-cost-members`),

  getClaimsTrend: (id: string) =>
    fetchApi<ClaimsTrendMonth[]>(`/groups/${id}/reports/claims-trend`),

  getTopDrugs: (id: string) =>
    fetchApi<TopDrug[]>(`/groups/${id}/reports/top-drugs`),

  getUtilization: (id: string) =>
    fetchApi<UtilizationRow[]>(`/groups/${id}/reports/utilization`),

  getRiskCareGaps: (id: string) =>
    fetchApi<RiskCareGapsResponse>(`/groups/${id}/reports/risk-care-gaps`),

  // Report Card PDF
  getReportCardPdfUrl: (id: string) =>
    `${API_BASE}/groups/${id}/report-card-pdf`,

  // Renewal Scenario Modeling
  renewalScenario: (id: string, rateChangePct: number) =>
    fetchApi<RenewalScenarioResult>(`/groups/${id}/renewal-scenario`, {
      method: "POST",
      body: JSON.stringify({ rate_change_pct: rateChangePct }),
    }),

  // Competitive Benchmarking
  getCompetitiveBenchmark: (id: string) =>
    fetchApi<CompetitiveBenchmarkResponse>(`/groups/${id}/competitive-benchmark`),

  chatWithAgent: (groupId: string, question: string) =>
    fetchApi<AgentResponse>("/agent/chat", {
      method: "POST",
      body: JSON.stringify({ group_id: groupId, question }),
    }),

  askGenie: (question: string, conversationId?: string) =>
    fetchApi<GenieResponse>("/genie/ask", {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId }),
    }),

  chatWithAgentStream: async (
    groupId: string,
    question: string,
    onEvent: (event: AgentStreamEvent) => void,
    signal?: AbortSignal,
  ): Promise<void> => {
    const res = await fetch(`${API_BASE}/agent/chat/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ group_id: groupId, question }),
      signal,
    });
    if (!res.ok || !res.body) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `API error: ${res.status}`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    const flush = (block: string) => {
      let eventType = "message";
      const dataLines: string[] = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) eventType = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
      }
      if (!dataLines.length) return;
      try {
        onEvent({ type: eventType, ...JSON.parse(dataLines.join("\n")) } as AgentStreamEvent);
      } catch { /* ignore malformed frame */ }
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

  getTraces: () => fetchApi<{ traces: ObservabilityTrace[] }>("/observability/traces"),
  getCostSummary: () => fetchApi<{ costs: CostSummary[] }>("/observability/costs"),
};
