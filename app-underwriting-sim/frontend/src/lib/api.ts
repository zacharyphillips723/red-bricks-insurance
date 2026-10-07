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

// SSE events from the streaming underwriting agent.
export type AgentStreamEvent =
  | { type: "status"; stage: string; message: string }
  | { type: "final"; response: string; simulation_results?: SimulationResult[] }
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
  // 'uc_table' when read from the governed UC table, else 'fallback'.
  source?: string;
}

// --- Packaged Scenarios Types ---

export interface ScenarioPackageInput {
  group_id?: string;
  lob?: string;
  target_mlr?: number;
  admin_load_pct?: number;
  market_trend_pct?: number;
  min_margin_pct?: number;
  competitor_pmpm?: number;
  competitor_within_pct?: number;
  custom_rate_change_pct?: number;
}

export interface ScenarioLeg {
  name: string;
  label: string;
  rate_change_pct: number;
  projected_premium: number;
  projected_mlr: number;
  margin_pct: number;
  margin_dollars: number;
  retention_probability: number;
  expected_retained_margin: number;
  rationale: string;
}

export interface ScenarioPackage {
  group_id?: string;
  lob?: string;
  basis: string;
  current_premium: number;
  current_claims: number;
  current_mlr: number;
  member_count: number;
  target_mlr: number;
  admin_load_pct: number;
  scenarios: ScenarioLeg[];
  recommended_scenario: string;
  recommendation_narrative: string;
}

// --- Phase 2: Funding arrangements ---

export interface FundingArrangementInfo {
  key: string;
  label: string;
  risk_bearer: string;
  description: string;
}

export interface FundingLineItem {
  label: string;
  pmpm: number;
  annual: number;
  note?: string;
}

export interface FundingQuoteResult {
  arrangement: string;
  basis: string;
  member_count: number;
  expected_annual_claims: number;
  risk_bearer: string;
  line_items: FundingLineItem[];
  total_annual_cost: number;
  total_pmpm: number;
  employer_max_liability?: number | null;
  fi_premium_baseline: number;
  dollar_impact_vs_fi: number;
  narrative: string;
  warnings: string[];
  quote_id?: string | null;
  [key: string]: unknown; // arrangement-specific extras
}

export interface FundingQuote {
  quote_id: string;
  group_name: string;
  funding_arrangement: string;
  lob?: string;
  scope_group_id?: string;
  inputs: Record<string, unknown>;
  result: FundingQuoteResult;
  total_annual_cost?: number;
  dollar_impact?: number;
  status: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

// --- Phase 2: Approval routing ---

export interface AuthorityTier {
  tier: number;
  role: string;
  max_dollar_impact?: number | null;
  max_rate_change_pct?: number | null;
  description: string;
}

export interface Approval {
  approval_id: string;
  subject: string;
  decision_type: string;
  group_id?: string;
  lob?: string;
  dollar_impact?: number;
  rate_change_pct?: number;
  required_tier?: number;
  required_role?: string;
  status: string;
  requested_by: string;
  decided_by?: string;
  decision_notes?: string;
  context?: Record<string, unknown>;
  created_at?: string;
  decided_at?: string;
}

// --- Phase 2: Factor governance ---

export interface FactorRow {
  factor_type: string;
  factor_key: string;
  factor_value: number;
}

export interface FactorVersion {
  version_id: string;
  version: number;
  status: string;
  factors: FactorRow[];
  source?: string;
  notes?: string;
  created_by: string;
  approved_by?: string;
  published_by?: string;
  created_at?: string;
  approved_at?: string;
  published_at?: string;
}

export interface FactorPublishResult {
  published: boolean;
  rows_written?: number;
  error?: string | null;
  version?: FactorVersion;
}

// --- Phase 3: Intake, negotiation, ops ---

export interface SubmissionCompleteness {
  percent_complete: number;
  present_fields: string[];
  missing_fields: string[];
  quote_ready: boolean;
}

export interface SubmissionExtract {
  doc_type: string;
  fields: Record<string, unknown>;
  completeness: SubmissionCompleteness;
}

export interface IntakeParseResult {
  fields: Record<string, unknown>;
  completeness: SubmissionCompleteness;
  strategy_memo?: string | null;
}

export interface QuoteRevision {
  revision_id: string;
  quote_id: string;
  instruction?: string;
  param_changes: Record<string, unknown>;
  result: FundingQuoteResult;
  total_annual_cost?: number;
  created_by: string;
  created_at: string;
}

export interface RerateResult {
  quote_id: string;
  param_changes: Record<string, unknown>;
  result: FundingQuoteResult;
  revisions: QuoteRevision[];
}

export interface OpsAnalytics {
  funnel: {
    total_quotes: number;
    by_status: Record<string, number>;
    quote_to_sold_pct: number;
    sold_to_implemented_pct: number;
  };
  cycle_time: { avg_hours_to_advance: number | null; sample: number };
  approvals: {
    total: number;
    pending: number;
    decided: number;
    approval_rate_pct: number;
    avg_decision_hours: number | null;
  };
  factor_governance: {
    published_versions: number;
    total_versions: number;
    changed_factors: number;
    max_abs_pct_change: number;
    compared_versions?: number[];
  };
}

export interface ReconciliationStage {
  count: number;
  total_annual_cost: number;
}

export interface ReconciliationQuote {
  quote_id: string;
  group_name: string;
  funding_arrangement: string;
  status: string;
  total_annual_cost: number;
  dollar_impact: number;
  created_at?: string;
  updated_at?: string;
}

export interface Reconciliation {
  stage_summary: Record<string, ReconciliationStage>;
  quotes: ReconciliationQuote[];
}

// --- Phase 4: Digital twin ---

export interface GuardrailResult {
  rule_type: string;
  threshold: number;
  severity: string;
  tripped: boolean;
  note?: string | null;
}

export interface Guardrail {
  guardrail_id: string;
  version: number;
  rule_type: string;
  scope?: Record<string, unknown>;
  threshold?: number;
  severity: string;
  active: boolean;
  approved_by?: string;
  approved_at?: string;
}

export interface PrecedentDecision {
  decision_id: string;
  funding_arrangement?: string;
  lob?: string;
  group_size_band?: string;
  scenario_chosen?: string;
  projected_margin?: number;
  projected_mlr?: number;
  dollar_impact?: number;
  rationale?: string;
  twin_verdict?: string;
  status?: string;
  created_at?: string;
  similarity?: number | null;
  outcome?: { actual_mlr?: number; actual_margin?: number; retained?: boolean } | null;
  [key: string]: unknown;
}

export interface AnalystProfile {
  analyst_id: string;
  risk_appetite?: string;
  typical_margin?: number;
  decision_count?: number;
  updated_at?: string;
}

export interface PreflightResult {
  decision_id: string;
  verdict: "GREEN" | "AMBER" | "RED";
  dollar_impact: number;
  rate_change_pct: number;
  projected_margin?: number | null;
  projected_mlr?: number | null;
  guardrail_results: GuardrailResult[];
  critique?: string;
  concern_level?: string;
  cited_decision_ids: string[];
  cited_precedents: PrecedentDecision[];
  analyst_history_count: number;
  analyst_profile?: AnalystProfile | null;
  required_approval_tier: AuthorityTier;
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

  // Streaming agent: invokes onEvent for each SSE milestone.
  chatAgentStream: async (
    message: string,
    conversationHistory: Array<{ role: string; content: string }>,
    onEvent: (event: AgentStreamEvent) => void,
    signal?: AbortSignal,
  ): Promise<void> => {
    const res = await fetch(`${API_BASE}/agent/chat/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, conversation_history: conversationHistory }),
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
      } catch {
        /* ignore malformed frame */
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

  // Observability
  getTraces: () => fetchApi<{ traces: ObservabilityTrace[] }>("/observability/traces"),
  getCostSummary: () => fetchApi<{ costs: CostSummary[] }>("/observability/costs"),

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

  // Packaged Scenarios
  packageScenarios: (input: ScenarioPackageInput) =>
    fetchApi<ScenarioPackage>("/scenarios/package", {
      method: "POST",
      body: JSON.stringify(input),
    }),

  // Risk Pool
  getGroupRiskPool: (groupId: string) =>
    fetchApi<RiskPoolResult>(`/groups/${encodeURIComponent(groupId)}/risk-pool`),

  getBookOfBusinessSummary: () =>
    fetchApi<BookOfBusinessSummary>("/book-of-business/risk-summary"),

  // Phase 2: Funding arrangements
  listFundingArrangements: () =>
    fetchApi<FundingArrangementInfo[]>("/funding/arrangements"),

  priceFundingQuote: (body: {
    arrangement: string;
    group_name?: string;
    save?: boolean;
    parameters: Record<string, unknown>;
  }) =>
    fetchApi<FundingQuoteResult>("/funding/quote", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  listFundingQuotes: (status?: string) =>
    fetchApi<FundingQuote[]>(`/funding/quotes${status ? `?status=${encodeURIComponent(status)}` : ""}`),

  getFundingQuote: (id: string) => fetchApi<FundingQuote>(`/funding/quotes/${id}`),

  updateFundingQuoteStatus: (id: string, status: string) =>
    fetchApi<FundingQuote>(`/funding/quotes/${id}/status?status=${encodeURIComponent(status)}`, {
      method: "PATCH",
    }),

  // Phase 2: Approval routing
  getAuthorityMatrix: () => fetchApi<AuthorityTier[]>("/approvals/authority-matrix"),

  createApproval: (body: {
    subject: string;
    decision_type?: string;
    dollar_impact?: number;
    rate_change_pct?: number;
    group_id?: string;
    lob?: string;
    context?: Record<string, unknown>;
  }) => fetchApi<Approval>("/approvals", { method: "POST", body: JSON.stringify(body) }),

  listApprovals: (status?: string) =>
    fetchApi<Approval[]>(`/approvals${status ? `?status=${encodeURIComponent(status)}` : ""}`),

  decideApproval: (id: string, decision: string, notes?: string) =>
    fetchApi<Approval>(`/approvals/${id}/decide`, {
      method: "POST",
      body: JSON.stringify({ decision, notes }),
    }),

  // Phase 2: Factor governance
  listFactorVersions: () => fetchApi<FactorVersion[]>("/factors/versions"),

  createFactorVersion: (body: { notes?: string; source?: string; factors?: FactorRow[] }) =>
    fetchApi<FactorVersion>("/factors/versions", { method: "POST", body: JSON.stringify(body) }),

  approveFactorVersion: (id: string) =>
    fetchApi<FactorVersion>(`/factors/versions/${id}/approve`, { method: "POST" }),

  publishFactorVersion: (id: string) =>
    fetchApi<FactorPublishResult>(`/factors/versions/${id}/publish`, { method: "POST" }),

  // Phase 3: Intake & document intelligence
  extractSubmission: (text: string, docType = "submission") =>
    fetchApi<SubmissionExtract>("/intake/extract", {
      method: "POST",
      body: JSON.stringify({ text, doc_type: docType }),
    }),

  parseIntake: (text: string, strategyMemo = true) =>
    fetchApi<IntakeParseResult>("/intake/parse", {
      method: "POST",
      body: JSON.stringify({ text, strategy_memo: strategyMemo }),
    }),

  // Phase 3: Negotiation
  rerateQuote: (quoteId: string, instruction: string) =>
    fetchApi<RerateResult>(`/funding/quotes/${quoteId}/rerate`, {
      method: "POST",
      body: JSON.stringify({ instruction }),
    }),

  listQuoteRevisions: (quoteId: string) =>
    fetchApi<QuoteRevision[]>(`/funding/quotes/${quoteId}/revisions`),

  // Phase 3: Operational analytics
  getOpsAnalytics: () => fetchApi<OpsAnalytics>("/ops/analytics"),
  getReconciliation: () => fetchApi<Reconciliation>("/ops/reconciliation"),

  // Phase 4: Digital twin
  preflightCheck: (body: {
    funding_arrangement?: string;
    lob?: string;
    group_id?: string;
    group_size?: number;
    industry?: string;
    scenario_chosen?: string;
    rationale?: string;
    parameters?: Record<string, unknown>;
    projected_margin?: number;
    projected_mlr?: number;
    dollar_impact?: number;
  }) => fetchApi<PreflightResult>("/policy/preflight-check", { method: "POST", body: JSON.stringify(body) }),

  listGuardrails: () => fetchApi<Guardrail[]>("/twin/guardrails"),

  createGuardrail: (body: {
    rule_type: string;
    threshold: number;
    severity: string;
    scope?: Record<string, unknown>;
  }) => fetchApi<Guardrail>("/twin/guardrails", { method: "POST", body: JSON.stringify(body) }),

  getTwinMemory: (limit = 25) => fetchApi<PrecedentDecision[]>(`/twin/memory?limit=${limit}`),

  recordOutcome: (body: {
    decision_id: string;
    actual_mlr?: number;
    actual_margin?: number;
    retained?: boolean;
    note?: string;
  }) => fetchApi<{ feedback_id: string; decision_id: string }>("/twin/outcome", {
    method: "POST",
    body: JSON.stringify(body),
  }),
};
