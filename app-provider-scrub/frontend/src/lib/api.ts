const API_BASE = "/api";

async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
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

export type RequestType = "claim" | "prior_auth";
export type ScrubDecisionValue = "clean" | "at_risk" | "likely_denied";
export type ReasonLayerValue = "rule" | "ml" | "rag";

export interface ClaimLine {
  cpt: string;
  units: number;
  pos?: string | null;
}

export interface DraftClaim {
  member_id: string;
  provider_npi: string;
  date_of_service: string;
  request_type: RequestType;
  lines: ClaimLine[];
  dx_codes: string[];
  clinical_notes?: string | null;
  billed_amount?: number | null;
  line_of_business?: string | null;
  auth_reference?: string | null;
  resubmitted_from?: string | null;
}

export interface ReasonCard {
  carc_code: string;
  reason_label: string;
  reason_category: string;
  likelihood: number;
  layer: ReasonLayerValue;
  evidence?: string | null;
  remediation?: string | null;
  remediation_text?: string | null;
  required_action?: string | null;
  doc_needed?: string | null;
}

export interface FeatureContribution {
  feature: string;
  label?: string | null;
  value?: number | null;
  contribution?: number | null;
}

export interface ScrubResult {
  session_id: string;
  member_id: string;
  member_name?: string | null;
  provider_npi: string;
  date_of_service: string;
  request_type: RequestType;
  risk_score: number;
  decision: ScrubDecisionValue;
  ml_denial_prob?: number | null;
  ml_contributions?: FeatureContribution[];
  reason_cards: ReasonCard[];
  resubmitted_from?: string | null;
  trace_id?: string | null;
  evaluated_at?: string | null;
}

export interface FeedbackIn {
  trace_id?: string | null;
  session_id?: string | null;
  target: string; // "overall" or a CARC code
  value: boolean; // true = 👍, false = 👎
  rationale?: string | null;
}

export interface FeedbackRow {
  session_id?: string | null;
  trace_id?: string | null;
  target?: string | null;
  value?: number | null;
  rationale?: string | null;
  source_id?: string | null;
  created_at?: string | null;
}

export interface MemberSearchItem {
  member_id: string;
  member_name?: string | null;
  line_of_business?: string | null;
  is_active?: boolean | null;
  eligibility_start_date?: string | null;
  eligibility_end_date?: string | null;
}

export interface CarcReference {
  carc_code: string;
  group_code?: string | null;
  reason_category?: string | null;
  description?: string | null;
  patient_vs_payer?: string | null;
}

export interface SampleDraft {
  scenario: string;
  title: string;
  expected: string;
  request_type: RequestType;
  draft: DraftClaim;
}

export interface ScrubSessionSummary {
  session_id: string;
  member_id: string;
  member_name?: string | null;
  provider_npi?: string | null;
  date_of_service?: string | null;
  request_type?: string | null;
  risk_score?: number | null;
  decision?: string | null;
  finding_count: number;
  resubmitted_from?: string | null;
  created_at?: string | null;
}

// Denial Intelligence — values arrive as strings from Statement Execution; coerce in the UI.
export interface PropensityBucket { bucket: string; n: number | string; }
export interface ReasonCount { reason: string; n: number | string; }
export interface PropensitySummary { total?: number | string; avg_prob?: number | string; high_risk?: number | string; }
export interface PropensityDistribution {
  buckets: PropensityBucket[];
  reasons: ReasonCount[];
  summary: PropensitySummary;
}
export interface DenialDriver {
  rank: number | string;
  feature: string;
  label: string;
  importance: number | string;
  importance_pct: number | string;
  method: string;
}
export interface CorrelationRow {
  dimension_value: string;
  total: number | string;
  denied: number | string;
  denial_rate: number | string;
}
export interface ForecastPoint {
  ds: string;
  metric: string;
  actual: number | string | null;
  forecast: number | string | null;
  lower: number | string | null;
  upper: number | string | null;
  is_forecast: boolean;
  method: string;
}

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

// SSE events emitted by the streaming scrub pipeline.
export type ScrubStreamEvent =
  | { type: "status"; stage: string; message: string }
  | ({ type: "result" } & ScrubResult)
  | { type: "error"; message: string };

// --- SSE reader ---

async function streamSse<E>(
  path: string,
  body: unknown,
  onEvent: (event: E) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
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
      onEvent({ type: eventType, ...JSON.parse(dataLines.join("\n")) } as E);
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
}

// --- API Functions ---

export const api = {
  searchMembers: (q: string) =>
    fetchApi<MemberSearchItem[]>(`/members/search?q=${encodeURIComponent(q)}`),

  getCarcReference: () => fetchApi<CarcReference[]>("/reference/carc"),

  getSamples: () => fetchApi<{ samples: SampleDraft[] }>("/scrub/samples"),

  runScrub: (draft: DraftClaim) =>
    fetchApi<ScrubResult>("/scrub/run", {
      method: "POST",
      body: JSON.stringify(draft),
    }),

  runScrubStream: (
    draft: DraftClaim,
    onEvent: (event: ScrubStreamEvent) => void,
    signal?: AbortSignal,
  ) => streamSse<ScrubStreamEvent>("/scrub/run/stream", draft, onEvent, signal),

  resubmitScrub: (sessionId: string, draft: DraftClaim) =>
    fetchApi<ScrubResult>(`/scrub/${sessionId}/resubmit`, {
      method: "POST",
      body: JSON.stringify(draft),
    }),

  getHistory: () => fetchApi<ScrubSessionSummary[]>("/scrub/history"),

  getSession: (sessionId: string) =>
    fetchApi<ScrubResult & { clinical_notes?: string; dx_codes?: string }>(`/scrub/${sessionId}`),

  getTraces: () => fetchApi<{ traces: ObservabilityTrace[] }>("/observability/traces"),
  getCostSummary: () => fetchApi<{ costs: CostSummary[] }>("/observability/costs"),

  getPropensity: () => fetchApi<PropensityDistribution>("/analytics/propensity"),
  getDrivers: () => fetchApi<{ drivers: DenialDriver[] }>("/analytics/drivers"),
  getCorrelations: (dimension: string) =>
    fetchApi<{ dimension: string; rows: CorrelationRow[] }>(
      `/analytics/correlations?dimension=${encodeURIComponent(dimension)}`,
    ),
  getForecast: () => fetchApi<{ series: ForecastPoint[] }>("/analytics/forecast"),

  submitFeedback: (payload: FeedbackIn) =>
    fetchApi<{ status: string; trace_id?: string; mlflow_logged: boolean }>("/scrub/feedback", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getRecentFeedback: () => fetchApi<FeedbackRow[]>("/scrub/feedback/recent"),
};
