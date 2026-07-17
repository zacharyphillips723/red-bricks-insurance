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

export interface DashboardStats {
  total_requests: number;
  pending_count: number;
  in_review_count: number;
  expedited_pending: number;
  approved_count: number;
  denied_count: number;
  approval_rate: number | null;
  avg_turnaround_hours: number | null;
  cms_compliance_rate: number | null;
  overdue_count: number;
  auto_adjudicated_count: number;
  requests_by_status: Record<string, number>;
  requests_by_service_type: Record<string, number>;
  requests_by_urgency: Record<string, number>;
}

export interface PARequestListItem {
  auth_request_id: string;
  member_id: string;
  member_name: string | null;
  requesting_provider_npi: string;
  provider_name: string | null;
  service_type: string;
  procedure_code: string;
  procedure_description: string | null;
  diagnosis_codes: string | null;
  policy_name: string | null;
  line_of_business: string | null;
  urgency: string | null;
  estimated_cost: number | null;
  status: string | null;
  determination_tier: string | null;
  ai_recommendation: string | null;
  ai_confidence: number | null;
  tier1_auto_eligible: boolean | null;
  reviewer_name: string | null;
  reviewer_role: string | null;
  assigned_at: string | null;
  request_date: string | null;
  cms_deadline: string | null;
  cms_compliant: boolean | null;
  time_open: string | null;
  hours_until_deadline: number | null;
}

export interface ActionLogEntry {
  action_id: string;
  auth_request_id: string;
  reviewer_name: string | null;
  action_type: string;
  previous_status: string | null;
  new_status: string | null;
  note: string | null;
  created_at: string;
}

export interface PARequestDetail extends PARequestListItem {
  policy_id: string | null;
  clinical_summary: string | null;
  assigned_reviewer_id: string | null;
  clinical_extraction: string | null;
  determination_reason: string | null;
  denial_reason_code: string | null;
  reviewer_notes: string | null;
  determination_date: string | null;
  turnaround_hours: number | null;
  appeal_filed: boolean | null;
  appeal_date: string | null;
  appeal_outcome: string | null;
  audit_log: ActionLogEntry[];
  created_at: string | null;
  updated_at: string | null;
}

export interface Reviewer {
  reviewer_id: string;
  email: string;
  display_name: string;
  role: string;
  department: string | null;
  specialty: string | null;
  max_caseload: number;
  is_active: boolean;
}

export interface ReviewerCaseload {
  reviewer_id: string;
  display_name: string;
  role: string;
  specialty: string | null;
  max_caseload: number;
  active_cases: number;
  expedited_cases: number;
  in_review: number;
  awaiting_info: number;
  available_capacity: number;
}

export interface AgentResponse {
  answer: string;
  sources: Record<string, unknown>[];
}

// SSE events from the streaming PA agent.
export type AgentStreamEvent =
  | { type: "status"; stage: string; message: string }
  | { type: "review"; answer: string; sources: Record<string, unknown>[] }
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

export interface DocumentHandle {
  document_id: string;
  filename: string;
  volume_path: string;
}

export interface SampleScenario {
  scenario: string;
  title: string;
  procedure: string;
}

// SSE events emitted by the document adjudication pipeline.
export type AdjudicationEvent =
  | { type: "status"; stage: string; message: string }
  | { type: "parsed"; text: string; char_count: number }
  | { type: "extracted"; facts: Record<string, unknown> }
  | {
      type: "decision";
      decision: string;
      confidence: number;
      reasons: string[];
      matched_policy: Record<string, unknown> | null;
      extracted_procedure_codes: string[];
      extracted_diagnosis_codes: string[];
      has_documentation: boolean;
    }
  | { type: "persisted"; auth_request_id: string }
  | { type: "done"; decision: string }
  | { type: "error"; message: string };

export interface ComplianceMetrics {
  compliance_rate: number | null;
  avg_turnaround_standard: number | null;
  avg_turnaround_expedited: number | null;
  overdue_count: number;
  auto_adjudication_rate: number | null;
  total_determined: number;
  total_auto: number;
  turnaround_distribution: { bucket: string; count: number; compliant: boolean }[];
  weekly_trend: { week: string; compliance_rate: number; total: number }[];
}

export interface OverdueRequest {
  auth_request_id: string;
  member_name: string | null;
  service_type: string;
  procedure_code: string;
  urgency: string | null;
  reviewer_name: string | null;
  cms_deadline: string | null;
  hours_overdue: number;
  request_date: string | null;
}

// --- API Functions ---

export const api = {
  getDashboardStats: () => fetchApi<DashboardStats>("/dashboard/stats"),

  listRequests: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<PARequestListItem[]>(`/requests${qs}`);
  },

  getRequest: (id: string) => fetchApi<PARequestDetail>(`/requests/${id}`),

  assignReviewer: (id: string, reviewerId: string) =>
    fetchApi<PARequestDetail>(`/requests/${id}/assign`, {
      method: "POST",
      body: JSON.stringify({ reviewer_id: reviewerId }),
    }),

  updateStatus: (id: string, status: string, note?: string, determinationReason?: string, denialReasonCode?: string) =>
    fetchApi<PARequestDetail>(`/requests/${id}/status`, {
      method: "POST",
      body: JSON.stringify({
        status,
        note,
        determination_reason: determinationReason,
        denial_reason_code: denialReasonCode,
      }),
    }),

  addNote: (id: string, note: string) =>
    fetchApi<PARequestDetail>(`/requests/${id}/notes`, {
      method: "POST",
      body: JSON.stringify({ note }),
    }),

  listReviewers: () => fetchApi<Reviewer[]>("/reviewers"),
  getReviewerCaseload: () => fetchApi<ReviewerCaseload[]>("/reviewers/caseload"),

  listPolicies: () => fetchApi<Record<string, unknown>[]>("/policies"),
  getPolicyRules: (policyId: string) =>
    fetchApi<Record<string, unknown>[]>(`/policies/${policyId}/rules`),

  getMLPrediction: (reqId: string) =>
    fetchApi<Record<string, unknown>>(`/requests/${reqId}/ml-prediction`),

  queryAgent: (question: string, authRequestId?: string) =>
    fetchApi<AgentResponse>("/agent/query", {
      method: "POST",
      body: JSON.stringify({ question, auth_request_id: authRequestId }),
    }),

  // Streaming PA agent: invokes onEvent for each SSE milestone.
  queryAgentStream: async (
    question: string,
    authRequestId: string | undefined,
    onEvent: (event: AgentStreamEvent) => void,
    signal?: AbortSignal,
  ): Promise<void> => {
    const res = await fetch(`${API_BASE}/agent/query/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, auth_request_id: authRequestId }),
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

  getComplianceMetrics: () => fetchApi<ComplianceMetrics>("/compliance/metrics"),
  getOverdueRequests: () => fetchApi<OverdueRequest[]>("/compliance/overdue"),

  // --- Observability ---
  getTraces: () => fetchApi<{ traces: ObservabilityTrace[] }>("/observability/traces"),
  getCostSummary: () => fetchApi<{ costs: CostSummary[] }>("/observability/costs"),

  // --- Document Intake ---
  listSampleScenarios: () =>
    fetchApi<{ scenarios: SampleScenario[] }>("/documents/scenarios"),

  sampleDownloadUrl: (scenario: string) =>
    `${API_BASE}/documents/sample?scenario=${encodeURIComponent(scenario)}`,

  uploadDocument: async (file: File): Promise<DocumentHandle> => {
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${API_BASE}/documents/upload`, { method: "POST", body: form });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `Upload failed: ${res.status}`);
    }
    return res.json();
  },

  // Stream the parse -> extract -> adjudicate -> persist pipeline as SSE.
  adjudicateStream: async (
    handle: DocumentHandle,
    onEvent: (event: AdjudicationEvent) => void,
    signal?: AbortSignal,
  ): Promise<void> => {
    const res = await fetch(`${API_BASE}/documents/adjudicate/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(handle),
      signal,
    });
    if (!res.ok || !res.body) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw new Error(err.detail || `Adjudication failed: ${res.status}`);
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
        const payload = JSON.parse(dataLines.join("\n"));
        onEvent({ type: eventType, ...payload } as AdjudicationEvent);
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
};
