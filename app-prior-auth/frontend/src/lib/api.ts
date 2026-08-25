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
  criteria_source?: string | null;
  criteria_version?: string | null;
  criteria_effective_date?: string | null;
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

export interface Appeal {
  appeal_id: string;
  auth_request_id: string;
  member_name: string | null;
  service_type: string | null;
  procedure_code: string | null;
  procedure_description: string | null;
  line_of_business: string | null;
  original_denial_reason_code: string | null;
  original_determination_reason: string | null;
  original_status: string | null;
  appeal_type: string | null;
  urgency: string | null;
  filed_by: string | null;
  filed_date: string | null;
  status: string | null;
  determination: string | null;
  original_reviewer_name: string | null;
  appeal_reviewer_name: string | null;
  appeal_reviewer_role: string | null;
  assigned_at: string | null;
  cms_deadline: string | null;
  cms_compliant: boolean | null;
  determination_date: string | null;
  turnaround_hours: number | null;
  hours_until_deadline: number | null;
}

export interface BusinessRule {
  rule_id: string;
  name: string;
  description: string | null;
  category: string | null;
  line_of_business: string | null;
  service_type: string | null;
  conditions_json: Record<string, unknown>;
  action: string;
  action_detail: string | null;
  priority: number;
  effective_start_date: string | null;
  effective_end_date: string | null;
  version: number;
  status: string;
  created_by: string | null;
  approved_by: string | null;
  approved_at: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface RuleSimulation {
  total_evaluated: number;
  matched: number;
  match_rate_pct: number;
  action: string | null;
  would_agree: number;
  would_disagree: number;
  agreement_rate_pct: number | null;
  sample_matches: string[];
}

export interface RuleConflict {
  rule_a: { rule_id: string; name: string; action: string; priority: number };
  rule_b: { rule_id: string; name: string; action: string; priority: number };
  winner_rule_id: string;
}

export interface RuleEvaluation {
  decision: string | null;
  action: string | null;
  action_detail?: string | null;
  fired_rule: { rule_id: string; name: string } | null;
  matched_rules: { rule_id: string; name: string; action: string; priority: number }[];
}

export interface PeerReview {
  peer_review_id: string;
  auth_request_id: string;
  requested_by_name: string | null;
  peer_reviewer_name: string | null;
  peer_reviewer_role: string | null;
  requested_specialty: string | null;
  reason: string | null;
  status: string | null;
  p2p_requested: boolean | null;
  p2p_scheduled_at: string | null;
  p2p_completed_at: string | null;
  p2p_summary: string | null;
  determination: string | null;
  determination_notes: string | null;
  notified_at: string | null;
  created_at: string | null;
}

export interface Correspondence {
  notice_id: string;
  auth_request_id: string | null;
  notice_type: string;
  recipient: string | null;
  recipient_role: string | null;
  subject: string | null;
  body_markdown: string | null;
  body_redacted: boolean | null;
  redaction_notes: string | null;
  includes_appeal_rights: boolean | null;
  criteria_citation: string | null;
  template_version: string | null;
  pdf_path: string | null;
  delivery_channel: string | null;
  delivery_status: string | null;
  generated_by: string | null;
  generated_at: string | null;
  released_at: string | null;
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

  recordAIDecision: (reqId: string, action: "accept" | "override", reason?: string) =>
    fetchApi<PARequestDetail>(`/requests/${reqId}/ai-decision`, {
      method: "POST",
      body: JSON.stringify({ action, reason }),
    }),

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

  // --- Appeals & Reconsiderations ---
  listAppeals: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<Appeal[]>(`/appeals${qs}`);
  },
  getAppeal: (id: string) => fetchApi<Appeal>(`/appeals/${id}`),
  fileAppeal: (body: {
    auth_request_id: string;
    appeal_type?: string;
    urgency?: string;
    filed_by?: string;
    filing_reason?: string;
  }) =>
    fetchApi<Appeal>("/appeals", { method: "POST", body: JSON.stringify(body) }),
  assignAppeal: (id: string, reviewerId: string) =>
    fetchApi<Appeal>(`/appeals/${id}/assign`, {
      method: "POST",
      body: JSON.stringify({ reviewer_id: reviewerId }),
    }),
  decideAppeal: (
    id: string,
    body: {
      status: string;
      determination_reason?: string;
      determination_reason_external?: string;
      reviewer_notes_internal?: string;
    },
  ) =>
    fetchApi<Appeal>(`/appeals/${id}/determination`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  // --- Business Rules Engine ---
  listRules: (status?: string) => {
    const qs = status ? `?status=${encodeURIComponent(status)}` : "";
    return fetchApi<BusinessRule[]>(`/rules${qs}`);
  },
  createRule: (body: Partial<BusinessRule> & { name: string; action: string }) =>
    fetchApi<BusinessRule>("/rules", { method: "POST", body: JSON.stringify(body) }),
  updateRule: (id: string, body: Partial<BusinessRule> & { name: string; action: string }) =>
    fetchApi<BusinessRule>(`/rules/${id}`, { method: "PUT", body: JSON.stringify(body) }),
  activateRule: (id: string) =>
    fetchApi<BusinessRule>(`/rules/${id}/activate`, { method: "POST" }),
  retireRule: (id: string) =>
    fetchApi<BusinessRule>(`/rules/${id}/retire`, { method: "POST" }),
  simulateRule: (id: string) =>
    fetchApi<RuleSimulation>(`/rules/${id}/simulate`, { method: "POST" }),
  getRuleConflicts: () =>
    fetchApi<{ conflicts: RuleConflict[] }>("/rules/conflicts"),
  evaluateRequestRules: (reqId: string) =>
    fetchApi<RuleEvaluation>(`/requests/${reqId}/rule-evaluation`),

  // --- Peer / Physician Review ---
  listPeerReviews: (reqId: string) =>
    fetchApi<PeerReview[]>(`/requests/${reqId}/peer-reviews`),
  requestPeerReview: (
    reqId: string,
    body: { peer_reviewer_id?: string; requested_specialty?: string; reason?: string; p2p_requested?: boolean },
  ) =>
    fetchApi<PeerReview>(`/requests/${reqId}/peer-reviews`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  decidePeerReview: (
    peerReviewId: string,
    body: { determination: string; determination_notes?: string; p2p_summary?: string },
  ) =>
    fetchApi<PeerReview>(`/peer-reviews/${peerReviewId}/determination`, {
      method: "POST",
      body: JSON.stringify(body),
    }),

  // --- Correspondence / Determination Notices ---
  listNotices: (reqId: string) =>
    fetchApi<Correspondence[]>(`/requests/${reqId}/notices`),
  generateNotice: (
    reqId: string,
    body: { notice_type: string; recipient?: string; delivery_channel?: string },
  ) =>
    fetchApi<Correspondence>(`/requests/${reqId}/notices`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  releaseNotice: (noticeId: string) =>
    fetchApi<Correspondence>(`/notices/${noticeId}/release`, { method: "POST" }),

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
