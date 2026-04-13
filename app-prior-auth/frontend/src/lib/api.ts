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
};
