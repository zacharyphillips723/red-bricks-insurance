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

export interface AgentResponse {
  answer: string;
  sources: Record<string, unknown>[];
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

  // Agent
  queryAgent: (question: string, targetId?: string, targetType?: string) =>
    fetchApi<AgentResponse>("/agent/query", {
      method: "POST",
      body: JSON.stringify({
        question,
        target_id: targetId,
        target_type: targetType,
      }),
    }),

};
