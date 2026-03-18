/**
 * API client for the Population Health Command Center backend.
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
  total_alerts: number;
  unassigned_count: number;
  critical_count: number;
  high_count: number;
  resolved_this_month: number;
  avg_time_to_assign_hours: number | null;
  alerts_by_source: Record<string, number>;
  alerts_by_status: Record<string, number>;
}

export interface AlertListItem {
  alert_id: string;
  patient_id: string;
  mrn: string | null;
  member_id: string | null;
  risk_tier: string;
  risk_score: number | null;
  primary_driver: string;
  alert_source: string;
  status: string;
  payer: string | null;
  care_manager_name: string | null;
  care_manager_role: string | null;
  time_unassigned: string | null;
  created_at: string;
  updated_at: string;
}

export interface ActivityLog {
  activity_id: string;
  alert_id: string;
  care_manager_name: string | null;
  activity_type: string;
  previous_status: string | null;
  new_status: string | null;
  note: string | null;
  created_at: string;
}

export interface AlertDetail extends AlertListItem {
  secondary_drivers: string[];
  assigned_care_manager_id: string | null;
  assigned_at: string | null;
  status_changed_at: string | null;
  max_hba1c: number | null;
  max_blood_glucose: number | null;
  peak_ed_visits_12mo: number | null;
  last_encounter_date: string | null;
  last_facility: string | null;
  active_medications: string[];
  notes: string | null;
  activity_log: ActivityLog[];
  resolved_at: string | null;
}

export interface CareManager {
  care_manager_id: string;
  email: string;
  display_name: string;
  role: string;
  department: string | null;
  max_caseload: number;
  is_active: boolean;
}

export interface CareManagerCaseload {
  care_manager_id: string;
  display_name: string;
  role: string;
  max_caseload: number;
  active_cases: number;
  critical_cases: number;
  pending_outreach: number;
  pending_followup: number;
  available_capacity: number;
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

// --- Member 360 Types ---

export interface MemberListItem {
  member_id: string;
  first_name: string | null;
  last_name: string | null;
  member_name: string | null;
  date_of_birth: string | null;
  age: string | null;
  gender: string | null;
  line_of_business: string | null;
  risk_tier: string | null;
  raf_score: string | null;
  county: string | null;
}

export interface Member360Detail {
  member_id: string;
  first_name: string | null;
  last_name: string | null;
  member_name: string | null;
  date_of_birth: string | null;
  age: string | null;
  gender: string | null;
  address_line_1: string | null;
  city: string | null;
  state: string | null;
  zip_code: string | null;
  county: string | null;
  phone: string | null;
  email: string | null;
  line_of_business: string | null;
  plan_type: string | null;
  plan_id: string | null;
  group_name: string | null;
  eligibility_start_date: string | null;
  eligibility_end_date: string | null;
  monthly_premium: string | null;
  medical_claim_count: string | null;
  medical_total_paid_ytd: string | null;
  medical_total_billed_ytd: string | null;
  medical_member_responsibility_ytd: string | null;
  top_diagnoses: string | null;
  pharmacy_claim_count: string | null;
  pharmacy_spend_ytd: string | null;
  pharmacy_member_copay_ytd: string | null;
  total_paid_ytd: string | null;
  raf_score: string | null;
  hcc_codes: string | null;
  hcc_count: string | null;
  is_high_risk: string | null;
  risk_tier: string | null;
  hedis_gap_count: string | null;
  hedis_gap_measures: string | null;
  last_encounter_date: string | null;
  last_encounter_type: string | null;
  pcp_npi: string | null;
}

export interface CaseNote {
  document_id: string;
  member_id: string;
  document_type: string | null;
  title: string | null;
  created_date: string | null;
  author: string | null;
  full_text: string | null;
  text_length: string | null;
}

export interface AgentResponse {
  answer: string;
  sources: Record<string, string>[];
}

// --- API Functions ---

export const api = {
  getDashboardStats: () => fetchApi<DashboardStats>("/dashboard/stats"),

  listAlerts: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<AlertListItem[]>(`/alerts${qs}`);
  },

  getAlert: (id: string) => fetchApi<AlertDetail>(`/alerts/${id}`),

  assignAlert: (id: string, careManagerId: string) =>
    fetchApi<AlertDetail>(`/alerts/${id}/assign`, {
      method: "POST",
      body: JSON.stringify({ care_manager_id: careManagerId }),
    }),

  updateAlertStatus: (id: string, status: string, note?: string) =>
    fetchApi<AlertDetail>(`/alerts/${id}/status`, {
      method: "POST",
      body: JSON.stringify({ status, note }),
    }),

  addAlertNote: (id: string, note: string) =>
    fetchApi<AlertDetail>(`/alerts/${id}/notes`, {
      method: "POST",
      body: JSON.stringify({ note }),
    }),

  listCareManagers: () => fetchApi<CareManager[]>("/care-managers"),

  getCaseloadDashboard: () =>
    fetchApi<CareManagerCaseload[]>("/care-managers/caseload"),

  askGenie: (question: string, conversationId?: string) =>
    fetchApi<GenieResponse>("/genie/ask", {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId }),
    }),

  // Member 360
  searchMembers: (query: string) =>
    fetchApi<MemberListItem[]>(`/members?q=${encodeURIComponent(query)}`),

  getMember360: (memberId: string) =>
    fetchApi<Member360Detail>(`/members/${memberId}/360`),

  getCaseNotes: (memberId: string) =>
    fetchApi<CaseNote[]>(`/members/${memberId}/case-notes`),

  queryMemberAgent: (memberId: string, question: string) =>
    fetchApi<AgentResponse>(`/members/${memberId}/agent-query`, {
      method: "POST",
      body: JSON.stringify({ question }),
    }),
};
