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
  recent_labs: LabResult[];
}

export interface LabResult {
  lab_result_id: string | null;
  member_id: string | null;
  lab_name: string | null;
  value: string | null;
  unit: string | null;
  reference_range_low: string | null;
  reference_range_high: string | null;
  collection_date: string | null;
  is_abnormal: string | null;
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
  conversation_id?: string;
  message_id?: string;
  specialists_consulted?: string[];
  category?: string;
}

export interface ConversationListItem {
  conversation_id: string;
  member_id: string;
  title: string | null;
  message_count: number;
  created_at: string | null;
  updated_at: string | null;
}

export interface ConversationMessage {
  role: string;
  content: string;
  metadata: Record<string, unknown>;
  created_at: string | null;
}

export interface FeedbackResponse {
  feedback_id: string;
  status: string;
}

export interface UserInfo {
  email: string;
  display_name: string;
}

export interface MemberSdoh {
  member_id: string;
  screening_date: string | null;
  county: string | null;
  food_insecurity_flag: boolean;
  housing_instability_flag: boolean;
  transportation_barrier_flag: boolean;
  social_isolation_flag: boolean;
  financial_strain_flag: boolean;
  total_sdoh_flags: number;
  composite_sdoh_risk_score: number | null;
}

export interface NextBestAction {
  action: string;
  priority: string;
  category: string;
  detail: string;
}

export interface NextBestActionsResponse {
  actions: NextBestAction[];
  rationale: string;
}

// --- Outreach Draft Types ---

export interface OutreachDraft {
  channel: string;
  subject: string | null;
  script: string;
  key_talking_points: string[];
  member_name: string;
  generated_at: string;
}

// --- Cohort Builder Types ---

export interface CohortSearchCriteria {
  risk_tiers?: string[];
  counties?: string[];
  lines_of_business?: string[];
  min_age?: number | null;
  max_age?: number | null;
  gender?: string | null;
  min_raf_score?: number | null;
  has_hedis_gaps?: boolean | null;
  diagnoses_contain?: string | null;
  limit?: number;
}

export interface CohortMember {
  member_id: string;
  member_name: string | null;
  age: string | null;
  gender: string | null;
  county: string | null;
  risk_tier: string | null;
  raf_score: string | null;
  line_of_business: string | null;
  hedis_gap_count: string | null;
  top_diagnoses: string | null;
  total_paid_ytd: string | null;
}

export interface CohortAnalytics {
  total_members: number;
  risk_distribution: Record<string, number>;
  avg_raf_score: number | null;
  avg_age: number | null;
  total_cost_ytd: number | null;
  avg_cost_per_member: number | null;
  total_hedis_gaps: number;
  gender_distribution: Record<string, number>;
  lob_distribution: Record<string, number>;
  top_counties: Record<string, number>;
  members: CohortMember[];
}

export interface CohortFilterOptions {
  risk_tiers: string[];
  counties: string[];
  lines_of_business: string[];
  genders: string[];
}

export interface SavedCohort {
  cohort_id: string;
  cohort_name: string;
  description: string | null;
  criteria: Record<string, unknown>;
  member_count: number;
  created_by: string;
  created_at: string;
}

// --- Embed Config ---

export interface EmbedConfig {
  workspace_url: string;
  genie_space_id: string;
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

  queryMemberAgent: (memberId: string, question: string, conversationId?: string) =>
    fetchApi<AgentResponse>(`/members/${memberId}/agent-query`, {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId }),
    }),

  streamMemberAgent: (
    memberId: string,
    question: string,
    conversationId: string | undefined,
    onToken: (content: string) => void,
    onDone: (data: { conversation_id: string; message_id: string }) => void,
    onError: (error: string) => void,
    onStatus?: (status: { event: string; data: Record<string, unknown> }) => void,
  ) => {
    const controller = new AbortController();
    fetch(`${API_BASE}/members/${memberId}/agent-stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, conversation_id: conversationId }),
      signal: controller.signal,
    })
      .then(async (res) => {
        if (!res.ok) throw new Error(`Stream error: ${res.status}`);
        const reader = res.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";
          let currentEvent = "";
          for (const line of lines) {
            if (line.startsWith("event: ")) currentEvent = line.slice(7);
            else if (line.startsWith("data: ") && currentEvent) {
              const data = line.slice(6);
              try {
                const parsed = JSON.parse(data);
                if (currentEvent === "token") onToken(parsed.content || "");
                else if (currentEvent === "done") onDone({ conversation_id: parsed.conversation_id, message_id: parsed.message_id || "" });
                else if (currentEvent === "message_saved") onDone(parsed);
                else if (currentEvent === "error") onError(parsed.error || "Unknown error");
                else if (onStatus) onStatus({ event: currentEvent, data: parsed });
              } catch {
                // non-JSON event data — ignore
              }
              currentEvent = "";
            }
          }
        }
      })
      .catch((err) => {
        if (err.name !== "AbortError") onError(err.message);
      });
    return controller;
  },

  // Conversations
  listConversations: (memberId: string) =>
    fetchApi<ConversationListItem[]>(`/members/${memberId}/conversations`),

  getConversationMessages: (conversationId: string) =>
    fetchApi<ConversationMessage[]>(`/conversations/${conversationId}/messages`),

  // Feedback
  submitFeedback: (messageId: string, conversationId: string, rating: "positive" | "negative", comment?: string) =>
    fetchApi<FeedbackResponse>("/feedback", {
      method: "POST",
      body: JSON.stringify({ message_id: messageId, conversation_id: conversationId, rating, comment }),
    }),

  // SDOH
  getMemberSdoh: (memberId: string) =>
    fetchApi<MemberSdoh>(`/members/${memberId}/sdoh`),

  // Next Best Actions
  getNextBestActions: (alertId: string) =>
    fetchApi<NextBestActionsResponse>(`/alerts/${alertId}/next-actions`),

  // Care Plan
  generateCarePlan: (memberId: string) =>
    fetchApi<{ summary: string; goals: { goal: string; target_date: string; status: string; interventions: { action: string; responsible: string; frequency: string; status: string; notes: string }[] }[]; generated_at: string }>(`/members/${memberId}/care-plan`, {
      method: "POST",
    }),

  // Outreach Drafting
  generateOutreachDraft: (memberId: string, channel: string, context?: string) =>
    fetchApi<OutreachDraft>(`/members/${memberId}/outreach-draft`, {
      method: "POST",
      body: JSON.stringify({ channel, context }),
    }),

  // Cohort Builder
  searchCohort: (criteria: CohortSearchCriteria) =>
    fetchApi<CohortAnalytics>(`/cohorts/search`, {
      method: "POST",
      body: JSON.stringify(criteria),
    }),

  getCohortFilterOptions: () =>
    fetchApi<CohortFilterOptions>("/cohorts/filter-options"),

  saveCohort: (name: string, description: string | null, criteria: Record<string, unknown>, memberCount: number) =>
    fetchApi<SavedCohort>("/cohorts/save", {
      method: "POST",
      body: JSON.stringify({ cohort_name: name, description, criteria, member_count: memberCount }),
    }),

  getSavedCohorts: () =>
    fetchApi<SavedCohort[]>("/cohorts/saved"),

  deleteSavedCohort: (cohortId: string) =>
    fetchApi<{ status: string; cohort_id: string }>(`/cohorts/saved/${cohortId}`, {
      method: "DELETE",
    }),

  // User identity
  getCurrentUser: () => fetchApi<UserInfo>("/me"),

  // Embed config
  getEmbedConfig: () => fetchApi<EmbedConfig>("/embed-config"),

  // Care plan history (governed UC write-back)
  getCarePlanHistory: (memberId: string) =>
    fetchApi<{ plans: CarePlanHistoryItem[]; error?: string }>(`/members/${memberId}/care-plans`),

  // Observability
  getObservabilityTraces: () =>
    fetchApi<{ traces: ObservabilityTrace[]; error?: string }>("/observability/traces"),
  getObservabilityCosts: () =>
    fetchApi<{ costs: CostSummary[]; error?: string }>("/observability/costs"),

  // Eval quality
  getFeedbackSummary: () => fetchApi<FeedbackSummary>("/eval/feedback-summary"),
  runEvalScorers: (question: string, response: string) =>
    fetchApi<EvalScores>("/eval/run-scorers", {
      method: "POST",
      body: JSON.stringify({ question, response }),
    }),

  // PHI governance
  getGovernanceStatus: () => fetchApi<GovernanceStatus>("/governance/status"),
  getGovernedMembers: (limit = 15) =>
    fetchApi<{ members: Record<string, string>[]; error?: string }>(`/governance/members?limit=${limit}`),
};

// --- Extended types for new capabilities ---

export interface CarePlanHistoryItem {
  plan_id: string;
  member_name: string | null;
  generated_by: string | null;
  generated_at: string;
  model_endpoint: string | null;
  goal_count: number | null;
  summary: string | null;
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

export interface FeedbackSummary {
  total: number;
  positive: number;
  negative: number;
  with_comment: number;
  satisfaction_rate: number | null;
  recent: Array<{
    rating: string;
    comment: string | null;
    user_email: string | null;
    created_at: string;
    message_content: string | null;
  }>;
}

export interface EvalScores {
  relevance: number;
  groundedness: number;
  clinical_safety: number;
  rationale: string;
  judge_endpoint: string;
}

export interface GovernanceStatus {
  current_identity: string | null;
  sees_unmasked: boolean | null;
  access_level: string;
  masked_columns: string[];
  governed_view: string;
  unmask_group: string;
}
