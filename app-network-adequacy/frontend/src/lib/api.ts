/**
 * API client for the Network Adequacy Portal backend.
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

export interface ComplianceSummary {
  overall_compliance_pct: number;
  total_county_specialty_combos: number;
  non_compliant_count: number;
  total_gap_members: number;
}

export interface GhostSummary {
  total_flagged: number;
  high_severity: number;
  medium_severity: number;
  low_severity: number;
  total_impact_members: number;
}

export interface RecruitmentTarget {
  rendering_provider_npi: string;
  specialty: string | null;
  county_name: string | null;
  total_claims: number;
  total_paid: number;
  potential_savings: number;
  members_served: number;
  avg_member_distance_mi: number | null;
  recruitment_priority_score: number;
}

export interface DashboardStats {
  compliance_summary: ComplianceSummary;
  ghost_summary: GhostSummary;
  total_leakage_cost: number;
  total_oon_claims: number;
  telehealth_credits_applied: number;
  top_recruitment_targets: RecruitmentTarget[];
}

export interface ComplianceRow {
  county_fips: string;
  county_name: string;
  county_type: string;
  cms_specialty_type: string;
  max_distance_miles: number;
  max_time_minutes: number;
  total_members: number;
  compliant_members: number;
  pct_compliant: number;
  is_compliant: boolean;
  gap_members: number;
  avg_nearest_distance_mi: number | null;
  telehealth_available: boolean | null;
  telehealth_credit_applied: boolean | null;
}

export interface GhostProviderRow {
  npi: string;
  provider_name: string;
  specialty: string;
  cms_specialty_type: string | null;
  county: string;
  county_fips: string | null;
  ghost_severity: string;
  ghost_signal_count: number;
  is_ghost_flagged: boolean;
  impact_members: number;
  accepts_new_patients: boolean | null;
  telehealth_capable: boolean | null;
  panel_size: number | null;
  panel_capacity: number | null;
  appointment_wait_days: number | null;
  credentialing_status: string | null;
  last_claims_date: string | null;
  no_claims_12m: boolean | null;
  no_claims_6m: boolean | null;
  not_accepting: boolean | null;
  extreme_wait: boolean | null;
  credential_expired: boolean | null;
  panel_full: boolean | null;
}

export interface LeakageBySpecialty {
  cms_specialty_type: string;
  total_claims: number;
  total_paid: number;
  leakage_cost: number;
  unique_members: number;
  oon_providers: number;
}

export interface LeakageByCounty {
  county_name: string;
  county_type: string;
  total_claims: number;
  leakage_cost: number;
  unique_members: number;
}

export interface LeakageByReason {
  leakage_reason: string;
  total_claims: number;
  leakage_cost: number;
  unique_members: number;
}

export interface LeakageSummary {
  total_oon_claims: number;
  total_leakage_cost: number;
  total_oon_members: number;
  by_specialty: LeakageBySpecialty[];
  by_county: LeakageByCounty[];
  by_reason: LeakageByReason[];
}

export interface NetworkGap {
  county_name: string;
  county_type: string;
  cms_specialty_type: string;
  pct_compliant: number;
  gap_members: number;
  gap_status: string;
  priority_rank: number;
  cms_threshold_miles: number | null;
  avg_nearest_distance_mi: number | null;
  telehealth_credit_applied: boolean | null;
}

export interface CountyMapMetric {
  county_fips: string;
  county_name: string;
  county_type: string;
  latitude: number;
  longitude: number;
  avg_compliance_pct: number;
  non_compliant_specialties: number;
  total_specialties: number;
  gap_members: number;
  ghost_flagged_count: number;
  ghost_high_count: number;
  ghost_impact_members: number;
  oon_claims: number;
  leakage_cost: number;
  oon_members: number;
  total_providers: number;
  inn_providers: number;
  oon_providers: number;
}

export interface GenieResponse {
  conversation_id: string;
  message_id: string | null;
  sql_query: string | null;
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  description: string | null;
}

// --- API Functions ---

export const api = {
  // Dashboard
  getDashboardStats: () => fetchApi<DashboardStats>("/dashboard/stats"),

  // Compliance
  getCompliance: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<ComplianceRow[]>(`/compliance${qs}`);
  },
  getComplianceCounties: () => fetchApi<string[]>("/compliance/counties"),
  getComplianceSpecialties: () => fetchApi<string[]>("/compliance/specialties"),

  // Ghost Network
  getGhostProviders: (params?: Record<string, string>) => {
    const qs = params ? "?" + new URLSearchParams(params).toString() : "";
    return fetchApi<GhostProviderRow[]>(`/ghost-network${qs}`);
  },

  // Leakage
  getLeakageSummary: () => fetchApi<LeakageSummary>("/leakage"),

  // Recruitment
  getRecruitmentTargets: (limit = 20) =>
    fetchApi<RecruitmentTarget[]>(`/recruitment?limit=${limit}`),

  // Network Gaps
  getNetworkGaps: (maxPriority = 3) =>
    fetchApi<NetworkGap[]>(`/gaps?max_priority=${maxPriority}`),

  // Map
  getCountyMapMetrics: () => fetchApi<CountyMapMetric[]>("/map/county-metrics"),

  // Genie
  askGenie: (question: string, conversationId?: string) =>
    fetchApi<GenieResponse>("/genie/ask", {
      method: "POST",
      body: JSON.stringify({ question, conversation_id: conversationId }),
    }),
};
