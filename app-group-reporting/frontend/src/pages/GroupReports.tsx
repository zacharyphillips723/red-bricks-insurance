import { useState, useEffect } from "react";
import {
  ArrowLeft,
  Loader2,
  Building2,
  DollarSign,
  TrendingUp,
  Pill,
  Activity,
  ShieldAlert,
  AlertTriangle,
} from "lucide-react";
import {
  api,
  type HighCostMember,
  type ClaimsTrendMonth,
  type TopDrug,
  type UtilizationRow,
  type RiskCareGapsResponse,
  type GroupReportCard,
} from "@/lib/api";

function fmt(val: string | null, prefix = "", suffix = "", decimals = 0): string {
  if (!val) return "N/A";
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  return `${prefix}${num.toLocaleString(undefined, { maximumFractionDigits: decimals, minimumFractionDigits: decimals })}${suffix}`;
}

const TABS = [
  { id: "high-cost", label: "High-Cost Members", icon: DollarSign },
  { id: "trend", label: "Claims Trend", icon: TrendingUp },
  { id: "drugs", label: "Top Drugs", icon: Pill },
  { id: "utilization", label: "Utilization", icon: Activity },
  { id: "risk", label: "Risk & Care Gaps", icon: ShieldAlert },
] as const;

type TabId = (typeof TABS)[number]["id"];

const TIER_COLORS: Record<string, string> = {
  "Extreme Outlier": "bg-red-500",
  "High Cost": "bg-orange-500",
  "Rising Risk": "bg-amber-400",
  Expected: "bg-blue-400",
  "Low Utilizer": "bg-green-400",
};

const RISK_COLORS: Record<string, string> = {
  Critical: "text-red-600 bg-red-50",
  High: "text-orange-600 bg-orange-50",
  Elevated: "text-amber-600 bg-amber-50",
  Moderate: "text-blue-600 bg-blue-50",
  Low: "text-green-600 bg-green-50",
};

interface Props {
  groupId: string;
  onBack: () => void;
}

export function GroupReports({ groupId, onBack }: Props) {
  const [activeTab, setActiveTab] = useState<TabId>("high-cost");
  const [card, setCard] = useState<GroupReportCard | null>(null);
  const [loading, setLoading] = useState(false);

  // Report data
  const [highCost, setHighCost] = useState<HighCostMember[] | null>(null);
  const [trend, setTrend] = useState<ClaimsTrendMonth[] | null>(null);
  const [drugs, setDrugs] = useState<TopDrug[] | null>(null);
  const [utilization, setUtilization] = useState<UtilizationRow[] | null>(null);
  const [riskGaps, setRiskGaps] = useState<RiskCareGapsResponse | null>(null);

  // Load group header
  useEffect(() => {
    api.getReportCard(groupId).then(setCard).catch(console.error);
  }, [groupId]);

  // Load report data on tab change
  useEffect(() => {
    const loaders: Record<TabId, () => Promise<void>> = {
      "high-cost": async () => {
        if (!highCost) {
          setLoading(true);
          setHighCost(await api.getHighCostMembers(groupId));
          setLoading(false);
        }
      },
      trend: async () => {
        if (!trend) {
          setLoading(true);
          setTrend(await api.getClaimsTrend(groupId));
          setLoading(false);
        }
      },
      drugs: async () => {
        if (!drugs) {
          setLoading(true);
          setDrugs(await api.getTopDrugs(groupId));
          setLoading(false);
        }
      },
      utilization: async () => {
        if (!utilization) {
          setLoading(true);
          setUtilization(await api.getUtilization(groupId));
          setLoading(false);
        }
      },
      risk: async () => {
        if (!riskGaps) {
          setLoading(true);
          setRiskGaps(await api.getRiskCareGaps(groupId));
          setLoading(false);
        }
      },
    };
    loaders[activeTab]().catch((e) => {
      console.error(e);
      setLoading(false);
    });
  }, [activeTab, groupId]);

  return (
    <div className="space-y-5">
      {/* Header */}
      <div>
        <button
          onClick={onBack}
          className="text-sm text-gray-500 hover:text-databricks-red flex items-center gap-1 mb-2"
        >
          <ArrowLeft className="w-4 h-4" /> Back to report card
        </button>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Building2 className="w-6 h-6 text-databricks-red" />
          Standard Reports — {card?.group_name || groupId}
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          {card?.group_id} &middot; {card?.industry} &middot;{" "}
          {card?.funding_type} &middot; {card?.total_members} members
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 border-b border-gray-200">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const active = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
                active
                  ? "border-databricks-red text-databricks-red"
                  : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
              }`}
            >
              <Icon className="w-4 h-4" />
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* Content */}
      {loading ? (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
        </div>
      ) : (
        <div>
          {activeTab === "high-cost" && highCost && (
            <HighCostTable data={highCost} />
          )}
          {activeTab === "trend" && trend && <ClaimsTrendView data={trend} />}
          {activeTab === "drugs" && drugs && <TopDrugsTable data={drugs} />}
          {activeTab === "utilization" && utilization && (
            <UtilizationTable data={utilization} />
          )}
          {activeTab === "risk" && riskGaps && (
            <RiskCareGapsView data={riskGaps} />
          )}
        </div>
      )}
    </div>
  );
}

// ===================================================================
// Report 1: High-Cost Members
// ===================================================================

function HighCostTable({ data }: { data: HighCostMember[] }) {
  if (data.length === 0)
    return <p className="text-gray-400 text-sm py-8 text-center">No member data available.</p>;

  return (
    <div className="card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Member</th>
            <th className="px-4 py-3">Age</th>
            <th className="px-4 py-3">Cost Tier</th>
            <th className="px-4 py-3 text-right">Total Paid</th>
            <th className="px-4 py-3 text-right">Medical</th>
            <th className="px-4 py-3 text-right">Pharmacy</th>
            <th className="px-4 py-3">TCI</th>
            <th className="px-4 py-3">Top Diagnoses</th>
            <th className="px-4 py-3">Care Gaps</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {data.map((m) => (
            <tr key={m.member_id} className="hover:bg-gray-50">
              <td className="px-4 py-3 font-medium">
                {m.first_name} {m.last_name}
                <div className="text-xs text-gray-400">{m.member_id}</div>
              </td>
              <td className="px-4 py-3">{m.age}</td>
              <td className="px-4 py-3">
                <span
                  className={`inline-block w-2 h-2 rounded-full mr-1.5 ${
                    TIER_COLORS[m.cost_tier || ""] || "bg-gray-400"
                  }`}
                />
                {m.cost_tier}
              </td>
              <td className="px-4 py-3 text-right font-semibold">
                {fmt(m.total_paid, "$", "", 0)}
              </td>
              <td className="px-4 py-3 text-right">{fmt(m.medical_paid, "$", "", 0)}</td>
              <td className="px-4 py-3 text-right">{fmt(m.pharmacy_paid, "$", "", 0)}</td>
              <td className="px-4 py-3">
                <span
                  className={
                    parseFloat(m.tci || "0") >= 2
                      ? "text-red-600 font-semibold"
                      : parseFloat(m.tci || "0") >= 1.5
                      ? "text-amber-600 font-semibold"
                      : ""
                  }
                >
                  {fmt(m.tci, "", "", 2)}
                </span>
              </td>
              <td className="px-4 py-3 text-xs text-gray-600 max-w-[200px] truncate">
                {m.top_diagnoses || "—"}
              </td>
              <td className="px-4 py-3">
                {parseInt(m.hedis_gap_count || "0") > 0 ? (
                  <span className="inline-flex items-center gap-1 text-amber-600 text-xs font-medium">
                    <AlertTriangle className="w-3 h-3" />
                    {m.hedis_gap_count} gaps
                  </span>
                ) : (
                  <span className="text-green-600 text-xs">None</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ===================================================================
// Report 2: Claims Trend
// ===================================================================

function ClaimsTrendView({ data }: { data: ClaimsTrendMonth[] }) {
  if (data.length === 0)
    return <p className="text-gray-400 text-sm py-8 text-center">No trend data available.</p>;

  const maxPmpm = Math.max(...data.map((d) => parseFloat(d.total_pmpm || "0")), 1);

  return (
    <div className="space-y-4">
      {/* Bar chart */}
      <div className="card p-5">
        <h3 className="text-sm font-semibold text-databricks-dark mb-4">
          Monthly Claims PMPM
        </h3>
        <div className="flex items-end gap-1 h-48">
          {data.map((d) => {
            const medPct =
              (parseFloat(d.medical_pmpm || "0") / maxPmpm) * 100;
            const rxPct =
              (parseFloat(d.pharmacy_pmpm || "0") / maxPmpm) * 100;
            return (
              <div
                key={d.month}
                className="flex-1 flex flex-col items-center justify-end h-full group relative"
              >
                <div className="w-full flex flex-col justify-end h-full">
                  <div
                    className="w-full bg-blue-400 rounded-t"
                    style={{ height: `${rxPct}%` }}
                    title={`Rx: ${fmt(d.pharmacy_pmpm, "$")}`}
                  />
                  <div
                    className="w-full bg-databricks-red"
                    style={{ height: `${medPct}%` }}
                    title={`Medical: ${fmt(d.medical_pmpm, "$")}`}
                  />
                </div>
                <span className="text-[9px] text-gray-400 mt-1 -rotate-45 origin-left">
                  {d.month?.slice(0, 7)}
                </span>
                {/* Tooltip */}
                <div className="absolute bottom-full mb-2 hidden group-hover:block bg-databricks-dark text-white text-xs rounded px-2 py-1 whitespace-nowrap z-10">
                  {d.month?.slice(0, 7)}: {fmt(d.total_pmpm, "$")} PMPM
                </div>
              </div>
            );
          })}
        </div>
        <div className="flex items-center gap-4 mt-4 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 bg-databricks-red rounded" /> Medical
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 bg-blue-400 rounded" /> Pharmacy
          </span>
        </div>
      </div>

      {/* Table */}
      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-4 py-3">Month</th>
              <th className="px-4 py-3 text-right">Total PMPM</th>
              <th className="px-4 py-3 text-right">Medical PMPM</th>
              <th className="px-4 py-3 text-right">Rx PMPM</th>
              <th className="px-4 py-3 text-right">Total Paid</th>
              <th className="px-4 py-3 text-right">Med Claims</th>
              <th className="px-4 py-3 text-right">Rx Claims</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {data.map((d) => (
              <tr key={d.month} className="hover:bg-gray-50">
                <td className="px-4 py-2 font-medium">{d.month?.slice(0, 7)}</td>
                <td className="px-4 py-2 text-right font-semibold">
                  {fmt(d.total_pmpm, "$")}
                </td>
                <td className="px-4 py-2 text-right">{fmt(d.medical_pmpm, "$")}</td>
                <td className="px-4 py-2 text-right">{fmt(d.pharmacy_pmpm, "$")}</td>
                <td className="px-4 py-2 text-right">{fmt(d.total_paid, "$")}</td>
                <td className="px-4 py-2 text-right">{d.medical_claims}</td>
                <td className="px-4 py-2 text-right">{d.pharmacy_claims}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ===================================================================
// Report 3: Top Drugs
// ===================================================================

function TopDrugsTable({ data }: { data: TopDrug[] }) {
  if (data.length === 0)
    return <p className="text-gray-400 text-sm py-8 text-center">No pharmacy data available.</p>;

  return (
    <div className="card overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Drug Name</th>
            <th className="px-4 py-3">Therapeutic Class</th>
            <th className="px-4 py-3">Specialty</th>
            <th className="px-4 py-3 text-right">Plan Paid</th>
            <th className="px-4 py-3 text-right">Total Cost</th>
            <th className="px-4 py-3 text-right">Fills</th>
            <th className="px-4 py-3 text-right">Members</th>
            <th className="px-4 py-3 text-right">Avg/Fill</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {data.map((d, i) => (
            <tr key={i} className="hover:bg-gray-50">
              <td className="px-4 py-3 font-medium">{d.drug_name}</td>
              <td className="px-4 py-3 text-gray-600">{d.therapeutic_class}</td>
              <td className="px-4 py-3">
                {d.is_specialty === "1" ? (
                  <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-semibold bg-purple-100 text-purple-800">
                    Specialty
                  </span>
                ) : (
                  <span className="text-gray-400 text-xs">No</span>
                )}
              </td>
              <td className="px-4 py-3 text-right font-semibold">
                {fmt(d.total_plan_paid, "$", "", 0)}
              </td>
              <td className="px-4 py-3 text-right">{fmt(d.total_cost, "$", "", 0)}</td>
              <td className="px-4 py-3 text-right">{d.fill_count}</td>
              <td className="px-4 py-3 text-right">{d.member_count}</td>
              <td className="px-4 py-3 text-right">{fmt(d.avg_cost_per_fill, "$", "", 2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ===================================================================
// Report 4: Utilization Summary
// ===================================================================

function UtilizationTable({ data }: { data: UtilizationRow[] }) {
  if (data.length === 0)
    return <p className="text-gray-400 text-sm py-8 text-center">No utilization data available.</p>;

  return (
    <div className="space-y-4">
      <div className="card overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-4 py-3">Claim Type</th>
              <th className="px-4 py-3 text-right">Claims</th>
              <th className="px-4 py-3 text-right">Unique Members</th>
              <th className="px-4 py-3 text-right">Total Paid</th>
              <th className="px-4 py-3 text-right">Avg/Claim</th>
              <th className="px-4 py-3 text-right">Per 1,000</th>
              <th className="px-4 py-3">Top Diagnoses</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {data.map((d, i) => (
              <tr key={i} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-medium">{d.claim_type}</td>
                <td className="px-4 py-3 text-right">{d.claim_count}</td>
                <td className="px-4 py-3 text-right">{d.unique_members}</td>
                <td className="px-4 py-3 text-right font-semibold">
                  {fmt(d.total_paid, "$", "", 0)}
                </td>
                <td className="px-4 py-3 text-right">{fmt(d.avg_paid_per_claim, "$", "", 0)}</td>
                <td className="px-4 py-3 text-right">
                  <span
                    className={
                      parseFloat(d.per_1000 || "0") > 500
                        ? "text-red-600 font-semibold"
                        : ""
                    }
                  >
                    {d.per_1000}
                  </span>
                </td>
                <td className="px-4 py-3 text-xs text-gray-600 max-w-[250px]">
                  {d.top_diagnoses || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ===================================================================
// Report 5: Risk & Care Gaps
// ===================================================================

function RiskCareGapsView({ data }: { data: RiskCareGapsResponse }) {
  const totalCostMembers = data.cost_tiers.reduce(
    (s, t) => s + parseInt(t.member_count || "0"),
    0
  );
  const totalRiskMembers = data.risk_tiers.reduce(
    (s, t) => s + parseInt(t.member_count || "0"),
    0
  );

  return (
    <div className="space-y-4">
      {/* Summary cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="card p-4 text-center">
          <div className="text-2xl font-bold text-databricks-dark">
            {data.summary.total_members || "0"}
          </div>
          <div className="text-xs text-gray-500 mt-1">Total Members</div>
        </div>
        <div className="card p-4 text-center">
          <div className="text-2xl font-bold text-amber-600">
            {data.summary.members_with_gaps || "0"}
          </div>
          <div className="text-xs text-gray-500 mt-1">Members with Care Gaps</div>
        </div>
        <div className="card p-4 text-center">
          <div className="text-2xl font-bold text-red-600">
            {data.summary.total_gaps || "0"}
          </div>
          <div className="text-xs text-gray-500 mt-1">Total HEDIS Gaps</div>
        </div>
        <div className="card p-4 text-center">
          <div className="text-2xl font-bold text-databricks-dark">
            {fmt(data.summary.avg_raf_score, "", "", 3)}
          </div>
          <div className="text-xs text-gray-500 mt-1">Avg RAF Score</div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Cost tier distribution */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-4">
            Cost Tier Distribution
          </h3>
          <div className="space-y-3">
            {data.cost_tiers.map((t) => {
              const count = parseInt(t.member_count || "0");
              const pct = totalCostMembers > 0 ? (count / totalCostMembers) * 100 : 0;
              return (
                <div key={t.cost_tier} className="space-y-1">
                  <div className="flex justify-between text-xs">
                    <span className="font-medium">{t.cost_tier}</span>
                    <span className="text-gray-500">
                      {count} ({pct.toFixed(0)}%) &middot; TCI: {t.avg_tci}
                    </span>
                  </div>
                  <div className="h-3 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${
                        TIER_COLORS[t.cost_tier || ""] || "bg-gray-400"
                      }`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Risk tier distribution */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-4">
            Risk Tier Distribution
          </h3>
          <div className="space-y-3">
            {data.risk_tiers.map((t) => {
              const count = parseInt(t.member_count || "0");
              const pct = totalRiskMembers > 0 ? (count / totalRiskMembers) * 100 : 0;
              return (
                <div key={t.risk_tier} className="space-y-1">
                  <div className="flex justify-between text-xs">
                    <span
                      className={`font-medium px-1.5 py-0.5 rounded ${
                        RISK_COLORS[t.risk_tier || ""] || ""
                      }`}
                    >
                      {t.risk_tier}
                    </span>
                    <span className="text-gray-500">
                      {count} ({pct.toFixed(0)}%) &middot; Avg RAF: {t.avg_raf}
                    </span>
                  </div>
                  <div className="h-3 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full bg-databricks-red/70"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Rising risk members */}
      {data.rising_risk_members.length > 0 && (
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-1">
            Rising Risk Members (TCI 1.5 - 2.0)
          </h3>
          <p className="text-xs text-gray-400 mb-4">
            Intervention sweet spot — trending toward high-cost but still manageable
          </p>
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
                <th className="px-4 py-2">Member</th>
                <th className="px-4 py-2">Age</th>
                <th className="px-4 py-2">TCI</th>
                <th className="px-4 py-2 text-right">Total Paid</th>
                <th className="px-4 py-2">Top Diagnoses</th>
                <th className="px-4 py-2">Care Gaps</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {data.rising_risk_members.map((m) => (
                <tr key={m.member_id} className="hover:bg-gray-50">
                  <td className="px-4 py-2 font-medium">
                    {m.first_name} {m.last_name}
                    <div className="text-xs text-gray-400">{m.member_id}</div>
                  </td>
                  <td className="px-4 py-2">{m.age}</td>
                  <td className="px-4 py-2 text-amber-600 font-semibold">
                    {fmt(m.tci, "", "", 2)}
                  </td>
                  <td className="px-4 py-2 text-right">{fmt(m.total_paid, "$", "", 0)}</td>
                  <td className="px-4 py-2 text-xs text-gray-600 max-w-[200px] truncate">
                    {m.top_diagnoses || "—"}
                  </td>
                  <td className="px-4 py-2">
                    {parseInt(m.hedis_gap_count || "0") > 0 ? (
                      <span className="text-amber-600 text-xs font-medium">
                        {m.hedis_gap_count} gaps
                      </span>
                    ) : (
                      <span className="text-green-600 text-xs">None</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
