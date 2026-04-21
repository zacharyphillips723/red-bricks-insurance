import { useEffect, useState } from "react";
import {
  ShieldCheck,
  Ghost,
  ArrowRightLeft,
  Users,
  Wifi,
  Target,
} from "lucide-react";
import { api, type DashboardStats } from "@/lib/api";
import { formatCurrency, formatNumber, formatPercent } from "@/lib/utils";

interface DashboardProps {
  onNavigate: (page: string) => void;
}

export function Dashboard({ onNavigate }: DashboardProps) {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDashboardStats().then(setStats).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">Network Adequacy Dashboard</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="card p-6 animate-pulse">
              <div className="h-4 bg-gray-200 rounded w-24 mb-3" />
              <div className="h-8 bg-gray-200 rounded w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (!stats) return null;

  const { compliance_summary: cs, ghost_summary: gs } = stats;

  const kpiCards = [
    {
      label: "Overall Compliance",
      value: formatPercent(cs.overall_compliance_pct),
      sub: `${formatNumber(cs.non_compliant_count)} non-compliant combinations`,
      icon: ShieldCheck,
      color: cs.overall_compliance_pct >= 90 ? "text-green-600" : "text-red-600",
      bg: cs.overall_compliance_pct >= 90 ? "bg-green-50" : "bg-red-50",
    },
    {
      label: "Ghost Providers Flagged",
      value: formatNumber(gs.total_flagged),
      sub: `${gs.high_severity} high / ${gs.medium_severity} medium / ${gs.low_severity} low`,
      icon: Ghost,
      color: "text-amber-600",
      bg: "bg-amber-50",
    },
    {
      label: "OON Leakage Cost",
      value: formatCurrency(stats.total_leakage_cost),
      sub: `${formatNumber(stats.total_oon_claims)} out-of-network claims`,
      icon: ArrowRightLeft,
      color: "text-red-600",
      bg: "bg-red-50",
    },
    {
      label: "Gap Members",
      value: formatNumber(cs.total_gap_members),
      sub: "Members without adequate in-network access",
      icon: Users,
      color: "text-purple-600",
      bg: "bg-purple-50",
    },
    {
      label: "Telehealth Credits",
      value: formatNumber(stats.telehealth_credits_applied),
      sub: "County-specialty combos with telehealth credit",
      icon: Wifi,
      color: "text-blue-600",
      bg: "bg-blue-50",
    },
    {
      label: "Ghost Impact",
      value: formatNumber(gs.total_impact_members),
      sub: "Members relying on flagged ghost providers",
      icon: Target,
      color: "text-orange-600",
      bg: "bg-orange-50",
    },
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark">Network Adequacy Dashboard</h2>

      {/* KPI cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {kpiCards.map((card) => {
          const Icon = card.icon;
          return (
            <div key={card.label} className="card p-6">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium text-gray-500">{card.label}</span>
                <div className={`${card.bg} p-2 rounded-lg`}>
                  <Icon className={`w-4 h-4 ${card.color}`} />
                </div>
              </div>
              <p className={`text-3xl font-bold ${card.color}`}>{card.value}</p>
              <p className="text-xs text-gray-400 mt-1">{card.sub}</p>
            </div>
          );
        })}
      </div>

      {/* Top Recruitment Targets */}
      {stats.top_recruitment_targets.length > 0 && (
        <div className="card p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-databricks-dark">
              Top Recruitment Targets
            </h3>
            <button
              onClick={() => onNavigate("gaps")}
              className="text-sm text-databricks-red hover:underline"
            >
              View all
            </button>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">NPI</th>
                  <th className="text-left py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">Specialty</th>
                  <th className="text-left py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">County</th>
                  <th className="text-right py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">Potential Savings</th>
                  <th className="text-right py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">Members</th>
                  <th className="text-right py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">Priority</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {stats.top_recruitment_targets.map((t) => (
                  <tr key={t.rendering_provider_npi} className="hover:bg-gray-50">
                    <td className="py-2.5 px-3 font-mono text-xs text-gray-700">{t.rendering_provider_npi}</td>
                    <td className="py-2.5 px-3 text-gray-700">{t.specialty ?? "—"}</td>
                    <td className="py-2.5 px-3 text-gray-700">{t.county_name ?? "—"}</td>
                    <td className="py-2.5 px-3 text-right text-green-700 font-semibold">
                      {formatCurrency(t.potential_savings)}
                    </td>
                    <td className="py-2.5 px-3 text-right text-gray-700">{formatNumber(t.members_served)}</td>
                    <td className="py-2.5 px-3 text-right">
                      <span className="inline-block bg-databricks-red text-white text-xs font-bold px-2 py-0.5 rounded-full">
                        {formatNumber(t.recruitment_priority_score)}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
