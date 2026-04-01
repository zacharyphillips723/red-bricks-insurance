import { useEffect, useState } from "react";
import {
  DollarSign,
  Users,
  TrendingUp,
  Activity,
  ArrowRight,
  Calculator,
  Shield,
  Layers,
  BarChart3,
  Target,
  Zap,
  PieChart,
  FileBarChart,
  Landmark,
  RefreshCw,
} from "lucide-react";
import { api, BaselineSummary, SimulationListItem } from "@/lib/api";
import {
  formatCurrency,
  formatPercent,
  formatNumber,
  formatDateTime,
  SIMULATION_TYPE_LABELS,
} from "@/lib/utils";

interface DashboardProps {
  onNavigateToBuilder: (simType?: string) => void;
}

const SIM_TYPE_CARDS: {
  type: string;
  label: string;
  desc: string;
  icon: React.ElementType;
}[] = [
  { type: "premium_rate", label: "Premium Rate", desc: "Adjust rates and see MLR impact", icon: DollarSign },
  { type: "benefit_design", label: "Benefit Design", desc: "Model deductible/copay changes", icon: Shield },
  { type: "group_renewal", label: "Group Renewal", desc: "Credibility-weighted pricing", icon: Layers },
  { type: "population_mix", label: "Population Mix", desc: "Enrollment shift impact", icon: Users },
  { type: "medical_trend", label: "Medical Trend", desc: "Project claims trajectory", icon: TrendingUp },
  { type: "stop_loss", label: "Stop-Loss", desc: "Threshold change analysis", icon: Target },
  { type: "risk_adjustment", label: "Risk Adjustment", desc: "RAF capture and revenue", icon: BarChart3 },
  { type: "utilization_change", label: "Utilization", desc: "Category-level changes", icon: Activity },
  { type: "new_group_quote", label: "New Group Quote", desc: "Peer-based pricing", icon: Calculator },
  { type: "ibnr_reserve", label: "IBNR Reserve", desc: "Completion factor shifts", icon: Landmark },
];

export default function Dashboard({ onNavigateToBuilder }: DashboardProps) {
  const [baseline, setBaseline] = useState<BaselineSummary | null>(null);
  const [recent, setRecent] = useState<SimulationListItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getBaseline().catch(() => null),
      api.listSimulations().catch(() => []),
    ]).then(([b, s]) => {
      setBaseline(b);
      setRecent(s.slice(0, 5));
      setLoading(false);
    });
  }, []);

  if (loading) {
    return (
      <div className="p-8 flex items-center justify-center h-full">
        <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
      </div>
    );
  }

  return (
    <div className="p-8 space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">
          Book Overview
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          Current-state financials across all lines of business
        </p>
      </div>

      {/* KPI Cards */}
      {baseline && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          <KpiCard
            label="Total Premium"
            value={formatCurrency(baseline.total_premium)}
            icon={DollarSign}
          />
          <KpiCard
            label="Total Claims"
            value={formatCurrency(baseline.total_claims)}
            icon={FileBarChart}
          />
          <KpiCard
            label="Overall MLR"
            value={formatPercent(baseline.overall_mlr)}
            icon={PieChart}
            highlight={baseline.overall_mlr > 85}
          />
          <KpiCard
            label="Total Members"
            value={formatNumber(baseline.total_members)}
            icon={Users}
          />
          <KpiCard
            label="Member Months"
            value={formatNumber(baseline.total_member_months)}
            icon={Zap}
          />
        </div>
      )}

      {/* LOB Breakdown */}
      {baseline && Object.keys(baseline.mlr_by_lob).length > 0 && (
        <div className="card">
          <h2 className="text-lg font-semibold text-databricks-dark mb-4">
            By Line of Business
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-2 pr-4 font-medium text-gray-500">LOB</th>
                  <th className="text-right py-2 px-4 font-medium text-gray-500">Members</th>
                  <th className="text-right py-2 px-4 font-medium text-gray-500">PMPM</th>
                  <th className="text-right py-2 pl-4 font-medium text-gray-500">MLR</th>
                </tr>
              </thead>
              <tbody>
                {Object.keys(baseline.mlr_by_lob).map((lob) => (
                  <tr key={lob} className="border-b border-gray-50">
                    <td className="py-2 pr-4 font-medium">{lob}</td>
                    <td className="text-right py-2 px-4">
                      {formatNumber(baseline.member_count_by_lob[lob] || 0)}
                    </td>
                    <td className="text-right py-2 px-4">
                      {formatCurrency(baseline.pmpm_by_lob[lob] || 0)}
                    </td>
                    <td className="text-right py-2 pl-4">
                      <span
                        className={
                          (baseline.mlr_by_lob[lob] || 0) > 85
                            ? "text-red-600 font-medium"
                            : ""
                        }
                      >
                        {formatPercent(baseline.mlr_by_lob[lob] || 0)}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Quick Simulate */}
      <div>
        <h2 className="text-lg font-semibold text-databricks-dark mb-4">
          Quick Simulate
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {SIM_TYPE_CARDS.map(({ type, label, desc, icon: Icon }) => (
            <button
              key={type}
              onClick={() => onNavigateToBuilder(type)}
              className="sim-type-card text-left group"
            >
              <Icon className="w-5 h-5 text-databricks-red mb-2" />
              <div className="font-medium text-sm text-databricks-dark">
                {label}
              </div>
              <div className="text-xs text-gray-500 mt-0.5">{desc}</div>
              <ArrowRight className="w-4 h-4 text-gray-300 mt-2 group-hover:text-databricks-red transition-colors" />
            </button>
          ))}
        </div>
      </div>

      {/* Recent Simulations */}
      {recent.length > 0 && (
        <div className="card">
          <h2 className="text-lg font-semibold text-databricks-dark mb-4">
            Recent Simulations
          </h2>
          <div className="space-y-2">
            {recent.map((sim) => (
              <div
                key={sim.simulation_id}
                className="flex items-center justify-between py-2 border-b border-gray-50 last:border-0"
              >
                <div>
                  <div className="font-medium text-sm">{sim.simulation_name}</div>
                  <div className="text-xs text-gray-500">
                    {SIMULATION_TYPE_LABELS[sim.simulation_type] || sim.simulation_type}
                    {sim.scope_lob && ` \u2022 ${sim.scope_lob}`}
                  </div>
                </div>
                <div className="text-xs text-gray-400">
                  {formatDateTime(sim.created_at)}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function KpiCard({
  label,
  value,
  icon: Icon,
  highlight,
}: {
  label: string;
  value: string;
  icon: React.ElementType;
  highlight?: boolean;
}) {
  return (
    <div className={`kpi-card ${highlight ? "ring-2 ring-red-200" : ""}`}>
      <div className="flex items-center gap-2 mb-2">
        <Icon className="w-4 h-4 text-gray-400" />
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">
          {label}
        </span>
      </div>
      <div
        className={`text-xl font-bold ${
          highlight ? "text-red-600" : "text-databricks-dark"
        }`}
      >
        {value}
      </div>
    </div>
  );
}
