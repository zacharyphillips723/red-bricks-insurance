import { useEffect, useState } from "react";
import {
  ShieldAlert,
  AlertTriangle,
  DollarSign,
  TrendingUp,
  CheckCircle2,
  FileSearch,
} from "lucide-react";
import { api, type DashboardStats } from "@/lib/api";
import { formatCurrency, formatPercent } from "@/lib/utils";

interface DashboardProps {
  onSelectInvestigation: (id: string) => void;
}

export function Dashboard({ onSelectInvestigation }: DashboardProps) {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDashboardStats().then(setStats).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">FWA Dashboard</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
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

  const statCards = [
    {
      label: "Total Investigations",
      value: stats.total_investigations,
      icon: FileSearch,
      color: "text-databricks-red",
      bg: "bg-red-50",
    },
    {
      label: "Open Cases",
      value: stats.open_count,
      icon: AlertTriangle,
      color: "text-amber-600",
      bg: "bg-amber-50",
    },
    {
      label: "Critical",
      value: stats.critical_count,
      icon: ShieldAlert,
      color: "text-red-700",
      bg: "bg-red-50",
    },
    {
      label: "Closed This Month",
      value: stats.closed_this_month,
      icon: CheckCircle2,
      color: "text-green-600",
      bg: "bg-green-50",
    },
  ];

  const financialCards = [
    {
      label: "Estimated Overpayment",
      value: formatCurrency(stats.total_estimated_overpayment),
      icon: DollarSign,
      color: "text-red-600",
    },
    {
      label: "Total Recovered",
      value: formatCurrency(stats.total_recovered),
      icon: TrendingUp,
      color: "text-green-600",
    },
    {
      label: "Recovery Rate",
      value: formatPercent(stats.recovery_rate),
      icon: CheckCircle2,
      color: "text-blue-600",
    },
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark">FWA Dashboard</h2>

      {/* Stat cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {statCards.map((card) => {
          const Icon = card.icon;
          return (
            <div key={card.label} className="card p-6">
              <div className="flex items-center justify-between mb-3">
                <span className="text-sm font-medium text-gray-500">{card.label}</span>
                <div className={`${card.bg} p-2 rounded-lg`}>
                  <Icon className={`w-4 h-4 ${card.color}`} />
                </div>
              </div>
              <p className="text-3xl font-bold text-databricks-dark">{card.value}</p>
            </div>
          );
        })}
      </div>

      {/* Financial cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {financialCards.map((card) => {
          const Icon = card.icon;
          return (
            <div key={card.label} className="card p-6">
              <div className="flex items-center gap-2 mb-2">
                <Icon className={`w-4 h-4 ${card.color}`} />
                <span className="text-sm font-medium text-gray-500">{card.label}</span>
              </div>
              <p className={`text-2xl font-bold ${card.color}`}>{card.value}</p>
            </div>
          );
        })}
      </div>

      {/* Breakdown cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* By Status */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By Status</h3>
          <div className="space-y-3">
            {Object.entries(stats.investigations_by_status).map(([status, count]) => (
              <div key={status} className="flex items-center justify-between">
                <span className="text-sm text-gray-600 truncate mr-2">{status}</span>
                <div className="flex items-center gap-3">
                  <div className="w-24 bg-gray-100 rounded-full h-2">
                    <div
                      className="bg-databricks-red h-2 rounded-full"
                      style={{ width: `${(count / stats.total_investigations) * 100}%` }}
                    />
                  </div>
                  <span className="text-sm font-semibold text-databricks-dark w-6 text-right">
                    {count}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* By Severity */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By Severity</h3>
          <div className="space-y-3">
            {Object.entries(stats.investigations_by_severity).map(([sev, count]) => {
              const color =
                sev === "Critical" ? "bg-red-500" :
                sev === "High" ? "bg-orange-500" :
                sev === "Medium" ? "bg-yellow-500" : "bg-green-500";
              return (
                <div key={sev} className="flex items-center justify-between">
                  <span className="text-sm text-gray-600">{sev}</span>
                  <div className="flex items-center gap-3">
                    <div className="w-24 bg-gray-100 rounded-full h-2">
                      <div
                        className={`${color} h-2 rounded-full`}
                        style={{ width: `${(count / stats.total_investigations) * 100}%` }}
                      />
                    </div>
                    <span className="text-sm font-semibold text-databricks-dark w-6 text-right">
                      {count}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* By Type */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By Type</h3>
          <div className="space-y-3">
            {Object.entries(stats.investigations_by_type).map(([type, count]) => (
              <div key={type} className="flex items-center justify-between">
                <span className="text-sm text-gray-600">{type}</span>
                <div className="flex items-center gap-3">
                  <div className="w-24 bg-gray-100 rounded-full h-2">
                    <div
                      className="bg-blue-500 h-2 rounded-full"
                      style={{ width: `${(count / stats.total_investigations) * 100}%` }}
                    />
                  </div>
                  <span className="text-sm font-semibold text-databricks-dark w-6 text-right">
                    {count}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
