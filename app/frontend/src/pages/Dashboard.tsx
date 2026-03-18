import { useEffect, useState } from "react";
import { AlertTriangle, Clock, CheckCircle2, Users, TrendingUp, ShieldAlert } from "lucide-react";
import { api, type DashboardStats } from "@/lib/api";

export function Dashboard() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDashboardStats().then(setStats).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">Dashboard</h2>
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
      label: "Total Alerts",
      value: stats.total_alerts,
      icon: AlertTriangle,
      color: "text-databricks-red",
      bg: "bg-red-50",
    },
    {
      label: "Unassigned",
      value: stats.unassigned_count,
      icon: Clock,
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
      label: "Resolved This Month",
      value: stats.resolved_this_month,
      icon: CheckCircle2,
      color: "text-green-600",
      bg: "bg-green-50",
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-databricks-dark">Dashboard</h2>
        {stats.avg_time_to_assign_hours && (
          <div className="flex items-center gap-2 text-sm text-gray-500">
            <TrendingUp className="w-4 h-4" />
            Avg. time to assign: {stats.avg_time_to_assign_hours}h
          </div>
        )}
      </div>

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

      {/* Breakdown cards */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* By Source */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">Alerts by Source</h3>
          <div className="space-y-3">
            {Object.entries(stats.alerts_by_source).map(([source, count]) => (
              <div key={source} className="flex items-center justify-between">
                <span className="text-sm text-gray-600">{source}</span>
                <div className="flex items-center gap-3">
                  <div className="w-32 bg-gray-100 rounded-full h-2">
                    <div
                      className="bg-databricks-red h-2 rounded-full"
                      style={{
                        width: `${(count / stats.total_alerts) * 100}%`,
                      }}
                    />
                  </div>
                  <span className="text-sm font-semibold text-databricks-dark w-8 text-right">
                    {count}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* By Status */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">Alerts by Status</h3>
          <div className="space-y-3">
            {Object.entries(stats.alerts_by_status).map(([status, count]) => (
              <div key={status} className="flex items-center justify-between">
                <span className="text-sm text-gray-600">{status}</span>
                <div className="flex items-center gap-3">
                  <div className="w-32 bg-gray-100 rounded-full h-2">
                    <div
                      className="bg-blue-500 h-2 rounded-full"
                      style={{
                        width: `${(count / stats.total_alerts) * 100}%`,
                      }}
                    />
                  </div>
                  <span className="text-sm font-semibold text-databricks-dark w-8 text-right">
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
