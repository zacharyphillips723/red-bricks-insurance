import { useState, useEffect } from "react";
import { api, DashboardStats } from "@/lib/api";
import {
  ClipboardList,
  Clock,
  CheckCircle,
  XCircle,
  AlertTriangle,
  Zap,
  Shield,
  Timer,
} from "lucide-react";

interface DashboardProps {
  onSelectRequest: (id: string) => void;
}

function formatPct(val: number | null): string {
  if (val === null) return "N/A";
  return `${(val * 100).toFixed(1)}%`;
}

export function Dashboard({ onSelectRequest: _onSelectRequest }: DashboardProps) {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getDashboardStats().then(setStats).catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">PA Operations Dashboard</h2>
        <div className="grid grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="card animate-pulse h-28 bg-gray-100" />
          ))}
        </div>
      </div>
    );
  }

  if (!stats) return <div className="text-red-600">Failed to load dashboard.</div>;

  const statCards = [
    { label: "Total Requests", value: stats.total_requests.toLocaleString(), icon: ClipboardList, color: "text-blue-600" },
    { label: "Pending Review", value: stats.pending_count.toLocaleString(), icon: Clock, color: "text-amber-600" },
    { label: "In Review", value: stats.in_review_count.toLocaleString(), icon: Timer, color: "text-indigo-600" },
    { label: "Expedited Pending", value: stats.expedited_pending.toLocaleString(), icon: Zap, color: "text-red-600" },
    { label: "Approved", value: stats.approved_count.toLocaleString(), icon: CheckCircle, color: "text-green-600" },
    { label: "Denied", value: stats.denied_count.toLocaleString(), icon: XCircle, color: "text-red-500" },
    { label: "Overdue (CMS)", value: stats.overdue_count.toLocaleString(), icon: AlertTriangle, color: stats.overdue_count > 0 ? "text-red-600" : "text-green-600" },
    { label: "Auto-Adjudicated", value: stats.auto_adjudicated_count.toLocaleString(), icon: Shield, color: "text-purple-600" },
  ];

  const kpiCards = [
    { label: "Approval Rate", value: formatPct(stats.approval_rate), good: stats.approval_rate !== null && stats.approval_rate > 0.7 },
    { label: "Avg Turnaround", value: stats.avg_turnaround_hours ? `${stats.avg_turnaround_hours}h` : "N/A", good: stats.avg_turnaround_hours !== null && stats.avg_turnaround_hours < 48 },
    { label: "CMS Compliance", value: stats.cms_compliance_rate ? `${stats.cms_compliance_rate}%` : "N/A", good: stats.cms_compliance_rate !== null && stats.cms_compliance_rate > 95 },
  ];

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark">PA Operations Dashboard</h2>

      {/* Stat cards */}
      <div className="grid grid-cols-4 gap-4">
        {statCards.map((c) => {
          const Icon = c.icon;
          return (
            <div key={c.label} className="card flex items-center gap-4">
              <div className={`p-2 rounded-lg bg-gray-50 ${c.color}`}>
                <Icon size={22} />
              </div>
              <div>
                <p className="text-2xl font-bold text-databricks-dark">{c.value}</p>
                <p className="text-xs text-gray-500">{c.label}</p>
              </div>
            </div>
          );
        })}
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-3 gap-4">
        {kpiCards.map((k) => (
          <div key={k.label} className="card">
            <p className="text-sm text-gray-500 mb-1">{k.label}</p>
            <p className={`text-3xl font-bold ${k.good ? "text-green-600" : "text-amber-600"}`}>{k.value}</p>
          </div>
        ))}
      </div>

      {/* Breakdown cards */}
      <div className="grid grid-cols-3 gap-4">
        <BreakdownCard title="By Status" data={stats.requests_by_status} total={stats.total_requests} />
        <BreakdownCard title="By Service Type" data={stats.requests_by_service_type} total={stats.total_requests} />
        <BreakdownCard title="By Urgency" data={stats.requests_by_urgency} total={stats.total_requests} />
      </div>
    </div>
  );
}

function BreakdownCard({ title, data, total }: { title: string; data: Record<string, number>; total: number }) {
  const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
  return (
    <div className="card">
      <h3 className="font-semibold text-databricks-dark mb-3">{title}</h3>
      <div className="space-y-2">
        {entries.map(([key, val]) => {
          const pct = total > 0 ? (val / total) * 100 : 0;
          return (
            <div key={key}>
              <div className="flex justify-between text-sm mb-1">
                <span className="text-gray-600 truncate">{key}</span>
                <span className="font-medium">{val.toLocaleString()}</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-1.5">
                <div
                  className="bg-databricks-red h-1.5 rounded-full"
                  style={{ width: `${Math.min(pct, 100)}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
