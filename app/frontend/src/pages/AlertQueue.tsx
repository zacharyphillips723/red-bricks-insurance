import { useEffect, useState } from "react";
import { Filter, ChevronRight } from "lucide-react";
import { api, type AlertListItem } from "@/lib/api";
import { riskBadgeClass, statusColor, formatDate, sourceIcon } from "@/lib/utils";

interface AlertQueueProps {
  onSelectAlert: (id: string) => void;
}

const RISK_TIERS = ["All", "Critical", "High", "Elevated", "Moderate", "Low"];
const SOURCES = ["All", "High Glucose No Insulin", "ED High Utilizer", "SDOH Risk", "Manual"];
const STATUSES = [
  "All", "Unassigned", "Assigned", "Outreach Attempted", "Outreach Successful",
  "Assessment In Progress", "Intervention Active", "Follow-Up Scheduled",
  "Resolved", "Escalated", "Closed — Unable to Reach",
];

export function AlertQueue({ onSelectAlert }: AlertQueueProps) {
  const [alerts, setAlerts] = useState<AlertListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [riskFilter, setRiskFilter] = useState("All");
  const [sourceFilter, setSourceFilter] = useState("All");
  const [statusFilter, setStatusFilter] = useState("All");
  const [showFilters, setShowFilters] = useState(false);

  useEffect(() => {
    const params: Record<string, string> = {};
    if (riskFilter !== "All") params.risk_tier = riskFilter;
    if (sourceFilter !== "All") params.alert_source = sourceFilter;
    if (statusFilter !== "All") params.status = statusFilter;

    setLoading(true);
    api.listAlerts(params).then(setAlerts).finally(() => setLoading(false));
  }, [riskFilter, sourceFilter, statusFilter]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-databricks-dark">Alert Queue</h2>
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-500">{alerts.length} alerts</span>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`btn-secondary flex items-center gap-2 text-sm ${
              showFilters ? "bg-gray-100" : ""
            }`}
          >
            <Filter className="w-4 h-4" />
            Filters
          </button>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="card p-4 flex flex-wrap gap-4">
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">Risk Tier</label>
            <select
              value={riskFilter}
              onChange={(e) => setRiskFilter(e.target.value)}
              className="mt-1 block w-full rounded-lg border-gray-300 text-sm py-2 px-3 border focus:ring-databricks-red focus:border-databricks-red"
            >
              {RISK_TIERS.map((t) => <option key={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">Source</label>
            <select
              value={sourceFilter}
              onChange={(e) => setSourceFilter(e.target.value)}
              className="mt-1 block w-full rounded-lg border-gray-300 text-sm py-2 px-3 border focus:ring-databricks-red focus:border-databricks-red"
            >
              {SOURCES.map((s) => <option key={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">Status</label>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="mt-1 block w-full rounded-lg border-gray-300 text-sm py-2 px-3 border focus:ring-databricks-red focus:border-databricks-red"
            >
              {STATUSES.map((s) => <option key={s}>{s}</option>)}
            </select>
          </div>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50/50">
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Risk</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Patient</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Primary Driver</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Source</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Status</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Assigned To</th>
                <th className="text-left py-3 px-4 font-medium text-gray-500 uppercase tracking-wide text-xs">Created</th>
                <th className="py-3 px-4"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {loading ? (
                [...Array(6)].map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {[...Array(8)].map((_, j) => (
                      <td key={j} className="py-3 px-4">
                        <div className="h-4 bg-gray-200 rounded w-20" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : alerts.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-12 text-center text-gray-400">
                    No alerts match your filters
                  </td>
                </tr>
              ) : (
                alerts.map((alert) => (
                  <tr
                    key={alert.alert_id}
                    onClick={() => onSelectAlert(alert.alert_id)}
                    className="hover:bg-gray-50 cursor-pointer transition-colors"
                  >
                    <td className="py-3 px-4">
                      <span className={riskBadgeClass(alert.risk_tier)}>
                        {alert.risk_tier}
                      </span>
                    </td>
                    <td className="py-3 px-4">
                      <div className="font-medium text-databricks-dark">
                        {alert.mrn || alert.patient_id.slice(0, 8)}
                      </div>
                      {alert.payer && (
                        <div className="text-xs text-gray-400">{alert.payer}</div>
                      )}
                    </td>
                    <td className="py-3 px-4 max-w-xs truncate text-gray-600">
                      {alert.primary_driver}
                    </td>
                    <td className="py-3 px-4 whitespace-nowrap">
                      <span className="text-base mr-1">{sourceIcon(alert.alert_source)}</span>
                      <span className="text-gray-600">{alert.alert_source}</span>
                    </td>
                    <td className="py-3 px-4">
                      <span className={`font-medium ${statusColor(alert.status)}`}>
                        {alert.status}
                      </span>
                      {alert.time_unassigned && (
                        <div className="text-xs text-gray-400">{alert.time_unassigned}</div>
                      )}
                    </td>
                    <td className="py-3 px-4 text-gray-600">
                      {alert.care_manager_name ? (
                        <div>
                          <div>{alert.care_manager_name}</div>
                          <div className="text-xs text-gray-400">{alert.care_manager_role}</div>
                        </div>
                      ) : (
                        <span className="text-gray-300">—</span>
                      )}
                    </td>
                    <td className="py-3 px-4 text-gray-500 whitespace-nowrap">
                      {formatDate(alert.created_at)}
                    </td>
                    <td className="py-3 px-4">
                      <ChevronRight className="w-4 h-4 text-gray-300" />
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
