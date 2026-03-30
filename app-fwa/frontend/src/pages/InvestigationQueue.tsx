import { useEffect, useState } from "react";
import { Filter, Search, ChevronRight } from "lucide-react";
import { api, type InvestigationListItem } from "@/lib/api";
import { formatCurrency, formatDate, severityBadgeClass, statusColor } from "@/lib/utils";

interface InvestigationQueueProps {
  onSelectInvestigation: (id: string) => void;
}

const STATUSES = [
  "Open",
  "Under Review",
  "Evidence Gathering",
  "Referred to SIU",
  "Recovery In Progress",
];

const SEVERITIES = ["Critical", "High", "Medium", "Low"];
const TYPES = ["Provider", "Member", "Network"];

export function InvestigationQueue({ onSelectInvestigation }: InvestigationQueueProps) {
  const [investigations, setInvestigations] = useState<InvestigationListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [searchText, setSearchText] = useState("");

  const loadData = () => {
    setLoading(true);
    const params: Record<string, string> = {};
    if (statusFilter) params.status = statusFilter;
    if (severityFilter) params.severity = severityFilter;
    if (typeFilter) params.investigation_type = typeFilter;
    api.listInvestigations(params).then(setInvestigations).finally(() => setLoading(false));
  };

  useEffect(() => {
    loadData();
  }, [statusFilter, severityFilter, typeFilter]);

  const filtered = investigations.filter((inv) => {
    if (!searchText) return true;
    const q = searchText.toLowerCase();
    return (
      inv.investigation_id.toLowerCase().includes(q) ||
      (inv.target_name || "").toLowerCase().includes(q) ||
      (inv.target_id || "").toLowerCase().includes(q) ||
      (inv.fraud_types || []).some((t) => t.toLowerCase().includes(q))
    );
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-databricks-dark">Investigation Queue</h2>
        <span className="text-sm text-gray-500">{filtered.length} investigations</span>
      </div>

      {/* Filters */}
      <div className="card p-4">
        <div className="flex flex-wrap items-center gap-3">
          <Filter className="w-4 h-4 text-gray-400" />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="text-sm border border-gray-300 rounded-lg px-3 py-1.5"
          >
            <option value="">All Statuses</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select
            value={severityFilter}
            onChange={(e) => setSeverityFilter(e.target.value)}
            className="text-sm border border-gray-300 rounded-lg px-3 py-1.5"
          >
            <option value="">All Severities</option>
            {SEVERITIES.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
            className="text-sm border border-gray-300 rounded-lg px-3 py-1.5"
          >
            <option value="">All Types</option>
            {TYPES.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
          <div className="flex-1" />
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              placeholder="Search ID, target, fraud type..."
              className="pl-9 pr-3 py-1.5 text-sm border border-gray-300 rounded-lg w-64"
            />
          </div>
        </div>
      </div>

      {/* Table */}
      {loading ? (
        <div className="card p-8 text-center text-gray-500">Loading investigations...</div>
      ) : (
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b">
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">ID</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Target</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Fraud Types</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Severity</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Status</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Risk Score</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase">Est. Overpayment</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Investigator</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase">Opened</th>
                  <th className="w-8"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filtered.map((inv) => (
                  <tr
                    key={inv.investigation_id}
                    onClick={() => onSelectInvestigation(inv.investigation_id)}
                    className="hover:bg-gray-50 cursor-pointer transition-colors"
                  >
                    <td className="py-3 px-4 font-mono text-xs font-semibold text-databricks-dark">
                      {inv.investigation_id}
                    </td>
                    <td className="py-3 px-4">
                      <div className="font-medium text-databricks-dark">{inv.target_name || inv.target_id}</div>
                      <div className="text-xs text-gray-400">{inv.investigation_type} - {inv.target_type}</div>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex flex-wrap gap-1">
                        {(inv.fraud_types || []).slice(0, 2).map((ft) => (
                          <span key={ft} className="bg-gray-100 text-gray-600 text-xs px-2 py-0.5 rounded">
                            {ft}
                          </span>
                        ))}
                        {(inv.fraud_types || []).length > 2 && (
                          <span className="text-xs text-gray-400">+{inv.fraud_types.length - 2}</span>
                        )}
                      </div>
                    </td>
                    <td className="py-3 px-4">
                      <span className={severityBadgeClass(inv.severity)}>{inv.severity}</span>
                    </td>
                    <td className="py-3 px-4">
                      <span className={`text-sm font-medium ${statusColor(inv.status)}`}>
                        {inv.status}
                      </span>
                    </td>
                    <td className="py-3 px-4">
                      {inv.composite_risk_score != null ? (
                        <div className="flex items-center gap-2">
                          <div className="w-12 bg-gray-100 rounded-full h-1.5">
                            <div
                              className={`h-1.5 rounded-full ${
                                inv.composite_risk_score > 0.7 ? "bg-red-500" :
                                inv.composite_risk_score > 0.4 ? "bg-amber-500" : "bg-green-500"
                              }`}
                              style={{ width: `${inv.composite_risk_score * 100}%` }}
                            />
                          </div>
                          <span className="text-xs text-gray-500">
                            {(inv.composite_risk_score * 100).toFixed(0)}
                          </span>
                        </div>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                    </td>
                    <td className="py-3 px-4 text-right font-medium">
                      {formatCurrency(inv.estimated_overpayment)}
                    </td>
                    <td className="py-3 px-4">
                      {inv.investigator_name ? (
                        <div>
                          <div className="text-sm">{inv.investigator_name}</div>
                          <div className="text-xs text-gray-400">{inv.investigator_role}</div>
                        </div>
                      ) : (
                        <span className="text-xs text-amber-600 font-medium">Unassigned</span>
                      )}
                    </td>
                    <td className="py-3 px-4 text-xs text-gray-500">
                      {formatDate(inv.created_at)}
                    </td>
                    <td className="py-3 px-4">
                      <ChevronRight className="w-4 h-4 text-gray-300" />
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
