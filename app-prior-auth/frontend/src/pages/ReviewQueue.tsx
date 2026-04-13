import { useState, useEffect } from "react";
import { api, PARequestListItem } from "@/lib/api";
import { ChevronRight, AlertTriangle, Zap, Search } from "lucide-react";

interface ReviewQueueProps {
  onSelectRequest: (id: string) => void;
}

function urgencyBadge(urgency: string | null): string {
  switch (urgency) {
    case "expedited": return "bg-red-100 text-red-800";
    case "standard": return "bg-blue-100 text-blue-800";
    case "retrospective": return "bg-gray-100 text-gray-800";
    default: return "bg-gray-100 text-gray-600";
  }
}

function statusColor(status: string | null): string {
  if (!status) return "text-gray-500";
  if (status === "Approved" || status === "Appeal Overturned") return "text-green-700 bg-green-50";
  if (status === "Denied" || status === "Appeal Upheld") return "text-red-700 bg-red-50";
  if (status === "Pending Review") return "text-amber-700 bg-amber-50";
  if (status === "In Review") return "text-blue-700 bg-blue-50";
  if (status === "Additional Info Requested") return "text-orange-700 bg-orange-50";
  return "text-gray-700 bg-gray-50";
}

function formatDate(d: string | null): string {
  if (!d) return "";
  return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function formatCurrency(val: number | null): string {
  if (val === null || val === undefined) return "";
  return `$${val.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
}

export function ReviewQueue({ onSelectRequest }: ReviewQueueProps) {
  const [requests, setRequests] = useState<PARequestListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState("");
  const [filterUrgency, setFilterUrgency] = useState("");
  const [filterService, setFilterService] = useState("");
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    const params: Record<string, string> = {};
    if (filterStatus) params.status = filterStatus;
    if (filterUrgency) params.urgency = filterUrgency;
    if (filterService) params.service_type = filterService;
    setLoading(true);
    api.listRequests(params).then(setRequests).catch(console.error).finally(() => setLoading(false));
  }, [filterStatus, filterUrgency, filterService]);

  const filtered = searchTerm
    ? requests.filter((r) =>
        [r.auth_request_id, r.member_name, r.provider_name, r.procedure_code, r.service_type]
          .filter(Boolean)
          .some((v) => v!.toLowerCase().includes(searchTerm.toLowerCase()))
      )
    : requests;

  const statuses = ["Pending Review", "In Review", "Additional Info Requested", "Approved", "Denied", "Partially Approved", "Peer Review Requested"];
  const urgencies = ["expedited", "standard", "retrospective"];
  const serviceTypes = [...new Set(requests.map((r) => r.service_type))].sort();

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-databricks-dark">Review Queue</h2>
        <span className="text-sm text-gray-500">{filtered.length} requests</span>
      </div>

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <div className="relative flex-1 max-w-sm">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search by ID, member, provider, procedure..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/20"
          />
        </div>
        <select value={filterStatus} onChange={(e) => setFilterStatus(e.target.value)} className="border border-gray-300 rounded-md px-3 py-2 text-sm">
          <option value="">All Statuses</option>
          {statuses.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select value={filterUrgency} onChange={(e) => setFilterUrgency(e.target.value)} className="border border-gray-300 rounded-md px-3 py-2 text-sm">
          <option value="">All Urgencies</option>
          {urgencies.map((u) => <option key={u} value={u}>{u}</option>)}
        </select>
        <select value={filterService} onChange={(e) => setFilterService(e.target.value)} className="border border-gray-300 rounded-md px-3 py-2 text-sm">
          <option value="">All Services</option>
          {serviceTypes.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      {/* Table */}
      <div className="card p-0 overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400">Loading...</div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <th className="px-4 py-3">ID</th>
                <th className="px-4 py-3">Member</th>
                <th className="px-4 py-3">Service</th>
                <th className="px-4 py-3">Procedure</th>
                <th className="px-4 py-3">Urgency</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">AI</th>
                <th className="px-4 py-3">Cost</th>
                <th className="px-4 py-3">Reviewer</th>
                <th className="px-4 py-3">Deadline</th>
                <th className="px-4 py-3"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filtered.map((r) => {
                const openStatuses = ["Pending Review", "In Review", "Additional Info Requested", "Peer Review Requested"];
                const isOpen = openStatuses.includes(r.status || "");
                const hoursLeft = r.hours_until_deadline;
                const isOverdue = isOpen && hoursLeft !== null && hoursLeft < 0;
                const isUrgent = isOpen && hoursLeft !== null && hoursLeft < 24 && hoursLeft >= 0;
                return (
                  <tr
                    key={r.auth_request_id}
                    onClick={() => onSelectRequest(r.auth_request_id)}
                    className="hover:bg-gray-50 cursor-pointer"
                  >
                    <td className="px-4 py-3 font-mono text-xs">{r.auth_request_id.slice(0, 12)}...</td>
                    <td className="px-4 py-3">
                      <div className="font-medium">{r.member_name || r.member_id}</div>
                      <div className="text-xs text-gray-400">{r.provider_name}</div>
                    </td>
                    <td className="px-4 py-3">{r.service_type}</td>
                    <td className="px-4 py-3 font-mono text-xs">{r.procedure_code}</td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${urgencyBadge(r.urgency)}`}>
                        {r.urgency === "expedited" && <Zap size={10} className="inline mr-1" />}
                        {r.urgency}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${statusColor(r.status)}`}>
                        {r.status}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      {r.ai_confidence !== null && (
                        <div className="flex items-center gap-1">
                          <div className="w-10 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                            <div
                              className={`h-full rounded-full ${
                                r.ai_confidence > 0.8 ? "bg-green-500" : r.ai_confidence > 0.6 ? "bg-amber-500" : "bg-red-500"
                              }`}
                              style={{ width: `${(r.ai_confidence || 0) * 100}%` }}
                            />
                          </div>
                          <span className="text-xs text-gray-400">{((r.ai_confidence || 0) * 100).toFixed(0)}%</span>
                        </div>
                      )}
                    </td>
                    <td className="px-4 py-3 text-right">{formatCurrency(r.estimated_cost)}</td>
                    <td className="px-4 py-3 text-xs">{r.reviewer_name || <span className="text-gray-300">Unassigned</span>}</td>
                    <td className="px-4 py-3">
                      {isOpen ? (
                        <div className={`flex items-center gap-1 text-xs ${isOverdue ? "text-red-600 font-semibold" : isUrgent ? "text-amber-600" : "text-gray-500"}`}>
                          {isOverdue && <AlertTriangle size={12} />}
                          {hoursLeft !== null
                            ? isOverdue
                              ? `${Math.abs(Math.round(hoursLeft))}h overdue`
                              : `${Math.round(hoursLeft)}h left`
                            : formatDate(r.cms_deadline)}
                        </div>
                      ) : (
                        <span className={`text-xs ${r.cms_compliant ? "text-green-600" : "text-red-600"}`}>
                          {r.cms_compliant ? "Met" : "Missed"}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <ChevronRight size={16} className="text-gray-300" />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
