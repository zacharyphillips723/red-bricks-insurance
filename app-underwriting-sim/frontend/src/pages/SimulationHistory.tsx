import { useState, useEffect } from "react";
import {
  History,
  Filter,
  ChevronDown,
  ChevronUp,
  Trash2,
  CheckCircle,
  Archive,
  RefreshCw,
} from "lucide-react";
import { api, SimulationListItem, SimulationDetail, AuditEntry } from "@/lib/api";
import {
  formatDateTime,
  statusBadgeClass,
  SIMULATION_TYPE_LABELS,
} from "@/lib/utils";

interface SimulationHistoryProps {
  onCountChange: (count: number) => void;
}

export default function SimulationHistory({ onCountChange }: SimulationHistoryProps) {
  const [simulations, setSimulations] = useState<SimulationListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<SimulationDetail | null>(null);
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [filterType, setFilterType] = useState("");
  const [filterStatus, setFilterStatus] = useState("");

  const load = () => {
    setLoading(true);
    api
      .listSimulations({
        simulation_type: filterType || undefined,
        status: filterStatus || undefined,
      })
      .then((sims) => {
        setSimulations(sims);
        onCountChange(sims.length);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, [filterType, filterStatus]);

  const toggleExpand = async (id: string) => {
    if (expandedId === id) {
      setExpandedId(null);
      setDetail(null);
      setAudit([]);
      return;
    }
    setExpandedId(id);
    try {
      const [d, a] = await Promise.all([
        api.getSimulation(id),
        api.getAuditLog(id),
      ]);
      setDetail(d);
      setAudit(a);
    } catch (err) {
      console.error(err);
    }
  };

  const handleApprove = async (id: string) => {
    await api.updateSimulation(id, { status: "approved" });
    load();
  };

  const handleArchive = async (id: string) => {
    await api.updateSimulation(id, { status: "archived" });
    load();
  };

  const handleDelete = async (id: string) => {
    if (!confirm("Delete this simulation?")) return;
    await api.deleteSimulation(id);
    setExpandedId(null);
    load();
  };

  return (
    <div className="p-8 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
            <History className="w-6 h-6" />
            Simulation History
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            {simulations.length} saved simulations
          </p>
        </div>
        <button onClick={load} className="btn-secondary flex items-center gap-2 text-sm">
          <RefreshCw className="w-4 h-4" /> Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <Filter className="w-4 h-4 text-gray-400" />
        <select
          value={filterType}
          onChange={(e) => setFilterType(e.target.value)}
          className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm"
        >
          <option value="">All Types</option>
          {Object.entries(SIMULATION_TYPE_LABELS).map(([k, v]) => (
            <option key={k} value={k}>
              {v}
            </option>
          ))}
        </select>
        <select
          value={filterStatus}
          onChange={(e) => setFilterStatus(e.target.value)}
          className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm"
        >
          <option value="">All Statuses</option>
          <option value="computed">Computed</option>
          <option value="approved">Approved</option>
          <option value="archived">Archived</option>
        </select>
      </div>

      {/* List */}
      {loading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
        </div>
      ) : simulations.length === 0 ? (
        <div className="card text-center text-gray-500 py-12">
          No simulations found. Run and save simulations in the Builder.
        </div>
      ) : (
        <div className="space-y-2">
          {simulations.map((sim) => (
            <div key={sim.simulation_id} className="card !p-0 overflow-hidden">
              <button
                onClick={() => toggleExpand(sim.simulation_id)}
                className="w-full flex items-center justify-between p-4 text-left hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center gap-4">
                  <div>
                    <div className="font-medium text-sm">{sim.simulation_name}</div>
                    <div className="text-xs text-gray-500 mt-0.5">
                      {SIMULATION_TYPE_LABELS[sim.simulation_type] || sim.simulation_type}
                      {sim.scope_lob && ` \u2022 ${sim.scope_lob}`}
                      {sim.scope_group_id && ` \u2022 ${sim.scope_group_id}`}
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <span className={statusBadgeClass(sim.status)}>{sim.status}</span>
                  <span className="text-xs text-gray-400">
                    {formatDateTime(sim.created_at)}
                  </span>
                  {expandedId === sim.simulation_id ? (
                    <ChevronUp className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  )}
                </div>
              </button>

              {/* Expanded Detail */}
              {expandedId === sim.simulation_id && detail && (
                <div className="border-t border-gray-100 p-4 bg-gray-50 space-y-4">
                  {/* Narrative */}
                  {sim.narrative && (
                    <div>
                      <h4 className="text-xs font-medium text-gray-500 uppercase mb-1">
                        Narrative
                      </h4>
                      <p className="text-sm text-gray-700">{sim.narrative}</p>
                    </div>
                  )}

                  {/* Parameters */}
                  <div>
                    <h4 className="text-xs font-medium text-gray-500 uppercase mb-1">
                      Parameters
                    </h4>
                    <pre className="text-xs bg-white rounded p-3 overflow-x-auto border">
                      {JSON.stringify(detail.parameters, null, 2)}
                    </pre>
                  </div>

                  {/* Audit Trail */}
                  {audit.length > 0 && (
                    <div>
                      <h4 className="text-xs font-medium text-gray-500 uppercase mb-1">
                        Audit Trail
                      </h4>
                      <div className="space-y-1">
                        {audit.map((entry) => (
                          <div
                            key={entry.audit_id}
                            className="flex items-center gap-3 text-xs text-gray-600"
                          >
                            <span className="text-gray-400 w-32 flex-shrink-0">
                              {formatDateTime(entry.created_at)}
                            </span>
                            <span className="font-medium">{entry.action}</span>
                            <span className="text-gray-400">by {entry.actor}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Actions */}
                  <div className="flex gap-2 pt-2 border-t border-gray-200">
                    {detail.status === "computed" && (
                      <button
                        onClick={() => handleApprove(sim.simulation_id)}
                        className="btn-secondary text-xs flex items-center gap-1"
                      >
                        <CheckCircle className="w-3.5 h-3.5" /> Approve
                      </button>
                    )}
                    {detail.status !== "archived" && (
                      <button
                        onClick={() => handleArchive(sim.simulation_id)}
                        className="btn-secondary text-xs flex items-center gap-1"
                      >
                        <Archive className="w-3.5 h-3.5" /> Archive
                      </button>
                    )}
                    <button
                      onClick={() => handleDelete(sim.simulation_id)}
                      className="text-xs text-red-500 hover:text-red-700 flex items-center gap-1 px-3 py-1.5"
                    >
                      <Trash2 className="w-3.5 h-3.5" /> Delete
                    </button>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
