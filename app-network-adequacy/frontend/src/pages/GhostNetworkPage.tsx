import { useEffect, useState } from "react";
import { Ghost, Filter, AlertTriangle, Clock, UserX, ShieldOff, Users } from "lucide-react";
import { api, type GhostProviderRow } from "@/lib/api";
import { formatNumber, severityBadgeClass } from "@/lib/utils";

export function GhostNetworkPage() {
  const [rows, setRows] = useState<GhostProviderRow[]>([]);
  const [loading, setLoading] = useState(true);

  const [filterSeverity, setFilterSeverity] = useState("");
  const [filterFlagged, setFilterFlagged] = useState("true");

  useEffect(() => {
    setLoading(true);
    const params: Record<string, string> = {};
    if (filterSeverity) params.severity = filterSeverity;
    if (filterFlagged) params.flagged_only = filterFlagged;
    api.getGhostProviders(params).then(setRows).finally(() => setLoading(false));
  }, [filterSeverity, filterFlagged]);

  const signalIcon = (label: string) => {
    switch (label) {
      case "No claims 12m": return <Clock className="w-3.5 h-3.5" />;
      case "Not accepting": return <UserX className="w-3.5 h-3.5" />;
      case "Extreme wait": return <AlertTriangle className="w-3.5 h-3.5" />;
      case "Credential expired": return <ShieldOff className="w-3.5 h-3.5" />;
      case "Panel full": return <Users className="w-3.5 h-3.5" />;
      default: return null;
    }
  };

  const getSignals = (r: GhostProviderRow): string[] => {
    const signals: string[] = [];
    if (r.no_claims_12m) signals.push("No claims 12m");
    if (r.not_accepting) signals.push("Not accepting");
    if (r.extreme_wait) signals.push("Extreme wait");
    if (r.credential_expired) signals.push("Credential expired");
    if (r.panel_full) signals.push("Panel full");
    return signals;
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Ghost className="w-6 h-6 text-databricks-red" />
          Ghost Network Detection
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Providers listed in the directory but effectively unavailable to members
        </p>
      </div>

      {/* Filters */}
      <div className="card p-4">
        <div className="flex items-center gap-4 flex-wrap">
          <Filter className="w-4 h-4 text-gray-400" />
          <select
            value={filterSeverity}
            onChange={(e) => setFilterSeverity(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value="">All Severities</option>
            <option value="High">High</option>
            <option value="Medium">Medium</option>
            <option value="Low">Low</option>
          </select>
          <select
            value={filterFlagged}
            onChange={(e) => setFilterFlagged(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value="true">Flagged Only</option>
            <option value="">All Providers</option>
          </select>
          {(filterSeverity || filterFlagged !== "true") && (
            <button
              onClick={() => { setFilterSeverity(""); setFilterFlagged("true"); }}
              className="text-sm text-databricks-red hover:underline"
            >
              Reset
            </button>
          )}
          <span className="ml-auto text-sm text-gray-400">
            {formatNumber(rows.length)} providers
          </span>
        </div>
      </div>

      {/* Cards */}
      {loading ? (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="card p-6 animate-pulse">
              <div className="h-4 bg-gray-200 rounded w-48 mb-3" />
              <div className="h-3 bg-gray-200 rounded w-32" />
            </div>
          ))}
        </div>
      ) : rows.length === 0 ? (
        <div className="card p-8 text-center text-gray-400">
          No ghost providers match your filters.
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {rows.map((r) => {
            const signals = getSignals(r);
            return (
              <div key={r.npi} className="card p-5">
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <p className="font-semibold text-databricks-dark">{r.provider_name}</p>
                    <p className="text-xs text-gray-500">
                      NPI: {r.npi} &middot; {r.specialty} &middot; {r.county}
                    </p>
                  </div>
                  <span className={severityBadgeClass(r.ghost_severity)}>
                    {r.ghost_severity}
                  </span>
                </div>

                {/* Signals */}
                <div className="flex flex-wrap gap-2 mb-3">
                  {signals.map((s) => (
                    <span
                      key={s}
                      className="inline-flex items-center gap-1 bg-gray-100 text-gray-700 text-xs px-2 py-1 rounded-md"
                    >
                      {signalIcon(s)} {s}
                    </span>
                  ))}
                </div>

                {/* Details */}
                <div className="grid grid-cols-3 gap-3 text-xs">
                  <div>
                    <span className="text-gray-400 block">Impact</span>
                    <span className="font-semibold text-gray-700">
                      {formatNumber(r.impact_members)} members
                    </span>
                  </div>
                  <div>
                    <span className="text-gray-400 block">Panel</span>
                    <span className="font-semibold text-gray-700">
                      {r.panel_size ?? "—"} / {r.panel_capacity ?? "—"}
                    </span>
                  </div>
                  <div>
                    <span className="text-gray-400 block">Wait</span>
                    <span className="font-semibold text-gray-700">
                      {r.appointment_wait_days != null ? `${r.appointment_wait_days}d` : "—"}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
