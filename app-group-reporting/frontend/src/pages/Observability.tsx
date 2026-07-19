import { useState, useEffect, useCallback } from "react";
import { Activity, Loader2, DollarSign, Zap, RefreshCw } from "lucide-react";
import { api } from "@/lib/api";
import type { ObservabilityTrace, CostSummary } from "@/lib/api";

const MODEL_LABELS: Record<string, string> = {
  "databricks-llama-4-maverick": "Llama 4 Maverick",
};
const COST_PER_1K: Record<string, { input: number; output: number }> = {
  "databricks-llama-4-maverick": { input: 0.0004, output: 0.0016 },
};

export function Observability() {
  const [traces, setTraces] = useState<ObservabilityTrace[]>([]);
  const [costs, setCosts] = useState<CostSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      const [t, c] = await Promise.all([
        api.getTraces().catch(() => ({ traces: [] })),
        api.getCostSummary().catch(() => ({ costs: [] })),
      ]);
      setTraces(t.traces || []);
      setCosts(c.costs || []);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading) {
    return <div className="flex items-center justify-center h-64"><Loader2 className="w-8 h-8 text-databricks-red animate-spin" /></div>;
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Activity className="w-6 h-6 text-databricks-red" /> Observability
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          MLflow traces and model usage for the Sales Coach, captured in Unity Catalog OTel tables.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {(costs.length > 0 ? costs : [
          { endpoint: "databricks-llama-4-maverick", request_count: 0, total_input_tokens: 0, total_output_tokens: 0 },
        ]).map((c) => {
          const rates = COST_PER_1K[c.endpoint] || { input: 0, output: 0 };
          const inTok = Number(c.total_input_tokens || 0), outTok = Number(c.total_output_tokens || 0);
          const est = c.estimated_cost_usd != null ? Number(c.estimated_cost_usd) : (inTok / 1000) * rates.input + (outTok / 1000) * rates.output;
          const req = Number(c.request_count || 0);
          return (
            <div key={c.endpoint} className="card p-5">
              <div className="text-sm font-medium text-gray-500 mb-1">{MODEL_LABELS[c.endpoint] || c.endpoint}</div>
              <div className="grid grid-cols-2 gap-3 mt-3">
                <div className="flex items-center gap-2"><Zap className="w-4 h-4 text-amber-500" /><div><div className="text-lg font-bold">{req.toLocaleString()}</div><div className="text-xs text-gray-400">Requests (30d)</div></div></div>
                <div className="flex items-center gap-2"><DollarSign className="w-4 h-4 text-green-500" /><div><div className="text-lg font-bold">{est === 0 ? "—" : `$${est.toFixed(2)}`}</div><div className="text-xs text-gray-400">Est. Cost</div></div></div>
                <div className="flex items-center gap-2"><Activity className="w-4 h-4 text-blue-500" /><div><div className="text-lg font-bold">{(inTok + outTok).toLocaleString()}</div><div className="text-xs text-gray-400">Tokens</div></div></div>
                <div className="flex items-center gap-2"><DollarSign className="w-4 h-4 text-purple-500" /><div><div className="text-lg font-bold">{req ? `$${(est / req).toFixed(4)}` : "—"}</div><div className="text-xs text-gray-400">Per Request</div></div></div>
              </div>
            </div>
          );
        })}
      </div>

      <div className="card">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h3 className="font-semibold text-databricks-dark">Recent Traces</h3>
          <button onClick={() => load(true)} disabled={refreshing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? "animate-spin" : ""}`} />{refreshing ? "Refreshing..." : "Refresh"}
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50"><tr>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Trace ID</th>
              <th className="px-4 py-3 text-left font-medium text-gray-600">Status</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Spans</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Duration</th>
              <th className="px-4 py-3 text-right font-medium text-gray-600">Timestamp</th>
            </tr></thead>
            <tbody className="divide-y divide-gray-100">
              {traces.slice(0, 25).map((t, i) => (
                <tr key={i}>
                  <td className="px-4 py-3 font-mono text-xs">{String(t.request_id || "").slice(0, 16)}…</td>
                  <td className="px-4 py-3"><span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${t.status === "OK" ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"}`}>{String(t.status || "unknown")}</span></td>
                  <td className="px-4 py-3 text-right">{t.span_count || "—"}</td>
                  <td className="px-4 py-3 text-right">{t.execution_time_ms ? `${Math.round(Number(t.execution_time_ms))}ms` : "—"}</td>
                  <td className="px-4 py-3 text-right text-gray-500">{t.timestamp_ms ? new Date(Number(t.timestamp_ms)).toLocaleString() : "—"}</td>
                </tr>
              ))}
              {traces.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-8 text-center text-gray-400">No traces yet. Ask the Sales Coach a question to generate traces.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
