import { useState, useEffect, useCallback } from "react";
import { Activity, Loader2, DollarSign, Zap, RefreshCw, ShieldCheck, Target } from "lucide-react";
import { api } from "@/lib/api";
import type { ObservabilityTrace, CostSummary, AIQuality } from "@/lib/api";

const MODEL_LABELS: Record<string, string> = {
  "databricks-llama-4-maverick": "Llama 4 Maverick",
  "databricks-claude-haiku-4-5": "Claude Haiku 4.5",
};

// FMAPI pay-per-token pricing per 1K tokens.
const COST_PER_1K: Record<string, { input: number; output: number }> = {
  "databricks-llama-4-maverick": { input: 0.0004, output: 0.0016 },
  "databricks-claude-haiku-4-5": { input: 0.001, output: 0.005 },
};

export function Observability() {
  const [traces, setTraces] = useState<ObservabilityTrace[]>([]);
  const [costs, setCosts] = useState<CostSummary[]>([]);
  const [quality, setQuality] = useState<AIQuality | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const loadData = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      const [traceRes, costRes, qualityRes] = await Promise.all([
        api.getTraces().catch(() => ({ traces: [] })),
        api.getCostSummary().catch(() => ({ costs: [] })),
        api.getAIQuality().catch(() => null),
      ]);
      setTraces(traceRes.traces || []);
      setCosts(costRes.costs || []);
      setQuality(qualityRes);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Activity className="w-6 h-6 text-databricks-red" /> Observability
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          MLflow traces and model usage for the PA review agent and document
          auto-adjudication, captured in Unity Catalog OTel tables.
        </p>
      </div>

      {/* Cost summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {(costs.length > 0 ? costs : [
          { endpoint: "databricks-claude-haiku-4-5", request_count: 0, total_input_tokens: 0, total_output_tokens: 0 },
          { endpoint: "databricks-llama-4-maverick", request_count: 0, total_input_tokens: 0, total_output_tokens: 0 },
        ]).map((c) => {
          const rates = COST_PER_1K[c.endpoint] || { input: 0, output: 0 };
          const inputTokens = Number(c.total_input_tokens || 0);
          const outputTokens = Number(c.total_output_tokens || 0);
          const estCost = c.estimated_cost_usd != null
            ? Number(c.estimated_cost_usd)
            : (inputTokens / 1000) * rates.input + (outputTokens / 1000) * rates.output;
          const totalTokens = inputTokens + outputTokens;
          const reqCount = Number(c.request_count || 0);
          const costPerReq = reqCount ? estCost / reqCount : 0;
          return (
            <div key={c.endpoint} className="card p-5">
              <div className="text-sm font-medium text-gray-500 mb-1">
                {MODEL_LABELS[c.endpoint] || c.endpoint}
              </div>
              <div className="grid grid-cols-2 gap-3 mt-3">
                <div className="flex items-center gap-2">
                  <Zap className="w-4 h-4 text-amber-500" />
                  <div>
                    <div className="text-lg font-bold">{reqCount.toLocaleString()}</div>
                    <div className="text-xs text-gray-400">Requests (30d)</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <DollarSign className="w-4 h-4 text-green-500" />
                  <div>
                    <div className="text-lg font-bold">{estCost === 0 ? "—" : `$${estCost.toFixed(2)}`}</div>
                    <div className="text-xs text-gray-400">Est. Cost</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Activity className="w-4 h-4 text-blue-500" />
                  <div>
                    <div className="text-lg font-bold">{totalTokens.toLocaleString()}</div>
                    <div className="text-xs text-gray-400">Tokens</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <DollarSign className="w-4 h-4 text-purple-500" />
                  <div>
                    <div className="text-lg font-bold">{costPerReq > 0 ? `$${costPerReq.toFixed(4)}` : "—"}</div>
                    <div className="text-xs text-gray-400">Per Request</div>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* AI Quality & Accuracy (RFI: AI & Advanced Intelligence — accuracy rate + eval) */}
      {quality && (
        <div className="card">
          <div className="p-4 border-b border-gray-200 flex items-center gap-2">
            <ShieldCheck className="w-5 h-5 text-databricks-red" />
            <h3 className="font-semibold text-databricks-dark">AI Quality & Accuracy</h3>
            <span className="text-xs text-gray-400 ml-1">
              measured over {quality.evaluated_count.toLocaleString()} evaluations
            </span>
          </div>
          <div className="p-4 space-y-4">
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
              <div className="flex items-center gap-2">
                <Target className="w-5 h-5 text-green-600" />
                <div>
                  <div className="text-2xl font-bold text-databricks-dark">
                    {quality.overall_accuracy_pct != null ? `${quality.overall_accuracy_pct}%` : "—"}
                  </div>
                  <div className="text-xs text-gray-400">Determination agreement</div>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Activity className="w-5 h-5 text-amber-500" />
                <div>
                  <div className="text-2xl font-bold text-databricks-dark">
                    {quality.overall_overturn_rate_pct != null ? `${quality.overall_overturn_rate_pct}%` : "—"}
                  </div>
                  <div className="text-xs text-gray-400">Appeal overturn (error signal)</div>
                </div>
              </div>
            </div>

            {/* Per-tier accuracy */}
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-3 py-2 text-left font-medium text-gray-600">Tier</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-600">Evaluated</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-600">Accuracy</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-600">Overturn Rate</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {quality.by_tier.map((t) => (
                    <tr key={t.tier}>
                      <td className="px-3 py-2 font-mono text-xs">{t.tier}</td>
                      <td className="px-3 py-2 text-right">{t.total.toLocaleString()}</td>
                      <td className="px-3 py-2 text-right font-medium">{t.accuracy_pct != null ? `${t.accuracy_pct}%` : "—"}</td>
                      <td className="px-3 py-2 text-right text-gray-500">{t.appeal_overturn_rate_pct != null ? `${t.appeal_overturn_rate_pct}%` : "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="text-xs text-gray-400">
              Evaluation scorers (mlflow.genai.evaluate):{" "}
              {quality.scorers.map((s) => (
                <span key={s} className="inline-block bg-gray-100 text-gray-600 rounded px-1.5 py-0.5 mr-1 font-mono">{s}</span>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Recent traces */}
      <div className="card">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h3 className="font-semibold text-databricks-dark">Recent Traces</h3>
          <button
            onClick={() => loadData(true)}
            disabled={refreshing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600
                       border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors
                       disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? "animate-spin" : ""}`} />
            {refreshing ? "Refreshing..." : "Refresh"}
          </button>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Trace ID</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Status</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Spans</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Duration</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Timestamp</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {traces.slice(0, 25).map((t, i) => (
                <tr key={i}>
                  <td className="px-4 py-3 font-mono text-xs">{String(t.request_id || "").slice(0, 16)}…</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                      t.status === "OK" ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
                    }`}>
                      {String(t.status || "unknown")}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right">{t.span_count || "—"}</td>
                  <td className="px-4 py-3 text-right">
                    {t.execution_time_ms ? `${Math.round(Number(t.execution_time_ms))}ms` : "—"}
                  </td>
                  <td className="px-4 py-3 text-right text-gray-500">
                    {t.timestamp_ms ? new Date(Number(t.timestamp_ms)).toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
              {traces.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                    No traces yet. Run a PA agent query or adjudicate a document to generate traces.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
