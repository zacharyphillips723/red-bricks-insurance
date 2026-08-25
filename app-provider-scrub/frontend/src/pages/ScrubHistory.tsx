import { useState, useEffect } from "react";
import { History, Loader2, X } from "lucide-react";
import { api } from "@/lib/api";
import type { ScrubSessionSummary, ScrubResult } from "@/lib/api";

function decisionBadge(decision?: string | null) {
  const map: Record<string, string> = {
    clean: "bg-green-100 text-green-800",
    at_risk: "bg-amber-100 text-amber-800",
    likely_denied: "bg-red-100 text-red-800",
  };
  return map[decision || ""] || "bg-gray-100 text-gray-600";
}

function scoreColor(score?: number | null) {
  if (score == null) return "text-gray-400";
  if (score >= 70) return "text-red-600";
  if (score >= 35) return "text-amber-600";
  return "text-green-600";
}

export function ScrubHistory() {
  const [rows, setRows] = useState<ScrubSessionSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [detail, setDetail] = useState<(ScrubResult & { clinical_notes?: string }) | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    api.getHistory().then(setRows).catch(() => setRows([])).finally(() => setLoading(false));
  }, []);

  const openDetail = async (sessionId: string) => {
    setDetailLoading(true);
    try {
      setDetail(await api.getSession(sessionId));
    } catch (e) {
      console.error(e);
    } finally {
      setDetailLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );
  }

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <History className="text-databricks-red" /> Scrub History
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Every pre-submission scrub, persisted in Lakebase — including fix-and-resubmit lineage.
        </p>
      </div>

      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Session</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Member</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">DOS</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Type</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Risk</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Decision</th>
                <th className="px-4 py-3 text-right font-medium text-gray-600">Findings</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">When</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map((r) => (
                <tr key={r.session_id} onClick={() => openDetail(r.session_id)} className="hover:bg-gray-50 cursor-pointer">
                  <td className="px-4 py-3 font-mono text-xs">
                    {r.session_id}
                    {r.resubmitted_from && <span className="ml-1 text-[10px] text-teal-600">↻</span>}
                  </td>
                  <td className="px-4 py-3">{r.member_name || r.member_id}</td>
                  <td className="px-4 py-3 text-gray-500">{r.date_of_service}</td>
                  <td className="px-4 py-3 text-gray-500">{r.request_type}</td>
                  <td className={`px-4 py-3 text-right font-bold ${scoreColor(r.risk_score)}`}>{r.risk_score ?? "—"}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${decisionBadge(r.decision)}`}>
                      {r.decision || "—"}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right">{r.finding_count}</td>
                  <td className="px-4 py-3 text-gray-500">
                    {r.created_at ? new Date(r.created_at).toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-8 text-center text-gray-400">
                    No scrubs yet. Compose a claim and run a scrub to see it here.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Detail drawer */}
      {(detail || detailLoading) && (
        <div className="fixed inset-0 bg-black/30 flex justify-end z-20" onClick={() => setDetail(null)}>
          <div className="w-full max-w-md bg-white h-full overflow-y-auto p-6" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between mb-4">
              <h3 className="font-semibold text-databricks-dark">Scrub detail</h3>
              <button onClick={() => setDetail(null)} className="text-gray-400 hover:text-databricks-red">
                <X className="w-5 h-5" />
              </button>
            </div>
            {detailLoading ? (
              <div className="flex items-center justify-center h-40">
                <Loader2 className="w-6 h-6 text-databricks-red animate-spin" />
              </div>
            ) : detail ? (
              <div className="space-y-4">
                <div className="text-sm">
                  <div className="font-mono text-xs text-gray-500">{detail.session_id}</div>
                  <div className="mt-1">{detail.member_name || detail.member_id} · DOS {detail.date_of_service}</div>
                  <div className={`mt-1 font-bold ${scoreColor(detail.risk_score)}`}>Risk {detail.risk_score}</div>
                </div>
                {detail.reason_cards?.map((c, i) => (
                  <div key={i} className="border border-gray-200 rounded-md p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-mono text-xs font-bold bg-databricks-dark text-white rounded px-2 py-0.5">{c.carc_code}</span>
                      <span className="text-sm font-medium">{c.reason_label}</span>
                    </div>
                    {c.evidence && <p className="text-xs text-gray-600">{c.evidence}</p>}
                    {(c.remediation || c.remediation_text) && (
                      <p className="text-xs text-amber-800 mt-2">{c.remediation || c.remediation_text}</p>
                    )}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}
