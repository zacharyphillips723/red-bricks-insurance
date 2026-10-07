import { useEffect, useState } from "react";
import { RefreshCw, Check, X, HelpCircle, ShieldCheck, Layers } from "lucide-react";
import { api, Approval, AuthorityTier } from "@/lib/api";
import { formatCurrency, formatPercent, formatDateTime } from "@/lib/utils";

const STATUS_BADGE: Record<string, string> = {
  pending: "badge-warning",
  approved: "badge-positive",
  rejected: "badge-negative",
  needs_info: "badge-neutral",
};

export default function Approvals() {
  const [matrix, setMatrix] = useState<AuthorityTier[]>([]);
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<string | null>(null);

  const refresh = () => {
    Promise.all([
      api.getAuthorityMatrix().catch(() => []),
      api.listApprovals().catch(() => []),
    ]).then(([m, a]) => {
      setMatrix(m);
      setApprovals(a);
      setLoading(false);
    });
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const decide = async (id: string, decision: string) => {
    setBusyId(id);
    try {
      await api.decideApproval(id, decision);
      refresh();
    } finally {
      setBusyId(null);
    }
  };

  const limit = (v?: number | null) => (v == null ? "No cap" : formatCurrency(v));
  const rateLimit = (v?: number | null) => (v == null ? "No cap" : formatPercent(v, 0));

  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Approval Routing</h1>
        <p className="text-sm text-gray-500 mt-1">
          Decisions route to the lowest authority tier whose dollar and rate-change limits both cover
          them. Every request and decision is written to the audit log.
        </p>
      </div>

      {/* Authority matrix */}
      <div className="card">
        <div className="flex items-center gap-3 mb-4">
          <Layers className="w-5 h-5 text-databricks-red" />
          <h2 className="text-lg font-semibold text-databricks-dark">Authority Matrix</h2>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 text-left text-gray-500">
              <th className="py-2 pr-4 font-medium">Tier</th>
              <th className="py-2 px-4 font-medium">Approver Role</th>
              <th className="py-2 px-4 font-medium text-right">Max $ Impact</th>
              <th className="py-2 px-4 font-medium text-right">Max Rate Change</th>
              <th className="py-2 pl-4 font-medium">Scope</th>
            </tr>
          </thead>
          <tbody>
            {matrix.map((t) => (
              <tr key={t.tier} className="border-b border-gray-50">
                <td className="py-2 pr-4 font-semibold text-databricks-dark">{t.tier}</td>
                <td className="py-2 px-4">{t.role}</td>
                <td className="py-2 px-4 text-right">{limit(t.max_dollar_impact)}</td>
                <td className="py-2 px-4 text-right">{rateLimit(t.max_rate_change_pct)}</td>
                <td className="py-2 pl-4 text-gray-500 text-xs">{t.description}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Approval queue */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <ShieldCheck className="w-5 h-5 text-databricks-red" />
            <h2 className="text-lg font-semibold text-databricks-dark">Approval Queue</h2>
          </div>
          <button onClick={refresh} className="btn-secondary flex items-center gap-2 text-sm">
            <RefreshCw className="w-4 h-4" /> Refresh
          </button>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-8">
            <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
          </div>
        ) : approvals.length === 0 ? (
          <p className="text-sm text-gray-500 py-6 text-center">
            No approval requests yet. Route a quote from the Funding Quote screen.
          </p>
        ) : (
          <div className="space-y-3">
            {approvals.map((a) => (
              <div key={a.approval_id} className="border border-gray-100 rounded-lg p-4">
                <div className="flex items-start justify-between gap-4 flex-wrap">
                  <div>
                    <div className="font-medium text-databricks-dark">{a.subject}</div>
                    <div className="text-xs text-gray-500 mt-1">
                      Tier {a.required_tier} · {a.required_role} · impact{" "}
                      {formatCurrency(a.dollar_impact || 0)}
                      {a.rate_change_pct ? ` · ${formatPercent(a.rate_change_pct)} rate` : ""} ·{" "}
                      requested by {a.requested_by}
                      {a.created_at ? ` · ${formatDateTime(a.created_at)}` : ""}
                    </div>
                    {a.decision_notes && (
                      <div className="text-xs text-gray-500 mt-1 italic">“{a.decision_notes}”</div>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    <span className={STATUS_BADGE[a.status] || "badge-neutral"}>{a.status}</span>
                    {a.status === "pending" && (
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => decide(a.approval_id, "approved")}
                          disabled={busyId === a.approval_id}
                          title="Approve"
                          className="p-1.5 rounded-lg bg-green-100 text-green-700 hover:bg-green-200 disabled:opacity-50"
                        >
                          <Check className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => decide(a.approval_id, "needs_info")}
                          disabled={busyId === a.approval_id}
                          title="Needs info"
                          className="p-1.5 rounded-lg bg-gray-100 text-gray-700 hover:bg-gray-200 disabled:opacity-50"
                        >
                          <HelpCircle className="w-4 h-4" />
                        </button>
                        <button
                          onClick={() => decide(a.approval_id, "rejected")}
                          disabled={busyId === a.approval_id}
                          title="Reject"
                          className="p-1.5 rounded-lg bg-red-100 text-red-700 hover:bg-red-200 disabled:opacity-50"
                        >
                          <X className="w-4 h-4" />
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
