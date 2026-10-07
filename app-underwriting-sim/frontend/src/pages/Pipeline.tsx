import { useEffect, useState } from "react";
import { RefreshCw, TrendingUp, Gavel, GitBranch, Clock, MessageSquare, ArrowRight } from "lucide-react";
import { api, OpsAnalytics, Reconciliation, ReconciliationQuote } from "@/lib/api";
import { formatCurrency, formatPercent } from "@/lib/utils";

const STAGES = ["rated", "sold", "implemented", "lost"];
const STAGE_LABEL: Record<string, string> = {
  rated: "Rated",
  sold: "Sold",
  implemented: "Implemented",
  lost: "Lost",
};
const STATUS_BADGE: Record<string, string> = {
  rated: "badge-neutral",
  sold: "badge-warning",
  implemented: "badge-positive",
  lost: "badge-negative",
};
// Allowed forward transitions per stage.
const NEXT: Record<string, { status: string; label: string }[]> = {
  rated: [
    { status: "sold", label: "Mark Sold" },
    { status: "lost", label: "Mark Lost" },
  ],
  sold: [{ status: "implemented", label: "Mark Implemented" }],
  implemented: [],
  lost: [],
};

export default function Pipeline() {
  const [ops, setOps] = useState<OpsAnalytics | null>(null);
  const [recon, setRecon] = useState<Reconciliation | null>(null);
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [rerateFor, setRerateFor] = useState<string | null>(null);
  const [instruction, setInstruction] = useState("");
  const [rerateNote, setRerateNote] = useState<string | null>(null);

  const refresh = () => {
    Promise.all([
      api.getOpsAnalytics().catch(() => null),
      api.getReconciliation().catch(() => null),
    ]).then(([o, r]) => {
      setOps(o);
      setRecon(r);
      setLoading(false);
    });
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const advance = async (quote: ReconciliationQuote, status: string) => {
    setBusyId(quote.quote_id);
    try {
      await api.updateFundingQuoteStatus(quote.quote_id, status);
      refresh();
    } finally {
      setBusyId(null);
    }
  };

  const rerate = async (quoteId: string) => {
    if (!instruction.trim()) return;
    setBusyId(quoteId);
    setRerateNote(null);
    try {
      const res = await api.rerateQuote(quoteId, instruction);
      const changes = Object.keys(res.param_changes || {});
      setRerateNote(
        changes.length
          ? `Re-rated: ${changes.map((k) => `${k}=${String(res.param_changes[k])}`).join(", ")} → ${formatCurrency(res.result.total_annual_cost)}/yr.`
          : "No parameter changes were inferred from that instruction."
      );
      setInstruction("");
      setRerateFor(null);
      refresh();
    } catch (e) {
      setRerateNote(e instanceof Error ? e.message : "Re-rate failed");
    } finally {
      setBusyId(null);
    }
  };

  if (loading) {
    return (
      <div className="p-8 flex items-center justify-center">
        <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
      </div>
    );
  }

  return (
    <div className="p-8 space-y-8">
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div>
          <h1 className="text-2xl font-bold text-databricks-dark">Quote Pipeline</h1>
          <p className="text-sm text-gray-500 mt-1">
            Rated → Sold → Implemented reconciliation, operational analytics, and natural-language
            re-rate — the lifecycle audit trail that never existed before.
          </p>
        </div>
        <button onClick={refresh} className="btn-secondary flex items-center gap-2 text-sm">
          <RefreshCw className="w-4 h-4" /> Refresh
        </button>
      </div>

      {/* Ops KPIs */}
      {ops && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Kpi icon={TrendingUp} label="Quote → Sold" value={formatPercent(ops.funnel.quote_to_sold_pct, 0)} />
          <Kpi icon={Gavel} label="Approval Rate" value={formatPercent(ops.approvals.approval_rate_pct, 0)}
               sub={`${ops.approvals.pending} pending`} />
          <Kpi icon={Clock} label="Avg Decision"
               value={ops.approvals.avg_decision_hours != null ? `${ops.approvals.avg_decision_hours}h` : "—"} />
          <Kpi icon={GitBranch} label="Factor Drift"
               value={`${ops.factor_governance.changed_factors} chg`}
               sub={`max ${formatPercent(ops.factor_governance.max_abs_pct_change, 1)}`} />
        </div>
      )}

      {/* Reconciliation stage summary */}
      {recon && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {STAGES.map((s) => {
            const stage = recon.stage_summary[s];
            return (
              <div key={s} className="card">
                <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                  {STAGE_LABEL[s]}
                </div>
                <div className="text-2xl font-bold text-databricks-dark mt-1">{stage?.count ?? 0}</div>
                <div className="text-xs text-gray-400 mt-0.5">
                  {formatCurrency(stage?.total_annual_cost ?? 0)}/yr
                </div>
              </div>
            );
          })}
        </div>
      )}

      {rerateNote && (
        <div className="card border-blue-200 bg-blue-50 text-sm text-blue-800">{rerateNote}</div>
      )}

      {/* Quote lineage */}
      <div className="card">
        <h2 className="text-lg font-semibold text-databricks-dark mb-4">Quote Lineage</h2>
        {!recon || recon.quotes.length === 0 ? (
          <p className="text-sm text-gray-500 py-6 text-center">
            No saved quotes yet. Price and save a quote from the Funding Quote screen.
          </p>
        ) : (
          <div className="space-y-3">
            {recon.quotes.map((q) => (
              <div key={q.quote_id} className="border border-gray-100 rounded-lg p-4">
                <div className="flex items-start justify-between gap-4 flex-wrap">
                  <div>
                    <div className="font-medium text-databricks-dark">{q.group_name}</div>
                    <div className="text-xs text-gray-500 mt-1">
                      {q.funding_arrangement} · {formatCurrency(q.total_annual_cost)}/yr · impact{" "}
                      {q.dollar_impact >= 0 ? "+" : ""}
                      {formatCurrency(q.dollar_impact)}
                    </div>
                  </div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className={STATUS_BADGE[q.status] || "badge-neutral"}>{q.status}</span>
                    {(NEXT[q.status] || []).map((t) => (
                      <button
                        key={t.status}
                        onClick={() => advance(q, t.status)}
                        disabled={busyId === q.quote_id}
                        className="btn-secondary text-xs py-1 flex items-center gap-1"
                      >
                        {t.label} <ArrowRight className="w-3 h-3" />
                      </button>
                    ))}
                    <button
                      onClick={() => {
                        setRerateFor(rerateFor === q.quote_id ? null : q.quote_id);
                        setInstruction("");
                      }}
                      className="btn-secondary text-xs py-1 flex items-center gap-1"
                    >
                      <MessageSquare className="w-3 h-3" /> Re-rate
                    </button>
                  </div>
                </div>
                {rerateFor === q.quote_id && (
                  <div className="mt-3 flex gap-2">
                    <input
                      value={instruction}
                      onChange={(e) => setInstruction(e.target.value)}
                      placeholder="e.g. raise the specific attachment to $300K, or +150 lives"
                      className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm
                                 focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
                    />
                    <button
                      onClick={() => rerate(q.quote_id)}
                      disabled={busyId === q.quote_id}
                      className="btn-primary text-sm flex items-center gap-2"
                    >
                      {busyId === q.quote_id ? <RefreshCw className="w-4 h-4 animate-spin" /> : "Apply"}
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function Kpi({
  icon: Icon,
  label,
  value,
  sub,
}: {
  icon: React.ElementType;
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="kpi-card">
      <div className="flex items-center gap-2 mb-2">
        <Icon className="w-4 h-4 text-gray-400" />
        <span className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</span>
      </div>
      <div className="text-xl font-bold text-databricks-dark">{value}</div>
      {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
    </div>
  );
}
