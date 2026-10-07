import { useEffect, useState } from "react";
import {
  Brain, RefreshCw, ShieldCheck, ShieldAlert, ShieldX, Quote, History, Scale,
} from "lucide-react";
import { api, PreflightResult, Guardrail, PrecedentDecision } from "@/lib/api";
import { formatCurrency, formatPercent, formatDateTime } from "@/lib/utils";

const ARRANGEMENTS = ["fully_insured", "aso", "level_funded", "specialty", "stop_loss", "disability"];
const SCENARIOS = ["standard", "competitive", "retention", "custom"];

const VERDICT_STYLE: Record<string, { bg: string; text: string; icon: React.ElementType; label: string }> = {
  GREEN: { bg: "bg-green-50 border-green-200", text: "text-green-700", icon: ShieldCheck, label: "GREEN — proceed" },
  AMBER: { bg: "bg-amber-50 border-amber-200", text: "text-amber-700", icon: ShieldAlert, label: "AMBER — review" },
  RED: { bg: "bg-red-50 border-red-200", text: "text-red-700", icon: ShieldX, label: "RED — do not proceed" },
};

export default function DigitalTwin() {
  const [form, setForm] = useState<Record<string, string>>({
    funding_arrangement: "fully_insured",
    scenario_chosen: "standard",
    group_size: "3000",
    industry: "manufacturing",
    rate_change_pct: "12",
    projected_margin: "2.5",
    projected_mlr: "90",
  });
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<PreflightResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [guardrails, setGuardrails] = useState<Guardrail[]>([]);
  const [memory, setMemory] = useState<PrecedentDecision[]>([]);

  const loadContext = () => {
    api.listGuardrails().then(setGuardrails).catch(() => setGuardrails([]));
    api.getTwinMemory(10).then(setMemory).catch(() => setMemory([]));
  };

  useEffect(() => {
    loadContext();
  }, []);

  const set = (k: string, v: string) => setForm((p) => ({ ...p, [k]: v }));

  const run = async () => {
    setRunning(true);
    setError(null);
    try {
      const res = await api.preflightCheck({
        funding_arrangement: form.funding_arrangement,
        lob: form.lob || undefined,
        group_id: form.group_id || undefined,
        group_size: form.group_size ? Number(form.group_size) : undefined,
        industry: form.industry || undefined,
        scenario_chosen: form.scenario_chosen,
        rationale: form.rationale || undefined,
        parameters: form.rate_change_pct ? { rate_change_pct: Number(form.rate_change_pct) } : {},
        projected_margin: form.projected_margin ? Number(form.projected_margin) : undefined,
        projected_mlr: form.projected_mlr ? Number(form.projected_mlr) : undefined,
      });
      setResult(res);
      loadContext(); // the candidate decision was written to memory
    } catch (e) {
      setError(e instanceof Error ? e.message : "Preflight check failed");
    } finally {
      setRunning(false);
    }
  };

  const verdict = result ? VERDICT_STYLE[result.verdict] : null;

  return (
    <div className="p-8 space-y-8">
      <div className="flex items-start gap-3">
        <Brain className="w-7 h-7 text-databricks-red flex-shrink-0" />
        <div>
          <h1 className="text-2xl font-bold text-databricks-dark">Underwriting Digital Twin</h1>
          <p className="text-sm text-gray-500 mt-1">
            An advisory pre-implementation check. Before you commit a policy, the twin recalls
            institutional precedent, projects the dollar impact, runs the actuary-owned guardrails,
            and critiques the decision grounded in cited precedent. Human-in-the-loop — it checks, it
            doesn't decide.
          </p>
        </div>
      </div>

      {/* Proposed decision form */}
      <div className="card">
        <h2 className="text-lg font-semibold text-databricks-dark mb-4">Proposed Decision</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <Field label="Funding Arrangement">
            <select value={form.funding_arrangement} onChange={(e) => set("funding_arrangement", e.target.value)} className={selectCls}>
              {ARRANGEMENTS.map((a) => <option key={a} value={a}>{a}</option>)}
            </select>
          </Field>
          <Field label="Scenario Chosen">
            <select value={form.scenario_chosen} onChange={(e) => set("scenario_chosen", e.target.value)} className={selectCls}>
              {SCENARIOS.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
          </Field>
          <TextField label="Line of Business" value={form.lob || ""} onChange={(v) => set("lob", v)} />
          <TextField label="Group ID" value={form.group_id || ""} onChange={(v) => set("group_id", v)} />
          <NumField label="Group Size (lives)" value={form.group_size || ""} onChange={(v) => set("group_size", v)} />
          <TextField label="Industry" value={form.industry || ""} onChange={(v) => set("industry", v)} />
          <NumField label="Rate Change %" value={form.rate_change_pct || ""} onChange={(v) => set("rate_change_pct", v)} />
          <NumField label="Projected Margin %" value={form.projected_margin || ""} onChange={(v) => set("projected_margin", v)} />
          <NumField label="Projected MLR %" value={form.projected_mlr || ""} onChange={(v) => set("projected_mlr", v)} />
        </div>
        <div className="mt-4">
          <label className="block text-sm font-medium text-gray-700 mb-1">Rationale</label>
          <textarea
            value={form.rationale || ""}
            onChange={(e) => set("rationale", e.target.value)}
            rows={2}
            placeholder="Why this decision? (e.g. aggressive renewal to defend margin on a deteriorating group)"
            className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/30"
          />
        </div>
        <div className="mt-6">
          <button onClick={run} disabled={running} className="btn-primary flex items-center gap-2">
            {running ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Brain className="w-4 h-4" />}
            {running ? "Checking…" : "Run Pre-Implementation Check"}
          </button>
        </div>
      </div>

      {error && <div className="card border-red-200 bg-red-50 text-sm text-red-700">{error}</div>}

      {result && verdict && (
        <>
          {/* Verdict */}
          <div className={`card border ${verdict.bg}`}>
            <div className="flex items-center gap-3">
              <verdict.icon className={`w-6 h-6 ${verdict.text}`} />
              <div className={`text-xl font-bold ${verdict.text}`}>{verdict.label}</div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
              <Stat label="Dollar Impact" value={`${result.dollar_impact >= 0 ? "+" : ""}${formatCurrency(result.dollar_impact)}`} />
              <Stat label="Projected MLR" value={result.projected_mlr != null ? formatPercent(result.projected_mlr) : "—"} />
              <Stat label="Projected Margin" value={result.projected_margin != null ? formatPercent(result.projected_margin) : "—"} />
              <Stat label="Approval Tier" value={`T${result.required_approval_tier.tier} · ${result.required_approval_tier.role}`} />
            </div>
          </div>

          {/* Guardrails */}
          <div className="card">
            <div className="flex items-center gap-3 mb-4">
              <Scale className="w-5 h-5 text-databricks-red" />
              <h2 className="text-lg font-semibold text-databricks-dark">Guardrail Evaluation</h2>
              <span className="text-xs text-gray-400">deterministic · actuary-owned</span>
            </div>
            <div className="space-y-2">
              {result.guardrail_results.map((g, i) => (
                <div key={i} className="flex items-center justify-between text-sm border-b border-gray-50 py-1.5">
                  <span className="text-gray-700">
                    {g.rule_type.replace(/_/g, " ")} <span className="text-gray-400">(≤/≥ {g.threshold})</span>
                    {g.note && <span className="text-gray-400"> — {g.note}</span>}
                  </span>
                  <span className={g.tripped ? (g.severity === "block" ? "badge-negative" : "badge-warning") : "badge-positive"}>
                    {g.tripped ? `${g.severity} tripped` : "ok"}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Grounded critique */}
          {result.critique && (
            <div className="card border-l-4 border-l-databricks-red">
              <div className="flex items-center gap-2 mb-2">
                <Quote className="w-4 h-4 text-databricks-red" />
                <span className="text-sm font-semibold text-databricks-dark">
                  Grounded Critique
                  <span className="text-gray-400 font-normal"> · concern: {result.concern_level}</span>
                </span>
              </div>
              <p className="text-sm text-gray-700 leading-relaxed">{result.critique}</p>
              {result.cited_decision_ids.length > 0 && (
                <div className="text-xs text-gray-400 mt-2">
                  Cites {result.cited_decision_ids.length} precedent decision(s).
                </div>
              )}
            </div>
          )}

          {/* Cited precedents */}
          {result.cited_precedents.length > 0 && (
            <div className="card">
              <h2 className="text-lg font-semibold text-databricks-dark mb-4">Recalled Precedent</h2>
              <div className="space-y-2">
                {result.cited_precedents.map((p) => (
                  <PrecedentRow key={p.decision_id} p={p} />
                ))}
              </div>
            </div>
          )}
        </>
      )}

      {/* Context: active guardrails + institutional memory */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <div className="flex items-center gap-3 mb-4">
            <ShieldCheck className="w-5 h-5 text-databricks-red" />
            <h2 className="text-lg font-semibold text-databricks-dark">Active Guardrails</h2>
          </div>
          {guardrails.length === 0 ? (
            <p className="text-sm text-gray-500">No guardrails loaded.</p>
          ) : (
            <ul className="space-y-1.5 text-sm">
              {guardrails.map((g) => (
                <li key={g.guardrail_id} className="flex items-center justify-between border-b border-gray-50 py-1.5">
                  <span className="text-gray-700">
                    {g.rule_type.replace(/_/g, " ")} · {g.threshold} <span className="text-gray-400">v{g.version}</span>
                  </span>
                  <span className={g.severity === "block" ? "badge-negative" : "badge-warning"}>{g.severity}</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="card">
          <div className="flex items-center gap-3 mb-4">
            <History className="w-5 h-5 text-databricks-red" />
            <h2 className="text-lg font-semibold text-databricks-dark">Institutional Memory</h2>
          </div>
          {memory.length === 0 ? (
            <p className="text-sm text-gray-500">No decisions in memory yet — run a check to seed it.</p>
          ) : (
            <div className="space-y-2">
              {memory.map((p) => <PrecedentRow key={p.decision_id} p={p} compact />)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function PrecedentRow({ p, compact }: { p: PrecedentDecision; compact?: boolean }) {
  const oc = p.outcome;
  return (
    <div className="border border-gray-100 rounded-lg p-3">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <div className="text-sm text-databricks-dark">
          {p.funding_arrangement} · {p.group_size_band} · {p.scenario_chosen}
          {p.similarity != null && (
            <span className="text-xs text-databricks-red ml-2">sim {(p.similarity * 100).toFixed(0)}%</span>
          )}
        </div>
        <div className="text-xs text-gray-400">
          {p.projected_mlr != null ? `MLR ${formatPercent(p.projected_mlr)}` : ""}
          {oc?.actual_mlr != null ? ` · actual ${formatPercent(oc.actual_mlr)}` : ""}
          {oc?.retained != null ? ` · ${oc.retained ? "retained" : "lost"}` : ""}
          {!compact && p.created_at ? ` · ${formatDateTime(p.created_at)}` : ""}
        </div>
      </div>
      {!compact && p.rationale && <div className="text-xs text-gray-500 mt-1 italic">“{p.rationale}”</div>}
    </div>
  );
}

const selectCls =
  "w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      {children}
    </div>
  );
}

function TextField({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <Field label={label}>
      <input value={value} onChange={(e) => onChange(e.target.value)} className={selectCls} />
    </Field>
  );
}

function NumField({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <Field label={label}>
      <input type="number" step="any" value={value} onChange={(e) => onChange(e.target.value)} className={selectCls} />
    </Field>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</div>
      <div className="text-base font-bold text-databricks-dark mt-0.5">{value}</div>
    </div>
  );
}
