import { useEffect, useState } from "react";
import { Play, RefreshCw, Save, ShieldAlert, Check, Landmark, AlertTriangle } from "lucide-react";
import { api, FundingArrangementInfo, FundingQuoteResult } from "@/lib/api";
import { formatCurrency, formatCurrencyPrecise } from "@/lib/utils";

interface FieldDef {
  key: string;
  label: string;
  default?: string | number;
  suffix?: string;
  type?: "number" | "text" | "select";
  options?: string[];
}

const COMMON_FIELDS: FieldDef[] = [
  { key: "group_name", label: "Group / Employer", type: "text" },
  { key: "group_id", label: "Group ID", type: "text" },
  { key: "lob", label: "Line of Business", type: "text" },
  { key: "member_count", label: "Member Count", type: "number" },
];

const ARRANGEMENT_FIELDS: Record<string, FieldDef[]> = {
  fully_insured: [
    { key: "admin_load_pct", label: "Admin Load", default: 12, suffix: "%" },
    { key: "margin_pct", label: "Risk / Profit Margin", default: 4, suffix: "%" },
    { key: "trend_pct", label: "Medical Trend", default: 7, suffix: "%" },
  ],
  aso: [
    { key: "aso_admin_fee_pmpm", label: "ASO Admin Fee", default: 45, suffix: "$ PMPM" },
    { key: "specific_attachment", label: "Specific Attachment", default: 250000, suffix: "$" },
    { key: "aggregate_corridor_pct", label: "Aggregate Corridor", default: 125, suffix: "%" },
  ],
  level_funded: [
    { key: "admin_fee_pmpm", label: "Admin Fee", default: 55, suffix: "$ PMPM" },
    { key: "claims_margin_pct", label: "Claims Buffer", default: 15, suffix: "%" },
    { key: "surplus_share_pct", label: "Surplus Share to Employer", default: 50, suffix: "%" },
  ],
  specialty: [
    { key: "manual_pmpm", label: "Manual Rate", default: 32, suffix: "$ PMPM" },
    { key: "target_loss_ratio", label: "Target Loss Ratio", default: 72, suffix: "%" },
    { key: "credibility", label: "Credibility (0-1)", default: 0 },
  ],
  stop_loss: [
    { key: "specific_attachment", label: "Specific Attachment", default: 250000, suffix: "$" },
    { key: "sl_target_loss_ratio", label: "SL Target Loss Ratio", default: 70, suffix: "%" },
  ],
  disability: [
    { key: "disability_product", label: "Product", default: "LTD", type: "select", options: ["LTD", "STD"] },
    { key: "avg_monthly_salary", label: "Avg Monthly Salary", default: 5500, suffix: "$" },
    { key: "occupation_class", label: "Occupation Class (1-4)", default: 2 },
  ],
};

const TEXT_KEYS = new Set(["group_name", "group_id", "lob", "disability_product"]);

export default function FundingQuote() {
  const [arrangements, setArrangements] = useState<FundingArrangementInfo[]>([]);
  const [selected, setSelected] = useState<string>("fully_insured");
  const [values, setValues] = useState<Record<string, string>>({});
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<FundingQuoteResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);

  useEffect(() => {
    api.listFundingArrangements().then(setArrangements).catch(() => setArrangements([]));
  }, []);

  // Reset defaults when the arrangement changes.
  useEffect(() => {
    const defaults: Record<string, string> = { ...values };
    for (const f of ARRANGEMENT_FIELDS[selected] || []) {
      if (f.default !== undefined && defaults[f.key] === undefined) defaults[f.key] = String(f.default);
    }
    setValues(defaults);
    setResult(null);
    setNote(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected]);

  const fields = [...COMMON_FIELDS, ...(ARRANGEMENT_FIELDS[selected] || [])];

  const buildParams = (): Record<string, unknown> => {
    const params: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(values)) {
      if (v === "" || k === "group_name") continue;
      params[k] = TEXT_KEYS.has(k) ? v : Number(v);
    }
    return params;
  };

  const price = async (save: boolean) => {
    setRunning(true);
    setError(null);
    setNote(null);
    try {
      const res = await api.priceFundingQuote({
        arrangement: selected,
        group_name: values.group_name || undefined,
        save,
        parameters: buildParams(),
      });
      setResult(res);
      if (save && res.quote_id) setNote(`Quote saved (id ${res.quote_id.slice(0, 8)}…) — now in the Rated funnel.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to price quote");
      setResult(null);
    } finally {
      setRunning(false);
    }
  };

  const routeForApproval = async () => {
    if (!result) return;
    setError(null);
    try {
      const info = arrangements.find((a) => a.key === selected);
      const appr = await api.createApproval({
        subject: `${values.group_name || "Group"} — ${info?.label || selected} quote`,
        decision_type: "funding_quote",
        dollar_impact: result.dollar_impact_vs_fi,
        rate_change_pct: 0,
        group_id: values.group_id || undefined,
        lob: values.lob || undefined,
        context: { arrangement: selected, total_annual_cost: result.total_annual_cost },
      });
      setNote(`Routed to ${appr.required_role} (Tier ${appr.required_tier}) for approval.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to route for approval");
    }
  };

  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Funding Quote</h1>
        <p className="text-sm text-gray-500 mt-1">
          Route a quote by funding arrangement — fully-insured, ASO/self-funded, level-funded,
          specialty, stop-loss, or disability — and price it with the right build-up.
        </p>
      </div>

      {/* Arrangement selector */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
        {arrangements.map((a) => (
          <button
            key={a.key}
            onClick={() => setSelected(a.key)}
            className={`sim-type-card text-left ${selected === a.key ? "selected" : ""}`}
          >
            <Landmark className={`w-5 h-5 mb-2 ${selected === a.key ? "text-databricks-red" : "text-gray-400"}`} />
            <div className="font-medium text-sm text-databricks-dark">{a.label}</div>
            <div className="text-xs text-gray-500 mt-0.5">Risk: {a.risk_bearer}</div>
          </button>
        ))}
      </div>

      {/* Inputs */}
      <div className="card">
        <h2 className="text-lg font-semibold text-databricks-dark mb-4">Quote Inputs</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {fields.map((field) => (
            <div key={field.key}>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {field.label}
                {field.suffix && <span className="text-gray-400 ml-1">({field.suffix})</span>}
              </label>
              {field.type === "select" ? (
                <select
                  value={values[field.key] ?? ""}
                  onChange={(e) => setValues((p) => ({ ...p, [field.key]: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                             focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
                >
                  {(field.options || []).map((o) => (
                    <option key={o} value={o}>{o}</option>
                  ))}
                </select>
              ) : (
                <input
                  type={TEXT_KEYS.has(field.key) ? "text" : "number"}
                  step={TEXT_KEYS.has(field.key) ? undefined : "any"}
                  value={values[field.key] ?? ""}
                  onChange={(e) => setValues((p) => ({ ...p, [field.key]: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                             focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
                />
              )}
            </div>
          ))}
        </div>
        <div className="mt-6 flex gap-3 flex-wrap">
          <button onClick={() => price(false)} disabled={running} className="btn-primary flex items-center gap-2">
            {running ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            {running ? "Pricing..." : "Price Quote"}
          </button>
          {result && (
            <>
              <button onClick={() => price(true)} disabled={running} className="btn-secondary flex items-center gap-2">
                <Save className="w-4 h-4" /> Save Quote
              </button>
              <button onClick={routeForApproval} className="btn-secondary flex items-center gap-2">
                <ShieldAlert className="w-4 h-4" /> Route for Approval
              </button>
            </>
          )}
        </div>
      </div>

      {error && <div className="card border-red-200 bg-red-50 text-sm text-red-700">{error}</div>}
      {note && (
        <div className="card border-green-200 bg-green-50 text-sm text-green-800 flex items-center gap-2">
          <Check className="w-4 h-4" /> {note}
        </div>
      )}

      {result && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <Stat label="Total Annual Cost" value={formatCurrency(result.total_annual_cost)} />
            <Stat label="Total PMPM" value={formatCurrencyPrecise(result.total_pmpm)} />
            <Stat
              label="Impact vs Fully-Insured"
              value={`${result.dollar_impact_vs_fi >= 0 ? "+" : ""}${formatCurrency(result.dollar_impact_vs_fi)}`}
              highlight={result.dollar_impact_vs_fi < 0}
            />
            <Stat label="Risk Bearer" value={result.risk_bearer} />
          </div>

          <div className="card">
            <h2 className="text-lg font-semibold text-databricks-dark mb-4">Cost Build-Up</h2>
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left text-gray-500">
                  <th className="py-2 pr-4 font-medium">Component</th>
                  <th className="py-2 px-4 font-medium text-right">PMPM</th>
                  <th className="py-2 px-4 font-medium text-right">Annual</th>
                  <th className="py-2 pl-4 font-medium">Note</th>
                </tr>
              </thead>
              <tbody>
                {result.line_items.map((li, i) => (
                  <tr key={i} className="border-b border-gray-50">
                    <td className="py-2 pr-4 font-medium text-databricks-dark">{li.label}</td>
                    <td className="py-2 px-4 text-right">{formatCurrencyPrecise(li.pmpm)}</td>
                    <td className="py-2 px-4 text-right">{formatCurrency(li.annual)}</td>
                    <td className="py-2 pl-4 text-gray-500 text-xs">{li.note}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {result.employer_max_liability != null && (
              <div className="mt-4 text-sm text-gray-600">
                Modeled maximum employer liability:{" "}
                <span className="font-semibold text-databricks-dark">
                  {formatCurrency(result.employer_max_liability)}
                </span>
              </div>
            )}
            <p className="text-sm text-gray-600 mt-4 leading-relaxed">{result.narrative}</p>
          </div>

          {result.warnings.length > 0 && (
            <div className="card border-amber-200 bg-amber-50">
              <ul className="space-y-1 text-sm text-amber-800">
                {result.warnings.map((w, i) => (
                  <li key={i} className="flex items-start gap-2">
                    <AlertTriangle className="w-4 h-4 flex-shrink-0 mt-0.5" /> {w}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="kpi-card">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</div>
      <div className={`text-lg font-bold mt-1 ${highlight ? "text-green-600" : "text-databricks-dark"}`}>
        {value}
      </div>
    </div>
  );
}
