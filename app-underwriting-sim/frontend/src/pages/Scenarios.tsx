import { useState } from "react";
import { Play, RefreshCw, Trophy, TrendingUp, Shield, Target, Sparkles, Info } from "lucide-react";
import { api, ScenarioLeg, ScenarioPackage, ScenarioPackageInput } from "@/lib/api";
import { formatCurrency, formatPercent } from "@/lib/utils";

// Form field definitions for the scenario-package request.
interface FieldDef {
  key: string;
  label: string;
  default?: number | string;
  suffix?: string;
  hint?: string;
}

const FIELDS: FieldDef[] = [
  { key: "group_id", label: "Group ID", hint: "Blank = book averages" },
  { key: "lob", label: "Line of Business", hint: "Blank = all LOBs" },
  { key: "target_mlr", label: "Target MLR", default: 82, suffix: "%" },
  { key: "admin_load_pct", label: "Admin Load", default: 12, suffix: "%" },
  { key: "market_trend_pct", label: "Market Trend", default: 7, suffix: "%" },
  { key: "min_margin_pct", label: "Margin Floor", default: 3, suffix: "%" },
  { key: "competitor_pmpm", label: "Competitor PMPM", suffix: "$", hint: "Drives Competitive" },
  { key: "competitor_within_pct", label: "Match Within", default: 2, suffix: "%" },
  { key: "custom_rate_change_pct", label: "Custom Rate Change", suffix: "%", hint: "Underwriter-defined" },
];

const NUMERIC_KEYS = new Set([
  "target_mlr",
  "admin_load_pct",
  "market_trend_pct",
  "min_margin_pct",
  "competitor_pmpm",
  "competitor_within_pct",
  "custom_rate_change_pct",
]);

const LEG_ICONS: Record<string, React.ElementType> = {
  standard: Target,
  competitive: TrendingUp,
  retention: Shield,
  custom: Sparkles,
};

export default function Scenarios() {
  const [values, setValues] = useState<Record<string, string>>(() => {
    const d: Record<string, string> = {};
    for (const f of FIELDS) if (f.default !== undefined) d[f.key] = String(f.default);
    return d;
  });
  const [running, setRunning] = useState(false);
  const [pkg, setPkg] = useState<ScenarioPackage | null>(null);
  const [error, setError] = useState<string | null>(null);

  const run = async () => {
    setRunning(true);
    setError(null);
    try {
      const input: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(values)) {
        if (v === "") continue;
        input[k] = NUMERIC_KEYS.has(k) ? Number(v) : v;
      }
      setPkg(await api.packageScenarios(input as ScenarioPackageInput));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to package scenarios");
      setPkg(null);
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="p-8 space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Packaged Scenarios</h1>
        <p className="text-sm text-gray-500 mt-1">
          Generate Standard, Competitive, Retention, and Custom renewal options from one input set —
          each with projected margin, MLR, and modeled retention — plus an explainable recommendation.
        </p>
      </div>

      {/* Input form */}
      <div className="card">
        <h2 className="text-lg font-semibold text-databricks-dark mb-4">Renewal Inputs</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {FIELDS.map((field) => (
            <div key={field.key}>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {field.label}
                {field.suffix && <span className="text-gray-400 ml-1">({field.suffix})</span>}
              </label>
              <input
                type={NUMERIC_KEYS.has(field.key) ? "number" : "text"}
                step={NUMERIC_KEYS.has(field.key) ? "any" : undefined}
                value={values[field.key] || ""}
                onChange={(e) => setValues((prev) => ({ ...prev, [field.key]: e.target.value }))}
                placeholder={field.hint}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                           focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
              />
              {field.hint && <p className="text-xs text-gray-400 mt-1">{field.hint}</p>}
            </div>
          ))}
        </div>
        <div className="mt-6">
          <button onClick={run} disabled={running} className="btn-primary flex items-center gap-2">
            {running ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            {running ? "Packaging..." : "Generate Scenarios"}
          </button>
        </div>
      </div>

      {error && (
        <div className="card border-red-200 bg-red-50 text-sm text-red-700">{error}</div>
      )}

      {pkg && (
        <>
          {/* Current position */}
          <div className="card">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <h2 className="text-lg font-semibold text-databricks-dark">Current Position</h2>
              <span className="text-xs text-gray-400">{pkg.basis}</span>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
              <Stat label="Current Premium" value={formatCurrency(pkg.current_premium)} />
              <Stat label="Current Claims" value={formatCurrency(pkg.current_claims)} />
              <Stat label="Current MLR" value={formatPercent(pkg.current_mlr)} highlight={pkg.current_mlr > 85} />
              <Stat label="Members" value={pkg.member_count.toLocaleString()} />
            </div>
          </div>

          {/* Recommendation */}
          <div className="card border-l-4 border-l-databricks-red">
            <div className="flex items-start gap-3">
              <Trophy className="w-5 h-5 text-databricks-red flex-shrink-0 mt-0.5" />
              <div>
                <div className="text-sm font-semibold text-databricks-dark mb-1">
                  Recommendation
                </div>
                <p className="text-sm text-gray-700 leading-relaxed">
                  {pkg.recommendation_narrative}
                </p>
              </div>
            </div>
          </div>

          {/* Scenario legs */}
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
            {pkg.scenarios.map((leg) => (
              <ScenarioCard
                key={leg.name}
                leg={leg}
                recommended={leg.name === pkg.recommended_scenario}
              />
            ))}
          </div>

          {/* Explainability note (NAIC alignment) */}
          <div className="card bg-databricks-light/60 border-gray-200">
            <div className="flex items-start gap-2 text-xs text-gray-600 leading-relaxed">
              <Info className="w-4 h-4 flex-shrink-0 mt-0.5 text-gray-400" />
              <span>
                <strong>How the recommendation is made:</strong> each scenario's{" "}
                <em>expected retained margin</em> = projected margin dollars × modeled retention
                probability. Retention is a transparent, documented logistic curve of the rate increase
                relative to market trend — not a black box — so the recommendation stays explainable and
                auditable (NAIC AI model-governance alignment).
              </span>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

function Stat({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div>
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">{label}</div>
      <div className={`text-lg font-bold mt-0.5 ${highlight ? "text-red-600" : "text-databricks-dark"}`}>
        {value}
      </div>
    </div>
  );
}

function ScenarioCard({ leg, recommended }: { leg: ScenarioLeg; recommended: boolean }) {
  const Icon = LEG_ICONS[leg.name] || Target;
  return (
    <div
      className={`rounded-xl border p-5 bg-white transition-all ${
        recommended ? "border-databricks-red ring-2 ring-red-100 shadow-md" : "border-gray-200"
      }`}
    >
      <div className="flex items-center justify-between mb-3">
        <Icon className={`w-5 h-5 ${recommended ? "text-databricks-red" : "text-gray-400"}`} />
        {recommended && (
          <span className="badge-positive flex items-center gap-1">
            <Trophy className="w-3 h-3" /> Recommended
          </span>
        )}
      </div>
      <div className="font-semibold text-sm text-databricks-dark">{leg.label}</div>
      <div className="text-3xl font-bold text-databricks-dark mt-2">
        {leg.rate_change_pct >= 0 ? "+" : ""}
        {leg.rate_change_pct.toFixed(1)}%
      </div>
      <div className="text-xs text-gray-400 mb-4">rate change</div>

      <dl className="space-y-1.5 text-sm">
        <Row label="Projected MLR" value={formatPercent(leg.projected_mlr)} />
        <Row label="Margin" value={`${formatPercent(leg.margin_pct)} · ${formatCurrency(leg.margin_dollars)}`} />
        <Row label="Retention" value={formatPercent(leg.retention_probability * 100, 0)} />
        <Row
          label="Exp. retained margin"
          value={formatCurrency(leg.expected_retained_margin)}
          strong
        />
      </dl>
      <p className="text-xs text-gray-500 mt-4 leading-relaxed">{leg.rationale}</p>
    </div>
  );
}

function Row({ label, value, strong }: { label: string; value: string; strong?: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-gray-500">{label}</dt>
      <dd className={strong ? "font-semibold text-databricks-dark" : "text-gray-700"}>{value}</dd>
    </div>
  );
}
