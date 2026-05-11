import { useState, useEffect } from "react";
import {
  Loader2,
  DollarSign,
  ArrowRight,
  ChevronDown,
  ChevronUp,
  Info,
  TrendingUp,
  TrendingDown,
} from "lucide-react";
import {
  api,
  type RateBuildupResult,
  type RateBuildupInput,
  type FactorTables,
} from "@/lib/api";

const AGE_BANDS = ["0-17", "18-25", "26-35", "36-45", "46-55", "56-64", "65+"];
const COUNTY_TYPES = ["urban", "suburban", "rural"];
const INDUSTRIES = [
  "healthcare", "office", "manufacturing", "technology", "retail",
  "construction", "education", "finance", "hospitality", "transportation",
  "government", "agriculture",
];
const LOBS = ["Commercial", "Medicare Advantage", "Medicaid", "Individual"];

function StepCard({
  step,
  index,
}: {
  step: { step_name: string; factor_label: string; factor_value: number; running_total: number; description: string };
  index: number;
}) {
  const isBase = step.step_name === "base_rate";
  const isGood = step.factor_value < 1;
  const isBad = step.factor_value > 1;

  return (
    <div className="flex items-center gap-3">
      <div className="flex-shrink-0 w-8 h-8 rounded-full bg-databricks-dark text-white flex items-center justify-center text-xs font-bold">
        {index + 1}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700 truncate">
            {step.factor_label}
          </span>
          {!isBase && (
            <span
              className={`text-sm font-mono font-semibold ${
                isGood ? "text-green-600" : isBad ? "text-red-600" : "text-gray-600"
              }`}
            >
              x{step.factor_value.toFixed(4)}
            </span>
          )}
        </div>
        <div className="flex items-center justify-between mt-0.5">
          <span className="text-xs text-gray-400">{step.description}</span>
          <span className="text-xs font-mono text-databricks-dark font-semibold">
            ${step.running_total.toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function PricingPage() {
  const [factors, setFactors] = useState<FactorTables | null>(null);
  const [showFactors, setShowFactors] = useState(false);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<RateBuildupResult | null>(null);

  // Form state
  const [input, setInput] = useState<RateBuildupInput>({
    avg_age_band: "36-45",
    county_type: "suburban",
    sic_code: "office",
    lob: "Commercial",
    trend_pct: 8,
    credibility_factor: 0.5,
  });

  useEffect(() => {
    api.getFactorTables().then(setFactors).catch(() => {});
  }, []);

  const handleCompute = async () => {
    setLoading(true);
    try {
      const res = await api.computeRateBuildup(input);
      setResult(res);
    } catch (err) {
      console.error("Rate build-up error:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 max-w-6xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <DollarSign className="w-6 h-6 text-databricks-red" />
          Actuarial Rate Build-Up
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          Community-rated pricing model: Base Rate x Age x Area x Industry x Experience Mod x Trend
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Panel */}
        <div className="card p-5 space-y-4">
          <h2 className="text-sm font-semibold text-databricks-dark">Rating Parameters</h2>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Line of Business</label>
              <select
                value={input.lob || "Commercial"}
                onChange={(e) => setInput({ ...input, lob: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              >
                {LOBS.map((l) => (
                  <option key={l} value={l}>{l}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Average Age Band</label>
              <select
                value={input.avg_age_band || "36-45"}
                onChange={(e) => setInput({ ...input, avg_age_band: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              >
                {AGE_BANDS.map((b) => (
                  <option key={b} value={b}>{b}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">County Type</label>
              <select
                value={input.county_type || "suburban"}
                onChange={(e) => setInput({ ...input, county_type: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              >
                {COUNTY_TYPES.map((c) => (
                  <option key={c} value={c}>{c[0].toUpperCase() + c.slice(1)}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Industry</label>
              <select
                value={input.sic_code || "office"}
                onChange={(e) => setInput({ ...input, sic_code: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              >
                {INDUSTRIES.map((i) => (
                  <option key={i} value={i}>{i[0].toUpperCase() + i.slice(1)}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">
                Medical Trend (%)
              </label>
              <input
                type="number"
                min={0}
                max={20}
                step={0.5}
                value={input.trend_pct ?? 8}
                onChange={(e) => setInput({ ...input, trend_pct: parseFloat(e.target.value) || 8 })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              />
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">
                Credibility Factor (0-1)
              </label>
              <input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={input.credibility_factor ?? 0.5}
                onChange={(e) => setInput({ ...input, credibility_factor: parseFloat(e.target.value) || 0.5 })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              />
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">
                Group Loss Ratio (optional)
              </label>
              <input
                type="number"
                min={0}
                max={3}
                step={0.01}
                value={input.loss_ratio ?? ""}
                onChange={(e) =>
                  setInput({
                    ...input,
                    loss_ratio: e.target.value ? parseFloat(e.target.value) : undefined,
                  })
                }
                placeholder="e.g. 0.85"
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              />
            </div>

            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">
                Group ID (optional)
              </label>
              <input
                type="text"
                value={input.group_id ?? ""}
                onChange={(e) =>
                  setInput({
                    ...input,
                    group_id: e.target.value || undefined,
                  })
                }
                placeholder="e.g. GRP-001"
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              />
            </div>
          </div>

          <button
            onClick={handleCompute}
            disabled={loading}
            className="w-full btn-primary flex items-center justify-center gap-2 py-2.5"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <ArrowRight className="w-4 h-4" />
            )}
            Compute Rate
          </button>
        </div>

        {/* Result Panel */}
        <div className="space-y-4">
          {result ? (
            <>
              {/* Final Rate */}
              <div className="card p-5">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-sm font-semibold text-databricks-dark">
                    Final Rate — {result.lob}
                  </h2>
                  {result.rate_change_pct != null && (
                    <div
                      className={`flex items-center gap-1 text-sm font-semibold ${
                        result.rate_change_pct > 0 ? "text-red-600" : "text-green-600"
                      }`}
                    >
                      {result.rate_change_pct > 0 ? (
                        <TrendingUp className="w-4 h-4" />
                      ) : (
                        <TrendingDown className="w-4 h-4" />
                      )}
                      {result.rate_change_pct > 0 ? "+" : ""}
                      {result.rate_change_pct.toFixed(1)}% vs current
                    </div>
                  )}
                </div>
                <div className="text-center py-4">
                  <div className="text-5xl font-bold text-databricks-dark">
                    ${result.final_rate.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </div>
                  <div className="text-sm text-gray-500 mt-1">PMPM</div>
                  {result.current_rate && (
                    <div className="text-xs text-gray-400 mt-2">
                      Current: ${result.current_rate.toLocaleString(undefined, { minimumFractionDigits: 2 })} PMPM
                      {result.rate_change != null && (
                        <> | Change: ${result.rate_change > 0 ? "+" : ""}
                        {result.rate_change.toLocaleString(undefined, { minimumFractionDigits: 2 })}</>
                      )}
                    </div>
                  )}
                </div>
              </div>

              {/* Step-by-Step */}
              <div className="card p-5">
                <h2 className="text-sm font-semibold text-databricks-dark mb-4">
                  Rate Build-Up Steps
                </h2>
                <div className="space-y-4">
                  {result.steps.map((step, idx) => (
                    <StepCard key={step.step_name} step={step} index={idx} />
                  ))}
                </div>
              </div>

              {/* Narrative */}
              <div className="card p-4 bg-blue-50 border-blue-200">
                <div className="flex gap-2">
                  <Info className="w-4 h-4 text-blue-600 flex-shrink-0 mt-0.5" />
                  <p className="text-sm text-blue-800">{result.narrative}</p>
                </div>
              </div>
            </>
          ) : (
            <div className="card p-12 text-center">
              <DollarSign className="w-12 h-12 text-gray-300 mx-auto mb-3" />
              <h3 className="text-lg font-semibold text-gray-400">
                Configure & Compute
              </h3>
              <p className="text-sm text-gray-400 mt-1">
                Set your rating parameters and click "Compute Rate" to see the
                full rate build-up cascade.
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Factor Reference Tables */}
      {factors && (
        <div className="card p-5">
          <button
            onClick={() => setShowFactors(!showFactors)}
            className="w-full flex items-center justify-between text-sm font-semibold text-databricks-dark"
          >
            <span>Reference Factor Tables</span>
            {showFactors ? (
              <ChevronUp className="w-4 h-4" />
            ) : (
              <ChevronDown className="w-4 h-4" />
            )}
          </button>
          {showFactors && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-4">
              {[
                factors.age_factors,
                factors.area_factors,
                factors.industry_factors,
                factors.trend_factors,
                factors.experience_mod_ranges,
              ].map((table) => (
                <div key={table.table_name} className="border border-gray-200 rounded-lg p-3">
                  <h4 className="text-xs font-semibold text-databricks-dark mb-1">
                    {table.table_name}
                  </h4>
                  <p className="text-xs text-gray-400 mb-2">{table.description}</p>
                  <table className="w-full text-xs">
                    <tbody className="divide-y divide-gray-100">
                      {table.factors.map((row, i) => {
                        const entries = Object.entries(row);
                        return (
                          <tr key={i}>
                            <td className="py-1 text-gray-600">{String(entries[0]?.[1] ?? "")}</td>
                            <td className="py-1 text-right font-mono font-semibold text-databricks-dark">
                              {typeof entries[1]?.[1] === "number"
                                ? entries[1][1].toFixed(2)
                                : String(entries[1]?.[1] ?? "")}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
