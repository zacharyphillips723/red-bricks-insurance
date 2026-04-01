import { useState, useEffect, useCallback } from "react";
import {
  Play,
  Save,
  AlertTriangle,
  ArrowUpDown,
  RefreshCw,
  Check,
} from "lucide-react";
import { api, SimulationResult } from "@/lib/api";
import {
  formatCurrency,
  formatPercent,
  deltaColor,
  deltaArrow,
  SIMULATION_TYPE_LABELS,
} from "@/lib/utils";

interface SimulationBuilderProps {
  onSaved: () => void;
}

type SimType = keyof typeof SIMULATION_TYPE_LABELS;

const SIM_TYPES = Object.keys(SIMULATION_TYPE_LABELS) as SimType[];

// --- Parameter form definitions ---
interface FieldDef {
  key: string;
  label: string;
  type: "number" | "text" | "select";
  default?: string | number;
  options?: string[];
  suffix?: string;
  hint?: string;
}

const FORM_FIELDS: Record<string, FieldDef[]> = {
  premium_rate: [
    { key: "rate_change_pct", label: "Rate Change", type: "number", default: 5, suffix: "%" },
    { key: "lob", label: "Line of Business", type: "text", hint: "Leave blank for all LOBs" },
  ],
  benefit_design: [
    { key: "lob", label: "Line of Business", type: "text", default: "Commercial" },
    { key: "deductible_change_pct", label: "Deductible Change", type: "number", default: 10, suffix: "%" },
    { key: "copay_change_pct", label: "Copay Change", type: "number", default: 0, suffix: "%" },
    { key: "coinsurance_change_pct", label: "Coinsurance Change", type: "number", default: 0, suffix: "%" },
    { key: "elasticity_factor", label: "Elasticity Factor", type: "number", default: 0.15, hint: "0.10-0.25 typical" },
  ],
  group_renewal: [
    { key: "group_id", label: "Group ID", type: "text", default: "GRP-001" },
    { key: "manual_rate_change_pct", label: "Manual Rate Change", type: "number", default: 5, suffix: "%" },
    { key: "credibility_weight", label: "Credibility Override", type: "number", hint: "0-1, leave blank for auto" },
  ],
  population_mix: [
    { key: "mix_Commercial", label: "Commercial Delta", type: "number", default: 0, hint: "Member count change" },
    { key: "mix_Medicare Advantage", label: "Medicare Advantage Delta", type: "number", default: 0 },
    { key: "mix_Medicaid", label: "Medicaid Delta", type: "number", default: 0 },
  ],
  medical_trend: [
    { key: "annual_trend_pct", label: "Annual Trend Rate", type: "number", default: 7, suffix: "%" },
    { key: "months", label: "Projection Months", type: "number", default: 12 },
    { key: "lob", label: "Line of Business", type: "text", hint: "Leave blank for all" },
  ],
  stop_loss: [
    { key: "group_id", label: "Group ID", type: "text", default: "GRP-001" },
    { key: "current_threshold", label: "Current Attachment", type: "number", default: 250000, suffix: "$" },
    { key: "new_threshold", label: "New Attachment", type: "number", default: 300000, suffix: "$" },
  ],
  risk_adjustment: [
    { key: "raf_improvement_pct", label: "RAF Improvement", type: "number", default: 5, suffix: "%" },
    { key: "lob", label: "Line of Business", type: "text", default: "Medicare Advantage" },
  ],
  utilization_change: [
    { key: "chg_Inpatient", label: "Inpatient Change", type: "number", default: 0, suffix: "%" },
    { key: "chg_Outpatient", label: "Outpatient Change", type: "number", default: 0, suffix: "%" },
    { key: "chg_Pharmacy", label: "Pharmacy Change", type: "number", default: 0, suffix: "%" },
    { key: "chg_Emergency", label: "Emergency Change", type: "number", default: 0, suffix: "%" },
    { key: "lob", label: "Line of Business", type: "text", hint: "Leave blank for all" },
  ],
  new_group_quote: [
    { key: "proposed_members", label: "Proposed Members", type: "number", default: 100 },
    { key: "lob", label: "Line of Business", type: "text", default: "Commercial" },
    { key: "target_mlr", label: "Target MLR", type: "number", default: 82, suffix: "%" },
  ],
  ibnr_reserve: [
    { key: "completion_factor_shift_pct", label: "CF Shift", type: "number", default: 2, suffix: "%" },
    { key: "lob", label: "Line of Business", type: "text", hint: "Leave blank for all" },
  ],
};

function buildParams(simType: string, formValues: Record<string, string>): Record<string, unknown> {
  const params: Record<string, unknown> = {};

  // Handle special composite fields
  if (simType === "population_mix") {
    const mix: Record<string, number> = {};
    for (const [k, v] of Object.entries(formValues)) {
      if (k.startsWith("mix_") && v) {
        mix[k.replace("mix_", "")] = Number(v);
      }
    }
    params.mix_changes = mix;
    return params;
  }

  if (simType === "utilization_change") {
    const changes: Record<string, number> = {};
    for (const [k, v] of Object.entries(formValues)) {
      if (k.startsWith("chg_") && v) {
        changes[k.replace("chg_", "")] = Number(v);
      }
    }
    params.changes = changes;
    if (formValues.lob) params.lob = formValues.lob;
    return params;
  }

  // Standard fields
  for (const field of FORM_FIELDS[simType] || []) {
    const val = formValues[field.key];
    if (val === undefined || val === "") continue;
    if (field.type === "number") {
      params[field.key] = Number(val);
    } else {
      params[field.key] = val;
    }
  }
  return params;
}

export default function SimulationBuilder({ onSaved }: SimulationBuilderProps) {
  const [selectedType, setSelectedType] = useState<SimType | "">("");
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<SimulationResult | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [saveName, setSaveName] = useState("");

  // Listen for external type selection (from Dashboard quick-sim cards)
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail as string;
      if (detail && SIM_TYPES.includes(detail as SimType)) {
        selectType(detail as SimType);
      }
    };
    window.addEventListener("select-sim-type", handler);
    return () => window.removeEventListener("select-sim-type", handler);
  }, []);

  const selectType = useCallback((type: SimType) => {
    setSelectedType(type);
    setResult(null);
    setSaved(false);
    // Set defaults
    const defaults: Record<string, string> = {};
    for (const field of FORM_FIELDS[type] || []) {
      if (field.default !== undefined) {
        defaults[field.key] = String(field.default);
      }
    }
    setFormValues(defaults);
    setSaveName(`${SIMULATION_TYPE_LABELS[type]} - ${new Date().toLocaleDateString()}`);
  }, []);

  const handleRun = async () => {
    if (!selectedType) return;
    setRunning(true);
    setResult(null);
    setSaved(false);
    try {
      const params = buildParams(selectedType, formValues);
      const res = await api.simulate({
        simulation_type: selectedType,
        parameters: params,
      });
      setResult(res);
    } catch (err) {
      console.error(err);
    } finally {
      setRunning(false);
    }
  };

  const handleSave = async () => {
    if (!selectedType || !result || !saveName) return;
    setSaving(true);
    try {
      const params = buildParams(selectedType, formValues);
      await api.simulate({
        simulation_type: selectedType,
        parameters: params,
        save: true,
        name: saveName,
      });
      setSaved(true);
      onSaved();
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="p-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">
          Simulation Builder
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          Select a scenario type, configure parameters, and run a what-if analysis
        </p>
      </div>

      {/* Type Selector */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {SIM_TYPES.map((type) => (
          <button
            key={type}
            onClick={() => selectType(type)}
            className={`sim-type-card text-left text-sm ${
              selectedType === type ? "selected" : ""
            }`}
          >
            <div className="font-medium">{SIMULATION_TYPE_LABELS[type]}</div>
          </button>
        ))}
      </div>

      {/* Parameter Form */}
      {selectedType && (
        <div className="card">
          <h2 className="text-lg font-semibold text-databricks-dark mb-4">
            {SIMULATION_TYPE_LABELS[selectedType]} Parameters
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {(FORM_FIELDS[selectedType] || []).map((field) => (
              <div key={field.key}>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  {field.label}
                  {field.suffix && (
                    <span className="text-gray-400 ml-1">({field.suffix})</span>
                  )}
                </label>
                <input
                  type={field.type === "number" ? "number" : "text"}
                  step={field.type === "number" ? "any" : undefined}
                  value={formValues[field.key] || ""}
                  onChange={(e) =>
                    setFormValues((prev) => ({
                      ...prev,
                      [field.key]: e.target.value,
                    }))
                  }
                  placeholder={field.hint}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm
                             focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
                />
                {field.hint && (
                  <p className="text-xs text-gray-400 mt-1">{field.hint}</p>
                )}
              </div>
            ))}
          </div>
          <div className="mt-6 flex gap-3">
            <button onClick={handleRun} disabled={running} className="btn-primary flex items-center gap-2">
              {running ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Play className="w-4 h-4" />
              )}
              {running ? "Running..." : "Run Simulation"}
            </button>
          </div>
        </div>
      )}

      {/* Results */}
      {result && (
        <div className="space-y-4">
          {/* Metric Comparison */}
          <div className="card">
            <h2 className="text-lg font-semibold text-databricks-dark mb-4 flex items-center gap-2">
              <ArrowUpDown className="w-5 h-5" />
              Results: Baseline vs Projected
            </h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 pr-4 font-medium text-gray-500">Metric</th>
                    <th className="text-right py-2 px-4 font-medium text-gray-500">Baseline</th>
                    <th className="text-right py-2 px-4 font-medium text-gray-500">Projected</th>
                    <th className="text-right py-2 px-4 font-medium text-gray-500">Delta</th>
                    <th className="text-right py-2 pl-4 font-medium text-gray-500">% Change</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.keys(result.baseline).map((key) => (
                    <tr key={key} className="border-b border-gray-50">
                      <td className="py-2 pr-4 font-medium capitalize">
                        {key.replace(/_/g, " ")}
                      </td>
                      <td className="text-right py-2 px-4">
                        {formatMetricValue(key, result.baseline[key])}
                      </td>
                      <td className="text-right py-2 px-4">
                        {formatMetricValue(key, result.projected[key])}
                      </td>
                      <td className={`text-right py-2 px-4 font-medium ${deltaColor(result.delta[key])}`}>
                        {deltaArrow(result.delta[key])} {formatMetricValue(key, Math.abs(result.delta[key]))}
                      </td>
                      <td className={`text-right py-2 pl-4 font-medium ${deltaColor(result.delta_pct[key])}`}>
                        {result.delta_pct[key] > 0 ? "+" : ""}
                        {formatPercent(result.delta_pct[key])}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Narrative */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-2">Analysis</h3>
            <p className="text-sm text-gray-700 leading-relaxed">
              {result.narrative}
            </p>
          </div>

          {/* Warnings */}
          {result.warnings.length > 0 && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl p-4">
              <div className="flex items-center gap-2 mb-2">
                <AlertTriangle className="w-5 h-5 text-amber-600" />
                <span className="font-semibold text-amber-800">Warnings</span>
              </div>
              <ul className="space-y-1">
                {result.warnings.map((w, i) => (
                  <li key={i} className="text-sm text-amber-700">
                    {w}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Save */}
          <div className="card flex items-center gap-4">
            <input
              type="text"
              value={saveName}
              onChange={(e) => setSaveName(e.target.value)}
              placeholder="Simulation name"
              className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm
                         focus:outline-none focus:ring-2 focus:ring-databricks-red/30"
            />
            <button
              onClick={handleSave}
              disabled={saving || saved || !saveName}
              className="btn-primary flex items-center gap-2"
            >
              {saved ? (
                <>
                  <Check className="w-4 h-4" /> Saved
                </>
              ) : saving ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin" /> Saving...
                </>
              ) : (
                <>
                  <Save className="w-4 h-4" /> Save to Lakebase
                </>
              )}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function formatMetricValue(key: string, value: number): string {
  const k = key.toLowerCase();
  if (k.includes("mlr") || k.includes("pct") || k.includes("rate") || k.includes("completeness")) {
    return formatPercent(value);
  }
  if (k.includes("premium") || k.includes("claims") || k.includes("revenue") ||
      k.includes("reserve") || k.includes("ibnr") || k.includes("cost") ||
      k.includes("pmpm") || k.includes("excess") || k.includes("threshold")) {
    return formatCurrency(value);
  }
  if (k.includes("member") || k.includes("claimant") || k.includes("count")) {
    return value.toLocaleString();
  }
  if (k.includes("raf") || k.includes("credibility") || k.includes("factor")) {
    return value.toFixed(3);
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
