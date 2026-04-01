import { useState, useEffect } from "react";
import { GitCompareArrows, Plus, RefreshCw, Save, Check } from "lucide-react";
import { api, SimulationListItem, SimulationDetail } from "@/lib/api";
import {
  formatCurrency,
  formatPercent,
  SIMULATION_TYPE_LABELS,
  deltaColor,
  deltaArrow,
} from "@/lib/utils";

export default function ScenarioComparison() {
  const [simulations, setSimulations] = useState<SimulationListItem[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [details, setDetails] = useState<SimulationDetail[]>([]);
  const [loading, setLoading] = useState(true);
  const [comparing, setComparing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [compName, setCompName] = useState("");

  useEffect(() => {
    api.listSimulations().then((sims) => {
      setSimulations(sims);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      if (prev.includes(id)) return prev.filter((x) => x !== id);
      if (prev.length >= 4) return prev;
      return [...prev, id];
    });
    setDetails([]);
    setSaved(false);
  };

  const handleCompare = async () => {
    if (selected.length < 2) return;
    setComparing(true);
    try {
      const results = await Promise.all(
        selected.map((id) => api.getSimulation(id))
      );
      setDetails(results);
    } catch (err) {
      console.error(err);
    } finally {
      setComparing(false);
    }
  };

  const handleSaveComparison = async () => {
    if (!compName || selected.length < 2) return;
    setSaving(true);
    try {
      await api.createComparison({
        comparison_name: compName,
        simulation_ids: selected,
      });
      setSaved(true);
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  // Extract all metric keys from results
  const metricKeys = details.length > 0
    ? Object.keys(
        (details[0].results as Record<string, Record<string, number>> | null)?.baseline || {}
      )
    : [];

  if (loading) {
    return (
      <div className="p-8 flex items-center justify-center h-full">
        <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
      </div>
    );
  }

  return (
    <div className="p-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <GitCompareArrows className="w-6 h-6" />
          Compare Scenarios
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          Select 2-4 saved simulations for side-by-side comparison
        </p>
      </div>

      {/* Selection */}
      {simulations.length === 0 ? (
        <div className="card text-center text-gray-500 py-12">
          No saved simulations yet. Run and save simulations in the Builder first.
        </div>
      ) : (
        <div className="card">
          <h2 className="font-semibold text-databricks-dark mb-3">
            Select Simulations ({selected.length}/4)
          </h2>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {simulations.map((sim) => (
              <label
                key={sim.simulation_id}
                className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                  selected.includes(sim.simulation_id)
                    ? "border-databricks-red bg-red-50"
                    : "border-gray-200 hover:border-gray-300"
                }`}
              >
                <input
                  type="checkbox"
                  checked={selected.includes(sim.simulation_id)}
                  onChange={() => toggleSelect(sim.simulation_id)}
                  disabled={
                    !selected.includes(sim.simulation_id) && selected.length >= 4
                  }
                  className="accent-databricks-red"
                />
                <div className="flex-1">
                  <div className="font-medium text-sm">{sim.simulation_name}</div>
                  <div className="text-xs text-gray-500">
                    {SIMULATION_TYPE_LABELS[sim.simulation_type] || sim.simulation_type}
                    {sim.scope_lob && ` \u2022 ${sim.scope_lob}`}
                  </div>
                </div>
                <span className="text-xs text-gray-400">{sim.status}</span>
              </label>
            ))}
          </div>
          <div className="mt-4">
            <button
              onClick={handleCompare}
              disabled={selected.length < 2 || comparing}
              className="btn-primary flex items-center gap-2"
            >
              {comparing ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Plus className="w-4 h-4" />
              )}
              {comparing ? "Loading..." : "Compare Selected"}
            </button>
          </div>
        </div>
      )}

      {/* Comparison Table */}
      {details.length >= 2 && (
        <div className="card overflow-x-auto">
          <h2 className="font-semibold text-databricks-dark mb-4">
            Side-by-Side Comparison
          </h2>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left py-2 pr-4 font-medium text-gray-500">
                  Metric
                </th>
                {details.map((d) => (
                  <th
                    key={d.simulation_id}
                    className="text-right py-2 px-4 font-medium text-gray-500"
                  >
                    <div>{d.simulation_name}</div>
                    <div className="text-xs font-normal text-gray-400">
                      {SIMULATION_TYPE_LABELS[d.simulation_type] || d.simulation_type}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {/* Baseline row */}
              <tr className="bg-gray-50">
                <td className="py-2 pr-4 font-medium text-gray-500" colSpan={details.length + 1}>
                  Baseline Values
                </td>
              </tr>
              {metricKeys.map((key) => (
                <tr key={`base-${key}`} className="border-b border-gray-50">
                  <td className="py-1.5 pr-4 text-gray-600 capitalize pl-4">
                    {key.replace(/_/g, " ")}
                  </td>
                  {details.map((d) => {
                    const results = d.results as Record<string, Record<string, number>> | null;
                    const val = results?.baseline?.[key] ?? 0;
                    return (
                      <td key={d.simulation_id} className="text-right py-1.5 px-4">
                        {formatAutoMetric(key, val)}
                      </td>
                    );
                  })}
                </tr>
              ))}

              {/* Projected row */}
              <tr className="bg-gray-50">
                <td className="py-2 pr-4 font-medium text-gray-500" colSpan={details.length + 1}>
                  Projected Values
                </td>
              </tr>
              {metricKeys.map((key) => (
                <tr key={`proj-${key}`} className="border-b border-gray-50">
                  <td className="py-1.5 pr-4 text-gray-600 capitalize pl-4">
                    {key.replace(/_/g, " ")}
                  </td>
                  {details.map((d) => {
                    const results = d.results as Record<string, Record<string, number>> | null;
                    const val = results?.projected?.[key] ?? 0;
                    const delta = results?.delta?.[key] ?? 0;
                    return (
                      <td
                        key={d.simulation_id}
                        className={`text-right py-1.5 px-4 font-medium ${deltaColor(delta)}`}
                      >
                        {formatAutoMetric(key, val)}{" "}
                        <span className="text-xs">{deltaArrow(delta)}</span>
                      </td>
                    );
                  })}
                </tr>
              ))}

              {/* Narrative */}
              <tr className="bg-gray-50">
                <td className="py-2 pr-4 font-medium text-gray-500" colSpan={details.length + 1}>
                  Narrative
                </td>
              </tr>
              <tr>
                <td className="py-2" />
                {details.map((d) => {
                  const results = d.results as Record<string, unknown> | null;
                  return (
                    <td key={d.simulation_id} className="py-2 px-4 text-xs text-gray-600 align-top">
                      {(results?.narrative as string) || "—"}
                    </td>
                  );
                })}
              </tr>
            </tbody>
          </table>

          {/* Save Comparison */}
          <div className="mt-4 flex items-center gap-3 border-t border-gray-100 pt-4">
            <input
              type="text"
              value={compName}
              onChange={(e) => setCompName(e.target.value)}
              placeholder="Comparison name"
              className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm
                         focus:outline-none focus:ring-2 focus:ring-databricks-red/30"
            />
            <button
              onClick={handleSaveComparison}
              disabled={saving || saved || !compName}
              className="btn-secondary flex items-center gap-2"
            >
              {saved ? <Check className="w-4 h-4" /> : <Save className="w-4 h-4" />}
              {saved ? "Saved" : "Save Comparison"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function formatAutoMetric(key: string, value: number): string {
  const k = key.toLowerCase();
  if (k.includes("mlr") || k.includes("pct") || k.includes("rate") || k.includes("completeness"))
    return formatPercent(value);
  if (
    k.includes("premium") || k.includes("claims") || k.includes("revenue") ||
    k.includes("cost") || k.includes("reserve") || k.includes("ibnr") ||
    k.includes("pmpm") || k.includes("threshold") || k.includes("excess")
  )
    return formatCurrency(value);
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
