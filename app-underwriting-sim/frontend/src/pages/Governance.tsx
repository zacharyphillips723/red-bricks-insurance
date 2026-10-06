import { useEffect, useState } from "react";
import { ShieldCheck, Database, FileCheck, Scale, RefreshCw, CheckCircle2, AlertCircle } from "lucide-react";
import { api, FactorTables } from "@/lib/api";

/**
 * Governance & Explainability surface — the NAIC-AI alignment panel. Surfaces
 * what the engine already produces: the provenance of the rating factors, an
 * actuarial-model inventory, the explainability basis for recommendations, and
 * the consumer adverse-action / human-oversight posture.
 */
export default function Governance() {
  const [factors, setFactors] = useState<FactorTables | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .getFactorTables()
      .then(setFactors)
      .catch(() => setFactors(null))
      .finally(() => setLoading(false));
  }, []);

  const governed = factors?.source === "uc_table";

  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Governance & Explainability</h1>
        <p className="text-sm text-gray-500 mt-1">
          Model-risk posture for the underwriting engine — factor provenance, model inventory, and the
          explainability basis behind every rate and recommendation (NAIC AI Model Bulletin alignment).
        </p>
      </div>

      {/* Factor provenance */}
      <div className="card">
        <div className="flex items-center gap-3 mb-4">
          <Database className="w-5 h-5 text-databricks-red" />
          <h2 className="text-lg font-semibold text-databricks-dark">Rating Factor Provenance</h2>
          {loading ? (
            <RefreshCw className="w-4 h-4 animate-spin text-gray-400" />
          ) : governed ? (
            <span className="badge-positive flex items-center gap-1">
              <CheckCircle2 className="w-3 h-3" /> Governed (Unity Catalog)
            </span>
          ) : (
            <span className="badge-warning flex items-center gap-1">
              <AlertCircle className="w-3 h-3" /> Built-in fallback
            </span>
          )}
        </div>
        <p className="text-sm text-gray-600 leading-relaxed">
          {governed ? (
            <>
              Rating factors are read from the governed Unity Catalog table{" "}
              <code className="bg-gray-100 px-1.5 py-0.5 rounded text-xs">analytics.gold_pricing_factors</code>
              , with full lineage, access control, and an actuarial review trail. The pricing engine
              overlays this table over its code defaults on a 15-minute refresh.
            </>
          ) : (
            <>
              Rating factors are currently served from the engine's built-in actuarial defaults because
              the governed Unity Catalog table{" "}
              <code className="bg-gray-100 px-1.5 py-0.5 rounded text-xs">analytics.gold_pricing_factors</code>{" "}
              was not reachable. Publish that table to put factors under lineage and actuarial version
              control.
            </>
          )}
        </p>
        {factors && (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mt-4">
            {[
              ["Age", factors.age_factors],
              ["Area", factors.area_factors],
              ["Industry", factors.industry_factors],
              ["Trend", factors.trend_factors],
              ["Experience Mod", factors.experience_mod_ranges],
            ].map(([label, tbl]) => (
              <div key={label as string} className="rounded-lg border border-gray-100 p-3">
                <div className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                  {label as string}
                </div>
                <div className="text-lg font-bold text-databricks-dark">
                  {(tbl as FactorTables["age_factors"]).factors.length}
                </div>
                <div className="text-xs text-gray-400">factor rows</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Model inventory */}
      <div className="card">
        <div className="flex items-center gap-3 mb-4">
          <FileCheck className="w-5 h-5 text-databricks-red" />
          <h2 className="text-lg font-semibold text-databricks-dark">Model Inventory</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 text-left text-gray-500">
                <th className="py-2 pr-4 font-medium">Model / Component</th>
                <th className="py-2 px-4 font-medium">Type</th>
                <th className="py-2 px-4 font-medium">Basis</th>
                <th className="py-2 pl-4 font-medium">Human Oversight</th>
              </tr>
            </thead>
            <tbody className="text-gray-700">
              {MODEL_INVENTORY.map((m) => (
                <tr key={m.name} className="border-b border-gray-50">
                  <td className="py-2 pr-4 font-medium text-databricks-dark">{m.name}</td>
                  <td className="py-2 px-4">{m.type}</td>
                  <td className="py-2 px-4">{m.basis}</td>
                  <td className="py-2 pl-4">{m.oversight}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Explainability + adverse-action */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="card">
          <div className="flex items-center gap-3 mb-3">
            <Scale className="w-5 h-5 text-databricks-red" />
            <h2 className="text-lg font-semibold text-databricks-dark">Explainability Basis</h2>
          </div>
          <ul className="space-y-2 text-sm text-gray-600 list-disc pl-5 leading-relaxed">
            <li>
              Every quoted rate is produced by a <strong>step-by-step build-up</strong> (base rate →
              age → area → industry → trend → experience) that is shown in full on the Rate Build-Up
              screen — no opaque scoring.
            </li>
            <li>
              Packaged-scenario recommendations rank on a <strong>documented formula</strong> (margin
              dollars × a transparent logistic retention curve), not a learned black box.
            </li>
            <li>
              The conversational agent is <strong>analytical, not decisioning</strong>; it narrates and
              re-runs the deterministic engine, and its traces are captured for audit.
            </li>
          </ul>
        </div>

        <div className="card">
          <div className="flex items-center gap-3 mb-3">
            <ShieldCheck className="w-5 h-5 text-databricks-red" />
            <h2 className="text-lg font-semibold text-databricks-dark">Oversight & Adverse Action</h2>
          </div>
          <ul className="space-y-2 text-sm text-gray-600 list-disc pl-5 leading-relaxed">
            <li>
              All pricing is group-level and <strong>community-rated</strong>; the engine does not use
              individual health status as a rating input.
            </li>
            <li>
              Rate changes above authority thresholds route to a <strong>human approver</strong> and are
              recorded in the simulation audit log with actor and timestamp.
            </li>
            <li>
              Factor governance keeps actuaries in the <strong>review → approve → version</strong> loop;
              the system computes, actuaries govern.
            </li>
            <li className="text-gray-400">
              Roadmap: a pre-implementation digital-twin check that cites institutional precedent before
              million-dollar policies are committed.
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
}

const MODEL_INVENTORY: { name: string; type: string; basis: string; oversight: string }[] = [
  {
    name: "Rate Build-Up Engine",
    type: "Deterministic actuarial",
    basis: "Community-rated factor cascade",
    oversight: "Actuary-governed factors",
  },
  {
    name: "Simulation Engine (11 models)",
    type: "Deterministic actuarial",
    basis: "Closed-form financial projections",
    oversight: "Underwriter review",
  },
  {
    name: "Scenario Recommendation",
    type: "Rules + logistic curve",
    basis: "Expected retained margin",
    oversight: "Underwriter decides",
  },
  {
    name: "Underwriting Agent",
    type: "LLM (analytical)",
    basis: "Narrates / invokes the engine",
    oversight: "Non-decisioning, traced",
  },
];
