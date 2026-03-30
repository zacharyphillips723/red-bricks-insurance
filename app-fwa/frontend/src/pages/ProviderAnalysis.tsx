import { useState, useEffect } from "react";
import { Search, ShieldAlert, AlertTriangle, TrendingUp, Loader2 } from "lucide-react";
import { api, type ProviderRisk } from "@/lib/api";

interface ProviderAnalysisProps {
  initialNpi?: string;
}

export function ProviderAnalysis({ initialNpi }: ProviderAnalysisProps) {
  const [npi, setNpi] = useState(initialNpi || "");
  const [searchNpi, setSearchNpi] = useState(initialNpi || "");
  const [profile, setProfile] = useState<ProviderRisk | null>(null);
  const [claims, setClaims] = useState<Record<string, unknown>[]>([]);
  const [mlScores, setMlScores] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSearch = async (searchValue?: string) => {
    const query = searchValue || npi;
    if (!query.trim()) return;
    setSearchNpi(query);
    setLoading(true);
    setError("");
    setProfile(null);
    setClaims([]);
    setMlScores([]);
    try {
      const [profileData, claimsData, mlData] = await Promise.all([
        api.getProviderRisk(query),
        api.getProviderClaims(query),
        api.getProviderMLScores(query),
      ]);
      setProfile(profileData);
      setClaims(claimsData);
      setMlScores(mlData);
    } catch (err) {
      setError(`Provider ${query} not found or an error occurred.`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (initialNpi) handleSearch(initialNpi);
  }, [initialNpi]);

  const riskTierColor = (tier: string | null) => {
    if (!tier) return "text-gray-500";
    if (tier.toLowerCase().includes("critical") || tier.toLowerCase().includes("very high")) return "text-red-600";
    if (tier.toLowerCase().includes("high")) return "text-orange-600";
    if (tier.toLowerCase().includes("medium")) return "text-yellow-600";
    return "text-green-600";
  };

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
        <ShieldAlert className="w-6 h-6 text-databricks-red" /> Provider Analysis
      </h2>

      {/* Search */}
      <div className="card p-4">
        <div className="flex gap-3">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              value={npi}
              onChange={(e) => setNpi(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleSearch()}
              placeholder="Enter Provider NPI (e.g. 1234567890)"
              className="w-full pl-10 pr-4 py-2.5 text-sm border border-gray-300 rounded-lg"
            />
          </div>
          <button onClick={() => handleSearch()} disabled={!npi.trim() || loading} className="btn-primary flex items-center gap-2">
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
            Analyze
          </button>
        </div>
      </div>

      {error && <div className="card p-4 text-sm text-red-600">{error}</div>}

      {loading && (
        <div className="card p-8 text-center text-gray-500">
          <Loader2 className="w-6 h-6 animate-spin mx-auto mb-2" />
          Loading provider analysis...
        </div>
      )}

      {profile && (
        <>
          {/* Provider header */}
          <div className="card p-6">
            <div className="flex items-start justify-between">
              <div>
                <h3 className="text-xl font-bold text-databricks-dark">
                  {profile.provider_name || `NPI: ${profile.provider_npi}`}
                </h3>
                <p className="text-sm text-gray-500">{profile.specialty} | NPI: {profile.provider_npi}</p>
              </div>
              <div className="text-right">
                <div className={`text-2xl font-bold ${riskTierColor(profile.risk_tier)}`}>
                  {profile.risk_tier || "Unknown"}
                </div>
                <p className="text-xs text-gray-400">Risk Tier</p>
              </div>
            </div>
          </div>

          {/* Metrics grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
            {[
              { label: "Total Claims", value: profile.total_claims },
              { label: "Total Billed", value: profile.total_billed, prefix: "$" },
              { label: "Total Paid", value: profile.total_paid, prefix: "$" },
              { label: "Billed/Allowed Ratio", value: profile.billed_to_allowed_ratio },
              { label: "E5 Visit %", value: profile.e5_visit_pct },
              { label: "Denial Rate", value: profile.denial_rate },
              { label: "FWA Signals", value: profile.fwa_signal_count },
              { label: "Avg Fraud Score", value: profile.fwa_avg_score },
              { label: "Est. Overpayment", value: profile.fwa_estimated_overpayment, prefix: "$" },
              { label: "Composite Risk", value: profile.composite_risk_score },
              { label: "Specialty Rank", value: profile.specialty_risk_rank },
              { label: "Overall Rank", value: profile.overall_risk_rank },
            ].map((m) => (
              <div key={m.label} className="card p-4">
                <div className="text-xs text-gray-500 mb-1">{m.label}</div>
                <div className="text-sm font-bold text-databricks-dark">
                  {m.value != null ? `${m.prefix || ""}${m.value}` : "—"}
                </div>
              </div>
            ))}
          </div>

          {/* ML Model Scores */}
          {mlScores.length > 0 && (
            <div className="card p-5">
              <h3 className="text-lg font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <TrendingUp className="w-5 h-5 text-purple-600" /> ML Model Predictions ({mlScores.length})
              </h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 border-b">
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Claim ID</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">ML Probability</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Risk Tier</th>
                      <th className="text-right py-2 px-3 text-xs font-medium text-gray-500 uppercase">Billed</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Procedure</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Type</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {mlScores.slice(0, 20).map((row, i) => (
                      <tr key={i} className="hover:bg-gray-50">
                        <td className="py-2 px-3 font-mono text-xs">{String(row.claim_id ?? "—")}</td>
                        <td className="py-2 px-3">
                          <div className="flex items-center gap-2">
                            <div className="w-16 bg-gray-100 rounded-full h-1.5">
                              <div
                                className={`h-1.5 rounded-full ${
                                  Number(row.ml_fraud_probability) > 0.7 ? "bg-red-500" :
                                  Number(row.ml_fraud_probability) > 0.4 ? "bg-amber-500" : "bg-green-500"
                                }`}
                                style={{ width: `${Number(row.ml_fraud_probability) * 100}%` }}
                              />
                            </div>
                            <span className="text-xs">{(Number(row.ml_fraud_probability) * 100).toFixed(1)}%</span>
                          </div>
                        </td>
                        <td className="py-2 px-3 text-xs">{String(row.ml_risk_tier ?? "—")}</td>
                        <td className="py-2 px-3 text-right text-xs">${String(row.billed_amount ?? "—")}</td>
                        <td className="py-2 px-3 text-xs">{String(row.procedure_code ?? "—")}</td>
                        <td className="py-2 px-3 text-xs">{String(row.claim_type ?? "—")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Flagged Claims */}
          {claims.length > 0 && (
            <div className="card p-5">
              <h3 className="text-lg font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-amber-600" /> Flagged Claims ({claims.length})
              </h3>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 border-b">
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Claim ID</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Fraud Type</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Score</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Severity</th>
                      <th className="text-right py-2 px-3 text-xs font-medium text-gray-500 uppercase">Est. Overpmt</th>
                      <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Evidence</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {claims.slice(0, 20).map((row, i) => (
                      <tr key={i} className="hover:bg-gray-50">
                        <td className="py-2 px-3 font-mono text-xs">{String(row.claim_id ?? "—")}</td>
                        <td className="py-2 px-3 text-xs">{String(row.fraud_type ?? "—")}</td>
                        <td className="py-2 px-3 text-xs">{String(row.fraud_score ?? "—")}</td>
                        <td className="py-2 px-3 text-xs">{String(row.severity ?? "—")}</td>
                        <td className="py-2 px-3 text-right text-xs">${String(row.estimated_overpayment ?? "—")}</td>
                        <td className="py-2 px-3 text-xs text-gray-500 max-w-xs truncate">
                          {String(row.evidence_summary ?? "—")}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}

      {!profile && !loading && !error && (
        <div className="card p-8 text-center">
          <ShieldAlert className="w-12 h-12 text-gray-300 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-databricks-dark mb-2">
            Enter a Provider NPI to analyze
          </h3>
          <p className="text-sm text-gray-500">
            View risk scorecards, flagged claims, ML model predictions, and peer comparisons
          </p>
        </div>
      )}
    </div>
  );
}
