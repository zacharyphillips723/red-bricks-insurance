import { useEffect, useState } from "react";
import { Target, AlertCircle } from "lucide-react";
import { api, type NetworkGap, type RecruitmentTarget } from "@/lib/api";
import { formatCurrency, formatNumber, formatPercent, gapStatusColor } from "@/lib/utils";

export function GapsRecruitmentPage() {
  const [gaps, setGaps] = useState<NetworkGap[]>([]);
  const [targets, setTargets] = useState<RecruitmentTarget[]>([]);
  const [loading, setLoading] = useState(true);
  const [maxPriority, setMaxPriority] = useState(3);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getNetworkGaps(maxPriority),
      api.getRecruitmentTargets(30),
    ])
      .then(([g, t]) => {
        setGaps(g);
        setTargets(t);
      })
      .finally(() => setLoading(false));
  }, [maxPriority]);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Target className="w-6 h-6 text-databricks-red" />
          Network Gaps & Recruitment
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Prioritized network gaps and OON provider recruitment targets
        </p>
      </div>

      {/* Network Gaps */}
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-red-500" />
            <h3 className="font-semibold text-databricks-dark">Network Gaps</h3>
          </div>
          <select
            value={maxPriority}
            onChange={(e) => setMaxPriority(Number(e.target.value))}
            className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value={1}>Critical Only (P1)</option>
            <option value={2}>P1 + P2</option>
            <option value={3}>P1 + P2 + P3</option>
            <option value={4}>All Priorities</option>
          </select>
        </div>
        {loading ? (
          <div className="p-8 text-center text-gray-400">Loading gaps...</div>
        ) : gaps.length === 0 ? (
          <div className="p-8 text-center text-gray-400">No network gaps at this priority level.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">County</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Type</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Specialty</th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Status</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">% Compliant</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Gap Members</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">CMS Limit (mi)</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Avg Dist (mi)</th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">P</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {gaps.map((g, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="py-2.5 px-4 text-gray-800 font-medium">{g.county_name}</td>
                    <td className="py-2.5 px-4 text-gray-500 text-xs">{g.county_type}</td>
                    <td className="py-2.5 px-4 text-gray-700">{g.cms_specialty_type}</td>
                    <td className="py-2.5 px-4 text-center">
                      <span className={`inline-block text-xs font-semibold px-2.5 py-0.5 rounded-full ${gapStatusColor(g.gap_status)}`}>
                        {g.gap_status}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right">
                      <span className={g.pct_compliant >= 90 ? "text-green-700" : "text-red-700 font-semibold"}>
                        {formatPercent(g.pct_compliant)}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(g.gap_members)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-500">{g.cms_threshold_miles ?? "—"}</td>
                    <td className="py-2.5 px-4 text-right text-gray-500">
                      {g.avg_nearest_distance_mi != null ? g.avg_nearest_distance_mi.toFixed(1) : "—"}
                    </td>
                    <td className="py-2.5 px-4 text-center">
                      <span className={`inline-block w-6 h-6 text-xs font-bold rounded-full flex items-center justify-center ${
                        g.priority_rank === 1 ? "bg-red-100 text-red-700" :
                        g.priority_rank === 2 ? "bg-orange-100 text-orange-700" :
                        g.priority_rank === 3 ? "bg-yellow-100 text-yellow-700" :
                        "bg-gray-100 text-gray-600"
                      }`}>
                        {g.priority_rank}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Recruitment Targets */}
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center gap-2">
            <Target className="w-5 h-5 text-green-600" />
            <h3 className="font-semibold text-databricks-dark">OON Provider Recruitment Targets</h3>
          </div>
          <p className="text-xs text-gray-400 mt-1">
            Ranked by recruitment priority score = (leakage cost / 1000) x (members / 10) x (1 / nearest INN distance)
          </p>
        </div>
        {loading ? (
          <div className="p-8 text-center text-gray-400">Loading targets...</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-center py-3 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">#</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">NPI</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Specialty</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">County</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Claims</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Total Paid</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Potential Savings</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Members</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Avg Dist</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {targets.map((t, i) => (
                  <tr key={t.rendering_provider_npi} className="hover:bg-gray-50">
                    <td className="py-2.5 px-3 text-center text-gray-400 text-xs">{i + 1}</td>
                    <td className="py-2.5 px-4 font-mono text-xs text-gray-700">{t.rendering_provider_npi}</td>
                    <td className="py-2.5 px-4 text-gray-700">{t.specialty ?? "—"}</td>
                    <td className="py-2.5 px-4 text-gray-700">{t.county_name ?? "—"}</td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(t.total_claims)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatCurrency(t.total_paid)}</td>
                    <td className="py-2.5 px-4 text-right text-green-700 font-semibold">{formatCurrency(t.potential_savings)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(t.members_served)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-500">
                      {t.avg_member_distance_mi != null ? `${t.avg_member_distance_mi.toFixed(1)}` : "—"}
                    </td>
                    <td className="py-2.5 px-4 text-right">
                      <span className="inline-block bg-databricks-red text-white text-xs font-bold px-2 py-0.5 rounded-full">
                        {formatNumber(t.recruitment_priority_score)}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
