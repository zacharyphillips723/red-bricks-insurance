/**
 * Cohort Builder — Search and analyze member populations.
 */

import { useState, useEffect } from "react";
import {
  Users,
  Search,
  Loader2,
  BarChart3,
  Filter,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { api, type CohortAnalytics, type CohortSearchCriteria, type CohortFilterOptions } from "@/lib/api";

export function CohortBuilder() {
  const [filters, setFilters] = useState<CohortSearchCriteria>({
    risk_tiers: [],
    counties: [],
    lines_of_business: [],
    limit: 100,
  });
  const [filterOptions, setFilterOptions] = useState<CohortFilterOptions | null>(null);
  const [results, setResults] = useState<CohortAnalytics | null>(null);
  const [loading, setLoading] = useState(false);
  const [showFilters, setShowFilters] = useState(true);
  const [showMembers, setShowMembers] = useState(false);

  useEffect(() => {
    api.getCohortFilterOptions().then(setFilterOptions).catch(() => {});
  }, []);

  const handleSearch = async () => {
    setLoading(true);
    try {
      const result = await api.searchCohort(filters);
      setResults(result);
    } catch (e) {
      console.error("Cohort search failed:", e);
    } finally {
      setLoading(false);
    }
  };

  const toggleArrayFilter = (key: "risk_tiers" | "counties" | "lines_of_business", value: string) => {
    setFilters((prev) => {
      const arr = prev[key] || [];
      return {
        ...prev,
        [key]: arr.includes(value) ? arr.filter((v) => v !== value) : [...arr, value],
      };
    });
  };

  const riskColors: Record<string, string> = {
    Critical: "bg-red-500",
    High: "bg-orange-500",
    Elevated: "bg-yellow-500",
    Moderate: "bg-blue-500",
    Low: "bg-green-500",
  };

  const formatCurrency = (val: number | null) =>
    val != null ? `$${val.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 })}` : "N/A";

  return (
    <div>
      <h2 className="text-2xl font-bold text-gray-800 flex items-center gap-2 mb-1">
        <Users className="w-6 h-6 text-red-600" /> Population Cohort Builder
      </h2>
      <p className="text-sm text-gray-500 mb-6">
        Define cohorts, analyze populations, and identify intervention opportunities
      </p>

      {/* Filters Panel */}
      <div className="bg-white rounded-xl border mb-6">
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="w-full flex items-center justify-between px-5 py-3 hover:bg-gray-50"
        >
          <span className="flex items-center gap-2 font-medium text-sm">
            <Filter className="w-4 h-4 text-gray-500" /> Cohort Filters
          </span>
          {showFilters ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </button>

        {showFilters && filterOptions && (
          <div className="px-5 pb-5 space-y-4 border-t">
            {/* Risk Tiers */}
            <div className="pt-4">
              <label className="text-xs font-medium text-gray-500 uppercase mb-2 block">Risk Tier</label>
              <div className="flex flex-wrap gap-2">
                {filterOptions.risk_tiers.filter(Boolean).sort().map((tier) => (
                  <button
                    key={tier}
                    onClick={() => toggleArrayFilter("risk_tiers", tier)}
                    className={`px-3 py-1.5 rounded-full text-xs font-medium border transition-colors ${
                      (filters.risk_tiers || []).includes(tier)
                        ? "bg-red-50 border-red-300 text-red-700"
                        : "bg-white border-gray-200 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    {tier}
                  </button>
                ))}
              </div>
            </div>

            {/* LOB */}
            <div>
              <label className="text-xs font-medium text-gray-500 uppercase mb-2 block">Line of Business</label>
              <div className="flex flex-wrap gap-2">
                {filterOptions.lines_of_business.filter(Boolean).sort().map((lob) => (
                  <button
                    key={lob}
                    onClick={() => toggleArrayFilter("lines_of_business", lob)}
                    className={`px-3 py-1.5 rounded-full text-xs font-medium border transition-colors ${
                      (filters.lines_of_business || []).includes(lob)
                        ? "bg-red-50 border-red-300 text-red-700"
                        : "bg-white border-gray-200 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    {lob}
                  </button>
                ))}
              </div>
            </div>

            {/* Age + RAF + Gender row */}
            <div className="grid grid-cols-4 gap-4">
              <div>
                <label className="text-xs font-medium text-gray-500 uppercase mb-1 block">Min Age</label>
                <input
                  type="number"
                  value={filters.min_age ?? ""}
                  onChange={(e) => setFilters({ ...filters, min_age: e.target.value ? Number(e.target.value) : null })}
                  className="w-full px-3 py-1.5 border rounded-lg text-sm"
                  placeholder="0"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-gray-500 uppercase mb-1 block">Max Age</label>
                <input
                  type="number"
                  value={filters.max_age ?? ""}
                  onChange={(e) => setFilters({ ...filters, max_age: e.target.value ? Number(e.target.value) : null })}
                  className="w-full px-3 py-1.5 border rounded-lg text-sm"
                  placeholder="120"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-gray-500 uppercase mb-1 block">Min RAF Score</label>
                <input
                  type="number"
                  step="0.1"
                  value={filters.min_raf_score ?? ""}
                  onChange={(e) => setFilters({ ...filters, min_raf_score: e.target.value ? Number(e.target.value) : null })}
                  className="w-full px-3 py-1.5 border rounded-lg text-sm"
                  placeholder="0.0"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-gray-500 uppercase mb-1 block">Gender</label>
                <select
                  value={filters.gender || ""}
                  onChange={(e) => setFilters({ ...filters, gender: e.target.value || null })}
                  className="w-full px-3 py-1.5 border rounded-lg text-sm"
                >
                  <option value="">All</option>
                  {filterOptions.genders.filter(Boolean).map((g) => (
                    <option key={g} value={g}>{g}</option>
                  ))}
                </select>
              </div>
            </div>

            {/* Diagnosis search + HEDIS gaps */}
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-xs font-medium text-gray-500 uppercase mb-1 block">Diagnoses Contain</label>
                <input
                  type="text"
                  value={filters.diagnoses_contain || ""}
                  onChange={(e) => setFilters({ ...filters, diagnoses_contain: e.target.value || null })}
                  className="w-full px-3 py-1.5 border rounded-lg text-sm"
                  placeholder="e.g., diabetes, hypertension"
                />
              </div>
              <div className="flex items-end">
                <label className="flex items-center gap-2 text-sm text-gray-600 pb-1">
                  <input
                    type="checkbox"
                    checked={filters.has_hedis_gaps ?? false}
                    onChange={(e) => setFilters({ ...filters, has_hedis_gaps: e.target.checked || null })}
                    className="rounded"
                  />
                  Has open HEDIS gaps
                </label>
              </div>
            </div>

            <button
              onClick={handleSearch}
              disabled={loading}
              className="flex items-center gap-2 px-5 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm font-medium"
            >
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
              {loading ? "Searching..." : "Search Cohort"}
            </button>
          </div>
        )}
      </div>

      {/* Results */}
      {results && (
        <div className="space-y-4">
          {/* KPI Cards */}
          <div className="grid grid-cols-4 gap-4">
            {[
              { label: "Total Members", value: results.total_members.toLocaleString() },
              { label: "Avg RAF Score", value: results.avg_raf_score?.toFixed(2) ?? "N/A" },
              { label: "Avg Age", value: results.avg_age?.toFixed(0) ?? "N/A" },
              { label: "Total Cost YTD", value: formatCurrency(results.total_cost_ytd) },
            ].map((kpi) => (
              <div key={kpi.label} className="bg-white rounded-xl border p-4">
                <p className="text-xs text-gray-500 uppercase">{kpi.label}</p>
                <p className="text-2xl font-bold text-gray-800 mt-1">{kpi.value}</p>
              </div>
            ))}
          </div>

          {/* Analytics row */}
          <div className="grid grid-cols-3 gap-4">
            {/* Risk Distribution */}
            <div className="bg-white rounded-xl border p-4">
              <h4 className="text-sm font-semibold text-gray-700 flex items-center gap-1 mb-3">
                <BarChart3 className="w-4 h-4" /> Risk Distribution
              </h4>
              <div className="space-y-2">
                {Object.entries(results.risk_distribution)
                  .sort(([a], [b]) => {
                    const order = ["Critical", "High", "Elevated", "Moderate", "Low"];
                    return order.indexOf(a) - order.indexOf(b);
                  })
                  .map(([tier, count]) => (
                    <div key={tier} className="flex items-center gap-2">
                      <span className={`w-2.5 h-2.5 rounded-full ${riskColors[tier] || "bg-gray-400"}`} />
                      <span className="text-sm text-gray-600 flex-1">{tier}</span>
                      <span className="text-sm font-medium">{count}</span>
                      <span className="text-xs text-gray-400">
                        ({((count / results.total_members) * 100).toFixed(0)}%)
                      </span>
                    </div>
                  ))}
              </div>
            </div>

            {/* Gender Distribution */}
            <div className="bg-white rounded-xl border p-4">
              <h4 className="text-sm font-semibold text-gray-700 mb-3">Gender Distribution</h4>
              <div className="space-y-2">
                {Object.entries(results.gender_distribution).map(([g, count]) => (
                  <div key={g} className="flex items-center justify-between">
                    <span className="text-sm text-gray-600">{g}</span>
                    <span className="text-sm font-medium">{count}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* LOB Distribution */}
            <div className="bg-white rounded-xl border p-4">
              <h4 className="text-sm font-semibold text-gray-700 mb-3">Line of Business</h4>
              <div className="space-y-2">
                {Object.entries(results.lob_distribution).map(([lob, count]) => (
                  <div key={lob} className="flex items-center justify-between">
                    <span className="text-sm text-gray-600 truncate">{lob}</span>
                    <span className="text-sm font-medium">{count}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Additional stats */}
          <div className="grid grid-cols-2 gap-4">
            <div className="bg-white rounded-xl border p-4">
              <p className="text-xs text-gray-500 uppercase">Total HEDIS Gaps</p>
              <p className="text-2xl font-bold text-gray-800 mt-1">{results.total_hedis_gaps.toLocaleString()}</p>
            </div>
            <div className="bg-white rounded-xl border p-4">
              <p className="text-xs text-gray-500 uppercase">Avg Cost per Member</p>
              <p className="text-2xl font-bold text-gray-800 mt-1">{formatCurrency(results.avg_cost_per_member)}</p>
            </div>
          </div>

          {/* Member Table */}
          <div className="bg-white rounded-xl border overflow-hidden">
            <button
              onClick={() => setShowMembers(!showMembers)}
              className="w-full flex items-center justify-between px-5 py-3 hover:bg-gray-50"
            >
              <span className="font-medium text-sm">
                Members ({results.total_members})
              </span>
              {showMembers ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
            </button>

            {showMembers && (
              <div className="overflow-x-auto border-t">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50">
                    <tr className="text-xs text-gray-500 uppercase">
                      <th className="px-4 py-2 text-left">Member</th>
                      <th className="px-4 py-2 text-left">Age</th>
                      <th className="px-4 py-2 text-left">Gender</th>
                      <th className="px-4 py-2 text-left">County</th>
                      <th className="px-4 py-2 text-left">Risk</th>
                      <th className="px-4 py-2 text-left">RAF</th>
                      <th className="px-4 py-2 text-left">LOB</th>
                      <th className="px-4 py-2 text-left">HEDIS Gaps</th>
                      <th className="px-4 py-2 text-right">Cost YTD</th>
                    </tr>
                  </thead>
                  <tbody>
                    {results.members.map((m) => (
                      <tr key={m.member_id} className="border-t hover:bg-gray-50">
                        <td className="px-4 py-2">
                          <span className="font-medium">{m.member_name || m.member_id}</span>
                          <span className="text-gray-400 text-xs ml-1">{m.member_id}</span>
                        </td>
                        <td className="px-4 py-2 text-gray-600">{m.age}</td>
                        <td className="px-4 py-2 text-gray-600">{m.gender}</td>
                        <td className="px-4 py-2 text-gray-600">{m.county}</td>
                        <td className="px-4 py-2">
                          <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                            m.risk_tier === "Critical" || m.risk_tier === "High"
                              ? "bg-red-100 text-red-700"
                              : m.risk_tier === "Elevated"
                              ? "bg-yellow-100 text-yellow-700"
                              : "bg-gray-100 text-gray-600"
                          }`}>
                            {m.risk_tier}
                          </span>
                        </td>
                        <td className="px-4 py-2 text-gray-600">{m.raf_score}</td>
                        <td className="px-4 py-2 text-gray-600 truncate max-w-[120px]">{m.line_of_business}</td>
                        <td className="px-4 py-2 text-gray-600">{m.hedis_gap_count}</td>
                        <td className="px-4 py-2 text-right text-gray-600">
                          {m.total_paid_ytd ? `$${Number(m.total_paid_ytd).toLocaleString()}` : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {!results && !loading && (
        <div className="text-center py-16 text-gray-400">
          <Users className="w-12 h-12 mx-auto mb-3 opacity-50" />
          <h3 className="font-semibold text-lg text-gray-500">Build a Cohort</h3>
          <p className="text-sm">Use the filters above to define a population and analyze member demographics</p>
        </div>
      )}
    </div>
  );
}
