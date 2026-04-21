import { useEffect, useState } from "react";
import { ShieldCheck, Filter, Download } from "lucide-react";
import { api, type ComplianceRow } from "@/lib/api";
import { formatNumber, formatPercent, complianceBadgeClass } from "@/lib/utils";

export function CompliancePage() {
  const [rows, setRows] = useState<ComplianceRow[]>([]);
  const [counties, setCounties] = useState<string[]>([]);
  const [specialties, setSpecialties] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  const [filterCounty, setFilterCounty] = useState("");
  const [filterSpecialty, setFilterSpecialty] = useState("");
  const [filterCompliant, setFilterCompliant] = useState<string>("");

  useEffect(() => {
    Promise.all([
      api.getComplianceCounties(),
      api.getComplianceSpecialties(),
    ]).then(([c, s]) => {
      setCounties(c);
      setSpecialties(s);
    });
  }, []);

  useEffect(() => {
    setLoading(true);
    const params: Record<string, string> = {};
    if (filterCounty) params.county = filterCounty;
    if (filterSpecialty) params.specialty = filterSpecialty;
    if (filterCompliant === "true") params.compliant_only = "true";
    if (filterCompliant === "false") params.compliant_only = "false";
    api.getCompliance(params).then(setRows).finally(() => setLoading(false));
  }, [filterCounty, filterSpecialty, filterCompliant]);

  const compliantCount = rows.filter((r) => r.is_compliant).length;
  const nonCompliantCount = rows.filter((r) => !r.is_compliant).length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
            <ShieldCheck className="w-6 h-6 text-databricks-red" />
            CMS Compliance (42 CFR 422.116)
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            Network adequacy compliance by county and CMS specialty type — 90% threshold
          </p>
        </div>
      </div>

      {/* Summary badges */}
      <div className="flex gap-4">
        <div className="card px-4 py-3 flex items-center gap-2">
          <span className="text-sm text-gray-500">Total:</span>
          <span className="font-bold text-databricks-dark">{formatNumber(rows.length)}</span>
        </div>
        <div className="card px-4 py-3 flex items-center gap-2">
          <span className="w-2 h-2 bg-green-500 rounded-full" />
          <span className="text-sm text-gray-500">Compliant:</span>
          <span className="font-bold text-green-700">{formatNumber(compliantCount)}</span>
        </div>
        <div className="card px-4 py-3 flex items-center gap-2">
          <span className="w-2 h-2 bg-red-500 rounded-full" />
          <span className="text-sm text-gray-500">Non-Compliant:</span>
          <span className="font-bold text-red-700">{formatNumber(nonCompliantCount)}</span>
        </div>
      </div>

      {/* Filters */}
      <div className="card p-4">
        <div className="flex items-center gap-4 flex-wrap">
          <Filter className="w-4 h-4 text-gray-400" />
          <select
            value={filterCounty}
            onChange={(e) => setFilterCounty(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value="">All Counties</option>
            {counties.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <select
            value={filterSpecialty}
            onChange={(e) => setFilterSpecialty(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value="">All Specialties</option>
            {specialties.map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <select
            value={filterCompliant}
            onChange={(e) => setFilterCompliant(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          >
            <option value="">All Status</option>
            <option value="true">Compliant Only</option>
            <option value="false">Non-Compliant Only</option>
          </select>
          {(filterCounty || filterSpecialty || filterCompliant) && (
            <button
              onClick={() => { setFilterCounty(""); setFilterSpecialty(""); setFilterCompliant(""); }}
              className="text-sm text-databricks-red hover:underline"
            >
              Clear filters
            </button>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400">Loading compliance data...</div>
        ) : rows.length === 0 ? (
          <div className="p-8 text-center text-gray-400">No compliance records match your filters.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">County</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Type</th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">CMS Specialty</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Members</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Compliant</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">% Compliant</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Gap</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Max Mi</th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Avg Dist</th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {rows.map((r, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="py-2.5 px-4 text-gray-800 font-medium">{r.county_name}</td>
                    <td className="py-2.5 px-4 text-gray-500 text-xs">{r.county_type}</td>
                    <td className="py-2.5 px-4 text-gray-700">{r.cms_specialty_type}</td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(r.total_members)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(r.compliant_members)}</td>
                    <td className="py-2.5 px-4 text-right">
                      <span className={r.pct_compliant >= 90 ? "text-green-700 font-semibold" : "text-red-700 font-semibold"}>
                        {formatPercent(r.pct_compliant)}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(r.gap_members)}</td>
                    <td className="py-2.5 px-4 text-right text-gray-500">{r.max_distance_miles}</td>
                    <td className="py-2.5 px-4 text-right text-gray-500">
                      {r.avg_nearest_distance_mi != null ? r.avg_nearest_distance_mi.toFixed(1) : "—"}
                    </td>
                    <td className="py-2.5 px-4 text-center">
                      <span className={complianceBadgeClass(r.is_compliant)}>
                        {r.is_compliant ? "Compliant" : "Non-Compliant"}
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
