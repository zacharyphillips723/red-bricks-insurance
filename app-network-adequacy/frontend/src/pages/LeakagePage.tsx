import { useEffect, useState } from "react";
import { ArrowRightLeft, DollarSign, Users, FileText } from "lucide-react";
import { api, type LeakageSummary } from "@/lib/api";
import { formatCurrency, formatNumber } from "@/lib/utils";

export function LeakagePage() {
  const [data, setData] = useState<LeakageSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getLeakageSummary().then(setData).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">OON Leakage Analysis</h2>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="card p-6 animate-pulse">
              <div className="h-4 bg-gray-200 rounded w-24 mb-3" />
              <div className="h-8 bg-gray-200 rounded w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (!data) return null;

  const summaryCards = [
    {
      label: "Total Leakage Cost",
      value: formatCurrency(data.total_leakage_cost),
      icon: DollarSign,
      color: "text-red-600",
    },
    {
      label: "OON Claims",
      value: formatNumber(data.total_oon_claims),
      icon: FileText,
      color: "text-amber-600",
    },
    {
      label: "Affected Members",
      value: formatNumber(data.total_oon_members),
      icon: Users,
      color: "text-purple-600",
    },
  ];

  const maxSpecCost = Math.max(...data.by_specialty.map((s) => s.leakage_cost), 1);
  const maxCountyCost = Math.max(...data.by_county.map((c) => c.leakage_cost), 1);
  const maxReasonCost = Math.max(...data.by_reason.map((r) => r.leakage_cost), 1);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <ArrowRightLeft className="w-6 h-6 text-databricks-red" />
          OON Leakage Analysis
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Out-of-network utilization cost analysis and leakage drivers
        </p>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {summaryCards.map((card) => {
          const Icon = card.icon;
          return (
            <div key={card.label} className="card p-6">
              <div className="flex items-center gap-2 mb-2">
                <Icon className={`w-4 h-4 ${card.color}`} />
                <span className="text-sm font-medium text-gray-500">{card.label}</span>
              </div>
              <p className={`text-2xl font-bold ${card.color}`}>{card.value}</p>
            </div>
          );
        })}
      </div>

      {/* Three breakdown panels */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* By Specialty */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By Specialty</h3>
          <div className="space-y-3">
            {data.by_specialty.slice(0, 10).map((s) => (
              <div key={s.cms_specialty_type}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-gray-600 truncate mr-2">{s.cms_specialty_type}</span>
                  <span className="text-sm font-semibold text-red-700">{formatCurrency(s.leakage_cost)}</span>
                </div>
                <div className="w-full bg-gray-100 rounded-full h-2">
                  <div
                    className="bg-red-500 h-2 rounded-full"
                    style={{ width: `${(s.leakage_cost / maxSpecCost) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* By County */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By County</h3>
          <div className="space-y-3">
            {data.by_county.slice(0, 10).map((c) => (
              <div key={c.county_name}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-gray-600 truncate mr-2">
                    {c.county_name}
                    <span className="text-xs text-gray-400 ml-1">({c.county_type})</span>
                  </span>
                  <span className="text-sm font-semibold text-red-700">{formatCurrency(c.leakage_cost)}</span>
                </div>
                <div className="w-full bg-gray-100 rounded-full h-2">
                  <div
                    className="bg-amber-500 h-2 rounded-full"
                    style={{ width: `${(c.leakage_cost / maxCountyCost) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* By Reason */}
        <div className="card p-6">
          <h3 className="text-lg font-semibold text-databricks-dark mb-4">By Reason</h3>
          <div className="space-y-3">
            {data.by_reason.map((r) => (
              <div key={r.leakage_reason}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-sm text-gray-600 truncate mr-2">{r.leakage_reason}</span>
                  <span className="text-sm font-semibold text-red-700">{formatCurrency(r.leakage_cost)}</span>
                </div>
                <div className="w-full bg-gray-100 rounded-full h-2">
                  <div
                    className="bg-purple-500 h-2 rounded-full"
                    style={{ width: `${(r.leakage_cost / maxReasonCost) * 100}%` }}
                  />
                </div>
                <div className="flex justify-between text-xs text-gray-400 mt-0.5">
                  <span>{formatNumber(r.total_claims)} claims</span>
                  <span>{formatNumber(r.unique_members)} members</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Specialty detail table */}
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <h3 className="font-semibold text-databricks-dark">Leakage by Specialty — Detail</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">CMS Specialty</th>
                <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Claims</th>
                <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Total Paid</th>
                <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Leakage Cost</th>
                <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">Members</th>
                <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">OON Providers</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {data.by_specialty.map((s) => (
                <tr key={s.cms_specialty_type} className="hover:bg-gray-50">
                  <td className="py-2.5 px-4 text-gray-800 font-medium">{s.cms_specialty_type}</td>
                  <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(s.total_claims)}</td>
                  <td className="py-2.5 px-4 text-right text-gray-700">{formatCurrency(s.total_paid)}</td>
                  <td className="py-2.5 px-4 text-right text-red-700 font-semibold">{formatCurrency(s.leakage_cost)}</td>
                  <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(s.unique_members)}</td>
                  <td className="py-2.5 px-4 text-right text-gray-700">{formatNumber(s.oon_providers)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
