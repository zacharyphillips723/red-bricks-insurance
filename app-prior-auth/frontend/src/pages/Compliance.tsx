import { useState, useEffect } from "react";
import { api, ComplianceMetrics, OverdueRequest } from "@/lib/api";
import {
  Shield,
  Clock,
  AlertTriangle,
  Zap,
  CheckCircle,
  XCircle,
  Timer,
} from "lucide-react";

interface ComplianceProps {
  onSelectRequest: (id: string) => void;
}

export function Compliance({ onSelectRequest }: ComplianceProps) {
  const [metrics, setMetrics] = useState<ComplianceMetrics | null>(null);
  const [overdue, setOverdue] = useState<OverdueRequest[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.getComplianceMetrics(),
      api.getOverdueRequests(),
    ])
      .then(([m, o]) => {
        setMetrics(m);
        setOverdue(o);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark">CMS Compliance Dashboard</h2>
        <div className="grid grid-cols-5 gap-4">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="card animate-pulse h-28 bg-gray-100" />
          ))}
        </div>
        <div className="grid grid-cols-2 gap-4">
          {Array.from({ length: 2 }).map((_, i) => (
            <div key={i} className="card animate-pulse h-64 bg-gray-100" />
          ))}
        </div>
      </div>
    );
  }

  if (!metrics) return <div className="text-red-600">Failed to load compliance data.</div>;

  const complianceGood = metrics.compliance_rate !== null && metrics.compliance_rate >= 95;
  const standardGood = metrics.avg_turnaround_standard !== null && metrics.avg_turnaround_standard < 72;
  const expeditedGood = metrics.avg_turnaround_expedited !== null && metrics.avg_turnaround_expedited < 24;

  // SVG chart dimensions
  const chartW = 560;
  const chartH = 200;
  const barPadding = 4;
  const maxCount = Math.max(...metrics.turnaround_distribution.map((b) => b.count), 1);
  const bucketCount = metrics.turnaround_distribution.length || 1;
  const barW = (chartW - 60) / bucketCount - barPadding;

  // Trend chart
  const trendW = 560;
  const trendH = 200;
  const trendData = metrics.weekly_trend;
  const trendMaxTotal = Math.max(...trendData.map((t) => t.total), 1);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark">CMS Compliance Dashboard</h2>
        <p className="text-sm text-gray-500 mt-1">CMS-0057-F Prior Authorization compliance metrics and monitoring</p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-5 gap-4">
        <div className="card">
          <div className="flex items-center gap-2 mb-2">
            <div className={`p-1.5 rounded-lg bg-gray-50 ${complianceGood ? "text-green-600" : "text-red-600"}`}>
              <Shield size={18} />
            </div>
            <span className="text-xs text-gray-500">CMS Compliance</span>
          </div>
          <p className={`text-3xl font-bold ${complianceGood ? "text-green-600" : "text-red-600"}`}>
            {metrics.compliance_rate !== null ? `${metrics.compliance_rate}%` : "N/A"}
          </p>
          <p className="text-xs text-gray-400 mt-1">Target: 100%</p>
        </div>

        <div className="card">
          <div className="flex items-center gap-2 mb-2">
            <div className={`p-1.5 rounded-lg bg-gray-50 ${standardGood ? "text-green-600" : "text-amber-600"}`}>
              <Clock size={18} />
            </div>
            <span className="text-xs text-gray-500">Standard Avg</span>
          </div>
          <p className={`text-3xl font-bold ${standardGood ? "text-green-600" : "text-amber-600"}`}>
            {metrics.avg_turnaround_standard !== null ? `${metrics.avg_turnaround_standard}h` : "N/A"}
          </p>
          <p className="text-xs text-gray-400 mt-1">CMS limit: 72h</p>
        </div>

        <div className="card">
          <div className="flex items-center gap-2 mb-2">
            <div className={`p-1.5 rounded-lg bg-gray-50 ${expeditedGood ? "text-green-600" : "text-red-600"}`}>
              <Zap size={18} />
            </div>
            <span className="text-xs text-gray-500">Expedited Avg</span>
          </div>
          <p className={`text-3xl font-bold ${expeditedGood ? "text-green-600" : "text-red-600"}`}>
            {metrics.avg_turnaround_expedited !== null ? `${metrics.avg_turnaround_expedited}h` : "N/A"}
          </p>
          <p className="text-xs text-gray-400 mt-1">CMS limit: 24h</p>
        </div>

        <div className="card">
          <div className="flex items-center gap-2 mb-2">
            <div className={`p-1.5 rounded-lg bg-gray-50 ${overdue.length === 0 ? "text-green-600" : "text-red-600"}`}>
              <AlertTriangle size={18} />
            </div>
            <span className="text-xs text-gray-500">Overdue</span>
          </div>
          <p className={`text-3xl font-bold ${overdue.length === 0 ? "text-green-600" : "text-red-600"}`}>
            {overdue.length}
          </p>
          <p className="text-xs text-gray-400 mt-1">Past CMS deadline</p>
        </div>

        <div className="card">
          <div className="flex items-center gap-2 mb-2">
            <div className="p-1.5 rounded-lg bg-gray-50 text-purple-600">
              <Timer size={18} />
            </div>
            <span className="text-xs text-gray-500">Auto-Adjudication</span>
          </div>
          <p className="text-3xl font-bold text-purple-600">
            {metrics.auto_adjudication_rate !== null ? `${metrics.auto_adjudication_rate}%` : "N/A"}
          </p>
          <p className="text-xs text-gray-400 mt-1">{metrics.total_auto} of {metrics.total_determined} determined</p>
        </div>
      </div>

      {/* Charts row */}
      <div className="grid grid-cols-2 gap-4">
        {/* Turnaround Distribution */}
        <div className="card">
          <h3 className="font-semibold text-databricks-dark mb-3">Turnaround Distribution</h3>
          <svg viewBox={`0 0 ${chartW} ${chartH + 40}`} className="w-full">
            {/* Y-axis labels */}
            {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
              const y = chartH - frac * chartH + 10;
              const val = Math.round(frac * maxCount);
              return (
                <g key={frac}>
                  <line x1="50" y1={y} x2={chartW} y2={y} stroke="#e5e7eb" strokeWidth="1" />
                  <text x="45" y={y + 4} textAnchor="end" fill="#9ca3af" fontSize="10">{val}</text>
                </g>
              );
            })}
            {/* 72h deadline line - between 48-72h and 72-96h buckets */}
            {metrics.turnaround_distribution.length > 0 && (() => {
              const deadlineIdx = metrics.turnaround_distribution.findIndex((b) => !b.compliant);
              if (deadlineIdx > 0) {
                const x = 60 + (deadlineIdx) * (barW + barPadding) - barPadding / 2;
                return (
                  <g>
                    <line x1={x} y1="5" x2={x} y2={chartH + 10} stroke="#ef4444" strokeWidth="2" strokeDasharray="4,3" />
                    <text x={x + 4} y="14" fill="#ef4444" fontSize="10" fontWeight="600">72h CMS deadline</text>
                  </g>
                );
              }
              return null;
            })()}
            {/* Bars */}
            {metrics.turnaround_distribution.map((bucket, i) => {
              const barH = (bucket.count / maxCount) * chartH;
              const x = 60 + i * (barW + barPadding);
              const y = chartH - barH + 10;
              return (
                <g key={bucket.bucket}>
                  <rect
                    x={x}
                    y={y}
                    width={barW}
                    height={barH}
                    rx="3"
                    fill={bucket.compliant ? "#22c55e" : "#ef4444"}
                    opacity="0.85"
                  />
                  <text
                    x={x + barW / 2}
                    y={y - 4}
                    textAnchor="middle"
                    fill="#374151"
                    fontSize="10"
                    fontWeight="600"
                  >
                    {bucket.count}
                  </text>
                  <text
                    x={x + barW / 2}
                    y={chartH + 26}
                    textAnchor="middle"
                    fill="#6b7280"
                    fontSize="10"
                  >
                    {bucket.bucket}
                  </text>
                </g>
              );
            })}
          </svg>
          <div className="flex items-center justify-center gap-4 mt-1 text-xs text-gray-500">
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-sm bg-green-500 inline-block" /> Compliant (&lt;72h)
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded-sm bg-red-500 inline-block" /> Non-compliant (&ge;72h)
            </span>
          </div>
        </div>

        {/* Compliance Trend */}
        <div className="card">
          <h3 className="font-semibold text-databricks-dark mb-3">Compliance Trend (Weekly)</h3>
          {trendData.length === 0 ? (
            <p className="text-sm text-gray-400">No trend data available.</p>
          ) : (
            <svg viewBox={`0 0 ${trendW} ${trendH + 40}`} className="w-full">
              {/* Y-axis grid */}
              {[0, 25, 50, 75, 100].map((pct) => {
                const y = trendH - (pct / 100) * trendH + 10;
                return (
                  <g key={pct}>
                    <line x1="40" y1={y} x2={trendW} y2={y} stroke="#e5e7eb" strokeWidth="1" />
                    <text x="35" y={y + 4} textAnchor="end" fill="#9ca3af" fontSize="10">{pct}%</text>
                  </g>
                );
              })}
              {/* 95% target line */}
              {(() => {
                const y = trendH - (95 / 100) * trendH + 10;
                return (
                  <g>
                    <line x1="40" y1={y} x2={trendW} y2={y} stroke="#f59e0b" strokeWidth="1.5" strokeDasharray="4,3" />
                    <text x={trendW - 2} y={y - 4} textAnchor="end" fill="#f59e0b" fontSize="10" fontWeight="600">95% target</text>
                  </g>
                );
              })()}
              {/* Line + dots */}
              {trendData.length > 1 && (
                <polyline
                  fill="none"
                  stroke="#1b3a57"
                  strokeWidth="2.5"
                  strokeLinejoin="round"
                  points={trendData.map((d, i) => {
                    const x = 50 + (i / (trendData.length - 1)) * (trendW - 70);
                    const y = trendH - (d.compliance_rate / 100) * trendH + 10;
                    return `${x},${y}`;
                  }).join(" ")}
                />
              )}
              {trendData.map((d, i) => {
                const x = trendData.length === 1
                  ? trendW / 2
                  : 50 + (i / (trendData.length - 1)) * (trendW - 70);
                const y = trendH - (d.compliance_rate / 100) * trendH + 10;
                const good = d.compliance_rate >= 95;
                return (
                  <g key={d.week}>
                    <circle cx={x} cy={y} r="5" fill={good ? "#22c55e" : "#ef4444"} stroke="white" strokeWidth="2" />
                    <text x={x} y={y - 10} textAnchor="middle" fill="#374151" fontSize="9" fontWeight="600">
                      {d.compliance_rate}%
                    </text>
                    {/* Bar for volume */}
                    <rect
                      x={x - 8}
                      y={trendH + 12}
                      width="16"
                      height={Math.max((d.total / trendMaxTotal) * 18, 2)}
                      fill="#cbd5e1"
                      rx="2"
                    />
                    <text
                      x={x}
                      y={trendH + 38}
                      textAnchor="middle"
                      fill="#9ca3af"
                      fontSize="8"
                      transform={`rotate(-30, ${x}, ${trendH + 38})`}
                    >
                      {d.week.slice(5)}
                    </text>
                  </g>
                );
              })}
            </svg>
          )}
        </div>
      </div>

      {/* Overdue Requests Table */}
      <div className="card">
        <h3 className="font-semibold text-databricks-dark mb-3 flex items-center gap-2">
          <AlertTriangle size={16} className="text-red-500" />
          Overdue Requests ({overdue.length})
        </h3>
        {overdue.length === 0 ? (
          <div className="flex items-center gap-2 text-sm text-green-600 py-4">
            <CheckCircle size={16} />
            All open requests are within CMS deadlines.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left">
                  <th className="pb-2 text-gray-500 font-medium">Request ID</th>
                  <th className="pb-2 text-gray-500 font-medium">Member</th>
                  <th className="pb-2 text-gray-500 font-medium">Service Type</th>
                  <th className="pb-2 text-gray-500 font-medium">Urgency</th>
                  <th className="pb-2 text-gray-500 font-medium">Reviewer</th>
                  <th className="pb-2 text-gray-500 font-medium">CMS Deadline</th>
                  <th className="pb-2 text-gray-500 font-medium">Hours Overdue</th>
                </tr>
              </thead>
              <tbody>
                {overdue.map((req) => (
                  <tr
                    key={req.auth_request_id}
                    onClick={() => onSelectRequest(req.auth_request_id)}
                    className="border-b border-gray-100 hover:bg-red-50 cursor-pointer transition-colors"
                  >
                    <td className="py-2 font-mono text-xs text-databricks-red">{req.auth_request_id}</td>
                    <td className="py-2">{req.member_name || "N/A"}</td>
                    <td className="py-2">{req.service_type}</td>
                    <td className="py-2">
                      <span className={`inline-flex items-center gap-1 ${req.urgency === "expedited" ? "text-red-600 font-medium" : ""}`}>
                        {req.urgency === "expedited" && <Zap size={12} />}
                        {req.urgency}
                      </span>
                    </td>
                    <td className="py-2">{req.reviewer_name || <span className="text-gray-400">Unassigned</span>}</td>
                    <td className="py-2 text-gray-600">
                      {req.cms_deadline ? new Date(req.cms_deadline).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }) : "N/A"}
                    </td>
                    <td className="py-2">
                      <span className="flex items-center gap-1 text-red-600 font-semibold">
                        <XCircle size={14} />
                        {Math.round(req.hours_overdue)}h
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
