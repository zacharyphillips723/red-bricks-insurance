import { useState } from "react";
import {
  Loader2,
  BarChart3,
  AlertTriangle,
  Search,
  ShieldAlert,
  Activity,
  Users,
} from "lucide-react";
import {
  api,
  type RiskPoolResult,
  type BookOfBusinessSummary,
} from "@/lib/api";

function DistributionChart({
  title,
  data,
  colorGroup,
  colorBook,
}: {
  title: string;
  data: { label: string; group_value: number; book_value: number }[];
  colorGroup: string;
  colorBook: string;
}) {
  const maxVal = Math.max(...data.flatMap((d) => [d.group_value, d.book_value]), 1);

  return (
    <div>
      <h3 className="text-sm font-semibold text-databricks-dark mb-3">{title}</h3>
      <div className="space-y-2">
        {data.map((d) => (
          <div key={d.label} className="space-y-1">
            <div className="flex items-center justify-between text-xs text-gray-500">
              <span className="font-medium">{d.label}</span>
              <span>
                <span className={colorGroup}>Group {d.group_value.toFixed(1)}%</span>
                {" / "}
                <span className={colorBook}>Book {d.book_value.toFixed(1)}%</span>
              </span>
            </div>
            <div className="relative h-5 bg-gray-100 rounded overflow-hidden">
              <div
                className="absolute inset-y-0 left-0 bg-databricks-red/20 border-r-2 border-databricks-red rounded"
                style={{ width: `${(d.group_value / maxVal) * 100}%` }}
              />
              <div
                className="absolute inset-y-0 left-0 bg-blue-500/20 border-r-2 border-blue-500 rounded"
                style={{ width: `${(d.book_value / maxVal) * 100}%`, height: "40%", top: "60%" }}
              />
            </div>
          </div>
        ))}
      </div>
      <div className="flex items-center gap-4 mt-2 text-xs text-gray-400">
        <span className="flex items-center gap-1">
          <span className="w-3 h-1.5 bg-databricks-red rounded" /> Group
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-1.5 bg-blue-500 rounded" /> Book of Business
        </span>
      </div>
    </div>
  );
}

function ConditionTable({
  conditions,
}: {
  conditions: { condition: string; group_pct: number; book_pct: number; delta_pct: number }[];
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b bg-gray-50">
            <th className="text-left py-2 px-3 text-xs font-medium text-gray-500 uppercase">Condition</th>
            <th className="text-right py-2 px-3 text-xs font-medium text-gray-500 uppercase">Group</th>
            <th className="text-right py-2 px-3 text-xs font-medium text-gray-500 uppercase">Book</th>
            <th className="text-right py-2 px-3 text-xs font-medium text-gray-500 uppercase">Delta</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {conditions.map((c) => (
            <tr key={c.condition} className="hover:bg-gray-50">
              <td className="py-2 px-3 text-gray-700">{c.condition}</td>
              <td className="py-2 px-3 text-right font-mono">{c.group_pct.toFixed(1)}%</td>
              <td className="py-2 px-3 text-right font-mono text-gray-500">{c.book_pct.toFixed(1)}%</td>
              <td
                className={`py-2 px-3 text-right font-mono font-semibold ${
                  c.delta_pct > 2 ? "text-red-600" : c.delta_pct < -2 ? "text-green-600" : "text-gray-500"
                }`}
              >
                {c.delta_pct > 0 ? "+" : ""}{c.delta_pct.toFixed(1)}%
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function RiskPoolPage() {
  const [groupId, setGroupId] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<RiskPoolResult | null>(null);
  const [bookSummary, setBookSummary] = useState<BookOfBusinessSummary | null>(null);
  const [bookLoading, setBookLoading] = useState(false);

  const handleAnalyze = async () => {
    if (!groupId.trim()) return;
    setLoading(true);
    try {
      const res = await api.getGroupRiskPool(groupId.trim());
      setResult(res);
    } catch (err) {
      console.error("Risk pool error:", err);
    } finally {
      setLoading(false);
    }
  };

  const handleLoadBook = async () => {
    setBookLoading(true);
    try {
      const res = await api.getBookOfBusinessSummary();
      setBookSummary(res);
    } catch (err) {
      console.error("Book summary error:", err);
    } finally {
      setBookLoading(false);
    }
  };

  return (
    <div className="p-6 max-w-6xl mx-auto space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <BarChart3 className="w-6 h-6 text-databricks-red" />
          Risk Pool Visualization
        </h1>
        <p className="text-sm text-gray-500 mt-1">
          Compare a group's risk profile against the book of business — RAF distribution, age mix, chronic condition prevalence, and adverse selection detection.
        </p>
      </div>

      {/* Search */}
      <div className="card p-4">
        <div className="flex gap-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              value={groupId}
              onChange={(e) => setGroupId(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleAnalyze()}
              placeholder="Enter Group ID (e.g. GRP-001)"
              className="w-full pl-10 pr-4 py-2.5 rounded-lg border border-gray-300 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
            />
          </div>
          <button
            onClick={handleAnalyze}
            disabled={loading || !groupId.trim()}
            className="btn-primary flex items-center gap-2 px-6"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <BarChart3 className="w-4 h-4" />}
            Analyze
          </button>
          <button
            onClick={handleLoadBook}
            disabled={bookLoading}
            className="px-4 py-2 rounded-lg border border-gray-300 text-sm font-medium text-gray-600 hover:bg-gray-50 flex items-center gap-2"
          >
            {bookLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Users className="w-4 h-4" />}
            Book Summary
          </button>
        </div>
      </div>

      {/* Book of Business Summary */}
      {bookSummary && (
        <div className="card p-5">
          <h2 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
            <Users className="w-4 h-4 text-databricks-red" />
            Book of Business Summary
          </h2>
          <div className="grid grid-cols-3 gap-4 mb-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-databricks-dark">
                {bookSummary.total_members.toLocaleString()}
              </div>
              <div className="text-xs text-gray-500">Total Members</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-databricks-dark">
                {bookSummary.avg_raf.toFixed(3)}
              </div>
              <div className="text-xs text-gray-500">Avg RAF Score</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-databricks-dark">
                {bookSummary.avg_age.toFixed(1)}
              </div>
              <div className="text-xs text-gray-500">Avg Age</div>
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
            <div>
              <h4 className="font-semibold text-gray-600 mb-1">RAF Distribution</h4>
              {bookSummary.raf_distribution.map((r, i) => (
                <div key={i} className="flex justify-between py-0.5">
                  <span className="text-gray-500">{String(r.bucket || r.label || "")}</span>
                  <span className="font-mono">{Number(r.pct || r.value || 0).toFixed(1)}%</span>
                </div>
              ))}
            </div>
            <div>
              <h4 className="font-semibold text-gray-600 mb-1">Age Distribution</h4>
              {bookSummary.age_distribution.map((a, i) => (
                <div key={i} className="flex justify-between py-0.5">
                  <span className="text-gray-500">{String(a.band || a.label || "")}</span>
                  <span className="font-mono">{Number(a.pct || a.value || 0).toFixed(1)}%</span>
                </div>
              ))}
            </div>
            <div>
              <h4 className="font-semibold text-gray-600 mb-1">Top Chronic Conditions</h4>
              {bookSummary.top_chronic_conditions.slice(0, 8).map((c, i) => (
                <div key={i} className="flex justify-between py-0.5">
                  <span className="text-gray-500 truncate mr-2">{String(c.condition || "")}</span>
                  <span className="font-mono">{Number(c.prevalence_pct || 0).toFixed(1)}%</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      {result ? (
        <div className="space-y-6">
          {/* Adverse Selection Alert */}
          {result.adverse_selection_flag && (
            <div
              className={`card p-4 border-l-4 ${
                result.adverse_selection_severity === "high"
                  ? "border-red-500 bg-red-50"
                  : result.adverse_selection_severity === "moderate"
                  ? "border-amber-500 bg-amber-50"
                  : "border-yellow-500 bg-yellow-50"
              }`}
            >
              <div className="flex items-start gap-2">
                <AlertTriangle
                  className={`w-5 h-5 flex-shrink-0 ${
                    result.adverse_selection_severity === "high"
                      ? "text-red-600"
                      : "text-amber-600"
                  }`}
                />
                <div>
                  <h3 className="text-sm font-semibold text-gray-800">
                    Adverse Selection Detected — {result.adverse_selection_severity?.toUpperCase()} Severity
                  </h3>
                  <p className="text-sm text-gray-600 mt-1">
                    Group avg RAF ({result.group_avg_raf.toFixed(3)}) exceeds book avg (
                    {result.book_avg_raf.toFixed(3)}) by{" "}
                    {((result.group_avg_raf / result.book_avg_raf - 1) * 100).toFixed(1)}%.
                    Consider experience-rated surcharge or enhanced underwriting review.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Summary Stats */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="card p-4 text-center">
              <div className="text-2xl font-bold text-databricks-dark">
                {result.group_member_count.toLocaleString()}
              </div>
              <div className="text-xs text-gray-500">Group Members</div>
            </div>
            <div className="card p-4 text-center">
              <div
                className={`text-2xl font-bold ${
                  result.group_avg_raf > result.book_avg_raf ? "text-red-600" : "text-green-600"
                }`}
              >
                {result.group_avg_raf.toFixed(3)}
              </div>
              <div className="text-xs text-gray-500">
                Group Avg RAF (Book: {result.book_avg_raf.toFixed(3)})
              </div>
            </div>
            <div className="card p-4 text-center">
              <div className="text-2xl font-bold text-databricks-dark">
                {(result.group_avg_raf / result.book_avg_raf).toFixed(2)}x
              </div>
              <div className="text-xs text-gray-500">RAF Ratio (Group / Book)</div>
            </div>
            <div className="card p-4 text-center">
              <div
                className={`text-2xl font-bold ${
                  result.adverse_selection_flag ? "text-red-600" : "text-green-600"
                }`}
              >
                {result.adverse_selection_flag ? "Yes" : "No"}
              </div>
              <div className="text-xs text-gray-500">Adverse Selection</div>
            </div>
          </div>

          {/* Distribution Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="card p-5">
              <DistributionChart
                title="RAF Score Distribution"
                data={result.raf_distribution}
                colorGroup="text-databricks-red font-semibold"
                colorBook="text-blue-600"
              />
            </div>
            <div className="card p-5">
              <DistributionChart
                title="Age Distribution"
                data={result.age_distribution}
                colorGroup="text-databricks-red font-semibold"
                colorBook="text-blue-600"
              />
            </div>
          </div>

          {/* Condition Prevalence & Cost Drivers */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <Activity className="w-4 h-4 text-databricks-red" />
                Chronic Condition Prevalence
              </h3>
              <ConditionTable conditions={result.condition_prevalence} />
            </div>

            <div className="card p-5">
              <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <ShieldAlert className="w-4 h-4 text-databricks-red" />
                Top Cost Drivers (Group PMPM)
              </h3>
              <div className="space-y-3">
                {result.top_cost_drivers.map((d) => (
                  <div key={d.category} className="space-y-1">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-700">{d.category}</span>
                      <span className="font-mono font-semibold text-databricks-dark">
                        ${d.pmpm.toFixed(2)} ({d.pct_of_total.toFixed(1)}%)
                      </span>
                    </div>
                    <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-databricks-red rounded-full"
                        style={{ width: `${d.pct_of_total}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Narrative */}
          <div className="card p-4 bg-blue-50 border-blue-200">
            <p className="text-sm text-blue-800">{result.narrative}</p>
          </div>
        </div>
      ) : (
        !bookSummary && (
          <div className="card p-12 text-center">
            <BarChart3 className="w-12 h-12 text-gray-300 mx-auto mb-3" />
            <h3 className="text-lg font-semibold text-gray-400">
              Enter a Group ID
            </h3>
            <p className="text-sm text-gray-400 mt-1">
              Compare any group's risk profile (RAF, age, conditions) against the
              book of business to detect adverse selection.
            </p>
          </div>
        )
      )}
    </div>
  );
}
