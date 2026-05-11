import { useState, useEffect, useCallback } from "react";
import {
  ArrowLeft,
  Loader2,
  DollarSign,
  ShieldAlert,
  TrendingUp,
  Activity,
  Heart,
  MessageSquareText,
  Users,
  Building2,
  Download,
  BarChart3,
  Swords,
  Star,
} from "lucide-react";
import {
  api,
  type GroupReportCard as ReportCardData,
  type GroupTcocItem,
  type RenewalScenarioResult,
  type CompetitiveBenchmarkResponse,
} from "@/lib/api";
import { MetricCard } from "@/components/MetricCard";
import { PercentileGauge } from "@/components/PercentileGauge";
import { ChatPanel } from "@/components/ChatPanel";

function fmt(val: string | null, prefix = "", suffix = "", decimals = 0): string {
  if (!val) return "N/A";
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  return `${prefix}${num.toLocaleString(undefined, { maximumFractionDigits: decimals, minimumFractionDigits: decimals })}${suffix}`;
}

function pct(val: string | null): string {
  if (!val) return "N/A";
  return `${(parseFloat(val) * 100).toFixed(1)}%`;
}

function HealthScore({ score }: { score: number }) {
  const color =
    score >= 70 ? "#16a34a" : score >= 40 ? "#ca8a04" : "#dc2626";
  const label = score >= 70 ? "Healthy" : score >= 40 ? "At Risk" : "Critical";

  return (
    <div className="flex flex-col items-center">
      <div
        className="w-20 h-20 rounded-full flex items-center justify-center border-4"
        style={{ borderColor: color }}
      >
        <span className="text-2xl font-bold" style={{ color }}>
          {score}
        </span>
      </div>
      <span className="text-xs font-medium mt-1" style={{ color }}>
        {label}
      </span>
    </div>
  );
}

function RenewalBadge({ action }: { action: string | null }) {
  const colors: Record<string, string> = {
    "Rate Increase Required": "bg-red-100 text-red-800 border-red-200",
    "Moderate Increase": "bg-amber-100 text-amber-800 border-amber-200",
    "Trend-Only Increase": "bg-yellow-100 text-yellow-800 border-yellow-200",
    "Favorable - Hold or Decrease": "bg-green-100 text-green-800 border-green-200",
  };
  return (
    <span
      className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-semibold border ${
        colors[action ?? ""] || "bg-gray-100 text-gray-600 border-gray-200"
      }`}
    >
      {action || "N/A"}
    </span>
  );
}

// ===================================================================
// Renewal Scenario Modeling Component
// ===================================================================

function RenewalScenarioPanel({ groupId }: { groupId: string }) {
  const [rateChange, setRateChange] = useState(5);
  const [result, setResult] = useState<RenewalScenarioResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [curveData, setCurveData] = useState<{ pct: number; churn: number }[]>([]);

  const runScenario = useCallback(async (pct: number) => {
    setLoading(true);
    try {
      const r = await api.renewalScenario(groupId, pct);
      setResult(r);
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  }, [groupId]);

  // Load initial scenario + churn curve
  useEffect(() => {
    runScenario(rateChange);
    // Build churn curve from -10 to 25
    const points: Promise<{ pct: number; churn: number }>[] = [];
    for (let p = -10; p <= 25; p += 5) {
      points.push(
        api.renewalScenario(groupId, p).then((r) => ({ pct: p, churn: r.churn_probability }))
      );
    }
    Promise.all(points).then(setCurveData).catch(console.error);
  }, [groupId]);

  const handleChange = (val: number) => {
    setRateChange(val);
    runScenario(val);
  };

  const churnColor = (prob: number) =>
    prob < 0.1 ? "text-green-600" : prob < 0.25 ? "text-amber-600" : "text-red-600";
  const churnBg = (prob: number) =>
    prob < 0.1 ? "bg-green-50 border-green-200" : prob < 0.25 ? "bg-amber-50 border-amber-200" : "bg-red-50 border-red-200";

  const maxChurn = Math.max(...curveData.map((d) => d.churn), 0.5);

  return (
    <div className="card p-5">
      <h3 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
        <BarChart3 className="w-4 h-4 text-databricks-red" /> Renewal Scenario Modeling
      </h3>

      {/* Slider */}
      <div className="mb-4">
        <div className="flex items-center justify-between mb-1">
          <span className="text-xs text-gray-500">Rate Change</span>
          <span className="text-sm font-bold text-databricks-dark">
            {rateChange > 0 ? "+" : ""}{rateChange}%
          </span>
        </div>
        <input
          type="range"
          min={-10}
          max={25}
          step={0.5}
          value={rateChange}
          onChange={(e) => handleChange(parseFloat(e.target.value))}
          className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-databricks-red"
        />
        <div className="flex justify-between text-[9px] text-gray-400 mt-1">
          <span>-10%</span>
          <span>0%</span>
          <span>+10%</span>
          <span>+25%</span>
        </div>
      </div>

      {/* Results */}
      {loading && !result ? (
        <div className="flex justify-center py-4">
          <Loader2 className="w-5 h-5 text-databricks-red animate-spin" />
        </div>
      ) : result ? (
        <div className="space-y-3">
          <div className="grid grid-cols-3 gap-3">
            <div className="text-center">
              <span className="text-xs text-gray-400 block">Projected PMPM</span>
              <span className="text-lg font-bold text-databricks-dark">
                ${result.projected_pmpm.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              </span>
            </div>
            <div className="text-center">
              <span className="text-xs text-gray-400 block">Projected Loss Ratio</span>
              <span className={`text-lg font-bold ${result.projected_loss_ratio > 1 ? "text-red-600" : result.projected_loss_ratio > 0.85 ? "text-amber-600" : "text-green-600"}`}>
                {(result.projected_loss_ratio * 100).toFixed(1)}%
              </span>
            </div>
            <div className="text-center">
              <span className="text-xs text-gray-400 block">Churn Probability</span>
              <div className={`inline-block px-3 py-1 rounded-full border ${churnBg(result.churn_probability)}`}>
                <span className={`text-lg font-bold ${churnColor(result.churn_probability)}`}>
                  {(result.churn_probability * 100).toFixed(1)}%
                </span>
              </div>
            </div>
          </div>

          {/* Mini churn curve chart */}
          {curveData.length > 0 && (
            <div className="mt-3">
              <span className="text-xs text-gray-400 block mb-2">Churn Risk vs. Rate Change</span>
              <div className="flex items-end gap-1 h-20">
                {curveData.map((d) => {
                  const barH = (d.churn / maxChurn) * 100;
                  const isActive = Math.abs(d.pct - rateChange) < 2.5;
                  return (
                    <div key={d.pct} className="flex-1 flex flex-col items-center justify-end h-full">
                      <div
                        className={`w-full rounded-t transition-all ${
                          isActive ? "bg-databricks-red" : d.churn < 0.1 ? "bg-green-300" : d.churn < 0.25 ? "bg-amber-300" : "bg-red-300"
                        }`}
                        style={{ height: `${barH}%` }}
                      />
                      <span className="text-[8px] text-gray-400 mt-1">
                        {d.pct > 0 ? "+" : ""}{d.pct}%
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          <div className="text-[10px] text-gray-400 mt-2">
            Model inputs: Health Score {result.health_score}, Tenure ~{result.group_tenure_years}yr
          </div>
        </div>
      ) : null}
    </div>
  );
}


// ===================================================================
// Competitive Benchmarking Component
// ===================================================================

function CompetitiveLandscape({ groupId }: { groupId: string }) {
  const [data, setData] = useState<CompetitiveBenchmarkResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.getCompetitiveBenchmark(groupId)
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [groupId]);

  if (loading) {
    return (
      <div className="card p-5">
        <h3 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
          <Swords className="w-4 h-4 text-databricks-red" /> Competitive Landscape
        </h3>
        <div className="flex justify-center py-8">
          <Loader2 className="w-6 h-6 text-databricks-red animate-spin" />
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="card p-5">
        <h3 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
          <Swords className="w-4 h-4 text-databricks-red" /> Competitive Landscape
        </h3>
        <p className="text-sm text-gray-400">No competitive data available.</p>
      </div>
    );
  }

  const allPmpms = [data.red_bricks_pmpm, ...data.competitors.map((c) => c.pmpm)];
  const maxPmpm = Math.max(...allPmpms);

  return (
    <div className="card p-5">
      <h3 className="text-sm font-semibold text-databricks-dark mb-1 flex items-center gap-2">
        <Swords className="w-4 h-4 text-databricks-red" /> Competitive Landscape
      </h3>
      <p className="text-xs text-gray-400 mb-4">
        Synthetic benchmark vs. {data.sic_code} / {data.size_tier} competitors
      </p>

      {/* Comparison table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wider">
              <th className="px-3 py-2">Carrier</th>
              <th className="px-3 py-2 text-right">PMPM</th>
              <th className="px-3 py-2">vs. Red Bricks</th>
              <th className="px-3 py-2">Network</th>
              <th className="px-3 py-2">Satisfaction</th>
              <th className="px-3 py-2">Wellness Programs</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {/* Red Bricks row */}
            <tr className="bg-red-50/40 font-medium">
              <td className="px-3 py-2.5">
                <span className="flex items-center gap-1.5">
                  <span className="w-2 h-2 rounded-full bg-databricks-red" />
                  Red Bricks Insurance
                </span>
              </td>
              <td className="px-3 py-2.5 text-right font-bold">
                ${data.red_bricks_pmpm.toLocaleString(undefined, { minimumFractionDigits: 2 })}
              </td>
              <td className="px-3 py-2.5 text-gray-400 text-xs">--</td>
              <td className="px-3 py-2.5 text-xs">Regional PPO 16,000+ providers</td>
              <td className="px-3 py-2.5">
                <SatisfactionStars rating={4.1} />
              </td>
              <td className="px-3 py-2.5 text-xs text-gray-600">
                Wellness Portal, Chronic Care, Telehealth
              </td>
            </tr>

            {/* Competitor rows */}
            {data.competitors.map((c) => {
              const diff = ((c.pmpm - data.red_bricks_pmpm) / data.red_bricks_pmpm) * 100;
              const isLower = c.pmpm < data.red_bricks_pmpm;
              return (
                <tr key={c.carrier_name} className="hover:bg-gray-50">
                  <td className="px-3 py-2.5">
                    <span className="flex items-center gap-1.5">
                      <span className="w-2 h-2 rounded-full bg-gray-400" />
                      {c.carrier_name}
                    </span>
                  </td>
                  <td className="px-3 py-2.5 text-right font-semibold">
                    ${c.pmpm.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </td>
                  <td className="px-3 py-2.5">
                    <span className={`text-xs font-semibold ${isLower ? "text-red-600" : "text-green-600"}`}>
                      {isLower ? "" : "+"}{diff.toFixed(1)}%
                      <span className="text-[9px] ml-0.5 font-normal text-gray-400">
                        {isLower ? "(cheaper)" : "(more expensive)"}
                      </span>
                    </span>
                  </td>
                  <td className="px-3 py-2.5 text-xs text-gray-600">{c.network_size}</td>
                  <td className="px-3 py-2.5">
                    <SatisfactionStars rating={c.member_satisfaction} />
                  </td>
                  <td className="px-3 py-2.5 text-xs text-gray-600">
                    {c.wellness_programs.join(", ")}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* PMPM comparison bar */}
      <div className="mt-4">
        <span className="text-xs text-gray-400 block mb-2">PMPM Comparison</span>
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="text-xs w-32 truncate font-medium">Red Bricks</span>
            <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
              <div
                className="h-full bg-databricks-red rounded-full"
                style={{ width: `${(data.red_bricks_pmpm / maxPmpm) * 100}%` }}
              />
            </div>
            <span className="text-xs font-semibold w-16 text-right">
              ${data.red_bricks_pmpm.toFixed(0)}
            </span>
          </div>
          {data.competitors.map((c) => (
            <div key={c.carrier_name} className="flex items-center gap-2">
              <span className="text-xs w-32 truncate">{c.carrier_name}</span>
              <div className="flex-1 h-4 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${c.pmpm <= data.red_bricks_pmpm ? "bg-red-400" : "bg-green-400"}`}
                  style={{ width: `${(c.pmpm / maxPmpm) * 100}%` }}
                />
              </div>
              <span className="text-xs font-semibold w-16 text-right">
                ${c.pmpm.toFixed(0)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SatisfactionStars({ rating }: { rating: number }) {
  const full = Math.floor(rating);
  const partial = rating - full;
  return (
    <div className="flex items-center gap-0.5">
      {Array.from({ length: 5 }, (_, i) => (
        <Star
          key={i}
          className={`w-3 h-3 ${
            i < full
              ? "text-amber-400 fill-amber-400"
              : i === full && partial >= 0.5
              ? "text-amber-400 fill-amber-200"
              : "text-gray-300"
          }`}
        />
      ))}
      <span className="text-[10px] text-gray-500 ml-1">{rating.toFixed(1)}</span>
    </div>
  );
}


const TIER_COLORS: Record<string, string> = {
  "Extreme Outlier": "bg-red-500",
  "High Cost": "bg-orange-500",
  "Rising Risk": "bg-amber-400",
  "Expected": "bg-blue-400",
  "Low Utilizer": "bg-green-400",
};

interface Props {
  groupId: string;
  onBack: () => void;
  onOpenCoach: () => void;
  onOpenReports: () => void;
}

export function GroupReportCard({ groupId, onBack, onOpenCoach, onOpenReports }: Props) {
  const [card, setCard] = useState<ReportCardData | null>(null);
  const [tcoc, setTcoc] = useState<GroupTcocItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([api.getReportCard(groupId), api.getTcoc(groupId)])
      .then(([c, t]) => {
        setCard(c);
        setTcoc(t);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [groupId]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-4rem)]">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );
  }

  if (!card) {
    return (
      <div className="text-center text-gray-400 mt-20">
        <p>Group not found.</p>
        <button onClick={onBack} className="btn-secondary mt-4">
          Back to search
        </button>
      </div>
    );
  }

  const healthScore = parseInt(card.group_health_score || "0");
  const totalTcocMembers = tcoc.reduce(
    (sum, t) => sum + parseInt(t.member_count || "0"),
    0
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <button
            onClick={onBack}
            className="text-sm text-gray-500 hover:text-databricks-red flex items-center gap-1 mb-2"
          >
            <ArrowLeft className="w-4 h-4" /> Back to search
          </button>
          <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
            <Building2 className="w-6 h-6 text-databricks-red" />
            {card.group_name}
          </h2>
          <div className="flex items-center gap-3 mt-1 text-sm text-gray-500">
            <span>{card.group_id}</span>
            <span>{card.industry}</span>
            <span>{card.funding_type}</span>
            <span>{card.group_size_tier}</span>
            <span>{card.state}</span>
          </div>
        </div>
        <div className="flex items-center gap-4">
          <HealthScore score={healthScore} />
          <RenewalBadge action={card.renewal_action} />
          <a
            href={api.getReportCardPdfUrl(groupId)}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-secondary flex items-center gap-2"
          >
            <Download className="w-4 h-4" /> Download Report Card
          </a>
          <button onClick={onOpenReports} className="btn-secondary flex items-center gap-2">
            <Activity className="w-4 h-4" /> Standard Reports
          </button>
          <button onClick={onOpenCoach} className="btn-primary flex items-center gap-2">
            <MessageSquareText className="w-4 h-4" /> Sales Coach
          </button>
        </div>
      </div>

      {/* Top metrics row */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        <div className="card p-4">
          <MetricCard label="Total Members" value={fmt(card.total_members)} />
        </div>
        <div className="card p-4">
          <MetricCard
            label="Claims PMPM"
            value={fmt(card.claims_pmpm, "$", "", 0)}
          />
        </div>
        <div className="card p-4">
          <MetricCard
            label="Loss Ratio"
            value={pct(card.loss_ratio)}
            color={
              parseFloat(card.loss_ratio || "0") > 1
                ? "red"
                : parseFloat(card.loss_ratio || "0") > 0.85
                ? "amber"
                : "green"
            }
          />
        </div>
        <div className="card p-4">
          <MetricCard
            label="Projected Renewal PMPM"
            value={fmt(card.projected_renewal_pmpm, "$", "", 2)}
          />
        </div>
        <div className="card p-4">
          <MetricCard
            label="Avg TCI"
            value={fmt(card.avg_tci, "", "", 3)}
            color={
              parseFloat(card.avg_tci || "0") > 1.5
                ? "red"
                : parseFloat(card.avg_tci || "0") > 1.0
                ? "amber"
                : "green"
            }
          />
        </div>
      </div>

      {/* Main content: 3-col layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Financial Overview */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
            <DollarSign className="w-4 h-4 text-databricks-red" /> Financial Overview
          </h3>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <MetricCard
              label="Premium Revenue"
              value={fmt(card.total_premium_revenue, "$", "", 0)}
            />
            <MetricCard
              label="Total Claims Paid"
              value={fmt(card.total_claims_paid, "$", "", 0)}
            />
            <MetricCard
              label="Medical PMPM"
              value={fmt(card.medical_pmpm, "$", "", 0)}
            />
            <MetricCard
              label="Pharmacy PMPM"
              value={fmt(card.pharmacy_pmpm, "$", "", 0)}
            />
            <MetricCard
              label="Medical Claims"
              value={fmt(card.medical_claims_paid, "$", "", 0)}
            />
            <MetricCard
              label="Pharmacy Claims"
              value={fmt(card.pharmacy_claims_paid, "$", "", 0)}
            />
          </div>
        </div>

        {/* Utilization & Stop-Loss */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
            <Activity className="w-4 h-4 text-databricks-red" /> Utilization &
            Stop-Loss
          </h3>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <MetricCard
              label="IP Admits/1000"
              value={fmt(card.ip_admits_per_1000, "", "", 1)}
            />
            <MetricCard
              label="ER Visits/1000"
              value={fmt(card.er_visits_per_1000, "", "", 1)}
            />
            <MetricCard
              label="High-Cost Claimants"
              value={fmt(card.high_cost_claimants)}
              color={parseInt(card.high_cost_claimants || "0") > 5 ? "red" : "default"}
            />
            <MetricCard
              label="Specific SL Excess"
              value={fmt(card.specific_sl_excess, "$", "", 0)}
            />
            <MetricCard
              label="Aggregate Ratio"
              value={fmt(card.aggregate_attachment_ratio, "", "", 4)}
              color={
                parseFloat(card.aggregate_attachment_ratio || "0") > 1
                  ? "red"
                  : "default"
              }
            />
            <MetricCard
              label="High-Cost Members"
              value={fmt(card.high_cost_members)}
            />
          </div>
        </div>

        {/* Renewal Projection */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-databricks-red" /> Renewal
            Projection
          </h3>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <MetricCard
              label="Actual vs Expected"
              value={fmt(card.actual_to_expected, "", "", 4)}
              color={
                parseFloat(card.actual_to_expected || "0") > 1
                  ? "red"
                  : "green"
              }
            />
            <MetricCard
              label="Credibility Factor"
              value={fmt(card.credibility_factor, "", "", 2)}
            />
            <MetricCard
              label="Trend Factor"
              value={fmt(card.trend_factor, "", "", 3)}
            />
            <MetricCard
              label="Pct High Cost"
              value={fmt(card.pct_high_cost, "", "%", 1)}
              color={
                parseFloat(card.pct_high_cost || "0") > 10 ? "red" : "default"
              }
            />
            <MetricCard
              label="Renewal Date"
              value={card.renewal_date || "N/A"}
            />
            <MetricCard
              label="Avg Member TCOC"
              value={fmt(card.avg_member_tcoc, "$", "", 0)}
            />
          </div>
        </div>
      </div>

      {/* Bottom row: Peer Benchmarks + Cost Tier Distribution */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Peer Benchmarks */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
            <ShieldAlert className="w-4 h-4 text-databricks-red" /> Peer
            Benchmarks
          </h3>
          <p className="text-xs text-gray-400 mb-4">
            vs. {card.industry} / {card.group_size_tier} peers (lower
            percentile = better)
          </p>
          <div className="space-y-4">
            <PercentileGauge
              label="Claims PMPM"
              percentile={parseFloat(card.claims_pmpm_pctl || "0.5")}
            />
            <PercentileGauge
              label="Loss Ratio"
              percentile={parseFloat(card.loss_ratio_pctl || "0.5")}
            />
            <PercentileGauge
              label="ER Visits/1000"
              percentile={parseFloat(card.er_visits_pctl || "0.5")}
            />
            <PercentileGauge
              label="Total Cost Index"
              percentile={parseFloat(card.tci_pctl || "0.5")}
            />
          </div>
        </div>

        {/* Cost Tier Distribution */}
        <div className="card p-5">
          <h3 className="text-sm font-semibold text-databricks-dark mb-4 flex items-center gap-2">
            <Heart className="w-4 h-4 text-databricks-red" /> Cost Tier
            Distribution
          </h3>
          {tcoc.length === 0 ? (
            <p className="text-sm text-gray-400">No TCOC data available.</p>
          ) : (
            <div className="space-y-3">
              {tcoc.map((tier) => {
                const count = parseInt(tier.member_count || "0");
                const barWidth =
                  totalTcocMembers > 0
                    ? (count / totalTcocMembers) * 100
                    : 0;
                return (
                  <div key={tier.cost_tier} className="space-y-1">
                    <div className="flex justify-between text-xs">
                      <span className="font-medium text-gray-700">
                        {tier.cost_tier}
                      </span>
                      <span className="text-gray-500">
                        {count} members | Avg TCOC: ${fmt(tier.avg_tcoc)} |
                        TCI: {tier.avg_tci}
                      </span>
                    </div>
                    <div className="h-3 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${
                          TIER_COLORS[tier.cost_tier || ""] || "bg-gray-400"
                        }`}
                        style={{ width: `${barWidth}%` }}
                      />
                    </div>
                  </div>
                );
              })}
              <div className="text-xs text-gray-400 mt-2">
                <Users className="w-3 h-3 inline mr-1" />
                {totalTcocMembers} total members with cost data
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Renewal Scenario Modeling */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <RenewalScenarioPanel groupId={groupId} />
        <CompetitiveLandscape groupId={groupId} />
      </div>
    </div>
  );
}
