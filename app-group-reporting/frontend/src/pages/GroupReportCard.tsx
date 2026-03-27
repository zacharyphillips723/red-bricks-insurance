import { useState, useEffect } from "react";
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
} from "lucide-react";
import { api, type GroupReportCard as ReportCardData, type GroupTcocItem } from "@/lib/api";
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
    </div>
  );
}
