import { useState, useEffect } from "react";
import { ArrowLeft, Loader2, Building2 } from "lucide-react";
import { api, type GroupReportCard } from "@/lib/api";
import { ChatPanel } from "@/components/ChatPanel";
import { MetricCard } from "@/components/MetricCard";

function fmt(val: string | null, prefix = "", suffix = "", decimals = 0): string {
  if (!val) return "N/A";
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  return `${prefix}${num.toLocaleString(undefined, { maximumFractionDigits: decimals })}${suffix}`;
}

function pct(val: string | null): string {
  if (!val) return "N/A";
  return `${(parseFloat(val) * 100).toFixed(1)}%`;
}

interface Props {
  groupId: string;
  onBack: () => void;
}

export function SalesCoach({ groupId, onBack }: Props) {
  const [card, setCard] = useState<GroupReportCard | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api
      .getReportCard(groupId)
      .then(setCard)
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

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      {/* Header */}
      <div className="mb-4">
        <button
          onClick={onBack}
          className="text-sm text-gray-500 hover:text-databricks-red flex items-center gap-1 mb-2"
        >
          <ArrowLeft className="w-4 h-4" /> Back to report card
        </button>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Building2 className="w-6 h-6 text-databricks-red" />
          Sales Coach — {card?.group_name || groupId}
        </h2>
      </div>

      <div className="flex-1 grid grid-cols-1 lg:grid-cols-5 gap-4 min-h-0">
        {/* Left: Quick reference card */}
        <div className="lg:col-span-2 card p-5 overflow-y-auto">
          <h3 className="text-sm font-semibold text-databricks-dark mb-4">
            Quick Reference
          </h3>
          {card && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-3">
                <MetricCard label="Members" value={fmt(card.total_members)} />
                <MetricCard label="Industry" value={card.industry || "N/A"} />
                <MetricCard label="Funding" value={card.funding_type || "N/A"} />
                <MetricCard label="State" value={card.state || "N/A"} />
              </div>

              <hr className="border-gray-200" />

              <div className="grid grid-cols-2 gap-3">
                <MetricCard label="Claims PMPM" value={fmt(card.claims_pmpm, "$")} />
                <MetricCard label="Loss Ratio" value={pct(card.loss_ratio)} />
                <MetricCard
                  label="Projected Renewal"
                  value={fmt(card.projected_renewal_pmpm, "$", "", 2)}
                />
                <MetricCard label="Renewal Action" value={card.renewal_action || "N/A"} />
              </div>

              <hr className="border-gray-200" />

              <div className="grid grid-cols-2 gap-3">
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
                />
                <MetricCard
                  label="SL Excess"
                  value={fmt(card.specific_sl_excess, "$")}
                />
              </div>

              <hr className="border-gray-200" />

              <div className="grid grid-cols-2 gap-3">
                <MetricCard label="Avg TCOC" value={fmt(card.avg_member_tcoc, "$")} />
                <MetricCard label="Avg TCI" value={fmt(card.avg_tci, "", "", 3)} />
                <MetricCard
                  label="% High Cost"
                  value={fmt(card.pct_high_cost, "", "%", 1)}
                />
                <MetricCard
                  label="Health Score"
                  value={fmt(card.group_health_score)}
                />
              </div>

              <hr className="border-gray-200" />

              <div>
                <h4 className="text-xs text-gray-400 mb-2">Peer Percentiles</h4>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div className="flex justify-between">
                    <span className="text-gray-500">PMPM:</span>
                    <span className="font-medium">
                      {Math.round(parseFloat(card.claims_pmpm_pctl || "0.5") * 100)}th
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Loss Ratio:</span>
                    <span className="font-medium">
                      {Math.round(parseFloat(card.loss_ratio_pctl || "0.5") * 100)}th
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">ER Visits:</span>
                    <span className="font-medium">
                      {Math.round(parseFloat(card.er_visits_pctl || "0.5") * 100)}th
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">TCI:</span>
                    <span className="font-medium">
                      {Math.round(parseFloat(card.tci_pctl || "0.5") * 100)}th
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Right: Chat panel */}
        <div className="lg:col-span-3 card min-h-0">
          <ChatPanel groupId={groupId} />
        </div>
      </div>
    </div>
  );
}
