import { useEffect, useState } from "react";
import {
  Target,
  AlertCircle,
  Mail,
  ChevronRight,
  Loader2,
  Copy,
  Check,
  X,
} from "lucide-react";
import {
  api,
  type NetworkGap,
  type RecruitmentTarget,
  type RecruitmentRecord,
} from "@/lib/api";
import {
  formatCurrency,
  formatNumber,
  formatPercent,
  gapStatusColor,
} from "@/lib/utils";

// ---------------------------------------------------------------------------
// Recruitment status pipeline
// ---------------------------------------------------------------------------

const STATUSES = [
  "Identified",
  "Contacted",
  "Interested",
  "Contracted",
  "Active",
] as const;

type RecruitmentStatus = (typeof STATUSES)[number];

const STATUS_COLORS: Record<string, { bg: string; text: string; ring: string }> = {
  Identified: { bg: "bg-gray-100", text: "text-gray-700", ring: "ring-gray-300" },
  Contacted: { bg: "bg-blue-100", text: "text-blue-700", ring: "ring-blue-300" },
  Interested: { bg: "bg-amber-100", text: "text-amber-700", ring: "ring-amber-300" },
  Contracted: { bg: "bg-purple-100", text: "text-purple-700", ring: "ring-purple-300" },
  Active: { bg: "bg-green-100", text: "text-green-700", ring: "ring-green-300" },
};

function StatusBadge({ status }: { status: string }) {
  const colors = STATUS_COLORS[status] || STATUS_COLORS.Identified;
  return (
    <span
      className={`inline-flex items-center text-xs font-semibold px-2.5 py-0.5 rounded-full ${colors.bg} ${colors.text}`}
    >
      {status}
    </span>
  );
}

function StatusPipeline({
  currentStatus,
  onAdvance,
  loading,
}: {
  currentStatus: string;
  onAdvance: (newStatus: string) => void;
  loading: boolean;
}) {
  const currentIdx = STATUSES.indexOf(currentStatus as RecruitmentStatus);

  return (
    <div className="flex items-center gap-1">
      {STATUSES.map((s, idx) => {
        const isComplete = idx < currentIdx;
        const isCurrent = idx === currentIdx;
        const isNext = idx === currentIdx + 1;
        const colors = STATUS_COLORS[s];

        return (
          <div key={s} className="flex items-center">
            <button
              disabled={!isNext || loading}
              onClick={() => isNext && onAdvance(s)}
              title={isNext ? `Advance to ${s}` : s}
              className={`relative flex items-center justify-center w-7 h-7 rounded-full text-[10px] font-bold transition-all ${
                isComplete
                  ? `${colors.bg} ${colors.text}`
                  : isCurrent
                  ? `${colors.bg} ${colors.text} ring-2 ${colors.ring}`
                  : isNext
                  ? "bg-white border-2 border-dashed border-gray-300 text-gray-400 hover:border-gray-500 hover:text-gray-600 cursor-pointer"
                  : "bg-gray-50 text-gray-300"
              }`}
            >
              {isComplete ? (
                <Check className="w-3.5 h-3.5" />
              ) : loading && isNext ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                idx + 1
              )}
            </button>
            {idx < STATUSES.length - 1 && (
              <ChevronRight
                className={`w-3 h-3 mx-0.5 ${
                  idx < currentIdx ? "text-green-400" : "text-gray-200"
                }`}
              />
            )}
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Outreach letter modal
// ---------------------------------------------------------------------------

function LetterModal({
  letter,
  npi,
  onClose,
}: {
  letter: string;
  npi: string;
  onClose: () => void;
}) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(letter);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm">
      <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full mx-4 max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h3 className="text-lg font-semibold text-databricks-dark">
              Outreach Letter
            </h3>
            <p className="text-xs text-gray-500">NPI: {npi}</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handleCopy}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
            >
              {copied ? (
                <Check className="w-3.5 h-3.5 text-green-600" />
              ) : (
                <Copy className="w-3.5 h-3.5" />
              )}
              {copied ? "Copied" : "Copy"}
            </button>
            <button
              onClick={onClose}
              className="p-1.5 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-gray-100"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-y-auto px-6 py-4">
          <pre className="whitespace-pre-wrap text-sm text-gray-700 font-sans leading-relaxed">
            {letter}
          </pre>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export function GapsRecruitmentPage() {
  const [gaps, setGaps] = useState<NetworkGap[]>([]);
  const [targets, setTargets] = useState<RecruitmentTarget[]>([]);
  const [statuses, setStatuses] = useState<Record<string, RecruitmentRecord>>(
    {}
  );
  const [loading, setLoading] = useState(true);
  const [maxPriority, setMaxPriority] = useState(3);
  const [letterModal, setLetterModal] = useState<{
    npi: string;
    letter: string;
  } | null>(null);
  const [generatingLetter, setGeneratingLetter] = useState<string | null>(null);
  const [advancingStatus, setAdvancingStatus] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.getNetworkGaps(maxPriority),
      api.getRecruitmentTargets(30),
      api.getRecruitmentStatuses(),
    ])
      .then(([g, t, s]) => {
        setGaps(g);
        setTargets(t);
        const statusMap: Record<string, RecruitmentRecord> = {};
        s.forEach((r) => (statusMap[r.npi] = r));
        setStatuses(statusMap);
      })
      .finally(() => setLoading(false));
  }, [maxPriority]);

  const handleGenerateLetter = async (target: RecruitmentTarget) => {
    setGeneratingLetter(target.rendering_provider_npi);
    try {
      const resp = await api.generateOutreachLetter({
        npi: target.rendering_provider_npi,
        specialty: target.specialty || undefined,
        county_name: target.county_name || undefined,
        potential_savings: target.potential_savings,
        members_served: target.members_served,
      });
      setLetterModal({
        npi: target.rendering_provider_npi,
        letter: resp.letter,
      });

      // Auto-set status to Identified if not tracked yet
      if (!statuses[target.rendering_provider_npi]) {
        const record = await api.updateRecruitmentStatus(
          target.rendering_provider_npi,
          "Identified",
          "Outreach letter generated"
        );
        setStatuses((prev) => ({
          ...prev,
          [target.rendering_provider_npi]: record,
        }));
      }
    } catch {
      // Error handled gracefully
    } finally {
      setGeneratingLetter(null);
    }
  };

  const handleAdvanceStatus = async (npi: string, newStatus: string) => {
    setAdvancingStatus(npi);
    try {
      const record = await api.updateRecruitmentStatus(npi, newStatus);
      setStatuses((prev) => ({ ...prev, [npi]: record }));
    } catch {
      // Error handled gracefully
    } finally {
      setAdvancingStatus(null);
    }
  };

  const getStatus = (npi: string): string => {
    return statuses[npi]?.status || "Identified";
  };

  // Summary stats for recruitment pipeline
  const pipelineCounts = STATUSES.reduce(
    (acc, s) => {
      acc[s] = Object.values(statuses).filter((r) => r.status === s).length;
      return acc;
    },
    {} as Record<string, number>
  );

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Target className="w-6 h-6 text-databricks-red" />
          Network Gaps & Recruitment
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Prioritized network gaps and OON provider recruitment targets with
          workflow tracking
        </p>
      </div>

      {/* Recruitment Pipeline Summary */}
      {Object.values(statuses).length > 0 && (
        <div className="card p-4">
          <h3 className="text-sm font-semibold text-databricks-dark mb-3">
            Recruitment Pipeline
          </h3>
          <div className="flex items-center gap-2">
            {STATUSES.map((s, idx) => {
              const colors = STATUS_COLORS[s];
              return (
                <div key={s} className="flex items-center">
                  <div
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg ${colors.bg}`}
                  >
                    <span className={`text-2xl font-bold ${colors.text}`}>
                      {pipelineCounts[s] || 0}
                    </span>
                    <span
                      className={`text-xs font-medium ${colors.text} opacity-80`}
                    >
                      {s}
                    </span>
                  </div>
                  {idx < STATUSES.length - 1 && (
                    <ChevronRight className="w-4 h-4 text-gray-300 mx-1" />
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Network Gaps */}
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-red-500" />
            <h3 className="font-semibold text-databricks-dark">
              Network Gaps
            </h3>
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
          <div className="p-8 text-center text-gray-400">
            No network gaps at this priority level.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    County
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Type
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Specialty
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Status
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    % Compliant
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Gap Members
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    CMS Limit (mi)
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Avg Dist (mi)
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    P
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {gaps.map((g, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    <td className="py-2.5 px-4 text-gray-800 font-medium">
                      {g.county_name}
                    </td>
                    <td className="py-2.5 px-4 text-gray-500 text-xs">
                      {g.county_type}
                    </td>
                    <td className="py-2.5 px-4 text-gray-700">
                      {g.cms_specialty_type}
                    </td>
                    <td className="py-2.5 px-4 text-center">
                      <span
                        className={`inline-block text-xs font-semibold px-2.5 py-0.5 rounded-full ${gapStatusColor(
                          g.gap_status
                        )}`}
                      >
                        {g.gap_status}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right">
                      <span
                        className={
                          g.pct_compliant >= 90
                            ? "text-green-700"
                            : "text-red-700 font-semibold"
                        }
                      >
                        {formatPercent(g.pct_compliant)}
                      </span>
                    </td>
                    <td className="py-2.5 px-4 text-right text-gray-700">
                      {formatNumber(g.gap_members)}
                    </td>
                    <td className="py-2.5 px-4 text-right text-gray-500">
                      {g.cms_threshold_miles ?? "--"}
                    </td>
                    <td className="py-2.5 px-4 text-right text-gray-500">
                      {g.avg_nearest_distance_mi != null
                        ? g.avg_nearest_distance_mi.toFixed(1)
                        : "--"}
                    </td>
                    <td className="py-2.5 px-4 text-center">
                      <span
                        className={`inline-flex items-center justify-center w-6 h-6 text-xs font-bold rounded-full ${
                          g.priority_rank === 1
                            ? "bg-red-100 text-red-700"
                            : g.priority_rank === 2
                            ? "bg-orange-100 text-orange-700"
                            : g.priority_rank === 3
                            ? "bg-yellow-100 text-yellow-700"
                            : "bg-gray-100 text-gray-600"
                        }`}
                      >
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

      {/* Recruitment Targets with Workflow */}
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center gap-2">
            <Target className="w-5 h-5 text-green-600" />
            <h3 className="font-semibold text-databricks-dark">
              OON Provider Recruitment Targets
            </h3>
          </div>
          <p className="text-xs text-gray-400 mt-1">
            Ranked by recruitment priority score. Track status and generate
            outreach letters using AI.
          </p>
        </div>
        {loading ? (
          <div className="p-8 text-center text-gray-400">
            Loading targets...
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-center py-3 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    #
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    NPI
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Specialty
                  </th>
                  <th className="text-left py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    County
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Savings
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Members
                  </th>
                  <th className="text-right py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Score
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Status
                  </th>
                  <th className="text-center py-3 px-4 font-medium text-gray-500 text-xs uppercase tracking-wide">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {targets.map((t, i) => {
                  const currentStatus = getStatus(t.rendering_provider_npi);
                  return (
                    <tr
                      key={t.rendering_provider_npi}
                      className="hover:bg-gray-50"
                    >
                      <td className="py-2.5 px-3 text-center text-gray-400 text-xs">
                        {i + 1}
                      </td>
                      <td className="py-2.5 px-4 font-mono text-xs text-gray-700">
                        {t.rendering_provider_npi}
                      </td>
                      <td className="py-2.5 px-4 text-gray-700">
                        {t.specialty ?? "--"}
                      </td>
                      <td className="py-2.5 px-4 text-gray-700">
                        {t.county_name ?? "--"}
                      </td>
                      <td className="py-2.5 px-4 text-right text-green-700 font-semibold">
                        {formatCurrency(t.potential_savings)}
                      </td>
                      <td className="py-2.5 px-4 text-right text-gray-700">
                        {formatNumber(t.members_served)}
                      </td>
                      <td className="py-2.5 px-4 text-right">
                        <span className="inline-block bg-databricks-red text-white text-xs font-bold px-2 py-0.5 rounded-full">
                          {formatNumber(t.recruitment_priority_score)}
                        </span>
                      </td>
                      <td className="py-2 px-3">
                        <StatusPipeline
                          currentStatus={currentStatus}
                          onAdvance={(s) =>
                            handleAdvanceStatus(
                              t.rendering_provider_npi,
                              s
                            )
                          }
                          loading={
                            advancingStatus === t.rendering_provider_npi
                          }
                        />
                      </td>
                      <td className="py-2.5 px-4 text-center">
                        <button
                          onClick={() => handleGenerateLetter(t)}
                          disabled={
                            generatingLetter === t.rendering_provider_npi
                          }
                          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-databricks-red bg-red-50 border border-red-200 rounded-lg hover:bg-red-100 transition-colors disabled:opacity-50"
                          title="Generate AI outreach letter"
                        >
                          {generatingLetter === t.rendering_provider_npi ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          ) : (
                            <Mail className="w-3.5 h-3.5" />
                          )}
                          Letter
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Letter Modal */}
      {letterModal && (
        <LetterModal
          npi={letterModal.npi}
          letter={letterModal.letter}
          onClose={() => setLetterModal(null)}
        />
      )}
    </div>
  );
}
