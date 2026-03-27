import { useState, useEffect } from "react";
import {
  Search,
  Building2,
  Loader2,
  TrendingUp,
  TrendingDown,
  Minus,
  ArrowRight,
} from "lucide-react";
import { api, type GroupListItem } from "@/lib/api";

function HealthScoreBadge({ score }: { score: number }) {
  const color =
    score >= 70
      ? "bg-green-100 text-green-800 border-green-200"
      : score >= 40
      ? "bg-amber-100 text-amber-800 border-amber-200"
      : "bg-red-100 text-red-800 border-red-200";

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold border ${color}`}
    >
      {score}
    </span>
  );
}

function RenewalBadge({ action }: { action: string | null }) {
  const cfg: Record<string, { color: string; icon: typeof TrendingUp }> = {
    "Rate Increase Required": {
      color: "bg-red-50 text-red-700 border-red-200",
      icon: TrendingUp,
    },
    "Moderate Increase": {
      color: "bg-amber-50 text-amber-700 border-amber-200",
      icon: TrendingUp,
    },
    "Trend-Only Increase": {
      color: "bg-yellow-50 text-yellow-700 border-yellow-200",
      icon: Minus,
    },
    "Favorable - Hold or Decrease": {
      color: "bg-green-50 text-green-700 border-green-200",
      icon: TrendingDown,
    },
  };
  const c = cfg[action ?? ""] || {
    color: "bg-gray-50 text-gray-600 border-gray-200",
    icon: Minus,
  };
  const Icon = c.icon;
  return (
    <span
      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border ${c.color}`}
    >
      <Icon className="w-3 h-3" /> {action || "N/A"}
    </span>
  );
}

interface Props {
  onSelectGroup: (groupId: string) => void;
}

export function GroupSearch({ onSelectGroup }: Props) {
  const [query, setQuery] = useState("");
  const [groups, setGroups] = useState<GroupListItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoad, setInitialLoad] = useState(true);

  // Filters
  const [industry, setIndustry] = useState("");
  const [fundingType, setFundingType] = useState("");
  const [renewalAction, setRenewalAction] = useState("");

  const loadGroups = async () => {
    setLoading(true);
    try {
      const params: Record<string, string> = {};
      if (query.trim()) params.q = query;
      if (industry) params.industry = industry;
      if (fundingType) params.funding_type = fundingType;
      if (renewalAction) params.renewal_action = renewalAction;
      const data = await api.listGroups(
        Object.keys(params).length > 0 ? params : undefined
      );
      setGroups(data);
    } catch (err) {
      console.error("Failed to load groups:", err);
    } finally {
      setLoading(false);
      setInitialLoad(false);
    }
  };

  useEffect(() => {
    loadGroups();
  }, [industry, fundingType, renewalAction]);

  useEffect(() => {
    const timeout = setTimeout(() => {
      loadGroups();
    }, 300);
    return () => clearTimeout(timeout);
  }, [query]);

  // Extract unique values for filter dropdowns
  const industries = [...new Set(groups.map((g) => g.industry).filter(Boolean))];
  const fundingTypes = [
    ...new Set(groups.map((g) => g.funding_type).filter(Boolean)),
  ];
  const renewalActions = [
    ...new Set(groups.map((g) => g.renewal_action).filter(Boolean)),
  ];

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Building2 className="w-6 h-6 text-databricks-red" /> Employer Groups
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Search and filter employer groups for renewal preparation
        </p>
      </div>

      {/* Search + Filters */}
      <div className="flex flex-wrap gap-3 mb-4">
        <div className="relative flex-1 min-w-[280px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by group name or ID..."
            className="w-full pl-10 pr-4 py-2.5 rounded-xl border border-gray-300 text-sm
                       focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
          />
        </div>
        <select
          value={industry}
          onChange={(e) => setIndustry(e.target.value)}
          className="px-3 py-2.5 rounded-xl border border-gray-300 text-sm bg-white"
        >
          <option value="">All Industries</option>
          {industries.map((i) => (
            <option key={i!} value={i!}>
              {i}
            </option>
          ))}
        </select>
        <select
          value={fundingType}
          onChange={(e) => setFundingType(e.target.value)}
          className="px-3 py-2.5 rounded-xl border border-gray-300 text-sm bg-white"
        >
          <option value="">All Funding Types</option>
          {fundingTypes.map((f) => (
            <option key={f!} value={f!}>
              {f}
            </option>
          ))}
        </select>
        <select
          value={renewalAction}
          onChange={(e) => setRenewalAction(e.target.value)}
          className="px-3 py-2.5 rounded-xl border border-gray-300 text-sm bg-white"
        >
          <option value="">All Renewal Actions</option>
          {renewalActions.map((r) => (
            <option key={r!} value={r!}>
              {r}
            </option>
          ))}
        </select>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto">
        {loading && initialLoad ? (
          <div className="flex items-center justify-center h-40">
            <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
          </div>
        ) : groups.length === 0 ? (
          <div className="flex items-center justify-center h-40 text-gray-400 text-sm">
            No groups found
          </div>
        ) : (
          <div className="space-y-2">
            {groups.map((g) => (
              <button
                key={g.group_id}
                onClick={() => onSelectGroup(g.group_id)}
                className="w-full card p-4 hover:border-databricks-red/50 hover:shadow-md
                           transition-all flex items-center justify-between group text-left"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-semibold text-sm text-databricks-dark truncate">
                      {g.group_name}
                    </span>
                    <span className="text-xs text-gray-400 shrink-0">
                      {g.group_id}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 text-xs text-gray-500">
                    <span>{g.industry}</span>
                    <span>{g.funding_type}</span>
                    <span>{g.group_size_tier}</span>
                    <span>{g.total_members} members</span>
                  </div>
                </div>
                <div className="flex items-center gap-4 shrink-0 ml-4">
                  <div className="text-right">
                    <span className="text-xs text-gray-400 block">PMPM</span>
                    <span className="text-sm font-semibold text-databricks-dark">
                      ${parseFloat(g.claims_pmpm || "0").toFixed(0)}
                    </span>
                  </div>
                  <div className="text-right">
                    <span className="text-xs text-gray-400 block">Loss Ratio</span>
                    <span className="text-sm font-semibold text-databricks-dark">
                      {(parseFloat(g.loss_ratio || "0") * 100).toFixed(1)}%
                    </span>
                  </div>
                  <HealthScoreBadge
                    score={parseInt(g.group_health_score || "0")}
                  />
                  <RenewalBadge action={g.renewal_action} />
                  <ArrowRight className="w-4 h-4 text-gray-300 group-hover:text-databricks-red transition-colors" />
                </div>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Count */}
      <div className="mt-3 text-xs text-gray-400">
        {groups.length} groups{loading && !initialLoad && " (updating...)"}
      </div>
    </div>
  );
}
