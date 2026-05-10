/**
 * Care Plan Viewer/Editor — AI-generated and manually editable care plans.
 *
 * Shows structured care plans with goals, interventions, milestones,
 * and responsible parties. Supports LLM-powered plan generation.
 */

import { useState } from "react";
import {
  ClipboardList,
  Sparkles,
  Loader2,
  CheckCircle2,
  Clock,
  Target,
  ChevronDown,
  ChevronRight,
  Search,
} from "lucide-react";
import { api, type MemberListItem } from "@/lib/api";

interface CarePlanGoal {
  goal: string;
  target_date: string;
  status: string;
  interventions: CarePlanIntervention[];
}

interface CarePlanIntervention {
  action: string;
  responsible: string;
  frequency: string;
  status: string;
  notes: string;
}

interface CarePlan {
  summary: string;
  goals: CarePlanGoal[];
  generated_at: string;
}

export function CarePlan() {
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<MemberListItem[]>([]);
  const [selectedMember, setSelectedMember] = useState<MemberListItem | null>(null);
  const [carePlan, setCarePlan] = useState<CarePlan | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedGoals, setExpandedGoals] = useState<Set<number>>(new Set());

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    try {
      const results = await api.searchMembers(searchQuery);
      setSearchResults(results);
    } catch {
      setSearchResults([]);
    }
  };

  const selectMember = (member: MemberListItem) => {
    setSelectedMember(member);
    setSearchResults([]);
    setSearchQuery("");
    setCarePlan(null);
  };

  const generateCarePlan = async () => {
    if (!selectedMember) return;
    setLoading(true);
    try {
      const result = await api.generateCarePlan(selectedMember.member_id);
      setCarePlan(result);
      // Expand all goals by default
      setExpandedGoals(new Set(result.goals.map((_: CarePlanGoal, i: number) => i)));
    } catch (e) {
      console.error("Failed to generate care plan:", e);
    } finally {
      setLoading(false);
    }
  };

  const toggleGoal = (index: number) => {
    setExpandedGoals((prev) => {
      const next = new Set(prev);
      if (next.has(index)) next.delete(index);
      else next.add(index);
      return next;
    });
  };

  const statusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
      case "met":
        return <CheckCircle2 className="w-4 h-4 text-green-500" />;
      case "in progress":
      case "active":
        return <Clock className="w-4 h-4 text-blue-500" />;
      default:
        return <Target className="w-4 h-4 text-gray-400" />;
    }
  };

  const statusBadgeColor = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
      case "met":
        return "bg-green-100 text-green-800";
      case "in progress":
      case "active":
        return "bg-blue-100 text-blue-800";
      default:
        return "bg-gray-100 text-gray-600";
    }
  };

  return (
    <div>
      <h2 className="text-2xl font-bold text-gray-800 flex items-center gap-2 mb-1">
        <ClipboardList className="w-6 h-6 text-red-600" /> Care Plan Manager
      </h2>
      <p className="text-sm text-gray-500 mb-6">
        AI-generated care plans with goals, interventions, and milestones
      </p>

      {/* Search */}
      <div className="relative mb-6 max-w-xl">
        <input
          type="text"
          placeholder="Search by member name or ID..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleSearch()}
          className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-red-200 focus:border-red-400 outline-none"
        />
        <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
        {searchResults.length > 0 && (
          <div className="absolute top-full left-0 right-0 mt-1 bg-white border rounded-lg shadow-lg z-10 max-h-48 overflow-y-auto">
            {searchResults.map((m) => (
              <button
                key={m.member_id}
                onClick={() => selectMember(m)}
                className="w-full text-left px-4 py-2 hover:bg-gray-50 text-sm border-b last:border-b-0"
              >
                <span className="font-medium">{m.member_name || [m.first_name, m.last_name].filter(Boolean).join(" ") || m.member_id}</span>
                <span className="text-gray-400 ml-2">{m.member_id}</span>
                {m.risk_tier && (
                  <span className={`ml-2 px-2 py-0.5 rounded text-xs ${
                    m.risk_tier === "High" || m.risk_tier === "Critical"
                      ? "bg-red-100 text-red-700"
                      : "bg-gray-100 text-gray-600"
                  }`}>
                    {m.risk_tier}
                  </span>
                )}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Selected member header */}
      {selectedMember && (
        <div className="bg-white rounded-xl border p-4 mb-6 flex items-center justify-between">
          <div>
            <h3 className="font-semibold text-lg">
              {selectedMember.member_name || [selectedMember.first_name, selectedMember.last_name].filter(Boolean).join(" ") || selectedMember.member_id}
            </h3>
            <p className="text-sm text-gray-500">
              {selectedMember.member_id} | {selectedMember.gender} | Age {selectedMember.age} |{" "}
              {selectedMember.line_of_business}
              {selectedMember.risk_tier && ` | ${selectedMember.risk_tier}`}
            </p>
          </div>
          <button
            onClick={generateCarePlan}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm font-medium"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Sparkles className="w-4 h-4" />
            )}
            {loading ? "Generating..." : carePlan ? "Regenerate Plan" : "Generate Care Plan"}
          </button>
        </div>
      )}

      {/* Care Plan Display */}
      {carePlan && (
        <div className="space-y-4">
          {/* Summary */}
          <div className="bg-white rounded-xl border p-5">
            <h4 className="font-semibold text-gray-800 mb-2">Plan Summary</h4>
            <p className="text-sm text-gray-600 leading-relaxed whitespace-pre-line">
              {carePlan.summary}
            </p>
            <p className="text-xs text-gray-400 mt-3">
              Generated {new Date(carePlan.generated_at).toLocaleString()}
            </p>
          </div>

          {/* Goals */}
          {carePlan.goals.map((goal, gi) => (
            <div key={gi} className="bg-white rounded-xl border overflow-hidden">
              <button
                onClick={() => toggleGoal(gi)}
                className="w-full flex items-center gap-3 p-4 hover:bg-gray-50 text-left"
              >
                {expandedGoals.has(gi) ? (
                  <ChevronDown className="w-4 h-4 text-gray-400" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-gray-400" />
                )}
                {statusIcon(goal.status)}
                <span className="flex-1 font-medium text-sm">{goal.goal}</span>
                <span className={`px-2 py-0.5 rounded text-xs ${statusBadgeColor(goal.status)}`}>
                  {goal.status}
                </span>
                {goal.target_date && (
                  <span className="text-xs text-gray-400">Target: {goal.target_date}</span>
                )}
              </button>

              {expandedGoals.has(gi) && goal.interventions.length > 0 && (
                <div className="border-t bg-gray-50">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-xs text-gray-500 uppercase">
                        <th className="px-4 py-2 text-left">Intervention</th>
                        <th className="px-4 py-2 text-left">Responsible</th>
                        <th className="px-4 py-2 text-left">Frequency</th>
                        <th className="px-4 py-2 text-left">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {goal.interventions.map((iv, ii) => (
                        <tr key={ii} className="border-t border-gray-200">
                          <td className="px-4 py-2 text-gray-700">{iv.action}</td>
                          <td className="px-4 py-2 text-gray-600">{iv.responsible}</td>
                          <td className="px-4 py-2 text-gray-600">{iv.frequency}</td>
                          <td className="px-4 py-2">
                            <span className={`px-2 py-0.5 rounded text-xs ${statusBadgeColor(iv.status)}`}>
                              {iv.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Empty state */}
      {!selectedMember && (
        <div className="text-center py-16 text-gray-400">
          <ClipboardList className="w-12 h-12 mx-auto mb-3 opacity-50" />
          <h3 className="font-semibold text-lg text-gray-500">Search for a member</h3>
          <p className="text-sm">Select a member to view or generate their care plan</p>
        </div>
      )}
    </div>
  );
}
