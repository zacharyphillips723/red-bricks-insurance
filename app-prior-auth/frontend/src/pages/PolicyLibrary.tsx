import { useState, useEffect } from "react";
import { api } from "@/lib/api";
import { BookOpen, ChevronDown, ChevronRight } from "lucide-react";

interface Policy {
  policy_id: string;
  policy_name: string;
  service_category: string;
  policy_summary: string;
}

interface PolicyRule {
  rule_id: string;
  rule_type: string;
  rule_text: string;
  procedure_codes: string | null;
  diagnosis_codes: string | null;
}

export function PolicyLibrary() {
  const [policies, setPolicies] = useState<Policy[]>([]);
  const [loading, setLoading] = useState(true);
  const [expandedPolicy, setExpandedPolicy] = useState<string | null>(null);
  const [rules, setRules] = useState<Record<string, PolicyRule[]>>({});
  const [rulesLoading, setRulesLoading] = useState<string | null>(null);

  useEffect(() => {
    api.listPolicies()
      .then((data) => setPolicies(data as unknown as Policy[]))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const togglePolicy = async (policyId: string) => {
    if (expandedPolicy === policyId) {
      setExpandedPolicy(null);
      return;
    }
    setExpandedPolicy(policyId);

    if (!rules[policyId]) {
      setRulesLoading(policyId);
      try {
        const policyRules = await api.getPolicyRules(policyId) as unknown as PolicyRule[];
        setRules((prev) => ({ ...prev, [policyId]: policyRules }));
      } catch {
        setRules((prev) => ({ ...prev, [policyId]: [] }));
      } finally {
        setRulesLoading(null);
      }
    }
  };

  function ruleTypeBadge(ruleType: string): string {
    switch (ruleType) {
      case "coverage_criteria": return "bg-blue-100 text-blue-800";
      case "clinical_requirement": return "bg-green-100 text-green-800";
      case "exclusion": return "bg-red-100 text-red-800";
      case "documentation_requirement": return "bg-amber-100 text-amber-800";
      default: return "bg-gray-100 text-gray-800";
    }
  }

  if (loading) {
    return (
      <div className="space-y-4">
        <h2 className="text-2xl font-bold text-databricks-dark">Policy Library</h2>
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, i) => <div key={i} className="card h-24 animate-pulse bg-gray-100" />)}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <BookOpen size={24} /> Policy Library
        </h2>
        <span className="text-sm text-gray-500">{policies.length} medical policies</span>
      </div>

      <div className="space-y-3">
        {policies.map((p) => {
          const isExpanded = expandedPolicy === p.policy_id;
          const policyRules = rules[p.policy_id] || [];

          return (
            <div key={p.policy_id} className="card">
              <button
                onClick={() => togglePolicy(p.policy_id)}
                className="w-full flex items-center gap-3 text-left"
              >
                {isExpanded ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                <div className="flex-1">
                  <h3 className="font-semibold text-databricks-dark">{p.policy_name}</h3>
                  <p className="text-xs text-gray-500">{p.service_category} — ID: {p.policy_id}</p>
                </div>
              </button>

              {isExpanded && (
                <div className="mt-4 pt-4 border-t">
                  {p.policy_summary && (
                    <div className="mb-4">
                      <h4 className="text-sm font-medium text-gray-600 mb-1">Summary</h4>
                      <p className="text-sm text-gray-700">{p.policy_summary}</p>
                    </div>
                  )}

                  <h4 className="text-sm font-medium text-gray-600 mb-2">Policy Rules</h4>
                  {rulesLoading === p.policy_id ? (
                    <p className="text-sm text-gray-400">Loading rules...</p>
                  ) : policyRules.length === 0 ? (
                    <p className="text-sm text-gray-400">No rules loaded for this policy.</p>
                  ) : (
                    <div className="space-y-2">
                      {policyRules.map((r) => (
                        <div key={r.rule_id} className="bg-gray-50 rounded-md p-3">
                          <div className="flex items-center gap-2 mb-1">
                            <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${ruleTypeBadge(r.rule_type)}`}>
                              {r.rule_type.replace(/_/g, " ")}
                            </span>
                            <span className="text-xs text-gray-400 font-mono">{r.rule_id}</span>
                          </div>
                          <p className="text-sm text-gray-700">{r.rule_text}</p>
                          {r.procedure_codes && (
                            <p className="text-xs text-gray-500 mt-1">CPT: <span className="font-mono">{r.procedure_codes}</span></p>
                          )}
                          {r.diagnosis_codes && (
                            <p className="text-xs text-gray-500">ICD-10: <span className="font-mono">{r.diagnosis_codes}</span></p>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
