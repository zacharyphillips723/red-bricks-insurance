import { useState, useEffect, useCallback } from "react";
import { api, PARequestDetail, RuleEvaluation } from "@/lib/api";
import { Sparkles, ShieldCheck, ThumbsUp, ThumbsDown, Layers, Cpu, BookMarked, Power } from "lucide-react";

interface ExplainabilityPanelProps {
  detail: PARequestDetail;
  onChanged?: () => void;
}

const AI_TOGGLE_KEY = "pa.ai_assist_enabled";

function readToggle(): boolean {
  try {
    const v = localStorage.getItem(AI_TOGGLE_KEY);
    return v === null ? true : v === "true";
  } catch {
    return true;
  }
}

/**
 * Responsible-AI panel: explains WHY the AI recommended what it did (sources,
 * confidence, criteria version, Tier-1 rules, no-code rules, ML prediction),
 * lets a reviewer ACCEPT or OVERRIDE the recommendation (logged to the audit
 * trail), and exposes a per-workflow AI enable/disable control.
 */
export function ExplainabilityPanel({ detail, onChanged }: ExplainabilityPanelProps) {
  const reqId = detail.auth_request_id;
  const [aiEnabled, setAiEnabled] = useState(readToggle());
  const [ruleEval, setRuleEval] = useState<RuleEvaluation | null>(null);
  const [mlPred, setMlPred] = useState<Record<string, unknown> | null>(null);
  const [overrideMode, setOverrideMode] = useState(false);
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [decisionLogged, setDecisionLogged] = useState<string | null>(null);

  const loadEvidence = useCallback(() => {
    api.evaluateRequestRules(reqId).then(setRuleEval).catch(() => setRuleEval(null));
    api.getMLPrediction(reqId).then((p) => setMlPred("message" in p ? null : p)).catch(() => setMlPred(null));
  }, [reqId]);

  useEffect(() => { if (aiEnabled) loadEvidence(); }, [aiEnabled, loadEvidence]);

  const toggleAi = (val: boolean) => {
    setAiEnabled(val);
    try { localStorage.setItem(AI_TOGGLE_KEY, String(val)); } catch { /* ignore */ }
  };

  const record = async (action: "accept" | "override") => {
    setBusy(true);
    try {
      await api.recordAIDecision(reqId, action, reason || undefined);
      setDecisionLogged(action === "accept" ? "AI recommendation accepted" : "AI recommendation overridden");
      setOverrideMode(false);
      setReason("");
      onChanged?.();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="card border-l-4 border-l-indigo-400">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-databricks-dark flex items-center gap-2">
          <Sparkles size={16} className="text-indigo-500" /> AI Decision Support &amp; Explainability
        </h3>
        {/* Per-workflow AI enable/disable (governance: AI can be turned off) */}
        <button
          onClick={() => toggleAi(!aiEnabled)}
          className={`flex items-center gap-1 text-xs px-2 py-1 rounded-full border ${
            aiEnabled ? "border-green-300 text-green-700 bg-green-50" : "border-gray-300 text-gray-500"
          }`}
          title="Enable or disable AI assistance for this workflow"
        >
          <Power size={12} /> AI {aiEnabled ? "On" : "Off"}
        </button>
      </div>

      {!aiEnabled ? (
        <p className="text-sm text-gray-500">
          AI assistance is disabled for this workflow. Reviews proceed with deterministic
          rules and manual clinical judgment only. Re-enable above to view AI evidence.
        </p>
      ) : (
        <div className="space-y-3">
          {/* Recommendation + confidence */}
          <div className="flex items-center gap-3">
            <span className="text-sm font-medium text-gray-800">
              {detail.ai_recommendation || "No AI recommendation on file"}
            </span>
            {detail.ai_confidence !== null && detail.ai_confidence !== undefined && (
              <span className="ml-auto flex items-center gap-2">
                <div className="w-24 h-1.5 bg-gray-200 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${
                      detail.ai_confidence > 0.8 ? "bg-green-500" : detail.ai_confidence > 0.6 ? "bg-amber-500" : "bg-red-500"
                    }`}
                    style={{ width: `${detail.ai_confidence * 100}%` }}
                  />
                </div>
                <span className="text-xs text-gray-500">{(detail.ai_confidence * 100).toFixed(0)}% confidence</span>
              </span>
            )}
          </div>

          {/* Contributing factors */}
          <div className="grid grid-cols-2 gap-2 text-sm">
            <Factor icon={<Layers size={14} className="text-gray-400" />} label="Tier-1 deterministic rules">
              {detail.tier1_auto_eligible === true ? "Auto-eligible (all rules pass)"
                : detail.tier1_auto_eligible === false ? "Not auto-eligible" : "—"}
            </Factor>
            <Factor icon={<Cpu size={14} className="text-gray-400" />} label="ML model prediction">
              {mlPred ? `${mlPred.predicted_determination} (${((Number(mlPred.confidence) || 0) * 100).toFixed(0)}%)` : "—"}
            </Factor>
            <Factor icon={<BookMarked size={14} className="text-gray-400" />} label="Criteria applied">
              {detail.criteria_version || detail.criteria_source || "—"}
            </Factor>
            <Factor icon={<ShieldCheck size={14} className="text-gray-400" />} label="No-code rule engine">
              {ruleEval?.fired_rule
                ? `${ruleEval.action?.replace(/_/g, " ")} — ${ruleEval.fired_rule.name}`
                : "No rule fired"}
            </Factor>
          </div>

          {ruleEval && ruleEval.matched_rules.length > 0 && (
            <div className="text-xs text-gray-500">
              Rules matched: {ruleEval.matched_rules.map((m) => m.name).join(", ")}
            </div>
          )}

          <div className="text-xs text-gray-400">
            Sources: medical policy criteria, member clinical evidence, ML model, and payer rules.
            Full agent reasoning trace is captured in Observability (MLflow → Unity Catalog).
          </div>

          {/* Human oversight: accept / override */}
          {decisionLogged ? (
            <div className="text-sm text-green-700 flex items-center gap-1">
              <ThumbsUp size={14} /> {decisionLogged} — logged to the audit trail.
            </div>
          ) : detail.ai_recommendation ? (
            overrideMode ? (
              <div className="space-y-2">
                <textarea
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                  placeholder="Reason for overriding the AI recommendation (required for audit)…"
                  className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm h-16"
                />
                <div className="flex gap-2">
                  <button onClick={() => record("override")} disabled={busy || !reason.trim()} className="btn-primary text-sm disabled:opacity-40">
                    Confirm Override
                  </button>
                  <button onClick={() => setOverrideMode(false)} className="btn-secondary text-sm">Cancel</button>
                </div>
              </div>
            ) : (
              <div className="flex gap-2">
                <button onClick={() => record("accept")} disabled={busy} className="btn-secondary text-sm flex items-center gap-1">
                  <ThumbsUp size={14} /> Accept AI
                </button>
                <button onClick={() => setOverrideMode(true)} className="text-sm flex items-center gap-1 px-3 py-1.5 rounded-md border border-gray-300 text-gray-600 hover:bg-gray-50">
                  <ThumbsDown size={14} /> Override
                </button>
              </div>
            )
          ) : null}
        </div>
      )}
    </div>
  );
}

function Factor({ icon, label, children }: { icon: React.ReactNode; label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-2 p-2 bg-gray-50 rounded-md">
      {icon}
      <div>
        <div className="text-xs text-gray-400">{label}</div>
        <div className="text-gray-800">{children}</div>
      </div>
    </div>
  );
}
