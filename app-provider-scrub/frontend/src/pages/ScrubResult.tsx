import { useState } from "react";
import {
  ShieldAlert, ShieldCheck, ShieldX, Loader2, RefreshCw, ArrowRight,
  FileWarning, Cpu, ScrollText, Wrench, CheckCircle2, ThumbsUp, ThumbsDown, MessageSquare,
} from "lucide-react";
import { api } from "@/lib/api";
import type { DraftClaim, ReasonCard, ScrubResult as ScrubResultType } from "@/lib/api";

// Logs an MLflow human-feedback assessment on the agent trace via the backend.
function useFeedback(traceId?: string | null, sessionId?: string) {
  const [sent, setSent] = useState<Record<string, boolean>>({});
  const send = async (target: string, value: boolean, rationale?: string) => {
    try {
      await api.submitFeedback({ trace_id: traceId, session_id: sessionId, target, value, rationale });
      setSent((s) => ({ ...s, [target]: true }));
    } catch (e) {
      console.error("feedback failed", e);
    }
  };
  return { sent, send };
}

function ThumbButtons({ onVote, disabled }: { onVote: (up: boolean) => void; disabled?: boolean }) {
  return (
    <div className="flex items-center gap-1">
      <button
        onClick={() => onVote(true)}
        disabled={disabled}
        title="Correct / useful"
        className="p-1.5 rounded-md text-gray-400 hover:text-green-600 hover:bg-green-50 disabled:opacity-40"
      >
        <ThumbsUp className="w-4 h-4" />
      </button>
      <button
        onClick={() => onVote(false)}
        disabled={disabled}
        title="Wrong / not useful"
        className="p-1.5 rounded-md text-gray-400 hover:text-databricks-red hover:bg-red-50 disabled:opacity-40"
      >
        <ThumbsDown className="w-4 h-4" />
      </button>
    </div>
  );
}

interface Props {
  result: ScrubResultType | null;
  draft: DraftClaim | null;
  onResult: (r: ScrubResultType) => void;
  onDraftChange?: (d: DraftClaim) => void;
  onEditDraft?: () => void;
  onGoCompose: () => void;
}

function scoreTheme(score: number) {
  if (score >= 70) return { color: "#DC2626", ring: "text-red-600", bg: "bg-red-50", label: "Likely Denied", Icon: ShieldX };
  if (score >= 35) return { color: "#D97706", ring: "text-amber-600", bg: "bg-amber-50", label: "At Risk", Icon: ShieldAlert };
  return { color: "#16A34A", ring: "text-green-600", bg: "bg-green-50", label: "Clean", Icon: ShieldCheck };
}

const LAYER_STYLE: Record<string, { label: string; cls: string; Icon: typeof Cpu }> = {
  rule: { label: "Rule", cls: "bg-blue-100 text-blue-800", Icon: ScrollText },
  ml: { label: "ML model", cls: "bg-purple-100 text-purple-800", Icon: Cpu },
  rag: { label: "Policy RAG", cls: "bg-teal-100 text-teal-800", Icon: FileWarning },
};

function Gauge({ score }: { score: number }) {
  const theme = scoreTheme(score);
  const r = 52;
  const circ = 2 * Math.PI * r;
  const offset = circ * (1 - score / 100);
  return (
    <div className="relative w-36 h-36">
      <svg className="w-36 h-36 -rotate-90" viewBox="0 0 120 120">
        <circle cx="60" cy="60" r={r} fill="none" stroke="#E5E7EB" strokeWidth="10" />
        <circle
          cx="60" cy="60" r={r} fill="none" stroke={theme.color} strokeWidth="10"
          strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round"
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className={`text-3xl font-bold ${theme.ring}`}>{score}</span>
        <span className="text-[10px] uppercase tracking-wide text-gray-400">Risk</span>
      </div>
    </div>
  );
}

export function ScrubResult({ result, draft, onResult, onDraftChange, onEditDraft, onGoCompose }: Props) {
  const [authRef, setAuthRef] = useState("");
  const [addedNotes, setAddedNotes] = useState("");
  const [fixed, setFixed] = useState<Set<string>>(new Set());
  const [resubmitting, setResubmitting] = useState(false);
  const [prevScore, setPrevScore] = useState<number | null>(null);
  const [noChangeHint, setNoChangeHint] = useState(false);
  const [comment, setComment] = useState("");
  const fb = useFeedback(result?.trace_id, result?.session_id);

  if (!result) {
    return (
      <div className="max-w-3xl mx-auto text-center py-20">
        <ShieldAlert className="w-12 h-12 text-gray-300 mx-auto mb-4" />
        <p className="text-gray-500 mb-4">No scrub result yet.</p>
        <button onClick={onGoCompose} className="btn-primary">Compose a claim</button>
      </div>
    );
  }

  const theme = scoreTheme(result.risk_score);
  const toggleFixed = (carc: string) =>
    setFixed((s) => {
      const n = new Set(s);
      n.has(carc) ? n.delete(carc) : n.add(carc);
      return n;
    });

  const hasAuthCard = result.reason_cards.some((c) => c.carc_code === "CO-197");
  // Whether the user has supplied a real amendment the engine can act on.
  const hasAmendment = authRef.trim().length > 0 || addedNotes.trim().length > 0;

  const resubmit = async () => {
    if (!draft) return;
    setResubmitting(true);
    setNoChangeHint(false);
    setPrevScore(result.risk_score);
    try {
      const amended: DraftClaim = { ...draft };
      if (authRef.trim()) amended.auth_reference = authRef.trim();
      if (addedNotes.trim()) {
        amended.clinical_notes = [draft.clinical_notes, addedNotes.trim()]
          .filter(Boolean)
          .join("\n\n");
      }
      const res = await api.resubmitScrub(result.session_id, amended);
      onDraftChange?.(amended);
      onResult(res);
      setFixed(new Set());
      setNoChangeHint(res.risk_score === result.risk_score);
    } catch (e) {
      console.error(e);
    } finally {
      setResubmitting(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
            <ShieldAlert className="text-databricks-red" /> Scrub Result
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            {result.member_name || result.member_id} · DOS {result.date_of_service} ·{" "}
            {result.request_type === "claim" ? "Medical Claim" : "Prior-Auth Request"}
            {result.resubmitted_from && (
              <span className="ml-2 text-xs bg-gray-100 rounded px-2 py-0.5">
                resubmitted from {result.resubmitted_from}
              </span>
            )}
          </p>
        </div>
        <button onClick={onGoCompose} className="btn-secondary text-sm">New scrub</button>
      </div>

      {/* Score panel */}
      <div className={`card p-6 ${theme.bg} border`}>
        <div className="flex items-center gap-8">
          <Gauge score={result.risk_score} />
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-1">
              <theme.Icon className={`w-6 h-6 ${theme.ring}`} />
              <span className={`text-xl font-bold ${theme.ring}`}>{theme.label}</span>
            </div>
            <p className="text-sm text-gray-600">
              {result.reason_cards.length === 0
                ? "No denial-risk issues detected. Safe to submit."
                : `${result.reason_cards.length} potential denial reason(s) detected before submission.`}
            </p>
            {result.ml_denial_prob != null && (
              <p className="text-xs text-gray-500 mt-2">
                Model denial probability: <span className="font-semibold">{Math.round(result.ml_denial_prob * 100)}%</span>
              </p>
            )}
          </div>
          {prevScore != null && prevScore !== result.risk_score && (
            <div className="text-center">
              <div className="flex items-center gap-2 text-sm">
                <span className="text-gray-400 line-through">{prevScore}</span>
                <ArrowRight className="w-4 h-4 text-gray-400" />
                <span className={`text-2xl font-bold ${theme.ring}`}>{result.risk_score}</span>
              </div>
              <span className="text-xs text-gray-400">after fixes</span>
            </div>
          )}
        </div>
      </div>

      {/* Overall feedback — logs an MLflow assessment on the agent trace */}
      <div className="card p-5">
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-2">
            <MessageSquare className="w-4 h-4 text-databricks-red" />
            <span className="font-semibold text-databricks-dark">Was this scrub accurate and useful?</span>
          </div>
          {fb.sent["overall"] ? (
            <span className="inline-flex items-center gap-1.5 text-sm text-green-700">
              <CheckCircle2 className="w-4 h-4" /> Thanks — feedback recorded on the trace
            </span>
          ) : (
            <div className="flex items-center gap-3">
              <input
                className="border border-gray-300 rounded-md px-3 py-1.5 text-sm w-72"
                placeholder="Optional comment (what was right/wrong)…"
                value={comment}
                onChange={(e) => setComment(e.target.value)}
              />
              <ThumbButtons
                disabled={!result.trace_id}
                onVote={(up) => fb.send("overall", up, comment.trim() || undefined)}
              />
            </div>
          )}
        </div>
        {!result.trace_id && !fb.sent["overall"] && (
          <p className="text-[11px] text-gray-400 mt-2">Trace id unavailable — feedback logging disabled for this run.</p>
        )}
      </div>

      {/* Per-claim ML drivers (local SHAP) */}
      {result.ml_contributions && result.ml_contributions.length > 0 && (
        <div className="card p-5">
          <div className="flex items-center gap-2 mb-1">
            <Cpu className="w-4 h-4 text-purple-600" />
            <h3 className="font-semibold text-databricks-dark">Why this claim scored as it did</h3>
          </div>
          <p className="text-xs text-gray-500 mb-3">
            Top model drivers for THIS claim (SHAP contribution to denial probability).
          </p>
          <div className="space-y-2">
            {result.ml_contributions.map((c) => {
              const contribNum = Number(c.contribution) || 0;
              const up = contribNum >= 0;
              return (
                <div key={c.feature} className="flex items-center gap-3">
                  <div className="w-56 shrink-0 text-sm text-gray-700 truncate" title={c.label || c.feature}>
                    {c.label || c.feature}
                  </div>
                  <div className="flex-1 flex items-center">
                    <div className="flex-1 flex justify-end">
                      {!up && <div className="h-3 bg-green-500 rounded-l" style={{ width: `${Math.min(100, Math.abs(contribNum) * 400)}%` }} />}
                    </div>
                    <div className="w-px h-4 bg-gray-300" />
                    <div className="flex-1">
                      {up && <div className="h-3 bg-databricks-red rounded-r" style={{ width: `${Math.min(100, Math.abs(contribNum) * 400)}%` }} />}
                    </div>
                  </div>
                  <div className={`w-20 shrink-0 text-right text-xs tabular-nums ${up ? "text-databricks-red" : "text-green-600"}`}>
                    {up ? "+" : ""}{contribNum.toFixed(3)}
                  </div>
                </div>
              );
            })}
          </div>
          <p className="text-[11px] text-gray-400 mt-3">Red raises denial risk · green lowers it.</p>
        </div>
      )}

      {/* Reason cards */}
      <div className="space-y-3">
        {result.reason_cards.map((card, i) => (
          <ReasonCardView
            key={`${card.carc_code}-${i}`}
            card={card}
            fixed={fixed.has(card.carc_code)}
            onToggleFixed={() => toggleFixed(card.carc_code)}
            feedbackSent={fb.sent[card.carc_code]}
            onFeedback={(up) => fb.send(card.carc_code, up)}
            feedbackDisabled={!result.trace_id}
          />
        ))}
      </div>

      {/* Resubmit */}
      {result.reason_cards.length > 0 && draft && (
        <div className="card p-6 space-y-4">
          <div>
            <h3 className="font-semibold text-databricks-dark">Fix &amp; re-scrub</h3>
            <p className="text-sm text-gray-500 mt-1">
              Apply the remediation, then re-scrub to confirm the risk drops before you submit.
              Coding or eligibility corrections need a claim edit — use <span className="font-medium">Edit draft</span>.
            </p>
          </div>

          {hasAuthCard && (
            <div>
              <label className="block text-xs font-semibold text-gray-500 mb-1">
                APPROVED AUTHORIZATION REFERENCE (clears CO-197)
              </label>
              <input
                className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
                placeholder="e.g. AUTH-99823"
                value={authRef}
                onChange={(e) => setAuthRef(e.target.value)}
              />
            </div>
          )}

          <div>
            <label className="block text-xs font-semibold text-gray-500 mb-1">
              ADD CLINICAL DOCUMENTATION (supports medical necessity — CO-50 / CO-16)
            </label>
            <textarea
              rows={3}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm"
              placeholder="Attach supporting clinical detail: prior conservative therapy, imaging, lab values, functional status…"
              value={addedNotes}
              onChange={(e) => setAddedNotes(e.target.value)}
            />
          </div>

          {noChangeHint && (
            <div className="text-sm text-amber-800 bg-amber-50 border border-amber-200 rounded-md px-3 py-2">
              Risk score unchanged — the remaining reasons (e.g. coding mismatch or eligibility) can't be
              resolved with an auth reference or documentation. Use <span className="font-medium">Edit draft</span> to
              correct the codes, diagnosis, or date of service.
            </div>
          )}

          <div className="flex items-center gap-3">
            <button
              onClick={resubmit}
              disabled={resubmitting || !hasAmendment}
              title={hasAmendment ? "" : "Enter an auth reference or add documentation first"}
              className="btn-primary flex items-center gap-2 whitespace-nowrap disabled:opacity-50"
            >
              {resubmitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
              Re-scrub clean
            </button>
            {onEditDraft && (
              <button onClick={onEditDraft} className="btn-secondary text-sm">Edit draft</button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function ReasonCardView({
  card, fixed, onToggleFixed, feedbackSent, onFeedback, feedbackDisabled,
}: {
  card: ReasonCard;
  fixed: boolean;
  onToggleFixed: () => void;
  feedbackSent?: boolean;
  onFeedback: (up: boolean) => void;
  feedbackDisabled?: boolean;
}) {
  const layer = LAYER_STYLE[card.layer] || LAYER_STYLE.rule;
  const remediation = card.remediation || card.remediation_text;
  const pct = Math.round(card.likelihood * 100);
  return (
    <div className={`card p-5 ${fixed ? "opacity-60" : ""}`}>
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 mb-1">
            <span className="font-mono text-xs font-bold bg-databricks-dark text-white rounded px-2 py-0.5">
              {card.carc_code}
            </span>
            <span className="font-semibold text-databricks-dark">{card.reason_label}</span>
            <span className={`inline-flex items-center gap-1 text-xs font-medium rounded px-2 py-0.5 ${layer.cls}`}>
              <layer.Icon className="w-3 h-3" /> {layer.label}
            </span>
          </div>
          {/* Likelihood bar */}
          <div className="flex items-center gap-2 mt-2 mb-3">
            <div className="flex-1 h-2 bg-gray-100 rounded-full overflow-hidden max-w-xs">
              <div className="h-full bg-databricks-red" style={{ width: `${pct}%` }} />
            </div>
            <span className="text-xs text-gray-500">{pct}% likelihood</span>
          </div>
          {card.evidence && <p className="text-sm text-gray-600 mb-3">{card.evidence}</p>}
          {remediation && (
            <div className="bg-amber-50 border border-amber-200 rounded-md p-3">
              <div className="flex items-center gap-1.5 text-xs font-semibold text-amber-800 mb-1">
                <Wrench className="w-3.5 h-3.5" /> Remediation
              </div>
              <p className="text-sm text-amber-900">{remediation}</p>
              {(card.required_action || card.doc_needed) && (
                <p className="text-xs text-amber-700 mt-2">
                  {card.required_action && <>Action: <span className="font-medium">{card.required_action}</span></>}
                  {card.doc_needed && <> · Needs: <span className="font-medium">{card.doc_needed}</span></>}
                </p>
              )}
            </div>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          <button
            onClick={onToggleFixed}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium border transition-colors ${
              fixed ? "bg-green-50 text-green-700 border-green-200" : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
            }`}
          >
            <CheckCircle2 className="w-3.5 h-3.5" /> {fixed ? "Fixed" : "Mark fixed"}
          </button>
          {feedbackSent ? (
            <span className="inline-flex items-center gap-1 text-[11px] text-green-700">
              <CheckCircle2 className="w-3 h-3" /> Rated
            </span>
          ) : (
            <div className="flex items-center gap-1" title="Was this denial reason correct?">
              <span className="text-[11px] text-gray-400">Reason right?</span>
              <ThumbButtons disabled={feedbackDisabled} onVote={onFeedback} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
