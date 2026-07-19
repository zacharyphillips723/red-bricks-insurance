import { useState, useEffect, useCallback } from "react";
import { Gauge, Loader2, ThumbsUp, ThumbsDown, RefreshCw, FlaskConical, Play } from "lucide-react";
import { api } from "@/lib/api";
import type { FeedbackSummary, EvalScores } from "@/lib/api";

function scoreColor(v: number): string {
  if (v >= 4) return "text-green-600";
  if (v >= 3) return "text-amber-600";
  return "text-red-600";
}

export function EvalQuality() {
  const [summary, setSummary] = useState<FeedbackSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  // Judge tester
  const [question, setQuestion] = useState("");
  const [response, setResponse] = useState("");
  const [scores, setScores] = useState<EvalScores | null>(null);
  const [scoring, setScoring] = useState(false);
  const [scoreErr, setScoreErr] = useState("");

  const load = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      setSummary(await api.getFeedbackSummary().catch(() => null));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);
  useEffect(() => { load(); }, [load]);

  const runScorers = async () => {
    if (!response.trim() || scoring) return;
    setScoring(true);
    setScores(null);
    setScoreErr("");
    try {
      setScores(await api.runEvalScorers(question.trim(), response.trim()));
    } catch (e) {
      setScoreErr(String(e));
    } finally {
      setScoring(false);
    }
  };

  if (loading)
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );

  const rate = summary?.satisfaction_rate;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-800 flex items-center gap-2">
          <Gauge className="w-6 h-6 text-databricks-red" /> Agent Quality
        </h2>
        <p className="text-sm text-gray-500 mt-0.5">
          Care-manager feedback captured in Lakebase, plus on-demand LLM-as-judge scoring — the same relevance / groundedness / safety dimensions <span className="font-mono text-xs">mlflow.genai.evaluate</span> applies, run live.
        </p>
      </div>

      {/* Feedback aggregates */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card p-5">
          <div className="text-3xl font-bold text-gray-800">{summary?.total ?? 0}</div>
          <div className="text-xs text-gray-400 mt-1">Total Ratings</div>
        </div>
        <div className="card p-5">
          <div className="text-3xl font-bold text-green-600 flex items-center gap-1.5"><ThumbsUp className="w-5 h-5" />{summary?.positive ?? 0}</div>
          <div className="text-xs text-gray-400 mt-1">Positive</div>
        </div>
        <div className="card p-5">
          <div className="text-3xl font-bold text-red-600 flex items-center gap-1.5"><ThumbsDown className="w-5 h-5" />{summary?.negative ?? 0}</div>
          <div className="text-xs text-gray-400 mt-1">Negative</div>
        </div>
        <div className="card p-5">
          <div className="text-3xl font-bold text-databricks-red">{rate != null ? `${Math.round(rate * 100)}%` : "—"}</div>
          <div className="text-xs text-gray-400 mt-1">Satisfaction Rate</div>
        </div>
      </div>

      {/* LLM-as-judge tester */}
      <div className="card p-5">
        <h3 className="font-semibold text-gray-800 flex items-center gap-2 mb-1">
          <FlaskConical className="w-4 h-4 text-databricks-red" /> LLM-as-Judge Scorer
        </h3>
        <p className="text-xs text-gray-500 mb-3">Paste an agent response (and optionally the question) to score it on three quality dimensions.</p>
        <label className="block text-xs text-gray-500 mb-1">Question (optional)</label>
        <input value={question} onChange={(e) => setQuestion(e.target.value)}
          placeholder="What outreach should we prioritize for this member?"
          className="w-full mb-3 px-3 py-2 rounded-lg border border-gray-300 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red" />
        <label className="block text-xs text-gray-500 mb-1">Agent Response</label>
        <textarea value={response} onChange={(e) => setResponse(e.target.value)}
          rows={4} placeholder="Paste the agent's answer here…"
          className="w-full mb-3 px-3 py-2 rounded-lg border border-gray-300 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red" />
        <button onClick={runScorers} disabled={!response.trim() || scoring}
          className="btn-primary px-4 py-2 text-sm flex items-center gap-2 disabled:opacity-50">
          {scoring ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />} Run Scorers
        </button>

        {scoreErr && <p className="mt-3 text-xs text-red-600">{scoreErr}</p>}
        {scores && (
          <div className="mt-4 grid grid-cols-3 gap-4">
            {(["relevance", "groundedness", "clinical_safety"] as const).map((k) => (
              <div key={k} className="text-center border border-gray-100 rounded-lg py-3">
                <div className={`text-3xl font-bold ${scoreColor(Number(scores[k]))}`}>{scores[k]}<span className="text-base text-gray-300">/5</span></div>
                <div className="text-xs text-gray-400 mt-1 capitalize">{k.replace("_", " ")}</div>
              </div>
            ))}
            <div className="col-span-3 text-xs text-gray-500 italic">{scores.rationale}</div>
            <div className="col-span-3 text-[11px] text-gray-400">Judged by {scores.judge_endpoint}</div>
          </div>
        )}
      </div>

      {/* Recent feedback */}
      <div className="card">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h3 className="font-semibold text-gray-800">Recent Feedback</h3>
          <button onClick={() => load(true)} disabled={refreshing}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50">
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? "animate-spin" : ""}`} />Refresh
          </button>
        </div>
        <div className="divide-y divide-gray-100">
          {(summary?.recent || []).map((r, i) => (
            <div key={i} className="p-4 flex items-start gap-3">
              {r.rating === "positive"
                ? <ThumbsUp className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
                : <ThumbsDown className="w-4 h-4 text-red-500 mt-0.5 shrink-0" />}
              <div className="flex-1 min-w-0">
                {r.message_content && <p className="text-sm text-gray-700 line-clamp-2">{r.message_content}</p>}
                {r.comment && <p className="text-xs text-gray-500 mt-1 italic">“{r.comment}”</p>}
                <div className="text-[11px] text-gray-400 mt-1">{r.user_email || "unknown"} · {r.created_at ? new Date(r.created_at).toLocaleString() : ""}</div>
              </div>
            </div>
          ))}
          {(!summary?.recent || summary.recent.length === 0) && (
            <div className="px-4 py-8 text-center text-gray-400 text-sm">No feedback captured yet. Rate an agent response with 👍/👎 to populate this panel.</div>
          )}
        </div>
      </div>
    </div>
  );
}
