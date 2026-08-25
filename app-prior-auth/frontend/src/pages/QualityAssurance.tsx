import { useState, useEffect, useCallback } from "react";
import { api, QAReview, QAQuestion, QAReviewerScorecard } from "@/lib/api";
import { ClipboardCheck, Play, X, AlertTriangle, CheckCircle, XCircle } from "lucide-react";

function pill(s: string | null): string {
  if (s === "Scored") return "bg-blue-50 text-blue-700";
  if (s === "Pending Score") return "bg-amber-50 text-amber-700";
  return "bg-gray-100 text-gray-600";
}

export function QualityAssurance() {
  const [reviews, setReviews] = useState<QAReview[]>([]);
  const [questions, setQuestions] = useState<QAQuestion[]>([]);
  const [scorecard, setScorecard] = useState<QAReviewerScorecard[]>([]);
  const [samplePct, setSamplePct] = useState(10);
  const [scoring, setScoring] = useState<QAReview | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api.listQAReviews().then(setReviews).catch(console.error);
    api.getQAReviewerScorecard().then(setScorecard).catch(console.error);
  }, []);
  useEffect(() => { load(); api.listQAQuestions().then(setQuestions).catch(console.error); }, [load]);

  const genSample = async () => {
    setBusy(true);
    try {
      const r = await api.generateQASample(samplePct);
      alert(`Sampled ${r.sampled} additional case(s) into the QA queue.`);
      load();
    } catch (e) { alert((e as Error).message); }
    finally { setBusy(false); }
  };

  const scored = reviews.filter((r) => r.status === "Scored");
  const passRate = scored.length ? Math.round(scored.filter((r) => r.passed).length / scored.length * 100) : null;
  const avgScore = scored.length ? Math.round(scored.reduce((a, r) => a + (r.score_pct || 0), 0) / scored.length) : null;
  const critical = scored.filter((r) => r.critical_error).length;
  const pending = reviews.filter((r) => r.status === "Pending Score").length;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <ClipboardCheck size={22} className="text-databricks-dark" />
        <h2 className="text-2xl font-bold text-databricks-dark">Quality Assurance</h2>
      </div>

      <div className="grid grid-cols-4 gap-4">
        <Kpi label="Pass Rate" value={passRate === null ? "—" : `${passRate}%`} />
        <Kpi label="Avg Score" value={avgScore === null ? "—" : `${avgScore}%`} />
        <Kpi label="Critical Errors" value={critical} tone={critical ? "bad" : "ok"} />
        <Kpi label="Pending QA" value={pending} />
      </div>

      <div className="card flex items-end gap-3">
        <div>
          <div className="text-xs text-gray-500 uppercase tracking-wider">Random sample %</div>
          <input type="number" value={samplePct} min={1} max={100} onChange={(e) => setSamplePct(Number(e.target.value))}
            className="border border-gray-300 rounded-md px-2 py-1.5 text-sm w-24" />
        </div>
        <button onClick={genSample} disabled={busy} className="btn-primary text-sm flex items-center gap-1">
          <Play size={14} /> Generate QA Sample
        </button>
        <span className="text-xs text-gray-400 ml-2">Pulls a random % of completed determinations into the QA queue.</span>
      </div>

      {/* Reviewer scorecard */}
      {scorecard.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-2 text-xs font-semibold text-gray-500 uppercase tracking-wider bg-gray-50">Reviewer Quality Scorecard</div>
          <table className="w-full text-sm">
            <thead><tr className="text-left text-xs text-gray-500">
              <th className="px-4 py-2">Reviewer</th><th className="px-4 py-2">Role</th><th className="px-4 py-2">Scored</th>
              <th className="px-4 py-2">Avg %</th><th className="px-4 py-2">Pass Rate</th><th className="px-4 py-2">Critical</th>
            </tr></thead>
            <tbody className="divide-y divide-gray-100">
              {scorecard.map((s) => (
                <tr key={s.reviewer_id}>
                  <td className="px-4 py-2 font-medium">{s.display_name}</td>
                  <td className="px-4 py-2 text-gray-500">{s.role}</td>
                  <td className="px-4 py-2">{s.reviews_scored}</td>
                  <td className="px-4 py-2">{s.avg_score_pct ?? "—"}%</td>
                  <td className={`px-4 py-2 font-medium ${(s.pass_rate_pct ?? 100) < 90 ? "text-red-600" : "text-green-700"}`}>{s.pass_rate_pct ?? "—"}%</td>
                  <td className="px-4 py-2">{s.critical_errors > 0 ? <span className="text-red-600">{s.critical_errors}</span> : 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* QA review list */}
      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Case</th><th className="px-4 py-3">Reviewer Audited</th><th className="px-4 py-3">Sample</th>
            <th className="px-4 py-3">Status</th><th className="px-4 py-3">Score</th><th className="px-4 py-3">Result</th><th className="px-4 py-3"></th>
          </tr></thead>
          <tbody className="divide-y divide-gray-100">
            {reviews.map((r) => (
              <tr key={r.qa_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs">{r.auth_request_id}</td>
                <td className="px-4 py-3">{r.case_reviewer_name || "—"}</td>
                <td className="px-4 py-3 text-xs text-gray-500 capitalize">{r.sample_reason}</td>
                <td className="px-4 py-3"><span className={`px-2 py-0.5 rounded text-xs font-medium ${pill(r.status)}`}>{r.status}</span></td>
                <td className="px-4 py-3">{r.score_pct !== null ? `${r.score_pct}%` : "—"}</td>
                <td className="px-4 py-3">
                  {r.status === "Scored" && (r.critical_error
                    ? <span className="text-red-600 flex items-center gap-1 text-xs"><AlertTriangle size={12} /> Critical</span>
                    : r.passed ? <span className="text-green-700 flex items-center gap-1 text-xs"><CheckCircle size={12} /> Pass</span>
                    : <span className="text-amber-700 flex items-center gap-1 text-xs"><XCircle size={12} /> Fail</span>)}
                </td>
                <td className="px-4 py-3">
                  {r.status === "Pending Score" && (
                    <button onClick={() => setScoring(r)} className="text-xs text-databricks-red">Score</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {scoring && (
        <ScorecardModal
          review={scoring}
          questions={questions}
          onClose={() => setScoring(null)}
          onScored={() => { setScoring(null); load(); }}
        />
      )}
    </div>
  );
}

function ScorecardModal({ review, questions, onClose, onScored }: {
  review: QAReview; questions: QAQuestion[]; onClose: () => void; onScored: () => void;
}) {
  // Default: full credit per question (reviewer deducts where deficient).
  const [awarded, setAwarded] = useState<Record<string, number>>(
    () => Object.fromEntries(questions.map((q) => [q.question_id, q.weight])),
  );
  const [findings, setFindings] = useState("");
  const [busy, setBusy] = useState(false);

  const maxScore = questions.reduce((a, q) => a + q.weight, 0);
  const total = questions.reduce((a, q) => a + (awarded[q.question_id] ?? 0), 0);
  const pct = maxScore ? Math.round(total / maxScore * 100) : 0;
  const critical = questions.some((q) => q.is_critical && (awarded[q.question_id] ?? 0) < q.weight);
  const passed = pct >= 90 && !critical;

  const submit = async () => {
    setBusy(true);
    try {
      await api.scoreQAReview(review.qa_id, { awarded, findings: findings || undefined });
      onScored();
    } catch (e) { alert((e as Error).message); }
    finally { setBusy(false); }
  };

  return (
    <div className="fixed inset-0 bg-black/30 flex justify-end z-50" onClick={onClose}>
      <div className="w-[520px] max-w-full bg-white h-full overflow-y-auto shadow-2xl p-6 space-y-4" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between">
          <div>
            <h3 className="text-lg font-bold text-databricks-dark">QA Scorecard</h3>
            <p className="text-xs font-mono text-gray-400">{review.auth_request_id} · {review.case_reviewer_name}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
        </div>

        <div className="space-y-3">
          {questions.map((q) => (
            <div key={q.question_id} className="border border-gray-200 rounded-md p-3">
              <div className="flex items-start justify-between gap-2">
                <div className="text-sm">
                  {q.question_text}
                  {q.is_critical && <span className="ml-2 text-xs bg-red-50 text-red-700 px-1.5 py-0.5 rounded">critical</span>}
                </div>
                <span className="text-xs text-gray-400 whitespace-nowrap">/{q.weight}</span>
              </div>
              <div className="flex gap-2 mt-2">
                {[["Full", q.weight], ["Partial", Math.round(q.weight / 2)], ["Fail", 0]].map(([label, val]) => (
                  <button key={label as string}
                    onClick={() => setAwarded((a) => ({ ...a, [q.question_id]: val as number }))}
                    className={`px-2 py-1 rounded text-xs font-medium border ${
                      awarded[q.question_id] === val ? "border-databricks-red bg-red-50 text-databricks-red" : "border-gray-300 text-gray-600"
                    }`}>{label}</button>
                ))}
              </div>
            </div>
          ))}
        </div>

        <div className={`rounded-md p-3 text-sm ${critical ? "bg-red-50 text-red-700" : passed ? "bg-green-50 text-green-700" : "bg-amber-50 text-amber-700"}`}>
          Score: <span className="font-semibold">{pct}%</span> · {critical ? "Critical error — auto-fail" : passed ? "Pass" : "Below 90% — fail"}
        </div>

        <textarea value={findings} onChange={(e) => setFindings(e.target.value)} placeholder="Findings / coaching notes…"
          className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm h-20" />
        <button onClick={submit} disabled={busy} className="btn-primary w-full disabled:opacity-40">
          {busy ? "Saving…" : "Submit Scorecard"}
        </button>
      </div>
    </div>
  );
}

function Kpi({ label, value, tone }: { label: string; value: string | number; tone?: "ok" | "bad" }) {
  return (
    <div className="card">
      <div className="text-xs text-gray-500 uppercase tracking-wider">{label}</div>
      <div className={`text-2xl font-bold mt-1 ${tone === "bad" ? "text-red-600" : "text-databricks-dark"}`}>{value}</div>
    </div>
  );
}
