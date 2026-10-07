import { useState } from "react";
import { Sparkles, FileText, RefreshCw, CheckCircle2, AlertCircle, ClipboardList } from "lucide-react";
import { api, IntakeParseResult, SubmissionExtract, SubmissionCompleteness } from "@/lib/api";

type Mode = "intake" | "document";

const SAMPLE = `New business submission — Acme Manufacturing. ~420 eligible employees, suburban Ohio,
manufacturing (SIC heavy). Current fully-insured premium about $4.1M/yr with roughly $3.4M in
paid claims last year. Looking at self-funded (ASO) for a 1/1 effective date. Competitor quoted
around $820 PMPM. They're expecting something near a 9% increase if they stay fully insured.`;

export default function Intake() {
  const [mode, setMode] = useState<Mode>("intake");
  const [docType, setDocType] = useState("submission");
  const [textValue, setTextValue] = useState("");
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fields, setFields] = useState<Record<string, unknown> | null>(null);
  const [completeness, setCompleteness] = useState<SubmissionCompleteness | null>(null);
  const [memo, setMemo] = useState<string | null>(null);

  const run = async () => {
    if (!textValue.trim()) return;
    setRunning(true);
    setError(null);
    setMemo(null);
    try {
      if (mode === "intake") {
        const res: IntakeParseResult = await api.parseIntake(textValue, true);
        setFields(res.fields);
        setCompleteness(res.completeness);
        setMemo(res.strategy_memo || null);
      } else {
        const res: SubmissionExtract = await api.extractSubmission(textValue, docType);
        setFields(res.fields);
        setCompleteness(res.completeness);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Extraction failed");
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Quote Intake</h1>
        <p className="text-sm text-gray-500 mt-1">
          Turn a free-text submission or a pasted census/SBC/competitor quote into a structured,
          completeness-gated submission. The AE verifies a pre-filled memo instead of starting blank.
        </p>
      </div>

      {/* Mode toggle */}
      <div className="flex items-center gap-2">
        <button
          onClick={() => setMode("intake")}
          className={`px-3 py-1.5 rounded-full text-xs font-medium flex items-center gap-1.5 ${
            mode === "intake" ? "bg-databricks-red text-white" : "bg-gray-100 text-gray-600 hover:bg-gray-200"
          }`}
        >
          <Sparkles className="w-3.5 h-3.5" /> NL Submission
        </button>
        <button
          onClick={() => setMode("document")}
          className={`px-3 py-1.5 rounded-full text-xs font-medium flex items-center gap-1.5 ${
            mode === "document" ? "bg-databricks-red text-white" : "bg-gray-100 text-gray-600 hover:bg-gray-200"
          }`}
        >
          <FileText className="w-3.5 h-3.5" /> Document Extract
        </button>
      </div>

      <div className="card space-y-4">
        {mode === "document" && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Document Type</label>
            <select
              value={docType}
              onChange={(e) => setDocType(e.target.value)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/30"
            >
              <option value="submission">Submission</option>
              <option value="census">Census</option>
              <option value="sbc">Summary of Benefits (SBC)</option>
              <option value="competitor_quote">Competitor Quote</option>
            </select>
          </div>
        )}
        <textarea
          value={textValue}
          onChange={(e) => setTextValue(e.target.value)}
          rows={7}
          placeholder={mode === "intake" ? "Paste or type the submission in plain language…" : "Paste the document text…"}
          className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm font-mono
                     focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
        />
        <div className="flex gap-3">
          <button onClick={run} disabled={running} className="btn-primary flex items-center gap-2">
            {running ? <RefreshCw className="w-4 h-4 animate-spin" /> : <ClipboardList className="w-4 h-4" />}
            {running ? "Analyzing…" : mode === "intake" ? "Parse Submission" : "Extract Fields"}
          </button>
          <button onClick={() => setTextValue(SAMPLE)} className="btn-secondary text-sm">
            Load sample
          </button>
        </div>
      </div>

      {error && <div className="card border-red-200 bg-red-50 text-sm text-red-700">{error}</div>}

      {completeness && (
        <div className="card">
          <div className="flex items-center justify-between flex-wrap gap-2 mb-4">
            <h2 className="text-lg font-semibold text-databricks-dark">Completeness Gate</h2>
            <span
              className={
                completeness.quote_ready
                  ? "badge-positive flex items-center gap-1"
                  : "badge-warning flex items-center gap-1"
              }
            >
              {completeness.quote_ready ? <CheckCircle2 className="w-3 h-3" /> : <AlertCircle className="w-3 h-3" />}
              {completeness.quote_ready ? "Quote-ready" : "Needs more info"}
            </span>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-3 mb-2">
            <div
              className="bg-databricks-red h-3 rounded-full transition-all"
              style={{ width: `${completeness.percent_complete}%` }}
            />
          </div>
          <div className="text-sm text-gray-600">
            Pre-filled {completeness.percent_complete}%.
            {completeness.missing_fields.length > 0 && (
              <> Missing: {completeness.missing_fields.map((f) => f.replace(/_/g, " ")).join(", ")}.</>
            )}
          </div>
        </div>
      )}

      {memo && (
        <div className="card border-l-4 border-l-databricks-red">
          <div className="text-sm font-semibold text-databricks-dark mb-1">Underwriting Strategy Memo</div>
          <p className="text-sm text-gray-700 leading-relaxed">{memo}</p>
        </div>
      )}

      {fields && (
        <div className="card">
          <h2 className="text-lg font-semibold text-databricks-dark mb-4">Extracted Submission</h2>
          <table className="w-full text-sm">
            <tbody>
              {Object.entries(fields).map(([k, v]) => (
                <tr key={k} className="border-b border-gray-50">
                  <td className="py-2 pr-4 font-medium text-gray-500 capitalize">{k.replace(/_/g, " ")}</td>
                  <td className="py-2 text-databricks-dark">
                    {v === null || v === undefined || v === "" ? (
                      <span className="text-gray-300">—</span>
                    ) : (
                      String(v)
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
