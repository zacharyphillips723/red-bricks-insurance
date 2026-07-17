import { useState, useEffect, useRef } from "react";
import {
  FileUp, Loader2, CheckCircle2, XCircle, AlertTriangle, Download,
  FileText, Sparkles, Database, ScanText, Gavel,
} from "lucide-react";
import { api, AdjudicationEvent, DocumentHandle, SampleScenario } from "@/lib/api";

interface DecisionState {
  decision: string;
  confidence: number;
  reasons: string[];
  matched_policy: Record<string, unknown> | null;
  extracted_procedure_codes: string[];
  extracted_diagnosis_codes: string[];
}

const STEPS = [
  { key: "parsing", label: "Parse document", sub: "ai_parse_document", icon: ScanText },
  { key: "extracting", label: "Extract clinical facts", sub: "ai_extract", icon: Sparkles },
  { key: "adjudicating", label: "Match medical policies", sub: "Tier-1 rules", icon: Gavel },
  { key: "persisting", label: "Create PA request", sub: "Lakebase write-back", icon: Database },
];

function decisionStyle(decision: string) {
  if (decision === "Auto-Approve")
    return { icon: CheckCircle2, cls: "text-green-700 bg-green-50 border-green-200" };
  if (decision === "Auto-Deny")
    return { icon: XCircle, cls: "text-red-700 bg-red-50 border-red-200" };
  return { icon: AlertTriangle, cls: "text-amber-700 bg-amber-50 border-amber-200" };
}

export function DocumentIntake({ onSelectRequest }: { onSelectRequest?: (id: string) => void }) {
  const [scenarios, setScenarios] = useState<SampleScenario[]>([]);
  const [running, setRunning] = useState(false);
  const [activeStage, setActiveStage] = useState<string>("");
  const [completedStages, setCompletedStages] = useState<Set<string>>(new Set());
  const [statusMsg, setStatusMsg] = useState("");
  const [parsedText, setParsedText] = useState("");
  const [facts, setFacts] = useState<Record<string, unknown> | null>(null);
  const [decision, setDecision] = useState<DecisionState | null>(null);
  const [persistedId, setPersistedId] = useState("");
  const [errorMsg, setErrorMsg] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    api.listSampleScenarios().then((r) => setScenarios(r.scenarios)).catch(() => {});
  }, []);

  const reset = () => {
    setActiveStage(""); setCompletedStages(new Set()); setStatusMsg("");
    setParsedText(""); setFacts(null); setDecision(null); setPersistedId(""); setErrorMsg("");
  };

  const runPipeline = async (handle: DocumentHandle) => {
    const done = new Set<string>();
    const markDone = (stage: string) => { done.add(stage); setCompletedStages(new Set(done)); };

    await api.adjudicateStream(handle, (ev: AdjudicationEvent) => {
      switch (ev.type) {
        case "status":
          // Completing a step means the previous active stage is done.
          setActiveStage((prev) => { if (prev) markDone(prev); return ev.stage; });
          setStatusMsg(ev.message);
          break;
        case "parsed":
          markDone("parsing"); setParsedText(ev.text);
          break;
        case "extracted":
          markDone("extracting"); setFacts(ev.facts);
          break;
        case "decision":
          markDone("adjudicating"); setDecision(ev as unknown as DecisionState);
          break;
        case "persisted":
          markDone("persisting"); setPersistedId(ev.auth_request_id);
          break;
        case "done":
          setActiveStage("");
          break;
        case "error":
          setErrorMsg(ev.message); setActiveStage("");
          break;
      }
    });
  };

  const handleFile = async (file: File) => {
    reset();
    setRunning(true);
    try {
      setStatusMsg("Uploading document to Unity Catalog volume…");
      const handle = await api.uploadDocument(file);
      await runPipeline(handle);
    } catch (e) {
      setErrorMsg(String(e));
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <FileUp className="text-databricks-red" /> Document Intake & Auto-Adjudication
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Upload a medical record. It is parsed with <code>ai_parse_document</code>, structured
          with <code>ai_extract</code>, and checked against medical policies in real time to
          determine if it can be auto-approved.
        </p>
      </div>

      {/* Upload + sample download */}
      <div className="card p-6">
        <div className="flex flex-wrap items-center gap-4">
          <button
            onClick={() => fileRef.current?.click()}
            disabled={running}
            className="btn-primary flex items-center gap-2"
          >
            {running ? <Loader2 className="w-4 h-4 animate-spin" /> : <FileUp className="w-4 h-4" />}
            Upload medical record
          </button>
          <input
            ref={fileRef}
            type="file"
            accept="application/pdf,image/*"
            className="hidden"
            onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFile(f); e.target.value = ""; }}
          />
          <span className="text-sm text-gray-400">or download a sample record to try:</span>
          <div className="flex flex-wrap gap-2">
            {scenarios.map((s) => (
              <a
                key={s.scenario}
                href={api.sampleDownloadUrl(s.scenario)}
                className="inline-flex items-center gap-1 text-xs px-2.5 py-1.5 rounded-md border border-gray-200 text-gray-600 hover:border-databricks-red hover:text-databricks-red transition-colors"
                title={s.procedure}
              >
                <Download className="w-3 h-3" /> {s.title}
              </a>
            ))}
          </div>
        </div>
        <p className="text-xs text-gray-400 mt-3">
          Samples are synthetic (no real PHI). Some deliberately have missing or non-covered data
          to demonstrate the "needs review" and "deny" paths.
        </p>
      </div>

      {errorMsg && (
        <div className="card p-4 border-red-200 bg-red-50 text-sm text-red-700 flex items-center gap-2">
          <XCircle className="w-4 h-4" /> {errorMsg}
        </div>
      )}

      {/* Live pipeline steps */}
      {(running || completedStages.size > 0 || decision) && (
        <div className="card p-6">
          <div className="flex items-center gap-2 mb-4 text-sm font-medium text-databricks-dark">
            {running && <Loader2 className="w-4 h-4 animate-spin text-databricks-red" />}
            {statusMsg || "Processing…"}
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-4 gap-3">
            {STEPS.map((step) => {
              const Icon = step.icon;
              const isDone = completedStages.has(step.key);
              const isActive = activeStage === step.key;
              return (
                <div
                  key={step.key}
                  className={`p-3 rounded-lg border text-center transition-colors ${
                    isDone ? "bg-green-50 border-green-200"
                    : isActive ? "bg-blue-50 border-blue-300"
                    : "bg-gray-50 border-gray-200"
                  }`}
                >
                  <div className="flex items-center justify-center mb-1">
                    {isDone ? <CheckCircle2 className="w-5 h-5 text-green-600" />
                     : isActive ? <Loader2 className="w-5 h-5 text-blue-600 animate-spin" />
                     : <Icon className="w-5 h-5 text-gray-400" />}
                  </div>
                  <p className="text-xs font-semibold text-gray-700">{step.label}</p>
                  <p className="text-[10px] text-gray-400 font-mono">{step.sub}</p>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Extracted facts + parsed text */}
      {(parsedText || facts) && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {parsedText && (
            <div className="card p-4">
              <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                <FileText className="w-4 h-4 text-gray-400" /> Parsed document text
              </h3>
              <pre className="text-xs text-gray-600 whitespace-pre-wrap max-h-48 overflow-y-auto bg-gray-50 rounded p-2">
                {parsedText}
              </pre>
            </div>
          )}
          {facts && (
            <div className="card p-4">
              <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                <Sparkles className="w-4 h-4 text-databricks-red" /> Extracted clinical facts
              </h3>
              <div className="space-y-1 text-xs">
                {Object.entries(facts).map(([k, v]) => (
                  <div key={k} className="flex gap-2">
                    <span className="text-gray-400 font-mono min-w-[130px]">{k}</span>
                    <span className="text-gray-700">{String(v)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Decision */}
      {decision && (() => {
        const { icon: DIcon, cls } = decisionStyle(decision.decision);
        return (
          <div className={`card p-6 border ${cls}`}>
            <div className="flex items-center gap-3 mb-3">
              <DIcon className="w-7 h-7" />
              <div>
                <h3 className="text-lg font-bold">{decision.decision}</h3>
                <p className="text-xs opacity-70">
                  Confidence {(decision.confidence * 100).toFixed(0)}% · Tier-1 deterministic rules
                </p>
              </div>
            </div>
            <ul className="space-y-1 text-sm">
              {decision.reasons.map((r, i) => (
                <li key={i} className="flex gap-2"><span>•</span><span>{r}</span></li>
              ))}
            </ul>
            {decision.matched_policy && (
              <p className="text-xs mt-3 opacity-80">
                Matched policy: <strong>{String((decision.matched_policy as Record<string, unknown>).policy_id)}</strong>
                {" — "}{String((decision.matched_policy as Record<string, unknown>).policy_name)}
              </p>
            )}
            {persistedId && (
              <div className="mt-4 pt-3 border-t border-current/10 flex items-center gap-2 text-sm">
                <Database className="w-4 h-4" />
                Created PA request <strong>{persistedId}</strong> in the review queue.
                {onSelectRequest && (
                  <button
                    onClick={() => onSelectRequest(persistedId)}
                    className="ml-2 underline font-medium"
                  >
                    Open request →
                  </button>
                )}
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
}
