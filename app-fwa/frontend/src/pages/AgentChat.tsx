import { useState, useRef, useEffect } from "react";
import { Bot, Send, Loader2, Sparkles, ChevronDown, ChevronUp, Shield, Network, Database, Brain, FileText, CheckCircle2 } from "lucide-react";
import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";
import { api } from "@/lib/api";
import type { PolicyChunk } from "@/lib/api";

const mdComponents: Components = {
  h2: ({ children }) => (
    <h2 className="text-base font-bold text-databricks-dark mt-6 mb-2 pb-1 border-b border-gray-200 first:mt-0">
      {children}
    </h2>
  ),
  h3: ({ children }) => (
    <h3 className="text-sm font-semibold text-databricks-dark mt-4 mb-1">{children}</h3>
  ),
  p: ({ children }) => <p className="my-2 leading-relaxed">{children}</p>,
  ol: ({ children }) => <ol className="list-decimal pl-5 my-2 space-y-1">{children}</ol>,
  ul: ({ children }) => <ul className="list-disc pl-5 my-2 space-y-1">{children}</ul>,
  li: ({ children }) => <li className="leading-relaxed">{children}</li>,
  strong: ({ children }) => <strong className="font-semibold text-gray-900">{children}</strong>,
  code: ({ children }) => (
    <code className="bg-gray-100 text-databricks-red px-1 py-0.5 rounded text-xs font-mono">{children}</code>
  ),
};

const SUGGESTED = [
  "[INV-0001] Give me a full investigation briefing",
  "[INV-0005] What evidence supports upcoding for this provider?",
  "Which providers in orthopedics have the highest fraud scores?",
  "[INV-0010] What are the recommended next steps?",
  "Compare rules-based flags vs ML model scores for critical investigations",
  "Show me providers with both high ML fraud probability and multiple fraud types",
];

interface ChatEntry {
  question: string;
  answer: string;
  sources: Record<string, unknown>[];
  modelUsed?: string;
  policyChunks?: PolicyChunk[];
}

function FwaClassificationBadge({ answer }: { answer: string }) {
  const upper = answer.toUpperCase();
  if (upper.includes('"FRAUD"') || upper.includes("CLASSIFICATION: FRAUD") || /\*\*FRAUD\*\*/.test(answer))
    return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-100 text-red-800">Fraud</span>;
  if (upper.includes('"WASTE"') || upper.includes("CLASSIFICATION: WASTE") || /\*\*WASTE\*\*/.test(answer))
    return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-amber-100 text-amber-800">Waste</span>;
  if (upper.includes('"ABUSE"') || upper.includes("CLASSIFICATION: ABUSE") || /\*\*ABUSE\*\*/.test(answer))
    return <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-orange-100 text-orange-800">Abuse</span>;
  return null;
}

function PolicyContextPanel({ answer, policyChunks }: { answer: string; policyChunks?: PolicyChunk[] }) {
  const [expanded, setExpanded] = useState(false);
  const hasPolicySection = answer.toUpperCase().includes("POLICY COMPLIANCE");
  const hasChunks = policyChunks && policyChunks.length > 0;
  if (!hasPolicySection && !hasChunks) return null;

  return (
    <div className="mt-3 border border-blue-200 rounded-lg bg-blue-50">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-4 py-2 text-sm font-medium text-blue-800"
      >
        <span className="flex items-center gap-2">
          <Shield className="w-4 h-4" />
          Retrieved Policy Context
          <FwaClassificationBadge answer={answer} />
          {hasChunks && (
            <span className="text-xs font-normal text-blue-600">
              ({policyChunks.length} {policyChunks.length === 1 ? "chunk" : "chunks"} from Vector Search)
            </span>
          )}
        </span>
        {expanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
      </button>
      {expanded && (
        <div className="px-4 pb-3 space-y-3">
          {hasChunks ? (
            policyChunks.map((chunk, i) => (
              <div key={chunk.chunk_id || i} className="bg-white rounded-md border border-blue-100 p-3">
                <div className="flex items-center gap-2 mb-1">
                  <FileText className="w-3.5 h-3.5 text-blue-600" />
                  <span className="text-xs font-semibold text-blue-900">{chunk.policy_name}</span>
                  {chunk.service_category && (
                    <span className="text-xs px-1.5 py-0.5 rounded bg-blue-100 text-blue-700">
                      {chunk.service_category}
                    </span>
                  )}
                </div>
                <p className="text-xs text-gray-700 whitespace-pre-wrap leading-relaxed">
                  {chunk.chunk_text.length > 600
                    ? chunk.chunk_text.slice(0, 600) + "..."
                    : chunk.chunk_text}
                </p>
                <div className="mt-1 text-[10px] text-gray-400 font-mono">{chunk.chunk_id}</div>
              </div>
            ))
          ) : (
            <p className="text-xs text-blue-700 italic">
              No raw policy chunks available — the agent's policy analysis is embedded in the response above.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

function AgentArchitectureBadge({ sources }: { sources: Record<string, unknown>[] }) {
  const src = sources[0];
  if (!src || src.type !== "supervisor_agent") return null;

  const genieQuestions = (src.genie_questions as number) || 0;
  const geminiTables = (src.gemini_tables_queried as number) || 0;
  const geminiTools = (src.gemini_tools_used as string[]) || [];

  return (
    <div className="mb-3 flex flex-wrap gap-2">
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
        <Network className="w-3 h-3" />
        Supervisor (Llama 4 Maverick)
      </span>
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium bg-green-50 text-green-700 border border-green-200">
        <Database className="w-3 h-3" />
        Genie ({genieQuestions} {genieQuestions === 1 ? "query" : "queries"})
      </span>
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded-md text-xs font-medium bg-blue-50 text-blue-700 border border-blue-200">
        <Brain className="w-3 h-3" />
        Gemini Analyst ({geminiTables} tables, {geminiTools.length} tools)
      </span>
    </div>
  );
}

export function AgentChat() {
  const [question, setQuestion] = useState("");
  const [history, setHistory] = useState<ChatEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [statusMsg, setStatusMsg] = useState<string>("");
  const [geniePending, setGeniePending] = useState(true);
  const [geminiPending, setGeminiPending] = useState(true);
  const [earlyGemini, setEarlyGemini] = useState<{ analysis: string; policyChunks?: PolicyChunk[] } | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  const handleAsk = async (q?: string) => {
    const text = q || question;
    if (!text.trim()) return;

    setLoading(true);
    setStatusMsg("Routing to Genie + Gemini sub-agents in parallel…");
    setGeniePending(true);
    setGeminiPending(true);
    setEarlyGemini(null);
    setQuestion("");

    let targetId: string | undefined;
    let targetType: string | undefined;
    let cleanQuestion = text;

    if (text.startsWith("[") && text.includes("]")) {
      const bracketEnd = text.indexOf("]");
      const prefix = text.substring(1, bracketEnd).trim();
      cleanQuestion = text.substring(bracketEnd + 1).trim();

      if (prefix.startsWith("INV-")) {
        targetId = prefix;
        targetType = "investigation";
      } else if (/^\d{10}$/.test(prefix) || prefix.startsWith("PRV-")) {
        targetId = prefix.startsWith("PRV-") ? prefix.substring(4) : prefix;
        targetType = "provider";
      }
    } else {
      const invMatch = text.match(/^(INV-\d+)\s+(.*)/i);
      const npiMatch = text.match(/^(\d{10})\s+(.*)/);
      if (invMatch) {
        targetId = invMatch[1].toUpperCase();
        targetType = "investigation";
        cleanQuestion = invMatch[2];
      } else if (npiMatch) {
        targetId = npiMatch[1];
        targetType = "provider";
        cleanQuestion = npiMatch[2];
      }
    }

    try {
      await api.queryAgentStream(
        cleanQuestion || text,
        targetId,
        targetType,
        (event) => {
          switch (event.type) {
            case "status":
              setStatusMsg(event.message);
              break;
            case "gemini":
              // Gemini finishes well before Genie — surface its clinical
              // analysis immediately so the user isn't staring at a spinner.
              setGeminiPending(false);
              setEarlyGemini({ analysis: event.analysis, policyChunks: event.policy_chunks });
              break;
            case "genie":
              setGeniePending(false);
              break;
            case "final":
              setHistory((prev) => [
                ...prev,
                {
                  question: text,
                  answer: event.answer,
                  sources: event.sources,
                  modelUsed: event.model_used,
                  policyChunks: event.policy_chunks,
                },
              ]);
              setEarlyGemini(null);
              setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
              break;
            case "error":
              setHistory((prev) => [...prev, { question: text, answer: `Error: ${event.message}`, sources: [] }]);
              setEarlyGemini(null);
              break;
          }
        },
      );
    } catch (err) {
      setHistory((prev) => [...prev, { question: text, answer: `Error: ${err}`, sources: [] }]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="mb-4">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Bot className="w-6 h-6 text-databricks-red" /> FWA Investigation Agent
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Supervisor agent routes to <strong>Genie</strong> (structured claims SQL) and{" "}
          <strong>Gemini 2.5 Flash</strong> (medical policy RAG) in parallel, then synthesizes a unified briefing.
        </p>
      </div>

      <div className="flex-1 overflow-y-auto space-y-6 pb-4">
        {history.length === 0 && !loading && (
          <div className="card p-8 text-center">
            <Sparkles className="w-12 h-12 text-databricks-red mx-auto mb-4 opacity-50" />
            <h3 className="text-lg font-semibold text-databricks-dark mb-2">
              Multi-Agent FWA Investigation System
            </h3>
            <p className="text-sm text-gray-500 mb-4">
              Your question is routed to two specialized sub-agents in parallel:
            </p>
            <div className="flex justify-center gap-4 mb-6">
              <div className="text-left p-3 rounded-lg border border-green-200 bg-green-50 max-w-xs">
                <div className="flex items-center gap-2 text-sm font-semibold text-green-800 mb-1">
                  <Database className="w-4 h-4" /> Genie (NL-to-SQL)
                </div>
                <p className="text-xs text-green-700">Queries structured claims data — billing totals, procedure distributions, denial rates</p>
              </div>
              <div className="text-left p-3 rounded-lg border border-blue-200 bg-blue-50 max-w-xs">
                <div className="flex items-center gap-2 text-sm font-semibold text-blue-800 mb-1">
                  <Brain className="w-4 h-4" /> Gemini 2.5 Flash (Tool-Calling)
                </div>
                <p className="text-xs text-blue-700">Searches medical policies via Vector Search, analyzes compliance, classifies Fraud/Waste/Abuse</p>
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 max-w-2xl mx-auto">
              {SUGGESTED.map((q) => (
                <button
                  key={q}
                  onClick={() => handleAsk(q)}
                  className="text-left px-4 py-3 rounded-lg border border-gray-200
                             hover:border-databricks-red hover:bg-red-50 transition-colors
                             text-sm text-gray-600"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {history.map((entry, idx) => (
          <div key={idx} className="space-y-3">
            <div className="flex justify-end">
              <div className="bg-databricks-dark text-white rounded-2xl rounded-tr-sm px-4 py-2.5 max-w-lg text-sm">
                {entry.question}
              </div>
            </div>
            <div className="card p-5">
              <AgentArchitectureBadge sources={entry.sources} />
              {entry.modelUsed && (
                <div className="mb-2 flex items-center gap-2">
                  <FwaClassificationBadge answer={entry.answer} />
                </div>
              )}
              <div className="text-sm text-gray-700">
                <ReactMarkdown components={mdComponents}>{entry.answer}</ReactMarkdown>
              </div>
              <PolicyContextPanel answer={entry.answer} policyChunks={entry.policyChunks} />
            </div>
          </div>
        ))}

        {loading && (
          <div className="card p-6">
            <div className="flex items-center gap-3 mb-3">
              <Loader2 className="w-5 h-5 text-databricks-red animate-spin" />
              <span className="text-sm font-medium text-databricks-dark">{statusMsg || "Supervisor coordinating sub-agents..."}</span>
            </div>
            <div className="flex gap-3">
              <div className={`flex-1 p-2 rounded-md border ${geniePending ? "bg-green-50 border-green-200" : "bg-green-100 border-green-300"}`}>
                <div className="flex items-center gap-1 text-xs text-green-700">
                  <Database className="w-3 h-3" />
                  <span className="font-medium">Genie</span>
                  {geniePending
                    ? <Loader2 className="w-3 h-3 animate-spin ml-auto" />
                    : <CheckCircle2 className="w-3 h-3 ml-auto" />}
                </div>
                <p className="text-xs text-green-600 mt-1">{geniePending ? "Querying claims data..." : "Claims data retrieved ✓"}</p>
              </div>
              <div className={`flex-1 p-2 rounded-md border ${geminiPending ? "bg-blue-50 border-blue-200" : "bg-blue-100 border-blue-300"}`}>
                <div className="flex items-center gap-1 text-xs text-blue-700">
                  <Brain className="w-3 h-3" />
                  <span className="font-medium">Gemini Analyst</span>
                  {geminiPending
                    ? <Loader2 className="w-3 h-3 animate-spin ml-auto" />
                    : <CheckCircle2 className="w-3 h-3 ml-auto" />}
                </div>
                <p className="text-xs text-blue-600 mt-1">{geminiPending ? "Analyzing policies & compliance..." : "Clinical analysis ready ✓"}</p>
              </div>
            </div>

            {/* Gemini finishes early — show its analysis while Genie is still running. */}
            {earlyGemini && (
              <div className="mt-4 border-t border-gray-100 pt-3">
                <div className="flex items-center gap-2 mb-2 text-xs font-semibold text-blue-800">
                  <Brain className="w-3.5 h-3.5" /> Preliminary clinical analysis (Gemini)
                  <FwaClassificationBadge answer={earlyGemini.analysis} />
                </div>
                <div className="text-sm text-gray-700 opacity-90">
                  <ReactMarkdown components={mdComponents}>{earlyGemini.analysis}</ReactMarkdown>
                </div>
                <PolicyContextPanel answer={earlyGemini.analysis} policyChunks={earlyGemini.policyChunks} />
                <p className="text-xs text-gray-400 mt-2 italic">Finalizing with structured claims data…</p>
              </div>
            )}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      <div className="border-t border-gray-200 pt-4">
        <div className="flex gap-3">
          <div className="flex-1 relative">
            <Bot className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleAsk()}
              placeholder="[INV-0001] Give me a full briefing..."
              className="w-full pl-10 pr-4 py-3 rounded-xl border border-gray-300 text-sm
                         focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
              disabled={loading}
            />
          </div>
          <button
            onClick={() => handleAsk()}
            disabled={!question.trim() || loading}
            className="btn-primary flex items-center gap-2 px-6"
          >
            <Send className="w-4 h-4" /> Ask
          </button>
        </div>
        {history.length > 0 && (
          <button
            onClick={() => setHistory([])}
            className="text-xs text-gray-400 hover:text-databricks-red mt-2 transition-colors"
          >
            Clear conversation
          </button>
        )}
      </div>
    </div>
  );
}
