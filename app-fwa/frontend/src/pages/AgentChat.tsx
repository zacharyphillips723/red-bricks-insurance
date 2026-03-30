import { useState, useRef } from "react";
import { Bot, Send, Loader2, Sparkles } from "lucide-react";
import { api } from "@/lib/api";

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
}

export function AgentChat() {
  const [question, setQuestion] = useState("");
  const [history, setHistory] = useState<ChatEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  const handleAsk = async (q?: string) => {
    const text = q || question;
    if (!text.trim()) return;

    setLoading(true);
    setQuestion("");

    // Parse target from [XXX-YYYY] prefix if present
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
      // Also detect bare INV-XXXX or 10-digit NPI at start of input (without brackets)
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
      const result = await api.queryAgent(cleanQuestion || text, targetId, targetType);
      setHistory((prev) => [...prev, { question: text, answer: result.answer, sources: result.sources }]);
      setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
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
          Ask questions about investigations, providers, or fraud patterns.
          Use [INV-XXXX] or [PRV-NPI] prefix to target specific entities.
        </p>
      </div>

      <div className="flex-1 overflow-y-auto space-y-6 pb-4">
        {history.length === 0 && !loading && (
          <div className="card p-8 text-center">
            <Sparkles className="w-12 h-12 text-databricks-red mx-auto mb-4 opacity-50" />
            <h3 className="text-lg font-semibold text-databricks-dark mb-2">
              AI-powered FWA investigation assistant
            </h3>
            <p className="text-sm text-gray-500 mb-6">
              The agent dynamically queries Unity Catalog tables, retrieves ML model predictions,
              and synthesizes structured investigation briefings.
            </p>
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
              <div className="prose prose-sm max-w-none text-gray-700 whitespace-pre-wrap">
                {entry.answer}
              </div>
              {entry.sources.length > 0 && (
                <div className="mt-3 pt-3 border-t border-gray-100 text-xs text-gray-400">
                  Sources: {entry.sources.map((s) => JSON.stringify(s)).join(", ")}
                </div>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="card p-6 flex items-center gap-3">
            <Loader2 className="w-5 h-5 text-databricks-red animate-spin" />
            <span className="text-sm text-gray-500">Agent is querying data and generating analysis...</span>
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
