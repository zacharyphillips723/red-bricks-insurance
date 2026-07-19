import { useState, useRef, useEffect } from "react";
import { Send, Loader2, Sparkles, Slack, Globe, Database, FileText } from "lucide-react";
import ReactMarkdown from "react-markdown";
import type { Components } from "react-markdown";
import { api } from "@/lib/api";

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

const SUGGESTED_QUESTIONS = [
  "Prepare me for the renewal meeting",
  "Why is a rate increase needed for this group?",
  "How does this group compare to its peers?",
  "What care management programs can I offer this group?",
  "Simulate a renewal negotiation — play the benefits director",
  "Quiz me — ask questions like a CFO would",
];

interface ChatPanelProps {
  groupId: string;
}

interface ChatMessage {
  role: "user" | "agent";
  text: string;
  enrichmentSources?: string[];
  planDocSources?: string[];
}

const sourceIcons: Record<string, typeof Slack> = {
  slack: Slack,
  glean: Globe,
  salesforce: Database,
};

export function ChatPanel({ groupId }: ChatPanelProps) {
  const [question, setQuestion] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [loading, setLoading] = useState(false);
  const [statusMsg, setStatusMsg] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setMessages([]);
  }, [groupId]);

  const handleAsk = async (q?: string) => {
    const text = q || question;
    if (!text.trim() || loading) return;

    setMessages((prev) => [...prev, { role: "user", text }]);
    setQuestion("");
    setLoading(true);
    setStatusMsg("Preparing your briefing…");

    try {
      await api.chatWithAgentStream(groupId, text, (ev) => {
        if (ev.type === "status") {
          setStatusMsg(ev.message);
        } else if (ev.type === "final") {
          setMessages((prev) => [
            ...prev,
            { role: "agent", text: ev.answer, enrichmentSources: ev.enrichment_sources, planDocSources: ev.plan_doc_sources },
          ]);
        } else if (ev.type === "error") {
          setMessages((prev) => [...prev, { role: "agent", text: `Error: ${ev.message}` }]);
        }
      });
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: "agent", text: `Error: ${err instanceof Error ? err.message : "Unknown error"}` },
      ]);
    } finally {
      setLoading(false);
      setStatusMsg("");
      setTimeout(() => {
        containerRef.current?.scrollTo({
          top: containerRef.current.scrollHeight,
          behavior: "smooth",
        });
      }, 100);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="px-4 py-3 border-b border-gray-200">
        <h4 className="text-sm font-semibold text-databricks-dark flex items-center gap-2">
          <Sparkles className="w-4 h-4 text-databricks-red" /> Sales Coach
        </h4>
        <p className="text-xs text-gray-400">
          Ask about this group's renewal, financials, or practice your pitch
        </p>
      </div>

      {/* Messages */}
      <div ref={containerRef} className="flex-1 overflow-y-auto p-4 space-y-3">
        {messages.length === 0 && !loading && (
          <div className="space-y-2">
            <p className="text-xs text-gray-400 mb-3">Suggested questions:</p>
            {SUGGESTED_QUESTIONS.map((q) => (
              <button
                key={q}
                onClick={() => handleAsk(q)}
                className="block w-full text-left px-3 py-2 rounded-lg border border-gray-200
                           hover:border-databricks-red hover:bg-red-50 transition-colors text-xs text-gray-600"
              >
                {q}
              </button>
            ))}
          </div>
        )}

        {messages.map((msg, idx) => (
          <div
            key={idx}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div className="max-w-[90%]">
              {msg.role === "user" ? (
                <div className="rounded-2xl rounded-tr-sm px-3 py-2 text-sm bg-databricks-dark text-white">
                  {msg.text}
                </div>
              ) : (
                <div className="rounded-2xl rounded-tl-sm px-3 py-2 text-sm bg-gray-100 text-gray-800">
                  <ReactMarkdown components={mdComponents}>{msg.text}</ReactMarkdown>
                </div>
              )}
              {msg.enrichmentSources && msg.enrichmentSources.length > 0 && (
                <div className="flex items-center gap-1.5 mt-1 ml-1">
                  <span className="text-[10px] text-gray-400">Enriched from:</span>
                  {msg.enrichmentSources.map((src) => {
                    const Icon = sourceIcons[src] || Globe;
                    return (
                      <span
                        key={src}
                        className="inline-flex items-center gap-0.5 text-[10px] text-gray-500 bg-gray-100 rounded px-1.5 py-0.5"
                      >
                        <Icon className="w-2.5 h-2.5" /> {src}
                      </span>
                    );
                  })}
                </div>
              )}
              {msg.planDocSources && msg.planDocSources.length > 0 && (
                <div className="flex items-center flex-wrap gap-1.5 mt-1 ml-1">
                  <span className="text-[10px] text-gray-400">Plan docs:</span>
                  {msg.planDocSources.map((src) => (
                    <span key={src} className="inline-flex items-center gap-0.5 text-[10px] text-databricks-red bg-red-50 rounded px-1.5 py-0.5">
                      <FileText className="w-2.5 h-2.5" /> {src}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex items-center gap-2 text-xs text-gray-400">
            <Loader2 className="w-4 h-4 animate-spin text-databricks-red" />
            {statusMsg || "Coach is preparing your briefing..."}
          </div>
        )}
      </div>

      {/* Input */}
      <div className="p-3 border-t border-gray-200">
        <div className="flex gap-2">
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleAsk()}
            placeholder="Ask about this group..."
            className="flex-1 px-3 py-2 rounded-lg border border-gray-300 text-sm
                       focus:ring-2 focus:ring-databricks-red focus:border-databricks-red"
            disabled={loading}
          />
          <button
            onClick={() => handleAsk()}
            disabled={!question.trim() || loading}
            className="btn-primary px-3 py-2"
          >
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
