import { useState, useRef, useEffect } from "react";
import { api, AgentResponse } from "@/lib/api";
import { Bot, Send, Trash2 } from "lucide-react";
import ReactMarkdown from "react-markdown";

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

const SUGGESTED_QUESTIONS = [
  "What is the current approval rate for expedited PA requests?",
  "Which service types have the highest denial rates?",
  "Show me provider patterns for high-volume PA requestors.",
  "What percentage of PA requests are auto-adjudicated by Tier 1 rules?",
  "Compare CMS compliance rates across standard vs expedited requests.",
  "Which medical policies generate the most PA denials?",
];

export function AgentChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [_lastResponse, setLastResponse] = useState<AgentResponse | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const handleSend = async (question?: string) => {
    const q = question || input;
    if (!q.trim() || loading) return;

    setMessages((prev) => [...prev, { role: "user", content: q }]);
    setInput("");
    setLoading(true);

    try {
      // Extract PA request ID from question if present (e.g. "PA-2025-005424")
      const paMatch = q.match(/PA-\d{4}-\d{5,}/i);
      const resp = await api.queryAgent(q, paMatch?.[0]);
      setLastResponse(resp);
      setMessages((prev) => [...prev, { role: "assistant", content: resp.answer }]);
    } catch (e) {
      setMessages((prev) => [...prev, { role: "assistant", content: `Error: ${e}` }]);
    } finally {
      setLoading(false);
    }
  };

  const handleClear = () => {
    setMessages([]);
    setLastResponse(null);
  };

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Bot size={24} /> PA Review Agent
        </h2>
        {messages.length > 0 && (
          <button onClick={handleClear} className="btn-secondary text-sm flex items-center gap-1">
            <Trash2 size={14} /> Clear
          </button>
        )}
      </div>

      {/* Chat area */}
      <div className="flex-1 overflow-y-auto card p-4 space-y-4 mb-4">
        {messages.length === 0 && (
          <div className="text-center py-12">
            <Bot size={48} className="mx-auto text-gray-300 mb-4" />
            <h3 className="text-lg font-medium text-gray-600 mb-2">Ask the PA Review Agent</h3>
            <p className="text-sm text-gray-400 mb-6">
              I can query PA data, medical policies, ML predictions, and clinical records.
            </p>
            <div className="grid grid-cols-2 gap-2 max-w-xl mx-auto">
              {SUGGESTED_QUESTIONS.map((q, i) => (
                <button
                  key={i}
                  onClick={() => handleSend(q)}
                  className="text-left text-xs bg-gray-50 hover:bg-gray-100 text-gray-600 p-3 rounded-md transition-colors border border-gray-200"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((m, i) => (
          <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
            <div className={`max-w-[80%] rounded-lg px-4 py-3 text-sm ${
              m.role === "user"
                ? "bg-databricks-dark text-white"
                : "bg-gray-50 text-gray-800"
            }`}>
              {m.role === "assistant" ? (
                <div className="prose prose-sm max-w-none">
                  <ReactMarkdown
                    components={{
                      h1: ({ children }) => <h3 className="font-semibold text-databricks-dark text-base mt-4 mb-2">{children}</h3>,
                      h2: ({ children }) => <h3 className="font-semibold text-databricks-dark text-base mt-4 mb-2">{children}</h3>,
                      h3: ({ children }) => <h4 className="font-semibold text-databricks-dark text-sm mt-3 mb-1">{children}</h4>,
                      p: ({ children }) => <p className="text-sm text-gray-700 leading-relaxed mb-2">{children}</p>,
                      ul: ({ children }) => <ul className="text-sm text-gray-700 list-disc ml-4 mb-2 space-y-1">{children}</ul>,
                      li: ({ children }) => <li className="leading-relaxed">{children}</li>,
                      strong: ({ children }) => <span className="font-semibold text-gray-900">{children}</span>,
                    }}
                  >{m.content}</ReactMarkdown>
                </div>
              ) : (
                m.content
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex justify-start">
            <div className="bg-gray-50 rounded-lg px-4 py-3">
              <div className="flex items-center gap-2 text-sm text-gray-500">
                <div className="animate-spin h-4 w-4 border-2 border-databricks-red border-t-transparent rounded-full" />
                Querying PA data and analyzing...
              </div>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="flex gap-2">
        <input
          type="text"
          placeholder="Ask about PA requests, policies, or clinical evidence..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              handleSend();
            }
          }}
          className="flex-1 border border-gray-300 rounded-md px-4 py-2.5 text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/20"
        />
        <button onClick={() => handleSend()} disabled={loading || !input.trim()} className="btn-primary flex items-center gap-2">
          <Send size={16} /> Send
        </button>
      </div>
    </div>
  );
}
