import { useState, useRef, useEffect } from "react";
import { Bot, Send, User, RefreshCw, TrendingUp } from "lucide-react";
import ReactMarkdown from "react-markdown";
import { api, SimulationResult } from "@/lib/api";
import {
  formatCurrency,
  formatPercent,
  deltaColor,
  deltaArrow,
  SIMULATION_TYPE_LABELS,
} from "@/lib/utils";

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  simResults?: SimulationResult[];
}

export default function Agent() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSend = async () => {
    const msg = input.trim();
    if (!msg || loading) return;
    setInput("");

    const userMsg: ChatMessage = { role: "user", content: msg };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      // Build conversation history for context
      const history = messages.map((m) => ({
        role: m.role,
        content: m.content,
      }));

      const result = await api.chatAgent(msg, history);

      const assistantMsg: ChatMessage = {
        role: "assistant",
        content: result.response,
        simResults: result.simulation_results || undefined,
      };
      setMessages((prev) => [...prev, assistantMsg]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Error: ${err}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex flex-col h-screen">
      {/* Header */}
      <div className="px-8 py-4 border-b border-gray-200 bg-white">
        <h1 className="text-xl font-bold text-databricks-dark flex items-center gap-2">
          <Bot className="w-5 h-5 text-databricks-red" />
          Underwriting Agent
        </h1>
        <p className="text-xs text-gray-500 mt-0.5">
          Ask questions, run simulations, and get actuarial analysis
        </p>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-8 py-6 space-y-6">
        {messages.length === 0 && (
          <div className="text-center py-20">
            <Bot className="w-12 h-12 text-gray-300 mx-auto mb-4" />
            <h2 className="text-lg font-semibold text-gray-400">
              How can I help with underwriting today?
            </h2>
            <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-2 max-w-xl mx-auto">
              {EXAMPLE_QUESTIONS.map((q, i) => (
                <button
                  key={i}
                  onClick={() => {
                    setInput(q);
                  }}
                  className="text-left text-sm px-4 py-3 bg-white rounded-lg border border-gray-200
                             hover:border-databricks-red hover:bg-red-50 transition-colors"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex gap-3 ${
              msg.role === "user" ? "justify-end" : ""
            }`}
          >
            {msg.role === "assistant" && (
              <div className="w-8 h-8 rounded-full bg-databricks-dark flex items-center justify-center flex-shrink-0">
                <Bot className="w-4 h-4 text-white" />
              </div>
            )}
            <div
              className={`max-w-[75%] rounded-xl px-4 py-3 ${
                msg.role === "user"
                  ? "bg-databricks-red text-white"
                  : "bg-white border border-gray-200"
              }`}
            >
              {msg.role === "user" ? (
                <p className="text-sm">{msg.content}</p>
              ) : (
                <div className="prose prose-sm max-w-none">
                  <ReactMarkdown>{msg.content}</ReactMarkdown>
                </div>
              )}

              {/* Inline simulation results */}
              {msg.simResults && msg.simResults.length > 0 && (
                <div className="mt-3 space-y-3">
                  {msg.simResults.map((sim, j) => (
                    <SimResultCard key={j} result={sim} />
                  ))}
                </div>
              )}
            </div>
            {msg.role === "user" && (
              <div className="w-8 h-8 rounded-full bg-gray-200 flex items-center justify-center flex-shrink-0">
                <User className="w-4 h-4 text-gray-600" />
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div className="flex gap-3">
            <div className="w-8 h-8 rounded-full bg-databricks-dark flex items-center justify-center flex-shrink-0">
              <Bot className="w-4 h-4 text-white" />
            </div>
            <div className="bg-white border border-gray-200 rounded-xl px-4 py-3">
              <RefreshCw className="w-4 h-4 animate-spin text-gray-400" />
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="px-8 py-4 border-t border-gray-200 bg-white">
        <div className="flex gap-3 max-w-4xl mx-auto">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about premiums, renewals, risk adjustment, or run a simulation..."
            rows={1}
            className="flex-1 px-4 py-2.5 border border-gray-300 rounded-xl text-sm resize-none
                       focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
          />
          <button
            onClick={handleSend}
            disabled={!input.trim() || loading}
            className="btn-primary px-4"
          >
            <Send className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

const EXAMPLE_QUESTIONS = [
  "What happens if we raise Commercial premiums 5%?",
  "Show me the current book-level financials",
  "Run a medical trend analysis at 8% for 12 months",
  "What if we improve RAF coding completeness to 85%?",
];

function SimResultCard({ result }: { result: SimulationResult }) {
  return (
    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
      <div className="flex items-center gap-2 mb-2">
        <TrendingUp className="w-4 h-4 text-databricks-red" />
        <span className="text-xs font-semibold text-databricks-dark">
          {SIMULATION_TYPE_LABELS[result.simulation_type] || result.simulation_type}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-2">
        {Object.keys(result.baseline).slice(0, 4).map((key) => (
          <div key={key} className="text-xs">
            <span className="text-gray-500 capitalize">
              {key.replace(/_/g, " ")}:
            </span>{" "}
            <span className={`font-medium ${deltaColor(result.delta[key])}`}>
              {deltaArrow(result.delta[key])}{" "}
              {formatSimMetric(key, result.projected[key])}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function formatSimMetric(key: string, value: number): string {
  const k = key.toLowerCase();
  if (k.includes("mlr") || k.includes("pct") || k.includes("rate"))
    return formatPercent(value);
  if (k.includes("premium") || k.includes("claims") || k.includes("revenue") || k.includes("cost"))
    return formatCurrency(value);
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
