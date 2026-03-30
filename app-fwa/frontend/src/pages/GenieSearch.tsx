import { useState, useRef } from "react";
import { Search, Sparkles, Code2, Table2, Send, Loader2 } from "lucide-react";
import { api, type GenieResponse } from "@/lib/api";

const SUGGESTED_QUESTIONS = [
  "Which providers have the highest estimated overpayment?",
  "Show me investigations with Critical severity that are still Open",
  "What are the most common fraud types by signal count?",
  "Compare ML model fraud predictions vs rules-based flags by provider",
  "Which members have the highest doctor shopping scores?",
  "Show total estimated overpayment by line of business",
];

export function GenieSearch() {
  const [question, setQuestion] = useState("");
  const [conversationId, setConversationId] = useState<string | undefined>();
  const [responses, setResponses] = useState<
    { question: string; response: GenieResponse }[]
  >([]);
  const [loading, setLoading] = useState(false);
  const [showSql, setShowSql] = useState<string | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  const handleAsk = async (q?: string) => {
    const text = q || question;
    if (!text.trim()) return;

    setLoading(true);
    setQuestion("");
    try {
      const response = await api.askGenie(text, conversationId);
      setConversationId(response.conversation_id);
      setResponses((prev) => [...prev, { question: text, response }]);
      setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
    } catch (err) {
      console.error("Genie error:", err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="mb-4">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Sparkles className="w-6 h-6 text-databricks-red" /> FWA Data Search
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Ask natural language questions about FWA data using Databricks Genie
        </p>
      </div>

      <div className="flex-1 overflow-y-auto space-y-6 pb-4">
        {responses.length === 0 && !loading && (
          <div className="card p-8 text-center">
            <Sparkles className="w-12 h-12 text-databricks-red mx-auto mb-4 opacity-50" />
            <h3 className="text-lg font-semibold text-databricks-dark mb-2">
              Ask anything about FWA data
            </h3>
            <p className="text-sm text-gray-500 mb-6">
              Genie translates your questions into SQL and queries Unity Catalog gold tables
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 max-w-2xl mx-auto">
              {SUGGESTED_QUESTIONS.map((q) => (
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

        {responses.map((entry, idx) => (
          <div key={idx} className="space-y-3">
            <div className="flex justify-end">
              <div className="bg-databricks-dark text-white rounded-2xl rounded-tr-sm px-4 py-2.5 max-w-lg text-sm">
                {entry.question}
              </div>
            </div>

            <div className="card p-5">
              {entry.response.description && (
                <p className="text-sm text-gray-700 mb-4">{entry.response.description}</p>
              )}

              {entry.response.row_count > 0 && (
                <div className="overflow-x-auto rounded-lg border border-gray-200 mb-3">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-gray-50 border-b">
                        {entry.response.columns.map((col) => (
                          <th key={col} className="text-left py-2 px-3 font-medium text-gray-500 text-xs uppercase tracking-wide">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {entry.response.rows.slice(0, 20).map((row, rowIdx) => (
                        <tr key={rowIdx} className="hover:bg-gray-50">
                          {entry.response.columns.map((col) => (
                            <td key={col} className="py-2 px-3 text-gray-700">
                              {row[col] ?? "\u2014"}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {entry.response.row_count > 20 && (
                    <div className="px-3 py-2 bg-gray-50 text-xs text-gray-500 border-t">
                      Showing 20 of {entry.response.row_count} rows
                    </div>
                  )}
                </div>
              )}

              <div className="flex items-center gap-4 text-xs text-gray-400">
                <span className="flex items-center gap-1">
                  <Table2 className="w-3.5 h-3.5" /> {entry.response.row_count} rows
                </span>
                {entry.response.sql_query && (
                  <button
                    onClick={() =>
                      setShowSql(showSql === entry.response.message_id ? null : entry.response.message_id)
                    }
                    className="flex items-center gap-1 hover:text-databricks-red transition-colors"
                  >
                    <Code2 className="w-3.5 h-3.5" /> View SQL
                  </button>
                )}
              </div>

              {showSql === entry.response.message_id && entry.response.sql_query && (
                <pre className="mt-3 bg-databricks-dark text-green-400 rounded-lg p-4 text-xs overflow-x-auto">
                  {entry.response.sql_query}
                </pre>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="card p-6 flex items-center gap-3">
            <Loader2 className="w-5 h-5 text-databricks-red animate-spin" />
            <span className="text-sm text-gray-500">Genie is thinking...</span>
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      <div className="border-t border-gray-200 pt-4">
        <div className="flex gap-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleAsk()}
              placeholder="Ask about providers, fraud patterns, overpayment..."
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
        {conversationId && (
          <button
            onClick={() => { setConversationId(undefined); setResponses([]); }}
            className="text-xs text-gray-400 hover:text-databricks-red mt-2 transition-colors"
          >
            Start new conversation
          </button>
        )}
      </div>
    </div>
  );
}
