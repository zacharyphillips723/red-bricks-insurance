import { useState } from "react";
import { Sparkles, Send, RefreshCw, Database } from "lucide-react";
import { api, GenieResponse } from "@/lib/api";

const EXAMPLES = [
  "Which groups have the highest loss ratios?",
  "Show total premium and member count by industry",
  "What is the average renewal rate change across all groups?",
  "List the top 10 groups by claims PMPM",
];

export function GenieSearch() {
  const [question, setQuestion] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<GenieResponse | null>(null);
  const [error, setError] = useState("");
  const [conversationId, setConversationId] = useState<string | undefined>(undefined);

  const ask = async (q?: string) => {
    const query = (q || question).trim();
    if (!query || loading) return;
    setLoading(true);
    setError("");
    try {
      const res = await api.askGenie(query, conversationId);
      setResult(res);
      setConversationId(res.conversation_id || undefined);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const cols = result?.columns ?? [];

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Sparkles className="w-6 h-6 text-databricks-red" /> Genie — Natural Language SQL
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Ask questions in plain English about the group book of business. Genie generates and runs
          the SQL against Unity Catalog gold tables.
        </p>
      </div>

      <div className="card p-4">
        <div className="flex gap-3">
          <input
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") ask(); }}
            placeholder="Ask about groups, premiums, loss ratios, renewals…"
            className="flex-1 px-4 py-2.5 border border-gray-300 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
          />
          <button onClick={() => ask()} disabled={!question.trim() || loading} className="btn-primary px-4">
            {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          </button>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          {EXAMPLES.map((q, i) => (
            <button key={i} onClick={() => { setQuestion(q); ask(q); }}
              className="text-xs px-2.5 py-1.5 rounded-md border border-gray-200 text-gray-600 hover:border-databricks-red hover:text-databricks-red transition-colors">
              {q}
            </button>
          ))}
        </div>
      </div>

      {error && <div className="card p-4 border-red-200 bg-red-50 text-sm text-red-700">{error}</div>}
      {loading && (
        <div className="card p-6 flex items-center gap-2 text-sm text-gray-500">
          <RefreshCw className="w-4 h-4 animate-spin text-databricks-red" /> Genie is generating SQL and querying…
        </div>
      )}

      {result && !loading && (
        <div className="space-y-4">
          {result.description && <div className="card p-4 text-sm text-gray-700">{result.description}</div>}
          {result.sql_query && (
            <div className="card">
              <div className="px-4 py-2 border-b border-gray-200 flex items-center gap-2 text-xs font-semibold text-gray-600">
                <Database className="w-3.5 h-3.5" /> Generated SQL
              </div>
              <pre className="p-4 text-xs text-gray-700 overflow-x-auto whitespace-pre-wrap">{result.sql_query}</pre>
            </div>
          )}
          {cols.length > 0 && (
            <div className="card overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50"><tr>
                  {cols.map((c, i) => <th key={i} className="px-4 py-3 text-left font-medium text-gray-600">{c}</th>)}
                </tr></thead>
                <tbody className="divide-y divide-gray-100">
                  {result.rows.slice(0, 100).map((row, i) => (
                    <tr key={i}>{cols.map((c, j) => <td key={j} className="px-4 py-2.5 text-gray-700">{String(row[c] ?? "")}</td>)}</tr>
                  ))}
                </tbody>
              </table>
              {result.rows.length === 0 && <div className="px-4 py-8 text-center text-gray-400 text-sm">No rows returned.</div>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
