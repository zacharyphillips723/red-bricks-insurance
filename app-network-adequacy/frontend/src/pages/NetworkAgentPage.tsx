import { useState, useRef, useEffect } from "react";
import { Bot, Send, Loader2, FlaskConical, TrendingUp, CheckCircle2, XCircle } from "lucide-react";
import ReactMarkdown from "react-markdown";
import { api, WhatIfResult } from "@/lib/api";

interface ChatMessage { role: "user" | "agent"; text: string; }

const SUGGESTED = [
  "Which counties have the worst primary care compliance?",
  "What if we recruit all out-of-network Primary Care providers in Gaston?",
  "Summarize overall network compliance and total gap members",
  "Which specialties have the most ghost-network providers?",
];

export function NetworkAgentPage() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [statusMsg, setStatusMsg] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  // What-if panel state
  const [wcounty, setWcounty] = useState("Gaston");
  const [wspec, setWspec] = useState("Primary Care");
  const [wLoading, setWLoading] = useState(false);
  const [wResult, setWResult] = useState<WhatIfResult | null>(null);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages, statusMsg]);

  const send = async (q?: string) => {
    const text = q || input;
    if (!text.trim() || loading) return;
    setMessages((p) => [...p, { role: "user", text }]);
    setInput("");
    setLoading(true);
    setStatusMsg("Analyzing…");
    const history = messages.map((m) => ({ role: m.role === "agent" ? "assistant" : "user", content: m.text }));
    try {
      await api.chatAgentStream(text, history, (ev) => {
        if (ev.type === "status") setStatusMsg(ev.message);
        else if (ev.type === "final") setMessages((p) => [...p, { role: "agent", text: ev.response }]);
        else if (ev.type === "error") setMessages((p) => [...p, { role: "agent", text: `Error: ${ev.message}` }]);
      });
    } catch (e) {
      setMessages((p) => [...p, { role: "agent", text: `Error: ${e}` }]);
    } finally {
      setLoading(false);
      setStatusMsg("");
    }
  };

  const runWhatIf = async () => {
    if (!wcounty.trim() || !wspec.trim() || wLoading) return;
    setWLoading(true);
    setWResult(null);
    try {
      setWResult(await api.simulateRecruitment(wcounty.trim(), wspec.trim()));
    } catch (e) {
      setWResult({ error: String(e) } as WhatIfResult);
    } finally {
      setWLoading(false);
    }
  };

  return (
    <div className="max-w-6xl mx-auto">
      <h1 className="text-xl font-bold text-databricks-dark flex items-center gap-2 mb-1">
        <Bot className="w-5 h-5 text-databricks-red" /> Network Agent
      </h1>
      <p className="text-sm text-gray-500 mb-6">
        Ask about compliance, ghost networks, leakage, and recruitment — or run a geospatial what-if simulation.
      </p>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Chat */}
        <div className="lg:col-span-2 card flex flex-col h-[600px]">
          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {messages.length === 0 && (
              <div className="space-y-2">
                <p className="text-xs text-gray-400 mb-2">Try asking:</p>
                {SUGGESTED.map((q) => (
                  <button key={q} onClick={() => send(q)}
                    className="block w-full text-left px-3 py-2 rounded-lg border border-gray-200 hover:border-databricks-red hover:bg-red-50 transition-colors text-xs text-gray-600">
                    {q}
                  </button>
                ))}
              </div>
            )}
            {messages.map((m, i) => (
              <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
                <div className={`max-w-[90%] rounded-2xl px-3 py-2 text-sm ${m.role === "user" ? "bg-databricks-dark text-white" : "bg-gray-100 text-gray-800"}`}>
                  {m.role === "user" ? m.text : <div className="prose prose-sm max-w-none"><ReactMarkdown>{m.text}</ReactMarkdown></div>}
                </div>
              </div>
            ))}
            {loading && (
              <div className="flex items-center gap-2 text-xs text-gray-400">
                <Loader2 className="w-4 h-4 animate-spin text-databricks-red" /> {statusMsg || "Thinking…"}
              </div>
            )}
            <div ref={bottomRef} />
          </div>
          <div className="p-3 border-t border-gray-200 flex gap-2">
            <input value={input} onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && send()}
              placeholder="Ask about the network…" disabled={loading}
              className="flex-1 px-3 py-2 rounded-lg border border-gray-300 text-sm focus:ring-2 focus:ring-databricks-red focus:border-databricks-red" />
            <button onClick={() => send()} disabled={!input.trim() || loading} className="btn-primary px-3 py-2">
              <Send className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* What-if panel */}
        <div className="card p-4 h-fit">
          <h3 className="text-sm font-semibold text-databricks-dark flex items-center gap-2 mb-3">
            <FlaskConical className="w-4 h-4 text-databricks-red" /> Network What-If
          </h3>
          <p className="text-xs text-gray-500 mb-3">
            Recompute a county + specialty's compliance if all out-of-network providers are recruited in-network (real geospatial distance recompute).
          </p>
          <label className="block text-xs text-gray-500 mb-1">County</label>
          <input value={wcounty} onChange={(e) => setWcounty(e.target.value)}
            className="w-full mb-2 px-3 py-1.5 rounded-lg border border-gray-300 text-sm" />
          <label className="block text-xs text-gray-500 mb-1">CMS Specialty Type</label>
          <input value={wspec} onChange={(e) => setWspec(e.target.value)}
            className="w-full mb-3 px-3 py-1.5 rounded-lg border border-gray-300 text-sm" />
          <button onClick={runWhatIf} disabled={wLoading} className="btn-primary w-full py-2 text-sm flex items-center justify-center gap-2">
            {wLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <TrendingUp className="w-4 h-4" />} Run Simulation
          </button>

          {wResult && !wResult.error && (
            <div className="mt-4 space-y-2 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-gray-500">Providers recruited</span>
                <span className="font-semibold">{wResult.providers_recruited}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-gray-500">Baseline compliance</span>
                <span className="font-semibold">{wResult.baseline_pct_compliant}%</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-gray-500">Projected compliance</span>
                <span className="font-bold text-databricks-red">{wResult.projected_pct_compliant}%</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-gray-500">Members gained</span>
                <span className={`font-semibold ${wResult.members_gained > 0 ? "text-green-600" : "text-gray-600"}`}>
                  {wResult.members_gained > 0 ? "+" : ""}{wResult.members_gained}
                </span>
              </div>
              <div className={`flex items-center gap-2 mt-2 text-xs font-medium ${wResult.meets_90pct_threshold ? "text-green-700" : "text-amber-700"}`}>
                {wResult.meets_90pct_threshold ? <CheckCircle2 className="w-4 h-4" /> : <XCircle className="w-4 h-4" />}
                {wResult.meets_90pct_threshold ? "Meets CMS 90% threshold" : "Below CMS 90% threshold"}
              </div>
              <p className="text-[11px] text-gray-400 mt-1">
                {wResult.total_members} members · {wResult.max_distance_miles}mi standard
              </p>
            </div>
          )}
          {wResult?.error && <p className="mt-3 text-xs text-red-600">{wResult.error}</p>}
        </div>
      </div>
    </div>
  );
}
