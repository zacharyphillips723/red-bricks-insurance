import { useState, useEffect, useCallback } from "react";
import {
  Network, GaugeCircle, Route, AlertTriangle, ArrowUpCircle, Inbox,
  Loader2, Play, CheckCircle, Power,
} from "lucide-react";
import {
  api, WorkQueue, Bottleneck, WorkloadResponse, RoutingRule,
  StalledResponse, Escalation, InboundCorrespondence,
} from "@/lib/api";

type Tab = "queues" | "workload" | "routing" | "stalled" | "escalations" | "inbound";

const TABS: { id: Tab; label: string; icon: typeof Network }[] = [
  { id: "queues", label: "Queue Monitor", icon: GaugeCircle },
  { id: "workload", label: "Workload Balance", icon: Network },
  { id: "routing", label: "Routing Rules", icon: Route },
  { id: "stalled", label: "Stalled Work", icon: AlertTriangle },
  { id: "escalations", label: "Escalations", icon: ArrowUpCircle },
  { id: "inbound", label: "Inbound Docs", icon: Inbox },
];

export function WorkManagement() {
  const [tab, setTab] = useState<Tab>("queues");
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Network size={22} className="text-databricks-dark" />
        <h2 className="text-2xl font-bold text-databricks-dark">Work Management</h2>
        <span className="text-xs text-gray-400 ml-2">Configurable queues · routing · workload · SLA monitoring</span>
      </div>

      <div className="flex gap-1 border-b border-gray-200">
        {TABS.map((t) => {
          const Icon = t.icon;
          return (
            <button key={t.id} onClick={() => setTab(t.id)}
              className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 -mb-px ${
                tab === t.id ? "border-databricks-red text-databricks-red" : "border-transparent text-gray-500 hover:text-gray-700"
              }`}>
              <Icon size={15} /> {t.label}
            </button>
          );
        })}
      </div>

      {tab === "queues" && <QueueMonitor />}
      {tab === "workload" && <WorkloadBalance />}
      {tab === "routing" && <RoutingRules />}
      {tab === "stalled" && <StalledWork />}
      {tab === "escalations" && <Escalations />}
      {tab === "inbound" && <InboundDocs />}
    </div>
  );
}

function Spinner() {
  return <div className="flex justify-center py-12"><Loader2 className="w-7 h-7 text-databricks-red animate-spin" /></div>;
}

function Kpi({ label, value, tone }: { label: string; value: string | number; tone?: "ok" | "bad" | "warn" }) {
  const c = tone === "bad" ? "text-red-600" : tone === "warn" ? "text-amber-600" : "text-databricks-dark";
  return (
    <div className="card">
      <div className="text-xs text-gray-500 uppercase tracking-wider">{label}</div>
      <div className={`text-2xl font-bold mt-1 ${c}`}>{value}</div>
    </div>
  );
}

// ---------------- Queue Monitor ----------------
function QueueMonitor() {
  const [queues, setQueues] = useState<WorkQueue[]>([]);
  const [bottlenecks, setBottlenecks] = useState<Bottleneck[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.listWorkQueues(), api.getWorkflowBottlenecks()])
      .then(([q, b]) => { setQueues(q); setBottlenecks(b.bottlenecks); })
      .catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) return <Spinner />;

  const totalOpen = queues.reduce((a, q) => a + q.open_cases, 0);
  const totalBreached = queues.reduce((a, q) => a + q.sla_breached, 0);
  const totalUnassigned = queues.reduce((a, q) => a + q.unassigned_cases, 0);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-4 gap-4">
        <Kpi label="Open Cases" value={totalOpen} />
        <Kpi label="SLA Breached" value={totalBreached} tone={totalBreached ? "bad" : "ok"} />
        <Kpi label="Unassigned" value={totalUnassigned} tone={totalUnassigned ? "warn" : "ok"} />
        <Kpi label="Active Queues" value={queues.length} />
      </div>

      {bottlenecks.length > 0 && (
        <div className="card bg-amber-50 border border-amber-200">
          <div className="text-xs font-semibold text-amber-800 uppercase tracking-wider mb-2 flex items-center gap-1">
            <AlertTriangle size={13} /> AI-detected bottlenecks
          </div>
          <div className="space-y-1">
            {bottlenecks.slice(0, 4).map((b) => (
              <div key={b.queue_id} className="text-sm text-amber-900">
                <span className="font-medium">{b.name}</span> — {b.reason}
                <span className="text-amber-600 text-xs ml-2">(score {b.bottleneck_score})</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Queue</th><th className="px-4 py-3">Type</th><th className="px-4 py-3">SLA (h)</th>
            <th className="px-4 py-3">Open</th><th className="px-4 py-3">Aging (0–24 / 24–72 / 72h+)</th>
            <th className="px-4 py-3">Unassigned</th><th className="px-4 py-3">SLA Breach</th><th className="px-4 py-3">Avg Age</th>
          </tr></thead>
          <tbody className="divide-y divide-gray-100">
            {queues.map((q) => (
              <tr key={q.queue_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-medium">{q.name}</td>
                <td className="px-4 py-3 text-gray-500 capitalize">{q.queue_type}</td>
                <td className="px-4 py-3">{q.sla_hours}</td>
                <td className="px-4 py-3 font-semibold">{q.open_cases}</td>
                <td className="px-4 py-3 text-xs">
                  <span className="text-green-700">{q.age_0_24h}</span> /{" "}
                  <span className="text-amber-600">{q.age_24_72h}</span> /{" "}
                  <span className="text-red-600 font-medium">{q.age_72h_plus}</span>
                </td>
                <td className="px-4 py-3">{q.unassigned_cases}</td>
                <td className={`px-4 py-3 font-medium ${q.sla_breached ? "text-red-600" : "text-gray-400"}`}>{q.sla_breached}</td>
                <td className="px-4 py-3 text-gray-500">{q.avg_age_hours != null ? `${q.avg_age_hours}h` : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------- Workload Balance ----------------
function WorkloadBalance() {
  const [data, setData] = useState<WorkloadResponse | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    api.getWorkloadBalance().then(setData).catch(console.error).finally(() => setLoading(false));
  }, []);
  if (loading) return <Spinner />;
  if (!data) return null;
  const rec = data.recommendation;

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-4">
        <Kpi label="Overloaded Reviewers" value={rec.overloaded_count} tone={rec.overloaded_count ? "bad" : "ok"} />
        <Kpi label="Have Capacity" value={rec.underutilized_count} />
        <Kpi label="Rebalanceable Cases" value={rec.rebalanced_cases} tone={rec.rebalanced_cases ? "warn" : "ok"} />
      </div>

      {rec.moves.length > 0 && (
        <div className="card bg-blue-50 border border-blue-200">
          <div className="text-xs font-semibold text-blue-800 uppercase tracking-wider mb-2">
            Recommended rebalancing (target {rec.target_utilization_pct}% utilization)
          </div>
          <div className="space-y-1">
            {rec.moves.map((m, i) => (
              <div key={i} className="text-sm text-blue-900">
                Move <span className="font-semibold">{m.cases}</span> case(s):{" "}
                <span className="font-medium">{m.from_name}</span> → <span className="font-medium">{m.to_name}</span>
                <span className="text-blue-600 text-xs ml-2">{m.reason}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Reviewer</th><th className="px-4 py-3">Role</th><th className="px-4 py-3">Active</th>
            <th className="px-4 py-3">Capacity</th><th className="px-4 py-3">Utilization</th>
          </tr></thead>
          <tbody className="divide-y divide-gray-100">
            {data.workloads.map((w) => (
              <tr key={w.reviewer_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-medium">{w.display_name}</td>
                <td className="px-4 py-3 text-gray-500">{w.role}</td>
                <td className="px-4 py-3">{w.active_cases} / {w.max_caseload}</td>
                <td className="px-4 py-3">{w.available_capacity}</td>
                <td className="px-4 py-3 w-48">
                  <div className="flex items-center gap-2">
                    <div className="flex-1 h-2 bg-gray-100 rounded-full overflow-hidden">
                      <div className={`h-full ${w.is_overloaded ? "bg-red-500" : (w.utilization_pct ?? 0) > 85 ? "bg-amber-500" : "bg-green-500"}`}
                        style={{ width: `${Math.min(w.utilization_pct ?? 0, 100)}%` }} />
                    </div>
                    <span className={`text-xs font-medium ${w.is_overloaded ? "text-red-600" : "text-gray-600"}`}>
                      {w.utilization_pct ?? 0}%
                    </span>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------- Routing Rules ----------------
function RoutingRules() {
  const [rules, setRules] = useState<RoutingRule[]>([]);
  const [queues, setQueues] = useState<WorkQueue[]>([]);
  const [loading, setLoading] = useState(true);
  const [showNew, setShowNew] = useState(false);

  const load = useCallback(() => {
    Promise.all([api.listRoutingRules(), api.listWorkQueues()])
      .then(([r, q]) => { setRules(r); setQueues(q); })
      .catch(console.error).finally(() => setLoading(false));
  }, []);
  useEffect(() => { load(); }, [load]);

  if (loading) return <Spinner />;

  return (
    <div className="space-y-4">
      <div className="flex justify-end">
        <button onClick={() => setShowNew(true)} className="btn-primary text-sm flex items-center gap-1">
          <Play size={14} /> New Routing Rule
        </button>
      </div>
      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Priority</th><th className="px-4 py-3">Rule</th><th className="px-4 py-3">Scope</th>
            <th className="px-4 py-3">Routes To</th><th className="px-4 py-3">Strategy</th><th className="px-4 py-3">Status</th><th className="px-4 py-3"></th>
          </tr></thead>
          <tbody className="divide-y divide-gray-100">
            {rules.map((r) => (
              <tr key={r.routing_rule_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 text-gray-500">{r.priority}</td>
                <td className="px-4 py-3">
                  <div className="font-medium">{r.name}</div>
                  {r.description && <div className="text-xs text-gray-400">{r.description}</div>}
                </td>
                <td className="px-4 py-3 text-xs text-gray-500">
                  {[r.line_of_business, r.service_type].filter(Boolean).join(" · ") || "All"}
                </td>
                <td className="px-4 py-3">{r.target_queue_name || r.target_role || "—"}</td>
                <td className="px-4 py-3 text-xs text-gray-500">{r.assignment_strategy}</td>
                <td className="px-4 py-3">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${r.is_active ? "bg-green-50 text-green-700" : "bg-gray-100 text-gray-500"}`}>
                    {r.is_active ? "Active" : "Inactive"}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <button onClick={() => api.toggleRoutingRule(r.routing_rule_id).then(load).catch((e) => alert((e as Error).message))}
                    className="text-gray-400 hover:text-databricks-red" title="Toggle active">
                    <Power size={15} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {showNew && <NewRoutingRuleModal queues={queues} onClose={() => setShowNew(false)} onCreated={() => { setShowNew(false); load(); }} />}
    </div>
  );
}

function NewRoutingRuleModal({ queues, onClose, onCreated }: { queues: WorkQueue[]; onClose: () => void; onCreated: () => void }) {
  const [name, setName] = useState("");
  const [svc, setSvc] = useState("");
  const [urgency, setUrgency] = useState("");
  const [queueId, setQueueId] = useState(queues[0]?.queue_id || "");
  const [priority, setPriority] = useState(100);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setBusy(true);
    try {
      const conditions = urgency ? { all: [{ field: "urgency", op: "eq", value: urgency }] } : {};
      await api.createRoutingRule({
        name, service_type: svc || undefined, conditions_json: conditions,
        target_queue_id: queueId || undefined, assignment_strategy: "least_loaded", priority,
      });
      onCreated();
    } catch (e) { alert((e as Error).message); } finally { setBusy(false); }
  };

  return (
    <div className="fixed inset-0 bg-black/30 flex justify-end z-50" onClick={onClose}>
      <div className="w-[460px] max-w-full bg-white h-full overflow-y-auto shadow-2xl p-6 space-y-3" onClick={(e) => e.stopPropagation()}>
        <h3 className="text-lg font-bold text-databricks-dark">New Routing Rule</h3>
        <label className="block text-xs text-gray-500 uppercase tracking-wider">Name</label>
        <input value={name} onChange={(e) => setName(e.target.value)} className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm" />
        <label className="block text-xs text-gray-500 uppercase tracking-wider">Service type (blank = all)</label>
        <input value={svc} onChange={(e) => setSvc(e.target.value)} placeholder="e.g. behavioral_health" className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm" />
        <label className="block text-xs text-gray-500 uppercase tracking-wider">Urgency condition (optional)</label>
        <select value={urgency} onChange={(e) => setUrgency(e.target.value)} className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm">
          <option value="">(any)</option><option value="expedited">expedited</option><option value="standard">standard</option>
        </select>
        <label className="block text-xs text-gray-500 uppercase tracking-wider">Route to queue</label>
        <select value={queueId} onChange={(e) => setQueueId(e.target.value)} className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm">
          {queues.map((q) => <option key={q.queue_id} value={q.queue_id}>{q.name}</option>)}
        </select>
        <label className="block text-xs text-gray-500 uppercase tracking-wider">Priority (lower = first)</label>
        <input type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value))} className="w-32 border border-gray-300 rounded-md px-2 py-1.5 text-sm" />
        <button onClick={submit} disabled={busy || !name} className="btn-primary w-full disabled:opacity-40 mt-2">
          {busy ? "Creating…" : "Create Rule"}
        </button>
      </div>
    </div>
  );
}

// ---------------- Stalled Work ----------------
const FLAG_STYLE: Record<string, string> = {
  sla_breached: "bg-red-50 text-red-700",
  orphaned: "bg-purple-50 text-purple-700",
  stalled: "bg-amber-50 text-amber-700",
  at_risk: "bg-blue-50 text-blue-700",
};

function StalledWork() {
  const [data, setData] = useState<StalledResponse | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.getStalledCases().then(setData).catch(console.error).finally(() => setLoading(false)); }, []);
  if (loading) return <Spinner />;
  if (!data) return null;

  const escalate = async (aid: string) => {
    try { await api.createEscalation({ auth_request_id: aid, reason: "stalled", detail: "Escalated from Stalled Work panel" }); alert("Escalated to supervisor."); }
    catch (e) { alert((e as Error).message); }
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-4 gap-4">
        <Kpi label="Total Flagged" value={data.total} tone={data.total ? "warn" : "ok"} />
        <Kpi label="SLA Breached" value={data.by_flag.sla_breached || 0} tone={(data.by_flag.sla_breached || 0) ? "bad" : "ok"} />
        <Kpi label="Orphaned" value={data.by_flag.orphaned || 0} />
        <Kpi label="Stalled" value={data.by_flag.stalled || 0} tone={(data.by_flag.stalled || 0) ? "warn" : "ok"} />
      </div>
      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
            <th className="px-4 py-3">Case</th><th className="px-4 py-3">Flag</th><th className="px-4 py-3">Queue</th>
            <th className="px-4 py-3">Age</th><th className="px-4 py-3">Since Action</th><th className="px-4 py-3">Recommended Action</th><th className="px-4 py-3"></th>
          </tr></thead>
          <tbody className="divide-y divide-gray-100">
            {data.cases.map((c) => (
              <tr key={c.auth_request_id} className="hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-xs">{c.auth_request_id}</td>
                <td className="px-4 py-3"><span className={`px-2 py-0.5 rounded text-xs font-medium ${FLAG_STYLE[c.flag_reason] || "bg-gray-100"}`}>{c.flag_reason}</span></td>
                <td className="px-4 py-3 text-gray-500">{c.queue_name || "—"}</td>
                <td className="px-4 py-3">{c.age_hours != null ? `${Math.round(c.age_hours)}h` : "—"}</td>
                <td className="px-4 py-3">{c.hours_since_action != null ? `${Math.round(c.hours_since_action)}h` : "—"}</td>
                <td className="px-4 py-3 text-xs text-gray-600">{c.recommended_action}</td>
                <td className="px-4 py-3">
                  <button onClick={() => escalate(c.auth_request_id)} className="text-xs text-databricks-red">Escalate</button>
                </td>
              </tr>
            ))}
            {data.cases.length === 0 && (
              <tr><td colSpan={7} className="px-4 py-8 text-center text-gray-400">No stalled or at-risk work. 🎉</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------- Escalations ----------------
function Escalations() {
  const [items, setItems] = useState<Escalation[]>([]);
  const [loading, setLoading] = useState(true);
  const load = useCallback(() => { api.listEscalations().then(setItems).catch(console.error).finally(() => setLoading(false)); }, []);
  useEffect(() => { load(); }, [load]);
  if (loading) return <Spinner />;

  const resolve = async (id: string) => {
    try { await api.resolveEscalation(id, "Reviewed and resolved by supervisor."); load(); }
    catch (e) { alert((e as Error).message); }
  };

  return (
    <div className="card p-0 overflow-hidden">
      <table className="w-full text-sm">
        <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
          <th className="px-4 py-3">Case</th><th className="px-4 py-3">Reason</th><th className="px-4 py-3">Detail</th>
          <th className="px-4 py-3">Assigned To</th><th className="px-4 py-3">Status</th><th className="px-4 py-3"></th>
        </tr></thead>
        <tbody className="divide-y divide-gray-100">
          {items.map((e) => (
            <tr key={e.escalation_id} className="hover:bg-gray-50">
              <td className="px-4 py-3 font-mono text-xs">{e.auth_request_id}</td>
              <td className="px-4 py-3 capitalize">{e.reason.replace("_", " ")}</td>
              <td className="px-4 py-3 text-xs text-gray-500">{e.detail || "—"}</td>
              <td className="px-4 py-3">{e.escalated_to_name || "—"}</td>
              <td className="px-4 py-3">
                <span className={`px-2 py-0.5 rounded text-xs font-medium ${e.status === "resolved" ? "bg-green-50 text-green-700" : "bg-amber-50 text-amber-700"}`}>{e.status}</span>
              </td>
              <td className="px-4 py-3">
                {e.status !== "resolved" && (
                  <button onClick={() => resolve(e.escalation_id)} className="text-xs text-databricks-red flex items-center gap-1"><CheckCircle size={12} /> Resolve</button>
                )}
              </td>
            </tr>
          ))}
          {items.length === 0 && <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">No escalations.</td></tr>}
        </tbody>
      </table>
    </div>
  );
}

// ---------------- Inbound Docs ----------------
const SAMPLE_INBOUND = `Fax cover sheet — Re: Prior Authorization
Attached please find the requesting provider's office notes, imaging report, and
history of conservative treatment (6 weeks physical therapy, NSAIDs) supporting
medical necessity for the requested MRI lumbar spine. Please add to the member's
authorization case for clinical review.`;

function InboundDocs() {
  const [items, setItems] = useState<InboundCorrespondence[]>([]);
  const [loading, setLoading] = useState(true);
  const [text, setText] = useState(SAMPLE_INBOUND);
  const [channel, setChannel] = useState("fax");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api.listInboundCorrespondence().then(setItems).catch(console.error).finally(() => setLoading(false));
  }, []);
  useEffect(() => { load(); }, [load]);

  const ingest = async () => {
    setBusy(true);
    try { await api.ingestInboundCorrespondence({ source_channel: channel, raw_text: text }); load(); }
    catch (e) { alert((e as Error).message); } finally { setBusy(false); }
  };

  return (
    <div className="space-y-4">
      <div className="card space-y-2">
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Digitize inbound correspondence</div>
        <p className="text-xs text-gray-400">Faxed/mailed/emailed correspondence is OCR'd, then AI-classified and indexed to a case (ai_query classification).</p>
        <textarea value={text} onChange={(e) => setText(e.target.value)} className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm h-28" />
        <div className="flex items-center gap-2">
          <select value={channel} onChange={(e) => setChannel(e.target.value)} className="border border-gray-300 rounded-md px-2 py-1.5 text-sm">
            <option value="fax">Fax</option><option value="mail">Mail</option>
            <option value="secure_email">Secure email</option><option value="portal">Portal</option>
          </select>
          <button onClick={ingest} disabled={busy || !text} className="btn-primary text-sm flex items-center gap-1 disabled:opacity-40">
            <Play size={14} /> {busy ? "Classifying…" : "Ingest & Classify"}
          </button>
        </div>
      </div>

      {loading ? <Spinner /> : (
        <div className="card p-0 overflow-hidden">
          <table className="w-full text-sm">
            <thead><tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <th className="px-4 py-3">Received</th><th className="px-4 py-3">Channel</th><th className="px-4 py-3">Classified As</th>
              <th className="px-4 py-3">Confidence</th><th className="px-4 py-3">Summary</th><th className="px-4 py-3">Case</th>
            </tr></thead>
            <tbody className="divide-y divide-gray-100">
              {items.map((d) => (
                <tr key={d.inbound_id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-xs text-gray-500">{d.received_at ? new Date(d.received_at).toLocaleString() : "—"}</td>
                  <td className="px-4 py-3 capitalize">{d.source_channel}</td>
                  <td className="px-4 py-3"><span className="px-2 py-0.5 rounded text-xs font-medium bg-blue-50 text-blue-700">{d.classified_type}</span></td>
                  <td className="px-4 py-3">{d.classification_confidence != null ? `${Math.round(d.classification_confidence * 100)}%` : "—"}</td>
                  <td className="px-4 py-3 text-xs text-gray-600 max-w-md truncate">{d.extracted_summary}</td>
                  <td className="px-4 py-3 font-mono text-xs">{d.auth_request_id || <span className="text-amber-600">unindexed</span>}</td>
                </tr>
              ))}
              {items.length === 0 && <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-400">No inbound correspondence yet.</td></tr>}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
