import { useState, useEffect, useCallback } from "react";
import { api, BusinessRule, RuleConflict, RuleSimulation } from "@/lib/api";
import { Workflow, AlertTriangle, Plus, Play, CheckCircle, Trash2, X } from "lucide-react";

const ACTIONS = [
  { value: "auto_approve", label: "Auto-Approve" },
  { value: "auto_deny", label: "Auto-Deny" },
  { value: "pend", label: "Pend for Review" },
  { value: "route", label: "Route" },
];
const OPS = ["eq", "ne", "in", "not_in", "gt", "gte", "lt", "lte", "contains", "exists"];
const FIELDS = ["line_of_business", "service_type", "procedure_code", "diagnosis_codes", "urgency", "estimated_cost"];

interface Condition { field: string; op: string; value: string }

function actionColor(a: string): string {
  if (a === "auto_approve") return "bg-green-50 text-green-700";
  if (a === "auto_deny") return "bg-red-50 text-red-700";
  if (a === "pend") return "bg-amber-50 text-amber-700";
  return "bg-blue-50 text-blue-700";
}
function statusColor(s: string): string {
  if (s === "active") return "bg-green-100 text-green-800";
  if (s === "draft") return "bg-gray-100 text-gray-600";
  if (s === "retired") return "bg-gray-100 text-gray-400 line-through";
  return "bg-amber-100 text-amber-800";
}

// Parse a rule's conditions_json ({all:[...]}) into editable rows.
function toRows(conditions: Record<string, unknown>): Condition[] {
  const all = (conditions?.all as { field: string; op: string; value: unknown }[]) || [];
  return all.map((c) => ({
    field: c.field,
    op: c.op,
    value: Array.isArray(c.value) ? c.value.join(", ") : String(c.value ?? ""),
  }));
}
function toConditions(rows: Condition[]): Record<string, unknown> {
  const all = rows
    .filter((r) => r.field)
    .map((r) => {
      const listOps = ["in", "not_in", "contains"];
      const numeric = ["gt", "gte", "lt", "lte"].includes(r.op);
      let value: unknown = r.value;
      if (listOps.includes(r.op)) value = r.value.split(",").map((v) => v.trim()).filter(Boolean);
      else if (numeric) value = Number(r.value);
      return { field: r.field, op: r.op, value };
    });
  return { all };
}

export function RulesStudio() {
  const [rules, setRules] = useState<BusinessRule[]>([]);
  const [conflicts, setConflicts] = useState<RuleConflict[]>([]);
  const [editing, setEditing] = useState<BusinessRule | "new" | null>(null);
  const [sim, setSim] = useState<Record<string, RuleSimulation>>({});
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api.listRules().then(setRules).catch(console.error);
    api.getRuleConflicts().then((r) => setConflicts(r.conflicts)).catch(console.error);
  }, []);
  useEffect(() => { load(); }, [load]);

  const handleActivate = async (id: string) => {
    await api.activateRule(id); load();
  };
  const handleRetire = async (id: string) => {
    await api.retireRule(id); load();
  };
  const handleSimulate = async (id: string) => {
    setBusy(true);
    try {
      const result = await api.simulateRule(id);
      setSim((s) => ({ ...s, [id]: result }));
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const conflictIds = new Set(conflicts.flatMap((c) => [c.rule_a.rule_id, c.rule_b.rule_id]));

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Workflow size={22} className="text-databricks-dark" />
          <h2 className="text-2xl font-bold text-databricks-dark">Business Rules Studio</h2>
        </div>
        <button onClick={() => setEditing("new")} className="btn-primary text-sm flex items-center gap-1">
          <Plus size={16} /> New Rule
        </button>
      </div>
      <p className="text-sm text-gray-500 -mt-2">
        No-code adjudication &amp; routing rules. Runs in parallel with the Tier-1 deterministic
        SQL engine; changes need no code deploy. Each edit is versioned and requires approval to activate.
      </p>

      {conflicts.length > 0 && (
        <div className="card border-l-4 border-l-amber-400 bg-amber-50/40">
          <div className="flex items-center gap-2 font-semibold text-amber-800">
            <AlertTriangle size={16} /> {conflicts.length} rule conflict{conflicts.length > 1 ? "s" : ""} detected
          </div>
          <ul className="mt-2 text-sm text-amber-800 space-y-1">
            {conflicts.map((c, i) => (
              <li key={i}>
                <span className="font-medium">{c.rule_a.name}</span> ({c.rule_a.action}) overlaps{" "}
                <span className="font-medium">{c.rule_b.name}</span> ({c.rule_b.action}) — higher priority wins.
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="card p-0 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <th className="px-4 py-3">Priority</th>
              <th className="px-4 py-3">Rule</th>
              <th className="px-4 py-3">Scope</th>
              <th className="px-4 py-3">Action</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">v</th>
              <th className="px-4 py-3">Simulate</th>
              <th className="px-4 py-3"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {rules.map((r) => (
              <tr key={r.rule_id} className={conflictIds.has(r.rule_id) ? "bg-amber-50/40" : ""}>
                <td className="px-4 py-3 font-mono">{r.priority}</td>
                <td className="px-4 py-3">
                  <button className="font-medium text-left hover:text-databricks-red" onClick={() => setEditing(r)}>
                    {r.name}
                  </button>
                  <div className="text-xs text-gray-400">{r.category}</div>
                </td>
                <td className="px-4 py-3 text-xs text-gray-500">
                  {r.line_of_business || "All LOB"} · {r.service_type || "All services"}
                </td>
                <td className="px-4 py-3">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${actionColor(r.action)}`}>
                    {r.action.replace(/_/g, " ")}
                  </span>
                </td>
                <td className="px-4 py-3">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${statusColor(r.status)}`}>{r.status}</span>
                </td>
                <td className="px-4 py-3 text-xs text-gray-400">{r.version}</td>
                <td className="px-4 py-3">
                  <button onClick={() => handleSimulate(r.rule_id)} disabled={busy} className="text-blue-600 hover:text-blue-800 flex items-center gap-1 text-xs">
                    <Play size={12} /> Run
                  </button>
                  {sim[r.rule_id] && (
                    <div className="text-xs text-gray-500 mt-1">
                      {sim[r.rule_id].matched}/{sim[r.rule_id].total_evaluated} matched
                      {sim[r.rule_id].agreement_rate_pct !== null && (
                        <> · {sim[r.rule_id].agreement_rate_pct}% agree</>
                      )}
                    </div>
                  )}
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    {r.status !== "active" && r.status !== "retired" && (
                      <button onClick={() => handleActivate(r.rule_id)} className="text-green-600 hover:text-green-800" title="Activate">
                        <CheckCircle size={16} />
                      </button>
                    )}
                    {r.status !== "retired" && (
                      <button onClick={() => handleRetire(r.rule_id)} className="text-gray-400 hover:text-red-600" title="Retire">
                        <Trash2 size={16} />
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {editing && (
        <RuleEditor
          rule={editing === "new" ? null : editing}
          onClose={() => setEditing(null)}
          onSaved={() => { setEditing(null); load(); }}
        />
      )}
    </div>
  );
}

function RuleEditor({ rule, onClose, onSaved }: {
  rule: BusinessRule | null;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [name, setName] = useState(rule?.name || "");
  const [category, setCategory] = useState(rule?.category || "auto-adjudication");
  const [lob, setLob] = useState(rule?.line_of_business || "");
  const [svc, setSvc] = useState(rule?.service_type || "");
  const [action, setAction] = useState(rule?.action || "auto_approve");
  const [actionDetail, setActionDetail] = useState(rule?.action_detail || "");
  const [priority, setPriority] = useState(rule?.priority ?? 100);
  const [rows, setRows] = useState<Condition[]>(rule ? toRows(rule.conditions_json) : [{ field: "service_type", op: "eq", value: "" }]);
  const [busy, setBusy] = useState(false);

  const setRow = (i: number, patch: Partial<Condition>) =>
    setRows((rs) => rs.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));

  const handleSave = async () => {
    setBusy(true);
    try {
      const body = {
        name,
        category,
        line_of_business: lob || null,
        service_type: svc || null,
        conditions_json: toConditions(rows),
        action,
        action_detail: actionDetail || null,
        priority: Number(priority),
      } as Partial<BusinessRule> & { name: string; action: string };
      if (rule) await api.updateRule(rule.rule_id, body);
      else await api.createRule(body);
      onSaved();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/30 flex justify-end z-50" onClick={onClose}>
      <div className="w-[560px] max-w-full bg-white h-full overflow-y-auto shadow-2xl p-6 space-y-4" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between">
          <h3 className="text-lg font-bold text-databricks-dark">{rule ? "Edit Rule" : "New Rule"}</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600"><X size={20} /></button>
        </div>

        <div className="space-y-3">
          <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Rule name"
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm" />
          <div className="grid grid-cols-2 gap-3">
            <input value={category} onChange={(e) => setCategory(e.target.value)} placeholder="Category"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm" />
            <input type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value))} placeholder="Priority"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm" />
            <input value={lob} onChange={(e) => setLob(e.target.value)} placeholder="Line of business (blank = all)"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm" />
            <input value={svc} onChange={(e) => setSvc(e.target.value)} placeholder="Service type (blank = all)"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm" />
          </div>

          {/* Conditions builder (all-of) */}
          <div className="border border-gray-200 rounded-md p-3 space-y-2">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Conditions (all must match)</div>
            {rows.map((r, i) => (
              <div key={i} className="flex gap-2 items-center">
                <select value={r.field} onChange={(e) => setRow(i, { field: e.target.value })}
                  className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-xs">
                  {FIELDS.map((f) => <option key={f} value={f}>{f}</option>)}
                </select>
                <select value={r.op} onChange={(e) => setRow(i, { op: e.target.value })}
                  className="border border-gray-300 rounded-md px-2 py-1.5 text-xs">
                  {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                </select>
                <input value={r.value} onChange={(e) => setRow(i, { value: e.target.value })} placeholder="value"
                  className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-xs" />
                <button onClick={() => setRows((rs) => rs.filter((_, idx) => idx !== i))} className="text-gray-400 hover:text-red-600">
                  <X size={14} />
                </button>
              </div>
            ))}
            <button onClick={() => setRows((rs) => [...rs, { field: "procedure_code", op: "in", value: "" }])}
              className="text-xs text-blue-600 flex items-center gap-1">
              <Plus size={12} /> Add condition
            </button>
            <p className="text-xs text-gray-400">Use commas for list ops (in / not_in / contains).</p>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <select value={action} onChange={(e) => setAction(e.target.value)}
              className="border border-gray-300 rounded-md px-3 py-2 text-sm">
              {ACTIONS.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
            </select>
            <input value={actionDetail} onChange={(e) => setActionDetail(e.target.value)} placeholder="Action detail (optional)"
              className="border border-gray-300 rounded-md px-3 py-2 text-sm" />
          </div>

          <button onClick={handleSave} disabled={busy || !name} className="btn-primary w-full disabled:opacity-40">
            {busy ? "Saving…" : rule ? "Save New Version" : "Create Draft"}
          </button>
          {rule && <p className="text-xs text-gray-400">Saved as a new draft version; activate to deploy to production.</p>}
        </div>
      </div>
    </div>
  );
}
