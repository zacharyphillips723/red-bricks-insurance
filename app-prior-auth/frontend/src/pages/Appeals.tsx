import { useState, useEffect, useCallback } from "react";
import { api, Appeal, Reviewer } from "@/lib/api";
import { AlertTriangle, Zap, Scale, X, ArrowRight } from "lucide-react";

function urgencyBadge(urgency: string | null): string {
  switch (urgency) {
    case "expedited": return "bg-red-100 text-red-800";
    case "standard": return "bg-blue-100 text-blue-800";
    default: return "bg-gray-100 text-gray-600";
  }
}

function appealStatusColor(status: string | null): string {
  if (!status) return "text-gray-500 bg-gray-50";
  if (status === "Overturned") return "text-green-700 bg-green-50";
  if (status === "Partially Overturned") return "text-teal-700 bg-teal-50";
  if (status === "Upheld") return "text-red-700 bg-red-50";
  if (status === "Received") return "text-amber-700 bg-amber-50";
  if (status === "In Review") return "text-blue-700 bg-blue-50";
  return "text-gray-700 bg-gray-50";
}

function formatDate(d: string | null): string {
  if (!d) return "—";
  return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

const OPEN_STATUSES = [
  "Received", "In Review", "Additional Info Requested",
  "Peer Review Requested", "Hearing Scheduled", "IRO Referred",
];
const DETERMINATIONS = ["Overturned", "Partially Overturned", "Upheld"];

export function Appeals() {
  const [appeals, setAppeals] = useState<Appeal[]>([]);
  const [reviewers, setReviewers] = useState<Reviewer[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState("");
  const [selected, setSelected] = useState<Appeal | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    const params: Record<string, string> = {};
    if (filterStatus) params.status = filterStatus;
    setLoading(true);
    api.listAppeals(params).then(setAppeals).catch(console.error).finally(() => setLoading(false));
  }, [filterStatus]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { api.listReviewers().then(setReviewers).catch(console.error); }, []);

  const refreshSelected = async (id: string) => {
    const fresh = await api.getAppeal(id);
    setSelected(fresh);
    load();
  };

  const handleAssign = async (reviewerId: string) => {
    if (!selected) return;
    setBusy(true);
    try {
      await api.assignAppeal(selected.appeal_id, reviewerId);
      await refreshSelected(selected.appeal_id);
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const handleDecide = async (status: string, externalReason: string, internalNotes: string) => {
    if (!selected) return;
    setBusy(true);
    try {
      await api.decideAppeal(selected.appeal_id, {
        status,
        determination_reason_external: externalReason,
        reviewer_notes_internal: internalNotes,
      });
      await refreshSelected(selected.appeal_id);
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const statuses = [
    "Received", "In Review", "Additional Info Requested", "Peer Review Requested",
    "Hearing Scheduled", "IRO Referred", "Overturned", "Partially Overturned", "Upheld",
  ];

  // Overturn rate among decided appeals (a headline UM quality metric)
  const decided = appeals.filter((a) => DETERMINATIONS.includes(a.status || ""));
  const overturned = decided.filter((a) => (a.status || "").includes("Overturned"));
  const overturnRate = decided.length ? Math.round((overturned.length / decided.length) * 100) : null;
  const openCount = appeals.filter((a) => OPEN_STATUSES.includes(a.status || "")).length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Scale size={22} className="text-databricks-dark" />
          <h2 className="text-2xl font-bold text-databricks-dark">Appeals &amp; Reconsiderations</h2>
        </div>
        <span className="text-sm text-gray-500">{appeals.length} appeals</span>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card">
          <div className="text-xs text-gray-500 uppercase tracking-wider">Open Appeals</div>
          <div className="text-2xl font-bold text-databricks-dark mt-1">{openCount}</div>
        </div>
        <div className="card">
          <div className="text-xs text-gray-500 uppercase tracking-wider">Decided</div>
          <div className="text-2xl font-bold text-databricks-dark mt-1">{decided.length}</div>
        </div>
        <div className="card">
          <div className="text-xs text-gray-500 uppercase tracking-wider">Overturn Rate</div>
          <div className="text-2xl font-bold text-databricks-dark mt-1">
            {overturnRate === null ? "—" : `${overturnRate}%`}
          </div>
        </div>
      </div>

      <div className="flex gap-3 items-center">
        <select
          value={filterStatus}
          onChange={(e) => setFilterStatus(e.target.value)}
          className="border border-gray-300 rounded-md px-3 py-2 text-sm"
        >
          <option value="">All Statuses</option>
          {statuses.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      <div className="card p-0 overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400">Loading...</div>
        ) : appeals.length === 0 ? (
          <div className="p-8 text-center text-gray-400">No appeals match the current filter.</div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <th className="px-4 py-3">Appeal</th>
                <th className="px-4 py-3">Member</th>
                <th className="px-4 py-3">Service</th>
                <th className="px-4 py-3">Type</th>
                <th className="px-4 py-3">Urgency</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Appeals Reviewer</th>
                <th className="px-4 py-3">Filed</th>
                <th className="px-4 py-3">Deadline</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {appeals.map((a) => {
                const isOpen = OPEN_STATUSES.includes(a.status || "");
                const hoursLeft = a.hours_until_deadline;
                const isOverdue = isOpen && hoursLeft !== null && hoursLeft < 0;
                return (
                  <tr
                    key={a.appeal_id}
                    onClick={() => setSelected(a)}
                    className="hover:bg-gray-50 cursor-pointer"
                  >
                    <td className="px-4 py-3 font-mono text-xs">{a.appeal_id.slice(0, 8)}</td>
                    <td className="px-4 py-3 font-medium">{a.member_name || "—"}</td>
                    <td className="px-4 py-3">
                      {a.service_type}
                      <div className="text-xs text-gray-400 font-mono">{a.procedure_code}</div>
                    </td>
                    <td className="px-4 py-3 capitalize">{a.appeal_type}</td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${urgencyBadge(a.urgency)}`}>
                        {a.urgency === "expedited" && <Zap size={10} className="inline mr-1" />}
                        {a.urgency}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${appealStatusColor(a.status)}`}>
                        {a.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-xs">
                      {a.appeal_reviewer_name || <span className="text-gray-300">Unassigned</span>}
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">{formatDate(a.filed_date)}</td>
                    <td className="px-4 py-3">
                      {isOpen ? (
                        <div className={`flex items-center gap-1 text-xs ${isOverdue ? "text-red-600 font-semibold" : "text-gray-500"}`}>
                          {isOverdue && <AlertTriangle size={12} />}
                          {hoursLeft !== null
                            ? isOverdue
                              ? `${Math.abs(Math.round(hoursLeft))}h overdue`
                              : `${Math.round(hoursLeft)}h left`
                            : formatDate(a.cms_deadline)}
                        </div>
                      ) : (
                        <span className={`text-xs ${a.cms_compliant ? "text-green-600" : "text-red-600"}`}>
                          {a.cms_compliant ? "Met" : "Missed"}
                        </span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {selected && (
        <AppealPanel
          appeal={selected}
          reviewers={reviewers}
          busy={busy}
          onClose={() => setSelected(null)}
          onAssign={handleAssign}
          onDecide={handleDecide}
        />
      )}
    </div>
  );
}

interface PanelProps {
  appeal: Appeal;
  reviewers: Reviewer[];
  busy: boolean;
  onClose: () => void;
  onAssign: (reviewerId: string) => void;
  onDecide: (status: string, externalReason: string, internalNotes: string) => void;
}

function AppealPanel({ appeal, reviewers, busy, onClose, onAssign, onDecide }: PanelProps) {
  const [reviewerId, setReviewerId] = useState("");
  const [determination, setDetermination] = useState("Upheld");
  const [externalReason, setExternalReason] = useState("");
  const [internalNotes, setInternalNotes] = useState("");

  const decided = DETERMINATIONS.includes(appeal.status || "");
  const assigned = !!appeal.appeal_reviewer_name;

  return (
    <div className="fixed inset-0 bg-black/30 flex justify-end z-50" onClick={onClose}>
      <div
        className="w-[520px] max-w-full bg-white h-full overflow-y-auto shadow-2xl p-6 space-y-5"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between">
          <div>
            <h3 className="text-lg font-bold text-databricks-dark">Appeal Review</h3>
            <p className="text-xs font-mono text-gray-400">{appeal.appeal_id}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>

        {/* Original determination context */}
        <div className="rounded-md border border-gray-200 p-4 space-y-1 text-sm">
          <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1">
            Original Determination
          </div>
          <div className="flex items-center gap-2">
            <span className="font-medium">{appeal.member_name}</span>
            <span className="text-gray-400">·</span>
            <span>{appeal.service_type}</span>
            <span className="font-mono text-xs text-gray-500">{appeal.procedure_code}</span>
          </div>
          <div className="text-gray-600">
            Status: <span className="font-medium">{appeal.original_status}</span>
            {appeal.original_denial_reason_code && (
              <span className="ml-2 font-mono text-xs bg-red-50 text-red-700 px-1.5 py-0.5 rounded">
                {appeal.original_denial_reason_code}
              </span>
            )}
          </div>
          {appeal.original_determination_reason && (
            <div className="text-gray-500 text-xs mt-1">{appeal.original_determination_reason}</div>
          )}
          <div className="text-xs text-gray-400 mt-1">
            Original reviewer: {appeal.original_reviewer_name || "—"} · Source case{" "}
            <span className="font-mono">{appeal.auth_request_id.slice(0, 12)}</span>
          </div>
        </div>

        {/* Appeal facts */}
        <div className="grid grid-cols-2 gap-3 text-sm">
          <Field label="Appeal Type" value={appeal.appeal_type} />
          <Field label="Urgency" value={appeal.urgency} />
          <Field label="Filed By" value={appeal.filed_by} />
          <Field label="Filed" value={formatDate(appeal.filed_date)} />
          <Field label="Appeals Reviewer" value={appeal.appeal_reviewer_name || "Unassigned"} />
          <Field label="Determination" value={appeal.determination || "Pending"} />
        </div>

        {/* Assignment (conflict-of-interest enforced server-side) */}
        {!decided && (
          <div className="rounded-md border border-gray-200 p-4 space-y-2">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              Assign Appeals Reviewer
            </div>
            <p className="text-xs text-gray-400">
              Cannot be the original determining reviewer — enforced by the review integrity rule.
            </p>
            <div className="flex gap-2">
              <select
                value={reviewerId}
                onChange={(e) => setReviewerId(e.target.value)}
                className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm"
              >
                <option value="">Select reviewer…</option>
                {reviewers.map((r) => (
                  <option key={r.reviewer_id} value={r.reviewer_id}>
                    {r.display_name} — {r.role}
                  </option>
                ))}
              </select>
              <button
                disabled={!reviewerId || busy}
                onClick={() => onAssign(reviewerId)}
                className="btn-primary disabled:opacity-40"
              >
                Assign
              </button>
            </div>
          </div>
        )}

        {/* Determination */}
        {!decided && (
          <div className="rounded-md border border-gray-200 p-4 space-y-3">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              Record Determination
            </div>
            <div className="flex gap-2">
              {DETERMINATIONS.map((d) => (
                <button
                  key={d}
                  onClick={() => setDetermination(d)}
                  className={`px-3 py-1.5 rounded-md text-sm font-medium border ${
                    determination === d
                      ? "border-databricks-red bg-red-50 text-databricks-red"
                      : "border-gray-300 text-gray-600"
                  }`}
                >
                  {d}
                </button>
              ))}
            </div>
            <textarea
              placeholder="Member/provider-facing rationale (external)…"
              value={externalReason}
              onChange={(e) => setExternalReason(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm h-20"
            />
            <textarea
              placeholder="Internal reviewer notes…"
              value={internalNotes}
              onChange={(e) => setInternalNotes(e.target.value)}
              className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm h-16"
            />
            <button
              disabled={busy || !assigned}
              onClick={() => onDecide(determination, externalReason, internalNotes)}
              className="btn-primary w-full disabled:opacity-40 flex items-center justify-center gap-2"
            >
              Submit Determination <ArrowRight size={16} />
            </button>
            {!assigned && (
              <p className="text-xs text-amber-600">Assign an appeals reviewer before deciding.</p>
            )}
          </div>
        )}

        {decided && (
          <div className={`rounded-md p-4 ${appealStatusColor(appeal.status)}`}>
            <div className="font-semibold">Appeal {appeal.status}</div>
            {appeal.turnaround_hours !== null && (
              <div className="text-xs mt-1">
                Turnaround: {Math.round(appeal.turnaround_hours)}h ·{" "}
                {appeal.cms_compliant ? "CMS compliant" : "CMS deadline missed"}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string | null }) {
  return (
    <div>
      <div className="text-xs text-gray-400 uppercase tracking-wider">{label}</div>
      <div className="text-gray-800 capitalize">{value || "—"}</div>
    </div>
  );
}
