import { useEffect, useState } from "react";
import {
  ArrowLeft,
  User,
  Clock,
  DollarSign,
  MessageSquare,
  Send,
  Loader2,
  Bot,
  FileText,
  ShieldAlert,
  ExternalLink,
} from "lucide-react";
import { api, type InvestigationDetail as InvDetail, type Investigator } from "@/lib/api";
import {
  formatCurrency,
  formatDate,
  formatDateTime,
  severityBadgeClass,
  statusColor,
} from "@/lib/utils";

interface InvestigationDetailProps {
  investigationId: string;
  onBack: () => void;
  onViewProvider: (npi: string) => void;
}

const STATUS_OPTIONS = [
  "Open",
  "Under Review",
  "Evidence Gathering",
  "Referred to SIU",
  "Recovery In Progress",
  "Closed \u2014 Confirmed Fraud",
  "Closed \u2014 No Fraud",
  "Closed \u2014 Insufficient Evidence",
];

export function InvestigationDetail({
  investigationId,
  onBack,
  onViewProvider,
}: InvestigationDetailProps) {
  const [inv, setInv] = useState<InvDetail | null>(null);
  const [investigators, setInvestigators] = useState<Investigator[]>([]);
  const [loading, setLoading] = useState(true);
  const [note, setNote] = useState("");
  const [newStatus, setNewStatus] = useState("");
  const [statusNote, setStatusNote] = useState("");
  const [recoveryAmount, setRecoveryAmount] = useState("");
  const [agentQuestion, setAgentQuestion] = useState("");
  const [agentAnswer, setAgentAnswer] = useState("");
  const [agentLoading, setAgentLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);

  useEffect(() => {
    Promise.all([
      api.getInvestigation(investigationId),
      api.listInvestigators(),
    ]).then(([invData, invList]) => {
      setInv(invData);
      setInvestigators(invList);
    }).finally(() => setLoading(false));
  }, [investigationId]);

  const handleAssign = async (investigatorId: string) => {
    setActionLoading(true);
    try {
      const updated = await api.assignInvestigation(investigationId, investigatorId);
      setInv(updated);
    } finally {
      setActionLoading(false);
    }
  };

  const handleStatusUpdate = async () => {
    if (!newStatus) return;
    setActionLoading(true);
    try {
      const updated = await api.updateInvestigationStatus(investigationId, newStatus, statusNote || undefined);
      setInv(updated);
      setNewStatus("");
      setStatusNote("");
    } finally {
      setActionLoading(false);
    }
  };

  const handleAddNote = async () => {
    if (!note.trim()) return;
    setActionLoading(true);
    try {
      const updated = await api.addNote(investigationId, note);
      setInv(updated);
      setNote("");
    } finally {
      setActionLoading(false);
    }
  };

  const handleRecovery = async () => {
    const amt = parseFloat(recoveryAmount);
    if (isNaN(amt) || amt <= 0) return;
    setActionLoading(true);
    try {
      const updated = await api.recordRecovery(investigationId, amt);
      setInv(updated);
      setRecoveryAmount("");
    } finally {
      setActionLoading(false);
    }
  };

  const handleAgentQuery = async () => {
    if (!agentQuestion.trim()) return;
    setAgentLoading(true);
    setAgentAnswer("");
    try {
      const result = await api.queryAgent(agentQuestion, investigationId, "investigation");
      setAgentAnswer(result.answer);
    } catch (err) {
      setAgentAnswer(`Error: ${err}`);
    } finally {
      setAgentLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="space-y-6">
        <button onClick={onBack} className="flex items-center gap-2 text-sm text-gray-500 hover:text-databricks-dark">
          <ArrowLeft className="w-4 h-4" /> Back to Queue
        </button>
        <div className="card p-8 text-center text-gray-500">Loading investigation...</div>
      </div>
    );
  }

  if (!inv) return null;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <button onClick={onBack} className="flex items-center gap-2 text-sm text-gray-500 hover:text-databricks-dark mb-2">
            <ArrowLeft className="w-4 h-4" /> Back to Queue
          </button>
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-bold text-databricks-dark">{inv.investigation_id}</h2>
            <span className={severityBadgeClass(inv.severity)}>{inv.severity}</span>
            <span className={`text-sm font-medium ${statusColor(inv.status)}`}>{inv.status}</span>
          </div>
          <p className="text-gray-500 mt-1">
            {inv.investigation_type} investigation — {inv.target_name || inv.target_id}
          </p>
        </div>
        {inv.target_type === "provider" && inv.target_id && (
          <button
            onClick={() => onViewProvider(inv.target_id!)}
            className="btn-secondary flex items-center gap-2 text-sm"
          >
            <ExternalLink className="w-4 h-4" /> View Provider Profile
          </button>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left column: Details + Actions */}
        <div className="lg:col-span-2 space-y-6">
          {/* Key info cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="card p-4">
              <div className="text-xs text-gray-500 mb-1">Est. Overpayment</div>
              <div className="text-lg font-bold text-red-600">{formatCurrency(inv.estimated_overpayment)}</div>
            </div>
            <div className="card p-4">
              <div className="text-xs text-gray-500 mb-1">Recovered</div>
              <div className="text-lg font-bold text-green-600">{formatCurrency(inv.recovered_amount)}</div>
            </div>
            <div className="card p-4">
              <div className="text-xs text-gray-500 mb-1">Claims Involved</div>
              <div className="text-lg font-bold text-databricks-dark">{inv.claims_involved_count ?? "—"}</div>
            </div>
            <div className="card p-4">
              <div className="text-xs text-gray-500 mb-1">Composite Risk</div>
              <div className="text-lg font-bold text-databricks-dark">
                {inv.composite_risk_score != null ? `${(inv.composite_risk_score * 100).toFixed(0)}%` : "—"}
              </div>
              <div className="flex gap-2 mt-1 text-xs text-gray-400">
                {inv.rules_risk_score != null && <span>Rules: {(inv.rules_risk_score * 100).toFixed(0)}</span>}
                {inv.ml_risk_score != null && <span>ML: {(inv.ml_risk_score * 100).toFixed(0)}</span>}
              </div>
            </div>
          </div>

          {/* Fraud types */}
          {inv.fraud_types && inv.fraud_types.length > 0 && (
            <div className="card p-4">
              <h3 className="text-sm font-semibold text-databricks-dark mb-2">Fraud Types</h3>
              <div className="flex flex-wrap gap-2">
                {inv.fraud_types.map((ft) => (
                  <span key={ft} className="bg-red-50 text-red-700 text-sm px-3 py-1 rounded-lg">
                    {ft}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Summary */}
          {inv.investigation_summary && (
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-databricks-dark mb-2">Investigation Summary</h3>
              <p className="text-sm text-gray-700 whitespace-pre-wrap">{inv.investigation_summary}</p>
            </div>
          )}

          {/* Agent Chat */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <Bot className="w-4 h-4 text-databricks-red" /> FWA Investigation Agent
            </h3>
            <div className="flex gap-2 mb-3">
              <input
                value={agentQuestion}
                onChange={(e) => setAgentQuestion(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleAgentQuery()}
                placeholder="Ask the agent about this investigation..."
                className="flex-1 px-3 py-2 text-sm border border-gray-300 rounded-lg"
                disabled={agentLoading}
              />
              <button
                onClick={handleAgentQuery}
                disabled={!agentQuestion.trim() || agentLoading}
                className="btn-primary flex items-center gap-2 text-sm"
              >
                {agentLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
                Ask
              </button>
            </div>
            <div className="flex flex-wrap gap-2 mb-3">
              {["Give me a full briefing", "Summarize the evidence", "What are the recommended next steps?"].map((q) => (
                <button
                  key={q}
                  onClick={() => { setAgentQuestion(q); }}
                  className="text-xs px-3 py-1 rounded-lg border border-gray-200 hover:border-databricks-red hover:bg-red-50 text-gray-500"
                >
                  {q}
                </button>
              ))}
            </div>
            {agentAnswer && (
              <div className="bg-gray-50 rounded-lg p-4 text-sm text-gray-700 whitespace-pre-wrap max-h-96 overflow-y-auto">
                {agentAnswer}
              </div>
            )}
          </div>

          {/* Evidence */}
          {inv.evidence.length > 0 && (
            <div className="card p-5">
              <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <FileText className="w-4 h-4" /> Evidence ({inv.evidence.length})
              </h3>
              <div className="space-y-3">
                {inv.evidence.map((e) => (
                  <div key={e.evidence_id} className="border border-gray-100 rounded-lg p-3">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs font-semibold text-gray-500 uppercase">{e.evidence_type}</span>
                      <span className="text-xs text-gray-400">{formatDateTime(e.created_at)}</span>
                    </div>
                    <p className="text-sm text-gray-700">{e.description}</p>
                    {e.added_by_name && <p className="text-xs text-gray-400 mt-1">Added by {e.added_by_name}</p>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Audit Log */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <Clock className="w-4 h-4" /> Audit Trail ({inv.audit_log.length})
            </h3>
            <div className="space-y-3">
              {inv.audit_log.map((entry) => (
                <div key={entry.audit_id} className="flex gap-3 border-l-2 border-gray-200 pl-3">
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-databricks-dark capitalize">
                        {entry.action_type.replace(/_/g, " ")}
                      </span>
                      {entry.previous_status && entry.new_status && (
                        <span className="text-xs text-gray-400">
                          {entry.previous_status} → {entry.new_status}
                        </span>
                      )}
                    </div>
                    {entry.note && <p className="text-sm text-gray-600 mt-0.5">{entry.note}</p>}
                    <div className="text-xs text-gray-400 mt-0.5">
                      {entry.investigator_name && `${entry.investigator_name} — `}
                      {formatDateTime(entry.created_at)}
                    </div>
                  </div>
                </div>
              ))}
              {inv.audit_log.length === 0 && (
                <p className="text-sm text-gray-400">No audit entries yet.</p>
              )}
            </div>
          </div>
        </div>

        {/* Right column: Actions */}
        <div className="space-y-6">
          {/* Assignment */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <User className="w-4 h-4" /> Assigned Investigator
            </h3>
            {inv.investigator_name ? (
              <div className="mb-3">
                <p className="text-sm font-medium">{inv.investigator_name}</p>
                <p className="text-xs text-gray-400">{inv.investigator_role}</p>
                <p className="text-xs text-gray-400">Assigned {formatDate(inv.assigned_at)}</p>
              </div>
            ) : (
              <p className="text-sm text-amber-600 mb-3">Not yet assigned</p>
            )}
            <select
              onChange={(e) => e.target.value && handleAssign(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded-lg px-3 py-2"
              disabled={actionLoading}
              defaultValue=""
            >
              <option value="">Assign to...</option>
              {investigators.map((i) => (
                <option key={i.investigator_id} value={i.investigator_id}>
                  {i.display_name} ({i.role})
                </option>
              ))}
            </select>
          </div>

          {/* Status Update */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <ShieldAlert className="w-4 h-4" /> Update Status
            </h3>
            <select
              value={newStatus}
              onChange={(e) => setNewStatus(e.target.value)}
              className="w-full text-sm border border-gray-300 rounded-lg px-3 py-2 mb-2"
            >
              <option value="">Select new status...</option>
              {STATUS_OPTIONS.filter((s) => s !== inv.status).map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            <textarea
              value={statusNote}
              onChange={(e) => setStatusNote(e.target.value)}
              placeholder="Add a note (optional)"
              rows={2}
              className="w-full text-sm border border-gray-300 rounded-lg px-3 py-2 mb-2"
            />
            <button
              onClick={handleStatusUpdate}
              disabled={!newStatus || actionLoading}
              className="btn-primary w-full text-sm"
            >
              Update Status
            </button>
          </div>

          {/* Add Note */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <MessageSquare className="w-4 h-4" /> Add Note
            </h3>
            <textarea
              value={note}
              onChange={(e) => setNote(e.target.value)}
              placeholder="Enter note..."
              rows={3}
              className="w-full text-sm border border-gray-300 rounded-lg px-3 py-2 mb-2"
            />
            <button
              onClick={handleAddNote}
              disabled={!note.trim() || actionLoading}
              className="btn-primary w-full text-sm"
            >
              Add Note
            </button>
          </div>

          {/* Record Recovery */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <DollarSign className="w-4 h-4" /> Record Recovery
            </h3>
            <div className="flex items-center gap-2 mb-2">
              <span className="text-gray-500">$</span>
              <input
                type="number"
                value={recoveryAmount}
                onChange={(e) => setRecoveryAmount(e.target.value)}
                placeholder="Amount"
                className="flex-1 text-sm border border-gray-300 rounded-lg px-3 py-2"
              />
            </div>
            <button
              onClick={handleRecovery}
              disabled={!recoveryAmount || actionLoading}
              className="btn-primary w-full text-sm"
            >
              Record Recovery
            </button>
          </div>

          {/* Key Dates */}
          <div className="card p-5">
            <h3 className="text-sm font-semibold text-databricks-dark mb-3">Timeline</h3>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Created</span>
                <span>{formatDate(inv.created_at)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Last Updated</span>
                <span>{formatDate(inv.updated_at)}</span>
              </div>
              {inv.closed_at && (
                <div className="flex justify-between">
                  <span className="text-gray-500">Closed</span>
                  <span>{formatDate(inv.closed_at)}</span>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
