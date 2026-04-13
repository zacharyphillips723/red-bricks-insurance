import { useState, useEffect, useRef } from "react";
import { api, PARequestDetail, Reviewer, AgentResponse } from "@/lib/api";
import {
  ArrowLeft,
  Bot,
  Clock,
  Shield,
  Zap,
  Send,
  CheckCircle,
  XCircle,
  AlertTriangle,
  FileText,
} from "lucide-react";
import ReactMarkdown from "react-markdown";

interface RequestDetailProps {
  requestId: string;
  onBack: () => void;
}

function statusColor(status: string | null): string {
  if (!status) return "text-gray-500 bg-gray-50";
  if (status === "Approved") return "text-green-700 bg-green-50 border-green-200";
  if (status === "Denied") return "text-red-700 bg-red-50 border-red-200";
  if (status === "Pending Review") return "text-amber-700 bg-amber-50 border-amber-200";
  if (status === "In Review") return "text-blue-700 bg-blue-50 border-blue-200";
  return "text-gray-700 bg-gray-50 border-gray-200";
}

function formatDate(d: string | null): string {
  if (!d) return "N/A";
  return new Date(d).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

export function RequestDetail({ requestId, onBack }: RequestDetailProps) {
  const [detail, setDetail] = useState<PARequestDetail | null>(null);
  const [reviewers, setReviewers] = useState<Reviewer[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedReviewer, setSelectedReviewer] = useState("");
  const [newStatus, setNewStatus] = useState("");
  const [statusNote, setStatusNote] = useState("");
  const [note, setNote] = useState("");
  const [agentQuestion, setAgentQuestion] = useState("");
  const [agentResponse, setAgentResponse] = useState<AgentResponse | null>(null);
  const [agentLoading, setAgentLoading] = useState(false);
  const agentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    Promise.all([
      api.getRequest(requestId),
      api.listReviewers(),
    ]).then(([req, revs]) => {
      setDetail(req);
      setReviewers(revs);
    }).catch(console.error).finally(() => setLoading(false));
  }, [requestId]);

  const handleAssign = async () => {
    if (!selectedReviewer) return;
    const updated = await api.assignReviewer(requestId, selectedReviewer);
    setDetail(updated);
    setSelectedReviewer("");
  };

  const handleStatusChange = async () => {
    if (!newStatus) return;
    const updated = await api.updateStatus(requestId, newStatus, statusNote || undefined);
    setDetail(updated);
    setNewStatus("");
    setStatusNote("");
  };

  const handleAddNote = async () => {
    if (!note.trim()) return;
    const updated = await api.addNote(requestId, note);
    setDetail(updated);
    setNote("");
  };

  const handleAgentQuery = async (question?: string) => {
    const q = question || agentQuestion;
    if (!q.trim()) return;
    setAgentLoading(true);
    setAgentResponse(null);
    try {
      const resp = await api.queryAgent(q, requestId);
      setAgentResponse(resp);
      agentRef.current?.scrollIntoView({ behavior: "smooth" });
    } catch (e) {
      setAgentResponse({ answer: `Error: ${e}`, sources: [] });
    } finally {
      setAgentLoading(false);
      setAgentQuestion("");
    }
  };

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="flex items-center gap-3">
          <button onClick={onBack} className="text-gray-400 hover:text-gray-600"><ArrowLeft size={20} /></button>
          <div className="h-8 w-64 bg-gray-100 animate-pulse rounded" />
        </div>
        <div className="grid grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => <div key={i} className="card h-32 animate-pulse bg-gray-100" />)}
        </div>
      </div>
    );
  }

  if (!detail) return <div className="text-red-600">Request not found.</div>;

  const hoursLeft = detail.hours_until_deadline;
  const openStatuses = ["Pending Review", "In Review", "Additional Info Requested", "Peer Review Requested"];
  const isOpen = openStatuses.includes(detail.status || "");
  const isDetermined = !!detail.determination_date;
  const isOverdue = isOpen && hoursLeft !== null && hoursLeft < 0;
  const suggestedQuestions = [
    `Review the clinical evidence for PA request ${requestId} and recommend approve or deny.`,
    `What medical policy criteria apply to procedure ${detail.procedure_code}?`,
    `Show the ML model prediction and Tier 1 rule evaluation for this request.`,
  ];

  const statuses = ["Pending Review", "In Review", "Additional Info Requested", "Approved", "Denied", "Partially Approved", "Peer Review Requested"];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <button onClick={onBack} className="text-gray-400 hover:text-gray-600"><ArrowLeft size={20} /></button>
        <div className="flex-1">
          <h2 className="text-xl font-bold text-databricks-dark flex items-center gap-2">
            PA Request
            <span className="font-mono text-sm text-gray-500">{detail.auth_request_id}</span>
          </h2>
          <p className="text-sm text-gray-500">
            {detail.procedure_description || detail.procedure_code} — {detail.service_type}
          </p>
        </div>
        <span className={`px-3 py-1 rounded-md text-sm font-medium border ${statusColor(detail.status)}`}>
          {detail.status}
        </span>
      </div>

      <div className="grid grid-cols-3 gap-6">
        {/* Left column — 2/3 */}
        <div className="col-span-2 space-y-4">
          {/* Key metrics */}
          <div className="grid grid-cols-4 gap-3">
            <div className="card py-3 text-center">
              <p className="text-2xl font-bold text-databricks-dark">${(detail.estimated_cost || 0).toLocaleString()}</p>
              <p className="text-xs text-gray-500">Estimated Cost</p>
            </div>
            <div className="card py-3 text-center">
              <div className="flex items-center justify-center gap-1">
                {detail.urgency === "expedited" && <Zap size={16} className="text-red-500" />}
                <p className="text-lg font-bold text-databricks-dark">{detail.urgency}</p>
              </div>
              <p className="text-xs text-gray-500">Urgency</p>
            </div>
            <div className="card py-3 text-center">
              {isDetermined ? (
                <p className={`text-lg font-bold ${detail.cms_compliant ? "text-green-600" : "text-red-600"}`}>
                  {detail.cms_compliant ? "Met" : "Missed"}
                </p>
              ) : (
                <p className={`text-lg font-bold ${isOverdue ? "text-red-600" : hoursLeft !== null && hoursLeft < 24 ? "text-amber-600" : "text-green-600"}`}>
                  {hoursLeft !== null ? (isOverdue ? `${Math.abs(Math.round(hoursLeft))}h overdue` : `${Math.round(hoursLeft)}h left`) : "N/A"}
                </p>
              )}
              <p className="text-xs text-gray-500">CMS Deadline</p>
            </div>
            <div className="card py-3 text-center">
              {detail.ai_confidence !== null ? (
                <>
                  <p className={`text-lg font-bold ${detail.ai_confidence > 0.8 ? "text-green-600" : detail.ai_confidence > 0.6 ? "text-amber-600" : "text-red-600"}`}>
                    {((detail.ai_confidence || 0) * 100).toFixed(0)}%
                  </p>
                  <p className="text-xs text-gray-500">AI Confidence</p>
                </>
              ) : (
                <>
                  <p className="text-lg font-bold text-gray-400">N/A</p>
                  <p className="text-xs text-gray-500">AI Confidence</p>
                </>
              )}
            </div>
          </div>

          {/* Request details */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-3">Request Details</h3>
            <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
              <div><span className="text-gray-500">Member:</span> <span className="font-medium">{detail.member_name || detail.member_id}</span></div>
              <div><span className="text-gray-500">Provider:</span> <span className="font-medium">{detail.provider_name || detail.requesting_provider_npi}</span></div>
              <div><span className="text-gray-500">Procedure:</span> <span className="font-mono">{detail.procedure_code}</span> — {detail.procedure_description}</div>
              <div><span className="text-gray-500">Diagnosis:</span> <span className="font-mono">{detail.diagnosis_codes || "N/A"}</span></div>
              <div><span className="text-gray-500">Policy:</span> {detail.policy_name || "N/A"}</div>
              <div><span className="text-gray-500">LOB:</span> {detail.line_of_business || "N/A"}</div>
              <div><span className="text-gray-500">Tier:</span> {detail.determination_tier || "N/A"}</div>
              <div><span className="text-gray-500">Requested:</span> {formatDate(detail.request_date)}</div>
            </div>
          </div>

          {/* Clinical summary */}
          {detail.clinical_summary && (
            <div className="card">
              <h3 className="font-semibold text-databricks-dark mb-2 flex items-center gap-2">
                <FileText size={16} /> Clinical Summary
              </h3>
              <p className="text-sm text-gray-700 leading-relaxed">{detail.clinical_summary}</p>
            </div>
          )}

          {/* AI Recommendation */}
          {detail.ai_recommendation && (
            <div className="card border-l-4 border-l-purple-400">
              <h3 className="font-semibold text-databricks-dark mb-2 flex items-center gap-2">
                <Shield size={16} className="text-purple-500" /> AI Recommendation
              </h3>
              <p className="text-sm text-gray-700">{detail.ai_recommendation}</p>
              {detail.tier1_auto_eligible && (
                <div className="mt-2 flex items-center gap-1 text-xs text-green-700 bg-green-50 px-2 py-1 rounded w-fit">
                  <CheckCircle size={12} /> Tier 1 Auto-Eligible
                </div>
              )}
            </div>
          )}

          {/* PA Review Agent */}
          <div className="card" ref={agentRef}>
            <h3 className="font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <Bot size={16} /> PA Review Agent
            </h3>
            <div className="flex flex-wrap gap-2 mb-3">
              {suggestedQuestions.map((q, i) => (
                <button
                  key={i}
                  onClick={() => handleAgentQuery(q)}
                  disabled={agentLoading}
                  className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 px-3 py-1.5 rounded-full transition-colors"
                >
                  {q.slice(0, 60)}...
                </button>
              ))}
            </div>
            <div className="flex gap-2">
              <input
                type="text"
                placeholder="Ask the PA Review Agent..."
                value={agentQuestion}
                onChange={(e) => setAgentQuestion(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleAgentQuery()}
                className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-databricks-red/20"
              />
              <button onClick={() => handleAgentQuery()} disabled={agentLoading} className="btn-primary flex items-center gap-1">
                <Send size={14} /> {agentLoading ? "Thinking..." : "Ask"}
              </button>
            </div>
            {agentLoading && (
              <div className="mt-3 flex items-center gap-2 text-sm text-gray-500">
                <div className="animate-spin h-4 w-4 border-2 border-databricks-red border-t-transparent rounded-full" />
                Analyzing clinical evidence and policy criteria...
              </div>
            )}
            {agentResponse && (
              <div className="mt-4 border-t pt-4">
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
                  >{agentResponse.answer}</ReactMarkdown>
                </div>
              </div>
            )}
          </div>

          {/* Audit Trail */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-3">Activity Log</h3>
            {detail.audit_log.length === 0 ? (
              <p className="text-sm text-gray-400">No activity yet.</p>
            ) : (
              <div className="space-y-3">
                {detail.audit_log.map((a) => (
                  <div key={a.action_id} className="flex gap-3 text-sm">
                    <div className="w-2 h-2 rounded-full bg-gray-300 mt-1.5 flex-shrink-0" />
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{a.reviewer_name || "System"}</span>
                        <span className="text-gray-400 text-xs">{a.action_type.replace(/_/g, " ")}</span>
                        {a.new_status && (
                          <span className="text-xs bg-gray-100 px-1.5 py-0.5 rounded">{a.new_status}</span>
                        )}
                      </div>
                      {a.note && <p className="text-gray-600 mt-0.5">{a.note}</p>}
                      <p className="text-xs text-gray-400 mt-0.5">{formatDate(a.created_at)}</p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Right column — 1/3 */}
        <div className="space-y-4">
          {/* Assign Reviewer */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-2">Assigned Reviewer</h3>
            {detail.reviewer_name ? (
              <div className="text-sm">
                <p className="font-medium">{detail.reviewer_name}</p>
                <p className="text-gray-500">{detail.reviewer_role}</p>
              </div>
            ) : (
              <p className="text-sm text-gray-400 mb-2">Unassigned</p>
            )}
            <div className="flex gap-2 mt-3">
              <select value={selectedReviewer} onChange={(e) => setSelectedReviewer(e.target.value)} className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-sm">
                <option value="">Select reviewer...</option>
                {reviewers.map((r) => (
                  <option key={r.reviewer_id} value={r.reviewer_id}>{r.display_name} ({r.role})</option>
                ))}
              </select>
              <button onClick={handleAssign} disabled={!selectedReviewer} className="btn-primary text-sm px-3">Assign</button>
            </div>
          </div>

          {/* Update Status */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-2">Update Status</h3>
            <select value={newStatus} onChange={(e) => setNewStatus(e.target.value)} className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm mb-2">
              <option value="">Select new status...</option>
              {statuses.map((s) => <option key={s} value={s}>{s}</option>)}
            </select>
            <textarea
              placeholder="Optional note..."
              value={statusNote}
              onChange={(e) => setStatusNote(e.target.value)}
              rows={2}
              className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm mb-2"
            />
            <button onClick={handleStatusChange} disabled={!newStatus} className="btn-primary text-sm w-full">
              {newStatus === "Approved" ? <><CheckCircle size={14} className="inline mr-1" />Approve</> :
               newStatus === "Denied" ? <><XCircle size={14} className="inline mr-1" />Deny</> :
               "Update Status"}
            </button>
          </div>

          {/* Add Note */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-2">Add Note</h3>
            <textarea
              placeholder="Add a review note..."
              value={note}
              onChange={(e) => setNote(e.target.value)}
              rows={3}
              className="w-full border border-gray-300 rounded-md px-2 py-1.5 text-sm mb-2"
            />
            <button onClick={handleAddNote} disabled={!note.trim()} className="btn-secondary text-sm w-full">Add Note</button>
          </div>

          {/* Timeline */}
          <div className="card">
            <h3 className="font-semibold text-databricks-dark mb-2 flex items-center gap-2">
              <Clock size={14} /> Timeline
            </h3>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Requested</span>
                <span>{formatDate(detail.request_date)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">CMS Deadline</span>
                <span className={isDetermined ? (detail.cms_compliant ? "text-green-600" : "text-red-600 font-medium") : isOverdue ? "text-red-600 font-medium" : ""}>
                  {!isDetermined && isOverdue && <AlertTriangle size={12} className="inline mr-1" />}
                  {formatDate(detail.cms_deadline)}
                </span>
              </div>
              {detail.determination_date && (
                <div className="flex justify-between">
                  <span className="text-gray-500">Determined</span>
                  <span>{formatDate(detail.determination_date)}</span>
                </div>
              )}
              {detail.turnaround_hours && (
                <div className="flex justify-between">
                  <span className="text-gray-500">Turnaround</span>
                  <span>{detail.turnaround_hours.toFixed(1)}h</span>
                </div>
              )}
              <div className="flex justify-between">
                <span className="text-gray-500">CMS Compliant</span>
                <span className={detail.cms_compliant ? "text-green-600" : "text-red-600"}>
                  {detail.cms_compliant ? "Yes" : "No"}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
