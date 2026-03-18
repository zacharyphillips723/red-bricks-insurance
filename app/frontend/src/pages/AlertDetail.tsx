import { useEffect, useState } from "react";
import {
  ArrowLeft, UserPlus, Send, MessageSquarePlus,
  Activity, Pill, Building2, Calendar,
} from "lucide-react";
import { api, type AlertDetail as AlertDetailType, type CareManager } from "@/lib/api";
import { riskBadgeClass, statusColor, formatDateTime, sourceIcon } from "@/lib/utils";

const STATUS_TRANSITIONS: Record<string, string[]> = {
  Unassigned: ["Assigned"],
  Assigned: ["Outreach Attempted", "Escalated"],
  "Outreach Attempted": ["Outreach Successful", "Closed — Unable to Reach", "Escalated"],
  "Outreach Successful": ["Assessment In Progress"],
  "Assessment In Progress": ["Intervention Active", "Escalated"],
  "Intervention Active": ["Follow-Up Scheduled", "Resolved", "Escalated"],
  "Follow-Up Scheduled": ["Resolved", "Intervention Active"],
  Escalated: ["Intervention Active", "Resolved"],
  Resolved: [],
  "Closed — Unable to Reach": [],
};

interface AlertDetailProps {
  alertId: string;
  onBack: () => void;
}

export function AlertDetailPage({ alertId, onBack }: AlertDetailProps) {
  const [alert, setAlert] = useState<AlertDetailType | null>(null);
  const [careManagers, setCareManagers] = useState<CareManager[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedCM, setSelectedCM] = useState("");
  const [newNote, setNewNote] = useState("");
  const [statusNote, setStatusNote] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    Promise.all([api.getAlert(alertId), api.listCareManagers()])
      .then(([a, cms]) => {
        a.secondary_drivers = a.secondary_drivers || [];
        a.active_medications = a.active_medications || [];
        setAlert(a);
        setCareManagers(cms);
      })
      .catch((err) => console.error("Failed to load alert:", err))
      .finally(() => setLoading(false));
  }, [alertId]);

  const handleAssign = async () => {
    if (!selectedCM) return;
    setSubmitting(true);
    const updated = await api.assignAlert(alertId, selectedCM);
    setAlert(updated);
    setSelectedCM("");
    setSubmitting(false);
  };

  const handleStatusChange = async (newStatus: string) => {
    setSubmitting(true);
    const updated = await api.updateAlertStatus(alertId, newStatus, statusNote || undefined);
    setAlert(updated);
    setStatusNote("");
    setSubmitting(false);
  };

  const handleAddNote = async () => {
    if (!newNote.trim()) return;
    setSubmitting(true);
    const updated = await api.addAlertNote(alertId, newNote);
    setAlert(updated);
    setNewNote("");
    setSubmitting(false);
  };

  if (loading || !alert) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-8 bg-gray-200 rounded w-48" />
        <div className="grid grid-cols-3 gap-6">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="card p-6"><div className="h-32 bg-gray-200 rounded" /></div>
          ))}
        </div>
      </div>
    );
  }

  const nextStatuses = STATUS_TRANSITIONS[alert.status] || [];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <button onClick={onBack} className="btn-secondary p-2">
          <ArrowLeft className="w-4 h-4" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-bold text-databricks-dark">
              {alert.mrn || alert.patient_id.slice(0, 12)}
            </h2>
            <span className={riskBadgeClass(alert.risk_tier)}>{alert.risk_tier}</span>
            <span className={`font-medium text-sm ${statusColor(alert.status)}`}>
              {alert.status}
            </span>
          </div>
          <p className="text-sm text-gray-500 mt-1">
            {sourceIcon(alert.alert_source)} {alert.alert_source} · {alert.primary_driver}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left column: clinical details */}
        <div className="lg:col-span-2 space-y-6">
          {/* Clinical Indicators */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold text-databricks-dark mb-4 flex items-center gap-2">
              <Activity className="w-5 h-5 text-databricks-red" /> Clinical Indicators
            </h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {alert.max_hba1c && (
                <div className="bg-red-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 uppercase tracking-wide">HbA1c</p>
                  <p className="text-xl font-bold text-red-700">{alert.max_hba1c}%</p>
                </div>
              )}
              {alert.max_blood_glucose && (
                <div className="bg-orange-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 uppercase tracking-wide">Blood Glucose</p>
                  <p className="text-xl font-bold text-orange-700">{alert.max_blood_glucose} mg/dL</p>
                </div>
              )}
              {alert.peak_ed_visits_12mo && (
                <div className="bg-purple-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 uppercase tracking-wide">ED Visits (12mo)</p>
                  <p className="text-xl font-bold text-purple-700">{alert.peak_ed_visits_12mo}</p>
                </div>
              )}
              {alert.last_facility && (
                <div className="bg-blue-50 rounded-lg p-3">
                  <p className="text-xs text-gray-500 uppercase tracking-wide">Last Facility</p>
                  <p className="text-sm font-semibold text-blue-700 truncate">{alert.last_facility}</p>
                </div>
              )}
            </div>

            {alert.secondary_drivers && alert.secondary_drivers.length > 0 && (
              <div className="mt-4">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-2">Contributing Factors</p>
                <div className="flex flex-wrap gap-2">
                  {alert.secondary_drivers.map((d, i) => (
                    <span key={i} className="bg-gray-100 text-gray-700 text-xs px-2.5 py-1 rounded-full">
                      {d}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Medications */}
          {alert.active_medications && alert.active_medications.length > 0 && (
            <div className="card p-6">
              <h3 className="text-lg font-semibold text-databricks-dark mb-3 flex items-center gap-2">
                <Pill className="w-5 h-5 text-databricks-red" /> Active Medications
              </h3>
              <div className="flex flex-wrap gap-2">
                {alert.active_medications.map((med, i) => (
                  <span key={i} className="bg-emerald-50 text-emerald-700 text-sm px-3 py-1 rounded-full">
                    {med}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Add Note */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold text-databricks-dark mb-3 flex items-center gap-2">
              <MessageSquarePlus className="w-5 h-5 text-databricks-red" /> Notes
            </h3>
            {alert.notes && (
              <div className="bg-gray-50 rounded-lg p-3 mb-4 text-sm text-gray-700">
                {alert.notes}
              </div>
            )}
            <div className="flex gap-3">
              <textarea
                value={newNote}
                onChange={(e) => setNewNote(e.target.value)}
                placeholder="Add a clinical note..."
                rows={2}
                className="flex-1 rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-databricks-red focus:border-databricks-red resize-none"
              />
              <button
                onClick={handleAddNote}
                disabled={!newNote.trim() || submitting}
                className="btn-primary self-end flex items-center gap-2"
              >
                <Send className="w-4 h-4" /> Add Note
              </button>
            </div>
          </div>

          {/* Activity Log */}
          <div className="card p-6">
            <h3 className="text-lg font-semibold text-databricks-dark mb-4">Activity Log</h3>
            {alert.activity_log.length === 0 ? (
              <p className="text-sm text-gray-400">No activity yet</p>
            ) : (
              <div className="space-y-3">
                {alert.activity_log.map((log) => (
                  <div key={log.activity_id} className="flex gap-3 text-sm">
                    <div className="w-2 h-2 rounded-full bg-databricks-red mt-2 shrink-0" />
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-databricks-dark">
                          {log.care_manager_name || "System"}
                        </span>
                        <span className="text-gray-400">·</span>
                        <span className="text-gray-400">{formatDateTime(log.created_at)}</span>
                      </div>
                      <p className="text-gray-600">
                        {log.activity_type === "status_change" &&
                          `Status changed: ${log.previous_status} → ${log.new_status}`}
                        {log.activity_type === "assignment" && `Assigned (${log.previous_status} → ${log.new_status})`}
                        {log.activity_type === "note_added" && "Note added"}
                        {log.activity_type === "escalation" && "Escalated"}
                      </p>
                      {log.note && (
                        <p className="text-gray-500 italic mt-1">"{log.note}"</p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Right column: actions */}
        <div className="space-y-6">
          {/* Patient Info */}
          <div className="card p-6">
            <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Patient Info</h3>
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between">
                <dt className="text-gray-500">MRN</dt>
                <dd className="font-medium">{alert.mrn || "—"}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-500">Member ID</dt>
                <dd className="font-medium">{alert.member_id || "—"}</dd>
              </div>
              <div className="flex justify-between">
                <dt className="text-gray-500">Payer</dt>
                <dd className="font-medium">{alert.payer || "—"}</dd>
              </div>
              {alert.last_encounter_date && (
                <div className="flex justify-between">
                  <dt className="text-gray-500">Last Encounter</dt>
                  <dd className="font-medium">{formatDateTime(alert.last_encounter_date)}</dd>
                </div>
              )}
            </dl>
          </div>

          {/* Assign */}
          {(!alert.assigned_care_manager_id || alert.status === "Unassigned") && (
            <div className="card p-6">
              <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3 flex items-center gap-2">
                <UserPlus className="w-4 h-4" /> Assign Care Manager
              </h3>
              <select
                value={selectedCM}
                onChange={(e) => setSelectedCM(e.target.value)}
                className="w-full rounded-lg border border-gray-300 text-sm py-2 px-3 mb-3 focus:ring-databricks-red focus:border-databricks-red"
              >
                <option value="">Select care manager...</option>
                {careManagers.map((cm) => (
                  <option key={cm.care_manager_id} value={cm.care_manager_id}>
                    {cm.display_name} ({cm.role})
                  </option>
                ))}
              </select>
              <button
                onClick={handleAssign}
                disabled={!selectedCM || submitting}
                className="btn-primary w-full"
              >
                {submitting ? "Assigning..." : "Assign & Claim"}
              </button>
            </div>
          )}

          {/* Current Assignment */}
          {alert.care_manager_name && (
            <div className="card p-6">
              <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">Assigned To</h3>
              <p className="font-medium text-databricks-dark">{alert.care_manager_name}</p>
              <p className="text-sm text-gray-500">{alert.care_manager_role}</p>
              {alert.assigned_at && (
                <p className="text-xs text-gray-400 mt-1">Since {formatDateTime(alert.assigned_at)}</p>
              )}
            </div>
          )}

          {/* Status Update */}
          {nextStatuses.length > 0 && (
            <div className="card p-6">
              <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-3">
                Update Status
              </h3>
              <textarea
                value={statusNote}
                onChange={(e) => setStatusNote(e.target.value)}
                placeholder="Optional note for status change..."
                rows={2}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm mb-3 focus:ring-databricks-red focus:border-databricks-red resize-none"
              />
              <div className="space-y-2">
                {nextStatuses.map((s) => (
                  <button
                    key={s}
                    onClick={() => handleStatusChange(s)}
                    disabled={submitting}
                    className={`w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-colors border ${
                      s === "Escalated"
                        ? "border-red-300 text-red-700 hover:bg-red-50"
                        : s === "Resolved"
                        ? "border-green-300 text-green-700 hover:bg-green-50"
                        : "border-gray-300 text-gray-700 hover:bg-gray-50"
                    }`}
                  >
                    → {s}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
