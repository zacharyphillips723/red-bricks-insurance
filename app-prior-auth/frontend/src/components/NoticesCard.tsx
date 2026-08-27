import { useState, useEffect, useCallback } from "react";
import { api, Correspondence } from "@/lib/api";
import { Mail, ShieldCheck, Send, FileText, CheckCircle } from "lucide-react";
import ReactMarkdown from "react-markdown";

interface NoticesCardProps {
  requestId: string;
  status: string | null;
}

// Suggest the notice type that matches the current determination.
function defaultNoticeType(status: string | null): string {
  if (status === "Approved") return "approval";
  if (status === "Denied") return "denial";
  if (status === "Partially Approved") return "partial_approval";
  if (status === "Additional Info Requested") return "additional_info_request";
  return "denial";
}

const NOTICE_TYPES = [
  { value: "approval", label: "Approval" },
  { value: "denial", label: "Denial (adverse)" },
  { value: "partial_approval", label: "Partial Approval" },
  { value: "additional_info_request", label: "Request Additional Info" },
];

// Multilingual correspondence generation (RFI: Correspondence Management).
const LANGUAGES = [
  { value: "en", label: "English" },
  { value: "es", label: "Spanish" },
  { value: "zh", label: "Chinese" },
  { value: "vi", label: "Vietnamese" },
  { value: "tl", label: "Tagalog" },
  { value: "ru", label: "Russian" },
];

function statusPill(s: string | null): string {
  if (s === "released") return "bg-green-50 text-green-700";
  if (s === "delivered") return "bg-blue-50 text-blue-700";
  if (s === "draft") return "bg-amber-50 text-amber-700";
  return "bg-gray-100 text-gray-600";
}

export function NoticesCard({ requestId, status }: NoticesCardProps) {
  const [notices, setNotices] = useState<Correspondence[]>([]);
  const [noticeType, setNoticeType] = useState(defaultNoticeType(status));
  const [language, setLanguage] = useState("en");
  const [busy, setBusy] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  const load = useCallback(() => {
    api.listNotices(requestId).then(setNotices).catch(console.error);
  }, [requestId]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { setNoticeType(defaultNoticeType(status)); }, [status]);

  const handleGenerate = async () => {
    setBusy(true);
    try {
      const created = await api.generateNotice(requestId, {
        notice_type: noticeType,
        delivery_channel: "portal",
        language,
      });
      setExpanded(created.notice_id);
      load();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const handleRelease = async (noticeId: string) => {
    setBusy(true);
    try {
      await api.releaseNotice(noticeId);
      load();
    } catch (e) {
      alert((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="card">
      <h3 className="font-semibold text-databricks-dark mb-3 flex items-center gap-2">
        <Mail size={16} className="text-blue-500" /> Determination Notices
      </h3>

      {/* Generate */}
      <div className="flex gap-2 mb-4">
        <select
          value={noticeType}
          onChange={(e) => setNoticeType(e.target.value)}
          className="flex-1 border border-gray-300 rounded-md px-2 py-1.5 text-sm"
        >
          {NOTICE_TYPES.map((n) => (
            <option key={n.value} value={n.value}>{n.label}</option>
          ))}
        </select>
        <select
          value={language}
          onChange={(e) => setLanguage(e.target.value)}
          className="border border-gray-300 rounded-md px-2 py-1.5 text-sm"
          title="Correspondence language"
        >
          {LANGUAGES.map((l) => (
            <option key={l.value} value={l.value}>{l.label}</option>
          ))}
        </select>
        <button onClick={handleGenerate} disabled={busy} className="btn-primary text-sm flex items-center gap-1">
          <FileText size={14} /> {busy ? "Generating…" : "Generate"}
        </button>
      </div>

      {notices.length === 0 ? (
        <p className="text-sm text-gray-400">
          No notices generated. Notices include the decision, clinical rationale,
          criteria citation, and appeal rights — with a PHI-redaction gate before release.
        </p>
      ) : (
        <div className="space-y-2">
          {notices.map((n) => (
            <div key={n.notice_id} className="border border-gray-200 rounded-md">
              <div
                className="flex items-center gap-2 p-2.5 cursor-pointer hover:bg-gray-50"
                onClick={() => setExpanded(expanded === n.notice_id ? null : n.notice_id)}
              >
                <span className="text-sm font-medium capitalize flex-1">
                  {n.notice_type.replace(/_/g, " ")}
                </span>
                {n.language && n.language !== "en" && (
                  <span className="text-xs bg-indigo-50 text-indigo-700 px-1.5 py-0.5 rounded uppercase">{n.language}</span>
                )}
                {n.includes_appeal_rights && (
                  <span className="text-xs text-gray-400">appeal rights</span>
                )}
                <span className="flex items-center gap-1 text-xs text-green-600" title={n.redaction_notes || ""}>
                  <ShieldCheck size={12} /> PHI gated
                </span>
                <span className={`px-2 py-0.5 rounded text-xs font-medium ${statusPill(n.delivery_status)}`}>
                  {n.delivery_status}
                </span>
              </div>
              {expanded === n.notice_id && (
                <div className="border-t border-gray-100 p-3 space-y-3">
                  <div className="text-xs text-gray-500">
                    {n.criteria_citation} · template {n.template_version} · {n.redaction_notes}
                  </div>
                  {n.validation_notes && (
                    <div className={`text-xs flex items-center gap-1 ${n.validation_status === "passed" ? "text-green-600" : "text-amber-600"}`}>
                      <ShieldCheck size={12} /> {n.validation_notes}
                    </div>
                  )}
                  <div className="prose prose-sm max-w-none bg-gray-50 rounded p-3 max-h-72 overflow-y-auto">
                    <ReactMarkdown>{n.body_markdown || ""}</ReactMarkdown>
                  </div>
                  {n.delivery_status === "draft" ? (
                    <button
                      onClick={() => handleRelease(n.notice_id)}
                      disabled={busy}
                      className="btn-primary text-sm flex items-center gap-1"
                    >
                      <Send size={14} /> Release via {n.delivery_channel}
                    </button>
                  ) : (
                    <div className="flex items-center gap-1 text-xs text-green-700">
                      <CheckCircle size={14} /> Released
                      {n.released_at && ` · ${new Date(n.released_at).toLocaleString()}`}
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
