import { useEffect, useState } from "react";
import { RefreshCw, GitBranch, Check, UploadCloud, Plus, FileClock } from "lucide-react";
import { api, FactorVersion } from "@/lib/api";
import { formatDateTime } from "@/lib/utils";

const STATUS_BADGE: Record<string, string> = {
  draft: "badge-neutral",
  approved: "badge-warning",
  published: "badge-positive",
  archived: "bg-gray-200 text-gray-600 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium",
};

export default function FactorGovernance() {
  const [versions, setVersions] = useState<FactorVersion[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [draftNotes, setDraftNotes] = useState("");

  const refresh = () => {
    api
      .listFactorVersions()
      .then(setVersions)
      .catch(() => setVersions([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const createDraft = async () => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const v = await api.createFactorVersion({ notes: draftNotes || undefined, source: "snapshot" });
      setNote(`Created draft version ${v.version} from the current factor snapshot.`);
      setDraftNotes("");
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create draft");
    } finally {
      setBusy(false);
    }
  };

  const approve = async (id: string) => {
    setBusy(true);
    setError(null);
    try {
      await api.approveFactorVersion(id);
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to approve");
    } finally {
      setBusy(false);
    }
  };

  const publish = async (id: string) => {
    setBusy(true);
    setError(null);
    setNote(null);
    try {
      const res = await api.publishFactorVersion(id);
      if (res.published) {
        setNote(`Published ${res.rows_written} factor rows to the governed Unity Catalog table.`);
      } else {
        setError(res.error || "Publish failed");
      }
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to publish");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="p-8 space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-databricks-dark">Factor Governance</h1>
        <p className="text-sm text-gray-500 mt-1">
          Actuaries govern rating factors through a review → approve → publish workflow. Publishing an
          approved version writes to the governed Unity Catalog table the pricing engine reads — the
          system computes, actuaries govern.
        </p>
      </div>

      {/* New draft */}
      <div className="card">
        <div className="flex items-center gap-3 mb-4">
          <Plus className="w-5 h-5 text-databricks-red" />
          <h2 className="text-lg font-semibold text-databricks-dark">New Factor Version</h2>
        </div>
        <p className="text-sm text-gray-500 mb-3">
          Snapshot the current factor set into a new draft version for review.
        </p>
        <div className="flex gap-3 flex-wrap">
          <input
            value={draftNotes}
            onChange={(e) => setDraftNotes(e.target.value)}
            placeholder="Version notes (e.g. 'Q4 trend refresh')"
            className="flex-1 min-w-[240px] px-3 py-2 border border-gray-300 rounded-lg text-sm
                       focus:outline-none focus:ring-2 focus:ring-databricks-red/30 focus:border-databricks-red"
          />
          <button onClick={createDraft} disabled={busy} className="btn-primary flex items-center gap-2">
            {busy ? <RefreshCw className="w-4 h-4 animate-spin" /> : <GitBranch className="w-4 h-4" />}
            Snapshot Draft
          </button>
        </div>
      </div>

      {error && <div className="card border-red-200 bg-red-50 text-sm text-red-700">{error}</div>}
      {note && (
        <div className="card border-green-200 bg-green-50 text-sm text-green-800 flex items-center gap-2">
          <Check className="w-4 h-4" /> {note}
        </div>
      )}

      {/* Version history */}
      <div className="card">
        <div className="flex items-center gap-3 mb-4">
          <FileClock className="w-5 h-5 text-databricks-red" />
          <h2 className="text-lg font-semibold text-databricks-dark">Version History</h2>
        </div>
        {loading ? (
          <div className="flex items-center justify-center py-8">
            <RefreshCw className="w-6 h-6 animate-spin text-databricks-red" />
          </div>
        ) : versions.length === 0 ? (
          <p className="text-sm text-gray-500 py-6 text-center">
            No factor versions yet. Snapshot a draft above to start the governance trail.
          </p>
        ) : (
          <div className="space-y-3">
            {versions.map((v) => (
              <div key={v.version_id} className="border border-gray-100 rounded-lg p-4">
                <div className="flex items-start justify-between gap-4 flex-wrap">
                  <div>
                    <div className="font-medium text-databricks-dark">
                      Version {v.version}
                      <span className="text-gray-400 font-normal"> · {v.factors.length} factors · {v.source}</span>
                    </div>
                    <div className="text-xs text-gray-500 mt-1">
                      created by {v.created_by}
                      {v.created_at ? ` · ${formatDateTime(v.created_at)}` : ""}
                      {v.approved_by ? ` · approved by ${v.approved_by}` : ""}
                      {v.published_by ? ` · published by ${v.published_by}` : ""}
                    </div>
                    {v.notes && <div className="text-xs text-gray-500 mt-1 italic">“{v.notes}”</div>}
                    <button
                      onClick={() => setExpanded(expanded === v.version_id ? null : v.version_id)}
                      className="text-xs text-databricks-red mt-2 hover:underline"
                    >
                      {expanded === v.version_id ? "Hide factors" : "Show factors"}
                    </button>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className={STATUS_BADGE[v.status] || "badge-neutral"}>{v.status}</span>
                    {v.status === "draft" && (
                      <button
                        onClick={() => approve(v.version_id)}
                        disabled={busy}
                        className="btn-secondary text-sm flex items-center gap-1 py-1.5"
                      >
                        <Check className="w-4 h-4" /> Approve
                      </button>
                    )}
                    {v.status === "approved" && (
                      <button
                        onClick={() => publish(v.version_id)}
                        disabled={busy}
                        className="btn-primary text-sm flex items-center gap-1 py-1.5"
                      >
                        <UploadCloud className="w-4 h-4" /> Publish
                      </button>
                    )}
                  </div>
                </div>
                {expanded === v.version_id && (
                  <div className="mt-3 overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-gray-200 text-left text-gray-500">
                          <th className="py-1.5 pr-4 font-medium">Factor Type</th>
                          <th className="py-1.5 px-4 font-medium">Key</th>
                          <th className="py-1.5 pl-4 font-medium text-right">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {v.factors.map((f, i) => (
                          <tr key={i} className="border-b border-gray-50">
                            <td className="py-1.5 pr-4">{f.factor_type}</td>
                            <td className="py-1.5 px-4">{f.factor_key}</td>
                            <td className="py-1.5 pl-4 text-right font-mono">{f.factor_value}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
