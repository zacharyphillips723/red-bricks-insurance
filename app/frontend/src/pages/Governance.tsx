import { useState, useEffect, useCallback } from "react";
import { ShieldCheck, Loader2, Lock, Unlock, RefreshCw, EyeOff } from "lucide-react";
import { api } from "@/lib/api";
import type { GovernanceStatus } from "@/lib/api";

const DISPLAY_COLS = [
  { key: "member_id", label: "Member ID", phi: false },
  { key: "member_name", label: "Name", phi: true },
  { key: "date_of_birth", label: "DOB", phi: true },
  { key: "age", label: "Age", phi: false },
  { key: "gender", label: "Gender", phi: false },
  { key: "address", label: "Address", phi: true },
  { key: "phone", label: "Phone", phi: true },
  { key: "email", label: "Email", phi: true },
  { key: "risk_tier", label: "Risk Tier", phi: false },
];

export function Governance() {
  const [status, setStatus] = useState<GovernanceStatus | null>(null);
  const [members, setMembers] = useState<Record<string, string>[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async (isRefresh = false) => {
    if (isRefresh) setRefreshing(true);
    try {
      const [s, m] = await Promise.all([
        api.getGovernanceStatus().catch(() => null),
        api.getGovernedMembers(15).catch(() => ({ members: [] })),
      ]);
      setStatus(s);
      setMembers(m.members || []);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);
  useEffect(() => { load(); }, [load]);

  if (loading)
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );

  const unmasked = status?.sees_unmasked === true;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-800 flex items-center gap-2">
          <ShieldCheck className="w-6 h-6 text-databricks-red" /> PHI Governance
        </h2>
        <p className="text-sm text-gray-500 mt-0.5">
          Unity Catalog column masking on the governed member view. What you see below depends on the querying identity's group membership — the same policy enforces on every query, from this app to a notebook to a BI dashboard.
        </p>
      </div>

      {/* Access banner */}
      <div className={`card p-5 flex items-start gap-4 ${unmasked ? "border-l-4 border-l-green-500" : "border-l-4 border-l-amber-500"}`}>
        {unmasked ? <Unlock className="w-6 h-6 text-green-600 mt-0.5" /> : <Lock className="w-6 h-6 text-amber-600 mt-0.5" />}
        <div className="flex-1">
          <div className="font-semibold text-gray-800">{status?.access_level}</div>
          <div className="text-sm text-gray-500 mt-0.5">
            Querying as <span className="font-mono text-xs bg-gray-100 px-1.5 py-0.5 rounded">{status?.current_identity || "unknown"}</span>
          </div>
          <div className="text-xs text-gray-400 mt-2">
            Membership in <span className="font-mono">{status?.unmask_group}</span> unlocks unmasked PHI. Add the app service principal (or an on-behalf-of user) to that account group to flip the view live during a demo.
          </div>
        </div>
        <button onClick={() => load(true)} disabled={refreshing}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-gray-600 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50">
          <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? "animate-spin" : ""}`} />Refresh
        </button>
      </div>

      {/* Masked columns chips */}
      <div className="card p-5">
        <div className="text-sm font-medium text-gray-600 mb-2 flex items-center gap-2">
          <EyeOff className="w-4 h-4 text-gray-400" /> Governed PHI Columns
        </div>
        <div className="flex flex-wrap gap-2">
          {(status?.masked_columns || []).map((c) => (
            <span key={c} className="px-2.5 py-1 rounded-full text-xs font-medium bg-red-50 text-databricks-red border border-red-100">
              {c}
            </span>
          ))}
        </div>
        <div className="text-xs text-gray-400 mt-3 font-mono break-all">{status?.governed_view}</div>
      </div>

      {/* Live governed data */}
      <div className="card">
        <div className="p-4 border-b border-gray-200">
          <h3 className="font-semibold text-gray-800">Governed Member View — Live</h3>
          <p className="text-xs text-gray-400 mt-0.5">Rows below are returned through the masking policy in real time.</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50"><tr>
              {DISPLAY_COLS.map((col) => (
                <th key={col.key} className="px-3 py-2.5 text-left font-medium text-gray-600 whitespace-nowrap">
                  {col.label}
                  {col.phi && <Lock className="w-3 h-3 inline-block ml-1 text-gray-300" />}
                </th>
              ))}
            </tr></thead>
            <tbody className="divide-y divide-gray-100">
              {members.map((m, i) => (
                <tr key={i}>
                  {DISPLAY_COLS.map((col) => (
                    <td key={col.key} className={`px-3 py-2.5 whitespace-nowrap ${col.phi && !unmasked ? "font-mono text-gray-400" : "text-gray-700"}`}>
                      {m[col.key] ?? "—"}
                    </td>
                  ))}
                </tr>
              ))}
              {members.length === 0 && <tr><td colSpan={DISPLAY_COLS.length} className="px-4 py-8 text-center text-gray-400">No member data available.</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
