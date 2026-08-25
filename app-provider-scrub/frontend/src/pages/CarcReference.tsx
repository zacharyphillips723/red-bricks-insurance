import { useState, useEffect } from "react";
import { BookOpen, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import type { CarcReference as CarcRef } from "@/lib/api";

export function CarcReference() {
  const [rows, setRows] = useState<CarcRef[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getCarcReference().then(setRows).catch(() => setRows([])).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-databricks-red animate-spin" />
      </div>
    );
  }

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <BookOpen className="text-databricks-red" /> CARC Reference
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Claim Adjustment Reason Codes the scrubber predicts, mapped to reason categories and
          who bears responsibility (payer vs. patient).
        </p>
      </div>

      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left font-medium text-gray-600">CARC</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Group</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Reason Category</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Description</th>
                <th className="px-4 py-3 text-left font-medium text-gray-600">Responsibility</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {rows.map((r) => (
                <tr key={r.carc_code}>
                  <td className="px-4 py-3">
                    <span className="font-mono text-xs font-bold bg-databricks-dark text-white rounded px-2 py-0.5">
                      {r.carc_code}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-500">{r.group_code || "—"}</td>
                  <td className="px-4 py-3">{r.reason_category || "—"}</td>
                  <td className="px-4 py-3 text-gray-600">{r.description || "—"}</td>
                  <td className="px-4 py-3 capitalize text-gray-500">{r.patient_vs_payer || "—"}</td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                    No CARC reference loaded.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
