import { useEffect, useState } from "react";
import { Users, Loader2 } from "lucide-react";
import { api, type InvestigatorCaseload } from "@/lib/api";
import { formatCurrency } from "@/lib/utils";

export function CaseloadView() {
  const [caseload, setCaseload] = useState<InvestigatorCaseload[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getInvestigatorCaseload().then(setCaseload).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <Users className="w-6 h-6 text-databricks-red" /> Investigator Caseload
        </h2>
        <div className="card p-8 text-center text-gray-500">
          <Loader2 className="w-6 h-6 animate-spin mx-auto" />
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
        <Users className="w-6 h-6 text-databricks-red" /> Investigator Caseload
      </h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {caseload.map((inv) => {
          const utilization = inv.max_caseload > 0 ? inv.active_cases / inv.max_caseload : 0;
          const barColor =
            utilization > 0.85 ? "bg-red-500" :
            utilization > 0.6 ? "bg-amber-500" : "bg-green-500";

          return (
            <div key={inv.investigator_id} className="card p-5">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <h3 className="font-semibold text-databricks-dark">{inv.display_name}</h3>
                  <p className="text-xs text-gray-400">{inv.role}</p>
                </div>
                <span className="text-sm font-bold text-databricks-dark">
                  {inv.active_cases}/{inv.max_caseload}
                </span>
              </div>

              {/* Utilization bar */}
              <div className="w-full bg-gray-100 rounded-full h-2 mb-3">
                <div
                  className={`${barColor} h-2 rounded-full transition-all`}
                  style={{ width: `${Math.min(utilization * 100, 100)}%` }}
                />
              </div>

              <div className="grid grid-cols-2 gap-2 text-sm">
                <div>
                  <span className="text-gray-500 text-xs">Critical</span>
                  <p className="font-medium text-red-600">{inv.critical_cases}</p>
                </div>
                <div>
                  <span className="text-gray-500 text-xs">Evidence Gathering</span>
                  <p className="font-medium">{inv.evidence_gathering}</p>
                </div>
                <div>
                  <span className="text-gray-500 text-xs">Recovery In Progress</span>
                  <p className="font-medium">{inv.recovery_in_progress}</p>
                </div>
                <div>
                  <span className="text-gray-500 text-xs">Available</span>
                  <p className="font-medium text-green-600">{inv.available_capacity}</p>
                </div>
              </div>

              <div className="mt-3 pt-3 border-t border-gray-100 flex justify-between text-xs text-gray-500">
                <span>Active Overpayment: {formatCurrency(inv.total_active_overpayment)}</span>
                <span>Recovered: {formatCurrency(inv.total_recovered)}</span>
              </div>
            </div>
          );
        })}
      </div>

      {caseload.length === 0 && (
        <div className="card p-8 text-center text-gray-500">
          No investigators found.
        </div>
      )}
    </div>
  );
}
