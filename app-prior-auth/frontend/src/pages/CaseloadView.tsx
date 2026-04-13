import { useState, useEffect } from "react";
import { api, ReviewerCaseload } from "@/lib/api";
import { Users, Zap } from "lucide-react";

export function CaseloadView() {
  const [caseload, setCaseload] = useState<ReviewerCaseload[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getReviewerCaseload().then(setCaseload).catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-4">
        <h2 className="text-2xl font-bold text-databricks-dark">Reviewer Caseload</h2>
        <div className="grid grid-cols-3 gap-4">
          {Array.from({ length: 6 }).map((_, i) => <div key={i} className="card h-40 animate-pulse bg-gray-100" />)}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
        <Users size={24} /> Reviewer Caseload
      </h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {caseload.map((r) => {
          const utilization = r.max_caseload > 0 ? r.active_cases / r.max_caseload : 0;
          const utilizationColor =
            utilization > 0.85 ? "bg-red-500" :
            utilization > 0.6 ? "bg-amber-500" : "bg-green-500";

          return (
            <div key={r.reviewer_id} className="card">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h3 className="font-semibold text-databricks-dark">{r.display_name}</h3>
                  <p className="text-xs text-gray-500">{r.role}{r.specialty ? ` — ${r.specialty}` : ""}</p>
                </div>
                <span className="text-sm font-medium text-gray-600">
                  {r.active_cases}/{r.max_caseload}
                </span>
              </div>

              {/* Utilization bar */}
              <div className="w-full bg-gray-100 rounded-full h-2 mb-4">
                <div
                  className={`h-2 rounded-full ${utilizationColor}`}
                  style={{ width: `${Math.min(utilization * 100, 100)}%` }}
                />
              </div>

              <div className="grid grid-cols-2 gap-2 text-sm">
                <div className="flex items-center gap-1">
                  <Zap size={12} className="text-red-500" />
                  <span className="text-gray-500">Expedited:</span>
                  <span className="font-medium">{r.expedited_cases}</span>
                </div>
                <div>
                  <span className="text-gray-500">In Review:</span>{" "}
                  <span className="font-medium">{r.in_review}</span>
                </div>
                <div>
                  <span className="text-gray-500">Awaiting Info:</span>{" "}
                  <span className="font-medium">{r.awaiting_info}</span>
                </div>
                <div>
                  <span className="text-gray-500">Available:</span>{" "}
                  <span className={`font-medium ${r.available_capacity <= 5 ? "text-red-600" : "text-green-600"}`}>
                    {r.available_capacity}
                  </span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
