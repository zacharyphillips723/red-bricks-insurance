import { useEffect, useState } from "react";
import { Users, AlertTriangle, Phone, CalendarCheck } from "lucide-react";
import { api, type CareManagerCaseload } from "@/lib/api";

export function Caseload() {
  const [caseloads, setCaseloads] = useState<CareManagerCaseload[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getCaseloadDashboard().then(setCaseloads).finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
        <Users className="w-6 h-6 text-databricks-red" /> Care Manager Caseload
      </h2>

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="card p-6 animate-pulse">
              <div className="h-5 bg-gray-200 rounded w-32 mb-3" />
              <div className="h-4 bg-gray-200 rounded w-20 mb-4" />
              <div className="h-2 bg-gray-200 rounded w-full mb-4" />
              <div className="grid grid-cols-3 gap-2">
                {[...Array(3)].map((_, j) => (
                  <div key={j} className="h-12 bg-gray-200 rounded" />
                ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {caseloads.map((cm) => {
            const utilization =
              cm.max_caseload > 0
                ? Math.round((cm.active_cases / cm.max_caseload) * 100)
                : 0;
            const barColor =
              utilization > 90
                ? "bg-red-500"
                : utilization > 70
                ? "bg-amber-500"
                : "bg-green-500";

            return (
              <div key={cm.care_manager_id} className="card p-6">
                <div className="flex items-center justify-between mb-1">
                  <h3 className="font-semibold text-databricks-dark">{cm.display_name}</h3>
                  <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full">
                    {cm.role}
                  </span>
                </div>

                {/* Utilization bar */}
                <div className="mb-4">
                  <div className="flex justify-between text-xs text-gray-500 mb-1">
                    <span>{cm.active_cases} / {cm.max_caseload} cases</span>
                    <span>{utilization}%</span>
                  </div>
                  <div className="w-full bg-gray-100 rounded-full h-2">
                    <div
                      className={`${barColor} h-2 rounded-full transition-all`}
                      style={{ width: `${Math.min(utilization, 100)}%` }}
                    />
                  </div>
                </div>

                {/* Metrics */}
                <div className="grid grid-cols-3 gap-2">
                  <div className="bg-red-50 rounded-lg p-2 text-center">
                    <AlertTriangle className="w-4 h-4 text-red-600 mx-auto mb-1" />
                    <p className="text-lg font-bold text-red-700">{cm.critical_cases}</p>
                    <p className="text-[10px] text-gray-500 uppercase">Critical</p>
                  </div>
                  <div className="bg-indigo-50 rounded-lg p-2 text-center">
                    <Phone className="w-4 h-4 text-indigo-600 mx-auto mb-1" />
                    <p className="text-lg font-bold text-indigo-700">{cm.pending_outreach}</p>
                    <p className="text-[10px] text-gray-500 uppercase">Outreach</p>
                  </div>
                  <div className="bg-cyan-50 rounded-lg p-2 text-center">
                    <CalendarCheck className="w-4 h-4 text-cyan-600 mx-auto mb-1" />
                    <p className="text-lg font-bold text-cyan-700">{cm.pending_followup}</p>
                    <p className="text-[10px] text-gray-500 uppercase">Follow-Up</p>
                  </div>
                </div>

                {cm.available_capacity > 0 && (
                  <p className="mt-3 text-xs text-green-600 font-medium">
                    {cm.available_capacity} available slots
                  </p>
                )}
                {cm.available_capacity <= 0 && (
                  <p className="mt-3 text-xs text-red-600 font-medium">
                    At capacity
                  </p>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
