import { useState, useEffect, useCallback } from "react";
import { Sidebar } from "@/components/Sidebar";
import { Dashboard } from "@/pages/Dashboard";
import { AlertQueue } from "@/pages/AlertQueue";
import { AlertDetailPage } from "@/pages/AlertDetail";
import { GenieSearch } from "@/pages/GenieSearch";
import { Caseload } from "@/pages/Caseload";
import { Member360 } from "@/pages/Member360";
import { CarePlan } from "@/pages/CarePlan";
import { OutreachDraft } from "@/pages/OutreachDraft";
import { CohortBuilder } from "@/pages/CohortBuilder";
import { ToastNotifications } from "@/components/ToastNotifications";
import { useNotifications } from "@/lib/useNotifications";
import { api } from "@/lib/api";

export default function App() {
  const [page, setPage] = useState("dashboard");
  const [selectedAlertId, setSelectedAlertId] = useState<string | null>(null);
  const [unassignedCount, setUnassignedCount] = useState(0);

  const refreshCounts = useCallback(() => {
    api.getDashboardStats().then((s) => setUnassignedCount(s.unassigned_count)).catch(() => {});
  }, []);

  useEffect(() => {
    refreshCounts();
  }, [page, refreshCounts]);

  const { notifications, dismissNotification } = useNotifications({
    onNotification: () => {
      // Refresh badge counts when any notification arrives
      refreshCounts();
    },
  });

  const handleSelectAlert = (id: string) => {
    setSelectedAlertId(id);
    setPage("alert-detail");
  };

  const handleBackFromDetail = () => {
    setSelectedAlertId(null);
    setPage("alerts");
  };

  const handleToastClick = (alertId: string) => {
    setSelectedAlertId(alertId);
    setPage("alert-detail");
  };

  const renderPage = () => {
    switch (page) {
      case "dashboard":
        return <Dashboard />;
      case "alerts":
        return <AlertQueue onSelectAlert={handleSelectAlert} />;
      case "alert-detail":
        return selectedAlertId ? (
          <AlertDetailPage alertId={selectedAlertId} onBack={handleBackFromDetail} />
        ) : (
          <AlertQueue onSelectAlert={handleSelectAlert} />
        );
      case "member360":
        return <Member360 />;
      case "careplan":
        return <CarePlan />;
      case "outreach":
        return <OutreachDraft />;
      case "cohorts":
        return <CohortBuilder />;
      case "genie":
        return <GenieSearch />;
      case "caseload":
        return <Caseload />;
      default:
        return <Dashboard />;
    }
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar
        activePage={page === "alert-detail" ? "alerts" : page}
        onNavigate={setPage}
        unassignedCount={unassignedCount}
      />
      <main className="flex-1 p-8 overflow-y-auto">{renderPage()}</main>
      <ToastNotifications
        notifications={notifications}
        onDismiss={dismissNotification}
        onClickAlert={handleToastClick}
      />
    </div>
  );
}
