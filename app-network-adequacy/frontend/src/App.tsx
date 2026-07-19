import { ErrorBoundary } from "@/components/ErrorBoundary";
import { Sidebar } from "@/components/Sidebar";
import { Dashboard } from "@/pages/Dashboard";
import { CompliancePage } from "@/pages/CompliancePage";
import { GhostNetworkPage } from "@/pages/GhostNetworkPage";
import { LeakagePage } from "@/pages/LeakagePage";
import { GapsRecruitmentPage } from "@/pages/GapsRecruitmentPage";
import { GeniePage } from "@/pages/GeniePage";
import { MapPage } from "@/pages/MapPage";
import { NetworkAgentPage } from "@/pages/NetworkAgentPage";
import { ObservabilityPage } from "@/pages/ObservabilityPage";
import { useHashRouter } from "@/lib/useHashRouter";

export default function App() {
  const [page, setPage] = useHashRouter("dashboard");

  const renderPage = () => {
    switch (page) {
      case "dashboard":
        return <Dashboard onNavigate={setPage} />;
      case "map":
        return <MapPage />;
      case "compliance":
        return <CompliancePage />;
      case "ghost-network":
        return <GhostNetworkPage />;
      case "leakage":
        return <LeakagePage />;
      case "gaps":
        return <GapsRecruitmentPage />;
      case "genie":
        return <GeniePage />;
      case "agent":
        return <NetworkAgentPage />;
      case "observability":
        return <ObservabilityPage />;
      default:
        return <Dashboard onNavigate={setPage} />;
    }
  };

  return (
    <ErrorBoundary>
      <div className="flex min-h-screen">
        <Sidebar activePage={page} onNavigate={setPage} />
        <main className="flex-1 p-8 overflow-y-auto">{renderPage()}</main>
      </div>
    </ErrorBoundary>
  );
}
