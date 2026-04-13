import { useState, useEffect } from "react";
import { Sidebar } from "@/components/Sidebar";
import { Dashboard } from "@/pages/Dashboard";
import { ReviewQueue } from "@/pages/ReviewQueue";
import { RequestDetail } from "@/pages/RequestDetail";
import { PolicyLibrary } from "@/pages/PolicyLibrary";
import { AgentChat } from "@/pages/AgentChat";
import { CaseloadView } from "@/pages/CaseloadView";
import { api } from "@/lib/api";

export default function App() {
  const [page, setPage] = useState("dashboard");
  const [selectedReqId, setSelectedReqId] = useState<string | null>(null);
  const [pendingCount, setPendingCount] = useState(0);

  useEffect(() => {
    api.getDashboardStats().then((s) => setPendingCount(s.pending_count)).catch(() => {});
  }, [page]);

  const handleSelectRequest = (id: string) => {
    setSelectedReqId(id);
    setPage("request-detail");
  };

  const handleBackFromDetail = () => {
    setSelectedReqId(null);
    setPage("queue");
  };

  const renderPage = () => {
    switch (page) {
      case "dashboard":
        return <Dashboard onSelectRequest={handleSelectRequest} />;
      case "queue":
        return <ReviewQueue onSelectRequest={handleSelectRequest} />;
      case "request-detail":
        return selectedReqId ? (
          <RequestDetail
            requestId={selectedReqId}
            onBack={handleBackFromDetail}
          />
        ) : (
          <ReviewQueue onSelectRequest={handleSelectRequest} />
        );
      case "policies":
        return <PolicyLibrary />;
      case "agent":
        return <AgentChat />;
      case "caseload":
        return <CaseloadView />;
      default:
        return <Dashboard onSelectRequest={handleSelectRequest} />;
    }
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar
        activePage={page === "request-detail" ? "queue" : page}
        onNavigate={setPage}
        pendingCount={pendingCount}
      />
      <main className="flex-1 p-8 overflow-y-auto">{renderPage()}</main>
    </div>
  );
}
