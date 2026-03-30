import { useState, useEffect } from "react";
import { Sidebar } from "@/components/Sidebar";
import { Dashboard } from "@/pages/Dashboard";
import { InvestigationQueue } from "@/pages/InvestigationQueue";
import { InvestigationDetail } from "@/pages/InvestigationDetail";
import { ProviderAnalysis } from "@/pages/ProviderAnalysis";
import { AgentChat } from "@/pages/AgentChat";
import { CaseloadView } from "@/pages/CaseloadView";
import { api } from "@/lib/api";

export default function App() {
  const [page, setPage] = useState("dashboard");
  const [selectedInvId, setSelectedInvId] = useState<string | null>(null);
  const [selectedNpi, setSelectedNpi] = useState<string | null>(null);
  const [openCount, setOpenCount] = useState(0);

  useEffect(() => {
    api.getDashboardStats().then((s) => setOpenCount(s.open_count)).catch(() => {});
  }, [page]);

  const handleSelectInvestigation = (id: string) => {
    setSelectedInvId(id);
    setPage("investigation-detail");
  };

  const handleViewProvider = (npi: string) => {
    setSelectedNpi(npi);
    setPage("provider-detail");
  };

  const handleBackFromDetail = () => {
    setSelectedInvId(null);
    setPage("investigations");
  };

  const renderPage = () => {
    switch (page) {
      case "dashboard":
        return <Dashboard onSelectInvestigation={handleSelectInvestigation} />;
      case "investigations":
        return <InvestigationQueue onSelectInvestigation={handleSelectInvestigation} />;
      case "investigation-detail":
        return selectedInvId ? (
          <InvestigationDetail
            investigationId={selectedInvId}
            onBack={handleBackFromDetail}
            onViewProvider={handleViewProvider}
          />
        ) : (
          <InvestigationQueue onSelectInvestigation={handleSelectInvestigation} />
        );
      case "providers":
        return <ProviderAnalysis />;
      case "provider-detail":
        return selectedNpi ? (
          <ProviderAnalysis initialNpi={selectedNpi} />
        ) : (
          <ProviderAnalysis />
        );
      case "agent":
        return <AgentChat />;
      case "caseload":
        return <CaseloadView />;
      default:
        return <Dashboard onSelectInvestigation={handleSelectInvestigation} />;
    }
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar
        activePage={
          page === "investigation-detail" ? "investigations" :
          page === "provider-detail" ? "providers" : page
        }
        onNavigate={setPage}
        openCount={openCount}
      />
      <main className="flex-1 p-8 overflow-y-auto">{renderPage()}</main>
    </div>
  );
}
