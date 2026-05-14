import { useState, useEffect } from "react";
import { ErrorBoundary } from "./components/ErrorBoundary";
import Sidebar from "./components/Sidebar";
import Dashboard from "./pages/Dashboard";
import SimulationBuilder from "./pages/SimulationBuilder";
import ScenarioComparison from "./pages/ScenarioComparison";
import SimulationHistory from "./pages/SimulationHistory";
import Agent from "./pages/Agent";
import RateBuildup from "./pages/RateBuildup";
import RiskPool from "./pages/RiskPool";
import { useHashRouter } from "./lib/useHashRouter";
import { api } from "./lib/api";

type Page = "dashboard" | "builder" | "comparison" | "history" | "agent" | "rate-buildup" | "risk-pool";

export default function App() {
  const [page, setPage] = useHashRouter<Page>("dashboard");
  const [savedCount, setSavedCount] = useState(0);

  const refreshSavedCount = () => {
    api.listSimulations().then((sims) => setSavedCount(sims.length)).catch(() => {});
  };

  useEffect(() => {
    refreshSavedCount();
  }, []);

  const navigateToBuilder = (simType?: string) => {
    setPage("builder");
    // If simType provided, the builder will pick it up via a shared mechanism
    if (simType) {
      window.dispatchEvent(new CustomEvent("select-sim-type", { detail: simType }));
    }
  };

  return (
    <ErrorBoundary>
      <div className="flex min-h-screen">
        <Sidebar currentPage={page} onNavigate={setPage} savedCount={savedCount} />
        <main className="flex-1 overflow-auto">
          {page === "dashboard" && (
            <Dashboard onNavigateToBuilder={navigateToBuilder} />
          )}
          {page === "builder" && (
            <SimulationBuilder onSaved={refreshSavedCount} />
          )}
          {page === "rate-buildup" && <RateBuildup />}
          {page === "risk-pool" && <RiskPool />}
          {page === "comparison" && <ScenarioComparison />}
          {page === "history" && (
            <SimulationHistory onCountChange={setSavedCount} />
          )}
          {page === "agent" && <Agent />}
        </main>
      </div>
    </ErrorBoundary>
  );
}
