import { useState, useEffect } from "react";
import Sidebar from "./components/Sidebar";
import Dashboard from "./pages/Dashboard";
import SimulationBuilder from "./pages/SimulationBuilder";
import ScenarioComparison from "./pages/ScenarioComparison";
import SimulationHistory from "./pages/SimulationHistory";
import Agent from "./pages/Agent";
import { api } from "./lib/api";

type Page = "dashboard" | "builder" | "comparison" | "history" | "agent";

export default function App() {
  const [page, setPage] = useState<Page>("dashboard");
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
    <div className="flex min-h-screen">
      <Sidebar currentPage={page} onNavigate={setPage} savedCount={savedCount} />
      <main className="flex-1 overflow-auto">
        {page === "dashboard" && (
          <Dashboard onNavigateToBuilder={navigateToBuilder} />
        )}
        {page === "builder" && (
          <SimulationBuilder onSaved={refreshSavedCount} />
        )}
        {page === "comparison" && <ScenarioComparison />}
        {page === "history" && (
          <SimulationHistory onCountChange={setSavedCount} />
        )}
        {page === "agent" && <Agent />}
      </main>
    </div>
  );
}
