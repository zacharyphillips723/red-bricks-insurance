import { useState, useEffect } from "react";
import { ErrorBoundary } from "./components/ErrorBoundary";
import Sidebar from "./components/Sidebar";
import PersonaBar from "./components/PersonaBar";
import Dashboard from "./pages/Dashboard";
import SimulationBuilder from "./pages/SimulationBuilder";
import Scenarios from "./pages/Scenarios";
import Intake from "./pages/Intake";
import FundingQuote from "./pages/FundingQuote";
import Approvals from "./pages/Approvals";
import Pipeline from "./pages/Pipeline";
import FactorGovernance from "./pages/FactorGovernance";
import DigitalTwin from "./pages/DigitalTwin";
import ScenarioComparison from "./pages/ScenarioComparison";
import SimulationHistory from "./pages/SimulationHistory";
import Agent from "./pages/Agent";
import RateBuildup from "./pages/RateBuildup";
import RiskPool from "./pages/RiskPool";
import Governance from "./pages/Governance";
import Observability from "./pages/Observability";
import Genie from "./pages/Genie";
import { useHashRouter } from "./lib/useHashRouter";
import { api } from "./lib/api";
import {
  Page,
  PersonaId,
  getStoredPersona,
  storePersona,
  visiblePages,
} from "./lib/personas";

const ALL_PAGES: Page[] = [
  "dashboard",
  "intake",
  "builder",
  "scenarios",
  "funding",
  "approvals",
  "pipeline",
  "comparison",
  "history",
  "agent",
  "rate-buildup",
  "risk-pool",
  "factors",
  "twin",
  "governance",
  "observability",
  "genie",
];

export default function App() {
  const [page, setPage] = useHashRouter<Page>("dashboard");
  const [persona, setPersonaState] = useState<PersonaId>(getStoredPersona);
  const [savedCount, setSavedCount] = useState(0);

  const refreshSavedCount = () => {
    api.listSimulations().then((sims) => setSavedCount(sims.length)).catch(() => {});
  };

  useEffect(() => {
    refreshSavedCount();
  }, []);

  const allowed = visiblePages(persona, ALL_PAGES);

  const setPersona = (id: PersonaId) => {
    setPersonaState(id);
    storePersona(id);
    // If the current page is hidden for the new persona, fall back to dashboard.
    if (!visiblePages(id, ALL_PAGES).has(page)) setPage("dashboard");
  };

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
        <Sidebar
          currentPage={page}
          onNavigate={setPage}
          savedCount={savedCount}
          visiblePages={allowed}
        />
        <main className="flex-1 overflow-auto">
          <PersonaBar persona={persona} onChange={setPersona} />
          {page === "dashboard" && (
            <Dashboard onNavigateToBuilder={navigateToBuilder} />
          )}
          {page === "builder" && (
            <SimulationBuilder onSaved={refreshSavedCount} />
          )}
          {page === "intake" && <Intake />}
          {page === "scenarios" && <Scenarios />}
          {page === "funding" && <FundingQuote />}
          {page === "approvals" && <Approvals />}
          {page === "pipeline" && <Pipeline />}
          {page === "factors" && <FactorGovernance />}
          {page === "twin" && <DigitalTwin />}
          {page === "rate-buildup" && <RateBuildup />}
          {page === "risk-pool" && <RiskPool />}
          {page === "comparison" && <ScenarioComparison />}
          {page === "history" && (
            <SimulationHistory onCountChange={setSavedCount} />
          )}
          {page === "agent" && <Agent />}
          {page === "genie" && <Genie />}
          {page === "governance" && <Governance />}
          {page === "observability" && <Observability />}
        </main>
      </div>
    </ErrorBoundary>
  );
}
