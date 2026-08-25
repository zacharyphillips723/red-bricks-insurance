import { useState } from "react";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { Sidebar } from "@/components/Sidebar";
import { ComposeClaim } from "@/pages/ComposeClaim";
import { ScrubResult } from "@/pages/ScrubResult";
import { DenialIntel } from "@/pages/DenialIntel";
import { ScrubHistory } from "@/pages/ScrubHistory";
import { CarcReference } from "@/pages/CarcReference";
import { Observability } from "@/pages/Observability";
import { useHashRouter } from "@/lib/useHashRouter";
import type { DraftClaim, ScrubResult as ScrubResultType } from "@/lib/api";

export default function App() {
  const [page, setPage] = useHashRouter("compose");
  // Shared scrub state: the latest result + the draft that produced it (for resubmit).
  const [result, setResult] = useState<ScrubResultType | null>(null);
  const [draft, setDraft] = useState<DraftClaim | null>(null);

  const handleScrubComplete = (res: ScrubResultType, d: DraftClaim) => {
    setResult(res);
    setDraft(d);
    setPage("result");
  };

  const renderPage = () => {
    switch (page) {
      case "compose":
        return <ComposeClaim initialDraft={draft} onScrubComplete={handleScrubComplete} />;
      case "result":
        return (
          <ScrubResult
            result={result}
            draft={draft}
            onResult={setResult}
            onDraftChange={setDraft}
            onEditDraft={() => setPage("compose")}
            onGoCompose={() => { setDraft(null); setPage("compose"); }}
          />
        );
      case "intel":
        return <DenialIntel />;
      case "history":
        return <ScrubHistory />;
      case "reference":
        return <CarcReference />;
      case "observability":
        return <Observability />;
      default:
        return <ComposeClaim onScrubComplete={handleScrubComplete} />;
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
