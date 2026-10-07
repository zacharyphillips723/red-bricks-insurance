// Persona shell — mirrors the "future of underwriting" vision's persona bar.
// Pure frontend state: a persona filters which workbench pages are shown so each
// audience (CFO, Underwriter, AE/Broker, Actuary) sees a focused surface. "All"
// restores the full workbench.

export type Page =
  | "dashboard"
  | "intake"
  | "builder"
  | "scenarios"
  | "funding"
  | "approvals"
  | "pipeline"
  | "comparison"
  | "history"
  | "agent"
  | "rate-buildup"
  | "risk-pool"
  | "factors"
  | "twin"
  | "governance"
  | "observability"
  | "genie";

export type PersonaId = "all" | "cfo" | "underwriter" | "ae" | "actuary";

export interface Persona {
  id: PersonaId;
  label: string;
  blurb: string;
  // null = every page (full workbench). Otherwise the allow-list of visible pages.
  pages: Page[] | null;
}

export const PERSONAS: Persona[] = [
  {
    id: "all",
    label: "All Capabilities",
    blurb: "Full underwriting workbench — every tool enabled.",
    pages: null,
  },
  {
    id: "cfo",
    label: "CFO / Chief Actuary",
    blurb: "Portfolio margin, pipeline, approvals, and model governance.",
    pages: [
      "dashboard", "scenarios", "pipeline", "comparison",
      "approvals", "factors", "twin", "governance", "observability",
    ],
  },
  {
    id: "underwriter",
    label: "Underwriter",
    blurb: "Intake, quote, package scenarios, route funding, and decide.",
    pages: [
      "dashboard",
      "intake",
      "scenarios",
      "funding",
      "pipeline",
      "builder",
      "rate-buildup",
      "risk-pool",
      "comparison",
      "approvals",
      "twin",
      "history",
      "agent",
      "genie",
    ],
  },
  {
    id: "ae",
    label: "AE / Broker",
    blurb: "Intake, client-ready scenarios, funding options, and answers.",
    pages: ["dashboard", "intake", "scenarios", "funding", "agent", "genie"],
  },
  {
    id: "actuary",
    label: "Actuarial Team",
    blurb: "Rate build-up, risk pools, factor governance, and model oversight.",
    pages: ["dashboard", "builder", "rate-buildup", "risk-pool", "factors", "governance", "observability"],
  },
];

const STORAGE_KEY = "uw-persona";

export function getStoredPersona(): PersonaId {
  const stored = (typeof window !== "undefined" && window.localStorage.getItem(STORAGE_KEY)) || "";
  return PERSONAS.some((p) => p.id === stored) ? (stored as PersonaId) : "all";
}

export function storePersona(id: PersonaId): void {
  if (typeof window !== "undefined") window.localStorage.setItem(STORAGE_KEY, id);
}

export function personaById(id: PersonaId): Persona {
  return PERSONAS.find((p) => p.id === id) || PERSONAS[0];
}

/** Pages visible for a persona (full workbench when pages is null). */
export function visiblePages(id: PersonaId, allPages: Page[]): Set<Page> {
  const persona = personaById(id);
  return new Set(persona.pages ?? allPages);
}
