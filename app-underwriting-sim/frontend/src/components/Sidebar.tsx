import {
  LayoutDashboard,
  Calculator,
  GitCompareArrows,
  History,
  Bot,
  TrendingUp,
} from "lucide-react";

type Page = "dashboard" | "builder" | "comparison" | "history" | "agent";

interface SidebarProps {
  currentPage: Page;
  onNavigate: (page: Page) => void;
  savedCount: number;
}

const NAV_ITEMS: { page: Page; label: string; icon: React.ElementType }[] = [
  { page: "dashboard", label: "Dashboard", icon: LayoutDashboard },
  { page: "builder", label: "Simulation Builder", icon: Calculator },
  { page: "comparison", label: "Compare Scenarios", icon: GitCompareArrows },
  { page: "history", label: "Simulation History", icon: History },
  { page: "agent", label: "Underwriting Agent", icon: Bot },
];

export default function Sidebar({ currentPage, onNavigate, savedCount }: SidebarProps) {
  return (
    <aside className="w-64 bg-databricks-dark text-white flex flex-col min-h-screen">
      {/* Logo */}
      <div className="p-6 border-b border-white/10">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-databricks-red rounded-lg flex items-center justify-center">
            <TrendingUp className="w-5 h-5 text-white" />
          </div>
          <div>
            <div className="font-bold text-sm leading-tight">Underwriting</div>
            <div className="text-xs text-white/60">Simulation Portal</div>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-1">
        {NAV_ITEMS.map(({ page, label, icon: Icon }) => (
          <button
            key={page}
            onClick={() => onNavigate(page)}
            className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
              currentPage === page
                ? "bg-white/10 text-white"
                : "text-white/60 hover:text-white hover:bg-white/5"
            }`}
          >
            <Icon className="w-5 h-5 flex-shrink-0" />
            <span>{label}</span>
            {page === "history" && savedCount > 0 && (
              <span className="ml-auto bg-databricks-red text-white text-xs px-2 py-0.5 rounded-full">
                {savedCount}
              </span>
            )}
          </button>
        ))}
      </nav>

      {/* Footer */}
      <div className="p-4 border-t border-white/10">
        <div className="text-xs text-white/40 text-center">
          Powered by Databricks
        </div>
      </div>
    </aside>
  );
}
