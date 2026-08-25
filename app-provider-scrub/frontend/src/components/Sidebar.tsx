import {
  ClipboardEdit,
  ShieldAlert,
  BarChart3,
  History,
  BookOpen,
  Activity,
} from "lucide-react";

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
}

const NAV_ITEMS = [
  { id: "compose", label: "Compose Claim", icon: ClipboardEdit },
  { id: "result", label: "Scrub Result", icon: ShieldAlert },
  { id: "intel", label: "Denial Intelligence", icon: BarChart3 },
  { id: "history", label: "History", icon: History },
  { id: "reference", label: "CARC Reference", icon: BookOpen },
  { id: "observability", label: "Observability", icon: Activity },
];

export function Sidebar({ activePage, onNavigate }: SidebarProps) {
  return (
    <aside className="w-64 bg-databricks-dark text-white flex flex-col min-h-screen">
      <div className="px-6 py-5 border-b border-white/10">
        <h1 className="text-lg font-bold tracking-tight">Claims Denial App</h1>
        <p className="text-xs text-white/50 mt-1">Denial Risk Predictor</p>
        <span className="inline-block mt-3 text-[10px] uppercase tracking-wide font-semibold
                         bg-white/10 text-white/70 rounded px-2 py-1">
          Red Bricks Insurance — Provider
        </span>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          const isActive = activePage === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors ${
                isActive
                  ? "bg-white/15 text-white"
                  : "text-white/70 hover:bg-white/10 hover:text-white"
              }`}
            >
              <Icon size={18} />
              <span className="flex-1 text-left">{item.label}</span>
            </button>
          );
        })}
      </nav>

      <div className="px-6 py-4 border-t border-white/10 text-xs text-white/30">
        Powered by Databricks
      </div>
    </aside>
  );
}
