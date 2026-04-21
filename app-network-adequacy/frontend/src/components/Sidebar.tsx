import {
  LayoutDashboard,
  ShieldCheck,
  Ghost,
  ArrowRightLeft,
  Target,
  Sparkles,
  Network,
  MapPin,
} from "lucide-react";

const navItems = [
  { id: "dashboard", label: "Dashboard", icon: LayoutDashboard },
  { id: "map", label: "Geographic View", icon: MapPin },
  { id: "compliance", label: "CMS Compliance", icon: ShieldCheck },
  { id: "ghost-network", label: "Ghost Network", icon: Ghost },
  { id: "leakage", label: "OON Leakage", icon: ArrowRightLeft },
  { id: "gaps", label: "Gaps & Recruitment", icon: Target },
  { id: "genie", label: "Network Analytics", icon: Sparkles },
];

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
}

export function Sidebar({ activePage, onNavigate }: SidebarProps) {
  return (
    <aside className="w-64 bg-databricks-dark min-h-screen flex flex-col">
      {/* Logo */}
      <div className="px-6 py-5 border-b border-white/10">
        <div className="flex items-center gap-3">
          <Network className="w-7 h-7 text-databricks-red" />
          <div>
            <h1 className="text-white font-bold text-sm leading-tight">
              Network Adequacy
            </h1>
            <p className="text-gray-400 text-xs">Portal</p>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = activePage === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onNavigate(item.id)}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? "bg-white/10 text-white"
                  : "text-gray-400 hover:bg-white/5 hover:text-gray-200"
              }`}
            >
              <Icon className="w-4 h-4 shrink-0" />
              <span className="flex-1 text-left">{item.label}</span>
            </button>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-6 py-4 border-t border-white/10">
        <p className="text-gray-500 text-xs">
          Powered by Databricks
        </p>
      </div>
    </aside>
  );
}
