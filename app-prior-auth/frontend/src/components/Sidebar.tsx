import {
  LayoutDashboard,
  ClipboardList,
  BookOpen,
  Bot,
  Users,
} from "lucide-react";

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
  pendingCount: number;
}

const NAV_ITEMS = [
  { id: "dashboard", label: "Dashboard", icon: LayoutDashboard },
  { id: "queue", label: "Review Queue", icon: ClipboardList, badge: true },
  { id: "policies", label: "Policy Library", icon: BookOpen },
  { id: "agent", label: "PA Review Agent", icon: Bot },
  { id: "caseload", label: "Caseload", icon: Users },
];

export function Sidebar({ activePage, onNavigate, pendingCount }: SidebarProps) {
  return (
    <aside className="w-64 bg-databricks-dark text-white flex flex-col min-h-screen">
      <div className="px-6 py-5 border-b border-white/10">
        <h1 className="text-lg font-bold tracking-tight">PA Review Portal</h1>
        <p className="text-xs text-white/50 mt-1">Prior Authorization Management</p>
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
              {item.badge && pendingCount > 0 && (
                <span className="bg-databricks-red text-white text-xs font-bold rounded-full px-2 py-0.5 min-w-[22px] text-center">
                  {pendingCount}
                </span>
              )}
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
