import {
  Building2,
  ClipboardList,
  FileBarChart,
  MessageSquareText,
  Search,
  Briefcase,
} from "lucide-react";

const navItems = [
  { id: "groups", label: "Group Search", icon: Search },
  { id: "report-card", label: "Report Card", icon: FileBarChart },
  { id: "reports", label: "Standard Reports", icon: ClipboardList },
  { id: "coach", label: "Sales Coach", icon: MessageSquareText },
];

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
  selectedGroupId: string | null;
  onSelectGroup: (id: string) => void;
}

export function Sidebar({ activePage, onNavigate, selectedGroupId }: SidebarProps) {
  return (
    <aside className="w-64 bg-databricks-dark min-h-screen flex flex-col">
      {/* Logo */}
      <div className="px-6 py-5 border-b border-white/10">
        <div className="flex items-center gap-3">
          <Building2 className="w-7 h-7 text-databricks-red" />
          <div>
            <h1 className="text-white font-bold text-sm leading-tight">
              Group Reporting
            </h1>
            <p className="text-gray-400 text-xs">Sales Enablement Portal</p>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = activePage === item.id;
          const disabled =
            (item.id === "report-card" || item.id === "reports" || item.id === "coach") && !selectedGroupId;

          return (
            <button
              key={item.id}
              onClick={() => !disabled && onNavigate(item.id)}
              disabled={disabled}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                disabled
                  ? "text-gray-600 cursor-not-allowed"
                  : isActive
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

      {/* Selected group indicator */}
      {selectedGroupId && (
        <div className="px-4 py-3 border-t border-white/10">
          <div className="flex items-center gap-2 text-gray-400 text-xs">
            <Briefcase className="w-3.5 h-3.5" />
            <span className="truncate">{selectedGroupId}</span>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="px-6 py-4 border-t border-white/10">
        <p className="text-gray-500 text-xs">Powered by Databricks</p>
      </div>
    </aside>
  );
}
