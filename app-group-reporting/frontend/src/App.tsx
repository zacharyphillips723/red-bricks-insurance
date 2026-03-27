import { useState } from "react";
import { Sidebar } from "@/components/Sidebar";
import { GroupSearch } from "@/pages/GroupSearch";
import { GroupReportCard } from "@/pages/GroupReportCard";
import { GroupReports } from "@/pages/GroupReports";
import { SalesCoach } from "@/pages/SalesCoach";

export default function App() {
  const [page, setPage] = useState("groups");
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);

  const handleSelectGroup = (groupId: string) => {
    setSelectedGroupId(groupId);
    setPage("report-card");
  };

  const handleOpenCoach = (groupId: string) => {
    setSelectedGroupId(groupId);
    setPage("coach");
  };

  const handleOpenReports = (groupId: string) => {
    setSelectedGroupId(groupId);
    setPage("reports");
  };

  const renderPage = () => {
    switch (page) {
      case "groups":
        return <GroupSearch onSelectGroup={handleSelectGroup} />;
      case "report-card":
        return selectedGroupId ? (
          <GroupReportCard
            groupId={selectedGroupId}
            onBack={() => setPage("groups")}
            onOpenCoach={() => handleOpenCoach(selectedGroupId)}
            onOpenReports={() => handleOpenReports(selectedGroupId)}
          />
        ) : (
          <GroupSearch onSelectGroup={handleSelectGroup} />
        );
      case "reports":
        return selectedGroupId ? (
          <GroupReports
            groupId={selectedGroupId}
            onBack={() => setPage("report-card")}
          />
        ) : (
          <GroupSearch onSelectGroup={handleSelectGroup} />
        );
      case "coach":
        return selectedGroupId ? (
          <SalesCoach
            groupId={selectedGroupId}
            onBack={() => {
              setPage("report-card");
            }}
          />
        ) : (
          <GroupSearch onSelectGroup={handleSelectGroup} />
        );
      default:
        return <GroupSearch onSelectGroup={handleSelectGroup} />;
    }
  };

  return (
    <div className="flex min-h-screen">
      <Sidebar
        activePage={page}
        onNavigate={setPage}
        selectedGroupId={selectedGroupId}
        onSelectGroup={handleSelectGroup}
      />
      <main className="flex-1 p-8 overflow-y-auto">{renderPage()}</main>
    </div>
  );
}
