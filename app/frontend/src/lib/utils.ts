export function riskBadgeClass(tier: string): string {
  const map: Record<string, string> = {
    Critical: "badge-critical",
    High: "badge-high",
    Elevated: "badge-elevated",
    Moderate: "badge-moderate",
    Low: "badge-low",
  };
  return map[tier] || "badge-low";
}

export function statusColor(status: string): string {
  const map: Record<string, string> = {
    Unassigned: "text-gray-500",
    Assigned: "text-blue-600",
    "Outreach Attempted": "text-indigo-600",
    "Outreach Successful": "text-violet-600",
    "Assessment In Progress": "text-purple-600",
    "Intervention Active": "text-amber-600",
    "Follow-Up Scheduled": "text-cyan-600",
    Resolved: "text-green-600",
    Escalated: "text-red-600",
    "Closed — Unable to Reach": "text-gray-400",
  };
  return map[status] || "text-gray-500";
}

export function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

export function formatDateTime(dateStr: string): string {
  return new Date(dateStr).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function sourceIcon(source: string): string {
  const map: Record<string, string> = {
    "High Glucose No Insulin": "🩸",
    "ED High Utilizer": "🏥",
    "SDOH Risk": "🏘️",
    "Readmission Risk": "🔄",
    Manual: "✏️",
  };
  return map[source] || "📋";
}
