export function formatCurrency(value: number | null | undefined): string {
  if (value == null) return "$0";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null) return "0";
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null) return "0%";
  return `${(value * 100).toFixed(1)}%`;
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  return new Date(value).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  return new Date(value).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function severityBadgeClass(severity: string | null): string {
  switch (severity) {
    case "Critical":
      return "badge-critical";
    case "High":
      return "badge-high";
    case "Medium":
      return "badge-medium";
    case "Low":
      return "badge-low";
    default:
      return "bg-gray-100 text-gray-600 text-xs font-semibold px-2.5 py-0.5 rounded-full";
  }
}

export function statusColor(status: string | null): string {
  if (!status) return "text-gray-500";
  if (status.startsWith("Closed")) return "text-gray-500";
  if (status === "Open") return "text-blue-600";
  if (status === "Under Review") return "text-amber-600";
  if (status === "Evidence Gathering") return "text-purple-600";
  if (status === "Referred to SIU") return "text-red-600";
  if (status === "Recovery In Progress") return "text-green-600";
  return "text-gray-600";
}
