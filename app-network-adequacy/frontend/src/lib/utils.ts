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
  return `${value.toFixed(1)}%`;
}

export function severityBadgeClass(severity: string | null): string {
  switch (severity) {
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

export function complianceBadgeClass(isCompliant: boolean): string {
  return isCompliant ? "badge-compliant" : "badge-non-compliant";
}

export function gapStatusColor(status: string): string {
  switch (status) {
    case "Critical":
      return "text-red-700 bg-red-50";
    case "Non-Compliant":
      return "text-red-600 bg-red-50";
    case "At Risk":
      return "text-amber-700 bg-amber-50";
    case "Marginal":
      return "text-yellow-700 bg-yellow-50";
    case "Compliant":
      return "text-green-700 bg-green-50";
    default:
      return "text-gray-600 bg-gray-50";
  }
}
