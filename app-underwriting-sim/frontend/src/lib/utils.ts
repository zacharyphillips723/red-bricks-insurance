export function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatCurrencyPrecise(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatPercent(value: number, decimals = 1): string {
  return `${value.toFixed(decimals)}%`;
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
    hour: "numeric",
    minute: "2-digit",
  });
}

export function deltaColor(value: number): string {
  if (value > 0) return "text-green-600";
  if (value < 0) return "text-red-600";
  return "text-gray-500";
}

export function deltaArrow(value: number): string {
  if (value > 0) return "\u2191";
  if (value < 0) return "\u2193";
  return "\u2192";
}

export function statusBadgeClass(status: string): string {
  switch (status) {
    case "computed":
      return "badge-positive";
    case "approved":
      return "bg-blue-100 text-blue-800 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium";
    case "draft":
      return "badge-neutral";
    case "archived":
      return "bg-gray-200 text-gray-600 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium";
    default:
      return "badge-neutral";
  }
}

export const SIMULATION_TYPE_LABELS: Record<string, string> = {
  premium_rate: "Premium Rate Change",
  benefit_design: "Benefit Design",
  group_renewal: "Group Renewal",
  population_mix: "Population Mix",
  medical_trend: "Medical Trend",
  stop_loss: "Stop-Loss",
  risk_adjustment: "Risk Adjustment",
  utilization_change: "Utilization Change",
  new_group_quote: "New Group Quote",
  ibnr_reserve: "IBNR Reserve",
};
