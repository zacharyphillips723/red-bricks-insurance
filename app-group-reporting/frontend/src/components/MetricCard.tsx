interface MetricCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  color?: "default" | "green" | "red" | "amber";
}

const colorMap = {
  default: "text-databricks-dark",
  green: "text-green-600",
  red: "text-red-600",
  amber: "text-amber-600",
};

export function MetricCard({ label, value, subtitle, color = "default" }: MetricCardProps) {
  return (
    <div>
      <span className="text-gray-400 text-xs">{label}</span>
      <p className={`text-xl font-bold ${colorMap[color]}`}>{value}</p>
      {subtitle && <span className="text-xs text-gray-400">{subtitle}</span>}
    </div>
  );
}
