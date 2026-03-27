interface PercentileGaugeProps {
  label: string;
  percentile: number; // 0-1
  invertColor?: boolean; // if true, lower percentile = better (green)
}

export function PercentileGauge({ label, percentile, invertColor = true }: PercentileGaugeProps) {
  const pct = Math.round(percentile * 100);
  const effectivePct = invertColor ? 100 - pct : pct;

  const color =
    effectivePct >= 70
      ? "#16a34a"
      : effectivePct >= 40
      ? "#ca8a04"
      : "#dc2626";

  const displayLabel = invertColor
    ? `${pct}th pctl (lower is better)`
    : `${pct}th percentile`;

  return (
    <div className="space-y-1">
      <div className="flex justify-between items-center">
        <span className="text-xs text-gray-500">{label}</span>
        <span className="text-xs font-semibold" style={{ color }}>
          {pct}th
        </span>
      </div>
      <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{
            width: `${pct}%`,
            backgroundColor: color,
          }}
        />
      </div>
      <span className="text-[10px] text-gray-400">{displayLabel}</span>
    </div>
  );
}
