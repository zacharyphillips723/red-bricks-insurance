import { useState, useEffect } from "react";
import { BarChart3, TrendingUp, Cpu, GitCompare, LineChart, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import type {
  PropensityDistribution, DenialDriver, CorrelationRow, ForecastPoint,
} from "@/lib/api";

const num = (v: unknown): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const CORR_DIMS = [
  { id: "procedure", label: "Procedure (CPT)" },
  { id: "diagnosis", label: "Diagnosis (ICD-10)" },
  { id: "provider", label: "Provider (NPI)" },
  { id: "lob", label: "Line of business" },
];

const FORECAST_METRICS = [
  { id: "denial_rate", label: "Denial rate", pct: true },
  { id: "denied_count", label: "Denied claim count", pct: false },
  { id: "denied_amount", label: "Denied $", pct: false },
];

function ForecastChart({ points, pct }: { points: ForecastPoint[]; pct: boolean }) {
  if (points.length < 2) return <p className="text-sm text-gray-400">Not enough history to plot.</p>;
  const W = 720, H = 220, padX = 48, padY = 20;
  const xs = points.map((_, i) => i);
  const ys = points.flatMap((p) => [num(p.actual), num(p.forecast), num(p.lower), num(p.upper)].filter((v) => v > 0));
  const yMax = Math.max(...ys, 0.0001) * 1.1;
  const yMin = Math.min(...ys, 0);
  const px = (i: number) => padX + (i / (xs.length - 1)) * (W - padX - 10);
  const py = (v: number) => H - padY - ((v - yMin) / (yMax - yMin)) * (H - 2 * padY);
  const fmt = (v: number) => (pct ? `${(v * 100).toFixed(1)}%` : v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toFixed(0));

  // is_forecast can arrive as a real boolean or the string "true" (Statement Execution).
  const isFc = (p: ForecastPoint) => p.is_forecast === true || String(p.is_forecast) === "true";
  const firstFc = points.findIndex(isFc);
  // Actuals only on history; forecast line anchored to the last actual for continuity.
  const actualPts = points.map((p, i) => ({ i, v: isFc(p) ? null : num(p.actual) }));
  const fcPts = points.map((p, i) => ({
    i,
    v: isFc(p) ? num(p.forecast) : (firstFc > 0 && i === firstFc - 1 ? num(p.actual) : null),
  }));
  const line = (pts: { i: number; v: number | null }[]) =>
    pts.filter((p) => p.v != null).map((p, k) => `${k === 0 ? "M" : "L"}${px(p.i)},${py(p.v as number)}`).join(" ");
  const bandPts = points.filter(isFc).map((p) => ({
    i: points.indexOf(p), lo: p.lower == null ? null : num(p.lower), hi: p.upper == null ? null : num(p.upper),
  })).filter((p) => p.lo != null && p.hi != null);
  const band = bandPts.length
    ? `${bandPts.map((p) => `${px(p.i)},${py(p.hi as number)}`).join(" L")} L${bandPts.slice().reverse().map((p) => `${px(p.i)},${py(p.lo as number)}`).join(" L")}`
    : "";

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 240 }}>
      {[0, 0.5, 1].map((t) => {
        const v = yMin + t * (yMax - yMin);
        return (
          <g key={t}>
            <line x1={padX} x2={W - 10} y1={py(v)} y2={py(v)} stroke="#F1F1F3" />
            <text x={4} y={py(v) + 3} fontSize="9" fill="#9CA3AF">{fmt(v)}</text>
          </g>
        );
      })}
      {firstFc > 0 && <line x1={px(firstFc)} x2={px(firstFc)} y1={padY} y2={H - padY} stroke="#D1D5DB" strokeDasharray="3 3" />}
      {band && <path d={`M${band} Z`} fill="#FF3621" opacity="0.10" />}
      <path d={line(actualPts)} fill="none" stroke="#1B3139" strokeWidth="2" />
      <path d={line(fcPts)} fill="none" stroke="#FF3621" strokeWidth="2" strokeDasharray="5 4" />
    </svg>
  );
}

function StatTile({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="card p-4">
      <p className="text-xs uppercase tracking-wide text-gray-400">{label}</p>
      <p className="text-2xl font-bold text-databricks-dark mt-1">{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}

function Bar({ label, value, pct, color = "bg-databricks-red", right }: {
  label: string; value: string; pct: number; color?: string; right?: string;
}) {
  return (
    <div className="flex items-center gap-3">
      <div className="w-52 shrink-0 text-sm text-gray-700 truncate" title={label}>{label}</div>
      <div className="flex-1 h-5 bg-gray-100 rounded overflow-hidden">
        <div className={`h-full ${color} rounded`} style={{ width: `${Math.max(2, Math.min(100, pct))}%` }} />
      </div>
      <div className="w-28 shrink-0 text-right text-sm text-gray-600 tabular-nums">
        {value}{right && <span className="text-gray-400 ml-1">{right}</span>}
      </div>
    </div>
  );
}

export function DenialIntel() {
  const [prop, setProp] = useState<PropensityDistribution | null>(null);
  const [drivers, setDrivers] = useState<DenialDriver[]>([]);
  const [dim, setDim] = useState("procedure");
  const [corr, setCorr] = useState<CorrelationRow[]>([]);
  const [forecast, setForecast] = useState<ForecastPoint[]>([]);
  const [fcMetric, setFcMetric] = useState("denial_rate");
  const [loading, setLoading] = useState(true);
  const [corrLoading, setCorrLoading] = useState(false);

  useEffect(() => {
    Promise.all([api.getPropensity(), api.getDrivers(), api.getForecast()])
      .then(([p, d, f]) => { setProp(p); setDrivers(d.drivers); setForecast(f.series); })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    setCorrLoading(true);
    api.getCorrelations(dim)
      .then((r) => setCorr(r.rows))
      .catch(() => setCorr([]))
      .finally(() => setCorrLoading(false));
  }, [dim]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20 text-gray-400">
        <Loader2 className="w-6 h-6 animate-spin mr-2" /> Loading denial intelligence…
      </div>
    );
  }

  const total = num(prop?.summary?.total);
  const avgProb = num(prop?.summary?.avg_prob);
  const highRisk = num(prop?.summary?.high_risk);
  const maxBucket = Math.max(1, ...(prop?.buckets || []).map((b) => num(b.n)));
  const maxReason = Math.max(1, ...(prop?.reasons || []).map((r) => num(r.n)));
  const maxDriver = Math.max(1, ...drivers.map((d) => num(d.importance_pct)));
  const maxCorr = Math.max(0.01, ...corr.map((c) => num(c.denial_rate)));

  return (
    <div className="max-w-5xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
          <BarChart3 className="text-databricks-red" /> Denial Intelligence
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Book-level denial propensity, the drivers behind it, and how denial rates correlate
          with procedures, diagnoses, providers, and lines of business.
        </p>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatTile label="Claims scored" value={total.toLocaleString()} />
        <StatTile label="Avg denial propensity" value={`${Math.round(avgProb * 100)}%`} />
        <StatTile label="High-risk (≥50%)" value={highRisk.toLocaleString()}
          sub={total ? `${Math.round((highRisk / total) * 100)}% of scored claims` : undefined} />
        <StatTile label="Reason categories" value={String((prop?.reasons || []).length)} />
      </div>

      {/* Forecast */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-1">
          <h3 className="font-semibold text-databricks-dark flex items-center gap-2">
            <LineChart className="w-4 h-4 text-databricks-red" /> Denial forecast (next 6 months)
          </h3>
          <select
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm"
            value={fcMetric}
            onChange={(e) => setFcMetric(e.target.value)}
          >
            {FORECAST_METRICS.map((m) => <option key={m.id} value={m.id}>{m.label}</option>)}
          </select>
        </div>
        <p className="text-xs text-gray-500 mb-3">
          Solid = actuals · dashed = forecast · shaded = confidence band.
          {forecast.find((p) => p.metric === fcMetric)?.method
            ? ` Method: ${forecast.find((p) => p.metric === fcMetric)?.method}.`
            : ""}
        </p>
        <ForecastChart
          points={forecast.filter((p) => p.metric === fcMetric)}
          pct={FORECAST_METRICS.find((m) => m.id === fcMetric)?.pct ?? false}
        />
      </div>

      {/* Propensity distribution + reason mix */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card p-6">
          <h3 className="font-semibold text-databricks-dark flex items-center gap-2 mb-4">
            <TrendingUp className="w-4 h-4 text-databricks-red" /> Denial-propensity distribution
          </h3>
          <div className="space-y-2">
            {(prop?.buckets || []).map((b) => (
              <Bar key={b.bucket} label={b.bucket} value={num(b.n).toLocaleString()}
                pct={(num(b.n) / maxBucket) * 100} />
            ))}
          </div>
        </div>
        <div className="card p-6">
          <h3 className="font-semibold text-databricks-dark mb-1">Denial-reason mix</h3>
          <p className="text-xs text-gray-500 mb-4">Actual reasons on historically denied claims.</p>
          <div className="space-y-2">
            {(prop?.reasons || []).map((r) => (
              <Bar key={r.reason} label={r.reason} value={num(r.n).toLocaleString()}
                pct={(num(r.n) / maxReason) * 100} color="bg-purple-500" />
            ))}
          </div>
        </div>
      </div>

      {/* Denial drivers */}
      <div className="card p-6">
        <h3 className="font-semibold text-databricks-dark flex items-center gap-2 mb-1">
          <Cpu className="w-4 h-4 text-databricks-red" /> Top denial drivers
        </h3>
        <p className="text-xs text-gray-500 mb-4">
          Global feature importance from the trained denial-risk model
          {drivers[0]?.method ? ` (${drivers[0].method === "shap" ? "SHAP" : "model gain"})` : ""}.
        </p>
        <div className="space-y-2">
          {drivers.length === 0 && <p className="text-sm text-gray-400">No driver data available.</p>}
          {drivers.map((d) => (
            <Bar key={d.feature} label={d.label} value={`${num(d.importance_pct).toFixed(1)}%`}
              pct={(num(d.importance_pct) / maxDriver) * 100} color="bg-teal-500" />
          ))}
        </div>
      </div>

      {/* Correlation analysis */}
      <div className="card p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold text-databricks-dark flex items-center gap-2">
            <GitCompare className="w-4 h-4 text-databricks-red" /> Denial-rate correlation
          </h3>
          <select
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm"
            value={dim}
            onChange={(e) => setDim(e.target.value)}
          >
            {CORR_DIMS.map((d) => <option key={d.id} value={d.id}>{d.label}</option>)}
          </select>
        </div>
        <p className="text-xs text-gray-500 mb-4">
          Historical denial rate by {CORR_DIMS.find((d) => d.id === dim)?.label.toLowerCase()} (min. 30 claims),
          ranked by denial rate — the factors most correlated with denials.
        </p>
        {corrLoading ? (
          <div className="flex items-center py-8 text-gray-400">
            <Loader2 className="w-5 h-5 animate-spin mr-2" /> Loading…
          </div>
        ) : (
          <div className="space-y-2">
            {corr.length === 0 && <p className="text-sm text-gray-400">No data for this dimension.</p>}
            {corr.map((c) => (
              <Bar
                key={c.dimension_value}
                label={c.dimension_value}
                value={`${(num(c.denial_rate) * 100).toFixed(1)}%`}
                right={`${num(c.denied).toLocaleString()}/${num(c.total).toLocaleString()}`}
                pct={(num(c.denial_rate) / maxCorr) * 100}
                color="bg-databricks-red"
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
