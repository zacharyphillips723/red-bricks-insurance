import { useEffect, useState, useMemo } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  useMap,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { api, CountyMapMetric } from "@/lib/api";
import { formatCurrency, formatNumber, formatPercent } from "@/lib/utils";

// ---------------------------------------------------------------------------
// Metric definitions
// ---------------------------------------------------------------------------

type MetricKey =
  | "compliance"
  | "leakage"
  | "gaps"
  | "ghost"
  | "providers";

interface MetricDef {
  label: string;
  description: string;
  getValue: (c: CountyMapMetric) => number;
  format: (v: number) => string;
  colorScale: (v: number, min: number, max: number) => string;
  radiusScale: (v: number, min: number, max: number) => number;
  legendLabels: (min: number, max: number) => string[];
}

function interpolateColor(
  ratio: number,
  fromRgb: [number, number, number],
  toRgb: [number, number, number]
): string {
  const r = Math.round(fromRgb[0] + (toRgb[0] - fromRgb[0]) * ratio);
  const g = Math.round(fromRgb[1] + (toRgb[1] - fromRgb[1]) * ratio);
  const b = Math.round(fromRgb[2] + (toRgb[2] - fromRgb[2]) * ratio);
  return `rgb(${r},${g},${b})`;
}

function normalize(v: number, min: number, max: number): number {
  if (max === min) return 0.5;
  return Math.max(0, Math.min(1, (v - min) / (max - min)));
}

const GREEN: [number, number, number] = [34, 197, 94];
const YELLOW: [number, number, number] = [234, 179, 8];
const RED: [number, number, number] = [239, 68, 68];

// Green-to-red diverging (higher = worse)
function badScale(v: number, min: number, max: number): string {
  const ratio = normalize(v, min, max);
  if (ratio < 0.5) return interpolateColor(ratio * 2, GREEN, YELLOW);
  return interpolateColor((ratio - 0.5) * 2, YELLOW, RED);
}

// Red-to-green diverging (higher = better)
function goodScale(v: number, min: number, max: number): string {
  const ratio = normalize(v, min, max);
  if (ratio < 0.5) return interpolateColor(ratio * 2, RED, YELLOW);
  return interpolateColor((ratio - 0.5) * 2, YELLOW, GREEN);
}

function radius(v: number, min: number, max: number, minR = 8, maxR = 32): number {
  const ratio = normalize(v, min, max);
  return minR + ratio * (maxR - minR);
}

const METRICS: Record<MetricKey, MetricDef> = {
  compliance: {
    label: "CMS Compliance",
    description: "Average compliance % across specialties",
    getValue: (c) => c.avg_compliance_pct,
    format: formatPercent,
    colorScale: goodScale,
    radiusScale: (v, min, max) => radius(v, min, max, 12, 28),
    legendLabels: (min, max) => [
      formatPercent(min),
      formatPercent((min + max) / 2),
      formatPercent(max),
    ],
  },
  leakage: {
    label: "OON Leakage Cost",
    description: "Total out-of-network cost differential",
    getValue: (c) => c.leakage_cost,
    format: formatCurrency,
    colorScale: badScale,
    radiusScale: (v, min, max) => radius(v, min, max),
    legendLabels: (min, max) => [
      formatCurrency(min),
      formatCurrency((min + max) / 2),
      formatCurrency(max),
    ],
  },
  gaps: {
    label: "Gap Members",
    description: "Members outside CMS distance thresholds",
    getValue: (c) => c.gap_members,
    format: formatNumber,
    colorScale: badScale,
    radiusScale: (v, min, max) => radius(v, min, max),
    legendLabels: (min, max) => [
      formatNumber(min),
      formatNumber((min + max) / 2),
      formatNumber(max),
    ],
  },
  ghost: {
    label: "Ghost Network Flags",
    description: "Providers flagged with ghost network signals",
    getValue: (c) => c.ghost_flagged_count,
    format: formatNumber,
    colorScale: badScale,
    radiusScale: (v, min, max) => radius(v, min, max),
    legendLabels: (min, max) => [
      formatNumber(min),
      formatNumber((min + max) / 2),
      formatNumber(max),
    ],
  },
  providers: {
    label: "Provider Count",
    description: "Total providers in directory",
    getValue: (c) => c.total_providers,
    format: formatNumber,
    colorScale: goodScale,
    radiusScale: (v, min, max) => radius(v, min, max),
    legendLabels: (min, max) => [
      formatNumber(min),
      formatNumber((min + max) / 2),
      formatNumber(max),
    ],
  },
};

// ---------------------------------------------------------------------------
// Auto-fit bounds component
// ---------------------------------------------------------------------------

function FitBounds({ data }: { data: CountyMapMetric[] }) {
  const map = useMap();
  useEffect(() => {
    if (data.length === 0) return;
    const lats = data.map((d) => d.latitude);
    const lons = data.map((d) => d.longitude);
    const bounds: [[number, number], [number, number]] = [
      [Math.min(...lats) - 0.3, Math.min(...lons) - 0.3],
      [Math.max(...lats) + 0.3, Math.max(...lons) + 0.3],
    ];
    map.fitBounds(bounds, { padding: [30, 30] });
  }, [data, map]);
  return null;
}

// ---------------------------------------------------------------------------
// Legend
// ---------------------------------------------------------------------------

function Legend({
  metric,
  min,
  max,
}: {
  metric: MetricDef;
  min: number;
  max: number;
}) {
  const labels = metric.legendLabels(min, max);
  const colors = [
    metric.colorScale(min, min, max),
    metric.colorScale((min + max) / 2, min, max),
    metric.colorScale(max, min, max),
  ];
  return (
    <div className="absolute bottom-6 left-6 z-[1000] bg-white rounded-lg shadow-lg px-4 py-3 border border-gray-200">
      <p className="text-xs font-semibold text-gray-700 mb-2">
        {metric.label}
      </p>
      <div className="flex items-center gap-1">
        <span className="text-[10px] text-gray-500">{labels[0]}</span>
        <div
          className="h-3 w-32 rounded-sm"
          style={{
            background: `linear-gradient(to right, ${colors[0]}, ${colors[1]}, ${colors[2]})`,
          }}
        />
        <span className="text-[10px] text-gray-500">{labels[2]}</span>
      </div>
      <p className="text-[10px] text-gray-400 mt-1">
        Circle size = relative magnitude
      </p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// County type filter
// ---------------------------------------------------------------------------

function CountyTypeFilter({
  types,
  selected,
  onToggle,
}: {
  types: string[];
  selected: Set<string>;
  onToggle: (t: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {types.map((t) => (
        <button
          key={t}
          onClick={() => onToggle(t)}
          className={`px-2.5 py-1 text-xs font-medium rounded-full border transition-colors ${
            selected.has(t)
              ? "bg-databricks-red text-white border-databricks-red"
              : "bg-white text-gray-600 border-gray-300 hover:border-gray-400"
          }`}
        >
          {t}
        </button>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main MapPage
// ---------------------------------------------------------------------------

export function MapPage() {
  const [data, setData] = useState<CountyMapMetric[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeMetric, setActiveMetric] = useState<MetricKey>("compliance");
  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(new Set());

  useEffect(() => {
    api
      .getCountyMapMetrics()
      .then((rows) => {
        setData(rows.filter((r) => r.latitude !== 0 && r.longitude !== 0));
        // Initialize with all county types selected
        const types = new Set(rows.map((r) => r.county_type).filter(Boolean));
        setSelectedTypes(types);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const countyTypes = useMemo(
    () => [...new Set(data.map((d) => d.county_type).filter(Boolean))].sort(),
    [data]
  );

  const toggleType = (t: string) => {
    setSelectedTypes((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return next;
    });
  };

  const filtered = useMemo(
    () =>
      data.filter(
        (d) => selectedTypes.size === 0 || selectedTypes.has(d.county_type)
      ),
    [data, selectedTypes]
  );

  const metric = METRICS[activeMetric];
  const values = filtered.map(metric.getValue);
  const minVal = values.length ? Math.min(...values) : 0;
  const maxVal = values.length ? Math.max(...values) : 1;

  if (loading)
    return (
      <div className="flex items-center justify-center h-96">
        <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-databricks-red" />
      </div>
    );

  if (error)
    return (
      <div className="bg-red-50 text-red-700 p-4 rounded-lg">{error}</div>
    );

  return (
    <div className="space-y-4">
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Geographic View</h2>
        <p className="text-sm text-gray-500 mt-1">
          County-level network metrics — sized and colored by selected measure
        </p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-start gap-6 bg-white rounded-xl border border-gray-200 p-4">
        {/* Metric selector */}
        <div className="space-y-1.5">
          <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
            Metric
          </label>
          <div className="flex flex-wrap gap-1.5">
            {(Object.keys(METRICS) as MetricKey[]).map((key) => (
              <button
                key={key}
                onClick={() => setActiveMetric(key)}
                className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors ${
                  activeMetric === key
                    ? "bg-databricks-dark text-white border-databricks-dark"
                    : "bg-white text-gray-600 border-gray-300 hover:border-gray-400"
                }`}
              >
                {METRICS[key].label}
              </button>
            ))}
          </div>
          <p className="text-xs text-gray-400">{metric.description}</p>
        </div>

        {/* County type filter */}
        {countyTypes.length > 1 && (
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              County Type
            </label>
            <CountyTypeFilter
              types={countyTypes}
              selected={selectedTypes}
              onToggle={toggleType}
            />
          </div>
        )}

        {/* Summary stats */}
        <div className="ml-auto flex gap-4 text-center">
          <div>
            <p className="text-lg font-bold text-gray-900">{filtered.length}</p>
            <p className="text-xs text-gray-500">Counties</p>
          </div>
          <div>
            <p className="text-lg font-bold text-gray-900">
              {formatNumber(filtered.reduce((s, c) => s + c.total_providers, 0))}
            </p>
            <p className="text-xs text-gray-500">Providers</p>
          </div>
          <div>
            <p className="text-lg font-bold text-gray-900">
              {formatCurrency(filtered.reduce((s, c) => s + c.leakage_cost, 0))}
            </p>
            <p className="text-xs text-gray-500">OON Leakage</p>
          </div>
        </div>
      </div>

      {/* Map */}
      <div className="relative rounded-xl overflow-hidden border border-gray-200 shadow-sm"
        style={{ height: "calc(100vh - 320px)", minHeight: 400 }}>
        <MapContainer
          center={[35.5, -79.5]}
          zoom={7}
          className="h-full w-full"
          zoomControl={true}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>'
            url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
          />
          <FitBounds data={filtered} />
          {filtered.map((county) => {
            const val = metric.getValue(county);
            const color = metric.colorScale(val, minVal, maxVal);
            const r = metric.radiusScale(val, minVal, maxVal);
            return (
              <CircleMarker
                key={county.county_fips}
                center={[county.latitude, county.longitude]}
                radius={r}
                pathOptions={{
                  fillColor: color,
                  fillOpacity: 0.75,
                  color: "white",
                  weight: 2,
                  opacity: 0.9,
                }}
              >
                <Popup>
                  <div className="text-sm min-w-[220px]">
                    <p className="font-bold text-base mb-0.5">
                      {county.county_name}
                    </p>
                    <p className="text-gray-500 text-xs mb-2">
                      {county.county_type} &middot; FIPS {county.county_fips}
                    </p>
                    <table className="w-full text-xs">
                      <tbody>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Compliance</td>
                          <td className="py-1 font-semibold text-right">
                            {formatPercent(county.avg_compliance_pct)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">
                            Non-Compliant Specialties
                          </td>
                          <td className="py-1 font-semibold text-right">
                            {county.non_compliant_specialties} / {county.total_specialties}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Gap Members</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(county.gap_members)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Ghost Flagged</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(county.ghost_flagged_count)}
                            {county.ghost_high_count > 0 && (
                              <span className="text-red-600 ml-1">
                                ({county.ghost_high_count} high)
                              </span>
                            )}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">OON Leakage</td>
                          <td className="py-1 font-semibold text-right">
                            {formatCurrency(county.leakage_cost)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">OON Claims</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(county.oon_claims)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Providers</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(county.inn_providers)} INN / {formatNumber(county.oon_providers)} OON
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}
        </MapContainer>
        <Legend metric={metric} min={minVal} max={maxVal} />
      </div>
    </div>
  );
}
