import { useEffect, useState, useMemo, useCallback } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  useMap,
  LayersControl,
  LayerGroup,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";
import {
  api,
  CountyMapMetric,
  GeoProvider,
  GeoMemberCluster,
  CountyComplianceSummary,
} from "@/lib/api";
import { formatCurrency, formatNumber, formatPercent } from "@/lib/utils";

// ---------------------------------------------------------------------------
// Color palettes
// ---------------------------------------------------------------------------

const SPECIALTY_COLORS: Record<string, string> = {
  "Primary Care": "#2563eb",
  "Behavioral Health": "#7c3aed",
  "OB/GYN": "#db2777",
  "Cardiology": "#dc2626",
  "Orthopedic Surgery": "#ea580c",
  "Dermatology": "#d97706",
  "Oncology": "#4f46e5",
  "Endocrinology": "#0891b2",
  "Gastroenterology": "#059669",
  "Pulmonology": "#65a30d",
};

function getSpecialtyColor(specialty: string): string {
  return SPECIALTY_COLORS[specialty] || "#6b7280";
}

const RISK_TIER_COLORS: Record<string, string> = {
  Low: "#22c55e",
  Standard: "#3b82f6",
  High: "#f59e0b",
  Critical: "#ef4444",
};

function getRiskColor(tier: string): string {
  return RISK_TIER_COLORS[tier] || "#6b7280";
}

// ---------------------------------------------------------------------------
// Metric definitions (for county bubble overlay)
// ---------------------------------------------------------------------------

type MetricKey = "compliance" | "leakage" | "gaps" | "ghost" | "providers";

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

function badScale(v: number, min: number, max: number): string {
  const ratio = normalize(v, min, max);
  if (ratio < 0.5) return interpolateColor(ratio * 2, GREEN, YELLOW);
  return interpolateColor((ratio - 0.5) * 2, YELLOW, RED);
}

function goodScale(v: number, min: number, max: number): string {
  const ratio = normalize(v, min, max);
  if (ratio < 0.5) return interpolateColor(ratio * 2, RED, YELLOW);
  return interpolateColor((ratio - 0.5) * 2, YELLOW, GREEN);
}

function radius(
  v: number,
  min: number,
  max: number,
  minR = 8,
  maxR = 32
): number {
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
// Active layer types
// ---------------------------------------------------------------------------

type LayerType = "counties" | "providers" | "members" | "compliance";

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
// Legend component
// ---------------------------------------------------------------------------

function MapLegend({
  activeLayers,
  metric,
  minVal,
  maxVal,
  specialties,
}: {
  activeLayers: Set<LayerType>;
  metric: MetricDef;
  minVal: number;
  maxVal: number;
  specialties: string[];
}) {
  const labels = metric.legendLabels(minVal, maxVal);
  const colors = [
    metric.colorScale(minVal, minVal, maxVal),
    metric.colorScale((minVal + maxVal) / 2, minVal, maxVal),
    metric.colorScale(maxVal, minVal, maxVal),
  ];

  return (
    <div className="absolute bottom-6 left-6 z-[1000] bg-white rounded-lg shadow-lg px-4 py-3 border border-gray-200 max-w-xs">
      {activeLayers.has("counties") && (
        <div className="mb-3">
          <p className="text-xs font-semibold text-gray-700 mb-1.5">
            {metric.label}
          </p>
          <div className="flex items-center gap-1">
            <span className="text-[10px] text-gray-500">{labels[0]}</span>
            <div
              className="h-3 w-24 rounded-sm"
              style={{
                background: `linear-gradient(to right, ${colors[0]}, ${colors[1]}, ${colors[2]})`,
              }}
            />
            <span className="text-[10px] text-gray-500">{labels[2]}</span>
          </div>
        </div>
      )}

      {activeLayers.has("providers") && (
        <div className="mb-3">
          <p className="text-xs font-semibold text-gray-700 mb-1.5">
            Provider Specialty
          </p>
          <div className="grid grid-cols-2 gap-x-3 gap-y-0.5">
            {specialties.slice(0, 10).map((s) => (
              <div key={s} className="flex items-center gap-1.5">
                <span
                  className="inline-block w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: getSpecialtyColor(s) }}
                />
                <span className="text-[10px] text-gray-600 truncate">
                  {s}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {activeLayers.has("members") && (
        <div className="mb-3">
          <p className="text-xs font-semibold text-gray-700 mb-1.5">
            Member Risk Tier
          </p>
          <div className="flex gap-3">
            {Object.entries(RISK_TIER_COLORS).map(([tier, color]) => (
              <div key={tier} className="flex items-center gap-1">
                <span
                  className="inline-block w-2.5 h-2.5 rounded-full"
                  style={{ backgroundColor: color }}
                />
                <span className="text-[10px] text-gray-600">{tier}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {activeLayers.has("compliance") && (
        <div>
          <p className="text-xs font-semibold text-gray-700 mb-1.5">
            CMS Compliance
          </p>
          <div className="flex gap-3">
            <div className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm border-2 border-green-500 bg-green-500/20" />
              <span className="text-[10px] text-gray-600">Meeting</span>
            </div>
            <div className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded-sm border-2 border-red-500 bg-red-500/20" />
              <span className="text-[10px] text-gray-600">Gaps</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Layer toggle buttons
// ---------------------------------------------------------------------------

function LayerToggles({
  activeLayers,
  onToggle,
}: {
  activeLayers: Set<LayerType>;
  onToggle: (layer: LayerType) => void;
}) {
  const layers: { key: LayerType; label: string; color: string }[] = [
    { key: "counties", label: "County Metrics", color: "bg-gray-600" },
    { key: "providers", label: "Providers", color: "bg-blue-600" },
    { key: "members", label: "Members", color: "bg-amber-500" },
    { key: "compliance", label: "Compliance Zones", color: "bg-green-600" },
  ];

  return (
    <div className="flex flex-wrap gap-1.5">
      {layers.map((l) => (
        <button
          key={l.key}
          onClick={() => onToggle(l.key)}
          className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-all ${
            activeLayers.has(l.key)
              ? "bg-databricks-dark text-white border-databricks-dark shadow-sm"
              : "bg-white text-gray-500 border-gray-300 hover:border-gray-400"
          }`}
        >
          <span
            className={`w-2 h-2 rounded-full ${
              activeLayers.has(l.key) ? "bg-white" : l.color
            }`}
          />
          {l.label}
        </button>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main MapPage
// ---------------------------------------------------------------------------

export function MapPage() {
  const [countyData, setCountyData] = useState<CountyMapMetric[]>([]);
  const [providers, setProviders] = useState<GeoProvider[]>([]);
  const [members, setMembers] = useState<GeoMemberCluster[]>([]);
  const [compliance, setCompliance] = useState<CountyComplianceSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeMetric, setActiveMetric] = useState<MetricKey>("compliance");
  const [activeLayers, setActiveLayers] = useState<Set<LayerType>>(
    new Set(["counties", "compliance"])
  );
  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(new Set());

  useEffect(() => {
    Promise.all([
      api.getCountyMapMetrics(),
      api.getGeoProviders(),
      api.getGeoMembers(),
      api.getGeoCompliance(),
    ])
      .then(([county, prov, mem, comp]) => {
        const validCounty = county.filter(
          (r) => r.latitude !== 0 && r.longitude !== 0
        );
        setCountyData(validCounty);
        setProviders(prov);
        setMembers(mem);
        setCompliance(comp);
        const types = new Set(
          validCounty.map((r) => r.county_type).filter(Boolean)
        );
        setSelectedTypes(types);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const toggleLayer = useCallback((layer: LayerType) => {
    setActiveLayers((prev) => {
      const next = new Set(prev);
      if (next.has(layer)) next.delete(layer);
      else next.add(layer);
      return next;
    });
  }, []);

  const countyTypes = useMemo(
    () =>
      [...new Set(countyData.map((d) => d.county_type).filter(Boolean))].sort(),
    [countyData]
  );

  const toggleType = (t: string) => {
    setSelectedTypes((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return next;
    });
  };

  const filteredCounties = useMemo(
    () =>
      countyData.filter(
        (d) => selectedTypes.size === 0 || selectedTypes.has(d.county_type)
      ),
    [countyData, selectedTypes]
  );

  const specialties = useMemo(
    () => [...new Set(providers.map((p) => p.specialty))].sort(),
    [providers]
  );

  const metric = METRICS[activeMetric];
  const values = filteredCounties.map(metric.getValue);
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
          Interactive map of providers, members, and CMS compliance across North
          Carolina
        </p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-start gap-6 bg-white rounded-xl border border-gray-200 p-4">
        {/* Layer toggles */}
        <div className="space-y-1.5">
          <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
            Layers
          </label>
          <LayerToggles activeLayers={activeLayers} onToggle={toggleLayer} />
        </div>

        {/* Metric selector (only for county bubble layer) */}
        {activeLayers.has("counties") && (
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              County Metric
            </label>
            <div className="flex flex-wrap gap-1.5">
              {(Object.keys(METRICS) as MetricKey[]).map((key) => (
                <button
                  key={key}
                  onClick={() => setActiveMetric(key)}
                  className={`px-2.5 py-1 text-xs font-medium rounded-lg border transition-colors ${
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
        )}

        {/* County type filter */}
        {countyTypes.length > 1 && (
          <div className="space-y-1.5">
            <label className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
              County Type
            </label>
            <div className="flex flex-wrap gap-1.5">
              {countyTypes.map((t) => (
                <button
                  key={t}
                  onClick={() => toggleType(t)}
                  className={`px-2.5 py-1 text-xs font-medium rounded-full border transition-colors ${
                    selectedTypes.has(t)
                      ? "bg-databricks-red text-white border-databricks-red"
                      : "bg-white text-gray-600 border-gray-300 hover:border-gray-400"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Summary stats */}
        <div className="ml-auto flex gap-4 text-center">
          <div>
            <p className="text-lg font-bold text-gray-900">
              {filteredCounties.length}
            </p>
            <p className="text-xs text-gray-500">Counties</p>
          </div>
          <div>
            <p className="text-lg font-bold text-gray-900">
              {formatNumber(providers.length)}
            </p>
            <p className="text-xs text-gray-500">Providers</p>
          </div>
          <div>
            <p className="text-lg font-bold text-gray-900">
              {formatNumber(
                members.reduce((s, m) => s + m.member_count, 0)
              )}
            </p>
            <p className="text-xs text-gray-500">Members</p>
          </div>
          <div>
            <p className="text-lg font-bold text-gray-900">
              {formatCurrency(
                filteredCounties.reduce((s, c) => s + c.leakage_cost, 0)
              )}
            </p>
            <p className="text-xs text-gray-500">OON Leakage</p>
          </div>
        </div>
      </div>

      {/* Map */}
      <div
        className="relative rounded-xl overflow-hidden border border-gray-200 shadow-sm"
        style={{ height: "calc(100vh - 340px)", minHeight: 450 }}
      >
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
          <FitBounds data={filteredCounties} />

          {/* Compliance zones layer */}
          {activeLayers.has("compliance") &&
            compliance.map((c) => {
              if (c.latitude === 0 && c.longitude === 0) return null;
              const isCompliant = c.overall_compliant;
              return (
                <CircleMarker
                  key={`comp-${c.county_fips}`}
                  center={[c.latitude, c.longitude]}
                  radius={22}
                  pathOptions={{
                    fillColor: isCompliant ? "#22c55e" : "#ef4444",
                    fillOpacity: 0.12,
                    color: isCompliant ? "#16a34a" : "#dc2626",
                    weight: 2,
                    opacity: 0.6,
                    dashArray: isCompliant ? undefined : "6 4",
                  }}
                >
                  <Popup>
                    <div className="text-sm min-w-[240px]">
                      <p className="font-bold text-base mb-0.5">
                        {c.county_name}
                      </p>
                      <p className="text-xs mb-2">
                        <span
                          className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${
                            isCompliant
                              ? "bg-green-100 text-green-800"
                              : "bg-red-100 text-red-800"
                          }`}
                        >
                          {isCompliant ? "CMS Compliant" : "Has Gaps"}
                        </span>
                        <span className="text-gray-400 ml-2">
                          {c.county_type}
                        </span>
                      </p>
                      <table className="w-full text-xs">
                        <tbody>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">
                              Avg Compliance
                            </td>
                            <td className="py-1 font-semibold text-right">
                              {formatPercent(c.avg_compliance_pct)}
                            </td>
                          </tr>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">Specialties</td>
                            <td className="py-1 font-semibold text-right">
                              <span className="text-green-600">
                                {c.specialties_compliant}
                              </span>
                              {" / "}
                              <span className="text-red-600">
                                {c.specialties_non_compliant}
                              </span>
                              {" of "}
                              {c.total_specialties}
                            </td>
                          </tr>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">Gap Members</td>
                            <td className="py-1 font-semibold text-right">
                              {formatNumber(c.gap_members)}
                            </td>
                          </tr>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">
                              Total Members
                            </td>
                            <td className="py-1 font-semibold text-right">
                              {formatNumber(c.total_members)}
                            </td>
                          </tr>
                        </tbody>
                      </table>
                      {c.non_compliant_specialties.length > 0 && (
                        <div className="mt-2 pt-2 border-t border-gray-100">
                          <p className="text-xs font-semibold text-red-600 mb-1">
                            Non-Compliant Specialties:
                          </p>
                          <div className="flex flex-wrap gap-1">
                            {c.non_compliant_specialties.map((s) => (
                              <span
                                key={s}
                                className="text-[10px] bg-red-50 text-red-700 px-1.5 py-0.5 rounded"
                              >
                                {s}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </Popup>
                </CircleMarker>
              );
            })}

          {/* Member clusters layer */}
          {activeLayers.has("members") &&
            members.map((m) => (
              <CircleMarker
                key={`mem-${m.county_fips}-${m.zip_code}`}
                center={[m.latitude, m.longitude]}
                radius={Math.max(3, Math.min(10, Math.sqrt(m.member_count)))}
                pathOptions={{
                  fillColor: getRiskColor(m.risk_tier),
                  fillOpacity: 0.5,
                  color: getRiskColor(m.risk_tier),
                  weight: 1,
                  opacity: 0.7,
                }}
              >
                <Popup>
                  <div className="text-sm min-w-[180px]">
                    <p className="font-bold text-sm mb-0.5">
                      {m.county_name} — ZIP {m.zip_code}
                    </p>
                    <table className="w-full text-xs">
                      <tbody>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Members</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(m.member_count)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Access Risk</td>
                          <td className="py-1 text-right">
                            <span
                              className="inline-block text-xs font-semibold px-2 py-0.5 rounded-full"
                              style={{
                                backgroundColor: `${getRiskColor(
                                  m.risk_tier
                                )}20`,
                                color: getRiskColor(m.risk_tier),
                              }}
                            >
                              {m.risk_tier}
                            </span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </Popup>
              </CircleMarker>
            ))}

          {/* County metric bubbles layer */}
          {activeLayers.has("counties") &&
            filteredCounties.map((county) => {
              const val = metric.getValue(county);
              const color = metric.colorScale(val, minVal, maxVal);
              const r = metric.radiusScale(val, minVal, maxVal);
              return (
                <CircleMarker
                  key={`county-${county.county_fips}`}
                  center={[county.latitude, county.longitude]}
                  radius={r}
                  pathOptions={{
                    fillColor: color,
                    fillOpacity: 0.7,
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
                              Non-Compliant
                            </td>
                            <td className="py-1 font-semibold text-right">
                              {county.non_compliant_specialties} /{" "}
                              {county.total_specialties}
                            </td>
                          </tr>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">Gap Members</td>
                            <td className="py-1 font-semibold text-right">
                              {formatNumber(county.gap_members)}
                            </td>
                          </tr>
                          <tr className="border-t border-gray-100">
                            <td className="py-1 text-gray-500">
                              Ghost Flagged
                            </td>
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
                            <td className="py-1 text-gray-500">Providers</td>
                            <td className="py-1 font-semibold text-right">
                              {formatNumber(county.inn_providers)} INN /{" "}
                              {formatNumber(county.oon_providers)} OON
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </Popup>
                </CircleMarker>
              );
            })}

          {/* Provider markers layer */}
          {activeLayers.has("providers") &&
            providers.map((p) => (
              <CircleMarker
                key={`prov-${p.npi}`}
                center={[p.latitude, p.longitude]}
                radius={5}
                pathOptions={{
                  fillColor: getSpecialtyColor(p.specialty),
                  fillOpacity: 0.85,
                  color: "#ffffff",
                  weight: 1.5,
                  opacity: 1,
                }}
              >
                <Popup>
                  <div className="text-sm min-w-[220px]">
                    <p className="font-bold text-sm mb-0.5">
                      {p.provider_name}
                    </p>
                    <div className="flex items-center gap-2 mb-2">
                      <span
                        className="inline-block text-[10px] font-semibold px-2 py-0.5 rounded-full text-white"
                        style={{
                          backgroundColor: getSpecialtyColor(p.specialty),
                        }}
                      >
                        {p.specialty}
                      </span>
                      <span
                        className={`text-[10px] font-medium px-1.5 py-0.5 rounded ${
                          p.network_status === "In-Network"
                            ? "bg-green-100 text-green-700"
                            : "bg-red-100 text-red-700"
                        }`}
                      >
                        {p.network_status}
                      </span>
                    </div>
                    <table className="w-full text-xs">
                      <tbody>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">NPI</td>
                          <td className="py-1 font-mono text-right">
                            {p.npi}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">County</td>
                          <td className="py-1 text-right">{p.county_name}</td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Panel Size</td>
                          <td className="py-1 font-semibold text-right">
                            {formatNumber(p.panel_size)}
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">
                            Accepting Patients
                          </td>
                          <td className="py-1 text-right">
                            <span
                              className={
                                p.accepts_new_patients
                                  ? "text-green-600"
                                  : "text-red-600"
                              }
                            >
                              {p.accepts_new_patients ? "Yes" : "No"}
                            </span>
                          </td>
                        </tr>
                        <tr className="border-t border-gray-100">
                          <td className="py-1 text-gray-500">Telehealth</td>
                          <td className="py-1 text-right">
                            {p.telehealth_capable ? "Yes" : "No"}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </Popup>
              </CircleMarker>
            ))}
        </MapContainer>

        <MapLegend
          activeLayers={activeLayers}
          metric={metric}
          minVal={minVal}
          maxVal={maxVal}
          specialties={specialties}
        />
      </div>
    </div>
  );
}
