import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { Share2, Loader2, AlertTriangle, Users, DollarSign, FileText } from "lucide-react";
import ForceGraph2D from "react-force-graph-2d";
import { api, type NetworkNode, type NetworkGraphData } from "@/lib/api";
import { formatCurrency, formatNumber } from "@/lib/utils";

interface GraphNode extends NetworkNode {
  x?: number;
  y?: number;
  fx?: number;
  fy?: number;
}

interface GraphLink {
  source: string | GraphNode;
  target: string | GraphNode;
  weight: number;
  fraud_score?: number;
  claim_count?: number;
}

export function NetworkGraph() {
  const [data, setData] = useState<NetworkGraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [selectedProvider, setSelectedProvider] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 900, height: 600 });
  const fgRef = useRef<any>(null);

  const hoveredNodeRef = useRef<GraphNode | null>(null);
  const selectedProviderRef = useRef<string | null>(null);
  selectedProviderRef.current = selectedProvider;

  // Track whether the initial zoomToFit has fired
  const didInitialFit = useRef(false);

  useEffect(() => {
    setLoading(true);
    setError("");
    api.getNetworkGraph()
      .then(setData)
      .catch((err) => setError(String(err)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      const { width } = entries[0].contentRect;
      setDimensions({ width, height: Math.max(550, width * 0.55) });
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  // Build graph data ONCE from API response — never rebuild on selection change.
  // Selection highlighting is handled purely in the paint functions via refs.
  const graphData = useMemo(() => {
    if (!data) return { nodes: [] as GraphNode[], links: [] as GraphLink[] };

    const nodes: GraphNode[] = data.nodes.map((n) => ({ ...n }));
    const links: GraphLink[] = data.edges.map((e) => ({
      source: e.source,
      target: e.target,
      weight: e.weight,
      fraud_score: e.fraud_score,
      claim_count: e.claim_count,
    }));

    return { nodes, links };
  }, [data]);

  // Pre-compute adjacency for fast highlight lookup
  const adjacency = useMemo(() => {
    const map = new Map<string, Set<string>>();
    if (!data) return map;
    for (const e of data.edges) {
      if (!map.has(e.source)) map.set(e.source, new Set());
      if (!map.has(e.target)) map.set(e.target, new Set());
      map.get(e.source)!.add(e.target);
      map.get(e.target)!.add(e.source);
    }
    return map;
  }, [data]);

  const isConnected = useCallback((nodeId: string) => {
    const sel = selectedProviderRef.current;
    if (!sel) return true;
    if (nodeId === sel) return true;
    return adjacency.get(sel)?.has(nodeId) ?? false;
  }, [adjacency]);

  const nodeColorFn = useCallback((node: GraphNode) => {
    if (node.type === "provider") {
      if (node.risk_score > 0.7) return "#ef4444";
      if (node.risk_score > 0.4) return "#f97316";
      return "#fb923c";
    }
    return "#3b82f6";
  }, []);

  const paintNode = useCallback((node: GraphNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
    // Base radius in graph-space units
    const baseR = node.type === "provider" ? 5 + node.risk_score * 12 : 3 + (node.claim_count ?? 1) * 0.25;
    // Scale radius so nodes visibly grow/shrink with zoom.
    // The canvas transform already applies globalScale, but we add an extra
    // sqrt(globalScale) factor so the size change is more perceptible.
    const r = baseR * Math.sqrt(globalScale) / globalScale;
    // Net visual radius on screen = r * globalScale (from canvas transform)
    //   = baseR * sqrt(globalScale)
    // So at 4x zoom, visual radius = baseR * 2  (double)
    // At 0.25x zoom, visual radius = baseR * 0.5 (half)

    const connected = isConnected(node.id);
    const isSelected = selectedProviderRef.current === node.id;
    const isHovered = hoveredNodeRef.current?.id === node.id;

    ctx.globalAlpha = connected ? 1 : 0.1;

    ctx.beginPath();
    ctx.arc(node.x!, node.y!, r, 0, 2 * Math.PI);
    ctx.fillStyle = nodeColorFn(node);
    ctx.fill();

    if (isHovered || isSelected) {
      ctx.strokeStyle = "#1e293b";
      ctx.lineWidth = 2.5 / globalScale;
      ctx.stroke();
    } else if (connected) {
      ctx.strokeStyle = "rgba(255,255,255,0.8)";
      ctx.lineWidth = 1.2 / globalScale;
      ctx.stroke();
    }

    if (node.type === "provider" && globalScale > 0.7 && connected) {
      const label = node.name.length > 18 ? node.name.slice(0, 16) + "\u2026" : node.name;
      const fontSize = Math.max(10 / globalScale, 2);
      ctx.font = `500 ${fontSize}px sans-serif`;
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillStyle = "#475569";
      ctx.fillText(label, node.x!, node.y! + r + 2 / globalScale);
    }

    ctx.globalAlpha = 1;
  }, [nodeColorFn, isConnected]);

  const linkColorFn = useCallback((link: GraphLink) => {
    const sel = selectedProviderRef.current;
    if (sel) {
      const src = typeof link.source === "string" ? link.source : link.source.id;
      const tgt = typeof link.target === "string" ? link.target : link.target.id;
      if (src !== sel && tgt !== sel) return "rgba(148,163,184,0.04)";
    }
    const score = link.fraud_score;
    if (score == null) return "rgba(148,163,184,0.35)";
    if (score > 0.7) return "rgba(239,68,68,0.55)";
    if (score > 0.4) return "rgba(245,158,11,0.45)";
    return "rgba(34,197,94,0.35)";
  }, []);

  const linkWidthFn = useCallback((link: GraphLink) => {
    const sel = selectedProviderRef.current;
    if (sel) {
      const src = typeof link.source === "string" ? link.source : link.source.id;
      const tgt = typeof link.target === "string" ? link.target : link.target.id;
      if (src !== sel && tgt !== sel) return 0.1;
    }
    return 0.5 + link.weight * 0.4;
  }, []);

  const handleNodeClick = useCallback((node: GraphNode) => {
    if (node.type === "provider") {
      setSelectedProvider((prev) => (prev === node.id ? null : node.id));
    }
  }, []);

  const handleBackgroundClick = useCallback(() => {
    setSelectedProvider(null);
  }, []);

  const handleNodeHover = useCallback((node: GraphNode | null) => {
    hoveredNodeRef.current = node;
    if (containerRef.current) {
      containerRef.current.style.cursor = node?.type === "provider" ? "pointer" : node ? "default" : "grab";
    }
  }, []);

  const handleEngineStop = useCallback(() => {
    const gd = fgRef.current?.graphData?.();
    if (gd?.nodes) {
      gd.nodes.forEach((n: any) => { n.fx = n.x; n.fy = n.y; });
    }
    if (!didInitialFit.current) {
      didInitialFit.current = true;
      // Delay zoomToFit to ensure the canvas has rendered the first frame
      setTimeout(() => fgRef.current?.zoomToFit(400, 50), 100);
    }
  }, []);

  // Fallback: if onEngineStop doesn't fire in time, force zoomToFit after mount
  useEffect(() => {
    if (!data || loading) return;
    const timer = setTimeout(() => {
      if (!didInitialFit.current) {
        didInitialFit.current = true;
        fgRef.current?.zoomToFit(400, 50);
      }
    }, 1500);
    return () => clearTimeout(timer);
  }, [data, loading]);

  const handleNodeDragEnd = useCallback((node: any) => {
    node.fx = node.x;
    node.fy = node.y;
  }, []);

  const nodeMap = new Map<string, NetworkNode>();
  data?.nodes.forEach((n) => nodeMap.set(n.id, n));

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold text-databricks-dark flex items-center gap-2">
        <Share2 className="w-6 h-6 text-databricks-red" /> Fraud Network Graph
      </h2>

      {loading && (
        <div className="card p-8 text-center text-gray-500">
          <Loader2 className="w-6 h-6 animate-spin mx-auto mb-2" />
          Building network graph...
        </div>
      )}

      {error && <div className="card p-4 text-sm text-red-600">{error}</div>}

      {data && !loading && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="card p-4">
              <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
                <AlertTriangle className="w-3.5 h-3.5" /> Providers
              </div>
              <div className="text-lg font-bold text-databricks-dark">
                {formatNumber(data.stats.total_providers)}
              </div>
            </div>
            <div className="card p-4">
              <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
                <Users className="w-3.5 h-3.5" /> Members
              </div>
              <div className="text-lg font-bold text-databricks-dark">
                {formatNumber(data.stats.total_members)}
              </div>
            </div>
            <div className="card p-4">
              <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
                <FileText className="w-3.5 h-3.5" /> Flagged Claims
              </div>
              <div className="text-lg font-bold text-databricks-dark">
                {formatNumber(data.stats.total_claims)}
              </div>
            </div>
            <div className="card p-4">
              <div className="flex items-center gap-2 text-xs text-gray-500 mb-1">
                <DollarSign className="w-3.5 h-3.5" /> Est. Overpayment
              </div>
              <div className="text-lg font-bold text-red-600">
                {formatCurrency(data.stats.total_overpayment)}
              </div>
            </div>
          </div>

          {selectedProvider && (
            <div className="flex items-center gap-2 text-sm text-gray-600 bg-blue-50 rounded-lg px-4 py-2">
              <span>Filtering by provider: <strong>{nodeMap.get(selectedProvider)?.name ?? selectedProvider}</strong></span>
              <button
                onClick={() => setSelectedProvider(null)}
                className="ml-2 text-xs px-2 py-0.5 rounded bg-blue-100 hover:bg-blue-200 text-blue-700"
              >
                Clear filter
              </button>
            </div>
          )}

          <div className="card p-2 relative" ref={containerRef}>
            <div className="flex items-center justify-between mb-2 px-2">
              <div className="flex items-center gap-4 text-xs text-gray-500">
                <div className="flex items-center gap-1">
                  <svg width="12" height="12"><circle cx="6" cy="6" r="5" fill="#ef4444" /></svg>
                  Provider (sized by risk)
                </div>
                <div className="flex items-center gap-1">
                  <svg width="12" height="12"><circle cx="6" cy="6" r="5" fill="#3b82f6" /></svg>
                  Member
                </div>
                <div className="flex items-center gap-1">
                  <svg width="24" height="4"><line x1="0" y1="2" x2="24" y2="2" stroke="#ef4444" strokeWidth="2" /></svg>
                  High fraud score
                </div>
                <div className="flex items-center gap-1">
                  <svg width="24" height="4"><line x1="0" y1="2" x2="24" y2="2" stroke="#22c55e" strokeWidth="2" /></svg>
                  Low fraud score
                </div>
              </div>
              <div className="flex items-center gap-2 text-xs text-gray-500">
                <span>Scroll to zoom, drag to pan</span>
                <button
                  onClick={() => fgRef.current?.zoomToFit(300, 40)}
                  className="px-2 py-0.5 rounded bg-gray-100 hover:bg-gray-200 text-gray-600"
                >
                  Fit to view
                </button>
              </div>
            </div>

            <div className="rounded-lg overflow-hidden bg-gray-50">
              <ForceGraph2D
                ref={fgRef}
                graphData={graphData}
                width={dimensions.width - 16}
                height={dimensions.height}
                nodeId="id"
                nodeCanvasObject={paintNode}
                nodePointerAreaPaint={(node: GraphNode, color: string, ctx: CanvasRenderingContext2D, globalScale: number) => {
                  const baseR = node.type === "provider" ? 5 + node.risk_score * 12 : 4;
                  const r = baseR * Math.sqrt(globalScale) / globalScale;
                  ctx.beginPath();
                  ctx.arc(node.x!, node.y!, r + 3 / globalScale, 0, 2 * Math.PI);
                  ctx.fillStyle = color;
                  ctx.fill();
                }}
                linkColor={linkColorFn}
                linkWidth={linkWidthFn}
                linkDirectionalParticles={0}
                onNodeClick={handleNodeClick}
                onBackgroundClick={handleBackgroundClick}
                onNodeHover={handleNodeHover}
                nodeLabel={(node: GraphNode) => {
                  const lines = [
                    `<strong>${node.name}</strong>`,
                    `<em style="text-transform:capitalize">${node.type}</em>`,
                    `Risk: ${(node.risk_score * 100).toFixed(0)}%`,
                  ];
                  if (node.investigation_count) lines.push(`Investigations: ${node.investigation_count}`);
                  if (node.claim_count != null) lines.push(`Claims: ${node.claim_count}`);
                  if (node.estimated_overpayment != null) lines.push(`Est. Overpayment: ${formatCurrency(node.estimated_overpayment)}`);
                  if (node.type === "provider") lines.push(`<span style="color:#3b82f6">Click to highlight connections</span>`);
                  return lines.join("<br/>");
                }}
                cooldownTicks={120}
                cooldownTime={4000}
                warmupTicks={30}
                d3AlphaDecay={0.03}
                d3VelocityDecay={0.35}
                d3AlphaMin={0.001}
                onEngineStop={handleEngineStop}
                enableZoomInteraction={true}
                enablePanInteraction={true}
                enableNodeDrag={true}
                onNodeDragEnd={handleNodeDragEnd}
                backgroundColor="#f9fafb"
                minZoom={0.2}
                maxZoom={12}
              />
            </div>
          </div>

          {data.nodes.length === 0 && (
            <div className="card p-8 text-center">
              <Share2 className="w-12 h-12 text-gray-300 mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-databricks-dark mb-2">
                No network data available
              </h3>
              <p className="text-sm text-gray-500">
                Network graphs are built from investigation and evidence data.
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}
