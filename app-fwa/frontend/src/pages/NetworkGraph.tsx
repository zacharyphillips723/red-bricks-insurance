import { useState, useEffect, useRef, useCallback } from "react";
import { Share2, Loader2, AlertTriangle, Users, DollarSign, FileText } from "lucide-react";
import { api, type NetworkNode, type NetworkEdge, type NetworkGraphData } from "@/lib/api";
import { formatCurrency, formatNumber } from "@/lib/utils";

// ---------------------------------------------------------------------------
// Force-directed layout (pure JS, no D3)
// ---------------------------------------------------------------------------

interface LayoutNode extends NetworkNode {
  x: number;
  y: number;
  vx: number;
  vy: number;
}

function runForceLayout(
  nodes: NetworkNode[],
  edges: NetworkEdge[],
  width: number,
  height: number,
  iterations: number = 120,
): LayoutNode[] {
  const layoutNodes: LayoutNode[] = nodes.map((n, i) => ({
    ...n,
    x: width / 2 + (Math.cos(i * 2.399) * width * 0.35),
    y: height / 2 + (Math.sin(i * 2.399) * height * 0.35),
    vx: 0,
    vy: 0,
  }));

  const nodeMap = new Map<string, LayoutNode>();
  layoutNodes.forEach((n) => nodeMap.set(n.id, n));

  const repulsion = 3000;
  const attraction = 0.005;
  const damping = 0.85;
  const centerGravity = 0.01;

  for (let iter = 0; iter < iterations; iter++) {
    const temp = 1 - iter / iterations;

    // Repulsion between all node pairs
    for (let i = 0; i < layoutNodes.length; i++) {
      for (let j = i + 1; j < layoutNodes.length; j++) {
        const a = layoutNodes[i];
        const b = layoutNodes[j];
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
        const force = (repulsion * temp) / (dist * dist);
        const fx = (dx / dist) * force;
        const fy = (dy / dist) * force;
        a.vx += fx;
        a.vy += fy;
        b.vx -= fx;
        b.vy -= fy;
      }
    }

    // Attraction along edges
    for (const edge of edges) {
      const source = nodeMap.get(edge.source);
      const target = nodeMap.get(edge.target);
      if (!source || !target) continue;
      const dx = target.x - source.x;
      const dy = target.y - source.y;
      const dist = Math.max(Math.sqrt(dx * dx + dy * dy), 1);
      const force = dist * attraction * (1 + edge.weight * 0.1);
      const fx = (dx / dist) * force;
      const fy = (dy / dist) * force;
      source.vx += fx;
      source.vy += fy;
      target.vx -= fx;
      target.vy -= fy;
    }

    // Center gravity
    for (const node of layoutNodes) {
      node.vx += (width / 2 - node.x) * centerGravity;
      node.vy += (height / 2 - node.y) * centerGravity;
    }

    // Apply velocities with damping
    for (const node of layoutNodes) {
      node.vx *= damping;
      node.vy *= damping;
      node.x += node.vx;
      node.y += node.vy;
      // Keep in bounds
      node.x = Math.max(40, Math.min(width - 40, node.x));
      node.y = Math.max(40, Math.min(height - 40, node.y));
    }
  }

  return layoutNodes;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function NetworkGraph() {
  const [data, setData] = useState<NetworkGraphData | null>(null);
  const [layoutNodes, setLayoutNodes] = useState<LayoutNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [hoveredNode, setHoveredNode] = useState<LayoutNode | null>(null);
  const [selectedProvider, setSelectedProvider] = useState<string | null>(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });
  const svgRef = useRef<SVGSVGElement>(null);

  const SVG_WIDTH = 900;
  const SVG_HEIGHT = 600;

  useEffect(() => {
    setLoading(true);
    setError("");
    api.getNetworkGraph()
      .then((result) => {
        setData(result);
        if (result.nodes.length > 0) {
          const nodes = runForceLayout(result.nodes, result.edges, SVG_WIDTH, SVG_HEIGHT);
          setLayoutNodes(nodes);
        }
      })
      .catch((err) => setError(String(err)))
      .finally(() => setLoading(false));
  }, []);

  const handleNodeHover = useCallback((node: LayoutNode | null, e?: React.MouseEvent) => {
    setHoveredNode(node);
    if (e && node) {
      const svg = svgRef.current;
      if (svg) {
        const rect = svg.getBoundingClientRect();
        setTooltipPos({ x: e.clientX - rect.left + 12, y: e.clientY - rect.top - 10 });
      }
    }
  }, []);

  const handleNodeClick = useCallback((node: LayoutNode) => {
    if (node.type === "provider") {
      setSelectedProvider((prev) => (prev === node.id ? null : node.id));
    }
  }, []);

  // Filter edges/nodes by selected provider
  const filteredEdges = data?.edges.filter((e) => {
    if (!selectedProvider) return true;
    return e.source === selectedProvider || e.target === selectedProvider;
  }) ?? [];

  const connectedNodeIds = new Set<string>();
  if (selectedProvider) {
    connectedNodeIds.add(selectedProvider);
    filteredEdges.forEach((e) => {
      connectedNodeIds.add(e.source);
      connectedNodeIds.add(e.target);
    });
  }

  const nodeMap = new Map<string, LayoutNode>();
  layoutNodes.forEach((n) => nodeMap.set(n.id, n));

  const nodeRadius = (node: LayoutNode) => {
    if (node.type === "provider") {
      return 8 + node.risk_score * 16;
    }
    return 6 + (node.claim_count ?? 1) * 0.5;
  };

  const edgeColor = (score?: number) => {
    if (score == null) return "#94a3b8";
    if (score > 0.7) return "#ef4444";
    if (score > 0.4) return "#f59e0b";
    return "#22c55e";
  };

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
          {/* Summary stats */}
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

          {/* Filter indicator */}
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

          {/* Network graph SVG */}
          <div className="card p-4 relative">
            <div className="flex items-center gap-4 mb-3 text-xs text-gray-500">
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

            <svg
              ref={svgRef}
              viewBox={`0 0 ${SVG_WIDTH} ${SVG_HEIGHT}`}
              className="w-full border border-gray-100 rounded-lg bg-gray-50"
              style={{ maxHeight: "600px" }}
            >
              {/* Edges */}
              {filteredEdges.map((edge, i) => {
                const source = nodeMap.get(edge.source);
                const target = nodeMap.get(edge.target);
                if (!source || !target) return null;
                const opacity = selectedProvider
                  ? 0.8
                  : 0.3 + (edge.fraud_score ?? 0) * 0.5;
                return (
                  <line
                    key={`edge-${i}`}
                    x1={source.x}
                    y1={source.y}
                    x2={target.x}
                    y2={target.y}
                    stroke={edgeColor(edge.fraud_score)}
                    strokeWidth={1 + edge.weight * 0.5}
                    opacity={opacity}
                  />
                );
              })}

              {/* Nodes */}
              {layoutNodes.map((node) => {
                const isVisible = !selectedProvider || connectedNodeIds.has(node.id);
                const r = nodeRadius(node);
                const fillColor = node.type === "provider"
                  ? (node.risk_score > 0.7 ? "#ef4444" : node.risk_score > 0.4 ? "#f97316" : "#fb923c")
                  : "#3b82f6";
                return (
                  <g
                    key={node.id}
                    opacity={isVisible ? 1 : 0.15}
                    style={{ cursor: node.type === "provider" ? "pointer" : "default" }}
                    onMouseEnter={(e) => handleNodeHover(node, e)}
                    onMouseLeave={() => handleNodeHover(null)}
                    onClick={() => handleNodeClick(node)}
                  >
                    <circle
                      cx={node.x}
                      cy={node.y}
                      r={r}
                      fill={fillColor}
                      stroke={hoveredNode?.id === node.id || selectedProvider === node.id ? "#1e293b" : "white"}
                      strokeWidth={hoveredNode?.id === node.id || selectedProvider === node.id ? 2.5 : 1.5}
                    />
                    {r > 12 && (
                      <text
                        x={node.x}
                        y={node.y + r + 12}
                        textAnchor="middle"
                        fontSize="9"
                        fill="#475569"
                        fontWeight="500"
                      >
                        {node.name.length > 16 ? node.name.slice(0, 14) + "..." : node.name}
                      </text>
                    )}
                  </g>
                );
              })}
            </svg>

            {/* Tooltip */}
            {hoveredNode && (
              <div
                className="absolute bg-white border border-gray-200 shadow-lg rounded-lg px-3 py-2 text-xs pointer-events-none z-10"
                style={{ left: tooltipPos.x, top: tooltipPos.y }}
              >
                <div className="font-semibold text-databricks-dark">{hoveredNode.name}</div>
                <div className="text-gray-500 capitalize">{hoveredNode.type}</div>
                <div className="mt-1 space-y-0.5">
                  <div>Risk Score: <strong>{(hoveredNode.risk_score * 100).toFixed(0)}%</strong></div>
                  <div>Investigations: {hoveredNode.investigation_count}</div>
                  {hoveredNode.claim_count != null && <div>Claims: {hoveredNode.claim_count}</div>}
                  {hoveredNode.estimated_overpayment != null && (
                    <div>Est. Overpayment: {formatCurrency(hoveredNode.estimated_overpayment)}</div>
                  )}
                </div>
                {hoveredNode.type === "provider" && (
                  <div className="text-blue-600 mt-1">Click to filter</div>
                )}
              </div>
            )}
          </div>

          {data.nodes.length === 0 && (
            <div className="card p-8 text-center">
              <Share2 className="w-12 h-12 text-gray-300 mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-databricks-dark mb-2">
                No network data available
              </h3>
              <p className="text-sm text-gray-500">
                Network graphs are built from investigation and evidence data in Lakebase.
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}
