import React, { useEffect, useRef, useState, forwardRef, useImperativeHandle } from 'react';
import * as d3 from 'd3';
import axios from 'axios';

const API = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const NODE_COLORS = {
  salesOrder: '#3b82f6',
  delivery: '#10b981',
  billing: '#f59e0b',
  customer: '#8b5cf6',
  journalEntry: '#ef4444',
  product: '#06b6d4',
};

const NODE_RADIUS = {
  customer: 10,
  salesOrder: 7,
  delivery: 7,
  billing: 7,
  journalEntry: 6,
  product: 5,
};

const GraphView = forwardRef(({ highlightedNodes }, ref) => {
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [nodeDetail, setNodeDetail] = useState(null);
  const [detailPos, setDetailPos] = useState({ x: 0, y: 0 });
  const [loading, setLoading] = useState(true);
  const simulationRef = useRef(null);
  const nodesRef = useRef([]);
  const nodeCirclesRef = useRef(null);
  const linksRef = useRef(null);
  const linksDataRef = useRef([]);
  const zoomBehaviorRef = useRef(null);
  const gRef = useRef(null);

  // Expose focusNodes method to parent
  useImperativeHandle(ref, () => ({
    focusNodes: (ids) => {
      if (!svgRef.current || !nodesRef.current.length) return;

      // Find matching nodes
      const matched = nodesRef.current.filter(n =>
        ids.some(id => n.id.includes(id) || Object.values(n.data || {}).map(String).includes(id))
      );

      if (!matched.length) return;

      // Wait for nodes to have positions (force simulation may still be settling)
      const attemptZoom = () => {
        const validMatched = matched.filter(n => n.x !== undefined && n.y !== undefined && n.x > 0 && n.y > 0);
        
        if (validMatched.length === 0) {
          // Retry in 50ms if nodes don't have valid positions yet
          setTimeout(attemptZoom, 50);
          return;
        }

        const matchedIds = new Set(validMatched.map(n => n.id));

        // Calculate center and bounds of matched nodes
        const avgX = validMatched.reduce((sum, n) => sum + (n.x || 0), 0) / validMatched.length;
        const avgY = validMatched.reduce((sum, n) => sum + (n.y || 0), 0) / validMatched.length;

        const xs = validMatched.map(n => n.x || 0);
        const ys = validMatched.map(n => n.y || 0);
        const minX = Math.min(...xs);
        const maxX = Math.max(...xs);
        const minY = Math.min(...ys);
        const maxY = Math.max(...ys);
        const nodeSpread = Math.max(maxX - minX, maxY - minY, 120);

        const width = containerRef.current.clientWidth;
        const height = containerRef.current.clientHeight;
        const scale = Math.min(3.5, 0.7 * Math.min(width, height) / nodeSpread);

        // Pan and zoom to matched nodes with smooth animation
        const svg = d3.select(svgRef.current);
        
        if (zoomBehaviorRef.current) {
          svg.transition()
            .duration(900)
            .ease(d3.easeCubicInOut)
            .call(
              zoomBehaviorRef.current.transform,
              d3.zoomIdentity
                .translate(width / 2, height / 2)
                .scale(scale)
                .translate(-avgX, -avgY)
            );
        }

        // Highlight matched nodes with pulse effect
        if (nodeCirclesRef.current) {
          nodeCirclesRef.current
            .transition().duration(400)
            .ease(d3.easeLinear)
            .attr('stroke', n =>
              validMatched.find(m => m.id === n.id) ? '#facc15' : '#ffffff'
            )
            .attr('stroke-width', n =>
              validMatched.find(m => m.id === n.id) ? 4 : 1.5
            )
            .attr('r', n =>
              validMatched.find(m => m.id === n.id)
                ? (NODE_RADIUS[n.type] || 6) * 2.3
                : NODE_RADIUS[n.type] || 6
            );

          // Shrink back after 1.8 seconds but keep yellow highlight
          setTimeout(() => {
            nodeCirclesRef.current
              .transition().duration(500)
              .ease(d3.easeLinear)
              .attr('r', n => NODE_RADIUS[n.type] || 6);
          }, 1800);
        }

        // Highlight connected edges
        if (linksRef.current) {
          linksRef.current
            .transition().duration(300)
            .attr('stroke', link => {
              const sourceId = link.source.id || link.source;
              const targetId = link.target.id || link.target;
              return matchedIds.has(sourceId) && matchedIds.has(targetId) ? '#fbbf24' : '#c8d6e5';
            })
            .attr('stroke-width', link => {
              const sourceId = link.source.id || link.source;
              const targetId = link.target.id || link.target;
              return matchedIds.has(sourceId) && matchedIds.has(targetId) ? 3 : 1;
            })
            .attr('stroke-opacity', link => {
              const sourceId = link.source.id || link.source;
              const targetId = link.target.id || link.target;
              return matchedIds.has(sourceId) && matchedIds.has(targetId) ? 0.9 : 0.4;
            });
        }
      };

      attemptZoom();
    }
  }));

  useEffect(() => {
    axios.get(`${API}/graph/nodes`).then(res => {
      const rawNodes = res.data.nodes;
      const rawEdges = res.data.edges;

      const width = containerRef.current.clientWidth;
      const height = containerRef.current.clientHeight;

      const svg = d3.select(svgRef.current)
        .attr('width', width)
        .attr('height', height);

      svg.selectAll('*').remove();

      // Zoom support
      const g = svg.append('g');
      gRef.current = g;
      
      const zoomBehavior = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => {
          g.attr('transform', event.transform);
        });
      
      zoomBehaviorRef.current = zoomBehavior;
      svg.call(zoomBehavior);

      // Build node and link maps
      const nodeMap = {};
      rawNodes.forEach(n => { nodeMap[n.id] = n; });

      const links = rawEdges
        .filter(e => nodeMap[e.source] && nodeMap[e.target])
        .map(e => ({ ...e, source: e.source, target: e.target }));

      const nodes = rawNodes.map(n => ({ ...n }));
      nodesRef.current = nodes;
      linksDataRef.current = links; // Store links data for later reference

      // Force simulation
      const simulation = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(links).id(d => d.id).distance(80).strength(0.5))
        .force('charge', d3.forceManyBody().strength(-120))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(18));

      simulationRef.current = simulation;

      // Draw edges
      const link = g.append('g')
        .selectAll('line')
        .data(links)
        .join('line')
        .attr('stroke', '#c8d6e5')
        .attr('stroke-width', 1)
        .attr('stroke-opacity', 0.6);

      linksRef.current = link; // Store reference to link elements

      // Draw nodes
      const node = g.append('g')
        .selectAll('circle')
        .data(nodes)
        .join('circle')
        .attr('r', d => NODE_RADIUS[d.type] || 6)
        .attr('fill', d => NODE_COLORS[d.type] || '#6b7280')
        .attr('stroke', '#ffffff')
        .attr('stroke-width', 1.5)
        .style('cursor', 'pointer')
        .call(
          d3.drag()
            .on('start', (event, d) => {
              if (!event.active) simulation.alphaTarget(0.3).restart();
              d.fx = d.x; d.fy = d.y;
            })
            .on('drag', (event, d) => {
              d.fx = event.x; d.fy = event.y;
            })
            .on('end', (event, d) => {
              if (!event.active) simulation.alphaTarget(0);
              d.fx = null; d.fy = null;
            })
        )
        .on('click', (event, d) => {
          event.stopPropagation();
          const rect = containerRef.current.getBoundingClientRect();
          setDetailPos({
            x: event.clientX - rect.left + 12,
            y: event.clientY - rect.top - 20,
          });
          setSelectedNode(d);
          setNodeDetail(null);
          axios.get(`${API}/graph/node/${d.id}`).then(r => setNodeDetail(r.data));
        });

      nodeCirclesRef.current = node;

      // Labels for large nodes only
      const label = g.append('g')
        .selectAll('text')
        .data(nodes.filter(n => n.type === 'customer'))
        .join('text')
        .text(d => d.label)
        .attr('font-size', 9)
        .attr('fill', '#374151')
        .attr('text-anchor', 'middle')
        .attr('dy', -13)
        .style('pointer-events', 'none');

      // Tick
      simulation.on('tick', () => {
        link
          .attr('x1', d => d.source.x)
          .attr('y1', d => d.source.y)
          .attr('x2', d => d.target.x)
          .attr('y2', d => d.target.y);

        node
          .attr('cx', d => d.x)
          .attr('cy', d => d.y);

        label
          .attr('x', d => d.x)
          .attr('y', d => d.y);
      });

      // Click background to deselect
      svg.on('click', () => setSelectedNode(null));

      setLoading(false);
    }).catch(err => {
      console.error('Failed to fetch graph data:', err);
      setLoading(false);
      alert(`Failed to load graph. Make sure the backend is running at ${API}\n\nError: ${err.message}`);
    });
  }, []);

  // Highlight nodes and connected edges when chat references them
  useEffect(() => {
    if (!nodeCirclesRef.current || !highlightedNodes.length) return;

    const matchedIds = new Set();

    // Highlight nodes
    nodeCirclesRef.current
      .attr('stroke', d => {
        const isMatched = highlightedNodes.some(h => d.id.includes(h) || (d.data && Object.values(d.data).includes(h)));
        if (isMatched) matchedIds.add(d.id);
        return isMatched ? '#facc15' : '#ffffff';
      })
      .attr('stroke-width', d =>
        highlightedNodes.some(h => d.id.includes(h) || (d.data && Object.values(d.data).includes(h)))
          ? 3 : 1.5
      );

    // Highlight edges that connect matched nodes
    if (linksRef.current && linksDataRef.current.length) {
      linksRef.current
        .attr('stroke', link => {
          const sourceId = link.source.id || link.source;
          const targetId = link.target.id || link.target;
          return matchedIds.has(sourceId) && matchedIds.has(targetId) ? '#fbbf24' : '#c8d6e5';
        })
        .attr('stroke-width', link => {
          const sourceId = link.source.id || link.source;
          const targetId = link.target.id || link.target;
          return matchedIds.has(sourceId) && matchedIds.has(targetId) ? 2.5 : 1;
        })
        .attr('stroke-opacity', link => {
          const sourceId = link.source.id || link.source;
          const targetId = link.target.id || link.target;
          return matchedIds.has(sourceId) && matchedIds.has(targetId) ? 0.9 : 0.4;
        });
    }
  }, [highlightedNodes]);

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative', background: '#f8fafc' }}>

      {loading && (
        <div style={{
          position: 'absolute', inset: 0, display: 'flex',
          alignItems: 'center', justifyContent: 'center',
          color: '#6b7280', fontSize: 14
        }}>
          Building graph...
        </div>
      )}

      <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />

      {/* Legend */}
      <div style={{
        position: 'absolute', top: 16, left: 16,
        background: 'white', border: '1px solid #e5e7eb',
        borderRadius: 10, padding: '10px 14px',
        boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
      }}>
        <div style={{ fontSize: 11, fontWeight: 600, color: '#374151', marginBottom: 6 }}>Entity Types</div>
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: color, border: '1.5px solid white', boxShadow: '0 0 0 1px ' + color }} />
            <span style={{ fontSize: 11, color: '#6b7280', textTransform: 'capitalize' }}>{type}</span>
          </div>
        ))}
        <div style={{ fontSize: 10, color: '#9ca3af', marginTop: 6, borderTop: '1px solid #f3f4f6', paddingTop: 6 }}>
          Drag to explore · Scroll to zoom
        </div>
      </div>

      {/* Node detail card */}
      {selectedNode && (
        <div style={{
          position: 'absolute',
          left: Math.min(detailPos.x, containerRef.current?.clientWidth - 280 || 400),
          top: Math.min(detailPos.y, containerRef.current?.clientHeight - 320 || 400),
          width: 260,
          background: 'white',
          border: '1px solid #e5e7eb',
          borderRadius: 10,
          padding: '14px',
          boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
          zIndex: 20,
          maxHeight: 300,
          overflowY: 'auto',
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <div>
              <div style={{
                fontSize: 11, fontWeight: 700, color: 'white',
                background: NODE_COLORS[selectedNode.type] || '#6b7280',
                borderRadius: 4, padding: '2px 7px', display: 'inline-block', marginBottom: 3
              }}>
                {selectedNode.type}
              </div>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#111827' }}>{selectedNode.label}</div>
            </div>
            <button onClick={() => setSelectedNode(null)} style={{
              background: 'none', border: 'none', cursor: 'pointer',
              color: '#9ca3af', fontSize: 16, lineHeight: 1
            }}>✕</button>
          </div>

          {nodeDetail ? (
            <div>
              {Object.entries(nodeDetail.node || {})
                .filter(([, v]) => v && v !== '')
                .slice(0, 12)
                .map(([k, v]) => (
                  <div key={k} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, gap: 8 }}>
                    <span style={{ fontSize: 11, color: '#9ca3af', flexShrink: 0 }}>{k}</span>
                    <span style={{ fontSize: 11, color: '#374151', textAlign: 'right', wordBreak: 'break-all' }}>{String(v).slice(0, 30)}</span>
                  </div>
                ))}
              {nodeDetail.items?.length > 0 && (
                <div style={{ marginTop: 8, paddingTop: 8, borderTop: '1px solid #f3f4f6' }}>
                  <div style={{ fontSize: 11, color: '#6b7280', fontWeight: 600 }}>
                    {nodeDetail.items.length} line items
                  </div>
                </div>
              )}
              <div style={{ fontSize: 10, color: '#d1d5db', marginTop: 6 }}>
                Connections: {selectedNode.data ? Object.keys(selectedNode.data).length : 0}
              </div>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: '#9ca3af' }}>Loading details...</div>
          )}
        </div>
      )}
    </div>
  );
});

export default GraphView;