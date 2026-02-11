(() => {
  const root =
    typeof globalThis !== "undefined"
      ? globalThis
      : typeof window !== "undefined"
      ? window
      : global;

  function buildProjectionGraph(device, projection) {
    const nodes = Array.isArray(projection.nodes) ? projection.nodes : [];
    const edges = Array.isArray(projection.edges) ? projection.edges : [];
    const nodeById = new Map();
    nodes.forEach((node) => {
      nodeById.set(node.id, {
        ...node,
        plane: projection.plane,
        deviceAddress: device.address,
        deviceLabel: device.label,
      });
    });
    const children = new Map();
    const parents = new Map();
    const hasParent = new Set();
    edges.forEach((edge) => {
      if (!nodeById.has(edge.from) || !nodeById.has(edge.to)) return;
      if (!children.has(edge.from)) {
        children.set(edge.from, []);
      }
      children.get(edge.from).push(edge.to);
      hasParent.add(edge.to);
      if (!parents.has(edge.to)) {
        parents.set(edge.to, []);
      }
      parents.get(edge.to).push(edge.from);
    });
    const roots = [];
    nodes.forEach((node) => {
      if (!hasParent.has(node.id)) {
        roots.push(node.id);
      }
    });
    if (roots.length === 0 && nodes.length > 0) {
      nodes.forEach((node) => roots.push(node.id));
    }
    return {
      nodes,
      edges,
      nodeById,
      children,
      parents,
      roots,
    };
  }

  function computeNodeDepths(graph) {
    const depths = new Map();
    const queue = [];
    graph.roots.forEach((rootId) => {
      depths.set(rootId, 0);
      queue.push(rootId);
    });
    while (queue.length) {
      const nodeId = queue.shift();
      const nextDepth = (depths.get(nodeId) || 0) + 1;
      const children = graph.children.get(nodeId) || [];
      children.forEach((childId) => {
        const existing = depths.get(childId);
        if (existing === undefined || existing < nextDepth) {
          depths.set(childId, nextDepth);
          queue.push(childId);
        }
      });
    }
    graph.nodes.forEach((node) => {
      if (!depths.has(node.id)) {
        depths.set(node.id, 0);
      }
    });
    return depths;
  }

  function buildEdgePath(from, to, curvature) {
    const control = curvature ?? Math.max(40, Math.abs(to.x - from.x) / 2);
    const c1x = from.x + control;
    const c2x = to.x - control;
    return `M ${from.x} ${from.y} C ${c1x} ${from.y} ${c2x} ${to.y} ${to.x} ${to.y}`;
  }

  function layoutProjectionGraph(graph, options = {}) {
    const nodeWidth = options.nodeWidth ?? 240;
    const nodeHeight = options.nodeHeight ?? 32;
    const nodeSpacingX = options.nodeSpacingX ?? 260;
    const nodeSpacingY = options.nodeSpacingY ?? 56;
    const padding = options.padding ?? 20;
    const depths = computeNodeDepths(graph);
    const levels = new Map();
    graph.nodes.forEach((node) => {
      const depth = depths.get(node.id) ?? 0;
      if (!levels.has(depth)) {
        levels.set(depth, []);
      }
      levels.get(depth).push(node);
    });
    levels.forEach((nodes) =>
      nodes.sort((a, b) => a.canonicalPath.localeCompare(b.canonicalPath))
    );
    const levelKeys = [...levels.keys()].sort((a, b) => a - b);
    const positions = new Map();
    levelKeys.forEach((depth) => {
      const nodes = levels.get(depth);
      nodes.forEach((node, index) => {
        positions.set(node.id, {
          x: padding + depth * nodeSpacingX,
          y: padding + index * nodeSpacingY,
        });
      });
    });
    const nodes = graph.nodes.map((node) => {
      const pos = positions.get(node.id) || { x: padding, y: padding };
      return {
        ...node,
        x: pos.x,
        y: pos.y,
        width: nodeWidth,
        height: nodeHeight,
        label: node.canonicalPath,
        depth: depths.get(node.id) ?? 0,
      };
    });
    const edges = graph.edges
      .filter((edge) => positions.has(edge.from) && positions.has(edge.to))
      .map((edge) => {
        const fromPos = positions.get(edge.from);
        const toPos = positions.get(edge.to);
        const from = {
          x: fromPos.x + nodeWidth,
          y: fromPos.y + nodeHeight / 2,
        };
        const to = {
          x: toPos.x,
          y: toPos.y + nodeHeight / 2,
        };
        return {
          ...edge,
          from,
          to,
          path: buildEdgePath(from, to, options.curvature),
        };
      });
    const maxDepth = levelKeys.length ? Math.max(...levelKeys) : 0;
    const maxLevelSize = levelKeys.length
      ? Math.max(...levelKeys.map((depth) => levels.get(depth).length))
      : 0;
    const width = padding * 2 + maxDepth * nodeSpacingX + nodeWidth;
    const height =
      padding * 2 + Math.max(0, maxLevelSize - 1) * nodeSpacingY + nodeHeight;
    return {
      nodes,
      edges,
      width,
      height,
      nodeWidth,
      nodeHeight,
    };
  }

  const helpers = {
    buildProjectionGraph,
    computeNodeDepths,
    buildEdgePath,
    layoutProjectionGraph,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = helpers;
  } else {
    root.ProjectionHelpers = helpers;
  }
})();
