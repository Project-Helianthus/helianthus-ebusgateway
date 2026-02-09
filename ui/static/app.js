const graphqlPath = document.body.dataset.graphql || "/graphql";

const elements = {
  deviceSelect: document.getElementById("deviceSelect"),
  planeSelect: document.getElementById("planeSelect"),
  searchInput: document.getElementById("searchInput"),
  searchScope: document.getElementById("searchScope"),
  refreshBtn: document.getElementById("refreshBtn"),
  pauseBtn: document.getElementById("pauseBtn"),
  snapshotBtn: document.getElementById("snapshotBtn"),
  statusBar: document.getElementById("statusBar"),
  treeContainer: document.getElementById("treeContainer"),
  detailContainer: document.getElementById("detailContainer"),
  pathList: document.getElementById("pathList"),
  crossPlaneList: document.getElementById("crossPlaneList"),
};

const defaultPlaneOrder = ["Service", "Observability", "Debug", "Virtual"];

const state = {
  devices: [],
  graphs: new Map(),
  prevGraphs: new Map(),
  snapshotGraphs: new Map(),
  selectedDevice: null,
  selectedPlane: null,
  selectedNodeId: null,
  paused: false,
  intervalMs: 5000,
  intervalId: null,
  lastUpdated: null,
  error: null,
  searchQuery: "",
  searchScope: "all",
};

function addressHex(address) {
  if (typeof address !== "number") {
    return "0x00";
  }
  return `0x${address.toString(16).padStart(2, "0").toUpperCase()}`;
}

function deviceLabel(device) {
  const parts = [addressHex(device.address)];
  if (device.manufacturer) parts.push(device.manufacturer);
  if (device.deviceId) parts.push(device.deviceId);
  return parts.join(" ");
}

function graphKey(deviceAddress, plane) {
  return `${deviceAddress}-${plane}`;
}

async function fetchGraph() {
  const query = `
    query PortalProjections {
      devices {
        address
        manufacturer
        deviceId
        projections {
          plane
          nodes { id path canonicalPath }
          edges { id from to }
        }
      }
    }
  `;
  const response = await fetch(graphqlPath, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!response.ok) {
    throw new Error(`GraphQL HTTP ${response.status}`);
  }
  const payload = await response.json();
  if (payload.errors && payload.errors.length) {
    const message = payload.errors.map((err) => err.message).join("; ");
    throw new Error(message);
  }
  return payload.data || { devices: [] };
}

function normalizeDevices(data) {
  const devices = Array.isArray(data.devices) ? data.devices : [];
  return devices.map((device) => {
    const projections = Array.isArray(device.projections) ? device.projections : [];
    return {
      address: device.address,
      label: deviceLabel(device),
      projections,
    };
  });
}

function buildGraph(device, projection) {
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

function buildGraphs(devices) {
  const graphs = new Map();
  devices.forEach((device) => {
    device.projections.forEach((projection) => {
      graphs.set(graphKey(device.address, projection.plane), buildGraph(device, projection));
    });
  });
  return graphs;
}

function ensureSelection() {
  if (!state.devices.length) {
    state.selectedDevice = null;
    state.selectedPlane = null;
    state.selectedNodeId = null;
    return;
  }
  if (!state.selectedDevice || !state.devices.find((d) => d.address === state.selectedDevice)) {
    state.selectedDevice = state.devices[0].address;
  }
  const planes = availablePlanes(state.selectedDevice);
  if (!state.selectedPlane || !planes.includes(state.selectedPlane)) {
    state.selectedPlane = planes[0] || defaultPlaneOrder[0];
  }
}

function availablePlanes(deviceAddress) {
  const device = state.devices.find((d) => d.address === deviceAddress);
  if (!device) return [];
  return device.projections.map((projection) => projection.plane);
}

function currentGraph() {
  if (!state.selectedDevice || !state.selectedPlane) return null;
  return state.graphs.get(graphKey(state.selectedDevice, state.selectedPlane)) || null;
}

function previousGraph() {
  if (!state.selectedDevice || !state.selectedPlane) return null;
  return state.prevGraphs.get(graphKey(state.selectedDevice, state.selectedPlane)) || null;
}

function snapshotGraph() {
  if (!state.selectedDevice || !state.selectedPlane) return null;
  return state.snapshotGraphs.get(graphKey(state.selectedDevice, state.selectedPlane)) || null;
}

function setSnapshot(graph) {
  if (!state.selectedDevice || !state.selectedPlane) return;
  if (graph) {
    state.snapshotGraphs.set(graphKey(state.selectedDevice, state.selectedPlane), graph);
  } else {
    state.snapshotGraphs.delete(graphKey(state.selectedDevice, state.selectedPlane));
  }
}

function computeDiff(graph, baseGraph) {
  if (!graph || !baseGraph) {
    return { newIds: new Set(), goneIds: new Set(), goneNodes: [] };
  }
  const baseIds = new Set(baseGraph.nodes.map((node) => node.id));
  const currentIds = new Set(graph.nodes.map((node) => node.id));
  const newIds = new Set();
  const goneIds = new Set();
  graph.nodes.forEach((node) => {
    if (!baseIds.has(node.id)) newIds.add(node.id);
  });
  baseGraph.nodes.forEach((node) => {
    if (!currentIds.has(node.id)) goneIds.add(node.id);
  });
  const goneNodes = [];
  baseGraph.nodes.forEach((node) => {
    if (goneIds.has(node.id)) goneNodes.push(node);
  });
  return { newIds, goneIds, goneNodes };
}

function pathLabel(path) {
  if (!path) return "";
  const parts = path.split("/").filter(Boolean);
  return parts[parts.length - 1] || path;
}

function renderStatus(graph, diff) {
  elements.statusBar.innerHTML = "";
  const items = [];
  if (state.error) {
    items.push({ label: "Error", value: state.error });
  }
  if (state.lastUpdated) {
    items.push({ label: "Last update", value: state.lastUpdated.toLocaleTimeString() });
  }
  items.push({ label: "Auto-refresh", value: state.paused ? "Paused" : "On" });
  if (snapshotGraph()) {
    items.push({ label: "Snapshot", value: "Active" });
  }
  if (graph) {
    items.push({ label: "Nodes", value: graph.nodes.length });
    items.push({ label: "Edges", value: graph.edges.length });
    items.push({ label: "New", value: diff.newIds.size });
    items.push({ label: "Removed", value: diff.goneIds.size });
  }
  items.forEach((item) => {
    const span = document.createElement("span");
    span.className = "status-item";
    const label = document.createElement("span");
    label.className = "status-label";
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "status-value";
    value.textContent = item.value;
    span.append(label, value);
    elements.statusBar.appendChild(span);
  });
}

function renderDeviceSelect() {
  elements.deviceSelect.innerHTML = "";
  state.devices.forEach((device) => {
    const option = document.createElement("option");
    option.value = device.address;
    option.textContent = device.label;
    elements.deviceSelect.appendChild(option);
  });
  if (state.selectedDevice !== null) {
    elements.deviceSelect.value = state.selectedDevice;
  }
}

function renderPlaneSelect() {
  const planes = new Set(defaultPlaneOrder);
  availablePlanes(state.selectedDevice).forEach((plane) => planes.add(plane));
  const ordered = [...planes].sort((a, b) => {
    const ai = defaultPlaneOrder.indexOf(a);
    const bi = defaultPlaneOrder.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });
  elements.planeSelect.innerHTML = "";
  ordered.forEach((plane) => {
    const option = document.createElement("option");
    option.value = plane;
    option.textContent = plane;
    elements.planeSelect.appendChild(option);
  });
  if (state.selectedPlane) {
    elements.planeSelect.value = state.selectedPlane;
  }
}

function nodeMatches(node) {
  if (!state.searchQuery) return true;
  const query = state.searchQuery.toLowerCase();
  if (state.searchScope === "path") {
    return node.path.toLowerCase().includes(query);
  }
  if (state.searchScope === "canonical") {
    return node.canonicalPath.toLowerCase().includes(query);
  }
  return (
    node.path.toLowerCase().includes(query) ||
    node.canonicalPath.toLowerCase().includes(query)
  );
}

function renderTree(graph, diff) {
  elements.treeContainer.innerHTML = "";
  if (!graph || graph.nodes.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "No nodes for this plane.";
    elements.treeContainer.appendChild(empty);
    return;
  }
  const matches = new Set();
  graph.nodes.forEach((node) => {
    if (nodeMatches(node)) matches.add(node.id);
  });
  const ul = document.createElement("ul");
  graph.roots.forEach((rootId) => {
    const li = renderNode(rootId, graph, diff, matches, new Set());
    if (li) ul.appendChild(li);
  });
  if (!ul.children.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "No matches for the current search.";
    elements.treeContainer.appendChild(empty);
    return;
  }
  elements.treeContainer.appendChild(ul);
}

function renderNode(nodeId, graph, diff, matches, trail) {
  if (trail.has(nodeId)) {
    return null;
  }
  const node = graph.nodeById.get(nodeId);
  if (!node) return null;
  const nextTrail = new Set(trail);
  nextTrail.add(nodeId);
  const children = graph.children.get(nodeId) || [];
  const childItems = [];
  children.forEach((childId) => {
    const childLi = renderNode(childId, graph, diff, matches, nextTrail);
    if (childLi) childItems.push(childLi);
  });
  const matchesSelf = matches.has(nodeId);
  if (!matchesSelf && childItems.length === 0) return null;
  const li = document.createElement("li");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "node-button";
  if (nodeId === state.selectedNodeId) {
    button.classList.add("selected");
  }
  button.dataset.nodeId = nodeId;
  const label = document.createElement("span");
  label.className = "node-label";
  label.textContent = pathLabel(node.path) || node.path;
  label.title = node.path;
  button.appendChild(label);
  if (diff.newIds.has(nodeId)) {
    const badge = document.createElement("span");
    badge.className = "badge badge-new";
    badge.textContent = "NEW";
    button.appendChild(badge);
  }
  li.appendChild(button);
  if (childItems.length) {
    const childUl = document.createElement("ul");
    childItems.forEach((child) => childUl.appendChild(child));
    li.appendChild(childUl);
  }
  return li;
}

function renderDetail(graph, diff) {
  elements.detailContainer.innerHTML = "";
  if (!graph || !state.selectedNodeId || !graph.nodeById.has(state.selectedNodeId)) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "Select a node to view details.";
    elements.detailContainer.appendChild(empty);
    return;
  }
  const node = graph.nodeById.get(state.selectedNodeId);
  const detail = document.createElement("div");
  detail.className = "detail-card";
  detail.appendChild(detailRow("Plane", node.plane));
  detail.appendChild(detailRow("Device", node.deviceLabel));
  detail.appendChild(detailRow("Node ID", node.id));
  detail.appendChild(detailRow("Raw path", node.path));
  detail.appendChild(detailRow("Canonical path", node.canonicalPath));
  const inCount = (graph.parents.get(node.id) || []).length;
  const outCount = (graph.children.get(node.id) || []).length;
  detail.appendChild(detailRow("Edges", `in ${inCount} / out ${outCount}`));
  if (diff.newIds.has(node.id)) {
    detail.appendChild(detailRow("Status", "NEW"));
  }
  elements.detailContainer.appendChild(detail);
  const crumbs = document.createElement("div");
  crumbs.className = "breadcrumbs";
  node.canonicalPath
    .split("/")
    .filter(Boolean)
    .forEach((segment) => {
      const chip = document.createElement("span");
      chip.className = "crumb";
      chip.textContent = segment;
      crumbs.appendChild(chip);
    });
  if (crumbs.children.length) {
    elements.detailContainer.appendChild(crumbs);
  }
  if (diff.goneNodes.length) {
    const gone = document.createElement("div");
    gone.className = "gone-list";
    const label = document.createElement("div");
    label.className = "section-label";
    label.textContent = "Removed since baseline";
    gone.appendChild(label);
    diff.goneNodes.slice(0, 6).forEach((oldNode) => {
      const item = document.createElement("div");
      item.className = "gone-item";
      item.textContent = oldNode.path;
      gone.appendChild(item);
    });
    if (diff.goneNodes.length > 6) {
      const more = document.createElement("div");
      more.className = "gone-item";
      more.textContent = `... +${diff.goneNodes.length - 6} more`;
      gone.appendChild(more);
    }
    elements.detailContainer.appendChild(gone);
  }
}

function detailRow(label, value) {
  const row = document.createElement("div");
  row.className = "detail-row";
  const labelSpan = document.createElement("span");
  labelSpan.className = "detail-label";
  labelSpan.textContent = label;
  const valueSpan = document.createElement("span");
  valueSpan.className = "detail-value";
  valueSpan.textContent = value;
  row.append(labelSpan, valueSpan);
  return row;
}

function renderPathList(graph) {
  elements.pathList.innerHTML = "";
  if (!graph) return;
  const nodes = graph.nodes.slice().sort((a, b) => a.canonicalPath.localeCompare(b.canonicalPath));
  nodes.forEach((node) => {
    if (!nodeMatches(node)) return;
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.dataset.nodeId = node.id;
    chip.textContent = node.canonicalPath;
    chip.title = node.path;
    elements.pathList.appendChild(chip);
  });
}

function renderCrossPlaneList() {
  elements.crossPlaneList.innerHTML = "";
  const device = state.devices.find((d) => d.address === state.selectedDevice);
  if (!device) return;
  const nodes = [];
  device.projections.forEach((projection) => {
    projection.nodes.forEach((node) => {
      nodes.push({
        plane: projection.plane,
        id: node.id,
        canonicalPath: node.canonicalPath,
        path: node.path,
      });
    });
  });
  nodes.sort((a, b) => a.canonicalPath.localeCompare(b.canonicalPath));
  nodes.forEach((node) => {
    if (state.searchQuery && !nodeMatches(node)) return;
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.dataset.nodeId = node.id;
    chip.dataset.plane = node.plane;
    chip.textContent = `${node.plane}: ${node.canonicalPath}`;
    chip.title = node.path;
    elements.crossPlaneList.appendChild(chip);
  });
}

function render() {
  ensureSelection();
  renderDeviceSelect();
  renderPlaneSelect();
  const graph = currentGraph();
  const baseline = snapshotGraph() || previousGraph();
  const diff = computeDiff(graph, baseline);
  renderStatus(graph, diff);
  renderTree(graph, diff);
  renderDetail(graph, diff);
  renderPathList(graph);
  renderCrossPlaneList();
}

async function loadData() {
  try {
    state.error = null;
    const data = await fetchGraph();
    const devices = normalizeDevices(data);
    state.prevGraphs = state.graphs;
    state.devices = devices;
    state.graphs = buildGraphs(devices);
    state.lastUpdated = new Date();
    if (state.selectedNodeId && currentGraph() && !currentGraph().nodeById.has(state.selectedNodeId)) {
      state.selectedNodeId = null;
    }
    render();
  } catch (err) {
    state.error = err instanceof Error ? err.message : String(err);
    render();
  }
}

function startAutoRefresh() {
  if (state.intervalId) {
    clearInterval(state.intervalId);
    state.intervalId = null;
  }
  if (state.paused) return;
  state.intervalId = setInterval(loadData, state.intervalMs);
}

elements.deviceSelect.addEventListener("change", (event) => {
  state.selectedDevice = Number(event.target.value);
  state.selectedNodeId = null;
  render();
});

elements.planeSelect.addEventListener("change", (event) => {
  state.selectedPlane = event.target.value;
  state.selectedNodeId = null;
  render();
});

elements.searchInput.addEventListener("input", (event) => {
  state.searchQuery = event.target.value.trim();
  render();
});

elements.searchScope.addEventListener("change", (event) => {
  state.searchScope = event.target.value;
  render();
});

elements.refreshBtn.addEventListener("click", () => {
  loadData();
});

elements.pauseBtn.addEventListener("click", () => {
  state.paused = !state.paused;
  elements.pauseBtn.textContent = state.paused ? "Resume" : "Pause";
  startAutoRefresh();
  render();
});

elements.snapshotBtn.addEventListener("click", () => {
  const graph = currentGraph();
  if (!graph) return;
  if (snapshotGraph()) {
    setSnapshot(null);
    elements.snapshotBtn.textContent = "Snapshot";
  } else {
    setSnapshot(graph);
    elements.snapshotBtn.textContent = "Clear snapshot";
  }
  render();
});

elements.treeContainer.addEventListener("click", (event) => {
  const target = event.target.closest("button[data-node-id]");
  if (!target) return;
  state.selectedNodeId = target.dataset.nodeId;
  render();
});

elements.pathList.addEventListener("click", (event) => {
  const target = event.target.closest("button[data-node-id]");
  if (!target) return;
  state.selectedNodeId = target.dataset.nodeId;
  render();
});

elements.crossPlaneList.addEventListener("click", (event) => {
  const target = event.target.closest("button[data-node-id][data-plane]");
  if (!target) return;
  state.selectedPlane = target.dataset.plane;
  state.selectedNodeId = target.dataset.nodeId;
  render();
});

loadData();
startAutoRefresh();
