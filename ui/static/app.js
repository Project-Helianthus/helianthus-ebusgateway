const graphqlPath = document.body.dataset.graphql || "/graphql";
const ProjectionHelpers = window.ProjectionHelpers || {};
const { buildProjectionGraph, layoutProjectionGraph } = ProjectionHelpers;

const elements = {
  deviceList: document.getElementById("deviceList"),
  planeSelect: document.getElementById("planeSelect"),
  refreshBtn: document.getElementById("refreshBtn"),
  pauseBtn: document.getElementById("pauseBtn"),
  statusText: document.getElementById("statusText"),
  graphContainer: document.getElementById("graphContainer"),
  graphMeta: document.getElementById("graphMeta"),
  detailContainer: document.getElementById("detailContainer"),
};

const defaultPlaneOrder = ["Service", "Observability", "Debug", "Virtual"];

const state = {
  devices: [],
  graphs: new Map(),
  selectedDevice: null,
  selectedPlane: null,
  selectedNodeId: null,
  paused: false,
  intervalMs: 5000,
  intervalId: null,
  lastUpdated: null,
  error: null,
};

const svgNS = "http://www.w3.org/2000/svg";

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

function buildGraphs(devices) {
  const graphs = new Map();
  devices.forEach((device) => {
    device.projections.forEach((projection) => {
      graphs.set(graphKey(device.address, projection.plane), buildProjectionGraph(device, projection));
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

function renderStatus(graph) {
  if (!elements.statusText) return;
  if (state.error) {
    elements.statusText.textContent = `Error: ${state.error}`;
    elements.statusText.classList.add("error");
    return;
  }
  elements.statusText.classList.remove("error");
  if (!state.lastUpdated) {
    elements.statusText.textContent = "Waiting for data…";
    return;
  }
  const meta = graph ? `• ${graph.nodes.length} nodes, ${graph.edges.length} edges` : "";
  elements.statusText.textContent = `Last update ${state.lastUpdated.toLocaleTimeString()} ${meta}`;
}

function renderDeviceList() {
  elements.deviceList.innerHTML = "";
  if (!state.devices.length) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "No devices detected.";
    elements.deviceList.appendChild(empty);
    return;
  }
  state.devices.forEach((device) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "device-button";
    if (device.address === state.selectedDevice) {
      button.classList.add("selected");
    }
    button.dataset.address = device.address;
    button.textContent = device.label;
    elements.deviceList.appendChild(button);
  });
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

function renderGraph(graph) {
  elements.graphContainer.innerHTML = "";
  if (!graph || graph.nodes.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "No projection nodes for this plane.";
    elements.graphContainer.appendChild(empty);
    if (elements.graphMeta) elements.graphMeta.textContent = "";
    return;
  }
  const layout = layoutProjectionGraph(graph, {
    nodeWidth: 260,
    nodeHeight: 34,
    nodeSpacingX: 280,
    nodeSpacingY: 64,
    padding: 24,
  });
  if (elements.graphMeta) {
    elements.graphMeta.textContent = `${graph.nodes.length} nodes / ${graph.edges.length} edges`;
  }
  const svg = document.createElementNS(svgNS, "svg");
  svg.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);
  svg.classList.add("projection-svg");
  const edgesGroup = document.createElementNS(svgNS, "g");
  edgesGroup.classList.add("edges");
  layout.edges.forEach((edge) => {
    const path = document.createElementNS(svgNS, "path");
    path.setAttribute("d", edge.path);
    path.classList.add("edge");
    edgesGroup.appendChild(path);
  });
  const nodesGroup = document.createElementNS(svgNS, "g");
  nodesGroup.classList.add("nodes");
  layout.nodes.forEach((node) => {
    const group = document.createElementNS(svgNS, "g");
    group.classList.add("node");
    if (node.id === state.selectedNodeId) {
      group.classList.add("selected");
    }
    group.dataset.nodeId = node.id;
    const rect = document.createElementNS(svgNS, "rect");
    rect.setAttribute("x", node.x);
    rect.setAttribute("y", node.y);
    rect.setAttribute("width", node.width);
    rect.setAttribute("height", node.height);
    rect.setAttribute("rx", 6);
    rect.setAttribute("ry", 6);
    const text = document.createElementNS(svgNS, "text");
    text.setAttribute("x", node.x + 10);
    text.setAttribute("y", node.y + node.height / 2);
    text.setAttribute("dominant-baseline", "middle");
    text.classList.add("node-label");
    text.textContent = node.label;
    const title = document.createElementNS(svgNS, "title");
    title.textContent = node.canonicalPath;
    group.append(rect, text, title);
    nodesGroup.appendChild(group);
  });
  svg.append(edgesGroup, nodesGroup);
  elements.graphContainer.appendChild(svg);
}

function renderDetail(graph) {
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
  detail.appendChild(detailRow("ID", node.id));
  detail.appendChild(detailRow("Plane path", node.path));
  detail.appendChild(detailRow("Canonical path", node.canonicalPath));
  elements.detailContainer.appendChild(detail);
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

function render() {
  ensureSelection();
  renderDeviceList();
  renderPlaneSelect();
  const graph = currentGraph();
  renderStatus(graph);
  renderGraph(graph);
  renderDetail(graph);
}

async function loadData() {
  try {
    state.error = null;
    const data = await fetchGraph();
    const devices = normalizeDevices(data);
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

elements.deviceList.addEventListener("click", (event) => {
  const target = event.target.closest("button[data-address]");
  if (!target) return;
  state.selectedDevice = Number(target.dataset.address);
  state.selectedNodeId = null;
  render();
});

elements.planeSelect.addEventListener("change", (event) => {
  state.selectedPlane = event.target.value;
  state.selectedNodeId = null;
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

elements.graphContainer.addEventListener("click", (event) => {
  const target = event.target.closest("g[data-node-id]");
  if (!target) return;
  state.selectedNodeId = target.dataset.nodeId;
  render();
});

if (!buildProjectionGraph || !layoutProjectionGraph) {
  state.error = "Projection helpers missing (projection_helpers.js).";
  render();
} else {
  loadData();
  startAutoRefresh();
}
