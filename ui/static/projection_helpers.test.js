const test = require("node:test");
const assert = require("node:assert/strict");
const {
  buildProjectionGraph,
  layoutProjectionGraph,
  computeNodeDepths,
} = require("./projection_helpers");

test("buildProjectionGraph indexes nodes and roots", () => {
  const device = { address: 12, label: "0x0C ACME" };
  const projection = {
    plane: "Service",
    nodes: [
      { id: "root", path: "/root", canonicalPath: "/root" },
      { id: "child", path: "/root/child", canonicalPath: "/root/child" },
      { id: "sibling", path: "/sibling", canonicalPath: "/sibling" },
    ],
    edges: [{ id: "e1", from: "root", to: "child" }],
  };
  const graph = buildProjectionGraph(device, projection);
  assert.equal(graph.nodes.length, 3);
  assert.deepEqual(new Set(graph.roots), new Set(["root", "sibling"]));
  assert.ok(graph.nodeById.has("root"));
  assert.equal(graph.nodeById.get("root").plane, "Service");
  assert.equal(graph.nodeById.get("root").deviceAddress, 12);
});

test("computeNodeDepths assigns deeper levels to children", () => {
  const device = { address: 1, label: "0x01 Demo" };
  const projection = {
    plane: "Observability",
    nodes: [
      { id: "a", path: "/a", canonicalPath: "/a" },
      { id: "b", path: "/a/b", canonicalPath: "/a/b" },
      { id: "c", path: "/a/b/c", canonicalPath: "/a/b/c" },
    ],
    edges: [
      { id: "e1", from: "a", to: "b" },
      { id: "e2", from: "b", to: "c" },
    ],
  };
  const graph = buildProjectionGraph(device, projection);
  const depths = computeNodeDepths(graph);
  assert.equal(depths.get("a"), 0);
  assert.equal(depths.get("b"), 1);
  assert.equal(depths.get("c"), 2);
});

test("layoutProjectionGraph sorts nodes within a depth by canonical path", () => {
  const device = { address: 1, label: "0x01 Demo" };
  const projection = {
    plane: "Service",
    nodes: [
      { id: "b", path: "/b", canonicalPath: "/b" },
      { id: "a", path: "/a", canonicalPath: "/a" },
    ],
    edges: [],
  };
  const graph = buildProjectionGraph(device, projection);
  const layout = layoutProjectionGraph(graph, {
    nodeSpacingY: 50,
    nodeSpacingX: 200,
    nodeWidth: 160,
    nodeHeight: 24,
    padding: 10,
  });
  const nodeA = layout.nodes.find((node) => node.id === "a");
  const nodeB = layout.nodes.find((node) => node.id === "b");
  assert.ok(nodeA.y < nodeB.y);
});

test("layoutProjectionGraph generates edge paths", () => {
  const device = { address: 1, label: "0x01 Demo" };
  const projection = {
    plane: "Service",
    nodes: [
      { id: "root", path: "/root", canonicalPath: "/root" },
      { id: "child", path: "/root/child", canonicalPath: "/root/child" },
    ],
    edges: [{ id: "e1", from: "root", to: "child" }],
  };
  const graph = buildProjectionGraph(device, projection);
  const layout = layoutProjectionGraph(graph);
  assert.equal(layout.edges.length, 1);
  assert.match(layout.edges[0].path, /^M\s/);
});
