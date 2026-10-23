// The Portal page resolves every request against the base it was served
// from. Browsers resolve relative fetch/EventSource URLs against the document
// base (<base href>), so this harness records each request resolved the same
// way and asserts it stays under the served prefix. The Ingress token is
// synthetic.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

const INGRESS = "/api/hassio_ingress/fixture-token_0";
const sourcePath = fileURLToPath(new URL("../src/app.js", import.meta.url));

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

// Mirrors the handler's composition: gateway endpoints are prefix + endpoint
// and Portal routes are prefix + mount + suffix.
function bootstrapFor(prefix, overrides = {}) {
  const base = `${prefix}/portal/`;
  return {
    capabilities: {
      registry: true, semantic: true, bus_observability: true, projection: true, search: true,
      stream: true, timeline: true, provenance: true, snapshots: true, sessions: true,
      eebus_admin: true, semantic_pv: true,
    },
    endpoints: {
      graphql: `${prefix}/graphql`,
      portal_catalog: `${prefix}/graphql/portal/v1`,
      subscriptions: `${prefix}/graphql/subscriptions`,
      eebus_admin: `${prefix}/admin/eebus/v1`,
      stream: `${base}api/v1/stream`,
      ...overrides,
    },
    portal_read: { base_path: base, snapshot_operation: "semanticSnapshot", stream_endpoint: null, unavailable_operations: [] },
  };
}

function genericElement() {
  return {
    innerHTML: "", textContent: "", value: "", hidden: false, disabled: false, dataset: {},
    classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
    setAttribute() {}, getAttribute() { return null; }, removeAttribute() {},
    addEventListener() {}, querySelector() { return null; }, querySelectorAll() { return []; },
    focus() {}, append() {}, replaceChildren() {},
  };
}

async function createHarness({ documentURL, baseHref, bootstrap }) {
  const source = await readFile(sourcePath, "utf8");
  // The document base the browser uses for every relative request.
  const baseURL = new URL(baseHref, documentURL);
  const requests = [];
  const record = (kind, target, init) => {
    const resolved = new URL(String(target), baseURL);
    requests.push({ kind, target: String(target), method: init?.method || "GET", origin: resolved.origin, path: resolved.pathname });
    return resolved;
  };
  class FakeHTMLElement {
    constructor() { this._isConnected = true; }
    get isConnected() { return this._isConnected !== false; }
  }
  class FakeEventSource {
    constructor(target) { this.url = record("eventsource", target).href; }
    addEventListener() {}
    close() {}
  }
  const json = (payload, status = 200) => ({ ok: status < 400, status, json: async () => payload });
  const sandbox = {
    console: { error() {}, log() {}, warn() {} },
    document: { baseURI: baseURL.href, visibilityState: "visible", documentElement: { setAttribute() {} }, addEventListener() {}, removeEventListener() {}, createElement: genericElement },
    customElements: { define() {} },
    HTMLElement: FakeHTMLElement,
    localStorage: { getItem() { return null; }, setItem() {} },
    crypto: { randomUUID: () => "fixture-idempotency-key" },
    setInterval() { return {}; }, clearInterval() {},
    setTimeout, clearTimeout, AbortController, URLSearchParams, TextDecoder, TextEncoder,
    EventSource: FakeEventSource,
    fetch: async (target, init) => {
      const resolved = record("fetch", target, init);
      if (resolved.pathname.endsWith("/api/v1/health")) return json({ status: "ok", gateway_version: "fixture" });
      if (resolved.pathname.endsWith("/api/v1/bootstrap")) return json(bootstrap);
      if (resolved.pathname.endsWith("/status")) return json({ state_revision: 1 });
      return json({});
    },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, { filename: pathToFileURL(sourcePath).href });
  const shell = new sandbox.__PortalShell();
  shell._isConnected = true;
  shell.render = () => {};
  shell.bindEvents = () => {};
  const elements = new Map();
  shell.querySelector = (selector) => {
    if (!elements.has(selector)) elements.set(selector, genericElement());
    return elements.get(selector);
  };
  shell.querySelectorAll = () => [];
  return { shell, requests, baseURL };
}

// Drives the page's request builders: bootstrap-time loads, the live stream,
// the catalog, GraphQL, the eeBUS admin boundary and representative relative
// API calls.
async function exerciseRequests(shell) {
  const token = shell.beginBootstrapLifecycle();
  await shell.loadStatus(token, shell.bootstrapLifecycleAbort);
  await flush();
  await shell.refreshPortalCatalog();
  await shell._gqlRequest("query Fixture { __typename }", {});
  await shell.eebusAdminFetch("/status");
  await shell.refreshTimeline(token, shell.bootstrapLifecycleAbort);
  await flush();
}

const kindsExercised = (requests) => new Set(requests.map(({ kind, path }) => `${kind} ${path}`));

test("Portal under an Ingress prefix builds every request under that prefix", async () => {
  const documentURL = `https://ha.example.invalid${INGRESS}/portal/`;
  const { shell, requests } = await createHarness({ documentURL, baseHref: `${INGRESS}/portal/`, bootstrap: bootstrapFor(INGRESS) });
  await exerciseRequests(shell);

  assert.ok(requests.length >= 8, `too few requests exercised: ${requests.length}`);
  for (const request of requests) {
    assert.equal(request.origin, "https://ha.example.invalid", `${request.target} left the page origin`);
    assert.ok(request.path.startsWith(`${INGRESS}/`), `${request.kind} ${request.target} resolved to ${request.path} outside ${INGRESS}`);
  }
  const seen = kindsExercised(requests);
  for (const expected of [
    `fetch ${INGRESS}/portal/api/v1/health`,
    `fetch ${INGRESS}/portal/api/v1/bootstrap`,
    `eventsource ${INGRESS}/portal/api/v1/stream`,
    `fetch ${INGRESS}/graphql/portal/v1`,
    `fetch ${INGRESS}/graphql`,
    `fetch ${INGRESS}/admin/eebus/v1/status`,
    `fetch ${INGRESS}/portal/api/v1/timeline/events`,
  ]) {
    assert.ok(seen.has(expected), `missing ${expected}; saw ${[...seen].join(", ")}`);
  }
});

test("Portal on the direct port keeps its root endpoints unchanged", async () => {
  const documentURL = "http://gateway.invalid:8080/portal/";
  const { shell, requests } = await createHarness({ documentURL, baseHref: "/portal/", bootstrap: bootstrapFor("") });
  await exerciseRequests(shell);

  const seen = kindsExercised(requests);
  for (const expected of ["fetch /portal/api/v1/bootstrap", "eventsource /portal/api/v1/stream", "fetch /graphql/portal/v1", "fetch /graphql", "fetch /admin/eebus/v1/status"]) {
    assert.ok(seen.has(expected), `missing ${expected}; saw ${[...seen].join(", ")}`);
  }
  for (const request of requests) {
    assert.ok(/^\/(portal\/|graphql(\/|$)|admin\/eebus\/v1\/)/.test(request.path), `${request.target} resolved to ${request.path}`);
  }
});

test("Portal never falls back to a root endpoint for a missing or unsafe bootstrap endpoint", async () => {
  for (const unsafe of [undefined, 42, "graphql", "//outside.invalid/graphql", "https://outside.invalid/graphql", `${INGRESS}/../graphql`, `${INGRESS}/graph ql`, `${INGRESS}//graphql`]) {
    const bootstrap = bootstrapFor(INGRESS, { graphql: unsafe, portal_catalog: unsafe, eebus_admin: unsafe });
    const { shell, requests } = await createHarness({
      documentURL: `https://ha.example.invalid${INGRESS}/portal/`, baseHref: `${INGRESS}/portal/`, bootstrap,
    });
    const token = shell.beginBootstrapLifecycle();
    await shell.loadStatus(token, shell.bootstrapLifecycleAbort);
    await flush();
    assert.equal(shell._graphqlEndpoint, "", `graphql ${String(unsafe)} admitted`);
    assert.equal(shell._portalCatalogEndpoint, "", `catalog ${String(unsafe)} admitted`);
    assert.equal(shell._eebusAdminPath, "", `eeBUS admin ${String(unsafe)} admitted`);
    const before = requests.length;
    await assert.rejects(shell._gqlRequest("query Fixture { __typename }", {}), /GraphQL endpoint unavailable/);
    await assert.rejects(shell.eebusAdminFetch("/status"), /eeBUS admin boundary unavailable/);
    assert.equal(await shell.refreshPortalCatalog(), false);
    assert.equal(requests.length, before, `a request was issued without a safe endpoint for ${String(unsafe)}`);
    for (const request of requests) {
      assert.ok(request.path.startsWith(`${INGRESS}/portal/`), `${request.target} resolved to ${request.path}`);
    }
  }
});
