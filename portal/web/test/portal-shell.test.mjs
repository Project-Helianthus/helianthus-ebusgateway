import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

function createDeferredResponse(payload) {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return {
    promise,
    resolve() {
      resolve({
        ok: true,
        status: 200,
        json: async () => payload,
      });
    },
  };
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

function createPortalShellHarness({ source, sourcePath, elements, fetchImpl }) {
  const intervalCalls = [];
  const clearedIntervals = [];
  const fetchRequests = [];
  const fetchSignals = [];

  class FakeHTMLElement {
    constructor() {
      this._isConnected = true;
    }

    get isConnected() {
      return this._isConnected !== false;
    }
  }

  const sandbox = {
    console: {
      error() {},
      log() {},
      warn() {},
    },
    document: {
      documentElement: {
        setAttribute() {},
      },
    },
    customElements: {
      define() {},
    },
    HTMLElement: FakeHTMLElement,
    localStorage: {
      getItem() {
        return null;
      },
      setItem() {},
    },
    setInterval(callback, delay) {
      const handle = { callback, delay, id: intervalCalls.length + 1 };
      intervalCalls.push(handle);
      return handle;
    },
    clearInterval(handle) {
      clearedIntervals.push(handle);
    },
    setTimeout,
    clearTimeout,
    AbortController,
    URLSearchParams,
    TextDecoder,
    TextEncoder,
  };

  sandbox.fetch = (url, requestInit) => {
    fetchRequests.push(url);
    fetchSignals.push(requestInit?.signal ?? null);
    return fetchImpl(url, requestInit);
  };
  sandbox.globalThis = sandbox;

  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, {
    filename: pathToFileURL(sourcePath).href,
  });

  const PortalShell = sandbox.__PortalShell;
  const shell = new PortalShell();
  shell._isConnected = true;
  shell.render = () => {};
  shell.bindEvents = () => {};
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];

  return {
    shell,
    intervalCalls,
    clearedIntervals,
    fetchRequests,
    fetchSignals,
  };
}

test("PortalShell ignores stale bootstrap completions when arming bus observability polling", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

  const firstHealth = createDeferredResponse({ status: "ok" });
  const firstBootstrap = createDeferredResponse({
    capabilities: { bus_observability: true },
    endpoints: { graphql: "/graphql" },
  });
  const firstBusObservability = createDeferredResponse({ status: {} });
  const secondHealth = createDeferredResponse({ status: "ok" });
  const secondBootstrap = createDeferredResponse({
    capabilities: { bus_observability: true },
    endpoints: { graphql: "/graphql" },
  });
  const secondBusObservability = createDeferredResponse({ status: {} });
  const fetchQueue = [
    firstHealth.promise,
    firstBootstrap.promise,
    firstBusObservability.promise,
    secondHealth.promise,
    secondBootstrap.promise,
    secondBusObservability.promise,
  ];

  const elements = new Map([
    ["[data-role=\"status\"]", { textContent: "" }],
    ["[data-role=\"meta\"]", { textContent: "" }],
    ["[data-role=\"bus-banner\"]", { className: "", textContent: "" }],
    ["[data-role=\"bus-observability\"]", { innerHTML: "" }],
  ]);

  const {
    shell,
    intervalCalls,
    clearedIntervals,
    fetchRequests,
    fetchSignals,
  } = createPortalShellHarness({
    source,
    sourcePath,
    elements,
    fetchImpl() {
      const next = fetchQueue.shift();
      if (!next) {
        throw new Error("unexpected fetch");
      }
      return next;
    },
  });

  shell.connectedCallback();
  firstHealth.resolve();
  firstBootstrap.resolve();
  await flush();
  shell._isConnected = false;
  shell.disconnectedCallback();
  assert.equal(fetchSignals[0]?.aborted, true, "first bootstrap health request should abort on detach");
  assert.equal(fetchSignals[1]?.aborted, true, "first bootstrap bootstrap request should abort on detach");

  shell._isConnected = true;
  shell.connectedCallback();

  secondHealth.resolve();
  secondBootstrap.resolve();
  secondBusObservability.resolve();
  await flush();
  await flush();

  assert.equal(fetchSignals[3]?.aborted, false, "reconnect should use a fresh health signal");
  assert.equal(fetchSignals[4]?.aborted, false, "reconnect should use a fresh bootstrap signal");

  const liveHandle = shell.busObservabilityInterval;
  assert.ok(liveHandle, "expected live bus observability interval after reconnect");
  assert.equal(intervalCalls.length, 1, "expected one bus observability interval after reconnect");
  assert.equal(liveHandle.delay, 3000, "bus observability interval should use the 3s poll cadence");

  firstBusObservability.resolve();
  await flush();
  await flush();

  assert.equal(shell.busObservabilityInterval, liveHandle, "stale bootstrap completion must not replace the live bus interval");
  assert.equal(intervalCalls.length, 1, "stale bootstrap completion must not arm a second bus interval");
  assert.equal(clearedIntervals.length, 0, "stale bootstrap completion must not clear the live bus interval");
  assert.deepEqual(fetchRequests, [
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/bus/observability",
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/bus/observability",
  ]);
});

test("PortalShell renders the gateway version in the topbar status", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

  const health = createDeferredResponse({ status: "ok", gateway_version: "0.6.32" });
  const bootstrap = createDeferredResponse({
    capabilities: {},
    endpoints: { graphql: "/graphql" },
  });
  const fetchQueue = [health.promise, bootstrap.promise];

  const elements = new Map([
    ["[data-role=\"status\"]", { textContent: "" }],
    ["[data-role=\"meta\"]", { textContent: "" }],
  ]);

  const { shell } = createPortalShellHarness({
    source,
    sourcePath,
    elements,
    fetchImpl() {
      const next = fetchQueue.shift();
      if (!next) {
        throw new Error("unexpected fetch");
      }
      return next;
    },
  });

  const status = shell.loadStatus();
  health.resolve();
  bootstrap.resolve();
  await status;

  assert.equal(elements.get("[data-role=\"status\"]").textContent, "Gateway ok (0.6.32)");
});

test("PortalShell renders unsupported adapter info with an unsupported label", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

  const snapshot = createDeferredResponse({
    adapter_info: {
      firmware_version: "0x31",
      info_supported: false,
      is_wifi: false,
      is_ethernet: false,
    },
  });

  const elements = new Map([
    ["[data-role=\"adapter-identity-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-telemetry-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-refresh-status\"]", { textContent: "" }],
  ]);

  const { shell } = createPortalShellHarness({
    source,
    sourcePath,
    elements,
    fetchImpl() {
      return snapshot.promise;
    },
  });

  const refresh = shell.refreshAdapterInfo();
  snapshot.resolve();
  await refresh;

  const identityBody = elements.get("[data-role=\"adapter-identity-body\"]");
  assert.ok(identityBody.innerHTML.includes("Unsupported/Unknown"), "unsupported adapter info should show an unsupported label");
  assert.ok(!identityBody.innerHTML.includes("Serial"), "unsupported adapter info must not be labeled as Serial");
});

test("PortalShell semantic refresh fails closed without shared-read bootstrap metadata", async () => {
  const sourcePath = fileURLToPath(new URL("../src/app.js", import.meta.url));
  const source = await readFile(sourcePath, "utf8");
  const list = { innerHTML: "" };
  const { shell, fetchRequests } = createPortalShellHarness({ source, sourcePath, elements: new Map(), fetchImpl() { throw new Error("unexpected legacy request"); } });
  const token = shell.beginBootstrapLifecycle();
  assert.equal(await shell.loadSemanticPreview(list, token, shell.bootstrapLifecycleAbort), false);
  assert.match(list.innerHTML, /unavailable/);
  assert.deepEqual(fetchRequests, []);
});

test("PortalShell contribution controls expose loading and error announcements", async () => {
  const source = await readFile(new URL("../src/app.js", import.meta.url), "utf8");
  const semantic = source.slice(source.indexOf('<section id="section-semantic"'), source.indexOf('<section id="section-bus"'));
  assert.match(semantic, /<button type="button" data-role="semantic-refresh">Refresh Contributions<\/button>/);
  assert.match(semantic, /data-role="semantic-notice" role="status" aria-live="polite" aria-atomic="true"/);
  assert.match(semantic, /data-role="semantic-list" role="status" aria-live="polite" aria-atomic="true" aria-busy="false"/);
});

// Synthetic admitted descriptors and shared reads exercise the production card path.
function createSemanticContributionFixture(resources) {
  const hash = (value) => createHash("sha256").update(value).digest("hex");
  const identity = { driver_id: "fixture.thermal", manifest_id: "portal.fixture", manifest_version: "1.0.0", digest: `sha256:${hash("descriptor")}` };
  const pack = { id: "helianthus.pack.thermal", version: "1.0.0" };
  const descriptor = { ...identity, descriptor_version: "helianthus.gateway.portal-descriptor/v1", groups: [], fields: [], views: [], diagnostics: [] };
  const catalog = {
    contract: "helianthus.gateway.portal-catalog/v1", catalog_revision: hash("revision"), catalog_digest: hash("catalog"), evaluation_instant: "synthetic-evaluation",
    domains: ["evse", "infrastructure", "pv", "storage", "thermal"].map((id) => ({ pack: { id: `helianthus.pack.${id}`, version: id === "storage" ? "1.1.0" : "1.0.0" }, source_state: "PRESENT" })),
    contributions: [identity], descriptors: [descriptor], resources: [], fields: [], actions: [], quarantines: [],
  };
  const assets = resources.map(({ id, fields }) => {
    const revisions = { semantic: "1", identity: "2", facts: "3", services: "4", capabilities: "5" };
    const binding = { binding_id: `binding:${id}`, source_epoch_id: "epoch:fixture", driver_generation: "1", asset_id: `asset:${id}`, state: "current" };
    const snapshot = { asset_id: binding.asset_id, snapshot_id: `snapshot:${id}`, revisions, bindings: [binding], facts: [] };
    const tuple = { snapshot_id: snapshot.snapshot_id, revisions };
    const asset = { asset_id: binding.asset_id, snapshot, evaluation: { ...tuple, evaluation_digest: "synthetic-evaluation", facts: [] }, projection: { ...tuple, dispositions: [] }, selections: [] };
    descriptor.groups.push({ id, order: descriptor.groups.length, resource_context: id, label: { default: id } });
    descriptor.views.push({ id, group: id, label: { default: id }, renderer: "summary", slot: "lens", field_ids: [], diagnostic_ids: [], order: 0 });
    for (const [label, value, unavailable = false] of fields) {
      const fieldId = `${id}.${label}`;
      const key = { pack_id: pack.id, pack_version: pack.version, fact_id: fieldId, dimensions: [] };
      const candidate = { candidate_id: `candidate:${fieldId}`, revision: "1", key, value, quality: { assertion: "native_observation", qualification: "qualified", promotion: "promoted", validity: "good", freshness: "fresh", availability: unavailable ? "unavailable" : "available", reasons: [] } };
      snapshot.facts.push({ key, candidates: [candidate], conflicts: [] });
      asset.evaluation.facts.push({ candidate_id: candidate.candidate_id, candidate_revision: candidate.revision, freshness: "fresh", effective_availability: candidate.quality.availability });
      asset.projection.dispositions.push({ kind: "fact", item_id: fieldId, outcome: "exact", source_keys: [key], loss: [] });
      descriptor.fields.push({ id: fieldId, group: id, label: { default: label }, ref: { pack, id: fieldId, version: "1.0.0" }, order: descriptor.fields.length });
      descriptor.views.at(-1).field_ids.push(fieldId);
    }
    catalog.resources.push({ id, domain: pack.id, service_id: "fixture.service", capability_id: "fixture.read", contribution_driver_id: identity.driver_id, contribution_manifest_id: identity.manifest_id, contribution_manifest_version: identity.manifest_version, state: "CURRENT", source: { asset_id: snapshot.asset_id, snapshot_id: snapshot.snapshot_id, revision: JSON.stringify(revisions), evaluation_digest: asset.evaluation.evaluation_digest, binding_id: binding.binding_id, source_epoch: binding.source_epoch_id, driver_generation: binding.driver_generation, snapshot: structuredClone(snapshot), evaluation: {}, projection: {}, selections: [] } });
    return asset;
  });
  return { catalog, assets };
}

async function renderSemanticContributionFixture(fixture) {
  const sourcePath = fileURLToPath(new URL("../src/app.js", import.meta.url));
  const source = await readFile(sourcePath, "utf8");
  const catalog = createDeferredResponse({ data: { portalCatalogV1: fixture.catalog } });
  const snapshot = createDeferredResponse({ data: { semanticSnapshot: { assets: fixture.assets } } });
  const semanticList = { innerHTML: "" };
  const { shell, fetchRequests } = createPortalShellHarness({
    source, sourcePath, elements: new Map(),
    fetchImpl(url) {
      if (url === "/fixture/catalog") return catalog.promise;
      if (url === "/fixture/graphql") return snapshot.promise;
      throw new Error(`unexpected endpoint: ${url}`);
    },
  });
  shell._portalCatalogEndpoint = "/fixture/catalog";
  shell._semanticGraphqlEndpoint = "/fixture/graphql";
  shell._portalRead = { snapshot_operation: "semanticSnapshot", unavailable_operations: [] };
  const token = shell.beginBootstrapLifecycle();
  const render = shell.loadSemanticPreview(semanticList, token, shell.bootstrapLifecycleAbort);
  catalog.resolve();
  snapshot.resolve();
  assert.equal(await render, true);
  assert.deepEqual(fetchRequests, ["/fixture/catalog", "/fixture/graphql"]);
  return semanticList.innerHTML;
}

test("PortalShell renders promoted false and zero values distinctly from unavailable", async () => {
  const zero = { kind: "quantity", quantity: { number: { coefficient: "0", exponent10: 0 }, unit: "unit.degree_celsius" } };
  const off = { kind: "boolean", boolean: false };
  const auto = { kind: "text", text: "auto" };
  const empty = { kind: "text", text: "" };
  const fixture = createSemanticContributionFixture([
    { id: "Kitchen", fields: [["current", zero], ["target", zero], ["mode_changeable", off], ["mode", auto]] },
    { id: "DHW", fields: [["current", zero], ["target", zero], ["mode_changeable", off], ["overrun", off], ["mode", auto]] },
    { id: "System", fields: [["outdoor", zero], ["brand", empty], ["vendor", empty]] },
    { id: "Office", fields: [["current", zero, true], ["target", zero, true]] },
  ]);
  const html = await renderSemanticContributionFixture(fixture);
  const card = (id) => html.split('<li class="portal-catalog-resource">').find((row) => row.includes(` — ${id}</strong>`));
  for (const id of ["Kitchen", "DHW"]) {
    assert.match(card(id), /current: 0e0 unit.degree_celsius/);
    assert.match(card(id), /target: 0e0 unit.degree_celsius/);
    assert.match(card(id), /mode_changeable: false/);
    assert.match(card(id), /mode: auto/);
    assert.doesNotMatch(card(id), /withheld|availability=unavailable/);
  }
  assert.match(card("DHW"), /overrun: false/);
  assert.match(card("System"), /outdoor: 0e0 unit.degree_celsius/);
  assert.match(card("System"), /brand: &lt;empty&gt;/);
  assert.match(card("System"), /vendor: &lt;empty&gt;/);
  assert.match(card("Office"), /current: withheld;[^<]*availability=unavailable/);
  assert.match(card("Office"), /target: withheld;[^<]*availability=unavailable/);
  assert.doesNotMatch(card("Office"), /0e0|: false/);
});

test("PortalShell renders the provider-owned FM5 degraded verdict", async () => {
  for (const reason of ["EVIDENCE_STALE", "INCOHERENT_ACQUISITION"]) {
    const fixture = createSemanticContributionFixture([{ id: "FM5", fields: [
      ["mode", { kind: "text", text: "GPIO_ONLY" }],
      ["reason", { kind: "text", text: reason }],
      ["revision", { kind: "text", text: "fm5-g7-a42" }],
    ] }]);
    for (const fact of fixture.assets[0].snapshot.facts) fact.candidates[0].quality.availability = "degraded";
    for (const fact of fixture.assets[0].evaluation.facts) fact.effective_availability = "degraded";
    const html = await renderSemanticContributionFixture(fixture);
    assert.match(html, /mode: GPIO_ONLY/);
    assert.match(html, new RegExp(`reason: ${reason}`));
    assert.match(html, /revision: fm5-g7-a42/);
    assert.match(html, /availability=degraded/);
    assert.match(html, new RegExp(`mode: GPIO_ONLY.*reason: ${reason}.*revision: fm5-g7-a42`));
  }
});
