import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, "../src/app.js");

class FakeNode {
  constructor(tag = "div") {
    this.tagName = tag;
    this.children = [];
    this.attributes = {};
    this.textContent = "";
    this.disabled = false;
    this.hidden = false;
    this.className = "";
  }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; this.textContent = ""; }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  querySelector() { return null; }
}

function validCatalog() {
  const source = {
    asset_id: "asset-1", snapshot_id: "snap-1", revision: "rev-1", evaluation_digest: "eval-1",
    binding_id: "binding-1", source_epoch: "epoch-1", driver_generation: 7,
    snapshot: {
      sources: [{ source_id: "modbus-source", source_epoch_id: "epoch-1", protocol_id: "modbus", profile_id: "fronius", profile_version: "1.0.0", state: "current", revision: "7" }],
      bindings: [{ binding_id: "binding-1", asset_id: "asset-1", source_id: "modbus-source", source_epoch_id: "epoch-1", driver_generation: "7", state: "current", revision: "7" }],
      facts: [{ asset_id: "asset-1", candidates: [{ candidate_id: "candidate-1", quality: { qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" }, revision: "7" }], conflicts: [], revision: "7" }],
      fences: [{ source_id: "old-source", source_epoch_id: "old-epoch", driver_generation: "6", reason: "driver_replaced", revision: "6" }],
      cursors: [{ source_id: "modbus-source", source_epoch_id: "epoch-1", driver_generation: "7", fenced: false, last_sequence: "7" }],
    },
    evaluation: { context: { evaluated_at: { unix_nanoseconds: "1789516800000000000", clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: "7" } }, facts: [{ candidate_id: "candidate-1", candidate_revision: "7", freshness: "fresh", effective_availability: "available" }] },
    selections: [{ selected_candidate: "candidate-1" }], projection: { contract: "helianthus.semantic.projection/v1", snapshot_id: "snap-1", dispositions: [{ item_id: "pv.ac.frequency", outcome: "transformed", loss: [{ kind: "precision", source_items: ["pv.native.frequency"] }, { kind: "provenance", source_items: ["pv.native.frequency"] }] }] },
  };
  const identity = (driver, manifest) => ({ driver_id: driver, manifest_id: manifest, manifest_version: "1.0.0", digest: `sha256:${"a".repeat(64)}` });
  const resource = (id, domain, driver, manifest) => ({
    id, domain, service_id: `${domain}.service`, capability_id: `${domain}.capability`,
    contribution_driver_id: driver, contribution_manifest_id: manifest, contribution_manifest_version: "1.0.0",
    source, state: "CURRENT",
  });
  const field = (id, resourceID, domain, driver, manifest, value) => ({
    id, resource_id: resourceID, definition_id: `${domain}.field`, unit_id: "unit.percent",
    contribution_driver_id: driver, contribution_manifest_id: manifest, contribution_manifest_version: "1.0.0",
    value, quality: "GOOD", projection: "LOSSLESS",
  });
  return {
    contract: "helianthus.gateway.portal-catalog/v1", catalog_revision: "c".repeat(64), catalog_digest: "d".repeat(64),
    evaluation_instant: "2026-09-16T00:00:00Z", authorization_scope: "public-read",
    domains: [
      { pack: { id: "helianthus.pack.evse", version: "1.0.0" }, source_state: "PRESENT" },
      { pack: { id: "helianthus.pack.infrastructure", version: "1.0.0" }, source_state: "ABSENT", reason: "source_unavailable" },
      { pack: { id: "helianthus.pack.pv", version: "1.0.0" }, source_state: "PRESENT" },
      { pack: { id: "helianthus.pack.storage", version: "1.1.0" }, source_state: "PRESENT" },
      { pack: { id: "helianthus.pack.thermal", version: "1.0.0" }, source_state: "ABSENT", reason: "source_unavailable" },
    ],
    contributions: [identity("evse.driver", "portal.evse"), identity("pv.driver", "portal.pv"), identity("storage.driver", "portal.storage")],
    resources: [
      resource("evse-1", "helianthus.pack.evse", "evse.driver", "portal.evse"),
      resource("pv-1", "helianthus.pack.pv", "pv.driver", "portal.pv"),
      resource("storage-1", "helianthus.pack.storage", "storage.driver", "portal.storage"),
    ],
    fields: [
      field("evse-field", "evse-1", "evse", "evse.driver", "portal.evse", 16),
      field("pv-field", "pv-1", "pv", "pv.driver", "portal.pv", 42),
      field("storage-field", "storage-1", "storage", "storage.driver", "portal.storage", 73),
    ],
    actions: [{ id: "catalog-action", resource_id: "pv-1", service_id: "pv.service", capability_id: "pv.capability", operation_id: "pv.read", contribution_driver_id: "pv.driver", contribution_manifest_id: "portal.pv", contribution_manifest_version: "1.0.0", source, discoverable: true, enabled: true }],
    quarantines: [{ driver_id: "bad.driver", manifest_id: "bad.manifest", manifest_version: "1.0.0", reason: "digest_conflict" }],
  };
}

async function harness(fetchImpl, elements = new Map(), timers = {}) {
  const source = await readFile(sourcePath, "utf8");
  class FakeHTMLElement { constructor() { this._isConnected = true; } get isConnected() { return this._isConnected; } }
  const calls = [];
  const sandbox = {
    console: { error() {}, log() {}, warn() {} },
    document: { documentElement: { setAttribute() {} }, createElement: (tag) => new FakeNode(tag) },
    customElements: { define() {} }, HTMLElement: FakeHTMLElement,
    localStorage: { getItem: () => null, setItem() {} }, AbortController, URLSearchParams, TextDecoder,
    setInterval: () => ({}), clearInterval() {}, setTimeout: timers.setTimeout || setTimeout, clearTimeout: timers.clearTimeout || clearTimeout,
    fetch: (url, init) => { calls.push({ url, init }); return fetchImpl(url, init); },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, { filename: pathToFileURL(sourcePath).href });
  const shell = new sandbox.__PortalShell();
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];
  return { shell, calls, source };
}

function response(payload, ok = true) { return { ok, status: ok ? 200 : 503, json: async () => payload }; }
function text(node) { return `${node.textContent || ""} ${node.children.map(text).join(" ")}`; }
function tags(node, into = []) { into.push(node); node.children.forEach((child) => tags(child, into)); return into; }
function deferred() { let resolve; return { promise: new Promise((r) => { resolve = r; }), resolve }; }
function flush() { return new Promise((resolve) => setImmediate(resolve)); }

function overBudgetCatalog() {
  const catalog = validCatalog();
  catalog.contributions = [];
  catalog.resources = [];
  catalog.fields = [];
  catalog.actions = [];
  catalog.quarantines = [];
  for (let index = 0; index < 64; index += 1) {
    const driver = `driver-${index}`;
    const manifest = `manifest-${index}`;
    catalog.contributions.push({ driver_id: driver, manifest_id: manifest, manifest_version: "1.0.0", digest: `sha256:${"a".repeat(64)}` });
    catalog.resources.push({
      ...validCatalog().resources[1], id: `resource-${index}`,
      contribution_driver_id: driver, contribution_manifest_id: manifest, contribution_manifest_version: "1.0.0",
    });
  }
  return catalog;
}

test("Portal catalog sends only the exact dedicated request with the bootstrap signal", async () => {
  const body = new FakeNode();
  const { shell, calls } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: validCatalog() } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/graphql/portal/v1");
  assert.equal(calls[0].init.method, "POST");
  assert.deepEqual(JSON.parse(calls[0].init.body), { operationName: "PortalCatalogV1", variables: {} });
  assert.notEqual(calls[0].init.signal, shell.bootstrapLifecycleAbort.signal);
  assert.equal(calls[0].init.signal.aborted, false);
});

test("Portal catalog preserves server domain order and renders admitted records with explicit evidence summaries", async () => {
  const body = new FakeNode();
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: validCatalog() } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = text(body);
  assert.match(rendered, /helianthus\.pack\.evse/);
  assert.match(rendered, /helianthus\.pack\.infrastructure.*ABSENT.*source_unavailable/);
  assert.match(rendered, /helianthus\.pack\.thermal.*ABSENT.*source_unavailable/);
  assert.match(rendered, /evse-1/);
  assert.match(rendered, /pv-1/);
  assert.match(rendered, /storage-1/);
  assert.match(rendered, /Evaluation: freshness=fresh, availability=available/);
  assert.match(rendered, /evaluated_at=\{"unix_nanoseconds":"1789516800000000000","clock_id":"wall","uncertainty_ns":"0"\}/);
  assert.match(rendered, /Provenance: sources=modbus-source@epoch-1:current/);
  assert.match(rendered, /binding-1@epoch-1:generation=7, state=current/);
  assert.match(rendered, /old-source@old-epoch:generation=6, reason=driver_replaced/);
  assert.match(rendered, /modbus-source@epoch-1:generation=7, fenced=false/);
  assert.match(rendered, /Quality: candidate-1: qualification=qualified, promotion=promoted, validity=good, availability=available, freshness=fresh/);
  assert.match(rendered, /quality=GOOD/);
  assert.match(rendered, /projection=LOSSLESS/);
  assert.match(rendered, /Projection: pv\.ac\.frequency: outcome=transformed; loss=precision,provenance/);
  assert.match(rendered, /Quarantined contributions/);
  assert.match(rendered, /bad\.driver\/bad\.manifest@1\.0\.0: digest_conflict/);
  assert.deepEqual(body.children[0].children.slice(0, 5).map((card) => text(card).match(/helianthus\.pack\.[a-z]+/)?.[0]), ["helianthus.pack.evse", "helianthus.pack.infrastructure", "helianthus.pack.pv", "helianthus.pack.storage", "helianthus.pack.thermal"]);
});

test("Portal catalog accepts long contract-valid source evidence and truncates only rendered text", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.revision = "r".repeat(2048);
  source.binding_id = "b".repeat(2048);
  source.source_epoch = "e".repeat(2048);
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /pv-1/);
  assert.doesNotMatch(text(body), /Catalog refresh failed|Catalog unavailable/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog rejects oversized bounded source identity members", async () => {
  for (const key of ["asset_id", "snapshot_id", "evaluation_digest"]) {
    const body = new FakeNode();
    const catalog = validCatalog();
    catalog.resources[1].source[key] = "x".repeat(513);
    const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
    const token = shell.beginBootstrapLifecycle();
    await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
    assert.match(text(body), /Catalog refresh failed/, key);
  }
});

test("Portal catalog marks every truncated nested SemReg collection", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.evaluation.facts = Array.from({ length: 9 }, (_, index) => ({ freshness: `fresh-${index}`, effective_availability: "available" }));
  source.snapshot.facts = Array.from({ length: 9 }, (_, fact) => ({ candidates: Array.from({ length: 9 }, (_, candidate) => ({ candidate_id: `candidate-${fact}-${candidate}`, quality: { qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" } })) }));
  source.snapshot.sources = Array.from({ length: 9 }, (_, index) => ({ source_id: `source-${index}`, source_epoch_id: `epoch-${index}`, state: "current" }));
  source.snapshot.bindings = Array.from({ length: 9 }, (_, index) => ({ binding_id: `binding-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), state: "current" }));
  source.snapshot.fences = Array.from({ length: 9 }, (_, index) => ({ source_id: `fence-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), reason: "retired" }));
  source.snapshot.cursors = Array.from({ length: 9 }, (_, index) => ({ source_id: `cursor-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), fenced: false }));
  source.projection.dispositions = Array.from({ length: 9 }, (_, index) => ({ item_id: `item-${index}`, outcome: "transformed", loss: Array.from({ length: 5 }, (_, loss) => ({ kind: `loss-${loss}` })) }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = text(body);
  for (const marker of ["+1 fact(s) omitted", "+40 candidate(s) omitted", "+1 source(s) omitted", "+1 binding(s) omitted", "+1 fence(s) omitted", "+1 cursor(s) omitted", "+1 disposition(s) omitted", "+8 loss item(s) omitted"]) {
    assert.ok(rendered.includes(marker), `missing truncation marker: ${marker}`);
  }
});

test("Portal catalog keeps duplicate bare resource IDs isolated by delimiter-containing contribution identities", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const pv = catalog.resources[1];
  const storage = catalog.resources[2];
  const pvIdentity = catalog.contributions[1];
  const storageIdentity = catalog.contributions[2];
  pvIdentity.driver_id = "a\u0000b";
  pvIdentity.manifest_id = "c";
  storageIdentity.driver_id = "a";
  storageIdentity.manifest_id = "b\u0000c";
  for (const row of [pv, catalog.fields[1], catalog.actions[0]]) {
    row.contribution_driver_id = pvIdentity.driver_id;
    row.contribution_manifest_id = pvIdentity.manifest_id;
  }
  for (const row of [storage, catalog.fields[2]]) {
    row.contribution_driver_id = storageIdentity.driver_id;
    row.contribution_manifest_id = storageIdentity.manifest_id;
  }
  pv.id = "asset";
  storage.id = "asset";
  catalog.fields[1].resource_id = "asset";
  catalog.fields[2].resource_id = "asset";
  catalog.actions[0].resource_id = "asset";
  catalog.actions.push({
    ...catalog.actions[0], id: "storage-action", resource_id: "asset", service_id: "storage.service", capability_id: "storage.capability", operation_id: "storage.read",
    contribution_driver_id: storage.contribution_driver_id, contribution_manifest_id: storage.contribution_manifest_id, contribution_manifest_version: storage.contribution_manifest_version,
  });
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const cards = tags(body).filter((node) => node.tagName === "article" && node.children.some((child) => child.tagName === "h4" && child.textContent === "asset"));
  assert.equal(cards.length, 2);
  const pvCard = cards.find((node) => text(node).includes("pv.field"));
  const storageCard = cards.find((node) => text(node).includes("storage.field"));
  assert.match(text(pvCard), /pv\.field/);
  assert.match(text(pvCard), /catalog-action/);
  assert.doesNotMatch(text(pvCard), /storage\.field|storage-action/);
  assert.match(text(storageCard), /storage\.field/);
  assert.match(text(storageCard), /storage-action/);
  assert.doesNotMatch(text(storageCard), /pv\.field|catalog-action/);
});

test("Portal catalog rejects an over-budget valid catalog without silently omitting records", async () => {
  const body = new FakeNode();
  const catalog = overBudgetCatalog();
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /too many admitted records to render safely/);
  assert.doesNotMatch(text(body), /resource-63/);
  assert.equal(tags(body).filter((node) => node.tagName === "article").length, 0);
});

test("Portal catalog retains the prior cards and announces an over-budget refresh rejection", async () => {
  const body = new FakeNode();
  const notice = new FakeNode();
  const responses = [response({ data: { portalCatalogV1: validCatalog() } }), response({ data: { portalCatalogV1: overBudgetCatalog() } })];
  const { shell } = await harness(() => Promise.resolve(responses.shift()), new Map([
    ["[data-role=\"portal-catalog-body\"]", body],
    ["[data-role=\"portal-catalog-notice\"]", notice],
  ]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /pv-1/);
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /pv-1/);
  assert.match(notice.textContent, /rejected: too many admitted records.*last valid catalog snapshot/);
  assert.equal(notice.hidden, false);
});

test("Portal bootstrap does not await a hung catalog request and timeout reports unavailable without aborting bootstrap", async () => {
  const body = new FakeNode();
  const notice = new FakeNode();
  const status = new FakeNode();
  const meta = new FakeNode();
  const timers = [];
  const { shell } = await harness((url, init = {}) => {
    if (url === "api/v1/health") return Promise.resolve(response({ status: "ok", gateway_version: "test" }));
    if (url === "api/v1/bootstrap") return Promise.resolve(response({ capabilities: {}, endpoints: { graphql: "/graphql", portal_catalog: "/graphql/portal/v1" } }));
    return new Promise((_resolve, reject) => init.signal.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true }));
  }, new Map([
    ["[data-role=\"portal-catalog-body\"]", body],
    ["[data-role=\"portal-catalog-notice\"]", notice],
    ["[data-role=\"status\"]", status],
    ["[data-role=\"meta\"]", meta],
  ]), {
    setTimeout(callback, delay) { const timer = { callback, delay, cleared: false }; timers.push(timer); return timer; },
    clearTimeout(timer) { timer.cleared = true; },
  });
  const token = shell.beginBootstrapLifecycle();
  await shell.loadStatus(token, shell.bootstrapLifecycleAbort);
  assert.equal(status.textContent, "Gateway ok (test)");
  assert.match(meta.textContent, /Portal capabilities/);
  assert.equal(timers.length, 1);
  assert.equal(timers[0].delay, 5000);
  assert.equal(shell.bootstrapLifecycleAbort.signal.aborted, false);
  timers[0].callback();
  await flush();
  assert.match(text(body), /Catalog refresh timed out/);
  assert.match(notice.textContent, /Catalog refresh timed out/);
  assert.equal(shell.bootstrapLifecycleAbort.signal.aborted, false);
  assert.equal(timers[0].cleared, true);
});

test("Portal catalog refresh control and catalog navigation fetch a newer snapshot and retain it after failure", async () => {
  const body = new FakeNode();
  const refresh = { disabled: true, addEventListener(_event, handler) { this.handler = handler; } };
  const first = validCatalog();
  const second = validCatalog();
  second.resources[1].id = "newest-pv";
  second.fields[1].resource_id = "newest-pv";
  second.actions[0].resource_id = "newest-pv";
  const responses = [response({ data: { portalCatalogV1: first } }), response({ data: { portalCatalogV1: second } }), response({}, false)];
  const elements = new Map([
    ["[data-role=\"portal-catalog-body\"]", body],
    ["[data-role=\"portal-catalog-refresh\"]", refresh],
  ]);
  const { shell } = await harness(() => Promise.resolve(responses.shift()), elements);
  const token = shell.beginBootstrapLifecycle();
  shell._portalCatalogEndpoint = "/graphql/portal/v1";
  shell.bindExplorerEvents = () => {};
  shell.bindL7CatalogEvents = () => {};
  shell.bindVaillantB503Events = () => {};
  shell.bindEEBusAdminEvents = () => {};
  shell.bindEvents();
  shell.setPortalCatalogRefreshState(true);
  assert.equal(refresh.disabled, false);
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  refresh.handler();
  await flush();
  assert.match(text(body), /newest-pv/);
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /newest-pv/);

  const navCalls = [];
  shell.refreshPortalCatalog = (...args) => { navCalls.push(args); return Promise.resolve(true); };
  shell.activateSection("section-portal-catalog");
  assert.equal(navCalls.length, 1);
  assert.equal(navCalls[0].length, 0);
});

test("Portal catalog fails closed for GraphQL, malformed, wrong-pack-version, and oversized records without inventing a value", async () => {
  const wrongPackVersion = validCatalog();
  wrongPackVersion.domains[2].pack.version = "9.9.9";
  for (const payload of [
    { errors: [{ message: "denied" }] },
    { data: { portalCatalogV1: { ...validCatalog(), fields: [{ id: "missing-everything" }] } } },
    { data: { portalCatalogV1: wrongPackVersion } },
    { data: { portalCatalogV1: { ...validCatalog(), fields: Array.from({ length: 129 }, () => validCatalog().fields[0]) } } },
  ]) {
    const body = new FakeNode();
    const { shell } = await harness(() => Promise.resolve(response(payload)), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
    const token = shell.beginBootstrapLifecycle();
    await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
    assert.match(text(body), /Catalog refresh failed/);
    assert.doesNotMatch(text(body), /pv-1/);
  }
});

test("Portal catalog makes catalog strings inert text, bounds nodes, and has no executable action", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].id = '<img src=x onerror=alert(1)>';
  catalog.fields[1].resource_id = catalog.resources[1].id;
  catalog.actions[0].resource_id = catalog.resources[1].id;
  catalog.fields[1].value = '<style>body{display:none}</style>';
  catalog.actions[0].id = 'javascript:alert(1)';
  const { shell, source } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const action = tags(body).find((node) => node.tagName === "button");
  assert.ok(action);
  assert.equal(action.disabled, true);
  assert.equal(action.attributes.type, "button");
  assert.match(text(body), /<img src=x onerror=alert\(1\)>/);
  assert.ok(text(body).includes("<style>body{display:none}</style>"));
  assert.ok(tags(body).length <= 512);
  assert.doesNotMatch(source, /PortalActionInvokeV1/);
});

test("Portal catalog keeps the latest valid UI across HTTP failure, abort, stale completion, and repeated refresh", async () => {
  const body = new FakeNode();
  const first = deferred();
  const second = deferred();
  const queue = [first.promise, second.promise, Promise.resolve(response({}, false))];
  const aborting = deferred();
  queue.push(aborting.promise);
  const { shell, calls } = await harness(() => queue.shift(), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  const refreshOne = shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const refreshTwo = shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const newer = validCatalog(); newer.resources[1].id = "newest-pv"; newer.fields[1].resource_id = "newest-pv"; newer.actions[0].resource_id = "newest-pv";
  second.resolve(response({ data: { portalCatalogV1: newer } }));
  await refreshTwo;
  first.resolve(response({ data: { portalCatalogV1: validCatalog() } }));
  await refreshOne;
  assert.match(text(body), /newest-pv/);
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /newest-pv/);
  const stale = shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  shell.endBootstrapLifecycle();
  aborting.resolve(response({ data: { portalCatalogV1: validCatalog() } }));
  await stale;
  assert.equal(calls.at(-1).init.signal.aborted, true);
  assert.match(text(body), /newest-pv/);
});
