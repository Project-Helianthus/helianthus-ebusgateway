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
  addEventListener(name, handler) { (this.handlers ||= {})[name] = handler; }
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
    selections: [{
      contract: "helianthus.semantic.selection/v1", snapshot_id: `sha256:${"s".repeat(64)}`,
      revisions: { semantic: "7", identity: "7", facts: "7", services: "7", capabilities: "7" }, evaluation_digest: `sha256:${"e".repeat(64)}`,
      context: { evaluated_at: { unix_nanoseconds: "1789516800000000000", clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: "7" } },
      key: { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: "pv.ac.frequency", dimensions: [] },
      policy_id: "promotion-policy", policy_version: "1.0.0", selected_candidate: "candidate-1", candidate_revision: "7", presentation_only: true,
    }], projection: { contract: "helianthus.semantic.projection/v1", snapshot_id: "snap-1", dispositions: [{ item_id: "pv.ac.frequency", outcome: "transformed", loss: [{ kind: "precision", source_items: ["pv.native.frequency"] }, { kind: "provenance", source_items: ["pv.native.frequency"] }] }] },
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
function control(body, label) {
  const node = tags(body).find((candidate) => candidate.tagName === "button" && candidate.textContent === label);
  assert.ok(node, `missing ${label} control`);
  return node;
}
function advanceEvidenceUntil(body, expected, maxPages = 32) {
  for (let page = 0; page < maxPages && !text(body).includes(expected); page += 1) {
    const next = control(body, "Next Evidence");
    assert.equal(next.disabled, false, `could not reach ${expected}`);
    next.handlers.click();
  }
  assert.ok(text(body).includes(expected), `missing paged evidence: ${expected}`);
}
function collectEvidencePages(body, maxPages = 32) {
  const pages = [];
  for (let page = 0; page < maxPages; page += 1) {
    pages.push(text(body));
    const next = control(body, "Next Evidence");
    if (next.disabled) return pages.join(" ");
    next.handlers.click();
  }
  assert.fail("evidence pager exceeded bounded test window");
}
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
  catalog.quarantines = Array.from({ length: 64 }, (_, index) => ({ driver_id: `quarantined-driver-${index}`, manifest_id: `quarantined-manifest-${index}`, manifest_version: "1.0.0", reason: "not_admitted" }));
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
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /helianthus\.pack\.evse/);
  assert.match(rendered, /helianthus\.pack\.infrastructure.*ABSENT.*source_unavailable/);
  assert.match(rendered, /helianthus\.pack\.thermal.*ABSENT.*source_unavailable/);
  assert.match(rendered, /evse-1/);
  assert.match(rendered, /pv-1/);
  assert.match(rendered, /storage-1/);
  assert.match(rendered, /Evaluation: candidate_id=candidate-1; candidate_revision=7; freshness=fresh; availability=available/);
  assert.match(rendered, /evaluated_at=\{"unix_nanoseconds":"1789516800000000000","clock_id":"wall","uncertainty_ns":"0"\}/);
  assert.match(rendered, /Provenance source: modbus-source@epoch-1; state=current/);
  assert.match(rendered, /Provenance binding: binding-1@epoch-1; generation=7; state=current/);
  assert.match(rendered, /Lifecycle fence: old-source@old-epoch; generation=6; reason=driver_replaced/);
  assert.match(rendered, /Lifecycle cursor: modbus-source@epoch-1; generation=7; fenced=false/);
  assert.match(rendered, /Quality: candidate=candidate-1; assertion=unavailable; qualification=qualified; promotion=promoted; validity=good; availability=available; freshness=fresh/);
  assert.match(rendered, /Selection: key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; selected_candidate=candidate-1; candidate_revision=7; policy=promotion-policy@1\.0\.0/);
  assert.match(rendered, /Action: catalog-action; server_enabled=true; read-only/);
  assert.match(rendered, /quality=GOOD/);
  assert.match(rendered, /projection=LOSSLESS/);
  assert.match(rendered, /Projection: item=pv\.ac\.frequency; kind=unavailable; outcome=transformed; reason=unavailable; source_keys=0/);
  assert.match(rendered, /Projection loss: item=pv\.ac\.frequency; kind=precision/);
  assert.match(rendered, /Projection loss: item=pv\.ac\.frequency; kind=provenance/);
  assert.match(rendered, /Catalog revision: c+/);
  assert.match(rendered, /Catalog digest: d+/);
  assert.match(rendered, /Catalog evaluation instant: 2026-09-16T00:00:00Z/);
  assert.match(rendered, /Catalog authorization scope: public-read/);
  assert.match(rendered, /Contribution digest: pv\.driver\/portal\.pv@1\.0\.0; digest=sha256:a+/);
  assert.equal((rendered.match(/Contribution digest:/g) || []).length, 3);
  assert.match(rendered, /Quarantined contributions/);
  assert.match(rendered, /bad\.driver\/bad\.manifest@1\.0\.0: digest_conflict/);
  assert.deepEqual(body.children[0].children.slice(0, 5).map((card) => text(card).match(/helianthus\.pack\.[a-z]+/)?.[0]), ["helianthus.pack.evse", "helianthus.pack.infrastructure", "helianthus.pack.pv", "helianthus.pack.storage", "helianthus.pack.thermal"]);
});

test("Portal catalog renders bounded SemReg disposition kind, reason, and source-key evidence", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.projection.dispositions = [{
    kind: "fact", item_id: "pv.ac.frequency", outcome: "withheld", reason: "policy.withheld",
    source_keys: Array.from({ length: 9 }, (_, index) => ({ pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: `pv.source-${index}`, dimensions: [] })),
    loss: [],
  }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Projection: item=pv\.ac\.frequency; kind=fact; outcome=withheld; reason=policy\.withheld; source_keys=8; \+1 source key\(s\) omitted/);
  assert.match(rendered, /Projection source key: item=pv\.ac\.frequency; source_key=helianthus\.pack\.pv@1\.0\.0\/pv\.source-7/);
  assert.doesNotMatch(rendered, /source_key=.*pv\.source-8/);
});

test("Portal catalog distinguishes production FactKeys by bounded dimensions", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const key = { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: "pv.ac.frequency" };
  catalog.resources[1].source.projection.dispositions = [{
    kind: "fact", item_id: "pv.ac.frequency", outcome: "transformed", source_keys: [
      { ...key, dimensions: [{ id: "phase", value: "L1" }] },
      { ...key, dimensions: [{ id: "phase", value: "L2" }] },
      { ...key, dimensions: Array.from({ length: 9 }, (_, index) => ({ id: `dimension-${index}`, value: `value-${index}` })) },
    ], loss: [],
  }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /source_key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; id=phase; value=L1/);
  assert.match(rendered, /source_key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; id=phase; value=L2/);
  assert.match(rendered, /id=dimension-7; value=value-7/);
  assert.match(rendered, /\+1 dimension\(s\) omitted/);
  assert.doesNotMatch(rendered, /id=dimension-8; value=value-8/);
});

test("Portal catalog identifies each evaluated candidate and revision", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.evaluation.facts = [
    { candidate_id: "candidate-a", candidate_revision: "4", freshness: "fresh", effective_availability: "available" },
    { candidate_id: "candidate-b", candidate_revision: "9", freshness: "stale", effective_availability: "degraded" },
  ];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Evaluation: candidate_id=candidate-a; candidate_revision=4; freshness=fresh; availability=available/);
  assert.match(rendered, /Evaluation: candidate_id=candidate-b; candidate_revision=9; freshness=stale; availability=degraded/);
});

test("Portal catalog distinguishes same-kind projection losses by description and source evidence", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.projection.dispositions = [{ item_id: "pv.ac.frequency", outcome: "transformed", loss: [
    { kind: "precision", description: "rounded from meter A", source_items: Array.from({ length: 9 }, (_, index) => `meter-a-${index}`) },
    { kind: "precision", description: "rounded from meter B", source_items: ["meter-b-0"] },
  ] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Projection loss: item=pv\.ac\.frequency; kind=precision; description=rounded from meter A; source_items=8; \+1 source item\(s\) omitted/);
  assert.match(rendered, /Projection loss source: item=pv\.ac\.frequency; kind=precision; source_item=meter-a-7/);
  assert.doesNotMatch(rendered, /source_item=meter-a-8/);
  assert.match(rendered, /Projection loss: item=pv\.ac\.frequency; kind=precision; description=rounded from meter B; source_items=1/);
  assert.match(rendered, /Projection loss source: item=pv\.ac\.frequency; kind=precision; source_item=meter-b-0/);
});

test("Portal catalog renders each bounded candidate quality state and marks actual omissions", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.snapshot.facts = [{ candidates: Array.from({ length: 9 }, (_, index) => ({
    candidate_id: `candidate-${index}`,
    quality: { qualification: `qualified-${index}`, promotion: `promoted-${index}`, validity: `valid-${index}`, availability: `available-${index}`, freshness: `fresh-${index}` },
  })) }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (let index = 0; index < 8; index += 1) {
    assert.match(rendered, new RegExp(`candidate=candidate-${index}; assertion=unavailable; qualification=qualified-${index}; promotion=promoted-${index}`));
  }
  assert.doesNotMatch(rendered, /candidate=candidate-8/);
  assert.match(rendered, /\+1 candidate\(s\) omitted/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog renders candidate assertion states and bounded qualification reasons", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.snapshot.facts = [{ candidates: [
    { candidate_id: "candidate-observed", key: {}, value: null, quality: { assertion: "observed", qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh", reasons: ["reason.observed"] } },
    { candidate_id: "candidate-inferred", key: {}, value: null, quality: { assertion: "inferred", qualification: "candidate", promotion: "unpromoted", validity: "suspect", availability: "available", freshness: "fresh", reasons: Array.from({ length: 9 }, (_, index) => `reason.inferred-${index}`) } },
  ] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Quality: candidate=candidate-observed; assertion=observed; qualification=qualified/);
  assert.match(rendered, /Quality: candidate=candidate-inferred; assertion=inferred; qualification=candidate/);
  assert.match(rendered, /Quality reason: candidate=candidate-observed; reason=reason\.observed/);
  assert.match(rendered, /Quality reason: candidate=candidate-inferred; \+1 reason\(s\) omitted/);
  assert.match(rendered, /Quality reason: candidate=candidate-inferred; reason=reason\.inferred-7/);
  assert.doesNotMatch(rendered, /reason=reason\.inferred-8/);
});

test("Portal catalog renders production quantity candidates without a selection and bounds complex values", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.selections = [];
  source.snapshot.facts = [{ candidates: [
    { candidate_id: "candidate:storage:power", key: { pack_id: "helianthus.pack.storage", pack_version: "1.1.0", fact_id: "storage.dc.power", dimensions: [{ id: "phase", value: { kind: "symbol", symbol: { namespace: "phase", token: "L1", known: true } } }] }, value: { kind: "quantity", quantity: { number: { coefficient: "5234", exponent10: -1 }, unit: "unit.watt" } }, quality: { qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" } },
    { candidate_id: "candidate:storage:modes", key: { pack_id: "helianthus.pack.storage", pack_version: "1.1.0", fact_id: "storage.mode", dimensions: [] }, value: { kind: "symbols", symbols: Array.from({ length: 9 }, (_, index) => ({ namespace: "mode", token: `mode-${index}`, known: true })) }, quality: { qualification: "qualified", promotion: "unpromoted", validity: "good", availability: "available", freshness: "fresh" } },
  ] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Quality key: candidate=candidate:storage:power; key=helianthus\.pack\.storage@1\.1\.0\/storage\.dc\.power/);
  assert.match(rendered, /Quality key dimension: candidate=candidate:storage:power; key=helianthus\.pack\.storage@1\.1\.0\/storage\.dc\.power; id=phase; value=\{"kind":"symbol","symbol":\{"namespace":"phase","token":"L1","known":true\}\}/);
  assert.match(rendered, /Quality value: candidate=candidate:storage:power; kind=quantity; coefficient=5234; exponent10=-1; unit=unit\.watt/);
  assert.match(rendered, /Quality value: candidate=candidate:storage:modes; kind=symbols; symbols=8; \+1 symbol\(s\) omitted/);
  assert.match(rendered, /Quality value symbol: candidate=candidate:storage:modes; namespace=mode; token=mode-7; known=true/);
  assert.doesNotMatch(rendered, /token=mode-8/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog renders each production candidate time coordinate and freshness policy in bounded rows", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const point = (unixNanoseconds, clockID, uncertaintyNS) => ({ unix_nanoseconds: unixNanoseconds, clock_id: clockID, uncertainty_ns: uncertaintyNS });
  const times = (prefix) => ({
    phenomenon_at: point(`${prefix}01`, `${prefix}-phenomenon-clock`, "1"),
    source_at: point(`${prefix}02`, `${prefix}-source-clock`, "2"),
    received_at: point(`${prefix}03`, `${prefix}-receipt-clock`, "3"),
    receipt_monotonic: { clock_epoch_id: `${prefix}-receipt-epoch`, nanoseconds: `${prefix}04` },
    evaluated_at: point(`${prefix}05`, `${prefix}-evaluation-clock`, "5"),
    evaluate_monotonic: { clock_epoch_id: `${prefix}-evaluation-epoch`, nanoseconds: `${prefix}06` },
  });
  const policy = (prefix, policyID) => ({
    policy_id: policyID, version: `${prefix}.1.0`, fresh_for_ns: `${prefix}11`, retain_for_ns: `${prefix}12`, max_wall_uncertainty_ns: `${prefix}13`,
  });
  catalog.resources[1].source.snapshot.facts = [{ candidates: [
    { candidate_id: "candidate-timed-a", times: times("a"), freshness_policy: policy("policy-a", "policy-a"), quality: {} },
    { candidate_id: "candidate-timed-b", times: times("b"), freshness_policy: policy("policy-b", `policy-b-${"p".repeat(400)}`), quality: {} },
  ] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const expected of [
    "Quality time: candidate=candidate-timed-a; coordinate=phenomenon_at.unix_nanoseconds; value=a01",
    "Quality time: candidate=candidate-timed-a; coordinate=phenomenon_at.clock_id; value=a-phenomenon-clock",
    "Quality time: candidate=candidate-timed-a; coordinate=phenomenon_at.uncertainty_ns; value=1",
    "Quality time: candidate=candidate-timed-a; coordinate=source_at.unix_nanoseconds; value=a02",
    "Quality time: candidate=candidate-timed-a; coordinate=source_at.clock_id; value=a-source-clock",
    "Quality time: candidate=candidate-timed-a; coordinate=source_at.uncertainty_ns; value=2",
    "Quality time: candidate=candidate-timed-a; coordinate=received_at.unix_nanoseconds; value=a03",
    "Quality time: candidate=candidate-timed-a; coordinate=received_at.clock_id; value=a-receipt-clock",
    "Quality time: candidate=candidate-timed-a; coordinate=received_at.uncertainty_ns; value=3",
    "Quality monotonic: candidate=candidate-timed-a; coordinate=receipt_monotonic.clock_epoch_id; value=a-receipt-epoch",
    "Quality monotonic: candidate=candidate-timed-a; coordinate=receipt_monotonic.nanoseconds; value=a04",
    "Quality time: candidate=candidate-timed-a; coordinate=evaluated_at.unix_nanoseconds; value=a05",
    "Quality time: candidate=candidate-timed-a; coordinate=evaluated_at.clock_id; value=a-evaluation-clock",
    "Quality time: candidate=candidate-timed-a; coordinate=evaluated_at.uncertainty_ns; value=5",
    "Quality monotonic: candidate=candidate-timed-a; coordinate=evaluate_monotonic.clock_epoch_id; value=a-evaluation-epoch",
    "Quality monotonic: candidate=candidate-timed-a; coordinate=evaluate_monotonic.nanoseconds; value=a06",
    "Quality time: candidate=candidate-timed-b; coordinate=source_at.clock_id; value=b-source-clock",
    "Quality monotonic: candidate=candidate-timed-b; coordinate=evaluate_monotonic.nanoseconds; value=b06",
    "Quality freshness policy: candidate=candidate-timed-a; policy_id=policy-a",
    "Quality freshness policy: candidate=candidate-timed-a; version=policy-a.1.0",
    "Quality freshness policy: candidate=candidate-timed-a; fresh_for_ns=policy-a11",
    "Quality freshness policy: candidate=candidate-timed-a; retain_for_ns=policy-a12",
    "Quality freshness policy: candidate=candidate-timed-a; max_wall_uncertainty_ns=policy-a13",
    "Quality freshness policy: candidate=candidate-timed-b; max_wall_uncertainty_ns=policy-b13",
  ]) assert.ok(rendered.includes(expected), `missing time or policy coordinate: ${expected}`);
  assert.match(rendered, /Quality freshness policy: candidate=candidate-timed-b; policy_id=policy-b-p+…/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog renders bounded production causal and derivation provenance for inferred candidates", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  catalog.resources = [catalog.resources[1]];
  catalog.fields = [catalog.fields[1]];
  catalog.actions = [catalog.actions[0]];
  const evidence = Array.from({ length: 9 }, (_, index) => ({
    owner: `derivation-owner-${index}`, kind: `derivation-kind-${index}`, digest: `sha256:derivation-${index}`,
    contract: "helianthus.native.evidence/v1", access: "public", redaction: "none",
  }));
  const sourcePaths = (input) => Array.from({ length: 9 }, (_, path) => ({
    binding_id: `binding-${input}-${path}`, source_id: `source-${input}-${path}`,
    source_epoch_id: `epoch-${input}-${path}`, driver_generation: `${input}${path}`,
  }));
  source.snapshot.facts = [{ candidates: [{
    candidate_id: "candidate-derived", revision: "99", key: {}, value: null,
    quality: { assertion: "inferred", qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" },
    causal: {
      origin: { origin_id: "causal-origin", kind: "derived", source_id: "causal-source", source_epoch_id: "causal-epoch", binding_id: "causal-binding", evidence: [] },
      correlation_id: "correlation-root", parent_correlation_id: "correlation-parent", hop_count: 2, max_hops: 8,
      first_seen_at: { unix_nanoseconds: "100", clock_id: "causal-clock", uncertainty_ns: "1" },
      expires_at: { unix_nanoseconds: "200", clock_id: "causal-clock", uncertainty_ns: "2" },
      path: Array.from({ length: 9 }, (_, index) => `target-${index}`),
    },
    derivation: {
      algorithm: "helianthus.derivation.aggregate", version: "2.0.0",
      inputs: Array.from({ length: 9 }, (_, input) => ({ candidate_id: `input-candidate-${input}`, candidate_revision: `${input}`, source_paths: sourcePaths(input) })),
      evidence,
    },
  }] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const expected of [
    "Candidate revision: candidate=candidate-derived; revision=99",
    "Causal origin: candidate=candidate-derived; origin_id=causal-origin",
    "Causal origin: candidate=candidate-derived; kind=derived",
    "Causal origin: candidate=candidate-derived; source_id=causal-source",
    "Causal origin: candidate=candidate-derived; source_epoch_id=causal-epoch",
    "Causal origin: candidate=candidate-derived; binding_id=causal-binding",
    "Causal correlation: candidate=candidate-derived; correlation_id=correlation-root",
    "Causal correlation: candidate=candidate-derived; parent_correlation_id=correlation-parent",
    "Causal hops: candidate=candidate-derived; hop_count=2",
    "Causal hops: candidate=candidate-derived; max_hops=8",
    "Causal time: candidate=candidate-derived; coordinate=causal.first_seen_at.unix_nanoseconds; value=100",
    "Causal time: candidate=candidate-derived; coordinate=causal.expires_at.uncertainty_ns; value=2",
    "Causal path: candidate=candidate-derived; path=7; target_id=target-7",
    "Derivation: candidate=candidate-derived; algorithm=helianthus.derivation.aggregate",
    "Derivation: candidate=candidate-derived; version=2.0.0",
    "Derivation input: candidate=candidate-derived; input=7; candidate_id=input-candidate-7",
    "Derivation input: candidate=candidate-derived; input=7; candidate_revision=7",
    "Derivation source path: candidate=candidate-derived; input=7; path=7; binding_id=binding-7-7",
    "Derivation source path: candidate=candidate-derived; input=7; path=7; source_id=source-7-7",
    "Derivation source path: candidate=candidate-derived; input=7; path=7; source_epoch_id=epoch-7-7",
    "Derivation source path: candidate=candidate-derived; input=7; path=7; driver_generation=77",
    "Derivation evidence: candidate=candidate-derived; ref=7; owner=derivation-owner-7",
    "Derivation evidence: candidate=candidate-derived; ref=7; kind=derivation-kind-7",
  ]) assert.ok(rendered.includes(expected), `missing causal or derivation evidence: ${expected}`);
  for (const marker of ["+1 target(s) omitted", "+1 input(s) omitted", "+1 source path(s) omitted", "+1 evidence ref(s) omitted"]) assert.ok(rendered.includes(marker), `missing omission marker: ${marker}`);
  assert.doesNotMatch(rendered, /target-8|input-candidate-8|binding-7-8|derivation-owner-8/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog renders candidate-specific provenance and bounded native evidence", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const evidence = (prefix, count) => Array.from({ length: count }, (_, index) => ({ owner: `owner-${prefix}-${index}`, kind: `kind-${prefix}-${index}`, digest: `sha256:${prefix}${index}`, contract: "helianthus.native.evidence/v1", access: "public", redaction: "none" }));
  catalog.resources[1].source.snapshot.facts = [{ candidates: [
    { candidate_id: "candidate-a", key: { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: "pv.ac.frequency", dimensions: [] }, value: { kind: "quantity", quantity: { number: { coefficient: "1", exponent10: 0 }, unit: "unit.hertz" } }, quality: {}, binding_id: "binding-a", source_epoch_id: "epoch-a", driver_generation: "7", origin: { origin_id: "origin-a", kind: "native_observation", source_id: "source-a", source_epoch_id: "epoch-a", binding_id: "binding-a", evidence: evidence("origin-a", 9) }, evidence: evidence("candidate-a", 9) },
    { candidate_id: "candidate-b", key: { pack_id: "helianthus.pack.storage", pack_version: "1.1.0", fact_id: "storage.state.soc", dimensions: [] }, value: { kind: "quantity", quantity: { number: { coefficient: "50", exponent10: 0 }, unit: "unit.percent" } }, quality: {}, binding_id: "binding-b", source_epoch_id: "epoch-b", driver_generation: "8", origin: { origin_id: "origin-b", kind: "native_observation", source_id: "source-b", source_epoch_id: "epoch-b", binding_id: "binding-b", evidence: [] }, evidence: [] },
  ] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const value of ["binding_id=binding-a", "source_epoch_id=epoch-a", "driver_generation=7", "origin_source_id=source-a"]) assert.match(rendered, new RegExp(`Quality provenance: candidate=candidate-a; ${value}`));
  for (const value of ["binding_id=binding-b", "source_epoch_id=epoch-b", "driver_generation=8", "origin_source_id=source-b"]) assert.match(rendered, new RegExp(`Quality provenance: candidate=candidate-b; ${value}`));
  assert.match(rendered, /Quality origin evidence: candidate=candidate-a; \+1 evidence ref\(s\) omitted/);
  assert.match(rendered, /Quality origin evidence: candidate=candidate-a; ref=7; owner=owner-origin-a-7/);
  assert.match(rendered, /Quality origin evidence: candidate=candidate-a; ref=7; kind=kind-origin-a-7/);
  assert.match(rendered, /Quality evidence: candidate=candidate-a; \+1 evidence ref\(s\) omitted/);
  assert.match(rendered, /Quality evidence: candidate=candidate-a; ref=7; owner=owner-candidate-a-7/);
  assert.match(rendered, /Quality evidence: candidate=candidate-a; ref=7; kind=kind-candidate-a-7/);
  assert.doesNotMatch(rendered, /owner=owner-origin-a-8|owner=owner-candidate-a-8/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog keeps every field of the eighth long EvidenceRef in bounded rows", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const long = (character, count) => character.repeat(count);
  const evidence = Array.from({ length: 8 }, (_, index) => ({
    owner: `owner-${index}-${long("o", 80)}`, kind: `kind-${index}-${long("k", 80)}`, digest: `sha256:${index}-${long("d", 140)}`,
    contract: `contract-${index}-${long("c", 80)}`, access: `access-${index}-${long("a", 36)}`, redaction: `redaction-${index}-${long("r", 32)}`,
  }));
  catalog.resources[1].source.snapshot.facts = [{ candidates: [{
    candidate_id: "candidate-long-evidence", key: {}, value: null, quality: {}, origin: { evidence: [] }, evidence,
  }] }];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const field of ["owner", "kind", "digest", "contract", "access", "redaction"]) {
    assert.ok(rendered.includes(`Quality evidence: candidate=candidate-long-evidence; ref=7; ${field}=${evidence[7][field]}`), `missing complete eighth ${field}`);
  }
  assert.doesNotMatch(rendered, /evidence ref\(s\) omitted/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog keeps eight near-limit SemReg selections visible as bounded rows", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.selections = Array.from({ length: 8 }, (_, index) => ({
    contract: "helianthus.semantic.selection/v1", snapshot_id: `sha256:${"s".repeat(64)}`,
    revisions: { semantic: String(index), identity: String(index), facts: String(index), services: String(index), capabilities: String(index) }, evaluation_digest: `sha256:${"e".repeat(64)}`,
    context: { evaluated_at: { unix_nanoseconds: "1789516800000000000", clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: "7" } },
    key: { pack_id: `pack-${index}-${"p".repeat(40)}`, pack_version: `1.0.${index}`, fact_id: `fact-${index}-${"f".repeat(72)}`, dimensions: [] },
    policy_id: `policy-${index}-${"q".repeat(55)}`, policy_version: `1.0.${index}`, selected_candidate: `candidate-${index}-${"c".repeat(68)}`, candidate_revision: String(index), presentation_only: true,
  }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (let index = 0; index < 8; index += 1) {
    assert.match(rendered, new RegExp(`Selection: key=pack-${index}-p+@1\\.0\\.${index}/fact-${index}-f+; selected_candidate=candidate-${index}-c+; candidate_revision=${index}; policy=policy-${index}-q+@1\\.0\\.${index}`));
  }
  assert.doesNotMatch(rendered, /selection\(s\) omitted/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog renders only production Selection identity and complete bindings", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const selection = catalog.resources[1].source.selections[0];
  selection.candidate_id = "incidental-non-wire-candidate";
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Selection: key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; selected_candidate=candidate-1; candidate_revision=7; policy=promotion-policy@1\.0\.0/);
  assert.doesNotMatch(rendered, /selected_candidate=incidental-non-wire-candidate/);
  assert.match(rendered, /Selection contract: selected_candidate=candidate-1; contract=helianthus\.semantic\.selection\/v1/);
  assert.match(rendered, /Selection snapshot: selected_candidate=candidate-1; snapshot_id=sha256:s+/);
  assert.match(rendered, /Selection evaluation: selected_candidate=candidate-1; evaluation_digest=sha256:e+/);
  assert.match(rendered, /Selection presentation: selected_candidate=candidate-1; presentation_only=true/);
  for (const revision of ["semantic", "identity", "facts", "services", "capabilities"]) assert.match(rendered, new RegExp(`Selection revision: selected_candidate=candidate-1; ${revision}=7`));
  assert.match(rendered, /Selection context evaluated_at: selected_candidate=candidate-1; unix_nanoseconds=1789516800000000000; clock_id=wall; uncertainty_ns=0/);
  assert.match(rendered, /Selection context evaluate_monotonic: selected_candidate=candidate-1; clock_epoch_id=process; nanoseconds=7/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog distinguishes SemReg selection keys by dimensions and marks the ninth", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const key = { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: "pv.ac.frequency" };
  const selection = (candidate, dimensions) => ({
    contract: "helianthus.semantic.selection/v1", snapshot_id: `sha256:${"s".repeat(64)}`,
    revisions: { semantic: "7", identity: "7", facts: "7", services: "7", capabilities: "7" }, evaluation_digest: `sha256:${"e".repeat(64)}`,
    context: { evaluated_at: { unix_nanoseconds: "1789516800000000000", clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: "7" } },
    key: { ...key, dimensions }, policy_id: "promotion-policy", policy_version: "1.0.0", selected_candidate: candidate, candidate_revision: "7", presentation_only: true,
  });
  catalog.resources[1].source.selections = [
    selection("candidate-l1", [{ id: "phase", value: "L1" }]),
    selection("candidate-l2", [{ id: "phase", value: "L2" }]),
    selection("candidate-many", Array.from({ length: 9 }, (_, index) => ({ id: `phase-${index}`, value: `L${index}` }))),
  ];
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Selection key dimension: selected_candidate=candidate-l1; key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; id=phase; value=L1/);
  assert.match(rendered, /Selection key dimension: selected_candidate=candidate-l2; key=helianthus\.pack\.pv@1\.0\.0\/pv\.ac\.frequency; id=phase; value=L2/);
  assert.match(rendered, /id=phase-7; value=L7/);
  assert.match(rendered, /\+1 dimension\(s\) omitted/);
  assert.doesNotMatch(rendered, /id=phase-8; value=L8/);
});

test("Portal catalog presents server action eligibility while keeping actions inert", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.actions[0].enabled = false;
  catalog.actions[0].reason = "driver is cooling down";
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const action = tags(body).find((node) => node.tagName === "button" && node.textContent.startsWith("Action:"));
  assert.equal(action.disabled, true);
  assert.equal(action.textContent, "Action: catalog-action; server_enabled=false; reason=driver is cooling down; read-only");
});

test("Portal catalog rejects records attached to an ABSENT domain and retains the prior valid cards", async () => {
  const body = new FakeNode();
  const notice = new FakeNode();
  const invalid = validCatalog();
  invalid.domains[2].source_state = "ABSENT";
  invalid.domains[2].reason = "source_unavailable";
  const responses = [response({ data: { portalCatalogV1: validCatalog() } }), response({ data: { portalCatalogV1: invalid } })];
  const { shell } = await harness(() => Promise.resolve(responses.shift()), new Map([
    ["[data-role=\"portal-catalog-body\"]", body],
    ["[data-role=\"portal-catalog-notice\"]", notice],
  ]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /pv-1/);
  assert.match(notice.textContent, /Catalog refresh failed.*last valid catalog snapshot/);
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

test("Portal catalog renders the current capture fence when historical lifecycle rows differ", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.binding_id = "current-binding";
  source.source_epoch = "current-epoch";
  source.evaluation_digest = "current-evaluation-digest";
  source.snapshot.bindings = Array.from({ length: 8 }, (_, index) => ({ binding_id: `historic-binding-${index}`, source_epoch_id: `historic-epoch-${index}`, driver_generation: String(index), state: "historic" }));
  source.snapshot.sources = Array.from({ length: 8 }, (_, index) => ({ source_id: `historic-source-${index}`, source_epoch_id: `historic-epoch-${index}`, state: "historic" }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Capture binding: binding_id=current-binding/);
  assert.match(rendered, /Capture epoch: source_epoch=current-epoch/);
  assert.match(rendered, /Capture evaluation: evaluation_digest=current-evaluation-digest/);
  assert.doesNotMatch(rendered, /Provenance binding: current-binding|Provenance source: current-epoch/);
});

test("Portal catalog truncates individual long capture and field values without erasing trailing metadata", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  catalog.resources[1].source.revision = "r".repeat(2048);
  catalog.resources[1].source.driver_generation = 77;
  catalog.fields[1].value = "v".repeat(2048);
  catalog.fields[1].unit_id = "unit-tail";
  catalog.fields[1].quality = "quality-tail";
  catalog.fields[1].projection = "projection-tail";
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /revision=r+…; driver_generation=77/);
  assert.match(rendered, /pv\.field=v+… unit-tail; quality=quality-tail; projection=projection-tail/);
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
  // Keep the max-cardinality marker fixture within the 512-node renderer budget by
  // concentrating dense SemReg evidence on one admitted resource.
  catalog.resources[0].source = structuredClone(source);
  catalog.resources[2].source = structuredClone(source);
  source.evaluation.facts = Array.from({ length: 9 }, (_, index) => ({ freshness: `fresh-${index}`, effective_availability: "available" }));
  source.snapshot.facts = Array.from({ length: 9 }, (_, fact) => ({ candidates: Array.from({ length: 9 }, (_, candidate) => ({ candidate_id: `candidate-${fact}-${candidate}`, quality: { qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" } })) }));
  source.snapshot.sources = Array.from({ length: 9 }, (_, index) => ({ source_id: `source-${index}`, source_epoch_id: `epoch-${index}`, state: "current" }));
  source.snapshot.bindings = Array.from({ length: 9 }, (_, index) => ({ binding_id: `binding-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), state: "current" }));
  source.snapshot.fences = Array.from({ length: 9 }, (_, index) => ({ source_id: `fence-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), reason: "retired" }));
  source.snapshot.cursors = Array.from({ length: 9 }, (_, index) => ({ source_id: `cursor-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), fenced: false }));
  source.selections = Array.from({ length: 9 }, (_, index) => ({
    contract: "helianthus.semantic.selection/v1", snapshot_id: `sha256:${"s".repeat(64)}`,
    revisions: { semantic: String(index), identity: String(index), facts: String(index), services: String(index), capabilities: String(index) }, evaluation_digest: `sha256:${"e".repeat(64)}`,
    context: { evaluated_at: { unix_nanoseconds: "1789516800000000000", clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: "7" } }, key: { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: `pv.fact-${index}`, dimensions: [] },
    policy_id: "promotion-policy", policy_version: "1.0.0", selected_candidate: `selected-${index}`, candidate_revision: String(index), presentation_only: true,
  }));
  source.projection.dispositions = Array.from({ length: 9 }, (_, index) => ({ item_id: `item-${index}`, outcome: "transformed", loss: Array.from({ length: 9 }, (_, loss) => ({ kind: `loss-${loss}` })) }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const marker of ["+1 fact(s) omitted", "+64 candidate(s) omitted", "+1 source(s) omitted", "+1 binding(s) omitted", "+1 fence(s) omitted", "+1 cursor(s) omitted", "+1 selection(s) omitted", "+1 disposition(s) omitted", "+1 loss item(s) omitted"]) {
    assert.ok(rendered.includes(marker), `missing truncation marker: ${marker}`);
  }
});

test("Portal catalog renders bounded current conflict and retained-observation evidence", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.snapshot.facts = [{
    candidates: [],
    conflicts: Array.from({ length: 9 }, (_, index) => ({ conflict_id: `conflict-${index}`, kind: "value", state: "open" })),
  }];
  source.snapshot.retained_observations = Array.from({ length: 9 }, (_, index) => ({
    contract: "helianthus.semantic.kernel/v1",
    candidate: { candidate_id: `retained-${index}` },
    removal: "generation_fence",
  }));
  source.evaluation.retained_observations = Array.from({ length: 9 }, (_, index) => ({
    observation: { candidate: { candidate_id: `evaluated-retained-${index}` }, removal: "generation_fence" },
    freshness: "stale",
  }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  assert.match(rendered, /Conflicts: \+1 conflict\(s\) omitted/);
  assert.match(rendered, /Conflict: conflict=conflict-0; kind=value; state=open/);
  assert.match(rendered, /Retained observation: \+1 observation\(s\) omitted/);
  assert.match(rendered, /Retained observation: candidate=retained-0; removal=generation_fence/);
  assert.match(rendered, /Retained evaluation: \+1 observation\(s\) omitted/);
  assert.match(rendered, /Retained evaluation: candidate=evaluated-retained-0; removal=generation_fence; freshness=stale/);
});

test("Portal catalog keeps eight nested evidence records visible in individually bounded rows", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  source.evaluation.facts = Array.from({ length: 8 }, (_, index) => ({ freshness: `freshness-${index}`, effective_availability: `availability-${index}` }));
  source.evaluation.retained_observations = Array.from({ length: 8 }, (_, index) => ({ observation: { candidate: { candidate_id: `evaluation-retained-${index}` }, removal: `removal-${index}` }, freshness: `stale-${index}` }));
  source.snapshot.sources = Array.from({ length: 8 }, (_, index) => ({ source_id: `source-${index}`, source_epoch_id: `epoch-${index}`, state: `state-${index}` }));
  source.snapshot.bindings = Array.from({ length: 8 }, (_, index) => ({ binding_id: `binding-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), state: `state-${index}` }));
  source.snapshot.fences = Array.from({ length: 8 }, (_, index) => ({ source_id: `fence-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), reason: `reason-${index}` }));
  source.snapshot.cursors = Array.from({ length: 8 }, (_, index) => ({ source_id: `cursor-${index}`, source_epoch_id: `epoch-${index}`, driver_generation: String(index), fenced: false }));
  source.snapshot.facts = [{ candidates: [], conflicts: Array.from({ length: 8 }, (_, index) => ({ conflict_id: `conflict-${index}`, kind: `kind-${index}`, state: `state-${index}` })) }];
  source.snapshot.retained_observations = Array.from({ length: 8 }, (_, index) => ({ candidate: { candidate_id: `retained-${index}` }, removal: `removal-${index}` }));
  source.projection.dispositions = Array.from({ length: 8 }, (_, index) => ({ item_id: `item-${index}`, outcome: `outcome-${index}`, loss: [{ kind: `loss-${index}` }] }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const expected of [
    "freshness=freshness-7; availability=availability-7", "candidate=evaluation-retained-7; removal=removal-7; freshness=stale-7",
    "source-7@epoch-7; state=state-7", "binding-7@epoch-7; generation=7; state=state-7",
    "fence-7@epoch-7; generation=7; reason=reason-7", "cursor-7@epoch-7; generation=7; fenced=false",
    "item=item-7; kind=unavailable; outcome=outcome-7", "item=item-7; kind=loss-7", "conflict=conflict-7; kind=kind-7; state=state-7", "candidate=retained-7; removal=removal-7",
  ]) assert.ok(rendered.includes(expected), `missing final nested record: ${expected}`);
  assert.doesNotMatch(rendered, /omitted/);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal reattach clears discarded catalog validity before a failed first refresh", async () => {
  const body = new FakeNode();
  const notice = new FakeNode();
  const { shell } = await harness(() => Promise.resolve(response({}, false)), new Map([
    ["[data-role=\"portal-catalog-body\"]", body],
    ["[data-role=\"portal-catalog-notice\"]", notice],
  ]));
  shell._portalCatalogHasValid = true;
  shell.render();
  assert.equal(shell._portalCatalogHasValid, false);
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /Catalog refresh failed/);
  assert.doesNotMatch(notice.textContent, /last valid catalog snapshot/);
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
  assert.match(text(cards[0]), /contribution=a\u0000b\/c@1\.0\.0|contribution=a\/b\u0000c@1\.0\.0/);
  assert.match(text(cards[1]), /contribution=a\u0000b\/c@1\.0\.0|contribution=a\/b\u0000c@1\.0\.0/);
  assert.notEqual(text(cards[0]), text(cards[1]));
  assert.match(text(body), /pv\.field/);
  assert.match(text(body), /catalog-action/);
  assert.doesNotMatch(text(body), /storage\.field|storage-action/);
  control(body, "Next Resource").handlers.click();
  assert.match(text(body), /storage\.field/);
  assert.match(text(body), /storage-action/);
  assert.doesNotMatch(text(body), /pv\.field|catalog-action/);
});

test("Portal catalog keeps a maximum structural catalog visible with paged details", async () => {
  const body = new FakeNode();
  const catalog = overBudgetCatalog();
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = text(body);
  assert.match(rendered, /resource-0/);
  assert.match(rendered, /resource-63/);
  assert.match(rendered, /state=CURRENT; contribution=driver-63\/manifest-63@1\.0\.0/);
  assert.match(rendered, /Contribution digest: driver-63\/manifest-63@1\.0\.0/);
  assert.match(rendered, /quarantined-driver-63\/quarantined-manifest-63@1\.0\.0: not_admitted/);
  assert.equal((rendered.match(/Contribution digest:/g) || []).length, 64);
  assert.match(rendered, /Resource page 1 of 64/);
  assert.match(rendered, /Evidence page 1 of [0-9]+; page size 92; rows 1-/);
  assert.ok(tags(body).length <= 512);
});

test("Portal catalog pages a formerly over-budget refresh without replacing valid cards", async () => {
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
  assert.match(text(body), /resource-63/);
  assert.equal(notice.hidden, true);
});

test("Portal catalog paginates dense admitted PV evidence while preserving every domain and resource summary", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const pv = catalog.resources[1];
  catalog.resources[0].source = structuredClone(pv.source);
  catalog.resources[2].source = structuredClone(pv.source);
  pv.source.evaluation.facts = Array.from({ length: 11 }, (_, index) => ({ candidate_id: `evaluation-${index}`, candidate_revision: `${index}`, freshness: "fresh", effective_availability: "available" }));
  pv.source.snapshot.facts = Array.from({ length: 11 }, (_, index) => ({ candidates: [{ candidate_id: `candidate-${index}`, revision: `${index}`, quality: { assertion: "observed", qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" } }] }));
  pv.source.selections = Array.from({ length: 11 }, (_, index) => ({
    contract: "helianthus.semantic.selection/v1", snapshot_id: `snapshot-${index}`,
    revisions: { semantic: `${index}`, identity: `${index}`, facts: `${index}`, services: `${index}`, capabilities: `${index}` }, evaluation_digest: `evaluation-${index}`,
    context: { evaluated_at: { unix_nanoseconds: `${index}`, clock_id: "wall", uncertainty_ns: "0" }, evaluate_monotonic: { clock_epoch_id: "process", nanoseconds: `${index}` } },
    key: { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: `pv.selection-${index}`, dimensions: [] },
    policy_id: "promotion-policy", policy_version: "1.0.0", selected_candidate: `candidate-${index}`, candidate_revision: `${index}`, presentation_only: false,
  }));
  pv.source.projection.dispositions = Array.from({ length: 14 }, (_, index) => ({ item_id: `disposition-${index}`, kind: "fact", outcome: "transformed", reason: "projection", source_keys: [], loss: [] }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const initial = text(body);
  for (const domain of ["helianthus.pack.evse", "helianthus.pack.infrastructure", "helianthus.pack.pv", "helianthus.pack.storage", "helianthus.pack.thermal"]) assert.ok(initial.includes(domain), `missing domain summary: ${domain}`);
  for (const resource of ["evse-1", "pv-1", "storage-1"]) assert.ok(initial.includes(resource), `missing resource summary: ${resource}`);
  assert.match(initial, /Resource page 2 of 3/);
  control(body, "Previous Resource").handlers.click();
  assert.match(text(body), /Resource 1 of 3: evse-1/);
  control(body, "Next Resource").handlers.click();
  assert.match(text(body), /Resource 2 of 3: pv-1/);
  const pagedPV = collectEvidencePages(body);
  for (const expected of ["Evaluation: +3 fact(s) omitted", "Quality: +3 fact(s) omitted", "Projection: +6 disposition(s) omitted", "Selections: +3 selection(s) omitted", "Selection: key=helianthus.pack.pv@1.0.0/pv.selection-7; selected_candidate=candidate-7"]) assert.ok(pagedPV.includes(expected), `missing paged PV evidence: ${expected}`);
  assert.match(text(body), /Evidence page [2-9][0-9]* of [2-9][0-9]*/);
  control(body, "Next Resource").handlers.click();
  assert.match(text(body), /Resource 3 of 3: storage-1/);
  assert.ok(tags(body).length <= 512);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
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
  const wrongCatalogDigest = validCatalog();
  wrongCatalogDigest.catalog_digest = "d".repeat(63);
  for (const payload of [
    { errors: [{ message: "denied" }] },
    { data: { portalCatalogV1: { ...validCatalog(), fields: [{ id: "missing-everything" }] } } },
    { data: { portalCatalogV1: wrongPackVersion } },
    { data: { portalCatalogV1: wrongCatalogDigest } },
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
  const action = tags(body).find((node) => node.tagName === "button" && node.textContent.startsWith("Action:"));
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

test("Portal catalog pages the production SemReg schema checklist without hiding bounded evidence", async () => {
  const body = new FakeNode();
  const catalog = validCatalog();
  const source = catalog.resources[1].source;
  catalog.resources[0].source = structuredClone(source);
  catalog.resources[2].source = structuredClone(source);
  const evidence = (prefix, count = 1) => Array.from({ length: count }, (_, index) => ({ owner: `${prefix}-owner-${index}`, kind: `${prefix}-kind-${index}`, digest: `${prefix}-digest-${index}`, contract: `${prefix}-contract`, access: "public", redaction: "none" }));
  const point = (prefix) => ({ unix_nanoseconds: `${prefix}-ns`, clock_id: `${prefix}-clock`, uncertainty_ns: `${prefix}-uncertainty` });
  source.snapshot = {
    contract: "snapshot-contract", snapshot_id: "snapshot-id", asset_id: "snapshot-asset",
    revisions: { semantic: "snapshot-semantic", identity: "snapshot-identity", facts: "snapshot-facts", services: "snapshot-services", capabilities: "snapshot-capabilities" },
    evaluated_at: point("snapshot-evaluated"), evaluate_monotonic: { clock_epoch_id: "snapshot-epoch", nanoseconds: "snapshot-monotonic" },
    identity_links: Array.from({ length: 9 }, (_, index) => ({ asset_id: `link-asset-${index}`, binding_id: `link-binding-${index}`, state: "qualified", revision: `link-revision-${index}`, basis: evidence(`link-basis-${index}`, 9) })),
    sources: Array.from({ length: 9 }, (_, index) => ({ source_id: `source-id-${index}`, source_epoch_id: `source-epoch-${index}`, protocol_id: `protocol-${index}`, profile_id: `profile-${index}`, profile_version: `profile-version-${index}`, registry_evidence: evidence(`registry-${index}`)[0], started_at: point(`source-start-${index}`), state: "current", revision: `source-revision-${index}` })),
    bindings: Array.from({ length: 9 }, (_, index) => ({ binding_id: `binding-id-${index}`, asset_id: `binding-asset-${index}`, source_id: `binding-source-${index}`, source_epoch_id: `binding-epoch-${index}`, driver_generation: `binding-generation-${index}`, native_resource: evidence(`native-resource-${index}`)[0], state: "current", revision: `binding-revision-${index}` })),
    fences: Array.from({ length: 9 }, (_, index) => ({ source_id: `fence-source-${index}`, source_epoch_id: `fence-epoch-${index}`, driver_generation: `fence-generation-${index}`, reason: `fence-reason-${index}`, evidence: evidence(`fence-evidence-${index}`, 9), revision: `fence-revision-${index}` })),
    cursors: Array.from({ length: 9 }, (_, index) => ({ source_id: `cursor-source-${index}`, source_epoch_id: `cursor-epoch-${index}`, driver_generation: `cursor-generation-${index}`, last_sequence: `cursor-sequence-${index}`, last_batch_digest: `cursor-digest-${index}`, fenced: false })),
    facts: [{ candidates: [{ candidate_id: "candidate-sentinel", revision: "candidate-revision", key: { pack_id: "helianthus.pack.pv", pack_version: "1.0.0", fact_id: "pv.schema", dimensions: [{ id: "phase", value: { kind: "symbol", symbol: { namespace: "phase-namespace", token: "L1-sentinel", known: true } } }] }, value: { kind: "quantity", quantity: { number: { coefficient: "123", exponent10: -1 }, unit: "unit.watt" } }, quality: { assertion: "observed", qualification: "qualified", promotion: "promoted", validity: "good", availability: "available", freshness: "fresh" } }], conflicts: Array.from({ length: 9 }, (_, index) => ({ conflict_id: `conflict-id-${index}`, kind: `conflict-kind-${index}`, state: "open", candidates: Array.from({ length: 9 }, (_, candidate) => `conflict-candidate-${index}-${candidate}`), evidence: evidence(`conflict-evidence-${index}`, 9) })) }],
    retained_observations: Array.from({ length: 9 }, (_, index) => ({ contract: `retained-contract-${index}`, candidate: { candidate_id: `snapshot-retained-${index}`, revision: `snapshot-retained-revision-${index}`, key: {}, value: null, quality: {}, evidence: [] }, removal: "generation_fence" })),
  };
  source.evaluation = {
    contract: "evaluation-contract", snapshot_id: "evaluation-snapshot", revisions: { semantic: "evaluation-semantic", identity: "evaluation-identity", facts: "evaluation-facts", services: "evaluation-services", capabilities: "evaluation-capabilities" },
    context: { evaluated_at: point("evaluation-context"), evaluate_monotonic: { clock_epoch_id: "evaluation-epoch", nanoseconds: "evaluation-monotonic" } }, evaluation_digest: "evaluation-digest",
    facts: [{ candidate_id: "evaluation-candidate", candidate_revision: "evaluation-revision", freshness: "fresh", effective_availability: "available" }],
    retained_observations: Array.from({ length: 9 }, (_, index) => ({ observation: { contract: `evaluation-retained-contract-${index}`, candidate: { candidate_id: `evaluation-retained-${index}`, revision: `evaluation-retained-revision-${index}`, key: {}, value: null, quality: {}, evidence: [] }, removal: "source_retirement" }, freshness: `retained-freshness-${index}` })),
  };
  source.projection = {
    contract: "projection-contract", snapshot_id: "projection-snapshot", revisions: { semantic: "projection-semantic", identity: "projection-identity", facts: "projection-facts", services: "projection-services", capabilities: "projection-capabilities" }, evaluation_digest: "projection-digest",
    manifest: { target_id: "target-id", target_version: "target-version", kernel_version: "kernel-version", mapping_revision: "mapping-revision", pack_versions: Array.from({ length: 9 }, (_, index) => ({ id: `pack-${index}`, version: `pack-version-${index}` })) },
    requested: Array.from({ length: 9 }, (_, index) => ({ item_id: `requested-item-${index}`, kind: `requested-kind-${index}` })),
    causal: { origin: { origin_id: "projection-origin", kind: "projection", evidence: evidence("projection-origin-evidence", 9) }, correlation_id: "projection-correlation", hop_count: 1, max_hops: 4, first_seen_at: point("projection-first"), expires_at: point("projection-expires"), path: Array.from({ length: 9 }, (_, index) => `projection-target-${index}`) },
    dispositions: [{ item_id: "projection-item", kind: "fact", outcome: "transformed", reason: "projection-reason", source_keys: [], loss: [{ kind: "precision", reversible: true, description: "loss-description", source_items: [] }] }],
  };
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]]));
  const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const expected of ["snapshot-contract", "snapshot-capabilities", "snapshot-evaluated-ns", "link-asset-7", "link-basis-7-owner-7", "source-id-7", "protocol-7", "binding-id-7", "native-resource-7-owner-0", "fence-evidence-7-owner-7", "cursor-digest-7", "evaluation-contract", "evaluation-digest", "target-id", "mapping-revision", "pack-7", "requested-item-7", "projection-target-7", "reversible=true", "conflict-candidate-7-7", "conflict-evidence-7-owner-7", "retained-contract-7", "evaluation-retained-contract-7", "retained-freshness-7", "L1-sentinel"]) assert.ok(rendered.includes(expected), `missing schema sentinel: ${expected}`);
  for (const marker of ["+1 link(s) omitted", "+1 source(s) omitted", "+1 binding(s) omitted", "+1 fence(s) omitted", "+1 cursor(s) omitted", "+1 target(s) omitted", "+1 candidate(s) omitted", "+1 evidence ref(s) omitted", "+1 observation(s) omitted"]) assert.ok(rendered.includes(marker), `missing schema omission: ${marker}`);
  for (const absent of ["link-asset-8", "source-id-8", "binding-id-8", "fence-source-8", "cursor-source-8", "projection-target-8", "pack-8", "conflict-candidate-7-8", "retained-contract-8"]) assert.doesNotMatch(rendered, new RegExp(absent));
  assert.ok(tags(body).length <= 512);
  assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog pages Snapshot fact, service, and capability metadata", async () => {
  const body = new FakeNode(); const catalog = validCatalog(); const source = catalog.resources[1].source;
  const caps = Array.from({ length: 9 }, (_, index) => ({ instance_id: `capability-${index}`, asset_id: `cap-asset-${index}`, service_instance: `service-${index}`, definition: { pack: { id: `cap-pack-${index}`, version: `cap-pack-version-${index}` }, id: `cap-definition-${index}`, version: `cap-definition-version-${index}` }, binding_id: `cap-binding-${index}`, source_epoch_id: `cap-epoch-${index}`, driver_generation: `${index}`, qualification: "qualified", availability: "available", revision: `${index}`, constraints: [{ id: `constraint-${index}`, value: { kind: "symbols", symbols: Array.from({ length: 9 }, (_, symbol) => ({ namespace: `namespace-${index}`, token: `token-${index}-${symbol}`, known: true })) } }], activation_evidence: Array.from({ length: 9 }, (_, evidence) => ({ owner: `activation-owner-${index}-${evidence}`, kind: "activation", digest: `digest-${evidence}`, contract: "contract", access: "public", redaction: "none" })) }));
  source.snapshot.services = Array.from({ length: 9 }, (_, index) => ({ instance_id: `service-${index}`, asset_id: `service-asset-${index}`, definition: { pack: { id: `service-pack-${index}`, version: `service-pack-version-${index}` }, id: `service-definition-${index}`, version: `service-definition-version-${index}` }, binding_id: `service-binding-${index}`, source_epoch_id: `service-epoch-${index}`, driver_generation: `${index}`, qualification: "qualified", availability: "available", revision: `${index}` }));
  source.snapshot.capabilities = caps;
  source.snapshot.facts = Array.from({ length: 9 }, (_, index) => ({ asset_id: `fact-asset-${index}`, key: { pack_id: `fact-pack-${index}`, pack_version: "1.0.0", fact_id: `fact-${index}`, dimensions: [{ id: "phase", value: { kind: "symbol", symbol: { namespace: "phase", token: `L${index}`, known: true } } }] }, revision: `${index}`, candidates: [] }));
  const { shell } = await harness(() => Promise.resolve(response({ data: { portalCatalogV1: catalog } })), new Map([["[data-role=\"portal-catalog-body\"]", body]])); const token = shell.beginBootstrapLifecycle(); await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  const rendered = collectEvidencePages(body);
  for (const sentinel of ["service-7", "service-definition-7", "capability-7", "cap-definition-7", "token-7-7", "activation-owner-7-7", "fact-asset-7", "fact-7", "L7"]) assert.ok(rendered.includes(sentinel), sentinel);
  for (const marker of ["+1 service(s) omitted", "+1 capability(s) omitted", "+1 fact(s) omitted", "+1 symbol(s) omitted", "+1 evidence ref(s) omitted"]) assert.ok(rendered.includes(marker), marker);
  assert.doesNotMatch(rendered, /service-8|capability-8|fact-asset-8|token-7-8/); assert.ok(tags(body).length <= 512); assert.ok(tags(body).every((node) => node.textContent.length <= 512));
});

test("Portal catalog rejects a reordered five-domain refresh and retains the latest valid UI", async () => {
  const body = new FakeNode(); const notice = new FakeNode(); const valid = validCatalog(); const reordered = validCatalog(); [reordered.domains[0], reordered.domains[1]] = [reordered.domains[1], reordered.domains[0]];
  const responses = [response({ data: { portalCatalogV1: valid } }), response({ data: { portalCatalogV1: reordered } })];
  const { shell } = await harness(() => Promise.resolve(responses.shift()), new Map([["[data-role=\"portal-catalog-body\"]", body], ["[data-role=\"portal-catalog-notice\"]", notice]])); const token = shell.beginBootstrapLifecycle();
  await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort); await shell.refreshPortalCatalog("/graphql/portal/v1", token, shell.bootstrapLifecycleAbort);
  assert.match(text(body), /pv-1/); assert.match(notice.textContent, /Catalog refresh failed.*last valid catalog snapshot/);
});
