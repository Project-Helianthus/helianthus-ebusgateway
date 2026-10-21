import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const source = await readFile(new URL("../src/app.js", import.meta.url), "utf8");
const digest = (text) => createHash("sha256").update(text).digest("hex");
const clone = structuredClone;

function fixture(count = 6) {
  const catalog = {
    contract: "helianthus.gateway.portal-catalog/v1", catalog_revision: digest("revision"), catalog_digest: digest("catalog"), evaluation_instant: "synthetic-evaluation",
    domains: ["evse", "infrastructure", "pv", "storage", "thermal"].map((id) => ({ pack: { id: `helianthus.pack.${id}`, version: id === "storage" ? "1.1.0" : "1.0.0" }, source_state: "PRESENT" })),
    contributions: [], descriptors: [], resources: [], fields: [], actions: [], quarantines: [],
  };
  const assets = [];
  for (let i = 0; i < count; i++) {
    const identity = { driver_id: `fixture.driver.${i}`, manifest_id: `portal.fixture.${i}`, manifest_version: "1.0.0", digest: `sha256:${digest(`descriptor-${i}`)}` };
    const group = { id: "summary", order: 0, resource_context: "resource", label: { default: "Summary" } };
    const ref = { pack: { id: "helianthus.pack.pv", version: "1.0.0" }, id: "fixture.power", version: "1.0.0" };
    const key = { pack_id: ref.pack.id, pack_version: ref.pack.version, fact_id: ref.id, dimensions: [] };
    const revisions = { semantic: "1", identity: "2", facts: "3", services: "4", capabilities: "5" };
    const binding = { binding_id: "binding:fixture", source_epoch_id: "epoch:fixture", driver_generation: (2n ** 53n + 1n).toString(), asset_id: `asset:fixture:${i}`, state: "current" };
    const candidate = { binding_id: binding.binding_id, source_epoch_id: binding.source_epoch_id, driver_generation: binding.driver_generation, candidate_id: "candidate:fixture", revision: "7", key: clone(key), value: { kind: "quantity", quantity: { number: { coefficient: "0", exponent10: 0 }, unit: "unit.watt" } }, quality: { assertion: "native_observation", qualification: "qualified", promotion: "promoted", validity: "good", freshness: "fresh", availability: "available", reasons: [] } };
    const snapshot = { asset_id: binding.asset_id, snapshot_id: `snapshot:fixture:${i}`, revisions, bindings: [binding], facts: [{ key, candidates: [candidate], conflicts: [] }] };
    const tuple = { snapshot_id: snapshot.snapshot_id, revisions: clone(revisions) };
    const asset = { asset_id: snapshot.asset_id, snapshot, evaluation: { ...clone(tuple), evaluation_digest: `sha256:${digest("shared-evaluation")}`, facts: [{ candidate_id: candidate.candidate_id, candidate_revision: candidate.revision, freshness: "fresh", effective_availability: "available" }] }, projection: { ...clone(tuple), dispositions: [{ kind: "fact", item_id: ref.id, outcome: "exact", source_keys: [clone(key)], loss: [] }] }, selections: [] };
    const field = { id: "power", group: group.id, label: { default: "Power" }, ref, service_ref: { ...clone(ref), id: "fixture.service" }, capability_ref: { ...clone(ref), id: "fixture.read" }, unit_ref: { ...clone(ref), id: "unit.watt" }, order: 0 };
    catalog.contributions.push(identity);
    catalog.descriptors.push({ ...identity, descriptor_version: "helianthus.gateway.portal-descriptor/v1", groups: [group], fields: [field], views: [{ id: "summary", group: group.id, label: { default: "Readings" }, renderer: "summary", slot: "lens", field_ids: [field.id], diagnostic_ids: [], order: 0 }], diagnostics: [] });
    catalog.resources.push({ id: "resource", domain: ref.pack.id, service_id: "fixture.service", capability_id: "fixture.read", contribution_driver_id: identity.driver_id, contribution_manifest_id: identity.manifest_id, contribution_manifest_version: identity.manifest_version, state: "CURRENT", source: { asset_id: snapshot.asset_id, snapshot_id: snapshot.snapshot_id, revision: JSON.stringify(revisions), evaluation_digest: "catalog-local-evaluation", binding_id: binding.binding_id, source_epoch: binding.source_epoch_id, driver_generation: binding.driver_generation, snapshot: clone(snapshot), evaluation: {}, projection: {}, selections: [] } });
    assets.push(asset);
  }
  return { catalog, assets };
}

function harness() {
  const document = { activeElement: null };
  const node = () => ({ innerHTML: "", textContent: "", attributes: {}, handlers: {}, setAttribute(k, v) { this.attributes[k] = v; }, addEventListener(k, f) { this.handlers[k] = f; }, focus() { document.activeElement = this; } });
  const list = node(), notice = node(), button = node(), search = node();
  const elements = new Map([["semantic-list", list], ["semantic-notice", notice], ["semantic-refresh", button], ["search-input", search]]);
  const requests = [], queue = [], deadlines = [];
  const sandbox = { HTMLElement: class {}, document, customElements: { define() {} }, console, AbortController, TextEncoder, setTimeout(callback, delay) { deadlines.push(callback); return setTimeout(callback, delay); }, clearTimeout, fetch: async (url, init) => {
    requests.push({ url, body: JSON.parse(init.body), signal: init.signal });
    assert.ok(queue.length, "unexpected request");
    const next = queue.shift();
    if (next instanceof Error) throw next;
    return await next;
  } };
  vm.createContext(sandbox);
  vm.runInContext(`${source}\nglobalThis.Shell = PortalShell; globalThis.readQuery = PORTAL_SEMANTIC_QUERY; globalThis.renderCards = semanticContributionCards;`, sandbox);
  const shell = new sandbox.Shell(); shell.isConnected = true;
  shell.querySelector = (selector) => elements.get(selector.match(/data-role="([^"]+)"/)?.[1]) || null;
  shell.querySelectorAll = () => [];
  shell._portalCatalogEndpoint = "/fixture/catalog"; shell._semanticGraphqlEndpoint = "/fixture/graphql";
  shell.beginBootstrapLifecycle();
  shell._portalRead = { snapshot_operation: "semanticSnapshot", unavailable_operations: [] };
  shell.bindEvents();
  const respond = (data) => queue.push({ ok: true, json: async () => ({ data }) });
  return { shell, list, notice, button, search, document, requests, queue, renderCards: sandbox.renderCards, query: sandbox.readQuery, deadline: () => deadlines.at(-1)(),
    pair(f = fixture()) { respond({ portalCatalogV1: f.catalog }); respond({ semanticSnapshot: { assets: f.assets } }); },
    refresh: () => shell.loadSemanticPreview(list),
  };
}

const candidate = (f, index = 0) => f.assets[index].snapshot.facts[0].candidates[0];

test("M23 sixth contribution renders through production preview with the same pack and no family branch", async () => {
  const h = harness(), f = fixture(); h.pair(f);
  assert.equal(await h.refresh(), true);
  assert.equal((h.list.innerHTML.match(/class="portal-catalog-resource"/g) || []).length, 6);
  assert.match(h.list.innerHTML, /fixture.driver.5\/portal.fixture.5@1.0.0/);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  assert.deepEqual(h.requests.map((request) => request.url), ["/fixture/catalog", "/fixture/graphql"]);
  assert.deepEqual(h.requests[1].body.variables.assetIds, f.assets.map((asset) => asset.asset_id));
  assert.match(h.query, /asset_id: assetId/); assert.match(h.query, /retained_observations: retainedObservations/);
  assert.match(h.query, /evidence \{ \.\.\.ReadSemanticEvidenceRef/);
});

for (const context of ["domain", "service", "capability"]) test(`M23 contribution fields stay within resource ${context} context`, async () => {
  const h = harness(), f = fixture(1);
  const descriptor = f.catalog.descriptors[0], asset = f.assets[0];
  const field = clone(descriptor.fields[0]);
  field.id = "foreign-power";
  field.label.default = "Foreign power";
  field.ref.id = "fixture.foreign-power";
  if (context === "domain") {
    for (const ref of [field.ref, field.service_ref, field.capability_ref, field.unit_ref]) ref.pack.id = "helianthus.pack.storage";
  } else field[`${context}_ref`].id = `fixture.foreign-${context}`;
  descriptor.fields.push(field);
  descriptor.views[0].field_ids.push(field.id);
  const fact = clone(asset.snapshot.facts[0]);
  fact.key.pack_id = field.ref.pack.id;
  fact.key.fact_id = field.ref.id;
  fact.candidates[0].key = clone(fact.key);
  fact.candidates[0].candidate_id = "candidate:foreign";
  fact.candidates[0].value = { kind: "text", text: "foreign-value" };
  asset.snapshot.facts.push(fact);
  asset.evaluation.facts.push({ ...clone(asset.evaluation.facts[0]), candidate_id: "candidate:foreign" });
  asset.projection.dispositions.push({ ...clone(asset.projection.dispositions[0]), item_id: field.ref.id, source_keys: [clone(fact.key)] });
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  assert.doesNotMatch(h.list.innerHTML, /Foreign power|foreign-value/);
});

test("M23 values distinguish zero, false, empty, unknown, withheld and projection loss", async () => {
  const h = harness(), f = fixture();
  candidate(f, 1).value = { kind: "boolean", boolean: false };
  candidate(f, 2).value = { kind: "text", text: "" };
  candidate(f, 3).value = null;
  f.assets[4].projection.dispositions[0].outcome = "withheld";
  candidate(f, 4).value = { kind: "text", text: "must-never-render" };
  const last = f.assets[5];
  candidate(f, 5).value.quantity.number = { coefficient: ((2n ** 53n + 1n) * 10n ** 5n + (123n * 100n + 45n)).toString(), exponent10: -8 };
  last.projection.dispositions[0].outcome = "transformed";
  last.projection.dispositions[0].loss.push({ kind: "precision", description: "synthetic loss", reversible: false });
  last.evaluation.facts[0].freshness = "stale";
  h.pair(f); assert.equal(await h.refresh(), true);
  for (const text of [/Power: 0e0/, /Power: false/, /Power: &lt;empty&gt;/, /Power: unknown/, /Power: withheld/, /synthetic loss/, /historical/]) assert.match(h.list.innerHTML, text);
  assert.ok(h.list.innerHTML.includes(["9007", "1992", "5474", "0993", "1234", "5e-8 unit.watt"].join("")));
  assert.doesNotMatch(h.list.innerHTML, /must-never-render/);
});

test("M23 unavailable asset keeps readable siblings and escapes supplied labels", async () => {
  const h = harness(), f = fixture(2);
  f.assets[0] = { asset_id: f.assets[0].asset_id, selections: [], unavailable_reason: "semantic.read.reason.publication_withheld" };
  f.catalog.descriptors[1].fields[0].label.default = "<script>fixture</script>";
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /publication_withheld/); assert.match(h.list.innerHTML, /0e0 unit.watt/);
  assert.match(h.list.innerHTML, /&lt;script&gt;/); assert.doesNotMatch(h.list.innerHTML, /<script>/);
});

test("M23 read error announces historical retained cards and preserves Refresh focus", async () => {
  const h = harness(); h.pair(); await h.refresh();
  const previous = h.list.innerHTML;
  h.button.focus(); h.queue.push(new Error("synthetic failure"));
  assert.equal(await h.refresh(), false);
  assert.equal(h.list.innerHTML, previous); assert.equal(h.document.activeElement, h.button);
  assert.match(h.notice.textContent, /failed.*historical last valid/);
  assert.equal(h.list.attributes["aria-busy"], "false");
  h.pair(); h.button.handlers.click(); await new Promise(setImmediate);
  assert.match(h.notice.textContent, /loaded/); assert.equal(h.document.activeElement, h.button);
});

test("M23 pending refresh keeps retained fresh-source cards explicitly historical", async () => {
  const h = harness(); h.pair(); assert.equal(await h.refresh(), true);
  const previous = h.list.innerHTML;
  assert.match(previous, /freshness=fresh/);
  h.queue.push(new Error("synthetic failure"));
  assert.equal(await h.refresh(), false);
  assert.match(h.notice.textContent, /historical last valid/);
  let complete;
  h.queue.push(new Promise((resolve) => { complete = resolve; }));
  const pending = h.refresh();
  assert.equal(h.list.innerHTML, previous);
  assert.equal(h.list.attributes["aria-busy"], "true");
  assert.match(h.notice.textContent, /Loading.*historical last valid/);
  complete({ ok: false });
  assert.equal(await pending, false);
  assert.match(h.notice.textContent, /failed.*historical last valid/);
  h.pair(); assert.equal(await h.refresh(), true);
  assert.doesNotMatch(h.notice.textContent, /historical/);
});

for (const failure of ["HTTP", "timeout", "malformed response"]) test(`M23 reattachment retains historical cards after ${failure}`, async () => {
  const h = harness(), f = fixture(1);
  h.pair(f); assert.equal(await h.refresh(), true);
  const previous = h.list.innerHTML;
  // Model DOM replacement so the real render() cannot silently discard cards.
  Object.defineProperty(h.shell, "innerHTML", { set(html) {
    h.list.innerHTML = html.match(/<ul data-role="semantic-list"[^>]*>([\s\S]*?)<\/ul>/)[1];
  } });
  h.shell.endBootstrapLifecycle();
  h.shell.beginBootstrapLifecycle();
  h.shell._portalRead = { snapshot_operation: "semanticSnapshot", unavailable_operations: [] };
  h.shell.render();
  assert.equal(h.list.innerHTML, previous);
  assert.equal(h.shell._semanticHasValid, true);
  let abort;
  if (failure === "HTTP") h.queue.push({ ok: false });
  else if (failure === "malformed response") h.queue.push({ ok: true, json: async () => ({ data: {} }) });
  else h.queue.push(new Promise((resolve, reject) => { abort = () => reject(new Error("synthetic abort")); }));
  const pending = h.refresh();
  if (abort) { h.deadline(); abort(); }
  assert.equal(await pending, false);
  assert.equal(h.list.innerHTML, previous);
  assert.match(h.notice.textContent, /historical last valid/);
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.doesNotMatch(h.notice.textContent, /historical/);
});

test("M23 pending and overlapping reads expose busy state and ignore late completions", async () => {
  const h = harness(); let complete;
  h.queue.push(new Promise((resolve) => { complete = resolve; }));
  const old = h.refresh(); assert.equal(h.list.attributes["aria-busy"], "true");
  assert.match(h.notice.textContent, /Loading/);
  h.pair(); assert.equal(await h.refresh(), true); const previous = h.list.innerHTML;
  complete({ ok: true, json: async () => ({ data: { portalCatalogV1: fixture(1).catalog } }) });
  assert.equal(await old, false); assert.equal(h.list.innerHTML, previous);
  assert.equal(h.list.attributes["aria-busy"], "false"); assert.equal(h.requests[0].signal.aborted, true);
});

test("M23 unavailable operations and unsafe endpoints fail closed without native fallback", async () => {
  for (const configure of [
    (s) => { s._portalRead.unavailable_operations = ["semanticSnapshot"]; },
    (s) => { s._portalRead.unavailable_operations = ["PortalCatalogV1"]; },
    (s) => { s._portalRead = undefined; },
    (s) => { s._semanticGraphqlEndpoint = "//fixture.invalid/graphql"; },
    (s) => { s._semanticGraphqlEndpoint = "/fixture/../graphql"; },
  ]) {
    const h = harness(); configure(h.shell); assert.equal(await h.refresh(), false);
    assert.equal(h.requests.length, 0); assert.match(h.list.innerHTML, /unavailable/);
  }
});

// Each participating identity dimension has its own mutation. Removing that
// production comparison would admit the changed shared source and fail the test.
const sourceMutations = {
  asset_id: (a) => { a.snapshot.asset_id = "asset:other"; },
  snapshot_id: (a) => { a.snapshot.snapshot_id = "snapshot:other"; a.evaluation.snapshot_id = a.projection.snapshot_id = a.snapshot.snapshot_id; },
  binding_id: (a) => { a.snapshot.bindings[0].binding_id = "binding:other"; },
  source_epoch: (a) => { a.snapshot.bindings[0].source_epoch_id = "epoch:other"; },
  driver_generation: (a) => { a.snapshot.bindings[0].driver_generation = (2n ** 53n).toString(); },
  ...Object.fromEntries(["semantic", "identity", "facts", "services", "capabilities"].map((key) => [`revisions.${key}`, (a) => { a.snapshot.revisions[key] = "8"; a.evaluation.revisions[key] = a.projection.revisions[key] = "8"; }])),
};
for (const [field, mutate] of Object.entries(sourceMutations)) test(`M23 source identity mismatch withholds values after bounded retries: ${field}`, async () => {
  const h = harness(), f = fixture(1); mutate(f.assets[0]);
  for (let i = 0; i < 3; i++) h.pair(f);
  assert.equal(await h.refresh(), false); assert.equal(h.requests.length, 6);
  assert.match(h.list.innerHTML, /generation conflict/); assert.doesNotMatch(h.list.innerHTML, /Power:/);
});

test("M23 evaluation drift does not conflict; shared freshness controls withholding", async () => {
  const h = harness(), f = fixture(1);
  f.assets[0].evaluation.evaluation_digest = `sha256:${digest("later evaluation")}`;
  f.assets[0].evaluation.facts[0].freshness = "expired";
  f.assets[0].evaluation.facts[0].effective_availability = "unavailable";
  h.pair(f); assert.equal(await h.refresh(), true); assert.equal(h.requests.length, 2);
  assert.match(h.list.innerHTML, /Power: withheld/); assert.match(h.list.innerHTML, /freshness=expired/);
});

test("M23 mismatch refetches both sources and converges on a matching pair", async () => {
  const h = harness(), changed = fixture(1); sourceMutations.binding_id(changed.assets[0]);
  h.pair(changed); h.pair(fixture(1));
  assert.equal(await h.refresh(), true); assert.equal(h.requests.length, 4); assert.match(h.list.innerHTML, /Power: 0e0/);
});

for (const field of ["driver_id", "manifest_id", "manifest_version"]) test(`M23 descriptor identity compares ${field}`, async () => {
  const h = harness(), f = fixture(1);
  f.catalog.descriptors[0][field] += ".other";
  h.pair(f); assert.equal(await h.refresh(), false); assert.doesNotMatch(h.list.innerHTML, /Power:/);
});

test("M23 replacement catalog withdraws cards; unsupported view preserves valid siblings", async () => {
  const h = harness(); h.pair(); await h.refresh();
  const f = fixture(2); f.catalog.descriptors[0].views[0].renderer = "unrecognized";
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /unsupported contribution view/); assert.match(h.list.innerHTML, /Power: 0e0/);
  assert.doesNotMatch(h.list.innerHTML, /fixture.driver.5/);
});

for (const scope of ["another resource", "native diagnostics", "same resource"]) test(`M23 admitted timeline for ${scope} preserves supported views in the same descriptor`, async () => {
  const h = harness(), f = fixture(1), descriptor = f.catalog.descriptors[0];
  const view = { ...clone(descriptor.views[0]), id: "timeline", renderer: "timeline", label: { default: "Unsupported timeline" }, order: -1 };
  if (scope === "another resource") {
    descriptor.groups.push({ id: "other", resource_context: "another-resource", order: -1, label: { default: "Other" } });
    view.group = "other";
  }
  if (scope === "native diagnostics") view.slot = "native_diagnostics";
  descriptor.views.push(view);
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Readings/);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  assert.doesNotMatch(h.list.innerHTML, /Unsupported timeline/);
  if (scope === "another resource") assert.doesNotMatch(h.list.innerHTML, /unsupported contribution view/);
  else assert.match(h.list.innerHTML, /unsupported contribution view/);
});

for (const field of ["pack_id", "pack_version", "fact_id", "dimensions"]) test(`M23 projection FactKey includes ${field}`, async () => {
  const h = harness(), f = fixture(1), key = f.assets[0].projection.dispositions[0].source_keys[0];
  key[field] = field === "dimensions" ? [{ id: "fixture.dimension", value: { kind: "boolean", boolean: false } }] : "other";
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: withheld/); assert.doesNotMatch(h.list.innerHTML, /Power: 0e0/);
});

for (const field of ["candidate_id", "revision", "key"]) test(`M23 selected candidate requires exact ${field}`, async () => {
  const h = harness(), f = fixture(1), asset = f.assets[0], selected = candidate(f);
  asset.selections.push({ snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(selected.key), selected_candidate: selected.candidate_id, candidate_revision: selected.revision });
  if (field === "key") selected.key.dimensions.push({ id: "fixture.dimension", value: { kind: "boolean", boolean: true } });
  else selected[field] = "other";
  h.pair(f); assert.equal(await h.refresh(), true); assert.match(h.list.innerHTML, /Power: withheld/);
});

test("M23 GraphQL errors and malformed shared responses retain previous valid cards", async () => {
  for (const response of [
    { errors: [{ message: "synthetic error" }] },
    { data: { semanticSnapshot: { assets: [] } } },
    { data: { semanticSnapshot: { assets: [{ asset_id: "asset:fixture:0", selections: [], unavailable_reason: "invented" }] } } },
  ]) {
    const h = harness(), f = fixture(1); h.pair(f); await h.refresh(); const previous = h.list.innerHTML;
    h.queue.push({ ok: true, json: async () => ({ data: { portalCatalogV1: f.catalog } }) });
    h.queue.push({ ok: true, json: async () => response });
    assert.equal(await h.refresh(), false); assert.equal(h.list.innerHTML, previous); assert.match(h.notice.textContent, /historical/);
  }
});

for (const code of ["RESOURCE_EXHAUSTED", "CONFLICT"]) test(`M23 structured ${code} shared-read error withholds previous values`, async () => {
  const h = harness(), f = fixture(1); h.pair(f);
  assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  h.queue.push({ ok: true, json: async () => ({ data: { portalCatalogV1: f.catalog } }) });
  h.queue.push({ ok: true, json: async () => ({ data: { semanticSnapshot: null }, errors: [{ message: "synthetic read refusal", extensions: { code } }] }) });
  assert.equal(await h.refresh(), false);
  assert.match(h.list.innerHTML, /generation conflict or read limit.*Values withheld/);
  assert.doesNotMatch(h.list.innerHTML, /Power:|portal-catalog-resource/);
  assert.doesNotMatch(h.notice.textContent, /historical/);
  assert.equal(h.shell._semanticHasValid, false);
});

test("M23 disconnect fences a late semantic completion", async () => {
  const h = harness(); let resolve;
  h.queue.push(new Promise((complete) => { resolve = complete; }));
  const pending = h.refresh(); h.shell.endBootstrapLifecycle();
  resolve({ ok: true, json: async () => ({ data: { portalCatalogV1: fixture(1).catalog } }) });
  assert.equal(await pending, false); assert.equal(h.requests.length, 1); assert.equal(h.list.innerHTML, "");
  assert.equal(h.requests[0].signal.aborted, true);
});

for (const field of ["id", "value.kind", "value.boolean"]) test(`M23 dimension identity includes ${field}`, async () => {
  const h = harness(), f = fixture(1), asset = f.assets[0];
  const dimension = { id: "fixture.dimension", value: { kind: "boolean", boolean: false } };
  asset.snapshot.facts[0].key.dimensions = [clone(dimension)];
  candidate(f).key.dimensions = [clone(dimension)];
  const key = asset.projection.dispositions[0].source_keys[0]; key.dimensions = [clone(dimension)];
  if (field === "id") key.dimensions[0].id = "fixture.other";
  if (field === "value.kind") key.dimensions[0].value.kind = "text";
  if (field === "value.boolean") key.dimensions[0].value.boolean = true;
  h.pair(f); assert.equal(await h.refresh(), true); assert.match(h.list.innerHTML, /Power: withheld/);
});

test("M23 exact selected candidate is readable while duplicate selection policies withhold", async () => {
  const f = fixture(1), asset = f.assets[0], selected = candidate(f);
  const selection = { snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(selected.key), selected_candidate: selected.candidate_id, candidate_revision: selected.revision };
  asset.selections = [selection];
  const h = harness(); h.pair(f); assert.equal(await h.refresh(), true); assert.match(h.list.innerHTML, /Power: 0e0/);
  asset.selections.push(clone(selection)); h.pair(f); assert.equal(await h.refresh(), true); assert.match(h.list.innerHTML, /Power: withheld/);
});

for (const policyField of ["policy_id", "policy_version"]) {
  for (const outcome of ["agreement", "different candidate", "different revision", "foreign binding", "foreign epoch", "foreign generation"]) test(`M23 distinct ${policyField} selections handle ${outcome}`, async () => {
    const h = harness(), f = fixture(1), asset = f.assets[0], selected = candidate(f);
    const selection = { snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(selected.key), policy_id: "policy:fixture", policy_version: "1", selected_candidate: selected.candidate_id, candidate_revision: selected.revision };
    asset.selections = [selection, { ...clone(selection), [policyField]: "other" }];
    if (outcome === "different candidate") {
      const sibling = clone(selected); sibling.candidate_id = "candidate:other";
      asset.snapshot.facts[0].candidates.push(sibling);
      asset.evaluation.facts.push({ ...clone(asset.evaluation.facts[0]), candidate_id: sibling.candidate_id });
      asset.selections[1].selected_candidate = sibling.candidate_id;
    }
    if (outcome === "different revision") asset.selections[1].candidate_revision = "8";
    if (outcome === "foreign binding") selected.binding_id = "binding:other";
    if (outcome === "foreign epoch") selected.source_epoch_id = "epoch:other";
    if (outcome === "foreign generation") selected.driver_generation = "9007199254740994";
    for (const selections of [asset.selections, [...asset.selections].reverse()]) {
      asset.selections = selections;
      h.pair(f); assert.equal(await h.refresh(), true);
      if (outcome === "agreement") {
        assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
        assert.doesNotMatch(h.list.innerHTML, /Power: withheld/);
      } else {
        assert.match(h.list.innerHTML, /Power: withheld/);
        assert.doesNotMatch(h.list.innerHTML, /Power: 0e0/);
      }
    }
  });
}

test("M23 whole-read bound withholds previous values and reports limit", async () => {
  const h = harness(), f = fixture(1); h.pair(f); await h.refresh();
  candidate(f).value = { kind: "text", text: "x".repeat(4 * 1024 * 1024) };
  h.pair(f); assert.equal(await h.refresh(), false);
  assert.match(h.list.innerHTML, /read limit/); assert.doesNotMatch(h.list.innerHTML, /Power:/);
});

test("M23 views remain scoped to their resource context and follow group/view order", async () => {
  const h = harness(), f = fixture(1), descriptor = f.catalog.descriptors[0];
  descriptor.groups.push({ id: "other", resource_context: "another-resource", order: -1, label: { default: "Other" } });
  descriptor.views.push({ ...clone(descriptor.views[0]), id: "other", group: "other", label: { default: "must-never-render" } });
  descriptor.views.push({ ...clone(descriptor.views[0]), id: "first", order: -1, label: { default: "First view" } });
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.doesNotMatch(h.list.innerHTML, /must-never-render/);
  assert.ok(h.list.innerHTML.indexOf("First view") < h.list.innerHTML.indexOf("Readings"));
});

test("M23 semantic timeout announces failure and retains last valid cards", async () => {
  const h = harness(); h.pair(fixture(1)); await h.refresh(); const previous = h.list.innerHTML;
  // Model an aborting fetch while invoking the same deadline callback as the browser.
  let abort;
  h.queue.push(new Promise((resolve, reject) => { abort = () => reject(new Error("synthetic abort")); }));
  const pending = h.refresh();
  h.deadline(); abort();
  assert.equal(await pending, false); assert.equal(h.list.innerHTML, previous);
  assert.match(h.notice.textContent, /timed out.*historical last valid/);
});

for (const [axis, state] of [
  ["qualification", "candidate"], ["qualification", "unknown"], ["qualification", "unsupported"],
  ["qualification", "rejected"], ["promotion", "unpromoted"],
  ["validity", "bad"], ["validity", "unknown"],
]) test(`M23 selected ${axis}=${state} cannot borrow an eligible sibling's projection`, async () => {
  const h = harness(), f = fixture(1), asset = f.assets[0], selected = candidate(f);
  const sibling = clone(selected);
  sibling.candidate_id = "candidate:eligible-sibling";
  asset.snapshot.facts[0].candidates.push(sibling);
  asset.evaluation.facts.push({ ...clone(asset.evaluation.facts[0]), candidate_id: sibling.candidate_id });
  selected.quality[axis] = state;
  selected.value = { kind: "text", text: "audit-only-value" };
  asset.selections = [{ snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(selected.key), selected_candidate: selected.candidate_id, candidate_revision: selected.revision, presentation_only: true }];
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: withheld/);
  assert.doesNotMatch(h.list.innerHTML, /audit-only-value/);
  assert.match(h.list.innerHTML, new RegExp(`${axis}.*${state}`));
});

test("M23 promoted suspect value retains degraded shared availability", async () => {
  const h = harness(), f = fixture(1);
  candidate(f).quality.validity = "suspect";
  f.assets[0].evaluation.facts[0].effective_availability = "degraded";
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  assert.match(h.list.innerHTML, /availability=degraded/);
});

test("M23 cards retain selected provenance, evidence policy and competing fact conflicts", async () => {
  const h = harness(), f = fixture(1), asset = f.assets[0], selected = candidate(f);
  const evidence = { owner: "fixture.owner", kind: "native", digest: `sha256:${digest("synthetic evidence")}`, contract: "fixture.evidence", access: "restricted", redaction: "redacted" };
  Object.assign(selected, {
    binding_id: "binding:fixture", source_epoch_id: "epoch:fixture", driver_generation: "9007199254740993",
    origin: { origin_id: "origin:selected", kind: "driver", source_id: "source:selected", evidence: [clone(evidence)] },
    causal: { correlation_id: "correlation:selected", path: ["source:selected"] },
    times: { received_at: { unix_nanoseconds: "9007199254740993", clock_id: "clock:fixture" } },
    evidence: [evidence], derivation: { algorithm: "fixture.derivation", version: "1", inputs: [{ candidate_id: "candidate:input", candidate_revision: "9" }], evidence: [clone(evidence)] },
  });
  const sibling = clone(selected); sibling.candidate_id = "candidate:competing";
  sibling.value = { kind: "text", text: "unselected-value" };
  asset.snapshot.facts[0].candidates.push(sibling);
  asset.snapshot.facts[0].conflicts = [{ conflict_id: "conflict:fixture", kind: "value", candidates: [selected.candidate_id, sibling.candidate_id], state: "open", evidence: [{ ...clone(evidence), owner: "<fixture-conflict>", access: "authorized", redaction: "metadata_only" }] }];
  asset.selections = [{ snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(selected.key), selected_candidate: selected.candidate_id, candidate_revision: selected.revision }];
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
  for (const text of ["Provenance:", "binding:fixture", "epoch:fixture", "9007199254740993", "source:selected", "origin:selected", "correlation:selected", "received_at", "fixture.derivation", "candidate:input", "Evidence:", evidence.digest, "fixture.owner", "fixture.evidence", "restricted", "redacted", "Conflicts:", "conflict:fixture", "candidate:competing", "value", "open", "authorized", "metadata_only", "&lt;fixture-conflict&gt;"]) assert.ok(h.list.innerHTML.includes(text), text);
  const evidenceRow = h.list.innerHTML.match(/<p>Evidence: ([\s\S]*?)<\/p>/)[1];
  for (const text of [evidence.owner, evidence.kind, evidence.digest, evidence.contract, evidence.access, evidence.redaction]) assert.ok(evidenceRow.includes(text), text);
  const conflictRow = h.list.innerHTML.match(/<p>Conflicts: ([\s\S]*?)<\/p>/)[1];
  for (const text of ["conflict:fixture", selected.candidate_id, sibling.candidate_id, "authorized", "metadata_only"]) assert.ok(conflictRow.includes(text), text);
  assert.doesNotMatch(h.list.innerHTML, /unselected-value|<fixture-conflict>/);
  assert.equal(h.requests.length, 2);
  asset.projection.dispositions[0].outcome = "withheld";
  h.pair(f); assert.equal(await h.refresh(), true);
  assert.match(h.list.innerHTML, /Power: withheld/);
  assert.match(h.list.innerHTML, /Provenance:.*source:selected/);
  assert.match(h.list.innerHTML, /Evidence:.*restricted/);
  assert.match(h.list.innerHTML, /Conflicts:.*candidate:competing/);
});

for (const field of ["binding_id", "source_epoch_id", "driver_generation"]) {
  for (const choice of ["default", "explicit selection", "default with matching sibling"]) test(`M23 candidate source ${field} must match resource through ${choice}`, async () => {
    const h = harness(), f = fixture(1), asset = f.assets[0], foreign = candidate(f);
    const matching = clone(foreign);
    matching.candidate_id = "candidate:matching";
    foreign[field] = field === "driver_generation" ? "9007199254740994" : "other-source";
    foreign.value = { kind: "text", text: "foreign-source-value" };
    if (choice !== "default") {
      asset.snapshot.facts[0].candidates.push(matching);
      asset.evaluation.facts.push({ ...clone(asset.evaluation.facts[0]), candidate_id: matching.candidate_id });
    }
    if (choice === "explicit selection") asset.selections = [{ snapshot_id: asset.snapshot.snapshot_id, revisions: clone(asset.snapshot.revisions), evaluation_digest: asset.evaluation.evaluation_digest, key: clone(foreign.key), selected_candidate: foreign.candidate_id, candidate_revision: foreign.revision }];
    h.pair(f); assert.equal(await h.refresh(), true);
    assert.doesNotMatch(h.list.innerHTML, /foreign-source-value/);
    if (choice === "default with matching sibling") assert.match(h.list.innerHTML, /Power: 0e0 unit.watt/);
    else assert.match(h.list.innerHTML, /Power: withheld/);
  });
}

function repeatViews(f, count) {
  const descriptor = f.catalog.descriptors[0], view = descriptor.views[0];
  descriptor.views = Array.from({ length: count }, (_, order) => ({ ...clone(view), id: `view:${order}`, order }));
}

test("M23 semantic expansion fits the node budget at its boundary", async () => {
  const h = harness(), f = fixture(1);
  repeatViews(f, 36);
  h.pair(f); assert.equal(await h.refresh(), true);
  // Each card has three nodes; each view has seven paragraph/text pairs.
  assert.equal((h.list.innerHTML.match(/<p>/g) || []).length, 252);
});

for (const shape of ["repeated views", "repeated fields", "many facts", "many projections", "many diagnostics", "multiple resources"]) {
  test(`M23 semantic expansion rejects ${shape} beyond the node budget`, async () => {
    const h = harness(), f = fixture(shape === "multiple resources" ? 2 : 1);
    const descriptor = f.catalog.descriptors[0], asset = f.assets[0];
    if (shape === "repeated views") repeatViews(f, 37);
    if (shape === "repeated fields") {
      descriptor.fields = Array.from({ length: 512 }, (_, order) => ({ ...clone(descriptor.fields[0]), id: `field:${order}`, order }));
      descriptor.views[0].field_ids = descriptor.fields.map((field) => field.id);
      repeatViews(f, 64);
    }
    if (shape === "many facts") asset.snapshot.facts = Array.from({ length: 64 }, () => clone(asset.snapshot.facts[0]));
    if (shape === "many projections") asset.projection.dispositions = Array.from({ length: 256 }, () => clone(asset.projection.dispositions[0]));
    if (shape === "many diagnostics") {
      descriptor.diagnostics = Array.from({ length: 64 }, (_, order) => ({ id: `diagnostic:${order}`, label: { default: "Diagnostic" }, kind: "fixture", member_id: "fixture", order }));
      descriptor.views[0].diagnostic_ids = descriptor.diagnostics.map((row) => row.id);
      repeatViews(f, 4);
    }
    if (shape === "multiple resources") repeatViews(f, 36);
    assert.ok(JSON.stringify(f).length < 4 * 1024 * 1024);
    h.pair(f); assert.equal(await h.refresh(), false);
    assert.equal(h.shell._semanticHasValid, undefined);
    assert.doesNotMatch(h.list.innerHTML, /portal-catalog-resource|Power:|Quality:|Provenance:/);
    assert.match(h.notice.textContent, /unavailable/i);
  });
}

test("M23 oversized semantic expansion retains historical cards", async () => {
  const h = harness(), f = fixture(1);
  h.pair(f); assert.equal(await h.refresh(), true);
  const previous = h.list.innerHTML;
  repeatViews(f, 37);
  h.pair(f); assert.equal(await h.refresh(), false);
  assert.equal(h.list.innerHTML, previous);
  assert.match(h.notice.textContent, /historical last valid/);
});

test("M23 semantic budget is checked before value and audit row formatting", () => {
  const h = harness(), f = fixture(1);
  repeatViews(f, 37);
  let formatted = 0;
  Object.defineProperty(candidate(f), "value", { get() { formatted++; return { kind: "text", text: "value" }; } });
  Object.defineProperty(candidate(f), "evidence", { get() { formatted++; return []; } });
  assert.throws(() => h.renderCards(f.catalog, f.assets), /semantic render node budget exceeded/);
  assert.equal(formatted, 0);
});

for (const record of ["value", "provenance", "evidence", "conflicts", "loss"]) {
  test(`M23 semantic text budget rejects repeated ${record} below the input and node limits`, async () => {
    const h = harness(), f = fixture(1), asset = f.assets[0];
    h.pair(f); assert.equal(await h.refresh(), true);
    const previous = h.list.innerHTML;
    const large = "<&界".repeat(16 * 1024);
    if (record === "value") candidate(f).value = { kind: "text", text: large };
    if (record === "provenance") candidate(f).origin = { origin_id: large };
    if (record === "evidence") candidate(f).evidence = [{ owner: large }];
    if (record === "conflicts") asset.snapshot.facts[0].conflicts = [{ conflict_id: large }];
    if (record === "loss") asset.projection.dispositions[0].loss = [{ reason: large }];
    repeatViews(f, 36);
    assert.ok(Buffer.byteLength(JSON.stringify(f)) < 4 * 1024 * 1024);
    h.pair(f); assert.equal(await h.refresh(), false);
    assert.equal(h.list.innerHTML, previous);
    assert.match(h.notice.textContent, /historical last valid/);
    const initial = harness();
    initial.pair(f); assert.equal(await initial.refresh(), false);
    assert.doesNotMatch(initial.list.innerHTML, /portal-catalog-resource|Power:|Provenance:/);
  });
}

test("M23 semantic HTML byte budget includes escaping and UTF-8 at its boundary", () => {
  const h = harness(), f = fixture(1), limit = 4 * 1024 * 1024;
  candidate(f).value = { kind: "text", text: `界<&>"'` };
  const remaining = limit - Buffer.byteLength(h.renderCards(f.catalog, f.assets));
  candidate(f).value.text += "x".repeat(remaining);
  assert.equal(Buffer.byteLength(h.renderCards(f.catalog, f.assets)), limit);
  candidate(f).value.text += "x";
  assert.throws(() => h.renderCards(f.catalog, f.assets), /semantic render byte budget exceeded/);
});

test("M23 semantic byte budget is shared across resources and stops formatting on overflow", () => {
  const h = harness(), f = fixture(2);
  candidate(f).value = { kind: "text", text: "x".repeat(2 * 1024 * 1024) };
  candidate(f, 1).value = clone(candidate(f).value);
  let formatted = 0;
  Object.defineProperty(candidate(f, 1), "evidence", { get() { formatted++; return []; } });
  assert.throws(() => h.renderCards(f.catalog, f.assets), /semantic render byte budget exceeded/);
  assert.equal(formatted, 0);
});
