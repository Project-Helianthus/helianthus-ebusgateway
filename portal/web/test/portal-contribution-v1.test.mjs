import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

const here = path.dirname(fileURLToPath(import.meta.url));
const prototype = path.resolve(here, "../prototype");
const fixture = path.resolve(here, "../../contributionv1/testdata/five-domain-catalog.json");
const states = path.resolve(here, "../../contributionv1/testdata/state-matrix.json");
const schema = path.resolve(here, "../../../docs/schemas/portal-driver-contribution-v1.schema.json");

test("INT-09 fixture pins all five domains and the exact pack versions", async () => {
  const catalog = JSON.parse(await readFile(fixture, "utf8"));
  assert.equal(catalog.manifests.length, 5);
  assert.deepEqual(catalog.index.packs.map((p) => `${p.id}@${p.version}`), [
    "helianthus.pack.thermal@1.0.0", "helianthus.pack.pv@1.0.0", "helianthus.pack.storage@1.1.0", "helianthus.pack.evse@1.0.0", "helianthus.pack.infrastructure@1.0.0"
  ]);
  assert.equal(catalog.resources.length, 5);
  assert.equal(catalog.contribution_states.length, 5);
  assert.equal(catalog.navigation.default_resource, "thermal-system");
});

test("INT-09 schema is closed and matches the host renderer boundary", async () => {
  const parsed = JSON.parse(await readFile(schema, "utf8"));
  assert.equal(parsed.additionalProperties, false);
  assert.equal(parsed.properties.contract.const, "helianthus.gateway.portal-contribution/v1");
  assert.deepEqual(parsed.$defs.view.properties.renderer.enum, ["summary", "field_table", "relationship_graph", "state_strip", "timeline", "evidence_table", "session_controls"]);
  assert.deepEqual(parsed.$defs.view.properties.slot.enum, ["lens", "native_diagnostics"]);
  assert.ok(!("ref" in parsed.$defs.action.properties));
  assert.ok(!parsed.$defs.action.required.includes("ref"));
  assert.ok(parsed.$defs.action.required.includes("operation_ref"));
});

test("INT-09 prototype is fixture-only and reads perspectives from the fixture", async () => {
  const [html, js] = await Promise.all([readFile(path.join(prototype, "int09.html"), "utf8"), readFile(path.join(prototype, "int09.js"), "utf8")]);
  assert.match(html, /href="#main"/);
  assert.match(html, /role="status" aria-live="polite"/);
  assert.match(js, /five-domain-catalog\.json/);
  assert.match(js, /state-matrix\.json/);
  const catalog = JSON.parse(await readFile(fixture, "utf8"));
  assert.deepEqual(catalog.navigation.perspectives.map((item) => item.label), ["Registry", "Plane", "Lens", "Compare", "Provenance", "History", "Native"]);
  assert.doesNotMatch(js, /manifest\.manifest_id\s*===\s*["']portal/);
  assert.doesNotMatch(js, /fetch\(\s*["']\/(api|graphql)/i);
});

test("INT-09 fixture navigation handles changed state and a valid sixth contribution generically", async () => {
  const catalog = JSON.parse(await readFile(fixture, "utf8"));
  const mod = await import(pathToFileURL(path.join(prototype, "int09.js")).href);
  let state = mod.initialNavigation(catalog);
  state = mod.reduceNavigation(catalog, state, {kind: "perspective", id: "provenance"});
  assert.equal(state.perspective, "provenance");
  state = mod.reduceNavigation(catalog, state, {kind: "resource", id: "storage-pack"});
  assert.equal(state.capability, "storage-read");
  assert.equal(mod.visibleContributions(catalog, state)[0].state, "partial");

  const sixth = structuredClone(catalog.manifests[0]);
  sixth.manifest_id = "portal.fixture.sixth";
  sixth.contributor.driver_id = "fixture.sixth";
  catalog.manifests.push(sixth);
  catalog.resources.push({id: "sixth-resource", domain: "Fixture", items: ["resource"], detail: "Generic fixture contribution.", state: "stale", contribution_id: sixth.manifest_id, capabilities: [{id: "sixth-read", label: "Fixture read", state: "stale"}]});
  catalog.contribution_states.push({manifest_id: sixth.manifest_id, resource_id: "sixth-resource", capability_id: "sixth-read", state: "stale"});
  state = mod.reduceNavigation(catalog, state, {kind: "resource", id: "sixth-resource"});
  const visible = mod.visibleContributions(catalog, state);
  assert.equal(visible.length, 1);
  assert.equal(visible[0].manifest.manifest_id, sixth.manifest_id);
  assert.equal(visible[0].state, "stale");
});

test("INT-09 preserves every requested state and caller-scoped action outcome", async () => {
  const matrix = JSON.parse(await readFile(states, "utf8"));
  assert.deepEqual(matrix.states.map((s) => s.id), ["unavailable", "stale", "conflict", "partial", "unknown", "unsupported", "withdrawn", "withheld", "invalid-contribution"]);
  assert.deepEqual(matrix.actions.map((a) => a.id), ["unauthorized-hidden", "authorized-disabled", "admitted-enabled"]);
  assert.equal(matrix.b503.transport, "graphql-only");
  assert.match(matrix.b503.issue, /remains open/);
});

test("INT-09 prototype uses focus-visible and non-color text state cues", async () => {
  const css = await readFile(path.join(prototype, "int09.css"), "utf8");
  assert.match(css, /:focus-visible/);
  const matrix = JSON.parse(await readFile(states, "utf8"));
  for (const state of matrix.states) assert.ok(state.text.length > state.id.length, `${state.id} needs textual cue`);
});
