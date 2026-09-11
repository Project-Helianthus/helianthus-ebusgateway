import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
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

test("INT-09 prototype is fixture-only and declares all host perspectives", async () => {
  const [html, js] = await Promise.all([readFile(path.join(prototype, "int09.html"), "utf8"), readFile(path.join(prototype, "int09.js"), "utf8")]);
  assert.match(html, /href="#main"/);
  assert.match(html, /role="status" aria-live="polite"/);
  assert.match(js, /five-domain-catalog\.json/);
  assert.match(js, /state-matrix\.json/);
  for (const name of ["Registry", "Plane", "Lens", "Compare", "Provenance", "History", "Native"]) assert.match(js, new RegExp(`"${name}"`));
  assert.doesNotMatch(js, /fetch\(\s*["']\/(api|graphql)/i);
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
