import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";
import test from "node:test";

const source = await readFile(new URL("../src/app.js", import.meta.url), "utf8");
function fixture() {
  const state = (desired = false) => ({ desired, effective: false, status: "withheld", reason: desired ? "evidence_unavailable" : "not_selected" });
  return {
    desired: { format: "helianthus.portal-config/v1", revision: "4", drivers: [{ driver_id: "fixture.family.a", enabled: false }, { driver_id: "fixture.family.b", enabled: true }], sharing: { graphql: false, mcp: false }, adapter_proxy: false, output_bindings: { matter: false } },
    listener_application: "pending",
    state: { drivers: [{ driver_id: "fixture.family.b", ...state(true) }, { driver_id: "fixture.family.a", ...state() }], sharing: { graphql: state(), mcp: state() }, adapter_proxy: state(), output_bindings: { matter: state() } },
  };
}
function harness(base = "/") {
  const elements = new Map();
  const node = () => ({ innerHTML: "", textContent: "", disabled: false, handlers: {}, inputs: [], addEventListener(k, fn) { this.handlers[k] = fn; }, querySelectorAll() { return this.inputs; } });
  for (const role of ["config-fields", "config-form", "config-notice", "config-save", "config-reload"]) elements.set(role, node());
  const requests = [], queue = [];
  let deadline;
  const sandbox = { document: {}, HTMLElement: class {}, customElements: { define() {} }, console, AbortController,
    setTimeout(fn) { deadline = fn; return 1; }, clearTimeout() {},
    fetch: async (relative, init) => {
      requests.push({ relative, path: new URL(relative, `https://example.invalid${base}`).pathname, ...init });
      assert.ok(queue.length, "all responses must be injected");
      const next = queue.shift();
      if (next instanceof Error) throw next;
      return await next;
    } };
  vm.createContext(sandbox);
  vm.runInContext(`${source}\nglobalThis.Shell = PortalShell;`, sandbox);
  const shell = new sandbox.Shell();
  shell.querySelector = (selector) => elements.get(selector.match(/data-role="([^"]+)"/)?.[1]) || null;
  shell.querySelectorAll = () => [];
  shell.bindEvents();
  return { shell, requests, queue, elements, deadline: () => deadline(), respond(view, ok = true) { queue.push({ ok, json: async () => structuredClone(view) }); } };
}
const plain = (value) => JSON.parse(JSON.stringify(value));

test("M27 controls cover exactly provisioned driver enablement and four sharing options", async () => {
  const h = harness(); h.respond(fixture());
  assert.equal(await h.shell.requestConfiguration(false), true);
  assert.equal(h.shell.configurationOptions().length, 6);
  const options = h.shell.configurationOptions();
  for (const [index, option] of options.entries()) option.set(index % 2 === 0);
  assert.deepEqual(plain(h.shell._configDraft), {
    ...fixture().desired, drivers: [{ driver_id: "fixture.family.a", enabled: true }, { driver_id: "fixture.family.b", enabled: false }],
    sharing: { graphql: true, mcp: false }, adapter_proxy: true, output_bindings: { matter: false },
  });
  assert.match(h.elements.get("config-fields").innerHTML, /Driver fixture.family.a[\s\S]*Saved desired: Unselected/);
  assert.match(h.elements.get("config-fields").innerHTML, /Driver fixture.family.b[\s\S]*Saved desired: Selected/);
  assert.match(source, /Passive observation and cache are always-on/);
  assert.doesNotMatch(source, /data-config-(observation|cache)/);
});

for (const base of ["/", "/embedded/portal/"]) test(`M27 pending save uses mount-relative GET and PUT at ${base}`, async () => {
  const h = harness(base); h.respond(fixture()); await h.shell.requestConfiguration(false);
  h.shell.configurationOptions()[2].set(true);
  let resolve;
  h.queue.push(new Promise((done) => { resolve = done; }));
  const saving = h.shell.requestConfiguration(true);
  assert.equal(h.elements.get("config-save").disabled, true);
  assert.doesNotMatch(h.elements.get("config-fields").innerHTML, /Effective: Enabled/);
  const view = fixture(); view.desired.revision = "5"; view.desired.sharing.graphql = true;
  view.state.sharing.graphql = { desired: true, effective: false, status: "pending", reason: "listener_application_pending" };
  resolve({ ok: true, json: async () => view });
  assert.equal(await saving, true);
  assert.deepEqual(h.requests.map((r) => [r.relative, r.path, r.method]), [["api/v1/config", `${base}api/v1/config`, "GET"], ["api/v1/config", `${base}api/v1/config`, "PUT"]]);
  const sent = JSON.parse(h.requests[1].body);
  assert.equal(sent.revision, "4"); assert.equal(sent.sharing.graphql, true);
  assert.deepEqual(Object.keys(sent).sort(), ["adapter_proxy", "drivers", "format", "output_bindings", "revision", "sharing"]);
  assert.equal(h.shell._configDraft.revision, "5");
  assert.match(h.elements.get("config-fields").innerHTML, /Saved desired: Selected; Effective: Disabled; Status: pending/);
  assert.doesNotMatch(h.elements.get("config-fields").innerHTML, /Effective: Enabled/);
});

for (const code of ["unsupported_selection", "persistence_failed", "revision_conflict", "listener_conflict"]) test(`M27 ${code} retains input and announces error despite historical response`, async () => {
  const h = harness(); h.respond(fixture()); await h.shell.requestConfiguration(false);
  for (const option of h.shell.configurationOptions()) option.set(!option.value);
  const before = plain(h.shell._configDraft);
  h.respond({ ...fixture(), historical: true, error: { code, next_step: "review saved choices" } }, false);
  assert.equal(await h.shell.requestConfiguration(true), false);
  assert.deepEqual(plain(h.shell._configDraft), before);
  assert.match(h.elements.get("config-notice").textContent, new RegExp(`${code}: review saved choices`));
  assert.equal(h.elements.get("config-save").disabled, false);
  assert.match(source, /data-role="config-notice" role="alert" aria-live="assertive" aria-atomic="true"/);
  const html = h.elements.get("config-fields").innerHTML;
  assert.match(html, /checked/); assert.doesNotMatch(html, /Effective: Enabled/);
});

for (const code of ["invalid_record", "persistence_failed"]) test(`M27 first GET ${code} loads retained historical choices`, async () => {
  const h = harness(), view = fixture();
  view.historical = true;
  view.error = { code, next_step: "review saved choices" };
  view.state.drivers[0].effective = true;
  h.respond(view, false);
  assert.equal(await h.shell.requestConfiguration(false), false);
  assert.deepEqual(plain(h.shell._configView), view);
  assert.deepEqual(plain(h.shell._configDraft), view.desired);
  assert.equal(h.elements.get("config-save").disabled, false);
  assert.match(h.elements.get("config-notice").textContent, new RegExp(`${code}: review saved choices`));
  const html = h.elements.get("config-fields").innerHTML;
  assert.match(html, /Driver fixture.family.b desired selection[\s\S]*Saved desired: Selected; Effective: Unavailable/);
  assert.doesNotMatch(html, /Effective: (Enabled|Disabled)/);
});

test("M27 revision conflict refreshes historical saved choices while retaining rejected edits", async () => {
  const h = harness(); h.respond(fixture()); await h.shell.requestConfiguration(false);
  h.shell.configurationOptions()[3].set(true);
  const before = plain(h.shell._configDraft), view = fixture();
  view.desired.revision = "5";
  view.desired.sharing.graphql = true;
  view.state.sharing.graphql = { desired: true, effective: true, status: "applied", reason: "owner_readback" };
  view.historical = true;
  view.error = { code: "revision_conflict", next_step: "reload saved choices" };
  h.respond(view, false);
  assert.equal(await h.shell.requestConfiguration(true), false);
  assert.deepEqual(plain(h.shell._configView), view);
  assert.deepEqual(plain(h.shell._configDraft), before);
  const html = h.elements.get("config-fields").innerHTML;
  assert.match(html, /GraphQL sharing desired selection<\/label><br><span>Saved desired: Selected; Effective: Unavailable/);
  assert.match(html, /checked > MCP sharing desired selection/);
  assert.doesNotMatch(html, /Effective: (Enabled|Disabled)/);
  assert.match(h.elements.get("config-notice").textContent, /revision_conflict: reload saved choices/);
  assert.equal(h.elements.get("config-save").disabled, false);
});

test("M27 malformed historical records cannot replace accepted choices or rejected edits", async () => {
  for (const loaded of [false, true]) {
    const h = harness();
    if (loaded) { h.respond(fixture()); await h.shell.requestConfiguration(false); h.shell.configurationOptions()[3].set(true); }
    const before = h.shell._configDraft && plain(h.shell._configDraft);
    const view = fixture(); view.desired.sharing.graphql = "invalid";
    h.respond({ ...view, historical: true, error: { code: "persistence_failed" } }, false);
    assert.equal(await h.shell.requestConfiguration(false), false);
    assert.deepEqual(h.shell._configDraft && plain(h.shell._configDraft), before);
    assert.equal(h.elements.get("config-save").disabled, !loaded);
    if (loaded) assert.deepEqual(plain(h.shell._configView.desired), fixture().desired);
    else assert.equal(h.shell._configView, undefined);
  }
});

test("M27 API readback alone enables effective state; evidence-blocked and historical state stays unavailable", async () => {
  const h = harness(), view = fixture();
  view.state.sharing.graphql = { desired: false, effective: true, status: "applied", reason: "owner_readback" };
  view.state.output_bindings.matter = { desired: true, effective: true, status: "withheld", reason: "evidence_unavailable" };
  h.respond(view); await h.shell.requestConfiguration(false);
  const html = h.elements.get("config-fields").innerHTML;
  assert.match(html, /GraphQL sharing[\s\S]*Saved desired: Unselected; Effective: Enabled/);
  assert.match(html, /Matter output binding[\s\S]*Effective: Unavailable; Status: withheld; Reason: evidence_unavailable/);
  h.respond({ ...view, historical: true });
  assert.equal(await h.shell.requestConfiguration(false), false);
  assert.doesNotMatch(h.elements.get("config-fields").innerHTML, /Effective: Enabled/);
});

test("M27 rendered checkboxes and submit/reload handlers update only the selected field", async () => {
  const h = harness(); h.respond(fixture()); await h.shell.requestConfiguration(false);
  const fields = h.elements.get("config-fields");
  for (let index = 0; index < 6; index++) {
    const input = { checked: true, handlers: {}, getAttribute: () => String(index), addEventListener(k, fn) { this.handlers[k] = fn; } };
    fields.inputs = [input]; h.shell.renderConfiguration(); input.handlers.change();
    assert.equal(h.shell.configurationOptions()[index].value, true);
  }
  let prevented = false;
  h.respond(fixture());
  h.elements.get("config-form").handlers.submit({ preventDefault() { prevented = true; } });
  await new Promise(setImmediate);
  assert.equal(prevented, true); assert.equal(h.requests.at(-1).method, "PUT");
  h.respond(fixture()); h.elements.get("config-reload").handlers.click(); await new Promise(setImmediate);
  assert.equal(h.requests.at(-1).method, "GET");
});

test("M27 transport/malformed failures retain input and stale detached completions cannot replace it", async () => {
  const h = harness(); h.respond(fixture()); await h.shell.requestConfiguration(false);
  h.shell.configurationOptions()[3].set(true);
  const before = plain(h.shell._configDraft);
  for (const reply of [new Error("synthetic failure"), { ok: true, json: async () => ({ desired: {} }) }]) {
    h.queue.push(reply); assert.equal(await h.shell.requestConfiguration(true), false);
    assert.deepEqual(plain(h.shell._configDraft), before);
  }
  let resolve; h.queue.push(new Promise((done) => { resolve = done; }));
  const pending = h.shell.requestConfiguration(true);
  h.deadline(); resolve({ ok: true, json: async () => fixture() });
  assert.equal(await pending, false); assert.deepEqual(plain(h.shell._configDraft), before);
  assert.match(h.elements.get("config-notice").textContent, /request timed out/);
  h.queue.push(Promise.resolve({ ok: true, json: async () => fixture() }));
  const detached = h.shell.requestConfiguration(true);
  h.shell._configAbort.abort(); h.shell._configRequest++;
  assert.equal(await detached, false); assert.deepEqual(plain(h.shell._configDraft), before);
});

for (let index = 0; index < 6; index++) test(`M27 option ${index} saves only its own desired field`, async () => {
  const h = harness(); h.respond(fixture()); await h.shell.requestConfiguration(false);
  const expected = fixture().desired;
  const mutations = [
    () => { expected.drivers[0].enabled = true; },
    () => { expected.drivers[1].enabled = false; },
    () => { expected.sharing.graphql = true; },
    () => { expected.sharing.mcp = true; },
    () => { expected.adapter_proxy = true; },
    () => { expected.output_bindings.matter = true; },
  ];
  mutations[index]();
  const option = h.shell.configurationOptions()[index]; option.set(!option.value);
  h.respond(fixture()); await h.shell.requestConfiguration(true);
  assert.deepEqual(JSON.parse(h.requests.at(-1).body), expected);
});
