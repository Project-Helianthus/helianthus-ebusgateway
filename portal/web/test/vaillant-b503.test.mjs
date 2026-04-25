// Tests for the M3_PORTAL Vaillant B503 pane (issue #521).
//
// Mirrors the FakeDOM + audit-log harness from l7-catalog.test.mjs. The
// Vaillant B503 pane is read-only over GraphQL; there is NO install-write
// UI (plan AD02). The live-monitor tab auto-disables on nav-away.

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

function makeAuditedElement(extra = {}) {
  const audit = [];
  let innerHTMLValue = "";
  let textContentValue = "";
  const listeners = new Map();
  const el = {
    _audit: audit,
    _listeners: listeners,
    get innerHTML() {
      return innerHTMLValue;
    },
    set innerHTML(v) {
      innerHTMLValue = String(v);
      audit.push({ prop: "innerHTML", value: String(v) });
    },
    get textContent() {
      return textContentValue;
    },
    set textContent(v) {
      textContentValue = String(v);
      audit.push({ prop: "textContent", value: String(v) });
    },
    className: "",
    style: {},
    disabled: false,
    value: "",
    _isConnected: true,
    get isConnected() {
      return this._isConnected !== false;
    },
    setAttribute() {},
    addEventListener(name, handler) {
      if (!listeners.has(name)) listeners.set(name, []);
      listeners.get(name).push(handler);
    },
    dispatch(name, evt = {}) {
      const arr = listeners.get(name) || [];
      for (const h of arr) {
        h(evt);
      }
    },
    ...extra,
  };
  return el;
}

function flush() {
  return new Promise((resolve) => setImmediate(resolve));
}

async function loadShellSource() {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");
  return { source, sourcePath };
}

function buildSandbox({ source, sourcePath, elements, fetchImpl }) {
  class FakeHTMLElement {
    constructor() {
      this._isConnected = true;
    }
    get isConnected() {
      return this._isConnected !== false;
    }
  }
  const fetchRequests = [];
  const sandbox = {
    console: { error() {}, log() {}, warn() {} },
    document: { documentElement: { setAttribute() {} } },
    customElements: { define() {} },
    HTMLElement: FakeHTMLElement,
    localStorage: { getItem: () => null, setItem() {} },
    setInterval: () => ({}),
    clearInterval: () => {},
    setTimeout,
    clearTimeout,
    AbortController,
    URLSearchParams,
    TextDecoder,
    fetch: (url, init) => {
      fetchRequests.push({ url, init });
      return Promise.resolve(fetchImpl(url, init));
    },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(
    `${source}\n;globalThis.__PortalShell = PortalShell;`,
    sandbox,
    { filename: pathToFileURL(sourcePath).href },
  );
  const PortalShell = sandbox.__PortalShell;
  const shell = new PortalShell();
  shell._isConnected = true;
  shell.render = () => {};
  shell.bindEvents = () => {};
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];
  return { shell, fetchRequests };
}

// Parse a GraphQL fetch call and return {query, variables} from the init body.
function parseGqlInit(init) {
  if (!init || !init.body) return { query: "", variables: {} };
  try {
    const body = JSON.parse(String(init.body));
    return {
      query: String(body.query || ""),
      variables: body.variables || {},
    };
  } catch {
    return { query: "", variables: {} };
  }
}

// Build a fetchImpl that routes GraphQL POSTs by query-name substring.
function makeGqlFetchImpl(routes, fallback = { data: {}, errors: null }) {
  return (url, init) => {
    const { query, variables } = parseGqlInit(init);
    for (const route of routes) {
      if (query.includes(route.match)) {
        const payload = typeof route.reply === "function"
          ? route.reply(variables)
          : route.reply;
        return { ok: true, status: 200, json: async () => payload };
      }
    }
    return { ok: true, status: 200, json: async () => fallback };
  };
}

// ---- 1. Nav item registered ----

test("VaillantB503Pane_NavItemRegistered", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source, sourcePath, elements: new Map(),
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.render = proto.render;
  let capturedHTML = "";
  Object.defineProperty(shell, "innerHTML", {
    set(v) { capturedHTML = String(v); },
    get() { return capturedHTML; },
    configurable: true,
  });
  shell.render();

  assert.ok(capturedHTML.includes('data-role="nav-vaillant-b503"'),
    "render() must emit nav-vaillant-b503 sidebar button");
  assert.ok(capturedHTML.includes('data-nav-target="section-vaillant-b503"'),
    "nav button must target section-vaillant-b503");
  assert.ok(capturedHTML.includes('id="section-vaillant-b503"'),
    "render() must emit id=section-vaillant-b503");
});

// ---- 2. Unavailable capability → empty-state placeholder ----

test("VaillantB503Pane_Unavailable_ShowsPlaceholder", async () => {
  // M8 F2 tightened the reason-render contract: NOT_SUPPORTED renders
  // its own distinct copy (not aliased to UNKNOWN). The M3 baseline
  // assertion is preserved by mocking NOT_SUPPORTED here; the UNKNOWN
  // reason now renders a probe-failure hint distinct from
  // "not supported" (verified by M8_F2_ReasonRender_UNKNOWN).
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const elements = new Map([
    ['[data-role="vaillant-b503-body"]', paneBody],
  ]);
  const { shell } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: false, reason: "NOT_SUPPORTED" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;

  assert.equal(typeof shell.refreshVaillantB503Capability, "function",
    "refreshVaillantB503Capability must be defined");

  await shell.refreshVaillantB503Capability();
  await flush();

  const htmlWrites = paneBody._audit.filter((e) => e.prop === "innerHTML");
  assert.ok(htmlWrites.length >= 1, "pane body should receive rendered HTML");
  const rendered = htmlWrites.map((e) => e.value).join("\n").toLowerCase();
  assert.ok(rendered.includes("not implemented") || rendered.includes("not supported"),
    `unavailable state must render a 'not supported / not implemented' placeholder; got: ${rendered}`);
});

// ---- 3. Available capability → three tabs ----

test("VaillantB503Pane_Available_ShowsThreeTabs", async () => {
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const elements = new Map([
    ['[data-role="vaillant-b503-body"]', paneBody],
  ]);
  const { shell } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;

  await shell.refreshVaillantB503Capability();
  await flush();

  const htmlWrites = paneBody._audit.filter((e) => e.prop === "innerHTML");
  const rendered = htmlWrites.map((e) => e.value).join("\n");
  assert.ok(/data-role="vaillant-b503-tab-errors"/.test(rendered),
    "errors tab must be rendered");
  assert.ok(/data-role="vaillant-b503-tab-service"/.test(rendered),
    "service tab must be rendered");
  assert.ok(/data-role="vaillant-b503-tab-live-monitor"/.test(rendered),
    "live-monitor tab must be rendered");
});

// ---- 4. Errors tab renders slots ----

test("VaillantB503Pane_ErrorsTab_RendersSlots", async () => {
  const { source, sourcePath } = await loadShellSource();
  const errorsBody = makeAuditedElement();
  const elements = new Map([
    ['[data-role="vaillant-b503-errors-body"]', errorsBody],
  ]);
  const { shell } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantErrors",
        reply: {
          data: {
            vaillantErrors: {
              firstActiveError: 281,
              slots: [281, null, null, null, null],
            },
          },
        },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.refreshVaillantErrors = proto.refreshVaillantErrors;

  assert.equal(typeof shell.refreshVaillantErrors, "function",
    "refreshVaillantErrors must be defined");

  await shell.refreshVaillantErrors();
  await flush();

  const htmlWrites = errorsBody._audit.filter((e) => e.prop === "innerHTML");
  assert.ok(htmlWrites.length >= 1, "errors body should render");
  const rendered = htmlWrites.map((e) => e.value).join("\n");
  assert.ok(rendered.includes("281"), "firstActiveError 281 must be rendered");
  // 4 em-dashes for the 4 null slots.
  const emDashCount = (rendered.match(/—/g) || []).length;
  assert.ok(emDashCount >= 4,
    `expected at least 4 em-dashes for null slots; got ${emDashCount} in: ${rendered}`);
});

// ---- 5. No install-write affordance ----

test("VaillantB503Pane_NoInstallWriteAffordance", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source, sourcePath, elements: new Map(),
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.render = proto.render;
  let capturedHTML = "";
  Object.defineProperty(shell, "innerHTML", {
    set(v) { capturedHTML = String(v); },
    get() { return capturedHTML; },
    configurable: true,
  });
  shell.render();

  // Extract the vaillant-b503 pane subtree: from the section open tag until
  // the matching closing </section>. We do a non-greedy slice — if any of
  // the forbidden verbs appears inside the pane, fail.
  const openTag = 'id="section-vaillant-b503"';
  const start = capturedHTML.indexOf(openTag);
  assert.ok(start >= 0, "section-vaillant-b503 must exist in rendered HTML");
  // Search forward for </section> starting from openTag.
  const end = capturedHTML.indexOf("</section>", start);
  assert.ok(end > start, "section-vaillant-b503 must have a closing </section>");
  const paneHTML = capturedHTML.slice(start, end).toLowerCase();

  // If the pane renders its body dynamically (body placeholder), also include
  // a capability=AVAILABLE dynamic render to audit live-monitor markup.
  const paneBody = makeAuditedElement();
  const elements = new Map([
    ['[data-role="vaillant-b503-body"]', paneBody],
  ]);
  const { shell: shell2 } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: (url, init) => {
      const body = init && init.body ? JSON.parse(String(init.body)) : {};
      const q = String(body.query || "");
      if (q.includes("vaillantCapabilities")) {
        return { ok: true, status: 200, json: async () => ({ data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } }) };
      }
      return { ok: true, status: 200, json: async () => ({ data: {} }) };
    },
  });
  const proto2 = Object.getPrototypeOf(shell2);
  shell2.renderVaillantB503Pane = proto2.renderVaillantB503Pane;
  shell2.refreshVaillantB503Capability = proto2.refreshVaillantB503Capability;
  await shell2.refreshVaillantB503Capability();
  await flush();

  const dynamicHTML = paneBody._audit
    .filter((e) => e.prop === "innerHTML")
    .map((e) => e.value).join("\n").toLowerCase();

  const combinedHTML = paneHTML + "\n" + dynamicHTML;
  for (const banned of ["clear", "delete", "reset"]) {
    assert.ok(!combinedHTML.includes(banned),
      `vaillant-b503 pane must not contain '${banned}' (install-write forbidden per AD02/AD06). Found in: ${combinedHTML}`);
  }
});

// ---- 6. Live-monitor auto-disable on nav leave ----

test("VaillantB503Pane_LiveMonitor_AutoDisableOnLeave", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: new Map(),
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantLiveMonitor",
        reply: (vars) => {
          if (vars && vars.action === "enable") {
            return { data: { vaillantLiveMonitor: { issuerToken: "tok-xyz-123", rawHex: null, disabled: false } } };
          }
          if (vars && vars.action === "disable") {
            return { data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } };
          }
          return { data: { vaillantLiveMonitor: { issuerToken: null, rawHex: "aabb", disabled: false } } };
        },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.handleVaillantB503NavAway = proto.handleVaillantB503NavAway;

  assert.equal(typeof shell.invokeVaillantLiveMonitor, "function",
    "invokeVaillantLiveMonitor must be defined");
  assert.equal(typeof shell.handleVaillantB503NavAway, "function",
    "handleVaillantB503NavAway must be defined");

  // M8 F1: tokens are now per-target. The M3 single-token field
  // (_vaillantB503LiveToken) is replaced by a per-target Map; the
  // accessor vaillantB503LiveTokenForTarget(addr) is the supported
  // surface. The M3 nav-away semantics still hold: an enabled session
  // must produce a disable GraphQL call on nav-away. We bind the
  // per-target accessor to keep this test asserting the same contract.
  shell.vaillantB503LiveTokenForTarget = proto.vaillantB503LiveTokenForTarget;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  // Default target = 0 (no projection list provided in this M3 baseline test).
  shell._vaillantB503Target = 0;

  // Simulate Enable: issuerToken captured into per-target token map.
  await shell.invokeVaillantLiveMonitor("enable");
  await flush();
  assert.equal(shell.vaillantB503LiveTokenForTarget(0), "tok-xyz-123",
    "shell must store issuer token on enable (per-target map)");

  // Simulate nav-away from vaillant-b503.
  const beforeCount = fetchRequests.length;
  await shell.handleVaillantB503NavAway();
  await flush();

  // Assert a disable GraphQL call was made with the stored token.
  const afterCalls = fetchRequests.slice(beforeCount);
  const disableCall = afterCalls.find((r) => {
    const { query, variables } = parseGqlInit(r.init);
    return query.includes("vaillantLiveMonitor") && variables && variables.action === "disable";
  });
  assert.ok(disableCall,
    `a vaillantLiveMonitor(action:"disable") call must fire on nav-away; saw: ${JSON.stringify(afterCalls.map((c) => parseGqlInit(c.init)))}`);
  const { variables: disableVars } = parseGqlInit(disableCall.init);
  assert.equal(disableVars.issuerToken, "tok-xyz-123",
    "disable call must pass the stored issuerToken");
  // Token must be cleared after disable.
  assert.ok(!shell.vaillantB503LiveTokenForTarget(0),
    "issuerToken must be cleared after auto-disable");
});

// =====================================================================
// M8_PORTAL_UX_GAPS — F1..F8 acceptance tests (issue #552)
//
// Plan ref: vaillant-b503-namespace-w17-26.implementing/13-amendment-1-dispatcher-portal-ux.md §M8
// Canonical-SHA256: 86495340799be9340dc191c371a49a958f65c357c76a1e0a2974502c8489b508
// =====================================================================

// Helper: build an elements-map that lazily creates audited elements as
// the production code queries selectors it needs. Keeps tests robust to
// ordering of selector probes inside renderVaillantB503Pane.
function makeLazyElementMap(prepopulated = {}) {
  const map = new Map();
  for (const [k, v] of Object.entries(prepopulated)) {
    map.set(k, v);
  }
  const get = (selector) => {
    if (!map.has(selector)) {
      map.set(selector, makeAuditedElement());
    }
    return map.get(selector);
  };
  return { map, get };
}

// Helper: install a more permissive shell.querySelector that materialises
// audited elements on demand AND supports composite-rendered children.
// The pane body's innerHTML is virtual so child elements don't actually
// render — but the production code only reads textContent / dispatches
// click events on selectors it pre-registered, so a lazy map is enough.
function attachLazyQuerySelector(shell, lazy) {
  shell.querySelector = (sel) => lazy.get(sel);
  shell.querySelectorAll = (sel) => {
    const el = lazy.get(sel);
    return el ? [el] : [];
  };
}

function combinedHTML(elementMap) {
  const out = [];
  for (const el of elementMap.values()) {
    if (!el || !el._audit) continue;
    for (const e of el._audit) {
      if (e.prop === "innerHTML" || e.prop === "textContent") {
        out.push(e.value);
      }
    }
  }
  return out.join("\n");
}

// ---- F2 reason-render matrix: 5 separate tests, one per state ------
// Each state must render its own data-testid AND a state-specific artefact
// so the UI is matrix-asserted rather than visual-only.

const reasonContract = [
  {
    state: "AVAILABLE",
    testid: "b503-state-available",
    // available state must render one of the live-data anchors
    artefactRegex: /vaillant-b503-tab-(errors|service|live-monitor|history)/,
    artefactName: "live-data tab anchors",
  },
  {
    state: "NOT_SUPPORTED",
    testid: "b503-state-not-supported",
    artefactRegex: /b503 not implemented/i,
    artefactName: "support-limitation copy",
  },
  {
    state: "TRANSPORT_DOWN",
    testid: "b503-state-transport-down",
    artefactRegex: /transport|adapter/i,
    artefactName: "transport warning copy",
  },
  {
    state: "SESSION_BUSY",
    testid: "b503-state-session-busy",
    artefactRegex: /owner|another client|release/i,
    artefactName: "ownership warning copy",
  },
  {
    state: "UNKNOWN",
    testid: "b503-state-unknown",
    artefactRegex: /probe|diagnostic|retry/i,
    artefactName: "probe-failure-hint copy",
  },
];

for (const row of reasonContract) {
  test(`M8_F2_ReasonRender_${row.state}`, async () => {
    const { source, sourcePath } = await loadShellSource();
    const paneBody = makeAuditedElement();
    const elements = new Map([
      ['[data-role="vaillant-b503-body"]', paneBody],
    ]);
    const { shell } = buildSandbox({
      source, sourcePath, elements,
      fetchImpl: makeGqlFetchImpl([
        {
          match: "vaillantCapabilities",
          reply: { data: { vaillantCapabilities: { vaillantB503: { available: row.state === "AVAILABLE", reason: row.state } } } },
        },
      ]),
    });
    const proto = Object.getPrototypeOf(shell);
    shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
    shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;

    await shell.refreshVaillantB503Capability();
    await flush();

    const htmlWrites = paneBody._audit.filter((e) => e.prop === "innerHTML");
    const rendered = htmlWrites.map((e) => e.value).join("\n");
    assert.ok(
      rendered.includes(`data-testid="${row.testid}"`),
      `${row.state}: required data-testid="${row.testid}" missing in rendered HTML; got: ${rendered}`,
    );
    assert.ok(
      row.artefactRegex.test(rendered),
      `${row.state}: required artefact (${row.artefactName}) matching ${row.artefactRegex} missing; got: ${rendered}`,
    );
  });
}

// ---- F1 per-target awareness ----------------------------------------

test("M8_F1_TargetSelector_PopulatedFromProjectionDevices", async () => {
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-body"]': paneBody,
  });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  attachLazyQuerySelector(shell, lazy);

  // Seed the projection device list (same source that feeds section-projection).
  shell.projectionDevices = [
    { address: 8, display_name: "BAI00", projections: [] },
    { address: 21, display_name: "BASV2", projections: [] },
    { address: 16, display_name: "Controller", projections: [] },
  ];

  await shell.refreshVaillantB503Capability();
  await flush();

  // Assert the implementation queried the b503-target-select element
  // (proves wiring to the static section markup) AND populated it with
  // option entries for every projection device.
  assert.ok(lazy.map.has('[data-role="b503-target-select"]'),
    "target selector [data-role=\"b503-target-select\"] must be queried by the implementation");
  const selectEl = lazy.map.get('[data-role="b503-target-select"]');
  const html = combinedHTML(lazy.map);
  assert.ok(
    /<option /.test(selectEl.innerHTML || ""),
    `target selector innerHTML must contain <option> entries; got innerHTML: ${selectEl.innerHTML}`,
  );
  // Selector must list at least the three seeded devices.
  for (const addr of ["8", "21", "16"]) {
    assert.ok(
      html.includes(`value="${addr}"`),
      `target selector must contain device address ${addr}; got: ${html}`,
    );
  }
});

test("M8-TGT-01_CapabilityInvalidation_PerTargetCacheKeys", async () => {
  // Threading targetAddress through every B503 GraphQL query: when target
  // switches, the next capability fetch and downstream queries MUST carry
  // the new targetAddress.
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-body"]': paneBody,
  });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } },
      },
      { match: "vaillantErrors", reply: { data: { vaillantErrors: { firstActiveError: null, slots: [] } } } },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  shell.refreshVaillantErrors = proto.refreshVaillantErrors;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [
    { address: 8, display_name: "BAI00", projections: [] },
    { address: 21, display_name: "BASV2", projections: [] },
  ];
  assert.equal(typeof shell.setVaillantB503Target, "function",
    "setVaillantB503Target must be defined for F1 target switching");

  await shell.setVaillantB503Target(8);
  await flush();
  await shell.refreshVaillantErrors();
  await flush();

  const beforeSwitch = fetchRequests.length;
  await shell.setVaillantB503Target(21);
  await flush();
  await shell.refreshVaillantErrors();
  await flush();

  const afterSwitchCalls = fetchRequests.slice(beforeSwitch);
  const errorsCallsForT2 = afterSwitchCalls.filter((r) => {
    const { query, variables } = parseGqlInit(r.init);
    return query.includes("vaillantErrors") && Number(variables.targetAddress) === 21;
  });
  assert.ok(
    errorsCallsForT2.length >= 1,
    `errors query MUST carry targetAddress=21 after switch; saw: ${JSON.stringify(afterSwitchCalls.map((c) => parseGqlInit(c.init)))}`,
  );
});

test("M8-TGT-02_LiveMonitorOwnership_PerTargetIsolation", async () => {
  // IsOwned()-derived ownership state must be tracked per-target, not globally.
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantLiveMonitor",
        reply: (vars) => {
          if (vars && vars.action === "enable") {
            return { data: { vaillantLiveMonitor: { issuerToken: `tok-${vars.targetAddress || "x"}`, rawHex: null, disabled: false } } };
          }
          return { data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } };
        },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  shell.vaillantB503LiveTokenForTarget = proto.vaillantB503LiveTokenForTarget;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [
    { address: 8, display_name: "BAI00" },
    { address: 21, display_name: "BASV2" },
  ];
  assert.equal(typeof shell.vaillantB503LiveTokenForTarget, "function",
    "vaillantB503LiveTokenForTarget must be defined for per-target ownership");

  await shell.setVaillantB503Target(8);
  await shell.invokeVaillantLiveMonitor("enable");
  await flush();

  await shell.setVaillantB503Target(21);
  await flush();
  // After switching to T2, the per-target lookup for T1 should still show
  // the local-user-owned token; T2 starts with no token.
  assert.equal(shell.vaillantB503LiveTokenForTarget(8), `tok-8`,
    "T1 ownership must persist after switching to T2");
  assert.ok(!shell.vaillantB503LiveTokenForTarget(21),
    "T2 must have no token before its own enable");
});

test("M8-TGT-03_HistoryInvalidation_PerTargetCache", async () => {
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantErrorHistory",
        reply: (vars) => ({
          data: { vaillantErrorHistory: { index: vars.index || 0, firstActiveError: 100 + (vars.targetAddress || 0), slots: [] } },
        }),
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.refreshVaillantErrorHistory = proto.refreshVaillantErrorHistory;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [
    { address: 8, display_name: "BAI00" },
    { address: 21, display_name: "BASV2" },
  ];
  assert.equal(typeof shell.refreshVaillantErrorHistory, "function",
    "refreshVaillantErrorHistory must be defined for F5 history sub-tab");

  await shell.setVaillantB503Target(8);
  await shell.refreshVaillantErrorHistory();
  await flush();

  const beforeSwitch = fetchRequests.length;
  await shell.setVaillantB503Target(21);
  await shell.refreshVaillantErrorHistory();
  await flush();

  const afterCalls = fetchRequests.slice(beforeSwitch);
  const t2Call = afterCalls.find((r) => {
    const { query, variables } = parseGqlInit(r.init);
    return query.includes("vaillantErrorHistory") && Number(variables.targetAddress) === 21;
  });
  assert.ok(t2Call,
    `history query must carry targetAddress=21 after switch; got: ${JSON.stringify(afterCalls.map((c) => parseGqlInit(c.init)))}`,
  );
});

test("M8-TGT-04_TargetSwitchDuringEnableInflight_StaleResponseDiscarded", async () => {
  // R5 A1 contract: target-switch during live-monitor enable in-flight on T1 with
  // switch to T2 before completion. T1 enable completion MUST NOT mutate T2 strip
  // state. If local ownership is obtained on T1 after the switch, the frontend
  // MUST immediately issue disable for T1.
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  let releaseT1Enable;
  const t1EnablePromise = new Promise((resolve) => { releaseT1Enable = resolve; });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: (url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("vaillantLiveMonitor") && variables.action === "enable" && Number(variables.targetAddress) === 8) {
        // Hold T1 enable until released by the test.
        return t1EnablePromise.then(() => ({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: "tok-T1", rawHex: null, disabled: false } } }),
        }));
      }
      if (query.includes("vaillantLiveMonitor")) {
        return Promise.resolve({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } }),
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({ data: {} }) });
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  shell.vaillantB503LiveTokenForTarget = proto.vaillantB503LiveTokenForTarget;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [
    { address: 8, display_name: "BAI00" },
    { address: 21, display_name: "BASV2" },
  ];

  await shell.setVaillantB503Target(8);
  // Kick off enable on T1 but don't await — leave it in flight.
  const t1EnableP = shell.invokeVaillantLiveMonitor("enable");
  await flush();

  // Switch to T2 while T1 enable is still pending.
  await shell.setVaillantB503Target(21);
  await flush();

  // Release T1 enable response now (post-switch).
  releaseT1Enable();
  await t1EnableP.catch(() => {}); // tolerate any propagated state-mutation block
  await flush();
  // Allow internal disable-cleanup to fire.
  await flush();
  await flush();

  // T2 strip MUST NOT have been mutated by T1 enable completion.
  assert.ok(!shell.vaillantB503LiveTokenForTarget(21),
    "T1 enable completion MUST NOT mutate T2 ownership");

  // After switch: an immediate disable for T1 must have fired (preferred per R5 A1).
  const disableForT1 = fetchRequests.find((r) => {
    const { query, variables } = parseGqlInit(r.init);
    return query.includes("vaillantLiveMonitor") && variables.action === "disable" && Number(variables.targetAddress) === 8;
  });
  assert.ok(disableForT1,
    "after target-switch with local-user-owned T1 enable completing, frontend MUST issue disable for T1");
});

// ---- F4 session-state strip transitions ------------------------------

test("M8_F4_SessionStateStrip_IdleToEnablingToActiveToDisabled", async () => {
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  let releaseEnable;
  const enablePromise = new Promise((resolve) => { releaseEnable = resolve; });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: (url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("vaillantLiveMonitor") && variables.action === "enable") {
        return enablePromise.then(() => ({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: "tok-strip", rawHex: null, disabled: false } } }),
        }));
      }
      if (query.includes("vaillantLiveMonitor") && variables.action === "disable") {
        return Promise.resolve({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } }),
        });
      }
      return Promise.resolve({ ok: true, status: 200, json: async () => ({ data: {} }) });
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.vaillantB503SessionStateForTarget = proto.vaillantB503SessionStateForTarget;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  attachLazyQuerySelector(shell, lazy);

  assert.equal(typeof shell.vaillantB503SessionStateForTarget, "function",
    "vaillantB503SessionStateForTarget must be defined for F4 session strip");

  shell.projectionDevices = [{ address: 8, display_name: "BAI00" }];
  await shell.setVaillantB503Target(8);

  // Idle initially.
  assert.equal(shell.vaillantB503SessionStateForTarget(8), "Idle");

  // Kick off enable; while pending, state must be Enabling.
  const enP = shell.invokeVaillantLiveMonitor("enable");
  await flush();
  assert.equal(shell.vaillantB503SessionStateForTarget(8), "Enabling",
    "while enable is in-flight, state must be Enabling");

  // Resolve enable; state Active.
  releaseEnable();
  await enP;
  await flush();
  assert.equal(shell.vaillantB503SessionStateForTarget(8), "Active",
    "after enable resolves with token, state must be Active");

  // Disable; state Disabled or Idle.
  await shell.invokeVaillantLiveMonitor("disable");
  await flush();
  const finalState = shell.vaillantB503SessionStateForTarget(8);
  assert.ok(finalState === "Idle" || finalState === "Disabled",
    `after disable, state must be Idle or Disabled; got ${finalState}`);
});

test("M8_F4_SessionStateStrip_OwnedByOther_WhenSessionBusy_NoLocalToken", async () => {
  // Capability=SESSION_BUSY without a local token implies another client owns
  // the session. Strip must surface "Owned by another client" affordance.
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-body"]': paneBody,
  });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: false, reason: "SESSION_BUSY" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  attachLazyQuerySelector(shell, lazy);

  await shell.refreshVaillantB503Capability();
  await flush();

  const html = combinedHTML(lazy.map);
  assert.ok(
    /b503-state-session-busy/.test(html) && /(another client|owned by other|owner)/i.test(html),
    `SESSION_BUSY must render owned-by-other affordance; got: ${html}`,
  );
});

// ---- F5 history tab loading ------------------------------------------

test("M8_F5_HistoryTab_LoadsAndRendersRecords", async () => {
  const { source, sourcePath } = await loadShellSource();
  const historyBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-history-body"]': historyBody,
  });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantErrorHistory",
        reply: (vars) => ({
          data: { vaillantErrorHistory: { index: vars.index || 0, firstActiveError: 281, slots: [281, null, null, null, null] } },
        }),
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.refreshVaillantErrorHistory = proto.refreshVaillantErrorHistory;
  attachLazyQuerySelector(shell, lazy);

  await shell.refreshVaillantErrorHistory();
  await flush();

  const html = historyBody._audit.filter((e) => e.prop === "innerHTML").map((e) => e.value).join("\n");
  assert.ok(html.includes("281"), `history must render record value 281; got: ${html}`);
});

test("M8_F5_HistoryTab_EmptyState", async () => {
  const { source, sourcePath } = await loadShellSource();
  const historyBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-history-body"]': historyBody,
  });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantErrorHistory",
        reply: { data: { vaillantErrorHistory: { index: 0, firstActiveError: null, slots: [] } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.refreshVaillantErrorHistory = proto.refreshVaillantErrorHistory;
  attachLazyQuerySelector(shell, lazy);

  await shell.refreshVaillantErrorHistory();
  await flush();

  const html = historyBody._audit.filter((e) => e.prop === "innerHTML").map((e) => e.value).join("\n");
  assert.ok(/—|empty|no records/i.test(html),
    `empty history must show em-dash or empty-state; got: ${html}`);
});

test("M8_F5_HistoryTab_RenderedInAvailableState", async () => {
  // History must appear as a tab in the AVAILABLE state markup.
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-body"]': paneBody,
  });
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  attachLazyQuerySelector(shell, lazy);

  await shell.refreshVaillantB503Capability();
  await flush();

  const html = paneBody._audit.filter((e) => e.prop === "innerHTML").map((e) => e.value).join("\n");
  assert.ok(/data-role="vaillant-b503-tab-history"/.test(html),
    `History tab must be rendered alongside Errors / Service / Live-Monitor; got: ${html}`);
});

// ---- F4 nav-away disable (extends M3 baseline with target awareness) -

test("M8_F4_NavAway_DisableIssuedForOwnedTargets", async () => {
  // When local user enabled session for T1, navigating away MUST issue a
  // targeted disable for T1.
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantLiveMonitor",
        reply: (vars) => {
          if (vars && vars.action === "enable") {
            return { data: { vaillantLiveMonitor: { issuerToken: `tok-${vars.targetAddress || "x"}`, rawHex: null, disabled: false } } };
          }
          return { data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } };
        },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.handleVaillantB503NavAway = proto.handleVaillantB503NavAway;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [{ address: 8, display_name: "BAI00" }];
  await shell.setVaillantB503Target(8);
  await shell.invokeVaillantLiveMonitor("enable");
  await flush();

  const beforeNav = fetchRequests.length;
  await shell.handleVaillantB503NavAway();
  await flush();

  const after = fetchRequests.slice(beforeNav);
  const disable = after.find((r) => {
    const { query, variables } = parseGqlInit(r.init);
    return query.includes("vaillantLiveMonitor") && variables.action === "disable" && Number(variables.targetAddress) === 8;
  });
  assert.ok(disable,
    `nav-away must issue targeted disable for T1=8; saw: ${JSON.stringify(after.map((c) => parseGqlInit(c.init)))}`);
});

// ---- F6 AD02 install-writes banner ----------------------------------

test("M8_F6_AD02Banner_PresentWithStableSelectors", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source, sourcePath, elements: new Map(),
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.render = proto.render;
  let capturedHTML = "";
  Object.defineProperty(shell, "innerHTML", {
    set(v) { capturedHTML = String(v); },
    get() { return capturedHTML; },
    configurable: true,
  });
  shell.render();

  const start = capturedHTML.indexOf('id="section-vaillant-b503"');
  assert.ok(start >= 0, "section-vaillant-b503 must exist in rendered HTML");
  const end = capturedHTML.indexOf("</section>", start);
  const paneHTML = capturedHTML.slice(start, end);

  assert.ok(/data-testid="b503-install-writes-banner"/.test(paneHTML),
    `banner with data-testid="b503-install-writes-banner" must be in pane; got: ${paneHTML}`);
  assert.ok(/id="b503-ad02-tooltip-anchor"/.test(paneHTML),
    `help affordance with id="b503-ad02-tooltip-anchor" must be in pane; got: ${paneHTML}`);
  // tooltip target must point to canonical plan ref.
  assert.ok(/vaillant-b503-namespace-w17-26/.test(paneHTML),
    `tooltip target must reference canonical plan slug; got: ${paneHTML}`);
});

// ---- F3 projection fold-in ------------------------------------------

test("M8_F3_Projection_B503PlaneCardAppears_WhenAvailable", async () => {
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: true, reason: "AVAILABLE" } } } },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderProjectionB503Card = proto.renderProjectionB503Card;
  attachLazyQuerySelector(shell, lazy);
  assert.equal(typeof shell.renderProjectionB503Card, "function",
    "renderProjectionB503Card must be defined for F3 projection fold-in");

  // Render the B503 plane card for 3 distinct devices that all have
  // capability=AVAILABLE for B503.
  for (const addr of [0x08, 0x15, 0x10]) {
    await shell.renderProjectionB503Card({ address: addr, display_name: `dev-${addr}` }, "AVAILABLE");
    await flush();
  }
  const html = combinedHTML(lazy.map);
  // The card must be rendered for ≥3 distinct addresses.
  let count = 0;
  for (const addr of [0x08, 0x15, 0x10]) {
    const hex = `0x${addr.toString(16).padStart(2, "0")}`;
    if (html.includes(`data-role="projection-b503-card"`) && html.includes(hex)) count += 1;
  }
  assert.ok(count >= 3,
    `projection B503 plane card must appear for ≥3 distinct addresses (got ${count}); html: ${html}`);
});

test("M8_F3_Projection_NoB503CardWhenNotAvailable", async () => {
  const { source, sourcePath } = await loadShellSource();
  const lazy = makeLazyElementMap();
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderProjectionB503Card = proto.renderProjectionB503Card;
  attachLazyQuerySelector(shell, lazy);

  await shell.renderProjectionB503Card({ address: 0x08, display_name: "BAI" }, "NOT_SUPPORTED");
  await flush();
  const html = combinedHTML(lazy.map);
  assert.ok(!/data-role="projection-b503-card"/.test(html),
    `B503 plane card must NOT render for capability=NOT_SUPPORTED; html: ${html}`);
});

// ---- Frontend epoch-rollover composite (F3+F4 R4 A3) ----------------

test("M8_EpochRollover_Composite_4Step", async () => {
  const { source, sourcePath } = await loadShellSource();
  const paneBody = makeAuditedElement();
  const lazy = makeLazyElementMap({
    '[data-role="vaillant-b503-body"]': paneBody,
  });
  let capState = "AVAILABLE";
  const { shell } = buildSandbox({
    source, sourcePath, elements: lazy.map,
    fetchImpl: makeGqlFetchImpl([
      {
        match: "vaillantCapabilities",
        reply: () => ({ data: { vaillantCapabilities: { vaillantB503: { available: capState === "AVAILABLE", reason: capState } } } }),
      },
      {
        match: "vaillantLiveMonitor",
        reply: (vars) => {
          if (vars && vars.action === "enable") {
            return { data: { vaillantLiveMonitor: { issuerToken: "tok-epoch", rawHex: null, disabled: false } } };
          }
          return { data: { vaillantLiveMonitor: { issuerToken: null, rawHex: null, disabled: true } } };
        },
      },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  shell.refreshVaillantB503Capability = proto.refreshVaillantB503Capability;
  shell.invokeVaillantLiveMonitor = proto.invokeVaillantLiveMonitor;
  shell.vaillantB503SessionStateForTarget = proto.vaillantB503SessionStateForTarget;
  shell.setVaillantB503Target = proto.setVaillantB503Target;
  attachLazyQuerySelector(shell, lazy);

  shell.projectionDevices = [{ address: 8, display_name: "BAI" }];
  await shell.setVaillantB503Target(8);

  // Step 1: capability=AVAILABLE, session enabled, State=Active.
  await shell.refreshVaillantB503Capability();
  await shell.invokeVaillantLiveMonitor("enable");
  await flush();
  assert.equal(shell.vaillantB503SessionStateForTarget(8), "Active",
    "step 1: post-enable state must be Active");

  // Step 2: transport-down — capability flips to TRANSPORT_DOWN; strip leaves Active.
  capState = "TRANSPORT_DOWN";
  await shell.refreshVaillantB503Capability();
  await flush();
  const stepTwoState = shell.vaillantB503SessionStateForTarget(8);
  assert.notEqual(stepTwoState, "Active",
    `step 2: after capability=TRANSPORT_DOWN, state MUST leave Active immediately; got ${stepTwoState}`);

  // Step 3: reconnect, capability=UNKNOWN — strip must NOT show stale Active.
  capState = "UNKNOWN";
  await shell.refreshVaillantB503Capability();
  await flush();
  const stepThreeState = shell.vaillantB503SessionStateForTarget(8);
  assert.notEqual(stepThreeState, "Active",
    `step 3: capability=UNKNOWN must NOT show stale Active; got ${stepThreeState}`);

  // Step 4: post-reconnect first success → AVAILABLE; strip back to Idle.
  capState = "AVAILABLE";
  await shell.refreshVaillantB503Capability();
  await flush();
  const stepFourState = shell.vaillantB503SessionStateForTarget(8);
  assert.ok(stepFourState === "Idle" || stepFourState === "Disabled",
    `step 4: after first post-reconnect success, state MUST be Idle (NOT Active until user re-enables); got ${stepFourState}`);
});
