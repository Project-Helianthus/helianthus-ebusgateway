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
  const el = {
    _audit: audit,
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
    _isConnected: true,
    get isConnected() {
      return this._isConnected !== false;
    },
    setAttribute() {},
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
  const createdElements = [];
  const sandbox = {
    console: { error() {}, log() {}, warn() {} },
    document: {
      documentElement: { setAttribute() {} },
      createElement() {
        const element = makeAuditedElement({
          addEventListener() {}, append() {},
          setAttribute(name, value) { this._audit.push({ prop: String(name), value: String(value) }); },
        });
        createdElements.push(element);
        return element;
      },
    },
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
  return { shell, fetchRequests, createdElements };
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

test("VaillantB503Pane_Unknown_ShowsProbeFailureHint", async () => {
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
        reply: { data: { vaillantCapabilities: { vaillantB503: { available: false, reason: "UNKNOWN" } } } },
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
  assert.ok(rendered.includes("probe failure hint"),
    `UNKNOWN state must render the probe-failure hint; got: ${rendered}`);
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

  // Simulate Enable: issuerToken captured into shell state.
  await shell.invokeVaillantLiveMonitor("enable");
  await flush();
  assert.equal(shell._vaillantB503LiveToken, "tok-xyz-123",
    "shell must store issuer token on enable");

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
  assert.ok(!shell._vaillantB503LiveToken,
    "issuerToken must be cleared after auto-disable");
});

test("VaillantB503Pane_reason_matrix_has_stable_state_selectors", async () => {
  const { source, sourcePath } = await loadShellSource();
  for (const [reason, selector, required] of [
    ["AVAILABLE", "b503-state-available", "Live-Monitor"],
    ["NOT_SUPPORTED", "b503-state-not-supported", "Support limitation"],
    ["TRANSPORT_DOWN", "b503-state-transport-down", "Transport warning"],
    ["SESSION_BUSY", "b503-state-session-busy", "Session contention"],
    ["UNKNOWN", "b503-state-unknown", "Probe failure hint"],
  ]) {
    const paneBody = makeAuditedElement();
    const { shell } = buildSandbox({ source, sourcePath, elements: new Map([["[data-role=\"vaillant-b503-body\"]", paneBody]]), fetchImpl: async () => ({ ok: true, json: async () => ({}) }) });
    Object.getPrototypeOf(shell).renderVaillantB503Pane.call(shell, reason, paneBody);
    const rendered = paneBody.innerHTML;
    assert.ok(rendered.includes(`data-testid=\"${selector}\"`), `${reason} must expose ${selector}`);
    assert.ok(rendered.includes(required), `${reason} must expose its documented helpful artefact`);
    assert.ok(rendered.includes('data-role="vaillant-b503-target"'), `${reason} must retain the target selector`);
  }
});

test("VaillantB503Pane_empty_target_preserves_configured_default_for_every_request", async () => {
  const { source, sourcePath } = await loadShellSource();
  const target = makeAuditedElement({ value: "" });
  const elements = new Map([
    ['[data-role="vaillant-b503-target"]', target],
    ['[data-role="vaillant-b503-errors-body"]', makeAuditedElement()],
    ['[data-role="vaillant-b503-service-body"]', makeAuditedElement()],
    ['[data-role="vaillant-b503-history-body"]', makeAuditedElement()],
    ['[data-role="vaillant-b503-live-status"]', makeAuditedElement()],
    ['[data-role="vaillant-b503-live-output"]', makeAuditedElement()],
    ['[data-role="vaillant-b503-session-strip"]', makeAuditedElement()],
  ]);
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: makeGqlFetchImpl([
      { match: "VaillantErrorsHistory", reply: { data: { vaillantErrorsHistory: [] } } },
      { match: "VaillantErrors", reply: { data: { vaillantErrors: { firstActiveError: null, slots: [] } } } },
      { match: "VaillantServiceCurrent", reply: { data: { vaillantServiceCurrent: { firstActiveError: null, slots: [] } } } },
      { match: "VaillantLiveMonitorSession", reply: { data: { vaillantLiveMonitorSession: { state: "Idle", owned: false } } } },
      { match: "VaillantLive", reply: { data: { vaillantLiveMonitor: { rawHex: "", issuerToken: null, disabled: false } } } },
    ]),
  });
  const proto = Object.getPrototypeOf(shell);
  await proto.refreshVaillantErrors.call(shell);
  await proto.refreshVaillantServiceCurrent.call(shell);
  await proto.refreshVaillantErrorsHistory.call(shell);
  await proto.refreshVaillantLiveMonitorSession.call(shell);
  await proto.invokeVaillantLiveMonitor.call(shell, "read");

  assert.equal(proto._vaillantB503Target.call(shell), null, "empty selection must mean configured default");
  const calls = fetchRequests.map((request) => parseGqlInit(request.init));
  assert.equal(calls.length, 6, "live-monitor read also refreshes the session strip");
  for (const call of calls) {
    assert.equal(call.variables.targetAddress, null, `${call.query} must preserve null/default target`);
  }
});

test("VaillantB503Pane_nonempty_targets_keep_the_probed_configured_default_selected", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source, sourcePath, elements: new Map(),
    fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ data: {} }) }),
  });
  shell.projectionDevices = [
    { address: 21, display_name: "Regulator" },
    { address: 8, display_name: "Boiler" },
  ];
  const options = Object.getPrototypeOf(shell)._vaillantB503TargetOptions.call(shell);
  assert.match(options, /^<option value="" selected>Configured default<\/option>/,
    "a direct-open capability probe on configured default must keep null selected");
  assert.match(options, /value="21"/);
  assert.match(options, /value="8"/);
});

test("VaillantB503ProjectionCard_renders_without_projection_planes", async () => {
  const { source, sourcePath } = await loadShellSource();
  const appended = [];
  const grid = makeAuditedElement({ querySelector: () => null, append: (node) => appended.push(node) });
  const deviceSelect = makeAuditedElement({ value: "8" });
  const { shell } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="projection-uml-grid"]', grid],
      ['[data-role="projection-device-select"]', deviceSelect],
    ]),
    fetchImpl: makeGqlFetchImpl([
      { match: "VaillantB503Projection", reply: { data: { vaillantCapabilities: { vaillantB503: { reason: "AVAILABLE" } } } } },
    ]),
  });
  shell.projectionDevices = [{ address: 8, display_name: "Boiler", projections: [] }];
  await Object.getPrototypeOf(shell).loadAllProjectionPlanes.call(shell);
  assert.match(grid.innerHTML, /No non-empty projection planes/);
  assert.equal(appended.length, 1, "capability card must be appended even without graph planes");
  assert.match(appended[0].textContent, /Vaillant B503/);
  assert.ok(appended[0]._audit.some((entry) => entry.prop === "data-role" && entry.value === "projection-b503-card"),
    "projection card must retain the public data-role selector");
});

test("VaillantB503Pane_session_status_does_not_infer_a_foreign_owner", async () => {
  const { source, sourcePath } = await loadShellSource();
  const strip = makeAuditedElement();
  const target = makeAuditedElement({ value: "8" });
  const { shell } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="vaillant-b503-session-strip"]', strip],
      ['[data-role="vaillant-b503-target"]', target],
    ]),
    fetchImpl: makeGqlFetchImpl([
      { match: "VaillantLiveMonitorSession", reply: { data: { vaillantLiveMonitorSession: { state: "Active", owned: true } } } },
    ]),
  });
  await Object.getPrototypeOf(shell).refreshVaillantLiveMonitorSession.call(shell);
  assert.match(strip.innerHTML, /Gateway session gate is held/);
  assert.doesNotMatch(strip.innerHTML.toLowerCase(), /another client|foreign owner/);
  assert.match(strip.innerHTML, /data-testid="b503-session-state-label"/);
});

test("VaillantB503Pane_threads_target_and_discards_stale_error_response", async () => {
  const { source, sourcePath } = await loadShellSource();
  const errorsBody = makeAuditedElement();
  let resolveOld;
  const old = new Promise((resolve) => { resolveOld = resolve; });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements: new Map([["[data-role=\"vaillant-b503-errors-body\"]", errorsBody]]),
    fetchImpl: () => old,
  });
  shell._vaillantB503TargetAddress = 8;
  const pending = Object.getPrototypeOf(shell).refreshVaillantErrors.call(shell);
  shell._vaillantB503Epoch = 1;
  shell._vaillantB503TargetAddress = 21;
  resolveOld({ ok: true, status: 200, json: async () => ({ data: { vaillantErrors: { firstActiveError: 281, slots: [281] } } }) });
  await pending;
  const { variables } = parseGqlInit(fetchRequests[0].init);
  assert.equal(variables.targetAddress, 8, "request must bind the target selected at request time");
  assert.equal(errorsBody.innerHTML, "", "old target response must not mutate the new target view");
});

test("VaillantB503Pane_uses_graphql_history_session_and_AD02_contract", async () => {
  const { source } = await loadShellSource();
  assert.match(source, /vaillantErrorsHistory\(targetAddress: \$targetAddress, limit: \$limit\)/);
  assert.match(source, /vaillantLiveMonitorSession\(targetAddress: \$targetAddress\)/);
  assert.match(source, /b503-install-writes-banner/);
  assert.match(source, /b503-ad02-tooltip-anchor/);
  assert.doesNotMatch(source, /api\/v1\/vaillant/);
});
