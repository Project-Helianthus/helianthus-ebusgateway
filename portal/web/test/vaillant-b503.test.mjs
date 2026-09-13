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
          _listeners: new Map(),
          addEventListener(type, listener) { this._listeners.set(type, listener); },
          append() {},
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

test("VaillantB503Pane_AutoDisableKeepsTokenOnGraphQLError", async () => {
  const { source, sourcePath } = await loadShellSource();
  const status = makeAuditedElement();
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([['[data-role="vaillant-b503-live-status"]', status]]),
    // HTTP transport succeeds, but GraphQL rejects the disable request.
    fetchImpl: makeGqlFetchImpl([
      { match: "vaillantLiveMonitor", reply: { data: { vaillantLiveMonitor: null }, errors: [{ message: "SESSION_BUSY" }] } },
    ]),
  });
  shell._vaillantB503LiveToken = "tok-still-needed";
  shell._vaillantB503LiveTarget = 8;

  const cleaned = await Object.getPrototypeOf(shell).handleVaillantB503NavAway.call(shell);
  assert.equal(cleaned, false, "GraphQL error must leave cleanup pending");
  assert.equal(shell._vaillantB503LiveToken, "tok-still-needed",
    "HTTP-successful GraphQL failure must retain issuer token for a later bounded cleanup");
  assert.equal(shell._vaillantB503LiveTarget, 8,
    "failure must retain the old target paired with the issuer token");
  assert.match(status.textContent, /Disable pending: SESSION_BUSY/,
    "the UI must state that session closure was not confirmed");
  assert.equal(fetchRequests.length, 1, "the handler must not auto-retry indefinitely");
});

test("VaillantB503Pane_LateEnable_AtoBRetainsPriorTokenUntilConfirmedCleanup", async () => {
  const { source, sourcePath } = await loadShellSource();
  const status = makeAuditedElement();
  let resolveEnable;
  let disableAttempts = 0;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([['[data-role="vaillant-b503-live-status"]', status]]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("VaillantLive(") && variables.action === "enable") {
        return new Promise((resolve) => {
          resolveEnable = () => resolve({
            ok: true, status: 200,
            json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: "token-A", rawHex: null, disabled: false } } }),
          });
        });
      }
      if (query.includes("VaillantLiveDisable") && variables.action === "disable") {
        disableAttempts += 1;
        const reply = disableAttempts === 1
          ? { data: { vaillantLiveMonitor: null }, errors: [{ message: "SESSION_BUSY" }] }
          : { data: { vaillantLiveMonitor: { disabled: true } } };
        return Promise.resolve({ ok: true, status: 200, json: async () => reply });
      }
      throw new Error(`unexpected GraphQL request ${query}`);
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503TargetAddress = 8;
  const lateA = proto.invokeVaillantLiveMonitor.call(shell, "enable");
  await flush();

  // A completes only after the operator selected B. The canonical B context
  // must remain untouched while A's token-bound cleanup is retained.
  shell._vaillantB503TargetAddress = 21;
  shell._vaillantB503Epoch = 1;
  resolveEnable();
  await lateA;

  assert.equal(shell._vaillantB503TargetAddress, 21, "late A completion must not replace current B target");
  assert.equal(shell._vaillantB503Epoch, 1, "late A completion must not mutate B epoch");
  assert.equal(shell._vaillantB503LiveToken, "token-A", "failed A cleanup must retain A issuer token");
  assert.equal(shell._vaillantB503LiveTarget, 8, "failed A cleanup must retain A target");
  assert.match(status.textContent, /Disable pending: SESSION_BUSY/);
  assert.equal(fetchRequests.length, 2, "late completion gets exactly one immediate cleanup attempt");
  const firstDisable = parseGqlInit(fetchRequests[1].init).variables;
  assert.deepEqual(firstDisable, { action: "disable", issuerToken: "token-A", targetAddress: 8 },
    "late cleanup must use A's returned token and A target");

  const cleaned = await proto.handleVaillantB503NavAway.call(shell);
  assert.equal(cleaned, true, "one later cleanup may clear after disabled=true");
  assert.equal(shell._vaillantB503LiveToken, null);
  assert.equal(shell._vaillantB503LiveTarget, null);
  assert.equal(shell._vaillantB503TargetAddress, 21, "later A cleanup must still preserve B selection");
  assert.equal(fetchRequests.length, 3, "later cleanup is bounded to one explicit attempt");
  const secondDisable = parseGqlInit(fetchRequests[2].init).variables;
  assert.deepEqual(secondDisable, { action: "disable", issuerToken: "token-A", targetAddress: 8 });
});

test("VaillantB503Pane_StaleEnableKeepsNewerSessionWhenOldCleanupIsBusy", async () => {
  const { source, sourcePath } = await loadShellSource();
  const status = makeAuditedElement({ textContent: "Live-monitor session active." });
  let resolveEnableA;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([['[data-role="vaillant-b503-live-status"]', status]]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("VaillantLive(") && variables.action === "enable") {
        return new Promise((resolve) => {
          resolveEnableA = () => resolve({
            ok: true, status: 200,
            json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: "token-A", rawHex: null, disabled: false } } }),
          });
        });
      }
      if (query.includes("VaillantLiveDisable") && variables.action === "disable") {
        return Promise.resolve({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: null }, errors: [{ message: "SESSION_BUSY" }] }),
        });
      }
      throw new Error(`unexpected GraphQL request ${query}`);
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503TargetAddress = 8;
  const lateA = proto.invokeVaillantLiveMonitor.call(shell, "enable");
  await flush();

  // B is already a newer active session when the old A enable completes.
  shell._vaillantB503TargetAddress = 21;
  shell._vaillantB503Epoch = 1;
  shell._vaillantB503LiveToken = "token-B";
  shell._vaillantB503LiveTarget = 21;
  resolveEnableA();
  await lateA;

  assert.equal(shell._vaillantB503LiveToken, "token-B",
    "stale A enable must never publish over a newer B cleanup token");
  assert.equal(shell._vaillantB503LiveTarget, 21,
    "stale A enable must never publish over B's target");
  assert.equal(shell._vaillantB503TargetAddress, 21);
  assert.equal(status.textContent, "Live-monitor session active.",
    "old cleanup failure must not overwrite B's current status");
  assert.equal(fetchRequests.length, 2, "late A gets one direct bounded cleanup attempt");
  assert.deepEqual(parseGqlInit(fetchRequests[0].init).variables,
    { action: "enable", targetAddress: 8 });
  assert.deepEqual(parseGqlInit(fetchRequests[1].init).variables,
    { action: "disable", issuerToken: "token-A", targetAddress: 8 },
    "direct cleanup must use the obsolete A token and captured A target");
});

test("VaillantB503Pane_DelayedOldCleanupPreservesReplacementToken", async () => {
  const { source, sourcePath } = await loadShellSource();
  const status = makeAuditedElement({ textContent: "Live-monitor session active." });
  let resolveOldCleanup;
  let disableAttempts = 0;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([['[data-role="vaillant-b503-live-status"]', status]]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (!query.includes("VaillantLiveDisable") || variables.action !== "disable") {
        throw new Error(`unexpected GraphQL request ${query}`);
      }
      disableAttempts += 1;
      if (disableAttempts === 1) {
        return new Promise((resolve) => {
          resolveOldCleanup = () => resolve({
            ok: true, status: 200,
            json: async () => ({ data: { vaillantLiveMonitor: { disabled: true } } }),
          });
        });
      }
      return Promise.resolve({
        ok: true, status: 200,
        json: async () => ({ data: { vaillantLiveMonitor: { disabled: true } } }),
      });
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503LiveToken = "token-A";
  shell._vaillantB503LiveTarget = 8;
  shell._vaillantB503TargetAddress = 8;

  const oldCleanup = proto.handleVaillantB503NavAway.call(shell);
  await flush();

  // The pane reopens and a current B enable replaces the recoverable cleanup
  // pair while the old A disable response remains pending.
  shell._vaillantB503LiveToken = "token-B";
  shell._vaillantB503LiveTarget = 21;
  shell._vaillantB503TargetAddress = 21;
  shell._vaillantB503Epoch = 1;
  resolveOldCleanup();
  assert.equal(await oldCleanup, true, "old cleanup was confirmed by the gateway");

  assert.equal(shell._vaillantB503LiveToken, "token-B",
    "confirmed old A cleanup must not erase a newer B cleanup token");
  assert.equal(shell._vaillantB503LiveTarget, 21,
    "confirmed old A cleanup must not erase B's paired target");
  assert.equal(shell._vaillantB503TargetAddress, 21,
    "old completion must not mutate the current B target");
  assert.equal(status.textContent, "Live-monitor session active.",
    "old completion must not replace B's current session status");
  assert.deepEqual(parseGqlInit(fetchRequests[0].init).variables,
    { action: "disable", issuerToken: "token-A", targetAddress: 8 });

  assert.equal(await proto.handleVaillantB503NavAway.call(shell), true,
    "the replacement B pair remains available for one later bounded cleanup");
  assert.equal(shell._vaillantB503LiveToken, null);
  assert.equal(shell._vaillantB503LiveTarget, null);
  assert.equal(fetchRequests.length, 2, "cleanup attempts remain explicit and bounded");
  assert.deepEqual(parseGqlInit(fetchRequests[1].init).variables,
    { action: "disable", issuerToken: "token-B", targetAddress: 21 });
});

test("VaillantB503Pane_TargetProbeUnmountsOldLiveControlsBeforeBQualification", async () => {
  const { source, sourcePath } = await loadShellSource();
  const body = makeAuditedElement();
  const target = makeAuditedElement({ value: "21", addEventListener() {} });
  const status = makeAuditedElement();
  const output = makeAuditedElement();
  const controls = new Map();
  for (const role of ["enable", "read", "disable"]) {
    const listeners = new Map();
    controls.set(role, makeAuditedElement({
      _listeners: listeners,
      addEventListener(type, listener) { listeners.set(type, listener); },
    }));
  }
  let resolveBProbe;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="vaillant-b503-body"]', body],
      ['[data-role="vaillant-b503-target"]', target],
      ['[data-role="vaillant-b503-live-status"]', status],
      ['[data-role="vaillant-b503-live-output"]', output],
      ['[data-role="vaillant-b503-live-enable"]', controls.get("enable")],
      ['[data-role="vaillant-b503-live-read"]', controls.get("read")],
      ['[data-role="vaillant-b503-live-disable"]', controls.get("disable")],
    ]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("VaillantB503Cap")) {
        assert.equal(variables.targetAddress, 21, "only B is qualified after target selection");
        return new Promise((resolve) => {
          resolveBProbe = () => resolve({
            ok: true, status: 200,
            json: async () => ({ data: { vaillantCapabilities: { vaillantB503: { available: false, reason: "NOT_SUPPORTED" } } } }),
          });
        });
      }
      throw new Error(`unexpected B503 operation during target qualification: ${query}`);
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503TargetAddress = 8;
  shell._vaillantB503CapabilityReason = "AVAILABLE";
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;

  // These are click handlers from the previously available A live-monitor
  // pane. The pending state must make even those stale references inert.
  controls.get("enable").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "enable"));
  controls.get("read").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "read"));
  controls.get("disable").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "disable"));

  const changing = proto.changeVaillantB503Target.call(shell);
  assert.equal(shell._vaillantB503CapabilityReason, "PENDING",
    "B must become operation-pending synchronously before cleanup/probe yields");
  assert.match(body.innerHTML, /b503-state-pending/);
  assert.doesNotMatch(body.innerHTML, /vaillant-b503-live-(?:enable|read|disable)/,
    "pending pane must unmount every B503 live-monitor control");
  for (const role of ["enable", "read", "disable"]) controls.get(role)._listeners.get("click")();
  await flush();

  const liveOperations = fetchRequests.map((request) => parseGqlInit(request.init))
    .filter(({ query }) => query.includes("VaillantLive("));
  assert.equal(liveOperations.length, 0,
    "stale A controls clicked while B probes must send no B SERVICE_WRITE/read operation");
  assert.equal(fetchRequests.length, 1, "only B's capability probe may leave the browser during qualification");

  resolveBProbe();
  await changing;
  assert.equal(shell._vaillantB503CapabilityReason, "NOT_SUPPORTED");
  assert.match(body.innerHTML, /b503-state-not-supported/);
});

test("VaillantB503Pane_DelayedExplicitDisablePreservesNewSameTargetEnable", async () => {
  const { source, sourcePath } = await loadShellSource();
  const status = makeAuditedElement();
  const session = makeAuditedElement();
  let resolveOldDisable;
  let disableAttempts = 0;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="vaillant-b503-live-status"]', status],
      ['[data-role="vaillant-b503-live-output"]', makeAuditedElement()],
      ['[data-role="vaillant-b503-session-strip"]', session],
    ]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("VaillantLive(") && variables.action === "disable") {
        disableAttempts += 1;
        if (disableAttempts === 1) {
          return new Promise((resolve) => {
            resolveOldDisable = () => resolve({
              ok: true, status: 200,
              json: async () => ({ data: { vaillantLiveMonitor: { disabled: true } } }),
            });
          });
        }
        return Promise.resolve({ ok: true, status: 200, json: async () => ({ data: { vaillantLiveMonitor: { disabled: true } } }) });
      }
      if (query.includes("VaillantLive(") && variables.action === "enable") {
        return Promise.resolve({
          ok: true, status: 200,
          json: async () => ({ data: { vaillantLiveMonitor: { issuerToken: "token-B", rawHex: null, disabled: false } } }),
        });
      }
      if (query.includes("VaillantLiveMonitorSession")) {
        return Promise.resolve({ ok: true, status: 200, json: async () => ({ data: { vaillantLiveMonitorSession: { state: "Active", owned: true } } }) });
      }
      throw new Error(`unexpected GraphQL request ${query}`);
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503CapabilityReason = "AVAILABLE";
  shell._vaillantB503TargetAddress = 21;
  shell._vaillantB503LiveToken = "token-A";
  shell._vaillantB503LiveTarget = 21;

  const oldDisable = proto.invokeVaillantLiveMonitor.call(shell, "disable");
  await flush();
  await proto.invokeVaillantLiveMonitor.call(shell, "enable");
  assert.equal(shell._vaillantB503LiveToken, "token-B", "new same-target enable replaces A token while disable is pending");
  assert.equal(shell._vaillantB503LiveTarget, 21);
  assert.equal(status.textContent, "Live-monitor session active.");

  resolveOldDisable();
  await oldDisable;
  assert.equal(shell._vaillantB503LiveToken, "token-B",
    "late A disable confirmation must not erase same-target B token");
  assert.equal(shell._vaillantB503LiveTarget, 21);
  assert.equal(status.textContent, "Live-monitor session active.",
    "late A disable must not replace B's active status");

  await proto.invokeVaillantLiveMonitor.call(shell, "disable");
  assert.equal(shell._vaillantB503LiveToken, null,
    "a later explicit B disable may clear exactly its submitted pair");
  assert.equal(shell._vaillantB503LiveTarget, null);
  const liveCalls = fetchRequests.map((request) => parseGqlInit(request.init))
    .filter(({ query }) => query.includes("VaillantLive("));
  assert.deepEqual(liveCalls.map(({ variables }) => variables), [
    { action: "disable", targetAddress: 21, issuerToken: "token-A" },
    { action: "enable", targetAddress: 21 },
    { action: "disable", targetAddress: 21, issuerToken: "token-B" },
  ]);
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

test("VaillantB503ProjectionCard_ignores_out_of_order_A_B_A_capability_results", async () => {
  const { source, sourcePath } = await loadShellSource();
  const appended = [];
  const grid = makeAuditedElement({ querySelector: () => null, append: (node) => appended.push(node) });
  const deviceSelect = makeAuditedElement({ value: "8" });
  const pending = [];
  const { shell } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="projection-uml-grid"]', grid],
      ['[data-role="projection-device-select"]', deviceSelect],
    ]),
    fetchImpl: (_url, init) => new Promise((resolve) => {
      pending.push({
        target: parseGqlInit(init).variables.targetAddress,
        resolve: () => resolve({ ok: true, status: 200, json: async () => ({ data: { vaillantCapabilities: { vaillantB503: { reason: "AVAILABLE" } } } }) }),
      });
    }),
  });
  shell.projectionDevices = [
    { address: 8, display_name: "A", projections: [] },
    { address: 21, display_name: "B", projections: [] },
  ];
  const proto = Object.getPrototypeOf(shell);

  const firstA = proto.loadAllProjectionPlanes.call(shell);
  await flush();
  deviceSelect.value = "21";
  const B = proto.loadAllProjectionPlanes.call(shell);
  await flush();
  deviceSelect.value = "8";
  const newestA = proto.loadAllProjectionPlanes.call(shell);
  await flush();
  assert.deepEqual(pending.map((request) => request.target), [8, 21, 8]);

  pending[0].resolve();
  await firstA;
  pending[1].resolve();
  await B;
  assert.equal(appended.length, 0, "superseded A and B requests must not append cards");
  pending[2].resolve();
  await newestA;
  assert.equal(appended.length, 1, "only the newest A invocation may append its card");
});

test("VaillantB503ProjectionCard_target_wins_over_a_stale_hidden_picker", async () => {
  const { source, sourcePath } = await loadShellSource();
  const appended = [];
  const grid = makeAuditedElement({ querySelector: () => null, append: (node) => appended.push(node) });
  const staleTarget = makeAuditedElement({ value: "8" });
  const paneBody = makeAuditedElement();
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="projection-device-select"]', makeAuditedElement({ value: "21" })],
      ['[data-role="vaillant-b503-target"]', staleTarget],
      ['[data-role="vaillant-b503-body"]', paneBody],
      ['[data-role="vaillant-b503-errors-body"]', makeAuditedElement()],
    ]),
    fetchImpl: makeGqlFetchImpl([
      { match: "VaillantB503Projection", reply: { data: { vaillantCapabilities: { vaillantB503: { reason: "AVAILABLE" } } } } },
      { match: "vaillantCapabilities", reply: { data: { vaillantCapabilities: { vaillantB503: { reason: "AVAILABLE" } } } } },
      { match: "VaillantErrors", reply: { data: { vaillantErrors: { firstActiveError: null, slots: [] } } } },
    ]),
  });

  await Object.getPrototypeOf(shell).renderVaillantB503ProjectionCard.call(shell, grid, 21);
  assert.equal(appended.length, 1, "the admitted card must be rendered");
  const click = appended[0]._listeners.get("click");
  assert.equal(typeof click, "function", "the projection card must be navigable");
  click();
  await flush();

  assert.equal(Object.getPrototypeOf(shell)._vaillantB503Target.call(shell), 21,
    "the card target must remain canonical despite the stale picker");
  await Object.getPrototypeOf(shell).refreshVaillantErrors.call(shell);
  const b503Calls = fetchRequests
    .map((request) => parseGqlInit(request.init))
    .filter(({ query }) => query.includes("vaillantCapabilities") || query.includes("VaillantErrors"));
  assert.ok(b503Calls.length >= 2, "card navigation must probe capability and issue later B503 requests");
  for (const call of b503Calls) {
    assert.equal(call.variables.targetAddress, 21,
      `stale picker must not override ${call.query}`);
  }
});

test("VaillantB503ProjectionCard_qualifiesTargetBeforeStaleLiveControlsCanRun", async () => {
  const { source, sourcePath } = await loadShellSource();
  const appended = [];
  const grid = makeAuditedElement({ querySelector: () => null, append: (node) => appended.push(node) });
  const paneBody = makeAuditedElement();
  const staleTarget = makeAuditedElement({ value: "8", addEventListener() {} });
  const status = makeAuditedElement();
  const output = makeAuditedElement();
  const controls = new Map();
  for (const role of ["enable", "read", "disable"]) {
    const listeners = new Map();
    controls.set(role, makeAuditedElement({
      _listeners: listeners,
      addEventListener(type, listener) { listeners.set(type, listener); },
    }));
  }
  let resolveBProbe;
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath,
    elements: new Map([
      ['[data-role="projection-device-select"]', makeAuditedElement({ value: "21" })],
      ['[data-role="vaillant-b503-target"]', staleTarget],
      ['[data-role="vaillant-b503-body"]', paneBody],
      ['[data-role="vaillant-b503-live-status"]', status],
      ['[data-role="vaillant-b503-live-output"]', output],
      ['[data-role="vaillant-b503-live-enable"]', controls.get("enable")],
      ['[data-role="vaillant-b503-live-read"]', controls.get("read")],
      ['[data-role="vaillant-b503-live-disable"]', controls.get("disable")],
    ]),
    fetchImpl: (_url, init) => {
      const { query, variables } = parseGqlInit(init);
      if (query.includes("VaillantB503Projection")) {
        return Promise.resolve({ ok: true, status: 200, json: async () => ({ data: { vaillantCapabilities: { vaillantB503: { reason: "AVAILABLE" } } } }) });
      }
      if (query.includes("VaillantB503Cap")) {
        assert.equal(variables.targetAddress, 21, "card target B must be the only target qualified");
        return new Promise((resolve) => {
          resolveBProbe = () => resolve({
            ok: true, status: 200,
            json: async () => ({ data: { vaillantCapabilities: { vaillantB503: { reason: "NOT_SUPPORTED" } } } }),
          });
        });
      }
      throw new Error(`unexpected B503 operation while card target qualifies: ${query}`);
    },
  });
  const proto = Object.getPrototypeOf(shell);
  shell._vaillantB503TargetAddress = 8;
  shell._vaillantB503CapabilityReason = "AVAILABLE";
  shell.renderVaillantB503Pane = proto.renderVaillantB503Pane;
  controls.get("enable").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "enable"));
  controls.get("read").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "read"));
  controls.get("disable").addEventListener("click", () => proto.invokeVaillantLiveMonitor.call(shell, "disable"));

  await proto.renderVaillantB503ProjectionCard.call(shell, grid, 21);
  assert.equal(appended.length, 1, "available projection capability must expose a card");
  appended[0]._listeners.get("click")();
  assert.equal(shell._vaillantB503CapabilityReason, "PENDING",
    "card navigation must publish pending before any cleanup/probe await");
  assert.equal(proto._vaillantB503Target.call(shell), 21);
  assert.match(paneBody.innerHTML, /b503-state-pending/);
  assert.doesNotMatch(paneBody.innerHTML, /vaillant-b503-live-(?:enable|read|disable)/,
    "pending card navigation must withdraw old live controls");
  for (const role of ["enable", "read", "disable"]) controls.get(role)._listeners.get("click")();
  await flush();

  const liveOperations = fetchRequests.map((request) => parseGqlInit(request.init))
    .filter(({ query }) => query.includes("VaillantLive("));
  assert.equal(liveOperations.length, 0,
    "stale A controls after clicking B card must not dispatch a B service operation");
  assert.equal(fetchRequests.length, 2, "only card availability and B qualification requests may occur");
  resolveBProbe();
  await flush();
  await flush();
  assert.equal(shell._vaillantB503CapabilityReason, "NOT_SUPPORTED");
  assert.match(paneBody.innerHTML, /b503-state-not-supported/);
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
