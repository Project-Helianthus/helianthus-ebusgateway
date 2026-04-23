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
  assert.ok(rendered.includes("not supported"),
    `unavailable state must render a 'not supported' placeholder; got: ${rendered}`);
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
