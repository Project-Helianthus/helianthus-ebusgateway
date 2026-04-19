// Tests for the M5_PORTAL ebus_standard L7 consumer UI.
//
// These tests extend the existing PortalShell harness (see portal-shell.test.mjs)
// with an innerHTML/textContent audit log on each FakeHTMLElement, which is
// load-bearing for the XSS assertions on the decode sandbox.
//
// Runner: node:test (NOT Jest / JSDOM). The audit-log pattern is more
// auditable than JSDOM's silent parse because it records the *exact* setter
// sequence — a violation is a recorded innerHTML assignment, not "did a
// <script> node materialize somewhere in the DOM".

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

// AuditedElement wraps an element with setter intercepts for innerHTML and
// textContent. Every assignment is recorded on the element's _audit array.
// The property values remain directly readable so the SUT-under-test can
// assert what it just wrote.
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
    _isConnected: true,
    get isConnected() {
      return this._isConnected !== false;
    },
    setAttribute() {},
    ...extra,
  };
  return el;
}

function createDeferredResponse(payload) {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return {
    promise,
    resolve() {
      resolve({
        ok: true,
        status: 200,
        json: async () => payload,
      });
    },
  };
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
      return fetchImpl(url, init);
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

// ---- 1. Section render uses escapeHtml for catalog strings ----

test("L7 catalog services list renders via escapeHtml (safe) for catalog strings", async () => {
  const { source, sourcePath } = await loadShellSource();
  const servicesBody = makeAuditedElement();
  const elements = new Map([
    ["[data-role=\"l7-services-body\"]", servicesBody],
  ]);
  const servicesResponse = createDeferredResponse({
    meta: {
      contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 },
      consistency: { mode: "LIVE" },
      data_hash: "a".repeat(64),
    },
    data: {
      namespace: "ebus_standard",
      catalog_version: "v-test",
      services: [
        { pb: 5, name: "identification", description: "Identify", command_count: 2 },
      ],
    },
    error: null,
  });
  const { shell } = buildSandbox({
    source,
    sourcePath,
    elements,
    fetchImpl: () => servicesResponse.promise,
  });

  assert.equal(typeof shell.refreshL7Services, "function",
    "PortalShell must expose refreshL7Services for the L7 catalog section");

  const p = shell.refreshL7Services();
  servicesResponse.resolve();
  await p;

  // Catalog tables use innerHTML (after escapeHtml). At least one innerHTML
  // assignment must have occurred on the services body.
  const htmlWrites = servicesBody._audit.filter((e) => e.prop === "innerHTML");
  assert.ok(htmlWrites.length >= 1, "services body should receive rendered HTML");
  assert.ok(htmlWrites.some((e) => e.value.includes("identification")),
    "service name should appear in rendered HTML");
});

// ---- 2. Decode sandbox output uses textContent only (XSS hardening) ----

test("L7 decode sandbox output uses textContent, never innerHTML, for user-controlled bytes", async () => {
  const { source, sourcePath } = await loadShellSource();
  const decodeOutput = makeAuditedElement();
  const decodeStatus = makeAuditedElement();
  const elements = new Map([
    ["[data-role=\"l7-decode-output\"]", decodeOutput],
    ["[data-role=\"l7-decode-status\"]", decodeStatus],
  ]);

  const xssHex = "3c7363726970743e616c6572742831293c2f7363726970743e"; // "<script>alert(1)</script>"
  const decodeResponse = createDeferredResponse({
    meta: {
      contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 },
      consistency: { mode: "LIVE" },
      data_hash: "b".repeat(64),
    },
    data: {
      namespace: "ebus_standard",
      catalog_version: "v-test",
      command_id: "cmd.decoded",
      // Server echoes raw bytes + decoded repr containing literal script tag —
      // the frontend must render this as plain text.
      raw_bytes: [0x3c, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x3e],
      decoded_repr: "<script>alert(1)</script>",
      validity: "catalog_identified",
    },
    error: null,
  });

  const { shell } = buildSandbox({
    source,
    sourcePath,
    elements,
    fetchImpl: () => decodeResponse.promise,
  });

  assert.equal(typeof shell.submitL7Decode, "function",
    "PortalShell must expose submitL7Decode for the decode sandbox");

  const p = shell.submitL7Decode({
    pb: 5,
    sb: 4,
    direction: "master_to_slave",
    frame_type: "MM",
    payload_hex: xssHex,
  });
  decodeResponse.resolve();
  await p;

  // (a) decode-output element received content via textContent setter.
  const textWrites = decodeOutput._audit.filter((e) => e.prop === "textContent");
  assert.ok(textWrites.length >= 1,
    "decode output must be set via textContent (XSS hardening)");

  // (b) innerHTML was NEVER assigned on the decode-output element.
  const htmlWrites = decodeOutput._audit.filter((e) => e.prop === "innerHTML");
  assert.equal(htmlWrites.length, 0,
    `decode output must never use innerHTML; saw: ${JSON.stringify(htmlWrites)}`);

  // (c) literal <script>...</script> substring is present as plain text.
  const finalText = textWrites.map((e) => e.value).join("\n");
  assert.ok(finalText.includes("<script>"),
    "literal <script> substring must survive as text, not be HTML-parsed");
  assert.ok(finalText.includes("</script>"),
    "literal </script> substring must survive as text");
});

// ---- 3. Unknown enum (safety_class / direction / frame_type) → "unknown" fallback ----

test("L7 command view renders unknown safety_class as 'unknown' fallback", async () => {
  const { source, sourcePath } = await loadShellSource();
  const cmdBody = makeAuditedElement();
  const elements = new Map([
    ["[data-role=\"l7-command-body\"]", cmdBody],
  ]);

  const cmdResponse = createDeferredResponse({
    meta: {
      contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 },
      consistency: { mode: "LIVE" },
      data_hash: "c".repeat(64),
    },
    data: {
      namespace: "ebus_standard",
      catalog_version: "v-test",
      command: {
        id: "cmd.future",
        name: "Future",
        safety_class: "brand_new_class_xyz",
        identity: {
          pb: 5, sb: 4,
          direction: "weird_new_direction",
          telegram_class: "weird_frame",
        },
        request: [],
        response: [],
      },
    },
    error: null,
  });

  const { shell } = buildSandbox({
    source,
    sourcePath,
    elements,
    fetchImpl: () => cmdResponse.promise,
  });

  assert.equal(typeof shell.refreshL7Command, "function",
    "PortalShell must expose refreshL7Command for the command detail view");

  const p = shell.refreshL7Command("cmd.future");
  cmdResponse.resolve();
  await p;

  const htmlWrites = cmdBody._audit.filter((e) => e.prop === "innerHTML");
  assert.ok(htmlWrites.length >= 1, "command body should render");
  const rendered = htmlWrites.map((e) => e.value).join("\n");

  // Unknown safety_class must render with "unknown" fallback label. The raw
  // open-enum value is passed through as evidence; the UI just labels it.
  assert.ok(rendered.toLowerCase().includes("unknown"),
    "unknown safety_class must render with 'unknown' label; got: " + rendered);
});

// ---- 4. render() emits the L7 Standard Catalog section markup ----
//
// Regression for the Codex P1 finding on PR #507: the four L7 methods
// existed but render() never emitted the data-role="l7-*" DOM, so the
// methods were dead code. This test runs render() for real and asserts
// the required selectors + controls are present.

test("L7 Standard Catalog section is emitted by render() with required data-role elements", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source,
    sourcePath,
    elements: new Map(),
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  // Re-attach the real render() — buildSandbox stubs it with a no-op so
  // the other tests could pre-install elements. Here we want the real
  // markup string assigned to innerHTML.
  const proto = Object.getPrototypeOf(shell);
  shell.render = proto.render;
  let capturedHTML = "";
  Object.defineProperty(shell, "innerHTML", {
    set(v) { capturedHTML = String(v); },
    get() { return capturedHTML; },
    configurable: true,
  });
  shell.render();

  // Section container + nav entry.
  assert.ok(capturedHTML.includes('id="section-l7-catalog"'),
    "render() must emit id=section-l7-catalog");
  assert.ok(capturedHTML.includes('data-role="nav-l7-catalog"'),
    "render() must emit nav-l7-catalog sidebar button");
  assert.ok(capturedHTML.includes('data-nav-target="section-l7-catalog"'),
    "nav button must target section-l7-catalog");

  // All four data-role selectors required by the existing methods.
  for (const role of [
    "l7-services-body",
    "l7-commands-body",
    "l7-command-body",
    "l7-decode-output",
    "l7-decode-status",
  ]) {
    assert.ok(capturedHTML.includes(`data-role="${role}"`),
      `render() must emit data-role="${role}"`);
  }

  // Interactive controls wired in bindL7CatalogEvents.
  for (const role of [
    "l7-refresh-services",
    "l7-refresh-commands",
    "l7-refresh-command",
    "l7-pb-filter",
    "l7-command-id",
    "l7-decode-pb",
    "l7-decode-sb",
    "l7-decode-direction",
    "l7-decode-frame-type",
    "l7-decode-payload",
    "l7-decode-submit",
  ]) {
    assert.ok(capturedHTML.includes(`data-role="${role}"`),
      `render() must emit data-role="${role}"`);
  }
});

// ---- 5. End-to-end: click-through decode flow preserves textContent XSS path ----
//
// Simulates the full operator flow: click the decode submit button, the
// event handler gathers inputs, calls submitL7Decode, fetch returns a
// payload with a literal <script> tag in decoded_repr, and the output
// element is populated via textContent only (no innerHTML sink).

test("L7 decode click-through flow renders output via textContent (XSS hardening preserved)", async () => {
  const { source, sourcePath } = await loadShellSource();
  const decodeOutput = makeAuditedElement();
  const decodeStatus = makeAuditedElement();

  // Input elements — auditable but backed by .value reads.
  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("5");
  const sbInput = makeInput("4");
  const dirInput = makeInput("master_to_slave");
  const frameInput = makeInput("MM");
  const payloadInput = makeInput("3c7363726970743e");

  // Capture the click listener registered by bindL7CatalogEvents.
  let submitClickHandler = null;
  const submitBtn = {
    isConnected: true,
    addEventListener(name, fn) {
      if (name === "click") submitClickHandler = fn;
    },
  };
  // Pass-through no-op for other buttons bindEvents queries.
  const noopListener = { addEventListener() {} };

  const elements = new Map([
    ['[data-role="l7-decode-output"]', decodeOutput],
    ['[data-role="l7-decode-status"]', decodeStatus],
    ['[data-role="l7-decode-pb"]', pbInput],
    ['[data-role="l7-decode-sb"]', sbInput],
    ['[data-role="l7-decode-direction"]', dirInput],
    ['[data-role="l7-decode-frame-type"]', frameInput],
    ['[data-role="l7-decode-payload"]', payloadInput],
    ['[data-role="l7-decode-submit"]', submitBtn],
    ['[data-role="l7-refresh-services"]', noopListener],
    ['[data-role="l7-refresh-commands"]', noopListener],
    ['[data-role="l7-refresh-command"]', noopListener],
    ['[data-role="l7-pb-filter"]', makeInput("")],
    ['[data-role="l7-command-id"]', makeInput("")],
  ]);

  const decodeResponse = createDeferredResponse({
    meta: { contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 }, consistency: { mode: "LIVE" }, data_hash: "d".repeat(64) },
    data: {
      namespace: "ebus_standard",
      catalog_version: "v-test",
      command_id: "cmd.decoded",
      raw_bytes: [0x3c, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x3e],
      decoded_repr: "<script>alert('xss')</script>",
      validity: "catalog_identified",
    },
    error: null,
  });

  const { shell, fetchRequests } = buildSandbox({
    source,
    sourcePath,
    elements,
    fetchImpl: () => decodeResponse.promise,
  });

  // Invoke the real bindL7CatalogEvents so the click listener registers.
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();

  assert.equal(typeof submitClickHandler, "function",
    "decode submit button must have a click listener registered");

  // Trigger the click. The handler kicks off submitL7Decode asynchronously.
  submitClickHandler();
  decodeResponse.resolve();
  await flush();
  // flush twice — the handler awaits fetch, then awaits json().
  await flush();
  await flush();

  // (a) fetch was issued with all five query params.
  assert.ok(fetchRequests.length >= 1, "fetch must be issued");
  const url = String(fetchRequests[0].url);
  assert.ok(url.includes("pb=5"), `fetch URL must include pb: ${url}`);
  assert.ok(url.includes("sb=4"), `fetch URL must include sb: ${url}`);
  assert.ok(url.includes("direction=master_to_slave"),
    `fetch URL must include direction: ${url}`);
  assert.ok(url.includes("frame_type=MM"), `fetch URL must include frame_type: ${url}`);
  assert.ok(url.includes("payload_hex=3c7363726970743e"),
    `fetch URL must include payload_hex: ${url}`);

  // (b) output element was populated via textContent ONLY. This is the
  //     load-bearing XSS hardening assertion that the new click-through
  //     wiring does not regress.
  const textWrites = decodeOutput._audit.filter((e) => e.prop === "textContent");
  const htmlWrites = decodeOutput._audit.filter((e) => e.prop === "innerHTML");
  assert.ok(textWrites.length >= 1,
    "decode output must be populated via textContent");
  assert.equal(htmlWrites.length, 0,
    `click-through decode must never use innerHTML; saw: ${JSON.stringify(htmlWrites)}`);

  // (c) literal <script> survives as text.
  const finalText = textWrites.map((e) => e.value).join("\n");
  assert.ok(finalText.includes("<script>"),
    "literal <script> must survive as plain text through click-through flow");
});
