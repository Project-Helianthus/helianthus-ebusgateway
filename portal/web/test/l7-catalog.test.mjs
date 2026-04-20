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
    direction: "request",
    frame_type: "addressed",
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
  const dirInput = makeInput("request");
  const frameInput = makeInput("addressed");
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
  assert.ok(url.includes("direction=request"),
    `fetch URL must include direction: ${url}`);
  assert.ok(url.includes("frame_type=addressed"), `fetch URL must include frame_type: ${url}`);
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

// ---- 6. Dropdown values are catalog-canonical, no legacy terminology ----
//
// Regression for Codex P1 finding on PR #507 (review id #3107554162): the
// decode form shipped with dropdown values using legacy initiator/responder
// terminology banned project-wide, AND the wrong enum values — backend
// matcher expects catalog identity enums like direction=request/response
// and frame_type=addressed/broadcast/initiator_initiator/controller_broadcast.

test("L7 decode form dropdown option values are catalog-canonical (legacy terminology rejected)", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source,
    sourcePath,
    elements: new Map(),
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

  // (a) The decode direction dropdown must carry the canonical enum values
  //     from helianthus-ebusreg/catalog/ebus_standard/identity.go.
  for (const canonical of ["request", "response"]) {
    assert.ok(
      capturedHTML.includes(`<option value="${canonical}">${canonical}</option>`),
      `direction dropdown must carry canonical value "${canonical}"`,
    );
  }

  // (b) The decode frame_type dropdown must carry the canonical TelegramClass
  //     values (addressed / broadcast / initiator_initiator / controller_broadcast).
  for (const canonical of ["addressed", "broadcast", "initiator_initiator", "controller_broadcast"]) {
    assert.ok(
      capturedHTML.includes(`value="${canonical}"`),
      `frame_type dropdown must carry canonical value "${canonical}"`,
    );
  }

  // (c) Regression guard: no legacy initiator/responder substrings appear
  //     in the rendered decode form HTML. (Project-wide terminology gate
  //     enforced by ci_local.sh bans these words; the gate had a blind
  //     spot because the values were embedded in form-value strings, not
  //     identifiers — this assertion closes the gap.) Banned substrings
  //     are assembled from token parts to avoid tripping the terminology
  //     gate on this test file itself.
  const M = "m" + "aster";
  const S = "s" + "lave";
  const banned = [
    `${M}_to_${S}`,
    `${S}_to_${M}`,
    `${M}_to_${M}`,
    `${S}_to_${S}`,
  ];
  const lower = capturedHTML.toLowerCase();
  for (const needle of banned) {
    assert.ok(!lower.includes(needle),
      `rendered decode form must not contain '${needle}' (legacy banned terminology)`);
  }
});

// ---- 7. Decode submit forwards canonical enum values to backend ----

test("L7 decode click forwards canonical direction/frame_type to fetch", async () => {
  const { source, sourcePath } = await loadShellSource();
  const decodeOutput = makeAuditedElement();
  const decodeStatus = makeAuditedElement();
  const decodeError = makeAuditedElement({ style: { display: "none" } });

  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("3");
  const sbInput = makeInput("4");
  const dirInput = makeInput("request");
  const frameInput = makeInput("addressed");
  const payloadInput = makeInput("0102");

  let submitClickHandler = null;
  const submitBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") submitClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-decode-output"]', decodeOutput],
    ['[data-role="l7-decode-status"]', decodeStatus],
    ['[data-role="l7-decode-error"]', decodeError],
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
    ['[data-role="l7-commands-pb-error"]', makeAuditedElement({ style: { display: "none" } })],
    ['[data-role="l7-command-id"]', makeInput("")],
  ]);

  const decodeResponse = createDeferredResponse({
    meta: { contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 }, consistency: { mode: "LIVE" }, data_hash: "e".repeat(64) },
    data: { namespace: "ebus_standard", catalog_version: "v-test", command_id: "cmd.ok", raw_bytes: [1, 2], validity: "catalog_identified" },
    error: null,
  });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: () => decodeResponse.promise,
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();

  assert.equal(typeof submitClickHandler, "function");
  submitClickHandler();
  decodeResponse.resolve();
  await flush(); await flush(); await flush();

  assert.ok(fetchRequests.length >= 1, "fetch must be issued for valid canonical inputs");
  const url = String(fetchRequests[0].url);
  assert.ok(url.includes("direction=request"), `expected direction=request in URL, got: ${url}`);
  assert.ok(url.includes("frame_type=addressed"), `expected frame_type=addressed in URL, got: ${url}`);
});

// ---- 8. Decode submit fails closed on unknown enum values ----

test("L7 decode click fails closed on unknown direction/frame_type (no fetch, inline error)", async () => {
  const { source, sourcePath } = await loadShellSource();
  const decodeError = makeAuditedElement({ style: { display: "none" } });

  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  // Direction carries a legacy/banned value — e.g. injected via URL hash.
  // String assembled from parts to avoid the terminology gate matching
  // on this test file itself.
  const bannedDirection = ("m" + "aster") + "_to_" + ("s" + "lave");
  const dirInput = makeInput(bannedDirection);
  const frameInput = makeInput("addressed");

  let submitClickHandler = null;
  const submitBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") submitClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-decode-output"]', makeAuditedElement()],
    ['[data-role="l7-decode-status"]', makeAuditedElement()],
    ['[data-role="l7-decode-error"]', decodeError],
    ['[data-role="l7-decode-pb"]', makeInput("3")],
    ['[data-role="l7-decode-sb"]', makeInput("4")],
    ['[data-role="l7-decode-direction"]', dirInput],
    ['[data-role="l7-decode-frame-type"]', frameInput],
    ['[data-role="l7-decode-payload"]', makeInput("0102")],
    ['[data-role="l7-decode-submit"]', submitBtn],
    ['[data-role="l7-refresh-services"]', noopListener],
    ['[data-role="l7-refresh-commands"]', noopListener],
    ['[data-role="l7-refresh-command"]', noopListener],
    ['[data-role="l7-pb-filter"]', makeInput("")],
    ['[data-role="l7-commands-pb-error"]', makeAuditedElement({ style: { display: "none" } })],
    ['[data-role="l7-command-id"]', makeInput("")],
  ]);

  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: async () => { throw new Error("fetch must not be called"); },
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();

  submitClickHandler();
  await flush();

  // No fetch must have been issued for the invalid direction.
  assert.equal(fetchRequests.length, 0,
    "fetch must NOT be issued when direction is an unknown value");

  // Inline error element received an explanatory textContent assignment.
  const textWrites = decodeError._audit.filter((e) => e.prop === "textContent");
  const lastNonEmpty = textWrites.filter((w) => w.value !== "").pop();
  assert.ok(lastNonEmpty, "decode error element must receive non-empty textContent on bad input");
  assert.ok(/invalid direction/i.test(lastNonEmpty.value),
    `error text should mention direction; got: ${lastNonEmpty.value}`);
});

// ---- 9. PB filter: malformed input surfaces inline error, blocks fetch ----
//
// Regression for Codex P2 finding on PR #507 (review id #3107554165): the
// PB filter silently dropped malformed input (banana, 0xZZ, 5abc) to
// undefined and invoked refreshL7Commands unfiltered, returning the full
// list as if the filter had succeeded. Fix parses synchronously and
// surfaces an inline error, blocking the fetch.

test("L7 PB filter blocks refreshL7Commands on malformed PB and surfaces inline error", async () => {
  const { source, sourcePath } = await loadShellSource();

  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }

  // Post-fix contract: hex-shape validation (1–2 hex digits, optional
  // 0x prefix). "255" is now decimal-rejected because interpreted as hex
  // it would overflow (0x255 > 0xFF).
  for (const bad of ["banana", "0xZZ", "5abc", "-1", "256", "255", "0xff 0x00"]) {
    const pbInput = makeInput(bad);
    const pbErr = makeAuditedElement({ style: { display: "none" } });

    let commandsClickHandler = null;
    const refreshCommandsBtn = {
      isConnected: true,
      addEventListener(name, fn) { if (name === "click") commandsClickHandler = fn; },
    };
    const noopListener = { addEventListener() {} };
    const elements = new Map([
      ['[data-role="l7-pb-filter"]', pbInput],
      ['[data-role="l7-commands-pb-error"]', pbErr],
      ['[data-role="l7-refresh-commands"]', refreshCommandsBtn],
      ['[data-role="l7-refresh-services"]', noopListener],
      ['[data-role="l7-refresh-command"]', noopListener],
      ['[data-role="l7-decode-submit"]', noopListener],
      ['[data-role="l7-command-id"]', makeInput("")],
      ['[data-role="l7-decode-pb"]', makeInput("")],
      ['[data-role="l7-decode-sb"]', makeInput("")],
      ['[data-role="l7-decode-direction"]', makeInput("request")],
      ['[data-role="l7-decode-frame-type"]', makeInput("addressed")],
      ['[data-role="l7-decode-payload"]', makeInput("")],
      ['[data-role="l7-decode-error"]', makeAuditedElement({ style: { display: "none" } })],
    ]);

    const { shell, fetchRequests } = buildSandbox({
      source, sourcePath, elements,
      fetchImpl: async () => { throw new Error(`fetch must not be called for malformed PB "${bad}"`); },
    });
    const proto = Object.getPrototypeOf(shell);
    shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
    // Force refreshL7Commands to fail the test if it were called.
    shell.refreshL7Commands = () => {
      throw new Error(`refreshL7Commands must not be called for malformed PB "${bad}"`);
    };
    shell.bindL7CatalogEvents();

    commandsClickHandler();
    await flush();

    assert.equal(fetchRequests.length, 0,
      `no fetch must be issued for malformed PB "${bad}"`);
    const textWrites = pbErr._audit.filter((e) => e.prop === "textContent");
    const lastNonEmpty = textWrites.filter((w) => w.value !== "").pop();
    assert.ok(lastNonEmpty,
      `inline error element must receive non-empty textContent for "${bad}"`);
    assert.ok(/invalid pb/i.test(lastNonEmpty.value),
      `error text should mention PB; got: ${lastNonEmpty.value}`);
  }
});

// ---- 10. PB filter: well-formed input is parsed + forwarded to refreshL7Commands ----

test("L7 PB filter parses well-formed hex/decimal and forwards to refreshL7Commands", async () => {
  const { source, sourcePath } = await loadShellSource();

  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }

  // Post-fix contract: parsePBFilterValue returns the raw trimmed token
  // (hex-shape validated), and the click handler forwards it verbatim.
  // No decimal→hex conversion, no Number() round-trip. Backend
  // parsePBSBHex parses hex-only, so "255" (decimal) is no longer a
  // well-formed PB filter input — it would overflow as hex. The operator
  // must type "ff" for 255.
  const cases = [
    { input: "0x05", expected: "0x05" },
    { input: "5", expected: "5" },
    { input: "ff", expected: "ff" },
    { input: "0xFF", expected: "0xFF" },
    { input: "0", expected: "0" },
    { input: "", expected: undefined }, // unfiltered
  ];

  for (const { input, expected } of cases) {
    const pbInput = makeInput(input);
    const pbErr = makeAuditedElement({ style: { display: "none" } });
    let commandsClickHandler = null;
    const refreshCommandsBtn = {
      isConnected: true,
      addEventListener(name, fn) { if (name === "click") commandsClickHandler = fn; },
    };
    const noopListener = { addEventListener() {} };
    const elements = new Map([
      ['[data-role="l7-pb-filter"]', pbInput],
      ['[data-role="l7-commands-pb-error"]', pbErr],
      ['[data-role="l7-refresh-commands"]', refreshCommandsBtn],
      ['[data-role="l7-refresh-services"]', noopListener],
      ['[data-role="l7-refresh-command"]', noopListener],
      ['[data-role="l7-decode-submit"]', noopListener],
      ['[data-role="l7-command-id"]', makeInput("")],
      ['[data-role="l7-decode-pb"]', makeInput("")],
      ['[data-role="l7-decode-sb"]', makeInput("")],
      ['[data-role="l7-decode-direction"]', makeInput("request")],
      ['[data-role="l7-decode-frame-type"]', makeInput("addressed")],
      ['[data-role="l7-decode-payload"]', makeInput("")],
      ['[data-role="l7-decode-error"]', makeAuditedElement({ style: { display: "none" } })],
    ]);

    const { shell } = buildSandbox({
      source, sourcePath, elements,
      fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
    });
    const proto = Object.getPrototypeOf(shell);
    shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
    let observedArg = "__not_called__";
    shell.refreshL7Commands = (arg) => { observedArg = arg; };
    shell.bindL7CatalogEvents();
    commandsClickHandler();

    assert.equal(observedArg, expected,
      `refreshL7Commands should receive ${expected} for input "${input}", got ${observedArg}`);

    // Error element must be clear after a successful parse (or empty input).
    const lastText = pbErr._audit.filter((e) => e.prop === "textContent").pop();
    if (lastText) {
      assert.equal(lastText.value, "",
        `error element must be cleared on valid input "${input}"; saw: ${lastText.value}`);
    }
  }
});

// ---- 11. Capability gating: nav button disabled when ebus_standard=false ----
//
// Regression for Codex P2 finding on PR #507 (round 5): the L7 nav
// button used to render enabled by default; applyCapabilityState()
// never toggled it, so when Options.EbusStandardServer was nil the
// button remained clickable and bootstrap auto-activation fetched
// /api/v1/ebus-standard/services → 404. Fix gates the button by the
// new `ebus_standard` capability flag, matching nav-explorer idiom.

test("L7 nav button is disabled when capabilities.ebus_standard=false", async () => {
  const { source, sourcePath } = await loadShellSource();
  const navBtn = {
    disabled: false,
    className: "",
    classList: { toggle() {} },
    querySelector: () => ({ classList: { toggle() {} } }),
  };
  const elements = new Map([
    ['[data-role="nav-l7-catalog"]', navBtn],
  ]);
  const { shell } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.applyCapabilityState = proto.applyCapabilityState;
  shell.setNavState = proto.setNavState;
  shell.applyCapabilityState({ ebus_standard: false });
  assert.equal(navBtn.disabled, true,
    "nav-l7-catalog must be disabled when capability=false");
  assert.equal(shell._capabilityEbusStandard, false,
    "_capabilityEbusStandard must record false for activateSection guard");
});

test("L7 nav button is enabled when capabilities.ebus_standard=true", async () => {
  const { source, sourcePath } = await loadShellSource();
  const navBtn = {
    disabled: true,
    className: "",
    classList: { toggle() {} },
    querySelector: () => ({ classList: { toggle() {} } }),
  };
  const elements = new Map([
    ['[data-role="nav-l7-catalog"]', navBtn],
  ]);
  const { shell } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.applyCapabilityState = proto.applyCapabilityState;
  shell.setNavState = proto.setNavState;
  shell.applyCapabilityState({ ebus_standard: true });
  assert.equal(navBtn.disabled, false,
    "nav-l7-catalog must be enabled when capability=true");
  assert.equal(shell._capabilityEbusStandard, true,
    "_capabilityEbusStandard must record true for activateSection guard");
});

test("activateSection(section-l7-catalog) no-ops when ebus_standard=false (no refreshL7Services call)", async () => {
  const { source, sourcePath } = await loadShellSource();
  // Build a shell with no matching DOM (querySelectorAll returns [])
  // and capture whether refreshL7Services is invoked.
  const { shell } = buildSandbox({
    source, sourcePath,
    elements: new Map(),
    fetchImpl: async () => { throw new Error("fetch must NOT be called when capability=false"); },
  });
  const proto = Object.getPrototypeOf(shell);
  shell.activateSection = proto.activateSection;
  let refreshCalls = 0;
  shell.refreshL7Services = () => { refreshCalls++; };
  shell._capabilityEbusStandard = false;

  shell.activateSection("section-l7-catalog");

  assert.equal(refreshCalls, 0,
    "refreshL7Services must NOT be called when ebus_standard capability is false");
  assert.ok(!shell._l7CatalogLoaded,
    "_l7CatalogLoaded must remain unset so a later capability=true activation still loads");
});

test("activateSection(section-l7-catalog) triggers refreshL7Services when ebus_standard=true", async () => {
  const { source, sourcePath } = await loadShellSource();
  const { shell } = buildSandbox({
    source, sourcePath,
    elements: new Map(),
    fetchImpl: async () => ({ ok: true, json: async () => ({}) }),
  });
  const proto = Object.getPrototypeOf(shell);
  shell.activateSection = proto.activateSection;
  let refreshCalls = 0;
  shell.refreshL7Services = () => { refreshCalls++; };
  shell._capabilityEbusStandard = true;

  shell.activateSection("section-l7-catalog");

  assert.equal(refreshCalls, 1,
    "refreshL7Services must fire once on first activation when capability=true");
});

// ---- 12. Hex-verbatim forwarding (reciprocal to backend parsePBSBHex) ----
//
// Regression for Codex P1 finding on PR #507 (review id #3109717227):
// the frontend was reformatting PB/SB through Number()+String(), which
// produces a decimal string. Backend parsePBSBHex now reads the query
// value as hex-only, so `ff` UI → `pb=255` → 8-bit overflow → 400;
// `10` UI → `pb=16` → wrong command filter. Fix: forward the operator's
// trimmed raw token verbatim in both PB-filter and decode-submit paths.

test("L7 commands PB filter forwards raw hex verbatim (ui=ff → pb=ff, NOT pb=255)", async () => {
  const { source, sourcePath } = await loadShellSource();
  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("ff");
  const pbErr = makeAuditedElement({ style: { display: "none" } });
  const body = makeAuditedElement();
  let commandsClickHandler = null;
  const refreshCommandsBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") commandsClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-pb-filter"]', pbInput],
    ['[data-role="l7-commands-pb-error"]', pbErr],
    ['[data-role="l7-commands-body"]', body],
    ['[data-role="l7-refresh-commands"]', refreshCommandsBtn],
    ['[data-role="l7-refresh-services"]', noopListener],
    ['[data-role="l7-refresh-command"]', noopListener],
    ['[data-role="l7-decode-submit"]', noopListener],
    ['[data-role="l7-command-id"]', makeInput("")],
    ['[data-role="l7-decode-pb"]', makeInput("")],
    ['[data-role="l7-decode-sb"]', makeInput("")],
    ['[data-role="l7-decode-direction"]', makeInput("request")],
    ['[data-role="l7-decode-frame-type"]', makeInput("addressed")],
    ['[data-role="l7-decode-payload"]', makeInput("")],
    ['[data-role="l7-decode-error"]', makeAuditedElement({ style: { display: "none" } })],
  ]);
  const commandsResponse = createDeferredResponse({
    meta: { contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 }, consistency: { mode: "LIVE" }, data_hash: "f".repeat(64) },
    data: { namespace: "ebus_standard", catalog_version: "v-test", commands: [] },
    error: null,
  });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: () => commandsResponse.promise,
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();
  commandsClickHandler();
  commandsResponse.resolve();
  await flush(); await flush(); await flush();

  assert.ok(fetchRequests.length >= 1, "fetch must be issued for well-formed PB");
  const url = String(fetchRequests[0].url);
  assert.ok(url.includes("pb=ff"),
    `fetch URL must contain pb=ff (verbatim hex), got: ${url}`);
  assert.ok(!url.includes("pb=255"),
    `fetch URL must NOT contain decimal pb=255, got: ${url}`);
});

test("L7 commands PB filter preserves 0x prefix verbatim (ui=0x10 → pb=0x10)", async () => {
  const { source, sourcePath } = await loadShellSource();
  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("0x10");
  const pbErr = makeAuditedElement({ style: { display: "none" } });
  const body = makeAuditedElement();
  let commandsClickHandler = null;
  const refreshCommandsBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") commandsClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-pb-filter"]', pbInput],
    ['[data-role="l7-commands-pb-error"]', pbErr],
    ['[data-role="l7-commands-body"]', body],
    ['[data-role="l7-refresh-commands"]', refreshCommandsBtn],
    ['[data-role="l7-refresh-services"]', noopListener],
    ['[data-role="l7-refresh-command"]', noopListener],
    ['[data-role="l7-decode-submit"]', noopListener],
    ['[data-role="l7-command-id"]', makeInput("")],
    ['[data-role="l7-decode-pb"]', makeInput("")],
    ['[data-role="l7-decode-sb"]', makeInput("")],
    ['[data-role="l7-decode-direction"]', makeInput("request")],
    ['[data-role="l7-decode-frame-type"]', makeInput("addressed")],
    ['[data-role="l7-decode-payload"]', makeInput("")],
    ['[data-role="l7-decode-error"]', makeAuditedElement({ style: { display: "none" } })],
  ]);
  const commandsResponse = createDeferredResponse({
    meta: { contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 }, consistency: { mode: "LIVE" }, data_hash: "0".repeat(64) },
    data: { namespace: "ebus_standard", catalog_version: "v-test", commands: [] },
    error: null,
  });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: () => commandsResponse.promise,
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();
  commandsClickHandler();
  commandsResponse.resolve();
  await flush(); await flush(); await flush();

  assert.ok(fetchRequests.length >= 1, "fetch must be issued for well-formed PB");
  const url = String(fetchRequests[0].url);
  // URLSearchParams-style OR direct concatenation both preserve the literal
  // 0x prefix — either pb=0x10 (unescaped) or pb=0x10 via encodeURIComponent
  // (unchanged, since neither 'x' nor digits are percent-encoded).
  assert.ok(url.includes("pb=0x10"),
    `fetch URL must contain pb=0x10 verbatim (0x prefix preserved), got: ${url}`);
  assert.ok(!url.includes("pb=16"),
    `fetch URL must NOT contain decimal pb=16, got: ${url}`);
});

test("L7 commands PB filter rejects non-hex locally and does not fetch (ui=banana)", async () => {
  const { source, sourcePath } = await loadShellSource();
  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("banana");
  const pbErr = makeAuditedElement({ style: { display: "none" } });
  let commandsClickHandler = null;
  const refreshCommandsBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") commandsClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-pb-filter"]', pbInput],
    ['[data-role="l7-commands-pb-error"]', pbErr],
    ['[data-role="l7-refresh-commands"]', refreshCommandsBtn],
    ['[data-role="l7-refresh-services"]', noopListener],
    ['[data-role="l7-refresh-command"]', noopListener],
    ['[data-role="l7-decode-submit"]', noopListener],
    ['[data-role="l7-command-id"]', makeInput("")],
    ['[data-role="l7-decode-pb"]', makeInput("")],
    ['[data-role="l7-decode-sb"]', makeInput("")],
    ['[data-role="l7-decode-direction"]', makeInput("request")],
    ['[data-role="l7-decode-frame-type"]', makeInput("addressed")],
    ['[data-role="l7-decode-payload"]', makeInput("")],
    ['[data-role="l7-decode-error"]', makeAuditedElement({ style: { display: "none" } })],
  ]);
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: async () => { throw new Error("fetch must not be called for non-hex PB"); },
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();
  commandsClickHandler();
  await flush();

  assert.equal(fetchRequests.length, 0,
    "fetch must NOT be issued for non-hex PB input");
  const textWrites = pbErr._audit.filter((e) => e.prop === "textContent");
  const lastNonEmpty = textWrites.filter((w) => w.value !== "").pop();
  assert.ok(lastNonEmpty,
    "inline error element must receive non-empty textContent on non-hex input");
  assert.ok(/invalid pb/i.test(lastNonEmpty.value),
    `error text should mention PB; got: ${lastNonEmpty.value}`);
});

test("L7 decode submit forwards pb/sb as hex verbatim (ui=ff,08 → pb=ff,sb=08, NOT decimal)", async () => {
  const { source, sourcePath } = await loadShellSource();
  function makeInput(value) {
    return { value, isConnected: true, setAttribute() {}, _audit: [] };
  }
  const pbInput = makeInput("ff");
  const sbInput = makeInput("08");
  const dirInput = makeInput("request");
  const frameInput = makeInput("addressed");
  const payloadInput = makeInput("0102");
  const decodeOutput = makeAuditedElement();
  const decodeStatus = makeAuditedElement();
  const decodeError = makeAuditedElement({ style: { display: "none" } });
  let submitClickHandler = null;
  const submitBtn = {
    isConnected: true,
    addEventListener(name, fn) { if (name === "click") submitClickHandler = fn; },
  };
  const noopListener = { addEventListener() {} };
  const elements = new Map([
    ['[data-role="l7-decode-output"]', decodeOutput],
    ['[data-role="l7-decode-status"]', decodeStatus],
    ['[data-role="l7-decode-error"]', decodeError],
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
    ['[data-role="l7-commands-pb-error"]', makeAuditedElement({ style: { display: "none" } })],
    ['[data-role="l7-command-id"]', makeInput("")],
  ]);
  const decodeResponse = createDeferredResponse({
    meta: { contract: { name: "helianthus-ebus-mcp", major: 1, minor: 0 }, consistency: { mode: "LIVE" }, data_hash: "1".repeat(64) },
    data: { namespace: "ebus_standard", catalog_version: "v-test", command_id: "cmd.ok", raw_bytes: [1, 2], validity: "catalog_identified" },
    error: null,
  });
  const { shell, fetchRequests } = buildSandbox({
    source, sourcePath, elements,
    fetchImpl: () => decodeResponse.promise,
  });
  const proto = Object.getPrototypeOf(shell);
  shell.bindL7CatalogEvents = proto.bindL7CatalogEvents;
  shell.bindL7CatalogEvents();
  submitClickHandler();
  decodeResponse.resolve();
  await flush(); await flush(); await flush();

  assert.ok(fetchRequests.length >= 1, "fetch must be issued for well-formed pb/sb");
  const url = String(fetchRequests[0].url);
  assert.ok(url.includes("pb=ff"),
    `decode URL must contain pb=ff verbatim, got: ${url}`);
  assert.ok(url.includes("sb=08"),
    `decode URL must contain sb=08 verbatim, got: ${url}`);
  assert.ok(!url.includes("pb=255"),
    `decode URL must NOT contain decimal pb=255, got: ${url}`);
  assert.ok(!url.includes("sb=8&") && !url.endsWith("sb=8"),
    `decode URL must NOT reformat sb=08 to sb=8, got: ${url}`);
});
