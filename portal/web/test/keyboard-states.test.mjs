import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const sourceURL = new URL("../src/app.js", import.meta.url);
const source = await readFile(sourceURL, "utf8");
const digest = (value) => createHash("sha256").update(value).digest("hex");

function catalog(value = 12) {
  // The catalog wire contract encodes generation counters as decimal strings.
  const identity = { contribution_driver_id: "fixture.driver", contribution_manifest_id: "fixture.portal", contribution_manifest_version: "1.0.0" };
  return {
    contract: "helianthus.gateway.portal-catalog/v1",
    catalog_revision: digest(`revision-${value}`), catalog_digest: digest(`catalog-${value}`),
    evaluation_instant: "synthetic-evaluation", authorization_scope: "public-read",
    domains: ["evse", "infrastructure", "pv", "storage", "thermal"].map((name) => ({
      pack: { id: `helianthus.pack.${name}`, version: name === "storage" ? "1.1.0" : "1.0.0" },
      source_state: name === "pv" ? "PRESENT" : "ABSENT",
    })),
    contributions: [{ driver_id: "fixture.driver", manifest_id: "fixture.portal", manifest_version: "1.0.0", digest: `sha256:${digest("fixture")}` }],
    resources: ["primary", "secondary"].map((id) => ({
      ...identity, id, domain: "helianthus.pack.pv", service_id: "fixture.service", capability_id: "fixture.read", state: "CURRENT",
      source: { asset_id: id, snapshot_id: "synthetic-snapshot", evaluation_digest: "synthetic-evaluation", revision: "1", binding_id: "fixture-binding", source_epoch: "fixture-epoch", driver_generation: "1", snapshot: {}, evaluation: {}, selections: [], projection: {} },
    })),
    fields: [{ ...identity, id: "power", resource_id: "primary", definition_id: "fixture.power", unit_id: "unit.watt", value, quality: "GOOD", projection: "LOSSLESS" }],
    actions: [], quarantines: [],
  };
}

// Extend the existing VM/FakeNode seam with focus and removal semantics. Native
// button key activation and sequential Tab are modeled here, not browser-tested.
function harness() {
  const document = { activeElement: null, documentElement: { setAttribute() {} } };
  class FakeNode {
    constructor(tag = "div") {
      this.tagName = tag; this.children = []; this.attributes = {}; this.handlers = {};
      this.style = {}; this._text = ""; this.disabled = false; this.hidden = false;
      this.className = "";
      this.classList = { toggle: (name, on) => {
        const classes = new Set(this.className.split(/\s+/).filter(Boolean));
        if (on) classes.add(name); else classes.delete(name);
        this.className = [...classes].join(" ");
      } };
    }
    get id() { return this.getAttribute("id"); }
    get isConnected() { return this === shell || Boolean(this.parentNode?.isConnected); }
    get textContent() { return this._text + this.children.map((node) => node.textContent).join(" "); }
    set textContent(value) { this.replaceChildren(); this._text = String(value); }
    setAttribute(name, value) {
      this.attributes[name] = String(value);
      if (name === "disabled" || name === "hidden") this[name] = true;
      if (name === "class") this.className = String(value);
    }
    getAttribute(name) { return this.attributes[name] ?? null; }
    addEventListener(name, handler) { (this.handlers[name] ||= []).push(handler); }
    dispatch(name, event = {}) { for (const handler of this.handlers[name] || []) handler(event); }
    append(...nodes) { for (const node of nodes) { node.parentNode = this; this.children.push(node); } }
    contains(node) { return this === node || this.children.some((child) => child.contains(node)); }
    replaceChildren(...nodes) {
      if (this.children.some((child) => child.contains(document.activeElement))) document.activeElement = null;
      for (const child of this.children) child.parentNode = null;
      this.children = []; this._text = ""; this.append(...nodes);
    }
    focus() { if (this.isConnected && !this.disabled && visible(this)) document.activeElement = this; }
    querySelectorAll(selector) {
      return descendants(this).filter((node) => {
        if (selector === "main .registry-preview") return node.className.split(/\s+/).includes("registry-preview");
        const match = selector.match(/^\[([^=\]]+)(?:="([^"]*)")?\]$/);
        return match && node.getAttribute(match[1]) !== null && (match[2] === undefined || node.getAttribute(match[1]) === match[2]);
      });
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    set innerHTML(html) {
      this.replaceChildren();
      const stack = [this];
      for (const token of html.match(/<[^>]+>|[^<]+/g) || []) {
        if (token.startsWith("</")) { if (stack.length > 1) stack.pop(); continue; }
        if (!token.startsWith("<")) { stack.at(-1)._text += token; continue; }
        const tag = token.match(/^<([\w-]+)/)?.[1];
        if (!tag) continue;
        const node = new FakeNode(tag);
        for (const attr of token.slice(tag.length + 1, -1).matchAll(/([\w-]+)(?:="([^"]*)")?/g)) node.setAttribute(attr[1], attr[2] ?? "");
        stack.at(-1).append(node);
        if (!["input", "br", "hr", "img", "meta", "link"].includes(tag)) stack.push(node);
      }
    }
  }
  const descendants = (node) => node.children.flatMap((child) => [child, ...descendants(child)]);
  const visible = (node) => !node.hidden && node.style.display !== "none" && (!node.parentNode || visible(node.parentNode));
  document.createElement = (tag) => new FakeNode(tag);
  const queue = [], requests = [], pending = [];
  const sandbox = {
    document, HTMLElement: FakeNode, customElements: { define() {} }, console,
    localStorage: { getItem: () => null, setItem() {} }, AbortController, URLSearchParams, TextDecoder,
    setTimeout, clearTimeout, setInterval: () => ({}), clearInterval() {},
    fetch(url, init) {
      // Exercise the dedicated catalog request at the supplied prefixed endpoint.
      assert.equal(url, "/fixture/catalog");
      assert.equal(init.method, "POST");
      assert.deepEqual(JSON.parse(init.body), { operationName: "PortalCatalogV1", variables: {} });
      requests.push(url);
      assert.ok(queue.length, "unexpected synthetic fetch");
      return queue.shift();
    },
  };
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.PortalShellForTest = PortalShell;`, sandbox, { filename: sourceURL.href });
  const shell = new sandbox.PortalShellForTest();
  shell.render();
  shell.beginBootstrapLifecycle();
  shell._portalCatalogEndpoint = "/fixture/catalog";
  shell.setNavState("registry", true);
  shell.setNavState("portal-catalog", true);
  shell.setPortalCatalogRefreshState(true);
  // Preserve the real refresh implementation and retain its completion promise
  // for click handlers that intentionally discard their return value.
  const refresh = shell.refreshPortalCatalog.bind(shell);
  shell.refreshPortalCatalog = (...args) => { const result = refresh(...args); pending.push(result); return result; };
  shell.bindEvents();
  function key(keyName, shiftKey = false) {
    const target = document.activeElement;
    let prevented = false;
    const event = { key: keyName, shiftKey, target, preventDefault() { prevented = true; } };
    target?.dispatch("keydown", event);
    if (!prevented && keyName === "Tab") {
      const stops = descendants(shell).filter((node) => ["button", "input", "select"].includes(node.tagName) && !node.disabled && visible(node));
      const index = stops.indexOf(target);
      stops[(index + (shiftKey ? -1 : 1) + stops.length) % stops.length]?.focus();
    }
    if (!prevented && target?.tagName === "button" && !target.disabled && keyName === "Enter") target.dispatch("click", event);
    target?.dispatch("keyup", event);
    if (!prevented && target?.tagName === "button" && !target.disabled && keyName === " ") target.dispatch("click", event);
  }
  const get = (role) => shell.querySelector(`[data-role="${role}"]`);
  return {
    shell, document, key, get, requests,
    button: (label) => descendants(shell).find((node) => node.tagName === "button" && node.textContent.trim() === label),
    respond(payload = catalog(), ok = true) { queue.push(Promise.resolve({ ok, json: async () => ({ data: { portalCatalogV1: payload } }) })); },
    defer() { let resolve; queue.push(new Promise((done) => { resolve = done; })); return (payload = catalog()) => resolve({ ok: true, json: async () => ({ data: { portalCatalogV1: payload } }) }); },
    async settle() { await Promise.all(pending.splice(0)); },
  };
}

function tabToCatalogRefresh(h) {
  h.key("Tab");
  assert.equal(h.document.activeElement, h.get("nav-configuration"));
  h.key("Tab");
  assert.equal(h.document.activeElement, h.get("portal-catalog-refresh"));
}

function tabBackToCatalogNav(h, nav) {
  h.key("Tab", true);
  assert.equal(h.document.activeElement, h.get("nav-configuration"));
  h.key("Tab", true);
  assert.equal(h.document.activeElement, nav);
}

function assertLive(node) {
  assert.equal(node.getAttribute("role"), "status");
  assert.equal(node.getAttribute("aria-live"), "polite");
  assert.equal(node.getAttribute("aria-atomic"), "true");
}

test("keyboard Enter/Space select shell views and Tab can leave the selection", async () => {
  const h = await harness();
  const registry = h.get("nav-registry"), catalogNav = h.get("nav-portal-catalog");
  registry.focus(); h.key("Enter");
  assert.equal(h.shell.querySelectorAll("main .registry-preview").find((node) => node.id === "section-registry").style.display, "");
  h.key("Tab"); assert.equal(h.document.activeElement, catalogNav);
  h.respond(); h.key(" "); await h.settle();
  assert.match(catalogNav.className, /active/);
  assert.doesNotMatch(registry.className, /active/);
  assert.equal(h.shell.querySelectorAll("main .registry-preview").find((node) => node.id === "section-registry").style.display, "none");
  assert.equal(h.document.activeElement, catalogNav);
  tabToCatalogRefresh(h);
  tabBackToCatalogNav(h, catalogNav);
});

test("selected shell view is announced to assistive technology", async () => {
  const h = await harness();
  const nav = h.get("nav-registry"); nav.focus(); h.key("Enter");
  assert.equal(nav.getAttribute("aria-current"), "page", "visual active state needs an accessible current-view equivalent");
  const next = h.get("nav-portal-catalog");
  h.respond(); next.focus(); h.key("Enter"); await h.settle();
  assert.equal(next.getAttribute("aria-current"), "page");
  assert.equal(nav.getAttribute("aria-current"), "false");
});

test("successful catalog render has live semantics and preserves focus on Refresh", async () => {
  const h = await harness();
  assertLive(h.get("portal-catalog-body"));
  const nav = h.get("nav-portal-catalog");
  h.respond(); nav.focus(); h.key("Enter"); await h.settle();
  h.respond(catalog(24));
  const button = h.get("portal-catalog-refresh"); button.focus(); h.key("Enter"); await h.settle();
  assert.match(h.get("portal-catalog-body").textContent, /24/);
  assert.equal(h.get("portal-catalog-notice").hidden, true);
  assert.equal(h.document.activeElement, button);
  tabBackToCatalogNav(h, nav);
});

test("loading announces busy state until the deferred fetch completes", async () => {
  const h = await harness();
  const complete = h.defer();
  const request = h.shell.refreshPortalCatalog();
  const body = h.get("portal-catalog-body");
  try {
    assertLive(body);
    assert.equal(body.getAttribute("aria-busy"), "true", "pending catalog request must expose busy state");
  } finally { complete(); await request; }
  assert.notEqual(body.getAttribute("aria-busy"), "true");
});

test("an older request cannot clear the busy state of a newer pending refresh", async () => {
  const h = harness();
  const completeOlder = h.defer();
  const older = h.shell.refreshPortalCatalog();
  const completeNewer = h.defer();
  const newer = h.shell.refreshPortalCatalog();
  try {
    completeOlder(); await older;
    assert.equal(h.get("portal-catalog-body").getAttribute("aria-busy"), "true");
  } finally { completeNewer(); await newer; }
  assert.equal(h.get("portal-catalog-body").getAttribute("aria-busy"), "false");
});

test("failed fetch announces an error and keeps keyboard retry and escape usable", async () => {
  const h = await harness();
  h.respond(null, false);
  const nav = h.get("nav-portal-catalog"); nav.focus(); h.key("Enter"); await h.settle();
  const notice = h.get("portal-catalog-notice");
  assertLive(notice); assert.equal(notice.hidden, false); assert.match(notice.textContent, /failed/i);
  assert.equal(h.get("portal-catalog-body").getAttribute("aria-busy"), "false");
  assert.equal(h.document.activeElement, nav);
  tabToCatalogRefresh(h); const retry = h.get("portal-catalog-refresh");
  assert.equal(h.document.activeElement, retry); assert.equal(retry.disabled, false);
  h.respond(); h.key(" "); await h.settle();
  assert.equal(notice.hidden, true); assert.equal(h.document.activeElement, retry);
  tabBackToCatalogNav(h, nav);
});

test("updating a contribution leaves unrelated search focus and navigation usable", async () => {
  const h = await harness();
  h.respond(); assert.equal(await h.shell.refreshPortalCatalog(), true);
  const search = h.get("search-input"); search.focus();
  h.respond(catalog(24)); assert.equal(await h.shell.refreshPortalCatalog(), true);
  assert.equal(h.document.activeElement, search);
  h.key("Tab"); assert.equal(h.document.activeElement, h.get("theme-toggle"));
});

test("updating a contribution preserves a usable focused catalog control", async () => {
  const h = await harness();
  h.respond(); const nav = h.get("nav-portal-catalog"); nav.focus(); h.key("Enter"); await h.settle();
  tabToCatalogRefresh(h); h.key("Tab");
  assert.equal(h.document.activeElement, h.button("Next Resource"));
  h.respond(catalog(24)); assert.equal(await h.shell.refreshPortalCatalog(), true);
  assert.ok(h.document.activeElement?.isConnected, "refresh detached the focused catalog control without restoring focus");
  assert.equal(h.document.activeElement, h.button("Next Resource"));
  h.key("Tab", true);
  assert.equal(h.document.activeElement, h.get("portal-catalog-refresh"));
});

test("paging to a disabled replacement returns keyboard focus to Refresh", async () => {
  const h = harness();
  h.respond(); const nav = h.get("nav-portal-catalog"); nav.focus(); h.key("Enter"); await h.settle();
  tabToCatalogRefresh(h); h.key("Tab"); h.key("Enter");
  assert.equal(h.button("Next Resource").disabled, true);
  assert.equal(h.document.activeElement, h.get("portal-catalog-refresh"));
  h.key("Tab"); assert.equal(h.document.activeElement, h.button("Previous Resource"));
});

test("refresh with no remaining catalog controls returns keyboard focus to Refresh", async () => {
  const h = harness();
  h.respond(); const nav = h.get("nav-portal-catalog"); nav.focus(); h.key("Enter"); await h.settle();
  tabToCatalogRefresh(h); h.key("Tab");
  const empty = catalog(); empty.resources = []; empty.fields = [];
  h.respond(empty); assert.equal(await h.shell.refreshPortalCatalog(), true);
  assert.equal(h.button("Next Resource"), undefined);
  assert.equal(h.document.activeElement, h.get("portal-catalog-refresh"));
});


test("failed contribution update retains last valid content and focused evidence control", async () => {
  const h = await harness();
  h.respond(); const nav = h.get("nav-portal-catalog"); nav.focus(); h.key("Enter"); await h.settle();
  tabToCatalogRefresh(h); h.key("Tab");
  const focused = h.button("Next Resource");
  assert.equal(h.document.activeElement, focused);
  const body = h.get("portal-catalog-body"), previous = body.children[0];
  h.respond(null, false); await h.shell.refreshPortalCatalog();
  assert.equal(body.children[0], previous);
  assertLive(h.get("portal-catalog-notice"));
  assert.match(h.get("portal-catalog-notice").textContent, /failed.*last valid catalog snapshot/i);
  assert.equal(h.document.activeElement, focused);
  h.key("Tab", true); assert.equal(h.document.activeElement, h.get("portal-catalog-refresh"));
});
