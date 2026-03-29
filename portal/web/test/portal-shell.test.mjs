import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

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

function createPortalShellHarness({ source, sourcePath, elements, fetchImpl }) {
  const intervalCalls = [];
  const clearedIntervals = [];
  const fetchRequests = [];
  const fetchSignals = [];

  class FakeHTMLElement {
    constructor() {
      this._isConnected = true;
    }

    get isConnected() {
      return this._isConnected !== false;
    }
  }

  const sandbox = {
    console: {
      error() {},
      log() {},
      warn() {},
    },
    document: {
      documentElement: {
        setAttribute() {},
      },
    },
    customElements: {
      define() {},
    },
    HTMLElement: FakeHTMLElement,
    localStorage: {
      getItem() {
        return null;
      },
      setItem() {},
    },
    setInterval(callback, delay) {
      const handle = { callback, delay, id: intervalCalls.length + 1 };
      intervalCalls.push(handle);
      return handle;
    },
    clearInterval(handle) {
      clearedIntervals.push(handle);
    },
    setTimeout,
    clearTimeout,
    AbortController,
    URLSearchParams,
    TextDecoder,
  };

  sandbox.fetch = (url, requestInit) => {
    fetchRequests.push(url);
    fetchSignals.push(requestInit?.signal ?? null);
    return fetchImpl(url, requestInit);
  };
  sandbox.globalThis = sandbox;

  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, {
    filename: pathToFileURL(sourcePath).href,
  });

  const PortalShell = sandbox.__PortalShell;
  const shell = new PortalShell();
  shell._isConnected = true;
  shell.render = () => {};
  shell.bindEvents = () => {};
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];

  return {
    shell,
    intervalCalls,
    clearedIntervals,
    fetchRequests,
    fetchSignals,
  };
}

test("PortalShell ignores stale bootstrap completions when arming bus observability polling", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

  const firstHealth = createDeferredResponse({ status: "ok" });
  const firstBootstrap = createDeferredResponse({
    capabilities: { bus_observability: true },
    endpoints: { graphql: "/graphql" },
  });
  const firstBusObservability = createDeferredResponse({ status: {} });
  const secondHealth = createDeferredResponse({ status: "ok" });
  const secondBootstrap = createDeferredResponse({
    capabilities: { bus_observability: true },
    endpoints: { graphql: "/graphql" },
  });
  const secondBusObservability = createDeferredResponse({ status: {} });
  const fetchQueue = [
    firstHealth.promise,
    firstBootstrap.promise,
    firstBusObservability.promise,
    secondHealth.promise,
    secondBootstrap.promise,
    secondBusObservability.promise,
  ];

  const elements = new Map([
    ["[data-role=\"status\"]", { textContent: "" }],
    ["[data-role=\"meta\"]", { textContent: "" }],
    ["[data-role=\"bus-banner\"]", { className: "", textContent: "" }],
    ["[data-role=\"bus-observability\"]", { innerHTML: "" }],
  ]);

  const {
    shell,
    intervalCalls,
    clearedIntervals,
    fetchRequests,
    fetchSignals,
  } = createPortalShellHarness({
    source,
    sourcePath,
    elements,
    fetchImpl() {
      const next = fetchQueue.shift();
      if (!next) {
        throw new Error("unexpected fetch");
      }
      return next;
    },
  });

  shell.connectedCallback();
  firstHealth.resolve();
  firstBootstrap.resolve();
  await flush();
  shell._isConnected = false;
  shell.disconnectedCallback();
  assert.equal(fetchSignals[0]?.aborted, true, "first bootstrap health request should abort on detach");
  assert.equal(fetchSignals[1]?.aborted, true, "first bootstrap bootstrap request should abort on detach");

  shell._isConnected = true;
  shell.connectedCallback();

  secondHealth.resolve();
  secondBootstrap.resolve();
  secondBusObservability.resolve();
  await flush();
  await flush();

  assert.equal(fetchSignals[3]?.aborted, false, "reconnect should use a fresh health signal");
  assert.equal(fetchSignals[4]?.aborted, false, "reconnect should use a fresh bootstrap signal");

  const liveHandle = shell.busObservabilityInterval;
  assert.ok(liveHandle, "expected live bus observability interval after reconnect");
  assert.equal(intervalCalls.length, 1, "expected one bus observability interval after reconnect");
  assert.equal(liveHandle.delay, 3000, "bus observability interval should use the 3s poll cadence");

  firstBusObservability.resolve();
  await flush();
  await flush();

  assert.equal(shell.busObservabilityInterval, liveHandle, "stale bootstrap completion must not replace the live bus interval");
  assert.equal(intervalCalls.length, 1, "stale bootstrap completion must not arm a second bus interval");
  assert.equal(clearedIntervals.length, 0, "stale bootstrap completion must not clear the live bus interval");
  assert.deepEqual(fetchRequests, [
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/bus/observability",
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/bus/observability",
  ]);
});

test("PortalShell renders unsupported adapter info with an unsupported label", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

  const snapshot = createDeferredResponse({
    adapter_info: {
      firmware_version: "0x31",
      info_supported: false,
      is_wifi: false,
      is_ethernet: false,
    },
  });

  const elements = new Map([
    ["[data-role=\"adapter-identity-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-telemetry-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-refresh-status\"]", { textContent: "" }],
  ]);

  const { shell } = createPortalShellHarness({
    source,
    sourcePath,
    elements,
    fetchImpl() {
      return snapshot.promise;
    },
  });

  const refresh = shell.refreshAdapterInfo();
  snapshot.resolve();
  await refresh;

  const identityBody = elements.get("[data-role=\"adapter-identity-body\"]");
  assert.ok(identityBody.innerHTML.includes("Unsupported/Unknown"), "unsupported adapter info should show an unsupported label");
  assert.ok(!identityBody.innerHTML.includes("Serial"), "unsupported adapter info must not be labeled as Serial");
});
