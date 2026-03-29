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

test("PortalShell ignores stale bootstrap completions when arming adapter polling", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");

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
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, {
    filename: pathToFileURL(sourcePath).href,
  });

  const PortalShell = sandbox.__PortalShell;
  const firstHealth = createDeferredResponse({ status: "ok" });
  const firstBootstrap = createDeferredResponse({
    capabilities: { semantic: true },
    endpoints: { graphql: "/graphql" },
  });
  const secondHealth = createDeferredResponse({ status: "ok" });
  const secondBootstrap = createDeferredResponse({
    capabilities: { semantic: true },
    endpoints: { graphql: "/graphql" },
  });
  const adapterSnapshot = createDeferredResponse({
    adapter_info: {
      firmware_version: "0x31",
      info_supported: true,
      temperature_c: 25,
    },
  });
  const fetchQueue = [
    firstHealth.promise,
    firstBootstrap.promise,
    secondHealth.promise,
    secondBootstrap.promise,
    adapterSnapshot.promise,
  ];
  sandbox.fetch = (url, requestInit) => {
    fetchRequests.push(url);
    fetchSignals.push(requestInit?.signal ?? null);
    const next = fetchQueue.shift();
    if (!next) {
      throw new Error(`unexpected fetch for ${url}`);
    }
    return next;
  };

  const elements = new Map([
    ["[data-role=\"status\"]", { textContent: "" }],
    ["[data-role=\"meta\"]", { textContent: "" }],
    ["[data-role=\"adapter-identity-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-telemetry-body\"]", { innerHTML: "" }],
    ["[data-role=\"adapter-refresh-status\"]", { textContent: "" }],
  ]);

  const shell = new PortalShell();
  shell._isConnected = true;
  shell.render = () => {};
  shell.bindEvents = () => {};
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];

  shell.connectedCallback();
  shell._isConnected = false;
  shell.disconnectedCallback();
  assert.equal(fetchSignals[0]?.aborted, true, "first bootstrap signal should be aborted on detach");
  assert.equal(fetchSignals[1]?.aborted, true, "second bootstrap signal should be aborted on detach");

  shell._isConnected = true;
  shell.connectedCallback();
  assert.equal(fetchSignals[2]?.aborted, false, "reconnect should use a fresh, live bootstrap signal");
  assert.equal(fetchSignals[3]?.aborted, false, "reconnect should use a fresh, live bootstrap signal");

  secondHealth.resolve();
  secondBootstrap.resolve();
  adapterSnapshot.resolve();
  await flush();
  await flush();

  assert.equal(intervalCalls.length, 1, "expected one adapter polling interval after reconnect");
  assert.equal(shell.bootstrapLifecycleToken, 3, "reconnect should advance the lifecycle token across disconnect/reconnect");

  firstHealth.resolve();
  firstBootstrap.resolve();
  await flush();
  await flush();

  assert.equal(intervalCalls.length, 1, "stale bootstrap completion must not arm a second interval");
  assert.equal(clearedIntervals.length, 0, "stale bootstrap completion should not clear the live interval");
  assert.deepEqual(fetchRequests, [
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/health",
    "api/v1/bootstrap",
    "api/v1/semantic/snapshot",
  ]);
});
