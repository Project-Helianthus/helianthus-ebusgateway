import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import vm from "node:vm";

test("raw Modbus browser call uses the fixed Portal route", async () => {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const source = await readFile(path.resolve(here, "../src/app.js"), "utf8");
  assert.match(source, /api\/v1\/explorer\/modbus\/raw-read/);
  assert.match(source, /api\/v1\/semantic\/pv\/current/);
  assert.match(source, /setInterval\(\(\) => this\.refreshPV\(\), 5000\)/);
  assert.doesNotMatch(source, /portal-pv-m2m-url|m2m-graphql-listen/);
});

async function loadPortalShell() {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");
  const cleared = [];
  const sandbox = {
    console: { error() {}, log() {}, warn() {} },
    document: { documentElement: { setAttribute() {} }, removeEventListener() {} },
    customElements: { define() {} },
    HTMLElement: class {},
    localStorage: { getItem() { return null; }, setItem() {} },
    clearInterval(value) { cleared.push(value); },
    clearTimeout() {},
    setInterval() { return {}; },
    setTimeout,
    AbortController,
    URLSearchParams,
    TextDecoder,
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, { filename: sourcePath });
  return { PortalShell: sandbox.__PortalShell, sandbox, cleared, source };
}

test("PV and raw panels follow all independent capability combinations", async () => {
  const { PortalShell } = await loadPortalShell();
  for (const semanticPV of [false, true]) {
    for (const rawModbus of [false, true]) {
      const pvPanel = { hidden: false };
      const rawPanel = { hidden: false };
      const shell = new PortalShell();
      shell.querySelector = (selector) => selector === '[data-role="pv-panel"]' ? pvPanel : selector === '[data-role="modbus-raw-panel"]' ? rawPanel : null;
      shell.applyPVModbusCapabilityState({ semantic_pv: semanticPV, modbus_raw_read: rawModbus });
      assert.equal(pvPanel.hidden, !semanticPV, `semantic_pv=${semanticPV} raw=${rawModbus}`);
      assert.equal(rawPanel.hidden, !rawModbus, `semantic_pv=${semanticPV} raw=${rawModbus}`);
    }
  }
});

test("PV polling interval is cleared during teardown", async () => {
  const { PortalShell, cleared, source } = await loadPortalShell();
  assert.match(source, /if \(this\.pvInterval\)[\s\S]*clearInterval\(this\.pvInterval\)[\s\S]*this\.pvInterval = undefined/);
  const shell = new PortalShell();
  const interval = {};
  shell.pvInterval = interval;
  shell.endBootstrapLifecycle = () => {};
  shell.clearEEBusVisibilityAuthority = () => {};
  shell.clearEEBusSPINETree = () => {};
  shell.clearEEBusPendingMutation = () => {};
  shell.disconnectedCallback();
  assert.ok(cleared.includes(interval));
  assert.equal(shell.pvInterval, undefined);
});

test("raw Modbus renders bounded unavailable text for network and non-JSON failures", async () => {
  for (const failure of ["network", "non_json"]) {
    const { PortalShell, sandbox } = await loadPortalShell();
    const output = { textContent: "" };
    const values = { unit: "1", function: "3", offset: "0", quantity: "1" };
    const shell = new PortalShell();
    shell._capabilityRawModbus = true;
    shell.querySelector = (selector) => {
      if (selector === '[data-role="modbus-raw-output"]') return output;
      const match = selector.match(/data-role="modbus-([^"]+)"/);
      return match && values[match[1]] !== undefined ? { value: values[match[1]] } : null;
    };
    sandbox.fetch = failure === "network"
      ? async () => { throw new Error("private upstream detail"); }
      : async () => ({ json: async () => { throw new Error("private non-JSON body"); } });
    await shell.readRawModbus();
    assert.equal(output.textContent, "Raw read unavailable");
    assert.ok(output.textContent.length <= 64);
  }
});

test("raw Modbus rejects a range crossing 65536 before fetch", async () => {
  const { PortalShell, sandbox } = await loadPortalShell();
  const output = { textContent: "" };
  let fetches = 0;
  sandbox.fetch = async () => { fetches += 1; return { json: async () => ({}) }; };
  const values = { unit: "1", function: "3", offset: "65535", quantity: "2" };
  const shell = new PortalShell();
  shell._capabilityRawModbus = true;
  shell.querySelector = (selector) => {
    if (selector === '[data-role="modbus-raw-output"]') return output;
    const match = selector.match(/data-role="modbus-([^"]+)"/);
    return match && values[match[1]] !== undefined ? { value: values[match[1]] } : null;
  };
  await shell.readRawModbus();
  assert.equal(fetches, 0);
  assert.equal(output.textContent, "Invalid raw read bounds");
});
