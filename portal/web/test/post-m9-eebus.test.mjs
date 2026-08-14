import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

async function issue809Shell(fetchImpl, elements = new Map()) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");
  class FakeHTMLElement {}
  const storageWrites = [];
  const documentListeners = new Map();
  let uuidSequence = 0;
  const sandbox = {
    console: { error() {}, warn() {}, log() {} },
    document: {
      documentElement: { setAttribute() {} },
      visibilityState: "visible",
      addEventListener(name, handler) { documentListeners.set(name, handler); },
      removeEventListener(name, handler) {
        if (documentListeners.get(name) === handler) documentListeners.delete(name);
      },
    },
    customElements: { define() {} },
    HTMLElement: FakeHTMLElement,
    localStorage: {
      getItem: () => null,
      setItem: (key, value) => storageWrites.push([key, value]),
    },
    fetch: fetchImpl,
    btoa: (value) => Buffer.from(value, "binary").toString("base64"),
    crypto: { randomUUID: () => `80900000-0000-4000-8000-${String(++uuidSequence).padStart(12, "0")}` },
    AbortController,
    URLSearchParams,
    TextDecoder,
    setInterval: () => 1,
    clearInterval() {},
    setTimeout,
    clearTimeout,
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, { filename: pathToFileURL(sourcePath).href });
  const shell = new sandbox.__PortalShell();
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];
  shell._eebusAdminPath = "/admin/eebus/v1";
  return { shell, storageWrites, documentListeners, document: sandbox.document };
}

function response(payload, csrf = "") {
  return {
    ok: true,
    status: 200,
    headers: { get: (name) => name.toLowerCase() === "x-csrf-token" ? csrf : null },
    json: async () => payload,
  };
}

test("owner login creates cookie session and candidate identity remains active-memory only", async () => {
  const calls = [];
  const status = { textContent: "" };
  const candidate = { textContent: "" };
  const elements = new Map([
    ['[data-role="eebus-status"]', status],
    ['[data-role="eebus-candidate"]', candidate],
  ]);
  const fetchImpl = async (url, init = {}) => {
    calls.push({ url, init });
    if (String(url).endsWith("/status")) {
      return response({ state_revision: 7, data: { status: "ready" } }, "csrf-809");
    }
    return response({ state_revision: 7, data: { partners: [{ view: "candidate", remote_ski: "a".repeat(40), candidate_state: "tls_bound" }] } });
  };
  const { shell, storageWrites } = await issue809Shell(fetchImpl, elements);

  await shell.loginEEBusAdmin("owner", "owner-password");
  assert.match(calls[0].init.headers.Authorization, /^Basic /);
  assert.equal(shell._eebusCSRFToken, "csrf-809");
  assert.equal(shell._eebusStateRevision, 7);
  assert.equal(shell._eebusOwnerPassword, undefined);

  await shell.refreshEEBusPartners("candidate");
  assert.equal(shell._eebusCandidate.remote_ski, "a".repeat(40));
  assert.equal(storageWrites.length, 0, "candidate/admin state must never enter localStorage");
  shell.clearEEBusCandidate();
  assert.equal(shell._eebusCandidate, undefined);
  assert.equal(candidate.textContent, "");
});

test("confirm mutation carries CSRF, revision and exact SKI but no transport coordinate", async () => {
  const calls = [];
  const { shell } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    return response({ state_revision: 10, data: { outcome: "confirmed" } }, "csrf-next");
  });
  shell._eebusCSRFToken = "csrf-current";
  shell._eebusStateRevision = 9;
  shell._eebusCandidate = { remote_ski: "b".repeat(40) };
  await shell.confirmEEBusCandidate("b".repeat(40));

  const request = calls[0];
  const body = JSON.parse(request.init.body);
  assert.equal(request.url, "/admin/eebus/v1/candidate:confirm");
  assert.equal(request.init.headers["X-CSRF-Token"], "csrf-current");
  assert.equal(request.init.headers["Idempotency-Key"], "80900000-0000-4000-8000-000000000001");
  assert.deepEqual(body, { state_revision: 9, expected_ski: "b".repeat(40) });
  for (const forbidden of ["endpoint", "host", "port", "observation_id"]) {
    assert.equal(Object.hasOwn(body, forbidden), false);
  }
  assert.equal(shell._eebusCandidate, undefined);
});

test("selection requires independently entered OOB SKI and never copies discovery identity into action authority", async () => {
  const calls = [];
  const partners = { innerHTML: "" };
  const selectSKI = { value: "c".repeat(40) };
  const { shell } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    return response({ state_revision: 8, data: { outcome: "selected", selection_id: "selection-1" } });
  }, new Map([
    ['[data-role="eebus-partners"]', partners],
    ['[data-role="eebus-select-ski"]', selectSKI],
  ]));
  shell._eebusCSRFToken = "csrf";
  shell._eebusStateRevision = 7;
  shell.renderEEBusPartners([{ observation_id: "observation-1", remote_ski: "d".repeat(40), view: "discovered" }], "discovered");
  assert.doesNotMatch(partners.innerHTML, /data-eebus-ski=/, "discovery identity must not become button authority");

  await shell.selectEEBusObservation("observation-1");
  assert.equal(JSON.parse(calls[0].init.body).expected_ski, "c".repeat(40));
  assert.equal(selectSKI.value, "", "OOB input must be cleared after use");
});

test("lost mutation response retains the exact idempotency binding and selection until terminal replay", async () => {
  const calls = [];
  let attempt = 0;
  const { shell } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    if (++attempt === 1) throw new Error("response lost after effect");
    return response({ state_revision: 13, data: { outcome: "connecting", replayed: true } });
  });
  shell._eebusCSRFToken = "csrf";
  shell._eebusStateRevision = 12;
  shell._eebusSelection = { id: "selection-1" };

  await assert.rejects(shell.connectEEBusSelection(), /response lost/);
  assert.equal(shell._eebusSelection?.id, "selection-1", "ambiguous network failure must retain selection authority");
  await shell.connectEEBusSelection();

  assert.equal(calls.length, 2);
  assert.equal(calls[0].url, calls[1].url);
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(calls[0].init.headers["Idempotency-Key"], calls[1].init.headers["Idempotency-Key"]);
  assert.equal(shell._eebusSelection, undefined, "terminal replay consumes selection authority");
  assert.equal(shell._eebusPendingMutation, undefined, "terminal replay retires active-memory idempotency state");
});

test("lost confirm response clears visibility authority and retries only the frozen request", async () => {
  const calls = [];
  let attempt = 0;
  const candidate = { textContent: "sensitive-candidate" };
  const input = { value: "a".repeat(40) };
  const { shell, documentListeners, document, storageWrites } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    if (String(url).endsWith("/status")) return response({ state_revision: 14, data: { status: "ready" } });
    if (++attempt === 1) throw new Error("response lost after effect");
    return response({ state_revision: 15, data: { outcome: "confirmed", replayed: true } });
  }, new Map([
    ['[data-role="eebus-candidate"]', candidate],
    ['[data-role="eebus-confirm-ski"]', input],
  ]));
  shell._eebusCSRFToken = "csrf";
  shell._eebusStateRevision = 14;
  shell._eebusCandidate = { remote_ski: "a".repeat(40) };
  shell._eebusSelection = { id: "selection-1" };
  shell._eebusUntrustArmedID = "partner-1";
  shell.bindEEBusAdminEvents();

  await assert.rejects(shell.confirmEEBusCandidate("a".repeat(40)), /response lost/);
  document.visibilityState = "hidden";
  documentListeners.get("visibilitychange")();
  await shell.refreshEEBusStatus();

  assert.equal(candidate.textContent, "", "candidate raw UI clears while hidden");
  assert.equal(input.value, "", "candidate input clears while hidden");
  assert.equal(shell._eebusCandidate, undefined, "candidate model clears while hidden");
  assert.equal(shell._eebusSelection, undefined, "selection clears while hidden");
  assert.equal(shell._eebusUntrustArmedID, undefined, "untrust arm clears while hidden");
  assert.equal(shell._eebusSpinePartnerID, undefined, "SPINE partner clears while hidden");
  assert.equal(shell._eebusSpineSnapshotID, undefined, "SPINE snapshot clears while hidden");
  assert.deepEqual(Object.keys(shell._eebusPendingMutation).sort(), ["body", "expiresAt", "idempotencyKey", "method", "path"], "pending state retains only a frozen bounded request");
  assert.equal(storageWrites.length, 0, "replay authority is never persisted");
  await assert.rejects(shell.openEEBusPairingWindow(), /previous eeBUS mutation outcome is unknown/);

  await shell.retryPendingEEBusMutation();
  assert.equal(calls.filter((call) => call.url.endsWith("candidate:confirm")).length, 2);
  assert.equal(calls[0].init.headers["Idempotency-Key"], calls[2].init.headers["Idempotency-Key"]);
  assert.equal(calls[0].init.body, calls[2].init.body, "retry must send its frozen pre-refresh revision/body");
  assert.equal(shell._eebusPendingMutation, undefined, "terminal replay retires pending authority");
});

test("SPINE browser loads root and expands only opaque server-issued node identifiers", async () => {
  const calls = [];
  const tree = { innerHTML: "" };
  const { shell } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    return response({ data: { snapshot_id: "snapshot-1", snapshot_hash: "sha256:" + "c".repeat(64), parent_node_id: null, nodes: [{ node_id: "node-1", parent_node_id: null, kind: "device", sort_key: "device|vr940", payload: { type: "EnergyManagementSystem" } }] } });
  }, new Map([['[data-role="eebus-spine-tree"]', tree]]));

  await shell.loadEEBusSPINERoot("partner-1");
  assert.match(calls[0].url, /partners\/partner-1\/spine\?request=root$/);
  assert.equal(shell._eebusSpineSnapshotID, "snapshot-1");
  assert.match(tree.innerHTML, /EnergyManagementSystem/);
  assert.doesNotMatch(tree.innerHTML, /undefined/);
});

test("untrust requires an explicit arm step for the same opaque partner", async () => {
  const calls = [];
  const { shell } = await issue809Shell(async (url, init = {}) => {
    calls.push({ url, init });
    return response({ state_revision: 12, data: { outcome: "untrusted" } });
  });
  shell._eebusCSRFToken = "csrf";
  shell._eebusStateRevision = 11;
  await assert.rejects(shell.untrustEEBusPartner("partner-1"), /not armed/);
  assert.equal(calls.length, 0);
  shell.armEEBusUntrust("partner-1");
  await shell.untrustEEBusPartner("partner-1");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "/admin/eebus/v1/partners/partner-1/trust");
  assert.equal(calls[0].init.method, "DELETE");
});

test("every later admin response clears an active candidate before replacement", async () => {
  const candidate = { textContent: "sensitive-candidate" };
  const input = { value: "a".repeat(40) };
  const { shell } = await issue809Shell(async () => response({ state_revision: 18, data: { status: "ready" } }), new Map([
    ['[data-role="eebus-candidate"]', candidate],
    ['[data-role="eebus-confirm-ski"]', input],
  ]));
  shell._eebusCandidate = { remote_ski: "a".repeat(40) };
  await shell.refreshEEBusStatus();
  assert.equal(shell._eebusCandidate, undefined);
  assert.equal(candidate.textContent, "");
  assert.equal(input.value, "");
});
