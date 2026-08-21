import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import vm from "node:vm";
import test from "node:test";

async function issue817Shell(fetchImpl, elements = new Map()) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sourcePath = path.resolve(here, "../src/app.js");
  const source = await readFile(sourcePath, "utf8");
  class FakeHTMLElement {}
  const storageWrites = [];
  const documentListeners = new Map();
  const timers = new Map();
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
    crypto: { randomUUID: () => `81700000-0000-4000-8000-${String(++uuidSequence).padStart(12, "0")}` },
    AbortController,
    URLSearchParams,
    TextDecoder,
    setInterval: () => 1,
    clearInterval() {},
    setTimeout: (callback, delay) => {
      const timer = { callback, delay, cleared: false };
      timers.set(timer, timer);
      return timer;
    },
    clearTimeout: (timer) => { if (timer) timer.cleared = true; },
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(`${source}\n;globalThis.__PortalShell = PortalShell;`, sandbox, { filename: pathToFileURL(sourcePath).href });
  const shell = new sandbox.__PortalShell();
  shell.querySelector = (selector) => elements.get(selector) || null;
  shell.querySelectorAll = () => [];
  shell._eebusAdminPath = "/admin/eebus/v1";
  return { shell, storageWrites, documentListeners, document: sandbox.document, timers, source };
}

function response(payload) {
  return {
    ok: true,
    status: 200,
    headers: { get: () => null },
    json: async () => payload,
  };
}

test("Portal source contains no eeBUS login, credential, session, or CSRF surface", async () => {
  const { source } = await issue817Shell(async () => response({ state_revision: 1, data: {} }));
  for (const forbidden of [
    "loginEEBusAdmin",
    "_eebusCSRFToken",
    "X-CSRF-Token",
    "eebus-owner-username",
    "eebus-owner-password",
    "eebus-login",
    "Owner username",
    "Owner credential",
    ">Authenticate<",
    "owner session is not authenticated",
  ]) {
    assert.equal(source.includes(forbidden), false, `Portal retains eeBUS auth token ${forbidden}`);
  }
  assert.match(source, /data-role="eebus-window-open"/);
  assert.match(source, /data-role="eebus-candidate-confirm"/);
  assert.match(source, /data-role="eebus-spine-tree"/);
});

test("Portal status and all four partner views use one state_revision envelope without auth headers", async () => {
  const calls = [];
  const status = { textContent: "" };
  const partners = { innerHTML: "", addEventListener() {} };
  const candidate = { textContent: "" };
  const { shell } = await issue817Shell(async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).endsWith("/status")) return response({ state_revision: 7, data: { status: "ready" }, error: null });
    const view = new URLSearchParams(String(url).split("?")[1]).get("view");
    const row = view === "candidate" ? { view, remote_ski: "a".repeat(40), candidate_state: "tls_bound" } : { view };
    return response({ state_revision: 7, data: { partners: [row] }, error: null });
  }, new Map([
    ['[data-role="eebus-status"]', status],
    ['[data-role="eebus-partners"]', partners],
    ['[data-role="eebus-candidate"]', candidate],
  ]));

  await shell.refreshEEBusStatus();
  for (const view of ["trusted", "connected", "discovered", "candidate"]) await shell.refreshEEBusPartners(view);
  assert.equal(shell._eebusStateRevision, 7);
  assert.equal(shell._eebusCandidate.remote_ski, "a".repeat(40));
  for (const call of calls) {
    assert.equal(Object.hasOwn(call.init.headers, "Authorization"), false);
    assert.equal(Object.hasOwn(call.init.headers, "X-CSRF-Token"), false);
  }
});

test("Portal emits the exact closed mutation matrix with revision and Idempotency-Key only", async (t) => {
  const cases = [
    { name: "open", invoke: (shell) => shell.openEEBusPairingWindow(), path: "/pairing-window:open", method: "POST", body: { duration_seconds: 300, state_revision: 9 } },
    { name: "close", invoke: (shell) => shell.closeEEBusPairingWindow(), path: "/pairing-window:close", method: "POST", body: { state_revision: 9 } },
    { name: "cancel", setup: (shell) => { shell._eebusCandidate = { remote_ski: "a".repeat(40) }; }, invoke: (shell) => shell.cancelEEBusCandidate(), path: "/candidate:cancel", method: "POST", body: { state_revision: 9 } },
    { name: "retry", invoke: (shell) => shell.retryEEBusPartner("partner-1"), path: "/partners/partner-1:retry", method: "POST", body: { state_revision: 9 } },
    { name: "untrust", setup: (shell) => { shell._eebusUntrustArmedID = "partner-1"; }, invoke: (shell) => shell.untrustEEBusPartner("partner-1"), path: "/partners/partner-1/trust", method: "DELETE", body: { state_revision: 9 } },
    { name: "connect", setup: (shell) => { shell._eebusSelection = { id: "selection-1" }; }, invoke: (shell) => shell.connectEEBusSelection(), path: "/selections/selection-1:connect", method: "POST", body: { state_revision: 9 } },
    { name: "confirm", setup: (shell) => { shell._eebusCandidate = { remote_ski: "b".repeat(40) }; }, invoke: (shell) => shell.confirmEEBusCandidate("b".repeat(40)), path: "/candidate:confirm", method: "POST", body: { state_revision: 9, expected_ski: "b".repeat(40) } },
  ];

  for (const entry of cases) {
    await t.test(entry.name, async () => {
      const calls = [];
      const { shell } = await issue817Shell(async (url, init = {}) => {
        calls.push({ url: String(url), init });
        return response({ state_revision: 10, data: entry.name === "connect" ? { outcome: entry.name, action_id: "action-817" } : { outcome: entry.name }, error: null });
      });
      shell._eebusStateRevision = 9;
      entry.setup?.(shell);
      await entry.invoke(shell);
      assert.equal(calls.length, 1);
      assert.equal(calls[0].url, `/admin/eebus/v1${entry.path}`);
      assert.equal(calls[0].init.method, entry.method);
      assert.deepEqual(JSON.parse(calls[0].init.body), entry.body);
      assert.equal(calls[0].init.headers["Idempotency-Key"], "81700000-0000-4000-8000-000000000001");
      assert.equal(Object.hasOwn(calls[0].init.headers, "Authorization"), false);
      assert.equal(Object.hasOwn(calls[0].init.headers, "X-CSRF-Token"), false);
    });
  }
});

test("selection requires independently entered complete OOB SKI and connect keeps only selection_id", async () => {
  const calls = [];
  const partners = { innerHTML: "" };
  const selectSKI = { value: "c".repeat(40) };
  const { shell } = await issue817Shell(async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).includes(":select")) return response({ state_revision: 8, data: { outcome: "selected", selection_id: "selection-1" }, error: null });
    return response({ state_revision: 9, data: { outcome: "connecting", action_id: "action-1" }, error: null });
  }, new Map([
    ['[data-role="eebus-partners"]', partners],
    ['[data-role="eebus-select-ski"]', selectSKI],
  ]));
  shell._eebusStateRevision = 7;
  shell.renderEEBusPartners([{ observation_id: "observation-1", remote_ski: "d".repeat(40), endpoint: "192.0.2.10:4712", view: "discovered" }], "discovered");
  assert.doesNotMatch(partners.innerHTML, /data-eebus-ski=/);

  await shell.selectEEBusObservation("observation-1");
  assert.deepEqual(JSON.parse(calls[0].init.body), { state_revision: 7, expected_ski: "c".repeat(40) });
  assert.equal(selectSKI.value, "");
  await shell.connectEEBusSelection();
  assert.deepEqual(JSON.parse(calls[1].init.body), { state_revision: 8 });
  for (const forbidden of ["expected_ski", "observation_id", "endpoint", "host", "port"]) {
    assert.equal(Object.hasOwn(JSON.parse(calls[1].init.body), forbidden), false);
  }
});

test("PIN connect never enters generic pending replay and clears the password field immediately", async () => {
  const calls = [];
  const pin = { value: "A1b2C3d4" };
  const status = { textContent: "" };
  const { shell, storageWrites } = await issue817Shell(async (url, init = {}) => {
    calls.push({ url: String(url), init });
    return response({ state_revision: 11, data: { action_id: "action-848", outcome: "connection_started" }, error: null });
  }, new Map([
    ['[data-role="eebus-connect-pin"]', pin],
    ['[data-role="eebus-pairing-status"]', status],
  ]));
  shell._eebusStateRevision = 10;
  shell._eebusSelection = { id: "selection-848" };
  shell._eebusPINRequested = true;
  await shell.connectEEBusSelection();
  assert.equal(pin.value, "");
  assert.equal(shell._eebusPendingMutation, undefined);
  assert.equal(shell._eebusActiveActionID, "action-848");
  assert.equal(storageWrites.length, 0);
  assert.equal(calls.length, 1);
  assert.deepEqual(JSON.parse(calls[0].init.body), { state_revision: 10, pin: "A1b2C3d4" });
  assert.equal(calls[0].init.headers["Idempotency-Key"], "81700000-0000-4000-8000-000000000001");
  assert.doesNotMatch(shell._eebusPendingMutation ? JSON.stringify(shell._eebusPendingMutation) : "", /A1b2C3d4/);
});

test("Portal renders only the six documented terminal PIN outcomes", async () => {
  const status = { textContent: "" };
  const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), new Map([
    ['[data-role="eebus-pairing-status"]', status],
  ]));
  shell._eebusActiveActionID = "action-848";
  for (const outcome of ["pin_required", "pin_optional", "pin_busy", "pin_rejected", "pin_unavailable", "pin_protocol_error"]) {
    shell._eebusActiveActionID = "action-848";
    shell.renderEEBusActiveAction({ action_id: "action-848", state: "terminal", outcome, expiry: new Date(Date.now() + 60_000).toISOString() });
    assert.notEqual(status.textContent, "");
  }
  assert.equal((await readFile(path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../src/app.js"), "utf8")).includes("localStorage.setItem(\"eebus"), false);
});

test("network retry reuses the exact body and Idempotency-Key without session state", async () => {
  const calls = [];
  let first = true;
  const { shell } = await issue817Shell(async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (first) {
      first = false;
      throw new Error("connection reset after send");
    }
    return response({ state_revision: 10, data: { outcome: "pairing_window_opened", replayed: true }, error: null });
  });
  shell._eebusStateRevision = 9;
  await assert.rejects(shell.openEEBusPairingWindow(), /connection reset/);
  await shell.retryPendingEEBusMutation();
  assert.equal(calls.length, 2);
  assert.equal(calls[0].init.body, calls[1].init.body);
  assert.equal(calls[0].init.headers["Idempotency-Key"], calls[1].init.headers["Idempotency-Key"]);
  assert.equal(Object.hasOwn(calls[1].init.headers, "X-CSRF-Token"), false);
});

test("candidate and raw SPINE remain active-memory only and clear on visibility loss", async () => {
  const candidate = { textContent: "" };
  const input = { value: "a".repeat(40) };
  const tree = { innerHTML: "", addEventListener() {} };
  const { shell, storageWrites, documentListeners, document } = await issue817Shell(async (url) => {
    if (String(url).includes("/spine?request=root")) {
      return response({ state_revision: 12, data: { snapshot_id: "snapshot-1", snapshot_hash: `sha256:${"c".repeat(64)}`, parent_node_id: null, nodes: [{ node_id: "node-1", parent_node_id: null, kind: "device", sort_key: "device|vr940", payload: { type: "EnergyManagementSystem" } }] }, error: null });
    }
    return response({ state_revision: 12, data: { partners: [{ view: "candidate", remote_ski: "a".repeat(40), candidate_state: "tls_bound" }] }, error: null });
  }, new Map([
    ['[data-role="eebus-candidate"]', candidate],
    ['[data-role="eebus-confirm-ski"]', input],
    ['[data-role="eebus-spine-tree"]', tree],
  ]));

  shell.bindEEBusAdminEvents();
  await shell.refreshEEBusPartners("candidate");
  assert.equal(shell._eebusCandidate.remote_ski, "a".repeat(40));
  assert.equal(storageWrites.length, 0);
  await shell.loadEEBusSPINERoot("partner-1");
  assert.equal(storageWrites.length, 0);
  assert.equal(shell._eebusCandidate, undefined, "a later operator response replaces the active candidate view");
  assert.match(tree.innerHTML, /EnergyManagementSystem/);
  document.visibilityState = "hidden";
  documentListeners.get("visibilitychange")();
  assert.equal(shell._eebusCandidate, undefined);
  assert.equal(candidate.textContent, "");
  assert.equal(input.value, "");
  assert.doesNotMatch(tree.innerHTML, /EnergyManagementSystem/);
});

test("eeBUS renders three separate Pairing SHIP and SPINE workspaces", async () => {
  const { source } = await issue817Shell(async () => response({ state_revision: 1, data: {} }));
  assert.match(source, /data-role="eebus-workspace-nav"/);
  for (const workspace of ["pairing", "ship", "spine"]) {
    assert.match(source, new RegExp(`data-eebus-workspace="${workspace}"`));
    assert.match(source, new RegExp(`data-eebus-workspace-target="${workspace}"`));
  }
  const spinePanel = source.match(/data-eebus-workspace="spine"[\s\S]*?<\/section>/)?.[0] || "";
  for (const forbidden of ["eebus-window-open", "eebus-candidate-confirm", "data-eebus-action=\\\"retry\\\"", "data-eebus-action=\\\"untrust\\\""]) {
    assert.equal(spinePanel.includes(forbidden), false, `SPINE retains mutation control ${forbidden}`);
  }
});

test("trusted offline rows show reconnect required and never expose Browse SPINE", async () => {
  const shipPartners = { innerHTML: "" };
  const { shell } = await issue817Shell(async () => response({ state_revision: 4, data: {}, error: null }), new Map([
    ['[data-role="eebus-ship-partners"]', shipPartners],
  ]));
  shell.renderEEBusPartners([{ partner_id: "trusted-1", view: "trusted", trust_state: "trusted" }], "trusted");
  assert.match(shipPartners.innerHTML, /Reconnect required/);
  assert.match(shipPartners.innerHTML, /data-eebus-action="retry"/);
  assert.doesNotMatch(shipPartners.innerHTML, /data-eebus-action="spine"/);
});

test("SPINE peer refresh requests only the connected view and Browse stays GET-only", async () => {
  const calls = [];
  const spinePeers = { innerHTML: "" };
  const tree = { innerHTML: "" };
  const { shell } = await issue817Shell(async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).includes("partners?view=connected")) {
      return response({ state_revision: 5, data: { partners: [{ partner_id: "live-1", view: "connected", connection_state: "connected" }] }, error: null });
    }
    return response({ state_revision: 5, data: { snapshot_id: "snapshot-1", snapshot_hash: `sha256:${"a".repeat(64)}`, parent_node_id: null, nodes: [] }, error: null });
  }, new Map([
    ['[data-role="eebus-spine-peers"]', spinePeers],
    ['[data-role="eebus-spine-tree"]', tree],
  ]));

  await shell.refreshEEBusSPINEPeers();
  assert.match(spinePeers.innerHTML, /data-eebus-action="spine"/);
  await shell.loadEEBusSPINERoot("live-1");
  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /partners\?view=connected$/);
  assert.equal(calls[0].init.method, "GET");
  assert.equal(calls[1].init.method, "GET");
  assert.match(calls[1].url, /\/partners\/live-1\/spine\?request=root$/);
});

test("workspace navigation clears only the departing workspace volatile authority", async () => {
  const panels = ["pairing", "ship", "spine"].map((name) => ({ dataset: { eebusWorkspace: name }, hidden: false }));
  const buttons = ["pairing", "ship", "spine"].map((name) => ({ dataset: { eebusWorkspaceTarget: name }, setAttribute() {} }));
  const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {}, error: null }));
  shell.querySelectorAll = (selector) => selector.includes("workspace-target") ? buttons : panels;
  shell.clearEEBusCandidate = () => { shell._candidateCleared = true; };
  shell.clearEEBusSPINETree = () => { shell._spineCleared = true; };
  shell._eebusWorkspace = "pairing";
  shell._eebusSelection = { id: "selection-1" };
  shell._eebusUntrustArmedID = "partner-1";
  shell.switchEEBusWorkspace("ship");
  assert.equal(shell._candidateCleared, true);
  assert.equal(shell._eebusSelection, undefined);
  shell.switchEEBusWorkspace("spine");
  assert.equal(shell._eebusUntrustArmedID, undefined);
  shell.switchEEBusWorkspace("pairing");
  assert.equal(shell._spineCleared, true);
});

function issue848PairingElements() {
  const elements = new Map([
    ['[data-role="eebus-connect-pin"]', { value: "A1b2C3d4" }],
    ['[data-role="eebus-select-ski"]', { value: "a".repeat(40) }],
    ['[data-role="eebus-confirm-ski"]', { value: "b".repeat(40) }],
    ['[data-role="eebus-candidate"]', { textContent: "candidate-secret" }],
    ['[data-role="eebus-retry-pending"]', { disabled: false }],
    ['[data-role="eebus-pairing-status"]', { textContent: "pairing" }],
  ]);
  for (const element of elements.values()) element.addEventListener = () => {};
  return elements;
}

function primeIssue848PairingAuthority(shell) {
  shell._eebusWorkspace = "pairing";
  shell._eebusCandidate = { remote_ski: "b".repeat(40), candidate_state: "tls_bound" };
  shell._eebusCandidateTimer = { cleared: false };
  shell._eebusSelection = { id: "selection-secret" };
  shell._eebusPINRequested = true;
  shell._eebusPendingMutation = { method: "POST", path: "/candidate:confirm", body: "request-secret", idempotencyKey: "pending-secret", workspace: "pairing" };
  shell._eebusPendingMutationTimer = { cleared: false };
  shell._eebusActiveActionID = "action-secret";
  shell._eebusLastActiveAction = { action_id: "action-secret", state: "running" };
  shell._eebusActiveActionTimer = { cleared: false };
  shell._eebusActiveActionRetries = 1;
  shell._eebusUntrustArmedID = "partner-secret";
}

function assertIssue848PairingAuthorityCleared(shell, elements) {
  for (const role of ["eebus-connect-pin", "eebus-select-ski", "eebus-confirm-ski"]) {
    assert.equal(elements.get(`[data-role="${role}"]`).value, "", `${role} was retained`);
  }
  assert.equal(elements.get('[data-role="eebus-candidate"]').textContent, "");
  assert.equal(elements.get('[data-role="eebus-retry-pending"]').disabled, true);
  assert.equal(shell._eebusCandidate, undefined);
  assert.equal(shell._eebusCandidateTimer, undefined);
  assert.equal(shell._eebusSelection, undefined);
  assert.equal(shell._eebusPINRequested, false);
  assert.equal(shell._eebusPendingMutation, undefined);
  assert.equal(shell._eebusPendingMutationTimer, undefined);
  assert.equal(shell._eebusActiveActionID, undefined);
  assert.equal(shell._eebusLastActiveAction, undefined);
  assert.equal(shell._eebusActiveActionTimer, undefined);
  assert.equal(shell._eebusActiveActionRetries, 0);
}

test("Pairing to SHIP and Pairing to SPINE clear every transient pairing authority", async (t) => {
  for (const destination of ["ship", "spine"]) {
    await t.test(destination, async () => {
      const elements = issue848PairingElements();
      const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
      shell.querySelectorAll = () => [];
      primeIssue848PairingAuthority(shell);
      shell.switchEEBusWorkspace(destination);
      assertIssue848PairingAuthorityCleared(shell, elements);
    });
  }
});

test("visibility loss, component disconnect, and leaving eeBUS clear pairing authority", async (t) => {
  await t.test("visibility loss", async () => {
    const elements = issue848PairingElements();
    const { shell, document, documentListeners } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
    shell.bindEEBusAdminEvents();
    primeIssue848PairingAuthority(shell);
    document.visibilityState = "hidden";
    documentListeners.get("visibilitychange")();
    assertIssue848PairingAuthorityCleared(shell, elements);
  });

  await t.test("component disconnect", async () => {
    const elements = issue848PairingElements();
    const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
    shell.endBootstrapLifecycle = () => {};
    primeIssue848PairingAuthority(shell);
    shell.disconnectedCallback();
    assertIssue848PairingAuthorityCleared(shell, elements);
  });

  await t.test("leave eeBUS section", async () => {
    const elements = issue848PairingElements();
    const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
    shell.querySelectorAll = () => [];
    shell._activeSectionTarget = "section-eebus";
    primeIssue848PairingAuthority(shell);
    shell.activateSection("section-registry");
    assertIssue848PairingAuthorityCleared(shell, elements);
  });
});

test("candidate, pending replay, and active action expiry retire their volatile caches", async () => {
  const elements = issue848PairingElements();
  let fetchMode = "candidate";
  const { shell } = await issue817Shell(async () => {
    if (fetchMode === "candidate") {
      return response({ state_revision: 12, data: { partners: [{ view: "candidate", remote_ski: "b".repeat(40), candidate_state: "tls_bound", candidate_expires_at: new Date(Date.now() + 1000).toISOString() }] }, error: null });
    }
    throw new Error("ambiguous network failure");
  }, elements);
  await shell.refreshEEBusPartners("candidate");
  assert.ok(shell._eebusCandidateTimer);
  shell._eebusCandidateTimer.callback();
  assert.equal(shell._eebusCandidate, undefined);
  assert.equal(elements.get('[data-role="eebus-confirm-ski"]').value, "");

  fetchMode = "pending";
  shell._eebusStateRevision = 12;
  await assert.rejects(shell.openEEBusPairingWindow(), /ambiguous network failure/);
  assert.ok(shell._eebusPendingMutationTimer);
  shell._eebusPendingMutationTimer.callback();
  assert.equal(shell._eebusPendingMutation, undefined);
  assert.equal(shell._eebusPendingMutationTimer, undefined);
  assert.equal(elements.get('[data-role="eebus-retry-pending"]').disabled, true);

  shell._eebusActiveActionID = "action-expired";
  shell._eebusLastActiveAction = { action_id: "action-expired", state: "running" };
  shell._eebusActiveActionTimer = { cleared: false };
  shell._eebusActiveActionRetries = 1;
  shell.renderEEBusActiveAction({ action_id: "action-expired", state: "running", expiry: new Date(Date.now() - 1).toISOString() });
  assert.equal(shell._eebusActiveActionID, undefined);
  assert.equal(shell._eebusLastActiveAction, undefined);
  assert.equal(shell._eebusActiveActionTimer, undefined);
  assert.equal(shell._eebusActiveActionRetries, 0);
});

test("successive pairing actions ignore stale status and render each terminal only once", async () => {
  let renders = 0;
  let renderedText = "";
  const status = {};
  Object.defineProperty(status, "textContent", {
    get: () => renderedText,
    set: (value) => { renderedText = value; renders += 1; },
  });
  const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), new Map([
    ['[data-role="eebus-pairing-status"]', status],
  ]));

  shell._eebusActiveActionID = "action-1";
  shell._eebusActiveActionTimer = { cleared: false };
  shell._eebusActiveActionRetries = 1;
  const firstTerminal = { action_id: "action-1", state: "terminal", outcome: "pin_rejected", expiry: new Date(Date.now() + 60_000).toISOString() };
  shell.renderEEBusActiveAction(firstTerminal);
  assert.equal(renders, 1);
  assert.equal(shell._eebusActiveActionID, undefined);
  assert.equal(shell._eebusLastActiveAction, undefined);
  assert.equal(shell._eebusActiveActionTimer, undefined);
  assert.equal(shell._eebusActiveActionRetries, 0);
  shell.renderEEBusActiveAction(firstTerminal);
  assert.equal(renders, 1, "first terminal rendered more than once");

  shell._eebusActiveActionID = "action-2";
  shell._eebusActiveActionTimer = { cleared: false };
  shell.renderEEBusActiveAction(firstTerminal, "action-1");
  assert.equal(shell._eebusActiveActionID, "action-2", "stale first action retired the second action");
  assert.equal(shell._eebusLastActiveAction, undefined, "stale first action contaminated the second cache");
  shell.renderEEBusActiveAction({ action_id: "action-2", state: "running", expiry: new Date(Date.now() + 60_000).toISOString() });
  assert.equal(shell._eebusLastActiveAction.action_id, "action-2");
  const secondTerminal = { action_id: "action-2", state: "terminal", outcome: "connection_started", expiry: new Date(Date.now() + 60_000).toISOString() };
  shell.renderEEBusActiveAction(secondTerminal);
  const afterSecondTerminal = renders;
  shell.renderEEBusActiveAction(secondTerminal);
  assert.equal(renders, afterSecondTerminal, "second terminal rendered more than once");
  assert.equal(shell._eebusActiveActionID, undefined);
  assert.equal(shell._eebusLastActiveAction, undefined);
  assert.equal(shell._eebusActiveActionRetries, 0);
});

test("status requests may retire only the action that was current when the request started", async (t) => {
  for (const staleActiveAction of [undefined, { action_id: "action-1", state: "terminal", outcome: "pin_required", expiry: new Date(Date.now() + 60_000).toISOString() }]) {
    await t.test(staleActiveAction ? "mismatched terminal" : "missing active action", async () => {
      let resolvePayload;
      const delayedPayload = new Promise((resolve) => { resolvePayload = resolve; });
      const { shell } = await issue817Shell(async () => ({
        ok: true,
        status: 200,
        headers: { get: () => null },
        json: async () => delayedPayload,
      }));
      shell._eebusActiveActionID = "action-1";
      shell._eebusActiveActionTimer = { owner: "action-1", cleared: false };
      const delayedStatus = shell.refreshEEBusStatus();

      shell.clearEEBusActiveAction();
      const action2Timer = { owner: "action-2", cleared: false };
      shell._eebusActiveActionID = "action-2";
      shell._eebusLastActiveAction = { action_id: "action-2", state: "running" };
      shell._eebusActiveActionTimer = action2Timer;
      shell._eebusActiveActionRetries = 1;
      resolvePayload({ state_revision: 22, data: staleActiveAction ? { active_action: staleActiveAction } : {}, error: null });
      await delayedStatus;

      assert.equal(shell._eebusActiveActionID, "action-2");
      assert.equal(shell._eebusLastActiveAction.action_id, "action-2");
      assert.equal(shell._eebusActiveActionTimer, action2Timer);
      assert.equal(action2Timer.cleared, false);
      assert.equal(shell._eebusActiveActionRetries, 1);
    });
  }
});

test("terminal PIN outcomes and action expiry abort every pairing authority", async (t) => {
  for (const entry of [
    { name: "pin required", action: { action_id: "action-secret", state: "terminal", outcome: "pin_required", expiry: new Date(Date.now() + 60_000).toISOString() } },
    { name: "pin optional", action: { action_id: "action-secret", state: "terminal", outcome: "pin_optional", expiry: new Date(Date.now() + 60_000).toISOString() } },
    { name: "expired", action: { action_id: "action-secret", state: "running", expiry: new Date(Date.now() - 1).toISOString() } },
  ]) {
    await t.test(entry.name, async () => {
      const elements = issue848PairingElements();
      const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
      primeIssue848PairingAuthority(shell);
      shell.renderEEBusActiveAction(entry.action, "action-secret");
      assertIssue848PairingAuthorityCleared(shell, elements);
    });
  }

  await t.test("stale terminal cannot abort the next selection", async () => {
    const elements = issue848PairingElements();
    const { shell } = await issue817Shell(async () => response({ state_revision: 1, data: {} }), elements);
    shell._eebusActiveActionID = "action-2";
    shell._eebusSelection = { id: "selection-2" };
    shell._eebusPINRequested = true;
    shell.renderEEBusActiveAction({ action_id: "action-1", state: "terminal", outcome: "pin_required", expiry: new Date(Date.now() + 60_000).toISOString() }, "action-1");
    assert.equal(shell._eebusActiveActionID, "action-2");
    assert.equal(shell._eebusSelection.id, "selection-2");
    assert.equal(shell._eebusPINRequested, true);
  });
});
