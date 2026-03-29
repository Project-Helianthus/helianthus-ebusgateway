// Generated from portal/web/src/app.js. DO NOT EDIT.
const THEME_KEY = "helianthus-portal-theme";

function loadTheme() {
  const stored = localStorage.getItem(THEME_KEY);
  if (stored === "light" || stored === "dark") {
    return stored;
  }
  return "dark";
}

function setTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem(THEME_KEY, theme);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatFixed(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return number.toFixed(digits);
}

function formatTemperature(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return `${number.toFixed(1)}°C`;
}

function formatPercent(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return `${number.toFixed(0)}%`;
}

function formatToggle(value) {
  if (value === true) {
    return "on";
  }
  if (value === false) {
    return "off";
  }
  return "n/a";
}

function formatYesNo(value) {
  if (value === true) {
    return "yes";
  }
  if (value === false) {
    return "no";
  }
  return "n/a";
}

function formatInteger(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return String(Math.round(number));
}

function formatSeriesYearly(values, digits = 2) {
  if (!Array.isArray(values) || values.length === 0) {
    return "n/a";
  }
  return values.map((value) => formatFixed(value, digits)).join(", ");
}

function formatEnergyMeta(meta) {
  if (!meta || typeof meta !== "object") {
    return "state=n/a source=n/a";
  }
  const state = String(meta.freshness_state || "never_seen");
  const source = String(meta.provenance || "none");
  const age = Number(meta.age_seconds);
  const ageLabel = Number.isFinite(age) ? ` age=${formatFixed(age, 0)}s` : "";
  const stale = meta.stale === true ? " stale=yes" : " stale=no";
  return `state=${state} source=${source}${ageLabel}${stale}`;
}

function formatAddress(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "0x??";
  }
  return `0x${number.toString(16).padStart(2, "0")}`;
}

function isValidHex(str, maxNibbles) {
  if (!str || str.length === 0 || str.length > maxNibbles) return false;
  return /^[0-9a-fA-F]+$/.test(str);
}

function explorerDecode(rawHex, rawLen, type) {
  if (!rawHex || rawLen === 0) return "";
  if (rawHex.length % 2 !== 0) return rawHex;
  const bytes = new Uint8Array(rawHex.match(/.{1,2}/g).map((b) => parseInt(b, 16)));
  const view = new DataView(bytes.buffer);
  try {
    switch (type) {
      case "exp":
        return rawLen >= 4 ? view.getFloat32(0, true).toFixed(4) : "n/a (<4B)";
      case "ulg":
        return rawLen >= 4 ? String(view.getUint32(0, true)) : "n/a (<4B)";
      case "uin":
        return rawLen >= 2 ? String(view.getUint16(0, true)) : "n/a (<2B)";
      case "sin":
        return rawLen >= 2 ? String(view.getInt16(0, true)) : "n/a (<2B)";
      case "uch":
        return rawLen >= 1 ? String(view.getUint8(0)) : "n/a";
      case "sch":
        return rawLen >= 1 ? String(view.getInt8(0)) : "n/a";
      case "str": {
        const decoder = new TextDecoder("utf-8", { fatal: false });
        const text = decoder.decode(bytes);
        return text.replace(/\0.*$/, "");
      }
      case "hex":
      default:
        return rawHex;
    }
  } catch {
    return rawHex;
  }
}

class PortalShell extends HTMLElement {
  connectedCallback() {
    this.render();
    setTheme(loadTheme());
    this.bindEvents();
    this.loadStatus();
  }

  disconnectedCallback() {
    if (this.streamSource) {
      this.streamSource.close();
      this.streamSource = undefined;
    }
    if (this.timelineInterval) {
      clearInterval(this.timelineInterval);
      this.timelineInterval = undefined;
    }
    if (this.timelineTimer) {
      clearTimeout(this.timelineTimer);
      this.timelineTimer = undefined;
    }
    if (this.adapterInfoInterval) {
      clearInterval(this.adapterInfoInterval);
      this.adapterInfoInterval = undefined;
    }
    if (this.provenanceInterval) {
      clearInterval(this.provenanceInterval);
      this.provenanceInterval = undefined;
    }
    if (this.provenanceTimer) {
      clearTimeout(this.provenanceTimer);
      this.provenanceTimer = undefined;
    }
    if (this.snapshotInterval) {
      clearInterval(this.snapshotInterval);
      this.snapshotInterval = undefined;
    }
    if (this.busObservabilityInterval) {
      clearInterval(this.busObservabilityInterval);
      this.busObservabilityInterval = undefined;
    }
    if (this._explorerPollTimer) {
      clearInterval(this._explorerPollTimer);
      this._explorerPollTimer = undefined;
    }
    if (this._explorerSSE) {
      this._explorerSSE.close();
      this._explorerSSE = undefined;
    }
  }

  bindEvents() {
    const toggle = this.querySelector('[data-role="theme-toggle"]');
    const search = this.querySelector('[data-role="search-input"]');
    const correlation = this.querySelector('[data-role="timeline-correlation"]');
    const provenanceCorrelation = this.querySelector('[data-role="provenance-correlation"]');
    const captureButton = this.querySelector('[data-role="snapshot-capture"]');
    const retentionButton = this.querySelector('[data-role="snapshot-retention-apply"]');
    const diffButton = this.querySelector('[data-role="snapshot-diff-run"]');
    const sessionSave = this.querySelector('[data-role="session-save"]');
    const sessionLoad = this.querySelector('[data-role="session-load"]');
    const issueDraftButton = this.querySelector('[data-role="issue-draft-run"]');
    const issueExportButton = this.querySelector('[data-role="issue-export-run"]');
    const projectionDeviceSelect = this.querySelector('[data-role="projection-device-select"]');
    if (toggle) {
      toggle.addEventListener("click", () => {
        const current = loadTheme();
        setTheme(current === "dark" ? "light" : "dark");
      });
    }
    if (search) {
      search.addEventListener("input", () => {
        this.scheduleSearch(search.value);
      });
    }
    if (correlation) {
      correlation.addEventListener("input", () => {
        this.scheduleTimelineRefresh();
      });
    }
    if (provenanceCorrelation) {
      provenanceCorrelation.addEventListener("input", () => {
        this.scheduleProvenanceRefresh();
      });
    }
    if (captureButton) {
      captureButton.addEventListener("click", () => {
        this.captureSnapshot();
      });
    }
    if (retentionButton) {
      retentionButton.addEventListener("click", () => {
        this.updateSnapshotRetention();
      });
    }
    if (diffButton) {
      diffButton.addEventListener("click", () => {
        this.runSnapshotDiff();
      });
    }
    if (sessionSave) {
      sessionSave.addEventListener("click", () => {
        this.saveSession();
      });
    }
    if (sessionLoad) {
      sessionLoad.addEventListener("click", () => {
        this.loadSession();
      });
    }
    if (issueDraftButton) {
      issueDraftButton.addEventListener("click", () => {
        this.generateIssueDraft();
      });
    }
    if (issueExportButton) {
      issueExportButton.addEventListener("click", () => {
        this.generateIssueExport();
      });
    }
    if (projectionDeviceSelect) {
      projectionDeviceSelect.addEventListener("change", () => {
        this.loadAllProjectionPlanes();
      });
    }
    this.querySelectorAll("[data-nav-target]").forEach((button) => {
      button.addEventListener("click", () => {
        const targetID = button.getAttribute("data-nav-target");
        if (!targetID) {
          return;
        }
        this.activateSection(targetID);
      });
    });
    this.bindExplorerEvents();
  }

  activateSection(targetID) {
    const sectionMap = {
      "section-registry": ["section-registry"],
      "section-semantic": ["section-semantic"],
      "section-bus": ["section-bus"],
      "section-projection": ["section-projection"],
      "section-explorer": ["section-explorer"],
      "section-adapter": ["section-adapter"],
      "section-timeline": ["section-timeline", "section-provenance"],
      "section-snapshots": ["section-snapshots", "section-snapshot-diff", "section-sessions"],
      "section-issue-builder": ["section-issue-builder"],
    };
    const visible = new Set(sectionMap[targetID] || [targetID]);
    visible.add("section-search");
    this.querySelectorAll("main .registry-preview").forEach((section) => {
      const id = section.id || section.getAttribute("data-section");
      section.style.display = visible.has(id) ? "" : "none";
    });
    this.querySelectorAll("[data-nav-target]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-nav-target") === targetID);
    });
  }

  async loadStatus() {
    const statusEl = this.querySelector('[data-role="status"]');
    const metaEl = this.querySelector('[data-role="meta"]');
    const listEl = this.querySelector('[data-role="registry-list"]');
    const semanticEl = this.querySelector('[data-role="semantic-list"]');
    const busObservabilityEl = this.querySelector('[data-role="bus-observability"]');
    const projectionEl = this.querySelector('[data-role="projection-list"]');
    const searchInput = this.querySelector('[data-role="search-input"]');
    const searchList = this.querySelector('[data-role="search-list"]');
    const timelineList = this.querySelector('[data-role="timeline-list"]');
    const provenanceList = this.querySelector('[data-role="provenance-list"]');
    const snapshotsList = this.querySelector('[data-role="snapshots-list"]');
    const retentionInput = this.querySelector('[data-role="snapshot-retention"]');
    const diffList = this.querySelector('[data-role="snapshot-diff-list"]');
    const sessionsList = this.querySelector('[data-role="sessions-list"]');
    const issuePreview = this.querySelector('[data-role="issue-preview"]');
    try {
      const [healthRes, bootstrapRes] = await Promise.all([
        fetch("api/v1/health"),
        fetch("api/v1/bootstrap"),
      ]);
      const health = await healthRes.json();
      const bootstrap = await bootstrapRes.json();
      if (statusEl) {
        statusEl.textContent = `Gateway ${health.status}`;
      }
      const capabilities = bootstrap.capabilities || {};
      this.applyCapabilityState(capabilities);
      const firstEnabled = this.querySelector("[data-nav-target]:not([disabled])");
      if (firstEnabled) {
        this.activateSection(firstEnabled.getAttribute("data-nav-target"));
      }
      if (metaEl) {
        const caps = capabilities;
        const enabled = Object.keys(caps).filter((key) => caps[key]).length;
        metaEl.textContent =
          `Portal capabilities: ${enabled}/${Object.keys(caps).length || 0} enabled. GraphQL=${bootstrap.endpoints?.graphql || "n/a"}`;
      }
      if (searchInput) {
        searchInput.disabled = !capabilities.search;
        searchInput.title = capabilities.search ? "" : "Search is unavailable (no data providers)";
      }
      if (capabilities.registry && listEl) {
        await this.loadRegistryPreview(listEl);
      }
      if (capabilities.semantic && semanticEl) {
        await this.loadSemanticPreview(semanticEl);
      }
      if (busObservabilityEl) {
        busObservabilityEl.innerHTML = capabilities.bus_observability
          ? "<li>Loading bus observability...</li>"
          : "<li>Bus observability unavailable.</li>";
      }
      if (capabilities.bus_observability) {
        await this.refreshBusObservability();
        if (this.busObservabilityInterval) {
          clearInterval(this.busObservabilityInterval);
        }
        this.busObservabilityInterval = setInterval(() => {
          this.refreshBusObservability();
        }, 3000);
      }
      if (capabilities.projection && projectionEl) {
        await this.loadProjectionPreview(projectionEl);
      }
      if (searchList) {
        searchList.innerHTML = capabilities.search
          ? "<li>Type at least 2 characters to search across registry, semantic and projection layers.</li>"
          : "<li>Search unavailable: no readable layers enabled.</li>";
      }
      if (capabilities.stream) {
        this.startStream();
      }
      if (timelineList) {
        timelineList.innerHTML = capabilities.timeline
          ? "<li>Loading timeline events...</li>"
          : "<li>Timeline unavailable: stream capability disabled.</li>";
      }
      if (capabilities.timeline) {
        await this.refreshTimeline();
        if (this.timelineInterval) {
          clearInterval(this.timelineInterval);
        }
        this.timelineInterval = setInterval(() => {
          this.refreshTimeline();
        }, 3000);
      }
      if (provenanceList) {
        provenanceList.innerHTML = capabilities.provenance
          ? "<li>Loading provenance records...</li>"
          : "<li>Provenance unavailable: stream capability disabled.</li>";
      }
      if (capabilities.provenance) {
        await this.refreshProvenance();
        if (this.provenanceInterval) {
          clearInterval(this.provenanceInterval);
        }
        this.provenanceInterval = setInterval(() => {
          this.refreshProvenance();
        }, 4000);
      }
      if (snapshotsList) {
        snapshotsList.innerHTML = capabilities.snapshots
          ? "<li>Loading snapshot store...</li>"
          : "<li>Snapshots unavailable: stream capability disabled.</li>";
      }
      if (capabilities.snapshots) {
        await this.refreshSnapshots();
        const retention = await this.fetchSnapshotRetention();
        if (retentionInput && retention > 0) {
          retentionInput.value = String(retention);
        }
        if (this.snapshotInterval) {
          clearInterval(this.snapshotInterval);
        }
        this.snapshotInterval = setInterval(() => {
          this.refreshSnapshots();
        }, 5000);
      }
      if (diffList) {
        diffList.innerHTML = capabilities.snapshot_diff
          ? "<li>Select snapshot IDs and run diff.</li>"
          : "<li>Snapshot diff unavailable.</li>";
      }
      if (sessionsList) {
        sessionsList.innerHTML = capabilities.sessions
          ? "<li>Loading sessions...</li>"
          : "<li>Sessions unavailable.</li>";
      }
      if (capabilities.sessions) {
        await this.refreshSessions();
      }
      if (issuePreview) {
        issuePreview.textContent = capabilities.issue_builder
          ? "Issue builder ready. Fill fields and generate draft."
          : "Issue builder unavailable.";
      }
      if (capabilities.explorer) {
        this.initExplorer();
      }
      if (capabilities.semantic) {
        await this.refreshAdapterInfo();
        if (this.adapterInfoInterval) {
          clearInterval(this.adapterInfoInterval);
        }
        this.adapterInfoInterval = setInterval(() => {
          this.refreshAdapterInfo();
        }, 30000);
      }
    } catch (err) {
      if (statusEl) {
        statusEl.textContent = "Gateway unavailable";
      }
      if (metaEl) {
        metaEl.textContent = "Bootstrap fetch failed";
      }
      console.error("portal bootstrap failed", err);
    }
  }

  applyCapabilityState(capabilities) {
    const cap = capabilities || {};
    this.setNavState("registry", cap.registry);
    this.setNavState("semantic", cap.semantic);
    this.setNavState("bus", cap.bus_observability);
    this.setNavState("projection", cap.projection);
    this.setNavState("explorer", cap.explorer);
    this.setNavState("adapter", cap.semantic);
    this.setNavState("timeline", cap.timeline || cap.provenance);
    this.setNavState("snapshots", cap.snapshots || cap.snapshot_diff);
    this.setNavState("issue-builder", cap.issue_builder);
  }

  setNavState(name, enabled) {
    const button = this.querySelector(`[data-role="nav-${name}"]`);
    if (!button) {
      return;
    }
    button.disabled = !enabled;
    button.classList.toggle("enabled", Boolean(enabled));
    const bullet = button.querySelector(".nav-bullet");
    if (bullet) {
      bullet.classList.toggle("available", Boolean(enabled));
      bullet.classList.toggle("unavailable", !enabled);
    }
  }

  async refreshBusObservability() {
    const listEl = this.querySelector('[data-role="bus-observability"]');
    const bannerEl = this.querySelector('[data-role="bus-banner"]');
    if (!listEl || !bannerEl) {
      return;
    }
    try {
      const response = await fetch("api/v1/bus/observability");
      if (!response.ok) {
        throw new Error(`status ${response.status}`);
      }
      const payload = await response.json();
      const summary = payload && payload.status ? payload : payload?.summary;
      if (!summary || !summary.status) {
        throw new Error("missing status");
      }
      const status = summary.status || {};
      const capability = status.capability || {};
      const warmup = status.warmup || {};
      const degraded = status.degraded || {};
      const featureFlags = status.feature_flags || {};
      const transportClass = String(status.transport_class || "unknown");
      const passiveState = String(warmup.state || capability.passive_state || "unknown");
      const warmupState = passiveState === "warming_up";
      const degradedState = !warmupState && degraded.active === true;
      const unavailableState =
        !warmupState &&
        !degradedState &&
        (passiveState === "unavailable" || capability.passive_supported === false);
      const viewState = warmupState
        ? "warming_up"
        : degradedState
          ? "degraded"
          : unavailableState
            ? "unavailable"
            : "available";

      const stateLabel = {
        available: "Passive observability available",
        warming_up: "Passive warmup in progress",
        degraded: "Observability degraded",
        unavailable: "Passive observability unavailable",
      }[viewState] || "Passive observability state unknown";

      const reasons = Array.isArray(degraded.reasons) ? degraded.reasons : [];
      const endpoint = capability.endpoint_state ? ` endpoint=${capability.endpoint_state}` : "";
      const connected = capability.tap_connected === true ? " connected" : " disconnected";
      let bannerText = `${stateLabel} (${transportClass}${endpoint}${connected})`;
      if (transportClass === "ebusd-tcp" && (viewState === "degraded" || viewState === "unavailable")) {
        bannerText += " | ebusd-tcp transport limits passive observe-first coverage.";
      }
      bannerEl.className = `bus-banner bus-state-${viewState}`;
      bannerEl.textContent = bannerText;

      const elapsedSeconds = Number(warmup.elapsed_seconds);
      const elapsedLabel = Number.isFinite(elapsedSeconds) ? `${formatFixed(elapsedSeconds, 1)}s` : "n/a";
      const completedTransactions = formatInteger(warmup.completed_transactions);
      const requiredTransactions = formatInteger(warmup.required_transactions);
      const passiveReason = capability.passive_reason ? String(capability.passive_reason) : "none";
      const warmupBlocker = warmup.blocker ? String(warmup.blocker) : "none";
      const completionMode = warmup.completion_mode ? String(warmup.completion_mode) : "n/a";
      const normalization = Array.isArray(featureFlags.normalizations) && featureFlags.normalizations.length > 0
        ? featureFlags.normalizations.join(", ")
        : "none";

      const rows = [
        `transport=${escapeHtml(transportClass)} passive_supported=${escapeHtml(formatYesNo(capability.passive_supported))} passive_available=${escapeHtml(formatYesNo(capability.passive_available))}`,
        `passive_state=${escapeHtml(passiveState)} passive_reason=${escapeHtml(passiveReason)}`,
        `warmup blocker=${escapeHtml(warmupBlocker)} elapsed=${escapeHtml(elapsedLabel)} transactions=${escapeHtml(completedTransactions)}/${escapeHtml(requiredTransactions)} completion_mode=${escapeHtml(completionMode)}`,
        `feature_flags observe_first_enabled=${escapeHtml(formatYesNo(featureFlags.observe_first_enabled))} state_direct_apply=${escapeHtml(formatYesNo(featureFlags.passive_state_direct_apply))} config_direct_apply=${escapeHtml(formatYesNo(featureFlags.passive_config_direct_apply))} external_write_policy=${escapeHtml(String(featureFlags.external_write_policy || "n/a"))} normalizations=${escapeHtml(normalization)}`,
      ];
      if (reasons.length > 0) {
        rows.push(`degraded_reasons=${escapeHtml(reasons.join(", "))}`);
      }
      listEl.innerHTML = rows.map((row) => `<li>${row}</li>`).join("");
    } catch (err) {
      bannerEl.className = "bus-banner bus-state-unavailable";
      bannerEl.textContent = "Bus observability endpoint unavailable";
      listEl.innerHTML = "<li>Bus observability fetch failed.</li>";
      console.error("bus observability query failed", err);
    }
  }

  startStream() {
    const streamStatus = this.querySelector('[data-role="stream-status"]');
    if (this.streamSource) {
      this.streamSource.close();
      this.streamSource = undefined;
    }
    const source = new EventSource("api/v1/stream?max_events_per_second=2&interval_ms=1000");
    this.streamSource = source;
    source.addEventListener("update", (event) => {
      if (!streamStatus) {
        return;
      }
      try {
        const payload = JSON.parse(event.data);
        const layer = payload.layer || "unknown";
        const at = payload.at || "n/a";
        streamStatus.textContent = `Stream live: layer=${layer} at=${at}`;
        this.scheduleTimelineRefresh();
        this.scheduleProvenanceRefresh();
        this.refreshBusObservability();
        this.refreshSnapshots();
      } catch (err) {
        streamStatus.textContent = "Stream payload parse error";
      }
    });
    source.onerror = () => {
      if (streamStatus) {
        streamStatus.textContent = "Stream disconnected";
      }
    };
  }

  scheduleTimelineRefresh() {
    if (this.timelineTimer) {
      clearTimeout(this.timelineTimer);
    }
    this.timelineTimer = setTimeout(() => {
      this.refreshTimeline();
    }, 220);
  }

  async refreshTimeline() {
    const timelineList = this.querySelector('[data-role="timeline-list"]');
    const correlationInput = this.querySelector('[data-role="timeline-correlation"]');
    if (!timelineList) {
      return;
    }
    try {
      const correlation = correlationInput ? String(correlationInput.value || "").trim() : "";
      const query = new URLSearchParams();
      query.set("limit", "8");
      if (correlation.length > 0) {
        query.set("correlation_id", correlation);
      }
      const response = await fetch(`api/v1/timeline/events?${query.toString()}`);
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        timelineList.innerHTML = "<li>No timeline events yet.</li>";
        return;
      }
      timelineList.innerHTML = items
        .map((item) => {
          const layer = escapeHtml(item.layer || "unknown");
          const corr = escapeHtml(item.correlation_id || "n/a");
          const at = escapeHtml(item.at || "n/a");
          const hasPayload = item.payload && Object.keys(item.payload).length > 0;
          const payloadBlock = hasPayload
            ? `<details><summary>payload</summary><pre style="max-height:200px;overflow:auto;font-size:0.85em">${escapeHtml(JSON.stringify(item.payload, null, 2))}</pre></details>`
            : "";
          return `<li><span class="pill">${layer}</span> <strong>${corr}</strong> <span class="muted-inline">${at}</span>${payloadBlock}</li>`;
        })
        .join("");
    } catch (err) {
      timelineList.innerHTML = "<li>Timeline query failed.</li>";
      console.error("timeline query failed", err);
    }
  }

  scheduleProvenanceRefresh() {
    if (this.provenanceTimer) {
      clearTimeout(this.provenanceTimer);
    }
    this.provenanceTimer = setTimeout(() => {
      this.refreshProvenance();
    }, 220);
  }

  async refreshProvenance() {
    const provenanceList = this.querySelector('[data-role="provenance-list"]');
    const provenanceCorrelation = this.querySelector('[data-role="provenance-correlation"]');
    if (!provenanceList) {
      return;
    }
    try {
      const correlation = provenanceCorrelation ? String(provenanceCorrelation.value || "").trim() : "";
      const query = new URLSearchParams();
      query.set("limit", "8");
      if (correlation.length > 0) {
        query.set("correlation_id", correlation);
      }
      const response = await fetch(`api/v1/provenance/events?${query.toString()}`);
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        provenanceList.innerHTML = "<li>No provenance records yet.</li>";
        return;
      }
      provenanceList.innerHTML = items
        .map((item) => {
          const source = escapeHtml(item.source || "unknown");
          const corr = escapeHtml(item.correlation_id || "n/a");
          const keys = Array.isArray(item.payload_keys) ? item.payload_keys.join(",") : "";
          const confidence = Number(item.confidence || 0).toFixed(2);
          const decodePath = Array.isArray(item.decode_path) && item.decode_path.length > 0
            ? ` path=${escapeHtml(item.decode_path.join(" → "))}`
            : "";
          return `<li><span class="pill">prov</span> <strong>${corr}</strong> <span class="muted-inline">${source} keys=${escapeHtml(keys)} conf=${confidence}${decodePath}</span></li>`;
        })
        .join("");
    } catch (err) {
      provenanceList.innerHTML = "<li>Provenance query failed.</li>";
      console.error("provenance query failed", err);
    }
  }

  async fetchSnapshotRetention() {
    try {
      const response = await fetch("api/v1/snapshots/retention");
      const payload = await response.json();
      return Number(payload.max_snapshots || 0);
    } catch (err) {
      return 0;
    }
  }

  async refreshSnapshots() {
    const list = this.querySelector('[data-role="snapshots-list"]');
    if (!list) {
      return;
    }
    try {
      const response = await fetch("api/v1/snapshots?limit=6");
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        list.innerHTML = "<li>No snapshots captured yet.</li>";
        return;
      }
      list.innerHTML = items
        .map((item) => {
          const id = escapeHtml(item.id || "n/a");
          const label = escapeHtml(item.label || "snapshot");
          const at = escapeHtml(item.captured_at || "n/a");
          return `<li><span class="pill">snap</span> <a href="#" class="snapshot-view-link" data-snapshot-id="${id}"><strong>${id}</strong></a> <span class="muted-inline">${label} ${at}</span></li>`;
        })
        .join("");
      list.querySelectorAll(".snapshot-view-link").forEach((link) => {
        link.addEventListener("click", (e) => {
          e.preventDefault();
          this.viewSnapshot(link.dataset.snapshotId);
        });
      });

      const fromInput = this.querySelector('[data-role="snapshot-diff-from"]');
      const toInput = this.querySelector('[data-role="snapshot-diff-to"]');
      if (toInput && !String(toInput.value || "").trim() && items[0]?.id) {
        toInput.value = String(items[0].id);
      }
      if (fromInput && !String(fromInput.value || "").trim() && items[1]?.id) {
        fromInput.value = String(items[1].id);
      }
    } catch (err) {
      list.innerHTML = "<li>Snapshot list query failed.</li>";
      console.error("snapshot query failed", err);
    }
  }

  async viewSnapshot(id) {
    const list = this.querySelector('[data-role="snapshots-list"]');
    if (!list) {
      return;
    }
    if (this._snapshotViewPending) {
      return;
    }
    const existing = list.querySelector(`[data-snapshot-content="${id}"]`);
    if (existing) {
      existing.remove();
      return;
    }
    this._snapshotViewPending = true;
    try {
      const response = await fetch(`api/v1/snapshots/view?id=${encodeURIComponent(id)}`);
      if (!response.ok) {
        return;
      }
      const snapshot = await response.json();
      if (list.querySelector(`[data-snapshot-content="${id}"]`)) {
        return;
      }
      const link = list.querySelector(`[data-snapshot-id="${id}"]`);
      if (!link) {
        return;
      }
      const li = link.closest("li");
      if (!li) {
        return;
      }
      const detail = document.createElement("li");
      detail.setAttribute("data-snapshot-content", id);
      detail.innerHTML = `<details open><summary>Snapshot ${escapeHtml(id)} payload</summary><pre style="max-height:300px;overflow:auto;font-size:0.85em">${escapeHtml(JSON.stringify(snapshot.payload, null, 2))}</pre></details>`;
      li.after(detail);
    } catch (err) {
      console.error("snapshot view failed", err);
    } finally {
      this._snapshotViewPending = false;
    }
  }

  async captureSnapshot() {
    const labelInput = this.querySelector('[data-role="snapshot-label"]');
    const streamStatus = this.querySelector('[data-role="stream-status"]');
    const label = labelInput ? String(labelInput.value || "").trim() : "";
    try {
      const query = new URLSearchParams();
      if (label.length > 0) {
        query.set("label", label);
      }
      const response = await fetch(`api/v1/snapshots/capture?${query.toString()}`);
      const payload = await response.json();
      if (streamStatus) {
        const snapID = payload.snapshot?.id || "unknown";
        streamStatus.textContent = `Snapshot captured: ${snapID}`;
      }
      await this.refreshSnapshots();
    } catch (err) {
      if (streamStatus) {
        streamStatus.textContent = "Snapshot capture failed";
      }
    }
  }

  async updateSnapshotRetention() {
    const retentionInput = this.querySelector('[data-role="snapshot-retention"]');
    const streamStatus = this.querySelector('[data-role="stream-status"]');
    if (!retentionInput) {
      return;
    }
    const value = String(retentionInput.value || "").trim();
    try {
      const response = await fetch(`api/v1/snapshots/retention?max_snapshots=${encodeURIComponent(value)}`);
      const payload = await response.json();
      if (streamStatus) {
        streamStatus.textContent = `Snapshot retention=${payload.max_snapshots}`;
      }
      await this.refreshSnapshots();
    } catch (err) {
      if (streamStatus) {
        streamStatus.textContent = "Snapshot retention update failed";
      }
    }
  }

  async runSnapshotDiff() {
    const list = this.querySelector('[data-role="snapshot-diff-list"]');
    const fromInput = this.querySelector('[data-role="snapshot-diff-from"]');
    const toInput = this.querySelector('[data-role="snapshot-diff-to"]');
    if (!list) {
      return;
    }
    try {
      const query = new URLSearchParams();
      query.set("limit", "12");
      const from = fromInput ? String(fromInput.value || "").trim() : "";
      const to = toInput ? String(toInput.value || "").trim() : "";
      if (from.length > 0) {
        query.set("from_id", from);
      }
      if (to.length > 0) {
        query.set("to_id", to);
      }
      const response = await fetch(`api/v1/snapshots/diff?${query.toString()}`);
      if (!response.ok) {
        list.innerHTML = `<li>Snapshot diff unavailable (${response.status}).</li>`;
        return;
      }
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        list.innerHTML = "<li>No diff changes detected.</li>";
        return;
      }
      list.innerHTML = items
        .map((item) => {
          const path = escapeHtml(item.path || "");
          const change = escapeHtml(item.change || "changed");
          const fromVal = escapeHtml(item.from || "");
          const toVal = escapeHtml(item.to || "");
          return `<li><span class="pill">${change}</span> <strong>${path}</strong> <span class="muted-inline">${fromVal} -> ${toVal}</span></li>`;
        })
        .join("");
    } catch (err) {
      list.innerHTML = "<li>Snapshot diff failed.</li>";
      console.error("snapshot diff failed", err);
    }
  }

  async refreshSessions() {
    const sessionsList = this.querySelector('[data-role="sessions-list"]');
    if (!sessionsList) {
      return;
    }
    try {
      const response = await fetch("api/v1/sessions?limit=8");
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        sessionsList.innerHTML = "<li>No saved sessions.</li>";
        return;
      }
      sessionsList.innerHTML = items
        .map((item) => {
          const id = escapeHtml(item.id || "n/a");
          const name = escapeHtml(item.name || "session");
          const updated = escapeHtml(item.updated_at || "");
          return `<li><span class="pill">sess</span> <strong>${id}</strong> <span class="muted-inline">${name} ${updated}</span></li>`;
        })
        .join("");
    } catch (err) {
      sessionsList.innerHTML = "<li>Sessions query failed.</li>";
      console.error("sessions query failed", err);
    }
  }

  async saveSession() {
    const nameInput = this.querySelector('[data-role="session-name"]');
    const streamStatus = this.querySelector('[data-role="stream-status"]');
    const searchInput = this.querySelector('[data-role="search-input"]');
    const timelineInput = this.querySelector('[data-role="timeline-correlation"]');
    const provenanceInput = this.querySelector('[data-role="provenance-correlation"]');
    const fromInput = this.querySelector('[data-role="snapshot-diff-from"]');
    const toInput = this.querySelector('[data-role="snapshot-diff-to"]');

    const query = new URLSearchParams();
    query.set("name", nameInput ? String(nameInput.value || "").trim() : "");
    query.set("search_query", searchInput ? String(searchInput.value || "").trim() : "");
    query.set("timeline_correlation", timelineInput ? String(timelineInput.value || "").trim() : "");
    query.set("provenance_correlation", provenanceInput ? String(provenanceInput.value || "").trim() : "");
    query.set("snapshot_from_id", fromInput ? String(fromInput.value || "").trim() : "");
    query.set("snapshot_to_id", toInput ? String(toInput.value || "").trim() : "");

    try {
      const response = await fetch(`api/v1/sessions/save?${query.toString()}`);
      const payload = await response.json();
      const id = payload.session?.id || "unknown";
      const loadInput = this.querySelector('[data-role="session-load-id"]');
      if (loadInput) {
        loadInput.value = id;
      }
      if (streamStatus) {
        streamStatus.textContent = `Session saved: ${id}`;
      }
      await this.refreshSessions();
    } catch (err) {
      if (streamStatus) {
        streamStatus.textContent = "Session save failed";
      }
    }
  }

  async loadSession() {
    const loadInput = this.querySelector('[data-role="session-load-id"]');
    const streamStatus = this.querySelector('[data-role="stream-status"]');
    const sessionID = loadInput ? String(loadInput.value || "").trim() : "";
    if (!sessionID) {
      if (streamStatus) {
        streamStatus.textContent = "Session id missing";
      }
      return;
    }
    try {
      const response = await fetch(`api/v1/sessions/load?id=${encodeURIComponent(sessionID)}`);
      if (!response.ok) {
        if (streamStatus) {
          streamStatus.textContent = `Session load failed (${response.status})`;
        }
        return;
      }
      const payload = await response.json();
      const state = payload.session?.state || {};
      const searchInput = this.querySelector('[data-role="search-input"]');
      const timelineInput = this.querySelector('[data-role="timeline-correlation"]');
      const provenanceInput = this.querySelector('[data-role="provenance-correlation"]');
      const fromInput = this.querySelector('[data-role="snapshot-diff-from"]');
      const toInput = this.querySelector('[data-role="snapshot-diff-to"]');
      if (searchInput) searchInput.value = state.search_query || "";
      if (timelineInput) timelineInput.value = state.timeline_correlation || "";
      if (provenanceInput) provenanceInput.value = state.provenance_correlation || "";
      if (fromInput) fromInput.value = state.snapshot_from_id || "";
      if (toInput) toInput.value = state.snapshot_to_id || "";

      this.scheduleSearch(searchInput ? searchInput.value : "");
      this.scheduleTimelineRefresh();
      this.scheduleProvenanceRefresh();
      this.runSnapshotDiff();

      if (streamStatus) {
        streamStatus.textContent = `Session loaded: ${sessionID}`;
      }
    } catch (err) {
      if (streamStatus) {
        streamStatus.textContent = "Session load failed";
      }
    }
  }

  async generateIssueDraft() {
    const titleInput = this.querySelector('[data-role="issue-title"]');
    const obsInput = this.querySelector('[data-role="issue-observation"]');
    const hypoInput = this.querySelector('[data-role="issue-hypothesis"]');
    const preview = this.querySelector('[data-role="issue-preview"]');
    try {
      const query = new URLSearchParams();
      query.set("title", titleInput ? String(titleInput.value || "").trim() : "");
      query.set("observation", obsInput ? String(obsInput.value || "").trim() : "");
      query.set("hypothesis", hypoInput ? String(hypoInput.value || "").trim() : "");
      const response = await fetch(`api/v1/issues/draft?${query.toString()}`);
      const payload = await response.json();
      if (preview) {
        preview.textContent = payload.markdown || "Draft generation failed";
      }
    } catch (err) {
      if (preview) {
        preview.textContent = "Draft generation failed";
      }
    }
  }

  async generateIssueExport() {
    const titleInput = this.querySelector('[data-role="issue-title"]');
    const preview = this.querySelector('[data-role="issue-preview"]');
    try {
      const query = new URLSearchParams();
      query.set("title", titleInput ? String(titleInput.value || "").trim() : "");
      const response = await fetch(`api/v1/issues/export?${query.toString()}`);
      const payload = await response.json();
      if (preview) {
        preview.textContent = JSON.stringify(payload, null, 2);
      }
    } catch (err) {
      if (preview) {
        preview.textContent = "Issue export failed";
      }
    }
  }

  scheduleSearch(rawQuery) {
    if (this.searchTimer) {
      clearTimeout(this.searchTimer);
    }
    this.searchTimer = setTimeout(() => {
      this.runSearch(rawQuery);
    }, 220);
  }

  async runSearch(rawQuery) {
    const listEl = this.querySelector('[data-role="search-list"]');
    if (!listEl) {
      return;
    }
    const query = String(rawQuery || "").trim();
    if (query.length < 2) {
      listEl.innerHTML = "<li>Type at least 2 characters.</li>";
      return;
    }
    try {
      const response = await fetch(`api/v1/search?q=${encodeURIComponent(query)}&limit=20`);
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        listEl.innerHTML = "<li>No matches.</li>";
        return;
      }
      listEl.innerHTML = items
        .map((item) => {
          const layer = escapeHtml(item.layer || "unknown");
          const title = escapeHtml(item.title || item.id || "item");
          const subtitle = item.subtitle ? ` <span class="muted-inline">${escapeHtml(item.subtitle)}</span>` : "";
          return `<li><span class="pill">${layer}</span> <strong>${title}</strong>${subtitle}</li>`;
        })
        .join("");
    } catch (err) {
      listEl.innerHTML = "<li>Search request failed.</li>";
      console.error("search failed", err);
    }
  }

  async loadRegistryPreview(listEl) {
    try {
      const response = await fetch("api/v1/registry/devices?limit=8");
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        listEl.innerHTML = "<li>No devices discovered yet.</li>";
        return;
      }
      listEl.innerHTML = items
        .map((item) => {
          const addrs = (Array.isArray(item.addresses) && item.addresses.length > 0) ? item.addresses : [item.address];
          const sorted = addrs.map(Number).sort((a, b) => a - b);
          let addrStr;
          if (sorted.length === 2 && sorted[1] - sorted[0] === 5) {
            addrStr = `0x${sorted[0].toString(16).padStart(2, "0")}(M) / 0x${sorted[1].toString(16).padStart(2, "0")}(S)`;
          } else {
            addrStr = sorted.map((a) => `0x${a.toString(16).padStart(2, "0")}`).join(" / ");
          }
          const label = escapeHtml(item.display_name || item.device_id || "unknown");
          const vendor = escapeHtml(item.manufacturer || "unknown");
          const role = item.role ? ` role=${escapeHtml(item.role)}` : "";
          const sw = item.software_version ? ` sw=${escapeHtml(item.software_version)}` : "";
          const hw = item.hardware_version ? ` hw=${escapeHtml(item.hardware_version)}` : "";
          return `<li><strong>${addrStr}</strong> ${vendor} ${label}<span class="muted-inline">${role}${sw}${hw}</span></li>`;
        })
        .join("");
    } catch (err) {
      listEl.innerHTML = "<li>Registry preview unavailable.</li>";
      console.error("registry preview failed", err);
    }
  }

  async loadSemanticPreview(listEl) {
    try {
      const response = await fetch("api/v1/semantic/snapshot");
      const payload = await response.json();
      const zones = Array.isArray(payload.zones) ? payload.zones : [];
      const circuits = Array.isArray(payload.circuits) ? payload.circuits : [];
      const radioDevices = Array.isArray(payload.radio_devices) ? payload.radio_devices : [];
      const cylinders = Array.isArray(payload.cylinders) ? payload.cylinders : [];
      const dhw = payload.dhw;
      const rows = [];
      rows.push(`<li><strong>Zones detected:</strong> ${zones.length}</li>`);
      if (zones.length === 0) {
        rows.push("<li>No semantic zones available.</li>");
      } else {
        zones.forEach((zone) => {
          const state = zone.state || {};
          const config = zone.config || {};
          const name = escapeHtml(zone.name || zone.id || "zone");
          const mode = escapeHtml(config.operating_mode || "n/a");
          const preset = escapeHtml(config.preset || "n/a");
          const current = formatTemperature(state.current_temp_c);
          const target = formatTemperature(config.target_temp_c);
          const demand = formatPercent(state.heating_demand_pct);
          const hvacAction = escapeHtml(state.hvac_action || "n/a");
          const circuitType = escapeHtml(config.circuit_type || "n/a");
          rows.push(
            `<li><strong>${name}</strong> <span class="muted-inline">mode=${mode} preset=${preset} current=${escapeHtml(current)} target=${escapeHtml(target)} demand=${escapeHtml(demand)} hvac=${hvacAction} circuit=${circuitType}</span></li>`,
          );
        });
      }
      if (dhw) {
        const dhwState = dhw.state || {};
        const dhwConfig = dhw.config || {};
        const dhwPreset = dhwConfig.preset ? ` preset=${escapeHtml(dhwConfig.preset)}` : "";
        const dhwDemand = dhwState.heating_demand_pct != null
          ? ` demand=${escapeHtml(formatPercent(dhwState.heating_demand_pct))}`
          : "";
        const specialFunction = dhwState.special_function
          ? ` special=${escapeHtml(dhwState.special_function)}`
          : "";
        rows.push(`<li><strong>DHW</strong> <span class="muted-inline">mode=${escapeHtml(dhwConfig.operating_mode || "n/a")}${dhwPreset} current=${escapeHtml(formatTemperature(dhwState.current_temp_c))} target=${escapeHtml(formatTemperature(dhwConfig.target_temp_c))}${dhwDemand}${specialFunction}</span></li>`);
      }
      if (payload.boiler_status) {
        const boilerState = payload.boiler_status.state || {};
        const boilerConfig = payload.boiler_status.config || {};
        const boilerPressure = formatFixed(boilerState.water_pressure_bar, 2);
        rows.push(
          `<li><strong>Boiler</strong> <span class="muted-inline">flow=${escapeHtml(formatTemperature(boilerState.flow_temperature_c))} return=${escapeHtml(formatTemperature(boilerState.return_temperature_c))} pressure=${escapeHtml(boilerPressure === "n/a" ? boilerPressure : `${boilerPressure}bar`)} flame=${escapeHtml(formatToggle(boilerState.flame_active))} dhw_mode=${escapeHtml(boilerConfig.dhw_operating_mode || "n/a")}</span></li>`,
        );
      }
      if (payload.system) {
        const systemState = payload.system.state || {};
        const systemConfig = payload.system.config || {};
        const systemPressure = formatFixed(systemState.system_water_pressure, 2);
        rows.push(
          `<li><strong>System</strong> <span class="muted-inline">flow=${escapeHtml(formatTemperature(systemState.system_flow_temperature))} pressure=${escapeHtml(systemPressure === "n/a" ? systemPressure : `${systemPressure}bar`)} outdoor=${escapeHtml(formatTemperature(systemState.outdoor_temperature))} maintenance=${escapeHtml(formatYesNo(systemState.maintenance_due))} adaptive=${escapeHtml(formatYesNo(systemConfig.adaptive_heating_curve))}</span></li>`,
        );
      }
      if (payload.fm5_semantic_mode) {
        rows.push(`<li><strong>FM5</strong> <span class="muted-inline">mode=${escapeHtml(payload.fm5_semantic_mode)}</span></li>`);
      }
      if (payload.solar) {
        const solar = payload.solar || {};
        rows.push(
          `<li><strong>Solar</strong> <span class="muted-inline">collector=${escapeHtml(formatTemperature(solar.collector_temperature_c))} return=${escapeHtml(formatTemperature(solar.return_temperature_c))} pump=${escapeHtml(formatToggle(solar.pump_active))} yield=${escapeHtml(formatFixed(solar.current_yield, 2))}</span></li>`,
        );
      }
      if (payload.energy_totals) {
        const et = payload.energy_totals;
        rows.push(
          `<li><strong>Energy (gas)</strong> <span class="muted-inline">today climate=${escapeHtml(formatFixed(et.gas?.climate?.today, 2))} dhw=${escapeHtml(formatFixed(et.gas?.dhw?.today, 2))} yearly climate=[${escapeHtml(formatSeriesYearly(et.gas?.climate?.yearly))}] dhw=[${escapeHtml(formatSeriesYearly(et.gas?.dhw?.yearly))}] monthly climate=[${escapeHtml(formatSeriesYearly(et.gas?.climate?.monthly))}] dhw=[${escapeHtml(formatSeriesYearly(et.gas?.dhw?.monthly))}] meta climate(${escapeHtml(formatEnergyMeta(et.gas?.climate?.today_meta))}) dhw(${escapeHtml(formatEnergyMeta(et.gas?.dhw?.today_meta))})</span></li>`,
        );
        rows.push(
          `<li><strong>Energy (electric)</strong> <span class="muted-inline">today climate=${escapeHtml(formatFixed(et.electric?.climate?.today, 2))} dhw=${escapeHtml(formatFixed(et.electric?.dhw?.today, 2))} yearly climate=[${escapeHtml(formatSeriesYearly(et.electric?.climate?.yearly))}] dhw=[${escapeHtml(formatSeriesYearly(et.electric?.dhw?.yearly))}] monthly climate=[${escapeHtml(formatSeriesYearly(et.electric?.climate?.monthly))}] dhw=[${escapeHtml(formatSeriesYearly(et.electric?.dhw?.monthly))}] meta climate(${escapeHtml(formatEnergyMeta(et.electric?.climate?.today_meta))}) dhw(${escapeHtml(formatEnergyMeta(et.electric?.dhw?.today_meta))})</span></li>`,
        );
        rows.push(
          `<li><strong>Energy (solar)</strong> <span class="muted-inline">today climate=${escapeHtml(formatFixed(et.solar?.climate?.today, 2))} dhw=${escapeHtml(formatFixed(et.solar?.dhw?.today, 2))} yearly climate=[${escapeHtml(formatSeriesYearly(et.solar?.climate?.yearly))}] dhw=[${escapeHtml(formatSeriesYearly(et.solar?.dhw?.yearly))}] monthly climate=[${escapeHtml(formatSeriesYearly(et.solar?.climate?.monthly))}] dhw=[${escapeHtml(formatSeriesYearly(et.solar?.dhw?.monthly))}] meta climate(${escapeHtml(formatEnergyMeta(et.solar?.climate?.today_meta))}) dhw(${escapeHtml(formatEnergyMeta(et.solar?.dhw?.today_meta))})</span></li>`,
        );
      }
      if (circuits.length > 0) {
        circuits.forEach((circuit) => {
          const state = circuit.state || {};
          const config = circuit.config || {};
          rows.push(
            `<li><strong>Circuit ${escapeHtml(formatInteger(circuit.index))}</strong> <span class="muted-inline">type=${escapeHtml(circuit.circuit_type || "n/a")} mixer=${escapeHtml(formatYesNo(circuit.has_mixer))} flow=${escapeHtml(formatTemperature(state.flow_temperature_c))} setpoint=${escapeHtml(formatTemperature(state.flow_setpoint_c))} pump=${escapeHtml(formatToggle(state.pump_active))} curve=${escapeHtml(formatFixed(config.heating_curve, 2))}</span></li>`,
          );
        });
      }
      if (radioDevices.length > 0) {
        radioDevices.forEach((device) => {
          rows.push(
            `<li><strong>Radio ${escapeHtml(device.device_model || `group-${formatInteger(device.group)}-${formatInteger(device.instance)}`)}</strong> <span class="muted-inline">slot=${escapeHtml(device.slot_mode || "n/a")} connected=${escapeHtml(formatYesNo(device.device_connected))} zone=${escapeHtml(formatInteger(device.zone_assignment))} temp=${escapeHtml(formatTemperature(device.room_temperature_c))} humidity=${escapeHtml(formatPercent(device.room_humidity_pct))}</span></li>`,
          );
        });
      }
      if (cylinders.length > 0) {
        cylinders.forEach((cylinder) => {
          rows.push(
            `<li><strong>Cylinder ${escapeHtml(formatInteger(cylinder.index))}</strong> <span class="muted-inline">temp=${escapeHtml(formatTemperature(cylinder.temperature_c))} max=${escapeHtml(formatTemperature(cylinder.max_setpoint_c))} hysteresis=${escapeHtml(formatTemperature(cylinder.charge_hysteresis_c))} offset=${escapeHtml(formatTemperature(cylinder.charge_offset_c))}</span></li>`,
          );
        });
      }
      listEl.innerHTML = rows.join("");
    } catch (err) {
      listEl.innerHTML = "<li>Semantic preview unavailable.</li>";
      console.error("semantic preview failed", err);
    }
  }

  async refreshAdapterInfo() {
    const identityBody = this.querySelector('[data-role="adapter-identity-body"]');
    const telemetryBody = this.querySelector('[data-role="adapter-telemetry-body"]');
    const statusEl = this.querySelector('[data-role="adapter-refresh-status"]');
    try {
      const response = await fetch("api/v1/semantic/snapshot");
      const payload = await response.json();
      const info = payload.adapter_info;
      if (!info) {
        if (identityBody) {
          identityBody.innerHTML = `<tr><td colspan="2">Adapter info not available (INFO protocol not supported or adapter not connected).</td></tr>`;
        }
        if (telemetryBody) {
          telemetryBody.innerHTML = `<tr><td colspan="2">No telemetry data.</td></tr>`;
        }
        if (statusEl) {
          statusEl.textContent = `Last refresh: ${new Date().toLocaleTimeString()} (no data)`;
        }
        return;
      }
      if (identityBody) {
        const rows = [];
        rows.push(this.adapterRow("Firmware Version", info.firmware_version || "n/a"));
        if (info.firmware_checksum) {
          rows.push(this.adapterRow("Firmware Checksum", info.firmware_checksum));
        }
        if (info.bootloader_version) {
          rows.push(this.adapterRow("Bootloader Version", info.bootloader_version));
        }
        if (info.bootloader_checksum) {
          rows.push(this.adapterRow("Bootloader Checksum", info.bootloader_checksum));
        }
        if (info.hardware_id) {
          rows.push(this.adapterRow("Hardware ID", info.hardware_id));
        }
        if (info.hardware_config) {
          rows.push(this.adapterRow("Hardware Config", info.hardware_config));
        }
        rows.push(this.adapterRow("Connection Type", info.is_wifi ? "WiFi" : info.is_ethernet ? "Ethernet" : "Serial"));
        if (info.jumper_flags && info.jumper_flags.length > 0) {
          rows.push(this.adapterRow("Jumper Flags", info.jumper_flags.join(", ")));
        }
        rows.push(this.adapterRow("INFO Supported", info.info_supported ? "Yes" : "No"));
        rows.push(this.adapterRow("Version Response Length", String(info.version_response_len)));
        identityBody.innerHTML = rows.join("");
      }
      if (telemetryBody) {
        const rows = [];
        if (info.temperature_c != null) {
          rows.push(this.adapterRow("Temperature", formatTemperature(info.temperature_c)));
        }
        if (info.supply_voltage_mv != null) {
          rows.push(this.adapterRow("Supply Voltage", `${info.supply_voltage_mv} mV`));
        }
        if (info.bus_voltage_max_dv != null) {
          rows.push(this.adapterRow("Bus Voltage Max", `${formatFixed(info.bus_voltage_max_dv * 0.1, 1)} V`));
        }
        if (info.bus_voltage_min_dv != null) {
          rows.push(this.adapterRow("Bus Voltage Min", `${formatFixed(info.bus_voltage_min_dv * 0.1, 1)} V`));
        }
        if (info.reset_cause != null) {
          rows.push(this.adapterRow("Reset Cause", escapeHtml(info.reset_cause)));
        }
        if (info.restart_count != null) {
          rows.push(this.adapterRow("Restart Count", String(info.restart_count)));
        }
        if (info.wifi_rssi_dbm != null) {
          rows.push(this.adapterRow("WiFi RSSI", `${info.wifi_rssi_dbm} dBm`));
        }
        if (rows.length === 0) {
          rows.push(`<tr><td colspan="2">No telemetry data available yet.</td></tr>`);
        }
        telemetryBody.innerHTML = rows.join("");
      }
      if (statusEl) {
        const parts = [];
        if (info.last_identity_query) {
          parts.push(`identity: ${new Date(info.last_identity_query).toLocaleTimeString()}`);
        }
        if (info.last_telemetry_query) {
          parts.push(`telemetry: ${new Date(info.last_telemetry_query).toLocaleTimeString()}`);
        }
        statusEl.textContent = `Last refresh: ${new Date().toLocaleTimeString()}${parts.length > 0 ? ` (${parts.join(", ")})` : ""}`;
      }
    } catch (err) {
      console.error("adapter info refresh failed", err);
      if (statusEl) {
        statusEl.textContent = `Last refresh: ${new Date().toLocaleTimeString()} (error)`;
      }
    }
  }

  adapterRow(label, value) {
    return `<tr><td><strong>${escapeHtml(label)}</strong></td><td>${escapeHtml(value)}</td></tr>`;
  }

  async loadProjectionPreview(listEl) {
    const gridEl = this.querySelector('[data-role="projection-uml-grid"]');
    const controls = this.querySelector('[data-role="projection-controls"]');
    try {
      const response = await fetch("api/v1/projection/devices?limit=30");
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      this.projectionDevices = items;
      if (items.length === 0) {
        listEl.innerHTML = "<li>No projection graphs available from current registry snapshot.</li>";
        if (controls) {
          controls.classList.add("disabled");
        }
        if (gridEl) {
          gridEl.innerHTML = `<p class="projection-empty">Projection graph unavailable. No device projections published yet.</p>`;
        }
        return;
      }
      listEl.innerHTML = items
        .map((item) => {
          const nonEmpty = Array.isArray(item.projections)
            ? item.projections.filter((p) => (p.edge_count || 0) > 0)
            : [];
          const label = item.display_name || item.device_id || formatAddress(item.address);
          const planes = nonEmpty.map((p) => p.plane).filter(Boolean).join(", ");
          return `<li><strong>${escapeHtml(label)}</strong> <span class="muted-inline">addr=${escapeHtml(formatAddress(item.address))} planes=${nonEmpty.length}${planes ? ` (${escapeHtml(planes)})` : ""}</span></li>`;
        })
        .join("");
      this.populateProjectionDeviceOptions();
      if (controls) {
        controls.classList.remove("disabled");
      }
      await this.loadAllProjectionPlanes();
    } catch (err) {
      listEl.innerHTML = "<li>Projection preview unavailable.</li>";
      if (gridEl) {
        gridEl.innerHTML = `<p class="projection-empty">Projection preview request failed.</p>`;
      }
      console.error("projection preview failed", err);
    }
  }

  populateProjectionDeviceOptions() {
    const deviceSelect = this.querySelector('[data-role="projection-device-select"]');
    if (!deviceSelect) {
      return;
    }
    const items = Array.isArray(this.projectionDevices) ? this.projectionDevices : [];
    if (items.length === 0) {
      deviceSelect.innerHTML = "<option value=\"\">No devices</option>";
      deviceSelect.disabled = true;
      return;
    }
    deviceSelect.disabled = false;
    deviceSelect.innerHTML = items
      .map((item) => {
        const label = item.display_name || item.device_id || formatAddress(item.address);
        return `<option value="${escapeHtml(String(item.address))}">${escapeHtml(`${label} (${formatAddress(item.address)})`)}</option>`;
      })
      .join("");
  }

  async loadAllProjectionPlanes() {
    const gridEl = this.querySelector('[data-role="projection-uml-grid"]');
    const deviceSelect = this.querySelector('[data-role="projection-device-select"]');
    if (!gridEl || !deviceSelect) {
      return;
    }
    const addressRaw = String(deviceSelect.value || "").trim();
    if (!addressRaw) {
      gridEl.innerHTML = `<p class="projection-empty">Select a device to view projection planes.</p>`;
      return;
    }
    const items = Array.isArray(this.projectionDevices) ? this.projectionDevices : [];
    const device = items.find((item) => String(item.address) === addressRaw) || null;
    if (!device) {
      gridEl.innerHTML = `<p class="projection-empty">Device not found.</p>`;
      return;
    }
    const nonEmpty = Array.isArray(device.projections)
      ? device.projections.filter((p) => (p.edge_count || 0) > 0)
      : [];
    if (nonEmpty.length === 0) {
      gridEl.innerHTML = `<p class="projection-empty">No non-empty projection planes for this device.</p>`;
      return;
    }
    gridEl.innerHTML = `<p class="projection-empty">Loading ${nonEmpty.length} plane(s)...</p>`;
    try {
      const fetches = nonEmpty.map((p) => {
        const query = new URLSearchParams();
        query.set("address", addressRaw);
        query.set("plane", p.plane);
        return fetch(`api/v1/projection/graph?${query.toString()}`)
          .then((r) => (r.ok ? r.json() : null))
          .catch(() => null);
      });
      const graphs = await Promise.all(fetches);
      const results = nonEmpty
        .map((p, i) => ({ plane: p.plane, graph: graphs[i] }))
        .filter((r) => r.graph);
      this.renderProjectionUML(gridEl, device, results);
    } catch (err) {
      gridEl.innerHTML = `<p class="projection-empty">Projection graph request failed.</p>`;
      console.error("projection planes failed", err);
    }
  }

  renderProjectionUML(target, device, planeResults) {
    if (!target) {
      return;
    }
    if (planeResults.length === 0) {
      target.innerHTML = `<p class="projection-empty">No projection data returned.</p>`;
      return;
    }
    const label = device.display_name || device.device_id || formatAddress(device.address);
    const metaHtml = `<div class="projection-uml-meta">
      <span class="pill">${escapeHtml(label)}</span>
      <span class="muted-inline">addr=${escapeHtml(formatAddress(device.address))}</span>
      <span class="muted-inline">${planeResults.length} plane(s)</span>
    </div>`;

    const boxesHtml = planeResults.map((result) => {
      const nodes = Array.isArray(result.graph?.nodes) ? result.graph.nodes : [];
      const methods = [];
      for (const node of nodes) {
        const pathText = String(node.path || node.canonical_path || node.id || "");
        const segments = pathText.split("/").filter(Boolean);
        if (segments.length === 0) continue;
        const last = segments[segments.length - 1];
        const atIndex = last.indexOf("@");
        if (atIndex < 0) continue;
        const kind = last.substring(0, atIndex);
        const name = last.substring(atIndex + 1);
        if (kind === "device" || kind === "addr") continue;
        methods.push({ kind, name });
      }
      if (methods.length === 0) return "";
      const methodsHtml = methods
        .map((m) => {
          const prefix = m.kind === "register"
            ? `<span class="uml-method-prefix">register</span>`
            : `<span class="uml-method-prefix">+</span>`;
          return `<li class="uml-method">${prefix}${escapeHtml(m.name)}</li>`;
        })
        .join("");
      return `<div class="uml-box">
        <div class="uml-header">\u00AB${escapeHtml(result.plane)}\u00BB</div>
        <ul class="uml-body">${methodsHtml}</ul>
      </div>`;
    }).filter(Boolean).join("");

    target.innerHTML = `${metaHtml}<div class="uml-grid">${boxesHtml}</div>`;
  }

  // --- Explorer ---

  bindExplorerEvents() {
    const kindSelect = this.querySelector('[data-role="explorer-kind"]');
    const scanButton = this.querySelector('[data-role="explorer-scan"]');
    const cancelButton = this.querySelector('[data-role="explorer-cancel"]');
    const quickReadButton = this.querySelector('[data-role="explorer-quick-read"]');
    const typeSelect = this.querySelector('[data-role="explorer-type-select"]');
    if (kindSelect) {
      kindSelect.addEventListener("change", () => {
        const b524Opts = this.querySelector('[data-role="explorer-b524-opts"]');
        const b509Opts = this.querySelector('[data-role="explorer-b509-opts"]');
        const opcodeSelect = this.querySelector('[data-role="explorer-opcode"]');
        const quickDiv = this.querySelector('[data-role="explorer-quick"]');
        if (kindSelect.value === "b509") {
          if (b524Opts) b524Opts.style.display = "none";
          if (b509Opts) b509Opts.style.display = "";
          if (opcodeSelect) opcodeSelect.style.display = "none";
          if (quickDiv) quickDiv.style.display = "none";
        } else {
          if (b524Opts) b524Opts.style.display = "";
          if (b509Opts) b509Opts.style.display = "none";
          if (opcodeSelect) opcodeSelect.style.display = "";
          if (quickDiv) quickDiv.style.display = "";
        }
      });
    }
    if (scanButton) {
      scanButton.addEventListener("click", () => {
        this.startExplorerScan();
      });
    }
    if (cancelButton) {
      cancelButton.addEventListener("click", () => {
        this.cancelExplorerScan();
      });
    }
    if (quickReadButton) {
      quickReadButton.addEventListener("click", () => {
        this.explorerQuickRead();
      });
    }
    if (typeSelect) {
      typeSelect.addEventListener("change", () => {
        this.recastExplorerResults();
      });
    }
    const scanIDButton = this.querySelector('[data-role="explorer-b509-scanid"]');
    if (scanIDButton) {
      scanIDButton.addEventListener("click", () => {
        this.explorerReadSerial();
      });
    }
  }

  async initExplorer() {
    const deviceSelect = this.querySelector('[data-role="explorer-device"]');
    if (!deviceSelect) return;
    try {
      const res = await fetch("api/v1/registry/devices");
      const payload = await res.json();
      const devices = Array.isArray(payload.items) ? payload.items : [];
      deviceSelect.innerHTML = '<option value="">Select device...</option>' +
        devices.map((d) => {
          const addr = formatAddress(d.address);
          const name = escapeHtml(d.display_name || d.device_id || "unknown");
          return `<option value="${escapeHtml(String(d.address))}">${addr} ${name}</option>`;
        }).join("");
    } catch (err) {
      console.error("explorer: failed to load devices", err);
    }
    const quickDiv = this.querySelector('[data-role="explorer-quick"]');
    if (quickDiv) quickDiv.style.display = "";
  }

  async startExplorerScan() {
    const deviceSelect = this.querySelector('[data-role="explorer-device"]');
    const kindSelect = this.querySelector('[data-role="explorer-kind"]');
    const opcodeSelect = this.querySelector('[data-role="explorer-opcode"]');
    const statusEl = this.querySelector('[data-role="explorer-status"]');
    const scanButton = this.querySelector('[data-role="explorer-scan"]');
    const cancelButton = this.querySelector('[data-role="explorer-cancel"]');
    if (!deviceSelect || !deviceSelect.value) {
      if (statusEl) statusEl.textContent = "Select a device first";
      return;
    }
    // Clear stale state from previous scan.
    this._explorerResults = [];
    const groupsDiv = this.querySelector('[data-role="explorer-groups"]');
    const resultsDiv = this.querySelector('[data-role="explorer-results"]');
    const progressDiv = this.querySelector('[data-role="explorer-progress"]');
    if (groupsDiv) groupsDiv.style.display = "none";
    if (resultsDiv) resultsDiv.style.display = "none";
    if (progressDiv) progressDiv.style.display = "none";
    const kind = kindSelect ? kindSelect.value : "b524";
    const body = {
      kind: kind,
      target: parseInt(deviceSelect.value, 10),
    };
    if (kind === "b524") {
      const gMinRaw = this.querySelector('[data-role="explorer-group-min"]')?.value || "0";
      const gMaxRaw = this.querySelector('[data-role="explorer-group-max"]')?.value || "10";
      const iMaxRaw = this.querySelector('[data-role="explorer-instance-max"]')?.value || "a";
      const rMaxRaw = this.querySelector('[data-role="explorer-register-max"]')?.value || "20";
      if (!isValidHex(gMinRaw, 2) || !isValidHex(gMaxRaw, 2) || !isValidHex(iMaxRaw, 2) || !isValidHex(rMaxRaw, 4)) {
        if (statusEl) statusEl.textContent = "Invalid hex value in scan parameters";
        return;
      }
      body.opcode = parseInt(opcodeSelect ? opcodeSelect.value : "02", 16);
      body.group_min = parseInt(gMinRaw, 16);
      body.group_max = parseInt(gMaxRaw, 16);
      body.instance_max = parseInt(iMaxRaw, 16);
      body.register_max = parseInt(rMaxRaw, 16);
    } else {
      const b509OpcodeSelect = this.querySelector('[data-role="explorer-b509-opcode"]');
      body.opcode = parseInt(b509OpcodeSelect ? b509OpcodeSelect.value : "0d", 16);
      const bMinRaw = this.querySelector('[data-role="explorer-b509-min"]')?.value || "0";
      const bMaxRaw = this.querySelector('[data-role="explorer-b509-max"]')?.value || "ff";
      if (!isValidHex(bMinRaw, 4) || !isValidHex(bMaxRaw, 4)) {
        if (statusEl) statusEl.textContent = "Invalid hex value in address range";
        return;
      }
      body.b509_addr_min = parseInt(bMinRaw, 16);
      body.b509_addr_max = parseInt(bMaxRaw, 16);
    }
    try {
      if (scanButton) scanButton.disabled = true;
      if (cancelButton) cancelButton.disabled = false;
      if (statusEl) statusEl.textContent = "Starting scan...";
      const res = await fetch("api/v1/explorer/scans", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const text = await res.text();
        if (statusEl) statusEl.textContent = `Error: ${text}`;
        if (scanButton) scanButton.disabled = false;
        if (cancelButton) cancelButton.disabled = true;
        return;
      }
      this.startExplorerPolling();
    } catch (err) {
      if (statusEl) statusEl.textContent = `Error: ${err.message}`;
      if (scanButton) scanButton.disabled = false;
      if (cancelButton) cancelButton.disabled = true;
    }
  }

  async cancelExplorerScan() {
    const statusEl = this.querySelector('[data-role="explorer-status"]');
    try {
      await fetch("api/v1/explorer/scans/current", { method: "DELETE" });
      if (statusEl) statusEl.textContent = "Cancelled";
    } catch (err) {
      if (statusEl) statusEl.textContent = `Cancel error: ${err.message}`;
    }
  }

  startExplorerPolling() {
    this.stopExplorerPolling();
    this._explorerPollFails = 0;
    this._explorerPollTimer = setInterval(() => {
      this.pollExplorerState();
    }, 500);
    this.pollExplorerState();
  }

  stopExplorerPolling() {
    if (this._explorerPollTimer) {
      clearInterval(this._explorerPollTimer);
      this._explorerPollTimer = undefined;
    }
  }

  async pollExplorerState() {
    const progressDiv = this.querySelector('[data-role="explorer-progress"]');
    const progressFill = this.querySelector('[data-role="explorer-progress-fill"]');
    const progressText = this.querySelector('[data-role="explorer-progress-text"]');
    const statusEl = this.querySelector('[data-role="explorer-status"]');
    const scanButton = this.querySelector('[data-role="explorer-scan"]');
    const cancelButton = this.querySelector('[data-role="explorer-cancel"]');
    const groupsDiv = this.querySelector('[data-role="explorer-groups"]');
    const groupsBody = this.querySelector('[data-role="explorer-groups-body"]');
    const resultsDiv = this.querySelector('[data-role="explorer-results"]');
    const resultsBody = this.querySelector('[data-role="explorer-results-body"]');
    const resultCount = this.querySelector('[data-role="explorer-result-count"]');
    try {
      const res = await fetch("api/v1/explorer/scans/current");
      const state = await res.json();
      this._explorerPollFails = 0;
      const phase = state.phase || "idle";
      const progress = state.progress || {};
      const pct = progress.percent || 0;
      if (progressDiv) progressDiv.style.display = phase === "idle" ? "none" : "";
      if (progressFill) progressFill.style.width = `${pct}%`;
      if (progressText) progressText.textContent = `${pct}% — ${escapeHtml(progress.description || phase)}`;
      if (statusEl) statusEl.textContent = `${phase} (${state.completed_reads || 0}/${state.total_reads || 0})`;
      // Groups
      const groups = state.groups || [];
      if (groups.length > 0 && groupsDiv && groupsBody) {
        groupsDiv.style.display = "";
        groupsBody.innerHTML = groups.map((g) => {
          return `<tr><td>0x${escapeHtml(g.group_hex)}</td><td>${g.exists ? "yes" : "no"}</td><td>${g.instanced ? "yes" : "-"}</td><td>${formatFixed(g.value, 2)}</td></tr>`;
        }).join("");
      }
      // Results
      this._explorerResults = state.results || [];
      if (this._explorerResults.length > 0 && resultsDiv) {
        resultsDiv.style.display = "";
        if (resultCount) resultCount.textContent = `(${this._explorerResults.length} registers)`;
        this.renderExplorerResults();
      }
      // Terminal state
      const terminal = phase === "done" || phase === "cancelled" || phase === "error";
      if (terminal) {
        this.stopExplorerPolling();
        if (scanButton) scanButton.disabled = false;
        if (cancelButton) cancelButton.disabled = true;
        if (phase === "error") {
          if (statusEl) statusEl.textContent = `Error: ${state.error || "unknown"}`;
        }
      }
    } catch (err) {
      this._explorerPollFails = (this._explorerPollFails || 0) + 1;
      console.error("explorer poll error", err);
      if (this._explorerPollFails >= 10) {
        this.stopExplorerPolling();
        const statusEl = this.querySelector('[data-role="explorer-status"]');
        const scanButton = this.querySelector('[data-role="explorer-scan"]');
        const cancelButton = this.querySelector('[data-role="explorer-cancel"]');
        if (statusEl) statusEl.textContent = "Poll failed (gateway unreachable)";
        if (scanButton) scanButton.disabled = false;
        if (cancelButton) cancelButton.disabled = true;
      }
    }
  }

  renderExplorerResults() {
    const resultsBody = this.querySelector('[data-role="explorer-results-body"]');
    if (!resultsBody || !this._explorerResults) return;
    const typeSelect = this.querySelector('[data-role="explorer-type-select"]');
    const type = typeSelect ? typeSelect.value : "exp";
    resultsBody.innerHTML = this._explorerResults.map((r) => {
      const decoded = r.error ? escapeHtml(r.error) : escapeHtml(explorerDecode(r.raw_hex, r.raw_len, type));
      const cls = r.error ? ' class="explorer-error"' : "";
      return `<tr${cls}><td>0x${escapeHtml(Number(r.group).toString(16).padStart(2, "0"))}</td><td>0x${escapeHtml(Number(r.instance).toString(16).padStart(2, "0"))}</td><td>${escapeHtml(r.addr_hex)}</td><td>${escapeHtml(r.raw_hex || "")}</td><td>${decoded}</td><td>${r.raw_len}</td></tr>`;
    }).join("");
  }

  recastExplorerResults() {
    this.renderExplorerResults();
  }

  async explorerQuickRead() {
    const deviceSelect = this.querySelector('[data-role="explorer-device"]');
    const opcodeSelect = this.querySelector('[data-role="explorer-opcode"]');
    const resultEl = this.querySelector('[data-role="explorer-quick-result"]');
    const kindSelect = this.querySelector('[data-role="explorer-kind"]');
    if (!deviceSelect || !deviceSelect.value) {
      if (resultEl) resultEl.textContent = "Select a device first";
      return;
    }
    const kind = kindSelect ? kindSelect.value : "b524";
    const target = deviceSelect.value;
    const opcode = opcodeSelect ? opcodeSelect.value : "02";
    const group = this.querySelector('[data-role="explorer-quick-group"]')?.value || "00";
    const instance = this.querySelector('[data-role="explorer-quick-instance"]')?.value || "00";
    const addr = this.querySelector('[data-role="explorer-quick-addr"]')?.value || "0000";
    if (kind === "b524") {
      if (!isValidHex(group, 2)) { if (resultEl) resultEl.textContent = "Invalid hex in GG field"; return; }
      if (!isValidHex(instance, 2)) { if (resultEl) resultEl.textContent = "Invalid hex in II field"; return; }
    }
    if (!isValidHex(addr, 4)) { if (resultEl) resultEl.textContent = "Invalid hex in RR/Addr field"; return; }
    try {
      if (resultEl) resultEl.textContent = "Reading...";
      const params = new URLSearchParams({ target, opcode, group, instance, addr });
      const endpoint = kind === "b509" ? "api/v1/explorer/read/b509" : "api/v1/explorer/read/b524";
      const res = await fetch(`${endpoint}?${params.toString()}`);
      const data = await res.json();
      if (data.error) {
        if (resultEl) resultEl.textContent = `Error: ${data.error}`;
      } else {
        const typeSelect = this.querySelector('[data-role="explorer-type-select"]');
        const type = typeSelect ? typeSelect.value : "exp";
        const decoded = explorerDecode(data.raw_hex, data.raw_len, type);
        if (resultEl) resultEl.textContent = `${data.raw_hex} → ${decoded} (${data.raw_len}B)`;
      }
    } catch (err) {
      if (resultEl) resultEl.textContent = `Error: ${err.message}`;
    }
  }

  async explorerReadSerial() {
    const deviceSelect = this.querySelector('[data-role="explorer-device"]');
    const resultEl = this.querySelector('[data-role="explorer-serial-result"]');
    if (!deviceSelect || !deviceSelect.value) {
      if (resultEl) resultEl.textContent = "Select a device first";
      return;
    }
    const targetHex = parseInt(deviceSelect.value, 10).toString(16).padStart(2, "0");
    try {
      if (resultEl) resultEl.textContent = "Reading...";
      const res = await fetch(`api/v1/explorer/read/scanid?target=${targetHex}`);
      const data = await res.json();
      if (data.error) {
        if (resultEl) resultEl.textContent = `Error: ${data.error}`;
      } else {
        if (resultEl) resultEl.textContent = data.serial || "(empty)";
      }
    } catch (err) {
      if (resultEl) resultEl.textContent = `Error: ${err.message}`;
    }
  }

  render() {
    this.innerHTML = `
      <div class="shell">
        <header class="topbar">
          <div class="brand">Helianthus Dynamic Portal</div>
          <div class="status" data-role="status">Gateway checking...</div>
          <select class="select" aria-label="Controller selector" disabled>
            <option>Controller</option>
          </select>
          <input class="search" type="search" data-role="search-input" aria-label="Search" placeholder="Search across layers" />
          <button class="button" data-role="theme-toggle" aria-label="Toggle theme">Theme</button>
        </header>
        <div class="content">
          <aside class="sidebar" aria-label="Portal sections">
            <h2>Views</h2>
            <button data-role="nav-registry" data-nav-target="section-registry" disabled><span class="nav-bullet"></span> Registry</button>
            <button data-role="nav-semantic" data-nav-target="section-semantic" disabled><span class="nav-bullet"></span> Semantic</button>
            <button data-role="nav-bus" data-nav-target="section-bus" disabled><span class="nav-bullet"></span> Bus</button>
            <button data-role="nav-projection" data-nav-target="section-projection" disabled><span class="nav-bullet"></span> Projection</button>
            <button data-role="nav-explorer" data-nav-target="section-explorer" disabled><span class="nav-bullet"></span> Explorer</button>
            <button data-role="nav-adapter" data-nav-target="section-adapter" disabled><span class="nav-bullet"></span> Adapter</button>
            <button data-role="nav-timeline" data-nav-target="section-timeline" disabled><span class="nav-bullet"></span> Timeline</button>
            <button data-role="nav-snapshots" data-nav-target="section-snapshots" disabled><span class="nav-bullet"></span> Snapshots</button>
            <button data-role="nav-issue-builder" data-nav-target="section-issue-builder" disabled><span class="nav-bullet"></span> Issue Builder</button>
          </aside>
          <main class="main">
            <h1>Portal Overview</h1>
            <p class="hero">Explore registry, semantic state, projection topology and evidence workflows from one gateway-native surface.</p>
            <section id="section-registry" class="registry-preview">
              <h2>Registry Preview</h2>
              <ul data-role="registry-list">
                <li>Loading discovered devices...</li>
              </ul>
            </section>
            <section id="section-semantic" class="registry-preview">
              <h2>Semantic Preview</h2>
              <ul data-role="semantic-list">
                <li>Loading semantic snapshot...</li>
              </ul>
            </section>
            <section id="section-bus" class="registry-preview">
              <h2>Bus Observability</h2>
              <div class="bus-banner bus-state-unavailable" data-role="bus-banner">Loading bus observability...</div>
              <ul data-role="bus-observability">
                <li>Loading bus observability...</li>
              </ul>
            </section>
            <section id="section-projection" class="registry-preview">
              <h2>Projection Preview</h2>
              <div class="projection-controls disabled" data-role="projection-controls">
                <select class="select" data-role="projection-device-select" aria-label="Projection device" disabled>
                  <option value="">No projection devices</option>
                </select>
              </div>
              <ul data-role="projection-list">
                <li>Loading projection summary...</li>
              </ul>
              <div class="projection-uml-grid" data-role="projection-uml-grid">
                <p class="projection-empty">Select a device to view projection planes.</p>
              </div>
            </section>
            <section id="section-explorer" class="registry-preview">
              <h2>Register Explorer</h2>
              <div class="explorer-controls">
                <div class="explorer-row">
                  <select class="select" data-role="explorer-device" aria-label="Explorer device">
                    <option value="">Select device...</option>
                  </select>
                  <select class="select" data-role="explorer-kind" aria-label="Scan kind">
                    <option value="b524">B5.24 Extended Registers</option>
                    <option value="b509">B5.09 Registers</option>
                  </select>
                  <select class="select" data-role="explorer-opcode" aria-label="Opcode">
                    <option value="02">0x02 Local</option>
                    <option value="06">0x06 Remote</option>
                  </select>
                </div>
                <div class="explorer-row" data-role="explorer-b524-opts">
                  <label class="explorer-label">GG <input class="search explorer-input" data-role="explorer-group-min" type="text" value="00" size="3" /></label>
                  <label class="explorer-label">– <input class="search explorer-input" data-role="explorer-group-max" type="text" value="10" size="3" /></label>
                  <label class="explorer-label">II max <input class="search explorer-input" data-role="explorer-instance-max" type="text" value="0a" size="3" /></label>
                  <label class="explorer-label">RR max <input class="search explorer-input" data-role="explorer-register-max" type="text" value="0020" size="5" /></label>
                </div>
                <div class="explorer-row" data-role="explorer-b509-opts" style="display:none">
                  <select class="select" data-role="explorer-b509-opcode" aria-label="B509 opcode">
                    <option value="0d">0x0D Read</option>
                    <option value="29">0x29 Passive</option>
                  </select>
                  <label class="explorer-label">Addr min <input class="search explorer-input" data-role="explorer-b509-min" type="text" value="0000" size="5" /></label>
                  <label class="explorer-label">– max <input class="search explorer-input" data-role="explorer-b509-max" type="text" value="00ff" size="5" /></label>
                  <button class="button" data-role="explorer-b509-scanid" type="button">Read Serial</button>
                  <span class="muted-inline" data-role="explorer-serial-result"></span>
                </div>
                <div class="explorer-row">
                  <button class="button" data-role="explorer-scan" type="button">Start Scan</button>
                  <button class="button" data-role="explorer-cancel" type="button" disabled>Cancel</button>
                  <span class="muted-inline" data-role="explorer-status">Ready</span>
                </div>
              </div>
              <div class="explorer-progress" data-role="explorer-progress" style="display:none">
                <div class="explorer-progress-bar"><div class="explorer-progress-fill" data-role="explorer-progress-fill"></div></div>
                <span class="muted-inline" data-role="explorer-progress-text">0%</span>
              </div>
              <div data-role="explorer-groups" style="display:none">
                <h3>Discovered Groups</h3>
                <table class="explorer-table">
                  <thead><tr><th>Group</th><th>Exists</th><th>Instanced</th><th>Value</th></tr></thead>
                  <tbody data-role="explorer-groups-body"></tbody>
                </table>
              </div>
              <div data-role="explorer-results" style="display:none">
                <h3>Register Results <span class="muted-inline" data-role="explorer-result-count"></span></h3>
                <div class="explorer-row">
                  <label class="explorer-label">Type <select class="select" data-role="explorer-type-select">
                    <option value="hex">HEX</option>
                    <option value="exp" selected>EXP (float32)</option>
                    <option value="ulg">ULG (uint32)</option>
                    <option value="uin">UIN (uint16)</option>
                    <option value="sin">SIN (int16)</option>
                    <option value="uch">UCH (uint8)</option>
                    <option value="sch">SCH (int8)</option>
                    <option value="str">STR (string)</option>
                  </select></label>
                </div>
                <table class="explorer-table">
                  <thead><tr><th>Group</th><th>Inst</th><th>Addr</th><th>Raw</th><th>Decoded</th><th>Len</th></tr></thead>
                  <tbody data-role="explorer-results-body"></tbody>
                </table>
              </div>
              <div data-role="explorer-quick" style="display:none">
                <h3>Quick Read</h3>
                <div class="explorer-row">
                  <label class="explorer-label">GG <input class="search explorer-input" data-role="explorer-quick-group" type="text" value="00" size="3" /></label>
                  <label class="explorer-label">II <input class="search explorer-input" data-role="explorer-quick-instance" type="text" value="00" size="3" /></label>
                  <label class="explorer-label">RR <input class="search explorer-input" data-role="explorer-quick-addr" type="text" value="0000" size="5" /></label>
                  <button class="button" data-role="explorer-quick-read" type="button">Read</button>
                  <span class="muted-inline" data-role="explorer-quick-result"></span>
                </div>
              </div>
            </section>
            <section id="section-adapter" class="registry-preview">
              <h2>Adapter Hardware Info</h2>
              <div data-role="adapter-identity" class="adapter-panel">
                <h3>Identity</h3>
                <table class="explorer-table">
                  <tbody data-role="adapter-identity-body">
                    <tr><td colspan="2">Loading adapter info...</td></tr>
                  </tbody>
                </table>
              </div>
              <div data-role="adapter-telemetry" class="adapter-panel">
                <h3>Telemetry</h3>
                <table class="explorer-table">
                  <tbody data-role="adapter-telemetry-body">
                    <tr><td colspan="2">Waiting for telemetry data...</td></tr>
                  </tbody>
                </table>
              </div>
              <div class="muted-inline" data-role="adapter-refresh-status">Last refresh: never</div>
            </section>
            <section id="section-search" class="registry-preview">
              <h2>Search Results</h2>
              <ul data-role="search-list">
                <li>Loading search capability...</li>
              </ul>
            </section>
            <section id="section-timeline" class="registry-preview">
              <h2>Timeline</h2>
              <input class="search timeline-filter" data-role="timeline-correlation" type="search" placeholder="Filter by correlation id" aria-label="Filter timeline by correlation id" />
              <ul data-role="timeline-list">
                <li>Loading timeline capability...</li>
              </ul>
            </section>
            <section id="section-provenance" class="registry-preview">
              <h2>Provenance Inspector</h2>
              <input class="search timeline-filter" data-role="provenance-correlation" type="search" placeholder="Filter provenance by correlation id" aria-label="Filter provenance by correlation id" />
              <ul data-role="provenance-list">
                <li>Loading provenance capability...</li>
              </ul>
            </section>
            <section id="section-snapshots" class="registry-preview">
              <h2>Snapshots</h2>
              <div class="snapshot-controls">
                <input class="search timeline-filter" data-role="snapshot-label" type="search" placeholder="Snapshot label (optional)" aria-label="Snapshot label" />
                <button class="button" data-role="snapshot-capture" type="button">Capture</button>
                <input class="search timeline-filter snapshot-retention" data-role="snapshot-retention" type="number" min="1" max="500" placeholder="Retention max" aria-label="Snapshot retention max" />
                <button class="button" data-role="snapshot-retention-apply" type="button">Apply Retention</button>
              </div>
              <ul data-role="snapshots-list">
                <li>Loading snapshots capability...</li>
              </ul>
            </section>
            <section id="section-snapshot-diff" class="registry-preview">
              <h2>Snapshot Diff</h2>
              <div class="snapshot-controls">
                <input class="search timeline-filter" data-role="snapshot-diff-from" type="search" placeholder="from_id (optional)" aria-label="Snapshot diff from id" />
                <input class="search timeline-filter" data-role="snapshot-diff-to" type="search" placeholder="to_id (optional)" aria-label="Snapshot diff to id" />
                <button class="button" data-role="snapshot-diff-run" type="button">Run Diff</button>
              </div>
              <ul data-role="snapshot-diff-list">
                <li>Loading snapshot diff capability...</li>
              </ul>
            </section>
            <section id="section-sessions" class="registry-preview">
              <h2>Sessions</h2>
              <div class="snapshot-controls">
                <input class="search timeline-filter" data-role="session-name" type="search" placeholder="Session name" aria-label="Session name" />
                <button class="button" data-role="session-save" type="button">Save Session</button>
                <input class="search timeline-filter" data-role="session-load-id" type="search" placeholder="Session id" aria-label="Session id" />
                <button class="button" data-role="session-load" type="button">Load Session</button>
              </div>
              <ul data-role="sessions-list">
                <li>Loading sessions capability...</li>
              </ul>
            </section>
            <section id="section-issue-builder" class="registry-preview">
              <h2>Issue Builder</h2>
              <div class="snapshot-controls">
                <input class="search timeline-filter" data-role="issue-title" type="search" placeholder="Issue title" aria-label="Issue title" />
                <input class="search timeline-filter" data-role="issue-observation" type="search" placeholder="Observation" aria-label="Issue observation" />
                <input class="search timeline-filter" data-role="issue-hypothesis" type="search" placeholder="Hypothesis" aria-label="Issue hypothesis" />
                <button class="button" data-role="issue-draft-run" type="button">Draft Markdown</button>
                <button class="button" data-role="issue-export-run" type="button">Export Bundle</button>
              </div>
              <pre class="issue-preview" data-role="issue-preview">Loading issue builder capability...</pre>
            </section>
            <div class="meta" data-role="stream-status">Stream idle</div>
            <div class="meta" data-role="meta">Waiting for bootstrap...</div>
          </main>
        </div>
      </div>
    `;
  }
}

customElements.define("portal-shell", PortalShell);
