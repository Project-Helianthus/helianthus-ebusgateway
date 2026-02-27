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

function formatAddress(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "0x??";
  }
  return `0x${number.toString(16).padStart(2, "0")}`;
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
    const projectionPlaneSelect = this.querySelector('[data-role="projection-plane-select"]');
    const projectionLoadButton = this.querySelector('[data-role="projection-load"]');
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
        this.refreshProjectionPlaneOptions();
        this.loadSelectedProjectionGraph();
      });
    }
    if (projectionPlaneSelect) {
      projectionPlaneSelect.addEventListener("change", () => {
        this.loadSelectedProjectionGraph();
      });
    }
    if (projectionLoadButton) {
      projectionLoadButton.addEventListener("click", () => {
        this.loadSelectedProjectionGraph();
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
  }

  activateSection(targetID) {
    const sectionMap = {
      "section-registry": ["section-registry"],
      "section-semantic": ["section-semantic"],
      "section-projection": ["section-projection", "section-search"],
      "section-timeline": ["section-timeline", "section-provenance"],
      "section-snapshots": ["section-snapshots", "section-snapshot-diff", "section-sessions"],
      "section-issue-builder": ["section-issue-builder"],
    };
    const visible = new Set(sectionMap[targetID] || [targetID]);
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
    this.setNavState("projection", cap.projection);
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
        this.refreshSnapshots();
        this.runSnapshotDiff();
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
          const slave = Number(item.address);
          const master = slave - 5;
          const slaveHex = slave.toString(16).padStart(2, "0");
          const masterHex = master >= 0 ? master.toString(16).padStart(2, "0") : null;
          const addrStr = masterHex ? `0x${masterHex}(M) / 0x${slaveHex}(S)` : `0x${slaveHex}`;
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
      const dhw = payload.dhw;
      const rows = [];
      rows.push(`<li><strong>Zones detected:</strong> ${zones.length}</li>`);
      if (zones.length === 0) {
        rows.push("<li>No semantic zones available.</li>");
      } else {
        zones.forEach((zone) => {
          const name = escapeHtml(zone.name || zone.id || "zone");
          const mode = escapeHtml(zone.operating_mode || "unknown");
          const preset = escapeHtml(zone.preset || "n/a");
          const current = formatTemperature(zone.current_temp_c);
          const target = formatTemperature(zone.target_temp_c);
          const demand = formatPercent(zone.heating_demand);
          rows.push(
            `<li><strong>${name}</strong> <span class="muted-inline">mode=${mode} preset=${preset} current=${escapeHtml(current)} target=${escapeHtml(target)} demand=${escapeHtml(demand)}</span></li>`,
          );
        });
      }
      if (dhw) {
        const dhwPreset = dhw.preset ? ` preset=${escapeHtml(dhw.preset)}` : "";
        const dhwDemand = dhw.heating_demand != null ? ` demand=${escapeHtml(formatPercent(dhw.heating_demand))}` : "";
        rows.push(`<li><strong>DHW</strong> <span class="muted-inline">mode=${escapeHtml(dhw.operating_mode || "unknown")}${dhwPreset} current=${escapeHtml(formatTemperature(dhw.current_temp_c))} target=${escapeHtml(formatTemperature(dhw.target_temp_c))}${dhwDemand}</span></li>`);
      }
      if (payload.energy_totals) {
        const et = payload.energy_totals;
        rows.push(
          `<li><strong>Energy today (gas)</strong> <span class="muted-inline">climate=${escapeHtml(formatFixed(et.gas?.climate?.today, 2))} dhw=${escapeHtml(formatFixed(et.gas?.dhw?.today, 2))}</span></li>`,
        );
        const elecClimate = et.electric?.climate?.today || 0;
        const elecDHW = et.electric?.dhw?.today || 0;
        if (elecClimate > 0 || elecDHW > 0) {
          rows.push(
            `<li><strong>Energy today (electric)</strong> <span class="muted-inline">climate=${escapeHtml(formatFixed(elecClimate, 2))} dhw=${escapeHtml(formatFixed(elecDHW, 2))}</span></li>`,
          );
        }
        const solarClimate = et.solar?.climate?.today || 0;
        const solarDHW = et.solar?.dhw?.today || 0;
        if (solarClimate > 0 || solarDHW > 0) {
          rows.push(
            `<li><strong>Energy today (solar)</strong> <span class="muted-inline">climate=${escapeHtml(formatFixed(solarClimate, 2))} dhw=${escapeHtml(formatFixed(solarDHW, 2))}</span></li>`,
          );
        }
      }
      listEl.innerHTML = rows.join("");
    } catch (err) {
      listEl.innerHTML = "<li>Semantic preview unavailable.</li>";
      console.error("semantic preview failed", err);
    }
  }

  async loadProjectionPreview(listEl) {
    const graphEl = this.querySelector('[data-role="projection-graph"]');
    const controls = this.querySelector('[data-role="projection-controls"]');
    const loadButton = this.querySelector('[data-role="projection-load"]');
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
        if (loadButton) {
          loadButton.disabled = true;
        }
        this.renderProjectionState(graphEl, "Projection graph unavailable. No device projections published yet.");
        return;
      }
      listEl.innerHTML = items
        .map((item) => {
          const projectionCount = Array.isArray(item.projections) ? item.projections.length : 0;
          const label = item.display_name || item.device_id || formatAddress(item.address);
          const planes = Array.isArray(item.projections)
            ? item.projections.map((projection) => projection.plane).filter(Boolean).join(", ")
            : "";
          return `<li><strong>${escapeHtml(label)}</strong> <span class="muted-inline">addr=${escapeHtml(formatAddress(item.address))} projections=${projectionCount}${planes ? ` planes=${escapeHtml(planes)}` : ""}</span></li>`;
        })
        .join("");
      this.populateProjectionDeviceOptions();
      if (controls) {
        controls.classList.remove("disabled");
      }
      if (loadButton) {
        loadButton.disabled = false;
      }
      await this.loadSelectedProjectionGraph();
    } catch (err) {
      listEl.innerHTML = "<li>Projection preview unavailable.</li>";
      this.renderProjectionState(graphEl, "Projection preview request failed.");
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
      this.refreshProjectionPlaneOptions();
      return;
    }
    deviceSelect.disabled = false;
    deviceSelect.innerHTML = items
      .map((item) => {
        const label = item.display_name || item.device_id || formatAddress(item.address);
        return `<option value="${escapeHtml(String(item.address))}">${escapeHtml(`${label} (${formatAddress(item.address)})`)}</option>`;
      })
      .join("");
    this.refreshProjectionPlaneOptions();
  }

  refreshProjectionPlaneOptions() {
    const deviceSelect = this.querySelector('[data-role="projection-device-select"]');
    const planeSelect = this.querySelector('[data-role="projection-plane-select"]');
    if (!deviceSelect || !planeSelect) {
      return;
    }
    const address = Number(deviceSelect.value);
    const items = Array.isArray(this.projectionDevices) ? this.projectionDevices : [];
    const current = items.find((item) => Number(item.address) === address) || null;
    const planes = current && Array.isArray(current.projections)
      ? current.projections.map((projection) => projection.plane).filter(Boolean)
      : [];
    if (planes.length === 0) {
      planeSelect.innerHTML = "<option value=\"\">No planes</option>";
      planeSelect.disabled = true;
      return;
    }
    planeSelect.disabled = false;
    planeSelect.innerHTML = planes
      .map((plane) => `<option value="${escapeHtml(plane)}">${escapeHtml(plane)}</option>`)
      .join("");
  }

  async loadSelectedProjectionGraph() {
    const graphEl = this.querySelector('[data-role="projection-graph"]');
    const deviceSelect = this.querySelector('[data-role="projection-device-select"]');
    const planeSelect = this.querySelector('[data-role="projection-plane-select"]');
    if (!graphEl || !deviceSelect || !planeSelect) {
      return;
    }
    const addressRaw = String(deviceSelect.value || "").trim();
    const plane = String(planeSelect.value || "").trim();
    if (!addressRaw || !plane) {
      this.renderProjectionState(graphEl, "Pick device and plane to load graph.");
      return;
    }
    const query = new URLSearchParams();
    query.set("address", addressRaw);
    query.set("plane", plane);
    try {
      const response = await fetch(`api/v1/projection/graph?${query.toString()}`);
      if (!response.ok) {
        this.renderProjectionState(graphEl, `Projection graph request failed (${response.status}).`);
        return;
      }
      const payload = await response.json();
      this.renderProjectionGraph(graphEl, payload);
    } catch (err) {
      this.renderProjectionState(graphEl, "Projection graph request failed.");
      console.error("projection graph failed", err);
    }
  }

  renderProjectionState(target, text) {
    if (!target) {
      return;
    }
    target.innerHTML = `<p class="projection-empty">${escapeHtml(text || "Projection graph unavailable.")}</p>`;
  }

  renderProjectionGraph(target, payload) {
    if (!target) {
      return;
    }
    const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
    const edges = Array.isArray(payload?.edges) ? payload.edges : [];
    if (nodes.length === 0) {
      this.renderProjectionState(target, "No projection nodes for selected plane.");
      return;
    }

    const nodeByID = new Map();
    nodes.forEach((node) => {
      const id = String(node.id || "");
      if (id) {
        nodeByID.set(id, node);
      }
    });

    const validEdges = edges.filter((edge) => nodeByID.has(String(edge.from || "")) && nodeByID.has(String(edge.to || "")));
    const incoming = new Map();
    const outgoing = new Map();
    nodeByID.forEach((_, id) => {
      incoming.set(id, 0);
      outgoing.set(id, []);
    });
    validEdges.forEach((edge) => {
      const from = String(edge.from || "");
      const to = String(edge.to || "");
      incoming.set(to, (incoming.get(to) || 0) + 1);
      outgoing.get(from).push(to);
    });

    const roots = [];
    incoming.forEach((count, id) => {
      if (count === 0) {
        roots.push(id);
      }
    });
    if (roots.length === 0 && nodes[0]?.id) {
      roots.push(String(nodes[0].id));
    }

    const depth = new Map();
    const queue = [...roots];
    roots.forEach((id) => depth.set(id, 0));
    while (queue.length > 0) {
      const current = queue.shift();
      const nextDepth = (depth.get(current) || 0) + 1;
      const neighbors = outgoing.get(current) || [];
      neighbors.forEach((next) => {
        if (!depth.has(next) || nextDepth < depth.get(next)) {
          depth.set(next, nextDepth);
          queue.push(next);
        }
      });
    }
    let maxDepth = 0;
    depth.forEach((value) => {
      if (value > maxDepth) {
        maxDepth = value;
      }
    });
    nodeByID.forEach((_, id) => {
      if (!depth.has(id)) {
        maxDepth += 1;
        depth.set(id, maxDepth);
      }
    });

    const columns = new Map();
    depth.forEach((value, id) => {
      if (!columns.has(value)) {
        columns.set(value, []);
      }
      columns.get(value).push(id);
    });
    const orderedDepths = [...columns.keys()].sort((a, b) => a - b);
    orderedDepths.forEach((columnDepth) => {
      columns.get(columnDepth).sort((left, right) => left.localeCompare(right));
    });

    const colGap = 220;
    const rowGap = 84;
    const paddingX = 70;
    const paddingY = 60;
    const maxRows = orderedDepths.reduce((acc, columnDepth) => {
      return Math.max(acc, columns.get(columnDepth).length);
    }, 1);
    const width = paddingX * 2 + Math.max(1, orderedDepths.length - 1) * colGap + 190;
    const height = paddingY * 2 + Math.max(1, maxRows - 1) * rowGap + 70;

    const positions = new Map();
    orderedDepths.forEach((columnDepth, columnIndex) => {
      const ids = columns.get(columnDepth);
      ids.forEach((id, rowIndex) => {
        const x = paddingX + columnIndex * colGap;
        const y = paddingY + rowIndex * rowGap;
        positions.set(id, { x, y });
      });
    });

    const edgeMarkup = validEdges.map((edge) => {
      const from = positions.get(String(edge.from || ""));
      const to = positions.get(String(edge.to || ""));
      if (!from || !to) {
        return "";
      }
      const startX = from.x + 154;
      const startY = from.y + 20;
      const endX = to.x;
      const endY = to.y + 20;
      const cx1 = startX + 56;
      const cx2 = endX - 56;
      return `<path d="M ${startX} ${startY} C ${cx1} ${startY}, ${cx2} ${endY}, ${endX} ${endY}" class="projection-edge" />`;
    }).join("");

    const nodeMarkup = [...positions.entries()].map(([id, pos]) => {
      const node = nodeByID.get(id) || {};
      const pathText = String(node.path || node.canonical_path || id);
      const segments = pathText.split("/").filter(Boolean);
      const title = segments.length > 0 ? segments[segments.length - 1] : pathText;
      const subtitle = pathText.length > 38 ? `${pathText.slice(0, 35)}...` : pathText;
      return `<g class="projection-node" transform="translate(${pos.x},${pos.y})">
        <rect width="154" height="40" rx="10" ry="10"></rect>
        <text x="10" y="16" class="projection-node-title">${escapeHtml(title)}</text>
        <text x="10" y="31" class="projection-node-subtitle">${escapeHtml(subtitle)}</text>
      </g>`;
    }).join("");

    target.innerHTML = `
      <div class="projection-graph-meta">
        <span class="pill">nodes ${nodes.length}</span>
        <span class="pill">edges ${validEdges.length}</span>
        <span class="muted-inline">plane=${escapeHtml(payload?.plane || "unknown")} address=${escapeHtml(formatAddress(payload?.address))}</span>
      </div>
      <svg class="projection-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Projection graph">
        ${edgeMarkup}
        ${nodeMarkup}
      </svg>
    `;
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
            <button data-role="nav-projection" data-nav-target="section-projection" disabled><span class="nav-bullet"></span> Projection</button>
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
            <section id="section-projection" class="registry-preview">
              <h2>Projection Preview</h2>
              <div class="projection-controls disabled" data-role="projection-controls">
                <select class="select" data-role="projection-device-select" aria-label="Projection device" disabled>
                  <option value="">No projection devices</option>
                </select>
                <select class="select" data-role="projection-plane-select" aria-label="Projection plane" disabled>
                  <option value="">No planes</option>
                </select>
                <button class="button" data-role="projection-load" type="button" disabled>Load Graph</button>
              </div>
              <ul data-role="projection-list">
                <li>Loading projection summary...</li>
              </ul>
              <div class="projection-graph" data-role="projection-graph">
                <p class="projection-empty">Projection graph will appear here.</p>
              </div>
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
