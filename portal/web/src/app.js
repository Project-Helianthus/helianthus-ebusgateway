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
      if (metaEl) {
        const caps = bootstrap.capabilities || {};
        metaEl.textContent =
          `Capabilities: registry=${caps.registry}, semantic=${caps.semantic}, projection=${caps.projection}, search=${caps.search}, stream=${caps.stream}, timeline=${caps.timeline}, provenance=${caps.provenance}, snapshots=${caps.snapshots}, snapshot_diff=${caps.snapshot_diff}`;
      }
      if (searchInput) {
        searchInput.disabled = !bootstrap.capabilities?.search;
        searchInput.title = bootstrap.capabilities?.search ? "" : "Search is unavailable (no data providers)";
      }
      if (bootstrap.capabilities?.registry && listEl) {
        await this.loadRegistryPreview(listEl);
      }
      if (bootstrap.capabilities?.semantic && semanticEl) {
        await this.loadSemanticPreview(semanticEl);
      }
      if (bootstrap.capabilities?.projection && projectionEl) {
        await this.loadProjectionPreview(projectionEl);
      }
      if (searchList) {
        searchList.innerHTML = bootstrap.capabilities?.search
          ? "<li>Type at least 2 characters to search across registry, semantic and projection layers.</li>"
          : "<li>Search unavailable: no readable layers enabled.</li>";
      }
      if (bootstrap.capabilities?.stream) {
        this.startStream();
      }
      if (timelineList) {
        timelineList.innerHTML = bootstrap.capabilities?.timeline
          ? "<li>Loading timeline events...</li>"
          : "<li>Timeline unavailable: stream capability disabled.</li>";
      }
      if (bootstrap.capabilities?.timeline) {
        await this.refreshTimeline();
        if (this.timelineInterval) {
          clearInterval(this.timelineInterval);
        }
        this.timelineInterval = setInterval(() => {
          this.refreshTimeline();
        }, 3000);
      }
      if (provenanceList) {
        provenanceList.innerHTML = bootstrap.capabilities?.provenance
          ? "<li>Loading provenance records...</li>"
          : "<li>Provenance unavailable: stream capability disabled.</li>";
      }
      if (bootstrap.capabilities?.provenance) {
        await this.refreshProvenance();
        if (this.provenanceInterval) {
          clearInterval(this.provenanceInterval);
        }
        this.provenanceInterval = setInterval(() => {
          this.refreshProvenance();
        }, 4000);
      }
      if (snapshotsList) {
        snapshotsList.innerHTML = bootstrap.capabilities?.snapshots
          ? "<li>Loading snapshot store...</li>"
          : "<li>Snapshots unavailable: stream capability disabled.</li>";
      }
      if (bootstrap.capabilities?.snapshots) {
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
        diffList.innerHTML = bootstrap.capabilities?.snapshot_diff
          ? "<li>Select snapshot IDs and run diff.</li>"
          : "<li>Snapshot diff unavailable.</li>";
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
          return `<li><span class="pill">${layer}</span> <strong>${corr}</strong> <span class="muted-inline">${at}</span></li>`;
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
          return `<li><span class="pill">prov</span> <strong>${corr}</strong> <span class="muted-inline">${source} keys=${escapeHtml(keys)} conf=${confidence}</span></li>`;
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
          return `<li><span class="pill">snap</span> <strong>${id}</strong> <span class="muted-inline">${label} ${at}</span></li>`;
        })
        .join("");

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
          const addr = Number(item.address).toString(16).padStart(2, "0");
          const model = item.device_id || "unknown";
          const vendor = item.manufacturer || "unknown";
          return `<li><strong>0x${addr}</strong> ${vendor} ${model}</li>`;
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
      rows.push(`<li>Zones: ${zones.length}</li>`);
      if (zones[0]) {
        rows.push(`<li>Zone #1: ${zones[0].name || zones[0].id || "unknown"}</li>`);
      }
      if (dhw) {
        rows.push(`<li>DHW mode: ${dhw.operating_mode || "unknown"}</li>`);
      }
      listEl.innerHTML = rows.join("");
    } catch (err) {
      listEl.innerHTML = "<li>Semantic preview unavailable.</li>";
      console.error("semantic preview failed", err);
    }
  }

  async loadProjectionPreview(listEl) {
    try {
      const response = await fetch("api/v1/projection/devices?limit=5");
      const payload = await response.json();
      const items = Array.isArray(payload.items) ? payload.items : [];
      if (items.length === 0) {
        listEl.innerHTML = "<li>No projection graphs available.</li>";
        return;
      }
      listEl.innerHTML = items
        .map((item) => {
          const projectionCount = Array.isArray(item.projections) ? item.projections.length : 0;
          const label = item.display_name || item.device_id || `0x${Number(item.address).toString(16)}`;
          return `<li><strong>${label}</strong> projections=${projectionCount}</li>`;
        })
        .join("");
    } catch (err) {
      listEl.innerHTML = "<li>Projection preview unavailable.</li>";
      console.error("projection preview failed", err);
    }
  }

  render() {
    this.innerHTML = `
      <div class="shell">
        <header class="topbar">
          <div class="brand">Helianthus Dynamic Portal</div>
          <div class="status" data-role="status">Gateway checking...</div>
          <select class="select" aria-label="Controller selector">
            <option>Controller (M1)</option>
          </select>
          <input class="search" type="search" data-role="search-input" aria-label="Search" placeholder="Search across layers" />
          <button class="button" data-role="theme-toggle" aria-label="Toggle theme">Theme</button>
        </header>
        <div class="content">
          <aside class="sidebar" aria-label="Portal sections">
            <h2>Views</h2>
            <button disabled>Registry (M1)</button>
            <button disabled>Semantic (M1)</button>
            <button disabled>Projection (M1)</button>
            <button disabled>Timeline (M2)</button>
            <button disabled>Snapshots (M3)</button>
            <button disabled>Issue Builder (M5)</button>
          </aside>
          <main class="main">
            <h1>Portal Overview</h1>
            <p class="hero">M0 skeleton online. Discovery and evidence workflows unlock in subsequent milestones.</p>
            <section class="cards" aria-label="Capability cards">
              <article class="card"><h3>Registry</h3><span class="badge">Coming Soon</span></article>
              <article class="card"><h3>Semantic</h3><span class="badge">Coming Soon</span></article>
              <article class="card"><h3>Projection</h3><span class="badge">Coming Soon</span></article>
              <article class="card"><h3>Timeline</h3><span class="badge">Coming Soon</span></article>
              <article class="card"><h3>Snapshots</h3><span class="badge">Coming Soon</span></article>
              <article class="card"><h3>Issue Builder</h3><span class="badge">Coming Soon</span></article>
            </section>
            <section class="registry-preview">
              <h2>Registry Preview</h2>
              <ul data-role="registry-list">
                <li>Loading discovered devices...</li>
              </ul>
            </section>
            <section class="registry-preview">
              <h2>Semantic Preview</h2>
              <ul data-role="semantic-list">
                <li>Loading semantic snapshot...</li>
              </ul>
            </section>
            <section class="registry-preview">
              <h2>Projection Preview</h2>
              <ul data-role="projection-list">
                <li>Loading projection summary...</li>
              </ul>
            </section>
            <section class="registry-preview">
              <h2>Search Results</h2>
              <ul data-role="search-list">
                <li>Loading search capability...</li>
              </ul>
            </section>
            <section class="registry-preview">
              <h2>Timeline</h2>
              <input class="search timeline-filter" data-role="timeline-correlation" type="search" placeholder="Filter by correlation id" aria-label="Filter timeline by correlation id" />
              <ul data-role="timeline-list">
                <li>Loading timeline capability...</li>
              </ul>
            </section>
            <section class="registry-preview">
              <h2>Provenance Inspector</h2>
              <input class="search timeline-filter" data-role="provenance-correlation" type="search" placeholder="Filter provenance by correlation id" aria-label="Filter provenance by correlation id" />
              <ul data-role="provenance-list">
                <li>Loading provenance capability...</li>
              </ul>
            </section>
            <section class="registry-preview">
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
            <section class="registry-preview">
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
            <div class="meta" data-role="stream-status">Stream idle</div>
            <div class="meta" data-role="meta">Waiting for bootstrap...</div>
          </main>
        </div>
      </div>
    `;
  }
}

customElements.define("portal-shell", PortalShell);
