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
  }

  bindEvents() {
    const toggle = this.querySelector('[data-role="theme-toggle"]');
    const search = this.querySelector('[data-role="search-input"]');
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
  }

  async loadStatus() {
    const statusEl = this.querySelector('[data-role="status"]');
    const metaEl = this.querySelector('[data-role="meta"]');
    const listEl = this.querySelector('[data-role="registry-list"]');
    const semanticEl = this.querySelector('[data-role="semantic-list"]');
    const projectionEl = this.querySelector('[data-role="projection-list"]');
    const searchInput = this.querySelector('[data-role="search-input"]');
    const searchList = this.querySelector('[data-role="search-list"]');
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
          `Capabilities: registry=${caps.registry}, semantic=${caps.semantic}, projection=${caps.projection}, search=${caps.search}, stream=${caps.stream}`;
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
            <div class="meta" data-role="stream-status">Stream idle</div>
            <div class="meta" data-role="meta">Waiting for bootstrap...</div>
          </main>
        </div>
      </div>
    `;
  }
}

customElements.define("portal-shell", PortalShell);
