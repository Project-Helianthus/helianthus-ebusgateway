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

class PortalShell extends HTMLElement {
  connectedCallback() {
    this.render();
    setTheme(loadTheme());
    this.bindEvents();
    this.loadStatus();
  }

  bindEvents() {
    const toggle = this.querySelector('[data-role="theme-toggle"]');
    if (toggle) {
      toggle.addEventListener("click", () => {
        const current = loadTheme();
        setTheme(current === "dark" ? "light" : "dark");
      });
    }
  }

  async loadStatus() {
    const statusEl = this.querySelector('[data-role="status"]');
    const metaEl = this.querySelector('[data-role="meta"]');
    const listEl = this.querySelector('[data-role="registry-list"]');
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
          `Capabilities: registry=${caps.registry}, semantic=${caps.semantic}, projection=${caps.projection}, stream=${caps.stream}`;
      }
      if (bootstrap.capabilities?.registry && listEl) {
        await this.loadRegistryPreview(listEl);
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

  render() {
    this.innerHTML = `
      <div class="shell">
        <header class="topbar">
          <div class="brand">Helianthus Dynamic Portal</div>
          <div class="status" data-role="status">Gateway checking...</div>
          <select class="select" aria-label="Controller selector">
            <option>Controller (M1)</option>
          </select>
          <input class="search" type="search" disabled title="Available in M1" aria-label="Search" placeholder="Search (M1)" />
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
            <div class="meta" data-role="meta">Waiting for bootstrap...</div>
          </main>
        </div>
      </div>
    `;
  }
}

customElements.define("portal-shell", PortalShell);
