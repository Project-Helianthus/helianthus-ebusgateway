const fixtureURL = "../../contributionv1/testdata/five-domain-catalog.json";
const stateURL = "../../contributionv1/testdata/state-matrix.json";

const text = (value) => document.createTextNode(String(value));
function element(name, attributes = {}, children = []) {
  const node = document.createElement(name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  for (const child of children) node.append(child instanceof Node ? child : text(child));
  return node;
}
function replace(container, nodes) { container.replaceChildren(...nodes); }
function findResource(catalog, id) { return catalog.resources.find((resource) => resource.id === id); }

export function initialNavigation(catalog) {
  const resource = findResource(catalog, catalog.navigation.default_resource);
  return { perspective: catalog.navigation.default_perspective, resource: resource.id, capability: resource.capabilities[0].id };
}

// Unknown selections retain state; no product or manifest identity routes UI.
export function reduceNavigation(catalog, state, selection) {
  if (selection.kind === "perspective" && catalog.navigation.perspectives.some((item) => item.id === selection.id)) return {...state, perspective: selection.id};
  if (selection.kind === "resource") {
    const resource = findResource(catalog, selection.id);
    if (resource) return {...state, resource: resource.id, capability: resource.capabilities[0].id};
  }
  const resource = findResource(catalog, state.resource);
  if (selection.kind === "capability" && resource?.capabilities.some((item) => item.id === selection.id)) return {...state, capability: selection.id};
  return state;
}

export function visibleContributions(catalog, state) {
  return catalog.contribution_states
    .filter((item) => item.resource_id === state.resource && item.capability_id === state.capability)
    .map((item) => ({manifest: catalog.manifests.find((manifest) => manifest.manifest_id === item.manifest_id), state: item.state}))
    .filter((item) => item.manifest);
}

function renderPerspectives(catalog, state, select) {
  replace(document.querySelector("#perspectives"), catalog.navigation.perspectives.map((item) => {
    const button = element("button", {type: "button", "aria-pressed": String(item.id === state.perspective)}, [item.label]);
    button.addEventListener("click", () => select({kind: "perspective", id: item.id}));
    return button;
  }));
}
function renderResources(catalog, state, select) {
  replace(document.querySelector("#resources"), catalog.resources.map((resource) => {
    const resourceButton = element("button", {type: "button", "aria-pressed": String(resource.id === state.resource)}, [resource.domain]);
    resourceButton.addEventListener("click", () => select({kind: "resource", id: resource.id}));
    const capabilities = element("div", {class: "capabilities", "aria-label": `${resource.domain} capabilities`}, resource.capabilities.map((capability) => {
      const button = element("button", {type: "button", "aria-pressed": String(resource.id === state.resource && capability.id === state.capability), "data-state": capability.state}, [capability.label]);
      button.addEventListener("click", () => select({kind: "capability", id: capability.id}));
      return button;
    }));
    return element("article", {class: "resource-domain", "data-state": resource.state, "data-resource": resource.id}, [resourceButton, element("ul", {}, resource.items.map((item) => element("li", {}, [item]))), element("p", {}, [resource.detail]), capabilities]);
  }));
}
function renderCatalog(catalog, state) {
  const visible = visibleContributions(catalog, state);
  const nodes = [element("p", {class: "perspective-context", "data-perspective": state.perspective}, [`Perspective: ${state.perspective}`])];
  if (!visible.length) nodes.push(element("p", {class: "state state-empty"}, ["No contribution is admitted for this fixture selection."]));
  for (const {manifest, state: contributionState} of visible) {
    const fields = manifest.fields.map((field) => `${field.label.default} (${field.unit_ref.id})`);
    const actions = manifest.actions.length ? manifest.actions.map((action) => action.label.default).join(", ") : "No declared operation";
    nodes.push(element("article", {class: `contribution state state-${contributionState}`, "data-manifest": manifest.manifest_id, "data-state": contributionState}, [element("h3", {}, [manifest.manifest_id]), element("p", {}, [`${contributionState}: ${fields.join(", ")}`]), element("p", {}, [`Actions: ${actions}`]), element("p", {}, [`Host renderer: ${manifest.views.map((view) => view.renderer).join(", ")}`])]));
  }
  replace(document.querySelector("#catalog"), nodes);
}
function renderStates(states) { replace(document.querySelector("#states"), states.map((state) => element("article", {class: `state state-${state.id}`}, [element("strong", {}, [state.id]), element("span", {}, [state.text])]))); }
export function discoverableActions(actions) { return actions.filter((action) => action.visible); }
function renderActions(actions) {
  replace(document.querySelector("#actions"), discoverableActions(actions).map((action) => {
    const attributes = {type: "button", "aria-describedby": `reason-${action.id}`}; if (!action.enabled) attributes.disabled = "";
    return element("article", {class: "action-case"}, [element("button", attributes, [action.id]), element("span", {id: `reason-${action.id}`}, [action.text])]);
  }));
}
function renderB503(b503) { replace(document.querySelector("#b503"), [element("p", {}, [`Target context: ${b503.target_context}; transport: ${b503.transport}. ${b503.issue}.`]), element("p", {}, [`Native availability: ${b503.availability.join(", ")}`]), element("p", {}, [`INT-10 proof: ${b503.test_ids.join(", ")}`])]); }

export function render(catalog, matrix, state = initialNavigation(catalog)) {
  const select = (selection) => render(catalog, matrix, reduceNavigation(catalog, state, selection));
  renderPerspectives(catalog, state, select); renderResources(catalog, state, select); renderCatalog(catalog, state); renderStates(matrix.states); renderActions(matrix.actions); renderB503(matrix.b503);
  document.querySelector("#announcement").textContent = `Loaded ${catalog.manifests.length} isolated fixture contributions.`;
}
export async function start(load = fetch) {
  const [catalogResponse, stateResponse] = await Promise.all([load(fixtureURL), load(stateURL)]);
  if (!catalogResponse.ok || !stateResponse.ok) throw new Error("Fixture catalog unavailable");
  render(await catalogResponse.json(), await stateResponse.json());
}
if (typeof document !== "undefined") start().catch((error) => { document.querySelector("#announcement").textContent = `Prototype unavailable: ${error.message}`; });
