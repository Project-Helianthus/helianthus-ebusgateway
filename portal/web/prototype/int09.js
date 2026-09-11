const fixtureURL = "../../contributionv1/testdata/five-domain-catalog.json";
const stateURL = "../../contributionv1/testdata/state-matrix.json";
const perspectives = ["Registry", "Plane", "Lens", "Compare", "Provenance", "History", "Native"];

const text = (value) => document.createTextNode(String(value));
function element(name, attributes = {}, children = []) {
  const node = document.createElement(name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  for (const child of children) node.append(child instanceof Node ? child : text(child));
  return node;
}
function replace(container, nodes) { container.replaceChildren(...nodes); }

function renderPerspectives() {
  replace(document.querySelector("#perspectives"), perspectives.map((name, index) => element("button", {type: "button", "aria-pressed": index === 0 ? "true" : "false"}, [name])));
}
function renderResources(resources) {
  replace(document.querySelector("#resources"), resources.map(({domain, items, detail}) => element("article", {class: "resource-domain"}, [element("h3", {}, [domain]), element("ul", {}, items.map((item) => element("li", {}, [item]))), element("p", {}, [detail])])));
}
function renderCatalog(manifests) {
  replace(document.querySelector("#catalog"), manifests.map((manifest) => {
    const fields = manifest.fields.map((field) => `${field.label.default} (${field.unit_ref.id})`);
    const actions = manifest.actions.length ? manifest.actions.map((action) => action.label.default).join(", ") : "No declared operation";
    const availability = manifest.manifest_id === "portal.infrastructure" ? "unavailable" : manifest.manifest_id === "portal.storage" ? "partial" : "available";
    return element("article", {class: "contribution", "data-manifest": manifest.manifest_id}, [
      element("h3", {}, [manifest.manifest_id]),
      element("p", {class: `state state-${availability}`}, [`${availability}: ${fields.join(", ")}`]),
      element("p", {}, [`Actions: ${actions}`]),
      element("p", {}, [`Host renderer: ${manifest.views.map((view) => view.renderer).join(", ")}`])
    ]);
  }));
}
function renderStates(states) {
  replace(document.querySelector("#states"), states.map((state) => element("article", {class: `state state-${state.id}`}, [element("strong", {}, [state.id]), element("span", {}, [state.text])])));
}
function renderActions(actions) {
  replace(document.querySelector("#actions"), actions.map((action) => {
    if (!action.visible) return element("article", {class: "action-case"}, [element("strong", {}, [action.id]), element("span", {}, [" Action intentionally absent from discovery."])]);
    const attributes = {type: "button", "aria-describedby": `reason-${action.id}`};
    if (!action.enabled) attributes.disabled = "";
    return element("article", {class: "action-case"}, [element("button", attributes, [action.id]), element("span", {id: `reason-${action.id}`}, [action.text])]);
  }));
}
function renderB503(b503) {
  replace(document.querySelector("#b503"), [element("p", {}, [`Target context: ${b503.target_context}; transport: ${b503.transport}. ${b503.issue}.`]), element("p", {}, [`Native availability: ${b503.availability.join(", ")}`]), element("p", {}, [`INT-10 proof: ${b503.test_ids.join(", ")}`])]);
}

export function render(catalog, matrix) {
  renderPerspectives(); renderResources(catalog.resources); renderCatalog(catalog.manifests); renderStates(matrix.states); renderActions(matrix.actions); renderB503(matrix.b503);
  document.querySelector("#announcement").textContent = `Loaded ${catalog.manifests.length} isolated fixture contributions.`;
}
export async function start(load = fetch) {
  const [catalogResponse, stateResponse] = await Promise.all([load(fixtureURL), load(stateURL)]);
  if (!catalogResponse.ok || !stateResponse.ok) throw new Error("Fixture catalog unavailable");
  render(await catalogResponse.json(), await stateResponse.json());
}
if (typeof document !== "undefined") start().catch((error) => { document.querySelector("#announcement").textContent = `Prototype unavailable: ${error.message}`; });
