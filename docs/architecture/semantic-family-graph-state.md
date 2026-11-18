# Semantic family graph state

This page lists every family whose facts the gateway composes on the semantic
publication hub and says whether that family is in the graph. A family is in
the graph when its production declarations ask the hub for a graph-enforced
kernel and its publishers publish resources and relation instances as the
[graph contract](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/graph.md)
and the family's mapping gate define them.

The switch is the gateway table `semanticFamilyGraphTable` in
`cmd/gateway/semantic_family_graph.go`. `Hub.GraphState()` reports each
declared asset as `enforced` or `not_enforced` and reports the element catalog
digest once. A family that is off keeps its assets ungraphed, so its published
output stays as it was.

## State

| Family | Production publisher | Graph state | Why |
| --- | --- | --- | --- |
| EVSE | The configured charger's EVSE asset | Not yet in the graph | The table entry is off. The publisher publishes no resource or relation instance; its EVSE and connector are named only by configured identifiers in dimension values. |
| PV (logger-attached) | The PV class of inverters read behind a logger or a dongle | In the graph | The `pv.logger` table entry is on. Each child asset publishes its PV system, inverter and DC inputs as resources with the gate's `gate_rule` relation instances, each checked against the gate before it is offered. The logger is generic equipment in its own asset; its `relation.relays` instances are withheld until the kernel can name the core relays definition. A withheld child keeps its records as `withdrawn`, and no absence statement is published because the gate states none. See [PV identity consumer inventory](pv-identity-consumer-inventory.md). |
| PV (SunSpec) | The SunSpec PV class | Not yet in the graph | The `pv` table entry is off. The publisher publishes no resource or relation instance; inverter and system dimension values are still built from the asset. |
| Storage | The configured battery storage asset | Not yet in the graph | The table entry is off. The publisher publishes no resource or relation instance and still uses the asset identifier as the storage dimension value. |
| Thermal | The eBUS boiler asset | Not yet in the graph | The table entry is off. The publisher publishes no resource or relation instance; the circuit, zone and energy-total joins of the eBUS views are still made in gateway code. |
| Infrastructure | None | Proven offline, without a production publisher | The family has no table entry because no gateway code publishes the infrastructure pack. A synthetic publisher declared with enforcement on proves the pack graph-enforced, as described below. |

Every family that is not yet in the graph leaves the graph through its own
slice of the same issue, once its mapping gate is accepted and its publishers
publish resources and relation instances. Switching a family on changes what
it may publish: every pack its declarations register must then be covered by
the element catalog.

## Infrastructure proven offline

`cmd/gateway/semantic_infrastructure_graph_test.go` runs the infrastructure
pack graph-enforced on synthetic input, with the operator-record path that
production uses for configuration:

- One synthetic site publisher declares its asset with enforcement on and names
  the configuration provider as a contributor. The hub reports the asset
  `enforced` under the pinned element catalog.
- The publisher observes a site, the site's grid connection and one meter as
  current resources of that asset. One binding observes the grid connection;
  a second binding observes the meter and publishes the meter's qualified read
  capability.
- Power sits on the grid connection and energy on the meter, as the gateway
  meter gate places them: import and export active power keyed by the grid
  connection, import and export energy keyed by the meter.
- An operator-record section declares the meter role as an instance of
  `infrastructure.relation.grid_connection_meter` and the site's
  `infrastructure.relation.site_grid_connection` instance. The configuration
  provider publishes both into the grid connection's home asset with basis
  class `configuration`, an operator origin and the declaration's audit
  evidence. Both are `coherent` edges of a complete cut, with no finding.
- The role never changes a fact definition. Publishing it leaves every fact
  envelope unchanged. With the role configured, power keyed by the meter and
  energy keyed by the grid connection are still refused: a field under another
  field's dimension with `invalid_value`, and a field's own dimension naming a
  resource of another kind with `identity_drift`. A refused batch leaves the
  asset byte-identical.
- An intent on the grid connection is refused without a send, both through a
  capability of the grid connection that the publisher does not publish and
  through the meter's read capability. The pack defines
  no operation, so no capability of the grid connection can carry one, and the
  kernel refuses with `definition_owner_missing`. The role and the asset stay
  unchanged.

## Limits

- The proof is offline and synthetic. It is not a device support claim, and
  it qualifies no meter. The gateway meter gate keeps every native meter value
  withheld until a meter class contract exists.
- The gate anchors the site, the grid connection and the meter by an operator
  label. The synthetic publisher anchors them by its own native slots, because
  a fact's subject must be a resource of the publishing asset that the
  publisher's binding observes.
- The intent refusal covers the pack's lack of operations. It is raised before
  capability admission, so it does not show whether the role reaches or
  withholds a capability; that needs an operation owned by a kernel pack.
- No production composition declares the configuration provider as a
  contributor of an infrastructure asset, and the startup configuration holds
  no infrastructure record. A production infrastructure publisher remains
  outside this release.

See [operator records](operator-records.md) for the configuration provider and
[semantic resource identity](semantic-resource-identity.md) for resource
identifiers.
