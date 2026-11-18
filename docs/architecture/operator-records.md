# Operator records

An operator record is a graph record whose origin is an operator
declaration, not a native observation. Examples are a configured meter role,
a battery attached to an inverter, or a resource that exists only because the
operator named it. Each one carries an `operator` origin with the audit
evidence of the declaration that created it. It never counts as native
qualification.

The gateway's configuration provider publishes every operator record. The
code is in `cmd/gateway/operator_record_publisher.go`. The records come from
the operator-record section of the Portal configuration (see
[Portal configuration](portal-config-sharing.md), section "Operator-record
section"), which the Portal configuration runtime feeds to the provider. The
preflight also wires the provider through `composeOperatorRecords` in
`cmd/gateway/operator_record_composition.go` and loads the configured record
set. An empty section still admits no source, declares no asset and publishes
nothing, so the published output of an installation without an accepted
section is unchanged. A section that does not load is unavailable: startup
logs the closed code and publishes nothing, and semantic preflight continues
so the shell still starts. Startup declares a configuration-only home for the
configuration provider. It does not declare a home a driver already owns, and
it does not declare a home the record set does not mark configuration-only,
so the driver that discovers that home can claim it. The shape of the
identifier is not ownership. A home a driver already owns stays with that
driver. The configured set is still composed: it is published when that owner
has admitted the provider, it stays pending while the home or its starting
end is not there yet, and a home the owner has not opened to the provider is
reported and left unpublished.

## Who publishes it

The provider is one hub-native source of the semantic publication hub,
`source:gateway-configuration`, with the driver label `gateway-configuration`.
It admits its source the first time it publishes. It publishes on its own key:
its own source, epoch and generation. It never publishes inside a driver's
batch sequence.

- **A record whose starting end is a driver's resource** is published on that
  driver's asset. Examples are a relation instance from an inverter to an
  array, or a meter role on a meter. The asset's owner names the provider as a
  declared contributor (`operatorRecordPublisher.Contributor`). The
  contributor's packs are the packs of the record definitions, a subset of the
  asset's packs. The hub refuses a relation instance of any other pack, so the
  provider cannot publish outside the packs its contributor declaration names.
- **A resource that only configuration knows** is homed in an asset whose
  single source is the provider (`operatorRecordPublisher.DeclareHome`). That
  asset is graph-enforced. It holds no binding and no fact, and its resources
  are anchored by an `operator_slot`.
- **An operator identity declaration in the form of an identity link** names
  the home asset, the native resource and the configuration basis class. The
  section records it as `identity_link`. Only the binding's source can publish
  it, so it stays in the driver's own batch. The provider does not publish it.
  It declares the link to the hub (`Hub.SetOperatorLinks`) for the source
  whose binding observes the native resource, with the declaration's audit
  evidence as its basis, and withdraws that declaration when the section no
  longer holds the link. While no binding of the home observes the native
  resource, the link waits with reason `native_unobserved` and is retried
  after later publications. Once declared, its status stays waiting with
  reason `identity_link_pending`, also when its home holds records that were
  published: the declaration was applied, and it was not left unapplied.

## Records and revisions

`operatorRecordPublisher.Set(asset, records, audit)` replaces the desired
record set of one asset. The provider then publishes the difference from the
asset's current snapshot as one batch. It builds that batch under the asset
commit lock from the snapshot it reads there.

- A new or changed record is upserted with its next revision. A record keeps
  its own revision sequence, so a record that is withdrawn and later declared
  again continues its revisions.
- An unchanged record publishes nothing. An unchanged set submits no batch.
- A relation that is no longer declared is withdrawn. A resource that is no
  longer declared is upserted as `withdrawn` with a new revision, and its
  identity record stays in the snapshot.
- Every record carries the declaration's audit evidence: as origin evidence,
  as the basis of a relation instance (basis class `configuration`), and as the
  anchor evidence of a resource.

## Refusals

A record set is checked before anything is submitted. A refused set leaves the
snapshot, the event stream, the provider's admission and its desired records
as they were. The refusal names the asset, the resource and one reason:

| Reason | Condition |
|---|---|
| `operator.record.starting_end_unresolved` | a relation's starting end is neither a current resource of the asset nor a resource of the same set |
| `operator.record.audit_missing` | the set carries no valid declaration digest |
| `operator.record.duplicate` | the set names one record key twice |
| `operator.record.native_unobserved` | an identity link names a native resource that no binding of the asset observes yet; the set waits and is retried |

The kernel's own checks still apply to the batch. They cover catalog
membership, the basis classes a definition admits, end kinds and revision
order. A preflight refusal is fatal, as a declaration collision is, with one
exception: at startup no driver has published yet, so a set whose starting end
does not resolve, or whose asset is not declared yet, stays pending instead of
failing. The provider touches nothing for it and applies it again after each
later publication of the hub, until the owner's resource exists. A pending set
that a later attempt refuses for another reason is dropped and not published;
every such refusal of one attempt is logged on its own, so none is lost when
one publication resolves several sets. A Portal replacement supersedes the
sets still pending from startup. A relation the operator removed is not
published when its home asset appears later, and a relation the operator
replaced is the one that is published.

## Lifecycle against fences

- **The driver's fence does not block the provider.** The provider has its own
  control gate on the shared asset. Its records are published, revised and
  withdrawn while the driver's generation is fenced or the driver is stopped.
- **Configuration records survive fences and retirements.** A fence or
  retirement removes only records with a native basis whose binding is no
  longer current. A `configuration` relation instance stays until the provider
  withdraws it.
- **Withdrawal does not depend on the driver.** A relation configured on a
  driver that stays stopped can still be withdrawn, because the provider
  publishes the withdrawal on its own key.
- **Resources do not change on fences.** The driver's native resource stays
  `current` as an identity record after the fence. The relation instance that
  names it keeps resolving.

## Scenarios

`cmd/gateway/operator_record_acceptance_test.go` follows one declaration from
the configuration document to the snapshot and to a graph cut. Its fixtures
are synthetic families and use the catalog's packs. Each scenario below is one
test.

- **From the document to the cut.** The operator-record section declares a
  meter seen through two configuration-only homes (a pairing) and a `measures`
  relation from a driver's grid connection to the first view. After the
  replacement both records show `published`. The relation is on the driver's
  asset with basis class `configuration`, an operator origin and the digest of
  its declaration as audit evidence. Each home holds one current
  `operator_slot` view of the meter, and the provider is its only source. In a
  complete graph cut the relation is a qualified, `coherent` edge, and the
  only finding is `same_unit` for the two views.
- **Published while the driver is fenced.** After the driver's generation is
  fenced, the configured relation is still there. A second meter and its
  relation added to the section are published. The driver's resource stays
  `current`, and both relations are coherent in the cut.
- **Withdrawn while the driver is stopped.** The driver's source epoch is
  retired, as when a driver stops. The configured relations survive. Removing
  the second meter and its relation from the section withdraws the relation
  and marks the meter `withdrawn`. The driver stays stopped, and the relation
  that stays configured stays coherent.
- **A configured relation grants no control.** A meter declared to measure a
  driver's EVSE is a coherent edge. An intent on that EVSE is still refused
  without a send (`ambiguous_route`: no eligible route) while the driver
  publishes no qualified control capability of its own. Once the driver
  publishes one, the same intent is admitted and routed to the driver's own
  binding. The relation is the same in both publications. The catalog has no
  `controls`-type definition yet; a `measures` instance never routes an
  operation.
- **A join of different native anchors is refused.** A pairing that names two
  natively observed meters with different native slots would replace both
  native anchors with one operator slot. The kernel refuses that change in
  each home with `identity_drift`. Both records show `refused`, both snapshots
  stay byte-identical, and the cut reports no `same_unit` finding.

## The remaining limit

An operator identity declaration in the form of an identity link names the
driver's binding, and only the binding's source may publish it. The provider
therefore never publishes it. `Hub.SetOperatorLinks` keeps it pending for the
driver's source, and `Tx.OperatorLinks` hands it to the driver's next
transaction. Declaring it publishes nothing. A driver batch that does not
carry the pending links leaves them pending. The declaration appears only
after the driver's next batch that carries them, so it waits while the driver
is fenced or stopped. The acceptance test covers this limit too.

No family publisher carries the pending links yet. Until a family adopts
`Tx.OperatorLinks`, an operator identity declaration is never published.

The section records an identity link: the home asset, the native resource and
the configuration basis class. Startup loads the configured record set, and
the provider declares each configured link to the hub for the binding's
source. An empty section still admits nothing.

No family composition names the provider as a contributor of a shared asset
yet. Startup does declare a configuration-only home, whose single source is
the provider. Ownership is explicit and does not follow the identifier's
shape. A home is configuration-owned when its record set holds a resource
that only configuration knows and no driver has declared that home; startup
declares it and publishes the anchor or pairing. A home a driver has already
declared stays with that driver, and startup does not declare a contributor
for it. A home the record set does not mark configuration-only stays
undeclared, so the driver that discovers it can claim it, and a record on
that home stays pending. The acceptance test makes the owner-side contributor
declarations itself, as an owner would, so it proves the path from the
document to the cut once the owner has declared them. A Portal replacement
publishes nothing for an asset whose owner has not admitted the provider, and
it supersedes a startup-pending relation the operator removed so that
relation is not published when the asset appears.

## Ownership

A driver is never a source of the provider's configuration-only asset, so it
cannot publish there. On a shared asset the hub records which driver created
each inferred candidate. Refusing another driver's revision or withdrawal of a
configuration relation instance or of an operator resource needs the same
record for graph records, which the hub does not keep yet. Until it does, the
owner's family publishers must not upsert or withdraw records they did not
create.

See [semantic resource identity](semantic-resource-identity.md) for the
allocation of resource identifiers. See the
[runtime driver provider contract](runtime-driver-provider-contract-v1.md),
section "Control gates of a shared asset", for the contributor gates.
