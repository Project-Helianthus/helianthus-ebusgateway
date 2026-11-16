# Operator records

An operator record is a graph record whose origin is an operator
declaration, not a native observation. Examples are a configured meter role,
a battery attached to an inverter, or a resource that exists only because the
operator named it. Each one carries an `operator` origin with the audit
evidence of the declaration that created it. It never counts as native
qualification.

The gateway's configuration provider publishes every operator record. The
code is in `cmd/gateway/operator_record_publisher.go`. The preflight wires it
through `composeOperatorRecords` in `cmd/gateway/operator_record_composition.go`.
The configuration declares no operator record yet. With nothing configured,
the provider admits no source, declares no asset and publishes nothing, so the
published output of every installation is unchanged.

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
  asset's packs.
- **A resource that only configuration knows** is homed in an asset whose
  single source is the provider (`operatorRecordPublisher.DeclareHome`). That
  asset is graph-enforced. It holds no binding and no fact, and its resources
  are anchored by an `operator_slot`.
- **An operator identity declaration in the form of an identity link** names
  the driver's binding. Only the binding's source can publish it, so it stays
  in the driver's own batch. The provider does not publish it.

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

The kernel's own checks still apply to the batch. They cover catalog
membership, the basis classes a definition admits, end kinds and revision
order. A preflight refusal is fatal, as a declaration collision is, with one
exception: at startup no driver has published yet, so a set whose starting end
does not resolve, or whose asset is not declared yet, stays pending instead of
failing. The provider touches nothing for it and applies it again after each
later publication of the hub, until the owner's resource exists. A pending set
that a later attempt refuses for another reason is dropped and not published;
every such refusal of one attempt is logged on its own, so none is lost when
one publication resolves several sets.

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
