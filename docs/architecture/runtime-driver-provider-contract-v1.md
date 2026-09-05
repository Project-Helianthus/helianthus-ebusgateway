# Runtime driver/provider and north-south contract v1

Status: proposed software 0.7 contract. This document specifies a subsequent
implementation; the current binary does not expose the lifecycle service or a
universal manager for every protocol family.

Contract ID: `helianthus.gateway.driver-runtime/v1`

Gateway baseline: `Project-Helianthus/helianthus-ebusgateway` commit
[`e31106c9c726fbb8df7546901763e19b93659e72`](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/e31106c9c726fbb8df7546901763e19b93659e72)

Program source: software 0.7 guide commit
[`ec6050fb31738f201c96203c52be0702671df343`](https://github.com/Project-Helianthus/helianthus-execution-plans/tree/ec6050fb31738f201c96203c52be0702671df343/software-stabilization-07-08.implementing)

## 1. Decision and boundary

The gateway owns one runtime control service and one composition boundary. It
adapts existing native runtimes; it does not replace their protocol state
machines, qualification, I/O, retry, reconnect, framing, authorization, raw
evidence, acknowledgement, readback, or close mechanisms.

The contract covers:

- deterministic `list` and `get` views;
- `start`, `stop`, and `restart` without restarting the gateway process;
- persistent desired state separate from observed runtime state;
- optimistic revision checks and persistent idempotency records;
- source-epoch and driver-generation fencing;
- capability activation, admission, withdrawal, drain, and close proof;
- native observation to qualified semantic publication and projection;
- semantic intent to one admitted native route and truthful outcome;
- bounded causal loop and echo suppression; and
- compatibility with current public surfaces during staged migration.

This contract adds no autonomous control, optimization, fallback policy, or
write authority. A semantic capability describes a qualified possibility. A
current admission, authority decision, precondition decision, exact route, and
deadline are still required for every operation.

## 2. Semantic dependency

Reusable protocol-neutral types belong to the public semantic owners, not this
repository. This contract depends on the accepted revision of
`helianthus.semantic.kernel/v1` published by
[`Project-Helianthus/helianthus-docs-semantic`](https://github.com/Project-Helianthus/helianthus-docs-semantic)
and implemented by
[`Project-Helianthus/helianthus-semreg`](https://github.com/Project-Helianthus/helianthus-semreg).

The accepted dependency is docs-semantic
[#3](https://github.com/Project-Helianthus/helianthus-docs-semantic/issues/3),
merged PR [#5](https://github.com/Project-Helianthus/helianthus-docs-semantic/pull/5),
remote-main commit `b16667d719defc7b0fef0400ee3ad387469018ac`, specifically
[`api/v1/kernel.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/b16667d719defc7b0fef0400ee3ad387469018ac/api/v1/kernel.md),
[`serialization.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/b16667d719defc7b0fef0400ee3ad387469018ac/api/v1/serialization.md),
[`acceptance.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/b16667d719defc7b0fef0400ee3ad387469018ac/api/v1/acceptance.md),
and
[`acceptance-vectors.json`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/b16667d719defc7b0fef0400ee3ad387469018ac/api/v1/acceptance-vectors.json).
That squash commit has contract content identical to independently accepted PR
HEAD `395733fd19c1e5d92a9d1fbc0afb79bddd107a22`. The complete exact-HEAD Astra
audit returned `NO_BLOCKING_FINDINGS`; issue #3 is closed. A future successor
requires its own compatibility reconciliation before replacing this pin.

| Semantic package | Types consumed here |
|---|---|
| `semreg/v1` | `ContractVersion`, `SemanticVersion`, `PackRef`, `DefinitionRef`, `DefinitionIndex`, `PredicateOp`, `ErrorID`, `AssetID`, `SourceID`, `SourceEpochID`, `SourceDescriptor`, `SourceState`, `NativeBindingID`, `NativeBinding`, `BindingState`, `OriginRef`, `ClockEpochID`, `SnapshotID`, `RevisionVector`, `FactKey`, `FactCandidate`, `FactEnvelope`, `SourcePathRef`, `DerivationInput`, `ServiceInstance`, `CapabilityInstance`, `CapabilityInstanceID`, `PolicyID`, `EvidenceRef`, `Digest`, `BatchID`, `GenerationFence`, `PublicationBatch`, `PublicationCursor`, `Snapshot`, `EvaluationContext`, `EvaluatedFact`, `EvaluationView`, `Selection`, `SelectionPolicy`, `CausalContext`, `PackValidator` |
| `semreg/v1/operation` | `Intent`, `CapabilityRequirement`, `Precondition`, `ExpectedEffect`, `OperationPackValidator`, `Route`, `DispatchEvidence`, `Acknowledgement`, `Readback`, `ExecutionRecord` |
| `semreg/v1/projection` | versioned target manifest, projection result, and loss report |

The dependency must preserve these distinct identities:

- process/source epoch;
- native transport connection generation;
- driver generation;
- semantic snapshot and object revisions;
- lifecycle operation ID;
- intent ID and idempotency key; and
- causal correlation ID.

No counter substitutes for another. Presentation selection, projection choice,
public alias, and compatibility identity never choose a native route.

The dependency preserves `Precondition.CandidateID` and
`CandidateRevision`, `Intent.ExpectedEffect`, `OperationPackValidator`,
mandatory `GenerationFence` supersession of every older unfenced generation,
`DispatchEvidence.Started`/`Completed` and `Readback.Evaluation` as complete
`EvaluationContext` records, the entered-receiver `CausalContext.Path` order,
pure `SelectPresentation(Snapshot, EvaluationView, FactKey, PolicyID,
SemanticVersion)` with fully snapshot/evaluation-bound `Selection`, explicit
`SourceState`/`BindingState` tombstones, and `PackRef`/`DefinitionIndex`
ownership for every service, capability, operation, and effect definition.

## 3. Ownership

| Owner | Required responsibility |
|---|---|
| Gateway runtime control | Driver catalog, desired-state persistence, lifecycle serialization, revision/idempotency decisions, per-driver isolation, admission coordination, composition, and public lifecycle projection |
| Native driver/adapter | Native endpoint identity, connection and protocol lifecycle, discovery, qualification, decode, raw evidence, request construction, native retry/reconnect, native authorization, ACK/readback interpretation, and resource close proof |
| Semantic kernel and packs | Protocol-neutral identities, facts, services, capabilities, operation intents/outcomes, provenance, validation, conflict, and projection-loss types |
| Native-to-semantic binding | Exact profile/version and unique `PackRef`/`DefinitionIndex` mapping from qualified native evidence to semantic facts/capabilities and from a semantic operation and typed `ExpectedEffect` to one typed native operation and pack-evaluated readback predicate |
| Public output/consumer | Target projection and compatibility behavior; never upstream qualification or native route selection |

Common lifecycle code must not switch on protocol, vendor, model, register,
feature, or function code. Native mappings register against an exact protocol,
profile, profile version, capability definition, and operation definition.

## 4. Control-plane types

The subsequent Go implementation is versioned under the gateway module. The
shapes below are normative Go-like definitions; names imported from semreg use
the accepted dependency without local copies. `semv1` denotes `semreg/v1` and
`operationv1` denotes `semreg/v1/operation` in the snippets.

```go
package driverv1

type ContractID string        // fixed: helianthus.gateway.driver-runtime/v1
type DriverID string          // stable configured runtime identity
type DriverRevision uint64    // revision of one persistent desired record
type CatalogRevision uint64   // revision-consistent list view
type DriverGeneration uint64  // starts at 1 inside one SourceEpochID
type LifecycleOperationID string
type LifecycleSequence uint64  // monotonic per DriverID, starts at 1
type IdempotencyKey string

type LifecycleOperationKind string
const (
    LifecycleStart   LifecycleOperationKind = "START"
    LifecycleStop    LifecycleOperationKind = "STOP"
    LifecycleRestart LifecycleOperationKind = "RESTART"
)

type LifecycleOperationState string
const (
    OperationPending   LifecycleOperationState = "PENDING"
    OperationActive    LifecycleOperationState = "ACTIVE"
    OperationCompleted LifecycleOperationState = "COMPLETED"
)

type ReasonCode string
const (
    ReasonNone                  ReasonCode = "NONE"
    ReasonConfigDisabled        ReasonCode = "CONFIG_DISABLED"
    ReasonOperatorStopped       ReasonCode = "OPERATOR_STOPPED"
    ReasonStartRequested        ReasonCode = "START_REQUESTED"
    ReasonStopRequested         ReasonCode = "STOP_REQUESTED"
    ReasonConfigInvalid         ReasonCode = "CONFIG_INVALID"
    ReasonProviderUnavailable   ReasonCode = "PROVIDER_UNAVAILABLE"
    ReasonDependencyUnavailable ReasonCode = "DEPENDENCY_UNAVAILABLE"
    ReasonRuntimeNotReady       ReasonCode = "RUNTIME_NOT_READY"
    ReasonCapabilityDegraded    ReasonCode = "CAPABILITY_DEGRADED"
    ReasonRetryScheduled        ReasonCode = "RETRY_SCHEDULED"
    ReasonRetryExhausted        ReasonCode = "RETRY_EXHAUSTED"
    ReasonStopTimeout           ReasonCode = "STOP_TIMEOUT"
    ReasonCloseUnconfirmed      ReasonCode = "CLOSE_UNCONFIRMED"
    ReasonInternalError         ReasonCode = "INTERNAL_ERROR"
)

type Reason struct {
    Code      ReasonCode
    Retryable bool
}

type NativeContractRef struct {
    Owner    string // public native owner repository/module
    Contract string // owner-defined operation/profile contract
    Version  string // exact owner-defined version or revision
}

type GenerationKey struct {
    DriverID    DriverID
    SourceID    semv1.SourceID
    SourceEpoch semv1.SourceEpochID
    Generation  DriverGeneration
}

type LifecycleCorrelation struct {
    Generation  GenerationKey
    OperationID LifecycleOperationID
}

type ControlErrorCode string
const (
    ErrorInvalidArgument      ControlErrorCode = "INVALID_ARGUMENT"
    ErrorPermissionDenied     ControlErrorCode = "PERMISSION_DENIED"
    ErrorNotFound             ControlErrorCode = "NOT_FOUND"
    ErrorRevisionConflict     ControlErrorCode = "REVISION_CONFLICT"
    ErrorIdempotencyConflict  ControlErrorCode = "IDEMPOTENCY_CONFLICT"
    ErrorFailedPrecondition   ControlErrorCode = "FAILED_PRECONDITION"
    ErrorUnavailable          ControlErrorCode = "UNAVAILABLE"
    ErrorDeadlineExceeded     ControlErrorCode = "DEADLINE_EXCEEDED"
    ErrorCloseUnconfirmed     ControlErrorCode = "CLOSE_UNCONFIRMED"
)

type ControlError struct {
    Code        ControlErrorCode
    Message     string
    Retriable   bool
    Current     *DriverView
    OperationID *LifecycleOperationID
}

type DesiredState string
const (
    DesiredRunning DesiredState = "RUNNING"
    DesiredStopped DesiredState = "STOPPED"
)

type ObservedState string
const (
    ObservedDisabled ObservedState = "DISABLED"
    ObservedStopped  ObservedState = "STOPPED"
    ObservedStarting ObservedState = "STARTING"
    ObservedRunning  ObservedState = "RUNNING"
    ObservedDegraded ObservedState = "DEGRADED"
    ObservedBackoff  ObservedState = "BACKOFF"
    ObservedStopping ObservedState = "STOPPING"
    ObservedFailed   ObservedState = "FAILED"
)

type DesiredRecord struct {
    DriverID       DriverID
    State          DesiredState
    Revision       DriverRevision
    LastOperation  LifecycleOperationID
    UpdatedAtUTC   time.Time
}

type LifecycleOperationRecord struct {
    OperationID     LifecycleOperationID
    DriverID        DriverID
    Sequence        LifecycleSequence
    Kind            LifecycleOperationKind
    State           LifecycleOperationState
    ExpectedRevision DriverRevision
    DesiredRevision DriverRevision
    RequestDigest   string
    AcceptedAtUTC   time.Time
    StartedAtUTC    *time.Time
    CompletedAtUTC  *time.Time
    TerminalReason  *Reason
}

type ObservedRecord struct {
    State                 ObservedState
    Reason                Reason
    SourceEpoch           semv1.SourceEpochID
    Generation            DriverGeneration
    NativeRevision        uint64
    EffectiveCapabilities []semv1.CapabilityInstance
    Quarantined           bool
    ActiveOperation       *LifecycleOperationID
    ChangedAtUTC          time.Time
}

type EpochTransitionState string
const (
    EpochTransitionPending  EpochTransitionState = "PENDING"
    EpochTransitionComplete EpochTransitionState = "COMPLETE"
    EpochTransitionBlocked  EpochTransitionState = "BLOCKED"
)

type GenerationReservationState string
const (
    ReservationControlOwned    GenerationReservationState = "CONTROL_OWNED"
    ReservationAvailable       GenerationReservationState = "AVAILABLE"
    ReservationProviderClaimed GenerationReservationState = "PROVIDER_CLAIMED"
    ReservationConsumed        GenerationReservationState = "CONSUMED"
    ReservationRetired         GenerationReservationState = "RETIRED"
)

type GenerationReservationOwner string
const (
    ReservationOwnerNone            GenerationReservationOwner = "NONE"
    ReservationOwnerEpochBarrier    GenerationReservationOwner = "EPOCH_BARRIER"
    ReservationOwnerSemanticCleanup GenerationReservationOwner = "SEMANTIC_CLEANUP"
    ReservationOwnerRecovery        GenerationReservationOwner = "RECOVERY"
    ReservationOwnerLifecycle       GenerationReservationOwner = "LIFECYCLE"
)

type GenerationReservation struct {
    Key                     GenerationKey
    State                   GenerationReservationState
    Owner                   GenerationReservationOwner
    LifecycleOperation      *LifecycleOperationID
    CleanupFailedGeneration *GenerationKey
    NextPublicationSequence uint64
    ClaimedAtUTC            *time.Time
    ConsumedAtUTC           *time.Time
}

type EpochAssetTransition struct {
    AssetID                  semv1.AssetID
    InputSnapshot            semv1.SnapshotID
    ExpectedSemanticRevision uint64
    PriorEpochs              []semv1.SourceEpochID
    Sequence                 uint64
    BatchID                  semv1.BatchID
    BatchDigest              semv1.Digest
    AcceptedSnapshot         *semv1.SnapshotID
}

type SourceEpochTransitionRecord struct {
    DriverID         DriverID
    SourceID         semv1.SourceID
    NewSourceEpoch   semv1.SourceEpochID
    Reservation     GenerationReservation
    Assets           []EpochAssetTransition
    State            EpochTransitionState
    Attempt          uint32
    AttemptLimit     uint32
    DeadlineUTC      time.Time
    NextAttemptAtUTC *time.Time
    LastError        *semv1.ErrorID
}

type DriverView struct {
    Contract ContractID
    Desired  DesiredRecord
    Observed ObservedRecord
}

type ListDriversRequest struct{}
type ListDriversResult struct {
    CatalogRevision CatalogRevision
    Drivers         []DriverView
}

type GetDriverRequest struct { DriverID DriverID }
type GetDriverResult struct {
    CatalogRevision CatalogRevision
    Driver          DriverView
}

type StartDriverRequest struct {
    DriverID         DriverID
    ExpectedRevision DriverRevision
    IdempotencyKey   IdempotencyKey
}
type StopDriverRequest = StartDriverRequest
type RestartDriverRequest = StartDriverRequest

type LifecycleDisposition string
const (
    LifecycleAccepted LifecycleDisposition = "ACCEPTED"
    LifecycleNoOp     LifecycleDisposition = "NO_OP"
)

type LifecycleReceipt struct {
    OperationID LifecycleOperationID
    Sequence    *LifecycleSequence
    Disposition LifecycleDisposition
    Desired     DesiredRecord
}

type ControlService interface {
    ListDrivers(context.Context, ListDriversRequest) (ListDriversResult, error)
    GetDriver(context.Context, GetDriverRequest) (GetDriverResult, error)
    StartDriver(context.Context, StartDriverRequest) (LifecycleReceipt, error)
    StopDriver(context.Context, StopDriverRequest) (LifecycleReceipt, error)
    RestartDriver(context.Context, RestartDriverRequest) (LifecycleReceipt, error)
}
```

`ControlErrorCode` is confined to gateway lifecycle transport and persistence.
It does not rename a semantic rejection. Semantic validation, publication,
evaluation, admission, execution-record, and projection operations return the
kernel's `ErrorID` unchanged. The exhaustive v1 set is:
`invalid_json`, `invalid_contract`, `missing_member`, `invalid_identifier`,
`invalid_decimal`, `invalid_value`, `invalid_enum`, `bounds_exceeded`,
`invalid_time`, `incomparable_clock_epoch`, `invalid_evidence`,
`noncanonical_order`, `digest_mismatch`, `dangling_reference`,
`derivation_cycle`, `identity_not_qualified`, `revision_conflict`,
`sequence_conflict`, `stale_source_epoch`, `stale_driver_generation`,
`capability_not_qualified`, `capability_unavailable`, `ambiguous_route`,
`deadline_expired`, `precondition_failed`, `authority_missing`,
`route_selection_forbidden`, `causal_budget_exceeded`, `echo_suppressed`,
`retry_forbidden`, `projection_incomplete`, `alias_not_routable`,
`invalid_outcome`, `duplicate_key`, `unknown_member`,
`generation_transition_incomplete`, `definition_owner_conflict`, and
`definition_owner_missing`. The gateway applies
the exact eight-level precedence in the pinned semantic serialization contract
when more than one rejection applies. Semantic validators select the one exact
error for overlapping evidence shape, qualification, activation, and reference
failures; gateway composition preserves that result instead of independently
reclassifying it. It never mints a 39th semantic error, maps zero routes to an
invented `NO_ROUTE`, or changes a semantic error into an uppercase lifecycle
error.

An `ACCEPTED` receipt has a non-empty `OperationID` and a non-nil `Sequence`
containing the exact per-driver sequence allocated in the same transaction as
its operation record. A `NO_OP` receipt has an empty operation ID and nil
sequence. The persisted idempotency receipt uses the same representation, so an
identical replay returns the original operation ID and sequence and never
allocates another record.

`DriverID` is stable across process restart and gateway rename. It is not a bus
address, semantic asset ID, native binding ID, or public compatibility alias.
IDs are canonical non-empty UTF-8 strings matching
`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`, with a maximum of 128 bytes.
Every `NativeContractRef` field is non-empty and records the exact public native
owner, owner-defined contract ID, and owner-defined version/revision. It contains
no endpoint, credential, raw payload, or handle.

`ListDrivers` returns all configured drivers sorted bytewise by `DriverID`. The
entries and `CatalogRevision` come from one read transaction. `GetDriver`
returns `NOT_FOUND` for an unknown ID; it never constructs a driver as a read
side effect.

An epoch transition has a non-zero `AttemptLimit` and finite `DeadlineUTC` from
the manager's lifecycle-construction policy. Those bounds persist across
component/reconciler recovery in one source epoch and are not reset by a wakeup.
After `BLOCKED`, only a new accepted lifecycle operation may supply another
bounded budget; it does not reopen admission or provider work before the same
transition barrier completes.

## 5. Mutation, persistence, and reconciliation

Lifecycle calls acknowledge a persistent control decision. They do not claim
that native startup, shutdown, or replacement has completed.

For every mutation, the control service performs one atomic transaction:

1. Validate the caller's lifecycle authority, closed request shape, driver ID,
   non-zero expected revision, non-empty idempotency key, and caller deadline.
2. Canonically hash contract ID, operation name, driver ID, expected revision,
   and all request fields.
3. Look up `(authority principal, idempotency key)` in the persistent ledger.
   An identical hash returns the stored receipt. A different hash returns
   `IDEMPOTENCY_CONFLICT` without state change.
4. Compare `ExpectedRevision` with the current persistent driver revision. A
   mismatch returns `REVISION_CONFLICT` with the current view.
5. Resolve `ACCEPTED` or `NO_OP`. For `ACCEPTED`, allocate the next per-driver
   lifecycle sequence and commit the desired record, typed `PENDING`
   `LifecycleOperationRecord`, request hash, and receipt together, then wake the
   asynchronous reconciler. The operation record stores the exact `START`,
   `STOP`, or `RESTART` kind; a digest is never decoded to recover it. For
   `NO_OP`, persist only the request hash and receipt against the unchanged
   desired record; its operation ID is empty and no operation record or
   reconciler wakeup is required.

The idempotency ledger survives process restart and remains at least as long as
the desired-state schema's rollback window. Expiry cannot occur while the
associated lifecycle operation or migration rollback remains reachable.

Operation semantics are:

- `start`: change desired state to `RUNNING`. If it is already `RUNNING`, return
  `NO_OP` without resetting backoff, replacing a degraded generation, or bumping
  revision. Recovery that replaces a live/degraded generation uses `restart`.
- `stop`: change desired state to `STOPPED`. If already `STOPPED`, return
  `NO_OP`. A no-op does not assert that an unconfirmed native close succeeded;
  the observed view retains failure/quarantine.
- `restart`: require desired state `RUNNING`, increment its revision, record a
  new operation that makes the reconciler fence the current generation and
  construct a replacement. A stopped driver returns `FAILED_PRECONDITION`.

For accepted changes, the driver revision and catalog revision increment once.
An idempotent replay and a no-op do not increment either revision. The
reconciler reads the lowest nonterminal lifecycle sequence for a driver and
executes the stored kind and desired revision; it never infers an action from
the final desired record or opaque digest. It does not coalesce accepted
records, including `RESTART`, and it cannot begin the next sequence until the
current record reaches `COMPLETED` with a terminal reason.

The transition from `PENDING` to `ACTIVE` is durable. Successful or terminally
failed reconciliation commits the operation's completion, terminal reason, and
observed transition in one transaction. On process restart, any nonterminal
record is requeued in sequence. An `ACTIVE` record belongs to the old source
epoch and resumes as pending work under the new epoch; old callbacks and
observations remain fenced. Replaying a `RESTART` with no surviving native
generation starts exactly one new generation under the new epoch and completes
that restart record; it is never lost merely because the final desired state is
already `RUNNING`.

At process start the HTTP/API/health shell becomes available before drivers are
reconciled. The store loads every desired record independently. Corrupt or
unmigratable state for one driver produces that driver's `FAILED` observation;
it does not prevent other drivers or the control service from starting.

Every configured source instance creates a new `SourceEpochID` on process start.
No admission, callback, observation, capability, route, or close proof from an
earlier source epoch is current. The reconciler starts or leaves stopped each
driver from the ordered nonterminal operation journal first, then reconciles
the latest persistent desired state when the journal is empty, and records new
observations. Persisted observed state is historical diagnostic evidence, never
startup authority.

Creating a source epoch starts a manager-owned epoch-transition barrier before
`Prepare` or `Activate`, even for a driver whose desired state is `STOPPED`.
The manager first closes gateway admission for every older epoch of that exact
`SourceID`, creates generation 1, and persists one
`SourceEpochTransitionRecord`. Its reservation is
`CONTROL_OWNED/EPOCH_BARRIER` with `NextPublicationSequence=1`; generation 0 is
never used. The record's `Assets` is a bytewise `AssetID`-sorted materialization
of every asset in the gateway's durable source-publication index, including
assets carried forward from an incomplete prior transition or semantic-cleanup
journal. Before forwarding any manager or adapter batch, the manager persists
its canonical bytes and exact asset/source ownership as pending publication
work; an accepted result updates the index with `SnapshotID`, semantic revision,
epoch, generation, sequence, batch ID, and digest. Crash recovery therefore
cannot omit an accepted, possibly submitted, or cleanup-incomplete asset. The
index is never rebuilt from a live provider, current discovery result, or
compatibility alias.

For each recorded asset, the manager reads the exact immutable current
`Snapshot`, verifies that its `SnapshotID` and semantic revision match the
materialized input, and enumerates every non-retired `SourceDescriptor` for the
owned `SourceID`. It then submits one control-only `PublicationBatch` with the
new epoch and reserved generation, the next allocated sequence from the one
kernel `PublicationCursor` for
`(SourceID, SourceEpochID, DriverGeneration)`, the exact expected semantic
revision, a current inert `SourceDescriptor` in
`SourceUpserts`, and the complete old-epoch set in `SourceRetirements`. Binding,
identity, fact, service, capability, and generation-fence upsert arrays are
empty. The descriptor uses the persisted configured profile/version and
registry evidence; it asserts source ownership only. It does not assert a
provider observation, binding, activation, capability, or observed `RUNNING`.
Only the gateway manager may construct this batch for its owned source.

Only one asset batch is in flight. The first uses the reservation's sequence 1;
after acceptance, the manager durably records its result and advances
`Reservation.NextPublicationSequence` before assigning the next consecutive
sequence to the next bytewise-sorted asset. A rejected revision-conflict batch
did not advance the kernel cursor, so its rebuilt batch retains that unaccepted
next sequence. The manager never preassigns one sequence to multiple assets,
never uses generation 0, and never submits a later asset before persisting the
prior accepted result. Later asset entries keep internal `Sequence=0` until
assigned; zero is never sent on the semantic wire. An empty affected-asset set
completes the barrier with the reservation cursor still at 1.

Each asset batch is one kernel-atomic transition. It retires old source and
binding records, retains their irreversible tombstones and withdrawn
identity/service/capability history, and removes their observed candidates,
conflict references, and derived closure from the new current snapshot. Assets
commit separately because the kernel supplies no cross-asset transaction.

The external visibility fence is exact to the transitioning source, not to the
whole asset or immutable snapshot. For each asset awaiting retirement it covers
the recorded older source epochs; their bindings and identity links; their
services and capabilities; every observed candidate whose `SourcePathRef`
matches one of those paths; and every inferred candidate whose direct or
transitive `DerivationInput` includes a covered path. A projection item,
selection, precondition, readback, or aggregate derived value is covered only
when its exact lineage or chosen record intersects that set. The manager derives
the set from the complete immutable `Snapshot` named by `InputSnapshot` and
recomputes it after every revision-conflict refresh. It never matches a record
whose complete native and derivation paths belong only to another source.

Gateway admission is closed for every route owned by the transitioning source.
Scoped read, evaluation, presentation selection, projection, and route admission
still consume the complete matching `Snapshot` and `EvaluationView`. A result
whose exact selected record or dependencies are covered is returned as
unavailable/`withheld`; the gateway does not remove candidates, synthesize a
filtered snapshot, rerun a policy, or silently select an uncovered alternative.
An uncovered result from source B remains visible with its original candidate,
revision, provenance, and freshness even when source A shares the asset or fact
envelope. An aggregate public read or projection returns unaffected B-only
items with their ordinary dispositions and marks only covered or
mixed-dependency items `withheld`; a scoped B-only read returns normally, while
a scoped covered read is unavailable. The complete internal
`Snapshot`/`EvaluationView` remains unchanged and is never rewritten as a
partial public snapshot. Once an asset's retirement batch is accepted, its new
complete snapshot supplies the ordinary kernel view and the temporary source
fence for that asset is removed.

A revision conflict or changed old-epoch set rejects only that asset batch. The
manager refreshes that asset's exact snapshot and revision, rebuilds the complete
retirement set, and persists the new input snapshot, expected revision, prior
epochs, batch ID, and digest before retrying the same not-yet-accepted sequence.
Each rejection increments `Attempt` and retries only within the persisted limit
and deadline. The manager never skips the asset, invents a successful
observation, or claims cross-asset atomicity. An exhausted budget sets the
transition to `BLOCKED` and the driver observation to `FAILED`, leaves that
source's admission and affected-path visibility closed, and retains the
remaining work for an explicit lifecycle retry. Uncovered paths from other
sources remain public. Recovery while the same process/source epoch remains
active loads the same record before allocating provider work. Completed assets
are not replayed.

For the sole possibly submitted-but-unrecorded batch, it reads the exact kernel
`PublicationCursor`: an exact last-sequence/digest match permits idempotent
replay and accepted-snapshot readback, an unchanged cursor permits resubmission,
and any incompatible cursor blocks the transition as `sequence_conflict`.

After the last asset accepts, the manager atomically changes the same durable
reservation to `AVAILABLE/NONE`; it does not allocate another generation. The
reservation retains the exact next cursor, whether that is 1 for an empty
barrier or `PublicationCursor.LastSequence+1` for a nonempty barrier. A driver
left desired `STOPPED` therefore retains one inert available reservation. A
later accepted lifecycle operation claims that exact key and cursor by changing
it to `PROVIDER_CLAIMED/LIFECYCLE` and storing the operation ID in the same
transaction. A repeated reconciler wake may only recover that claim for the
same operation; another operation cannot claim it, increment its generation, or
reset its cursor.

A gateway process restart allocates a new source epoch as required above. Its
new transition supersedes, rather than resumes, an incomplete prior-epoch
record. The new record takes the union of that record's assets, all pending
publication assets, every incomplete semantic-cleanup asset, and the durable
index; rematerializes every current snapshot and revision; and starts its new
epoch/generation-1 reservation at sequence 1. This includes assets already
transitioned to the prior inert epoch, which the new batch now retires. The
prior reservation becomes `RETIRED` and can never be claimed in the new epoch.
After the last asset accepts, the manager exposes the new reservation as
`AVAILABLE` at `PublicationCursor.LastSequence+1` and passes that exact cursor
when it is claimed for activation. `Prepare`/`Activate` failure does not undo a
completed retirement barrier or abandon incomplete retirement work.

## 6. Provider SPI

The public SPI adapts the existing eBUS `Runtime`/`Admit`, eeBUS runtime slot,
and Modbus adapter boundaries. Protocol-specific types remain below the SPI.

```go
type ProviderDescriptor struct {
    DriverID       DriverID
    ProviderKind   string
    Contract       ContractID
    NativeContract NativeContractRef
}

type StartContext struct {
    Correlation             LifecycleCorrelation
    Deadline                time.Time
    NextPublicationSequence uint64
    Publish                 ActivationPublisher
    Report                  HealthReporter
}

type ActivationProof struct {
    Correlation LifecycleCorrelation
    Binding     semv1.NativeBinding
    ActivatedAt time.Time
    Evidence    []semv1.EvidenceRef
}

type FenceProof struct {
    Generation        GenerationKey
    Withdrawal        semv1.GenerationFence
    AdmissionClosedAt time.Time
    InFlightAtFence   uint64
}

type CloseStatus string
const (
    CloseProven      CloseStatus = "PROVEN"
    CloseUnconfirmed CloseStatus = "UNCONFIRMED"
)

type CloseProof struct {
    Generation  GenerationKey
    Status      CloseStatus
    DrainedAt   *time.Time
    ClosedAt    *time.Time
    Evidence    []semv1.EvidenceRef
}

type ProviderFactory interface {
    Descriptor() ProviderDescriptor
    Prepare(context.Context, StartContext) (PreparedGeneration, error)
}

type ReplacementFactory interface {
    PrepareReplacement(context.Context, StartContext, Generation) (PreparedGeneration, error)
}

type PreparedGeneration interface {
    Key() GenerationKey
    Activate(context.Context) (Generation, ActivationProof, error)
    Abort(context.Context) (CloseProof, error)
}

type Generation interface {
    Admit(context.Context, AdmissionRequest) (ExecutionLease, error)
    FenceWithdrawal(GenerationKey) FenceProof
    Stop(context.Context, FenceProof) (CloseProof, error)
}

type NativeOperationBody interface {
    NativeOperationContract() NativeContractRef
    Validate() error
}

type AdmissionRequest struct {
    Generation         GenerationKey
    CapabilityInstance semv1.CapabilityInstanceID
    NativeBinding      semv1.NativeBindingID
    NativeOperation    NativeContractRef
    Deadline           time.Time
}

type NativeExecutionEvidence struct {
    Dispatch        operationv1.DispatchEvidence
    Acknowledgement *operationv1.Acknowledgement
    Readback        *operationv1.Readback
}

type ExecutionLease interface {
    Generation() GenerationKey
    Execute(context.Context, NativeOperationBody) (NativeExecutionEvidence, error)
    Release() error
}

type PublicationEnvelope struct {
    Generation    GenerationKey
    InputSnapshot semv1.SnapshotID
    Sequence      uint64
    Digest        semv1.Digest
    Batch         semv1.PublicationBatch
}

const (
    MaxActivationPublicationBatches = 128
    MaxActivationPublicationBytes   = 8 * 1024 * 1024
)

type ActivationPromotionState string
const (
    ActivationStaging    ActivationPromotionState = "STAGING"
    ActivationCommitting ActivationPromotionState = "COMMITTING"
    ActivationCleaning   ActivationPromotionState = "CLEANING"
    ActivationComplete   ActivationPromotionState = "COMPLETE"
    ActivationFailed     ActivationPromotionState = "FAILED"
    ActivationBlocked    ActivationPromotionState = "BLOCKED"
)

type NativeCloseState string
const (
    NativeClosePending     NativeCloseState = "PENDING"
    NativeCloseActive      NativeCloseState = "ACTIVE"
    NativeCloseProven      NativeCloseState = "PROVEN"
    NativeCloseUnconfirmed NativeCloseState = "UNCONFIRMED"
)

type NativeCloseWork struct {
    State       NativeCloseState
    DeadlineUTC time.Time
    Proof       *CloseProof
}

type SemanticCleanupState string
const (
    SemanticCleanupPending    SemanticCleanupState = "PENDING"
    SemanticCleanupActive     SemanticCleanupState = "ACTIVE"
    SemanticCleanupComplete   SemanticCleanupState = "COMPLETE"
    SemanticCleanupBlocked    SemanticCleanupState = "BLOCKED"
    SemanticCleanupSuperseded SemanticCleanupState = "SUPERSEDED"
)

type SemanticCleanupAttemptResult string
const (
    CleanupPrepared          SemanticCleanupAttemptResult = "PREPARED"
    CleanupSubmittedUnknown  SemanticCleanupAttemptResult = "SUBMITTED_UNKNOWN"
    CleanupRejectedUnapplied SemanticCleanupAttemptResult = "REJECTED_UNAPPLIED"
    CleanupAccepted          SemanticCleanupAttemptResult = "ACCEPTED"
)

type SemanticCleanupAttempt struct {
    AttemptNumber            uint32
    InputSnapshot            semv1.SnapshotID
    ExpectedSemanticRevision uint64
    Sequence                 uint64
    BatchID                  semv1.BatchID
    Digest                   semv1.Digest
    CanonicalEnvelope        []byte
    CanonicalBytes           uint64
    Result                   SemanticCleanupAttemptResult
    AcceptedSnapshot         *semv1.SnapshotID
    AcceptedSemanticRevision *uint64
    Error                    *semv1.ErrorID
}

type SemanticCleanupAsset struct {
    AssetID  semv1.AssetID
    Attempts []SemanticCleanupAttempt
    Complete bool
}

type SemanticCleanupWork struct {
    FailedGeneration GenerationKey
    Reservation      GenerationReservation
    Fence             FenceProof
    Assets            []SemanticCleanupAsset
    State             SemanticCleanupState
    RetryCount        uint32
    RetryLimit        uint32
    DeadlineUTC       time.Time
    NextAttemptAtUTC  *time.Time
    LastError         *semv1.ErrorID
}

type ActivationBatchRecord struct {
    AssetID                  semv1.AssetID
    InputSnapshot            semv1.SnapshotID
    ExpectedSemanticRevision uint64
    Sequence                 uint64
    Digest                   semv1.Digest
    CanonicalEnvelope        []byte
    CanonicalBytes           uint64
    AcceptedSnapshot         *semv1.SnapshotID
}

type ActivationPromotionRecord struct {
    Generation           GenerationKey
    FirstSequence        uint64
    Batches              []ActivationBatchRecord
    TotalBytes           uint64
    DeadlineUTC          time.Time
    State                ActivationPromotionState
    SuccessorReservation *GenerationReservation
    SemanticCleanup      *SemanticCleanupWork
    NativeClose          NativeCloseWork
}

type PublicationSink interface {
    Publish(context.Context, PublicationEnvelope) error
}

type ActivationPublisher interface {
    Publish(context.Context, PublicationEnvelope) error
}

// Owned only by the manager; the provider receives Publisher(), not this control.
type ActivationPublicationController interface {
    Publisher() ActivationPublisher
    Promote(context.Context, GenerationKey, ActivationProof) error
    Abort(GenerationKey)
}

type HealthReporter interface {
    ReportDegraded(context.Context, LifecycleCorrelation, Reason, []semv1.CapabilityInstanceID) bool
    ReportFailure(context.Context, LifecycleCorrelation, Reason) bool
}
```

`ProviderKind` and `NativeContract` select a registered adapter. They do not
permit common code to inspect vendor payloads. `NativeOperationBody` must be one
concrete typed value owned by the selected native binding. A mismatched contract
or concrete type fails before dispatch.

`GenerationKey` is the gateway's live admission identity. It is not a semantic
withdrawal record. The accepted semantic `GenerationFence` is the irreversible
withdrawal record and supplies `SourceID`, `SourceEpochID`, driver generation,
reason, evidence, and revision. The accepted `PublicationBatch` repeats the
exact source/epoch/generation and carries its own batch ID, digest, sequence,
expected semantic revision, observed time, typed upserts/withdrawals, and
generation fences. The gateway must validate those fields instead of filling
them from ambient process state. All semantic wire `uint64` values use the
kernel's canonical decimal-string representation.

`LifecycleCorrelation` keeps the lifecycle operation ID distinct from its
generation key. Construction completion and every asynchronous health callback
must carry the correlation supplied by the manager. A callback for a superseded
operation is rejected even if its native code reports a plausible generation.

`GenerationReservation` is the single durable allocation and cursor authority
for a generation. `CONTROL_OWNED` permits only the named epoch barrier or
semantic cleanup to publish; `RECOVERY` owns no publication and only holds a
successor unavailable until native close is resolved. `AVAILABLE` is inert and
unclaimed.
`PROVIDER_CLAIMED` binds exactly one lifecycle operation to the existing key and
cursor before `Prepare`; recovery for that operation reuses the claim and no
other operation may claim it. Immediately before the first call to `Activate`,
the manager durably changes it to `CONSUMED`. Once consumed it cannot return to
`AVAILABLE`, even when activation fails, a wake repeats, or the component loses
memory. Cleanup then allocates the next generation as its own control-owned
reservation. A new source epoch retires every reservation from the prior epoch.

Every reservation has a non-zero generation and next sequence. Its key must
match the containing driver/source epoch. `AVAILABLE` and `RETIRED` require
owner `NONE`; `PROVIDER_CLAIMED` and provider-created `CONSUMED` require owner
`LIFECYCLE` plus exactly one operation ID; epoch and cleanup control records
require their matching owner, and cleanup/recovery records identify exactly one
failed generation. A state/owner/key mismatch is corrupt persistent state: it
closes admission and requires bounded reconciliation, never a guessed counter or
cursor.

A `Prepare` failure may release `PROVIDER_CLAIMED` back to `AVAILABLE` at the
unchanged cursor only when the native owner proves that no prepared/native
resource remains, no publication was staged, and `Activate` was never entered;
the release and attempt result persist in one transaction. An uncertain result
or surviving prepared object retains the claim for the same lifecycle operation
until bounded recovery obtains an abort/close proof; it never allocates a
replacement key merely because the wake repeats.
When a currently consumed generation is replaced without an intervening cleanup
reservation, the lifecycle transaction allocates and claims one strictly newer
generation at sequence 1. Its first accepted provider batch must atomically fence
every older unfenced generation. Allocation is never inferred from observed
state or from a process-local counter.

`Prepare` allocates native state while the manager-owned admission record for
the reserved key is closed. It must not activate I/O, publish capabilities, or
expose a `Generation`. `PreparedGeneration.Activate` runs behind that closed
gate, performs the native owner's activation, and returns both the activated
`Generation` and its proof. The manager is the only caller allowed to open the
gate, and does so only after validating the proof. `Abort` closes a prepared or
activated-but-unpublished result on an independent bounded cleanup context.
`Prepare` has only two clean return forms: `(prepared, nil)` transfers one
abortable prepared object, while `(nil, error)` proves that the owner retained no
native resource, staged publication, or activation side effect. An owner that
cannot prove the latter returns its prepared object with the error so the
manager retains the claim and calls `Abort`; it never hides partial native state
behind a nil object.
Adapters may implement this split as a thin wrapper: for example, an existing
native `Start` that both constructs and activates runs inside `Activate`, while
the gateway admission record remains closed.

`StartContext.Publish` is never the live semantic sink during preparation or
activation. The manager creates one `ActivationPublicationController` for the
reserved generation and passes only its publisher view. The preceding epoch
transition may already have consumed publication sequences, so
`NextPublicationSequence` is the exact next value persisted by the manager.

While `Activate` runs, the publisher stages an ordered set of zero through 128
canonical envelopes totaling at most 8 MiB, all before `StartContext.Deadline`.
The first sequence equals `NextPublicationSequence`; each later distinct batch
is exactly one greater, and every envelope names the reserved source, epoch, and
generation. Each batch also supplies its exact `InputSnapshot`, whose asset and
semantic revision match `Batch.AssetID` and
`Batch.ExpectedSemanticRevision`. An identical replay of any staged
sequence/digest is a no-op and does not count twice. Another digest at a staged
sequence, a regression, gap, other generation, count/byte overflow, or arrival
after the deadline poisons staging and fails activation closed.

At most one distinct staged batch may name an `AssetID`. This prevents a later
same-asset batch from guessing the semantic revision that the uncommitted first
batch would create. An adapter combines synchronous activation observations for
one asset into that initial atomic batch; subsequent same-asset deltas use the
live publisher after promotion. The manager persists an
empty `ActivationPromotionRecord{State:STAGING}` in the same transaction that
consumes the reservation and before it calls `Activate`. Each publisher call
appends its complete ordered canonical bytes, exact input snapshot/revision,
sequence/digest, total bytes, and original deadline before returning to the
provider. This bounded set is initial publication state, not a general retry
queue. Each `CanonicalEnvelope` is the complete immutable gateway-envelope
encoding, including generation, input snapshot, sequence, digest, and the full
`PublicationBatch` with its `BatchID`, upserts, withdrawals, fences, and
evidence. `CanonicalBytes` equals its exact length and the record's `TotalBytes`
is their sum; neither count is a substitute for the stored payload.

After activation proof validation and generation installation behind the closed
admission gate, the manager alone calls `Promote`. Promotion serializes with
publishers, freezes the set, and submits each batch to the live semantic sink in
global sequence order. Each batch is kernel-atomic; the set is not a cross-asset
transaction. Before each submission the manager verifies the complete current
snapshot ID and expected revision still match the staged input. It persists the
accepted snapshot and updated cursor before submitting the next batch. The
controller extends the source-scoped visibility fence to the reserved
generation before the first submission, and admission remains closed through
the whole set, so no accepted prefix becomes public or actionable. A callback
for the following sequence waits at this boundary and cannot overtake the
staged set.

After the final staged batch is accepted, the manager persists the next global
cursor, switches the same publisher object to live forwarding, releases waiting
callbacks, and only then removes the source visibility fence and opens
admission. If the set is empty, the first live publication must still use
`NextPublicationSequence`. Recovery in `COMMITTING` uses the same one-uncertain-
batch cursor/digest rule as the epoch barrier; accepted earlier batches are not
replayed. A waiting callback for an asset already changed by the staged set is
revalidated after the drain; a stale `InputSnapshot` or expected revision returns
`revision_conflict` without advancing the cursor, and the adapter must refresh
and rebuild at the same still-next sequence. It is never blindly forwarded with
the pre-promotion revision.

If promotion fails before any staged batch commits, the manager persists
promotion state `CLEANING`, `NativeCloseWork`, and one strictly newer
`SuccessorReservation` at sequence 1 in `CONTROL_OWNED/RECOVERY`, then calls
`Abort` on its independent bounded close context. The failed consumed generation
is never reused. If close is proven, the manager atomically records
`ActivationFailed` and makes the successor `AVAILABLE/NONE`; its first provider
batch must fence the failed generation even though that generation published no
provider records. If close is unconfirmed, it records `ActivationBlocked`, keeps
the successor held, and quarantines the source. The staged set is discarded
because it created no provider semantic state; the existing inert source
descriptor and immutable history remain. If a
prefix commits, the controller permanently rejects generation callbacks. Under
the same callback/admission boundary it invokes the installed generation's
`FenceWithdrawal` and atomically persists promotion state `CLEANING`, one
`SemanticCleanupWork`, and `NativeCloseWork{State:PENDING}`. The cleanup record
contains the exact `FenceProof`, failed generation, a
`CONTROL_OWNED/SEMANTIC_CLEANUP` reservation for `generation+1` at sequence 1,
an independent finite deadline and retry budget, and its complete bytewise
`AssetID`-ordered work set. Semantic cleanup and native close are two durable
obligations; neither completion is inferred from the other.

Recovery of a `CONSUMED/STAGING` record with no terminal activation result treats
native activation as possible and never calls `Activate` again. Under the same
record transaction it creates the recovery successor only when absent, closes
callbacks, and schedules owner-specific abort/close reconciliation for the
original lifecycle correlation. Repeated wakes and component recovery reuse that
same successor and close work. Only proven close makes the successor available;
uncertainty keeps it held and quarantines the source.

The cleanup asset set is materialized once from the durable union of
epoch-transition and accepted activation-prefix assets whose accepted snapshots
contain the failed generation's cursor, binding, records, or direct/transitive
derived closure. It remains the complete work set even after some assets accept.
`Assets` contains unique bytewise-sorted IDs; each asset's attempts have
consecutive `AttemptNumber` values in creation order, and `Complete` is true only
when its final attempt is durably `ACCEPTED` with a snapshot and semantic
revision.
Staged or pending provider batches that never committed are discarded and never
fabricated as cleanup state. In bytewise asset order, manager-owned cleanup
batches use the cleanup reservation's one global cursor and include the accepted
kernel `GenerationFence` for the failed generation plus the required withdrawal
cascade. They contain no activated binding, observation, service, capability, or
fact upsert. Each asset cleanup is separately kernel-atomic, and the
source-scoped visibility fence keeps the failed prefix non-public throughout.

For each asset attempt the manager reads one complete immutable snapshot,
derives the exact withdrawal set, allocates only the reservation's current
sequence, and persists a `SemanticCleanupAttempt` in `PREPARED` before
submission. That write-ahead record contains the exact input snapshot and
semantic revision, sequence, batch ID, digest, complete canonical cleanup
envelope and byte length. Immediately before submission it persists
`SUBMITTED_UNKNOWN`. A synchronous kernel rejection that proves no mutation is
persisted as `REJECTED_UNAPPLIED`; an accepted result and accepted snapshot are
persisted with the resulting semantic revision before the reservation cursor
advances or the next asset begins. There is at most one uncertain cleanup
attempt globally.

Only a proven unaccepted `revision_conflict` may refresh the same asset. The
manager appends a new attempt with the refreshed snapshot/revision and rebuilt
canonical envelope at the same still-next sequence; it never overwrites the
earlier attempt or reuses a sequence after acceptance. Every rejected retry
charges the cleanup record's persisted `RetryCount`/`RetryLimit` and
`DeadlineUTC`; `NextAttemptAtUTC` and `LastError` are cleanup state, independent
of both the expired activation deadline and the native-close deadline.

Immediately after that durable split, the manager schedules `Stop` with the
stored fence proof outside the manager lock. It runs to its independent close
deadline even when the semantic sink conflicts, times out, or marks cleanup
`BLOCKED`. The manager persists `CloseProven` or `CloseUnconfirmed` separately;
an unconfirmed close sets the parent promotion `BLOCKED`, quarantines the source
epoch, and prohibits replacement. A proven close does not release the semantic
visibility fence or erase unfinished cleanup.

Component recovery loads both obligations without resetting either budget. For
the sole `SUBMITTED_UNKNOWN` cleanup attempt it reads the exact cleanup
generation's `PublicationCursor`. An exact sequence/digest match triggers
accepted-snapshot readback and records `ACCEPTED`; an unchanged predecessor
cursor resubmits the stored canonical envelope byte-for-byte; an incompatible
cursor blocks with `sequence_conflict`. A `PREPARED` attempt is submitted from
its stored bytes. Accepted earlier assets and proven rejected attempt bytes are
not replayed. Native `PENDING`/`ACTIVE` close work is rescheduled separately
within its original independent deadline.

After every cleanup asset accepts, the manager persists the cleanup state
`COMPLETE`, changes that same reservation to `AVAILABLE/NONE` at its exact next
cursor, and retains the generation inert. The next provider must claim this
reservation through the ordinary start procedure; it cannot allocate another
generation or reset to sequence 1. No publication returns to the fenced
generation. The parent promotion becomes `FAILED` only when semantic cleanup is
complete and native close is proven; until then it remains `CLEANING`, and an
available reservation cannot be claimed. Replacement is permitted only from the
durable `FAILED` state.

Exhausted cleanup and its parent promotion remain `BLOCKED` with every incomplete
asset and attempt durable, source admission and affected-path visibility closed,
and independent native close still completed or quarantined; unrelated sources
stay visible. A
new process epoch includes all incomplete cleanup assets in its epoch-transition
barrier. Only after each carried asset is retired in the new epoch may the old
cleanup record become `SUPERSEDED` and its reservation `RETIRED`; the old native
close proof or quarantine remains independently durable. No path fabricates a
cross-asset transaction or an observation.

`ReplacementFactory` is optional. If absent, `restart` fences and proves close
of the old generation before calling `Prepare` for the new generation. If close
is unconfirmed, replacement is prohibited and the driver is quarantined for the
current process epoch.

The gateway manager may retain its bounded lifecycle construction retry policy.
It does not retry a native operation or duplicate an adapter's reconnect loop.
An adapter may report a retry classification and owner-authorized earliest time;
the manager consumes it within one configured lifecycle budget. Modbus reconnect
and request retry remain inside the current Modbus owner and its one total
operation context.

## 7. Lifecycle ordering

### Start and activation

1. Complete or resume the current source epoch's control-owned transition
   barrier for every recorded affected asset. No provider method runs while the
   barrier is incomplete.
2. In one durable lifecycle transaction, claim the epoch barrier's or completed
   cleanup's `AVAILABLE` reservation with its existing generation and next
   cursor. Only when replacing a consumed live generation with no available
   cleanup reservation may this transaction allocate and claim a strictly newer
   generation at a new sequence-1 cursor. Install manager admission closed for
   the claimed key.
3. Create the activation publisher with the claim's exact next sequence, then
   call `Prepare` outside the manager state lock. It returns only a
   `PreparedGeneration`; no native I/O or capability may yet be public. A wake
   or component recovery for the same operation loads this claim instead of
   allocating another generation.
4. If the lifecycle operation and claim are still current, persist the
   reservation as `CONSUMED` together with its empty durable `STAGING` record,
   then call `PreparedGeneration.Activate` outside the lock while admission
   remains closed. It returns the activated `Generation` and `ActivationProof`.
5. Validate the proof's exact source epoch, driver generation, native binding,
   profile/version, and close owner. A mismatch fails closed and calls `Abort`;
   the consumed generation proceeds through fencing/cleanup and is not reused.
6. Install the activated generation with admission still closed, then promote
   its activation publisher. The bounded staged set commits in global sequence
   order before any waiting later publication can forward. The first provider
   publication of an ordinary higher generation includes every required older
   generation fence.
7. In one externally consistent transition, open admission and publish observed
   `RUNNING`; any qualified available capability instances from the complete
   staged set become externally visible at this same gateway serialization
   boundary.
8. Continue asynchronous health and publication callbacks through the same
   exact generation correlation and now-live publisher; no callback may bypass
   that object.

A capability cannot be externally available before native activation and live
admission both succeed. Static catalog entries may remain `candidate`,
`unsupported`, or `unknown`; they are not effective device capabilities.

### Withdrawal, stop, and restart

1. Under the same serialization boundary used by admission, close admission for
   the exact generation.
2. Atomically publish capability/service withdrawals and observed `STOPPING` (or
   `BACKOFF`/`FAILED` for a failure path).
3. Cancel generation-owned asynchronous work and wait for admitted leases to
   release within the operation deadline.
4. Ask the native owner to close its resources and return `CloseProof`.
5. Publish `STOPPED` only for `CloseProven`. For timeout or unconfirmed close,
   publish `FAILED` with `CLOSE_UNCONFIRMED` and quarantine the driver for the
   source epoch.
6. A restart may claim an available cleanup reservation, or atomically allocate
   and claim one strictly newer generation at cursor 1, only after close is
   proven. Its first accepted batch supplies every older unfenced generation
   fence. The sole exception is a native replacement seam that itself proves
   non-overlap, fencing, drain, and ownership transfer; it still uses the same
   durable reservation and cursor rules.

Withdrawal hooks are bounded, non-blocking, and must not re-enter the manager.
Drain and I/O occur outside a manager lock. A prepared or activated generation
whose start operation was superseded is never installed or admitted; the
manager calls `Abort`, or fences and stops an already activated result, on an
independent bounded cleanup deadline. It never becomes effective, and an
unconfirmed close quarantines the driver for the source epoch. Before
`Activate`, a proven abort with no staged publication releases the exact claimed
reservation at its unchanged cursor; an uncertain abort retains the claim and
quarantine. After the durable `CONSUMED` transition, supersession always uses a
new cleanup reservation and never releases the consumed generation for reuse.

## 8. Upward flow

The upward path is:

```text
native observation
  -> native-owner validation and qualification
  -> exact native binding and evidence references
  -> typed semantic facts/services/capabilities
  -> atomic publication batch
  -> revision-consistent semantic snapshot
  -> versioned target projection plus loss report
```

Every observation records its exact source epoch and driver generation, native
profile/version, native observation identity, available native/source/receipt
times, and raw evidence reference. Endpoint secrets and private payloads remain
behind the native owner's evidence access contract.

Qualification remains `candidate`, `qualified`, `unsupported`, or `unknown`.
Runtime availability remains `available`, `degraded`, or `withdrawn`. These axes
must not be collapsed. Only a qualified capability on the current admitted
generation may become available.

Publication rules:

- sequence starts at 1 for each generation and increases by exactly one;
- replay of the same `(generation key, sequence, digest)` is a no-op;
- reuse of `(generation key, sequence)` with another digest returns
  `sequence_conflict` and rejects that batch without semantic mutation; the
  gateway then closes admission and initiates a separate atomic fenced
  withdrawal/degrade transition;
- a sequence gap rejects later deltas until the adapter supplies a complete
  resynchronization batch;
- batches from another source epoch, withdrawn generation, or future generation
  are rejected without mutating semantic state;
- the manager-owned epoch-transition batches precede provider publication and
  carry the complete `SourceRetirements` set for all older non-retired epochs
  of the same source on every affected asset; generation counters are scoped to
  an epoch and never suppress this rule;
- the first accepted publication for a higher driver generation must contain a
  `GenerationFence` for every older unfenced generation; the same atomic
  snapshot withdraws old bindings, identities, facts, services, capabilities,
  and derived closure and invalidates guarded callbacks, while omission returns
  `generation_transition_incomplete` without state change;
- publication is atomic: facts, services, capabilities, withdrawals, conflicts,
  and the resulting revision vector become visible together; and
- a partial read updates only evidenced fields. Retained fields keep their own
  provenance and age under their own freshness rules.

An observed fact has one exact `SourcePathRef`. An inferred fact supplies
`DerivationInput` records that bind every input candidate revision and the
sorted union of all transitive source paths. The inferred candidate omits the
single-path binding, source-epoch, and driver-generation fields rather than
inventing a synthetic native binding. Before accepting a batch, semreg computes
the transitive inferred closure affected by an input withdrawal, source
retirement, generation fence, binding invalidation, or dependency revision
change. The affected derived candidates and dependents withdraw atomically in
the same snapshot and semantic revision change. The gateway cannot postpone
that invalidation or repair it through a later publication.

Candidate withdrawal, generation fencing, and derived cascade also apply the
kernel's deterministic atomic `FactEnvelope` conflict transition. After all
candidate and dependency changes, the kernel recomputes one canonical open
value conflict from the complete qualified/promoted candidate set or emits no
current conflict. The gateway cannot publish, resolve, or select this metadata,
retain a removed candidate reference, or fabricate resolution evidence.
Immutable prior snapshots preserve the earlier candidates and conflict as
history. `Selection` is a pure result and is never stored in `Snapshot` or
`FactEnvelope`; a result bound to an older snapshot/evaluation is simply not
valid for the new snapshot and must be recomputed.

The kernel distinguishes retained withdrawal/fence tombstones from actionable
current paths. Source retirement retains a `SourceDescriptor` with
`SourceState=retired` and a `NativeBinding` with `BindingState=retired`.
Generation fencing retains the current source descriptor and a binding with
`BindingState=fenced`. Withdrawn identity, service, and capability records
remain resolvable through those bindings, while observed candidates and derived
closure are removed. These records are historical and non-actionable: gateway
route validation requires a current source descriptor, current binding, and
unfenced exact generation and cannot use a tombstone or withdrawn record.

Passage of time does not publish or mutate semantic state. For reads and for
operation admission, the gateway calls the kernel's pure
`EvaluateSnapshot(snapshot, EvaluationContext)` with an explicit trusted wall
estimate and monotonic point. The resulting
`helianthus.semantic.evaluation/v1` `EvaluationView` has its own digest while
the source snapshot bytes, `SnapshotID`, revision vector, candidates, and
stored quality remain unchanged. Immediately before precondition checks and
route selection, admission evaluates the admitted immutable snapshot again.
Only `fresh` facts with effective availability `available` may satisfy a fact
precondition. `Precondition.Fact`, `CandidateID`, and `CandidateRevision` bind
one exact candidate, which must be qualified, promoted, validity `good`, and
absent from every open conflict. The exact fact pack's
`PackValidator.EvaluatePredicate` evaluates the typed `PredicateOp` and value;
the kernel never searches another same-key candidate or presentation selection.
Candidate, unpromoted, suspect/bad/unknown, stale, expired, conflicted,
degraded, unavailable, missing, or revision-changed evidence returns
`precondition_failed` before dispatch.

Identity conflict is public conflict. Similar addresses, values, model strings,
or topology positions cannot merge assets. Presentation selection calls the
pure `SelectPresentation` operation with the complete immutable `Snapshot`,
matching complete `EvaluationView`, one `FactKey`, and exact policy ID/version.
The kernel verifies view digest, snapshot ID, complete revision vector, context,
and every evaluated candidate ID/revision before invoking the one registered
`SelectionPolicy`. Its `Selection` repeats that binding plus the requested key
and chosen candidate revision. It is valid only for that snapshot/evaluation,
never removes alternatives or conflicts, and grants no operation authority or
route.

Projection consumes one revision-consistent snapshot and a versioned target
manifest. Every requested `(kind,item_id)` tuple receives exactly one disposition
with the identical tuple and one of the semantic contract's `exact`,
`transformed`, `withheld`, `unrepresentable`, `unsupported`, or `unknown`
outcomes, including structured loss where required. Equal item IDs under two
kinds are independent requests and require two dispositions. A projected or
reflected value is still an observation and cannot create a fresh command.

## 9. Downward flow and outcome truth

The downward path is:

```text
typed intent
  -> caller authority
  -> current qualified capability
  -> typed preconditions and deadline
  -> exactly one native binding, driver, endpoint, source epoch, and generation
  -> live admission
  -> one typed native operation
  -> dispatch evidence
  -> native acknowledgement
  -> readback/observation
  -> execution record and public state
```

Resolution returns exactly one semreg `Route`. Zero or more than one otherwise
eligible route returns `ambiguous_route`; zero may return the more specific
`capability_not_qualified` or `capability_unavailable` only when that cause is
unique. There is no priority-based silent selection. A cached route is a hint
only; admission rechecks current authority, capability, binding, epoch,
generation, snapshot and object revisions, freshly evaluated preconditions,
causal budget, and deadline.
The accepted route must name the capability instance, service instance, native
binding, source, source epoch, and driver generation. Gateway-only endpoint and
typed native-operation details remain behind that exact binding.

Every service, capability, operation, and expected-effect definition carries a
`DefinitionRef` with an exact `PackRef`. The kernel's prebuilt
`DefinitionIndex` establishes one owner by definition kind, ID, and version;
`CapabilityRequirement.Pack` and `Intent.Kind.Pack` dispatch directly to that
owner. Common gateway code never infers ownership from an ID prefix, probes
validators in registration order, or consults a private definition map. A
missing mapping or operation hook returns `definition_owner_missing`; a
duplicate or mismatched owner returns `definition_owner_conflict`, before route
admission.

Before dispatch, cancellation, deadline, stale revision, withdrawal, missing
native operation interface, failed authority, or failed precondition returns a
terminal rejection with no native I/O. After dispatch, the gateway never falls
back to another route and never turns a timeout into permission to repeat a
mutation.

An execution record uses the semantic contract's exact outcomes:

| State | Meaning |
|---|---|
| `rejected` | No route or dispatch was admitted; a stable admission error is explicit |
| `failed_no_contact` | Dispatch evidence proves `not_sent` and no possible side effect |
| `acknowledged_unverified` | Native accepted/provisional ACK is proven; confirming readback is absent |
| `applied` | Current-generation readback/observation satisfies the typed success predicate |
| `no_effect` | Current-generation evidence proves that the requested effect did not occur |
| `conflict` | Post-dispatch evidence contradicts the requested effect or another current result |
| `indeterminate` | Dispatch may have occurred and evidence cannot prove applied or no effect |

ACK is not readback. Readback is not permission. Public state changes only from
qualified observation/readback, not from requested intent or ACK. A later
current-generation readback may append a linked reconciliation record proving
`applied`, `no_effect`, or `conflict`; it does not rewrite the original immutable
execution record or create a second native execution.

A confirming semantic `Readback` is created only from a later retrievable
immutable snapshot whose semantic revision is greater than the admitted
revision. It names the exact `SnapshotID`, `RevisionVector`, observed
`CandidateID` and `CandidateRevision`, `NativeBindingID`, `SourceID`,
`SourceEpochID`, and driver generation. That candidate's complete path must
resolve in the named snapshot and equal the admitted `Route`. A missing or
revision-mismatched snapshot/candidate is `dangling_reference`; a retired epoch
is `stale_source_epoch`; a mismatched or fenced generation is
`stale_driver_generation`; and a different binding/source or inferred candidate
is `invalid_outcome`. None can prove `applied`.

Matching the admitted native path is necessary but insufficient.
`Intent.ExpectedEffect` carries the exact pack-owned rule, fact,
`PredicateOp`, and expected value. The operation owner's
`OperationPackValidator.ValidateIntent` derives those fields and requires an
exact match before admission. For outcome evaluation, its `EvaluateReadback`
receives the unchanged admitted intent and exact resolved observed candidate;
the serialized relation must equal the computed relation. An unrelated
same-route observation returns `invalid_outcome` and cannot support `applied`.

The confirming observed candidate must be a new post-dispatch observation, not
a pre-dispatch candidate retained unchanged in a later snapshot. Its native and
receipt timing must prove that the exact new candidate revision was observed
strictly after `DispatchEvidence.Completed`. `DispatchEvidence.Started` and its
optional `Completed` are complete `EvaluationContext` records. With matching
monotonic epochs, `FactCandidate.Times.ReceiptMonotonic` must be strictly greater
than completion; across epochs, the earliest plausible UTC receipt must be
greater than the latest plausible UTC completion. `Readback.Evaluation` is the
complete wall-plus-monotonic context passed to `EvaluateSnapshot` and must be at
or after receipt under the same comparison rules. An unchanged matching
candidate, absent completion, equality or overlapping uncertainty,
incomparable timing, or ineligible evaluation returns `invalid_outcome` and
cannot support `applied`. A value satisfied before dispatch requires a separate
explicit pack-owned preflight contract that skips native dispatch; v1 does not
record it as `applied`.

Native retry is allowed only when the exact native operation contract proves it
safe and keeps all attempts inside the original route, generation, authority,
idempotency identity, deadline, and retry quota. A mutation with indeterminate
dispatch cannot be retried automatically. Lifecycle construction retry remains
separate from operation retry.

## 10. Causal loop and echo suppression

Every intent and resulting observation/projection carries semreg
`CausalContext`. Each output binding preserves the `OriginRef`, correlation ID,
optional parent correlation ID, and hop count.

The accepted semantic context names `Origin`, `CorrelationID`, optional
`ParentCorrelationID`, `HopCount`, `MaxHops`, `FirstSeenAt`, `ExpiresAt`, and
the traversed target `Path`. `Path` records target processors that have accepted
ingress, in entry order. A context created inside target A starts as `[A]` with
hop count 1; an external context before its first target starts empty at zero.
Egress leaves both fields unchanged. On ingress, receiver `R` atomically
validates incoming count/path/expiry, rejects `echo_suppressed` if `R` is
already present, rejects `causal_budget_exceeded` if adding `R` would exceed
`MaxHops`, then appends `R` and increments `HopCount` before processing. A
rejection does not mutate the context or replace the original correlation.

The v1 bounds are:

- maximum hop count: 8;
- every operation definition supplies an `echo_window` greater than zero and no
  more than the semantic v1 causal limit of 300 seconds before it is eligible
  for output binding;
- correlation tombstones remain through the later of the operation deadline or
  dispatch time plus `echo_window`, plus the semantic contract's clock
  uncertainty; and
- a context older than its echo window or beyond 8 hops may still contribute a
  truthful observation, but it cannot produce a derived intent.

An output rejects an intent when its causal ancestry already includes that
output binding and operation definition inside the echo window, or when the
same correlation/operation was already admitted. Rejection is recorded as
`echo_suppressed` with no I/O.

A separate, explicitly authorized intent with a new intent ID and causal
correlation remains eligible even when its value equals a recent output. Value
equality alone is not loop detection. Idempotency suppresses duplicate requests;
causal suppression prevents cross-output loops. Neither grants authority.

## 11. Current operation inventory at the pinned baseline

This inventory describes code reachable from the production composition at the
pinned gateway baseline. It is not a 0.7 capability promise. Profile/catalog
code that is not connected to that composition stays unavailable.

| Runtime/profile | Current reachable operations | Current boundary and INT-06 disposition |
|---|---|---|
| eBUS `ebus.primary` | Discovery, raw evidence, read, semantic projection, and write are declared on the one managed runtime. Stable MCP provides runtime/registry/semantic reads, guarded schedules/config writes, and registry-routed `ebus.v1.rpc.invoke`; B503 adds five read-only evidence/session views. | Reuse current `DriverManager`, generation admission, selected-source intersection, and registry method mutability. A declared `WRITE` is not universal semantic write authority; the exact current registry method and source admission still decide. |
| eeBUS SHIP/SPINE runtime | Public redacted runtime, service, session, topology, pairing, and snapshot reads. Owner-only raw feature and mutation-record reads exist. Raw feature set and rollback exist only through the owner boundary, exact write authorization, configured mutation-lab profile, and a runtime implementing the optional mutation interface. | Adapt the eeBUS runtime slot and command router. Missing mutation interface is `unsupported`, never success. No generic semantic eeBUS write is inferred. Exact normative use-case mappings remain a separate docs/registry dependency. |
| Modbus TCP and qualified SunSpec/Fronius path | Bounded FC03/FC04 raw read, retained profile observation, qualified canonical PV read, and the existing qualify/refresh worker. | Read-only. Reuse adapter-owned scheduling, one owner-gated reconnect/retry, endpoint sanitization, and full wire/logical/physical/generation provenance. No FC06/FC16 or vendor-private write is permitted by the current gateway provider. |
| Tesla HSC provider in production composition | One disabled-by-default status snapshot with compatibility `unknown` and registry-derived `outbound_allowed` (currently false). | No serial acquisition or transmission. FC100/101/102 records and current-limit evidence types do not authorize a live route. |
| Huawei SmartLogger/EMMA/S-Dongle, Growatt Protocol II and BMS RS485, OutBack AXS, Fronius-specific status, Tesla FC100/WC/current-limit optional tools | Typed read/injected-provider contracts exist in the repository, but the production `gatewayModbusMCPProvider` does not implement their optional provider interfaces. | `unavailable` or `uncomposed` at this baseline. Later INT-07 composition must use exact qualified profile/version evidence. No write is inferred from a decoder, fixture, or provisional ACK/readback record. |
| Gree CAN and Growatt CAN | No production provider is composed by this gateway baseline. | `unavailable` here. Receive-only registry readiness remains native evidence, not a gateway operation or transmit grant. |
| Matter output | No production output binding is composed. | No read, write, or conformance claim. INT-13 owns the later versioned target mapping. |

The exact currently registered profile-facing operation names relevant to this
contract are:

- eBUS registry and generic route: `ebus.v1.registry.devices.list`,
  `ebus.v1.registry.devices.get`, `ebus.v1.registry.planes.list`,
  `ebus.v1.registry.methods.list`, and `ebus.v1.rpc.invoke`. Invoke permits
  `READ_ONLY` only for a known read-only registry method. `MUTATE` requires the
  current registry route, dangerous-operation acknowledgement, idempotency key,
  deadline, and admitted source.
- eBUS named semantic mutation surfaces:
  `ebus.v1.semantic.schedules.set_zone_time_program`,
  `ebus.v1.semantic.schedules.set_dhw_time_program`,
  `ebus.v1.semantic.system.set_config`, and
  `ebus.v1.semantic.boiler_status.set_config`. Their presence is not proof that
  every configured device supplies the corresponding writer or native method.
- eBUS standard catalog reads: `ebus.v1.ebus_standard.services.list`,
  `ebus.v1.ebus_standard.commands.list`,
  `ebus.v1.ebus_standard.command.get`, and
  `ebus.v1.ebus_standard.decode`.
- Vaillant B503 reads: `ebus.v1.vaillant.errors.get`,
  `ebus.v1.vaillant.errors.history.get`,
  `ebus.v1.vaillant.service.current.get`,
  `ebus.v1.vaillant.service.history.get`, and
  `ebus.v1.vaillant.live_monitor.get`. No B503 install-write tool is present.
- eeBUS public reads: `eebus.v1.runtime.status.get`,
  `eebus.v1.services.list`, `eebus.v1.services.get`,
  `eebus.v1.sessions.list`, `eebus.v1.sessions.get`,
  `eebus.v1.topology.get`, `eebus.v1.snapshot.capture`,
  `eebus.v1.snapshot.drop`, and `eebus.v1.pairing.status.get`.
- eeBUS owner-only raw operations: `eebus.v1.features.get`,
  `eebus.v1.features.data.get`, `eebus.v1.features.data.set`,
  `eebus.v1.mutations.get`, and `eebus.v1.mutations.rollback`. Set and rollback
  remain unavailable without the exact write authorization, mutation-lab
  profile, and optional native mutation interface.
- Modbus production composition: `modbus.v1.raw.read`,
  `modbus.v1.profile.observation.get`, `modbus.v1.semantic.pv.get`, and
  `modbus.v1.tesla.hsc.status.get`. Only FC03/FC04 raw reads are admitted; the
  Tesla status is an inert disabled-profile report with outbound disabled.

The gateway exposes no public driver lifecycle `list/get/start/stop/restart`
operation at this baseline. The control service in section 4 is the contract
for that later implementation.

Exact public source anchors for these statements:

- eBUS manager configuration and declared capabilities:
  [`cmd/gateway/ebus_driver_runtime.go#L18-L94`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L18-L94)
- eBUS source/write admission and lifecycle callbacks:
  [`cmd/gateway/ebus_driver_runtime.go#L241-L282`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L241-L282) and
  [`#L345-L573`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/ebus_driver_runtime.go#L345-L573)
- eBUS public tool inventory, invoke guard, and B503 read-only surface:
  [`mcp/server_tools.go#L1-L369`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/server_tools.go#L1-L369),
  [`mcp/server.go#L1932-L1989`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/server.go#L1932-L1989), and
  [`mcp/vaillant_b503.go#L46-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/vaillant_b503.go#L46-L152)
- current manager lifecycle, late admission retirement, generation callback
  fencing, and admission:
  [`internal/drivermanager/manager.go#L93-L189`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L93-L189),
  [`#L325-L658`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L325-L658), and
  [`#L764-L845`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/drivermanager/manager.go#L764-L845)
- eeBUS slot replacement/retirement and provider adapter:
  [`cmd/gateway/eebus_runtime_adapter.go#L26-L156`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/eebus_runtime_adapter.go#L26-L156)
- eeBUS public read inventory and owner-only raw command inventory:
  [`mcp/eebus_v1.go#L19-L108`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/eebus_v1.go#L19-L108) and
  [`mcp/eebus_v1_commands.go#L18-L69`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/eebus_v1_commands.go#L18-L69)
- eeBUS native authorization and optional mutation runtime:
  [`internal/eebuscommand/router.go#L50-L127`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/eebuscommand/router.go#L50-L127)
  and the closed mutation-lab profile loader
  [`cmd/gateway/eebus_mutation_lab_profile.go#L12-L84`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/eebus_mutation_lab_profile.go#L12-L84)
- Modbus public tool/provider surface and composition:
  [`mcp/modbus_v1.go#L15-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/mcp/modbus_v1.go#L15-L152),
  [`cmd/gateway/modbus_mcp_provider.go#L32-L159`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/modbus_mcp_provider.go#L32-L159), and
  [`cmd/gateway/gateway_http_server.go#L68-L85`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/gateway_http_server.go#L68-L85)
- Modbus adapter ownership and reconnect boundary:
  [`internal/modbusadapter/adapter.go#L19-L75`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/modbusadapter/adapter.go#L19-L75) and
  [`#L257-L299`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/internal/modbusadapter/adapter.go#L257-L299)
- independent driver startup and process availability behavior:
  [`cmd/gateway/gateway_run_lifecycle.go#L43-L140`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/e31106c9c726fbb8df7546901763e19b93659e72/cmd/gateway/gateway_run_lifecycle.go#L43-L140)

## 12. Compatibility and staged migration

1. Freeze exact current MCP schemas/goldens, GraphQL behavior, Portal views, HA
   identities, eBUS registry IDs, eeBUS pairing/trust state, PV snapshot
   accounting, and persistent runtime-state behavior at their owning revisions.
2. Add the runtime control service internally. Adapt only `ebus.primary` first,
   preserving current `DriverManager` behavior and stable public APIs.
3. Publish semreg facts/capabilities in shadow mode beside current eBUS and PV
   donors. Compare value, unit, dimensions, quality, freshness, provenance,
   identity, availability, and explicit loss over the same fixtures/replays.
4. Adapt eeBUS without weakening the runtime-slot drain/close boundary or pairing
   state. Adapt Modbus without adding a second scheduler/reconnect owner.
5. Connect each remaining native family only after its provider issue supplies an
   exact profile/version contract and qualification. An unavailable family stays
   visible as unavailable; it does not disappear or become a generic profile.
6. Expose lifecycle `list/get/start/stop/restart` through INT-08 public surfaces.
   Existing APIs remain unchanged until their own compatibility/parity acceptance
   passes. HA later consumes the GraphQL form under INT-15.
7. Activate one driver/consumer slice behind a reversible versioned selection.
   Persist the selected semantic contract and migration version. Rollback restores
   the previous compatible path and state schema, never a mixed semantic state.
8. Remove historical eBUS-owned universal semantic donors only after every known
   consumer and persisted identifier has migrated and the exact integrated BOM
   passes INT-17/release acceptance.

Compatibility aliases can locate a public semantic asset. They cannot populate
`DriverID`, `NativeBindingID`, endpoint, generation, or route. No existing
`ebus.v1.*`, `eebus.v1.*`, or `modbus.v1.*` contract is redefined by this
document.

## 13. Acceptance scenarios for implementation

Each scenario becomes an executable fixture or integration test in its later
owning issue. The expected result is normative.

| ID | Scenario | Expected result |
|---|---|---|
| CTL-01 | List with drivers in arbitrary registration order | Bytewise `DriverID` order and one catalog revision |
| CTL-02 | Get an unknown driver | `NOT_FOUND`; no factory call or state mutation |
| CTL-03 | Start with stale expected revision | `REVISION_CONFLICT` plus current view; no persistent/native change |
| CTL-04 | Replay identical start key and request after process restart | Original receipt; no revision bump or second native start |
| CTL-05 | Reuse an idempotency key with another request hash | `IDEMPOTENCY_CONFLICT`; no state change |
| CTL-06 | Start an already desired-running driver | `NO_OP`; no backoff reset, replacement, or revision bump |
| CTL-07 | Restart a desired-stopped driver | `FAILED_PRECONDITION`; no state change |
| CTL-08 | Accepted start, restart, and stop calls queue before the reconciler consumes its wake | Three typed records retain exact kinds and consecutive per-driver sequences; reconciliation executes them in order without coalescing |
| CTL-09 | Process exits after a restart record becomes `ACTIVE` but before terminal completion | The new source epoch requeues that exact nonterminal restart in order and starts one new generation; final `RUNNING` desired state does not erase it |
| CTL-10 | Idempotent replay of an accepted queued operation | Original receipt returns with the same non-nil operation sequence; no duplicate journal record, revision bump, or native action |
| CTL-11 | Lifecycle request resolves as `NO_OP` | Receipt has empty operation ID and nil sequence; replay preserves both and allocates no journal record |
| LIFE-01 | Prepare returns behind a closed gate, then activation succeeds with a matching proof | Only the post-proof manager transition installs the generation and exposes admission, `RUNNING`, and qualified capabilities together |
| LIFE-02 | One driver startup fails | That driver becomes `BACKOFF`/`FAILED`; API/health and unrelated drivers remain available |
| LIFE-03 | Stop drains and native close is proven | Capabilities withdraw before further admission; final state `STOPPED` |
| LIFE-04 | Stop deadline expires without close proof | `FAILED/CLOSE_UNCONFIRMED`, source-epoch quarantine, no replacement |
| LIFE-05 | Restart succeeds | Old generation is fenced, withdrawn, and closed before a strictly newer generation publishes |
| LIFE-06 | Gateway restarts with desired running and historical observed running | New source epoch reconciles a new generation; historical observation grants no admission |
| LIFE-07 | Canceled start returns a provider late | Provider is never published, is fenced and retired on an independent deadline; unproven close quarantines |
| LIFE-08 | Callback arrives from prior epoch/generation or superseded lifecycle operation | Rejected with no semantic or observed-state mutation |
| LIFE-09 | Prepare or activation completes after its lifecycle operation was superseded | Result is never installed or admitted; `Abort`, or fence plus stop, runs on an independent cleanup deadline |
| LIFE-10 | Activation returns a proof for another binding, epoch, or generation | Fail closed; no capability or `RUNNING` publication, and cleanup/quarantine follows close proof |
| LIFE-11 | Generation 8 publishes while generation 7 remains current but omits generation 7's `GenerationFence` | `generation_transition_incomplete`; reject the complete batch with no state change, and generation 8 does not become current or actionable |
| LIFE-12 | Generation 8 atomically supersedes generation 7 | The same new snapshot fences and withdraws generation 7, invalidates its callback, removes its derived closure, and exposes only qualified generation-8 capabilities |
| LIFE-13 | A post-supersession snapshot retains generation-7 withdrawal/fence tombstones | Full snapshot validation succeeds under the explicit historical-reference rule, while route admission and callbacks still reject generation 7 |
| LIFE-14 | Native activation stages a valid bounded contiguous set and a following callback races promotion | Every staged batch commits in order behind closed source visibility/admission; the callback waits and forwards only at the next persisted sequence after the complete set |
| LIFE-15 | Activation staging emits another generation, a regression/gap/conflict, a second distinct batch for one asset, exceeds 128 batches/8 MiB, or passes its deadline | Staging is poisoned and activation fails closed; no provider observation, capability, admission, or synthetic same-asset revision becomes public |
| LIFE-16 | A new source epoch starts at generation 1 while an older non-retired epoch ended at generation 8 | Before provider preparation, the manager materializes every affected asset and its exact snapshot/revision, then its per-asset control batch names every older epoch in `SourceRetirements`; each accepted snapshot retires/withdraws old state without inventing provider evidence |
| LIFE-17 | One epoch-transition asset batch omits an older non-retired epoch or loses its expected-revision race | Reject that asset batch; the transitioning source's affected paths and admission remain closed while the manager refreshes and rebuilds it within the bounded durable retry record |
| LIFE-18 | `Prepare` or `Activate` fails before any provider publication in a new process epoch | The earlier manager barrier has already removed every old current candidate and derived path and retained retired/withdrawn tombstones plus immutable history; the inert new descriptor exposes no binding, capability, observation, or route, and admission stays closed |
| LIFE-19 | The process exits after some, but not all, per-asset epoch transitions are accepted | Restart creates a new epoch/record, unions incomplete and indexed assets, rematerializes their current snapshots, and starts a new cursor at sequence 1; this also retires the prior inert epoch on already transitioned assets before provider work, with no cross-asset atomicity or old-snapshot fallback claim |
| LIFE-20 | Two assets complete the new-epoch generation-1 barrier at sequences 1 and 2; activation discovers two assets and stages sequences 3 and 4 while a callback publishes sequence 5 | The lifecycle operation claims that exact available generation-1 reservation and cursor 3, accepts both unique-asset batches within bounds, commits 3 then 4, persists cursor 4, and releases the callback at 5 with no collision, gap, second generation, cross-asset atomicity, or generation-0 convention |
| LIFE-21 | Activation batch 1 of 2 commits, batch 2 loses its exact input revision, and a callback is waiting | The prefix remains source-fenced and non-admissible; cleanup generation `g+1` fences `g` in global sequence order across the complete generation-owned asset set, while native close runs independently, before a later provider can use inert `g+1` at its persisted next sequence |
| LIFE-22 | Sources A and B share an asset; A's epoch retirement repeatedly conflicts and exhausts its budget | Old-A direct/transitive paths, selections, items, and routes remain unavailable/withheld, while scoped B reads/routes and B-only aggregate projection items remain visible with their unchanged snapshot-bound provenance; mixed items are withheld and no B fallback is silently selected |
| LIFE-23 | The first staged activation batch commits, then component memory is lost before the second is submitted | Recovery loads the persisted complete `CanonicalEnvelope` suffix, confirms the exact accepted cursor/digest, and submits the original second envelope byte-for-byte at the same next sequence; it never regenerates provider evidence from a digest, count, or reread |
| LIFE-24 | A committed activation prefix enters cleanup, but every semantic cleanup attempt conflicts until its budget expires | Callback/admission fencing first persists separate semantic and native-close obligations; `Stop`/`Abort` still runs within its independent deadline and records proven close or quarantine, unfinished semantic work remains durable and source-withheld, component recovery preserves both, and unrelated sources remain visible |
| LIFE-25 | Process start leaves the driver desired `STOPPED`; two epoch-barrier assets accept at generation 1 sequences 1 and 2; a later `START` succeeds, then a later real replacement succeeds | `START` claims the still-inert generation-1 reservation and first provider publication uses sequence 3. The replacement allocates generation 2 with cursor 1, and its first accepted batch atomically fences generation 1; neither path resets or crosses a generation cursor |
| LIFE-26 | An empty or nonempty epoch barrier completes, then `Prepare` fails or its reconciler wake/component loses memory before `Activate` | Every retry for that lifecycle record uses the same provider claim, generation, cursor, and correlation. Only proof that no prepared/native state or staged publication remains may release the claim unchanged; uncertainty retains the claim or quarantines it, and no wake allocates another generation or consumes the cursor |
| LIFE-27 | Cleanup loses memory (a) after persisting an attempt but before submission, (b) after recording one accepted asset, or (c) after submission before recording its result | Recovery loads the original ordered asset set and unchanged budget. It submits stored bytes for (a), skips the durable accepted prefix for (b), and for (c) uses the cleanup cursor/digest to record accepted readback or replay the exact envelope. It never loses an asset, changes bytes at an accepted sequence, or reuses the failed generation |
| LIFE-28 | Cleanup exhausts with native close already proven, then the gateway process restarts | The new epoch barrier carries every incomplete cleanup asset into its own generation-1 retirement work while the old cleanup journal remains durable; only accepted retirement of every carried asset supersedes and retires the old cleanup reservation. Close proof remains independent, source-owned paths stay withheld, and healthy source-B paths remain visible |
| LIFE-29 | `Prepare` succeeds, the reservation is durably consumed, then `Activate` fails before its first provider batch commits and recovery repeats | The failed generation is never released or reused. The manager durably holds one generation+1 successor at cursor 1 while native close is unresolved; proven close makes it available, and a subsequent start claims it once and atomically fences the failed generation in its first accepted batch without inventing an observation |
| UP-01 | Partial observation fails for one field | Only evidenced fields update; other candidates retain provenance and age normally |
| UP-02 | Same publication sequence and digest repeats | No-op with no semantic revision bump |
| UP-03 | Same sequence repeats with another digest | Reject that batch with `sequence_conflict` and no semantic mutation; then close admission and publish a separate atomic fenced withdrawal/degrade transition |
| UP-04 | Publication sequence has a gap | Reject deltas until complete resynchronization |
| UP-05 | Similar native identities compete for one asset | Public identity conflict; no silent merge or route |
| UP-06 | Catalog knows an operation but device evidence does not qualify it | Candidate/unknown/unsupported as justified; no available capability |
| UP-07 | An immutable snapshot is evaluated before, at, and after one fact's fresh/stale/expired thresholds without publication | Evaluation views change deterministically while snapshot bytes, ID, revisions, candidates, and stored quality remain unchanged |
| UP-08 | An inferred fact depends on two exact source paths and one source is fenced | The inferred fact and its transitive dependents withdraw in the same accepted snapshot/revision change; no synthetic binding or later cleanup |
| UP-09 | A presentation-selected candidate in an open value conflict is withdrawn or fenced while another candidate remains | The new snapshot deterministically recomputes or removes current conflict metadata; the old immutable snapshot retains history, and its pure `Selection` remains bound only to that old snapshot/evaluation |
| UP-10 | `SelectPresentation` receives a complete matching snapshot/view/key and exact policy, then repeats with one binding changed | Matching inputs return one deterministic fully bound presentation-only `Selection`; digest mismatch, snapshot/revision/context/candidate mismatch, or missing key/candidate returns the kernel's exact stable error without mutation |
| PROJ-01 | One manifest requests the same item ID as both capability and operation | Two independent `(kind,item_id)` dispositions are required; missing, duplicate, or mismatched tuples return `projection_incomplete` |
| DOWN-01 | Exactly one current route passes authority, capability, preconditions, and deadline | One live lease and one native dispatch path |
| DOWN-02 | Zero or two current routes match after otherwise successful filtering | `ambiguous_route`, or one uniquely applicable qualification/availability error for zero routes; no I/O |
| DOWN-03 | Route was resolved before generation withdrawal | Admission recheck rejects the stale generation key; no I/O |
| DOWN-04 | Native optional mutation interface is absent | Capability cannot qualify; reject with `capability_not_qualified` and no synthesized success |
| DOWN-05 | Deadline expires before route admission or native dispatch | `rejected` with exact `deadline_expired`; no route, dispatch, acknowledgement, readback, or native I/O |
| DOWN-05A | Route and execution lease were admitted, but the native dispatch attempt proves `not_sent` and `possible_side_effect=false` | `failed_no_contact` with the dispatch and outcome evidence; no acknowledgement or readback is invented |
| DOWN-06 | Deadline expires after dispatch with no terminal proof | `indeterminate`; no fallback or automatic mutation retry |
| DOWN-07 | Native ACK arrives without matching readback | `acknowledged_unverified`; projected state unchanged |
| DOWN-08 | A later retrievable snapshot contains a newly revised observed candidate whose native path matches the admitted route and whose receipt is strictly after completed sent dispatch | A reproducibly eligible pack-confirmed readback may prove `applied`; facts update from readback evidence and the original execution stays immutable |
| DOWN-09 | Native NACK plus current evidence proves no effect | `no_effect`; no success projection |
| DOWN-10 | Readback names a missing snapshot/candidate, stale epoch/generation, different binding/source, or inferred candidate | Exact semantic `dangling_reference`, `stale_source_epoch`, `stale_driver_generation`, or `invalid_outcome`; never `applied` |
| DOWN-11 | A fact was fresh in the admitted snapshot but is stale at the immediate pre-dispatch evaluation context | `precondition_failed`; no route admission or native I/O, and the immutable snapshot is unchanged |
| DOWN-12 | `Precondition.CandidateID` names a fresh/available unqualified candidate, or a different same-key candidate is eligible | `precondition_failed`; no fallback candidate, presentation selection, or native I/O |
| DOWN-13 | A later observed candidate matches the admitted route but not `Intent.ExpectedEffect.Fact` or its pack-evaluated predicate | `OperationPackValidator.EvaluateReadback` returns `invalid_outcome`; `applied` is forbidden |
| DOWN-14 | Multiple packs are registered and a definition has missing or duplicate `DefinitionIndex` ownership | `definition_owner_missing` or `definition_owner_conflict`; no prefix inference, registration-order probing, route, or I/O |
| DOWN-15 | A later unrelated snapshot retains the same matching candidate and revision that existed before dispatch | `invalid_outcome`; no newly revised post-completion observation proves the effect |
| DOWN-16 | `Readback.Evaluation` is absent/invalid or its observation cannot be ordered strictly after `DispatchEvidence.Completed` under monotonic or uncertainty-aware UTC rules | `invalid_outcome`; no implicit process time or stored publication-time freshness is used |
| ERR-01 | One semantic record violates several validation classes | Return the first of the pinned contract's exhaustive 38 IDs by its eight-level precedence; no state mutation or gateway-specific remapping |
| ERR-02 | Empty `IdentityLink.basis`, empty capability activation evidence, or a decoded missing/revision-changed exact precondition candidate reaches semantic validation | Preserve `identity_not_qualified`, `capability_not_qualified`, or `precondition_failed`, respectively; malformed non-empty evidence remains `invalid_evidence` |
| ERR-03 | A causal context exceeds a hard collection/count/lifetime bound or overlaps that defect with receiver re-entry | Preserve the accepted causal-domain error partition and ingress precedence; generic bounds/time classification cannot override it |
| LOOP-01 | Matter/eeBUS output returns through another output with the same ancestry | `echo_suppressed`; no I/O |
| LOOP-02 | Hop count exceeds 8 or echo context expires | Observation may publish; derived intent is rejected |
| LOOP-03 | Independent authorized intent requests the same value | It remains eligible with its own correlation, subject to ordinary checks |
| LOOP-04 | A emits `[A]/1` to B, then B emits onward to C | B ingress accepts `[A,B]/2`; B egress is unchanged; C ingress accepts `[A,B,C]/3` |
| LOOP-05 | C attempts to reflect `[A,B,C]/3` back to A | A sees itself in the incoming path and returns `echo_suppressed` before mutating the context |
| LOOP-06 | Receiver re-entry and exhausted hop capacity overlap | Ingress order returns `echo_suppressed` before `causal_budget_exceeded`; context remains unchanged |
| MIG-01 | Old and semreg paths run on the same fixture | Comparator covers value, exact unit, dimensions, quality, time, provenance, identity, availability, and loss |
| MIG-02 | Gateway rolls back within the supported state window | Previous compatible APIs and persisted state load without mixed contract versions |

## 14. Evidence and completion boundary

Acceptance of this document proves only an implementable gateway-owned contract
against the pinned source baseline and accepted semantic dependency. It does not
prove that the lifecycle service, SPI, semantic publications, output bindings,
vendor profiles, conformance, Portal, HA, deployment, or physical hardware work.

Later implementation must add executable tests for every applicable scenario,
current-source comparators, profile-specific mapping acceptance, public API
parity, and full exact-BOM integration. Live writes, installations, credentials,
and physical control remain action-time operator decisions.
