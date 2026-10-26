# Runtime driver/provider and north-south contract v1

Status: proposed software 0.7 contract. This document specifies a subsequent
implementation; the current binary does not expose the lifecycle service or a
universal manager for every protocol family.

Implementation status (issue
[#1024](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1024)):
the gateway-internal semantic publication hub (`internal/semhub`) implements
the publication side of sections 4 to 8 for in-process publishers: stable
asset and source identities with per-incarnation binding, service and
capability identities; source epochs admitted before a provider opens, with
distinct epoch and driver-generation identities; one transactional
publication path over the SemReg publication kernel with the kernel's own
replay and error identifiers; ordered, bounded event fan-out; and the
generation fence, epoch retirement, activation and reprojection rules of the
withdrawal sequence. The hub is runtime composition, not a public API, and
persists nothing: epochs are journalled in memory only. The hub itself
serves no public tool or lifecycle projection. Issue
[#1157](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1157)
adds the durable control storage seam in `internal/runtimestate` (section 5,
S3 durable control store) without a production caller, live reconciliation or
endpoint I/O. Issue
[#1112](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1112)
adds unused, opt-in export/restore hooks for a future service-owned recovery
path. Existing production publishers do not enable them and retain their
in-memory-only recovery boundary. Issue
[#1131](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1131)
adds the asset-free hub-native successor admission (`Hub.AdmitSuccessor`,
section 7, hub-native epoch succession) that a future lifecycle service uses
for an explicit RESTART or a START after a completed STOP. No service, eBUS or
production caller uses it yet. Issue
[#1183](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1183)
adds the internal desired-state service in `internal/runtimecontrol` (section
5, S1/D3 desired-state service) over that storage seam, with an injected
lifecycle executor and recovery adapter. As added there it had no production
registration, no MCP surface and no endpoint I/O; issues #1222 and #1227 below
supply the registration and the MCP surface, and no authentication applies
(D4). Issue
[#1188](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1188)
adds the service write-ahead recorder for service-routed managed writers
(section 5, S3.7 to S3.9 service write-ahead recovery): two durable steps
around every managed hub call, restoration before admission with exact-byte
gap resolution, and compaction only at a verified restore point. It is
exercised with a synthetic two-asset publisher only; no production publisher
is migrated, no eBUS shell is wired and direct publishers keep their
memory-only recovery boundary. Issue
[#1190](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1190)
adds the S4 DriverManager hooks (admission hold, classified close) and the
lifecycle-only `ebus.primary` bridge in `cmd/gateway` (section 7, S4
manager-to-hub bridge). Issue
[#1193](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1193)
adds the generation-owned post-start task runner. Issue
[#1222](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1222)
installs them into process startup (section 7, S4/S6/S7 shell integration):
`ebus.primary` is the only production registration, the durable control
service is the single owner of eBUS startup, post-start eBUS work runs as
generation-owned tasks and adapter status follows native admission. A5 is
therefore implemented as an internal service for `ebus.primary` only; it is
not a universal provider SPI. Issue
[#1227](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1227)
exposes that service through five ordinary MCP tools,
`helianthus.runtime.list`, `get`, `start`, `stop` and `restart` (section 5,
S2/D4 ordinary MCP lifecycle tools), as thin delegates with no authorization
before 0.9 (D4).

A fence or retirement batch uses the caller's `ObservedAt` and monotonic point
as its publication instant. The requesting family supplies them on the same
wall and monotonic clocks as its own facts, so the kernel can evaluate the
retained observations it keeps across the transition.

Deviation record (the normative passages are amended where they apply):

1. The single source-generation publication sequence becomes per-asset
   cursors: a sequence is scoped to (asset, source, epoch, generation) and the
   first sequence of an unseen key is 1, because the SemReg publication kernel
   is asset-local. Order across assets is the hub's in-process commit order
   only.
2. A legacy publisher's declared epoch may repeat across process
   incarnations (it may not repeat within one), instead of a new epoch per
   source on every process start (section 5, epoch barrier). The one
   exception within an incarnation is a withdrawn admission: when a runtime
   fails to start after its admission and before any publication (no asset
   holds a cursor of the epoch and no control operation targeted it), the
   admission is withdrawn and the same epoch and generation may be admitted
   again. The observable lifecycle is admitted, then retired with no
   publication, then admitted; the retirement is a hub admission record, not
   a SemReg publication retirement, and no batch carries it. Every admission
   has its own admission identity, which survives fences and activations
   but changes on readmission, so a handle of the withdrawn admission stays
   revoked for publication, transitions and activation.
3. A legacy publisher's starting driver generation may be any nonzero value
   instead of 1 (section 4, generation identity).
4. The legacy EVSE successor is admitted with a new epoch and generation
   current+1 on the retained kernel, instead of a per-epoch generation reset
   (section 4, generation identity).
5. The legacy PV epoch is sampled after the endpoint factory, dial and open,
   not before the provider factory runs (section 5, epoch barrier).

Contract ID: `helianthus.gateway.driver-runtime/v1`

Gateway source:
[public runtime repository](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/main).

Program source:
[software 0.7 guide](https://github.com/Project-Helianthus/helianthus-execution-plans/tree/main/software-stabilization-07-08.implementing).
Historical review coordinates remain in the linked issue and PR records.

## 1. Decision and boundary

The gateway owns one runtime control service and one composition boundary. It
adapts existing native runtimes; it does not replace their protocol state
machines, qualification, I/O, retry, reconnect, framing, authorization, raw
evidence, acknowledgement, readback, or close mechanisms.

The contract covers:

- deterministic `list` and `get` views;
- `start`, `stop`, and `restart` without restarting the gateway process;
- persistent desired state separate from observed runtime state;
- optimistic revision checks against durable desired revisions, with no
  lifecycle idempotency record (final D3);
- source-epoch and driver-generation fencing;
- capability activation, admission, withdrawal, drain, and close proof;
- native observation to qualified semantic publication and projection;
- semantic intent to one admitted native route and truthful outcome;
- bounded causal loop and echo suppression; and
- one SemReg public surface per migrated capability, with no pre-v1 legacy
  fallback, adapter, shadow authority, or dual semantic publication.

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
with public contract files
[`api/v1/kernel.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/kernel.md),
[`serialization.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/serialization.md),
[`acceptance.md`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/acceptance.md),
and
[`acceptance-vectors.json`](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/acceptance-vectors.json).
The merged dependency preserves the independently accepted PR content. The complete exact-HEAD Astra
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
- internal lifecycle sequence and correlation (never a client operation
  handle, final D3);
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
| Gateway runtime control | Driver catalog, desired-state persistence, lifecycle serialization, optimistic revision decisions, per-driver isolation, admission coordination, composition, and public lifecycle projection |
| Native driver/adapter | Native endpoint identity, connection and protocol lifecycle, discovery, qualification, decode, raw evidence, request construction, native retry/reconnect, native authorization, ACK/readback interpretation, and resource close proof |
| Semantic kernel and packs | Protocol-neutral identities, facts, services, capabilities, operation intents/outcomes, provenance, validation, conflict, and projection-loss types |
| Native-to-semantic binding | Exact profile/version and unique `PackRef`/`DefinitionIndex` mapping from qualified native evidence to semantic facts/capabilities and from a semantic operation and typed `ExpectedEffect` to one typed native operation and pack-evaluated readback predicate |
| Public output/consumer | Target projection and cutover behavior; never upstream qualification or native route selection |

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
    ErrorNotFound             ControlErrorCode = "NOT_FOUND"
    ErrorFailedPrecondition   ControlErrorCode = "FAILED_PRECONDITION"
    ErrorUnavailable          ControlErrorCode = "UNAVAILABLE"
    ErrorDeadlineExceeded     ControlErrorCode = "DEADLINE_EXCEEDED"
    ErrorRevisionConflict     ControlErrorCode = "REVISION_CONFLICT"
    ErrorTransitionBusy       ControlErrorCode = "TRANSITION_BUSY"
    ErrorResourceExhausted    ControlErrorCode = "RESOURCE_EXHAUSTED"
    ErrorCounterExhausted     ControlErrorCode = "COUNTER_EXHAUSTED"
)

type ControlError struct {
    Code      ControlErrorCode
    Row       int            // refusal-table order of the lifecycle page
    Message   string
    Retriable bool
    Current   *DesiredResult // REVISION_CONFLICT only
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
    Withdrawal      *GenerationWithdrawalRecord
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
    PredecessorGeneration   *GenerationKey
    // NextPublicationSequence is per asset: the next sequence of each asset
    // kernel cursor for this generation key. An absent asset starts at 1.
    NextPublicationSequence map[AssetID]uint64
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
}
type StopDriverRequest = StartDriverRequest
type RestartDriverRequest = StartDriverRequest

// DesiredResult is the whole accepted or same-desired no-op result:
// {desired_state, desired_revision}.
type DesiredResult struct {
    DesiredState    DesiredState
    DesiredRevision DriverRevision
}

type ControlService interface {
    ListDrivers(context.Context, ListDriversRequest) (ListDriversResult, error)
    GetDriver(context.Context, GetDriverRequest) (GetDriverResult, error)
    StartDriver(context.Context, StartDriverRequest) (DesiredResult, error)
    StopDriver(context.Context, StopDriverRequest) (DesiredResult, error)
    RestartDriver(context.Context, RestartDriverRequest) (DesiredResult, error)
}
```

D3 amendment to the types above: the request has no idempotency key, the
result is `DesiredResult` only, and `IDEMPOTENCY_CONFLICT`,
`PERMISSION_DENIED` and every receipt, disposition and operation-ID member are
withdrawn (final D3/D4). `DesiredRecord.LastOperation`,
`ObservedRecord.ActiveOperation` and the operation ID, request digest and
completion members of `LifecycleOperationRecord` are superseded as well: the
durable in-flight state is the typed work slot record of section 5, and
completed operations leave no durable record. `CLOSE_UNCONFIRMED` remains an
observed-state reason, not a request refusal. The lifecycle request carries no
idempotency key at all; the intent idempotency of sections 8 to 10 is a
separate semantic mechanism. `DriverView`, `ListDriversResult` and
`GetDriverResult` above describe internal content, not the wire: the public
projection is the closed eight-member driver object of section 5 (S2/D4
ordinary MCP lifecycle tools).

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

An accepted decision and a same-desired no-op return the same closed
`DesiredResult`; neither carries an operation ID, a lifecycle sequence, a
disposition or a completion claim. A lost response is resolved by list/get
readback, and a repeated changing request with an old expected revision
conflicts.

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

### S0 registry and typed provider seam

Package [`internal/runtimecontrol`](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/main/internal/runtimecontrol) implements
the S0 slice of the registration companion
[runtime driver lifecycle](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
(A1): descriptor format `v1`, validation before effects, a frozen in-memory
registry with coherent list/get reads, and the typed lifecycle-adapter seam
(`NewAdapter` over a factory returning a `drivermanager.Runtime`). By itself
it performs no construction, desired-state persistence, lifecycle control or
runtime composition (the production registration of `ebus.primary` is
section 7, S4/S6/S7 shell integration), and it imports only the standard
library and `internal/drivermanager`.

`Register` validates a whole batch, disabled entries included, and installs it
in one step or not at all. Per descriptor, in order: format, `DriverID`,
direction, disposition, `NativeContractRef`, adapter, owned sources, hooks.
Then, against the installed registry and the batch: duplicate `DriverID`,
duplicate source ownership, and a source the hub already attributes to
another driver. Any refusal invokes no adapter, bumps no `CatalogRevision` and
mutates no hub ownership; the hub is consulted through a read-only view. A
`NativeContractRef` field is 1 to 256 bytes of `[A-Za-z0-9._/+-]`, starting
alphanumeric, with no `..`, no `//` and no IP literal, so URLs, host:port
pairs and userinfo cannot pass. A source ID follows the SemReg opaque rule.
An empty owned-source set is accepted. Every hook declaration fails:
`mcp`, `setup`, `portal`, `semantic` and `evidence` as unimplemented, any
other name as unknown; `ResolveHook` returns the same refusals for a
registered driver. Reads return detached copies; `Binding` returns the frozen
construction material and stored adapter without invoking it, and unknown or
noncanonical IDs return `NOT_FOUND`.

`DriverID`, `SourceID`, `SourceEpoch`, `DriverGeneration`, `NativeGeneration`,
`DesiredRevision`, `CatalogRevision`, `LifecycleSequence` and `HubSeq` are
distinct defined types in this package, not aliases of each other or of the
publication hub's identifiers. The snapshot and correlation types carry no
receipt, idempotency key, principal or authorization field (final D3/D4).

An epoch transition has a non-zero `AttemptLimit` and finite `DeadlineUTC` from
the manager's lifecycle-construction policy. Those bounds persist across
component/reconciler recovery in one source epoch and are not reset by a wakeup.
After `BLOCKED`, only a new accepted lifecycle operation may supply another
bounded budget; it does not reopen admission or provider work before the same
transition barrier completes.

## 5. Mutation, persistence, and reconciliation

Lifecycle calls acknowledge a persistent control decision. They do not claim
that native startup, shutdown, or replacement has completed.

D3 amendment: the docs-ebus
[runtime driver lifecycle](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
sections "S1/D3: minimal durable control decisions" and "S3/D2: bootstrap,
load failures and explicit repair" supersede the receipt, idempotency-key,
principal-scoped ledger, request-hash and completed-operation rules of this
section, of the receipt paragraphs in section 4 and of the receipt-specific
CTL rows in section 13. Where this document still describes those
mechanisms, the lifecycle page wins. The subsection below records what the
storage seam implements.

### S3 durable control store (implemented storage seam)

Package [`internal/runtimestate`](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/main/internal/runtimestate) owns the
independently versioned `runtime_control` namespace (format version 1) under
the sole runtime-state writer. Top-level, meta and eBUS versions are
unchanged; unknown namespaces keep round-tripping. The namespace holds only:
one issued-revision mark, the catalog view revision, one record per decided
driver (desired `RUNNING` or `STOPPED`, its desired revision, the internal
lifecycle sequence and at most two typed work slots, current and reserved (the current slot always holds
the older transition: its sequence precedes the reserved sequence, and a record
violating this fails only that driver),
with exact kind, accepted revision, sequence, phase, correlation, cleanup
obligations, bounded attempts and deadline) and a typed D5 recovery envelope
whose payload is opaque to the writer and verified by digest. There is no
receipt, idempotency key, principal, request digest or completed-operation
store. Revisions are encoded as canonical decimal strings and held as
unsigned 64-bit values; every increment that would overflow refuses with
`COUNTER_EXHAUSTED` before mutation.

Load dispositions are distinct: `MISSING` (readable state with no control
material), `LOADED`, `MISSING_WITH_MATERIAL` (no namespace while a quarantine
copy exists), `CORRUPT` (whole-file corruption, or a namespace whose
structure, mark, catalog revision or recovery envelope cannot be validated),
`UNREADABLE`, `UNSUPPORTED` (a schema version other than 1) and `LOCKED`
(the store lock cannot be acquired). Every disposition except `MISSING` and
`LOADED` fails control closed for the namespace only: the shell keeps
starting, every configured driver is withheld as `FAILED`, nothing is seeded
and the namespace bytes are preserved verbatim on every later write. A
malformed driver record fails only that driver while independent records stay
usable. A file that cannot be read, or whose corrupt quarantine rename fails,
blocks persistence so its bytes are never overwritten; a quarantine copy is
never replaced. The writer holds an exclusive `flock` on the `<state>.lock`
sidecar from load until stop, which the offline reset also takes. Any sidecar
open or lock acquisition error (another holder or any other failure) yields
`LOCKED` and blocks persistence for the process: no decision, work, catalog,
recovery, seed, registration or generic write is accepted, and a corrupt file
is not quarantined. The readable file is still decoded read-only: the GUID, the
eBUS cache and opaque namespaces keep their last-known-good in-memory values,
and the last-known-good `runtime_control` namespace is served by the snapshot
and per-driver reads with `ReadOnly` set while every configured driver starts
`FAILED`. It is never read as missing and never overwritten later. Stop waits
for every in-flight writer (a control transaction or a ticker flush) before it
closes write admission and releases the lock. The final flush takes the writer
slot with the stop context; a context that ended before or during Stop skips
the flush and is returned, and the lock is still released as soon as the
in-flight writer finishes. Control admission reports closed once the manager is
stopped or persistence is blocked. A work update of a stored identity may not
move its phase backwards, lower its attempts used, or extend or remove an
established deadline.

The one-time seed runs only for `MISSING` with a valid persisted instance
GUID: the mark starts at 0 and each configured driver, in bytewise `DriverID`
order, is one accepted decision at mark + 1 (enabled `RUNNING`, disabled
`STOPPED`). The store never generates or replaces the GUID and never touches
meta. In an existing namespace a configured driver without a record becomes
visible through a mark-only write, and a record whose driver is no longer
configured and holds no work slot or D5 reference is deleted with a mark-only
write, in one durable transaction before any read. A driver registered earlier
in the process that is later unconfigured also advances the mark and catalog
(a mark-only write) even when it has no stored record. The startup plan derives
`STARTING` only from durable desired `RUNNING` of an enabled driver; durable
`STOPPED` and record-less drivers start `STOPPED`, disabled configuration
yields `DISABLED` without rewriting intent, and failed control yields
`FAILED`. Process shutdown never changes durable intent.

`DecideControl` applies the lifecycle refusal table in order: closed request
shape (`DriverID`, `ExpectedRevision` as a nonzero canonical decimal string),
registration, control state (a failed load or record is `FAILED_PRECONDITION`,
pending readback is `UNAVAILABLE`), configured eligibility and a finite
unexpired deadline, all before the serialization point. The namespace-wide
serialization point is the runtime-state sole-writer lock shared with cache,
meta and ticker writes; the wait is bounded and never outlasts the deadline.
After acquiring it the request reconfirms admission, rechecks the deadline,
reads the mark, compares the expected revision with the driver's current
desired revision (the mark for a driver without a record), answers a
same-desired START or STOP as a no-op without writing, refuses RESTART in
desired `STOPPED`, consults the slot planner (rows 7 and 8), checks the record
and byte limits and the counters, rechecks the deadline and durably writes the
decision with the advanced mark and catalog revision. A bounded-wait timeout
refuses `REVISION_CONFLICT` only for a conclusively stale expectation and
`TRANSITION_BUSY` otherwise, with no durable change. Accepted and no-op results
are `{desired_state, desired_revision}` only.

Every control commit (seed, registration, decision, work, catalog and
recovery) checks the namespace byte limit on the staged namespace first and
refuses `RESOURCE_EXHAUSTED` without writing. A work update may only advance the
phase, correlation and obligations of a stored transition, promote the reserved
slot or clear slots; the accepted identity (kind, desired revision and
sequence) never changes.

A control transaction stages privately, persists the whole file through a
same-directory temporary file, a required file fsync and a rename (no
cross-filesystem fallback) and exposes the committed state only after a
successful parent-directory fsync. Any failure before the rename discards the
staging and refuses `UNAVAILABLE`; no later cache, meta or ticker write can
publish it, because generic writes re-emit only the committed namespace. A
directory fsync failure after a successful rename, including an error the
generic namespaces tolerate, is ambiguous persistence: nothing is
acknowledged or exposed, admission is closed for every driver and generic
writes are deferred until the readback finds the stored namespace equal to
the staged or the committed one and a directory fsync succeeds; readback
agreement alone never reopens admission. The generic namespaces keep the
best-effort directory fsync.

The offline repair is `scripts/reset_runtime_control.py --state-file <file>
--namespace runtime_control --desired STOPPED --driver-id <id>...`. It
previews by default and replaces only with `--apply --gateway-stopped`, refuses
while another process holds the store lock, refuses a corrupt or unreadable
file (including any preserved meta/eBUS value the gateway loader rejects:
invalid typed fields, out-of-range addresses or duplicate member addresses), a missing or invalid GUID, an unsupported namespace
version or unknown namespace member, an unvalidated mark or catalog revision, an
unverifiable D5
envelope (including a driver ID that is not canonical or exceeds 128 bytes) or
one recording a revision above the mark, and an unvalidated
configured ID set (`FAILED_PRECONDITION`, or `COUNTER_EXHAUSTED` when `H + N`
or `C + 1` overflows). It writes an exclusive, fsynced quarantine copy of the
exact original bytes, then replaces only the namespace member: one `STOPPED`
record per configured ID at `H + 1` through `H + N` in bytewise order with no
work slot, the mark `H + N` and catalog revision `C + 1`, copying every other
member and the D5 envelope byte-for-byte. Failures before replacement leave
the original file intact; a rerun overwrites its stale temporary file. After
replacement, a directory fsync failure or unconfirmed readback reports
`AMBIGUOUS` (exit 5), retains the quarantine copy and requires inspection of
the state before retrying; it never claims that the original was preserved.

### S1/D3 desired-state service (implemented internal service)

The receipt, idempotency-ledger and operation-record transaction that this
section described before the D3 amendment is withdrawn. Package
[`internal/runtimecontrol`](https://github.com/Project-Helianthus/helianthus-ebusgateway/tree/main/internal/runtimecontrol) implements the
S1/S2.1/S5/S6.1 slice of the lifecycle page as an internal `Service`. It
performs no endpoint I/O of its own and has no authentication (D4). Its
production registration is section 7 (S4/S6/S7 shell integration), its only
external surface is the ordinary MCP adapter below (S2/D4 ordinary MCP
lifecycle tools), and it still imports only the standard library and
`internal/drivermanager`.

**Seams.** The service reaches the S3 store only through the `Store`
interface (decide, commit work, advance catalog, commit recovery, coherent
catalog and driver reads, admission and readback). Each driver has an
injected `LifecycleExecutor` (`Start` and `Stop` with a
`LifecycleCorrelation`; `Stop` reports native close and semantic cleanup as
two independent obligations), and the service has one injected
`RecoveryAdapter` (`Restore`, `Secure`). There is no default recovery adapter
and no fake success path. `New` refuses a missing store, recovery adapter or
incarnation name, a noncanonical or duplicate driver ID, a missing executor,
more than 128 drivers and an attempt limit above 8, all before any store,
executor or adapter call; `Start` refuses a driver set that differs from the
store's visible drivers without writing.

A `DriverConfig` may carry a `RetryPolicy` that narrows the retry policy for
that driver only. Its `MaxAttempts` bounds the start attempts of one epoch
(the generation counts them) and never raises the service limit; a positive
`Delay` replaces the service retry delay before each of that driver's
retries. A retry still needs a retryable executor error and stays inside the
persisted per-operation budget. The zero value keeps the service policy, and
`New` refuses a negative value, an attempt limit above 8 or a delay above the
30-second budget.

**Decisions.** `Control` decodes the closed two-member request (refusal-table
row 1) and `Decide` checks the method and registration (rows 1 and 2)
without the store; the store applies rows 3 to 10 under its namespace-wide
serialization point. The service's slot planner runs under that point and
takes only the driver's own mutex:

- with no typed work the request takes the current slot, except during an
  untyped process-start attempt, where STOP or RESTART synthesizes
  `CANCEL_ATTEMPT` as the current slot (with the attempt's epoch, generation,
  both cleanup obligations and a deadline) and takes the reserved slot;
- behind a current transition the request takes the empty reserved slot;
  STOP or RESTART behind a START fences that START;
- an executing RESTART or a full reserved slot is `TRANSITION_BUSY` (row 8);
- quarantine or unresolved recovery refuses START and RESTART at row 7, while
  STOP stays allowed;
- every plan must enqueue exactly one new transition of the requested kind,
  keep stored work unchanged and never leave a reserved slot without a
  current one; anything else fails closed as `TRANSITION_BUSY`.

An accepted decision is durable before `Decide` returns. Only then does the
service publish the view, emit `DESIRED_ACCEPTED`, fence the superseded
attempt (cancel its context and mark its pending result as never to be
installed) and wake the driver's reconciler. From its planner until that
acceptance is published, the decision holds the driver: no attempt result is
installed, no slot is pruned and the reconciler takes no step for it, even
when it already sees the committed slots, so reconciliation never runs ahead
of the acceptance. A same-desired no-op writes,
publishes, emits, fences and wakes nothing. Both return
`{desired_state, desired_revision}` only. Cancelling the caller's context
after acceptance retracts nothing: the work runs under the service's own
lifetime, within the persisted per-operation budget (at most 8 attempts and a
30-second deadline, never renewed by a wake, a no-op or recovery).

**Reconciliation.** One reconciler per eligible driver derives work only from
the committed typed slots. `PENDING` is accepted and not begun. `ACTIVE`
carries the correlation being started or stopped with both obligations
pending. `CLEANUP` is settling, and with both obligations cleared it is
terminal. A START with a reserved successor is canceled: a Stop with its own
correlation settles its obligations, and a late success is never published.
STOP withdraws the admitted generation and publishes `STOPPED` only after both
obligations are durably cleared. An unconfirmed native close publishes
`FAILED/CLOSE_UNCONFIRMED` with the safety quarantine, retains the slot and
blocks admission until process restart. RESTART withdraws the admitted
generation, then starts one replacement under a new epoch. A START that
exhausts its budget ends `FAILED` with desired `RUNNING` unchanged; recovery is
a RESTART, never a START no-op.

Cleanup runs within the same nonrenewable budget. Each cleanup attempt is
charged durably in the slot before the executor's `Stop` is invoked, so
process loss during `Stop` never grants an extra attempt. `Stop` runs under a
context that ends at the persisted deadline; once the deadline has passed it
receives an already-expired context, and a late process-start success is
withdrawn under the attempt's original deadline in the same way. No new
deadline is computed for cleanup. The attempt limit governs start attempts
only: cleanup owns one further reserved attempt, accounted apart from the start
budget, so a limit of one cannot let a START consume the attempt that native
close and semantic cleanup need. A START that succeeds after its deadline still
gets that reserved cleanup attempt under its original deadline. When the
reserved cleanup attempt or the deadline is
spent with semantic cleanup still pending, the driver ends `FAILED` with a
`CLEANUP_STALLED` diagnostic, START and RESTART refuse at row 7 until process
restart, the obligation stays durable and no further `Stop` runs. An
unconfirmed native close quarantines as described above. A RESTART creates its
replacement only while a start attempt and time remain in the slot's budget
after the predecessor's cleanup; when the predecessor's cleanup used the last
permitted attempt, the RESTART ends `FAILED` with a retry-exhausted diagnostic
and no replacement is started. Budgets are never renewed: a new RESTART is a
new decision with a new budget. A charge whose commit outcome was unknown and
that readback adopted is not charged a second time.

Epochs are derived from the incarnation name,
the driver ID and a per-process ordinal, so every process start, RESTART and
START after STOP gets a new epoch (final D1). The generation starts at 1 in
each epoch and grows by one per retry.

Before every work commit the service also checks the progression of the
stored transition. An unassigned correlation may be assigned once. Within one
epoch the generation only grows, and an obligation only goes from pending to
cleared, except at the `PENDING` to `ACTIVE` step that incurs it. A new epoch
replaces the old one only after both old obligations are cleared, and an
established epoch is never removed.

**Recovery.** `Start` restores the committed D5 material before any executor
call or admission. An adapter that implements `EmptyRestorer` is also told
when no material is committed (`RestoreEmpty`), so its managed writers open
only after this step. A restore failure withholds the drivers it names as
`FAILED/DEPENDENCY_UNAVAILABLE` with no reconciler, and START or RESTART for
them refuses at row 7. A `*RestoreError` names exactly the drivers it failed
closed; any other failure names the material's drivers, or every driver when
the material names none. Otherwise each slot resumes from its own state:

- `PENDING` runs;
- `ACTIVE` left by an earlier incarnation first settles the historical
  correlation, then runs under a new epoch, so an unfinished RESTART performs
  its one replacement although desired already says `RUNNING`;
- `CLEANUP` with pending obligations only settles them;
- a terminal slot is only pruned.

A driver with no slot and desired `RUNNING` gets the untyped process-start
attempt, which persists nothing.

**Ambiguous persistence.** When a decision or work commit reports ambiguous
persistence, the store keeps the staged state hidden and closes admission.
The service then runs a bounded readback itself, without waiting for another
request. The first readback runs at once, and at most the attempt limit of
readbacks follow, spaced by the retry delay and each bounded by an equal share
of the operation budget (about 50 seconds in all with the defaults). Admission
reopens only when the store's readback agrees with the staged or the committed
namespace and directory durability is re-established. The service then
publishes the committed view and wakes every reconciler, so an adopted
decision is shown and executed. A decision whose commit was ambiguous keeps
holding its driver until then, and a further decision for that driver is
refused `TRANSITION_BUSY` meanwhile. It is adopted exactly when the driver's
committed desired revision moved off the request's expected revision. An
adopted decision publishes its `DESIRED_ACCEPTED` with that view, exactly as a
direct commit would and never earlier, and only then fences the START it
supersedes. A rejected decision emits nothing and fences nothing: a STOP or
RESTART that did not persist leaves the START it would have canceled running
with its attempt, budget and original deadline unchanged. A disagreement fails control closed in the
store; an unresolved series is reported, and admission stays closed. List and
get keep serving the last committed view throughout. A driver with desired `STOPPED` and no slot
is left alone. No completed-operation history is consulted.

**Pruning.** A terminal slot is pruned only after `Secure` returns its D5
material and that material is committed; a `Secure` failure keeps the slot
durable and retries. While a terminal slot with an empty reserved slot is
being pruned, a changing decision for that driver that still sees the slot is
refused `TRANSITION_BUSY` until the prune commits, because the store accepts
clearing both slots and a newly reserved transition must never be dropped.
Diagnostics are an in-memory
ring of at most 32 sanitized terminal summaries of at most 512 bytes per
driver, built from closed codes and counters only. They are dropped with the
process and never used for admission, deduplication or recovery.

**Views and events.** List and get read an atomically published view and
never wait for a store write, a reconciler or a decision. The view is rebuilt
from one coherent store read plus the staged observations. Every observed
transition durably advances the catalog revision before the view that shows it
is exposed, and a revision's content never changes once exposed. Observed
transitions never change desired state, its revision or the mark. Events
(`DESIRED_ACCEPTED`, `OBSERVED_CHANGED`) are appended to a bounded ring
(default 256, at most 4096) only after the transition is committed and visible
at the event's catalog revision. A reader behind the window, or ahead of it,
is told to resync from `List`, whose `EventCursor` names the newest event
included. A view and its events are published atomically for event readers:
the events are retained and the view stored under the event log's lock, and
waiters are woken only afterwards. A resync at a published `EventCursor`
therefore always finds that cursor's events. A pending `OBSERVED_CHANGED`
event that the published view no longer shows (because a newer observation
of the same driver is staged and has its own event) is coalesced away rather
than stamped with the newer view's revision. When control state is read-only
at start, every driver is withheld as `FAILED`, but the view keeps the stored
catalog revision and each stored desired revision unchanged. Durable withholding refuses lifecycle mutations with non-retriable `FAILED_PRECONDITION` carrying the withholding reason, transient store conditions retain retriable `UNAVAILABLE`, and the withheld view preserves configured enabled state and available durable desired intent independently of service startup. An event carries the driver ID, its kind, the catalog revision,
desired state and revision, observed state, reason, the quarantine flag and
the lifecycle correlation (source epoch and driver generation) the observation
refers to; an accepted decision carries the driver's correlation at
acceptance. Views and events carry closed codes and identities only: no
operation ID, endpoint, credential, raw error, principal or authority field.

**Isolation.** The only service-wide lock guards in-memory view assembly and
is never held across a store write, an executor call or an adapter call. A
blocked or failing executor of one driver delays only that driver's
reconciler. While one driver's store write is blocked, other drivers' reads
stay immediate and their decisions wait at most the store's bounded
serialization wait before `TRANSITION_BUSY`.

**Errors.** Recovery-adapter (semantic) failures are reported as
`SemanticError`, which keeps the original error for `errors.Is` and
`errors.As` and is never turned into a lifecycle refusal code. Executor
failures are `LifecycleError`, and request refusals are `Refusal` with the
refusal-table code and row. Executable evidence is
`internal/runtimecontrol/service_test.go` against the merged store.

### S2/D4 ordinary MCP lifecycle tools (implemented MCP adapter)

Issue
[#1227](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1227)
implements the first external control surface of the lifecycle contract: the
D4 amendment to the docs-ebus
[MCP page](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/mcp.md)
and the
[runtime driver lifecycle](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
section "S2/D4: ordinary MCP external control", proposed in
[docs-ebus #718](https://github.com/Project-Helianthus/helianthus-docs-ebus/pull/718).
It replaces revision 9's kernel operator boundary and experimental tools.
Where that amendment and the merged S1/D3 rules differ, the merged rules and
the service behaviour above apply.

**Registration.** Package `mcp` registers `helianthus.runtime.list`,
`helianthus.runtime.get`, `helianthus.runtime.start`,
`helianthus.runtime.stop` and `helianthus.runtime.restart`, in that order, on
the ordinary tool inventory (`RegisterRuntimeControlTools`). The production
HTTP shell registers them unconditionally while it composes the ordinary MCP
server, before the MCP route is mounted, so they stay listed for the shell's
lifetime whatever eBUS does (STOPPED, FAILED, withheld or disabled) and with
eeBUS disabled or absent. A disabled driver stays visible; tool disappearance
never signals unavailability. A control path whose service could not be built
still lists the tools and answers `UNAVAILABLE`. The tools are host tools,
not driver contributions: driver hooks stay unimplemented.

**Transports.** The tools use the ordinary registration and dispatch path, so
they are served by every ordinary MCP transport the gateway offers. Today
that is the HTTP JSON-RPC endpoint. The gateway has no stdio MCP transport
and this slice adds none; the amendment's "HTTP and stdio" is read as every
ordinary MCP transport the gateway offers. An endpoint that serves the
ordinary inventory (the owner-only eeBUS operator endpoint does) lists them
too; the read-only evidence scope does not expose them.

**Delegation.** The adapter is a thin delegate of one `ControlService` and
never calls DriverManager, constructs a provider, opens an endpoint,
reconciles or polls:

| Tool | Complete arguments | Service call |
|---|---|---|
| `helianthus.runtime.list` | `{}` | `List()`: one coherent cached view |
| `helianthus.runtime.get` | `{"DriverID": ...}` | `Get(DriverID)`: one cached entry |
| `helianthus.runtime.start` | `{"DriverID": ..., "ExpectedRevision": ...}` | `Control(ctx, START, body)`: the service's closed decoding, then `Decide` |
| `helianthus.runtime.stop` | same two members | `Control(ctx, STOP, body)` |
| `helianthus.runtime.restart` | same two members | `Control(ctx, RESTART, body)` |

**Closed inputs.** Arguments are closed, case-sensitive objects: list takes
no member, get exactly `DriverID`, each mutation exactly `DriverID` and
`ExpectedRevision`, every value a JSON string. Missing, additional or
duplicate members, non-string values, non-object arguments (an explicit
`null` included) and trailing data are refused as `INVALID_ARGUMENT` (refusal
row 1) before any service read or decision; an omitted `arguments` member
reads as the empty object. Duplicates are detected anywhere in the call
parameters on the token stream, before any decoding could discard one, and
answer the same tool-level refusal. Mutation bodies reach the service
unchanged, so the MCP refusals are the service's own. Inside a well-formed
JSON-RPC request the arguments value cannot carry trailing data; a request
body with trailing data is refused by the JSON-RPC layer before dispatch.
`DriverID` follows the canonical pattern of section 4 and at most 128 bytes;
a noncanonical or unregistered ID is `NOT_FOUND` from the cached read.
`ExpectedRevision` echoes the read-back `desired_revision` as a canonical
nonzero unsigned 64-bit decimal string. No idempotency key, digest, receipt,
principal, authority, operation handle or deadline member is accepted or
returned.

**Deadlines.** Every mutation reaches the service with a finite deadline.
When the transport supplies none (an HTTP request has none), the adapter
bounds the call to 10 seconds, longer than the store's bounded namespace-wide
serialization wait, so contention answers `TRANSITION_BUSY`; an earlier
caller deadline is kept. The service's refusal for a call without a finite
deadline (refusal row 5, `INVALID_ARGUMENT`) is therefore unreachable through
MCP by construction: the tools always supply the 10 second default and keep
an earlier caller deadline. The reads take no deadline: they never wait for a
store write, a reconciler or a decision.

**Data.** List `data` is exactly `{catalog_revision, drivers}` and get `data`
exactly `{catalog_revision, driver}`. A driver object has exactly
`driver_id`, `enabled`, `desired_state`, `desired_revision`,
`observed_state`, `reason`, `safety_quarantined` and `revision`, in that
order, projected from the cached `DriverStatus`. List entries come from one
coherent view in bytewise `driver_id` order, each carrying the view's catalog
revision, with `drivers: []` for an empty catalog; get uses its entry's
revision as `catalog_revision`. Revisions are canonical decimal strings and
read `"0"` only for a withheld or unissued view. Accepted and same-desired
no-op mutations answer exactly `{desired_state, desired_revision}` after the
decision is durable: completion is observed through list or get, and readback
never attributes a completion to a call. START while desired `RUNNING` and
STOP while desired `STOPPED` are no-ops after the revision comparison. Every
accepted RESTART takes a new revision and executes one replacement; after a
lost RESTART response an old-revision retry conflicts, and a fresh-revision
retry after settlement restarts again.

**Envelope and errors.** All five tools use the ordinary envelope:
`meta.contract` `helianthus-ebus-mcp` 1.0, `meta.consistency` `LIVE` with no
snapshot argument, `meta.data_timestamp` and `meta.data_hash` (SHA-256 of
the serialized `data` alone). Success has `error: null`. A refusal sets MCP
`isError` and the error object `code`, `message`, `retriable` and
`source_layer` (`gateway`), keeping the lifecycle code and the refusal
table's retryable column:

| Code | Retriable | Data |
|---|---|---|
| `INVALID_ARGUMENT` | no | null |
| `NOT_FOUND` | no | null |
| `FAILED_PRECONDITION` | no | null |
| `UNAVAILABLE` | yes | null |
| `DEADLINE_EXCEEDED` | yes | null |
| `REVISION_CONFLICT` | yes | the cached `{desired_state, desired_revision}` |
| `TRANSITION_BUSY` | yes | null |
| `RESOURCE_EXHAUSTED` | yes | null |
| `COUNTER_EXHAUSTED` | no | null |
| `INTERNAL` | no | null |

Messages are fixed categorical texts per code and refusal row; refusal
reasons and raw errors, which can name storage paths, never reach the wire.
A service error outside the refusal table (an unresolved readback or a
closed service), and a refusal code the table does not list, answers
`INTERNAL`, not retriable, after the service's own readback; the caller reads
the durable state through list or get before deciding again. `UNAVAILABLE` is
reserved for the table's transient rows.

**Boundaries.** There is no lifecycle principal, authority adapter or
authorization check before 0.9, no separate listener or socket and no
operator-boundary-only registration. The tools never route through
`ebus.v1.rpc.invoke` and grant no native pairing or device-write authority;
the eeBUS operator tools and every native-write safeguard are unchanged. The
tools are classified `core_stable`, which implies no GraphQL face: no
lifecycle GraphQL field, resolver, mutation or subscription exists, and the
eBUS MCP/GraphQL status parity is unchanged. Every namespace stays pre-stable
until 1.0.

Executable evidence: `mcp/runtime_control_test.go` (schema and output
goldens `mcp/testdata/runtime_control_tools.json` and
`mcp/testdata/runtime_control_outputs.json`, strict decoding, deadlines,
MCP-to-service equivalence for every refusal code) and the fake-endpoint
`runGatewayLifecycle` integration through the production HTTP shell in
`cmd/gateway/runtime_control_test.go`. These are offline software tests; they
claim no transport execution on hardware, smoke result or qualification.

### S3.7 to S3.9 service write-ahead recovery (implemented internal recorder)

`internal/runtimecontrol` adds a `Recorder`, the service's D5 recovery adapter
for registered, service-routed managed sources. Each `ManagedSource` names its
driver, its source and its complete owned asset set; an asset belongs to one
source. The recorder still imports only the standard library and
`internal/drivermanager`. It reaches the publication hub through the
`ManagedHub` and `Replay` seams (opaque bytes only) and a `RecoveryClock`
(process samples, restart-comparable elapsed-time anchors). The bridge from the
in-process hub to these seams exists only in the package tests; a later wiring
ticket places it in the composition layer, as for the store bridge. Direct
publishers that do not route every mutation through a `Writer` keep their
memory-only boundary and get no durable-replay claim.

**Two durable steps.** A `Writer` publication or control takes the
service-side source gate, then every affected asset gate in bytewise order
(the hub and the store manager hold no lock across any of it). Under those
gates it assigns the source's next WAL sequence, one intent ordinal per asset
and one monotonic point, and durably persists them with the exact staged
envelope, the advanced per-asset clock high-water mark and its elapsed-time
anchor. Only then does it call the hub. A control's per-asset attempt bytes
exist only inside the hub call, so each asset's exact attempt is persisted
from the hub's pre-submission callback, outside every hub lock, before that
asset is submitted; a failed write refuses that asset's submission. After the
hub returns, the gates are released and a second transaction persists each
asset's accepted step (exact bytes, original point, opaque result with
snapshot identity, canonical bytes, revision vector and cursor, incarnation
and `HubSeq`) or its verbatim rejection identifier. Both steps rewrite the one
D5 envelope; memory changes only after a write succeeds, so a failed intent
consumes no counter, never reaches the hub and leaves no staging behind. A
failed outcome write leaves the intent pending (`ErrOutcomeNotDurable`) until
`Flush`, `Secure` or a restore resolves it. An uncertain hub completion blocks
the source and its assets for the rest of the process.

**Clock.** Each new point is `max(H + d + (m - m0), last + 1)`, where `H` is
the asset's durable high-water mark, `d` the verified elapsed time from its
anchor to the process sample `m0` taken at restore, and `m` the current
sample. Every reserved point, including pending and rejected attempts, is
persisted with its intent; `H` is never lowered. Missing or unverifiable
continuity, a backward clock or arithmetic overflow fails the asset closed.

**Restore before admission.** `Start` calls `Restore` (or `RestoreEmpty`)
before any executor call. The recorder validates ownership and ordering: a
stream naming an unmanaged source or asset is refused as a whole, a WAL
sequence gap (for example a removed intent) fails its source closed, and an
ordinal gap or malformed asset record fails its asset closed. It replays the
verified checkpoint on detached kernels, then walks the intents in stream
order. That order respects both the per-source WAL order and the per-asset
ordinal order, because every intent is persisted under its source gate and
all its asset gates, so every lower pending entry is resolved before a
dependent recorded outcome. A recorded accepted step is replayed at its
original point and verified against its stored result; a recorded rejection
is reinstated and never applied. A pending step compares the replayed cursor:
a matching sequence and digest reads back, an unchanged cursor resubmits the
exact stored bytes at their original point, and anything else, or an
uncertain resubmission, fails the source closed. A control asset that was
never durably staged was never submitted and consumes its ordinal without a
result. The replay is then installed into the hub, which refuses any asset
that already holds state or a source that was already admitted. Each
installed asset is reprojected afresh; only an acknowledged reprojection
reopens reads, so an asset behind a rejected or unattempted control stays
closed. Failures are returned as `*RestoreError`, which withholds only the
named drivers; healthy unrelated assets and sources are restored. Prior
epochs, admissions and native-close claims stay historical: the new epoch's
first batch retires the prior epoch, and an unfinished cleanup keeps its
native-close obligation until the executor proves the close.

**Verified compaction.** `Compact` (also run after a successful restore and
when the entry count reaches the configured threshold, default 64) takes every
source and asset gate and refuses while any intent lacks a durable or
restore-resolved outcome, any outcome is queued, or any path is failed closed
or blocked: that material stays reachable. It exports a hub checkpoint,
restores it into a scratch hub and compares every managed asset's exact state
(accepted log and results, snapshot identity, canonical bytes, revisions,
`HubSeq`, control gate) with the live hub. Only a match replaces the stream in
one atomic write that keeps every counter and clock mark; any refusal,
mismatch or write failure leaves the previous stream byte-identical.

**Budgets.** One operation stages at most 128 assets and 8 MiB; at most 128
intents may lack a durable outcome; the encoded stream is bounded (default
48 MiB of the 64 MiB namespace). Before an intent is accepted the recorder
reserves room for its outcome (four times the staged size plus twice the
largest recorded result per asset, plus 4 KiB per asset). A request that
cannot fit is refused `RESOURCE_EXHAUSTED` before any counter, point or hub
call is consumed; nothing reachable is evicted. The stream holds no receipt,
idempotency key, principal or completed-operation record: entries leave it
only at a verified restore point.

**Ambiguous recorder writes.** A recorder write after which the store keeps
admission closed (for example a rename that succeeded and a directory sync
that failed) is handled exactly like a service-owned write. The recorder
implements `ReadbackBinder`: `Start` binds the service's bounded readback
resolver before restoration, and the ambiguous write arms it at once, with no
further request. The first readback runs immediately, the bounded retries
follow, and admission reopens only when the readback agrees with the staged
or the committed envelope and directory durability is re-established;
otherwise control stays failed closed. While the readback is pending, managed
publications, controls and `Secure` are refused `UNAVAILABLE` (retryable),
before any gate, hub call or store write; each refusal re-arms the resolver if
it has ended. When a readback reopens admission, the service calls
`ReadbackResolved` and the recorder settles the write against the committed
envelope, without calling the hub and without rebuilding or replaying
anything. If the staged bytes persisted, memory adopts them exactly; if they
did not, the staged transaction is discarded. An intent whose hub call never
ran (its caller was already refused) is withdrawn rather than adopted: memory
keeps the previous stream, and the next transaction rewrites the envelope
without it. A restart before that transaction resolves the intent like any
pending intent, so the hub sees it at most once. A queued outcome is written
again by the next `Flush` or `Secure`; reapplying an adopted outcome is
byte-identical. A committed envelope that matches neither stream fails every
managed path closed and refuses every later recorder write. No fsync runs
under a hub or store-manager lock.

**Isolation bound.** No durable write runs under a hub lock or the store
manager's read path. While a managed source's durable write is stalled inside
the store's file write, another family's `Admitted` and `ReadAdmitted` reads,
its direct publications, a managed asset's `Inspect` and the committed
control read each return within 100 ms.

**Limits.** A transition cover that the hub installs inside a publication
commit is not routed through a `Writer`; managed writers must not rely on
generation transitions until a later slice records covers. The hub itself
keeps every accepted batch in memory, so a checkpoint grows with history; the
stream bound refuses new work rather than evicting it. Executable evidence
is `internal/runtimecontrol/recovery_test.go` (S3.9 cases a to f, cursor
rules, compaction, clock, capacity, isolation and ambiguous recorder writes)
against the merged store and the in-process hub.

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
`CONTROL_OWNED/EPOCH_BARRIER` with an empty per-asset
`NextPublicationSequence` map, so every asset starts at sequence 1; generation
0 is never used. The record's `Assets` is a bytewise `AssetID`-sorted materialization
of every asset in the gateway's durable source-publication index, including
assets carried forward from an incomplete prior transition or semantic-cleanup
journal. Before forwarding any manager or adapter batch, the manager persists
its canonical bytes and exact asset/source ownership as pending publication
work; an accepted result updates the index with `SnapshotID`, semantic revision,
epoch, generation, sequence, batch ID, and digest. Crash recovery therefore
cannot omit an accepted, possibly submitted, or cleanup-incomplete asset. The
index is never rebuilt from a live provider, current discovery result, or
compatibility alias.

A6 recovery prerequisite (S3.8): the future control service owns write-ahead
persistence outside every hub mutex. `internal/semhub` supplies only opt-in
`Options.Recovery` hooks, `Tx.Export`, `ExportRecovery`, and `Restore`; these
perform no filesystem, fsync, provider, or native I/O. `BeforeControl` receives
an owned copy of the exact staged JSON batch, its sequence/digest, control key,
and persisted monotonic point before submission, and can refuse submission.
`Accepted` receives a separate owned copy of an accepted publication or control
result after commit, including the exact canonical snapshot, snapshot ID,
revision vector, cursor/digest, incarnation, and HubSeq. Publication intent can
be exported after `Apply` and before `Commit`; that draft is never acceptance.
Accepted delivery is at-least-once: the service deduplicates by asset,
publication key (source, epoch, generation), sequence and digest. A replayed
control acceptance retains its original incarnation, HubSeq and exact result;
an exact replay of an accepted publication repeats its stored acceptance with
the original HubSeq, so an interrupted delivery is repeated; a new event
position does not identify a new accepted batch.
An accepted callback cannot roll back a committed batch: if it is interrupted,
the batch stays accepted, the call reports the interruption, and an exact
publication replay or a `RetryControl` delivers the stored acceptance again with
its original HubSeq. Callbacks run outside
the hub, source and asset locks while the exact pending attempt remains reserved;
they must not reenter a hub mutation. Storage failures and uncertain response windows remain
the service's WAL obligations. A `BeforeControl` refusal is a terminal
`Rejected` outcome with error ID `semhub.control_export_refused`; `RetryControl`
does not retry that refusal. After repairing storage, the service must
`AbandonControl` and issue a new fence or retirement request. The rejected
attempt's exact bytes remain historical; the new operation builds its own
attempt from the current snapshot. No refused attempt advances a cursor.

`ExportRecovery` captures a coherent detached record of declaration identities,
pack references, every accepted batch in per-asset commit order, its original
monotonic point and exact result, the last control gate/attempts, epoch
history, and each historically admitted source's identity regime (hub-native
or its legacy family) with the driver that owns the source. A source whose
every admission was withdrawn is listed as unfixed instead, so every source
with epoch history carries exactly one of a regime or an unfixed marker.
Copying coherent metadata uses the existing hub
synchronization; serialization, buffer export, validators, and persistence run
outside it.
Rejected attempts remain control history and never enter the accepted replay
log or advance a restored cursor. Enabled recovery retains the complete log;
the service must bound admission and retain replay material until a verified
restore point permits compaction. Default production hubs do not collect it.

Restore requires the same declared asset/source/driver identity and compatible
validators, empty asset kernels, and no prior admission of an affected source
in the new incarnation, including withdrawn admissions. Every declared recovery
asset must be present, including assets of sources omitted entirely from the
checkpoint; missing coverage rejects the record and closes all empty declared
assets. Checkpoint entries with undeclared asset IDs also reject the record and
close all empty declared assets. Malformed epoch identities, zero generations, non-lifecycle
event kinds, invalid or unordered asset IDs, missing or invalid source
identity regimes (including a source with history that has neither a regime
nor an unfixed marker), unfixed markers without history or with an admitted
epoch that was never retired, and missing,
unknown or conflicting source driver ownership reject the checkpoint before
replay or epoch reservation. A control status that reports an attempt the
accepted log already holds as pending, rejected or uncertain also rejects it.
Restored cursor and history epoch IDs are reserved against admission and epoch
transitions, even for assetless sources or with a different generation:
restoration requires a new epoch, and the generation sequence restarts with
that epoch (as in LIFE-06); generation current+1 applies to legacy successors
on the retained kernel within one incarnation, and a hub-native successor
starts at generation 1 (section 7, hub-native epoch succession). Successor admission must retain the
restored identity regime; changing legacy family or switching between legacy
and hub-native identity is refused. An unfixed source admits any regime, as
after a withdrawn first admission. Restored sources stay claimed by their
owning driver, including sources without assets; another driver's admission
or declaration is a collision. A source restores once per incarnation: a
second restore of the same source, or an admission that lands before the
restored state is installed, refuses the checkpoint. `ExportRecovery` during a restore returns the zero record (version 0), which no
restore accepts, so a partially installed hub never becomes a checkpoint.
Restore and admission never
interleave: from the coverage scan to the end of installation, declarations and
admissions are refused with a restore-in-progress error and retried by the
caller afterwards. Ownership, regimes and historical epochs are installed in
one critical section, and an admission that commits afterwards rechecks the
restored regime. A withdrawn epoch may be admitted again; the later admission
is the current one. A regime requires an
admission in the history or accepted publications, and an unfixed marker
requires an admission in the history.
It replays accepted publication and control batches on detached kernels using
the persisted monotonic points, verifies every exact result, and reinstates retained
history/tombstones and per-key cursors. A corrupt step or changed validator
behavior installs no partial asset state and leaves that asset closed;
independent valid assets may restore but remain closed too until each fresh
family reprojection acknowledges its restored snapshot through `Resettle`.
A missing callback cannot acknowledge restoration. The current prerequisite
uses a synthetic reprojection gate for publication-only and empty entries:
`Resettle` reports `Kind=generation_fenced` with a zero `ControlID` when no
historical control exists. For a never-published entry that callback view has
`Present=false` and an empty `SnapshotID`; it acknowledges rebuilding an empty
cache. The callback preserves the absence reported by `Inspect`. These callback
markers confer no control authority.
Historical settlement grants no current read acknowledgement. Outstanding
semantic gates remain closed; the future service must resolve their original WAL work before new admission.
Prior admissions, activations, revocations, and retirements remain inert
history and never create live handles, native-close proof, or a current epoch.
No historical control operation is revived as a current-incarnation retry.
The active epoch journal remains in memory and is not replayed by restoration.
Full service loss/crash-at-acceptance integration (S3.9) remains subsequent
work; this prerequisite does not implement the lifecycle service.

For each recorded asset, the manager reads the exact immutable current
`Snapshot`, verifies that its `SnapshotID` and semantic revision match the
materialized input, and enumerates every non-retired `SourceDescriptor` for the
owned `SourceID`. It then submits one control-only `PublicationBatch` with the
new epoch and reserved generation, that asset's next sequence from its own
kernel `PublicationCursor` for
`(SourceID, SourceEpochID, DriverGeneration)`, the exact expected semantic
revision, a current inert `SourceDescriptor` in
`SourceUpserts`, and the complete old-epoch set in `SourceRetirements`. Binding,
identity, fact, service, capability, and generation-fence upsert arrays are
empty. The descriptor uses the persisted configured profile/version and
registry evidence; it asserts source ownership only. It does not assert a
provider observation, binding, activation, capability, or observed `RUNNING`.
Only the gateway manager may construct this batch for its owned source.

Only one asset batch is in flight. Publication sequences are per asset: the
SemReg publication kernel keeps one cursor per asset for
`(SourceID, SourceEpochID, DriverGeneration)` and requires 1 for an unseen
key, so each asset's barrier batch uses that asset's next sequence (1 for the
new generation). After acceptance, the manager durably records its result and
advances that asset's entry in `Reservation.NextPublicationSequence` before it
submits the next bytewise-sorted asset. A rejected revision-conflict batch did
not advance the asset cursor, so its rebuilt batch retains that unaccepted next
sequence. The manager never uses generation 0 and never submits a later asset
before persisting the prior accepted result. Later asset entries keep internal
`Sequence=0` until assigned; zero is never sent on the semantic wire. An empty
affected-asset set completes the barrier with an empty per-asset cursor map.
Order across assets is the manager's commit order only; no sequence orders
publications of different assets.

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

For the sole possibly submitted-but-unrecorded batch, it reads that asset's exact
kernel `PublicationCursor`: an exact last-sequence/digest match permits
idempotent replay and accepted-snapshot readback, an unchanged cursor permits
resubmission, and any incompatible cursor blocks the transition as
`sequence_conflict`.

After the last asset accepts, the manager atomically changes the same durable
reservation to `AVAILABLE/NONE`; it does not allocate another generation. The
reservation retains the exact per-asset next cursors: each barrier asset's
`PublicationCursor.LastSequence+1`, and 1 for every asset the barrier did not
touch. A driver
left desired `STOPPED` therefore retains one inert available reservation. A
later accepted lifecycle operation claims that exact key and per-asset cursor map by changing
it to `PROVIDER_CLAIMED/LIFECYCLE` and storing the operation ID in the same
transaction. A repeated reconciler wake may only recover that claim for the
same operation; another operation cannot claim it, increment its generation, or
reset any asset cursor.

A gateway process restart allocates a new source epoch as required above. Its
new transition supersedes, rather than resumes, an incomplete prior-epoch
record. The new record takes the union of that record's assets, all pending
publication assets, every incomplete semantic-cleanup asset, and the durable
index; rematerializes every current snapshot and revision; and starts its new
epoch/generation-1 reservation with every asset cursor at sequence 1. This includes assets already
transitioned to the prior inert epoch, which the new batch now retires. The
prior reservation becomes `RETIRED` and can never be claimed in the new epoch.
After the last asset accepts, the manager exposes the new reservation as
`AVAILABLE` with each asset's `PublicationCursor.LastSequence+1` and passes that
exact per-asset cursor map when it is claimed for activation. `Prepare`/`Activate` failure does not undo a
completed retirement barrier or abandon incomplete retirement work. A requeued
STOP/RESTART withdrawal retains its old native-close proof or quarantine; its
old semantic work becomes `SUPERSEDED` only after every carried asset retires in
the new barrier. A requeued RESTART cannot claim the new reservation until that
barrier completes and the old native close is proven.

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
    NextPublicationSequence map[AssetID]uint64 // per asset; absent = 1
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
    Correlation       LifecycleCorrelation
    RetiringGeneration GenerationKey
    Reservation       GenerationReservation
    Assets             []SemanticCleanupAsset
    State              SemanticCleanupState
    RetryCount         uint32
    RetryLimit         uint32
    DeadlineUTC        time.Time
    NextAttemptAtUTC   *time.Time
    LastError          *semv1.ErrorID
}

type GenerationWithdrawalState string
const (
    GenerationWithdrawalPending    GenerationWithdrawalState = "PENDING"
    GenerationWithdrawalActive     GenerationWithdrawalState = "ACTIVE"
    GenerationWithdrawalComplete   GenerationWithdrawalState = "COMPLETE"
    GenerationWithdrawalBlocked    GenerationWithdrawalState = "BLOCKED"
    GenerationWithdrawalSuperseded GenerationWithdrawalState = "SUPERSEDED"
)

type GenerationWithdrawalRecord struct {
    Correlation          LifecycleCorrelation
    Fence                *FenceProof
    SemanticCleanup      *SemanticCleanupWork
    SuccessorReservation *GenerationReservation
    NativeClose          NativeCloseWork
    State                GenerationWithdrawalState
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
    Generation GenerationKey
    // FirstSequence is per asset: the claimed next sequence of each asset at
    // promotion start (an absent asset starts at 1). Each batch record's
    // Sequence equals its asset's entry; an empty set still records the map.
    FirstSequence map[semv1.AssetID]uint64
    Batches       []ActivationBatchRecord
    TotalBytes    uint64
    DeadlineUTC   time.Time
    State         ActivationPromotionState
    Withdrawal    *GenerationWithdrawalRecord
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

Every reservation has a non-zero generation and a per-asset next-sequence map
(an absent asset starts at 1). Its key must
match the containing driver/source epoch. `AVAILABLE` and `RETIRED` require
owner `NONE`; `PROVIDER_CLAIMED` and provider-created `CONSUMED` require owner
`LIFECYCLE` plus exactly one operation ID; epoch and cleanup control records
require their matching owner, and cleanup/recovery records identify exactly one
predecessor generation. A state/owner/key mismatch is corrupt persistent state: it
closes admission and requires bounded reconciliation, never a guessed counter or
cursor.

A `Prepare` failure may release `PROVIDER_CLAIMED` back to `AVAILABLE` at the
unchanged cursor only when the native owner proves that no prepared/native
resource remains, no publication was staged, and `Activate` was never entered;
the release and attempt result persist in one transaction. An uncertain result
or surviving prepared object retains the claim for the same lifecycle operation
until bounded recovery obtains an abort/close proof; it never allocates a
replacement key merely because the wake repeats.
Only an explicit live replacement without an intervening cleanup reservation
allocates and claims one strictly newer generation with every asset cursor at
sequence 1. A normal STOP
or close-before-Prepare RESTART instead creates its successor through the common
withdrawal/cleanup record and later claims that exact reservation/cursor. Each
asset's first accepted higher-generation batch must atomically fence every older
unfenced generation. Allocation is never inferred from observed state or from a
process-local counter.

The reservation/lifecycle transition matrix is closed:

| Current durable state | START | STOP | RESTART / recovery |
|---|---|---|---|
| Epoch barrier `CONTROL_OWNED` | Wait for the same barrier; no allocation or provider call | Desired-stopped reconciliation completes the barrier and leaves its reservation available | A queued operation resumes only after the same barrier completes |
| `AVAILABLE/NONE` | Claim once for the operation at the stored cursor | If desired is already stopped, return `NO_OP`; do not retire the reservation | An in-progress close-before-Prepare restart or eligible lifecycle operation claims this exact successor |
| `PROVIDER_CLAIMED/LIFECYCLE` | Only the owning operation may resume Prepare; another request cannot claim | Supersession aborts the exact prepared state, then releases only with clean proof or quarantines | Repeated wake/component recovery uses the same operation, key, cursor, and correlation |
| `CONSUMED` and live | Already desired running is `NO_OP` | Create one generation+1 withdrawal reservation and the two durable obligations | Close-before-Prepare creates the same withdrawal; an explicit proven-overlap replacement allocates once under its native seam |
| Old `CONSUMED` plus withdrawal `PENDING`, `ACTIVE`, or `BLOCKED` | Resume the same obligations; a newly accepted Start may renew only the finite semantic budget, and cannot claim until completion | Resume the same semantic and native-close obligations; a no-op does not assert cleanup | Resume the same obligations; a later accepted Restart may renew only semantic budget, with no Prepare until completion |
| Withdrawal `COMPLETE`, successor `AVAILABLE` | Claim that exact newer generation/cursor once | Completed STOP remains stopped; no additional generation | The already accepted RESTART continues by claiming that exact successor |
| Successor `CONTROL_OWNED/RECOVERY` | Reject/wait until native close proof makes it available | Resume the one close obligation | Repeated recovery reuses the same successor; no second allocation |
| `RETIRED` or prior source epoch | Never claim | No current native authority | New process work uses only the new epoch's generation domain |

A component restart reloads the same row and operation record. A process restart
retires every old-epoch row and creates only the new epoch's barrier reservation.
No transition derives authority from observed state or from a missing journal
entry.

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
An `Activate` error may return a non-nil `Generation`; that exact object uses the
generation-object fence/stop branch. A nil generation uses only the existing
prepared object's `Abort` branch. The manager never assumes a generation exists
from reservation state or fabricates `FenceProof`/`CloseProof` when either
object is unavailable.
Adapters may implement this split as a thin wrapper: for example, an existing
native `Start` that both constructs and activates runs inside `Activate`, while
the gateway admission record remains closed.

`StartContext.Publish` is never the live semantic sink during preparation or
activation. The manager creates one `ActivationPublicationController` for the
reserved generation and passes only its publisher view. The preceding epoch
transition may already have consumed publication sequences, so
`NextPublicationSequence` is the exact per-asset map persisted by the manager.

While `Activate` runs, the publisher stages an ordered set of zero through 128
canonical envelopes totaling at most 8 MiB, all before `StartContext.Deadline`.
Each distinct batch's sequence equals `NextPublicationSequence[Batch.AssetID]`
(1 when the asset is absent), and every envelope names the reserved source,
epoch, and generation. Because at most one distinct staged batch may name an
asset, no two staged batches share an asset cursor. Each batch also supplies its exact `InputSnapshot`, whose asset and
semantic revision match `Batch.AssetID` and
`Batch.ExpectedSemanticRevision`. Replay and conflict are classified by the
asset-scoped publication tuple (asset, source, epoch, generation, sequence,
digest): an identical replay of a staged tuple is a no-op and does not count
twice. Another digest at a staged (asset, source, epoch, generation, sequence),
a regression or gap of that asset's sequence, another generation, a count/byte
overflow, or arrival after the deadline poisons staging and fails activation
closed.

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
staging order; that order is the only order across assets. Each batch is kernel-atomic; the set is not a cross-asset
transaction. Before each submission the manager verifies the complete current
snapshot ID and expected revision still match the staged input. It persists the
accepted snapshot and updated cursor before submitting the next batch. The
controller extends the source-scoped visibility fence to the reserved
generation before the first submission, and admission remains closed through
the whole set, so no accepted prefix becomes public or actionable. A callback
for any asset's following sequence waits at this boundary and cannot overtake
the staged set.

After the final staged batch is accepted, the manager persists each asset's
next cursor, switches the same publisher object to live forwarding, releases waiting
callbacks, and only then removes the source visibility fence and opens
admission. If the set is empty, the first live publication of each asset must still use
that asset's `NextPublicationSequence` entry. Recovery in `COMMITTING` uses the same one-uncertain-
batch rule on that batch's asset cursor and digest as the epoch barrier; accepted
earlier batches are not replayed. A waiting callback for an asset already changed by the staged set is
revalidated after the drain; a stale `InputSnapshot` or expected revision returns
`revision_conflict` without advancing that asset's cursor, and the adapter must
refresh and rebuild at the same still-next sequence of that asset. It is never blindly forwarded with
the pre-promotion revision.

Every consumed-generation retirement uses one durable
`GenerationWithdrawalRecord`; activation failure and ordinary STOP/RESTART do
not define separate cleanup protocols. When a `Generation` object exists,
the manager first persists closed admission plus a `PENDING` record containing
the exact lifecycle correlation, original finite native-close deadline, and any
materialized semantic-cleanup work. Under the admission/callback serialization
boundary it then invokes that generation's bounded, idempotent
`FenceWithdrawal`, validates that the returned proof generation equals
`Correlation.Generation`, and persists the proof plus state `ACTIVE` before
either external obligation starts. Recovery of `PENDING` repeats only that same
keyed fence call. A mismatch blocks semantic cleanup and quarantines the source;
it never supplies invented proof to `Stop`.

A consumed `STAGING` activation that has not returned an installed `Generation`
cannot call `Generation.FenceWithdrawal` or `Generation.Stop`. The manager
instead durably aborts the exact activation publisher/controller key, rejects
all its callbacks, records `Fence=nil` and `SemanticCleanup=nil`, and calls only
the retained `PreparedGeneration.Abort`. If component recovery cannot recover
that prepared object, native close is `UNCONFIRMED` and the source plus successor
remain quarantined; no close proof is invented. These two SPI shapes share the
same withdrawal record and terminal rules but never call an unavailable method.
For installed generations, callback rejection is effective at the exact fence
boundary and source-scoped semantic visibility remains until cleanup completes.

The record has exactly one successor location. When semantic records may be
current, `SemanticCleanup` is non-nil, its reservation names the next generation
and exact predecessor, its correlation/retiring generation equal the parent
record, and `SuccessorReservation` is nil. When no provider batch
committed, `SemanticCleanup` is nil and `SuccessorReservation` holds the next
generation for recovery. `NativeClose` is always present. `COMPLETE` requires
every required semantic asset accepted and `CloseProven`. It also requires an
exact non-nil fence whenever a generation object or semantic cleanup exists;
the sole nil-fence case is a never-installed, controller-aborted activation with
no committed provider batch. `BLOCKED` retains all obligations and cannot
authorize preparation or admission.

If promotion fails before any staged batch commits, that record has no
`SemanticCleanup` because no provider record became current. It contains one
strictly newer `SuccessorReservation` with every asset cursor at 1 in
`CONTROL_OWNED/RECOVERY` and `NativeCloseWork{State:PENDING}`. The failed
consumed generation is never reused. The manager durably applies
`ActivationPublicationController.Abort` to the exact key, then schedules the
retained `PreparedGeneration.Abort` immediately on the independent close context.
Proven close atomically completes withdrawal,
records `ActivationFailed`, and makes the successor `AVAILABLE/NONE`; its first
provider batch must fence the failed generation even though that generation
published no provider records. Unconfirmed close blocks withdrawal and
activation, keeps the successor held, and quarantines the source. The staged set
is discarded; the existing inert descriptor and immutable history remain.

If a prefix commits, the controller permanently rejects generation callbacks
and persists a withdrawal record whose `SemanticCleanup` owns a
`CONTROL_OWNED/SEMANTIC_CLEANUP` reservation for `generation+1` with every
asset cursor at sequence 1,
an independent finite deadline and retry budget, and its complete bytewise
`AssetID`-ordered work set. `SuccessorReservation` is nil because the cleanup
reservation is the successor. Semantic cleanup and native close are two durable
obligations; neither completion is inferred from the other.

Recovery of a `CONSUMED/STAGING` record with no terminal activation result treats
native activation as possible and never calls `Activate` again. Under the same
record transaction it creates the withdrawal and recovery successor only when
absent, closes callbacks, and schedules owner-specific abort/close
reconciliation for the original lifecycle correlation. Repeated wakes and
component recovery reuse that same withdrawal, successor, and close work. Only
the retained prepared object may receive `Abort`; if it is absent, recovery
records no synthetic close. Only proven close makes the successor available;
uncertainty keeps it held and quarantines the source.

For activation-prefix cleanup, the asset set is materialized once from the
durable union of epoch-transition and accepted activation-prefix assets whose
accepted snapshots contain the retiring generation's cursor, binding, records,
or direct/transitive
derived closure. It remains the complete work set even after some assets accept.
`Assets` contains unique bytewise-sorted IDs; each asset's attempts have
consecutive `AttemptNumber` values in creation order, and `Complete` is true only
when its final attempt is durably `ACCEPTED` with a snapshot and semantic
revision.
Staged or pending provider batches that never committed are discarded and never
fabricated as cleanup state. In bytewise asset order, manager-owned cleanup
batches use the cleanup reservation's per-asset cursors (each asset's next
sequence for the cleanup generation key, 1 when unseen) and include the accepted
kernel `GenerationFence` for the retiring generation plus the required withdrawal
cascade. They contain no activated binding, observation, service, capability, or
fact upsert. Each asset cleanup is separately kernel-atomic, and the
source-scoped visibility fence keeps the failed prefix non-public throughout.

For each asset attempt the manager reads one complete immutable snapshot,
derives the exact withdrawal set, allocates only that asset's current cursor
sequence from the reservation's per-asset map, and persists a `SemanticCleanupAttempt` in `PREPARED` before
submission. That write-ahead record contains the exact input snapshot and
semantic revision, sequence, batch ID, digest, complete canonical cleanup
envelope and byte length. Immediately before submission it persists
`SUBMITTED_UNKNOWN`. A synchronous kernel rejection that proves no mutation is
persisted as `REJECTED_UNAPPLIED`; an accepted result and accepted snapshot are
persisted with the resulting semantic revision before that asset's reservation
cursor advances or the next asset begins. There is at most one uncertain cleanup
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
an unconfirmed close sets the withdrawal `BLOCKED`, quarantines the source epoch,
and prohibits replacement; an activation-failure parent is also `BLOCKED`. A
proven close does not release the semantic visibility fence or erase unfinished
cleanup.

Component recovery loads both obligations without resetting either budget. For
the sole `SUBMITTED_UNKNOWN` cleanup attempt it reads the attempt asset's exact
`PublicationCursor` for the cleanup generation. An exact sequence/digest match triggers
accepted-snapshot readback and records `ACCEPTED`; an unchanged predecessor
cursor resubmits the stored canonical envelope byte-for-byte; an incompatible
cursor blocks with `sequence_conflict`. A `PREPARED` attempt is submitted from
its stored bytes. Accepted earlier assets and proven rejected attempt bytes are
not replayed. Native `PENDING`/`ACTIVE` close work is rescheduled separately
within its original independent deadline.

Semantic retry exhaustion marks the withdrawal `BLOCKED` and the current
lifecycle attempt terminally failed. A later accepted eligible START or RESTART
may supply a new finite semantic retry budget against the same ordered work,
cursor, fence, and successor. It cannot recreate the reservation, discard attempt
history, reopen admission, repeat native close after proof, or reset the original
native-close deadline.

After every cleanup asset accepts, the manager persists the cleanup state
`COMPLETE`, changes that same reservation to `AVAILABLE/NONE` at its exact
per-asset next cursors, and retains the generation inert. The next provider must claim this
reservation through the ordinary start procedure; it cannot allocate another
generation or reset any asset cursor to 1. No publication returns to the fenced
generation. The withdrawal becomes `COMPLETE` only when semantic cleanup is
complete and native close is proven; until then an available reservation cannot
be claimed. Activation failure then records its parent promotion `FAILED`; an
ordinary STOP may record `STOPPED`, and RESTART may proceed to preparation.

Exhausted cleanup and its withdrawal remain `BLOCKED` with every incomplete asset
and attempt durable, source admission and affected-path visibility closed, and
independent native close still completed or quarantined; an activation-failure
parent is also `BLOCKED`. Unrelated sources stay visible. A
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
the manager consumes it within one configured lifecycle budget. A driver policy
may add a persistent phase after that budget: while the driver is desired
`RUNNING`, a retryable failure series then continues with capped exponential
backoff instead of ending `FAILED`/`RETRY_EXHAUSTED`. Only `ebus.primary`
enables it (described under the shell integration below); every other driver
keeps the bounded budget. Modbus reconnect and request retry remain inside the
current Modbus owner and its one total operation context.

## 7. Lifecycle ordering

### Start and activation

1. Complete or resume the current source epoch's control-owned transition
   barrier for every recorded affected asset. No provider method runs while the
   barrier is incomplete.
2. In one durable lifecycle transaction, claim the epoch barrier's or completed
   withdrawal/cleanup's `AVAILABLE` reservation with its existing generation and
   per-asset next cursors. This includes a START after successful STOP and a close-before-
   Prepare RESTART. Only an explicit live replacement with no successor may
   atomically allocate and claim one strictly newer generation with every asset
   cursor at sequence 1. A consumed, held, or retired reservation is never reused.
   Install manager admission closed for the claimed key.
3. Create the activation publisher with the claim's exact per-asset next
   sequences, then
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
   its activation publisher. The bounded staged set commits in staging order
   (sequences are per asset) before any waiting later publication can forward. The first provider
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

1. Under the admission/callback serialization boundary, close admission for the
   exact running generation, publish gateway observed `STOPPING` (or
   `BACKOFF`/`FAILED` for a failure path), and attach one `PENDING`
   `GenerationWithdrawalRecord` to the lifecycle operation. It binds the exact
   lifecycle correlation, creates `SemanticCleanupWork` for the complete
   generation-owned asset set, reserves exactly one strictly newer cleanup
   generation at cursor 1, and creates `NativeCloseWork{State:PENDING}` with the
   native owner's original finite close deadline.
2. Invoke the exact generation's idempotent `FenceWithdrawal`, validate its
   correlation, and persist the `FenceProof` plus withdrawal state `ACTIVE`.
   Callback rejection and source-path visibility withholding precede any
   external cleanup or close call; component recovery of a PENDING record repeats
   only this same keyed fence boundary.
3. Start both obligations independently. Semantic attempts use the common
   write-ahead cleanup journal and separately atomic per-asset generation-fence /
   withdrawal batches. Native cancellation, lease drain, and `Stop` run outside
   the manager lock on the stored close context even when semantic publication
   rejects, times out, or has an uncertain result.
4. Persist semantic attempts/results and `CloseProof` independently. Component
   recovery reloads the same operation, fence correlation, exact uncertain
   envelope/cursor, remaining semantic budget, and original native-close
   deadline; a repeated wake neither resubmits an accepted asset nor starts a
   second native retry owner.
5. Complete the withdrawal only when semantic cleanup is complete and native
   close is proven. A STOP then publishes observed `STOPPED`. Semantic exhaustion
   retains exact cleanup work and the source visibility fence; close still runs.
   Timeout or unconfirmed close publishes `FAILED/CLOSE_UNCONFIRMED`, retains any
   semantic work, and quarantines the source epoch.
6. A close-before-Prepare RESTART claims the completed cleanup reservation at its
   persisted next cursor only after both obligations complete. Under the A2
   lifecycle amendment the composed service instead retires the old epoch and
   admits one new hub-native epoch through the succession seam below; the
   same-epoch text of this item remains the rule only until that service lands. It never allocates
   another generation, reuses the retired generation, or accepts an old callback.
   The sole exception is a native replacement seam that itself proves
   non-overlap, fencing, drain, and ownership transfer; it still persists one
   exact withdrawal/reservation transition and satisfies the same semantic
   supersession and cursor rules.

### Hub-native epoch succession

`Hub.AdmitSuccessor(Succession{Previous, Operation})` is the publication-free
hub seam for the A2 lifecycle amendment
([docs-ebus runtime driver lifecycle, A2](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)).
`Previous` is a handle of the predecessor admission and `Operation` is the
opaque correlation of the accepted lifecycle operation (an explicit RESTART or
a START after a completed STOP). The operation needs no asset declaration and
submits no SemReg batch; a lifecycle-only source with no assets succeeds the
same way. The implemented rules are:

1. Admission rules. The source must be hub-native: a legacy source, or a source
   whose restored regime is legacy, is refused with `ErrAdmission` and keeps its
   legacy successor path (`AdmitSource` with generation current+1, or
   `BeginEpoch`). `Previous` must belong to this hub incarnation and name the
   source's current admission identity and epoch; a handle of another
   incarnation or of a withdrawn admission is refused with SemReg's
   `StaleSourceEpoch`, and a handle of an already succeeded admission resolves
   only under rule 4. The current epoch must be retired by
   `RetireEpoch` (a fence alone is refused), and the latest control operation
   of the source, including its reprojection, must be finished: an unfinished
   operation returns `ControlPendingError`, a settling one `SettlePendingError`
   after one reprojection pass, and an abandoned operation with an open asset
   `ErrControlAbandoned`. Native close proof and the durable intent/outcome
   protocol remain service obligations; the hub cannot observe them.
2. Identity. The successor epoch derives from (hub incarnation, `SourceID`,
   admission ordinal). The ordinal is per source and per incarnation, starts at
   1 for the first succession and advances by exactly one per successful
   succession; the source's first admission in an incarnation keeps its
   existing (incarnation, `SourceID`) derivation. The successor starts at driver
   generation 1, is not the first admission of the source, receives a new
   admission identity, and every asset's cursor for the new key starts at
   sequence 1. Old cursors, tombstones and retained history stay unchanged. A
   derived epoch that repeats a current, retired or restored epoch is refused.
3. One write-ahead operation. The retired predecessor (its epoch and last
   generation, covering no asset because its asset retirements were journalled
   by the retirement control) and the successor admission (epoch, generation 1)
   are appended as one all-or-none journal operation. Both records carry the
   ordinal in `EpochRecord.Ordinal`, which is zero on every other record. A
   failed append exposes no handle, consumes no ordinal, records no claim and
   emits no event. A successful succession emits one `epoch_admitted` event.
4. One successor per predecessor. The successor is recorded against the
   predecessor's admission identity together with `Operation`. A repeated or
   concurrent claim with the same operation returns that same successor handle
   (stale once the successor itself has moved on); a claim with another
   operation is refused with `ErrAdmission`. No duplicate allocates another
   ordinal, epoch or admission.
5. Counter exhaustion. When the ordinal, the admission identity or the handle
   counter cannot advance without wrapping, the succession fails with
   `ErrAdmission` and `ErrCounterExhausted` and changes nothing.
6. Afterwards. The predecessor handle and every older handle remain rejected
   for publication, `Activate` and `AdmitSuccessor`. A manager retry inside the
   successor epoch uses the existing `Fence`/`Activate` path and keeps the
   epoch. `Withdraw` refuses a successor admission: the succession is
   journalled, so a successor that fails to start is retired and succeeded
   again under the next ordinal. `Withdraw` of a first admission and the legacy
   `AdmitSource`, `BeginEpoch` and `Withdraw` paths are unchanged.
7. Recovery. `ExportRecovery` carries both succession records with their
   ordinal in the epoch history, and `Restore` reinstates them verbatim as
   inert history; every named epoch stays reserved and the restored regime stays
   hub-native. No restored succession, handle or ordinal is current: a
   prior-incarnation handle cannot claim a successor, the new incarnation's
   first admission derives its own epoch, and its ordinal starts again at 1 under
   the new incarnation identity. `Restore` rejects ordinals on unrelated events,
   malformed or nonadjacent retirement/admission pairs, and ordinals that do not
   start at 1 and advance by one per source within each incarnation. Succession
   pairs must retain a retired, previously admitted predecessor and the derived
   successor identity at generation 1, with no assets on either paired record.

For an ordinary promoted generation, the cleanup asset set is the bytewise union
of every asset in its durable publication index and every pending/uncertain
publication entry. Each cleanup batch uses the new reservation and includes the
required fence for the old generation plus its complete automatic withdrawal and
derived cascade. The common journal's revision-conflict, exact replay/readback,
deadline, retry, source-isolation, process-retirement, and independent-close
rules apply unchanged. No whole-asset visibility suppression or cross-asset
atomic transaction is introduced; healthy unrelated source paths remain visible.

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

### S4 manager-to-hub bridge (implemented callable composition)

This subsection records the implemented S4/A3/A4 bridge of the
[docs-ebus runtime driver lifecycle](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
for the lifecycle-only `ebus.primary` driver. The next subsection records
how process startup installs it.

DriverManager hooks (`internal/drivermanager`):

1. `LifecycleHooks.HoldAdmission` runs after a provider generation was
   constructed and before `RUNNING` publishes. Observed state stays
   `STARTING` with no effective capability, so `Invoke` refuses the
   generation. The hook runs with every manager lock released and may do hub
   work. On a nil result the manager rechecks that its operation is still
   current, runs the bounded pre-admission activation (the existing B503
   observer activation) and publishes `RUNNING` with the effective
   capabilities in one state transition. A superseded operation is retired as
   a late admission. A hook error retires the never-published generation on
   the independent late-retirement deadline. An unproven close quarantines
   the driver (`FAILED/CLOSE_UNCONFIRMED`). A proven close fails the attempt
   through the driver's ordinary classification, so any retry stays
   manager-owned and constructs afresh. `AdmissionHold` names the native
   generation, the manager operation, the previous admitted generation and
   whether the attempt is a manager-owned retry or a replacement.
2. `StopWithReport` performs the same transition as `Stop` and returns a
   `CloseReport` with a `CloseClass`. `PROVEN` (nil) and
   `PROVEN_AFTER_DRAIN_TIMEOUT` (drain expired, close confirmed) are close
   proof, and so is `NOT_REQUIRED` (nothing was open). `UNCONFIRMED`
   (quarantine) and `INTERNAL_ERROR` are not. Runtime quarantine dominates the
   class even when `Stop` returned nil. The observed transition keeps its
   characterized precedence, in which a nil result reads `STOPPED`; changing
   that observed precedence is left to the owner of the drivermanager
   characterization. A stopped driver with a construction still in flight
   reports `INTERNAL_ERROR`, because a late admission may still follow.
   `LifecycleHooks.Closed` receives the report of every explicit stop, every
   superseded late admission and every failed hold after the manager locks
   are released.
3. `BindHooks` binds the hooks of one driver once. A lifecycle bridge can
   therefore attach to a manager that was composed elsewhere. `Watch` returns
   a snapshot and a channel that closes at the next observed transition.

The `ebus.primary` bridge (`cmd/gateway`, `ebusControlRuntime`) implements the
service's `LifecycleExecutor`:

1. `Prepare` admits the configured source's inert first hub epoch at
   generation 1 before any endpoint is constructed, including when desired
   state is `STOPPED`. It declares no asset, so the epoch barrier is empty and
   complete. The source identity and the native contract reference are
   supplied by the caller and are required. No placeholder is installed and
   no identity is derived from an endpoint.
2. Correlation. Each service correlation becomes a full key: `DriverID`,
   `SourceID`, source epoch, driver generation and the operation's lifecycle
   sequence. The service correlation carries no `SourceID` yet, so the bridge
   binds the one configured source. A different driver or source is refused.
   The first START of the incarnation claims the inert epoch. A new epoch
   after a fenced predecessor requires proven native close and the
   predecessor's hub retirement, and then admits exactly one hub-native
   successor through `AdmitSuccessor`. The successor carries an opaque
   operation identity derived from the key, so repeated or concurrent calls
   for one operation resolve to that one successor. A repeated `Start` of the
   same key joins the start in flight or returns its recorded result. It
   never calls DriverManager again, so no native budget is renewed. A late or
   stale key (another epoch, generation or sequence) and a key whose STOP
   arrived before it bound are refused before any hub or native work.
3. Admission. The bridge calls `StartAsync` once per accepted operation and
   reports success only when DriverManager published `RUNNING` for a native
   generation that the hold mapped to this epoch. On the first admission of
   an epoch, the hold checks that the claimed hub generation is live; a
   mismatch fails closed (LIFE-10). On a manager-driven retry or failure
   replacement, the hold keeps the epoch, fences the old driver generation
   and activates the next one through `Fence` and `Activate`, outside manager
   locks and with admission still held. The explicit
   `{key, native generation}` mapping is recorded before admission. Driver
   generation restarts at 1 in every epoch, while native generation is
   process monotonic. Mappings are process-local and historical after process
   loss. Executor errors are closed and non-retryable (`ReasonCode` only), so
   the service never adds a second native retry loop. A failed start fences
   the epoch and requests the manager-owned close on the bridge's independent
   cleanup deadline before it returns, so nothing stays open.
4. Stop. `Stop` fences the epoch at once, which immediately rejects late
   construction, health and publication. It then runs two independent
   obligations. The native one is one manager-owned `StopWithReport`, which
   repeated calls join and which revokes leases, B503 issuance and proxy
   readiness at the manager's existing withdrawal boundary. The semantic one
   is hub `RetireEpoch` of the asset-free epoch, run once the withdrawal
   fence is visible. Neither obligation waits for the other's success.
   `NativeClosed` reports only a proven close class. `UNCONFIRMED`
   quarantines the bridge and blocks every successor whatever the epoch.
   `INTERNAL_ERROR` is retried by the next `Stop`. A retirement failure keeps
   the epoch unretired, which blocks succession, and the next `Stop` or
   succession retries it.
5. Views and gates. `View`, `Admitted`, `PublicationHandle` and
   `ReportFailure` read one manager snapshot, so admission and observed
   `RUNNING` are visible together. Semantic capabilities are always empty.
   Native capabilities are the manager's effective invocation gates, reported
   only while the exact key is admitted. The lifecycle-only source publishes
   nothing. The publication gate exists so that a stale key never obtains a
   live handle. The hub seam has no declaration, transaction or withdrawal
   method.

LIFE applicability for this asset-free adapter: LIFE-01–LIFE-10 and the A2
rows LIFE-05, LIFE-25, LIFE-26, LIFE-31 and LIFE-32 have named fake-endpoint
tests. The bridge tests and the J10 service tests together cover LIFE-02.
The rows that need semantic assets, staged activation batches, per-asset
cursors or shared assets (LIFE-14–LIFE-24 and LIFE-27–LIFE-30, LIFE-22
included) do not apply to an asset-free source. They stay with the first
family migration that declares assets. Executor-originated observations
reach the service through `Service.ObserveNative`, described with the shell
integration below, so a manager-driven `BACKOFF`, retry or terminal `FAILED`
after `RUNNING` is visible in the service view as well as the bridge view.

### S4/S6/S7 shell integration (implemented production composition)

This subsection records how `runGatewayLifecycle` installs the bridge, the
desired-state service and the generation task runner. It implements the
"Shell and preparation order", "STOP and generation task ownership", S6 and
S7 sections of the docs-ebus runtime driver lifecycle for `ebus.primary`.

Composition order (before any eBUS endpoint, before the HTTP shell):

1. The configured source of `ebus.primary` is derived from its `DriverID`
   and the configured owner identity, the persisted instance GUID, as a
   hub-native source identifier. It never uses an endpoint, address,
   connection handle or discovery result. The native contract reference
   names the public eBUS transport module, its `transport.DriverRuntime`
   contract and the exact module version linked into the binary. When the
   build records no exact version (a workspace build reports a devel
   version, or a Replace directive substitutes the module), the reference
   names the module path with the `devel` marker, registration is admitted
   and status presents the reference verbatim. A process without an instance
   GUID has no source identity: `ebus.primary` is withheld and presented
   `FAILED`, and no placeholder is installed.
2. The bridge binds its DriverManager hooks to the existing manager before
   anything can start it. Then the descriptor is validated, frozen and
   registered as the only production registration (southbound, enabled,
   lifecycle-only, one source, no hooks).
3. The configured drivers are registered in the durable store: the one-time
   seed when no control state exists and an instance GUID is persisted, a
   mark-only registration otherwise. A failed load disposition (corrupt,
   unreadable, unsupported or locked control state, or a missing GUID for
   the seed) writes nothing and presents `FAILED`. A disabled configured
   disposition wins over durable desired state and never rewrites it.
4. The service starts. Its recovery step first restores committed D5
   material: through a managed recovery adapter when the composition has
   one; with no managed owner, committed material fails the drivers it names
   closed and stays committed unchanged. Only then is each driver that
   restoration did not withhold prepared: for `ebus.primary`, the inert hub
   admission with an empty, complete epoch barrier, also when desired state
   is `STOPPED`. Then the reconcilers start. When durable intent is
   `RUNNING`, startup waits, within a short bound, until the reconciler's
   first native start reached DriverManager, never for `RUNNING`.

The production composition has no managed semantic asset, so it uses the
lifecycle-only recovery adapter. The bridge from the store to the service's
`Store` seam lives in `cmd/gateway`, because `internal/runtimecontrol` may not
import the store or the hub.

Single startup owner. The historical unconditional DriverManager start and
the one-shot wait for the first `RUNNING` generation are removed. Only the
service's reconciliation, through the bridge, starts, stops or replaces the
eBUS generation; DriverManager stays the only native retry owner. No
composition failure is a process error: HTTP, health, MCP, GraphQL and status
start when eBUS is stopped, starting, failing, withheld or failed to load.

Executor. The service's executor for `ebus.primary` is the bridge plus the
task runner:

1. `Start` consults the runner first. While a predecessor generation's tasks
   are `DRAINING` (fenced, not joined), one bounded join is attempted. A drain
   that outlives it refuses the start with `DEPENDENCY_UNAVAILABLE`, so no
   successor is started; the start can be requested again once the drain
   joined.
2. `Stop` fences the runner's generation before the bridge's own STOP fence,
   outside every bridge and manager lock, so no effect of the stopped
   generation lands after the bridge fences. The admitted mutation source is
   withdrawn at the same point. The bridge then runs the two independent
   obligations, and the tasks get one bounded drain. A generation that does
   not join stays `DRAINING` with its pending names.
3. Task synchronization runs after every start and stop and on every
   DriverManager change, so a manager-driven same-epoch replacement, a loss
   of admission or a late admission is reconciled without a lifecycle
   decision. A generation that is not yet admitted is simply retried on the
   next change.

Generation tasks. A task set is owned by the full admitted generation key:
`DriverID`, `SourceID`, epoch and driver generation must be set, plus the
native-generation mapping. The lifecycle sequence may be 0: it names the
untyped process-start attempt of durable desired `RUNNING`, which has no
typed work slot, and the epoch plus the process-monotonic native generation
keep the key unique. The post-start eBUS steps run as four ordered,
start-critical tasks (source admission, semantic surface, startup scan,
passive observe-first) once per admitted generation: current-session source validation and admission (default
policy, configured source, passive advisory warmup, or static and override
admission), the startup discovery scan, the M5 revalidation loop, background
full scanning and the passive promotion loop. A later START from `STOPPED`
and a manager-driven replacement rerun them. The task reuses the existing
native selection, scan, revalidation, background scan and promotion
functions. Every effect it installs, such as the admitted mutation source,
goes through the generation scope, and the goroutines it starts are joined
with the generation. Process-scoped consumers are created once, by the first
admitted generation, and survive stopped periods: the semantic poller and
its writers, the passive reconstructor and listener, the address-table
inserter subscription and the promoter. Their I/O through the stable
transport reports unavailability while no generation is admitted. A task
failure is logged with its native generation and never starts, restarts or
replaces a generation. A failed start-critical task (for example the passive
reconstructor start, the passive shadow attach or the leaf-promotion capture
runtime) keeps the shell and the native generation up, skips its dependent
tasks and is presented on the adapter status as `degraded`; MCP names the
task in the adapter status `condition`. Best-effort work (revalidation,
background scanning, passive promotion) runs as joined helpers that log and
continue. The startup discovery scan loop reports its completion through the
scan signals, and the generation is not joined before that loop returned.

Status (S6). Daemon status keeps describing the process. Adapter status
follows current native admission from the cached DriverManager snapshot, with
no endpoint I/O. While a generation is admitted (`RUNNING` or `DEGRADED`) the
historical projection applies. While `STOPPED` or `DISABLED` the adapter
reports `stopped`, and while starting, backing off, stopping or failed it
reports `unavailable`; an admitted generation with a failed start-critical
task reports `degraded`. The retained firmware version stays visible unchanged
as identity evidence. MCP and GraphQL use the same projection. The admitted
initiator address and proxy readiness keep deriving from live DriverManager
admission (proxy `DISABLED`/`DEGRADED`/`READY`).

Retained evidence (S7). Bus observability, identification evidence and the
active/passive deduplicator stay process-scoped. STOP, START and RESTART
neither move, clear nor reset them. Capture pauses while no generation is
admitted, and existing records keep their identity, provenance and age.

Process exit closes mutation admission first: the service accepts no further
decision and its reconcilers stop without writing. Durable desired `RUNNING`
or `STOPPED` and in-flight work stay exactly as committed, and no STOP
operation is synthesized. The generation tasks are then fenced and joined
(bounded) before the shell and the process consumers are torn down, and
DriverManager shuts the native generation down through its existing owner.
Cached list and get views stay readable until teardown.

A manager-driven same-epoch replacement is admitted by the bridge's
admission hold only after the predecessor generation's tasks joined: the
hold withdraws them (fence, one bounded join) and refuses while they drain,
as a retryable dependency failure that DriverManager retries within its own
budget.

Manager-owned recovery after `RUNNING`. When a generation reports its own
loss (the adapter-direct mux delegates a terminal read error or the AM27
blackhole, the proxy listener fails, or a consumer call finds the provider
transport closed), DriverManager withdraws the generation and runs the
`ebus.primary` retry policy. Five fast replacement attempts come first: the
first after one second, each further delay doubled up to thirty seconds.
While the driver is desired `RUNNING` the series then never ends by itself:
the persistent phase restarts at one second and doubles up to five minutes.
Every delay carries 20% jitter and is clamped to its phase's bounds, so a
delay on the cap lies between four and five minutes. The proxy class reaches
the same owner and follows the same policy. The AM27 threshold itself is
unchanged.

Only a retryable failure continues the series: a dial failure, a timeout, a
closed transport or a refused admission while the predecessor's tasks drain.
A failure that retrying cannot heal ends it `FAILED` with its own reason: a
rejected configuration (an invalid endpoint, or an endpoint that rejects the
configured protocol's INIT) as `CONFIG_INVALID`, a lifecycle identity
mismatch (the hub source no longer carries the epoch and generation the
bridge holds) as `INTERNAL_ERROR`, an unavailable or superseded runtime as
`PROVIDER_UNAVAILABLE`, and an unconfirmed close as `CLOSE_UNCONFIRMED` with
quarantine. An explicit `STOP` ends the series with `STOPPED`. An operator
`RESTART`, also one during a backoff wait, cancels the wait at once and opens
a new epoch whose next failure series starts again with the full fast budget.
`RETRY_EXHAUSTED` is not reached by `ebus.primary`.

Opening START admitted into recovery. The opening START of an epoch (process
start, `START` after `STOP`, `RESTART`) runs under the service's
per-operation budget of thirty seconds, and that budget bounds only its first
attempt. When the first attempt fails for a retryable reason (a dial failure,
a timeout, a closed transport), DriverManager schedules the first retry of a
persistent series, and the bridge admits the START into that recovery at the
first manager snapshot that shows it: `BACKOFF` with `RETRY_SCHEDULED`, the
schedule, a persistent series (`RetrySnapshot.Persistent`), desired `RUNNING`,
and a manager revision after the bridge dispatched this epoch's start.
`Start` then returns `runtimecontrol.RecoveringStart` with that `BACKOFF`.
The service admits the epoch like a `RUNNING` one: the slot completes, the
cleanup obligations pass to the admitted generation, so `STOP` and
`RESTART` withdraw it, and a repeated `START` is a no-op. The service
publishes the `BACKOFF` with its schedule instead of `RUNNING`. From then on
the recovery is the one described above: the same policy and terminal set,
the same forwarded transitions, the same view and the same log lines. The
failed start is logged with its class, cause text and the schedule, and each
retry is a recovery attempt whose first reason is the failed start's class.
The first successful construction is admitted as the epoch's generation, and
`RUNNING` reaches the service as a native observation. A terminal class
still fails the START with its own reason, because DriverManager publishes
`FAILED` for it and never schedules a retry. A first attempt that is still in
flight when the budget ends fails the START as before. A `RecoveringStart`
whose observation the service cannot admit is a failed start: its generation
is withdrawn. Drivers with a bounded policy are unchanged: their retries
stay within the opening START's budget.

The gateway logs the loss with its closed trigger category and the schedule,
every visible `BACKOFF` with the next attempt's number, due time (UTC), delay
and phase, every replacement attempt with its number, phase and reason (the
trigger, then the previous attempt's error class), every failed attempt with
its closed error class and cause text, and the admitted recovered generation.
A failed start outside a recovery series is logged the same way. The cause
text is the innermost cause of the failure (the socket call and its errno,
a resolver's verdict, an i/o timeout or a protocol sentinel). The wrapping
that names the dialed address is dropped, anything address-like that remains
is redacted, and the text is bounded to one line, so no line names an
endpoint.

The gateway's manager watch forwards these manager-driven transitions to the
service through `Service.ObserveNative`, once the epoch's opening START has
succeeded or was admitted into recovery: `BACKOFF` with `RETRY_SCHEDULED`
and its schedule (the ordinal of the next attempt within the failure series
and its due time), a retry
`STARTING`, the recovered `RUNNING` or `DEGRADED` generation, and the
terminal `FAILED` of a failure that retrying cannot heal (or
`CLOSE_UNCONFIRMED` with quarantine). The service's own reconciler applies
such an observation only while the driver has no work slot, no held decision
and no attempt in flight, and only for the source epoch and driver generation
of the admitted generation. A stale or foreign observation is dropped.
`STOPPING`, `STOPPED` and `DISABLED` are never accepted from an executor, and
a schedule is accepted only with `BACKOFF`, with both of its fields set. An
observation changes no desired state, slot, budget or cleanup obligation, so
a `FAILED` generation is still withdrawn by a later `STOP` or `RESTART`. A
quarantining observation blocks admission like a service-observed unconfirmed
close. As a result `helianthus.runtime.list`, `helianthus.runtime.get` and
the Portal driver readback leave `RUNNING` as soon as the transport is lost.
They never show `RUNNING` over a closed transport. The service view's
`DriverStatus` carries the schedule as `NextAttempt` and `NextAttemptUTC`
while it shows that `BACKOFF`, and zero values in every other state; service
events stay categorical and do not carry it. The `helianthus.runtime.*` MCP
projection keeps its closed driver object (see "Data." above), so the
schedule is not an MCP member. An operator `helianthus.runtime.restart`
starts a new epoch.

Limits of this composition: the service correlation carries no `SourceID`
and the driver-to-native generation mapping stays process-local.

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

- sequences are per asset: for each `(asset, generation key)` the sequence
  starts at 1 and increases by exactly one, independently of every other
  asset;
- replay of the same `(asset, generation key, sequence, digest)` is a no-op;
- reuse of `(asset, generation key, sequence)` with another digest returns
  `sequence_conflict` and rejects that batch without semantic mutation; the
  gateway then closes admission and initiates a separate atomic fenced
  withdrawal/degrade transition; equal sequences on different assets never
  conflict;
- a sequence gap on one asset rejects that asset's later deltas until the
  adapter supplies a complete resynchronization batch for that asset;
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
| eBUS `ebus.primary` | Discovery, raw evidence, read, semantic projection, and write are declared on the one managed runtime. Stable MCP provides runtime/registry/semantic reads, guarded schedules/config writes, and registry-routed `ebus.v1.rpc.invoke`. B503 exposes seven stable operations: `ebus.v1.vaillant.errors.get`, `ebus.v1.vaillant.errors.history.get`, `ebus.v1.vaillant.errors.history.list`, `ebus.v1.vaillant.service.current.get`, `ebus.v1.vaillant.service.history.get`, `ebus.v1.vaillant.live_monitor.get`, and `ebus.v1.vaillant.live_monitor.session.get`. The evidence/history/session reads are non-mutating; `live_monitor.get` is the bounded session action surface. | Reuse current `DriverManager`, generation admission, selected-source intersection, and registry method mutability. A declared `WRITE` is not universal semantic write authority; the exact current registry method and a nonzero source still decide. An explicit valid nonzero RPC source overrides the startup-admitted source. |
| eeBUS SHIP/SPINE runtime | Public redacted runtime, service, session, topology, pairing, and snapshot reads. Owner-only raw feature and mutation-record reads exist. Raw feature set and rollback exist only through the owner boundary, exact write authorization, configured mutation-lab profile, and a runtime implementing the optional mutation interface. | Adapt the eeBUS runtime slot and command router. Missing mutation interface is `unsupported`, never success. No generic semantic eeBUS write is inferred. Exact normative use-case mappings remain a separate docs/registry dependency. |
| Modbus TCP and qualified SunSpec/Fronius path | Bounded FC03/FC04 raw read, retained profile observation, and one qualified/promoted `helianthus.pack.pv@1.0.0` SemReg projection through the existing qualify/refresh worker. | Read-only. The projection preserves admitted native observation and counter continuity evidence; generated energy is exact Wh-to-kWh with `counter_continuity_unavailable` as a declared projection loss. Reuse adapter-owned scheduling, one owner-gated reconnect/retry, endpoint sanitization, and full wire/logical/physical/generation provenance. No FC06/FC16 or vendor-private write is permitted by the current gateway provider. |
| Tesla HSC provider in production composition | One disabled-by-default status snapshot with compatibility `unknown` and registry-derived `outbound_allowed` (currently false). | No serial acquisition or transmission. FC100/101/102 records and current-limit evidence types do not authorize a live route. |
| Tesla Gen3 WC3 24.44.3 current-limit SemReg projection | Explicit disabled-by-default `tesla-gen3-hsc-*` gateway configuration composes one retained-record owner. After exact completed outcomes are ingested, it exposes evidence-only `modbus.v1.tesla.gen3.evse.current_limit.get`, `semantic.v1.evse.current.get`, mTLS `SemanticEVSECurrent`, and the detached EVSE domain on the configured semantic Prometheus listener. `evse.limit.configured_current` is published only from valid persistent terminal evidence. `evse.limit.allocated_current` is published only from correlated provisional set/ACK/readback evidence with a finite 1..86399 second timeout and `InhibitCharging=false`. | Evidence digests serialize the version, decoded values, every native payload, and captured lifecycle coordinates. The first accepted sequence is `1`; later distinct input must advance by exactly one. An identical accepted `(generation, sequence, input digest)` retry is a no-op; a distinct input at the same sequence and every gap are rejected without mutation. Qualified available read capabilities carry the accepted immutable persistent record evidence and, only when valid, the bounded provisional record evidence; source identity alone never activates them. Freshness and allocation expiry use the injected independent monotonic clock; provisional lifetime starts at the set/ACK receipt while readback advances evaluation. Native receipt/evaluation admission remains independent of consumer reads; a buffered later-correlated outcome keeps its native axes while the aggregate public evaluation clamps to the read high-water. A newer accepted bundle that cannot promote allocation withdraws the prior allocated candidate from SemReg while retaining configured current; its stable candidate revision high-water is retained so later reactivation advances rather than reuses history. Missing, malformed, expired, inhibited, zero-timeout, or correlation-mismatched provisional evidence withholds only allocated current with an explicit projection loss; valid configured current remains. The owner creates no serial listener, acquisition, request, write, operation capability, Portal view, alias, fallback, shadow, or dual publication. |
| Growatt BMS RS-485 1xSxxP ESS V2.02 | When every `growatt-bms-rs485-*` configuration value is explicit and valid, `modbus.v1.growatt.bms.rs485.status.get` exposes the qualified native status and `semantic.v1.storage.growatt.current.get` publishes the accepted seven-fact Storage projection. The same detached SemReg report is available through mTLS GraphQL `SemanticStorageCurrent` and the Portal Storage endpoint when those surfaces are configured. | The endpoint owns serial opening, FC03 correlation, framing, immutable request/response ADU receipts, recovery, transport generation, and close. Gateway owns exact tuple/unit admission, stable asset/source/driver identity, serialized publication, and public routing; `helianthus-modbusreg` owns the four-slice observer and decoder. The semantic path is read-only, performs no Ah-to-kWh conversion, withholds `soft_starting`, and records explicit current, operating-state, and counter-continuity loss. No TCP dependency, scan, broadcast, retry loop, write, or control is introduced. |
| Huawei SmartLogger/EMMA/S-Dongle, Growatt Protocol II, OutBack AXS, Fronius-specific status, and Tesla FC100/WC optional tools other than the retained current-limit read | Typed read/injected-provider contracts exist in the repository, but the production `gatewayModbusMCPProvider` does not implement their optional provider interfaces. | `unavailable` or `uncomposed` at this baseline. Later INT-07 composition must use exact qualified profile/version evidence. No write is inferred from a decoder, fixture, or provisional ACK/readback record. |
| Gree CAN and Growatt CAN | No production provider is composed by this gateway baseline. | `unavailable` here. Receive-only registry readiness remains native evidence, not a gateway operation or transmit grant. |
| Matter output | No production output binding is composed. | No read, write, or conformance claim. INT-13 owns the later versioned target mapping. |

The exact currently registered profile-facing operation names relevant to this
contract are:

- eBUS registry and generic route: `ebus.v1.registry.devices.list`,
  `ebus.v1.registry.devices.get`, `ebus.v1.registry.planes.list`,
  `ebus.v1.registry.methods.list`, and `ebus.v1.rpc.invoke`. Discover the
  address, then plane, then method through those list operations before invoking.
  The invoke schema describes those canonical names; method-specific `params`
  remain native to the selected method. Omitting `params.source` uses the
  startup-admitted source, while an explicit nonzero byte source overrides it.
  Invoke permits `READ_ONLY` only for a known read-only
  registry method. `MUTATE` requires the current registry route, dangerous-
  operation acknowledgement, idempotency key, deadline, and a nonzero source.

### Address-table provenance labels

For the current SemReg public-surface cutover, GraphQL
`Device.verificationState` and MCP `verification_state` on
`ebus.v1.registry.devices.list`, `ebus.v1.registry.devices.get`, and
`ebus.devices` project native `VerificationStateCorroborated` as
`corroborated`. This is the exact normative lifecycle label in the public
[Address Table Model](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/atr/01-address-table-model.md):
`nil -> candidate -> corroborated -> identity_confirmed`.

This is an in-place hard correction selected for gateway issue #607. The
gateway has no alternate spelling, fallback projection, or compatibility-only
semantic namespace for this state. It does not change native verification
state, discovery provenance, lifecycle transitions, or the separate runtime
confidence model.
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
  `ebus.v1.vaillant.errors.history.list`,
  `ebus.v1.vaillant.service.current.get`,
  `ebus.v1.vaillant.service.history.get`, and
  `ebus.v1.vaillant.live_monitor.session.get`. The bounded
  `ebus.v1.vaillant.live_monitor.get` session action is separate; no B503
  install-write tool is present. GraphQL singular reads, the bounded
  `vaillantErrorsHistory`, `vaillantLiveMonitorSession`, and
  `vaillantCapabilities` use nullable roots with non-null returned child
  elements. The history root returns a typed `records` prefix plus nullable
  `failure { index, code, message }`: it stops at the first failed indexed
  read, preserves earlier verified rows, and neither fabricates nor probes
  later indices. The same stable MCP list output is `{records, failure}`.
  An unavailable session or invalid-target capability failure is field-local
  and preserves unrelated root fields in the same query.
  The stable gateway-owned live-monitor session state is one of `Idle`,
  `Enabling`, `Active`, `Refreshing`, or `Disabled`. `Refreshing` means an
  epoch refresh retains the ownership gate while every live-monitor operation
  is busy; a successful refresh returns `Active`, while refresh failure releases
  ownership and returns to `Idle`. `Disabled` never reports `owned: true`.
  While the Live-Monitor tab is visible, Portal refreshes this read-only session
  strip every five seconds so idle expiry and external clients become visible;
  it stops that polling on tab/section exit, document hiding, or component
  disconnect, and every result remains fenced to the selected target and epoch.
  If navigation occurs while an owned session is `Refreshing`, Portal retains
  the exact issuer-token/target pair and uses a separate bounded, read-only
  status check after the visible poll has stopped.  It performs at most one
  token-bound cleanup write after an `Active` confirmation, clears the local
  pair only after a released `Idle`/`Disabled` confirmation or that write's
  `disabled: true`, and stops on pair replacement or finite status-failure and
  non-terminal attempt budgets.  A GraphQL error or null session root is a
  failed refresh: it leaves the last valid state and retained pair intact
  rather than inferring an unowned `Unknown` session.
  Target qualification publishes `PENDING` before it schedules old-pair
  cleanup and never waits for that cleanup RPC; repeated navigation shares a
  blocked cleanup for the same exact pair. Visible and deferred session status
  reads share one serialized request slot. A newer target, epoch, visibility,
  or lifecycle transition supersedes an older result before it may update
  state or issue cleanup.
  Projection B503 capability cards are optional decoration: their probes never
  block projection/bootstrap completion and remain generation-fenced. When
  projection discovery later supplies target addresses to an already-open B503
  pane, Portal rerenders only the picker against the existing qualified state;
  it does not promote or expose actions for an unqualified target. Portal
  history retention is keyed by resolved target identity (including the
  configured-default identity), while qualification epoch only rejects stale
  responses; returning from B to A can therefore retain A's verified rows.
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
- Modbus TCP production composition: `modbus.v1.raw.read`,
  `modbus.v1.profile.observation.get`, `semantic.v1.pv.current.get`, and
  `modbus.v1.tesla.hsc.status.get`. Only FC03/FC04 raw reads are admitted; the
  Tesla status is an inert disabled-profile report with outbound disabled.
- Independent Growatt BMS RTU composition:
  `modbus.v1.growatt.bms.rs485.status.get` and
  `semantic.v1.storage.growatt.current.get`, only when the explicit RTU,
  stable asset/source identity, and lifecycle configuration have been admitted.
  Enabling it without Modbus TCP does not advertise the TCP raw, profile, Tesla,
  or SemReg PV tools. A configured M2M listener exposes the same Storage report
  as `SemanticStorageCurrent`; Portal exposes
  `/api/v1/semantic/storage/current` only after the matching producer starts.
- Independent Tesla Gen3 HSC retained-owner composition:
  `modbus.v1.tesla.gen3.evse.current_limit.get` and
  `semantic.v1.evse.current.get`, only when every `tesla-gen3-hsc-*` identity
  and lifecycle value is explicit and valid. It remains available without the
  Modbus TCP core and does not advertise TCP raw/profile/PV, unrelated Tesla,
  or absent Growatt tools. A configured mTLS listener exposes
  `SemanticEVSECurrent`; the configured semantic Prometheus listener consumes
  the same detached EVSE report. No read surface performs acquisition or I/O.

The gateway constructs the Growatt optional MCP provider only when the RTU
runtime exists. Its wrapper explicitly delegates core availability from the
concrete TCP provider, so the four states remain disjoint: disabled registers no
Modbus tool; TCP-only registers only core tools; BMS-only registers the Growatt
native and Storage semantic tools; TCP+BMS registers core plus both Growatt tools.
The lifecycle factory receives the concrete runtime pointer and checks it before
any optional-interface conversion, so a disabled typed-nil runtime cannot
advertise the Growatt tool.

Portal raw Modbus is a TCP-core diagnostic surface. Its bootstrap capability
and raw-read route are absent for disabled and BMS-only composition even when
the Portal raw-read setting is enabled; TCP-only and TCP+BMS composition retain
the route.

The gateway exposes no public driver lifecycle `list/get/start/stop/restart`
operation at this baseline. The control service in section 4 is the contract
for that later implementation.

### 11.1 Growatt BMS RS-485 V2.02 native and Storage composition

The Growatt BMS observer is disabled unless all of the following are explicitly
configured: `growatt-bms-rs485-enabled`, asset ID, source ID, source epoch,
nonzero driver generation, unicast unit ID, serial path, baud/parity/stop bits, response
timeout, maximum response delay, and maximum quiescence. The selected protocol
tuple is fixed in the gateway binary to `1xSxxP ESS`, `Rev2.01`, `V2.0`, and
`2.02`; configuration cannot select another family or revision. Absent,
disabled, partial, invalid, broadcast/reserved-unit, or invalid-timing input
never opens a serial endpoint.

For each successful poll, the gateway retains one immutable native envelope
with its owner-assigned observation ID/revision, configured source ID/source
epoch/driver generation, upstream clock and transport-generation receipts, last
receipt wall and monotonic coordinates, and all four FC03 request/response ADUs
and words. The envelope is committed only after every ordered slice is bound to
its exact unit/function/offset/quantity and the existing registry observer has
qualified the typed status. A partial read, malformed or mismatched response,
stale/fenced endpoint, mixed transport generation, or changed clock epoch
returns no status and commits no envelope. `outbound_allowed` remains false.

A successful on-demand `modbus.v1.growatt.bms.rs485.status.get` call is `LIVE`.
Its MCP `data_timestamp` is the completed envelope's final receipt wall time,
formatted as RFC3339Nano; it is never synthesized at the handler. The provider
must carry that receipt with the qualified status. Missing receipt metadata
fails closed with the existing unavailable envelope semantics and no timestamp.

A terminal upstream `write_fault` or `transport_fault` marks the endpoint for
recovery but does not retry the failed sample. Before one later poll, the
serialized provider calls only the upstream bounded `Recover`; recovery failure
remains unavailable and fail-closed. Modbus exceptions, admission denial, and
native qualification/semantic failures do not request recovery. A successful
recovery creates the upstream successor transport generation, then the later
poll starts a new normal four-slice sample.

`helianthus-modbus` owns serial lifecycle, request correlation, immutable ADU
evidence, recovery, and transport generation. The gateway does not reopen,
decode, correlate, or retry around that endpoint; it supplies only exact
admission for the four read slices. `helianthus-modbusreg` owns the fixed slice
contract and typed decoding. Gateway binds each completed four-slice native
envelope to one stateful SemReg Storage publication. MCP, GraphQL, and Portal
consume that same publication path; none primes or substitutes a separate cache.
Rejected or cancelled samples preserve the last committed report and do not
advance its semantic sequence. Home Assistant adoption remains a separate
consumer issue. Matter, eeBUS, Prometheus, write, and control publication remain
outside this composition.

Exact public source anchors for these statements:

- eBUS manager configuration and declared capabilities:
  [`cmd/gateway/ebus_driver_runtime.go#L18-L94`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/ebus_driver_runtime.go#L18-L94)
- eBUS source/write admission and lifecycle callbacks:
  [`cmd/gateway/ebus_driver_runtime.go#L241-L282`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/ebus_driver_runtime.go#L241-L282) and
  [`#L345-L573`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/ebus_driver_runtime.go#L345-L573)
- eBUS public tool inventory, invoke guard, and B503 read-only surface:
  [`mcp/server_tools.go#L1-L369`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/server_tools.go#L1-L369),
  [`mcp/server.go#L1932-L1989`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/server.go#L1932-L1989), and
  [`mcp/vaillant_b503.go#L46-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/vaillant_b503.go#L46-L152)
- current manager lifecycle, late admission retirement, generation callback
  fencing, and admission:
  [`internal/drivermanager/manager.go#L93-L189`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/drivermanager/manager.go#L93-L189),
  [`#L325-L658`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/drivermanager/manager.go#L325-L658), and
  [`#L764-L845`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/drivermanager/manager.go#L764-L845)
- eeBUS slot replacement/retirement and provider adapter:
  [`cmd/gateway/eebus_runtime_adapter.go#L26-L156`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/eebus_runtime_adapter.go#L26-L156)
- eeBUS public read inventory and owner-only raw command inventory:
  [`mcp/eebus_v1.go#L19-L108`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/eebus_v1.go#L19-L108) and
  [`mcp/eebus_v1_commands.go#L18-L69`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/eebus_v1_commands.go#L18-L69)
- eeBUS native authorization and optional mutation runtime:
  [`internal/eebuscommand/router.go#L50-L127`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/eebuscommand/router.go#L50-L127)
  and the closed mutation-lab profile loader
  [`cmd/gateway/eebus_mutation_lab_profile.go#L12-L84`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/eebus_mutation_lab_profile.go#L12-L84)
- Modbus public tool/provider surface and composition:
  [`mcp/modbus_v1.go#L15-L152`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/mcp/modbus_v1.go#L15-L152),
  [`cmd/gateway/modbus_mcp_provider.go#L32-L159`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/modbus_mcp_provider.go#L32-L159), and
  [`cmd/gateway/gateway_http_server.go#L68-L85`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_http_server.go#L68-L85)
- Modbus adapter ownership and reconnect boundary:
  [`internal/modbusadapter/adapter.go#L19-L75`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/adapter.go#L19-L75) and
  [`#L257-L299`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/adapter.go#L257-L299)
- independent driver startup and process availability behavior:
  [`cmd/gateway/gateway_run_lifecycle.go#L43-L140`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_run_lifecycle.go#L43-L140)

## 12. SemReg cutover and retained native evidence

1. Freeze exact current MCP schemas/goldens, GraphQL behavior, Portal views, HA
   identities, eBUS registry IDs, eeBUS pairing/trust state, PV snapshot
   accounting, and persistent runtime-state behavior at their owning revisions.
2. Add the runtime control service internally. Adapt only `ebus.primary` first,
   preserving current `DriverManager` behavior and stable public APIs.
3. The migrated Modbus PV capability publishes one evaluated
   `helianthus.pack.pv@1.0.0` SemReg projection to MCP, dedicated M2M GraphQL,
   and Portal. Its fixture/replay evidence covers value, unit, dimensions,
   quality, freshness, provenance, identity, availability, and explicit loss;
   it has no shadow, comparator, fallback, or dual-publication path.
4. Adapt eeBUS without weakening the runtime-slot drain/close boundary or pairing
   state. Adapt Modbus without adding a second scheduler/reconnect owner.
5. Connect each remaining native family only after its provider issue supplies an
   exact profile/version contract and qualification. An unavailable family stays
   visible as unavailable; it does not disappear or become a generic profile.
6. Expose lifecycle `list/get/start/stop/restart` through INT-08 public surfaces.
   Each applicable consumer migrates in the same capability wave as its public
   SemReg route.
7. Cut each mappable capability and its consumers to one SemReg semantic path.
   Remove compatibility-only fallback, comparator, shadow authority, adapter,
   and dual semantic publication before accepting that capability. Preserve the
   native protocol evidence surface and its transport, qualification, lifecycle,
   and safety gates.
8. Remove historical eBUS-owned universal semantic donors when their mappable
   consumers have cut over. A required dependency that does not map to SemReg is
   escalated as a concrete architecture choice; it is not silently retained as a
   second semantic layer.

An identity alias may locate a public semantic asset, but it cannot populate
`DriverID`, `NativeBindingID`, endpoint, generation, or route. Native
`ebus.v1.*`, `eebus.v1.*`, and `modbus.v1.*` evidence contracts retain their
protocol ownership; they are not compatibility semantic APIs.

## 13. Acceptance scenarios for implementation

Each scenario becomes an executable fixture or integration test in its later
owning issue. The expected result is normative. CTL-01 to CTL-11 below are the
D3 concurrency and readback rows of the docs-ebus runtime driver lifecycle page.
They replace the former idempotency-key, authority and receipt rows; no test
of those rows applies any more. CTL-12 to CTL-19 remain on the lifecycle page.
The service tests prove the rows against the durable store; the ordinary MCP
adapter (section 5, S2/D4) is proven equivalent to the service for every
refusal code and for the no-op, conflict and lost-RESTART rows, without
adding any receipt, key or authority row.

| ID | Scenario | Expected result |
|---|---|---|
| CTL-01 | List and get with drivers in arbitrary registration order, while decisions and observed transitions race | Bytewise `DriverID` order; every entry carries the view's one catalog revision; a revision never shows two contents and never goes backwards for a reader |
| CTL-02 | Get or decide an unknown or noncanonical driver | `NOT_FOUND`; no executor call, store write or view change |
| CTL-03 | Changing or same-desired request with a stale expected revision | `REVISION_CONFLICT` with the current desired view; no durable, view, event or native change |
| CTL-04 | Accepted START response is lost; read back and restart the process | Durable desired state and revision are read back; a same-desired START is a no-op with no key or receipt lookup; a changing START persists typed START in the current slot and recovery resumes it |
| CTL-05 | Request with any member besides `DriverID` and `ExpectedRevision`, a duplicate member, trailing JSON or a malformed revision | `INVALID_ARGUMENT` at row 1 before any store access |
| CTL-06 | START while desired `RUNNING` in `BACKOFF` or `FAILED` (including quarantined), STOP while desired `STOPPED` with an unconfirmed close | No-op returning desired state and revision only; retry budget, quarantine, observations, both revisions and the mark unchanged |
| CTL-07 | RESTART in desired `STOPPED`; repeated RESTARTs in `RUNNING`; RESTART, STOP and START during `STOPPING` | `FAILED_PRECONDITION` for RESTART in `STOPPED` (also during `STOPPING`); each accepted RESTART takes a new revision and runs one stop-then-start under a new epoch; STOP during `STOPPING` is a no-op; START takes the reserved slot and a further changing request is `TRANSITION_BUSY` |
| CTL-08 | STOP or RESTART during `STARTING` and `BACKOFF`, with a typed START and with an untyped process-start attempt | Takes the reserved slot (the untyped case synthesizes `CANCEL_ATTEMPT` as the current slot with the attempt's correlation and obligations); the old attempt is fenced, its cleanup runs with its own correlation and a late result is never published; a further changing request is `TRANSITION_BUSY`; no accepted RESTART disappears |
| CTL-09 | Process memory is lost while an accepted RESTART is `ACTIVE` | D5 is restored first; the new incarnation settles the historical correlation and performs the retained replacement under a new epoch before dropping the slot |
| CTL-10 | Lost RESTART response | Resolved by readback; the old-revision retry conflicts and a fresh-revision retry after settlement executes another restart |
| CTL-11 | Concurrent same-desired requests, also while slots are occupied, and concurrent changing requests with one expected revision | Same-desired requests are no-ops with no durable record, wake, event or counter; of the changing requests exactly one is accepted and the others conflict |
| LIFE-01 | Prepare returns behind a closed gate, then activation succeeds with a matching proof | Only the post-proof manager transition installs the generation and exposes admission, `RUNNING`, and qualified capabilities together |
| LIFE-02 | One driver startup fails | That driver becomes `BACKOFF`/`FAILED`; API/health and unrelated drivers remain available |
| LIFE-03 | Ordinary Stop completes its semantic withdrawal and proves native close | One durable two-obligation record fences callbacks, retires generation-owned semantic paths through the cleanup reservation, records close proof, leaves that successor inert/available, then publishes `STOPPED` |
| LIFE-04 | Stop's native-close deadline expires without close proof | Semantic cleanup remains independently recoverable; the driver publishes `FAILED/CLOSE_UNCONFIRMED`, retains source-epoch quarantine, and cannot reuse or claim the successor |
| LIFE-05 | Close-before-Prepare Restart succeeds | Old generation callbacks/admission are fenced; semantic cleanup and native close both complete before the exact successor reservation is claimed and publishes each asset at its persisted per-asset next cursor |
| LIFE-06 | Gateway restarts with desired running and historical observed running | New source epoch reconciles a new generation; historical observation grants no admission |
| LIFE-07 | Canceled start returns a provider late | Provider is never published, is fenced and retired on an independent deadline; unproven close quarantines |
| LIFE-08 | Callback arrives from prior epoch/generation or superseded lifecycle operation | Rejected with no semantic or observed-state mutation |
| LIFE-09 | Prepare or activation completes after its lifecycle operation was superseded | Result is never installed or admitted; `Abort`, or fence plus stop, runs on an independent cleanup deadline |
| LIFE-10 | Activation returns a proof for another binding, epoch, or generation | Fail closed; no capability or `RUNNING` publication, and cleanup/quarantine follows close proof |
| LIFE-11 | Generation 8 publishes while generation 7 remains current but omits generation 7's `GenerationFence` | `generation_transition_incomplete`; reject the complete batch with no state change, and generation 8 does not become current or actionable |
| LIFE-12 | Generation 8 atomically supersedes generation 7 | The same new snapshot fences and withdraws generation 7, invalidates its callback, removes its derived closure, and exposes only qualified generation-8 capabilities |
| LIFE-13 | A post-supersession snapshot retains generation-7 withdrawal/fence tombstones | Full snapshot validation succeeds under the explicit historical-reference rule, while route admission and callbacks still reject generation 7 |
| LIFE-14 | Native activation stages a valid bounded set (at most one batch per asset) and a following callback races promotion | Every staged batch commits in staging order behind closed source visibility/admission, each at its asset's persisted next sequence; the callback waits and forwards only at its asset's next persisted sequence after the complete set |
| LIFE-15 | Activation staging emits another generation, a regression/gap/conflict, a second distinct batch for one asset, exceeds 128 batches/8 MiB, or passes its deadline | Staging is poisoned and activation fails closed; no provider observation, capability, admission, or synthetic same-asset revision becomes public |
| LIFE-16 | A new source epoch starts at generation 1 while an older non-retired epoch ended at generation 8 | Before provider preparation, the manager materializes every affected asset and its exact snapshot/revision, then its per-asset control batch names every older epoch in `SourceRetirements`; each accepted snapshot retires/withdraws old state without inventing provider evidence |
| LIFE-17 | One epoch-transition asset batch omits an older non-retired epoch or loses its expected-revision race | Reject that asset batch; the transitioning source's affected paths and admission remain closed while the manager refreshes and rebuilds it within the bounded durable retry record |
| LIFE-18 | `Prepare` or `Activate` fails before any provider publication in a new process epoch | The earlier manager barrier has already removed every old current candidate and derived path and retained retired/withdrawn tombstones plus immutable history; the inert new descriptor exposes no binding, capability, observation, or route, and admission stays closed |
| LIFE-19 | The process exits after some, but not all, per-asset epoch transitions are accepted | Restart creates a new epoch/record, unions incomplete and indexed assets, rematerializes their current snapshots, and starts a new cursor at sequence 1; this also retires the prior inert epoch on already transitioned assets before provider work, with no cross-asset atomicity or old-snapshot fallback claim |
| LIFE-20 | Two assets A and B complete the new-epoch generation-1 barrier at sequence 1 each; activation discovers both and stages one batch per asset while a callback publishes on A | The lifecycle operation claims that exact available generation-1 reservation and per-asset cursors {A: 2, B: 2}, accepts both unique-asset batches within bounds, commits A at 2 then B at 2, persists {A: 3, B: 3}, and releases the callback on A at sequence 3 with no collision, gap, second generation, cross-asset sequence, cross-asset atomicity, or generation-0 convention |
| LIFE-21 | Activation batch 1 of 2 commits, batch 2 loses its exact input revision, and a callback is waiting | The prefix remains source-fenced and non-admissible; cleanup generation `g+1` fences `g` on every asset of the complete generation-owned asset set, in bytewise asset order, each at that asset's next sequence, while native close runs independently, before a later provider can use inert `g+1` at its persisted per-asset next sequences |
| LIFE-22 | Sources A and B share an asset; A's epoch retirement repeatedly conflicts and exhausts its budget | Old-A direct/transitive paths, selections, items, and routes remain unavailable/withheld, while scoped B reads/routes and B-only aggregate projection items remain visible with their unchanged snapshot-bound provenance; mixed items are withheld and no B fallback is silently selected |
| LIFE-23 | The first staged activation batch commits, then component memory is lost before the second is submitted | Recovery loads the persisted complete `CanonicalEnvelope` suffix, confirms the exact accepted cursor/digest of the first asset, and submits the original second envelope byte-for-byte at its own asset's same next sequence; it never regenerates provider evidence from a digest, count, or reread |
| LIFE-24 | A committed activation prefix enters cleanup, but every semantic cleanup attempt conflicts until its budget expires | Callback/admission fencing first persists separate semantic and native-close obligations; `Stop`/`Abort` still runs within its independent deadline and records proven close or quarantine, unfinished semantic work remains durable and source-withheld, component recovery preserves both, and unrelated sources remain visible |
| LIFE-25 | Process start leaves the driver desired `STOPPED`; two epoch-barrier assets A and B accept at generation 1, sequence 1 each; a later `START` succeeds, then a later close-before-Prepare replacement succeeds | `START` claims the still-inert generation-1 reservation and the first provider publication on each asset uses sequence 2. Replacement cleanup creates generation 2 with every asset cursor at 1, its first accepted retirement batch on each asset atomically fences generation 1, and provider activation claims generation 2 at the cleanup's per-asset cursors; neither path allocates an extra generation or crosses a cursor |
| LIFE-26 | An empty or nonempty epoch barrier completes, then `Prepare` fails or its reconciler wake/component loses memory before `Activate` | Every retry for that lifecycle record uses the same provider claim, generation, per-asset cursors, and correlation. Only proof that no prepared/native state or staged publication remains may release the claim unchanged; uncertainty retains the claim or quarantines it, and no wake allocates another generation or consumes any asset cursor |
| LIFE-27 | Cleanup loses memory (a) after persisting an attempt but before submission, (b) after recording one accepted asset, or (c) after submission before recording its result | Recovery loads the original ordered asset set and unchanged budget. It submits stored bytes for (a), skips the durable accepted prefix for (b), and for (c) uses that asset's cleanup cursor/digest to record accepted readback or replay the exact envelope. It never loses an asset, changes bytes at an accepted sequence, or reuses the failed generation |
| LIFE-28 | Cleanup exhausts with native close already proven, then the gateway process restarts | The new epoch barrier carries every incomplete cleanup asset into its own generation-1 retirement work while the old cleanup journal remains durable; only accepted retirement of every carried asset supersedes and retires the old cleanup reservation. Close proof remains independent, source-owned paths stay withheld, and healthy source-B paths remain visible |
| LIFE-29 | `Prepare` succeeds, the reservation is durably consumed, then `Activate` fails before its first provider batch commits and recovery repeats | The failed generation is never released or reused. A returned Generation object uses its exact keyed fence/Stop; a nil Generation durably aborts the exact publisher key and uses only the retained PreparedGeneration.Abort. If that prepared object is unavailable after recovery, close stays unconfirmed/quarantined. One generation+1 successor remains held with every asset cursor at 1 until proven close, then a subsequent start claims it once and fences the predecessor without inventing proof or observation |
| LIFE-30 | A promoted RUNNING generation begins ordinary Stop; its first semantic withdrawal is submitted but the result is lost, and the sink then remains unavailable through semantic retry exhaustion | The exact lifecycle correlation, fence, ordered canonical attempts, cleanup generation and per-asset cursors, and original retry/deadline remain durable. Native cancellation/drain/close still runs once on its independent original deadline and records proof or quarantine; recovery resolves the uncertain semantic envelope by its asset's cursor/digest, retains unfinished work and source-path withholding, and leaves healthy source-B paths visible |
| LIFE-31 | START consumes generation 1, ordinary STOP completes, then a second START is woken twice and the component restarts before `Prepare` | STOP created exactly one generation-2 cleanup reservation with every asset cursor at 1 and advanced only the cursors of accepted retirement assets. The second START and every repeated/recovered wake claim that same generation-2 key at its persisted per-asset next cursors; generation 1 stays permanently non-reusable/fenced and all old callbacks are rejected |
| LIFE-32 | Close-before-Prepare RESTART loses or conflicts on semantic withdrawal while native close succeeds | The same durable withdrawal record preserves the exact uncertain/rebuilt semantic attempts and close proof independently. No replacement preparation starts until cleanup also completes; it then claims the one cleanup successor without allocating another generation or resetting any asset cursor |
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
| MIG-02 | Gateway restarts or recovers within the supported state window | The current SemReg state contract and persisted state load without mixed versions. Release rollback to a legacy semantic API is unsupported before v1. |

## 14. Evidence and completion boundary

Acceptance of this document proves only an implementable gateway-owned contract
against the pinned source baseline and accepted semantic dependency. It does not
prove that the lifecycle service, SPI, semantic publications, output bindings,
vendor profiles, conformance, Portal, HA, deployment, or physical hardware work.

Later implementation must add executable tests for every applicable scenario,
current-source comparators, profile-specific mapping acceptance, public API
parity, and full exact-BOM integration. Live writes, installations, credentials,
and physical control remain action-time operator decisions.
