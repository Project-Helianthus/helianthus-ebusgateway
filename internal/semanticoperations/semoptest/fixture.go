// Package semoptest provides an offline synthetic operation pack, current
// publication, intent owner, executor lookup and executors for semantic
// operation route tests. The pack has no native mapping, arguments or effect on
// equipment; executors simulate evidence only and never open a device or send.
package semoptest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
)

// Synthetic identities of the one published asset.
const (
	AssetID              semreg.AssetID              = "asset:synthetic"
	SourceID             semreg.SourceID             = "source:synthetic"
	SourceEpochID        semreg.SourceEpochID        = "epoch:synthetic"
	DriverGeneration     semreg.Uint64               = "4"
	BindingID            semreg.NativeBindingID      = "binding:synthetic"
	ServiceInstanceID    semreg.ServiceInstanceID    = "service:synthetic"
	CapabilityInstanceID semreg.CapabilityInstanceID = "capability:synthetic"
	ObserveInstanceID    semreg.CapabilityInstanceID = "capability:synthetic-observe"
	InterlockCandidate   semreg.CandidateID          = "candidate:interlock"
	LevelCandidate       semreg.CandidateID          = "candidate:level"

	clockEpoch semreg.ClockEpochID = "clock-epoch:synthetic"
)

var (
	PackRef           = semreg.PackRef{ID: "fixture.pack.synthetic", Version: "1.0.0"}
	FieldLevel        = definition("fixture.field.level")
	ServiceControl    = definition("fixture.service.control")
	CapabilityLevel   = definition("fixture.capability.level")
	CapabilityObserve = definition("fixture.capability.observe")
	Operation         = definition("fixture.operation.synthetic")
	Effect            = definition("fixture.effect.level")
	LevelFact         = semreg.FactKey{PackID: PackRef.ID, PackVersion: PackRef.Version, FactID: "fixture.fact.level", Dimensions: []semreg.Dimension{}}
	InterlockFact     = semreg.FactKey{PackID: PackRef.ID, PackVersion: PackRef.Version, FactID: "fixture.fact.interlock", Dimensions: []semreg.Dimension{}}
)

func definition(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: PackRef, ID: id, Version: "1.0.0"}
}

// Bool returns a boolean SemReg value.
func Bool(value bool) semreg.Value {
	copied := value
	return semreg.Value{Kind: semreg.ValueBoolean, Boolean: &copied}
}

func text(value string) semreg.Value {
	copied := value
	return semreg.Value{Kind: semreg.ValueText, Text: &copied}
}

// Evidence returns a public synthetic evidence reference whose digest is
// computed from label.
func Evidence(label string) semreg.EvidenceRef {
	sum := sha256.Sum256([]byte("semoptest:" + label))
	return semreg.EvidenceRef{
		Owner: "fixture.owner", Kind: "fixture.evidence", Contract: "fixture.evidence/v1",
		Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])),
		Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone,
	}
}

// Pack is the synthetic operation pack. Level readback values are boolean; a
// text level value is a valid but inconclusive observation.
type Pack struct{}

var _ operation.OperationPackValidator = Pack{}

func (Pack) Pack() semreg.PackRef { return PackRef }
func (Pack) Definitions() semreg.DefinitionIndex {
	return semreg.DefinitionIndex{
		Pack: PackRef, Fields: []semreg.DefinitionRef{FieldLevel}, Services: []semreg.DefinitionRef{ServiceControl},
		Capabilities: []semreg.DefinitionRef{CapabilityLevel, CapabilityObserve},
		Operations:   []semreg.DefinitionRef{Operation}, EffectRules: []semreg.DefinitionRef{Effect},
	}
}
func (Pack) ValidateFact(key semreg.FactKey, value *semreg.Value) error {
	if key.PackID != PackRef.ID || key.PackVersion != PackRef.Version || len(key.Dimensions) != 0 {
		return errors.New("unknown fact")
	}
	switch key.FactID {
	case LevelFact.FactID:
		if value != nil && (value.Kind == semreg.ValueBoolean && value.Boolean != nil || value.Kind == semreg.ValueText && value.Text != nil) {
			return nil
		}
	case InterlockFact.FactID:
		if value != nil && value.Kind == semreg.ValueBoolean && value.Boolean != nil {
			return nil
		}
	}
	return errors.New("invalid fact value")
}
func (Pack) ValidateService(semreg.ServiceInstance) error       { return nil }
func (Pack) ValidateCapability(semreg.CapabilityInstance) error { return nil }
func (Pack) ValidateField(ref semreg.DefinitionRef, field semreg.TypedField) error {
	if ref != FieldLevel || field.ID != FieldLevel.ID || field.Value.Kind != semreg.ValueBoolean || field.Value.Boolean == nil {
		return errors.New("invalid level argument")
	}
	return nil
}
func (Pack) MatchConstraints(_ semreg.CapabilityInstance, fields []semreg.TypedField) error {
	if len(fields) != 1 || fields[0].ID != FieldLevel.ID || fields[0].Value.Boolean == nil {
		return errors.New("constraint mismatch")
	}
	return nil
}
func (Pack) EvaluatePredicate(candidate semreg.FactCandidate, operator semreg.PredicateOp, expected semreg.Value) (bool, error) {
	if operator != semreg.PredicateEqual || candidate.Value == nil || candidate.Value.Boolean == nil || expected.Boolean == nil {
		return false, errors.New("unsupported predicate")
	}
	return *candidate.Value.Boolean == *expected.Boolean, nil
}
func (Pack) ValidateIntent(intent operation.Intent) error {
	if intent.Kind != Operation || len(intent.Arguments) != 1 || intent.Arguments[0].ID != FieldLevel.ID || intent.Arguments[0].Value.Boolean == nil {
		return &semreg.Error{ID: semreg.InvalidValue, Detail: "synthetic operation arguments"}
	}
	requirement := intent.RequiredCapability
	if requirement.Pack != PackRef || requirement.DefinitionID != CapabilityLevel.ID || requirement.Versions != (semreg.VersionRange{Minimum: "1.0.0", MaximumExclusive: "2.0.0"}) {
		return &semreg.Error{ID: semreg.InvalidValue, Detail: "synthetic required capability"}
	}
	effect := intent.ExpectedEffect
	if effect.Rule != Effect || effect.Operator != semreg.PredicateEqual || effect.Fact.FactID != LevelFact.FactID || effect.Expected.Boolean == nil ||
		*effect.Expected.Boolean != *intent.Arguments[0].Value.Boolean {
		return &semreg.Error{ID: semreg.InvalidValue, Detail: "synthetic expected effect"}
	}
	return nil
}
func (Pack) EvaluateReadback(intent operation.Intent, candidate semreg.FactCandidate) (operation.ReadbackRelation, error) {
	if candidate.Value == nil || candidate.Key.FactID != intent.ExpectedEffect.Fact.FactID || intent.ExpectedEffect.Expected.Boolean == nil {
		return "", errors.New("readback fact")
	}
	if candidate.Value.Boolean == nil {
		return operation.ReadbackInconclusive, nil
	}
	if *candidate.Value.Boolean == *intent.ExpectedEffect.Expected.Boolean {
		return operation.ReadbackConfirms, nil
	}
	return operation.ReadbackContradicts, nil
}

// Fixture owns one synthetic current publication and the clocks that order
// admission, dispatch and readback evidence.
type Fixture struct {
	tb     testing.TB
	Kernel *operation.Kernel

	mu          sync.Mutex
	publication *semreg.PublicationKernel
	snapshot    semreg.Snapshot
	sequence    int
	ticks       uint64
	attempts    int
	generation  semreg.Uint64

	captures atomic.Int32
}

// New publishes the synthetic asset in three batches so the semantic,
// capability-collection and capability-instance revisions all differ.
func New(tb testing.TB) *Fixture {
	tb.Helper()
	publication, err := semreg.NewPublicationKernel(AssetID, Pack{})
	if err != nil {
		tb.Fatal(err)
	}
	kernel, err := operation.NewKernel(Pack{})
	if err != nil {
		tb.Fatal(err)
	}
	f := &Fixture{tb: tb, Kernel: kernel, publication: publication, generation: DriverGeneration}
	f.apply(func(batch *semreg.PublicationBatch) {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: SourceID, SourceEpochID: SourceEpochID, ProtocolID: "fixture.protocol", ProfileID: "fixture.profile", ProfileVersion: "1", RegistryEvidence: Evidence("registry"), StartedAt: f.wall(), State: semreg.SourceCurrent, Revision: "1"}}
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: BindingID, AssetID: AssetID, SourceID: SourceID, SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration, NativeResource: Evidence("resource"), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: AssetID, BindingID: BindingID, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{Evidence("identity")}, Revision: "1"}}
		batch.FactUpserts = []semreg.FactCandidate{f.candidate(InterlockCandidate, InterlockFact, Bool(true), "1")}
		batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: ServiceInstanceID, AssetID: AssetID, Definition: ServiceControl, BindingID: BindingID, SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}}
		batch.CapabilityUpserts = []semreg.CapabilityInstance{f.capability(CapabilityInstanceID, CapabilityLevel)}
	})
	f.apply(func(batch *semreg.PublicationBatch) {
		batch.CapabilityUpserts = []semreg.CapabilityInstance{f.capability(ObserveInstanceID, CapabilityObserve)}
	})
	f.apply(func(batch *semreg.PublicationBatch) {
		batch.FactUpserts = []semreg.FactCandidate{f.candidate(InterlockCandidate, InterlockFact, Bool(true), "2")}
	})
	return f
}

func (f *Fixture) capability(id semreg.CapabilityInstanceID, definition semreg.DefinitionRef) semreg.CapabilityInstance {
	return semreg.CapabilityInstance{
		InstanceID: id, AssetID: AssetID, ServiceInstance: ServiceInstanceID, Definition: definition, BindingID: BindingID,
		SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration, Qualification: semreg.QualificationQualified,
		Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{Evidence("activation")}, Revision: "1",
	}
}

// apply must be called with f.mu held or before the fixture is shared.
func (f *Fixture) apply(mutate func(*semreg.PublicationBatch)) semreg.Snapshot {
	f.tb.Helper()
	f.sequence++
	expected := semreg.Uint64("0")
	if f.sequence > 1 {
		expected = f.snapshot.Revisions.Semantic
	}
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:synthetic:" + strconv.Itoa(f.sequence)), AssetID: AssetID, SourceID: SourceID,
		SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration, Sequence: semreg.Uint64(strconv.Itoa(f.sequence)),
		ExpectedSemanticRevision: expected, ObservedAt: f.wall(),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{},
		IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{},
		CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	mutate(&batch)
	digest, err := batch.ComputedDigest()
	if err != nil {
		f.tb.Fatal(err)
	}
	batch.BatchDigest = digest
	snapshot, _, err := f.publication.Apply(batch, f.tick())
	if err != nil {
		f.tb.Fatal(err)
	}
	f.snapshot = snapshot
	return snapshot
}

func (f *Fixture) candidate(id semreg.CandidateID, key semreg.FactKey, value semreg.Value, revision semreg.Uint64) semreg.FactCandidate {
	binding, epoch, generation, source := BindingID, SourceEpochID, DriverGeneration, SourceID
	received, receipt := f.wall(), f.tick()
	return semreg.FactCandidate{
		CandidateID: id, Key: key, Value: &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: received, ReceiptMonotonic: receipt, EvaluatedAt: received, EvaluateMonotonic: receipt},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy.synthetic", Version: "1.0.0", FreshForNS: "3600000000000", RetainForNS: "7200000000000", MaxWallUncertaintyNS: "1000000"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:" + string(id)), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{Evidence("observation")}},
		Evidence: []semreg.EvidenceRef{Evidence("observation")}, Revision: revision,
	}
}

// wall and tick read the fixture clocks; callers hold f.mu or own the fixture.
func (f *Fixture) wall() semreg.TimePoint { return TimePoint(time.Now()) }
func (f *Fixture) tick() semreg.MonotonicPoint {
	f.ticks += 1000
	return semreg.MonotonicPoint{ClockEpochID: clockEpoch, Nanoseconds: semreg.Uint64(strconv.FormatUint(f.ticks, 10))}
}

// TimePoint converts a wall instant to a SemReg UTC point.
func TimePoint(at time.Time) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(at.UnixNano(), 10)), ClockID: "clock.utc", UncertaintyNS: "0"}
}

// Context returns a fresh evaluation context on the fixture clocks.
func (f *Fixture) Context() semreg.EvaluationContext {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.contextLocked()
}

func (f *Fixture) contextLocked() semreg.EvaluationContext {
	return semreg.EvaluationContext{EvaluatedAt: f.wall(), EvaluateMonotonic: f.tick()}
}

// Snapshot returns the current publication.
func (f *Fixture) Snapshot() semreg.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot
}

// Route is the one route the kernel admits for the synthetic capability.
func (f *Fixture) Route() operation.Route {
	return operation.Route{CapabilityInstance: CapabilityInstanceID, ServiceInstance: ServiceInstanceID, BindingID: BindingID, SourceID: SourceID, SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration}
}

// Request returns a valid request against the current publication.
func (f *Fixture) Request() semanticoperations.Request {
	snapshot := f.Snapshot()
	return semanticoperations.Request{
		AssetID: AssetID, BindingID: BindingID, CapabilityInstanceID: CapabilityInstanceID, Operation: Operation,
		Arguments:                          []semreg.TypedField{{ID: FieldLevel.ID, Value: Bool(true)}},
		ExpectedSemanticRevision:           snapshot.Revisions.Semantic,
		ExpectedCapabilityRevision:         snapshot.Revisions.Capabilities,
		ExpectedCapabilityInstanceRevision: "1",
		ExpectedSourceEpochID:              SourceEpochID,
		ExpectedDriverGeneration:           DriverGeneration,
		Preconditions:                      []operation.Precondition{{Fact: InterlockFact, CandidateID: InterlockCandidate, CandidateRevision: "2", Operator: semreg.PredicateEqual, Expected: Bool(true)}},
		Timeout:                            2 * time.Second,
	}
}

// Captures counts snapshot captures.
func (f *Fixture) Captures() int { return int(f.captures.Load()) }

// Capture is the current-generation capture of the synthetic asset; every
// other asset is unknown.
func (f *Fixture) Capture(ctx context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	f.captures.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	inputs := make([]semanticview.Input, 0, len(ids))
	for _, id := range ids {
		if id != AssetID {
			inputs = append(inputs, semanticview.Input{AssetID: id, UnavailableReason: "semantic.read.reason.asset_unknown"})
			continue
		}
		snapshot := f.snapshot
		evaluation, err := semreg.EvaluateSnapshot(snapshot, f.contextLocked())
		if err != nil {
			return nil, nil, err
		}
		inputs = append(inputs, semanticview.Input{AssetID: id, Snapshot: &snapshot, Evaluation: &evaluation})
	}
	return nil, inputs, nil
}

// Fence retires the live native generation the executors recheck, as a
// concurrent driver restart would, without touching the publication.
func (f *Fixture) Fence() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.generation = "5"
}

func (f *Fixture) guardCurrent(guard operation.GuardClaims) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return guard.AssetID == AssetID && guard.BindingID == BindingID && guard.SourceID == SourceID && guard.SourceEpochID == SourceEpochID &&
		guard.DriverGeneration == f.generation && guard.ServiceInstance == ServiceInstanceID && guard.CapabilityInstance == CapabilityInstanceID
}

// Owner is a fake runtime intent owner. With Replay set it completes every
// draft into byte-identical intent members, as an owner deriving a stable
// idempotency key would.
type Owner struct {
	Replay bool
	Err    error
	Mutate func(*operation.Intent)

	mu      sync.Mutex
	next    int
	start   time.Time
	replay  time.Time
	calls   atomic.Int32
	budgets []time.Time
}

func (f *Fixture) Owner() *Owner {
	now := time.Now()
	return &Owner{start: now, replay: now.Add(time.Minute)}
}

func (o *Owner) Calls() int { return int(o.calls.Load()) }

func (o *Owner) CompleteIntent(_ context.Context, draft operation.Intent, budget time.Time) (operation.Intent, operation.AuthorityResolver, error) {
	o.calls.Add(1)
	if o.Err != nil {
		return operation.Intent{}, nil, o.Err
	}
	o.mu.Lock()
	o.next++
	sequence, deadline := o.next, budget
	o.budgets = append(o.budgets, budget)
	if o.Replay {
		sequence, deadline = 1, o.replay
	}
	o.mu.Unlock()
	suffix := strconv.Itoa(sequence)
	intent := draft
	intent.IntentID = semreg.IntentID("intent:synthetic:" + suffix)
	intent.IdempotencyKey = semreg.IdempotencyKey("idempotency:synthetic:" + suffix)
	intent.RequiredCapability = operation.CapabilityRequirement{
		Pack: PackRef, DefinitionID: CapabilityLevel.ID, Versions: semreg.VersionRange{Minimum: "1.0.0", MaximumExclusive: "2.0.0"},
		InstanceID: draft.RequiredCapability.InstanceID,
	}
	expected := Bool(true)
	if len(draft.Arguments) == 1 && draft.Arguments[0].Value.Boolean != nil {
		expected = draft.Arguments[0].Value
	}
	// The owner derives the effect rule inside the requested operation's pack;
	// an unknown pack or operation stays a kernel definition-owner rejection.
	rule := semreg.DefinitionRef{Pack: draft.Kind.Pack, ID: Effect.ID, Version: Effect.Version}
	intent.ExpectedEffect = operation.ExpectedEffect{Rule: rule, Fact: LevelFact, Operator: semreg.PredicateEqual, Expected: expected}
	intent.Authority = Evidence("authority")
	intent.Causal = semreg.CausalContext{
		Origin:        semreg.OriginRef{OriginID: "origin:operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{Evidence("operator")}},
		CorrelationID: semreg.CorrelationID("correlation:synthetic:" + suffix), HopCount: 0, MaxHops: 8,
		FirstSeenAt: TimePoint(o.start), ExpiresAt: TimePoint(o.replay.Add(time.Minute)), Path: []semreg.TargetID{},
	}
	intent.Deadline = TimePoint(deadline)
	if o.Mutate != nil {
		o.Mutate(&intent)
	}
	return intent, operation.AuthorityResolverFunc(func(operation.Intent, operation.Route, semreg.EvaluationContext) error { return nil }), nil
}

// Budgets returns every budget the router offered.
func (o *Owner) Budgets() []time.Time {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]time.Time(nil), o.budgets...)
}

// Outcome describes the evidence one fake executor reports after its guard
// recheck. A zero Kind reports no terminal evidence (accepted handoff).
type Outcome struct {
	Kind     operation.Outcome
	Delivery operation.DeliveryState
	Ack      operation.AckState
	Readback operation.ReadbackRelation
	Err      error
	// Invalid reports an applied outcome without readback, which the kernel rejects.
	Invalid bool
	// Block waits for cancellation of the dispatch context and then for
	// Executor.Release before reporting: with a zero Kind it returns the
	// context error, otherwise late evidence.
	Block bool
	// Panic panics after the simulated send.
	Panic bool
	// TamperReadback edits the readback evidence after it is built. earlier is
	// the publication that was current before the simulated send; assigning
	// it to *later reports that pre-dispatch snapshot instead.
	TamperReadback func(readback *operation.Readback, later *semreg.Snapshot, earlier semreg.Snapshot)
}

// Executor is a fake qualified executor. It counts invocations and simulated
// sends; it rechecks the guard against the fixture's live generation first.
type Executor struct {
	f       *Fixture
	outcome Outcome
	Before  func()

	calls    atomic.Int32
	sends    atomic.Int32
	mu       sync.Mutex
	received []semanticoperations.Dispatch
	hold     chan struct{}
	release  sync.Once
	released chan struct{}
}

// Executor returns a fake executor; a blocking one is released when the test
// ends at the latest.
func (f *Fixture) Executor(outcome Outcome) *Executor {
	e := &Executor{f: f, outcome: outcome, hold: make(chan struct{}), released: make(chan struct{})}
	f.tb.Cleanup(e.Release)
	return e
}

func (e *Executor) Calls() int { return int(e.calls.Load()) }
func (e *Executor) Sends() int { return int(e.sends.Load()) }

// Release lets a blocking executor report once its context is canceled.
func (e *Executor) Release() { e.release.Do(func() { close(e.hold) }) }

// Done is closed when a blocking executor has returned.
func (e *Executor) Done() <-chan struct{} { return e.released }

// Dispatches returns every dispatch received.
func (e *Executor) Dispatches() []semanticoperations.Dispatch {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]semanticoperations.Dispatch(nil), e.received...)
}

func (e *Executor) Execute(ctx context.Context, dispatch semanticoperations.Dispatch) (semanticoperations.Completion, error) {
	e.calls.Add(1)
	e.mu.Lock()
	e.received = append(e.received, dispatch)
	e.mu.Unlock()
	if e.Before != nil {
		e.Before()
	}
	if !e.f.guardCurrent(dispatch.Guard) {
		record := e.f.record(dispatch, operation.OutcomeFailedNoContact, operation.DeliveryNotSent)
		return semanticoperations.Completion{Record: &record}, nil
	}
	o := e.outcome
	if o.Block {
		defer close(e.released)
		<-ctx.Done()
		<-e.hold
		if o.Kind == "" {
			e.sends.Add(1)
			return semanticoperations.Completion{}, ctx.Err()
		}
	}
	if o.Kind != operation.OutcomeFailedNoContact {
		e.sends.Add(1)
	}
	if o.Panic {
		panic("synthetic executor failure")
	}
	if o.Err != nil {
		return semanticoperations.Completion{}, o.Err
	}
	if o.Kind == "" {
		return semanticoperations.Completion{}, nil
	}
	record := e.f.record(dispatch, o.Kind, o.Delivery)
	f := e.f
	if o.Ack != "" {
		record.Acknowledgement = &operation.Acknowledgement{State: o.Ack, At: TimePoint(time.Now()), Evidence: []semreg.EvidenceRef{Evidence("ack")}}
	}
	var later *semreg.Snapshot
	if o.Readback != "" {
		value := dispatch.Intent.ExpectedEffect.Expected
		switch o.Readback {
		case operation.ReadbackContradicts:
			value = Bool(!*value.Boolean)
		case operation.ReadbackInconclusive:
			value = text("level.unknown")
		}
		f.mu.Lock()
		earlier := f.snapshot
		snapshot := f.apply(func(batch *semreg.PublicationBatch) {
			batch.FactUpserts = []semreg.FactCandidate{f.candidate(LevelCandidate, LevelFact, value, "1")}
		})
		evaluation := f.contextLocked()
		f.mu.Unlock()
		record.Readback = &operation.Readback{
			SnapshotID: snapshot.SnapshotID, Revisions: snapshot.Revisions, CandidateID: LevelCandidate, CandidateRevision: "1",
			BindingID: BindingID, SourceID: SourceID, SourceEpochID: SourceEpochID, DriverGeneration: DriverGeneration,
			Relation: o.Readback, Evaluation: evaluation, Evidence: []semreg.EvidenceRef{Evidence("readback")},
		}
		later = &snapshot
		if o.TamperReadback != nil {
			o.TamperReadback(record.Readback, later, earlier)
		}
	}
	if o.Invalid {
		record.Outcome, record.Readback, later = operation.OutcomeApplied, nil, nil
	}
	return semanticoperations.Completion{Record: &record, ReadbackSnapshot: later}, nil
}

func (f *Fixture) record(dispatch semanticoperations.Dispatch, outcome operation.Outcome, delivery operation.DeliveryState) operation.ExecutionRecord {
	f.mu.Lock()
	f.attempts++
	attempt := semreg.AttemptID("attempt:synthetic:" + strconv.Itoa(f.attempts))
	started := f.contextLocked()
	completed := f.contextLocked()
	f.mu.Unlock()
	record := dispatch.ExecutionRecord()
	record.Dispatch = &operation.DispatchEvidence{
		AttemptID: attempt, Started: started, Completed: &completed, Delivery: delivery,
		PossibleSideEffect: delivery != operation.DeliveryNotSent, Evidence: []semreg.EvidenceRef{Evidence("dispatch")},
	}
	record.Outcome = outcome
	record.OutcomeEvidence = []semreg.EvidenceRef{Evidence("outcome")}
	return record
}

// Lookup is a fake executor lookup keyed by the exact admitted route and
// operation.
type Lookup struct {
	mu      sync.Mutex
	entries []lookupEntry
	calls   atomic.Int32
}

type lookupEntry struct {
	route    operation.Route
	kind     semreg.DefinitionRef
	executor semanticoperations.Executor
}

func (l *Lookup) Register(route operation.Route, kind semreg.DefinitionRef, executor semanticoperations.Executor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, lookupEntry{route: route, kind: kind, executor: executor})
}

func (l *Lookup) Calls() int { return int(l.calls.Load()) }

func (l *Lookup) Lookup(route operation.Route, kind semreg.DefinitionRef) (semanticoperations.Executor, bool) {
	l.calls.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, entry := range l.entries {
		if entry.route == route && entry.kind == kind {
			return entry.executor, true
		}
	}
	return nil, false
}
