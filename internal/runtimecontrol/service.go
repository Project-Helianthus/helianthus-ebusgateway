package runtimecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// Service bounds. The operation budget and attempt limit are the contract's
// per-operation semantic bounds (8 attempts within 30 seconds); they persist
// in the typed slot and are never renewed by a wake, a no-op or recovery.
const (
	DefaultOperationBudget = 30 * time.Second
	DefaultMaxAttempts     = 8
	MaxServiceDrivers      = 128
	MaxDiagnostics         = 32
	MaxDiagnosticBytes     = 512
	maxIncarnationBytes    = 64
	maxPublishAttempts     = 4
	maxCommitAttempts      = 3
	maxPendingEvents       = 256
)

// Service errors outside the refusal table.
var (
	ErrInvalidConfig   = errors.New("runtimecontrol: invalid service configuration")
	ErrServiceState    = errors.New("runtimecontrol: service not started or already closed")
	ErrStaleSlot       = errors.New("runtimecontrol: work slot no longer current")
	ErrInvalidWork     = errors.New("runtimecontrol: invalid work update")
	ErrInvalidRecovery = errors.New("runtimecontrol: invalid recovery material")
)

var incarnationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// DriverConfig is one driver served by the control service. Configured
// eligibility (enabled or disabled) comes from the store's committed view.
type DriverConfig struct {
	DriverID DriverID
	Executor LifecycleExecutor
}

// Options configures a Service. Store, Recovery, Incarnation and at least one
// driver are required; there is no default recovery adapter and no fake
// success path.
type Options struct {
	Store    Store
	Recovery RecoveryAdapter
	Drivers  []DriverConfig
	// Incarnation names this process incarnation; every source epoch the
	// service allocates is derived from it, so epochs never repeat across
	// incarnations when the caller supplies a fresh value per process.
	Incarnation     string
	Now             func() time.Time
	OperationBudget time.Duration
	MaxAttempts     int
	// RetryDelay returns the backoff before the next attempt; attempt counts
	// the attempts already used.
	RetryDelay    func(attempt int) time.Duration
	EventCapacity int
	// OnError receives internal errors with their identity preserved: a
	// *SemanticError for recovery-adapter failures, a *LifecycleError for
	// executor failures and the store's own error for persistence failures.
	OnError func(error)
}

// DriverStatus is one entry of the cached catalog view. It carries no
// endpoint, credential, raw error, principal or authority material.
type DriverStatus struct {
	DriverID          DriverID
	Enabled           bool
	DesiredState      DesiredState
	DesiredRevision   DesiredRevision
	ObservedState     drivermanager.ObservedState
	Reason            drivermanager.ReasonCode
	SafetyQuarantined bool
	Revision          CatalogRevision
}

// Catalog is one coherent cached view: every served driver sorted bytewise by
// DriverID, each entry carrying the view revision. EventCursor is the cursor
// of the newest event emitted at or before this view.
type Catalog struct {
	Revision    CatalogRevision
	EventCursor uint64
	Drivers     []DriverStatus
}

// SemanticError wraps a recovery-adapter (semantic registry) failure. It
// preserves the original error unchanged for errors.Is and errors.As and is
// never converted into a lifecycle refusal code.
type SemanticError struct {
	DriverID DriverID
	Op       string
	Err      error
}

func (e *SemanticError) Error() string {
	return fmt.Sprintf("runtimecontrol: semantic recovery %s failed for %q: %v", e.Op, e.DriverID, e.Err)
}

func (e *SemanticError) Unwrap() error { return e.Err }

// LifecycleError wraps a lifecycle executor failure of one typed or untyped
// attempt. It is distinct from Refusal (a request refusal) and SemanticError.
type LifecycleError struct {
	DriverID DriverID
	Kind     TransitionKind
	Err      error
}

func (e *LifecycleError) Error() string {
	return fmt.Sprintf("runtimecontrol: lifecycle %s failed for %q: %v", e.Kind, e.DriverID, e.Err)
}

func (e *LifecycleError) Unwrap() error { return e.Err }

// Diagnostic is one sanitized terminal summary kept in a bounded in-memory
// ring. It is never used for admission, deduplication or recovery and is
// dropped with the process.
type Diagnostic struct {
	Kind            TransitionKind
	DesiredRevision DesiredRevision
	Outcome         string
	AttemptsUsed    int
	Summary         string
}

type observation struct {
	state       drivermanager.ObservedState
	reason      drivermanager.ReasonCode
	quarantined bool
	epoch       SourceEpoch
	generation  DriverGeneration
}

// attempt is one in-memory start attempt and its correlation. fenced is
// guarded by the owning driverState.mu.
type attempt struct {
	corr     LifecycleCorrelation
	ctx      context.Context
	cancel   context.CancelFunc
	fenced   bool
	sequence LifecycleSequence // typed slot sequence, 0 for an untyped attempt
	admitted bool
}

type driverState struct {
	id   DriverID
	exec LifecycleExecutor
	wake chan struct{}

	mu   sync.Mutex
	cond *sync.Cond
	// deciding counts decisions whose planner ran for this driver and whose
	// store call has not returned; attempt settlement and slot pruning wait
	// for it to drop to zero.
	deciding        int
	pruning         bool
	untyped         *attempt
	inflight        *attempt
	live            *attempt
	quarantined     bool
	recoveryBlocked bool
	// cleanupRetries counts consecutive unfinished semantic cleanups.
	cleanupRetries int
	autoStartDone  bool
	epochOrdinal   uint64
	// corr is the correlation the driver's current observation refers to:
	// the attempt being started or the generation being withdrawn.
	corr LifecycleCorrelation

	diagMu sync.Mutex
	diags  []Diagnostic
}

// Service is the internal S1/D3 desired-state service. Its only global lock
// (viewMu) guards in-memory view assembly and is never held across store
// writes, executor calls or recovery-adapter calls; list and get read an
// atomically published view and never wait.
type Service struct {
	store       Store
	recovery    RecoveryAdapter
	incarnation string
	now         func() time.Time
	budget      time.Duration
	maxAttempts int
	retryDelay  func(int) time.Duration
	onError     func(error)

	drivers map[DriverID]*driverState
	order   []DriverID
	events  *eventLog

	viewMu   sync.Mutex
	observed map[DriverID]observation
	// pendingEv holds committed-in-memory events whose view publication
	// failed; they are emitted, in order, with the next published view.
	pendingEv  []Event
	dirty      bool
	retryArmed bool
	view       atomic.Pointer[Catalog]

	lifeMu  sync.Mutex
	started bool
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan:
		return v.IsNil()
	}
	return false
}

// New validates the configuration and builds a Service. Validation performs
// no store access and invokes no executor or recovery adapter.
func New(opts Options) (*Service, error) {
	if isNil(opts.Store) {
		return nil, fmt.Errorf("%w: missing store", ErrInvalidConfig)
	}
	if isNil(opts.Recovery) {
		return nil, fmt.Errorf("%w: missing recovery adapter", ErrInvalidConfig)
	}
	if len(opts.Incarnation) == 0 || len(opts.Incarnation) > maxIncarnationBytes || !incarnationPattern.MatchString(opts.Incarnation) {
		return nil, fmt.Errorf("%w: invalid incarnation", ErrInvalidConfig)
	}
	if len(opts.Drivers) == 0 || len(opts.Drivers) > MaxServiceDrivers {
		return nil, fmt.Errorf("%w: driver count must be 1 to %d", ErrInvalidConfig, MaxServiceDrivers)
	}
	if opts.OperationBudget < 0 || opts.OperationBudget > DefaultOperationBudget || opts.MaxAttempts < 0 || opts.MaxAttempts > DefaultMaxAttempts {
		return nil, fmt.Errorf("%w: invalid operation bounds", ErrInvalidConfig)
	}
	s := &Service{
		store:       opts.Store,
		recovery:    opts.Recovery,
		incarnation: opts.Incarnation,
		now:         opts.Now,
		budget:      opts.OperationBudget,
		maxAttempts: opts.MaxAttempts,
		retryDelay:  opts.RetryDelay,
		onError:     opts.OnError,
		drivers:     make(map[DriverID]*driverState, len(opts.Drivers)),
		events:      newEventLog(opts.EventCapacity),
		observed:    make(map[DriverID]observation, len(opts.Drivers)),
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.budget == 0 {
		s.budget = DefaultOperationBudget
	}
	if s.maxAttempts == 0 {
		s.maxAttempts = DefaultMaxAttempts
	}
	if s.retryDelay == nil {
		s.retryDelay = defaultRetryDelay
	}
	for _, cfg := range opts.Drivers {
		if !canonicalDriverID(cfg.DriverID) {
			return nil, fmt.Errorf("%w: noncanonical driver id", ErrInvalidConfig)
		}
		if _, dup := s.drivers[cfg.DriverID]; dup {
			return nil, fmt.Errorf("%w: duplicate driver id %s", ErrInvalidConfig, cfg.DriverID)
		}
		if isNil(cfg.Executor) {
			return nil, fmt.Errorf("%w: driver %s has no lifecycle executor", ErrInvalidConfig, cfg.DriverID)
		}
		d := &driverState{id: cfg.DriverID, exec: cfg.Executor, wake: make(chan struct{}, 1)}
		d.cond = sync.NewCond(&d.mu)
		s.drivers[cfg.DriverID] = d
		s.order = append(s.order, cfg.DriverID)
	}
	sort.Slice(s.order, func(i, j int) bool { return s.order[i] < s.order[j] })
	s.view.Store(&Catalog{})
	return s, nil
}

func defaultRetryDelay(attempt int) time.Duration {
	delay := 100 * time.Millisecond << uint(min(attempt, 6))
	return min(delay, 5*time.Second)
}

func (s *Service) report(err error) {
	if err != nil && s.onError != nil {
		s.onError(err)
	}
}

// Start restores D5 material before any admission, derives each driver's
// initial observation from durable state only, publishes the first view and
// starts one reconciler per eligible driver. It returns the store or restore
// failure; the view stays readable either way and failed drivers report
// FAILED.
func (s *Service) Start(ctx context.Context) error {
	s.lifeMu.Lock()
	if s.started || s.closed {
		s.lifeMu.Unlock()
		return ErrServiceState
	}
	s.started = true
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.lifeMu.Unlock()

	if err := s.start(ctx); err != nil {
		var fatal *startFatal
		if errors.As(err, &fatal) {
			s.lifeMu.Lock()
			s.started = false
			s.cancel()
			s.lifeMu.Unlock()
			return fatal.err
		}
		return err
	}
	return nil
}

// startFatal marks a Start failure after which no reconciler runs and the
// service accepts no decision; Start may be called again.
type startFatal struct{ err error }

func (e *startFatal) Error() string { return e.err.Error() }
func (e *startFatal) Unwrap() error { return e.err }

func (s *Service) start(ctx context.Context) error {
	catalog, err := s.store.Catalog()
	if err != nil || catalog.ReadOnly {
		s.publishWithheld(catalog, err == nil)
		if err == nil {
			err = &Refusal{Code: RefusalFailedPrecondition, Row: 3, Reason: "control state is read-only"}
		}
		return &startFatal{err}
	}
	records := make(map[DriverID]ControlRecord, len(catalog.Drivers))
	for _, rec := range catalog.Drivers {
		records[rec.DriverID] = rec
	}
	if len(records) != len(s.drivers) {
		return &startFatal{fmt.Errorf("%w: service drivers differ from the store's visible drivers", ErrInvalidConfig)}
	}
	for id := range s.drivers {
		if _, ok := records[id]; !ok {
			return &startFatal{fmt.Errorf("%w: driver %s is not visible in the store", ErrInvalidConfig, id)}
		}
	}

	// D5 first: restore before any executor call or admission.
	var restoreErrs []error
	if material := s.store.Recovery(); material != nil {
		if rerr := s.recovery.Restore(ctx, cloneMaterial(material)); rerr != nil {
			affected := material.DriverIDs
			if len(affected) == 0 {
				affected = s.order
			}
			for _, id := range affected {
				if d, ok := s.drivers[id]; ok {
					d.mu.Lock()
					d.recoveryBlocked = true
					d.mu.Unlock()
				}
				restoreErrs = append(restoreErrs, &SemanticError{DriverID: id, Op: "restore", Err: rerr})
			}
		}
	}

	s.viewMu.Lock()
	for _, id := range s.order {
		d := s.drivers[id]
		d.mu.Lock()
		blocked := d.recoveryBlocked
		d.mu.Unlock()
		s.observed[id] = initialObservation(records[id], blocked)
	}
	s.viewMu.Unlock()
	if !s.commitView(s.ctx, nil, true) {
		s.report(fmt.Errorf("%w: initial view not committed", ErrServiceState))
	}

	for _, id := range s.order {
		rec := records[id]
		d := s.drivers[id]
		d.mu.Lock()
		blocked := d.recoveryBlocked
		d.mu.Unlock()
		if rec.Failed || !rec.Enabled || blocked {
			continue
		}
		s.wg.Add(1)
		go s.worker(d)
		s.wakeDriver(d)
	}
	return errors.Join(restoreErrs...)
}

// Close cancels every reconciler and waits for them. Durable intent and
// in-flight work are left exactly as committed.
func (s *Service) Close() {
	s.lifeMu.Lock()
	s.closed = true
	cancel := s.cancel
	s.lifeMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
}

func (s *Service) running() bool {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	return s.started && !s.closed
}

func initialObservation(rec ControlRecord, blocked bool) observation {
	switch {
	case rec.Failed:
		return observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonInternalError}
	case !rec.Enabled:
		return observation{state: drivermanager.ObservedDisabled, reason: drivermanager.ReasonConfigDisabled}
	case blocked:
		return observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonDependencyUnavailable}
	}
	if t := rec.Current; t != nil {
		if t.Kind == KindStop || t.Kind == KindCancel {
			return observation{state: drivermanager.ObservedStopping, reason: drivermanager.ReasonStopRequested}
		}
		return observation{state: drivermanager.ObservedStarting, reason: drivermanager.ReasonStartRequested}
	}
	if rec.DesiredState == DesiredRunning {
		return observation{state: drivermanager.ObservedStarting, reason: drivermanager.ReasonStartRequested}
	}
	return observation{state: drivermanager.ObservedStopped, reason: drivermanager.ReasonNone}
}

// publishWithheld exposes every served driver as FAILED when control state
// cannot be used; a read-only catalog keeps its last-known-good intent.
func (s *Service) publishWithheld(catalog ControlCatalog, readable bool) {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	records := map[DriverID]ControlRecord{}
	if readable {
		for _, rec := range catalog.Drivers {
			records[rec.DriverID] = rec
		}
	}
	view := &Catalog{EventCursor: s.events.cursor()}
	for _, id := range s.order {
		rec := records[id]
		desired := rec.DesiredState
		if desired == "" {
			desired = DesiredStopped
		}
		s.observed[id] = observation{state: drivermanager.ObservedFailed, reason: drivermanager.ReasonInternalError}
		view.Drivers = append(view.Drivers, DriverStatus{
			DriverID: id, Enabled: rec.Enabled, DesiredState: desired, DesiredRevision: rec.DesiredRevision,
			ObservedState: drivermanager.ObservedFailed, Reason: drivermanager.ReasonInternalError,
		})
	}
	s.view.Store(view)
}

// List returns the current coherent cached view. It never waits for a store
// write, a reconciler or a decision.
func (s *Service) List() Catalog {
	return copyCatalog(s.view.Load())
}

// Get returns one entry of the current cached view with its revision, or a
// NOT_FOUND refusal for a noncanonical or unserved ID.
func (s *Service) Get(id DriverID) (DriverStatus, error) {
	if !canonicalDriverID(id) {
		return DriverStatus{}, &Refusal{Code: RefusalNotFound, Row: 2, Reason: "driver not registered"}
	}
	if _, ok := s.drivers[id]; !ok {
		return DriverStatus{}, &Refusal{Code: RefusalNotFound, Row: 2, Reason: "driver not registered"}
	}
	view := s.view.Load()
	for _, entry := range view.Drivers {
		if entry.DriverID == id {
			return entry, nil
		}
	}
	return DriverStatus{DriverID: id, Revision: view.Revision, DesiredState: DesiredStopped,
		ObservedState: drivermanager.ObservedFailed, Reason: drivermanager.ReasonInternalError}, nil
}

// Diagnostics returns a copy of the driver's bounded terminal-summary ring.
func (s *Service) Diagnostics(id DriverID) []Diagnostic {
	d, ok := s.drivers[id]
	if !ok {
		return nil
	}
	d.diagMu.Lock()
	defer d.diagMu.Unlock()
	return append([]Diagnostic(nil), d.diags...)
}

func copyCatalog(view *Catalog) Catalog {
	if view == nil {
		return Catalog{}
	}
	out := *view
	out.Drivers = append([]DriverStatus(nil), view.Drivers...)
	return out
}

// ParseRequest applies closed decoding (refusal-table row 1): exactly the
// members DriverID and ExpectedRevision, both JSON strings, no duplicate or
// additional member, no trailing JSON, and ExpectedRevision a canonical
// decimal string of a nonzero unsigned 64-bit value.
func ParseRequest(data []byte) (Request, error) {
	invalid := func(reason string) (Request, error) {
		return Request{}, &Refusal{Code: RefusalInvalidArgument, Row: 1, Reason: reason}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return invalid("request is not one closed JSON object")
	}
	members := map[string]string{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return invalid("request is not one closed JSON object")
		}
		key, ok := keyTok.(string)
		if !ok {
			return invalid("request is not one closed JSON object")
		}
		if _, dup := members[key]; dup {
			return invalid("duplicate request member")
		}
		valueTok, err := dec.Token()
		if err != nil {
			return invalid("request is not one closed JSON object")
		}
		value, ok := valueTok.(string)
		if !ok {
			return invalid("request members must be strings")
		}
		members[key] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return invalid("request is not one closed JSON object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("trailing data after the request object")
	}
	id, okID := members["DriverID"]
	revText, okRev := members["ExpectedRevision"]
	if len(members) != 2 || !okID || !okRev {
		return invalid("request must contain exactly DriverID and ExpectedRevision")
	}
	rev, ok := parseRevision(revText)
	if !ok {
		return invalid("ExpectedRevision must be a canonical nonzero decimal string")
	}
	return Request{DriverID: DriverID(id), ExpectedRevision: rev}, nil
}

func parseRevision(text string) (DesiredRevision, bool) {
	if len(text) == 0 || len(text) > 20 || (len(text) > 1 && text[0] == '0') {
		return 0, false
	}
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseUint(text, 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return DesiredRevision(v), true
}

// Control decodes a closed request body and decides it.
func (s *Service) Control(ctx context.Context, method Method, body []byte) (DesiredView, error) {
	if !validMethod(method) {
		return DesiredView{}, &Refusal{Code: RefusalInvalidArgument, Row: 1, Reason: "unknown method"}
	}
	req, err := ParseRequest(body)
	if err != nil {
		return DesiredView{}, err
	}
	return s.Decide(ctx, method, req)
}

func validMethod(method Method) bool {
	return method == MethodStart || method == MethodStop || method == MethodRestart
}

// Decide validates the method, request shape and registration (rows 1 and
// 2) without the store, then lets the store apply rows 3 to 10 under its
// namespace-wide serialization point. An accepted changing decision is
// durable before the call returns, and only then is the view published, the
// event emitted, an untyped or superseded attempt fenced and the reconciler
// woken. A same-desired no-op writes, publishes, emits and wakes nothing.
// Cancelling ctx after the store accepted cannot retract the decision: the
// reconciler runs the persisted work under the service's own lifetime.
func (s *Service) Decide(ctx context.Context, method Method, req Request) (DesiredView, error) {
	if !validMethod(method) || req.ExpectedRevision == 0 {
		return DesiredView{}, &Refusal{Code: RefusalInvalidArgument, Row: 1, Reason: "invalid method or expected revision"}
	}
	d, ok := s.drivers[req.DriverID]
	if !canonicalDriverID(req.DriverID) || !ok {
		return DesiredView{}, &Refusal{Code: RefusalNotFound, Row: 2, Reason: "driver id noncanonical or not registered"}
	}
	if !s.running() {
		return DesiredView{}, &Refusal{Code: RefusalUnavailable, Row: 3, Reason: "control service not running"}
	}
	if !s.store.AdmissionOpen() {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && s.store.ResolveReadback(ctx) == nil && s.store.AdmissionOpen() {
			// The readback adopted or rejected an ambiguous commit: expose the
			// committed state and let every reconciler re-read its slots.
			s.commitView(s.ctx, nil, false)
			for _, id := range s.order {
				s.wakeDriver(s.drivers[id])
			}
		}
	}
	var planned, fence bool
	view, err := s.store.Decide(ctx, method, req, s.planner(d, &planned, &fence))
	changed := err == nil && view.DesiredRevision != req.ExpectedRevision
	if planned {
		d.mu.Lock()
		if fence && changed {
			for _, a := range []*attempt{d.untyped, d.inflight} {
				if a != nil {
					a.fenced = true
					a.cancel()
				}
			}
		}
		d.deciding--
		d.cond.Broadcast()
		d.mu.Unlock()
	}
	if err != nil {
		return DesiredView{}, err
	}
	if !changed {
		return view, nil
	}
	d.mu.Lock()
	corr := d.corr
	d.mu.Unlock()
	s.viewMu.Lock()
	obs := s.observed[d.id]
	s.viewMu.Unlock()
	s.commitView(s.ctx, &Event{Kind: EventDesiredAccepted, DriverID: d.id,
		DesiredState: view.DesiredState, DesiredRevision: view.DesiredRevision,
		ObservedState: obs.state, Reason: obs.reason, SafetyQuarantined: obs.quarantined,
		Epoch: corr.Epoch, Generation: corr.Generation}, false)
	s.wakeDriver(d)
	return view, nil
}

// planner places a changing request in the typed slots. It runs under the
// store's serialization point and takes only the driver's own mutex.
func (s *Service) planner(d *driverState, planned, fence *bool) Planner {
	return func(in PlanInput) (Plan, *Refusal) {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.pruning && in.Current != nil {
			// The prune of this terminal slot has not committed yet and would
			// clear a slot reserved now. Once it committed, the planner sees
			// no current slot and proceeds.
			return Plan{}, &Refusal{Code: RefusalTransitionBusy, Row: 8, Reason: "terminal work slot is being pruned"}
		}
		if in.Method != MethodStop && (d.quarantined || d.recoveryBlocked) {
			return Plan{}, &Refusal{Code: RefusalFailedPrecondition, Row: 7, Reason: "quarantine or unresolved recovery blocks admission"}
		}
		next := &Transition{Kind: TransitionKind(in.Method), Phase: PhasePending}
		var plan Plan
		fenceIt := false
		switch {
		case in.Current == nil:
			if a := d.untyped; a != nil && in.Method != MethodStart && !a.fenced {
				// An untyped STARTING or BACKOFF attempt (process-start
				// reconciliation): synthesize its cancellation as the current
				// slot with its correlation, obligations and budget.
				plan = Plan{
					Current: &Transition{
						Kind: KindCancel, Phase: PhasePending, Epoch: a.corr.Epoch, Generation: a.corr.Generation,
						NativeClosePending: true, SemanticCleanupPending: true,
						DeadlineUTC: s.now().Add(s.budget).UTC(),
					},
					Reserved: next,
				}
				fenceIt = true
			} else {
				plan = Plan{Current: next}
			}
		case in.Reserved != nil || (in.Current.Kind == KindRestart && !terminal(in.Current)):
			return Plan{}, &Refusal{Code: RefusalTransitionBusy, Row: 8, Reason: "typed transition slots are occupied"}
		default:
			plan = Plan{Current: cloneTransition(in.Current), Reserved: next}
			fenceIt = in.Current.Kind == KindStart && in.Method != MethodStart
		}
		if err := validatePlan(in, plan); err != nil {
			return Plan{}, &Refusal{Code: RefusalTransitionBusy, Row: 8, Reason: err.Error()}
		}
		*planned = true
		*fence = fenceIt
		d.deciding++
		return plan, nil
	}
}

// validatePlan checks that an accepted changing request enqueues exactly one
// new transition of the requested kind, keeps every stored transition
// unchanged and never leaves a reserved slot without a current slot. A
// synthesized cancellation is the only other new transition allowed, and only
// in the current slot ahead of the requested one.
func validatePlan(in PlanInput, plan Plan) error {
	want := TransitionKind(in.Method)
	if plan.Reserved != nil && plan.Current == nil {
		return fmt.Errorf("%w: reserved slot without current slot", ErrInvalidWork)
	}
	if in.Current != nil && (plan.Current == nil || *plan.Current != *in.Current) {
		return fmt.Errorf("%w: stored current transition not retained", ErrInvalidWork)
	}
	if in.Reserved != nil && (plan.Reserved == nil || *plan.Reserved != *in.Reserved) {
		return fmt.Errorf("%w: stored reserved transition not retained", ErrInvalidWork)
	}
	requested, others := 0, 0
	for i, t := range []*Transition{plan.Current, plan.Reserved} {
		if t == nil || t.Sequence != 0 {
			continue
		}
		switch {
		case t.Kind == want && t.Phase == PhasePending && t.DesiredRevision == 0:
			requested++
		case t.Kind == KindCancel && i == 0 && in.Current == nil && plan.Reserved != nil:
			others++
		default:
			return fmt.Errorf("%w: unexpected new transition %s", ErrInvalidWork, t.Kind)
		}
	}
	if requested != 1 {
		return fmt.Errorf("%w: plan must enqueue exactly one %s transition", ErrInvalidWork, want)
	}
	if others == 1 && (plan.Reserved == nil || plan.Reserved.Kind != want || plan.Reserved.Sequence != 0) {
		return fmt.Errorf("%w: cancellation must precede the requested transition", ErrInvalidWork)
	}
	return nil
}

// terminal reports a slot whose work and cleanup obligations are settled and
// which only awaits pruning.
func terminal(t *Transition) bool {
	return t.Phase == PhaseCleanup && !t.NativeClosePending && !t.SemanticCleanupPending
}

func (s *Service) wakeDriver(d *driverState) {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// stageObservation records the driver's new observation for the next view.
// It reports whether the observation changed.
func (s *Service) stageObservation(id DriverID, obs observation) bool {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	if s.observed[id] == obs {
		return false
	}
	s.observed[id] = obs
	return true
}

// observe stages an observed transition, durably advances the catalog
// revision and publishes the view; the event follows the commit. Observed
// transitions never touch desired state or its revision.
func (s *Service) observe(d *driverState, obs observation) {
	d.mu.Lock()
	obs.epoch, obs.generation = d.corr.Epoch, d.corr.Generation
	d.mu.Unlock()
	if s.stageObservation(d.id, obs) {
		s.commitObserved(d, obs)
	}
}

func (s *Service) commitObserved(d *driverState, obs observation) {
	s.commitView(s.ctx, &Event{Kind: EventObservedChanged, DriverID: d.id, ObservedState: obs.state,
		Reason: obs.reason, SafetyQuarantined: obs.quarantined, Epoch: obs.epoch, Generation: obs.generation}, true)
}

// commitView publishes a view whose revision is durable. With advance set it
// first advances the catalog revision; if a concurrent publisher already
// exposed a different view at the same revision it advances again, within a
// small bound. It returns whether the view (and event) was published. A
// failed publication is never dropped: the event is retained in order and a
// bounded-delay retry republishes the view, so a staged transition cannot
// stay absent from List when no later action occurs.
func (s *Service) commitView(ctx context.Context, event *Event, advance bool) bool {
	for i := 0; i < maxPublishAttempts; i++ {
		if advance || i > 0 {
			if ctx.Err() != nil {
				break
			}
			if _, err := s.store.AdvanceCatalog(ctx); err != nil {
				s.report(err)
				break
			}
		}
		if s.publish(event) {
			return true
		}
	}
	s.deferPublication(event)
	return false
}

// deferPublication retains an unpublished event and arms one retry timer.
func (s *Service) deferPublication(event *Event) {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	if event != nil {
		if len(s.pendingEv) == maxPendingEvents {
			copy(s.pendingEv, s.pendingEv[1:])
			s.pendingEv = s.pendingEv[:maxPendingEvents-1]
		}
		s.pendingEv = append(s.pendingEv, *event)
	}
	s.dirty = true
	ctx := s.ctx
	if s.retryArmed || ctx == nil || ctx.Err() != nil {
		return
	}
	s.retryArmed = true
	time.AfterFunc(s.retryDelay(1), func() {
		s.viewMu.Lock()
		s.retryArmed = false
		s.viewMu.Unlock()
		if ctx.Err() != nil {
			return
		}
		s.commitView(ctx, nil, true)
	})
}

// publish rebuilds the view from one coherent store read plus the staged
// observations. A view is exposed only at a strictly higher revision, or
// re-exposed unchanged at the same revision, so content at a revision never
// changes once exposed. Events are appended only after the view that makes
// them visible is stored, so a reader woken by an event always finds a list
// at least as new as the event's catalog revision.
func (s *Service) publish(event *Event) bool {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	catalog, err := s.store.Catalog()
	if err != nil || catalog.ReadOnly {
		return false
	}
	built := s.buildLocked(catalog)
	current := s.view.Load()
	switch {
	case built.Revision > current.Revision:
	case built.Revision == current.Revision && reflect.DeepEqual(built.Drivers, current.Drivers):
	default:
		return false
	}
	emit := append([]Event(nil), s.pendingEv...)
	if event != nil {
		emit = append(emit, *event)
	}
	for i := range emit {
		e := &emit[i]
		e.CatalogRevision = built.Revision
		for _, entry := range built.Drivers {
			if entry.DriverID == e.DriverID && e.Kind == EventObservedChanged {
				e.DesiredState = entry.DesiredState
				e.DesiredRevision = entry.DesiredRevision
			}
		}
	}
	built.EventCursor = s.events.cursor() + uint64(len(emit))
	s.view.Store(&built)
	for _, e := range emit {
		s.events.append(e)
	}
	s.pendingEv, s.dirty = nil, false
	return true
}

func (s *Service) buildLocked(catalog ControlCatalog) Catalog {
	records := make(map[DriverID]ControlRecord, len(catalog.Drivers))
	for _, rec := range catalog.Drivers {
		records[rec.DriverID] = rec
	}
	view := Catalog{Revision: catalog.CatalogRevision, Drivers: make([]DriverStatus, 0, len(s.order))}
	for _, id := range s.order {
		rec, ok := records[id]
		obs := s.observed[id]
		entry := DriverStatus{DriverID: id, Revision: catalog.CatalogRevision, DesiredState: DesiredStopped,
			ObservedState: obs.state, Reason: obs.reason, SafetyQuarantined: obs.quarantined}
		if ok {
			entry.Enabled = rec.Enabled
			entry.DesiredState = rec.DesiredState
			entry.DesiredRevision = rec.DesiredRevision
			if rec.Failed {
				entry.ObservedState, entry.Reason = drivermanager.ObservedFailed, drivermanager.ReasonInternalError
			}
		}
		if entry.ObservedState == "" {
			entry.ObservedState, entry.Reason = drivermanager.ObservedFailed, drivermanager.ReasonInternalError
		}
		view.Drivers = append(view.Drivers, entry)
	}
	return view
}

func (s *Service) appendDiagnostic(d *driverState, t Transition, outcome string) {
	summary := fmt.Sprintf("kind=%s outcome=%s desired_revision=%d attempts_used=%d", t.Kind, outcome, t.DesiredRevision, t.AttemptsUsed)
	if len(summary) > MaxDiagnosticBytes {
		summary = summary[:MaxDiagnosticBytes]
	}
	d.diagMu.Lock()
	defer d.diagMu.Unlock()
	if len(d.diags) == MaxDiagnostics {
		copy(d.diags, d.diags[1:])
		d.diags = d.diags[:MaxDiagnostics-1]
	}
	d.diags = append(d.diags, Diagnostic{Kind: t.Kind, DesiredRevision: t.DesiredRevision, Outcome: outcome,
		AttemptsUsed: t.AttemptsUsed, Summary: summary})
}

func cloneTransition(t *Transition) *Transition {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func cloneMaterial(m *RecoveryMaterial) *RecoveryMaterial {
	if m == nil {
		return nil
	}
	return &RecoveryMaterial{
		DriverIDs:          append([]DriverID(nil), m.DriverIDs...),
		MaxDesiredRevision: m.MaxDesiredRevision,
		Payload:            append([]byte(nil), m.Payload...),
	}
}
