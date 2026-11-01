// Package libraryhost owns the lifecycle of an optional in-process Matter
// library behind injected ports. It imports no SDK and allocates nothing while
// disabled. Qualification rows are an evidence boundary, not an authorization
// gate, and passing them here establishes no conformance or runtime claim.
package libraryhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var (
	// ErrUnavailable matches every UnavailableError: the option is enabled but
	// nothing was launched.
	ErrUnavailable = errors.New("matter library host unavailable")
	// ErrQuiesce reports a stop that did not drain and release within its budget.
	ErrQuiesce = errors.New("matter library host did not quiesce")
	// ErrQuarantined refuses a start while an old generation still holds resources.
	ErrQuarantined = errors.New("matter library host is quarantined by an unfinished stop")
	// ErrStaleCallback rejects work that does not belong to the running generation.
	ErrStaleCallback = errors.New("matter library callback is stale")
	// ErrGenerationChanged withholds a projection whose source moved after
	// qualification; only a new start can qualify the new generations.
	ErrGenerationChanged = errors.New("matter library source generations changed")
	// ErrSnapshotUnavailable reports a failed shared snapshot capture.
	ErrSnapshotUnavailable = errors.New("matter library snapshot is unavailable")
)

// Reason is a categorical cause. Port errors are not retained, so a reason
// carries no endpoint, path or other configuration detail.
type Reason string

const (
	ReasonSelectionIncomplete Reason = "selection_incomplete"
	ReasonFactoryMissing      Reason = "factory_missing"
	ReasonQualifierMissing    Reason = "qualifier_missing"
	ReasonSnapshotMissing     Reason = "snapshot_port_missing"
	ReasonTokenFailed         Reason = "lifecycle_token_failed"
	ReasonTokenReused         Reason = "lifecycle_token_reused"
	ReasonSnapshotFailed      Reason = "snapshot_failed"
	ReasonGenerationsInvalid  Reason = "generations_invalid"
	ReasonQualifierFailed     Reason = "qualifier_failed"
	ReasonEvidenceAbsent      Reason = "evidence_absent"
	ReasonEvidenceIncomplete  Reason = "evidence_incomplete"
	ReasonEvidenceAmbiguous   Reason = "evidence_ambiguous"
	ReasonEvidenceNotPassed   Reason = "evidence_not_passed"
	ReasonSelectionMismatch   Reason = "evidence_selection_mismatch"
	ReasonGenerationMismatch  Reason = "evidence_generation_mismatch"
	ReasonTokenMismatch       Reason = "evidence_token_mismatch"
	ReasonOriginMismatch      Reason = "evidence_origin_mismatch"
	ReasonCancelled           Reason = "start_cancelled"
	ReasonFactoryFailed       Reason = "factory_failed"
)

// UnavailableError is the fail-closed outcome of an enabled start.
type UnavailableError struct{ Reason Reason }

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("%v: %s", ErrUnavailable, e.Reason)
}

func (e *UnavailableError) Is(target error) bool { return target == ErrUnavailable }

// Selection names the exact identities every evidence row must describe.
type Selection struct {
	Tag      string
	Target   string
	Profile  string
	Evidence string
}

func (s Selection) complete() bool {
	return s.Tag != "" && s.Target != "" && s.Profile != "" && s.Evidence != ""
}

// Generations are independent checks; none implies another.
type Generations struct {
	Source       uint64
	Binding      uint64
	NativeDriver uint64
}

func (g Generations) valid() bool {
	return g.Source != 0 && g.Binding != 0 && g.NativeDriver != 0
}

// Stamp identifies one started generation. Callbacks must present all of it.
type Stamp struct {
	Selection   Selection
	Generations Generations
	Epoch       uint64
	Token       string
}

// BindingClass separates a synthetic fixture from a production binding, so
// evidence for one can never launch the other.
type BindingClass string

const (
	ClassSyntheticFixture BindingClass = "synthetic-fixture"
	ClassProduction       BindingClass = "production"
)

// RowKind names one required evidence row.
type RowKind string

const (
	RowTag       RowKind = "tag"
	RowBuild     RowKind = "build"
	RowRuntime   RowKind = "runtime"
	RowProfile   RowKind = "profile"
	RowAdmission RowKind = "admission"
)

var requiredRows = [...]RowKind{RowTag, RowBuild, RowRuntime, RowProfile, RowAdmission}

// Verdict is the outcome of one row. Its zero value and Unknown do not pass.
type Verdict string

const (
	VerdictPass    Verdict = "pass"
	VerdictFail    Verdict = "fail"
	VerdictUnknown Verdict = "unknown"
)

// Row is one retained evidence result for the exact request it answers.
type Row struct {
	Kind        RowKind
	Verdict     Verdict
	Origin      BindingClass
	Selection   Selection
	Generations Generations
	Token       string
}

// Request asks for the rows that apply to one lifecycle token.
type Request struct {
	Selection   Selection
	Generations Generations
	Token       string
}

// Qualifier supplies retained evidence and the binding's admission decision.
type Qualifier interface {
	Qualify(context.Context, Request) ([]Row, error)
}

// Capture is one detached snapshot and evaluation with its current generations.
type Capture struct {
	Generations Generations
	Snapshot    semreg.Snapshot
	Evaluation  semreg.EvaluationView
}

// SnapshotPort is the shared semantic source; it keeps no second truth store.
type SnapshotPort interface {
	Capture(context.Context) (Capture, error)
}

// Binding is one launched library. Close releases its resources after the
// generation has drained and must return when its context ends.
type Binding interface {
	Close(context.Context) error
}

// BindingFactory constructs a binding. The context bounds construction only;
// long-lived work belongs to Session.Go.
type BindingFactory interface {
	Class() BindingClass
	New(context.Context, *Session) (Binding, error)
}

// Config has a default-false option: its zero value launches nothing.
type Config struct {
	Enabled   bool
	Selection Selection
	Factory   BindingFactory
	Qualifier Qualifier
	Snapshots SnapshotPort
	NewToken  func() (string, error)
	// CleanupBudget bounds the teardown of a binding built under a start that
	// was cancelled; zero selects DefaultCleanupBudget.
	CleanupBudget time.Duration
}

// DefaultCleanupBudget is the teardown allowance after a cancelled start.
const DefaultCleanupBudget = 10 * time.Second

// State is the host's contract state.
type State string

const (
	StateDisabled    State = "disabled"
	StateUnavailable State = "unavailable"
	StateStarting    State = "starting"
	StateRunning     State = "running"
	StateStopping    State = "stopping"
	StateStopped     State = "stopped"
	StateQuarantined State = "quarantined"
)

// Status counts what the host still owns. A fixture class is reported so that a
// fixture-qualified generation cannot be read as production admission.
type Status struct {
	State         State
	Reason        Reason
	Class         BindingClass
	Epoch         uint64
	OwnedWork     int
	Resources     int
	StaleRejected uint64
}

type generation struct {
	stamp   Stamp
	class   BindingClass
	ctx     context.Context
	cancel  context.CancelFunc
	owned   int
	fenced  bool
	drained chan struct{}
	done    bool
	binding Binding
	closing chan closeResult
}

// closeResult carries one Close attempt and the context that bounded it.
type closeResult struct {
	err error
	ctx context.Context
}

// Host fences one binding generation at a time. Start, Stop and Restart are
// serialized; callbacks use the state lock only.
type Host struct {
	cfg Config
	ops chan struct{}

	mu      sync.Mutex
	state   State
	reason  Reason
	epoch   uint64
	issued  map[string]struct{}
	current *generation
	stale   uint64

	// stopGen counts Stop calls; cancelStart aborts the start in flight. Both
	// let Stop fence a start that holds the operation lock.
	stopGen     uint64
	cancelStart context.CancelFunc
}

// New touches no port.
func New(cfg Config) *Host {
	state := StateDisabled
	if cfg.Enabled {
		state = StateStopped
	}
	return &Host{cfg: cfg, ops: make(chan struct{}, 1), state: state}
}

func (h *Host) acquire(ctx context.Context) error {
	select {
	case h.ops <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) release() { <-h.ops }

// Status returns the current counts.
func (h *Host) Status() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	status := Status{State: h.state, Reason: h.reason, Epoch: h.epoch, StaleRejected: h.stale}
	if g := h.current; g != nil {
		status.Class = g.class
		status.OwnedWork = g.owned
		if g.binding != nil {
			status.Resources = 1
		}
	}
	return status
}

// Start launches when every row passes. It is desired-state idempotent, a
// disabled host returns nil, and a missed row returns an UnavailableError.
func (h *Host) Start(ctx context.Context) error {
	entry := h.stopGeneration()
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer h.release()
	return h.fencedStart(ctx, entry)
}

func (h *Host) stopGeneration() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stopGen
}

// fencedStart runs start under a context that Stop cancels. A Stop issued
// after entry, even one that timed out waiting for the lock, cancels it.
func (h *Host) fencedStart(ctx context.Context, entry uint64) error {
	startCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.mu.Lock()
	if h.stopGen != entry {
		h.mu.Unlock()
		return &UnavailableError{Reason: ReasonCancelled}
	}
	h.cancelStart = cancel
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.cancelStart = nil
		h.mu.Unlock()
	}()
	return h.start(startCtx)
}

// Stop fences, drains and releases within the context's budget. It is
// idempotent, and after ErrQuiesce a further call resumes the same teardown.
func (h *Host) Stop(ctx context.Context) error {
	h.mu.Lock()
	h.stopGen++
	if h.cancelStart != nil {
		h.cancelStart()
	}
	h.mu.Unlock()
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer h.release()
	h.mu.Lock()
	g := h.current
	h.mu.Unlock()
	if g == nil {
		return nil
	}
	return h.teardown(ctx, g)
}

// Restart always executes: a fresh epoch and token are re-qualified, and no
// start is attempted until the previous generation has been released.
func (h *Host) Restart(ctx context.Context) error {
	entry := h.stopGeneration()
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer h.release()
	h.mu.Lock()
	g := h.current
	h.mu.Unlock()
	if g != nil {
		if err := h.teardown(ctx, g); err != nil {
			return err
		}
	}
	return h.fencedStart(ctx, entry)
}

func (h *Host) start(ctx context.Context) error {
	if !h.cfg.Enabled {
		return nil
	}
	h.mu.Lock()
	g := h.current
	state := h.state
	h.mu.Unlock()
	if g != nil {
		if state == StateQuarantined {
			return ErrQuarantined
		}
		return nil
	}
	switch {
	case !h.cfg.Selection.complete():
		return h.unavailable(ReasonSelectionIncomplete)
	case h.cfg.Factory == nil:
		return h.unavailable(ReasonFactoryMissing)
	case h.cfg.Qualifier == nil:
		return h.unavailable(ReasonQualifierMissing)
	case h.cfg.Snapshots == nil:
		return h.unavailable(ReasonSnapshotMissing)
	}
	token, reason := h.nextToken()
	if reason != "" {
		return h.unavailable(reason)
	}
	captured, err := h.cfg.Snapshots.Capture(ctx)
	if err != nil {
		return h.unavailable(ReasonSnapshotFailed)
	}
	if !captured.Generations.valid() {
		return h.unavailable(ReasonGenerationsInvalid)
	}
	request := Request{Selection: h.cfg.Selection, Generations: captured.Generations, Token: token}
	rows, err := h.cfg.Qualifier.Qualify(ctx, request)
	if err != nil {
		return h.unavailable(ReasonQualifierFailed)
	}
	if reason := checkRows(rows, request); reason != "" {
		return h.unavailable(reason)
	}
	class := h.cfg.Factory.Class()
	if reason := checkOrigin(rows, class); reason != "" {
		return h.unavailable(reason)
	}
	// Qualification can outlast a source, binding or driver change: fence the
	// generations again immediately before anything is allocated.
	latest, err := h.cfg.Snapshots.Capture(ctx)
	if err != nil {
		return h.unavailable(ReasonSnapshotFailed)
	}
	if latest.Generations != request.Generations {
		return h.unavailable(ReasonGenerationMismatch)
	}
	if ctx.Err() != nil {
		return h.unavailable(ReasonCancelled)
	}
	return h.launch(ctx, request, class)
}

func (h *Host) nextToken() (string, Reason) {
	newToken := h.cfg.NewToken
	if newToken == nil {
		newToken = randomToken
	}
	token, err := newToken()
	if err != nil || token == "" {
		return "", ReasonTokenFailed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, reused := h.issued[token]; reused {
		return "", ReasonTokenReused
	}
	if h.issued == nil {
		h.issued = make(map[string]struct{})
	}
	h.issued[token] = struct{}{}
	return token, ""
}

func randomToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func checkRows(rows []Row, request Request) Reason {
	if len(rows) == 0 {
		return ReasonEvidenceAbsent
	}
	seen := make(map[RowKind]struct{}, len(rows))
	for _, row := range rows {
		if _, duplicate := seen[row.Kind]; duplicate {
			return ReasonEvidenceAmbiguous
		}
		seen[row.Kind] = struct{}{}
	}
	if len(seen) > len(requiredRows) {
		return ReasonEvidenceAmbiguous
	}
	for _, kind := range requiredRows {
		if _, ok := seen[kind]; !ok {
			return ReasonEvidenceIncomplete
		}
	}
	for _, row := range rows {
		switch {
		case row.Verdict != VerdictPass:
			return ReasonEvidenceNotPassed
		case row.Selection != request.Selection:
			return ReasonSelectionMismatch
		case row.Generations != request.Generations:
			return ReasonGenerationMismatch
		case row.Token != request.Token:
			return ReasonTokenMismatch
		}
	}
	return ""
}

func checkOrigin(rows []Row, class BindingClass) Reason {
	if class != ClassSyntheticFixture && class != ClassProduction {
		return ReasonOriginMismatch
	}
	for _, row := range rows {
		if row.Origin != class {
			return ReasonOriginMismatch
		}
	}
	return ""
}

func (h *Host) launch(ctx context.Context, request Request, class BindingClass) error {
	lifecycle, cancel := context.WithCancel(context.Background())
	g := &generation{class: class, ctx: lifecycle, cancel: cancel, drained: make(chan struct{})}
	h.mu.Lock()
	h.epoch++
	g.stamp = Stamp{Selection: request.Selection, Generations: request.Generations, Epoch: h.epoch, Token: request.Token}
	h.current = g
	h.state = StateStarting
	h.reason = ""
	h.mu.Unlock()

	binding, err := h.cfg.Factory.New(ctx, &Session{host: h, gen: g})
	if err != nil || binding == nil {
		if binding != nil {
			// A partially constructed binding still owns resources: teardown closes it.
			h.mu.Lock()
			g.binding = binding
			h.mu.Unlock()
		}
		cleanup, release := h.cleanupContext(ctx)
		defer release()
		if teardownErr := h.teardown(cleanup, g); teardownErr != nil {
			return teardownErr
		}
		return h.unavailable(ReasonFactoryFailed)
	}
	if ctx.Err() != nil {
		// The start was cancelled while the factory ran: do not publish a running binding.
		h.mu.Lock()
		g.binding = binding
		h.mu.Unlock()
		// The cancelled context cannot carry the teardown: use a finite budget and
		// quarantine on expiry so Start never hangs under the lifecycle lock.
		budget := h.cfg.CleanupBudget
		if budget <= 0 {
			budget = DefaultCleanupBudget
		}
		cleanup, release := context.WithTimeout(context.WithoutCancel(ctx), budget)
		defer release()
		if teardownErr := h.teardown(cleanup, g); teardownErr != nil {
			return teardownErr
		}
		return h.unavailable(ReasonCancelled)
	}
	h.mu.Lock()
	g.binding = binding
	h.state = StateRunning
	h.mu.Unlock()
	return nil
}

// cleanupContext gives a start-time teardown its own finite budget, independent
// of a start context that may already be cancelled or expired.
func (h *Host) cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	budget := h.cfg.CleanupBudget
	if budget <= 0 {
		budget = DefaultCleanupBudget
	}
	return context.WithTimeout(context.WithoutCancel(ctx), budget)
}

func (h *Host) unavailable(reason Reason) error {
	h.mu.Lock()
	h.state = StateUnavailable
	h.reason = reason
	h.mu.Unlock()
	return &UnavailableError{Reason: reason}
}

// teardown fences before it waits and drains before it releases. A generation
// that misses its budget stays current, fenced and quarantined.
func (h *Host) teardown(ctx context.Context, g *generation) error {
	h.mu.Lock()
	g.fenced = true
	h.state = StateStopping
	g.cancel()
	h.finishDrainLocked(g)
	h.mu.Unlock()

	select {
	case <-g.drained:
	default:
		select {
		case <-g.drained:
		case <-ctx.Done():
			return h.quarantine(ctx.Err())
		}
	}
	if g.binding != nil {
		for done := false; !done; {
			if g.closing == nil {
				closing := make(chan closeResult, 1)
				g.closing = closing
				binding := g.binding
				go func() { closing <- closeResult{err: binding.Close(ctx), ctx: ctx} }()
			}
			select {
			case res := <-g.closing:
				g.closing = nil
				if res.err != nil {
					if res.ctx.Err() != nil && ctx.Err() == nil && res.ctx != ctx {
						// The error came from an expired earlier budget: retry Close
						// under the current one instead of quarantining again.
						continue
					}
					return h.quarantine(res.err)
				}
				done = true
			case <-ctx.Done():
				return h.quarantine(ctx.Err())
			}
		}
	}
	h.mu.Lock()
	g.binding = nil
	h.current = nil
	h.state = StateStopped
	h.reason = ""
	h.mu.Unlock()
	return nil
}

func (h *Host) quarantine(cause error) error {
	h.mu.Lock()
	h.state = StateQuarantined
	h.mu.Unlock()
	return fmt.Errorf("%w: %w", ErrQuiesce, cause)
}

func (h *Host) finishDrainLocked(g *generation) {
	if g.fenced && g.owned == 0 && !g.done {
		g.done = true
		close(g.drained)
	}
}

func (h *Host) admit(g *generation, stamp Stamp) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if g.fenced || stamp != g.stamp {
		h.stale++
		return ErrStaleCallback
	}
	g.owned++
	return nil
}

func (h *Host) settle(g *generation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	g.owned--
	h.finishDrainLocked(g)
}

// Session is the fenced route from one binding generation back into the host.
type Session struct {
	host *Host
	gen  *generation
}

// Stamp returns the identity callbacks of this generation must present.
func (s *Session) Stamp() Stamp { return s.gen.stamp }

// Go runs an owned task that stop cancels and joins.
func (s *Session) Go(task func(context.Context)) error {
	if err := s.host.admit(s.gen, s.gen.stamp); err != nil {
		return err
	}
	go func() {
		defer s.host.settle(s.gen)
		task(s.gen.ctx)
	}()
	return nil
}

// Deliver runs one callback only when its session and full stamp are current.
// A rejected callback runs nothing and changes no generation state.
func (s *Session) Deliver(stamp Stamp, callback func(context.Context) error) error {
	if err := s.host.admit(s.gen, stamp); err != nil {
		return err
	}
	defer s.host.settle(s.gen)
	return callback(s.gen.ctx)
}

// Project reads the shared port through the AA03 projector. The host keeps no
// result, so a withheld read leaves last-known-good state with its owner and a
// retained value can never become fresh here.
func (s *Session) Project(ctx context.Context) (matter.Result161, error) {
	if err := s.host.admit(s.gen, s.gen.stamp); err != nil {
		return matter.Result161{}, err
	}
	defer s.host.settle(s.gen)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(s.gen.ctx, cancel)()

	captured, err := s.host.cfg.Snapshots.Capture(ctx)
	if err != nil {
		if s.gen.ctx.Err() != nil {
			return matter.Result161{}, ErrStaleCallback
		}
		return matter.Result161{}, fmt.Errorf("%w: %w", ErrSnapshotUnavailable, err)
	}
	if captured.Generations != s.gen.stamp.Generations {
		return matter.Result161{}, ErrGenerationChanged
	}
	result, err := matter.Project161(captured.Snapshot, captured.Evaluation)
	if err != nil {
		return matter.Result161{}, err
	}
	if s.gen.ctx.Err() != nil {
		return matter.Result161{}, ErrStaleCallback
	}
	return result, nil
}
