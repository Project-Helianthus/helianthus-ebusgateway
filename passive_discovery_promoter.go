package ebusgateway

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// maxConfirmationAttempts caps the number of active confirmation
// attempts the promoter makes against a single passive-promoted
// address before demoting the candidate from the EvidenceBuffer's
// promoted set. Without this cap, a stale or spoofed promotion
// would retry on every backoff cycle for the gateway's lifetime,
// wasting bus arbitration slots and accumulating dead state.
//
// Sized at 5 to allow for transient bus contention or temporarily
// non-responsive devices to recover within a single backoff plateau
// (~5min cumulative). Demoted candidates can be re-promoted by
// fresh passive evidence.
const maxConfirmationAttempts = 5

// perCandidateConfirmationBudget caps a single ConfirmB524Coherent
// invocation. The B524 capability probe runs len(b524CapabilityProbes)
// (=2) probes; each probeB524Register retries up to 3 times with a 5s
// per-attempt floor (minB524ProbeTimeout) plus 200ms inter-attempt
// backoff. Worst-case per probe: 3 * (5s + 200ms) ≈ 16s; worst-case
// per coherency check: 2 * 16s ≈ 32s.
//
// A tighter cap (e.g. tickInterval/2 = 15s) would expire mid-retry on
// busy adapter-direct buses, falsely demoting late-arrival devices
// after maxConfirmationAttempts ticks even though the bus contention
// would have resolved within the built-in retry budget. The 45s
// budget below adds a ~40% safety margin over the worst-case sum so
// transient contention cannot starve a reachable candidate.
//
// processOnce derives the actual per-candidate timeout as the lesser
// of perCandidateConfirmationBudget and tickInterval — a tickInterval
// shorter than the budget (e.g. tests using millisecond ticks) is
// honored, but normal operation gets the full budget.
const perCandidateConfirmationBudget = 45 * time.Second

// PassiveDiscoveryPromoter is the runtime-phase counterpart to the
// startup directed-probe scan. It bridges the gap where a Vaillant
// device — typically the regulator — boots after the gateway and
// therefore never appears in the directed startup probe target list.
//
// Pipeline:
//
//  1. The bus_observability store records passive evidence per
//     observed address into its EvidenceBuffer. Once an address
//     crosses the buffer's promotion threshold (>=2 weak observations
//     OR >=1 strong observation; strong = B524 / B509 ScanID
//     response), it is exposed via PromotedAddresses().
//
//  2. The promoter periodically polls PromotedAddresses(), filters
//     candidates already in the registry / equal to the admitted
//     source / outside the responder range, and runs bounded active
//     confirmation against each remaining candidate using the existing
//     B524 capability probe.
//
//  3. On confirmation: a minimal Vaillant entry is registered, router
//     planes are refreshed (so live event routing reflects the new
//     device), and the semantic poller's discovery refresh is
//     enqueued so the regulator surface populates without a gateway
//     restart.
//
// Source-address invariant: active confirmation always sources from
// the gateway's admitted source. The promoter never overrides the
// admitted source under any condition.
//
// Rate limiting: per-address attempt counter with exponential
// backoff via RejoinBackoffSchedule. A failed confirmation
// schedules the next attempt at backoff(attempt) with a jittered
// floor; success clears the per-address state.
type PassiveDiscoveryPromoter struct {
	registry          *registry.DeviceRegistry
	evidenceBuf       *EvidenceBuffer
	confirmFn         func(ctx context.Context, target byte) bool
	registerFn        func(target byte)
	semanticRefreshFn func()
	routerRefreshFn   func()
	admittedSourceFn  func() byte

	tickInterval time.Duration
	now          func() time.Time

	mu       sync.Mutex
	attempts map[byte]*promoterAttemptState

	// metrics
	confirmedTotal uint64
	rejectedTotal  uint64
	skippedTotal   uint64
}

type promoterAttemptState struct {
	attempts      int
	nextAttemptAt time.Time
}

// PassiveDiscoveryPromoterOptions configures a new promoter.
type PassiveDiscoveryPromoterOptions struct {
	// Registry is the gateway device registry — used to filter
	// candidates already known and to register confirmed devices.
	// Required.
	Registry *registry.DeviceRegistry

	// EvidenceBuffer feeds candidate addresses. Required.
	EvidenceBuffer *EvidenceBuffer

	// ConfirmFn runs the B524 capability coherency probe against the
	// candidate target using the admitted source. Returns true on
	// coherent response. Required.
	ConfirmFn func(ctx context.Context, target byte) bool

	// SemanticRefreshFn enqueues the semantic poller's discovery
	// refresh after a successful registration so the regulator
	// surface populates. Optional — promoter still registers and
	// refreshes router planes when nil.
	SemanticRefreshFn func()

	// RouterRefreshFn refreshes router planes after a successful
	// registration so live event routing reflects the new device.
	// Optional.
	RouterRefreshFn func()

	// AdmittedSourceFn returns the gateway's admitted source. The
	// promoter filters this address out of candidates so the gateway
	// never confirms its own source. Required.
	AdmittedSourceFn func() byte

	// TickInterval bounds how often the promoter polls
	// PromotedAddresses. Defaults to 30s if unset.
	TickInterval time.Duration

	// Now returns the current time. Defaults to time.Now if unset.
	Now func() time.Time
}

// NewPassiveDiscoveryPromoter constructs a promoter. Returns an error
// if any required option is nil.
func NewPassiveDiscoveryPromoter(opts PassiveDiscoveryPromoterOptions) (*PassiveDiscoveryPromoter, error) {
	if opts.Registry == nil {
		return nil, errors.New("passive discovery promoter: nil registry")
	}
	if opts.EvidenceBuffer == nil {
		return nil, errors.New("passive discovery promoter: nil evidence buffer")
	}
	if opts.ConfirmFn == nil {
		return nil, errors.New("passive discovery promoter: nil confirm fn")
	}
	if opts.AdmittedSourceFn == nil {
		return nil, errors.New("passive discovery promoter: nil admitted-source fn")
	}
	tick := opts.TickInterval
	if tick <= 0 {
		tick = 30 * time.Second
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &PassiveDiscoveryPromoter{
		registry:          opts.Registry,
		evidenceBuf:       opts.EvidenceBuffer,
		confirmFn:         opts.ConfirmFn,
		registerFn:        nil, // injected by Run; callers may override via SetRegisterFn for tests
		semanticRefreshFn: opts.SemanticRefreshFn,
		routerRefreshFn:   opts.RouterRefreshFn,
		admittedSourceFn:  opts.AdmittedSourceFn,
		tickInterval:      tick,
		now:               now,
		attempts:          make(map[byte]*promoterAttemptState),
	}, nil
}

// SetRegisterFn overrides the registry-write function. Used by tests
// to substitute a no-op or to capture the registered address. The
// production path uses the default registry.Register helper.
func (p *PassiveDiscoveryPromoter) SetRegisterFn(fn func(target byte)) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.registerFn = fn
	p.mu.Unlock()
}

// Run polls the evidence buffer at the configured tick interval and
// runs active confirmation on each promoted candidate not already in
// the registry. Returns when ctx is cancelled.
func (p *PassiveDiscoveryPromoter) Run(ctx context.Context) {
	if p == nil {
		return
	}
	ticker := time.NewTicker(p.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.processOnce(ctx)
		}
	}
}

// processOnce runs a single tick of the promotion loop. Public for
// testing — production callers use Run.
//
// Source-admission gate: when no source has been admitted (admission
// still pending in source-selection mode, or in degraded state after
// active_probe_failed), the promoter MUST NOT issue active
// confirmation probes. The downstream confirmFn sources from the
// semantic poller's configured source, which is set even before
// admission completes. Issuing probes while admission is unresolved
// violates the source-admission invariant — admission gates ALL
// gateway-originated active traffic, including this pipeline. The
// gate yields the tick (no candidates probed); the next tick re-
// evaluates admission state. Evidence keeps accumulating in the
// buffer regardless.
//
// Per-candidate deadline: each confirmation runs under a derived
// context capped at tickInterval/2, so one slow / non-responsive
// candidate cannot starve subsequent candidates within the same tick
// or run past the next tick.
//
// Demotion: when a candidate has failed maxConfirmationAttempts
// times, its EvidenceBuffer entry is demoted (promoted=false,
// counters reset). Subsequent fresh passive evidence can re-promote
// it. The promoter does not retry the demoted entry on its current
// promotion.
//
// Refresh coalescing: a single tick that confirms multiple candidates
// emits at most ONE semantic-refresh signal (the implicit dedupe at
// the task scheduler is best-effort; coalescing here avoids piling up
// redundant priorityHigh tasks).
func (p *PassiveDiscoveryPromoter) processOnce(ctx context.Context) {
	if p == nil {
		return
	}
	now := p.now()
	admittedSource := p.admittedSourceFn()
	if admittedSource == 0 {
		// Source admission is pending or has degraded. Defer all
		// active confirmation until admission resolves; evidence
		// keeps accumulating in the buffer for the next tick.
		return
	}
	candidates := p.evidenceBuf.PromotedAddresses()
	confirmedThisTick := 0
	// Per-candidate budget: large enough to accommodate the full
	// ConfirmB524Coherent retry chain on a busy adapter-direct bus
	// (~32s worst case, 45s with safety margin). The candidate
	// budget intentionally exceeds the default tickInterval so
	// transient bus contention does not falsely demote reachable
	// late-arrival devices. processOnce is reentrant-safe wrt the
	// next ticker.C: Run's select drops a tick that fires while
	// processOnce is still in flight, and ConfirmB524Coherent
	// bounds itself internally so a stuck candidate cannot block
	// past the budget.
	perCandidateTimeout := perCandidateConfirmationBudget
	for _, addr := range candidates {
		if !p.shouldAttempt(addr, admittedSource, now) {
			continue
		}
		p.recordAttemptStart(addr)

		probeCtx, cancel := context.WithTimeout(ctx, perCandidateTimeout)
		ok := p.confirmFn(probeCtx, addr)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if !ok {
			demote := p.recordAttemptFailure(addr, now)
			if demote {
				log.Printf("passive_discovery_promoter_demoted address=0x%02x reason=max_confirmation_attempts", addr)
				p.evidenceBuf.Demote(addr)
			}
			continue
		}
		p.recordAttemptSuccess(addr)
		p.commitConfirmedRegistration(addr)
		confirmedThisTick++
	}

	// Semantic refresh is emitted ONCE per tick, AFTER all
	// confirmed candidates have been registered and router planes
	// refreshed. Firing it during the loop (after the first
	// registration) would let the semantic discovery task start
	// and snapshot the registry before later candidates in the
	// same tick land — those devices would then miss the immediate
	// refresh and only surface on the next periodic discovery
	// interval.
	if confirmedThisTick > 0 && p.semanticRefreshFn != nil {
		p.semanticRefreshFn()
	}
}

// commitConfirmedRegistration registers the candidate in the device
// registry and refreshes router planes. Semantic refresh is emitted
// once per tick by processOnce after all registrations commit.
func (p *PassiveDiscoveryPromoter) commitConfirmedRegistration(addr byte) {
	p.mu.Lock()
	registerFn := p.registerFn
	p.mu.Unlock()
	if registerFn != nil {
		registerFn(addr)
	} else {
		p.registry.Register(registry.DeviceInfo{
			Address:      addr,
			Manufacturer: "Vaillant",
		})
	}
	log.Printf("passive_discovery_promoter_registered address=0x%02x source=passive_promotion", addr)
	if p.routerRefreshFn != nil {
		p.routerRefreshFn()
	}
}

// shouldAttempt reports whether addr is a valid attempt-now candidate.
// Filters: address already in registry, address equals admitted
// source (self), invalid responder range, per-address backoff not yet
// elapsed.
func (p *PassiveDiscoveryPromoter) shouldAttempt(addr, admittedSource byte, now time.Time) bool {
	if !isPassiveEvidenceCandidate(addr, admittedSource) {
		p.incSkipped()
		return false
	}
	if p.registryContains(addr) {
		p.incSkipped()
		return false
	}
	p.mu.Lock()
	state := p.attempts[addr]
	p.mu.Unlock()
	if state == nil {
		return true
	}
	if !state.nextAttemptAt.IsZero() && now.Before(state.nextAttemptAt) {
		p.incSkipped()
		return false
	}
	return true
}

func (p *PassiveDiscoveryPromoter) incSkipped() {
	p.mu.Lock()
	p.skippedTotal++
	p.mu.Unlock()
}

func (p *PassiveDiscoveryPromoter) recordAttemptStart(addr byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.attempts[addr]
	if !ok {
		state = &promoterAttemptState{}
		p.attempts[addr] = state
	}
	state.attempts++
}

// recordAttemptFailure schedules per-address exponential backoff and
// returns true iff the candidate has reached the failure cap and
// should be permanently demoted from the evidence buffer (see B5
// fix). Demotion releases the candidate from the EvidenceBuffer's
// promoted set so subsequent fresh evidence can re-promote, but the
// promoter does not retry on the existing stale promotion.
func (p *PassiveDiscoveryPromoter) recordAttemptFailure(addr byte, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.attempts[addr]
	if state == nil {
		return false
	}
	backoff := RejoinBackoffSchedule(state.attempts, StartupAdmissionRejoinBackoffBaseSeconds, StartupAdmissionRejoinBackoffMaxSeconds)
	state.nextAttemptAt = now.Add(backoff)
	p.rejectedTotal++
	log.Printf("passive_discovery_promoter_confirm_failed address=0x%02x attempt=%d next_attempt_in=%s", addr, state.attempts, backoff)
	if state.attempts >= maxConfirmationAttempts {
		delete(p.attempts, addr)
		return true
	}
	return false
}

func (p *PassiveDiscoveryPromoter) recordAttemptSuccess(addr byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.attempts, addr)
	p.confirmedTotal++
}

func (p *PassiveDiscoveryPromoter) registryContains(addr byte) bool {
	if p.registry == nil {
		return false
	}
	found := false
	p.registry.Iterate(func(entry registry.DeviceEntry) bool {
		if entry == nil {
			return true
		}
		if entry.PrimaryDisplayAddress() == addr {
			found = true
			return false
		}
		return true
	})
	return found
}

// Snapshot returns a point-in-time view of promoter state for
// diagnostics and tests.
type PassiveDiscoveryPromoterSnapshot struct {
	ConfirmedTotal uint64
	RejectedTotal  uint64
	SkippedTotal   uint64
	PendingByAddr  map[byte]int
}

// Snapshot captures current promoter counters and per-address state
// for diagnostics and tests.
func (p *PassiveDiscoveryPromoter) Snapshot() PassiveDiscoveryPromoterSnapshot {
	if p == nil {
		return PassiveDiscoveryPromoterSnapshot{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := make(map[byte]int, len(p.attempts))
	for addr, state := range p.attempts {
		pending[addr] = state.attempts
	}
	return PassiveDiscoveryPromoterSnapshot{
		ConfirmedTotal: p.confirmedTotal,
		RejectedTotal:  p.rejectedTotal,
		SkippedTotal:   p.skippedTotal,
		PendingByAddr:  pending,
	}
}

// String renders the snapshot for log lines.
func (s PassiveDiscoveryPromoterSnapshot) String() string {
	return fmt.Sprintf("passive_discovery_promoter confirmed=%d rejected=%d skipped=%d pending=%d", s.ConfirmedTotal, s.RejectedTotal, s.SkippedTotal, len(s.PendingByAddr))
}
