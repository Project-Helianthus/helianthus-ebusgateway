package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// These tests measure, on the offline replay fixtures, how one PV asset's
// snapshot grows per identity-loss cause and how many events reach a kernel
// bound. They pin today's behaviour; they change none of it.

const (
	// The pinned kernel also bounds an asset at 128 generation fences and 128
	// publication cursors; the publication refuses only at the other bounds.
	growthKernelFences  = 128
	growthKernelCursors = 128
	// growthCycle is the refresh interval of the Huawei families.
	growthCycle = time.Minute
	// growthWeek is the event budget of the remedy rule: one event per hour
	// for a week.
	growthWeek = 168
)

// growthCounts are the bounded members of one asset snapshot. Retired counts
// the bindings of another epoch than the publishing one and their links.
type growthCounts struct {
	Sources, Bindings, Links, Fences, Cursors, RetiredBindings, RetiredLinks int
}

func (c growthCounts) minus(o growthCounts) growthCounts {
	return growthCounts{c.Sources - o.Sources, c.Bindings - o.Bindings, c.Links - o.Links, c.Fences - o.Fences, c.Cursors - o.Cursors, c.RetiredBindings - o.RetiredBindings, c.RetiredLinks - o.RetiredLinks}
}

func (c growthCounts) String() string {
	return fmt.Sprintf("src %+d, bind %+d, link %+d, fence %+d, cursor %+d, retired bind %+d, retired link %+d", c.Sources, c.Bindings, c.Links, c.Fences, c.Cursors, c.RetiredBindings, c.RetiredLinks)
}

func growthSample(t *testing.T, hub *semhub.Hub, asset semreg.AssetID, epoch semreg.SourceEpochID) growthCounts {
	t.Helper()
	view, err := hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	s := view.Snapshot
	out := growthCounts{Sources: len(s.Sources), Bindings: len(s.Bindings), Links: len(s.IdentityLinks), Fences: len(s.Fences), Cursors: len(s.Cursors)}
	retired := map[semreg.NativeBindingID]bool{}
	for _, binding := range s.Bindings {
		if binding.SourceEpochID != epoch {
			retired[binding.BindingID] = true
			out.RetiredBindings++
		}
	}
	for _, link := range s.IdentityLinks {
		if retired[link.BindingID] {
			out.RetiredLinks++
		}
	}
	return out
}

// growthFirstBound returns the events after which the first member growing by
// delta per event reaches its kernel bound, and that member; zero when none
// grows. The remedy rule leaves identity links out: they grow with bindings.
func growthFirstBound(base, delta growthCounts, links bool) (int, string) {
	members := []struct {
		name               string
		base, delta, bound int
	}{
		{"sources", base.Sources, delta.Sources, huaweiMaxAssetSources},
		{"bindings", base.Bindings, delta.Bindings, huaweiMaxAssetBindings},
		{"identity links", base.Links, delta.Links, huaweiMaxAssetIdentityLinks},
		{"fences", base.Fences, delta.Fences, growthKernelFences},
		{"cursors", base.Cursors, delta.Cursors, growthKernelCursors},
	}
	best, name := 0, ""
	for _, m := range members {
		if m.delta <= 0 || !links && m.name == "identity links" {
			continue
		}
		if n := (m.bound - m.base + m.delta - 1) / m.delta; best == 0 || n < best {
			best, name = n, m.name
		}
	}
	return best, name
}

// growthFixtureCycle replays the fixture's first cycle and keeps two admitted
// children. A fixture with one admitted child gets a synthetic sibling decoded
// from the same answered groups under the next routed unit.
func growthFixtureCycle(t *testing.T, name string) modbusreg.HuaweiProviderCycle {
	t.Helper()
	provider, _, _, clock := h10Native(t, h10Load(t, name))
	cycle := h10Refresh(t, provider, clock)
	if cycle.Outcome != modbusreg.HuaweiProviderCycleComplete {
		t.Fatalf("fixture cycle outcome %s", cycle.Outcome)
	}
	var admitted []modbusreg.HuaweiProviderChildObservation
	for _, child := range cycle.Children {
		if child.Evidence != modbusreg.HuaweiProviderEvidenceWithheld {
			admitted = append(admitted, child)
		}
	}
	if len(admitted) == 0 {
		t.Fatal("fixture admits no child")
	}
	if len(admitted) == 1 {
		admitted = append(admitted, growthSyntheticSibling(t, admitted[0]))
	}
	cycle.Children = admitted[:2]
	return cycle
}

func growthSyntheticSibling(t *testing.T, child modbusreg.HuaweiProviderChildObservation) modbusreg.HuaweiProviderChildObservation {
	t.Helper()
	var profile modbusreg.HuaweiSUN2000MonitoringProfile
	for _, candidate := range modbusreg.HuaweiSUN2000MonitoringProfiles() {
		if candidate.ID() == child.ProfileID {
			profile = candidate
		}
	}
	var answered []modbusreg.HuaweiSUN2000GroupReading
	for _, group := range child.Reading.Groups() {
		if group.Outcome == modbusreg.HuaweiSUN2000GroupAnswered {
			answered = append(answered, group)
		}
	}
	sibling := child
	sibling.UnitID = child.UnitID + 1
	reading, err := profile.Decode(sibling.UnitID, answered)
	if err != nil {
		t.Fatal(err)
	}
	sibling.Reading = reading
	sibling.Groups = append([]modbusreg.HuaweiProviderGroupEvidence(nil), child.Groups...)
	return sibling
}

// growthRig drives one publication with synthetic cycles derived from a
// fixture cycle: the victim child loses its identity, the sibling does not.
type growthRig struct {
	t               *testing.T
	hub             *semhub.Hub
	pub             *huaweiPVPublication
	cycle           modbusreg.HuaweiProviderCycle
	victim, sibling modbusreg.HuaweiProviderChildObservation
	assets          [2]semreg.AssetID
}

func growthSetup(t *testing.T, cycle modbusreg.HuaweiProviderCycle) *growthRig {
	t.Helper()
	hub := newTestSemanticHub(t)
	pub, err := newHuaweiPVPublication(hub, "growth-test", "source:synthetic", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	r := &growthRig{t: t, hub: hub, pub: pub, cycle: cycle, victim: cycle.Children[0], sibling: cycle.Children[1]}
	r.assets = [2]semreg.AssetID{huaweiAssetID("alpha", "pv", r.victim.UnitID), huaweiAssetID("alpha", "pv", r.sibling.UnitID)}
	r.must(pub.Publish(cycle))
	if len(pub.published) != 2 {
		t.Fatalf("fixture published %d children, want 2", len(pub.published))
	}
	return r
}

func (r *growthRig) must(err error) {
	r.t.Helper()
	if err != nil {
		r.t.Fatal(err)
	}
}

// publish offers the next cycle, one second after the last, with children.
func (r *growthRig) publish(outcome modbusreg.HuaweiProviderCycleOutcome, children ...modbusreg.HuaweiProviderChildObservation) error {
	r.cycle.Cycle++
	r.cycle.StartedAt = r.cycle.StartedAt.Add(time.Second)
	r.cycle.CompletedAt = r.cycle.CompletedAt.Add(time.Second)
	r.cycle.Outcome = outcome
	r.cycle.Children = children
	return r.pub.Publish(r.cycle)
}

func growthAs(child modbusreg.HuaweiProviderChildObservation, evidence modbusreg.HuaweiProviderEvidenceState, reason modbusreg.HuaweiProviderReason) modbusreg.HuaweiProviderChildObservation {
	child.Evidence, child.Reason = evidence, reason
	return child
}

// growthRebase identifies a child in a new transport generation: its groups
// are read again in that generation.
func growthRebase(child modbusreg.HuaweiProviderChildObservation, generation uint64) modbusreg.HuaweiProviderChildObservation {
	child.IdentifiedGeneration = generation
	child.Groups = append([]modbusreg.HuaweiProviderGroupEvidence(nil), child.Groups...)
	for i := range child.Groups {
		child.Groups[i].Generation = generation
	}
	return child
}

type growthMeasure struct {
	victim, sibling []growthCounts
	refused         int
	// cycles is the number of cycles one event spans: at least two for events
	// paced by the listing, the actual publication count for a restart.
	cycles int
}

func (r *growthRig) sample() (growthCounts, growthCounts) {
	epoch := r.pub.handle.Epoch()
	return growthSample(r.t, r.hub, r.assets[0], epoch), growthSample(r.t, r.hub, r.assets[1], epoch)
}

// measure runs events, sampling both assets after each one. An event returns
// the error of its re-admission; only the binding-capacity refusal is expected.
func (r *growthRig) measure(events int, event func(*growthRig) error) growthMeasure {
	r.t.Helper()
	v, s := r.sample()
	m := growthMeasure{victim: []growthCounts{v}, sibling: []growthCounts{s}}
	for i := 1; i <= events; i++ {
		before := r.cycle.Cycle
		if err := event(r); errors.Is(err, errHuaweiBindingCapacityExhausted) {
			if m.refused == 0 {
				m.refused = i
			}
		} else if err != nil {
			r.t.Fatalf("event %d: %v", i, err)
		}
		if i == 1 {
			m.cycles = max(int(r.cycle.Cycle-before), 2)
		}
		v, s = r.sample()
		m.victim, m.sibling = append(m.victim, v), append(m.sibling, s)
	}
	if view, err := r.hub.Inspect(r.assets[1]); err != nil || len(view.Snapshot.Facts) == 0 || r.pub.published[r.sibling.UnitID] != r.assets[1] {
		r.t.Fatalf("sibling no longer published: %v", err)
	}
	if m.refused == 0 {
		return m
	}
	r.pub.mu.Lock()
	children := huaweiStatusChildren(r.pub, r.pub.latest)
	r.pub.mu.Unlock()
	// The refused child's asset holds the binding bound and only withdrawn
	// links of this generation; the sibling's asset keeps its headroom.
	for _, child := range children {
		victim := child.Unit == r.victim.UnitID && (child.Refusal != huaweiRefusalBindingCapacity || child.AssetID != "" || child.BindingIncarnations != mcp.HuaweiAssetBindingBound || child.SourceDescriptors != 1 || child.IdentityLinkState != mcp.HuaweiIdentityLinkWithdrawn)
		sibling := child.Unit == r.sibling.UnitID && (child.AssetID == "" || child.BindingIncarnations != 1 || child.SourceDescriptors != 1 || child.IdentityLinkState != mcp.HuaweiIdentityLinkQualified)
		if victim || sibling {
			r.t.Fatalf("status after the refusal: %+v", children)
		}
	}
	return m
}

// growthPin requires exactly the given growth per event until the first
// bound, none after it, and the refusal at the first event beyond it.
func growthPin(t *testing.T, label string, m growthMeasure, victim, sibling growthCounts) {
	t.Helper()
	limit, _ := growthFirstBound(m.victim[0], victim, true)
	for i := 1; i < len(m.victim); i++ {
		wantV, wantS := victim, sibling
		if limit > 0 && i > limit {
			wantV, wantS = growthCounts{}, growthCounts{}
		}
		if got := m.victim[i].minus(m.victim[i-1]); got != wantV {
			t.Fatalf("%s: event %d grew the affected asset by %s, want %s", label, i, got, wantV)
		}
		if got := m.sibling[i].minus(m.sibling[i-1]); got != wantS {
			t.Fatalf("%s: event %d grew the sibling by %s, want %s", label, i, got, wantS)
		}
	}
	want := 0
	if limit > 0 && len(m.victim)-1 > limit {
		want = limit + 1
	}
	if m.refused != want {
		t.Fatalf("%s: first refusal at event %d, want %d", label, m.refused, want)
	}
}

// growthClass is what the remedy rule asks of a scenario.
type growthClass int

const (
	growthGenuine growthClass = iota
	growthTransientLoss
	growthAbsence
	growthNoAbsence
	growthOperator
)

type growthRow struct {
	scenario, fixture string
	class             growthClass
	victim, sibling   growthCounts
	events            int
	cycles            int
	member            string
	ruleEvents        int
}

func growthRowOf(scenario, fixture string, class growthClass, m growthMeasure) growthRow {
	row := growthRow{scenario: scenario, fixture: fixture, class: class, victim: m.victim[1].minus(m.victim[0]), sibling: m.sibling[1].minus(m.sibling[0]), cycles: m.cycles}
	row.events, row.member = growthFirstBound(m.victim[0], row.victim, true)
	row.ruleEvents, _ = growthFirstBound(m.victim[0], row.victim, false)
	return row
}

func (row growthRow) String() string {
	bound, paced, hourly := "none", "never", "never"
	if row.events > 0 {
		bound = fmt.Sprintf("%d (%s)", row.events, row.member)
		paced = (time.Duration(row.events*row.cycles) * growthCycle).String()
		hourly = (time.Duration(row.events) * time.Hour).String()
	}
	return fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s |", row.scenario, row.fixture, row.victim, row.sibling, bound, paced, hourly)
}

type growthScenario struct {
	name   string
	class  growthClass
	events int
	event  func(*growthRig) error
	// grows is the measured growth of the affected asset per event.
	grows growthCounts
}

var growthIncarnation = growthCounts{Bindings: 1, Links: 1}

// readmit offers the victim again. Like the loss cycles that do not need the
// sibling, it carries the victim alone in an incomplete listing: the sibling
// stays published and is sampled without a batch every cycle.
func (r *growthRig) readmit() error {
	return r.publish(modbusreg.HuaweiProviderCycleIncomplete, r.victim)
}

func growthScenarios() []growthScenario {
	complete, incomplete := modbusreg.HuaweiProviderCycleComplete, modbusreg.HuaweiProviderCycleIncomplete
	out := []growthScenario{{"identity change", growthGenuine, 140, func(r *growthRig) error {
		r.must(r.publish(incomplete, growthAs(r.victim, modbusreg.HuaweiProviderEvidenceWithheld, modbusreg.HuaweiProviderReasonIdentityMismatch)))
		return r.readmit()
	}, growthIncarnation}}
	for lag, cycles := range []string{1: "one stale cycle", 2: "two stale cycles"} {
		if lag > 0 {
			out = append(out, growthScenario{"reconnect, identified after " + cycles, growthTransientLoss, 20, growthReconnect(lag), growthIncarnation})
		}
	}
	reasons := []modbusreg.HuaweiProviderReason{modbusreg.HuaweiProviderReasonChildTimeout, modbusreg.HuaweiProviderReasonChildReadFailed, modbusreg.HuaweiProviderReasonChildBackoff, modbusreg.HuaweiProviderReasonListingUnconfirmed}
	for _, evidence := range []modbusreg.HuaweiProviderEvidenceState{modbusreg.HuaweiProviderEvidenceStale, modbusreg.HuaweiProviderEvidenceWithheld} {
		grows := growthCounts{}
		if evidence == modbusreg.HuaweiProviderEvidenceWithheld {
			grows = growthIncarnation
		}
		for _, reason := range reasons {
			out = append(out, growthScenario{fmt.Sprintf("%s reported %s", reason, strings.ToLower(string(evidence))), growthTransientLoss, 20, growthTransient(reason, evidence), grows})
		}
	}
	for _, outcome := range []modbusreg.HuaweiProviderCycleOutcome{complete, incomplete} {
		name, class, grows := "absent from an incomplete listing", growthNoAbsence, growthCounts{}
		if outcome == complete {
			name, class, grows = "absent from a complete listing", growthAbsence, growthIncarnation
		}
		out = append(out, growthScenario{name, class, 20, func(r *growthRig) error {
			r.must(r.publish(outcome, r.sibling))
			return r.readmit()
		}, grows})
	}
	return out
}

// growthReconnect raises the transport generation. The sibling is identified
// again at once; the victim stays stale with its previous identified
// generation for lag cycles, then is identified.
func growthReconnect(lag int) func(*growthRig) error {
	return func(r *growthRig) error {
		r.cycle.Generation++
		r.sibling = growthRebase(r.sibling, r.cycle.Generation)
		stale := growthAs(r.victim, modbusreg.HuaweiProviderEvidenceStale, modbusreg.HuaweiProviderReasonChildBackoff)
		for i := 0; i < lag; i++ {
			r.must(r.publish(modbusreg.HuaweiProviderCycleComplete, stale, r.sibling))
		}
		r.victim = growthRebase(r.victim, r.cycle.Generation)
		return r.readmit()
	}
}

// growthTransient reports the victim with a transient reason for one cycle in
// the same transport generation. The native provider reports an identified
// child stale; the withheld form is measured too. An unconfirmed listing
// reports every known child unread, the sibling included.
func growthTransient(reason modbusreg.HuaweiProviderReason, evidence modbusreg.HuaweiProviderEvidenceState) func(*growthRig) error {
	return func(r *growthRig) error {
		children := []modbusreg.HuaweiProviderChildObservation{growthAs(r.victim, evidence, reason)}
		if reason == modbusreg.HuaweiProviderReasonListingUnconfirmed {
			children = append(children, growthAs(r.sibling, modbusreg.HuaweiProviderEvidenceStale, reason))
		}
		r.must(r.publish(modbusreg.HuaweiProviderCycleIncomplete, children...))
		return r.readmit()
	}
}

// growthRestarts restarts a managed instance through the driver lifecycle and
// samples both assets after each successor epoch published, until the source
// bound refuses a START.
func growthRestarts(t *testing.T, cycle modbusreg.HuaweiProviderCycle, restarts int) growthMeasure {
	hub := newTestSemanticHub(t)
	var mu sync.Mutex
	var previous *semhub.Handle
	var pubs []*huaweiPVPublication
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return &m03Endpoint{}, nil
	}, func(ctx context.Context, adapter *modbusadapter.Adapter) modbusManagedWork {
		run, cancel := context.WithCancel(ctx)
		mu.Lock()
		defer mu.Unlock()
		publication, err := newHuaweiPVPublication(hub, "pv.primary", "pv.configured", "alpha", previous, adapter.PublicationActive)
		if err == nil {
			previous = publication.handle
			pubs = append(pubs, publication)
		}
		return &huaweiManagedWork{ctx: run, cancel: cancel, adapter: adapter, instance: modbusTCPInstance{label: "alpha", family: modbusTCPFamilyHuaweiSmartLogger, cycle: time.Hour}, publication: publication, provider: &h10SingleCycleProvider{cycle: cycle}, err: err, done: make(chan struct{})}
	})
	assets := []semreg.AssetID{huaweiAssetID("alpha", "pv", cycle.Children[0].UnitID), huaweiAssetID("alpha", "pv", cycle.Children[1].UnitID)}
	var epoch semreg.SourceEpochID
	start := func(n int) error {
		if _, err := driver.Start(context.Background()); err != nil {
			return err
		}
		m03Eventually(t, fmt.Sprintf("epoch %d publication", n), func() bool {
			mu.Lock()
			defer mu.Unlock()
			if len(pubs) != n {
				return false
			}
			pubs[n-1].mu.Lock()
			defer pubs[n-1].mu.Unlock()
			epoch = pubs[n-1].handle.Epoch()
			return len(pubs[n-1].published) == len(assets)
		})
		// Every epoch adds one descriptor and one binding to each asset, and
		// only the publishing epoch's link is reported.
		mu.Lock()
		publication := pubs[n-1]
		mu.Unlock()
		publication.mu.Lock()
		children := huaweiStatusChildren(publication, publication.latest)
		publication.mu.Unlock()
		for _, child := range children {
			if child.SourceDescriptors != n || child.BindingIncarnations != n || child.IdentityLinkState != mcp.HuaweiIdentityLinkQualified {
				t.Fatalf("status after START %d: %+v", n, children)
			}
		}
		return nil
	}
	if err := start(1); err != nil {
		t.Fatal(err)
	}
	m := growthMeasure{victim: []growthCounts{growthSample(t, hub, assets[0], epoch)}, sibling: []growthCounts{growthSample(t, hub, assets[1], epoch)}}
	for i := 1; i <= restarts; i++ {
		if err := driver.Stop(context.Background()); err != nil {
			t.Fatalf("STOP %d: %v", i, err)
		}
		if err := start(i + 1); errors.Is(err, errHuaweiSourceCapacityExhausted) {
			if m.refused == 0 {
				m.refused = i
			}
		} else if err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		m.victim = append(m.victim, growthSample(t, hub, assets[0], epoch))
		m.sibling = append(m.sibling, growthSample(t, hub, assets[1], epoch))
	}
	m.cycles = 1
	return m
}

type growthProbeResult struct {
	demote, requalify, linkOnly error
	before, after               growthCounts
	// The probed binding's link at the end, and the asset's fact envelopes
	// before the probe and at the end.
	state              semreg.LinkState
	revision           semreg.Uint64
	factsBefore, facts int
}

func growthAccepted(err error) string {
	if err == nil {
		return "accepted"
	}
	return "rejected: " + err.Error()
}

// apply builds one batch for asset the way publishChild does, lets build fill
// its records and applies and commits it.
func (r *growthRig) apply(asset semreg.AssetID, build func(semreg.Snapshot, *semreg.PublicationBatch) error) error {
	p := r.pub
	r.cycle.Cycle++
	r.cycle.CompletedAt = r.cycle.CompletedAt.Add(time.Second)
	tx, err := p.hub.Begin(p.handle, asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	current, _ := tx.Current()
	source, epoch, generation := p.handle.Source(), p.handle.Epoch(), p.handle.GenerationID()
	evidence := huaweiEvidence("native.huawei.provider.observation", r.cycle.Cycle)
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:huawei-pv:" + huaweiHash(string(asset), string(epoch), string(generation), string(tx.NextSequence()))[:32]), AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: current.Revisions.Semantic, ObservedAt: growattWall(r.cycle.CompletedAt),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: append([]semreg.SourceEpochID{}, tx.SourceRetirements()...), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	for _, old := range tx.GenerationFences() {
		batch.GenerationFences = append(batch.GenerationFences, semreg.GenerationFence{SourceID: source, SourceEpochID: epoch, DriverGeneration: old, Reason: "lifecycle.driver_generation_replaced", Evidence: []semreg.EvidenceRef{evidence}, Revision: "1"})
	}
	if err := build(current, &batch); err != nil {
		return err
	}
	sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	sort.Slice(batch.FactWithdrawals, func(i, j int) bool { return batch.FactWithdrawals[i] < batch.FactWithdrawals[j] })
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		return err
	}
	if _, _, err := tx.Apply(batch, p.point(r.cycle.CompletedAt)); err != nil {
		return err
	}
	_, err = tx.Commit(nil)
	return err
}

// growthProbe moves the victim's qualified link to candidate while withdrawing
// its facts, then requalifies the same binding with a higher revision and its
// facts; when that is rejected, the link alone is requalified.
func growthProbe(t *testing.T, r *growthRig) growthProbeResult {
	asset := r.assets[0]
	var res growthProbeResult
	res.before, _ = r.sample()
	if view, err := r.hub.Inspect(asset); err == nil {
		res.factsBefore = len(view.Snapshot.Facts)
	}
	var binding semreg.NativeBindingID
	res.demote = r.apply(asset, func(current semreg.Snapshot, batch *semreg.PublicationBatch) error {
		for _, link := range current.IdentityLinks {
			if link.State == semreg.LinkQualified {
				binding = link.BindingID
				link.State, link.Revision = semreg.LinkCandidate, huaweiNext(link.Revision)
				batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, link)
			}
		}
		for _, envelope := range current.Facts {
			for _, candidate := range envelope.Candidates {
				if candidate.BindingID != nil && *candidate.BindingID == binding {
					batch.FactWithdrawals = append(batch.FactWithdrawals, candidate.CandidateID)
				}
			}
		}
		if binding == "" {
			return errors.New("no qualified link to move")
		}
		return nil
	})
	requalify := func(facts bool) func(semreg.Snapshot, *semreg.PublicationBatch) error {
		return func(current semreg.Snapshot, batch *semreg.PublicationBatch) error {
			evidence := huaweiEvidence("native.huawei.provider.observation", r.cycle.Cycle)
			for _, link := range current.IdentityLinks {
				if link.BindingID == binding {
					link.State, link.Revision, link.Basis = semreg.LinkQualified, huaweiNext(link.Revision), []semreg.EvidenceRef{evidence}
					batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, link)
				}
			}
			if !facts {
				return nil
			}
			var err error
			batch.FactUpserts, err = r.pub.facts(asset, binding, r.victim, r.cycle, evidence, current)
			return err
		}
	}
	res.requalify = r.apply(asset, requalify(true))
	if res.requalify != nil {
		res.linkOnly = r.apply(asset, requalify(false))
	}
	res.after, _ = r.sample()
	view, err := r.hub.Inspect(asset)
	if err != nil {
		t.Fatal(err)
	}
	res.facts = len(view.Snapshot.Facts)
	for _, link := range view.Snapshot.IdentityLinks {
		if link.BindingID == binding {
			res.state, res.revision = link.State, link.Revision
		}
	}
	return res
}

// The scenarios are independent: each owns its hub and publication and only
// reads the replayed fixture cycle, so they run in parallel with each other
// and with the package's other parallel tests.
func TestHuaweiIdentityGrowthMeasurement(t *testing.T) {
	t.Parallel()
	fixtures := []string{"smartlogger", "sdongle"}
	scenarios := growthScenarios()
	// Each subtest fills only its own slot; t.Run of the group returns after
	// every parallel subtest has finished.
	rows := make([]*growthRow, len(fixtures)*(len(scenarios)+1))
	probes := make([]*growthProbeResult, len(fixtures))
	t.Run("scenarios", func(t *testing.T) {
		for f, fixture := range fixtures {
			// One replay per fixture; every scenario derives its cycles from it.
			cycle := growthFixtureCycle(t, fixture)
			slot := f * (len(scenarios) + 1)
			for i, scenario := range scenarios {
				t.Run(fixture+"/"+scenario.name, func(t *testing.T) {
					t.Parallel()
					m := growthSetup(t, cycle).measure(scenario.events, scenario.event)
					growthPin(t, scenario.name, m, scenario.grows, growthCounts{})
					row := growthRowOf(scenario.name, fixture, scenario.class, m)
					rows[slot+i] = &row
				})
			}
			t.Run(fixture+"/operator restarts", func(t *testing.T) {
				t.Parallel()
				// Every restart is a new epoch: one descriptor, one cursor, one
				// binding and one link more, the previous epoch's binding retired.
				restart := growthCounts{Sources: 1, Bindings: 1, Links: 1, Cursors: 1, RetiredBindings: 1, RetiredLinks: 1}
				m := growthRestarts(t, cycle, 32)
				growthPin(t, "operator restarts", m, restart, restart)
				row := growthRowOf("operator restarts", fixture, growthOperator, m)
				rows[slot+len(scenarios)] = &row
			})
			t.Run(fixture+"/kernel round trip", func(t *testing.T) {
				t.Parallel()
				res := growthProbe(t, growthSetup(t, cycle))
				growthProbeCheck(t, fixture, res)
				probes[f] = &res
			})
		}
	})
	// The table and the rule need every scenario of both fixtures; a run
	// filtered to some subtests only pins their growth.
	var table []growthRow
	for _, row := range rows {
		if row != nil {
			table = append(table, *row)
		}
	}
	measured := map[string]growthProbeResult{}
	for f, res := range probes {
		if res != nil {
			measured[fixtures[f]] = *res
		}
	}
	if t.Failed() || len(table) != len(rows) || len(measured) != len(fixtures) {
		t.Logf("partial run: table and remedy not evaluated")
		return
	}
	t.Logf("| scenario | fixture | affected asset per event | sibling per event | events to first bound | one event per two %s cycles, three where the event publishes three | one event per hour |", growthCycle)
	for _, row := range table {
		t.Logf("%s", row)
	}
	// Measured: every condition of the rule holds, so it selects all three.
	if continuity, hysteresis, compaction := growthRemedy(t, table, measured); !continuity || !hysteresis || !compaction {
		t.Fatalf("remedy selection changed: continuity %v, hysteresis %v, compaction %v", continuity, hysteresis, compaction)
	}
}

// growthProbeCheck logs the kernel round trip and pins its measured outcome:
// the pinned kernel accepts it on the same binding, restores every fact and
// grows nothing.
func growthProbeCheck(t *testing.T, fixture string, res growthProbeResult) {
	t.Helper()
	linkOnly := "not needed"
	if res.requalify != nil {
		linkOnly = growthAccepted(res.linkOnly)
	}
	t.Logf("kernel round trip on %s: qualified to candidate with its facts withdrawn: %s; requalified with a higher revision and its facts: %s; link alone: %s; link ends %s at revision %s with %d of %d fact envelopes; asset grew by %s", fixture, growthAccepted(res.demote), growthAccepted(res.requalify), linkOnly, res.state, res.revision, res.facts, res.factsBefore, res.after.minus(res.before))
	if res.demote != nil || res.requalify != nil || res.state != semreg.LinkQualified || res.revision != "3" || res.facts == 0 || res.facts != res.factsBefore || res.after != res.before {
		t.Fatalf("kernel round trip changed: %+v", res)
	}
}

// growthRemedy applies the selection rule to the measurements.
func growthRemedy(t *testing.T, rows []growthRow, probes map[string]growthProbeResult) (continuity, hysteresis, compaction bool) {
	accepted := len(probes) == 2
	for _, res := range probes {
		accepted = accepted && res.demote == nil && res.requalify == nil
	}
	var transient, absence bool
	for _, row := range rows {
		grows := row.victim != growthCounts{}
		switch row.class {
		case growthTransientLoss:
			transient = transient || grows
		case growthAbsence:
			absence = absence || grows
		}
		compaction = compaction || row.class != growthOperator && row.ruleEvents > 0 && row.ruleEvents <= growthWeek
	}
	compaction = compaction || !accepted
	t.Logf("kernel round trip accepted: %v; a reconnect or transient loss grows: %v; absence from a complete listing grows: %v; a bound within %d events without operator action: %v", accepted, transient, absence, growthWeek, compaction)
	t.Logf("identity continuity in the gateway driver selected: %v", accepted && transient)
	t.Logf("absence hysteresis over three complete listings selected: %v", absence)
	t.Logf("kernel compaction needed: %v", compaction)
	return accepted && transient, absence, compaction
}

// The managed driver reports no failure of a running generation, so the
// manager never restarts an instance that has published; its automatic retry
// follows only a failed START, which publishes nothing.
func TestHuaweiIdentityGrowthAutomaticRestartReach(t *testing.T) {
	driver := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		return &m03Endpoint{}, nil
	}, func(context.Context, *modbusadapter.Adapter) modbusManagedWork { return h10NoopWork{} })
	var runtime any = driver
	_, binds := runtime.(interface {
		BindCorrelation(drivermanager.Correlation)
	})
	_, activates := runtime.(interface {
		ActivateCorrelation(drivermanager.Correlation)
	})
	// Work exposes only Start and Close: nothing a running generation could
	// use to report a failure after it published.
	work := reflect.TypeOf((*modbusManagedWork)(nil)).Elem()
	fails := work.NumMethod() != 2 || work.Method(0).Name != "Close" || work.Method(1).Name != "Start"
	if binds || activates || fails {
		t.Fatal("a running managed generation can now report failure; measure automatic restarts after publication")
	}
	t.Logf("automatic restart after publication: not reachable offline")
}

// growthStatus reads one publication's status children by routed unit.
func growthStatus(t *testing.T, p *huaweiPVPublication) map[byte]mcp.HuaweiInstanceChildStatus {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[byte]mcp.HuaweiInstanceChildStatus{}
	for _, child := range huaweiStatusChildren(p, p.latest) {
		out[child.Unit] = child
	}
	return out
}

// The instance status reports, per child, the identity-link state of the
// publishing generation and the binding and source-descriptor counts of the
// child's own asset. The asset key is the configured label, the class and the
// routed unit: the sibling (another unit) and a second instance (another label,
// same unit) keep their own counts while the affected asset grows.
func TestHuaweiIdentityGrowthStatusReportsHeadroom(t *testing.T) {
	t.Parallel()
	for _, bound := range []struct {
		name            string
		status, publish int
	}{{"bindings", mcp.HuaweiAssetBindingBound, huaweiMaxAssetBindings}, {"identity links", mcp.HuaweiAssetBindingBound, huaweiMaxAssetIdentityLinks}, {"source descriptors", mcp.HuaweiAssetSourceDescriptorBound, huaweiMaxAssetSources}} {
		if bound.status != bound.publish {
			t.Fatalf("status bound of %s is %d, the publication refuses at %d", bound.name, bound.status, bound.publish)
		}
	}
	cycle := growthFixtureCycle(t, "smartlogger")
	r := growthSetup(t, cycle)
	beta, err := newHuaweiPVPublication(r.hub, "growth-test-beta", "source:synthetic-beta", "beta", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	betaCycle := cycle
	betaCycle.Source = "beta"
	r.must(beta.Publish(betaCycle))
	type want struct {
		asset, link   string
		bindings, src int
	}
	check := func(step string, p *huaweiPVPublication, expected map[byte]want) {
		t.Helper()
		got := growthStatus(t, p)
		for unit, w := range expected {
			child, ok := got[unit]
			if !ok || child.AssetID != w.asset || child.IdentityLinkState != w.link || child.BindingIncarnations != w.bindings || child.SourceDescriptors != w.src {
				t.Fatalf("%s: unit %d status %+v, want %+v", step, unit, child, w)
			}
		}
	}
	victim, sibling := r.victim.UnitID, r.sibling.UnitID
	alphaVictim, alphaSibling := string(r.assets[0]), string(r.assets[1])
	betaVictim := string(huaweiAssetID("beta", "pv", victim))
	qualified, withdrawn := mcp.HuaweiIdentityLinkQualified, mcp.HuaweiIdentityLinkWithdrawn
	check("first admission", r.pub, map[byte]want{victim: {alphaVictim, qualified, 1, 1}, sibling: {alphaSibling, qualified, 1, 1}})
	check("first admission of the second instance", beta, map[byte]want{victim: {betaVictim, qualified, 1, 1}})

	r.must(r.publish(modbusreg.HuaweiProviderCycleIncomplete, growthAs(r.victim, modbusreg.HuaweiProviderEvidenceWithheld, modbusreg.HuaweiProviderReasonIdentityMismatch), r.sibling))
	check("identity mismatch", r.pub, map[byte]want{victim: {"", withdrawn, 1, 1}, sibling: {alphaSibling, qualified, 1, 1}})

	r.must(r.publish(modbusreg.HuaweiProviderCycleIncomplete, r.victim, r.sibling))
	check("re-admission", r.pub, map[byte]want{victim: {alphaVictim, qualified, 2, 1}, sibling: {alphaSibling, qualified, 1, 1}})
	check("second instance after the re-admission", beta, map[byte]want{victim: {betaVictim, qualified, 1, 1}})
}

// The reported link belongs to a binding the publishing source, epoch and
// generation hold current. Each case adds a qualified link whose binding
// differs from the publishing one in exactly one of those fields, so dropping
// that field from the filter reports qualified instead of withdrawn.
func TestHuaweiIdentityGrowthStatusLinkState(t *testing.T) {
	const source, epoch, generation = semreg.SourceID("source:a"), semreg.SourceEpochID("epoch:a"), semreg.Uint64("2")
	binding := func(id string, src semreg.SourceID, ep semreg.SourceEpochID, gen semreg.Uint64, state semreg.BindingState) semreg.NativeBinding {
		return semreg.NativeBinding{BindingID: semreg.NativeBindingID(id), SourceID: src, SourceEpochID: ep, DriverGeneration: gen, State: state}
	}
	link := func(id string, state semreg.LinkState) semreg.IdentityLink {
		return semreg.IdentityLink{BindingID: semreg.NativeBindingID(id), State: state}
	}
	own := binding("own", source, epoch, generation, semreg.BindingCurrent)
	for _, test := range []struct {
		name     string
		bindings []semreg.NativeBinding
		links    []semreg.IdentityLink
		want     string
	}{
		{"no link", []semreg.NativeBinding{own}, nil, ""},
		{"withdrawn", []semreg.NativeBinding{own}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"candidate", []semreg.NativeBinding{own}, []semreg.IdentityLink{link("own", semreg.LinkCandidate)}, mcp.HuaweiIdentityLinkCandidate},
		{"qualified", []semreg.NativeBinding{own}, []semreg.IdentityLink{link("own", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkQualified},
		{"state the publication never writes", []semreg.NativeBinding{own}, []semreg.IdentityLink{link("own", semreg.LinkConflict), link("own", semreg.LinkRejected)}, ""},
		{"candidate outranks withdrawn", []semreg.NativeBinding{own, binding("next", source, epoch, generation, semreg.BindingCurrent)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("next", semreg.LinkCandidate)}, mcp.HuaweiIdentityLinkCandidate},
		{"qualified outranks candidate", []semreg.NativeBinding{own, binding("next", source, epoch, generation, semreg.BindingCurrent)}, []semreg.IdentityLink{link("next", semreg.LinkQualified), link("own", semreg.LinkCandidate)}, mcp.HuaweiIdentityLinkQualified},
		{"link of no binding", []semreg.NativeBinding{own}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("orphan", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"another source", []semreg.NativeBinding{own, binding("other", "source:b", epoch, generation, semreg.BindingCurrent)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("other", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"another epoch", []semreg.NativeBinding{own, binding("other", source, "epoch:b", generation, semreg.BindingCurrent)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("other", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"another generation", []semreg.NativeBinding{own, binding("other", source, epoch, "1", semreg.BindingCurrent)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("other", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"retired binding", []semreg.NativeBinding{own, binding("other", source, epoch, generation, semreg.BindingRetired)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("other", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
		{"fenced binding", []semreg.NativeBinding{own, binding("other", source, epoch, generation, semreg.BindingFenced)}, []semreg.IdentityLink{link("own", semreg.LinkWithdrawn), link("other", semreg.LinkQualified)}, mcp.HuaweiIdentityLinkWithdrawn},
	} {
		if got := huaweiStatusLinkState(semreg.Snapshot{Bindings: test.bindings, IdentityLinks: test.links}, source, epoch, generation); got != test.want {
			t.Errorf("%s: link state %q, want %q", test.name, got, test.want)
		}
	}
}

// A restarted generation that holds no link on the child's asset reports no
// link state and, with it, no counts: the three members are present together
// or absent together.
func TestHuaweiIdentityGrowthStatusAbsentWithoutPublishingLink(t *testing.T) {
	t.Parallel()
	cycle := growthFixtureCycle(t, "smartlogger")
	r := growthSetup(t, cycle)
	r.must(r.pub.Fence())
	successor, err := newHuaweiPVPublication(r.hub, "growth-test", "source:synthetic", "alpha", r.pub.handle, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	successor.mu.Lock()
	children := huaweiStatusChildren(successor, cycle)
	successor.mu.Unlock()
	if len(children) == 0 {
		t.Fatal("no children reported")
	}
	for _, child := range children {
		if child.IdentityLinkState != "" || child.BindingIncarnations != 0 || child.SourceDescriptors != 0 {
			t.Fatalf("child without a publishing link reports %+v", child)
		}
	}
}
