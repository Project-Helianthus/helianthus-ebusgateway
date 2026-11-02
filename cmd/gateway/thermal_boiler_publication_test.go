package main

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func m15Fixture(t *testing.T) (*boilerThermalPublisher, *semhub.Handle, *int64) {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := hub.AdmitSource(semhub.Admission{Source: "source:synthetic-boiler", Driver: string(primaryEBusDriverID)})
	if err != nil {
		t.Fatal(err)
	}
	id := boilerThermalIdentity{Asset: "asset:synthetic-boiler", Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.GenerationID(), Binding: "binding:synthetic-boiler", Service: "service:synthetic-boiler", Product: "synthetic-boiler-family", Profile: "boiler.synthetic", Revision: "1", Codec: "DATA2c", ResponseForm: "operation_selector", Initiator: 0x31, Target: 0x36, PB: 0xb5, SB: 9, Operation: 0x0d, Selector: [2]byte{0x18, 0}}
	tick := int64(0)
	pub := &boilerThermalPublisher{hub: hub, handle: func() (*semhub.Handle, error) { return handle, nil }}
	pub.now = func() (semreg.TimePoint, semreg.MonotonicPoint) {
		return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(1800000000000000000+tick, 10)), ClockID: "clock.utc", UncertaintyNS: "0"}, semreg.MonotonicPoint{ClockEpochID: "clock:synthetic-boiler", Nanoseconds: semreg.Uint64(strconv.FormatInt(tick, 10))}
	}
	wall, mono := pub.now()
	o := &boilerThermalObservation{Identity: id, Request: []byte{0x0d, 0x18, 0}, Reply: []byte{0x0d, 0x18, 0, 0x20, 3}, Received: wall, Receipt: mono, Valid: true}
	pub.binding = &boilerThermalBinding{Identity: id, Qualified: true, Basis: []semreg.EvidenceRef{boilerThermalEvidence(o)}, Policy: semreg.FreshnessPolicy{PolicyID: "policy:synthetic-boiler", Version: "1.0.0", FreshForNS: "100", RetainForNS: "200", MaxWallUncertaintyNS: "0"}}
	pub.attempt = o
	return pub, handle, &tick
}

func TestM15QualifiedFlowOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want float64
	}{{"normal", []byte{0x20, 3}, 50}, {"zero", []byte{0, 0}, 0}, {"lower", []byte{0xc0, 0xf9}, -100}, {"upper", []byte{0x80, 0x0c}, 200}, {"fraction", []byte{1, 0}, 0.0625}} {
		t.Run(tc.name, func(t *testing.T) {
			p, _, _ := m15Fixture(t)
			p.attempt.Reply = append([]byte{0x0d, 0x18, 0}, tc.raw...)
			got := p.scalar(p.binding.Identity.Target)
			if got == nil || *got != tc.want {
				t.Fatalf("flow=%v, error=%v", got, p.lastError)
			}
			view, err := p.hub.Inspect(p.binding.Identity.Asset)
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Snapshot.Facts) != 1 || len(view.Snapshot.Facts[0].Candidates) != 1 {
				t.Fatalf("facts=%+v", view.Snapshot.Facts)
			}
			f := view.Snapshot.Facts[0].Candidates[0]
			if !reflect.DeepEqual(f.Key, boilerThermalKey()) || f.Times.ReceiptMonotonic != p.attempt.Receipt || f.Times.ReceivedAt != p.attempt.Received || f.Times.SourceAt != nil || f.Times.PhenomenonAt != nil {
				t.Fatalf("fact=%+v", f)
			}
			if len(p.legacyProjection.Loss) != 4 {
				t.Fatal("legacy scalar losses missing")
			}
			if len(f.Evidence) != 1 || f.Evidence[0] != boilerThermalEvidence(p.attempt) {
				t.Fatal("native evidence lost")
			}
		})
	}
}

func TestM15RejectWithoutDefaultOrAgeRefresh(t *testing.T) {
	for _, raw := range [][]byte{nil, {0x0d, 0x18, 0}, {0x0d, 0x18, 0, 1}, {0x0d, 0x18, 0, 0x20, 3, 0}, {0x0d, 0x19, 0, 0x20, 3}, {0x0d, 0x18, 1, 0x20, 3}, {0x0e, 0x18, 0, 0x20, 3}, {0x0d, 0x18, 0, 0, 0x80}, {0x0d, 0x18, 0, 0xbf, 0xf9}, {0x0d, 0x18, 0, 0x81, 0x0c}} {
		p, _, tick := m15Fixture(t)
		original := *p.attempt
		if p.scalar(p.binding.Identity.Target) == nil {
			t.Fatal(p.lastError)
		}
		*tick = 50
		wall, mono := p.now()
		p.attempt = &boilerThermalObservation{Identity: p.binding.Identity, Request: original.Request, Reply: raw, Valid: true, Received: wall, Receipt: mono}
		if got := p.scalar(p.binding.Identity.Target); got == nil || *got != 50 {
			t.Fatalf("retention=%v error=%v", got, p.lastError)
		}
		if !reflect.DeepEqual(p.good, &original) {
			t.Fatal("failure changed retained native evidence")
		}
		*tick = 101
		if p.scalar(p.binding.Identity.Target) != nil {
			t.Fatal("stale scalar admitted")
		}
		*tick = 201
		if p.scalar(p.binding.Identity.Target) != nil {
			t.Fatal("expired scalar admitted")
		}
		q, _, _ := m15Fixture(t)
		q.attempt.Reply = raw
		if q.scalar(q.binding.Identity.Target) != nil {
			t.Fatal("invalid sample seeded state")
		}
	}
}

func TestM15QualificationPolicyAndLifecycleWithholding(t *testing.T) {
	for _, mutate := range []func(*boilerThermalPublisher){func(p *boilerThermalPublisher) { p.binding.Qualified = false }, func(p *boilerThermalPublisher) { p.binding = nil }, func(p *boilerThermalPublisher) { p.binding.Policy = semreg.FreshnessPolicy{} }, func(p *boilerThermalPublisher) { p.binding.Basis = nil }, func(p *boilerThermalPublisher) {
		p.handle = func() (*semhub.Handle, error) { return nil, errors.New("not admitted") }
	}} {
		p, _, _ := m15Fixture(t)
		mutate(p)
		if p.scalar(0x36) != nil {
			t.Fatal("missing prerequisites admitted")
		}
	}
	p, h, _ := m15Fixture(t)
	if p.scalar(0x36) == nil {
		t.Fatal(p.lastError)
	}
	p.binding.Qualified = false
	if p.scalar(0x36) != nil {
		t.Fatal("qualification loss retained scalar")
	}
	if p.withheldNative == nil || len(p.withheldNative.Reply) == 0 {
		t.Fatal("qualification loss discarded native evidence")
	}
	view, _ := p.hub.Inspect(p.binding.Identity.Asset)
	for _, e := range view.Snapshot.Facts {
		for _, f := range e.Candidates {
			if f.Quality.Availability == semreg.AvailabilityAvailable {
				t.Fatal("qualification loss left hub fact available")
			}
		}
	}
	p.binding.Qualified = true
	wall, mono := p.now()
	if _, err := p.hub.RetireEpoch(semhub.RetireRequest{Source: h.Source(), Epoch: h.Epoch(), Reason: "boiler.test.retire", Evidence: p.binding.Basis, ObservedAt: wall, Monotonic: mono}); err != nil {
		t.Fatal(err)
	}
	if p.scalar(0x36) != nil {
		t.Fatal("withdrawn lifecycle handle published")
	}
}

func TestM15IdentityEveryField(t *testing.T) {
	p, _, _ := m15Fixture(t)
	typ := reflect.TypeOf(p.binding.Identity)
	for i := 0; i < typ.NumField(); i++ {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			q, _, _ := m15Fixture(t)
			v := reflect.ValueOf(&q.attempt.Identity).Elem().Field(i)
			switch v.Kind() {
			case reflect.String:
				v.SetString(v.String() + "-other")
			case reflect.Uint8:
				v.SetUint(v.Uint() + 1)
			case reflect.Array:
				v.Index(1).SetUint(1)
			default:
				t.Fatal("uncovered identity field")
			}
			if q.scalar(0x36) != nil {
				t.Fatal("foreign observation admitted")
			}
		})
	}
	q, _, _ := m15Fixture(t)
	q.attempt.Identity.Selector[0]++
	if q.scalar(0x36) != nil {
		t.Fatal("foreign selector first byte admitted")
	}
}

func TestM15SnapshotExcludesMirrorAndCache(t *testing.T) {
	pub, _, _ := m15Fixture(t)
	poller := &vaillantSemanticPoller{provider: graphql.NewLiveSemanticProvider(), boilerAddress: 0x36, boilerThermal: pub, boiler: &vaillantBoilerSnapshot{FlowTemperatureC: m15Float(99)}}
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if got := poller.provider.BoilerStatus().State.FlowTemperatureC; got == nil || *got != 50 {
		t.Fatalf("flow=%v error=%v", got, pub.lastError)
	}
	pub.binding = nil
	poller.publishBoilerStatus(semanticSnapshotSourceCache)
	if poller.provider.BoilerStatus().State.FlowTemperatureC != nil {
		t.Fatal("legacy cached flow admitted")
	}
}

func TestM15RestartWithholdsCachedFlowKeepsLegacyReturnUntilFirstMerge(t *testing.T) {
	store := newSemanticCacheStore(filepath.Join(t.TempDir(), "semantic-cache.json"), t.Logf)
	if err := store.writeV3(semanticCacheV3{
		SchemaVersion: semanticCacheSchemaVersionV3,
		Boiler: &semanticCacheBoiler{
			FlowTemperatureC:   m15Float(99),
			ReturnTemperatureC: m15Float(88),
			DhwTemperatureC:    m15Float(45),
		},
	}); err != nil {
		t.Fatal(err)
	}
	provider := graphql.NewLiveSemanticProvider()
	snapshot, ok := preloadSemanticCache(provider, store)
	if !ok || snapshot.Boiler == nil {
		t.Fatal("populated boiler cache was discarded wholesale")
	}
	// Check the public provider before attaching a publisher or polling: no
	// later reprojection may be needed to remove provenance-free flow.
	status := provider.BoilerStatus()
	if status == nil || status.State.FlowTemperatureC != nil {
		t.Fatalf("pre-polling boiler status exposes cached flow: %+v", status)
	}
	if got := status.State.ReturnTemperatureC; got == nil || *got != 88 {
		t.Fatalf("pre-polling legacy return=%v, want cached 88", got)
	}
	if status.State.DhwTemperatureC == nil || *status.State.DhwTemperatureC != 45 {
		t.Fatal("unrelated cached boiler field lost")
	}
	pub, _, _ := m15Fixture(t)
	fresh := pub.attempt
	pub.attempt = nil
	poller := &vaillantSemanticPoller{provider: provider, boilerAddress: 0x36, boilerThermal: pub}
	poller.hydrateFromCache(snapshot)
	if poller.boiler == nil || poller.boiler.FlowTemperatureC != nil {
		t.Fatal("hydration restored provenance-free flow")
	}
	if got := poller.boiler.ReturnTemperatureC; got == nil || *got != 88 {
		t.Fatalf("hydrated legacy return=%v, want cached 88", got)
	}
	poller.publishBoilerStatus(semanticSnapshotSourceCache)
	view, err := pub.hub.Inspect(pub.binding.Identity.Asset)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Facts) != 0 || pub.good != nil || provider.BoilerStatus().State.FlowTemperatureC != nil {
		t.Fatal("cache seeded a flow candidate or retained observation")
	}
	if got := provider.BoilerStatus().State.ReturnTemperatureC; got == nil || *got != 88 {
		t.Fatalf("cache publication legacy return=%v, want cached 88", got)
	}
	poller.retainBoilerThermalRead(protocol.Frame{FrameType: protocol.FrameTypeInitiatorTarget, Source: 0x31, Target: 0x36, Primary: 0xb5, Secondary: 9, Data: fresh.Request}, &protocol.Frame{Data: fresh.Reply}, nil)
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if got := provider.BoilerStatus().State.FlowTemperatureC; got == nil || *got != 50 {
		t.Fatalf("first fresh flow=%v error=%v", got, pub.lastError)
	}
	view, err = pub.hub.Inspect(pub.binding.Identity.Asset)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Facts) != 1 || len(view.Snapshot.Facts[0].Candidates) != 1 || pub.good == nil {
		t.Fatal("fresh qualified observation did not publish a flow candidate")
	}
	if got := provider.BoilerStatus().State.ReturnTemperatureC; got == nil || *got != 88 {
		t.Fatalf("fresh flow changed legacy return=%v, want cached 88", got)
	}
	// The first boiler merge retains the pre-existing legacy return behavior:
	// even an unrelated successful read clears the restored return value.
	poller.boiler = mergeBoilerSnapshotNonDestructive(poller.boiler, &vaillantBoilerSnapshot{WaterPressureBar: m15Float(1.5)})
	if poller.boiler.ReturnTemperatureC != nil {
		t.Fatal("first boiler merge retained cached legacy return")
	}
	if got := poller.boiler.WaterPressureBar; got == nil || *got != 1.5 {
		t.Fatalf("merged water pressure=%v, want 1.5", got)
	}
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if provider.BoilerStatus().State.ReturnTemperatureC != nil {
		t.Fatal("first boiler merge still serves cached legacy return")
	}
	if got := provider.BoilerStatus().State.FlowTemperatureC; got == nil || *got != 50 {
		t.Fatalf("flow after first boiler merge=%v error=%v", got, pub.lastError)
	}
	view, err = pub.hub.Inspect(pub.binding.Identity.Asset)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Facts) != 1 || len(view.Snapshot.Facts[0].Candidates) != 1 || !reflect.DeepEqual(view.Snapshot.Facts[0].Candidates[0].Key, boilerThermalKey()) || pub.good == nil {
		t.Fatal("first boiler merge changed qualified flow publication or emitted a return fact")
	}
}

func TestM15AcquisitionRetainsReceipt(t *testing.T) {
	pub, _, tick := m15Fixture(t)
	poller := &vaillantSemanticPoller{boilerThermal: pub}
	request := protocol.Frame{FrameType: protocol.FrameTypeInitiatorTarget, Source: 0x31, Target: 0x36, Primary: 0xb5, Secondary: 9, Data: []byte{0x0d, 0x18, 0}}
	reply := &protocol.Frame{Data: []byte{0x0d, 0x18, 0, 0x20, 3}}
	poller.retainBoilerThermalRead(request, reply, nil)
	reply.Data[3] = 0
	if pub.scalar(0x36) == nil {
		t.Fatal(pub.lastError)
	}
	receipt := pub.good.Receipt
	*tick = 50
	if pub.scalar(0x36) == nil || pub.good.Receipt != receipt {
		t.Fatal("evaluation refreshed native receipt")
	}
	poller.retainBoilerThermalRead(request, nil, errors.New("synthetic read failure"))
	if pub.scalar(0x36) == nil || pub.good.Receipt != receipt {
		t.Fatalf("failure changed projection: error=%v, good=%+v, original=%+v", pub.lastError, pub.good, receipt)
	}
}

func m15Float(v float64) *float64 { return &v }

func TestM15UnqualifiedFlowOnlyCyclePublishesChangedDHW(t *testing.T) {
	pub, _, _ := m15Fixture(t)
	pub.binding.Qualified = false
	pub.attempt = nil
	poller := &vaillantSemanticPoller{
		scheduler:     ebusgateway.NewSemanticReadScheduler(),
		provider:      graphql.NewLiveSemanticProvider(),
		hub:           graphql.NewBroadcastHub(nil),
		source:        pub.binding.Identity.Initiator,
		controller:    0x15,
		boilerAddress: pub.binding.Identity.Target,
		boilerThermal: pub,
		boiler:        &vaillantBoilerSnapshot{DhwTemperatureC: m15Float(40)},
		dhw:           &vaillantDhwSnapshot{CurrentTempC: m15Float(47.5)},
		// No circuit snapshot supplies a fresh circuit-state mirror.
	}
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if got := poller.provider.BoilerStatus(); got == nil || got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 40 || got.State.FlowTemperatureC != nil {
		t.Fatalf("initial public boiler status = %+v; want DHW 40 and withheld flow", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := poller.hub.SubscribeBoiler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flowReads := 0
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if frame.Primary != vaillantB509Primary || frame.Secondary != vaillantB509Secondary {
			t.Fatalf("unexpected non-B509 read: %+v", frame)
		}
		if reflect.DeepEqual(frame.Data, buildB509ReadSelector(boiler_b509_flow_temperature)) {
			flowReads++
			return &protocol.Frame{Data: []byte{0x0d, 0x18, 0, 0x20, 3}}, nil
		}
		return nil, errors.New("synthetic unrelated read failure")
	}

	poller.refreshBoilerStatusTier(ctx, boilerStatusTierFast)

	if flowReads != 1 || pub.attempt == nil || !pub.attempt.Valid {
		t.Fatal("cycle did not acquire exactly one successful native flow read")
	}
	if poller.boiler.DhwTemperatureC == nil || *poller.boiler.DhwTemperatureC != 47.5 || poller.boiler.FlowTemperatureC != nil {
		t.Fatalf("internal boiler = %+v; want merged DHW 47.5 and withheld flow", poller.boiler)
	}
	if pub.good != nil || pub.scalar(poller.boilerAddress) != nil {
		t.Fatal("unqualified acquisition supplied a flow fact")
	}
	if got := poller.provider.BoilerStatus(); got == nil || got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 47.5 || got.State.FlowTemperatureC != nil {
		t.Fatalf("public boiler status = %+v; want changed DHW 47.5 and withheld flow", got)
	}
	select {
	case update := <-updates:
		got := update.(*graphql.BoilerStatus)
		if got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 47.5 || got.State.FlowTemperatureC != nil {
			t.Fatalf("subscriber boiler status = %+v; want changed DHW 47.5 and withheld flow", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("changed DHW was not published to boiler subscribers")
	}
}

func TestM15FreshSystemFlowMirrorPublishesChangedDHWWithFailedB509(t *testing.T) {
	pub, _, _ := m15Fixture(t)
	observedAt := time.Date(2026, time.October, 20, 12, 0, 0, 0, time.UTC)
	pub.attempt = nil
	poller := &vaillantSemanticPoller{
		scheduler:     ebusgateway.NewSemanticReadScheduler(),
		provider:      graphql.NewLiveSemanticProvider(),
		hub:           graphql.NewBroadcastHub(nil),
		source:        pub.binding.Identity.Initiator,
		controller:    0x15,
		boilerAddress: pub.binding.Identity.Target,
		boilerThermal: pub,
		nowFn:         func() time.Time { return observedAt.Add(time.Second) },
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       m15Float(65),
			SystemFlowTemperatureLiveAt: observedAt,
		},
		boiler: &vaillantBoilerSnapshot{DhwTemperatureC: m15Float(40)},
		dhw:    &vaillantDhwSnapshot{CurrentTempC: m15Float(47.5)},
		// No circuit snapshot supplies a fresh circuit-state mirror.
	}
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if got := poller.provider.BoilerStatus(); got == nil || got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 40 || got.State.FlowTemperatureC != nil {
		t.Fatalf("initial public boiler status = %+v; want DHW 40 and withheld flow", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := poller.hub.SubscribeBoiler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	flowReads, failedReads := 0, 0
	poller.sendFrameFn = func(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
		if frame.Primary != vaillantB509Primary || frame.Secondary != vaillantB509Secondary {
			t.Fatalf("unexpected non-B509 read: %+v", frame)
		}
		if reflect.DeepEqual(frame.Data, buildB509ReadSelector(boiler_b509_flow_temperature)) {
			flowReads++
		}
		failedReads++
		return nil, errors.New("synthetic B509 read failure")
	}

	poller.refreshBoilerStatusTier(ctx, boilerStatusTierFast)

	if flowReads != 3 || failedReads <= flowReads || pub.attempt == nil || pub.attempt.Valid {
		t.Fatal("cycle did not fail every B509 read, including all three flow attempts")
	}
	if poller.boiler.DhwTemperatureC == nil || *poller.boiler.DhwTemperatureC != 47.5 || poller.boiler.FlowTemperatureC != nil {
		t.Fatalf("internal boiler = %+v; want merged DHW 47.5 and withheld flow", poller.boiler)
	}
	if pub.good != nil || pub.scalar(poller.boilerAddress) != nil {
		t.Fatal("system-flow mirror or failed B509 acquisition supplied a flow fact")
	}
	view, err := pub.hub.Inspect(pub.binding.Identity.Asset)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot.Facts) != 0 {
		t.Fatal("system-flow mirror seeded a SemReg flow fact")
	}
	if got := poller.provider.BoilerStatus(); got == nil || got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 47.5 || got.State.FlowTemperatureC != nil {
		t.Fatalf("public boiler status = %+v; want changed DHW 47.5 and withheld flow", got)
	}
	select {
	case update := <-updates:
		got := update.(*graphql.BoilerStatus)
		if got.State.DhwTemperatureC == nil || *got.State.DhwTemperatureC != 47.5 || got.State.FlowTemperatureC != nil {
			t.Fatalf("subscriber boiler status = %+v; want changed DHW 47.5 and withheld flow", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("changed DHW was not published to boiler subscribers")
	}
}

func TestM15NewObservationAndFreshnessBoundaries(t *testing.T) {
	p, _, tick := m15Fixture(t)
	if p.scalar(0x36) == nil {
		t.Fatal(p.lastError)
	}
	*tick = 50
	o := *p.attempt
	o.Reply = []byte{0x0d, 0x18, 0, 0x30, 3}
	o.Received, o.Receipt = p.now()
	p.attempt = &o
	if got := p.scalar(0x36); got == nil || *got != 51 {
		t.Fatalf("new value=%v error=%v", got, p.lastError)
	}
	*tick = 149
	if p.scalar(0x36) == nil {
		t.Fatal("fresh before deadline withheld")
	}
	*tick = 150
	if p.scalar(0x36) != nil {
		t.Fatal("fresh deadline extended")
	}
	*tick = 250
	if p.scalar(0x36) != nil {
		t.Fatal("retention deadline extended")
	}
	if p.good.Receipt != o.Receipt {
		t.Fatal("retention receipt changed")
	}
}

func TestM15CurrentIdentityEveryFieldWithdraws(t *testing.T) {
	p, _, _ := m15Fixture(t)
	typ := reflect.TypeOf(p.binding.Identity)
	for i := 0; i < typ.NumField(); i++ {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			q, _, _ := m15Fixture(t)
			id := q.binding.Identity
			if q.scalar(0x36) == nil {
				t.Fatal(q.lastError)
			}
			v := reflect.ValueOf(&q.binding.Identity).Elem().Field(i)
			switch v.Kind() {
			case reflect.String:
				v.SetString(v.String() + "-other")
			case reflect.Uint8:
				v.SetUint(v.Uint() + 1)
			case reflect.Array:
				v.Index(1).SetUint(1)
			default:
				t.Fatal("uncovered field")
			}
			if q.scalar(0x36) != nil {
				t.Fatal("conflicting current identity retained scalar")
			}
			view, _ := q.hub.Inspect(id.Asset)
			for _, e := range view.Snapshot.Facts {
				for _, f := range e.Candidates {
					if f.Quality.Availability == semreg.AvailabilityAvailable {
						t.Fatal("conflict left hub fact available")
					}
				}
			}
		})
	}
}

func TestM15SelectorOnlyAndClockLoss(t *testing.T) {
	p, _, tick := m15Fixture(t)
	p.binding.Identity.ResponseForm = "selector"
	p.attempt.Identity = p.binding.Identity
	p.attempt.Reply = []byte{0x18, 0, 0x20, 3}
	if p.scalar(0x36) == nil {
		t.Fatal(p.lastError)
	}
	original := p.now
	p.now = func() (semreg.TimePoint, semreg.MonotonicPoint) {
		w, m := original()
		m.ClockEpochID = "clock:other"
		return w, m
	}
	*tick = 1
	if p.scalar(0x36) != nil {
		t.Fatal("clock discontinuity promoted observation")
	}
}

func TestM15LifecycleClearsLegacyProjection(t *testing.T) {
	for _, control := range []string{"retire", "fence"} {
		t.Run(control, func(t *testing.T) { testM15LifecycleClearsLegacyProjection(t, control) })
	}
}

func testM15LifecycleClearsLegacyProjection(t *testing.T, control string) {
	fixture, h, _ := m15Fixture(t)
	poller := &vaillantSemanticPoller{provider: graphql.NewLiveSemanticProvider(), boilerAddress: 0x36, hub: graphql.NewBroadcastHub(nil)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := poller.hub.SubscribeBoiler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	poller.attachBoilerThermal(fixture.hub, fixture.handle)
	pub := poller.boilerThermal
	pub.binding, pub.attempt, pub.now = fixture.binding, fixture.attempt, fixture.now
	ack := make(chan struct{}, 1)
	original := pub.onControl
	pub.onControl = func(e semhub.ControlEvent) error { err := original(e); ack <- struct{}{}; return err }
	poller.publishBoilerStatus(semanticSnapshotSourceLive)
	if poller.provider.BoilerStatus().State.FlowTemperatureC == nil {
		t.Fatal(pub.lastError)
	}
	select {
	case update := <-updates:
		if update.(*graphql.BoilerStatus).State.FlowTemperatureC == nil {
			t.Fatal("initial subscription flow missing")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("initial subscription update missing")
	}
	wall, mono := pub.now()
	if control == "retire" {
		_, err = pub.hub.RetireEpoch(semhub.RetireRequest{Source: h.Source(), Epoch: h.Epoch(), Reason: "boiler.test.retire", Evidence: pub.binding.Basis, ObservedAt: wall, Monotonic: mono})
	} else {
		_, err = pub.hub.Fence(semhub.FenceRequest{Source: h.Source(), Epoch: h.Epoch(), Generation: h.Generation(), Reason: "boiler.test.fence", Evidence: pub.binding.Basis, ObservedAt: wall, Monotonic: mono})
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ack:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle reprojection did not settle")
	}
	if poller.provider.BoilerStatus().State.FlowTemperatureC != nil {
		t.Fatal("retired flow remained on legacy surface")
	}
	select {
	case update := <-updates:
		if update.(*graphql.BoilerStatus).State.FlowTemperatureC != nil {
			t.Fatal("lifecycle subscription retained flow")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle subscription update missing")
	}
}

func TestM15PayloadOnlyRequiresExactLength(t *testing.T) {
	for _, tc := range []struct {
		name  string
		raw   []byte
		valid bool
	}{
		{"normal", []byte{0x20, 3}, true},
		{"zero", []byte{0, 0}, true},
		{"fraction", []byte{1, 0}, true},
		{"missing", nil, false},
		{"short", []byte{0x20}, false},
		{"long", []byte{0x20, 3, 0}, false},
		{"selector", []byte{0x18, 0, 0x20, 3}, false},
		{"operation_selector", []byte{0x0d, 0x18, 0, 0x20, 3}, false},
		{"sentinel", []byte{0, 0x80}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub, _, _ := m15Fixture(t)
			pub.binding.Identity.ResponseForm = "payload"
			poller := &vaillantSemanticPoller{boilerThermal: pub}
			poller.retainBoilerThermalRead(protocol.Frame{FrameType: protocol.FrameTypeInitiatorTarget, Source: 0x31, Target: 0x36, Primary: 0xb5, Secondary: 9, Data: []byte{0x0d, 0x18, 0}}, &protocol.Frame{Data: tc.raw}, nil)
			if got := pub.scalar(0x36); (got != nil) != tc.valid {
				t.Fatalf("scalar=%v valid=%v error=%v", got, tc.valid, pub.lastError)
			}
		})
	}
}

func TestM15SuccessorPublishesAfterBindingChange(t *testing.T) {
	for _, transition := range []string{"epoch", "generation"} {
		t.Run(transition, func(t *testing.T) {
			pub, old, tick := m15Fixture(t)
			if pub.scalar(0x36) == nil {
				t.Fatal(pub.lastError)
			}
			*tick = 1
			wall, mono := pub.now()
			var next *semhub.Handle
			var err error
			if transition == "epoch" {
				_, err = pub.hub.RetireEpoch(semhub.RetireRequest{Source: old.Source(), Epoch: old.Epoch(), Reason: "boiler.test.retire", Evidence: pub.binding.Basis, ObservedAt: wall, Monotonic: mono})
				if err != nil {
					t.Fatal(err)
				}
				next, err = pub.hub.AdmitSuccessor(semhub.Succession{Previous: old, Operation: "boiler.test.restart"})
			} else {
				var tx *semhub.Tx
				tx, err = pub.hub.BeginGeneration(old, pub.binding.Identity.Asset, old.Generation()+1)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Abort()
				current, _ := tx.Current()
				batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: "batch:synthetic-transition", AssetID: pub.binding.Identity.Asset, SourceID: old.Source(), SourceEpochID: old.Epoch(), DriverGeneration: "2", Sequence: tx.NextSequence(), ExpectedSemanticRevision: current.Revisions.Semantic, ObservedAt: wall,
					GenerationFences: []semreg.GenerationFence{{SourceID: old.Source(), SourceEpochID: old.Epoch(), DriverGeneration: old.GenerationID(), Reason: "boiler.test.replaced", Evidence: pub.binding.Basis, Revision: "1"}}}
				batch.SourceUpserts = []semreg.SourceDescriptor{}
				batch.BindingUpserts = []semreg.NativeBinding{}
				batch.IdentityLinkUpserts = []semreg.IdentityLink{}
				batch.FactUpserts = []semreg.FactCandidate{}
				batch.FactWithdrawals = []semreg.CandidateID{}
				batch.ServiceUpserts = []semreg.ServiceInstance{}
				batch.ServiceWithdrawals = []semreg.ServiceInstanceID{}
				batch.CapabilityUpserts = []semreg.CapabilityInstance{}
				batch.CapabilityWithdrawals = []semreg.CapabilityInstanceID{}
				batch.SourceRetirements = []semreg.SourceEpochID{}
				batch.BatchDigest, err = batch.ComputedDigest()
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err = tx.Apply(batch, mono); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Commit(nil); err != nil {
					t.Fatal(err)
				}
				next = tx.Handle()
			}
			if err != nil {
				t.Fatal(err)
			}
			pub.handle = func() (*semhub.Handle, error) { return next, nil }
			pub.binding.Identity.Epoch = next.Epoch()
			pub.binding.Identity.Generation = next.GenerationID()
			pub.binding.Identity.Binding = "binding:synthetic-successor"
			o := *pub.attempt
			o.Identity = pub.binding.Identity
			o.Received, o.Receipt = pub.now()
			o.Reply = []byte{0x0d, 0x18, 0, 0x30, 3}
			pub.attempt = &o
			for i := 0; i < 2; i++ {
				if got := pub.scalar(0x36); got == nil || *got != 51 {
					t.Fatalf("successor scalar=%v error=%v", got, pub.lastError)
				}
			}
			if pub.published.Identity != pub.binding.Identity {
				t.Fatal("successor binding not tracked")
			}
		})
	}
}
