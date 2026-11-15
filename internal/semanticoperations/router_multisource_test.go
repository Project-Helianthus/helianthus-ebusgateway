package semanticoperations_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
)

// The shared-asset route fixture: one hub asset whose owner and one declared
// contributor each publish their own binding, service and capability of the
// synthetic operation pack. Every identity is synthetic.
const multisourceAsset semreg.AssetID = "asset:shared-route"

// multisourcePath is one publishing source of the shared asset and the route
// its capability admits.
type multisourcePath struct {
	source     semreg.SourceID
	driver     string
	epoch      semreg.SourceEpochID
	generation uint64
	binding    semreg.NativeBindingID
	service    semreg.ServiceInstanceID
	capability semreg.CapabilityInstanceID
	handle     *semhub.Handle
}

func (p multisourcePath) generationID() semreg.Uint64 {
	return semreg.Uint64(strconv.FormatUint(p.generation, 10))
}

func (p multisourcePath) route() operation.Route {
	return operation.Route{CapabilityInstance: p.capability, ServiceInstance: p.service, BindingID: p.binding, SourceID: p.source, SourceEpochID: p.epoch, DriverGeneration: p.generationID()}
}

// hubCapture adapts the hub's coherent capture to the router's capture seam.
type hubCapture struct{ hub *semhub.Hub }

func (c hubCapture) Capture(ctx context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	capture, err := c.hub.Capture(ctx, ids, time.Now)
	if err != nil {
		return nil, nil, err
	}
	inputs := make([]semanticview.Input, 0, len(capture.Assets))
	for _, item := range capture.Assets {
		inputs = append(inputs, semanticview.Input{AssetID: item.AssetID, Snapshot: item.Snapshot, Evaluation: item.Evaluation, UnavailableReason: item.UnavailableReason})
	}
	return capture.Metadata, inputs, nil
}

// recordingExecutor accepts every dispatch without terminal evidence and
// records the routes it received.
type recordingExecutor struct {
	mu     sync.Mutex
	routes []operation.Route
}

func (e *recordingExecutor) Execute(_ context.Context, dispatch semanticoperations.Dispatch) (semanticoperations.Completion, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, dispatch.Route)
	return semanticoperations.Completion{}, nil
}

func (e *recordingExecutor) received() []operation.Route {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]operation.Route(nil), e.routes...)
}

type multisourceHarness struct {
	hub       *semhub.Hub
	owner     multisourcePath
	peer      multisourcePath
	executors map[semreg.SourceID]*recordingExecutor
	router    *semanticoperations.Router
}

func newMultisourceHarness(t *testing.T) *multisourceHarness {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h := &multisourceHarness{hub: hub,
		owner: multisourcePath{source: "source:route-owner", driver: "driver-route-owner", epoch: "epoch:route-owner", generation: 2,
			binding: "binding:route-owner", service: "service:route-owner", capability: "capability:route-owner"},
		peer: multisourcePath{source: "source:route-peer", driver: "driver-route-peer", epoch: "epoch:route-peer", generation: 7,
			binding: "binding:route-peer", service: "service:route-peer", capability: "capability:route-peer"},
		executors: map[semreg.SourceID]*recordingExecutor{},
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: multisourceAsset, SourceID: h.owner.source, Driver: h.owner.driver, Validators: []semreg.PackValidator{semoptest.Pack{}},
		Contributors: []semhub.ContributorSpec{{SourceID: h.peer.source, Driver: h.peer.driver, Validators: []semreg.PackValidator{semoptest.Pack{}}}}}); err != nil {
		t.Fatalf("declare shared asset: %v", err)
	}
	lookup := &semoptest.Lookup{}
	for _, path := range []*multisourcePath{&h.owner, &h.peer} {
		handle, err := hub.AdmitSource(semhub.Admission{Source: path.source, Driver: path.driver, Legacy: semhub.LegacyGrowattStorage, Epoch: path.epoch, Generation: path.generation})
		if err != nil {
			t.Fatal(err)
		}
		path.handle = handle
		h.publish(t, *path)
		executor := &recordingExecutor{}
		h.executors[path.source] = executor
		lookup.Register(path.route(), semoptest.Operation, executor)
	}
	kernel, err := operation.NewKernel(semoptest.Pack{})
	if err != nil {
		t.Fatal(err)
	}
	f := semoptest.New(t)
	h.router = semanticoperations.NewRouter(semanticoperations.Config{Snapshots: hubCapture{hub: hub}, Kernel: kernel, Intents: f.Owner(), Executors: lookup})
	return h
}

func (h *multisourceHarness) tick() (semreg.TimePoint, semreg.MonotonicPoint) {
	at := time.Now()
	return semoptest.TimePoint(at), semreg.MonotonicPoint{ClockEpochID: "clock-epoch:shared-route", Nanoseconds: semreg.Uint64(strconv.FormatInt(at.UnixNano(), 10))}
}

// publish publishes the path's source descriptor, binding, identity link,
// service and capability on the shared asset through the hub.
func (h *multisourceHarness) publish(t *testing.T, path multisourcePath) {
	t.Helper()
	tx, err := h.hub.Begin(path.handle, multisourceAsset)
	if err != nil {
		t.Fatalf("begin %s: %v", path.source, err)
	}
	defer tx.Abort()
	wall, monotonic := h.tick()
	expected := semreg.Uint64("0")
	if current, ok := tx.Current(); ok {
		expected = current.Revisions.Semantic
	}
	generation := path.generationID()
	sequence := tx.NextSequence()
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:shared-route:" + string(path.source) + ":" + string(sequence)), AssetID: multisourceAsset,
		SourceID: path.source, SourceEpochID: path.epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts:       []semreg.SourceDescriptor{{SourceID: path.source, SourceEpochID: path.epoch, ProtocolID: "fixture.protocol", ProfileID: "fixture.profile", ProfileVersion: "1", RegistryEvidence: semoptest.Evidence("registry"), StartedAt: wall, State: semreg.SourceCurrent, Revision: "1"}},
		SourceRetirements:   []semreg.SourceEpochID{},
		BindingUpserts:      []semreg.NativeBinding{{BindingID: path.binding, AssetID: multisourceAsset, SourceID: path.source, SourceEpochID: path.epoch, DriverGeneration: generation, NativeResource: semoptest.Evidence("resource:" + string(path.source)), State: semreg.BindingCurrent, Revision: "1"}},
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: multisourceAsset, BindingID: path.binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{semoptest.Evidence("identity")}, Revision: "1"}},
		FactUpserts:         []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{{InstanceID: path.service, AssetID: multisourceAsset, Definition: semoptest.ServiceControl, BindingID: path.binding, SourceEpochID: path.epoch, DriverGeneration: generation,
			Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}},
		ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{{InstanceID: path.capability, AssetID: multisourceAsset, ServiceInstance: path.service, Definition: semoptest.CapabilityLevel, BindingID: path.binding,
			SourceEpochID: path.epoch, DriverGeneration: generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
			Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{semoptest.Evidence("activation")}, Revision: "1"}},
		CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(batch, monotonic); err != nil {
		t.Fatalf("apply %s: %v", path.source, err)
	}
	if outcome, err := tx.Commit(nil); err != nil || outcome != semhub.Accepted {
		t.Fatalf("commit %s: %v/%v", path.source, outcome, err)
	}
}

// fence fences the path's current generation on every asset of its source.
func (h *multisourceHarness) fence(t *testing.T, path multisourcePath) {
	t.Helper()
	wall, monotonic := h.tick()
	if _, err := h.hub.Fence(semhub.FenceRequest{Source: path.source, Epoch: path.epoch, Generation: path.generation, Reason: "lifecycle.generation_replaced",
		Evidence:   []semreg.EvidenceRef{semoptest.Evidence("fence")},
		ObservedAt: wall, Monotonic: monotonic}); err != nil {
		t.Fatalf("fence %s: %v", path.source, err)
	}
}

// request addresses the path's capability against the current publication.
func (h *multisourceHarness) request(t *testing.T, path multisourcePath) semanticoperations.Request {
	t.Helper()
	view, err := h.hub.Inspect(multisourceAsset)
	if err != nil || !view.Present {
		t.Fatalf("inspect: %v", err)
	}
	return semanticoperations.Request{
		AssetID: multisourceAsset, BindingID: path.binding, CapabilityInstanceID: path.capability, Operation: semoptest.Operation,
		Arguments:                          []semreg.TypedField{{ID: semoptest.FieldLevel.ID, Value: semoptest.Bool(true)}},
		ExpectedSemanticRevision:           view.Snapshot.Revisions.Semantic,
		ExpectedCapabilityRevision:         view.Snapshot.Revisions.Capabilities,
		ExpectedCapabilityInstanceRevision: "1",
		ExpectedSourceEpochID:              path.epoch,
		ExpectedDriverGeneration:           path.generationID(),
		Preconditions:                      []operation.Precondition{},
		Timeout:                            2 * time.Second,
	}
}

func (h *multisourceHarness) route(t *testing.T, request semanticoperations.Request) view {
	t.Helper()
	result, err := h.router.Route(context.Background(), request)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	return viewOf(result)
}

// A contributor's capability on a shared asset routes by the contributor's
// own binding, source, epoch and generation, to the executor of that route
// only; the owner's capability routes to the owner's.
func TestMultisourceRouteResolvesEachContributorByItsOwnBinding(t *testing.T) {
	h := newMultisourceHarness(t)
	accepted := view{"accepted", null, null, null, null, null}
	for _, path := range []multisourcePath{h.peer, h.owner} {
		if got := h.route(t, h.request(t, path)); got != accepted {
			t.Fatalf("%s route=%+v want %+v", path.source, got, accepted)
		}
		routes := h.executors[path.source].received()
		if len(routes) != 1 || routes[0] != path.route() {
			t.Fatalf("%s executor routes=%+v want %+v", path.source, routes, path.route())
		}
	}
	// Each executor saw only its own route: one dispatch each.
	if len(h.executors[h.owner.source].received()) != 1 || len(h.executors[h.peer.source].received()) != 1 {
		t.Fatal("a dispatch reached the other contributor's executor")
	}

	// A request that names one contributor's capability with the other's
	// binding, epoch or generation is refused without a send.
	before := map[semreg.SourceID]int{h.owner.source: 1, h.peer.source: 1}
	mismatches := map[string]struct {
		mutate func(*semanticoperations.Request)
		reason string
	}{
		"owner binding":    {func(r *semanticoperations.Request) { r.BindingID = h.owner.binding }, semanticoperations.ReasonBindingMismatch},
		"owner epoch":      {func(r *semanticoperations.Request) { r.ExpectedSourceEpochID = h.owner.epoch }, string(semreg.StaleSourceEpoch)},
		"owner generation": {func(r *semanticoperations.Request) { r.ExpectedDriverGeneration = h.owner.generationID() }, string(semreg.StaleDriverGeneration)},
	}
	for name, mismatch := range mismatches {
		request := h.request(t, h.peer)
		mismatch.mutate(&request)
		if got := h.route(t, request); got != refusedView(mismatch.reason) {
			t.Fatalf("%s: route=%+v", name, got)
		}
	}
	for source, calls := range before {
		if got := len(h.executors[source].received()); got != calls {
			t.Fatalf("a refused request reached %s: %d dispatches", source, got)
		}
	}
}

// A fenced contributor's capability is refused without a send, while the
// asset stays admitted through the other contributor, whose capability still
// routes.
func TestMultisourceFencedContributorCapabilityIsRefused(t *testing.T) {
	for _, fenced := range []string{"peer", "owner"} {
		t.Run(fenced, func(t *testing.T) {
			h := newMultisourceHarness(t)
			stale, live := h.peer, h.owner
			if fenced == "owner" {
				stale, live = h.owner, h.peer
			}
			request := h.request(t, stale)
			h.fence(t, stale)
			// Both a request prepared before the fence and one against the
			// current publication are refused by the kernel's generation check.
			for name, request := range map[string]semanticoperations.Request{"prepared before the fence": request, "against the current publication": h.request(t, stale)} {
				if got := h.route(t, request); got != refusedView(string(semreg.StaleDriverGeneration)) {
					t.Fatalf("fenced %s capability %s: route=%+v", fenced, name, got)
				}
			}
			if calls := len(h.executors[stale.source].received()); calls != 0 {
				t.Fatalf("the fenced %s executor was dispatched %d times", fenced, calls)
			}
			if got := h.route(t, h.request(t, live)); got != (view{"accepted", null, null, null, null, null}) {
				t.Fatalf("the unfenced contributor's capability=%+v", got)
			}
			if routes := h.executors[live.source].received(); len(routes) != 1 || routes[0] != live.route() {
				t.Fatalf("the unfenced contributor's executor routes=%+v", routes)
			}
		})
	}
}
