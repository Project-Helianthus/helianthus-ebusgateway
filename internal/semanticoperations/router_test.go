package semanticoperations_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
)

const null = "<null>"

type view struct{ status, outcome, delivery, ack, readback, reason string }

func viewOf(result semanticoperations.Result) view {
	v := view{status: string(result.Status), outcome: null, delivery: null, ack: null, readback: null, reason: null}
	if result.KernelOutcome != nil {
		v.outcome = string(*result.KernelOutcome)
	}
	if result.Delivery != nil {
		v.delivery = string(*result.Delivery)
	}
	if result.Acknowledgement != nil {
		v.ack = string(*result.Acknowledgement)
	}
	if result.ReadbackRelation != nil {
		v.readback = string(*result.ReadbackRelation)
	}
	if result.Reason != nil {
		v.reason = *result.Reason
	}
	return v
}

func refusedView(reason string) view {
	return view{status: "failed", outcome: "rejected", delivery: "not_sent", ack: null, readback: null, reason: reason}
}

type harness struct {
	f        *semoptest.Fixture
	owner    *semoptest.Owner
	lookup   *semoptest.Lookup
	selected *semoptest.Executor
	router   *semanticoperations.Router
}

func newHarness(t *testing.T, outcome semoptest.Outcome) harness {
	t.Helper()
	f := semoptest.New(t)
	h := harness{f: f, owner: f.Owner(), lookup: &semoptest.Lookup{}, selected: f.Executor(outcome)}
	h.lookup.Register(f.Route(), semoptest.Operation, h.selected)
	h.router = semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: h.owner, Executors: h.lookup})
	return h
}

func (h harness) route(t *testing.T, request semanticoperations.Request) view {
	t.Helper()
	result, err := h.router.Route(context.Background(), request)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	return viewOf(result)
}

func requireCode(t *testing.T, err error, want semanticoperations.Code) {
	t.Helper()
	var routeErr *semanticoperations.Error
	if !errors.As(err, &routeErr) || routeErr.Code != want {
		t.Fatalf("error = %v; want code %s", err, want)
	}
}

func TestM56RouterReachesExactlyOneSelectedExecutor(t *testing.T) {
	h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
	staleGeneration := h.f.Route()
	staleGeneration.DriverGeneration = "5"
	otherBinding := h.f.Route()
	otherBinding.BindingID = "binding:other"
	otherOperation := semoptest.Operation
	otherOperation.ID = "fixture.operation.other"
	decoys := []*semoptest.Executor{h.f.Executor(semoptest.Outcome{}), h.f.Executor(semoptest.Outcome{}), h.f.Executor(semoptest.Outcome{})}
	h.lookup.Register(staleGeneration, semoptest.Operation, decoys[0])
	h.lookup.Register(otherBinding, semoptest.Operation, decoys[1])
	h.lookup.Register(h.f.Route(), otherOperation, decoys[2])

	request := h.f.Request()
	got := h.route(t, request)
	want := view{status: "protocol_ack", outcome: "acknowledged_unverified", delivery: "sent", ack: "accepted", readback: null, reason: null}
	if got != want {
		t.Fatalf("result = %+v; want %+v", got, want)
	}
	if h.selected.Calls() != 1 || h.selected.Sends() != 1 || h.lookup.Calls() != 1 {
		t.Fatalf("selected calls=%d sends=%d lookups=%d; want 1/1/1", h.selected.Calls(), h.selected.Sends(), h.lookup.Calls())
	}
	for index, decoy := range decoys {
		if decoy.Calls() != 0 {
			t.Fatalf("decoy %d invoked %d times", index, decoy.Calls())
		}
	}
	dispatch := h.selected.Dispatches()[0]
	if dispatch.Route != h.f.Route() || dispatch.Intent.Kind != semoptest.Operation || dispatch.Intent.AssetID != semoptest.AssetID {
		t.Fatalf("dispatch = %+v", dispatch)
	}
	guard := dispatch.Guard
	if guard.AssetID != semoptest.AssetID || guard.BindingID != semoptest.BindingID || guard.CapabilityInstance != semoptest.CapabilityInstanceID ||
		guard.ServiceInstance != semoptest.ServiceInstanceID || guard.SourceID != semoptest.SourceID || guard.SourceEpochID != semoptest.SourceEpochID ||
		guard.DriverGeneration != semoptest.DriverGeneration || guard.ExpectedCapabilityRevision != request.ExpectedCapabilityRevision ||
		guard.ExpectedCapabilityInstanceRevision != request.ExpectedCapabilityInstanceRevision {
		t.Fatalf("guard claims = %+v", guard)
	}
}

func TestM56RouterProjectsDistinctEvidence(t *testing.T) {
	sent, unknownDelivery, notSent := operation.DeliverySent, operation.DeliveryUnknown, operation.DeliveryNotSent
	// Readback that the kernel cannot bind to this admission, route and a later
	// publication never becomes readback_confirmed.
	confirming := func(tamper func(*operation.Readback, *semreg.Snapshot, semreg.Snapshot)) semoptest.Outcome {
		return semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackConfirms, TamperReadback: tamper}
	}
	invalidEvidence := view{"unknown", null, null, null, null, semanticoperations.ReasonExecutionEvidenceInvalid}
	tests := []struct {
		name    string
		outcome semoptest.Outcome
		want    view
	}{
		{"accepted handoff is admission only", semoptest.Outcome{}, view{"accepted", null, null, null, null, null}},
		{"accepted native ACK", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckAccepted}, view{"protocol_ack", "acknowledged_unverified", "sent", "accepted", null, null}},
		{"provisional ACK with inconclusive readback", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckProvisional, Readback: operation.ReadbackInconclusive}, view{"protocol_ack", "acknowledged_unverified", "sent", "provisional", "inconclusive", null}},
		{"readback without native ACK", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackConfirms}, view{"readback_confirmed", "applied", "sent", null, "confirms", null}},
		{"readback retains native ACK", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Ack: operation.AckAccepted, Readback: operation.ReadbackConfirms}, view{"readback_confirmed", "applied", "sent", "accepted", "confirms", null}},
		{"no contact", semoptest.Outcome{Kind: operation.OutcomeFailedNoContact, Delivery: notSent}, view{"failed", "failed_no_contact", "not_sent", null, null, null}},
		{"no effect after rejected ACK", semoptest.Outcome{Kind: operation.OutcomeNoEffect, Delivery: sent, Ack: operation.AckRejected}, view{"failed", "no_effect", "sent", "rejected", null, null}},
		{"no effect outranks accepted ACK", semoptest.Outcome{Kind: operation.OutcomeNoEffect, Delivery: sent, Ack: operation.AckAccepted}, view{"failed", "no_effect", "sent", "accepted", null, null}},
		{"conflict outranks accepted ACK", semoptest.Outcome{Kind: operation.OutcomeConflict, Delivery: sent, Ack: operation.AckAccepted, Readback: operation.ReadbackContradicts}, view{"failed", "conflict", "sent", "accepted", "contradicts", null}},
		{"indeterminate delivery", semoptest.Outcome{Kind: operation.OutcomeIndeterminate, Delivery: unknownDelivery}, view{"unknown", "indeterminate", "unknown", null, null, null}},
		{"rejected ACK is not protocol ACK", semoptest.Outcome{Kind: operation.OutcomeIndeterminate, Delivery: sent, Ack: operation.AckRejected}, view{"unknown", "indeterminate", "sent", "rejected", null, null}},
		{"executor error proves nothing", semoptest.Outcome{Err: errors.New("transport detail must not leak")}, view{"unknown", null, null, null, null, semanticoperations.ReasonExecutionEvidenceMissing}},
		{"executor panic proves nothing", semoptest.Outcome{Panic: true}, view{"unknown", null, null, null, null, semanticoperations.ReasonExecutionEvidenceMissing}},
		{"kernel-rejected evidence proves nothing", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckAccepted, Invalid: true}, invalidEvidence},
		{"foreign readback binding never confirms", confirming(func(r *operation.Readback, _ *semreg.Snapshot, _ semreg.Snapshot) { r.BindingID = "binding:other" }), invalidEvidence},
		{"retired readback generation never confirms", confirming(func(r *operation.Readback, _ *semreg.Snapshot, _ semreg.Snapshot) { r.DriverGeneration = "5" }), invalidEvidence},
		{"pre-dispatch snapshot never confirms", confirming(func(r *operation.Readback, later *semreg.Snapshot, earlier semreg.Snapshot) {
			*later, r.SnapshotID, r.Revisions = earlier, earlier.SnapshotID, earlier.Revisions
		}), invalidEvidence},
		{"requested value is not readback", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackContradicts,
			TamperReadback: func(r *operation.Readback, _ *semreg.Snapshot, _ semreg.Snapshot) {
				r.Relation = operation.ReadbackConfirms
			}}, invalidEvidence},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, test.outcome)
			got := h.route(t, h.f.Request())
			if got != test.want {
				t.Fatalf("result = %+v; want %+v", got, test.want)
			}
			if h.selected.Calls() != 1 {
				t.Fatalf("executor calls = %d; want 1", h.selected.Calls())
			}
		})
	}
}

// Each request identity and revision member is varied alone. The selected
// executor stays registered for the fixture route, so any dispatch is visible.
func TestM56RouterRefusalsProduceNoSend(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*semanticoperations.Request)
		reason string
	}{
		{"asset", func(r *semanticoperations.Request) { r.AssetID = "asset:other" }, "semantic.read.reason.asset_unknown"},
		{"binding", func(r *semanticoperations.Request) { r.BindingID = "binding:other" }, semanticoperations.ReasonBindingMismatch},
		{"capability instance", func(r *semanticoperations.Request) { r.CapabilityInstanceID = semoptest.ObserveInstanceID }, string(semreg.AmbiguousRoute)},
		{"operation id", func(r *semanticoperations.Request) { r.Operation.ID = "fixture.operation.other" }, string(semreg.DefinitionOwnerMissing)},
		{"operation version", func(r *semanticoperations.Request) { r.Operation.Version = "1.0.1" }, string(semreg.DefinitionOwnerMissing)},
		{"operation pack id", func(r *semanticoperations.Request) { r.Operation.Pack.ID = "fixture.pack.other" }, string(semreg.DefinitionOwnerMissing)},
		{"operation pack version", func(r *semanticoperations.Request) { r.Operation.Pack.Version = "1.0.1" }, string(semreg.DefinitionOwnerMissing)},
		{"semantic revision", func(r *semanticoperations.Request) { r.ExpectedSemanticRevision = r.ExpectedCapabilityRevision }, string(semreg.RevisionConflict)},
		{"capability collection revision", func(r *semanticoperations.Request) {
			r.ExpectedCapabilityRevision = r.ExpectedCapabilityInstanceRevision
		}, string(semreg.RevisionConflict)},
		{"capability instance revision", func(r *semanticoperations.Request) {
			r.ExpectedCapabilityInstanceRevision = r.ExpectedCapabilityRevision
		}, string(semreg.RevisionConflict)},
		{"source epoch", func(r *semanticoperations.Request) { r.ExpectedSourceEpochID = "epoch:retired" }, string(semreg.StaleSourceEpoch)},
		{"driver generation", func(r *semanticoperations.Request) { r.ExpectedDriverGeneration = "3" }, string(semreg.StaleDriverGeneration)},
		{"stale precondition candidate", func(r *semanticoperations.Request) { r.Preconditions[0].CandidateRevision = "1" }, string(semreg.PreconditionFailed)},
		{"unsatisfied precondition", func(r *semanticoperations.Request) { r.Preconditions[0].Expected = semoptest.Bool(false) }, string(semreg.PreconditionFailed)},
		{"invalid pack argument", func(r *semanticoperations.Request) { r.Arguments[0].ID = "fixture.field.other" }, string(semreg.DefinitionOwnerMissing)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
			request := h.f.Request()
			test.mutate(&request)
			if got := h.route(t, request); got != refusedView(test.reason) {
				t.Fatalf("result = %+v; want %+v", got, refusedView(test.reason))
			}
			if h.selected.Calls() != 0 || h.selected.Sends() != 0 {
				t.Fatalf("executor calls=%d sends=%d; want none", h.selected.Calls(), h.selected.Sends())
			}
		})
	}
}

func TestM56RouterFixtureRevisionsAreDistinct(t *testing.T) {
	request := semoptest.New(t).Request()
	if request.ExpectedSemanticRevision == request.ExpectedCapabilityRevision || request.ExpectedCapabilityRevision == request.ExpectedCapabilityInstanceRevision ||
		request.ExpectedSemanticRevision == request.ExpectedCapabilityInstanceRevision {
		t.Fatalf("fixture revisions must differ to catch cross-wiring: %+v", request)
	}
}

func TestM56RouterAbsentExecutorAndWithheldPublicationProduceNoSend(t *testing.T) {
	t.Run("default lookup", func(t *testing.T) {
		f := semoptest.New(t)
		if executor, ok := semanticoperations.NoExecutors().Lookup(f.Route(), semoptest.Operation); ok || executor != nil {
			t.Fatal("default lookup returned an executor")
		}
		var typedNil *semoptest.Lookup
		owner := f.Owner()
		for _, lookup := range []semanticoperations.ExecutorLookup{nil, typedNil, semanticoperations.NoExecutors()} {
			router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: owner, Executors: lookup})
			result, err := router.Route(context.Background(), f.Request())
			if err != nil || viewOf(result) != refusedView(semanticoperations.ReasonExecutorMissing) {
				t.Fatalf("result = %+v, %v", viewOf(result), err)
			}
		}
	})
	t.Run("registration for another route", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{})
		h.lookup = &semoptest.Lookup{}
		other := h.f.Route()
		other.ServiceInstance = "service:other"
		h.lookup.Register(other, semoptest.Operation, h.selected)
		h.router = semanticoperations.NewRouter(semanticoperations.Config{Snapshots: h.f, Kernel: h.f.Kernel, Intents: h.owner, Executors: h.lookup})
		if got := h.route(t, h.f.Request()); got != refusedView(semanticoperations.ReasonExecutorMissing) || h.selected.Calls() != 0 {
			t.Fatalf("result = %+v calls = %d", got, h.selected.Calls())
		}
	})
	for _, reason := range []string{"semantic.read.reason.source_unavailable", "semantic.read.reason.publication_withheld", "semantic.read.reason.snapshot_unavailable", "private.reason"} {
		t.Run(reason, func(t *testing.T) {
			h := newHarness(t, semoptest.Outcome{})
			capture := withheldCapture{reason: reason}
			router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: capture, Kernel: h.f.Kernel, Intents: h.owner, Executors: h.lookup})
			result, err := router.Route(context.Background(), h.f.Request())
			want := reason
			if reason == "private.reason" {
				want = semanticoperations.ReasonSnapshotUnavailable
			}
			if err != nil || viewOf(result) != refusedView(want) || h.selected.Calls() != 0 {
				t.Fatalf("result = %+v, %v, calls %d", viewOf(result), err, h.selected.Calls())
			}
		})
	}
}

type withheldCapture struct{ reason string }

func (c withheldCapture) Capture(_ context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	return nil, []semanticview.Input{{AssetID: ids[0], UnavailableReason: c.reason}}, nil
}

type failingCapture struct{ inputs []semanticview.Input }

func (c failingCapture) Capture(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	if c.inputs != nil {
		return nil, c.inputs, nil
	}
	return nil, nil, errors.New("hub restoring")
}

type expiringCapture struct{}

func (expiringCapture) Capture(ctx context.Context, _ []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func TestM56RouterCaptureDeadlineExpiryIsNoSendDeadlineExpired(t *testing.T) {
	h := newHarness(t, semoptest.Outcome{})
	router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: expiringCapture{}, Kernel: h.f.Kernel, Intents: h.owner, Executors: h.lookup})
	request := h.f.Request()
	request.Timeout = time.Millisecond
	result, err := router.Route(context.Background(), request)
	if err != nil || viewOf(result) != refusedView(string(semreg.DeadlineExpired)) || h.selected.Calls() != 0 {
		t.Fatalf("result = %+v, %v, calls %d", viewOf(result), err, h.selected.Calls())
	}
}

func TestM56RouterGuardChangeBeforeInvocationSendsNothing(t *testing.T) {
	h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: operation.DeliverySent, Readback: operation.ReadbackConfirms})
	h.selected.Before = h.f.Fence
	got := h.route(t, h.f.Request())
	want := view{status: "failed", outcome: "failed_no_contact", delivery: "not_sent", ack: null, readback: null, reason: null}
	if got != want || h.selected.Calls() != 1 || h.selected.Sends() != 0 {
		t.Fatalf("result = %+v calls=%d sends=%d; want %+v with no send", got, h.selected.Calls(), h.selected.Sends(), want)
	}
}

func TestM56RouterReplayNeverRedispatches(t *testing.T) {
	t.Run("recorded evidence is projected again", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
		h.owner.Replay = true
		first, second := h.route(t, h.f.Request()), h.route(t, h.f.Request())
		if first != second || first.status != "protocol_ack" || h.selected.Calls() != 1 {
			t.Fatalf("first=%+v second=%+v calls=%d", first, second, h.selected.Calls())
		}
	})
	t.Run("changed binding on replay is refused", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
		h.owner.Replay = true
		if first := h.route(t, h.f.Request()); first.status != "protocol_ack" {
			t.Fatalf("first = %+v", first)
		}
		other := h.f.Request()
		other.BindingID = "binding:other"
		if got := h.route(t, other); got != refusedView(semanticoperations.ReasonBindingMismatch) || h.selected.Calls() != 1 {
			t.Fatalf("replay = %+v calls=%d", got, h.selected.Calls())
		}
	})
	t.Run("in-flight admission is not dispatched twice", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Block: true})
		h.owner.Replay = true
		request := h.f.Request()
		request.Timeout = 30 * time.Millisecond
		first := h.route(t, request)
		second := h.route(t, request)
		h.selected.Release()
		<-h.selected.Done()
		third := h.route(t, request)
		accepted := view{"accepted", null, null, null, null, null}
		if first != (view{"unknown", null, null, null, null, semanticoperations.ReasonEvidenceTimeout}) || second != accepted || third != accepted || h.selected.Calls() != 1 {
			t.Fatalf("first=%+v second=%+v third=%+v calls=%d", first, second, third, h.selected.Calls())
		}
	})
	t.Run("undispatched earlier admission is not revived", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
		h.owner.Replay = true
		empty := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: h.f, Kernel: h.f.Kernel, Intents: h.owner})
		result, err := empty.Route(context.Background(), h.f.Request())
		if err != nil || viewOf(result) != refusedView(semanticoperations.ReasonExecutorMissing) {
			t.Fatalf("first = %+v, %v", viewOf(result), err)
		}
		if got := h.route(t, h.f.Request()); got != refusedView(semanticoperations.ReasonAdmissionReplayed) || h.selected.Calls() != 0 {
			t.Fatalf("replay = %+v calls=%d", got, h.selected.Calls())
		}
	})
	t.Run("admission rejection is recorded by the kernel", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{})
		h.owner.Replay = true
		var intents []operation.Intent
		h.owner.Mutate = func(intent *operation.Intent) { intents = append(intents, *intent) }
		request := h.f.Request()
		request.ExpectedSourceEpochID = "epoch:retired"
		first, second := h.route(t, request), h.route(t, request)
		if first != refusedView(string(semreg.StaleSourceEpoch)) || second != first || h.selected.Calls() != 0 {
			t.Fatalf("first=%+v second=%+v", first, second)
		}
		if _, err := h.f.Kernel.RecordRejection(intents[0], semreg.AmbiguousRoute, []semreg.EvidenceRef{}); semreg.ErrorIdentifier(err) != semreg.SequenceConflict {
			t.Fatalf("kernel holds no rejection record for the intent: %v", err)
		}
	})
}

func TestM56RouterBoundsTheWait(t *testing.T) {
	t.Run("expired wait is unknown without delivery claim", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Block: true})
		request := h.f.Request()
		request.Timeout = 25 * time.Millisecond
		got := h.route(t, request)
		h.selected.Release()
		<-h.selected.Done()
		if got != (view{"unknown", null, null, null, null, semanticoperations.ReasonEvidenceTimeout}) {
			t.Fatalf("result = %+v", got)
		}
	})
	t.Run("late evidence becomes the record a replay projects", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted, Block: true})
		h.owner.Replay = true
		request := h.f.Request()
		request.Timeout = 25 * time.Millisecond
		if got := h.route(t, request); got != (view{"unknown", null, null, null, null, semanticoperations.ReasonEvidenceTimeout}) {
			t.Fatalf("first = %+v", got)
		}
		h.selected.Release()
		<-h.selected.Done()
		want := view{"protocol_ack", "acknowledged_unverified", "sent", "accepted", null, null}
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
			got := h.route(t, request)
			if got == want {
				break
			}
			if got != (view{"accepted", null, null, null, null, null}) || time.Now().After(deadline) {
				t.Fatalf("replay = %+v; want %+v", got, want)
			}
		}
		if h.selected.Calls() != 1 || h.selected.Sends() != 1 {
			t.Fatalf("calls=%d sends=%d; want one late send", h.selected.Calls(), h.selected.Sends())
		}
	})
	t.Run("server budget caps the request budget", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		limit, _ := parent.Deadline()
		request := h.f.Request()
		request.Timeout = semanticoperations.MaxTimeout
		if _, err := h.router.Route(parent, request); err != nil {
			t.Fatal(err)
		}
		if budgets := h.owner.Budgets(); len(budgets) != 1 || budgets[0].After(limit) {
			t.Fatalf("budgets = %v; want one no later than %v", budgets, limit)
		}
	})
	t.Run("deadline expiry before dispatch is no send", func(t *testing.T) {
		h := newHarness(t, semoptest.Outcome{})
		slow := slowLookup{inner: h.lookup, delay: 60 * time.Millisecond}
		router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: h.f, Kernel: h.f.Kernel, Intents: h.owner, Executors: slow})
		request := h.f.Request()
		request.Timeout = 20 * time.Millisecond
		result, err := router.Route(context.Background(), request)
		if err != nil || viewOf(result) != refusedView(string(semreg.DeadlineExpired)) || h.selected.Calls() != 0 {
			t.Fatalf("result = %+v, %v, calls %d", viewOf(result), err, h.selected.Calls())
		}
	})
}

type slowLookup struct {
	inner semanticoperations.ExecutorLookup
	delay time.Duration
}

func (l slowLookup) Lookup(route operation.Route, kind semreg.DefinitionRef) (semanticoperations.Executor, bool) {
	time.Sleep(l.delay)
	return l.inner.Lookup(route, kind)
}

func TestM56RouterUnwiredDependenciesAreUnavailable(t *testing.T) {
	f := semoptest.New(t)
	executor := f.Executor(semoptest.Outcome{})
	lookup := &semoptest.Lookup{}
	lookup.Register(f.Route(), semoptest.Operation, executor)
	failingOwner := f.Owner()
	failingOwner.Err = errors.New("no owner integration")
	var typedNilOwner *semoptest.Owner
	tests := []struct {
		name   string
		router *semanticoperations.Router
	}{
		{"nil router", nil},
		{"no intent owner", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Executors: lookup})},
		{"typed nil intent owner", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: typedNilOwner, Executors: lookup})},
		{"no kernel", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Intents: f.Owner(), Executors: lookup})},
		{"no capture", semanticoperations.NewRouter(semanticoperations.Config{Kernel: f.Kernel, Intents: f.Owner(), Executors: lookup})},
		{"owner cannot construct", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: failingOwner, Executors: lookup})},
		{"capture unavailable", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: failingCapture{}, Kernel: f.Kernel, Intents: f.Owner(), Executors: lookup})},
		{"capture substitutes asset", semanticoperations.NewRouter(semanticoperations.Config{Snapshots: failingCapture{inputs: []semanticview.Input{{AssetID: "asset:other", UnavailableReason: "semantic.read.reason.asset_unknown"}}}, Kernel: f.Kernel, Intents: f.Owner(), Executors: lookup})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := f.Captures()
			_, err := test.router.Route(context.Background(), f.Request())
			requireCode(t, err, semanticoperations.CodeUnavailable)
			if executor.Calls() != 0 {
				t.Fatal("executor invoked")
			}
			if test.name != "capture unavailable" && test.name != "capture substitutes asset" && f.Captures() != before {
				t.Fatal("unwired route captured a snapshot")
			}
		})
	}
}

func TestM56RouterRejectsOwnerAlteredRequestMembers(t *testing.T) {
	tests := map[string]func(*operation.Intent){
		"contract":  func(i *operation.Intent) { i.Contract = "helianthus.semantic.operation/v0" },
		"operation": func(i *operation.Intent) { i.Kind.ID = "fixture.operation.other" },
		"asset":     func(i *operation.Intent) { i.AssetID = "asset:other" },
		"arguments": func(i *operation.Intent) { i.Arguments[0].Value = semoptest.Bool(false) },
		"capability instance": func(i *operation.Intent) {
			other := semoptest.ObserveInstanceID
			i.RequiredCapability.InstanceID = &other
		},
		"unpinned instance":       func(i *operation.Intent) { i.RequiredCapability.InstanceID = nil },
		"semantic revision":       func(i *operation.Intent) { i.ExpectedSemanticRevision = "9" },
		"capability revision":     func(i *operation.Intent) { i.ExpectedCapabilityRevision = "9" },
		"instance revision":       func(i *operation.Intent) { i.ExpectedCapabilityInstanceRevision = "9" },
		"source epoch":            func(i *operation.Intent) { i.ExpectedSourceEpochID = "epoch:other" },
		"driver generation":       func(i *operation.Intent) { i.ExpectedDriverGeneration = "9" },
		"preconditions":           func(i *operation.Intent) { i.Preconditions = []operation.Precondition{} },
		"invalid owned deadline":  func(i *operation.Intent) { i.Deadline.ClockID = "clock.other" },
		"invalid owned authority": func(i *operation.Intent) { i.Authority = semreg.EvidenceRef{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, semoptest.Outcome{})
			h.owner.Mutate = mutate
			_, err := h.router.Route(context.Background(), h.f.Request())
			requireCode(t, err, semanticoperations.CodeUnavailable)
			if h.f.Captures() != 0 || h.selected.Calls() != 0 {
				t.Fatalf("captures=%d calls=%d; want none", h.f.Captures(), h.selected.Calls())
			}
		})
	}
}

func TestM56RouterValidatesRequestBounds(t *testing.T) {
	long := func(n int) string { return "a" + strings.Repeat("b", n-1) }
	argument := func(id string) semreg.TypedField {
		return semreg.TypedField{ID: semreg.DefinitionID(id), Value: semoptest.Bool(true)}
	}
	tests := []struct {
		name   string
		mutate func(*semanticoperations.Request)
		code   semanticoperations.Code
	}{
		{"argument overflow", func(r *semanticoperations.Request) {
			r.Arguments = make([]semreg.TypedField, semanticoperations.MaxArguments+1)
		}, semanticoperations.CodeResourceExhausted},
		{"precondition overflow", func(r *semanticoperations.Request) {
			r.Preconditions = make([]operation.Precondition, semanticoperations.MaxPreconditions+1)
		}, semanticoperations.CodeResourceExhausted},
		{"overlong asset", func(r *semanticoperations.Request) { r.AssetID = semreg.AssetID(long(257)) }, semanticoperations.CodeResourceExhausted},
		{"overlong nested candidate", func(r *semanticoperations.Request) {
			r.Preconditions[0].CandidateID = semreg.CandidateID(long(257))
		}, semanticoperations.CodeResourceExhausted},
		{"overlong nested field", func(r *semanticoperations.Request) { r.Arguments[0].ID = semreg.DefinitionID(long(257)) }, semanticoperations.CodeResourceExhausted},
		{"zero revision", func(r *semanticoperations.Request) { r.ExpectedSemanticRevision = "0" }, semanticoperations.CodeInvalidArgument},
		{"noncanonical revision", func(r *semanticoperations.Request) { r.ExpectedCapabilityRevision = "01" }, semanticoperations.CodeInvalidArgument},
		{"revision above 64 bits", func(r *semanticoperations.Request) { r.ExpectedDriverGeneration = "18446744073709551616" }, semanticoperations.CodeInvalidArgument},
		{"signed revision", func(r *semanticoperations.Request) { r.ExpectedCapabilityInstanceRevision = "-1" }, semanticoperations.CodeInvalidArgument},
		{"zero timeout", func(r *semanticoperations.Request) { r.Timeout = 0 }, semanticoperations.CodeInvalidArgument},
		{"timeout above bound", func(r *semanticoperations.Request) { r.Timeout = semanticoperations.MaxTimeout + time.Millisecond }, semanticoperations.CodeInvalidArgument},
		{"missing arguments", func(r *semanticoperations.Request) { r.Arguments = nil }, semanticoperations.CodeInvalidArgument},
		{"missing preconditions", func(r *semanticoperations.Request) { r.Preconditions = nil }, semanticoperations.CodeInvalidArgument},
		{"unordered arguments", func(r *semanticoperations.Request) {
			r.Arguments = []semreg.TypedField{argument("fixture.field.level"), argument("fixture.field.alpha")}
		}, semanticoperations.CodeInvalidArgument},
		{"duplicate arguments", func(r *semanticoperations.Request) {
			r.Arguments = []semreg.TypedField{argument("fixture.field.level"), argument("fixture.field.level")}
		}, semanticoperations.CodeInvalidArgument},
		{"duplicate preconditions", func(r *semanticoperations.Request) {
			r.Preconditions = append(r.Preconditions, r.Preconditions[0])
		}, semanticoperations.CodeInvalidArgument},
		{"unordered preconditions", func(r *semanticoperations.Request) {
			later := r.Preconditions[0]
			later.CandidateRevision = "10"
			r.Preconditions = []operation.Precondition{later, r.Preconditions[0]}
		}, semanticoperations.CodeInvalidArgument},
		{"mismatched value arm", func(r *semanticoperations.Request) {
			value := "true"
			r.Arguments[0].Value = semreg.Value{Kind: semreg.ValueBoolean, Text: &value}
		}, semanticoperations.CodeInvalidArgument},
		{"malformed asset", func(r *semanticoperations.Request) { r.AssetID = " asset:synthetic" }, semanticoperations.CodeInvalidArgument},
		{"malformed operation", func(r *semanticoperations.Request) { r.Operation.Version = "one" }, semanticoperations.CodeInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, semoptest.Outcome{})
			request := h.f.Request()
			test.mutate(&request)
			_, err := h.router.Route(context.Background(), request)
			requireCode(t, err, test.code)
			if h.owner.Calls() != 0 || h.f.Captures() != 0 || h.selected.Calls() != 0 {
				t.Fatal("invalid request reached the owner, capture or executor")
			}
		})
	}
	t.Run("inclusive bounds", func(t *testing.T) {
		request := semoptest.New(t).Request()
		request.AssetID = semreg.AssetID(long(256))
		request.ExpectedDriverGeneration = "18446744073709551615"
		for _, timeout := range []time.Duration{semanticoperations.MinTimeout, semanticoperations.MaxTimeout} {
			request.Timeout = timeout
			if err := request.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		}
	})
}

func TestM56BuiltinKernelComposes(t *testing.T) {
	if kernel, err := semanticoperations.NewBuiltinKernel(); err != nil || kernel == nil {
		t.Fatalf("NewBuiltinKernel() = %v, %v", kernel, err)
	}
}
