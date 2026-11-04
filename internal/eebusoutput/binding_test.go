package eebusoutput_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusoutput"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const (
	m40Std01   semreg.DefinitionID = "unknown_pending_std_01"
	m40OtherID semreg.AssetID      = "asset:synthetic-other"
)

func m40JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func m40Registry(t *testing.T) *semreg.PackMetadataRegistry {
	t.Helper()
	registry, err := packs.NewMetadataRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

// m40Seal recomputes the snapshot identity after a synthetic edit.
func m40Seal(t *testing.T, snapshot *semreg.Snapshot) {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(m40JSON(t, snapshot), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	digest := sha256.Sum256(m40JSON(t, document))
	snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", digest))
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
}

// m40Input captures the synthetic asset, optionally edits its interlock
// candidate, and re-evaluates it on the fixture clocks.
func m40Input(t *testing.T, f *semoptest.Fixture, edit func(*semreg.FactCandidate)) semanticview.Input {
	t.Helper()
	snapshot := f.Snapshot()
	var copied semreg.Snapshot
	if err := json.Unmarshal(m40JSON(t, snapshot), &copied); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		for i := range copied.Facts {
			if copied.Facts[i].Key.FactID == semoptest.InterlockFact.FactID {
				edit(&copied.Facts[i].Candidates[0])
			}
		}
		m40Seal(t, &copied)
	}
	evaluation, err := semreg.EvaluateSnapshot(copied, f.Context())
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Input{AssetID: copied.AssetID, Snapshot: &copied, Evaluation: &evaluation}
}

type m40Capture struct {
	calls atomic.Int32
	mu    sync.Mutex
	next  func(ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
}

func (c *m40Capture) set(next func([]semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next = next
}

func (c *m40Capture) Capture(_ context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.next(ids)
}

func m40Fixed(registry *semreg.PackMetadataRegistry, inputs ...semanticview.Input) func([]semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	return func([]semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		return registry, inputs, nil
	}
}

func m40Binding(t *testing.T, config eebusoutput.Config) *eebusoutput.Binding {
	t.Helper()
	binding, err := eebusoutput.NewBinding(config)
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func m40Source(t *testing.T, registry *semreg.PackMetadataRegistry, input semanticview.Input) projection.ProjectionReport {
	t.Helper()
	view, err := semanticview.Build(registry, []semanticview.Input{input})
	if err != nil {
		t.Fatal(err)
	}
	return *view.Assets[0].Projection
}

func m40Disposition(t *testing.T, report projection.ProjectionReport, kind projection.ItemKind, id semreg.DefinitionID) projection.ProjectionDisposition {
	t.Helper()
	for _, d := range report.Dispositions {
		if d.Kind == kind && d.ItemID == id {
			return d
		}
	}
	t.Fatalf("missing disposition %s %s", kind, id)
	return projection.ProjectionDisposition{}
}

// m40CheckLedger asserts complete accounting against the shared view, a
// placeholder manifest bound to the same snapshot, no exact or transformed
// outcome, verbatim non-exact source dispositions and an empty document.
func m40CheckLedger(t *testing.T, input semanticview.Input, source projection.ProjectionReport, result eebusoutput.Result, mapped map[semreg.DefinitionID]semreg.DefinitionID) {
	t.Helper()
	report := result.Report
	if _, err := projection.ValidateReport(*input.Snapshot, report); err != nil {
		t.Fatalf("report does not validate against its snapshot: %v", err)
	}
	if report.SnapshotID != input.Snapshot.SnapshotID || report.Revisions != input.Snapshot.Revisions {
		t.Fatal("report mixed snapshot identity")
	}
	want := eebusoutput.Manifest(source.Manifest.PackVersions)
	if !bytes.Equal(m40JSON(t, report.Manifest), m40JSON(t, want)) || report.Manifest.TargetID != "target:eebus-output-placeholder" || report.Manifest.TargetVersion != "0" {
		t.Fatalf("manifest %s", m40JSON(t, report.Manifest))
	}
	if !bytes.Equal(m40JSON(t, report.Requested), m40JSON(t, source.Requested)) || len(report.Dispositions) != len(source.Requested) {
		t.Fatal("requested item lost, added or reordered")
	}
	for i, d := range report.Dispositions {
		from := source.Dispositions[i]
		if d.Kind != from.Kind || d.ItemID != from.ItemID {
			t.Fatal("disposition order changed")
		}
		switch d.Outcome {
		case projection.ProjectionWithheld, projection.ProjectionUnsupported, projection.ProjectionUnknown:
		default:
			t.Fatalf("%s reached %s before STD-01", d.ItemID, d.Outcome)
		}
		if d.Reason == nil {
			t.Fatalf("%s has no reason", d.ItemID)
		}
		if !bytes.Equal(m40JSON(t, d.SourceKeys), m40JSON(t, from.SourceKeys)) || !bytes.Equal(m40JSON(t, d.Loss), m40JSON(t, from.Loss)) {
			t.Fatalf("%s source keys or loss changed", d.ItemID)
		}
		if reason, remapped := mapped[d.ItemID]; remapped {
			if *d.Reason != reason {
				t.Fatalf("%s reason %s, want %s", d.ItemID, *d.Reason, reason)
			}
			continue
		}
		if !bytes.Equal(m40JSON(t, d), m40JSON(t, from)) {
			t.Fatalf("source disposition not verbatim: %s vs %s", m40JSON(t, d), m40JSON(t, from))
		}
	}
	if got := string(m40JSON(t, result.Document)); got != "{}" {
		t.Fatalf("document not empty: %s", got)
	}
}

func TestM40LedgerAccountsForEveryItemWithoutExactMapping(t *testing.T) {
	registry := m40Registry(t)
	for _, tt := range []struct {
		name       string
		edit       func(*semreg.FactCandidate)
		sourceWant projection.ProjectionOutcome
		outcome    projection.ProjectionOutcome
		reason     semreg.DefinitionID
	}{
		{"qualified promoted fact has no proven target", nil, projection.ProjectionExact, projection.ProjectionUnknown, eebusoutput.ReasonMappingUnknown},
		{"candidate fact stays withheld", func(c *semreg.FactCandidate) {
			c.Quality.Qualification = semreg.QualificationCandidate
			c.Quality.Promotion = semreg.PromotionUnpromoted
		}, projection.ProjectionWithheld, projection.ProjectionWithheld, "semantic.read.reason.capability_unqualified"},
		{"candidate native reason is verbatim", func(c *semreg.FactCandidate) {
			c.Quality.Qualification = semreg.QualificationCandidate
			c.Quality.Promotion = semreg.PromotionUnpromoted
			c.Quality.Reasons = []semreg.DefinitionID{"fixture.reason.native"}
		}, projection.ProjectionWithheld, projection.ProjectionWithheld, "fixture.reason.native"},
		{"unsupported fact stays unsupported", func(c *semreg.FactCandidate) {
			c.Value = nil
			c.Quality.Qualification = semreg.QualificationUnsupported
			c.Quality.Promotion = semreg.PromotionUnpromoted
		}, projection.ProjectionUnsupported, projection.ProjectionUnsupported, "semantic.read.reason.value_unknown"},
		{"STD-01 reason keeps precedence", func(c *semreg.FactCandidate) {
			c.Quality.Qualification = semreg.QualificationQualified
			c.Quality.Reasons = []semreg.DefinitionID{"a.unrelated", m40Std01}
		}, projection.ProjectionUnknown, projection.ProjectionUnknown, m40Std01},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := semoptest.New(t)
			input := m40Input(t, f, tt.edit)
			source := m40Source(t, registry, input)
			fact := m40Disposition(t, source, projection.ItemFact, semoptest.InterlockFact.FactID)
			if fact.Outcome != tt.sourceWant {
				t.Fatalf("synthetic source outcome %s, want %s", fact.Outcome, tt.sourceWant)
			}
			capture := &m40Capture{}
			capture.set(m40Fixed(registry, input))
			binding := m40Binding(t, eebusoutput.Config{Snapshots: capture})
			out, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID})
			if err != nil {
				t.Fatal(err)
			}
			if capture.calls.Load() != 1 || len(out.Assets) != 1 || out.Assets[0].Result == nil || out.Assets[0].WithheldReason != "" {
				t.Fatalf("calls=%d projection=%s", capture.calls.Load(), m40JSON(t, out))
			}
			result := *out.Assets[0].Result
			mapped := map[semreg.DefinitionID]semreg.DefinitionID{}
			if tt.sourceWant == projection.ProjectionExact {
				mapped[semoptest.InterlockFact.FactID] = tt.reason
			}
			m40CheckLedger(t, input, source, result, mapped)
			got := m40Disposition(t, result.Report, projection.ItemFact, semoptest.InterlockFact.FactID)
			if got.Outcome != tt.outcome || *got.Reason != tt.reason {
				t.Fatalf("fact %s/%s, want %s/%s", got.Outcome, *got.Reason, tt.outcome, tt.reason)
			}
			if len(got.SourceKeys) != 1 || !bytes.Equal(m40JSON(t, got.SourceKeys[0]), m40JSON(t, semoptest.InterlockFact)) {
				t.Fatal("unknown or withheld fact lost its source key")
			}
			if bytes.Contains(m40JSON(t, result), []byte(`"value"`)) {
				t.Fatal("ledger carries a synthetic value")
			}
		})
	}
}

// Mapping rows are keyed by requested item kind and item id. Each case varies
// one key field away from the source item and must leave the fact unknown.
func TestM40MappingRowKeyCoversKindAndItemID(t *testing.T) {
	registry := m40Registry(t)
	fact := projection.RequestedItem{Kind: projection.ItemFact, ItemID: semoptest.InterlockFact.FactID}
	for _, tt := range []struct {
		name    string
		rows    []eebusoutput.MappingRow
		outcome projection.ProjectionOutcome
		reason  semreg.DefinitionID
	}{
		{"no row", nil, projection.ProjectionUnknown, eebusoutput.ReasonMappingUnknown},
		{"unknown row", []eebusoutput.MappingRow{{Item: fact, Evidence: eebusoutput.EvidenceUnknown}}, projection.ProjectionUnknown, eebusoutput.ReasonMappingUnknown},
		{"hypothesis row on the exact key", []eebusoutput.MappingRow{{Item: fact, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"}}, projection.ProjectionWithheld, eebusoutput.ReasonMappingCandidate},
		// Catches a lookup keyed by item id only.
		{"hypothesis row with another kind", []eebusoutput.MappingRow{{Item: projection.RequestedItem{Kind: projection.ItemCapability, ItemID: fact.ItemID}, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"}}, projection.ProjectionUnknown, eebusoutput.ReasonMappingUnknown},
		// Catches a lookup keyed by kind only.
		{"hypothesis row with another item id", []eebusoutput.MappingRow{{Item: projection.RequestedItem{Kind: projection.ItemFact, ItemID: semoptest.LevelFact.FactID}, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"}}, projection.ProjectionUnknown, eebusoutput.ReasonMappingUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := semoptest.New(t)
			input := m40Input(t, f, nil)
			capture := &m40Capture{}
			capture.set(m40Fixed(registry, input))
			binding := m40Binding(t, eebusoutput.Config{Snapshots: capture, Mappings: tt.rows})
			out, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID})
			if err != nil {
				t.Fatal(err)
			}
			result := *out.Assets[0].Result
			m40CheckLedger(t, input, m40Source(t, registry, input), result, map[semreg.DefinitionID]semreg.DefinitionID{fact.ItemID: tt.reason})
			got := m40Disposition(t, result.Report, projection.ItemFact, fact.ItemID)
			if got.Outcome != tt.outcome {
				t.Fatalf("outcome %s, want %s", got.Outcome, tt.outcome)
			}
		})
	}

	// A hypothesis row never overrides a non-exact source disposition.
	t.Run("hypothesis row keeps source reason", func(t *testing.T) {
		f := semoptest.New(t)
		input := m40Input(t, f, func(c *semreg.FactCandidate) { c.Quality.Reasons = []semreg.DefinitionID{m40Std01} })
		capture := &m40Capture{}
		capture.set(m40Fixed(registry, input))
		binding := m40Binding(t, eebusoutput.Config{Snapshots: capture, Mappings: []eebusoutput.MappingRow{
			{Item: fact, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"},
			{Item: projection.RequestedItem{Kind: projection.ItemCapability, ItemID: semoptest.CapabilityLevel.ID}, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"},
		}})
		out, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID})
		if err != nil {
			t.Fatal(err)
		}
		m40CheckLedger(t, input, m40Source(t, registry, input), *out.Assets[0].Result, nil)
	})
}

func TestM40ProvenAndMalformedMappingRowsFailClosed(t *testing.T) {
	fact := projection.RequestedItem{Kind: projection.ItemFact, ItemID: semoptest.InterlockFact.FactID}
	for _, tt := range []struct {
		name string
		rows []eebusoutput.MappingRow
		want error
	}{
		{"proven row", []eebusoutput.MappingRow{{Item: fact, Evidence: eebusoutput.EvidenceProven, Source: "public contract"}}, eebusoutput.ErrMappingFrozen},
		{"hypothesis without public source", []eebusoutput.MappingRow{{Item: fact, Evidence: eebusoutput.EvidenceHypothesis}}, eebusoutput.ErrMappingInvalid},
		{"qualification word is not evidence", []eebusoutput.MappingRow{{Item: fact, Evidence: "qualified"}}, eebusoutput.ErrMappingInvalid},
		{"empty evidence", []eebusoutput.MappingRow{{Item: fact}}, eebusoutput.ErrMappingInvalid},
		{"invalid kind", []eebusoutput.MappingRow{{Item: projection.RequestedItem{Kind: "entity", ItemID: fact.ItemID}, Evidence: eebusoutput.EvidenceUnknown}}, eebusoutput.ErrMappingInvalid},
		{"invalid item id", []eebusoutput.MappingRow{{Item: projection.RequestedItem{Kind: fact.Kind, ItemID: "Not An ID"}, Evidence: eebusoutput.EvidenceUnknown}}, eebusoutput.ErrMappingInvalid},
		{"duplicate key", []eebusoutput.MappingRow{{Item: fact, Evidence: eebusoutput.EvidenceUnknown}, {Item: fact, Evidence: eebusoutput.EvidenceHypothesis, Source: "public candidate"}}, eebusoutput.ErrMappingInvalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			binding, err := eebusoutput.NewBinding(eebusoutput.Config{Mappings: tt.rows})
			if !errors.Is(err, tt.want) || binding != nil {
				t.Fatalf("err=%v binding=%v, want %v", err, binding, tt.want)
			}
		})
	}
}

func TestM40CaptureWithheldAssetsAndLastAcceptedState(t *testing.T) {
	registry := m40Registry(t)
	f := semoptest.New(t)
	input := m40Input(t, f, nil)
	capture := &m40Capture{}
	sink := &eebusoutput.MemorySink{}
	binding := m40Binding(t, eebusoutput.Config{Snapshots: capture, Sink: sink})
	both := []semreg.AssetID{semoptest.AssetID, m40OtherID}

	capture.set(m40Fixed(registry, input, semanticview.Input{AssetID: m40OtherID, UnavailableReason: "semantic.read.reason.publication_withheld"}))
	out, err := binding.Project(context.Background(), both)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Assets) != 2 || out.Assets[0].AssetID != semoptest.AssetID || out.Assets[0].Result == nil {
		t.Fatalf("projection %s", m40JSON(t, out))
	}
	if out.Assets[1].AssetID != m40OtherID || out.Assets[1].Result != nil || out.Assets[1].WithheldReason != "semantic.read.reason.publication_withheld" {
		t.Fatalf("capture-withheld asset was projected: %s", m40JSON(t, out.Assets[1]))
	}
	first, ok := sink.Accepted(semoptest.AssetID)
	if !ok || !bytes.Equal(m40JSON(t, first), m40JSON(t, *out.Assets[0].Result)) {
		t.Fatal("accepted result not kept")
	}
	if _, ok := sink.Accepted(m40OtherID); ok {
		t.Fatal("withheld asset reached the sink")
	}

	for _, reason := range []string{"semantic.read.reason.asset_unknown", "semantic.read.reason.snapshot_unavailable", "semantic.read.reason.source_unavailable", "semantic.read.reason.publication_withheld"} {
		t.Run(reason, func(t *testing.T) {
			capture.set(m40Fixed(registry, semanticview.Input{AssetID: semoptest.AssetID, UnavailableReason: reason}))
			out, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID})
			if err != nil {
				t.Fatal(err)
			}
			if out.Assets[0].Result != nil || out.Assets[0].WithheldReason != reason {
				t.Fatalf("reason not verbatim: %s", m40JSON(t, out))
			}
			kept, ok := sink.Accepted(semoptest.AssetID)
			if !ok || !bytes.Equal(m40JSON(t, kept), m40JSON(t, first)) {
				t.Fatal("withheld capture replaced last accepted result")
			}
		})
	}

	for _, tt := range []struct {
		name string
		next func([]semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
		want error
	}{
		{"capture error", func([]semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
			return nil, nil, errors.New("synthetic capture failure")
		}, eebusoutput.ErrCaptureUnavailable},
		{"missing asset", m40Fixed(registry, input), eebusoutput.ErrCaptureSelection},
		{"other asset", m40Fixed(registry, input, semanticview.Input{AssetID: "asset:synthetic-third", UnavailableReason: "semantic.read.reason.asset_unknown"}), eebusoutput.ErrCaptureSelection},
		{"duplicate asset", m40Fixed(registry, input, input), eebusoutput.ErrCaptureSelection},
		{"no metadata", m40Fixed(nil, input, semanticview.Input{AssetID: m40OtherID, UnavailableReason: "semantic.read.reason.asset_unknown"}), nil},
		{"unlisted reason", m40Fixed(registry, input, semanticview.Input{AssetID: m40OtherID, UnavailableReason: "semantic.read.reason.other"}), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			capture.set(tt.next)
			before := capture.calls.Load()
			out, err := binding.Project(context.Background(), both)
			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) || out.Assets != nil {
				t.Fatalf("err=%v out=%s, want %v", err, m40JSON(t, out), tt.want)
			}
			if capture.calls.Load() != before+1 {
				t.Fatal("projection did not use exactly one capture")
			}
			kept, ok := sink.Accepted(semoptest.AssetID)
			if !ok || !bytes.Equal(m40JSON(t, kept), m40JSON(t, first)) {
				t.Fatal("failed projection replaced last accepted result")
			}
			if _, ok := sink.Accepted(m40OtherID); ok {
				t.Fatal("failed projection reached the sink")
			}
		})
	}

	t.Run("unwired capture", func(t *testing.T) {
		var typedNil *m40Capture
		for _, config := range []eebusoutput.Config{{}, {Snapshots: typedNil}} {
			binding := m40Binding(t, config)
			if _, err := binding.Project(context.Background(), both); !errors.Is(err, eebusoutput.ErrUnwired) {
				t.Fatalf("err=%v", err)
			}
		}
		var nilBinding *eebusoutput.Binding
		if _, err := nilBinding.Project(context.Background(), both); !errors.Is(err, eebusoutput.ErrUnwired) {
			t.Fatalf("err=%v", err)
		}
	})
}

type m40Operations struct {
	calls    atomic.Int32
	requests []semanticoperations.Request
	mu       sync.Mutex
	result   semanticoperations.Result
	err      error
}

func (o *m40Operations) Route(_ context.Context, request semanticoperations.Request) (semanticoperations.Result, error) {
	o.calls.Add(1)
	o.mu.Lock()
	o.requests = append(o.requests, request)
	o.mu.Unlock()
	return o.result, o.err
}

func TestM40RelayReturnsRouteResultUnchanged(t *testing.T) {
	outcome, delivery, ack, relation, reason := operation.OutcomeAcknowledgedUnverified, operation.DeliverySent, operation.AckProvisional, operation.ReadbackInconclusive, "fixture.reason.relay"
	canned := semanticoperations.Result{Status: semanticoperations.StatusProtocolAck, KernelOutcome: &outcome, Delivery: &delivery, Acknowledgement: &ack, ReadbackRelation: &relation, Reason: &reason}
	routeErr := &semanticoperations.Error{Code: semanticoperations.CodeInvalidArgument, Detail: "synthetic"}
	f := semoptest.New(t)
	request := f.Request()

	ops := &m40Operations{result: canned}
	binding := m40Binding(t, eebusoutput.Config{Operations: ops})
	got, err := binding.Relay(context.Background(), request)
	if err != nil || got.KernelOutcome != canned.KernelOutcome || got.Delivery != canned.Delivery || got.Acknowledgement != canned.Acknowledgement ||
		got.ReadbackRelation != canned.ReadbackRelation || got.Reason != canned.Reason || got.Status != canned.Status {
		t.Fatalf("relay rewrote the route result: %s err=%v", m40JSON(t, got), err)
	}
	if ops.calls.Load() != 1 || !bytes.Equal(m40JSON(t, ops.requests[0]), m40JSON(t, request)) {
		t.Fatal("relay altered, retried or dropped the request")
	}

	ops = &m40Operations{result: canned, err: routeErr}
	binding = m40Binding(t, eebusoutput.Config{Operations: ops})
	if _, err := binding.Relay(context.Background(), request); err != routeErr || ops.calls.Load() != 1 {
		t.Fatalf("relay rewrote the route error: %v calls=%d", err, ops.calls.Load())
	}

	var typedNil *m40Operations
	var nilBinding *eebusoutput.Binding
	for _, relay := range []*eebusoutput.Binding{m40Binding(t, eebusoutput.Config{}), m40Binding(t, eebusoutput.Config{Operations: typedNil}), nilBinding} {
		got, err := relay.Relay(context.Background(), request)
		var refusal *semanticoperations.Error
		if !errors.As(err, &refusal) || refusal.Code != semanticoperations.CodeUnavailable || got.Status != "" {
			t.Fatalf("unwired relay answered %s err=%v", m40JSON(t, got), err)
		}
	}
}

// The relay over the real router and a fake executor keeps admission-only,
// protocol ACK and readback confirmation distinct and sends at most once.
func TestM40RelayKeepsAcceptedAckAndReadbackDistinct(t *testing.T) {
	sent := operation.DeliverySent
	type view struct {
		Status, Outcome, Delivery, Ack, Readback *string
	}
	str := func(value string) *string { return &value }
	for _, tt := range []struct {
		name    string
		outcome semoptest.Outcome
		want    view
	}{
		{"accepted is admission only", semoptest.Outcome{}, view{str("accepted"), nil, nil, nil, nil}},
		{"protocol ack is not readback", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckAccepted},
			view{str("protocol_ack"), str("acknowledged_unverified"), str("sent"), str("accepted"), nil}},
		{"inconclusive readback stays protocol ack", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckProvisional, Readback: operation.ReadbackInconclusive},
			view{str("protocol_ack"), str("acknowledged_unverified"), str("sent"), str("provisional"), str("inconclusive")}},
		{"readback confirmed without ack", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackConfirms},
			view{str("readback_confirmed"), str("applied"), str("sent"), nil, str("confirms")}},
		{"readback confirmed keeps ack", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Ack: operation.AckAccepted, Readback: operation.ReadbackConfirms},
			view{str("readback_confirmed"), str("applied"), str("sent"), str("accepted"), str("confirms")}},
		{"no contact is a refusal", semoptest.Outcome{Kind: operation.OutcomeFailedNoContact, Delivery: operation.DeliveryNotSent},
			view{str("failed"), str("failed_no_contact"), str("not_sent"), nil, nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := semoptest.New(t)
			executor := f.Executor(tt.outcome)
			lookup := &semoptest.Lookup{}
			lookup.Register(f.Route(), semoptest.Operation, executor)
			router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: f.Owner(), Executors: lookup})
			binding := m40Binding(t, eebusoutput.Config{Operations: router})
			got, err := binding.Relay(context.Background(), f.Request())
			if err != nil {
				t.Fatal(err)
			}
			pick := func(value any) *string {
				switch v := value.(type) {
				case *operation.Outcome:
					if v != nil {
						return str(string(*v))
					}
				case *operation.DeliveryState:
					if v != nil {
						return str(string(*v))
					}
				case *operation.AckState:
					if v != nil {
						return str(string(*v))
					}
				case *operation.ReadbackRelation:
					if v != nil {
						return str(string(*v))
					}
				}
				return nil
			}
			have := view{str(string(got.Status)), pick(got.KernelOutcome), pick(got.Delivery), pick(got.Acknowledgement), pick(got.ReadbackRelation)}
			if !bytes.Equal(m40JSON(t, have), m40JSON(t, tt.want)) {
				t.Fatalf("relay result %s, want %s", m40JSON(t, have), m40JSON(t, tt.want))
			}
			if executor.Calls() != 1 || f.Captures() != 1 {
				t.Fatalf("executor calls=%d captures=%d, want one each", executor.Calls(), f.Captures())
			}
		})
	}

	t.Run("no executor is a no-send refusal", func(t *testing.T) {
		f := semoptest.New(t)
		router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: f.Owner()})
		binding := m40Binding(t, eebusoutput.Config{Operations: router})
		got, err := binding.Relay(context.Background(), f.Request())
		if err != nil || got.Status != semanticoperations.StatusFailed || got.Delivery == nil || *got.Delivery != operation.DeliveryNotSent ||
			got.Reason == nil || *got.Reason != semanticoperations.ReasonExecutorMissing {
			t.Fatalf("relay converted a refusal: %s err=%v", m40JSON(t, got), err)
		}
	})
}

func TestM40ConcurrentProjectAndRelay(t *testing.T) {
	registry := m40Registry(t)
	f := semoptest.New(t)
	input := m40Input(t, f, nil)
	capture := &m40Capture{}
	capture.set(m40Fixed(registry, input))
	sink := &eebusoutput.MemorySink{}
	ops := &m40Operations{result: semanticoperations.Result{Status: semanticoperations.StatusAccepted}}
	binding := m40Binding(t, eebusoutput.Config{Snapshots: capture, Operations: ops, Sink: sink})
	request := f.Request()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID}); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if got, err := binding.Relay(context.Background(), request); err != nil || got.Status != semanticoperations.StatusAccepted {
				t.Error("relay changed result")
			}
		}()
	}
	wg.Wait()
	if capture.calls.Load() != 8 || ops.calls.Load() != 8 {
		t.Fatalf("captures=%d relays=%d", capture.calls.Load(), ops.calls.Load())
	}
	if _, ok := sink.Accepted(semoptest.AssetID); !ok {
		t.Fatal("no accepted result")
	}
}

func TestM40MemorySinkIsolatesRetainedState(t *testing.T) {
	registry := m40Registry(t)
	f := semoptest.New(t)
	input := m40Input(t, f, nil)
	capture := &m40Capture{}
	capture.set(m40Fixed(registry, input))
	sink := &eebusoutput.MemorySink{}
	binding := m40Binding(t, eebusoutput.Config{Snapshots: capture, Sink: sink})
	out, err := binding.Project(context.Background(), []semreg.AssetID{semoptest.AssetID})
	if err != nil {
		t.Fatal(err)
	}
	want := m40JSON(t, *out.Assets[0].Result)

	scribble := func(r *eebusoutput.Result) {
		for i := range r.Report.Dispositions {
			d := &r.Report.Dispositions[i]
			d.Outcome = projection.ProjectionUnsupported
			if d.Reason != nil {
				*d.Reason = "mutated"
			}
			for j := range d.SourceKeys {
				d.SourceKeys[j].PackID = "mutated"
			}
		}
		for i := range r.Report.Requested {
			r.Report.Requested[i].ItemID = "mutated"
		}
	}
	scribble(out.Assets[0].Result)
	got, ok := sink.Accepted(semoptest.AssetID)
	if !ok || !bytes.Equal(m40JSON(t, got), want) {
		t.Fatal("mutating the Project result changed retained state")
	}
	scribble(&got)
	again, _ := sink.Accepted(semoptest.AssetID)
	if !bytes.Equal(m40JSON(t, again), want) {
		t.Fatal("mutating the Accepted result changed retained state")
	}

	own := again
	sink.Accept(semoptest.AssetID, own)
	scribble(&own)
	final, _ := sink.Accepted(semoptest.AssetID)
	if !bytes.Equal(m40JSON(t, final), want) {
		t.Fatal("mutating an ingress result changed retained state")
	}
}
