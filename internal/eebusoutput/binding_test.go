package eebusoutput_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/eebusoutput"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
)

func m40JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func m40Binding(t *testing.T, config eebusoutput.Config) *eebusoutput.Binding {
	t.Helper()
	binding, err := eebusoutput.NewBinding(config)
	if err != nil {
		t.Fatal(err)
	}
	return binding
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

func TestM40ConcurrentRelay(t *testing.T) {
	f := semoptest.New(t)
	ops := &m40Operations{result: semanticoperations.Result{Status: semanticoperations.StatusAccepted}}
	binding := m40Binding(t, eebusoutput.Config{Operations: ops})
	request := f.Request()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := binding.Relay(context.Background(), request); err != nil || got.Status != semanticoperations.StatusAccepted {
				t.Error("relay changed result")
			}
		}()
	}
	wg.Wait()
	if ops.calls.Load() != 8 {
		t.Fatalf("relays=%d", ops.calls.Load())
	}
}

// m40RefusalNames are the exported identifiers through which the package
// refused a mapping, or published a placeholder target, because a native
// revision was unresolved.
var m40RefusalNames = []string{
	"ErrMappingFrozen", "ReasonMappingUnknown", "ReasonMappingCandidate", "EvidenceProven",
	"Document", "TargetID", "TargetVersion", "MappingRevision", "Manifest",
}

// m40RefusalWords mark an exported string that refuses a mapping pending an
// unresolved revision.
var m40RefusalWords = []string{"STD-01", "pending", "frozen", "blocked", "mapping_unknown", "placeholder"}

// m40Exported parses the package sources and returns every exported top-level
// identifier with the string literals of its declaration.
func m40Exported(t *testing.T) map[string][]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	exported := map[string][]string{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		literals := func(node ast.Node) []string {
			var out []string
			ast.Inspect(node, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					out = append(out, lit.Value)
				}
				return true
			})
			return out
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					exported[d.Name.Name] = nil
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							exported[s.Name.Name] = nil
						}
					case *ast.ValueSpec:
						for i, id := range s.Names {
							if !id.IsExported() {
								continue
							}
							var values []string
							if i < len(s.Values) {
								values = literals(s.Values[i])
							}
							exported[id.Name] = values
						}
					}
				}
			}
		}
	}
	return exported
}

// No exported error or reason refuses a mapping for an unresolved revision,
// and the relay still returns the route's result and error unchanged.
func TestM40NoMappingRefusalForUnresolvedRevision(t *testing.T) {
	exported := m40Exported(t)
	if len(exported) == 0 {
		t.Fatal("no exported identifier parsed")
	}
	for _, name := range m40RefusalNames {
		if _, ok := exported[name]; ok {
			t.Errorf("exported %s still refuses a mapping or names a placeholder target", name)
		}
	}
	for name, values := range exported {
		for _, value := range values {
			for _, word := range m40RefusalWords {
				if strings.Contains(strings.ToLower(value), strings.ToLower(word)) {
					t.Errorf("exported %s = %s refuses a mapping (%q)", name, value, word)
				}
			}
		}
	}

	// Every result field and the error differ between the two routes, so a
	// relay that rewrites, caches or drops any of them fails.
	f := semoptest.New(t)
	request := f.Request()
	outcomeA, deliveryA, ackA, readbackA, reasonA := operation.OutcomeAcknowledgedUnverified, operation.DeliverySent, operation.AckProvisional, operation.ReadbackInconclusive, "fixture.reason.relay-a"
	outcomeB, deliveryB, ackB, readbackB, reasonB := operation.OutcomeFailedNoContact, operation.DeliveryNotSent, operation.AckAccepted, operation.ReadbackConfirms, "fixture.reason.relay-b"
	for _, tt := range []struct {
		name   string
		result semanticoperations.Result
		err    error
	}{
		{"route a", semanticoperations.Result{Status: semanticoperations.StatusProtocolAck, KernelOutcome: &outcomeA, Delivery: &deliveryA, Acknowledgement: &ackA, ReadbackRelation: &readbackA, Reason: &reasonA}, nil},
		{"route b", semanticoperations.Result{Status: semanticoperations.StatusFailed, KernelOutcome: &outcomeB, Delivery: &deliveryB, Acknowledgement: &ackB, ReadbackRelation: &readbackB, Reason: &reasonB}, &semanticoperations.Error{Code: semanticoperations.CodeInvalidArgument, Detail: "synthetic"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ops := &m40Operations{result: tt.result, err: tt.err}
			binding := m40Binding(t, eebusoutput.Config{Operations: ops})
			got, err := binding.Relay(context.Background(), request)
			if err != tt.err || got.Status != tt.result.Status || got.KernelOutcome != tt.result.KernelOutcome || got.Delivery != tt.result.Delivery ||
				got.Acknowledgement != tt.result.Acknowledgement || got.ReadbackRelation != tt.result.ReadbackRelation || got.Reason != tt.result.Reason {
				t.Fatalf("relay changed the route result: %s err=%v", m40JSON(t, got), err)
			}
			if ops.calls.Load() != 1 || !bytes.Equal(m40JSON(t, ops.requests[0]), m40JSON(t, request)) {
				t.Fatal("relay altered, retried or dropped the request")
			}
		})
	}
}
