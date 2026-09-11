package catalogv1

import (
	"encoding/json"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"sync/atomic"
	"testing"
	"time"
)

type sourceFake struct {
	calls     atomic.Int32
	resources []Resource
	fields    []Field
}

func (s *sourceFake) Capture(time.Time) ([]Resource, []Field, error) {
	s.calls.Add(1)
	return s.resources, s.fields, nil
}

type authFake struct{}

func (authFake) Scope(any) (string, error) { return "scope-r1", nil }
func (authFake) Discover(any, Action) bool { return true }
func (authFake) Invoke(any, Action) bool   { return true }

type invokeFake struct{ calls atomic.Int32 }

func (i *invokeFake) Invoke(Action, Claims) (json.RawMessage, error) {
	i.calls.Add(1)
	return json.RawMessage(`{"outcome":"ACK"}`), nil
}

type revalidateFake struct{ calls atomic.Int32 }

func (r *revalidateFake) Revalidate(Action, Claims) error { r.calls.Add(1); return nil }

func actionManifestAndIndex() (contributionv1.Manifest, *contributionv1.StaticIndex) {
	p := contributionv1.PackRef{ID: "test.pack", Version: "1.0.0"}
	ref := func(id string) contributionv1.DefinitionRef {
		return contributionv1.DefinitionRef{Pack: p, ID: id, Version: "1.0.0"}
	}
	service, capability, operation, argument, effect := ref("test.service"), ref("test.capability"), ref("test.operation"), ref("test.argument"), ref("test.effect")
	idx := contributionv1.NewStaticIndex()
	idx.AddPack(p)
	for _, r := range []contributionv1.DefinitionRef{service, capability, operation, argument, effect} {
		idx.AddDefinition(r)
	}
	idx.AddServiceCapability(service, capability)
	idx.AddOperation(operation, capability, service, argument, effect)
	m := contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: "test.manifest", ManifestVersion: "1.0.0", Contributor: contributionv1.Contributor{DriverID: "test.driver", NativeContract: contributionv1.NativeContractRef{Owner: "test", Contract: "test-contract", Version: "1.0.0"}}, Requires: contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{p}}, Groups: []contributionv1.Group{{ID: "group", Label: contributionv1.Label{Key: "test.group", Default: "Group"}, ResourceContext: "group", Order: 0}}, Fields: []contributionv1.Field{}, Views: []contributionv1.View{}, Actions: []contributionv1.Action{{ID: "action", Group: "group", Label: contributionv1.Label{Key: "test.action", Default: "Action"}, OperationRef: operation, CapabilityRef: capability, ServiceRef: service, ArgumentRef: argument, EffectRef: effect, Order: 0}}, Diagnostics: []contributionv1.Diagnostic{}}
	return m, idx
}
func TestCatalogFivePacksAbsentAndDeterministic(t *testing.T) {
	s := &sourceFake{}
	c := New(contributionv1.NewStaticIndex(), s, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(1, 0).UTC() }
	a, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Domains) != 5 || a.CatalogDigest != b.CatalogDigest || s.calls.Load() != 2 {
		t.Fatalf("catalog=%+v calls=%d", a, s.calls.Load())
	}
	for _, d := range a.Domains {
		if d.SourceState != "ABSENT" {
			t.Fatal(d)
		}
	}
}
func TestCatalogMakesSourcePresenceTruthful(t *testing.T) {
	s := &sourceFake{resources: []Resource{{ID: "pv-1", Domain: "helianthus.pv", State: "CURRENT"}}}
	c := New(contributionv1.NewStaticIndex(), s, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(1, 0).UTC() }
	got, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range got.Domains {
		if domain.Pack.ID == "helianthus.pv" && domain.SourceState != "PRESENT" {
			t.Fatalf("pv domain=%+v", domain)
		}
		if domain.Pack.ID != "helianthus.pv" && domain.SourceState != "ABSENT" {
			t.Fatalf("absent domain=%+v", domain)
		}
	}
}

func TestConflictQuarantinesBothAndInvocationIsExactlyOnce(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker := &invokeFake{}
	c := New(idx, &sourceFake{resources: []Resource{{ID: "group", Domain: "helianthus.pv"}}}, authFake{}, invoker)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	revalidator := &revalidateFake{}
	c.SetRevalidator(revalidator)
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	catalog, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	claim := Claims{CatalogRevision: catalog.CatalogRevision, ActionID: "action", ResourceID: "group", CapabilityID: "test.capability", IdempotencyKey: "once", Deadline: time.Unix(20, 0)}
	if _, err := c.Invoke("caller", claim); err != nil {
		t.Fatal(err)
	}
	if invoker.calls.Load() != 1 {
		t.Fatalf("calls=%d", invoker.calls.Load())
	}
	if revalidator.calls.Load() != 1 {
		t.Fatalf("revalidations=%d", revalidator.calls.Load())
	}
	m.Groups[0].Label.Default = "Changed"
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	after, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Contributions) != 0 || len(after.Quarantines) != 1 {
		t.Fatalf("after=%+v", after)
	}
}
