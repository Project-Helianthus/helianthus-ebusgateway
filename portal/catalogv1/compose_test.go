package catalogv1

import (
	"encoding/json"
	"errors"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sourceFake struct {
	calls     atomic.Int32
	resources []Resource
	fields    []Field
}
type fencedSourceFake struct {
	sourceFake
	fence string
}

func (s *fencedSourceFake) Fence() string { return s.fence }

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

type rejectRevalidate struct{ calls atomic.Int32 }

func (r *rejectRevalidate) Revalidate(Action, Claims) error {
	r.calls.Add(1)
	return errors.New("source generation changed")
}

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
	m := contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: "test.manifest", ManifestVersion: "1.0.0", Contributor: contributionv1.Contributor{DriverID: "test.driver", NativeContract: contributionv1.NativeContractRef{Owner: "test", Contract: "test-contract", Version: "1.0.0"}}, Requires: contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{p}}, Groups: []contributionv1.Group{{ID: "group", Label: contributionv1.Label{Key: "test.group", Default: "Group"}, ResourceContext: "asset", Order: 0}}, Fields: []contributionv1.Field{}, Views: []contributionv1.View{}, Actions: []contributionv1.Action{{ID: "action", Group: "group", Label: contributionv1.Label{Key: "test.action", Default: "Action"}, OperationRef: operation, CapabilityRef: capability, ServiceRef: service, ArgumentRef: argument, EffectRef: effect, Order: 0}}, Diagnostics: []contributionv1.Diagnostic{}}
	return m, idx
}

func TestInvokeDoesNotReachNativeOwnerAfterFailedRevalidation(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker, reject := &invokeFake{}, &rejectRevalidate{}
	c := New(idx, &sourceFake{resources: []Resource{{ID: "asset", Domain: "test.pack"}}}, authFake{}, invoker)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	c.SetRevalidator(reject)
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	catalog, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	claim := Claims{CatalogRevision: catalog.CatalogRevision, Digest: catalog.Contributions[0].Digest, DriverID: "test.driver", ManifestID: "test.manifest", ManifestVersion: "1.0.0", ActionID: "action", ResourceID: "asset", CapabilityID: "test.capability", IdempotencyKey: "once", Deadline: time.Unix(20, 0)}
	if _, err := c.Invoke("caller", claim); err == nil {
		t.Fatal("stale source revalidation invoked native owner")
	}
	if reject.calls.Load() != 1 || invoker.calls.Load() != 0 {
		t.Fatalf("revalidation/native calls = %d/%d", reject.calls.Load(), invoker.calls.Load())
	}
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
	c := New(idx, &sourceFake{resources: []Resource{{ID: "asset", Domain: "helianthus.pv"}}}, authFake{}, invoker)
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
	if len(catalog.Actions) != 1 || catalog.Actions[0].ResourceID != "asset" {
		t.Fatalf("action mapping=%+v", catalog.Actions)
	}
	claim := Claims{CatalogRevision: catalog.CatalogRevision, Digest: catalog.Contributions[0].Digest, DriverID: "test.driver", ManifestID: "test.manifest", ManifestVersion: "1.0.0", ActionID: "action", ResourceID: "asset", CapabilityID: "test.capability", IdempotencyKey: "once", Deadline: time.Unix(20, 0)}
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

func TestCanonicalOrderUsesCompleteTuplesAndRevisionBindsFields(t *testing.T) {
	base := Catalog{Contract: Contract, Domains: []Domain{}, Contributions: []Identity{{DriverID: "a", ManifestID: "bc", ManifestVersion: "1"}, {DriverID: "ab", ManifestID: "c", ManifestVersion: "1"}}, Resources: []Resource{{ID: "same", ContributionDriverID: "a", ContributionManifestID: "bc", ContributionManifestVersion: "1"}, {ID: "same", ContributionDriverID: "ab", ContributionManifestID: "c", ContributionManifestVersion: "1"}}, Fields: []Field{{ID: "same", ResourceID: "r", ContributionDriverID: "a", ContributionManifestID: "bc", ContributionManifestVersion: "1"}, {ID: "same", ResourceID: "r", ContributionDriverID: "ab", ContributionManifestID: "c", ContributionManifestVersion: "1"}}, Actions: []Action{}, Quarantines: []Quarantine{{DriverID: "a", ManifestID: "bc", ManifestVersion: "1"}, {DriverID: "ab", ManifestID: "c", ManifestVersion: "1"}}}
	want, _ := CanonicalJSON(base)
	for i := 0; i < 50; i++ {
		got := clone(base)
		rand.New(rand.NewSource(int64(i))).Shuffle(len(got.Contributions), func(i, j int) {
			got.Contributions[i], got.Contributions[j] = got.Contributions[j], got.Contributions[i]
		})
		rand.New(rand.NewSource(int64(i+100))).Shuffle(len(got.Resources), func(i, j int) { got.Resources[i], got.Resources[j] = got.Resources[j], got.Resources[i] })
		b, _ := CanonicalJSON(got)
		if string(b) != string(want) {
			t.Fatalf("noncanonical iteration %d", i)
		}
	}
	s := &fencedSourceFake{sourceFake: sourceFake{resources: []Resource{{ID: "r", Domain: "helianthus.pv"}}, fields: []Field{{ID: "f", ResourceID: "r", Value: json.RawMessage(`1`)}}}, fence: "one"}
	c := New(contributionv1.NewStaticIndex(), s, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(1, 0) }
	first, err := c.Catalog("c")
	if err != nil {
		t.Fatal(err)
	}
	s.fields[0].Value = json.RawMessage(`2`)
	s.fence = "two"
	second, err := c.Catalog("c")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision == second.CatalogRevision {
		t.Fatal("field/fence change did not revise catalog")
	}
}

func TestWireMembersUseClosedSnakeCaseNames(t *testing.T) {
	b, err := json.Marshal(Catalog{Contract: Contract, Contributions: []Identity{{DriverID: "driver", ManifestID: "manifest", ManifestVersion: "1", Digest: "digest"}}, Resources: []Resource{{ID: "resource", Source: Source{SnapshotID: "snapshot"}}}, Fields: []Field{{ID: "field", ResourceID: "resource"}}, Actions: []Action{{ID: "action", ResourceID: "resource"}}, Quarantines: []Quarantine{{DriverID: "driver", ManifestID: "manifest", ManifestVersion: "1", Reason: "conflict"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"driver_id"`, `"manifest_id"`, `"manifest_version"`, `"snapshot_id"`, `"resource_id"`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %s in %s", want, b)
		}
	}
}
