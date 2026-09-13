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

type volatileEvaluationSource struct{ captures int }

func (s *volatileEvaluationSource) Capture(time.Time) ([]Resource, []Field, error) {
	s.captures++
	evaluation := json.RawMessage(`{"context":{"evaluated_at":"2026-09-11T00:00:0` + string(rune('0'+s.captures)) + `Z","evaluate_monotonic":{"nanoseconds":"` + string(rune('0'+s.captures)) + `"}},"facts":[{"freshness":"fresh"}],"evaluation_digest":"digest-` + string(rune('0'+s.captures)) + `"}`)
	resource := actionResource()
	resource.Source = Source{AssetID: "asset", SnapshotID: "snapshot", Revision: "revision", EvaluationDigest: "digest-" + string(rune('0'+s.captures)), BindingID: "binding", SourceEpoch: "epoch", DriverGeneration: 1, Evaluation: evaluation}
	resource.State = "CURRENT"
	return []Resource{resource}, nil, nil
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

func actionResource() Resource {
	return Resource{
		ID:                          "asset",
		Domain:                      "test.pack",
		ServiceID:                   "test.service",
		CapabilityID:                "test.capability",
		ContributionDriverID:        "test.driver",
		ContributionManifestID:      "test.manifest",
		ContributionManifestVersion: "1.0.0",
	}
}

func TestInvokeDoesNotReachNativeOwnerAfterFailedRevalidation(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker, reject := &invokeFake{}, &rejectRevalidate{}
	c := New(idx, &sourceFake{resources: []Resource{actionResource()}}, authFake{}, invoker)
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

func TestPublishDetachesCanonicalManifestAndNoopsOnIdenticalDigest(t *testing.T) {
	m, idx := actionManifestAndIndex()
	c := New(idx, nil, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := contributionv1.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(replayed); err != nil {
		t.Fatal(err)
	}
	second, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision != second.CatalogRevision {
		t.Fatalf("identical publication changed revision: %s != %s", first.CatalogRevision, second.CatalogRevision)
	}
	m.Actions[0].Label.Default = "caller mutation"
	key := identity{driver: "test.driver", manifest: "test.manifest", version: "1.0.0"}
	if got := c.descriptors[key].Manifest.Actions[0].Label.Default; got == "caller mutation" {
		t.Fatal("composer retained caller-owned manifest slice")
	}
}

func TestWithdrawAbsentOrRepeatedIdentityIsRevisionNoop(t *testing.T) {
	m, idx := actionManifestAndIndex()
	c := New(idx, nil, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	c.Withdraw("missing.driver", "missing.manifest", "1.0.0")
	afterMissing, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision != afterMissing.CatalogRevision {
		t.Fatal("absent withdrawal changed catalog revision")
	}
	c.Withdraw("test.driver", "test.manifest", "1.0.0")
	afterPresent, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision == afterPresent.CatalogRevision {
		t.Fatal("present withdrawal did not change catalog revision")
	}
	c.Withdraw("test.driver", "test.manifest", "1.0.0")
	afterRepeated, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if afterPresent.CatalogRevision != afterRepeated.CatalogRevision {
		t.Fatal("repeated withdrawal changed catalog revision")
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
	m, idx := actionManifestAndIndex()
	resource := actionResource()
	resource.Domain = "helianthus.pack.pv"
	s := &sourceFake{resources: []Resource{resource}}
	c := New(idx, s, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(1, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	got, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range got.Domains {
		if domain.Pack.ID == "helianthus.pack.pv" && domain.SourceState != "PRESENT" {
			t.Fatalf("pv domain=%+v", domain)
		}
		if domain.Pack.ID != "helianthus.pack.pv" && domain.SourceState != "ABSENT" {
			t.Fatalf("absent domain=%+v", domain)
		}
	}
}

func TestConflictQuarantinesBothAndInvocationIsExactlyOnce(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker := &invokeFake{}
	c := New(idx, &sourceFake{resources: []Resource{actionResource()}}, authFake{}, invoker)
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

func TestInvokeCatalogRevisionIgnoresResponseEvaluationInstant(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker, revalidator := &invokeFake{}, &revalidateFake{}
	c := New(idx, &sourceFake{resources: []Resource{actionResource()}}, authFake{}, invoker)
	now := time.Unix(10, 0).UTC()
	c.now = func() time.Time { now = now.Add(time.Second); return now }
	c.SetRevalidator(revalidator)
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	claim := Claims{CatalogRevision: first.CatalogRevision, Digest: first.Contributions[0].Digest, DriverID: "test.driver", ManifestID: "test.manifest", ManifestVersion: "1.0.0", ActionID: "action", ResourceID: "asset", CapabilityID: "test.capability", IdempotencyKey: "once", Deadline: time.Unix(100, 0)}
	if _, err := c.Invoke("caller", claim); err != nil {
		t.Fatalf("response evaluation time made action stale: %v", err)
	}
	if invoker.calls.Load() != 1 || revalidator.calls.Load() != 1 {
		t.Fatalf("calls=%d revalidations=%d", invoker.calls.Load(), revalidator.calls.Load())
	}
}

func TestInvokeCatalogRevisionNormalizesProductionSourceEvaluationClock(t *testing.T) {
	m, idx := actionManifestAndIndex()
	invoker, revalidator := &invokeFake{}, &revalidateFake{}
	source := &volatileEvaluationSource{}
	c := New(idx, source, authFake{}, invoker)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	c.SetRevalidator(revalidator)
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	claim := Claims{CatalogRevision: first.CatalogRevision, Digest: first.Contributions[0].Digest, DriverID: "test.driver", ManifestID: "test.manifest", ManifestVersion: "1.0.0", ActionID: "action", ResourceID: "asset", CapabilityID: "test.capability", SnapshotID: "snapshot", Revision: "revision", BindingID: "binding", SourceEpoch: "epoch", DriverGeneration: 1, IdempotencyKey: "once", Deadline: time.Unix(100, 0)}
	if _, err := c.Invoke("caller", claim); err != nil {
		t.Fatalf("volatile production-source evaluation clock made action stale: %v", err)
	}
	if source.captures != 2 || invoker.calls.Load() != 1 {
		t.Fatalf("captures/native=%d/%d", source.captures, invoker.calls.Load())
	}
}

func TestCatalogRevisionPreservesLargeNonClockEvaluationIntegers(t *testing.T) {
	m, idx := actionManifestAndIndex()
	resource := actionResource()
	resource.Source = Source{SnapshotID: "snapshot", Revision: "revision", BindingID: "binding", SourceEpoch: "epoch", DriverGeneration: 1, EvaluationDigest: "volatile", Evaluation: json.RawMessage(`{"context":{"evaluated_at":"2026-09-13T00:00:00Z"},"facts":[{"counter":9007199254740992}]}`)}
	source := &sourceFake{resources: []Resource{resource}}
	c := New(idx, source, authFake{}, &invokeFake{})
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	source.resources[0].Source.Evaluation = json.RawMessage(`{"context":{"evaluated_at":"2026-09-13T00:00:01Z"},"facts":[{"counter":9007199254740993}]}`)
	second, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision == second.CatalogRevision {
		t.Fatal("adjacent large non-clock integers collapsed into one action revision")
	}
}

func TestCatalogRevisionPreservesSameNamedNonContextEvidence(t *testing.T) {
	m, idx := actionManifestAndIndex()
	resource := actionResource()
	resource.Source = Source{SnapshotID: "snapshot", Revision: "revision", BindingID: "binding", SourceEpoch: "epoch", DriverGeneration: 1, EvaluationDigest: "outer-1", Evaluation: json.RawMessage(`{"context":{"evaluated_at":"2026-09-13T00:00:00Z","evaluate_monotonic":{"nanoseconds":"1"}},"facts":[{"evaluated_at":"native-evidence-1","evaluate_monotonic":{"counter":"1"}}],"evaluation_digest":"inner-1"}`)}
	source := &sourceFake{resources: []Resource{resource}}
	c := New(idx, source, authFake{}, &invokeFake{})
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	first, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	source.resources[0].Source.EvaluationDigest = "outer-2"
	source.resources[0].Source.Evaluation = json.RawMessage(`{"context":{"evaluated_at":"2026-09-13T00:00:01Z","evaluate_monotonic":{"nanoseconds":"2"}},"facts":[{"evaluated_at":"native-evidence-2","evaluate_monotonic":{"counter":"2"}}],"evaluation_digest":"inner-2"}`)
	second, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if first.CatalogRevision == second.CatalogRevision {
		t.Fatal("same-named non-context evidence was stripped from action revision")
	}
}

func TestCatalogActionRequiresExactContributionAndSemanticContext(t *testing.T) {
	m, idx := actionManifestAndIndex()
	valid := actionResource()
	cases := []struct {
		name   string
		mutate func(*Resource)
	}{
		{"driver", func(r *Resource) { r.ContributionDriverID = "other.driver" }},
		{"manifest", func(r *Resource) { r.ContributionManifestID = "other.manifest" }},
		{"version", func(r *Resource) { r.ContributionManifestVersion = "2.0.0" }},
		{"missing-service", func(r *Resource) { r.ServiceID = "" }},
		{"service", func(r *Resource) { r.ServiceID = "other.service" }},
		{"missing-capability", func(r *Resource) { r.CapabilityID = "" }},
		{"capability", func(r *Resource) { r.CapabilityID = "other.capability" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resource := valid
			tc.mutate(&resource)
			c := New(idx, &sourceFake{resources: []Resource{resource}}, authFake{}, &invokeFake{})
			c.now = func() time.Time { return time.Unix(10, 0).UTC() }
			if err := c.Publish(m); err != nil {
				t.Fatal(err)
			}
			catalog, err := c.Catalog("caller")
			if err != nil {
				t.Fatal(err)
			}
			if len(catalog.Actions) != 0 {
				t.Fatalf("mismatched source exposed action: %+v", catalog.Actions)
			}
		})
	}
}

func TestCatalogScopesCollidingResourceIDsByContribution(t *testing.T) {
	first, idx := actionManifestAndIndex()
	second := first
	second.Contributor.DriverID = "other.driver"
	second.ManifestID = "other.manifest"
	firstResource := actionResource()
	secondResource := firstResource
	secondResource.ContributionDriverID = second.Contributor.DriverID
	secondResource.ContributionManifestID = second.ManifestID
	c := New(idx, &sourceFake{resources: []Resource{firstResource, secondResource}}, authFake{}, &invokeFake{})
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(first); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(second); err != nil {
		t.Fatal(err)
	}
	catalog, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Resources) != 2 || len(catalog.Actions) != 2 {
		t.Fatalf("cross-contribution resource collision hid valid rows: resources=%+v actions=%+v", catalog.Resources, catalog.Actions)
	}
	if catalog.Actions[0].ContributionDriverID == catalog.Actions[1].ContributionDriverID {
		t.Fatalf("actions lost contribution ownership: %+v", catalog.Actions)
	}
}

func TestCatalogFiltersWithdrawnRowsAndRejectsDanglingAcceptedFields(t *testing.T) {
	m, idx := actionManifestAndIndex()
	resource := actionResource()
	field := Field{ID: "field", ResourceID: resource.ID, ContributionDriverID: resource.ContributionDriverID, ContributionManifestID: resource.ContributionManifestID, ContributionManifestVersion: resource.ContributionManifestVersion, Value: json.RawMessage(`1`)}
	source := &sourceFake{resources: []Resource{resource}, fields: []Field{field}}
	c := New(idx, source, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(10, 0).UTC() }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	before, err := c.Catalog("caller")
	if err != nil || len(before.Resources) != 1 || len(before.Fields) != 1 {
		t.Fatalf("accepted rows missing: catalog=%+v err=%v", before, err)
	}
	c.Withdraw(m.Contributor.DriverID, m.ManifestID, m.ManifestVersion)
	after, err := c.Catalog("caller")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Resources) != 0 || len(after.Fields) != 0 {
		t.Fatalf("withdrawn source rows remained visible: %+v", after)
	}
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
	source.resources = nil
	if _, err := c.Catalog("caller"); err == nil || !strings.Contains(err.Error(), "field has no exact") {
		t.Fatalf("dangling accepted field err=%v", err)
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
	m, idx := actionManifestAndIndex()
	resource := actionResource()
	resource.Domain = "helianthus.pack.pv"
	field := Field{ID: "f", ResourceID: resource.ID, ContributionDriverID: resource.ContributionDriverID, ContributionManifestID: resource.ContributionManifestID, ContributionManifestVersion: resource.ContributionManifestVersion, Value: json.RawMessage(`1`)}
	s := &fencedSourceFake{sourceFake: sourceFake{resources: []Resource{resource}, fields: []Field{field}}, fence: "one"}
	c := New(idx, s, authFake{}, nil)
	c.now = func() time.Time { return time.Unix(1, 0) }
	if err := c.Publish(m); err != nil {
		t.Fatal(err)
	}
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
