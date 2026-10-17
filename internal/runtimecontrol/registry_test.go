package runtimecontrol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

const modulePath = "github.com/Project-Helianthus/helianthus-ebusgateway"

type fakeRuntime struct{ family string }

func (*fakeRuntime) Start(context.Context) (uint64, error)   { return 1, nil }
func (*fakeRuntime) Replace(context.Context) (uint64, error) { return 2, nil }
func (*fakeRuntime) Stop(context.Context) error              { return nil }
func (*fakeRuntime) Generation() uint64                      { return 1 }
func (*fakeRuntime) Revision() uint64                        { return 1 }
func (*fakeRuntime) SafetyQuarantined() bool                 { return false }

type alphaRuntime struct {
	fakeRuntime
	construction Construction
}

type betaRuntime struct {
	fakeRuntime
	construction Construction
}

type pointerAdapter struct{ calls *atomic.Int64 }

func (a *pointerAdapter) Construct(context.Context, Construction) (drivermanager.Runtime, error) {
	a.calls.Add(1)
	return &fakeRuntime{}, nil
}

type fakeHub map[SourceID]DriverID

func (h fakeHub) SourceOwner(source SourceID) (DriverID, bool) {
	owner, ok := h[source]
	return owner, ok
}

func countingAdapter(calls *atomic.Int64) LifecycleAdapter {
	return NewAdapter(func(context.Context, Construction) (*fakeRuntime, error) {
		calls.Add(1)
		return &fakeRuntime{}, nil
	})
}

func fixture(calls *atomic.Int64, id DriverID, direction Direction, sources ...SourceID) Descriptor {
	return Descriptor{
		Format:      DescriptorFormatV1,
		DriverID:    id,
		Direction:   direction,
		Disposition: DispositionEnabled,
		NativeContract: NativeContractRef{
			Owner:    "example.org/native/owner-" + string(id),
			Contract: "synthetic.contract",
			Version:  "v1.0.0",
		},
		Adapter: countingAdapter(calls),
		Sources: append([]SourceID(nil), sources...),
	}
}

func TestRegisterOneFixturePerDirection(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(fakeHub{})
	south := fixture(&calls, "fake.south", DirectionSouthbound, "south-src")
	north := fixture(&calls, "fake.north", DirectionNorthbound, "north-src")
	north.Disposition = DispositionDisabled
	both := fixture(&calls, "fake.both", DirectionBoth, "both-src-b", "both-src-a")
	if err := registry.Register(south, north, both); err != nil {
		t.Fatalf("Register: %v", err)
	}
	list := registry.List()
	if list.CatalogRevision != 1 {
		t.Fatalf("catalog revision = %d, want 1", list.CatalogRevision)
	}
	var ids []DriverID
	for _, entry := range list.Drivers {
		ids = append(ids, entry.DriverID)
	}
	if want := []DriverID{"fake.both", "fake.north", "fake.south"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("list order = %v, want %v", ids, want)
	}
	directions := map[DriverID]Direction{"fake.both": DirectionBoth, "fake.north": DirectionNorthbound, "fake.south": DirectionSouthbound}
	for _, entry := range list.Drivers {
		if entry.Direction != directions[entry.DriverID] {
			t.Fatalf("%s direction = %s", entry.DriverID, entry.Direction)
		}
	}
	got, err := registry.Get("fake.north")
	if err != nil || got.Driver.Disposition != DispositionDisabled || got.CatalogRevision != 1 {
		t.Fatalf("Get disabled entry = %+v, %v", got, err)
	}
	gotBoth, _ := registry.Get("fake.both")
	if want := []SourceID{"both-src-a", "both-src-b"}; !reflect.DeepEqual(gotBoth.Driver.Sources, want) {
		t.Fatalf("sources = %v, want sorted %v", gotBoth.Driver.Sources, want)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestRegisterRejectsBeforeEffects(t *testing.T) {
	type mutation func(calls *atomic.Int64) []Descriptor
	single := func(edit func(*Descriptor)) mutation {
		return func(calls *atomic.Int64) []Descriptor {
			sibling := fixture(calls, "sibling.ok", DirectionSouthbound, "sibling-src")
			bad := fixture(calls, "fake.bad", DirectionNorthbound, "bad-src")
			edit(&bad)
			return []Descriptor{sibling, bad}
		}
	}
	native := func(owner, contract, version string) mutation {
		return single(func(d *Descriptor) {
			d.NativeContract = NativeContractRef{Owner: owner, Contract: contract, Version: version}
		})
	}
	cases := []struct {
		name  string
		batch mutation
		want  error
	}{
		{"empty batch", func(*atomic.Int64) []Descriptor { return nil }, ErrEmptyRegistration},
		{"missing format", single(func(d *Descriptor) { d.Format = "" }), ErrUnsupportedFormat},
		{"unknown format version", single(func(d *Descriptor) { d.Format = "v2" }), ErrUnsupportedFormat},
		{"empty driver id", single(func(d *Descriptor) { d.DriverID = "" }), ErrNoncanonicalDriverID},
		{"uppercase driver id", single(func(d *Descriptor) { d.DriverID = "Fake.bad" }), ErrNoncanonicalDriverID},
		{"leading digit driver id", single(func(d *Descriptor) { d.DriverID = "1fake" }), ErrNoncanonicalDriverID},
		{"trailing separator driver id", single(func(d *Descriptor) { d.DriverID = "fake." }), ErrNoncanonicalDriverID},
		{"double separator driver id", single(func(d *Descriptor) { d.DriverID = "fake..bad" }), ErrNoncanonicalDriverID},
		{"underscore driver id", single(func(d *Descriptor) { d.DriverID = "fake_bad" }), ErrNoncanonicalDriverID},
		{"whitespace driver id", single(func(d *Descriptor) { d.DriverID = " fake.bad" }), ErrNoncanonicalDriverID},
		{"driver id over 128 bytes", single(func(d *Descriptor) { d.DriverID = DriverID("a" + strings.Repeat("b", 128)) }), ErrNoncanonicalDriverID},
		{"duplicate driver id in batch", func(calls *atomic.Int64) []Descriptor {
			return []Descriptor{fixture(calls, "fake.dup", DirectionSouthbound, "dup-a"), fixture(calls, "fake.dup", DirectionNorthbound, "dup-b")}
		}, ErrDuplicateDriverID},
		{"duplicate of installed driver id", single(func(d *Descriptor) { d.DriverID = "base.driver" }), ErrDuplicateDriverID},
		{"empty direction", single(func(d *Descriptor) { d.Direction = "" }), ErrInvalidDirection},
		{"uppercase direction", single(func(d *Descriptor) { d.Direction = "SOUTHBOUND" }), ErrInvalidDirection},
		{"unknown direction", single(func(d *Descriptor) { d.Direction = "sideways" }), ErrInvalidDirection},
		{"empty disposition", single(func(d *Descriptor) { d.Disposition = "" }), ErrInvalidDisposition},
		{"unknown disposition", single(func(d *Descriptor) { d.Disposition = "maybe" }), ErrInvalidDisposition},
		{"empty native owner", native("", "synthetic.contract", "v1"), ErrInvalidNativeContractRef},
		{"empty native contract", native("example.org/owner", "", "v1"), ErrInvalidNativeContractRef},
		{"empty native version", native("example.org/owner", "synthetic.contract", ""), ErrInvalidNativeContractRef},
		{"endpoint url in native owner", native("tcp://device.example:502", "synthetic.contract", "v1"), ErrInvalidNativeContractRef},
		{"credential in native owner", native("user:secret@example.org", "synthetic.contract", "v1"), ErrInvalidNativeContractRef},
		{"ip literal in native contract", native("example.org/owner", "192.0.2.10", "v1"), ErrInvalidNativeContractRef},
		{"host port in native version", native("example.org/owner", "synthetic.contract", "host:502"), ErrInvalidNativeContractRef},
		{"path traversal in native owner", native("example.org/../owner", "synthetic.contract", "v1"), ErrInvalidNativeContractRef},
		{"whitespace in native contract", native("example.org/owner", "synthetic contract", "v1"), ErrInvalidNativeContractRef},
		{"control byte in native version", native("example.org/owner", "synthetic.contract", "v1\n"), ErrInvalidNativeContractRef},
		{"oversized native field", native("example.org/"+strings.Repeat("o", 256), "synthetic.contract", "v1"), ErrInvalidNativeContractRef},
		{"nil adapter", single(func(d *Descriptor) { d.Adapter = nil }), ErrMissingAdapter},
		{"typed nil pointer adapter", single(func(d *Descriptor) { d.Adapter = (*pointerAdapter)(nil) }), ErrMissingAdapter},
		{"typed adapter with nil factory", single(func(d *Descriptor) {
			d.Adapter = NewAdapter[*fakeRuntime](nil)
		}), ErrMissingAdapter},
		{"empty source id", single(func(d *Descriptor) { d.Sources = []SourceID{""} }), ErrInvalidSourceID},
		{"whitespace source id", single(func(d *Descriptor) { d.Sources = []SourceID{"bad src"} }), ErrInvalidSourceID},
		{"duplicate source within descriptor", single(func(d *Descriptor) { d.Sources = []SourceID{"x-src", "x-src"} }), ErrDuplicateSourceID},
		{"duplicate source across batch", single(func(d *Descriptor) { d.Sources = []SourceID{"sibling-src"} }), ErrDuplicateSourceID},
		{"source owned by installed driver", single(func(d *Descriptor) { d.Sources = []SourceID{"base-src"} }), ErrDuplicateSourceID},
		{"source owned in hub", single(func(d *Descriptor) { d.Sources = []SourceID{"hub-src"} }), ErrHubSourceCollision},
		{"unknown hook", single(func(d *Descriptor) { d.Hooks = []HookDeclaration{{Name: "telemetry", Version: "v1"}} }), ErrUnknownHook},
		{"empty hook name", single(func(d *Descriptor) { d.Hooks = []HookDeclaration{{Name: "", Version: "v1"}} }), ErrUnknownHook},
		{"reserved hook unknown version", single(func(d *Descriptor) { d.Hooks = []HookDeclaration{{Name: HookMCP, Version: "v999"}} }), ErrHookUnimplemented},
	}
	for _, name := range []HookName{HookMCP, HookSetup, HookPortal, HookSemantic, HookEvidence} {
		name := name
		cases = append(cases, struct {
			name  string
			batch mutation
			want  error
		}{"reserved hook " + string(name), single(func(d *Descriptor) { d.Hooks = []HookDeclaration{{Name: name, Version: "v1"}} }), ErrHookUnimplemented})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			hub := fakeHub{"hub-src": "other.driver"}
			registry := NewRegistry(hub)
			if err := registry.Register(fixture(&calls, "base.driver", DirectionBoth, "base-src")); err != nil {
				t.Fatalf("baseline Register: %v", err)
			}
			before := registry.List()
			hubBefore := map[SourceID]DriverID{"hub-src": "other.driver"}
			err := registry.Register(tc.batch(&calls)...)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Register error = %v, want %v", err, tc.want)
			}
			if after := registry.List(); !reflect.DeepEqual(before, after) {
				t.Fatalf("registry changed after refusal:\nbefore %+v\nafter  %+v", before, after)
			}
			if _, err := registry.Get("sibling.ok"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("valid sibling installed after refusal: %v", err)
			}
			if !reflect.DeepEqual(map[SourceID]DriverID(hub), hubBefore) {
				t.Fatalf("hub ownership mutated: %v", hub)
			}
			if calls.Load() != 0 {
				t.Fatalf("factory calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestRegistrationErrorDoesNotEchoRefusedMaterial(t *testing.T) {
	var calls atomic.Int64
	bad := fixture(&calls, "fake.bad", DirectionSouthbound, "src")
	bad.NativeContract.Owner = "user:secret@example.org"
	err := NewRegistry(nil).Register(bad)
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error = %v", err)
	}
	bad = fixture(&calls, "Bad Id secret", DirectionSouthbound, "src")
	err = NewRegistry(nil).Register(bad)
	var refused *RegistrationError
	if !errors.As(err, &refused) || refused.DriverID != "" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("error = %v", err)
	}
}

func TestHubSourceOwnedBySameDriverIsNotACollision(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(fakeHub{"own-src": "fake.own"})
	if err := registry.Register(fixture(&calls, "fake.own", DirectionSouthbound, "own-src")); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

func TestDescriptorsDeepCopyAndFreeze(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.frozen", DirectionBoth, "frozen-b", "frozen-a")
	descriptor.Sources = append(make([]SourceID, 0, 8), descriptor.Sources...)
	callerSources := descriptor.Sources
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	want, _ := registry.Get("fake.frozen")
	want = GetResult{CatalogRevision: want.CatalogRevision, Driver: want.Driver.clone()}

	callerSources[0] = "mutated-caller"
	callerSources[1] = "mutated-caller-2"
	descriptor.NativeContract.Version = "v9"
	descriptor.Direction = DirectionNorthbound

	listed := registry.List()
	listed.Drivers[0].Sources[0] = "mutated-list"
	listed.Drivers[0].NativeContract.Owner = "mutated"
	got, _ := registry.Get("fake.frozen")
	got.Driver.Sources[0] = "mutated-get"
	binding, err := registry.Binding("fake.frozen")
	if err != nil {
		t.Fatalf("Binding: %v", err)
	}
	binding.Construction.Sources[0] = "mutated-binding"

	again, _ := registry.Get("fake.frozen")
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("frozen entry changed:\nwant %+v\ngot  %+v", want, again)
	}
	if want := []SourceID{"frozen-a", "frozen-b"}; !reflect.DeepEqual(again.Driver.Sources, want) {
		t.Fatalf("sources = %v, want %v", again.Driver.Sources, want)
	}
	rebound, _ := registry.Binding("fake.frozen")
	if !reflect.DeepEqual(rebound.Construction.Sources, []SourceID{"frozen-a", "frozen-b"}) {
		t.Fatalf("binding sources = %v", rebound.Construction.Sources)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func ctl01Fixtures(calls *atomic.Int64) []Descriptor {
	disabled := fixture(calls, "zeta.disabled", DirectionNorthbound, "zeta-src")
	disabled.Disposition = DispositionDisabled
	return []Descriptor{
		fixture(calls, "alpha.one", DirectionSouthbound, "alpha-src"),
		fixture(calls, "alpha-two", DirectionBoth, "alpha2-src-b", "alpha2-src-a"),
		fixture(calls, "beta", DirectionNorthbound),
		disabled,
		fixture(calls, "alpha", DirectionSouthbound, "alpha0-src"),
	}
}

func TestListGetRegistrationOrderIndependentCTL01(t *testing.T) {
	var calls atomic.Int64
	forward := ctl01Fixtures(&calls)
	reversed := make([]Descriptor, len(forward))
	for i := range forward {
		reversed[len(forward)-1-i] = forward[i]
	}
	rotated := append(append([]Descriptor(nil), forward[2:]...), forward[:2]...)

	batchA, batchB := NewRegistry(nil), NewRegistry(nil)
	if err := batchA.Register(forward...); err != nil {
		t.Fatal(err)
	}
	if err := batchB.Register(reversed...); err != nil {
		t.Fatal(err)
	}
	stepA, stepB := NewRegistry(nil), NewRegistry(nil)
	for _, d := range reversed {
		if err := stepA.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range rotated {
		if err := stepB.Register(d); err != nil {
			t.Fatal(err)
		}
	}
	compare := func(name string, a, b *Registry) {
		t.Helper()
		listA, listB := a.List(), b.List()
		if !reflect.DeepEqual(listA, listB) {
			t.Fatalf("%s: list differs:\n%+v\n%+v", name, listA, listB)
		}
		for i := 1; i < len(listA.Drivers); i++ {
			if listA.Drivers[i-1].DriverID >= listA.Drivers[i].DriverID {
				t.Fatalf("%s: not bytewise sorted: %v", name, listA.Drivers)
			}
		}
		for _, entry := range listA.Drivers {
			getA, errA := a.Get(entry.DriverID)
			getB, errB := b.Get(entry.DriverID)
			if errA != nil || errB != nil || !reflect.DeepEqual(getA, getB) {
				t.Fatalf("%s: get %s differs: %+v %v / %+v %v", name, entry.DriverID, getA, errA, getB, errB)
			}
			if !reflect.DeepEqual(getA.Driver, entry) || getA.CatalogRevision != listA.CatalogRevision {
				t.Fatalf("%s: get %s incoherent with list", name, entry.DriverID)
			}
		}
	}
	compare("batch", batchA, batchB)
	compare("stepwise", stepA, stepB)
	if got := batchA.List(); len(got.Drivers) != 5 || got.CatalogRevision != 1 {
		t.Fatalf("batch list = %+v", got)
	}
	if got := stepA.List(); got.CatalogRevision != 5 {
		t.Fatalf("stepwise revision = %d, want 5", got.CatalogRevision)
	}
	want := []DriverID{"alpha", "alpha-two", "alpha.one", "beta", "zeta.disabled"}
	var ids []DriverID
	for _, entry := range batchA.List().Drivers {
		ids = append(ids, entry.DriverID)
	}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("order = %v, want %v", ids, want)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestUnknownAndInvalidIDsConstructNothing(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	if err := registry.Register(fixture(&calls, "fake.known", DirectionSouthbound, "known-src")); err != nil {
		t.Fatal(err)
	}
	before := registry.List()
	for _, id := range []DriverID{"fake.unknown", "", "Fake.Known", "fake.known ", "fake..known"} {
		if _, err := registry.Get(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%q) = %v, want ErrNotFound", id, err)
		}
		if _, err := registry.Binding(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Binding(%q) = %v, want ErrNotFound", id, err)
		}
		if err := registry.ResolveHook(id, HookMCP); !errors.Is(err, ErrNotFound) {
			t.Fatalf("ResolveHook(%q) = %v, want ErrNotFound", id, err)
		}
	}
	if _, err := registry.Binding("fake.known"); err != nil {
		t.Fatalf("Binding(known) = %v", err)
	}
	if after := registry.List(); !reflect.DeepEqual(before, after) {
		t.Fatalf("registry changed by reads")
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestEveryHookFailsAsUnimplemented(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	if err := registry.Register(fixture(&calls, "fake.hooks", DirectionBoth, "hooks-src")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []HookName{HookMCP, HookSetup, HookPortal, HookSemantic, HookEvidence} {
		err := registry.ResolveHook("fake.hooks", name)
		if !errors.Is(err, ErrHookUnimplemented) {
			t.Fatalf("ResolveHook(%s) = %v, want ErrHookUnimplemented", name, err)
		}
	}
	for _, name := range []HookName{"", "telemetry", "MCP", "evidence.v1"} {
		if err := registry.ResolveHook("fake.hooks", name); !errors.Is(err, ErrUnknownHook) {
			t.Fatalf("ResolveHook(%q) = %v, want ErrUnknownHook", name, err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestTwoUnrelatedFamiliesShareAdapterSeam(t *testing.T) {
	var alphaCalls, betaCalls atomic.Int64
	alpha := fixture(&alphaCalls, "family-alpha.main", DirectionSouthbound, "alpha-owner-1")
	alpha.Adapter = NewAdapter(func(_ context.Context, c Construction) (*alphaRuntime, error) {
		alphaCalls.Add(1)
		return &alphaRuntime{fakeRuntime: fakeRuntime{family: "alpha"}, construction: c}, nil
	})
	beta := fixture(&betaCalls, "family-beta.main", DirectionNorthbound, "beta-owner-1")
	beta.Adapter = NewAdapter(func(_ context.Context, c Construction) (*betaRuntime, error) {
		betaCalls.Add(1)
		return &betaRuntime{fakeRuntime: fakeRuntime{family: "beta"}, construction: c}, nil
	})
	failing := fixture(&betaCalls, "family-beta.failing", DirectionNorthbound)
	failing.Adapter = NewAdapter(func(context.Context, Construction) (*betaRuntime, error) {
		return nil, errors.New("synthetic construction failure")
	})
	registry := NewRegistry(nil)
	if err := registry.Register(alpha, beta, failing); err != nil {
		t.Fatal(err)
	}
	if alphaCalls.Load() != 0 || betaCalls.Load() != 0 {
		t.Fatalf("registration invoked a factory")
	}
	alphaBinding, err := registry.Binding("family-alpha.main")
	if err != nil {
		t.Fatal(err)
	}
	betaBinding, err := registry.Binding("family-beta.main")
	if err != nil {
		t.Fatal(err)
	}
	alphaRun, err := alphaBinding.Adapter.Construct(context.Background(), alphaBinding.Construction)
	if err != nil {
		t.Fatal(err)
	}
	betaRun, err := betaBinding.Adapter.Construct(context.Background(), betaBinding.Construction)
	if err != nil {
		t.Fatal(err)
	}
	a, okA := alphaRun.(*alphaRuntime)
	b, okB := betaRun.(*betaRuntime)
	if !okA || !okB || a.family != "alpha" || b.family != "beta" {
		t.Fatalf("runtime types = %T, %T", alphaRun, betaRun)
	}
	if a.construction.DriverID != "family-alpha.main" || !reflect.DeepEqual(a.construction.Sources, []SourceID{"alpha-owner-1"}) ||
		b.construction.DriverID != "family-beta.main" || b.construction.Direction != DirectionNorthbound {
		t.Fatalf("construction material = %+v / %+v", a.construction, b.construction)
	}
	if alphaCalls.Load() != 1 || betaCalls.Load() != 1 {
		t.Fatalf("calls = %d/%d, want 1/1", alphaCalls.Load(), betaCalls.Load())
	}
	failingBinding, _ := registry.Binding("family-beta.failing")
	if run, err := failingBinding.Adapter.Construct(context.Background(), failingBinding.Construction); err == nil || run != nil {
		t.Fatalf("failing construction = %v, %v; want nil runtime and error", run, err)
	}
}

func TestIdentityTypesAreDistinct(t *testing.T) {
	types := map[string]reflect.Type{
		"DriverID":          reflect.TypeOf(DriverID("")),
		"SourceID":          reflect.TypeOf(SourceID("")),
		"SourceEpoch":       reflect.TypeOf(SourceEpoch("")),
		"DriverGeneration":  reflect.TypeOf(DriverGeneration(0)),
		"NativeGeneration":  reflect.TypeOf(NativeGeneration(0)),
		"DesiredRevision":   reflect.TypeOf(DesiredRevision(0)),
		"CatalogRevision":   reflect.TypeOf(CatalogRevision(0)),
		"LifecycleSequence": reflect.TypeOf(LifecycleSequence(0)),
		"HubSeq":            reflect.TypeOf(HubSeq(0)),
	}
	pkg := reflect.TypeOf(Registry{}).PkgPath()
	seen := map[reflect.Type]string{}
	for name, typ := range types {
		if typ.Name() != name || typ.PkgPath() != pkg {
			t.Fatalf("%s resolves to %s.%s; want a distinct defined type in %s", name, typ.PkgPath(), typ.Name(), pkg)
		}
		if other, dup := seen[typ]; dup {
			t.Fatalf("%s and %s are the same type", name, other)
		}
		seen[typ] = name
	}
}

func TestSnapshotTypesCarryNoControlAuthority(t *testing.T) {
	forbidden := []string{"receipt", "idempoten", "principal", "authoriz", "authority", "permission", "actor", "token", "credential"}
	visited := map[reflect.Type]bool{}
	var walk func(path string, typ reflect.Type)
	walk = func(path string, typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if visited[typ] {
			return
		}
		visited[typ] = true
		lower := strings.ToLower(typ.Name())
		for _, word := range forbidden {
			if strings.Contains(lower, word) {
				t.Fatalf("%s has forbidden type %s", path, typ)
			}
		}
		if typ.Kind() != reflect.Struct {
			return
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := strings.ToLower(field.Name)
			for _, word := range forbidden {
				if strings.Contains(name, word) {
					t.Fatalf("%s.%s is a forbidden field", path, field.Name)
				}
			}
			walk(path+"."+field.Name, field.Type)
		}
	}
	for _, typ := range []reflect.Type{
		reflect.TypeOf(Descriptor{}), reflect.TypeOf(DriverEntry{}), reflect.TypeOf(ListResult{}),
		reflect.TypeOf(GetResult{}), reflect.TypeOf(LifecycleCorrelation{}), reflect.TypeOf(Construction{}),
		reflect.TypeOf(Binding{}), reflect.TypeOf(RegistrationError{}),
	} {
		walk(typ.Name(), typ)
	}
}

var forbiddenImportTokens = []string{
	"vaillant", "growatt", "huawei", "fronius", "sunspec", "ebus", "modbus", "eebus", "matter", "tesla",
	"gree", "sma", "victron", "solaredge", "goodwe", "deye", "sofar", "kostal", "bosch", "viessmann",
	"daikin", "mitsubishi", "panasonic", "knx", "canbus", "semhub", "semreg",
}

func TestImportGraphIsProtocolNeutral(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Fatalf("go tool unavailable: %v", err)
	}
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}
	allowedInternal := map[string]bool{
		"internal/runtimecontrol": true,
		"internal/drivermanager":  true,
	}
	var internal []string
	for _, path := range strings.Fields(string(out)) {
		first := strings.SplitN(path, "/", 2)[0]
		if !strings.Contains(first, ".") {
			continue
		}
		if !strings.HasPrefix(path, modulePath+"/") {
			t.Fatalf("external dependency %s", path)
		}
		rel := strings.TrimPrefix(path, modulePath+"/")
		checkTokens(t, "dependency "+path, rel)
		if !allowedInternal[rel] {
			t.Fatalf("unexpected internal dependency %s", rel)
		}
		internal = append(internal, rel)
	}
	if len(internal) != len(allowedInternal) {
		t.Fatalf("internal dependencies = %v", internal)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		checkTokens(t, "source "+file, strings.ReplaceAll(string(data), modulePath, ""))
	}
}

func checkTokens(t *testing.T, where, text string) {
	t.Helper()
	lower := strings.ToLower(text)
	words := strings.FieldsFunc(lower, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	for _, token := range forbiddenImportTokens {
		if len(token) <= 4 {
			for _, word := range words {
				if word == token {
					t.Fatalf("%s names protocol, vendor or family %q", where, token)
				}
			}
			continue
		}
		if strings.Contains(lower, token) {
			t.Fatalf("%s names protocol, vendor or family %q", where, token)
		}
	}
}
