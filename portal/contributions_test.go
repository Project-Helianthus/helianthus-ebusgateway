package portal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
)

func m22Fixture() (contributionv1.Manifest, *contributionv1.StaticIndex) {
	p := contributionv1.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}
	ref := func(id string) contributionv1.DefinitionRef {
		return contributionv1.DefinitionRef{Pack: p, ID: id, Version: "1.0.0"}
	}
	field, unit, service, capability := ref("fixture.field"), ref("fixture.unit"), ref("fixture.service"), ref("fixture.capability")
	index := contributionv1.NewStaticIndex()
	index.AddPack(p)
	for _, r := range []contributionv1.DefinitionRef{field, unit, service, capability} {
		index.AddDefinition(r)
	}
	index.AddFieldRelation(field, service, capability)
	index.AddServiceCapability(service, capability)
	index.AddCanonicalUnit(field, unit)
	label := contributionv1.Label{Key: "fixture.label", Default: "Synthetic contribution"}
	m := contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: "portal.fixture", ManifestVersion: "1.0.0", Contributor: contributionv1.Contributor{DriverID: "fixture", NativeContract: contributionv1.NativeContractRef{Owner: "fixture", Contract: "fixture-native", Version: "1.0.0"}}, Requires: contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{p}}, Groups: []contributionv1.Group{{ID: "summary", Label: label, ResourceContext: "fixture.resource"}}, Fields: []contributionv1.Field{{ID: "field", Group: "summary", Label: label, Ref: field, UnitRef: unit, ServiceRef: service, CapabilityRef: capability}}, Views: []contributionv1.View{{ID: "summary", Group: "summary", Label: label, Renderer: "summary", Slot: "lens", FieldIDs: []string{"field"}, DiagnosticIDs: []string{}}}, Diagnostics: []contributionv1.Diagnostic{}, Actions: []contributionv1.Action{}}
	return m, index
}

func m22Request(t *testing.T, h http.Handler, path, body string) map[string]json.RawMessage {
	t.Helper()
	method := http.MethodGet
	if body != "" {
		method = http.MethodPost
	}
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	if res.Code != http.StatusOK {
		t.Fatalf("status %d: %s", res.Code, res.Body.String())
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(res.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func m22Catalog(t *testing.T, h http.Handler) catalogv1.Catalog {
	t.Helper()
	out := m22Request(t, h, "/graphql/portal/v1", `{"operationName":"PortalCatalogV1"}`)
	if out["errors"] != nil {
		t.Fatalf("catalog error: %s", out["errors"])
	}
	var data struct {
		Catalog catalogv1.Catalog `json:"portalCatalogV1"`
	}
	if err := json.Unmarshal(out["data"], &data); err != nil {
		t.Fatal(err)
	}
	return data.Catalog
}

func TestM22RealHandlerSixContributionsAndDetachedDescriptors(t *testing.T) {
	m, index := m22Fixture()
	registry := contributionv1.NewRegistry(index)
	for n := 5; n >= 0; n-- {
		fixture := m
		fixture.Contributor.DriverID = fmt.Sprintf("fixture.%d", n)
		if n == 5 {
			fixture.Contributor.DriverID = "fixture.sixth"
			fixture.ManifestID = "portal.fixture.sixth"
			// Detach the group slice before customizing the sixth registration.
			fixture.Groups = append([]contributionv1.Group(nil), m.Groups...)
			fixture.Groups[0].ResourceContext = "asset:fixture:sixth"
		}
		if err := RegisterContributions(registry, contributionv1.DriverGeneration{DriverID: fixture.Contributor.DriverID, Generation: 1}, []contributionv1.Manifest{fixture}); err != nil {
			t.Fatal(err)
		}
	}
	h := NewHandler(Options{Contributions: &ContributionOptions{Registry: registry, Index: index}})
	first := m22Catalog(t, h)
	if len(first.Descriptors) != 6 || len(first.Resources) != 0 {
		t.Fatalf("unexpected catalog: %+v", first)
	}
	for n, d := range first.Descriptors {
		wantDriver := fmt.Sprintf("fixture.%d", n)
		if n == 5 {
			wantDriver = "fixture.sixth"
		}
		if d.DriverID != wantDriver || d.DescriptorVersion != catalogv1.DescriptorVersion || d.Diagnostics == nil || d.Views[0].Renderer != "summary" {
			t.Fatalf("descriptor %d: %+v", n, d)
		}
		digest, err := contributionv1.CanonicalDigest(registry.Snapshot().Accepted[n].Manifest, index)
		if err != nil || d.Digest != digest {
			t.Fatal("descriptor digest is not the admitted canonical digest")
		}
	}
	first.Descriptors[0].Groups[0].Label.Default = "changed"
	first.Descriptors[0].Views[0].FieldIDs[0] = "changed"
	second := m22Catalog(t, h)
	if second.Descriptors[0].Groups[0].Label.Default != "Synthetic contribution" || second.Descriptors[0].Views[0].FieldIDs[0] != "field" {
		t.Fatal("response aliases registry")
	}
	for _, domain := range second.Domains {
		if domain.SourceState != "ABSENT" || domain.Reason != "source_unavailable" {
			t.Fatal("nil provider invented availability")
		}
	}
	bootstrap := m22Request(t, h, "/api/v1/bootstrap", "")
	var read portalReadBootstrap
	if err := json.Unmarshal(bootstrap["portal_read"], &read); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.UnavailableOperations, []string{"semanticSnapshot"}) {
		t.Fatalf("availability: %+v", read)
	}
}

func TestM22DuplicateIdentityAndEveryIdentityComponent(t *testing.T) {
	for _, field := range []string{"duplicate", "driver_id", "manifest_id", "manifest_version"} {
		t.Run(field, func(t *testing.T) {
			m, index := m22Fixture()
			registry := contributionv1.NewRegistry(index)
			other := m
			switch field {
			case "driver_id":
				other.Contributor.DriverID = "fixture.other"
			case "manifest_id":
				other.ManifestID = "portal.other"
			case "manifest_version":
				other.ManifestVersion = "1.0.1"
			}
			owner := contributionv1.DriverGeneration{DriverID: m.Contributor.DriverID, Generation: 1}
			if field == "driver_id" {
				if err := RegisterContributions(registry, owner, []contributionv1.Manifest{m}); err != nil {
					t.Fatal(err)
				}
				owner.DriverID = other.Contributor.DriverID
				if err := RegisterContributions(registry, owner, []contributionv1.Manifest{other}); err != nil {
					t.Fatal(err)
				}
			} else {
				err := RegisterContributions(registry, owner, []contributionv1.Manifest{m, other})
				if field == "duplicate" {
					if err == nil || len(registry.Snapshot().Accepted) != 0 {
						t.Fatal("duplicate admitted or partially installed")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(registry.Snapshot().Accepted) != 2 {
				t.Fatal("complete identities aliased")
			}
		})
	}
	var registry *contributionv1.Registry
	if RegisterContributions(registry, contributionv1.DriverGeneration{}, nil) == nil {
		t.Fatal("nil registry admitted")
	}
}

func TestM22BootstrapNilProvidersAndSharedReadFailure(t *testing.T) {
	bootstrap := m22Request(t, NewHandler(Options{}), "/api/v1/bootstrap", "")
	var read portalReadBootstrap
	if err := json.Unmarshal(bootstrap["portal_read"], &read); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read.UnavailableOperations, []string{"PortalCatalogV1", "semanticSnapshot"}) {
		t.Fatalf("availability: %+v", read)
	}
	m, index := m22Fixture()
	registry := contributionv1.NewRegistry(index)
	if err := RegisterContributions(registry, contributionv1.DriverGeneration{DriverID: m.Contributor.DriverID, Generation: 1}, []contributionv1.Manifest{m}); err != nil {
		t.Fatal(err)
	}
	called := false
	h := NewHandler(Options{Contributions: &ContributionOptions{Registry: registry, Index: index, Capture: func(semanticview.View) ([]catalogv1.Resource, []catalogv1.Field, error) {
		called = true
		return nil, nil, nil
	}}, SharedSemanticView: func(context.Context) (semanticview.View, error) {
		return semanticview.View{}, errors.New("synthetic read unavailable")
	}})
	out := m22Request(t, h, "/graphql/portal/v1", `{"operationName":"PortalCatalogV1"}`)
	if out["errors"] == nil || called {
		t.Fatal("failed shared read became success")
	}
}

func TestM22SharedViewUnavailableSiblingAndRegistryFence(t *testing.T) {
	m, index := m22Fixture()
	registry := contributionv1.NewRegistry(index)
	owner := contributionv1.DriverGeneration{DriverID: m.Contributor.DriverID, Generation: 1}
	if err := RegisterContributions(registry, owner, []contributionv1.Manifest{m}); err != nil {
		t.Fatal(err)
	}
	// The provider is an M08 detached unavailable entry, with no invented snapshot.
	view := semanticview.View{}
	if err := json.Unmarshal([]byte(`{"assets":[{"asset_id":"asset:fixture:unavailable","selections":[],"unavailable_reason":"semantic.read.reason.source_unavailable"}]}`), &view); err != nil {
		t.Fatal(err)
	}
	captured := false
	opts := Options{Contributions: &ContributionOptions{Registry: registry, Index: index, Capture: func(got semanticview.View) ([]catalogv1.Resource, []catalogv1.Field, error) {
		captured = true
		if !reflect.DeepEqual(got, view) {
			t.Fatal("shared unavailable entry changed")
		}
		return nil, nil, nil
	}}, SharedSemanticView: func(context.Context) (semanticview.View, error) { return view, nil }}
	result := m22Catalog(t, NewHandler(opts))
	if !captured || len(result.Descriptors) != 1 || len(result.Resources) != 0 {
		t.Fatal("unavailable asset invented data or removed descriptor")
	}
	opts.Contributions.Capture = func(semanticview.View) ([]catalogv1.Resource, []catalogv1.Field, error) {
		registry.WithdrawGeneration(owner)
		return nil, nil, nil
	}
	out := m22Request(t, NewHandler(opts), "/graphql/portal/v1", `{"operationName":"PortalCatalogV1"}`)
	if out["errors"] == nil {
		t.Fatal("registry generation changed during capture but catalog succeeded")
	}
}

func TestM22SharedReadAdvertisementRequiresMountedOperation(t *testing.T) {
	provider := func(context.Context) (semanticview.View, error) {
		t.Fatal("bootstrap must not read data")
		return semanticview.View{}, nil
	}
	for _, mounted := range []bool{false, true} {
		h := NewHandler(Options{SharedSemanticView: provider, SemanticSnapshotAvailable: mounted, GraphQLPath: "/graphql"})
		out := m22Request(t, h, "/api/v1/bootstrap", "")
		var read portalReadBootstrap
		if err := json.Unmarshal(out["portal_read"], &read); err != nil {
			t.Fatal(err)
		}
		want := []string{"PortalCatalogV1"}
		if !mounted {
			want = append(want, "semanticSnapshot")
		}
		if !reflect.DeepEqual(read.UnavailableOperations, want) {
			t.Fatalf("mounted=%v: %+v", mounted, read)
		}
		var endpoints map[string]string
		if err := json.Unmarshal(out["endpoints"], &endpoints); err != nil {
			t.Fatal(err)
		}
		if endpoints["graphql"] != "/graphql" || endpoints["portal_catalog"] != "/graphql/portal/v1" {
			t.Fatal("shared endpoints were removed")
		}
	}
}

func TestM22RealHandlerPreservesProjectionLoss(t *testing.T) {
	m, index := m22Fixture()
	registry := contributionv1.NewRegistry(index)
	if err := RegisterContributions(registry, contributionv1.DriverGeneration{DriverID: m.Contributor.DriverID, Generation: 1}, []contributionv1.Manifest{m}); err != nil {
		t.Fatal(err)
	}
	// A synthetic transport fixture exercises copying, not kernel qualification.
	var view semanticview.View
	if err := json.Unmarshal([]byte(`{"assets":[{"asset_id":"asset:fixture:loss","selections":[],"projection":{"dispositions":[{"kind":"fact","item_id":"fixture.field","outcome":"withheld","reason":"fixture.unavailable","source_keys":[],"loss":[{"kind":"omission","source_items":["fixture.field"],"description":"Synthetic loss","reversible":false}]}]}}]}`), &view); err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(view.Assets[0].Projection)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Options{SharedSemanticView: func(context.Context) (semanticview.View, error) { return view, nil }, Contributions: &ContributionOptions{Registry: registry, Index: index, Capture: func(got semanticview.View) ([]catalogv1.Resource, []catalogv1.Field, error) {
		raw, err := json.Marshal(got.Assets[0].Projection)
		return []catalogv1.Resource{{ID: m.Groups[0].ResourceContext, Domain: m.Requires.Packs[0].ID, ServiceID: m.Fields[0].ServiceRef.ID, CapabilityID: m.Fields[0].CapabilityRef.ID, ContributionDriverID: m.Contributor.DriverID, ContributionManifestID: m.ManifestID, ContributionManifestVersion: m.ManifestVersion, State: "WITHHELD", Source: catalogv1.Source{Projection: raw}}}, nil, err
	}}})
	result := m22Catalog(t, h)
	if len(result.Resources) != 1 || result.Resources[0].State != "WITHHELD" || !bytes.Equal(result.Resources[0].Source.Projection, expected) {
		t.Fatal("real handler changed loss/unavailable disposition")
	}
}
