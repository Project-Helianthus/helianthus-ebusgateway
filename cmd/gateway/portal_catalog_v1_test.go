package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type portalCatalogBarrierSource struct {
	contributions *gatewayPortalCatalogContributions
	entered       chan struct{}
	release       chan struct{}
	once          sync.Once
}

func (s *portalCatalogBarrierSource) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil, nil, nil
}
func (s *portalCatalogBarrierSource) PortalCatalogContributions() *gatewayPortalCatalogContributions {
	return s.contributions
}

func startPortalTestLease(t *testing.T, contributions *gatewayPortalCatalogContributions, manifest contributionv1.Manifest) *gatewayPortalContributionLifecycle {
	t.Helper()
	lease, err := startGatewayPortalContributionLifecycle(contributions, contributionv1.DriverGeneration{DriverID: manifest.Contributor.DriverID, Generation: 1}, []contributionv1.Manifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func startPortalTestBaseline(t *testing.T, contributions *gatewayPortalCatalogContributions) []*gatewayPortalContributionLifecycle {
	t.Helper()
	return []*gatewayPortalContributionLifecycle{
		startPortalTestLease(t, contributions, gatewayPortalPVDescriptor()),
		startPortalTestLease(t, contributions, gatewayPortalStorageDescriptor()),
		startPortalTestLease(t, contributions, gatewayPortalEVSEDescriptor()),
	}
}

func TestPortalCatalogDescriptorsUseAcceptedSemRegMetadata(t *testing.T) {
	index, err := newGatewayPortalSemanticIndex()
	if err != nil {
		t.Fatal(err)
	}
	manifests := []contributionv1.Manifest{gatewayPortalPVDescriptor(), gatewayPortalStorageDescriptor(), gatewayPortalEVSEDescriptor()}
	if len(manifests) != 3 {
		t.Fatalf("descriptor count = %d; want PV, Storage and EVSE", len(manifests))
	}
	want := map[string]struct{}{"pv.ac.frequency": {}, "storage.state.soc": {}, "evse.limit.configured_current": {}}
	for _, manifest := range manifests {
		if _, err := contributionv1.CanonicalDigest(manifest, index); err != nil {
			t.Fatalf("descriptor %s does not validate against SemReg metadata: %v", manifest.ManifestID, err)
		}
		for _, field := range manifest.Fields {
			delete(want, field.Ref.ID)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing production descriptor fields: %v", want)
	}
}

func TestPortalCatalogProductionResourcesBindExactSemanticContext(t *testing.T) {
	for _, descriptor := range []contributionv1.Manifest{gatewayPortalPVDescriptor(), gatewayPortalStorageDescriptor(), gatewayPortalEVSEDescriptor()} {
		t.Run(descriptor.ManifestID, func(t *testing.T) {
			serviceRef := semanticRef(descriptor.Fields[0].ServiceRef)
			capabilityRef := semanticRef(descriptor.Fields[0].CapabilityRef)
			snapshot := semreg.Snapshot{
				AssetID:    "asset",
				SnapshotID: "snapshot",
				Services: []semreg.ServiceInstance{{
					InstanceID: "service-instance", AssetID: "asset", Definition: serviceRef,
					BindingID: "binding", SourceEpochID: "epoch", DriverGeneration: "7",
				}},
				Capabilities: []semreg.CapabilityInstance{{
					InstanceID: "capability-instance", AssetID: "asset", Definition: capabilityRef,
					ServiceInstance: "service-instance", BindingID: "binding", SourceEpochID: "epoch", DriverGeneration: "7",
				}},
				Sources:  []semreg.SourceDescriptor{{SourceID: "source", SourceEpochID: "epoch", State: semreg.SourceCurrent}},
				Bindings: []semreg.NativeBinding{{BindingID: "binding", AssetID: "asset", SourceID: "source", SourceEpochID: "epoch", DriverGeneration: "7", State: semreg.BindingCurrent}},
			}
			resources, _ := appendPortalSnapshot(nil, nil, descriptor, snapshot, semreg.EvaluationView{}, []semreg.Selection{}, map[string]any{})
			if len(resources) != 1 || resources[0].ID != descriptor.Groups[0].ResourceContext || resources[0].Source.AssetID != "asset" || resources[0].ServiceID != descriptor.Fields[0].ServiceRef.ID || resources[0].CapabilityID != descriptor.Fields[0].CapabilityRef.ID || resources[0].Source.DriverGeneration != 7 {
				t.Fatalf("resource context=%+v", resources)
			}
			snapshot.Capabilities = nil
			resources, _ = appendPortalSnapshot(nil, nil, descriptor, snapshot, semreg.EvaluationView{}, []semreg.Selection{}, map[string]any{})
			if len(resources) != 0 {
				t.Fatalf("resource emitted without exact capability context: %+v", resources)
			}
		})
	}
}

func TestAppendPortalSnapshotUsesMatchedCurrentBindingAndEpochScalars(t *testing.T) {
	descriptor := gatewayPortalPVDescriptor()
	serviceRef := semanticRef(descriptor.Fields[0].ServiceRef)
	capabilityRef := semanticRef(descriptor.Fields[0].CapabilityRef)
	const binding = semreg.NativeBindingID("binding:current")
	const epoch = semreg.SourceEpochID("epoch:current")
	snapshot := semreg.Snapshot{
		AssetID: "asset", SnapshotID: "snapshot",
		Services:     []semreg.ServiceInstance{{InstanceID: "service:current", AssetID: "asset", Definition: serviceRef, BindingID: binding, SourceEpochID: epoch, DriverGeneration: "7"}},
		Capabilities: []semreg.CapabilityInstance{{InstanceID: "capability:current", AssetID: "asset", Definition: capabilityRef, ServiceInstance: "service:current", BindingID: binding, SourceEpochID: epoch, DriverGeneration: "7"}},
	}
	for i := 0; i < 8; i++ {
		historicalBinding := semreg.NativeBindingID(fmt.Sprintf("binding:historical:%d", i))
		historicalEpoch := semreg.SourceEpochID(fmt.Sprintf("epoch:historical:%d", i))
		snapshot.Sources = append(snapshot.Sources, semreg.SourceDescriptor{SourceID: semreg.SourceID(fmt.Sprintf("source:historical:%d", i)), SourceEpochID: historicalEpoch, State: semreg.SourceRetired})
		snapshot.Bindings = append(snapshot.Bindings, semreg.NativeBinding{BindingID: historicalBinding, AssetID: "asset", SourceEpochID: historicalEpoch, DriverGeneration: "6", State: semreg.BindingRetired})
	}
	snapshot.Sources = append(snapshot.Sources, semreg.SourceDescriptor{SourceID: "source:current", SourceEpochID: epoch, State: semreg.SourceCurrent})
	snapshot.Bindings = append(snapshot.Bindings, semreg.NativeBinding{BindingID: binding, AssetID: "asset", SourceID: "source:current", SourceEpochID: epoch, DriverGeneration: "7", State: semreg.BindingCurrent})
	resources, _ := appendPortalSnapshot(nil, nil, descriptor, snapshot, semreg.EvaluationView{}, []semreg.Selection{}, map[string]any{})
	if len(resources) != 1 {
		t.Fatalf("resources=%+v", resources)
	}
	if got := resources[0].Source; got.BindingID != string(binding) || got.SourceEpoch != string(epoch) {
		t.Fatalf("capture scalars binding=%q epoch=%q", got.BindingID, got.SourceEpoch)
	}
	if !bytes.Contains(resources[0].Source.Snapshot, []byte("binding:historical:7")) || !bytes.Contains(resources[0].Source.Snapshot, []byte("binding:current")) {
		t.Fatalf("snapshot lost nested binding evidence: %s", resources[0].Source.Snapshot)
	}
	withoutResource := func(name string, mutate func(*semreg.Snapshot)) {
		t.Helper()
		candidate := snapshot
		candidate.Sources = append([]semreg.SourceDescriptor(nil), snapshot.Sources...)
		candidate.Bindings = append([]semreg.NativeBinding(nil), snapshot.Bindings...)
		candidate.Services = append([]semreg.ServiceInstance(nil), snapshot.Services...)
		candidate.Capabilities = append([]semreg.CapabilityInstance(nil), snapshot.Capabilities...)
		mutate(&candidate)
		got, _ := appendPortalSnapshot(nil, nil, descriptor, candidate, semreg.EvaluationView{}, []semreg.Selection{}, map[string]any{})
		if len(got) != 0 {
			t.Fatalf("%s emitted resource: %+v", name, got)
		}
	}
	withoutResource("inconsistent capability epoch", func(candidate *semreg.Snapshot) { candidate.Capabilities[0].SourceEpochID = "epoch:inconsistent" })
	withoutResource("inconsistent capability generation", func(candidate *semreg.Snapshot) { candidate.Capabilities[0].DriverGeneration = "8" })
	withoutResource("missing bindings", func(candidate *semreg.Snapshot) { candidate.Bindings = nil })
	withoutResource("retired binding", func(candidate *semreg.Snapshot) {
		candidate.Bindings[len(candidate.Bindings)-1].State = semreg.BindingRetired
	})
	withoutResource("binding generation mismatch", func(candidate *semreg.Snapshot) { candidate.Bindings[len(candidate.Bindings)-1].DriverGeneration = "8" })
	withoutResource("missing sources", func(candidate *semreg.Snapshot) { candidate.Sources = nil })
	withoutResource("retired source", func(candidate *semreg.Snapshot) {
		candidate.Sources[len(candidate.Sources)-1].State = semreg.SourceRetired
	})
	withoutResource("source epoch mismatch", func(candidate *semreg.Snapshot) {
		candidate.Sources[len(candidate.Sources)-1].SourceEpochID = "epoch:inconsistent"
	})
}

func TestPortalCatalogRuntimeHandlerInstallsProductionDescriptors(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	leases := startPortalTestBaseline(t, contributions)
	defer func() {
		for _, lease := range leases {
			lease.Stop()
		}
	}()
	source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
	h := newPortalCatalogV1Handler(source)
	req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("handler status=%d body=%s", res.Code, res.Body.String())
	}
	for _, want := range []string{`"portal.pv"`, `"portal.storage"`, `"portal.evse"`} {
		if !bytes.Contains(res.Body.Bytes(), []byte(want)) {
			t.Fatalf("catalog omitted %s: %s", want, res.Body.String())
		}
	}
}

func TestPortalCatalogSourceLifecycleWithdrawsOwnedGenerations(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	leases := startPortalTestBaseline(t, contributions)
	source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
	h := newPortalCatalogV1Handler(source)
	request := func() []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("handler status=%d body=%s", res.Code, res.Body.String())
		}
		return res.Body.Bytes()
	}
	if body := request(); !bytes.Contains(body, []byte(`"portal.pv"`)) || !bytes.Contains(body, []byte(`"portal.storage"`)) || !bytes.Contains(body, []byte(`"portal.evse"`)) {
		t.Fatalf("source lifecycle did not publish all owned descriptors: %s", body)
	}
	for _, lease := range leases {
		lease.Stop()
	}
	if body := request(); bytes.Contains(body, []byte(`"portal.pv"`)) || bytes.Contains(body, []byte(`"portal.storage"`)) || bytes.Contains(body, []byte(`"portal.evse"`)) {
		t.Fatalf("source lifecycle did not withdraw its exact owned generations: %s", body)
	}
}

func TestPortalCatalogHandlerAdmitsAndWithdrawsDriverGeneration(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	leases := startPortalTestBaseline(t, contributions)
	defer func() {
		for _, lease := range leases {
			lease.Stop()
		}
	}()
	source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
	h := newPortalCatalogV1Handler(source)
	request := func() []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("handler status=%d body=%s", res.Code, res.Body.String())
		}
		return res.Body.Bytes()
	}

	extra := gatewayPortalPVDescriptor()
	extra.ManifestID = "portal.pv.secondary"
	extra.Contributor.DriverID = "pv.secondary"
	extra.Groups[0].Label.Key = "portal.pv.secondary.group"
	extra.Views[0].ID = "portal.pv.secondary.summary"
	extra.Views[0].Label.Key = "portal.pv.secondary.summary"
	owner := contributionv1.DriverGeneration{DriverID: extra.Contributor.DriverID, Generation: 1}
	if err := contributions.PublishGeneration(owner, []contributionv1.Manifest{extra}); err != nil {
		t.Fatalf("publish a valid new driver contribution: %v", err)
	}
	if body := request(); !bytes.Contains(body, []byte(`"portal.pv.secondary"`)) || !bytes.Contains(body, []byte(`"portal.storage"`)) {
		t.Fatalf("published contribution or unrelated row missing: %s", body)
	}
	if !contributions.WithdrawGeneration(owner) {
		t.Fatal("withdraw current contribution generation = false")
	}
	if body := request(); bytes.Contains(body, []byte(`"portal.pv.secondary"`)) || !bytes.Contains(body, []byte(`"portal.storage"`)) {
		t.Fatalf("withdrawal did not atomically remove only the owned row: %s", body)
	}
}

func TestPortalCatalogHandlerSnapshotsCompleteGenerationDuringReplaceAndWithdraw(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	leases := startPortalTestBaseline(t, contributions)
	defer func() {
		for _, lease := range leases {
			lease.Stop()
		}
	}()
	source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
	h := newPortalCatalogV1Handler(source)
	first := gatewayPortalPVDescriptor()
	first.Contributor.DriverID, first.ManifestID = "fixture.driver", "portal.fixture.pv"
	first.Groups[0].Label.Key, first.Views[0].ID, first.Views[0].Label.Key = "portal.fixture.pv.group", "portal.fixture.pv.summary", "portal.fixture.pv.summary"
	second := gatewayPortalStorageDescriptor()
	second.Contributor.DriverID, second.ManifestID = "fixture.driver", "portal.fixture.storage"
	second.Groups[0].Label.Key, second.Views[0].ID, second.Views[0].Label.Key = "portal.fixture.storage.group", "portal.fixture.storage.summary", "portal.fixture.storage.summary"
	request := func() []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("handler status=%d body=%s", res.Code, res.Body.String())
		}
		return res.Body.Bytes()
	}

	start := make(chan struct{})
	published := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var writerErr error
	var writerMu sync.Mutex
	go func() {
		defer close(done)
		<-start
		for generation := uint64(1); generation <= 64; generation++ {
			owner := contributionv1.DriverGeneration{DriverID: first.Contributor.DriverID, Generation: generation}
			if err := contributions.PublishGeneration(owner, []contributionv1.Manifest{first, second}); err != nil {
				writerMu.Lock()
				writerErr = err
				writerMu.Unlock()
				return
			}
			if generation == 1 {
				close(published)
				<-release
			}
			contributions.WithdrawGeneration(owner)
		}
	}()
	close(start)
	<-published
	for i := 0; i < 16; i++ {
		body := request()
		if !bytes.Contains(body, []byte(`"portal.fixture.pv"`)) || !bytes.Contains(body, []byte(`"portal.fixture.storage"`)) {
			t.Fatalf("complete published generation was partially visible: %s", body)
		}
	}
	close(release)
	for i := 0; i < 128; i++ {
		body := request()
		if bytes.Contains(body, []byte(`"portal catalog changed during detached capture"`)) {
			continue
		}
		gotFirst := bytes.Contains(body, []byte(`"portal.fixture.pv"`))
		gotSecond := bytes.Contains(body, []byte(`"portal.fixture.storage"`))
		if gotFirst != gotSecond {
			t.Fatalf("mixed complete-generation snapshot: %s", body)
		}
		if !bytes.Contains(body, []byte(`"portal.storage"`)) {
			t.Fatalf("unrelated source-owner contribution disappeared: %s", body)
		}
	}
	<-done
	writerMu.Lock()
	err = writerErr
	writerMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if body := request(); bytes.Contains(body, []byte(`"portal.fixture.pv"`)) || bytes.Contains(body, []byte(`"portal.fixture.storage"`)) {
		t.Fatalf("withdrawn complete generation remains visible: %s", body)
	}
}

func TestPortalCatalogHandlerRejectsRegistryChangeDuringCapture(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	manifest := gatewayPortalPVDescriptor()
	owner := contributionv1.DriverGeneration{DriverID: manifest.Contributor.DriverID, Generation: 1}
	if err := contributions.PublishGeneration(owner, []contributionv1.Manifest{manifest}); err != nil {
		t.Fatal(err)
	}
	source := &portalCatalogBarrierSource{contributions: contributions, entered: make(chan struct{}), release: make(chan struct{})}
	h := newPortalCatalogV1Handler(source)
	serve := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/graphql/portal/v1", bytes.NewBufferString(`{"operationName":"PortalCatalogV1","variables":{}}`))
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- serve() }()
	<-source.entered
	if err := contributions.PublishGeneration(contributionv1.DriverGeneration{DriverID: owner.DriverID, Generation: 2}, []contributionv1.Manifest{manifest}); err != nil {
		t.Fatal(err)
	}
	close(source.release)
	res := <-result
	if res.Code != http.StatusOK || !bytes.Contains(res.Body.Bytes(), []byte(`"portal catalog changed during detached capture"`)) {
		t.Fatalf("capture race response=%d body=%s", res.Code, res.Body.String())
	}
	next := serve()
	if next.Code != http.StatusOK || !bytes.Contains(next.Body.Bytes(), []byte(`"portal.pv"`)) || bytes.Contains(next.Body.Bytes(), []byte(`"portal catalog changed during detached capture"`)) {
		t.Fatalf("successor request did not capture the new complete generation: %d %s", next.Code, next.Body.String())
	}
}
