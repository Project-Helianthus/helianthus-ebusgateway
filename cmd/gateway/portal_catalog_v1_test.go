package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
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
