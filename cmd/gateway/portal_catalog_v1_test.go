package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	sunspectest "github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter/testfixture"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
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

// Characterization goldens for issue #1024. They were captured from the
// pre-hub publication paths of the three PublicationKernel owners and must stay
// byte-identical when those kernels move behind the semantic publication hub.
// Regenerate them only on a deliberate, reviewed change with
// HELIANTHUS_UPDATE_CHARACTERIZATION=1.

// characterizationAnchor is the one wall origin shared by every fixture so a
// single consumer instant evaluates all three families coherently.
var characterizationAnchor = time.Unix(1_800_000_000, 0).UTC()

func characterizationGolden(t *testing.T, dir, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "characterization", dir, name)
	if os.Getenv("HELIANTHUS_UPDATE_CHARACTERIZATION") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read characterization golden %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("characterization golden %s changed:\n got: %s\nwant: %s", path, got, want)
	}
}

func characterizationJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// characterizationCanonical returns the canonical SemReg bytes of the snapshot
// carried by one public {snapshot, evaluation, selections, projection} value.
func characterizationCanonical(t *testing.T, public []byte) ([]byte, semreg.Snapshot) {
	t.Helper()
	var value struct {
		Snapshot semreg.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(public, &value); err != nil {
		t.Fatal(err)
	}
	canonical, err := semreg.CanonicalJSON(value.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, value.Snapshot
}

// characterizationIdentity summarizes every identifier family the hub
// minters must reproduce: asset, source, epoch, generation, binding, service,
// capability, candidate and cursor key.
func characterizationIdentity(t *testing.T, snapshot semreg.Snapshot) []byte {
	t.Helper()
	summary := map[string][]string{"asset": {string(snapshot.AssetID)}}
	add := func(kind string, fields ...string) {
		summary[kind] = append(summary[kind], strings.Join(fields, " "))
	}
	for _, source := range snapshot.Sources {
		add("sources", string(source.SourceID), string(source.SourceEpochID), string(source.State))
	}
	for _, binding := range snapshot.Bindings {
		add("bindings", string(binding.BindingID), string(binding.SourceID), string(binding.SourceEpochID), string(binding.DriverGeneration), string(binding.State))
	}
	for _, link := range snapshot.IdentityLinks {
		add("identity_links", string(link.BindingID), string(link.State))
	}
	for _, service := range snapshot.Services {
		add("services", string(service.InstanceID), string(service.Definition.ID), string(service.BindingID))
	}
	for _, capability := range snapshot.Capabilities {
		add("capabilities", string(capability.InstanceID), string(capability.Definition.ID), string(capability.ServiceInstance), string(capability.BindingID))
	}
	for _, envelope := range snapshot.Facts {
		for _, candidate := range envelope.Candidates {
			add("candidates", string(candidate.CandidateID), string(envelope.Key.FactID), string(candidate.Origin.OriginID))
		}
	}
	for _, cursor := range snapshot.Cursors {
		add("cursors", string(cursor.SourceID), string(cursor.SourceEpochID), string(cursor.DriverGeneration), string(cursor.LastSequence))
	}
	for _, values := range summary {
		sort.Strings(values)
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

// characterizationMCPCall returns the exact JSON-RPC response body.
func characterizationMCPCall(t *testing.T, handler http.Handler, tool string, arguments string) []byte {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + tool + `","arguments":` + arguments + `}}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(body)))
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(`"isError":true`)) {
		t.Fatalf("MCP %s status=%d body=%s", tool, response.Code, response.Body.String())
	}
	return response.Body.Bytes()
}

// characterizationClock is one injected wall and read-clock pair.
type characterizationClock struct {
	mu   sync.Mutex
	wall time.Time
	read uint64
}

func (clock *characterizationClock) set(wall time.Time, read time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.wall, clock.read = wall, uint64(read)
}

func (clock *characterizationClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.wall
}

func (clock *characterizationClock) readClock() (uint64, error) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.read, nil
}

func injectTeslaCharacterizationClock(t *testing.T, owner *teslaHSCRetainedOwner, clock *characterizationClock) {
	t.Helper()
	owner.publication.SetClock(clock.now, clock.readClock)
}

// startTeslaCharacterizationOwner ingests one persistent and one provisional
// outcome on the first incarnation with an injected publication clock.
func startTeslaCharacterizationOwner(t *testing.T) (*teslaHSCRetainedOwner, *characterizationClock) {
	t.Helper()
	owner := startTeslaRetainedFixture(t, teslaRetainedConfig())
	clock := &characterizationClock{}
	injectTeslaCharacterizationClock(t, owner, clock)
	clock.set(characterizationAnchor.Add(1500*time.Millisecond), 0)
	persistent := teslaPersistentOutcome(t, 1, 16)
	persistent.Exchange.ReceiptWall = characterizationAnchor.Add(time.Second)
	if err := owner.IngestPersistent(context.Background(), persistent); err != nil {
		t.Fatal(err)
	}
	clock.set(characterizationAnchor.Add(4*time.Second), 2500*time.Millisecond)
	provisional := teslaProvisionalOutcome(t, 2, 12, 600, false, 32)
	provisional.Set.ReceiptWall = characterizationAnchor.Add(2 * time.Second)
	provisional.Readback.ReceiptWall = characterizationAnchor.Add(3 * time.Second)
	if err := owner.IngestProvisional(context.Background(), provisional); err != nil {
		t.Fatal(err)
	}
	return owner, clock
}

// characterizationPVEndpoint is an inert endpoint: SunSpec observations are
// recorded directly, so no socket identity enters the PV source hash. Its
// open, like the factory and the dial, advances the injected clock.
type characterizationPVEndpoint struct{ advance func() }

func (endpoint characterizationPVEndpoint) OpenConnection(net.Conn) (modbus.TCPConnectionHandle, error) {
	endpoint.advance()
	return modbus.TCPConnectionHandle{}, nil
}
func (characterizationPVEndpoint) EnqueueRead(modbus.TCPReadPlan) (modbus.TCPRequestHandle, error) {
	return modbus.TCPRequestHandle{}, errors.New("characterization endpoint performs no reads")
}
func (characterizationPVEndpoint) Dispatch() (modbus.TCPDispatch, bool) {
	return modbus.TCPDispatch{}, false
}
func (characterizationPVEndpoint) Write(context.Context, modbus.TCPDispatch) (modbus.OwnerTransition, error) {
	return modbus.OwnerTransition{}, errors.New("characterization endpoint performs no writes")
}
func (characterizationPVEndpoint) Read(context.Context, modbus.TCPConnectionHandle) (modbus.TCPReadBatch, error) {
	return modbus.TCPReadBatch{}, errors.New("characterization endpoint performs no reads")
}
func (characterizationPVEndpoint) Cancel(modbus.TCPRequestHandle) error { return nil }
func (characterizationPVEndpoint) Snapshot() modbus.TCPEndpointSnapshot {
	return modbus.TCPEndpointSnapshot{}
}
func (characterizationPVEndpoint) Close() error { return nil }

// characterizationSunSpecObservation replays the accepted SunSpec chain words
// into one immutable qualification observation.
func characterizationSunSpecObservation(t *testing.T, words []uint16, pollGeneration, deadlineIdentity uint64) modbusreg.SunSpecQualificationObservation {
	t.Helper()
	registry, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := modbusreg.NewSunSpecChainPlan(modbusreg.SunSpecChainPlanSpec{
		SchemaRevision: modbusreg.SunSpecModelsRevisionV1, BaseCandidates: []uint16{40000},
		Limits: modbusreg.SunSpecChainLimits{MaxTotalWords: 1024, MaxOccurrences: 32}, DecoderKeys: registry.DecoderKeys(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chain, offset := modbusreg.NewSunSpecChain(plan), 0
	var completed modbusreg.SunSpecChainSnapshot
	for id := uint64(1); ; id++ {
		requests := chain.NextRequests()
		if len(requests) != 1 {
			t.Fatalf("fixture replay requests=%d", len(requests))
		}
		request := requests[0]
		count := int(request.WordCount())
		if offset+count > len(words) {
			t.Fatal("fixture replay exceeded immutable words")
		}
		view, err := modbusreg.NewLogicalViewSnapshot(modbusreg.LogicalViewRecord{
			LogicalViewID: id, WireResponseID: id + 100, PhysicalRequestID: id + 200,
			Endpoint: "fixture", ConnectionID: 4, Transport: modbusreg.TransportTCP, TransportGeneration: 5,
			UnitID: 1, RequestedFunction: modbusreg.FunctionReadHoldingRegisters, ReceivedFunction: modbusreg.FunctionReadHoldingRegisters,
			Table: modbusreg.HoldingRegisters, PhysicalOffset: request.Address(), PhysicalWordCount: request.WordCount(),
			AuthorizationScope: "fixture:characterization", PollGeneration: pollGeneration, DeadlineIdentity: deadlineIdentity,
			LogicalOffset: request.Address(), LogicalWordCount: request.WordCount(), SliceOffset: 0, SliceWordCount: request.WordCount(),
			Words: append([]uint16(nil), words[offset:offset+count]...), WireResponseBytes: []byte{byte(id)},
		})
		if err != nil {
			t.Fatal(err)
		}
		offset += count
		completed, err = chain.AdmitReplay(request, view)
		if err != nil {
			t.Fatal(err)
		}
		if len(completed.RawWords()) != 0 {
			break
		}
	}
	observation, err := modbusreg.NewSunSpecQualificationObservation(registry, completed)
	if err != nil {
		t.Fatal(err)
	}
	return observation
}

type characterizationFamilies struct {
	pv        *modbusadapter.Adapter
	pvProfile string
	pvSample  string
	pvAsset   string
	growatt   *growattBMSRS485ProductionProvider
	tesla     *teslaHSCRetainedOwner
	clock     *characterizationClock
}

// startCharacterizationFamilies composes the production PV, Growatt and Tesla
// paths with deterministic native fixtures and injected clocks.
func startCharacterizationFamilies(t *testing.T) characterizationFamilies {
	t.Helper()
	config, err := mapModbusRuntimeConfig(ebusgateway.ModbusTCPConfig{Enabled: true, Endpoint: "tcp://127.0.0.1:1502", DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var pvMu sync.Mutex
	pvNow := characterizationAnchor.Add(-30 * time.Second)
	advance := func() {
		pvMu.Lock()
		pvNow = pvNow.Add(time.Second)
		pvMu.Unlock()
	}
	setPV := func(at time.Time) {
		pvMu.Lock()
		pvNow = at
		pvMu.Unlock()
	}
	config.Clock = func() time.Time {
		pvMu.Lock()
		defer pvMu.Unlock()
		return pvNow
	}
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	config.SemanticPV = newTestSemanticPVClass(t)
	adapter, err := modbusadapter.Start(context.Background(), config,
		func(context.Context, string, string) (net.Conn, error) { advance(); return client, nil },
		func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
			advance()
			return characterizationPVEndpoint{advance: advance}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	words := sunspectest.ObservedFroniusFloatControlsWords()
	sunspectest.SetFloat(words, 20, 1_234.5)
	sunspectest.SetFloat(words, 22, 50)
	setPV(characterizationAnchor.Add(-20 * time.Second))
	qualified := characterizationSunSpecObservation(t, words, 701, 801)
	if err := adapter.RecordSunSpecQualificationObservation(qualified); err != nil {
		t.Fatal(err)
	}
	setPV(characterizationAnchor.Add(5 * time.Second))
	refreshed := append([]uint16(nil), words...)
	sunspectest.SetFloat(refreshed, 20, 4_321.5)
	if err := adapter.RecordSunSpecCurrentObservation(characterizationSunSpecObservation(t, refreshed, 702, 802)); err != nil {
		t.Fatal(err)
	}
	setPV(characterizationAnchor.Add(10 * time.Second))
	current, ok := adapter.SemanticPVCurrentSingle()
	if !ok {
		t.Fatal("PV publication unavailable")
	}
	growatt := startGrowattRuntimeWithFake(t, growattProductionConfig(), &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 4})
	if _, err := growatt.GrowattStorageSemanticCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	tesla, clock := startTeslaCharacterizationOwner(t)
	return characterizationFamilies{pv: adapter, pvProfile: qualified.Capability().ProfileID(), pvSample: qualified.SampleID(), pvAsset: string(current.Snapshot.AssetID), growatt: growatt, tesla: tesla, clock: clock}
}

// characterizationFixedSource evaluates every capture at one fixed instant,
// because the Matter feed captures at the process wall clock.
type characterizationFixedSource struct {
	source catalogv1.SourceCapture
	at     time.Time
}

func (s characterizationFixedSource) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	return s.source.Capture(s.at)
}

func (s characterizationFixedSource) PortalCatalogContributions() *gatewayPortalCatalogContributions {
	return s.source.(gatewayPortalCatalogContributionOwner).PortalCatalogContributions()
}

func TestCharacterizationConsumersOfAllThreeFamilies(t *testing.T) {
	families := startCharacterizationFamilies(t)
	at := characterizationAnchor.Add(10 * time.Second)
	families.clock.set(at, 9*time.Second)
	const dir = "consumers"

	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mcp.RegisterModbusV1Tools(server, newGatewayModbusMCPProviderWithRuntimes(families.pv, families.growatt, families.tesla))
	characterizationGolden(t, dir, "mcp_pv.json", characterizationMCPCall(t, server.Handler(), mcp.SemanticV1PVCurrentGetTool, `{"profile_id":"`+families.pvProfile+`","sample_id":"`+families.pvSample+`"}`))
	characterizationGolden(t, dir, "mcp_storage.json", characterizationMCPCall(t, server.Handler(), mcp.SemanticV1GrowattStorageCurrentGetTool, `{}`))
	characterizationGolden(t, dir, "mcp_evse.json", characterizationMCPCall(t, server.Handler(), mcp.SemanticV1EVSECurrentGetTool, `{}`))

	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	pvLease, err := startGatewayPortalPVContribution(families.pv, contributions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pvLease.Stop)
	if err := attachGatewayPortalStorageContribution(families.growatt, contributions); err != nil {
		t.Fatal(err)
	}
	if err := attachGatewayPortalEVSEContribution(families.tesla, contributions); err != nil {
		t.Fatal(err)
	}
	source := newGatewayPortalCatalogSource(families.pv, gatewayPortalStoragePublication(families.growatt), families.tesla, contributions)
	resources, fields, err := source.Capture(at)
	if err != nil {
		t.Fatal(err)
	}
	characterizationGolden(t, dir, "portal_source_capture.json", characterizationJSON(t, map[string]any{"resources": resources, "fields": fields}))
	composer, err := contributions.composer(source)
	if err != nil {
		t.Fatal(err)
	}
	composer.SetClock(func() time.Time { return at })
	catalog, err := composer.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	characterizationGolden(t, dir, "portal_catalog.json", characterizationJSON(t, catalog))

	rendered := ebusgateway.RenderSemanticPrometheus(semanticPrometheusDomains(families.pv, families.growatt, families.tesla, true, true, true, string(families.growatt.storage.assetID), at), at)
	var metrics []string
	for _, line := range strings.Split(rendered, "\n") {
		if strings.Contains(line, "helianthus_semantic_") {
			metrics = append(metrics, line)
		}
	}
	characterizationGolden(t, dir, "prometheus_semantic.txt", []byte(strings.Join(metrics, "\n")+"\n"))

	feed, err := newGatewayMatterBindingFeedProvider(characterizationFixedSource{source: source, at: at}, []string{families.pvAsset, string(families.growatt.storage.assetID), families.tesla.cfg.AssetID})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := feed.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	characterizationGolden(t, dir, "matter_feed.json", characterizationJSON(t, snapshot))
}
