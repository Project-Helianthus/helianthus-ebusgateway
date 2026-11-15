//go:build !nomatter

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/m2mgraphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/matter/feedv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
)

func TestGatewayMatterBindingFeedProviderEmptyCatalogIsExactAndFailClosed(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	source := newGatewayPortalCatalogSource(nil, nil, nil, contributions)
	provider, err := newGatewayMatterBindingFeedProvider(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := provider.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	requested, dispositions := matter.Ledger()
	if len(snapshot.Resources) != 0 || len(snapshot.Operations) != 0 || len(snapshot.Ledger.Requested) != len(requested) || len(snapshot.Ledger.Dispositions) != len(dispositions) {
		t.Fatalf("unexpected empty feed snapshot: %+v", snapshot)
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("feed snapshot validation: %v", err)
	}
	if _, err := provider.Invoke(context.Background(), feedv1.Invocation{}); !errors.Is(err, errMatterBindingOperationUnavailable) {
		t.Fatalf("unadmitted invocation error=%v", err)
	}
}

type matterFeedCatalogSource struct {
	contributions *gatewayPortalCatalogContributions
	resources     []catalogv1.Resource
}

func (s *matterFeedCatalogSource) PortalCatalogContributions() *gatewayPortalCatalogContributions {
	return s.contributions
}
func (s *matterFeedCatalogSource) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	return append([]catalogv1.Resource(nil), s.resources...), []catalogv1.Field{}, nil
}

func TestGatewayMatterBindingFeedProviderAppliesM2MAssetAllowlistBeforeProjection(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	allowedDescriptor := gatewayPortalEVSEDescriptor()
	blockedDescriptor := gatewayPortalEVSEDescriptor()
	blockedDescriptor.Contributor.DriverID = "evse.blocked"
	blockedDescriptor.ManifestID = "portal.evse.blocked"
	blockedDescriptor.Groups[0].ID = "blocked"
	blockedDescriptor.Groups[0].ResourceContext = "evse-blocked"
	blockedDescriptor.Groups[0].Label.Key = "portal.evse.blocked.group"
	blockedDescriptor.Fields[0].Group = "blocked"
	blockedDescriptor.Views[0].ID = "portal.evse.blocked.summary"
	blockedDescriptor.Views[0].Group = "blocked"
	blockedDescriptor.Views[0].Label.Key = "portal.evse.blocked.summary"
	for _, item := range []struct {
		owner      contributionv1.DriverGeneration
		descriptor contributionv1.Manifest
	}{
		{contributionv1.DriverGeneration{DriverID: allowedDescriptor.Contributor.DriverID, Generation: 1}, allowedDescriptor},
		{contributionv1.DriverGeneration{DriverID: blockedDescriptor.Contributor.DriverID, Generation: 1}, blockedDescriptor},
	} {
		if err := contributions.PublishGeneration(item.owner, []contributionv1.Manifest{item.descriptor}); err != nil {
			t.Fatal(err)
		}
	}
	allowed := matterFeedCatalogResource(t, "asset:allowed", "allowed", allowedDescriptor)
	blocked := matterFeedCatalogResource(t, "asset:blocked", "blocked", blockedDescriptor)
	if _, _, err := projectMatterFeedResource(allowed); err != nil {
		t.Fatalf("direct allowed projection: %v", err)
	}
	source := &matterFeedCatalogSource{contributions: contributions, resources: []catalogv1.Resource{blocked, allowed}}
	provider, err := newGatewayMatterBindingFeedProvider(source, []string{"asset:allowed"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := provider.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Resources) != 1 || snapshot.Resources[0].AssetID != "asset:allowed" || len(snapshot.Operations) != 0 {
		t.Fatalf("allowlisted snapshot leaked another asset: %+v", snapshot)
	}
	server, err := feedv1.New(provider, feedv1.Options{InstanceID: "allowlist-test", ReplayLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, "/v1/snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte("asset:blocked")) {
		t.Fatalf("wire snapshot leaked disallowed asset: %d %s", response.Code, response.Body.String())
	}
	if _, err := provider.Invoke(context.Background(), feedv1.Invocation{Intent: feedv1.Intent{AssetID: "asset:blocked"}}); !errors.Is(err, errMatterBindingOperationUnavailable) {
		t.Fatalf("disallowed invocation error=%v", err)
	}
}

func matterFeedCatalogResource(t *testing.T, asset, tag string, descriptor contributionv1.Manifest) catalogv1.Resource {
	t.Helper()
	snapshot, evaluation := matterFeedSemanticFixture(t, asset, tag)
	snapshotRaw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	evaluationRaw, err := json.Marshal(evaluation)
	if err != nil {
		t.Fatal(err)
	}
	revisions, err := json.Marshal(snapshot.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	return catalogv1.Resource{
		ID: descriptor.Groups[0].ResourceContext, Domain: descriptor.Requires.Packs[0].ID,
		ServiceID: descriptor.Fields[0].ServiceRef.ID, CapabilityID: descriptor.Fields[0].CapabilityRef.ID,
		ContributionDriverID: descriptor.Contributor.DriverID, ContributionManifestID: descriptor.ManifestID,
		ContributionManifestVersion: descriptor.ManifestVersion, State: feedv1.ResourceCurrent,
		Source: catalogv1.Source{AssetID: asset, SnapshotID: string(snapshot.SnapshotID), Revision: string(revisions), BindingID: "binding:" + tag, SourceEpoch: "epoch:" + tag, DriverGeneration: 1, Snapshot: snapshotRaw, Evaluation: evaluationRaw, Selections: json.RawMessage(`[]`), Projection: json.RawMessage(`{}`)},
	}
}

func matterFeedSemanticFixture(t *testing.T, asset, tag string) (semreg.Snapshot, semreg.EvaluationView) {
	t.Helper()
	kernel, err := semreg.NewPublicationKernel(semreg.AssetID(asset), evse.New())
	if err != nil {
		t.Fatal(err)
	}
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("synthetic public feed evidence")))), Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	point := semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.utc", UncertaintyNS: "0"}
	bindingID := semreg.NativeBindingID("binding:" + tag)
	epochID := semreg.SourceEpochID("epoch:" + tag)
	generation := semreg.Uint64("1")
	sourceID := semreg.SourceID("source:" + tag)
	phase := "l1"
	value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "32", Exponent10: -1}, Unit: "unit.ampere"}}
	candidate := semreg.FactCandidate{
		CandidateID:     semreg.CandidateID("candidate:" + tag),
		Key:             semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: "evse.ac.current", Dimensions: []semreg.Dimension{{ID: "evse.dimension.phase", Value: semreg.Value{Kind: semreg.ValueText, Text: &phase}}}},
		Value:           &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: point, ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "100"}, EvaluatedAt: point, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "100"}},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy.matter.current", Version: "1.0.0", FreshForNS: "1000", RetainForNS: "2000", MaxWallUncertaintyNS: "0"},
		BindingID:       &bindingID, SourceEpochID: &epochID, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:" + tag), Kind: semreg.OriginNativeObservation, SourceID: &sourceID, SourceEpochID: &epochID, BindingID: &bindingID, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: "1",
	}
	batch := semreg.PublicationBatch{
		Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:" + tag), AssetID: semreg.AssetID(asset),
		SourceID: semreg.SourceID("source:" + tag), SourceEpochID: semreg.SourceEpochID("epoch:" + tag), DriverGeneration: "1", Sequence: "1", ExpectedSemanticRevision: "0", ObservedAt: point,
		SourceUpserts:       []semreg.SourceDescriptor{{SourceID: semreg.SourceID("source:" + tag), SourceEpochID: semreg.SourceEpochID("epoch:" + tag), ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: point, State: semreg.SourceCurrent, Revision: "1"}},
		SourceRetirements:   []semreg.SourceEpochID{},
		BindingUpserts:      []semreg.NativeBinding{{BindingID: semreg.NativeBindingID("binding:" + tag), AssetID: semreg.AssetID(asset), SourceID: semreg.SourceID("source:" + tag), SourceEpochID: semreg.SourceEpochID("epoch:" + tag), DriverGeneration: "1", NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"}},
		IdentityLinkUpserts: []semreg.IdentityLink{{AssetID: semreg.AssetID(asset), BindingID: semreg.NativeBindingID("binding:" + tag), State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}},
		FactUpserts:         []semreg.FactCandidate{candidate}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{},
	}
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := kernel.Apply(batch, semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "100"})
	if err != nil {
		t.Fatal(err)
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "150", ClockID: "clock.utc", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "150"}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, evaluation
}

type matterFeedRuntimeFixture struct{ snapshot feedv1.Snapshot }

func (f matterFeedRuntimeFixture) Capture(context.Context) (feedv1.Snapshot, error) {
	return f.snapshot, nil
}
func (matterFeedRuntimeFixture) Invoke(context.Context, feedv1.Invocation) (feedv1.Execution, error) {
	return feedv1.Execution{}, errors.New("not admitted")
}

func TestM2MRuntimeMountsMatterFeedBehindServerTLSWithOptionalClientCertificate(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"asset:test"},
	}}
	provider := matterFeedRuntimeFixture{snapshot: matterFeedRuntimeEmptySnapshot()}
	runtime, err := newM2MGraphQLRuntimeWithMatter(cfg, nil, nil, nil, provider)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil {
		t.Fatal("enabled M2M runtime is nil")
	}
	t.Cleanup(func() { _ = runtime.Close() })

	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: certs.pool, ServerName: "m2m.gateway.test", MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certs.goodClient},
	}}}
	response, err := client.Get("https://" + runtime.Addr() + "/matter-binding/v1/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("feed status=%d body=%s", response.StatusCode, body)
	}
	var decoded feedv1.SnapshotResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Snapshot.Contract != feedv1.Contract || decoded.Cursor.Sequence != 1 || decoded.Cursor.Instance == "" {
		t.Fatalf("unexpected feed response: %+v", decoded)
	}

	withoutCertificate := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: certs.pool, ServerName: "m2m.gateway.test", MinVersion: tls.VersionTLS13,
	}}}
	plain, err := withoutCertificate.Get("https://" + runtime.Addr() + "/matter-binding/v1/snapshot")
	if err != nil {
		t.Fatalf("feed rejected a client without a certificate: %v", err)
	}
	defer func() { _ = plain.Body.Close() }()
	if plain.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(plain.Body)
		t.Fatalf("certificate-less feed status=%d body=%s", plain.StatusCode, body)
	}
	// Both snapshots spent two tokens; invoke can spend the third token
	// from the same host without waiting for refill.
	invoke, err := withoutCertificate.Post("https://"+runtime.Addr()+"/matter-binding/v1/invoke", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = invoke.Body.Close() }()
	invokeBody, _ := io.ReadAll(invoke.Body)
	if invoke.StatusCode == http.StatusUnauthorized || bytes.Contains(invokeBody, []byte("principal_required")) || invoke.StatusCode != http.StatusBadRequest {
		t.Fatalf("certificate-less invoke status=%d body=%s; want invalid_intent, not principal_required", invoke.StatusCode, invokeBody)
	}
	mismatched := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: certs.pool, ServerName: "other.gateway.test", MinVersion: tls.VersionTLS13,
	}}}
	if response, err := mismatched.Get("https://" + runtime.Addr() + "/matter-binding/v1/snapshot"); err == nil {
		_ = response.Body.Close()
		t.Fatal("feed client accepted a server certificate for a different identity")
	}
}

func matterFeedRuntimeEmptySnapshot() feedv1.Snapshot {
	requested, dispositions := matter.Ledger()
	return feedv1.Snapshot{
		Contract: feedv1.Contract,
		Target: feedv1.Target{
			ProjectionContract: matter.Contract,
			TargetID:           string(matter.TargetID),
			TargetVersion:      string(matter.TargetVersion),
			MappingRevision:    string(matter.MappingRevision),
		},
		Ledger:    feedv1.Ledger{Manifest: matter.Manifest(), Requested: requested, Dispositions: dispositions},
		Resources: []feedv1.Resource{}, Operations: []feedv1.AdmittedOperation{},
	}
}

// blockingMatterFeedProvider holds its first capture until released.
type blockingMatterFeedProvider struct {
	matterFeedRuntimeFixture
	once    *sync.Once
	entered chan struct{}
	release chan struct{}
}

func (p blockingMatterFeedProvider) Capture(ctx context.Context) (feedv1.Snapshot, error) {
	p.once.Do(func() {
		close(p.entered)
		<-p.release
	})
	return p.matterFeedRuntimeFixture.Capture(ctx)
}

func TestM2MRuntimeMatterRoutesShareThePerHostRequestBudget(t *testing.T) {
	certs := newM2MTLSCertificates(t)
	cfg := ebusgateway.Config{M2MGraphQL: ebusgateway.M2MGraphQLConfig{
		ListenAddr: "127.0.0.1:0", ServerName: "m2m.gateway.test",
		ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
		AllowedAssets: []string{"pv-asset-known"},
	}}
	provider := blockingMatterFeedProvider{
		matterFeedRuntimeFixture: matterFeedRuntimeFixture{snapshot: matterFeedRuntimeEmptySnapshot()},
		once:                     &sync.Once{}, entered: make(chan struct{}), release: make(chan struct{}),
	}
	runtime, err := newM2MGraphQLRuntimeWithMatter(cfg, nil, nil, nil, provider)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	released := false
	t.Cleanup(func() {
		if !released {
			close(provider.release)
		}
	})
	// Drive the composed listener handler directly so distinct remote hosts
	// can be presented; the composition is the one served over TLS.
	serve := func(method, path, remote, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		request.RemoteAddr = remote
		response := httptest.NewRecorder()
		runtime.server.Handler.ServeHTTP(response, request)
		return response
	}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- serve(http.MethodGet, "/matter-binding/v1/snapshot", "198.51.100.7:40001", "") }()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Matter capture did not start")
	}
	// A refusal is immediate; without admission the Matter request would wait
	// on the feed's serialized capture, so bound each call.
	serveRefused := func(method, path, remote, body string) *httptest.ResponseRecorder {
		t.Helper()
		result := make(chan *httptest.ResponseRecorder, 1)
		go func() { result <- serve(method, path, remote, body) }()
		select {
		case response := <-result:
			return response
		case <-time.After(2 * time.Second):
			t.Fatalf("concurrent same-host request to %s was not refused; it reached the serialized capture", path)
			return nil
		}
	}
	matterRefusal := serveRefused(http.MethodGet, "/matter-binding/v1/changes?cursor=x:1", "198.51.100.7:40002", "")
	graphqlBody := `{"operationName":"SemanticPVCurrent","query":"query SemanticPVCurrent($request: m2m_current_snapshot_request!) { semantic_pv_current(request: $request) { snapshot evaluation selections projection } }","variables":{"request":{"contract_id":"PUBLIC_GRAPHQL_SEMANTIC_PV_V1","asset_ref":"pv-asset-known"}}}`
	graphqlRefusal := serveRefused(http.MethodPost, "/graphql/m2m/v1", "198.51.100.7:40003", graphqlBody)
	if matterRefusal.Code != http.StatusServiceUnavailable || strings.TrimSpace(matterRefusal.Body.String()) != `{"contract":"`+feedv1.Contract+`","error":"unavailable"}` {
		t.Fatalf("concurrent same-host Matter refusal=%d %s; want the feed's unavailable error", matterRefusal.Code, matterRefusal.Body.String())
	}
	if graphqlRefusal.Code != http.StatusOK || !strings.Contains(graphqlRefusal.Body.String(), `"REQUEST_LIMIT_EXCEEDED"`) || !strings.Contains(graphqlRefusal.Body.String(), `"path":[]`) {
		t.Fatalf("GraphQL refusal changed: %d %s", graphqlRefusal.Code, graphqlRefusal.Body.String())
	}
	// A different host has its own budget: it is admitted and then waits for
	// the feed's serialized capture rather than being refused.
	other := make(chan *httptest.ResponseRecorder, 1)
	go func() { other <- serve(http.MethodGet, "/matter-binding/v1/snapshot", "198.51.100.8:40001", "") }()
	select {
	case response := <-other:
		t.Fatalf("different host answered before the capture was released: %d %s", response.Code, response.Body.String())
	case <-time.After(100 * time.Millisecond):
	}
	close(provider.release)
	released = true
	for name, channel := range map[string]chan *httptest.ResponseRecorder{"first": first, "different-host": other} {
		select {
		case response := <-channel:
			if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "REQUEST_LIMIT_EXCEEDED") {
				t.Fatalf("%s Matter request=%d %s", name, response.Code, response.Body.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Matter request did not complete", name)
		}
	}
}

func TestMatterProfileCharacterizationFeed(t *testing.T) {
	families := startCharacterizationFamilies(t)
	at := characterizationAnchor.Add(10 * time.Second)
	families.clock.set(at, 9*time.Second)
	const dir = "consumers"
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := startGatewayPortalPVContribution(families.pv, contributions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Stop)
	if err := attachGatewayPortalStorageContribution(families.growatt, contributions); err != nil {
		t.Fatal(err)
	}
	if err := attachGatewayPortalEVSEContribution(families.tesla, contributions); err != nil {
		t.Fatal(err)
	}
	source := newGatewayPortalCatalogSource(families.pv, gatewayPortalStoragePublication(families.growatt), families.tesla, contributions)
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

// With the publication hub installed as the driver resolver, the families'
// runtime drivers still attribute each single-source resource to its
// descriptor, so the feed keeps every projection, the EVSE one included.
func TestMatterBindingFeedKeepsSingleSourceFamiliesWithTheHubResolver(t *testing.T) {
	hub, families, source, at := startSharedHubPortalSource(t)
	feed, err := newGatewayMatterBindingFeedProvider(characterizationFixedSource{source: source, at: at}, []string{families.pvAsset, string(families.growatt.storage.assetID), families.tesla.cfg.AssetID})
	if err != nil {
		t.Fatal(err)
	}
	before, err := feed.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	source.contributions.UseSourceDrivers(hub)
	after, err := feed.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := characterizationJSON(t, after), characterizationJSON(t, before); !bytes.Equal(got, want) {
		t.Fatalf("the hub resolver changed the feed:\n%s\nwant\n%s", got, want)
	}
	evse := false
	for _, resource := range after.Resources {
		evse = evse || resource.AssetID == families.tesla.cfg.AssetID
	}
	if !evse {
		t.Fatalf("feed resources=%+v; want the EVSE projection", after.Resources)
	}
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

func TestMatterProfileRoutesUseSharedBudgetOffline(t *testing.T) {
	graphqlHandler, err := m2mgraphql.NewHandler(m2mgraphql.Config{AllowedAssets: map[string]struct{}{"asset:synthetic": {}}, SemanticPVCurrent: func(context.Context, string) (json.RawMessage, bool) { return nil, false }})
	if err != nil {
		t.Fatal(err)
	}
	provider := blockingMatterFeedProvider{
		matterFeedRuntimeFixture: matterFeedRuntimeFixture{snapshot: matterFeedRuntimeEmptySnapshot()},
		once:                     &sync.Once{}, entered: make(chan struct{}), release: make(chan struct{}),
	}
	handler, err := matterProfileRoutes(graphqlHandler, provider)
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(provider.release) }) })
	serve := func(method, path, body string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		handler.ServeHTTP(response, request)
		return response
	}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- serve(http.MethodGet, "/matter-binding/v1/snapshot", "") }()
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("feed route did not capture")
	}
	// Both routes must refuse before the feed's serialized capture is entered.
	for _, route := range []struct {
		method, path, body string
		status             int
		contains           string
	}{
		{http.MethodGet, "/matter-binding/v1/snapshot", "", http.StatusServiceUnavailable, `"error":"unavailable"`},
		{http.MethodPost, "/graphql/m2m/v1", `{}`, http.StatusOK, `"REQUEST_LIMIT_EXCEEDED"`},
	} {
		result := make(chan *httptest.ResponseRecorder, 1)
		go func() { result <- serve(route.method, route.path, route.body) }()
		select {
		case response := <-result:
			if response.Code != route.status || !strings.Contains(response.Body.String(), route.contains) {
				t.Fatalf("route %s: status=%d body=%s", route.path, response.Code, response.Body.String())
			}
		case <-time.After(time.Second):
			t.Fatalf("route %s bypassed the shared budget", route.path)
		}
	}
	release.Do(func() { close(provider.release) })
	select {
	case response := <-first:
		if response.Code != http.StatusOK {
			t.Fatalf("snapshot status=%d body=%s", response.Code, response.Body.String())
		}
		var decoded feedv1.SnapshotResponse
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		want, _ := json.Marshal(provider.snapshot)
		got, _ := json.Marshal(decoded.Snapshot)
		if !bytes.Equal(got, want) {
			t.Fatal("snapshot changed during route composition")
		}
	case <-time.After(time.Second):
		t.Fatal("feed route did not release")
	}
}

// matterSharedAssetSource captures one asset with two sources through the
// Portal's own selection, so the feed reads exactly what the catalog keeps.
type matterSharedAssetSource struct {
	contributions *gatewayPortalCatalogContributions
	attribution   portalAttribution
	snapshot      semreg.Snapshot
	evaluation    semreg.EvaluationView
}

func (s *matterSharedAssetSource) PortalCatalogContributions() *gatewayPortalCatalogContributions {
	return s.contributions
}

func (s *matterSharedAssetSource) Capture(time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	resources, fields, _ := portalSnapshotCapture(s.attribution, gatewayPortalEVSEDescriptor(), s.snapshot, s.evaluation, []semreg.Selection{}, map[string]any{})
	return resources, fields, nil
}

// matterSharedAssetFixture publishes the mapped current, the EVSE service and
// its read capability from the owner, and the same service and capability plus
// a short-lived voltage from a contributor, then evaluates after the voltage
// has gone stale.
func matterSharedAssetFixture(t *testing.T) (semreg.Snapshot, semreg.EvaluationView) {
	t.Helper()
	const asset = semreg.AssetID("asset:shared")
	kernel, err := semreg.NewPublicationKernel(asset, evse.New())
	if err != nil {
		t.Fatal(err)
	}
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("synthetic public shared asset evidence")))), Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	point := semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.utc", UncertaintyNS: "0"}
	monotonic := semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "100"}
	pack := catalogv1.FivePacks[3]
	definition := func(id string) semreg.DefinitionRef {
		return semreg.DefinitionRef{Pack: semreg.PackRef{ID: semreg.DefinitionID(pack.ID), Version: semreg.SemanticVersion(pack.Version)}, ID: semreg.DefinitionID(id), Version: semreg.SemanticVersion(pack.Version)}
	}
	candidate := func(tag string, generation semreg.Uint64, fact, dimension, dimensionValue, coefficient, unit, freshFor string) semreg.FactCandidate {
		binding, epoch, source := semreg.NativeBindingID("binding:"+tag), semreg.SourceEpochID("epoch:"+tag), semreg.SourceID("source:"+tag)
		value := semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: -1}, Unit: semreg.DefinitionID(unit)}}
		return semreg.FactCandidate{
			CandidateID:     semreg.CandidateID("candidate:" + tag + ":" + fact),
			Key:             semreg.FactKey{PackID: "helianthus.pack.evse", PackVersion: "1.0.0", FactID: semreg.DefinitionID(fact), Dimensions: []semreg.Dimension{{ID: semreg.DefinitionID(dimension), Value: semreg.Value{Kind: semreg.ValueText, Text: &dimensionValue}}}},
			Value:           &value,
			Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
			Times:           semreg.Times{ReceivedAt: point, ReceiptMonotonic: monotonic, EvaluatedAt: point, EvaluateMonotonic: monotonic},
			FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy.matter.current", Version: "1.0.0", FreshForNS: semreg.Uint64(freshFor), RetainForNS: "2000", MaxWallUncertaintyNS: "0"},
			BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
			Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:" + tag + ":" + fact), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
			Evidence: []semreg.EvidenceRef{evidence}, Revision: "1",
		}
	}
	expected := semreg.Uint64("0")
	var snapshot semreg.Snapshot
	for _, item := range []struct {
		tag        string
		generation semreg.Uint64
		facts      []semreg.FactCandidate
	}{
		{"owner", "3", []semreg.FactCandidate{candidate("owner", "3", "evse.ac.current", "evse.dimension.phase", "l1", "32", "unit.ampere", "1000")}},
		{"contributor", "5", []semreg.FactCandidate{candidate("contributor", "5", "evse.ac.voltage", "evse.dimension.phase", "l1", "2301", "unit.volt", "10")}},
	} {
		binding, epoch, source := semreg.NativeBindingID("binding:"+item.tag), semreg.SourceEpochID("epoch:"+item.tag), semreg.SourceID("source:"+item.tag)
		service := semreg.ServiceInstanceID("service:" + item.tag)
		batch := semreg.PublicationBatch{
			Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:" + item.tag), AssetID: asset,
			SourceID: source, SourceEpochID: epoch, DriverGeneration: item.generation, Sequence: "1", ExpectedSemanticRevision: expected, ObservedAt: point,
			SourceUpserts:         []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "1", RegistryEvidence: evidence, StartedAt: point, State: semreg.SourceCurrent, Revision: "1"}},
			SourceRetirements:     []semreg.SourceEpochID{},
			BindingUpserts:        []semreg.NativeBinding{{BindingID: binding, AssetID: asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: item.generation, NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1"}},
			IdentityLinkUpserts:   []semreg.IdentityLink{{AssetID: asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"}},
			FactUpserts:           item.facts,
			FactWithdrawals:       []semreg.CandidateID{},
			ServiceUpserts:        []semreg.ServiceInstance{{InstanceID: service, AssetID: asset, Definition: definition("evse.service.evse"), BindingID: binding, SourceEpochID: epoch, DriverGeneration: item.generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}},
			ServiceWithdrawals:    []semreg.ServiceInstanceID{},
			CapabilityUpserts:     []semreg.CapabilityInstance{{InstanceID: semreg.CapabilityInstanceID("capability:" + item.tag), AssetID: asset, ServiceInstance: service, Definition: definition("evse.capability.read.evse"), BindingID: binding, SourceEpochID: epoch, DriverGeneration: item.generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{evidence}, Revision: "1"}},
			CapabilityWithdrawals: []semreg.CapabilityInstanceID{},
			GenerationFences:      []semreg.GenerationFence{},
		}
		batch.BatchDigest, err = batch.ComputedDigest()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, _, err = kernel.Apply(batch, monotonic)
		if err != nil {
			t.Fatalf("batch %s: %v", item.tag, err)
		}
		expected = snapshot.Revisions.Semantic
	}
	evaluation, err := semreg.EvaluateSnapshot(snapshot, semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "150", ClockID: "clock.utc", UncertaintyNS: "0"}, EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "150"}})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, evaluation
}

func TestMatterBindingFeedKeepsSharedAssetResource(t *testing.T) {
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	lease := startPortalTestLease(t, contributions, gatewayPortalEVSEDescriptor())
	defer lease.Stop()
	snapshot, evaluation := matterSharedAssetFixture(t)
	stale := 0
	for _, item := range evaluation.Facts {
		if item.Freshness != semreg.FreshnessFresh {
			stale++
		}
	}
	if len(snapshot.Services) != 2 || stale != 1 {
		t.Fatalf("fixture services=%d stale=%d; want two carriers and one stale unmapped fact", len(snapshot.Services), stale)
	}
	source := &matterSharedAssetSource{contributions: contributions, snapshot: snapshot, evaluation: evaluation, attribution: portalAttribution{drivers: portalTestDrivers{"source:owner": teslaEVSEDriver, "source:contributor": "evse.contributor"}, accepted: contributions.snapshot().Accepted}.publishedBy(teslaEVSEDriver)}
	provider, err := newGatewayMatterBindingFeedProvider(source, []string{"asset:shared"})
	if err != nil {
		t.Fatal(err)
	}
	feed, err := provider.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(feed.Resources) != 1 {
		t.Fatalf("feed resources=%+v; want the shared asset's resource", feed.Resources)
	}
	got := feed.Resources[0]
	if got.AssetID != "asset:shared" || got.BindingID != "binding:owner" || got.SourceEpoch != "epoch:owner" || got.DriverGeneration != 3 {
		t.Fatalf("feed resource=%+v; want the owner's binding", got)
	}
	if got.Projected == nil || len(got.Projected.Document.Attributes) != 1 || got.Projected.Document.Attributes[0].Value.Milliampere != "3200" {
		t.Fatalf("projection=%+v", got.Projected)
	}
}

func TestMatterBindingFeedOperationNamesTheBindingsDriver(t *testing.T) {
	resource := feedv1.Resource{AssetID: "asset:shared", ResourceID: "connector", SnapshotID: "snapshot:shared", BindingID: "binding:contributor", SourceEpoch: "epoch:contributor", DriverGeneration: 5, State: feedv1.ResourceCurrent}
	action := catalogv1.Action{
		ID: "allocate", ResourceID: "connector", ServiceID: "evse.service.connector", CapabilityID: "evse.capability.set_allocated_current", OperationID: "evse.operation.set_allocated_current",
		ContributionDriverID: "evse.contributor", ContributionManifestID: "portal.evse.contributed", ContributionManifestVersion: "1.1.0",
		Source:       catalogv1.Source{AssetID: "asset:shared", SnapshotID: "snapshot:shared", Revision: "revision:shared", BindingID: "binding:contributor", SourceEpoch: "epoch:contributor", DriverGeneration: 5},
		Discoverable: true, Enabled: true,
	}
	operation, err := matterBindingFeedOperation("catalog:revision", "digest:contributor", action, resource)
	if err != nil {
		t.Fatal(err)
	}
	want := feedv1.OperationClaim{CatalogRevision: "catalog:revision", Digest: "digest:contributor", DriverID: "evse.contributor", ManifestID: "portal.evse.contributed", ManifestVersion: "1.1.0", ActionID: "allocate", ResourceID: "connector", CapabilityID: "evse.capability.set_allocated_current", OperationID: "evse.operation.set_allocated_current", SnapshotID: "snapshot:shared", Revision: "revision:shared", BindingID: "binding:contributor", SourceEpoch: "epoch:contributor", DriverGeneration: 5}
	if operation.Claim != want || operation.AssetID != "asset:shared" {
		t.Fatalf("claim=%+v; want %+v", operation.Claim, want)
	}
	// Each member of the binding path must agree between the action and its
	// resource; a different owner's binding never stands in for it.
	for name, mutate := range map[string]func(*feedv1.Resource){
		"asset":      func(r *feedv1.Resource) { r.AssetID = "asset:other" },
		"snapshot":   func(r *feedv1.Resource) { r.SnapshotID = "snapshot:other" },
		"binding":    func(r *feedv1.Resource) { r.BindingID = "binding:owner" },
		"epoch":      func(r *feedv1.Resource) { r.SourceEpoch = "epoch:owner" },
		"generation": func(r *feedv1.Resource) { r.DriverGeneration = 3 },
		"resource":   func(r *feedv1.Resource) { r.ResourceID = "evse" },
	} {
		other := resource
		mutate(&other)
		if _, err := matterBindingFeedOperation("catalog:revision", "digest:contributor", action, other); err == nil {
			t.Fatalf("%s mismatch admitted an operation", name)
		}
	}
}
