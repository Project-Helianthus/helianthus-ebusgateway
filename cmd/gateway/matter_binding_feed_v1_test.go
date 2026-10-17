package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
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
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
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
		ClientCAFile: certs.caFile, ServerCertFile: certs.serverCertFile, ServerKeyFile: certs.serverKeyFile,
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
	// The Matter routes spend the per-host request budget (two tokens, one
	// refill per second); both snapshots above came from this loopback host.
	time.Sleep(1100 * time.Millisecond)
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
	graphqlBody := `{"operationName":"SemanticPVCurrent","query":"query SemanticPVCurrent($request: M2MCurrentSnapshotRequest!) { semanticPVCurrent(request: $request) { snapshot evaluation selections projection } }","variables":{"request":{"contractId":"PUBLIC_GRAPHQL_SEMANTIC_PV_V1","assetRef":"pv-asset-known"}}}`
	graphqlRefusal := serveRefused(http.MethodPost, "/graphql/m2m/v1", "198.51.100.7:40003", graphqlBody)
	if !strings.Contains(matterRefusal.Body.String(), `"REQUEST_LIMIT_EXCEEDED"`) || matterRefusal.Code != graphqlRefusal.Code || matterRefusal.Body.String() != graphqlRefusal.Body.String() {
		t.Fatalf("concurrent same-host Matter refusal=%d %s; GraphQL refusal=%d %s", matterRefusal.Code, matterRefusal.Body.String(), graphqlRefusal.Code, graphqlRefusal.Body.String())
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
