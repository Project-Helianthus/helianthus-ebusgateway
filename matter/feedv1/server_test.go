package feedv1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/matter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

type fakeProvider struct {
	mu       sync.Mutex
	snapshot Snapshot
	captures int
	invokes  int
	reject   bool
	last     Invocation
}

func (f *fakeProvider) Capture(context.Context) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.captures++
	return detach(f.snapshot)
}
func (f *fakeProvider) Invoke(_ context.Context, call Invocation) (Execution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invokes++
	f.last = call
	if f.reject {
		return Execution{}, ErrInvalidIntent
	}
	if call.Intent.Claim != call.Operation.Claim {
		return Execution{}, ErrInvalidIntent
	}
	return Execution{Contract: Contract, ACK: "ACKNOWLEDGED", Readback: json.RawMessage(`{"value":1}`), Outcome: json.RawMessage(`{"state":"completed"}`)}, nil
}

func validSnapshot(t *testing.T, state string, includeOperation bool) Snapshot {
	t.Helper()
	requested, dispositions := matter.Ledger()
	revisions := semreg.RevisionVector{Semantic: "1", Identity: "1", Facts: "1", Services: "1", Capabilities: "1"}
	report := projection.ProjectionReport{Contract: projection.ContractProjectionV1, Manifest: matter.Manifest(), SnapshotID: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Revisions: revisions, Requested: requested, Dispositions: dispositions}
	result := matter.Result{Document: matter.Document{Contract: matter.Contract, TargetID: matter.TargetID, TargetVersion: matter.TargetVersion, MappingRevision: matter.MappingRevision, Attributes: []matter.TargetAttribute{}}, Report: report}
	resource := Resource{AssetID: "asset:test", ResourceID: "evse", State: state}
	if state == ResourceCurrent {
		resource.SnapshotID, resource.Revisions, resource.BindingID, resource.SourceEpoch, resource.DriverGeneration, resource.Projected = string(report.SnapshotID), revisions, "binding:test", "epoch:test", 1, &result
	}
	snapshot := Snapshot{Contract: Contract, Target: expectedTarget(), Ledger: Ledger{Manifest: matter.Manifest(), Requested: requested, Dispositions: dispositions}, Resources: []Resource{resource}, Operations: []AdmittedOperation{}}
	if includeOperation && state == ResourceCurrent {
		claim := OperationClaim{CatalogRevision: "catalog", Digest: "digest", DriverID: "driver", ManifestID: "manifest", ManifestVersion: "1.0.0", ActionID: "set", ResourceID: "evse", CapabilityID: "capability", OperationID: "operation", SnapshotID: resource.SnapshotID, Revision: revisionsText(revisions), BindingID: resource.BindingID, SourceEpoch: resource.SourceEpoch, DriverGeneration: 1}
		snapshot.Operations = append(snapshot.Operations, AdmittedOperation{AssetID: resource.AssetID, Action: catalogv1.Action{ID: "set", ResourceID: "evse", CapabilityID: "capability", OperationID: "operation", ContributionDriverID: "driver", ContributionManifestID: "manifest", ContributionManifestVersion: "1.0.0", Discoverable: true, Enabled: true}, Claim: claim})
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("fixture invalid: %v", err)
	}
	return snapshot
}

func validIntent(snapshot Snapshot) Intent {
	operation := snapshot.Operations[0]
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	causal := semreg.CausalContext{Origin: semreg.OriginRef{OriginID: "origin:test", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{evidence}}, CorrelationID: "correlation:test", MaxHops: 1, FirstSeenAt: semreg.TimePoint{UnixNanoseconds: "1", ClockID: "clock.utc", UncertaintyNS: "0"}, ExpiresAt: semreg.TimePoint{UnixNanoseconds: "2", ClockID: "clock.utc", UncertaintyNS: "0"}, Path: []semreg.TargetID{}}
	return Intent{AssetID: operation.AssetID, Claim: operation.Claim, IdempotencyKey: "key:test", Deadline: time.Now().Add(time.Minute).UTC(), Arguments: json.RawMessage(`{"setpoint":"32"}`), Causal: causal}
}

func request(t *testing.T, server http.Handler, method, path string, body any, principal bool) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if principal {
		r = r.WithContext(WithPrincipal(r.Context(), "m2m:test"))
	}
	w := httptest.NewRecorder()
	server.ServeHTTP(w, r)
	return w
}

func TestStandaloneConsumerLifecycleAndAdmission(t *testing.T) {
	fake := &fakeProvider{snapshot: validSnapshot(t, ResourceCurrent, true)}
	server, err := New(fake, Options{InstanceID: "feed-a", ReplayLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	first := request(t, server, http.MethodGet, "/v1/snapshot", nil, false)
	if first.Code != http.StatusOK {
		t.Fatalf("snapshot=%d %s", first.Code, first.Body.String())
	}
	firstBytes := append([]byte(nil), first.Body.Bytes()...)
	second := request(t, server, http.MethodGet, "/v1/snapshot", nil, false)
	if !bytes.Equal(firstBytes, second.Body.Bytes()) {
		t.Fatal("no-op capture changed deterministic response")
	}
	if got := request(t, server, http.MethodGet, "/v1/changes?cursor=feed-a:1", nil, false); got.Code != http.StatusOK || bytes.Contains(got.Body.Bytes(), []byte(`"changes":[]`)) == false {
		t.Fatalf("stable changes=%d %s", got.Code, got.Body.String())
	}
	fake.mu.Lock()
	fake.snapshot = validSnapshot(t, ResourceWithdrawn, false)
	fake.mu.Unlock()
	changed := request(t, server, http.MethodGet, "/v1/changes?cursor=feed-a:1", nil, false)
	if changed.Code != http.StatusOK || !bytes.Contains(changed.Body.Bytes(), []byte("WITHDRAWN")) {
		t.Fatalf("withdrawal=%d %s", changed.Code, changed.Body.String())
	}
	fake.mu.Lock()
	fake.snapshot = validSnapshot(t, ResourceUnavailable, false)
	fake.mu.Unlock()
	if got := request(t, server, http.MethodGet, "/v1/changes?cursor=feed-a:2", nil, false); got.Code != http.StatusOK {
		t.Fatalf("second change=%d", got.Code)
	}
	if got := request(t, server, http.MethodGet, "/v1/changes?cursor=feed-a:1", nil, false); got.Code != http.StatusConflict {
		t.Fatalf("replay exhaustion=%d", got.Code)
	}
	restarted, err := New(fake, Options{InstanceID: "feed-b", ReplayLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := request(t, restarted, http.MethodGet, "/v1/changes?cursor=feed-a:2", nil, false); got.Code != http.StatusConflict {
		t.Fatalf("restart resync=%d", got.Code)
	}

	fake.mu.Lock()
	fake.snapshot = validSnapshot(t, ResourceCurrent, true)
	fake.mu.Unlock()
	intent := validIntent(fake.snapshot)
	if got := request(t, server, http.MethodPost, "/v1/invoke", intent, false); got.Code != http.StatusUnauthorized {
		t.Fatalf("missing principal=%d", got.Code)
	}
	fake.mu.Lock()
	before := fake.invokes
	fake.mu.Unlock()
	if got := request(t, server, http.MethodPost, "/v1/invoke", intent, true); got.Code != http.StatusOK {
		t.Fatalf("invoke=%d %s", got.Code, got.Body.String())
	}
	fake.mu.Lock()
	after := fake.invokes
	fake.mu.Unlock()
	if after != before+1 {
		t.Fatalf("native owner calls=%d want=%d", after, before+1)
	}
	fake.mu.Lock()
	fake.reject = true
	fake.mu.Unlock()
	if got := request(t, server, http.MethodPost, "/v1/invoke", intent, true); got.Code != http.StatusForbidden {
		t.Fatalf("causal rejection=%d", got.Code)
	}
}

func TestInputDetachmentAndStrictRejectionHaveNoInvoke(t *testing.T) {
	fake := &fakeProvider{snapshot: validSnapshot(t, ResourceCurrent, true)}
	server, err := New(fake, Options{InstanceID: "feed-c", ReplayLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	intent := validIntent(fake.snapshot)
	if got := request(t, server, http.MethodPost, "/v1/invoke", map[string]any{"asset_id": "asset:test", "route": "native"}, true); got.Code != http.StatusBadRequest {
		t.Fatalf("route injection=%d", got.Code)
	}
	fake.mu.Lock()
	calls := fake.invokes
	fake.mu.Unlock()
	if calls != 0 {
		t.Fatal("malformed intent reached provider")
	}
	expired := intent
	expired.Deadline = time.Now().Add(-time.Second)
	if got := request(t, server, http.MethodPost, "/v1/invoke", expired, true); got.Code != http.StatusBadRequest {
		t.Fatalf("expired deadline=%d", got.Code)
	}
	tooLong := intent
	tooLong.IdempotencyKey = strings.Repeat("a", 257)
	if got := request(t, server, http.MethodPost, "/v1/invoke", tooLong, true); got.Code != http.StatusBadRequest {
		t.Fatalf("long idempotency=%d", got.Code)
	}
	fake.mu.Lock()
	calls = fake.invokes
	fake.mu.Unlock()
	if calls != 0 {
		t.Fatal("rejected deadline or idempotency reached provider")
	}
	raw, _ := json.Marshal(intent)
	r := httptest.NewRequest(http.MethodPost, "/v1/invoke", bytes.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	r = r.WithContext(WithPrincipal(r.Context(), "m2m:test"))
	w := httptest.NewRecorder()
	server.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("invoke=%d", w.Code)
	}
	intent.Arguments[2] = 'X'
	fake.mu.Lock()
	stored := append([]byte(nil), fake.last.Intent.Arguments...)
	fake.mu.Unlock()
	if bytes.Contains(stored, []byte("X")) {
		t.Fatal("provider invocation aliases input")
	}
	if fake.snapshot.Operations[0].Claim != validSnapshot(t, ResourceCurrent, true).Operations[0].Claim {
		t.Fatal("input mutation changed captured operation")
	}
}

func TestConcurrentCaptureKeepsOneCursor(t *testing.T) {
	fake := &fakeProvider{snapshot: validSnapshot(t, ResourceCurrent, false)}
	server, err := New(fake, Options{InstanceID: "feed-race", ReplayLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			got := request(t, server, http.MethodGet, "/v1/snapshot", nil, false)
			if got.Code != http.StatusOK {
				t.Errorf("snapshot status=%d", got.Code)
			}
		}()
	}
	group.Wait()
	got := request(t, server, http.MethodGet, "/v1/changes?cursor=feed-race:1", nil, false)
	if got.Code != http.StatusOK || !bytes.Contains(got.Body.Bytes(), []byte(`"changes":[]`)) {
		t.Fatalf("concurrent cursor=%d %s", got.Code, got.Body.String())
	}
}

func TestStandaloneClientUsesOnlyPublicWireContract(t *testing.T) {
	fake := &fakeProvider{snapshot: validSnapshot(t, ResourceCurrent, false)}
	server, err := New(fake, Options{InstanceID: "feed-client", ReplayLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client := Client{BaseURL: httpServer.URL, HTTPClient: httpServer.Client()}
	first, err := client.Snapshot(context.Background())
	if err != nil || first.Cursor != (Cursor{Instance: "feed-client", Sequence: 1}) || first.Snapshot.Contract != Contract {
		t.Fatalf("snapshot=%+v err=%v", first, err)
	}
	changes, err := client.Changes(context.Background(), first.Cursor)
	if err != nil || len(changes.Changes) != 0 {
		t.Fatalf("changes=%+v err=%v", changes, err)
	}
}

func TestStandaloneClientReturnsTypedResyncForForeignAheadAndExpiredCursors(t *testing.T) {
	fake := &fakeProvider{snapshot: validSnapshot(t, ResourceCurrent, false)}
	server, err := New(fake, Options{InstanceID: "feed-resync", ReplayLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client := Client{BaseURL: httpServer.URL, HTTPClient: httpServer.Client()}

	foreign, err := client.Changes(context.Background(), Cursor{Instance: "other-process", Sequence: 1})
	if err != nil || !foreign.ResyncRequired || foreign.Cursor != (Cursor{Instance: "feed-resync", Sequence: 1}) {
		t.Fatalf("foreign resync=%+v err=%v", foreign, err)
	}
	ahead, err := client.Changes(context.Background(), Cursor{Instance: "feed-resync", Sequence: 99})
	if err != nil || !ahead.ResyncRequired || ahead.Cursor.Sequence != 1 {
		t.Fatalf("ahead resync=%+v err=%v", ahead, err)
	}

	first, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.snapshot = validSnapshot(t, ResourceWithdrawn, false)
	fake.mu.Unlock()
	if _, err := client.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.snapshot = validSnapshot(t, ResourceUnavailable, false)
	fake.mu.Unlock()
	if _, err := client.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	expired, err := client.Changes(context.Background(), first.Cursor)
	if err != nil || !expired.ResyncRequired || expired.Cursor.Sequence != 3 {
		t.Fatalf("expired resync=%+v err=%v", expired, err)
	}
	resynced, err := client.Snapshot(context.Background())
	if err != nil || resynced.Cursor != expired.Cursor || resynced.Snapshot.Resources[0].State != ResourceUnavailable {
		t.Fatalf("resynced snapshot=%+v err=%v", resynced, err)
	}
}
