package eebusadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

type adminV1Stub struct {
	mu            sync.Mutex
	snapshot      eebusruntime.AdminSnapshotV1
	snapshots     map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1
	snapshotCalls int
	openCalls     []eebusruntime.OpenPairingWindowRequestV1
	selectCalls   []eebusruntime.SelectRequestV1
	connectCalls  []eebusruntime.ConnectRequestV1
	confirmCalls  []eebusruntime.ConfirmRequestV1
	cancelCalls   []eebusruntime.CancelRequestV1
	closeCalls    []eebusruntime.ClosePairingWindowRequestV1
	retryCalls    []eebusruntime.RetryTrustedRequestV1
	untrustCalls  []eebusruntime.UntrustRequestV1
}

func (stub *adminV1Stub) Snapshot(_ context.Context, request eebusruntime.AdminSnapshotRequestV1) (eebusruntime.AdminSnapshotV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.snapshotCalls++
	if snapshot, ok := stub.snapshots[request.View]; ok {
		return snapshot, nil
	}
	return stub.snapshot, nil
}

func (stub *adminV1Stub) OpenPairingWindow(_ context.Context, request eebusruntime.OpenPairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.openCalls = append(stub.openCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "pairing_window_opened"}, nil
}

func (stub *adminV1Stub) ClosePairingWindow(_ context.Context, request eebusruntime.ClosePairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.closeCalls = append(stub.closeCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "pairing_window_closed"}, nil
}

func (stub *adminV1Stub) Select(_ context.Context, request eebusruntime.SelectRequestV1) (eebusruntime.AdminSelectionResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.selectCalls = append(stub.selectCalls, request)
	return eebusruntime.AdminSelectionResultV1{AdminMutationResultV1: eebusruntime.AdminMutationResultV1{
		StateRevision: request.ExpectedStateRevision + 1,
		Outcome:       "selected",
	}}, nil
}

func (stub *adminV1Stub) Connect(_ context.Context, request eebusruntime.ConnectRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.connectCalls = append(stub.connectCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "connecting"}, nil
}

func (stub *adminV1Stub) Confirm(_ context.Context, request eebusruntime.ConfirmRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.confirmCalls = append(stub.confirmCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "confirmed"}, nil
}

func (stub *adminV1Stub) Cancel(_ context.Context, request eebusruntime.CancelRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.cancelCalls = append(stub.cancelCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "cancelled"}, nil
}

func (stub *adminV1Stub) RetryTrusted(_ context.Context, request eebusruntime.RetryTrustedRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.retryCalls = append(stub.retryCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "retry_started"}, nil
}

func (stub *adminV1Stub) Untrust(_ context.Context, request eebusruntime.UntrustRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.untrustCalls = append(stub.untrustCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: "untrusted"}, nil
}

func TestIssue817UnavailableBoundaryStaysMountedAndSanitized(t *testing.T) {
	handler := NewUnavailableHandler()
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.Header.Set("Authorization", "Bearer must-not-be-reflected")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	assertIssue817ErrorEnvelope(t, response.Body.String(), "admin_boundary_unavailable")
	if strings.Contains(response.Body.String(), "must-not-be-reflected") {
		t.Fatalf("unavailable boundary reflected request material: %s", response.Body.String())
	}
}

func TestIssue817CredentialFreeReadsUseOneStateRevisionEnvelope(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 21
	snapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, SHIPID: "ship-1", TrustState: "durably_trusted"}}
	snapshot.Connected = []eebusruntime.ConnectedPartnerV1{{SKI: ski, SHIPID: "ship-1", ConnectionState: "connected"}}
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Identifier: "ship-1", Endpoint: "192.0.2.10:4712", ObservationRevision: 3}}
	snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute)}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{
		eebusruntime.AdminViewV1Trusted: snapshot, eebusruntime.AdminViewV1Connected: snapshot,
		eebusruntime.AdminViewV1Discovered: snapshot, eebusruntime.AdminViewV1Candidate: snapshot,
	}}
	handler := newIssue817Server(t, admin, nil, nil)

	paths := []string{"/admin/eebus/v1/status"}
	for _, view := range []string{"trusted", "connected", "discovered", "candidate"} {
		paths = append(paths, "/admin/eebus/v1/partners?view="+view)
	}
	for index, target := range paths {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		if index%2 == 1 {
			issue817AddIrrelevantAuthMaterial(request)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", target, response.Code, response.Body.String())
		}
		assertIssue817OperatorEnvelope(t, response, 21)
		assertIssue817NoAuthResponseHeaders(t, response)
		if strings.Contains(response.Body.String(), "projection_revision") {
			t.Fatalf("GET %s returned a split HA projection envelope: %s", target, response.Body.String())
		}
	}
}

func TestIssue817CapabilityCapacityFailsClosedWithoutPartialRows(t *testing.T) {
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 22
	for index := 0; index < 129; index++ {
		snapshot.Discovered = append(snapshot.Discovered, eebusruntime.DiscoveredPartnerV1{
			SKI:                 strings.Repeat(string(rune('a'+index%6)), 40),
			Endpoint:            "192.0.2.10:4712",
			ObservationRevision: uint64(index + 1),
			Identifier:          string(rune('A' + index)),
		})
	}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue817Server(t, admin, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("capacity status=%d body=%s", response.Code, response.Body.String())
	}
	assertIssue817ErrorEnvelope(t, response.Body.String(), "admin_boundary_unavailable")
	if strings.Contains(response.Body.String(), "partners") {
		t.Fatalf("capacity failure emitted partial rows: %s", response.Body.String())
	}
}

func TestIssue817SelectionAndConnectShareProcessLocalScopeAcrossRequests(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 11
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue817Server(t, admin, nil, nil)

	list := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil)
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, list)
	observationID := issue817FirstOpaqueID(t, listResponse, "observation_id")

	selectRequest := issue817Mutation(http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", "select-817", `{"state_revision":11,"expected_ski":"`+ski+`"}`)
	issue817AddIrrelevantAuthMaterial(selectRequest)
	selectResponse := httptest.NewRecorder()
	handler.ServeHTTP(selectResponse, selectRequest)
	if selectResponse.Code != http.StatusOK {
		t.Fatalf("select status=%d body=%s", selectResponse.Code, selectResponse.Body.String())
	}
	selectionID := issue817DataString(t, selectResponse, "selection_id")

	connectRequest := issue817Mutation(http.MethodPost, "/admin/eebus/v1/selections/"+selectionID+":connect", "connect-817", `{"state_revision":12}`)
	connectRequest.Header.Set("Authorization", "Basic another-client-value")
	connectResponse := httptest.NewRecorder()
	handler.ServeHTTP(connectResponse, connectRequest)
	if connectResponse.Code != http.StatusOK {
		t.Fatalf("connect status=%d body=%s", connectResponse.Code, connectResponse.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.selectCalls) != 1 || len(admin.connectCalls) != 1 || admin.selectCalls[0].ExpectedSKI != ski {
		t.Fatalf("select/connect calls=%#v/%#v", admin.selectCalls, admin.connectCalls)
	}
}

func TestIssue817ClosedMutationMatrixNeedsOnlyRevisionIdempotencyAndTypedArguments(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"

	t.Run("open and close", func(t *testing.T) {
		admin := &adminV1Stub{snapshot: testAdminSnapshot()}
		handler := newIssue817Server(t, admin, nil, nil)
		for _, mutation := range []struct{ path, key, body string }{
			{"/admin/eebus/v1/pairing-window:open", "open-817", `{"duration_seconds":60,"state_revision":7}`},
			{"/admin/eebus/v1/pairing-window:close", "close-817", `{"state_revision":7}`},
		} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(http.MethodPost, mutation.path, mutation.key, mutation.body))
			if response.Code != http.StatusOK {
				t.Fatalf("POST %s status=%d body=%s", mutation.path, response.Code, response.Body.String())
			}
			assertIssue817NoAuthResponseHeaders(t, response)
		}
		if len(admin.openCalls) != 1 || len(admin.closeCalls) != 1 {
			t.Fatalf("open/close calls=%d/%d", len(admin.openCalls), len(admin.closeCalls))
		}
	})

	for _, test := range []struct {
		name, route, key, body string
		confirm                bool
	}{
		{"confirm", "/admin/eebus/v1/candidate:confirm", "confirm-817", `{"state_revision":31,"expected_ski":"` + ski + `"}`, true},
		{"cancel", "/admin/eebus/v1/candidate:cancel", "cancel-817", `{"state_revision":31}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 31
			snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute)}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Candidate: snapshot}}
			handler := newIssue817Server(t, admin, nil, nil)
			candidate := httptest.NewRecorder()
			handler.ServeHTTP(candidate, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=candidate", nil))
			if candidate.Code != http.StatusOK || !strings.Contains(candidate.Body.String(), ski) {
				t.Fatalf("candidate status=%d body=%s", candidate.Code, candidate.Body.String())
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(http.MethodPost, test.route, test.key, test.body))
			if response.Code != http.StatusOK {
				t.Fatalf("%s status=%d body=%s", test.name, response.Code, response.Body.String())
			}
			if test.confirm && (len(admin.confirmCalls) != 1 || admin.confirmCalls[0].ExpectedSKI != ski) {
				t.Fatalf("confirm calls=%#v", admin.confirmCalls)
			}
			if !test.confirm && len(admin.cancelCalls) != 1 {
				t.Fatalf("cancel calls=%#v", admin.cancelCalls)
			}
		})
	}

	for _, test := range []struct{ name, method, suffix, key string }{
		{"retry", http.MethodPost, ":retry", "retry-817"},
		{"untrust", http.MethodDelete, "/trust", "untrust-817"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 41
			snapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, TrustState: "durably_trusted"}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: snapshot}}
			handler := newIssue817Server(t, admin, nil, nil)
			trusted := httptest.NewRecorder()
			handler.ServeHTTP(trusted, httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=trusted", nil))
			partnerID := issue817FirstOpaqueID(t, trusted, "partner_id")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue817Mutation(test.method, "/admin/eebus/v1/partners/"+partnerID+test.suffix, test.key, `{"state_revision":41}`))
			if response.Code != http.StatusOK {
				t.Fatalf("%s status=%d body=%s", test.name, response.Code, response.Body.String())
			}
			if test.name == "retry" && len(admin.retryCalls) != 1 {
				t.Fatalf("retry calls=%#v", admin.retryCalls)
			}
			if test.name == "untrust" && len(admin.untrustCalls) != 1 {
				t.Fatalf("untrust calls=%#v", admin.untrustCalls)
			}
		})
	}
}

func TestIssue817HTTPReplayRemainsBoundedAndExactWithoutASession(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	handler := newIssue817Server(t, admin, nil, nil)
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, issue817Mutation(http.MethodPost, "/admin/eebus/v1/pairing-window:open", "replay-817", `{"duration_seconds":60,"state_revision":7}`))
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, issue817Mutation(http.MethodPost, "/admin/eebus/v1/pairing-window:open", "replay-817", `{"duration_seconds":60,"state_revision":7}`))
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, issue817Mutation(http.MethodPost, "/admin/eebus/v1/pairing-window:open", "replay-817", `{"duration_seconds":120,"state_revision":7}`))
	if first.Code != http.StatusOK || second.Code != http.StatusOK || first.Body.String() != second.Body.String() || conflict.Code != http.StatusConflict {
		t.Fatalf("replay statuses/bodies=%d/%d/%d\n%s\n%s\n%s", first.Code, second.Code, conflict.Code, first.Body.String(), second.Body.String(), conflict.Body.String())
	}
	if len(admin.openCalls) != 1 {
		t.Fatalf("replayed mutation invoked runtime %d times", len(admin.openCalls))
	}
}

func TestIssue817AuditUsesOneOperatorClassificationAndExcludesRequestSecrets(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	var events []AuditEvent
	handler := newIssue817Server(t, admin, nil, func(event AuditEvent) { events = append(events, event) })
	request := issue817Mutation(http.MethodPost, "/admin/eebus/v1/pairing-window:open", "audit-key-817", `{"duration_seconds":60,"state_revision":7}`)
	issue817AddIrrelevantAuthMaterial(request)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(events) != 1 {
		t.Fatalf("audit mutation status/events=%d/%d body=%s", response.Code, len(events), response.Body.String())
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	audit := string(encoded)
	for _, forbidden := range []string{"portal_owner", "ha_integration", "audit-key-817", "irrelevant-secret", "csrf", "cookie"} {
		if strings.Contains(strings.ToLower(audit), strings.ToLower(forbidden)) {
			t.Fatalf("audit retains split identity or request secret %q: %s", forbidden, audit)
		}
	}
}

func newIssue817Server(t *testing.T, admin eebusruntime.AdminV1, raw RawSnapshotProvider, audit func(AuditEvent)) http.Handler {
	t.Helper()
	handler, err := NewServer(Config{Admin: admin, Raw: raw, Audit: audit})
	if err != nil {
		t.Fatalf("credential-free NewServer: %v", err)
	}
	return handler
}

func issue817Mutation(method, path, idempotencyKey, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	return request
}

func issue817AddIrrelevantAuthMaterial(request *http.Request) {
	request.Header.Set("Authorization", "Bearer irrelevant-secret")
	request.Header.Set("Origin", "https://irrelevant.invalid")
	request.Header.Set("Referer", "https://irrelevant.invalid/portal")
	request.Header.Set("X-CSRF-Token", "irrelevant-csrf")
	request.AddCookie(&http.Cookie{Name: "irrelevant", Value: "irrelevant-cookie"})
}

func assertIssue817NoAuthResponseHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Set-Cookie") != "" || response.Header().Get("X-CSRF-Token") != "" {
		t.Fatalf("eeBUS operator response emitted auth/session headers: %v", response.Header())
	}
}

func assertIssue817OperatorEnvelope(t *testing.T, response *httptest.ResponseRecorder, revision uint64) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode envelope: %v body=%s", err, response.Body.String())
	}
	if envelope["contract"] != ContractV1 || envelope["state_revision"] != float64(revision) || envelope["request_id"] == "" || envelope["error"] != nil {
		t.Fatalf("operator envelope=%#v", envelope)
	}
}

func issue817FirstOpaqueID(t *testing.T, response *httptest.ResponseRecorder, field string) string {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data struct {
			Partners []map[string]any `json:"partners"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil || len(envelope.Data.Partners) != 1 {
		t.Fatalf("decode partners: %v body=%s", err, response.Body.String())
	}
	value, _ := envelope.Data.Partners[0][field].(string)
	if value == "" {
		t.Fatalf("partner row lacks %s: %s", field, response.Body.String())
	}
	return value
}

func issue817DataString(t *testing.T, response *httptest.ResponseRecorder, field string) string {
	t.Helper()
	var envelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	value, _ := envelope.Data[field].(string)
	if value == "" {
		t.Fatalf("response lacks data.%s: %s", field, response.Body.String())
	}
	return value
}

func testAdminSnapshot() eebusruntime.AdminSnapshotV1 {
	return eebusruntime.AdminSnapshotV1{
		StateRevision: 7, CapturedAt: time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC),
		Status: "ready", Window: "open", WindowDeadline: time.Date(2026, 8, 14, 8, 5, 0, 0, time.UTC),
		Register: "true", Listener: "ready", Discovery: "ready",
		TrustedCount: 1, ConnectedCount: 1, DiscoveredCount: 1, CandidateCount: 1,
	}
}

func assertIssue817ErrorEnvelope(t *testing.T, body, code string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%q", err, body)
	}
	errorObject, ok := envelope["error"].(map[string]any)
	if envelope["contract"] != ContractV1 || !ok || errorObject["code"] != code {
		t.Fatalf("error envelope=%#v, want %q", envelope, code)
	}
}
