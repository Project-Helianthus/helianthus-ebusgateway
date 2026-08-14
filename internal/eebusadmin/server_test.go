package eebusadmin

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

const (
	testOwnerOrigin = "http://gateway.test:8080"
	testOwnerSecret = "owner-secret-value"
	testHASecret    = "ha-secret-value"
)

type adminV1Stub struct {
	mu            sync.Mutex
	snapshot      eebusruntime.AdminSnapshotV1
	snapshots     map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1
	snapshotCalls int
	openCalls     int
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

func (stub *adminV1Stub) OpenPairingWindow(context.Context, eebusruntime.OpenPairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.openCalls++
	return eebusruntime.AdminMutationResultV1{
		StateRevision: stub.snapshot.StateRevision + 1,
		Outcome:       eebusruntime.AdminOutcomeV1("pairing_window_opened"),
	}, nil
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
		Outcome:       eebusruntime.AdminOutcomeV1("selected"),
	}}, nil
}

func (stub *adminV1Stub) Connect(_ context.Context, request eebusruntime.ConnectRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.connectCalls = append(stub.connectCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: eebusruntime.AdminOutcomeV1("connecting")}, nil
}

func (stub *adminV1Stub) Confirm(_ context.Context, request eebusruntime.ConfirmRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.confirmCalls = append(stub.confirmCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: eebusruntime.AdminOutcomeV1("confirmed")}, nil
}

func (stub *adminV1Stub) Cancel(_ context.Context, request eebusruntime.CancelRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.cancelCalls = append(stub.cancelCalls, request)
	return eebusruntime.AdminMutationResultV1{StateRevision: request.ExpectedStateRevision + 1, Outcome: eebusruntime.AdminOutcomeV1("cancelled")}, nil
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

func TestIssue809AdminBoundaryFailsClosedBeforeRuntimeContact(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	handler := newIssue809Server(t, admin)

	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want %d", response.Code, http.StatusUnauthorized)
	}
	assertIssue809ErrorEnvelope(t, response.Body.String(), "unauthenticated")
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if admin.snapshotCalls != 0 || admin.openCalls != 0 {
		t.Fatalf("runtime calls snapshot/open=%d/%d, want 0/0", admin.snapshotCalls, admin.openCalls)
	}
}

func TestIssue809UnavailableBoundaryIsAlwaysMountedAndReturnsNoOperationalData(t *testing.T) {
	handler := NewUnavailableHandler()
	for _, test := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/admin/eebus/v1/status"},
		{method: http.MethodPost, path: "/admin/eebus/v1/pairing-window:open", body: `{"duration_seconds":60,"state_revision":7}`},
	} {
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		request.Header.Set("Authorization", "Bearer must-not-be-reflected")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, response.Code, response.Body.String())
		}
		assertIssue809ErrorEnvelope(t, response.Body.String(), "admin_boundary_unavailable")
		if strings.Contains(response.Body.String(), "must-not-be-reflected") {
			t.Fatalf("unavailable boundary reflected authentication material: %s", response.Body.String())
		}
	}
}

func TestIssue809OwnerSessionRequiresCSRFAndStrictSameOrigin(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	handler := newIssue809Server(t, admin)

	statusRequest := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	statusRequest.SetBasicAuth("owner", testOwnerSecret)
	statusResponse := httptest.NewRecorder()
	handler.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("owner status=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	cookies := statusResponse.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("owner session cookies=%#v, want one HttpOnly SameSite=Strict cookie", cookies)
	}
	csrf := statusResponse.Header().Get("X-CSRF-Token")
	if csrf == "" {
		t.Fatal("owner status omitted session-bound CSRF token")
	}

	for _, test := range []struct {
		name    string
		origin  string
		referer string
		csrf    string
	}{
		{name: "missing csrf", origin: testOwnerOrigin, referer: testOwnerOrigin + "/portal/eebus"},
		{name: "wrong origin", origin: "http://attacker.invalid", referer: testOwnerOrigin + "/portal/eebus", csrf: csrf},
		{name: "wrong referer", origin: testOwnerOrigin, referer: "http://attacker.invalid/", csrf: csrf},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/admin/eebus/v1/pairing-window:open", strings.NewReader(`{"duration_seconds":60,"state_revision":7}`))
			request.AddCookie(cookies[0])
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "open-window-1")
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Referer", test.referer)
			request.Header.Set("X-CSRF-Token", test.csrf)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s, want 403", response.Code, response.Body.String())
			}
			assertIssue809ErrorEnvelope(t, response.Body.String(), "csrf_rejected")
		})
	}

	admin.mu.Lock()
	defer admin.mu.Unlock()
	if admin.openCalls != 0 {
		t.Fatalf("rejected requests reached runtime %d times", admin.openCalls)
	}
}

func TestIssue809HAProfileIsCandidateFreeAndMutationDenied(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	handler := newIssue809Server(t, admin)

	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.Header.Set("Authorization", "Bearer "+testHASecret)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("HA status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{"state_revision", "request_id", "candidate", "pairing_window", "register"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("HA status leaks %q: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, `"projection_revision":1`) {
		t.Fatalf("HA status missing independent projection revision: %s", body)
	}

	mutation := httptest.NewRequest(http.MethodPost, "/admin/eebus/v1/pairing-window:open", strings.NewReader(`{"duration_seconds":60,"state_revision":7}`))
	mutation.Header.Set("Authorization", "Bearer "+testHASecret)
	mutation.Header.Set("Content-Type", "application/json")
	mutation.Header.Set("Idempotency-Key", "ha-must-not-mutate")
	mutationResponse := httptest.NewRecorder()
	handler.ServeHTTP(mutationResponse, mutation)
	if mutationResponse.Code != http.StatusForbidden {
		t.Fatalf("HA mutation status=%d body=%s, want 403", mutationResponse.Code, mutationResponse.Body.String())
	}
	assertIssue809ErrorEnvelope(t, mutationResponse.Body.String(), "forbidden")

	admin.mu.Lock()
	defer admin.mu.Unlock()
	if admin.openCalls != 0 {
		t.Fatalf("HA mutation reached runtime %d times", admin.openCalls)
	}
}

func TestIssue809HAProjectionIsByteIdenticalAcrossCandidateOnlyLifecycle(t *testing.T) {
	base := testAdminSnapshot()
	base.Window = "closed"
	base.Register = "false"
	base.CandidateCount = 0
	admin := &adminV1Stub{snapshot: base}
	handler := newIssue809Server(t, admin)
	request := func() *http.Request {
		value := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
		value.Header.Set("Authorization", "Bearer "+testHASecret)
		return value
	}
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request())

	admin.mu.Lock()
	admin.snapshot.Window = "transient_trusted"
	admin.snapshot.Register = "true"
	admin.snapshot.CandidateCount = 1
	admin.snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: strings.Repeat("a", 40), State: "tls_bound", AssociationComplete: true}}
	admin.snapshot.DegradedCode = eebusruntime.AdminErrorCodeV1PersistenceFailure
	admin.mu.Unlock()
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request())
	if first.Code != http.StatusOK || second.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("candidate-only HA projection changed:\nfirst=%s\nsecond=%s", first.Body.String(), second.Body.String())
	}
}

func TestIssue809HAStatusIgnoresCandidateOnlyOwnerRevisionChurn(t *testing.T) {
	trusted := testAdminSnapshot()
	connected := trusted
	connected.StateRevision++
	admin := &adminV1Stub{snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{
		eebusruntime.AdminViewV1Trusted: trusted, eebusruntime.AdminViewV1Connected: connected,
	}}
	handler := newIssue809Server(t, admin)
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.Header.Set("Authorization", "Bearer "+testHASecret)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("candidate-only revision churn HA status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "state_conflict") || strings.Contains(response.Body.String(), "state_revision") {
		t.Fatalf("candidate-only owner revision became an HA side channel: %s", response.Body.String())
	}
}

func TestIssue809MutationEmitsSanitizedAuditOutcome(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	var events []AuditEvent
	handler, err := NewServer(Config{
		Admin: admin,
		Audit: func(event AuditEvent) { events = append(events, event) },
		Auth: AuthConfig{
			OwnerUsername: "owner", OwnerSecret: []byte(testOwnerSecret), HASecret: []byte(testHASecret),
			OwnerOrigin: testOwnerOrigin, SessionTTL: 15 * time.Minute,
			Now: func() time.Time { return time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC) }, Random: rand.Reader,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cookie, csrf := issue809OwnerSession(t, handler)
	request := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:open", cookie, csrf, "audit-open-1", `{"duration_seconds":60,"state_revision":7}`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("mutation status=%d body=%s", response.Code, response.Body.String())
	}
	replay := httptest.NewRecorder()
	handler.ServeHTTP(replay, issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:open", cookie, csrf, "audit-open-1", `{"duration_seconds":60,"state_revision":7}`))
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:open", cookie, csrf, "audit-open-1", `{"duration_seconds":120,"state_revision":7}`))
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:open", cookie, csrf, "audit-invalid-1", `{"duration_seconds":0,"state_revision":7}`))
	if replay.Code != http.StatusOK || conflict.Code != http.StatusConflict || rejected.Code != http.StatusBadRequest {
		t.Fatalf("terminal statuses replay/conflict/rejected=%d/%d/%d", replay.Code, conflict.Code, rejected.Code)
	}
	if len(events) != 4 {
		t.Fatalf("audit events=%d, want executed/replayed/conflict/rejected", len(events))
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	audit := string(encoded)
	for _, required := range []string{"open_pairing_window", "portal_owner", "executed", "replayed", "conflict", "rejected", "precondition_accepted", "changed"} {
		if !strings.Contains(audit, required) {
			t.Errorf("audit missing %q: %s", required, audit)
		}
	}
	for _, forbidden := range []string{"audit-open-1", testOwnerSecret, testHASecret, "expected_ski", "endpoint", "token", "key"} {
		if strings.Contains(strings.ToLower(audit), strings.ToLower(forbidden)) {
			t.Errorf("audit leaks %q: %s", forbidden, audit)
		}
	}
	for _, recorder := range []*httptest.ResponseRecorder{conflict, rejected} {
		var envelope ownerEnvelope
		if json.Unmarshal(recorder.Body.Bytes(), &envelope) != nil || envelope.RequestID == "" {
			t.Fatalf("owner terminal error lacks audit request_id: %s", recorder.Body.String())
		}
	}
}

func TestIssue809AuthenticatedPreCaptureMutationRejectionsEmitOneSanitizedAudit(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	var events []AuditEvent
	handler, err := NewServer(Config{
		Admin: admin,
		Audit: func(event AuditEvent) { events = append(events, event) },
		Auth: AuthConfig{
			OwnerUsername: "owner", OwnerSecret: []byte(testOwnerSecret), HASecret: []byte(testHASecret),
			OwnerOrigin: testOwnerOrigin, SessionTTL: 15 * time.Minute,
			Now: func() time.Time { return time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC) }, Random: rand.Reader,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cookie, _ := issue809OwnerSession(t, handler)
	csrfRejected := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:open", cookie, "wrong-csrf", "sensitive-idempotency-key", `{"duration_seconds":60,"state_revision":7}`)
	csrfResponse := httptest.NewRecorder()
	handler.ServeHTTP(csrfResponse, csrfRejected)
	if csrfResponse.Code != http.StatusForbidden {
		t.Fatalf("CSRF rejection status=%d body=%s", csrfResponse.Code, csrfResponse.Body.String())
	}

	haRejected := httptest.NewRequest(http.MethodPost, "/admin/eebus/v1/pairing-window:open", strings.NewReader(`{"duration_seconds":60,"state_revision":7}`))
	haRejected.Header.Set("Authorization", "Bearer "+testHASecret)
	haRejected.Header.Set("Content-Type", "application/json")
	haRejected.Header.Set("Idempotency-Key", "sensitive-idempotency-key")
	haResponse := httptest.NewRecorder()
	handler.ServeHTTP(haResponse, haRejected)
	if haResponse.Code != http.StatusForbidden {
		t.Fatalf("HA rejection status=%d body=%s", haResponse.Code, haResponse.Body.String())
	}

	unauthenticated := httptest.NewRequest(http.MethodPost, "/admin/eebus/v1/pairing-window:open", nil)
	unauthenticatedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d body=%s", unauthenticatedResponse.Code, unauthenticatedResponse.Body.String())
	}
	if len(events) != 2 {
		t.Fatalf("audit events=%d, want one for each authenticated pre-capture rejection", len(events))
	}
	for _, event := range events {
		if event.Action != "open_pairing_window" || event.IdempotencyOutcome != "rejected" || event.PriorStateClass != "authenticated" || event.ResultingStateClass != "rejected" || event.RequestID == "" {
			t.Fatalf("unexpected rejection audit event: %#v", event)
		}
	}
	if events[0].Principal != PrincipalPortalOwner || events[0].Reason != "csrf_rejected" {
		t.Fatalf("owner CSRF audit=%#v", events[0])
	}
	if events[1].Principal != PrincipalHAIntegration || events[1].Reason != "forbidden" {
		t.Fatalf("HA forbidden audit=%#v", events[1])
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"sensitive-idempotency-key", testOwnerSecret, testHASecret, "endpoint", "expected_ski", "token", "key"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
			t.Fatalf("audit leaks %q: %s", forbidden, encoded)
		}
	}
}

func TestIssue809UnknownRuntimeErrorIsNotReflected(t *testing.T) {
	handler, err := NewServer(Config{Admin: &adminV1Stub{snapshot: testAdminSnapshot()}, Auth: issue809AuthConfig()})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.(*server).writeAdminFailure(response, PrincipalPortalOwner, &eebusruntime.AdminErrorV1{Code: "private/internal/path"})
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown error status=%d body=%s", response.Code, response.Body.String())
	}
	assertIssue809ErrorEnvelope(t, response.Body.String(), "unknown_state")
	if strings.Contains(response.Body.String(), "private") {
		t.Fatalf("unknown runtime error reflected: %s", response.Body.String())
	}
}

func TestIssue809MutationRejectsOversizedTrailingWhitespaceBeforeRuntime(t *testing.T) {
	admin := &adminV1Stub{snapshot: testAdminSnapshot()}
	handler := newIssue809Server(t, admin)
	cookie, csrf := issue809OwnerSession(t, handler)
	body := `{"state_revision":7}` + strings.Repeat(" ", maxRequestBodyBytes)
	request := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/pairing-window:close", cookie, csrf, "oversized", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized status=%d body=%s", response.Code, response.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.closeCalls) != 0 {
		t.Fatalf("oversized request reached runtime %d times", len(admin.closeCalls))
	}
}

func TestIssue809OwnerSelectThenConnectUsesOnlyServerHeldCapabilities(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 11
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{
		SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 4,
		LastSeen: snapshot.CapturedAt, Brand: "Vaillant", Type: "gateway", Model: "VR940",
	}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{
		eebusruntime.AdminViewV1Discovered: snapshot,
	}}
	handler := newIssue809Server(t, admin)
	cookie, csrf := issue809OwnerSession(t, handler)

	partners := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil)
	partners.AddCookie(cookie)
	partnersResponse := httptest.NewRecorder()
	handler.ServeHTTP(partnersResponse, partners)
	if partnersResponse.Code != http.StatusOK {
		t.Fatalf("partners status=%d body=%s", partnersResponse.Code, partnersResponse.Body.String())
	}
	if partnersResponse.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("partners Cache-Control=%q", partnersResponse.Header().Get("Cache-Control"))
	}
	var envelope struct {
		StateRevision uint64 `json:"state_revision"`
		Data          struct {
			Partners []struct {
				ObservationID string `json:"observation_id"`
				RemoteSKI     string `json:"remote_ski"`
			} `json:"partners"`
		} `json:"data"`
	}
	if err := json.Unmarshal(partnersResponse.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.StateRevision != 11 || len(envelope.Data.Partners) != 1 || envelope.Data.Partners[0].ObservationID == "" || envelope.Data.Partners[0].RemoteSKI != ski {
		t.Fatalf("discovered envelope=%#v", envelope)
	}
	observationID := envelope.Data.Partners[0].ObservationID

	selectRequest := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/observations/"+observationID+":select", cookie, csrf, "select-1", `{"state_revision":11,"expected_ski":"`+ski+`"}`)
	selectResponse := httptest.NewRecorder()
	handler.ServeHTTP(selectResponse, selectRequest)
	if selectResponse.Code != http.StatusOK {
		t.Fatalf("select status=%d body=%s", selectResponse.Code, selectResponse.Body.String())
	}
	var selectionEnvelope struct {
		StateRevision uint64 `json:"state_revision"`
		Data          struct {
			SelectionID string `json:"selection_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(selectResponse.Body.Bytes(), &selectionEnvelope); err != nil {
		t.Fatal(err)
	}
	if selectionEnvelope.StateRevision != 12 || selectionEnvelope.Data.SelectionID == "" {
		t.Fatalf("selection envelope=%#v", selectionEnvelope)
	}

	connectRequest := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/selections/"+selectionEnvelope.Data.SelectionID+":connect", cookie, csrf, "connect-1", `{"state_revision":12}`)
	connectResponse := httptest.NewRecorder()
	handler.ServeHTTP(connectResponse, connectRequest)
	if connectResponse.Code != http.StatusOK {
		t.Fatalf("connect status=%d body=%s", connectResponse.Code, connectResponse.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.selectCalls) != 1 || len(admin.connectCalls) != 1 || admin.selectCalls[0].ExpectedSKI != ski {
		t.Fatalf("select/connect calls=%d/%d", len(admin.selectCalls), len(admin.connectCalls))
	}
}

func TestIssue809ExactHTTPMutationReplaySurvivesCapabilityInvalidation(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 61
	snapshot.Discovered = []eebusruntime.DiscoveredPartnerV1{{SKI: ski, Endpoint: "192.0.2.20:4712", ObservationRevision: 9}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Discovered: snapshot}}
	handler := newIssue809Server(t, admin)
	cookie, csrf := issue809OwnerSession(t, handler)
	partners := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=discovered", nil)
	partners.AddCookie(cookie)
	partnersResponse := httptest.NewRecorder()
	handler.ServeHTTP(partnersResponse, partners)
	var partnersEnvelope struct {
		Data struct {
			Partners []struct {
				ObservationID string `json:"observation_id"`
			} `json:"partners"`
		} `json:"data"`
	}
	if partnersResponse.Code != http.StatusOK || json.Unmarshal(partnersResponse.Body.Bytes(), &partnersEnvelope) != nil || len(partnersEnvelope.Data.Partners) != 1 {
		t.Fatalf("discovered status=%d body=%s", partnersResponse.Code, partnersResponse.Body.String())
	}
	target := "/admin/eebus/v1/observations/" + partnersEnvelope.Data.Partners[0].ObservationID + ":select"
	body := `{"state_revision":61,"expected_ski":"` + ski + `"}`
	request := func(expectedSKI string) *http.Request {
		value := issue809OwnerMutation(t, http.MethodPost, target, cookie, csrf, "select-replay-1", body)
		if expectedSKI != ski {
			value = issue809OwnerMutation(t, http.MethodPost, target, cookie, csrf, "select-replay-1", `{"state_revision":61,"expected_ski":"`+expectedSKI+`"}`)
		}
		return value
	}
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request(ski))
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request(ski))
	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("exact replay status first/second=%d/%d bodies=%s / %s", first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	var firstEnvelope, secondEnvelope struct {
		StateRevision uint64 `json:"state_revision"`
		Data          struct {
			SelectionID string `json:"selection_id"`
			Replayed    bool   `json:"replayed"`
		} `json:"data"`
	}
	if json.Unmarshal(first.Body.Bytes(), &firstEnvelope) != nil || json.Unmarshal(second.Body.Bytes(), &secondEnvelope) != nil ||
		firstEnvelope.StateRevision != 62 || secondEnvelope.StateRevision != 62 || firstEnvelope.Data.SelectionID == "" ||
		firstEnvelope.Data.SelectionID != secondEnvelope.Data.SelectionID || !secondEnvelope.Data.Replayed {
		t.Fatalf("exact replay envelopes first=%#v second=%#v", firstEnvelope, secondEnvelope)
	}
	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, request(strings.Repeat("b", 40)))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed binding replay status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	assertIssue809ErrorEnvelope(t, conflict.Body.String(), "idempotency_conflict")
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.selectCalls) != 1 {
		t.Fatalf("HTTP replay executed select %d times, want exactly one", len(admin.selectCalls))
	}
}

func TestIssue809CandidateIdentityIsOwnerOnlyNoStoreAndCurrentSessionBound(t *testing.T) {
	const ski = "fedcba9876543210fedcba9876543210fedcba98"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 21
	snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute), AssociationComplete: true}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{
		eebusruntime.AdminViewV1Candidate: snapshot,
	}}
	handler := newIssue809Server(t, admin)
	cookie, csrf := issue809OwnerSession(t, handler)

	candidate := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=candidate", nil)
	candidate.AddCookie(cookie)
	candidateResponse := httptest.NewRecorder()
	handler.ServeHTTP(candidateResponse, candidate)
	if candidateResponse.Code != http.StatusOK || candidateResponse.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("candidate status/cache=%d/%q body=%s", candidateResponse.Code, candidateResponse.Header().Get("Cache-Control"), candidateResponse.Body.String())
	}
	if !strings.Contains(candidateResponse.Body.String(), ski) || strings.Contains(candidateResponse.Body.String(), "candidate_ref") {
		t.Fatalf("candidate response disclosure shape=%s", candidateResponse.Body.String())
	}

	confirm := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/candidate:confirm", cookie, csrf, "confirm-1", `{"state_revision":21,"expected_ski":"`+ski+`"}`)
	confirmResponse := httptest.NewRecorder()
	handler.ServeHTTP(confirmResponse, confirm)
	if confirmResponse.Code != http.StatusOK {
		t.Fatalf("confirm status=%d body=%s", confirmResponse.Code, confirmResponse.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.confirmCalls) != 1 || admin.confirmCalls[0].ExpectedSKI != ski {
		t.Fatalf("confirm calls=%#v", admin.confirmCalls)
	}
}

func TestIssue809OwnerLifecycleMutationsUseOnlyCurrentServerHeldCapabilities(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	for _, mutation := range []struct {
		method, target, key string
	}{
		{http.MethodPost, ":retry", "retry-1"},
		{http.MethodDelete, "/trust", "untrust-1"},
	} {
		t.Run(mutation.key, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 31
			snapshot.Trusted = []eebusruntime.TrustedPartnerV1{{SKI: ski, SHIPID: "remote-service", TrustState: "durably_trusted", LastSeen: snapshot.CapturedAt}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Trusted: snapshot}}
			handler := newIssue809Server(t, admin)
			cookie, csrf := issue809OwnerSession(t, handler)
			trusted := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=trusted", nil)
			trusted.AddCookie(cookie)
			trustedResponse := httptest.NewRecorder()
			handler.ServeHTTP(trustedResponse, trusted)
			var envelope struct {
				Data struct {
					Partners []struct {
						PartnerID string `json:"partner_id"`
					} `json:"partners"`
				} `json:"data"`
			}
			if trustedResponse.Code != http.StatusOK || json.Unmarshal(trustedResponse.Body.Bytes(), &envelope) != nil || len(envelope.Data.Partners) != 1 || envelope.Data.Partners[0].PartnerID == "" {
				t.Fatalf("trusted status=%d body=%s", trustedResponse.Code, trustedResponse.Body.String())
			}
			target := "/admin/eebus/v1/partners/" + envelope.Data.Partners[0].PartnerID + mutation.target
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue809OwnerMutation(t, mutation.method, target, cookie, csrf, mutation.key, `{"state_revision":31}`))
			if response.Code != http.StatusOK {
				t.Fatalf("%s %s status=%d body=%s", mutation.method, target, response.Code, response.Body.String())
			}
			admin.mu.Lock()
			defer admin.mu.Unlock()
			if mutation.key == "retry-1" && len(admin.retryCalls) != 1 {
				t.Fatalf("retry calls=%d", len(admin.retryCalls))
			}
			if mutation.key == "untrust-1" && len(admin.untrustCalls) != 1 {
				t.Fatalf("untrust calls=%d", len(admin.untrustCalls))
			}
		})
	}

	for _, mutation := range []struct {
		target, key string
	}{
		{"/admin/eebus/v1/candidate:cancel", "cancel-1"},
		{"/admin/eebus/v1/pairing-window:close", "close-1"},
	} {
		t.Run(mutation.key, func(t *testing.T) {
			snapshot := testAdminSnapshot()
			snapshot.StateRevision = 31
			snapshot.Candidates = []eebusruntime.CandidateV1{{SKI: ski, State: "tls_bound", ExpiresAt: snapshot.CapturedAt.Add(time.Minute), AssociationComplete: true}}
			admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Candidate: snapshot}}
			handler := newIssue809Server(t, admin)
			cookie, csrf := issue809OwnerSession(t, handler)
			if mutation.key == "cancel-1" {
				candidate := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=candidate", nil)
				candidate.AddCookie(cookie)
				candidateResponse := httptest.NewRecorder()
				handler.ServeHTTP(candidateResponse, candidate)
				if candidateResponse.Code != http.StatusOK {
					t.Fatalf("candidate status=%d body=%s", candidateResponse.Code, candidateResponse.Body.String())
				}
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, issue809OwnerMutation(t, http.MethodPost, mutation.target, cookie, csrf, mutation.key, `{"state_revision":31}`))
			if response.Code != http.StatusOK {
				t.Fatalf("POST %s status=%d body=%s", mutation.target, response.Code, response.Body.String())
			}
			admin.mu.Lock()
			defer admin.mu.Unlock()
			if mutation.key == "cancel-1" && len(admin.cancelCalls) != 1 {
				t.Fatalf("cancel calls=%d", len(admin.cancelCalls))
			}
			if mutation.key == "close-1" && len(admin.closeCalls) != 1 {
				t.Fatalf("close calls=%d", len(admin.closeCalls))
			}
		})
	}
}

func TestIssue809ConnectedBrowseCapabilityCannotAuthorizeTrustMutation(t *testing.T) {
	const ski = "0123456789abcdef0123456789abcdef01234567"
	snapshot := testAdminSnapshot()
	snapshot.StateRevision = 51
	snapshot.Connected = []eebusruntime.ConnectedPartnerV1{{SKI: ski, TrustState: "durably_trusted", ConnectionState: "connected", Endpoint: "192.0.2.20:4712"}}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Connected: snapshot}}
	handler := newIssue809Server(t, admin)
	cookie, csrf := issue809OwnerSession(t, handler)
	connected := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=connected", nil)
	connected.AddCookie(cookie)
	connectedResponse := httptest.NewRecorder()
	handler.ServeHTTP(connectedResponse, connected)
	var envelope struct {
		Data struct {
			Partners []struct {
				PartnerID string `json:"partner_id"`
			} `json:"partners"`
		} `json:"data"`
	}
	if connectedResponse.Code != http.StatusOK || json.Unmarshal(connectedResponse.Body.Bytes(), &envelope) != nil || len(envelope.Data.Partners) != 1 {
		t.Fatalf("connected status=%d body=%s", connectedResponse.Code, connectedResponse.Body.String())
	}
	mutation := issue809OwnerMutation(t, http.MethodPost, "/admin/eebus/v1/partners/"+envelope.Data.Partners[0].PartnerID+":retry", cookie, csrf, "connected-cannot-retry", `{"state_revision":51}`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, mutation)
	if response.Code != http.StatusConflict {
		t.Fatalf("connected capability retry status=%d body=%s", response.Code, response.Body.String())
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if len(admin.retryCalls) != 0 {
		t.Fatalf("connected browse capability reached retry runtime %d times", len(admin.retryCalls))
	}
}

func TestIssue809MultipleCurrentCandidatesFailClosedWithoutPartialRows(t *testing.T) {
	snapshot := testAdminSnapshot()
	snapshot.Candidates = []eebusruntime.CandidateV1{
		{SKI: strings.Repeat("a", 40), State: "tls_bound"},
		{SKI: strings.Repeat("b", 40), State: "tls_bound"},
	}
	admin := &adminV1Stub{snapshot: snapshot, snapshots: map[eebusruntime.AdminViewV1]eebusruntime.AdminSnapshotV1{eebusruntime.AdminViewV1Candidate: snapshot}}
	handler := newIssue809Server(t, admin)
	cookie, _ := issue809OwnerSession(t, handler)
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/partners?view=candidate", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), strings.Repeat("a", 40)) || strings.Contains(response.Body.String(), strings.Repeat("b", 40)) {
		t.Fatalf("multiple-candidate response status=%d body=%s", response.Code, response.Body.String())
	}
}

func newIssue809Server(t *testing.T, admin eebusruntime.AdminV1) http.Handler {
	t.Helper()
	handler, err := NewServer(Config{
		Admin: admin,
		Auth: AuthConfig{
			OwnerUsername: "owner",
			OwnerSecret:   []byte(testOwnerSecret),
			HASecret:      []byte(testHASecret),
			OwnerOrigin:   testOwnerOrigin,
			SessionTTL:    15 * time.Minute,
			Now:           func() time.Time { return time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC) },
			Random:        rand.Reader,
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return handler
}

func issue809OwnerSession(t *testing.T, handler http.Handler) (*http.Cookie, string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.SetBasicAuth("owner", testOwnerSecret)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 1 || response.Header().Get("X-CSRF-Token") == "" {
		t.Fatalf("owner session status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	return response.Result().Cookies()[0], response.Header().Get("X-CSRF-Token")
}

func issue809OwnerMutation(t *testing.T, method, path string, cookie *http.Cookie, csrf, idempotencyKey, body string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.AddCookie(cookie)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("Origin", testOwnerOrigin)
	request.Header.Set("Referer", testOwnerOrigin+"/portal/eebus")
	request.Header.Set("X-CSRF-Token", csrf)
	return request
}

func testAdminSnapshot() eebusruntime.AdminSnapshotV1 {
	return eebusruntime.AdminSnapshotV1{
		StateRevision:   7,
		CapturedAt:      time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC),
		Status:          "ready",
		Window:          "open",
		WindowDeadline:  time.Date(2026, 8, 14, 8, 5, 0, 0, time.UTC),
		Register:        "true",
		Listener:        "ready",
		Discovery:       "ready",
		TrustedCount:    1,
		ConnectedCount:  1,
		DiscoveredCount: 1,
		CandidateCount:  1,
	}
}

func assertIssue809ErrorEnvelope(t *testing.T, body string, code string) {
	t.Helper()
	var envelope map[string]any
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%q", err, body)
	}
	if envelope["contract"] != "helianthus.eebus.operator-admin.v1" {
		t.Fatalf("contract=%v", envelope["contract"])
	}
	errorObject, ok := envelope["error"].(map[string]any)
	if !ok || errorObject["code"] != code {
		t.Fatalf("error=%#v, want code %q", envelope["error"], code)
	}
	if data, exists := envelope["data"]; exists && data != nil {
		t.Fatalf("error response contains data: %#v", data)
	}
}
