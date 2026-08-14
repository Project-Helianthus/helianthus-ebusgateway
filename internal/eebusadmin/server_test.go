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
	snapshotCalls int
	openCalls     int
}

func (stub *adminV1Stub) Snapshot(context.Context, eebusruntime.AdminSnapshotRequestV1) (eebusruntime.AdminSnapshotV1, *eebusruntime.AdminErrorV1) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.snapshotCalls++
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

func (*adminV1Stub) ClosePairingWindow(context.Context, eebusruntime.ClosePairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected ClosePairingWindow")
}

func (*adminV1Stub) Select(context.Context, eebusruntime.SelectRequestV1) (eebusruntime.AdminSelectionResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Select")
}

func (*adminV1Stub) Connect(context.Context, eebusruntime.ConnectRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Connect")
}

func (*adminV1Stub) Confirm(context.Context, eebusruntime.ConfirmRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Confirm")
}

func (*adminV1Stub) Cancel(context.Context, eebusruntime.CancelRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Cancel")
}

func (*adminV1Stub) RetryTrusted(context.Context, eebusruntime.RetryTrustedRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected RetryTrusted")
}

func (*adminV1Stub) Untrust(context.Context, eebusruntime.UntrustRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Untrust")
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
