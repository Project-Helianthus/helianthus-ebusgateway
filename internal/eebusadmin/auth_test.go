package eebusadmin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type issue809FailingReader struct{}

func (issue809FailingReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}

func TestIssue809OwnerBasicAndCookieReuseOneBoundedSession(t *testing.T) {
	auth, err := newAuthentication(issue809AuthConfig())
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	firstRequest.SetBasicAuth("owner", testOwnerSecret)
	firstResponse := httptest.NewRecorder()
	identity, failure := auth.authenticate(firstResponse, firstRequest)
	if failure != "" || identity.session.id == "" || len(firstResponse.Result().Cookies()) != 1 {
		t.Fatalf("first auth identity/failure/cookies=%#v/%q/%#v", identity, failure, firstResponse.Result().Cookies())
	}

	for index := 0; index < maxOwnerSessions+2; index++ {
		request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
		request.SetBasicAuth("owner", testOwnerSecret)
		request.AddCookie(firstResponse.Result().Cookies()[0])
		response := httptest.NewRecorder()
		got, gotFailure := auth.authenticate(response, request)
		if gotFailure != "" || got.session.id != identity.session.id {
			t.Fatalf("reuse %d identity/failure=%#v/%q", index, got, gotFailure)
		}
	}
	auth.mu.Lock()
	defer auth.mu.Unlock()
	if len(auth.sessions) != 1 {
		t.Fatalf("sessions=%d, want 1", len(auth.sessions))
	}
}

func TestIssue809HABearerRejectsBrowserAuthorityAndBadEntropyFailsClosed(t *testing.T) {
	auth, err := newAuthentication(issue809AuthConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*http.Request){
		func(request *http.Request) { request.AddCookie(&http.Cookie{Name: "unrelated", Value: "cookie"}) },
		func(request *http.Request) { request.Header.Set("Origin", testOwnerOrigin) },
		func(request *http.Request) { request.Header.Set("Referer", testOwnerOrigin+"/portal/eebus") },
	} {
		request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
		request.Header.Set("Authorization", "Bearer "+testHASecret)
		mutate(request)
		if _, failure := auth.authenticate(httptest.NewRecorder(), request); failure != "forbidden" {
			t.Fatalf("HA browser authority failure=%q, want forbidden", failure)
		}
	}

	config := issue809AuthConfig()
	config.Random = issue809FailingReader{}
	badEntropy, err := newAuthentication(config)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.SetBasicAuth("owner", testOwnerSecret)
	if _, failure := badEntropy.authenticate(httptest.NewRecorder(), request); failure != "admin_boundary_unavailable" {
		t.Fatalf("bad entropy failure=%q", failure)
	}
}

func TestIssue809ExpiredOwnerSessionCannotAuthorizeMutation(t *testing.T) {
	now := time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC)
	config := issue809AuthConfig()
	config.Now = func() time.Time { return now }
	config.SessionTTL = time.Minute
	auth, err := newAuthentication(config)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.SetBasicAuth("owner", testOwnerSecret)
	response := httptest.NewRecorder()
	identity, failure := auth.authenticate(response, request)
	if failure != "" {
		t.Fatal(failure)
	}
	now = now.Add(time.Minute)
	expired := httptest.NewRequest(http.MethodPost, "/admin/eebus/v1/pairing-window:close", strings.NewReader(`{"state_revision":1}`))
	expired.AddCookie(&http.Cookie{Name: ownerSessionCookie, Value: identity.session.id})
	if _, expiredFailure := auth.authenticate(httptest.NewRecorder(), expired); expiredFailure != "unauthenticated" {
		t.Fatalf("expired failure=%q", expiredFailure)
	}
}

func TestIssue809AuthenticationRejectsHeaderUnsafeCredentials(t *testing.T) {
	for _, mutate := range []func(*AuthConfig){
		func(config *AuthConfig) { config.OwnerUsername = "owner name" },
		func(config *AuthConfig) { config.OwnerUsername = "owner:name" },
		func(config *AuthConfig) { config.OwnerSecret = []byte("owner-secret-\u00e9") },
		func(config *AuthConfig) { config.HASecret = []byte("ha-secret-value ") },
	} {
		config := issue809AuthConfig()
		mutate(&config)
		if _, err := newAuthentication(config); err == nil {
			t.Fatal("header-unsafe authentication config accepted")
		}
	}
}

func TestIssue809HTTPSOwnerOriginAlwaysIssuesSecureCookie(t *testing.T) {
	config := issue809AuthConfig()
	config.OwnerOrigin = "https://gateway.example.test"
	auth, err := newAuthentication(config)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/eebus/v1/status", nil)
	request.SetBasicAuth("owner", testOwnerSecret)
	response := httptest.NewRecorder()
	if _, failure := auth.authenticate(response, request); failure != "" {
		t.Fatal(failure)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Fatalf("HTTPS owner origin cookie=%#v, want Secure", cookies)
	}
}

func issue809AuthConfig() AuthConfig {
	return AuthConfig{
		OwnerUsername: "owner", OwnerSecret: []byte(testOwnerSecret), HASecret: []byte(testHASecret),
		OwnerOrigin: testOwnerOrigin, SessionTTL: 15 * time.Minute,
		Now:    func() time.Time { return time.Date(2026, 8, 14, 8, 0, 0, 0, time.UTC) },
		Random: strings.NewReader(strings.Repeat("x", 4096)),
	}
}
