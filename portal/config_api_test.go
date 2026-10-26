package portal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
)

func TestM26ConfigAPI(t *testing.T) {
	initial := portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}}}
	store, e := portalconfig.Open(t.TempDir(), initial, portalconfig.Constraints{Drivers: []string{"family.primary"}, GraphQL: true})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	}()
	h := NewHandler(Options{BasePath: "/portal/", ExternalPrefix: "/embedded", Config: &ConfigCallbacks{
		Read:   func(context.Context) (portalconfig.Record, error) { return store.Read() },
		Update: func(_ context.Context, r portalconfig.Record) (portalconfig.Record, error) { return store.Update(r) },
	}})
	request := func(method, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/embedded/portal/api/v1/config", strings.NewReader(body)))
		return w
	}
	w := request(http.MethodGet, "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	initial.Sharing.GraphQL = true
	b, _ := json.Marshal(initial)
	w = request(http.MethodPut, string(b))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var view struct {
		Desired portalconfig.Record `json:"desired"`
		State   struct {
			Sharing struct {
				GraphQL configSelection `json:"graphql"`
			} `json:"sharing"`
		} `json:"state"`
	}
	if e = json.Unmarshal(w.Body.Bytes(), &view); e != nil {
		t.Fatal(e)
	}
	state := view.State.Sharing.GraphQL
	if view.Desired.Revision != "1" || !state.Desired || state.Effective || state.Status != "withheld" || state.Reason != "evidence_unavailable" {
		t.Fatal("save falsely claimed runtime application", w.Body.String())
	}
	w = request(http.MethodPut, string(b))
	if w.Code != http.StatusConflict {
		t.Fatal("stale update accepted")
	}
	for _, body := range []string{`{"private_marker":true}`, strings.Repeat("x", portalconfig.MaxBytes+1), strings.Replace(string(b), `"graphql":true`, `"graphql":null`, 1)} {
		w = request(http.MethodPut, body)
		if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "private_marker") {
			t.Fatal("invalid input not bounded", w.Code, w.Body.String())
		}
	}
	w = request(http.MethodPost, "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatal(w.Code)
	}
}

func TestM26ConfigUnavailablePreservesShellAndRedacts(t *testing.T) {
	for _, callbacks := range []*ConfigCallbacks{nil, {Read: func(context.Context) (portalconfig.Record, error) {
		return portalconfig.Record{}, errors.New("private_storage_marker")
	}},
		{Read: func(context.Context) (portalconfig.Record, error) {
			return portalconfig.Record{}, &portalconfig.Error{Code: "invalid_record", Field: "private_field_marker", NextStep: "private_step_marker"}
		}}} {
		h := NewHandler(Options{Config: callbacks})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
		if w.Code != 503 && w.Code != 400 {
			t.Fatal(w.Code)
		}
		var unavailable struct {
			Desired *portalconfig.Record `json:"desired"`
			State   struct {
				AdapterProxy struct {
					Desired   *bool  `json:"desired"`
					Effective bool   `json:"effective"`
					Status    string `json:"status"`
				} `json:"adapter_proxy"`
			} `json:"state"`
		}
		if e := json.Unmarshal(w.Body.Bytes(), &unavailable); e != nil {
			t.Fatal(e)
		}
		if unavailable.Desired != nil || unavailable.State.AdapterProxy.Desired != nil || unavailable.State.AdapterProxy.Effective || unavailable.State.AdapterProxy.Status != "error" {
			t.Fatal("cold failure has no unavailable state", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private_") {
			t.Fatal("raw error leaked")
		}
		shell := httptest.NewRecorder()
		h.ServeHTTP(shell, httptest.NewRequest(http.MethodGet, "/", nil))
		if shell.Code != 200 {
			t.Fatal("shell unavailable", shell.Code)
		}
	}
}

func TestM26ConfigHistoricalDesiredOnLoadError(t *testing.T) {
	record := portalconfig.Record{Format: portalconfig.Format, Revision: "3", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}}, Sharing: portalconfig.Sharing{GraphQL: true}}
	h := NewHandler(Options{Config: &ConfigCallbacks{Read: func(context.Context) (portalconfig.Record, error) {
		return record, &portalconfig.Error{Code: "invalid_record"}
	}}})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
	var got struct {
		Historical bool                `json:"historical"`
		Desired    portalconfig.Record `json:"desired"`
		Error      *portalconfig.Error `json:"error"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if w.Code != 400 || !got.Historical || got.Desired.Revision != "3" || !got.Desired.Sharing.GraphQL || got.Error == nil {
		t.Fatal("historical selection lost", w.Body.String())
	}
}

type configFailingBody struct{}

func (configFailingBody) Read([]byte) (int, error) { return 0, errors.New("private_body_marker") }

func TestM26InvalidPutPreservesHistoricalDesired(t *testing.T) {
	accepted := portalconfig.Record{Format: portalconfig.Format, Revision: "3", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}}, Sharing: portalconfig.Sharing{GraphQL: true}}
	for _, state := range []string{"accepted", "historical", "unavailable"} {
		for _, body := range []string{"malformed", "oversized", "read_error"} {
			t.Run(state+"/"+body, func(t *testing.T) {
				reads, updates := 0, 0
				h := NewHandler(Options{Config: &ConfigCallbacks{
					Read: func(context.Context) (portalconfig.Record, error) {
						reads++
						if state == "unavailable" {
							return portalconfig.Record{}, errors.New("private_storage_marker")
						}
						if state == "historical" {
							return accepted, errors.New("private_storage_marker")
						}
						return accepted, nil
					},
					Update: func(context.Context, portalconfig.Record) (portalconfig.Record, error) {
						updates++
						return portalconfig.Record{}, nil
					},
				}})
				var input io.Reader = strings.NewReader(`{"private_body_marker":true}`)
				if body == "oversized" {
					input = strings.NewReader(strings.Repeat("x", portalconfig.MaxBytes+1))
				}
				if body == "read_error" {
					input = configFailingBody{}
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/config", input))
				var got struct {
					Desired    *portalconfig.Record `json:"desired"`
					Historical bool                 `json:"historical"`
					Error      *portalconfig.Error  `json:"error"`
					State      struct {
						Sharing struct {
							GraphQL configSelection `json:"graphql"`
						} `json:"sharing"`
					} `json:"state"`
				}
				if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
					t.Fatal(e)
				}
				if w.Code != http.StatusBadRequest || got.Error == nil || got.Error.Code != "invalid_record" || reads != 1 || updates != 0 || strings.Contains(w.Body.String(), "private_") {
					t.Fatal("invalid PUT handling", reads, updates, w.Code, w.Body.String())
				}
				if state == "unavailable" {
					if got.Desired != nil || got.Historical {
						t.Fatal("cold failure invented history", w.Body.String())
					}
				} else if got.Desired == nil || got.Desired.Revision != accepted.Revision || !got.Desired.Sharing.GraphQL || !got.Historical || !got.State.Sharing.GraphQL.Desired || got.State.Sharing.GraphQL.Effective || got.State.Sharing.GraphQL.Status != "withheld" {
					t.Fatal("historical selection lost or applied", w.Body.String())
				}
			})
		}
	}
}

type configSelection struct {
	Desired   bool   `json:"desired"`
	Effective bool   `json:"effective"`
	Status    string `json:"status"`
	Reason    string `json:"reason"`
}

// TestM28ConfigDriverStatesComeFromHost: host driver rows replace the
// evidence_unavailable defaults for accepted, historical and missing records,
// and a host answer without rows still serializes an empty list.
func TestM28ConfigDriverStatesComeFromHost(t *testing.T) {
	accepted := portalconfig.Record{Format: portalconfig.Format, Revision: "2", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}}}
	enabled := true
	for _, tc := range []struct {
		name     string
		readErr  error
		record   portalconfig.Record
		nilRows  bool
		wantSeen string
		want     string
	}{
		{name: "accepted", record: accepted, wantSeen: "2", want: `[{"driver_id":"family.primary","desired":true,"effective":true,"status":"effective","reason":"running"}]`},
		{name: "historical", readErr: &portalconfig.Error{Code: "invalid_record"}, record: accepted, wantSeen: "2", want: `[{"driver_id":"family.primary","desired":true,"effective":true,"status":"effective","reason":"running"}]`},
		{name: "missing", readErr: &portalconfig.Error{Code: "invalid_record"}, wantSeen: "none", want: `[{"driver_id":"family.primary","desired":true,"effective":true,"status":"effective","reason":"running"}]`},
		{name: "no_rows", record: accepted, nilRows: true, wantSeen: "2", want: `[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := ""
			h := NewHandler(Options{Config: &ConfigCallbacks{
				Read: func(context.Context) (portalconfig.Record, error) { return tc.record, tc.readErr },
				Drivers: func(_ context.Context, r *portalconfig.Record) []ConfigDriverState {
					seen = "none"
					if r != nil {
						seen = r.Revision
					}
					if tc.nilRows {
						return nil
					}
					return []ConfigDriverState{{DriverID: "family.primary", Desired: &enabled, Effective: true, Status: "effective", Reason: "running"}}
				},
			}})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
			var got struct {
				State struct {
					Drivers json.RawMessage `json:"drivers"`
				} `json:"state"`
			}
			if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
				t.Fatal(e)
			}
			if seen != tc.wantSeen || string(got.State.Drivers) != tc.want {
				t.Fatalf("host saw %q, rows %s; want %q, %s", seen, got.State.Drivers, tc.wantSeen, tc.want)
			}
		})
	}
	// Without a host callback, rows never claim application.
	h := NewHandler(Options{Config: &ConfigCallbacks{Read: func(context.Context) (portalconfig.Record, error) { return accepted, nil }}})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
	if !strings.Contains(w.Body.String(), `"drivers":[{"driver_id":"family.primary","desired":true,"effective":false,"status":"withheld","reason":"evidence_unavailable"}]`) {
		t.Fatalf("default rows changed: %s", w.Body.String())
	}
}

// Issue #1375: a driver row carries the recovery schedule only while the host
// reports the driver in backoff. The member is closed to the four members of
// the MCP runtime tools' recovery object, and it is absent, never null, in every
// other row.
func TestIssue1375ConfigDriverRecoveryIsOptionalAndClosed(t *testing.T) {
	accepted := portalconfig.Record{Format: portalconfig.Format, Revision: "2", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}, {ID: "family.secondary", Enabled: true}}}
	enabled := true
	h := NewHandler(Options{Config: &ConfigCallbacks{
		Read: func(context.Context) (portalconfig.Record, error) { return accepted, nil },
		Drivers: func(context.Context, *portalconfig.Record) []ConfigDriverState {
			return []ConfigDriverState{
				{DriverID: "family.primary", Desired: &enabled, Status: "pending", Reason: "backoff",
					Recovery: &ConfigDriverRecovery{Attempt: 3, NextAttemptAt: "2000-01-01T00:00:08Z", LastFailureClass: "DIAL_FAILED", LastFailureCause: "dial: connection refused"}},
				{DriverID: "family.secondary", Desired: &enabled, Effective: true, Status: "effective", Reason: "running"},
			}
		},
	}})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
	var got struct {
		State struct {
			Drivers json.RawMessage `json:"drivers"`
		} `json:"state"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	const want = `[{"driver_id":"family.primary","desired":true,"effective":false,"status":"pending","reason":"backoff",` +
		`"recovery":{"attempt":3,"next_attempt_at":"2000-01-01T00:00:08Z","last_failure_class":"DIAL_FAILED","last_failure_cause":"dial: connection refused"}},` +
		`{"driver_id":"family.secondary","desired":true,"effective":true,"status":"effective","reason":"running"}]`
	if string(got.State.Drivers) != want {
		t.Fatalf("rows = %s\nwant %s", got.State.Drivers, want)
	}
}
