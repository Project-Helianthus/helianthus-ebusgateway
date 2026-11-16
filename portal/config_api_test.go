package portal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
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

// operatorRecordFixture is a synthetic section with one record of each form.
func operatorRecordFixture() portalconfig.OperatorRecordSet {
	definition := func(id string) *semreg.DefinitionRef {
		return &semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}, ID: semreg.DefinitionID(id), Version: "1.0.0"}
	}
	return portalconfig.OperatorRecordSet{Format: portalconfig.OperatorRecordsFormat, Revision: "0", Records: []portalconfig.OperatorRecord{
		{Form: portalconfig.OperatorFormAnchor, Home: "asset:portal-opr-home", ResourceID: "resource:portal-opr:array-1", Kind: "resource.pv_array", Slot: "array-south"},
		{Form: portalconfig.OperatorFormRelation, Home: "asset:portal-opr-inverter", Definition: definition("pv.relation.inverter_array"), From: "resource:portal-opr:inverter-1", To: "resource:portal-opr:array-1", BasisClass: "configuration"},
		{Form: portalconfig.OperatorFormPairing, Kind: "resource.pv_system", Slot: "system-main", Views: []portalconfig.OperatorView{
			{Home: "asset:portal-opr-home", ResourceID: "resource:portal-opr:system-a"},
			{Home: "asset:portal-opr-other", ResourceID: "resource:portal-opr:system-b"},
		}},
	}}
}

type operatorRecordView struct {
	Desired    *portalconfig.OperatorRecordSet `json:"desired"`
	Historical bool                            `json:"historical"`
	Error      *portalconfig.Error             `json:"error"`
	State      struct {
		Records []portalconfig.OperatorRecordState `json:"records"`
	} `json:"state"`
}

// The operator-record section is read, replaced and reported under the
// configuration route; the configuration view carries it only when it holds
// records, and a refusal names its field.
func TestOperatorRecordConfigAPIReadReplaceAndStatus(t *testing.T) {
	initial := portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{{ID: "family.primary", Enabled: true}}}
	store, e := portalconfig.Open(t.TempDir(), initial, portalconfig.Constraints{Drivers: []string{"family.primary"}})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = store.Close() }()
	outcomes := []portalconfig.OperatorRecordState{
		{Status: portalconfig.OperatorRecordPublished},
		{Status: portalconfig.OperatorRecordWaiting, Reason: "home_not_declared"},
		{Status: portalconfig.OperatorRecordRefused, Reason: "identity_drift"},
	}
	h := NewHandler(Options{Config: &ConfigCallbacks{
		Read:   func(context.Context) (portalconfig.Record, error) { return store.Read() },
		Update: func(_ context.Context, r portalconfig.Record) (portalconfig.Record, error) { return store.Update(r) },
		OperatorRecords: &ConfigOperatorRecords{
			Read: func(context.Context) (portalconfig.OperatorRecordSet, error) { return store.OperatorRecords() },
			Replace: func(_ context.Context, set portalconfig.OperatorRecordSet) (portalconfig.OperatorRecordSet, error) {
				return store.ReplaceOperatorRecords(set)
			},
			States: func(_ context.Context, set portalconfig.OperatorRecordSet) []portalconfig.OperatorRecordState {
				states := []portalconfig.OperatorRecordState{}
				for i, record := range set.Records {
					state := outcomes[i%len(outcomes)]
					state.Index, state.Form = i, record.Form
					states = append(states, state)
				}
				return states
			},
		},
	}})
	request := func(method, target, body string) (*httptest.ResponseRecorder, operatorRecordView) {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, target, strings.NewReader(body)))
		var view operatorRecordView
		if w.Code != http.StatusMethodNotAllowed {
			if e := json.Unmarshal(w.Body.Bytes(), &view); e != nil {
				t.Fatalf("%s %s: %v %s", method, target, e, w.Body.String())
			}
		}
		return w, view
	}
	const section = "/api/v1/config?section=operator_records"

	w, view := request(http.MethodGet, section, "")
	if w.Code != 200 || view.Desired == nil || view.Desired.Revision != "0" || len(view.Desired.Records) != 0 || len(view.State.Records) != 0 || view.Error != nil {
		t.Fatal("empty section", w.Code, w.Body.String())
	}
	w, _ = request(http.MethodGet, "/api/v1/config", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "operator_records") {
		t.Fatal("an empty section is not omitted from the configuration view", w.Body.String())
	}

	body, _ := json.Marshal(operatorRecordFixture())
	w, view = request(http.MethodPut, section, string(body))
	if w.Code != 200 || view.Desired == nil || view.Desired.Revision != "1" || len(view.Desired.Records) != 3 {
		t.Fatal("replace", w.Code, w.Body.String())
	}
	want := []portalconfig.OperatorRecordState{
		{Index: 0, Form: "anchor", Status: "published"},
		{Index: 1, Form: "relation", Status: "waiting", Reason: "home_not_declared"},
		{Index: 2, Form: "pairing", Status: "refused", Reason: "identity_drift"},
	}
	if !reflect.DeepEqual(view.State.Records, want) {
		t.Fatalf("states %+v, want %+v", view.State.Records, want)
	}
	var config struct {
		OperatorRecords operatorRecordView `json:"operator_records"`
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
	if e := json.Unmarshal(w.Body.Bytes(), &config); e != nil || w.Code != 200 {
		t.Fatal(e, w.Code)
	}
	if config.OperatorRecords.Desired == nil || config.OperatorRecords.Desired.Revision != "1" || !reflect.DeepEqual(config.OperatorRecords.State.Records, want) {
		t.Fatal("the configuration view does not carry the section", w.Body.String())
	}

	refused := operatorRecordFixture()
	refused.Revision = "1"
	refused.Records[1].Definition.ID = "pv.relation.inverter_meter"
	body, _ = json.Marshal(refused)
	w, view = request(http.MethodPut, section, string(body))
	if w.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "relation_definition_unknown" || view.Error.Field != "records[1].definition" || view.Error.NextStep == "" {
		t.Fatal("refusal without its field", w.Code, w.Body.String())
	}
	if !view.Historical || view.Desired == nil || view.Desired.Revision != "1" || len(view.State.Records) != 3 {
		t.Fatal("a refusal hides the accepted section", w.Body.String())
	}

	stale, _ := json.Marshal(operatorRecordFixture())
	w, view = request(http.MethodPut, section, string(stale))
	if w.Code != http.StatusConflict || view.Error == nil || view.Error.Code != "revision_conflict" {
		t.Fatal("stale replace", w.Code, w.Body.String())
	}
	for _, malformed := range []string{`{"private_marker":true}`, strings.Repeat("x", portalconfig.MaxBytes+1)} {
		w, view = request(http.MethodPut, section, malformed)
		if w.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "invalid_record" || strings.Contains(w.Body.String(), "private_marker") {
			t.Fatal("malformed section", w.Code, w.Body.String())
		}
	}
	for _, target := range []string{"/api/v1/config?section=drivers", "/api/v1/config?section=operator_records&section=operator_records"} {
		w, view = request(http.MethodGet, target, "")
		if w.Code != http.StatusBadRequest || view.Error == nil || view.Error.Code != "invalid_record" || view.Error.Field != "section" {
			t.Fatal("unknown section", target, w.Code, w.Body.String())
		}
	}
	w, _ = request(http.MethodPost, section, string(body))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatal(w.Code)
	}
	if read, e := store.OperatorRecords(); e != nil || read.Revision != "1" {
		t.Fatal("refused requests changed the section", read, e)
	}
}

// Without host callbacks the section is unavailable and never claims
// publication; without a status callback every record waits.
func TestOperatorRecordConfigAPIWithholdsWithoutHostCallbacks(t *testing.T) {
	record := portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{}}
	read := func(context.Context) (portalconfig.Record, error) { return record, nil }
	h := NewHandler(Options{Config: &ConfigCallbacks{Read: read}})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config?section=operator_records", nil))
	var view operatorRecordView
	if e := json.Unmarshal(w.Body.Bytes(), &view); e != nil || w.Code != http.StatusServiceUnavailable || view.Error == nil || view.Error.Code != "persistence_failed" || view.Desired != nil {
		t.Fatal("unavailable section", w.Code, w.Body.String())
	}
	set := operatorRecordFixture()
	h = NewHandler(Options{Config: &ConfigCallbacks{Read: read, OperatorRecords: &ConfigOperatorRecords{
		Read: func(context.Context) (portalconfig.OperatorRecordSet, error) { return set, nil },
	}}})
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config?section=operator_records", nil))
	view = operatorRecordView{}
	if e := json.Unmarshal(w.Body.Bytes(), &view); e != nil || w.Code != 200 || len(view.State.Records) != 3 {
		t.Fatal(e, w.Code, w.Body.String())
	}
	for i, state := range view.State.Records {
		if state.Index != i || state.Status != portalconfig.OperatorRecordWaiting || state.Reason != "evidence_unavailable" {
			t.Fatal("a record claims publication without host status", w.Body.String())
		}
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/v1/config?section=operator_records", strings.NewReader("{}")))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("replace without a host callback", w.Code, w.Body.String())
	}
	failing := NewHandler(Options{Config: &ConfigCallbacks{Read: read, OperatorRecords: &ConfigOperatorRecords{
		Read: func(context.Context) (portalconfig.OperatorRecordSet, error) {
			return portalconfig.OperatorRecordSet{}, &portalconfig.Error{Code: "private_code_marker", Field: "private_field_marker"}
		},
	}}})
	w = httptest.NewRecorder()
	failing.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/config", nil))
	if w.Code != 200 || strings.Contains(w.Body.String(), "private_") || !strings.Contains(w.Body.String(), `"operator_records":{"desired":null,"error":{"code":"persistence_failed"`) {
		t.Fatal("a section failure is hidden or leaks", w.Code, w.Body.String())
	}
}
