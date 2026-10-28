package portalconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func o10Record() Record {
	return Record{Format: Format, Revision: "37", Drivers: []Driver{{ID: "o10-driver-marker", Enabled: true}}, Sharing: Sharing{GraphQL: true, MCP: true}, AdapterProxy: true, OutputBindings: Outputs{Matter: true}}
}

func o10JSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestO10SafeSelections(t *testing.T) {
	for bits := 0; bits < 16; bits++ {
		t.Run(fmt.Sprint(bits), func(t *testing.T) {
			r := o10Record()
			r.Sharing = Sharing{GraphQL: bits&1 != 0, MCP: bits&2 != 0}
			r.AdapterProxy, r.OutputBindings.Matter = bits&4 != 0, bits&8 != 0
			want := O10Diagnostics{Format: Format, SelectionsAvailable: true, Status: "pending", Reason: "listener_application_pending", GraphQL: bits&1 != 0, MCP: bits&2 != 0, AdapterProxy: bits&4 != 0, Matter: bits&8 != 0}
			if got := O10ProjectDiagnostics(r, nil); got != want {
				t.Fatalf("got %+v; want %+v", got, want)
			}
		})
	}
}

func TestO10ForbiddenRecordFields(t *testing.T) {
	r := o10Record()
	want := `{"format":"helianthus.portal-config/v1","selections_available":true,"status":"pending","reason":"listener_application_pending","graphql":true,"mcp":true,"adapter_proxy":true,"matter":true}`
	if got := o10JSON(t, O10ProjectDiagnostics(r, nil)); got != want {
		t.Fatalf("unexpected diagnostic shape: %s", got)
	}
	for _, field := range []string{"revision", "driver_id", "driver_enabled"} {
		t.Run(field, func(t *testing.T) {
			r := o10Record()
			switch field {
			case "revision":
				r.Revision = "83"
			case "driver_id":
				r.Drivers[0].ID = "o10-other-driver-marker"
			case "driver_enabled":
				r.Drivers[0].Enabled = false
			}
			if got := o10JSON(t, O10ProjectDiagnostics(r, nil)); got != want {
				t.Fatalf("forbidden field affected output: %s", got)
			}
		})
	}
}

func TestO10Unavailable(t *testing.T) {
	for _, code := range []string{"invalid_record", "unsupported_format", "unsupported_selection", "revision_conflict", "persistence_failed", "revision_exhausted", "evidence_unavailable", "listener_conflict", "invalid_binding", "o10-unknown-code-marker"} {
		t.Run(code, func(t *testing.T) {
			reason := code
			if strings.HasPrefix(code, "o10-") {
				reason = "persistence_failed"
			}
			got := O10ProjectDiagnostics(o10Record(), fmt.Errorf("o10-wrapper-marker: %w", &Error{Code: code, Field: "o10-field-marker", NextStep: "o10-next-step-marker"}))
			want := O10Diagnostics{Format: Format, Status: "error", Reason: reason}
			if got != want {
				t.Fatalf("got %+v; want %+v", got, want)
			}
		})
	}
	for _, r := range []Record{{}, {Format: "o10-format-marker", Sharing: Sharing{GraphQL: true, MCP: true}, AdapterProxy: true, OutputBindings: Outputs{Matter: true}}} {
		if got := O10ProjectDiagnostics(r, nil); got != (O10Diagnostics{Format: Format, Status: "error", Reason: "evidence_unavailable"}) {
			t.Fatalf("missing accepted state: %+v", got)
		}
	}
}

func TestO10RawErrorsExcluded(t *testing.T) {
	for _, err := range []error{errors.New("o10-raw-error-marker"), &Error{Code: "o10-code-marker", Field: "o10-location-marker", NextStep: "o10-repair-marker"}} {
		want := `{"format":"helianthus.portal-config/v1","selections_available":false,"status":"error","reason":"persistence_failed","graphql":false,"mcp":false,"adapter_proxy":false,"matter":false}`
		if got := o10JSON(t, O10ProjectDiagnostics(o10Record(), err)); got != want {
			t.Fatalf("error escaped allowlist: %s", got)
		}
	}
}

func TestO10DetachedDeterministic(t *testing.T) {
	r := o10Record()
	before := o10JSON(t, r)
	d := O10ProjectDiagnostics(r, nil)
	encoded := o10JSON(t, d)
	if o10JSON(t, r) != before || o10JSON(t, O10ProjectDiagnostics(r, nil)) != encoded {
		t.Fatal("projection mutated input or is nondeterministic")
	}
	r.Drivers[0] = Driver{ID: "o10-mutated-driver-marker"}
	r.Revision = "91"
	r.Sharing = Sharing{}
	r.AdapterProxy, r.OutputBindings.Matter = false, false
	if o10JSON(t, d) != encoded {
		t.Fatal("projection aliases input")
	}
	d.GraphQL = false
	if O10ProjectDiagnostics(o10Record(), nil).GraphQL != true {
		t.Fatal("output mutation affected another projection")
	}
	// The allowlist must remain value-only; pointers, slices, maps and interfaces
	// would require a new detachment and redaction review.
	typ := reflect.TypeOf(d)
	for i := 0; i < typ.NumField(); i++ {
		kind := typ.Field(i).Type.Kind()
		if kind != reflect.Bool && kind != reflect.String {
			t.Fatalf("reference-capable field %s", typ.Field(i).Name)
		}
	}
}

func TestO10ForbiddenSourceMarkers(t *testing.T) {
	for _, field := range []string{"record_bytes", "binding_address", "path", "raw_error", "unknown_key", "caller_context", "native_evidence"} {
		t.Run(field, func(t *testing.T) {
			data := o10JSON(t, o10Record())
			data = strings.TrimSuffix(data, "}") + fmt.Sprintf(",%q:%q}", field, "o10-"+field+"-marker")
			r, err := Decode([]byte(data))
			if err == nil {
				t.Fatal("unknown source field accepted")
			}
			want := `{"format":"helianthus.portal-config/v1","selections_available":false,"status":"error","reason":"invalid_record","graphql":false,"mcp":false,"adapter_proxy":false,"matter":false}`
			if got := o10JSON(t, O10ProjectDiagnostics(r, err)); got != want {
				t.Fatalf("rejected source escaped projection: %s", got)
			}
		})
	}
}

func TestO10ErrorFieldsIndependent(t *testing.T) {
	for _, field := range []string{"code", "field", "next_step"} {
		t.Run(field, func(t *testing.T) {
			err := &Error{Code: "invalid_record", Field: "record", NextStep: "repair structure"}
			reason := "invalid_record"
			switch field {
			case "code":
				err.Code, reason = "o10-independent-code-marker", "persistence_failed"
			case "field":
				err.Field = "o10-independent-field-marker"
			case "next_step":
				err.NextStep = "o10-independent-step-marker"
			}
			want := O10Diagnostics{Format: Format, Status: "error", Reason: reason}
			if got := O10ProjectDiagnostics(o10Record(), err); got != want {
				t.Fatalf("unsafe error field: %+v", got)
			}
		})
	}
}

func TestO10DriverIdentityExcluded(t *testing.T) {
	for _, id := range []string{"o10-driver-marker", "o10-distinct-driver-marker"} {
		r := o10Record()
		r.Drivers[0].ID = id
		if strings.Contains(o10JSON(t, O10ProjectDiagnostics(r, nil)), id) {
			t.Fatal("driver identity retained")
		}
	}
}

func TestO10RevisionExcluded(t *testing.T) {
	for _, revision := range []string{"37", "83"} {
		r := o10Record()
		r.Revision = revision
		if strings.Contains(o10JSON(t, O10ProjectDiagnostics(r, nil)), revision) {
			t.Fatal("revision retained")
		}
	}
}
