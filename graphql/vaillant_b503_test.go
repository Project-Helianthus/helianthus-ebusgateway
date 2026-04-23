package graphql

import (
	"context"
	"errors"
	"strings"
	"testing"

	graphqlgo "github.com/graphql-go/graphql"
)

// fakeB503Provider is a test double for VaillantB503Provider. It returns
// canned payloads from its fields. Tests set fields explicitly.
type fakeB503Provider struct {
	errorsFn         func(ctx context.Context, target *byte) (VaillantB503Errors, error)
	errorHistoryFn   func(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error)
	serviceCurrentFn func(ctx context.Context, target *byte) (VaillantB503Errors, error)
	serviceHistoryFn func(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error)
	liveMonitorFn    func(ctx context.Context, action string, issuerToken *string, target *byte) (VaillantB503LiveMonitor, error)
	availabilityFn   func(ctx context.Context) string
}

func (f *fakeB503Provider) Errors(ctx context.Context, target *byte) (VaillantB503Errors, error) {
	if f.errorsFn == nil {
		return VaillantB503Errors{}, errors.New("not stubbed")
	}
	return f.errorsFn(ctx, target)
}

func (f *fakeB503Provider) ErrorHistory(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error) {
	if f.errorHistoryFn == nil {
		return VaillantB503HistoryRecord{}, errors.New("not stubbed")
	}
	return f.errorHistoryFn(ctx, target, index)
}

func (f *fakeB503Provider) ServiceCurrent(ctx context.Context, target *byte) (VaillantB503Errors, error) {
	if f.serviceCurrentFn == nil {
		return VaillantB503Errors{}, errors.New("not stubbed")
	}
	return f.serviceCurrentFn(ctx, target)
}

func (f *fakeB503Provider) ServiceHistory(ctx context.Context, target *byte, index *byte) (VaillantB503HistoryRecord, error) {
	if f.serviceHistoryFn == nil {
		return VaillantB503HistoryRecord{}, errors.New("not stubbed")
	}
	return f.serviceHistoryFn(ctx, target, index)
}

func (f *fakeB503Provider) LiveMonitor(ctx context.Context, action string, issuerToken *string, target *byte) (VaillantB503LiveMonitor, error) {
	if f.liveMonitorFn == nil {
		return VaillantB503LiveMonitor{}, errors.New("not stubbed")
	}
	return f.liveMonitorFn(ctx, action, issuerToken, target)
}

func (f *fakeB503Provider) Availability(ctx context.Context) string {
	if f.availabilityFn == nil {
		return "UNKNOWN"
	}
	return f.availabilityFn(ctx)
}

func newB503TestSchema(t *testing.T, provider VaillantB503Provider) graphqlgo.Schema {
	t.Helper()
	builder := NewBuilder(nil, nil)
	builder.SetVaillantB503Provider(provider)
	schema, err := NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema error = %v", err)
	}
	return schema
}

func doGraphQL(t *testing.T, schema graphqlgo.Schema, query string) *graphqlgo.Result {
	t.Helper()
	return graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query})
}

// -- Schema-shape tests -----------------------------------------------------

func TestVaillantB503GraphQL_ErrorsGet_Schema(t *testing.T) {
	schema := newB503TestSchema(t, &fakeB503Provider{})

	res := doGraphQL(t, schema, `{
		__schema {
			queryType { fields { name } }
			types { name kind enumValues { name } }
		}
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("introspection errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	sch, _ := data["__schema"].(map[string]any)
	qt, _ := sch["queryType"].(map[string]any)
	fields, _ := qt["fields"].([]any)
	want := map[string]bool{
		"vaillantErrors":         false,
		"vaillantErrorHistory":   false,
		"vaillantServiceCurrent": false,
		"vaillantServiceHistory": false,
		"vaillantLiveMonitor":    false,
		"vaillantCapabilities":   false,
	}
	for _, f := range fields {
		m, _ := f.(map[string]any)
		name, _ := m["name"].(string)
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	for k, present := range want {
		if !present {
			t.Errorf("expected query field %q missing from schema", k)
		}
	}

	// Confirm types present.
	types, _ := sch["types"].([]any)
	neededTypes := map[string]bool{
		"B503Availability":             false,
		"VaillantB503Errors":           false,
		"VaillantB503HistoryRecord":    false,
		"VaillantB503LiveMonitor":      false,
		"VaillantB503Capability":       false,
		"VaillantCapabilities":         false,
	}
	for _, tEntry := range types {
		m, _ := tEntry.(map[string]any)
		name, _ := m["name"].(string)
		if _, ok := neededTypes[name]; ok {
			neededTypes[name] = true
		}
	}
	for k, present := range neededTypes {
		if !present {
			t.Errorf("expected type %q missing from schema", k)
		}
	}
}

func TestVaillantB503GraphQL_NoExpiredInEnum(t *testing.T) {
	schema := newB503TestSchema(t, &fakeB503Provider{})
	res := doGraphQL(t, schema, `{
		__type(name: "B503Availability") { enumValues { name } }
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("introspection errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	tObj, _ := data["__type"].(map[string]any)
	ev, _ := tObj["enumValues"].([]any)
	if len(ev) != 5 {
		t.Fatalf("expected 5 enum values, got %d: %+v", len(ev), ev)
	}
	want := map[string]bool{
		"AVAILABLE":      false,
		"NOT_SUPPORTED":  false,
		"TRANSPORT_DOWN": false,
		"SESSION_BUSY":   false,
		"UNKNOWN":        false,
	}
	for _, e := range ev {
		m, _ := e.(map[string]any)
		name, _ := m["name"].(string)
		if name == "EXPIRED" {
			t.Fatalf("EXPIRED MUST NOT be a member of B503Availability enum")
		}
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	for k, present := range want {
		if !present {
			t.Errorf("expected enum value %q missing", k)
		}
	}
}

func TestVaillantB503GraphQL_NoMutationsForB503(t *testing.T) {
	schema := newB503TestSchema(t, &fakeB503Provider{})
	res := doGraphQL(t, schema, `{
		__schema { mutationType { fields { name } } }
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("introspection errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	sch, _ := data["__schema"].(map[string]any)
	mt := sch["mutationType"]
	if mt == nil {
		return // no mutations at all — also acceptable
	}
	m, _ := mt.(map[string]any)
	fields, _ := m["fields"].([]any)
	for _, f := range fields {
		mm, _ := f.(map[string]any)
		name, _ := mm["name"].(string)
		if strings.HasPrefix(strings.ToLower(name), "vaillantb503") {
			t.Fatalf("mutation %q must not exist — B503 is read-only (plan AD02)", name)
		}
	}
}

// -- Behaviour tests --------------------------------------------------------

// TestVaillantB503GraphQL_ErrorsGet_DecodesSpecFixture verifies that a stubbed
// provider returning the spec §5.3 fixture (firstActiveError=281) round-trips
// through the GraphQL resolver.
func TestVaillantB503GraphQL_ErrorsGet_DecodesSpecFixture(t *testing.T) {
	first := 281
	slot1 := 281
	provider := &fakeB503Provider{
		errorsFn: func(ctx context.Context, target *byte) (VaillantB503Errors, error) {
			return VaillantB503Errors{
				FirstActiveError: &first,
				Slots:            []*int{&slot1, nil, nil, nil, nil},
			}, nil
		},
	}
	schema := newB503TestSchema(t, provider)
	res := doGraphQL(t, schema, `{
		vaillantErrors {
			firstActiveError
			slots
		}
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("query errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	errs, _ := data["vaillantErrors"].(map[string]any)
	if errs == nil {
		t.Fatalf("vaillantErrors payload nil; data=%+v", data)
	}
	if got, _ := errs["firstActiveError"].(int); got != 281 {
		t.Fatalf("firstActiveError = %v; want 281", errs["firstActiveError"])
	}
	slots, _ := errs["slots"].([]any)
	if len(slots) != 5 {
		t.Fatalf("slots len = %d; want 5", len(slots))
	}
	if slots[0] != 281 {
		t.Fatalf("slots[0] = %v; want 281", slots[0])
	}
	for i := 1; i < 5; i++ {
		if slots[i] != nil {
			t.Fatalf("slots[%d] = %v; want nil", i, slots[i])
		}
	}
}

func TestVaillantB503GraphQL_Capability_Available(t *testing.T) {
	provider := &fakeB503Provider{
		availabilityFn: func(ctx context.Context) string { return "AVAILABLE" },
	}
	schema := newB503TestSchema(t, provider)
	res := doGraphQL(t, schema, `{
		vaillantCapabilities {
			vaillantB503 {
				reason
				available
			}
		}
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("query errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	caps, _ := data["vaillantCapabilities"].(map[string]any)
	b503, _ := caps["vaillantB503"].(map[string]any)
	if b503["reason"] != "AVAILABLE" {
		t.Fatalf("reason = %v; want AVAILABLE", b503["reason"])
	}
	if b503["available"] != true {
		t.Fatalf("available = %v; want true", b503["available"])
	}
}

// TestVaillantB503GraphQL_EXPIREDAlwaysMasked — defense in depth: if provider
// leaks the string "EXPIRED", the GraphQL layer must remap it to SESSION_BUSY
// before serving to clients (plan AD14).
func TestVaillantB503GraphQL_EXPIREDAlwaysMasked(t *testing.T) {
	provider := &fakeB503Provider{
		availabilityFn: func(ctx context.Context) string { return "EXPIRED" },
	}
	schema := newB503TestSchema(t, provider)
	res := doGraphQL(t, schema, `{
		vaillantCapabilities {
			vaillantB503 { reason available }
		}
	}`)
	if len(res.Errors) > 0 {
		t.Fatalf("query errors = %+v", res.Errors)
	}
	data, _ := res.Data.(map[string]any)
	caps, _ := data["vaillantCapabilities"].(map[string]any)
	b503, _ := caps["vaillantB503"].(map[string]any)
	if b503["reason"] != "SESSION_BUSY" {
		t.Fatalf("reason = %v; want SESSION_BUSY (EXPIRED must never leak)", b503["reason"])
	}
	if b503["available"] != false {
		t.Fatalf("available = %v; want false", b503["available"])
	}
}
