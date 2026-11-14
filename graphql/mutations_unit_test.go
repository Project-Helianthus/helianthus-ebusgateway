package graphql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	"github.com/Project-Helianthus/helianthus-ebusreg/schema"
	graphqlgo "github.com/graphql-go/graphql"
)

type schemaTemplate struct {
	primary   byte
	secondary byte
	params    schema.Schema
}

func (template schemaTemplate) Primary() byte {
	return template.primary
}

func (template schemaTemplate) Secondary() byte {
	return template.secondary
}

func (template schemaTemplate) ParamSchema() schema.Schema {
	return template.params
}

type buildTemplate struct {
	primary   byte
	secondary byte
}

func (template buildTemplate) Primary() byte {
	return template.primary
}

func (template buildTemplate) Secondary() byte {
	return template.secondary
}

func (template buildTemplate) Build(params map[string]any) ([]byte, error) {
	if params == nil {
		return nil, ebuserrors.ErrInvalidPayload
	}
	value, ok := params["period"]
	if !ok {
		return nil, ebuserrors.ErrInvalidPayload
	}
	switch typed := value.(type) {
	case int:
		return []byte{byte(typed)}, nil
	case int64:
		return []byte{byte(typed)}, nil
	case uint8:
		return []byte{typed}, nil
	default:
		return nil, ebuserrors.ErrInvalidPayload
	}
}

type mockBoilerWriter struct {
	result BoilerConfigMutationResult
	calls  []struct {
		field string
		value string
	}
}

func (writer *mockBoilerWriter) SetBoilerConfig(_ context.Context, fieldName string, rawValue string) BoilerConfigMutationResult {
	writer.calls = append(writer.calls, struct {
		field string
		value string
	}{field: fieldName, value: rawValue})
	return writer.result
}

type mockScheduleWriter struct {
	zoneResult *mcp.TimeProgramWriteResult
	dhwResult  *mcp.TimeProgramWriteResult
	zoneErr    error
	dhwErr     error
	zoneCalls  []struct {
		zone    int
		weekday int
		slots   []mcp.TimeProgramSlot
	}
	dhwCalls []struct {
		weekday int
		slots   []mcp.TimeProgramSlot
	}
}

func (writer *mockScheduleWriter) SetZoneTimeProgram(_ context.Context, zone int, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	cloned := append([]mcp.TimeProgramSlot(nil), slots...)
	writer.zoneCalls = append(writer.zoneCalls, struct {
		zone    int
		weekday int
		slots   []mcp.TimeProgramSlot
	}{
		zone:    zone,
		weekday: weekday,
		slots:   cloned,
	})
	return writer.zoneResult, writer.zoneErr
}

func (writer *mockScheduleWriter) SetDhwTimeProgram(_ context.Context, weekday int, slots []mcp.TimeProgramSlot) (*mcp.TimeProgramWriteResult, error) {
	cloned := append([]mcp.TimeProgramSlot(nil), slots...)
	writer.dhwCalls = append(writer.dhwCalls, struct {
		weekday int
		slots   []mcp.TimeProgramSlot
	}{
		weekday: weekday,
		slots:   cloned,
	})
	return writer.dhwResult, writer.dhwErr
}

func TestValidateInvokeParams_SchemaCoercion(t *testing.T) {
	method := mockMethod{
		name:     "set_level",
		readOnly: false,
		template: schemaTemplate{
			primary:   0xB5,
			secondary: 0x05,
			params: schema.Schema{
				Fields: []schema.SchemaField{
					{Name: "level", Type: types.DATA1b{}},
				},
			},
		},
	}

	got, err := validateInvokeParams(method, map[string]any{"level": float64(3)})
	if err != nil {
		t.Fatalf("validateInvokeParams error = %v", err)
	}
	if value, ok := got["level"].(int64); !ok || value != 3 {
		t.Fatalf("normalized level = %#v; want int64(3)", got["level"])
	}

	_, err = validateInvokeParams(method, map[string]any{})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("missing field error = %v; want ErrInvalidPayload", err)
	}

	_, err = validateInvokeParams(method, map[string]any{"level": 1, "extra": 2})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("extra field error = %v; want ErrInvalidPayload", err)
	}
}

func TestValidateInvokeParams_NoParamsAllowed(t *testing.T) {
	method := mockMethod{
		name:     "get_status",
		readOnly: true,
		template: mockTemplate{primary: 0xB5, secondary: 0x04},
	}

	_, err := validateInvokeParams(method, map[string]any{"unexpected": 1})
	if !errors.Is(err, ebuserrors.ErrInvalidPayload) {
		t.Fatalf("unexpected params error = %v; want ErrInvalidPayload", err)
	}
}

func TestValidateInvokeParams_BuilderCoercion(t *testing.T) {
	method := mockMethod{
		name:     "get_energy",
		readOnly: true,
		template: buildTemplate{primary: 0xB5, secondary: 0x16},
	}

	got, err := validateInvokeParams(method, map[string]any{"period": float64(2)})
	if err != nil {
		t.Fatalf("validateInvokeParams error = %v", err)
	}
	if value, ok := got["period"].(int64); !ok || value != 2 {
		t.Fatalf("normalized period = %#v; want int64(2)", got["period"])
	}
}

func TestMapInvokeError_Categories(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		code     string
		category string
	}{
		{name: "invalid", err: ebuserrors.ErrInvalidPayload, code: "INVALID_PAYLOAD", category: "INVALID"},
		{name: "timeout", err: ebuserrors.ErrTimeout, code: "TIMEOUT", category: "TRANSIENT"},
		{name: "nack", err: ebuserrors.ErrNACK, code: "NACK", category: "DEFINITIVE"},
		{name: "closed", err: ebuserrors.ErrTransportClosed, code: "TRANSPORT_CLOSED", category: "FATAL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mapInvokeError(tc.err)
			if got == nil {
				t.Fatalf("mapInvokeError returned nil")
			}
			if got.Code != tc.code || got.Category != tc.category {
				t.Fatalf("mapInvokeError = %+v; want code=%s category=%s", got, tc.code, tc.category)
			}
		})
	}
}

func TestNormalizeInvokeResult_ConstraintFields(t *testing.T) {
	result := map[string]types.Value{
		"constraints": {
			Value: map[string]any{
				"type": "tempv",
				"min":  float64(35),
				"max":  float64(70),
				"step": float64(1),
			},
			Valid: true,
		},
		"constraint_type": {Value: "tempv", Valid: true},
		"constraint_min":  {Value: float64(35), Valid: true},
		"constraint_max":  {Value: float64(70), Valid: true},
		"constraint_step": {Value: float64(1), Valid: true},
		"value":           {Valid: false},
	}

	normalized, ok := normalizeInvokeResult(result).(map[string]any)
	if !ok {
		t.Fatalf("normalizeInvokeResult type = %T; want map[string]any", normalizeInvokeResult(result))
	}
	constraints, ok := normalized["constraints"].(map[string]any)
	if !ok {
		t.Fatalf("constraints type = %T; want map[string]any", normalized["constraints"])
	}
	if constraints["type"] != "tempv" || constraints["min"] != float64(35) || constraints["max"] != float64(70) || constraints["step"] != float64(1) {
		t.Fatalf("constraints mismatch: %#v", constraints)
	}
	if normalized["constraint_type"] != "tempv" || normalized["constraint_min"] != float64(35) ||
		normalized["constraint_max"] != float64(70) || normalized["constraint_step"] != float64(1) {
		t.Fatalf("flattened constraints mismatch: %#v", normalized)
	}
	if normalized["value"] != nil {
		t.Fatalf("value = %#v; want nil for invalid source value", normalized["value"])
	}
}

func TestSetBoilerConfigMutation_UnsupportedInReducedProfile(t *testing.T) {
	queryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "query",
		Fields: graphqlgo.Fields{
			"noop": &graphqlgo.Field{Type: graphqlgo.String},
		},
	})
	mutationType := buildMutationType(nil, nil, nil, nil, nil, func() (byte, bool) { return 0, false })
	schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
	if err != nil {
		t.Fatalf("NewSchema error = %v", err)
	}

	result := graphqlgo.Do(graphqlgo.Params{
		Schema: schema,
		RequestString: `mutation {
			set_boiler_config(field: "maximum_heating_flow_temperature_setpoint", value: "55") {
				success
				error
			}
		}`,
	})
	if len(result.Errors) > 0 {
		t.Fatalf("mutation errors = %+v", result.Errors)
	}

	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("result data type = %T; want map", result.Data)
	}
	payload, ok := data["set_boiler_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_boiler_config payload type = %T; want map", data["set_boiler_config"])
	}
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("set_boiler_config success = %v; want false", got)
	}
	errMessage, _ := payload["error"].(string)
	if errMessage == "" {
		t.Fatalf("set_boiler_config error message empty")
	}
}

func TestSetCircuitConfigMutation_Success(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x00, 0x00, 0xC0, 0x3F}, Valid: true}, // 1.5
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		set_circuit_config(index: 1, field: "circuit_heating_curve_factor", value: "1.5") {
			success
			error
		}
	}`)

	payload, ok := data["set_circuit_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_circuit_config payload type = %T; want map", data["set_circuit_config"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_circuit_config success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("set_circuit_config error = %#v; want nil", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if first.Plane != mutationSystemPlane || first.Method != mutationSetExtRegisterMethod {
		t.Fatalf("first call = %+v; want system/%s", first, mutationSetExtRegisterMethod)
	}
	if got := first.Params["group"]; got != byte(0x02) {
		t.Fatalf("first call group = %#v; want 0x02", got)
	}
	if got := first.Params["instance"]; got != byte(0x01) {
		t.Fatalf("first call instance = %#v; want 0x01", got)
	}
	if got := first.Params["addr"]; got != uint16(0x000F) {
		t.Fatalf("first call addr = %#v; want 0x000F", got)
	}
	if got := first.Params["opcode"]; got != byte(0x02) {
		t.Fatalf("first call opcode = %#v; want 0x02", got)
	}
	if got := first.Params["source"]; got != byte(0x7F) {
		t.Fatalf("first call source = %#v; want 0x7F", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x00, 0x00, 0xC0, 0x3F}) {
		t.Fatalf("first call data = %v; want [00 00 C0 3F]", first.Params["data"])
	}

	second := invoker.calls[1]
	if second.Method != mutationGetExtRegisterMethod {
		t.Fatalf("second call method = %q; want %q", second.Method, mutationGetExtRegisterMethod)
	}
	if _, ok := second.Params["data"]; ok {
		t.Fatalf("second call unexpectedly has data param: %#v", second.Params["data"])
	}
}

func TestSetCircuitConfigMutation_FailsClosedBeforeSourceAdmission(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{}

	data := executeMutation(t, buildMutationSchemaWithSourceProvider(t, registry, invoker, nil, nil, func() (byte, bool) {
		return 0, false
	}), `mutation {
		set_circuit_config(index: 1, field: "circuit_heating_curve_factor", value: "1.5") {
			success
			error
		}
	}`)

	payload, ok := data["set_circuit_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_circuit_config payload type = %T; want map", data["set_circuit_config"])
	}
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("set_circuit_config success = %v; want false", got)
	}
	errMessage, _ := payload["error"].(string)
	if !strings.Contains(errMessage, "source selection not active") {
		t.Fatalf("set_circuit_config error = %q; want source selection not active", errMessage)
	}
	if len(invoker.calls) != 0 {
		t.Fatalf("invoker calls = %d; want 0 before source admission", len(invoker.calls))
	}
}

func TestIssue851BuilderMutationProviderFencesDirectGraphQLInvoker(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{}
	admitted := false
	builder := NewBuilder(nil, nil)
	builder.SetAdmittedMutationSource(0x77)
	builder.SetAdmittedMutationSourceProvider(func() (byte, bool) {
		return 0x77, admitted
	})
	schema, err := NewSchema(builder, registry, invoker, nil)
	if err != nil {
		t.Fatalf("NewSchema() error = %v", err)
	}

	data := executeMutation(t, schema, `mutation {
		set_circuit_config(index: 1, field: "circuit_heating_curve_factor", value: "1.5") {
			success
			error
		}
	}`)
	payload, _ := data["set_circuit_config"].(map[string]any)
	if success, _ := payload["success"].(bool); success {
		t.Fatalf("withdrawn GraphQL mutation = %#v, want unavailable", payload)
	}
	if len(invoker.calls) != 0 {
		t.Fatalf("withdrawn GraphQL invoker calls = %d, want 0", len(invoker.calls))
	}
}

func TestApplyConfigMutation_FailsClosedBeforeSourceAdmission(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{}

	result := ApplyConfigMutation(context.Background(), registry, invoker, 0, false, "circuit_heating_curve_factor", "1.5", circuitConfigFieldSpecs)
	if result.Success {
		t.Fatalf("ApplyConfigMutation success = true; want false")
	}
	if !strings.Contains(result.Error, "source selection not active") {
		t.Fatalf("ApplyConfigMutation error = %q; want source selection not active", result.Error)
	}
	if len(invoker.calls) != 0 {
		t.Fatalf("invoker calls = %d; want 0 before source admission", len(invoker.calls))
	}
}

func TestSetBoilerConfigMutation_DelegatesToWriter(t *testing.T) {
	writer := &mockBoilerWriter{
		result: BoilerConfigMutationResult{Success: true},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, writer, nil), `mutation {
		set_boiler_config(field: "maximum_heating_flow_temperature_setpoint", value: "55") {
			success
			error
		}
	}`)

	payload, ok := data["set_boiler_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_boiler_config payload type = %T; want map", data["set_boiler_config"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_boiler_config success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("set_boiler_config error = %#v; want nil", got)
	}
	if len(writer.calls) != 1 {
		t.Fatalf("writer calls = %d; want 1", len(writer.calls))
	}
	if writer.calls[0].field != "maximum_heating_flow_temperature_setpoint" || writer.calls[0].value != "55" {
		t.Fatalf("writer call = %#v; want flowset_hc_max_c/55", writer.calls[0])
	}
}

func TestSetSystemConfigMutation_Success(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x01}, Valid: true},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		set_system_config(field: "adaptive_heating_curve_enabled", value: "true") {
			success
			error
		}
	}`)

	payload, ok := data["set_system_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_system_config payload type = %T; want map", data["set_system_config"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_system_config success = %v; want true", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if first.Method != mutationSetExtRegisterMethod {
		t.Fatalf("first call method = %q; want %q", first.Method, mutationSetExtRegisterMethod)
	}
	if got := first.Params["group"]; got != byte(0x00) {
		t.Fatalf("first call group = %#v; want 0x00", got)
	}
	if got := first.Params["instance"]; got != byte(0x00) {
		t.Fatalf("first call instance = %#v; want 0x00", got)
	}
	if got := first.Params["addr"]; got != uint16(0x0014) {
		t.Fatalf("first call addr = %#v; want 0x0014", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x01}) {
		t.Fatalf("first call data = %v; want [01]", first.Params["data"])
	}
}

func TestSetSystemConfigMutation_IntegerLikeFloatString(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x37, 0x00}, Valid: true}, // 55
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		set_system_config(field: "room_humidity_limit_maximum", value: "55.0") {
			success
			error
		}
	}`)

	payload, ok := data["set_system_config"].(map[string]any)
	if !ok {
		t.Fatalf("set_system_config payload type = %T; want map", data["set_system_config"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_system_config success = %v; want true", got)
	}

	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
	first := invoker.calls[0]
	if got := first.Params["addr"]; got != uint16(0x000E) {
		t.Fatalf("first call addr = %#v; want 0x000E", got)
	}
	dataBytes, ok := first.Params["data"].([]byte)
	if !ok || !bytes.Equal(dataBytes, []byte{0x37, 0x00}) {
		t.Fatalf("first call data = %v; want [37 00]", first.Params["data"])
	}
}

func TestSetCircuitConfigMutation_ValidationFailures(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantErrPart string
	}{
		{
			name: "unknown field",
			query: `mutation {
				set_circuit_config(index: 1, field: "bogus", value: "1") { success error }
			}`,
			wantErrPart: "unknown circuit field",
		},
		{
			name: "float out of range",
			query: `mutation {
				set_circuit_config(index: 1, field: "circuit_heating_curve_factor", value: "8.1") { success error }
			}`,
			wantErrPart: "out of range",
		},
		{
			name: "invalid enum",
			query: `mutation {
				set_circuit_config(index: 1, field: "circuit_room_temperature_influence_mode", value: "manual") { success error }
			}`,
			wantErrPart: "invalid enum value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := mutationTestRegistry{
				entries: map[byte]registry.DeviceEntry{
					0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
				},
				order: []byte{0x15},
			}
			invoker := &mutationTestInvoker{}
			data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), test.query)

			payload, ok := data["set_circuit_config"].(map[string]any)
			if !ok {
				t.Fatalf("set_circuit_config payload type = %T; want map", data["set_circuit_config"])
			}
			if got, _ := payload["success"].(bool); got {
				t.Fatalf("set_circuit_config success = %v; want false", got)
			}
			errMessage, _ := payload["error"].(string)
			if !strings.Contains(errMessage, test.wantErrPart) {
				t.Fatalf("set_circuit_config error = %q; want contains %q", errMessage, test.wantErrPart)
			}
			if len(invoker.calls) != 0 {
				t.Fatalf("invoker calls = %d; want 0 for validation failure", len(invoker.calls))
			}
		})
	}
}

func TestSetSystemConfigMutation_FailureScenarios(t *testing.T) {
	t.Run("invalid bool", func(t *testing.T) {
		registry := mutationTestRegistry{
			entries: map[byte]registry.DeviceEntry{
				0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
			},
			order: []byte{0x15},
		}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			set_system_config(field: "adaptive_heating_curve_enabled", value: "maybe") { success error }
		}`)
		payload, _ := data["set_system_config"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("set_system_config success = %v; want false", got)
		}
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "invalid boolean value") {
			t.Fatalf("set_system_config error = %q; want invalid boolean", errMessage)
		}
		if len(invoker.calls) != 0 {
			t.Fatalf("invoker calls = %d; want 0", len(invoker.calls))
		}
	})

	t.Run("missing controller", func(t *testing.T) {
		registry := mutationTestRegistry{}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			set_system_config(field: "adaptive_heating_curve_enabled", value: "true") { success error }
		}`)
		payload, _ := data["set_system_config"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("set_system_config success = %v; want false", got)
		}
		// P9.5 pivot — error message changed from "controller BASV2
		// not found" (identifier-coupled) to "no heating regulator at
		// canonical eBUS target addresses" (identifier-free).
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "no heating regulator at canonical eBUS target addresses") {
			t.Fatalf("set_system_config error = %q; want missing-regulator error", errMessage)
		}
	})

	t.Run("missing get method", func(t *testing.T) {
		registry := mutationTestRegistry{
			entries: map[byte]registry.DeviceEntry{
				0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod),
			},
			order: []byte{0x15},
		}
		invoker := &mutationTestInvoker{}
		data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
			set_system_config(field: "adaptive_heating_curve_enabled", value: "true") { success error }
		}`)
		payload, _ := data["set_system_config"].(map[string]any)
		if got, _ := payload["success"].(bool); got {
			t.Fatalf("set_system_config success = %v; want false", got)
		}
		if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, mutationGetExtRegisterMethod) {
			t.Fatalf("set_system_config error = %q; want mention missing get method", errMessage)
		}
		if len(invoker.calls) != 0 {
			t.Fatalf("invoker calls = %d; want 0", len(invoker.calls))
		}
	})
}

func TestSetCircuitConfigMutation_ReadbackConfirmFailure(t *testing.T) {
	registry := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	invoker := &mutationTestInvoker{
		responses: []any{
			map[string]types.Value{},
			map[string]types.Value{
				"value": {Value: []byte{0x00, 0x00, 0x40, 0x40}, Valid: true}, // 3.0 (mismatch)
			},
		},
	}
	data := executeMutation(t, buildMutationSchema(t, registry, invoker, nil, nil), `mutation {
		set_circuit_config(index: 2, field: "circuit_heating_curve_factor", value: "1.5") { success error }
	}`)
	payload, _ := data["set_circuit_config"].(map[string]any)
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("set_circuit_config success = %v; want false", got)
	}
	if errMessage, _ := payload["error"].(string); !strings.Contains(errMessage, "read-back mismatch") {
		t.Fatalf("set_circuit_config error = %q; want read-back mismatch", errMessage)
	}
	if len(invoker.calls) != 2 {
		t.Fatalf("invoker calls = %d; want 2", len(invoker.calls))
	}
}

func TestSetZoneTimeProgramMutation_Success(t *testing.T) {
	writer := &mockScheduleWriter{
		zoneResult: &mcp.TimeProgramWriteResult{
			Success: true,
			SlotResults: []mcp.TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		set_zone_time_program(
			zone: 2
			weekday: 1
			slots: "[{\"start_hour\":6,\"start_minute\":0,\"end_hour\":22,\"end_minute\":0,\"temperature_c\":21}]"
		) {
			success
			error
			slot_results {
				slot_index
				accepted
				error_code
				error_description
			}
		}
	}`)

	payload, ok := data["set_zone_time_program"].(map[string]any)
	if !ok {
		t.Fatalf("set_zone_time_program payload type = %T; want map", data["set_zone_time_program"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_zone_time_program success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("set_zone_time_program error = %#v; want nil", got)
	}
	slotResults, ok := payload["slot_results"].([]any)
	if !ok || len(slotResults) != 1 {
		t.Fatalf("set_zone_time_program slot_results = %#v; want one result", payload["slot_results"])
	}
	firstSlot, ok := slotResults[0].(map[string]any)
	if !ok {
		t.Fatalf("set_zone_time_program first slot type = %T; want map", slotResults[0])
	}
	if got, _ := firstSlot["slot_index"].(int); got != 0 {
		t.Fatalf("slot_index = %v; want 0", got)
	}
	if got, _ := firstSlot["accepted"].(bool); !got {
		t.Fatalf("accepted = %v; want true", got)
	}
	if got, _ := firstSlot["error_code"].(int); got != 0 {
		t.Fatalf("error_code = %v; want 0", got)
	}
	if got := firstSlot["error_description"]; got != nil {
		t.Fatalf("error_description = %#v; want nil", got)
	}

	if len(writer.zoneCalls) != 1 {
		t.Fatalf("zone writer calls = %d; want 1", len(writer.zoneCalls))
	}
	call := writer.zoneCalls[0]
	if call.zone != 2 || call.weekday != 1 {
		t.Fatalf("zone writer call = %#v; want zone=2 weekday=1", call)
	}
	if len(call.slots) != 1 {
		t.Fatalf("zone writer slots = %#v; want one slot", call.slots)
	}
	slot := call.slots[0]
	if slot.StartHour != 6 || slot.StartMinute != 0 || slot.EndHour != 22 || slot.EndMinute != 0 {
		t.Fatalf("zone slot = %#v; want 06:00-22:00", slot)
	}
	if slot.TemperatureC == nil || *slot.TemperatureC != 21 {
		t.Fatalf("zone slot temperature = %#v; want 21", slot.TemperatureC)
	}
}

func TestSetDhwTimeProgramMutation_Success(t *testing.T) {
	writer := &mockScheduleWriter{
		dhwResult: &mcp.TimeProgramWriteResult{
			Success: true,
			SlotResults: []mcp.TimeProgramSlotResult{
				{SlotIndex: 0, Accepted: true, ErrorCode: 0},
			},
		},
	}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		set_dhw_time_program(
			weekday: 5
			slots: "[{\"start_hour\":5,\"start_minute\":30,\"end_hour\":7,\"end_minute\":0}]"
		) {
			success
			error
			slot_results {
				slot_index
				accepted
				error_code
				error_description
			}
		}
	}`)

	payload, ok := data["set_dhw_time_program"].(map[string]any)
	if !ok {
		t.Fatalf("set_dhw_time_program payload type = %T; want map", data["set_dhw_time_program"])
	}
	if got, _ := payload["success"].(bool); !got {
		t.Fatalf("set_dhw_time_program success = %v; want true", got)
	}
	if got := payload["error"]; got != nil {
		t.Fatalf("set_dhw_time_program error = %#v; want nil", got)
	}
	if len(writer.dhwCalls) != 1 {
		t.Fatalf("dhw writer calls = %d; want 1", len(writer.dhwCalls))
	}
	call := writer.dhwCalls[0]
	if call.weekday != 5 {
		t.Fatalf("dhw writer weekday = %d; want 5", call.weekday)
	}
	if len(call.slots) != 1 {
		t.Fatalf("dhw writer slots = %#v; want one slot", call.slots)
	}
	slot := call.slots[0]
	if slot.StartHour != 5 || slot.StartMinute != 30 || slot.EndHour != 7 || slot.EndMinute != 0 {
		t.Fatalf("dhw slot = %#v; want 05:30-07:00", slot)
	}
	if slot.TemperatureC != nil {
		t.Fatalf("dhw slot temperature = %#v; want nil", slot.TemperatureC)
	}
}

func TestSetZoneTimeProgramMutation_InvalidJSON(t *testing.T) {
	writer := &mockScheduleWriter{}

	data := executeMutation(t, buildMutationSchema(t, nil, nil, nil, writer), `mutation {
		set_zone_time_program(zone: 1, weekday: 2, slots: "not-json") {
			success
			error
			slot_results {
				slot_index
			}
		}
	}`)

	payload, ok := data["set_zone_time_program"].(map[string]any)
	if !ok {
		t.Fatalf("set_zone_time_program payload type = %T; want map", data["set_zone_time_program"])
	}
	if got, _ := payload["success"].(bool); got {
		t.Fatalf("set_zone_time_program success = %v; want false", got)
	}
	errMessage, _ := payload["error"].(string)
	if !strings.Contains(errMessage, "invalid slots JSON") {
		t.Fatalf("set_zone_time_program error = %q; want invalid slots JSON", errMessage)
	}
	slotResults, ok := payload["slot_results"].([]any)
	if !ok || len(slotResults) != 0 {
		t.Fatalf("set_zone_time_program slot_results = %#v; want empty list", payload["slot_results"])
	}
	if len(writer.zoneCalls) != 0 {
		t.Fatalf("zone writer calls = %d; want 0", len(writer.zoneCalls))
	}
}

func buildMutationSchema(t *testing.T, registry InvokeRegistry, invoker Invoker, boilerWriter BoilerConfigWriter, scheduleWriter ScheduleWriter) graphqlgo.Schema {
	return buildMutationSchemaWithSourceProvider(t, registry, invoker, boilerWriter, scheduleWriter, func() (byte, bool) { return 0x7F, true })
}

func buildMutationSchemaWithSourceProvider(t *testing.T, registry InvokeRegistry, invoker Invoker, boilerWriter BoilerConfigWriter, scheduleWriter ScheduleWriter, sourceProvider func() (byte, bool)) graphqlgo.Schema {
	t.Helper()
	queryType := graphqlgo.NewObject(graphqlgo.ObjectConfig{
		Name: "query",
		Fields: graphqlgo.Fields{
			"noop": &graphqlgo.Field{Type: graphqlgo.String},
		},
	})
	mutationType := buildMutationType(registry, invoker, boilerWriter, nil, scheduleWriter, sourceProvider)
	schema, err := graphqlgo.NewSchema(graphqlgo.SchemaConfig{
		Query:    queryType,
		Mutation: mutationType,
	})
	if err != nil {
		t.Fatalf("NewSchema error = %v", err)
	}
	return schema
}

func executeMutation(t *testing.T, schema graphqlgo.Schema, request string) map[string]any {
	t.Helper()
	result := graphqlgo.Do(graphqlgo.Params{
		Schema:        schema,
		RequestString: request,
	})
	if len(result.Errors) > 0 {
		t.Fatalf("mutation errors = %+v", result.Errors)
	}
	data, ok := result.Data.(map[string]any)
	if !ok {
		t.Fatalf("result data type = %T; want map", result.Data)
	}
	return data
}

func testControllerEntryWithMethods(methodNames ...string) registry.DeviceEntry {
	methods := make([]registry.Method, 0, len(methodNames))
	for _, name := range methodNames {
		methods = append(methods, mockMethod{
			name:     name,
			readOnly: name == mutationGetExtRegisterMethod,
			template: mockTemplate{primary: 0xB5, secondary: 0x24},
		})
	}

	plane := &invokePlane{
		name:            mutationSystemPlane,
		source:          0x10,
		target:          0x15,
		hardwareVersion: "7603",
		methods:         methods,
	}

	return mockEntry{
		info: registry.DeviceInfo{
			Address:      0x15,
			Manufacturer: "Vaillant",
			DeviceID:     "BASV2",
		},
		planes: []registry.Plane{plane},
	}
}

type mutationTestRegistry struct {
	entries map[byte]registry.DeviceEntry
	order   []byte
}

func (reg mutationTestRegistry) Lookup(address byte) (registry.DeviceEntry, bool) {
	if reg.entries == nil {
		return nil, false
	}
	entry, ok := reg.entries[address]
	return entry, ok
}

func (reg mutationTestRegistry) Iterate(fn func(registry.DeviceEntry) bool) {
	if fn == nil || reg.entries == nil {
		return
	}
	for _, addr := range reg.order {
		entry, ok := reg.entries[addr]
		if !ok || entry == nil {
			continue
		}
		if !fn(entry) {
			return
		}
	}
}

type mutationInvokeCall struct {
	Plane  string
	Method string
	Params map[string]any
}

type mutationTestInvoker struct {
	calls     []mutationInvokeCall
	responses []any
	errs      []error
}

func (invoker *mutationTestInvoker) Invoke(_ context.Context, plane router.Plane, methodName string, params map[string]any) (any, error) {
	if invoker == nil {
		return nil, nil
	}
	copied := cloneMutationParams(params)
	planeName := ""
	if plane != nil {
		planeName = plane.Name()
	}
	invoker.calls = append(invoker.calls, mutationInvokeCall{
		Plane:  planeName,
		Method: methodName,
		Params: copied,
	})

	index := len(invoker.calls) - 1
	if index < len(invoker.errs) && invoker.errs[index] != nil {
		return nil, invoker.errs[index]
	}
	if index < len(invoker.responses) {
		return invoker.responses[index], nil
	}
	return map[string]types.Value{}, nil
}

func cloneMutationParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		if data, ok := value.([]byte); ok {
			cloned := make([]byte, len(data))
			copy(cloned, data)
			out[key] = cloned
			continue
		}
		out[key] = value
	}
	return out
}

// TestSetZoneConfigMutation_EveryCanonicalKey writes every zone key through
// set_zone_config. Each case uses its own index, register and payload, so a
// renamed key (unknown field), a moved register or group, a wrong instance or
// a changed encoding (value type, enum mapping) fails its case; the second
// call must read back the same register.
func TestSetZoneConfigMutation_EveryCanonicalKey(t *testing.T) {
	cases := []struct {
		field   string
		index   int
		value   string
		addr    uint16
		payload []byte
	}{
		{field: "heating_zone_operating_mode", index: 0, value: "manual", addr: 0x0006, payload: []byte{0x02, 0x00}},
		{field: "zone_room_temperature_setpoint_at_quick_veto", index: 1, value: "22.5", addr: 0x0008, payload: []byte{0x00, 0x00, 0xB4, 0x41}},
		{field: "zone_quick_veto_time", index: 2, value: "3.5", addr: 0x0026, payload: []byte{0x00, 0x00, 0x60, 0x40}},
		{field: "heating_zone_room_manual_temperature_setpoint", index: 3, value: "21", addr: 0x0022, payload: []byte{0x00, 0x00, 0xA8, 0x41}},
		{field: "zone_room_temperature_setpoint_at_holiday", index: 4, value: "12", addr: 0x0005, payload: []byte{0x00, 0x00, 0x40, 0x41}},
	}
	if len(cases) != len(zoneConfigFieldSpecs) {
		t.Fatalf("cases cover %d zone keys; specs have %d", len(cases), len(zoneConfigFieldSpecs))
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			reg := mutationTestRegistry{
				entries: map[byte]registry.DeviceEntry{
					0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
				},
				order: []byte{0x15},
			}
			invoker := &mutationTestInvoker{responses: []any{
				map[string]types.Value{},
				map[string]types.Value{"value": {Value: append([]byte(nil), tc.payload...), Valid: true}},
			}}
			query := fmt.Sprintf(`mutation { set_zone_config(index: %d, field: %q, value: %q) { success error } }`, tc.index, tc.field, tc.value)
			payload, _ := executeMutation(t, buildMutationSchema(t, reg, invoker, nil, nil), query)["set_zone_config"].(map[string]any)
			if got, _ := payload["success"].(bool); !got || payload["error"] != nil {
				t.Fatalf("set_zone_config(%s) = %#v; want success", tc.field, payload)
			}
			if len(invoker.calls) != 2 {
				t.Fatalf("invoker calls = %d; want write and read-back", len(invoker.calls))
			}
			for i, method := range []string{mutationSetExtRegisterMethod, mutationGetExtRegisterMethod} {
				call := invoker.calls[i]
				if call.Plane != mutationSystemPlane || call.Method != method {
					t.Fatalf("call %d = %s/%s; want %s/%s", i, call.Plane, call.Method, mutationSystemPlane, method)
				}
				if call.Params["opcode"] != byte(0x02) || call.Params["group"] != byte(0x03) || call.Params["instance"] != byte(tc.index) || call.Params["addr"] != tc.addr {
					t.Fatalf("call %d selector = %#v; want opcode 0x02 group 0x03 instance %d addr 0x%04X", i, call.Params, tc.index, tc.addr)
				}
			}
			if data, _ := invoker.calls[0].Params["data"].([]byte); !bytes.Equal(data, tc.payload) {
				t.Fatalf("write payload = %x; want %x", data, tc.payload)
			}
		})
	}
}

// TestConfigSetterCamelCaseKeysRejectedWithSnakeCaseList sends every former
// camelCase key (and the withdrawn circuit frost-protection keys) to its
// setter. Each must fail before any invoke, and the error must list exactly
// the accepted snake_case keys.
func TestConfigSetterCamelCaseKeysRejectedWithSnakeCaseList(t *testing.T) {
	const (
		circuitAllowed = "circuit_cooling_enabled, circuit_flow_temperature_setpoint_minimum, circuit_heating_curve_factor, circuit_room_temperature_influence_mode, heating_circuit_flow_temperature_setpoint_maximum, heating_circuit_outdoor_temperature_limit_maximum"
		zoneAllowed    = "heating_zone_operating_mode, heating_zone_room_manual_temperature_setpoint, zone_quick_veto_time, zone_room_temperature_setpoint_at_holiday, zone_room_temperature_setpoint_at_quick_veto"
		systemAllowed  = "adaptive_heating_curve_enabled, dhw_flow_temperature_setpoint_maximum, dhw_outdoor_bivalence_temperature_limit, heating_flow_temperature_setpoint_maximum_at_heat_pump_fault, heating_outdoor_bivalence_temperature_limit, installer_access_code, installer_name, installer_phone_number, maintenance_date, room_humidity_limit_maximum"
	)
	cases := []struct {
		mutation, scope, allowed, index, value string
		fields                                 []string
	}{
		{"set_circuit_config", "circuit", circuitAllowed, "index: 1, ", "1", []string{"heatingCurve", "flowTempMaxC", "flowTempMinC", "summerLimitC", "roomTempControl", "coolingEnabled", "frostProtC", "frost_prot_c"}},
		{"set_zone_config", "zone", zoneAllowed, "index: 0, ", "1", []string{"operatingMode", "quickVetoTemperature", "quickVetoDuration", "desiredSetpoint", "holidaySetpoint"}},
		{"set_system_config", "system", systemAllowed, "", "1", []string{"dhwBivalencePointC", "maxRoomHumidityPct", "adaptiveHeatingCurve", "hcBivalencePointC", "hcEmergencyTempC", "hwcMaxFlowTempC", "maintenanceDate", "installerName", "installerPhone", "installerMenuCode"}},
	}
	reg := mutationTestRegistry{
		entries: map[byte]registry.DeviceEntry{
			0x15: testControllerEntryWithMethods(mutationSetExtRegisterMethod, mutationGetExtRegisterMethod),
		},
		order: []byte{0x15},
	}
	for _, tc := range cases {
		for _, field := range tc.fields {
			t.Run(tc.mutation+"/"+field, func(t *testing.T) {
				invoker := &mutationTestInvoker{}
				query := fmt.Sprintf(`mutation { %s(%sfield: %q, value: %q) { success error } }`, tc.mutation, tc.index, field, tc.value)
				payload, _ := executeMutation(t, buildMutationSchema(t, reg, invoker, nil, nil), query)[tc.mutation].(map[string]any)
				want := fmt.Sprintf("unknown %s field %q (allowed: %s): %s", tc.scope, field, tc.allowed, ebuserrors.ErrInvalidPayload.Error())
				if got, _ := payload["error"].(string); payload["success"] != false || got != want {
					t.Fatalf("%s(%s) = %#v; want error %q", tc.mutation, field, payload, want)
				}
				if len(invoker.calls) != 0 {
					t.Fatalf("invoker calls = %d; want none", len(invoker.calls))
				}
			})
		}
	}
}

var _ registry.FrameTemplate = schemaTemplate{}
var _ registry.FrameTemplate = buildTemplate{}
